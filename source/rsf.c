/*
 * Copyright (C) 2021  Environnement et Changement climatique Canada
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation,
 * version 2.1 of the License.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * Author:
 *     M. Valin,   Recherche en Prevision Numerique, 2021
 */
#include <rsf_int.h>

// =================================  table of pointers to rsf files =================================
static pointer *rsf_files = NULL ;         // global table of pointers to rsf files (slot table)
static int rsf_files_open = 0 ;            // number of rsf files currently open
static int max_rsf_files_open = 1024 ;     // by default no more than 1024 files open simultaneously

// allocate global table of pointers to rsf files if not already done
// return table address if successful, NULL otherwise
static void *RSF_Slot_table_allocate()
{
  struct rlimit rlim ;

  if(rsf_files != NULL) return rsf_files ;                    // slot table already allocated
  getrlimit(RLIMIT_NOFILE, &rlim) ;                           // get file number limit for process
  if(rlim.rlim_cur > max_rsf_files_open) max_rsf_files_open = rlim.rlim_cur ;
  max_rsf_files_open = (max_rsf_files_open <= 131072) ? max_rsf_files_open : 131072 ; // no more than 128K files
  return  malloc( sizeof(pointer) * max_rsf_files_open) ;     // allocate table for max number of allowed files
}

// find slot matching p in global slot table (p is a pointer to RSF_file)
// return slot number if successful 
static int32_t RSF_Find_file_slot(void *p)
{
  int i ;
  if(rsf_files == NULL) rsf_files = RSF_Slot_table_allocate() ;
  if(rsf_files == NULL) return -1 ;

  if(p == NULL) return -1 ;
  for(i = 0 ; i < max_rsf_files_open ; i++) {
    if(rsf_files[i] == p) return i ;  // slot number
  }
  return -1 ;   // not found
}

// find a free slot in global slot table and set it to p (p is a pointer to RSF_file)
// return slot number if successful 
static int32_t RSF_Set_file_slot(void *p)
{
  int i ;

  if(rsf_files == NULL) rsf_files = RSF_Slot_table_allocate() ;
  if(rsf_files == NULL) return -1 ;

  for(i = 0 ; i < max_rsf_files_open ; i++) {
    if(rsf_files[i] == NULL) {
      rsf_files[i] = p ;
      rsf_files_open ++ ;     // one more open file
      return i ;              // slot number
    }
  }
  return -1 ;     // eventually add code to allocate a larger table
}

// remove existing file pointer p from global slot table (p is a pointer to RSF_file)
// return slot number if successful , -1 if error
static int32_t RSF_Purge_file_slot(void *p)
{
  int i ;

  if(rsf_files == NULL) return -1 ;  // no file table

  for(i = 0 ; i < max_rsf_files_open ; i++) {
    if(rsf_files[i] == p) {
      rsf_files[i] = (void *) NULL ;
      rsf_files_open-- ;     // one less open file
fprintf(stderr,"DEBUG: rsf file table slot %d freed\n",i);
      return i ;             // slot number
    }
  }
  return -1 ;   // not found
}

// =================================  utility functions =================================

// consistency checks on file handle fp
// if valid, return slot number + 1, otherwise return 0 (corresponding to an invalid slot)
static int32_t RSF_Valid_file(RSF_File *fp){
  if(fp == NULL) {
    fprintf(stderr,"ERROR: RSF_Valid_file, file handle is NULL\n");
    return 0 ;                   // NULL pointer
  }
  if(fp->fd < 0) {
    fprintf(stderr,"ERROR: RSF_Valid_file, fd < 0 (%d)\n", fp->fd);
    return 0 ;                   // file is not open, ERROR
  }
  // get file slot from file handle table if not initialized
  if(fp->slot < 0) fp->slot = RSF_Find_file_slot(fp) ;
  if(fp->slot < 0) {
    fprintf(stderr,"ERROR: RSF_Valid_file, no slot found\n");
    return 0 ;                 // not in file handle table
  }
  if(fp != rsf_files[fp->slot] ) {
    fprintf(stderr,"ERROR: RSF_Valid_file, inconsistent slot data %p %p, slot = %d\n", fp, rsf_files[fp->slot], fp->slot);
    return 0 ;   // inconsistent slot
  }
//   fprintf(stderr,"DEBUG: slot = %d\n", fp->slot);
  return fp->slot + 1;
}

// set/release a lock on a start of segment in a file
// start : offset in file of segment to lock
// set implies wait for lock to be free
// lock == 0 : release lock
// lock == 1 : set lock
// F_WRLCK will fail as expected if fd is O_RDONLY
// tested with local files, NFS, lustre, and GPFS
static int32_t RSF_Lock_sos(RSF_handle h, off_t start, int lock){
  RSF_File *fp = (RSF_File *) h.p ;
  struct flock file_lock ;

  file_lock.l_type = lock ? F_WRLCK : F_UNLCK ;      // write lock | unlock
  file_lock.l_whence = SEEK_SET ;                    // locked area is at beginning of file
  file_lock.l_start = start ;                        // base of segment to be locked
  file_lock.l_len = sizeof(start_of_segment) - 1 ;

  return fcntl(fp->fd, F_SETLKW, &file_lock) ;
}

// =================================  directory management =================================

// add a new blank directory page to the list of pages for file pointed to by fp
// return pointer to new page if successful, NULL if unsuccessful
static dir_page *RSF_Add_directory_page(RSF_File *fp)
{
  size_t dir_page_size = RSF_Dir_page_size(fp) ;    // size of a directory page in memory
  dir_page *newpage = malloc(dir_page_size) ;       // allocate a new directory page
  if(newpage == NULL) return NULL ;                 // allocation failure
  bzero(newpage, dir_page_size) ;                   // initialize page to zero

  newpage->nused = 0 ;                   // page is empty
  newpage->nmax  = DIR_PAGE_SIZE ;       // page capacity
  fp->dir_slots += DIR_PAGE_SIZE ;       // update total available directory entries

  // do we have a page table ? 
  // if not, allocate a default sized one, make sure there is always a NULL at the end
  if(fp->pagetable == NULL) {
    fp->pagetable = (dir_page **) malloc( (DEFAULT_PAGE_TABLE + 1) * sizeof(void *) ) ;  // 1 more entry than needed
    if(fp->pagetable == NULL) return NULL ;                                              // allocation failed

    bzero( fp->pagetable, (DEFAULT_PAGE_TABLE + 1) * sizeof(void *) ) ;                  // fill with NULLs
    fp->dirpages = DEFAULT_PAGE_TABLE ;                                                  // current size of page table
    fp->lastpage = -1 ;                                                                  // no pages used yet
  }
  fp->lastpage = fp->lastpage + 1;     // bump last page used index (will be 0 at table creation)

  if(fp->lastpage >= fp->dirpages){    // page table too small, addd DEFAULT_PAGE_TABLE more entries
    dir_page **newtable ;                                                     // new page table
    size_t table_size = fp->dirpages + DEFAULT_PAGE_TABLE ;                   // new size, DEFAULT_PAGE_TABLE larger
    newtable =  (dir_page **) malloc( table_size * sizeof(void *) ) ;
    if(newtable == NULL ) return NULL ;                                       // allocation failed
    bzero( newtable, table_size * sizeof(void *) ) ;                          // fill new table with NULLs

    memcpy( newtable, fp->pagetable, (fp->dirpages) * sizeof(void *) ) ;      // copy old table into new table
    free(fp->pagetable) ;                                                     // free old table
    fp->pagetable = newtable ;                                                // update directory page table pointer
    fp->dirpages = fp->dirpages + DEFAULT_PAGE_TABLE ;                        // new size of page table
  }
  (fp->pagetable)[fp->lastpage] = newpage ;

  return newpage ;                   // address of new page
}

// add a new record entry into the file directory
// returns file index in upper 32 bits, record index in lower 32 bits (both in origin 1)
// returns -1 in case of failure
// meta   : record primary metadata (used for searches) (direntry_size 32 bit elements)
// wa     : address in file (in 64 bit units)
// rl     : record length (metadata + data + head + tail) (in 64 bit units)
static int64_t RSF_Add_directory_entry(RSF_File *fp, uint32_t *meta, uint64_t wa, uint64_t rl)
{
  dir_page *cur_page ;
  int index, i ;
  int64_t slot ;

  if( ! (slot = RSF_Valid_file(fp)) ) return -1 ;     // something not O.K. with fp

  // slot in origin 1 (zero is invalid)
  slot <<= 32 ;                              // move to upper 32 bits

  if(fp->dir_slots <= fp->dir_used) {         // directory is full (or non existent)
    cur_page = RSF_Add_directory_page(fp) ;
    if(cur_page == NULL) return -1 ;         // failed to allocate new directory page
  }
  cur_page = fp->pagetable[fp->lastpage] ;   // point to last page from directory page table

  slot |= fp->dir_used ;                     // add index into directory
  slot++ ;                                   // set origin 1
  fp->dir_used++ ;                           // bump directory used slots
  index = cur_page->nused ;
  cur_page->nused ++ ;                       // bump directory page used slots

  cur_page->warl[index].wa = wa ;            // insert file address
  cur_page->warl[index].rl = rl ;            // insert record length
  index = index * fp->meta_dim ;             // index into metadata for this index
  for(i = 0 ; i < fp->meta_dim ; i++ ){      // copy record metadata into page entry
    cur_page->meta[index+i] = meta[i] ;
  }
  
  return slot ;  // the minimum valid value is 0x100000001   (file slot 0, directory index 0)
}

// scan directory of file fp to find a record whose (metadata & mask)  matches (criteria & mask)
// start one posotion after position indicated by key0 (if key0 == 0, start from beginning)
// returns file index in upper 32 bits, record index in lower 32 bits (both in origin 1)
// returns -1 in case of failure
static int64_t RSF_Scan_directory(RSF_File *fp, int64_t key0, uint32_t *criteria, uint32_t *mask, uint64_t *wa, uint64_t *rl)
{
  int64_t slot ;
  dir_page *cur_page ;
  RSF_Match_fn *scan_match = NULL ;
  int i, index, index0 ;
  int nitems ;
  uint32_t *meta ;
  int scanpage ;

  *wa = 0 ;                            // precondition for failure, wa and rl set to zero
  *rl = 0 ;
  if( ! (slot = RSF_Valid_file(fp)) ) return -1 ;             // something not O.K. with fp
  if( (key0 > 0) && ((key0 >> 32) != slot) ) return -1 ;      // slot in key different from file table slot
  // slot is origin 1 (zero is invalid)
  slot <<= 32 ;                        // move to upper 32 bits
  if( key0 <= 0 ) key0 = 0 ;           // first record position for this file
  index = key0 & 0x7FFFFFFF ;          // starting ordinal for search (one more than what key0 points to)
// fprintf(stderr,"key0 = %12.12lx, index = %d, mask = %8.8x\n",key0, index, mask[0]) ;
  if(index > fp->dir_used) {
fprintf(stderr,"DEBUG: key0 points beyond last record\n") ;
    return -1 ; // key0 points beyond last record
  }

  scan_match = fp->matchfn ;           // get metadata match function associated to this file
  if(scan_match == NULL) scan_match = &RSF_Default_match ;     // no function associated, use default function

  nitems = fp->meta_dim ;              // size of metadata in 32 bit elements
  if(nitems <= 0) {
fprintf(stderr,"DEBUG: nitems <= 0\n") ;
    return -1 ;          // invalid metadata size
  }

  scanpage = index >> DIR_PAGE_SHFT ;  // first page to scan
  index0   = index & DIR_PAGE_MASK ;   // scan from this position in first page
  for( ; scanpage < fp->dirpages ; scanpage++) {
    cur_page = fp->pagetable[scanpage] ;
    if(cur_page == NULL) {
      break ;
    }
// fprintf(stderr,"RSF_Scan_directory scanpage = %d, fp->dirpages = %d, index0 = %d; cur_page = %p, cur_page->nused = %d\n", scanpage, fp->dirpages, index0, cur_page, cur_page->nused) ;
    meta = cur_page->meta ;             // bottom of metadata for this page
    meta += nitems * index0 ;           // bump meta to reflect index0 (initial position)
    for(i = index0 ; i < cur_page->nused ; i++){

// fprintf(stderr,"RSF_Scan_directory mask = %8.8x, nitems = %d %d\n",mask[0], nitems, fp->meta_dim);
      if((*scan_match)(criteria, meta, mask, nitems) == 1 ){   // do we have a match at position i ?
        slot = slot + index + 1 ;       // add record number (origin 1) to slot
        *wa = cur_page->warl[i].wa ;    // position of record in file
        *rl = cur_page->warl[i].rl ;    // record length
// fprintf(stderr,"RSF_Scan_directory slot = %12.12lx\n",slot) ;
        return slot ;                   // return key value containing file slot and record index
      }
      index++ ;                         // next record
      meta += nitems ;                  // metadata for next record
    }
    index0 = 0 ;                        // after first page, start from bottom of page
  }
fprintf(stderr,"DEBUG: RSF_Scan_directory returns -1\n");
  return -1 ;  // if we get here, no match was found
}

// get full metadata information for key from file fp
// key from RSF_Scan_directory
static inline void RSF_Get_dir_entry(RSF_File *fp, int64_t key, uint64_t *wa, uint64_t *rl, uint32_t *meta) 
{
  int page, indx, j ;

  *wa = 0 ;                               // failure preset, wa and rl set to zero, meta untouched
  *rl = 0 ;
  if( ! RSF_Valid_file(fp) ) return ;     // something not O.K. with fp

  indx = (key & 0x7FFFFFFF) - 1 ;         // get record position in file in origin 0 ;
  page = indx >> DIR_PAGE_SHFT ;          // get page number
  indx = indx & DIR_PAGE_MASK ;           // get position in page

  *wa = (fp->pagetable[page])->warl[indx].wa ;      // offset from start of segment
  *rl = (fp->pagetable[page])->warl[indx].rl ;      // record length
  indx = indx * fp->meta_dim ;                      // offset into metadata for this index
  for(j = 0 ; j < fp->meta_dim ; j++){              // record metadata
    meta[j] = ((fp->pagetable[page])->meta)[indx + j] ;
  }
}

// read file directory from storage device (all segments) into memory directory
// return number of records found in segment directories (-1 upon error)
static int32_t RSF_Read_directory(RSF_File *fp){
  int32_t entries = 0 ;
  int32_t segments = 0 ;
  int32_t slot ;
  uint64_t size_seg, dir_off, dir_size, dir_size2 ;
  uint64_t wa, rl ;
  start_of_segment sos ;
  off_t offset, off_seg ;
  ssize_t nc ;
  disk_directory *ddir = NULL ;
  disk_dir_entry *entry ;
  char *e ;
  int i ;
  uint64_t dir_entry_size ;
  uint32_t *meta ;
  uint32_t meta_dim ;
// fprintf(stderr,"read directory, file '%s', slot = %d\n",fp->name, RSF_Valid_file(fp)) ;
  if( ! (slot = RSF_Valid_file(fp)) ) return -1 ;           // something not O.K. with fp
  off_seg = 0 ;                                             // first segment at beginning of file

  while(1){                                                 // loop over segments
// fprintf(stderr,"segment offset = %ld\n",off_seg) ;
    lseek(fp->fd, offset = off_seg , SEEK_SET) ;            // start of target segment
    nc = read(fp->fd, &sos, sizeof(start_of_segment)) ;     // try to read start of segment
    if(nc < sizeof(start_of_segment)) break ;               // end of file reached, done
    segments++ ;

    if( RSF_Rl_sor(sos.head, RT_SOS) == 0) goto ERROR ;     // invalid sos record (wrong record type)
    size_seg = RSF_32_to_64(sos.sseg) ;                     // segment size
    if(size_seg == 0) break ;                               // open, compact, this is the last segment
    dir_off  = RSF_32_to_64(sos.dir) ;                      // offset of directory in segment
    dir_size = RSF_32_to_64(sos.dirs) ;                     // directory size from start of segment

    if(dir_size > 0 && dir_off > 0) {                       // non empty segment
      ddir = (disk_directory *) malloc(dir_size) ;          // allocate memory to read segment directory from file
      if(ddir == NULL) goto ERROR ;                         // malloc failed

      lseek(fp->fd, offset = off_seg + dir_off, SEEK_SET) ; // seek to segment directory
      nc = read(fp->fd, ddir, dir_size) ;                   // read segment directory
      dir_size2 = RSF_32_to_64(ddir->sor.rl) ;              // directory size from record
      if(dir_size2 != dir_size) goto ERROR ;                // inconsistent sizes

      meta_dim       = ddir->meta_dim ;                     // metadata size in 32 bit units
      fp->meta_dim   = meta_dim ;
      dir_entry_size = sizeof(uint32_t)*meta_dim + sizeof(disk_dir_entry) ;  // size of a file directory entry
      e = (char *) &(ddir->entry[0]) ;                      // pointer to first directory entry
      for(i = 0 ; i < ddir->entries_nused ; i++){           // loop over entries in disk directory
        entry = (disk_dir_entry *) e ;                      // disk directory entry (wa, rl, meta)
        wa = RSF_32_to_64(entry->wa) + off_seg ;            // record offset from start of file
        rl = RSF_32_to_64(entry->rl) ;                      // record length
        meta = &(entry->meta[0]) ;                          // record metadata
        RSF_Add_directory_entry(fp, meta, wa, rl) ;         // add entry into memory directory
        entries ++ ;                                         // one more valid recird
  // fprintf(stderr,"%12.12x(%8.8x) %8.8x %8.8x %8.8x\n", wa, rl,  meta[0], meta[meta_dim/2], meta[meta_dim-1]) ;
        e += dir_entry_size ;                               // next directory entry
      }
      if(ddir) free(ddir) ;                                 // free memory used to read segment directory from file
      ddir = NULL ;                                         // to avoid a double free
    }
    off_seg += size_seg ;                                   // offset of the start of the next segment
  }  // while(1)
fprintf(stderr,"DEBUG: found %d entries in %d segment directories\n", entries, segments) ;
  return entries ;                                          // return number of records found in segment directories

ERROR:
  if(ddir != NULL) free(ddir) ;
  return -1 ;  
}

// write file directory to storage device from memory directory
static int64_t RSF_Write_directory(RSF_File *fp){
  int32_t slot ;
  disk_dir_entry *entry ;
  disk_directory *ddir ;
  end_of_record *eorp ;
  uint64_t dir_entry_size = RSF_Disk_dir_entry_size(fp) ;
  size_t dir_rec_size = RSF_Disk_dir_size(fp) ;
  ssize_t n_written ;
  char *p = NULL ;
  char *e ;
  int i ;
  uint64_t wa64, rl64 ;

  if( ! (slot = RSF_Valid_file(fp)) ) return 0 ;             // something not O.K. with fp
  if( ( p = malloc(dir_rec_size) ) == NULL ) return 0 ;      // allocation failed
// fprintf(stderr,"writing directory, record size = %ld, dir entry size = %ld , used = %d\n", dir_rec_size, dir_entry_size, fp->dir_used);
  ddir = (disk_directory *) p ;
  eorp = (end_of_record *) (p + dir_rec_size - sizeof(end_of_record)) ;  // point to eor at end of record

  ddir->sor.rt = RT_DIR ;                           // start of record
//   ddir->sor.rl = dir_rec_size ;
  RSF_64_to_32(ddir->sor.rl, dir_rec_size) ;
  ddir->sor.zr = 0 ;

  ddir->meta_dim = fp->meta_dim ;                   // directory record body
  ddir->entries_nused = fp->dir_used ;

  eorp->rt = RT_DIR ;                               // end or record
//   eorp->rl = dir_rec_size ;
  RSF_64_to_32(eorp->rl, dir_rec_size) ;
  eorp->zr = 0 ;

  // fill ddir->entry
  e = (char *) &(ddir->entry[0]) ;                  // start of directory metadata portion
// fprintf(stderr,"p = %16p, eorp = %16p, e = %16p\n", p, eorp, e);
// do not start at entry no 1, but entry no fp->dir_read + 1 (only write entries from "active" segment)
  for(i = fp->dir_read + 1 ; i <= fp->dir_used ; i++){             // fill from in core directory
    entry = (disk_dir_entry *) e ;
    // get wa, rl, meta for entry i from in core directory
    RSF_Get_dir_entry(fp, i, &wa64, &rl64, entry->meta) ;
    RSF_64_to_32(entry->wa, wa64) ;
    RSF_64_to_32(entry->rl, rl64) ;
    e += dir_entry_size ;
  }
  if(fp->next_write != fp->cur_pos)                 // set position after last write if not there yet
    fp->cur_pos = lseek(fp->fd, fp->next_write , SEEK_SET) ;
  n_written = write(fp->fd, ddir, dir_rec_size) ;   // write directory record
  if(n_written != dir_rec_size) dir_rec_size = 0 ;  // everything written ?
  fp->next_write += n_written ;                     // update last write
  fp->cur_pos = fp->next_write ;                    // file current position
  fp->last_op = OP_WRITE ;                          // last operation was a write
// fprintf(stderr,"RSF_Write_directory: records = %d\n", ddir->entries_nused);
  free(p) ;
  return dir_rec_size ;
}


// =================================  user callable rsf file functions =================================

// match criteria and meta where mask has bits set to 1
// if mask == NULL, it is not used
// criteria, mask and meta MUST have the same dimension : nitems
// returns 0 in case of no match, 1 otherwise
int32_t RSF_Default_match(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int nitems)
{
  int i ;
  if(mask != NULL) {
    for(i = 0 ; i < nitems ; i++){
      if( (criteria[i] & mask[i]) != (meta[i] & mask[i]) ) {
        fprintf(stderr,"DEBUG: rsf_default_match, MISMATCH at %d, criteria = %8.8x, meta = %8.8x, mask = %8.8x, nitems = %d\n",i, criteria[i], meta[i], mask[i], nitems) ;
        return 0 ;  // mismatch, no need to go any further
      }
    }
  }else{
    for(i = 0 ; i < nitems ; i++){
      if( criteria[i] != meta[i] ) return 0 ;  // mismatch, no need to go any further
    }
  }
//   fprintf(stderr,"DEBUG: rsf_default_match, nitems = %d, MATCH\n", nitems);
  return 1 ;
}

// same as RSF_Default_match but ignores mask
int32_t RSF_Base_match(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int nitems)
{
  int i ;
//   fprintf(stderr,"DEBUG: calling rsf_base_match, nitems = %d\n", nitems);
  for(i = 0 ; i < nitems ; i++){
    if( criteria[i] != meta[i] ) return 0 ;  // mismatch, no need to go any further
  }
  return 1 ;
}

// is this a vlid data record structure ?
int32_t RSF_Valid_record(RSF_record *r){
  void *p = (void *) r ;
  int64_t rsz ;
  if( r->sor != (void *) r->d )                                             return 1 ; // sor is wrong
  if( (r->sor + sizeof(start_of_record)) != (void *) r->meta)               return 2 ; // meta - sor wrong
  if( (r->data - (void *) r->meta) != sizeof(int32_t) * r->meta_size )      return 3 ; // meta_size inconsistent
  rsz = (r->rsz >= 0) ? r->rsz : -(r->rsz)  ;   // use absolute value
// printf("max_data = %ld, rsz = %ld, sor = %p, meta = %p, data = %p, eor max = %p\n",r->max_data,rsz,r->sor,r->meta,r->data,r->eor) ;
// printf("&r->d[r->max_data] = %p, eor = %p\n",(void *)&r->d[r->max_data], ( p + rsz - sizeof(end_of_record) ));
  if( (void *)&r->d[r->max_data] > ( p + rsz - sizeof(end_of_record) ) )    return 4 ; // data goes beyond EOR
  return 0 ;
}

// allocate a data record structure that can accomodate up to max_data data bytes of payload
// (in that case, the caller MUST free the allocated space when no longer needed)
// or
// the caller has the option to supply its own memory array (t != NULL) of size szt bytes
// if szt == 0, it must be a previously created "record" large enough to accomodate max_data
//
// this record structure will be used by RSF_Get, RSF_Put
//
// this record is reusable as long as the allocated size can accomodate max_data data bytes of payload
// this is why the allocated size is kept in the struct
// if the szt field is negative, it was not allocated by RSF_New_record
RSF_record *RSF_New_record(RSF_handle h, size_t max_data, void *t, size_t szt){
  RSF_File *fp = (RSF_File *) h.p ;
  size_t record_size ;
  RSF_record *r ;
  void *p ;
  start_of_record *sor ;
  end_of_record   *eor ;

  if( ! RSF_Valid_file(fp) ) return NULL ;
  // calculate space needed for the requested "record"
  record_size = sizeof(start_of_record) +               // start of record marker
                sizeof(uint32_t) *  fp->meta_dim +      // metadata size in bytes
                max_data +                              // maximum data payload size
                sizeof(end_of_record) ;                 // end of record marker
  if(t != NULL){                                        // caller supplied space
    p = t ;                                             // use caller supplied space
    r = (RSF_record *) p ;                              // pointer to record structure
    if(szt == 0) {                                      // passing a previously allocated record
      if( RSF_Valid_record(r) != 0 ) return NULL ;      // inconsistent info in structure
      szt = (r->rsz >= 0) ? r->rsz : -(r->rsz) ;        // get memory size from existing record
    }else{
      if(szt < 0) return NULL ;                         // invalid size for caller supplied space
      r->rsz = -szt ;                                   // caller allocated space, szt stored as negative value
    }
    if(szt < record_size) return NULL ;                 // not enough space
  }else{
    p = malloc( record_size + sizeof(RSF_record) ) ;    // add overhead size to allocated record size
    r = (RSF_record *) p ;                              // pointer to record structure
    if(p == NULL) return NULL ;                         // malloc failed 
    r->rsz = record_size + sizeof(RSF_record) ;         // allocated memory size
  }

  p += sizeof(RSF_record) ;   // skip overhead. p now points to data record part (SOR)

  sor = (start_of_record *) p ;
  r->sor  = sor ;
  sor->zr = ZR_SOR ; sor->rt = RT_DATA ; sor->rlm = fp->meta_dim ; RSF_64_to_32(sor->rl, record_size) ; // provisional sor, assuming full record

  eor = (end_of_record *) (p + record_size - sizeof(end_of_record)) ;
  r->eor  = eor ;
  eor->zr = ZR_EOR ; eor->rt = RT_DATA ; eor->rlm = fp->meta_dim ; RSF_64_to_32(eor->rl, record_size) ; // provisional eor, assuming full record

  r->meta = (uint32_t *) (p + sizeof(start_of_record)) ;                                                // points to metadata
  r->data = (void *)  (p + sizeof(start_of_record) + sizeof(uint32_t) *  fp->meta_dim) ;                // points to data payload
  r->meta_size = fp->meta_dim ;                      // metadata size in 32 bit units
  r->data_size = 0 ;                                 // no data in record yet
  r->max_data  = max_data ;                          // max data payload for this record
  // r-> rsz already set, > 0 if allocated by RSF_New_record, < 0 otherwise

  return r ; 
}

// add data to payload in record allocated by RSF_New_record
// returns how much free space remains available (-1 if data_size was too large)
// r points to a "record" allocated/initialized by RSF_New_record
// data points to data to be added to current record payload
// data_size is size in bytes of added data
int64_t RSF_Record_add_data(RSF_record *r, void *data, size_t data_size){

  if( (r->data_size + data_size) > r->max_data ) return -1 ; // data to insert too large
  memcpy(r->data + r->data_size , data, data_size ) ;        // add data to current payload
  r->data_size = r->data_size + data_size ;                  // update payload size
  return (r->max_data -r->data_size) ;                       // free space remaining
}

// free dynamic record allocated by RSF_New_record
void RSF_Free_record(RSF_record *r){
  if(r->rsz > 0) free(r) ;    // only if allocated by RSF_New_record
  return ;
}

// space available for more data in record allocated by RSF_New_record
int64_t RSF_Record_free_space(RSF_record *r){  // asssuming record payload is "managed"
  return (r->max_data -r->data_size) ;
}

// allocated size of record allocated by RSF_New_record
int64_t RSF_Record_allocated(RSF_record *r){  // asssuming record payload is "managed"
  return (r->rsz >= 0) ? r->rsz : -(r->rsz) ;
}

// maximum data payload size in record allocated by RSF_New_record
int64_t RSF_Record_max_space(RSF_record *r){  // asssuming record payload is "managed"
  return r->max_data ;
}

// pointer to data payload in record allocated by RSF_New_record
void *RSF_Record_data(RSF_record *r){  // asssuming record payload is "managed"
  return r->data ;
}

// size of data payload in record allocated by RSF_New_record
uint64_t RSF_Record_data_size(RSF_record *r){  // asssuming record payload is "managed"
  return r->data_size ;
}

// pointer to metadata in record allocated by RSF_New_record
void *RSF_Record_meta(RSF_record *r){
  return r->meta ;
}

// size of metadata in record allocated by RSF_New_record
uint32_t RSF_Record_meta_size(RSF_record *r){
  return r->meta_size ;
}

// adjust data size for record created with RSF_New_record
size_t RSF_Adjust_data_record(RSF_handle h, uint8_t *record, size_t data_size){
  RSF_File *fp = (RSF_File *) h.p ;
  start_of_record *sor ;
  end_of_record   *eor ;
  size_t new_size, old_size ;

  if( ! RSF_Valid_file(fp) ) return 0L ;

  new_size = sizeof(start_of_record) + sizeof(uint32_t) *  fp->meta_dim + data_size + sizeof(end_of_record) ;
  sor = (start_of_record *) record ;
  eor = (end_of_record *) record + new_size - sizeof(end_of_record) ;

//   old_size = sor->rlx ; old_size <<= 32 ; old_size += sor->rl ;
  old_size = RSF_32_to_64(sor->rl) ;
  if(old_size < new_size) return 0L ;
  if(old_size == new_size) return old_size ;
  sor->zr = ZR_SOR ; sor->rt = RT_DATA ; sor->rlm = fp->meta_dim ; RSF_64_to_32(sor->rl, new_size) ;
  eor->zr = ZR_EOR ; eor->rt = RT_DATA ; eor->rlm = fp->meta_dim ; RSF_64_to_32(eor->rl, new_size) ;

  return new_size ;   // adjusted size
}

// write data record to file
// meta is a pointer to record metadata (length imposed by the file)
// data is a pointer to record data
// data_size is the size (in bytes) of the data portion
// if meta is NULL, data is a pointer to a pre allocated record ( RSF_record )
// RSF_Adjust_data_record may have to be called
int64_t RSF_Put_data(RSF_handle h, uint32_t *meta, void *data, size_t data_size){
  RSF_File *fp = (RSF_File *) h.p ;
  uint64_t record_size, directory_record_size ;
  int64_t slot ;
  start_of_record sor = SOR ;      // start of data record
  end_of_record   eor = EOR ;      // end of data record
  off_t offset ;
  start_of_segment sos ;
  RSF_record *record = NULL ;
  ssize_t nc ;
//   int status ;

  if( ! RSF_Valid_file(fp) ) return 0 ;            // something not O.K. with fp

  if( (fp->mode & RSF_RW) != RSF_RW ) return 0 ;   // file not open in write mode
  if( fp->next_write <= 0) return 0 ;              // next_write address is not set

  // mark start_of_segment with segment length = 0 (file being written marker) if necessary
  if(fp->nwritten == 0 && fp->isnew == 0) {        // a new file (segment) is already flagged
    // TODO: lock current segment of file instead (fp->seg_base)
    RSF_Lock_sos(h, offset = 0, 1) ;               // lock (wait) first segment of file
    offset = fp->seg_base ;                        // set position to beginning of segment
    lseek(fp->fd, offset, SEEK_SET) ;
    nc = read(fp->fd, &sos, sizeof(start_of_segment)) ; // read start of segment
    if(sos.seg[0] == 0 && sos.seg[1] == 0) {       // old file already being written into by another process
      return 0 ;  // ERROR
    }
    lseek(fp->fd, offset, SEEK_SET) ;
    sos.seg[0] = 0 ;                               // set segment length to 0
    sos.seg[1] = 0 ;
    nc = write(fp->fd, &sos, sizeof(start_of_segment)) ; // rewrite start_of_segment
    // TODO: unlock current segment of file instead (fp->seg_base)
    RSF_Lock_sos(h, offset = 0, 0) ;                     // unlock first segment of file
  }
  record_size = sizeof(start_of_record) +          // start of record
                fp->meta_dim * sizeof(uint32_t) +  // record metadata
                data_size +                        // the data itself
                sizeof(end_of_record) ;            // end of record
// fprintf(stderr,"record size = %ld, %ld, %ld, %ld, %ld\n",
//        record_size,sizeof(start_of_record),fp->meta_dim * sizeof(uint32_t), data_size, sizeof(end_of_record)) ;
  // write record if enough room left in segment (always possible if not sparse)
  if(fp->seg_max > 0){                                     // sparse seegmented file
    directory_record_size = RSF_Disk_dir_size(fp) +        // space for currrent records
                            RSF_Disk_dir_entry_size(fp) ;  // + size of entry for this record
    if( (fp->next_write + record_size + directory_record_size) > fp->seg_max ) return 0 ; // not enough room
  }
  lseek(fp->fd , fp->next_write + fp->seg_base , SEEK_SET) ; // position file at fp->next_write
  // write record 
  if(meta == NULL){                                            // pre allocated record
    // check that metadata sizes are coherent
    record = (RSF_record *) data ;
    if(fp->meta_dim != record->meta_size) return 0 ;
    RSF_Adjust_data_record(h, record->data, data_size) ;               // adjust to actual data size
    nc = write(fp->fd, record->data, data_size) ;                           // write to disk
    // SHOULD WE FREE THE PRE ALLOCATED RECORD (data) HERE ? (probably not)
  }else{
    sor.rlm = fp->meta_dim ;
    RSF_64_to_32(sor.rl, record_size) ;
    nc = write(fp->fd, &sor, sizeof(start_of_record)) ;             // start of record
    nc = write(fp->fd, meta, fp->meta_dim * sizeof(uint32_t)) ;     // record metadata
    nc = write(fp->fd, data, data_size) ;                           // record data
    eor.rlm = fp->meta_dim ;
    RSF_64_to_32(eor.rl, record_size) ;
    nc = write(fp->fd, &eor, sizeof(end_of_record)) ;               // end_of_record
  }

  // update directory in memory
  slot = RSF_Add_directory_entry(fp, meta, fp->next_write, record_size)  ;

  fp->next_write += record_size ;         // update fp->next_write and fp->cur_pos
  fp->cur_pos = fp->next_write ;
  fp->last_op = OP_WRITE ;                // last operation is write
  fp->nwritten += 1 ;                     // update unmber of writes
  // return slot/index for record (0 in case of error)
  return slot ;
}

// write pre allocated data record to file
// record is a pointer from RSF_New_record
// data_size is the actual data size
int64_t RSF_Put_record(RSF_handle h, RSF_record *record, size_t data_size){

  return RSF_Put_data(h, NULL, record, data_size) ;
}

// get key to record from file fp, matching criteria & mask, starting at key0 (slot/index)
// key0 <= 0 means start from beginning of file
int64_t RSF_Lookup(RSF_handle h, int64_t key0, uint32_t *criteria, uint32_t *mask){
  RSF_File *fp = (RSF_File *) h.p ;
  uint64_t wa, rl ;
// fprintf(stderr,"in RSF_Lookup key = %16.16lx, crit = %8.8x @%p, mask = %8.8x @%p\n", key0, criteria[0], criteria, mask[0], mask) ;
// for(i=0 ; i<6 ; i++) fprintf(stderr,"%8.8x ",mask[i]) ; fprintf(stderr,"\n") ;
  return RSF_Scan_directory(fp, key0, criteria, mask, &wa, &rl) ;  // wa and rl not sentback to caller
}

// key from RSF_Lookup
// returns a RSF_record_handle (NULL in case or error)
// size of data and metadata returned if corresponding pointers are not NULL
// it is up to the caller to free the space
// in case of error, the function returns NULL, datasize, metasize, data, and meta are IGNORED
// the caller is responsible for freeing the allocated space
RSF_record *RSF_Get_record(RSF_handle h, int64_t key){
  RSF_File *fp = (RSF_File *) h.p ;
  int32_t indx, page ;
  uint64_t recsize, wa ;
  off_t offset ;
  int64_t payload ;
  RSF_record *record = NULL;
  ssize_t nc ;

  if( ! RSF_Valid_file(fp) ) return NULL ;            // something wrong with fp

  indx = key & 0x7FFFFFFF ;                           // get record numer from key
  indx-- ;                                            // set indx to origin 0 ;
  page  = indx >> DIR_PAGE_SHFT ;                     // directory page number
  indx &= DIR_PAGE_MASK ;                             // offset in directory page

  wa = (fp->pagetable[page])->warl[indx].wa ;         // record position in file
  recsize = (fp->pagetable[page])->warl[indx].rl ;    // record size
  // size of the data payload (record size - overhead)
  payload = recsize - 
            sizeof(start_of_record) -                 // sor marker
            fp->meta_dim * sizeof(uint32_t) -         // metadata
            sizeof(end_of_record) ;                   // eor marker
  record = RSF_New_record(h, payload, NULL, 0l) ;     // allocate a new record structure

  if(record) {                                        // allocation was succdessful
    offset = lseek(fp->fd, offset = wa, SEEK_SET) ;   // start of record in file
    nc = read(fp->fd, record->data, recsize) ;        // read record from file
    fp->cur_pos = offset + recsize ;                  // current position is after record
    fp->last_op = OP_READ ;                           // last operation is a read operation
  }

  return record ;
}

// get pointer to metadata associated with record poited to by key from RSF_Lookup
// return pointer to metadata (NULL in case or error)
void *RSF_Get_record_meta(RSF_handle h, int64_t key, int32_t *metasize, uint64_t *datasize){
  RSF_File *fp ;
  int32_t indx, page ;
  uint32_t *meta ;

  fp = (RSF_File *) h.p ;
  if( ! RSF_Valid_file(fp) ) return NULL ;   // something not O.K. with fp
  indx = key & 0x7FFFFFFF ;               // get record numer from key
  indx-- ;                                // set indx to origin 0 ;
  page  = indx >> DIR_PAGE_SHFT ;         // directory page number
  indx &= DIR_PAGE_MASK ;                 // offset in directory page
  meta = (fp->pagetable[page])->meta + (indx * fp->meta_dim) ;
  if(metasize) *metasize = fp->meta_dim ;
  if(datasize) *datasize = (fp->pagetable[page])->warl[indx].rl ;
  return ( meta ) ; // address of record metadata from directory in memory
}

// check the validity of the end of segment at file address offset_eos
static int32_t RSF_Valid_segment(int32_t fd, off_t offset_eos){
  end_of_segment eos ;
  start_of_segment sos ;
  int64_t size_seg1,  size_seg2 ;
  int64_t dir_pos1, dir_pos2 ;
  int64_t dir_siz1, dir_siz2 ;

  lseek(fd, offset_eos, SEEK_SET) ;                              // seek end of segment
  read(fd, &eos, sizeof(eos)) ;                                  // get end of segment
  if( RSF_Rl_eor(eos.h.tail, RT_EOS) == 0 ) return  -1;          // invalid EOR in EOS
  size_seg1 = RSF_32_to_64(eos.h.sseg) ;                         // end of segment offset from eos
  dir_pos1  = RSF_32_to_64(eos.h.dir) ;                          // directory offset from eos
  dir_siz1  = RSF_32_to_64(eos.h.dirs) ;                         // directory size

  lseek(fd, -size_seg1, SEEK_CUR) ;                              // seek start of segment
  read(fd, &sos, sizeof(sos)) ;                                  // get start of segment
  if( RSF_Rl_sor(sos.head, RT_SOS) != sizeof(sos) ) return  -1 ; // invalid SOR in EOS
  if( RSF_Rl_eor(sos.tail, RT_SOS) != sizeof(sos) ) return  -1 ; // invalid EOR in SOS
  size_seg2 = RSF_32_to_64(sos.sseg) ;                           // end of segment offset from sos
  dir_pos2  = RSF_32_to_64(sos.dir) ;                            // directory offset from eos
  dir_siz2  = RSF_32_to_64(sos.dirs) ;                           // directory size

  if(size_seg1 != size_seg2 || 
     dir_pos1  != dir_pos2  || 
     dir_siz1  != dir_siz2    ) return  -1 ;   // inconsistent SOS/EOS pair
  return 0 ;
}

int32_t RSF_Valid_handle(RSF_handle h){
  RSF_File *fp = (RSF_File *) h.p ;
  if( RSF_Valid_file(fp) ) return 1 ;
  return 0 ;
}

static int32_t RSF_File_lock(int fd, off_t start, off_t length, int lock){
  struct flock file_lock ;

  file_lock.l_type = lock ? F_WRLCK : F_UNLCK ;      // write lock | unlock
  file_lock.l_whence = SEEK_SET ;                    // locked area is at beginning of file
  file_lock.l_start = start ;                        // base of segment to be locked
  file_lock.l_len = length ;

  return fcntl(fd, F_SETLKW, &file_lock) ;
}

// create an empty RSF segment : start of segment, empty directory, end of segment
// if sparse_size == 0 , create a "compact" segment,
// otherwise create a "sparse" segment of size at least sparse_size (in bytes)
int RSF_New_empty_segment(RSF_File *fp, int32_t meta_dim, const char *appl, uint64_t sparse_size){
  ssize_t nc ;
  int i ;
  off_t start = 0 ;
  start_of_segment sos0 = SOS ;
  start_of_segment sos1 = SOS ;
  end_of_segment eos = EOS ;
  end_of_segment eos1 ;
  uint64_t rl_sparse ;

  if(sparse_size < 0) return -1 ;
  if(meta_dim <= 0) return -1 ;

  RSF_File_lock(fp->fd, start, sizeof(start_of_segment), 1) ;    // lock file address range 
  nc = read(fp->fd, &sos0, sizeof(start_of_segment)) ;           // try to read first start of segment into sos0

  if(nc == 0){                                                   // empty file

    fp->isnew = 1 ;                                              // NEW file
    sos0.meta_dim = meta_dim ;
    for(i=0 ; i<4 ; i++) sos0.sig1[4+i] = appl[i] ;              // copy application signature
    start = 0 ;                                                  // offset of end of file
    RSF_64_to_32(sos0.sseg, sparse_size) ;                       // sseg MUST be non zero in a new sparse segment

  }else{                                                         // existing file 

    fp->isnew = 0 ;                                              // NOT a new file
    if(nc !=  sizeof(start_of_segment)){
      fprintf(stderr, "RSF_New_empty_segment: ERROR, not a valid RSF file\n") ;
      goto ERROR ;
    }

    if(RSF_Rl_sos(sos0) == 0) {                           // check that SOS sos0 looks valid
      fprintf(stderr, "RSF_New_empty_segment: ERROR, bad start of segment in file\n");
      goto ERROR ;
    }
    if(sparse_size == 0 && sos0.head.rlm != 0) {          // already open for write (compact or sparse)
      fprintf(stderr, "RSF_New_empty_segment: ERROR, cannot open file for exclusive write\n") ;
      goto ERROR ;
    }
    if(sparse_size > 0 && sos0.head.rlm == 0xFFFF) {      // already open for "compact" write
      fprintf(stderr, "RSF_New_empty_segment: ERROR, file already open for exclusive write\n") ;
      goto ERROR ;
    }
    start = lseek(fp->fd, -sizeof(eos1), SEEK_END) ;             // offset of last end of segment in file
    nc = read(fp->fd, &eos1, sizeof(end_of_segment)) ;           // read last end of segment
    start += sizeof(end_of_segment) ;                            // offset of end of file

    if(RSF_Rl_eosh(eos1.h) == 0) {                               // check that last EOS (high part) looks valid
      fprintf(stderr, "RSF_New_empty_segment: ERROR, bad end of segment in file\n");
      goto ERROR ;
    }
  }  // if(nc == 0)

  memcpy(&sos1, &sos0, sizeof(start_of_segment)) ;               // copy sos0 into sos1

  if(sparse_size > 0) {
    // mark segment as being open for write (add one to parallel segment count in rlm)
    sos0.head.rlm = sos0.head.rlm + 1 ;
    rl_sparse = sparse_size - sizeof(start_of_segment) ;         // end of sparse segment record length
    // TODO : fill eos and sos with proper values (sparse segment size, record length)
    RSF_64_to_32(eos.h.tail.rl, rl_sparse) ;
    RSF_64_to_32(eos.h.sseg, sparse_size) ;                      // segment length = sparse_size
    eos.h.meta_dim = meta_dim ;
    RSF_64_to_32(eos.l.head.rl, rl_sparse) ;

    lseek(fp->fd, start + sparse_size - sizeof(end_of_segment_hi), SEEK_SET) ;
    nc = write(fp->fd, &eos.h, sizeof(end_of_segment_hi)) ;      // write end of segment (high part) at the correct position

    RSF_64_to_32(sos1.sseg, sparse_size) ;                       // sseg MUST be non zero in a new sparse segment
    fp->seg_max = sparse_size ;
  }else{
    sos0.head.rlm = 0xFFFF ;                                     // mark segment 0 as being open for write
    fp->seg_max = 0 ;                                            // open size segment
  }
  sos1.head.rlm = sos0.head.rlm ;                                // mark active segment as being open for write

  memcpy(&(fp->sos1), &sos1, sizeof(start_of_segment)) ;         // copy into RSF_File structure (current segment)
  lseek(fp->fd, start, SEEK_SET) ;
  nc = write(fp->fd, &sos1, sizeof(start_of_segment)) ;          // write sos1 at proper position
  memcpy(&(fp->eos1), &eos, sizeof(end_of_segment)) ;            // copy eos into RSF_File structure
  nc = write(fp->fd, &eos.l, sizeof(end_of_segment_lo)) ;        // write end of segment (low part) at the correct position

  memcpy(&(fp->sos0), &sos0, sizeof(start_of_segment)) ;         // copy sos0 into RSF_File structure
  lseek(fp->fd, 0L, SEEK_SET) ;
  nc = write(fp->fd, &sos0, sizeof(start_of_segment)) ;          // write sos0 back into to file segment 0
  if(nc != sizeof(start_of_segment)) goto ERROR ;                // write failed

#if 0
  if(nc > 0) {                                                   // existing file 

    
    start = lseek(fp->fd, start = 0, SEEK_SET) ;
    nc = write(fp->fd, &sos0, sizeof(start_of_segment)) ;        // write back sos0 to file segment 0
    start = lseek(fp->fd, -sizeof(eos1), SEEK_END) ;             // offset of last end of segment in file
    nc = read(fp->fd, &eos1, sizeof(end_of_segment)) ;           // read last end of segment
    start += sizeof(end_of_segment) ;                            // offset of end of file

    if(RSF_Rl_eosh(eos1.h) == 0) {                               // check that last EOS (high part) looks valid
      fprintf(stderr, "RSF_New_empty_segment: ERROR, bad end of segment in file\n");
      goto ERROR ;
    }
    sos1.head.rlm = 0xFFFF ;                                     // build start of segment (mark as being written into)
    sos1.meta_dim = meta_dim ;
    for(i=0 ; i<4 ; i++) sos1.sig1[4+i] = appl[i] ;              // copy application signature

    if(sparse_size > 0) {                                        // sparse segment
fprintf(stderr, "RSF_New_empty_segment DEBUG : sparse file\n") ;
      RSF_64_to_32(sos1.sseg, sparse_size) ;                     // sseg MUST be non zero in a new sparse segment
      lseek(fp->fd, start + sparse_size - sizeof(end_of_segment_hi), SEEK_SET) ;
      nc = write(fp->fd, &eos.h, sizeof(end_of_segment_hi)) ;    // write end of segment (high part) at the correct position

      lseek(fp->fd, start + sizeof(sos1), SEEK_SET) ;
      nc = write(fp->fd, &eos.l, sizeof(end_of_segment_lo)) ;    // write end of segment (LOW part) at the correct position
      lseek(fp->fd, start, SEEK_SET) ;                           // back to start of segment position
    }
    nc = write(fp->fd, &sos1, sizeof(start_of_segment)) ;        // write empty segment (marked as being written into)
    if(nc != sizeof(start_of_segment)) goto ERROR ;              // write failed
    memcpy(&(fp->sos1), &sos1, sizeof(start_of_segment)) ;       // copy into RSF_File structure (current segment)
    fp->isnew = 0 ;    // NOT a new file
//     fprintf(stderr, "RSF_New_empty_segment: ERROR, file already exists\n") ;
//     goto ERROR ;

  }else{                                                       // new file

fprintf(stderr, "RSF_New_empty_segment DEBUG : new file\n") ;
    if(sparse_size > 0) {
      RSF_64_to_32(sos0.sseg, sparse_size) ;
      rl_sparse = sparse_size - sizeof(sos1) ;                   // end of sparse segment record length
    }
    nc = write(fp->fd, &sos0, sizeof(start_of_segment)) ;      // write empty file (marked as being written into)
    if(nc != sizeof(start_of_segment)) goto ERROR ;            // write failed
    memcpy(&(fp->sos1), &sos0, sizeof(start_of_segment)) ;     // copy sos into RSF_File structure
  }

//   empty_segment.sos.head.rlm = 0xFFFF ;  // mark segment as being open for write
//   empty_segment.sos.meta_dim = meta_dim ;
//   empty_segment.sos.seg[0]  = 0 ; empty_segment.sos.seg[1]  = sizeof(empty_segment) - sizeof(end_of_segment) ;
//   empty_segment.sos.sseg[0] = 0 ; empty_segment.sos.sseg[1] = sizeof(empty_segment) ;
//   empty_segment.sos.dir[0]  = 0 ; empty_segment.sos.dir[1]  = sizeof(start_of_segment) ;
//   empty_segment.sos.dirs[0] = 0 ; empty_segment.sos.dirs[1] = sizeof(empty_disk_directory) ;
//   for(i=0 ; i<4 ; i++) empty_segment.sos.sig1[4+i] = appl[i] ;       // copy application signature
// 
//   empty_segment.d.entries_nused = 0 ;
//   empty_segment.d.meta_dim = meta_dim ;
// 
//   empty_segment.eos.l.head.rl[0] = 0 ;
//   empty_segment.eos.l.head.rl[1] = sizeof(end_of_segment) ;
//   empty_segment.eos.h.meta_dim = meta_dim ;
//   empty_segment.eos.h.seg[0]   = 0 ; empty_segment.eos.h.seg[1]   = sizeof(empty_segment) - sizeof(end_of_segment) ;
//   empty_segment.eos.h.sseg[0]  = 0 ; empty_segment.eos.h.sseg[1]  = sizeof(empty_segment) ;
//   empty_segment.eos.h.dir[0]   = 0 ; empty_segment.eos.h.dir[1]   = sizeof(start_of_segment) ;
//   empty_segment.eos.h.dirs[0]  = 0 ; empty_segment.eos.h.dirs[1]  = sizeof(empty_disk_directory) ;
//   empty_segment.eos.h.tail.rl[0] = 0 ;
//   empty_segment.eos.h.tail.rl[1] = sizeof(end_of_segment) ;

//   nc = write(fp->fd, &empty_segment, sizeof(empty_segment)) ;           // write empty file (file marked as being written into)
//   if(nc != sizeof(empty_segment)) goto ERROR ;
//   nc = write(fp->fd, &empty_segment, sizeof(start_of_segment)) ;           // write empty file (file marked as being written into)
//   if(nc != sizeof(start_of_segment)) goto ERROR ;
//   memcpy(&(fp->sos0), &empty_segment.sos, sizeof(start_of_segment)) ; // copy sos into RSF_File structure
#endif

  fp->meta_dim = meta_dim ;
  fp->slot = RSF_Set_file_slot(fp) ;
  fp->seg_base = start ;                                     // base offset of active segment
  fp->next_write = start + sizeof(start_of_segment) ;
  lseek(fp->fd, fp->next_write, SEEK_SET) ;
  fp->cur_pos = fp->next_write ;
  fp->last_op = OP_WRITE ;

  RSF_File_lock(fp->fd, start, sizeof(start_of_segment), 0) ;  // unlock file address range
  return 0 ;
ERROR :
  RSF_File_lock(fp->fd, start, sizeof(start_of_segment), 0) ;  // unlock file address range
  return -1 ;
}

// placeholder for now
// closes the file and return an error status
// will be replaced by RSF_New_empty_segment(fp, meta_dim, appl, segsizep)
int RSF_Add_sparse_segment(RSF_File *fp, int64_t segsizep){
  close(fp->fd) ;   // for now, just close file
  return -1 ;
}

// open a file (file segment), return handle (pointer to file control structure) or NULL if error
//
// fname is the path to the file (it will be stored internally as a canonical path)
// mode -s RO/RW/WO/AP/... 
//   if mode is zero, it will be RW if file is writable or can be created, RO otherwise
// meta_dim is only used as input if creating a file, upon return, it is set to meta_dim from the file
// appl is the 4 character identifier for the application
//
// segsizep is only used as input if the file is to be "sparse", segsizep[0] = segment size
// for a sparse file, the segment size is always a multiple of 1MByte
// the lower 20 bits are normally unused, segment size = segsizep[0] rounded down to the nearest MegaByte
// if non zero N = segsizep[0] & 0xFF , open Nth sparse segment
// segsizep[0] is set to segment size from file upon return
// if segsizep is NULL, or *segsizep == 0, it is ignored
//
// even if a file is open in write mode, its previous contents (but no new records) will remain available
//  if the file is open in read mode by another thread/process
// a file CANNOT be open i write mode by 2 threads/processes except if the file is sparse
//  (different threads/processe may be writing simultaneously into different segments)
//
// a segment is marked as being written into by setting the seg field of the start_of_segment to 0
//   (done under file region lock if supported by the filesystem)
RSF_handle RSF_Open_file(char *fname, int32_t mode, int32_t *meta_dim, char *appl, int64_t *segsizep){
  RSF_File *fp = (RSF_File *) malloc(sizeof(RSF_File)) ;   // allocate a new RSF_File structure
  struct stat statbuf ;
  start_of_segment sos = SOS ;
  RSF_handle handle ;
  off_t offset_eof, offset_last ;
  int64_t size_last, size_last2, dir_last, dir_last2, ssize_last, ssize_last2 ;
  end_of_segment eos ;
  int i ;
  int64_t segsize = 0;
  ssize_t nc ;
  char *errmsg = "" ;

// fprintf(stderr,"RSF_Open_file: 0, filep = '%p'\n", fname);
  if(segsizep) segsize = *segsizep;      // segsize will be 0 if segsizep is NULL

  handle.p = NULL ;
  if(fp == NULL) return handle ;         // allocation failed

  RSF_File_init(fp) ;                    // set safe initial values

  if(mode == 0) mode = RSF_RW ;          // automatic mode, try to open for read+write with ro fallback
  switch(mode & (RSF_RO | RSF_RW | RSF_AP | RSF_NSEG)){

    case RSF_RO:                         // open for read only
      errmsg = " file not found" ;
      if( (fp->fd = open(fname, O_RDONLY)) == -1 ) goto ERROR ;  // file does not exist or is not readable
      break ;

    case RSF_RW:                         // open for read+write, create if it does not exist
      fp->fd = open(fname, O_RDWR | O_CREAT, 0777) ;
      if(fp->fd == -1){                  // fallback, try to open in read only mode
        mode = RSF_RO ;
        errmsg = " file does not exist or is not readable" ;
        if( (fp->fd = open(fname, O_RDONLY)) == -1 ) goto ERROR ;  // file does not exist or is not readable
      }
      break ;

    case RSF_NSEG:                       // to be added later, add a new sparse segment
      if(*segsizep <= 0) goto ERROR ;    // invalid segment size
      mode = RSF_NSEG | RSF_RW ;         // open a new sparse segment (new or existing file)
      if( (fp->fd = open(fname, O_RDWR | O_CREAT, 0777)) == -1 ) goto ERROR ;
      // for now, RSF_Add_sparse_segment will always fail (after closing the file)
      // RSF_Add_sparse_segment will mark the file as in use for write in a sparse segment
      if( RSF_Add_sparse_segment(fp, *segsizep) < 0) goto ERROR ; // cannot add a sparse segment at end of file
      break ;

    case RSF_AP:                         // to be added later
      mode = RSF_RW ;                    // open existing file for read+write, no fallback
      errmsg = " cannot open in write mode" ;
      if( (fp->fd = open(fname, O_RDWR, 0777)) == -1) goto ERROR ;  // cannot open in write mode
      break ;

    default:
      break ;
  }
  errmsg = "" ;

// fprintf(stderr,"RSF_Open_file: 2, fd = %d\n", fp->fd);
  fp->mode = mode ;
  fp->name = realpath(fname, NULL) ;     // get canonical path
//   stat(fp->name, &statbuf) ;             // get file information
//   fp->size = statbuf.st_size ;           // file size
  fp->seg_base = 0 ;                     // a priori first segment of the file
// fprintf(stderr, "file='%s', mode = %d, fd = %d, size = %ld\n",fp->name,mode,fp->fd,fp->size);

  if( (fp->mode & RSF_RW) == RSF_RW) {               // read+write

    if( RSF_New_empty_segment(fp, *meta_dim, appl, segsize) == -1 ){
      errmsg = " cannot create empty segment at end of file" ;
      goto ERROR ;
    }
    fp->slot = RSF_Set_file_slot(fp) ;
// fprintf(stderr,"RSF_Open_file: 3a\n");
    if(0){                             // append to existing file
// fprintf(stderr,"RSF_Open_file: 3c\n");
      // TODO: check that some other thread/process is not already writing into the file
      // and if O.K. mark file as being potentially written into
      // use sos.head.rlm to mark the file as open for write (lock, rewrite sos.head, unlock)
      offset_eof = lseek(fp->fd, -sizeof(eos), SEEK_END) + sizeof(eos) ;
      nc = read(fp->fd, &eos, sizeof(eos)) ;  // get end of segment expected at end of file
      if(eos.h.tail.rt != RT_EOS || eos.h.tail.zr != ZR_EOR) {   // invalid EOS
        fprintf(stderr,"ERROR: invalid EOS, rt = %d, zr = %d, rl = %8.8x %8.8x, rlm = %d\n", 
               eos.h.tail.rt, eos.h.tail.zr, eos.h.tail.rl[0], eos.h.tail.rl[1], eos.h.tail.rlm);
        goto ERROR ;
      }
// fprintf(stderr,"RSF_Open_file: opening in append mode\n");
      size_last  = RSF_32_to_64(eos.h.seg) ;
      ssize_last = RSF_32_to_64(eos.h.sseg) ;
      dir_last   = RSF_32_to_64(eos.h.dir) ;
      offset_last = offset_eof - ssize_last ;
      if(offset_last < 0) goto ERROR ;         // invalid EOS

      lseek(fp->fd, offset_last, SEEK_SET) ;
      nc = read(fp->fd, &sos, sizeof(sos)) ;                // get associated SOS
      memcpy(&(fp->sos0), &sos, sizeof(start_of_segment)) ; // copy sos into RSF_File structure
      size_last2  = RSF_32_to_64(sos.seg) ;
      ssize_last2 = RSF_32_to_64(sos.sseg) ;
      dir_last2   = RSF_32_to_64(sos.dir) ;
      if(sos.head.rt != RT_SOS ||              // check SOS/EOS coherence
         sos.head.zr != ZR_SOR || 
         size_last2  != size_last ||
         ssize_last2 != ssize_last ||
         dir_last2   != dir_last ){
        fprintf(stderr,"RSF_Open_file: inconsistent SOS/EOS ") ;
        fprintf(stderr,"size %ld %ld, ssize %ld %ld, dir %ld %ld\n", size_last, size_last2, ssize_last, ssize_last2, dir_last, dir_last2);
        goto ERROR ;
      }
      fp->slot = RSF_Set_file_slot(fp) ;        // insert into file table
      if( RSF_Read_directory(fp) < 0 ){         // read directory from all segments
        RSF_Purge_file_slot(fp) ;               // remove from file table in case of error
        goto ERROR ;
      }
      // TODO: fix next line for sparse files (use offset_last + size_last as offset_eof ?)
      offset_eof -= sizeof(end_of_segment) ;    // old EOS will be overwritten
      lseek(fp->fd, offset_eof, SEEK_SET) ;
      *meta_dim = fp->meta_dim ;
      fp->next_write = offset_eof ;
      fp->cur_pos = fp->next_write ;
      fp->last_op = OP_NONE ;
      fp->isnew = 0 ;
      fp->seg_max = 0 ;                  // not a sparse segment
      // is it valid ?
    }
  }else{                                  // read only mode

    lseek(fp->fd, fp->seg_base = 0, SEEK_SET) ;               // first segment of the file
    if( (nc = read(fp->fd, &sos, sizeof(sos))) < sizeof(sos)) {
      errmsg = " file is empty" ;
      close(fp->fd) ;
      goto ERROR ;  // invalid SOS (too short)
    }
    // TODO: check that sos.head.rlm == 0 (file not open for write and read exclusive mode is selected)
//     if(sos.head.rlm != 0)                   goto ERROR ;      // file is open 
    if( RSF_Rl_sor(sos.head, RT_SOS) == 0 ) goto ERROR ;      // invalid SOS (wrong record type)
    if( RSF_Rl_eor(sos.tail, RT_SOS) == 0 ) goto ERROR ;      // invalid SOS (wrong record type)
    fp->slot = RSF_Set_file_slot(fp) ;        // insert into file table
    if( RSF_Read_directory(fp) < 0 ){         // read directory from all segments
      RSF_Purge_file_slot(fp) ;               // remove from file table in case of error
      goto ERROR ;
    }
    fp->next_write = -1 ;              // will not be writing
    fp->cur_pos = -1 ;                 // position and last operation are undefined
    fp->last_op = OP_NONE ;
    fp->isnew = 0 ;                    // not a new file
    fp->seg_max = 0 ;                  // not a sparse segment

  }
// fprintf(stderr,"RSF_Open_file: 4, fp = %p\n", fp);
  handle.p = fp ;
  return handle ;

ERROR:
fprintf(stderr,"RSF_Open_file ERROR : '%s' %s\n", fname, errmsg);
  if(fp->name != NULL) free(fp->name) ;  // free allocated string for file name
  free(fp) ;                             // free allocated structure
  return handle ;
}

RSF_handle RSF_Open_file_old(char *fname, int32_t mode, int32_t *meta_dim, char *appl, int64_t *segsizep){
  RSF_File *fp = (RSF_File *) malloc(sizeof(RSF_File)) ;   // allocate a new RSF_File structure
  struct stat statbuf ;
  start_of_segment sos = SOS ;
  RSF_handle handle ;
  off_t offset_eof, offset_last ;
  int64_t size_last, size_last2, dir_last, dir_last2, ssize_last, ssize_last2 ;
  end_of_segment eos ;
  int i ;
  int64_t segsize = 0;
  ssize_t nc ;

// fprintf(stderr,"RSF_Open_file: 0, filep = '%p'\n", fname);
  if(segsizep) segsize = *segsizep;      // segsize will be 0 if segsizep is NULL

  handle.p = NULL ;
  if(fp == NULL) return handle ;         // allocation failed

  RSF_File_init(fp) ;                    // set safe initial values

  if(mode == 0) mode = RSF_RW ;          // automatic mode, try to open for read+write with ro fallback
  switch(mode & (RSF_RO | RSF_RW | RSF_AP | RSF_NSEG)){

    case RSF_RO:                         // open for read only
      if( (fp->fd = open(fname, O_RDONLY)) == -1 ) goto ERROR ;  // file does not exist or is not readable
      break ;

    case RSF_RW:                         // open for read+write, create if it does not exist
      fp->fd = open(fname, O_RDWR | O_CREAT, 0777) ;
      if(fp->fd == -1){                  // fallback, try to open in read only mode
        mode = RSF_RO ;
        if( (fp->fd = open(fname, O_RDONLY)) == -1 ) goto ERROR ;  // file does not exist or is not readable
      }
      break ;

    case RSF_NSEG:                       // to be added later, add a new sparse segment
      if(*segsizep <= 0) goto ERROR ;    // invalid segment size
      mode = RSF_NSEG | RSF_RW ;         // open a new sparse segment (new or existing file)
      if( (fp->fd = open(fname, O_RDWR | O_CREAT, 0777)) == -1 ) goto ERROR ;
      // for now, RSF_Add_sparse_segment will always fail (after closing the file)
      // RSF_Add_sparse_segment will mark the file as in use for write in a sparse segment
      if( RSF_Add_sparse_segment(fp, *segsizep) < 0) goto ERROR ; // cannot add a sparse segment at end of file
      break ;

    case RSF_AP:                         // to be added later
      mode = RSF_RW ;                    // open existing file for read+write, no fallback
      if( (fp->fd = open(fname, O_RDWR, 0777)) == -1) goto ERROR ;  // cannot open in wite mode
      break ;

    default:
      break ;
  }

// fprintf(stderr,"RSF_Open_file: 2, fd = %d\n", fp->fd);
  fp->mode = mode ;
  fp->name = realpath(fname, NULL) ;     // get canonical path
  stat(fp->name, &statbuf) ;             // get file information
  fp->size = statbuf.st_size ;           // file size
  fp->seg_base = 0 ;                     // a priori first segment of the file
// fprintf(stderr, "file='%s', mode = %d, fd = %d, size = %ld\n",fp->name,mode,fp->fd,fp->size);

  if( (fp->mode & RSF_RW) == RSF_RW) {               // read+write mode ON
// fprintf(stderr,"RSF_Open_file: 3a\n");
    if(fp->size == 0){                   // zero size file in write mode
// fprintf(stderr,"RSF_Open_file: 3b, meta_dim = %d\n", *meta_dim) ;
      if( RSF_New_empty_segment(fp, *meta_dim, appl, segsize) == -1 ){  // try to create an empty non sparse file
        unlink(fname) ;
        goto ERROR ;
      }
// fprintf(stderr,"RSF_Open_file: 3c\n");
      fp->slot = RSF_Set_file_slot(fp) ;
      // NO real need to call RSF_Read_directory
//       RSF_Read_directory(fp) ;
    } else {                             // append to existing file
// fprintf(stderr,"RSF_Open_file: 3c\n");
      // TODO: check that some other thread/process is not already writing into the file
      // and if O.K. mark file as being potentially written into
      // use sos.head.rlm to mark the file as open for write (lock, rewrite sos.head, unlock)
      offset_eof = lseek(fp->fd, -sizeof(eos), SEEK_END) + sizeof(eos) ;
      nc = read(fp->fd, &eos, sizeof(eos)) ;  // get end of segment expected at end of file
      if(eos.h.tail.rt != RT_EOS || eos.h.tail.zr != ZR_EOR) {   // invalid EOS
        fprintf(stderr,"ERROR: invalid EOS, rt = %d, zr = %d, rl = %8.8x %8.8x, rlm = %d\n", 
               eos.h.tail.rt, eos.h.tail.zr, eos.h.tail.rl[0], eos.h.tail.rl[1], eos.h.tail.rlm);
        goto ERROR ;
      }
// fprintf(stderr,"RSF_Open_file: opening in append mode\n");
      size_last  = RSF_32_to_64(eos.h.seg) ;
      ssize_last = RSF_32_to_64(eos.h.sseg) ;
      dir_last   = RSF_32_to_64(eos.h.dir) ;
      offset_last = offset_eof - ssize_last ;
      if(offset_last < 0) goto ERROR ;         // invalid EOS

      lseek(fp->fd, offset_last, SEEK_SET) ;
      nc = read(fp->fd, &sos, sizeof(sos)) ;                // get associated SOS
      memcpy(&(fp->sos0), &sos, sizeof(start_of_segment)) ; // copy sos into RSF_File structure
      size_last2  = RSF_32_to_64(sos.seg) ;
      ssize_last2 = RSF_32_to_64(sos.sseg) ;
      dir_last2   = RSF_32_to_64(sos.dir) ;
      if(sos.head.rt != RT_SOS ||              // check SOS/EOS coherence
         sos.head.zr != ZR_SOR || 
         size_last2  != size_last ||
         ssize_last2 != ssize_last ||
         dir_last2   != dir_last ){
        fprintf(stderr,"RSF_Open_file: inconsistent SOS/EOS ") ;
        fprintf(stderr,"size %ld %ld, ssize %ld %ld, dir %ld %ld\n", size_last, size_last2, ssize_last, ssize_last2, dir_last, dir_last2);
        goto ERROR ;
      }
      fp->slot = RSF_Set_file_slot(fp) ;        // insert into file table
      if( RSF_Read_directory(fp) < 0 ){         // read directory from all segments
        RSF_Purge_file_slot(fp) ;               // remove from file table in case of error
        goto ERROR ;
      }
      // TODO: fix next line for sparse files (use offset_last + size_last as offset_eof ?)
      offset_eof -= sizeof(end_of_segment) ;    // old EOS will be overwritten
      lseek(fp->fd, offset_eof, SEEK_SET) ;
      *meta_dim = fp->meta_dim ;
      fp->next_write = offset_eof ;
      fp->cur_pos = fp->next_write ;
      fp->last_op = OP_NONE ;
      fp->isnew = 0 ;
      fp->seg_max = 0 ;                  // not a sparse segment
      // is it valid ?
    }
  }else{                                  // read only mode
    if(fp->size == 0) {                   // file is empty
      fprintf(stderr,"RSF_Open_file: RO file is empty\n");
      close(fp->fd) ;
      goto ERROR ;
    }
    lseek(fp->fd, fp->seg_base = 0, SEEK_SET) ;               // first segment of the file
    if( (nc = read(fp->fd, &sos, sizeof(sos))) != sizeof(sos)) goto ERROR ;  // invalid SOS (too short)
    // TODO: check that sos.head.rlm == 0 (file not open for write and read exclusive mode is selected)
//     if(sos.head.rlm != 0)                   goto ERROR ;      // file is open 
    if( RSF_Rl_sor(sos.head, RT_SOS) == 0 ) goto ERROR ;      // invalid SOS (wrong record type)
    if( RSF_Rl_eor(sos.tail, RT_SOS) == 0 ) goto ERROR ;      // invalid SOS (wrong record type)
    fp->slot = RSF_Set_file_slot(fp) ;        // insert into file table
    if( RSF_Read_directory(fp) < 0 ){         // read directory from all segments
      RSF_Purge_file_slot(fp) ;               // remove from file table in case of error
      goto ERROR ;
    }
    fp->next_write = -1 ;              // will not be writing
    fp->cur_pos = -1 ;                 // position and last operation are undefined
    fp->last_op = OP_NONE ;
    fp->isnew = 0 ;                    // not a new file
    fp->seg_max = 0 ;                  // not a sparse segment
  }
// fprintf(stderr,"RSF_Open_file: 4, fp = %p\n", fp);
  handle.p = fp ;
  return handle ;

ERROR:
fprintf(stderr,"RSF_Open_file: ERROR\n");
  if(fp->name != NULL) free(fp->name) ;  // free allocated string for file name
  free(fp) ;                             // free allocated structure
  return handle ;
}

// close a file
int32_t RSF_Close_file(RSF_handle h){
  RSF_File *fp = (RSF_File *) h.p ;
  int32_t i, slot ;
  start_of_segment sos = SOS ;
  end_of_segment eos = EOS ;
  off_t offset, offset_dir, offset_eof, cur ;
  uint64_t dir_size, sparse_size, rl_eos ;
  ssize_t nc ;

  if( ! (slot = RSF_Valid_file(fp)) ) return 0 ;   // something not O.K. with fp

  if( (fp->mode & RSF_RO) == RSF_RO) goto CLOSE ;  // file open in read only mode, nothing to rewrite

  // TODO : discriminate between compact segment (exclusive write) and sparse segment (parallel write)
  // in sparse mode, 2 SOS records (first SOS in file and SOS of sparse segment) must be rewritten
  //                 1 EOS (split EOS) record must be rewritten
  // in compact mode, 2 SOS records (first SOS in file and active segment SOS) must be rewritten
  //                  1 EOS record (at end of file) must be rewritten
  // in fuse mode     the original SOS for the active segment is reinitialized to 0
  //                  the first SOS in file becomes the only good one and points to the final EOS
  //                  the EOS at end of file becomes the only good one
  //                  all other EOS/SOS records in file (if any) will be ignored
  //                  all DIR records except the last one will be ignored
  if( (fp->mode & RSF_FUSE) == RSF_FUSE ) {
    lseek(fp->fd, fp->seg_base , SEEK_SET) ;
    nc = write(fp->fd, &sos, sizeof(start_of_segment)) ;    // reset original start_of_segment for this segment
    fp->seg_base = 0 ;    // segment 0 will become the only segment in file
    fp->isnew = 1 ;       // saved sos0 will not be used
    fp->dir_read = 0 ;    // all directory entries will be written
    fprintf(stderr,"RSF_Close_file DEBUG : fusing segments into one\n") ;
  }
  offset_dir = fp->next_write - fp->seg_base ;
  offset_eof = offset_dir + RSF_Disk_dir_size(fp) + sizeof(end_of_segment) ;  // offset_eof points to after eos
// fprintf(stderr,"offset_dir = %16lo, offset_eof = %16lo\n",offset_dir, offset_eof);

  dir_size = RSF_Write_directory(fp) ;             // write directory to file

  // write end of segment
  eos.l.head.rlm = fp->meta_dim ;
  RSF_64_to_32(eos.l.head.rl, sizeof(end_of_segment)) ;
  eos.h.meta_dim = fp->meta_dim ;
  RSF_64_to_32(eos.h.dir,  offset_dir) ;           // directory record position in file
  RSF_64_to_32(eos.h.dirs, dir_size) ;             // directory record size
  RSF_64_to_32(eos.h.sseg, offset_eof) ;           // segment size including EOS
  RSF_64_to_32(eos.h.seg,  offset_eof - sizeof(end_of_segment)) ;           // segment size excluding EOS
  eos.h.tail.rlm = fp->meta_dim ;
  RSF_64_to_32(eos.h.tail.rl, sizeof(end_of_segment)) ;
  nc = write(fp->fd, &eos, sizeof(end_of_segment)) ;    // write end of compact segment

fprintf(stderr,"DEBUG: CLOSE: '%s' EOS, rt = %d, zr = %d, rl = %8.8x %8.8x, rlm = %d\n", 
        fp->name, eos.h.tail.rt, eos.h.tail.zr, eos.h.tail.rl[0], eos.h.tail.rl[1], eos.h.tail.rlm);

  if(fp->seg_max > 0){
    sparse_size = fp->seg_max - offset_eof ;         // new sparse segment size

//     fprintf(stderr,"RSF_Close_file DEBUG : adjusting sparse segment size from %ld to %ld\n", fp->seg_max, sparse_size);
    RSF_64_to_32(sos.sseg, sparse_size) ;
    nc = write(fp->fd, &sos, sizeof(start_of_segment)) ;

    memcpy(&eos, &fp->eos1, sizeof(eos)) ;         // get or1ginal EOS
    rl_eos = sparse_size - sizeof(start_of_segment) ;
    RSF_64_to_32(eos.l.head.rl, rl_eos) ;
    nc = write(fp->fd, &eos.l, sizeof(end_of_segment_lo)) ;

    RSF_64_to_32(eos.h.tail.rl, rl_eos) ;
    RSF_64_to_32(eos.h.sseg, sparse_size) ;
    lseek(fp->fd, rl_eos - sizeof(end_of_segment_hi) - sizeof(end_of_segment_lo), SEEK_CUR);
    nc = write(fp->fd, &eos.h, sizeof(end_of_segment_hi)) ;
  }

  // fix start of active segment (segment size + address of directory)
  lseek(fp->fd, fp->seg_base , SEEK_SET) ;
  nc = read(fp->fd, &sos, sizeof(start_of_segment)) ;   // read start of segment (to keep appl signature)
  RSF_64_to_32(sos.dir,  offset_dir) ;             // directory record position in file
  RSF_64_to_32(sos.dirs, dir_size) ;               // directory record size
  RSF_64_to_32(sos.sseg,  offset_eof) ;            // segment size including EOS
  RSF_64_to_32(sos.seg,  offset_eof - sizeof(end_of_segment)) ;             // segment size excluding EOS
  sos.head.rlm = 0 ;    // fix SOR of appropriate SOS record if file was open in write mode
  lseek(fp->fd, fp->seg_base , SEEK_SET) ;
  nc = write(fp->fd, &sos, sizeof(start_of_segment)) ;      // rewrite start of active segment
// fprintf(stderr,"RSF_Close_file DEBUG : start_of_segment at %ld\n",fp->seg_base);
  if(fp->isnew == 0) {  // skip this for new files and fused files
    lseek(fp->fd, offset = 0 , SEEK_SET) ;
    fp->sos0.head.rlm = 0 ;
    nc = write(fp->fd, &fp->sos0, sizeof(start_of_segment)) ;  // rewrite start of segment 0
  }

CLOSE :
  close(fp->fd) ;                                  // close file

  RSF_Purge_file_slot(fp) ;                        // remove from file table
  // free memory associated with file
  free(fp->name) ;                                 // free file name buffer
  for(i = 0 ; i < fp->dirpages ; i++ ) free (fp->pagetable[i]) ;  // free directory pages
  free(fp) ;   // free file control structure

  return 1 ;
}

#define MAX_META 256
// dump the contents of a file's directory in condensed format
void RSF_Dump_dir(RSF_handle h) {
  RSF_File *fp = (RSF_File *) h.p ;
  int32_t i, j, slot, meta_dim ;
  uint64_t wa, rl  ;
  int64_t key ;
  uint32_t meta[MAX_META] ;

  if( ! (slot = RSF_Valid_file(fp)) ) return;   // something not O.K. with fp

  meta_dim = fp->meta_dim ;
  for(i = 0 ; i < fp->dir_used ; i++){
    key = slot ; key <<= 32 ; key = key + i + 1 ;
    RSF_Get_dir_entry(fp, key, &wa, &rl, meta) ;
    fprintf(stderr,"%12.12lx (%8.8lx)", wa, rl) ;
    for(j = 0 ; j < meta_dim ; j++) fprintf(stderr," %8.8x", meta[j]) ;
    fprintf(stderr,"\n") ;
  }
}

// dump the contents of a file in condensed format
void RSF_Dump(char *name){
  int fd = open(name, O_RDONLY) ;
  start_of_record sor ;
  end_of_record   eor ;
  start_of_segment sos ;
  end_of_segment   eos ;
  int rec = 0 ;
  off_t reclen, datalen, tlen, eoslen ;
  ssize_t nc ;
  char *tab[] = { "NULL", "DATA", "DIR ", "SOS ", "EOS ", "DELT" } ;
  int meta_dim = -1 ;
  uint64_t segsize ;
  disk_directory *d = NULL ;
  uint32_t *meta ;
  int i ;
  uint32_t *data, signature ;
  int ndata ;
  off_t dir_offset, dir_addr, rec_offset, offset, seg_offset ;
  int64_t wa, rl ;

  if(fd < 0) return ;
  dir_addr = 0 ;
  offset = 0 ;
  rec_offset = offset ;  // current position
  seg_offset = 0 ;
  nc = read(fd, &sor, sizeof(sor)) ;
  while(nc > 0) {
//     reclen = sor.rlx ; reclen <<= 32 ; reclen += sor.rl ;
    reclen = RSF_32_to_64(sor.rl) ;
    datalen = reclen - sizeof(sor) - sizeof(eor) ;
    if(sor.rt > 5) sor.rt = 5 ;
    fprintf(stderr," %s %5d [%12.12lx], rl = %6ld, dl = %6ld,",tab[sor.rt],  rec, rec_offset, reclen, datalen) ;
    switch(sor.rt){
      case RT_DIR :
        dir_offset = lseek(fd, -sizeof(sor), SEEK_CUR) ; 
        d = (disk_directory *) malloc(datalen + sizeof(sor)) ;
        nc =  read(fd, d, datalen + sizeof(sor) ) ;  // read directory
        fprintf(stderr," meta_dim = %d, records  = %8d, dir_offset %s %8.8lx, rlm = %d", 
               d->meta_dim, d->entries_nused, (dir_offset == dir_addr) ? "==": "!=", dir_addr, d->sor.rlm) ;
        break ;
      case RT_DATA :
        data = (uint32_t *) malloc(datalen) ;
        nc = read(fd, data, datalen) ;
        ndata = datalen/sizeof(int32_t) - meta_dim;
        fprintf(stderr," ml = %2d, dl = %6d, %8.8x %8.8x %8.8x", meta_dim, ndata, data[meta_dim], data[meta_dim+ndata/2], data[meta_dim+ndata-1]) ;
        signature = data[meta_dim] ;
        for(i=1 ; i<ndata ; i++) signature ^= data[meta_dim + i] ;
        fprintf(stderr," |%8.8x|", signature) ;
        if(data) free(data) ;
        break ;
      case RT_SOS :
        lseek(fd, -sizeof(sor), SEEK_CUR) ; 
        seg_offset = rec_offset ;
        nc = read(fd, &sos, sizeof(sos) - sizeof(eor)) ;
        meta_dim = sos.meta_dim ;
        dir_addr = RSF_32_to_64(sos.dir) ;
        segsize  = RSF_32_to_64(sos.sseg) ;
        fprintf(stderr," meta_dim = %d, seg size = %8ld, dir_offset == %8.8lx, rlm = %d",meta_dim, segsize, dir_addr, sos.head.rlm) ;
        break ;
      case RT_EOS :
        lseek(fd, -sizeof(sor), SEEK_CUR) ;
        nc = read(fd, &eos, sizeof(eos) - sizeof(eor)) ;          // read full EOS without end of record part
        eoslen = RSF_32_to_64(eos.l.head.rl) ;
        if(eoslen > sizeof(eos)) {                                // sparse segment EOS
          lseek(fd, eoslen - sizeof(eos) - sizeof(end_of_segment_hi) + sizeof(eor), SEEK_CUR) ;
          nc = read(fd, &eos.h, sizeof(end_of_segment_hi) - sizeof(eor)) ;   // get high part of EOS
        }
        meta_dim = eos.h.meta_dim ;
        segsize  = RSF_32_to_64(eos.h.sseg) ;
        dir_addr = RSF_32_to_64(eos.h.dir) ;
        fprintf(stderr," meta_dim = %d, seg size = %8ld, dir_offset == %8.8lx, rlm = %d",meta_dim, segsize, dir_addr, eos.l.head.rlm) ;
        break ;
      default :
        lseek(fd, datalen, SEEK_CUR) ;   // skip data
        break ;
    }
    nc = read(fd, &eor, sizeof(eor)) ;
    tlen = RSF_32_to_64(eor.rl) ;
    fprintf(stderr,"|%d rl = %ld|%ld %s\n",eor.rlm, reclen, tlen, (tlen==reclen) ? ", O.K." : ", ERROR") ;
    if(tlen != reclen) return ;
    if(sor.rt == RT_DIR && d != NULL){
      meta = (uint32_t *) &(d->entry[0]) ;
      for(i=0 ; i < d->entries_nused ; i++){
//         fprintf(stderr," [%6d] %4.4x%8.8x %4.4x%8.8x", i, meta[0],meta[1],meta[2],meta[3]);
        wa = RSF_32_to_64(meta) ;
        meta = meta + 2 ;
        rl = RSF_32_to_64(meta) ;
        meta = meta + 2 ;
        fprintf(stderr," [%6d] %12.12lx [%12.12lx] %12.12lx", i, wa, wa+seg_offset, rl) ;
//         for(j=0 ; j<meta_dim ; j++) fprintf(stderr," %8.8x",meta[j]) ; 
//         fprintf(stderr,"\n") ;
        fprintf(stderr," %8.8x %8.8x %8.8x\n", meta[0], meta[meta_dim/2] , meta[meta_dim-1]) ;
        meta = meta + meta_dim ;
      }
    }
    rec++ ;
    rec_offset = lseek(fd, offset = 0, SEEK_CUR) ;  // current position
    nc = read(fd, &sor, sizeof(sor)) ;
  }
  close(fd) ;
  fprintf(stderr,"DEBUG: file '%s' closed\n",name) ;
}
