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
fprintf(stderr,"rsf file table slot %d freed\n",i);
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

// set/release a lock on the first start of segment in a file
// set implies wait for lock to be free
// lock == 0 : release lock
// lock == 1 : set lock
// F_WRLCK will fail as expected if fd is O_RDONLY
// tested with local files, NFS, lustre, and GPFS
static int32_t RSF_Lock_sos(RSF_handle h, int lock){
  RSF_File *fp = (RSF_File *) h.p ;
  struct flock file_lock ;

  file_lock.l_type = lock ? F_WRLCK : F_UNLCK ;   // write lock | unlock
  file_lock.l_whence = SEEK_SET ;                 // locked area is at beginning of file
  file_lock.l_start = 0 ;
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
fprintf(stderr,"key0 points beyond last record\n") ;
    return -1 ; // key0 points beyond last record
  }

  scan_match = fp->matchfn ;           // get metadata match function associated to this file
  if(scan_match == NULL) scan_match = &RSF_Default_match ;     // no function associated, use default function

  nitems = fp->meta_dim ;              // size of metadata in 32 bit elements
  if(nitems <= 0) {
fprintf(stderr,"nitems <= 0\n") ;
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
fprintf(stderr,"RSF_Scan_directory returns -1\n");
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
static int32_t RSF_Read_directory(RSF_File *fp){
  int32_t status = -1 ;
  int32_t slot ;
  uint64_t size_seg, dir_seg, dir_size, dir_size2 ;
  uint64_t wa, rl ;
  start_of_segment sos ;
  start_of_record  sor ;
  off_t offset, off_seg ;
  ssize_t nc ;
  disk_directory *ddir ;
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
    lseek(fp->fd, offset = off_seg , SEEK_SET) ;            // start of segment
    nc = read(fp->fd, &sos, sizeof(start_of_segment)) ;     // read segment SOS
    if(nc <= 0) break ;                                     // end of file, was last segment

    if( RSF_Rl_sor(sos.head, RT_SOS) == 0) goto ERROR ;     // invalid sos record
    size_seg = RSF_32_to_64(sos.seg) ;                      // segment size
    dir_seg  = RSF_32_to_64(sos.dir) ;                      // offset of directory in segment
    dir_size = RSF_32_to_64(sos.dirs) ;                     // directory size from start of segment
    if(dir_size == 0) goto ERROR ;                          // something is very wrong

    ddir = (disk_directory *) malloc(dir_size) ;            // allocate directory
    if(ddir == NULL) return status ;                        // malloc failed

    lseek(fp->fd, offset = off_seg + dir_seg, SEEK_SET) ;   // seek to segment directory
    read(fp->fd, ddir, dir_size) ;                          // read segment directory
//     dir_size2 = ddir->sor.rlx ; dir_size2 <<= 32 ; dir_size2 += ddir->sor.rl ;     // directory size from record
    dir_size2 = RSF_32_to_64(ddir->sor.rl) ;
    if(dir_size2 != dir_size) goto ERROR ;                  // inconsistent sizes

    meta_dim       = ddir->meta_dim ;                       // metadata size in 32 bit units
    fp->meta_dim   = meta_dim ;
    dir_entry_size = sizeof(uint32_t)*meta_dim + sizeof(disk_dir_entry) ;  // size of a file directory entry
    e = (char *) &(ddir->entry[0]) ;                        // pointer to first directory entry
    for(i = 0 ; i < ddir->entries_nused ; i++){                    // loop over entries in disk directory
      entry = (disk_dir_entry *) e ;                        // disk directory entry (wa, rl, meta)
      wa = RSF_32_to_64(entry->wa) + off_seg ;              // record offset from start of file
      rl = RSF_32_to_64(entry->rl) ;                        // record length
      meta = &(entry->meta[0]) ;                            // record metadata
      RSF_Add_directory_entry(fp, meta, wa, rl) ;           // add entry into memory directory
// fprintf(stderr,"%12.12x(%8.8x) %8.8x %8.8x %8.8x\n", wa, rl,  meta[0], meta[meta_dim/2], meta[meta_dim-1]) ;
      e += dir_entry_size ;                                 // next entry
    }
    off_seg += size_seg ;                                   // offset for next segment
  }
  status = 0 ;

ERROR:
  free(ddir) ;
  return status ;  
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
  for(i = 1 ; i <= fp->dir_used ; i++){             // fill from in core directory
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

// allocate a data record structure that can accomodate up to max_data data bytes
// user MUST free the allocated space when no longer needed
// will be used by RSF_get
RSF_record *RSF_New_record(RSF_handle h, size_t max_data){
  RSF_File *fp = (RSF_File *) h.p ;
  size_t record_size ;
  RSF_record *r ;
  void *p ;
  start_of_record *sor ;
  end_of_record   *eor ;

  if( ! RSF_Valid_file(fp) ) return NULL ;

  record_size = sizeof(start_of_record) + sizeof(uint32_t) *  fp->meta_dim + max_data + sizeof(end_of_record) ;
  p = malloc( record_size + sizeof(RSF_record) ) ;
  if(p == NULL) return NULL ;

  r = (RSF_record *) p ;      // record structure
  p += sizeof(RSF_record) ;   // skip start of structure. p now points to dynamic part (SOR)

  sor = (start_of_record *) p ;
  sor->zr = ZR_SOR ; sor->rt = RT_DATA ; sor->rlm = fp->meta_dim ; RSF_64_to_32(sor->rl, record_size) ;

  eor = (end_of_record *) p + record_size - sizeof(end_of_record) ;
  eor->zr = ZR_EOR ; eor->rt = RT_DATA ; eor->rlm = fp->meta_dim ; RSF_64_to_32(eor->rl, record_size) ;

  r->meta = (uint32_t *) (p + sizeof(start_of_record)) ;                                                // points to metadata
  r->data = (uint32_t *) (p + sizeof(start_of_record) + sizeof(uint32_t) *  fp->meta_dim) ;             // points to data
  r->meta_size = fp->meta_dim ;
  r->data_size = max_data ;

  return r ; 
}

void RSF_Free_record(RSF_record *rh){
  return free(rh) ;
}

#if 0
// allocate a data record, ready to be written to disk
// user MUST free the allocated space after use
void *RSF_New_data_record(RSF_handle h, uint32_t **meta, uint32_t **data, size_t max_data){
  RSF_File *fp = (RSF_File *) h.p ;
  uint8_t *p ;
  start_of_record *sor ;
  end_of_record   *eor ;
  size_t record_size ;

  if( ! RSF_Valid_file(fp) ) return NULL ;

  record_size = sizeof(start_of_record) + sizeof(uint32_t) *  fp->meta_dim + max_data + sizeof(end_of_record) ;
  p = malloc(record_size) ;
  *meta = NULL ;
  *data = NULL ;

  if(p != NULL){
    sor = (start_of_record *) p ;
//     sor->zr = ZR_SOR ; sor->rt = RT_DATA ; sor->rlx = record_size >> 32 ; sor->rl = record_size & 0xFFFFFFFFu ;
    sor->zr = ZR_SOR ; sor->rt = RT_DATA ; sor->rlm = fp->meta_dim ; RSF_64_to_32(sor->rl, record_size) ;
    *meta = (uint32_t *) p + sizeof(start_of_record) ;
    *data = (uint32_t *) meta + sizeof(uint32_t) *  fp->meta_dim ;
    eor = (end_of_record *) p + record_size - sizeof(end_of_record) ;
//     eor->zr = ZR_EOR ; eor->rt = RT_DATA ; eor->rlx = record_size >> 32 ; eor->rl = record_size & 0xFFFFFFFFu ;
    eor->zr = ZR_EOR ; eor->rt = RT_DATA ; eor->rlm = fp->meta_dim ; RSF_64_to_32(eor->rl, record_size) ;
  }
  return p ;
}
#endif

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
// if meta is NULL, data is a pointer to a pre allocated record
// RSF_Adjust_data_record may have to be called
int64_t RSF_Put_data(RSF_handle h, uint32_t *meta, void *data, size_t data_size){
  RSF_File *fp = (RSF_File *) h.p ;
  uint64_t record_size, directory_record_size ;
  int64_t slot ;
  uint32_t rl ;                    // lower 32 bits of record length (bytes)
  uint16_t rlx ;                   // upper 16 bits of record length (bytes)
  start_of_record sor = SOR ;      // start of data record
  end_of_record   eor = EOR ;      // end of data record
  off_t offset ;
  start_of_segment sos ;
  RSF_record *record = NULL ;
//   int status ;

  if( ! RSF_Valid_file(fp) ) return 0 ;            // something not O.K. with fp

  if( (fp->mode & RSF_RW) != RSF_RW ) return 0 ;   // file not open in write mode
  if( fp->next_write <= 0) return 0 ;              // next_write address is not set

  // mark start_of_segment with segment length = 0 (file being written marker) if necessary
  if(fp->nwritten == 0 && fp->isnew == 0) {        // a new file (segment) is already flagged
    RSF_Lock_sos(h, 1) ;                           // lock (wait) sos portion of file
    offset = fp->seg_base ;                        // set position to beginning of segment
    lseek(fp->fd, offset, SEEK_SET) ;
    read(fp->fd, &sos, sizeof(start_of_segment)) ; // read start of segment
    if(sos.seg[0] == 0 && sos.seg[1] == 0) {       // old file already being written by another process
      return 0 ;  // ERROR
    }
    lseek(fp->fd, offset, SEEK_SET) ;
    sos.seg[0] = 0 ;                               // set segment length to 0
    sos.seg[1] = 0 ;
    write(fp->fd, &sos, sizeof(start_of_segment)) ; // rewrite start_of_segment
    RSF_Lock_sos(h, 0) ;                           // unlock sos portion of file
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
    write(fp->fd, record->data, data_size) ;                           // write to disk
    // SHOULD WE FREE THE PRE ALLOCATED RECORD (data) HERE ? (probably not)
  }else{
//     rl  = record_size & 0xFFFFFFFFu ;                // lower 32 bits
//     rlx = (record_size >> 32) & 0xFFFF ;             // upper 16 bits
//     sor.rlx = rlx ;  sor.rl  = rl  ;                           //   start_of_record
    sor.rlm = fp->meta_dim ;
    RSF_64_to_32(sor.rl, record_size) ;
    write(fp->fd, &sor, sizeof(start_of_record)) ;
    write(fp->fd, meta, fp->meta_dim * sizeof(uint32_t)) ;     // record metadata
    write(fp->fd, data, data_size) ;                           // record data
//     eor.rlx = rlx ;  eor.rl  = rl ;                            // end_of_record
    eor.rlm = fp->meta_dim ;
    RSF_64_to_32(eor.rl, record_size) ;
    write(fp->fd, &eor, sizeof(end_of_record)) ;
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
  int i ;
// fprintf(stderr,"in RSF_Lookup key = %16.16lx, crit = %8.8x @%p, mask = %8.8x @%p\n", key0, criteria[0], criteria, mask[0], mask) ;
// for(i=0 ; i<6 ; i++) fprintf(stderr,"%8.8x ",mask[i]) ; fprintf(stderr,"\n") ;
  return RSF_Scan_directory(fp, key0, criteria, mask, &wa, &rl) ;  // wa and rl not sentback to caller
}

// key from RSF_Lookup
// returns a RSF_record_handle (NULL in case or error)
// size of data and metadata returned if corresponding pointers are not NULL
// it is up to the caller to free the space
// in case of error, the function returns NULL, datasize, metasize, data, and meta are IGNORED
RSF_record *RSF_Get_new_record(RSF_handle h, int64_t key){
  RSF_File *fp = (RSF_File *) h.p ;
  int32_t indx, page ;
  uint64_t recsize, wa ;
  off_t offset ;
  int64_t payload ;
  RSF_record *record = NULL;

  if( ! RSF_Valid_file(fp) ) return NULL ;   // something wrong with fp

  indx = key & 0x7FFFFFFF ;               // get record numer from key
  indx-- ;                                // set indx to origin 0 ;
  page  = indx >> DIR_PAGE_SHFT ;         // directory page number
  indx &= DIR_PAGE_MASK ;                 // offset in directory page

  wa = (fp->pagetable[page])->warl[indx].wa ;       // record position in file
  recsize = (fp->pagetable[page])->warl[indx].rl ;  // record size
  // size of the data payload (record size - overhead)
  payload = recsize - sizeof(start_of_record) - fp->meta_dim * sizeof(uint32_t) - sizeof(end_of_record) ;
  record = RSF_New_record(h, payload) ;    // allocate a new record structure

  if(record) {
    offset = lseek(fp->fd, offset = wa, SEEK_SET) ;   // start of record in file
    read(fp->fd, record->data, recsize) ;             // read record from file
    fp->cur_pos = offset + recsize ;                  // current position is after record
    fp->last_op = OP_READ ;                           // last operation is a read operation
  }

  return record ;
}

// key from RSF_Lookup
// returns a  pointer to record blob (NULL in case or error)
// address and size of data and metadata returned if appropriate pointers are not NULL
// if record is NULL, a record buffer of appropriate dimension will be allocated, and size will be ignored
// it is up to the caller to free that space
// if record is not NULL, size must be large enough to accomodate record
// in case of error, the function returns NULL, datasize, metasize, data, and meta are IGNORED
void *RSF_Get_record(RSF_handle h, int64_t key, void *record, uint64_t size, void **meta, int32_t *metasize, void **data, uint64_t *datasize){
  RSF_File *fp = (RSF_File *) h.p ;
  int32_t indx, page ;
  uint64_t recsize, wa ;
  off_t offset ;
  int64_t payload ;
  start_of_record *sor ;

  if( ! RSF_Valid_file(fp) ) return NULL ;   // something not O.K. with fp

  indx = key & 0x7FFFFFFF ;               // get record numer from key
  indx-- ;                                // set indx to origin 0 ;
  page  = indx >> DIR_PAGE_SHFT ;         // directory page number
  indx &= DIR_PAGE_MASK ;                 // offset in directory page

  recsize = (fp->pagetable[page])->warl[indx].rl ;  // record size
  wa = (fp->pagetable[page])->warl[indx].wa ;       // record position in file
  if(record == NULL) {                              // allocate record buffer if none supplied
    record = malloc(recsize) ;
    size   = recsize ;                              // size is O.K. by definition
  }
  if(size < recsize) return NULL ;                  // supplied buffer too small

  if(record){                                       // allocate successful
    offset = lseek(fp->fd, offset = wa, SEEK_SET) ; // start of record
    read(fp->fd, record, recsize) ;                 // read record
    fp->cur_pos = offset + recsize ;                // current position is after record
    fp->last_op = OP_READ ;                         // last operation is read

    if(metasize) *metasize = fp->meta_dim ;                 // size of metadata
    if(meta) *meta = record + sizeof(start_of_record) ;     // address of metadata

    sor = (start_of_record *) record ;
//     payload = sor->rlx ; payload <<= 32 ; payload += sor->rl ;
    payload = RSF_32_to_64(sor->rl) ;
    payload = payload - sizeof(start_of_record) - fp->meta_dim * sizeof(uint32_t) - sizeof(end_of_record) ;
    if(datasize) *datasize = payload ;                      // size of data payload
    if(data) *data = record + sizeof(start_of_record) + fp->meta_dim * sizeof(uint32_t) ;  // address of data payload
  }
  return record ;
}

void *RSF_Record_meta(RSF_handle h, void *record, int32_t *metasize){
  RSF_File *fp = (RSF_File *) h.p ;

  *metasize = 0 ;
  if( ! RSF_Valid_file(fp) ) return NULL ;      // something not O.K. with fp

  *metasize = fp->meta_dim ;
  return record + sizeof(start_of_record) ;     // position of metadata in record
}

void *RSF_Record_data(RSF_handle h, void *record, int64_t *datasize){
  RSF_File *fp = (RSF_File *) h.p ;
  int64_t payload ;
  start_of_record *sor = (start_of_record *) record ;

  *datasize = 0 ;
  if( ! RSF_Valid_file(fp) ) return NULL ;      // something not O.K. with fp

//   payload = sor->rlx ; payload <<= 32 ; payload += sor->rl ;
  payload = RSF_32_to_64(sor->rl) ;
  // payload size = record size - sor size - metadata size - eor size
  payload = payload - sizeof(start_of_record) - fp->meta_dim * sizeof(uint32_t) - sizeof(end_of_record) ;
  *datasize = payload ;
  // position of data in record, after sor and metadata
  return record + sizeof(start_of_record) + fp->meta_dim * sizeof(uint32_t) ;
}

void *RSF_Get_meta(RSF_handle h, int64_t key, int32_t *metasize, uint64_t *datasize){
  RSF_File *fp ;
  int32_t indx, page ;
  uint32_t *meta ;
fprintf(stderr,"DEBUG: RSF_Get_meta\n");
return NULL ;
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
  int32_t status = -1 ;

  lseek(fd, offset_eos, SEEK_SET) ;                              // seek end of segment
  read(fd, &eos, sizeof(eos)) ;                                  // get end of segment
  if( RSF_Rl_eor(eos.h.tail, RT_EOS) == 0 ) return  -1;          // invalid EOR in EOS
  size_seg1 = RSF_32_to_64(eos.h.seg) ;                          // segment size from eos
  dir_pos1  = RSF_32_to_64(eos.h.dir) ;                          // directory offset from eos
  dir_siz1  = RSF_32_to_64(eos.h.dirs) ;                         // directory size

  lseek(fd, -size_seg1, SEEK_CUR) ;                              // seek start of segment
  read(fd, &sos, sizeof(sos)) ;                                  // get start of segment
  if( RSF_Rl_sor(sos.head, RT_SOS) != sizeof(sos) ) return  -1 ; // invalid SOR in EOS
  if( RSF_Rl_eor(sos.tail, RT_SOS) != sizeof(sos) ) return  -1 ; // invalid EOR in SOS
  size_seg2 = RSF_32_to_64(sos.seg) ;                            // segment size from sos
  dir_pos2  = RSF_32_to_64(sos.dir) ;                            // directory offset from eos
  dir_siz2  = RSF_32_to_64(sos.dirs) ;                           // directory size

  if(size_seg1 != size_seg2 || 
     dir_pos1  != dir_pos2  || 
     dir_siz1  != dir_siz2    ) return  -1 ;   // inconsistent SOS/EOS pair
}

int32_t RSF_Valid_handle(RSF_handle h){
  RSF_File *fp = (RSF_File *) h.p ;
  if( RSF_Valid_file(fp) ) return 1 ;
  return 0 ;
}

// open a file, return handle (pointer to file control structure) or NULL if error
// fname is the path to the file (it will be stored internally as a canonical path)
// mode -s RO/RW/WO/AP/... 
//   if mode is zero, it will be RW if file is writable or can be created, RO otherwise
// meta_dim is only used as input if creating a file, upon return, it is set to meta_dim from the file
// appl is the 4 character identifier for the application
// segsizep is only used as input if the file is to be "sparse", it is set to segment size from file upon return
// if segsizep is NULL, or *segsizep == 0, it is ignored
RSF_handle RSF_Open_file(char *fname, int32_t mode, int32_t *meta_dim, char *appl, int64_t *segsizep){
  RSF_File *fp = (RSF_File *) malloc(sizeof(RSF_File)) ;
  struct stat statbuf ;
  start_of_segment sos = SOS ;
  RSF_handle handle ;
  off_t offset_eof, offset_last ;
  int64_t size_last, size_last2, dir_last, dir_last2 ;
  end_of_segment eos ;
  int i ;
  int64_t segsize = 0;

// fprintf(stderr,"RSF_Open_file: 0\n");
// fprintf(stderr,"RSF_Open_file: 0b, filep = '%p'\n", fname);
  if(segsizep) segsize = *segsizep;      // segsize will be 0 if segsizep is NULL
  handle.p = NULL ;
  if(fp == NULL) return handle ;         // allocation failed

  RSF_File_init(fp) ;

  switch(mode){
    case 0:                              // automatic mode
      mode = RSF_RW ;                    // try to open for read+write/create 
      fp->fd = open(fname, O_RDWR | O_CREAT, 0777) ;
// fprintf(stderr,"RSF_Open_file: 1a, fd = %d\n", fp->fd);
      if(fp->fd == -1){                  // fallback, open in read only mode
        mode = RSF_RO ;
        fp->fd = open(fname, O_RDONLY) ;
      }
      break ;
    case RSF_RO:                         // read only open
      fp->fd = open(fname, O_RDONLY) ;
      break ;
    case RSF_RW:                         // open for read+write, create if it does not exist
      fp->fd = open(fname, O_RDWR | O_CREAT, 0777) ;
      if(fp->fd == -1){                  // fallback, open in read only mode
        mode = RSF_RO ;
        fp->fd = open(fname, O_RDONLY) ;
      }
      break ;
    case RSF_AP:                         // to be added later
      mode = RSF_RW ;                    // open existing file for read+write, no fallback
      fp->fd = open(fname, O_RDWR, 0777) ;
      break ;
    default:
      break ;
  }

// fprintf(stderr,"RSF_Open_file: 2, fd = %d\n", fp->fd);
  if(fp->fd == -1){                      // open in requested mode failed
    goto ERROR ;
  }
  fp->mode = mode ;
  fp->name = realpath(fname, NULL) ;     // get canonical path
  stat(fp->name, &statbuf) ;             // get file information
  fp->size = statbuf.st_size ;           // file size
// fprintf(stderr, "file='%s', mode = %d, fd = %d, size = %ld\n",fp->name,mode,fp->fd,fp->size);

  if(fp->mode == RSF_RW) {               // read+write
// fprintf(stderr,"RSF_Open_file: 3a\n");
    if(fp->size == 0){                   // zero size file in write mode
// fprintf(stderr,"RSF_Open_file: 3b, meta_dim = %d\n", *meta_dim) ;
      if(*meta_dim <= 0) {
        unlink(fname) ;
        goto ERROR ;    // metadata length MUST BE > 0 when creating a file
      }
      sos.meta_dim = *meta_dim ;
      fp->meta_dim = *meta_dim ;
      for(i=0 ; i<4 ; i++) sos.sig1[4+i] = appl[i] ;    // copy application signature
//       bcopy(appl, &sos.sig1[4], 4L) ;
      write(fp->fd, &sos, sizeof(sos)) ; // write initial start of segment
      fp->slot = RSF_Set_file_slot(fp) ;
      fp->next_write = sizeof(sos) ;
      fp->cur_pos = fp->next_write ;
      fp->last_op = OP_WRITE ;
      fp->isnew = 1 ;
      fp->seg_max = 0 ;                  // not a sparse segment
// fprintf(stderr,"RSF_Open_file: 3c\n");
    } else {                             // append to file
// fprintf(stderr,"RSF_Open_file: 3c\n");
      offset_eof = lseek(fp->fd, -sizeof(eos), SEEK_END) + sizeof(eos) ;
      read(fp->fd, &eos, sizeof(eos)) ;  // get end of segment expected at end of file
      if(eos.h.tail.rt != RT_EOS || eos.h.tail.zr != ZR_EOR) {   // invalid EOS
        fprintf(stderr,"ERROR: invalid EOS, rt = %d, zr = %d, rl = %8.8x %8.8x, rlm = %d\n", 
               eos.h.tail.rt, eos.h.tail.zr, eos.h.tail.rl[0], eos.h.tail.rl[1], eos.h.tail.rlm);
        goto ERROR ;
      }
// fprintf(stderr,"RSF_Open_file: opening in append mode\n");
      size_last = RSF_32_to_64(eos.h.seg) ;
      dir_last  = RSF_32_to_64(eos.h.dir) ;
      offset_last = offset_eof - size_last ;
      if(offset_last < 0) goto ERROR ;    // invalid EOS

      lseek(fp->fd, offset_last, SEEK_SET) ;
      read(fp->fd, &sos, sizeof(sos)) ;   // get associated SOS
      size_last2 = RSF_32_to_64(sos.seg) ;
      dir_last2  = RSF_32_to_64(sos.dir) ;
      if(sos.head.rt != RT_SOS || 
        sos.head.zr != 0 || 
        size_last2 != size_last ||
        dir_last2  != dir_last ){   // inconsistent SOS/EOS pair
        goto ERROR ;
      }
      fp->slot = RSF_Set_file_slot(fp) ;
      RSF_Read_directory(fp) ;      // read directory from all segments
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
    offset_eof = lseek(fp->fd, -sizeof(eos), SEEK_END) + sizeof(eos) ;
    read(fp->fd, &eos, sizeof(eos)) ;                         // get end of segment expected at end of file
    if( RSF_Rl_eor(eos.h.tail, RT_EOS) == 0 ) goto ERROR ;    // invalid EOS
    lseek(fp->fd, offset_last = 0, SEEK_SET) ;
    read(fp->fd, &sos, sizeof(sos)) ;                         // get associated SOS
    if( RSF_Rl_sor(sos.head, RT_SOS) == 0 ) goto ERROR ;      // invalid SOS
    if( RSF_Rl_eor(sos.tail, RT_SOS) == 0 ) goto ERROR ;      // invalid SOS
    fp->slot = RSF_Set_file_slot(fp) ;
    RSF_Read_directory(fp) ;           // read directory from all segments
    fp->next_write = -1 ;              // will not be writing
    fp->cur_pos = -1 ;                 // position and last operation are undefined
    fp->last_op = OP_NONE ;
    fp->isnew = 0 ;                    // not a new file
    fp->seg_max = 0 ;                  // not a sparse segment
  }
// fprintf(stderr,"RSF_Open_file: 4, fp = %p\n", fp);
  handle.p = fp ;
//   fp->slot = RSF_Set_file_slot(fp) ;
  return handle ;

ERROR:
  if(fp->name != NULL) free(fp->name) ;
  free(fp) ;
  return handle ;
}

// close a file
int32_t RSF_Close_file(RSF_handle h){
  RSF_File *fp = (RSF_File *) h.p ;
  int32_t i, slot ;
  start_of_segment sos ;
  end_of_segment eos = EOS ;
  off_t offset, offset_dir, offset_eof ;
  int64_t dir_size ;

  if( ! (slot = RSF_Valid_file(fp)) ) return 0 ;   // something not O.K. with fp

  offset_dir = fp->next_write ;
  offset_eof = offset_dir + RSF_Disk_dir_size(fp) + sizeof(end_of_segment) ;  // offset_eof points to after eos
// fprintf(stderr,"offset_dir = %16lo, offset_eof = %16lo\n",offset_dir, offset_eof);

  dir_size = RSF_Write_directory(fp) ;             // write directory to file

  // write end of segment
//   eos.l.head.rlx = 0 ;
  eos.l.head.rlm = fp->meta_dim ;
//   eos.l.head.rl  = sizeof(end_of_segment) ;
  RSF_64_to_32(eos.l.head.rl, sizeof(end_of_segment)) ;
  eos.h.meta_dim = fp->meta_dim ;
  RSF_64_to_32(eos.h.dir,  offset_dir) ;           // directory record position in file
  RSF_64_to_32(eos.h.dirs, dir_size) ;             // directory record size
  RSF_64_to_32(eos.h.seg,  offset_eof) ;           // segment size
//   eos.h.tail.rlx = 0 ;
  eos.h.tail.rlm = fp->meta_dim ;
//   eos.h.tail.rl  = sizeof(end_of_segment) ;
  RSF_64_to_32(eos.h.tail.rl, sizeof(end_of_segment)) ;
  write(fp->fd, &eos, sizeof(end_of_segment)) ;    // write end of segment (non sparse case for now)
fprintf(stderr,"CLOSE: '%s' EOS, rt = %d, zr = %d, rl = %8.8x %8.8x, rlm = %d\n", 
        fp->name, eos.h.tail.rt, eos.h.tail.zr, eos.h.tail.rl[0], eos.h.tail.rl[1], eos.h.tail.rlm);

  // fix start of segment (segment size + address of directory)
  lseek(fp->fd, offset = 0 , SEEK_SET) ;
  read(fp->fd, &sos, sizeof(start_of_segment)) ;   // read start of segment
  RSF_64_to_32(sos.dir,  offset_dir) ;             // directory record position in file
  RSF_64_to_32(sos.dirs, dir_size) ;               // directory record size
  RSF_64_to_32(sos.seg,  offset_eof) ;             // segment size
  lseek(fp->fd, offset = 0 , SEEK_SET) ;
  write(fp->fd, &sos, sizeof(start_of_segment)) ;  // rewrite start of segment

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
  int64_t wa, rl, key ;
  int32_t meta[MAX_META] ;

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
  off_t reclen, datalen, tlen ;
  ssize_t nc ;
  char *tab[] = { "NULL", "DATA", "DIR ", "SOS ", "EOS ", "DELT" } ;
  int meta_dim = -1 ;
  uint64_t segsize ;
  disk_directory *d ;
  uint32_t *meta ;
  int i ;
  uint32_t *data, signature ;
  int ndata ;
  off_t dir_offset, dir_addr, rec_offset, offset, seg_offset ;
  int64_t wa, rl ;

  if(fd < 0) return ;
  offset = 0 ;
  rec_offset = offset ;  // current position
  seg_offset = 0 ;
  nc = read(fd, &sor, sizeof(sor)) ;
  while(nc > 0) {
//     reclen = sor.rlx ; reclen <<= 32 ; reclen += sor.rl ;
    reclen = RSF_32_to_64(sor.rl) ;
    datalen = reclen - sizeof(sor) - sizeof(eor) ;
    if(sor.rt > 5) sor.rt = 5 ;
    fprintf(stderr," %s %5d [%12.12lx], len = %6ld, payload = %6ld,",tab[sor.rt],  rec, rec_offset, reclen, datalen) ;
    switch(sor.rt){
      case RT_DIR :
        dir_offset = lseek(fd, -sizeof(sor), SEEK_CUR) ; 
        d = (disk_directory *) malloc(datalen + sizeof(sor)) ;
        nc =  read(fd, d, datalen + sizeof(sor) ) ;  // read directory
        fprintf(stderr," meta_dim = %d, records = %d, dir_offset %s %8.8lx", 
               d->meta_dim, d->entries_nused, (dir_offset == dir_addr) ? "==": "!=", dir_addr) ;
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
        segsize  = RSF_32_to_64(sos.seg) ;
//         dir_addr = sos.dir[0] ; dir_addr <<= 32 ; dir_addr += sos.dir[1] ;
//         segsize = sos.seg[0] ; segsize <<= 32 ; segsize += sos.seg[1] ;
        fprintf(stderr," meta_dim = %d, seg size = %ld, dir_offset = %8.8lx",meta_dim, segsize, dir_addr) ;
        break ;
      case RT_EOS :
        lseek(fd, -sizeof(sor), SEEK_CUR) ;
        nc = read(fd, &eos, sizeof(eos) - sizeof(eor)) ;
        meta_dim = eos.h.meta_dim ;
        segsize = eos.h.seg[0] ; segsize <<= 32 ; segsize += eos.h.seg[1] ;
        fprintf(stderr," meta_dim = %d, seg size = %ld",meta_dim, segsize) ;
        break ;
      default :
        lseek(fd, datalen, SEEK_CUR) ;   // skip data
        break ;
    }
    nc = read(fd, &eor, sizeof(eor)) ;
//     tlen = eor.rlx ; tlen <<= 32 ; tlen += eor.rl ;
    tlen = RSF_32_to_64(eor.rl) ;
    fprintf(stderr,"%s\n",(tlen==reclen) ? ", O.K." : ", ERROR") ;
    if(tlen != reclen) return ;
    if(sor.rt == RT_DIR){
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
  fprintf(stderr,"file '%s' closed\n",name) ;
}
