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
printf("rsf file table slot %d freed\n",i);
      return i ;             // slot number
    }
  }
  return -1 ;   // not found
}

// =================================  utility functions =================================

// consistency checks on file handle fp
// if valid, return slot number + 1, otherwise return 0
static int32_t RSF_Valid_file(RSF_File *fp){
  if(fp == NULL) return 0 ;                   // NULL pointer
  if(fp->fd < 0) return 0 ;                   // file is not open, ERROR
  // get file slot from file handle table if not initialized
  if(fp->slot < 0) fp->slot = RSF_Find_file_slot(fp) ;
  if(fp->slot < 0) return 0 ;                 // not in file handle table
  if(fp != rsf_files[fp->slot] ) return 0 ;   // inconsistent slot
  return fp->slot + 1;
}

// =================================  directory management =================================

// add a new blank directory page to the list of pages for file pointed to by fp
// return pointer to new page if successful, NULL if unsuccessful
static dir_page *RSF_Add_directory_page(RSF_File *fp)
{
  size_t dir_page_size = RSF_Dir_page_size(*fp) ;   // size of a directory page in memory
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
  if(index > fp->dir_used) return -1 ; // key0 points beypnd last record

  scan_match = fp->matchfn ;           // get metadata match function associated to this file
  if(scan_match == NULL) scan_match = &RSF_Default_match ;     // no function associated, use default function

  nitems = fp->meta_dim ;              // size of metadata in 32 bit elements
  if(nitems <= 0) return -1 ;          // invalid metadata size

  scanpage = index >> DIR_PAGE_SHFT ;  // first page to scan
  index0   = index & DIR_PAGE_MASK ;   // scan from this position in first page
  for( ; scanpage < fp->dirpages ; scanpage++) {
    cur_page = fp->pagetable[scanpage] ;
    if(cur_page == NULL) break ;

    meta = cur_page->meta ;             // bottom of metadata for this page
    meta += nitems * index0 ;           // bump meta to reflect index0 (initial position)
    for(i = index0 ; i < cur_page->nused ; i++){
      if((*scan_match)(criteria, meta, mask, nitems) == 1 ){   // do we have a match at position i ?
        slot = slot + index + 1 ;       // add record number (origin 1) to slot
        *wa = cur_page->warl[i].wa ;    // position of record in file
        *rl = cur_page->warl[i].rl ;    // record length
        return slot ;                   // return key value containing file slot and record index
      }
      index++ ;                         // next record
      meta += nitems ;                  // metadata for next record
    }
    index0 = 0 ;                        // after first page, start from bottom of page
  }
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

// write file directory to storage device
static int32_t RSF_Write_directory(RSF_File *fp){
  int32_t slot ;
  disk_dir_entry *entry ;
  disk_directory *ddir ;
  end_of_record *eorp ;
  uint64_t dir_entry_size = RSF_Disk_dir_entry_size(*fp) ;
  size_t dir_rec_size = RSF_Disk_dir_size(*fp) ;
  ssize_t n_written ;
  int32_t status = -1 ;
  char *p = NULL ;
  char *e ;
  int i ;
  uint64_t wa64, rl64 ;

  if( ! (slot = RSF_Valid_file(fp)) ) return -1 ;             // something not O.K. with fp
  if( ( p = malloc(dir_rec_size) ) == NULL ) return -1 ;      // allocation failed
// printf("writing directory, record size = %ld, dir entry size = %ld , used = %d\n", dir_rec_size, dir_entry_size, fp->dir_used);
  ddir = (disk_directory *) p ;
  eorp = (end_of_record *) (p + dir_rec_size - sizeof(end_of_record)) ;  // point to eor at end of record

  ddir->sor.rt = RT_DIR ;                           // start of record
  ddir->sor.rl = dir_rec_size ;
  ddir->sor.zr = 0 ;

  ddir->meta_dim = fp->meta_dim ;                   // directory record body
  ddir->entries_nused = fp->dir_used ;

  eorp->rt = RT_DIR ;                               // end or record
  eorp->rl = dir_rec_size ;
  eorp->zr = 0 ;

  // fill ddir->entry
  e = (char *) &(ddir->entry[0]) ;                  // start of directory metadata portion
// printf("p = %16p, eorp = %16p, e = %16p\n", p, eorp, e);
  for(i = 1 ; i <= fp->dir_used ; i++){             // fill from in core directory
    entry = (disk_dir_entry *) e ;
    // get wa, rl, meta for entry i from in core directory
    RSF_Get_dir_entry(fp, i, &wa64, &rl64, entry->meta) ;
    entry->wa[0] = wa64 >> 32 ;                     // upper 32 bits
    entry->wa[1] = wa64 & 0xFFFFFFFFu ;             // lower 32 bits
    entry->rl[0] = rl64 >> 32 ;                     // upper 32 bits
    entry->rl[1] = rl64 & 0xFFFFFFFFu ;             // lower 32 bits
    e += dir_entry_size ;
  }
  if(fp->next_write != fp->cur_pos)                 // set position after last write if not there yet
    fp->cur_pos = lseek(fp->fd, fp->next_write , SEEK_SET) ;
  n_written = write(fp->fd, ddir, dir_rec_size) ;      // write directory record
  if(n_written == dir_rec_size) status = 0 ;        // everything writtne ?
  fp->next_write += n_written ;                     // update last write
  fp->cur_pos = fp->next_write ;                    // file current position
  fp->last_op = OP_WRITE ;                          // last operation was a write
// printf("RSF_Write_directory: records = %d\n", ddir->entries_nused);
  free(p) ;
  return status ;
}


// =================================  user callable rsf file functions =================================

// match criteria and meta where mask has bits set to 1
// if mask == NULL, it is not used
// criteria, mask and meta MUST have the same dimension : nitems
// returns 0 in case of no match, 1 otherwise
int32_t RSF_Default_match(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int nitems)
{
  int i ;
//   printf("DEBUG: calling rsf_default_match, nitems = %d\n", nitems);
  if(mask != NULL) {
    for(i = 0 ; i < nitems ; i++){
      if( (criteria[i] & mask[i]) != (meta[i] & mask[i]) ) return 0 ;  // mismatch, no need to go any further
    }
  }else{
    for(i = 0 ; i < nitems ; i++){
      if( criteria[i] != meta[i] ) return 0 ;  // mismatch, no need to go any further
    }
  }
  return 1 ;
}

// same as RSF_Default_match but ignores mask
int32_t RSF_Base_match(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int nitems)
{
  int i ;
//   printf("DEBUG: calling rsf_base_match, nitems = %d\n", nitems);
  for(i = 0 ; i < nitems ; i++){
    if( criteria[i] != meta[i] ) return 0 ;  // mismatch, no need to go any further
  }
  return 1 ;
}

int64_t RSF_Put(RSF_handle h, uint32_t *meta, void *data, size_t data_size){
  RSF_File *fp = (RSF_File *) h.p ;
  uint64_t record_size, directory_record_size ;
  int64_t slot ;
  uint32_t rl ;                    // lower 32 bits of record length (bytes)
  uint16_t rlx ;                   // upper 16 bits of record length (bytes)
  start_of_record sor = SOR ;
  end_of_record   eor = EOR ;

  if( ! RSF_Valid_file(fp) ) return 0 ;            // something not O.K. with fp
// printf("valid file\n");
  // ERROR if file not open in write mode
  if( (fp->mode & RSF_RW) != RSF_RW ) return 0 ;   // file not open in write mode
  if( fp->next_write <= 0) return 0 ;              // next_write not set

  record_size = sizeof(start_of_record) +          // start of record
                fp->meta_dim * sizeof(uint32_t) +  // record metadata
                data_size +                        // the data itself
                sizeof(end_of_record) ;            // end of record
  rl  = record_size & 0xFFFFFFFFu ;                // lower 32 bits
  rlx = (record_size >> 32) & 0xFFFF ;             // upper 16 bits
// printf("record size = %ld, %ld, %ld, %ld, %ld\n",
//        record_size,sizeof(start_of_record),fp->meta_dim * sizeof(uint32_t), data_size, sizeof(end_of_record)) ;
  // write record if enough room left in segment (always true if not sparse)
  if(fp->seg_max > 0){                                     // sparse seegmented file
    directory_record_size = RSF_Disk_dir_size(*fp) +       // space for currrent records
                            RSF_Disk_dir_entry_size(*fp) ; // + size of entry for this record
    if( (fp->next_write + record_size + directory_record_size) > fp->seg_max ) return 0 ; // not enough room
  }
  lseek(fp->fd , fp->next_write + fp->seg_base , SEEK_SET) ; // position file at fp->next_write
  // write record 
  sor.rlx = rlx ;  sor.rl  = rl  ;                           //   start_of_record
  write(fp->fd, &sor, sizeof(start_of_record)) ;
  write(fp->fd, meta, fp->meta_dim * sizeof(uint32_t)) ;     // record metadata
  write(fp->fd, data, data_size) ;                           // record data
  eor.rlx = rlx ;  eor.rl  = rl ;                            // end_of_record
  write(fp->fd, &eor, sizeof(end_of_record)) ;

  // update directory in memory
  slot = RSF_Add_directory_entry(fp, meta, fp->next_write, record_size)  ;

  fp->next_write += record_size ;         // update fp->next_write and fp->cur_pos
  fp->cur_pos = fp->next_write ;
  fp->last_op = OP_WRITE ;                // set fp->last_op to write
  // return slot/index for record (0 in case of error)
  return slot ;
}

// get key to record from file fp, matching criteria & mask, starting at key0 (slot/index)
// key0 <= 0 means start from beginning of file
int64_t RSF_Lookup(RSF_handle h, int64_t key0, uint32_t *criteria, uint32_t *mask){
  RSF_File *fp = (RSF_File *) h.p ;
  uint64_t wa, rl ;

  return RSF_Scan_directory(fp, key0, criteria, mask, &wa, &rl) ;  // wa and rl not sentback to caller
}

void *RSF_Get_data(RSF_handle h, int64_t key){
  RSF_File *fp = (RSF_File *) h.p ;

  if( ! RSF_Valid_file(fp) ) return 0 ;   // something not O.K. with fp
  // key from RSF_Lookup
  // return pointer to data blob (NULL in case or error)
  return NULL ;
}

// get pointer to metadata associated with record poited to by key from RSF_Lookup
// return pointer to metadata (NULL in case or error)
void *RSF_Get_meta(RSF_handle h, int64_t key){
  RSF_File *fp = (RSF_File *) h.p ;
  int32_t indx, page ;
  uint32_t *meta ;

  if( ! RSF_Valid_file(fp) ) return NULL ;   // something not O.K. with fp

  indx = key & 0x7FFFFFFF ;               // get record numer from key
  indx-- ;                                // set indx to origin 0 ;
  page  = indx >> DIR_PAGE_SHFT ;         // directory page number
  indx &= DIR_PAGE_MASK ;                 // offset in directory page
  meta = (fp->pagetable[page])->meta + (indx * fp->meta_dim) ;
  return ( meta ) ; // address of record metadata from directory in memory
}

// open a file, return handle (pointer to file control structure) or NULL if error
// fname is the path to the file (it will be stored internally as a canonical path)
// mode -s RO/RW/WO/AP/... 
//   if mode is zero, it will be RW if file is writable or can be created, RO otherwise
// meta_dim is only used as input if creating a file, upon return, it is set to meta_dim from the file
// appl is the 4 character identifier for the application
// segsize is only used as input if the file is to be "sparse", it is set to segment size from file upon return
// if segsize is NULL, it is ignored
RSF_handle RSF_Open_file(char *fname, int32_t mode, int32_t *meta_dim, char *appl, int64_t *segsize){
  RSF_File *fp = (RSF_File *) malloc(sizeof(RSF_File)) ;
  struct stat statbuf ;
  start_of_segment sos = SOS ;
  RSF_handle handle ;
//   end_of_segment eos = EOS ;

  handle.p = NULL ;
  if(fp == NULL) return handle ;

  RSF_File_init(*fp) ;

  switch(mode){
    case 0:                              // automatic mode
      mode = RSF_RW ;                    // try open for read+write/create 
      fp->fd = open(fname, O_RDWR | O_CREAT, 0777) ;
      if(fp->fd == -1){                  // fallback, open in read only mode
        mode = RSF_RO ;
        fp->fd = open(fname, O_RDONLY) ;
      }
      if(fp->fd > 0) fp->mode = mode ;
      break ;
    case RSF_RO:                         // read only open
      fp->fd = open(fname, O_RDONLY) ;
      if(fp->fd > 0) fp->mode = mode ;
      break ;
    case RSF_RW:                         // open for write, create if it does not exist
      fp->fd = open(fname, O_RDWR | O_CREAT, 0777) ;
      if(fp->fd > 0) fp->mode = mode ;
      break ;
    case RSF_AP:
      break ;
    default:
      break ;
  }

  if(fp->fd == -1){                      // open in requested mode failed
    free(fp) ;
    return handle ;
  }
  fp->name = realpath(fname, NULL) ;     // get canonical path
  stat(fp->name, &statbuf) ;             // get file information
  fp->size = statbuf.st_size ;           // file size
printf("file='%s', mode = %d, fd = %d, size = %ld\n",fp->name,mode,fp->fd,fp->size);

  if( (fp->size ==0) && ( fp->mode == RSF_RW ) ) {  // zero size file in write mode
    sos.meta_dim = *meta_dim ;
    fp->meta_dim = *meta_dim ;
    bcopy(appl, &sos.sig1[4], 4L) ;
    write(fp->fd, &sos, sizeof(sos)) ;              // write initial start of segment
    fp->next_write = sizeof(sos) ;
    fp->cur_pos = fp->next_write ;
    fp->last_op = OP_WRITE ;
  }

  handle.p = fp ;
  fp->slot = RSF_Set_file_slot(fp) ;
  return handle ;
}

// close a file
int32_t RSF_Close_file(RSF_handle h){
  RSF_File *fp = (RSF_File *) h.p ;
  int32_t i, slot ;
  start_of_segment sos ;
  end_of_segment eos = EOS ;
  off_t offset, offset_dir, offset_eof ;

  if( ! (slot = RSF_Valid_file(fp)) ) return 0 ;   // something not O.K. with fp

  offset_dir = fp->next_write ;
  offset_eof = offset_dir + RSF_Disk_dir_size(*fp) + sizeof(end_of_segment) ;  // offset_eof points to after eos
// printf("offset_dir = %16lo, offset_eof = %16lo\n",offset_dir, offset_eof);

  RSF_Write_directory(fp) ;             // write directory to file

  eos.l.head.rlx = 0 ;
  eos.l.head.rl  = sizeof(end_of_segment) ;
  eos.h.meta_dim = fp->meta_dim ;
  eos.h.dir[0] = offset_dir >> 32 ; eos.h.dir[1] = offset_dir & 0xFFFFFFFFu ;
  eos.h.seg[0] = offset_eof >> 32         ; eos.h.sseg[0] = eos.h.seg[0] ;
  eos.h.seg[1] = offset_eof & 0xFFFFFFFFu ; eos.h.sseg[1] = eos.h.seg[1] ;
  eos.h.tail.rlx = 0 ;
  eos.h.tail.rl  = sizeof(end_of_segment) ;
  write(fp->fd, &eos, sizeof(end_of_segment)) ;   // write end of segment (non sparse case for now)

  // fix start of segment (segment size + address of directory)
  lseek(fp->fd, offset = 0 , SEEK_SET) ;
  read(fp->fd, &sos, sizeof(start_of_segment)) ;   // read start of segment
  // fix sos.seg, sos.sseg , sos.dir  (fix code below)
  sos.seg[0] = offset_eof >> 32 ; sos.sseg[0] = sos.seg[0] ;
  sos.seg[1] = offset_eof & 0xFFFFFFFFu ; sos.sseg[1] = sos.seg[1] ;
  sos.dir[0] = offset_dir >> 32 ; sos.dir[1] = offset_dir & 0xFFFFFFFFu ;
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
  char *tab[] = { "RT_NONE", "RT_DATA", "RT_DIR ", "RT_SOS ", "RT_EOS ", "RT_DELT" } ;
  int meta_dim = -1 ;
  uint64_t segsize ;
  disk_directory *d ;
  uint32_t *meta ;
  int i ;
  uint32_t *data, signature ;
  int ndata ;
  off_t dir_offset, dir_addr ;

  if(fd < 0) return ;
  nc = read(fd, &sor, sizeof(sor)) ;
  while(nc > 0) {
    reclen = sor.rlx ; reclen <<= 32 ; reclen += sor.rl ;
    datalen = reclen - sizeof(sor) - sizeof(eor) ;
    if(sor.rt > 5) sor.rt = 5 ;
    printf(" rec %5d, len = %6ld, %s, payload = %6ld,", rec, reclen, tab[sor.rt], datalen) ;
    switch(sor.rt){
      case RT_DIR :
        dir_offset = lseek(fd, -sizeof(sor), SEEK_CUR) ; 
        d = (disk_directory *) malloc(datalen + sizeof(sor)) ;
        read(fd, d, datalen + sizeof(sor) ) ;  // read directory
        printf(" meta_dim = %d, records = %d, diradr %s %8.8lx", 
               d->meta_dim, d->entries_nused, (dir_offset == dir_addr) ? "==": "!=", dir_addr) ;
        break ;
      case RT_DATA :
        data = (uint32_t *) malloc(datalen) ;
        read(fd, data, datalen) ;
        ndata = datalen/sizeof(int32_t) - meta_dim;
        printf(" ml = %2d, dl = %6d, %8.8x %8.8x %8.8x", meta_dim, ndata, data[meta_dim], data[meta_dim+ndata/2], data[meta_dim+ndata-1]) ;
        signature = data[meta_dim] ;
        for(i=1 ; i<ndata ; i++) signature ^= data[meta_dim + i] ;
        printf(" |%8.8x|", signature) ;
        if(data) free(data) ;
        break ;
      case RT_SOS :
        lseek(fd, -sizeof(sor), SEEK_CUR) ; 
        read(fd, &sos, sizeof(sos) - sizeof(eor)) ;
        meta_dim = sos.meta_dim ;
        dir_addr = sos.dir[0] ; dir_addr <<= 32 ; dir_addr += sos.dir[1] ;
        segsize = sos.seg[0] ; segsize <<= 32 ; segsize += sos.seg[1] ;
        printf(" meta_dim = %d, seg size = %ld, diradr = %8.8lx",meta_dim, segsize, dir_addr) ;
        break ;
      case RT_EOS :
        lseek(fd, -sizeof(sor), SEEK_CUR) ;
        read(fd, &eos, sizeof(eos) - sizeof(eor)) ;
        meta_dim = eos.h.meta_dim ;
        segsize = eos.h.seg[0] ; segsize <<= 32 ; segsize += eos.h.seg[1] ;
        printf(" meta_dim = %d, seg size = %ld,",meta_dim, segsize) ;
        break ;
      default :
        lseek(fd, datalen, SEEK_CUR) ;   // skip data
        break ;
    }
    read(fd, &eor, sizeof(eor)) ;
    tlen = eor.rlx ; tlen <<= 32 ; tlen += eor.rl ;
    printf("%s\n",(tlen==reclen) ? ", O.K." : ", ERROR") ;
    if(sor.rt == RT_DIR){
      meta = (uint32_t *) &(d->entry[0]) ;
      for(i=0 ; i < d->entries_nused ; i++){
        printf(" [%6d] %4.4x%8.8x %4.4x%8.8x", i, meta[0],meta[1],meta[2],meta[3]);
        meta = meta + 4 ;
//         for(j=0 ; j<meta_dim ; j++) printf(" %8.8x",meta[j]) ; 
//         printf("\n") ;
        printf(" %8.8x %8.8x %8.8x\n", meta[0], meta[meta_dim/2] , meta[meta_dim-1]) ;
        meta = meta + meta_dim ;
      }
    }
    rec++ ;
    nc = read(fd, &sor, sizeof(sor)) ;
  }
}
