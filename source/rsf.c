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
static size_t max_rsf_files_open = 1024 ;  // by default no more than 1024 files open simultaneously

// allocate global table of pointers to rsf files if not already done
// return table address if successful, NULL otherwise
static void *RSF_Slot_table_allocate()
{
  struct rlimit rlim ;

  if(rsf_files != NULL) return rsf_files ;                    // slot table already allocated
  getrlimit(RLIMIT_NOFILE, &rlim) ;                           // get file number limit for process
  if(rlim.rlim_cur > max_rsf_files_open) max_rsf_files_open = rlim.rlim_cur ;
  max_rsf_files_open = (max_rsf_files_open <= 131072) ? max_rsf_files_open : 131072 ; // no more than 128K files
  return  calloc(sizeof(pointer), max_rsf_files_open) ;       // allocate zro filled table for max number of allowed files
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
fprintf(stderr,"RSF_Set_file_slot DEBUG: rsf file table slot %d assigned, p = %p\n", i, p);
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
fprintf(stderr,"RSF_Purge_file_slot DEBUG: rsf file table slot %d freed, p = %p\n", i, p);
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

#if 0
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
#endif
// =================================  directory management =================================
// =================================  VARIABLE metadata size =================================

// add a new directory block of size s and insert it into the block list of file fp
// (VARIABLE length metadata directories)
// return the address of the block
static directory_block *RSF_Add_vdir_block(RSF_File *fp)
{
  directory_block *dd ;
  size_t sz = sizeof(directory_block) + DIR_BLOCK_SIZE ;
  void *p = calloc(sz, sizeof(char)) ;   // allocate block filled with zeroes

  if(p == NULL) return NULL ;
  dd = (directory_block *) p ;
  dd->next = fp->dirblocks ;
  dd->cur = dd->entries ;
  dd->top = p + sz ;
  fp->dirblocks = dd ;
fprintf(stderr,"RSF_Add_vdir_block DEBUG: added block of size %ld, next = %p, fp->dir = %p\n", sz, dd->next, fp->dirblocks) ;
  return dd ;
}

// initialize VARIABLE metadata directory structures
static void RSF_Vdir_init(RSF_File *fp){
  int i ;
  if(fp->dirblocks == NULL) {
    RSF_Add_vdir_block(fp) ;                                // first time around
    fp->vdir_size = sizeof(disk_vdir) + sizeof(end_of_record) ;     // fixed part + end_of_record
fprintf(stderr,"RSF_Vdir_init DEBUG: fp->dir = %p\n", fp->dirblocks) ;
  }
  if(fp->vdir_used >= fp->vdir_slots) {                                     // need more slots, reallocate into larger memory area
    fp->vdir_slots += DIR_SLOTS_INCREMENT ;                                 // increase number of slots
    fp->vdir = (vdir_entry **) realloc(fp->vdir, fp->vdir_slots * sizeof(void *)) ;
    for(i = fp->vdir_used ; i < fp->vdir_slots ; i++) fp->vdir[i] = NULL ;  // nullify new slots
fprintf(stderr,"RSF_Vdir_init DEBUG: realloc fp->vdir = %p, slots = %d, used = %d\n", fp->vdir, fp->vdir_slots, fp->vdir_used) ;
  }
}

// add a new VARIABLE length metadata entry to directory
static int64_t RSF_Add_vdir_entry(RSF_File *fp, uint32_t *meta, uint32_t ml, uint64_t wa, uint64_t rl)
{
  directory_block *dd ;
  int needed = sizeof(vdir_entry) + ml * sizeof(uint32_t) ;   // needed size for entry to be added
  vdir_entry *entry ;
  int i ;
  int64_t index, slot ;
//   void *where ;

  if(fp == NULL) goto ERROR ;
  if( ! (slot = RSF_Valid_file(fp)) ) return -1 ;     // something not O.K. with fp
  slot <<= 32 ;
  RSF_Vdir_init(fp) ;
  dd = fp->dirblocks ;

  needed = sizeof(vdir_entry) +        // base size for entry to be added
           ml * sizeof(uint32_t) ;     // metadata size
  if(dd->top - dd->cur < needed) {                    // add a block if no room for entry
// fprintf(stderr,"RSF_Add_vdir_entry DEBUG: need %d, have %d, allocating a new block\n", needed, dd->top - dd->cur) ;
    dd = RSF_Add_vdir_block(fp) ;
  }
  if(dd == NULL) goto ERROR ;
  entry = (vdir_entry *) dd->cur ;
  dd->cur += needed ;
  RSF_64_to_32(entry->wa, wa) ;
  RSF_64_to_32(entry->rl, rl) ;
  entry->ml = ml ;
  for(i = 0 ; i < ml ; i++) entry->meta[i] = meta[i] ;

  fp->vdir[fp->vdir_used] = entry ;
//   where = &(fp->vdir[fp->vdir_used]) ;
  fp->vdir_used++ ;
  fp->vdir_size += needed ;    // add to directory size
  index = fp->vdir_used ;
  index |= slot ;
// fprintf(stderr,"RSF_Add_vdir_entry DEBUG: ts = %p, entry = %p,  wa = %lx, rl = %ld, ml = %d, index = %16.16lx\n", where, entry, wa, rl, ml, index) ;
  return index ;
ERROR:
  return -1 ;
}

int32_t RSF_Get_vdir_entry(RSF_File *fp, int64_t key, uint64_t *wa, uint64_t *rl, uint32_t **meta){
  int inxd ;
  int32_t slot, indx ;
  vdir_entry *ventry ;
  char *error = "unknown" ;

  *wa = 0 ;
  *rl = 0 ;
  *meta = NULL ;
  if( ! (slot = RSF_Valid_file(fp)) ) {
    error = "invalid file" ;
    goto ERROR ;
  }
  if( slot != (key >> 32) ){
    error = "invalid slot" ;
    goto ERROR ;                    // wrong slot for fp
  }
  if( (indx = (key & 0x7FFFFFFF) - 1) >= fp->vdir_used){
    error = "invalid record number" ;
    goto ERROR ;  // invalid record number
  }

  ventry = fp->vdir[indx] ;
  *wa = RSF_32_to_64(ventry->wa) ;
  *rl = RSF_32_to_64(ventry->rl) ;
  *meta = &(ventry->meta[0]) ;
  return ventry->ml ;

ERROR :
  fprintf(stderr,"RSF_Get_vdir_entry ERROR : %s\n", error) ;
  return -1 ;
}

// scan vdir (VARIABLE length metadata) of file fp to find a record where (metadata & mask)  matches (criteria & mask)
// start one position after position indicated by key0 (if key0 == 0, start from beginning of file)
// return file slot(index) in upper 32 bits, record index in lower 32 bits (both in origin 1)
// return an invalid key if no match is found
// criteria, mask are 32 bit arrays
// lcrit is the length of both criteria and mask arrays
int64_t RSF_Scan_vdir(RSF_File *fp, int64_t key0, uint32_t *criteria, uint32_t *mask, uint32_t lcrit, uint64_t *wa, uint64_t *rl)
{
  int64_t slot ;
  int index, i, nmeta ;
  vdir_entry *ventry ;
  uint32_t *meta ;
  uint32_t rt0, class0 ;
  RSF_Match_fn *scan_match = NULL ;
  uint32_t mask0 = 0 ;
  char *error = "" ;
  int64_t badkey = -1 ;

  if( ! (slot = RSF_Valid_file(fp)) ){
    error = "invalid file reference" ;
    goto ERROR ;      // something wrong. with fp
  }
  if( (key0 != 0) && ((key0 >> 32) != slot) ) {
    error = "invalid slot" ;
    goto ERROR ;      // slot in key different from file table slot
  }
  // slot is origin 1 (zero is invalid)
  slot <<= 32 ;                             // move to upper 32 bits
  if( key0 < -1 ) key0 = 0 ;                // first record position for this file
  index = key0 & 0x7FFFFFFF ;               // starting ordinal for search (one more than what key0 points to)
  if(index >= fp->vdir_used) {
    error = "beyond last record" ;
    goto ERROR ;   // beyond last record
  }

  scan_match = fp->matchfn ;           // get metadata match function associated to this file
  if(scan_match == NULL) scan_match = &RSF_Default_match ;     // no function associated, use default function

  mask0 = mask ? mask[0] : 0 ;                      // set mask0 to 0 if mask is NULL
  rt0      = (criteria[0] & mask0) & 0xFF ;         // low level record type match target
  if(rt0 >= RT_DEL) rt0 = 0 ;                       // invalid record type
  // check for valid type for a data record (RT_DATA, RT_XDAT, 8->RT_DEL-1 are valid)
  if(rt0 < 8 && rt0 != RT_DATA && rt0 != RT_XDAT) rt0 = 0 ;

  class0   = criteria[0] >> 8 ;                     // class from criteria
  if(class0 == 0) class0 = fp->rec_class ;          // if 0, use default class for file
  class0 &= (mask0 >> 8) ;                        // apply mask to get final low level record class match target

  for( ; index < fp->vdir_used ; index++ ){
    ventry = fp->vdir[index] ;
    meta = ventry->meta ;
    if( (rt0 != 0) && ( rt0 != (meta[0] & 0xFF) ) )     continue ;   // record type mismatch
    if( (class0 != 0) && ( class0 != (meta[0] >> 8) ) ) continue ;   // class mismatch
    // the first element of meta, mask, criteria is ignored, 
    nmeta = ventry->ml ;
//     if(lcrit > nmeta) continue ;                     // too many criteria, no match
    if(nmeta > lcrit) nmeta = lcrit ;
    if((*scan_match)(criteria+1, meta+1, mask ? mask+1 : NULL, lcrit-1, nmeta-1) == 1 ){   // do we have a match at position index ?
      slot = slot + index + 1 ;                                      // add record number (origin 1) to slot
      *wa = RSF_32_to_64(ventry->wa) ;                               // address of record in file
      *rl = RSF_32_to_64(ventry->rl) ;                               // record length
// fprintf(stderr,"RSF_Scan_directory slot = %12.12lx\n",slot) ;
      return slot ;                   // return key value containing file slot and record index
    }
  }
//   badkey = slot + fp->vdir_used + 1 ;   // DEBUG : falling through, return an index one beyond last record
  error = "no match found" ;
ERROR :
fprintf(stderr,"RSF_Scan_vdir ERROR : key = %16.16lx, len = %d,  %s\n", key0, lcrit, error) ;
  return badkey ;
}

// =================================  FIXED metadata size =================================

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

// add a new FIXED metadata length record entry into the file directory
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
  uint32_t rt0, class0 ;

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
    fprintf(stderr,"RSF_Scan_directory ERROR: metadata size <= 0 (%d)\n", nitems) ;
    return -1 ;          // invalid metadata size
  }

  scanpage = index >> DIR_PAGE_SHFT ;               // first page to scan
  index0   = index & DIR_PAGE_MASK ;                // scan from this position in first page

  rt0      = (criteria[0] & mask[0]) & 0xFF ;       // low level record type match target
  if(rt0 >= RT_DEL) rt0 = 0 ;                       // invalid record type
  // check for valid type for a data record (RT_DATA, RT_XDAT, 8->RT_DEL-1 are valid)
  if(rt0 < 8 && rt0 != RT_DATA && rt0 != RT_XDAT) rt0 = 0 ;
  class0   = criteria[0] >> 8 ;                     // class from criteria
  if(class0 == 0) class0 = fp->rec_class ;          // if 0, use default class for file
  class0 &= (mask[0] >> 8) ;                        // apply mask to get final low level record class match target
  for( ; scanpage < fp->dirpages ; scanpage++) {
    cur_page = fp->pagetable[scanpage] ;
    if(cur_page == NULL) {
      break ;
    }
// fprintf(stderr,"RSF_Scan_directory scanpage = %d, fp->dirpages = %d, index0 = %d; cur_page = %p, cur_page->nused = %d\n", scanpage, fp->dirpages, index0, cur_page, cur_page->nused) ;
    meta = cur_page->meta ;             // bottom of metadata for this page
    meta += nitems * index0 ;           // bump meta to reflect index0 (initial position)
    for(i = index0 ; i < cur_page->nused ; i++){

      // check 8 bit data record type to prevent further match ?
      // check 24 bit class mask here (from meta[0])
      if( (rt0 != 0) && ( rt0 != (meta[0] & 0xFF) ) ) goto NEXT ;      // record type mismatch
      if( (class0 != 0) && ( class0 != (meta[0] >> 8) ) ) goto NEXT ;  // class mismatch
      // if class mask not matching, DO NOT ATTEMPT firther match
      // if( (meta[0] & fp->class_mask & 0xFFFFFF) == 0 )
// fprintf(stderr,"RSF_Scan_directory mask = %8.8x, nitems = %d %d\n",mask[0], nitems, fp->meta_dim);
      // the first element of meta, mask, criteria is ignored, 
      // nitems -1 items get checked for a match
      if((*scan_match)(criteria+1, meta+1, mask+1, nitems-1, nitems-1) == 1 ){   // do we have a match at position i ?
        slot = slot + index + 1 ;       // add record number (origin 1) to slot
        *wa = cur_page->warl[i].wa ;    // position of record in file
        *rl = cur_page->warl[i].rl ;    // record length
// fprintf(stderr,"RSF_Scan_directory slot = %12.12lx\n",slot) ;
        return slot ;                   // return key value containing file slot and record index
      }
NEXT :
      index++ ;                         // next record
      meta += nitems ;                  // metadata for next record
    }  // for loop over index in page
    index0 = 0 ;                        // after first page, start from bottom of page
  }  // for loop over scanpage
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
// this function reads both FIXED and VARIABLE metadata directories
static int32_t RSF_Read_directory(RSF_File *fp){
  int32_t entries = 0 ;
  int32_t l_entries ;
  int32_t segments = 0 ;
  int32_t slot ;
  uint64_t size_seg, dir_off, vdir_off, dir_size, vdir_size, dir_size2 ;
  uint64_t wa, rl ;
  start_of_segment sos ;
  off_t off_seg ;
  ssize_t nc ;
  disk_directory *ddir = NULL ;
  disk_vdir *vdir = NULL ;
  disk_dir_entry *entry ;
  vdir_entry *ventry ;
  char *e ;
  int i ;
  uint64_t dir_entry_size ;
  uint32_t *meta ;
  uint32_t meta_dim ;
  char *errmsg = "" ;
  off_t wa_dir ;
  end_of_record eor ;

// fprintf(stderr,"read directory, file '%s', slot = %d\n",fp->name, RSF_Valid_file(fp)) ;
  if( ! (slot = RSF_Valid_file(fp)) ) return -1 ;           // something not O.K. with fp
  off_seg = 0 ;                                             // first segment at beginning of file

  while(1){                                                 // loop over segments
// fprintf(stderr,"segment offset = %ld\n",off_seg) ;
    lseek(fp->fd, off_seg , SEEK_SET) ;            // start of target segment

    nc = read(fp->fd, &sos, sizeof(start_of_segment)) ;     // try to read start of segment
    if(nc < sizeof(start_of_segment)) break ;               // end of file reached, done
    segments++ ;

    errmsg = "invalid start of record" ;
    if( RSF_Rl_sor(sos.head, RT_SOS) == 0) goto ERROR ;     // invalid sos record (wrong record type)
    size_seg = RSF_32_to_64(sos.sseg) ;                     // segment size
    if(size_seg == 0) break ;                               // open, compact, this is the last segment
    dir_off  = RSF_32_to_64(sos.dir) ;                      // offset of directory in segment
    vdir_off  = RSF_32_to_64(sos.vdir) ;                    // offset of vdir in segment
    dir_size = RSF_32_to_64(sos.dirs) ;                     // directory size from start of segment
    vdir_size = RSF_32_to_64(sos.vdirs) ;                   // vdir size from start of segment
fprintf(stderr,"RSF_Read_directory DEBUG: segment %d, at %lx, dir_off = %ld, dir_size = %ld", segments, off_seg, dir_off, dir_size) ;
fprintf(stderr,", vdir_off = %ld, vdir_size = %ld\n", vdir_off, vdir_size) ;
    // =======================   VARIABLE metadata length  =======================
    if(vdir_size > 0 && vdir_off > 0) {                     // non empty VARIABLE metadata segment
      fprintf(stderr,"RSF_Read_directory DEBUG: VARIABLE length directory detected size = %ld, off_seg = %lx\n", vdir_size, off_seg);
      l_entries = 0 ;
      vdir = (disk_vdir *) malloc(vdir_size) ;
      lseek(fp->fd, off_seg + vdir_off, SEEK_SET) ;
      nc = read(fp->fd, vdir, vdir_size) ;                   // read segment vdir
      e = (char *) &(vdir->entry[0]) ;
      for(i=0 ; i < vdir->entries_nused ; i++){
        ventry = (vdir_entry *) e ;
        wa = RSF_32_to_64(ventry->wa) + off_seg ;
        rl = RSF_32_to_64(ventry->rl) ;
        meta = &(ventry->meta[0]) ;
        RSF_Add_vdir_entry(fp, meta, ventry->ml, wa, rl) ;    // add entry into variable metadata directory
        e = e + sizeof(vdir_entry) + ventry->ml * sizeof(uint32_t) ;
      }
      if(vdir) free(vdir) ;                                 // free memory used to read segment vdir from file
      vdir = NULL ;                                         // to avoid a double free
      fprintf(stderr,"RSF_Read_directory DEBUG: found %d entries in segment %d\n", l_entries, segments-1) ;
    }
    // =======================   FIXED metadata length  =======================
    if(dir_size > 0 && dir_off > 0) {                       // non empty FIXED metadata segment
      fprintf(stderr,"RSF_Read_directory DEBUG: FIXED length directory detected size = %ld, off_seg = %lx\n", dir_size, off_seg);
      l_entries = 0 ;
      ddir = (disk_directory *) malloc(dir_size) ;          // allocate memory to read segment directory from file
      errmsg = "disk directory allocation failed" ;
      if(ddir == NULL) goto ERROR ;                         // malloc failed

      wa_dir = lseek(fp->fd, off_seg + dir_off, SEEK_SET) ; // seek to segment directory
//       lseek(fp->fd, -sizeof(end_of_record), SEEK_CUR) ;
//       read(fp->fd, &eor, sizeof(end_of_record)) ;
//       if(eor.rt == RT_VDIR) {
fprintf(stderr,"RSF_Read_directory DEBUG: segment directory wa = %lx, rl = %ld\n", wa_dir, dir_size) ;
      nc = read(fp->fd, ddir, dir_size) ;                   // read segment directory
      dir_size2 = RSF_32_to_64(ddir->sor.rl) ;              // directory size from record
      errmsg = "inconsistent directory sizes sor/dir" ;
      fprintf(stderr,"RSF_Read_directory DEBUG : segment %d,directory sizes %ld(sos) %ld(dir)\n", segments, dir_size, dir_size2) ;
      if(dir_size2 != dir_size) {
        goto ERROR ;                // inconsistent sizes
      }

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
//         RSF_Add_vdir_entry(fp, meta, meta_dim, wa, rl) ;    // add entry into variable metadata directory
        entries++ ;                                         // one more valid record
        l_entries++ ;                                       // one more record in this segment
        e += dir_entry_size ;                               // next directory entry
      }
      if(ddir) free(ddir) ;                                 // free memory used to read segment directory from file
      ddir = NULL ;                                         // to avoid a double free
      fprintf(stderr,"RSF_Read_directory DEBUG: found %d entries in segment %d\n", l_entries, segments-1) ;
    }
    off_seg += size_seg ;                                   // offset of the start of the next segment
  }  // while(1)
fprintf(stderr,"RSF_Read_directory DEBUG: found %d entries in %d segment directories\n", entries, segments) ;
  fp->dir_read = entries ;
  return entries ;                                          // return number of records found in segment directories

ERROR:
  fprintf(stderr,"RSF_Read_directory ERROR %s, %d entries, %d segments\n", errmsg, entries, segments);
  if(ddir != NULL) free(ddir) ;
  return -1 ;  
}

// =======================   FIXED metadata length  =======================
// write file directory to storage device from memory directory (fixed length metadata)
// it is NOT assumed that the file is correctly positioned
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
  ddir->sor.rlm = fp->meta_dim ;
  RSF_64_to_32(ddir->sor.rl, dir_rec_size) ;
  ddir->sor.zr = ZR_SOR ;

  ddir->meta_dim = fp->meta_dim ;                   // directory record body
  ddir->entries_nused = fp->dir_used - fp->dir_read ;

  eorp->rt = RT_DIR ;                               // end or record
  eorp->rlm = fp->meta_dim ;
  RSF_64_to_32(eorp->rl, dir_rec_size) ;
  eorp->zr = ZR_EOR ;

  // fill ddir->entry
  e = (char *) &(ddir->entry[0]) ;                  // start of directory metadata portion
// fprintf(stderr,"p = %16p, eorp = %16p, e = %16p\n", p, eorp, e);
// do not start at entry # 1, but entry # fp->dir_read + 1 (only write entries from "active" segment)
// when "fusing" segments, fp->dir_read gets reset to 0
fprintf(stderr,"RSF_Write_directory DEBUG : skipping %d records, segment base = %lx\n", fp->dir_read, fp->seg_base) ;
  for(i = fp->dir_read + 1 ; i <= fp->dir_used ; i++){             // fill from in core directory
    entry = (disk_dir_entry *) e ;
    // get wa, rl, meta for entry i from in core directory
    RSF_Get_dir_entry(fp, i, &wa64, &rl64, entry->meta) ;
    wa64 -= fp->seg_base ;
    RSF_64_to_32(entry->wa, wa64) ;
    RSF_64_to_32(entry->rl, rl64) ;
    e += dir_entry_size ;
// fprintf(stderr,"RSF_Write_directory DEBUG : WA = %lx, rl = %lx (%ld)\n", wa64, rl64, dir_entry_size) ;
  }
  if(fp->next_write != fp->cur_pos)                 // set position after last write (SOS or DATA record) if not there yet
    fp->cur_pos = lseek(fp->fd, fp->next_write , SEEK_SET) ;
fprintf(stderr,"RSF_Write_directory DEBUG : directory written at = %lx\n", fp->next_write) ;
  n_written = write(fp->fd, ddir, dir_rec_size) ;   // write directory record
  if(n_written != dir_rec_size) dir_rec_size = 0 ;  // everything written ?
  fp->next_write += n_written ;                     // update last write position
  fp->cur_pos = fp->next_write ;                    // update file current position
  fp->last_op = OP_WRITE ;                          // last operation was a write
// fprintf(stderr,"RSF_Write_directory: records = %d\n", ddir->entries_nused);
  free(p) ;
  return dir_rec_size ;
}

// =======================   VARIABLE metadata length  =======================
static size_t RSF_Vdir_record_size(RSF_File *fp){
  int i ;
  size_t dir_rec_size ;
  dir_rec_size = sizeof(disk_vdir) + sizeof(end_of_record) ;                      // fixed part + end_of_record
  for(i = fp->dir_read ; i < fp->vdir_used ; i++){                                // add entries (wa/rl/ml/metadata)
    dir_rec_size += ( sizeof(vdir_entry) + fp->vdir[i]->ml * sizeof(uint32_t) );  // fixed part + metadata
  }
fprintf(stderr, "RSF_Vdir_record_size DEBUG : dir rec size = %ld %ld\n", dir_rec_size, fp->vdir_size);
  return dir_rec_size ;
}
// write file directory to storage device from memory directory (variable length metadata)
// it is NOT assumed that the file is correctly positioned
static int64_t RSF_Write_vdir(RSF_File *fp){
  int32_t slot ;
  vdir_entry *entry ;
  disk_vdir *vdir ;
  end_of_record *eorp ;
  size_t dir_entry_size ;
  size_t dir_rec_size ;
  ssize_t n_written ;
  uint8_t *p = NULL ;
  uint8_t *e ;
  int i ;
  uint64_t wa64 ;
  void *where ;

  if( ! (slot = RSF_Valid_file(fp)) ) return 0 ;             // something wrong with fp
fprintf(stderr,"RSF_Write_vdir DEBUG : fp->vdir = %p\n", fp->vdir) ;
  if(fp->vdir == NULL) return 0 ;
  dir_rec_size = RSF_Vdir_record_size(fp) ;
//   dir_rec_size = sizeof(disk_vdir) + sizeof(end_of_record) ;                      // fixed part + end_of_record
//   for(i = fp->dir_read ; i < fp->vdir_used ; i++){                                 // add entries (wa/rl/ml/metadata)
//     where = fp->vdir[i] ;
// // fprintf(stderr,"RSF_Write_vdir DEBUG : record %d, where = %p, ml = %d\n", i, where, fp->vdir[i]->ml) ;
//     dir_rec_size += ( sizeof(vdir_entry) + fp->vdir[i]->ml * sizeof(uint32_t) );  // fixed part + metadata
//   }
fprintf(stderr,"RSF_Write_vdir DEBUG : slot = %d\n", slot)  ;
  if( ( p = malloc(dir_rec_size) ) == NULL ) return 0 ;      // allocation failed
fprintf(stderr,"RSF_Write_vdir DEBUG : p = %p\n", p) ;
  vdir = (disk_vdir *) p ;
  eorp = (end_of_record *) (p + dir_rec_size - sizeof(end_of_record)) ;  // point to eor at end of record

  vdir->sor.rt = RT_VDIR ;                           // adjust start of record
  vdir->sor.rlm = 1 ;                                // indicates variable length metadata
  RSF_64_to_32(vdir->sor.rl, dir_rec_size) ;         // record length
  vdir->sor.zr = ZR_SOR ;                            // SOR marker

  vdir->meta_dim = 1 ;                               // indicates variable length metadata
  vdir->entries_nused = fp->vdir_used - fp->dir_read ;

  eorp->rt = RT_VDIR ;                               // adjust end or record
  eorp->rlm = 1 ;                                    // indicates variable length metadata
  RSF_64_to_32(eorp->rl, dir_rec_size) ;             // record length
  eorp->zr = ZR_EOR ;                                // EOR marker

  e = (uint8_t *) &(vdir->entry[0]) ;                  // start of directory metadata portion

// do not start at entry # 0, but entry # fp->dir_read (only write entries from "active" segment)
// when "fusing" segments, fp->dir_read will be reset to 0
fprintf(stderr,"RSF_Write_vdir DEBUG : skipping %d records, segment base = %lx\n", fp->dir_read, fp->seg_base) ;
fprintf(stderr,"RSF_Write_vdir DEBUG : dir_rec_size = %ld ", dir_rec_size) ;
fprintf(stderr,"RSF_Write_vdir DEBUG : dir_read = %d, vdir_used = %d\n", fp->dir_read, fp->vdir_used) ;
// return 0 ;
  for(i = fp->dir_read ; i < fp->vdir_used ; i++){             // fill from in core directory
    entry = (vdir_entry *) e ;
    dir_entry_size = sizeof(vdir_entry) + fp->vdir[i]->ml * sizeof(uint32_t) ;
    memcpy(entry, fp->vdir[i], dir_entry_size) ;   // copy entry from memory directory
    wa64 = RSF_32_to_64(entry->wa) ;               // adjust wa64 (file address -> offset in segment)
    wa64 -= fp->seg_base ;
    RSF_64_to_32(entry->wa, wa64) ;
    e += dir_entry_size ;
  }
// goto END ;  // DEBUG, do not write to disk

  if(fp->next_write != fp->cur_pos)                 // set position after last write (SOS or DATA record) if not there yet
    fp->cur_pos = lseek(fp->fd, fp->next_write , SEEK_SET) ;
fprintf(stderr,"RSF_Write_vdir DEBUG : vdir written at = %lx\n", fp->next_write) ;
  n_written = write(fp->fd, vdir, dir_rec_size) ;   // write directory record
//   if(n_written != dir_rec_size) dir_rec_size = 0 ;  // everything written ?
  fp->next_write += n_written ;                     // update last write position
  fp->cur_pos = fp->next_write ;                    // update file current position
  fp->last_op = OP_WRITE ;                          // last operation was a write
fprintf(stderr,"RSF_Write_vdir DEBUG : fp->next_write = %lx, vdir record size = %ld \n", fp->next_write, dir_rec_size) ;
END:
  if(p) free(p) ;
  return dir_rec_size ;
}


// =================================  user callable rsf file functions =================================

// match criteria and meta where mask has bits set to 1
// where mask has bits set to 0, a don't care condition is assumed
// if mask == NULL, it is not used
// criteria and mask have the same dimension : ncrit
// meta dimension nmeta my be larger than ncrit
// ncrit > nmeta is undefined and considered as a NO MATCH for now
// returns 0 in case of no match, 1 otherwise
int32_t RSF_Default_match(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int ncrit, int nmeta)
{
  int i ;
  if(ncrit > nmeta) return 0;  // too many criteria, no match
  if(mask != NULL) {
    for(i = 0 ; i < ncrit ; i++){
      if( (criteria[i] & mask[i]) != (meta[i] & mask[i]) ) {
        fprintf(stderr,"DEBUG: rsf_default_match, MISMATCH at %d, criteria = %8.8x, meta = %8.8x, mask = %8.8x, ncrit = %d, nmeta = %d\n",
                i, criteria[i], meta[i], mask[i], ncrit, nmeta) ;
        return 0 ;  // mismatch, no need to go any further
      }
    }
  }else{
    for(i = 0 ; i < ncrit ; i++){
      if( criteria[i] != meta[i] ) return 0 ;  // mismatch, no need to go any further
    }
  }
//   fprintf(stderr,"DEBUG: rsf_default_match, nitems = %d, MATCH\n", nitems);
  return 1 ;   // if we get here, we have a match
}

// same as RSF_Default_match but ignores mask
int32_t RSF_Base_match(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int ncrit, int nmeta)
{
  int i ;
  if(ncrit > nmeta) return 0;  // too many criteria, no match
//   fprintf(stderr,"DEBUG: calling rsf_base_match, ncrit = %d\n", ncrit);
  for(i = 0 ; i < ncrit ; i++){
    if( criteria[i] != meta[i] ) return 0 ;  // mismatch, no need to go any further
  }
  return 1 ;   // if we get here, we have a match
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
// and up to max_meta 32 bit metadata items (max_meta == 0 means use fp->meta_dim)
// (in that case, the caller is responsible for releasing (free) the allocated space when no longer needed)
// or
// the caller has the option to supply a memory array (t != NULL) of size szt bytes
// if szt == 0, it must be a previously created "record" large enough to accomodate max_data / max_meta
//
// this record structure will be used by RSF_Get, RSF_Put
//
// this record is reusable as long as the allocated size can accomodate max_data data bytes of payload
// this is why the allocated size is kept in the struct
// if the szt field in the struct is negative, the memory was not allocated by RSF_New_record
RSF_record *RSF_New_record(RSF_handle h, int32_t max_meta, size_t max_data, void *t, int64_t szt){
  RSF_File *fp = (RSF_File *) h.p ;
  size_t record_size ;
  RSF_record *r ;
  void *p ;
  start_of_record *sor ;
  end_of_record   *eor ;

  if( ! RSF_Valid_file(fp) ) return NULL ;
  // calculate space needed for the requested "record"
  if(max_meta < fp->meta_dim) max_meta = fp->meta_dim ;
  record_size = sizeof(start_of_record) +               // start of record marker
                sizeof(uint32_t) * max_meta +           // metadata size in bytes
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

  }else{                                                // create a new record

    p = malloc( record_size + sizeof(RSF_record) ) ;    // allocated record + overhead
    r = (RSF_record *) p ;                              // pointer to record structure
    if(p == NULL) return NULL ;                         // malloc failed 
    r->rsz = record_size + sizeof(RSF_record) ;         // allocated memory size (positive, as it is allocated here)

  }  // if(t != NULL)

  p += sizeof(RSF_record) ;   // skip overhead. p now points to data record part (SOR)

  sor = (start_of_record *) p ;
  r->sor  = sor ;
  sor->zr = ZR_SOR ; sor->rt = RT_DATA ; sor->rlm = max_meta ; RSF_64_to_32(sor->rl, record_size) ; // provisional sor, assuming full record

  eor = (end_of_record *) (p + record_size - sizeof(end_of_record)) ;
  r->eor  = eor ;
  eor->zr = ZR_EOR ; eor->rt = RT_DATA ; eor->rlm = max_meta ; RSF_64_to_32(eor->rl, record_size) ; // provisional eor, assuming full record

  r->meta_size = max_meta ;                          // metadata size in 32 bit units (metadata filling is not tracked)
  r->meta = (uint32_t *) (p + sizeof(start_of_record)) ;                                                // points to metadata
  bzero(r->meta, sizeof(uint32_t) * max_meta) ;      // set metadata to 0
  r->max_data  = max_data ;                          // max data payload for this record
  r->data_size = 0 ;                                 // no data in record yet
  r->data = (void *)  (p + sizeof(start_of_record) + sizeof(uint32_t) *  max_meta) ;                    // points to data payload
  // r-> rsz already set, > 0 if allocated by RSF_New_record, < 0 otherwise

  return r ; // return address of allocated space
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

// current size of data payload in record allocated by RSF_New_record
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

// adjust record created with RSF_New_record (make it ready to write)
// return size of adjusted record (amount to write)
size_t RSF_Adjust_data_record(RSF_handle h, RSF_record *r, size_t data_size){
  RSF_File *fp = (RSF_File *) h.p ;
  start_of_record *sor ;
  end_of_record   *eor ;
  size_t new_size ;

  if( ! RSF_Valid_file(fp) ) return 0L ;
fprintf(stderr,"RSF_Adjust_data_record DEBUG : data_size = %ld\n", data_size) ;
  if(data_size == 0) data_size = r->data_size ;    // get data size from record
  if(data_size > r->max_data) return 0L ;          // not enough space

  new_size = sizeof(start_of_record) +             // sor
             sizeof(uint32_t) * r->meta_size +     // metadata
             data_size +                           // data
             sizeof(end_of_record) ;               // eor
//   sor = (start_of_record *) record ;
  sor = r->sor ;
  eor = r->sor + new_size - sizeof(end_of_record) ;

  // adjust eor and sor to reflect record contents
  sor->zr = ZR_SOR ; sor->rt = RT_DATA ; sor->rlm = r->meta_size ; RSF_64_to_32(sor->rl, new_size) ;
  eor->zr = ZR_EOR ; eor->rt = RT_DATA ; eor->rlm = r->meta_size ; RSF_64_to_32(eor->rl, new_size) ;

  return new_size ;   // adjusted size
}

// write data record to file
// meta is a pointer to record metadata
// meta[0] is not really part of the record metadata, it is used to pass optional extra record type information
// meta_size is the size (in 32 bit elements) of the metadata (must be >= file metadata dimension)
// data is a pointer to record data
// data_size is the size (in bytes) of the data portion
// if meta is NULL, data is a pointer to a pre allocated record ( RSF_record )
// RSF_Adjust_data_record may have to be called
// lower 16 bits of meta_size : length of metadata written to disk
// upper 16 bits of meta_size : length of metadata written into vdir (0 means same as disk)
// written into vdir <= written to disk
int64_t RSF_Put_data(RSF_handle h, uint32_t *meta, uint32_t meta_size, void *data, size_t data_size){
  RSF_File *fp = (RSF_File *) h.p ;
  uint64_t record_size, directory_record_size, total_size, needed ;
  int64_t slot ;
  start_of_record sor = SOR ;      // start of data record
  end_of_record   eor = EOR ;      // end of data record
  RSF_record *record = NULL ;
  ssize_t nc ;
  uint32_t meta0, rt0, class0 ;
  int32_t vdir_meta = (meta_size >> 16) ;          // keep the upper 16 bits for future reference
//   int status ;

  if( ! RSF_Valid_file(fp) ) goto ERROR ;          // something not O.K. with fp

  if( (fp->mode & RSF_RW) != RSF_RW ) goto ERROR ; // file not open in write mode
  if( fp->next_write <= 0) goto ERROR ;            // next_write address is not set
  meta_size &= 0xFFFF ;                            // only keep the lower 16 bits
  if(meta_size == 0) meta_size = fp->meta_dim ;    // get default metadata length from file structure
  if(meta_size < fp->meta_dim) goto ERROR ;        // metadata too small, must be at least fp->meta_dim

  record_size = sizeof(start_of_record) +          // start of record
                meta_size * sizeof(uint32_t) +     // metadata
                data_size +                        // data
                sizeof(end_of_record) ;            // end of record
// fprintf(stderr,"record size = %ld, %ld, %ld, %ld, %ld\n",
//        record_size,sizeof(start_of_record),fp->meta_dim * sizeof(uint32_t), data_size, sizeof(end_of_record)) ;
  // write record if enough room left in segment (always O.K. if compact segment)
  if(fp->seg_max > 0){                                                 // write into a sparse segment
    directory_record_size = RSF_Disk_dir_size(fp) +                    // space for currrent records
                            fp->vdir_size +                            // space for vdir
                            RSF_Disk_dir_entry_size(fp) ;              // size of entry for this record
    needed = fp->next_write + record_size +                       // record
             directory_record_size + sizeof(end_of_segment) +     // directory + end of compact segment
             sizeof(start_of_segment) + sizeof(end_of_segment) ;  // sparse segment at end (sos + eos)
    if( needed > fp->seg_max + fp->seg_base) {
      fprintf(stderr,"RSF_Put_data ERROR : sparse segment OVERFLOW\n");
      goto ERROR ; // not enough room in segment to accomodate record
    }
  }
  lseek(fp->fd , fp->next_write , SEEK_SET) ; // position file at fp->next_write
// fprintf(stderr,"RSF_Put_data DEBUG : write at %lx\n", fp->next_write) ;
  // write record 
  if(meta == NULL){                                                 // pre allocated record structure

    record = (RSF_record *) data ;
    total_size = RSF_Adjust_data_record(h, record, data_size) ;     // adjust to actual data size
    // make meta_size consistent with record->metasize
    meta_size = record->meta_size ;                                 // vdir_meta will come from the upper 16 bits of the meta_size argument
    meta = record->meta ;                                           // set meta to address of metadata from record
    meta0 = meta[0] ;                                               // save meta[0]
    rt0 = meta0 & 0xFF ;                                            // lower 8 bits
    class0 = meta0 >> 8 ;                                           // upper 24 bits
    if(rt0 != RT_XDAT)
      if(rt0 < 8 || rt0 >= RT_DEL) rt0 = RT_DATA ;                  // RT_DATA (normal data record) by default
    if(class0 == 0) class0 = (fp->rec_class & 0xFFFFFF) ;           // fp->rec_class if unspecified
    meta[0] = (class0 << 8) | ( rt0 & 0xFF) ;                       // RT + record class
    ((start_of_record *) record->sor)->rt = rt0 ;                   // alter record type in start_of_record
    ((end_of_record *)   record->eor)->rt = rt0 ;                   // alter record type in end_of_record
    nc = write(fp->fd, record->sor, total_size) ;                   // write record structure to disk

  }else{

    meta0 = meta[0] ;                                               // save meta[0]
    rt0 = meta0 & 0xFF ;                                            // lower 8 bits
    class0 = meta0 >> 8 ;                                           // upper 24 bits
    if(rt0 != RT_XDAT)
      if(rt0 < 8 || rt0 >= RT_DEL) rt0 = RT_DATA ;                  // RT_DATA (normal data record) by default
    if(class0 == 0) class0 = (fp->rec_class & 0xFFFFFF) ;           // fp->rec_class if unspecified
    meta[0] = (class0 << 8) | ( rt0 & 0xFF) ;                       // RT + record class
    sor.rlm = meta_size ;
    sor.rt = rt0 ;                                                  // alter record type in start_of_record
    RSF_64_to_32(sor.rl, record_size) ;
    nc = write(fp->fd, &sor, sizeof(start_of_record)) ;             // write start of record
    nc = write(fp->fd, meta, meta_size * sizeof(uint32_t)) ;        // write metadata
    nc = write(fp->fd, data, data_size) ;                           // write data
    eor.rlm = meta_size ;
    eor.rt = rt0 ;                                                  // alter record type in end_of_record
    RSF_64_to_32(eor.rl, record_size) ;
    nc = write(fp->fd, &eor, sizeof(end_of_record)) ;               // write end_of_record
  }

  // update directory in memory
  slot = RSF_Add_directory_entry(fp, meta, fp->next_write, record_size) ;
  // what is stored in vdir may be shorter than meta_size
  if(vdir_meta == 0) vdir_meta = meta_size & 0xFFFF ;
  if(vdir_meta > (meta_size & 0xFFFF)) vdir_meta = meta_size & 0xFFFF ;
  slot = RSF_Add_vdir_entry(fp, meta, vdir_meta, fp->next_write, record_size) ;
//   RSF_Add_vdir_entry(fp, meta, meta_size, fp->next_write, record_size) ;
  meta[0] = meta0 ;                                                 // restore meta[0] to original value

  fp->next_write += record_size ;         // update fp->next_write and fp->cur_pos
  fp->cur_pos = fp->next_write ;
  fp->last_op = OP_WRITE ;                // last operation was write
  fp->nwritten += 1 ;                     // update unmber of writes
  // return slot/index for record (0 in case of error)
  return slot ;
ERROR :
  return 0 ;
}

// retrieve file contained in RSF file ans restore it under the name alias
// also get associated metadata pointer and metadata length
// index : returned by RSF_Lookup when finding a record
// if alias is NULL, the file's own name will be used
// in case of success, index is returned,
// in case of error, -1 is returned, meta and meta_size are set to NULL and 0 respectively
int64_t RSF_Get_file(RSF_handle h, int64_t key, char *alias, uint32_t **meta, uint32_t *meta_size){
  RSF_File *fp = (RSF_File *) h.p ;
  start_of_record sor ;
  end_of_record   eor ;
  uint8_t copy_buf[1024*16] ;
  uint64_t wa, rl ;
  uint32_t *vmeta, *tempm ;
  int64_t slot ;
  int32_t ml ;
  char *temp0, *temp, *filename ;
  uint64_t file_size ;
  uint32_t nmeta ;
  int i ;
  int fd ;
  off_t offset ;
  ssize_t nread, nwritten, nc, toread, towrite ;

  *meta = NULL ;
  *meta_size = 0 ;
  if( ! (slot = RSF_Valid_file(fp)) ) goto ERROR ; // something wrong with fp

  ml = RSF_Get_vdir_entry(fp, key, &wa, &rl, &vmeta) ;
  if(ml == -1) goto ERROR ;                        // key not found

  temp0 = (char *) vmeta ;                               // start of metadata
  temp  = temp0 + ml * sizeof(uint32_t) ;                // end of metadata
  while((temp[ 0] == '\0') && (temp > temp0)) temp -- ;
  while((temp[-1] != '\0') && (temp > temp0)) temp -- ;
  temp0 = temp -1 ;
  tempm = (uint32_t *) temp0 ;
  nmeta = tempm - vmeta ;
  file_size = RSF_32_to_64(vmeta + nmeta - 2) ;
  filename = (alias != NULL) ? alias : temp ;
//   fprintf(stderr,"RSF_Get_file DEBUG : filename = '%s' [%ld] retrieved as '%s'\n", temp, file_size, filename) ;

  fp->last_op = OP_READ ;

  offset = wa ;
  lseek(fp->fd, offset, SEEK_SET) ;                               // position at start of record
  read(fp->fd, &sor, sizeof(start_of_record)) ;                   // read start of record
//   fprintf(stderr,"RSF_Get_file DEBUG : rlm = %d\n", sor.rlm);
  lseek(fp->fd, sor.rlm * sizeof(uint32_t), SEEK_CUR) ;           // skip metadata
  nread = 0 ; nwritten = 0 ;
  fd = open(filename, O_WRONLY | O_CREAT | O_EXCL, 0777) ;
  if(fd == -1) {
    fprintf(stderr,"RSF_Get_file ERROR : failed to create file '%s'\n", filename) ;
    goto ERROR ;
  }else{
//     fprintf(stderr,"RSF_Get_file DEBUG : successfully created file '%s'\n", filename) ;
  }
  towrite = file_size ;
  while(towrite > 0) {
    toread = (towrite < sizeof(copy_buf)) ? towrite : sizeof(copy_buf) ;
    towrite -= toread ;
    nread += read(fp->fd, copy_buf, toread) ;
    nwritten += write(fd, copy_buf, toread) ;
//     fprintf(stderr,".") ;
  }
//   fprintf(stderr,"\n");
//   fprintf(stderr,"RSF_Get_file DEBUG : read %ld, written %ld \n", nread, nwritten) ;
  close(fd) ;
  if(nread != nwritten) goto ERROR ;
  fprintf(stderr,"RSF_Get_file INFO : successfully copied image of '%s' into '%s'\n", temp, filename) ;

  *meta = vmeta ;       // address of directory metadata
  *meta_size = ml ;     // directory metadata length
  return key ;
ERROR :
  return -1 ;
}

// store an external file into a RSF file
// the external file will be opened as Read-Only
// meta and meta_size have the same use as for RSF_Put_file
int64_t RSF_Put_file(RSF_handle h, char *filename, uint32_t *meta, uint32_t meta_size){
  RSF_File *fp = (RSF_File *) h.p ;
  start_of_record sor = SOR ;      // start of data record
  end_of_record   eor = EOR ;      // end of data record
  int fd ;
  int64_t slot = -1 ;              // precondition for error
  int64_t index = -1 ;             // precondition for error
  uint64_t record_size ;           // sor + metadata + file + eor
  int32_t vdir_meta = (meta_size >> 16) ;          // keep the upper 16 bits for future use
  off_t file_size0, file_size2 ;
  size_t name_len = 1024 ;         // maximum allowed name length
  ssize_t nc, nread, nwritten ;
  int i ;
  uint32_t meta0 ;
  int extra_meta ;
  struct mmeta{
  uint32_t rl[2] ;
  char name[] ;
  } *fmeta ;
  uint32_t *dir_meta, *file_meta ;
  uint64_t needed ;
  uint8_t copy_buf[1024*1024] ;

  dir_meta = NULL ; file_meta = NULL ; fmeta = NULL ;

  if( ! (slot = RSF_Valid_file(fp)) ) goto ERROR ; // something wrong with fp
  slot <<= 32 ;
  if( (fp->mode & RSF_RW) != RSF_RW ) goto ERROR ; // file not open in write mode
  if( fp->next_write <= 0) goto ERROR ;            // next_write address is not set

  meta_size &= 0xFFFF ;                            // only keep the lower 16 bits
  fd = open(filename, O_RDONLY) ;
fprintf(stderr,"RSF_Put_file DEBUG : file = '%s', fd = %d\n", filename, fd) ;
  if(fd < 0) goto ERROR ;
  file_size0 = lseek(fd, file_size2, SEEK_END) ;   // get file size
  file_size2 = ((file_size0 + 3) & (~0x3)) ;       // file size rounded up to a multiple of 4
  name_len = strnlen(filename, name_len) ;         // length of file name

  extra_meta = 2 +                                 // file size (2 x 32 bit integers)
               ((name_len + 5) >> 2) ;             // name length + 2 rounded up to a multiple of 4 characters
  fmeta = (struct mmeta *) calloc(extra_meta, sizeof(uint32_t)) ;  // extra metadata
  RSF_64_to_32(fmeta->rl, file_size0) ;                     // record size
  fmeta->name[0] = '\0' ;
  for(i=0 ; i<name_len ; i++) fmeta->name[i+1] = filename[i] ;  // copy filename into fmeta->name

  if(vdir_meta == 0) vdir_meta = meta_size ;
  if(vdir_meta > meta_size) vdir_meta = meta_size ;
  dir_meta = (uint32_t *) calloc(vdir_meta + extra_meta, sizeof(uint32_t)) ; // metadata for in memory directory
  memcpy(dir_meta, meta, vdir_meta * sizeof(uint32_t)) ;        // copy meta[0 -> vdir_meta-1] into dir_meta
  memcpy(dir_meta + vdir_meta, (uint32_t *) fmeta, extra_meta * sizeof(uint32_t)) ;    // copy extra metadata into dir_meta

  file_meta = (uint32_t *) calloc(meta_size + extra_meta, sizeof(uint32_t)) ; // metadata for record in file
  memcpy(file_meta, meta, meta_size * sizeof(uint32_t)) ;        // copy meta[0 -> meta_size-1] into file_meta
  memcpy(file_meta + meta_size, (uint32_t *) fmeta, extra_meta * sizeof(uint32_t)) ; // copy extra metadata into file_meta

  record_size = sizeof(start_of_record) + 
                meta_size * sizeof(uint32_t) +   // file record metadata size
                extra_meta * sizeof(uint32_t) +  // extra metadata (unrounded file size and file name)
                file_size2 +                     // file size rounded up to a multiple of 4
                sizeof(end_of_record) ;

  if(fp->seg_max > 0){                             // write into a sparse segment not allowed for now
    needed = fp->next_write + 
             record_size +                         // this record
             fp->vdir_size +                       // current vdir directory size
             RSF_Disk_dir_size(fp) +               // fixed metadata directory size
//              RSF_Vdir_record_size(fp) +            // current vdir directory
             sizeof(vdir_entry) + (meta_size + extra_meta) * sizeof(uint32_t) +  // space for this entry
             sizeof(end_of_segment) +              // end of fixed segment
             sizeof(start_of_segment) + sizeof(end_of_segment) ;  // new sparse segment
    if(needed > fp->seg_max + fp->seg_base) {
      fprintf(stderr,"RSF_Put_file ERROR : sparse segment OVERFLOW\n") ;
      goto ERROR ;
    }
  }

  sor.rt = RT_FILE ;
  sor.rlm = meta_size + extra_meta ;
  RSF_64_to_32(sor.rl, record_size) ;
fprintf(stderr,"RSF_Put_file DEBUG : name = '%s', size = %ld(%ld), vdir_meta = %d, extra_meta = %d, meta_size = %d, record_size = %ld\n", 
        filename, file_size0, file_size2, vdir_meta, extra_meta, meta_size, record_size);
// close(fd) ;
// goto ERROR ;
  nc = write(fp->fd, &sor, sizeof(start_of_record)) ;

  meta0 = RT_FILE + (RT_FILE_CLASS << 8) ;          // record type and class
  file_meta[0] = meta0 ;
  nc = write(fp->fd, file_meta, (meta_size + extra_meta) * sizeof(uint32_t)) ;

//   nc = RSF_Copy_file(fp->fd, fd, file_size) ;
//   lseek(fp->fd, file_size2, SEEK_CUR) ;   // will be replaced by RSF_Copy_file
  nread = nwritten = 0 ;
  lseek(fd, 0l, SEEK_SET) ;                  // rewind file before copying it
  nc = read(fd, copy_buf, sizeof(copy_buf) );
  while(nc > 0){
    nread += nc ;
    if(nread > file_size2) break ;           // file being copied is changing size
    nwritten += write(fp->fd, copy_buf, nc) ;
    nc = read(fd, copy_buf, sizeof(copy_buf) );
  }
  if(nwritten < file_size2){
    write(fp->fd, copy_buf, file_size2 - nwritten) ;  // pad
fprintf(stderr,"RSF_Put_file DEBUG : read %ld bytes, wrote %ld bytes, padded with %ld bytes\n", nread, nwritten, file_size2 - nwritten) ;
  }

  eor.rt = RT_FILE ;
  RSF_64_to_32(eor.rl, record_size) ;
  eor.rlm = meta_size + extra_meta ;
  nc = write(fp->fd, &eor, sizeof(start_of_record)) ;

  close(fd) ;

  dir_meta[0] = meta0 ;
  index = RSF_Add_vdir_entry(fp, (uint32_t *) dir_meta, vdir_meta + extra_meta, fp->next_write, record_size) ; // add to directory

  fp->next_write  = lseek(fp->fd, 0l, SEEK_CUR) ;
  fp->cur_pos = fp->next_write ;
  fp->last_op = OP_WRITE ;                // last operation was write
  fp->nwritten += 1 ;                     // update unmber of writes

ERROR:
  if(fmeta) free(fmeta) ;
  if(file_meta) free(file_meta) ;
  if(dir_meta) free(dir_meta) ;
  return index ;
}

// write pre allocated data record to file
// record is a pointer from RSF_New_record
// data_size is the actual data size
int64_t RSF_Put_record(RSF_handle h, RSF_record *record, size_t data_size){

  return RSF_Put_data(h, NULL, 0, record, data_size) ;
}

// get key to record from file fp, matching criteria & mask, starting at key0 (slot/index)
// key0 <= 0 means start from beginning of file
int64_t RSF_Lookup(RSF_handle h, int64_t key0, uint32_t *criteria, uint32_t *mask){
  RSF_File *fp = (RSF_File *) h.p ;
  uint64_t wa, rl ;
// fprintf(stderr,"in RSF_Lookup key = %16.16lx, crit = %8.8x @%p, mask = %8.8x @%p\n", key0, criteria[0], criteria, mask[0], mask) ;
// for(i=0 ; i<6 ; i++) fprintf(stderr,"%8.8x ",mask[i]) ; fprintf(stderr,"\n") ;
  return RSF_Scan_directory(fp, key0, criteria, mask, &wa, &rl) ;  // wa and rl not sent back to caller
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
//   int64_t payload ;
  RSF_record *record = NULL;
  ssize_t nc ;
//   start_of_record *sor ;
  void *p ;
  int rlm ;

  if( ! RSF_Valid_file(fp) ) goto ERROR ;             // something wrong with fp

  indx = key & 0x7FFFFFFF ;                           // get record numer from key
  indx-- ;                                            // set indx to origin 0 ;
  page  = indx >> DIR_PAGE_SHFT ;                     // directory page number
  indx &= DIR_PAGE_MASK ;                             // offset in directory page

  wa = (fp->pagetable[page])->warl[indx].wa ;         // record position in file
  recsize = (fp->pagetable[page])->warl[indx].rl ;    // record size
  // size of the data payload (record size - overhead)
//   payload = recsize - 
//             sizeof(start_of_record) -                 // sor marker
//             fp->meta_dim * sizeof(uint32_t) -         // metadata
//             sizeof(end_of_record) ;                   // eor marker
//   record = RSF_New_record(h, payload, fp->meta_dim, NULL, 0l) ;  // allocate a new record structure

  if( (p = malloc(recsize + sizeof(RSF_record))) ) {
    record = (RSF_record *) p ;
    offset = lseek(fp->fd, offset = wa, SEEK_SET) ;   // start of record in file
    nc = read(fp->fd, record->data, recsize) ;        // read record from file
    fp->cur_pos = offset + nc ;                       // current position set after data read
    fp->last_op = OP_READ ;                           // last operation was a read operation
    if(nc != recsize) goto ERROR ;
    // TODO : adjust the record struct using data from record (especially meta_size)
    record->rsz = recsize ;
    record->sor = p + sizeof(RSF_record) ;
    rlm = ((start_of_record *)record->sor)->rlm ;
//     sor = (start_of_record *) record->sor ;
    record->meta_size = rlm ;
    record->meta = record->sor + sizeof(start_of_record) ;
    record->data_size = record->eor - record->data ;
    record->data = record->eor + sizeof(start_of_record) + sizeof(uint32_t) * rlm ;
    record->max_data = record->data_size ;
    record->eor = p + recsize - sizeof(end_of_record) ;
  }

  return record ;

ERROR:
  if(record) free(record) ;
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

#if 0
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
#endif

int32_t RSF_Valid_handle(RSF_handle h){
  RSF_File *fp = (RSF_File *) h.p ;
  if( RSF_Valid_file(fp) ) return 1 ;
  return 0 ;
}

static int32_t RSF_File_lock(int fd, int lock){
  struct flock file_lock ;
  int status ;

  file_lock.l_whence = SEEK_SET ;                // locked area is at beginning of file
  file_lock.l_start = 0 ;                        // base of segment to be locked
  file_lock.l_len = 0 ;                          // lock entire file

  if(lock){
    file_lock.l_type = F_WRLCK ;                 // exclusive lock
    status = fcntl(fd, F_SETLK, &file_lock) ;    // do not wait
    while ( status != 0) {
      status = fcntl(fd, F_GETLK, &file_lock) ;  // find which process holds the lock
      fprintf(stderr,"RSF_File_lock DEBUG : %d blocked by pid = %d\n", getpid(), file_lock.l_pid) ;
      usleep(1000) ;                             // wait 1 millisecond
      status = fcntl(fd, F_SETLK, &file_lock) ;  // try again
    }
    fprintf(stderr,"RSF_File_lock DEBUG : locked by pid %d\n", getpid());
  }else{
    file_lock.l_type = F_UNLCK ;                 // release lock
    status = fcntl(fd, F_SETLK, &file_lock) ;
    fprintf(stderr,"RSF_File_lock DEBUG : released by pid %d\n", getpid());
  }
  return status ;
}

// create an empty RSF segment : start of segment, empty directory, end of segment
// if sparse_size == 0 , create a "compact" segment,
// otherwise create a "sparse" segment of size at least sparse_size (in bytes) (some rounding up might be used)
int RSF_New_empty_segment(RSF_File *fp, int32_t meta_dim, const char *appl, uint64_t sparse_size){
  ssize_t nc ;
  int i ;
  off_t start = 0 ;
  start_of_segment sos0 = SOS ;
  start_of_segment sos1 = SOS ;
  end_of_segment eos = EOS ;
  end_of_segment eos1 ;
  uint64_t rl_sparse ;
  int status = -1 ;

// fprintf(stderr,"RSF_New_empty_segment DEBUG: waiting for lock, pid = %d\n",getpid());
//   RSF_File_lock(fp->fd, start, sizeof(start_of_segment), 1) ;    // lock file address range 
  RSF_File_lock(fp->fd, 1) ;    // lock file address range 
// fprintf(stderr,"RSF_New_empty_segment DEBUG: lock acquired, pid = %d\n",getpid());
usleep(1000) ;
  nc = read(fp->fd, &sos0, sizeof(start_of_segment)) ;           // try to read first start of segment into sos0

  if(nc == 0){                                                   // empty file

    if(meta_dim <= 0) {
      fprintf(stderr, "RSF_New_empty_segment: ERROR meta_dim <= 0, %d\n", meta_dim) ;
      goto ERROR ;
    }
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
    meta_dim = sos0.meta_dim ;                                   // get meta_dim from SOS for existing file
  }  // if(nc == 0)

  memcpy(&sos1, &sos0, sizeof(start_of_segment)) ;               // copy sos0 into sos1
// fprintf(stderr, "RSF_New_empty_segment: DEBUG sparse size = %ld\n", sparse_size) ;
// system("ls -l demo0.rsf") ;
  if(sparse_size > 0) {                                          // sparse segment

    // mark segment 0 as being open for parallel segments write (add one to parallel segment count in rlm)
    sos0.head.rlm = sos0.head.rlm + 1 ;
    rl_sparse = sparse_size - sizeof(start_of_segment) ;         // end of sparse segment record length
    // fill eos and sos1 with proper values (sparse segment size, record length)
    RSF_64_to_32(eos.h.tail.rl, rl_sparse) ;
    RSF_64_to_32(eos.h.sseg, sparse_size) ;                      // segment length = sparse_size
    eos.h.meta_dim = meta_dim ;
    RSF_64_to_32(eos.l.head.rl, rl_sparse) ;
    // write end of segment (high part) at the correct position (this will create the "hole" in the sparse file
    lseek(fp->fd, start + sparse_size - sizeof(end_of_segment_hi), SEEK_SET) ;
    nc = write(fp->fd, &eos.h, sizeof(end_of_segment_hi)) ;

    RSF_64_to_32(sos1.sseg, sparse_size) ;                       // sseg MUST be non zero in a new sparse segment

  }else{                                                         // compact segment

    sos0.head.rlm = 0xFFFF ;                                     // mark segment 0 as being open for exclusive write
    eos.l.head.rlm = meta_dim ;
    eos.h.tail.rlm = meta_dim ;
    RSF_64_to_32(sos1.sseg, 0L) ;                                // unlimited size segment

  } // if(sparse_size > 0)

  fp->seg_max = sparse_size ;                                    // 0 for compact (non sparse) unlimited size segment
  RSF_64_to_32(sos1.dir, 0L) ;                                   // no directory, set size and offset to 0 ;
  RSF_64_to_32(sos1.dirs, 0L) ;
  RSF_64_to_32(sos1.seg, 0L) ;                                   // seg = 0 when a segment is open
  sos1.head.rlm = sos0.head.rlm ;                                // mark active segment as being open for write (same as sos0)

  memcpy(&(fp->sos1), &sos1, sizeof(start_of_segment)) ;         // copy into RSF_File structure (current segment)
  lseek(fp->fd, start, SEEK_SET) ;
  nc = write(fp->fd, &sos1, sizeof(start_of_segment)) ;          // write sos1 at proper position (end of file)

  memcpy(&(fp->eos1), &eos, sizeof(end_of_segment)) ;            // copy eos into RSF_File structure
  nc = write(fp->fd, &eos.l, sizeof(end_of_segment_lo)) ;        // write end of segment (low part) at the correct position
  if(sparse_size == 0) nc = write(fp->fd, &eos.h, sizeof(end_of_segment_hi)) ;

  memcpy(&(fp->sos0), &sos0, sizeof(start_of_segment)) ;         // copy sos0 into RSF_File structure
  lseek(fp->fd, 0L, SEEK_SET) ;
  nc = write(fp->fd, &sos0, sizeof(start_of_segment)) ;          // write sos0 back into to file segment 0
  if(nc != sizeof(start_of_segment)) goto ERROR ;                // write failed


  fp->meta_dim = meta_dim ;
  fp->seg_base = start ;                                     // base offset of active segment
  fp->next_write = start + sizeof(start_of_segment) ;
  fprintf(stderr,"RSF_New_empty_segment DEBUG : EOF at %lx\n", lseek(fp->fd, 0L, SEEK_END)) ;
  fp->cur_pos = lseek(fp->fd, fp->next_write, SEEK_SET) ;
  fp->last_op = OP_WRITE ;
// fprintf(stderr, "RSF_New_empty_segment: DEBUG meta_dim = %d, write at %lx\n", meta_dim, fp->cur_pos) ;

  status = 0 ;
// fprintf(stderr,"after segment create %ld\n", lseek(fp->fd, 0L , SEEK_CUR));
// system("ls -l demo0.rsf") ;
ERROR :
//   RSF_File_lock(fp->fd, start, sizeof(start_of_segment), 0) ;  // unlock file address range
  RSF_File_lock(fp->fd, 0) ;  // unlock file address range
// fprintf(stderr,"RSF_New_empty_segment DEBUG: lock released, pid = %d\n",getpid());
  return status ;
}

// placeholder for now
// closes the file and return an error status
// will be replaced by RSF_New_empty_segment(fp, meta_dim, appl, segsizep)
// int RSF_Add_sparse_segment(RSF_File *fp, int64_t segsizep){
//   close(fp->fd) ;   // for now, just close file
//   return -1 ;
// }

// open a file (file segment), return handle (pointer to file control structure) or NULL if error
//
// fname is the path to the file (it will be stored internally as a canonical path)
// mode -s RO/RW/AP/... 
//   if mode is zero, it will be RW if the file is writable or can be created, RO otherwise
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
  start_of_segment sos = SOS ;
  RSF_handle handle ;
//   off_t offset_eof, offset_last ;
//   int64_t size_last, size_last2, dir_last, dir_last2, ssize_last, ssize_last2 ;
//   end_of_segment eos ;
//   int i ;
  int64_t segsize = 0;
  ssize_t nc ;
  char *errmsg = "" ;

  if(fp == NULL) {
    errmsg = " allocation failed" ;
    goto ERROR ;
  }

  if(segsizep) segsize = *segsizep;      // segsize will be 0 if segsizep is NULL

  if(segsize < 0) goto ERROR ;           // invalid segment size

  RSF_File_init(fp) ;                    // set safe initial values

  if(mode == 0) mode = RSF_RW ;          // automatic mode, try to open for read+write with RO fallback
  switch(mode & (RSF_RO | RSF_RW | RSF_AP)){

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

    case RSF_AP:                         // to be added later
      mode = RSF_RW ;                    // open existing file for read+write, no fallback
      errmsg = " cannot open in write mode" ;
      if( (fp->fd = open(fname, O_RDWR, 0777)) == -1) goto ERROR ;  // cannot open in write mode
      break ;

    default:
      break ;
  }
  errmsg = "" ;

  RSF_Vdir_init(fp) ;
// fprintf(stderr,"RSF_Open_file: 2, fd = %d\n", fp->fd);
  fp->mode = mode ;
  fp->name = realpath(fname, NULL) ;     // get canonical path
  fp->seg_base = 0 ;                     // a priori first segment of the file
// fprintf(stderr, "file='%s', mode = %d, fd = %d, size = %ld\n",fp->name,mode,fp->fd,fp->size);

  if( (fp->mode & RSF_RW) == RSF_RW) {               // read+write

    if( RSF_New_empty_segment(fp, *meta_dim, appl, segsize) == -1 ){
      errmsg = " cannot create empty segment at end of file" ;
      goto ERROR ;
    }
    *meta_dim = fp->meta_dim ;
    fp->slot = RSF_Set_file_slot(fp) ;
// fprintf(stderr,"RSF_Open_file DEBUG : slot assigned = %d\n", fp->slot) ;
    if( RSF_Read_directory(fp) < 0 ){         // read directory from all segments
      RSF_Purge_file_slot(fp) ;               // remove from file table in case of error
      errmsg = " Read_directory failed" ;
      goto ERROR ;
    }
// fprintf(stderr,"RSF_Open_file DEBUG : slot = %d\n", fp->slot);
  }else{                                  // read only mode

    lseek(fp->fd, fp->seg_base = 0, SEEK_SET) ;               // first segment of the file
    if( (nc = read(fp->fd, &sos, sizeof(sos))) < sizeof(sos)) {
      errmsg = " file is empty" ;
      close(fp->fd) ;
      goto ERROR ;  // invalid SOS (too short)
    }
    *meta_dim = sos.meta_dim ;
    fp->meta_dim = sos.meta_dim ;
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

  }  // (fp->mode == RSF_RW)

// fprintf(stderr,"RSF_Open_file: 4, fp = %p\n", fp);
// fprintf(stderr,"after open %ld, next write = %ld\n", lseek(fp->fd, 0L , SEEK_CUR), fp->next_write);
  handle.p = fp ;
  return handle ;

ERROR:
fprintf(stderr,"RSF_Open_file ERROR : '%s' %s\n", fname, errmsg);
  if(fp != NULL) {
    if(fp->name != NULL) free(fp->name) ;  // free allocated string for file name
    free(fp) ;                             // free allocated structure
  }
  handle.p = NULL ;
  return handle ;                          // return a NULL handle
}

// close a RSF file
int32_t RSF_Close_file(RSF_handle h){
  RSF_File *fp = (RSF_File *) h.p ;
  int32_t i, slot ;
  start_of_segment sos = SOS ;
  end_of_segment eos = EOS ;
  off_t offset, offset_dir, offset_vdir, offset_eof, cur ;
  uint64_t dir_size, vdir_size, sparse_size, rl_eos ;
  ssize_t nc ;
  uint64_t sparse_start, sparse_top ;
  int32_t write0 = 1 ;
  uint64_t base0_seg_max = 0 ;

  if( ! (slot = RSF_Valid_file(fp)) ) return 0 ;   // something not O.K. with fp

  base0_seg_max = RSF_32_to_64(fp->sos0.sseg) ;    // seg_max from first segment of file
//   if(fp->seg_max > 0 && fp->seg_base ==0) {
  if(base0_seg_max > 0) {
    write0 = 0 ;  // first segment is a sparse segment
    fprintf(stderr,"RSF_Close_file DEBUG : segment 0 is sparse\n");
  }
  if( (fp->mode & RSF_RO) == RSF_RO) goto CLOSE ;  // file open in read only mode, nothing to rewrite
// system("ls -l demo0.rsf");
// fprintf(stderr,"before close position %ld, next_write = %ld \n", lseek(fp->fd, 0L , SEEK_CUR), fp->next_write);
// fprintf(stderr,"RSF_Close_file DEBUG : beginning of close ");
  // discriminate between compact segment (exclusive write) and sparse segment (parallel write)
  // in sparse mode, 2 SOS records (first SOS in file and SOS of sparse segment) must be rewritten
  //                 1 EOS (split EOS) record must be rewritten
  // in compact mode, 2 SOS records (first SOS in file and active segment SOS) must be rewritten
  //                  1 EOS record (at end of file) must be rewritten
  // in fuse mode     the original SOS for the active segment is reinitialized to 0
  //                  the first SOS in file becomes the only good one and points to the final EOS
  //                  the EOS at end of file becomes the only good one
  //                  all other EOS/SOS records in file (if any) will be ignored
  //                  all DIR records except the last one will be ignored
  sparse_start = fp->seg_base ;                                         // original start of segment
  sparse_top = (fp->seg_max > 0) ? (fp->seg_base + fp->seg_max) : 0 ;   // original end of segment (0 if not sparse)
  if( (fp->mode & RSF_FUSE) == RSF_FUSE ) {
    lseek(fp->fd, fp->seg_base , SEEK_SET) ;                  // seek to start of this segment
    nc = write(fp->fd, &sos, sizeof(start_of_segment)) ;      // nullify original start_of_segment
//     fp->cur_pos = lseek(fp->fd, fp->next_write , SEEK_SET) ;  // reset position of write pointer
    fp->seg_base = 0 ;                                        // segment 0 will become the only segment in file
    fp->isnew = 1 ;                                           // saved sos0 will not be used
    fp->dir_read = 0 ;                                        // all directory entries will be written
    fprintf(stderr,"RSF_Close_file DEBUG : fusing segments into one\n") ;
  }
// fprintf(stderr," segmax = %ld, sparse_start = %ld, sparse_top = %ld, next write = %ld, segbase = %ld\n", 
//         fp->seg_max, sparse_start, sparse_top, fp->next_write, fp->seg_base);
offset_vdir = fp->next_write - fp->seg_base ;                    // vdir offset into segment
vdir_size = RSF_Write_vdir(fp) ;                                 // write vdir,
offset_eof = offset_dir + vdir_size + sizeof(end_of_segment) ;   // offset_eof points to after eventual eos

  offset_dir = fp->next_write - fp->seg_base ;                // directory offset into segment
  offset_eof = offset_dir + RSF_Disk_dir_size(fp) + sizeof(end_of_segment) ;  // offset_eof points to after eos
// fprintf(stderr,"RSF_Close_file DEBUG :, offset_dir = %lx, offset_eof = %lx\n", offset_dir, offset_eof) ;
// fprintf(stderr,"offset_dir = %16lo, offset_eof = %16lo\n",offset_dir, offset_eof);
  fp->cur_pos = lseek(fp->fd, fp->next_write , SEEK_SET) ;  // reset position of write pointer
// fprintf(stderr,"before write directory %ld\n", lseek(fp->fd, 0L , SEEK_CUR));
  dir_size = RSF_Write_directory(fp) ;             // write directory to file
// fprintf(stderr,"after write directory %ld\n", lseek(fp->fd, 0L , SEEK_CUR));

// fprintf(stderr,"RSF_Close_file DEBUG :, after write directory, pos = %lx\n", lseek(fp->fd, 0L, SEEK_CUR));
  // write compact end of segment
  eos.l.head.rlm = fp->meta_dim ;
  RSF_64_to_32(eos.l.head.rl, sizeof(end_of_segment)) ;
  eos.h.meta_dim = fp->meta_dim ;
  RSF_64_to_32(eos.h.dir,  offset_dir) ;           // directory record position in file
  RSF_64_to_32(eos.h.vdir,  offset_vdir) ;         // vdir record position in file
  RSF_64_to_32(eos.h.dirs, dir_size) ;             // directory record size
  RSF_64_to_32(eos.h.vdirs, vdir_size) ;           // vdir record size
  RSF_64_to_32(eos.h.sseg, offset_eof) ;           // segment size including EOS
  RSF_64_to_32(eos.h.seg,  offset_eof - sizeof(end_of_segment)) ;           // segment size excluding EOS
  eos.h.tail.rlm = fp->meta_dim ;
  RSF_64_to_32(eos.h.tail.rl, sizeof(end_of_segment)) ;
  nc = write(fp->fd, &eos, sizeof(end_of_segment)) ;    // write end of compact segment
  sparse_start = lseek(fp->fd, 0L, SEEK_CUR) ;

fprintf(stderr,"DEBUG: CLOSE: '%s' EOS, rt = %d, zr = %d, rl = %8.8x %8.8x, rlm = %d\n", 
        fp->name, eos.h.tail.rt, eos.h.tail.zr, eos.h.tail.rl[0], eos.h.tail.rl[1], eos.h.tail.rlm);
// fprintf(stderr,"RSF_Close_file DEBUG : middle of close, segmax = %ld, sparse_start = %ld, sparse_top = %ld\n",fp->seg_max, sparse_start, sparse_top);
// system("ls -l demo0.rsf");

  if(fp->seg_max > 0){
    sparse_start = lseek(fp->fd, 0L, SEEK_CUR) ;
    sparse_size = sparse_top - sparse_start ;         // new sparse segment size, original end of sparse segment - after compact eos
//     fprintf(stderr,"DEBUG: CLOSE: sparse_size = %lx %ld, offset_eof = %lx %ld\n", sparse_size, sparse_size, offset_eof, offset_eof) ;

    RSF_64_to_32(sos.seg, 0L) ;
    RSF_64_to_32(sos.sseg, sparse_size) ;
//     cur = lseek(fp->fd, 0L, SEEK_CUR) ;
//     fprintf(stderr,"DEBUG: CLOSE: sos at %lx\n",cur) ;
    nc = write(fp->fd, &sos, sizeof(start_of_segment)) ;

    memcpy(&eos, &fp->eos1, sizeof(eos)) ;         // get or1ginal EOS
    rl_eos = sparse_size - sizeof(start_of_segment) ;
    RSF_64_to_32(eos.l.head.rl, rl_eos) ;
//     cur = lseek(fp->fd, 0L, SEEK_CUR) ;
//     fprintf(stderr,"DEBUG: CLOSE: eos.l at %lx\n",cur) ;
    nc = write(fp->fd, &eos.l, sizeof(end_of_segment_lo)) ;

    RSF_64_to_32(eos.h.tail.rl, rl_eos) ;
    RSF_64_to_32(eos.h.sseg, sparse_size) ;
    cur = lseek(fp->fd, rl_eos - sizeof(end_of_segment_hi) - sizeof(end_of_segment_lo), SEEK_CUR);
    nc = write(fp->fd, &eos.h, sizeof(end_of_segment_hi)) ;
//     fprintf(stderr,"DEBUG: CLOSE: eos.h %lx to %lx\n", cur, cur + nc -1);
  }

  // fix start of active segment (segment size + address of directory)
  lseek(fp->fd, fp->seg_base , SEEK_SET) ;
  nc = read(fp->fd, &sos, sizeof(start_of_segment)) ;   // read start of segment (to keep appl signature)
  RSF_64_to_32(sos.dir,  offset_dir) ;             // directory record position in file
  RSF_64_to_32(sos.vdir,  offset_vdir) ;           // vdir record position in file
  RSF_64_to_32(sos.dirs, dir_size) ;               // directory record size
  RSF_64_to_32(sos.vdirs, vdir_size) ;             // directory record size
  RSF_64_to_32(sos.sseg,  offset_eof) ;            // segment size including EOS
  RSF_64_to_32(sos.seg,  offset_eof - sizeof(end_of_segment)) ;             // segment size excluding EOS
  sos.head.rlm = 0 ;    // fix SOR of appropriate SOS record if file was open in write mode
  cur = lseek(fp->fd, fp->seg_base , SEEK_SET) ;
  nc = write(fp->fd, &sos, sizeof(start_of_segment)) ;      // rewrite start of active segment
// fprintf(stderr,"RSF_Close_file DEBUG : start_of_segment at %ld\n",fp->seg_base);
  if(fp->isnew == 0 && write0 == 1) {  // skip this for new files, fused files, and segment 0 sparse
    fprintf(stderr,"RSF_Close_file DEBUG : rewriting segment 0 header\n");
    lseek(fp->fd, offset = 0 , SEEK_SET) ;
    fp->sos0.head.rlm = 0 ;
    nc = write(fp->fd, &fp->sos0, sizeof(start_of_segment)) ;  // rewrite start of segment 0
  }
//   fprintf(stderr,"DEBUG: CLOSE: EOF at %lx\n", lseek(fp->fd, 0L, SEEK_END)) ;

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

// dump the contents of a file's directory in condensed format
void RSF_Dump_vdir(RSF_handle h) {
  RSF_File *fp = (RSF_File *) h.p ;
  int32_t i, j, slot, meta_dim ;
  uint64_t wa, rl  ;
  uint32_t *meta ;
  vdir_entry *ventry ;

  if( ! (slot = RSF_Valid_file(fp)) ) return;   // something not O.K. with fp

  for(i = 0 ; i < fp->vdir_used ; i++){
    ventry = fp->vdir[i] ;
    wa = RSF_32_to_64(ventry->wa) ;
    rl = RSF_32_to_64(ventry->rl) ;
    meta_dim = ventry->ml ;
    meta = &(ventry->meta[0]) ;
    fprintf(stderr,"%12.12lx (%8.8lx)", wa, rl) ;
    for(j = 0 ; j < meta_dim ; j++) fprintf(stderr," %8.8x", meta[j]) ;
    fprintf(stderr,"\n") ;
  }
}

// dump the contents of a file in condensed format
void RSF_Dump(char *name, int verbose){
  int fd = open(name, O_RDONLY) ;
  start_of_record sor ;
  end_of_record   eor ;
  start_of_segment sos ;
  end_of_segment   eos ;
  int rec = 0 ;
  off_t reclen, datalen, tlen, eoslen, read_len ;
  ssize_t nc ;
  char *tab[] = { " NULL", " DATA", "[DIR]", "[SOS]", "[EOS]", " XDAT", "[VLD]", " FILE",
                  " NULL", " DATA", "_dir_", "_sos_", "_eos_", " XDAT", "_vld_", " FILE"} ;
  int meta_dim = -1 ;
  uint64_t segsize, ssize ;
  disk_directory *d = NULL ;
  disk_vdir *vd = NULL ;
  disk_dir_entry *entry ;
  vdir_entry *ventry ;
  char *e ;
  uint64_t dir_entry_size ;
  uint32_t *meta ;
  int i, j ;
  uint32_t *data ;
  int ndata ;
  off_t dir_offset, dir_addr, vdir_addr, rec_offset, offset, seg_offset, dir_seg_offset, l_offset ;
  int64_t wa, rl ;
  int tabplus = 0 ;
  uint64_t seg_bot, seg_top, seg_dir, seg_vdir ;
  int segment = 0 ;
  uint64_t eof ;
  char buffer[4096] ;
  char *fname ;
  char *temp, *temp0 ;
  uint32_t *tempm ;
  uint32_t nmeta ;
  uint64_t temps ;

  if(fd < 0) return ;
  eof = lseek(fd, 0L, SEEK_END) ;
  lseek(fd, 0L, SEEK_SET) ;
  dir_addr = 0 ;
  vdir_addr = 0 ;
  offset = 0 ;
  rec_offset = offset ;  // current position
  seg_offset = 0 ;
  nc = read(fd, &sor, sizeof(sor)) ;
  seg_bot = 0 ;
  seg_top = 0 ;
  seg_dir = 0 ;
  while(nc > 0) {
    reclen = RSF_32_to_64(sor.rl) ;
    datalen = reclen - sizeof(sor) - sizeof(eor) ;
    tabplus = ((rec_offset < seg_dir) && (rec_offset < seg_vdir)) ? 8 : 0 ;  // only effective for sos, eos, dir records 
    if(sor.rt > 7) {
      snprintf(buffer,sizeof(buffer)," RT%2.2x %5d [%12.12lx], rl = %6ld(%6ld),",sor.rt,  rec, rec_offset, reclen, datalen) ;
      sor.rt = 0 ;
    }else{
      snprintf(buffer,sizeof(buffer),"%s %5d [%12.12lx], rl = %6ld(%6ld),",tab[sor.rt+tabplus],  rec, rec_offset, reclen, datalen) ;
    }
    switch(sor.rt){
      case RT_VDIR :
      case RT_DIR :
        dir_offset = lseek(fd, -sizeof(sor), SEEK_CUR) ; 
        d = (disk_directory *) malloc(datalen + sizeof(sor)) ;
        vd = (disk_vdir *) d ;
        nc =  read(fd, d, datalen + sizeof(sor) ) ;  // read directory
        fprintf(stderr,"  %s meta_dim = %d, records  = %8d, dir offset %8.8lx %s addr %8.8lx, rlm = %d", buffer,
                d->meta_dim, d->entries_nused, dir_offset, (dir_offset == dir_addr) ? "==": "!=", dir_addr, d->sor.rlm) ;
        break ;
      case 0 :
      case RT_DATA :
      case RT_XDAT :
        read_len = sor.rlm * sizeof(uint32_t) ;
        data = (uint32_t *) malloc(read_len) ;
        nc = read(fd, data, read_len) ;               // read metadata part
        lseek(fd, datalen - nc, SEEK_CUR) ;           // skip rest of record
        ndata = datalen/sizeof(int32_t) - sor.rlm;
        fprintf(stderr,"  %s DL = %6d, ML = %2d [", buffer, ndata, sor.rlm) ;
        for(i=0 ; i<sor.rlm ; i++) {
          fprintf(stderr," %8.8x", data[i]) ; 
        }
        fprintf(stderr,"], rlm = %d",sor.rlm) ;
        if(data) free(data) ;
        data = NULL ;
        break ;
      case RT_FILE :
        read_len = sor.rlm * sizeof(uint32_t) ;
        data = (uint32_t *) malloc(read_len) ;
        nc = read(fd, data, read_len) ;               // read metadata part
        temp0 = (char *) data ;                       // start of metadata
        temp = temp0 + nc -1 ;                  // end of metadata
        while((temp[ 0] == 0) && (temp > temp0)) temp-- ;    // skip trailing nulls
        while((temp[-1] != 0) && (temp > temp0)) temp-- ;    // back until null is found
        temp0 = temp -1 ;
        tempm = (uint32_t *) temp0 ;
        nmeta = tempm - data ;
        temps = RSF_32_to_64((data+nmeta-2)) ;
        lseek(fd, datalen - nc, SEEK_CUR) ;           // skip rest of record
        ndata = datalen/sizeof(int32_t) - sor.rlm;
        fprintf(stderr,"  %s DL = %6d, ML = %2d [ %8.8x ", buffer, ndata, sor.rlm, data[0]) ;
        for(j=1 ; j<nmeta-2 ; j++) fprintf(stderr,"%8.8x ", data[j]) ;
        fprintf(stderr,"] '%s'[%ld]", temp, temps) ;
//         for(i=0 ; i<sor.rlm ; i++) {
//           fprintf(stderr," %8.8x", data[i]) ; 
//         }
        fprintf(stderr,", rlm = %d",sor.rlm) ;
        if(data) free(data) ;
        data = NULL ;
        break ;
      case RT_SOS :
        lseek(fd, -sizeof(sor), SEEK_CUR) ; 
        seg_offset = rec_offset ;
        nc = read(fd, &sos, sizeof(sos) - sizeof(eor)) ;
        meta_dim = sos.meta_dim ;
        dir_addr = RSF_32_to_64(sos.dir) ;
        vdir_addr = RSF_32_to_64(sos.vdir) ;
        segsize  = RSF_32_to_64(sos.sseg) ;
        ssize    = RSF_32_to_64(sos.seg) ;
        if(seg_offset >= seg_top) {
          seg_bot = seg_offset ;
          seg_top = seg_bot + segsize - 1 ;
          seg_dir = seg_bot + dir_addr ;
          seg_vdir = seg_bot + vdir_addr ;
          dir_seg_offset = seg_offset ;
        }
        fprintf(stderr,"%s",(ssize == 0 && segsize != 0) ? "<>" : "  " ) ;
        fprintf(stderr,"%s meta_dim = %d, seg size = %8ld, dir_offset = %8.8lx %8.8lx, rlm = %d", buffer,
                meta_dim, segsize, dir_addr, vdir_addr, sos.head.rlm) ;
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
        ssize    = RSF_32_to_64(sos.seg) ;
        dir_addr = RSF_32_to_64(eos.h.dir) ;
        vdir_addr = RSF_32_to_64(eos.h.vdir) ;
        fprintf(stderr,"%s",(reclen > sizeof(end_of_segment)) ? "<>" : "  " ) ;
        fprintf(stderr,"%s meta_dim = %d, seg size = %8ld, dir_offset = %8.8lx %8.8lx, rlm = %d", buffer,
                meta_dim, segsize, dir_addr, vdir_addr, eos.l.head.rlm) ;
        break ;
      default :
        lseek(fd, datalen, SEEK_CUR) ;   // skip data
        break ;
    }
    nc = read(fd, &eor, sizeof(eor)) ;
    tlen = RSF_32_to_64(eor.rl) ;
    if(tlen != reclen){
      fprintf(stderr,"|%d rl = %ld|%ld S(%2.2d) %s\n",eor.rlm, reclen, tlen, segment, (tlen==reclen) ? ", O.K." : ", RL ERROR") ;
    }else{
      fprintf(stderr,"|%d S(%2.2d) %s\n",eor.rlm, segment, ", O.K.") ;
    }
    if(tabplus == 0 && sor.rt == RT_EOS) segment++ ;
    if(tlen != reclen) goto END ; ;
    if(sor.rt == RT_VDIR  && vd != NULL  && vd->entries_nused > 0){
      if( (verbose > 0 && tabplus == 0) || (verbose > 10) ){
        fprintf(stderr," Directory  WA(SEG)       WA(FILE)         RL      RT CLASS  META \n") ;
        e = (char *) &(d->entry[0]) ;
        l_offset = (tabplus == 0) ? dir_seg_offset : seg_offset ;
        for(i=0 ; i < d->entries_nused ; i++){
          ventry = (vdir_entry *) e ;
          wa = RSF_32_to_64(ventry->wa) ;
          rl = RSF_32_to_64(ventry->rl) ;
          meta = &(ventry->meta[0]) ;
          fprintf(stderr," [%6d] %12.12lx [%12.12lx] %12.12lx", i, wa, wa+l_offset, rl) ;
          fprintf(stderr," %2.2x %6.6x", meta[0] & 0xFF, meta[0] >> 8) ;
          if( (meta[0] & 0xFF) != RT_FILE){
            for(j=1 ; j<ventry->ml ; j++) fprintf(stderr," %8.8x", meta[j]) ;
            fprintf(stderr,"\n") ;
          }else{
            temp0 = (char *) meta ;
            temp = temp0 + (ventry->ml * sizeof(uint32_t)) -1 ;
            while((temp[ 0] == 0) && (temp > temp0)) temp-- ;    // skip trailing nulls
            while((temp[-1] != 0) && (temp > temp0)) temp-- ;    // back until null is found
            temp0 = temp -1 ;
            tempm = (uint32_t *) temp0 ;
            nmeta = tempm - meta ;
            temps = RSF_32_to_64((meta+nmeta-2)) ;
//             fprintf(stderr," %p %p",tempm, meta) ;
            for(j=1 ; j<nmeta-2 ; j++) fprintf(stderr," %8.8x", meta[j]) ;
            fprintf(stderr," '%s' [%ld]\n", temp, temps) ;
          }
          e = e + sizeof(vdir_entry) + ventry->ml * sizeof(uint32_t) ;
        }
        fprintf(stderr,"-------------------------------------------------\n") ;
      }
    }
    if(sor.rt == RT_DIR && d != NULL && d->entries_nused > 0){
      if( (verbose > 100 && tabplus == 0) || (verbose > 100) ){
        fprintf(stderr," Directory  WA(SEG)       WA(FILE)         RL      RT CLASS  META \n") ;
        e = (char *) &(d->entry[0]) ;
        meta_dim = d->meta_dim ;
        dir_entry_size = sizeof(uint32_t)*meta_dim + sizeof(disk_dir_entry) ;
        l_offset = (tabplus == 0) ? dir_seg_offset : seg_offset ;
        for(i=0 ; i < d->entries_nused ; i++){
          entry =  (disk_dir_entry *) e ;
          wa = RSF_32_to_64(entry->wa) ;
          rl = RSF_32_to_64(entry->rl) ;
          meta = &(entry->meta[0]) ;
          fprintf(stderr," [%6d] %12.12lx [%12.12lx] %12.12lx", i, wa, wa+l_offset, rl) ;
          fprintf(stderr," %2.2x %6.6x", meta[0] & 0xFF, meta[0] >> 8) ;
          for(j=1 ; j<meta_dim ; j++) fprintf(stderr," %8.8x", meta[j]) ; fprintf(stderr,"\n") ;
          e += dir_entry_size ;
        }
        fprintf(stderr,"-------------------------------------------------\n") ;
      }
    }
    if(d) {
      free(d) ;
      d = NULL ;
    }
    rec++ ;
    rec_offset = lseek(fd, offset = 0, SEEK_CUR) ;  // current position
    if(rec_offset >= eof) break ;
    nc = read(fd, &sor, sizeof(sor)) ;
    if((nc > 0) && (nc < sizeof(sor))){
      fprintf(stderr," invalid sor, len = %ld, expected = %ld\n", nc, sizeof(sor)) ;
      break ;
    }
  }
END :
//   fprintf(stderr,"DEBUG: DUMP: EOF at %lx\n", eof) ;
  close(fd) ;
  fprintf(stderr,"RSF_Dump DEBUG: file '%s' closed, fd = %d, EOF = %lx\n", name, fd, eof) ;
}
//       dir_entry_size = sizeof(uint32_t)*meta_dim + sizeof(disk_dir_entry) ;  // size of a file directory entry
//       e = (char *) &(ddir->entry[0]) ;                      // pointer to first directory entry
//       for(i = 0 ; i < ddir->entries_nused ; i++){           // loop over entries in disk directory
//         entry = (disk_dir_entry *) e ;                      // disk directory entry (wa, rl, meta)
//         wa = RSF_32_to_64(entry->wa) + off_seg ;            // record offset from start of file
//         rl = RSF_32_to_64(entry->rl) ;                      // record length
//         meta = &(entry->meta[0]) ;                          // record metadata
//         e += dir_entry_size ;                               // next directory entry
//       }
