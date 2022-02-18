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

#include <rsf.h>

// Random Segmented Files (RSF) internal (private) definitions and structures
//
//           file structure (N segments)
// +-----------+-----------+                  +-----------+
// | segment 1 | segment 2 |..................| segment N |
// +-----------+-----------+                  +-----------+
//
//           segment structure (compact segment)
//           data and directory record(s) may be absent (empty segment)
// +-----+------+               +------+-----------+------+------+
// | SOS | data |------data-----| data | directory | EOSl | EOSh |
// +-----+------+               +------+-----------+------+------+
//           segment structure (sparse segment)
//           data and directory record(s) may be absent (empty segment)
// +-----+------+               +------+-----------+------+               +------+
// | SOS | data |------data-----| data | directory | EOSl |......gap......| EOSh |
// +-----+------+               +------+-----------+------+               +------+
// SOS       start of data record
// data      data record
// directory directory record
// EOSl      low part of end of segment record
// EOSh      high part of end of segment record
// gap       unpopulated zone in file address space (sparse file)

// structure of a record (array of 32 bit items)
//   ZR    RLM    RT   RL[0]   RL[1]   LMETA                                       RL[0]   RL[1]   ZR    RLM    RT
// +----+-------+----+-------+-------+-------+----------+------------------------+-------+-------+----+-------+----+
// |    |       |    |       |       | LMETA | METADATA |       DATA             |       |       |    |       |    |
// +----+-------+----+-------+-------+-------+----------+------------------------+-------+-------+----+-------+----+
// <---------- start of record (SOR) -------->                                   <------- end of record (EOR) ----->
//   8     16     8     32      32    16/8/8   RLM x 32                             32      32      8    16     8   (# of bits)
// <--------------------------------------------  RL[0] * 2**32 + RL[1] bytes  ------------------------------------>
// ZR    : marker (0xFE for SOR, 0xFF for EOR)
// RLM   : length of metadata in 32 bit units (data record)
//         RLM in a data record MAY be larger than directory length of metadata
//         (secondary metadata not in directory)
//       : open-for-write flag (Start of segment record) (0 if not open for write)
//       : undefined (End of segment or other record)
//LMETA  : record local metadata, RLMD, UBC, DUL [] )
//         RLMD (16 bits) directory metadata length for this record
//         UBC  ( 8 bits) Unused Bit Count (RL is always a multiple of 4 Bytes)
//         DUL  ( 8 bits) Data Unit Length (1/2/4/8 bytes) for Endianness adjustment
// RT    : record type (normally 0 -> 0x80)
// RL    : record length = RL[0] * 2**32 + RL[1] bytes (ALWAYS a multiple of 4 bytes)
//
// for endianness purposes, these files are mostly composed of 32 bit items, 
//   but 8/16/32/64 data items may be found in these files (identified as such)
// 64 bit quantities may be represented with a pair of 32 bit values, Most Significant Part first
// record/segment lengths are always multiples of 4 bytes
// the first segment of a file MUST be a compact segment (CANNOT be a sparse segment)
// a segment can be opened for write into ONCE, and cannot be reopened to append data into it
// a sparse segment becomes a compact segment when closed (a dummy sparse segment takes the remaining space)
// the concatenation of 2 RSF files is a valid RSF file
// RSF files may be used as containers for other files
// a "FUSE" operation can consolidate all directories from a file into a single directory
//   to make future access more efficient
// while records are being added to a file (always in NEW segments) the "old" segments are readable
// sparse segment may be written in parallel by multiple processes, if the underlying
//   filesystem supports advisory locks.

// main record type rt values
// 0     : INVALID
// 1     : data record
// 2     : xdata record (extension record)
// 3     : start of segment record
// 4     : end of segment record
// 6     : segment directory record
// 7     : the record contains a file
// 8-127 : custom records (not defined yet)
// 128   : deleted record
//
// rt:rlm:zr can be used for endianness detection, least significant byte (zr) ZR_xx, most significant byte (rt) RT_xx
// rt and zr can never have the same value (RT = 0->128 , ZR = 254->255)
// the zr field is 0xFE for start_of_record and 0xFF for end_of_record
// max record length is 2**48 - 1 bytes (rl is in bytes) (256 TBytes) (for now)

#define ZR_SOR  0XFE
#define ZR_EOR  0xFF

#define RL_SOR sizeof(start_of_record)
#define RL_EOR sizeof(end_of_record)
#define RL_SOS sizeof(start_of_segment)
#define RL_EOSL sizeof(end_of_segment_lo)
#define RL_EOSH sizeof(end_of_segment_hi)
#define RL_EOS (RL_EOSL + RL_EOSH)

// convert a pair of unsigned 32 bit elements into an unsigned 64 bit element
static inline uint64_t RSF_32_to_64(uint32_t u32[2]){
  uint64_t u64;
  u64 = u32[0] ; u64 <<=32 ; u64 += u32[1] ;
  return u64 ;
}

static inline void RSF_64_to_32(uint32_t u32[2], uint64_t u64){
  u32[0] = (u64 >> 32) ;
  u32[1] = (u64 & 0xFFFFFFFFu) ;
}

typedef struct {                   // record header
  uint32_t rt:8, rlm:16, zr:8 ;    // ZR_SOR, metadata length (32 bit units), record type
  uint32_t rl[2] ;                 // upper[0], lower[1] 32 bits of record length (bytes)
  uint32_t  rlmd:16, ubc:8, dul:8 ;
} start_of_record ;

#define SOR {RT_DATA, 0, ZR_SOR, {0, 0}, 0, 0, 0}

// record length from start_of_record (0 if invalid)
// rt : expected record type (0 means anything is O.K.)
static inline uint64_t RSF_Rl_sor(start_of_record sor, int rt){
  uint64_t rl ;
  if(sor.zr != ZR_SOR ) {
    fprintf(stderr,"invalid sor, bad ZR marker, expected %4.4x, got %4.4x\n", ZR_SOR, sor.zr);
    return 0 ;               // invalid sor, bad ZR marker
  }
  if(sor.rt != rt && rt != 0 ) {
    fprintf(stderr,"invalid sor, bad RT, expected %d, got %d\n", rt, sor.rt) ;
    return 0 ;        // invalid sor, not expected RT
  }
  rl = RSF_32_to_64(sor.rl) ;                    // record length
  return rl ;
}

typedef struct {                   // record trailer
  uint32_t rl[2] ;                 // upper[0], lower[1] 32 bits of record length
  uint32_t rt:8, rlm:16, zr:8 ;    // ZR_EOR, metadata length (32 bit units), record type
} end_of_record ;

#define EOR {{0, 0}, RT_DATA, 0,ZR_EOR }

// record length from end_of_record (0 if invalid)
// rt : expected record type (0 means anything is O.K.)
static inline uint64_t RSF_Rl_eor(end_of_record eor, int rt){
  uint64_t rl ;
  if(eor.zr != ZR_EOR) return 0 ;                // invalid eor, bad ZR marker
  if(eor.rt != rt && rt != 0 ) return 0 ;        // invalid eor, not expected RT
  rl = RSF_32_to_64(eor.rl) ;                    // record length
  return rl ;
}
//
//   structure of a segment (if compact segment. GAP is 0 bytes)
//   +-----+------+                   +------+-----------+-------+                             +-------+
//   | SOS | data | ................. | data | DIrectory | EOSlo | .. GAP if sparse segment .. | EOShi |
//   +-----+------+                   +------+-----------+-------+                             +-------+
//   <-- DIR  =  dir[0]  * 2**32 + dir[1] --->
//   <-- SEG  =  seg[0]  * 2**32 + seg[1] --------------->
//   <---SSEG =  sseg[0] * 2**32 + sseg[1] ------------------------------------------------------------>
//
//   sparse segment  : the EOS record is essentially split in two, and its record length may be very large
//                     record length = sizeof(end_of_segment_lo) + sizeof(end_of_segment_hi) + GAP
//   compact segment   the two EOS parts (EOSlo and EOShi) are adjacent and 
//                     record length = sizeof(end_of_segment_lo) + sizeof(end_of_segment_hi)
//
typedef struct{           // start of segment record, matched by a corresponding end of segment record
  start_of_record head ;  // rt=3
  unsigned char sig1[8] ; // RSF marker + application marker ('RSF0cccc) where cccc is 4 character application signature
  uint32_t sign ;         // 0xDEADBEEF hex signature for start_of_segment
  uint32_t meta_dim ;     // directory entry metadata size (uint32_t units)
  uint32_t seg[2] ;       // upper[0], lower[1] 32 bits of segment size (bytes) (excluding EOS record)
  uint32_t sseg[2] ;      // upper[0], lower[1] 32 bits of segment size (bytes) (including EOS record)
  uint32_t vdir[2] ;      // upper[0], lower[1] 32 bits of variable directory record offset in segment (bytes)
  uint32_t vdirs[2] ;     // upper[0], lower[1] 32 bits of variable directory record size (bytes)
  end_of_record tail ;    // rt=3
} start_of_segment ;

// record length of start of segment
static inline uint64_t RSF_Rl_sos(start_of_segment sos){
  uint64_t rl1 = RSF_Rl_sor(sos.head, RT_SOS) ;
  uint64_t rl2 = RSF_Rl_eor(sos.tail, RT_SOS) ;
  if(rl1 != rl2) return 0 ;
  return rl1 ;
}

#define SOS { {RT_SOS, 0, ZR_SOR, {0, sizeof(start_of_segment)}},  \
              {'R','S','F','0','<','-','-','>'} , 0xDEADBEEF, 0, {0, 0}, {0, 0}, {0, 0}, {0, 0}, \
              {{0, sizeof(start_of_segment)}, RT_SOS, 0, ZR_EOR} }

typedef struct{           // head part of end_of_segment record (low address in file)
  start_of_record head ;  // rt=4
  uint32_t sign ;         // 0xBEBEFADA hex signature for end_of_segment_lo
} end_of_segment_lo ;

#define EOSLO { {RT_EOS, 0, ZR_SOR, {0, sizeof(end_of_segment_lo)+sizeof(end_of_segment_hi)}}, 0xBEBEFADA }

typedef struct{           // tail part of end_of_segment record (high address in file)
  uint32_t sign ;         // 0xCAFEFADE hex signature for end_of_segment_hi
  uint32_t meta_dim ;     // directory entry metadata size (uint32_t units)
  uint32_t seg[2] ;       // upper[0], lower[1] 32 bits of segment size (bytes)
  uint32_t sseg[2] ;      // upper[0], lower[1] 32 bits of sparse segment size (bytes) (0 if not sparse file)
  uint32_t vdir[2] ;      // upper[0], lower[1] 32 bits of variable directory record offset in segment (bytes)
  uint32_t vdirs[2] ;     // upper[0], lower[1] 32 bits of variable directory record size (bytes)
  end_of_record tail ;    // rt=4
} end_of_segment_hi ;

// record length of end of segment
static inline uint64_t RSF_Rl_eosl(end_of_segment_lo eosl){
  return RSF_Rl_sor(eosl.head, RT_EOS) ;
}
static inline uint64_t RSF_Rl_eosh(end_of_segment_hi eosh){
  return RSF_Rl_eor(eosh.tail, RT_EOS) ;
}
static inline uint64_t RSF_Rl_eos(end_of_segment_lo eosl, end_of_segment_hi eosh){
  uint64_t rl1 = RSF_Rl_eosl(eosl) ;
  uint64_t rl2 = RSF_Rl_eosh(eosh) ;
  if(rl1 != rl2) return 0 ;
  return rl1 ;
}

#define EOSHI { 0xCAFEFADE, 0, {0, 0}, {0, 0}, {0, 0}, {0, 0}, \
              {{0, sizeof(end_of_segment_lo)+sizeof(end_of_segment_hi)}, RT_EOS, 0, ZR_EOR} }

typedef struct{           // compact end of segment (non sparse file)
  end_of_segment_lo l ;   // head part of end_of_segment record
  end_of_segment_hi h ;   // tail part of end_of_segment record
} end_of_segment ;

#define EOS { EOSLO , EOSHI }

// memory directory entries are stored in a chain of directory blocks
// the size of entries[] will be DIR_BLOCK_SIZE
// the size of a block will be sizeof(directory_block) + DIR_BLOCK_SIZE
// pointer for entry associated with record N will be found in RSF_file->vdir[N]
// if array RSF_file->vdir is too small, it is reallocated in increments of DIR_SLOTS_INCREMENT
typedef struct directory_block directory_block ;
#define DIR_BLOCK_SIZE 512
#define DIR_SLOTS_INCREMENT 8
struct directory_block{
  directory_block *next ;
  uint8_t *cur ;
  uint8_t *top ;
  uint8_t entries[] ;
} ;

// directory metadata length : upper 16 bits if non zero, lower 16 bits if upper 16 are zero
#define DIR_ML(DRML) ( ((DRML) >> 16) == 0 ? (DRML) : ((DRML) >> 16) )
// record metadata length : lower 16 bits
#define REC_ML(DRML) ( (DRML) & 0xFFFF)
// build composite ml field 
#define DRML_32(ml_dir, ml_rec)  ( ( (ml_dir) << 16 ) | ( (ml_rec) & 0xFFFF ) )
// propagate lower 16 bits into upper 16 bits if upper 16 are zero
#define DRML_FIX(ML) ( ((ML) >> 16) == 0 ? ((ML) <<16) | (ML) : (ML) )

typedef struct{           // directory entry (both file and memory)
  uint32_t wa[2] ;        // upper[0], lower[1] 32 bits of offset in segment (or file)
  uint32_t rl[2] ;        // upper[0], lower[1] 32 bits of record length (bytes)
  uint32_t ml ;           // upper 16 bits directory metadata length, lower 16 record metadata length
  uint32_t meta[] ;       // open array for metadata
} vdir_entry ;

typedef struct{              // directory record to be written to file
  start_of_record sor ;      // start of record
  uint32_t entries_nused ;   // number of directory entries used
  uint32_t meta_dim ;        // size of a directory entry metadata (in 32 bit units)
  vdir_entry entry[] ;       // open array of directory entries
//end_of_record eor          // end of record, after last entry, at &entry[entries_nused]
} disk_vdir ;
#define DISK_VDIR_BASE_SIZE  ( sizeof(disk_vdir) + sizeof(end_of_record) )

typedef void * pointer ;

typedef struct RSF_File RSF_File ;

// for RSF_File.last_op
#define OP_NONE  0
#define OP_READ  1
#define OP_WRITE 2

struct RSF_File{                 // internal (in memory) structure for access to Random Segmented Files
  uint32_t version ;             // version identifier
  int32_t  fd ;                  // OS file descriptor (-1 if invalid)
  RSF_File *next ;               // pointer to next file if "linked" (NULL if not linked)
  char *name ;                   // file name (canonicalized absolute path name)
  RSF_Match_fn *matchfn ;        // pointer to metadata matching function
//   dir_page **pagetable ;         // directory page table (pointers to directory pages for this file)
  directory_block *dirblocks ;   // first "block" of directory data
  vdir_entry **vdir ;            // pointer to table of vdir_entry pointers (reallocated larger if it gets too small)
  uint64_t vdir_size ;           // total size of vdir entries (future size of directory record in file)
  uint64_t seg_base ;            // base address in file of the current active segment (0 if only one segment)
  uint64_t file_wa0 ;            // file address origin (normally 0) (used for file within file access)
  start_of_segment sos0 ;        // start of segment of first segment (as it was read from file)
  start_of_segment sos1 ;        // start of segment of active (new) (compact or sparse) segment
  end_of_segment eos1 ;          // end of segment of active (compact or sparse) segment
  uint64_t seg_max ;             // maximum address allowable in segment (0 means no limit) (ssegl if sparse file)
  off_t    size ;                // file size
  off_t    next_write ;          // file offset from beginning of file for next write operation ( -1 if not defined)
  off_t    cur_pos ;             // current file position from beginning of file ( -1 if not defined)
  uint32_t meta_dim ;            // directory entry metadata size (uint32_t units)
  uint32_t rec_class ;           // record class being writen (default : data class 1) (rightmost 24 bits only)
  uint32_t class_mask ;          // record class mask (for scan/read/...) (by default all ones)
  uint32_t dir_read ;            // number of entries read from file directory 
  uint32_t vdir_slots ;          // current size of vdir[] table of pointers to directory entries
  uint32_t vdir_used ;           // number of used pointers in vdir[] table
  int32_t  slot ;                // file slot number of file (-1 if invalid)
  uint32_t nwritten ;            // number of records written (useful when closing after write)
  int32_t  lock ;                // used to lock the file for thread safety
  uint16_t mode ;                // file mode (RO/RW/AP/...)
  uint8_t isnew ;                // new segment indicator
  uint8_t last_op ;              // last operation (1 = read) (2 = write) (0 = unknown/invalid)
} ;

// NOTE : explicit_bzero not available everywhere
static inline void RSF_File_init(RSF_File *fp){  // initialize a new RSF_File structure
//   explicit_bzero(fp, sizeof(RSF_File)) ;  // this will make a few of the following statements redundant
  bzero(fp, sizeof(RSF_File)) ;  // this will make a few of the following statements redundant
  fp->version    = RSF_VERSION ;
  fp->fd         = -1 ;
//   fp->next       = NULL ;
//   fp->name       = NULL ;
  fp->matchfn    = RSF_Default_match ;
// //   fp->pagetable  = NULL ;
//   fp->dirblocks  = NULL ;
//   fp->vdir  = NULL ;
//   fp->vdir_size  = 0 ;
//   fp->seg_base   =  0 ;
//   fp->file_wa0   =  0 ;
//   initialize sos0 here ( the explicit_bzero should do the job of initializing to invalid values)
//   initialize sos1 here ( the explicit_bzero should do the job of initializing to invalid values)
//   initialize eos1 here ( the explicit_bzero should do the job of initializing to invalid values)
//   fp->seg_max    =  0 ;  // redundant if sos is stored
//   fp->size       =  0 ;
  fp->next_write = -1 ;
  fp->cur_pos    = -1 ;
//   fp->meta_dim   =  0 ;
  fp->rec_class   =  RT_DATA_CLASS ;
  fp->class_mask  =  0xFFFFFFFFu ;
//   fp->dir_read  =  0 ;
//   fp->dir_slots  =  0 ;
//   fp->dir_used   =  0 ;
//   fp->vdir_slots  =  0 ;
//   fp->vdir_used   =  0 ;
  fp->slot       = -1 ;
//   fp->nwritten   =  0 ;
//   fp->lock       =  0 ;
//   fp->mode       =  0 ;
//   fp->isnew      =  0 ;
//   fp->last_op    =  0 ;
}

// timeout is in microseconds
static inline uint32_t RSF_Lock(RSF_File *fp, uint32_t id, uint32_t timeout){
  useconds_t usec = 100 ;                                        // 100 microseconds
  if(fp->lock == id) return 1 ;                                  // we already own the lock
  while(__sync_val_compare_and_swap(&(fp->lock), 0, id) != 0){   // wait until lock is free
    timeout = timeout - usec ;
    if(timeout <= 0) return 0 ;                                  // timeout
    usleep(usec) ;                                               // microsleep for usec microseconds
  }
  return 1 ;
}

static inline uint32_t RSF_Unlock(RSF_File *fp, uint32_t id){
  uint32_t old_id = fp->lock ;
  if(old_id == id) fp->lock = 0 ;
  return old_id == id ;            // 1 if successful, 0 if locked by some other code
}
