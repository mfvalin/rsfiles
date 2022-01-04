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

// Random Segmented Files internal (private) definitions and structures
//
// for endianness purposes, these files are composed of 32 bit items
// 64 bit quantities are represented with a pair of 32 bit values, Most Significant Part first
// record/segment lengths are always multiples of 8 bytes

// record type rt values
// 0   : INVALID
// 1   : data record
// 2   : segment directory record
// 3   : start of segment record
// 4   : end of segment record
// 255 : deleted record
//
// rt:rlm:zr can be used for endianness detection, least significant byte (zr) ZR_xx, most significant byte (rt) RT_xx
// rt and zr can never be both 0
// the zr field is 0 for start_of_record and 0xFF for end_of_record
// max record length is 2**48 - 1 bytes (rl in bytes) (256 TBytes)

#define RT_NULL    0
#define RT_DATA    1
#define RT_DIR     2
#define RT_SOS     3
#define RT_EOS     4
#define RT_DEL  0x80

#define ZR_SOR  0XF0
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

//    structure of a record (array of 32 bit items)
//      ZR    RLM    RT   RL[0]   RL[1]                                       RL[0]   RL[1]   ZR    RLM    RT
//    +----+-------+----+-------+-------+----------+------------------------+-------+-------+----+-------+----+
//    |    |       |    |       |       | METADATA |       DATA             |       |       |    |       |    |
//    +----+-------+----+-------+-------+----------+------------------------+-------+-------+----+-------+----+
//      8     16     8     32      32     RLM x 32                             32      32      8    16     8   (# of bits)
//    <------------------------------------  RL[0] * 2**32 + RL[1] bytes  ------------------------------------>
//  ZR    : marker
//  RLM   : length of metadata in 32 bit units (data and directory records)
//        : open-for-write flag (Start of segment record) (0 if not open for write)
//        : undefined (End of segment or other record)
//  RT    : record type
//  RL    : record length = RL[0] * 2**32 + RL[1] bytes (ALWAYS a multiple of 4 bytes)

typedef struct {                   // record header
  uint32_t rt:8, rlm:16, zr:8 ;    // ZR_SOR, metadata length (32 bit units), record type
  uint32_t rl[2] ;                 // upper[0], lower[1] 32 bits of record length (bytes)
} start_of_record ;

#define SOR {RT_DATA, 0, ZR_SOR, {0, 0}}

// record length from start_of_record (0 if invalid)
// rt : expected record type (0 means anything is O.K.)
static inline uint64_t RSF_Rl_sor(start_of_record sor, int rt){
  uint64_t rl ;
  if(sor.zr != ZR_SOR ) return 0 ;               // invalid sor, bad ZR marker
  if(sor.rt != rt && rt != 0 ) return 0 ;        // invalid sor, not expected RT
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
  uint32_t dir[2] ;       // upper[0], lower[1] 32 bits of directory record offset in segment (bytes)
  uint32_t dirs[2] ;      // upper[0], lower[1] 32 bits of directory record size (bytes)
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

static start_of_segment sos0 = SOS ;

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
  uint32_t dir[2] ;       // upper[0], lower[1] 32 bits of directory record offset in segment (bytes)
  uint32_t dirs[2] ;      // upper[0], lower[1] 32 bits of directory record size (bytes)
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

#define EOSHI { 0xCAFEFADE, 0, {0, 0}, {0, 0}, {0, 0}, {0, 0}, {{0, sizeof(end_of_segment_lo)+sizeof(end_of_segment_hi)}, RT_EOS, 0, ZR_EOR} }

typedef struct{           // compact end of segment (non sparse file)
  end_of_segment_lo l ;   // head part of end_of_segment record
  end_of_segment_hi h ;   // tail part of end_of_segment record
} end_of_segment ;

#define EOS { EOSLO , EOSHI }

typedef struct{           // disk directory entry (one per record)
  uint32_t wa[2] ;        // upper[0], lower[1] 32 bits of offset in segment
  uint32_t rl[2] ;        // upper[0], lower[1] 32 bits of record length (bytes)
  uint32_t meta[] ;       // dynamic array for entry metadata (meta_dim elements)
} disk_dir_entry ;

typedef struct{              // directory record to be written on disk
  start_of_record sor ;      // start of record
  uint32_t entries_nused ;   // number of directory entries used
  uint32_t meta_dim ;        // size of a directory entry metadata (in 32 bit units)
  disk_dir_entry entry[] ;   // open array of directory entries
//end_of_record eor          // end of record, after last entry, at &entry[entries_nused]
} disk_directory ;

typedef struct{              // directory record to be written on disk
  start_of_record sor ;      // start of record
  uint32_t entries_nused ;   // number of directory entries used
  uint32_t meta_dim ;        // size of a directory entry metadata (in 32 bit units)
//   disk_dir_entry entry[] ;   // open array of directory entries, 0 entries in empty directory
  end_of_record eor ;        // end of record, after last entry, at &entry[entries_nused]
} empty_disk_directory ;

#define EMPTY_DIR { {RT_DIR, 0, ZR_SOR, {0, sizeof(empty_disk_directory)}},  \
                    0, 0, \
                    {{0, sizeof(empty_disk_directory)}, RT_DIR, 0, ZR_EOR} }

// size of an empty directory record
#define RL_EMPTY_DIR ( sizeof(disk_directory) + RL_EOR )

// the following 3 defines MUST BE KEPT CONSISTENT, DIR_PAGE_SIZE MUST BE A POWER OF 2, DIR_PAGE_SHFT is log2(DIR_PAGE_SIZE)
#define DIR_PAGE_SIZE 512
#define DIR_PAGE_MASK (DIR_PAGE_SIZE-1)
#define DIR_PAGE_SHFT 9

// by default a directory page table of DEFAULT_PAGE_TABLE is allocated when opening a file
#define DEFAULT_PAGE_TABLE 128

typedef struct{            // part of the in-core directory
  uint64_t wa ;            // offset from segment start
  uint64_t rl ;            // record length
} wa_rl ;

typedef struct {                 // directory page in core
  uint32_t nused ;               // number of entries in use
  uint32_t nmax ;                // max number of entries in page (normally DIR_PAGE_SIZE)
  wa_rl    warl[DIR_PAGE_SIZE] ; // file address + length for records
  uint32_t meta[] ;              // metadata [nmax, meta_dim] (allocated as [DIR_PAGE_SIZE, meta_dim] )
} dir_page ;

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
  dir_page **pagetable ;         // directory page table (pointers to directory pages for this file)
  uint64_t seg_base ;            // base address in file of the current active segment (0 if only one segment)
  start_of_segment sos0 ;        // start of segment of first segment (as it was read from file)
  start_of_segment sos1 ;        // start of segment of active (new) (compact or sparse) segment
  end_of_segment eos1 ;          // end of segment of active (compact or sparse) segment
  uint64_t seg_max ;             // maximum address allowable in segment (0 means no limit) (ssegl if sparse file)
  off_t    size ;                // file size
  off_t    next_write ;          // file offset from beginning of file for next write operation ( -1 if not defined)
  off_t    cur_pos ;             // current file position from beginning of file ( -1 if not defined)
  uint32_t meta_dim ;            // directory entry metadata size (uint32_t units)
  uint32_t dir_read ;            // number of entries read from file directory 
  uint32_t dir_slots ;           // max number of entries in directory (nb of directory pages * DIR_PAGE_SIZE)
  uint32_t dir_used ;            // number of directory entries in use (all pages belonging to this file/segment)
  int32_t  slot ;                // slot number of file (-1 if invalid)
  uint32_t nwritten ;            // number of records written (useful when closing after write)
  uint16_t isnew ;               // new segment indicator
  uint16_t last_op ;             // last operation (1 = read) (2 = write) (0 = unknown/invalid)
  uint16_t mode ;                // file mode (RO/RW/AP/...)
  int16_t  dirpages ;            // number of available directory pages (-1 if none )
  int16_t  curpage ;             // current page in use (-1 if not defined)
  int16_t  lastpage ;            // last directory page in use (-1 if not defined)
} ;

static inline void RSF_File_init(RSF_File *fp){  // initialize a new RSF_File structure
  explicit_bzero(fp, sizeof(RSF_File)) ;  // this will make a few of the following statements redundant
  fp->version    = RSF_VERSION ;
  fp->fd         = -1 ;
//   fp->next       = NULL ;
//   fp->name       = NULL ;
  fp->matchfn    = RSF_Default_match ;
  fp->pagetable  = NULL ;
//   fp->seg_base   =  0 ;
//   initialize sos0 here ( the explicit_bzero should do the job of initializing to invalid values)
//   initialize sos1 here ( the explicit_bzero should do the job of initializing to invalid values)
//   initialize eos1 here ( the explicit_bzero should do the job of initializing to invalid values)
//   fp->seg_max    =  0 ;  // redundant if sos is stored
//   fp->size       =  0 ;
  fp->next_write = -1 ;
  fp->cur_pos    = -1 ;
//   fp->meta_dim   =  0 ;
//   fp->dir_read  =  0 ;
//   fp->dir_slots  =  0 ;
//   fp->dir_used   =  0 ;
  fp->slot       = -1 ;
//   fp->nwritten   =  0 ;
//   fp->isnew      =  0 ;
//   fp->last_op    =  0 ;
//   fp->mode       =  0 ;
  fp->dirpages   = -1 ;
  fp->curpage    = -1 ;
  fp->lastpage   = -1 ;
}

static inline size_t RSF_Disk_dir_entry_size(RSF_File *fp){      // size of a file directory entry
  return ( sizeof(uint32_t)*fp->meta_dim + sizeof(disk_dir_entry) ) ;
}

static inline size_t RSF_Disk_dir_size(RSF_File *fp){      // size of the record containing the disk directory in file
  return ( sizeof(disk_directory) +                                                 // base size, includes SOR
           ( sizeof(uint32_t)*fp->meta_dim + sizeof(disk_dir_entry) ) * fp->dir_used +  // disk entry size * nb of records
           sizeof(end_of_record) ) ;                                                // end of record
}

static inline size_t RSF_Dir_page_size(RSF_File *fp){            // size of a directory page in memory
  return ( sizeof(dir_page) +                                    // base size, including warl table
           fp->meta_dim * sizeof(uint32_t) * DIR_PAGE_SIZE ) ;   // metadata for the page
}
