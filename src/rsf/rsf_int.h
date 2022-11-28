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

#include <rsf/rsf.h>

int32_t RSF_Switch_sparse_segment(RSF_handle h, int64_t min_size) ;

// Random Segmented Files (RSF) internal (private) definitions and structures
//
//           file structure (N segments)
// +-----------+-----------+                  +-------------+
// | segment 0 | segment 1 |..................| segment N-1 |
// +-----------+-----------+                  +-------------+
//
//           segment structure
//           data and directory record(s) may be absent (empty segment)
//
//           open compact segment
// +-----+------+               +------+
// | SOS | data |-- more data --| data |
// +-----+------+               +------+
//           compact segment after close
// +-----+------+               +------+-----------+------+------+
// | SOS | data |-- more data --| data | directory | EOSl | EOSh |
// +-----+------+               +------+-----------+------+------+
//           open sparse segment
// +-----+------+               +------+                                                       +------+
// | SOS | data |-- more data --| data |......................gap..............................| EOSh |
// +-----+------+               +------+                                                       +------+
//           sparse segment after close (becomes compact segment + sparse segment)
// +-----+------+               +------+-----------+------+------+------+------+               +------+
// | SOS | data |-- more data --| data | directory | EOSl | EOSh | SOS  | EOSl |......gap......| EOSh |
// +-----+------+               +------+-----------+------+------+------+------+               +------+
// <-------------------------- new compact segment --------------x----- new sparse segment ----------->
// SOS       start of data record
// data      data record
// directory directory record
// EOSl      low part of end of segment record
// EOSh      high part of end of segment record (adjacent to EOSl in compact segments, disjoint from it in sparse segments)
// gap       unpopulated zone in file address space (sparse segment)

// generic structure of a record
//   ZR    RLM    RT   RL[0]   RL[1]   LMETA <------ record specific part -------> RL[0]   RL[1]   ZR    RLM    RT
// +----+-------+----+-------+-------+-------+-----------------------------------+-------+-------+----+-------+----+
// |    |       |    |       |       |       |            PAYLOAD                |       |       |    |       |    |
// +----+-------+----+-------+-------+-------+-----------------------------------+-------+-------+----+-------+----+
// data record
// +----+-------+----+-------+-------+-------+----------+------------------------+-------+-------+----+-------+----+
// |    |       |    |       |       |       |  RMETA   |       DATA             |       |       |    |       |    |
// +----+-------+----+-------+-------+-------+----------+------------------------+-------+-------+----+-------+----+
// data record with data map
// +----+-------+----+-------+-------+-------+----------+------+-----------------+-------+-------+----+-------+----+
// |    |       |    |       |       |       |  RMETA   | DMAP |      DATA       |       |       |    |       |    |
// +----+-------+----+-------+-------+-------+----------+------+-----------------+-------+-------+----+-------+----+
// directory record
// +----+-------+----+-------+-------+-------+-----------------------------------+-------+-------+----+-------+----+
// |    |       |    |       |       |       |             DIRECTORY             |       |       |    |       |    |
// +----+-------+----+-------+-------+-------+-----------------------------------+-------+-------+----+-------+----+
// <---------- start of record (SOR) -------->                                   <------- end of record (EOR) ----->
//   8     16     8     32      32    16/8/8   RLM x 32                              32      32      8    16     8   (# of bits)
// <--------------------------------------------  RL[0] * 2**32 + RL[1] bytes  ------------------------------------>
// ZR    : marker (0xFE for SOR, 0xFF for EOR)
// RLM   : length of in record metadata in 32 bit units (data record only)
//         RLM in a data record MAY be larger than directory length of metadata (RLMD)
//         (secondary metadata not in directory)
//       : open-for-write flag (Start of segment record) (0 if not open for write)
//       : undefined (End of segment or other record)
// LMETA : record header metadata, RLMD, UBC, DUL
//         RLMD (16 bits) directory metadata length (data records) (RLMD <= RLM)
//              minimum directory metadata length (Start of segment or directory records)
//              undefined (other records)
//         UBC  ( 8 bits) Unused Bit Count (RL is always a multiple of 4 Bytes)
//         DUL  ( 8 bits) Data Unit Length (0/1/2/4/8 bytes) (used for Endianness management)
// RT    : record type (normally 0 -> 0x80)
// RL    : record length = RL[0] * 2**32 + RL[1] bytes (ALWAYS a multiple of 4 bytes)
// RMETA : array of 32 bit items (data record metadata) (the first RSF_META_RESERVED elements in RMETA have a special meaning)
//         meta[0] contains 8 bit record type (RT) and 24 bit record class
//         meta[1] special meaning ? == data map size ?
//         the record metadata may be longer than the directory metadata for a given record
//         record metadata == directory metadata followed by extra metadata information
//         the extra information is only available after the record has been (possibly partially) read
// DATA  : sequence of 8/16/32/64 bit items (length of items will be found in DUL)
// DMAP  : data map (optional, length of DMAP in meta[1] ? )
//         sequence of 32/64 bit elements in sections (grid coordinates / missing data / chunk map)
//
// some part of the PAYLOAD may be "virtual" (uses address space but without anything written)
// (sparse records like a sparse segment with "split" end of segment record)
// (see Linux/UNIX sparse files)
//
// record metadata = directory metadata followed by (optional) extra metadata
//
// for endianness management purposes, these files are mostly composed of 32 bit items, 
//   but 8/16/32/64 data items may be found in the payload
// some 64 bit quantities in control records are represented as a pair of 32 bit values, Most Significant Part first
//
// record lengths are always multiples of 4 bytes
//
// the first segment (0) of a file MUST be a compact segment (CANNOT be a sparse segment)
// if the first created segment is sparse, segment 0 is a minimal compact segment (SOS + EOS)
//
// a segment can only be opened for write ONCE, and cannot be reopened to append data into it (a new segment MUST be created)
//
// a sparse segment becomes a compact segment when closed (a dummy sparse segment takes up the remaining space)
//
// the concatenation of 2 RSF files is a valid RSF file
//
// RSF files may be used as containers for other files (records with RT = RT_FILE)
//
// a "FUSE" operation can be used to consolidate all directories from a file into a single directory
//   to make future access more efficient
//
// while records are being added to a file (always in NEW segments) the "old" segments are still readable
//
// multiple sparse segments may be written into in parallel by multiple processes, if the underlying
//   filesystem supports advisory locks.
//
// the top of a sparse segment is aligned to a (power of 2 - 1) address for performance reasons
// when created, the bottom of a sparse segment is normally aligned to a power of 2 block boundary
// (exception : if the first useful segment is sparse, it goes on top of the dummy compact segment 0)

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

// align top of sparse segments to 1MB ( 2**20 )
#define SPARSE_BLOCK_ALIGN 20
#define ARBITRARY_OVERHEAD 4096
#define NEW_SEGMENT_OVERHEAD (ARBITRARY_OVERHEAD + 2 * sizeof(start_of_segment) + sizeof(end_of_segment))

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
  uint32_t rlmd:16, ubc:8, dul:8 ;
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
//   empty compact segment
//   +-----+-------+-------+
//   | SOS | EOSlo | EOShi |
//   +-----+-------+-------+
//   open (active) compact segment
//   +-----+------+                   +------+
//   | SOS | data | ................. | data |
//   +-----+------+                   +------+
//
//   empty sparse segment
//   +-----+-------+            +-------+
//   | SOS | EOSlo | .. GAP  .. | EOShi |
//   +-----+-------+            +-------+
//   open (active) sparse segment
//   +-----+------+                   +------+            +-------+
//   | SOS | data | ................. | data | .. GAP  .. | EOShi |
//   +-----+------+                   +------+            +-------+
//
//   closed segment (if original segment was compact, the sparse part is absent)
//   +-----+------+                   +------+-----------+-------+-------+-----+-------+            +-------+
//   | SOS | data | ................. | data | DIrectory | EOSlo | EOShi | SOS | EOSlo | .. GAP  .. | EOShi |
//   +-----+------+                   +------+-----------+-------+-------+-----+-------+            +-------+
//   <-- VDIR =  vdir[0] * 2**32 + vdir[1] -->
//                                           <-- VDIRS --> =  vdirs[0] * 2**32 + vdirs[1]
//   <-- SEG  =  seg[0]  * 2**32 + seg[1] --------------->
//   <-- SSEG =  sseg[0] * 2**32 + sseg[1] ------------------------------x-------- new sparse segment ------->
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
  uint32_t seg[2] ;       // upper[0], lower[1] 32 bits of segment size (bytes) (excluding EOS record)
                          // seg = 0 for a sparse segment
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
              {'R','S','F','0','<','-','-','>'} , 0xDEADBEEF, {0, 0}, {0, 0}, {0, 0}, {0, 0}, \
              {{0, sizeof(start_of_segment)}, RT_SOS, 0, ZR_EOR} }

typedef struct{           // head part of end_of_segment record (low address in file)
  start_of_record head ;  // rt=4
  uint32_t sign ;         // 0xBEBEFADA hex signature for end_of_segment_lo
} end_of_segment_lo ;

#define EOSLO { {RT_EOS, 0, ZR_SOR, {0, sizeof(end_of_segment_lo)+sizeof(end_of_segment_hi)}}, 0xBEBEFADA }

typedef struct{           // tail part of end_of_segment record (high address in file)
  uint32_t sign ;         // 0xCAFEFADE hex signature for end_of_segment_hi
  uint32_t seg[2] ;       // upper[0], lower[1] 32 bits of segment size (bytes)
                          // seg = 0 for a sparse segment
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

#define EOSHI { 0xCAFEFADE, {0, 0}, {0, 0}, {0, 0}, {0, 0}, \
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

#if defined(DEBUG)
#define DIR_BLOCK_SIZE 512
#define DIR_SLOTS_INCREMENT 8
static int32_t debug = DEBUG ;
#else
#define DIR_BLOCK_SIZE 131072
#define DIR_SLOTS_INCREMENT 512
static int32_t debug = 0 ;
#endif

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
// #define DRML_FIX(ML) ( ((ML) >> 16) == 0 ? ((ML) <<16) | (ML) : (ML) )

typedef struct{           // directory entry (both file and memory)
  uint32_t wa[2] ;        // upper[0], lower[1] 32 bits of offset in segment (or file)
  uint32_t rl[2] ;        // upper[0], lower[1] 32 bits of record length (bytes)
  uint32_t ml ;           // upper 16 bits directory metadata length, lower 16 record metadata length
  uint32_t dul:8, reserved:24 ;  // data element length (use part of "reserved" for data map length ?)
  uint32_t meta[] ;       // open array for metadata    (put data map length in meta[1] ?)
} vdir_entry ;

typedef struct{              // directory record to be written to file
  start_of_record sor ;      // start of record
  uint32_t entries ;         // number of directory entries used
  vdir_entry entry[] ;       // open array of directory entries
//end_of_record eor          // end of record, after last entry, at &entry[entries]
} disk_vdir ;
#define DISK_VDIR_BASE_SIZE  ( sizeof(disk_vdir) + sizeof(end_of_record) )

typedef struct{
  uint64_t base ;
  uint64_t size ;
} sparse_entry ;

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
  directory_block *dirblocks ;   // first "block" of directory data
  vdir_entry **vdir ;            // pointer to table of vdir_entry pointers (reallocated larger if it gets too small)
  sparse_entry *sparse_segs ;    // pointer to table of sparse segments
  uint64_t vdir_size ;           // worst case of total size of vdir entries (future size of directory record in file)
  uint64_t seg_base ;            // base address in file of the current active segment (0 if only one segment)
  uint64_t file_wa0 ;            // file address origin (normally 0) (used for file within file access)
  start_of_segment sos0 ;        // start of segment of first segment (as it was read from file)
  start_of_segment sos1 ;        // start of segment of active (new) (compact or sparse) segment
  end_of_segment eos1 ;          // end of segment of active (compact or sparse) segment
  uint64_t seg_max ;             // maximum address allowable in segment (0 means no limit) (ssegl if sparse file)
  uint64_t seg_max_hint ;        // desired maximum address allowable in segment
  off_t    size ;                // file size
  off_t    next_write ;          // file offset from beginning of file for next write operation ( -1 if not defined)
  off_t    cur_pos ;             // current file position from beginning of file ( -1 if not defined)
  uint32_t rec_class ;           // record class being writen (default : data class 1) (rightmost 24 bits only)
  uint32_t class_mask ;          // record class mask (for scan/read/...) (by default all ones)
  uint32_t dir_read ;            // number of entries read from file directory 
  uint32_t vdir_slots ;          // current size of vdir[] table of pointers to directory entries
  uint32_t vdir_used ;           // number of used pointers in vdir[] table
  uint32_t sparse_used ;         // number of used entries in sparse_segments table
  uint32_t sparse_size ;         // size of sparse_segments table
  int32_t  slot ;                // file slot number of file (-1 if invalid)
  uint32_t nwritten ;            // number of records written (useful when closing after write)
  int32_t  lock ;                // used to lock the file for thread safety
  int32_t iun ;                  // eventual Fortran file number
  uint16_t rec_meta ;            // record metadata size (uint32_t units)
  uint16_t dir_meta ;            // directory entry metadata size (uint32_t units)
  uint16_t mode ;                // file mode (RO/RW/AP/...)
  uint8_t isnew ;                // new segment indicator
  uint8_t last_op ;              // last operation (1 = read) (2 = write) (0 = unknown/invalid)
  uint8_t exclusive ;            // RW in exclusive mode if 1
} ;

// NOTE : explicit_bzero not available everywhere
static inline void RSF_File_init(RSF_File *fp){  // initialize a new RSF_File structure
//   explicit_bzero(fp, sizeof(RSF_File)) ;  // this will make a few of the following statements redundant
  bzero(fp, sizeof(RSF_File)) ;  // this will make a few of the following statements redundant
  fp->version    = RSF_VERSION ;
  fp->fd         = -1 ;
//   fp->next       = NULL ;
//   fp->name       = NULL ;
//   fp->matchfn    = RSF_Default_match ;
//   fp->dirblocks  = NULL ;
//   fp->vdir  = NULL ;
//   fp->sparse_segs  = NULL ;
//   fp->vdir_size  = 0 ;
//   fp->seg_base   =  0 ;
//   fp->file_wa0   =  0 ;
//   initialize sos0 here ( the explicit_bzero should do the job of initializing to invalid values)
//   initialize sos1 here ( the explicit_bzero should do the job of initializing to invalid values)
//   initialize eos1 here ( the explicit_bzero should do the job of initializing to invalid values)
//   fp->seg_max    =  0 ;    // redundant if sos is stored
//   fp->seg_max_hint =  0 ;  // redundant if sos is stored
//   fp->size       =  0 ;
  fp->next_write = -1 ;
  fp->cur_pos    = -1 ;
  fp->rec_class   =  RT_DATA_CLASS ;
  fp->class_mask  =  0xFFFFFFFFu ;
//   fp->dir_read  =  0 ;
//   fp->dir_slots  =  0 ;
//   fp->dir_used   =  0 ;
//   fp->vdir_slots  =  0 ;
//   fp->vdir_used   =  0 ;
//   fp->sparse_used   =  0 ;
//   fp->sparse_size   =  0 ;
  fp->slot       = -1 ;
//   fp->nwritten   =  0 ;
//   fp->lock       =  0 ;
//   fp->iun       =  0 ;
//   fp->rec_meta       =  0 ;
//   fp->dir_meta       =  0 ;
//   fp->mode       =  0 ;
//   fp->isnew      =  0 ;
//   fp->last_op    =  0 ;
//   fp->exclusive    =  0 ;
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

// round sizes up to a multiple of 4
static inline uint64_t RSF_Round_size(size_t data_size){
  data_size += 3 ;
  data_size >>= 2 ;
  data_size <<= 2 ;
//   if(data_size & 0x3L) {    // not a multiple of 4
//     data_size |= 0x3L ;     // round up to a multiple of 4
//     data_size++ ;
//   }
  return data_size ;
}

// size of rsf record
// rec_meta  : number of metadata elements in record
// data_size : data payload in bytes

static inline uint64_t RSF_Record_size(uint32_t rec_meta, size_t data_size){
  int64_t record_size ;

  record_size = sizeof(start_of_record) +             // sor
                sizeof(uint32_t) * rec_meta +         // record metadata
                RSF_Round_size(data_size) +           // data
                sizeof(end_of_record) ;               // eor
  record_size = RSF_Round_size(record_size) ;         // rounded up size
  return record_size ;
}
