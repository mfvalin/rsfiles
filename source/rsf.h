#if 0
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

// Random Segmented Files PUBLIC interface
// RSF_RO implies that the file MUST exist
// RSF_RW implies create file if it does not exist
// RSF_AP implies that the file MUST exist and will be written into (implies RSF_RW)
// RSF_NSEG means that the file will be mostly "write" (only new sparse segment will be accessible)
// RSF_PSEG means parallel segment mode (mostly write, read from local segment only)
// RSF_FUSE means consolidate segments into ONE (ignored if RSF_RW not set or new file)
// otherwise the last segment gets extended and the other segments remain untouched

// RSF_NSEG and RSF_PSEG : deferred implementation
#define RSF_NSEG    16
#define RSF_PSEG    32

// META_RESERVED : number of metadata items reserved for internal use
// meta[0] : used for record class mask
#endif

#if ! defined(RSF_VERSION)

#define RSF_VERSION_STRING "1.0.0"
#define RSF_VERSION 10000
#define RSF_RO       2
#define RSF_RW       4
#define RSF_AP       8
#define RSF_FUSE  1024

#define RSF_META_RESERVED 1

#define RT_NULL    0
#define RT_DATA    1
#define RT_DIR     2
#define RT_SOS     3
#define RT_EOS     4
#define RT_XDAT    5
#define RT_VDIR    6
#define RT_FILE    7
#define RT_DEL  0x80

#define RT_DATA_CLASS 1
#define RT_FILE_CLASS 0x80000

#if defined(IN_FORTRAN_CODE)

  type, BIND(C) :: RSF_handle
    private
    type(C_PTR) :: p
  end type

  type, BIND(C) :: RSF_record         ! not a totally honest description
    private                           ! MUST REFLECT EXACTLY C struct RSF_record (see below)
    type(C_PTR) :: sor                ! pointer to start of record descriptor (not used by Fortran)
    type(C_PTR) :: meta               ! pointer to integer metadata array
    type(C_PTR) :: data               ! pointer to start of integer data array
    type(C_PTR) :: eor                ! pointer to end of record descriptor (not used by Fortran)
    integer(C_INT64_T) :: meta_size   ! metadata size in 32 bit units (max 0xFFFF)
    integer(C_INT64_T) :: data_size   ! data payload size in bytes (may remain 0 in unmanaged records)
    integer(C_INT64_T) :: max_data    ! maximum data payload size in bytes
    integer(C_INT64_T) :: rsz         ! allocated size of RSF_record
    ! dynamic data array follows, see C struct
  end type

  type, BIND(C) :: RSF_record_handle
    private
    type(C_PTR) :: record      ! pointer to RSF_record (see above)
    !  type(RSF_record_handle)   :: h
    !  type(RSF_record), pointer :: r
    !  call C_F_POINTER(h%record, r)
    !  r%meta, r%data, r%meta_size, r%data_size, etc ... now accessible in module procedures
  end type
#else

#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/resource.h>

typedef struct{   // this struct only contains a pointer to the actual full control structure
  void *p ;
} RSF_handle ;

typedef struct{
  void     *sor ;      // start of record address ( RSF_record.d )
  uint32_t *meta ;     // pointer to metadata array ( sor - sizeof(sor) )
  void     *data ;     // pointer to start of data payload array
  void     *eor ;      // end of record address ( (void *) RSF_record.d + max_data )
  uint64_t meta_size ; // metadata size in uint32_t units (max 0xFFFF)
  uint64_t data_size ; // actual data size in bytes (may remain 0 in unmanaged records)
  uint64_t max_data ;  // maximum data payload size in bytes
  int64_t rsz ;        // allocated size of RSF_record
  uint8_t  d[] ;       // dynamic data array (bytes)
} RSF_record ;

// typedef struct{   // this struct only contains a pointer to the actual composite record
//   void *p ;
// } RSF_record_handle ;

// metadata matching function (normally supplied by application)
typedef int32_t RSF_Match_fn(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int ncrit, int nmeta) ;

#endif

#if defined(IN_FORTRAN_CODE)
interface
#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Default_match(criteria, meta, mask, ncrit, nmeta) result(status) bind(C,name='RSF_Default_match')
    import :: C_INT32_T
    implicit none
    integer(C_INT32_T), intent(IN), dimension(*) :: criteria, meta, mask
    integer(C_INT32_T), intent(IN), value :: ncrit, nmeta
    integer(C_INT32_T) :: status
  end function RSF_Default_match

  function RSF_Base_match(criteria, meta, mask, ncrit, nmeta) result(status) bind(C,name='RSF_Base_match')
    import :: C_INT32_T
    implicit none
    integer(C_INT32_T), intent(IN), dimension(*) :: criteria, meta, mask
    integer(C_INT32_T), intent(IN), value :: ncrit, nmeta
    integer(C_INT32_T) :: status
  end function RSF_Base_match
#else
  int32_t RSF_Default_match(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int ncrit, int nmeta) ;
  int32_t RSF_Base_match(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int ncrit, int nmeta) ;  // ignores mask
#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Open_file(fname, mode, meta_dim, appl, segsize) result(handle) bind(C,name='RSF_Open_file')
    import :: RSF_handle, C_CHAR, C_INT32_T, C_INT64_T
    implicit none
    character(C_CHAR), intent(IN), dimension(*) :: fname
    integer(C_INT32_T), intent(IN), value :: mode
    integer(C_INT32_T), intent(INOUT) :: meta_dim
    character(C_CHAR), intent(IN), dimension(4) :: appl
    integer(C_INT64_T), intent(IN) :: segsize
    type(RSF_handle) :: handle
  end function RSF_Open_file
#else
  RSF_handle RSF_Open_file(char *fname, int32_t mode, int32_t *meta_dim, char *appl, int64_t *segsize);
#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Lookup(handle, key0, criteria, mask) result(key) bind(C,name='RSF_Lookup')
    import :: RSF_handle, C_INT32_T, C_INT64_T
    implicit none
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT64_T), intent(IN), value :: key0
    integer(C_INT32_T), intent(IN), dimension(*) :: criteria, mask
    integer(C_INT64_T) :: key
  end function RSF_Lookup
#else
  int64_t RSF_Lookup(RSF_handle h, int64_t key0, uint32_t *criteria, uint32_t *mask) ;
#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Get_record(handle, key) result(rh) bind(C,name='RSF_Get_record')
    import :: RSF_handle, C_INT32_T, C_INT64_T, RSF_record_handle
    implicit none
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT64_T), intent(IN), value :: key
    type(RSF_record_handle) :: rh
  end function RSF_Get_record
#else
  RSF_record *RSF_Get_record(RSF_handle h, int64_t key) ;
#endif

#if defined(IN_FORTRAN_CODE)
  end interface

  interface RSF_Put   ! generic put interface
  function RSF_Put_data(handle, meta, meta_size, data, data_size) result(key) bind(C,name='RSF_Put_data')
    import :: RSF_handle, C_INT32_T, C_PTR, C_SIZE_T, C_INT64_T
    implicit none
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT32_T), intent(IN), dimension(*) :: meta
    type(C_PTR), value :: data
    integer(C_INT32_T), intent(IN), value :: meta_size
    integer(C_SIZE_T), intent(IN), value :: data_size
    integer(C_INT64_T) :: key
  end function RSF_Put_data

  function RSF_Put_record(handle, r, data_size) result(key) bind(C,name='RSF_Put_record')
    import :: RSF_handle, C_INT64_T, C_SIZE_T, RSF_record
    implicit none
    type(RSF_handle), intent(IN), value :: handle
    type(RSF_record), intent(IN) :: r
    integer(C_SIZE_T), intent(IN), value :: data_size
    integer(C_INT64_T) :: key
  end function RSF_Put_record
  end interface

  interface
#else
  int64_t RSF_Put_data(RSF_handle h, uint32_t *meta, uint32_t meta_size, void *data, size_t data_size) ;
  int64_t RSF_Put_record(RSF_handle h, RSF_record *record, size_t data_size) ;
#endif

#if defined(IN_FORTRAN_CODE)
#else
  int64_t RSF_Put_file(RSF_handle h, char *filename, uint32_t *meta, uint32_t meta_size) ;
  int64_t RSF_Get_file(RSF_handle h, int64_t key, char *alias, uint32_t **meta, uint32_t *meta_size) ;
#endif
#if defined(IN_FORTRAN_CODE)
  function RSF_Get_record_meta(handle, key, metasize, datasize) result(p) bind(C,name='RSF_Get_record_meta')
    import :: RSF_handle, C_INT32_T, C_INT64_T, C_PTR
    implicit none
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT64_T), intent(IN), value :: key
    integer(C_INT32_T), intent(OUT) :: metasize
    integer(C_INT64_T), intent(OUT) :: datasize
    type(C_PTR) :: p
  end function RSF_Get_record_meta
#else
  void *RSF_Get_record_meta(RSF_handle h, int64_t key, int32_t *metasize, uint64_t *datasize) ;
#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Close_file(handle) result (status) bind(C,name='RSF_Close_file')
    import :: RSF_handle, C_INT32_T
    implicit none
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT32_T) :: status
  end function RSF_Close_file
#else
  int32_t RSF_Close_file(RSF_handle h) ;
#endif

#if defined(IN_FORTRAN_CODE)
  subroutine RSF_Dump(name, verbose) bind(C,name='RSF_Dump')
    import :: C_CHAR
    implicit none
    character(C_CHAR), dimension(*), intent(IN) :: name
    integer, intent(IN), value :: verbose
  end subroutine RSF_Dump
#else
  void RSF_Dump(char *name, int verbose) ;
#endif

#if defined(IN_FORTRAN_CODE)
  subroutine RSF_Dump_dir(handle) bind(C,name='RSF_Dump_dir')
    import :: RSF_handle, C_INT32_T
    implicit none
    type(RSF_handle), intent(IN), value :: handle
  end subroutine RSF_Dump_dir
#else
  void RSF_Dump_dir(RSF_handle h) ;
#endif

#if defined(IN_FORTRAN_CODE)
  subroutine RSF_Dump_vdir(handle) bind(C,name='RSF_Dump_vdir')
    import :: RSF_handle, C_INT32_T
    implicit none
    type(RSF_handle), intent(IN), value :: handle
  end subroutine RSF_Dump_vdir
#else
  void RSF_Dump_vdir(RSF_handle h) ;
#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Valid_handle(handle) result (status) bind(C,name='RSF_Valid_handle')
    import :: RSF_handle, C_INT32_T
    implicit none
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT32_T) :: status
  end function RSF_Valid_handle
#else
  int32_t RSF_Valid_handle(RSF_handle h) ;
#endif

#if defined(IN_FORTRAN_CODE)
! allocate a new record handle (Fortran)
  function RSF_New_record_handle(handle, max_meta, max_data, t, szt) result(rh) bind(C,name='RSF_New_record')
    import :: RSF_handle, C_INT32_t, C_INT64_T, RSF_record_handle, C_PTR
    implicit none
    type(RSF_handle), intent(IN), value :: handle
    type(C_PTR), value :: t
    integer(C_INT32_t), intent(IN), value :: max_meta
    integer(C_INT64_T), intent(IN), value :: max_data, szt
    type(RSF_record_handle) :: rh
  end function RSF_New_record_handle

! free the space allocated to that record
  subroutine RSF_Free_record_handle(rh) bind(C,name='RSF_Free_record')
    import :: C_INT32_T, RSF_record_handle
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
  end subroutine RSF_Free_record_handle

! get free space in record
  function RSF_Record_free_space(rh) result(s) bind(C,name='RSF_Record_free_space')
    import :: RSF_record_handle, C_INT64_T
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    integer(C_INT64_T) :: s
  end function RSF_Record_free_space

! allocated size of record
  function RSF_Record_allocated(rh) result(s) bind(C,name='RSF_Record_allocated')
    import :: RSF_record_handle, C_INT64_T
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    integer(C_INT64_T) :: s
  end function RSF_Record_allocated

! get max payload space in record
  function RSF_Record_max_space(rh) result(s) bind(C,name='RSF_Record_max_space')
    import :: RSF_record_handle, C_INT64_T
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    integer(C_INT64_T) :: s
  end function RSF_Record_max_space

! get pointer to payload in record
  function RSF_Record_data(rh) result(p) bind(C,name='RSF_Record_data')
    import :: RSF_record_handle, C_PTR
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    type(C_PTR) :: p
  end function RSF_Record_data

! get used payload space in record
  function RSF_Record_data_size(rh) result(s) bind(C,name='RSF_Record_data_size')
    import :: RSF_record_handle, C_INT64_T
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    integer(C_INT64_T) :: s
  end function RSF_Record_data_size

! get pointer to metadata in record
  function RSF_Record_meta(rh) result(p) bind(C,name='RSF_Record_meta')
    import :: RSF_record_handle, C_PTR
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    type(C_PTR) :: p
  end function RSF_Record_meta

! get metadata size in record
  function RSF_Record_meta_size(rh) result(s) bind(C,name='RSF_Record_meta_size')
    import :: RSF_record_handle, C_INT32_T
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    integer(C_INT32_T) :: s
  end function RSF_Record_meta_size

! does this look like a valid record ?
  function RSF_Valid_record_handle(rh) result(s) bind(C,name='RSF_Valid_record')
    import :: RSF_record_handle, C_INT32_T
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    integer(C_INT32_T) :: s
  end function RSF_Valid_record_handle
#else
  RSF_record *RSF_New_record(RSF_handle h, int32_t max_meta, size_t max_data, void *t, int64_t szt) ;  // create pointer to a new allocated record (C)
  void RSF_Free_record(RSF_record * r) ;                       // free the space allocated to that record
  int32_t RSF_Valid_record(RSF_record *r) ;                    // does this look like a valid record ?
  int64_t RSF_Record_free_space(RSF_record *r) ;               // space available for more data in record allocated by RSF_New_record
  int64_t RSF_Record_allocated(RSF_record *r) ;                // allocated size of record allocated by RSF_New_record
  int64_t RSF_Record_max_space(RSF_record *r) ;                // maximum data payload size in record allocated by RSF_New_record
  void *RSF_Record_data(RSF_record *r) ;                       // pointer to data payload in record allocated by RSF_New_record
  uint64_t RSF_Record_data_size(RSF_record *r) ;               // size of data payload in record allocated by RSF_New_record
  void *RSF_Record_meta(RSF_record *r) ;                       // pointer to metadata in record allocated by RSF_New_record
  uint32_t RSF_Record_meta_size(RSF_record *r) ;               // size of metadata in record allocated by RSF_New_record
#endif


#if defined(IN_FORTRAN_CODE)
end interface
#endif

#endif
