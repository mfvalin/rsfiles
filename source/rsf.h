#if ! defined(IN_FORTRAN_CODE)
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
// RSF_SEG1 means consolidate segments into ONE (ignored if RSF_RW not set or new file)
// otherwise the last segment gets extended and the other segments remain untouched
#endif

#define RSF_VERSION 10000
#define RSF_RO    2
#define RSF_RW    4
#define RSF_AP    8
#define RSF_SEG1  1024

#if defined(IN_FORTRAN_CODE)

  type, BIND(C) :: RSF_handle
    private
    type(C_PTR) :: p
  end type
#else

#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/resource.h>

typedef struct{   // this struct only contains a pointer to the actual full control structure
  void *p ;
} RSF_handle ;

// metadata matching function (normally supplied by application)
typedef int32_t RSF_Match_fn(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int n) ;

#endif

#if defined(IN_FORTRAN_CODE)
interface
#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Default_match(criteria, meta, mask, n) result(status) bind(C,name='RSF_Default_match')
    import :: C_INT32_T
    integer(C_INT32_T), intent(IN), dimension(*) :: criteria, meta, mask
    integer(C_INT32_T), intent(IN), value :: n
    integer(C_INT32_T) :: status
  end function RSF_Default_match

  function RSF_Base_match(criteria, meta, mask, n) result(status) bind(C,name='RSF_Default_match')
    import :: C_INT32_T
    integer(C_INT32_T), intent(IN), dimension(*) :: criteria, meta, mask
    integer(C_INT32_T), intent(IN), value :: n
    integer(C_INT32_T) :: status
  end function RSF_Base_match
#else

int32_t RSF_Default_match(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int n) ;
int32_t RSF_Base_match(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int n) ;  // ignores mask

#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Open_file(fname, mode, meta_dim, appl, segsize) result(handle) bind(C,name='RSF_Open_file')
    import :: RSF_handle, C_CHAR, C_INT32_T, C_INT64_T
    character(C_CHAR), intent(IN), dimension(*) :: fname
    integer(C_INT32_T), intent(IN), value :: mode
    integer(C_INT32_T), intent(INOUT) :: meta_dim
    character(C_CHAR), intent(IN), dimension(4) :: appl
    integer(C_INT64_T), intent(IN), value :: segsize
    type(RSF_handle) :: handle
  end function RSF_Open_file
#else
RSF_handle RSF_Open_file(char *fname, int32_t mode, int32_t *meta_dim, char *appl, int64_t *segsize);
#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Lookup(handle, key0, criteria, mask) result(key) bind(C,name='RSF_Lookup')
    import :: RSF_handle, C_INT32_T, C_INT64_T
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT64_T), intent(IN), value :: key0
    integer(C_INT32_T), intent(IN), dimension(*) :: criteria, mask
    integer(C_INT64_T) :: key
  end function RSF_Lookup
#else
int64_t RSF_Lookup(RSF_handle h, int64_t key0, uint32_t *criteria, uint32_t *mask) ;
#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Get_record(handle, key, record, size, meta, metasize, data, datasize) result(p) bind(C,name='RSF_Get_record')
    import :: RSF_handle, C_INT32_T, C_INT64_T, C_PTR
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT64_T), intent(IN), value :: key
    type(C_PTR), intent(IN), value :: record
    integer(C_INT64_T), intent(IN), value :: size
    integer(C_INT32_T), intent(OUT) :: metasize
    integer(C_INT64_T), intent(OUT) :: datasize
    type(C_PTR), intent(OUT) :: meta, data
    type(C_PTR) :: p
  end function RSF_Get_record

  function RSF_Record_meta(handle, record, metasize) result(p) bind(C,name='RSF_Record_meta')
    import :: RSF_handle, C_INT32_T, C_PTR
    type(RSF_handle), intent(IN), value :: handle
    type(C_PTR), intent(IN), value :: record
    integer(C_INT32_T), intent(OUT) :: metasize
    type(C_PTR) :: p
  end function RSF_Record_meta

  function RSF_Record_data(handle, record, datasize) result(p) bind(C,name='RSF_Record_data')
    import :: RSF_handle, C_INT32_T, C_PTR, C_INT64_T
    type(RSF_handle), intent(IN), value :: handle
    type(C_PTR), intent(IN), value :: record
    integer(C_INT64_T), intent(OUT) :: datasize
    type(C_PTR) :: p
  end function RSF_Record_data
#else

void *RSF_Get_record(RSF_handle h, int64_t key, void *record, uint64_t size, void **meta, int32_t *metasize, void **data, uint64_t *datasize) ;
void *RSF_Record_meta(RSF_handle h, void *record, int32_t *metasize) ;
void *RSF_Record_data(RSF_handle h, void *record, int64_t *datasize) ;

#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Put(handle, meta, data, data_size) result(key) bind(C,name='RSF_Put')
    import :: RSF_handle, C_INT32_T, C_PTR, C_SIZE_T, C_INT64_T
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT32_T), intent(IN), dimension(*) :: meta
    type(C_PTR), value :: data
    integer(C_SIZE_T), intent(IN), value :: data_size
    integer(C_INT64_T) :: key
  end function RSF_Put
#else
int64_t RSF_Put(RSF_handle h, uint32_t *meta, void *data, size_t data_size) ;
#endif

#if defined(IN_FORTRAN_CODE)

  function RSF_Get_meta(handle, key, metasize, datasize) result(p) bind(C,name='RSF_Get_meta')
    import :: RSF_handle, C_INT32_T, C_INT64_T, C_PTR
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT64_T), intent(IN), value :: key
    integer(C_INT32_T), intent(OUT) :: metasize
    integer(C_INT64_T), intent(OUT) :: datasize
    type(C_PTR) :: p
  end function RSF_Get_meta

  function RSF_Get_record_meta(handle, key, metasize, datasize) result(p) bind(C,name='RSF_Get_record_meta')
    import :: RSF_handle, C_INT32_T, C_INT64_T, C_PTR
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT64_T), intent(IN), value :: key
    integer(C_INT32_T), intent(OUT) :: metasize
    integer(C_INT64_T), intent(OUT) :: datasize
    type(C_PTR) :: p
  end function RSF_Get_record_meta
#else
void        *RSF_Get_meta(RSF_handle h, int64_t key, int32_t *metasize, uint64_t *datasize) ;
void        *RSF_get_meta(RSF_handle h, int64_t key, int32_t *metasize, uint64_t *datasize) ;
void *RSF_Get_record_meta(RSF_handle h, int64_t key, int32_t *metasize, uint64_t *datasize) ;
#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Close_file(handle) result (status) bind(C,name='RSF_Close_file')
    import :: RSF_handle, C_INT32_T
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT32_T) :: status
  end function RSF_Close_file
#else
int32_t RSF_Close_file(RSF_handle h) ;
#endif

#if defined(IN_FORTRAN_CODE)
  subroutine RSF_Dump(name) bind(C,name='RSF_Dump')
    import :: C_CHAR
    character(C_CHAR), dimension(*), intent(IN) :: name
  end subroutine RSF_Dump
#else
void RSF_Dump(char *name) ;
#endif

#if defined(IN_FORTRAN_CODE)
  subroutine RSF_Dump_dir(handle) bind(C,name='RSF_Dump_dir')
    import :: RSF_handle, C_INT32_T
    type(RSF_handle), intent(IN), value :: handle
  end subroutine RSF_Dump_dir
#else
void RSF_Dump_dir(RSF_handle h) ;
#endif

#if defined(IN_FORTRAN_CODE)
  function RSF_Valid_handle(handle) result (status) bind(C,name='RSF_Valid_handle')
    import :: RSF_handle, C_INT32_T
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT32_T) :: status
  end function RSF_Valid_handle
#else
int32_t RSF_Valid_handle(RSF_handle h) ;
#endif

#if defined(IN_FORTRAN_CODE)
end interface
#endif
