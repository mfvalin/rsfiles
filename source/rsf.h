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

#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/resource.h>

// Random Segmented Files PUBLIC interface
#define RSF_VERSION 10000

// RSF_RO implies that the file MUST exist
#define RSF_RO    2

// RSF_RW implies create file if it does not exist
#define RSF_RW    4

// RSF_AP implies that the file MUST exist and will be written into (implies RSF_RW)
#define RSF_AP    8

// RSF_SEG1 means consolidate segments into ONE (ignored if RSF_RW not set or new file)
// otherwise the last segment gets extended and the other segments remain untouched
#define RSF_SEG1  1024

typedef struct{   // this struct only contains a pointer to the actual full control structure
  void *p ;
} RSF_handle ;

// metadata matching function (normally supplied by application)
typedef int32_t RSF_Match_fn(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int n) ;

int32_t RSF_Default_match(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int n) ;

int32_t RSF_Base_match(uint32_t *criteria, uint32_t *meta, uint32_t *mask, int n) ;  // ignores mask

RSF_handle RSF_Open_file(char *fname, int32_t mode, int32_t *meta_dim, char *appl, int64_t *segsize);

int64_t RSF_Lookup(RSF_handle h, int64_t key0, uint32_t *criteria, uint32_t *mask) ;

void *RSF_Get_record(RSF_handle h, int64_t key, void *record, uint64_t size, void **meta, int32_t *metasize, void **data, uint64_t *datasize) ;
void *RSF_Record_meta(RSF_handle h, void *record, int32_t *metasize) ;
void *RSF_Record_data(RSF_handle h, void *record, int32_t *datasize) ;

void *RSF_Get_meta(RSF_handle h, int64_t key, int32_t *metasize, uint64_t *datasize) ;

int64_t RSF_Put(RSF_handle h, uint32_t *meta, void *data, size_t data_size) ;

int32_t RSF_Close_file(RSF_handle h) ;

void RSF_Dump(char *name) ;

void RSF_Dump_dir(RSF_handle h) ;

int32_t RSF_Valid_handle(RSF_handle h) ;
