#include <rsf.h>

#if defined(TEST1)
#define NDATA 10
#define NREC 3
#define META_SIZE 6
#define NFILES 4
int the_test(int argc, char **argv){
//   start_of_record sor = {0, 1, 4, 32767} ;
  int fd ;
  RSF_handle h ;
  int32_t meta_dim = META_SIZE ;
  uint32_t meta[META_SIZE] ;
  uint32_t mask[META_SIZE] ;
  uint32_t criteria[META_SIZE] ;
  int32_t data[NDATA+NREC*NFILES] ;
  size_t data_size ;
  int32_t i, j, k, ndata ;
  char *names[] = { "bad.rsf", "demo1.rsf", "demo2.rsf", "demo3.rsf", NULL };
  int32_t zero = 0 ;
  int64_t key0 ;
  int i0 = 0;
  int j0 ;
  int64_t keys[4096] ;
  uint32_t *dataptr, *metaptr ;
  int32_t meta_size ;

  bzero(mask, META_SIZE*sizeof(uint32_t)) ;  // search mask (ignore everything for match purposes)
  for(i=0 ; i<5 ; i++) fprintf(stderr,"%d %p",i, names[i]) ; fprintf(stderr,"\n");

  meta[0]           = 0xCAFEFADEu ;
  meta[META_SIZE-1] = 0xDEABDEEFu ;

  h = RSF_Open_file("tag-ada.rsf", RSF_RW, &zero, "DeMo", NULL);
  if( ! RSF_Valid_handle(h) ) {
    fprintf(stderr,"create with metadata length == 0 failed as expected\n") ;
  }else{
    fprintf(stderr,"ERROR: create with metadata length == 0 did not fail as expected\n") ;
  }

  fprintf(stderr,"=========== file creation test 1 ===========\n") ;
  for(k=0 ; k<4 ; k++){
//     fprintf(stderr,"before RSF_Open_file\n");
    fprintf(stderr,"before RSF_Open_file, k = %d, p = %s\n", k, names[k]) ;
    h = RSF_Open_file(names[k], 0, &meta_dim, "DeMo", NULL);
    fprintf(stderr,"after RSF_Open_file\n");
    if( ! RSF_Valid_handle(h) ) {
      fprintf(stderr,"ERROR: open failed for file '%s'\n", names[k]) ;
      continue ;
    }
//     fprintf(stderr,"before put\n");
    for(i = 0 ; i < NREC ; i++){
      fprintf(stderr,"writing record\n");
//       fprintf(stderr,"writing record %d\n",i) ;
      for(j=1 ; j < meta_dim-1 ; j++) {
        meta[j] = (j << 16) + i + (k << 8) ;
      }
      ndata = NDATA + i0 ;
      if(ndata > NDATA+NREC*NFILES){
        fprintf(stderr,"ERROR: data overflow ndata = %d, max allowed = %d\n", ndata, NDATA+NREC*NFILES);
        exit(1) ;
      }
      data_size = ndata * sizeof(data[0]) ;
      for(j=0 ; j < ndata    ; j++) {
        data[j] = j+i0 ;
      }
//       fprintf(stderr," : %9x %9x %9x\n",data[0], data[ndata/2], data[ndata-1]);
      RSF_Put(h, meta, data, data_size) ; i0++ ;
    }
    fprintf(stderr,"%s created\n",names[k]) ;
    RSF_Dump_dir(h) ;
    RSF_Close_file(h) ;
    RSF_Dump(names[k]) ;
  }
  system("cat demo[1-3].rsf >demo0.rsf") ;
  fprintf(stderr,"=========== concatenation test ===========\n") ;
  RSF_Dump("demo0.rsf") ;
// exit(0) ;
  fprintf(stderr,"=========== add records test ===========\n") ;
  meta_dim = 0 ;
  h = RSF_Open_file("demo0.rsf", RSF_RW, &meta_dim, "DeMo", NULL);
  fprintf(stderr,"meta_dim = %d\n", meta_dim) ;
  for(i = 0 ; i < NREC ; i++){
    for(j=1 ; j < meta_dim-1 ; j++) {
      meta[j] = (j << 16) + i + (0xF << 8) ;
    }
    ndata = NDATA + i0 ;
    data_size = ndata * sizeof(int32_t) ;
    if(ndata > NDATA+NREC*NFILES){
      fprintf(stderr,"ERROR: data overflow ndata = %d, max allowed = %d\n", ndata, NDATA+NREC*NFILES);
      exit(1) ;
    }
    for(j=0 ; j < ndata    ; j++) {
      data[j] = j+i0 ;
    }
    RSF_Put(h, meta, data, data_size) ; 
    i0++ ;
  }
  fprintf(stderr,"=========== dump memory directory test ===========\n") ;
  RSF_Dump_dir(h) ;
  RSF_Close_file(h) ;

  fprintf(stderr,"=========== scan test ===========\n") ;
  h = RSF_Open_file("demo0.rsf", RSF_RO, &meta_dim, "DeMo", NULL);
  key0 = 0 ;
  bzero(keys, sizeof(keys)) ;
  i0 = 0 ;
  bzero(mask,sizeof(mask)) ;
  for(j0 = 0 ; j0 < META_SIZE ; j0++) fprintf(stderr,"|%8.8x|",mask[j0]); fprintf(stderr,"\n");
// fprintf(stderr,"calling RSF_Lookup, crit = %8.8x @%p, mask = %8.8x @%p\n", criteria[0], criteria, mask[0], mask);
  key0 = RSF_Lookup(h, key0, &criteria[0], &mask[0]) ;
  while(key0 > 0) {
    keys[i0++] = key0 ;
    key0 = RSF_Lookup(h, key0, &criteria[0], &mask[0]) ;
  }
  for(j0 = 0 ; j0 < i0 ; j0++) fprintf(stderr,"%12.12lx ",keys[j0]) ;
//   for(j0 = 0 ; keys[j0] != 0 ; j0++) fprintf(stderr,"%12.12lx ",keys[j0]) ;
  fprintf(stderr,"\n") ;
  fprintf(stderr,"h = %p\n",h);
// exit(0) ;
  metaptr = NULL ;
  metaptr = (int32_t *) RSF_Get_record_meta(h, keys[0], &meta_size, &data_size) ;
  for(j0 = 0 ; keys[j0] != 0 ; j0++){
//     metaptr = (uint32_t *) RSF_Get_meta(h, keys[j0], &meta_size, &data_size) ;
    metaptr = (uint32_t *) RSF_Get_record_meta(h, keys[j0], &meta_size, &data_size) ;
    fprintf(stderr,"%p ",metaptr);
//     dataptr = RSF_Get(h, keys[j0], NULL, &data_size) ;
  }
  fprintf(stderr,"\n");
  RSF_Close_file(h) ;
// exit(0) ;

  fprintf(stderr,"=========== dump file test ===========\n") ;
  RSF_Dump("demo0.rsf") ;
}
#endif

#if defined(TEST3)
// basic function test
#define NDATA 10
#define NREC 3
#define META_SIZE 6
int the_test(int argc, char **argv){
//   start_of_record sor = {0, 1, 4, 32767} ;
  int fd ;
  RSF_handle h ;
  int32_t meta_dim = META_SIZE ;
  uint32_t meta[META_SIZE] ;
  uint32_t mask[META_SIZE] ;
  uint32_t criteria[META_SIZE] ;
//   int64_t s ;
  int32_t data[NDATA+NREC] ;
  size_t data_size ;
  int32_t i, j, k, ndata ;
  char *names[] = { "bad.rsf", "demo1.rsf", "demo2.rsf", "demo3.rsf", NULL };
  int32_t zero = 0 ;
  int64_t key0 ;
  int i0 = 0;
  int j0 ;
  int64_t keys[NREC * 16] ;
  uint32_t *dataptr, *metaptr ;
  int32_t meta_size ;

  bzero(mask, META_SIZE*sizeof(uint32_t)) ;  // search mask (ignore everything for match purposes)
  for(i=0 ; i<5 ; i++) fprintf(stderr,"%d %p",i, names[i]) ; fprintf(stderr,"\n");

  meta[0]           = 0xCAFEFADEu ;
  meta[META_SIZE-1] = 0xDEABDEEFu ;

  h = RSF_Open_file("tag-ada.rsf", RSF_RW, &zero, "DeMo", NULL);
  if( ! RSF_Valid_handle(h) ) {
    fprintf(stderr,"create with metadata length == 0 failed as expected\n") ;
  }else{
    fprintf(stderr,"ERROR: create with metadata length == 0 did not fail as expected\n") ;
  }

  fprintf(stderr,"=========== file creation test ===========\n") ;
  for(k=0 ; k<4 ; k++){
    fprintf(stderr,"before RSF_Open_file, k = %d, p = %s\n", k, names[k]) ;
    h = RSF_Open_file(names[k], 0, &meta_dim, "DeMo", NULL);
    if( ! RSF_Valid_handle(h) ) {
      fprintf(stderr,"ERROR: open failed for file '%s'\n", names[k]) ;
      continue ;
    }
  //   fprintf(stderr,"handle = %16p\n", h.p);
    for(i = 0 ; i < NREC ; i++){
      for(j=1 ; j < meta_dim-1 ; j++) {
        meta[j] = (j << 16) + i + (k << 8) ;
      }
      ndata = NDATA + i0 ;
      data_size = ndata * sizeof(data[0]) ;
      for(j=0 ; j < ndata    ; j++) {
        data[j] = j+i0 ;
      }
      RSF_Put(h, meta, data, data_size) ; i0++ ;
  //     s = RSF_Put(h, meta, data, data_size) ;
  //     fprintf(stderr,"slot = %16.16lx\n", s);
    }
    RSF_Dump_dir(h) ;
    RSF_Close_file(h) ;
//     fprintf(stderr,"after RSF_Close_file, k = %d\n", k);
//     RSF_Dump(names[k]) ;
  }
exit(0) ;
#if 0
//   bzero(mask, META_SIZE*sizeof(uint32_t)) ;  // search mask (ignore everything for match purposes)
  system("cat demo[1-3].rsf >demo0.rsf") ;
  fprintf(stderr,"=========== concatenation test ===========\n") ;
  RSF_Dump("demo0.rsf") ;
// exit(0) ;
  fprintf(stderr,"=========== add records test ===========\n") ;
  meta_dim = 0 ;
  h = RSF_Open_file("demo0.rsf", RSF_RW, &meta_dim, "DeMo", NULL);
  fprintf(stderr,"meta_dim = %d\n", meta_dim) ;
exit(0) ;
#endif
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++=//
#if 0
  RSF_Dump_dir(h) ;
exit(0) ;
  for(i = 0 ; i < NREC ; i++){
    for(j=1 ; j < meta_dim-1 ; j++) {
      meta[j] = (j << 16) + i + (0xF << 8) ;
    }
    ndata = NDATA + i0 ;
    data_size = ndata * sizeof(data[0]) ;
    for(j=0 ; j < ndata    ; j++) {
      data[j] = j+i0 ;
    }
    RSF_Put(h, meta, data, data_size) ; i0++ ;
  }
  fprintf(stderr,"=========== dump memory directory test ===========\n") ;
  RSF_Dump_dir(h) ;

  fprintf(stderr,"=========== scan test ===========\n") ;
  key0 = 0 ;
//   fprintf(stderr," <%12.12lx> ",key0) ;
  bzero(keys, sizeof(keys)) ;
//   for(j0 = 0 ; j0 < META_SIZE ; j0++) mask[j0] = 0 ;
//   bzero(mask, META_SIZE*sizeof(uint32_t)) ;  // search mask (ignore everything for match purposes)
  i0 = 0 ;
  for(j0 = 0 ; j0 < META_SIZE ; j0++) fprintf(stderr,"|%8.8x|",mask[j0]); fprintf(stderr,"\n");
fprintf(stderr,"calling RSF_Lookup, crit = %8.8x @%p, mask = %8.8x @%p\n", criteria[0], criteria, mask[0], mask);
  key0 = RSF_Lookup(h, key0, &criteria[0], &mask[0]) ;
//   while(i0 < 2) {
//     keys[i0++] = key0 ;
//     key0 = RSF_Lookup(h, key0, crit, mask) ;
//   }
//   keys[i0++] = key0 ;
  while(key0 > 0) {
    keys[i0++] = key0 ;
    key0 = RSF_Lookup(h, key0, &criteria[0], &mask[0]) ;
  }
  for(j0 = 0 ; j0 < i0 ; j0++) fprintf(stderr,"%12.12lx ",keys[j0]) ;
//   for(j0 = 0 ; keys[j0] != 0 ; j0++) fprintf(stderr,"%12.12lx ",keys[j0]) ;
  fprintf(stderr,"\n") ;
// exit(0) ;
  fprintf(stderr,"h = %p\n",h);
//   metaptr = (int32_t *) RSF_get_meta(h, keys[0], &meta_size, &data_size) ;
  for(j0 = 0 ; keys[j0] != 0 ; j0++){
    metaptr = (uint32_t *) RSF_Get_meta(h, keys[j0], &meta_size, &data_size) ;
//     dataptr = RSF_Get(h, keys[j0], NULL, &data_size) ;
  }
  RSF_Close_file(h) ;
// exit(0) ;

  fprintf(stderr,"=========== dump file test ===========\n") ;
//   RSF_Dump("demo0.rsf") ;
#endif
}
#endif

#if defined(TEST2)
// advisory lock with wait test
#include <mpi.h>
#include <errno.h>

static int32_t Lock(int fd, int lock){
  struct flock file_lock ;
  int status ;

  file_lock.l_type = (lock == 1) ? F_WRLCK : F_UNLCK ;
  file_lock.l_whence = SEEK_SET ;
  file_lock.l_start = 0 ;
  file_lock.l_len = sizeof(start_of_segment) -1 ;
  status = fcntl(fd, F_SETLKW, &file_lock) ;
// fprintf(stderr,"fd = %d, start = %ld, end = %ld, lock = %d, status = %d\n",fd, file_lock.l_start, file_lock.l_len, lock, status) ;
// if(status != 0) perror("LOCK ");
  return status ;
}
int the_test(int argc, char **argv){
  int fd ;
  int rank ;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank) ;
  fd = open("demo.lock", O_RDWR) ;
  if(rank == 0) {
    Lock(fd, 1) ;
    fprintf(stderr,"rank = %d, fd = %d locked\n",rank,fd) ;
    sleep(5) ;
    Lock(fd, 0) ;
    fprintf(stderr,"rank = %d, fd = %d unlocked\n",rank,fd) ;
  }
  if(rank > 0) {
    sleep(1) ;
    Lock(fd, 1) ;
    sleep(1) ;
    fprintf(stderr,"rank = %d, fd = %d locked\n",rank,fd) ;
    sleep(3) ;
    Lock(fd, 0) ;
    fprintf(stderr,"rank = %d, fd = %d unlocked\n",rank,fd) ;
  }
  MPI_Finalize() ;
}
#endif

int main(int argc, char **argv){
  return the_test(argc, argv) ;
}
