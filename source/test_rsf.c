#include <rsf_int.h>
#if defined(TEST1)
// basic function test
#define NDATA 8
#define NREC 5
#define META_SIZE 6
int main(int argc, char **argv){
  start_of_record sor = {0, 1, 4, 32767} ;
  int fd ;
  RSF_handle h ;
  int32_t meta_dim = META_SIZE ;
  uint32_t meta[META_SIZE] ;
//   int64_t s ;
  int32_t data[NDATA+NREC] ;
  size_t data_size ;
  int32_t i, j, k, ndata ;
  char *names[] = { "bad.rsf", "demo1.rsf", "demo2.rsf", "demo3.rsf", NULL };
  int32_t zero = 0 ;

  fd = open("demo.dat", O_RDWR + O_CREAT, 0755) ;
  write(fd, &sor, sizeof(sor));
  close(fd) ;
  meta[0]           = 0xCAFEFADEu ;
  meta[META_SIZE-1] = 0xDEABDEEFu ;

  h = RSF_Open_file("tag-ada.rsf", RSF_RW, &zero, "DeMo", NULL);
  if( ! RSF_Valid_handle(h) ) {
    printf("create with metadata length == 0 failed as expected\n") ;
  }else{
    printf("ERROR: create with metadata length == 0 did not fail as expected\n") ;
  }

  for(k=0 ; k<4 ; k++){
    h = RSF_Open_file(names[k], 0, &meta_dim, "DeMo", NULL);
    if( ! RSF_Valid_handle(h) ) {
      printf("ERROR: open failed for file '%s'\n", names[k]) ;
      continue ;
    }
  //   printf("handle = %16p\n", h.p);
    for(i = 0 ; i < NREC ; i++){
      for(j=1 ; j < meta_dim-1 ; j++) {
        meta[j] = (j << 16) + i + (k << 8) ;
      }
  //     for(j=0 ; j < meta_dim ; j++) {
  //       printf(" %8.8x", meta[j]);
  //     }
  //     printf("\n");
      ndata = NDATA + i ;
      data_size = ndata * sizeof(data[0]) ;
      for(j=0 ; j < ndata    ; j++) {
        data[j] = i+j ;
      }
      RSF_Put(h, meta, data, data_size) ;
  //     s = RSF_Put(h, meta, data, data_size) ;
  //     printf("slot = %16.16lx\n", s);
    }
    RSF_Close_file(h) ;
  }

  system("cat demo[1-3].rsf >demo0.rsf") ;
  RSF_Dump("demo0.rsf") ;
  meta_dim = 0 ;
  h = RSF_Open_file("demo0.rsf", RSF_RW, &meta_dim, "DeMo", NULL);
  printf("meta_dim = %d\n", meta_dim) ;
  RSF_Dump_dir(h) ;
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
// printf("fd = %d, start = %ld, end = %ld, lock = %d, status = %d\n",fd, file_lock.l_start, file_lock.l_len, lock, status) ;
// if(status != 0) perror("LOCK ");
  return status ;
}
int main(int argc, char **argv){
  int fd ;
  int rank ;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank) ;
  fd = open("demo.lock", O_RDWR) ;
  if(rank == 0) {
    Lock(fd, 1) ;
    printf("rank = %d, fd = %d locked\n",rank,fd) ;
    sleep(5) ;
    Lock(fd, 0) ;
    printf("rank = %d, fd = %d unlocked\n",rank,fd) ;
  }
  if(rank > 0) {
    sleep(1) ;
    Lock(fd, 1) ;
    sleep(1) ;
    printf("rank = %d, fd = %d locked\n",rank,fd) ;
    sleep(3) ;
    Lock(fd, 0) ;
    printf("rank = %d, fd = %d unlocked\n",rank,fd) ;
  }
  MPI_Finalize() ;
}
#endif
