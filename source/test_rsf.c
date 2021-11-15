#include <rsf_int.h>
#define NDATA 8
#define NREC 10
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
  int32_t i, j, ndata ;

  fd = open("demo.dat", O_RDWR + O_CREAT, 0755) ;
  write(fd, &sor, sizeof(sor));
  close(fd) ;
  h = RSF_Open_file("demo.rsf", 0, &meta_dim, "DeMo", NULL);
//   printf("handle = %16p\n", h.p);
  meta[0]           = 0xCAFEFADEu ;
  meta[META_SIZE-1] = 0xDEABDEEFu ;
  for(i = 0 ; i < NREC ; i++){
    for(j=1 ; j < meta_dim-1 ; j++) {
      meta[j] = (j << 16) + i ;
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

  RSF_Dump("demo.rsf") ;
}
