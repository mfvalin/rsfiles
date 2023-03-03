#include <stdint.h>

typedef  struct{
  int32_t  m_1 ;      // fixed item 1
//     ...            // other fixed items
  uint32_t m_n ;      // fixed item n
  uint32_t meta[] ;
} rsf_pure_meta ;

typedef struct{
  uint32_t sztotal ;  // size of meta + fixed items
  uint32_t szmeta ;   // size of meta
  union{
    struct{
      uint32_t dummy[0] ;
      int i[] ;
    } i ;
    rsf_pure_meta m ;
  } ;
} rsf_meta ;

