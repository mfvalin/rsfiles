program test_rsf
#define NDATA 10
#define NREC 3
#define META_SIZE 6
  use ISO_C_BINDING
  use rsf_mod
  implicit none
  character(len=9), dimension(0:3) :: names = [ "bad.rsf  ", "demo1.rsf", "demo2.rsf", "demo3.rsf"]
  type(RSF_handle) :: h
  integer :: i, j, k, ndat, i0
  integer(C_INT32_T) :: mode, meta_dim, status
  character(len=4)   :: appl
  integer(C_INT64_T) :: segsize, data_size, key
  integer(C_INT32_T), dimension(0:META_SIZE-1), target  :: meta
  integer(C_INT32_T), dimension(0:NDATA+NREC-1), target :: data

  segsize = 0
  i0  = 0
  mode = RSF_RW
  meta_dim = META_SIZE
  meta(0)         = int(Z"CAFEFADE")
  meta(META_SIZE-1) = int(Z"DEABDEEF")
  appl = "DeMo"
  print *,"=========== file creation test 1 ==========="
  do k = 0, 3
    h = RSF_Open_file(trim(names(k))//achar(0), RSF_RW, meta_dim, appl, segsize)
    if(RSF_Valid_handle(h) .eq. 0) then
      print *,"ERROR: open failed for file '"//trim(names(k))//"'"
      cycle
    endif
    do i = 1, NREC
      do j = 1, META_SIZE-2
        meta(j) = (j * 65536) + i + (k * 256) 
      enddo
      ndat = NDATA + i0
      data_size = ndat * 4
      do j = 0, ndat-1
        data(j) = j + i0
      enddo
      key = RSF_Put(h, meta, C_LOC(data), data_size)
      i0 = i0 + 1
    enddo
    status = RSF_Close_file(h)
    call RSF_Dump(trim(names(k))//achar(0))
  enddo
end program
