program test_rsf
#define NDATA 10
#define NREC 3
#define META_SIZE 6
#define NFILES 4
  use ISO_C_BINDING
  use rsf_mod
  implicit none
  character(len=9), dimension(0:3) :: names = [ "bad.rsf  ", "demo1.rsf", "demo2.rsf", "demo3.rsf"]
  type(RSF_handle) :: h
  integer :: i, j, k, ndat, i0
  integer(C_INT32_T) :: mode, meta_dim, status
  character(len=4)   :: appl
  integer(C_INT64_T) :: segsize, data_size, key, key0
  integer(C_INT32_T), dimension(0:META_SIZE-1), target  :: meta, mask, criteria
  integer(C_INT32_T), dimension(0:NDATA+NREC*NFILES-1), target :: data
  integer(C_INT64_T), dimension(4096) :: keys
  type(C_PTR), dimension(4096) :: metaptr
  integer(C_INTPTR_T), dimension(4096) :: metapti
  integer(C_INT32_T), dimension(:), pointer :: metap

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
    do i = 0, NREC-1
      do j = 1, META_SIZE-2
        meta(j) = (j * 65536) + i + (k * 256) 
      enddo
      ndat = NDATA + i0
      if(ndat > size(data)) then
        print *,'ERROR: data overflow, ndata =',ndat,' max =',size(data)
        stop 'error'
      endif
      data_size = ndat * 4
      do j = 0, ndat-1
        data(j) = j + i0
      enddo
!       print 1,':',data(0),data(ndat/2),data(ndat-1)
      key = RSF_Put(h, meta, C_LOC(data), data_size)
      i0 = i0 + 1
    enddo
    call RSF_Dump_dir(h)
    status = RSF_Close_file(h)
    call RSF_Dump(trim(names(k))//achar(0))
  enddo

  print *,"=========== concatenation dump test ==========="
  call system("cat demo[1-3].rsf >demo0.rsf")
  call RSF_Dump("demo0.rsf"//achar(0))

  print *,"=========== add records test ==========="
  meta_dim = 0
  h = RSF_Open_file("demo0.rsf"//achar(0), RSF_RW, meta_dim, "DeMo", segsize)
  print *,"meta_dim =",meta_dim
  do i = 0, NREC-1
    do j = 1, META_SIZE-2
      meta(j) = (j * 65536) + i + (int(Z"F") * 256) 
    enddo
    ndat = NDATA + i0
    data_size = ndat * 4
    if(ndat > size(data)) then
      print *,'ERROR: data overflow, ndata =',ndat,' max =',size(data)
      stop 'error'
    endif
    do j = 0, ndat-1
      data(j) = j + i0
    enddo
    key = RSF_Put(h, meta, C_LOC(data), data_size)
    i0 = i0 + 1
  enddo
  print *,"=========== dump memory directory test ==========="
  call RSF_Dump_dir(h)
  status = RSF_Close_file(h)

  print *,"=========== scan test ==========="
  h = RSF_Open_file("demo0.rsf"//achar(0), RSF_RO, meta_dim, "DeMo", segsize)
  key0 = 0
  keys = 0
  i0 = 0 
  mask = 0 ; criteria = 0
  key0 = RSF_Lookup(h, key0, criteria, mask)
  do while(key0 > 0)
    i0 = i0 + 1
    keys(i0) = key0
    key0 = RSF_Lookup(h, key0, criteria, mask)
  enddo
  print 2,keys(1:i0)
  do i = 1, i0
    metaptr(i) = RSF_Get_record_meta(h, keys(i), meta_dim, data_size)
    metapti(i) = transfer(metaptr(i), metapti(i))
    call C_F_POINTER(metaptr(i), metap, [meta_dim])
    print 3,metap(1:meta_dim)
  enddo
  print 2,metapti(1:i0)
  status = RSF_Close_file(h)

  print *,"=========== dump file test ==========="
  call RSF_Dump("demo0.rsf"//achar(0))
1 format(A1,30Z9.8)
2 format(30Z13.12)
3 format(30Z9.8)
end program
