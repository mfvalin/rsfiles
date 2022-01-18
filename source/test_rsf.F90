program test_rsf
#define NDATA 10
#define NREC 3
#define META_SIZE 6
#define NFILES 4
  use ISO_C_BINDING
  use ISO_FORTRAN_ENV, only : ERROR_UNIT
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
  type(RSF_record_handle) :: rh
  type(RSF_record), pointer :: rp
  integer(C_INT64_T) :: max_data
  integer(C_INT32_T), dimension(:), pointer :: mp, dp, mp2, dp2

  segsize = 0
  i0  = 0
  mode = RSF_RW
  meta_dim = META_SIZE
  meta(0)           = ishft(int(Z"CAFE"),16) + int(Z"FADE")  ! rigamarole because int(Z"CAFEFADE") is rejected by some compilers
  meta(META_SIZE-1) = ishft(int(Z"DEAD"),16) + int(Z"BEEF")
  appl = "DeMo"
  write(ERROR_UNIT,*)"=========== RSF file version ", RSF_VERSION_STRING , " test ==========="
  write(ERROR_UNIT,*)"=========== file creation test 1 ==========="
  do k = 0, 3
    h = RSF_Open_file(trim(names(k))//achar(0), RSF_RW, meta_dim, appl, segsize)
    if(RSF_Valid_handle(h) .eq. 0) then
      write(ERROR_UNIT,*)"ERROR: open failed for file '"//trim(names(k))//"'"
      cycle
    endif
    do i = 0, NREC-1
      do j = 1, META_SIZE-2
        meta(j) = (j * 65536) + i + (k * 256) 
      enddo
      ndat = NDATA + i0
      if(ndat > size(data)) then
        write(ERROR_UNIT,*)'ERROR: data overflow, ndata =',ndat,' max =',size(data)
        stop 'error'
      endif
      data_size = ndat * 4
      do j = 0, ndat-1
        data(j) = j + i0
      enddo
!       print 1,':',data(0),data(ndat/2),data(ndat-1)
      key = RSF_Put(h, meta, 0, C_LOC(data), data_size)
      i0 = i0 + 1
    enddo
    call RSF_Dump_dir(h)
    status = RSF_Close_file(h)
    call RSF_Dump(trim(names(k))//achar(0), 0)
  enddo

  write(ERROR_UNIT,*)"=========== concatenation dump test ==========="
  call system("cat demo[1-3].rsf >demo0.rsf")
  call RSF_Dump("demo0.rsf"//achar(0), 0)

  write(ERROR_UNIT,*)"=========== add records test (FUSE) ==========="
  meta_dim = 0
  segsize = 65536
  h = RSF_Open_file("demo0.rsf"//achar(0), RSF_RW + RSF_FUSE, meta_dim, "DeMo", segsize)
!   h = RSF_Open_file("demo0.rsf"//achar(0), RSF_RW, meta_dim, "DeMo", segsize)
  write(ERROR_UNIT,*)"meta_dim =",meta_dim
  call RSF_Dump("demo0.rsf"//achar(0), 0)
  do i = 0, NREC-1
    do j = 1, META_SIZE-2
      meta(j) = (j * 65536) + i + (int(Z"F") * 256) 
    enddo
    ndat = NDATA + i0
    data_size = ndat * 4
    if(ndat > size(data)) then
      write(ERROR_UNIT,*)'ERROR: data overflow, ndata =',ndat,' max =',size(data)
      stop 'error'
    endif
    do j = 0, ndat-1
      data(j) = j + i0
    enddo
    key = RSF_Put(h, meta, 0, C_LOC(data), data_size)
    i0 = i0 + 1
  enddo
! call RSF_Dump("demo0.rsf"//achar(0), 0)
  write(ERROR_UNIT,*)"=========== dump memory directory test ==========="
  call RSF_Dump_dir(h)
  status = RSF_Close_file(h)
! call RSF_Dump("demo0.rsf"//achar(0), 0)
! stop
  write(ERROR_UNIT,*)"=========== scan test ==========="
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
  write(ERROR_UNIT,*)"=========== record buffer syntax test ==========="
  max_data = 4096

  rh = RSF_New_record_handle(h, 0, max_data, C_NULL_PTR, max_data)   ! record handle
  status = RSF_Valid_record(rh)
  mp => RSF_Record_metadata(rh)
  write(ERROR_UNIT,*) "size of metadata mp  =",size(mp),' valid =',status
  dp => RSF_Record_payload(rh)
  write(ERROR_UNIT,*) "size of payload dp   =",size(dp),' alloc =',RSF_Record_allocsize(rh),' maxddata =',max_data
  call RSF_Free_record(rh)
  write(ERROR_UNIT,*) "record handle memory freed"

  rp => RSF_New_record(h, max_data)   ! pointer to record
  status = RSF_Valid_record(rp)
!   write(ERROR_UNIT,*) rp%meta_size     ! this line MUST fail to compile (referencing a private component)
  mp2 => RSF_Record_metadata(rp)
  write(ERROR_UNIT,*) "size of metadata mp2 =",size(mp2),' valid =',status
  dp2 => RSF_Record_payload(rp)
  write(ERROR_UNIT,*) "size of payload  dp2 =",size(dp2),' alloc =',RSF_Record_allocsize(rp),' maxddata =',max_data
  call RSF_Free_record(rp)
  write(ERROR_UNIT,*) "record memory freed"
  write(ERROR_UNIT,*)"=========== close file ==========="
  status = RSF_Close_file(h)

  write(ERROR_UNIT,*)"=========== dump file test ==========="
  segsize = 32768
  meta_dim = 0
  h = RSF_Open_file("demo0.rsf"//achar(0), RSF_RW, meta_dim, "DeMo", segsize)
  status = RSF_Close_file(h)
  write(ERROR_UNIT,*)"meta_dim, status =", meta_dim, status
  call RSF_Dump("demo0.rsf"//achar(0), 0)
  segsize = 16382
  meta_dim = 0
  h = RSF_Open_file("demo0.rsf"//achar(0), RSF_RW, meta_dim, "DeMo", segsize)
  status = RSF_Close_file(h)
  write(ERROR_UNIT,*)"meta_dim, status =", meta_dim, status
  call RSF_Dump("demo0.rsf"//achar(0), 1)
  h = RSF_Open_file("demo0.rsf"//achar(0), RSF_RO, meta_dim, "DeMo", segsize)
  status = RSF_Close_file(h)
! 1 format(A1,30Z9.8)
2 format(30Z13.12)
3 format(30Z9.8)
4 format(A,Z17.16)
end program
