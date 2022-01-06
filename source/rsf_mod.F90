module rsf_mod
  use ISO_C_BINDING
  implicit none

#include <rsf.hf>
  integer, parameter :: RSF_rw      = RSF_RW
  integer, parameter :: RSF_ro      = RSF_RO
  integer, parameter :: RSF_ap      = RSF_AP
!   integer, parameter :: RSF_nseg    = RSF_NSEG
!   integer, parameter :: RSF_pseg    = RSF_PSEG
  integer, parameter :: RSF_fuse    = RSF_FUSE
  integer, parameter :: RSF_version = RSF_VERSION
  character(len=*), parameter :: RSF_version_string = RSF_VERSION_STRING

! the generic interfaces that follow accept RSF_record or RSF_record_handle arguments
! 1 suffixed procedures accept RSF_record (a pointer to RSF_record may be obtained from RSF_New_record)
! 2 suffixed procedures accept RSF_record_handle (from RSF_New_record_handle)

  interface RSF_Valid_record
    module procedure RSF_Valid_record1
    module procedure RSF_Valid_record2
  end interface

  interface RSF_New_record
    module procedure RSF_New_record1        ! short call, allocate space internally
    module procedure RSF_New_record2        ! long call, use caller supplied space
  end interface

  interface RSF_Record_metadata             ! pointer to metadata
    module procedure RSF_Record_metadata1
    module procedure RSF_Record_metadata2
  end interface

  interface RSF_Record_allocsize            ! allocated size in bytes
    module procedure RSF_Record_allocsize1
    module procedure RSF_Record_allocsize2
  end interface

  interface RSF_Record_max_payload          ! max payload size in 32 bit units
    module procedure RSF_Record_max_payload1
    module procedure RSF_Record_max_payload2
  end interface

  interface RSF_Record_payload              ! pointer to payload
    module procedure RSF_Record_payload1
    module procedure RSF_Record_payload2
  end interface

  interface RSF_Free_record                 ! free a record allocated with RSF_New_record_handle
    module procedure RSF_Free_record1
    module procedure RSF_Free_record2
  end interface

 contains

  function RSF_Record_metadata1(r) result (meta)   ! get pointer to metadata array from record
    implicit none
    type(RSF_record), intent(IN) :: r
    integer(C_INT32_T), dimension(:), pointer :: meta

    ! pointer to metadata array
    call C_F_POINTER(r%meta, meta, [r%meta_size])      ! meta_size is in 32 bit units
  end function RSF_Record_metadata1

  function RSF_Record_metadata2(rh) result (meta)   ! get pointer to metadata array from record handle
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    integer(C_INT32_T), dimension(:), pointer :: meta
    type(RSF_record), pointer :: r

    call C_F_POINTER(rh%record, r)                          ! handle -> record pointer
    ! pointer to metadata array
    call C_F_POINTER(r%meta, meta, [r%meta_size])      ! meta_size is in 32 bit units
  end function RSF_Record_metadata2

  function RSF_Record_allocsize1(r) result(s)
    implicit none
    type(RSF_record), intent(IN) :: r
    integer(C_INT64_T) :: s

    ! allocated size of record
    s = r%rsz                                      ! rsz is in bytes
  end function RSF_Record_allocsize1

  function RSF_Record_allocsize2(rh) result(s)
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    integer(C_INT64_T) :: s
    type(RSF_record), pointer :: r

    call C_F_POINTER(rh%record, r)             ! handle -> record pointer
    ! allocated size of record
    s = r%rsz                                  ! rsz is in bytes
  end function RSF_Record_allocsize2

  function RSF_Record_max_payload1(r) result (s)    ! get max size of payload array from record
    implicit none
    type(RSF_record), intent(IN) :: r
    integer(C_INT64_T) :: s

    s = r%max_data / 4                              ! max_data is in bytes
  end function RSF_Record_max_payload1

  function RSF_Record_max_payload2(rh) result (s)   ! get pointer to payload array from record handle
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    integer(C_INT64_T) :: s
    type(RSF_record), pointer :: r

    call C_F_POINTER(rh%record, r)             ! handle -> record pointer
    ! pointer to data payload array
    s = r%max_data / 4                         ! max_data is in bytes
  end function RSF_Record_max_payload2

  function RSF_Record_payload1(r) result (data)     ! get pointer to payload array from record
    implicit none
    type(RSF_record), intent(IN) :: r
    integer(C_INT32_T), dimension(:), pointer :: data

    ! pointer to data payload array
    call C_F_POINTER(r%data, data, [r%data_size / 4])  ! data_size is in bytes
  end function RSF_Record_payload1

  function RSF_Record_payload2(rh) result (data)   ! get pointer to payload array from record handle
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    integer(C_INT32_T), dimension(:), pointer :: data
    type(RSF_record), pointer :: r

    call C_F_POINTER(rh%record, r)                          ! handle -> record pointer
    ! pointer to data payload array
    call C_F_POINTER(r%data, data, [r%data_size / 4])  ! data_size is in bytes
  end function RSF_Record_payload2

  function RSF_New_record1(fh, max_data) result(r)      ! return a pointer to RSF_record type
    implicit none
    type(RSF_handle), intent(IN), value :: fh
    integer(C_INT64_T), intent(IN), value :: max_data
    type(RSF_record) ,pointer :: r
    type(RSF_record_handle) :: rh

    rh = RSF_New_record_handle(fh, max_data, C_NULL_PTR, max_data)
    call C_F_POINTER(rh%record, r)                          ! handle -> record pointer
  end function RSF_New_record1

  function RSF_New_record2(fh, max_data, t, szt) result(r)  ! return a pointer to RSF_record type
    implicit none
    type(RSF_handle), intent(IN), value :: fh
    integer(C_INT64_T), intent(IN), value :: max_data
    type(C_PTR) :: t                                        ! user supplied space
    integer(C_INT64_T), intent(IN), value :: szt            ! size in bytes of user supplied space
    type(RSF_record) ,pointer :: r
    type(RSF_record_handle) :: rh

    rh = RSF_New_record_handle(fh, max_data, t, szt)
    call C_F_POINTER(rh%record, r)                          ! handle -> record pointer
  end function RSF_New_record2

  subroutine RSF_Free_record1(rh)
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    call RSF_Free_record_handle(rh)
  end subroutine RSF_Free_record1

  subroutine RSF_Free_record2(r)
    implicit none
    type(RSF_record), intent(IN), target :: r
    type(RSF_record_handle) :: rh
    rh%record = C_LOC(r)
    call RSF_Free_record_handle(rh)
  end subroutine RSF_Free_record2

  function RSF_Valid_record1(rh) result(s)
    implicit none
    type(RSF_record_handle), intent(IN), value :: rh
    integer(C_INT32_T) :: s
    s = RSF_Valid_record_handle(rh)
  end function RSF_Valid_record1

  function RSF_Valid_record2(r) result(s)
    implicit none
    type(RSF_record), intent(IN), target :: r
    integer(C_INT32_T) :: s
    type(RSF_record_handle) :: rh
    rh%record = C_LOC(r)
    s = RSF_Valid_record_handle(rh)
  end function RSF_Valid_record2

end module
