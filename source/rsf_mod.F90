module rsf_mod
  use ISO_C_BINDING
  implicit none

#include <rsf.hf>
  integer, parameter :: RSF_rw      = RSF_RW
  integer, parameter :: RSF_ro      = RSF_RO
  integer, parameter :: RSF_ap      = RSF_AP
  integer, parameter :: RSF_seg1    = RSF_SEG1
  integer, parameter :: RSF_version = RSF_VERSION

  interface RSF_Record_metadata
    module procedure RSF_Record_metadata1
    module procedure RSF_Record_metadata2
  end interface

  interface RSF_Record_payload
    module procedure RSF_Record_payload1
    module procedure RSF_Record_payload2
  end interface

  interface RSF_Free_record
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
    type(RSF_record), pointer :: record

    call C_F_POINTER(rh%record, record)                          ! handle -> record pointer
    ! pointer to metadata array
    call C_F_POINTER(record%meta, meta, [record%meta_size])      ! meta_size is in 32 bit units
  end function RSF_Record_metadata2

  function RSF_Record_payload1(r) result (data)   ! get pointer to payload array from record
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
    type(RSF_record), pointer :: record

    call C_F_POINTER(rh%record, record)                          ! handle -> record pointer
    ! pointer to data payload array
    call C_F_POINTER(record%data, data, [record%data_size / 4])  ! data_size is in bytes
  end function RSF_Record_payload2

  function RSF_New_record(handle, max_data) result(r)
    implicit none
    type(RSF_handle), intent(IN), value :: handle
    integer(C_INT64_T), intent(IN), value :: max_data
    type(RSF_record) ,pointer :: r
    type(RSF_record_handle) :: rh

    rh = RSF_New_record_handle(handle, max_data)
    call C_F_POINTER(rh%record, r)                          ! handle -> record pointer
  end function RSF_New_record

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

end module
