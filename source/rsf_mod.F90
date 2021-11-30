module rsf_mod
  use ISO_C_BINDING
  implicit none

#include <rsf.hf>
  integer, parameter :: RSF_rw      = RSF_RW
  integer, parameter :: RSF_ro      = RSF_RO
  integer, parameter :: RSF_ap      = RSF_AP
  integer, parameter :: RSF_seg1    = RSF_SEG1
  integer, parameter :: RSF_version = RSF_VERSION

end module
