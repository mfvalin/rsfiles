
#if ! defined(ERR_NO_FILE)
#if ! defined(IN_FORTRAN_CODE)
// Error codes originally from XDF and used also by RSF */
// (codes -1 to -23 taken from previous xdf version when applicable) */
// if ! defined(IN_FORTRAN_CODE) / endif sequences because Fortran code may ingest this file
//! NO ERROR
#endif
#define ERR_OK 0
#if ! defined(IN_FORTRAN_CODE)
//! File is not open or does not exist
#endif
#define ERR_NO_FILE -1
#if ! defined(IN_FORTRAN_CODE)
//! File table is full
#endif
#define ERR_FTAB_FULL -3
#if ! defined(IN_FORTRAN_CODE)
//! Short read, truncated record
#endif
#define ERR_SHORT_READ -4
#if ! defined(IN_FORTRAN_CODE)
//! Invalid unit number
#endif
#define ERR_BAD_UNIT -5
#if ! defined(IN_FORTRAN_CODE)
//! Src and dest files not compatible
#endif
#define ERR_NOT_COMP -6
#if ! defined(IN_FORTRAN_CODE)
//! No write permission
#endif
#define ERR_NO_WRITE -7
#if ! defined(IN_FORTRAN_CODE)
//! Invalid page number
#endif
#define ERR_BAD_PAGENO -8
#if ! defined(IN_FORTRAN_CODE)
//! Invalid handle
#endif
#define ERR_BAD_HNDL -9
#if ! defined(IN_FORTRAN_CODE)
//! Special record with idtyp = 0
#endif
#define ERR_SPECIAL -10
#if ! defined(IN_FORTRAN_CODE)
//! Deleted record
#endif
#define ERR_DELETED -11
#if ! defined(IN_FORTRAN_CODE)
//! Search target not found
#endif
#define ERR_NOT_FOUND -12
#if ! defined(IN_FORTRAN_CODE)
//! Error in record initialisation
#endif
#define ERR_BAD_INIT -13
#if ! defined(IN_FORTRAN_CODE)
//! Invalid datyp
#endif
#define ERR_BAD_DATYP -16
#if ! defined(IN_FORTRAN_CODE)
//! Addressing error (not a multiple of 64)
#endif
#define ERR_BAD_ADDR -18
#if ! defined(IN_FORTRAN_CODE)
//! Dimension of buf too small
#endif
#define ERR_BAD_DIM -19
#if ! defined(IN_FORTRAN_CODE)
//! Invalid option name or value
#endif
#define ERR_BAD_OPT -20
#if ! defined(IN_FORTRAN_CODE)
//! File already in used in write mode
#endif
#define ERR_STILL_OPN -21
#if ! defined(IN_FORTRAN_CODE)
//! Read only file
#endif
#define ERR_RDONLY -22
#if ! defined(IN_FORTRAN_CODE)
//! Invalid header length
#endif
#define ERR_BAD_LEN -23

#endif
