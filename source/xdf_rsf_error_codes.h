
#if ! defined(ERR_NO_FILE)
#if 0
// Error codes originally from XDF and used also by RSF */
// (codes -1 to -23 taken from previous xdf version when applicable) */
// if 0 / endif sequences because Fortran code may ingest this file
//! NO ERROR
#endif
#define ERR_OK 0
#if 0
//! File is not open or does not exist
#endif
#define ERR_NO_FILE -1
#if 0
//! File table is full
#endif
#define ERR_FTAB_FULL -3
#if 0
//! Short read, truncated record
#endif
#define ERR_SHORT_READ -4
#if 0
//! Invalid unit number
#endif
#define ERR_BAD_UNIT -5
#if 0
//! Src and dest files not compatible
#endif
#define ERR_NOT_COMP -6
#if 0
//! No write permission
#endif
#define ERR_NO_WRITE -7
#if 0
//! Invalid page number
#endif
#define ERR_BAD_PAGENO -8
#if 0
//! Invalid handle
#endif
#define ERR_BAD_HNDL -9
#if 0
//! Special record with idtyp = 0
#endif
#define ERR_SPECIAL -10
#if 0
//! Deleted record
#endif
#define ERR_DELETED -11
#if 0
//! Search target not found
#endif
#define ERR_NOT_FOUND -12
#if 0
//! Error in record initialisation
#endif
#define ERR_BAD_INIT -13
#if 0
//! Invalid datyp
#endif
#define ERR_BAD_DATYP -16
#if 0
//! Addressing error (not a multiple of 64)
#endif
#define ERR_BAD_ADDR -18
#if 0
//! Dimension of buf too small
#endif
#define ERR_BAD_DIM -19
#if 0
//! Invalid option name or value
#endif
#define ERR_BAD_OPT -20
#if 0
//! File already in used in write mode
#endif
#define ERR_STILL_OPN -21
#if 0
//! Read only file
#endif
#define ERR_RDONLY -22
#if 0
//! Invalid header length
#endif
#define ERR_BAD_LEN -23

#endif
