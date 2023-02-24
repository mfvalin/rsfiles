
#if ! defined(ERR_NO_FILE)
/*
Error codes originally from XDF and used also by RSF
(codes -1 to -23 taken from previous xdf version when applicable)
*/
/* NO ERROR */
#define ERR_OK 0
/* File is not open or does not exist */
#define ERR_NO_FILE -1
/* File table is full */
#define ERR_FTAB_FULL -3
/* Short read, truncated record */
#define ERR_SHORT_READ -4
/* Invalid unit number */
#define ERR_BAD_UNIT -5
/* Src and dest files not compatible */
#define ERR_NOT_COMP -6
/* No write permission */
#define ERR_NO_WRITE -7
/* Invalid page number */
#define ERR_BAD_PAGENO -8
/* Invalid handle */
#define ERR_BAD_HNDL -9
/* Special record with idtyp = 0 */
#define ERR_SPECIAL -10
/* Deleted record */
#define ERR_DELETED -11
/* Search target not found */
#define ERR_NOT_FOUND -12
/* Error in record initialisation */
#define ERR_BAD_INIT -13
/* Invalid datyp */
#define ERR_BAD_DATYP -16
/* Addressing error (not a multiple of 64) */
#define ERR_BAD_ADDR -18
/* Dimension of buf too small */
#define ERR_BAD_DIM -19
/* Invalid option name or value */
#define ERR_BAD_OPT -20
/* File already in used in write mode */
#define ERR_STILL_OPN -21
/* Read only file */
#define ERR_RDONLY -22
/* Invalid header length */
#define ERR_BAD_LEN -23

#endif
