/*-------------------------------------------------------------------------
 *
 * pg_cstore.h
 *	  definition of column stores - groups of attributes stored in
 *	  columnar orientation, along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_cstore.h
 *
 * NOTES
 *		the genbki.pl script reads this file and generates .bki
 *		information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CSTORE_H
#define PG_CSTORE_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_cstore definition.  cpp turns this into
 *		typedef struct FormData_pg_cstore
 * ----------------
 */
#define CStoreRelationId	3280

CATALOG(pg_cstore,3280)
{
	Oid			cststoreid;		/* OID of the cstore type */
	Oid			cstrelid;		/* relation containing this cstore */
	int16		cstnatts;		/* number of attributes in the cstore */
} FormData_pg_cstore;

/* ----------------
 *		Form_pg_cstore corresponds to a pointer to a tuple with
 *		the format of pg_cstore relation.
 * ----------------
 */
typedef FormData_pg_cstore *Form_pg_cstore;

/* ----------------
 *		compiler constants for pg_cstore
 * ----------------
 */
#define Natts_pg_cstore					3
#define Anum_pg_cstore_cststoreid		1
#define Anum_pg_cstore_cstrelid			2
#define Anum_pg_cstore_cstnatts			3

#endif   /* PG_CSTORE_H */
