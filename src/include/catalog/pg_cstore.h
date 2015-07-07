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
#define CStoreRelationId	3286

CATALOG(pg_cstore,3286)
{
	Oid			cststoreid;		/* OID of the cstore type */
	Oid			cstrelid;		/* relation containing this cstore */
	int16		cstnatts;		/* number of attributes in the cstore */

	/* variable-length fields start here, but we allow direct access to cstatts */
	int2vector	cstatts;		/* column numbers of cols in this store */
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
#define Natts_pg_cstore					4
#define Anum_pg_cstore_cststoreid		1
#define Anum_pg_cstore_cstrelid			2
#define Anum_pg_cstore_cstnatts			3
#define Anum_pg_cstore_cstatts			4

#endif   /* PG_CSTORE_H */
