/*-------------------------------------------------------------------------
 *
 * pg_cstore_type.h
 *	  definition of the system "cstore type" relation (pg_cstore_type)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_cstore_type.h
 *
 * NOTES
 *		the genbki.pl script reads this file and generates .bki
 *		information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CSTORE_TYPE_H
#define PG_CSTORE_TYPE_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_cstore_type definition.  cpp turns this into
 *		typedef struct FormData_pg_cstore_type
 * ----------------
 */
#define CStoreTypeRelationId	3287

CATALOG(pg_cstore_type,3287)
{
	NameData	cstname;		/* column store type name */
	regproc		cstopenstore;	/* open column store */
	regproc		cstgetvalue;	/* get value of column in a row */
	regproc		cstgetrows;		/* get rows from column store */
	regproc		cstputvalue;	/* set value of column in a row */
} FormData_pg_am;

/* ----------------
 *		Form_pg_am corresponds to a pointer to a tuple with
 *		the format of pg_am relation.
 * ----------------
 */
typedef FormData_pg_cstore_type *Form_pg_cstore_type;

/* ----------------
 *		compiler constants for pg_cstore_type
 * ----------------
 */
#define Natts_pg_cstore_type				5
#define Anum_pg_cstore_type_cstname			1
#define Anum_pg_cstore_type_cstopenstore	2
#define Anum_pg_cstore_type_cstgetvalue		3
#define Anum_pg_cstore_type_cstgetrows		4
#define Anum_pg_cstore_type_cstputvalue		5

#endif   /* PG_CSTORE_TYPE_H */
