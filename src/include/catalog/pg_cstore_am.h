/*-------------------------------------------------------------------------
 *
 * pg_cstore_am.h
 *	  definition of the system "cstore access method" relation
 *    (pg_cstore_am) along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_cstore_am.h
 *
 * NOTES
 *		the genbki.pl script reads this file and generates .bki
 *		information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CSTORE_AM_H
#define PG_CSTORE_AM_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_cstore_am definition.  cpp turns this into
 *		typedef struct FormData_pg_cstore_am
 * ----------------
 */
#define CStoreAmRelationId	3287

CATALOG(pg_cstore_am,3287)
{
	NameData	cstname;		/* column store am name */
	regproc		cstopenstore;	/* open column store */
	regproc		cstgetvalue;	/* get value of column in a row */
	regproc		cstgetrows;		/* get rows from column store */
	regproc		cstputvalue;	/* set value of column in a row */
} FormData_pg_cstore_am;

/* ----------------
 *		Form_pg_cstore_am corresponds to a pointer to a tuple with
 *		the format of pg_cstore_am relation.
 * ----------------
 */
typedef FormData_pg_cstore_am *Form_pg_cstore_am;

/* ----------------
 *		compiler constants for pg_cstore_am
 * ----------------
 */
#define Natts_pg_cstore_am				5
#define Anum_pg_cstore_am_cstname			1
#define Anum_pg_cstore_am_cstopenstore		2
#define Anum_pg_cstore_am_cstgetvalue		3
#define Anum_pg_cstore_am_cstgetrows		4
#define Anum_pg_cstore_am_cstputvalue		5

#endif   /* PG_CSTORE_AM_H */
