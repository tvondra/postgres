/*-------------------------------------------------------------------------
 *
 * pg_changeset.h
 *	  definition of the system "changeset" relation (pg_changeset)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_changeset.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CHANGESET_H
#define PG_CHANGESET_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_changeset definition.  cpp turns this into
 *		typedef struct FormData_pg_changeset.
 * ----------------
 */
#define ChangeSetRelationId  4002

CATALOG(pg_changeset,4002) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
	Oid			chsetid;		/* OID of the changeset */
	Oid			chsetrelid;		/* OID of the relation the changeset is defined on */

	int16		chsetnatts;		/* number of columns in changeset */

	/* variable-length fields start here, but we allow direct access to chsetkey */
	int2vector	chsetkey;		/* column numbers of changeset (cubenatts) */

} FormData_pg_changeset;

/* ----------------
 *		Form_pg_changeset corresponds to a pointer to a tuple with
 *		the format of pg_changeset relation.
 * ----------------
 */
typedef FormData_pg_changeset *Form_pg_changeset;

/* ----------------
 *		compiler constants for pg_changeset
 * ----------------
 */
#define Natts_pg_chageset					4
#define Anum_pg_changeset_chsetid			1
#define Anum_pg_changeset_chsetrelid		2
#define Anum_pg_changeset_chsetnatts		3
#define Anum_pg_changeset_chsetkey			4

#endif   /* PG_CHANGESET_H */
