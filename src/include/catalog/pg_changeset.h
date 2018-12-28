/*-------------------------------------------------------------------------
 *
 * pg_changeset.h
 *	  definition of the system "changeset" relation (pg_changeset), tracking
 *	  changes in a given table. The changes are in no particular order (the
 *	  only sensible order is commit order, and that's unknown at the moment
 *	  of adding data to changeset).
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
#include "catalog/pg_changeset_d.h"

/* ----------------
 *		pg_changeset definition.  cpp turns this into
 *		typedef struct FormData_pg_changeset.
 * ----------------
 */
CATALOG(pg_changeset,4002,ChangeSetRelationId) BKI_SCHEMA_MACRO
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

#define CHANGESET_INSERT		'I'
#define CHANGESET_DELETE		'D'

#endif   /* PG_CHANGESET_H */
