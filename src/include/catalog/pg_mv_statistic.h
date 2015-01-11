/*-------------------------------------------------------------------------
 *
 * pg_mv_statistic.h
 *	  definition of the system "multivariate statistic" relation (pg_mv_statistic)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_mv_statistic.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_MV_STATISTIC_H
#define PG_MV_STATISTIC_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_mv_statistic definition.  cpp turns this into
 *		typedef struct FormData_pg_mv_statistic
 * ----------------
 */
#define MvStatisticRelationId  3381

CATALOG(pg_mv_statistic,3381)
{
	/* These fields form the unique key for the entry: */
	Oid			starelid;			/* relation containing attributes */
	NameData	staname;			/* statistics name */
	Oid			stanamespace;		/* OID of namespace containing this statistics */
	Oid			staowner;			/* statistics owner */

	/* statistics requested to build */
	bool		deps_enabled;		/* analyze dependencies? */
	bool		mcv_enabled;		/* build MCV list? */
	bool		hist_enabled;		/* build histogram? */

	/* histogram / MCV size */
	int32		mcv_max_items;		/* max MCV items */
	int32		hist_max_buckets;	/* max histogram buckets */

	/* statistics that are available (if requested) */
	bool		deps_built;			/* dependencies were built */
	bool		mcv_built;			/* MCV list was built */
	bool		hist_built;			/* histogram was built */

	/* variable-length fields start here, but we allow direct access to stakeys */
	int2vector	stakeys;			/* array of column keys */

#ifdef CATALOG_VARLEN
	bytea		stadeps;			/* dependencies (serialized) */
	bytea		stamcv;				/* MCV list (serialized) */
	bytea		stahist;			/* MV histogram (serialized) */
#endif

} FormData_pg_mv_statistic;

/* ----------------
 *		Form_pg_mv_statistic corresponds to a pointer to a tuple with
 *		the format of pg_mv_statistic relation.
 * ----------------
 */
typedef FormData_pg_mv_statistic *Form_pg_mv_statistic;

/* ----------------
 *		compiler constants for pg_mv_statistic
 * ----------------
 */
#define Natts_pg_mv_statistic					16
#define Anum_pg_mv_statistic_starelid			1
#define Anum_pg_mv_statistic_staname			2
#define Anum_pg_mv_statistic_stanamespace		3
#define Anum_pg_mv_statistic_staowner			4
#define Anum_pg_mv_statistic_deps_enabled		5
#define Anum_pg_mv_statistic_mcv_enabled		6
#define Anum_pg_mv_statistic_hist_enabled		7
#define Anum_pg_mv_statistic_mcv_max_items		8
#define Anum_pg_mv_statistic_hist_max_buckets	9
#define Anum_pg_mv_statistic_deps_built			10
#define Anum_pg_mv_statistic_mcv_built			11
#define Anum_pg_mv_statistic_hist_built			12
#define Anum_pg_mv_statistic_stakeys			13
#define Anum_pg_mv_statistic_stadeps			14
#define Anum_pg_mv_statistic_stamcv				15
#define Anum_pg_mv_statistic_stahist			16

#endif   /* PG_MV_STATISTIC_H */
