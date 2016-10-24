/*-------------------------------------------------------------------------
 *
 * mvstats.h
 *	  Multivariate statistics and selectivity estimation functions.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/mvstats.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MVSTATS_H
#define MVSTATS_H

#include "fmgr.h"
#include "commands/vacuum.h"

#define MVSTATS_MAX_DIMENSIONS	8		/* max number of attributes */

#define MVSTAT_NDISTINCT_MAGIC		0xA352BFA4		/* marks serialized bytea */
#define MVSTAT_NDISTINCT_TYPE_BASIC	1		/* basic MCV list type */

/* Multivariate distinct coefficients. */
typedef struct MVNDistinctItem {
	double		ndistinct;
	int			nattrs;
	int		   *attrs;
} MVNDistinctItem;

typedef struct MVNDistinctData {
	uint32			magic;			/* magic constant marker */
	uint32			type;			/* type of ndistinct (BASIC) */
	int				nitems;
	MVNDistinctItem	items[FLEXIBLE_ARRAY_MEMBER];
} MVNDistinctData;

typedef MVNDistinctData *MVNDistinct;


MVNDistinct		load_mv_ndistinct(Oid mvoid);

bytea *serialize_mv_ndistinct(MVNDistinct ndistinct);

/* deserialization of stats (serialization is private to analyze) */
MVNDistinct deserialize_mv_ndistinct(bytea *data);


MVNDistinct build_mv_ndistinct(double totalrows, int numrows, HeapTuple *rows,
				 int2vector *attrs, VacAttrStats **stats);

void build_mv_stats(Relation onerel, double totalrows,
			   int numrows, HeapTuple *rows,
			   int natts, VacAttrStats **vacattrstats);

void update_mv_stats(Oid relid, MVNDistinct ndistinct,
					 int2vector *attrs, VacAttrStats **stats);

#endif
