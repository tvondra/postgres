/*-------------------------------------------------------------------------
 *
 * stats.h
 *	  Multivariate statistics and selectivity estimation functions.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/stats.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STATS_H
#define STATS_H

#include "fmgr.h"
#include "commands/vacuum.h"

#define STATS_MAX_DIMENSIONS	8		/* max number of attributes */

#define STATS_NDISTINCT_MAGIC		0xA352BFA4		/* marks serialized bytea */
#define STATS_NDISTINCT_TYPE_BASIC	1		/* basic MCV list type */

/* Multivariate distinct coefficients. */
typedef struct MVNDistinctItem
{
	double		ndistinct;
	AttrNumber	nattrs;
	AttrNumber *attrs;
} MVNDistinctItem;

typedef struct MVNDistinctData
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of ndistinct (BASIC) */
	uint32		nitems;			/* number of items in the statistic */
	MVNDistinctItem	items[FLEXIBLE_ARRAY_MEMBER];
} MVNDistinctData;

typedef MVNDistinctData *MVNDistinct;


MVNDistinct		load_ext_ndistinct(Oid mvoid);

bytea *serialize_ext_ndistinct(MVNDistinct ndistinct);

/* deserialization of stats (serialization is private to analyze) */
MVNDistinct deserialize_ext_ndistinct(bytea *data);


MVNDistinct build_ext_ndistinct(double totalrows, int numrows, HeapTuple *rows,
				 int2vector *attrs, VacAttrStats **stats);

void build_ext_stats(Relation onerel, double totalrows,
			   int numrows, HeapTuple *rows,
			   int natts, VacAttrStats **vacattrstats);

extern bool stats_are_enabled(HeapTuple htup, char type);
extern bool stats_are_built(HeapTuple htup, char type);

#endif	/* STATS_H */
