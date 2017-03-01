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

#define STATS_DEPS_MAGIC		0xB4549A2C		/* marks serialized bytea */
#define STATS_DEPS_TYPE_BASIC	1		/* basic dependencies type */

/*
 * Functional dependencies, tracking column-level relationships (values
 * in one column determine values in another one).
 */
typedef struct MVDependencyData
{
	double		degree;			/* degree of validity (0-1) */
	AttrNumber	nattributes;	/* number of attributes */
	AttrNumber	attributes[FLEXIBLE_ARRAY_MEMBER];	/* attribute numbers */
} MVDependencyData;

typedef MVDependencyData *MVDependency;

typedef struct MVDependenciesData
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of MV Dependencies (BASIC) */
	uint32		ndeps;			/* number of dependencies */
	MVDependency deps[FLEXIBLE_ARRAY_MEMBER];	/* dependencies */
} MVDependenciesData;

typedef MVDependenciesData *MVDependencies;



MVNDistinct		load_ext_ndistinct(Oid mvoid);

bytea *serialize_ext_ndistinct(MVNDistinct ndistinct);
bytea *serialize_ext_dependencies(MVDependencies dependencies);

/* deserialization of stats (serialization is private to analyze) */
MVNDistinct deserialize_ext_ndistinct(bytea *data);
MVDependencies deserialize_ext_dependencies(bytea *data);

MVNDistinct build_ext_ndistinct(double totalrows, int numrows, HeapTuple *rows,
							   int2vector *attrs, VacAttrStats **stats);

MVDependencies build_ext_dependencies(int numrows, HeapTuple *rows,
					  int2vector *attrs,
					  VacAttrStats **stats);

void build_ext_stats(Relation onerel, double totalrows,
			   int numrows, HeapTuple *rows,
			   int natts, VacAttrStats **vacattrstats);

extern bool stats_are_enabled(HeapTuple htup, char type);
extern bool stats_are_built(HeapTuple htup, char type);

#endif	/* STATS_H */
