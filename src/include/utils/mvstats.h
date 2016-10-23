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


#define MVSTAT_DEPS_MAGIC		0xB4549A2C		/* marks serialized bytea */
#define MVSTAT_DEPS_TYPE_BASIC	1		/* basic dependencies type */

/*
 * Functional dependencies, tracking column-level relationships (values
 * in one column determine values in another one).
 */
typedef struct MVDependencyData
{
	double		degree;			/* degree of validity (0-1) */
	int			nattributes;	/* number of attributes */
	int16		attributes[FLEXIBLE_ARRAY_MEMBER];	/* attribute numbers */
} MVDependencyData;

typedef MVDependencyData *MVDependency;

typedef struct MVDependenciesData
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of MV Dependencies (BASIC) */
	int32		ndeps;			/* number of dependencies */
	MVDependency deps[FLEXIBLE_ARRAY_MEMBER];	/* dependencies */
} MVDependenciesData;

typedef MVDependenciesData *MVDependencies;



MVNDistinct		load_mv_ndistinct(Oid mvoid);

bytea *serialize_mv_ndistinct(MVNDistinct ndistinct);
bytea *serialize_mv_dependencies(MVDependencies dependencies);

/* deserialization of stats (serialization is private to analyze) */
MVNDistinct deserialize_mv_ndistinct(bytea *data);
MVDependencies deserialize_mv_dependencies(bytea *data);

MVNDistinct build_mv_ndistinct(double totalrows, int numrows, HeapTuple *rows,
							   int2vector *attrs, VacAttrStats **stats);

MVDependencies build_mv_dependencies(int numrows, HeapTuple *rows,
					  int2vector *attrs,
					  VacAttrStats **stats);

void build_mv_stats(Relation onerel, double totalrows,
			   int numrows, HeapTuple *rows,
			   int natts, VacAttrStats **vacattrstats);

#endif
