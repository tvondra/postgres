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

/*
 * Functional dependencies, tracking column-level relationships (values
 * in one column determine values in another one).
 */
typedef struct MVDependencyData {
	int 	nattributes;	/* number of attributes */
	int16	attributes[1];	/* attribute numbers */
} MVDependencyData;

typedef MVDependencyData* MVDependency;

typedef struct MVDependenciesData {
	uint32			magic;		/* magic constant marker */
	uint32			type;		/* type of MV Dependencies (BASIC) */
	int32			ndeps;		/* number of dependencies */
	MVDependency	deps[1];	/* XXX why not a pointer? */
} MVDependenciesData;

typedef MVDependenciesData* MVDependencies;

#define MVSTAT_DEPS_MAGIC		0xB4549A2C	/* marks serialized bytea */
#define MVSTAT_DEPS_TYPE_BASIC	1			/* basic dependencies type */

/*
 * TODO Maybe fetching the histogram/MCV list separately is inefficient?
 *      Consider adding a single `fetch_stats` method, fetching all
 *      stats specified using flags (or something like that).
 */

bytea * serialize_mv_dependencies(MVDependencies dependencies);

/* deserialization of stats (serialization is private to analyze) */
MVDependencies	deserialize_mv_dependencies(bytea * data);

/* FIXME this probably belongs somewhere else (not to operations stats) */
extern Datum pg_mv_stats_dependencies_info(PG_FUNCTION_ARGS);
extern Datum pg_mv_stats_dependencies_show(PG_FUNCTION_ARGS);

MVDependencies
build_mv_dependencies(int numrows, HeapTuple *rows,
								  int2vector *attrs,
								  VacAttrStats **stats);

void build_mv_stats(Relation onerel, int numrows, HeapTuple *rows,
						   int natts, VacAttrStats **vacattrstats);

void update_mv_stats(Oid relid, MVDependencies dependencies, int2vector *attrs);

#endif
