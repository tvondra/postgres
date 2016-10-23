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


double		   load_mv_ndistinct(Oid mvoid);

double build_mv_ndistinct(double totalrows, int numrows, HeapTuple *rows,
				 int2vector *attrs, VacAttrStats **stats);

void build_mv_stats(Relation onerel, double totalrows,
			   int numrows, HeapTuple *rows,
			   int natts, VacAttrStats **vacattrstats);

void update_mv_stats(Oid relid, double ndistcoeff,
					 int2vector *attrs, VacAttrStats **stats);

#endif
