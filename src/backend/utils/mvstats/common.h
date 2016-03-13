/*-------------------------------------------------------------------------
 *
 * common.h
 *	  POSTGRES multivariate statistics
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/mvstats/common.h
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_mv_statistic.h"
#include "foreign/fdwapi.h"
#include "postmaster/autovacuum.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/mvstats.h"
#include "utils/sortsupport.h"
#include "utils/syscache.h"


/* FIXME private structure copied from analyze.c */

typedef struct
{
	Oid			eqopr;			/* '=' operator for datatype, if any */
	Oid			eqfunc;			/* and associated function */
	Oid			ltopr;			/* '<' operator for datatype, if any */
} StdAnalyzeData;

typedef struct
{
	Datum		value;			/* a data value */
	int			tupno;			/* position index for tuple it came from */
} ScalarItem;
 
/* multi-sort */
typedef struct MultiSortSupportData {
	int				ndims;		/* number of dimensions supported by the */
	SortSupportData	ssup[1];	/* sort support data for each dimension */
} MultiSortSupportData;

typedef MultiSortSupportData* MultiSortSupport;

typedef struct SortItem {
	Datum  *values;
	bool   *isnull;
} SortItem;

MultiSortSupport multi_sort_init(int ndims);

void multi_sort_add_dimension(MultiSortSupport mss, int sortdim,
							  int dim, VacAttrStats **vacattrstats);

int multi_sort_compare(const void *a, const void *b, void *arg);

int multi_sort_compare_dim(int dim, const SortItem *a,
						   const SortItem *b, MultiSortSupport mss);

/* comparators, used when constructing multivariate stats */
int compare_scalars_simple(const void *a, const void *b, void *arg);
int compare_scalars_partition(const void *a, const void *b, void *arg);
