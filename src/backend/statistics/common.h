/*-------------------------------------------------------------------------
 *
 * common.h
 *	  POSTGRES extended statistics
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/statistics/common.h
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_statistic_ext.h"
#include "foreign/fdwapi.h"
#include "postmaster/autovacuum.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/sortsupport.h"
#include "utils/stats.h"
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

/* (de)serialization info */
typedef struct DimensionInfo
{
	int			nvalues;		/* number of deduplicated values */
	int			nbytes;			/* number of bytes (serialized) */
	int			typlen;			/* pg_type.typlen */
	bool		typbyval;		/* pg_type.typbyval */
} DimensionInfo;

/* multi-sort */
typedef struct MultiSortSupportData
{
	int			ndims;			/* number of dimensions supported by the */
	SortSupportData ssup[1];	/* sort support data for each dimension */
} MultiSortSupportData;

typedef MultiSortSupportData *MultiSortSupport;

typedef struct SortItem
{
	Datum	   *values;
	bool	   *isnull;
	int			count;
} SortItem;

MultiSortSupport multi_sort_init(int ndims);

void multi_sort_add_dimension(MultiSortSupport mss, int sortdim,
						 int dim, VacAttrStats **vacattrstats);

int			multi_sort_compare(const void *a, const void *b, void *arg);

int multi_sort_compare_dim(int dim, const SortItem *a,
					   const SortItem *b, MultiSortSupport mss);

int multi_sort_compare_dims(int start, int end, const SortItem *a,
						const SortItem *b, MultiSortSupport mss);

/* comparators, used when constructing extended stats */
int			compare_datums_simple(Datum a, Datum b, SortSupport ssup);
int			compare_scalars_simple(const void *a, const void *b, void *arg);
int			compare_scalars_partition(const void *a, const void *b, void *arg);

void *bsearch_arg(const void *key, const void *base,
			size_t nmemb, size_t size,
			int (*compar) (const void *, const void *, void *),
			void *arg);

bool stats_are_enabled(HeapTuple htup, char type);
bool stats_are_built(HeapTuple htup, char type);
