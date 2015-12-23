/*-------------------------------------------------------------------------
 *
 * mvdist.c
 *	  POSTGRES multivariate distinct coefficients
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/mvstats/mvdist.c
 *
 *-------------------------------------------------------------------------
 */

#include "common.h"
#include "utils/lsyscache.h"

/*
 * 
 */
double
build_mv_ndistinct(int numrows, HeapTuple *rows, int2vector *attrs,
				   VacAttrStats **stats)
{
	int i, j;
	int numattrs = attrs->dim1;
	MultiSortSupport mss = multi_sort_init(numattrs);
	int ndistinct;
	double result;

	/*
	 * It's possible to sort the sample rows directly, but this seemed
	 * somehow simpler / less error prone. Another option would be to
	 * allocate the arrays for each SortItem separately, but that'd be
	 * significant overhead (not just CPU, but especially memory bloat).
	 */
	SortItem * items = (SortItem*)palloc0(numrows * sizeof(SortItem));

	Datum *values = (Datum*)palloc0(sizeof(Datum) * numrows * numattrs);
	bool  *isnull = (bool*)palloc0(sizeof(bool) * numrows * numattrs);

	for (i = 0; i < numrows; i++)
	{
		items[i].values = &values[i * numattrs];
		items[i].isnull = &isnull[i * numattrs];
	}

	Assert(numattrs >= 2);

	for (i = 0; i < numattrs; i++)
	{
		/* prepare the sort function for the first dimension */
		multi_sort_add_dimension(mss, i, i, stats);

		/* accumulate all the data into the array and sort it */
		for (j = 0; j < numrows; j++)
		{
			items[j].values[i]
				= heap_getattr(rows[j], attrs->values[i],
							   stats[i]->tupDesc, &items[j].isnull[i]);
		}
	}

	qsort_arg((void *) items, numrows, sizeof(SortItem),
			  multi_sort_compare, mss);

	/* count number of distinct combinations */

	ndistinct = 1;
	for (i = 1; i < numrows; i++)
	{
		if (multi_sort_compare(&items[i], &items[i-1], mss) != 0)
			ndistinct++;
	}

	result = 1 / (double)ndistinct;

	/*
	 * now count distinct values for each attribute and incrementally
	 * compute ndistinct(a,b) / (ndistinct(a) * ndistinct(b))
	 */
	for (i = 0; i < numattrs; i++)
	{
		SortSupportData ssup;
		StdAnalyzeData *tmp = (StdAnalyzeData *)stats[i]->extra_data;

		/* initialize sort support, etc. */
		memset(&ssup, 0, sizeof(ssup));
		ssup.ssup_cxt = CurrentMemoryContext;

		/* We always use the default collation for statistics */
		ssup.ssup_collation = DEFAULT_COLLATION_OID;
		ssup.ssup_nulls_first = false;

		PrepareSortSupportFromOrderingOp(tmp->ltopr, &ssup);

		memset(values, 0, sizeof(Datum) * numrows);

		/* accumulate all the data into the array and sort it */
		for (j = 0; j < numrows; j++)
		{
			bool isnull;
			values[j] = heap_getattr(rows[j], attrs->values[i],
									 stats[i]->tupDesc, &isnull);
		}

		qsort_arg((void *)values, numrows, sizeof(Datum),
				  compare_scalars_simple, &ssup);

		ndistinct = 1;
		for (j = 1; j < numrows; j++)
		{
			if (compare_scalars_simple(&values[j], &values[j-1], &ssup) != 0)
				ndistinct++;
		}

		result *= ndistinct;
	}

	return result;
}

double
load_mv_ndistinct(Oid mvoid)
{
	bool		isnull = false;
	Datum		deps;

	/* Prepare to scan pg_mv_statistic for entries having indrelid = this rel. */
	HeapTuple	htup = SearchSysCache1(MVSTATOID, ObjectIdGetDatum(mvoid));

#ifdef USE_ASSERT_CHECKING
	Form_pg_mv_statistic	mvstat = (Form_pg_mv_statistic) GETSTRUCT(htup);
	Assert(mvstat->ndist_enabled && mvstat->ndist_built);
#endif

	deps = SysCacheGetAttr(MVSTATOID, htup,
						   Anum_pg_mv_statistic_standist, &isnull);

	Assert(!isnull);

	ReleaseSysCache(htup);

	return DatumGetFloat8(deps);
}
