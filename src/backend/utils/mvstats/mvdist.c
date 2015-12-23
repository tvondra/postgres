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

#include <math.h>

#include "common.h"
#include "utils/lsyscache.h"

static double estimate_ndistinct(double totalrows, int numrows, int d, int f1);

/*
 * Compute ndistinct coefficient for the combination of attributes. This
 * computes the ndistinct estimate using the same estimator used in analyze.c
 * and then computes the coefficient.
 */
double
build_mv_ndistinct(double totalrows, int numrows, HeapTuple *rows,
				   int2vector *attrs, VacAttrStats **stats)
{
	int i, j;
	int f1, cnt, d;
	int nmultiple, summultiple;
	int numattrs = attrs->dim1;
	MultiSortSupport mss = multi_sort_init(numattrs);
	double ndistcoeff;

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

	f1 = 0;
	cnt = 1;
	d = 1;
	for (i = 1; i < numrows; i++)
	{
		if (multi_sort_compare(&items[i], &items[i-1], mss) != 0)
		{
			if (cnt == 1)
				f1 += 1;
			else
			{
				nmultiple += 1;
				summultiple += cnt;
			}

			d++;
			cnt = 0;
		}

		cnt += 1;
	}

	if (cnt == 1)
		f1 += 1;
	else
	{
		nmultiple += 1;
		summultiple += cnt;
	}

	ndistcoeff = 1 / estimate_ndistinct(totalrows, numrows, d, f1);

	/*
	 * now count distinct values for each attribute and incrementally
	 * compute ndistinct(a,b) / (ndistinct(a) * ndistinct(b))
	 *
	 * FIXME Probably need to handle cases when one of the ndistinct
	 *       estimates is negative, and also check that the combined
	 *       ndistinct is greater than any of those partial values.
	 */
	for (i = 0; i < numattrs; i++)
		ndistcoeff *= stats[i]->stadistinct;

	return ndistcoeff;
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

/* The Duj1 estimator (already used in analyze.c). */
static double
estimate_ndistinct(double totalrows, int numrows, int d, int f1)
{
	double	numer,
			denom,
			ndistinct;

	numer = (double) numrows *(double) d;

	denom = (double) (numrows - f1) +
			(double) f1 * (double) numrows / totalrows;

	ndistinct = numer / denom;

	/* Clamp to sane range in case of roundoff error */
	if (ndistinct < (double) d)
		ndistinct = (double) d;

	if (ndistinct > totalrows)
		ndistinct = totalrows;

	return floor(ndistinct + 0.5);
}
