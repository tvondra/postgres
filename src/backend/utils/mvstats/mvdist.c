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
#include "utils/bytea.h"
#include "utils/lsyscache.h"

static double estimate_ndistinct(double totalrows, int numrows, int d, int f1);

/* internal state for generator of k-combinations of n elements */
typedef struct CombinationGeneratorData
{

	int			k;				/* size of the combination */
	int			current;		/* index of the next combination to return */

	int			ncombinations;	/* number of combinations (size of array) */
	int		   *combinations;	/* array of pre-built combinations */

} CombinationGeneratorData;

typedef CombinationGeneratorData *CombinationGenerator;

/* generator API */
static CombinationGenerator generator_init(int2vector *attrs, int k);
static void generator_free(CombinationGenerator state);
static int *generator_next(CombinationGenerator state, int2vector *attrs);

static int n_choose_k(int n, int k);
static int num_combinations(int n);
static double ndistinct_for_combination(double totalrows, int numrows,
				   HeapTuple *rows, int2vector *attrs, VacAttrStats **stats,
				   int k, int *combination);

/*
 * Compute ndistinct coefficient for the combination of attributes. This
 * computes the ndistinct estimate using the same estimator used in analyze.c
 * and then computes the coefficient.
 */
MVNDistinct
build_mv_ndistinct(double totalrows, int numrows, HeapTuple *rows,
				   int2vector *attrs, VacAttrStats **stats)
{
	int		i, k;
	int		numattrs = attrs->dim1;
	int		numcombs = num_combinations(numattrs);

	MVNDistinct	result;

	result = palloc0(offsetof(MVNDistinctData, items) +
					 numcombs * sizeof(MVNDistinctItem));

	result->nitems = numcombs;

	i = 0;
	for (k = 2; k <= numattrs; k++)
	{
		int	* combination;
		CombinationGenerator generator;

		generator = generator_init(attrs, k);

		while ((combination = generator_next(generator, attrs)))
		{
			MVNDistinctItem *item = &result->items[i++];

			item->nattrs = k;
			item->ndistinct = ndistinct_for_combination(totalrows, numrows, rows,
												attrs, stats, k, combination);

			item->attrs = palloc(k * sizeof(int));
			memcpy(item->attrs, combination, k * sizeof(int));

			/* must not overflow the output array */
			Assert(i <= result->nitems);
		}

		generator_free(generator);
	}

	/* must consume exactly the whole output array */
	Assert(i == result->nitems);

	return result;
}

/*
 * ndistinct_for_combination
 *	Estimates number of distinct values in a combination of columns.
 *
 * This uses the same ndistinct estimator as compute_scalar_stats() in
 * ANALYZE, i.e.
 *
 *     n*d / (n - f1 + f1*n/N)
 *
 * except that instead of values in a single column we are dealing with
 * combination of multiple columns.
 */
static double
ndistinct_for_combination(double totalrows, int numrows, HeapTuple *rows,
				   int2vector *attrs, VacAttrStats **stats,
				   int k, int *combination)
{
	int i, j;
	int f1, cnt, d;
	int nmultiple, summultiple;
	MultiSortSupport mss = multi_sort_init(k);

	/*
	 * It's possible to sort the sample rows directly, but this seemed
	 * somehow simpler / less error prone. Another option would be to
	 * allocate the arrays for each SortItem separately, but that'd be
	 * significant overhead (not just CPU, but especially memory bloat).
	 */
	SortItem * items = (SortItem*)palloc0(numrows * sizeof(SortItem));

	Datum *values = (Datum*)palloc0(sizeof(Datum) * numrows * k);
	bool  *isnull = (bool*)palloc0(sizeof(bool) * numrows * k);

	Assert((k >= 2) && (k <= attrs->dim1));

	for (i = 0; i < numrows; i++)
	{
		items[i].values = &values[i * k];
		items[i].isnull = &isnull[i * k];
	}

	for (i = 0; i < k; i++)
	{
		/* prepare the sort function for the first dimension */
		multi_sort_add_dimension(mss, i, combination[i], stats);

		/* accumulate all the data into the array and sort it */
		for (j = 0; j < numrows; j++)
		{
			items[j].values[i]
				= heap_getattr(rows[j], attrs->values[combination[i]],
							   stats[combination[i]]->tupDesc,
							   &items[j].isnull[i]);
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

	return estimate_ndistinct(totalrows, numrows, d, f1);
}

MVNDistinct
load_mv_ndistinct(Oid mvoid)
{
	bool		isnull = false;
	Datum		ndist;

	/* Prepare to scan pg_mv_statistic for entries having indrelid = this rel. */
	HeapTuple	htup = SearchSysCache1(MVSTATOID, ObjectIdGetDatum(mvoid));

#ifdef USE_ASSERT_CHECKING
	Form_pg_mv_statistic	mvstat = (Form_pg_mv_statistic) GETSTRUCT(htup);
	Assert(mvstat->ndist_enabled && mvstat->ndist_built);
#endif

	ndist = SysCacheGetAttr(MVSTATOID, htup,
						   Anum_pg_mv_statistic_standist, &isnull);

	Assert(!isnull);

	ReleaseSysCache(htup);

	return deserialize_mv_ndistinct(DatumGetByteaP(ndist));
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


/*
 * pg_ndistinct_in		- input routine for type pg_ndistinct.
 *
 * pg_ndistinct is real enough to be a table column, but it has no operations
 * of its own, and disallows input too
 *
 * XXX This is inspired by what pg_node_tree does.
 */
Datum
pg_ndistinct_in(PG_FUNCTION_ARGS)
{
	/*
	 * pg_node_list stores the data in binary form and parsing text input is
	 * not needed, so disallow this.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_ndistinct")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

/*
 * pg_ndistinct		- output routine for type pg_ndistinct.
 *
 * histograms are serialized into a bytea value, so we simply call byteaout()
 * to serialize the value into text. But it'd be nice to serialize that into
 * a meaningful representation (e.g. for inspection by people).
 */
Datum
pg_ndistinct_out(PG_FUNCTION_ARGS)
{
	int i, j;
	char		   *ret;
	StringInfoData	str;

	bytea	   *data = PG_GETARG_BYTEA_PP(0);

	MVNDistinct ndist = deserialize_mv_ndistinct(data);

	initStringInfo(&str);
	appendStringInfoString(&str, "[");

	for (i = 0; i < ndist->nitems; i++)
	{
		MVNDistinctItem item = ndist->items[i];

		if (i > 0)
			appendStringInfoString(&str, ", ");

		appendStringInfoString(&str, "{");

		for (j = 0; j < item.nattrs; j++)
		{
			if (j > 0)
				appendStringInfoString(&str, ", ");

			appendStringInfo(&str, "%d", item.attrs[j]);
		}

		appendStringInfo(&str, ", %f", item.ndistinct);

		appendStringInfoString(&str, "}");
	}

	appendStringInfoString(&str, "]");

	ret = pstrdup(str.data);
	pfree(str.data);

	PG_RETURN_CSTRING(ret);
}

/*
 * pg_ndistinct_recv		- binary input routine for type pg_ndistinct.
 */
Datum
pg_ndistinct_recv(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_ndistinct")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

/*
 * pg_ndistinct_send		- binary output routine for type pg_ndistinct.
 *
 * XXX Histograms are serialized into a bytea value, so let's just send that.
 */
Datum
pg_ndistinct_send(PG_FUNCTION_ARGS)
{
	return byteasend(fcinfo);
}

static int
n_choose_k(int n, int k)
{
	int i, numer, denom;

	Assert((n > 0) && (k > 0) && (n >= k));

	numer = denom = 1;
	for (i = 1; i <= k; i++)
	{
		numer *= (n - i + 1);
		denom *= i;
	}

	Assert(numer % denom == 0);

	return numer / denom;
}

static int
num_combinations(int n)
{
	int k;
	int ncombs = 0;

	/* ignore combinations with a single column */
	for (k = 2; k <= n; k++)
		ncombs += n_choose_k(n, k);

	return ncombs;
}

/*
 * generate all combinations (k elements from n)
 */
static void
generate_combinations_recurse(CombinationGenerator state,
							  int n, int index, int start, int *current)
{
	/* If we haven't filled all the elements, simply recurse. */
	if (index < state->k)
	{
		int i;

		/*
		 * The values have to be in ascending order, so make sure we start
		 * with the value passed by parameter.
		 */

		for (i = start; i < n; i++)
		{
			current[index] = i;
			generate_combinations_recurse(state, n, (index+1), (i+1), current);
		}

		return;
	}
	else
	{
		/* we got a correct combination */
		state->combinations = (int*)repalloc(state->combinations,
						 state->k * (state->current + 1) * sizeof(int));
		memcpy(&state->combinations[(state->k * state->current)],
			   current, state->k * sizeof(int));
		state->current++;
	}
}

/* generate all k-combinations of n elements */
static void
generate_combinations(CombinationGenerator state, int n)
{
	int	   *current = (int *) palloc0(sizeof(int) * state->k);

	generate_combinations_recurse(state, n, 0, 0, current);

	pfree(current);
}

/*
 * initialize the generator of combinations, and prebuild them.
 *
 * This pre-builds all the combinations. We could also generate them in
 * generator_next(), but this seems simpler.
 */
static CombinationGenerator
generator_init(int2vector *attrs, int k)
{
	int			n = attrs->dim1;
	CombinationGenerator state;

	Assert((n >= k) && (k > 0));

	/* allocate the generator state as a single chunk of memory */
	state = (CombinationGenerator) palloc0(sizeof(CombinationGeneratorData));
	state->combinations = (int*)palloc(k * sizeof(int));

	state->ncombinations = n_choose_k(n, k);
	state->current = 0;
	state->k = k;

	/* now actually pre-generate all the combinations */
	generate_combinations(state, n);

	/* make sure we got the expected number of combinations */
	Assert(state->current == state->ncombinations);

	/* reset the number, so we start with the first one */
	state->current = 0;

	return state;
}

/* free the generator state */
static void
generator_free(CombinationGenerator state)
{
	/* we've allocated a single chunk, so just free it */
	pfree(state);
}

/* generate next combination */
static int *
generator_next(CombinationGenerator state, int2vector *attrs)
{
	if (state->current == state->ncombinations)
		return NULL;

	return &state->combinations[state->k * state->current++];
}

/*
 * serialize list of ndistinct items into a bytea
 */
bytea *
serialize_mv_ndistinct(MVNDistinct ndistinct)
{
	int			i;
	bytea	   *output;
	char	   *tmp;

	/* we need to store nitems */
	Size		len = VARHDRSZ + offsetof(MVNDistinctData, items) +
					  ndistinct->nitems * offsetof(MVNDistinctItem, attrs);

	/* and also include space for the actual attribute numbers */
	for (i = 0; i < ndistinct->nitems; i++)
		len += (sizeof(int) * ndistinct->items[i].nattrs);

	output = (bytea *) palloc0(len);
	SET_VARSIZE(output, len);

	tmp = VARDATA(output);

	ndistinct->magic = MVSTAT_NDISTINCT_MAGIC;
	ndistinct->type = MVSTAT_NDISTINCT_TYPE_BASIC;

	/* first, store the number of items */
	memcpy(tmp, ndistinct, offsetof(MVNDistinctData, items));
	tmp += offsetof(MVNDistinctData, items);

	/* store number of attributes and attribute numbers for each ndistinct entry */
	for (i = 0; i < ndistinct->nitems; i++)
	{
		MVNDistinctItem item = ndistinct->items[i];

		memcpy(tmp, &item, offsetof(MVNDistinctItem, attrs));
		tmp += offsetof(MVNDistinctItem, attrs);

		memcpy(tmp, item.attrs, sizeof(int) * item.nattrs);
		tmp += sizeof(int) * item.nattrs;

		Assert(tmp <= ((char *) output + len));
	}

	return output;
}

/*
 * Reads serialized ndistinct into MVNDistinct structure.
 */
MVNDistinct
deserialize_mv_ndistinct(bytea *data)
{
	int			i;
	Size		expected_size;
	MVNDistinct ndistinct;
	char	   *tmp;

	if (data == NULL)
		return NULL;

	if (VARSIZE_ANY_EXHDR(data) < offsetof(MVNDistinctData, items))
		elog(ERROR, "invalid MVNDistinct size %ld (expected at least %ld)",
			 VARSIZE_ANY_EXHDR(data), offsetof(MVNDistinctData, items));

	/* read the MVNDistinct header */
	ndistinct = (MVNDistinct) palloc0(sizeof(MVNDistinctData));

	/* initialize pointer to the data part (skip the varlena header) */
	tmp = VARDATA_ANY(data);

	/* get the header and perform basic sanity checks */
	memcpy(ndistinct, tmp, offsetof(MVNDistinctData, items));
	tmp += offsetof(MVNDistinctData, items);

	if (ndistinct->magic != MVSTAT_NDISTINCT_MAGIC)
		elog(ERROR, "invalid ndistinct magic %d (expected %dd)",
			 ndistinct->magic, MVSTAT_NDISTINCT_MAGIC);

	if (ndistinct->type != MVSTAT_NDISTINCT_TYPE_BASIC)
		elog(ERROR, "invalid ndistinct type %d (expected %dd)",
			 ndistinct->type, MVSTAT_NDISTINCT_TYPE_BASIC);

	Assert(ndistinct->nitems > 0);

	/* what minimum bytea size do we expect for those parameters */
	expected_size = offsetof(MVNDistinctData, items) +
		ndistinct->nitems * (offsetof(MVNDistinctItem, attrs) + sizeof(int) * 2);

	if (VARSIZE_ANY_EXHDR(data) < expected_size)
		elog(ERROR, "invalid dependencies size %ld (expected at least %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	/* allocate space for the ndistinct items */
	ndistinct = repalloc(ndistinct, offsetof(MVNDistinctData, items) +
						 (ndistinct->nitems * sizeof(MVNDistinctItem)));

	for (i = 0; i < ndistinct->nitems; i++)
	{
		MVNDistinctItem *item = &ndistinct->items[i];

		/* number of attributes */
		memcpy(item, tmp, offsetof(MVNDistinctItem, attrs));
		tmp += offsetof(MVNDistinctItem, attrs);

		/* is the number of attributes valid? */
		Assert((item->nattrs >= 2) && (item->nattrs <= MVSTATS_MAX_DIMENSIONS));

		/* now that we know the number of attributes, allocate the attribute */
		item->attrs = (int*)palloc0(item->nattrs * sizeof(int));

		/* copy attribute numbers */
		memcpy(item->attrs, tmp, sizeof(int) * item->nattrs);
		tmp += sizeof(int) * item->nattrs;

		/* still within the bytea */
		Assert(tmp <= ((char *) data + VARSIZE_ANY(data)));
	}

	/* we should have consumed the whole bytea exactly */
	Assert(tmp == ((char *) data + VARSIZE_ANY(data)));

	return ndistinct;
}
