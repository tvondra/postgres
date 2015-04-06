/*-------------------------------------------------------------------------
 *
 * dependencies.c
 *	  POSTGRES multivariate functional dependencies
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/mvstats/dependencies.c
 *
 *-------------------------------------------------------------------------
 */

#include "common.h"
#include "utils/lsyscache.h"

/*
 * Detect functional dependencies between columns.
 *
 * TODO This builds a complete set of dependencies, i.e. including transitive
 *      dependencies - if we identify [A => B] and [B => C], we're likely to
 *      identify [A => C] too. It might be better to  keep only the minimal set
 *      of dependencies, i.e. prune all the dependencies that we can recreate
 *      by transivitity.
 * 
 *      There are two conceptual ways to do that:
 * 
 *      (a) generate all the rules, and then prune the rules that may be
 *          recteated by combining other dependencies, or
 * 
 *      (b) performing the 'is combination of other dependencies' check before
 *          actually doing the work
 * 
 *      The second option has the advantage that we don't really need to perform
 *      the sort/count. It's not sufficient alone, though, because we may
 *      discover the dependencies in the wrong order. For example we may find
 *
 *          (a -> b), (a -> c) and then (b -> c)
 *
 *      None of those dependencies is a combination of the already known ones,
 *      yet (a -> C) is a combination of (a -> b) and (b -> c).
 *
 * 
 * FIXME Currently we simply replace NULL values with 0 and then handle is as
 *       a regular value, but that groups NULL and actual 0 values. That's
 *       clearly incorrect - we need to handle NULL values as a separate value.
 */
MVDependencies
build_mv_dependencies(int numrows, HeapTuple *rows, int2vector *attrs,
					  VacAttrStats **stats)
{
	int i;
	int numattrs = attrs->dim1;

	/* result */
	int ndeps = 0;
	MVDependencies	dependencies = NULL;
	MultiSortSupport mss = multi_sort_init(2);	/* 2 dimensions for now */

	/* TODO Maybe this should be somehow related to the number of
	 *      distinct values in the two columns we're currently analyzing.
	 *      Assuming the distribution is uniform, we can estimate the
	 *      average group size and use it as a threshold. Or something
	 *      like that. Seems better than a static approach.
	 */
	int min_group_size = 3;

	/* dimension indexes we'll check for associations [a => b] */
	int dima, dimb;

	/*
	 * We'll reuse the same array for all the 2-column combinations.
	 *
	 * It's possible to sort the sample rows directly, but this seemed
	 * somehow simples / less error prone. Another option would be to
	 * allocate the arrays for each SortItem separately, but that'd be
	 * significant overhead (not just CPU, but especially memory bloat).
	 */
	SortItem * items = (SortItem*)palloc0(numrows * sizeof(SortItem));

	Datum *values = (Datum*)palloc0(sizeof(Datum) * numrows * 2);
	bool  *isnull = (bool*)palloc0(sizeof(bool) * numrows * 2);

	for (i = 0; i < numrows; i++)
	{
		items[i].values = &values[i * 2];
		items[i].isnull = &isnull[i * 2];
	}

	Assert(numattrs >= 2);

	/*
	 * Evaluate all possible combinations of [A => B], using a simple algorithm:
	 *
	 * (a) sort the data by [A,B]
	 * (b) split the data into groups by A (new group whenever a value changes)
	 * (c) count different values in the B column (again, value changes)
	 *
	 * TODO It should be rather simple to merge [A => B] and [A => C] into
	 *      [A => B,C]. Just keep A constant, collect all the "implied" columns
	 *      and you're done.
	 */
	for (dima = 0; dima < numattrs; dima++)
	{
		/* prepare the sort function for the first dimension */
		multi_sort_add_dimension(mss, 0, dima, stats);

		for (dimb = 0; dimb < numattrs; dimb++)
		{
			SortItem current;

			/* number of groups supporting / contradicting the dependency */
			int n_supporting = 0;
			int n_contradicting = 0;

			/* counters valid within a group */
			int group_size = 0;
			int n_violations = 0;

			int n_supporting_rows = 0;
			int n_contradicting_rows = 0;

			/* make sure the columns are different (A => A) */
			if (dima == dimb)
				continue;

			/* prepare the sort function for the second dimension */
			multi_sort_add_dimension(mss, 1, dimb, stats);

			/* reset the values and isnull flags */
			memset(values, 0, sizeof(Datum) * numrows * 2);
			memset(isnull, 0, sizeof(bool)  * numrows * 2);

			/* accumulate all the data for both columns into an array and sort it */
			for (i = 0; i < numrows; i++)
			{
				items[i].values[0]
					= heap_getattr(rows[i], attrs->values[dima],
									stats[dima]->tupDesc, &items[i].isnull[0]);

				items[i].values[1]
					= heap_getattr(rows[i], attrs->values[dimb],
									stats[dimb]->tupDesc, &items[i].isnull[1]);
			}

			qsort_arg((void *) items, numrows, sizeof(SortItem),
					  multi_sort_compare, mss);

			/*
			 * Walk through the array, split it into rows according to
			 * the A value, and count distinct values in the other one.
			 * If there's a single B value for the whole group, we count
			 * it as supporting the association, otherwise we count it
			 * as contradicting.
			 *
			 * Furthermore we require a group to have at least a certain
			 * number of rows to be considered useful for supporting the
			 * dependency. But when it's contradicting, use it always useful.
			 */

			/* start with values from the first row */
			current = items[0];
			group_size  = 1;

			for (i = 1; i < numrows; i++)
			{
				/* end of the group */
				if (multi_sort_compare_dim(0, &items[i], &current, mss) != 0)
				{
					/*
					 * If there are no contradicting rows, count it as
					 * supporting (otherwise contradicting), but only if
					 * the group is large enough.
					 *
					 * The requirement of a minimum group size makes it
					 * impossible to identify [unique,unique] cases, but
					 * that's probably a different case. This is more
					 * about [zip => city] associations etc.
					 *
					 * If there are violations, count the group/rows as
					 * a violation.
					 *
					 * It may ne neither, if the group is too small (does
					 * not contain at least min_group_size rows).
					 */
					if ((n_violations == 0) && (group_size >= min_group_size))
					{
						n_supporting +=  1;
						n_supporting_rows += group_size;
					}
					else if (n_violations > 0)
					{
						n_contradicting +=  1;
						n_contradicting_rows += group_size;
					}

					/* current values start a new group */
					n_violations = 0;
					group_size = 0;
				}
				/* mismatch of a B value is contradicting */
				else if (multi_sort_compare_dim(1, &items[i], &current, mss) != 0)
				{
					n_violations += 1;
				}

				current = items[i];
				group_size += 1;
			}

			/* handle the last group (just like above) */
			if ((n_violations == 0) && (group_size >= min_group_size))
			{
				n_supporting += 1;
				n_supporting_rows += group_size;
			}
			else if (n_violations)
			{
				n_contradicting += 1;
				n_contradicting_rows += group_size;
			}

			/*
			 * See if the number of rows supporting the association is at least
			 * 10x the number of rows violating the hypothetical dependency.
			 *
			 * TODO This is rather arbitrary limit - I guess it's possible to do
			 *      some math to come up with a better rule (e.g. testing a hypothesis
			 *      'this is due to randomness'). We can create a contingency table
			 *      from the values and use it for testing. Possibly only when
			 *      there are no contradicting rows?
			 *
			 * TODO Also, if (a => b) and (b => a) at the same time, it pretty much
			 *      means there's a 1:1 relation (or one is a 'label'), making the
			 *      conditions rather redundant. Although it's possible that the
			 *      query uses incompatible combination of values.
			 */
			if (n_supporting_rows > (n_contradicting_rows * 10))
			{
				if (dependencies == NULL)
				{
					dependencies = (MVDependencies)palloc0(sizeof(MVDependenciesData));
					dependencies->magic = MVSTAT_DEPS_MAGIC;
				}
				else
					dependencies = repalloc(dependencies, offsetof(MVDependenciesData, deps)
											+ sizeof(MVDependency) * (dependencies->ndeps + 1));

				/* update the */
				dependencies->deps[ndeps] = (MVDependency)palloc0(sizeof(MVDependencyData));
				dependencies->deps[ndeps]->a = attrs->values[dima];
				dependencies->deps[ndeps]->b = attrs->values[dimb];

				dependencies->ndeps = (++ndeps);
			}
		}
	}

	pfree(items);
	pfree(values);
	pfree(isnull);
	pfree(stats);
	pfree(mss);

	return dependencies;
}

/*
 * Store the dependencies into a bytea, so that it can be stored in the
 * pg_mv_statistic catalog.
 *
 * Currently this only supports simple two-column rules, and stores them
 * as a sequence of attnum pairs. In the future, this needs to be made
 * more complex to support multiple columns on both sides of the
 * implication (using AND on left, OR on right).
 */
bytea *
serialize_mv_dependencies(MVDependencies dependencies)
{
	int i;

	/* we need to store ndeps, and each needs 2 * int16 */
	Size len = VARHDRSZ + offsetof(MVDependenciesData, deps)
				+ dependencies->ndeps * (sizeof(int16) * 2);

	bytea * output = (bytea*)palloc0(len);

	char * tmp = VARDATA(output);

	SET_VARSIZE(output, len);

	/* first, store the number of dimensions / items */
	memcpy(tmp, dependencies, offsetof(MVDependenciesData, deps));
	tmp += offsetof(MVDependenciesData, deps);

	/* walk through the dependencies and copy both columns into the bytea */
	for (i = 0; i < dependencies->ndeps; i++)
	{
		memcpy(tmp, &(dependencies->deps[i]->a), sizeof(int16));
		tmp += sizeof(int16);

		memcpy(tmp, &(dependencies->deps[i]->b), sizeof(int16));
		tmp += sizeof(int16);
	}

	return output;
}

/*
 * Reads serialized dependencies into MVDependencies structure.
 */
MVDependencies
deserialize_mv_dependencies(bytea * data)
{
	int		i;
	Size	expected_size;
	MVDependencies	dependencies;
	char   *tmp;

	if (data == NULL)
		return NULL;

	if (VARSIZE_ANY_EXHDR(data) < offsetof(MVDependenciesData,deps))
		elog(ERROR, "invalid MVDependencies size %ld (expected at least %ld)",
			 VARSIZE_ANY_EXHDR(data), offsetof(MVDependenciesData,deps));

	/* read the MVDependencies header */
	dependencies = (MVDependencies)palloc0(sizeof(MVDependenciesData));

	/* initialize pointer to the data part (skip the varlena header) */
	tmp = VARDATA(data);

	/* get the header and perform basic sanity checks */
	memcpy(dependencies, tmp, offsetof(MVDependenciesData, deps));
	tmp += offsetof(MVDependenciesData, deps);

	if (dependencies->magic != MVSTAT_DEPS_MAGIC)
	{
		pfree(dependencies);
		elog(WARNING, "not a MV Dependencies (magic number mismatch)");
		return NULL;
	}

	Assert(dependencies->ndeps > 0);

	/* what bytea size do we expect for those parameters */
	expected_size = offsetof(MVDependenciesData,deps) +
					dependencies->ndeps * sizeof(int16) * 2;

	if (VARSIZE_ANY_EXHDR(data) != expected_size)
		elog(ERROR, "invalid dependencies size %ld (expected %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	/* allocate space for the MCV items */
	dependencies = repalloc(dependencies, offsetof(MVDependenciesData,deps)
							+ (dependencies->ndeps * sizeof(MVDependency)));

	for (i = 0; i < dependencies->ndeps; i++)
	{
		dependencies->deps[i] = (MVDependency)palloc0(sizeof(MVDependencyData));

		memcpy(&(dependencies->deps[i]->a), tmp, sizeof(int16));
		tmp += sizeof(int16);

		memcpy(&(dependencies->deps[i]->b), tmp, sizeof(int16));
		tmp += sizeof(int16);
	}

	return dependencies;
}

/* print some basic info about dependencies (number of dependencies) */
Datum
pg_mv_stats_dependencies_info(PG_FUNCTION_ARGS)
{
	bytea	   *data = PG_GETARG_BYTEA_P(0);
	char	   *result;

	MVDependencies dependencies = deserialize_mv_dependencies(data);

	if (dependencies == NULL)
		PG_RETURN_NULL();

	result = palloc0(128);
	snprintf(result, 128, "dependencies=%d", dependencies->ndeps);

	/* FIXME free the deserialized data (pfree is not enough) */

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/* print the dependencies
 *
 * TODO  Would be nice if this knew the actual column names (instead of
 *       the attnums).
 *
 * FIXME This is really ugly and does not really check the lengths and
 *       strcpy/snprintf return values properly. Needs to be fixed.
 */
Datum
pg_mv_stats_dependencies_show(PG_FUNCTION_ARGS)
{
	int			i = 0;
	bytea	   *data = PG_GETARG_BYTEA_P(0);
	char	   *result = NULL;
	int			len = 0;

	MVDependencies dependencies = deserialize_mv_dependencies(data);

	if (dependencies == NULL)
		PG_RETURN_NULL();

	for (i = 0; i < dependencies->ndeps; i++)
	{
		MVDependency dependency = dependencies->deps[i];
		char	buffer[128];

		int		tmp = snprintf(buffer, 128, "%s%d => %d",
				((i == 0) ? "" : ", "), dependency->a, dependency->b);

		if (tmp < 127)
		{
			if (result == NULL)
				result = palloc0(len + tmp + 1);
			else
				result = repalloc(result, len + tmp + 1);

			strcpy(result + len, buffer);
			len += tmp;
		}
	}

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

MVDependencies
load_mv_dependencies(Oid mvoid)
{
	bool		isnull = false;
	Datum		deps;

	/* Prepare to scan pg_mv_statistic for entries having indrelid = this rel. */
	HeapTuple	htup = SearchSysCache1(MVSTATOID, ObjectIdGetDatum(mvoid));

#ifdef USE_ASSERT_CHECKING
	Form_pg_mv_statistic	mvstat = (Form_pg_mv_statistic) GETSTRUCT(htup);
	Assert(mvstat->deps_enabled && mvstat->deps_built);
#endif

	deps = SysCacheGetAttr(MVSTATOID, htup,
						   Anum_pg_mv_statistic_stadeps, &isnull);

	Assert(!isnull);

	ReleaseSysCache(htup);

	return deserialize_mv_dependencies(DatumGetByteaP(deps));
}
