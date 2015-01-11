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
 * Mine functional dependencies between columns, in the form (A => B),
 * meaning that a value in column 'A' determines value in 'B'. A simple
 * artificial example may be a table created like this
 *
 *     CREATE TABLE deptest (a INT, b INT)
 *        AS SELECT i, i/10 FROM generate_series(1,100000) s(i);
 *
 * Clearly, once we know the value for 'A' we can easily determine the
 * value of 'B' by dividing (A/10). A more practical example may be
 * addresses, where (ZIP code => city name), i.e. once we know the ZIP,
 * we probably know which city it belongs to. Larger cities usually have
 * multiple ZIP codes, so the dependency can't be reversed.
 *
 * Functional dependencies are a concept well described in relational
 * theory, especially in definition of normalization and "normal forms".
 * Wikipedia has a nice definition of a functional dependency [1]:
 *
 *     In a given table, an attribute Y is said to have a functional
 *     dependency on a set of attributes X (written X -> Y) if and only
 *     if each X value is associated with precisely one Y value. For
 *     example, in an "Employee" table that includes the attributes
 *     "Employee ID" and "Employee Date of Birth", the functional
 *     dependency {Employee ID} -> {Employee Date of Birth} would hold.
 *     It follows from the previous two sentences that each {Employee ID}
 *     is associated with precisely one {Employee Date of Birth}.
 *
 * [1] http://en.wikipedia.org/wiki/Database_normalization
 *
 * Most datasets might be normalized not to contain any such functional
 * dependencies, but sometimes it's not practical. In some cases it's
 * actually a conscious choice to model the dataset in denormalized way,
 * either because of performance or to make querying easier.
 *
 * The current implementation supports only dependencies between two
 * columns, but this is merely a simplification of the initial patch.
 * It's certainly useful to mine for dependencies involving multiple
 * columns on the 'left' side, i.e. a condition for the dependency.
 * That is dependencies [A,B] => C and so on.
 *
 * TODO The implementation may/should be smart enough not to mine both
 *      [A => B] and [A,C => B], because the second dependency is a
 *      consequence of the first one (if values of A determine values
 *      of B, adding another column won't change that). The ANALYZE
 *      should first analyze 1:1 dependencies, then 2:1 dependencies
 *      (and skip the already identified ones), etc.
 *
 * For example the dependency [city name => zip code] is much weaker
 * than [city name, state name => zip code], because there may be
 * multiple cities with the same name in various states. It's not
 * perfect though - there are probably cities with the same name within
 * the same state, but this is relatively rare occurence hopefully.
 * More about this in the section about dependency mining.
 *
 * Handling multiple columns on the right side is not necessary, as such
 * dependencies may be decomposed into a set of dependencies with
 * the same meaning, one for each column on the right side. For example
 *
 *     A => [B,C]
 *
 * is exactly the same as
 *
 *     (A => B) & (A => C).
 *
 * Of course, storing (A => [B, C]) may be more efficient thant storing
 * the two dependencies (A => B) and (A => C) separately.
 *
 *
 * Dependency mining (ANALYZE)
 * ---------------------------
 *
 * The current build algorithm is rather simple - for each pair [A,B] of
 * columns, the data are sorted lexicographically (first by A, then B),
 * and then a number of metrics is computed by walking the sorted data.
 *
 * In general the algorithm counts distict values of A (forming groups
 * thanks to the sorting), supporting or contradicting the hypothesis
 * that A => B (i.e. that values of B are predetermined by A). If there
 * are multiple values of B for a single value of A, it's counted as
 * contradicting.
 *
 * A group may be neither supporting nor contradicting. To be counted as
 * supporting, the group has to have at least min_group_size(=3) rows.
 * Smaller 'supporting' groups are counted as neutral.
 *
 * Finally, the number of rows in supporting and contradicting groups is
 * compared, and if there is at least 10x more supporting rows, the
 * dependency is considered valid.
 *
 *
 * Real-world datasets are imperfect - there may be errors (e.g. due to
 * data-entry mistakes), or factually correct records, yet contradicting
 * the dependency (e.g. when a city splits into two, but both keep the
 * same ZIP code). A strict ANALYZE implementation (where the functional
 * dependencies are identified) would ignore dependencies on such noisy
 * data, making the approach unusable in practice.
 *
 * The proposed implementation attempts to handle such noisy cases
 * gracefully, by tolerating small number of contradicting cases.
 *
 * In the future this might also perform some sort of test and decide
 * whether it's worth building any other kind of multivariate stats,
 * or whether the dependencies sufficiently describe the data. Or at
 * least not build the MCV list / histogram on the implied columns.
 * Such reduction would however make the 'verification' (see the next
 * section) impossible.
 *
 *
 * Clause reduction (planner/optimizer)
 * ------------------------------------
 *
 * Apllying the dependencies is quite simple - given a list of clauses,
 * try to apply all the dependencies. For example given clause list
 *
 *    (a = 1) AND (b = 1) AND (c = 1) AND (d < 100)
 *
 * and dependencies [a=>b] and [a=>d], this may be reduced to
 *
 *    (a = 1) AND (c = 1) AND (d < 100)
 *
 * The (d<100) can't be reduced as it's not an equality clause, so the
 * dependency [a=>d] can't be applied.
 *
 * See clauselist_apply_dependencies() for more details.
 *
 * The problem with the reduction is that the query may use conditions
 * that are not redundant, but in fact contradictory - e.g. the user
 * may search for a ZIP code and a city name not matching the ZIP code.
 *
 * In such cases, the condition on the city name is not actually
 * redundant, but actually contradictory (making the result empty), and
 * removing it while estimating the cardinality will make the estimate
 * worse.
 *
 * The current estimation assuming independence (and multiplying the
 * selectivities) works better in this case, but only by utter luck.
 *
 * In some cases this might be verified using the other multivariate
 * statistics - MCV lists and histograms. For MCV lists the verification
 * might be very simple - peek into the list if there are any items
 * matching the clause on the 'A' column (e.g. ZIP code), and if such
 * item is found, check that the 'B' column matches the other clause.
 * If it does not, the clauses are contradictory. We can't really say
 * if such item was not found, except maybe restricting the selectivity
 * using the MCV data (e.g. using min/max selectivity, or something).
 *
 * With histograms, it might work similarly - we can't check the values
 * directly (because histograms use buckets, unlike MCV lists, storing
 * the actual values). So we can only observe the buckets matching the
 * clauses - if those buckets have very low frequency, it probably means
 * the two clauses are incompatible.
 *
 * It's unclear what 'low frequency' is, but if one of the clauses is
 * implied (automatically true because of the other clause), then
 *
 *     selectivity[clause(A)] = selectivity[clause(A) & clause(B)]
 *
 * So we might compute selectivity of the first clause (on the column
 * A in dependency [A=>B]) - for example using regular statistics.
 * And then check if the selectivity computed from the histogram is
 * about the same (or significantly lower).
 *
 * The problem is that histograms work well only when the data ordering
 * matches the natural meaning. For values that serve as labels - like
 * city names or ZIP codes, or even generated IDs, histograms really
 * don't work all that well. For example sorting cities by name won't
 * match the sorting of ZIP codes, rendering the histogram unusable.
 *
 * The MCV are probably going to work much better, because they don't
 * really assume any sort of ordering. And it's probably more appropriate
 * for the label-like data.
 *
 * TODO Support dependencies with multiple columns on left/right.
 *
 * TODO Investigate using histogram and MCV list to confirm the
 *      functional dependencies.
 *
 * TODO Investigate statistical testing of the distribution (to decide
 *      whether it makes sense to build the histogram/MCV list).
 *
 * TODO Using a min/max of selectivities would probably make more sense
 *      for the associated columns.
 *
 * TODO Consider eliminating the implied columns from the histogram and
 *      MCV lists (but maybe that's not a good idea, because that'd make
 *      it impossible to use these stats for non-equality clauses and
 *      also it wouldn't be possible to use the stats for verification
 *      of the dependencies as proposed in another TODO).
 *
 * TODO This builds a complete set of dependencies, i.e. including
 *      transitive dependencies - if we identify [A => B] and [B => C],
 *      we're likely to identify [A => C] too. It might be better to
 *      keep only the minimal set of dependencies, i.e. prune all the
 *      dependencies that we can recreate by transivitity.
 *
 *      There are two conceptual ways to do that:
 *
 *      (a) generate all the rules, and then prune the rules that may
 *          be recteated by combining other dependencies, or
 *
 *      (b) performing the 'is combination of other dependencies' check
 *          before actually doing the work
 *
 *      The second option has the advantage that we don't really need
 *      to perform the sort/count. It's not sufficient alone, though,
 *      because we may discover the dependencies in the wrong order.
 *      For example [A => B], [A => C] and then [B => C]. None of those
 *      dependencies is a combination of the already known ones, yet
 *      [A => C] is a combination of [A => B] and [B => C].
 *
 * FIXME Not sure the current NULL handling makes much sense. We assume
 *       that NULL is 0, so it's handled like a regular value
 *       (NULL == NULL), so all NULLs in a single column form a single
 *       group. Maybe that's not the right thing to do, especially with
 *       equality conditions - in that case NULLs are irrelevant. So
 *       maybe the right solution would be to just ignore NULL values?
 *
 *       However simply "ignoring" the NULL values does not seem like
 *       a good idea - imagine columns A and B, where for each value of
 *       A, values in B are constant (same for the whole group) or NULL.
 *       Let's say only 10% of B values in each group is not NULL. Then
 *       ignoring the NULL values will result in 10x misestimate (and
 *       it's trivial to construct arbitrary errors). So maybe handling
 *       NULL values just like a regular value is the right thing here.
 *
 *       Or maybe NULL values should be treated differently on each side
 *       of the dependency? E.g. as ignored on the left (condition) and
 *       as regular values on the right - this seems consistent with how
 *       equality clauses work, as equality clause means 'NOT NULL'.
 *       So if we say [A => B] then it may also imply "NOT NULL" on the
 *       right side.
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
