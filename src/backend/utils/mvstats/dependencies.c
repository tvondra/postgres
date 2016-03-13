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

/* internal state for generator of variations (k-permutations of n elements) */
typedef struct VariationGeneratorData {

	int k;					/* size of the k-permutation */
	int current;			/* index of the next variation to return */

	int nvariations;		/* number of variations generated (size of array) */
	int	variations[1];		/* array of pre-built variations */

} VariationGeneratorData;

typedef VariationGeneratorData* VariationGenerator;

/*
 * generate all variations (k-permutations of n elements)
 */
static void
generate_variations(VariationGenerator state,
					int n, int maxlevel, int level, int *current)
{
	int i, j;

	/* initialize */
	if (level == 0)
	{
		current = (int*)palloc0(sizeof(int) * (maxlevel+1));
		state->current = 0;
	}

	for (i = 0; i < n; i++)
	{
		/* check if the value is already used current variation */
		bool found = false;
		for (j = 0; j < level; j++)
		{
			if (current[j] == i)
			{
				found = true;
				break;
			}
		}

		/* already used, so try the next element */
		if (found)
			continue;

		/* ok, we can use this element, so store it */
		current[level] = i;

		/* and check if we do have a complete variation of k elements */
		if (level == maxlevel)
		{
			/* yep, store the variation */
			Assert(state->current < state->nvariations);
			memcpy(&state->variations[(state->k * state->current)], current,
				   sizeof(int) * (maxlevel+1));
			state->current++;
		}
		else
			/* nope, look for additional elements */
			generate_variations(state, n, maxlevel, level+1, current);
	}

	if (level == 0)
		pfree(current);
}

/*
 * initialize the generator of variations, and prebuild the variations
 *
 * This pre-builds all the variations. We could also generate them in
 * generator_next(), but this seems simpler.
 */
static VariationGenerator
generator_init(int2vector *attrs, int k)
{
	int	i;
	int	n = attrs->dim1;
	int	nvariations;
	VariationGenerator	state;

	Assert((n >= k) &&  (k > 0));

	/* compute the total number of variations as n!/(n-k)! */
	nvariations = n;
	for (i = 1; i < k; i++)
		nvariations *= (n - i);

	/* allocate the generator state as a single chunk of memory */
	state = (VariationGenerator)palloc0(
					offsetof(VariationGeneratorData, variations)
					+ (nvariations * k * sizeof(int)));	/* variations */

	state->nvariations = nvariations;
	state->k = k;

	/* now actually pre-generate all the variations */
	generate_variations(state, n, (k-1), 0, NULL);

	/* we expect to generate exactly the right number of variations */
	Assert(state->nvariations == state->current);

	/* reset the index */
	state->current = 0;

	return state;
}

/* free the generator state */
static void
generator_free(VariationGenerator state)
{
	/* we've allocated a single chunk, so just free it */
	pfree(state);
}

/* generate next combination */
static int*
generator_next(VariationGenerator state, int2vector *attrs)
{
	if (state->current == state->nvariations)
		return NULL;

	return &state->variations[state->k * state->current++];
}

/*
 * check if the dependency is implied by existing dependencies
 *
 * A dependency is considered implied, if there exists a dependency with the
 * same column on the left, and a subset of columns on the right side. So for
 * example if we have a dependency
 *
 *     (a,b,c) -> d
 *
 * then we are looking for these six dependencies
 *
 *     (a) -> d
 *     (b) -> d
 *     (c) -> d
 *     (a,b) -> d
 *     (a,c) -> d
 *     (b,c) -> d
 *
 * This does not detect transitive dependencies. For example if we have
 *
 *     (a) -> b
 *     (b) -> c
 *
 * then obviously
 *
 *     (a) -> c
 *
 * but this is not detected. Extending the method to handle transitive cases
 * is future work.
 */
static bool
dependency_is_implied(MVDependencies dependencies, int k, int *dependency,
					  int2vector * attrs)
{
	bool	implied = false;
	int		i, j, l;
	int	   *tmp;

	if (dependencies == NULL)
		return false;

	tmp = (int*)palloc0(sizeof(int) * k);

	/* translate the indexes to actual attribute numbers */
	for (i = 0; i < k; i++)
		tmp[i] = attrs->values[dependency[i]];

	/* search for a smaller */
	for (i = 0; i < dependencies->ndeps; i++)
	{
		bool contained = true;
		MVDependency dep = dependencies->deps[i];

		/* does the last attribute match? */
		if (tmp[k-1] != dep->attributes[dep->nattributes-1])
			continue;	/* nope, no need to check this dependency further */

		/* are the conditions superset of the existing dependency? */
		for (j = 0; j < (dep->nattributes-1); j++)
		{
			bool found = false;

			for (l = 0; l < (k-1); l++)
			{
				if (tmp[l] == dep->attributes[j])
				{
					found = true;
					break;
				}
			}

			/* we've found an attribute not included in the new dependency */
			if (! found)
			{
				contained = false;
				break;
			}
		}

		/* we've found an existing dependency, trivially proving the new one */
		if (contained)
		{
			implied = true;
			break;
		}
	}

	pfree(tmp);

	return implied;
}

/*
 * validates functional dependency on the data
 *
 * An actual work horse of detecting functional dependencies. Given a variation
 * of k attributes, it checks that the first (k-1) are sufficient to determine
 * the last one.
 */
static bool
dependency_is_valid(int numrows, HeapTuple *rows, int k, int * dependency,
					VacAttrStats **stats, int2vector *attrs)
{
	int i, j;
	int nvalues = numrows * k;

	/*
	 * XXX Maybe the threshold should be somehow related to the number of
	 *     distinct values in the combination of columns we're analyzing.
	 *     Assuming the distribution is uniform, we can estimate the average
	 *     group size and use it as a threshold, similarly to what we do for
	 *     MCV lists.
	 */
	int min_group_size = 3;

	/* number of groups supporting / contradicting the dependency */
	int n_supporting = 0;
	int n_contradicting = 0;

	/* counters valid within a group */
	int group_size = 0;
	int n_violations = 0;

	int n_supporting_rows = 0;
	int n_contradicting_rows = 0;

	/* sort info for all attributes columns */
	MultiSortSupport mss = multi_sort_init(k);

	/* data for the sort */
	SortItem *items  = (SortItem*)palloc0(numrows * sizeof(SortItem));
	Datum    *values = (Datum*)palloc0(sizeof(Datum) * nvalues);
	bool     *isnull = (bool*)palloc0(sizeof(bool) * nvalues);

	/* fix the pointers to values/isnull */
	for (i = 0; i < numrows; i++)
	{
		items[i].values = &values[i * k];
		items[i].isnull = &isnull[i * k];
	}

	/*
	 * Verify the dependency (a,b,...)->z, using a rather simple algorithm:
	 *
	 * (a) sort the data lexicographically
	 *
	 * (b) split the data into groups by first (k-1) columns
	 *
	 * (c) for each group count different values in the last column
	 */

	/* prepare the sort function for the first dimension, and SortItem array */
	for (i = 0; i < k; i++)
	{
		multi_sort_add_dimension(mss, i, dependency[i], stats);

		/* accumulate all the data for both columns into an array and sort it */
		for (j = 0; j < numrows; j++)
		{
			items[j].values[i]
				= heap_getattr(rows[j], attrs->values[dependency[i]],
							   stats[i]->tupDesc, &items[j].isnull[i]);
		}
	}

	/* sort the items so that we can detect the groups */
	qsort_arg((void *) items, numrows, sizeof(SortItem),
			  multi_sort_compare, mss);

	/*
	 * Walk through the sorted array, split it into rows according to the first
	 * (k-1) columns. If there's a single value in the last column, we count
	 * the group as 'supporting' the functional dependency. Otherwise we count
	 * it as contradicting.
	 *
	 * We also require a group to have a minimum number of rows to be considered
	 * useful for supporting the dependency. Contradicting groups may be of
	 * any size, though.
	 *
	 * XXX The minimum size requirement makes it impossible to identify case
	 *     when both columns are unique (or nearly unique), and therefore
	 *     trivially functionally dependent.
	 */

	/* start with the first row forming a group */
	group_size  = 1;

	for (i = 1; i < numrows; i++)
	{
		/* end of the preceding group */
		if (multi_sort_compare_dims(0, (k-2), &items[i-1], &items[i], mss) != 0)
		{
			/*
			 * If there is a single are no contradicting rows, count the group
			 * as supporting, otherwise contradicting.
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
		/* first colums match, but the last one does not (so contradicting) */
		else if (multi_sort_compare_dims((k-1), (k-1), &items[i-1], &items[i], mss) != 0)
			n_violations += 1;

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

	pfree(items);
	pfree(values);
	pfree(isnull);
	pfree(mss);

	/*
	 * See if the number of rows supporting the association is at least 10x the
	 * number of rows violating the hypothetical dependency.
	 */
	return (n_supporting_rows > (n_contradicting_rows * 10));
}

/*
 * detects functional dependencies between groups of columns
 *
 * Generates all possible subsets of columns (variations) and checks if the
 * last one is determined by the preceding ones. For example given 3 columns,
 * there are 12 variations (6 for variations on 2 columns, 6 for 3 columns):
 *
 *     two columns            three columns
 *     -----------            -------------
 *     (a) -> c               (a,b) -> c
 *     (b) -> c               (b,a) -> c
 *     (a) -> b               (a,c) -> b
 *     (c) -> b               (c,a) -> b
 *     (c) -> a               (c,b) -> a
 *     (b) -> a               (b,c) -> a
 *
 * Clearly some of the variations are redundant, as the order of columns on the
 * left side does not matter. This is detected in dependency_is_implied, and
 * those dependencies are ignored.
 *
 * We however do not detect that dependencies are transitively implied. For
 * example given dependencies
 *
 *     (a) -> b
 *     (b) -> c
 *
 * then
 *
 *     (a) -> c
 *
 * is trivially implied. However we don't detect that and all three dependencies
 * will get included in the resulting set. Eliminating such transitively implied
 * dependencies is future work.
 */
MVDependencies
build_mv_dependencies(int numrows, HeapTuple *rows, int2vector *attrs,
					  VacAttrStats **stats)
{
	int i;
	int k;
	int numattrs = attrs->dim1;

	/* result */
	MVDependencies	dependencies = NULL;

	Assert(numattrs >= 2);

	/*
	 * We'll try build functional dependencies starting from the smallest ones
	 * covering jut 2 columns, to the largest ones, covering all columns
	 * included int the statistics. We start from the smallest ones because
	 * we want to be able to skip already implied ones.
	 */
	for (k = 2; k <= numattrs; k++)
	{
		int *dependency;	/* array with k elements */

		/* prepare a generator of variation */
		VariationGenerator generator = generator_init(attrs, k);

		/* generate all possible variations of k values (out of n) */
		while ((dependency = generator_next(generator, attrs)))
		{
			MVDependency d;

			/* skip dependencies that are already trivially implied */
			if (dependency_is_implied(dependencies, k, dependency, attrs))
				continue;

			/* also skip dependencies that don't seem to be valid */
			if (! dependency_is_valid(numrows, rows, k, dependency, stats, attrs))
				continue;

			d = (MVDependency)palloc0(offsetof(MVDependencyData, attributes)
											   + k * sizeof(int));

			/* copy the dependency, but translate it to actuall attnums */
			d->nattributes = k;
			for (i = 0; i < k; i++)
				d->attributes[i] = attrs->values[dependency[i]];

			/* initialize the list of dependencies */
			if (dependencies == NULL)
			{
				dependencies
					= (MVDependencies)palloc0(sizeof(MVDependenciesData));

				dependencies->magic = MVSTAT_DEPS_MAGIC;
				dependencies->type  = MVSTAT_DEPS_TYPE_BASIC;
				dependencies->ndeps = 0;
			}

			dependencies->ndeps++;
			dependencies = (MVDependencies)repalloc(dependencies,
							offsetof(MVDependenciesData, deps)
							+ dependencies->ndeps * sizeof(MVDependency));

			dependencies->deps[dependencies->ndeps-1] = d;
		}

		/* we're done with variations of k elements, so free the generator */
		generator_free(generator);
	}

	return dependencies;
}


/*
 * serialize list of dependencies into a bytea
 */
bytea *
serialize_mv_dependencies(MVDependencies dependencies)
{
	int i;
	bytea * output;
	char *tmp;

	/* we need to store ndeps, with a number of attributes for each one */
	Size len = VARHDRSZ + offsetof(MVDependenciesData, deps)
			  + sizeof(int) * dependencies->ndeps;

	/* and also include space for the actual attribute numbers */
	for (i = 0; i < dependencies->ndeps; i++)
		len += (sizeof(int16) * dependencies->deps[i]->nattributes);

	output = (bytea*)palloc0(len);
	SET_VARSIZE(output, len);

	tmp = VARDATA(output);

	/* first, store the number of dimensions / items */
	memcpy(tmp, dependencies, offsetof(MVDependenciesData, deps));
	tmp += offsetof(MVDependenciesData, deps);

	/* store number of attributes and attribute numbers for each dependency */
	for (i = 0; i < dependencies->ndeps; i++)
	{
		MVDependency d = dependencies->deps[i];

		memcpy(tmp, &(d->nattributes), sizeof(int));
		tmp += sizeof(int);

		memcpy(tmp, d->attributes, sizeof(int16) * d->nattributes);
		tmp += sizeof(int16) * d->nattributes;

		Assert(tmp <= ((char*)output + len));
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
		elog(ERROR, "invalid dependency type %d (expected %dd)",
			 dependencies->type, MVSTAT_DEPS_MAGIC);

	if (dependencies->type != MVSTAT_DEPS_TYPE_BASIC)
		elog(ERROR, "invalid dependency type %d (expected %dd)",
			 dependencies->type, MVSTAT_DEPS_TYPE_BASIC);

	Assert(dependencies->ndeps > 0);

	/* what minimum bytea size do we expect for those parameters */
	expected_size = offsetof(MVDependenciesData,deps) +
					dependencies->ndeps * (sizeof(int) + sizeof(int16) * 2);

	if (VARSIZE_ANY_EXHDR(data) < expected_size)
		elog(ERROR, "invalid dependencies size %ld (expected at least %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	/* allocate space for the MCV items */
	dependencies = repalloc(dependencies, offsetof(MVDependenciesData,deps)
							+ (dependencies->ndeps * sizeof(MVDependency)));

	for (i = 0; i < dependencies->ndeps; i++)
	{
		int k;
		MVDependency d;

		/* number of attributes */
		memcpy(&k, tmp, sizeof(int));
		tmp += sizeof(int);

		/* is the number of attributes valid? */
		Assert((k >= 2) && (k <= MVSTATS_MAX_DIMENSIONS));

		/* now that we know the number of attributes, allocate the dependency */
		d = (MVDependency)palloc0(offsetof(MVDependencyData, attributes)
								  + k * sizeof(int));

		d->nattributes = k;

		/* copy attribute numbers */
		memcpy(d->attributes, tmp, sizeof(int16) * d->nattributes);
		tmp += sizeof(int16) * d->nattributes;

		dependencies->deps[i] = d;

		/* still within the bytea */
		Assert(tmp <= ((char*)data + VARSIZE_ANY(data)));
	}

	/* we should have consumed the whole bytea exactly */
	Assert(tmp == ((char*)data + VARSIZE_ANY(data)));

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

/*
 * print the dependencies
 *
 * TODO  Would be nice if this printed column names (instead of just attnums).
 */
Datum
pg_mv_stats_dependencies_show(PG_FUNCTION_ARGS)
{
	int		i, j;
	bytea   *data = PG_GETARG_BYTEA_P(0);
	StringInfoData	buf;

	MVDependencies dependencies = deserialize_mv_dependencies(data);

	if (dependencies == NULL)
		PG_RETURN_NULL();

	initStringInfo(&buf);

	for (i = 0; i < dependencies->ndeps; i++)
	{
		MVDependency dependency = dependencies->deps[i];

		if (i > 0)
			appendStringInfo(&buf, ", ");

		/* conditions */
		appendStringInfoChar(&buf, '(');
		for (j = 0; j < dependency->nattributes-1; j++)
		{
			if (j > 0)
				appendStringInfoChar(&buf, ',');

			appendStringInfo(&buf, "%d", dependency->attributes[j]);
		}

		/* the implied attribute */
		appendStringInfo(&buf, ") => %d",
						 dependency->attributes[dependency->nattributes-1]);
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
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
