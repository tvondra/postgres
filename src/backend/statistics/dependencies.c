/*-------------------------------------------------------------------------
 *
 * dependencies.c
 *	  POSTGRES functional dependencies
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/statistics/dependencies.c
 *
 *-------------------------------------------------------------------------
 */

#include "common.h"

#include "utils/bytea.h"
#include "utils/lsyscache.h"

/*
 * Internal state for DependencyGenerator of dependencies. Dependencies are similar to
 * k-permutations of n elements, except that the order does not matter for the
 * first (k-1) elements. That is, (a,b=>c) and (b,a=>c) are equivalent.
 */
typedef struct DependencyGeneratorData
{
	int				k;				/* size of the dependency */
	int				current;		/* next dependency to return (index) */
	AttrNumber		ndependencies;	/* number of dependencies generated */
	AttrNumber	   *dependencies;	/* array of pre-generated dependencies  */
} DependencyGeneratorData;

typedef DependencyGeneratorData *DependencyGenerator;

static void
generate_dependencies_recurse(DependencyGenerator state, AttrNumber n, int index,
							  AttrNumber start, AttrNumber *current)
{
	/*
	 * The generator handles the first (k-1) elements differently from
	 * the last element.
	 */
	if (index < (state->k - 1))
	{
		AttrNumber	i;

		/*
		 * The first (k-1) values have to be in ascending order, which we
		 * generate recursively.
		 */

		for (i = start; i < n; i++)
		{
			current[index] = i;
			generate_dependencies_recurse(state, n, (index+1), (i+1), current);
		}
	}
	else
	{
		int i;

		/*
		 * the last element is the implied value, which does not respect the
		 * ascending order. We just need to check that the value is not in the
		 * first (k-1) elements.
		 */

		for (i = 0; i < n; i++)
		{
			int		j;
			bool	match = false;

			current[index] = i;

			for (j = 0; j < index; j++)
			{
				if (current[j] == i)
				{
					match = true;
					break;
				}
			}

			/*
			 * If the value is not found in the first part of the dependency,
			 * we're done.
			 */
			if (! match)
			{
				state->dependencies = (AttrNumber *)repalloc(state->dependencies,
					state->k * (state->ndependencies + 1) * sizeof(AttrNumber));
				memcpy(&state->dependencies[(state->k * state->ndependencies)],
					   current, state->k * sizeof(AttrNumber));
				state->ndependencies++;
			}
		}
	}
}

/* generate all dependencies (k-permutations of n elements) */
static void
generate_dependencies(DependencyGenerator state, int n)
{
	AttrNumber *current = (AttrNumber *) palloc0(sizeof(AttrNumber) * state->k);

	generate_dependencies_recurse(state, n, 0, 0, current);

	pfree(current);
}

/*
 * initialize the DependencyGenerator of variations, and prebuild the variations
 *
 * This pre-builds all the variations. We could also generate them in
 * DependencyGenerator_next(), but this seems simpler.
 */
static DependencyGenerator
DependencyGenerator_init(int2vector *attrs, int k)
{
	int			n = attrs->dim1;
	DependencyGenerator state;

	Assert((n >= k) && (k > 0));

	/* allocate the DependencyGenerator state as a single chunk of memory */
	state = (DependencyGenerator) palloc0(sizeof(DependencyGeneratorData));
	state->dependencies = (AttrNumber *)palloc(k * sizeof(AttrNumber));

	state->ndependencies = 0;
	state->current = 0;
	state->k = k;

	/* now actually pre-generate all the variations */
	generate_dependencies(state, n);

	return state;
}

/* free the DependencyGenerator state */
static void
DependencyGenerator_free(DependencyGenerator state)
{
	/* we've allocated a single chunk, so just free it */
	pfree(state);
}

/* generate next combination */
static AttrNumber *
DependencyGenerator_next(DependencyGenerator state, int2vector *attrs)
{
	if (state->current == state->ndependencies)
		return NULL;

	return &state->dependencies[state->k * state->current++];
}


/*
 * validates functional dependency on the data
 *
 * An actual work horse of detecting functional dependencies. Given a variation
 * of k attributes, it checks that the first (k-1) are sufficient to determine
 * the last one.
 */
static double
dependency_degree(int numrows, HeapTuple *rows, int k, AttrNumber *dependency,
				  VacAttrStats **stats, int2vector *attrs)
{
	int			i,
				j;
	int			nvalues = numrows * k;
	MultiSortSupport mss;
	SortItem   *items;
	Datum	   *values;
	bool	   *isnull;

	/*
	 * XXX Maybe the threshold should be somehow related to the number of
	 * distinct values in the combination of columns we're analyzing. Assuming
	 * the distribution is uniform, we can estimate the average group size and
	 * use it as a threshold, similarly to what we do for MCV lists.
	 */
	int			min_group_size = 3;

	/* counters valid within a group */
	int			group_size = 0;
	int			n_violations = 0;

	/* total number of rows supporting (consistent with) the dependency */
	int			n_supporting_rows = 0;

	/* Make sure we have at least two input attributes. */
	Assert(k >= 2);

	/* sort info for all attributes columns */
	mss = multi_sort_init(k);

	/* data for the sort */
	items = (SortItem *) palloc0(numrows * sizeof(SortItem));
	values = (Datum *) palloc0(sizeof(Datum) * nvalues);
	isnull = (bool *) palloc0(sizeof(bool) * nvalues);

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
	 * Walk through the sorted array, split it into rows according to the
	 * first (k-1) columns. If there's a single value in the last column, we
	 * count the group as 'supporting' the functional dependency. Otherwise we
	 * count it as contradicting.
	 *
	 * We also require a group to have a minimum number of rows to be
	 * considered useful for supporting the dependency. Contradicting groups
	 * may be of any size, though.
	 *
	 * XXX The minimum size requirement makes it impossible to identify case
	 * when both columns are unique (or nearly unique), and therefore
	 * trivially functionally dependent.
	 */

	/* start with the first row forming a group */
	group_size = 1;

	for (i = 1; i <= numrows; i++)
	{
		/*
		 * Check if the group ended, which may be either because we processed
		 * all the items (i==numrows), or because the i-th item is not equal
		 * to the preceding one.
		 */
		if ((i == numrows) ||
			(multi_sort_compare_dims(0, (k - 2), &items[i - 1], &items[i], mss) != 0))
		{
			/*
			 * Do accounting for the preceding group, and reset counters.
			 *
			 * If there were no contradicting rows in the group, count the
			 * rows as supporting.
			 */
			if ((n_violations == 0) && (group_size >= min_group_size))
				n_supporting_rows += group_size;

			/* current values start a new group */
			n_violations = 0;
			group_size = 0;
		}
		/* first colums match, but the last one does not (so contradicting) */
		else if (multi_sort_compare_dim((k - 1), &items[i - 1], &items[i], mss) != 0)
			n_violations += 1;

		group_size += 1;
	}

	pfree(items);
	pfree(values);
	pfree(isnull);
	pfree(mss);

	/* Compute the 'degree of validity' as (supporting/total). */
	return (n_supporting_rows * 1.0 / numrows);
}

/*
 * detects functional dependencies between groups of columns
 *
 * Generates all possible subsets of columns (variations) and checks if the
 * last one is determined by the preceding ones. For example given 3 columns,
 * there are 12 variations (6 for variations on 2 columns, 6 for 3 columns):
 *
 *	   two columns			  three columns
 *	   -----------			  -------------
 *	   (a) -> c				  (a,b) -> c
 *	   (b) -> c				  (b,a) -> c
 *	   (a) -> b				  (a,c) -> b
 *	   (c) -> b				  (c,a) -> b
 *	   (c) -> a				  (c,b) -> a
 *	   (b) -> a				  (b,c) -> a
 *
 * XXX Currently this builds redundant dependencies, becuse (a,b => c) and
 * (b,a => c) is exactly the same thing, but both versions are generated
 * and stored in the statistics.
 */
MVDependencies
build_ext_dependencies(int numrows, HeapTuple *rows, int2vector *attrs,
					  VacAttrStats **stats)
{
	int			i;
	int			k;
	int			numattrs = attrs->dim1;

	/* result */
	MVDependencies dependencies = NULL;

	Assert(numattrs >= 2);

	/*
	 * We'll try build functional dependencies starting from the smallest ones
	 * covering just 2 columns, to the largest ones, covering all columns
	 * included int the statistics. We start from the smallest ones because we
	 * want to be able to skip already implied ones.
	 */
	for (k = 2; k <= numattrs; k++)
	{
		AttrNumber *dependency; /* array with k elements */

		/* prepare a DependencyGenerator of variation */
		DependencyGenerator DependencyGenerator = DependencyGenerator_init(attrs, k);

		/* generate all possible variations of k values (out of n) */
		while ((dependency = DependencyGenerator_next(DependencyGenerator, attrs)))
		{
			double			degree;
			MVDependency	d;

			/* compute how valid the dependency seems */
			degree = dependency_degree(numrows, rows, k, dependency, stats, attrs);

			/* if the dependency seems entirely invalid, don't bother storing it */
			if (degree == 0.0)
				continue;

			d = (MVDependency) palloc0(offsetof(MVDependencyData, attributes)
									   + k * sizeof(AttrNumber));

			/* copy the dependency (and keep the indexes into stakeys) */
			d->degree = degree;
			d->nattributes = k;
			for (i = 0; i < k; i++)
				d->attributes[i] = dependency[i];

			/* initialize the list of dependencies */
			if (dependencies == NULL)
			{
				dependencies
					= (MVDependencies) palloc0(sizeof(MVDependenciesData));

				dependencies->magic = STATS_DEPS_MAGIC;
				dependencies->type = STATS_DEPS_TYPE_BASIC;
				dependencies->ndeps = 0;
			}

			dependencies->ndeps++;
			dependencies = (MVDependencies) repalloc(dependencies,
										   offsetof(MVDependenciesData, deps)
								+dependencies->ndeps * sizeof(MVDependency));

			dependencies->deps[dependencies->ndeps - 1] = d;
		}

		/* we're done with variations of k elements, so free the DependencyGenerator */
		DependencyGenerator_free(DependencyGenerator);
	}

	return dependencies;
}


/*
 * serialize list of dependencies into a bytea
 */
bytea *
serialize_ext_dependencies(MVDependencies dependencies)
{
	int			i;
	bytea	   *output;
	char	   *tmp;
	Size		len;

	/* we need to store ndeps, with a number of attributes for each one */
	len = VARHDRSZ + offsetof(MVDependenciesData, deps) +
		  dependencies->ndeps * offsetof(MVDependencyData, attributes);

	/* and also include space for the actual attribute numbers and degrees */
	for (i = 0; i < dependencies->ndeps; i++)
		len += (sizeof(AttrNumber) * dependencies->deps[i]->nattributes);

	output = (bytea *) palloc0(len);
	SET_VARSIZE(output, len);

	tmp = VARDATA(output);

	/* first, store the number of dimensions / items */
	memcpy(tmp, dependencies, offsetof(MVDependenciesData, deps));
	tmp += offsetof(MVDependenciesData, deps);

	/* store number of attributes and attribute numbers for each dependency */
	for (i = 0; i < dependencies->ndeps; i++)
	{
		MVDependency d = dependencies->deps[i];

		memcpy(tmp, d, offsetof(MVDependencyData, attributes));
		tmp += offsetof(MVDependencyData, attributes);

		memcpy(tmp, d->attributes, sizeof(AttrNumber) * d->nattributes);
		tmp += sizeof(AttrNumber) * d->nattributes;

		Assert(tmp <= ((char *) output + len));
	}

	return output;
}

/*
 * Reads serialized dependencies into MVDependencies structure.
 */
MVDependencies
deserialize_ext_dependencies(bytea *data)
{
	int			i;
	Size		expected_size;
	MVDependencies dependencies;
	char	   *tmp;

	if (data == NULL)
		return NULL;

	if (VARSIZE_ANY_EXHDR(data) < offsetof(MVDependenciesData, deps))
		elog(ERROR, "invalid MVDependencies size %ld (expected at least %ld)",
			 VARSIZE_ANY_EXHDR(data), offsetof(MVDependenciesData, deps));

	/* read the MVDependencies header */
	dependencies = (MVDependencies) palloc0(sizeof(MVDependenciesData));

	/* initialize pointer to the data part (skip the varlena header) */
	tmp = VARDATA_ANY(data);

	/* get the header and perform basic sanity checks */
	memcpy(dependencies, tmp, offsetof(MVDependenciesData, deps));
	tmp += offsetof(MVDependenciesData, deps);

	if (dependencies->magic != STATS_DEPS_MAGIC)
		elog(ERROR, "invalid dependency magic %d (expected %dd)",
			 dependencies->magic, STATS_DEPS_MAGIC);

	if (dependencies->type != STATS_DEPS_TYPE_BASIC)
		elog(ERROR, "invalid dependency type %d (expected %dd)",
			 dependencies->type, STATS_DEPS_TYPE_BASIC);

	Assert(dependencies->ndeps > 0);

	/* what minimum bytea size do we expect for those parameters */
	expected_size = offsetof(MVDependenciesData, deps) +
		dependencies->ndeps * (offsetof(MVDependencyData, attributes) +
							   sizeof(AttrNumber) * 2);

	if (VARSIZE_ANY_EXHDR(data) < expected_size)
		elog(ERROR, "invalid dependencies size %ld (expected at least %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	/* allocate space for the MCV items */
	dependencies = repalloc(dependencies, offsetof(MVDependenciesData, deps)
							+(dependencies->ndeps * sizeof(MVDependency)));

	for (i = 0; i < dependencies->ndeps; i++)
	{
		double		degree;
		int			k;
		MVDependency d;

		/* degree of validity */
		memcpy(&degree, tmp, sizeof(double));
		tmp += sizeof(double);

		/* number of attributes */
		memcpy(&k, tmp, sizeof(AttrNumber));
		tmp += sizeof(AttrNumber);

		/* is the number of attributes valid? */
		Assert((k >= 2) && (k <= STATS_MAX_DIMENSIONS));

		/* now that we know the number of attributes, allocate the dependency */
		d = (MVDependency) palloc0(offsetof(MVDependencyData, attributes) +
								   (k * sizeof(AttrNumber)));

		d->degree = degree;
		d->nattributes = k;

		/* copy attribute numbers */
		memcpy(d->attributes, tmp, sizeof(AttrNumber) * d->nattributes);
		tmp += sizeof(AttrNumber) * d->nattributes;

		dependencies->deps[i] = d;

		/* still within the bytea */
		Assert(tmp <= ((char *) data + VARSIZE_ANY(data)));
	}

	/* we should have consumed the whole bytea exactly */
	Assert(tmp == ((char *) data + VARSIZE_ANY(data)));

	return dependencies;
}

/*
 * dependency_is_fully_matched
 *		checks that a functional dependency is fully matched given clauses on
 * 		attributes (assuming the clauses are suitable equality clauses)
 */
bool
dependency_is_fully_matched(MVDependency dependency, Bitmapset *attnums,
							int16 *attmap)
{
	int j;

	/*
	 * Check that the dependency actually is fully covered by clauses. We
	 * have to translate all attribute numbers, as those are referenced
	 */
	for (j = 0; j < dependency->nattributes; j++)
	{
		int attnum = attmap[dependency->attributes[j]];

		if (! bms_is_member(attnum, attnums))
			return false;
	}

	return true;
}

/*
 * dependency_implies_attribute
 *		check that the attnum matches is implied by the functional dependency
 */
bool
dependency_implies_attribute(MVDependency dependency, AttrNumber attnum,
							 int16 *attmap)
{
	if (attnum == attmap[dependency->attributes[dependency->nattributes-1]])
		return true;

	return false;
}

MVDependencies
load_ext_dependencies(Oid mvoid)
{
	bool		isnull = false;
	Datum		deps;

	/* Prepare to scan pg_statistic_ext for entries having indrelid = this rel. */
	HeapTuple	htup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(mvoid));

	Assert(stats_are_enabled(htup, STATS_EXT_DEPENDENCIES));
	Assert(stats_are_built(htup, STATS_EXT_DEPENDENCIES));

	deps = SysCacheGetAttr(STATEXTOID, htup,
						   Anum_pg_statistic_ext_stadependencies, &isnull);

	Assert(!isnull);

	ReleaseSysCache(htup);

	return deserialize_ext_dependencies(DatumGetByteaP(deps));
}

/*
 * pg_dependencies_in		- input routine for type pg_dependencies.
 *
 * pg_dependencies is real enough to be a table column, but it has no operations
 * of its own, and disallows input too
 *
 * XXX This is inspired by what pg_node_tree does.
 */
Datum
pg_dependencies_in(PG_FUNCTION_ARGS)
{
	/*
	 * pg_node_list stores the data in binary form and parsing text input is
	 * not needed, so disallow this.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_dependencies")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

/*
 * pg_dependencies		- output routine for type pg_dependencies.
 *
 * histograms are serialized into a bytea value, so we simply call byteaout()
 * to serialize the value into text. But it'd be nice to serialize that into
 * a meaningful representation (e.g. for inspection by people).
 */
Datum
pg_dependencies_out(PG_FUNCTION_ARGS)
{
	int i, j;
	char		   *ret;
	StringInfoData	str;

	bytea	   *data = PG_GETARG_BYTEA_PP(0);

	MVDependencies dependencies = deserialize_ext_dependencies(data);

	initStringInfo(&str);
	appendStringInfoString(&str, "[");

	for (i = 0; i < dependencies->ndeps; i++)
	{
		MVDependency dependency = dependencies->deps[i];

		if (i > 0)
			appendStringInfoString(&str, ", ");

		appendStringInfoString(&str, "{");

		for (j = 0; j < dependency->nattributes; j++)
		{
			if (j == dependency->nattributes-1)
				appendStringInfoString(&str, " => ");
			else if (j > 0)
				appendStringInfoString(&str, ", ");

			appendStringInfo(&str, "%d", dependency->attributes[j]);
		}

		appendStringInfo(&str, " : %f", dependency->degree);

		appendStringInfoString(&str, "}");
	}

	appendStringInfoString(&str, "]");

	ret = pstrdup(str.data);
	pfree(str.data);

	PG_RETURN_CSTRING(ret);
}

/*
 * pg_dependencies_recv		- binary input routine for type pg_dependencies.
 */
Datum
pg_dependencies_recv(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_dependencies")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

/*
 * pg_dependencies_send		- binary output routine for type pg_dependencies.
 *
 * XXX Histograms are serialized into a bytea value, so let's just send that.
 */
Datum
pg_dependencies_send(PG_FUNCTION_ARGS)
{
	return byteasend(fcinfo);
}
