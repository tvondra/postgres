/*-------------------------------------------------------------------------
 *
 * execFilters.c
 *	  This code provides support for pushed-down filters.
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execFilters.c
 *
 *
 * FIXME Create a separate memory context for each filter, to make the
 * memory usage easier to understand.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planmain.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ruleutils.h"
#include "utils/typcache.h"
#include <math.h>

/*
 * ExecFilterGetHashValue
 *		Compute the hash value for a tuple
 *
 * The tuple to be tested must be in econtext->ecxt_outertuple (thus Vars in
 * the hashkeys expressions need to have OUTER_VAR as varno).
 *
 * A true result means the tuple's hash value has been successfully computed
 * and stored at *hashvalue.  A false result means the tuple cannot match
 * because it contains a null attribute, and hence it should be discarded
 * immediately.  (If keep_nulls is true then false is never returned.)
 *
 * XXX We probably don't need to worry about keep_nulls, because we don't
 * actually keep the NULLs - it's probably enough to remember there were
 * NULLs, assuming the operator is strict (in which case NULLs will never
 * match).
 */
static bool
ExecFilterGetHashValue(FilterState *filter,
					 ExprContext *econtext,
					 bool keep_nulls,
					 uint32 *hashvalue)
{
	uint32		hashkey = 0;
	FmgrInfo   *hashfunctions;
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	hashfunctions = filter->hashfunctions;

	foreach(hk, filter->hashclauses)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/* combine successive hashkeys by rotating */
		hashkey = pg_rotate_left32(hashkey, 1);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull);

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (filter->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */

			/*
			 * XXX Ignore if any of the values is NULL. At the moment we only
			 * have a single-key filters, but this should apply even to multiple
			 * keys I think.
			 */
			MemoryContextSwitchTo(oldContext);
			return false;	/* cannot match */
		}
		else
		{
			/* Compute the hash function */
			hashkey ^= DatumGetUInt32(FunctionCall1Coll(&hashfunctions[i], filter->collations[i], keyval));
		}

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	*hashvalue = hashkey;
	return true;
}

static bool
ExecFilterGetHashValueX(FilterState *filter,
						ExprContext *econtext,
						Datum *values,
						bool *isnull,
						bool keep_nulls,
						uint32 *hashvalue)
{
	uint32		hashkey = 0;
	FmgrInfo   *hashfunctions;
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	hashfunctions = filter->hashfunctions;

	for (int i = 0; i < list_length(filter->hashclauses); i++)
	{
		Datum		keyval;
		bool		isNull;

		/* combine successive hashkeys by rotating */
		hashkey = pg_rotate_left32(hashkey, 1);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = values[i];
		isNull = isnull[i];

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (filter->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */

			/*
			 * XXX Ignore if any of the values is NULL. At the moment we only
			 * have a single-key filters, but this should apply even to multiple
			 * keys I think.
			 */
			MemoryContextSwitchTo(oldContext);
			return false;	/* cannot match */
		}
		else
		{
			/* Compute the hash function */
			hashkey ^= DatumGetUInt32(FunctionCall1Coll(&hashfunctions[i], filter->collations[i], keyval));
		}
	}

	MemoryContextSwitchTo(oldContext);

	*hashvalue = hashkey;
	return true;
}

/*
 * ExecFilterGetValues
 *		Extract values from the tuple.
 *
 * Pretty much exactly the same as ExecFilterGetHashValue, but it returns
 * the values instead of hashing them.
 */
static bool
ExecFilterGetValues(FilterState *filter,
						   ExprContext *econtext,
						   bool keep_nulls,
						   Datum *values)
{
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	foreach(hk, filter->hashclauses)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull);

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (filter->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;
			}

			/* FIXME handle NULLs correctly, instead of just ignoring them */
			MemoryContextSwitchTo(oldContext);
			return false;

			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
		}
		else
		{
			int16	typlen;
			bool	typbyval;
			char	typalign;
			get_typlenbyvalalign(filter->types[i], &typlen, &typbyval, &typalign);

			values[i] = datumCopy(keyval, typbyval, typlen);
		}

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	return true;
}



/*
 * ExecFilterGetValues
 *		Extract values from the tuple.
 *
 * Pretty much exactly the same as ExecFilterGetHashValue, but it returns
 * the values instead of hashing them.
 */
static bool
ExecFilterGetValuesX(FilterState *filter,
						   ExprContext *econtext,
						   Datum *values,
						   bool *isnull,
						   bool keep_nulls,
						   Datum *entry)
{
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	for (int i = 0; i < list_length(filter->hashclauses); i++)
	{
		Datum		keyval;
		bool		isNull;

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = values[i];
		isNull = isnull[i];

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (filter->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;
			}

			/* FIXME handle NULLs correctly, instead of just ignoring them */
			MemoryContextSwitchTo(oldContext);
			return false;

			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
		}
		else
		{
			int16	typlen;
			bool	typbyval;
			char	typalign;
			get_typlenbyvalalign(filter->types[i], &typlen, &typbyval, &typalign);

			entry[i] = datumCopy(keyval, typbyval, typlen);
		}
	}

	MemoryContextSwitchTo(oldContext);

	return true;
}

typedef struct qsort_cxt
{
	int		nelements;
	SortSupportData	ssup[FLEXIBLE_ARRAY_MEMBER];
} qsort_cxt;

/* A simple pair of values, representing an interval. */
typedef struct FilterRange
{
	Datum	start;
	Datum	end;
	bool	is_point;	/* point has (start == end) */
	bool	removed;		/* should be removed / compacted (by merging with
							 * the preceding range) ? */
	int		merged_into;	/* with which range to merge */
} FilterRange;

typedef struct RangeDistance
{
	int		index;		/* distance is between (index-1) and index */
	double	distance;	/* distance */
	int		random;		/* to randomize ordering of equal distances */
} RangeDistance;

static int
filter_distance_comparator(const void *a, const void *b)
{
	RangeDistance *da = (RangeDistance *) a;
	RangeDistance *db = (RangeDistance *) b;

	if (da->distance < db->distance)
		return -1;
	else if (da->distance > db->distance)
		return 1;

	if (da->random < db->random)
		return -1;
	else if (da->random > db->random)
		return 1;

	return 0;
}

/*
 * Simple comparator of Datum arrays.
 *
 * FIXME This only works for byval types, needs to check byref types too. That
 * requires looking up comparators for types etc.
 */
static int
filter_value_comparator(const void *a, const void *b, void *c)
{
	Datum  *da = (Datum *) a;
	Datum  *db = (Datum *) b;
	qsort_cxt *cxt = (qsort_cxt *) c;

	for (int i = 0; i < cxt->nelements; i++)
	{
		int r = ApplySortComparator(da[i], false, db[i], false, &cxt->ssup[i]);

		if (r != 0)
			return r;
	}

	return 0;
}

/*
 * FilterRange comparator. It compares by start, then by end.
 *
 * FIXME This needs to use a comparator for the particular data type, instead of
 * just comparing the Datum values.
 */
static int
filter_range_cmp(const void *a, const void *b, qsort_cxt *cxt)
{
	int				r;
	FilterRange	   *ra = (FilterRange *) a;
	FilterRange	   *rb = (FilterRange *) b;

	r = filter_value_comparator(&ra->start, &rb->start, cxt);
	if (r != 0)
		return r;

	return filter_value_comparator(&ra->end, &rb->end, cxt);
}

/* FIXME Isn't this really just what filter_range_cmp already does? */
static int
filter_range_comparator(const void *a, const void *b, void *c)
{
	FilterRange  *ra = (FilterRange *) a;
	FilterRange  *rb = (FilterRange *) b;
	qsort_cxt *cxt = (qsort_cxt *) c;

	for (int i = 0; i < cxt->nelements; i++)
	{
		int r;

		/* FIXME subscript the start/end */
		r = ApplySortComparator(ra->start, false,
								rb->start, false, &cxt->ssup[i]);
		if (r != 0)
			return r;

		r = ApplySortComparator(ra->end, false,
								rb->end, false, &cxt->ssup[i]);
		if (r != 0)
			return r;
	}

	return 0;
}

/*
 * ranges_overlap
 *		returns true iff the two ranges overlap
 *
 * Assumes the ranges are sorted by filter_range_cmp, i.e. first by start
 * and then by end.
 *
 * FIXME This needs to use a comparator for the particular data type, instead of
 * just comparing the Datum values.
 */
static bool
ranges_overlap(FilterRange *ra, FilterRange *rb, qsort_cxt *cxt)
{
	if (filter_value_comparator(&ra->end, &rb->start, cxt) < 0)
		return false;

	if (filter_value_comparator(&ra->start, &rb->end, cxt) > 0)
		return false;

	return true;
}

/*
 * ranges_contiguous
 *		returns true iff the ranges are contiguous
 *
 * Assumes the ranges are sorted by filter_range_cmp, i.e. first by start
 * and then by end.
 *
 * Ranges are contiguous when there can be no values in between them. For
 * integer types this means the second range either starts where the first
 * range ends, or within distance "1" from that value.
 *
 * FIXME This needs to use a comparator for the particular data type, instead of
 * just comparing the Datum values.
 */
static bool
ranges_contiguous(FilterState *filter, FilterRange *ra, FilterRange *rb)
{
	qsort_cxt  *cxt = (qsort_cxt *) filter->private_data;
	Oid 		plusOid;
	FmgrInfo	opproc;
	Datum		increment;
	Datum		r;

#ifdef USE_ASSERT_CHECKING
	/* ranges are sorted in ascending order */
	Assert(ApplySortComparator(ra->end, false, rb->start, false, &cxt->ssup[0]) <= 0);
#endif

	plusOid = OpernameGetOprid(
					list_make2(makeString("pg_catalog"), makeString("+")),
					filter->types[0], filter->types[0]);

	fmgr_info(get_opcode(plusOid), &opproc);

	/* XXX maybe pass '1' into the type input function, instead of hardcoding
	 * it like this? */
	if (filter->types[0] == INT2OID)
		increment = Int16GetDatum(1);
	else if (filter->types[0] == INT4OID)
		increment = Int32GetDatum(1);
	else /* INT8OID */
		increment = Int64GetDatum(1);

	r = FunctionCall2Coll(&opproc, InvalidOid, ra->end, increment);

	if (ApplySortComparator(r, false, rb->start, false, &cxt->ssup[0]) >= 0)
		return true;

	return false;
}

static void
dump_filter(FilterState *filter)
{
	Oid		outfuncoid;
	bool	isvarlena;
	Datum  *values = (Datum *) filter->data;

	Assert((filter->filter_type == FilterTypeRange) ||
		   (filter->filter_type == FilterTypeExact));

	Assert(list_length(filter->clauses) == 1);
	Assert(list_length(filter->hashclauses) == 1);

	getTypeOutputInfo(filter->types[0], &outfuncoid, &isvarlena);

	elog(WARNING, "===============================================");

	elog(WARNING, "filter %p nranges %d nvalues %d",
		 filter, filter->nranges, filter->nvalues);

	for (int i = 0; i < filter->nvalues; i++)
	{
		Datum	r;
		Datum	value = values[i];

		r = OidFunctionCall1Coll(outfuncoid, filter->collations[0], value);
		elog(WARNING, "%d => '%s'", i, DatumGetPointer(r));
	}

}

static void
dump_ranges(FilterState *filter, FilterRange *ranges, int nranges)
{
	Oid		outfuncoid;
	bool	isvarlena;

	Assert(list_length(filter->clauses) == 1);
	Assert(list_length(filter->hashclauses) == 1);

	getTypeOutputInfo(filter->types[0], &outfuncoid, &isvarlena);

	for (int i = 0; i < nranges; i++)
	{
		Datum start = OidFunctionCall1Coll(outfuncoid, filter->collations[0], ranges[i].start);
		Datum end = OidFunctionCall1Coll(outfuncoid, filter->collations[0], ranges[i].end);

		elog(WARNING, "range %d => %s %s", i, DatumGetPointer(start), DatumGetPointer(end));
	}
}

/*
 * ExecFilterCompactRange
 *		Compact the range filter by combining closes values/ranges, etc.
 *
 * XXX This may not work if some of the values are very long (e.g. adding
 * a 64-kB value into a filter with work_mem=64kB can cause this).
 */
static void
ExecFilterCompactRange(FilterState *filter, bool reduce)
{
	Datum		   *values;
	FilterRange	   *ranges;
	int				nranges;
	int				rangeidx;
	qsort_cxt	   *cxt;
	int				nvalues_orig PG_USED_FOR_ASSERTS_ONLY;
	MemoryContext	oldCtx;

	Assert(filter->filter_type == FilterTypeRange);

	Assert(list_length(filter->clauses) == 1);
	Assert(list_length(filter->hashclauses) == 1);

	if (filter->nvalues < 1)
		return;

	oldCtx = MemoryContextSwitchTo(filter->buildCtx);

	// dump_filter(filter);

	/* deserialize the values into a simple Datum array */
	values = (Datum *) filter->data;
	cxt = (qsort_cxt *) filter->private_data;
	nvalues_orig = filter->nvalues;

	/* build the ranges (some of which may be just points with min==max) */
	nranges = (filter->nvalues - filter->nranges);
	ranges = palloc(sizeof(FilterRange) * nranges);

	/*
	 * use the first 2*nranges values for (min,max)
	 *
	 * FIXME this doesn't work for multi-column filters, the range boundaries
	 * need to be arrays, not individual Datum values.
	 */
	rangeidx = 0;
	for (int i = 0; i < filter->nranges; i++)
	{
		Assert(rangeidx < nranges);

		ranges[rangeidx].start = values[2*i];
		ranges[rangeidx].end = values[2*i + 1];
		ranges[rangeidx].is_point = false;
		ranges[rangeidx].removed = false;
		rangeidx++;
		Assert((2*i + 1) < filter->nvalues);
	}

	for (int i = 2 * filter->nranges; i < filter->nvalues; i++)
	{
		Assert(rangeidx < nranges);

		ranges[rangeidx].start = values[i];
		ranges[rangeidx].end = values[i];
		ranges[rangeidx].is_point = true;
		ranges[rangeidx].removed = false;
		rangeidx++;
		Assert(i < filter->nvalues);
	}

	// dump_ranges(filter, ranges, nranges);

	Assert(rangeidx == nranges);

	/* sort ranges by start/end */
	qsort_arg(ranges, nranges, sizeof(FilterRange),
			  filter_range_comparator, cxt);

	// dump_ranges(filter, ranges, nranges);

	/* combine overlapping ranges */
	rangeidx = 0;
	for (int i = 1; i < nranges; i++)
	{
		/*
		 * if the next range overlaps, combine them
		 *
		 * XXX This is problematic for multi-column filters, where some of
		 * the dimensions may overlap, some not. So combining overlapping
		 * ranges does not produce equivalent range to a union of ranges,
		 * because the overlap may be only partial (imagine two boxes that
		 * only partially overlap).
		 */
		if (ranges_overlap(&ranges[rangeidx], &ranges[i], cxt))
		{
			ranges[rangeidx].end = Max(ranges[rangeidx].end, ranges[i].end);
			ranges[rangeidx].is_point = false;
			continue;
		}

		/* no overlap, we have found the next separate range */
		ranges[++rangeidx] = ranges[i];
		Assert(rangeidx < nranges);
	}

	/* the last used range index determines how many ranges we have */
	nranges = (rangeidx + 1);

	// dump_ranges(filter, ranges, nranges);

	/*
	 * combine contiguous ranges
	 *
	 * Ranges may not exactly overlap, but it may be impossible to have
	 * values between them (e.g. for integers, ranges [1.10] and [11,20]
	 * can be combined into [1,20] without losing any information).
	 *
	 * XXX Only do this for some integer data types.
	 */
	if ((filter->types[0] == INT2OID) ||
		(filter->types[0] == INT4OID) ||
		(filter->types[0] == INT8OID))
	{
		rangeidx = 0;
		for (int i = 1; i < nranges; i++)
		{
			if (ranges_contiguous(filter, &ranges[rangeidx], &ranges[i]))
			{
				ranges[rangeidx].end = ranges[i].end;
				ranges[rangeidx].is_point = false;
				continue;
			}

			ranges[++rangeidx] = ranges[i];
			Assert(rangeidx < nranges);
		}
	}

	/* again, the last used range index determines how many ranges we have */
	nranges = (rangeidx + 1);

	/*
	 * Until now, all the changes were lossless, i.e. we haven't lost any
	 * filtering information (unless we do some approximation when evaluating
	 * overlaps of ranges in multi-column filters).
	 *
	 * Now we're going to start to redude the number of ranges by merging the
	 * closest ones, etc. We'll do that until we reduce the size enough to
	 * accept a bunch of new values. Ideally, we'd probably do that based on
	 * size required to store the data, but that's either expensive (having
	 * to calculate the size over and over) or complex (tracking the changes
	 * as we go). It's easier to just count the ranges, and use that as an
	 * approximation.
	 *
	 * We shoot for 0.75 load, i.e. we want to get rid of 25% values (but
	 * we need to be careful about collapsed ranges, because joining two
	 * such ranges does not reduce anything).
	 */
	if (reduce && (nranges > 1))
	{
		int		idx;
		int		nvalues = 0;
		RangeDistance	*distances;

		distances = (RangeDistance *) palloc(sizeof(RangeDistance) * (nranges - 1));

		/* count how many filter entries we have to store */
		for (int i = 0; i < nranges; i++)
		{
			if (i > 0)
			{
				distances[i-1].index = i;
				distances[i-1].distance = (ranges[i].start - ranges[i-1].end);
				distances[i-1].random = rand();
			}

			nvalues += (ranges[i].is_point) ? 1 : 2;
		}

		elog(WARNING, "ranges %d values %d", nranges, nvalues);

		/* did we already reduce the filter enough */
		if (nvalues > filter->nvalues * 0.75)
		{
			/* sort distances from smallest */
			pg_qsort(distances, (nranges - 1), sizeof(RangeDistance),
					 filter_distance_comparator);

			for (int i = 0; i < (nranges - 1); i++)
			{
				/* which range to merge into the preceding one */
				int		idx1 = distances[i].index;

				/*
				 * Which range to merge into? start with immediately preceding
				 * one, but it might be already merged so lookup recursively.
				 */
				int		idx2 = (distances[i].index - 1);

				while (ranges[idx2].removed)
				{
					Assert(idx2 > ranges[idx2].merged_into);
					idx2 = ranges[idx2].merged_into;
				}

				Assert(idx2 < idx1);

				nvalues -= (ranges[idx1].is_point) ? 1 : 2;
				nvalues -= (ranges[idx2].is_point) ? 1 : 2;

				ranges[idx1].removed = true;
				ranges[idx1].merged_into = idx2;

				ranges[idx2].end = ranges[idx1].end;
				ranges[idx2].is_point = false;

				if (nvalues <= filter->nvalues * 0.75)
					break;
			}

			idx = 0;
			for (int i = 0; i < nranges; i++)
			{
				if (ranges[i].removed)
					continue;

				ranges[idx++] = ranges[i];
			}

			nranges = idx;
		}

		pfree(distances);
	}

	/*
	 * Transform the filter ranges back into the simple Datum array. We store
	 * the ranges first, then the points (ranges of with start==end).
	 */
	filter->nranges = 0;
	filter->nvalues = 0;

	for (int i = 0; i < nranges; i++)
	{
		/*
		 * XXX Naybe we should keep ranges of length 1 (i.e.  [a, a+1]) as individual
		 * values, not as ranges. That'll just complicate stuff e.g. when passing
		 * the filter to a remote server by making the conditions more complex, but
		 * it doesn't really save any space. The ranges_contiguous() just combines
		 * such values, as it only sees values incrementally.
		 */
		if (filter_value_comparator(&ranges[i].start, &ranges[i].end, cxt) != 0)
		{
			values[filter->nvalues++] = ranges[i].start;
			values[filter->nvalues++] = ranges[i].end;
			filter->nranges++;

			Assert(filter->nvalues <= nvalues_orig);
			Assert(filter->nvalues <= filter->nallocated);
		}
	}

	for (int i = 0; i < nranges; i++)
	{
		if (filter_value_comparator(&ranges[i].start, &ranges[i].end, cxt) == 0)
		{
			values[filter->nvalues++] = ranges[i].start;

			Assert(filter->nvalues <= nvalues_orig);
			Assert(filter->nvalues <= filter->nallocated);
		}
	}

	pfree(ranges);

	Assert(filter->nallocated >= filter->nvalues);

	if (filter->nvalues < filter->nallocated)
	{
		memset(&values[filter->nvalues], 0x7f, sizeof(Datum) * (filter->nallocated - filter->nvalues));
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(filter->buildCtx);

	MemoryContextStats(TopMemoryContext);
}

/*
 * ExecFilterAddRange
 *		Add values to a range filter.
 *
 * If there's not enough space for the new value, combine the values into
 * fewer ranges. We combine ranges that overlap or are contiguous, and then
 * we combine closest ranges.
 */
static bool
ExecFilterAddRange(FilterState *filter, bool keep_nulls, ExprContext *econtext)
{
	int		entrylen = list_length(filter->hashclauses);
	Datum  *entry;
	Datum  *values;

	Assert(filter->filter_type == FilterTypeRange);

	entry = palloc(sizeof(Datum) * entrylen);

	/* consider enlarging the filter as long as needed */
	while (filter->nallocated - filter->nvalues < entrylen)
	{
		/* FIXME handle nicely */
		if (filter->nallocated * sizeof(Datum) * 2 > work_mem * 1024L)
		{
			ExecFilterCompactRange(filter, true);
			continue;
		}

		filter->nallocated *= 2;
		filter->data = repalloc(filter->data, filter->nallocated * sizeof(Datum));
	}

	if (!ExecFilterGetValues(filter, econtext, keep_nulls, entry))
	{
		pfree(entry);
		return false;
	}

	values = (Datum *) filter->data;

	for (int i = 0; i < entrylen; i++)
	{
		int16	typlen;
		bool	typbyval;
		char	typalign;

		get_typlenbyvalalign(filter->types[i], &typlen, &typbyval, &typalign);

		Assert(filter->nvalues < filter->nallocated);

		values[filter->nvalues++] = datumCopy(entry[i], typbyval, typlen);
	}

	pfree(entry);

	return true;
}

/*
 * ExecFilterAddRange
 *		Add values to a range filter.
 *
 * If there's not enough space for the new value, combine the values into
 * fewer ranges. We combine ranges that overlap or are contiguous, and then
 * we combine closest ranges.
 */
static bool
ExecFilterAddRangeX(FilterState *filter, ExprContext *econtext, bool keep_nulls, Datum *values, bool *isnull)
{
	int		entrylen = list_length(filter->hashclauses);
	Datum  *entry;
	Datum  *tmp;

	Assert(filter->filter_type == FilterTypeRange);

	entry = palloc(sizeof(Datum) * entrylen);

	/* consider enlarging the filter as long as needed */
	while (filter->nallocated - filter->nvalues < entrylen)
	{
		/* FIXME handle nicely */
		if (filter->nallocated * sizeof(Datum) * 2 > work_mem * 1024L)
		{
			ExecFilterCompactRange(filter, true);
			continue;
		}

		filter->nallocated *= 2;
		filter->data = repalloc(filter->data, filter->nallocated * sizeof(Datum));
	}

	if (!ExecFilterGetValuesX(filter, econtext, values, isnull, keep_nulls, entry))
	{
		pfree(entry);
		return false;
	}

	tmp = (Datum *) filter->data;

	for (int i = 0; i < entrylen; i++)
	{
		int16	typlen;
		bool	typbyval;
		char	typalign;

		get_typlenbyvalalign(filter->types[i], &typlen, &typbyval, &typalign);

		Assert(filter->nvalues < filter->nallocated);

		tmp[filter->nvalues++] = datumCopy(entry[i], typbyval, typlen);
	}

	pfree(entry);

	return true;
}

/*
 * ExecFilterFinalizeRange
 *		Combine filter represented as ranges.
 *
 * This is pretty much a subset of what ExecFilterAddRange does - it sorts
 * the ranges and values, combines overlapping/contiguous ranges, etc. The one
 * thing it does not do is combining close ranges, because we don't need to fit
 * any more values into the filter. We just want to make it easier to use.
 *
 * FIXME refactor to reuse as much of the code with ExecFilterAddRange.
 */
static void
ExecFilterFinalizeRange(FilterState *filter)
{
	Assert(filter->filter_type == FilterTypeRange);

	ExecFilterCompactRange(filter, false);
}

static void
ExecFilterFinalizeExact(FilterState *filter)
{
	Datum  *values;
	Size	entrylen = (sizeof(Datum) * list_length(filter->hashclauses));
	qsort_cxt *cxt;

	Assert(filter->filter_type == FilterTypeExact);

	/* nothing to do if the filter represents no values */
	if (filter->nvalues == 0)
		return;

	values = (Datum *) filter->data;
	cxt = (qsort_cxt *) filter->private_data;

	/* nothing to do if the filter represents no values */
	qsort_arg(values, filter->nvalues, entrylen, filter_value_comparator, cxt);

	/* FIXME deduplicate values */
}

/* FIXME deduplicate the values first */
static bool
ExecFilterAddExact(FilterState *filter, bool keep_nulls, ExprContext *econtext)
{
	int		entrylen = list_length(filter->hashclauses);
	Datum  *entry;
	Datum  *values;

	Assert(filter->filter_type == FilterTypeExact);

	entry = palloc(sizeof(Datum) * entrylen);

	/* consider enlarging the filter as long as needed */
	while (filter->nallocated - filter->nvalues < entrylen)
	{
		/* FIXME handle nicely */
		if (filter->nallocated * sizeof(Datum) * 2 > work_mem * 1024L)
			elog(ERROR, "filter exceeds work_mem");

		filter->nallocated *= 2;
		filter->data = repalloc(filter->data, filter->nallocated * sizeof(Datum));
	}

	/* now we know there's enough space, so add the entry */
	ExecFilterGetValues(filter, econtext, keep_nulls, entry);

	values = (Datum *) filter->data;
	memcpy(&values[filter->nvalues], entry, entrylen * sizeof(Datum));

	pfree(entry);

	filter->nvalues++;

	return true;
}

/* FIXME deduplicate the values first */
static bool
ExecFilterAddExactX(FilterState *filter, bool keep_nulls, ExprContext *econtext, Datum *values, bool *isnull)
{
	int		entrylen = list_length(filter->hashclauses);
	Datum  *entry;
	Datum  *tmp;

	Assert(filter->filter_type == FilterTypeExact);

	entry = palloc(sizeof(Datum) * entrylen);

	/* consider enlarging the filter as long as needed */
	while (filter->nallocated - filter->nvalues < entrylen)
	{
		/* FIXME handle nicely */
		if (filter->nallocated * sizeof(Datum) * 2 > work_mem * 1024L)
			elog(ERROR, "filter exceeds work_mem");

		filter->nallocated *= 2;
		filter->data = repalloc(filter->data, filter->nallocated * sizeof(Datum));
	}

	/* now we know there's enough space, so add the entry */
	ExecFilterGetValuesX(filter, econtext, values, isnull, keep_nulls, entry);

	tmp = (Datum *) filter->data;
	memcpy(&tmp[filter->nvalues], entry, entrylen * sizeof(Datum));

	pfree(entry);

	filter->nvalues++;

	return true;
}


/*
 * ExecFilterFinalize
 *		Finalize the filter (to have it nicely sorted etc.).
 */
static void
ExecFilterFinalize(FilterState *filter)
{
	if (filter->built)
		return;

	if (filter->filter_type == FilterTypeExact)
		ExecFilterFinalizeExact(filter);
	else if (filter->filter_type == FilterTypeRange)
		ExecFilterFinalizeRange(filter);

	filter->built = true;
}

#define BLOOM_SEED_1	0x71d924af
#define BLOOM_SEED_2	0xba48b314

/*
 * ExecFilterAddHash
 *		Add value (a hash of the actual value) to a Bloom filter.
 */
static void
ExecFilterAddHash(FilterState *filter, bool keep_nulls, ExprContext *econtext, uint32 hashvalue)
{
	uint64		h1,
				h2;
	int			i;

	Assert(filter->filter_type == FilterTypeBloom);

	/* compute the hashes, used for the bloom filter */
	h1 = hash_bytes_uint32_extended(hashvalue, BLOOM_SEED_1) % filter->nbits;
	h2 = hash_bytes_uint32_extended(hashvalue, BLOOM_SEED_2) % filter->nbits;

	/* compute the requested number of hashes */
	for (i = 0; i < filter->nhashes; i++)
	{
		/* h1 + h2 + f(i) */
		uint32		h = (h1 + i * h2) % filter->nbits;
		uint32		byte = (h / 8);
		uint32		bit = (h % 8);

		/* if the bit is not set, set it and remember we did that */
		if (!(filter->data[byte] & (0x01 << bit)))
			filter->data[byte] |= (0x01 << bit);
	}
}

/*
 * ExecFilterGetHashValue2
 *		Calculate hash for values represented by Datum array.
 *
 * This is used when switching from exact filter to a bloom (once it reaches the
 * size limit).
 */
static bool
ExecFilterGetHashValue2(FilterState *filter,
					 ExprContext *econtext,
					 Datum *values,
					 bool keep_nulls,
					 uint32 *hashvalue)
{
	uint32		hashkey = 0;
	FmgrInfo   *hashfunctions;
	int			i = 0;
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	hashfunctions = filter->hashfunctions;

	for (i = 0; i < list_length(filter->hashclauses); i++)
	{
		Datum		keyval;
		bool		isNull = false; /* FIXME */

		/* combine successive hashkeys by rotating */
		hashkey = pg_rotate_left32(hashkey, 1);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = values[i];

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (filter->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
		}
		else
		{
			/* Compute the hash function */
			uint64		hkey;

			hkey = DatumGetUInt32(FunctionCall1Coll(&hashfunctions[i], filter->collations[i], keyval));
			hashkey ^= hkey;
		}
	}

	MemoryContextSwitchTo(oldContext);

	*hashvalue = hashkey;
	return true;
}

/*
 * ExecFilterAddValue
 *		Add a value to a filter (of any type).
 */
static void
ExecFilterAddValue(FilterState *filter, ExprContext *econtext)
{
	/* second pass through the node init */
	if (filter->built)
		return;

	if (filter->filter_type == FilterTypeRange)
	{
		ExecFilterAddRange(filter, filter->keepNulls, econtext);
		return;
	}

	/* filter tracking exact values */
	if (filter->filter_type == FilterTypeExact)
	{
		int		i,
				nvalues;
		char   *data;
		MemoryContext oldcxt;

		Size	entrylen = sizeof(Datum) * list_length(filter->hashclauses);

		/* if adding value worker, we're done */
		if (ExecFilterAddExact(filter, filter->keepNulls, econtext))
			return;

		nvalues = filter->nvalues;
		data = filter->data;

		oldcxt = MemoryContextSwitchTo(filter->filterCxt);

		filter->data = palloc0(filter->nbits/8 + 10);
		filter->filter_type = FilterTypeBloom;
		filter->nvalues = 0;

		MemoryContextSwitchTo(oldcxt);

		for (i = 0; i < nvalues; i++)
		{
			uint32	hashvalue = 0;
			Datum  *values = (Datum *) (data + i * entrylen);

			/*
			 * XXX We ignore nulls when adding data to the filter, so we
			 * don't need to worry about them here either.
			 */
			ExecFilterGetHashValue2(filter, econtext, values, false, &hashvalue);
			ExecFilterAddHash(filter, false, econtext, hashvalue);
		}
	}

	Assert(filter->filter_type != FilterTypeExact);

	if (filter->filter_type == FilterTypeBloom)
	{
		uint32	hash = 0;
		ExecFilterGetHashValue(filter, econtext, filter->keepNulls, &hash);
		ExecFilterAddHash(filter, filter->keepNulls, econtext, hash);
		filter->nvalues++;
	}
}

/*
 * ExecFilterAddValue
 *		Add a value to a filter (of any type).
 */
static void
ExecFilterAddValueX(FilterState *filter, ExprContext *econtext, Datum *values, bool *isnull)
{
	/* second pass through the node init */
	if (filter->built)
		return;

	if (filter->filter_type == FilterTypeRange)
	{
		ExecFilterAddRangeX(filter, econtext, filter->keepNulls, values, isnull);
		return;
	}

	/* filter tracking exact values */
	if (filter->filter_type == FilterTypeExact)
	{
		int		i,
				nvalues;
		char   *data;
		MemoryContext oldcxt;

		Size	entrylen = sizeof(Datum) * list_length(filter->hashclauses);

		/* if adding value worker, we're done */
		if (ExecFilterAddExactX(filter, filter->keepNulls, econtext, values, isnull))
			return;

		nvalues = filter->nvalues;
		data = filter->data;

		oldcxt = MemoryContextSwitchTo(filter->filterCxt);

		filter->data = palloc0(filter->nbits/8 + 10);
		filter->filter_type = FilterTypeBloom;
		filter->nvalues = 0;

		MemoryContextSwitchTo(oldcxt);

		for (i = 0; i < nvalues; i++)
		{
			uint32	hashvalue = 0;
			Datum  *values = (Datum *) (data + i * entrylen);

			/*
			 * XXX We ignore nulls when adding data to the filter, so we
			 * don't need to worry about them here either.
			 */
			ExecFilterGetHashValue2(filter, econtext, values, false, &hashvalue);
			ExecFilterAddHash(filter, false, econtext, hashvalue);
		}
	}

	Assert(filter->filter_type != FilterTypeExact);

	if (filter->filter_type == FilterTypeBloom)
	{
		uint32	hash = 0;
		ExecFilterGetHashValueX(filter, econtext, values, isnull, filter->keepNulls, &hash);
		ExecFilterAddHash(filter, filter->keepNulls, econtext, hash);
		filter->nvalues++;
	}
}

/*
 * ExecScanGetFilterHashValue
 *		Calculate a hash value for the tuple in the scan slot.
 *
 * We'll then check the presence of this hash value in the bloom filter.
 *
 * XXX Almost the same as ExecFilterGetHashValue, except that it's
 * executed for the filter reference. Could we refactor it somehow to
 * reduce the code duplication?
 *
 * XXX In any case, we should rename this to not include "scan" because
 * we could inject this to other node types (e.g. subquery).
 */
static bool
ExecScanGetFilterHashValue(FilterState *filter,
						   ExprContext *econtext,
						   bool keep_nulls,
						   uint64 *hashvalue)
{
	uint64		hashkey = 0;
	FmgrInfo   *hashfunctions;
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	hashfunctions = filter->hashfunctions;

	/* XXX use expressions from the reference, with adjusted varnos etc. */
	foreach(hk, filter->clauses)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/* combine successive hashkeys by rotating */
		hashkey = pg_rotate_left32(hashkey, 1);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull);

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (filter->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
			/* FIXME should we ignore NULL values altogether? what about the
			 * keep_nulls flag? */
		}
		else
		{
			/* Compute the hash function */
			hashkey ^= DatumGetUInt32(FunctionCall1Coll(&hashfunctions[i], filter->collations[i], keyval));
		}

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	*hashvalue = hashkey;
	return true;
}

/*
 * ExecScanGetFilterGetValues
 *		Extract values from the scan tuple.
 *
 * FIXME Probably does not handle NULLs correctly, needs a separate isnull
 * array, or something like that?
 *
 * FIXME Almost the same as ExecFilterGetValues, except that it's
 * executed for the filter reference. Could we refactor it somehow to
 * reduce the code duplication?
 */
static bool
ExecScanGetFilterGetValues(FilterState *filter,
						   ExprContext *econtext,
						   bool keep_nulls,
						   Datum *values)
{
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/* XXX use expressions from the reference, with adjusted varnos etc. */
	foreach(hk, filter->clauses)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull);

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (filter->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
			/* FIXME should we ignore NULL values altogether? what about the
			 * keep_nulls flag? */
		}
		else
			/* FIXME probably needs to copy the value using datumCopy? */
			values[i] = keyval;

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	return true;
}

/*
 * ExecFilterContainsHash
 *		Check if the tuple matches the Bloom filter.
 */
static bool
ExecFilterContainsHash(FilterState *filter, ExprContext *econtext)
{
	int			i;
	uint64		h1,
				h2;
	uint64		hashvalue = 0;

	ExecScanGetFilterHashValue(filter, econtext, false, &hashvalue);

	Assert(filter->filter_type == FilterTypeBloom);

	/* compute the hashes, used for the bloom filter */
	h1 = hash_bytes_uint32_extended(hashvalue, BLOOM_SEED_1) % filter->nbits;
	h2 = hash_bytes_uint32_extended(hashvalue, BLOOM_SEED_2) % filter->nbits;

	/* compute the requested number of hashes */
	for (i = 0; i < filter->nhashes; i++)
	{
		/* h1 + h2 + f(i) */
		uint32		h = (h1 + i * h2) % filter->nbits;
		uint32		byte = (h / 8);
		uint32		bit = (h % 8);

		/* if the bit is not set, the value is not there */
		if (!(filter->data[byte] & (0x01 << bit)))
			return false;
	}

	filter->nhits++;

	/* all hashes found in bloom filter */
	return true;
}

/*
 * ExecFilterContainsExact
 *		Check if the filter (in 'exact' mode) contains exact value.
 *
 * FIXME This assumes all the types allow sorting, but that may not be true.
 * In that case this should just do linear search.
 */
static bool
ExecFilterContainsExact(FilterState *filter, ExprContext *econtext)
{
	Datum	   *values;
	Size		entrysize = sizeof(Datum) * list_length(filter->hashclauses);
	char	   *ptr;
	qsort_cxt  *cxt = (qsort_cxt *) filter->private_data;

	Assert(filter->filter_type == FilterTypeExact);

	values = palloc(entrysize);

	ExecScanGetFilterGetValues(filter, econtext, false, values);

	ptr = bsearch_arg(values, filter->data, filter->nvalues, entrysize,
					  filter_value_comparator, cxt);

	if (ptr != NULL)
		filter->nhits++;

	pfree(values);

	/* all hashes found in bloom filter */
	return (ptr != NULL);
}

/*
 * ExecFilterContainsExact
 *		Check if the filter (in 'range' mode) contains exact value.
 *
 * FIXME This assumes all the types allow sorting, but that may not be true.
 * In that case this should just do linear search.
 */
static bool
ExecFilterContainsRange(FilterState *filter, ExprContext *econtext)
{
	Datum	   *values;
	Datum	   *entry;
	Size		entrylen = list_length(filter->hashclauses);
	qsort_cxt  *cxt = (qsort_cxt *) filter->private_data;

	Assert(filter->filter_type == FilterTypeRange);

	values = (Datum *) filter->data;

	entry = palloc(entrylen * sizeof(Datum));

	/* reject NULL values */
	if (!ExecScanGetFilterGetValues(filter, econtext, false, entry))
	{
		pfree(entry);
		return false;
	}

	/* TODO use binary search to check ranges */
	for (int i = 0; i < filter->nranges; i++)
	{
		Datum  *start,
			   *end;

		start = &values[2 * i * entrylen];
		end = &values[(2 * i + 1) * entrylen];

		if ((filter_value_comparator(entry, start, cxt) >= 0) &&
			(filter_value_comparator(entry, end, cxt) <= 0))
		{
			filter->nhits++;
			pfree(entry);
			return true;
		}
	}

	for (int i = 2 * filter->nranges; i < filter->nvalues; i++)
	{
		Datum  *value = &values[i * entrylen];

		if (filter_value_comparator(entry, value, cxt) == 0)
		{
			filter->nhits++;
			pfree(entry);
			return true;
		}
	}

	pfree(entry);

	/* all hashes found in bloom filter */
	return false;
}

/*
 * ExecFilterContainsExact
 *		Check the filter - either in exact or hashed mode, as needed.
 */
bool
ExecFilterContainsValue(FilterState *filter, ExprContext *econtext)
{
	filter->nqueries++;

	if (filter->filter_type == FilterTypeExact)
		return ExecFilterContainsExact(filter, econtext);
	else if (filter->filter_type == FilterTypeRange)
		return ExecFilterContainsRange(filter, econtext);
	else
		return ExecFilterContainsHash(filter, econtext);
}

static FilterState *
ExecFilterInit(PlanState *planstate, Filter *filter,
				   EState *estate, int eflags)
{
	int			nkeys;
	int			i;
	ListCell   *ho,
			   *hc,
			   *hk;
	qsort_cxt  *cxt;
	// Plan	   *subplan = filter->subplan;

	FilterState *state = makeNode(FilterState);

	// state->planstate = ExecInitNode(filter->subplan, estate, eflags);

	/*
	 * Start the filter in exact mode, we'll switch to Bloom if we fill it.
	 *
	 * FIXME this is a bit misleading, because for byref values we only store
	 * the pointers to the filter. So there may be much more memory needed.
	 * This should copy the values into the filter.
	 */
	state->filter = filter;
	state->filterId = filter->filterId;

	state->clauses = ExecInitExprList(filter->clauses, planstate);

	/* the hashclauses are evaluated on the subplan, so initialize them accordingly */
	state->hashclauses = ExecInitExprList(filter->hashclauses, state->planstate);

	nkeys = list_length(filter->hashoperators);
	state->hashfunctions = palloc_array(FmgrInfo, nkeys);
	state->hashStrict = palloc_array(bool, nkeys);
	state->collations = palloc_array(Oid, nkeys);
	state->types = palloc(sizeof(Oid) * nkeys);

	/* FIXME */
	state->keepNulls = false;

	/* FIXME properly handle the left/right function, for details see
	 * ExecHashTableCreate() */
	i = 0;
	forboth(ho, filter->hashoperators, hc, filter->hashcollations)
	{
		Oid			hashop = lfirst_oid(ho);
		Oid			left_hashfn;
		Oid			right_hashfn;

		if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn))
			elog(ERROR, "could not find hash function for hash operator %u",
				hashop);
		fmgr_info(left_hashfn, &state->hashfunctions[i]);
		// fmgr_info(right_hashfn, &hashtable->inner_hashfunctions[i]);
		state->hashStrict[i] = op_strict(hashop);
		state->collations[i] = lfirst_oid(hc);
		i++;
	}

	i = 0;
	foreach(hk, filter->hashclauses)
	{
		state->types[i] = exprType(lfirst(hk));
		i++;
	}

	Assert(filter_pushdown_mode != FILTER_PUSHDOWN_OFF);

	if (filter_pushdown_mode == FILTER_PUSHDOWN_EXACT)
		state->filter_type = FilterTypeExact;
	else if (filter_pushdown_mode == FILTER_PUSHDOWN_RANGE)
		state->filter_type = FilterTypeRange;
	else if (filter_pushdown_mode == FILTER_PUSHDOWN_BLOOM)
		state->filter_type = FilterTypeBloom;

	/*
	 * Bloom filter is sized based on estimates and desired false positive
	 * rate, as usual.
	 *
	 * XXX For now we use a hard-coded 1%, but maybe we should be a bit
	 * smarter and do some balancing? It might be better to accept a bit
	 * higher FPR if the resulting filter fits into L2/L3 caches, or
	 * something like that?
	 *
	 * The exact/range filters simply start with 8kB and we double the size
	 * until we hit the work_mem. For exact filters that means failure, but
	 * maybe we should transform them into range/Bloom automatically?
	 */
	if (state->filter_type == FilterTypeBloom)
	{
		double		m,		/* size of filter (number of bits) */
					p,		/* false positive rate */
					n,		/* number of distinct elements */
					k;		/* number of hash functions */

		p = 0.01;	/* 1% false positive */

		/* 1000 seems like a reasonable lower bound */
		// n = Max(1000, subplan->plan_rows);	/* assume unique values */
		n = 1000;

		m = ceil((n * log(p)) / log(1 / pow(2, log(2))));
		k = round((m / n) * log(2));

		/* round size to multiples of 8 bit (whole bytes) */
		state->nbits = ((int) ((m + 7) / 8)) * 8;

		state->nhashes = k;
		state->data = palloc0(state->nbits / 8);
	}
	else
	{
		state->nallocated = 1024;	/* start with 8kB */
		state->data = palloc0(state->nallocated * sizeof(Datum));
	}

	cxt = palloc0(offsetof(qsort_cxt, ssup) +
				  sizeof(SortSupportData) * list_length(filter->clauses));

	for (int j = 0; j < list_length(filter->clauses); j++)
	{
		SortSupport		ssup = &cxt->ssup[j];
		TypeCacheEntry *entry
			= lookup_type_cache(state->types[j], TYPECACHE_LT_OPR);

		ssup->ssup_cxt = CurrentMemoryContext;
		ssup->ssup_collation = state->collations[j];
		ssup->ssup_nulls_first = false;	/* FIXME? */

		PrepareSortSupportFromOrderingOp(entry->lt_opr, ssup);

		cxt->nelements++;
	}

	state->private_data = cxt;

	state->filterCxt = AllocSetContextCreate(CurrentMemoryContext,
											 "hash filter context",
											 ALLOCSET_DEFAULT_SIZES);

	{
		ListCell   *lc2;
		List	   *context;
		RangeTblEntry *rte = exec_rt_fetch(filter->relid, estate);
		Oid			relid = rte->relid;
		bool		first;
		StringInfoData query;

		context = deparse_context_for("tmp", rte->relid);

		initStringInfo(&query);

		appendStringInfoString(&query, "SELECT ");

		/* deparse the filter value expression */
		first = true;
		foreach (lc2, filter->hashclauses)
		{
			char *str;
			Node *clause = (Node *) lfirst(lc2);

			str = deparse_expression(clause, context, false, false);

			if (!first)
				appendStringInfoString(&query, ", ");

			appendStringInfoString(&query, str);

			first = false;
		}

		appendStringInfoString(&query, " FROM ");

		appendStringInfoString(&query,
							   quote_qualified_identifier(get_namespace_name(get_rel_namespace(relid)),
														  get_rel_name(relid)));


		first = true;
		foreach (lc2, filter->restrictions)
		{
			char *str;
			Node *clause = (Node *) lfirst(lc2);

			str = deparse_expression(clause, context, false, false);

			if (first)
				appendStringInfoString(&query, " WHERE ");
			else
				appendStringInfoString(&query, " AND ");

			appendStringInfoString(&query, str);

			first = false;
		}

		state->query = pstrdup(query.data);
	}

	return state;
}

List *
ExecInitFilters(PlanState *planstate, List *filters, EState *estate, int eflags)
{
	ListCell   *lc;
	List	   *states = NIL;

	foreach (lc, filters)
	{
		Filter *filter = (Filter *) lfirst(lc);
		FilterState *state;

		state = ExecFilterInit(planstate, filter, estate, eflags);

		states = lappend(states, state);
	}

	return states;
}

void
ExecEndFilters(List *filters)
{
	ListCell   *lc;

	foreach (lc, filters)
	{
		FilterState *state = (FilterState *) lfirst(lc);

		ExecEndNode(state->planstate);
	}
}

static void
ExecBuildFilter(FilterState *filter, EState *estate, int types)
{
	ExprContext *econtext;
	Filter *f = filter->filter;
	SPIPlanPtr	spiPlan;
	Portal		spiPortal;

	Datum  *values;
	bool   *isnull;

	/* if filter is already built, we're done */
	if (filter->built)
		return;

	filter->buildCtx = AllocSetContextCreate(CurrentMemoryContext,
											 "filter build context",
											 ALLOCSET_DEFAULT_SIZES);

	values = (Datum *) palloc(sizeof(Datum) * list_length(f->clauses));
	isnull = (bool *) palloc(sizeof(bool) * list_length(f->clauses));

	/* create expression context */
	econtext = CreateExprContext(estate);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	spiPlan = SPI_prepare(filter->query, 0, NULL);
	if (spiPlan == NULL)
		elog(ERROR, "SPI_prepare failed");

	spiPortal = SPI_cursor_open(NULL, spiPlan, NULL, NULL, true);

	while (true)
	{
		SPI_cursor_fetch(spiPortal, true, 100);

		if (SPI_processed == 0)
		{
			SPI_cursor_close(spiPortal);
			break;
		}

		for (uint64 i = 0; i < SPI_processed; i++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[i];
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;

			for (int j = 0; j < list_length(f->clauses); j++)
				values[j] = heap_getattr(tuple, j+1, tupdesc, &isnull[j]);

			ExecFilterAddValueX(filter, econtext, values, isnull);

			SPI_freetuple(tuple);
		}

		SPI_freetuptable(SPI_tuptable);
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	ExecFilterFinalize(filter);

	MemoryContextDelete(filter->buildCtx);
}

void
ExecBuildFilters(ScanState *node, EState *estate, int types)
{
	ListCell *lc;

	// elog(WARNING, "building node->ss_Filters = %p (%d)", node->ss_Filters, list_length(node->ss_Filters));

	foreach (lc, node->ss_Filters)
	{
		FilterState *filter = (FilterState *) lfirst(lc);

		ExecBuildFilter(filter, estate, types);
	}
}

/*
 * ExecFilters
 *		Chech if the tuple matches the pushed-down filters.
 */
bool
ExecFilters(ScanState *node, ExprContext *econtext)
{
	ListCell *lc;
	List *filters;

	filters = node->ss_Filters;

	foreach (lc, filters)
	{
		FilterState *filterstate = (FilterState *) lfirst(lc);

		if (!filterstate)
			continue;

		if (!filterstate->built)
			continue;

		if (filterstate->skip)
			continue;

		if (!ExecFilterContainsValue(filterstate, econtext))
			return false;
	}

	return true;
}

static bool
filter_derive_minmax_range(FilterState *filter, Datum *minval, Datum *maxval)
{
	Datum  *values = (Datum *) filter->data;

	Assert(filter->filter_type != FilterTypeBloom);

	/* FIXME handle the case with nvalues = 0 */
	if (filter->nvalues == 0)
		return false;

	if (filter->filter_type == FilterTypeExact)
	{
		*minval = values[0];
		*maxval = values[filter->nvalues - 1];
	}
	else
	{
		if (filter->nranges > 0)
		{
			*minval = values[0];
			*maxval = values[2 * filter->nranges - 1];

			if (filter->nvalues > 2 * filter->nranges)
			{
				SortSupportData		ssup;
				Datum	tmpmin,
						tmpmax;

				TypeCacheEntry *typentry = lookup_type_cache(filter->types[0],
															 TYPECACHE_LT_OPR);

				memset(&ssup, 0, sizeof(SortSupportData));

				ssup.ssup_cxt = CurrentMemoryContext;
				ssup.ssup_collation = filter->collations[0];
				ssup.ssup_nulls_first = false;

				PrepareSortSupportFromOrderingOp(typentry->lt_opr, &ssup);

				tmpmin = values[2 * filter->nranges];
				tmpmax = values[filter->nvalues - 1];

				if (ApplySortComparator(*minval, false, tmpmin, false, &ssup) > 0)
					*minval = tmpmin;


				if (ApplySortComparator(*maxval, false, tmpmax, false, &ssup) < 0)
					*maxval = tmpmax;
			}
		}
		else
		{
			*minval = values[0];
			*maxval = values[filter->nvalues - 1];
		}
	}

	return true;
}

int
ExecFiltersCountScanKeys(FilterState *filter)
{
	/* column IN (...) */
	if (filter->filter_type == FilterTypeExact)
		return (filter->filter->searcharray) ? 1 : 2;

	/* column >= $1 AND column <= $2 */
	if (filter->filter_type == FilterTypeRange)
		return 2;

	return 0;
}

/*
 * XXX This may be wrong if there already are some scan keys - the scan keys
 * need to be ordered by attnum.
 */
static void
ExecFiltersAddScanKeys(FilterState *filter, ScanKeyData *keys)
{
	int		idx = 0;
	Datum  *values = (Datum *) filter->data;

	TypeCacheEntry *typentry;

	/* no scan keys for Bloom filters */
	if (filter->filter_type == FilterTypeBloom)
		return;

	typentry = lookup_type_cache(filter->types[0],
								 TYPECACHE_BTREE_OPFAMILY);

	/*
	 * XXX Not sure what to do about range filters with multi-range conditions.
	 * At the moment we just derive a single [min,max] range, but it'd be nice
	 * to allow more complex conditions without having to build BitmapOr, which
	 * does not quite work because (a) we don't know how many ranges there'll be
	 * during planning, and (b) we don't want to scan the index multiple times
	 * only to build the bitmap. For btree that may not be an issue, assuming
	 * the conditions allow quickly determining which part of the index to scan,
	 * but for BRIN that's an issue as it requires scanning the whole index
	 * repeatedly. That's kinda the point of the patch adding SK_SEARCHARRAY
	 * handling for BRIN (cheaper to deserialize once and match all scan keys).
	 *
	 * FIXME if there's nothing in the filter (nvalues == 0), build a scan key
	 * evaluating to false, or something like that. Or maybe we should just
	 * skip the whole plan execution and not return anything?
	 */
	if ((filter->filter_type == FilterTypeExact) && (filter->filter->searcharray))
	{
		int16	typlen;
		bool	typbyval;
		char	typalign;
		ArrayType *ret;

		Oid		eq_opr = get_opfamily_member(typentry->btree_opf,
									 typentry->btree_opintype,
									 typentry->btree_opintype,
									 BTEqualStrategyNumber);

		get_typlenbyvalalign(filter->types[0], &typlen, &typbyval, &typalign);


		ret = construct_array((Datum *) filter->data, filter->nvalues, filter->types[0],
							  typlen, typbyval, typalign);

		ScanKeyEntryInitialize(&keys[idx++],
							   SK_SEARCHARRAY,	// flags
							   filter->filter->attnum,	// FIXME attnum
							   BTEqualStrategyNumber,
							   filter->types[0],	// subtype
							   filter->collations[0],	// collation
							   get_opcode(eq_opr),	// int4ge
							   PointerGetDatum(ret));
	}
	else if (filter->filter_type == FilterTypeExact)
	{
		Datum	minval,
				maxval;

		Oid		ge_opr = get_opfamily_member(typentry->btree_opf,
									 typentry->btree_opintype,
									 typentry->btree_opintype,
									 BTGreaterEqualStrategyNumber);

		Oid		le_opr = get_opfamily_member(typentry->btree_opf,
									 typentry->btree_opintype,
									 typentry->btree_opintype,
									 BTLessEqualStrategyNumber);

		/*
		 * FIXME If index supports SK_SEARCHARRAY, build a single scankey
		 * with the exact values as an array.
		 */

		filter_derive_minmax_range(filter, &minval, &maxval);

		ScanKeyEntryInitialize(&keys[idx++],
							   0,	// flags
							   filter->filter->attnum,	// FIXME attnum
							   BTGreaterEqualStrategyNumber,
							   filter->types[0],		// subtype
							   filter->collations[0],	// collation
							   get_opcode(ge_opr),		// int4ge
							   values[0]);

		ScanKeyEntryInitialize(&keys[idx++],
							   0,	// flags
							   filter->filter->attnum,	// FIXME attnum
							   BTLessEqualStrategyNumber,
							   filter->types[0],		// subtype
							   filter->collations[0],	// collation
							   get_opcode(le_opr),		// int4le
							   values[filter->nvalues - 1]);
	}
	else if (filter->filter_type == FilterTypeRange)
	{
		Datum	minval,
				maxval;

		Oid		ge_opr = get_opfamily_member(typentry->btree_opf,
									 typentry->btree_opintype,
									 typentry->btree_opintype,
									 BTGreaterEqualStrategyNumber);

		Oid		le_opr = get_opfamily_member(typentry->btree_opf,
									 typentry->btree_opintype,
									 typentry->btree_opintype,
									 BTLessEqualStrategyNumber);

		filter_derive_minmax_range(filter, &minval, &maxval);

		ScanKeyEntryInitialize(&keys[idx++],
							   0,	// flags
							   filter->filter->attnum,	// FIXME index attnum
							   BTGreaterEqualStrategyNumber,
							   filter->types[0],		// subtype
							   filter->collations[0],	// collation
							   get_opcode(ge_opr),		// int4ge
							   minval);

		ScanKeyEntryInitialize(&keys[idx++],
							   0,	// flags
							   filter->filter->attnum,	// FIXME index attnum
							   BTLessEqualStrategyNumber,
							   filter->types[0],		// subtype
							   filter->collations[0],	// collation
							   get_opcode(le_opr),		// int4le
							   maxval);
	}

	/* skip the filter (maybe we should override it in some cases) */
	filter->skip = true;
}

static int
scankey_comparator(const void *a, const void *b)
{
	ScanKey sa = (ScanKey) a;
	ScanKey sb = (ScanKey) b;

	if (sa->sk_attno < sb->sk_attno)
		return -1;
	else if (sa->sk_attno > sb->sk_attno)
		return 1;

	return 0;
}

/*
 * FIXME possibly needs to care about the existing scankeys, to keep them
 * ordered by sk_attno (otherwise the scan may fail).
 */
void
ExecFiltersDeriveScanKeys(ScanState *state, int *nkeys, ScanKey *keys)
{
	/* number of scan keys derived from filters */
	int				numkeys = 0;
	ScanKeyData	   *scanKeys = *keys;
	ListCell	   *lc;

	/* count scan keys we can derive from the filter(s) */
	foreach (lc, state->ss_Filters)
	{
		FilterState *filter = (FilterState *) lfirst(lc);
		numkeys += ExecFiltersCountScanKeys(filter);
	}

	if (numkeys == 0)
		return;

	/*
	 * If we can derive any scan keys from filters, make sure we have enough
	 * space for them, and then derive the actual filters.
	 */
	scanKeys = repalloc(scanKeys,
						sizeof(ScanKeyData) * (*nkeys + numkeys));
	*keys = scanKeys;

	/* derive the actual scan keys from each filter */
	foreach (lc, state->ss_Filters)
	{
		FilterState	   *filter = (FilterState *) lfirst(lc);

		ExecFiltersAddScanKeys(filter,
							   &scanKeys[*nkeys]);

		*nkeys += ExecFiltersCountScanKeys(filter);
	}

	pg_qsort(*keys, *nkeys, sizeof(ScanKeyData), scankey_comparator);
}
