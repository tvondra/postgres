/*-------------------------------------------------------------------------
 *
 * execFilters.c
 *	  This code provides support for pushed-down filters.
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execFilters.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planmain.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


/*
 * ExecHashGetHashValue
 *		Compute the hash value for a tuple
 *
 * The tuple to be tested must be in econtext->ecxt_outertuple (thus Vars in
 * the hashkeys expressions need to have OUTER_VAR as varno). If outer_tuple
 * is false (meaning it's the HashJoin's inner node, Hash), econtext,
 * hashkeys, and slot need to be from Hash, with hashkeys/slot referencing and
 * being suitable for tuples from the node below the Hash. Conversely, if
 * outer_tuple is true, econtext is from HashJoin, and hashkeys/slot need to
 * be appropriate for tuples from HashJoin's outer node.
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
ExecHashGetFilterHashValue(HashFilterState *filter,
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

/*
 * ExecHashGetHashValue
 *		Extract values from the tuple.
 *
 * Pretty much exactly the same as ExecHashGetFilterHashValue, but it returns
 * the values instead of hashing them.
 */
static bool
ExecHashGetFilterGetValues(HashFilterState *filter,
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
				return false;
			}
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

/* A simple pair of values, representing an interval. */
typedef struct FilterRange
{
	Datum	start;
	Datum	end;
} FilterRange;


/*
 * FilterRange comparator. It compares by start, then by end.
 *
 * FIXME This needs to use a comparator for the particular data type, instead of
 * just comparing the Datum values.
 */
static int
filter_range_cmp(const void *a, const void *b)
{
	FilterRange *ra = (FilterRange *) a;
	FilterRange *rb = (FilterRange *) b;

	if (ra->start < rb->start)
		return -1;
	else if (ra->start > rb->start)
		return 1;

	if (ra->end < rb->end)
		return -1;
	else if (ra->end > rb->end)
		return 1;

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
ranges_overlap(FilterRange *ra, FilterRange *rb)
{
	if (ra->end < rb->start)
		return false;

	if (ra->start > rb->end)
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
ranges_contiguous(FilterRange *ra, FilterRange *rb)
{
	Assert(ra->end <= rb->start);
	if (ra->end + 1 >= rb->start)
		return true;

	return false;
}

/*
 * ExecHashFilterAddRange
 *		Add values to a range filter.
 *
 * If there's not enough space for the new value, combine the values into
 * fewer ranges. We combine ranges that overlap or are contiguous, and then
 * we combine closest ranges.
 */
static bool
ExecHashFilterAddRange(HashFilterState *filter, bool keep_nulls, ExprContext *econtext)
{
	Datum  *values;
	Size	entrylen = sizeof(Datum) * list_length(filter->clauses);
	int		offset;
	int		maxvalues = (filter->nbits/8 / entrylen);

	Assert(filter->filter_type == HashFilterRange);

	/* too much data for exact filter, compact the ranges */
	if ((filter->nvalues + 1) * entrylen > filter->nbits/8)
	{
		int		i;
		Datum  *filter_data = (Datum *) filter->data;
		int		filter_idx = 0;
		int		idx = 0;
		int		nranges = (filter->nranges + (filter->nvalues - 2 * filter->nranges));

		FilterRange *ranges = palloc(sizeof(FilterRange) * nranges);

		for (i = 0; i < filter->nranges; i++)
		{
			Assert(idx < nranges);

			ranges[idx].start = filter_data[filter_idx++];
			ranges[idx].end = filter_data[filter_idx++];
			idx++;
			Assert(filter_idx < filter->nvalues);
		}

		for (i = filter_idx; i < filter->nvalues; i++)
		{
			Assert(idx < nranges);

			ranges[idx].start = filter_data[i];
			ranges[idx].end = filter_data[i];
			Assert(i < filter->nvalues);
			idx++;
		}

		Assert(idx == nranges);

		// sort ranges by start
		pg_qsort(ranges, nranges, sizeof(FilterRange), filter_range_cmp);

		// combine overlapping ranges
		idx = 0;
		for (i = 1; i < nranges; i++)
		{
			if (ranges_overlap(&ranges[idx], &ranges[i]))
			{
				ranges[idx].end = Max(ranges[idx].end, ranges[i].end);
				continue;
			}

			idx++;

			ranges[idx] = ranges[i];

			Assert(idx < nranges);
		}

		nranges = (idx + 1);

		// combine contiguous ranges (e.g. for integers, ranges [1.10] and [11,20]
		// can be combined into [1,20]
		idx = 0;
		for (i = 1; i < nranges; i++)
		{
			if (ranges_contiguous(&ranges[idx], &ranges[i]))
			{
				ranges[idx].end = ranges[i].end;
				continue;
			}

			idx++;

			ranges[idx] = ranges[i];

			Assert(idx < nranges);
		}

		nranges = (idx + 1);

		// now combine the closest ranges
		while (true)
		{
			int		nvalues = 0;
			int		mindist;
			int		minidx;

			for (i = 0; i < nranges; i++)
			{
				if (ranges[i].start == ranges[i].end)
					nvalues++;
				else
					nvalues += 2;
			}

			if (nvalues <= maxvalues / 2)
				break;

			minidx = 1;
			mindist = ranges[1].end - ranges[0].start;

			for (i = 2; i < nranges; i++)
			{
				if (ranges[i].end - ranges[i-1].start < mindist)
				{
					mindist = ranges[i].end - ranges[i-1].start;
					minidx = i;
				}
			}

			ranges[minidx-1].end = ranges[minidx].end;
			memmove(&ranges[minidx], &ranges[minidx+1], sizeof(FilterRange) * (nranges - (minidx + 1)));
			nranges--;

		}

		filter->nranges = 0;
		filter->nvalues = 0;

		for (i = 0; i < nranges; i++)
		{
			if (ranges[i].start != ranges[i].end)
			{
				filter_data[filter->nvalues++] = ranges[i].start;
				filter_data[filter->nvalues++] = ranges[i].end;
				filter->nranges++;
				Assert(filter->nvalues <= maxvalues);
			}
		}

		for (i = 0; i < nranges; i++)
		{
			if (ranges[i].start == ranges[i].end)
			{
				filter_data[filter->nvalues++] = ranges[i].start;
				Assert(filter->nvalues <= maxvalues);
			}
		}

	}

	values = palloc(sizeof(Datum) * list_length(filter->clauses));

	ExecHashGetFilterGetValues(filter, econtext, keep_nulls, values);

	offset = entrylen * filter->nvalues;
	memcpy(&filter->data[offset], values, entrylen);

	pfree(values);

	filter->nvalues++;

	return true;
}

/*
 * ExecHashFilterFinalizeRange
 *		Combine filter represented as ranges.
 *
 * This is pretty much a subset of what ExecHashFilterAddRange does - it sorts
 * the ranges and values, combines overlapping/contiguous ranges, etc. The one
 * thing it does not do is combining close ranges, because we don't need to fit
 * any more values into the filter. We just want to make it easier to use.
 *
 * FIXME refactor to reuse as much of the code with ExecHashFilterAddRange.
 */
static void
ExecHashFilterFinalizeRange(HashFilterState *filter)
{
	int		i;
	Datum  *filter_data = (Datum *) filter->data;
	int		filter_idx = 0;
	int		idx = 0;
	int		nranges;

	FilterRange *ranges;

	Assert(filter->filter_type == HashFilterRange);

	/* nothing to do if the filter represents no values */
	if (filter->nvalues == 0)
		return;

	nranges = (filter->nranges + (filter->nvalues - 2 * filter->nranges));
	ranges = palloc(sizeof(FilterRange) * nranges);

	for (i = 0; i < filter->nranges; i++)
	{
		Assert(idx < nranges);

		ranges[idx].start = filter_data[filter_idx++];
		ranges[idx].end = filter_data[filter_idx++];
		idx++;
		Assert(filter_idx <= filter->nvalues);
	}

	for (i = filter_idx; i < filter->nvalues; i++)
	{
		Assert(idx < nranges);

		ranges[idx].start = filter_data[i];
		ranges[idx].end = filter_data[i];
		Assert(i < filter->nvalues);
		idx++;
	}

	Assert(idx == nranges);

	// sort ranges by start
	pg_qsort(ranges, nranges, sizeof(FilterRange), filter_range_cmp);

	// combine overlapping ranges
	idx = 0;
	for (i = 1; i < nranges; i++)
	{
		if (ranges_overlap(&ranges[idx], &ranges[i]))
		{
			ranges[idx].end = Max(ranges[idx].end, ranges[i].end);
			continue;
		}

		idx++;

		ranges[idx] = ranges[i];

		Assert(idx < nranges);
	}

	nranges = (idx + 1);

	// combine contiguous ranges (e.g. for integers, ranges [1.10] and [11,20]
	// can be combined into [1,20]
	idx = 0;
	for (i = 1; i < nranges; i++)
	{
		if (ranges_contiguous(&ranges[idx], &ranges[i]))
		{
			ranges[idx].end = ranges[i].end;
			continue;
		}
	
		idx++;
	
		ranges[idx] = ranges[i];
	
		Assert(idx < nranges);
	}

	nranges = (idx + 1);

	filter->nranges = 0;
	filter->nvalues = 0;

	for (i = 0; i < nranges; i++)
	{
		if (ranges[i].start != ranges[i].end)
		{
			filter_data[filter->nvalues++] = ranges[i].start;
			filter_data[filter->nvalues++] = ranges[i].end;
			filter->nranges++;
		}
	}

	for (i = 0; i < nranges; i++)
	{
		if (ranges[i].start == ranges[i].end)
		{
			filter_data[filter->nvalues++] = ranges[i].start;
		}
	}
}

/*
 * Simple comparator of Datum arrays.
 *
 * FIXME This only works for byval types, needs to check byref types too. That
 * requires looking up comparators for types etc.
 */
static int
filter_comparator(const void *a, const void *b, void *c)
{
	Size	len = * (Size *) c;

	return memcmp(a, b, len);
}

static void
ExecHashFilterFinalizeExact(HashFilterState *filter)
{
	Size	entrylen = sizeof(Datum) * list_length(filter->clauses);

	Assert(filter->filter_type == HashFilterExact);

	/* nothing to do if the filter represents no values */
	if (filter->nvalues == 0)
		return;

	/* nothing to do if the filter represents no values */
	qsort_arg(filter->data, filter->nvalues, entrylen, filter_comparator, &entrylen);
}

/* FIXME deduplicate the values first */
static bool
ExecHashFilterAddExact(HashFilterState *filter, bool keep_nulls, ExprContext *econtext)
{
	Datum  *values;
	Size	entrylen = sizeof(Datum) * list_length(filter->clauses);
	int		offset = entrylen * filter->nvalues;

	Assert(filter->filter_type == HashFilterExact);

	/* too much data for exact filter, switch to bloom */
	if ((filter->nvalues + 1) * entrylen > filter->nbits/8)
		return false;

	values = palloc(sizeof(Datum) * list_length(filter->clauses));

	ExecHashGetFilterGetValues(filter, econtext, keep_nulls, values);

	memcpy(&filter->data[offset], values, entrylen);

	pfree(values);

	filter->nvalues++;

	return true;
}


/*
 * ExecHashFilterFinalize
 *		Finalize the filter (to have it nicely sorted etc.).
 */
void
ExecHashFilterFinalize(HashState *node, HashFilterState *filter)
{
	if (filter->built)
		return;

	if (filter->filter_type == HashFilterExact)
		ExecHashFilterFinalizeExact(filter);
	else if (filter->filter_type == HashFilterRange)
		ExecHashFilterFinalizeRange(filter);

	filter->built = true;

	node->ps.state->es_filters
		= lappend(node->ps.state->es_filters, filter);
}

#define BLOOM_SEED_1	0x71d924af
#define BLOOM_SEED_2	0xba48b314

/*
 * ExecHashFilterAddHash
 *		Add value (a hash of the actual value) to a Bloom filter.
 */
static void
ExecHashFilterAddHash(HashFilterState *filter, bool keep_nulls, ExprContext *econtext, uint32 hashvalue)
{
	uint64		h1,
				h2;
	int			i;

	Assert(filter->filter_type == HashFilterBloom);

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
 * ExecHashGetFilterHashValue2
 *		Calculate hash for values represented by Datum array.
 *
 * This is used when switching from exact filter to a bloom (once it reaches the
 * size limit).
 */
static bool
ExecHashGetFilterHashValue2(HashFilterState *filter,
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

	for (i = 0; i < list_length(filter->clauses); i++)
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
 * ExecHashFilterAddValue
 *		Add a value to a filter (of any type).
 */
void
ExecHashFilterAddValue(HashJoinTable hashtable, HashFilterState *filter, ExprContext *econtext)
{
	/* second pass through the node init */
	if (filter->built)
		return;

	if (filter->filter_type == HashFilterRange)
	{
		ExecHashFilterAddRange(filter, hashtable->keepNulls, econtext);
		return;
	}

	/* filter tracking exact values */
	if (filter->filter_type == HashFilterExact)
	{
		int		i,
				nvalues;
		char   *data;
		MemoryContext oldcxt;

		Size	entrylen = sizeof(Datum) * list_length(filter->clauses);

		/* if adding value worker, we're done */
		if (ExecHashFilterAddExact(filter, hashtable->keepNulls, econtext))
			return;

		nvalues = filter->nvalues;
		data = filter->data;

		oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

		filter->data = palloc0(filter->nbits/8 + 10);
		filter->filter_type = HashFilterBloom;
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
			ExecHashGetFilterHashValue2(filter, econtext, values, false, &hashvalue);
			ExecHashFilterAddHash(filter, false, econtext, hashvalue);
		}
	}

	Assert(filter->filter_type != HashFilterExact);

	if (filter->filter_type == HashFilterBloom)
	{
		uint32	hash = 0;
		ExecHashGetFilterHashValue(filter, econtext, hashtable->keepNulls, &hash);
		ExecHashFilterAddHash(filter, hashtable->keepNulls, econtext, hash);
		filter->nvalues++;
	}
}

/*
 * ExecHashResetFilters
 *		Reset the filter state before a rescan.
 */
void
ExecHashResetFilters(HashState *node)
{
	ListCell *lc;

	foreach (lc, node->filters)
	{
		HashFilterState *filter = (HashFilterState *) lfirst(lc);

		filter->built = false;
		memset(filter->data, 0, filter->nbits/8);
		filter->nvalues = 0;
	}

}


/*
 * ExecScanGetFilterHashValue
 *		Calculate a hash value for the tuple in the scan slot.
 *
 * We'll then check the presence of this hash value in the bloom filter.
 *
 * XXX Almost the same as ExecHashGetFilterHashValue, except that it's
 * executed for the filter reference. Could we refactor it somehow to
 * reduce the code duplication?
 *
 * XXX In any case, we should rename this to not include "scan" because
 * we could inject this to other node types (e.g. subquery).
 */
bool
ExecScanGetFilterHashValue(HashFilterReferenceState *ref,
						   ExprContext *econtext,
						   bool keep_nulls,
						   uint64 *hashvalue)
{
	uint64		hashkey = 0;
	FmgrInfo   *hashfunctions;
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;
	HashFilterState *filter = ref->filter;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	hashfunctions = filter->hashfunctions;

	/* XXX use expressions from the reference, with adjusted varnos etc. */
	foreach(hk, ref->clauses)
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
 * FIXME Almost the same as ExecHashGetFilterGetValues, except that it's
 * executed for the filter reference. Could we refactor it somehow to
 * reduce the code duplication?
 */
static bool
ExecScanGetFilterGetValues(HashFilterReferenceState *ref,
						   ExprContext *econtext,
						   bool keep_nulls,
						   Datum *values)
{
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;
	HashFilterState *filter = ref->filter;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/* XXX use expressions from the reference, with adjusted varnos etc. */
	foreach(hk, ref->clauses)
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
 * ExecHashFilterContainsHash
 *		Check if the tuple matches the Bloom filter.
 */
static bool
ExecHashFilterContainsHash(HashFilterReferenceState *refstate, ExprContext *econtext)
{
	int			i;
	uint64		h1,
				h2;
	uint64		hashvalue = 0;
	HashFilterState *filter = refstate->filter;

	ExecScanGetFilterHashValue(refstate, econtext, false, &hashvalue);

	Assert(filter->filter_type == HashFilterBloom);

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
 * ExecHashFilterContainsExact
 *		Check if the filter (in 'exact' mode) contains exact value.
 *
 * FIXME This assumes all the types allow sorting, but that may not be true.
 * In that case this should just do linear search.
 */
static bool
ExecHashFilterContainsExact(HashFilterReferenceState *refstate, ExprContext *econtext)
{
	HashFilterState *filter = refstate->filter;
	Datum	   *values;
	Size		entrysize = sizeof(Datum) * list_length(refstate->clauses);
	char	   *ptr;

	Assert(filter->filter_type == HashFilterExact);

	values = palloc(entrysize);

	ExecScanGetFilterGetValues(refstate, econtext, false, values);

	/* FIXME wrong, needs to use the proper comparator, not memcmp() */
	ptr = bsearch_arg(values, filter->data, filter->nvalues, entrysize, filter_comparator, &entrysize);

	if (ptr != NULL)
		filter->nhits++;

	pfree(values);

	/* all hashes found in bloom filter */
	return (ptr != NULL);
}

/*
 * ExecHashFilterContainsExact
 *		Check if the filter (in 'range' mode) contains exact value.
 *
 * FIXME This assumes all the types allow sorting, but that may not be true.
 * In that case this should just do linear search.
 */
static bool
ExecHashFilterContainsRange(HashFilterReferenceState *refstate, ExprContext *econtext)
{
	HashFilterState *filter = refstate->filter;
	Datum	   *values;
	Size		entrysize = sizeof(Datum) * list_length(refstate->clauses);

	Assert(filter->filter_type == HashFilterRange);

	values = palloc(entrysize);

	ExecScanGetFilterGetValues(refstate, econtext, false, values);

	/* FIXME wrong, needs to use the proper comparator, not memcmp() */
	/* TODO use binary search to check ranges */
	for (int i = 0; i < filter->nranges; i++)
	{
		Datum  *start,
			   *end;

		start = (Datum *) (filter->data + (2 * i * entrysize));
		end = (Datum *) (filter->data + ((2 * i + 1) * entrysize));

		if ((memcmp(values, start, entrysize) >= 0) &&
			(memcmp(values, end, entrysize) <= 0))
		{
			filter->nhits++;
			pfree(values);
			return true;
		}
	}

	for (int i = 2 * filter->nranges; i < filter->nvalues; i++)
	{
		Datum  *entry;

		entry = (Datum *) (filter->data + (2 * i * entrysize));

		if (memcmp(values, entry, entrysize) == 0)
		{
			filter->nhits++;
			pfree(values);
			return true;
		}
	}

	pfree(values);

	/* all hashes found in bloom filter */
	return false;
}

/*
 * ExecHashFilterContainsExact
 *		Check the filter - either in exact or hashed mode, as needed.
 */
bool
ExecHashFilterContainsValue(HashFilterReferenceState *refstate, ExprContext *econtext)
{
	HashFilterState *filter = refstate->filter;

	filter->nqueries++;

	if (filter->filter_type == HashFilterExact)
		return ExecHashFilterContainsExact(refstate, econtext);
	else if (filter->filter_type == HashFilterRange)
		return ExecHashFilterContainsRange(refstate, econtext);
	else
		return ExecHashFilterContainsHash(refstate, econtext);
}

HashFilterState *
ExecHashFilterInit(HashState *hashstate, Plan *outerPlan, HashFilter *filter)
{
	int			nkeys;
	int			i;
	ListCell   *ho,
			   *hc,
			   *hk;

	double		m, p, n, k;

	HashFilterState *state = makeNode(HashFilterState);

	/*
	 * Start the filter in exact mode, we'll switch to Bloom if we fill it.
	 *
	 * FIXME this is a bit misleading, because for byref values we only store
	 * the pointers to the filter. So there may be much more memory needed.
	 * This should copy the values into the filter.
	 */
	state->filter = filter;
	state->filterId = filter->filterId;

	state->clauses = ExecInitExprList(filter->clauses, (PlanState *) hashstate);

	nkeys = list_length(filter->hashoperators);
	state->hashfunctions = palloc_array(FmgrInfo, nkeys);
	state->hashStrict = palloc_array(bool, nkeys);
	state->collations = palloc_array(Oid, nkeys);
	state->types = palloc(sizeof(Oid) * nkeys);

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
	foreach(hk, filter->clauses)
	{
		state->types[i] = exprType(lfirst(hk));
		i++;
	}

	Assert(filter_pushdown_mode != FILTER_PUSHDOWN_OFF);

	if (filter_pushdown_mode == FILTER_PUSHDOWN_EXACT)
		state->filter_type = HashFilterExact;
	else if (filter_pushdown_mode == FILTER_PUSHDOWN_RANGE)
		state->filter_type = HashFilterRange;
	else if (filter_pushdown_mode == FILTER_PUSHDOWN_BLOOM)
		state->filter_type = HashFilterBloom;

	/* size using estimates */

	/*
	 * Maybe we should do some sort of balancing - if the filter gets too
	 * large with these params, try with a lower p value? Better to fit in
	 * L2/L3 with worse false positive rate than cache misses.
	 */
	p = 0.01;				/* 1% false positive */

	/* 1000 seems like a reasonable lower bound */
	n = Max(1000, outerPlan->plan_rows);	/* assume unique values */

	m = ceil((n * log(p)) / log(1 / pow(2, log(2))));
	k = round((m / n) * log(2)); 

	/* round to multiples of 8 */
	state->nbits = ((int) ((m + 7) / 8)) * 8;

	state->nhashes = k;
	state->data = palloc0(state->nbits / 8);

	/* exact hash */
	state->nvalues = 0;

	/* consider the filter not built yet */
	state->built = false;

	return state;
}
