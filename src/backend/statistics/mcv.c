/*-------------------------------------------------------------------------
 *
 * mcv.c
 *	  POSTGRES multivariate MCV lists
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/statistics/mcv.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_statistic_ext.h"
#include "fmgr.h"
#include "funcapi.h"
#include "optimizer/clauses.h"
#include "statistics/extended_stats_internal.h"
#include "statistics/statistics.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/*
 * Each serialized item needs to store (in this order):
 *
 * - indexes			  (ndim * sizeof(uint16))
 * - null flags			  (ndim * sizeof(bool))
 * - frequency			  (sizeof(double))
 *
 * So in total:
 *
 *	 ndim * (sizeof(uint16) + sizeof(bool)) + sizeof(double)
 */
#define ITEM_SIZE(ndims)	\
	(ndims * (sizeof(uint16) + sizeof(bool)) + sizeof(double))

/* Macros for convenient access to parts of the serialized MCV item */
#define ITEM_INDEXES(item)			((uint16*)item)
#define ITEM_NULLS(item,ndims)		((bool*)(ITEM_INDEXES(item) + ndims))
#define ITEM_FREQUENCY(item,ndims)	((double*)(ITEM_NULLS(item,ndims) + ndims))

static MultiSortSupport build_mss(VacAttrStats **stats, Bitmapset *attrs);

static SortItem *build_sorted_items(int numrows, HeapTuple *rows,
				   TupleDesc tdesc, MultiSortSupport mss,
				   Bitmapset *attrs);

static SortItem *build_distinct_groups(int numrows, SortItem *items,
					  MultiSortSupport mss, int *ndistinct);

static int count_distinct_groups(int numrows, SortItem *items,
					  MultiSortSupport mss);

static bool mcv_is_compatible_clause(Node *clause, Index relid,
					  Bitmapset **attnums);

/*
 * Builds MCV list from the set of sampled rows.
 *
 * The algorithm is quite simple:
 *
 *	   (1) sort the data (default collation, '<' for the data type)
 *
 *	   (2) count distinct groups, decide how many to keep
 *
 *	   (3) build the MCV list using the threshold determined in (2)
 *
 *	   (4) remove rows represented by the MCV from the sample
 *
 * The method also removes rows matching the MCV items from the input array,
 * and passes the number of remaining rows (useful for building histograms)
 * using the numrows_filtered parameter.
 *
 * FIXME: Single-dimensional MCV is sorted by frequency (descending). We should
 * do that too, because when walking through the list we want to check
 * the most frequent items first.
 *
 * TODO: We're using Datum (8B), even for data types (e.g. int4 or float4).
 * Maybe we could save some space here, but the bytea compression should
 * handle it just fine.
 *
 * TODO: This probably should not use the ndistinct directly (as computed from
 * the table, but rather estimate the number of distinct values in the
 * table), no?
 */
MCVList *
statext_mcv_build(int numrows, HeapTuple *rows, Bitmapset *attrs,
				 VacAttrStats **stats, int *numrows_filtered)
{
	int			i;
	int			j;
	int			numattrs = bms_num_members(attrs);
	int			ndistinct = 0;
	int			mcv_threshold = 0;
	int			nitems = 0;
	int		   *attnums;

	MCVList	   *mcvlist = NULL;

	/* comparator for all the columns */
	MultiSortSupport mss = build_mss(stats, attrs);

	/* sort the rows */
	SortItem   *items = build_sorted_items(numrows, rows, stats[0]->tupDesc,
										   mss, attrs);

	/* transform the sorted rows into groups (sorted by frequency) */
	SortItem   *groups = build_distinct_groups(numrows, items, mss, &ndistinct);

	/*
	 * Determine the minimum size of a group to be eligible for MCV list, and
	 * check how many groups actually pass that threshold. We use 1.25x the
	 * avarage group size, just like for regular statistics.
	 *
	 * But if we can fit all the distinct values in the MCV list (i.e. if
	 * there are less distinct groups than STATS_MCVLIST_MAX_ITEMS), we'll
	 * require only 2 rows per group.
	 */
	mcv_threshold = 1.25 * numrows / ndistinct;
	mcv_threshold = (mcv_threshold < 4) ? 4 : mcv_threshold;

	if (ndistinct <= STATS_MCVLIST_MAX_ITEMS)
		mcv_threshold = 2;

	/* build array of attnums from the bitmapset */
	attnums = (int*)palloc(sizeof(int) * numattrs);
	i = 0;
	j = -1;
	while ((j = bms_next_member(attrs, j)) >= 0)
		attnums[i++] = j;

	/* Walk through the groups and stop once we fall below the threshold. */
	nitems = 0;
	for (i = 0; i < ndistinct; i++)
	{
		if (groups[i].count < mcv_threshold)
			break;

		nitems++;
	}

	/* we know the number of MCV list items, so let's build the list */
	if (nitems > 0)
	{
		/* allocate the MCV list structure, set parameters we know */
		mcvlist = (MCVList *) palloc0(sizeof(MCVList));

		mcvlist->magic = STATS_MCV_MAGIC;
		mcvlist->type = STATS_MCV_TYPE_BASIC;
		mcvlist->ndimensions = numattrs;
		mcvlist->nitems = nitems;

		/*
		 * Preallocate Datum/isnull arrays (not as a single chunk, as we will
		 * pass the result outside and thus it needs to be easy to pfree().
		 *
		 * XXX Although we're the only ones dealing with this.
		 */
		mcvlist->items = (MCVItem **) palloc0(sizeof(MCVItem *) * nitems);

		for (i = 0; i < nitems; i++)
		{
			mcvlist->items[i] = (MCVItem *) palloc0(sizeof(MCVItem));
			mcvlist->items[i]->values = (Datum *) palloc0(sizeof(Datum) * numattrs);
			mcvlist->items[i]->isnull = (bool *) palloc0(sizeof(bool) * numattrs);
		}

		/* Copy the first chunk of groups into the result. */
		for (i = 0; i < nitems; i++)
		{
			/* just pointer to the proper place in the list */
			MCVItem	   *item = mcvlist->items[i];

			/* copy values from the _previous_ group (last item of) */
			memcpy(item->values, groups[i].values, sizeof(Datum) * numattrs);
			memcpy(item->isnull, groups[i].isnull, sizeof(bool) * numattrs);

			/* and finally the group frequency */
			item->frequency = (double) groups[i].count / numrows;
		}

		/* make sure the loops are consistent */
		Assert(nitems == mcvlist->nitems);

		/*
		 * Remove the rows matching the MCV list (i.e. keep only rows that are
		 * not represented by the MCV list). We will first sort the groups by
		 * the keys (not by count) and then use binary search.
		 */
		if (nitems > ndistinct)
		{
			int			i,
						j;
			int			nfiltered = 0;

			/* used for the searches */
			SortItem	key;

			/* wfill this with data from the rows */
			key.values = (Datum *) palloc0(numattrs * sizeof(Datum));
			key.isnull = (bool *) palloc0(numattrs * sizeof(bool));

			/*
			 * Sort the groups for bsearch_r (but only the items that actually
			 * made it to the MCV list).
			 */
			qsort_arg((void *) groups, nitems, sizeof(SortItem),
					  multi_sort_compare, mss);

			/* walk through the tuples, compare the values to MCV items */
			for (i = 0; i < numrows; i++)
			{
				/* collect the key values from the row */
				for (j = 0; j < numattrs; j++)
					key.values[j]
						= heap_getattr(rows[i], attnums[j],
									   stats[j]->tupDesc, &key.isnull[j]);

				/* if not included in the MCV list, keep it in the array */
				if (bsearch_arg(&key, groups, nitems, sizeof(SortItem),
								multi_sort_compare, mss) == NULL)
					rows[nfiltered++] = rows[i];
			}

			/* remember how many rows we actually kept */
			*numrows_filtered = nfiltered;

			/* free all the data used here */
			pfree(key.values);
			pfree(key.isnull);
		}
		else
			/* the MCV list convers all the rows */
			*numrows_filtered = 0;
	}

	pfree(items);
	pfree(groups);

	return mcvlist;
}

/* build MultiSortSupport for the attributes passed in attrs */
static MultiSortSupport
build_mss(VacAttrStats **stats, Bitmapset *attrs)
{
	int			i, j;
	int			numattrs = bms_num_members(attrs);

	/* Sort by multiple columns (using array of SortSupport) */
	MultiSortSupport mss = multi_sort_init(numattrs);

	/* prepare the sort functions for all the attributes */
	i = 0;
	j = -1;
	while ((j = bms_next_member(attrs, j)) >= 0)
	{
		VacAttrStats *colstat = stats[i];	/* FIXME maybe this should be "j" ? */
		TypeCacheEntry *type;

		type = lookup_type_cache(colstat->attrtypid, TYPECACHE_LT_OPR);
		if (type->lt_opr == InvalidOid) /* shouldn't happen */
			elog(ERROR, "cache lookup failed for ordering operator for type %u",
				 colstat->attrtypid);

		multi_sort_add_dimension(mss, i, type->lt_opr);
	}

	return mss;
}

/* build sorted array of SortItem with values from rows */
static SortItem *
build_sorted_items(int numrows, HeapTuple *rows, TupleDesc tdesc,
				   MultiSortSupport mss, Bitmapset *attrs)
{
	int			i,
				j,
				len;
	int			numattrs = bms_num_members(attrs);
	int			nvalues = numrows * numattrs;
	int		   *attnums;

	/*
	 * We won't allocate the arrays for each item independenly, but in one
	 * large chunk and then just set the pointers.
	 */
	SortItem   *items;
	Datum	   *values;
	bool	   *isnull;
	char	   *ptr;

	/* build attnums from the bitmapset */
	attnums = (int*)palloc(sizeof(int) * numattrs);
	i = 0;
	j = -1;
	while ((j = bms_next_member(attrs, j)) >= 0)
		attnums[i++] = j;

	/* Compute the total amount of memory we need (both items and values). */
	len = numrows * sizeof(SortItem) + nvalues * (sizeof(Datum) + sizeof(bool));

	/* Allocate the memory and split it into the pieces. */
	ptr = palloc0(len);

	/* items to sort */
	items = (SortItem *) ptr;
	ptr += numrows * sizeof(SortItem);

	/* values and null flags */
	values = (Datum *) ptr;
	ptr += nvalues * sizeof(Datum);

	isnull = (bool *) ptr;
	ptr += nvalues * sizeof(bool);

	/* make sure we consumed the whole buffer exactly */
	Assert((ptr - (char *) items) == len);

	/* fix the pointers to Datum and bool arrays */
	for (i = 0; i < numrows; i++)
	{
		items[i].values = &values[i * numattrs];
		items[i].isnull = &isnull[i * numattrs];

		/* load the values/null flags from sample rows */
		for (j = 0; j < numattrs; j++)
		{
			items[i].values[j] = heap_getattr(rows[i],
											  attnums[j], /* attnum */
											  tdesc,
											  &items[i].isnull[j]);		/* isnull */
		}
	}

	/* do the sort, using the multi-sort */
	qsort_arg((void *) items, numrows, sizeof(SortItem),
			  multi_sort_compare, mss);

	return items;
}

/* count distinct combinations of SortItems in the array */
static int
count_distinct_groups(int numrows, SortItem *items, MultiSortSupport mss)
{
	int			i;
	int			ndistinct;

	ndistinct = 1;
	for (i = 1; i < numrows; i++)
		if (multi_sort_compare(&items[i], &items[i - 1], mss) != 0)
			ndistinct += 1;

	return ndistinct;
}

/* compares frequencies of the SortItem entries (in descending order) */
static int
compare_sort_item_count(const void *a, const void *b)
{
	SortItem   *ia = (SortItem *) a;
	SortItem   *ib = (SortItem *) b;

	if (ia->count == ib->count)
		return 0;
	else if (ia->count > ib->count)
		return -1;

	return 1;
}

/* builds SortItems for distinct groups and counts the matching items */
static SortItem *
build_distinct_groups(int numrows, SortItem *items, MultiSortSupport mss,
					  int *ndistinct)
{
	int			i,
				j;
	int			ngroups = count_distinct_groups(numrows, items, mss);

	SortItem   *groups = (SortItem *) palloc0(ngroups * sizeof(SortItem));

	j = 0;
	groups[0] = items[0];
	groups[0].count = 1;

	for (i = 1; i < numrows; i++)
	{
		if (multi_sort_compare(&items[i], &items[i - 1], mss) != 0)
			groups[++j] = items[i];

		groups[j].count++;
	}

	pg_qsort((void *) groups, ngroups, sizeof(SortItem),
			 compare_sort_item_count);

	*ndistinct = ngroups;
	return groups;
}


/* fetch the MCV list (as a bytea) from the pg_statistic_ext catalog */
MCVList *
statext_mcv_load(Oid mvoid)
{
	bool		isnull = false;
	Datum		mcvlist;

#ifdef USE_ASSERT_CHECKING
	// Form_pg_statistic_ext STATS;
#endif

	/* Prepare to scan pg_statistic_ext for entries having indrelid = this rel. */
	HeapTuple	htup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(mvoid));

	if (!HeapTupleIsValid(htup))
		return NULL;

#ifdef USE_ASSERT_CHECKING
	// STATS = (Form_pg_statistic_ext) GETSTRUCT(htup);
	// Assert(STATS->mcv_enabled && STATS->mcv_built);
#endif

	mcvlist = SysCacheGetAttr(STATEXTOID, htup,
							  Anum_pg_statistic_ext_stxmcv, &isnull);

	Assert(!isnull);

	ReleaseSysCache(htup);

	return statext_mcv_deserialize(DatumGetByteaP(mcvlist));
}

/* print some basic info about the MCV list
 *
 * TODO: Add info about what part of the table this covers.
 */
Datum
pg_stats_ext_mcvlist_info(PG_FUNCTION_ARGS)
{
	bytea	   *data = PG_GETARG_BYTEA_P(0);
	char	   *result;

	MCVList	   *mcvlist = statext_mcv_deserialize(data);

	result = palloc0(128);
	snprintf(result, 128, "nitems=%d", mcvlist->nitems);

	pfree(mcvlist);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * serialize MCV list into a bytea value
 *
 *
 * The basic algorithm is simple:
 *
 * (1) perform deduplication (for each attribute separately)
 *	   (a) collect all (non-NULL) attribute values from all MCV items
 *	   (b) sort the data (using 'lt' from VacAttrStats)
 *	   (c) remove duplicate values from the array
 *
 * (2) serialize the arrays into a bytea value
 *
 * (3) process all MCV list items
 *	   (a) replace values with indexes into the arrays
 *
 * Each attribute has to be processed separately, because we may be mixing
 * different datatypes, with different sort operators, etc.
 *
 * We'll use uint16 values for the indexes in step (3), as we don't allow more
 * than 8k MCV items, although that's mostly arbitrary limit. We might increase
 * this to 65k and still fit into uint16.
 *
 * We don't really expect the serialization to save as much space as for
 * histograms, because we are not doing any bucket splits (which is the source
 * of high redundancy in histograms).
 *
 * TODO: Consider packing boolean flags (NULL) for each item into a single char
 * (or a longer type) instead of using an array of bool items.
 */
bytea *
statext_mcv_serialize(MCVList *mcvlist, VacAttrStats **stats)
{
	int			i,
				j;
	int			ndims = mcvlist->ndimensions;
	int			itemsize = ITEM_SIZE(ndims);

	SortSupport ssup;
	DimensionInfo *info;

	Size		total_length;

	/* allocate just once */
	char	   *item = palloc0(itemsize);

	/* serialized items (indexes into arrays, etc.) */
	bytea	   *output;
	char	   *data = NULL;

	/* values per dimension (and number of non-NULL values) */
	Datum	  **values = (Datum **) palloc0(sizeof(Datum *) * ndims);
	int		   *counts = (int *) palloc0(sizeof(int) * ndims);

	/*
	 * We'll include some rudimentary information about the attributes (type
	 * length, etc.), so that we don't have to look them up while
	 * deserializing the MCV list.
	 */
	info = (DimensionInfo *) palloc0(sizeof(DimensionInfo) * ndims);

	/* sort support data for all attributes included in the MCV list */
	ssup = (SortSupport) palloc0(sizeof(SortSupportData) * ndims);

	/* collect and deduplicate values for all attributes */
	for (i = 0; i < ndims; i++)
	{
		int			ndistinct;
		StdAnalyzeData *tmp = (StdAnalyzeData *) stats[i]->extra_data;

		/* copy important info about the data type (length, by-value) */
		info[i].typlen = stats[i]->attrtype->typlen;
		info[i].typbyval = stats[i]->attrtype->typbyval;

		/* allocate space for values in the attribute and collect them */
		values[i] = (Datum *) palloc0(sizeof(Datum) * mcvlist->nitems);

		for (j = 0; j < mcvlist->nitems; j++)
		{
			/* skip NULL values - we don't need to serialize them */
			if (mcvlist->items[j]->isnull[i])
				continue;

			values[i][counts[i]] = mcvlist->items[j]->values[i];
			counts[i] += 1;
		}

		/* there are just NULL values in this dimension, we're done */
		if (counts[i] == 0)
			continue;

		/* sort and deduplicate the data */
		ssup[i].ssup_cxt = CurrentMemoryContext;
		ssup[i].ssup_collation = DEFAULT_COLLATION_OID;
		ssup[i].ssup_nulls_first = false;

		PrepareSortSupportFromOrderingOp(tmp->ltopr, &ssup[i]);

		qsort_arg(values[i], counts[i], sizeof(Datum),
				  compare_scalars_simple, &ssup[i]);

		/*
		 * Walk through the array and eliminate duplicate values, but keep the
		 * ordering (so that we can do bsearch later). We know there's at
		 * least one item as (counts[i] != 0), so we can skip the first
		 * element.
		 */
		ndistinct = 1;			/* number of distinct values */
		for (j = 1; j < counts[i]; j++)
		{
			/* if the value is the same as the previous one, we can skip it */
			if (!compare_datums_simple(values[i][j - 1], values[i][j], &ssup[i]))
				continue;

			values[i][ndistinct] = values[i][j];
			ndistinct += 1;
		}

		/* we must not exceed UINT16_MAX, as we use uint16 indexes */
		Assert(ndistinct <= UINT16_MAX);

		/*
		 * Store additional info about the attribute - number of deduplicated
		 * values, and also size of the serialized data. For fixed-length data
		 * types this is trivial to compute, for varwidth types we need to
		 * actually walk the array and sum the sizes.
		 */
		info[i].nvalues = ndistinct;

		if (info[i].typlen > 0) /* fixed-length data types */
			info[i].nbytes = info[i].nvalues * info[i].typlen;
		else if (info[i].typlen == -1)	/* varlena */
		{
			info[i].nbytes = 0;
			for (j = 0; j < info[i].nvalues; j++)
				info[i].nbytes += VARSIZE_ANY(values[i][j]);
		}
		else if (info[i].typlen == -2)	/* cstring */
		{
			info[i].nbytes = 0;
			for (j = 0; j < info[i].nvalues; j++)
				info[i].nbytes += strlen(DatumGetPointer(values[i][j]));
		}

		/* we know (count>0) so there must be some data */
		Assert(info[i].nbytes > 0);
	}

	/*
	 * Now we can finally compute how much space we'll actually need for the
	 * serialized MCV list, as it contains these fields:
	 *
	 * - length (4B) for varlena - magic (4B) - type (4B) - ndimensions (4B) -
	 * nitems (4B) - info (ndim * sizeof(DimensionInfo) - arrays of values for
	 * each dimension - serialized items (nitems * itemsize)
	 *
	 * So the 'header' size is 20B + ndim * sizeof(DimensionInfo) and then we
	 * will place all the data (values + indexes).
	 */
	total_length = (sizeof(int32) + offsetof(MCVList, items)
					+ndims * sizeof(DimensionInfo)
					+ mcvlist->nitems * itemsize);

	for (i = 0; i < ndims; i++)
		total_length += info[i].nbytes;

	/* enforce arbitrary limit of 1MB */
	if (total_length > (1024 * 1024))
		elog(ERROR, "serialized MCV list exceeds 1MB (%ld)", total_length);

	/* allocate space for the serialized MCV list, set header fields */
	output = (bytea *) palloc0(total_length);
	SET_VARSIZE(output, total_length);

	/* 'data' points to the current position in the output buffer */
	data = VARDATA(output);

	/* MCV list header (number of items, ...) */
	memcpy(data, mcvlist, offsetof(MCVList, items));
	data += offsetof(MCVList, items);

	/* information about the attributes */
	memcpy(data, info, sizeof(DimensionInfo) * ndims);
	data += sizeof(DimensionInfo) * ndims;

	/* now serialize the deduplicated values for all attributes */
	for (i = 0; i < ndims; i++)
	{
#ifdef USE_ASSERT_CHECKING
		char	   *tmp = data; /* remember the starting point */
#endif
		for (j = 0; j < info[i].nvalues; j++)
		{
			Datum		v = values[i][j];

			if (info[i].typbyval)		/* passed by value */
			{
				memcpy(data, &v, info[i].typlen);
				data += info[i].typlen;
			}
			else if (info[i].typlen > 0)		/* pased by reference */
			{
				memcpy(data, DatumGetPointer(v), info[i].typlen);
				data += info[i].typlen;
			}
			else if (info[i].typlen == -1)		/* varlena */
			{
				memcpy(data, DatumGetPointer(v), VARSIZE_ANY(v));
				data += VARSIZE_ANY(v);
			}
			else if (info[i].typlen == -2)		/* cstring */
			{
				memcpy(data, DatumGetPointer(v), strlen(DatumGetPointer(v)) + 1);
				data += strlen(DatumGetPointer(v)) + 1; /* terminator */
			}
		}

		/* make sure we got exactly the amount of data we expected */
		Assert((data - tmp) == info[i].nbytes);
	}

	/* finally serialize the items, with uint16 indexes instead of the values */
	for (i = 0; i < mcvlist->nitems; i++)
	{
		MCVItem	   *mcvitem = mcvlist->items[i];

		/* don't write beyond the allocated space */
		Assert(data <= (char *) output + total_length - itemsize);

		/* reset the item (we only allocate it once and reuse it) */
		memset(item, 0, itemsize);

		for (j = 0; j < ndims; j++)
		{
			Datum	   *v = NULL;

			/* do the lookup only for non-NULL values */
			if (mcvlist->items[i]->isnull[j])
				continue;

			v = (Datum *) bsearch_arg(&mcvitem->values[j], values[j],
									  info[j].nvalues, sizeof(Datum),
									  compare_scalars_simple, &ssup[j]);

			Assert(v != NULL);	/* serialization or deduplication error */

			/* compute index within the array */
			ITEM_INDEXES(item)[j] = (v - values[j]);

			/* check the index is within expected bounds */
			Assert(ITEM_INDEXES(item)[j] >= 0);
			Assert(ITEM_INDEXES(item)[j] < info[j].nvalues);
		}

		/* copy NULL and frequency flags into the item */
		memcpy(ITEM_NULLS(item, ndims), mcvitem->isnull, sizeof(bool) * ndims);
		memcpy(ITEM_FREQUENCY(item, ndims), &mcvitem->frequency, sizeof(double));

		/* copy the serialized item into the array */
		memcpy(data, item, itemsize);

		data += itemsize;
	}

	/* at this point we expect to match the total_length exactly */
	Assert((data - (char *) output) == total_length);

	return output;
}

/*
 * deserialize MCV list from the varlena value
 *
 *
 * We deserialize the MCV list fully, because we don't expect there bo be a lot
 * of duplicate values. But perhaps we should keep the MCV in serialized form
 * just like histograms.
 */
MCVList *
statext_mcv_deserialize(bytea *data)
{
	int			i,
				j;
	Size		expected_size;
	MCVList	   *mcvlist;
	char	   *tmp;

	int			ndims,
				nitems,
				itemsize;
	DimensionInfo *info = NULL;

	uint16	   *indexes = NULL;
	Datum	  **values = NULL;

	/* local allocation buffer (used only for deserialization) */
	int			bufflen;
	char	   *buff;
	char	   *ptr;

	/* buffer used for the result */
	int			rbufflen;
	char	   *rbuff;
	char	   *rptr;

	if (data == NULL)
		return NULL;

	/* we can't deserialize the MCV if there's not even a complete header */
	expected_size = offsetof(MCVList, items);

	if (VARSIZE_ANY_EXHDR(data) < expected_size)
		elog(ERROR, "invalid MCV Size %ld (expected at least %ld)",
			 VARSIZE_ANY_EXHDR(data), offsetof(MCVList, items));

	/* read the MCV list header */
	mcvlist = (MCVList *) palloc0(sizeof(MCVList));

	/* initialize pointer to the data part (skip the varlena header) */
	tmp = VARDATA_ANY(data);

	/* get the header and perform further sanity checks */
	memcpy(mcvlist, tmp, offsetof(MCVList, items));
	tmp += offsetof(MCVList, items);

	if (mcvlist->magic != STATS_MCV_MAGIC)
		elog(ERROR, "invalid MCV magic %d (expected %dd)",
			 mcvlist->magic, STATS_MCV_MAGIC);

	if (mcvlist->type != STATS_MCV_TYPE_BASIC)
		elog(ERROR, "invalid MCV type %d (expected %dd)",
			 mcvlist->type, STATS_MCV_TYPE_BASIC);

	nitems = mcvlist->nitems;
	ndims = mcvlist->ndimensions;
	itemsize = ITEM_SIZE(ndims);

	Assert((nitems > 0) && (nitems <= STATS_MCVLIST_MAX_ITEMS));
	Assert((ndims >= 2) && (ndims <= STATS_MAX_DIMENSIONS));

	/*
	 * Check amount of data including DimensionInfo for all dimensions and
	 * also the serialized items (including uint16 indexes). Also, walk
	 * through the dimension information and add it to the sum.
	 */
	expected_size += ndims * sizeof(DimensionInfo) +
		(nitems * itemsize);

	/* check that we have at least the DimensionInfo records */
	if (VARSIZE_ANY_EXHDR(data) < expected_size)
		elog(ERROR, "invalid MCV size %ld (expected %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	info = (DimensionInfo *) (tmp);
	tmp += ndims * sizeof(DimensionInfo);

	/* account for the value arrays */
	for (i = 0; i < ndims; i++)
	{
		Assert(info[i].nvalues >= 0);
		Assert(info[i].nbytes >= 0);

		expected_size += info[i].nbytes;
	}

	if (VARSIZE_ANY_EXHDR(data) != expected_size)
		elog(ERROR, "invalid MCV size %ld (expected %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	/* looks OK - not corrupted or something */

	/*
	 * Allocate one large chunk of memory for the intermediate data, needed
	 * only for deserializing the MCV list (and allocate densely to minimize
	 * the palloc overhead).
	 *
	 * Let's see how much space we'll actually need, and also include space
	 * for the array with pointers.
	 */
	bufflen = sizeof(Datum *) * ndims;	/* space for pointers */

	for (i = 0; i < ndims; i++)
		/* for full-size byval types, we reuse the serialized value */
		if (!(info[i].typbyval && info[i].typlen == sizeof(Datum)))
			bufflen += (sizeof(Datum) * info[i].nvalues);

	buff = palloc0(bufflen);
	ptr = buff;

	values = (Datum **) buff;
	ptr += (sizeof(Datum *) * ndims);

	/*
	 * XXX This uses pointers to the original data array (the types not passed
	 * by value), so when someone frees the memory, e.g. by doing something
	 * like this:
	 *
	 * bytea * data = ... fetch the data from catalog ... MCVList mcvlist =
	 * deserialize_mcv_list(data); pfree(data);
	 *
	 * then 'mcvlist' references the freed memory. Should copy the pieces.
	 */
	for (i = 0; i < ndims; i++)
	{
		if (info[i].typbyval)
		{
			/* passed by value / Datum - simply reuse the array */
			if (info[i].typlen == sizeof(Datum))
			{
				values[i] = (Datum *) tmp;
				tmp += info[i].nbytes;
			}
			else
			{
				values[i] = (Datum *) ptr;
				ptr += (sizeof(Datum) * info[i].nvalues);

				for (j = 0; j < info[i].nvalues; j++)
				{
					/* just point into the array */
					memcpy(&values[i][j], tmp, info[i].typlen);
					tmp += info[i].typlen;
				}
			}
		}
		else
		{
			/* all the other types need a chunk of the buffer */
			values[i] = (Datum *) ptr;
			ptr += (sizeof(Datum) * info[i].nvalues);

			/* pased by reference, but fixed length (name, tid, ...) */
			if (info[i].typlen > 0)
			{
				for (j = 0; j < info[i].nvalues; j++)
				{
					/* just point into the array */
					values[i][j] = PointerGetDatum(tmp);
					tmp += info[i].typlen;
				}
			}
			else if (info[i].typlen == -1)
			{
				/* varlena */
				for (j = 0; j < info[i].nvalues; j++)
				{
					/* just point into the array */
					values[i][j] = PointerGetDatum(tmp);
					tmp += VARSIZE_ANY(tmp);
				}
			}
			else if (info[i].typlen == -2)
			{
				/* cstring */
				for (j = 0; j < info[i].nvalues; j++)
				{
					/* just point into the array */
					values[i][j] = PointerGetDatum(tmp);
					tmp += (strlen(tmp) + 1);	/* don't forget the \0 */
				}
			}
		}
	}

	/* we should have exhausted the buffer exactly */
	Assert((ptr - buff) == bufflen);

	/* allocate space for all the MCV items in a single piece */
	rbufflen = (sizeof(MCVItem*) + sizeof(MCVItem) +
				sizeof(Datum) * ndims + sizeof(bool) * ndims) * nitems;

	rbuff = palloc0(rbufflen);
	rptr = rbuff;

	mcvlist->items = (MCVItem **) rbuff;
	rptr += (sizeof(MCVItem *) * nitems);

	for (i = 0; i < nitems; i++)
	{
		MCVItem	   *item = (MCVItem *) rptr;

		rptr += (sizeof(MCVItem));

		item->values = (Datum *) rptr;
		rptr += (sizeof(Datum) * ndims);

		item->isnull = (bool *) rptr;
		rptr += (sizeof(bool) * ndims);

		/* just point to the right place */
		indexes = ITEM_INDEXES(tmp);

		memcpy(item->isnull, ITEM_NULLS(tmp, ndims), sizeof(bool) * ndims);
		memcpy(&item->frequency, ITEM_FREQUENCY(tmp, ndims), sizeof(double));

#ifdef ASSERT_CHECKING
		for (j = 0; j < ndims; j++)
			Assert(indexes[j] <= UINT16_MAX);
#endif

		/* translate the values */
		for (j = 0; j < ndims; j++)
			if (!item->isnull[j])
				item->values[j] = values[j][indexes[j]];

		mcvlist->items[i] = item;

		tmp += ITEM_SIZE(ndims);

		Assert(tmp <= (char *) data + VARSIZE_ANY(data));
	}

	/* check that we processed all the data */
	Assert(tmp == (char *) data + VARSIZE_ANY(data));

	/* release the temporary buffer */
	pfree(buff);

	return mcvlist;
}

/*
 * SRF with details about buckets of a histogram:
 *
 * - item ID (0...nitems)
 * - values (string array)
 * - nulls only (boolean array)
 * - frequency (double precision)
 *
 * The input is the OID of the statistics, and there are no rows returned if
 * the statistics contains no histogram.
 */
PG_FUNCTION_INFO_V1(pg_stats_ext_mcvlist_items);

Datum
pg_stats_ext_mcvlist_items(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int			call_cntr;
	int			max_calls;
	TupleDesc	tupdesc;
	AttInMetadata *attinmeta;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		MCVList	   *mcvlist;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		mcvlist = statext_mcv_load(PG_GETARG_OID(0));

		funcctx->user_fctx = mcvlist;

		/* total number of tuples to be returned */
		funcctx->max_calls = 0;
		if (funcctx->user_fctx != NULL)
			funcctx->max_calls = mcvlist->nitems;

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		/* build metadata needed later to produce tuples from raw C-strings */
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls)	/* do when there is more left to send */
	{
		char	  **values;
		HeapTuple	tuple;
		Datum		result;
		int2vector *stakeys;
		Oid			relid;

		char	   *buff = palloc0(1024);
		char	   *format;

		int			i;

		Oid		   *outfuncs;
		FmgrInfo   *fmgrinfo;

		MCVList	   *mcvlist;
		MCVItem	   *item;

		mcvlist = (MCVList *) funcctx->user_fctx;

		Assert(call_cntr < mcvlist->nitems);

		item = mcvlist->items[call_cntr];

		stakeys = find_ext_attnums(PG_GETARG_OID(0), &relid);

		/*
		 * Prepare a values array for building the returned tuple. This should
		 * be an array of C strings which will be processed later by the type
		 * input functions.
		 */
		values = (char **) palloc(4 * sizeof(char *));

		values[0] = (char *) palloc(64 * sizeof(char));

		/* arrays */
		values[1] = (char *) palloc0(1024 * sizeof(char));
		values[2] = (char *) palloc0(1024 * sizeof(char));

		/* frequency */
		values[3] = (char *) palloc(64 * sizeof(char));

		outfuncs = (Oid *) palloc0(sizeof(Oid) * mcvlist->ndimensions);
		fmgrinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * mcvlist->ndimensions);

		for (i = 0; i < mcvlist->ndimensions; i++)
		{
			bool		isvarlena;

			getTypeOutputInfo(get_atttype(relid, stakeys->values[i]),
							  &outfuncs[i], &isvarlena);

			fmgr_info(outfuncs[i], &fmgrinfo[i]);
		}

		snprintf(values[0], 64, "%d", call_cntr);		/* item ID */

		for (i = 0; i < mcvlist->ndimensions; i++)
		{
			Datum		val,
						valout;

			format = "%s, %s";
			if (i == 0)
				format = "{%s%s";
			else if (i == mcvlist->ndimensions - 1)
				format = "%s, %s}";

			if (item->isnull[i])
				valout = CStringGetDatum("NULL");
			else
			{
				val = item->values[i];
				valout = FunctionCall1(&fmgrinfo[i], val);
			}

			snprintf(buff, 1024, format, values[1], DatumGetPointer(valout));
			strncpy(values[1], buff, 1023);
			buff[0] = '\0';

			snprintf(buff, 1024, format, values[2], item->isnull[i] ? "t" : "f");
			strncpy(values[2], buff, 1023);
			buff[0] = '\0';
		}

		snprintf(values[3], 64, "%f", item->frequency); /* frequency */

		/* build a tuple */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		/* clean up (this is not really necessary) */
		pfree(values[0]);
		pfree(values[1]);
		pfree(values[2]);
		pfree(values[3]);

		pfree(values);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else	/* do when there is no more left */
	{
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * pg_mcv_list_in		- input routine for type PG_MCV_LIST.
 *
 * pg_mcv_list is real enough to be a table column, but it has no operations
 * of its own, and disallows input too
 *
 * XXX This is inspired by what pg_node_tree does.
 */
Datum
pg_mcv_list_in(PG_FUNCTION_ARGS)
{
	/*
	 * pg_node_list stores the data in binary form and parsing text input is
	 * not needed, so disallow this.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_mcv_list")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}


/*
 * pg_mcv_list_out		- output routine for type PG_MCV_LIST.
 *
 * MCV lists are serialized into a bytea value, so we simply call byteaout()
 * to serialize the value into text. But it'd be nice to serialize that into
 * a meaningful representation (e.g. for inspection by people).
 *
 * FIXME not implemented yet, returning dummy value
 */
Datum
pg_mcv_list_out(PG_FUNCTION_ARGS)
{
	return byteaout(fcinfo);
}

/*
 * pg_mcv_list_recv		- binary input routine for type PG_MCV_LIST.
 */
Datum
pg_mcv_list_recv(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_mcv_list")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

/*
 * pg_mcv_list_send		- binary output routine for type PG_MCV_LIST.
 *
 * XXX MCV lists are serialized into a bytea value, so let's just send that.
 */
Datum
pg_mcv_list_send(PG_FUNCTION_ARGS)
{
	return byteasend(fcinfo);
}


/*
 * mcv_is_compatible_clause
 *		Determines if the clause is compatible with MCV lists
 *
 * Only OpExprs with two arguments using an equality operator are supported.
 * When returning True attnum is set to the attribute number of the Var within
 * the supported clause.
 *
 * Currently we only support Var = Const, or Const = Var. It may be possible
 * to expand on this later.
 */
static bool
mcv_is_compatible_clause(Node *clause, Index relid, Bitmapset **attnums)
{
	RestrictInfo *rinfo = (RestrictInfo *) clause;

	if (!IsA(rinfo, RestrictInfo))
		return false;

	/* Pseudoconstants are not really interesting here. */
	if (rinfo->pseudoconstant)
		return false;

	/* clauses referencing multiple varnos are incompatible */
	if (bms_membership(rinfo->clause_relids) != BMS_SINGLETON)
		return false;

	if (or_clause((Node *)rinfo->clause) ||
		and_clause((Node *)rinfo->clause) ||
		not_clause((Node *)rinfo->clause))
	{
		/*
		 * AND/OR/NOT-clauses are supported if all sub-clauses are supported
		 *
		 * TODO: We might support mixed case, where some of the clauses are
		 * supported and some are not, and treat all supported subclauses as a
		 * single clause, compute it's selectivity using mv stats, and compute
		 * the total selectivity using the current algorithm.
		 *
		 * TODO: For RestrictInfo above an OR-clause, we might use the
		 * orclause with nested RestrictInfo - we won't have to call
		 * pull_varnos() for each clause, saving time.
		 */
		BoolExpr   *expr = (BoolExpr *) rinfo->clause;
		ListCell   *lc;
		Bitmapset  *clause_attnums = NULL;

		foreach(lc, expr->args)
		{
			/*
			 * Had we found incompatible clause in the arguments, treat the
			 * whole clause as incompatible.
			 */
			if (!mcv_is_compatible_clause((Node *) lfirst(lc), relid,
										  &clause_attnums))
				return false;
		}

		/*
		 * Otherwise the clause is compatible, and we need to merge the
		 * attnums into the main bitmapset.
		 */
		*attnums = bms_join(*attnums, clause_attnums);

		return true;
	}

	if (IsA((Node *)rinfo->clause, NullTest))
	{
		NullTest   *nt = (NullTest *) rinfo->clause;

		/*
		 * Only simple (Var IS NULL) expressions supported for now. Maybe we
		 * could use examine_variable to fix this?
		 */
		if (!IsA(nt->arg, Var))
			return false;

		return mcv_is_compatible_clause((Node *) (nt->arg), relid, attnums);
	}

	if (is_opclause((Node *)rinfo->clause))
	{
		OpExpr	   *expr = (OpExpr *) rinfo->clause;
		Var		   *var;
		bool		varonleft = true;
		bool		ok;

		/* Only expressions with two arguments are considered compatible. */
		if (list_length(expr->args) != 2)
			return false;

		/* see if it actually has the right */
		ok = (NumRelids((Node *) expr) == 1) &&
			(is_pseudo_constant_clause(lsecond(expr->args)) ||
			 (varonleft = false,
			  is_pseudo_constant_clause(linitial(expr->args))));

		/* unsupported structure (two variables or so) */
		if (!ok)
			return false;

		/*
		 * If it's not one of the supported operators ("=", "<", ">", etc.),
		 * just ignore the clause, as it's not compatible with MCV lists.
		 *
		 * This uses the function for estimating selectivity, not the operator
		 * directly (a bit awkward, but well ...).
		 */
		if ((get_oprrest(expr->opno) != F_EQSEL) &&
			(get_oprrest(expr->opno) != F_SCALARLTSEL) &&
			(get_oprrest(expr->opno) != F_SCALARGTSEL))
			return false;

		var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);

		/* We only support plain Vars for now */
		if (!IsA(var, Var))
			return false;

		/* Ensure var is from the correct relation */
		if (var->varno != relid)
			return false;

		/* we also better ensure the Var is from the current level */
		if (var->varlevelsup > 0)
			return false;

		/* Also skip system attributes (we don't allow stats on those). */
		if (!AttrNumberIsForUserDefinedAttr(var->varattno))
			return false;

		*attnums = bms_add_member(*attnums, var->varattno);

		return true;
	}

	return false;
}


static int
mv_get_index(AttrNumber varattno, Bitmapset *keys)
{
	int	i, j;

	i = -1;
	j = 0;
	while (((i = bms_next_member(keys, i)) >= 0) && (i < varattno))
		j += 1;

	return j;
}

#define UPDATE_RESULT(m,r,isor) \
	(m) = (isor) ? (Max(m,r)) : (Min(m,r))

/*
 * Evaluate clauses using the MCV list, and update the match bitmap.
 *
 * The bitmap may be already partially set, so this is really a way to
 * combine results of several clause lists - either when computing
 * conditional probability P(A|B) or a combination of AND/OR clauses.
 *
 * TODO: This works with 'bitmap' where each bit is represented as a char,
 * which is slightly wasteful. Instead, we could use a regular
 * bitmap, reducing the size to ~1/8. Another thing is merging the
 * bitmaps using & and |, which might be faster than min/max.
 */
static int
mcv_update_match_bitmap(PlannerInfo *root, List *clauses,
						Bitmapset *keys, MCVList *mcvlist,
						int nmatches, char *matches,
						Selectivity *lowsel, bool *fullmatch,
						bool is_or)
{
	int			i;
	ListCell   *l;

	Bitmapset  *eqmatches = NULL;		/* attributes with equality matches */

	/* The bitmap may be partially built. */
	Assert(nmatches >= 0);
	Assert(nmatches <= mcvlist->nitems);
	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);
	Assert(mcvlist != NULL);
	Assert(mcvlist->nitems > 0);

	/* No possible matches (only works for AND-ded clauses) */
	if (((nmatches == 0) && (!is_or)) ||
		((nmatches == mcvlist->nitems) && is_or))
		return nmatches;

	/*
	 * find the lowest frequency in the MCV list
	 *
	 * We need to do that here, because we do various tricks in the following
	 * code - skipping items already ruled out, etc.
	 *
	 * XXX A loop is necessary because the MCV list is not sorted by
	 * frequency.
	 */
	*lowsel = 1.0;
	for (i = 0; i < mcvlist->nitems; i++)
	{
		MCVItem	   *item = mcvlist->items[i];

		if (item->frequency < *lowsel)
			*lowsel = item->frequency;
	}

	/*
	 * Loop through the list of clauses, and for each of them evaluate all the
	 * MCV items not yet eliminated by the preceding clauses.
	 */
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);

		/* if it's a RestrictInfo, then extract the clause */
		if (IsA(clause, RestrictInfo))
			clause = (Node *) ((RestrictInfo *) clause)->clause;

		/* if there are no remaining matches possible, we can stop */
		if (((nmatches == 0) && (!is_or)) ||
			((nmatches == mcvlist->nitems) && is_or))
			break;

		/* it's either OpClause, or NullTest */
		if (is_opclause(clause))
		{
			OpExpr	   *expr = (OpExpr *) clause;
			bool		varonleft = true;
			bool		ok;
			FmgrInfo	opproc;

			/* get procedure computing operator selectivity */
			RegProcedure oprrest = get_oprrest(expr->opno);

			fmgr_info(get_opcode(expr->opno), &opproc);

			ok = (NumRelids(clause) == 1) &&
				(is_pseudo_constant_clause(lsecond(expr->args)) ||
				 (varonleft = false,
				  is_pseudo_constant_clause(linitial(expr->args))));

			if (ok)
			{

				FmgrInfo	gtproc;
				Var		   *var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);
				Const	   *cst = (varonleft) ? lsecond(expr->args) : linitial(expr->args);
				bool		isgt = (!varonleft);

				TypeCacheEntry *typecache
				= lookup_type_cache(var->vartype, TYPECACHE_GT_OPR);

				/* FIXME proper matching attribute to dimension */
				int			idx = mv_get_index(var->varattno, keys);

				fmgr_info(get_opcode(typecache->gt_opr), &gtproc);

				/*
				 * Walk through the MCV items and evaluate the current clause.
				 * We can skip items that were already ruled out, and
				 * terminate if there are no remaining MCV items that might
				 * possibly match.
				 */
				for (i = 0; i < mcvlist->nitems; i++)
				{
					bool		mismatch = false;
					MCVItem	   *item = mcvlist->items[i];

					/*
					 * If there are no more matches (AND) or no remaining
					 * unmatched items (OR), we can stop processing this
					 * clause.
					 */
					if (((nmatches == 0) && (!is_or)) ||
						((nmatches == mcvlist->nitems) && is_or))
						break;

					/*
					 * For AND-lists, we can also mark NULL items as 'no
					 * match' (and then skip them). For OR-lists this is not
					 * possible.
					 */
					if ((!is_or) && item->isnull[idx])
						matches[i] = STATS_MATCH_NONE;

					/* skip MCV items that were already ruled out */
					if ((!is_or) && (matches[i] == STATS_MATCH_NONE))
						continue;
					else if (is_or && (matches[i] == STATS_MATCH_FULL))
						continue;

					switch (oprrest)
					{
						case F_EQSEL:

							/*
							 * We don't care about isgt in equality, because
							 * it does not matter whether it's (var = const)
							 * or (const = var).
							 */
							mismatch = !DatumGetBool(FunctionCall2Coll(&opproc,
													   DEFAULT_COLLATION_OID,
															 cst->constvalue,
														 item->values[idx]));

							if (!mismatch)
								eqmatches = bms_add_member(eqmatches, idx);

							break;

						case F_SCALARLTSEL:		/* column < constant */
						case F_SCALARGTSEL:		/* column > constant */

							/*
							 * First check whether the constant is below the
							 * lower boundary (in that case we can skip the
							 * bucket, because there's no overlap).
							 */
							if (isgt)
								mismatch = !DatumGetBool(FunctionCall2Coll(&opproc,
														   DEFAULT_COLLATION_OID,
															 cst->constvalue,
															item->values[idx]));
							else
								mismatch = !DatumGetBool(FunctionCall2Coll(&opproc,
														   DEFAULT_COLLATION_OID,
															 item->values[idx],
															  cst->constvalue));

							break;
					}

					/*
					 * XXX The conditions on matches[i] are not needed, as we
					 * skip MCV items that can't become true/false, depending
					 * on the current flag. See beginning of the loop over MCV
					 * items.
					 */

					if ((is_or) && (matches[i] == STATS_MATCH_NONE) && (!mismatch))
					{
						/* OR - was MATCH_NONE, but will be MATCH_FULL */
						matches[i] = STATS_MATCH_FULL;
						++nmatches;
						continue;
					}
					else if ((!is_or) && (matches[i] == STATS_MATCH_FULL) && mismatch)
					{
						/* AND - was MATC_FULL, but will be MATCH_NONE */
						matches[i] = STATS_MATCH_NONE;
						--nmatches;
						continue;
					}

				}
			}
		}
		else if (IsA(clause, NullTest))
		{
			NullTest   *expr = (NullTest *) clause;
			Var		   *var = (Var *) (expr->arg);

			/* FIXME proper matching attribute to dimension */
			int			idx = mv_get_index(var->varattno, keys);

			/*
			 * Walk through the MCV items and evaluate the current clause. We
			 * can skip items that were already ruled out, and terminate if
			 * there are no remaining MCV items that might possibly match.
			 */
			for (i = 0; i < mcvlist->nitems; i++)
			{
				MCVItem	   *item = mcvlist->items[i];

				/*
				 * if there are no more matches, we can stop processing this
				 * clause
				 */
				if (nmatches == 0)
					break;

				/* skip MCV items that were already ruled out */
				if (matches[i] == STATS_MATCH_NONE)
					continue;

				/* if the clause mismatches the MCV item, set it as MATCH_NONE */
				if (((expr->nulltesttype == IS_NULL) && (!item->isnull[idx])) ||
				((expr->nulltesttype == IS_NOT_NULL) && (item->isnull[idx])))
				{
					matches[i] = STATS_MATCH_NONE;
					--nmatches;
				}
			}
		}
		else if (or_clause(clause) || and_clause(clause))
		{
			/*
			 * AND/OR clause, with all clauses compatible with the selected MV
			 * stat
			 */

			int			i;
			BoolExpr   *orclause = ((BoolExpr *) clause);
			List	   *orclauses = orclause->args;

			/* match/mismatch bitmap for each MCV item */
			int			or_nmatches = 0;
			char	   *or_matches = NULL;

			Assert(orclauses != NIL);
			Assert(list_length(orclauses) >= 2);

			/* number of matching MCV items */
			or_nmatches = mcvlist->nitems;

			/* by default none of the MCV items matches the clauses */
			or_matches = palloc0(sizeof(char) * or_nmatches);

			if (or_clause(clause))
			{
				/* OR clauses assume nothing matches, initially */
				memset(or_matches, STATS_MATCH_NONE, sizeof(char) * or_nmatches);
				or_nmatches = 0;
			}
			else
			{
				/* AND clauses assume nothing matches, initially */
				memset(or_matches, STATS_MATCH_FULL, sizeof(char) * or_nmatches);
			}

			/* build the match bitmap for the OR-clauses */
			or_nmatches = mcv_update_match_bitmap(root, orclauses, keys,
										mcvlist, or_nmatches, or_matches,
										lowsel, fullmatch, or_clause(clause));

			/* merge the bitmap into the existing one */
			for (i = 0; i < mcvlist->nitems; i++)
			{
				/*
				 * Merge the result into the bitmap (Min for AND, Max for OR).
				 *
				 * FIXME this does not decrease the number of matches
				 */
				UPDATE_RESULT(matches[i], or_matches[i], is_or);
			}

			pfree(or_matches);

		}
		else
		{
			elog(ERROR, "unknown clause type: %d", clause->type);
		}
	}

	/*
	 * If all the columns were matched by equality, it's a full match. In this
	 * case there can be just a single MCV item, matching the clause (if there
	 * were two, both would match the other one).
	 */
	*fullmatch = (bms_num_members(eqmatches) == mcvlist->ndimensions);

	/* free the allocated pieces */
	if (eqmatches)
		pfree(eqmatches);

	return nmatches;
}


Selectivity
mcv_clauselist_selectivity(PlannerInfo *root, List *clauses, int varRelid,
						   JoinType jointype, SpecialJoinInfo *sjinfo,
						   RelOptInfo *rel, Bitmapset **estimatedclauses)
{
	int			i;
	ListCell   *l;
	Bitmapset  *clauses_attnums = NULL;
	Bitmapset **list_attnums;
	int			listidx;
	StatisticExtInfo *stat;
	MCVList	   *mcv;
	List	   *mcv_clauses;

	/* match/mismatch bitmap for each MCV item */
	char	   *matches = NULL;
	bool		fullmatch;
	Selectivity lowsel;
	int			nmatches = 0;
	Selectivity s, u;

	/* check if there's any stats that might be useful for us. */
	if (!has_stats_of_kind(rel->statlist, STATS_EXT_MCV))
		return 1.0;

	list_attnums = (Bitmapset **) palloc(sizeof(Bitmapset *) *
										 list_length(clauses));

	/*
	 * Pre-process the clauses list to extract the attnums seen in each item.
	 * We need to determine if there's any clauses which will be useful for
	 * dependency selectivity estimations. Along the way we'll record all of
	 * the attnums for each clause in a list which we'll reference later so we
	 * don't need to repeat the same work again. We'll also keep track of all
	 * attnums seen.
	 *
	 * FIXME Should skip already estimated clauses (using the estimatedclauses
	 * bitmap).
	 */
	listidx = 0;
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);
		Bitmapset  *attnums = NULL;

		if (mcv_is_compatible_clause(clause, rel->relid, &attnums))
		{
			list_attnums[listidx] = attnums;
			clauses_attnums = bms_join(clauses_attnums, attnums);
		}
		else
			list_attnums[listidx] = NULL;

		listidx++;
	}

	/* We need at least two attributes for MCV lists. */
	if (bms_num_members(clauses_attnums) < 2)
		return 1.0;

	/* find the best suited statistics object for these attnums */
	stat = choose_best_statistics(rel->statlist, clauses_attnums,
								  STATS_EXT_MCV);

	/* if no matching stats could be found then we've nothing to do */
	if (!stat)
		return 1.0;

	/* load the MCV list stored in the statistics object */
	mcv = statext_mcv_load(stat->statOid);

	/* now filter the clauses to be estimated using the selected MCV */
	mcv_clauses = NIL;

	listidx = 0;
	foreach (l, clauses)
	{
		/*
		 * If the clause is compatible with the selected MCV statistics,
		 * mark it as estimated and add it to the MCV list.
		 */
		if ((list_attnums[listidx] == NULL) &&
			(bms_is_subset(list_attnums[listidx], stat->keys)))
		{
			mcv_clauses = lappend(mcv_clauses, (Node *)lfirst(l));
			*estimatedclauses = bms_add_member(*estimatedclauses, listidx);
		}

		listidx++;
	}

	/**/

	/* by default all the MCV items match the clauses fully */
	matches = palloc0(sizeof(char) * mcv->nitems);
	memset(matches, STATS_MATCH_FULL, sizeof(char) * mcv->nitems);

	/* number of matching MCV items */
	nmatches = mcv->nitems;

	nmatches = mcv_update_match_bitmap(root, clauses,
									   stat->keys, mcv,
									   nmatches, matches,
									   &lowsel, &fullmatch, false);

	/* sum frequencies for all the matching MCV items */
	for (i = 0; i < mcv->nitems; i++)
	{
		/* used to 'scale' for MCV lists not covering all tuples */
		u += mcv->items[i]->frequency;

		if (matches[i] != STATS_MATCH_NONE)
			s += mcv->items[i]->frequency;
	}

	return s * u;
}
