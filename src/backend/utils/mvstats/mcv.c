/*-------------------------------------------------------------------------
 *
 * mcv.c
 *	  POSTGRES multivariate MCV lists
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/mvstats/mcv.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"

#include "utils/lsyscache.h"

#include "common.h"

/*
 * Each serialized item needs to store (in this order):
 *
 * - indexes              (ndim * sizeof(int32))
 * - null flags           (ndim * sizeof(bool))
 * - frequency            (sizeof(double))
 *
 * So in total:
 *
 *   ndim * (sizeof(int32) + sizeof(bool)) + sizeof(double)
 */
#define ITEM_SIZE(ndims)	\
	(ndims * (sizeof(uint16) + sizeof(bool)) + sizeof(double))

/* pointers into a flat serialized item of ITEM_SIZE(n) bytes */
#define ITEM_INDEXES(item)			((uint16*)item)
#define ITEM_NULLS(item,ndims)		((bool*)(ITEM_INDEXES(item) + ndims))
#define ITEM_FREQUENCY(item,ndims)	((double*)(ITEM_NULLS(item,ndims) + ndims))

/*
 * Builds MCV list from sample rows, and removes rows represented by
 * the MCV list from the sample (the number of remaining sample rows is
 * returned by the numrows_filtered parameter).
 *
 * The method is quite simple - in short it does about these steps:
 *
 *       (1) sort the data (default collation, '<' for the data type)
 *
 *       (2) count distinct groups, decide how many to keep
 *
 *       (3) build the MCV list using the threshold determined in (2)
 *
 *       (4) remove rows represented by the MCV from the sample
 *
 * For more details, see the comments in the code.
 *
 * FIXME Use max_mcv_items from ALTER TABLE ADD STATISTICS command.
 *
 * FIXME Single-dimensional MCV is sorted by frequency (descending). We
 *       should do that too, because when walking through the list we
 *       want to check the most frequent items first.
 *
 * TODO We're using Datum (8B), even for data types (e.g. int4 or
 *      float4). Maybe we could save some space here, but the bytea
 *      compression should handle it just fine.
 *
 * TODO This probably should not use the ndistinct directly (as computed
 *      from the table, but rather estimate the number of distinct
 *      values in the table), no?
 */
MCVList
build_mv_mcvlist(int numrows, HeapTuple *rows, int2vector *attrs,
					  VacAttrStats **stats, int *numrows_filtered)
{
	int i, j;
	int numattrs = attrs->dim1;
	int ndistinct = 0;
	int mcv_threshold = 0;
	int count = 0;
	int nitems = 0;

	MCVList	mcvlist = NULL;

	/* Sort by multiple columns (using array of SortSupport) */
	MultiSortSupport mss = multi_sort_init(numattrs);

	/*
	 * Preallocate space for all the items as a single chunk, and point
	 * the items to the appropriate parts of the array.
	 */
	SortItem   *items  = (SortItem*)palloc0(numrows * sizeof(SortItem));
	Datum	   *values = (Datum*)palloc0(sizeof(Datum) * numrows * numattrs);
	bool	   *isnull = (bool*)palloc0(sizeof(bool) * numrows * numattrs);

	/* keep all the rows by default (as if there was no MCV list) */
	*numrows_filtered = numrows;

	for (i = 0; i < numrows; i++)
	{
		items[i].values = &values[i * numattrs];
		items[i].isnull = &isnull[i * numattrs];
	}

	/* load the values/null flags from sample rows */
	for (j = 0; j < numrows; j++)
		for (i = 0; i < numattrs; i++)
			items[j].values[i] = heap_getattr(rows[j], attrs->values[i],
								stats[i]->tupDesc, &items[j].isnull[i]);

	/* prepare the sort functions for all the attributes */
	for (i = 0; i < numattrs; i++)
		multi_sort_add_dimension(mss, i, i, stats);

	/* do the sort, using the multi-sort */
	qsort_arg((void *) items, numrows, sizeof(SortItem),
				multi_sort_compare, mss);

	/*
	 * Count the number of distinct groups - just walk through the
	 * sorted list and count the number of key changes. We use this to
	 * determine the threshold (125% of the average frequency).
	 */
	ndistinct = 1;
	for (i = 1; i < numrows; i++)
		if (multi_sort_compare(&items[i], &items[i-1], mss) != 0)
			ndistinct += 1;

	/*
	 * Determine how many groups actually exceed the threshold, and then
	 * walk the array again and collect them into an array. We'll always
	 * require at least 4 rows per group.
	 *
	 * But if we can fit all the distinct values in the MCV list (i.e.
	 * if there are less distinct groups than MVSTAT_MCVLIST_MAX_ITEMS),
	 * we'll require only 2 rows per group.
	 *
	 * TODO For now the threshold is the same as in the single-column
	 *      case (average + 25%), but maybe that's worth revisiting
	 *      for the multivariate case.
	 *
	 * TODO We can do this only if we believe we got all the distinct
	 *      values of the table.
	 *
	 * FIXME This should really reference mcv_max_items (from catalog)
	 *       instead of the constant MVSTAT_MCVLIST_MAX_ITEMS.
	 */
	mcv_threshold = 1.25 * numrows / ndistinct;
	mcv_threshold = (mcv_threshold < 4) ? 4  : mcv_threshold;

	if (ndistinct <= MVSTAT_MCVLIST_MAX_ITEMS)
		mcv_threshold = 2;

	/*
	 * Walk through the sorted data again, and see how many groups
	 * reach the mcv_threshold (and become an item in the MCV list).
	 */
	count = 1;
	for (i = 1; i <= numrows; i++)
	{
		/* last row or new group, so check if we exceed  mcv_threshold */
		if ((i == numrows) || (multi_sort_compare(&items[i], &items[i-1], mss) != 0))
		{
			/* group hits the threshold, count the group as MCV item */
			if (count >= mcv_threshold)
				nitems += 1;

			count = 1;
		}
		else	/* within group, so increase the number of items */
			count += 1;
	}

	/* we know the number of MCV list items, so let's build the list */
	if (nitems > 0)
	{
		/* allocate the MCV list structure, set parameters we know */
		mcvlist = (MCVList)palloc0(sizeof(MCVListData));

		mcvlist->magic = MVSTAT_MCV_MAGIC;
		mcvlist->type = MVSTAT_MCV_TYPE_BASIC;
		mcvlist->ndimensions = numattrs;
		mcvlist->nitems = nitems;

		/*
		 * Preallocate Datum/isnull arrays (not as a single chunk, as
		 * we'll pass this outside this method and thus it needs to be
		 * easy to pfree() the data (and we wouldn't know where the
		 * arrays start).
		 *
		 * TODO Maybe the reasoning that we can't allocate a single
		 *      piece because we're passing it out is bogus? Who'd
		 *      free a single item of the MCV list, anyway?
		 *
		 * TODO Maybe with a proper encoding (stuffing all the values
		 *      into a list-level array, this will be untrue)?
		 */
		mcvlist->items = (MCVItem*)palloc0(sizeof(MCVItem)*nitems);

		for (i = 0; i < nitems; i++)
		{
			mcvlist->items[i] = (MCVItem)palloc0(sizeof(MCVItemData));
			mcvlist->items[i]->values = (Datum*)palloc0(sizeof(Datum)*numattrs);
			mcvlist->items[i]->isnull = (bool*)palloc0(sizeof(bool)*numattrs);
		}

		/*
		 * Repeat the same loop as above, but this time copy the data
		 * into the MCV list (for items exceeding the threshold).
		 *
		 * TODO Maybe we could simply remember indexes of the last item
		 *      in each group (from the previous loop)?
		 */
		count = 1;
		nitems = 0;
		for (i = 1; i <= numrows; i++)
		{
			/* last row or a new group */
			if ((i == numrows) || (multi_sort_compare(&items[i], &items[i-1], mss) != 0))
			{
				/* count the MCV item if exceeding the threshold (and copy into the array) */
				if (count >= mcv_threshold)
				{
					/* just pointer to the proper place in the list */
					MCVItem item = mcvlist->items[nitems];

					/* copy values from the _previous_ group (last item of) */
					memcpy(item->values, items[(i-1)].values, sizeof(Datum) * numattrs);
					memcpy(item->isnull, items[(i-1)].isnull, sizeof(bool)  * numattrs);


					/* and finally the group frequency */
					item->frequency = (double)count / numrows;

					/* next item */
					nitems += 1;
				}

				count = 1;
			}
			else	/* same group, just increase the number of items */
				count += 1;
		}

		/* make sure the loops are consistent */
		Assert(nitems == mcvlist->nitems);

		/*
		 * Remove the rows matching the MCV list (i.e. keep only rows
		 * that are not represented by the MCV list).
		 *
		 * FIXME This implementation is rather naive, effectively O(N^2).
		 *       As the MCV list grows, the check will take longer and
		 *       longer. And as the number of sampled rows increases (by
		 *       increasing statistics target), it will take longer and
		 *       longer. One option is to sort the MCV items first and
		 *       then perform a binary search.
		 *
		 *       A better option would be keeping the ID of the row in
		 *       the sort item, and then just walk through the items and
		 *       mark rows to remove (in a bitmap of the same size).
		 *       There's not space for that in SortItem at this moment,
		 *       but it's trivial to add 'private' pointer, or just
		 *       using another structure with extra field (starting with
		 *       SortItem, so that the comparators etc. still work).
		 *
		 *       Another option is to use the sorted array of items
		 *       (because that's how we sorted the source data), and
		 *       simply do a bsearch() into it. If we find a matching
		 *       item, the row belongs to the MCV list.
		 */
		if (nitems == ndistinct) /* all rows are covered by MCV items */
			*numrows_filtered = 0;
		else /* (nitems < ndistinct) && (nitems > 0) */
		{
			int nfiltered = 0;
			HeapTuple *rows_filtered = (HeapTuple*)palloc0(sizeof(HeapTuple) * numrows);

			/* used for the searches */
			SortItem item, mcvitem;;

			item.values = (Datum*)palloc0(numattrs * sizeof(Datum));
			item.isnull = (bool*)palloc0(numattrs * sizeof(bool));

			/*
			 * FIXME we don't need to allocate this, we can reference
			 *       the MCV item directly ...
			 */
			mcvitem.values = (Datum*)palloc0(numattrs * sizeof(Datum));
			mcvitem.isnull = (bool*)palloc0(numattrs * sizeof(bool));

			/* walk through the tuples, compare the values to MCV items */
			for (i = 0; i < numrows; i++)
			{
				bool	match = false;

				/* collect the key values from the row */
				for (j = 0; j < numattrs; j++)
					item.values[j] = heap_getattr(rows[i], attrs->values[j],
										stats[j]->tupDesc, &item.isnull[j]);

				/* scan through the MCV list for matches */
				for (j = 0; j < mcvlist->nitems; j++)
				{
					/*
					 * TODO Create a SortItem/MCVItem comparator so that
					 *      we don't need to do memcpy() like crazy.
					 */
					memcpy(mcvitem.values, mcvlist->items[j]->values,
							numattrs * sizeof(Datum));
					memcpy(mcvitem.isnull, mcvlist->items[j]->isnull,
							numattrs * sizeof(bool));

					if (multi_sort_compare(&item, &mcvitem, mss) == 0)
					{
						match = true;
						break;
					}
				}

				/* if no match in the MCV list, copy the row into the filtered ones */
				if (! match)
					memcpy(&rows_filtered[nfiltered++], &rows[i], sizeof(HeapTuple));
			}

			/* replace the rows and remember how many rows we kept */
			memcpy(rows, rows_filtered, sizeof(HeapTuple) * nfiltered);
			*numrows_filtered = nfiltered;

			/* free all the data used here */
			pfree(rows_filtered);
			pfree(item.values);
			pfree(item.isnull);
			pfree(mcvitem.values);
			pfree(mcvitem.isnull);
		}
	}

	pfree(values);
	pfree(items);
	pfree(isnull);

	return mcvlist;
}


/* fetch the MCV list (as a bytea) from the pg_mv_statistic catalog */
MCVList
load_mv_mcvlist(Oid mvoid)
{
	bool		isnull = false;
	Datum		mcvlist;

#ifdef USE_ASSERT_CHECKING
	Form_pg_mv_statistic	mvstat;
#endif

	/* Prepare to scan pg_mv_statistic for entries having indrelid = this rel. */
	HeapTuple	htup = SearchSysCache1(MVSTATOID, ObjectIdGetDatum(mvoid));

	if (! HeapTupleIsValid(htup))
		return NULL;

#ifdef USE_ASSERT_CHECKING
	mvstat = (Form_pg_mv_statistic) GETSTRUCT(htup);
	Assert(mvstat->mcv_enabled && mvstat->mcv_built);
#endif

	mcvlist = SysCacheGetAttr(MVSTATOID, htup,
						   Anum_pg_mv_statistic_stamcv, &isnull);

	Assert(!isnull);

	ReleaseSysCache(htup);

	return deserialize_mv_mcvlist(DatumGetByteaP(mcvlist));
}

/* print some basic info about the MCV list
 *
 * TODO Add info about what part of the table this covers.
 */
Datum
pg_mv_stats_mcvlist_info(PG_FUNCTION_ARGS)
{
	bytea	   *data = PG_GETARG_BYTEA_P(0);
	char	   *result;

	MCVList	mcvlist = deserialize_mv_mcvlist(data);

	result = palloc0(128);
	snprintf(result, 128, "nitems=%d", mcvlist->nitems);

	pfree(mcvlist);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/* used to pass context into bsearch() */
static SortSupport ssup_private = NULL;

static int bsearch_comparator(const void * a, const void * b);

/*
 * Serialize MCV list into a bytea value. The basic algorithm is simple:
 *
 * (1) perform deduplication for each attribute (separately)
 *     (a) collect all (non-NULL) attribute values from all MCV items
 *     (b) sort the data (using 'lt' from VacAttrStats)
 *     (c) remove duplicate values from the array
 *
 * (2) serialize the arrays into a bytea value
 *
 * (3) process all MCV list items
 *     (a) replace values with indexes into the arrays
 *
 * Each attribute has to be processed separately, because we're mixing
 * different datatypes, and we don't know what equality means for them.
 * We're also mixing pass-by-value and pass-by-ref types, and so on.
 *
 * We'll use uint16 values for the indexes in step (3), as we don't
 * allow more than 8k MCV items (see list max_mcv_items). We might
 * increase this to 65k and still fit into uint16.
 *
 * We don't really expect the high compression as with histograms,
 * because we're not doing any bucket splits etc. (which is the source
 * of high redundancy there), but we need to do it anyway as we need
 * to serialize varlena values etc. We might invent another way to
 * serialize MCV lists, but let's keep it consistent.
 *
 * FIXME This probably leaks memory, or at least uses it inefficiently
 *       (many small palloc() calls instead of a large one).
 *
 * TODO Consider using 16-bit values for the indexes in step (3).
 *
 * TODO Consider packing boolean flags (NULL) for each item into 'char'
 *      or a longer type (instead of using an array of bool items).
 */
bytea *
serialize_mv_mcvlist(MCVList mcvlist, int2vector *attrs,
					 VacAttrStats **stats)
{
	int	i, j;
	int	ndims = mcvlist->ndimensions;
	int	itemsize = ITEM_SIZE(ndims);

	Size	total_length = 0;

	char   *item = palloc0(itemsize);

	/* serialized items (indexes into arrays, etc.) */
	bytea  *output;
	char   *data = NULL;

	/* values per dimension (and number of non-NULL values) */
	Datum **values = (Datum**)palloc0(sizeof(Datum*) * ndims);
	int	   *counts = (int*)palloc0(sizeof(int) * ndims);

	/* info about dimensions (for deserialize) */
	DimensionInfo * info
				= (DimensionInfo *)palloc0(sizeof(DimensionInfo)*ndims);

	/* sort support data */
	SortSupport	ssup = (SortSupport)palloc0(sizeof(SortSupportData)*ndims);

	/* collect and deduplicate values for each dimension */
	for (i = 0; i < ndims; i++)
	{
		int count;
		StdAnalyzeData *tmp = (StdAnalyzeData *)stats[i]->extra_data;

		/* keep important info about the data type */
		info[i].typlen   = stats[i]->attrtype->typlen;
		info[i].typbyval = stats[i]->attrtype->typbyval;

		/* allocate space for all values, including NULLs (won't use them) */
		values[i] = (Datum*)palloc0(sizeof(Datum) * mcvlist->nitems);

		for (j = 0; j < mcvlist->nitems; j++)
		{
			if (! mcvlist->items[j]->isnull[i])	/* skip NULL values */
			{
				values[i][counts[i]] = mcvlist->items[j]->values[i];
				counts[i] += 1;
			}
		}

		/* there are just NULL values in this dimension */
		if (counts[i] == 0)
			continue;

		/* sort and deduplicate */
		ssup[i].ssup_cxt = CurrentMemoryContext;
		ssup[i].ssup_collation = DEFAULT_COLLATION_OID;
		ssup[i].ssup_nulls_first = false;

		PrepareSortSupportFromOrderingOp(tmp->ltopr, &ssup[i]);

		qsort_arg(values[i], counts[i], sizeof(Datum),
				  compare_scalars_simple, &ssup[i]);

		/*
		 * Walk through the array and eliminate duplicitate values, but
		 * keep the ordering (so that we can do bsearch later). We know
		 * there's at least 1 item, so we can skip the first element.
		 */
		count = 1;	/* number of deduplicated items */
		for (j = 1; j < counts[i]; j++)
		{
			/* if it's different from the previous value, we need to keep it */
			if (compare_datums_simple(values[i][j-1], values[i][j], &ssup[i]) != 0)
			{
				/* XXX: not needed if (count == j) */
				values[i][count] = values[i][j];
				count += 1;
			}
		}

		/* do not exceed UINT16_MAX */
		Assert(count <= UINT16_MAX);

		/* keep info about the deduplicated count */
		info[i].nvalues = count;

		/* compute size of the serialized data */
		if (info[i].typbyval || (info[i].typlen > 0))
			/* by value pased by reference, but fixed length */
			info[i].nbytes = info[i].nvalues * info[i].typlen;
		else if (info[i].typlen == -1)
			/* varlena, so just use VARSIZE_ANY */
			for (j = 0; j < info[i].nvalues; j++)
				info[i].nbytes += VARSIZE_ANY(values[i][j]);
		else if (info[i].typlen == -2)
			/* cstring, so simply strlen */
			for (j = 0; j < info[i].nvalues; j++)
				info[i].nbytes += strlen(DatumGetPointer(values[i][j]));
		else
			elog(ERROR, "unknown data type typbyval=%d typlen=%d",
				info[i].typbyval, info[i].typlen);
	}

	/*
	 * Now we finally know how much space we'll need for the serialized
	 * MCV list, as it contains these fields:
	 *
	 * - length (4B) for varlena
	 * - magic (4B)
	 * - type (4B)
	 * - ndimensions (4B)
	 * - nitems (4B)
	 * - info (ndim * sizeof(DimensionInfo)
	 * - arrays of values for each dimension
	 * - serialized items (nitems * itemsize)
	 *
	 * So the 'header' size is 20B + ndim * sizeof(DimensionInfo) and
	 * then we'll place the data.
	 */
	total_length = (sizeof(int32) + offsetof(MCVListData, items)
					+ ndims * sizeof(DimensionInfo)
					+ mcvlist->nitems * itemsize);

	for (i = 0; i < ndims; i++)
		total_length += info[i].nbytes;

	/* enforce arbitrary limit of 1MB */
	if (total_length > 1024 * 1024)
		elog(ERROR, "serialized MCV exceeds 1MB (%ld)", total_length);

	/* allocate space for the serialized MCV list, set header fields */
	output = (bytea*)palloc0(total_length);
	SET_VARSIZE(output, total_length);

	/* we'll use 'ptr' to keep track of the place to write data */
	data = VARDATA(output);

	memcpy(data, mcvlist, offsetof(MCVListData, items));
	data += offsetof(MCVListData, items);

	memcpy(data, info, sizeof(DimensionInfo) * ndims);
	data += sizeof(DimensionInfo) * ndims;

	/* value array for each dimension */
	for (i = 0; i < ndims; i++)
	{
#ifdef USE_ASSERT_CHECKING
		char *tmp = data;
#endif
		for (j = 0; j < info[i].nvalues; j++)
		{
			if (info[i].typbyval)
			{
				/* passed by value / Datum */
				memcpy(data, &values[i][j], info[i].typlen);
				data += info[i].typlen;
			}
			else if (info[i].typlen > 0)
			{
				/* pased by reference, but fixed length (name, tid, ...) */
				memcpy(data, &values[i][j], info[i].typlen);
				data += info[i].typlen;
			}
			else if (info[i].typlen == -1)
			{
				/* varlena */
				memcpy(data, DatumGetPointer(values[i][j]),
							VARSIZE_ANY(values[i][j]));
				data += VARSIZE_ANY(values[i][j]);
			}
			else if (info[i].typlen == -2)
			{
				/* cstring (don't forget the \0 terminator!) */
				memcpy(data, DatumGetPointer(values[i][j]),
							strlen(DatumGetPointer(values[i][j])) + 1);
				data += strlen(DatumGetPointer(values[i][j])) + 1;
			}
		}
		Assert((data - tmp) == info[i].nbytes);
	}

	/* and finally, the MCV items */
	for (i = 0; i < mcvlist->nitems; i++)
	{
		/* don't write beyond the allocated space */
		Assert(data <= (char*)output + total_length - itemsize);

		/* reset the values for each item */
		memset(item, 0, itemsize);

		for (j = 0; j < ndims; j++)
		{
			/* do the lookup only for non-NULL values */
			if (! mcvlist->items[i]->isnull[j])
			{
				Datum * v = NULL;
				ssup_private = &ssup[j];

				v = (Datum*)bsearch(&mcvlist->items[i]->values[j],
								values[j], info[j].nvalues, sizeof(Datum),
								bsearch_comparator);

				if (v == NULL)
					elog(ERROR, "value for dim %d not found in array", j);

				/* compute index within the array */
				ITEM_INDEXES(item)[j] = (v - values[j]);

				/* check the index is within expected bounds */
				Assert(ITEM_INDEXES(item)[j] >= 0);
				Assert(ITEM_INDEXES(item)[j] < info[j].nvalues);
			}
		}

		/* copy NULL and frequency flags into the item */
		memcpy(ITEM_NULLS(item, ndims),
				mcvlist->items[i]->isnull, sizeof(bool) * ndims);
		memcpy(ITEM_FREQUENCY(item, ndims),
				&mcvlist->items[i]->frequency, sizeof(double));

		/* copy the item into the array */
		memcpy(data, item, itemsize);

		data += itemsize;
	}

	/* at this point we expect to match the total_length exactly */
	Assert((data - (char*)output) == total_length);

	return output;
}

/*
 * Inverse to serialize_mv_mcvlist() - see the comment there.
 *
 * We'll do full deserialization, because we don't really expect high
 * duplication of values so the caching may not be as efficient as with
 * histograms.
 */
MCVList deserialize_mv_mcvlist(bytea * data)
{
	int		i, j;
	Size	expected_size;
	MCVList mcvlist;
	char   *tmp;

	int		ndims, nitems, itemsize;
	DimensionInfo *info = NULL;

	uint16 *indexes = NULL;
	Datum **values = NULL;

	/* local allocation buffer (used only for deserialization) */
	int		bufflen;
	char   *buff;
	char   *ptr;

	/* buffer used for the result */
	int		rbufflen;
	char   *rbuff;
	char   *rptr;

	if (data == NULL)
		return NULL;

	if (VARSIZE_ANY_EXHDR(data) < offsetof(MCVListData,items))
		elog(ERROR, "invalid MCV Size %ld (expected at least %ld)",
			 VARSIZE_ANY_EXHDR(data), offsetof(MCVListData,items));

	/* read the MCV list header */
	mcvlist = (MCVList)palloc0(sizeof(MCVListData));

	/* initialize pointer to the data part (skip the varlena header) */
	tmp = VARDATA(data);

	/* get the header and perform basic sanity checks */
	memcpy(mcvlist, tmp, offsetof(MCVListData,items));
	tmp += offsetof(MCVListData,items);

	if (mcvlist->magic != MVSTAT_MCV_MAGIC)
		elog(ERROR, "invalid MCV magic %d (expected %dd)",
			 mcvlist->magic, MVSTAT_MCV_MAGIC);

	if (mcvlist->type != MVSTAT_MCV_TYPE_BASIC)
		elog(ERROR, "invalid MCV type %d (expected %dd)",
			 mcvlist->type, MVSTAT_MCV_TYPE_BASIC);

	nitems = mcvlist->nitems;
	ndims = mcvlist->ndimensions;
	itemsize = ITEM_SIZE(ndims);

	Assert(nitems > 0);
	Assert((ndims >= 2) && (ndims <= MVSTATS_MAX_DIMENSIONS));

	/*
	 * What size do we expect with those parameters (it's incomplete,
	 * as we yet have to count the array sizes (from DimensionInfo
	 * records).
	 */
	expected_size = offsetof(MCVListData,items) +
					ndims * sizeof(DimensionInfo) +
					(nitems * itemsize);

	/* check that we have at least the DimensionInfo records */
	if (VARSIZE_ANY_EXHDR(data) < expected_size)
		elog(ERROR, "invalid MCV Size %ld (expected %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	info = (DimensionInfo*)(tmp);
	tmp += ndims * sizeof(DimensionInfo);

	/* account for the value arrays */
	for (i = 0; i < ndims; i++)
		expected_size += info[i].nbytes;

	if (VARSIZE_ANY_EXHDR(data) != expected_size)
		elog(ERROR, "invalid MCV Size %ld (expected %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	/* looks OK - not corrupted or something */

	/*
	 * We'll allocate one large chunk of memory for the intermediate
	 * data, needed only for deserializing the MCV list, and we'll pack
	 * use a local dense allocation to minimize the palloc overhead.
	 *
	 * Let's see how much space we'll actually need, and also include
	 * space for the array with pointers.
	 */
	bufflen = sizeof(Datum*) * ndims;			/* space for pointers */

	for (i = 0; i < ndims; i++)
		/* for full-size byval types, we reuse the serialized value */
		if (! (info[i].typbyval && info[i].typlen == sizeof(Datum)))
			bufflen += (sizeof(Datum) * info[i].nvalues);

	buff = palloc0(bufflen);
	ptr  = buff;

	values = (Datum**)buff;
	ptr += (sizeof(Datum*) * ndims);

	/*
	 * FIXME This uses pointers to the original data array (the types
	 *       not passed by value), so when someone frees the memory,
	 *       e.g. by doing something like this:
	 *
	 *           bytea * data = ... fetch the data from catalog ...
	 *           MCVList mcvlist = deserialize_mcv_list(data);
	 *           pfree(data);
	 *
	 *       then 'mcvlist' references the freed memory. This needs to
	 *       copy the pieces.
	 */
	for (i = 0; i < ndims; i++)
	{
		if (info[i].typbyval)
		{
			/* passed by value / Datum - simply reuse the array */
			if (info[i].typlen == sizeof(Datum))
			{
				values[i] = (Datum*)tmp;
				tmp += info[i].nbytes;
			}
			else
			{
				values[i] = (Datum*)ptr;
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
			/* all the varlena data need a chunk from the buffer */
			values[i] = (Datum*)ptr;
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
					tmp += (strlen(tmp) + 1); /* don't forget the \0 */
				}
			}
		}
	}

	/* we should exhaust the buffer exactly */
	Assert((ptr - buff) == bufflen);

	/* allocate space for the MCV items in a single piece */
	rbufflen = (sizeof(MCVItem) + sizeof(MCVItemData) +
				sizeof(Datum)*ndims + sizeof(bool)*ndims) * nitems;

	rbuff = palloc(rbufflen);
	rptr  = rbuff;

	mcvlist->items = (MCVItem*)rbuff;
	rptr += (sizeof(MCVItem) * nitems);

	for (i = 0; i < nitems; i++)
	{
		MCVItem item = (MCVItem)rptr;
		rptr += (sizeof(MCVItemData));

		item->values = (Datum*)rptr;
		rptr += (sizeof(Datum)*ndims);

		item->isnull = (bool*)rptr;
		rptr += (sizeof(bool) *ndims);

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
			if (! item->isnull[j])
				item->values[j] = values[j][indexes[j]];

		mcvlist->items[i] = item;

		tmp += ITEM_SIZE(ndims);

		Assert(tmp <= (char*)data + VARSIZE_ANY(data));
	}

	/* check that we processed all the data */
	Assert(tmp == (char*)data + VARSIZE_ANY(data));

	/* release the temporary buffer */
	pfree(buff);

	return mcvlist;
}

/*
 * We need to pass the SortSupport to the comparator, but bsearch()
 * has no 'context' parameter, so we use a global variable (ugly).
 */
static int
bsearch_comparator(const void * a, const void * b)
{
	Assert(ssup_private != NULL);
	return compare_scalars_simple(a, b, (void*)ssup_private);
}
/*
 * SRF with details about buckets of a histogram:
 *
 * - item ID (0...nitems)
 * - values (string array)
 * - nulls only (boolean array)
 * - frequency (double precision)
 *
 * The input is the OID of the statistics, and there are no rows
 * returned if the statistics contains no histogram.
 */
PG_FUNCTION_INFO_V1(pg_mv_mcv_items);

Datum
pg_mv_mcv_items(PG_FUNCTION_ARGS)
{
	FuncCallContext	   *funcctx;
	int					call_cntr;
	int					max_calls;
	TupleDesc			tupdesc;
	AttInMetadata	   *attinmeta;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldcontext;
		MCVList			mcvlist;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		mcvlist = load_mv_mcvlist(PG_GETARG_OID(0));

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

		/*
		 * generate attribute metadata needed later to produce tuples
		 * from raw C strings
		 */
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls)    /* do when there is more left to send */
	{
		char	  **values;
		HeapTuple	tuple;
		Datum		result;
		int2vector *stakeys;
		Oid			relid;

		char *buff = palloc0(1024);
		char *format;

		int			i;

		Oid		   *outfuncs;
		FmgrInfo   *fmgrinfo;

		MCVList		mcvlist;
		MCVItem		item;

		mcvlist = (MCVList)funcctx->user_fctx;

		Assert(call_cntr < mcvlist->nitems);

		item = mcvlist->items[call_cntr];

		stakeys = find_mv_attnums(PG_GETARG_OID(0), &relid);

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(4 * sizeof(char *));

		values[0] = (char *) palloc(64 * sizeof(char));

		/* arrays */
		values[1] = (char *) palloc0(1024 * sizeof(char));
		values[2] = (char *) palloc0(1024 * sizeof(char));

		/* frequency */
		values[3] = (char *) palloc(64 * sizeof(char));

		outfuncs = (Oid*)palloc0(sizeof(Oid) * mcvlist->ndimensions);
		fmgrinfo = (FmgrInfo*)palloc0(sizeof(FmgrInfo) * mcvlist->ndimensions);

		for (i = 0; i < mcvlist->ndimensions; i++)
		{
			bool isvarlena;

			getTypeOutputInfo(get_atttype(relid, stakeys->values[i]),
							  &outfuncs[i], &isvarlena);

			fmgr_info(outfuncs[i], &fmgrinfo[i]);
		}

		snprintf(values[0], 64, "%d", call_cntr);	/* item ID */

		for (i = 0; i < mcvlist->ndimensions; i++)
		{
			Datum val, valout;

			format = "%s, %s";
			if (i == 0)
				format = "{%s%s";
			else if (i == mcvlist->ndimensions-1)
				format = "%s, %s}";

			val = item->values[i];
			valout = FunctionCall1(&fmgrinfo[i], val);

			snprintf(buff, 1024, format, values[1], DatumGetPointer(valout));
			strncpy(values[1], buff, 1023);
			buff[0] = '\0';

			snprintf(buff, 1024, format, values[2], item->isnull[i] ? "t" : "f");
			strncpy(values[2], buff, 1023);
			buff[0] = '\0';
		}

		snprintf(values[3], 64, "%f", item->frequency);	/* frequency */

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
	else    /* do when there is no more left */
	{
		SRF_RETURN_DONE(funcctx);
	}
}
