/*-------------------------------------------------------------------------
 *
 * histogram.c
 *	  POSTGRES multivariate histograms
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/mvstats/histogram.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"

#include "utils/lsyscache.h"

#include "common.h"
#include <math.h>


static MVBucket create_initial_mv_bucket(int numrows, HeapTuple *rows,
										 int2vector *attrs,
										 VacAttrStats **stats);

static MVBucket select_bucket_to_partition(int nbuckets, MVBucket * buckets);

static MVBucket partition_bucket(MVBucket bucket, int2vector *attrs,
								 VacAttrStats **stats,
								 int *ndistvalues, Datum **distvalues);

static MVBucket copy_mv_bucket(MVBucket bucket, uint32 ndimensions);

static void update_bucket_ndistinct(MVBucket bucket, int2vector *attrs,
									VacAttrStats ** stats);

static void update_dimension_ndistinct(MVBucket bucket, int dimension,
									   int2vector *attrs,
									   VacAttrStats ** stats,
									   bool update_boundaries);

static void create_null_buckets(MVHistogram histogram, int bucket_idx,
								int2vector *attrs, VacAttrStats ** stats);

static Datum * build_ndistinct(int numrows, HeapTuple *rows, int2vector *attrs,
							   VacAttrStats **stats, int i, int *nvals);

/*
 * Each serialized bucket needs to store (in this order):
 *
 * - number of tuples     (float)
 * - number of distinct   (float)
 * - min inclusive flags  (ndim * sizeof(bool))
 * - max inclusive flags  (ndim * sizeof(bool))
 * - null dimension flags (ndim * sizeof(bool))
 * - min boundary indexes (2 * ndim * sizeof(uint16))
 * - max boundary indexes (2 * ndim * sizeof(uint16))
 *
 * So in total:
 *
 *   ndim * (4 * sizeof(uint16) + 3 * sizeof(bool)) + (2 * sizeof(float))
 */
#define BUCKET_SIZE(ndims)	\
	(ndims * (4 * sizeof(uint16) + 3 * sizeof(bool)) + sizeof(float))

/* pointers into a flat serialized bucket of BUCKET_SIZE(n) bytes */
#define BUCKET_NTUPLES(b)		(*(float*)b)
#define BUCKET_MIN_INCL(b,n)	((bool*)(b + sizeof(float)))
#define BUCKET_MAX_INCL(b,n)	(BUCKET_MIN_INCL(b,n) + n)
#define BUCKET_NULLS_ONLY(b,n)	(BUCKET_MAX_INCL(b,n) + n)
#define BUCKET_MIN_INDEXES(b,n)	((uint16*)(BUCKET_NULLS_ONLY(b,n) + n))
#define BUCKET_MAX_INDEXES(b,n)	((BUCKET_MIN_INDEXES(b,n) + n))

/* can't split bucket with less than 10 rows */
#define MIN_BUCKET_ROWS			10

/*
 * Data used while building the histogram.
 */
typedef struct HistogramBuildData {

	float	ndistinct;		/* frequency of distinct values */

	HeapTuple  *rows;		/* aray of sample rows */
	uint32		numrows;	/* number of sample rows (array size) */

	/*
	 * Number of distinct values in each dimension. This is used when
	 * building the histogram (and is not serialized/deserialized).
	 */
	uint32 *ndistincts;

} HistogramBuildData;

typedef HistogramBuildData	*HistogramBuild;

/*
 * builds a multivariate algorithm
 *
 * The build algorithm is iterative - initially a single bucket containing all
 * the sample rows is formed, and then repeatedly split into smaller buckets.
 * In each step the largest bucket (in some sense) is chosen to be split next.
 *
 * The criteria for selecting the largest bucket (and the dimension for the
 * split) needs to be elaborate enough to produce buckets of roughly the same
 * size, and also regular shape (not very long in one dimension).
 *
 * The current algorithm works like this:
 *
 *     build NULL-buckets (create_null_buckets)
 *
 *     while [maximum number of buckets not reached]
 *
 *         choose bucket to partition (largest bucket)
 *             if no bucket to partition
 *                 terminate the algorithm
 *
 *         choose bucket dimension to partition (largest dimension)
 *             split the bucket into two buckets
 *
 * See the discussion at select_bucket_to_partition and partition_bucket for
 * more details about the algorithm.
 */
MVHistogram
build_mv_histogram(int numrows, HeapTuple *rows, int2vector *attrs,
				   VacAttrStats **stats, int numrows_total)
{
	int i;
	int numattrs = attrs->dim1;

	int			   *ndistvalues;
	Datum		  **distvalues;

	MVHistogram		histogram;

	HeapTuple * rows_copy = (HeapTuple*)palloc0(numrows * sizeof(HeapTuple));
	memcpy(rows_copy, rows, sizeof(HeapTuple) * numrows);

	Assert((numattrs >= 2) && (numattrs <= MVSTATS_MAX_DIMENSIONS));

	/* build histogram header */

	histogram = (MVHistogram)palloc0(sizeof(MVHistogramData));

	histogram->magic = MVSTAT_HIST_MAGIC;
	histogram->type  = MVSTAT_HIST_TYPE_BASIC;

	histogram->nbuckets = 1;
	histogram->ndimensions = numattrs;

	/* create max buckets (better than repalloc for short-lived objects) */
	histogram->buckets
		= (MVBucket*)palloc0(MVSTAT_HIST_MAX_BUCKETS * sizeof(MVBucket));

	/* create the initial bucket, covering the whole sample set */
	histogram->buckets[0]
		= create_initial_mv_bucket(numrows, rows_copy, attrs, stats);

	/*
	 * Collect info on distinct values in each dimension (used later to select
	 * dimension to partition).
	 */
	ndistvalues = (int*)palloc0(sizeof(int) * numattrs);
	distvalues  = (Datum**)palloc0(sizeof(Datum*) * numattrs);

	for (i = 0; i < numattrs; i++)
		distvalues[i] = build_ndistinct(numrows, rows, attrs, stats, i,
										&ndistvalues[i]);

	/*
	 * Split the initial bucket into buckets that don't mix NULL and non-NULL
	 * values in a single dimension.
	 */
	create_null_buckets(histogram, 0, attrs, stats);

	/*
	 * Do the actual histogram build - select a bucket and split it.
	 *
	 * FIXME This should use  the max_buckets specified in CREATE STATISTICS.
	 */
	while (histogram->nbuckets < MVSTAT_HIST_MAX_BUCKETS)
	{
		MVBucket bucket = select_bucket_to_partition(histogram->nbuckets,
													 histogram->buckets);

		/* no buckets eligible for partitioning */
		if (bucket == NULL)
			break;

		/* we modify the bucket in-place and add one new bucket */
		histogram->buckets[histogram->nbuckets++]
			= partition_bucket(bucket, attrs, stats, ndistvalues, distvalues);
	}

	/* finalize the histogram build - compute the frequencies etc. */
	for (i = 0; i < histogram->nbuckets; i++)
	{
		HistogramBuild build_data
			= ((HistogramBuild)histogram->buckets[i]->build_data);

		/*
		 * The frequency has to be computed from the whole sample, in case some
		 * of the rows were used for MCV.
		 *
		 * XXX Perhaps this should simply compute frequency with respect to the
		 *     local freuquency, and then factor-in the MCV later.
		 *
		 * FIXME The 'ntuples' sounds a bit inappropriate for frequency.
		 */
		histogram->buckets[i]->ntuples
			= (build_data->numrows * 1.0) / numrows_total;
	}

	return histogram;
}

/* build array of distinct values for a single attribute */
static Datum *
build_ndistinct(int numrows, HeapTuple *rows, int2vector *attrs,
				VacAttrStats **stats, int i, int *nvals)
{
	int				j;
	int				nvalues,
					ndistinct;
	Datum		   *values,
				   *distvalues;

	SortSupportData	ssup;
	StdAnalyzeData *mystats = (StdAnalyzeData *) stats[i]->extra_data;

	/* initialize sort support, etc. */
	memset(&ssup, 0, sizeof(ssup));
	ssup.ssup_cxt = CurrentMemoryContext;

	/* We always use the default collation for statistics */
	ssup.ssup_collation = DEFAULT_COLLATION_OID;
	ssup.ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(mystats->ltopr, &ssup);

	nvalues = 0;
	values = (Datum*)palloc0(sizeof(Datum) * numrows);

	/* collect values from the sample rows, ignore NULLs */
	for (j = 0; j < numrows; j++)
	{
		Datum	value;
		bool	isnull;

		/* remember the index of the sample row, to make the partitioning simpler */
		value = heap_getattr(rows[j], attrs->values[i],
							 stats[i]->tupDesc, &isnull);

		if (isnull)
			continue;

		values[nvalues++] = value;
	}

	/* if no non-NULL values were found, free the memory and terminate */
	if (nvalues == 0)
	{
		pfree(values);
		return NULL;
	}

	/* sort the array of values using the SortSupport */
	qsort_arg((void *) values, nvalues, sizeof(Datum),
			  compare_scalars_simple, (void *) &ssup);

	/* count the distinct values first, and allocate just enough memory */
	ndistinct = 1;
	for (j = 1; j < nvalues; j++)
		if (compare_scalars_simple(&values[j], &values[j-1], &ssup) != 0)
			ndistinct += 1;

	distvalues = (Datum*)palloc0(sizeof(Datum) * ndistinct);

	/* now collect distinct values into the array */
	distvalues[0] = values[0];
	ndistinct = 1;

	for (j = 1; j < nvalues; j++)
	{
		if (compare_scalars_simple(&values[j], &values[j-1], &ssup) != 0)
		{
			distvalues[ndistinct] = values[j];
			ndistinct += 1;
		}
	}

	pfree(values);

	*nvals = ndistinct;
	return distvalues;
}

/* fetch the histogram (as a bytea) from the pg_mv_statistic catalog */
MVSerializedHistogram
load_mv_histogram(Oid mvoid)
{
	bool		isnull = false;
	Datum		histogram;

#ifdef USE_ASSERT_CHECKING
	Form_pg_mv_statistic	mvstat;
#endif

	/* Prepare to scan pg_mv_statistic for entries having indrelid = this rel. */
	HeapTuple	htup = SearchSysCache1(MVSTATOID, ObjectIdGetDatum(mvoid));

	if (! HeapTupleIsValid(htup))
		return NULL;

#ifdef USE_ASSERT_CHECKING
	mvstat = (Form_pg_mv_statistic) GETSTRUCT(htup);
	Assert(mvstat->hist_enabled && mvstat->hist_built);
#endif

	histogram = SysCacheGetAttr(MVSTATOID, htup,
						   Anum_pg_mv_statistic_stahist, &isnull);

	Assert(!isnull);

	ReleaseSysCache(htup);

	return deserialize_mv_histogram(DatumGetByteaP(histogram));
}

/* print some basic info about the histogram */
Datum
pg_mv_stats_histogram_info(PG_FUNCTION_ARGS)
{
	bytea	   *data = PG_GETARG_BYTEA_P(0);
	char	   *result;

	MVSerializedHistogram hist = deserialize_mv_histogram(data);

	result = palloc0(128);
	snprintf(result, 128, "nbuckets=%d", hist->nbuckets);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * Serialize the MV histogram into a bytea value. The basic algorithm is quite
 * simple, and mostly mimincs the MCV serialization:
 *
 * (1) perform deduplication for each attribute (separately)
 *
 *     (a) collect all (non-NULL) attribute values from all buckets
 *     (b) sort the data (using 'lt' from VacAttrStats)
 *     (c) remove duplicate values from the array
 *
 * (2) serialize the arrays into a bytea value
 *
 * (3) process all buckets
 *
 *     (a) replace min/max values with indexes into the arrays
 *
 * Each attribute has to be processed separately, as we're mixing different
 * datatypes, and we we need to use the right operators to compare/sort them.
 * We're also mixing pass-by-value and pass-by-ref types, and so on.
 *
 *
 * FIXME This probably leaks memory, or at least uses it inefficiently
 *       (many small palloc() calls instead of a large one).
 *
 * TODO Consider packing boolean flags (NULL) for each item into 'char'
 *      or a longer type (instead of using an array of bool items).
 */
bytea *
serialize_mv_histogram(MVHistogram histogram, int2vector *attrs,
					   VacAttrStats **stats)
{
	int i = 0, j = 0;
	Size	total_length = 0;

	bytea  *output = NULL;
	char   *data = NULL;

	DimensionInfo  *info;
	SortSupport		ssup;

	int		nbuckets = histogram->nbuckets;
	int		ndims    = histogram->ndimensions;

	/* allocated for serialized bucket data */
	int		bucketsize = BUCKET_SIZE(ndims);
	char   *bucket = palloc0(bucketsize);

	/* values per dimension (and number of non-NULL values) */
	Datum **values = (Datum**)palloc0(sizeof(Datum*) * ndims);
	int	   *counts = (int*)palloc0(sizeof(int) * ndims);

	/* info about dimensions (for deserialize) */
	info = (DimensionInfo *)palloc0(sizeof(DimensionInfo)*ndims);

	/* sort support data */
	ssup = (SortSupport)palloc0(sizeof(SortSupportData)*ndims);

	/* collect and deduplicate values for each dimension separately */
	for (i = 0; i < ndims; i++)
	{
		int count;
		StdAnalyzeData *tmp = (StdAnalyzeData *)stats[i]->extra_data;

		/* keep important info about the data type */
		info[i].typlen   = stats[i]->attrtype->typlen;
		info[i].typbyval = stats[i]->attrtype->typbyval;

		/*
		 * Allocate space for all min/max values, including NULLs (we won't use
		 * them, but we don't know how many are there), and then collect all
		 * non-NULL values.
		 */
		values[i] = (Datum*)palloc0(sizeof(Datum) * nbuckets * 2);

		for (j = 0; j < histogram->nbuckets; j++)
		{
			/* skip buckets where this dimension is NULL-only */
			if (! histogram->buckets[j]->nullsonly[i])
			{
				values[i][counts[i]] = histogram->buckets[j]->min[i];
				counts[i] += 1;

				values[i][counts[i]] = histogram->buckets[j]->max[i];
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

		/* make sure we fit into uint16 */
		Assert(count <= UINT16_MAX);

		/* keep info about the deduplicated count */
		info[i].nvalues = count;

		/* compute size of the serialized data */
		if (info[i].typlen > 0)
			/* byval or byref, but with fixed length (name, tid, ...) */
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
	 * histogram, as it contains these fields:
	 *
	 * - length (4B) for varlena
	 * - magic (4B)
	 * - type (4B)
	 * - ndimensions (4B)
	 * - nbuckets (4B)
	 * - info (ndim * sizeof(DimensionInfo)
	 * - arrays of values for each dimension
	 * - serialized buckets (nbuckets * bucketsize)
	 *
	 * So the 'header' size is 20B + ndim * sizeof(DimensionInfo) and
	 * then we'll place the data (and buckets).
	 */
	total_length = (sizeof(int32) + offsetof(MVHistogramData, buckets)
					+ ndims * sizeof(DimensionInfo)
					+ nbuckets * bucketsize);

	/* account for the deduplicated data */
	for (i = 0; i < ndims; i++)
		total_length += info[i].nbytes;

	/* enforce arbitrary limit of 1MB */
	if (total_length > (1024 * 1024))
		elog(ERROR, "serialized histogram exceeds 1MB (%ld > %d)",
					total_length, (1024 * 1024));

	/* allocate space for the serialized histogram list, set header */
	output = (bytea*)palloc0(total_length);
	SET_VARSIZE(output, total_length);

	/* we'll use 'data' to keep track of the place to write data */
	data = VARDATA(output);

	memcpy(data, histogram, offsetof(MVHistogramData, buckets));
	data += offsetof(MVHistogramData, buckets);

	memcpy(data, info, sizeof(DimensionInfo) * ndims);
	data += sizeof(DimensionInfo) * ndims;

	/* serialize the deduplicated values for all attributes */
	for (i = 0; i < ndims; i++)
	{
#ifdef USE_ASSERT_CHECKING
		char *tmp = data;
#endif
		for (j = 0; j < info[i].nvalues; j++)
		{
			Datum v = values[i][j];

			if (info[i].typbyval)			/* passed by value */
			{
				memcpy(data, &v, info[i].typlen);
				data += info[i].typlen;
			}
			else if (info[i].typlen > 0)	/* pased by reference */
			{
				memcpy(data, DatumGetPointer(v), info[i].typlen);
				data += info[i].typlen;
			}
			else if (info[i].typlen == -1)	/* varlena */
			{
				memcpy(data, DatumGetPointer(v), VARSIZE_ANY(v));
				data += VARSIZE_ANY(values[i][j]);
			}
			else if (info[i].typlen == -2)	/* cstring */
			{
				memcpy(data, DatumGetPointer(v), strlen(DatumGetPointer(v))+1);
				data += strlen(DatumGetPointer(v)) + 1;
			}
		}

		/* make sure we got exactly the amount of data we expected */
		Assert((data - tmp) == info[i].nbytes);
	}

	/* finally serialize the items, with uint16 indexes instead of the values */
	for (i = 0; i < nbuckets; i++)
	{
		/* don't write beyond the allocated space */
		Assert(data <= (char*)output + total_length - bucketsize);

		/* reset the values for each item */
		memset(bucket, 0, bucketsize);

		BUCKET_NTUPLES(bucket) = histogram->buckets[i]->ntuples;

		for (j = 0; j < ndims; j++)
		{
			/* do the lookup only for non-NULL values */
			if (! histogram->buckets[i]->nullsonly[j])
			{
				uint16 idx;
				Datum * v = NULL;

				/* min boundary */
				v = (Datum*)bsearch_arg(&histogram->buckets[i]->min[j],
								values[j], info[j].nvalues, sizeof(Datum),
								compare_scalars_simple, &ssup[j]);

				Assert(v != NULL);	/* serialization or deduplication error */

				/* compute index within the array */
				idx = (v - values[j]);

				Assert((idx >= 0) && (idx < info[j].nvalues));

				BUCKET_MIN_INDEXES(bucket, ndims)[j] = idx;

				/* max boundary */
				v = (Datum*)bsearch_arg(&histogram->buckets[i]->max[j],
								values[j], info[j].nvalues, sizeof(Datum),
								compare_scalars_simple, &ssup[j]);

				Assert(v != NULL);	/* serialization or deduplication error */

				/* compute index within the array */
				idx = (v - values[j]);

				Assert((idx >= 0) && (idx < info[j].nvalues));

				BUCKET_MAX_INDEXES(bucket, ndims)[j] = idx;
			}
		}

		/* copy flags (nulls, min/max inclusive) */
		memcpy(BUCKET_NULLS_ONLY(bucket, ndims),
				histogram->buckets[i]->nullsonly, sizeof(bool) * ndims);

		memcpy(BUCKET_MIN_INCL(bucket, ndims),
				histogram->buckets[i]->min_inclusive, sizeof(bool) * ndims);

		memcpy(BUCKET_MAX_INCL(bucket, ndims),
				histogram->buckets[i]->max_inclusive, sizeof(bool) * ndims);

		/* copy the item into the array */
		memcpy(data, bucket, bucketsize);

		data += bucketsize;
	}

	/* at this point we expect to match the total_length exactly */
	Assert((data - (char*)output) == total_length);

	/* free the values/counts arrays here */
	pfree(counts);
	pfree(info);
	pfree(ssup);

	for (i = 0; i < ndims; i++)
		pfree(values[i]);

	pfree(values);

	return output;
}

/*
 * Returns histogram in a partially-serialized form (keeps the boundary values
 * deduplicated, so that it's possible to optimize the estimation part by
 * caching function call results between buckets etc.).
 */
MVSerializedHistogram
deserialize_mv_histogram(bytea * data)
{
	int i = 0, j = 0;

	Size	expected_size;
	char   *tmp = NULL;

	MVSerializedHistogram histogram;
	DimensionInfo *info;

	int		nbuckets;
	int		ndims;
	int		bucketsize;

	/* temporary deserialization buffer */
	int		bufflen;
	char   *buff;
	char   *ptr;

	if (data == NULL)
		return NULL;

	if (VARSIZE_ANY_EXHDR(data) < offsetof(MVSerializedHistogramData,buckets))
		elog(ERROR, "invalid histogram size %ld (expected at least %ld)",
			 VARSIZE_ANY_EXHDR(data), offsetof(MVSerializedHistogramData,buckets));

	/* read the histogram header */
	histogram
		= (MVSerializedHistogram)palloc(sizeof(MVSerializedHistogramData));

	/* initialize pointer to the data part (skip the varlena header) */
	tmp = VARDATA(data);

	/* get the header and perform basic sanity checks */
	memcpy(histogram, tmp, offsetof(MVSerializedHistogramData, buckets));
	tmp += offsetof(MVSerializedHistogramData, buckets);

	if (histogram->magic != MVSTAT_HIST_MAGIC)
		elog(ERROR, "invalid histogram magic %d (expected %dd)",
			 histogram->magic, MVSTAT_HIST_MAGIC);

	if (histogram->type != MVSTAT_HIST_TYPE_BASIC)
		elog(ERROR, "invalid histogram type %d (expected %dd)",
			 histogram->type, MVSTAT_HIST_TYPE_BASIC);

	nbuckets = histogram->nbuckets;
	ndims    = histogram->ndimensions;
	bucketsize = BUCKET_SIZE(ndims);

	Assert((nbuckets > 0) && (nbuckets <= MVSTAT_HIST_MAX_BUCKETS));
	Assert((ndims >= 2) && (ndims <= MVSTATS_MAX_DIMENSIONS));

	/*
	 * What size do we expect with those parameters (it's incomplete, as we yet
	 * have to count the array sizes (from DimensionInfo records).
	 */
	expected_size = offsetof(MVSerializedHistogramData,buckets) +
					ndims * sizeof(DimensionInfo) +
					(nbuckets * bucketsize);

	/* check that we have at least the DimensionInfo records */
	if (VARSIZE_ANY_EXHDR(data) < expected_size)
		elog(ERROR, "invalid histogram size %ld (expected %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	info = (DimensionInfo*)(tmp);
	tmp += ndims * sizeof(DimensionInfo);

	/* account for the value arrays */
	for (i = 0; i < ndims; i++)
		expected_size += info[i].nbytes;

	if (VARSIZE_ANY_EXHDR(data) != expected_size)
		elog(ERROR, "invalid histogram size %ld (expected %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	/* looks OK - not corrupted or something */

	/* a single buffer for all the values and counts */
	bufflen = (sizeof(int)  + sizeof(Datum*)) * ndims;

	for (i = 0; i < ndims; i++)
		/* don't allocate space for byval types, matching Datum */
		if (! (info[i].typbyval && (info[i].typlen == sizeof(Datum))))
			bufflen += (sizeof(Datum) * info[i].nvalues);

	/* also, include space for the result, tracking the buckets */
	bufflen += nbuckets * (
			   sizeof(MVSerializedBucket) +		/* bucket pointer */
			   sizeof(MVSerializedBucketData));	/* bucket data */

	buff = palloc0(bufflen);
	ptr  = buff;

	histogram->nvalues = (int*)ptr;
	ptr += (sizeof(int) * ndims);

	histogram->values = (Datum**)ptr;
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
	 *
	 * TODO same as in MCV deserialization / consider moving to common.c
	 */
	for (i = 0; i < ndims; i++)
	{
		histogram->nvalues[i] = info[i].nvalues;

		if (info[i].typbyval)
		{
			/* passed by value / Datum - simply reuse the array */
			if (info[i].typlen == sizeof(Datum))
			{
				histogram->values[i] = (Datum*)tmp;
				tmp += info[i].nbytes;
			}
			else
			{
				histogram->values[i] = (Datum*)ptr;
				ptr += (sizeof(Datum) * info[i].nvalues);

				for (j = 0; j < info[i].nvalues; j++)
				{
					/* just point into the array */
					memcpy(&histogram->values[i][j], tmp, info[i].typlen);
					tmp += info[i].typlen;
				}
			}
		}
		else
		{
			/* all the other types need a chunk of the buffer */
			histogram->values[i] = (Datum*)ptr;
			ptr += (sizeof(Datum) * info[i].nvalues);

			if (info[i].typlen > 0)
			{
				/* pased by reference, but fixed length (name, tid, ...) */
				for (j = 0; j < info[i].nvalues; j++)
				{
					/* just point into the array */
					histogram->values[i][j] = PointerGetDatum(tmp);
					tmp += info[i].typlen;
				}
			}
			else if (info[i].typlen == -1)
			{
				/* varlena */
				for (j = 0; j < info[i].nvalues; j++)
				{
					/* just point into the array */
					histogram->values[i][j] = PointerGetDatum(tmp);
					tmp += VARSIZE_ANY(tmp);
				}
			}
			else if (info[i].typlen == -2)
			{
				/* cstring */
				for (j = 0; j < info[i].nvalues; j++)
				{
					/* just point into the array */
					histogram->values[i][j] = PointerGetDatum(tmp);
					tmp += (strlen(tmp) + 1); /* don't forget the \0 */
				}
			}
		}
	}

	histogram->buckets = (MVSerializedBucket*)ptr;
	ptr += (sizeof(MVSerializedBucket) * nbuckets);

	for (i = 0; i < nbuckets; i++)
	{
		MVSerializedBucket bucket = (MVSerializedBucket)ptr;
		ptr += sizeof(MVSerializedBucketData);

		bucket->ntuples			= BUCKET_NTUPLES(tmp);
		bucket->nullsonly		= BUCKET_NULLS_ONLY(tmp, ndims);
		bucket->min_inclusive	= BUCKET_MIN_INCL(tmp, ndims);
		bucket->max_inclusive	= BUCKET_MAX_INCL(tmp, ndims);

		bucket->min				= BUCKET_MIN_INDEXES(tmp, ndims);
		bucket->max				= BUCKET_MAX_INDEXES(tmp, ndims);

		histogram->buckets[i] = bucket;

		Assert(tmp <= (char*)data + VARSIZE_ANY(data));

		tmp += bucketsize;
	}

	/* at this point we expect to match the total_length exactly */
	Assert((tmp - VARDATA(data)) == expected_size);

	/* we should exhaust the output buffer exactly */
	Assert((ptr - buff) == bufflen);

	return histogram;
}

/*
 * Build the initial bucket, which will be then split into smaller ones.
 */
static MVBucket
create_initial_mv_bucket(int numrows, HeapTuple *rows, int2vector *attrs,
						 VacAttrStats **stats)
{
	int i;
	int	numattrs = attrs->dim1;
	HistogramBuild data = NULL;

	/* TODO allocate bucket as a single piece, including all the fields. */
	MVBucket bucket = (MVBucket)palloc0(sizeof(MVBucketData));

	Assert(numrows > 0);
	Assert(rows != NULL);
	Assert((numattrs >= 2) && (numattrs <= MVSTATS_MAX_DIMENSIONS));

	/* allocate the per-dimension arrays */

	/* flags for null-only dimensions */
	bucket->nullsonly = (bool*)palloc0(numattrs * sizeof(bool));

	/* inclusiveness boundaries - lower/upper bounds */
	bucket->min_inclusive = (bool*)palloc0(numattrs * sizeof(bool));
	bucket->max_inclusive = (bool*)palloc0(numattrs * sizeof(bool));

	/* lower/upper boundaries */
	bucket->min = (Datum*)palloc0(numattrs * sizeof(Datum));
	bucket->max = (Datum*)palloc0(numattrs * sizeof(Datum));

	/* build-data */
	data = (HistogramBuild)palloc0(sizeof(HistogramBuildData));

	/* number of distinct values (per dimension) */
	data->ndistincts = (uint32*)palloc0(numattrs * sizeof(uint32));

	/* all the sample rows fall into the initial bucket */
	data->numrows = numrows;
	data->rows = rows;

	bucket->build_data = data;

	/*
	 * Update the number of ndistinct combinations in the bucket (which we use
	 * when selecting bucket to partition), and then number of distinct values
	 * for each partition (which we use when choosing which dimension to split).
	 */
	update_bucket_ndistinct(bucket, attrs, stats);

	/* Update ndistinct (and also set min/max) for all dimensions. */
	for (i = 0; i < numattrs; i++)
		update_dimension_ndistinct(bucket, i, attrs, stats, true);

	return bucket;
}

/*
 * Choose the bucket to partition next.
 *
 * The current criteria is rather simple, chosen so that the algorithm produces
 * buckets with about equal frequency and regular size. We select the bucket
 * with the highest number of distinct values, and then split it by the longest
 * dimension.
 *
 * The distinct values are uniformly mapped to [0,1] interval, and this is used
 * to compute length of the value range.
 *
 * NOTE: This is not the same array used for deduplication, as this contains
 *       values for all the tuples from the sample, not just the boundary values.
 *
 * Returns either pointer to the bucket selected to be partitioned, or NULL if
 * there are no buckets that may be split (e.g. if all buckets are too small
 * or contain too few distinct values).
 *
 *
 * Tricky example
 * --------------
 *
 * Consider this table:
 *
 *     CREATE TABLE t AS SELECT i AS a, i AS b
 *                         FROM generate_series(1,1000000) s(i);
 *
 *     CREATE STATISTICS s1 ON t (a,b) WITH (histogram);
 *
 *     ANALYZE t;
 *
 * It's a very specific (and perhaps artificial) example, because every bucket
 * always has exactly the same number of distinct values in all dimensions,
 * which makes the partitioning tricky.
 *
 * Then:
 *
 *     SELECT * FROM t WHERE (a < 100) AND (b < 100);
 *
 * is estimated to return ~120 rows, while in reality it returns only 99.
 *
 *                           QUERY PLAN
 *     -------------------------------------------------------------
 *      Seq Scan on t  (cost=0.00..19425.00 rows=117 width=8)
 *                     (actual time=0.129..82.776 rows=99 loops=1)
 *        Filter: ((a < 100) AND (b < 100))
 *        Rows Removed by Filter: 999901
 *      Planning time: 1.286 ms
 *      Execution time: 82.984 ms
 *     (5 rows)
 *
 * So this estimate is reasonably close. Let's change the query to OR clause:
 *
 *     SELECT * FROM t WHERE (a < 100) OR (b < 100);
 *
 *                           QUERY PLAN
 *     -------------------------------------------------------------
 *      Seq Scan on t  (cost=0.00..19425.00 rows=8100 width=8)
 *                     (actual time=0.145..99.910 rows=99 loops=1)
 *        Filter: ((a < 100) OR (b < 100))
 *        Rows Removed by Filter: 999901
 *      Planning time: 1.578 ms
 *      Execution time: 100.132 ms
 *     (5 rows)
 *
 * That's clearly a much worse estimate. This happens because the histogram
 * contains buckets like this:
 *
 *     bucket 592  [3 30310] [30134 30593] => [0.000233]
 *
 * i.e. the length of "a" dimension is (30310-3)=30307, while the length of "b"
 * is (30593-30134)=459. So the "b" dimension is much narrower than "a".
 * Of course, there are also buckets where "b" is the wider dimension.
 *
 * This is partially mitigated by selecting the "longest" dimension but that
 * only happens after we already selected the bucket. So if we never select the
 * bucket, this optimization does not apply.
 *
 * The other reason why this particular example behaves so poorly is due to the
 * way we actually split the selected bucket. We do attempt to divide the bucket
 * into two parts containing about the same number of tuples, but that does not
 * too well when most of the tuples is squashed on one side of the bucket.
 *
 * For example for columns with data on the diagonal (i.e. when a=b), we end up
 * with a narrow bucket on the diagonal and a huge bucket overing the remaining
 * part (with much lower density).
 *
 * So perhaps we need two partitioning strategies - one aiming to split buckets
 * with high frequency (number of sampled rows), the other aiming to split
 * "large" buckets. And alternating between them, somehow.
 *
 * TODO Consider using similar lower boundary for row count as for simple
 *      histograms, i.e. 300 tuples per bucket.
 */
static MVBucket
select_bucket_to_partition(int nbuckets, MVBucket * buckets)
{
	int i;
	int numrows = 0;
	MVBucket bucket = NULL;

	for (i = 0; i < nbuckets; i++)
	{
		HistogramBuild data = (HistogramBuild)buckets[i]->build_data;

		/* if the number of rows is higher, use this bucket */
		if ((data->ndistinct > 2) &&
			(data->numrows > numrows) &&
			(data->numrows >= MIN_BUCKET_ROWS)) {
			bucket = buckets[i];
			numrows = data->numrows;
		}
	}

	/* may be NULL if there are not buckets with (ndistinct>1) */
	return bucket;
}

/*
 * A simple bucket partitioning implementation - we choose the longest bucket
 * dimension, measured using the array of distinct values built at the very
 * beginning of the build.
 *
 * We map all the distinct values to a [0,1] interval, uniformly distributed,
 * and then use this to measure length. It's essentially a number of distinct
 * values within the range, normalized to [0,1].
 *
 * Then we choose a 'middle' value splitting the bucket into two parts with
 * roughly the same frequency.
 *
 * This splits the bucket by tweaking the existing one, and returning the new
 * bucket (essentially shrinking the existing one in-place and returning the
 * other "half" as a new bucket). The caller is responsible for adding the new
 * bucket into the list of buckets.
 *
 * There are multiple histogram options, centered around the partitioning
 * criteria, specifying both how to choose a bucket and the dimension most in
 * need of a split. For a nice summary and general overview, see "rK-Hist : an
 * R-Tree based histogram for multi-dimensional selectivity estimation" thesis
 * by J. A. Lopez, Concordia University, p.34-37 (and possibly p. 32-34 for
 * explanation of the terms).
 *
 * It requires care to prevent splitting only one dimension and not splitting
 * another one at all (which might happen easily in case of strongly dependent
 * columns - e.g. y=x). The current algorithm minimizes this, but may still
 * happen for perfectly dependent examples (when all the dimensions have equal
 * length, the first one will be selected).
 *
 * TODO Should probably consider statistics target for the columns (e.g.
 *      to split dimensions with higher statistics target more frequently).
 */
static MVBucket
partition_bucket(MVBucket bucket, int2vector *attrs,
				 VacAttrStats **stats,
				 int *ndistvalues, Datum **distvalues)
{
	int i;
	int dimension;
	int numattrs = attrs->dim1;

	Datum split_value;
	MVBucket new_bucket;
	HistogramBuild new_data;

	/* needed for sort, when looking for the split value */
	bool isNull;
	int nvalues = 0;
	HistogramBuild data = (HistogramBuild)bucket->build_data;
	StdAnalyzeData * mystats = NULL;
	ScalarItem * values = (ScalarItem*)palloc0(data->numrows * sizeof(ScalarItem));
	SortSupportData ssup;

	int nrows = 1;		/* number of rows below current value */
	double delta;

	/* needed when splitting the values */
	HeapTuple * oldrows = data->rows;
	int oldnrows = data->numrows;

	/*
	 * We can't split buckets with a single distinct value (this also
	 * disqualifies NULL-only dimensions). Also, there has to be multiple
	 * sample rows (otherwise, how could there be more distinct values).
	 */
	Assert(data->ndistinct > 1);
	Assert(data->numrows > 1);
	Assert((numattrs >= 2) && (numattrs <= MVSTATS_MAX_DIMENSIONS));

	/* Look for the next dimension to split. */
	delta = 0.0;
	dimension = -1;

	for (i = 0; i < numattrs; i++)
	{
		Datum *a, *b;

		mystats = (StdAnalyzeData *) stats[i]->extra_data;

		/* initialize sort support, etc. */
		memset(&ssup, 0, sizeof(ssup));
		ssup.ssup_cxt = CurrentMemoryContext;

		/* We always use the default collation for statistics */
		ssup.ssup_collation = DEFAULT_COLLATION_OID;
		ssup.ssup_nulls_first = false;

		PrepareSortSupportFromOrderingOp(mystats->ltopr, &ssup);

		/* can't split NULL-only dimension */
		if (bucket->nullsonly[i])
			continue;

		/* can't split dimension with a single ndistinct value */
		if (data->ndistincts[i] <= 1)
			continue;

		/* search for min boundary in the distinct list */
		a = (Datum*)bsearch_arg(&bucket->min[i],
							distvalues[i], ndistvalues[i],
							sizeof(Datum), compare_scalars_simple, &ssup);

		b = (Datum*)bsearch_arg(&bucket->max[i],
							distvalues[i], ndistvalues[i],
							sizeof(Datum), compare_scalars_simple, &ssup);

		/* if this dimension is 'larger' then partition by it */
		if (((b-a)*1.0 / ndistvalues[i]) > delta)
		{
			delta = ((b-a)*1.0 / ndistvalues[i]);
			dimension = i;
		}
	}

	/*
	 * If we haven't found a dimension here, we've done something
	 * wrong in select_bucket_to_partition.
	 */
	Assert(dimension != -1);

	/*
	 * Walk through the selected dimension, collect and sort the values and
	 * then choose the value to use as the new boundary.
	 */
	mystats = (StdAnalyzeData *) stats[dimension]->extra_data;

	/* initialize sort support, etc. */
	memset(&ssup, 0, sizeof(ssup));
	ssup.ssup_cxt = CurrentMemoryContext;

	/* We always use the default collation for statistics */
	ssup.ssup_collation = DEFAULT_COLLATION_OID;
	ssup.ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(mystats->ltopr, &ssup);

	for (i = 0; i < data->numrows; i++)
	{
		/* remember the index of the sample row, to make the partitioning simpler */
		values[nvalues].value = heap_getattr(data->rows[i], attrs->values[dimension],
											 stats[dimension]->tupDesc, &isNull);
		values[nvalues].tupno = i;

		/* no NULL values allowed here (we never split null-only dimension) */
		Assert(!isNull);

		nvalues++;
	}

	/* sort the array of values */
	qsort_arg((void *) values, nvalues, sizeof(ScalarItem),
			  compare_scalars_partition, (void *) &ssup);

	/*
	 * We know there are bucket->ndistincts[dimension] distinct values in this
	 * dimension, and we want to split this into half, so walk through the
	 * array and stop once we see (ndistinct/2) values.
	 *
	 * We always choose the "next" value, i.e. (n/2+1)-th distinct value, and
	 * use it as an exclusive upper boundary (and inclusive lower boundary).
	 *
	 * TODO Maybe we should use "average" of the two middle distinct values
	 *      (at least for even distinct counts), but that would require being
	 *      able to do an average (which does not work for non-numeric types).
	 *
	 * TODO Another option is to look for a split that'd give about 50% tuples
	 *      (not distinct values) in each partition. That might work better
	 *      when there are a few very frequent values, and many rare ones.
	 */
	delta = fabs(data->numrows);
	split_value = values[0].value;

	for (i = 1; i < data->numrows; i++)
	{
		if (values[i].value != values[i-1].value)
		{
			/* are we closer to splitting the bucket in half? */
			if (fabs(i - data->numrows/2.0) < delta)
			{
				/* let's assume we'll use this value for the split */
				split_value = values[i].value;
				delta = fabs(i - data->numrows/2.0);
				nrows = i;
			}
		}
	}

	Assert(nrows > 0);
	Assert(nrows < data->numrows);

	/* create the new bucket as a (incomplete) copy of the one being partitioned. */
	new_bucket = copy_mv_bucket(bucket, numattrs);
	new_data = (HistogramBuild)new_bucket->build_data;

	/*
	* Do the actual split of the chosen dimension, using the split value as the
	* upper bound for the existing bucket, and lower bound for the new one.
	*/
	bucket->max[dimension]     = split_value;
	new_bucket->min[dimension] = split_value;

	bucket->max_inclusive[dimension]		= false;
	new_bucket->max_inclusive[dimension]	= true;

	/*
	 * Redistribute the sample tuples using the 'ScalarItem->tupno' index. We
	 * know 'nrows' rows should remain in the original bucket and the rest goes
	 * to the new one.
	 */

	data->rows     = (HeapTuple*)palloc0(nrows * sizeof(HeapTuple));
	new_data->rows = (HeapTuple*)palloc0((oldnrows - nrows) * sizeof(HeapTuple));

	data->numrows	 = nrows;
	new_data->numrows = (oldnrows - nrows);

	/*
	 * The first nrows should go to the first bucket, the rest should go to the
	 * new one. Use the tupno field to get the actual HeapTuple row from the
	 * original array of sample rows.
	 */
	for (i = 0; i < nrows; i++)
		memcpy(&data->rows[i], &oldrows[values[i].tupno], sizeof(HeapTuple));

	for (i = nrows; i < oldnrows; i++)
		memcpy(&new_data->rows[i-nrows], &oldrows[values[i].tupno], sizeof(HeapTuple));

	/* update ndistinct values for the buckets (total and per dimension) */
	update_bucket_ndistinct(bucket, attrs, stats);
	update_bucket_ndistinct(new_bucket, attrs, stats);

	/*
	 * TODO We don't need to do this for the dimension we used for split,
	 *      because we know how many distinct values went to each partition.
	 */
	for (i = 0; i < numattrs; i++)
	{
		update_dimension_ndistinct(bucket, i, attrs, stats, false);
		update_dimension_ndistinct(new_bucket, i, attrs, stats, false);
	}

	pfree(oldrows);
	pfree(values);

	return new_bucket;
}

/*
 * Copy a histogram bucket. The copy does not include the build-time data, i.e.
 * sampled rows etc.
 */
static MVBucket
copy_mv_bucket(MVBucket bucket, uint32 ndimensions)
{
	/* TODO allocate as a single piece (including all the fields) */
	MVBucket new_bucket = (MVBucket)palloc0(sizeof(MVBucketData));
	HistogramBuild data = (HistogramBuild)palloc0(sizeof(HistogramBuildData));

	/* Copy only the attributes that will stay the same after the split, and
	 * we'll recompute the rest after the split. */

	/* allocate the per-dimension arrays */
	new_bucket->nullsonly = (bool*)palloc0(ndimensions * sizeof(bool));

	/* inclusiveness boundaries - lower/upper bounds */
	new_bucket->min_inclusive = (bool*)palloc0(ndimensions * sizeof(bool));
	new_bucket->max_inclusive = (bool*)palloc0(ndimensions * sizeof(bool));

	/* lower/upper boundaries */
	new_bucket->min = (Datum*)palloc0(ndimensions * sizeof(Datum));
	new_bucket->max = (Datum*)palloc0(ndimensions * sizeof(Datum));

	/* copy data */
	memcpy(new_bucket->nullsonly, bucket->nullsonly, ndimensions * sizeof(bool));

	memcpy(new_bucket->min_inclusive, bucket->min_inclusive, ndimensions*sizeof(bool));
	memcpy(new_bucket->min, bucket->min, ndimensions*sizeof(Datum));

	memcpy(new_bucket->max_inclusive, bucket->max_inclusive, ndimensions*sizeof(bool));
	memcpy(new_bucket->max, bucket->max, ndimensions*sizeof(Datum));

	/* allocate and copy the interesting part of the build data */
	data->ndistincts = (uint32*)palloc0(ndimensions * sizeof(uint32));

	new_bucket->build_data = data;

	return new_bucket;
}

/*
 * Counts the number of distinct values in the bucket. This just copies the
 * Datum values into a simple array, and sorts them using memcmp-based
 * comparator. That means it only works for pass-by-value data types (assuming
 * they don't use collations etc.)
 */
static void
update_bucket_ndistinct(MVBucket bucket, int2vector *attrs, VacAttrStats ** stats)
{
	int i, j;
	int numattrs = attrs->dim1;

	HistogramBuild data = (HistogramBuild)bucket->build_data;
	int numrows = data->numrows;

	MultiSortSupport mss = multi_sort_init(numattrs);

	/*
	 * We could collect this while walking through all the attributes above
	 * (this way we have to call heap_getattr twice).
	 */
	SortItem   *items  = (SortItem*)palloc0(numrows * sizeof(SortItem));
	Datum	   *values = (Datum*)palloc0(numrows * sizeof(Datum) * numattrs);
	bool	   *isnull = (bool*)palloc0(numrows * sizeof(bool) * numattrs);

	for (i = 0; i < numrows; i++)
	{
		items[i].values = &values[i * numattrs];
		items[i].isnull = &isnull[i * numattrs];
	}

	/* prepare the sort function for the first dimension */
	for (i = 0; i < numattrs; i++)
		multi_sort_add_dimension(mss, i, i, stats);

	/* collect the values */
	for (i = 0; i < numrows; i++)
		for (j = 0; j < numattrs; j++)
			items[i].values[j]
				= heap_getattr(data->rows[i], attrs->values[j],
								stats[j]->tupDesc, &items[i].isnull[j]);

	qsort_arg((void *) items, numrows, sizeof(SortItem),
			  multi_sort_compare, mss);

	data->ndistinct = 1;

	for (i = 1; i < numrows; i++)
		if (multi_sort_compare(&items[i], &items[i-1], mss) != 0)
			data->ndistinct += 1;

	pfree(items);
	pfree(values);
	pfree(isnull);
}

/*
 * Count distinct values per bucket dimension.
 */
static void
update_dimension_ndistinct(MVBucket bucket, int dimension, int2vector *attrs,
						   VacAttrStats ** stats, bool update_boundaries)
{
	int j;
	int nvalues = 0;
	bool isNull;
	HistogramBuild data = (HistogramBuild)bucket->build_data;
	Datum * values = (Datum*)palloc0(data->numrows * sizeof(Datum));
	SortSupportData ssup;

	StdAnalyzeData * mystats = (StdAnalyzeData *) stats[dimension]->extra_data;

	/* we may already know this is a NULL-only dimension */
	if (bucket->nullsonly[dimension])
		data->ndistincts[dimension] = 1;

	memset(&ssup, 0, sizeof(ssup));
	ssup.ssup_cxt = CurrentMemoryContext;

	/* We always use the default collation for statistics */
	ssup.ssup_collation = DEFAULT_COLLATION_OID;
	ssup.ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(mystats->ltopr, &ssup);

	for (j = 0; j < data->numrows; j++)
	{
		values[nvalues] = heap_getattr(data->rows[j], attrs->values[dimension],
									   stats[dimension]->tupDesc, &isNull);

		/* ignore NULL values */
		if (! isNull)
			nvalues++;
	}

	/* there's always at least 1 distinct value (may be NULL) */
	data->ndistincts[dimension] = 1;

	/* if there are only NULL values in the column, mark it so and continue
	 * with the next one */
	if (nvalues == 0)
	{
		pfree(values);
		bucket->nullsonly[dimension] = true;
		return;
	}

	/* sort the array (pass-by-value datum */
	qsort_arg((void *) values, nvalues, sizeof(Datum),
			  compare_scalars_simple, (void *) &ssup);

	/*
	 * Update min/max boundaries to the smallest bounding box. Generally, this
	 * needs to be done only when constructing the initial bucket.
	 */
	if (update_boundaries)
	{
		/* store the min/max values */
		bucket->min[dimension] = values[0];
		bucket->min_inclusive[dimension] = true;

		bucket->max[dimension] = values[nvalues-1];
		bucket->max_inclusive[dimension] = true;
	}

	/*
	 * Walk through the array and count distinct values by comparing
	 * succeeding values.
	 *
	 * FIXME This only works for pass-by-value types (i.e. not VARCHARs
	 *       etc.). Although thanks to the deduplication it might work
	 *       even for those types (equal values will get the same item
	 *       in the deduplicated array).
	 */
	for (j = 1; j < nvalues; j++) {
		if (values[j] != values[j-1])
			data->ndistincts[dimension] += 1;
	}

	pfree(values);
}

/*
 * A properly built histogram must not contain buckets mixing NULL and non-NULL
 * values in a single dimension. Each dimension may either be marked as 'nulls
 * only', and thus containing only NULL values, or it must not contain any NULL
 * values.
 *
 * Therefore, if the sample contains NULL values in any of the columns, it's
 * necessary to build those NULL-buckets. This is done in an iterative way
 * using this algorithm, operating on a single bucket:
 *
 *     (1) Check that all dimensions are well-formed (not mixing NULL and
 *         non-NULL values).
 *
 *     (2) If all dimensions are well-formed, terminate.
 *
 *     (3) If the dimension contains only NULL values, but is not marked as
 *         NULL-only, mark it as NULL-only and run the algorithm again (on
 *         this bucket).
 *
 *     (4) If the dimension mixes NULL and non-NULL values, split the bucket
 *         into two parts - one with NULL values, one with non-NULL values
 *         (replacing the current one). Then run the algorithm on both buckets.
 *
 * This is executed in a recursive manner, but the number of executions should
 * be quite low - limited by the number of NULL-buckets. Also, in each branch
 * the number of nested calls is limited by the number of dimensions
 * (attributes) of the histogram.
 *
 * At the end, there should be buckets with no mixed dimensions. The number of
 * buckets produced by this algorithm is rather limited - with N dimensions,
 * there may be only 2^N such buckets (each dimension may be either NULL or
 * non-NULL). So with 8 dimensions (current value of MVSTATS_MAX_DIMENSIONS)
 * there may be only 256 such buckets.
 *
 * After this, a 'regular' bucket-split algorithm shall run, further optimizing
 * the histogram.
 */
static void
create_null_buckets(MVHistogram histogram, int bucket_idx,
					int2vector *attrs, VacAttrStats ** stats)
{
	int			i, j;
	int			null_dim = -1;
	int			null_count = 0;
	bool		null_found = false;
	MVBucket	bucket, null_bucket;
	int			null_idx, curr_idx;
	HistogramBuild	data, null_data;

	/* remember original values from the bucket */
	int			numrows;
	HeapTuple  *oldrows = NULL;

	Assert(bucket_idx < histogram->nbuckets);
	Assert(histogram->ndimensions == attrs->dim1);

	bucket = histogram->buckets[bucket_idx];
	data = (HistogramBuild)bucket->build_data;

	numrows = data->numrows;
	oldrows = data->rows;

	/*
	 * Walk through all rows / dimensions, and stop once we find NULL in a
	 * dimension not yet marked as NULL-only.
	 */
	for (i = 0; i < data->numrows; i++)
	{
		/*
		 * FIXME We don't need to start from the first attribute here - we can
		 *       start from the last known dimension.
		 */
		for (j = 0; j < histogram->ndimensions; j++)
		{
			/* Is this a NULL-only dimension? If yes, skip. */
			if (bucket->nullsonly[j])
				continue;

			/* found a NULL in that dimension? */
			if (heap_attisnull(data->rows[i], attrs->values[j]))
			{
				null_found = true;
				null_dim = j;
				break;
			}
		}

		/* terminate if we found attribute with NULL values */
		if (null_found)
			break;
	}

	/* no regular dimension contains NULL values => we're done */
	if (! null_found)
		return;

	/* walk through the rows again, count NULL values in 'null_dim' */
	for (i = 0; i < data->numrows; i++)
	{
		if (heap_attisnull(data->rows[i], attrs->values[null_dim]))
			null_count += 1;
	}

	Assert(null_count <= data->numrows);

	/*
	 * If (null_count == numrows) the dimension already is NULL-only, but is
	 * not yet marked like that. It's enough to mark it and repeat the process
	 * recursively (until we run out of dimensions).
	 */
	if (null_count == data->numrows)
	{
		bucket->nullsonly[null_dim] = true;
		create_null_buckets(histogram, bucket_idx, attrs, stats);
		return;
	}

	/*
	 * We have to split the bucket into two - one with NULL values in the
	 * dimension, one with non-NULL values. We don't need to sort the data or
	 * anything, but otherwise it's similar to what partition_bucket() does.
	 */

	/* create bucket with NULL-only dimension 'dim' */
	null_bucket = copy_mv_bucket(bucket, histogram->ndimensions);
	null_data = (HistogramBuild)null_bucket->build_data;

	/* remember the current array info */
	oldrows = data->rows;
	numrows = data->numrows;

	/* we'll keep non-NULL values in the current bucket */
	data->numrows = (numrows - null_count);
	data->rows
		= (HeapTuple*)palloc0(data->numrows * sizeof(HeapTuple));

	/* and the NULL values will go to the new one */
	null_data->numrows = null_count;
	null_data->rows
		= (HeapTuple*)palloc0(null_data->numrows * sizeof(HeapTuple));

	/* mark the dimension as NULL-only (in the new bucket) */
	null_bucket->nullsonly[null_dim] = true;

	/* walk through the sample rows and distribute them accordingly */
	null_idx = 0;
	curr_idx = 0;
	for (i = 0; i < numrows; i++)
	{
		if (heap_attisnull(oldrows[i], attrs->values[null_dim]))
			/* NULL => copy to the new bucket */
			memcpy(&null_data->rows[null_idx++], &oldrows[i],
					sizeof(HeapTuple));
		else
			memcpy(&data->rows[curr_idx++], &oldrows[i],
					sizeof(HeapTuple));
	}

	/* update ndistinct values for the buckets (total and per dimension) */
	update_bucket_ndistinct(bucket, attrs, stats);
	update_bucket_ndistinct(null_bucket, attrs, stats);

	/*
	 * TODO We don't need to do this for the dimension we used for split,
	 *      because we know how many distinct values went to each bucket (NULL
	 *      is not a value, so NULL buckets get 0, and the other bucket got all
	 *      the distinct values).
	 */
	for (i = 0; i < histogram->ndimensions; i++)
	{
		update_dimension_ndistinct(bucket, i, attrs, stats, false);
		update_dimension_ndistinct(null_bucket, i, attrs, stats, false);
	}

	pfree(oldrows);

	/* add the NULL bucket to the histogram */
	histogram->buckets[histogram->nbuckets++] = null_bucket;

	/*
	 * And now run the function recursively on both buckets (the new
	 * one first, because the call may change number of buckets, and
	 * it's used as an index).
	 */
	create_null_buckets(histogram, (histogram->nbuckets-1), attrs, stats);
	create_null_buckets(histogram, bucket_idx, attrs, stats);
}

/*
 * SRF with details about buckets of a histogram:
 *
 * - bucket ID (0...nbuckets)
 * - min values (string array)
 * - max values (string array)
 * - nulls only (boolean array)
 * - min inclusive flags (boolean array)
 * - max inclusive flags (boolean array)
 * - frequency (double precision)
 *
 * The input is the OID of the statistics, and there are no rows returned if the
 * statistics contains no histogram (or if there's no statistics for the OID).
 *
 * The second parameter (type) determines what values will be returned
 * in the (minvals,maxvals). There are three possible values:
 *
 * 0 (actual values)
 * -----------------
 *    - prints actual values
 *    - using the output function of the data type (as string)
 *    - handy for investigating the histogram
 *
 * 1 (distinct index)
 * ------------------
 *    - prints index of the distinct value (into the serialized array)
 *    - makes it easier to spot neighbor buckets, etc.
 *    - handy for plotting the histogram
 *
 * 2 (normalized distinct index)
 * -----------------------------
 *    - prints index of the distinct value, but normalized into [0,1]
 *    - similar to 1, but shows how 'long' the bucket range is
 *    - handy for plotting the histogram
 *
 * When plotting the histogram, be careful as the (1) and (2) options skew the
 * lengths by distributing the distinct values uniformly. For data types
 * without a clear meaning of 'distance' (e.g. strings) that is not a big deal,
 * but for numbers it may be confusing.
 */
PG_FUNCTION_INFO_V1(pg_mv_histogram_buckets);

#define OUTPUT_FORMAT_RAW		0
#define OUTPUT_FORMAT_INDEXES	1
#define	OUTPUT_FORMAT_DISTINCT	2

Datum
pg_mv_histogram_buckets(PG_FUNCTION_ARGS)
{
	FuncCallContext	   *funcctx;
	int					call_cntr;
	int					max_calls;
	TupleDesc			tupdesc;
	AttInMetadata	   *attinmeta;

	Oid					mvoid = PG_GETARG_OID(0);
	int					otype = PG_GETARG_INT32(1);

	if ((otype < 0) || (otype > 2))
		elog(ERROR, "invalid output type specified");

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext   oldcontext;
		MVSerializedHistogram histogram;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		histogram = load_mv_histogram(mvoid);

		funcctx->user_fctx = histogram;

		/* total number of tuples to be returned */
		funcctx->max_calls = 0;
		if (funcctx->user_fctx != NULL)
			funcctx->max_calls = histogram->nbuckets;

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
		double		bucket_volume = 1.0;
		StringInfo	bufs;

		char	   *format;
		int			i;

		Oid		   *outfuncs;
		FmgrInfo   *fmgrinfo;

		MVSerializedHistogram histogram;
		MVSerializedBucket bucket;

		histogram = (MVSerializedHistogram)funcctx->user_fctx;

		Assert(call_cntr < histogram->nbuckets);

		bucket = histogram->buckets[call_cntr];

		stakeys = find_mv_attnums(mvoid, &relid);

		/*
		 * The scalar values will be formatted directly, using snprintf.
		 *
		 * The 'array' values will be formatted through StringInfo.
		 */
		values = (char **) palloc0(9 * sizeof(char *));
		bufs   = (StringInfo) palloc0(9 * sizeof(StringInfoData));

		values[0] = (char *) palloc(64 * sizeof(char));

		initStringInfo(&bufs[1]);	/* lower boundaries */
		initStringInfo(&bufs[2]);	/* upper boundaries */
		initStringInfo(&bufs[3]);	/* nulls-only */
		initStringInfo(&bufs[4]);	/* lower inclusive */
		initStringInfo(&bufs[5]);	/* upper inclusive */

		values[6] = (char *) palloc(64 * sizeof(char));
		values[7] = (char *) palloc(64 * sizeof(char));
		values[8] = (char *) palloc(64 * sizeof(char));

		/* we need to do this only when printing the actual values */
		outfuncs = (Oid*)palloc0(sizeof(Oid) * histogram->ndimensions);
		fmgrinfo = (FmgrInfo*)palloc0(sizeof(FmgrInfo) * histogram->ndimensions);

		/*
		 * lookup output functions for all histogram dimensions
		 *
		 * XXX This might be one in the first call and stored in user_fctx.
		 */
		for (i = 0; i < histogram->ndimensions; i++)
		{
			bool isvarlena;

			getTypeOutputInfo(get_atttype(relid, stakeys->values[i]),
							  &outfuncs[i], &isvarlena);

			fmgr_info(outfuncs[i], &fmgrinfo[i]);
		}

		snprintf(values[0], 64, "%d", call_cntr);	/* bucket ID */

		/* for the arrays of lower/upper boundaries, formated according to otype */
		for (i = 0; i < histogram->ndimensions; i++)
		{
			Datum  *vals   = histogram->values[i];

			uint16	minidx = bucket->min[i];
			uint16	maxidx = bucket->max[i];

			/* compute bucket volume, using distinct values as a measure
			 *
			 * XXX Not really sure what to do for NULL dimensions here, so let's
			 *     simply count them as '1'.
			 */
			bucket_volume
				*= (double)(maxidx - minidx + 1) / (histogram->nvalues[i]-1);

			if (i == 0)
				format = "{%s";		/* fist dimension */
			else if (i < (histogram->ndimensions - 1))
				format = ", %s";	/* medium dimensions */
			else
				format = ", %s}";	/* last dimension */

			appendStringInfo(&bufs[3], format, bucket->nullsonly[i] ? "t" : "f");
			appendStringInfo(&bufs[4], format, bucket->min_inclusive[i] ? "t" : "f");
			appendStringInfo(&bufs[5], format, bucket->max_inclusive[i] ? "t" : "f");

			/* for NULL-only  dimension, simply put there the NULL and continue */
			if (bucket->nullsonly[i])
			{
				if (i == 0)
					format = "{%s";
				else if (i < (histogram->ndimensions - 1))
					format = ", %s";
				else
					format = ", %s}";

				appendStringInfo(&bufs[1], format, "NULL");
				appendStringInfo(&bufs[2], format, "NULL");

				continue;
			}

			/* otherwise we really need to format the value */
			switch (otype)
			{
				case OUTPUT_FORMAT_RAW:		/* actual boundary values */

					if (i == 0)
						format = "{%s";
					else if (i < (histogram->ndimensions - 1))
						format = ", %s";
					else
						format = ", %s}";

					appendStringInfo(&bufs[1], format,
									 FunctionCall1(&fmgrinfo[i], vals[minidx]));

					appendStringInfo(&bufs[2], format,
									 FunctionCall1(&fmgrinfo[i], vals[maxidx]));

					break;

				case OUTPUT_FORMAT_INDEXES:	/* indexes into deduplicated arrays */

					if (i == 0)
						format = "{%d";
					else if (i < (histogram->ndimensions - 1))
						format = ", %d";
					else
						format = ", %d}";

					appendStringInfo(&bufs[1], format, minidx);

					appendStringInfo(&bufs[2], format, maxidx);

					break;

				case OUTPUT_FORMAT_DISTINCT:	/* distinct arrays as measure */

					if (i == 0)
						format = "{%f";
					else if (i < (histogram->ndimensions - 1))
						format = ", %f";
					else
						format = ", %f}";

					appendStringInfo(&bufs[1], format,
									 (minidx * 1.0 / (histogram->nvalues[i]-1)));

					appendStringInfo(&bufs[2], format,
									 (maxidx * 1.0 / (histogram->nvalues[i]-1)));

					break;

				default:
					elog(ERROR, "unknown output type: %d", otype);
			}
		}

		values[1] = bufs[1].data;
		values[2] = bufs[2].data;
		values[3] = bufs[3].data;
		values[4] = bufs[4].data;
		values[5] = bufs[5].data;

		snprintf(values[6], 64, "%f", bucket->ntuples);	/* frequency */
		snprintf(values[7], 64, "%f", bucket->ntuples / bucket_volume);	/* density */
		snprintf(values[8], 64, "%f", bucket_volume);	/* volume (as a fraction) */

		/* build a tuple */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		/* clean up (this is not really necessary) */
		pfree(values[0]);
		pfree(values[6]);
		pfree(values[7]);
		pfree(values[8]);

		resetStringInfo(&bufs[1]);
		resetStringInfo(&bufs[2]);
		resetStringInfo(&bufs[3]);
		resetStringInfo(&bufs[4]);
		resetStringInfo(&bufs[5]);

		pfree(bufs);
		pfree(values);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else    /* do when there is no more left */
	{
		SRF_RETURN_DONE(funcctx);
	}
}

#ifdef DEBUG_MVHIST
/*
 * prints debugging info about matched histogram buckets (full/partial)
 *
 * XXX Currently works only for INT data type.
 */
void
debug_histogram_matches(MVSerializedHistogram mvhist, char *matches)
{
	int i, j;

	float ffull = 0, fpartial = 0;
	int nfull = 0, npartial = 0;

	StringInfoData	buf;

	initStringInfo(&buf);

	for (i = 0; i < mvhist->nbuckets; i++)
	{
		MVSerializedBucket bucket = mvhist->buckets[i];

		if (! matches[i])
			continue;

		/* increment the counters */
		nfull += (matches[i] == MVSTATS_MATCH_FULL) ? 1 : 0;
		npartial += (matches[i] == MVSTATS_MATCH_PARTIAL) ? 1 : 0;

		/* and also update the frequencies */
		ffull += (matches[i] == MVSTATS_MATCH_FULL) ? bucket->ntuples : 0;
		fpartial += (matches[i] == MVSTATS_MATCH_PARTIAL) ? bucket->ntuples : 0;

		resetStringInfo(&buf);

		/* build ranges for all the dimentions */
		for (j = 0; j < mvhist->ndimensions; j++)
		{
			appendStringInfo(&buf, '[%d %d]',
							 DatumGetInt32(mvhist->values[j][bucket->min[j]]),
							 DatumGetInt32(mvhist->values[j][bucket->max[j]]));
		}

		elog(WARNING, "bucket %d %s => %d [%f]", i, buf.data, matches[i], bucket->ntuples);
	}

	elog(WARNING, "full=%f partial=%f (%f)", ffull, fpartial, (ffull + 0.5 * fpartial));
}
#endif
