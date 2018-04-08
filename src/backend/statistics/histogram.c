/*-------------------------------------------------------------------------
 *
 * histogram.c
 *	  POSTGRES multivariate histograms
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/statistics/histogram.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

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
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


/*
 * A histogram bucket, with additional build infromation (distinct values
 * in each dimension, associated part of the row sample, etc.). Otherwise
 * it's almost the same thing as MVBucket, except that it's deserialized
 * form (no deduplication of boundary values).
 */
typedef struct MVBucketBuild
{
	/* Frequencies of this bucket. */
	float		frequency;

	/* Iformation about dimensions being NULL-only. Not yet used. */
	bool	   *nullsonly;

	/* lower boundaries - values and information about the inequalities */
	Datum	   *min;
	bool	   *min_inclusive;

	/* upper boundaries - values and information about the inequalities */
	Datum	   *max;
	bool	   *max_inclusive;

	/* number of distinct values in each dimension */
	uint32	   *ndistincts;

	/* number of distinct combination of values */
	uint32		ndistinct;

	/* aray of sample rows (for this bucket) */
	HeapTuple  *rows;
	uint32		numrows;

}			MVBucketBuild;

/*
 * A histogram representation, used while building the histogram. Almost
 * the same thing ans MVHistogram, except that we don't need the magic,
 * type, and varlena header. The main difference is that it contains
 * array of build buckets, not the serialized MVBucket.
 */
typedef struct MVHistogramBuild
{
	uint32		nbuckets;		/* number of buckets (buckets array) */
	uint32		ndimensions;	/* number of dimensions */
	MVBucketBuild **buckets;	/* array of buckets */
}			MVHistogramBuild;

static void create_initial_bucket(MVHistogramBuild * histogram,
					  int numrows, HeapTuple *rows,
					  Bitmapset *attrs, VacAttrStats **stats);

static void create_null_buckets(MVHistogramBuild * histogram, int bucket_idx,
					Bitmapset *attrs, VacAttrStats **stats);

static MVBucketBuild * select_bucket_to_partition(MVHistogramBuild * histogram);

static void partition_bucket(MVHistogramBuild * histogram, MVBucketBuild * bucket,
				 Bitmapset *attrs, VacAttrStats **stats,
				 int *ndistvalues, Datum **distvalues);

static MVBucketBuild * copy_bucket_info(MVHistogramBuild * histogram,
										MVBucketBuild * bucket);

static void update_bucket_ndistinct(MVBucketBuild * bucket, Bitmapset *attrs,
						VacAttrStats **stats);

static void update_dimension_ndistinct(MVBucketBuild * bucket, int dimension,
						   Bitmapset *attrs, VacAttrStats **stats,
						   bool update_boundaries);

static Datum *build_ndistinct(int numrows, HeapTuple *rows, VacAttrStats *stats,
				Oid typeoid, AttrNumber attnum, int *nvals);

static MVHistogram * serialize_histogram(MVHistogramBuild * histogram,
										 VacAttrStats **stats);

/*
 * Computes size of a serialized histogram bucket, depending on the number
 * of dimentions (columns) the statistic is defined on. The datum values
 * are stored in a separate array (deduplicated, to minimize the size), and
 * so the serialized buckets only store uint16 indexes into that array.
 *
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
 *	 ndim * (4 * sizeof(uint16) + 3 * sizeof(bool)) + (2 * sizeof(float))
 *
 * XXX We might save a bit more space by using actual bitmaps instead of
 * boolean arrays.
 */
#define BUCKET_SIZE(ndims)	\
	(ndims * (4 * sizeof(uint16) + 3 * sizeof(bool)) + sizeof(float))

/*
 * Computes the size of a serialized histogram, which consists of:
 *
 * - length      (4B) for varlena header
 * - magic       (4B)
 * - type        (4B)
 * - ndimensions (4B)
 * - nbuckets    (4B)
 * - info        (ndim * sizeof(DimensionInfo)
 * - serialized buckets (nbuckets * bucketsize)
 *
 * This does not include space for the (deserialized) values, which are
 * stored before the serialized buckets.
 */
#define HISTOGRAM_SIZE(ndims, nbuckets) \
	(offsetof(MVHistogram, buckets) + \
	 ndims * sizeof(DimensionInfo) + \
	 nbuckets * BUCKET_SIZE(ndims))

/*
 * Macros for convenient access to parts of a serialized bucket.
 */
#define BUCKET_FREQUENCY(b)		(*(float*)b)
#define BUCKET_MIN_INCL(b,n)	((bool*)(b + sizeof(float)))
#define BUCKET_MAX_INCL(b,n)	(BUCKET_MIN_INCL(b,n) + n)
#define BUCKET_NULLS_ONLY(b,n)	(BUCKET_MAX_INCL(b,n) + n)
#define BUCKET_MIN_INDEXES(b,n) ((uint16*)(BUCKET_NULLS_ONLY(b,n) + n))
#define BUCKET_MAX_INDEXES(b,n) ((BUCKET_MIN_INDEXES(b,n) + n))

/* Access to serialied histogram buckets */
#define BUCKET_MIN_INDEX(bucket,dim) bucket->min[dim]
#define BUCKET_MAX_INDEX(bucket,dim) bucket->max[dim]

#define BUCKET_MIN_VALUE(histogram,bucket, dim) \
	histogram->values[dim][BUCKET_MIN_INDEX(bucket, dim)]

#define BUCKET_MAX_VALUE(histogram,bucket, dim) \
	histogram->values[dim][BUCKET_MAX_INDEX(bucket, dim)]

#define BUCKET_MIN_INCLUSIVE(bucket,dim) \
	(bucket)->min_inclusive[dim]

#define BUCKET_MAX_INCLUSIVE(bucket,dim) \
	(bucket)->max_inclusive[dim]

/*
 * Print bucket and info about matches.
 *
 * XXX Only works for int32 for now.
 */
#ifdef DEBUG_STATS
#define DEBUG_MATCH(label, histogram, bucket, cst, idx, isgt, isstrict, res) \
	{ \
		elog(WARNING, "%s: %d %c%d, %d%c isgt=%d isstrict=%d %s", \
			 label, \
			 DatumGetInt32(cst->constvalue), \
			 BUCKET_MIN_INCLUSIVE(bucket, idx) ? '[' : '(', \
			 DatumGetInt32(BUCKET_MIN_VALUE(histogram, bucket, idx)), \
			 DatumGetInt32(BUCKET_MAX_VALUE(histogram, bucket, idx)), \
			 BUCKET_MAX_INCLUSIVE(bucket, idx) ? ']' : ')', \
			 isgt, isstrict, \
			 ((res == STATS_MATCH_FULL) ? "FULL" : \
				(res == STATS_MATCH_PARTIAL) ? "PARTIAL" : "NONE")); \
	}
#else
#define DEBUG_MATCH(label, histogram, bucket, cst, idx, isgt, isstrict, res)
#endif

/*
 * Minimal number of rows per bucket (can't split smaller buckets).
 *
 * XXX The single-column statistics (std_typanalyze) pretty much says we
 * need 300 rows per bucket. Should we use the same value here?
 */
#define MIN_BUCKET_ROWS			10

/*
 * Builds a multivariate histogram from the set of sampled rows.
 *
 * The build algorithm is iterative - initially there is just a single bucket
 * containing all sample rows, and then it gets repeatedly into smaller and
 * smaller buckets. In each round the largest bucket (measured by frequency)
 * is split into two smaller ones, with about the same frequencies.
 *
 * The criteria for selecting the largest bucket (and the dimension for the
 * split) needs to be elaborate enough to produce buckets of roughly the same
 * size, although we can't guarantee that exactly.
 *
 * Once we pick a bucket, we need to decide according to which dimension to
 * split it. We do consider the number of distinct values for each dimension,
 * but we do also consider shape of the buckets - we don't want buckets that
 * are very narrow in one dimension, because then many queries would hit it.
 * Thus we prefer buckets with about regular shape.
 *
 *
 * Bucket size
 * -----------
 * Note: This brings an interesting problem - How to measure "length" of a
 * bucket dimension, when the dimensions may different data types? Firstly,
 * the concept of "length" is independent of the data type. For example, a
 * column may be defined as NUMERIC but the application may only store
 * values between 0.0 and 1.0, while another NUMERIC column contain values
 * between 0.0 and 1,000,000.0. Interval [0.0, 0.5] means very different
 * things in these cases, yet has the same "numeric length." We're much
 * more interested in the "length" with respect to actual data range.
 *
 * Secondly, the data type may not even have any reasonable definition of
 * "distance". We only require the data type to support ordering (less-than
 * operator). For example, how do you define distance between strings?
 *
 * To work around this, we use the sampled data itself as a measure. We
 * deduplicate the values, order them, and map them uniformly to [0,1].
 * Then we use this to measure distance between values in each dimension.
 *
 *
 * NULL values
 * -----------
 * In case of the regular statistics, NULL values are filtered out before
 * the histogram is built. We cannot do that with multiple columns, though,
 * because a row may have NULL value in one column and non-NULL in another
 * one. Instead, we build buckets so that NULL and non-NULL values are not
 * mixed in a single dimension. Bucket is never split by a dimension that
 * contains only NULL values.
 *
 * The histogram build algorithm works roughly like this:
 *
 *   a) build NULL-buckets (create_null_buckets)
 *
 *   b) while [maximum number of buckets not reached]
 *
 *   c) choose bucket to partition (largest bucket)
 *
 *       c.1) if no bucket eligible to split, terminate the build
 *
 *       c.2) choose bucket dimension to partition (largest dimension)
 *
 *       c.3) split the bucket into two buckets
 *
 * XXX The function does not update the internal pointers, hence the
 * histogram is suitable only for storing. Before using it for estimation,
 * it needs to go through statext_histogram_deserialize() first.
 */
MVHistogram *
statext_histogram_build(int numrows, HeapTuple *rows, Bitmapset *attrs,
						VacAttrStats **stats)
{
	int			i;
	int			numattrs = bms_num_members(attrs);
	int		   *attnums;

	int		   *ndistvalues;
	Datum	  **distvalues;

	MVHistogramBuild *histogram;
	MVBucketBuild *bucket;
	HeapTuple  *rows_copy;

	/* not supposed to build of too few or too many columns */
	Assert((numattrs >= 2) && (numattrs <= STATS_MAX_DIMENSIONS));

	attnums = build_attnums(attrs);

	/*
	 * Build arrays of deduplicated values for each dimension. This is then
	 * used to measure distance when choosing dimension to partition.
	 */
	ndistvalues = (int *) palloc0(sizeof(int) * numattrs);
	distvalues = (Datum **) palloc0(sizeof(Datum *) * numattrs);

	for (i = 0; i < numattrs; i++)
		distvalues[i] = build_ndistinct(numrows, rows,
										stats[i], stats[i]->attrtypid,
										attnums[i], &ndistvalues[i]);


	/* we need to make a copy of the row array, as we'll modify it */
	rows_copy = (HeapTuple *) palloc0(numrows * sizeof(HeapTuple));
	memcpy(rows_copy, rows, sizeof(HeapTuple) * numrows);

	/* Initially we create a single bucket, containing all rows. */
	histogram = (MVHistogramBuild *) palloc0(sizeof(MVHistogramBuild));
	histogram->ndimensions = numattrs;
	histogram->nbuckets = 1;

	/*
	 * We don't know how many buckets are we going to produce, so we allocate
	 * space for the maximum possible number. It's bettern than repeatedly
	 * reallocating a larger and larger array.
	 *
	 * XXX It's possible to compute a lower limit, using MIN_BUCKET_ROWS and
	 * the maximum number of NULL buckets. But this will do for now.
	 */
	histogram->buckets
		= (MVBucketBuild * *) palloc0(STATS_HIST_MAX_BUCKETS * sizeof(MVBucketBuild));

	/* Create the initial bucket, containing all sampled rows */
	create_initial_bucket(histogram, numrows, rows_copy, attrs, stats);

	/*
	 * Split the initial bucket into buckets that don't mix NULL and non-NULL
	 * values in a single dimension. Since we only split the buckets (never
	 * merge them), we maintain this invariant.
	 */
	create_null_buckets(histogram, 0, attrs, stats);

	/*
	 * Split the buckets into smaller and smaller buckets, until either all
	 * buckets are too small (less than MIN_BUCKET_ROWS rows), or there are
	 * too many buckets in total (more than STATS_HIST_MAX_BUCKETS).
	 */
	while ((bucket = select_bucket_to_partition(histogram)) != NULL)
	{
		/* modify the bucket in-place and add one new bucket */
		partition_bucket(histogram, bucket, attrs, stats, ndistvalues, distvalues);
	}

	/* Finalize the histogram build - compute bucket frequencies etc. */
	for (i = 0; i < histogram->nbuckets; i++)
	{
		/* for convenience */
		bucket = histogram->buckets[i];

		/*
		 * The frequency has to be computed from the whole sample, in case
		 * some of the rows were filtered out in the MCV build.
		 */
		bucket->frequency = (bucket->numrows * 1.0) / numrows;
	}

	return serialize_histogram(histogram, stats);
}

/*
 * build_ndistinct
 *		build array of ndistinct values in a particular column
 *
 * We then use this to measure size / distance of values in each dimension.
 * See the 'bucket size' section at statext_histogram_build() for details.
 */
static Datum *
build_ndistinct(int numrows, HeapTuple *rows, VacAttrStats *stats,
				Oid typeoid, AttrNumber attnum, int *nvals)
{
	int			j;
	int			nvalues,
				ndistinct;
	Datum	   *values,
			   *distvalues;

	TypeCacheEntry *type;
	SortSupportData ssup;

	type = lookup_type_cache(typeoid, TYPECACHE_LT_OPR);

	/* initialize sort support, etc. */
	memset(&ssup, 0, sizeof(ssup));
	ssup.ssup_cxt = CurrentMemoryContext;

	/* We always use the default collation for statistics */
	ssup.ssup_collation = DEFAULT_COLLATION_OID;
	ssup.ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(type->lt_opr, &ssup);

	nvalues = 0;
	values = (Datum *) palloc0(sizeof(Datum) * numrows);

	/* collect values from the rows, ignore NULLs */
	for (j = 0; j < numrows; j++)
	{
		Datum		value;
		bool		isnull;

		value = heap_getattr(rows[j], attnum, stats->tupDesc, &isnull);

		if (isnull)
			continue;

		values[nvalues++] = value;
	}

	/* if no non-NULL values were found, we're done */
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
		if (compare_scalars_simple(&values[j], &values[j - 1], &ssup) != 0)
			ndistinct += 1;

	distvalues = (Datum *) palloc0(sizeof(Datum) * ndistinct);

	/* now collect distinct values into the array */
	distvalues[0] = values[0];
	ndistinct = 1;

	for (j = 1; j < nvalues; j++)
	{
		if (compare_scalars_simple(&values[j], &values[j - 1], &ssup) != 0)
		{
			distvalues[ndistinct] = values[j];
			ndistinct += 1;
		}
	}

	pfree(values);

	*nvals = ndistinct;
	return distvalues;
}

/*
 * statext_histogram_load
 *		Load the histogram list for the indicated pg_statistic_ext tuple
 */
MVHistogram *
statext_histogram_load(Oid mvoid)
{
	bool		isnull = false;
	Datum		histogram;
	HeapTuple	htup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(mvoid));

	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for statistics object %u", mvoid);

	histogram = SysCacheGetAttr(STATEXTOID, htup,
								Anum_pg_statistic_ext_stxhistogram, &isnull);

	ReleaseSysCache(htup);

	if (isnull)
		return NULL;

	return statext_histogram_deserialize(DatumGetByteaP(histogram));
}

/*
 * serialize_histogram
 *		Serialize the MV histogram into a varlena value.
 *
 * The algorithm is quite simple, and mostly mimincs the MCV serialization:
 *
 * (1) perform deduplication for each attribute (separately)
 *
 *   (a) collect all (non-NULL) attribute values from all buckets
 *   (b) sort the data (using 'lt' operator from VacAttrStats)
 *   (c) remove duplicate values from the array
 *
 * (2) serialize the arrays into a varlena value
 *
 * (3) process all buckets
 *
 *   (a) replace min/max values with indexes into the arrays
 *
 * Each attribute has to be processed separately, as we're mixing different
 * datatypes, and we we need to use the right operators to compare/sort them.
 * We're also mixing pass-by-value and pass-by-ref types, and so on.
 *
 * TODO Consider packing boolean flags (NULL) for each item into 'char' or
 * a longer type (instead of using an array of bool items). Currently the
 * number of attributes in statistics is limited to just 8, so the whole
 * bitmap might be stored in a single byte.
 *
 * XXX Do we need to care about alignment here? After deserialization we
 * access the histogram in mostly-serialized form (without reshuffling it
 * in any major way, except for updating the pointers), so perhaps some
 * of the fields need to be aligned?
 */
static MVHistogram *
serialize_histogram(MVHistogramBuild * histogram, VacAttrStats **stats)
{
	int			dim,
				i;
	Size		total_length = 0;

	bytea	   *output = NULL;	/* serialized histogram as bytea */
	char	   *data = NULL;

	DimensionInfo *info;
	SortSupport ssup;

	int			nbuckets = histogram->nbuckets;
	int			ndims = histogram->ndimensions;

	/* allocated for serialized bucket data */
	int			bucketsize = BUCKET_SIZE(ndims);
	char	   *bucket = palloc0(bucketsize);

	/* values per dimension (and number of non-NULL values) */
	Oid		   *types = (Oid *) palloc0(sizeof(Oid) * ndims);
	Datum	  **values = (Datum **) palloc0(sizeof(Datum *) * ndims);
	int		   *counts = (int *) palloc0(sizeof(int) * ndims);

	/* info about dimensions (for deserialize) */
	info = (DimensionInfo *) palloc0(sizeof(DimensionInfo) * ndims);

	/* sort support data */
	ssup = (SortSupport) palloc0(sizeof(SortSupportData) * ndims);

	/* collect and deduplicate values for each dimension separately */
	for (dim = 0; dim < ndims; dim++)
	{
		int			b;
		int			count;
		TypeCacheEntry *type;

		type = lookup_type_cache(stats[dim]->attrtypid, TYPECACHE_LT_OPR);

		/* OID of the data types */
		types[dim] = stats[dim]->attrtypid;

		/* keep important info about the data type */
		info[dim].typlen = stats[dim]->attrtype->typlen;
		info[dim].typbyval = stats[dim]->attrtype->typbyval;

		/*
		 * Allocate space for all min/max values, including NULLs (we won't
		 * use them, but we don't know how many are there), and then collect
		 * all non-NULL values.
		 */
		values[dim] = (Datum *) palloc0(sizeof(Datum) * nbuckets * 2);

		for (b = 0; b < histogram->nbuckets; b++)
		{
			/* skip buckets where this dimension is NULL-only */
			if (!histogram->buckets[b]->nullsonly[dim])
			{
				values[dim][counts[dim]] = histogram->buckets[b]->min[dim];
				counts[dim] += 1;

				values[dim][counts[dim]] = histogram->buckets[b]->max[dim];
				counts[dim] += 1;
			}
		}

		/* there are just NULL values in this dimension */
		if (counts[dim] == 0)
			continue;

		/* sort and deduplicate */
		ssup[dim].ssup_cxt = CurrentMemoryContext;
		ssup[dim].ssup_collation = DEFAULT_COLLATION_OID;
		ssup[dim].ssup_nulls_first = false;

		PrepareSortSupportFromOrderingOp(type->lt_opr, &ssup[dim]);

		qsort_arg(values[dim], counts[dim], sizeof(Datum),
				  compare_scalars_simple, &ssup[dim]);

		/*
		 * Walk through the array and eliminate duplicate values, but keep the
		 * ordering (so that we can do bsearch later). We know there's at
		 * least 1 item, so we can skip the first element.
		 */
		count = 1;				/* number of deduplicated items */
		for (i = 1; i < counts[dim]; i++)
		{
			/* if it's different from the previous value, we need to keep it */
			if (compare_datums_simple(values[dim][i - 1], values[dim][i], &ssup[dim]) != 0)
			{
				/* XXX: not needed if (count == i), but that's unlikely */
				values[dim][count] = values[dim][i];
				count += 1;
			}
		}

		/* make sure we fit into uint16 */
		Assert(count <= UINT16_MAX);

		/* keep info about the deduplicated count */
		info[dim].nvalues = count;

		/* compute size of the serialized data */
		if (info[dim].typlen > 0)
		{
			/* byval or byref, but with fixed length (name, tid, ...) */
			info[dim].nbytes = info[dim].nvalues * info[dim].typlen;
		}
		else if (info[dim].typlen == -1)
		{
			/* varlena, so just use VARSIZE_ANY */
			for (i = 0; i < info[dim].nvalues; i++)
				info[dim].nbytes += VARSIZE_ANY(values[dim][i]);
		}
		else if (info[dim].typlen == -2)
		{
			/* cstring, so simply strlen */
			for (i = 0; i < info[dim].nvalues; i++)
				info[dim].nbytes += strlen(DatumGetPointer(values[dim][i]));
		}
		else
			elog(ERROR, "unknown data type typbyval=%d typlen=%d",
				 info[dim].typbyval, info[dim].typlen);
	}

	/* Now we know how much space is needed for the serialized histogram. */
	total_length = HISTOGRAM_SIZE(ndims, nbuckets);

	/* account for the deduplicated data */
	for (dim = 0; dim < ndims; dim++)
		total_length += info[dim].nbytes;

	/*
	 * Enforce arbitrary limit of 1MB on the size of the serialized MCV list.
	 * This is meant as a protection against someone building MCV list on long
	 * values (e.g. text documents).
	 *
	 * XXX Should we enforce arbitrary limits like this one? Maybe it's not
	 * even necessary, as long values are usually unique and so won't make it
	 * into the MCV list in the first place. In the end, we have a 1GB limit
	 * on bytea values.
	 */
	if (total_length > (1024 * 1024))
		elog(ERROR, "serialized histogram exceeds 1MB (%ld > %d)",
			 total_length, (1024 * 1024));

	/* allocate space for the serialized histogram list, set header */
	output = (bytea *) palloc0(total_length);

	/*
	 * We'll use 'data' to keep track of the place to write data.
	 *
	 * XXX No VARDATA() here, as MVHistogram already includes the length.
	 */
	data = (char *) output;

	/* set the header fields (MVHistogramBuild only has some of them) */
	((MVHistogram *) output)->magic = STATS_HIST_MAGIC;
	((MVHistogram *) output)->type = STATS_HIST_TYPE_BASIC;
	((MVHistogram *) output)->nbuckets = histogram->nbuckets;
	((MVHistogram *) output)->ndimensions = histogram->ndimensions;

	/*
	 * Copy the array of type OIDs, which we built above. We do copy it
	 * through the 'histogram->types' field to make it properly aligned.
	 * Otherwise deserialization could not mempcy() on the whole header.
	 *
	 * XXX We store the whole array (STATS_MAX_DIMENSIONS items) for the same
	 * reason. We want to use the serialized histogram directly, and this
	 * would force us to shuffle it somehow.
	 */
	memcpy(((MVHistogram *) output)->types, types,
		   sizeof(Oid) * STATS_MAX_DIMENSIONS);

	/* skip the space we used for header */
	data += offsetof(MVHistogram, buckets);

	/* copy the array with information about dimensions */
	memcpy(data, info, sizeof(DimensionInfo) * ndims);
	data += sizeof(DimensionInfo) * ndims;

	/* serialize the deduplicated values for all attributes */
	for (dim = 0; dim < ndims; dim++)
	{
#ifdef USE_ASSERT_CHECKING
		char	   *tmp = data;
#endif
		for (i = 0; i < info[dim].nvalues; i++)
		{
			Datum		v = values[dim][i];

			if (info[dim].typbyval) /* passed by value */
			{
				memcpy(data, &v, info[dim].typlen);
				data += info[dim].typlen;
			}
			else if (info[dim].typlen > 0)	/* pased by reference */
			{
				memcpy(data, DatumGetPointer(v), info[dim].typlen);
				data += info[dim].typlen;
			}
			else if (info[dim].typlen == -1)	/* varlena */
			{
				memcpy(data, DatumGetPointer(v), VARSIZE_ANY(v));
				data += VARSIZE_ANY(values[dim][i]);
			}
			else if (info[dim].typlen == -2)	/* cstring */
			{
				memcpy(data, DatumGetPointer(v), strlen(DatumGetPointer(v)) + 1);
				data += strlen(DatumGetPointer(v)) + 1;
			}
		}

		/* make sure we got exactly the amount of data we expected */
		Assert((data - tmp) == info[dim].nbytes);
	}

	/* finally serialize the items, with uint16 indexes instead of the values */
	for (i = 0; i < nbuckets; i++)
	{
		/* don't write beyond the allocated space */
		Assert(data <= (char *) output + total_length - bucketsize);

		/* reset the values for each item */
		memset(bucket, 0, bucketsize);

		BUCKET_FREQUENCY(bucket) = histogram->buckets[i]->frequency;

		for (dim = 0; dim < ndims; dim++)
		{
			/* do the lookup only for non-NULL values */
			if (!histogram->buckets[i]->nullsonly[dim])
			{
				uint16		idx;
				Datum	   *v = NULL;

				/* min boundary */
				v = (Datum *) bsearch_arg(&histogram->buckets[i]->min[dim],
										  values[dim], info[dim].nvalues, sizeof(Datum),
										  compare_scalars_simple, &ssup[dim]);

				Assert(v != NULL);	/* serialization or deduplication error */

				/* compute index within the array */
				idx = (v - values[dim]);

				Assert((idx >= 0) && (idx < info[dim].nvalues));

				BUCKET_MIN_INDEXES(bucket, ndims)[dim] = idx;

				/* max boundary */
				v = (Datum *) bsearch_arg(&histogram->buckets[i]->max[dim],
										  values[dim], info[dim].nvalues, sizeof(Datum),
										  compare_scalars_simple, &ssup[dim]);

				Assert(v != NULL);	/* serialization or deduplication error */

				/* compute index within the array */
				idx = (v - values[dim]);

				Assert((idx >= 0) && (idx < info[dim].nvalues));

				BUCKET_MAX_INDEXES(bucket, ndims)[dim] = idx;
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
	Assert((data - (char *) output) == total_length);

	/* free the values/counts arrays here */
	pfree(counts);
	pfree(info);
	pfree(ssup);

	for (dim = 0; dim < ndims; dim++)
		pfree(values[dim]);

	pfree(values);

	/* make sure the length is correct */
	SET_VARSIZE(output, total_length);

	return (MVHistogram *) output;
}

/*
 * statext_histogram_deserialize
 *		Reads serialized histogram into MVHistogram structure.
 *
 * Returns histogram in a partially-serialized form (keeps the boundary
 * values deduplicated, so that it's possible to optimize the estimation
 * part by caching function call results across buckets etc.).
 */
MVHistogram *
statext_histogram_deserialize(bytea *data)
{
	int			dim,
				i;

	Size		expected_size;
	char	   *tmp = NULL;

	MVHistogram *histogram;
	DimensionInfo *info;

	int			nbuckets;
	int			ndims;
	int			bucketsize;

	/* temporary deserialization buffer */
	int			bufflen;
	char	   *buff;
	char	   *ptr;

	if (data == NULL)
		return NULL;

	/*
	 * We can't possibly deserialize a histogram if there's not even a
	 * complete header.
	 */
	if (VARSIZE_ANY_EXHDR(data) < offsetof(MVHistogram, buckets))
		elog(ERROR, "invalid histogram size %ld (expected at least %ld)",
			 VARSIZE_ANY_EXHDR(data), offsetof(MVHistogram, buckets));

	/* read the histogram header */
	histogram
		= (MVHistogram *) palloc(sizeof(MVHistogram));

	/* initialize pointer to data (varlena header is included) */
	tmp = (char *) data;

	/* get the header and perform basic sanity checks */
	memcpy(histogram, tmp, offsetof(MVHistogram, buckets));
	tmp += offsetof(MVHistogram, buckets);

	if (histogram->magic != STATS_HIST_MAGIC)
		elog(ERROR, "invalid histogram magic %d (expected %dd)",
			 histogram->magic, STATS_HIST_MAGIC);

	if (histogram->type != STATS_HIST_TYPE_BASIC)
		elog(ERROR, "invalid histogram type %d (expected %dd)",
			 histogram->type, STATS_HIST_TYPE_BASIC);

	if (histogram->ndimensions == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid zero-length dimension array in histogram")));
	else if (histogram->ndimensions > STATS_MAX_DIMENSIONS)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid length (%d) dimension array in histogram",
						histogram->ndimensions)));

	if (histogram->nbuckets == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid zero-length bucket array in histogram")));
	else if (histogram->nbuckets > STATS_HIST_MAX_BUCKETS)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid length (%d) bucket array in histogram",
						histogram->nbuckets)));

	nbuckets = histogram->nbuckets;
	ndims = histogram->ndimensions;
	bucketsize = BUCKET_SIZE(ndims);

	/*
	 * What size do we expect with those parameters (it's incomplete, as we
	 * yet have to count the array sizes (from DimensionInfo records).
	 */
	expected_size = offsetof(MVHistogram, buckets) +
		ndims * sizeof(DimensionInfo) +
		(nbuckets * bucketsize);

	/* check that we have at least the DimensionInfo records */
	if (VARSIZE_ANY(data) < expected_size)
		elog(ERROR, "invalid histogram size %ld (expected %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	/* Now it's safe to access the dimention info. */
	info = (DimensionInfo *) (tmp);
	tmp += ndims * sizeof(DimensionInfo);

	/* account for the value arrays */
	for (dim = 0; dim < ndims; dim++)
		expected_size += info[dim].nbytes;

	if (VARSIZE_ANY(data) != expected_size)
		elog(ERROR, "invalid histogram size %ld (expected %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	/* looks OK - not corrupted or something */

	/* a single buffer for all the values and counts */
	bufflen = (sizeof(int) + sizeof(Datum *)) * ndims;

	for (dim = 0; dim < ndims; dim++)
		/* don't allocate space for byval types, matching Datum */
		if (!(info[dim].typbyval && (info[dim].typlen == sizeof(Datum))))
			bufflen += (sizeof(Datum) * info[dim].nvalues);

	/* also, include space for the result, tracking the buckets */
	bufflen += nbuckets * (sizeof(MVBucket *) + /* bucket pointer */
						   sizeof(MVBucket));	/* bucket data */

	buff = palloc0(bufflen);
	ptr = buff;

	histogram->nvalues = (int *) ptr;
	ptr += (sizeof(int) * ndims);

	histogram->values = (Datum **) ptr;
	ptr += (sizeof(Datum *) * ndims);

	/*
	 * XXX This uses pointers to the original data array (the types not passed
	 * by value), so when someone frees the memory, e.g. by doing something
	 * like this:
	 *
	 * bytea * data = ... fetch the data from catalog ... MVHistogramBuild
	 * histogram = deserialize_histogram(data); pfree(data);
	 *
	 * then 'histogram' references the freed memory. Should copy the pieces.
	 */
	for (dim = 0; dim < ndims; dim++)
	{
#ifdef USE_ASSERT_CHECKING
		/* remember where data for this dimension starts */
		char	   *start = tmp;
#endif

		histogram->nvalues[dim] = info[dim].nvalues;

		if (info[dim].typbyval)
		{
			/* passed by value / Datum - simply reuse the array */
			if (info[dim].typlen == sizeof(Datum))
			{
				histogram->values[dim] = (Datum *) tmp;
				tmp += info[dim].nbytes;

				/* no overflow of input array */
				Assert(tmp <= start + info[dim].nbytes);
			}
			else
			{
				histogram->values[dim] = (Datum *) ptr;
				ptr += (sizeof(Datum) * info[dim].nvalues);

				for (i = 0; i < info[dim].nvalues; i++)
				{
					/* just point into the array */
					memcpy(&histogram->values[dim][i], tmp, info[dim].typlen);
					tmp += info[dim].typlen;

					/* no overflow of input array */
					Assert(tmp <= start + info[dim].nbytes);
				}
			}
		}
		else
		{
			/* all the other types need a chunk of the buffer */
			histogram->values[dim] = (Datum *) ptr;
			ptr += (sizeof(Datum) * info[dim].nvalues);

			if (info[dim].typlen > 0)
			{
				/* pased by reference, but fixed length (name, tid, ...) */
				for (i = 0; i < info[dim].nvalues; i++)
				{
					/* just point into the array */
					histogram->values[dim][i] = PointerGetDatum(tmp);
					tmp += info[dim].typlen;

					/* no overflow of input array */
					Assert(tmp <= start + info[dim].nbytes);
				}
			}
			else if (info[dim].typlen == -1)
			{
				/* varlena */
				for (i = 0; i < info[dim].nvalues; i++)
				{
					/* just point into the array */
					histogram->values[dim][i] = PointerGetDatum(tmp);
					tmp += VARSIZE_ANY(tmp);

					/* no overflow of input array */
					Assert(tmp <= start + info[dim].nbytes);
				}
			}
			else if (info[dim].typlen == -2)
			{
				/* cstring */
				for (i = 0; i < info[dim].nvalues; i++)
				{
					/* just point into the array */
					histogram->values[dim][i] = PointerGetDatum(tmp);
					tmp += (strlen(tmp) + 1);	/* don't forget the \0 */

					/* no overflow of input array */
					Assert(tmp <= start + info[dim].nbytes);
				}
			}
		}

		/* check we consumed the serialized data for this dimension exactly */
		Assert((tmp - start) == info[dim].nbytes);
	}

	/* now deserialize the buckets and point them into the varlena values */
	histogram->buckets = (MVBucket * *) ptr;
	ptr += (sizeof(MVBucket *) * nbuckets);

	for (i = 0; i < nbuckets; i++)
	{
		MVBucket   *bucket = (MVBucket *) ptr;

		ptr += sizeof(MVBucket);

		bucket->frequency = BUCKET_FREQUENCY(tmp);
		bucket->nullsonly = BUCKET_NULLS_ONLY(tmp, ndims);
		bucket->min_inclusive = BUCKET_MIN_INCL(tmp, ndims);
		bucket->max_inclusive = BUCKET_MAX_INCL(tmp, ndims);

		bucket->min = BUCKET_MIN_INDEXES(tmp, ndims);
		bucket->max = BUCKET_MAX_INDEXES(tmp, ndims);

		histogram->buckets[i] = bucket;

		Assert(tmp <= (char *) data + VARSIZE_ANY(data));

		tmp += bucketsize;
	}

	/* at this point we expect to match the total_length exactly */
	Assert((tmp - (char *) data) == expected_size);

	/* we should exhaust the output buffer exactly */
	Assert((ptr - buff) == bufflen);

	return histogram;
}

/*
 * create_initial_bucket
 *		Create an initial bucket, containing all the sampled rows.
 *
 * The initial bucket is somewhat special, because it's allowed to contain
 * a mix of NULL and non-NULL values in dimensions. Also, all boundaries
 * are inclusive, while for most other buckets some of the boundaries are
 * non-inclusive.
 */
static void
create_initial_bucket(MVHistogramBuild * histogram,
					  int numrows, HeapTuple *rows,
					  Bitmapset *attrs, VacAttrStats **stats)
{
	int			i;
	int			numattrs = bms_num_members(attrs);

	/* TODO allocate bucket as a single piece, including all the fields. */
	MVBucketBuild *bucket = (MVBucketBuild *) palloc0(sizeof(MVBucketBuild));

	Assert(numrows > 0);
	Assert(rows != NULL);
	Assert((numattrs >= 2) && (numattrs <= STATS_MAX_DIMENSIONS));

	/* allocate the per-dimension arrays */

	/* flags for null-only dimensions */
	bucket->nullsonly = (bool *) palloc0(numattrs * sizeof(bool));

	/* inclusiveness boundaries - lower/upper bounds */
	bucket->min_inclusive = (bool *) palloc0(numattrs * sizeof(bool));
	bucket->max_inclusive = (bool *) palloc0(numattrs * sizeof(bool));

	/* lower/upper boundaries */
	bucket->min = (Datum *) palloc0(numattrs * sizeof(Datum));
	bucket->max = (Datum *) palloc0(numattrs * sizeof(Datum));

	/* number of distinct values (per dimension) */
	bucket->ndistincts = (uint32 *) palloc0(numattrs * sizeof(uint32));

	/* all the sample rows fall into the initial bucket */
	bucket->numrows = numrows;
	bucket->rows = rows;

	/*
	 * Update the number of ndistinct combinations in the bucket (which we use
	 * when selecting bucket to partition), and then number of distinct values
	 * for each partition (which we use when choosing a dimension to split).
	 *
	 * Calling update_dimension_ndistinct also updates the boundary values and
	 * inclusive flags (which were only allocated so far).
	 */
	update_bucket_ndistinct(bucket, attrs, stats);

	for (i = 0; i < numattrs; i++)
		update_dimension_ndistinct(bucket, i, attrs, stats, true);

	/* the initial bucket is always the first one (obviously) */
	histogram->buckets[0] = bucket;
}

/*
 * Choose the bucket to partition next.
 *
 * The current criteria is rather simple, chosen so that the algorithm produces
 * buckets with about equal frequency and regular size. We select the bucket
 * with the highest frequency (number of rows), and then split it by the longest
 * dimension. We also require the bucket to have at least two distinct values
 * in any dimension, otherwise we wouldn't be able to split it.
 *
 * The distinct values are uniformly mapped to [0,1] interval, and this is used
 * to compute length of the value range.
 *
 * NOTE: This is not the same array used for deduplication, as this contains
 * all distinct values from the sample, not just the boundary values.
 *
 * Returns either pointer to the bucket selected to be partitioned, or NULL if
 * there are no buckets that may be split (e.g. if all buckets are too small
 * or contain too few distinct values).
 *
 * XXX Perhaps we should have multiple partitioning strategies? One aiming to
 * split buckets with high frequency (number of sampled rows), one to split
 * "large" buckets (by volume). And then alternate between them somehow.
 *
 * For example for columns with data on the diagonal (i.e. when a=b), we may
 * end up with a narrow bucket on the diagonal and a huge bucket overing the
 * remaining part (with much lower density).
 *
 * TODO Consider using similar lower boundary for row count as for simple
 * histograms, i.e. 300 tuples per bucket.
 */
static MVBucketBuild *
select_bucket_to_partition(MVHistogramBuild * histogram)
{
	int			i;
	int			numrows = 0;
	MVBucketBuild *selected_bucket = NULL;

	/* terminate if the histogram reached maxium number of buckets */
	if (histogram->nbuckets == STATS_HIST_MAX_BUCKETS)
		return NULL;

	for (i = 0; i < histogram->nbuckets; i++)
	{
		MVBucketBuild *bucket = histogram->buckets[i];

		/* if the number of rows is higher, use this bucket */
		if ((bucket->ndistinct > 1) &&
			(bucket->numrows > numrows) &&
			(bucket->numrows >= MIN_BUCKET_ROWS))
		{
			selected_bucket = bucket;
			numrows = selected_bucket->numrows;
		}
	}

	/* may be NULL if there are no buckets with (ndistinct > 1) */
	return selected_bucket;
}

/*
 * partition_bucket
 *		Split the selected bucket into two (the current one and a new one).
 *
 * A simple bucket partitioning implementation - we choose the longest bucket
 * dimension, measured using the array of distinct values (built at the very
 * beginning of the build) as a scale.
 *
 * We map all the distinct values to a [0,1] interval, uniformly distributed,
 * and then use this to measure length. It's essentially a number of distinct
 * values within the range, normalized to [0,1].
 *
 * Then we choose a 'middle' value splitting the bucket into two parts with
 * roughly the same frequency.
 *
 * One part of the bucket is then moved to a new one, and the bucket is
 * updated (boundary values, etc.). The new bucket is returned to the caller
 * who is responsible for adding it to the histogram.
 *
 * It requires care to prevent splitting only one dimension and not splitting
 * another one at all (which might happen easily in case of strongly dependent
 * columns - e.g. y=x). The current algorithm minimizes this, but it may still
 * happen for perfectly dependent examples (when all the dimensions have equal
 * length, the first one will be selected).
 *
 * TODO Should probably consider statistics target for the columns (e.g.
 * to split dimensions with higher statistics target more frequently).
 */
static void
partition_bucket(MVHistogramBuild * histogram, MVBucketBuild * bucket,
				 Bitmapset *attrs, VacAttrStats **stats,
				 int *ndistvalues, Datum **distvalues)
{
	int			i;
	int			dimension;
	int			numattrs = bms_num_members(attrs);

	Datum		split_value;
	MVBucketBuild *new_bucket;

	/* needed for sort, when looking for the split value */
	bool		isNull;
	int			nvalues = 0;
	TypeCacheEntry *type;
	ScalarItem *values;
	SortSupportData ssup;
	int		   *attnums;

	int			nrows = 1;		/* number of rows below current value */
	double		delta;

	/* needed when splitting the values */
	HeapTuple  *oldrows = bucket->rows;
	int			oldnrows = bucket->numrows;

	values = (ScalarItem *) palloc0(bucket->numrows * sizeof(ScalarItem));

	/*
	 * We can't split buckets with a single distinct value (this also
	 * disqualifies NULL-only dimensions). Also, there has to be multiple
	 * sample rows (otherwise, how could there be more distinct values).
	 */
	Assert(bucket->ndistinct > 1);
	Assert(bucket->numrows > 1);
	Assert((numattrs >= 2) && (numattrs <= STATS_MAX_DIMENSIONS));

	/* Look for the next dimension to split. */
	delta = 0.0;
	dimension = -1;

	/* Pick the dimension with the largest 'delta' on the [0,1] scale. */
	for (i = 0; i < numattrs; i++)
	{
		Datum	   *min,
				   *max;

		/* can't split NULL-only dimension */
		if (bucket->nullsonly[i])
			continue;

		/* can't split dimension with a single ndistinct value */
		if (bucket->ndistincts[i] <= 1)
			continue;

		/* initialize sort support, etc. */
		memset(&ssup, 0, sizeof(ssup));
		ssup.ssup_cxt = CurrentMemoryContext;

		/* We always use the default collation for statistics */
		ssup.ssup_collation = DEFAULT_COLLATION_OID;
		ssup.ssup_nulls_first = false;

		type = lookup_type_cache(stats[i]->attrtypid, TYPECACHE_LT_OPR);

		PrepareSortSupportFromOrderingOp(type->lt_opr, &ssup);

		/* search for min boundary in the distinct list */
		min = (Datum *) bsearch_arg(&bucket->min[i],
									distvalues[i], ndistvalues[i],
									sizeof(Datum), compare_scalars_simple, &ssup);

		max = (Datum *) bsearch_arg(&bucket->max[i],
									distvalues[i], ndistvalues[i],
									sizeof(Datum), compare_scalars_simple, &ssup);

		/* if this dimension is 'larger' then partition by it */
		if (((max - min) * 1.0 / ndistvalues[i]) > delta)
		{
			delta = ((max - min) * 1.0 / ndistvalues[i]);
			dimension = i;
		}
	}

	/*
	 * If we haven't found a dimension here, we've done something wrong in
	 * select_bucket_to_partition.
	 */
	Assert(dimension != -1);

	/*
	 * Walk through the selected dimension, collect and sort the values and
	 * then choose the value to use as the new boundary.
	 */
	type = lookup_type_cache(stats[dimension]->attrtypid, TYPECACHE_LT_OPR);

	/* initialize sort support, etc. */
	memset(&ssup, 0, sizeof(ssup));
	ssup.ssup_cxt = CurrentMemoryContext;

	/* We always use the default collation for statistics */
	ssup.ssup_collation = DEFAULT_COLLATION_OID;
	ssup.ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(type->lt_opr, &ssup);

	attnums = build_attnums(attrs);

	for (i = 0; i < bucket->numrows; i++)
	{
		/*
		 * remember the index of the sample row, to make the partitioning
		 * simpler
		 */
		values[nvalues].value = heap_getattr(bucket->rows[i], attnums[dimension],
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
	 * We want to split the bucket in half (or as close to it as possible),
	 * along the selected dimension. So we walk through the sorted array of
	 * values from that dimension, and whenever the value changes we check how
	 * far are we from the middle. If closer than the last split, then we
	 * update the split value.
	 */
	delta = bucket->numrows;
	split_value = values[0].value;
	nrows = 0;

	for (i = 1; i < bucket->numrows; i++)
	{
		/* check distance from middle when the value changes */
		if (compare_scalars_partition(&values[i - 1], &values[i], &ssup))
		{
			/* are we closer to splitting the bucket in half? */
			if (fabs(i - bucket->numrows / 2.0) < delta)
			{
				/* let's assume we'll use this value for the split */
				split_value = values[i].value;
				delta = fabs(i - bucket->numrows / 2.0);

				/*
				 * i-th row will go to the new bucket, and it's 0-based, so
				 * the old bucket will contain i rows.
				 */
				nrows = i;
			}
		}
	}

	Assert(nrows > 0);
	Assert(nrows < bucket->numrows);

	/* create new bucket and copy some basic info from the current one */
	new_bucket = copy_bucket_info(histogram, bucket);

	/*
	 * Do the actual split, using the split value as the upper bound for the
	 * existing bucket, and lower bound for the new one.
	 */
	bucket->max[dimension] = split_value;
	new_bucket->min[dimension] = split_value;

	/*
	 * Boundary values can only be inclusive in one of the buckets, where it's
	 * used as lower boundary. And exclusive in the other one.
	 */
	bucket->max_inclusive[dimension] = false;
	new_bucket->min_inclusive[dimension] = true;

	/*
	 * Redistribute the sample tuples using the 'ScalarItem->tupno' index. We
	 * know 'nrows' rows should remain in the original bucket and the rest
	 * goes to the new one.
	 */
	bucket->numrows = nrows;
	new_bucket->numrows = (oldnrows - nrows);

	bucket->rows = (HeapTuple *) palloc0(bucket->numrows * sizeof(HeapTuple));
	new_bucket->rows = (HeapTuple *) palloc0(new_bucket->numrows * sizeof(HeapTuple));

	/*
	 * The first nrows should go to the current bucket, the rest should go to
	 * the new one. Use the tupno field to get the actual HeapTuple row from
	 * the original array of sample rows.
	 */
	for (i = 0; i < nrows; i++)
		memcpy(&bucket->rows[i], &oldrows[values[i].tupno], sizeof(HeapTuple));

	for (i = nrows; i < oldnrows; i++)
		memcpy(&new_bucket->rows[i - nrows], &oldrows[values[i].tupno], sizeof(HeapTuple));

	/* update ndistinct values for the buckets (total and per dimension) */
	update_bucket_ndistinct(bucket, attrs, stats);
	update_bucket_ndistinct(new_bucket, attrs, stats);

	/* Also update the ndistinct counts for each dimension. */
	for (i = 0; i < numattrs; i++)
	{
		update_dimension_ndistinct(bucket, i, attrs, stats, false);
		update_dimension_ndistinct(new_bucket, i, attrs, stats, false);
	}

	pfree(oldrows);
	pfree(values);
}

/*
 * copy_bucket_info
 *		Copy basic info about a histogram bucket.
 *
 * This only copies the min/max values, inclusiveness flags, etc. Not the
 * the build-time data, like sampled rows etc.
 */
static MVBucketBuild *
copy_bucket_info(MVHistogramBuild * histogram, MVBucketBuild * bucket)
{
	/* TODO allocate as a single piece (including all the fields) */
	MVBucketBuild *new_bucket;
	int			ndimensions = histogram->ndimensions;

	/* make sure the histogram did not reach the maximum size yet */
	Assert(histogram->nbuckets < STATS_HIST_MAX_BUCKETS);

	new_bucket = (MVBucketBuild *) palloc0(sizeof(MVBucketBuild));
	histogram->buckets[histogram->nbuckets++] = new_bucket;

	/* allocate the per-dimension arrays */
	new_bucket->nullsonly = (bool *) palloc0(ndimensions * sizeof(bool));

	/* inclusiveness boundaries - lower/upper bounds */
	new_bucket->min_inclusive = (bool *) palloc0(ndimensions * sizeof(bool));
	new_bucket->max_inclusive = (bool *) palloc0(ndimensions * sizeof(bool));

	/* lower/upper boundaries */
	new_bucket->min = (Datum *) palloc0(ndimensions * sizeof(Datum));
	new_bucket->max = (Datum *) palloc0(ndimensions * sizeof(Datum));

	/* copy data */
	memcpy(new_bucket->nullsonly, bucket->nullsonly, ndimensions * sizeof(bool));

	memcpy(new_bucket->min_inclusive, bucket->min_inclusive, ndimensions * sizeof(bool));
	memcpy(new_bucket->min, bucket->min, ndimensions * sizeof(Datum));

	memcpy(new_bucket->max_inclusive, bucket->max_inclusive, ndimensions * sizeof(bool));
	memcpy(new_bucket->max, bucket->max, ndimensions * sizeof(Datum));

	/* allocate space for ndistinct counts (per dimension) */
	new_bucket->ndistincts = (uint32 *) palloc0(ndimensions * sizeof(uint32));

	return new_bucket;
}

/*
 * update_bucket_ndistinct
 *		Counts the number of distinct values in the bucket.
 *
 * Copies datum values into an array, sorts them using LT operator and
 * then counts the number of combinations in all columns.
 */
static void
update_bucket_ndistinct(MVBucketBuild * bucket, Bitmapset *attrs, VacAttrStats **stats)
{
	int			i;
	int			numattrs = bms_num_members(attrs);
	int			numrows = bucket->numrows;

	MultiSortSupport mss = multi_sort_init(numattrs);
	int		   *attnums;
	SortItem   *items;

	attnums = build_attnums(attrs);

	/* prepare the sort function for the first dimension */
	for (i = 0; i < numattrs; i++)
	{
		VacAttrStats *colstat = stats[i];
		TypeCacheEntry *type;

		type = lookup_type_cache(colstat->attrtypid, TYPECACHE_LT_OPR);
		if (type->lt_opr == InvalidOid) /* shouldn't happen */
			elog(ERROR, "cache lookup failed for ordering operator for type %u",
				 colstat->attrtypid);

		multi_sort_add_dimension(mss, i, type->lt_opr);
	}

	/*
	 * build an array of SortItem(s) sorted using the multi-sort support
	 *
	 * XXX This relies on all stats entries pointing to the same tuple
	 * descriptor. Not sure if that might not be the case.
	 */
	items = build_sorted_items(numrows, bucket->rows, stats[0]->tupDesc,
							   mss, numattrs, attnums);

	bucket->ndistinct = 1;

	for (i = 1; i < numrows; i++)
		if (multi_sort_compare(&items[i], &items[i - 1], mss) != 0)
			bucket->ndistinct += 1;

	pfree(items);
}

/*
 * update_dimension_ndistinct
 *		Update number of distinct values per bucket dimension.
 *
 * This is usually needed after the bucket gets split into two parts.
 * Can update the min/max for the boundary (when update_boundaries=true).
 */
static void
update_dimension_ndistinct(MVBucketBuild * bucket, int dimension, Bitmapset *attrs,
						   VacAttrStats **stats, bool update_boundaries)
{
	int			j;
	int			nvalues = 0;
	bool		isNull;
	Datum	   *values;
	SortSupportData ssup;
	TypeCacheEntry *type;
	int		   *attnums;

	values = (Datum *) palloc0(bucket->numrows * sizeof(Datum));
	type = lookup_type_cache(stats[dimension]->attrtypid, TYPECACHE_LT_OPR);

	/* we may already know this is a NULL-only dimension */
	if (bucket->nullsonly[dimension])
		bucket->ndistincts[dimension] = 1;

	memset(&ssup, 0, sizeof(ssup));
	ssup.ssup_cxt = CurrentMemoryContext;

	/* We always use the default collation for statistics */
	ssup.ssup_collation = DEFAULT_COLLATION_OID;
	ssup.ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(type->lt_opr, &ssup);

	attnums = build_attnums(attrs);

	for (j = 0; j < bucket->numrows; j++)
	{
		values[nvalues] = heap_getattr(bucket->rows[j], attnums[dimension],
									   stats[dimension]->tupDesc, &isNull);

		/* ignore NULL values */
		if (!isNull)
			nvalues++;
	}

	/* there's always at least 1 distinct value (may be NULL) */
	bucket->ndistincts[dimension] = 1;

	/*
	 * if there are only NULL values in the column, mark it so and continue
	 * with the next one
	 */
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

		bucket->max[dimension] = values[nvalues - 1];
		bucket->max_inclusive[dimension] = true;
	}

	/*
	 * Walk through the array and count distinct values by comparing
	 * succeeding values.
	 */
	for (j = 1; j < nvalues; j++)
	{
		if (compare_datums_simple(values[j - 1], values[j], &ssup) != 0)
			bucket->ndistincts[dimension] += 1;
	}

	pfree(values);
}

/*
 * create_null_buckets
 *		Creates the minimum number of buckets with NULL-only dimensions.
 *
 * A properly built histogram must not contain buckets mixing NULL and non-NULL
 * values in a single dimension. Each dimension may either be marked as 'nulls
 * only', and thus containing only NULL values, or it must not contain any NULL
 * values.
 *
 * Therefore, if the sample contains NULL values in any of the columns, it's
 * necessary to build those NULL-buckets. This is done in an iterative way
 * using this algorithm, operating on a single bucket:
 *
 *	   (1) Check that all dimensions are well-formed (not mixing NULL and
 *		   non-NULL values, and properly marked as NULL-only).
 *
 *	   (2) If all dimensions are well-formed, terminate.
 *
 *	   (3) If the dimension contains only NULL values, but is not marked as
 *		   NULL-only, mark it as NULL-only and run the algorithm again (on
 *		   this bucket).
 *
 *	   (4) If the dimension mixes NULL and non-NULL values, split the bucket
 *		   into two parts - one with NULL values, one with non-NULL values.
 *		   Then run the algorithm on both buckets.
 *
 * This is executed in a recursive manner, but the number of executions should
 * be quite low - limited by the number of NULL-buckets. Also, in each branch
 * the number of nested calls is limited by the number of dimensions
 * (attributes) of the histogram.
 *
 * At the end, there should be buckets with no mixed dimensions. The number of
 * buckets produced by this algorithm is rather limited - with N dimensions,
 * there may be only 2^N such buckets (each dimension may be either NULL or
 * non-NULL). So with 8 dimensions (current value of STATS_MAX_DIMENSIONS)
 * there may be only 256 such buckets.
 *
 * After this, a 'regular' bucket-split algorithm shall run, further optimizing
 * the histogram.
 */
static void
create_null_buckets(MVHistogramBuild * histogram, int bucket_idx,
					Bitmapset *attrs, VacAttrStats **stats)
{
	int			i,
				j;
	int			null_dim = -1;
	int			null_count = 0;
	bool		null_found = false;
	MVBucketBuild *bucket,
			   *null_bucket;
	int			null_idx,
				curr_idx;
	int		   *attnums;

	/* remember original values from the bucket */
	int			numrows;
	HeapTuple  *oldrows = NULL;

	Assert(bucket_idx < histogram->nbuckets);
	Assert(histogram->ndimensions == bms_num_members(attrs));

	bucket = histogram->buckets[bucket_idx];

	numrows = bucket->numrows;
	oldrows = bucket->rows;

	attnums = build_attnums(attrs);

	/*
	 * Walk through all rows / dimensions, and stop once we find NULL in a
	 * dimension not yet marked as NULL-only.
	 */
	for (i = 0; i < bucket->numrows; i++)
	{
		for (j = 0; j < histogram->ndimensions; j++)
		{
			/* Is this a NULL-only dimension? If yes, skip. */
			if (bucket->nullsonly[j])
				continue;

			/* found a NULL in that dimension? */
			if (heap_attisnull(bucket->rows[i], attnums[j],
							   stats[j]->tupDesc))
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
	if (!null_found)
		return;

	/* walk through the rows again, count NULL values in 'null_dim' */
	for (i = 0; i < bucket->numrows; i++)
	{
		if (heap_attisnull(bucket->rows[i], attnums[null_dim],
						   stats[null_dim]->tupDesc))
			null_count += 1;
	}

	Assert(null_count <= bucket->numrows);

	/*
	 * If (null_count == numrows) the dimension already is NULL-only, but is
	 * not yet marked like that. It's enough to mark it and repeat the process
	 * on the next dimension (until we run out of dimensions).
	 */
	if (null_count == bucket->numrows)
	{
		bucket->nullsonly[null_dim] = true;
		create_null_buckets(histogram, bucket_idx, attrs, stats);
		return;
	}

	/*
	 * The bucket contains both NULL and non-NULL values in this dimension. We
	 * have to split it into two. We don't need to sort the data, but
	 * otherwise it's pretty similar to what partition_bucket() does.
	 */

	/* create bucket with NULL-only dimension 'dim' */
	null_bucket = copy_bucket_info(histogram, bucket);

	/* remember the current array info */
	oldrows = bucket->rows;
	numrows = bucket->numrows;

	/* we'll keep non-NULL values in the current bucket */
	bucket->numrows = (numrows - null_count);
	bucket->rows
		= (HeapTuple *) palloc0(bucket->numrows * sizeof(HeapTuple));

	/* and the NULL values will go to the new one */
	null_bucket->numrows = null_count;
	null_bucket->rows
		= (HeapTuple *) palloc0(null_bucket->numrows * sizeof(HeapTuple));

	/* mark the dimension as NULL-only (in the new bucket) */
	null_bucket->nullsonly[null_dim] = true;

	/* walk through the sample rows and distribute them accordingly */
	null_idx = 0;
	curr_idx = 0;
	for (i = 0; i < numrows; i++)
	{
		if (heap_attisnull(oldrows[i], attnums[null_dim],
						   stats[null_dim]->tupDesc))
			/* NULL => copy to the new bucket */
			memcpy(&null_bucket->rows[null_idx++], &oldrows[i],
				   sizeof(HeapTuple));
		else
			memcpy(&bucket->rows[curr_idx++], &oldrows[i],
				   sizeof(HeapTuple));
	}

	/* update ndistinct values for the buckets (total and per dimension) */
	update_bucket_ndistinct(bucket, attrs, stats);
	update_bucket_ndistinct(null_bucket, attrs, stats);

	/*
	 * TODO We don't need to do this for the dimension we used for split,
	 * because we know how many distinct values went to each bucket (NULL is
	 * not a value, so NULL buckets get 0, and the other bucket got all the
	 * distinct values).
	 */
	for (i = 0; i < histogram->ndimensions; i++)
	{
		update_dimension_ndistinct(bucket, i, attrs, stats, false);
		update_dimension_ndistinct(null_bucket, i, attrs, stats, false);
	}

	pfree(oldrows);

	/*
	 * And now run the function recursively on both buckets (the new one
	 * first, because the call may change number of buckets, and it's used as
	 * an index).
	 */
	create_null_buckets(histogram, (histogram->nbuckets - 1), attrs, stats);
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
 *	  - prints actual values
 *	  - using the output function of the data type (as string)
 *	  - handy for investigating the histogram
 *
 * 1 (distinct index)
 * ------------------
 *	  - prints index of the distinct value (into the serialized array)
 *	  - makes it easier to spot neighbor buckets, etc.
 *	  - handy for plotting the histogram
 *
 * 2 (normalized distinct index)
 * -----------------------------
 *	  - prints index of the distinct value, but normalized into [0,1]
 *	  - similar to 1, but shows how 'long' the bucket range is
 *	  - handy for plotting the histogram
 *
 * When plotting the histogram, be careful as the (1) and (2) options skew the
 * lengths by distributing the distinct values uniformly. For data types
 * without a clear meaning of 'distance' (e.g. strings) that is not a big deal,
 * but for numbers it may be confusing.
 */
PG_FUNCTION_INFO_V1(pg_histogram_buckets);

#define OUTPUT_FORMAT_RAW		0
#define OUTPUT_FORMAT_INDEXES	1
#define OUTPUT_FORMAT_DISTINCT	2

Datum
pg_histogram_buckets(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int			call_cntr;
	int			max_calls;
	TupleDesc	tupdesc;
	AttInMetadata *attinmeta;

	int			otype = PG_GETARG_INT32(1);

	if ((otype < 0) || (otype > 2))
		elog(ERROR, "invalid output type specified");

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		MVHistogram *histogram;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		histogram = statext_histogram_deserialize(PG_GETARG_BYTEA_P(0));

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
		 * generate attribute metadata needed later to produce tuples from raw
		 * C strings
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

	if (call_cntr < max_calls)	/* do when there is more left to send */
	{
		char	  **values;
		HeapTuple	tuple;
		Datum		result;
		double		bucket_volume = 1.0;
		StringInfo	bufs;

		char	   *format;
		int			i;

		Oid		   *outfuncs;
		FmgrInfo   *fmgrinfo;

		MVHistogram *histogram;
		MVBucket   *bucket;

		histogram = (MVHistogram *) funcctx->user_fctx;

		Assert(call_cntr < histogram->nbuckets);

		bucket = histogram->buckets[call_cntr];

		/*
		 * The scalar values will be formatted directly, using snprintf.
		 *
		 * The 'array' values will be formatted through StringInfo.
		 */
		values = (char **) palloc0(9 * sizeof(char *));
		bufs = (StringInfo) palloc0(9 * sizeof(StringInfoData));

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
		outfuncs = (Oid *) palloc0(sizeof(Oid) * histogram->ndimensions);
		fmgrinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * histogram->ndimensions);

		/*
		 * lookup output functions for all histogram dimensions
		 *
		 * XXX This might be one in the first call and stored in user_fctx.
		 */
		for (i = 0; i < histogram->ndimensions; i++)
		{
			bool		isvarlena;

			getTypeOutputInfo(histogram->types[i], &outfuncs[i], &isvarlena);

			fmgr_info(outfuncs[i], &fmgrinfo[i]);
		}

		snprintf(values[0], 64, "%d", call_cntr);	/* bucket ID */

		/*
		 * for the arrays of lower/upper boundaries, formated according to
		 * otype
		 */
		for (i = 0; i < histogram->ndimensions; i++)
		{
			Datum	   *vals = histogram->values[i];

			uint16		minidx = bucket->min[i];
			uint16		maxidx = bucket->max[i];

			int			d = 1;

			/*
			 * compute bucket volume, using distinct values as a measure
			 *
			 * XXX Not really sure what to do for NULL dimensions or
			 * dimensions with just a single value here, so let's simply count
			 * them as 1. They will not affect the volume anyway.
			 */
			if (histogram->nvalues[i] > 1)
				d = (histogram->nvalues[i] - 1);

			bucket_volume *= (double) (maxidx - minidx + 1) / d;

			if (i == 0)
				format = "{%s"; /* fist dimension */
			else if (i < (histogram->ndimensions - 1))
				format = ", %s";	/* medium dimensions */
			else
				format = ", %s}";	/* last dimension */

			appendStringInfo(&bufs[3], format, bucket->nullsonly[i] ? "t" : "f");
			appendStringInfo(&bufs[4], format, bucket->min_inclusive[i] ? "t" : "f");
			appendStringInfo(&bufs[5], format, bucket->max_inclusive[i] ? "t" : "f");

			/*
			 * for NULL-only  dimension, simply put there the NULL and
			 * continue
			 */
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
				case OUTPUT_FORMAT_RAW: /* actual boundary values */

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

				case OUTPUT_FORMAT_INDEXES: /* indexes into deduplicated
											 * arrays */

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

					appendStringInfo(&bufs[1], format, (minidx * 1.0 / d));
					appendStringInfo(&bufs[2], format, (maxidx * 1.0 / d));

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

		snprintf(values[6], 64, "%f", bucket->frequency);	/* frequency */
		snprintf(values[7], 64, "%f", bucket->frequency / bucket_volume);	/* density */
		snprintf(values[8], 64, "%f", bucket_volume);	/* volume (as a
														 * fraction) */

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
	else						/* do when there is no more left */
	{
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * pg_histogram_in		- input routine for type pg_histogram.
 *
 * pg_histogram is real enough to be a table column, but it has no operations
 * of its own, and disallows input too
 */
Datum
pg_histogram_in(PG_FUNCTION_ARGS)
{
	/*
	 * pg_histogram stores the data in binary form and parsing text input is
	 * not needed, so disallow this.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_histogram")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

/*
 * pg_histogram_out		- output routine for type pg_histogram.
 *
 * histograms are serialized into a bytea value, so we simply call byteaout()
 * to serialize the value into text. But it'd be nice to serialize that into
 * a meaningful representation (e.g. for inspection by people).
 *
 * XXX This should probably return something meaningful, similar to what
 * pg_dependencies_out does. Not sure how to deal with the deduplicated
 * values, though - do we want to expand that or not?
 */
Datum
pg_histogram_out(PG_FUNCTION_ARGS)
{
	return byteaout(fcinfo);
}

/*
 * pg_histogram_recv		- binary input routine for type pg_histogram.
 */
Datum
pg_histogram_recv(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_histogram")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

/*
 * pg_histogram_send		- binary output routine for type pg_histogram.
 *
 * Histograms are serialized in a bytea value (although the type is named
 * differently), so let's just send that.
 */
Datum
pg_histogram_send(PG_FUNCTION_ARGS)
{
	return byteasend(fcinfo);
}

/*
 * selectivity estimation
 */

/*
 * When evaluating conditions on the histogram, we can leverage the fact that
 * each bucket boundary value is used by many buckets (each bucket split
 * introduces a single new value, duplicating all the other values). That
 * allows us to significantly reduce the number of function calls by caching
 * the results.
 *
 * This is one of the reasons why we keep the histogram in partially serialized
 * form, with deduplicated values. This allows us to maintain a simple array
 * of results indexed by uint16 values.
 *
 * We only need 2 bits per value, but we allocate a full char as it's more
 * convenient and there's not much to gain. 0 means 'unknown' as the function
 * was not executed for this value yet.
 */

#define HIST_CACHE_EMPTY			0x00
#define HIST_CACHE_FALSE			0x01
#define HIST_CACHE_TRUE				0x03
#define HIST_CACHE_MASK				0x02

#define CONST_IS_EQ_BOUNDARY(cache,idx) (cache[idx] == HIST_CACHE_TRUE)
#define CONST_IS_LT_BOUNDARY(cache,idx) (cache[idx] == HIST_CACHE_TRUE)
#define CONST_IS_GT_BOUNDARY(cache,idx) (cache[idx] == HIST_CACHE_FALSE)

/*
 * update_call_cache
 *		Ensure the call cache contains result for a given boundary value.
 *
 * If a result for a given boudary value is not stored in the call cache,
 * the function evaluates the operator and stores the result in the cache.
 */
static void
update_call_cache(char *cache, FmgrInfo proc,
				  Datum constvalue, Datum value, int index)
{
	Datum		r;

	/* if the result is already in the cache, we're done */
	if (cache[index] != HIST_CACHE_EMPTY)
		return;

	r = DatumGetBool(FunctionCall2Coll(&proc,
									   DEFAULT_COLLATION_OID,
									   constvalue,
									   value));

	cache[index] = (r) ? HIST_CACHE_TRUE : HIST_CACHE_FALSE;
}

/*
 * bucket_contains_value
 *		Decide if the bucket (a range of values in a particular dimension) may
 *		contain the supplied value.
 *
 * The function does not simply return true/false, but a "match level" (none,
 * partial, full), just like other similar functions. In fact, those function
 * only returns "partial" or "none" levels, as a range can never match exactly
 * a value (we never generate histograms with "collapsed" dimensions).
 */
static char
bucket_contains_value(FmgrInfo eqproc, FmgrInfo ltproc,
					  MVHistogram * histogram, MVBucket * bucket,
					  int dim, Datum constvalue,
					  char *eq_cache, char *lt_cache)
{
	bool		lower_equals,
				upper_equals;

	/*
	 * First we can do some quick equality checks on bucket boundaries. If
	 * both boundaries match, we immediately know it's a full match,
	 * irrespectedly of the inclusiveness of the boundaries.
	 *
	 * We do need this information anyway, so let's just do it both for
	 * equality and inequality operator now.
	 */
	update_call_cache(eq_cache, eqproc, constvalue,
					  BUCKET_MIN_VALUE(histogram, bucket, dim),
					  BUCKET_MIN_INDEX(bucket, dim));

	update_call_cache(eq_cache, eqproc, constvalue,
					  BUCKET_MAX_VALUE(histogram, bucket, dim),
					  BUCKET_MAX_INDEX(bucket, dim));

	update_call_cache(lt_cache, ltproc, constvalue,
					  BUCKET_MIN_VALUE(histogram, bucket, dim),
					  BUCKET_MIN_INDEX(bucket, dim));

	update_call_cache(lt_cache, ltproc, constvalue,
					  BUCKET_MAX_VALUE(histogram, bucket, dim),
					  BUCKET_MAX_INDEX(bucket, dim));

	/* get the cached result */
	lower_equals
		= CONST_IS_EQ_BOUNDARY(eq_cache, BUCKET_MIN_INDEX(bucket, dim));
	upper_equals
		= CONST_IS_EQ_BOUNDARY(eq_cache, BUCKET_MAX_INDEX(bucket, dim));

	/*
	 * If both boundaries match, then the whole bucket matches, no matter
	 * which of the boundaries is inclusive (it should be one of them).
	 */
	if (lower_equals && upper_equals)
		return STATS_MATCH_FULL;

	/*
	 * Otherwise it's a partial match (when the boundary is inclusive) or
	 * mismatch (if it's exclusive).
	 */
	if (lower_equals)
		return (BUCKET_MIN_INCLUSIVE(bucket, dim)) ? STATS_MATCH_PARTIAL : STATS_MATCH_NONE;

	if (upper_equals)
		return (BUCKET_MAX_INCLUSIVE(bucket, dim)) ? STATS_MATCH_PARTIAL : STATS_MATCH_NONE;

	/* From now on we know neither of the boundaries is equal. */
	Assert(!(lower_equals | upper_equals));

	/*
	 * When (constval < min_value) or (constval > max_value), then the bucket
	 * contains no matches.
	 */
	if (CONST_IS_LT_BOUNDARY(lt_cache, BUCKET_MIN_INDEX(bucket, dim)) ||
		CONST_IS_GT_BOUNDARY(lt_cache, BUCKET_MAX_INDEX(bucket, dim)))
		return STATS_MATCH_NONE;

	/*
	 * Otherwise it's a partial match (we know it's not a full match, as that
	 * case was handled above).
	 */
	return STATS_MATCH_PARTIAL;
}

/*
 * bucket_is_smaller_than_value
 *		Decide if the bucket (a range of values in a particular dimension) is
 *		smaller than the supplied value.
 *
 * The function does not simply return true/false, but a "match level" (none,
 * partial, full), just like other similar functions.
 *
 * Unlike bucket_contains_value this may return all three match levels, i.e.
 * "full" (e.g. [10,20] < 30), "partial" (e.g. [10,20] < 15) and "none"
 * (e.g. [10,20] < 5).
 */
static char
bucket_is_smaller_than_value(FmgrInfo eqproc, FmgrInfo ltproc,
							 MVHistogram * histogram, MVBucket * bucket,
							 int dim, Datum constvalue,
							 char *eq_cache, char *lt_cache,
							 bool isgt, bool isstrict)
{
	/*
	 * First update the cache values. We update all of them at once, because
	 * we'll probably need each of them anyway - either now or for some other
	 * bucket, and ensuring we have all the values simplifies the logic.
	 */
	update_call_cache(eq_cache, eqproc, constvalue,
					  BUCKET_MIN_VALUE(histogram, bucket, dim),
					  BUCKET_MIN_INDEX(bucket, dim));

	update_call_cache(eq_cache, eqproc, constvalue,
					  BUCKET_MAX_VALUE(histogram, bucket, dim),
					  BUCKET_MAX_INDEX(bucket, dim));

	update_call_cache(lt_cache, ltproc, constvalue,
					  BUCKET_MIN_VALUE(histogram, bucket, dim),
					  BUCKET_MIN_INDEX(bucket, dim));

	update_call_cache(lt_cache, ltproc, constvalue,
					  BUCKET_MAX_VALUE(histogram, bucket, dim),
					  BUCKET_MAX_INDEX(bucket, dim));

	/*
	 * Combine the results into the final answer. We need to be careful about
	 * the 'isgt' flag, which inverts the meaning in some cases, and also
	 * strictness (i.e. whether we're dealing with < or <=).
	 */

	/*
	 * We look at equalities with boundaries first. For each bundary that
	 * equals to a boundary value, we check whether it's inclusive or not, and
	 * then make decision based on isgt/isstrict flags.
	 */

	/*
	 * This means the constant is equal to lower boundary.
	 *
	 * In the (isgt=false) case, this means const <= [min,max], i.e. the whole
	 * bucket likely mismatches the condition. It may be a partial match,
	 * however, when the boundary is inclusive and the restriction is not
	 * strict.
	 *
	 * In the (isgt=true) case we apply the inverse logic.
	 */
	if (CONST_IS_EQ_BOUNDARY(eq_cache, BUCKET_MIN_INDEX(bucket, dim)))
	{
		char		match;

		if (!isgt)
		{
			match = STATS_MATCH_NONE;

			if (!isstrict && BUCKET_MIN_INCLUSIVE(bucket, dim))
				match = STATS_MATCH_PARTIAL;
		}
		else
		{
			match = STATS_MATCH_FULL;

			if (isstrict && BUCKET_MIN_INCLUSIVE(bucket, dim))
				match = STATS_MATCH_PARTIAL;
		}

		return match;
	}

	/*
	 * This means the constant is equal to upper boundary.
	 *
	 * In the (isgt=false) case, this means [min,max] <= const, i.e. the whole
	 * bucket likely matches the condition. It may be a partial match,
	 * however, when the boundary is inclusive and the restriction is strict.
	 */
	if (CONST_IS_EQ_BOUNDARY(eq_cache, BUCKET_MAX_INDEX(bucket, dim)))
	{
		char		match;

		if (!isgt)
		{
			match = STATS_MATCH_PARTIAL;

			if (!(isstrict && BUCKET_MAX_INCLUSIVE(bucket, dim)))
				match = STATS_MATCH_FULL;
		}
		else
		{
			match = STATS_MATCH_PARTIAL;

			if (!BUCKET_MAX_INCLUSIVE(bucket, dim) || isstrict)
				match = STATS_MATCH_NONE;
		}

		return match;
	}

	/*
	 * And now the inequalities. We can assume neither boundary is equal to
	 * the constant (those cases were already handled above), which greatly
	 * simplifies the reasoning here.
	 */
	if (!isgt)
	{
		if (CONST_IS_LT_BOUNDARY(lt_cache, BUCKET_MIN_INDEX(bucket, dim)))
			return STATS_MATCH_NONE;

		if (CONST_IS_LT_BOUNDARY(lt_cache, BUCKET_MAX_INDEX(bucket, dim)))
			return STATS_MATCH_PARTIAL;

		return STATS_MATCH_FULL;
	}
	else
	{
		if (CONST_IS_GT_BOUNDARY(lt_cache, BUCKET_MAX_INDEX(bucket, dim)))
			return STATS_MATCH_NONE;

		if (CONST_IS_GT_BOUNDARY(lt_cache, BUCKET_MIN_INDEX(bucket, dim)))
			return STATS_MATCH_PARTIAL;

		return STATS_MATCH_FULL;
	}
}

/*
 * Evaluate clauses using the histogram, and update the match bitmap.
 *
 * The bitmap may be already partially set, so this is really a way to
 * combine results of several clause lists - either when computing
 * conditional probability P(A|B) or a combination of AND/OR clauses.
 *
 * Note: This is not a simple bitmap in the sense that there are three
 * possible values for each item - no match, partial match and full match.
 * So we need at least 2 bits per item.
 *
 * TODO: This works with 'bitmap' where each item is represented as a
 * char, which is slightly wasteful. Instead, we could use a bitmap
 * with 2 bits per item, reducing the size to ~1/4. By using values
 * 0, 1 and 3 (instead of 0, 1 and 2), the operations (merging etc.)
 * might be performed just like for simple bitmap by using & and |,
 * which might be faster than min/max.
 */
static void
histogram_update_match_bitmap(PlannerInfo *root, List *clauses,
							  Bitmapset *stakeys,
							  MVHistogram * histogram,
							  char *matches, bool is_or)
{
	int			i;
	ListCell   *l;

	/*
	 * Used for caching function calls, only once per deduplicated value.
	 *
	 * We know may have up to (2 * nbuckets) values per dimension. It's
	 * probably overkill, but let's allocate that once for all clauses, to
	 * minimize overhead.
	 *
	 * Also, we only need two bits per value, but this allocates byte per
	 * value. Might be worth optimizing.
	 *
	 * 0x00 - not yet called 0x01 - called, result is 'false' 0x03 - called,
	 * result is 'true'
	 */
	char	   *eqcache = palloc(histogram->nbuckets);
	char	   *ltcache = palloc(histogram->nbuckets);

	Assert(histogram != NULL);
	Assert(histogram->nbuckets > 0);

	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);

	/* loop through the clauses and do the estimation */
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);

		/* if it's a RestrictInfo, then extract the clause */
		if (IsA(clause, RestrictInfo))
			clause = (Node *) ((RestrictInfo *) clause)->clause;

		/* it's either OpClause, or NullTest */
		if (is_opclause(clause))
		{
			OpExpr	   *expr = (OpExpr *) clause;
			bool		varonleft = true;
			bool		ok;

			/* reset the cache (per clause) */
			memset(eqcache, HIST_CACHE_EMPTY, histogram->nbuckets);
			memset(ltcache, HIST_CACHE_EMPTY, histogram->nbuckets);

			ok = (NumRelids(clause) == 1) &&
				(is_pseudo_constant_clause(lsecond(expr->args)) ||
				 (varonleft = false,
				  is_pseudo_constant_clause(linitial(expr->args))));

			if (ok)
			{
				/*
				 * This does rely on using equality and less-than operators
				 * instead of the operator referenced by the OpExpr. I'm not
				 * sure that's correct, but we are relying on the equality and
				 * less-than semantics anyway so I'm not sure it's wrong
				 * either. I just can't think of an example demonstrating why
				 * it's broken. After all, when the operator claims to use
				 * F_SCALARLTSEL, it better use the same semantics, I guess.
				 *
				 * For a while I thought it might be an issue with cross-type
				 * operators (e.g. int > numeric), in which case the operator
				 * associated with the type would not work correctly (because
				 * it expects the same type on both sides). But that's not an
				 * issue, as such clauses are rejected as incompatible, as the
				 * cast will generate FuncExpr for the implicit cast (and the
				 * regular stats behave just like that).
				 *
				 * So I'm a bit puzzled ...
				 */

				TypeCacheEntry *typecache;

				FmgrInfo	eqproc;
				FmgrInfo	ltproc;
				bool		isstrict;
				int			dim;

				RegProcedure oprrest = get_oprrest(expr->opno);

				Var		   *var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);
				Const	   *cst = (varonleft) ? lsecond(expr->args) : linitial(expr->args);
				bool		isgt = (!varonleft);

				/* Is the restriction a strict inequality? */
				isstrict = (oprrest == F_SCALARLTSEL) || (oprrest == F_SCALARGTSEL);

				/*
				 * We need equality and less-than operator (which is already
				 * required by the ANALYZE part, so we can rely on having it).
				 */
				typecache = lookup_type_cache(var->vartype,
											  TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR);

				/* lookup dimension for the attribute */
				dim = bms_member_index(stakeys, var->varattno);

				fmgr_info(get_opcode(typecache->lt_opr), &ltproc);
				fmgr_info(get_opcode(typecache->eq_opr), &eqproc);

				/*
				 * Evaluate the clause for all buckets that still can yield a
				 * positive match (i.e. those that have partial or full match
				 * in the bitmap).
				 *
				 * We know the clauses use supported operators (that's how we
				 * matched them to the statistics).
				 */
				for (i = 0; i < histogram->nbuckets; i++)
				{
					char		res = STATS_MATCH_NONE;

					MVBucket   *bucket = histogram->buckets[i];

					/*
					 * For AND-lists, we can also mark NULL buckets as 'no
					 * match' (and then skip them). For OR-lists this is not
					 * possible.
					 */
					if ((!is_or) && bucket->nullsonly[dim])
						matches[i] = STATS_MATCH_NONE;

					/*
					 * Skip buckets that were already eliminated - this is
					 * impotant considering how we update the info (we only
					 * lower the match). We can't really do anything about the
					 * MATCH_PARTIAL buckets.
					 */
					if ((!is_or) && (matches[i] == STATS_MATCH_NONE))
						continue;
					else if (is_or && (matches[i] == STATS_MATCH_FULL))
						continue;

					/*
					 * Handle common comparison operators (<, <=, >, >=, =).
					 */
					switch (oprrest)
					{
						case F_SCALARLESEL: /* Var <= Const */
						case F_SCALARLTSEL: /* Var < Const */

							res = bucket_is_smaller_than_value(eqproc, ltproc,
															   histogram, bucket,
															   dim, cst->constvalue,
															   eqcache, ltcache,
															   isgt, isstrict);

							DEBUG_MATCH("LT", histogram, bucket, cst, dim,
										isgt, isstrict, res);

							break;

						case F_SCALARGESEL: /* Var >= Const */
						case F_SCALARGTSEL: /* Var > Const */

							res = bucket_is_smaller_than_value(eqproc, ltproc,
															   histogram, bucket,
															   dim, cst->constvalue,
															   eqcache, ltcache,
															   !isgt, isstrict);

							DEBUG_MATCH("GT", histogram, bucket, cst, dim,
										!isgt, isstrict, res);

							break;

						case F_EQSEL:

							/*
							 * We only check whether the value is within the
							 * bucket, using the lt operator, and we also
							 * check for equality with the boundaries.
							 */

							res = bucket_contains_value(eqproc, ltproc,
														histogram, bucket,
														dim, cst->constvalue,
														eqcache, ltcache);

							DEBUG_MATCH("EQ", histogram, bucket, cst, dim,
										!isgt, isstrict, res);

							break;

						case F_NEQSEL:

							/*
							 * We only check whether the value is within the
							 * bucket, using the lt operator, and we also
							 * check for equality with the boundaries.
							 */

							res = bucket_contains_value(eqproc, ltproc,
														histogram, bucket,
														dim, cst->constvalue,
														eqcache, ltcache);

							/* inequality, so invert the result */
							if (res == STATS_MATCH_FULL)
								res = STATS_MATCH_NONE;
							else if (res == STATS_MATCH_NONE)
								res = STATS_MATCH_FULL;

							DEBUG_MATCH("NEQ", histogram, bucket, cst, dim,
										!isgt, isstrict, res);

							break;

						default:
							elog(ERROR, "unknown clause type: %d", clause->type);
					}

					/*
					 * Merge the result into the bitmap, depending on type of
					 * the current clause (AND or OR).
					 */
					if (is_or)
					{
						/* OR follows the Max() semantics */
						matches[i] = Max(matches[i], res);
					}
					else
					{
						/* AND follows Min() semantics */
						matches[i] = Min(matches[i], res);
					}
				}
			}
		}
		else if (IsA(clause, NullTest))
		{
			NullTest   *expr = (NullTest *) clause;
			Var		   *var = (Var *) (expr->arg);

			/* lookup index of attribute in the statistics */
			int			idx = bms_member_index(stakeys, var->varattno);

			/*
			 * Walk through the buckets and evaluate the current clause. We
			 * can skip items that were already ruled out, and terminate if
			 * there are no remaining buckets that might possibly match.
			 */
			for (i = 0; i < histogram->nbuckets; i++)
			{
				char		match = STATS_MATCH_NONE;
				MVBucket   *bucket = histogram->buckets[i];

				/*
				 * Skip buckets that were already eliminated - this is
				 * impotant considering how we update the info (we only lower
				 * the match)
				 */
				if ((!is_or) && (matches[i] == STATS_MATCH_NONE))
					continue;
				else if (is_or && (matches[i] == STATS_MATCH_FULL))
					continue;

				switch (expr->nulltesttype)
				{
					case IS_NULL:
						match = (bucket->nullsonly[idx]) ? STATS_MATCH_FULL : match;
						break;

					case IS_NOT_NULL:
						match = (!bucket->nullsonly[idx]) ? STATS_MATCH_FULL : match;
						break;
				}

				/* now, update the match bitmap, depending on OR/AND type */
				if (is_or)
					matches[i] = Max(matches[i], match);
				else
					matches[i] = Min(matches[i], match);
			}
		}
		else if (or_clause(clause) || and_clause(clause))
		{
			/*
			 * AND/OR clause, with all sub-clauses compatible with the stats
			 */

			int			i;
			BoolExpr   *bool_clause = ((BoolExpr *) clause);
			List	   *bool_clauses = bool_clause->args;

			/* match/mismatch bitmap for each bucket */
			char	   *bool_matches = NULL;

			Assert(bool_clauses != NIL);
			Assert(list_length(bool_clauses) >= 2);

			/* by default none of the buckets matches the clauses */
			bool_matches = palloc0(sizeof(char) * histogram->nbuckets);

			if (or_clause(clause))
			{
				/* OR clauses assume nothing matches, initially */
				memset(bool_matches, STATS_MATCH_NONE, sizeof(char) * histogram->nbuckets);
			}
			else
			{
				/* AND clauses assume nothing matches, initially */
				memset(bool_matches, STATS_MATCH_FULL, sizeof(char) * histogram->nbuckets);
			}

			/* build the match bitmap for the OR-clauses */
			histogram_update_match_bitmap(root, bool_clauses,
										  stakeys, histogram,
										  bool_matches, or_clause(clause));

			/*
			 * Merge the bitmap produced by histogram_update_match_bitmap into
			 * the current one. We need to consider if we're evaluating AND or
			 * OR condition when merging the results.
			 */
			for (i = 0; i < histogram->nbuckets; i++)
			{
				/* Is this OR or AND clause? */
				if (is_or)
					matches[i] = Max(matches[i], bool_matches[i]);
				else
					matches[i] = Min(matches[i], bool_matches[i]);
			}

			pfree(bool_matches);

		}
		else if (not_clause(clause))
		{
			/* NOT clause, with all subclauses compatible */

			int			i;
			BoolExpr   *not_clause = ((BoolExpr *) clause);
			List	   *not_args = not_clause->args;

			/* match/mismatch bitmap for each MCV item */
			char	   *not_matches = NULL;

			Assert(not_args != NIL);
			Assert(list_length(not_args) == 1);

			/* by default none of the MCV items matches the clauses */
			not_matches = palloc0(sizeof(char) * histogram->nbuckets);

			/* NOT clauses assume nothing matches, initially */
			memset(not_matches, STATS_MATCH_FULL, sizeof(char) * histogram->nbuckets);

			/* build the match bitmap for the OR-clauses */
			histogram_update_match_bitmap(root, not_args,
										  stakeys, histogram,
										  not_matches, false);

			/*
			 * Merge the bitmap produced by histogram_update_match_bitmap into
			 * the current one.
			 *
			 * This is similar to what mcv_update_match_bitmap does, but we
			 * need to be a tad more careful here, as histograms have three
			 * possible values - MATCH_FULL, MATCH_PARTIAL and MATCH_NONE.
			 */
			for (i = 0; i < histogram->nbuckets; i++)
			{
				/*
				 * When handling a NOT clause, invert the result before
				 * merging it into the global result. We don't care about
				 * partial matches here (those invert to partial).
				 */
				if (not_matches[i] == STATS_MATCH_NONE)
					not_matches[i] = STATS_MATCH_FULL;
				else if (not_matches[i] == STATS_MATCH_FULL)
					not_matches[i] = STATS_MATCH_NONE;

				/* Is this OR or AND clause? */
				if (is_or)
					matches[i] = Max(matches[i], not_matches[i]);
				else
					matches[i] = Min(matches[i], not_matches[i]);
			}

			pfree(not_matches);
		}
		else if (IsA(clause, Var))
		{
			/* Var (has to be a boolean Var, possibly from below NOT) */

			Var		   *var = (Var *) (clause);

			/* match the attribute to a dimension of the statistic */
			int			idx = bms_member_index(stakeys, var->varattno);

			Assert(var->vartype == BOOLOID);

			/*
			 * Walk through the buckets and evaluate the current clause.
			 */
			for (i = 0; i < histogram->nbuckets; i++)
			{
				MVBucket   *bucket = histogram->buckets[i];
				char		match = STATS_MATCH_NONE;

				/*
				 * If the bucket is NULL, it's a mismatch. Otherwise check if
				 * lower/upper boundaries match and choose partial/full match
				 * accordingly.
				 */
				if (!bucket->nullsonly[idx])
				{
					int			minidx = bucket->min[idx];
					int			maxidx = bucket->max[idx];

					bool		minval = DatumGetBool(histogram->values[idx][minidx]);
					bool		maxval = DatumGetBool(histogram->values[idx][maxidx]);

					/*
					 * When both values are the same, we don't care about the
					 * inclusiveness - the whole bucket either matches or
					 * mismatches. The mismatch is default.
					 */
					if (minval == maxval)
						match = (minval) ? STATS_MATCH_FULL : STATS_MATCH_NONE;
					else
					{
						/*
						 * Exactly one of the values is true. To get a match,
						 * we need the 'false' boundary to not be inclusive.
						 */
						if ((minval && !bucket->max_inclusive[idx]) ||
							(maxval && !bucket->min_inclusive[idx]))
							match = STATS_MATCH_FULL;

						/*
						 * When both are inclusive, it's a partial match.
						 *
						 * XXX We can't get two excluded boundary values here.
						 * Boolean only has two possible values, and the
						 * bucket has to contain at least one of them.
						 */
						if (bucket->min_inclusive[idx] == bucket->max_inclusive[idx])
							match = STATS_MATCH_PARTIAL;
					}
				}

				/* now, update the match bitmap, depending on OR/AND type */
				if (is_or)
					matches[i] = Max(matches[i], match);
				else
					matches[i] = Min(matches[i], match);
			}
		}
		else
			elog(ERROR, "unknown clause type: %d", clause->type);
	}

	/* free the call cache */
	pfree(eqcache);
	pfree(ltcache);
}

/*
 * Estimate selectivity of clauses using a histogram.
 *
 * If there's no histogram for the stats, the function returns 0.0.
 *
 * The general idea of this method is similar to how MCV lists are
 * processed, except that this introduces the concept of a partial
 * match (MCV only works with full match / mismatch).
 *
 * The algorithm works like this:
 *
 *	 1) mark all buckets as 'full match'
 *	 2) walk through all the clauses
 *	 3) for a particular clause, walk through all the buckets
 *	 4) skip buckets that are already 'no match'
 *	 5) check clause for buckets that still match (at least partially)
 *	 6) sum frequencies for buckets to get selectivity
 *
 * Unlike MCV lists, histograms have a concept of a partial match. In
 * that case we use 1/2 the bucket, to minimize the average error. The
 * MV histograms are usually less detailed than the per-column ones,
 * meaning the sum is often quite high (thanks to combining a lot of
 * "partially hit" buckets).
 *
 * Maybe we could use per-bucket information with number of distinct
 * values it contains (for each dimension), and then use that to correct
 * the estimate (so with 10 distinct values, we'd use 1/10 of the bucket
 * frequency). We might also scale the value depending on the actual
 * ndistinct estimate (not just the values observed in the sample).
 *
 * Another option would be to multiply the selectivities, i.e. if we get
 * 'partial match' for a bucket for multiple conditions, we might use
 * 0.5^k (where k is the number of conditions), instead of 0.5. This
 * probably does not minimize the average error, though.
 *
 * TODO: This might use a similar shortcut to MCV lists - count buckets
 * marked as partial/full match, and terminate once this drop to 0.
 * Not sure if it's really worth it - for MCV lists a situation like
 * this is not uncommon, but for histograms it's not that clear.
 */
Selectivity
histogram_clauselist_selectivity(PlannerInfo *root, StatisticExtInfo *stat,
								 List *clauses, int varRelid,
								 JoinType jointype, SpecialJoinInfo *sjinfo,
								 RelOptInfo *rel)
{
	int			i;
	MVHistogram *histogram;
	Selectivity s = 0.0;

	/* match/mismatch bitmap for each MCV item */
	char	   *matches = NULL;

	/* load the histogram stored in the statistics object */
	histogram = statext_histogram_load(stat->statOid);

	/* by default all the histogram buckets match the clauses fully */
	matches = palloc0(sizeof(char) * histogram->nbuckets);
	memset(matches, STATS_MATCH_FULL, sizeof(char) * histogram->nbuckets);

	histogram_update_match_bitmap(root, clauses, stat->keys,
								  histogram, matches, false);

	/* now, walk through the buckets and sum the selectivities */
	for (i = 0; i < histogram->nbuckets; i++)
	{
		if (matches[i] == STATS_MATCH_FULL)
			s += histogram->buckets[i]->frequency;
		else if (matches[i] == STATS_MATCH_PARTIAL)
		{
			/*
			 * Perhaps we could use convert_to_scalar() to compute the
			 * coefficient similarly to ineq_histogram_selectivity(). That is,
			 * we could compute a distance from the boundary values of a
			 * bucket, but it's unclear how to combine those values - we could
			 * multiply them, but that pretty much means we're assuming
			 * independence at the bucket level. Which is somewhat
			 * contradictory to the whole purpose.
			 */
			s += 0.5 * histogram->buckets[i]->frequency;
		}
	}

	return s;
}
