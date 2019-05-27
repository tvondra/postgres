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
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "statistics/extended_stats_internal.h"
#include "statistics/statistics.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


/*
 * Multivariate histograms
 */
typedef struct MVBucketBuild
{
	/* Frequencies of this bucket. */
	float		frequency;

	/*
	 * Information about dimensions being NULL-only. Not yet used.
	 */
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

typedef struct MVHistogramBuild
{
	int32		vl_len_;		/* unused: ensure same alignment as
								 * MVHistogram for serialization */
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of histogram (BASIC) */
	uint32		nbuckets;		/* number of buckets (buckets array) */
	uint32		ndimensions;	/* number of dimensions */
	Oid			types[STATS_MAX_DIMENSIONS];	/* OIDs of data types */
	MVBucketBuild **buckets;	/* array of buckets */
}			MVHistogramBuild;

static MVBucketBuild * create_initial_ext_bucket(int numrows, HeapTuple *rows,
												 Bitmapset *attrs, VacAttrStats **stats);

static MVBucketBuild * select_bucket_to_partition(int nbuckets, MVBucketBuild * *buckets);

static MVBucketBuild * partition_bucket(MVBucketBuild * bucket, Bitmapset *attrs,
										VacAttrStats **stats,
										int *ndistvalues, Datum **distvalues);

static MVBucketBuild * copy_ext_bucket(MVBucketBuild * bucket, uint32 ndimensions);

static void update_bucket_ndistinct(MVBucketBuild * bucket, Bitmapset *attrs,
						VacAttrStats **stats);

static void update_dimension_ndistinct(MVBucketBuild * bucket, int dimension,
						   Bitmapset *attrs, VacAttrStats **stats,
						   bool update_boundaries);

static void create_null_buckets(MVHistogramBuild * histogram, int bucket_idx,
					Bitmapset *attrs, VacAttrStats **stats);

static Datum *build_ndistinct(int numrows, HeapTuple *rows, Bitmapset *attrs,
				VacAttrStats **stats, int i, int *nvals);

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
 * XXX We might save a bit more space by using proper bitmaps instead of
 * boolean arrays.
 */
#define BUCKET_SIZE(ndims)	\
	MAXALIGN((ndims) * (4 * sizeof(uint16) + 3 * sizeof(bool)) + sizeof(float))

/*
 * Macros for convenient access to parts of a serialized bucket.
 */
#define BUCKET_FREQUENCY(b)		(*(float *) (b))
#define BUCKET_MIN_INCL(b,n)	((bool *) ((b) + sizeof(float)))
#define BUCKET_MAX_INCL(b,n)	(BUCKET_MIN_INCL(b,n) + (n))
#define BUCKET_NULLS_ONLY(b,n)	(BUCKET_MAX_INCL(b,n) + (n))
#define BUCKET_MIN_INDEXES(b,n) ((uint16 *) (BUCKET_NULLS_ONLY(b,n) + (n)))
#define BUCKET_MAX_INDEXES(b,n) ((BUCKET_MIN_INDEXES(b,n) + (n)))

/*
 * Used to compute size of serialized histogram representation.
 */
#define MinSizeOfMVHistogram		\
	(VARHDRSZ + sizeof(uint32) * 3 + sizeof(AttrNumber))

#define SizeOfMVHistogram(ndims,nbuckets)	\
	(MAXALIGN(MinSizeOfMVHistogram + sizeof(Oid) * (ndims)) + \
	 MAXALIGN((ndims) * sizeof(DimensionInfo)) + \
	 MAXALIGN((nbuckets) * BUCKET_SIZE(ndims)))

/*
 * Minimal number of rows per bucket (can't split smaller buckets).
 *
 * XXX The single-column statistics (std_typanalyze) pretty much says we
 * need 300 rows per bucket. Should we use the same value here?
 */
#define MIN_BUCKET_ROWS			10

static void
AssertIsAligned(const void *base, const void *ptr)
{
	Assert(((char *) ptr - (char *) base) == MAXALIGN((char *) ptr - (char *) base));
}

/*
 * Represents match info for a histogram bucket.
 */
typedef struct bucket_match
{
	bool		match;		/* true/false */
	double		fraction;	/* fraction of bucket */
} bucket_match;

/*
 * Builds a multivariate histogram from the set of sampled rows.
 *
 * The build algorithm is iterative - initially a single bucket containing all
 * sample rows is formed, and then repeatedly split into smaller buckets. In
 * each round the largest bucket is split into two smaller ones.
 *
 * The criteria for selecting the largest bucket (and the dimension for the
 * split) needs to be elaborate enough to produce buckets of roughly the same
 * size, and also regular shape (not very narrow in just one dimension).
 *
 * The current algorithm works like this:
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
 * See the discussion at select_bucket_to_partition and partition_bucket for
 * more details about the algorithm.
 *
 * The function does not update the interan pointers, hence the histogram
 * is suitable only for storing. Before using it for estimation, it needs
 * to go through statext_histogram_deserialize() first.
 */
MVHistogram *
statext_histogram_build(int numrows, HeapTuple *rows, Bitmapset *attrs,
						VacAttrStats **stats, int numrows_total)
{
	int			i;
	int			numattrs = bms_num_members(attrs);

	int		   *ndistvalues;
	Datum	  **distvalues;

	MVHistogramBuild *histogram;
	HeapTuple  *rows_copy;

	/* not supposed to build of too few or too many columns */
	Assert((numattrs >= 2) && (numattrs <= STATS_MAX_DIMENSIONS));

	/* we need to make a copy of the row array, as we'll modify it */
	rows_copy = (HeapTuple *) palloc0(numrows * sizeof(HeapTuple));
	memcpy(rows_copy, rows, sizeof(HeapTuple) * numrows);

	/* build the histogram header */

	histogram = (MVHistogramBuild *) palloc0(sizeof(MVHistogramBuild));

	histogram->magic = STATS_HIST_MAGIC;
	histogram->type = STATS_HIST_TYPE_BASIC;
	histogram->ndimensions = numattrs;
	histogram->nbuckets = 1;	/* initially just a single bucket */

	/*
	 * Allocate space for maximum number of buckets (better than repeatedly
	 * doing repalloc for short-lived objects).
	 */
	histogram->buckets
		= (MVBucketBuild * *) palloc0(STATS_HIST_MAX_BUCKETS * sizeof(MVBucketBuild));

	/* Create the initial bucket, covering all sampled rows */
	histogram->buckets[0]
		= create_initial_ext_bucket(numrows, rows_copy, attrs, stats);

	/*
	 * Collect info on distinct values in each dimension (used later to pick
	 * dimension to partition).
	 */
	ndistvalues = (int *) palloc0(sizeof(int) * numattrs);
	distvalues = (Datum **) palloc0(sizeof(Datum *) * numattrs);

	for (i = 0; i < numattrs; i++)
		distvalues[i] = build_ndistinct(numrows, rows, attrs, stats, i,
										&ndistvalues[i]);

	/*
	 * Split the initial bucket into buckets that don't mix NULL and non-NULL
	 * values in a single dimension.
	 *
	 * XXX Maybe this should be happening before the build_ndistinct()?
	 */
	create_null_buckets(histogram, 0, attrs, stats);

	/*
	 * Split the buckets into smaller and smaller buckets. The loop will end
	 * when either all buckets are too small (MIN_BUCKET_ROWS), or there are
	 * too many buckets in total (STATS_HIST_MAX_BUCKETS).
	 */
	while (histogram->nbuckets < STATS_HIST_MAX_BUCKETS)
	{
		MVBucketBuild *bucket = select_bucket_to_partition(histogram->nbuckets,
														   histogram->buckets);

		/* no bucket eligible for partitioning */
		if (bucket == NULL)
			break;

		/* we modify the bucket in-place and add one new bucket */
		histogram->buckets[histogram->nbuckets++]
			= partition_bucket(bucket, attrs, stats, ndistvalues, distvalues);
	}

	/* Finalize the histogram build - compute bucket frequencies etc. */
	for (i = 0; i < histogram->nbuckets; i++)
	{
		/*
		 * The frequency has to be computed from the whole sample, in case
		 * some of the rows were filtered out in the MCV build.
		 */
		histogram->buckets[i]->frequency
			= (histogram->buckets[i]->numrows * 1.0) / numrows_total;
	}

	return serialize_histogram(histogram, stats);
}

/*
 * build_ndistinct
 *		build array of ndistinct values in a particular column, count them
 *
 */
static Datum *
build_ndistinct(int numrows, HeapTuple *rows, Bitmapset *attrs,
				VacAttrStats **stats, int i, int *nvals)
{
	int			j;
	int			nvalues,
				ndistinct;
	Datum	   *values,
			   *distvalues;
	AttrNumber *attnums;
	int			numattrs;

	TypeCacheEntry *type;
	SortSupportData ssup;

	type = lookup_type_cache(stats[i]->attrtypid, TYPECACHE_LT_OPR);

	/* initialize sort support, etc. */
	memset(&ssup, 0, sizeof(ssup));
	ssup.ssup_cxt = CurrentMemoryContext;

	/* We always use the default collation for statistics */
	ssup.ssup_collation = DEFAULT_COLLATION_OID;
	ssup.ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(type->lt_opr, &ssup);

	nvalues = 0;
	values = (Datum *) palloc0(sizeof(Datum) * numrows);

	attnums = build_attnums_array(attrs, &numattrs);

	/* collect values from the sample rows, ignore NULLs */
	for (j = 0; j < numrows; j++)
	{
		Datum		value;
		bool		isnull;

		/*
		 * remember the index of the sample row, to make the partitioning
		 * simpler
		 */
		value = heap_getattr(rows[j], attnums[i],
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
 * Serialize the MV histogram into a bytea value. The basic algorithm is quite
 * simple, and mostly mimincs the MCV serialization:
 *
 * (1) perform deduplication for each attribute (separately)
 *
 *   (a) collect all (non-NULL) attribute values from all buckets
 *   (b) sort the data (using 'lt' from VacAttrStats)
 *   (c) remove duplicate values from the array
 *
 * (2) serialize the arrays into a bytea value
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
 * a longer type (instead of using an array of bool items).
 */
static MVHistogram *
serialize_histogram(MVHistogramBuild * histogram, VacAttrStats **stats)
{
	int			dim,
				i;
	Size		total_length = 0;

	/* serialized items (indexes into arrays, etc.) */
	char	   *raw;
	char	   *ptr;

	DimensionInfo *info;
	SortSupport ssup;

	int			nbuckets = histogram->nbuckets;
	int			ndims = histogram->ndimensions;

	/* allocated for serialized bucket data */
	int			bucketsize = BUCKET_SIZE(ndims);
	char	   *bucket = palloc0(bucketsize);

	/* values per dimension (and number of non-NULL values) */
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
		histogram->types[dim] = stats[dim]->attrtypid;

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
		 * Walk through the array and eliminate duplicitate values, but keep
		 * the ordering (so that we can do bsearch later). We know there's at
		 * least 1 item, so we can skip the first element.
		 */
		count = 1;				/* number of deduplicated items */
		for (i = 1; i < counts[dim]; i++)
		{
			/* if it's different from the previous value, we need to keep it */
			if (compare_datums_simple(values[dim][i - 1], values[dim][i], &ssup[dim]) != 0)
			{
				/* XXX: not needed if (count == j) */
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
			/* byval or byref, but with fixed length (name, tid, ...) */
			info[dim].nbytes = info[dim].nvalues * info[dim].typlen;
		else if (info[dim].typlen == -1)
		{
			/* varlena, so just use VARSIZE_ANY */
			for (i = 0; i < info[dim].nvalues; i++)
			{
				Size	len;

				values[dim][i] = PointerGetDatum(PG_DETOAST_DATUM(values[dim][i]));

				len = VARSIZE_ANY(values[dim][i]);
				info[dim].nbytes += MAXALIGN(len);
			}
		}
		else if (info[dim].typlen == -2)
		{
			/* cstring, so simply strlen */
			for (i = 0; i < info[dim].nvalues; i++)
			{
				Size	len;

				/* c-strings include terminator, so +1 byte */
				values[dim][i] = PointerGetDatum(PG_DETOAST_DATUM(values[dim][i]));

				len = strlen(DatumGetCString(values[dim][i])) + 1;
				info[dim].nbytes += MAXALIGN(len);
			}
		}

		/* we know (count > 0) so there must be some data */
		Assert(info[dim].nbytes > 0);
	}

	/*
	 * Now we can finally compute how much space we'll actually need for the
	 * whole serialized histogram (varlena header, histogram header, dimension
	 * info for each attribute, deduplicated values and buckets).
	 *
	 * The header fields are copied one by one, so that we don't need any
	 * explicit alignment (we copy them while deserializing). All fields
	 * after this need to be properly aligned, for direct access.
	 */
	total_length = MAXALIGN(VARHDRSZ + (3 * sizeof(uint32))
			+ sizeof(AttrNumber) + (ndims * sizeof(Oid)));

	/* dimension info (properly aligned) */
	total_length += MAXALIGN(ndims * sizeof(DimensionInfo));

	/* add space for the arrays of deduplicated values */
	for (i = 0; i < ndims; i++)
		total_length += MAXALIGN(info[i].nbytes);

	/*
	 * And finally the buckets (no additional alignment needed, we start
	 * at proper alignment and the bucketsize formula uses MAXALIGN)
	 */
	total_length += (nbuckets * bucketsize);

	/*
	 * Allocate space for the whole serialized histogram (we'll skip bytes,
	 * so we set them to zero to make the result more compressible).
	 */
	raw = palloc0(total_length);
	SET_VARSIZE(raw, total_length);
	ptr = VARDATA(raw);

	/* copy the histogram list header fields, one by one */
	memcpy(ptr, &histogram->magic, sizeof(uint32));
	ptr += sizeof(uint32);

	memcpy(ptr, &histogram->type, sizeof(uint32));
	ptr += sizeof(uint32);

	memcpy(ptr, &histogram->nbuckets, sizeof(uint32));
	ptr += sizeof(uint32);

	memcpy(ptr, &histogram->ndimensions, sizeof(AttrNumber));
	ptr += sizeof(AttrNumber);

	memcpy(ptr, histogram->types, sizeof(Oid) * ndims);
	ptr += (sizeof(Oid) * ndims);

	/* the header may not be exactly aligned, so make sure it is */
	ptr = raw + MAXALIGN(ptr - raw);

	/* store information about the attributes */
	memcpy(ptr, info, sizeof(DimensionInfo) * ndims);
	ptr += MAXALIGN(sizeof(DimensionInfo) * ndims);

	/* serialize the deduplicated values for all attributes */
	for (dim = 0; dim < ndims; dim++)
	{
		/* remember the starting point for Asserts later */
		char	   *start PG_USED_FOR_ASSERTS_ONLY = ptr;

		for (i = 0; i < info[dim].nvalues; i++)
		{
			Datum		value = values[dim][i];

			if (info[dim].typbyval) /* passed by value */
			{
				Datum		tmp;

				/*
				 * For values passed by value, we need to copy just the
				 * significant bytes - we can't use memcpy directly, as that
				 * assumes little endian behavior.  store_att_byval does
				 * almost what we need, but it requires properly aligned
				 * buffer - the output buffer does not guarantee that. So we
				 * simply use a static Datum variable (which guarantees proper
				 * alignment), and then copy the value from it.
				 */
				store_att_byval(&tmp, value, info[dim].typlen);

				memcpy(ptr, &tmp, info[dim].typlen);
				ptr += info[dim].typlen;
			}
			else if (info[dim].typlen > 0)	/* pased by reference */
			{
				/* no special alignment needed, treated as char array */
				memcpy(ptr, DatumGetPointer(value), info[dim].typlen);
				ptr += info[dim].typlen;
			}
			else if (info[dim].typlen == -1)	/* varlena */
			{
				int			len = VARSIZE_ANY(value);

				memcpy(ptr, DatumGetPointer(value), len);
				ptr += MAXALIGN(len);
			}
			else if (info[dim].typlen == -2)	/* cstring */
			{
				Size		len = strlen(DatumGetCString(value)) + 1;	/* terminator */

				memcpy(ptr, DatumGetCString(value), len);
				ptr += MAXALIGN(len);
			}

			/* no underflows or overflows */
			Assert((ptr > start) && ((ptr - start) <= info[dim].nbytes));
		}

		/* we should get exactly nbytes of data for this dimension */
		Assert((ptr - start) == info[dim].nbytes);

		/* make sure the pointer is aligned correctly after each dimension */
		ptr = raw + MAXALIGN(ptr - raw);
	}

	/* finally serialize the buckets, with uint16 indexes instead of the values */
	for (i = 0; i < nbuckets; i++)
	{
		MVBucketBuild *b = histogram->buckets[i];

		/* don't write beyond the allocated space */
		Assert(ptr <= raw + total_length - bucketsize);

		/* reset the values for each item */
		memset(bucket, 0, bucketsize);

		BUCKET_FREQUENCY(bucket) = b->frequency;

		for (dim = 0; dim < ndims; dim++)
		{
			/* do the lookup only for non-NULL values */
			if (!b->nullsonly[dim])
			{
				Datum	   *v = NULL;

				/* min boundary */
				v = (Datum *) bsearch_arg(&b->min[dim],
										  values[dim], info[dim].nvalues, sizeof(Datum),
										  compare_scalars_simple, &ssup[dim]);

				Assert(v != NULL);	/* serialization or deduplication error */

				/* compute index within the array */
				BUCKET_MIN_INDEXES(bucket, ndims)[dim] = (uint16) (v - values[dim]);

				Assert(BUCKET_MIN_INDEXES(bucket, ndims)[dim] < info[dim].nvalues);

				/* max boundary */
				v = (Datum *) bsearch_arg(&b->max[dim],
										  values[dim], info[dim].nvalues, sizeof(Datum),
										  compare_scalars_simple, &ssup[dim]);

				Assert(v != NULL);	/* serialization or deduplication error */

				/* compute index within the array */
				BUCKET_MAX_INDEXES(bucket, ndims)[dim] = (uint16) (v - values[dim]);

				Assert(BUCKET_MAX_INDEXES(bucket, ndims)[dim] < info[dim].nvalues);

				/*
				 * The array of deduplicated values is sorted, so the lower boundary
				 * should get a lower index.
				 */
				Assert(BUCKET_MIN_INDEXES(bucket, ndims)[dim] <= BUCKET_MAX_INDEXES(bucket, ndims)[dim]);
			}
		}

		/* copy flags (nulls, min/max inclusive) */
		memcpy(BUCKET_NULLS_ONLY(bucket, ndims),
			   b->nullsonly, sizeof(bool) * ndims);

		memcpy(BUCKET_MIN_INCL(bucket, ndims),
			   b->min_inclusive, sizeof(bool) * ndims);

		memcpy(BUCKET_MAX_INCL(bucket, ndims),
			   b->max_inclusive, sizeof(bool) * ndims);

		/* copy the item into the array */
		memcpy(ptr, bucket, bucketsize);

		ptr += bucketsize;
	}

	/* at this point we expect to match the total_length exactly */
	Assert((ptr - raw) == total_length);

	/* free the values/counts arrays here */
	pfree(bucket);
	pfree(counts);
	pfree(info);
	pfree(ssup);

	for (dim = 0; dim < ndims; dim++)
		pfree(values[dim]);

	return (MVHistogram *) raw;
}

/*
 * Reads serialized histogram into MVHistogram structure.
 *
 * Returns histogram in a partially-serialized form (keeps the boundary values
 * deduplicated, so that it's possible to optimize the estimation part by
 * caching function call results across buckets etc.).
 */
MVHistogram *
statext_histogram_deserialize(bytea *data)
{
	int			dim,
				i;

	Size		expected_size;
	char	   *tmp = NULL;

	MVHistogram *histogram;
	char	   *raw;
	DimensionInfo *info;

	int			nbuckets;
	int			ndims;
	int			bucketsize;

	/* temporary deserialization buffer */
	int			histlen;
	char	   *ptr;

	/* buffer used for the result */
	Size		datalen;
	char	   *dataptr;
	char	   *boolptr;
	char	   *indexptr;


	if (data == NULL)
		return NULL;

	/*
	 * We can't possibly deserialize a histogram if there's not even a complete
	 * header. We need an explicit formula here, because we serialize the
	 * header fields one by one, so we need to ignore struct alignment.
	 */
	if (VARSIZE_ANY(data) < MinSizeOfMVHistogram)
		elog(ERROR, "invalid histogram size %zd (expected at least %zu)",
			 VARSIZE_ANY(data), MinSizeOfMVHistogram);

	/* read the histogram header */
	histogram = (MVHistogram *) palloc0(offsetof(MVHistogram, buckets));

	/* pointer to the data part (skip the varlena header) */
	ptr = VARDATA_ANY(data);
	raw = (char *) data;

	/* get the header and perform further sanity checks */
	memcpy(&histogram->magic, ptr, sizeof(uint32));
	ptr += sizeof(uint32);

	memcpy(&histogram->type, ptr, sizeof(uint32));
	ptr += sizeof(uint32);

	memcpy(&histogram->nbuckets, ptr, sizeof(uint32));
	ptr += sizeof(uint32);

	memcpy(&histogram->ndimensions, ptr, sizeof(AttrNumber));
	ptr += sizeof(AttrNumber);

	if (histogram->magic != STATS_HIST_MAGIC)
		elog(ERROR, "invalid histogram magic %u (expected %u)",
			 histogram->magic, STATS_HIST_MAGIC);

	if (histogram->type != STATS_HIST_TYPE_BASIC)
		elog(ERROR, "invalid histogram type %u (expected %u)",
			 histogram->type, STATS_HIST_TYPE_BASIC);

	if (histogram->ndimensions == 0)
		elog(ERROR, "invalid zero-length dimension array in histogram");
	else if (histogram->ndimensions > STATS_MAX_DIMENSIONS)
		elog(ERROR, "invalid length (%u) dimension array in histogram",
			 histogram->ndimensions);

	if (histogram->nbuckets == 0)
		elog(ERROR, "invalid zero-length bucket array in histogram");
	else if (histogram->nbuckets > STATS_HIST_MAX_BUCKETS)
		elog(ERROR, "invalid length (%u) bucket array in histogram",
			 histogram->nbuckets);

	nbuckets = histogram->nbuckets;
	ndims = histogram->ndimensions;
	bucketsize = BUCKET_SIZE(ndims);

	/*
	 * Check amount of data including DimensionInfo for all dimensions and
	 * also the serialized buckets (including uint16 indexes). Also, walk
	 * through the dimension information and add it to the sum.
	 */
	expected_size = SizeOfMVHistogram(ndims, nbuckets);

	/*
	 * Check that we have at least the dimension and info records, along with
	 * the items. We don't know the size of the serialized values yet. We need
	 * to do this check first, before accessing the dimension info.
	 */
	if (VARSIZE_ANY(data) < expected_size)
		elog(ERROR, "invalid histogram size %zd (expected %zu)",
			 VARSIZE_ANY(data), expected_size);

	/* Now copy the array of type Oids. */
	memcpy(histogram->types, ptr, sizeof(Oid) * ndims);
	ptr += (sizeof(Oid) * ndims);

	/* ensure alignment of the pointer (after the header fields) */
	ptr = raw + MAXALIGN(ptr - raw);

	/* Now it's safe to access the dimension info. */
	info = (DimensionInfo *) ptr;
	ptr += MAXALIGN(ndims * sizeof(DimensionInfo));

	/* account for the value arrays */
	for (dim = 0; dim < ndims; dim++)
	{
		/*
		 * XXX I wonder if we can/should rely on asserts here. Maybe those
		 * checks should be done every time?
		 */
		Assert(info[dim].nvalues >= 0);
		Assert(info[dim].nbytes >= 0);

		expected_size += MAXALIGN(info[dim].nbytes);
	}

	/*
	 * Now we know the total expected MCV size, including all the pieces
	 * (header, dimension info. items and deduplicated data). So do the final
	 * check on size.
	 */
	if (VARSIZE_ANY(data) != expected_size)
		elog(ERROR, "invalid histogram size %zd (expected %zu)",
			 VARSIZE_ANY(data), expected_size);

	/*
	 * We need an array of Datum values for each dimension, so that we can
	 * easily translate the uint16 indexes later. We also need a top-level
	 * array of pointers to those per-dimension arrays.
	 *
	 * While allocating the arrays for dimensions, compute how much space we
	 * need for a copy of the by-ref data, as we can't simply point to the
	 * original values (it might go away).
	 */
	datalen = 0;				/* space for by-ref data */
	for (dim = 0; dim < ndims; dim++)
	{
		/* space needed for a copy of data for by-ref types */
		if (!info[dim].typbyval)
			datalen += MAXALIGN(info[dim].nbytes);

		/* mapping uint16 to pointer in the copy (for byref types those
		 * are just pointers elsewhere) */
		datalen += MAXALIGN(info[dim].nvalues * sizeof(Datum));
	}

	/*
	 * Now resize the histogram so that the allocation includes all the data
	 * Allocate space for a copy of the data, as we can't simply reference the
	 * original data - it may disappear while we're still using the histogram,
	 * e.g. due to catcache release. Only needed for by-ref types.
	 */
	histlen = MAXALIGN(sizeof(MVHistogram));

	/* space for buckets */
	histlen += MAXALIGN(sizeof(MVBucket) * nbuckets);

	/*
	 * We don't expand the histogram fully - we keep the arrays of deduplicated
	 * values, and use indexes into it in the buckets. That means we need to store
	 * the deduplicated values, and make them easily accessible. That's what
	 * nvalues/values in MVHistogram are for, and we need to account for that.
	 * The deduplicated values are then serialized elsewhere, these are just
	 * pointers to them.
	 */
	histlen += MAXALIGN(sizeof(int) * ndims);
	histlen += MAXALIGN(sizeof(Datum *) * ndims);

	/*
	 * Arrays of uint16 indexes and isnull/inclusive flags for all buckets.
	 *
	 * Each bucket has min/max boundary flags, and we need ndims elements for each.
	 */
	histlen += 2 * nbuckets * MAXALIGN(sizeof(uint16) * ndims);	/* min/max indexes */
	histlen += 2 * nbuckets * MAXALIGN(sizeof(bool) * ndims);	/* min/max inclusive */
	histlen += nbuckets * MAXALIGN(sizeof(bool) * ndims);	/* nullsonly */

	/* we don't quite need to align this, but it makes some asserts easier */
	histlen += MAXALIGN(datalen);

	/* now resize the deserialized histogram, and compute pointers to parts */
	histogram = repalloc(histogram, histlen);

	/* first byte (aligned) after the histogram header */
	tmp = (char *) histogram + MAXALIGN(sizeof(MVHistogram));

	/* histogram buckets */
	histogram->buckets = (MVBucket *) tmp;
	tmp += MAXALIGN(sizeof(MVBucket) * nbuckets);

	/* deduplicated arrays of per-dimension data (count and values) */
	histogram->nvalues = (int *) tmp;
	tmp += MAXALIGN(sizeof(int) * ndims);

	histogram->values = (Datum **) tmp;
	tmp += MAXALIGN(sizeof(Datum **) * ndims);

	/* set the per-dimension maps (uint16 -> (Datum *)) */
	for (dim = 0; dim < ndims; dim++)
	{
		/*
		 * FIXME can the number of values in a dimension be zero? Perhaps
		 * that may happen for NULL-only dimension, in which case we don't
		 * need to mess with the values.
		 */
		histogram->nvalues[dim] = info[dim].nvalues;
		histogram->values[dim] = (Datum *) tmp;
		tmp += MAXALIGN(histogram->nvalues[dim] * sizeof(Datum));
	}

	/* now the per-bucket fields */

	/*
	 * pointer to the beginning of uint16 indexes (we need two arrays per
	 * bucket, each with ndims elements)
	 */
	indexptr = tmp;
	tmp += 2 * nbuckets * MAXALIGN(sizeof(uint16) * ndims);

	/*
	 * bool flags (we need four arrays per bucket - two for nulls, two for
	 * inclusive flags - each with ndims elements)
	 */
	boolptr = tmp;
	tmp += 3 * nbuckets * MAXALIGN(sizeof(bool) * ndims);

	/*
	 * pointer to the beginning of deduplicated data arrays (we need four
	 * for each bucket min/max and nulls/inclusive)
	 */
	dataptr = tmp;

	/*
	 * Build mapping (index => value) for translating the uint16 indexes to
	 * the deduplicated values.
	 */
	for (dim = 0; dim < ndims; dim++)
	{
		/* remember start position in the input array */
		char	   *start PG_USED_FOR_ASSERTS_ONLY = ptr;

		if (info[dim].typbyval)
		{
			/* for by-val types we simply copy data directly into the mapping */
			for (i = 0; i < info[dim].nvalues; i++)
			{
				Datum		v = 0;

				memcpy(&v, ptr, info[dim].typlen);
				ptr += info[dim].typlen;

				histogram->values[dim][i] = fetch_att(&v, true, info[dim].typlen);

				/* no under/overflow of input array */
				Assert(ptr <= (start + info[dim].nbytes));
			}
		}
		else
		{
			/* for by-ref types we need to also make a copy of the data */

			/* passed by reference, but fixed length (name, tid, ...) */
			if (info[dim].typlen > 0)
			{
				for (i = 0; i < info[dim].nvalues; i++)
				{
					memcpy(dataptr, ptr, info[dim].typlen);
					ptr += info[dim].typlen;

					/* just point into the array */
					histogram->values[dim][i] = PointerGetDatum(dataptr);
					dataptr += info[dim].typlen;
				}
			}
			else if (info[dim].typlen == -1)
			{
				/* varlena */
				for (i = 0; i < info[dim].nvalues; i++)
				{
					Size		len = VARSIZE_ANY(ptr);

					memcpy(dataptr, ptr, len);
					ptr += MAXALIGN(len);

					/* just point into the array */
					histogram->values[dim][i] = PointerGetDatum(dataptr);
					dataptr += MAXALIGN(len);
				}
			}
			else if (info[dim].typlen == -2)
			{
				/* cstring */
				for (i = 0; i < info[dim].nvalues; i++)
				{
					Size		len = (strlen(ptr) + 1);	/* don't forget the \0 */

					memcpy(dataptr, ptr, len);
					ptr += MAXALIGN(len);

					/* just point into the array */
					histogram->values[dim][i] = PointerGetDatum(dataptr);
					dataptr += MAXALIGN(len);
				}
			}

			/* no under/overflow of input array */
			Assert(ptr <= (start + info[dim].nbytes));

			/* no overflow of the output histogram value */
			Assert(dataptr <= ((char *) histogram + histlen));
		}

		/* check we consumed input data for this dimension exactly */
		Assert(ptr == (start + info[dim].nbytes));

		/* ensure proper alignment of the data */
		ptr = raw + MAXALIGN(ptr - raw);
	}

	/* now deserialize the buckets and point them into the varlena values */
	for (i = 0; i < nbuckets; i++)
	{
		MVBucket   *bucket = &histogram->buckets[i];

		/* scalar values can be just directly assigned */
		bucket->frequency = BUCKET_FREQUENCY(ptr);

		/* for arrays we need to allocate + copy */
		bucket->nullsonly = (bool *) boolptr;
		boolptr += MAXALIGN(sizeof(bool) * ndims);
		memcpy(bucket->nullsonly, BUCKET_NULLS_ONLY(ptr, ndims), sizeof(bool) * ndims);

		bucket->min_inclusive = (bool *) boolptr;
		boolptr += MAXALIGN(sizeof(bool) * ndims);
		memcpy(bucket->min_inclusive, BUCKET_MIN_INCL(ptr, ndims), sizeof(bool) * ndims);

		bucket->max_inclusive = (bool *) boolptr;
		boolptr += MAXALIGN(sizeof(bool) * ndims);
		memcpy(bucket->max_inclusive, BUCKET_MAX_INCL(ptr, ndims), sizeof(bool) * ndims);

		bucket->min = (uint16 *) indexptr;
		indexptr += MAXALIGN(sizeof(uint16) * ndims);
		memcpy(bucket->min, BUCKET_MIN_INDEXES(ptr, ndims), sizeof(uint16) * ndims);

		bucket->max = (uint16 *) indexptr;
		indexptr += MAXALIGN(sizeof(uint16) * ndims);
		memcpy(bucket->max, BUCKET_MAX_INDEXES(ptr, ndims), sizeof(uint16) * ndims);

		ptr += bucketsize;

		/* check we're not overflowing the input */
		Assert(ptr <= (char *) raw + VARSIZE_ANY(data));
	}

	/* at this point we expect to match the total_length exactly */
	Assert((ptr - (char *) data) == expected_size);

	return histogram;
}

/*
 * create_initial_ext_bucket
 *		Create an initial bucket, covering all the sampled rows.
 */
static MVBucketBuild *
create_initial_ext_bucket(int numrows, HeapTuple *rows, Bitmapset *attrs,
						  VacAttrStats **stats)
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
	 * for each partition (which we use when choosing which dimension to
	 * split).
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
 *		 values for all the tuples from the sample, not just the boundary values.
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
 *	   CREATE TABLE t AS SELECT i AS a, i AS b
 *						   FROM generate_series(1,1000000) s(i);
 *
 *	   CREATE STATISTICS s1 ON t (a,b) WITH (histogram);
 *
 *	   ANALYZE t;
 *
 * It's a very specific (and perhaps artificial) example, because every bucket
 * always has exactly the same number of distinct values in all dimensions,
 * which makes the partitioning tricky.
 *
 * Then:
 *
 *	   SELECT * FROM t WHERE (a < 100) AND (b < 100);
 *
 * is estimated to return ~120 rows, while in reality it returns only 99.
 *
 *							 QUERY PLAN
 *	   -------------------------------------------------------------
 *		Seq Scan on t  (cost=0.00..19425.00 rows=117 width=8)
 *					   (actual time=0.129..82.776 rows=99 loops=1)
 *		  Filter: ((a < 100) AND (b < 100))
 *		  Rows Removed by Filter: 999901
 *		Planning time: 1.286 ms
 *		Execution time: 82.984 ms
 *	   (5 rows)
 *
 * So this estimate is reasonably close. Let's change the query to OR clause:
 *
 *	   SELECT * FROM t WHERE (a < 100) OR (b < 100);
 *
 *							 QUERY PLAN
 *	   -------------------------------------------------------------
 *		Seq Scan on t  (cost=0.00..19425.00 rows=8100 width=8)
 *					   (actual time=0.145..99.910 rows=99 loops=1)
 *		  Filter: ((a < 100) OR (b < 100))
 *		  Rows Removed by Filter: 999901
 *		Planning time: 1.578 ms
 *		Execution time: 100.132 ms
 *	   (5 rows)
 *
 * That's clearly a much worse estimate. This happens because the histogram
 * contains buckets like this:
 *
 *	   bucket 592  [3 30310] [30134 30593] => [0.000233]
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
 * histograms, i.e. 300 tuples per bucket.
 */
static MVBucketBuild *
select_bucket_to_partition(int nbuckets, MVBucketBuild * *buckets)
{
	int			i;
	int			numrows = 0;
	MVBucketBuild *bucket = NULL;

	for (i = 0; i < nbuckets; i++)
	{
		/* if the number of rows is higher, use this bucket */
		if ((buckets[i]->ndistinct > 2) &&
			(buckets[i]->numrows > numrows) &&
			(buckets[i]->numrows >= MIN_BUCKET_ROWS))
		{
			bucket = buckets[i];
			numrows = buckets[i]->numrows;
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
 * to split dimensions with higher statistics target more frequently).
 */
static MVBucketBuild *
partition_bucket(MVBucketBuild * bucket, Bitmapset *attrs,
				 VacAttrStats **stats,
				 int *ndistvalues, Datum **distvalues)
{
	int			i;
	int			dimension;
	int			numattrs;

	Datum		split_value;
	MVBucketBuild *new_bucket;

	/* needed for sort, when looking for the split value */
	bool		isNull;
	int			nvalues = 0;
	TypeCacheEntry *type;
	ScalarItem *values;
	SortSupportData ssup;
	AttrNumber *attnums;

	int			nrows = 1;		/* number of rows below current value */
	double		delta;

	/* needed when splitting the values */
	HeapTuple  *oldrows = bucket->rows;
	int			oldnrows = bucket->numrows;

	attnums = build_attnums_array(attrs, &numattrs);

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

	for (i = 0; i < numattrs; i++)
	{
		Datum	   *a,
				   *b;

		type = lookup_type_cache(stats[i]->attrtypid, TYPECACHE_LT_OPR);

		/* initialize sort support, etc. */
		memset(&ssup, 0, sizeof(ssup));
		ssup.ssup_cxt = CurrentMemoryContext;

		/* We always use the default collation for statistics */
		ssup.ssup_collation = DEFAULT_COLLATION_OID;
		ssup.ssup_nulls_first = false;

		PrepareSortSupportFromOrderingOp(type->lt_opr, &ssup);

		/* can't split NULL-only dimension */
		if (bucket->nullsonly[i])
			continue;

		/* can't split dimension with a single ndistinct value */
		if (bucket->ndistincts[i] <= 1)
			continue;

		/* search for min boundary in the distinct list */
		a = (Datum *) bsearch_arg(&bucket->min[i],
								  distvalues[i], ndistvalues[i],
								  sizeof(Datum), compare_scalars_simple, &ssup);

		b = (Datum *) bsearch_arg(&bucket->max[i],
								  distvalues[i], ndistvalues[i],
								  sizeof(Datum), compare_scalars_simple, &ssup);

		/* if this dimension is 'larger' then partition by it */
		if (((b - a) * 1.0 / ndistvalues[i]) > delta)
		{
			delta = ((b - a) * 1.0 / ndistvalues[i]);
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
	 * We know there are bucket->ndistincts[dimension] distinct values in this
	 * dimension, and we want to split this into half, so walk through the
	 * array and stop once we see (ndistinct/2) values.
	 *
	 * We always choose the "next" value, i.e. (n/2+1)-th distinct value, and
	 * use it as an exclusive upper boundary (and inclusive lower boundary).
	 *
	 * TODO Maybe we should use "average" of the two middle distinct values
	 * (at least for even distinct counts), but that would require being able
	 * to do an average (which does not work for non-numeric types).
	 *
	 * TODO Another option is to look for a split that'd give about 50% tuples
	 * (not distinct values) in each partition. That might work better when
	 * there are a few very frequent values, and many rare ones.
	 */
	delta = bucket->numrows;
	split_value = values[0].value;

	for (i = 1; i < bucket->numrows; i++)
	{
		if (values[i].value != values[i - 1].value)
		{
			/* are we closer to splitting the bucket in half? */
			if (fabs(i - bucket->numrows / 2.0) < delta)
			{
				/* let's assume we'll use this value for the split */
				split_value = values[i].value;
				delta = fabs(i - bucket->numrows / 2.0);
				nrows = i;
			}
		}
	}

	Assert(nrows > 0);
	Assert(nrows < bucket->numrows);

	/*
	 * create the new bucket as a (incomplete) copy of the one being
	 * partitioned.
	 */
	new_bucket = copy_ext_bucket(bucket, numattrs);

	/*
	 * Do the actual split of the chosen dimension, using the split value as
	 * the upper bound for the existing bucket, and lower bound for the new
	 * one.
	 */
	bucket->max[dimension] = split_value;
	new_bucket->min[dimension] = split_value;

	/*
	 * We also treat only one side of the new boundary as inclusive, in the
	 * bucket where it happens to be the upper boundary. We never set the
	 * min_inclusive[] to false anywhere, but we set it to true anyway.
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
	 * The first nrows should go to the first bucket, the rest should go to
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

	/*
	 * TODO We don't need to do this for the dimension we used for split,
	 * because we know how many distinct values went to each partition.
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
static MVBucketBuild *
copy_ext_bucket(MVBucketBuild * bucket, uint32 ndimensions)
{
	/* TODO allocate as a single piece (including all the fields) */
	MVBucketBuild *new_bucket = (MVBucketBuild *) palloc0(sizeof(MVBucketBuild));

	/*
	 * Copy only the attributes that will stay the same after the split, and
	 * we'll recompute the rest after the split.
	 */

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

	/* allocate and copy the interesting part of the build data */
	new_bucket->ndistincts = (uint32 *) palloc0(ndimensions * sizeof(uint32));

	return new_bucket;
}

/*
 * Counts the number of distinct values in the bucket. This just copies the
 * Datum values into a simple array, and sorts them using memcmp-based
 * comparator. That means it only works for pass-by-value data types (assuming
 * they don't use collations etc.)
 */
static void
update_bucket_ndistinct(MVBucketBuild * bucket, Bitmapset *attrs, VacAttrStats **stats)
{
	int			i;
	int			numattrs;
	int			numrows = bucket->numrows;

	MultiSortSupport mss;
	AttrNumber *attnums;
	SortItem   *items;
	int			nitems;

	attnums = build_attnums_array(attrs, &numattrs);
	mss = multi_sort_init(numattrs);

	/* prepare the sort function for the first dimension */
	for (i = 0; i < numattrs; i++)
	{
		VacAttrStats *colstat = stats[i];
		TypeCacheEntry *type;

		type = lookup_type_cache(colstat->attrtypid, TYPECACHE_LT_OPR);
		if (type->lt_opr == InvalidOid) /* shouldn't happen */
			elog(ERROR, "cache lookup failed for ordering operator for type %u",
				 colstat->attrtypid);

		multi_sort_add_dimension(mss, i, type->lt_opr, type->typcollation);
	}

	/*
	 * build an array of SortItem(s) sorted using the multi-sort support
	 *
	 * XXX This relies on all stats entries pointing to the same tuple
	 * descriptor. Not sure if that might not be the case.
	 */
	items = build_sorted_items(numrows, &nitems, bucket->rows, stats[0]->tupDesc,
							   mss, numattrs, attnums);

	bucket->ndistinct = 1;

	for (i = 1; i < nitems; i++)
		if (multi_sort_compare(&items[i], &items[i - 1], mss) != 0)
			bucket->ndistinct += 1;

	pfree(items);
}

/*
 * Count distinct values per bucket dimension.
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
	AttrNumber *attnums;
	int			numattrs;

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

	attnums = build_attnums_array(attrs, &numattrs);

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
 *		   non-NULL values).
 *
 *	   (2) If all dimensions are well-formed, terminate.
 *
 *	   (3) If the dimension contains only NULL values, but is not marked as
 *		   NULL-only, mark it as NULL-only and run the algorithm again (on
 *		   this bucket).
 *
 *	   (4) If the dimension mixes NULL and non-NULL values, split the bucket
 *		   into two parts - one with NULL values, one with non-NULL values
 *		   (replacing the current one). Then run the algorithm on both buckets.
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
	AttrNumber *attnums;
	int			numattrs;

	/* remember original values from the bucket */
	int			numrows;
	HeapTuple  *oldrows = NULL;

	Assert(bucket_idx < histogram->nbuckets);
	Assert(histogram->ndimensions == bms_num_members(attrs));

	bucket = histogram->buckets[bucket_idx];

	numrows = bucket->numrows;
	oldrows = bucket->rows;

	attnums = build_attnums_array(attrs, &numattrs);

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
	 * recursively (until we run out of dimensions).
	 */
	if (null_count == bucket->numrows)
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
	null_bucket = copy_ext_bucket(bucket, histogram->ndimensions);

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

	/* add the NULL bucket to the histogram */
	histogram->buckets[histogram->nbuckets++] = null_bucket;

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

		bucket = &histogram->buckets[call_cntr];

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

#define HIST_CACHE_FALSE			0x01
#define HIST_CACHE_TRUE				0x03
#define HIST_CACHE_MASK				0x02

/*
 * bucket_contains_value
 *		Decide if the bucket (a range of values in a particular dimension) may
 *		contain the supplied value.
 *
 * The function does not simply return true/false, but a "match level" (none,
 * partial, full), just like other similar functions. In fact, thise function
 * only returns "partial" or "none" levels, as a range can never match exactly
 * a value (we never generate histograms with "collapsed" dimensions).
 *
 * FIXME Should use a better estimate than DEFAULT_EQ_SEL, e.g. derived
 * from ndistinct for the variable. But for histograms we shouldn't really
 * get here, because equalities are handled as conditions (i.e. we'll get
 * here when deciding which buckets match the conditions, but the fraction
 * value does not really matter, we only care about the match flag).
 */
static bool
bucket_contains_value(FmgrInfo ltproc, Datum constvalue,
					  Datum min_value, Datum max_value,
					  int min_index, int max_index,
					  bool min_include, bool max_include,
					  char *callcache, double *fraction)
{
	bool		a,
				b;

	char		min_cached = callcache[min_index];
	char		max_cached = callcache[max_index];

	/*
	 * First some quick checks on equality - if any of the boundaries equals,
	 * we have a partial match (so no need to call the comparator).
	 */
	if (((min_value == constvalue) && (min_include)) ||
		((max_value == constvalue) && (max_include)))
	{
		*fraction = DEFAULT_EQ_SEL;
		return true;
	}

	/* Keep the values 0/1 because of the XOR at the end. */
	a = ((min_cached & HIST_CACHE_MASK) >> 1);
	b = ((max_cached & HIST_CACHE_MASK) >> 1);

	/*
	 * If result for the bucket lower bound not in cache, evaluate the
	 * function and store the result in the cache.
	 */
	if (!min_cached)
	{
		a = DatumGetBool(FunctionCall2Coll(&ltproc,
										   DEFAULT_COLLATION_OID,
										   constvalue, min_value));
		/* remember the result */
		callcache[min_index] = (a) ? HIST_CACHE_TRUE : HIST_CACHE_FALSE;
	}

	/* And do the same for the upper bound. */
	if (!max_cached)
	{
		b = DatumGetBool(FunctionCall2Coll(&ltproc,
										   DEFAULT_COLLATION_OID,
										   constvalue, max_value));
		/* remember the result */
		callcache[max_index] = (b) ? HIST_CACHE_TRUE : HIST_CACHE_FALSE;
	}

	*fraction = (a ^ b) ? DEFAULT_EQ_SEL : 0.0;

	return (a ^ b) ? true : false;
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
 *
 * FIXME Use a better estimate, instead of DEFAULT_INEQ_SEL, i.e. something
 * derived in a way similar to convert_to_scalar.
 */
static bool
bucket_is_smaller_than_value(FmgrInfo opproc, Oid typeoid, Oid colloid,
							 Datum constvalue,
							 Datum min_value, Datum max_value,
							 int min_index, int max_index,
							 bool min_include, bool max_include,
							 char *callcache, bool isgt,
							 double *fraction)
{
	char		min_cached = callcache[min_index];
	char		max_cached = callcache[max_index];

	/* Keep the values 0/1 because of the XOR at the end. */
	bool		a = ((min_cached & HIST_CACHE_MASK) >> 1);
	bool		b = ((max_cached & HIST_CACHE_MASK) >> 1);

	if (!min_cached)
	{
		a = DatumGetBool(FunctionCall2Coll(&opproc,
										   DEFAULT_COLLATION_OID,
										   min_value,
										   constvalue));
		/* remember the result */
		callcache[min_index] = (a) ? HIST_CACHE_TRUE : HIST_CACHE_FALSE;
	}

	if (!max_cached)
	{
		b = DatumGetBool(FunctionCall2Coll(&opproc,
										   DEFAULT_COLLATION_OID,
										   max_value,
										   constvalue));
		/* remember the result */
		callcache[max_index] = (b) ? HIST_CACHE_TRUE : HIST_CACHE_FALSE;
	}

	/*
	 * Now, we need to combine both results into the final answer, and we need
	 * to be careful about the 'isgt' variable which kinda inverts the
	 * meaning.
	 *
	 * First, we handle the case when each boundary returns different results.
	 * In that case the outcome can only be 'partial' match, and the fraction
	 * is computed using convert_to_scalar, just like for 1D histograms.
	 */
	if (a != b)
	{
		double	val, high, low, binfrac;

		if (convert_to_scalar(constvalue, typeoid, colloid, &val,
							  min_value, max_value, typeoid, &low, &high))
		{

			/* shamelessly copied from ineq_histogram_selectivity */
			if (high <= low)
			{
				/* cope if bin boundaries appear identical */
				binfrac = 0.5;
			}
			else if (val <= low)
				binfrac = 0.0;
			else if (val >= high)
				binfrac = 1.0;
			else
			{
				binfrac = (val - low) / (high - low);

				/*
				 * Watch out for the possibility that we got a NaN or
				 * Infinity from the division.  This can happen
				 * despite the previous checks, if for example "low"
				 * is -Infinity.
				 */
				if (isnan(binfrac) ||
					binfrac < 0.0 || binfrac > 1.0)
					binfrac = 0.5;
			}
		}
		else
			binfrac = 0.5;

		*fraction = (isgt) ? binfrac : (1-binfrac);
		return true;
	}

	/*
	 * When the results are the same, then it depends on the 'isgt' value.
	 * There are four options:
	 *
	 * isgt=false a=b=true	=> full match isgt=false a=b=false => empty
	 * isgt=true  a=b=true	=> empty isgt=true	a=b=false => full match
	 *
	 * We'll cheat a bit, because we know that (a=b) so we'll use just one of
	 * them.
	 */
	if (isgt)
	{
		*fraction = (!a) ? 1.0 : 0.0;
		return (!a);
	}
	else
	{
		*fraction = (a) ? 1.0 : 0.0;
		return a;
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
static bucket_match *
histogram_get_match_bitmap(PlannerInfo *root,
						   List *clauses, Bitmapset *stakeys,
						   MVHistogram *histogram, bool is_or)
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
	char	   *callcache = palloc(histogram->nbuckets);
	Size		len;
	bucket_match *matches;

	Assert(histogram != NULL);
	Assert(histogram->nbuckets > 0);

	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);

	/* size of the match "bitmap" */
	len = sizeof(bucket_match) * histogram->nbuckets;

	/* by default all the histogram buckets match the clauses fully */
	matches = palloc0(len);

	/*
	 * The default state is different for AND and OR clauses. For OR
	 * clauses we start with 'no matches' while for AND clauses we start
	 * with everything matching.
	 */
	for (i = 0; i < histogram->nbuckets; i++)
	{
		if (is_or)
		{
			matches[i].match = false;
			matches[i].fraction = 0.0;
		}
		else
		{
			matches[i].match = true;
			matches[i].fraction = 1.0;
		}
	}

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

			FmgrInfo	opproc; /* operator */

			fmgr_info(get_opcode(expr->opno), &opproc);

			/* reset the cache (per clause) */
			memset(callcache, 0, histogram->nbuckets);

			ok = (NumRelids(clause) == 1) &&
				(is_pseudo_constant_clause(lsecond(expr->args)) ||
				 (varonleft = false,
				  is_pseudo_constant_clause(linitial(expr->args))));

			if (ok)
			{
				FmgrInfo	ltproc;
				RegProcedure oprrest = get_oprrest(expr->opno);
				TypeCacheEntry *typecache;
				Oid			colloid;
				int			idx;

				Var		   *var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);
				Const	   *cst = (varonleft) ? lsecond(expr->args) : linitial(expr->args);
				bool		isgt = (!varonleft);

				/* strip binary-compatible relabeling */
				if (IsA(var, RelabelType))
					var = (Var *) ((RelabelType *) var)->arg;

				/* lookup dimension for the attribute */
				idx = bms_member_index(stakeys, var->varattno);

				typecache = lookup_type_cache(var->vartype, TYPECACHE_LT_OPR);
				fmgr_info(get_opcode(typecache->lt_opr), &ltproc);
				colloid = typecache->typcollation;

				/*
				 * Check this for all buckets that still have "true" in the
				 * bitmap
				 *
				 * We already know the clauses use suitable operators (because
				 * that's how we filtered them).
				 */
				for (i = 0; i < histogram->nbuckets; i++)
				{
					bool		res;
					double		fraction;

					MVBucket   *bucket = &histogram->buckets[i];

					/* histogram boundaries */
					Datum		minval,
								maxval;
					bool		mininclude,
								maxinclude;
					int			minidx,
								maxidx;

					/*
					 * For AND-lists, we can also mark NULL buckets as 'no
					 * match' (and then skip them). For OR-lists this is not
					 * possible.
					 */
					if ((!is_or) && bucket->nullsonly[idx])
						matches[i].match = false;

					/*
					 * XXX There used to be logic to skip buckets that can't
					 * possibly match, depending on the is_or flag (either
					 * fully matching or elimated). Once we abandoned the
					 * concept of NONE/PARTIAL/FULL matches and switched to
					 * a bool flag + fraction that does not seem possible.
					 * But maybe we can make it work somehow?
					 */

					/* lookup the values and cache of function calls */
					minidx = bucket->min[idx];
					maxidx = bucket->max[idx];

					minval = histogram->values[idx][bucket->min[idx]];
					maxval = histogram->values[idx][bucket->max[idx]];

					mininclude = bucket->min_inclusive[idx];
					maxinclude = bucket->max_inclusive[idx];

					/*
					 * If it's not a "<" or ">" or "=" operator, just ignore
					 * the clause. Otherwise note the relid and attnum for the
					 * variable.
					 *
					 * TODO I'm really unsure the handling of 'isgt' flag
					 * (that is, clauses with reverse order of
					 * variable/constant) is correct. I wouldn't be surprised
					 * if there was some mixup. Using the lt/gt operators
					 * instead of messing with the opproc could make it
					 * simpler. It would however be using a different operator
					 * than the query, although it's not any shadier than
					 * using the selectivity function as is done currently.
					 */
					switch (oprrest)
					{
						case F_SCALARLTSEL: /* Var < Const */
						case F_SCALARLESEL: /* Var <= Const */
						case F_SCALARGTSEL: /* Var > Const */
						case F_SCALARGESEL: /* Var >= Const */

							res = bucket_is_smaller_than_value(opproc, var->vartype, colloid,
															   cst->constvalue,
															   minval, maxval,
															   minidx, maxidx,
															   mininclude, maxinclude,
															   callcache, isgt, &fraction);

							break;

						case F_EQSEL:
						case F_NEQSEL:

							/*
							 * We only check whether the value is within the
							 * bucket, using the lt operator, and we also
							 * check for equality with the boundaries.
							 */

							res = bucket_contains_value(ltproc, cst->constvalue,
														minval, maxval,
														minidx, maxidx,
														mininclude, maxinclude,
														callcache, &fraction);

							break;

						default:
							elog(ERROR, "unexpected selectivity procedure");
					}

					/*
					 * Merge the result into the bitmap, depending on type
					 * of the current clause (AND or OR).
					 */
					if (is_or)
					{
						Selectivity s1, s2;

						/* OR follows the Max() semantics */
						matches[i].match |= res;

						/*
						 * Selectivities for an OR clause are combined as s1+s2 - s1*s2
						 * to account for the probable overlap of selected tuple sets.
						 * This is the same formula as in clause_selectivity, because
						 * the fraction is computed assuming independence (but then we
						 * also apply geometric mean).
						 */
						s1 = matches[i].fraction;
						s2 = fraction;

						matches[i].fraction = s1 + s2 - s1 * s2;

						CLAMP_PROBABILITY(matches[i].fraction);
					}
					else
					{
						/* AND follows Min() semantics */
						matches[i].match &= res;
						matches[i].fraction *= fraction;
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
				char		match = false;
				MVBucket   *bucket = &histogram->buckets[i];

				/*
				 * Skip buckets that were already eliminated - this is
				 * impotant considering how we update the info (we only lower
				 * the match)
				 */
				if ((!is_or) && (!matches[i].match))
					continue;
				else if (is_or && (matches[i].match))
					continue;

				switch (expr->nulltesttype)
				{
					case IS_NULL:
						match = (bucket->nullsonly[idx]) ? true : match;
						break;

					case IS_NOT_NULL:
						match = (!bucket->nullsonly[idx]) ? true : match;
						break;
				}

				/* now, update the match bitmap, depending on OR/AND type */
				if (is_or)
				{
					matches[i].match |= match;
					matches[i].fraction = (match) ? 1.0 : matches[i].fraction;
				}
				else
				{
					matches[i].match &= match;
					matches[i].fraction = (match) ? matches[i].fraction : 0.0;
				}
			}
		}
		else if (is_orclause(clause) || is_andclause(clause))
		{
			/*
			 * AND/OR clause, with all sub-clauses compatible with the stats
			 */

			int			i;
			BoolExpr   *bool_clause = ((BoolExpr *) clause);
			List	   *bool_clauses = bool_clause->args;

			/* match/mismatch bitmap for each bucket */
			bucket_match   *bool_matches = NULL;

			Assert(bool_clauses != NIL);
			Assert(list_length(bool_clauses) >= 2);

			bool_matches = histogram_get_match_bitmap(root, bool_clauses,
													  stakeys, histogram,
													  is_orclause(clause));

			/*
			 * Merge the bitmap produced by histogram_get_match_bitmap into
			 * the current one. We need to consider if we're evaluating AND or
			 * OR condition when merging the results.
			 */
			for (i = 0; i < histogram->nbuckets; i++)
			{
				/* Is this OR or AND clause? */
				if (is_or)
				{
					Selectivity	s1, s2;

					matches[i].match |= bool_matches[i].match;

					/*
					 * Selectivities for an OR clause are combined as s1+s2 - s1*s2
					 * to account for the probable overlap of selected tuple sets.
					 * This is the same formula as in clause_selectivity, because
					 * the fraction is computed assuming independence (but then we
					 * also apply geometric mean).
					 */
					s1 = matches[i].fraction;
					s2 = bool_matches[i].fraction;

					matches[i].fraction = s1 + s2 - s1 * s2;

					CLAMP_PROBABILITY(matches[i].fraction);
				}
				else
				{
					matches[i].match &= bool_matches[i].match;
					matches[i].fraction *= bool_matches[i].fraction;
				}
			}

			pfree(bool_matches);

		}
		else if (is_notclause(clause))
		{
			/* NOT clause, with all subclauses compatible */

			int			i;
			BoolExpr   *not_clause = ((BoolExpr *) clause);
			List	   *not_args = not_clause->args;

			/* match/mismatch bitmap for each MCV item */
			bucket_match   *not_matches = NULL;

			Assert(not_args != NIL);
			Assert(list_length(not_args) == 1);

			/* by default none of the MCV items matches the clauses */
			not_matches = palloc0(sizeof(bucket_match) * histogram->nbuckets);

			/* build the match bitmap for the clause(s) */
			not_matches = histogram_get_match_bitmap(root, not_args,
													 stakeys, histogram,
													 false);

			/*
			 * Merge the bitmap produced by histogram_get_match_bitmap into
			 * the current one.
			 *
			 * This is similar to what mcv_update_match_bitmap does, but we
			 * need to be a tad more careful here, as histograms also track
			 * what fraction of a bucket matches.
			 */
			for (i = 0; i < histogram->nbuckets; i++)
			{
				/*
				 * When handling a NOT clause, invert the result before
				 * merging it into the global result. We don't care about
				 * partial matches here (those invert to partial).
				 */
				not_matches[i].match = (!not_matches[i].match);

				/* Is this OR or AND clause? */
				if (is_or)
				{
					Selectivity s1, s2;

					matches[i].match |= not_matches[i].match;

					/*
					 * Selectivities for an OR clause are combined as s1+s2 - s1*s2
					 * to account for the probable overlap of selected tuple sets.
					 * This is the same formula as in clause_selectivity, because
					 * the fraction is computed assuming independence (but then we
					 * also apply geometric mean).
					 */
					s1 = matches[i].fraction;
					s2 = not_matches[i].fraction;

					matches[i].fraction = s1 + s2 - s1 * s2;

					CLAMP_PROBABILITY(matches[i].fraction);
				}
				else
				{
					matches[i].match &= not_matches[i].match;
					matches[i].fraction *= not_matches[i].fraction;
				}
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
				MVBucket   *bucket = &histogram->buckets[i];
				bool	match = false;
				double	fraction = 0.0;

				/*
				 * If the bucket is NULL, it's a mismatch. Otherwise check
				 * if lower/upper boundaries match and choose partial/full
				 * match accordingly.
				 */
				if (!bucket->nullsonly[idx])
				{
					int		minidx = bucket->min[idx];
					int		maxidx = bucket->max[idx];

					bool 	a = DatumGetBool(histogram->values[idx][minidx]);
					bool	b = DatumGetBool(histogram->values[idx][maxidx]);

					/* How many boundary values match? */
					if (a && b)
					{
						/* both values match - the whole bucket matches */
						match = true;
						fraction = 1.0;
					}
					else if (a || b)
					{
						/* one value matches - assume half the bucket matches */
						match = true;
						fraction = 0.5;
					}
				}

				/* now, update the match bitmap, depending on OR/AND type */
				if (is_or)
				{
					Selectivity	s1, s2;

					matches[i].match |= match;

					/*
					 * Selectivities for an OR clause are combined as s1+s2 - s1*s2
					 * to account for the probable overlap of selected tuple sets.
					 * This is the same formula as in clause_selectivity, because
					 * the fraction is computed assuming independence (but then we
					 * also apply geometric mean).
					 */
					s1 = matches[i].fraction;
					s2 = fraction;

					matches[i].fraction = s1 + s2 - s1 * s2;

					CLAMP_PROBABILITY(matches[i].fraction);
				}
				else
				{
					matches[i].match &= match;
					matches[i].fraction *= fraction;
				}
			}
		}
		else
			elog(ERROR, "unknown clause type: %d", clause->type);
	}

	/* free the call cache */
	pfree(callcache);

	return matches;
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
								 List *clauses, List *conditions,
								 int varRelid, JoinType jointype,
								 SpecialJoinInfo *sjinfo, RelOptInfo *rel)
{
	int			i;
	MVHistogram *histogram;
	Selectivity	s = 0.0;
	Selectivity	total_sel = 0.0;
	int			nclauses;

	/* match/mismatch bitmap clauses and conditions (for each bucket) */
	bucket_match   *matches = NULL;
	bucket_match   *cmatches = NULL;

	nclauses = list_length(clauses);

	/* load the histogram stored in the statistics object */
	histogram = statext_histogram_load(stat->statOid);

	/* build match bitmap for the clauses */
	matches = histogram_get_match_bitmap(root,
										 clauses, stat->keys,
										 histogram, false);

	/* if there are conditions, build a match bitmap for them too */
	if (conditions)
	{
		cmatches = histogram_get_match_bitmap(root,
											  conditions, stat->keys,
											  histogram, false);
	}

	/* now, walk through the buckets and sum the selectivities */
	for (i = 0; i < histogram->nbuckets; i++)
	{
		double fraction;

		/* skip buckets that don't satisfy the conditions */
		if (conditions && (!cmatches[i].match))
			continue;

		/* compute selectivity for buckets matching conditions */
		total_sel += histogram->buckets[i].frequency;

		/* geometric mean of the bucket fraction */
		fraction = pow(matches[i].fraction, 1.0 / nclauses);

		if (matches[i].match)
			s += histogram->buckets[i].frequency * fraction;
	}

	/* conditional selectivity P(clauses|conditions) */
	if (total_sel > 0.0)
		return (s / total_sel);

	return 0.0;
}
