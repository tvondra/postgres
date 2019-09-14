/*
 * brin_bloom.c
 *		Implementation of Bloom opclass for BRIN
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * A BRIN opclass summarizing page range into a bloom filter.
 *
 * Bloom filters allow efficient test whether a given page range contains
 * a particular value. Therefore, if we summarize each page range into a
 * bloom filter, we can easily and cheaply test wheter it containst values
 * we get later.
 *
 * The index only supports equality operator, similarly to hash indexes.
 * BRIN bloom indexes are however much smaller, and support only bitmap
 * scans.
 *
 * Note: Don't confuse this with bloom indexes, implemented in a contrib
 * module. That extension implements an entirely new AM, building a bloom
 * filter on multiple columns in a single row. This opclass works with an
 * existing AM (BRIN) and builds bloom filter on a column.
 *
 *
 * values vs. hashes
 * -----------------
 *
 * The original column values are not used directly, but are first hashed
 * using the regular type-specific hash function, producing a uint32 hash.
 * And this hash value is then added to the summary - either stored as is
 * (in the sorted mode), or hashed again and added to the bloom filter.
 *
 * This allows the code to treat all data types (byval/byref/...) the same
 * way, with only minimal space requirements. For example we don't need to
 * store varlena types differently in the sorted mode, etc. Everything is
 * uint32 making it much simpler.
 *
 * Of course, this assumes the built-in hash function is reasonably good,
 * without too many collisions etc. But that does seem to be the case, at
 * least based on past experience. After all, the same hash functions are
 * used for hash indexes, hash partitioning and so on.
 *
 *
 * sizing the bloom filter
 * -----------------------
 *
 * Size of a bloom filter depends on the number of distinct values we will
 * store in it, and the desired false positive rate. The higher the number
 * of distinct values and/or the lower the false positive rate, the larger
 * the bloom filter. On the other hand, we want to keep the index as small
 * as possible - that's one of the basic advantages of BRIN indexes.
 *
 * The number of distinct elements (in a page range) depends on the data,
 * we can consider it fixed. This simplifies the trade-off to just false
 * positive rate vs. size.
 *
 * At the page range level, false positive rate is a probability the bloom
 * filter matches a random value. For the whole index (with sufficiently
 * many page ranges) it represents the fraction of the index ranges (and
 * thus fraction of the table to be scanned) matching the random value.
 *
 * Furthermore, the size of the bloom filter is subject to implementation
 * limits - it has to fit onto a single index page (8kB by default). As
 * the bitmap is inherently random, compression can't reliably help here.
 * To reduce the size of a filter (to fit to a page), we have to either
 * accept higher false positive rate (undesirable), or reduce the number
 * of distinct items to be stored in the filter. We can't quite the input
 * data, of course, but we may make the BRIN page ranges smaller - instead
 * of the default 128 pages (1MB) we may build index with 16-page ranges,
 * or something like that. This does help even for random data sets, as
 * the number of rows per heap page is limited (to ~290 with very narrow
 * tables, likely ~20 in practice).
 *
 * Of course, good sizing decisions depend on having the necessary data,
 * i.e. number of distinct values in a page range (of a given size) and
 * table size (to estimate cost change due to change in false positive
 * rate due to having larger index vs. scanning larger indexes). We may
 * not have that data - for example when building an index on empty table
 * it's not really possible. And for some data we only have estimates for
 * the whole table and we can only estimate per-range values (ndistinct).
 *
 * Another challenge is that while the bloom filter is per-column, it's
 * the whole index tuple that has to fit into a page. And for multi-column
 * indexes that may include pieces we have no control over (not necessarily
 * bloom filters, the other columns may use other BRIN opclasses). So it's
 * not entirely clear how to distrubute the space between those columns.
 *
 * The current logic, implemented in brin_bloom_get_ndistinct, attempts to
 * make some basic sizing decisions, based on the table ndistinct estimate.
 *
 *
 * sort vs. hash
 * -------------
 *
 * As explained in the preceding section, sizing bloom filters correctly is
 * difficult in practice. It's also true that bloom filters quickly degrade
 * after exceeding the expected number of distinct items. It's therefore
 * expected that people will overshoot the parameters a bit (particularly
 * the ndistinct per page range), perhaps by a factor of 2 or more.
 *
 * It's also possible the data set is not uniform - it may have ranges with
 * very many distinct items per range, but also ranges with only very few
 * distinct items. The bloom filter has to be sized for the more variable
 * ranges, making it rather wasteful in the less variable part.
 *
 * For example, if some page ranges have 1000 distinct values, that means
 * about ~1.2kB bloom filter with 1% false positive rate. For page ranges
 * that only contain 10 distinct values, that's wasteful, because we might
 * store the values (the uint32 hashes) in just 40B.
 *
 * To address these issues, the opclass stores the raw values directly, and
 * only switches to the actual bloom filter after reaching the same space
 * requirements.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin_bloom.c
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/brin.h"
#include "access/brin_internal.h"
#include "access/brin_tuple.h"
#include "access/hash.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/stratnum.h"
#include "catalog/pg_type.h"
#include "catalog/pg_amop.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include <math.h>

#define BloomEqualStrategyNumber	1

/*
 * Additional SQL level support functions
 *
 * Procedure numbers must not use values reserved for BRIN itself; see
 * brin_internal.h.
 */
#define		BLOOM_MAX_PROCNUMS		1	/* maximum support procs we need */
#define		PROCNUM_HASH			11	/* required */

/*
 * Subtract this from procnum to obtain index in BloomOpaque arrays
 * (Must be equal to minimum of private procnums).
 */
#define		PROCNUM_BASE			11

/*
 * Flags. We only use a single bit for now, to decide whether we're in the
 * sorted or hash phase. So we have 15 bits for future use, if needed. The
 * filters are expected to be hundreds of bytes, so this is negligible.
 */
#define		BLOOM_FLAG_PHASE_HASH		0x0001

#define		BLOOM_IS_HASHED(f)		((f)->flags & BLOOM_FLAG_PHASE_HASH)
#define		BLOOM_IS_SORTED(f)		(!BLOOM_IS_HASHED(f))

/*
 * Number of hashes to accumulate before deduplicating and sorting in the
 * sort phase? We want this fairly small to reduce the amount of space and
 * also speed-up bsearch lookups.
 */
#define		BLOOM_MAX_UNSORTED		32

/*
 * Storage type for BRIN's reloptions.
 */
typedef struct BloomOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	double		nDistinctPerRange;	/* number of distinct values per range */
	double		falsePositiveRate;	/* false positive for bloom filter */
} BloomOptions;

/*
 * The current min value (16) is somewhat arbitrary, but it's based
 * on the fact that the filter header is ~20B alone, which is about
 * the same as the filter bitmap for 16 distinct items with 1% false
 * positive rate. So by allowing lower values we'd not gain much. In
 * any case, the min should not be larger than MaxHeapTuplesPerPage
 * (~290), which is the theoretical maximum for single-page ranges.
 */
#define		BLOOM_MIN_NDISTINCT_PER_RANGE		16

/*
 * Used to determine number of distinct items, based on the number of rows
 * in a page range. The 10% is somewhat similar to what estimate_num_groups
 * does, so we use the same factor here.
 */
#define		BLOOM_DEFAULT_NDISTINCT_PER_RANGE	-0.1	/* 10% of values */

/*
 * The 1% value is mostly arbitrary, it just looks nice.
 */
#define		BLOOM_DEFAULT_FALSE_POSITIVE_RATE	0.01	/* 1% fp rate */

#define BloomGetNDistinctPerRange(opts) \
	((opts) && (((BloomOptions *) (opts))->nDistinctPerRange != 0) ? \
	 (((BloomOptions *) (opts))->nDistinctPerRange) : \
	 BLOOM_DEFAULT_NDISTINCT_PER_RANGE)

#define BloomGetFalsePositiveRate(opts) \
	((opts) && (((BloomOptions *) (opts))->falsePositiveRate != 0.0) ? \
	 (((BloomOptions *) (opts))->falsePositiveRate) : \
	 BLOOM_DEFAULT_FALSE_POSITIVE_RATE)

/*
 * Bloom Filter
 *
 * Represents a bloom filter, built on hashes of the indexed values. That is,
 * we compute a uint32 hash of the value, and then store this hash into the
 * bloom filter (and compute additional hashes on it).
 *
 * We use an optimisation that initially we store the uint32 values directly,
 * without the extra hashing step. And only later filling the bitmap space,
 * we switch to the regular bloom filter mode.
 *
 * PHASE_SORTED
 *
 * Initially we copy the uint32 hash into the bitmap, regularly sorting the
 * hash values for fast lookup (we keep at most BLOOM_MAX_UNSORTED unsorted
 * values).
 *
 * The idea is that if we only see very few distinct values, we can store
 * them in less space compared to the (sparse) bloom filter bitmap. It also
 * stores them exactly, although that's not a big advantage as almost-empty
 * bloom filter has false positive rate close to zero anyway.
 *
 * PHASE_HASH
 *
 * Once we fill the bitmap space in the sorted phase, we switch to the hash
 * phase, where we actually use the bloom filter. We treat the uint32 hashes
 * as input values, and hash them again with different seeds (to get the k
 * hash functions needed for bloom filter).
 *
 *
 * XXX Perhaps we could save a few bytes by using different data types, but
 * considering the size of the bitmap, the difference is negligible.
 *
 * XXX We could also implement "sparse" bloom filters, keeping only the
 * bytes that are not entirely 0. That might make the "sorted" phase
 * mostly unnecessary.
 *
 * XXX We can also watch the number of bits set in the bloom filter, and
 * then stop using it (and not store the bitmap, to save space) when the
 * false positive rate gets too high.
 */
typedef struct BloomFilter
{
	/* varlena header (do not touch directly!) */
	int32	vl_len_;

	/* space for various flags (phase, etc.) */
	uint16	flags;

	/* fields used only in the SORTED phase */
	uint16	nvalues;	/* number of hashes stored (total) */
	uint16	nsorted;	/* number of hashes in the sorted part */

	/* fields for the HASHED phase */
	uint8	nhashes;	/* number of hash functions */
	uint32	nbits;		/* number of bits in the bitmap (size) */
	uint32	nbits_set;	/* number of bits set to 1 */

	/* data of the bloom filter (used both for sorted and hashed phase) */
	char	data[FLEXIBLE_ARRAY_MEMBER];

} BloomFilter;

static BloomFilter *bloom_switch_to_hashing(BloomFilter *filter);


/*
 * bloom_init
 * 		Initialize the Bloom Filter, allocate all the memory.
 *
 * The filter is initialized with optimal size for ndistinct expected
 * values, and requested false positive rate. The filter is stored as
 * varlena.
 */
static BloomFilter *
bloom_init(int ndistinct, double false_positive_rate)
{
	Size			len;
	BloomFilter	   *filter;

	int		m;	/* number of bits */
	double	k;	/* number of hash functions */

	Assert(ndistinct > 0);
	Assert((false_positive_rate > 0) && (false_positive_rate < 1.0));

	m = ceil((ndistinct * log(false_positive_rate)) / log(1.0 / (pow(2.0, log(2.0)))));

	/* round m to whole bytes */
	m = ((m + 7) / 8) * 8;

	/*
	 * round(log(2.0) * m / ndistinct), but assume round() may not be
	 * available on Windows
	 */
	k = log(2.0) * m / ndistinct;
	k = (k - floor(k) >= 0.5) ? ceil(k) : floor(k);

	/*
	 * Allocate the bloom filter with a minimum size 64B (about 40B in the
	 * bitmap part). We require space at least for the header.
	 *
	 * XXX Maybe the 64B min size is not really needed?
	 */
	len = Max(offsetof(BloomFilter, data), 64);

	filter = (BloomFilter *) palloc0(len);

	filter->flags = 0;	/* implies SORTED phase */
	filter->nhashes = (int) k;
	filter->nbits = m;

	SET_VARSIZE(filter, len);

	return filter;
}

/* simple uint32 comparator, for pg_qsort and bsearch */
static int
cmp_uint32(const void *a, const void *b)
{
	uint32 *ia = (uint32 *) a;
	uint32 *ib = (uint32 *) b;

	if (*ia == *ib)
		return 0;
	else if (*ia < *ib)
		return -1;
	else
		return 1;
}

/*
 * bloom_compact
 *		Compact the filter during the 'sorted' phase.
 *
 * We sort the uint32 hashes and remove duplicates, for two main reasons.
 * Firstly, to keep most of the data sorted for bsearch lookups. Secondly,
 * we try to save space by removing the duplicates, allowing us to stay
 * in the sorted phase a bit longer.
 *
 * We currently don't repalloc the bitmap, i.e. we don't free the memory
 * here - in the worst case we waste space for up to 32 unsorted hashes
 * (if all of them are already in the sorted part), so about 128B. We can
 * either reduce the number of unsorted items (e.g. to 8 hashes, which
 * would mean 32B), or start doing the repalloc.
 *
 * We do however set the varlena length, to minimize the storage needs.
 */
static void
bloom_compact(BloomFilter *filter)
{
	int		i,
			nvalues;
	Size	len;
	uint32 *values;

	/* never call compact on filters in HASH phase */
	Assert(BLOOM_IS_SORTED(filter));

	/* no chance to compact anything */
	if (filter->nvalues == filter->nsorted)
		return;

	values = (uint32 *) filter->data;

	/* TODO optimization: sort only the unsorted part, then merge */
	pg_qsort(values, filter->nvalues, sizeof(uint32), cmp_uint32);

	nvalues = 1;
	for (i = 1; i < filter->nvalues; i++)
	{
		/* if different from the last value, keep it */
		if (values[i] != values[nvalues - 1])
			values[nvalues++] = values[i];
	}

	filter->nvalues = nvalues;
	filter->nsorted = nvalues;

	len = offsetof(BloomFilter, data) +
				   (filter->nvalues) * sizeof(uint32);

	SET_VARSIZE(filter, len);
}

/*
 * bloom_add_value
 * 		Add value to the bloom filter.
 */
static BloomFilter *
bloom_add_value(BloomFilter *filter, uint32 value, bool *updated)
{
	int		i;
	uint32	big_h, h, d;

	/* assume 'not updated' by default */
	Assert(filter);

	/* if we're in the sorted phase, we store the hashes directly */
	if (BLOOM_IS_SORTED(filter))
	{
		/* how many uint32 hashes can we fit into the bitmap */
		int maxvalues = filter->nbits / (8 * sizeof(uint32));

		/* do not overflow the bitmap space or number of unsorted items */
		Assert(filter->nvalues <= maxvalues);
		Assert(filter->nvalues - filter->nsorted <= BLOOM_MAX_UNSORTED);

		/*
		 * In this branch we always update the filter - we either add the
		 * hash to the unsorted part, or switch the filter to hashing.
		 */
		if (updated)
			*updated = true;

		/*
		 * If the array is full, or if we reached the limit on unsorted
		 * items, try to compact the filter first, before attempting to
		 * add the new value.
		 */
		if ((filter->nvalues == maxvalues) ||
			(filter->nvalues - filter->nsorted == BLOOM_MAX_UNSORTED))
				bloom_compact(filter);

		/*
		 * Can we squeeze one more uint32 hash into the bitmap? Also make
		 * sure there's enough space in the bytea value first.
		 */
		if (filter->nvalues < maxvalues)
		{
			Size len = VARSIZE_ANY(filter);
			Size need = offsetof(BloomFilter, data) +
						(filter->nvalues + 1) * sizeof(uint32);

			/*
			 * We don't double the size here, as in the first place we care about
			 * reducing storage requirements, and the doubling happens automatically
			 * in memory contexts (so the repalloc should be cheap in most cases).
			 */
			if (len < need)
			{
				filter = (BloomFilter *) repalloc(filter, need);
				SET_VARSIZE(filter, need);
			}

			/* copy the new value into the filter */
			memcpy(&filter->data[filter->nvalues * sizeof(uint32)],
				   &value, sizeof(uint32));

			filter->nvalues++;

			/* we're done */
			return filter;
		}

		/* can't add any more exact hashes, so switch to hashing */
		filter = bloom_switch_to_hashing(filter);
	}

	/* we better be in the hashing phase */
	Assert(BLOOM_IS_HASHED(filter));

	/* compute the hashes, used for the bloom filter */
	big_h = ((uint32) DatumGetInt64(hash_uint32(value)));

	h = big_h % filter->nbits;
	d = big_h % (filter->nbits - 1);

	/* compute the requested number of hashes */
	for (i = 0; i < filter->nhashes; i++)
	{
		int byte = (h / 8);
		int bit  = (h % 8);

		/* if the bit is not set, set it and remember we did that */
		if (! (filter->data[byte] & (0x01 << bit)))
		{
			filter->data[byte] |= (0x01 << bit);
			filter->nbits_set++;
			if (updated)
				*updated = true;
		}

		/* next bit */
		h += d++;
		if (h >= filter->nbits)
			h -= filter->nbits;

		if (d == filter->nbits)
			d = 0;
	}

	return filter;
}

/*
 * bloom_switch_to_hashing
 * 		Switch the bloom filter from sorted to hashing mode.
 */
static BloomFilter *
bloom_switch_to_hashing(BloomFilter *filter)
{
	int		i;
	uint32 *values;
	Size			len;
	BloomFilter	   *newfilter;

	Assert(filter->nbits % 8 == 0);

	/*
	 * The new filter is allocated with all the memory, directly into
	 * the HASH phase.
	 */
	len = offsetof(BloomFilter, data) + (filter->nbits / 8);

	newfilter = (BloomFilter *) palloc0(len);

	newfilter->nhashes = filter->nhashes;
	newfilter->nbits = filter->nbits;
	newfilter->flags |= BLOOM_FLAG_PHASE_HASH;

	SET_VARSIZE(newfilter, len);

	values = (uint32 *) filter->data;

	for (i = 0; i < filter->nvalues; i++)
		/* ignore the return value here, re don't repalloc in hashing mode */
		bloom_add_value(newfilter, values[i], NULL);

	/* free the original filter, return the newly allocated one */
	pfree(filter);

	return newfilter;
}

/*
 * bloom_contains_value
 * 		Check if the bloom filter contains a particular value.
 */
static bool
bloom_contains_value(BloomFilter *filter, uint32 value)
{
	int		i;
	uint32	big_h, h, d;

	Assert(filter);

	/* in sorted mode we simply search the two arrays (sorted, unsorted) */
	if (BLOOM_IS_SORTED(filter))
	{
		int i;
		uint32 *values = (uint32 *) filter->data;

		/* first search through the sorted part */
		if ((filter->nsorted > 0) &&
			(bsearch(&value, values, filter->nsorted, sizeof(uint32), cmp_uint32) != NULL))
			return true;

		/* now search through the unsorted part - linear search */
		for (i = filter->nsorted; i < filter->nvalues; i++)
		{
			if (value == values[i])
				return true;
		}

		/* nothing found */
		return false;
	}

	/* now the regular hashing mode */
	Assert(BLOOM_IS_HASHED(filter));

	big_h = ((uint32) DatumGetInt64(hash_uint32(value)));

	h = big_h % filter->nbits;
	d = big_h % (filter->nbits - 1);

	/* compute the requested number of hashes */
	for (i = 0; i < filter->nhashes; i++)
	{
		int byte = (h / 8);
		int bit  = (h % 8);

		/* if the bit is not set, the value is not there */
		if (! (filter->data[byte] & (0x01 << bit)))
			return false;

		/* next bit */
		h += d++;
		if (h >= filter->nbits)
			h -= filter->nbits;

		if (d == filter->nbits)
			d = 0;
	}

	/* all hashes found in bloom filter */
	return true;
}

typedef struct BloomOpaque
{
	/*
	 * XXX At this point we only need a single proc (to compute the hash),
	 * but let's keep the array just like inclusion and minman opclasses,
	 * for consistency. We may need additional procs in the future.
	 */
	FmgrInfo	extra_procinfos[BLOOM_MAX_PROCNUMS];
	bool		extra_proc_missing[BLOOM_MAX_PROCNUMS];
} BloomOpaque;

static FmgrInfo *bloom_get_procinfo(BrinDesc *bdesc, uint16 attno,
					   uint16 procnum);


Datum
brin_bloom_opcinfo(PG_FUNCTION_ARGS)
{
	BrinOpcInfo *result;

	/*
	 * opaque->strategy_procinfos is initialized lazily; here it is set to
	 * all-uninitialized by palloc0 which sets fn_oid to InvalidOid.
	 *
	 * bloom indexes only store the filter as a single BYTEA column
	 */

	result = palloc0(MAXALIGN(SizeofBrinOpcInfo(1)) +
					 sizeof(BloomOpaque));
	result->oi_nstored = 1;
	result->oi_regular_nulls = true;
	result->oi_opaque = (BloomOpaque *)
		MAXALIGN((char *) result + SizeofBrinOpcInfo(1));
	result->oi_typcache[0] = lookup_type_cache(BYTEAOID, 0);

	PG_RETURN_POINTER(result);
}

/*
 * brin_bloom_get_ndistinct
 *		Determine the ndistinct value used to size bloom filter.
 *
 * Tweak the ndistinct value based on the pagesPerRange value. First,
 * if it's negative, it's assumed to be relative to maximum number of
 * tuples in the range (assuming each page gets MaxHeapTuplesPerPage
 * tuples, which is likely a significant over-estimate). We also clamp
 * the value, not to over-size the bloom filter unnecessarily.
 *
 * XXX We can only do this when the pagesPerRange value was supplied.
 * If it wasn't, it has to be a read-only access to the index, in which
 * case we don't really care. But perhaps we should fall-back to the
 * default pagesPerRange value?
 *
 * XXX We might also fetch info about ndistinct estimate for the column,
 * and compute the expected number of distinct values in a range. But
 * that may be tricky due to data being sorted in various ways, so it
 * seems better to rely on the upper estimate.
 */
static int
brin_bloom_get_ndistinct(BrinDesc *bdesc, BloomOptions *opts)
{
	double ndistinct;
	double	maxtuples;
	BlockNumber pagesPerRange;

	pagesPerRange = BrinGetPagesPerRange(bdesc->bd_index);
	ndistinct = BloomGetNDistinctPerRange(opts);

	Assert(BlockNumberIsValid(pagesPerRange));

	maxtuples = MaxHeapTuplesPerPage * pagesPerRange;

	/*
	 * Similarly to n_distinct, negative values are relative - in this
	 * case to maximum number of tuples in the page range (maxtuples).
	 */
	if (ndistinct < 0)
		ndistinct = (-ndistinct) * maxtuples;

	/*
	 * Positive values are to be used directly, but we still apply a
	 * couple of safeties no to use unreasonably small bloom filters.
	 */
	ndistinct = Max(ndistinct, BLOOM_MIN_NDISTINCT_PER_RANGE);

	/*
	 * And don't use more than the maximum possible number of tuples,
	 * in the range, which would be entirely wasteful.
	 */
	ndistinct = Min(ndistinct, maxtuples);

	return (int) ndistinct;
}

static double
brin_bloom_get_fp_rate(BrinDesc *bdesc, BloomOptions *opts)
{
	return BloomGetFalsePositiveRate(opts);
}

/*
 * Examine the given index tuple (which contains partial status of a certain
 * page range) by comparing it to the given value that comes from another heap
 * tuple.  If the new value is outside the bloom filter specified by the
 * existing tuple values, update the index tuple and return true.  Otherwise,
 * return false and do not modify in this case.
 */
Datum
brin_bloom_add_value(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	Datum		newval = PG_GETARG_DATUM(2);
	bool		isnull PG_USED_FOR_ASSERTS_ONLY = PG_GETARG_DATUM(3);
	BloomOptions *opts = (BloomOptions *) PG_GET_OPCLASS_OPTIONS();
	Oid			colloid = PG_GET_COLLATION();
	FmgrInfo   *hashFn;
	uint32		hashValue;
	bool		updated = false;
	AttrNumber	attno;
	BloomFilter *filter;

	Assert(!isnull);

	attno = column->bv_attno;

	/*
	 * If this is the first non-null value, we need to initialize the bloom
	 * filter. Otherwise just extract the existing bloom filter from BrinValues.
	 */
	if (column->bv_allnulls)
	{
		filter = bloom_init(brin_bloom_get_ndistinct(bdesc, opts),
							brin_bloom_get_fp_rate(bdesc, opts));
		column->bv_values[0] = PointerGetDatum(filter);
		column->bv_allnulls = false;
		updated = true;
	}
	else
		filter = (BloomFilter *) PG_DETOAST_DATUM(column->bv_values[0]);

	/*
	 * Compute the hash of the new value, using the supplied hash function,
	 * and then add the hash value to the bloom filter.
	 */
	hashFn = bloom_get_procinfo(bdesc, attno, PROCNUM_HASH);

	hashValue = DatumGetUInt32(FunctionCall1Coll(hashFn, colloid, newval));

	filter = bloom_add_value(filter, hashValue, &updated);

	column->bv_values[0] = PointerGetDatum(filter);

	PG_RETURN_BOOL(updated);
}

/*
 * Given an index tuple corresponding to a certain page range and a scan key,
 * return whether the scan key is consistent with the index tuple's bloom
 * filter.  Return true if so, false otherwise.
 */
Datum
brin_bloom_consistent(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	ScanKey	   *keys = (ScanKey *) PG_GETARG_POINTER(2);
	int			nkeys = PG_GETARG_INT32(3);
	Oid			colloid = PG_GET_COLLATION();
	AttrNumber	attno;
	Datum		value;
	Datum		matches;
	FmgrInfo   *finfo;
	uint32		hashValue;
	BloomFilter *filter;
	int			keyno;

	filter = (BloomFilter *) PG_DETOAST_DATUM(column->bv_values[0]);

	Assert(filter);

	matches = true;

	for (keyno = 0; keyno < nkeys; keyno++)
	{
		ScanKey	key = keys[keyno];

		/* NULL keys are handled and filtered-out in bringetbitmap */
		Assert(!(key->sk_flags & SK_ISNULL));

		attno = key->sk_attno;
		value = key->sk_argument;

		switch (key->sk_strategy)
		{
			case BloomEqualStrategyNumber:

				/*
				 * In the equality case (WHERE col = someval), we want to return
				 * the current page range if the minimum value in the range <=
				 * scan key, and the maximum value >= scan key.
				 */
				finfo = bloom_get_procinfo(bdesc, attno, PROCNUM_HASH);

				hashValue = DatumGetUInt32(FunctionCall1Coll(finfo, colloid, value));
				matches &= bloom_contains_value(filter, hashValue);

				break;
			default:
				/* shouldn't happen */
				elog(ERROR, "invalid strategy number %d", key->sk_strategy);
				matches = 0;
				break;
		}

		if (!matches)
			break;
	}

	PG_RETURN_DATUM(matches);
}

/*
 * Given two BrinValues, update the first of them as a union of the summary
 * values contained in both.  The second one is untouched.
 *
 * XXX We assume the bloom filters have the same parameters fow now. In the
 * future we should have 'can union' function, to decide if we can combine
 * two particular bloom filters.
 */
Datum
brin_bloom_union(PG_FUNCTION_ARGS)
{
	BrinValues *col_a = (BrinValues *) PG_GETARG_POINTER(1);
	BrinValues *col_b = (BrinValues *) PG_GETARG_POINTER(2);
	BloomFilter *filter_a;
	BloomFilter *filter_b;

	Assert(col_a->bv_attno == col_b->bv_attno);
	Assert(!col_a->bv_allnulls && !col_b->bv_allnulls);

	filter_a = (BloomFilter *) PG_DETOAST_DATUM(col_a->bv_values[0]);
	filter_b = (BloomFilter *) PG_DETOAST_DATUM(col_b->bv_values[0]);

	/* make sure neither of the bloom filters is NULL */
	Assert(filter_a && filter_b);
	Assert(filter_a->nbits == filter_b->nbits);

	/*
	 * Merging of the filters depends on the phase of both filters.
	 */
	if (BLOOM_IS_SORTED(filter_b))
	{
		/*
		 * Simply read all items from 'b' and add them to 'a' (the phase of
		 * 'a' does not really matter).
		 */
		int		i;
		uint32 *values = (uint32 *) filter_b->data;

		for (i = 0; i < filter_b->nvalues; i++)
			filter_a = bloom_add_value(filter_a, values[i], NULL);

		col_a->bv_values[0] = PointerGetDatum(filter_a);
	}
	else if (BLOOM_IS_SORTED(filter_a))
	{
		/*
		 * 'b' hashed, 'a' sorted - copy 'b' into 'a' and then add all values
		 * from 'a' into the new copy.
		 */
		int		i;
		BloomFilter *filter_c;
		uint32 *values = (uint32 *) filter_a->data;

		filter_c = (BloomFilter *) PG_DETOAST_DATUM(datumCopy(PointerGetDatum(filter_b), false, -1));

		for (i = 0; i < filter_a->nvalues; i++)
			filter_c = bloom_add_value(filter_c, values[i], NULL);

		col_a->bv_values[0] = PointerGetDatum(filter_c);
	}
	else if (BLOOM_IS_HASHED(filter_a))
	{
		/*
		 * 'b' hashed, 'a' hashed - merge the bitmaps by OR
		 */
		int		i;
		int		nbytes = (filter_a->nbits + 7) / 8;

		/* we better have "compatible" bloom filters */
		Assert(filter_a->nbits == filter_b->nbits);
		Assert(filter_a->nhashes == filter_b->nhashes);

		for (i = 0; i < nbytes; i++)
			filter_a->data[i] |= filter_b->data[i];
	}

	PG_RETURN_VOID();
}

/*
 * Cache and return inclusion opclass support procedure
 *
 * Return the procedure corresponding to the given function support number
 * or null if it is not exists.
 */
static FmgrInfo *
bloom_get_procinfo(BrinDesc *bdesc, uint16 attno, uint16 procnum)
{
	BloomOpaque *opaque;
	uint16		basenum = procnum - PROCNUM_BASE;

	/*
	 * We cache these in the opaque struct, to avoid repetitive syscache
	 * lookups.
	 */
	opaque = (BloomOpaque *) bdesc->bd_info[attno - 1]->oi_opaque;

	/*
	 * If we already searched for this proc and didn't find it, don't bother
	 * searching again.
	 */
	if (opaque->extra_proc_missing[basenum])
		return NULL;

	if (opaque->extra_procinfos[basenum].fn_oid == InvalidOid)
	{
		if (RegProcedureIsValid(index_getprocid(bdesc->bd_index, attno,
												procnum)))
		{
			fmgr_info_copy(&opaque->extra_procinfos[basenum],
						   index_getprocinfo(bdesc->bd_index, attno, procnum),
						   bdesc->bd_context);
		}
		else
		{
			opaque->extra_proc_missing[basenum] = true;
			return NULL;
		}
	}

	return &opaque->extra_procinfos[basenum];
}

Datum
brin_bloom_options(PG_FUNCTION_ARGS)
{
	local_relopts *relopts = (local_relopts *) PG_GETARG_POINTER(0);
	BloomOptions *blopts = NULL;

	extend_local_reloptions(relopts, blopts, sizeof(*blopts));

	add_local_real_reloption(relopts, "n_distinct_per_range",
							 "number of distinct items expected in a BRIN page range",
							 BLOOM_DEFAULT_NDISTINCT_PER_RANGE,
							 -1.0, INT_MAX, &blopts->nDistinctPerRange);

	add_local_real_reloption(relopts, "false_positive_rate",
							 "desired false-positive rate for the bloom filters",
							 BLOOM_DEFAULT_FALSE_POSITIVE_RATE,
							 0.001, 1.0, &blopts->falsePositiveRate);

	PG_RETURN_VOID();
}
