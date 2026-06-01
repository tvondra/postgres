/*-------------------------------------------------------------------------
 *
 * bloomfilter.c
 *		Space-efficient set membership testing
 *
 * A Bloom filter is a probabilistic data structure that is used to test an
 * element's membership of a set.  False positives are possible, but false
 * negatives are not; a test of membership of the set returns either "possibly
 * in set" or "definitely not in set".  This is typically very space efficient,
 * which can be a decisive advantage.
 *
 * Elements can be added to the set, but not removed.  The more elements that
 * are added, the larger the probability of false positives.  Caller must hint
 * an estimated total size of the set when the Bloom filter is initialized.
 * This is used to balance the use of memory against the final false positive
 * rate.
 *
 * The implementation is well suited to data synchronization problems between
 * unordered sets, especially where predictable performance is important and
 * some false positives are acceptable.  It's also well suited to cache
 * filtering problems where a relatively small and/or low cardinality set is
 * fingerprinted, especially when many subsequent membership tests end up
 * indicating that values of interest are not present.  That should save the
 * caller many authoritative lookups, such as expensive probes of a much larger
 * on-disk structure.
 *
 * Copyright (c) 2018-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/lib/bloomfilter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "common/hashfn.h"
#include "lib/bloomfilter.h"
#include "port/pg_bitutils.h"

/*
 * Default minimum size of the bitset, in bytes. bloom_create() won't create
 * a bitset smaller than this, even when the caller's total_elems estimate would
 * suggest a smaller one.
 */
#define DEFAULT_MIN_BITSET_BYTES	(1024 * 1024)

#define MAX_HASH_FUNCS		10

struct bloom_filter
{
	/* K hash functions are used, seeded by caller's seed */
	int			k_hash_funcs;
	uint64		seed;
	/* m is bitset size, in bits.  Must be a power of two <= 2^32.  */
	uint64		m;
	unsigned char bitset[FLEXIBLE_ARRAY_MEMBER];
};

static int	my_bloom_power(uint64 target_bitset_bits);
static int	optimal_k(uint64 bitset_bits, int64 total_elems);
static uint64 bloom_bitset_bytes(int64 total_elems, int bloom_work_mem,
								  Size min_filter_size);
static void k_hashes(bloom_filter *filter, uint32 *hashes, unsigned char *elem,
					 size_t len);
static inline uint32 mod_m(uint32 val, uint64 m);

/*
 * Determine the size of the bitset (in bytes) that bloom_create()/bloom_init()
 * will use for the given parameters.  The bitset is always a power-of-two
 * number of bits; see bloom_create() for the rationale behind the sizing.
 *
 * min_filter_size is the minimum size of the bitset, in bytes.  The bitset
 * will never be sized below this, even when the total_elems estimate would
 * suggest a smaller one.
 */
static uint64
bloom_bitset_bytes(int64 total_elems, int bloom_work_mem, Size min_filter_size)
{
	uint64		bitset_bytes;
	uint64		bitset_bits;
	int			bloom_power;

	/*
	 * Aim for two bytes per element; this is sufficient to get a false
	 * positive rate below 1%, independent of the size of the bitset or total
	 * number of elements.  Also, if rounding down the size of the bitset to
	 * the next lowest power of two turns out to be a significant drop, the
	 * false positive rate still won't exceed 2% in almost all cases.
	 */
	bitset_bytes = Min(bloom_work_mem * UINT64CONST(1024), total_elems * 2);
	bitset_bytes = Max(min_filter_size, bitset_bytes);

	/*
	 * Size in bits should be the highest power of two <= target.  bitset_bits
	 * is uint64 because PG_UINT32_MAX is 2^32 - 1, not 2^32
	 */
	bloom_power = my_bloom_power(bitset_bytes * BITS_PER_BYTE);
	bitset_bits = UINT64CONST(1) << bloom_power;
	bitset_bytes = bitset_bits / BITS_PER_BYTE;

	return bitset_bytes;
}

/*
 * Amount of memory (in bytes) that a Bloom filter sized for the given
 * parameters occupies, including the fixed-size header.  This lets callers
 * place a Bloom filter in caller-managed storage (for example shared memory)
 * with bloom_init().
 */
size_t
bloom_estimate(int64 total_elems, int bloom_work_mem)
{
	return bloom_estimate_custom(total_elems, bloom_work_mem,
								 BLOOM_DEFAULT_MIN_SIZE);
}

/*
 * Like bloom_estimate(), but the minimum size of the bitset (in bytes) is
 * provided by the caller instead of the default.  See bloom_create_custom().
 */
size_t
bloom_estimate_custom(int64 total_elems, int bloom_work_mem,
					  Size min_filter_size)
{
	return offsetof(bloom_filter, bitset) +
		sizeof(unsigned char) * bloom_bitset_bytes(total_elems, bloom_work_mem,
												   min_filter_size);
}

/*
 * Initialize a Bloom filter in caller-provided memory.
 *
 * "ptr" must point to at least bloom_estimate(total_elems, bloom_work_mem)
 * bytes.  This is useful when the filter must live in memory that the caller
 * manages itself, such as a DSA allocation shared between parallel workers.
 *
 * Two filters initialized with identical total_elems, bloom_work_mem and seed
 * values share the same dimensions and may be combined with bloom_merge().
 */
bloom_filter *
bloom_init(void *ptr, int64 total_elems, int bloom_work_mem, uint64 seed)
{
	return bloom_init_custom(ptr, total_elems, bloom_work_mem,
							 BLOOM_DEFAULT_MIN_SIZE, seed);
}

/*
 * Like bloom_init(), but the minimum size of the bitset (in bytes) is provided
 * by the caller instead of the default.  See bloom_create_custom().
 */
bloom_filter *
bloom_init_custom(void *ptr, int64 total_elems, int bloom_work_mem,
				  uint64 min_bitset_bytes, uint64 seed)
{
	bloom_filter *filter = (bloom_filter *) ptr;
	uint64		bitset_bytes = bloom_bitset_bytes(total_elems, bloom_work_mem,
												  min_filter_size);
	uint64		bitset_bits = bitset_bytes * BITS_PER_BYTE;

	filter->k_hash_funcs = optimal_k(bitset_bits, total_elems);
	filter->seed = seed;
	filter->m = bitset_bits;
	memset(filter->bitset, 0, bitset_bytes);

	return filter;
}

/*
 * Merge the bits set in "src" into "dst".
 *
 * Both filters must have been created with identical dimensions (that is, the
 * same total_elems, bloom_work_mem and seed values).  After this call "dst"
 * reports an element as possibly-present if it was possibly-present in either
 * of the input filters, which is exactly the filter that would have resulted
 * from adding every element of both filters to a single Bloom filter.
 */
void
bloom_merge(bloom_filter *dst, const bloom_filter *src)
{
	uint64		bitset_bytes;
	uint64		i;

	Assert(dst->m == src->m);
	Assert(dst->k_hash_funcs == src->k_hash_funcs);
	Assert(dst->seed == src->seed);

	bitset_bytes = dst->m / BITS_PER_BYTE;
	for (i = 0; i < bitset_bytes; i++)
		dst->bitset[i] |= src->bitset[i];
}

/*
 * Create Bloom filter in caller's memory context.  We aim for a false positive
 * rate of between 1% and 2% when bitset size is not constrained by memory
 * availability.
 *
 * total_elems is an estimate of the final size of the set.  It should be
 * approximately correct, but the implementation can cope well with it being
 * off by perhaps a factor of five or more.  See "Bloom Filters in
 * Probabilistic Verification" (Dillinger & Manolios, 2004) for details of why
 * this is the case.
 *
 * bloom_work_mem is sized in KB, in line with the general work_mem convention.
 * This determines the size of the underlying bitset (trivial bookkeeping space
 * isn't counted).  The bitset is always sized as a power of two number of
 * bits, and the largest possible bitset is 512MB (2^32 bits).  The
 * implementation allocates only enough memory to target its standard false
 * positive rate, using a simple formula with caller's total_elems estimate as
 * an input.
 *
 * The Bloom filter is seeded using a value provided by the caller.  Using a
 * distinct seed value on every call makes it unlikely that the same false
 * positives will reoccur when the same set is fingerprinted a second time.
 * Callers that don't care about this pass a constant as their seed, typically
 * 0.  Callers can also use a pseudo-random seed, eg from pg_prng_uint64().
 */
bloom_filter *
bloom_create(int64 total_elems, int bloom_work_mem, uint64 seed)
{
	return bloom_create_custom(total_elems, bloom_work_mem,
							   BLOOM_DEFAULT_MIN_SIZE, seed);
}

/*
 * Create Bloom filter in caller's memory context, like bloom_create(), but
 * with a caller-specified minimum bitset size.
 *
 * min_bitset_bytes is the minimum bitset size. The bitset might be as small
 * as 1KiB, even when bloom_work_mem is much higher. This is useful for callers
 * that want to allow filters smaller than the default DEFAULT_MIN_BITSET_BYTES
 * (1MB), for example when fingerprinting small sets where the 1MB minimum
 * would waste memory and would not fit into CPU caches. The bitset is still
 * sized as a power of two number of bits, and is never smaller than this
 * minimum (subject to that rounding).
 */
bloom_filter *
bloom_create_custom(int64 total_elems, int bloom_work_mem,
					uint64 min_bitset_bytes, uint64 seed)
{
	bloom_filter *filter;
	int			bloom_power;
	uint64		bitset_bytes;
	uint64		bitset_bits;

	/*
	 * Aim for two bytes per element; this is sufficient to get a false
	 * positive rate below 1%, independent of the size of the bitset or total
	 * number of elements.  Also, if rounding down the size of the bitset to
	 * the next lowest power of two turns out to be a significant drop, the
	 * false positive rate still won't exceed 2% in almost all cases.
	 */
	bitset_bytes = Min(bloom_work_mem * UINT64CONST(1024), total_elems * 2);
	bitset_bytes = Max(min_bitset_bytes, bitset_bytes);

	/*
	 * Size in bits should be the highest power of two <= target.  bitset_bits
	 * is uint64 because PG_UINT32_MAX is 2^32 - 1, not 2^32
	 */
	bloom_power = my_bloom_power(bitset_bytes * BITS_PER_BYTE);
	bitset_bits = UINT64CONST(1) << bloom_power;
	bitset_bytes = bitset_bits / BITS_PER_BYTE;

	/* Allocate bloom filter with unset bitset */
	filter = palloc0(offsetof(bloom_filter, bitset) +
					 sizeof(unsigned char) * bitset_bytes);
	filter->k_hash_funcs = optimal_k(bitset_bits, total_elems);
	filter->seed = seed;
	filter->m = bitset_bits;

	return filter;
}

/*
 * Create Bloom filter in caller's memory context, like bloom_create_custom(),
 * but with the minimum bitset size set to DEFAULT_MIN_BITSET_BYTES (i.e. 1MB).
 */
bloom_filter *
bloom_create(int64 total_elems, int bloom_work_mem, uint64 seed)
{
	return bloom_create_custom(total_elems, bloom_work_mem,
							   DEFAULT_MIN_BITSET_BYTES, seed);
}

/*
 * Free Bloom filter
 */
void
bloom_free(bloom_filter *filter)
{
	pfree(filter);
}

/*
 * Add element to Bloom filter
 */
void
bloom_add_element(bloom_filter *filter, unsigned char *elem, size_t len)
{
	uint32		hashes[MAX_HASH_FUNCS];
	int			i;

	k_hashes(filter, hashes, elem, len);

	/* Map a bit-wise address to a byte-wise address + bit offset */
	for (i = 0; i < filter->k_hash_funcs; i++)
	{
		filter->bitset[hashes[i] >> 3] |= 1 << (hashes[i] & 7);
	}
}

/*
 * Test if Bloom filter definitely lacks element.
 *
 * Returns true if the element is definitely not in the set of elements
 * observed by bloom_add_element().  Otherwise, returns false, indicating that
 * element is probably present in set.
 */
bool
bloom_lacks_element(bloom_filter *filter, unsigned char *elem, size_t len)
{
	uint32		hashes[MAX_HASH_FUNCS];
	int			i;

	k_hashes(filter, hashes, elem, len);

	/* Map a bit-wise address to a byte-wise address + bit offset */
	for (i = 0; i < filter->k_hash_funcs; i++)
	{
		if (!(filter->bitset[hashes[i] >> 3] & (1 << (hashes[i] & 7))))
			return true;
	}

	return false;
}

/*
 * What proportion of bits are currently set?
 *
 * Returns proportion, expressed as a multiplier of filter size.  That should
 * generally be close to 0.5, even when we have more than enough memory to
 * ensure a false positive rate within target 1% to 2% band, since more hash
 * functions are used as more memory is available per element.
 *
 * This is the only instrumentation that is low overhead enough to appear in
 * debug traces.  When debugging Bloom filter code, it's likely to be far more
 * interesting to directly test the false positive rate.
 */
double
bloom_prop_bits_set(bloom_filter *filter)
{
	int			bitset_bytes = filter->m / BITS_PER_BYTE;
	uint64		bits_set = pg_popcount((char *) filter->bitset, bitset_bytes);

	return bits_set / (double) filter->m;
}

/*
 * Returns the number of hash functions used by this Bloom filter.
 */
int
bloom_num_hash_funcs(bloom_filter *filter)
{
	return filter->k_hash_funcs;
}

/*
 * Returns the total size of the Bloom filter's bitset, in bits.
 */
uint64
bloom_total_bits(bloom_filter *filter)
{
	return filter->m;
}

/*
 * Estimate the current false positive rate of the Bloom filter.
 *
 * For a filter that uses k hash functions, the probability that a membership
 * test for an element that was never added still reports "possibly present" is
 * approximately p^k, where p is the proportion of bits currently set. This
 * reflects the actual contents of the filter rather than the target rate aimed
 * for at creation time.
 */
double
bloom_false_positive_rate(bloom_filter *filter)
{
	return pow(bloom_prop_bits_set(filter), filter->k_hash_funcs);
}

/*
 * Which element in the sequence of powers of two is less than or equal to
 * target_bitset_bits?
 *
 * Value returned here must be generally safe as the basis for actual bitset
 * size.
 *
 * Bitset is never allowed to exceed 2 ^ 32 bits (512MB).  This is sufficient
 * for the needs of all current callers, and allows us to use 32-bit hash
 * functions.  It also makes it easy to stay under the MaxAllocSize restriction
 * (caller needs to leave room for non-bitset fields that appear before
 * flexible array member, so a 1GB bitset would use an allocation that just
 * exceeds MaxAllocSize).
 */
static int
my_bloom_power(uint64 target_bitset_bits)
{
	int			bloom_power = -1;

	while (target_bitset_bits > 0 && bloom_power < 32)
	{
		bloom_power++;
		target_bitset_bits >>= 1;
	}

	return bloom_power;
}

/*
 * Determine optimal number of hash functions based on size of filter in bits,
 * and projected total number of elements.  The optimal number is the number
 * that minimizes the false positive rate.
 */
static int
optimal_k(uint64 bitset_bits, int64 total_elems)
{
	int			k = rint(log(2.0) * bitset_bits / total_elems);

	return Max(1, Min(k, MAX_HASH_FUNCS));
}

/*
 * Generate k hash values for element.
 *
 * Caller passes array, which is filled-in with k values determined by hashing
 * caller's element.
 *
 * Only 2 real independent hash functions are actually used to support an
 * interface of up to MAX_HASH_FUNCS hash functions; enhanced double hashing is
 * used to make this work.  The main reason we prefer enhanced double hashing
 * to classic double hashing is that the latter has an issue with collisions
 * when using power of two sized bitsets.  See Dillinger & Manolios for full
 * details.
 */
static void
k_hashes(bloom_filter *filter, uint32 *hashes, unsigned char *elem, size_t len)
{
	uint64		hash;
	uint32		x,
				y;
	uint64		m;
	int			i;

	/* Use 64-bit hashing to get two independent 32-bit hashes */
	hash = DatumGetUInt64(hash_any_extended(elem, len, filter->seed));
	x = (uint32) hash;
	y = (uint32) (hash >> 32);
	m = filter->m;

	x = mod_m(x, m);
	y = mod_m(y, m);

	/* Accumulate hashes */
	hashes[0] = x;
	for (i = 1; i < filter->k_hash_funcs; i++)
	{
		x = mod_m(x + y, m);
		y = mod_m(y + i, m);

		hashes[i] = x;
	}
}

/*
 * Calculate "val MOD m" inexpensively.
 *
 * Assumes that m (which is bitset size) is a power of two.
 *
 * Using a power of two number of bits for bitset size allows us to use bitwise
 * AND operations to calculate the modulo of a hash value.  It's also a simple
 * way of avoiding the modulo bias effect.
 */
static inline uint32
mod_m(uint32 val, uint64 m)
{
	Assert(m <= PG_UINT32_MAX + UINT64CONST(1));
	Assert(((m - 1) & m) == 0);

	return val & (m - 1);
}
