/*
 * brin_bloom.c
 *		Implementation of Bloom opclass for BRIN
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin_bloom.c
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/brin_internal.h"
#include "access/brin_tuple.h"
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
#define		BLOOM_MAX_PROCNUMS		4	/* maximum support procs we need */
#define		PROCNUM_HASH			11	/* required */

/*
 * Subtract this from procnum to obtain index in BloomOpaque arrays
 * (Must be equal to minimum of private procnums).
 */
#define		PROCNUM_BASE			11


#define		BLOOM_PHASE_SORTED		1
#define		BLOOM_PHASE_HASH		2

/* how many hashes to accumulate before hashing */
#define		BLOOM_MAX_UNSORTED		64

typedef struct BloomFilter
{
	/* varlena header (do not touch directly!) */
	int32	vl_len_;

	/* global bloom filter parameters */
	uint32	phase;		/* phase (initially SORTED, then HASH) */
	uint32	nhashes;	/* number of hash functions */
	uint32	nbits;		/* number of bits in the bitmap */
	uint32	nbits_set;	/* number of bits set to 1 */

	/* fields used only in the EXACT phase */
	uint32	nvalues;	/* number of hashes stored (sorted + extra) */
	uint32	nsorted;	/* number of uint32 hashes in sorted part */

	/* bitmap of the bloom filter */
	char 	bitmap[FLEXIBLE_ARRAY_MEMBER];

} BloomFilter;

static BloomFilter *
bloom_init(int ndistinct, double false_positives)
{
	Size			len;
	BloomFilter	   *filter;

	/* https://en.wikipedia.org/wiki/Bloom_filter */
	int m;	/* number of bits */
	int k;	/* number of hash functions */

	Assert((false_positives > 0) && (false_positives < 1.0));

	m = ceil((ndistinct * log(false_positives)) / log(1.0 / (pow(2.0, log(2.0)))));

	/* round m to whole bytes */
	m = ((m + 7) / 8) * 8;

	k = round(log(2.0) * m / ndistinct);

	elog(DEBUG1, "create bloom filter m=%d k=%d", m, k);

	/* allocate the bloom filter */
	len = offsetof(BloomFilter, bitmap) + (m/8);
	filter = (BloomFilter *) palloc0(len);

	filter->phase = BLOOM_PHASE_SORTED;
	filter->nhashes = k;
	filter->nbits = m;

	SET_VARSIZE(filter, len);

	return filter;
}

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


static void
bloom_compact(BloomFilter *filter)
{
	int i,
	nvalues;

	uint32 *values;

	/* no chance to compact anything */
	if (filter->nvalues == filter->nsorted)
		return;

	values = (uint32 *) palloc(filter->nvalues * sizeof(uint32));

	/* copy the data, then reset the bitmap */
	memcpy(values, filter->bitmap, filter->nvalues * sizeof(uint32));
	memset(filter->bitmap, 0, filter->nbits / 8);

	/* FIXME optimization: sort only the unsorted part, then merge */
	pg_qsort(values, filter->nvalues, sizeof(uint32), cmp_uint32);

	nvalues = 1;
	for (i = 1; i < filter->nvalues; i++)
	{
		/* if a new value, keep it */
		if (values[i] != values[i-1])
		{
			values[nvalues] = values[i];
			nvalues++;
		}
	}

	filter->nvalues = nvalues;
	filter->nsorted = nvalues;

	memcpy(filter->bitmap, values, nvalues * sizeof(uint32));

	pfree(values);
}

static void
bloom_switch_to_hashing(BloomFilter *filter);

static bool
bloom_add_value(BloomFilter *filter, uint32 value)
{
	int		i;
	bool	updated = false;

	Assert(filter);

	/* if we're in the sorted phase, we store the hashes directly */
	if (filter->phase == BLOOM_PHASE_SORTED)
	{
		int maxvalues = filter->nbits / (8 * sizeof(uint32));

		/*
		 * In this branch we always update the filter - we either add the
		 * hash to the unsorted part, or switch the filter to hashing.
		 */
		updated = true;

		/*
		 * If the array is full, or if we reached the limit on unsorted
		 * items, try to compact the filter first, before attempting to
		 * add the new value.
		 */
		if ((filter->nvalues == maxvalues) ||
			(filter->nvalues - filter->nsorted == BLOOM_MAX_UNSORTED))
				bloom_compact(filter);

		/* can we squeeze one more uint32 hash into the bitmap? */
		if (filter->nvalues < maxvalues)
		{
			/* copy the data into the bitmap */
			memcpy(&filter->bitmap[filter->nvalues * sizeof(uint32)],
				   &value, sizeof(uint32));

			filter->nvalues++;

			/* we're done */
			return updated;
		}

		/* can't add any more exact hashes, so switch to hashing */
		bloom_switch_to_hashing(filter);
	}

	/* we better be in the regular hashing phase */
	Assert(filter->phase == BLOOM_PHASE_HASH);

	/* we're in the ah */
	for (i = 0; i < filter->nhashes; i++)
	{
		uint32 h = (value + i * 937) % filter->nbits;

		int byte = (h / 8);
		int bit  = (h % 8);

		/* if the bit is not set, set it and remember we did that */
		if (! (filter->bitmap[byte] & (0x01 << bit)))
		{
			filter->bitmap[byte] |= (0x01 << bit);
			filter->nbits_set++;
			updated = true;
		}
	}

	return updated;
}

static void
bloom_switch_to_hashing(BloomFilter *filter)
{
	int		i;
	uint32 *values;

	filter->phase = BLOOM_PHASE_HASH;

	elog(DEBUG1, "switching %p to hashing", filter);

	values = (uint32 *) palloc(filter->nvalues * sizeof(uint32));
	memcpy(values, filter->bitmap, filter->nvalues * sizeof(uint32));
	memset(filter->bitmap, 0, filter->nvalues * sizeof(uint32));

	for (i = 0; i < filter->nvalues; i++)
		bloom_add_value(filter, values[i]);

	filter->nvalues = 0;
	filter->nsorted = 0;
}


static bool
bloom_contains_value(BloomFilter *filter, uint32 value)
{
	int		i;

	Assert(filter);

	if (filter->phase == BLOOM_PHASE_SORTED)
	{
		int i;
		uint32 *values = (uint32 *)filter->bitmap;

		/* first search through the sorted part */
		if ((filter->nsorted > 0) &&
			(bsearch(&value, values, filter->nsorted, sizeof(uint32), cmp_uint32) != NULL))
			return true;

		/* now search through the unsorted part - linear search */
		for (i = filter->nsorted; i < filter->nvalues; i++)
			if (value == values[i])
				return true;

		/* nothing found */
		return false;
	}

	/* now the regular hashing mode */
	Assert(filter->phase == BLOOM_PHASE_HASH);

	for (i = 0; i < filter->nhashes; i++)
	{
		uint32 h = (value + i * 937) % filter->nbits;

		int byte = (h / 8);
		int bit  = (h % 8);

		/* if the bit is not set, the value is not there */
		if (! (filter->bitmap[byte] & (0x01 << bit)))
			return false;
	}

	/* all hashes found in bloom filter */
	return true;
}

static int
bloom_filter_count(BloomFilter *filter)
{
	return ceil(- (filter->nbits * 1.0 / filter->nhashes * log(1 - filter->nbits_set * 1.0 / filter->nbits)));
}

typedef struct BloomOpaque
{
	/*
	 * XXX At this point we only need a single proc, but let's keep the
	 * array just like inclusion and minman opclasses, for consistency.
	 * Also, we may need additional procs in the future, e.g. to merge
	 * the bloom filters.
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
	 * bloom indexes only store a the filter as a single BYTEA column
	 */

	result = palloc0(MAXALIGN(SizeofBrinOpcInfo(1)) +
					 sizeof(BloomOpaque));
	result->oi_nstored = 1;
	result->oi_opaque = (BloomOpaque *)
		MAXALIGN((char *) result + SizeofBrinOpcInfo(1));
	result->oi_typcache[0] = lookup_type_cache(BYTEAOID, 0);

	PG_RETURN_POINTER(result);
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
	bool		isnull = PG_GETARG_DATUM(3);
	Oid			colloid = PG_GET_COLLATION();
	FmgrInfo   *hashFn;
	uint32		hashValue;
	bool		updated = false;
	AttrNumber	attno;
	BloomFilter *filter;

	/*
	 * If the new value is null, we record that we saw it if it's the first
	 * one; otherwise, there's nothing to do.
	 */
	if (isnull)
	{
		if (column->bv_hasnulls)
			PG_RETURN_BOOL(false);

		column->bv_hasnulls = true;
		PG_RETURN_BOOL(true);
	}

	attno = column->bv_attno;

	/*
	 * If this is the first non-null value, we need to initialize the bloom
	 * filter. Otherwise just extract the existing bloom filter from BrinValues.
	 */
	if (column->bv_allnulls)
	{
		filter = bloom_init(1000, 0.05);
		column->bv_values[0] = PointerGetDatum(filter);
		column->bv_allnulls = false;
		updated = true;
	}
	else
		filter = (BloomFilter *) DatumGetPointer(column->bv_values[0]);

	elog(DEBUG1, "brin_bloom_add_value: filter=%p bits=%d hashes=%d values=%d set=%d estimate=%d",
				  filter, filter->nbits, filter->nhashes, filter->nvalues, filter->nbits_set,
				  bloom_filter_count(filter));

	/*
	 * Compute the hash of the new value, using the supplied hash function,
	 * and then add the hash value to the bloom filter.
	 */
	hashFn = bloom_get_procinfo(bdesc, attno, PROCNUM_HASH);

	hashValue = DatumGetUInt32(FunctionCall1Coll(hashFn, colloid, newval));

	if (bloom_add_value(filter, hashValue))
		updated = true;

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
	ScanKey		key = (ScanKey) PG_GETARG_POINTER(2);
	Oid			colloid = PG_GET_COLLATION();
	AttrNumber	attno;
	Datum		value;
	Datum		matches;
	FmgrInfo   *finfo;
	uint32		hashValue;
	BloomFilter *filter;

	Assert(key->sk_attno == column->bv_attno);

	/* handle IS NULL/IS NOT NULL tests */
	if (key->sk_flags & SK_ISNULL)
	{
		if (key->sk_flags & SK_SEARCHNULL)
		{
			if (column->bv_allnulls || column->bv_hasnulls)
				PG_RETURN_BOOL(true);
			PG_RETURN_BOOL(false);
		}

		/*
		 * For IS NOT NULL, we can only skip ranges that are known to have
		 * only nulls.
		 */
		if (key->sk_flags & SK_SEARCHNOTNULL)
			PG_RETURN_BOOL(!column->bv_allnulls);

		/*
		 * Neither IS NULL nor IS NOT NULL was used; assume all indexable
		 * operators are strict and return false.
		 */
		PG_RETURN_BOOL(false);
	}

	/* if the range is all empty, it cannot possibly be consistent */
	if (column->bv_allnulls)
		PG_RETURN_BOOL(false);

	filter = (BloomFilter *) DatumGetPointer(column->bv_values[0]);

	Assert(filter);

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
			matches = bloom_contains_value(filter, hashValue);

			break;
		default:
			/* shouldn't happen */
			elog(ERROR, "invalid strategy number %d", key->sk_strategy);
			matches = 0;
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
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *col_a = (BrinValues *) PG_GETARG_POINTER(1);
	BrinValues *col_b = (BrinValues *) PG_GETARG_POINTER(2);
	AttrNumber	attno;
	Form_pg_attribute attr;

	Assert(col_a->bv_attno == col_b->bv_attno);

	/* Adjust "hasnulls" */
	if (!col_a->bv_hasnulls && col_b->bv_hasnulls)
		col_a->bv_hasnulls = true;

	/* If there are no values in B, there's nothing left to do */
	if (col_b->bv_allnulls)
		PG_RETURN_VOID();

	attno = col_a->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	/*
	 * Adjust "allnulls".  If A doesn't have values, just copy the values from
	 * B into A, and we're done.  We cannot run the operators in this case,
	 * because values in A might contain garbage.  Note we already established
	 * that B contains values.
	 */
	if (col_a->bv_allnulls)
	{
		col_a->bv_allnulls = false;
		col_a->bv_values[0] = datumCopy(col_b->bv_values[0],
										attr->attbyval, attr->attlen);
		PG_RETURN_VOID();
	}

	/* FIXME merge the two bloom filters */
	elog(DEBUG1, "FIXME: merge bloom filters");

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
