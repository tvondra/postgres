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

typedef struct BloomFilter
{
	/* varlena header (do not touch directly!) */
	int32	vl_len_;

	int		nhashes;	/* number of hash functions */
	int		nbits;		/* number of bits in the bloom filter */
	int		nvalues;	/* number of values added */
	int		nsets;		/* number of bits set */

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

	elog(WARNING, "create bloom filter m=%d k=%d", m, k);

	/* allocate the bloom filter */
	len = offsetof(BloomFilter, bitmap) + (m/8);
	filter = (BloomFilter *) palloc0(len);

	filter->nhashes = k;
	filter->nbits = m;

	SET_VARSIZE(filter, len);

	return filter;
}

static bool
bloom_add_value(BloomFilter *filter, uint32 value)
{
	int		i;
	bool	updated = false;

	Assert(filter);

	for (i = 0; i < filter->nhashes; i++)
	{
		uint32 h = (value + i * 937) % filter->nbits;

		int byte = (h / 8);
		int bit  = (h % 8);

		/* if the bit is not set, set it and remember we did that */
		if (! (filter->bitmap[byte] & (0x01 << bit)))
		{
			filter->bitmap[byte] |= (0x01 << bit);
			filter->nsets++;
			updated = true;
		}
	}

	filter->nvalues++;

	return updated;
}

static bool
bloom_contains_value(BloomFilter *filter, uint32 value)
{
	int		i;
	bool	contains = true;

	Assert(filter);

	for (i = 0; i < filter->nhashes; i++)
	{
		uint32 h = (value + i * 937) % filter->nbits;

		int byte = (h / 8);
		int bit  = (h % 8);

		/* if the bit is not set, the value is not there */
		if (! (filter->bitmap[byte] & (0x01 << bit)))
		{
			contains = false;
			break;
		}
	}

	filter->nvalues++;

	return contains;
}

static int
bloom_filter_count(BloomFilter *filter)
{
	return ceil(- filter->nbits * 1.0 / filter->nhashes * log(1 - filter->nsets * 1.0 / filter->nbits));
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

	elog(WARNING, "brin_bloom_add_value: filter=%p bits=%d hashes=%d values=%d set=%d count=%d",
				  filter, filter->nbits, filter->nhashes, filter->nvalues, filter->nsets,
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
		case BTEqualStrategyNumber:

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
	elog(WARNING, "FIXME: merge bloom filters");

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
