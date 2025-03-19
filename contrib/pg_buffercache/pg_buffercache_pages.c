/*-------------------------------------------------------------------------
 *
 * pg_buffercache_pages.c
 *	  display some contents of the buffer cache
 *
 *	  contrib/pg_buffercache/pg_buffercache_pages.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "port/pg_numa.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"


#define NUM_BUFFERCACHE_PAGES_MIN_ELEM	8
#define NUM_BUFFERCACHE_PAGES_ELEM	10
#define NUM_BUFFERCACHE_SUMMARY_ELEM 5
#define NUM_BUFFERCACHE_USAGE_COUNTS_ELEM 4

PG_MODULE_MAGIC_EXT(
					.name = "pg_buffercache",
					.version = PG_VERSION
);

/*
 * Record structure holding the to be exposed cache data.
 */
typedef struct
{
	uint32		bufferid;
	RelFileNumber relfilenumber;
	Oid			reltablespace;
	Oid			reldatabase;
	ForkNumber	forknum;
	BlockNumber blocknum;
	bool		isvalid;
	bool		isdirty;
	uint16		usagecount;

	/*
	 * An int32 is sufficiently large, as MAX_BACKENDS prevents a buffer from
	 * being pinned by too many backends and each backend will only pin once
	 * because of bufmgr.c's PrivateRefCount infrastructure.
	 */
	int32		pinning_backends;
	int32		numa_node_id;
} BufferCachePagesRec;


/*
 * Function context for data persisting over repeated calls.
 */
typedef struct
{
	TupleDesc	tupdesc;
	BufferCachePagesRec *record;
} BufferCachePagesContext;


/*
 * Function returning data from the shared buffer cache - buffer number,
 * relation node/tablespace/database/blocknum and dirty indicator.
 */
PG_FUNCTION_INFO_V1(pg_buffercache_pages);
PG_FUNCTION_INFO_V1(pg_buffercache_numa_pages);
PG_FUNCTION_INFO_V1(pg_buffercache_summary);
PG_FUNCTION_INFO_V1(pg_buffercache_usage_counts);
PG_FUNCTION_INFO_V1(pg_buffercache_evict);

/* Only need to touch memory once per backend process lifetime */
static bool firstNumaTouch = true;

/*
 * Helper routine to map Buffers into addresses that is used by
 * pg_numa_query_pages().
 *
 * When database block size (BLCKSZ) is smaller than the OS page size (4kB),
 * multiple database buffers will map to the same OS memory page. In this case,
 * we only need to query the NUMA node for the first memory address of each
 * unique OS page rather than for every buffer.
 *
 * In order to get reliable results we also need to touch memory pages, so that
 * inquiry about NUMA memory node doesn't return -2 (which indicates
 * unmapped/unallocated pages).
 */
static inline void
pg_buffercache_numa_prepare_ptrs(int buffer_id, float pages_per_blk,
								 Size os_page_size,
								 void **os_page_ptrs)
{
	size_t		blk2page = (size_t) (buffer_id * pages_per_blk);

	for (size_t j = 0; j < pages_per_blk; j++)
	{
		size_t		blk2pageoff = blk2page + j;

		if (os_page_ptrs[blk2pageoff] == 0)
		{
			volatile uint64 touch pg_attribute_unused();

			/* NBuffers starts from 1 */
			os_page_ptrs[blk2pageoff] = (char *) BufferGetBlock(buffer_id + 1) +
				(os_page_size * j);

			/* Only need to touch memory once per backend process lifetime */
			if (firstNumaTouch)
				pg_numa_touch_mem_if_required(touch, os_page_ptrs[blk2pageoff]);

		}

		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * Helper routine for pg_buffercache_pages() and pg_buffercache_numa_pages().
 */
static BufferCachePagesContext *
pg_buffercache_init_entries(FuncCallContext *funcctx, FunctionCallInfo fcinfo)
{
	BufferCachePagesContext *fctx;	/* User function context. */
	MemoryContext oldcontext;
	TupleDesc	tupledesc;
	TupleDesc	expected_tupledesc;

	/* Switch context when allocating stuff to be used in later calls */
	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	/* Create a user function context for cross-call persistence */
	fctx = (BufferCachePagesContext *) palloc(sizeof(BufferCachePagesContext));

	/*
	 * To smoothly support upgrades from version 1.0 of this extension
	 * transparently handle the (non-)existence of the pinning_backends
	 * column. ee unfortunately have to get the result type for that... - we
	 * can't use the result type determined by the function definition without
	 * potentially crashing when somebody uses the old (or even wrong)
	 * function definition though.
	 */
	if (get_call_result_type(fcinfo, NULL, &expected_tupledesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	if (expected_tupledesc->natts < NUM_BUFFERCACHE_PAGES_MIN_ELEM ||
		expected_tupledesc->natts > NUM_BUFFERCACHE_PAGES_ELEM)
		elog(ERROR, "incorrect number of output arguments");

	/* Construct a tuple descriptor for the result rows. */
	tupledesc = CreateTemplateTupleDesc(expected_tupledesc->natts);
	TupleDescInitEntry(tupledesc, (AttrNumber) 1, "bufferid",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 2, "relfilenode",
					   OIDOID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 3, "reltablespace",
					   OIDOID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 4, "reldatabase",
					   OIDOID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 5, "relforknumber",
					   INT2OID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 6, "relblocknumber",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 7, "isdirty",
					   BOOLOID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 8, "usage_count",
					   INT2OID, -1, 0);

	if (expected_tupledesc->natts >= NUM_BUFFERCACHE_PAGES_ELEM - 1)
		TupleDescInitEntry(tupledesc, (AttrNumber) 9, "pinning_backends",
						   INT4OID, -1, 0);
	if (expected_tupledesc->natts == NUM_BUFFERCACHE_PAGES_ELEM)
		TupleDescInitEntry(tupledesc, (AttrNumber) 10, "node_id",
						   INT4OID, -1, 0);

	fctx->tupdesc = BlessTupleDesc(tupledesc);

	/* Allocate NBuffers worth of BufferCachePagesRec records. */
	fctx->record = (BufferCachePagesRec *)
		MemoryContextAllocHuge(CurrentMemoryContext,
							   sizeof(BufferCachePagesRec) * NBuffers);

	/* Set max calls and remember the user function context. */
	funcctx->max_calls = NBuffers;
	funcctx->user_fctx = fctx;

	/* Return to original context when allocating transient memory */
	MemoryContextSwitchTo(oldcontext);
	return fctx;
}

/*
 * Helper routine for pg_buffercache_pages() and pg_buffercache_numa_pages().
 *
 * Save buffer cache information for a single buffer.
 */
static void
pg_buffercache_save_tuple(int record_id, BufferCachePagesContext *fctx)
{
	BufferDesc *bufHdr;
	uint32		buf_state;
	BufferCachePagesRec *bufRecord = &(fctx->record[record_id]);

	bufHdr = GetBufferDescriptor(record_id);
	/* Lock each buffer header before inspecting. */
	buf_state = LockBufHdr(bufHdr);

	bufRecord->bufferid = BufferDescriptorGetBuffer(bufHdr);
	bufRecord->relfilenumber = BufTagGetRelNumber(&bufHdr->tag);
	bufRecord->reltablespace = bufHdr->tag.spcOid;
	bufRecord->reldatabase = bufHdr->tag.dbOid;
	bufRecord->forknum = BufTagGetForkNum(&bufHdr->tag);
	bufRecord->blocknum = bufHdr->tag.blockNum;
	bufRecord->usagecount = BUF_STATE_GET_USAGECOUNT(buf_state);
	bufRecord->pinning_backends = BUF_STATE_GET_REFCOUNT(buf_state);

	if (buf_state & BM_DIRTY)
		bufRecord->isdirty = true;
	else
		bufRecord->isdirty = false;

	/* Note if the buffer is valid, and has storage created */
	if ((buf_state & BM_VALID) && (buf_state & BM_TAG_VALID))
		bufRecord->isvalid = true;
	else
		bufRecord->isvalid = false;

	bufRecord->numa_node_id = -1;

	UnlockBufHdr(bufHdr, buf_state);
}

/*
 * Helper routine for pg_buffercache_pages() and pg_buffercache_numa_pages().
 *
 * Format and return a tuple for a single buffer cache entry.
 */
static Datum
get_buffercache_tuple(int record_id, BufferCachePagesContext *fctx)
{
	Datum		values[NUM_BUFFERCACHE_PAGES_ELEM];
	bool		nulls[NUM_BUFFERCACHE_PAGES_ELEM];
	HeapTuple	tuple;
	BufferCachePagesRec *bufRecord = &(fctx->record[record_id]);

	values[0] = Int32GetDatum(bufRecord->bufferid);
	memset(nulls, false, NUM_BUFFERCACHE_PAGES_ELEM);

	/*
	 * Set all fields except the bufferid to null if the buffer is unused or
	 * not valid.
	 */
	if (bufRecord->blocknum == InvalidBlockNumber ||
		bufRecord->isvalid == false)
	{
		int			i;

		for (i = 1; i <= 9; i++)
			nulls[i] = true;
	}
	else
	{
		values[1] = ObjectIdGetDatum(bufRecord->relfilenumber);
		values[2] = ObjectIdGetDatum(bufRecord->reltablespace);
		values[3] = ObjectIdGetDatum(bufRecord->reldatabase);
		values[4] = ObjectIdGetDatum(bufRecord->forknum);
		values[5] = Int64GetDatum((int64) bufRecord->blocknum);
		values[6] = BoolGetDatum(bufRecord->isdirty);
		values[7] = Int16GetDatum(bufRecord->usagecount);

		/*
		 * unused for v1.0 callers, but the array is always long enough
		 */
		values[8] = Int32GetDatum(bufRecord->pinning_backends);
		values[9] = Int32GetDatum(bufRecord->numa_node_id);
	}

	/* Build and return the tuple. */
	tuple = heap_form_tuple(fctx->tupdesc, values, nulls);
	return HeapTupleGetDatum(tuple);
}

Datum
pg_buffercache_pages(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	BufferCachePagesContext *fctx;	/* User function context. */

	if (SRF_IS_FIRSTCALL())
	{
		int			i;

		funcctx = SRF_FIRSTCALL_INIT();
		fctx = pg_buffercache_init_entries(funcctx, fcinfo);

		/*
		 * Scan through all the buffers, saving the relevant fields in the
		 * fctx->record structure.
		 *
		 * We don't hold the partition locks, so we don't get a consistent
		 * snapshot across all buffers, but we do grab the buffer header
		 * locks, so the information of each buffer is self-consistent.
		 */
		for (i = 0; i < NBuffers; i++)
			pg_buffercache_save_tuple(i, fctx);
	}

	funcctx = SRF_PERCALL_SETUP();

	/* Get the saved state */
	fctx = funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		Datum		result;
		uint32		i = funcctx->call_cntr;

		result = get_buffercache_tuple(i, fctx);
		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * This is almost identical to the above, but performs
 * NUMA inuqiry about memory mappings.
 */
Datum
pg_buffercache_numa_pages(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	BufferCachePagesContext *fctx;	/* User function context. */

	if (SRF_IS_FIRSTCALL())
	{
		int			i;
		Size		os_page_size = 0;
		void	  **os_page_ptrs = NULL;
		int		   *os_pages_status = NULL;
		uint64		os_page_count = 0;
		float		pages_per_blk = 0;

		funcctx = SRF_FIRSTCALL_INIT();

		if (pg_numa_init() == -1)
			elog(ERROR, "libnuma initialization failed or NUMA is not supported on this platform");

		fctx = pg_buffercache_init_entries(funcctx, fcinfo);

		/*
		 * Different database block sizes (4kB, 8kB, ..., 32kB) can be used,
		 * while the OS may have different memory page sizes.
		 *
		 * To correctly map between them, we need to: 1. Determine the OS
		 * memory page size 2. Calculate how many OS pages are used by all
		 * buffer blocks 3. Calculate how many OS pages are contained within
		 * each database block.
		 *
		 * This information is needed before calling move_pages() for NUMA
		 * node id inquiry.
		 */
		os_page_size = pg_numa_get_pagesize();
		os_page_count = ((uint64) NBuffers * BLCKSZ) / os_page_size;
		pages_per_blk = (float) BLCKSZ / os_page_size;

		elog(DEBUG1, "NUMA: os_page_count=%lu os_page_size=%zu pages_per_blk=%.2f",
			 (unsigned long) os_page_count, os_page_size, pages_per_blk);

		os_page_ptrs = palloc0(sizeof(void *) * os_page_count);
		os_pages_status = palloc(sizeof(uint64) * os_page_count);

		/*
		 * If we ever get 0xff back from kernel inquiry, then we probably have
		 * bug in our buffers to OS page mapping code here.
		 *
		 */
		memset(os_pages_status, 0xff, sizeof(int) * os_page_count);

		if (firstNumaTouch)
			elog(DEBUG1, "NUMA: page-faulting the buffercache for proper NUMA readouts");

		/*
		 * Scan through all the buffers, saving the relevant fields in the
		 * fctx->record structure.
		 *
		 * We don't hold the partition locks, so we don't get a consistent
		 * snapshot across all buffers, but we do grab the buffer header
		 * locks, so the information of each buffer is self-consistent.
		 */
		for (i = 0; i < NBuffers; i++)
		{
			pg_buffercache_save_tuple(i, fctx);
			pg_buffercache_numa_prepare_ptrs(i, pages_per_blk, os_page_size,
											 os_page_ptrs);
		}

		if (pg_numa_query_pages(0, os_page_count, os_page_ptrs, os_pages_status) == -1)
			elog(ERROR, "failed NUMA pages inquiry: %m");

		for (i = 0; i < NBuffers; i++)
		{
			int			blk2page = (int) i * pages_per_blk;

			/*
			 * Set the NUMA node id for this buffer based on the first OS page
			 * it maps to.
			 *
			 * Note: We could check for errors in os_pages_status and report
			 * them. Also, a single DB block might span multiple NUMA nodes if
			 * it crosses OS pages on node boundaries, but we only record the
			 * node of the first page. This is a simplification but should be
			 * sufficient for most analyses.
			 */
			fctx->record[i].numa_node_id = os_pages_status[blk2page];
		}
	}

	funcctx = SRF_PERCALL_SETUP();

	/* Get the saved state */
	fctx = funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		Datum		result;
		uint32		i = funcctx->call_cntr;

		result = get_buffercache_tuple(i, fctx);
		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		firstNumaTouch = false;
		SRF_RETURN_DONE(funcctx);
	}
}

Datum
pg_buffercache_summary(PG_FUNCTION_ARGS)
{
	Datum		result;
	TupleDesc	tupledesc;
	HeapTuple	tuple;
	Datum		values[NUM_BUFFERCACHE_SUMMARY_ELEM];
	bool		nulls[NUM_BUFFERCACHE_SUMMARY_ELEM];

	int32		buffers_used = 0;
	int32		buffers_unused = 0;
	int32		buffers_dirty = 0;
	int32		buffers_pinned = 0;
	int64		usagecount_total = 0;

	if (get_call_result_type(fcinfo, NULL, &tupledesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	for (int i = 0; i < NBuffers; i++)
	{
		BufferDesc *bufHdr;
		uint32		buf_state;

		/*
		 * This function summarizes the state of all headers. Locking the
		 * buffer headers wouldn't provide an improved result as the state of
		 * the buffer can still change after we release the lock and it'd
		 * noticeably increase the cost of the function.
		 */
		bufHdr = GetBufferDescriptor(i);
		buf_state = pg_atomic_read_u32(&bufHdr->state);

		if (buf_state & BM_VALID)
		{
			buffers_used++;
			usagecount_total += BUF_STATE_GET_USAGECOUNT(buf_state);

			if (buf_state & BM_DIRTY)
				buffers_dirty++;
		}
		else
			buffers_unused++;

		if (BUF_STATE_GET_REFCOUNT(buf_state) > 0)
			buffers_pinned++;
	}

	memset(nulls, 0, sizeof(nulls));
	values[0] = Int32GetDatum(buffers_used);
	values[1] = Int32GetDatum(buffers_unused);
	values[2] = Int32GetDatum(buffers_dirty);
	values[3] = Int32GetDatum(buffers_pinned);

	if (buffers_used != 0)
		values[4] = Float8GetDatum((double) usagecount_total / buffers_used);
	else
		nulls[4] = true;

	/* Build and return the tuple. */
	tuple = heap_form_tuple(tupledesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

Datum
pg_buffercache_usage_counts(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			usage_counts[BM_MAX_USAGE_COUNT + 1] = {0};
	int			dirty[BM_MAX_USAGE_COUNT + 1] = {0};
	int			pinned[BM_MAX_USAGE_COUNT + 1] = {0};
	Datum		values[NUM_BUFFERCACHE_USAGE_COUNTS_ELEM];
	bool		nulls[NUM_BUFFERCACHE_USAGE_COUNTS_ELEM] = {0};

	InitMaterializedSRF(fcinfo, 0);

	for (int i = 0; i < NBuffers; i++)
	{
		BufferDesc *bufHdr = GetBufferDescriptor(i);
		uint32		buf_state = pg_atomic_read_u32(&bufHdr->state);
		int			usage_count;

		usage_count = BUF_STATE_GET_USAGECOUNT(buf_state);
		usage_counts[usage_count]++;

		if (buf_state & BM_DIRTY)
			dirty[usage_count]++;

		if (BUF_STATE_GET_REFCOUNT(buf_state) > 0)
			pinned[usage_count]++;
	}

	for (int i = 0; i < BM_MAX_USAGE_COUNT + 1; i++)
	{
		values[0] = Int32GetDatum(i);
		values[1] = Int32GetDatum(usage_counts[i]);
		values[2] = Int32GetDatum(dirty[i]);
		values[3] = Int32GetDatum(pinned[i]);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}

/*
 * Try to evict a shared buffer.
 */
Datum
pg_buffercache_evict(PG_FUNCTION_ARGS)
{
	Buffer		buf = PG_GETARG_INT32(0);

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pg_buffercache_evict function")));

	if (buf < 1 || buf > NBuffers)
		elog(ERROR, "bad buffer ID: %d", buf);

	PG_RETURN_BOOL(EvictUnpinnedBuffer(buf));
}
