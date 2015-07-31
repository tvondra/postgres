/*
 * contrib/pageinspect/colfuncs.c
 *
 *
 * colfuncs.c
 *
 * Copyright (c) 2006 Satoshi Nagayasu <nagayasus@nttdata.co.jp>
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose, without fee, and without a
 * written agreement is hereby granted, provided that the above
 * copyright notice and this paragraph and the following two
 * paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT,
 * INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
 * IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

#include "postgres.h"

#include "access/columnar.h"
#include "catalog/namespace.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/rel.h"

PG_FUNCTION_INFO_V1(col_page_segments);

#define IS_INDEX(r) ((r)->rd_rel->relkind == RELKIND_INDEX)
#define IS_COLUMNAR(r) ((r)->rd_rel->relam == COLUMNAR_AM_OID)

/* note: BlockNumber is unsigned, hence can't be negative */
#define CHECK_RELATION_BLOCK_RANGE(rel, blkno) { \
		if ( RelationGetNumberOfBlocks(rel) <= (BlockNumber) (blkno) ) \
			 elog(ERROR, "block number out of range"); }

/*
 * cross-call data structure for SRF
 */
struct user_args
{
	Page		page;
	PageSegment	seg;
};

Datum
col_page_segments(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_P(0);
	uint32		blkno = PG_GETARG_UINT32(1);
	Datum		result;
	char	   *values[10];
	HeapTuple	tuple;
	FuncCallContext *fctx;
	MemoryContext mctx;
	struct user_args *uargs;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pageinspect functions"))));

	if (SRF_IS_FIRSTCALL())
	{
		RangeVar   *relrv;
		Relation	rel;
		Buffer		buffer;
		TupleDesc	tupleDesc;

		fctx = SRF_FIRSTCALL_INIT();

		relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
		rel = relation_openrv(relrv, AccessShareLock);

		if (!IS_INDEX(rel) || !IS_COLUMNAR(rel))
			elog(ERROR, "relation \"%s\" is not a columnar index",
				 RelationGetRelationName(rel));

		/*
		 * Reject attempts to read non-local temporary relations; we would be
		 * likely to get wrong data since we have no visibility into the
		 * owning session's local buffers.
		 */
		if (RELATION_IS_OTHER_TEMP(rel))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("cannot access temporary tables of other sessions")));

		CHECK_RELATION_BLOCK_RANGE(rel, blkno);

		buffer = ReadBuffer(rel, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		/*
		 * We copy the page into local storage to avoid holding pin on the
		 * buffer longer than we must, and possibly failing to release it at
		 * all if the calling query doesn't fetch all rows.
		 */
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		uargs = palloc(sizeof(struct user_args));

		uargs->page = palloc(BLCKSZ);
		memcpy(uargs->page, BufferGetPage(buffer), BLCKSZ);

		UnlockReleaseBuffer(buffer);
		relation_close(rel, AccessShareLock);

		/* first segment */
		uargs->seg = (PageSegment)PageGetContents(uargs->page);

		/* one segment per column, plus TID segment */
		fctx->max_calls = (RelationGetDescr(rel)->natts+1);

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		fctx->attinmeta = TupleDescGetAttInMetadata(tupleDesc);

		fctx->user_fctx = uargs;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();
	uargs = fctx->user_fctx;

	if (fctx->call_cntr < fctx->max_calls)
	{
		PageSegment	seg;
		int			j;
		char	   *algo = "unknown";

		seg = uargs->seg;

		if (seg->seg_flags & SEG_COMPRESS_AUTO)
			algo = "auto";
		else if (seg->seg_flags & SEG_COMPRESS_DICT)
			algo = "dict";
		else if (seg->seg_flags & SEG_COMPRESS_RLE)
			algo = "rle";
		else if (seg->seg_flags & SEG_COMPRESS_PGLZ)
			algo = "pglz";

		j = 0;
		values[j++] = psprintf("%d", fctx->call_cntr);
		values[j++] = psprintf("%d", seg->seg_len);
		values[j++] = psprintf("%d", seg->seg_used_len);
		values[j++] = psprintf("%d", seg->seg_comp_len);
		values[j++] = psprintf("%d", seg->seg_raw_len);
		values[j++] = psprintf("%d", seg->seg_nitems);
		values[j++] = psprintf("%d", seg->seg_flags);
		values[j++] = psprintf("%s", algo);
		values[j++] = psprintf("%d", ((seg->seg_flags & SEG_COMPRESS_DISABLED) != 0));
		values[j++] = psprintf("%d", ((seg->seg_flags & SEG_NULLS_DISABLED) == 0));

		tuple = BuildTupleFromCStrings(fctx->attinmeta, values);
		result = HeapTupleGetDatum(tuple);

		uargs->seg = (PageSegment)((char*)uargs->seg + seg->seg_len);

		SRF_RETURN_NEXT(fctx, result);
	}
	else
	{
		pfree(uargs->page);
		pfree(uargs);
		SRF_RETURN_DONE(fctx);
	}
}
