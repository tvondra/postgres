/*-------------------------------------------------------------------------
 *
 * columnar.c
 *	  Implementation of columnar indexes for Postgres.
 *
 * NOTES
 *	  This file contains only the public interface routines.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/columnar/columnar.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/columnar.h"
#include "access/relscan.h"
#include "access/xlog.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "storage/indexfsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"

#include "miscadmin.h"

#include "access/nbtree.h"
#include "utils/lsyscache.h"

typedef struct ColBuildState
{
	Relation	heapRel;
	double		indtuples;

	/* state data for tuplesort.c */
	Tuplesortstate *sortstate;

} ColBuildState;

static bool _col_buffer_empty(IndexScanDesc scan, ColumnScanOpaque opaque);
static bool _col_load_buffer(Relation rel, ColumnScanOpaque so, int mincount);

static void _col_preprocess_keys(IndexScanDesc scan);

static int
_col_scankey_update_bitmap(Relation rel,
						   int keysz,
						   ScanKey key,
						   AttrNumber attno,
						   int nrows,
						   Datum *values,
						   bool *isnull,
						   bool *bitmap,
						   int nmatching);

static bool _bt_compare_scankey_args(IndexScanDesc scan, ScanKey op,
						 ScanKey leftarg, ScanKey rightarg,
						 bool *result);
static bool _bt_fix_scankey_strategy(ScanKey skey, int16 *indoption);
static void _bt_mark_scankey_required(ScanKey skey);

#define PageGetValue(page, opaque, nth, len) \
	((char*)page + opaque->colpo_data_offset + (nth) * (len))

#define PageGetNull(page, opaque, nth) \
	(*((char*)page + opaque->colpo_nulls_offset + ((nth)/8)) & (0x01 << ((nth) % 8)))

#define PageGetItemPointer(page, opaque, nth) \
	((char*)page + opaque->colpo_tids_offset + (nth) * sizeof(ItemPointerData))

static void
colbuildCallback(Relation index,
				HeapTuple htup,
				Datum *values,
				bool *isnull,
				bool tupleIsAlive,
				void *state);

/*
 *	colbuild() -- build a new columnar index.
 */
Datum
colbuild(PG_FUNCTION_ARGS)
{
	IndexTuple	itup;
	Relation	heap = (Relation) PG_GETARG_POINTER(0);
	Relation	index = (Relation) PG_GETARG_POINTER(1);
	IndexInfo  *indexInfo = (IndexInfo *) PG_GETARG_POINTER(2);

	TupleDesc	tdesc = RelationGetDescr(index);
	Datum	   *values = palloc0(tdesc->natts * sizeof(Datum));
	bool	   *isnull = palloc0(tdesc->natts * sizeof(bool));

	IndexBuildResult *result;
	double		reltuples;
	bool		should_free;
	ColBuildState buildstate;

	buildstate.sortstate
		= tuplesort_begin_index_btree(heap, index, false,
									  maintenance_work_mem, false);

	buildstate.heapRel = heap;
	buildstate.indtuples = 0;

	/*
	 * We expect to be called exactly once for any index relation. If that's
	 * not the case, big trouble's what we have.
	 */
	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	/* no UNIQUE support */
	if (indexInfo->ii_Unique)
		elog(ERROR, "columnar indexes do not support UNIQUE");

	/* do the heap scan */
	reltuples = IndexBuildHeapScan(heap, index, indexInfo, true,
								   colbuildCallback, (void *) &buildstate);

	tuplesort_performsort(buildstate.sortstate);

	while ((itup = tuplesort_getindextuple(buildstate.sortstate,
										   true, &should_free)) != NULL)
	{
		index_deform_tuple(itup, tdesc, values, isnull);

		_col_doinsert(index, &(itup->t_tid), values, isnull,
					  buildstate.heapRel);

		if (should_free)
			pfree(itup);
	}

	tuplesort_end(buildstate.sortstate);

	/*
	 * Return statistics
	 */
	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;

	PG_RETURN_POINTER(result);
}

/*
 * Per-tuple callback from IndexBuildHeapScan
 */
static void
colbuildCallback(Relation index,
				HeapTuple htup,
				Datum *values,
				bool *isnull,
				bool tupleIsAlive,
				void *state)
{
	ColBuildState  *buildstate = (ColBuildState *) state;

	tuplesort_putindextuplevalues(buildstate->sortstate, index,
								  &htup->t_self, values, isnull);

	buildstate->indtuples += 1;
}

/*
 *	colbuildempty() -- build an empty columnar index in the initialization fork
 */
Datum
colbuildempty(PG_FUNCTION_ARGS)
{
	elog(WARNING, "colbuildempty() : done (nothing to do here)");

	PG_RETURN_VOID();
}

/*
 *	colinsert() -- insert an index tuple into a column index.
 *
 *		Descend the tree recursively, find the appropriate location for our
 *		new tuple, and put it there.
 */
Datum
colinsert(PG_FUNCTION_ARGS)
{
	bool		result = false;
	Relation	rel = (Relation) PG_GETARG_POINTER(0);
	Datum	   *values = (Datum *) PG_GETARG_POINTER(1);
	bool	   *isnull = (bool *) PG_GETARG_POINTER(2);
	ItemPointer tid = (ItemPointer) PG_GETARG_POINTER(3);
	Relation	heapRel = (Relation) PG_GETARG_POINTER(4);

	result = _col_doinsert(rel, tid, values, isnull, heapRel);

	PG_RETURN_BOOL(result);
}

/*
 *	colgettuple() -- Get the next tuple in the scan.
 */
Datum
colgettuple(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	// ScanDirection dir = (ScanDirection) PG_GETARG_INT32(1);

	ColumnScanOpaque so = (ColumnScanOpaque) scan->opaque;

	/* columnar indexes are never lossy */
	scan->xs_recheck = false;

	/* if the current buffer is empty, try to fill it with data */
	if (_col_buffer_empty(scan, so))
		if (! _col_load_buffer(scan->indexRelation, so, 1000))
			PG_RETURN_BOOL(false);

	if (scan->xs_want_itup)
		scan->xs_itup = so->tuples[so->cur_item];

	/*
	 * Store the TID into the heap tuple too.
	 * 
	 * XXX is this really necessary?
	 */
	scan->xs_ctup.t_self = so->tuples[so->cur_item]->t_tid;

	so->cur_item++;

	PG_RETURN_BOOL(true);
}


/*
 * colgetbitmap() -- gets all matching tuples, and adds them to a bitmap
 */
Datum
colgetbitmap(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	TIDBitmap  *tbm = (TIDBitmap *) PG_GETARG_POINTER(1);
	ColumnScanOpaque so = (ColumnScanOpaque) scan->opaque;
	int64		ntids = 0;

	/* if the current buffer is empty, try to fill it with data */
	while (_col_load_buffer(scan->indexRelation, so, 1000))
	{
		while (! _col_buffer_empty(scan, so))
		{
			tbm_add_tuples(tbm, &so->tuples[so->cur_item]->t_tid, 1, false);
			so->cur_item++;
			ntids++;
		}
	}

	PG_RETURN_INT64(ntids);
}

/*
 *	colbeginscan() -- start a scan on a columnar index
 */
Datum
colbeginscan(PG_FUNCTION_ARGS)
{
	Relation			rel = (Relation) PG_GETARG_POINTER(0);
	int					nkeys = PG_GETARG_INT32(1);
	int					norderbys = PG_GETARG_INT32(2);
	IndexScanDesc		scan;
	ColumnScanOpaque	so;

	/* no order by operators allowed */
	Assert(norderbys == 0);

	/* get the scan */
	scan = RelationGetIndexScan(rel, nkeys, norderbys);

	/* allocate private workspace */
	so = (ColumnScanOpaque) palloc(sizeof(ColumnScanOpaqueData));

	/* reset the current position */
	so->max_page = RelationGetNumberOfBlocks(rel);
	so->cur_page = InvalidBlockNumber;

	if (scan->numberOfKeys > 0)
		so->keyData = (ScanKey) palloc(scan->numberOfKeys * sizeof(ScanKeyData));
	else
		so->keyData = NULL;

	so->tuples = NULL;

	scan->xs_itupdesc = RelationGetDescr(rel);

	scan->opaque = so;

	so->tmpctx = AllocSetContextCreate(CurrentMemoryContext,
									"columnar temporary context",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);

	so->bufctx = AllocSetContextCreate(CurrentMemoryContext,
									"columnar buffer context",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);

	elog(WARNING, "colbeginscan() : done");

	PG_RETURN_POINTER(scan);
}

/*
 *	colrescan() -- rescan an index relation
 */
Datum
colrescan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	ScanKey		scankey = (ScanKey) PG_GETARG_POINTER(1);

	/* remaining arguments are ignored */
	ColumnScanOpaque so = (ColumnScanOpaque) scan->opaque;

	/* reset the current position */
	so->max_page = RelationGetNumberOfBlocks(scan->indexRelation);
	so->cur_page = InvalidBlockNumber;

	so->cur_item = 0;
	so->num_items = 0;

	/*
	 * Reset the scan keys. Note that keys ordering stuff moved to _bt_first.
	 * - vadim 05/05/97
	 */
	if (scankey && scan->numberOfKeys > 0)
		memmove(scan->keyData,
				scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
	so->numberOfKeys = 0;		/* until _col_preprocess_keys sets it */

	_col_preprocess_keys(scan);

	/* probably not entirely necessary */
	MemoryContextReset(so->tmpctx);
	MemoryContextReset(so->bufctx);

	elog(WARNING, "colrescan() : done");

	PG_RETURN_VOID();
}

/*
 *	colendscan() -- close down a scan
 */
Datum
colendscan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	ColumnScanOpaque so = (ColumnScanOpaque) scan->opaque;

	/* so->markTuples should not be pfree'd, see colrescan */
	pfree(so);

	elog(WARNING, "colendscan() : done");

	PG_RETURN_VOID();
}

/*
 *	colmarkpos() -- save current scan position
 */
Datum
colmarkpos(PG_FUNCTION_ARGS)
{
	elog(WARNING, "colmarkpos() : not implemented");

	PG_RETURN_VOID();
}

/*
 *	colrestrpos() -- restore scan to last saved position
 */
Datum
colrestrpos(PG_FUNCTION_ARGS)
{
	elog(WARNING, "colrestrpos() : not implemented");

	PG_RETURN_VOID();
}

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
Datum
colbulkdelete(PG_FUNCTION_ARGS)
{
	IndexBulkDeleteResult *volatile stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);

	elog(WARNING, "colbulkdelete() : not implemented");

	PG_RETURN_POINTER(stats);
}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
Datum
colvacuumcleanup(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);

	/* No-op in ANALYZE ONLY mode */
	if (info->analyze_only)
		PG_RETURN_POINTER(stats);

	elog(WARNING, "colvacuumcleanup() : not implemented");

	PG_RETURN_POINTER(stats);
}


/*
 *	btcanreturn() -- Check whether columnar indexes support index-only scans.
 *
 * columnar indexes always do, so this is trivial.
 */
Datum
colcanreturn(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(true);
}

Datum
coloptions(PG_FUNCTION_ARGS)
{
	elog(WARNING, "coloptions() : not implemented");
	PG_RETURN_NULL();
}

static bool
_col_load_buffer(Relation rel, ColumnScanOpaque so, int mincount)
{
	int		i, j, k;
	Buffer	buf;
	Page	page;
	ColumnarPageOpaque opaque;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			natts = tupdesc->natts;

	Datum  *values;
	bool   *isnull;

	MemoryContext	oldctx;

	/* decompressed data */
	char	  **data;
	char	  **nulls;
	ItemPointer	tids;

	/* bitmap for scan keys */
	bool		*bitmap;

nextpage:

	/* proceed to the next page */
	so->cur_page++;

	/* if we've exhausted all the pages, we're done */
	if (so->max_page == so->cur_page)
		return false;

	/* otherwise read the page and load it into the buffer */
	buf = _col_getbuf(rel, so->cur_page, COL_READ);

	page = BufferGetPage(buf);
	opaque = (ColumnarPageOpaque)PageGetSpecialPointer(page);

	/* reset both the buffer-level and temporary memory contexts */
	MemoryContextReset(so->tmpctx);
	MemoryContextReset(so->bufctx);

	/* we'll need a Datum, bool and ItemPointerData per item */
	so->cur_item = 0;
	so->num_items = opaque->colpo_nitems;

	/* if there are no block, skip to the next one, but cleanup first */
	if (so->num_items == 0)
		goto cleanup;

	/* switch to the temporary context */
	oldctx = MemoryContextSwitchTo(so->tmpctx);

	/* decompress the data */
	data   = _col_page_get_data(rel, page, opaque);
	nulls  = _col_page_get_nulls(rel, page, opaque);
	tids   = _col_page_get_tupleids(rel, page, opaque);

	/* build match bitmap of tuples */
	bitmap = palloc(sizeof(bool) * so->num_items);
	memset(bitmap, TRUE, so->num_items);

	/* FIXME this should be done on the compressed data, not here */
	if (so->numberOfKeys > 0)
	{
		int			nmatches = so->num_items;	/* all rows matching */
		AttrNumber	attno;
		Bitmapset  *attnos = NULL;

		Datum	*tmpvalues = (Datum*)palloc0(sizeof(Datum) * so->num_items);
		bool	*tmpisnull = (bool*)palloc0(sizeof(bool) * so->num_items);

		for (i = 0; i < so->numberOfKeys; i++)
			attnos = bms_add_member(attnos, so->keyData[i].sk_attno);

		/* apply the scan keys, and stop once we eliminate all tuples from this page */
		while (((attno = bms_first_member(attnos)) >= 0) && (nmatches > 0))
		{
			/* reset the arrays */
			memset(tmpvalues, 0, sizeof(Datum) * so->num_items);
			memset(tmpisnull, 0, sizeof(bool) * so->num_items);

			/* fill it with data */
			for (i = 0; i < so->num_items; i++)
			{
				memcpy(&tmpvalues[i],
					   &data[attno-1][i * tupdesc->attrs[attno-1]->attlen],
					   tupdesc->attrs[attno-1]->attlen);

				tmpisnull[i] = nulls[attno-1][i/8] & (0x01 << (i % 8));
			}

			/* update the bitmap using scan keys for this attnum */
			nmatches = _col_scankey_update_bitmap(rel,
									   so->numberOfKeys, so->keyData,
									   attno, so->num_items,
									   tmpvalues, tmpisnull, bitmap,
									   nmatches);
		}
	}

	/* allocate in the temporary memory context */
	values = (Datum*)palloc0(sizeof(Datum) * natts);
	isnull = (bool* )palloc0(sizeof(bool) * natts);

	/* now switch to the buffer context, but keep the original oldctx */
	MemoryContextSwitchTo(so->bufctx);

	so->tuples = (IndexTuple*)palloc(sizeof(IndexTuple) * so->num_items);

	/* now load the data from the page */
	k = 0;
	for (i = 0; i < so->num_items; i++)
	{
		if (bitmap[i])
		{
			for (j = 0; j < natts; j++)
			{
				memcpy(&values[j], &data[j][i * tupdesc->attrs[j]->attlen],
												tupdesc->attrs[j]->attlen);
				isnull[j] = nulls[j][i/8] & (0x01 << (i % 8));
			}

			so->tuples[k] = index_form_tuple(tupdesc, values, isnull);
			so->tuples[k]->t_tid = tids[i];

			k++;
		}
	}
	so->num_items = k;

	/* and switch to the original context */
	MemoryContextSwitchTo(oldctx);

cleanup:

	_col_relbuf(rel, buf);

	if (so->num_items == 0)
		goto nextpage;

	return true;
}

static bool
_col_buffer_empty(IndexScanDesc scan, ColumnScanOpaque opaque)
{
	/* do we still have some values in the buffer? */
	return (opaque->cur_item >= opaque->num_items);
}


static void
_col_preprocess_keys(IndexScanDesc scan)
{
	ColumnScanOpaque so = (ColumnScanOpaque) scan->opaque;
	int			numberOfKeys = scan->numberOfKeys;
	int16	   *indoption = scan->indexRelation->rd_indoption;
	int			new_numberOfKeys;
	int			numberOfEqualCols;
	ScanKey		inkeys;
	ScanKey		outkeys;
	ScanKey		cur;
	ScanKey		xform[BTMaxStrategyNumber];
	bool		test_result;
	int			i,
				j;
	AttrNumber	attno;

	/* initialize result variables */
	so->qual_ok = true;
	so->numberOfKeys = 0;

	if (numberOfKeys < 1)
		return;					/* done if qual-less scan */

	inkeys = scan->keyData;

	outkeys = so->keyData;
	cur = &inkeys[0];
	/* we check that input keys are correctly ordered */
	if (cur->sk_attno < 1)
		elog(ERROR, "btree index keys must be ordered by attribute");

	/* We can short-circuit most of the work if there's just one key */
	if (numberOfKeys == 1)
	{
		/* Apply indoption to scankey (might change sk_strategy!) */
		if (!_bt_fix_scankey_strategy(cur, indoption))
			so->qual_ok = false;
		memcpy(outkeys, cur, sizeof(ScanKeyData));
		so->numberOfKeys = 1;
		/* We can mark the qual as required if it's for first index col */
		if (cur->sk_attno == 1)
			_bt_mark_scankey_required(outkeys);
		return;
	}

	/*
	 * Otherwise, do the full set of pushups.
	 */
	new_numberOfKeys = 0;
	numberOfEqualCols = 0;

	/*
	 * Initialize for processing of keys for attr 1.
	 *
	 * xform[i] points to the currently best scan key of strategy type i+1; it
	 * is NULL if we haven't yet found such a key for this attr.
	 */
	attno = 1;
	memset(xform, 0, sizeof(xform));

	/*
	 * Loop iterates from 0 to numberOfKeys inclusive; we use the last pass to
	 * handle after-last-key processing.  Actual exit from the loop is at the
	 * "break" statement below.
	 */
	for (i = 0;; cur++, i++)
	{
		if (i < numberOfKeys)
		{
			/* Apply indoption to scankey (might change sk_strategy!) */
			if (!_bt_fix_scankey_strategy(cur, indoption))
			{
				/* NULL can't be matched, so give up */
				so->qual_ok = false;
				return;
			}
		}

		/*
		 * If we are at the end of the keys for a particular attr, finish up
		 * processing and emit the cleaned-up keys.
		 */
		if (i == numberOfKeys || cur->sk_attno != attno)
		{
			int			priorNumberOfEqualCols = numberOfEqualCols;

			/* check input keys are correctly ordered */
			if (i < numberOfKeys && cur->sk_attno < attno)
				elog(ERROR, "btree index keys must be ordered by attribute");

			/*
			 * If = has been specified, all other keys can be eliminated as
			 * redundant.  If we have a case like key = 1 AND key > 2, we can
			 * set qual_ok to false and abandon further processing.
			 *
			 * We also have to deal with the case of "key IS NULL", which is
			 * unsatisfiable in combination with any other index condition. By
			 * the time we get here, that's been classified as an equality
			 * check, and we've rejected any combination of it with a regular
			 * equality condition; but not with other types of conditions.
			 */
			if (xform[BTEqualStrategyNumber - 1])
			{
				ScanKey		eq = xform[BTEqualStrategyNumber - 1];

				for (j = BTMaxStrategyNumber; --j >= 0;)
				{
					ScanKey		chk = xform[j];

					if (!chk || j == (BTEqualStrategyNumber - 1))
						continue;

					if (eq->sk_flags & SK_SEARCHNULL)
					{
						/* IS NULL is contradictory to anything else */
						so->qual_ok = false;
						return;
					}

					if (_bt_compare_scankey_args(scan, chk, eq, chk,
												 &test_result))
					{
						if (!test_result)
						{
							/* keys proven mutually contradictory */
							so->qual_ok = false;
							return;
						}
						/* else discard the redundant non-equality key */
						xform[j] = NULL;
					}
					/* else, cannot determine redundancy, keep both keys */
				}
				/* track number of attrs for which we have "=" keys */
				numberOfEqualCols++;
			}

			/* try to keep only one of <, <= */
			if (xform[BTLessStrategyNumber - 1]
				&& xform[BTLessEqualStrategyNumber - 1])
			{
				ScanKey		lt = xform[BTLessStrategyNumber - 1];
				ScanKey		le = xform[BTLessEqualStrategyNumber - 1];

				if (_bt_compare_scankey_args(scan, le, lt, le,
											 &test_result))
				{
					if (test_result)
						xform[BTLessEqualStrategyNumber - 1] = NULL;
					else
						xform[BTLessStrategyNumber - 1] = NULL;
				}
			}

			/* try to keep only one of >, >= */
			if (xform[BTGreaterStrategyNumber - 1]
				&& xform[BTGreaterEqualStrategyNumber - 1])
			{
				ScanKey		gt = xform[BTGreaterStrategyNumber - 1];
				ScanKey		ge = xform[BTGreaterEqualStrategyNumber - 1];

				if (_bt_compare_scankey_args(scan, ge, gt, ge,
											 &test_result))
				{
					if (test_result)
						xform[BTGreaterEqualStrategyNumber - 1] = NULL;
					else
						xform[BTGreaterStrategyNumber - 1] = NULL;
				}
			}

			/*
			 * Emit the cleaned-up keys into the outkeys[] array, and then
			 * mark them if they are required.  They are required (possibly
			 * only in one direction) if all attrs before this one had "=".
			 */
			for (j = BTMaxStrategyNumber; --j >= 0;)
			{
				if (xform[j])
				{
					ScanKey		outkey = &outkeys[new_numberOfKeys++];

					memcpy(outkey, xform[j], sizeof(ScanKeyData));
					if (priorNumberOfEqualCols == attno - 1)
						_bt_mark_scankey_required(outkey);
				}
			}

			/*
			 * Exit loop here if done.
			 */
			if (i == numberOfKeys)
				break;

			/* Re-initialize for new attno */
			attno = cur->sk_attno;
			memset(xform, 0, sizeof(xform));
		}

		/* check strategy this key's operator corresponds to */
		j = cur->sk_strategy - 1;

		/* if row comparison, push it directly to the output array */
		if (cur->sk_flags & SK_ROW_HEADER)
		{
			ScanKey		outkey = &outkeys[new_numberOfKeys++];

			memcpy(outkey, cur, sizeof(ScanKeyData));
			if (numberOfEqualCols == attno - 1)
				_bt_mark_scankey_required(outkey);

			/*
			 * We don't support RowCompare using equality; such a qual would
			 * mess up the numberOfEqualCols tracking.
			 */
			Assert(j != (BTEqualStrategyNumber - 1));
			continue;
		}

		/* have we seen one of these before? */
		if (xform[j] == NULL)
		{
			/* nope, so remember this scankey */
			xform[j] = cur;
		}
		else
		{
			/* yup, keep only the more restrictive key */
			if (_bt_compare_scankey_args(scan, cur, cur, xform[j],
										 &test_result))
			{
				if (test_result)
					xform[j] = cur;
				else if (j == (BTEqualStrategyNumber - 1))
				{
					/* key == a && key == b, but a != b */
					so->qual_ok = false;
					return;
				}
				/* else old key is more restrictive, keep it */
			}
			else
			{
				/*
				 * We can't determine which key is more restrictive.  Keep the
				 * previous one in xform[j] and push this one directly to the
				 * output array.
				 */
				ScanKey		outkey = &outkeys[new_numberOfKeys++];

				memcpy(outkey, cur, sizeof(ScanKeyData));
				if (numberOfEqualCols == attno - 1)
					_bt_mark_scankey_required(outkey);
			}
		}
	}

	so->numberOfKeys = new_numberOfKeys;
}

static bool
_bt_compare_scankey_args(IndexScanDesc scan, ScanKey op,
						 ScanKey leftarg, ScanKey rightarg,
						 bool *result)
{
	Relation	rel = scan->indexRelation;
	Oid			lefttype,
				righttype,
				optype,
				opcintype,
				cmp_op;
	StrategyNumber strat;

	/*
	 * First, deal with cases where one or both args are NULL.  This should
	 * only happen when the scankeys represent IS NULL/NOT NULL conditions.
	 */
	if ((leftarg->sk_flags | rightarg->sk_flags) & SK_ISNULL)
	{
		bool		leftnull,
					rightnull;

		if (leftarg->sk_flags & SK_ISNULL)
		{
			Assert(leftarg->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL));
			leftnull = true;
		}
		else
			leftnull = false;
		if (rightarg->sk_flags & SK_ISNULL)
		{
			Assert(rightarg->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL));
			rightnull = true;
		}
		else
			rightnull = false;

		/*
		 * We treat NULL as either greater than or less than all other values.
		 * Since true > false, the tests below work correctly for NULLS LAST
		 * logic.  If the index is NULLS FIRST, we need to flip the strategy.
		 */
		strat = op->sk_strategy;
		if (op->sk_flags & SK_BT_NULLS_FIRST)
			strat = BTCommuteStrategyNumber(strat);

		switch (strat)
		{
			case BTLessStrategyNumber:
				*result = (leftnull < rightnull);
				break;
			case BTLessEqualStrategyNumber:
				*result = (leftnull <= rightnull);
				break;
			case BTEqualStrategyNumber:
				*result = (leftnull == rightnull);
				break;
			case BTGreaterEqualStrategyNumber:
				*result = (leftnull >= rightnull);
				break;
			case BTGreaterStrategyNumber:
				*result = (leftnull > rightnull);
				break;
			default:
				elog(ERROR, "unrecognized StrategyNumber: %d", (int) strat);
				*result = false;	/* keep compiler quiet */
				break;
		}
		return true;
	}

	/*
	 * The opfamily we need to worry about is identified by the index column.
	 */
	Assert(leftarg->sk_attno == rightarg->sk_attno);

	opcintype = rel->rd_opcintype[leftarg->sk_attno - 1];

	/*
	 * Determine the actual datatypes of the ScanKey arguments.  We have to
	 * support the convention that sk_subtype == InvalidOid means the opclass
	 * input type; this is a hack to simplify life for ScanKeyInit().
	 */
	lefttype = leftarg->sk_subtype;
	if (lefttype == InvalidOid)
		lefttype = opcintype;
	righttype = rightarg->sk_subtype;
	if (righttype == InvalidOid)
		righttype = opcintype;
	optype = op->sk_subtype;
	if (optype == InvalidOid)
		optype = opcintype;

	/*
	 * If leftarg and rightarg match the types expected for the "op" scankey,
	 * we can use its already-looked-up comparison function.
	 */
	if (lefttype == opcintype && righttype == optype)
	{
		*result = DatumGetBool(FunctionCall2Coll(&op->sk_func,
												 op->sk_collation,
												 leftarg->sk_argument,
												 rightarg->sk_argument));
		return true;
	}

	/*
	 * Otherwise, we need to go to the syscache to find the appropriate
	 * operator.  (This cannot result in infinite recursion, since no
	 * indexscan initiated by syscache lookup will use cross-data-type
	 * operators.)
	 *
	 * If the sk_strategy was flipped by _bt_fix_scankey_strategy, we have to
	 * un-flip it to get the correct opfamily member.
	 */
	strat = op->sk_strategy;
	if (op->sk_flags & SK_BT_DESC)
		strat = BTCommuteStrategyNumber(strat);

	cmp_op = get_opfamily_member(rel->rd_opfamily[leftarg->sk_attno - 1],
								 lefttype,
								 righttype,
								 strat);
	if (OidIsValid(cmp_op))
	{
		RegProcedure cmp_proc = get_opcode(cmp_op);

		if (RegProcedureIsValid(cmp_proc))
		{
			*result = DatumGetBool(OidFunctionCall2Coll(cmp_proc,
														op->sk_collation,
														leftarg->sk_argument,
													 rightarg->sk_argument));
			return true;
		}
	}

	/* Can't make the comparison */
	*result = false;			/* suppress compiler warnings */
	return false;
}

/*
 * Adjust a scankey's strategy and flags setting as needed for indoptions.
 *
 * We copy the appropriate indoption value into the scankey sk_flags
 * (shifting to avoid clobbering system-defined flag bits).  Also, if
 * the DESC option is set, commute (flip) the operator strategy number.
 *
 * A secondary purpose is to check for IS NULL/NOT NULL scankeys and set up
 * the strategy field correctly for them.
 *
 * Lastly, for ordinary scankeys (not IS NULL/NOT NULL), we check for a
 * NULL comparison value.  Since all btree operators are assumed strict,
 * a NULL means that the qual cannot be satisfied.  We return TRUE if the
 * comparison value isn't NULL, or FALSE if the scan should be abandoned.
 *
 * This function is applied to the *input* scankey structure; therefore
 * on a rescan we will be looking at already-processed scankeys.  Hence
 * we have to be careful not to re-commute the strategy if we already did it.
 * It's a bit ugly to modify the caller's copy of the scankey but in practice
 * there shouldn't be any problem, since the index's indoptions are certainly
 * not going to change while the scankey survives.
 */
static bool
_bt_fix_scankey_strategy(ScanKey skey, int16 *indoption)
{
	int			addflags;

	addflags = indoption[skey->sk_attno - 1] << SK_BT_INDOPTION_SHIFT;

	/*
	 * We treat all btree operators as strict (even if they're not so marked
	 * in pg_proc). This means that it is impossible for an operator condition
	 * with a NULL comparison constant to succeed, and we can reject it right
	 * away.
	 *
	 * However, we now also support "x IS NULL" clauses as search conditions,
	 * so in that case keep going. The planner has not filled in any
	 * particular strategy in this case, so set it to BTEqualStrategyNumber
	 * --- we can treat IS NULL as an equality operator for purposes of search
	 * strategy.
	 *
	 * Likewise, "x IS NOT NULL" is supported.  We treat that as either "less
	 * than NULL" in a NULLS LAST index, or "greater than NULL" in a NULLS
	 * FIRST index.
	 *
	 * Note: someday we might have to fill in sk_collation from the index
	 * column's collation.  At the moment this is a non-issue because we'll
	 * never actually call the comparison operator on a NULL.
	 */
	if (skey->sk_flags & SK_ISNULL)
	{
		/* SK_ISNULL shouldn't be set in a row header scankey */
		Assert(!(skey->sk_flags & SK_ROW_HEADER));

		/* Set indoption flags in scankey (might be done already) */
		skey->sk_flags |= addflags;

		/* Set correct strategy for IS NULL or NOT NULL search */
		if (skey->sk_flags & SK_SEARCHNULL)
		{
			skey->sk_strategy = BTEqualStrategyNumber;
			skey->sk_subtype = InvalidOid;
			skey->sk_collation = InvalidOid;
		}
		else if (skey->sk_flags & SK_SEARCHNOTNULL)
		{
			if (skey->sk_flags & SK_BT_NULLS_FIRST)
				skey->sk_strategy = BTGreaterStrategyNumber;
			else
				skey->sk_strategy = BTLessStrategyNumber;
			skey->sk_subtype = InvalidOid;
			skey->sk_collation = InvalidOid;
		}
		else
		{
			/* regular qual, so it cannot be satisfied */
			return false;
		}

		/* Needn't do the rest */
		return true;
	}

	/* Adjust strategy for DESC, if we didn't already */
	if ((addflags & SK_BT_DESC) && !(skey->sk_flags & SK_BT_DESC))
		skey->sk_strategy = BTCommuteStrategyNumber(skey->sk_strategy);
	skey->sk_flags |= addflags;

	/* If it's a row header, fix row member flags and strategies similarly */
	if (skey->sk_flags & SK_ROW_HEADER)
	{
		ScanKey		subkey = (ScanKey) DatumGetPointer(skey->sk_argument);

		for (;;)
		{
			Assert(subkey->sk_flags & SK_ROW_MEMBER);
			addflags = indoption[subkey->sk_attno - 1] << SK_BT_INDOPTION_SHIFT;
			if ((addflags & SK_BT_DESC) && !(subkey->sk_flags & SK_BT_DESC))
				subkey->sk_strategy = BTCommuteStrategyNumber(subkey->sk_strategy);
			subkey->sk_flags |= addflags;
			if (subkey->sk_flags & SK_ROW_END)
				break;
			subkey++;
		}
	}

	return true;
}

/*
 * Mark a scankey as "required to continue the scan".
 *
 * Depending on the operator type, the key may be required for both scan
 * directions or just one.  Also, if the key is a row comparison header,
 * we have to mark the appropriate subsidiary ScanKeys as required.  In
 * such cases, the first subsidiary key is required, but subsequent ones
 * are required only as long as they correspond to successive index columns
 * and match the leading column as to sort direction.
 * Otherwise the row comparison ordering is different from the index ordering
 * and so we can't stop the scan on the basis of those lower-order columns.
 *
 * Note: when we set required-key flag bits in a subsidiary scankey, we are
 * scribbling on a data structure belonging to the index AM's caller, not on
 * our private copy.  This should be OK because the marking will not change
 * from scan to scan within a query, and so we'd just re-mark the same way
 * anyway on a rescan.  Something to keep an eye on though.
 */
static void
_bt_mark_scankey_required(ScanKey skey)
{
	int			addflags;

	switch (skey->sk_strategy)
	{
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			addflags = SK_BT_REQFWD;
			break;
		case BTEqualStrategyNumber:
			addflags = SK_BT_REQFWD | SK_BT_REQBKWD;
			break;
		case BTGreaterEqualStrategyNumber:
		case BTGreaterStrategyNumber:
			addflags = SK_BT_REQBKWD;
			break;
		default:
			elog(ERROR, "unrecognized StrategyNumber: %d",
				 (int) skey->sk_strategy);
			addflags = 0;		/* keep compiler quiet */
			break;
	}

	skey->sk_flags |= addflags;

	if (skey->sk_flags & SK_ROW_HEADER)
	{
		ScanKey		subkey = (ScanKey) DatumGetPointer(skey->sk_argument);
		AttrNumber	attno = skey->sk_attno;

		/* First subkey should be same as the header says */
		Assert(subkey->sk_attno == attno);

		for (;;)
		{
			Assert(subkey->sk_flags & SK_ROW_MEMBER);
			if (subkey->sk_attno != attno)
				break;			/* non-adjacent key, so not required */
			if (subkey->sk_strategy != skey->sk_strategy)
				break;			/* wrong direction, so not required */
			subkey->sk_flags |= addflags;
			if (subkey->sk_flags & SK_ROW_END)
				break;
			subkey++;
			attno++;
		}
	}
}

static int
_col_scankey_update_bitmap(Relation rel,
						   int keysz,
						   ScanKey key,
						   AttrNumber attno,
						   int nrows,
						   Datum *values,
						   bool *isnull,
						   bool *bitmap,
						   int   nmatching)
{
	int	i, j;

	/*
	 * The scan key is set up with the attribute number associated with each
	 * term in the key.  It is important that, if the index is multi-key, the
	 * scan contain the first k key attributes, and that they be in order.  If
	 * you think about how multi-key ordering works, you'll understand why
	 * this is.
	 *
	 * We don't test for violation of this condition here, however.  The
	 * initial setup for the index scan had better have gotten it right (see
	 * _bt_first).
	 */

	for (i = 1; i <= keysz; i++)
	{
		/* skip scankeys to be applied to other attributes */
		if (key->sk_attno != attno)
		{
			key++;
			continue;
		}

		for (j = 0; j < nrows; j++)
		{
			/* get values from the array */
			Datum	datum = values[j];
			bool	isNull = isnull[j];
			Datum	test;

			/* end if there are no more matching rows */
			if (nmatching == 0)
				return nmatching;

			/* skip tuples that were already deemed not matching */
			if (! bitmap[j])
				continue;

			if (key->sk_flags & SK_ISNULL)
			{
				/* Handle IS NULL/NOT NULL tests */
				if (key->sk_flags & SK_SEARCHNULL)
				{
					if (isNull)
						continue;	/* tuple satisfies this qual */
				}
				else
				{
					Assert(key->sk_flags & SK_SEARCHNOTNULL);
					if (!isNull)
						continue;	/* tuple satisfies this qual */
				}

				/*
				 * FIXME btree does some smart decisions about stopping
				 *       the search depending on forward/backward scans
				 *       here, see _bt_checkkeys() in nbtutils.c
				 */

				/*
				 * In any case, this indextuple doesn't match the qual.
				 */
				bitmap[j] = false;
				nmatching--;
			}
			else if (isNull)	/* but not (sk_flags & SK_ISNULL)) */
			{
				/* FIXME again forward/backward smartness in nbtutils.c */

				/*
				 * In any case, this indextuple doesn't match the qual.
				 */
				bitmap[j] = false;
				nmatching--;
			}
			else
			{
				test = FunctionCall2Coll(&key->sk_func, key->sk_collation,
										 datum, key->sk_argument);

				if (!DatumGetBool(test))
				{
					/* FIXME again forward/backward smartness in nbtutils.c */

					/*
					 * In any case, this indextuple doesn't match the qual.
					 */
					bitmap[j] = false;
					nmatching--;
				}
			}
		}

		key++;
	}

	return nmatching;
}
