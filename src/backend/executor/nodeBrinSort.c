/*-------------------------------------------------------------------------
 *
 * nodeBrinSort.c
 *	  Routines to support indexed scans of relations
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeBrinSort.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecBrinSort			scans a relation using an index
 *		IndexNext				retrieve next tuple using index
 *		ExecInitBrinSort		creates and initializes state info.
 *		ExecReScanBrinSort		rescans the indexed relation.
 *		ExecEndBrinSort			releases all storage.
 *		ExecIndexMarkPos		marks scan position.
 *		ExecIndexRestrPos		restores scan position.
 *		ExecBrinSortEstimate	estimates DSM space needed for parallel index scan
 *		ExecBrinSortInitializeDSM initialize DSM for parallel BrinSort
 *		ExecBrinSortReInitializeDSM reinitialize DSM for fresh scan
 *		ExecBrinSortInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/brin_revmap.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "executor/execdebug.h"
#include "executor/nodeBrinSort.h"
#include "lib/pairingheap.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * When an ordering operator is used, tuples fetched from the index that
 * need to be reordered are queued in a pairing heap, as ReorderTuples.
 */
typedef struct
{
	pairingheap_node ph_node;
	HeapTuple	htup;
	Datum	   *orderbyvals;
	bool	   *orderbynulls;
} ReorderTuple;

static TupleTableSlot *IndexNext(BrinSortState *node);
static bool IndexRecheck(BrinSortState *node, TupleTableSlot *slot);
static void ExecInitBrinSortRanges(BrinSort *node, BrinSortState *planstate);

static void
brinsort_start_tidscan(BrinSortState *node, int range_index, bool update_watermark)
{
	BrinSort   *plan = (BrinSort *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;

	Assert(range_index < node->bs_nranges);

	/* get the first range, read all tuples using a tid range scan */
	if (node->ss.ss_currentScanDesc == NULL)
	{
		TableScanDesc		tscandesc;
		ItemPointerData		mintid,
							maxtid;

		BrinSortRange *range = &node->bs_ranges[range_index];

		/*
		 * Remember maximum value for the current range (but not when
		 * processing overlapping ranges).
		 */
		if (update_watermark)
			node->bs_watermark = range->max_value;

		range->processed = true;

		ItemPointerSetBlockNumber(&mintid, range->blkno_start);
		ItemPointerSetOffsetNumber(&mintid, 0);

		ItemPointerSetBlockNumber(&maxtid, range->blkno_end);
		ItemPointerSetOffsetNumber(&maxtid, MaxHeapTuplesPerPage);

		elog(DEBUG1, "loading range %d, %d", range->blkno_start, range->blkno_end);

		tscandesc = table_beginscan_tidrange(node->ss.ss_currentRelation,
											 estate->es_snapshot,
											 &mintid, &maxtid);
		node->ss.ss_currentScanDesc = tscandesc;
	}

	if (node->bs_tuplesortstate == NULL)
	{
		TupleDesc	tupDesc = RelationGetDescr(node->ss.ss_currentRelation);

		node->bs_tuplesortstate = tuplesort_begin_heap(tupDesc,
													plan->numCols,
													plan->sortColIdx,
													plan->sortOperators,
													plan->collations,
													plan->nullsFirst,
													work_mem,
													NULL,
													TUPLESORT_NONE);

		node->bs_tuplestore = tuplestore_begin_heap(false, false, work_mem);
		/*
		tuplesort_begin_heap(tupDesc,
													plan->numCols,
													plan->sortColIdx,
													plan->sortOperators,
													plan->collations,
													plan->nullsFirst,
													work_mem,
													NULL,
													TUPLESORT_NONE);
		*/
	}

	node->bs_next_range++;
}

static void
brinsort_end_tidscan(BrinSortState *node)
{
	/* get the first range, read all tuples using a tid range scan */
	if (node->ss.ss_currentScanDesc != NULL)
	{
		table_endscan(node->ss.ss_currentScanDesc);
		node->ss.ss_currentScanDesc = NULL;
	}
}

static bool
brinsort_load_tuples(BrinSortState *node, bool check_watermark)
{
	bool			res = false;
	BrinSort   *plan = (BrinSort *) node->ss.ps.plan;
	TableScanDesc	scan = node->ss.ss_currentScanDesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;
	SortSupportData	ssup;

	estate = node->ss.ps.state;
	direction = estate->es_direction;

	slot = node->ss.ss_ScanTupleSlot;

	/*
	 * Sort ranges by maximum value.
	 *
	 * XXX Needs to consider the other parameters (ASC/DESC, NULLS FIRST/LAST, etc.),
	 */
	memset(&ssup, 0, sizeof(SortSupportData));
	PrepareSortSupportFromOrderingOp(plan->sortOperators[0], &ssup);

	while (table_scan_getnextslot_tidrange(scan, direction, slot))
	{
		ExprContext *econtext;
		ExprState  *qual;

		/*
		 * Fetch data from node
		 */
		qual = node->bs_qual;
		econtext = node->ss.ps.ps_ExprContext;

		/*
		 * place the current tuple into the expr context
		 */
		econtext->ecxt_scantuple = slot;

		/*
		 * check that the current tuple satisfies the qual-clause
		 *
		 * check for non-null qual here to avoid a function call to ExecQual()
		 * when the qual is null ... saves only a few cycles, but they add up
		 * ...
		 *
		 * XXX Done here, because in ExecScan we'll get different slot type
		 * (minimal tuple vs. buffered tuple). Scan expects slot while reading
		 * from the table (like here), but we're stashing it into a tuplesort.
		 *
		 * XXX Maybe we could eliminate many tuples by leveraging the BRIN
		 * range, by executing the consistent function. But we don't have
		 * the qual in appropriate format at the moment, so we'd preprocess
		 * the keys similarly to bringetbitmap(). In which case we should
		 * probably evaluate the stuff while building the ranges? Although,
		 * if the "consistent" function is expensive, it might be cheaper
		 * to do that incrementally, as we need the ranges. Would be a win
		 * for LIMIT queries, for example.
		 *
		 * XXX However, maybe we could also leverage other bitmap indexes,
		 * particularly for BRIN indexes because that makes it simpler to
		 * eliminage the ranges incrementally - we know which ranges to
		 * load from the index, while for other indexes (e.g. btree) we
		 * have to read the whole index and build a bitmap in order to have
		 * a bitmap for any range. Although, if the condition is very
		 * selective, we may need to read only a small fraction of the
		 * index, so maybe that's OK.
		 */
		if (qual == NULL || ExecQual(qual, econtext))
		{
			Datum	value;
			int		cmp = 0;
			bool	isnull;

			elog(DEBUG1, "adding tuple");

			value = slot_getattr(slot, plan->sortColIdx[0], &isnull);

			/*
			 * Not handling NULLS for now, we need to stash them into a
			 * separate tuplestore (so that we can output them first or
			 * last), and then skip them in the regular processing?
			 */
			Assert(!isnull);

			if (check_watermark)
				cmp = ApplySortComparator(value, false,
										  node->bs_watermark, false,
										  &ssup);

			elog(DEBUG1, "%ld %ld %d", node->bs_watermark, value, cmp);

			if (cmp <= 0)
				tuplesort_puttupleslot(node->bs_tuplesortstate, slot);
			else
				tuplestore_puttupleslot(node->bs_tuplestore, slot);

			res = true;
		}
		else
			elog(DEBUG1, "filtered tuple");

		ExecClearTuple(slot);
	}

	ExecClearTuple(slot);

	return res;
}

static void
brinsort_load_spill_tuples(BrinSortState *node, bool check_watermark)
{
	BrinSort   *plan = (BrinSort *) node->ss.ps.plan;
	Tuplestorestate *tupstore;
	SortSupportData	ssup;
	TupleTableSlot *slot;

	memset(&ssup, 0, sizeof(SortSupportData));
	PrepareSortSupportFromOrderingOp(plan->sortOperators[0], &ssup);

	slot = MakeSingleTupleTableSlot(RelationGetDescr(node->ss.ss_currentRelation),
									&TTSOpsMinimalTuple);

	tupstore = tuplestore_begin_heap(false, false, work_mem);

	tuplestore_rescan(node->bs_tuplestore);

	while (tuplestore_gettupleslot(node->bs_tuplestore, true, true, slot))
	{
		int		cmp = 0;

		bool	isnull;
		Datum	value;

		value = slot_getattr(slot, plan->sortColIdx[0], &isnull);

		/*
		 * Not handling NULLS for now, we need to stash them into a
		 * separate tuplestore (so that we can output them first or
		 * last), and then skip them in the regular processing?
		 */
		Assert(!isnull);

		if (check_watermark)
			cmp = ApplySortComparator(value, false,
									  node->bs_watermark, false,
									  &ssup);

		elog(DEBUG1, "watermark %ld value %ld %d", node->bs_watermark, value, cmp);

		elog(DEBUG1, "%ld %ld %d", node->bs_watermark, value, cmp);

		if (cmp <= 0)
			tuplesort_puttupleslot(node->bs_tuplesortstate, slot);
		else
			tuplestore_puttupleslot(tupstore, slot);
	}

	tuplestore_end(node->bs_tuplestore);

	node->bs_tuplestore = tupstore;

	ExecDropSingleTupleTableSlot(slot);
}

/* ----------------------------------------------------------------
 *		IndexNext
 *
 *		Retrieve a tuple from the BrinSort node's currentRelation
 *		using the index specified in the BrinSortState information.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
IndexNext(BrinSortState *node)
{
	EState	   *estate;
	BrinSort   *plan = (BrinSort *) node->ss.ps.plan;
	ScanDirection direction;
	IndexScanDesc scandesc;
	TupleTableSlot *slot;

	/*
	 * extract necessary information from index scan node
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	/* flip direction if this is an overall backward scan */
	if (ScanDirectionIsBackward(((BrinSort *) node->ss.ps.plan)->indexorderdir))
	{
		if (ScanDirectionIsForward(direction))
			direction = BackwardScanDirection;
		else if (ScanDirectionIsBackward(direction))
			direction = ForwardScanDirection;
	}
	scandesc = node->iss_ScanDesc;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the index scan is not parallel, or if we're
		 * serially executing an index scan that was planned to be parallel.
		 */
		scandesc = index_beginscan(node->ss.ss_currentRelation,
								   node->iss_RelationDesc,
								   estate->es_snapshot,
								   node->iss_NumScanKeys,
								   node->iss_NumOrderByKeys);

		node->iss_ScanDesc = scandesc;

		/*
		 * If no run-time keys to calculate or they are ready, go ahead and
		 * pass the scankeys to the index AM.
		 */
		if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady)
			index_rescan(scandesc,
						 node->iss_ScanKeys, node->iss_NumScanKeys,
						 node->iss_OrderByKeys, node->iss_NumOrderByKeys);

		/*
		 * Load info about BRIN ranges, sort them to match the desired ordering.
		 *
		 * Maybe this should happen later, i.e. at the beginning of execution,
		 * otherwise we might do expensive stuff in EXPLAIN.
		 */
		ExecInitBrinSortRanges((BrinSort *) node->ss.ps.plan, node);
		node->bs_next_range = 0;
		node->bs_phase = LOAD_RANGE;
	}

	/*
	 * ok, now that we have what we need, fetch the next tuple.
	 */
	while (node->bs_phase != FINISHED)
	{
		CHECK_FOR_INTERRUPTS();

		elog(DEBUG1, "phase = %d", node->bs_phase);

		switch (node->bs_phase)
		{
			case LOAD_RANGE:

				elog(DEBUG1, "phase = LOAD_RANGE");

				/* skip already processed ranges */
				while ((node->bs_next_range < node->bs_nranges) &&
					   (node->bs_ranges[node->bs_next_range].processed))
					node->bs_next_range++;

				if ((node->bs_next_range == node->bs_nranges) &&
					(node->bs_tuplestore != NULL))
				{
					brinsort_load_spill_tuples(node, false);

					node->bs_tuplestore = NULL;

					elog(DEBUG1, "performing sort");
					tuplesort_performsort(node->bs_tuplesortstate);

					node->bs_phase = PROCESS_RANGE;
					break;
				}

				if (node->bs_next_range == node->bs_nranges)
				{
					elog(DEBUG1, "phase => FINISHED");
					node->bs_phase = FINISHED;
					break;
				}
elog(DEBUG1, "loading range %d", node->bs_next_range);
				brinsort_start_tidscan(node, node->bs_next_range, true);

				/* no need to check the watermark */
				brinsort_load_tuples(node, false);

				brinsort_end_tidscan(node);

				/* load matching tuples from the current spill tuplestore */
				brinsort_load_spill_tuples(node, true);

				/* load intersecting ranges */
				for (int i = 0; i < node->bs_nranges; i++)
				{
					int	cmp;
					BrinSortRange  *range = &node->bs_ranges[i];
					SortSupportData	ssup;

					/* skip already processed ranges */
					if (range->processed)
						continue;

					memset(&ssup, 0, sizeof(SortSupportData));
					PrepareSortSupportFromOrderingOp(plan->sortOperators[0], &ssup);

					cmp = ApplySortComparator(range->min_value, false,
											  node->bs_watermark, false,
											  &ssup);

					/* no possible overlap, so skip this range */
					if (cmp > 0)
						continue;

					elog(DEBUG1, "loading intersecting range %d [%ld,%ld] %ld", i,
								  range->min_value, range->max_value,
								  node->bs_watermark);

					brinsort_start_tidscan(node, i, false);

					/* now check the watermark */
					brinsort_load_tuples(node, true);

					brinsort_end_tidscan(node);
				}

				elog(DEBUG1, "performing sort");
				tuplesort_performsort(node->bs_tuplesortstate);

				node->bs_phase = PROCESS_RANGE;
				break;

			case PROCESS_RANGE:

				slot = node->ss.ps.ps_ResultTupleSlot;

				if (node->bs_tuplesortstate != NULL)
				{
					elog(DEBUG1, "getting tuple");
					if (tuplesort_gettupleslot(node->bs_tuplesortstate,
										ScanDirectionIsForward(direction),
										false, slot, NULL))
					{
						return slot;
					}

					elog(DEBUG1, "resetting");
					tuplesort_reset(node->bs_tuplesortstate);
					node->bs_phase = LOAD_RANGE;
				}

				break;

			case FINISHED:
				elog(ERROR, "finished should not happen here");
				break;
		}
	}

	/*
	 * if we get here it means the index scan failed so we are at the end of
	 * the scan..
	 */
	node->iss_ReachedEnd = true;
	return ExecClearTuple(slot);
}

/*
 * IndexRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
IndexRecheck(BrinSortState *node, TupleTableSlot *slot)
{
	ExprContext *econtext;

	/*
	 * extract necessary information from index scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;

	/* Does the tuple meet the indexqual condition? */
	econtext->ecxt_scantuple = slot;
	return ExecQualAndReset(node->indexqualorig, econtext);
}


/* ----------------------------------------------------------------
 *		ExecBrinSort(node)
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecBrinSort(PlanState *pstate)
{
	BrinSortState *node = castNode(BrinSortState, pstate);

	/*
	 * If we have runtime keys and they've not already been set up, do it now.
	 */
	if (node->iss_NumRuntimeKeys != 0 && !node->iss_RuntimeKeysReady)
		ExecReScan((PlanState *) node);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) IndexNext,
					(ExecScanRecheckMtd) IndexRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanBrinSort(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 *
 *		Updating the scan key was formerly done separately in
 *		ExecUpdateBrinSortKeys. Integrating it into ReScan makes
 *		rescans of indices and relations/general streams more uniform.
 * ----------------------------------------------------------------
 */
void
ExecReScanBrinSort(BrinSortState *node)
{
	/*
	 * If we are doing runtime key calculations (ie, any of the index key
	 * values weren't simple Consts), compute the new key values.  But first,
	 * reset the context so we don't leak memory as each outer tuple is
	 * scanned.  Note this assumes that we will recalculate *all* runtime keys
	 * on each call.
	 */
	if (node->iss_NumRuntimeKeys != 0)
	{
		ExprContext *econtext = node->iss_RuntimeContext;

		ResetExprContext(econtext);
		ExecIndexEvalRuntimeKeys(econtext,
								 node->iss_RuntimeKeys,
								 node->iss_NumRuntimeKeys);
	}
	node->iss_RuntimeKeysReady = true;

	/* reset index scan */
	if (node->iss_ScanDesc)
		index_rescan(node->iss_ScanDesc,
					 node->iss_ScanKeys, node->iss_NumScanKeys,
					 node->iss_OrderByKeys, node->iss_NumOrderByKeys);
	node->iss_ReachedEnd = false;

	ExecScanReScan(&node->ss);
}


/* ----------------------------------------------------------------
 *		ExecEndBrinSort
 * ----------------------------------------------------------------
 */
void
ExecEndBrinSort(BrinSortState *node)
{
	Relation	indexRelationDesc;
	IndexScanDesc IndexScanDesc;

	/*
	 * extract information from the node
	 */
	indexRelationDesc = node->iss_RelationDesc;
	IndexScanDesc = node->iss_ScanDesc;

	/*
	 * Free the exprcontext(s) ... now dead code, see ExecFreeExprContext
	 */
#ifdef NOT_USED
	ExecFreeExprContext(&node->ss.ps);
	if (node->iss_RuntimeContext)
		FreeExprContext(node->iss_RuntimeContext, true);
#endif

	/*
	 * clear out tuple table slots
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close the index relation (no-op if we didn't open it)
	 */
	if (IndexScanDesc)
		index_endscan(IndexScanDesc);
	if (indexRelationDesc)
		index_close(indexRelationDesc, NoLock);

	if (node->ss.ss_currentScanDesc != NULL)
		table_endscan(node->ss.ss_currentScanDesc);

	if (node->bs_tuplestore != NULL)
		tuplestore_end(node->bs_tuplestore);
	node->bs_tuplestore = NULL;

	if (node->bs_tuplesortstate != NULL)
		tuplesort_end(node->bs_tuplesortstate);
	node->bs_tuplesortstate = NULL;
}

/* ----------------------------------------------------------------
 *		ExecIndexMarkPos
 *
 * Note: we assume that no caller attempts to set a mark before having read
 * at least one tuple.  Otherwise, iss_ScanDesc might still be NULL.
 * ----------------------------------------------------------------
 */
void
ExecBrinSortMarkPos(BrinSortState *node)
{
	EState	   *estate = node->ss.ps.state;
	EPQState   *epqstate = estate->es_epq_active;

	if (epqstate != NULL)
	{
		/*
		 * We are inside an EvalPlanQual recheck.  If a test tuple exists for
		 * this relation, then we shouldn't access the index at all.  We would
		 * instead need to save, and later restore, the state of the
		 * relsubs_done flag, so that re-fetching the test tuple is possible.
		 * However, given the assumption that no caller sets a mark at the
		 * start of the scan, we can only get here with relsubs_done[i]
		 * already set, and so no state need be saved.
		 */
		Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;

		Assert(scanrelid > 0);
		if (epqstate->relsubs_slot[scanrelid - 1] != NULL ||
			epqstate->relsubs_rowmark[scanrelid - 1] != NULL)
		{
			/* Verify the claim above */
			if (!epqstate->relsubs_done[scanrelid - 1])
				elog(ERROR, "unexpected ExecIndexMarkPos call in EPQ recheck");
			return;
		}
	}

	index_markpos(node->iss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecIndexRestrPos
 * ----------------------------------------------------------------
 */
void
ExecBrinSortRestrPos(BrinSortState *node)
{
	EState	   *estate = node->ss.ps.state;
	EPQState   *epqstate = estate->es_epq_active;

	if (estate->es_epq_active != NULL)
	{
		/* See comments in ExecIndexMarkPos */
		Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;

		Assert(scanrelid > 0);
		if (epqstate->relsubs_slot[scanrelid - 1] != NULL ||
			epqstate->relsubs_rowmark[scanrelid - 1] != NULL)
		{
			/* Verify the claim above */
			if (!epqstate->relsubs_done[scanrelid - 1])
				elog(ERROR, "unexpected ExecIndexRestrPos call in EPQ recheck");
			return;
		}
	}

	index_restrpos(node->iss_ScanDesc);
}

/* XXX copy from brin.c */
typedef struct BrinOpaque
{
	BlockNumber bo_pagesPerRange;
	BrinRevmap *bo_rmAccess;
	BrinDesc   *bo_bdesc;
} BrinOpaque;

typedef struct brin_cmp_context
{
	
} brin_cmp_context;

static int
brin_sort_range_cmp(const void *a, const void *b, void *arg)
{
	int				r;
	BrinSortRange  *ra = (BrinSortRange *) a;
	BrinSortRange  *rb = (BrinSortRange *) b;
	SortSupport		ssup = (SortSupport) arg;

	/* XXX consider NULL FIRST/LAST and ASC/DESC */
	/* XXX also handle un-summarized ranges */

	r = ApplySortComparator(ra->max_value, false, rb->max_value, false, ssup);
	if (r != 0)
		return r;

	return ApplySortComparator(ra->min_value, false, rb->min_value, false, ssup);
}

/* somewhat crippled verson of bringetbitmap */
static void
ExecInitBrinSortRanges(BrinSort *node, BrinSortState *planstate)
{
	Relation	indexRel = planstate->iss_RelationDesc;
	Relation	heapRel;
	BrinOpaque *opaque;
	BrinDesc   *bdesc;
	BlockNumber nblocks;
	BlockNumber	nranges;
	BlockNumber	heapBlk;
	Oid			heapOid;
	BrinMemTuple *dtup;
	BrinTuple  *btup = NULL;
	Size		btupsz = 0;
	Buffer		buf = InvalidBuffer;
	SortSupportData	ssup;

	IndexScanDesc	scan = planstate->iss_ScanDesc;

	/* index attribute we're interested in */
	int			attno;

	opaque = (BrinOpaque *) scan->opaque;
	bdesc = opaque->bo_bdesc;

	/*
	 * We need to know the size of the table so that we know how long to
	 * iterate on the revmap.
	 */
	heapOid = IndexGetRelation(RelationGetRelid(indexRel), false);
	heapRel = table_open(heapOid, AccessShareLock);
	nblocks = RelationGetNumberOfBlocks(heapRel);
	table_close(heapRel, AccessShareLock);

	/*
	 * How many ranges can there be?
	 *
	 * XXX Make sure not to overflow, so don't do (m + n - 1), and just
	 * add +1.
	 */
	nranges = (nblocks / opaque->bo_pagesPerRange) + 1;

	planstate->bs_nranges = 0;
	planstate->bs_ranges = (BrinSortRange *) palloc0(nranges * sizeof(BrinSortRange));

	/*
	 * XXX We might apply keys on the remaining index attributes, similarly
	 * to what bringetbitmap does.
	 */

	/* allocate an initial in-memory tuple, out of the per-range memcxt */
	dtup = brin_new_memtuple(bdesc);

	/*
	 * Map the sortColIdx (which is for the whole table) to index attnum.
	 *
	 * XXX Maybe this is wrong and we should arrange the tlists differently?
	 */
	Assert(node->numCols == 1);

	attno = 0;
	for (int i = 0; i < indexRel->rd_index->indnatts; i++)
	{
		if (indexRel->rd_index->indkey.values[i] == node->sortColIdx[0])
		{
			attno = (i + 1);
			break;
		}
	}

	/* make sure attnum is valid */
	Assert(attno != 0);
	Assert(attno <= bdesc->bd_tupdesc->natts);

	/*
	 * XXX We don't call consistent function (or any other function), so
	 * unlike bringetbitmap we don't set a separate memory context.
	 */

	/*
	 * Now scan the revmap.  We start by querying for heap page 0,
	 * incrementing by the number of pages per range; this gives us a full
	 * view of the table.
	 */
	for (heapBlk = 0; heapBlk < nblocks; heapBlk += opaque->bo_pagesPerRange)
	{
		bool		gottuple = false;
		BrinTuple  *tup;
		OffsetNumber off;
		Size		size;
		BrinSortRange *range = &planstate->bs_ranges[planstate->bs_nranges++];

		CHECK_FOR_INTERRUPTS();

		tup = brinGetTupleForHeapBlock(opaque->bo_rmAccess, heapBlk, &buf,
									   &off, &size, BUFFER_LOCK_SHARE,
									   scan->xs_snapshot);
		if (tup)
		{
			gottuple = true;
			btup = brin_copy_tuple(tup, size, btup, &btupsz);
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		}

		range->blkno_start = heapBlk;
		range->blkno_end = heapBlk + (opaque->bo_pagesPerRange - 1);

		/*
		 * Ranges with no indexed tuple may contain anything.
		 */
		if (!gottuple)
		{
			range->not_summarized = true;
		}
		else
		{
			dtup = brin_deform_tuple(bdesc, btup, dtup);
			if (dtup->bt_placeholder)
			{
				/*
				 * Placeholder tuples are treated as if not populated.
				 *
				 * XXX Is this correct?
				 */
				range->not_summarized = true;
			}
			else
			{
				BrinValues *bval;

				bval = &dtup->bt_columns[attno - 1];

				range->has_nulls = bval->bv_allnulls;
				range->all_nulls = bval->bv_hasnulls;

				if (!bval->bv_allnulls)
				{
					range->min_value = bval->bv_values[0];
					range->max_value = bval->bv_values[1];
				}
			}
		}
	}

	if (buf != InvalidBuffer)
		ReleaseBuffer(buf);

	/*
	 * Sort ranges by maximum value.
	 *
	 * XXX Needs to consider the other parameters (ASC/DESC, NULLS FIRST/LAST, etc.),
	 */
	memset(&ssup, 0, sizeof(SortSupportData));
	PrepareSortSupportFromOrderingOp(node->sortOperators[0], &ssup);

	/*
	 * XXX This needs a bit smore complicated sort. Yes, we need to sort by
	 * max_value in the first step, so that we can add ranges incrementally,
	 * as they add "minimum" number of rows.
	 *
	 * But then in the second step we need to add all intersecting ranges X
	 * until X.min_value > A.max_value (where A is the range added in first
	 * step). And for that we probably need a separate sort by min_value,
	 * perhaps of just a pointer array, pointing back to bs_ranges.
	 *
	 * XXX For DESC sort this would work the opposite way, i.e. first step
	 * sort by min_value, then max_value.
	 */
	qsort_arg(planstate->bs_ranges, planstate->bs_nranges, sizeof(BrinSortRange),
			  brin_sort_range_cmp, &ssup);

	elog(DEBUG1, "ranges = %d", planstate->bs_nranges);

	/* dump ranges for debugging */
	for (int i = 0; i < planstate->bs_nranges; i++)
	{
		elog(DEBUG1, "%d => (%d,%d) [%ld,%ld]", i,
			 planstate->bs_ranges[i].blkno_start,
			 planstate->bs_ranges[i].blkno_end,
			 planstate->bs_ranges[i].min_value,
			 planstate->bs_ranges[i].max_value);
	}
}

/* ----------------------------------------------------------------
 *		ExecInitBrinSort
 *
 *		Initializes the index scan's state information, creates
 *		scan keys, and opens the base and index relations.
 *
 *		Note: index scans have 2 sets of state information because
 *			  we have to keep track of the base relation and the
 *			  index relation.
 * ----------------------------------------------------------------
 */
BrinSortState *
ExecInitBrinSort(BrinSort *node, EState *estate, int eflags)
{
	BrinSortState *indexstate;
	Relation	currentRelation;
	LOCKMODE	lockmode;

	/*
	 * create state structure
	 */
	indexstate = makeNode(BrinSortState);
	indexstate->ss.ps.plan = (Plan *) node;
	indexstate->ss.ps.state = estate;
	indexstate->ss.ps.ExecProcNode = ExecBrinSort;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &indexstate->ss.ps);

	/*
	 * open the scan relation
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);

	indexstate->ss.ss_currentRelation = currentRelation;
	indexstate->ss.ss_currentScanDesc = NULL;	/* no heap scan here */

	/*
	 * get the scan type from the relation descriptor.
	 */
	ExecInitScanTupleSlot(estate, &indexstate->ss,
						  RelationGetDescr(currentRelation),
						  table_slot_callbacks(currentRelation));

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&indexstate->ss.ps);
	ExecAssignScanProjectionInfo(&indexstate->ss);

	/*
	 * initialize child expressions
	 *
	 * Note: we don't initialize all of the indexqual expression, only the
	 * sub-parts corresponding to runtime keys (see below).  Likewise for
	 * indexorderby, if any.  But the indexqualorig expression is always
	 * initialized even though it will only be used in some uncommon cases ---
	 * would be nice to improve that.  (Problem is that any SubPlans present
	 * in the expression must be found now...)
	 */
	indexstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) indexstate);
	indexstate->indexqualorig =
		ExecInitQual(node->indexqualorig, (PlanState *) indexstate);

	/*
	 * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
	 * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
	 * references to nonexistent indexes.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return indexstate;

	/* Open the index relation. */
	lockmode = exec_rt_fetch(node->scan.scanrelid, estate)->rellockmode;
	indexstate->iss_RelationDesc = index_open(node->indexid, lockmode);

	/*
	 * Initialize index-specific scan state
	 */
	indexstate->iss_RuntimeKeysReady = false;
	indexstate->iss_RuntimeKeys = NULL;
	indexstate->iss_NumRuntimeKeys = 0;

	/*
	 * build the index scan keys from the index qualification
	 */
	ExecIndexBuildScanKeys((PlanState *) indexstate,
						   indexstate->iss_RelationDesc,
						   node->indexqual,
						   false,
						   &indexstate->iss_ScanKeys,
						   &indexstate->iss_NumScanKeys,
						   &indexstate->iss_RuntimeKeys,
						   &indexstate->iss_NumRuntimeKeys,
						   NULL,	/* no ArrayKeys */
						   NULL);

	/*
	 * any ORDER BY exprs have to be turned into scankeys in the same way
	 */
	ExecIndexBuildScanKeys((PlanState *) indexstate,
						   indexstate->iss_RelationDesc,
						   NULL,
						   true,
						   &indexstate->iss_OrderByKeys,
						   &indexstate->iss_NumOrderByKeys,
						   &indexstate->iss_RuntimeKeys,
						   &indexstate->iss_NumRuntimeKeys,
						   NULL,	/* no ArrayKeys */
						   NULL);

	/*
	 * If we have runtime keys, we need an ExprContext to evaluate them. The
	 * node's standard context won't do because we want to reset that context
	 * for every tuple.  So, build another context just like the other one...
	 * -tgl 7/11/00
	 */
	if (indexstate->iss_NumRuntimeKeys != 0)
	{
		ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;

		ExecAssignExprContext(estate, &indexstate->ss.ps);
		indexstate->iss_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
		indexstate->ss.ps.ps_ExprContext = stdecontext;
	}
	else
	{
		indexstate->iss_RuntimeContext = NULL;
	}

	indexstate->bs_tuplesortstate = NULL;
	indexstate->bs_qual = indexstate->ss.ps.qual;
	indexstate->ss.ps.qual = NULL;
	ExecInitResultTupleSlotTL(&indexstate->ss.ps, &TTSOpsMinimalTuple);

	/*
	 * all done.
	 */
	return indexstate;
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecBrinSortEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecBrinSortEstimate(BrinSortState *node,
					  ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->iss_PscanLen = index_parallelscan_estimate(node->iss_RelationDesc,
													 estate->es_snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, node->iss_PscanLen);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecBrinSortInitializeDSM
 *
 *		Set up a parallel index scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecBrinSortInitializeDSM(BrinSortState *node,
						   ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	ParallelIndexScanDesc piscan;

	piscan = shm_toc_allocate(pcxt->toc, node->iss_PscanLen);
	index_parallelscan_initialize(node->ss.ss_currentRelation,
								  node->iss_RelationDesc,
								  estate->es_snapshot,
								  piscan);
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, piscan);
	node->iss_ScanDesc =
		index_beginscan_parallel(node->ss.ss_currentRelation,
								 node->iss_RelationDesc,
								 node->iss_NumScanKeys,
								 node->iss_NumOrderByKeys,
								 piscan);

	/*
	 * If no run-time keys to calculate or they are ready, go ahead and pass
	 * the scankeys to the index AM.
	 */
	if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady)
		index_rescan(node->iss_ScanDesc,
					 node->iss_ScanKeys, node->iss_NumScanKeys,
					 node->iss_OrderByKeys, node->iss_NumOrderByKeys);
}

/* ----------------------------------------------------------------
 *		ExecBrinSortReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecBrinSortReInitializeDSM(BrinSortState *node,
							 ParallelContext *pcxt)
{
	index_parallelrescan(node->iss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecBrinSortInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecBrinSortInitializeWorker(BrinSortState *node,
							  ParallelWorkerContext *pwcxt)
{
	ParallelIndexScanDesc piscan;

	piscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
	node->iss_ScanDesc =
		index_beginscan_parallel(node->ss.ss_currentRelation,
								 node->iss_RelationDesc,
								 node->iss_NumScanKeys,
								 node->iss_NumOrderByKeys,
								 piscan);

	/*
	 * If no run-time keys to calculate or they are ready, go ahead and pass
	 * the scankeys to the index AM.
	 */
	if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady)
		index_rescan(node->iss_ScanDesc,
					 node->iss_ScanKeys, node->iss_NumScanKeys,
					 node->iss_OrderByKeys, node->iss_NumOrderByKeys);
}
