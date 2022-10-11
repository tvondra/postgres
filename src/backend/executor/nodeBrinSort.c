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
 *		IndexNextWithReorder	same, but recheck ORDER BY expressions
 *		ExecInitBrinSort		creates and initializes state info.
 *		ExecReScanBrinSort		rescans the indexed relation.
 *		ExecEndBrinSort		releases all storage.
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
static TupleTableSlot *IndexNextWithReorder(BrinSortState *node);
static void EvalOrderByExpressions(BrinSortState *node, ExprContext *econtext);
static bool IndexRecheck(BrinSortState *node, TupleTableSlot *slot);
static int	cmp_orderbyvals(const Datum *adist, const bool *anulls,
							const Datum *bdist, const bool *bnulls,
							BrinSortState *node);
static int	reorderqueue_cmp(const pairingheap_node *a,
							 const pairingheap_node *b, void *arg);
static void reorderqueue_push(BrinSortState *node, TupleTableSlot *slot,
							  Datum *orderbyvals, bool *orderbynulls);
static HeapTuple reorderqueue_pop(BrinSortState *node);

static void ExecInitBrinSortRanges(BrinSort *node, BrinSortState *planstate);


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
	ExprContext *econtext;
	ScanDirection direction;
	IndexScanDesc scandesc;
	TupleTableSlot *slot;
	BrinSort *plan = (BrinSort *) node->ss.ps.plan;

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
	econtext = node->ss.ps.ps_ExprContext;
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
	}

	/*
	 * ok, now that we have what we need, fetch the next tuple.
	 */
	// while (true)
	{
		CHECK_FOR_INTERRUPTS();

		/* get the first range, read all tuples using a tid range scan */
		if (node->ss.ss_currentScanDesc == NULL)
		{
			TableScanDesc		tscandesc;
			ItemPointerData		mintid,
								maxtid;

			BrinSortRange *range = &node->bs_ranges[node->bs_next_range];

			ItemPointerSetBlockNumber(&mintid, range->blkno_start);
			ItemPointerSetOffsetNumber(&mintid, 0);

			ItemPointerSetBlockNumber(&maxtid, range->blkno_end);
			ItemPointerSetOffsetNumber(&maxtid, MaxHeapTuplesPerPage);

			elog(DEBUG1, "initializing tidscan");

			tscandesc = table_beginscan_tidrange(node->ss.ss_currentRelation,
												 estate->es_snapshot,
												 &mintid, &maxtid);
			node->ss.ss_currentScanDesc = tscandesc;
		}

		if (node->tuplesortstate == NULL)
		{
			TupleDesc	tupDesc = RelationGetDescr(node->ss.ss_currentRelation);

			node->tuplesortstate = tuplesort_begin_heap(tupDesc,
														plan->numCols,
														plan->sortColIdx,
														plan->sortOperators,
														plan->collations,
														plan->nullsFirst,
														work_mem,
														NULL,
														TUPLESORT_NONE);

			while (table_scan_getnextslot_tidrange(node->ss.ss_currentScanDesc, direction, slot))
			{
				elog(DEBUG1, "adding tuple");
				tuplesort_puttupleslot(node->tuplesortstate, slot);
				ExecClearTuple(slot);
			}

			ExecClearTuple(slot);

			elog(DEBUG1, "performing sort");
			tuplesort_performsort(node->tuplesortstate);
		}
		
		// break;
	}

		slot = node->ss.ps.ps_ResultTupleSlot;

		if (node->tuplesortstate != NULL)
		{
			elog(DEBUG1, "getting tuple");
			if (tuplesort_gettupleslot(node->tuplesortstate,
								  ScanDirectionIsForward(direction),
								  false, slot, NULL))
			{
				return slot;
			}

			node->tuplesortstate = NULL;
		}

	/*
	 * if we get here it means the index scan failed so we are at the end of
	 * the scan..
	 */
	node->iss_ReachedEnd = true;
	return ExecClearTuple(slot);
}


/* ----------------------------------------------------------------
 *		IndexNextWithReorder
 *
 *		Like IndexNext, but this version can also re-check ORDER BY
 *		expressions, and reorder the tuples as necessary.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
IndexNextWithReorder(BrinSortState *node)
{
	EState	   *estate;
	ExprContext *econtext;
	IndexScanDesc scandesc;
	TupleTableSlot *slot;
	ReorderTuple *topmost = NULL;
	bool		was_exact;
	Datum	   *lastfetched_vals;
	bool	   *lastfetched_nulls;
	int			cmp;

	estate = node->ss.ps.state;

	/*
	 * Only forward scan is supported with reordering.  Note: we can get away
	 * with just Asserting here because the system will not try to run the
	 * plan backwards if ExecSupportsBackwardScan() says it won't work.
	 * Currently, that is guaranteed because no index AMs support both
	 * amcanorderbyop and amcanbackward; if any ever do,
	 * ExecSupportsBackwardScan() will need to consider indexorderbys
	 * explicitly.
	 */
	Assert(!ScanDirectionIsBackward(((BrinSort *) node->ss.ps.plan)->indexorderdir));
	Assert(ScanDirectionIsForward(estate->es_direction));

	scandesc = node->iss_ScanDesc;
	econtext = node->ss.ps.ps_ExprContext;
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
	}

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * Check the reorder queue first.  If the topmost tuple in the queue
		 * has an ORDER BY value smaller than (or equal to) the value last
		 * returned by the index, we can return it now.
		 */
		if (!pairingheap_is_empty(node->iss_ReorderQueue))
		{
			topmost = (ReorderTuple *) pairingheap_first(node->iss_ReorderQueue);

			if (node->iss_ReachedEnd ||
				cmp_orderbyvals(topmost->orderbyvals,
								topmost->orderbynulls,
								scandesc->xs_orderbyvals,
								scandesc->xs_orderbynulls,
								node) <= 0)
			{
				HeapTuple	tuple;

				tuple = reorderqueue_pop(node);

				/* Pass 'true', as the tuple in the queue is a palloc'd copy */
				ExecForceStoreHeapTuple(tuple, slot, true);
				return slot;
			}
		}
		else if (node->iss_ReachedEnd)
		{
			/* Queue is empty, and no more tuples from index.  We're done. */
			return ExecClearTuple(slot);
		}

		/*
		 * Fetch next tuple from the index.
		 */
next_indextuple:
		if (!index_getnext_slot(scandesc, ForwardScanDirection, slot))
		{
			/*
			 * No more tuples from the index.  But we still need to drain any
			 * remaining tuples from the queue before we're done.
			 */
			node->iss_ReachedEnd = true;
			continue;
		}

		/*
		 * If the index was lossy, we have to recheck the index quals and
		 * ORDER BY expressions using the fetched tuple.
		 */
		if (scandesc->xs_recheck)
		{
			econtext->ecxt_scantuple = slot;
			if (!ExecQualAndReset(node->indexqualorig, econtext))
			{
				/* Fails recheck, so drop it and loop back for another */
				InstrCountFiltered2(node, 1);
				/* allow this loop to be cancellable */
				CHECK_FOR_INTERRUPTS();
				goto next_indextuple;
			}
		}

		if (scandesc->xs_recheckorderby)
		{
			econtext->ecxt_scantuple = slot;
			ResetExprContext(econtext);
			EvalOrderByExpressions(node, econtext);

			/*
			 * Was the ORDER BY value returned by the index accurate?  The
			 * recheck flag means that the index can return inaccurate values,
			 * but then again, the value returned for any particular tuple
			 * could also be exactly correct.  Compare the value returned by
			 * the index with the recalculated value.  (If the value returned
			 * by the index happened to be exact right, we can often avoid
			 * pushing the tuple to the queue, just to pop it back out again.)
			 */
			cmp = cmp_orderbyvals(node->iss_OrderByValues,
								  node->iss_OrderByNulls,
								  scandesc->xs_orderbyvals,
								  scandesc->xs_orderbynulls,
								  node);
			if (cmp < 0)
				elog(ERROR, "index returned tuples in wrong order");
			else if (cmp == 0)
				was_exact = true;
			else
				was_exact = false;
			lastfetched_vals = node->iss_OrderByValues;
			lastfetched_nulls = node->iss_OrderByNulls;
		}
		else
		{
			was_exact = true;
			lastfetched_vals = scandesc->xs_orderbyvals;
			lastfetched_nulls = scandesc->xs_orderbynulls;
		}

		/*
		 * Can we return this tuple immediately, or does it need to be pushed
		 * to the reorder queue?  If the ORDER BY expression values returned
		 * by the index were inaccurate, we can't return it yet, because the
		 * next tuple from the index might need to come before this one. Also,
		 * we can't return it yet if there are any smaller tuples in the queue
		 * already.
		 */
		if (!was_exact || (topmost && cmp_orderbyvals(lastfetched_vals,
													  lastfetched_nulls,
													  topmost->orderbyvals,
													  topmost->orderbynulls,
													  node) > 0))
		{
			/* Put this tuple to the queue */
			reorderqueue_push(node, slot, lastfetched_vals, lastfetched_nulls);
			continue;
		}
		else
		{
			/* Can return this tuple immediately. */
			return slot;
		}
	}

	/*
	 * if we get here it means the index scan failed so we are at the end of
	 * the scan..
	 */
	return ExecClearTuple(slot);
}

/*
 * Calculate the expressions in the ORDER BY clause, based on the heap tuple.
 */
static void
EvalOrderByExpressions(BrinSortState *node, ExprContext *econtext)
{
	int			i;
	ListCell   *l;
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	i = 0;
	foreach(l, node->indexorderbyorig)
	{
		ExprState  *orderby = (ExprState *) lfirst(l);

		node->iss_OrderByValues[i] = ExecEvalExpr(orderby,
												  econtext,
												  &node->iss_OrderByNulls[i]);
		i++;
	}

	MemoryContextSwitchTo(oldContext);
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


/*
 * Compare ORDER BY expression values.
 */
static int
cmp_orderbyvals(const Datum *adist, const bool *anulls,
				const Datum *bdist, const bool *bnulls,
				BrinSortState *node)
{
	int			i;
	int			result;

	for (i = 0; i < node->iss_NumOrderByKeys; i++)
	{
		SortSupport ssup = &node->iss_SortSupport[i];

		/*
		 * Handle nulls.  We only need to support NULLS LAST ordering, because
		 * match_pathkeys_to_index() doesn't consider indexorderby
		 * implementation otherwise.
		 */
		if (anulls[i] && !bnulls[i])
			return 1;
		else if (!anulls[i] && bnulls[i])
			return -1;
		else if (anulls[i] && bnulls[i])
			return 0;

		result = ssup->comparator(adist[i], bdist[i], ssup);
		if (result != 0)
			return result;
	}

	return 0;
}

/*
 * Pairing heap provides getting topmost (greatest) element while KNN provides
 * ascending sort.  That's why we invert the sort order.
 */
static int
reorderqueue_cmp(const pairingheap_node *a, const pairingheap_node *b,
				 void *arg)
{
	ReorderTuple *rta = (ReorderTuple *) a;
	ReorderTuple *rtb = (ReorderTuple *) b;
	BrinSortState *node = (BrinSortState *) arg;

	/* exchange argument order to invert the sort order */
	return cmp_orderbyvals(rtb->orderbyvals, rtb->orderbynulls,
						   rta->orderbyvals, rta->orderbynulls,
						   node);
}

/*
 * Helper function to push a tuple to the reorder queue.
 */
static void
reorderqueue_push(BrinSortState *node, TupleTableSlot *slot,
				  Datum *orderbyvals, bool *orderbynulls)
{
	IndexScanDesc scandesc = node->iss_ScanDesc;
	EState	   *estate = node->ss.ps.state;
	MemoryContext oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
	ReorderTuple *rt;
	int			i;

	rt = (ReorderTuple *) palloc(sizeof(ReorderTuple));
	rt->htup = ExecCopySlotHeapTuple(slot);
	rt->orderbyvals =
		(Datum *) palloc(sizeof(Datum) * scandesc->numberOfOrderBys);
	rt->orderbynulls =
		(bool *) palloc(sizeof(bool) * scandesc->numberOfOrderBys);
	for (i = 0; i < node->iss_NumOrderByKeys; i++)
	{
		if (!orderbynulls[i])
			rt->orderbyvals[i] = datumCopy(orderbyvals[i],
										   node->iss_OrderByTypByVals[i],
										   node->iss_OrderByTypLens[i]);
		else
			rt->orderbyvals[i] = (Datum) 0;
		rt->orderbynulls[i] = orderbynulls[i];
	}
	pairingheap_add(node->iss_ReorderQueue, &rt->ph_node);

	MemoryContextSwitchTo(oldContext);
}

/*
 * Helper function to pop the next tuple from the reorder queue.
 */
static HeapTuple
reorderqueue_pop(BrinSortState *node)
{
	HeapTuple	result;
	ReorderTuple *topmost;
	int			i;

	topmost = (ReorderTuple *) pairingheap_remove_first(node->iss_ReorderQueue);

	result = topmost->htup;
	for (i = 0; i < node->iss_NumOrderByKeys; i++)
	{
		if (!node->iss_OrderByTypByVals[i] && !topmost->orderbynulls[i])
			pfree(DatumGetPointer(topmost->orderbyvals[i]));
	}
	pfree(topmost->orderbyvals);
	pfree(topmost->orderbynulls);
	pfree(topmost);

	return result;
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

	if (node->iss_NumOrderByKeys > 0)
		return ExecScan(&node->ss,
						(ExecScanAccessMtd) IndexNextWithReorder,
						(ExecScanRecheckMtd) IndexRecheck);
	else
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

	/* flush the reorder queue */
	if (node->iss_ReorderQueue)
	{
		HeapTuple	tuple;

		while (!pairingheap_is_empty(node->iss_ReorderQueue))
		{
			tuple = reorderqueue_pop(node);
			heap_freetuple(tuple);
		}
	}

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
	indexstate->indexorderbyorig =
		ExecInitExprList(node->indexorderbyorig, (PlanState *) indexstate);

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
						   node->indexorderby,
						   true,
						   &indexstate->iss_OrderByKeys,
						   &indexstate->iss_NumOrderByKeys,
						   &indexstate->iss_RuntimeKeys,
						   &indexstate->iss_NumRuntimeKeys,
						   NULL,	/* no ArrayKeys */
						   NULL);

	/* Initialize sort support, if we need to re-check ORDER BY exprs */
	if (indexstate->iss_NumOrderByKeys > 0)
	{
		int			numOrderByKeys = indexstate->iss_NumOrderByKeys;
		int			i;
		ListCell   *lco;
		ListCell   *lcx;

		/*
		 * Prepare sort support, and look up the data type for each ORDER BY
		 * expression.
		 */
		Assert(numOrderByKeys == list_length(node->indexorderbyops));
		Assert(numOrderByKeys == list_length(node->indexorderbyorig));
		indexstate->iss_SortSupport = (SortSupportData *)
			palloc0(numOrderByKeys * sizeof(SortSupportData));
		indexstate->iss_OrderByTypByVals = (bool *)
			palloc(numOrderByKeys * sizeof(bool));
		indexstate->iss_OrderByTypLens = (int16 *)
			palloc(numOrderByKeys * sizeof(int16));
		i = 0;
		forboth(lco, node->indexorderbyops, lcx, node->indexorderbyorig)
		{
			Oid			orderbyop = lfirst_oid(lco);
			Node	   *orderbyexpr = (Node *) lfirst(lcx);
			Oid			orderbyType = exprType(orderbyexpr);
			Oid			orderbyColl = exprCollation(orderbyexpr);
			SortSupport orderbysort = &indexstate->iss_SortSupport[i];

			/* Initialize sort support */
			orderbysort->ssup_cxt = CurrentMemoryContext;
			orderbysort->ssup_collation = orderbyColl;
			/* See cmp_orderbyvals() comments on NULLS LAST */
			orderbysort->ssup_nulls_first = false;
			/* ssup_attno is unused here and elsewhere */
			orderbysort->ssup_attno = 0;
			/* No abbreviation */
			orderbysort->abbreviate = false;
			PrepareSortSupportFromOrderingOp(orderbyop, orderbysort);

			get_typlenbyval(orderbyType,
							&indexstate->iss_OrderByTypLens[i],
							&indexstate->iss_OrderByTypByVals[i]);
			i++;
		}

		/* allocate arrays to hold the re-calculated distances */
		indexstate->iss_OrderByValues = (Datum *)
			palloc(numOrderByKeys * sizeof(Datum));
		indexstate->iss_OrderByNulls = (bool *)
			palloc(numOrderByKeys * sizeof(bool));

		/* and initialize the reorder queue */
		indexstate->iss_ReorderQueue = pairingheap_allocate(reorderqueue_cmp,
															indexstate);
	}

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

	indexstate->tuplesortstate = NULL;

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
