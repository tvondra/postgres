/*-------------------------------------------------------------------------
 *
 * nodeBrinSort.c
 *	  Routines to support sorted scan of relations using a BRIN index
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * FIXME handling of ASC/DESC
 *
 * FIXME handling of NULLS FIRST/LAST
 *
 * FIXME handling of other brin opclasses (minmax, minmax-multi)
 *
 * FIXME improve costing
 *
 * FIXME handling of unsummarized ranges (both for NULL and regular phase)
 *
 *
 * Improvement ideas:
 *
 * 1) multiple tuplestores for overlapping ranges
 *
 * When there are many overlapping ranges (so that maxval > current.maxval),
 * we're loading all the "future" tuples into a new tuplestore. However, if
 * there are multiple such ranges (imagine ranges "shifting" by 10%, which
 * gives us 9 more ranges), we know in the next round we'll only need rows
 * until the next maxval. We'll not sort these rows, but we'll still shuffle
 * them around until we get to the proper range (so about 10x each row).
 * Maybe we should pre-allocate the tuplestores (or maybe even tuplesorts)
 * for future ranges, and route the tuples to the correct one? Maybe we
 * could be a bit smarter and discard tuples once we have enough rows for
 * the preceding ranges (say, with LIMIT queries). We'd also need to worry
 * about work_mem, though - we can't just use many tuplestores, each with
 * whole work_mem. So we'd probably use e.g. work_mem/2 for the next one,
 * and then /4, /8 etc. for the following ones. That's work_mem in total.
 * And there'd need to be some limit on number of tuplestores, I guess.
 *
 * 2) handling NULL values
 *
 * We need to handle NULLS FIRST / NULLS LAST cases. The question is how
 * to do that - the easiest way is to simply do a separate scan of ranges
 * that might contain NULL values, processing just rows with NULLs, and
 * discarding other rows. And then process non-NULL values as currently.
 * The NULL scan would happen before/after this regular phase.
 *
 * Byt maybe we could be smarter, and not do separate scans. When reading
 * a page, we might stash the tuple in a tuplestore, so that we can read
 * it the next round. Obviously, this might be expensive if we need to
 * keep too many rows, so the tuplestore would grow too large - in that
 * case it might be better to just do the two scans.
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
 *		ExecBrinSortMarkPos		marks scan position.
 *		ExecBrinSortRestrPos	restores scan position.
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

/* do various consistency checks */
static void
AssertCheckRanges(BrinSortState *node)
{
#ifdef USE_ASSERT_CHECKING

	/* the primary range index has to be valid */
	Assert((0 <= node->bs_next_range) &&
		   (node->bs_next_range <= node->bs_nranges));

	/* the intersect range index has to be valid*/
	Assert((0 <= node->bs_next_range_intersect) &&
		   (node->bs_next_range_intersect <= node->bs_nranges));

	/* all the ranges up to bs_next_range should be marked as processed */
	for (int i = 0; i < node->bs_next_range; i++)
	{
		BrinSortRange *range = &node->bs_ranges[i];
		Assert(range->processed);
	}

	/* same for bs_next_range_intersect */
	for (int i = 0; i < node->bs_next_range_intersect; i++)
	{
		BrinSortRange *range = node->bs_ranges_minval[i];
		Assert(range->processed);
	}
#endif
}

/*
 * brinsort_start_tidscan
 *		Start scanning tuples from a given page range.
 *
 * We open a TID range scan for the given range, and initialize the tuplesort.
 * Optionally, we update the watermark (with either high/low value). We only
 * need to do this for the main page range, not for the intersecting ranges.
 *
 * XXX Maybe we should initialize the tidscan only once, and then do rescan
 * for the following ranges? And similarly for the tuplesort?
 */
static void
brinsort_start_tidscan(BrinSortState *node, BrinSortRange *range,
					   bool update_watermark, bool mark_processed)
{
	BrinSort   *plan = (BrinSort *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;

	/*
	 * When scanning the range during NULL processing, in which case the range
	 * might be already marked as processed (for NULLS LAST). So we only check
	 * the page is not alreayd marked as processed when we're supposed to mark
	 * it as processed.
	 */
	Assert(!(mark_processed && range->processed));

	/* There must not be any TID scan in progress yet. */
	Assert(node->ss.ss_currentScanDesc == NULL);

	/* Initialize the TID range scan, for the provided block range. */
	if (node->ss.ss_currentScanDesc == NULL)
	{
		TableScanDesc		tscandesc;
		ItemPointerData		mintid,
							maxtid;

		ItemPointerSetBlockNumber(&mintid, range->blkno_start);
		ItemPointerSetOffsetNumber(&mintid, 0);

		ItemPointerSetBlockNumber(&maxtid, range->blkno_end);
		ItemPointerSetOffsetNumber(&maxtid, MaxHeapTuplesPerPage);

		elog(DEBUG1, "loading range blocks [%u, %u]",
			 range->blkno_start, range->blkno_end);

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
	}

	/*
	 * Remember maximum value for the current range (but not when
	 * processing overlapping ranges). We only do this during the
	 * regular tuple processing, not when scanning NULL values.
	 */
	if (update_watermark)
		node->bs_watermark = range->max_value;

	/* Maybe mark the range as processed. */
	range->processed = true;
}

/*
 * brinsort_end_tidscan
 *		Finish the TID range scan.
 */
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

/*
 * brinsort_load_tuples
 *		Load tuples from the TID range scan, add them to tuplesort/store.
 *
 * When called for the "current" range, we don't need to check the watermark,
 * we know the tuple goes into the tuplesort. So with check_watermark we
 * skip the comparator call to save CPU cost.
 */
static void
brinsort_load_tuples(BrinSortState *node, bool check_watermark)
{
	BrinSort	   *plan = (BrinSort *) node->ss.ps.plan;
	TableScanDesc	scan = node->ss.ss_currentScanDesc;
	EState		   *estate;
	ScanDirection	direction;
	TupleTableSlot *slot;
	SortSupportData	ssup;

	estate = node->ss.ps.state;
	direction = estate->es_direction;

	slot = node->ss.ss_ScanTupleSlot;

	/*
	 * Initialize info we'll need to compare the tuple value to the current
	 * watermark, so that we can decide if it goes into the tuplestore or
	 * tuplesort.
	 */
	memset(&ssup, 0, sizeof(SortSupportData));
	PrepareSortSupportFromOrderingOp(plan->sortOperators[0], &ssup);

	/*
	 * Read tuples, evaluate the filer (so that we don't keep tuples only to
	 * discard them later), and decide if it goes into the current range
	 * (tuplesort) or overflow (tuplestore).
	 */
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
			int		cmp = 0;	/* matters for check_watermark=false */
			Datum	value;
			bool	isnull;

			value = slot_getattr(slot, plan->sortColIdx[0], &isnull);

			/*
			 * FIXME Not handling NULLS for now, we need to stash them into
			 * a separate tuplestore (so that we can output them first or
			 * last), and then skip them in the regular processing?
			 */
			Assert(!isnull);

			if (check_watermark)
				cmp = ApplySortComparator(value, false,
										  node->bs_watermark, false,
										  &ssup);

			if (cmp <= 0)
				tuplesort_puttupleslot(node->bs_tuplesortstate, slot);
			else
				tuplestore_puttupleslot(node->bs_tuplestore, slot);
		}

		ExecClearTuple(slot);
	}

	ExecClearTuple(slot);
}

/*
 * brinsort_load_spill_tuples
 *		Load tuples from the spill tuplestore, and either stash them into
 *		a tuplesort or a new tuplestore.
 *
 * After processing the last range, we want to process all remaining ranges,
 * so with check_watermark=false we skip the check.
 */
static void
brinsort_load_spill_tuples(BrinSortState *node, bool check_watermark)
{
	BrinSort   *plan = (BrinSort *) node->ss.ps.plan;
	Tuplestorestate *tupstore;
	SortSupportData	ssup;
	TupleTableSlot *slot;

	/* prepare info for watermark comparison */
	memset(&ssup, 0, sizeof(SortSupportData));
	PrepareSortSupportFromOrderingOp(plan->sortOperators[0], &ssup);

	/* start scanning the existing tuplestore (XXX needed?) */
	tuplestore_rescan(node->bs_tuplestore);

	/*
	 * Create a new tuplestore, for tuples that exceed the watermark and so
	 * should not be included in the current sort.
	 */
	tupstore = tuplestore_begin_heap(false, false, work_mem);

	/*
	 * We need a slot for minimal tuples. The scan slot uses buffered tuples,
	 * so it'd trigger an error in the loop.
	 */
	slot = MakeSingleTupleTableSlot(RelationGetDescr(node->ss.ss_currentRelation),
									&TTSOpsMinimalTuple);

	while (tuplestore_gettupleslot(node->bs_tuplestore, true, true, slot))
	{
		int		cmp = 0;	/* matters for check_watermark=false */
		bool	isnull;
		Datum	value;

		value = slot_getattr(slot, plan->sortColIdx[0], &isnull);

		/* We shouldn't have NULL values in the spill, at least not now. */
		Assert(!isnull);

		if (check_watermark)
			cmp = ApplySortComparator(value, false,
									  node->bs_watermark, false,
									  &ssup);

		if (cmp <= 0)
			tuplesort_puttupleslot(node->bs_tuplesortstate, slot);
		else
			tuplestore_puttupleslot(tupstore, slot);
	}

	/*
	 * Discard the existing tuplestore (that we just processed), use the new
	 * one instead.
	 */
	tuplestore_end(node->bs_tuplestore);
	node->bs_tuplestore = tupstore;

	ExecDropSingleTupleTableSlot(slot);
}

/*
 * brinsort_load_intersecting_ranges
 *		Load ranges intersecting with the current watermark.
 *
 * This does not increment bs_next_range, but bs_next_range_intersect.
 */
static void
brinsort_load_intersecting_ranges(BrinSortState *node)
{
	BrinSort   *plan = (BrinSort *) node->ss.ps.plan;

	/* load intersecting ranges */
	for (int i = node->bs_next_range_intersect; i < node->bs_nranges; i++)
	{
		int	cmp;
		BrinSortRange  *range = node->bs_ranges_minval[i];
		SortSupportData	ssup;

		/* skip already processed ranges */
		if (range->processed)
			continue;

		memset(&ssup, 0, sizeof(SortSupportData));
		PrepareSortSupportFromOrderingOp(plan->sortOperators[0], &ssup);

		cmp = ApplySortComparator(range->min_value, false,
								  node->bs_watermark, false,
								  &ssup);

		/*
		 * No possible overlap, so break, we know all following ranges have
		 * a higher minval and thus can't intersect either.
		 */
		if (cmp > 0)
			break;

		node->bs_next_range_intersect++;

		elog(DEBUG1, "loading intersecting range %d (%u,%u) [%ld,%ld] %ld", i,
					  range->blkno_start, range->blkno_end,
					  range->min_value, range->max_value,
					  node->bs_watermark);

		/* load tuples from the rage, check the watermark */
		brinsort_start_tidscan(node, range, false, true);
		brinsort_load_tuples(node, true);
		brinsort_end_tidscan(node);
	}
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
	BrinSort   *plan = (BrinSort *) node->ss.ps.plan;
	EState	   *estate;
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
		 */
		ExecInitBrinSortRanges(plan, node);
		node->bs_next_range = 0;
		node->bs_next_range_intersect = 0;
		node->bs_phase = BRINSORT_START;


		/* dump ranges for debugging */
		for (int i = 0; i < node->bs_nranges; i++)
		{
			elog(DEBUG1, "%d => (%u,%u) [%ld,%ld]", i,
				 node->bs_ranges[i].blkno_start,
				 node->bs_ranges[i].blkno_end,
				 node->bs_ranges[i].min_value,
				 node->bs_ranges[i].max_value);
		}

		for (int i = 0; i < node->bs_nranges; i++)
		{
			elog(DEBUG1, "minval %d => (%u,%u) [%ld,%ld]", i,
				 node->bs_ranges_minval[i]->blkno_start,
				 node->bs_ranges_minval[i]->blkno_end,
				 node->bs_ranges_minval[i]->min_value,
				 node->bs_ranges_minval[i]->max_value);
		}
	}

	/*
	 * ok, now that we have what we need, fetch the next tuple.
	 */
	while (node->bs_phase != BRINSORT_FINISHED)
	{
		CHECK_FOR_INTERRUPTS();

		elog(DEBUG1, "phase = %d", node->bs_phase);

		AssertCheckRanges(node);

		switch (node->bs_phase)
		{
			case BRINSORT_START:
				node->bs_phase = BRINSORT_LOAD_RANGE;
				break;

			case BRINSORT_LOAD_RANGE:
				{
					BrinSortRange *range;

					elog(DEBUG1, "phase = LOAD_RANGE");

					/*
					 * Some of the ranges might intersect with already processed
					 * range and thus have already been processed, so skip them.
					 */
					while ((node->bs_next_range < node->bs_nranges) &&
						   (node->bs_ranges[node->bs_next_range].processed))
						node->bs_next_range++;

					Assert(node->bs_next_range <= node->bs_nranges);

					/*
					 * Did we process the last range? If we still have some rows
					 * in the tuplestore, sort them and move to process them.
					 */
					if ((node->bs_next_range == node->bs_nranges) &&
						(node->bs_tuplestore != NULL))
					{
						brinsort_load_spill_tuples(node, false);

						node->bs_tuplestore = NULL;

						tuplesort_performsort(node->bs_tuplesortstate);

						node->bs_phase = BRINSORT_PROCESS_RANGE;
						break;
					}

					/*
					 * We've reached the end, and there are no more rows in the
					 * tuplestore, so we're done.
					 */
					if (node->bs_next_range == node->bs_nranges)
					{
						elog(DEBUG1, "phase => FINISHED");
						node->bs_phase = BRINSORT_FINISHED;
						break;
					}

					range = &node->bs_ranges[node->bs_next_range];
					node->bs_next_range++;

					/*
					 * Load the next unprocessed range. We update the watermark,
					 * so that we don't need to check it when loading tuples.
					 */
					brinsort_start_tidscan(node, range, true, true);
					brinsort_load_tuples(node, false);
					brinsort_end_tidscan(node);

					/* Load matching tuples from the current spill tuplestore. */
					brinsort_load_spill_tuples(node, true);

					/*
					 * Load tuples from intersecting ranges.
					 *
					 * XXX We do this after processing the spill tuplestore,
					 * because we will add rows to it - but we know those rows
					 * should be there, and brinsort_load_spill would recheck
					 * them again unnecessarily.
					 */
					elog(DEBUG1, "loading intersecting ranges");
					brinsort_load_intersecting_ranges(node);

					elog(DEBUG1, "performing sort");
					tuplesort_performsort(node->bs_tuplesortstate);

					node->bs_phase = BRINSORT_PROCESS_RANGE;
					break;
				}
			case BRINSORT_PROCESS_RANGE:

				slot = node->ss.ps.ps_ResultTupleSlot;

				/* read tuples from the tuplesort range, and output them */
				if (node->bs_tuplesortstate != NULL)
				{
					if (tuplesort_gettupleslot(node->bs_tuplesortstate,
										ScanDirectionIsForward(direction),
										false, slot, NULL))
						return slot;

					/* once we're done with the tuplesort, reset it */
					tuplesort_reset(node->bs_tuplesortstate);
					node->bs_phase = BRINSORT_LOAD_RANGE;	/* load next range */
				}

				break;

			case BRINSORT_LOAD_NULLS:
				elog(ERROR, "unexpected BrinSort phase: LOAD_NULLS");
				break;

			case BRINSORT_FINISHED:
				elog(ERROR, "unexpected BrinSort phase: FINISHED");
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
 *		ExecBrinSortMarkPos
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
				elog(ERROR, "unexpected ExecBrinSortMarkPos call in EPQ recheck");
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
				elog(ERROR, "unexpected ExecBrinSortRestrPos call in EPQ recheck");
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

static int
brin_sort_rangeptr_cmp(const void *a, const void *b, void *arg)
{
	BrinSortRange  *ra = *(BrinSortRange **) a;
	BrinSortRange  *rb = *(BrinSortRange **) b;
	SortSupport		ssup = (SortSupport) arg;

	/* XXX consider NULL FIRST/LAST and ASC/DESC */
	/* XXX also handle un-summarized ranges */

	return ApplySortComparator(ra->min_value, false, rb->min_value, false, ssup);
}

/*
 * somewhat crippled verson of bringetbitmap
 *
 * XXX We don't call consistent function (or any other function), so unlike
 * bringetbitmap we don't set a separate memory context. If we end up filtering
 * the ranges somehow (e.g. by WHERE conditions), this might be necessary.
 *
 * XXX Should be part of opclass, to somewhere in brin_minmax.c etc.
 */
static void
ExecInitBrinSortRanges(BrinSort *node, BrinSortState *planstate)
{
	IndexScanDesc	scan = planstate->iss_ScanDesc;
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
	int			attno;

	/* BRIN Sort only allows ORDER BY using a single column */
	Assert(node->numCols == 1);

	/*
	 * Determine how many BRIN ranges could there be, allocate space and read
	 * all the min/max values.
	 */
	opaque = (BrinOpaque *) scan->opaque;
	bdesc = opaque->bo_bdesc;

	/*
	 * Determine index attnum we're interested in. The sortColIdx has attnums
	 * from the table, but we need index attnum so that we can fetch the right
	 * range summary.
	 *
	 * XXX Maybe we could/should arrange the tlists differently, so that this
	 * is not necessary?
	 */
	attno = 0;
	for (int i = 0; i < indexRel->rd_index->indnatts; i++)
	{
		if (indexRel->rd_index->indkey.values[i] == node->sortColIdx[0])
		{
			attno = (i + 1);
			break;
		}
	}

	/* make sure the calculated attnum is valid */
	Assert((attno > 0) && (attno <= bdesc->bd_tupdesc->natts));

	/*
	 * We need to know the size of the table so that we know how long to iterate
	 * on the revmap.
	 */
	heapOid = IndexGetRelation(RelationGetRelid(indexRel), false);
	heapRel = table_open(heapOid, AccessShareLock);
	nblocks = RelationGetNumberOfBlocks(heapRel);
	table_close(heapRel, AccessShareLock);

	/*
	 * How many ranges can there be? We simply look at the number of pages,
	 * divide it by the pages_per_range.
	 *
	 * XXX We need to be careful not to overflow nranges, so we just divide
	 * and then maybe add 1 for partial ranges.
	 */
	nranges = (nblocks / opaque->bo_pagesPerRange);
	if (nblocks % opaque->bo_pagesPerRange != 0)
		nranges += 1;

	/* allocate for space, and also for the alternative ordering */
	planstate->bs_nranges = 0;
	planstate->bs_ranges = (BrinSortRange *) palloc0(nranges * sizeof(BrinSortRange));
	planstate->bs_ranges_minval = (BrinSortRange **) palloc0(nranges * sizeof(BrinSortRange *));

	/* allocate an initial in-memory tuple, out of the per-range memcxt */
	dtup = brin_new_memtuple(bdesc);

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
		BrinSortRange *range = &planstate->bs_ranges[planstate->bs_nranges];

		planstate->bs_ranges_minval[planstate->bs_nranges] = range;

		planstate->bs_nranges++;

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
					/* FIXME copy the values, if needed (e.g. varlena) */
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

	qsort_arg(planstate->bs_ranges_minval, planstate->bs_nranges, sizeof(BrinSortRange *),
			  brin_sort_rangeptr_cmp, &ssup);
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
