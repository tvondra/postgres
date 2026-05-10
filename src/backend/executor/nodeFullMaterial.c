/*-------------------------------------------------------------------------
 *
 * nodeFullMaterial.c
 *	  Routines to handle FullMaterialization nodes.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeFullMaterial.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecFullMaterial			- FullMaterialize the result of a subplan
 *		ExecInitFullMaterial		- initialize node and subnodes
 *		ExecEndFullMaterial			- shutdown node and subnodes
 *
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeFullMaterial.h"
#include "miscadmin.h"
#include "utils/tuplestore.h"

/* ----------------------------------------------------------------
 *		ExecFullMaterial
 *
 *		As long as we are at the end of the data collected in the tuplestore,
 *		we collect one new row from the subplan on each call, and stash it
 *		aside in the tuplestore before returning it.  The tuplestore is
 *		only read if we are asked to scan backwards, rescan, or mark/restore.
 *
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* result tuple from subplan */
ExecFullMaterial(PlanState *pstate)
{
	FullMaterialState *node = castNode(FullMaterialState, pstate);
	EState	   *estate;
	ScanDirection dir;
	bool		forward;
	Tuplestorestate *tuplestorestate;
	bool		eof_tuplestore;
	TupleTableSlot *slot;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get state info from node
	 */
	estate = node->ss.ps.state;
	dir = estate->es_direction;
	forward = ScanDirectionIsForward(dir);
	tuplestorestate = node->tuplestorestate;

	/* only forward direction for now */
	Assert(forward);

	/*
	 * If first time through, initialize the tuplestore.
	 */
	if (tuplestorestate == NULL)
	{
		tuplestorestate = tuplestore_begin_heap(true, false, work_mem);
		tuplestore_set_eflags(tuplestorestate, node->eflags);
		if (node->eflags & EXEC_FLAG_MARK)
		{
			/*
			 * Allocate a second read pointer to serve as the mark. We know it
			 * must have index 1, so needn't store that.
			 */
			int			ptrno PG_USED_FOR_ASSERTS_ONLY;

			ptrno = tuplestore_alloc_read_pointer(tuplestorestate,
												  node->eflags);
			Assert(ptrno == 1);
		}
		node->tuplestorestate = tuplestorestate;

		/*
		 * Fetch all results from the subplan, add all tuples into the tuple
		 * store.
		 */
		for (;;)
		{
			PlanState  *outerNode;
			TupleTableSlot *outerslot;

			outerNode = outerPlanState(node);
			outerslot = ExecProcNode(outerNode);

			if (TupIsNull(outerslot))
				break;

			/*
			 * Append a copy of the returned tuple to tuplestore.  NOTE: because
			 * the tuplestore is certainly in EOF state, its read position will
			 * move forward over the added tuple.  This is what we want.
			 */
			if (tuplestorestate)
				tuplestore_puttupleslot(tuplestorestate, outerslot);
		}

		/* XXX not sure if necessary, but do it anyway */
		// tuplestore_rescan(tuplestorestate);
	}

	/*
	 * If we are not at the end of the tuplestore, or are going backwards, try
	 * to fetch a tuple from tuplestore.
	 */
	eof_tuplestore = (tuplestorestate == NULL) ||
		tuplestore_ateof(tuplestorestate);

	/*
	 * If we can fetch another tuple from the tuplestore, return it.
	 */
	slot = node->ss.ps.ps_ResultTupleSlot;
	if (!eof_tuplestore)
	{
		if (tuplestore_gettupleslot(tuplestorestate, forward, false, slot))
			return slot;
	}

	/*
	 * Nothing left ...
	 */
	return ExecClearTuple(slot);
}

/* ----------------------------------------------------------------
 *		ExecInitFullMaterial
 * ----------------------------------------------------------------
 */
FullMaterialState *
ExecInitFullMaterial(FullMaterial *node, EState *estate, int eflags)
{
	FullMaterialState *matstate;
	Plan	   *outerPlan;

	/*
	 * create state structure
	 */
	matstate = makeNode(FullMaterialState);
	matstate->ss.ps.plan = (Plan *) node;
	matstate->ss.ps.state = estate;
	matstate->ss.ps.ExecProcNode = ExecFullMaterial;

	/*
	 * We must have a tuplestore buffering the subplan output to do backward
	 * scan or mark/restore.  We also prefer to FullMaterialize the subplan output
	 * if we might be called on to rewind and replay it many times. However,
	 * if none of these cases apply, we can skip storing the data.
	 */
	matstate->eflags = (eflags & (EXEC_FLAG_REWIND |
								  EXEC_FLAG_BACKWARD |
								  EXEC_FLAG_MARK));

	/*
	 * Tuplestore's interpretation of the flag bits is subtly different from
	 * the general executor meaning: it doesn't think BACKWARD necessarily
	 * means "backwards all the way to start".  If told to support BACKWARD we
	 * must include REWIND in the tuplestore eflags, else tuplestore_trim
	 * might throw away too much.
	 */
	if (eflags & EXEC_FLAG_BACKWARD)
		matstate->eflags |= EXEC_FLAG_REWIND;

	matstate->tuplestorestate = NULL;

	/*
	 * Miscellaneous initialization
	 *
	 * FullMaterialization nodes don't need ExprContexts because they never call
	 * ExecQual or ExecProject.
	 */

	/*
	 * initialize child nodes
	 *
	 * We shield the child node from the need to support REWIND, BACKWARD, or
	 * MARK/RESTORE.
	 */
	eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

	outerPlan = outerPlan(node);
	outerPlanState(matstate) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * Initialize result type and slot. No need to initialize projection info
	 * because this node doesn't do projections.
	 *
	 * FullMaterial nodes only return tuples from their FullMaterialized relation.
	 */
	ExecInitResultTupleSlotTL(&matstate->ss.ps, &TTSOpsMinimalTuple);
	matstate->ss.ps.ps_ProjInfo = NULL;

	/*
	 * initialize tuple type.
	 */
	ExecCreateScanSlotFromOuterPlan(estate, &matstate->ss, &TTSOpsMinimalTuple);

	return matstate;
}

/* ----------------------------------------------------------------
 *		ExecEndFullMaterial
 * ----------------------------------------------------------------
 */
void
ExecEndFullMaterial(FullMaterialState *node)
{
	/*
	 * Release tuplestore resources
	 */
	if (node->tuplestorestate != NULL)
		tuplestore_end(node->tuplestorestate);
	node->tuplestorestate = NULL;

	/*
	 * shut down the subplan
	 */
	ExecEndNode(outerPlanState(node));
}

/* ----------------------------------------------------------------
 *		ExecFullMaterialMarkPos
 *
 *		Calls tuplestore to save the current position in the stored file.
 * ----------------------------------------------------------------
 */
void
ExecFullMaterialMarkPos(FullMaterialState *node)
{
	Assert(node->eflags & EXEC_FLAG_MARK);

	/*
	 * if we haven't FullMaterialized yet, just return.
	 */
	if (!node->tuplestorestate)
		return;

	/*
	 * copy the active read pointer to the mark.
	 */
	tuplestore_copy_read_pointer(node->tuplestorestate, 0, 1);

	/*
	 * since we may have advanced the mark, try to truncate the tuplestore.
	 */
	tuplestore_trim(node->tuplestorestate);
}

/* ----------------------------------------------------------------
 *		ExecFullMaterialRestrPos
 *
 *		Calls tuplestore to restore the last saved file position.
 * ----------------------------------------------------------------
 */
void
ExecFullMaterialRestrPos(FullMaterialState *node)
{
	Assert(node->eflags & EXEC_FLAG_MARK);

	/*
	 * if we haven't FullMaterialized yet, just return.
	 */
	if (!node->tuplestorestate)
		return;

	/*
	 * copy the mark to the active read pointer.
	 */
	tuplestore_copy_read_pointer(node->tuplestorestate, 1, 0);
}

/* ----------------------------------------------------------------
 *		ExecReScanFullMaterial
 *
 *		Rescans the FullMaterialized relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanFullMaterial(FullMaterialState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	if (node->eflags != 0)
	{
		/*
		 * If we haven't FullMaterialized yet, just return. If outerplan's
		 * chgParam is not NULL then it will be re-scanned by ExecProcNode,
		 * else no reason to re-scan it at all.
		 */
		if (!node->tuplestorestate)
			return;

		/*
		 * If subnode is to be rescanned then we forget previous stored
		 * results; we have to re-read the subplan and re-store.  Also, if we
		 * told tuplestore it needn't support rescan, we lose and must
		 * re-read.  (This last should not happen in common cases; else our
		 * caller lied by not passing EXEC_FLAG_REWIND to us.)
		 *
		 * Otherwise we can just rewind and rescan the stored output. The
		 * state of the subnode does not change.
		 */
		if (outerPlan->chgParam != NULL ||
			(node->eflags & EXEC_FLAG_REWIND) == 0)
		{
			tuplestore_end(node->tuplestorestate);
			node->tuplestorestate = NULL;
			if (outerPlan->chgParam == NULL)
				ExecReScan(outerPlan);
		}
		else
			tuplestore_rescan(node->tuplestorestate);
	}
	else
	{
		/* In this case we are just passing on the subquery's output */

		/*
		 * if chgParam of subnode is not null then plan will be re-scanned by
		 * first ExecProcNode.
		 */
		if (outerPlan->chgParam == NULL)
			ExecReScan(outerPlan);
	}
}
