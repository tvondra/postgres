/*-------------------------------------------------------------------------
 *
 * nodeBatch.c
 *	  Support routines for node Batching tuples from the child node.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeBatch.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecBatch			sequentially scans a relation.
 *		ExecInitBatch		creates and initializes a seqscan node.
 *		ExecEndBatch		releases any storage allocated.
 *		ExecReScanBatch	rescans the relation
 */
#include "postgres.h"

#include "access/relscan.h"
#include "executor/execdebug.h"
#include "executor/nodeBatch.h"
#include "utils/rel.h"
#include "utils/memutils.h"

/* ----------------------------------------------------------------
 *		ExecBatch(node)
 *
 *		Either gets the relation from a Batch, or from the child node.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecBatch(BatchState *state)
{
	Assert(state->batch_current <= state->batch_size);

	/* Do we need to fetch another batch? */
	if ((state->batch_size == state->batch_current) && (! state->finished))
	{
		int i;

		/* first, clear the slots (if necessary) */
		for (i = 0; i < state->batch_size; i++)
			ExecClearTuple(state->slots[i]);

		/* reset the counters */
		state->batch_current = 0;
		state->batch_size = 0;

		/* next fill the batch until it's full or we get the last tuple */
		while (state->batch_size < state->batch_limit)
		{
			TupleTableSlot * slot = ExecProcNode(state->ps.lefttree);

			/* if we got the last tuple, terminate */
			if (TupIsNull(slot))
			{
				state->finished = true;
				break;
			}

			/* init the slot if necessary */
			if (state->slots[state->batch_size] == NULL)
			{
				state->slots[state->batch_size]
					= MakeSingleTupleTableSlot(CreateTupleDescCopy(slot->tts_tupleDescriptor));
			}

			/* nope, got another tuple - store it into the batch */
			ExecCopySlot(state->slots[state->batch_size++], slot);
		}
	}

	/* if we still don't have any tuples, we're surely finished */
	if (state->batch_size == state->batch_current)
	{
		Assert(state->finished);
		return NULL;
	}

	return state->slots[state->batch_current++];
}

/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
BatchState *
ExecInitBatch(Batch *node, EState *estate, int eflags)
{
	Plan	   *outerNode;
	BatchState *scanstate;

	/*
	 * Batch always has outer relation and no inner relation.
	 */
	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(BatchState);
	scanstate->ps.plan = (Plan *) node;
	scanstate->ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ps);

	outerNode = outerPlan(node);

	outerPlanState(scanstate) = ExecInitNode(outerNode, estate, eflags);

	/*
	 * initialize child expressions
	 */
	scanstate->ps.targetlist = outerPlanState(scanstate)->targetlist;
	// scanstate->ps.qual = outerPlanState(scanstate)->qual;

	/*
	 * just reference the result tuple slot in the child
	 */
	scanstate->ps.ps_ResultTupleSlot = outerPlanState(scanstate)->ps_ResultTupleSlot;

	/*
	 * initialize the batch memory context etc.
	 */
	scanstate->mcontext_batch
		= AllocSetContextCreate(estate->es_query_cxt,
								"Batch Scan",
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_MAXSIZE);

	/* TODO this might/should be determined using the average tuple witdh */
	scanstate->batch_limit = 256;

	/* nothing loaded */
	scanstate->batch_size = 0;
	scanstate->batch_current = 0;
	scanstate->finished = false;

	scanstate->slots
		= (TupleTableSlot**)palloc0(scanstate->batch_limit * sizeof(TupleTableSlot*));

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndBatch
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndBatch(BatchState *node)
{
	/* FIXME release the resources properly */
	ExecEndNode(outerPlanState(node));
}

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanBatch(BatchState *node)
{
	/* just pass the rescan to the child node */
	ExecReScan(outerPlanState(node));

	/* reset the counters (just mark it as 'full') */
	node->batch_current = node->batch_size;
}
