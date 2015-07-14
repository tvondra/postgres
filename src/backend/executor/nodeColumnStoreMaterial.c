/*-------------------------------------------------------------------------
 *
 * nodeColumnStoreMaterial.c
 *	  Routines to handle column store materialization nodes.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeColumnStoreMaterial.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecColumnStoreMaterial		- materialize the result of a subplan
 *		ExecInitColumnStoreMaterial	- initialize node and subnodes
 *		ExecEndColumnStoreMaterial	- shutdown node and subnodes
 *
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeColumnStoreMaterial.h"
#include "miscadmin.h"

/* ----------------------------------------------------------------
 *		ExecColumnStoreMaterial
 *
 *		As long as we are at the end of the data collected in the tuplestore,
 *		we collect one new row from the subplan on each call, and stash it
 *		aside in the tuplestore before returning it.  The tuplestore is
 *		only read if we are asked to scan backwards, rescan, or mark/restore.
 *
 * ----------------------------------------------------------------
 */
TupleTableSlot *				/* result tuple from subplan */
ExecColumnStoreMaterial(ColumnStoreMaterialState *node)
{
//	EState	   *estate;
//	ScanDirection dir;
//	bool		forward;

	PlanState  *outerNode;
	TupleTableSlot *outerslot;

	/*
	 * get state info from node
	 */
//	estate = node->ss.ps.state;
//	dir = estate->es_direction;
//	forward = ScanDirectionIsForward(dir);

	/* simply read tuple from the outer node (left subtree) */
	outerNode = outerPlanState(node);
	outerslot = ExecProcNode(outerNode);

	if (TupIsNull(outerslot))
		return NULL;

	return outerslot;
}

/* ----------------------------------------------------------------
 *		ExecInitMaterial
 * ----------------------------------------------------------------
 */
ColumnStoreMaterialState *
ExecInitColumnStoreMaterial(ColumnStoreMaterial *node, EState *estate, int eflags)
{
	ColumnStoreMaterialState *colmatstate;
	Plan	   *outerPlan;

	/*
	 * create state structure
	 */
	colmatstate = makeNode(ColumnStoreMaterialState);
	colmatstate->ss.ps.plan = (Plan *) node;
	colmatstate->ss.ps.state = estate;

	colmatstate->eflags = (eflags & (EXEC_FLAG_REWIND |
									 EXEC_FLAG_BACKWARD |
									 EXEC_FLAG_MARK));

	/*
	 * Miscellaneous initialization
	 *
	 * Materialization nodes don't need ExprContexts because they never call
	 * ExecQual or ExecProject.
	 */

	/*
	 * tuple table initialization
	 *
	 * material nodes only return tuples from their materialized relation.
	 */
	ExecInitResultTupleSlot(estate, &colmatstate->ss.ps);
	ExecInitScanTupleSlot(estate, &colmatstate->ss);

	/*
	 * initialize child nodes
	 *
	 * We shield the child node from the need to support REWIND, BACKWARD, or
	 * MARK/RESTORE.
	 */
	eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

	outerPlan = outerPlan(node);
	outerPlanState(colmatstate) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * initialize tuple type.  no need to initialize projection info because
	 * this node doesn't do projections.
	 */
	ExecAssignResultTypeFromTL(&colmatstate->ss.ps);
	ExecAssignScanTypeFromOuterPlan(&colmatstate->ss);
	colmatstate->ss.ps.ps_ProjInfo = NULL;

	return colmatstate;
}

/* ----------------------------------------------------------------
 *		ExecColumnStoreEndMaterial
 * ----------------------------------------------------------------
 */
void
ExecEndColumnStoreMaterial(ColumnStoreMaterialState *node)
{
	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * shut down the subplan
	 */
	ExecEndNode(outerPlanState(node));
}

/* ----------------------------------------------------------------
 *		ExecColumnStoreMaterialMarkPos
 * ----------------------------------------------------------------
 */
void
ExecColumnStoreMaterialMarkPos(ColumnStoreMaterialState *node)
{
	Assert(node->eflags & EXEC_FLAG_MARK);

	// FIXME
}

/* ----------------------------------------------------------------
 *		ExecColumnStoreMaterialRestrPos
 * ----------------------------------------------------------------
 */
void
ExecColumnStoreMaterialRestrPos(ColumnStoreMaterialState *node)
{
	Assert(node->eflags & EXEC_FLAG_MARK);

	// FIXME
}

/* ----------------------------------------------------------------
 *		ExecColumnStoreReScanMaterial
 *
 *		Rescans the materialized relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanColumnStoreMaterial(ColumnStoreMaterialState *node)
{
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	if (node->ss.ps.lefttree->chgParam == NULL)
		ExecReScan(node->ss.ps.lefttree);

}
