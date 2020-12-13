/*-------------------------------------------------------------------------
 *
 * execChangeSet.c
 *	  routines for inserting changeset tuples and enforcing unique and
 *	  exclusive constraints.
 *
 * ExecInsertChangeSetTuples() is the main entry point.  It's called after
 * inserting a tuple to the heap, and it inserts corresponding tuples
 * into all changesets.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execChangeSet.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/xact.h"
#include "catalog/changeset.h"
#include "executor/executor.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

static HeapTuple GetTupleForChangeSet(EState *estate,
					 EPQState *epqstate,
					 ResultRelInfo *relinfo,
					 ItemPointer tid,
					 LockTupleMode lockmode,
					 TupleTableSlot **newSlot);
					 
/* ----------------------------------------------------------------
 *		ExecOpenChangeSets
 *
 *		Find the indices associated with a result relation, open them,
 *		and save information about them in the result ResultRelInfo.
 *
 *		At entry, caller has already opened and locked
 *		resultRelInfo->ri_RelationDesc.
 * ----------------------------------------------------------------
 */
void
ExecOpenChangeSets(ResultRelInfo *resultRelInfo)
{
	Relation	resultRelation = resultRelInfo->ri_RelationDesc;
	List	   *chsetoidlist;
	ListCell   *l;
	int			len,
				i;
	RelationPtr relationDescs;
	ChangeSetInfo **chsetInfoArray;

	resultRelInfo->ri_NumChangeSets = 0;

	/* FIXME fast path if no indexes */
	// if (!RelationGetForm(resultRelation)->relhasindex)
	//	return;

	/*
	 * Get cached list of index OIDs
	 */
	chsetoidlist = RelationGetChangeSetList(resultRelation);
	len = list_length(chsetoidlist);
	if (len == 0)
		return;

	/*
	 * allocate space for result arrays
	 */
	relationDescs = (RelationPtr) palloc(len * sizeof(Relation));
	chsetInfoArray = (ChangeSetInfo **) palloc(len * sizeof(ChangeSetInfo *));

	resultRelInfo->ri_NumChangeSets = len;
	resultRelInfo->ri_ChangeSetRelationDescs = relationDescs;
	resultRelInfo->ri_ChangeSetRelationInfo = chsetInfoArray;

	/*
	 * For each changeset, open the changeset relation and save pg_changeset
	 * info. We acquire RowExclusiveLock, signifying we will update the changeset.
	 */
	i = 0;
	foreach(l, chsetoidlist)
	{
		Oid			chsetOid = lfirst_oid(l);
		Relation	chsetDesc;
		ChangeSetInfo  *chi;

		chsetDesc = relation_open(chsetOid, RowExclusiveLock);

		/* extract changeset key information from the changeset's pg_changeset info */
		chi = BuildChangeSetInfo(chsetDesc);

		relationDescs[i] = chsetDesc;
		chsetInfoArray[i] = chi;
		i++;
	}

	list_free(chsetoidlist);
}

/* ----------------------------------------------------------------
 *		ExecCloseChangeSets
 *
 *		Close the index relations stored in resultRelInfo
 * ----------------------------------------------------------------
 */
void
ExecCloseChangeSets(ResultRelInfo *resultRelInfo)
{
	int			i;
	int			numChangeSets;
	RelationPtr chsetDescs;

	numChangeSets = resultRelInfo->ri_NumChangeSets;
	chsetDescs = resultRelInfo->ri_ChangeSetRelationDescs;

	for (i = 0; i < numChangeSets; i++)
	{
		if (chsetDescs[i] == NULL)
			continue;			/* shouldn't happen? */

		/* Drop lock acquired by ExecOpenChangeSets */
		relation_close(chsetDescs[i], RowExclusiveLock);
	}

	/*
	 * XXX should free indexInfo array here too?  Currently we assume that
	 * such stuff will be cleaned up automatically in FreeExecutorState.
	 */
}

/* ----------------------------------------------------------------
 *		ExecInsertChangeSetTuples
 *
 *		This routine takes care of inserting tuples into all the changesets
 *		when a heap tuple is inserted into the result relation.
 * ----------------------------------------------------------------
 */
void
ExecInsertChangeSetTuples(char changeType, TupleTableSlot *slot, EState *estate)
{
	ResultRelInfo *resultRelInfo;
	int			i;
	int			numChangeSets;
	RelationPtr relationDescs;
	ChangeSetInfo **chsetInfoArray;

	/*
	 * Get information from the result relation info structure.
	 */
	resultRelInfo = estate->es_result_relation_info;
	numChangeSets = resultRelInfo->ri_NumChangeSets;
	relationDescs = resultRelInfo->ri_ChangeSetRelationDescs;
	chsetInfoArray = resultRelInfo->ri_ChangeSetRelationInfo;

	/*
	 * for each changeset, form and insert the heap tuple
	 */
	for (i = 0; i < numChangeSets; i++)
	{
		Relation	chsetRelation = relationDescs[i];
		ChangeSetInfo  *chsetInfo;
		TupleDesc	tdesc;
		HeapTuple	htup;
		Datum	   *values;
		bool	   *isnull;
		int			natts;

		/* FIXME why do we need this (see ExecInsertIndexTuples)? */
		if (chsetRelation == NULL)
			continue;

		chsetInfo = chsetInfoArray[i];
		tdesc = RelationGetDescr(chsetRelation);

		/*
		 * allocate arrays for the tuple values
		 *
		 * FIXME allocate the arrays only once for all changesets, using the
		 *       largest number of attributes necessary.
		 */
		natts = tdesc->natts;
		values = palloc0(natts * sizeof(Datum));
		isnull = palloc0(natts * sizeof(bool));

		/*
		 * FormChangeSetDatum fills in its values and isnull parameters with
		 * the appropriate values for the column(s) of the changeset.
		 */
		FormChangeSetDatum(chsetInfo,
						   changeType,
						   slot,
						   values,
						   isnull);

		htup = heap_form_tuple(tdesc, values, isnull);

		/* do the actual insert */
		heap_insert(chsetRelation, htup, GetCurrentCommandId(true), 0, NULL);

		pfree(values);
		pfree(isnull);
	}
}


/* ----------------------------------------------------------------
 *		ExecInsertChangeSetTuples2
 *
 *		This routine takes care of inserting tuples into all the changesets
 *		when a heap tuple is deleted/updated in the result relation.
 * ----------------------------------------------------------------
 */
void
ExecInsertChangeSetTuples2(char changeType,
						   ItemPointer tupleid, HeapTuple tup,
						   EState *estate)
{
	ResultRelInfo *resultRelInfo;
	int			i;
	int			numChangeSets;
	RelationPtr relationDescs;
	ChangeSetInfo **chsetInfoArray;
	TupleDesc	rdesc;

	/*
	 * Get information from the result relation info structure.
	 */
	resultRelInfo = estate->es_result_relation_info;
	rdesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);
	numChangeSets = resultRelInfo->ri_NumChangeSets;
	relationDescs = resultRelInfo->ri_ChangeSetRelationDescs;
	chsetInfoArray = resultRelInfo->ri_ChangeSetRelationInfo;

	/* fetch the tuple using the item pointer if needed */
	if (tup == NULL)
		tup = GetTupleForChangeSet(estate,
								   NULL,
								   resultRelInfo,
								   tupleid,
								   LockTupleExclusive,
								   NULL);

	/*
	 * for each changeset, form and insert the heap tuple
	 */
	for (i = 0; i < numChangeSets; i++)
	{
		Relation	chsetRelation = relationDescs[i];
		ChangeSetInfo  *chsetInfo;
		TupleDesc	tdesc;
		HeapTuple	htup;
		Datum	   *values;
		bool	   *isnull;
		int			natts;

		/* FIXME why do we need this (see ExecInsertIndexTuples)? */
		if (chsetRelation == NULL)
			continue;

		chsetInfo = chsetInfoArray[i];
		tdesc = RelationGetDescr(chsetRelation);

		/*
		 * allocate arrays for the tuple values
		 *
		 * FIXME allocate the arrays only once for all changesets, using the
		 *       largest number of attributes necessary.
		 */
		natts = tdesc->natts;
		values = palloc0(natts * sizeof(Datum));
		isnull = palloc0(natts * sizeof(bool));

		/*
		 * FormChangeSetDatum fills in its values and isnull parameters with
		 * the appropriate values for the column(s) of the changeset.
		 */
		FormChangeSetDatum2(chsetInfo,
						   changeType,
						   rdesc, tup,
						   values,
						   isnull);

		htup = heap_form_tuple(tdesc, values, isnull);

		/* do the actual insert */
		heap_insert(chsetRelation, htup, GetCurrentCommandId(true), 0, NULL);

		pfree(values);
		pfree(isnull);
	}
}

/* copy-paste of GetTupleForTrigger (trigger.c) */
static HeapTuple
GetTupleForChangeSet(EState *estate,
					 EPQState *epqstate,
					 ResultRelInfo *relinfo,
					 ItemPointer tid,
					 LockTupleMode lockmode,
					 TupleTableSlot **newSlot)
{
	Relation	relation = relinfo->ri_RelationDesc;
	HeapTupleData tuple;
	HeapTuple	result;
	Buffer		buffer;

	if (newSlot != NULL)
	{
		HTSU_Result test;
		HeapUpdateFailureData hufd;

		*newSlot = NULL;

		/* caller must pass an epqstate if EvalPlanQual is possible */
		Assert(epqstate != NULL);

		/*
		 * lock tuple for update
		 */
ltrmark:;
		tuple.t_self = *tid;
		test = heap_lock_tuple(relation, &tuple,
							   estate->es_output_cid,
							   lockmode, LockWaitBlock,
							   false, &buffer, &hufd);
		switch (test)
		{
			case HeapTupleSelfUpdated:

				/*
				 * The target tuple was already updated or deleted by the
				 * current command, or by a later command in the current
				 * transaction.  We ignore the tuple in the former case, and
				 * throw error in the latter case, for the same reasons
				 * enumerated in ExecUpdate and ExecDelete in
				 * nodeModifyTable.c.
				 */
				if (hufd.cmax != estate->es_output_cid)
					ereport(ERROR,
							(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
							 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
							 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));

				/* treat it as deleted; do not process */
				ReleaseBuffer(buffer);
				return NULL;

			case HeapTupleMayBeUpdated:
				break;

			case HeapTupleUpdated:
				ReleaseBuffer(buffer);
				if (IsolationUsesXactSnapshot())
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));
				if (!ItemPointerEquals(&hufd.ctid, &tuple.t_self))
				{
					/* it was updated, so look at the updated version */
					TupleTableSlot *epqslot;

					epqslot = EvalPlanQual(estate,
										   epqstate,
										   relation,
										   relinfo->ri_RangeTableIndex,
										   lockmode,
										   &hufd.ctid,
										   hufd.xmax);
					if (!TupIsNull(epqslot))
					{
						*tid = hufd.ctid;
						*newSlot = epqslot;

						/*
						 * EvalPlanQual already locked the tuple, but we
						 * re-call heap_lock_tuple anyway as an easy way of
						 * re-fetching the correct tuple.  Speed is hardly a
						 * criterion in this path anyhow.
						 */
						goto ltrmark;
					}
				}

				/*
				 * if tuple was deleted or PlanQual failed for updated tuple -
				 * we must not process this tuple!
				 */
				return NULL;

			case HeapTupleInvisible:
				elog(ERROR, "attempted to lock invisible tuple");

			default:
				ReleaseBuffer(buffer);
				elog(ERROR, "unrecognized heap_lock_tuple status: %u", test);
				return NULL;	/* keep compiler quiet */
		}
	}
	else
	{
		Page		page;
		ItemId		lp;

		buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));

		/*
		 * Although we already know this tuple is valid, we must lock the
		 * buffer to ensure that no one has a buffer cleanup lock; otherwise
		 * they might move the tuple while we try to copy it.  But we can
		 * release the lock before actually doing the heap_copytuple call,
		 * since holding pin is sufficient to prevent anyone from getting a
		 * cleanup lock they don't already hold.
		 */
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);
		lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));

		Assert(ItemIdIsNormal(lp));

		tuple.t_data = (HeapTupleHeader) PageGetItem(page, lp);
		tuple.t_len = ItemIdGetLength(lp);
		tuple.t_self = *tid;
		tuple.t_tableOid = RelationGetRelid(relation);

		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	}

	result = heap_copytuple(&tuple);
	ReleaseBuffer(buffer);

	return result;
}
