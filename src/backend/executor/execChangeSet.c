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

#include "access/heapam.h"
#include "access/table.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "catalog/changeset.h"
#include "executor/executor.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"


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

		chsetDesc = table_open(chsetOid, RowExclusiveLock);

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
		table_close(chsetDescs[i], RowExclusiveLock);
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
ExecInsertChangeSetTuples(EState *estate, ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot)
{
	int			i;
	int			numChangeSets;
	RelationPtr relationDescs;
	ChangeSetInfo **chsetInfoArray;
	char		changeType = CHANGESET_INSERT;

	if (resultRelInfo->ri_NumChangeSets == 0)
		return;

	/*
	 * Get information from the result relation info structure.
	 */
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

void
ExecARUpdateChangeSets(EState *estate, ResultRelInfo *relinfo,
					   ItemPointer tupleid,
					   HeapTuple fdw_trigtuple,
					   TupleTableSlot *newslot,
					   List *recheckIndexes)
{
	for (int i = 0; i < relinfo->ri_NumChangeSets; i++)
		elog(WARNING, "not implemented");
}

void
ExecARDeleteChangeSets(EState *estate, ResultRelInfo *relinfo,
					   ItemPointer tupleid,
					   HeapTuple fdw_trigtuple)
{
	for (int i = 0; i < relinfo->ri_NumChangeSets; i++)
		elog(WARNING, "not implemented");
}
