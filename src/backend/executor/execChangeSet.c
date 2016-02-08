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
#include "catalog/changeset.h"
#include "executor/executor.h"


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
ExecInsertChangeSetTuples(TupleTableSlot *slot, EState *estate)
{
	elog(WARNING, "inserting tuple into changeset");
	return;
}
