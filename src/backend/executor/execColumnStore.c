/*-------------------------------------------------------------------------
 *
 * execColumnStore.c
 *	  routines for inserting tuples into column stores.
 *
 * ExecInsertColStoreTuples() is the main entry point.  It's called after
 * inserting a tuple to the heap, and it inserts corresponding values
 * into all column stores.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execColumnStore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "catalog/colstore.h"
#include "colstore/colstoreapi.h"
#include "executor/executor.h"
#include "nodes/nodeFuncs.h"
#include "storage/lmgr.h"
#include "utils/tqual.h"

/* ----------------------------------------------------------------
 *		ExecOpenColumnStores
 *
 *		Find the column stores associated with a result relation, open them,
 *		and save information about them in the result ResultRelInfo.
 *
 *		At entry, caller has already opened and locked
 *		resultRelInfo->ri_RelationDesc.
 * ----------------------------------------------------------------
 */
void
ExecOpenColumnStores(ResultRelInfo *resultRelInfo)
{
	Relation	resultRelation = resultRelInfo->ri_RelationDesc;
	List	   *colstoreoidlist;
	ListCell   *l;
	int			len,
				i;
	RelationPtr relationDescs;
	ColumnStoreInfo **columnStoreInfoArray;

	resultRelInfo->ri_NumColumnStores = 0;

	/* fast path if no column stores */
	if (!RelationGetForm(resultRelation)->relhascstore)
		return;

	/*
	 * Get cached list of colstore OIDs
	 */
	colstoreoidlist = RelationGetColStoreList(resultRelation);
	len = list_length(colstoreoidlist);
	if (len == 0)
		return;

	/*
	 * allocate space for result arrays
	 */
	relationDescs = (RelationPtr) palloc(len * sizeof(Relation));
	columnStoreInfoArray
		= (ColumnStoreInfo **) palloc(len * sizeof(ColumnStoreInfo *));

	resultRelInfo->ri_NumColumnStores = len;
	resultRelInfo->ri_ColumnStoreRelationDescs = relationDescs;
	resultRelInfo->ri_ColumnStoreRelationInfo = columnStoreInfoArray;

	/*
	 * For each column store, open the column store relation and save pg_cstore
	 * info. We acquire RowExclusiveLock, signifying we will update the column
	 * store.
	 */
	i = 0;
	foreach(l, colstoreoidlist)
	{
		Oid			cstoreOid = lfirst_oid(l);
		Relation	cstoreDesc;
		ColumnStoreInfo  *csi;

		cstoreDesc = relation_open(cstoreOid, RowExclusiveLock);

		/* extract column store information from the pg_cstore info */
		csi = BuildColumnStoreInfo(cstoreDesc);

		relationDescs[i] = cstoreDesc;
		columnStoreInfoArray[i] = csi;
		i++;
	}

	list_free(colstoreoidlist);
}

/* ----------------------------------------------------------------
 *		ExecCloseColumnStores
 *
 *		Close the column store relations stored in resultRelInfo
 * ----------------------------------------------------------------
 */
void
ExecCloseColumnStores(ResultRelInfo *resultRelInfo)
{
	int			i;
	int			numColumnStores;
	RelationPtr cstoreDescs;

	numColumnStores = resultRelInfo->ri_NumColumnStores;
	cstoreDescs = resultRelInfo->ri_ColumnStoreRelationDescs;

	for (i = 0; i < numColumnStores; i++)
	{
		if (cstoreDescs[i] == NULL)
			continue;			/* shouldn't happen? */

		/* Drop lock acquired by ExecOpenColumnStores */
		relation_close(cstoreDescs[i], RowExclusiveLock);
	}

	/*
	 * XXX should free ColumnStoreInfo array here too? Currently we assume that
	 * such stuff will be cleaned up automatically in FreeExecutorState.
	 */
}

/* ----------------------------------------------------------------
 *		ExecInsertColStoreTuples
 *
 *		This routine takes care of inserting column store tuples
 *		into all the relations vertically partitioning the result relation
 *		when a heap tuple is inserted into the result relation.
 *
 *		CAUTION: this must not be called for a HOT update.
 *		We can't defend against that here for lack of info.
 *		Should we change the API to make it safer?
 * ----------------------------------------------------------------
 */
void
ExecInsertColStoreTuples(HeapTuple tuple, EState *estate)
{
	ResultRelInfo *resultRelInfo;
	int			i;
	int			numColumnStores;
	RelationPtr relationDescs;
	Relation	heapRelation;
	ColumnStoreInfo **columnStoreInfoArray;
	Datum		values[INDEX_MAX_KEYS];	/* FIXME INDEX_MAX_KEYS=32 seems a bit low */
	bool		isnull[INDEX_MAX_KEYS];
	ItemPointer	tupleid = &(tuple->t_self);

	/*
	 * Get information from the result relation info structure.
	 */
	resultRelInfo = estate->es_result_relation_info;
	numColumnStores = resultRelInfo->ri_NumColumnStores;
	relationDescs = resultRelInfo->ri_ColumnStoreRelationDescs;
	columnStoreInfoArray = resultRelInfo->ri_ColumnStoreRelationInfo;
	heapRelation = resultRelInfo->ri_RelationDesc;

	/*
	 * for each column store, form and insert the tuple
	 */
	for (i = 0; i < numColumnStores; i++)
	{
		Relation	cstoreRelation = relationDescs[i];
		ColumnStoreInfo  *cstoreInfo;

		if (cstoreRelation == NULL)	/* XXX seems a bit strange ... */
			continue;

		cstoreInfo = columnStoreInfoArray[i];

		if (cstoreInfo->csi_ColumnStoreRoutine == NULL)
			elog(ERROR, "column store routine not available");

		if (cstoreInfo->csi_ColumnStoreRoutine->ExecColumnStoreInsert == NULL)
			elog(ERROR, "ExecColumnStoreInsert routine not available");

		/*
		 * FormColumnStoreDatum fills in its values and isnull parameters with
		 * the appropriate values for the column(s) of the column store.
		 */
		FormColumnStoreDatum(cstoreInfo,
					   tuple,
					   values,
					   isnull);

		cstoreInfo->csi_ColumnStoreRoutine->ExecColumnStoreInsert(
			heapRelation,	/* heap relation */
			cstoreRelation, /* column store relation */
			cstoreInfo,		/* column store info */
			cstoreInfo->csi_NumColumnStoreAttrs,
			values,			/* array of column store Datums */
			isnull,			/* null flags */
			tupleid);		/* tid of heap tuple */

	}

	return result;
}
