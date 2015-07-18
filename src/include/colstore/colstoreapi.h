/*-------------------------------------------------------------------------
 *
 * colstoreapi.h
 *	  API for column store implementations
 *
 * Copyright (c) 2010-2015, PostgreSQL Global Development Group
 *
 * src/include/colstore/colstoreapi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COLSTOREAPI_H
#define COLSTOREAPI_H

#include "nodes/execnodes.h"
#include "nodes/relation.h"

typedef void (*ExecColumnStoreInsert_function) (Relation rel,
				Relation colstorerel, ColumnStoreInfo *info,
				int natts, Datum *values, bool *nulls,
				ItemPointer tupleid);

typedef void (*ExecColumnStoreBatchInsert_function) (Relation rel,
				Relation colstorerel, ColumnStoreInfo *info,
				int nrows, int natts, Datum **values, bool **nulls,
				ItemPointer *tupleids);

typedef void (*ExecColumnStoreFetch_function) (Relation rel,
				Relation colstorerel, ColumnStoreInfo *info, ItemPointer tid);

typedef void (*ExecColumnStoreBatchFetch_function) (Relation rel,
				Relation colstorerel, ColumnStoreInfo *info,
				int ntids, ItemPointer *tids);

typedef void (*ExecColumnStoreDiscard_function) (Relation rel,
				Relation colstorerel, ColumnStoreInfo *info,
				int ntids, ItemPointer *tids);

typedef void (*ExecColumnStorePrune_function) (Relation rel,
				Relation colstorerel, ColumnStoreInfo *info,
				int ntids, ItemPointer *tids);

/*
 * ColumnStoreRoutine is the struct returned by a column store's handler
 * function.  It provides pointers to the callback functions needed by the
 * planner and executor.
 *
 * More function pointers are likely to be added in the future. Therefore
 * it's recommended that the handler initialize the struct with
 * makeNode(ColumnStoreRoutine) so that all fields are set to NULL. This will
 * ensure that no fields are accidentally left undefined.
 *
 * XXX Mostly a copy of FdwRoutine.
 */
typedef struct ColumnStoreRoutine
{
	NodeTag		type;

	/* insert a single row into the column store */
	ExecColumnStoreInsert_function	ExecColumnStoreInsert;

	/* insert a batch of rows into the column store */
	ExecColumnStoreBatchInsert_function	ExecColumnStoreBatchInsert;

	/* fetch values for a single row */
	ExecColumnStoreFetch_function ExecColumnStoreFetch;

	/* fetch a batch of values for a single row */
	ExecColumnStoreBatchFetch_function ExecColumnStoreBatchFetch;

	/* discard a batch of deleted rows from the column store */
	ExecColumnStoreDiscard_function	ExecColumnStoreDiscard;

	/* prune the store - keep only the valid rows */
	ExecColumnStorePrune_function	ExecColumnStorePrune;

} ColumnStoreRoutine;

extern Oid GetColumnStoreHandlerByRelId(Oid relid);
extern ColumnStoreRoutine *GetColumnStoreRoutine(Oid csthandler);
extern ColumnStoreRoutine *GetColumnStoreRoutineByRelId(Oid relid);
extern ColumnStoreRoutine *GetColumnStoreRoutineForRelation(Relation relation,
															bool makecopy);

#endif   /* COLSTOREAPI_H */
