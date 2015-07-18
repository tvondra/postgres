
#include "postgres.h"

#include "colstore_dummy.h"
#include "colstore/colstoreapi.h"

PG_FUNCTION_INFO_V1(cstore_dummy_handler);

static void cstore_dummy_insert(Relation rel,
				Relation colstorerel, ColumnStoreInfo *info,
				int natts, Datum *values, bool *nulls,
				ItemPointer tupleid);

static void cstore_dummy_batch_insert(Relation rel,
				Relation colstorerel, ColumnStoreInfo *info,
				int nrows, int natts, Datum **values, bool **nulls,
				ItemPointer *tupleids);

Datum
cstore_dummy_handler(PG_FUNCTION_ARGS)
{
        ColumnStoreRoutine *routine = makeNode(ColumnStoreRoutine);

		routine->ExecColumnStoreInsert = cstore_dummy_insert;
		routine->ExecColumnStoreBatchInsert = cstore_dummy_batch_insert;

        PG_RETURN_POINTER(routine);
}


static void
cstore_dummy_insert(Relation rel,
				Relation colstorerel, ColumnStoreInfo *info,
				int natts, Datum *values, bool *nulls,
				ItemPointer tupleid)
{
	elog(WARNING, "colstore dummy insert");
}

static void
cstore_dummy_batch_insert(Relation rel,
				Relation colstorerel, ColumnStoreInfo *info,
				int nrows, int natts, Datum **values, bool **nulls,
				ItemPointer *tupleids)
{
	elog(WARNING, "colstore dummy batch insert (%d rows)", nrows);
}
