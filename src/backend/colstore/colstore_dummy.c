
#include "postgres.h"

#include "colstore/colstoreapi.h"

PG_FUNCTION_INFO_V1(cstore_dummy_handler);

static void cstore_dummy_insert(Relation rel,
				Relation colstorerel, ColumnStoreInfo *info,
				int natts, Datum *values, bool *nulls,
				ItemPointer tupleid);

Datum
cstore_dummy_handler(PG_FUNCTION_ARGS)
{
        ColumnStoreRoutine *routine = makeNode(ColumnStoreRoutine);

		routine->ExecColumnStoreInsert = cstore_dummy_insert;

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
