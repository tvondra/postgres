
#include "postgres.h"

#include "colstore/colstoreapi.h"

PG_FUNCTION_INFO_V1(cstore_dummy_handler);


Datum
cstore_dummy_handler(PG_FUNCTION_ARGS)
{
        ColumnStoreRoutine *routine = makeNode(ColumnStoreRoutine);

        PG_RETURN_POINTER(routine);
}
