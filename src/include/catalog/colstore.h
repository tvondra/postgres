#ifndef COLSTORE_H
#define COLSTORE_H

#include "access/tupdesc.h"
#include "nodes/pg_list.h"

extern void generateColumnStores(List *colstores, List *schema,
					 TupleDesc tupdesc);

#endif		/* COLSTORE_H */
