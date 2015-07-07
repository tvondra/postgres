#ifndef COLSTORE_H
#define COLSTORE_H

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/relcache.h"

/*
 * When creating a table with column store declarations, this struct
 * carries the necessary info.
 *
 * We're catering for several different cases here: (a) column store
 * declarations as column constraints (attnum and cstoreClause are both set,
 * attnums and cstoreOid are invalid), (b) declarations as table constraint
 * (attnum is invalid but cstoreClause is set; attnums and cstoreOid are
 * invalid), and (c) store definitions inherited from parent relations (attnum
 * and cstoreClause are both invalid, attnums list the attribute numbers and
 * cstoreOid is the OID of the pg_cstore entry of the column store for the
 * parent relation.  Note that the cstatts data from the parent's entry must be
 * ignored in favor of the attnum list given here.)
 */
typedef struct ColumnStoreInfo
{
	AttrNumber	attnum;
	ColumnStoreClause *cstoreClause;
	List	   *attnums;
	Oid			cstoreOid;
} ColumnStoreInfo;


extern List *DetermineColumnStores(TupleDesc tupdesc, List *decl_cstores,
					  List *inh_cstores);
extern void CreateColumnStores(Relation rel, List *colstores);
extern List *CloneColumnStores(Relation rel);
extern Oid get_relation_cstore_oid(Oid relid, const char *cstore_name,
						bool missing_ok);
extern void RemoveColstoreById(Oid cstoreOid);

#endif		/* COLSTORE_H */
