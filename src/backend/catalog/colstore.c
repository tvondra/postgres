#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/colstore.h"
#include "catalog/pg_cstore.h"
#include "catalog/indexing.h"
#include "nodes/bitmapset.h"
#include "nodes/parsenodes.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"

typedef struct ColumnStoreElem
{
	Oid			cstid;
	Oid			parent_cstid;
	char	   *name;
	Oid			typeoid;
	List	   *columns;
} ColumnStoreElem;

static Oid CStoreTypeGetOid(char *cstypename);

/*
 * Figure out the set of column stores to create, and create them.  Also modify
 * the tuple descriptor with the OIDs of the created stores.
 */
void
generateColumnStores(List *colstores, List *schema, TupleDesc tupdesc)
{
	ListCell   *cell,
			   *cell2;
	int			natt;
	Bitmapset  *used = NULL;
	List	   *newstores = NIL;


	Assert(list_length(schema) == tupdesc->natts);

	/*
	 * FIXME --- there is no provision in this code for overriding the stores
	 * definition in an inherited relation, which probably is a problem.  Maybe
	 * we should allow merging elements by name, so if a name is already
	 * defined locally the parent's definition is ignored?
	 */

	/*
	 * We use this bitmapset to keep track of columns which have already been
	 * assigned to a store.
	 */
	used = NULL;

	/*
	 * First, scan the list of table-constraint-level column store
	 * declarations.  These can be multi-column.
	 */
	foreach(cell, colstores)
	{
		ColumnStoreClause  *store = (ColumnStoreClause *) lfirst(cell);
		List	   *stcols = NIL;
		ColumnStoreElem *newstore;

		/*
		 * Verify that the name has not already been taken
		 */
		foreach(cell2, newstores)
		{
			ColumnStoreElem *elem = (ColumnStoreElem *) lfirst(cell2);

			if (strcmp(elem->name, store->name) == 0)
				elog(ERROR, "duplicate column store name \"%s\"", elem->name);
		}

		foreach(cell2, store->columns)
		{
			char	   *colname = strVal(lfirst(cell2));
			AttrNumber	attnum,
						i;

			attnum = InvalidAttrNumber;
			for (i = 1; i <= tupdesc->natts; i++)
			{
				if (strcmp(NameStr(tupdesc->attrs[i - 1]->attname), colname) == 0)
					attnum = i;
			}
			if (attnum == InvalidAttrNumber)
				elog(ERROR, "no column \"%s\" in the table", colname);

			if (bms_is_member(attnum, used))
				elog(ERROR, "column already in a store");

			used = bms_add_member(used, attnum);
			stcols = lappend_int(stcols, attnum);
		}

		newstore = (ColumnStoreElem *) palloc(sizeof(ColumnStoreElem));
		newstore->name = store->name;
		newstore->typeoid = CStoreTypeGetOid(store->storetype);
		newstore->columns = stcols;

		newstores = lappend(newstores, newstore);
	}

	natt = 1;
	foreach(cell, schema)
	{
		ColumnDef  *def = (ColumnDef *) lfirst(cell);
		Form_pg_attribute att = tupdesc->attrs[natt - 1];
		ColumnStoreElem *newstore;

		natt++;

		if (def->cstoreClause == NULL && !OidIsValid(def->cstoreOid))
			continue;

		if (def->cstoreOid)
		{
			Relation	pg_cstore;
			SysScanDesc	scan;
			ScanKeyData	key;
			HeapTuple	tuple;
			Form_pg_cstore cstform;
			bool		found = false;
			int			i;

			/*
			 * If this inherited column store was already used in a previous
			 * column, we ignore it for this one.
			 */
			foreach(cell2, newstores)
			{
				ColumnStoreElem *prevstore = (ColumnStoreElem *) lfirst(cell2);

				if (prevstore->parent_cstid == def->cstoreOid)
				{
					found = true;
					break;
				}
			}
			if (found)
				continue;

			/*
			 * Determine the definition of the parent's store, and construct
			 * ours identically.
			 */
			ScanKeyInit(&key, ObjectIdAttributeNumber,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(def->cstoreOid));
			/* XXX probably keep it open ...? */
			pg_cstore = heap_open(CStoreRelationId, AccessShareLock);
			scan = systable_beginscan(pg_cstore,
									  CStoreStoreOidIndexId,
									  true, NULL, 1, &key);
			tuple = systable_getnext(scan);
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "could not find parent's cstore");
			cstform = (Form_pg_cstore) GETSTRUCT(tuple);

			/*
			 * Verify that the name has not already been taken
			 */
			foreach(cell2, newstores)
			{
				ColumnStoreElem *elem = (ColumnStoreElem *) lfirst(cell2);

				if (strncmp(elem->name, NameStr(cstform->cstname), NAMEDATALEN) == 0)
					elog(ERROR, "duplicate column store name \"%s\"", elem->name);
			}

			newstore = (ColumnStoreElem *) palloc(sizeof(ColumnStoreElem));
			newstore->name = pstrdup(NameStr(cstform->cstname));
			newstore->typeoid = cstform->cststoreid;
			newstore->parent_cstid = def->cstoreOid;

			/*
			 * Form the attnum list and we're done.  Since the attnums in the
			 * child relation doesn't necessarily match the parent's, we need
			 * to get the parent's column name and match that to the columns
			 * being defined.
			 */
			for (i = 0; i < cstform->cstnatts; i++)
			{
				AttrNumber	attnum;
				char	   *colname = get_attname(cstform->cstrelid,
												  cstform->cstatts.values[i]);

				attnum = InvalidAttrNumber;
				for (i = 1; i <= tupdesc->natts; i++)
				{
					if (strcmp(NameStr(tupdesc->attrs[i - 1]->attname), colname) == 0)
						attnum = i;
				}
				if (attnum == InvalidAttrNumber)
					elog(ERROR, "no column \"%s\" in the table", colname);
				pfree(colname);

				/* If column already used, boom */
				if (bms_is_member(attnum, used))
					elog(ERROR, "column already in a store");
				used = bms_add_member(used, attnum);

				newstore->columns = lappend_int(newstore->columns, attnum);
			}

			systable_endscan(scan);
			heap_close(pg_cstore, AccessShareLock);
		}
		else
		{
			/*
			 * This is a standalone single-column definition, not inherited.
			 * Verify that the name has not already been taken.
			 */
			foreach(cell2, newstores)
			{
				ColumnStoreElem *elem = (ColumnStoreElem *) lfirst(cell2);

				if (strcmp(elem->name, def->cstoreClause->name) == 0)
					elog(ERROR, "duplicate column store name \"%s\"", elem->name);
			}

			/* Make sure the column is not already used by some other store */
			if (bms_is_member(att->attnum, used))
				elog(ERROR, "column already in a store");
			used = bms_add_member(used, att->attnum);

			newstore = (ColumnStoreElem *) palloc(sizeof(ColumnStoreElem));
			newstore->name = def->cstoreClause->name;
			newstore->typeoid = CStoreTypeGetOid(def->cstoreClause->storetype);
			newstore->columns = list_make1_oid(att->attnum);
		}

		newstores = lappend(newstores, newstore);
	}

	foreach(cell, newstores)
	{
		ColumnStoreElem *elem = (ColumnStoreElem *) lfirst(cell);
		StringInfoData	str;

		initStringInfo(&str);
		foreach(cell2, elem->columns)
			appendStringInfo(&str, "%d ", lfirst_oid(cell2));

		elog(NOTICE, "column store: name %s type %u columns %s",
			 elem->name, elem->typeoid, str.data);
	}
}


static Oid
CStoreTypeGetOid(char *cstypename)
{
	if (strncmp(cstypename, "foo", NAMEDATALEN) != 0)
		elog(ERROR, "column store type \"%s\" not recognized", cstypename);

	/* FIXME look up pg_cstore_type */
	return 1667;
}
