/*-------------------------------------------------------------------------
 *
 * colstore.c
 * 		POSTGRES column store support code
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/catalog/colstore.c
 *
 * NOTES
 *	  At present, we don't bother creating pg_attribute rows for column stores.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/catalog.h"
#include "catalog/colstore.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_cstore.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"	/* DEFAULTTABLESPACE_OID */
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

typedef struct ColumnStoreElem
{
	char	   *name;
	Oid			cst_am_oid;
	Oid			tablespaceId;
	List	   *columns;	/* of AttrNumber */
} ColumnStoreElem;

static TupleDesc ColumnStoreBuildDesc(ColumnStoreElem *elem, Relation parent);
static void AddNewColstoreTuple(Oid storeId, Relation pg_cstore,
					ColumnStoreElem *entry, Oid relid);
static Oid CStoreTypeGetOid(char *cstypename);

/*
 * Figure out the set of column stores to create.
 *
 * decl_cstores is a list of column stores directly declared for the relation.
 * inh_cstores is a list of column stores inherited from parent relations.
 * We need this distinction because multiple uses of a column in declared ones
 * is an error, but we ignore duplicates for the inherited ones.
 *
 * Return value is a list of ColumnStoreInfo.
 */
List *
DetermineColumnStores(TupleDesc tupdesc, List *decl_cstores, List *inh_cstores)
{
	List	   *newstores = NIL;
	Bitmapset  *used;
	ListCell   *cell,
			   *cell2;

	/*
	 * There is no provision (not here, nor in the grammar) for a column that's
	 * part of a column store in the parent, but in the heap for the child
	 * table.  Do we need a fix for that?
	 */

	/*
	 * We use this bitmapset to keep track of columns which have already been
	 * assigned to a store.
	 */
	used = NULL;

	/*
	 * First scan the list of declared column stores.  Using columns here more
	 * than once causes an error to be raised.
	 */
	foreach(cell, decl_cstores)
	{
		ColumnStoreInfo	   *info = (ColumnStoreInfo *) lfirst(cell);
		ColumnStoreClause  *clause = info->cstoreClause;
		ColumnStoreElem	   *newstore;

		Assert(clause != NULL);
		Assert((info->attnum != InvalidAttrNumber) ||
			   (clause->columns != NIL));
		Assert(info->attnums == NIL && info->cstoreOid == InvalidOid);

		/*
		 * Verify that the name has not already been taken
		 */
		foreach(cell2, newstores)
		{
			ColumnStoreElem *elem = (ColumnStoreElem *) lfirst(cell2);

			if (strcmp(elem->name, clause->name) == 0)
				/* XXX ereport */
				elog(ERROR, "duplicate column store name \"%s\"", clause->name);
		}

		newstore = (ColumnStoreElem *) palloc(sizeof(ColumnStoreElem));
		newstore->name = clause->name;
		newstore->cst_am_oid = CStoreTypeGetOid(clause->storetype);
		newstore->tablespaceId = DEFAULTTABLESPACE_OID;	/* FIXME */

		/*
		 * Fill in the attnum list: if it's a column constraint, the only
		 * attnum was already determined by BuildDescForRelation, otherwise we
		 * need to resolve column names to attnums using the tuple descriptor.
		 */
		if (info->attnum != InvalidAttrNumber)
			newstore->columns = list_make1_int(info->attnum);
		else
		{
			foreach(cell2, clause->columns)
			{
				char	   *colname = strVal(lfirst(cell2));
				AttrNumber	attnum,
							i;

				attnum = InvalidAttrNumber;
				for (i = 1; i <= tupdesc->natts; i++)
				{
					if (namestrcmp(&(tupdesc->attrs[i - 1]->attname), colname) == 0)
						attnum = i;
				}
				/* XXX ereport */
				if (attnum == InvalidAttrNumber)
					elog(ERROR, "no column \"%s\" in the table", colname);

				newstore->columns = lappend_int(newstore->columns, attnum);
			}
		}

		/* Make sure there are no columns specified in multiple stores */
		foreach(cell2, newstore->columns)
		{
			AttrNumber	attno = lfirst_int(cell2);

			/* XXX ereport */
			if (bms_is_member(attno, used))
				elog(ERROR, "column already in a store");

			used = bms_add_member(used, attno);
		}

		newstores = lappend(newstores, newstore);
	}

	/*
	 * Now process the list of column stores coming from parent relations.
	 * Columns that are already used by previous stores are silently ignored.
	 * (In particular, this means that some parent stores might not exist at
	 * all in the child).
	 */
	foreach (cell, inh_cstores)
	{
		ColumnStoreInfo *info = (ColumnStoreInfo *) lfirst(cell);
		Relation		parentstore;
		List		   *attnums = NIL;

		Assert((info->attnum == InvalidAttrNumber) &&
			   (info->cstoreClause == NULL));
		Assert((info->attnums != NIL) &&
			   (info->cstoreOid != InvalidOid));

		parentstore = relation_open(info->cstoreOid, AccessShareLock);

		/*
		 * Examine the column list first.  If all columns are used in
		 * previously defined column stores, we can ignore this one.
		 */
		foreach(cell2, info->attnums)
		{
			AttrNumber	attnum = lfirst_int(cell2);

			if (bms_is_member(attnum, used))
				continue;
			attnums = lappend_int(attnums, attnum);
			used = bms_add_member(used, attnum);
		}

		/* If we ended up with a nonempty list, add this store to the list */
		if (attnums != NIL)
		{
			ColumnStoreElem *newstore;
			newstore = (ColumnStoreElem *) palloc(sizeof(ColumnStoreElem));

			newstore->name = pstrdup(RelationGetRelationName(parentstore));
			newstore->cst_am_oid = parentstore->rd_cstore->cststoreid;
			newstore->tablespaceId = parentstore->rd_rel->reltablespace;
			newstore->columns = attnums;

			newstores = lappend(newstores, newstore);
		}

		relation_close(parentstore, AccessShareLock);
	}

	/*
	 * Return the info we collected.
	 */
	return newstores;
}

/*
 * Create the column stores for the given table.  This creates the files
 * assigned as relfilenode for each column store; also, the pg_class and
 * pg_cstore catalog entries are created.
 */
void
CreateColumnStores(Relation rel, List *colstores)
{
	Relation	pg_class;
	Relation	pg_cstore;
	ListCell   *cell;

	if (colstores == NIL)
		return;

	pg_class = heap_open(RelationRelationId, RowExclusiveLock);
	pg_cstore = heap_open(CStoreRelationId, RowExclusiveLock);

	foreach(cell, colstores)
	{
		ColumnStoreElem *elem = (ColumnStoreElem *) lfirst(cell);
		Relation	store;
		Oid			newStoreId;
		TupleDesc	storedesc = ColumnStoreBuildDesc(elem, rel);

		/*
		 * Get the OID for the new column store.
		 */
		newStoreId = GetNewRelFileNode(elem->tablespaceId, pg_class,
									   rel->rd_rel->relpersistence);

		/*
		 * Create the relcache entry for the store.  This also creates the
		 * underlying storage; it's smgr's responsibility to remove the file if
		 * we fail later on.
		 */
		store =
			heap_create(elem->name,
						PG_COLSTORE_NAMESPACE,
						elem->tablespaceId,
						newStoreId,
						InvalidOid,
						storedesc,
						RELKIND_COLUMN_STORE,
						rel->rd_rel->relpersistence,
						false,
						false,
						allowSystemTableMods);

		/* insert pg_attribute tuples */
		AddNewAttributeTuples(newStoreId,
							  storedesc,
							  RELKIND_COLUMN_STORE,
							  false,
							  0);

		/*
		 * Insert the pg_class tuple
		 * XXX can we use InsertPgClassTuple instead?
		 */
		AddNewRelationTuple(pg_class, store,
							newStoreId,
							InvalidOid,	/* new_type_oid */
							InvalidOid,	/* reloftype */
							InvalidOid,	/* owner */
							RELKIND_COLUMN_STORE,
							(Datum) 0, /* relacl */
							(Datum) 0 /* reloptions */);

		/* And finally insert the pg_cstore tuple, and we're done */
		AddNewColstoreTuple(newStoreId, pg_cstore, elem,
							RelationGetRelid(rel));

		heap_close(store, NoLock);
	}

	heap_close(pg_class, RowExclusiveLock);
	heap_close(pg_cstore, RowExclusiveLock);
}

/*
 * Build a tuple descriptor for a not-yet-catalogued column store.
 */
static TupleDesc
ColumnStoreBuildDesc(ColumnStoreElem *elem, Relation parent)
{
	TupleDesc	tupdesc;
	TupleDesc	parentdesc;
	ListCell   *cell;
	AttrNumber	attnum = 1;

	parentdesc = RelationGetDescr(parent);

	tupdesc = CreateTemplateTupleDesc(list_length(elem->columns), false);

	foreach(cell, elem->columns)
	{
		AttrNumber	parentattnum = lfirst_int(cell);

		TupleDescInitEntry(tupdesc,
						   attnum++,
						   NameStr(parentdesc->attrs[parentattnum - 1]->attname),
						   parentdesc->attrs[parentattnum - 1]->atttypid,
						   parentdesc->attrs[parentattnum - 1]->atttypmod,
						   parentdesc->attrs[parentattnum - 1]->attndims);
	}

	return tupdesc;
}

/*
 * Return a list of column store definitions for an existing table
 */
List *
CloneColumnStores(Relation rel)
{
	/* FIXME fill this in */
	return NIL;
}

/*
 * Add a new pg_cstore tuple for a column store
 */
static void
AddNewColstoreTuple(Oid storeId, Relation pg_cstore, ColumnStoreElem *entry,
					Oid relid)
{
	HeapTuple	newtup;
	ListCell   *cell;
	Datum		values[Natts_pg_cstore];
	bool		nulls[Natts_pg_cstore];
	int			natts;
	int16	   *attrarr;
	int2vector *attrs;
	int			i = 0;

	/* build the int2vector of attribute numbers */
	natts = list_length(entry->columns);
	Assert(natts > 0);
	attrarr = palloc(sizeof(int16 *) * natts);
	foreach(cell, entry->columns)
		attrarr[i++] = (AttrNumber) lfirst_int(cell);
	attrs = buildint2vector(attrarr, natts);

	/* build the pg_cstore tuple */
	values[Anum_pg_cstore_cststoreid - 1] = ObjectIdGetDatum(entry->cst_am_oid);
	values[Anum_pg_cstore_cstrelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_cstore_cstnatts - 1] = Int32GetDatum(list_length(entry->columns));
	values[Anum_pg_cstore_cstatts - 1] = PointerGetDatum(attrs);
	memset(nulls, 0, sizeof(nulls));
	newtup = heap_form_tuple(RelationGetDescr(pg_cstore), values, nulls);

	HeapTupleSetOid(newtup, storeId);

	/* insert it into pg_cstore */
	simple_heap_insert(pg_cstore, newtup);

	/* keep indexes current */
	CatalogUpdateIndexes(pg_cstore, newtup);

	heap_freetuple(newtup);
}

static Oid
CStoreTypeGetOid(char *cstypename)
{
	if (strncmp(cstypename, "foo", NAMEDATALEN) != 0)
		elog(ERROR, "column store type \"%s\" not recognized", cstypename);

	/* FIXME look up pg_cstore_type */
	return 1667;
}
