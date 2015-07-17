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
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_cstore.h"
#include "catalog/pg_cstore_am.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"	/* GLOBALTABLESPACE_OID */
#include "colstore/colstoreapi.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/parsenodes.h"
#include "nodes/execnodes.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/rel.h"

typedef struct ColumnStoreElem
{
	char	   *name;
	Oid			cst_am_oid;
	Oid			tablespaceId;
	List	   *columns;	/* of AttrNumber */
} ColumnStoreElem;

static TupleDesc ColumnStoreBuildDesc(ColumnStoreElem *elem, Relation parent);
static void AddNewColstoreTuple(Relation pg_cstore, ColumnStoreElem *entry,
					Oid storeId, Oid relid);
static Oid CStoreAMGetOid(char *cstypename);

/*
 * Figure out the set of column stores to create.
 *
 * decl_cstores is a list of column stores directly declared for the relation.
 * inh_cstores is a list of column stores inherited from parent relations.
 * We need this distinction because multiple uses of a column in declared ones
 * is an error, but we ignore duplicates for the inherited ones.
 *
 * Return value is a list of ColumnStoreClauseInfo.
 */
List *
DetermineColumnStores(TupleDesc tupdesc, List *decl_cstores,
					  List *inh_cstores, Oid tablespaceId)
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
		ColumnStoreClauseInfo	   *info = (ColumnStoreClauseInfo *) lfirst(cell);
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
		newstore->cst_am_oid = CStoreAMGetOid(clause->storetype);
		
		/*
		 * Select tablespace to use. If not specified, use the tablespace
		 * of the parent relation.
		 *
		 * These are effectively the same checks as in DefineRelation.
		 */
		if (clause->tablespacename)
		{
			newstore->tablespaceId
				= get_tablespace_oid(clause->tablespacename, false);
		}
		else
			/* use tablespace of the relation */
			newstore->tablespaceId = tablespaceId;

		/* Check permissions except when using database's default */
		if (OidIsValid(newstore->tablespaceId) &&
			newstore->tablespaceId != MyDatabaseTableSpace)
		{
			AclResult	aclresult;

			aclresult = pg_tablespace_aclcheck(newstore->tablespaceId,
											   GetUserId(), ACL_CREATE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, ACL_KIND_TABLESPACE,
							   get_tablespace_name(newstore->tablespaceId));
		}

		/* In all cases disallow placing user relations in pg_global */
		if (newstore->tablespaceId == GLOBALTABLESPACE_OID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("only shared relations can be placed in pg_global tablespace")));

		/*
		 * Fill in the attnum list: if it's a column constraint, the only
		 * attnum was already determined by BuildDescForRelation, otherwise we
		 * need to resolve column names to attnums using the tuple descriptor.
		 */
		if (info->attnum != InvalidAttrNumber)
			newstore->columns = list_make1_int(info->attnum);
		else
		{
			newstore->columns = NIL;

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
		ColumnStoreClauseInfo *info = (ColumnStoreClauseInfo *) lfirst(cell);
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

			newstore->name = pstrdup(NameStr(parentstore->rd_cstore->cstname));
			newstore->cst_am_oid = parentstore->rd_cstore->cststoreid;
			newstore->tablespaceId = parentstore->rd_rel->reltablespace;
			newstore->columns = attnums;

			newstores = lappend(newstores, newstore);
		}

		relation_close(parentstore, AccessShareLock);
	}

	/*
	 * Check that the names of the column stores are unique, so that we can
	 * print a nice error message instead of a confusing unique violation
	 * later. This is O(N^2), but that should not be a problem.
	 *
	 * XXX We don't have relname here, so we can't put it to the message.
	 */
	foreach (cell, newstores)
	{
		ListCell * cell2;
		ColumnStoreElem *elem1 = (ColumnStoreElem *) lfirst(cell);
		
		foreach (cell2, newstores)
		{
			ColumnStoreElem *elem2 = (ColumnStoreElem *) lfirst(cell2);

			if (elem1 == elem2)	/* skip the same element */
				continue;

			/* same names */
			if (strcmp(elem1->name, elem2->name) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
				errmsg("column store \"%s\" already exists", elem1->name)));

		}
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
	int			storenum = 1;
	ObjectAddress parentrel;

	if (colstores == NIL)
		return;

	pg_class = heap_open(RelationRelationId, RowExclusiveLock);
	pg_cstore = heap_open(CStoreRelationId, RowExclusiveLock);

	ObjectAddressSet(parentrel, RelationRelationId,
					 RelationGetRelid(rel));

	foreach(cell, colstores)
	{
		ColumnStoreElem *elem = (ColumnStoreElem *) lfirst(cell);
		Relation	store;
		Oid			newStoreId;
		TupleDesc	storedesc = ColumnStoreBuildDesc(elem, rel);
		char	   *classname;
		ObjectAddress	myself;

		/*
		 * Get the OID for the new column store.
		 */
		newStoreId = GetNewRelFileNode(elem->tablespaceId, pg_class,
									   rel->rd_rel->relpersistence);

		classname = psprintf("pg_colstore_%u_%d",
							 RelationGetRelid(rel), storenum++);

		/*
		 * Create the relcache entry for the store.  This also creates the
		 * underlying storage; it's smgr's responsibility to remove the file if
		 * we fail later on.
		 */
		store =
			heap_create(classname,
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
		 */
		store->rd_rel->relam = elem->cst_am_oid;
		InsertPgClassTuple(pg_class, store, newStoreId,
						   (Datum) 0, (Datum) 0);

		/* And finally insert the pg_cstore tuple */
		AddNewColstoreTuple(pg_cstore, elem, newStoreId,
							RelationGetRelid(rel));

		/*
		 * This is a good place to record dependencies.  We choose to have all the
		 * subsidiary entries (both pg_class and pg_cstore entries) depend on the
		 * pg_class entry for the main relation.
		 */
		ObjectAddressSet(myself, CStoreRelationId, newStoreId);
		recordDependencyOn(&myself, &parentrel, DEPENDENCY_INTERNAL);
		ObjectAddressSet(myself, RelationRelationId, newStoreId);
		recordDependencyOn(&myself, &parentrel, DEPENDENCY_INTERNAL);

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
AddNewColstoreTuple(Relation pg_cstore, ColumnStoreElem *entry,
					Oid storeId, Oid relid)
{
	HeapTuple	newtup;
	ListCell   *cell;
	Datum		values[Natts_pg_cstore];
	bool		nulls[Natts_pg_cstore];
	NameData	cstname;
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
	namestrcpy(&cstname, entry->name);
	values[Anum_pg_cstore_cstname - 1] = NameGetDatum(&cstname);
	values[Anum_pg_cstore_cstrelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_cstore_cststoreid - 1] = ObjectIdGetDatum(storeId);
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

Oid
get_relation_cstore_oid(Oid relid, const char *cstore_name, bool missing_ok)
{
	Relation	pg_cstore_rel;
	ScanKeyData	skey[2];
	SysScanDesc	sscan;
	HeapTuple	cstore_tuple;
	Oid			cstore_oid;

	pg_cstore_rel = heap_open(CStoreRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_cstore_cstrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	ScanKeyInit(&skey[1],
				Anum_pg_cstore_cstname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(cstore_name));

	sscan = systable_beginscan(pg_cstore_rel,
							   CStoreCstRelidCstnameIndexId, true, NULL, 2,
							   skey);

	cstore_tuple = systable_getnext(sscan);

	if (!HeapTupleIsValid(cstore_tuple))
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("column store \"%s\" for table \"%s\" does not exist",
							cstore_name, get_rel_name(relid))));

		cstore_oid = InvalidOid;
	}
	else
		cstore_oid = HeapTupleGetOid(cstore_tuple);

	/* Clean up. */
	systable_endscan(sscan);
	heap_close(pg_cstore_rel, AccessShareLock);

	return cstore_oid;
}

void
RemoveColstoreById(Oid cstoreOid)
{
	Relation	pg_cstore;
	HeapTuple	tuple;

	pg_cstore = heap_open(CStoreRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(CSTOREOID, ObjectIdGetDatum(cstoreOid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for column store %u", cstoreOid);

	simple_heap_delete(pg_cstore, &tuple->t_self);

	ReleaseSysCache(tuple);

	heap_close(pg_cstore, RowExclusiveLock);
}

static Oid
CStoreAMGetOid(char *amname)
{
	Oid			oid;

	oid = GetSysCacheOid1(CSTOREAMNAME,
							CStringGetDatum(amname));
	if (!OidIsValid(oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("column store access method \"%s\" does not exist",
						amname)));

	return oid;
}



/* ----------------
 *		BuildColumnStoreInfo
 *			Construct an ColumnStoreInfo record for an open column store
 *
 * ColumnStoreInfo stores the information about the column store that's needed
 * by FormColumnStoreDatum, which is used for insertion of individual column
 * store tuples.  Normally we build a ColumnStoreInfo for a column store just
 * once per command, and then use it for (potentially) many tuples.
 * ----------------
 */
ColumnStoreInfo *
BuildColumnStoreInfo(Relation cstore)
{
	ColumnStoreInfo  *csi = makeNode(ColumnStoreInfo);
	Form_pg_cstore cstoreStruct = cstore->rd_cstore;
	int			i;
	int			numKeys;

	/* check the number of keys, and copy attr numbers into the IndexInfo */
	numKeys = cstoreStruct->cstnatts;
	if (numKeys < 1 || numKeys > INDEX_MAX_KEYS)
		elog(ERROR, "invalid cstnatts %d for column store %u",
			 numKeys, RelationGetRelid(cstore));
	csi->csi_NumColumnStoreAttrs = numKeys;
	for (i = 0; i < numKeys; i++)
		csi->csi_KeyAttrNumbers[i] = cstoreStruct->cstatts.values[i];

	csi->csi_ColumnStoreRoutine
		= GetColumnStoreRoutineForRelation(cstore, true);

	return csi;
}

/* ----------------
 *		FormColumnStoreDatum
 *			Construct values[] and isnull[] arrays for a new column store tuple.
 *
 *	columnStoreInfo	Info about the column store
 *	slot			Heap tuple for which we must prepare a column store entry
 *	values			Array of column store Datums (output area)
 *	isnull			Array of is-null indicators (output area)
 * ----------------
 */
void
FormColumnStoreDatum(ColumnStoreInfo *columnStoreInfo,
			   TupleTableSlot *slot,
			   Datum *values,
			   bool *isnull)
{
	elog(WARNING, "FormColumnStoreDatum: not yet implemented");
}

/*
 * Builds a descriptor for the heap part of the relation, and a tuple with only
 * the relevant attributes.
 */
HeapTuple
FilterHeapTuple(ResultRelInfo *resultRelInfo, HeapTuple tuple, TupleDesc *heapdesc)
{
	int			i, j, attnum;
	Bitmapset  *cstoreatts = NULL;	/* attributes mentioned in colstore */

	TupleDesc	origdesc = resultRelInfo->ri_RelationDesc->rd_att;
	HeapTuple	newtup;

	/* used to build the new descriptor / tuple */
	int		natts;
	Datum  *values;
	bool   *nulls;

	/* should not be called with no column stores */
	Assert(resultRelInfo->ri_NumColumnStores > 0);

	for (i = 0; i < resultRelInfo->ri_NumColumnStores; i++)
	{
		ColumnStoreInfo *cstinfo = resultRelInfo->ri_ColumnStoreRelationInfo[i];

		for (j = 0; j < cstinfo->csi_NumColumnStoreAttrs; j++)
			cstoreatts
				= bms_add_member(cstoreatts, cstinfo->csi_KeyAttrNumbers[j]);
	}

	/* we should get some columns from column stores */
	Assert(bms_num_members(cstoreatts) > 0);

	/* the new descriptor contains only the remaining attributes */
	natts = origdesc->natts - bms_num_members(cstoreatts);

	*heapdesc = CreateTemplateTupleDesc(natts, false);

	values = (Datum*)palloc0(sizeof(Datum) * natts);
	nulls  = (bool*)palloc0(sizeof(bool) * natts);

	attnum = 1;
	for (i = 0; i < origdesc->natts; i++)
	{
		/* if part of a column store, skip the attribute */
		if (bms_is_member(origdesc->attrs[i]->attnum, cstoreatts))
			continue;

		values[attnum-1] = heap_getattr(tuple, i+1, origdesc, &nulls[attnum-1]);

		TupleDescCopyEntry(*heapdesc, attnum++, origdesc, i+1);
	}

	newtup = heap_formtuple(*heapdesc, values, nulls);

	/* copy important header fields */
	newtup->t_self = tuple->t_self;
	newtup->t_data->t_ctid = tuple->t_data->t_ctid;

	pfree(values);
	pfree(nulls);

	return newtup;
}



/*
 * GetColumnStoreRoutine - call the specified column store handler routine
 * to get its ColumnStoreRoutine struct.
 */
ColumnStoreRoutine *
GetColumnStoreRoutine(Oid csthandler)
{
	Datum		datum;
	ColumnStoreRoutine *routine;

	datum = OidFunctionCall0(csthandler);
	routine = (ColumnStoreRoutine *) DatumGetPointer(datum);

	if (routine == NULL || !IsA(routine, ColumnStoreRoutine))
		elog(ERROR, "column store handler function %u did not return an ColumnStoreRoutine struct",
			 csthandler);

	return routine;
}


/*
 * GetColumnStoreHandlerByRelId - look up the handler of the column store handler
 * for the given column store relation
 */
Oid
GetColumnStoreHandlerByRelId(Oid relid)
{
	HeapTuple	tp;
	Form_pg_cstore_am cstform;
	Oid	csthandler;

	Relation	rel;
	Oid			amoid;

	rel = relation_open(relid, AccessShareLock);
	amoid = rel->rd_rel->relam;

	Assert(amoid != InvalidOid);
	Assert(rel->rd_rel->relkind == RELKIND_COLUMN_STORE);

	relation_close(rel, AccessShareLock);

	/* Get server OID for the foreign table. */
	tp = SearchSysCache1(CSTOREAMOID, ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for foreign table %u", amoid);
	cstform = (Form_pg_cstore_am) GETSTRUCT(tp);
	csthandler = cstform->csthandler;

	/* Complain if column store has been set to NO HANDLER. */
	if (!OidIsValid(csthandler))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("column store \"%s\" has no handler",
						NameStr(cstform->cstname))));

	ReleaseSysCache(tp);

	return csthandler;
}

/*
 * GetColumnStoreRoutineByRelId - look up the handler of the column store, and
 * retrieve its ColumnStoreRoutine struct.
 */
ColumnStoreRoutine *
GetColumnStoreRoutineByRelId(Oid relid)
{
	Oid	csthandler = GetColumnStoreHandlerByRelId(relid);

	return GetColumnStoreRoutine(csthandler);
}

/*
 * GetColumnStoreRoutineForRelation - look up the handler of the given column
 * store, and retrieve its ColumnStoreRoutine struct.
 *
 * This function is preferred over GetColumnStoreRoutineByRelId because it
 * caches the data in the relcache entry, saving a number of catalog lookups.
 *
 * If makecopy is true then the returned data is freshly palloc'd in the
 * caller's memory context.  Otherwise, it's a pointer to the relcache data,
 * which will be lost in any relcache reset --- so don't rely on it long.
 */
ColumnStoreRoutine *
GetColumnStoreRoutineForRelation(Relation relation, bool makecopy)
{
	ColumnStoreRoutine *cstroutine;
	ColumnStoreRoutine *ccstroutine;

	Assert(relation->rd_rel->relkind == RELKIND_COLUMN_STORE);
	Assert(relation->rd_rel->relam != 0);

	if (relation->rd_colstoreroutine == NULL)
	{
		/* Get the info by consulting the catalogs */
		cstroutine = GetColumnStoreRoutineByRelId(RelationGetRelid(relation));

		/* Save the data for later reuse in CacheMemoryContext */
		ccstroutine
			= (ColumnStoreRoutine *) MemoryContextAlloc(CacheMemoryContext,
													sizeof(ColumnStoreRoutine));
		memcpy(ccstroutine, cstroutine, sizeof(ColumnStoreRoutine));
		relation->rd_colstoreroutine = ccstroutine;

		/* Give back the locally palloc'd copy regardless of makecopy */
		return cstroutine;
	}

	/* We have valid cached data --- does the caller want a copy? */
	if (makecopy)
	{
		ccstroutine = (ColumnStoreRoutine *) palloc(sizeof(ColumnStoreRoutine));
		memcpy(ccstroutine, relation->rd_colstoreroutine, sizeof(ColumnStoreRoutine));
		return ccstroutine;
	}

	/* Only a short-lived reference is needed, so just hand back cached copy */
	return relation->rd_colstoreroutine;
}
