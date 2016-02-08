/*-------------------------------------------------------------------------
 *
 * changeset.c
 *	  code to create and destroy POSTGRES changeset relations
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/changeset.c
 *
 *
 * INTERFACE ROUTINES
 *		changeset_create()		- Create a cataloged changeset relation
 *		changeset_drop()		- Removes changeset relation from catalogs
 *		BuildChangeSetInfo()	- Prepare to insert changeset tuples
 *		FormChangeSetDatum()	- Construct datum vector for one changeset tuple
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "bootstrap/bootstrap.h"
#include "catalog/binary_upgrade.h"
#include "catalog/catalog.h"
#include "catalog/changeset.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_changeset.h"
#include "commands/cubes.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

static TupleDesc ConstructTupleDescriptor(Relation heapRelation,
						 ChangeSetInfo *chsetInfo,
						 List *chsetColNames);
static void InitializeAttributeOids(Relation chsetRelation,
						int numatts, Oid chsetoid);
static void AppendAttributeTuples(Relation chsetRelation, int numatts);
static void UpdateChangeSetRelation(Oid chsetoid, Oid heapoid,
					ChangeSetInfo *chsetInfo);

/*
 * changeset_create
 *
 * heapRelation: table to build changeset on (suitably locked by caller)
 * chsetRelationName: name of the changeset relation
 * chsetInfo: same info executor uses to insert into the changeset
 * chsetColNames: column names to use for changeset (List of char *)
 * tableSpaceId: OID of tablespace to use
 * reloptions: same as for heap
 * if_not_exists: if true, do not throw an error if a relation with
 *		the same name already exists.
 *
 * Returns the OID of the created changeset.
 */
Oid
changeset_create(Relation heapRelation,
			 const char *chsetRelationName,
			 ChangeSetInfo *chsetInfo,
			 List *chsetColNames,
			 Oid tableSpaceId,
			 Datum reloptions,
			 bool if_not_exists)
{
	Oid			chsetRelationId = InvalidOid;
	Oid			heapRelationId = RelationGetRelid(heapRelation);
	Relation	pg_class;
	Relation	chsetRelation;
	TupleDesc	chsetTupDesc;
	Oid			namespaceId;
	int			i;
	char		relpersistence;

	pg_class = heap_open(RelationRelationId, RowExclusiveLock);

	/*
	 * The index will be in the same namespace as its parent table, and it
	 * inherits the parent's relpersistence.
	 */
	namespaceId = RelationGetNamespace(heapRelation);
	relpersistence = heapRelation->rd_rel->relpersistence;

	/*
	 * check parameters
	 */
	if (chsetInfo->csi_NumChangeSetAttrs < 1)
		elog(ERROR, "changeset must contain at least one column");

	/* changesets on system catalogs not allowed */
	if (IsSystemRelation(heapRelation) &&
		IsNormalProcessingMode())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("changesets on system catalog tables are not supported")));

	if (get_relname_relid(chsetRelationName, namespaceId))
	{
		if (if_not_exists)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists, skipping",
							chsetRelationName)));
			heap_close(pg_class, RowExclusiveLock);
			return InvalidOid;
		}

		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists",
						chsetRelationName)));
	}

	/*
	 * construct tuple descriptor for changeset tuples
	 */
	chsetTupDesc = ConstructTupleDescriptor(heapRelation,
											chsetInfo,
											chsetColNames);

	/*
	 * Allocate an OID for the changeset.
	 */
	if (!OidIsValid(chsetRelationId))
	{
		/* Use binary-upgrade override for pg_class.oid/relfilenode? */
		if (IsBinaryUpgrade)
		{
			if (!OidIsValid(binary_upgrade_next_index_pg_class_oid))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("pg_class index OID value not set when in binary upgrade mode")));

			chsetRelationId = binary_upgrade_next_index_pg_class_oid;
			binary_upgrade_next_index_pg_class_oid = InvalidOid;
		}
		else
		{
			chsetRelationId =
				GetNewRelFileNode(tableSpaceId, pg_class, relpersistence);
		}
	}

	/*
	 * create the changeset relation's relcache entry and physical disk file. (If
	 * we fail further down, it's the smgr's responsibility to remove the disk
	 * file again.)
	 */
	chsetRelation = heap_create(chsetRelationName,
								namespaceId,
								tableSpaceId,
								chsetRelationId,
								InvalidOid,	/* relfilenode */
								chsetTupDesc,
								RELKIND_CHANGESET,
								relpersistence,
								false,	/* shared */
								false,	/* mapped */
								false); /* allow system mods */

	Assert(chsetRelationId == RelationGetRelid(chsetRelation));

	/*
	 * Obtain exclusive lock on it.  Although no other backends can see it
	 * until we commit, this prevents deadlock-risk complaints from lock
	 * manager in cases such as CLUSTER.
	 */
	LockRelation(chsetRelation, AccessExclusiveLock);

	/*
	 * Fill in fields of the changesets's pg_class entry that are not set
	 * correctly by heap_create.
	 *
	 * XXX should have a cleaner way to create cataloged changesets
	 */
	chsetRelation->rd_rel->relowner = heapRelation->rd_rel->relowner;
	chsetRelation->rd_rel->relhasoids = false;

	/*
	 * store changeset's pg_class entry
	 */
	InsertPgClassTuple(pg_class, chsetRelation,
					   RelationGetRelid(chsetRelation),
					   (Datum) 0,
					   reloptions);

	/* done with pg_class */
	heap_close(pg_class, RowExclusiveLock);

	/*
	 * now update the object id's of all the attribute tuple forms in the
	 * changeset relation's tuple descriptor
	 */
	InitializeAttributeOids(chsetRelation,
							chsetInfo->csi_NumChangeSetAttrs,
							chsetRelationId);

	/*
	 * append ATTRIBUTE tuples for the changeset
	 */
	AppendAttributeTuples(chsetRelation, chsetInfo->csi_NumChangeSetAttrs);

	/* ----------------
	 *	  update pg_changeset
	 *	  (append CHANGESET tuple)
	 * ----------------
	 */
	UpdateChangeSetRelation(chsetRelationId, heapRelationId, chsetInfo);

	/*
	 * Register dependencies for the changeset.
	 *
	 * We don't need a dependency on the namespace, because there'll be an
	 * indirect dependency via our parent table.
	 *
	 * During bootstrap we can't register any dependencies, but we don't
	 * support changesets during bootstrap anyway (so error out).
	 */
	if (!IsBootstrapProcessingMode())
	{
		ObjectAddress	myself,
						referenced;

		myself.classId = RelationRelationId;
		myself.objectId = chsetRelationId;
		myself.objectSubId = 0;

		/* create auto dependencies on the referenced columns */
		for (i = 0; i < chsetInfo->csi_NumChangeSetAttrs; i++)
		{
			if (chsetInfo->csi_KeyAttrNumbers[i] != 0)
			{
				referenced.classId = RelationRelationId;
				referenced.objectId = heapRelationId;
				referenced.objectSubId = chsetInfo->csi_KeyAttrNumbers[i];

				recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
			}
		}
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("changesets not supported during bootstap")));

	/* Post creation hook for new changeset */
	InvokeObjectPostCreateHookArg(RelationRelationId,
								  chsetRelationId, 0, false);

	/*
	 * Advance the command counter so that we can see the newly-entered
	 * catalog tuples for the changeset.
	 */
	CommandCounterIncrement();

	/*
	 * Close the changeset; but we keep the lock that we acquired above until
	 * end of transaction.  Closing the heap is caller's responsibility.
	 */
	relation_close(chsetRelation, NoLock);

	return chsetRelationId;

}

static TupleDesc
ConstructTupleDescriptor(Relation heapRelation,
						 ChangeSetInfo *chsetInfo,
						 List *chsetColNames)
{
	int			numatts = chsetInfo->csi_NumChangeSetAttrs;
	ListCell   *colnames_item = list_head(chsetColNames);
	TupleDesc	heapTupDesc;
	TupleDesc	chsetTupDesc;
	int			natts; 			/* #atts in heap rel */
	int			i;

	/* we need access to the table's tuple descriptor */
	heapTupDesc = RelationGetDescr(heapRelation);
	natts = RelationGetForm(heapRelation)->relnatts;

	/*
	 * allocate the new tuple descriptor
	 */
	chsetTupDesc = CreateTemplateTupleDesc(numatts, false);

	/*
	 * Changesets can only contain simple columns, so we copy the pg_attribute
	 * row from the parent relation and modify it as necessary.
	 */
	for (i = 0; i < numatts; i++)
	{
		AttrNumber	atnum = chsetInfo->csi_KeyAttrNumbers[i];
		Form_pg_attribute to = chsetTupDesc->attrs[i];

		/* Simple index column */
		Form_pg_attribute from;

		/*
		 * make sure only regular attributes make it into the descriptor
		 */
		if ((atnum < 0) || (atnum > natts))		/* safety check */
			elog(ERROR, "invalid column number %d", atnum);

		from = heapTupDesc->attrs[AttrNumberGetAttrOffset(atnum)];

		/*
		 * now that we've determined the "from", let's copy the tuple desc
		 * data...
		 */
		memcpy(to, from, ATTRIBUTE_FIXED_PART_SIZE);

		/*
		 * Fix the stuff that should not be the same as the underlying
		 * attr
		 */
		to->attnum = i + 1;

		to->attstattarget = -1;
		to->attcacheoff = -1;
		to->attnotnull = false;
		to->atthasdef = false;
		to->attislocal = true;
		to->attinhcount = 0;
		to->attcollation = 0;

		/*
		 * We do not yet have the correct relation OID for the index, so just
		 * set it invalid for now.  InitializeAttributeOids() will fix it
		 * later.
		 */
		to->attrelid = InvalidOid;

		/*
		 * Set the attribute name as specified by caller.
		 */
		if (colnames_item == NULL)		/* shouldn't happen */
			elog(ERROR, "too few entries in colnames list");
		namestrcpy(&to->attname, (const char *) lfirst(colnames_item));
		colnames_item = lnext(colnames_item);
	}

	return chsetTupDesc;

}

static void
InitializeAttributeOids(Relation chsetRelation,
						int numatts, Oid chsetoid)
{
	// FIXME
	return;
}

static void
AppendAttributeTuples(Relation chsetRelation, int numatts)
{
	Relation	pg_attribute;
	CatalogIndexState indstate;
	TupleDesc	chsetTupDesc;
	int			i;

	/*
	 * open the attribute relation and its indexes
	 */
	pg_attribute = heap_open(AttributeRelationId, RowExclusiveLock);

	indstate = CatalogOpenIndexes(pg_attribute);

	/*
	 * insert data from new changeset's tupdesc into pg_attribute
	 */
	chsetTupDesc = RelationGetDescr(chsetRelation);

	for (i = 0; i < numatts; i++)
	{
		Assert(chsetTupDesc->attrs[i]->attnum == i + 1);
		Assert(chsetTupDesc->attrs[i]->attcacheoff == -1);

		InsertPgAttributeTuple(pg_attribute, chsetTupDesc->attrs[i], indstate);
	}

	CatalogCloseIndexes(indstate);

	heap_close(pg_attribute, RowExclusiveLock);
}

static void
UpdateChangeSetRelation(Oid chsetoid, Oid heapoid,
						ChangeSetInfo *chsetInfo)
{
	int2vector *chsetkey;
	Datum		values[Natts_pg_changeset];
	bool		nulls[Natts_pg_changeset];
	Relation	pg_changeset;
	HeapTuple	tuple;
	int			i;

	/* copy the changeset key info into arrays */
	chsetkey = buildint2vector(NULL, chsetInfo->csi_NumChangeSetAttrs);
	for (i = 0; i < chsetInfo->csi_NumChangeSetAttrs; i++)
		chsetkey->values[i] = chsetInfo->csi_KeyAttrNumbers[i];

	/* open the system catalog changeset relation */
	pg_changeset = heap_open(ChangeSetRelationId, RowExclusiveLock);

	/* build a pg_changeset tuple */
	MemSet(nulls, false, sizeof(nulls));

	values[Anum_pg_changeset_chsetid    - 1] = ObjectIdGetDatum(chsetoid);
	values[Anum_pg_changeset_chsetrelid - 1] = ObjectIdGetDatum(heapoid);
	values[Anum_pg_changeset_chsetnatts - 1] = Int16GetDatum(chsetInfo->csi_NumChangeSetAttrs);
	values[Anum_pg_changeset_chsetkey   - 1] = PointerGetDatum(chsetkey);

	tuple = heap_form_tuple(RelationGetDescr(pg_changeset), values, nulls);

	/* insert the tuple into the pg_changeset catalog */
	simple_heap_insert(pg_changeset, tuple);

	/* update the indexes on pg_changeset */
	CatalogUpdateIndexes(pg_changeset, tuple);

	/* close the relation and free the tuple */
	heap_close(pg_changeset, RowExclusiveLock);
	heap_freetuple(tuple);
}
