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
#include "catalog/pg_type.h"
#include "commands/cubes.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

static TupleDesc ConstructTupleDescriptor(Relation heapRelation,
						 ChangeSetInfo *chsetInfo);
static void UpdateChangeSetRelation(Oid chsetoid, Oid heapoid,
					ChangeSetInfo *chsetInfo);

/*
 * changeset_create
 *
 * heapRelation: table to build changeset on (suitably locked by caller)
 * chsetRelationName: name of the changeset relation
 * chsetInfo: same info executor uses to insert into the changeset
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
			 Oid tableSpaceId,
			 Datum reloptions,
			 bool if_not_exists)
{
	Oid			chsetRelationId = InvalidOid;
	Oid			heapRelationId = RelationGetRelid(heapRelation);
	Relation	pg_class;
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
											chsetInfo);

	/*
	 * create the changeset relation's relcache entry and physical disk file. (If
	 * we fail further down, it's the smgr's responsibility to remove the disk
	 * file again.)
	 */
	chsetRelationId
		= heap_create_with_catalog(chsetRelationName,
								   namespaceId,
								   tableSpaceId,
								   InvalidOid, /* relid */
								   InvalidOid, /* reltypeid */
								   InvalidOid, /* reloftypeid */
								   heapRelation->rd_rel->relowner,
								   chsetTupDesc,
								   NIL,
								   RELKIND_CHANGESET,
								   relpersistence,
								   false, /* not shared */
								   false, /* not mapped */
								   false, /* oidislocal */
								   0,     /* attinhcount */
								   ONCOMMIT_NOOP,
								   reloptions,
								   false, /* no ACLs */
								   false, /* not a system catalog */
								   false, /* not internal */
								   NULL); /* no object address */

	Assert(OidIsValid(chsetRelationId));


	/* done with pg_class */
	heap_close(pg_class, RowExclusiveLock);

	/* ----------------
	 *	  update pg_changeset
	 *	  (append CHANGESET tuple)
	 * ----------------
	 */
	UpdateChangeSetRelation(chsetRelationId, heapRelationId, chsetInfo);

	/*
	 * Register additional dependencies for the changeset.
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

	/*
	 * Advance the command counter so that we can see the newly-entered
	 * catalog tuples for the changeset.
	 */
	CommandCounterIncrement();

	return chsetRelationId;

}

static TupleDesc
ConstructTupleDescriptor(Relation heapRelation,
						 ChangeSetInfo *chsetInfo)
{
	int			numatts = chsetInfo->csi_NumChangeSetAttrs;
	int			idx = 0;
	TupleDesc	heapTupDesc;
	TupleDesc	chsetTupDesc;
	int			natts; 			/* #atts in heap rel */
	int			i;

	/* we need access to the table's tuple descriptor */
	heapTupDesc = RelationGetDescr(heapRelation);
	natts = RelationGetForm(heapRelation)->relnatts;

	/* walk through column names, match them to attributes */

	/*
	 * allocate the new tuple descriptor
	 *
	 * include extra attribute for change type info (insert/delete)
	 */
	chsetTupDesc = CreateTemplateTupleDesc(numatts + 1, false);

	/*
	 * initialize the extra attribute (always the first one)
	 *
	 * FIXME The attribute has a hard-coded name, so it might conflict
	 *       with existing attributes. Not sure how to fix this, but
	 *       maybe we can make it customizable in reloptions (we can
	 *       rely on the position) or make it a system attribute (but
	 *       that seems annoying).
	 */
	TupleDescInitEntry(chsetTupDesc, 1, "change_type", CHAROID, 0, 0);

	/*
	 * Changesets can only contain simple columns, so we copy the pg_attribute
	 * row from the parent relation and modify it as necessary.
	 *
	 * While indexes only include regular attributes, we need all attributes
	 * including the system ones, as we need to track visibility and so on.
	 */
	for (i = 0; i < numatts; i++)
	{
		AttrNumber	atnum = chsetInfo->csi_KeyAttrNumbers[i];
		Form_pg_attribute to = chsetTupDesc->attrs[++idx];

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
		to->attnum = idx + 1;

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
		 * Set the attribute name same as in the heap relation.
		 */
		namecpy(&to->attname, &from->attname);
	}

	return chsetTupDesc;

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



/* ----------------
 *		BuildChangeSetInfo
 *			Construct an ChangeSetInfo record for an open changeset
 *
 * ChangeSetInfo stores the information about the changeset that's needed by
 * FormChangeSetDatum, which is used for insertion of tuples. Normally we
 * build an ChangeSetInfo for a changeset just once per command, and then
 * use it for (potentially) many tuples.
 * ----------------
 */
ChangeSetInfo *
BuildChangeSetInfo(Relation changeset)
{
	ChangeSetInfo  *csi = makeNode(ChangeSetInfo);
	Form_pg_changeset chsetStruct = changeset->rd_changeset;
	int			i;
	int			numKeys;

	/* check the number of keys, and copy attr numbers into the ChangeSetInfo */
	numKeys = chsetStruct->chsetnatts;
	if (numKeys < 1)
		elog(ERROR, "invalid chsetnatts %d for changeset %u",
			 numKeys, RelationGetRelid(changeset));

	csi->csi_NumChangeSetAttrs = numKeys;
	csi->csi_KeyAttrNumbers = palloc0(sizeof(AttrNumber) * numKeys);

	for (i = 0; i < numKeys; i++)
		csi->csi_KeyAttrNumbers[i] = chsetStruct->chsetkey.values[i];

	return csi;
}

/* ----------------
 *	FormChangeSetDatum
 *		Construct values[] and isnull[] arrays for a new changeset tuple.
 *
 *	chsetInfo		Info about the changeset
 *	slot			Heap tuple for which we must prepare an changeset entry
 *	values			Array of changeset Datums (output area)
 *	isnull			Array of is-null indicators (output area)
 *
 * Notice we don't actually call heap_form_tuple() here; we just prepare
 * its input arrays values[] and isnull[].  This is because that's how
 * indexes do that, so we follow the same structure.
 * ----------------
 */
void
FormChangeSetDatum(ChangeSetInfo *chsetInfo,
				   char changeType,
				   TupleTableSlot *slot,
				   Datum *values,
				   bool *isnull)
{
	int			i;

	Assert((changeType == CHANGESET_INSERT)	||
		   (changeType == CHANGESET_DELETE));

	/* first value is always the type of operation (insert/delete) */
	values[0] = changeType;
	isnull[0] = false;

	for (i = 0; i < chsetInfo->csi_NumChangeSetAttrs; i++)
	{
		int			keycol = chsetInfo->csi_KeyAttrNumbers[i];
		Datum		iDatum;
		bool		isNull;

		Assert(keycol > 0);

		iDatum = slot_getattr(slot, keycol, &isNull);

		values[i+1] = iDatum;
		isnull[i+1] = isNull;
	}
}
