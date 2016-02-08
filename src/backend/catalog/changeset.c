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
#include "access/xact.h"
#include "bootstrap/bootstrap.h"
#include "catalog/binary_upgrade.h"
#include "catalog/catalog.h"
#include "catalog/changeset.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/objectaccess.h"
#include "commands/cubes.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
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
	// FIXME
	return NULL;
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
	// FIXME
	return;
}

static void
UpdateChangeSetRelation(Oid chsetoid, Oid heapoid,
						ChangeSetInfo *chsetInfo)
{
	// FIXME
	return;
}
