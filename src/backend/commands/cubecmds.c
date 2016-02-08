/*-------------------------------------------------------------------------
 *
 * cubes.c
 *	  Commands to manipulate changesets and cubes
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/cubes.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/catalog.h"
#include "catalog/pg_cube.h"
#include "catalog/pg_changeset.h"
#include "catalog/changeset.h"
#include "commands/cubes.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"

static char *ChooseChangeSetName(const char *tabname, Oid namespaceId);
static char *ChooseCubeName(const char *tabname, Oid namespaceId);

static void ComputeChangeSetAttrs(ChangeSetInfo *chsetInfo,
				  Oid *typeOidP,
				  List *attList,
				  Oid relId);

static void ComputeCubeAttrs(CubeInfo *cubeInfo,
				  Oid *typeOidP,
				  List *attList,
				  Oid relId);
/*
 * CreateChangeSet
 *		Creates a new changeset
 *
 * stmt - ChangeSetStmt describing the properties of the new changeset
 *
 * Returns the object address of the created changeset.
 */
ObjectAddress
CreateChangeSet(ChangeSetStmt *stmt)
{
	Oid			relationId;
	char	   *chsetRelationName;
	Oid			chsetRelationId;
	Oid			namespaceId;
	Oid			tablespaceId;
	Oid		   *typeObjectId;
	Relation	rel;
	ChangeSetInfo  *changeSetInfo;
	Datum		reloptions;
	int			numberOfAttributes;
	ObjectAddress address;
	bool		check_rights = true;

	/* count attributes in changeset */
	numberOfAttributes = list_length(stmt->chsetColumns);
	if (numberOfAttributes <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("must specify at least one column")));

	/* No special locking needed, AccessShereLock is enough. */
	relationId = RangeVarGetRelid(stmt->relation, AccessShareLock, false);
 	rel = heap_open(relationId, NoLock);

	/* possibly not needed, copy-paste from indexcmds.c */
	relationId = RelationGetRelid(rel);
	namespaceId = RelationGetNamespace(rel);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table",
						RelationGetRelationName(rel))));

	/*
	 * Don't try to CREATE CHANGESET on temp tables of other backends.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot create changeset on temporary tables of other sessions")));

	/*
	 * Verify we (still) have CREATE rights in the rel's namespace.
	 * (Presumably we did when the rel was created, but maybe not anymore.)
	 * Skip check if caller doesn't want it.  Also skip check if
	 * bootstrapping, since permissions machinery may not be working yet.
	 */
	if (check_rights)
	{
		AclResult	aclresult;

		aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(),
										  ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
						   get_namespace_name(namespaceId));
	}

	/*
	 * Select tablespace to use.  If not specified, use default tablespace
	 * (which may in turn default to database's default).
	 */
	if (stmt->tableSpace)
	{
		tablespaceId = get_tablespace_oid(stmt->tableSpace, false);
	}
	else
	{
		tablespaceId = GetDefaultTablespace(rel->rd_rel->relpersistence);
		/* note InvalidOid is OK in this case */
	}

	/* Check permissions except when using database's default */
	if (OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace)
	{
		AclResult	aclresult;

		aclresult = pg_tablespace_aclcheck(tablespaceId, GetUserId(),
										   ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_TABLESPACE,
						   get_tablespace_name(tablespaceId));
	}

	/*
	 * Select name for changeset if caller didn't specify
	 */
	chsetRelationName = stmt->chsetname;
	if (chsetRelationName == NULL)
		chsetRelationName = ChooseChangeSetName(RelationGetRelationName(rel),
												namespaceId);

	/*
	 * Parse AM-specific options, convert to text array form, validate.
	 */
	reloptions = transformRelOptions((Datum) 0, stmt->options,
									 NULL, NULL, false, false);

	/* ChangeSets are simple heap relations, so allow the same options. */
	(void) heap_reloptions(RELKIND_RELATION, reloptions, true);

	/*
	 * Prepare arguments for changeset_create, primarily an ChangeSetInfo structure.
	 */
	changeSetInfo = makeNode(ChangeSetInfo);
	changeSetInfo->csi_NumChangeSetAttrs = numberOfAttributes;
	changeSetInfo->csi_KeyAttrNumbers = palloc0(numberOfAttributes * sizeof(AttrNumber));

	typeObjectId = (Oid *) palloc(numberOfAttributes * sizeof(Oid));

	ComputeChangeSetAttrs(changeSetInfo,
						  typeObjectId, stmt->chsetColumns, relationId);

	/* Make the catalog entries for the changeset. */
	chsetRelationId =  changeset_create(rel, chsetRelationName,
										changeSetInfo, stmt->chsetColumns,
										tablespaceId, reloptions,
										stmt->if_not_exists);

	ObjectAddressSet(address, RelationRelationId, chsetRelationId);

	if (!OidIsValid(chsetRelationId))
	{
		heap_close(rel, NoLock);
		return address;
	}

	/* Close the heap and we're done */
	heap_close(rel, NoLock);
	return address;
}

/*
 * CREATE CUBE
 */
ObjectAddress
CreateCube(CubeStmt *stmt)
{
	Oid			relationId;
	char	   *cubeRelationName;
	Oid			cubeRelationId;
	Oid			namespaceId;
	Oid			tablespaceId;
	Oid		   *typeObjectId;
	Relation	rel;
	CubeInfo   *cubeInfo;
	Datum		reloptions;
	int			numberOfAttributes;
	ObjectAddress address;
	bool		check_rights = true;

	/* count attributes in cube */
	numberOfAttributes = list_length(stmt->cubeExprs);
	if (numberOfAttributes <= 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("must specify at least two attributes")));

	/* FIXME check that there's at least one dimension and one aggregate */

	/* No special locking needed, AccessShereLock is enough. */
	relationId = RangeVarGetRelid(stmt->relation, AccessShareLock, false);
 	rel = heap_open(relationId, NoLock);

	/* possibly not needed, copy-paste from indexcmds.c */
	relationId = RelationGetRelid(rel);
	namespaceId = RelationGetNamespace(rel);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table",
						RelationGetRelationName(rel))));

	/*
	 * Don't try to CREATE CUBE on temp tables of other backends.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot create cube on temporary tables of other sessions")));

	/*
	 * Verify we (still) have CREATE rights in the rel's namespace.
	 * (Presumably we did when the rel was created, but maybe not anymore.)
	 * Skip check if caller doesn't want it.  Also skip check if
	 * bootstrapping, since permissions machinery may not be working yet.
	 */
	if (check_rights)
	{
		AclResult	aclresult;

		aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(),
										  ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
						   get_namespace_name(namespaceId));
	}

	/*
	 * Select tablespace to use.  If not specified, use default tablespace
	 * (which may in turn default to database's default).
	 */
	if (stmt->tableSpace)
	{
		tablespaceId = get_tablespace_oid(stmt->tableSpace, false);
	}
	else
	{
		tablespaceId = GetDefaultTablespace(rel->rd_rel->relpersistence);
		/* note InvalidOid is OK in this case */
	}

	/* Check permissions except when using database's default */
	if (OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace)
	{
		AclResult	aclresult;

		aclresult = pg_tablespace_aclcheck(tablespaceId, GetUserId(),
										   ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_TABLESPACE,
						   get_tablespace_name(tablespaceId));
	}

	/*
	 * Select name for cube if caller didn't specify
	 */
	cubeRelationName = stmt->cubename;
	if (cubeRelationName == NULL)
		cubeRelationName = ChooseCubeName(RelationGetRelationName(rel),
										  namespaceId);

	/*
	 * Parse AM-specific options, convert to text array form, validate.
	 */
	reloptions = transformRelOptions((Datum) 0, stmt->options,
									 NULL, NULL, false, false);

	/* Cubes are simple heap relations, so allow the same options. */
	(void) heap_reloptions(RELKIND_RELATION, reloptions, true);

	/*
	 * Prepare arguments for cube_create, primarily an CubeInfo structure.
	 */
	cubeInfo = makeNode(CubeInfo);
	cubeInfo->ci_NumCubeAttrs = numberOfAttributes;
	cubeInfo->ci_KeyAttrNumbers = palloc0(numberOfAttributes * sizeof(AttrNumber));

	typeObjectId = (Oid *) palloc(numberOfAttributes * sizeof(Oid));

	ComputeCubeAttrs(cubeInfo,
					 typeObjectId, stmt->cubeExprs, relationId);

	/* Make the catalog entries for the cube. */
	cubeRelationId =  cube_create(rel, cubeRelationName,
								  cubeInfo, stmt->cubeExprs,
								  tablespaceId, reloptions,
								  stmt->if_not_exists);

	ObjectAddressSet(address, RelationRelationId, cubeRelationId);

	if (!OidIsValid(cubeRelationId))
	{
		heap_close(rel, NoLock);
		return address;
	}

	/* Close the heap and we're done */
	heap_close(rel, NoLock);
	return address;
}

static char *
ChooseChangeSetName(const char *tabname, Oid namespaceId)
{
	return "changeset_xyz";
}
static char *
ChooseCubeName(const char *tabname, Oid namespaceId)
{
	return "cube_xyz";
}

static void
ComputeChangeSetAttrs(ChangeSetInfo *chsetInfo,
					  Oid *typeOidP,
					  List *attList,
					  Oid relId)
{
	// FIXME
	int i = 0;
	ListCell *lc;

	foreach (lc, attList)
	{
		chsetInfo->csi_KeyAttrNumbers[i] = (i+1);
		i++;
	}

	return;
}

static void
ComputeCubeAttrs(CubeInfo *cubeInfo,
				 Oid *typeOidP,
				 List *attList,
				 Oid relId)
{
	// FIXME
	int i = 0;
	ListCell *lc;

	foreach (lc, attList)
	{
		cubeInfo->ci_KeyAttrNumbers[i] = (i+1);
		i++;
	}

	return;
}
