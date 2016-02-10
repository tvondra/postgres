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
#include "catalog/pg_aggregate.h"
#include "catalog/pg_cube.h"
#include "catalog/pg_changeset.h"
#include "catalog/pg_type.h"
#include "catalog/changeset.h"
#include "commands/cubes.h"
#include "commands/tablespace.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static char *ChooseChangeSetName(const char *tabname, Oid namespaceId);
static char *ChooseCubeName(const char *tabname, Oid namespaceId);

static void ComputeChangeSetAttrs(ChangeSetInfo *chsetInfo,
				  List *attList,
				  Oid relId);

static void ComputeCubeAttrs(CubeInfo *cubeInfo,
				  Oid *typeOidP,
				  List *attList,
				  Oid relId);

static bool CheckMutability(Expr *expr);

static List *ChooseCubeColumnNames(List *cubeElems);

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

	/*
	 * ChangeSets are simple heap relations, so allow the same options.
	 *
	 * FIXME Prevent 'WITH OIDS' in the reloptions.
	 */
	(void) heap_reloptions(RELKIND_RELATION, reloptions, true);

	/*
	 * Prepare arguments for changeset_create, primarily an ChangeSetInfo structure.
	 */
	changeSetInfo = makeNode(ChangeSetInfo);

	ComputeChangeSetAttrs(changeSetInfo, stmt->chsetColumns, relationId);

	/* Make the catalog entries for the changeset. */
	chsetRelationId =  changeset_create(rel,
										chsetRelationName, changeSetInfo,
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
	Oid			changesetId;
	char	   *cubeRelationName;
	Oid			cubeRelationId;
	Oid			namespaceId;
	Oid			tablespaceId;
	Oid		   *typeObjectId;
	Relation	rel;
	Relation	chsetrel;

	CubeInfo   *cubeInfo;
	Datum		reloptions;
	int			numberOfAttributes;
	ObjectAddress address;
	bool		check_rights = true;
	List	   *cubeColNames;

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

	/* FIXME lookup changeset if needed (not supplied in command) */
	if (stmt->changeset != NULL)
	{
		changesetId = RangeVarGetRelid(stmt->changeset, AccessShareLock, false);
		chsetrel = changeset_open(changesetId, NoLock);
	}
	else
	{
		elog(WARNING, "lookup suitable changeset");
		chsetrel = NULL;
		changesetId = InvalidOid;
	}

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
	 * Choose the cube column names.
	 */
	cubeColNames = ChooseCubeColumnNames(stmt->cubeExprs);

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
	cubeRelationId =  cube_create(rel, chsetrel, cubeRelationName,
								  cubeInfo, typeObjectId, cubeColNames,
								  tablespaceId, reloptions,
								  stmt->if_not_exists);

	ObjectAddressSet(address, RelationRelationId, cubeRelationId);

	if (!OidIsValid(cubeRelationId))
	{
		heap_close(rel, NoLock);
		return address;
	}

	/* Close the heap and changeset and we're done */
	heap_close(rel, NoLock);
	changeset_close(chsetrel, NoLock);

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
					  List *attList,
					  Oid relId)
{
	ListCell   *lc;
	int			k;
	int			natts;
	int			attn = 0;
	Bitmapset  *attnums = NULL;

	foreach (lc, attList)
	{
		char	   *attname;
		HeapTuple	atttuple;
		Form_pg_attribute attform;

		attname = strVal(lfirst(lc));
		atttuple = SearchSysCacheAttName(relId, attname);

		if (!HeapTupleIsValid(atttuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" does not exist",
							attname)));

		attform = (Form_pg_attribute) GETSTRUCT(atttuple);

		attnums = bms_add_member(attnums, attform->attnum);

		ReleaseSysCache(atttuple);
	}

	/*
	 * By using bitmapset to collect the attnums, we ignore duplicates
	 * and also sort the attnums. This is intentional, and the order
	 * does not really matter (unlike from indexes, where it determines
	 * shape of the tree, ordering and so on).
	 */
	natts = bms_num_members(attnums);

	chsetInfo->csi_NumChangeSetAttrs = natts;
	chsetInfo->csi_KeyAttrNumbers = palloc0(natts * sizeof(AttrNumber));

	k = -1;
	while ((k = bms_next_member(attnums, k)) >= 0)
		chsetInfo->csi_KeyAttrNumbers[attn++] = k;

}

static void
ComputeCubeAttrs(CubeInfo *cubeInfo,
				 Oid *typeOidP,
				 List *attList,
				 Oid relId)
{
	int			attn;
	ListCell   *lc;

	attn = 0;
	foreach (lc, attList)
	{
		CubeElem   *attribute = (CubeElem *) lfirst(lc);
		Oid			atttype;

		/*
		 * Process the column-or-expression to be referenced by the cube.
		 */
		if (attribute->name != NULL)
		{
			/* Simple cube attribute */
			HeapTuple	atttuple;
			Form_pg_attribute attform;

			Assert(attribute->expr == NULL);
			atttuple = SearchSysCacheAttName(relId, attribute->name);

			if (!HeapTupleIsValid(atttuple))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" does not exist",
								attribute->name)));

			attform = (Form_pg_attribute) GETSTRUCT(atttuple);
			atttype = attform->atttypid;

			cubeInfo->ci_KeyAttrNumbers[attn] = attform->attnum;

			ReleaseSysCache(atttuple);
		}
		else
		{
			/*
			 * Cube expression - either it's a Var (i.e. a column in parens),
			 * or an aggregate call, or a generic expression.
			 */
			Node	   *expr = attribute->expr;

			Assert(expr != NULL);
			atttype = exprType(expr);

			if (IsA(expr, Var) &&
				((Var *) expr)->varattno != InvalidAttrNumber)
			{
				/*
				 * User wrote "(column)", so treat it like a simple attribute.
				 */
				cubeInfo->ci_KeyAttrNumbers[attn] = ((Var *) expr)->varattno;
			}
			else
			{
				cubeInfo->ci_KeyAttrNumbers[attn] = 0; /* marks expression */
				cubeInfo->ci_Expressions = lappend(cubeInfo->ci_Expressions,
													expr);

				/*
				 * transformExpr() should have already rejected subqueries,
				 * aggregates, and window functions, based on the EXPR_KIND_
				 * for an index expression.
				 */

				/*
				 * Aggregate function - we'll store the moving-aggregate in
				 * the cube, so so we need to check the aggregate function
				 * actually has a moving-aggregate methods, and that we know
				 * how to store the type (must not be internal).
				 */
				if (IsA(expr, Aggref))
				{
					HeapTuple	tup;
					Form_pg_aggregate classForm;
					Oid			mtransfn, minvtransfn, mtranstype;

					Aggref *aggref = (Aggref*)expr;

					tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggref->aggfnoid));
					if (!HeapTupleIsValid(tup))		/* should not happen */
						elog(ERROR, "cache lookup failed for aggregate %u", aggref->aggfnoid);

					classForm = (Form_pg_aggregate) GETSTRUCT(tup);

					mtransfn = classForm->aggmtransfn;
					minvtransfn = classForm->aggminvtransfn;
					mtranstype = classForm->aggmtranstype;

					ReleaseSysCache(tup);

					/*
					 * FIXME Probably need to check additional things, e.g. that it's
					 * 		 not a window function, ordered set aggregate etc.
					 */
					if (! (OidIsValid(mtransfn) &&
						   OidIsValid(minvtransfn) &&
						   OidIsValid(mtranstype)))
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("only moving-aggregates supported in cubes")));

					/* check we know how to serialize the moving-aggregate state */
					if (mtranstype == INTERNALOID)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("moving-aggregates with 'internal' state not supported in cubes")));

					/* remember the transition type for pg_attribute */
					atttype = mtranstype;
				}

				/*
				 * An expression using mutable functions is probably wrong,
				 * since if you aren't going to get the same result for the
				 * same data every time, it's not clear what the index entries
				 * mean at all.
				 */
				if (CheckMutability((Expr *) expr))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("functions in cube expression must be marked IMMUTABLE")));
			}
		}

		typeOidP[attn] = atttype;

		attn++;
	}

	return;
}


/*
 * CheckMutability
 *		Test whether given expression is mutable
 *
 * FIXME copy-paste from indexcmds.c
 */
static bool
CheckMutability(Expr *expr)
{
	/*
	 * First run the expression through the planner.  This has a couple of
	 * important consequences.  First, function default arguments will get
	 * inserted, which may affect volatility (consider "default now()").
	 * Second, inline-able functions will get inlined, which may allow us to
	 * conclude that the function is really less volatile than it's marked. As
	 * an example, polymorphic functions must be marked with the most volatile
	 * behavior that they have for any input type, but once we inline the
	 * function we may be able to conclude that it's not so volatile for the
	 * particular input type we're dealing with.
	 *
	 * We assume here that expression_planner() won't scribble on its input.
	 */
	expr = expression_planner(expr);

	/* Now we can search for non-immutable functions */
	return contain_mutable_functions((Node *) expr);
}

/*
 * Select the actual names to be used for the columns of a cube, given the
 * list of CubeElems for the columns.  This is mostly about ensuring the
 * names are unique so we don't get a conflicting-attribute-names error.
 *
 * Returns a List of plain strings (char *, not String nodes).
 */
static List *
ChooseCubeColumnNames(List *cubeExprs)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, cubeExprs)
	{
		CubeElem   *celem = (CubeElem *) lfirst(lc);
		const char *origname;
		const char *curname;
		int			i;
		char		buf[NAMEDATALEN];

		/* Get the preliminary name from the CubeElem */
		if (celem->cubecolname)
			origname = celem->cubecolname;	/* caller-specified name */
		else if (celem->name)
			origname = celem->name;			/* simple column reference */
		else
			origname = "expr";	/* default name for expression */

		/* If it conflicts with any previous column, tweak it */
		curname = origname;
		for (i = 1;; i++)
		{
			ListCell   *lc2;
			char		nbuf[32];
			int			nlen;

			foreach(lc2, result)
			{
				if (strcmp(curname, (char *) lfirst(lc2)) == 0)
					break;
			}
			if (lc2 == NULL)
				break;			/* found nonconflicting name */

			sprintf(nbuf, "%d", i);

			/* Ensure generated names are shorter than NAMEDATALEN */
			nlen = pg_mbcliplen(origname, strlen(origname),
								NAMEDATALEN - 1 - strlen(nbuf));
			memcpy(buf, origname, nlen);
			strcpy(buf + nlen, nbuf);
			curname = buf;
		}

		/* And attach to the result list */
		result = lappend(result, pstrdup(curname));
	}
	return result;
}
