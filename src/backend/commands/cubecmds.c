/*-------------------------------------------------------------------------
 *
 * cubecmds.c
 *	  Commands to manipulate changesets and cubes
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/cubecmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_changeset.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_cube.h"
#include "catalog/pg_type.h"
#include "catalog/changeset.h"
#include "commands/cubes.h"
#include "commands/defrem.h"
#include "commands/tablespace.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

static char *ChooseChangeSetName(const char *tabname, Oid namespaceId);
static char *ChooseCubeName(const char *tabname, Oid namespaceId);

static void ComputeChangeSetAttrs(ChangeSetInfo *chsetInfo,
				  List *attList,
				  Oid relId);

static void ComputeCubeAttrs(CubeInfo *cubeInfo,
				  Oid *typeOidP,
				  Oid *collationObjectId,
				  Oid *classObjectId,
				  List *attList,
				  Oid relId);

static bool CheckMutability(Expr *expr);

static List *ChooseCubeColumnNames(List *cubeElems);
static CubeOptInfo * build_cube_opt_info(Oid cubeoid);

/* FIXME probably should live in relcache.c next to RelationGetCubeList */
static List * ChangeSetGetCubeList(Relation changeset);

static TupleDesc build_tupdesc_for_cube(CubeOptInfo *cube,
					   ChangeSetInfo *chsetInfo,
					   TupleDesc chsetDesc, int *nkeys,
					   AttrNumber **attnums, Node ***expressions);

static Tuplesortstate *build_tss_for_cube(TupleDesc tdesc, int nkeys);

static void fix_changeset_vars(Node *node, ChangeSetInfo *changeset);

static Bitmapset *collect_simple_dim_attnums(CubeOptInfo *cube);
static Bitmapset *analyze_cube_expressions(CubeOptInfo *cube, int *numexprs);

static AttrNumber lookup_attnum_in_changeset(ChangeSetInfo *chsetInfo,
											 AttrNumber attnum);

static void update_cube(Relation chsetRel, ChangeSetInfo *chsetInfo,
					   TupleDesc chsetDesc, Oid cubeOid);

static void changeset_cleanup(Relation chsetRel);

/*
 * CreateChangeSet
 *		Creates a new changeset
 *
 * stmt - ChangeSetStmt describing the properties of the new changeset
 *
 * Returns the object address of the created changeset.
 */
ObjectAddress
CreateChangeSet(CreateChangeSetStmt *stmt)
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

	/* Force backends to reload the list of changesets on the table. */
	CacheInvalidateRelcache(rel);

	/* Close the heap and we're done */
	heap_close(rel, NoLock);
	return address;
}

/*
 * CREATE CUBE
 */
ObjectAddress
CreateCube(CreateCubeStmt *stmt)
{
	Oid			relationId;
	Oid			changesetId;
	char	   *cubeRelationName;
	Oid			cubeRelationId;
	Oid			namespaceId;
	Oid			tablespaceId;
	Oid		   *typeObjectId,
			   *collationObjectId,
			   *classObjectId;
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
	cubeInfo->ci_Expressions = NIL;

	typeObjectId = (Oid *) palloc(numberOfAttributes * sizeof(Oid));
	collationObjectId = (Oid *) palloc(numberOfAttributes * sizeof(Oid));
	classObjectId = (Oid *) palloc(numberOfAttributes * sizeof(Oid));

	ComputeCubeAttrs(cubeInfo, typeObjectId, collationObjectId, classObjectId,
					 stmt->cubeExprs, relationId);

	/* Make the catalog entries for the cube. */
	cubeRelationId =  cube_create(rel, chsetrel, cubeRelationName,
								  cubeInfo, typeObjectId, collationObjectId,
								  classObjectId, cubeColNames,
								  tablespaceId, reloptions,
								  stmt->if_not_exists);

	ObjectAddressSet(address, RelationRelationId, cubeRelationId);

	if (!OidIsValid(cubeRelationId))
	{
		heap_close(rel, NoLock);
		return address;
	}

	/* Force backends to reload the list of cubes on the table. */
	CacheInvalidateRelcache(rel);

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
				 Oid *collationOidP,
				 Oid *classOidP,
				 List *attList,
				 Oid relId)
{
	int			attn;
	ListCell   *lc;

	attn = 0;
	foreach (lc, attList)
	{
		IndexElem  *attribute = (IndexElem *) lfirst(lc);
		Oid			atttype;
		Oid			attcollation;

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
			attcollation = attform->attcollation;

			cubeInfo->ci_KeyAttrNumbers[attn] = attform->attnum;

			ReleaseSysCache(atttuple);
		}
		else
		{
			/*
			 * Cube expression - either it's a Var (i.e. a column in parens),
			 * or an aggregate call, or a generic expression.
			 *
			 * 
			 * FIXME this should differentiate between regular expressions (for
			 * cube dimensions) and aggregates - we only need collation/opclass
			 * for the former.
			 */
			Node	   *expr = attribute->expr;

			Assert(expr != NULL);
			atttype = exprType(expr);
			attcollation = exprCollation(expr);

			/*
			 * Strip any top-level COLLATE clause.  This ensures that we treat
			 * "x COLLATE y" and "(x COLLATE y)" alike.
			 */
			while (IsA(expr, CollateExpr))
				expr = (Node *) ((CollateExpr *) expr)->arg;

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

		/*
		 * Check we have a collation iff it's a collatable type.  The only
		 * expected failures here are (1) COLLATE applied to a noncollatable
		 * type, or (2) index expression had an unresolved collation.  But we
		 * might as well code this to be a complete consistency check.
		 */
		if (type_is_collatable(atttype))
		{
			if (!OidIsValid(attcollation))
				ereport(ERROR,
						(errcode(ERRCODE_INDETERMINATE_COLLATION),
						 errmsg("could not determine which collation to use for cube expression"),
						 errhint("Use the COLLATE clause to set the collation explicitly.")));
		}
		else
		{
			if (OidIsValid(attcollation))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("collations are not supported by type %s",
								format_type_be(atttype))));
		}

		/*
		 * Apply collation override if any
		 */
		if (attribute->collation)
			attcollation = get_collation_oid(attribute->collation, false);

		collationOidP[attn] = attcollation;

		/*
		 * Identify the opclass to use.
		 *
		 * XXX This uses default BTREE opclass.
		 */
		classOidP[attn] = GetDefaultOpClass(atttype, BTREE_AM_OID);

		if (!OidIsValid(classOidP[attn]))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("data type %s has no default btree operator class for access method",
							format_type_be(atttype))));

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
 * list of IndexElems for the columns.  This is mostly about ensuring the
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
		IndexElem  *celem = (IndexElem *) lfirst(lc);
		const char *origname;
		const char *curname;
		int			i;
		char		buf[NAMEDATALEN];

		/* Get the preliminary name from the CubeElem */
		if (celem->indexcolname)
			origname = celem->indexcolname;	/* caller-specified name */
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

/*
 * FlushChangeSet -- execute a FLUSH CHANGESET command
 *
 * This updates all cubes attached to a particular change set, and removes the
 * data from the set. This operation is MVCC-compliant and allows concurrent
 * writes to the changeset.
 *
 * To prevent multiple FLUSH CHANGESET operations running concurrently on
 * a given changeset, the operation acquires SHARE UPDATE EXCLUSIVE lock.
 */
ObjectAddress
ExecFlushChangeSet(FlushChangeSetStmt *stmt)
{
	ObjectAddress	address;
	Oid				chsetOid;
	Relation		chsetRel;

	/* changeset metadata */
	TupleDesc		chsetDesc;
	ChangeSetInfo  *chsetInfo;

	/* cubes attached to this changeset (OIDs) */
	ListCell	   *lc;
	List		   *cubes;

	/*
	 * Get a lock until end of transaction.
	 */
	chsetOid = RangeVarGetRelid(stmt->relation, ShareUpdateExclusiveLock, false);
	chsetRel = changeset_open(chsetOid, NoLock);

	/* fetch tuple descriptor for the changeset and construct ChangeSetInfo */
	chsetDesc = RelationGetDescr(chsetRel);
	chsetInfo = BuildChangeSetInfo(chsetRel);

	/* We don't allow an oid column for a changeset. */
	Assert(!chsetRel->rd_rel->relhasoids);

	/*
	 * FIXME Do we need to switch to the relation owner, similarly to REFRESH
	 * MATERIALIZED VIEW? See SetUserIdAndSecContext() in RefreshMatViewStmt.
	 */

	/* fetch list of cubes on this changeset */
	cubes = ChangeSetGetCubeList(chsetRel);

	/*
	 * We'll do one changeset scan for each cube. Perhaps that could be
	 * optimized in the future, but for now this is good enough. We're not
	 * going to lock the cubes against concurrent updates or so, as they are
	 * only receive modifications through the changeset, and we have already
	 * locked that.
	 */
	foreach (lc, cubes)
		update_cube(chsetRel, chsetInfo, chsetDesc, lfirst_oid(lc));

	/* remove the data from the changeset */
	changeset_cleanup(chsetRel);

	/* close the changelog (keep ShareUpdateExclusiveLock until commit) */
	changeset_close(chsetRel, NoLock);

	ObjectAddressSet(address, RelationRelationId, chsetOid);

	return address;
}

static void
update_cube(Relation chsetRel, ChangeSetInfo *chsetInfo, TupleDesc chsetDesc,
		   Oid cubeOid)
{
	CubeOptInfo *cube;

	TupleDesc cubeDesc;
	Tuplesortstate *tss;
	int nkeys;
	HeapTuple	htup;

	HeapScanDesc chsetScan;

	cube = build_cube_opt_info(cubeOid);

	cubeDesc = build_tupdesc_for_cube(cube, chsetInfo, chsetDesc,
									  &nkeys, NULL, NULL);

	tss = build_tss_for_cube(cubeDesc, nkeys);

	/* scan the visible part of the changeset */
	chsetScan = heap_beginscan(chsetRel, GetActiveSnapshot(), 0, NULL);

	/*
	 * Read all the tuples from the changeset, pass them to the tuplestore.
	 */
	while ((htup = heap_getnext(chsetScan, ForwardScanDirection)) != NULL)
	{
		tuplesort_putheaptuple(tss, htup);
	}

	/* Perform the sort, and then walk the sorted data and update the cube. */
	tuplesort_performsort(tss);

	/* get tuples from the sort, apply them to cube
	 *
	 * XXX We should probably require a single unique index on the cube,
	 * with exactly the cube dimensions (non-aggregate keys).
	 *
	extern HeapTuple tuplesort_getheaptuple(Tuplesortstate *state, bool forward,
					   bool *should_free); */

	/* we're done with the sort */
	tuplesort_end(tss);

	/* close the scan */
	heap_endscan(chsetScan);
}

static void
changeset_cleanup(Relation rel)
{
	HeapScanDesc	chsetScan;
	HeapTuple		htup;

	/* scan the visible part of the changeset */
	chsetScan = heap_beginscan(rel, GetActiveSnapshot(), 0, NULL);

	/*
	 * Read all the tuples from the changeset, pass them to tuplestores
	 * for all the cubes connected to this changeset, and delete them
	 * from the changeset.
	 */
	while ((htup = heap_getnext(chsetScan, ForwardScanDirection)) != NULL)
	{
		/* just delete the tuple */
		simple_heap_delete(rel, &htup->t_self);
	}

	/* close the scan */
	heap_endscan(chsetScan);
}


/*
 * ChangeSetGetCubeList -- get a list of OIDs of cubes on this changeset
 *
 * similar to RelationGetCubeList() from relcache.c
 */
static List *
ChangeSetGetCubeList(Relation changeset)
{
	Relation	cuberel;
	SysScanDesc cubescan;
	ScanKeyData skey;
	HeapTuple	htup;
	List	   *result;

	result = NIL;

	/* Prepare to scan pg_cube for entries having cubechsetid */
	ScanKeyInit(&skey,
				Anum_pg_cube_cubechsetid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(changeset)));

	cuberel = heap_open(CubeRelationId, AccessShareLock);
	cubescan = systable_beginscan(cuberel, CubeChangeSetIdIndexId, true,
								 NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(cubescan)))
	{
		Form_pg_cube cube = (Form_pg_cube) GETSTRUCT(htup);

		/* Add changeset's OID to result list in the proper order */
		result = lappend_oid(result, cube->cubeid);
	}

	systable_endscan(cubescan);

	heap_close(cuberel, AccessShareLock);

	return result;
}

/*
 * build_tss_for_cube
 * 		build a tuple descriptor for a cube
 *
 *
 * The tuple descriptor is built like this
 *
 *     (a) the change_type attribute
 *     (b) simple cube dimensions (direct references to table columns)
 *     (c) expression cube dimensions (referencing table columns)
 *     (d) attributes needed by the aggregate expressions, not in (b)
 *
 * When build like this, we know the (a), (b) and (c) columns are the
 * ones to sort and group by.
 *
 * We're not collecting attnums from dimension expressions, because we
 * need to evaluate the expressions before sorting and grouping the data,
 * so that e.g. when cube uses EXTRACT(MONTH FROM d) as one of the dims,
 * we do the grouping properly.
 *
 * This means we evaluate the expression using the tuple we get from the
 * changeset, but the aggregates using the data from tuplesort. That
 * means we need to mutate the expressions differently.
 *
 * XXX We could also extract the expressions from the aggregates and
 *     only put the results into the tuplesort.
 */
static TupleDesc
build_tupdesc_for_cube(CubeOptInfo *cube, ChangeSetInfo *chsetInfo,
					   TupleDesc chsetDesc, int *nkeys,
					   AttrNumber **attnums, Node ***expressions)
{
	int			i;
	ListCell   *lc;
	AttrNumber	attnum;
	TupleDesc	tdesc;

	int			numatts;	/* total number of attributes in descriptor */
	int			numexprs;	/* how many dimensions come from expressions */

	Bitmapset  *dimattnums;	/* attrs referenced by simple dimensions */
	Bitmapset  *aggattnums;	/* attrs referenced by aggregates */
	Bitmapset  *allattnums;	/* attrs union of the two (may overlap) */

	AttrNumber *aggmap;		/* mapping aggregates onto the tuplesort tuples */

	/* first collect attnums from simple dimensions (but skip expressions) */
	dimattnums = collect_simple_dim_attnums(cube);

	/*
	 * collect attnums needed by aggregate expressions, and also count
	 * dimension expressions at the same time
	 */
	aggattnums = analyze_cube_expressions(cube, &numexprs);

	/* combine the two sets of attributes (there may be an overlap) */
	allattnums = bms_union(dimattnums, aggattnums);

	/*
	 * The tuple descriptor needs to include the change type, all simple
	 * dimensions, expression dimensions and  and all attributes from
	 * aggregate expressions (not already included in simple dimensions).
	 */
	*nkeys  = 1 + numexprs + bms_num_members(dimattnums);
	numatts = 1 + numexprs + bms_num_members(allattnums);

	/* build mappping of attributes and simple expressions */
	*attnums = (AttrNumber*)palloc0(numatts * sizeof(AttrNumber));
	*expressions = (Node**)palloc0(numatts * sizeof(Node*));

	Assert(*nkeys <= numatts);

	tdesc = CreateTemplateTupleDesc(numatts, false);

	/*
	 * First attribute is always the change_type, so we can simply copy
	 * the attribute definition from the changeset.
	 */
	attnum = 1;
	TupleDescCopyEntry(tdesc, attnum, chsetDesc, attnum);
	attnum++;

	/* now add attributes for simple dimensions (again, skip expressions) */
	for (i = 0; i < cube->ncolumns; i++)
	{
		int k;

		/* skip cube expression (both dimensions and aggregates) */
		if (cube->cubekeys[i] == 0)
			continue;

		/* lookup the matching attribute from changeset, copy it */
		k = lookup_attnum_in_changeset(chsetInfo, cube->cubekeys[i]);

		Assert(k != InvalidAttrNumber);

		TupleDescCopyEntry(tdesc, attnum, chsetDesc, k);
		(*attnums)[attnum] = k;
		attnum++;
	}

	/* now the attributes for dimension expressions (which we'll compute) */
	foreach (lc, cube->cubeexprs)
	{
		Node *expr = (Node*)lfirst(lc);
		if (IsA(expr, Aggref))	/* skip aggregates */
			continue;

		TupleDescInitEntry(tdesc, attnum, "dim_expression", exprType(expr), 0, 0);
		expr = copyObject(expr);

		fix_changeset_vars(expr, chsetInfo);

		(*expressions)[attnum] = expr;
	}

	Assert(attnum == (*nkeys + 1));

	/* and finally the additional attributes referenced in aggregates */
	i = -1;
	while ((i = bms_next_member(allattnums, i)) >= 0)
	{
		int k;

		if (bms_is_member(i, dimattnums))
			continue;

		k = lookup_attnum_in_changeset(chsetInfo, cube->cubekeys[i]);

		Assert(k != InvalidAttrNumber);

		TupleDescCopyEntry(tdesc, attnum, chsetDesc, k);
		(*attnums)[attnum] = k;
		attnum++;

		aggmap[i] = attnum;
	}

	/* make sure we've added exactly the expected number of atttibutes */
	Assert(attnum = (numatts+1));

	bms_free(allattnums);
	bms_free(dimattnums);
	bms_free(aggattnums);

	return tdesc;
}

/*
 * default sort operators and
 * collations, with the exception of change_type where we use the
 * 'greater than' operator so that we get inserts first.
 */
static Tuplesortstate *
build_tss_for_cube(TupleDesc tdesc, int nkeys)
{
	int			i;

	AttrNumber *keys;
	Oid		   *sortops;
	Oid		   *collations;
	bool	   *nullsfirst;

	keys = (AttrNumber*)palloc0(nkeys * sizeof(AttrNumber));
	sortops = (Oid*)palloc0(nkeys * sizeof(Oid));
	collations = (Oid*)palloc0(nkeys * sizeof(Oid));
	nullsfirst = (bool*)palloc0(nkeys * sizeof(bool));

	for (i = 0; i < nkeys; i++)
	{
		keys[i] = i+1;	/* first nkeys are keys */
		get_sort_group_operators(tdesc->attrs[i]->atttypid,
								 true, false, false,
								 &sortops[i], NULL, NULL, NULL);
		collations[i] = DEFAULT_COLLATION_OID;
	}

	return tuplesort_begin_heap(tdesc,
					 nkeys, keys,
					 sortops, collations,
					 nullsfirst, work_mem, false);
}

static CubeOptInfo *
build_cube_opt_info(Oid cubeoid)
{
	Relation	cubeRelation;
	Form_pg_cube cube;
	CubeOptInfo *info;
	int			ncolumns;
	int			i;

	/*
	 * Extract info from the relation descriptor for the cube.
	 */
	cubeRelation = cube_open(cubeoid, AccessShareLock);
	cube = cubeRelation->rd_cube;

	info = makeNode(CubeOptInfo);

	info->cubeoid = cube->cubeid;
	info->reltablespace =
		RelationGetForm(cubeRelation)->reltablespace;
	/* FIXME info->rel = rel; */
	info->ncolumns = ncolumns = cube->cubenatts;
	info->cubekeys = (int *) palloc(sizeof(int) * ncolumns);

	for (i = 0; i < ncolumns; i++)
		info->cubekeys[i] = cube->cubekey.values[i];

	/*
	 * Fetch the cube expressions, if any.  We must modify the copies
	 * we obtain from the relcache to have the correct varno for the
	 * parent relation, so that they match up correctly against qual
	 * clauses.
	 */
	info->cubeexprs = RelationGetCubeExpressions(cubeRelation);

	/*
	 * FIXME we need to do this, just like in get_relation_info()
	 */
	/* if (info->cubeexprs && varno != 1)
		ChangeVarNodes((Node *) info->cubeexprs, 1, varno, 0); */

	/* Build targetlist using the completed cubeexprs data */
	// info->cubetlist = build_cube_tlist(root, info, relation);

	info->pages = RelationGetNumberOfBlocks(cubeRelation);
	// info->tuples = rel->tuples;

	cube_close(cubeRelation, AccessShareLock);

	return info;
}

static bool
fix_changeset_walker(Node *node, ChangeSetInfo *changeset)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref))	/* do not process aggregate references */
		return false;

	if (IsA(node, Var))
	{
		int i;
		bool	found = false;
		Var	   *var = (Var*)node;

		/* search for the attnum in the changeset */
		for (i = 0; i < changeset->csi_NumChangeSetAttrs; i++)
		{
			if (changeset->csi_KeyAttrNumbers[i] == var->varattno)
			{
				/* skip change type and 1-indexed */
				var->varattno = (i+2);
				break;
			}
		}

		/* make sure we found it */
		Assert(found);

		return false; /* we're done with this expression */
	}

	return expression_tree_walker(node, fix_changeset_walker,
								  (void *) changeset);
}

static void
fix_changeset_vars(Node *node, ChangeSetInfo *changeset)
{
	(void) fix_changeset_walker(node, changeset);
}

static Bitmapset *
collect_simple_dim_attnums(CubeOptInfo *cube)
{
	int i;
	Bitmapset *attnums = NULL;

	for (i = 0; i < cube->ncolumns; i++)
		if (cube->cubekeys[i] != 0)
			attnums = bms_add_member(attnums, cube->cubekeys[i]);

	return attnums;
}

static Bitmapset *
analyze_cube_expressions(CubeOptInfo *cube, int *numexprs)
{
	ListCell *lc;
	int k;
	Bitmapset *aggattnums = NULL;
	Bitmapset *result = NULL;

	*numexprs = 0;

	foreach (lc, cube->cubeexprs)
	{
		Node *expr = (Node*)lfirst(lc);
		if (IsA(expr, Aggref))
			pull_varattnos(expr, 1, &aggattnums);
		else
			/* otherwise just count the expression dimensions */
			*numexprs += 1;
	}

	/* fix the attnums (pull_varattnos offsets them because of system attrs */
	k = -1;
	while ((k = bms_next_member(aggattnums, k)) >= 0)
		result = bms_add_member(result,
								k + FirstLowInvalidHeapAttributeNumber);

	return result;
}

static AttrNumber
lookup_attnum_in_changeset(ChangeSetInfo *chsetInfo, AttrNumber attnum)
{
	int k;

	for (k = 0; k < chsetInfo->csi_NumChangeSetAttrs; k++)
	{
		if (attnum == chsetInfo->csi_KeyAttrNumbers[k])
			return (k+2);
	}

	return InvalidAttrNumber;
}
