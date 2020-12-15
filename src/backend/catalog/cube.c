/*-------------------------------------------------------------------------
 *
 * cube.c
 *	  code to create and destroy POSTGRES cube relations
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/cube.c
 *
 *
 * INTERFACE ROUTINES
 *		cube_create()		- Create a cataloged cube relation
 *		cube_drop()			- Removes cube relation from catalogs
 *		BuildCubeInfo()		- Prepare to insert cube tuples
 *		FormCubeDatum()		- Construct datum vector for one cube tuple
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "bootstrap/bootstrap.h"
#include "catalog/binary_upgrade.h"
#include "catalog/catalog.h"
#include "catalog/changeset.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_changeset.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/cubes.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_oper.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

static TupleDesc ConstructTupleDescriptor(Relation heapRelation,
						 CubeInfo *cubeInfo,
						 Oid *typeObjectId,
						 List *cubeColNames,
						 Oid *collationObjectId,
						 Oid *classObjectId);
static void UpdateCubeRelation(Oid cubeoid, Oid chsetoid, Oid heapoid,
						CubeInfo *cubeInfo, Oid *collationOids, Oid *classOids);
static Oid CubeGetRelation(Oid cubeId, bool missing_ok);

/*
 * cube_create
 *
 * heapRelation: table to build cube on (suitably locked by caller)
 * cubeRelationName: name of the cube relation
 * cubeInfo: same info executor uses to insert into the changeset
 * cubeColNames: column names to use for cube (List of char *)
 * tableSpaceId: OID of tablespace to use
 * reloptions: same as for heap
 * if_not_exists: if true, do not throw an error if a relation with
 *		the same name already exists.
 *
 * Returns the OID of the created cube.
 */
Oid
cube_create(Relation heapRelation,
			Relation changesetRelation,
			const char *cubeRelationName,
			CubeInfo *cubeInfo,
			Oid *typeObjectId,
			Oid *collationObjectId,
			Oid *classObjectId,
			List *cubeColNames,
			Oid tableSpaceId,
			Datum reloptions,
			bool if_not_exists)
{
	Oid			cubeRelationId = InvalidOid;
	Oid			chsetRelationId = RelationGetRelid(changesetRelation);
	Oid			heapRelationId = RelationGetRelid(heapRelation);
	Relation	pg_class;
	TupleDesc	cubeTupDesc;
	Oid			namespaceId;
	int			i;
	char		relpersistence;

	pg_class = table_open(RelationRelationId, RowExclusiveLock);

	/*
	 * The cube will be in the same namespace as its parent table, and it
	 * inherits the parent's relpersistence.
	 */
	namespaceId = RelationGetNamespace(heapRelation);
	relpersistence = heapRelation->rd_rel->relpersistence;

	/*
	 * check parameters
	 *
	 * XXX Maybe it'd make sense to have cubes with no key columns, to
	 * allow simple aggregates.
	 */
	if (cubeInfo->ci_NumCubeAttrs < 1)
		elog(ERROR, "cube must contain at least one columns");

	/*
	 * There must be at least one aggregate.
	 *
	 * XXX Could we allow no aggregates, to speed up DISTINCT or something
	 * like that? Although, that probably still needs at least COUNT().
	 */
	if (cubeInfo->ci_NumCubeAggregates < 1)
		elog(ERROR, "cube must contain at least one aggregate");

	/* cubes on system catalogs not allowed */
	if (IsSystemRelation(heapRelation) &&
		IsNormalProcessingMode())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cubes on system catalog tables are not supported")));

	if (get_relname_relid(cubeRelationName, namespaceId))
	{
		if (if_not_exists)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists, skipping",
							cubeRelationName)));
			table_close(pg_class, RowExclusiveLock);
			return InvalidOid;
		}

		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists",
						cubeRelationName)));
	}

	/* FIXME make sure the changeset is on the same heap table */

	/* FIXME make sure the changeset contains all necessary attributes */

	/*
	 * construct tuple descriptor for cube tuples
	 */
	cubeTupDesc = ConstructTupleDescriptor(heapRelation,
										   cubeInfo,
										   typeObjectId,
										   cubeColNames,
										   collationObjectId,
										   classObjectId);

	/*
	 * create the cube relation's relcache entry and physical disk file. (If
	 * we fail further down, it's the smgr's responsibility to remove the disk
	 * file again.)
	 */
	cubeRelationId
		= heap_create_with_catalog(cubeRelationName,
								   namespaceId,
								   tableSpaceId,
								   InvalidOid, /* relid */
								   InvalidOid, /* reltypeid */
								   InvalidOid, /* reloftypeid */
								   heapRelation->rd_rel->relowner,
								   HEAP_TABLE_AM_OID,
								   cubeTupDesc,
								   NIL,
								   RELKIND_CUBE,
								   relpersistence,
								   false, /* not shared */
								   false, /* not mapped */
								   ONCOMMIT_NOOP,
								   reloptions,
								   false, /* no ACLs */
								   false, /* not a system catalog */
								   false, /* not internal */
								   InvalidOid,
								   NULL); /* no object address */

	Assert(OidIsValid(cubeRelationId));

	/* done with pg_class */
	table_close(pg_class, RowExclusiveLock);

	/* ----------------
	 *	  update pg_cube
	 *	  (append CUBE tuple)
	 * ----------------
	 */
	UpdateCubeRelation(cubeRelationId, chsetRelationId, heapRelationId,
					   cubeInfo, collationObjectId, classObjectId);

	/*
	 * Register additional dependencies for the cube.
	 *
	 * We don't need a dependency on the namespace, because there'll be an
	 * indirect dependency via our parent table.
	 *
	 * FIXME This should rather create dependencies on the changeset.
	 *
	 * During bootstrap we can't register any dependencies, but we don't
	 * support cubes during bootstrap anyway (so error out).
	 */
	if (!IsBootstrapProcessingMode())
	{
		ObjectAddress	myself,
						referenced;

		myself.classId = RelationRelationId;
		myself.objectId = cubeRelationId;
		myself.objectSubId = 0;

		/* create auto dependencies on the referenced columns */
		for (i = 0; i < cubeInfo->ci_NumCubeAttrs; i++)
		{
			if (cubeInfo->ci_KeyAttrNumbers[i] != 0)
			{
				referenced.classId = RelationRelationId;
				referenced.objectId = heapRelationId;
				referenced.objectSubId = cubeInfo->ci_KeyAttrNumbers[i];

				recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
			}
			/* FIXME handle expressions here (see how index_create does that) */
		}
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("cubes not supported during bootstap")));

	/*
	 * Advance the command counter so that we can see the newly-entered
	 * catalog tuples for the cube.
	 */
	CommandCounterIncrement();

	return cubeRelationId;

}

void
cube_drop(Oid cubeId)
{
	Oid			heapId;
	HeapTuple	tuple;
	Relation	cubeRelation;
	Relation	userHeapRelation;
	Relation	userCubeRelation;
	LOCKMODE	lockmode = AccessExclusiveLock;

	heapId = CubeGetRelation(cubeId, false);

	/*
	 * To drop a cube safely, we must grab exclusive lock on its parent
	 * table.  Exclusive lock on the cube alone is insufficient because
	 * another backend might be about to execute a query on the parent table.
	 *
	 * XXX copied from index_drop, see details there
	 */
	userHeapRelation = table_open(heapId, lockmode);
	userCubeRelation = table_open(cubeId, lockmode);

	/*
	 * Close and flush the changeset's relcache entry, to ensure relcache
	 * doesn't try to rebuild it while we're deleting catalog entries. We
	 * keep the lock though.
	 */
	table_close(userCubeRelation, NoLock);

	RelationForgetRelation(cubeId);

	/*
	 * fix CUBE relation
	 */
	cubeRelation = table_open(CubeRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(CUBEOID, ObjectIdGetDatum(cubeId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for cube %u", cubeId);

	CatalogTupleDelete(cubeRelation, &tuple->t_self);

	ReleaseSysCache(tuple);
	table_close(cubeRelation, RowExclusiveLock);

	/*
	 * fix ATTRIBUTE relation
	 */
	DeleteAttributeTuples(cubeId);

	/*
	 * fix RELATION relation
	 */
	DeleteRelationTuple(cubeId);

	/*
	 * We must send out a shared-cache-inval notice on the owning relation
	 * to ensure other backends update their relcache lists of changesets.
	 */
	CacheInvalidateRelcache(userHeapRelation);

	/*
	 * Close owning rel, but keep lock
	 */
	table_close(userHeapRelation, NoLock);
}

static TupleDesc
ConstructTupleDescriptor(Relation heapRelation,
						 CubeInfo *cubeInfo,
						 Oid *typeObjectId,
						 List *cubeColNames,
						 Oid *collationObjectId,
						 Oid *classObjectId)
{
	int			numatts = cubeInfo->ci_NumCubeAttrs;
	int			numaggs = cubeInfo->ci_NumCubeAggregates;

	ListCell   *colnames_item = list_head(cubeColNames);
	ListCell   *cubeexpr_item = list_head(cubeInfo->ci_Expressions);
	TupleDesc	heapTupDesc;
	TupleDesc	cubeTupDesc;
	int			natts; 			/* #atts in heap rel */
	int			i;

	/* we need access to the table's tuple descriptor */
	heapTupDesc = RelationGetDescr(heapRelation);
	natts = RelationGetForm(heapRelation)->relnatts;

	/*
	 * allocate the new tuple descriptor
	 */
	cubeTupDesc = CreateTemplateTupleDesc(numatts + numaggs);

	/*
	 * Cubes can contain both simple columns and expressions (either regular
	 * expressions or aggregate references).
	 *
	 * For simple cube columns, we copy the pg_attribute row from the parent
	 * relation and modify it as necessary.
	 *
	 * For expressions we have to construct a pg_attribute row the hard way.
	 */
	for (i = 0; i < numatts; i++)
	{
		AttrNumber	atnum = cubeInfo->ci_KeyAttrNumbers[i];
		Oid			keyType;
		HeapTuple	tuple;
		Form_pg_type typeTup;
		Form_pg_attribute to = &cubeTupDesc->attrs[i];

		/* simple column (no system attributes) */
		if ((atnum > 0) && (atnum <= natts))
		{
			/* Simple index column */
			Form_pg_attribute from;

			/* normal attribute (1...n)	*/
			if (atnum > natts)		/* safety check */
				elog(ERROR, "invalid column number %d", atnum);

			from = &heapTupDesc->attrs[AttrNumberGetAttrOffset(atnum)];

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
		}
		else if (atnum == 0) /* expression as a cube column */
		{
			Node   *cubekey;

			MemSet(to, 0, ATTRIBUTE_FIXED_PART_SIZE);

			if (cubeexpr_item == NULL)	/* shouldn't happen */
				elog(ERROR, "too few entries in cubeexprs list");

			cubekey = (Node *) lfirst(cubeexpr_item);
			cubeexpr_item = lnext(cubeInfo->ci_Expressions, cubeexpr_item);

			/*
			 * Lookup the expression type in pg_type for the type length etc.
			 */
			keyType = typeObjectId[i];
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(keyType));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for type %u", keyType);
			typeTup = (Form_pg_type) GETSTRUCT(tuple);

			/*
			 * Assign some of the attributes values. Leave the rest as 0.
			 */
			to->attnum = i + 1;
			to->atttypid = keyType;
			to->attlen = typeTup->typlen;
			to->attbyval = typeTup->typbyval;
			to->attstorage = typeTup->typstorage;
			to->attalign = typeTup->typalign;
			to->attstattarget = -1;
			to->attcacheoff = -1;
			to->atttypmod = exprTypmod(cubekey);
			to->attislocal = true;
			to->attcollation = InvalidOid;	/* FIXME maybe collation? */

			ReleaseSysCache(tuple);

			/*
			 * Make sure the expression yields a type that's safe to store in
			 * a cube.
			 *
			 * FIXME Do we need this?
			 */
			CheckAttributeType(NameStr(to->attname),
							   to->atttypid, to->attcollation,
							   NIL, false);
		}
		else
			elog(ERROR, "invalid attribute number %d", atnum);

		/*
		 * We do not yet have the correct relation OID for the cube, so just
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
		colnames_item = lnext(cubeColNames, colnames_item);
	}

	/* now append the aggregates */
	cubeexpr_item = list_head(cubeInfo->ci_Aggregates);

	for (i = 0; i < numaggs; i++)
	{
		HeapTuple	tuple;
		Form_pg_type	typeTup;
		Oid			keyType;
		Form_pg_attribute to = &cubeTupDesc->attrs[numatts + i];
		Form_pg_aggregate aggForm;

		Node   *expr;
		Aggref *aggref;

		MemSet(to, 0, ATTRIBUTE_FIXED_PART_SIZE);

		if (cubeexpr_item == NULL)	/* shouldn't happen */
			elog(ERROR, "too few entries in cubeexprs list");

		expr = (Node *) lfirst(cubeexpr_item);
		cubeexpr_item = lnext(cubeInfo->ci_Aggregates, cubeexpr_item);

		Assert(IsA(expr, Aggref));

		aggref = (Aggref *) expr;

		/* info about moving aggregate */
		tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggref->aggfnoid));
		if (!HeapTupleIsValid(tuple))		/* should not happen */
			elog(ERROR, "cache lookup failed for aggregate %u", aggref->aggfnoid);

		aggForm = (Form_pg_aggregate) GETSTRUCT(tuple);

		keyType = aggForm->aggmtranstype;

		ReleaseSysCache(tuple);

		/*
		 * Lookup the mtranstype info in pg_type for the aggregate.
		 */
		tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(keyType));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for type %u", keyType);
		typeTup = (Form_pg_type) GETSTRUCT(tuple);

		/*
		 * Assign some of the attributes values. Leave the rest as 0.
		 */
		to->attnum = numatts + i + 1;
		to->atttypid = keyType;
		to->attlen = typeTup->typlen;
		to->attbyval = typeTup->typbyval;
		to->attstorage = typeTup->typstorage;
		to->attalign = typeTup->typalign;
		to->attstattarget = -1;
		to->attcacheoff = -1;
		to->atttypmod = -1;	/* FIXME? */
		to->attislocal = true;
		to->attcollation = InvalidOid; /* FIXME? */

		ReleaseSysCache(tuple);

		/* generate fake column mames */
		sprintf(NameStr(to->attname), "agg_%d", i+1);

		/*
		 * Make sure the expression yields a type that's safe to store in
		 * a cube.
		 *
		 * FIXME Do we need this?
		 */
		CheckAttributeType(NameStr(to->attname),
						   to->atttypid, to->attcollation,
						   NIL, false);
	}


	return cubeTupDesc;

}

static void
UpdateCubeRelation(Oid cubeoid, Oid chsetoid, Oid heapoid,
				   CubeInfo *cubeInfo, Oid *collationOids, Oid *classOids)
{
	int2vector *cubekey;
	oidvector  *cubecollation;
	oidvector  *cubeclass;
	Datum		values[Natts_pg_cube];
	bool		nulls[Natts_pg_cube];
	Relation	pg_cube;
	HeapTuple	tuple;
	Datum		exprsDatum;
	Datum		aggsDatum;
	int			i;

	/* copy the cube key info into arrays */
	cubekey = buildint2vector(NULL, cubeInfo->ci_NumCubeAttrs);
	for (i = 0; i < cubeInfo->ci_NumCubeAttrs; i++)
		cubekey->values[i] = cubeInfo->ci_KeyAttrNumbers[i];

	cubecollation = buildoidvector(collationOids, cubeInfo->ci_NumCubeAttrs);
	cubeclass = buildoidvector(classOids, cubeInfo->ci_NumCubeAttrs);

	/*
	 * Convert the cube expressions (if any) to a text datum
	 */
	if (cubeInfo->ci_Expressions != NIL)
	{
		char	   *exprsString;

		exprsString = nodeToString(cubeInfo->ci_Expressions);
		exprsDatum = CStringGetTextDatum(exprsString);
		pfree(exprsString);
	}
	else
		exprsDatum = (Datum) 0;

	/*
	 * Convert the cube aggregates to a text datum (there have to be some).
	 */
	if (cubeInfo->ci_Aggregates != NIL)
	{
		char	   *exprsString;

		exprsString = nodeToString(cubeInfo->ci_Aggregates);
		aggsDatum = CStringGetTextDatum(exprsString);
		pfree(exprsString);
	}
	else
		elog(ERROR, "missing cube aggregates");

	/* open the system catalog cube relation */
	pg_cube = table_open(CubeRelationId, RowExclusiveLock);

	/* build a pg_cube tuple */
	MemSet(nulls, false, sizeof(nulls));

	values[Anum_pg_cube_cubeid      - 1] = ObjectIdGetDatum(cubeoid);
	values[Anum_pg_cube_cuberelid   - 1] = ObjectIdGetDatum(heapoid);
	values[Anum_pg_cube_cubechsetid - 1] = ObjectIdGetDatum(chsetoid);
	values[Anum_pg_cube_cubenatts   - 1] = Int16GetDatum(cubeInfo->ci_NumCubeAttrs);
	values[Anum_pg_cube_cubenaggs   - 1] = Int16GetDatum(cubeInfo->ci_NumCubeAggregates);
	values[Anum_pg_cube_cubekey     - 1] = PointerGetDatum(cubekey);

	values[Anum_pg_cube_cubecollation - 1] = PointerGetDatum(cubecollation);
	values[Anum_pg_cube_cubeclass     - 1] = PointerGetDatum(cubeclass);

	values[Anum_pg_cube_cubeexprs   - 1] = exprsDatum;
	if (exprsDatum == (Datum) 0)
		nulls[Anum_pg_cube_cubeexprs - 1] = true;

	values[Anum_pg_cube_cubeaggs    - 1] = aggsDatum;

	tuple = heap_form_tuple(RelationGetDescr(pg_cube), values, nulls);

	/* insert the tuple into the pg_cube catalog */
	CatalogTupleInsert(pg_cube, tuple);

	/* free the tuple and close the relation */
	heap_freetuple(tuple);
	table_close(pg_cube, RowExclusiveLock);
}

/* ----------------
 *		BuildCubeInfo
 *			Construct an CubeInfo record for an open cube
 *
 * CubeInfo stores the information about the cube that's needed by 
 * FormCubeDatum, which is used for insertion of tuples. Normally we
 * build an CubeInfo for a cube just once per command, and then use it
 * for (potentially) many tuples.
 * ----------------
 */
CubeInfo *
BuildCubeInfo(Relation cube)
{
	CubeInfo  *ci = makeNode(CubeInfo);
	Form_pg_cube cubeStruct = cube->rd_cube;
	int			i;
	int			numKeys;
	int			numAggs;

	/* check the number of keys, and copy attr numbers into the CubeInfo */
	numKeys = cubeStruct->cubenatts;
	numAggs = cubeStruct->cubenaggs;

	if (numKeys < 1)
		elog(ERROR, "invalid cubenatts %d for cube %u",
			 numKeys, RelationGetRelid(cube));

	ci->ci_NumCubeAttrs = numKeys;
	ci->ci_KeyAttrNumbers = palloc0(sizeof(AttrNumber) * numKeys);

	if (numAggs < 1)
		elog(ERROR, "invalid cubenaggs %d for cube %u",
			 numAggs, RelationGetRelid(cube));

	ci->ci_NumCubeAggregates = cubeStruct->cubenaggs;

	for (i = 0; i < numKeys; i++)
		ci->ci_KeyAttrNumbers[i] = cubeStruct->cubekey.values[i];

	/* fetch any expressions needed for cube */
	ci->ci_Expressions = RelationGetCubeExpressions(cube);
	ci->ci_ExpressionsState = NIL;

	/* fetch any aggregates needed for cube */
	ci->ci_Aggregates = RelationGetCubeAggregates(cube);

	return ci;
}


typedef struct PerAggState {

	Aggref *aggref;
	List   *args;

	/* info about aggregate functions */
	Oid			transfn_oid;
	Oid			invtransfn_oid;
	Oid			finalfn_oid;
	bool		finalextra;
	char		finalmodify;
	Oid			transtype;
	Datum		initValue;
	bool		initValueIsNull;

	/*
	 * fmgr lookup data for transition function(s).
	 */
	FmgrInfo	transfn;
	FmgrInfo	invtransfn;
	FmgrInfo	finalfn;

	/* */
	Datum		transValue;
	bool		transtypeByVal;
	int16		transtypeLen;
	bool		transValueIsNull;
	bool		noTransValue;

} PerAggState;

static void
reset_group(PerAggState *aggs, int naggs)
{
	for (int i; i < naggs; i++)
	{
		/*
		 * (Re)set transValue to the initial value.
		 *
		 * Note that when the initial value is pass-by-ref, we must copy it (into
		 * the aggcontext) since we will pfree the transValue later.
		 */
		if (aggs[i].initValueIsNull)
			aggs[i].transValue = aggs[i].initValue;
		else
		{
			aggs[i].transValue = datumCopy(aggs[i].initValue,
										   aggs[i].transtypeByVal,
										   aggs[i].transtypeLen);
		}
		aggs[i].transValueIsNull = aggs[i].initValueIsNull;

		aggs[i].noTransValue = aggs[i].initValueIsNull;
	}
}

static Datum
GetAggInitVal(Datum textInitVal, Oid transtype)
{
	Oid			typinput,
				typioparam;
	char	   *strInitVal;
	Datum		initVal;

	getTypeInputInfo(transtype, &typinput, &typioparam);
	strInitVal = TextDatumGetCString(textInitVal);
	initVal = OidInputFunctionCall(typinput, strInitVal,
								   typioparam, -1);
	pfree(strInitVal);
	return initVal;
}

static void
init_agg_state(PerAggState *state, Aggref *aggref)
{
	HeapTuple	aggTuple;
	Datum		textInitVal;
	Form_pg_aggregate aggform;

	state->aggref = aggref;

	/* Fetch the pg_aggregate row */
	aggTuple = SearchSysCache1(AGGFNOID,
							   ObjectIdGetDatum(aggref->aggfnoid));
	if (!HeapTupleIsValid(aggTuple))
		elog(ERROR, "cache lookup failed for aggregate %u",
			 aggref->aggfnoid);
	aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);

	state->transfn_oid = aggform->aggmtransfn;
	state->invtransfn_oid = aggform->aggminvtransfn;
	state->finalfn_oid = aggform->aggmfinalfn;
	state->finalextra = aggform->aggmfinalextra;
	state->finalmodify = aggform->aggmfinalmodify;
	state->transtype = aggform->aggmtranstype;
	state->args = NIL;

	fmgr_info(state->transfn_oid, &state->transfn);
	fmgr_info(state->invtransfn_oid, &state->invtransfn);
	fmgr_info(state->finalfn_oid, &state->finalfn);

	/*
	 * initval is potentially null, so don't try to access it as a
	 * struct field. Must do it the hard way with SysCacheGetAttr.
	 */
	textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple,
								  Anum_pg_aggregate_aggminitval,
								  &state->initValueIsNull);
	if (state->initValueIsNull)
		state->initValue = (Datum) 0;
	else
		state->initValue = GetAggInitVal(textInitVal,
										 state->transtype);

	get_typlenbyval(state->transtype,
					&state->transtypeLen,
					&state->transtypeByVal);

	ReleaseSysCache(aggTuple);
}

/*
 * CubeGetRelation: given a cube's relation OID, get the OID of
 * the relation it is a cube on.  Uses the system cache.
 */
static Oid
CubeGetRelation(Oid cubeId, bool missing_ok)
{
	HeapTuple	tuple;
	Form_pg_cube cube;
	Oid			result;

	tuple = SearchSysCache1(CUBEOID, ObjectIdGetDatum(cubeId));
	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
			return InvalidOid;
		elog(ERROR, "cache lookup failed for cube %u", cubeId);
	}
	cube = (Form_pg_cube) GETSTRUCT(tuple);
	Assert(cube->cubeid == cubeId);

	result = cube->cuberelid;
	ReleaseSysCache(tuple);
	return result;
}

static void
cube_rebuild(Oid cubeId)
{
	Oid			heapOid;
	Relation	heapRelation;
	Relation	cubeRelation;
	TableScanDesc scan;
	CubeInfo   *cube;

	/*  */
	Tuplesortstate *tss;
	TupleTableSlot *slot;

	int				sortNKeys;
	AttrNumber	   *sortKeys;
	Oid			   *sortOperators;
	Oid			   *sortCollations;
	bool		   *sortNullsFirst;

	/* expressions in arguments for aggregates */
	int			nargs;
	List	   *agg_args = NIL;
	TupleDesc	tss_desc;
	TupleTableSlot *tss_slot;
	TupleDesc	tdesc;
	PerAggState *aggs;

	Datum	   *values;
	bool	   *isnull;

	heapOid = CubeGetRelation(cubeId, false);

	/*
	 * To rebuild a cube, we grab exclusive lock on the cube (so that we
	 * can truncate it), and a share lock on the heap relation.
	 */
	heapRelation = table_open(heapOid, AccessShareLock);
	cubeRelation = table_open(cubeId, AccessExclusiveLock);

	tdesc = RelationGetDescr(heapRelation);
	cube = BuildCubeInfo(cubeRelation);

	/* truncate the cube */
	heap_truncate_one_rel(cubeRelation);

	/* input slot, used to read stuff from the heap table */
	slot = MakeSingleTupleTableSlot(tdesc,
									table_slot_callbacks(heapRelation));

	/* build info needed to transform input tuples for the tuplesort */
	{
		ListCell *lc, *lc2;
		ListCell   *cubeexpr_item;
		AttrNumber	attnum;
		int			idx;

		/* first extract expressions from aggregates */
		nargs = 0;
		foreach (lc, cube->ci_Aggregates)
		{
			Aggref *aggref = (Aggref *) lfirst(lc);

			nargs += list_length(aggref->args);
		}

		/* ok, let's build the tuplesort info (arrays and tupledesc) */
		tss_desc = CreateTemplateTupleDesc(cube->ci_NumCubeAttrs + nargs);

		sortNKeys = cube->ci_NumCubeAttrs;
		sortKeys = (AttrNumber *) palloc(sizeof(AttrNumber) * sortNKeys);
		sortOperators = (Oid *) palloc(sizeof(Oid) * sortNKeys);
		sortCollations = (Oid *) palloc(sizeof(Oid) * sortNKeys);
		sortNullsFirst = (bool *) palloc(sizeof(bool) * sortNKeys);

		cubeexpr_item = list_head(cube->ci_Expressions);

		/* we stash the sort theys at the beginning */
		attnum = 0;
		for (int i = 0; i < cube->ci_NumCubeAttrs; i++)
		{
			Oid	collation = DEFAULT_COLLATION_OID;

			attnum++;

			/* just a reference to an attribute in heap relation */
			if (cube->ci_KeyAttrNumbers[i] != 0)
				TupleDescCopyEntry(tss_desc, attnum,
								   tdesc, cube->ci_KeyAttrNumbers[i]);
			else
			{
				Node *expr;

				expr = (Node *) lfirst(cubeexpr_item);
				cubeexpr_item = lnext(cube->ci_Expressions, cubeexpr_item);

				collation = exprCollation(expr);

				TupleDescInitEntry(tss_desc, attnum, "expr",
								   exprType(expr),
								   exprTypmod(expr),
								   0);
			}

			sortKeys[i] = attnum;

			get_sort_group_operators(tss_desc->attrs[i].atttypid,
									 true, false, false,
									 &sortOperators[i],
									 NULL, NULL, NULL);

			sortCollations[i] = collation;
			sortNullsFirst[i] = false;
		}

		aggs = (PerAggState *) palloc(sizeof(PerAggState) * cube->ci_NumCubeAggregates);
		agg_args = NIL;

		/* also add arguments for aggregates */
		idx = 0;
		foreach (lc, cube->ci_Aggregates)
		{
			Aggref *aggref = (Aggref *) lfirst(lc);

			init_agg_state(&aggs[idx], aggref);

			elog(WARNING, "agg %d => %d", idx, aggs[idx].transtype);

			foreach (lc2, aggref->args)
			{
				TargetEntry *te = (TargetEntry *) lfirst(lc2);
				Node *expr = (Node *) te->expr;

				agg_args = lappend(agg_args, expr);

				attnum++;

				TupleDescInitEntry(tss_desc, attnum, "expr",
								   exprType(expr),
								   exprTypmod(expr),
								   0);

				aggs[idx].args = lappend_int(aggs[idx].args, attnum);
			}

			idx++;
		}
	}

	values = (Datum *) palloc(sizeof(Datum) * tss_desc->natts);
	isnull = (bool *) palloc(sizeof(bool) * tss_desc->natts);

	/* slot for tuplesort */
	tss_slot = MakeSingleTupleTableSlot(tss_desc, &TTSOpsMinimalTuple);

	/* load data from the source table, sort them by keys */
	scan = table_beginscan(heapRelation, GetActiveSnapshot(), 0, NULL);

	/* build TSS with just enough columns from the heap relation */
	tss = tuplesort_begin_heap(tss_desc,
							   sortNKeys,
							   sortKeys,
							   sortOperators,
							   sortCollations,
							   sortNullsFirst,
							   work_mem,
							   NULL,
							   false);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		int	idx;
		ListCell *lc;
		HeapTuple tuple;

		memset(values, 0, sizeof(Datum) * tss_desc->natts);
		memset(isnull, 0, sizeof(bool) * tss_desc->natts);

		/* Build tuple in the tts_slot. */
		slot_getsomeattrs(slot, tdesc->natts);

		/* we stash the sort theys at the beginning */
		idx = 0;
		for (int i = 0; i < cube->ci_NumCubeAttrs; i++)
		{
			/* just a reference to an attribute in heap relation */
			if (cube->ci_KeyAttrNumbers[i] != 0)
			{
				AttrNumber src = cube->ci_KeyAttrNumbers[i];

				values[idx] = slot->tts_values[src-1];
				isnull[idx] = slot->tts_isnull[src-1];
			}
			else
			{
				elog(ERROR, "FIXME");
			}

			idx++;
		}

		/* also add arguments for aggregates */
		foreach (lc, agg_args)
		{
			Node *expr = (Node *) lfirst(lc);
			Var *var;
			AttrNumber src;

			if (!IsA(expr, Var))
				elog(ERROR, "FIXME");

			var = (Var *) expr;
			src = var->varattno;

			values[idx] = slot->tts_values[src-1];
			isnull[idx] = slot->tts_isnull[src-1];

			idx++;
		}

		tuple = heap_form_tuple(tss_desc, values, isnull);

		ExecForceStoreHeapTuple(tuple, tss_slot, false);

		tuplesort_puttupleslot(tss, tss_slot);
	}

	table_endscan(scan);

	/* sort the data */
	tuplesort_performsort(tss);

	/* read data from the sorted state */
	{
		HeapTuple	tup;
		HeapTuple	groupTuple = NULL;

		reset_group(aggs, cube->ci_NumCubeAggregates);

		while (tuplesort_gettupleslot(tss, true, false, tss_slot, NULL))
		{
			bool	newgroup = false;

			tup = ExecFetchSlotHeapTuple(tss_slot, true, NULL);

			/* if no group tuple, copy the new tuple */
			if (!groupTuple)
				groupTuple = heap_copytuple(tup);

			/* are we in the same group as groupTuple? */
			for (int i = 0; i < sortNKeys; i++)
			{
				FmgrInfo	finfo;
				Datum		value1, value2;
				bool		isnull1, isnull2;
				TypeCacheEntry *typcache;
				Oid			collation = tss_desc->attrs[i].attcollation;

				value1 = heap_getattr(groupTuple, (i + 1), tss_desc, &isnull1);
				value2 = heap_getattr(tup, (i + 1), tss_desc, &isnull2);

				/* only one is NULL */
				if (isnull1 != isnull2)
				{
					newgroup = true;
					break;
				}

				/* both values are NULL */
				if (isnull1 && isnull2)
					continue;

				/* both non-NULL */
				typcache = lookup_type_cache(tss_desc->attrs[i].atttypid,
										TYPECACHE_EQ_OPR_FINFO);

				Assert(OidIsValid(typcache->eq_opr_finfo.fn_oid));

				fmgr_info(typcache->eq_opr_finfo.fn_oid, &finfo);

				if (!DatumGetBool(FunctionCall2Coll(&finfo, collation,
													value1, value2)))
				{
					newgroup = true;
					break;
				}
			}

			if (newgroup)
			{
				reset_group(aggs, cube->ci_NumCubeAggregates);
				elog(WARNING, "new group");
				groupTuple = heap_copytuple(tup);
			}
		}
	}

	/* build info for sorted aggregation */

	/* do the aggregation, insert pre-aggregated data into the cube */


	/* we're done with the sort */
	tuplesort_end(tss);

	/* get rid of the slots */
	ExecDropSingleTupleTableSlot(slot);
	ExecDropSingleTupleTableSlot(tss_slot);

	/* unlock the relations */
	table_close(heapRelation, AccessShareLock);
	table_close(cubeRelation, AccessExclusiveLock);
}

Datum
pg_rebuild_cube(PG_FUNCTION_ARGS)
{
	Oid	cubeId = PG_GETARG_OID(0);

	cube_rebuild(cubeId);

	PG_RETURN_VOID();
}
