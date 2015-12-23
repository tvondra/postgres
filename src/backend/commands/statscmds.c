/*-------------------------------------------------------------------------
 *
 * statscmds.c
 *	  Commands for creating and altering multivariate statistics
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/statscmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_mv_statistic.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "catalog/storage.h"
#include "catalog/toasting.h"
#include "commands/cluster.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/policy.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "commands/user.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "parser/parser.h"
#include "pgstat.h"
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/predicate.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/typcache.h"
#include "utils/mvstats.h"


/* used for sorting the attnums in ExecCreateStatistics */
static int compare_int16(const void *a, const void *b)
{
	return memcmp(a, b, sizeof(int16));
}

/*
 * Implements the CREATE STATISTICS name ON table (columns) WITH (options)
 *
 * TODO Check that the types support sort, although maybe we can live
 *      without it (and only build MCV list / association rules).
 *
 * TODO This should probably check for duplicate stats (i.e. same
 *      keys, same options). Although maybe it's useful to have
 *      multiple stats on the same columns with different options
 *      (say, a detailed MCV-only stats for some queries, histogram
 *      for others, etc.)
 */
ObjectAddress
CreateStatistics(CreateStatsStmt *stmt)
{
	int			i, j;
	ListCell   *l;
	int16		attnums[INDEX_MAX_KEYS];
	int			numcols = 0;
	ObjectAddress	address = InvalidObjectAddress;
	char	   *namestr;
	NameData	staname;
	Oid			statoid;
	Oid			namespaceId;

	HeapTuple	htup;
	Datum		values[Natts_pg_mv_statistic];
	bool		nulls[Natts_pg_mv_statistic];
	int2vector *stakeys;
	Relation	mvstatrel;
	Relation	rel;
	ObjectAddress parentobject, childobject;

	/* by default build nothing */
	bool 	build_dependencies = false,
			build_mcv = false,
			build_histogram = false,
			build_ndistinct = false;

	int32 	max_buckets = -1,
			max_mcv_items = -1;

	/* options required because of other options */
	bool	require_mcv = false,
			require_histogram = false;

	Assert(IsA(stmt, CreateStatsStmt));

	/* resolve the pieces of the name (namespace etc.) */
	namespaceId = QualifiedNameGetCreationNamespace(stmt->defnames, &namestr);
	namestrcpy(&staname, namestr);

	/*
	 * If if_not_exists was given and the statistics already exists, bail out.
	 */
	if (stmt->if_not_exists &&
		SearchSysCacheExists2(MVSTATNAMENSP,
							  PointerGetDatum(&staname),
							  ObjectIdGetDatum(namespaceId)))
	{
		ereport(NOTICE,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("statistics \"%s\" already exists, skipping",
						namestr)));
		return InvalidObjectAddress;
	}

	rel = heap_openrv(stmt->relation, AccessExclusiveLock);

	/* transform the column names to attnum values */

	foreach(l, stmt->keys)
	{
		char	   *attname = strVal(lfirst(l));
		HeapTuple	atttuple;

		atttuple = SearchSysCacheAttName(RelationGetRelid(rel), attname);

		if (!HeapTupleIsValid(atttuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" referenced in statistics does not exist",
							attname)));

		/* more than MVHIST_MAX_DIMENSIONS columns not allowed */
		if (numcols >= MVSTATS_MAX_DIMENSIONS)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_COLUMNS),
					 errmsg("cannot have more than %d keys in a statistics",
							MVSTATS_MAX_DIMENSIONS)));

		attnums[numcols] = ((Form_pg_attribute) GETSTRUCT(atttuple))->attnum;
		ReleaseSysCache(atttuple);
		numcols++;
	}

	/*
	 * Check the lower bound (at least 2 columns), the upper bound was
	 * already checked in the loop.
	 */
	if (numcols < 2)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_COLUMNS),
					 errmsg("multivariate stats require 2 or more columns")));

	/* look for duplicities */
	for (i = 0; i < numcols; i++)
		for (j = 0; j < numcols; j++)
			if ((i != j) && (attnums[i] == attnums[j]))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("duplicate column name in statistics definition")));

	/* parse the statistics options */
	foreach (l, stmt->options)
	{
		DefElem *opt = (DefElem*)lfirst(l);

		if (strcmp(opt->defname, "dependencies") == 0)
			build_dependencies = defGetBoolean(opt);
		else if (strcmp(opt->defname, "ndistinct") == 0)
			build_ndistinct = defGetBoolean(opt);
		else if (strcmp(opt->defname, "mcv") == 0)
			build_mcv = defGetBoolean(opt);
		else if (strcmp(opt->defname, "max_mcv_items") == 0)
		{
			max_mcv_items = defGetInt32(opt);

			/* this option requires 'mcv' to be enabled */
			require_mcv = true;

			/* sanity check */
			if (max_mcv_items < MVSTAT_MCVLIST_MIN_ITEMS)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("max number of MCV items must be at least %d",
								MVSTAT_MCVLIST_MIN_ITEMS)));

			else if (max_mcv_items > MVSTAT_MCVLIST_MAX_ITEMS)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("max number of MCV items is %d",
								MVSTAT_MCVLIST_MAX_ITEMS)));

		}
		else if (strcmp(opt->defname, "histogram") == 0)
			build_histogram = defGetBoolean(opt);
		else if (strcmp(opt->defname, "max_buckets") == 0)
		{
			max_buckets = defGetInt32(opt);

			/* this option requires 'histogram' to be enabled */
			require_histogram = true;

			/* sanity check */
			if (max_buckets < MVSTAT_HIST_MIN_BUCKETS)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("minimum number of buckets is %d",
								MVSTAT_HIST_MIN_BUCKETS)));

			else if (max_buckets > MVSTAT_HIST_MAX_BUCKETS)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("maximum number of buckets is %d",
								MVSTAT_HIST_MAX_BUCKETS)));

		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized STATISTICS option \"%s\"",
							opt->defname)));
	}

	/* check that at least some statistics were requested */
	if (! (build_dependencies || build_mcv || build_histogram || build_ndistinct))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("no statistics type (dependencies, mcv, histogram, ndistinct) was requested")));

	/* now do some checking of the options */
	if (require_mcv && (! build_mcv))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("option 'mcv' is required by other options(s)")));

	if (require_histogram && (! build_histogram))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("option 'histogram' is required by other options(s)")));

	/* sort the attnums and build int2vector */
	qsort(attnums, numcols, sizeof(int16), compare_int16);
	stakeys = buildint2vector(attnums, numcols);

	/*
	 * Okay, let's create the pg_mv_statistic entry.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	/* no stats collected yet, so just the keys */
	values[Anum_pg_mv_statistic_starelid-1] = ObjectIdGetDatum(RelationGetRelid(rel));
	values[Anum_pg_mv_statistic_staname -1] = NameGetDatum(&staname);
	values[Anum_pg_mv_statistic_stanamespace -1] = ObjectIdGetDatum(namespaceId);

	values[Anum_pg_mv_statistic_stakeys -1] = PointerGetDatum(stakeys);

	values[Anum_pg_mv_statistic_deps_enabled -1] = BoolGetDatum(build_dependencies);
	values[Anum_pg_mv_statistic_mcv_enabled  -1] = BoolGetDatum(build_mcv);
	values[Anum_pg_mv_statistic_hist_enabled -1] = BoolGetDatum(build_histogram);
	values[Anum_pg_mv_statistic_ndist_enabled-1] = BoolGetDatum(build_ndistinct);

	values[Anum_pg_mv_statistic_mcv_max_items    -1] = Int32GetDatum(max_mcv_items);
	values[Anum_pg_mv_statistic_hist_max_buckets -1] = Int32GetDatum(max_buckets);

	nulls[Anum_pg_mv_statistic_stadeps  -1] = true;
	nulls[Anum_pg_mv_statistic_stamcv   -1] = true;
	nulls[Anum_pg_mv_statistic_stahist  -1] = true;
	nulls[Anum_pg_mv_statistic_standist -1] = true;

	/* insert the tuple into pg_mv_statistic */
	mvstatrel = heap_open(MvStatisticRelationId, RowExclusiveLock);

	htup = heap_form_tuple(mvstatrel->rd_att, values, nulls);

	simple_heap_insert(mvstatrel, htup);

	CatalogUpdateIndexes(mvstatrel, htup);

	statoid = HeapTupleGetOid(htup);

	heap_freetuple(htup);


	/*
	 * Store a dependency too, so that statistics are dropped on DROP TABLE
	 */
	parentobject.classId = RelationRelationId;
	parentobject.objectId = ObjectIdGetDatum(RelationGetRelid(rel));
	parentobject.objectSubId = 0;
	childobject.classId = MvStatisticRelationId;
	childobject.objectId = statoid;
	childobject.objectSubId = 0;

	recordDependencyOn(&childobject, &parentobject, DEPENDENCY_AUTO);

	/*
	 * Also record dependency on the schema (to drop statistics on DROP SCHEMA)
	 */
	parentobject.classId = NamespaceRelationId;
	parentobject.objectId = ObjectIdGetDatum(namespaceId);
	parentobject.objectSubId = 0;
	childobject.classId = MvStatisticRelationId;
	childobject.objectId = statoid;
	childobject.objectSubId = 0;

	recordDependencyOn(&childobject, &parentobject, DEPENDENCY_AUTO);


	heap_close(mvstatrel, RowExclusiveLock);

	relation_close(rel, NoLock);

	/*
	 * Invalidate relcache so that others see the new statistics.
	 */
	CacheInvalidateRelcache(rel);

	ObjectAddressSet(address, MvStatisticRelationId, statoid);

	return address;
}


/*
 * Implements the DROP STATISTICS
 *
 *     DROP STATISTICS stats_name ON table_name
 *
 * The first one requires an exact match, the second one just drops
 * all the statistics on a table.
 */
void
RemoveStatisticsById(Oid statsOid)
{
	Relation	relation;
	HeapTuple	tup;

	/*
	 * Delete the pg_proc tuple.
	 */
	relation = heap_open(MvStatisticRelationId, RowExclusiveLock);

	tup = SearchSysCache1(MVSTATOID, ObjectIdGetDatum(statsOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for statistics %u", statsOid);

	simple_heap_delete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}
