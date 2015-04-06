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

#include "access/relscan.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_mv_statistic.h"
#include "catalog/pg_namespace.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/mvstats.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/* used for sorting the attnums in ExecCreateStatistics */
static int
compare_int16(const void *a, const void *b)
{
	return memcmp(a, b, sizeof(int16));
}

/*
 * Implements the CREATE STATISTICS name ON table (columns) WITH (options)
 *
 * TODO: Check that the types support sort, although maybe we can live
 * without it (and only build MCV list / association rules).
 *
 * TODO: This should probably check for duplicate stats (i.e. same keys
 * keys, same options). Although maybe it's useful to have multiple stats
 * on the same columns with different options (say, a detailed MCV-only
 * stats for some queries, histogram for others, etc.)
 */
ObjectAddress
CreateStatistics(CreateStatsStmt *stmt)
{
	int			i,
				j;
	ListCell   *l;
	int16		attnums[INDEX_MAX_KEYS];
	int			numcols = 0;
	ObjectAddress address = InvalidObjectAddress;
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
	Oid			relid;
	ObjectAddress parentobject,
				childobject;

	/* by default build nothing */
	bool		build_dependencies = false,
				build_ndistinct = false,
				build_mcv = false;

	int32		max_mcv_items = -1;

	/* options required because of other options */
	bool		require_mcv = false;

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
	relid = RelationGetRelid(rel);

	/* transform the column names to attnum values */

	foreach(l, stmt->keys)
	{
		char	   *attname = strVal(lfirst(l));
		HeapTuple	atttuple;

		atttuple = SearchSysCacheAttName(relid, attname);

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
	 * Check the lower bound (at least 2 columns), the upper bound was already
	 * checked in the loop.
	 */
	if (numcols < 2)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("multivariate stats require 2 or more columns")));

	/* look for duplicities */
	for (i = 0; i < numcols; i++)
		for (j = 0; j < i; j++)
			if ((i != j) && (attnums[i] == attnums[j]))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
				  errmsg("duplicate column name in statistics definition")));

	/* parse the statistics options */
	foreach(l, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(l);

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
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized STATISTICS option \"%s\"",
							opt->defname)));
	}

	/* check that at least some statistics were requested */
	if (!(build_dependencies || build_ndistinct || build_mcv))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("no statistics type (dependencies, ndistinct, mcv) was requested")));

	/* now do some checking of the options */
	if (require_mcv && (!build_mcv))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("option 'mcv' is required by other options(s)")));

	/* sort the attnums and build int2vector */
	qsort(attnums, numcols, sizeof(int16), compare_int16);
	stakeys = buildint2vector(attnums, numcols);

	/*
	 * Okay, let's create the pg_mv_statistic entry.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	/* no stats collected yet, so just the keys */
	values[Anum_pg_mv_statistic_starelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_mv_statistic_staname - 1] = NameGetDatum(&staname);
	values[Anum_pg_mv_statistic_stanamespace - 1] = ObjectIdGetDatum(namespaceId);
	values[Anum_pg_mv_statistic_staowner - 1] = ObjectIdGetDatum(GetUserId());

	values[Anum_pg_mv_statistic_stakeys - 1] = PointerGetDatum(stakeys);

	values[Anum_pg_mv_statistic_deps_enabled - 1] = BoolGetDatum(build_dependencies);
	values[Anum_pg_mv_statistic_ndist_enabled - 1] = BoolGetDatum(build_ndistinct);
	values[Anum_pg_mv_statistic_mcv_enabled - 1] = BoolGetDatum(build_mcv);

	values[Anum_pg_mv_statistic_mcv_max_items - 1] = Int32GetDatum(max_mcv_items);

	nulls[Anum_pg_mv_statistic_stadeps - 1] = true;
	nulls[Anum_pg_mv_statistic_standist - 1] = true;
	nulls[Anum_pg_mv_statistic_stamcv - 1] = true;

	/* insert the tuple into pg_mv_statistic */
	mvstatrel = heap_open(MvStatisticRelationId, RowExclusiveLock);

	htup = heap_form_tuple(mvstatrel->rd_att, values, nulls);

	simple_heap_insert(mvstatrel, htup);

	CatalogUpdateIndexes(mvstatrel, htup);

	statoid = HeapTupleGetOid(htup);

	heap_freetuple(htup);

	/* Add a dependency on a table, so that stats get dropped on DROP TABLE. */
	ObjectAddressSet(parentobject, RelationRelationId, relid);
	ObjectAddressSet(childobject, MvStatisticRelationId, statoid);

	recordDependencyOn(&childobject, &parentobject, DEPENDENCY_AUTO);

	/*
	 * Also add dependency on the schema (to drop statistics on DROP SCHEMA).
	 * This is not handled automatically by DROP TABLE because statistics have
	 * their own schema.
	 */
	ObjectAddressSet(parentobject, NamespaceRelationId, namespaceId);

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
 *	   DROP STATISTICS stats_name ON table_name
 *
 * The first one requires an exact match, the second one just drops
 * all the statistics on a table.
 */
void
RemoveStatisticsById(Oid statsOid)
{
	Relation	relation;
	Oid			relid;
	Relation	rel;
	HeapTuple	tup;
	Form_pg_mv_statistic mvstat;

	/*
	 * Delete the pg_proc tuple.
	 */
	relation = heap_open(MvStatisticRelationId, RowExclusiveLock);

	tup = SearchSysCache1(MVSTATOID, ObjectIdGetDatum(statsOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for statistics %u", statsOid);

	mvstat = (Form_pg_mv_statistic) GETSTRUCT(tup);
	relid = mvstat->starelid;

	rel = heap_open(relid, AccessExclusiveLock);

	simple_heap_delete(relation, &tup->t_self);

	CacheInvalidateRelcache(rel);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
	heap_close(rel, NoLock);
}
