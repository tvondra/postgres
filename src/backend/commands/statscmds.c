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
 * Implements the CREATE STATISTICS command with syntax:
 *
 *    CREATE STATISTICS name WITH (options) ON (columns) FROM table
 *
 * We do require that the types support sorting (ltopr), although some
 * statistics might work with  equality only.
 */
ObjectAddress
CreateStatistics(CreateStatsStmt *stmt)
{
	int			i;
	ListCell   *l;
	int16		attnums[MVSTATS_MAX_DIMENSIONS];
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
	bool		build_ndistinct = false,
				build_dependencies = false,
				build_mcv = false,
				build_histogram = false;

	Assert(IsA(stmt, CreateStatsStmt));

	/* resolve the pieces of the name (namespace etc.) */
	namespaceId = QualifiedNameGetCreationNamespace(stmt->defnames, &namestr);
	namestrcpy(&staname, namestr);

	/*
	 * If if_not_exists was given and the statistics already exists, bail out.
	 */
	if (SearchSysCacheExists2(MVSTATNAMENSP,
							  PointerGetDatum(&staname),
							  ObjectIdGetDatum(namespaceId)))
	{
		if (stmt->if_not_exists)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					errmsg("statistics \"%s\" already exist, skipping",
							namestr)));
			return InvalidObjectAddress;
		}

		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				errmsg("statistics \"%s\" already exist", namestr)));
	}

	rel = heap_openrv(stmt->relation, AccessExclusiveLock);
	relid = RelationGetRelid(rel);

	/*
	 * Transform column names to array of attnums. While doing that, we
	 * also enforce the maximum number of keys.
	 */
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

		/* more than MVSTATS_MAX_DIMENSIONS columns not allowed */
		if (numcols >= MVSTATS_MAX_DIMENSIONS)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_COLUMNS),
					 errmsg("cannot have more than %d keys in statistics",
							MVSTATS_MAX_DIMENSIONS)));

		attnums[numcols] = ((Form_pg_attribute) GETSTRUCT(atttuple))->attnum;
		ReleaseSysCache(atttuple);
		numcols++;
	}

	/*
	 * Check that at least two columns were specified in the statement.
	 * The upper bound was already checked in the loop above.
	 */
	if (numcols < 2)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("statistics require at least 2 columns")));

	/*
	 * Sort the attnums, which makes detecting duplicies somewhat
	 * easier, and it does not hurt (it does not affect the efficiency,
	 * unlike for indexes, for example).
	 */
	qsort(attnums, numcols, sizeof(int16), compare_int16);

	/*
	 * Look for duplicities in the list of columns. The attnums are sorted
	 * so just check consecutive elements.
	 */
	for (i = 1; i < numcols; i++)
		if (attnums[i] == attnums[i-1])
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
			  errmsg("duplicate column name in statistics definition")));

	/*
	 * Parse the statistics options - currently only statistics types are
	 * recognized (ndistinct, dependencies).
	 */
	foreach(l, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(l);

		if (strcmp(opt->defname, "ndistinct") == 0)
			build_ndistinct = defGetBoolean(opt);
		else if (strcmp(opt->defname, "dependencies") == 0)
			build_dependencies = defGetBoolean(opt);
		else if (strcmp(opt->defname, "mcv") == 0)
			build_mcv = defGetBoolean(opt);
		else if (strcmp(opt->defname, "histogram") == 0)
			build_histogram = defGetBoolean(opt);
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized STATISTICS option \"%s\"",
							opt->defname)));
	}

	/* Make sure there's at least one statistics type specified. */
	if (!(build_ndistinct || build_dependencies || build_mcv || build_histogram))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("no statistics type (ndistinct, dependencies, mcv, histogram) requested")));

	stakeys = buildint2vector(attnums, numcols);

	/*
	 * Everything seems fine, so let's build the pg_mv_statistic entry.
	 * At this point we obviously only have the keys and options.
	 */

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	/* metadata */
	values[Anum_pg_mv_statistic_starelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_mv_statistic_staname - 1] = NameGetDatum(&staname);
	values[Anum_pg_mv_statistic_stanamespace - 1] = ObjectIdGetDatum(namespaceId);
	values[Anum_pg_mv_statistic_staowner - 1] = ObjectIdGetDatum(GetUserId());

	values[Anum_pg_mv_statistic_stakeys - 1] = PointerGetDatum(stakeys);

	/* enabled statistics */
	values[Anum_pg_mv_statistic_ndist_enabled - 1] = BoolGetDatum(build_ndistinct);
	values[Anum_pg_mv_statistic_deps_enabled - 1] = BoolGetDatum(build_dependencies);
	values[Anum_pg_mv_statistic_mcv_enabled - 1] = BoolGetDatum(build_mcv);
	values[Anum_pg_mv_statistic_hist_enabled - 1] = BoolGetDatum(build_histogram);

	nulls[Anum_pg_mv_statistic_standist - 1] = true;
	nulls[Anum_pg_mv_statistic_stadeps - 1] = true;
	nulls[Anum_pg_mv_statistic_stamcv - 1] = true;
	nulls[Anum_pg_mv_statistic_stahist - 1] = true;

	/* insert the tuple into pg_mv_statistic */
	mvstatrel = heap_open(MvStatisticRelationId, RowExclusiveLock);

	htup = heap_form_tuple(mvstatrel->rd_att, values, nulls);

	CatalogTupleInsert(mvstatrel, htup);

	statoid = HeapTupleGetOid(htup);

	heap_freetuple(htup);

	/*
	 * Add a dependency on a table, so that stats get dropped on DROP TABLE.
	 */
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
 *	   DROP STATISTICS stats_name
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
