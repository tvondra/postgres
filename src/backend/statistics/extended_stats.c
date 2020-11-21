/*-------------------------------------------------------------------------
 *
 * extended_stats.c
 *	  POSTGRES extended statistics
 *
 * Generic code supporting statistics objects created via CREATE STATISTICS.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/statistics/extended_stats.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_statistic_ext_data.h"
#include "executor/executor.h"
#include "commands/progress.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "statistics/extended_stats_internal.h"
#include "statistics/statistics.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/attoptcache.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/*
 * To avoid consuming too much memory during analysis and/or too much space
 * in the resulting pg_statistic rows, we ignore varlena datums that are wider
 * than WIDTH_THRESHOLD (after detoasting!).  This is legitimate for MCV
 * and distinct-value calculations since a wide value is unlikely to be
 * duplicated at all, much less be a most-common value.  For the same reason,
 * ignoring wide values will not affect our estimates of histogram bin
 * boundaries very much.
 */
#define WIDTH_THRESHOLD  1024

/*
 * Used internally to refer to an individual statistics object, i.e.,
 * a pg_statistic_ext entry.
 */
typedef struct StatExtEntry
{
	Oid			statOid;		/* OID of pg_statistic_ext entry */
	char	   *schema;			/* statistics object's schema */
	char	   *name;			/* statistics object's name */
	Bitmapset  *columns;		/* attribute numbers covered by the object */
	List	   *types;			/* 'char' list of enabled statistic kinds */
	int			stattarget;		/* statistics target (-1 for default) */
	List	   *exprs;			/* expressions */
} StatExtEntry;


static List *fetch_statentries_for_relation(Relation pg_statext, Oid relid);
static VacAttrStats **lookup_var_attr_stats(Relation rel, Bitmapset *attrs, List *exprs,
											int nvacatts, VacAttrStats **vacatts);
static void statext_store(Oid relid,
						  MVNDistinct *ndistinct, MVDependencies *dependencies,
						  MCVList *mcv, Datum exprs, VacAttrStats **stats);
static int	statext_compute_stattarget(int stattarget,
									   int natts, VacAttrStats **stats);

typedef struct AnlExprData
{
	Node		   *expr;			/* expression to analyze */
	VacAttrStats   *vacattrstat;	/* index attrs to analyze */
} AnlExprData;

static void compute_expr_stats(Relation onerel, double totalrows,
					AnlExprData *exprdata, int nexprs,
					HeapTuple *rows, int numrows);
static Datum serialize_expr_stats(AnlExprData *exprdata, int nexprs);
static Datum expr_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull);
static AnlExprData *build_expr_data(List *exprs);
static VacAttrStats *examine_expression(Node *expr);
static ExprInfo *evaluate_expressions(Relation rel, List *exprs,
									  int numrows, HeapTuple *rows);

/*
 * Compute requested extended stats, using the rows sampled for the plain
 * (single-column) stats.
 *
 * This fetches a list of stats types from pg_statistic_ext, computes the
 * requested stats, and serializes them back into the catalog.
 */
void
BuildRelationExtStatistics(Relation onerel, double totalrows,
						   int numrows, HeapTuple *rows,
						   int natts, VacAttrStats **vacattrstats)
{
	Relation	pg_stext;
	ListCell   *lc;
	List	   *stats;
	MemoryContext cxt;
	MemoryContext oldcxt;
	int64		ext_cnt;

	cxt = AllocSetContextCreate(CurrentMemoryContext,
								"BuildRelationExtStatistics",
								ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(cxt);

	pg_stext = table_open(StatisticExtRelationId, RowExclusiveLock);
	stats = fetch_statentries_for_relation(pg_stext, RelationGetRelid(onerel));

	/* report this phase */
	if (stats != NIL)
	{
		const int	index[] = {
			PROGRESS_ANALYZE_PHASE,
			PROGRESS_ANALYZE_EXT_STATS_TOTAL
		};
		const int64 val[] = {
			PROGRESS_ANALYZE_PHASE_COMPUTE_EXT_STATS,
			list_length(stats)
		};

		pgstat_progress_update_multi_param(2, index, val);
	}

	ext_cnt = 0;
	foreach(lc, stats)
	{
		StatExtEntry *stat = (StatExtEntry *) lfirst(lc);
		MVNDistinct *ndistinct = NULL;
		MVDependencies *dependencies = NULL;
		MCVList    *mcv = NULL;
		Datum		exprstats = (Datum) 0;
		VacAttrStats **stats;
		ListCell   *lc2;
		int			stattarget;
		ExprInfo   *exprs;

		/*
		 * Check if we can build these stats based on the column analyzed. If
		 * not, report this fact (except in autovacuum) and move on.
		 *
		 * FIXME This is confusing - we have 'stats' list, but it's shadowed
		 * by another 'stats' variable here.
		 */
		stats = lookup_var_attr_stats(onerel, stat->columns, stat->exprs,
									  natts, vacattrstats);
		if (!stats)
		{
			if (!IsAutoVacuumWorkerProcess())
				ereport(WARNING,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("statistics object \"%s.%s\" could not be computed for relation \"%s.%s\"",
								stat->schema, stat->name,
								get_namespace_name(onerel->rd_rel->relnamespace),
								RelationGetRelationName(onerel)),
						 errtable(onerel)));
			continue;
		}

		/* check allowed number of dimensions */
		Assert(bms_num_members(stat->columns) + list_length(stat->exprs) >= 2 &&
			   bms_num_members(stat->columns) + list_length(stat->exprs) <= STATS_MAX_DIMENSIONS);

		/* compute statistics target for this statistics */
		stattarget = statext_compute_stattarget(stat->stattarget,
												bms_num_members(stat->columns),
												stats);

		/*
		 * Don't rebuild statistics objects with statistics target set to 0
		 * (we just leave the existing values around, just like we do for
		 * regular per-column statistics).
		 */
		if (stattarget == 0)
			continue;

		/* evaluate expressions (if the statistics has any) */
		exprs = evaluate_expressions(onerel, stat->exprs, numrows, rows);

		/* compute statistic of each requested type */
		foreach(lc2, stat->types)
		{
			char		t = (char) lfirst_int(lc2);

			if (t == STATS_EXT_NDISTINCT)
				ndistinct = statext_ndistinct_build(totalrows, numrows, rows,
													exprs, stat->columns,
													stats);
			else if (t == STATS_EXT_DEPENDENCIES)
				dependencies = statext_dependencies_build(numrows, rows,
														  exprs, stat->columns,
														  stats);
			else if (t == STATS_EXT_MCV)
				mcv = statext_mcv_build(numrows, rows, exprs, stat->columns,
										stats, totalrows, stattarget);
			else if (t == STATS_EXT_EXPRESSIONS)
			{
				AnlExprData *exprdata;
				int			nexprs;

				/* should not happen, thanks to checks when defining stats */
				if (!stat->exprs)
					elog(ERROR, "requested expression stats, but there are no expressions");

				exprdata = build_expr_data(stat->exprs);
				nexprs = list_length(stat->exprs);

				compute_expr_stats(onerel, totalrows,
								   exprdata, nexprs,
								   rows, numrows);

				exprstats = serialize_expr_stats(exprdata, nexprs);
			}
		}

		/* store the statistics in the catalog */
		statext_store(stat->statOid, ndistinct, dependencies, mcv, exprstats, stats);

		/* for reporting progress */
		pgstat_progress_update_param(PROGRESS_ANALYZE_EXT_STATS_COMPUTED,
									 ++ext_cnt);

		pfree(exprs);
	}

	table_close(pg_stext, RowExclusiveLock);

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(cxt);
}

/*
 * ComputeExtStatisticsRows
 *		Compute number of rows required by extended statistics on a table.
 *
 * Computes number of rows we need to sample to build extended statistics on a
 * table. This only looks at statistics we can actually build - for example
 * when analyzing only some of the columns, this will skip statistics objects
 * that would require additional columns.
 *
 * See statext_compute_stattarget for details about how we compute statistics
 * target for a statistics objects (from the object target, attribute targets
 * and default statistics target).
 */
int
ComputeExtStatisticsRows(Relation onerel,
						 int natts, VacAttrStats **vacattrstats)
{
	Relation	pg_stext;
	ListCell   *lc;
	List	   *lstats;
	MemoryContext cxt;
	MemoryContext oldcxt;
	int			result = 0;

	cxt = AllocSetContextCreate(CurrentMemoryContext,
								"ComputeExtStatisticsRows",
								ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(cxt);

	pg_stext = table_open(StatisticExtRelationId, RowExclusiveLock);
	lstats = fetch_statentries_for_relation(pg_stext, RelationGetRelid(onerel));

	foreach(lc, lstats)
	{
		StatExtEntry *stat = (StatExtEntry *) lfirst(lc);
		int			stattarget;
		VacAttrStats **stats;
		int			nattrs = bms_num_members(stat->columns);

		/*
		 * Check if we can build this statistics object based on the columns
		 * analyzed. If not, ignore it (don't report anything, we'll do that
		 * during the actual build BuildRelationExtStatistics).
		 */
		stats = lookup_var_attr_stats(onerel, stat->columns, stat->exprs,
									  natts, vacattrstats);

		if (!stats)
			continue;

		/*
		 * Compute statistics target, based on what's set for the statistic
		 * object itself, and for its attributes.
		 */
		stattarget = statext_compute_stattarget(stat->stattarget,
												nattrs, stats);

		/* Use the largest value for all statistics objects. */
		if (stattarget > result)
			result = stattarget;
	}

	table_close(pg_stext, RowExclusiveLock);

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(cxt);

	/* compute sample size based on the statistics target */
	return (300 * result);
}

/*
 * statext_compute_stattarget
 *		compute statistics target for an extended statistic
 *
 * When computing target for extended statistics objects, we consider three
 * places where the target may be set - the statistics object itself,
 * attributes the statistics is defined on, and then the default statistics
 * target.
 *
 * First we look at what's set for the statistics object itself, using the
 * ALTER STATISTICS ... SET STATISTICS command. If we find a valid value
 * there (i.e. not -1) we're done. Otherwise we look at targets set for any
 * of the attributes the statistic is defined on, and if there are columns
 * with defined target, we use the maximum value. We do this mostly for
 * backwards compatibility, because this is what we did before having
 * statistics target for extended statistics.
 *
 * And finally, if we still don't have a statistics target, we use the value
 * set in default_statistics_target.
 */
static int
statext_compute_stattarget(int stattarget, int nattrs, VacAttrStats **stats)
{
	int			i;

	/*
	 * If there's statistics target set for the statistics object, use it. It
	 * may be set to 0 which disables building of that statistic.
	 */
	if (stattarget >= 0)
		return stattarget;

	/*
	 * The target for the statistics object is set to -1, in which case we
	 * look at the maximum target set for any of the attributes the object is
	 * defined on.
	 */
	for (i = 0; i < nattrs; i++)
	{
		/* keep the maximmum statistics target */
		if (stats[i]->attr->attstattarget > stattarget)
			stattarget = stats[i]->attr->attstattarget;
	}

	/*
	 * If the value is still negative (so neither the statistics object nor
	 * any of the columns have custom statistics target set), use the global
	 * default target.
	 */
	if (stattarget < 0)
		stattarget = default_statistics_target;

	/* As this point we should have a valid statistics target. */
	Assert((stattarget >= 0) && (stattarget <= 10000));

	return stattarget;
}

/*
 * statext_is_kind_built
 *		Is this stat kind built in the given pg_statistic_ext_data tuple?
 */
bool
statext_is_kind_built(HeapTuple htup, char type)
{
	AttrNumber	attnum;

	switch (type)
	{
		case STATS_EXT_NDISTINCT:
			attnum = Anum_pg_statistic_ext_data_stxdndistinct;
			break;

		case STATS_EXT_DEPENDENCIES:
			attnum = Anum_pg_statistic_ext_data_stxddependencies;
			break;

		case STATS_EXT_MCV:
			attnum = Anum_pg_statistic_ext_data_stxdmcv;
			break;

		case STATS_EXT_EXPRESSIONS:
			attnum = Anum_pg_statistic_ext_data_stxdexpr;
			break;

		default:
			elog(ERROR, "unexpected statistics type requested: %d", type);
	}

	return !heap_attisnull(htup, attnum, NULL);
}

/*
 * Return a list (of StatExtEntry) of statistics objects for the given relation.
 */
static List *
fetch_statentries_for_relation(Relation pg_statext, Oid relid)
{
	SysScanDesc scan;
	ScanKeyData skey;
	HeapTuple	htup;
	List	   *result = NIL;

	/*
	 * Prepare to scan pg_statistic_ext for entries having stxrelid = this
	 * rel.
	 */
	ScanKeyInit(&skey,
				Anum_pg_statistic_ext_stxrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(pg_statext, StatisticExtRelidIndexId, true,
							  NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		StatExtEntry *entry;
		Datum		datum;
		bool		isnull;
		int			i;
		ArrayType  *arr;
		char	   *enabled;
		Form_pg_statistic_ext staForm;
		List	   *exprs = NIL;

		entry = palloc0(sizeof(StatExtEntry));
		staForm = (Form_pg_statistic_ext) GETSTRUCT(htup);
		entry->statOid = staForm->oid;
		entry->schema = get_namespace_name(staForm->stxnamespace);
		entry->name = pstrdup(NameStr(staForm->stxname));
		entry->stattarget = staForm->stxstattarget;
		for (i = 0; i < staForm->stxkeys.dim1; i++)
		{
			entry->columns = bms_add_member(entry->columns,
											staForm->stxkeys.values[i]);
		}

		/* decode the stxkind char array into a list of chars */
		datum = SysCacheGetAttr(STATEXTOID, htup,
								Anum_pg_statistic_ext_stxkind, &isnull);
		Assert(!isnull);
		arr = DatumGetArrayTypeP(datum);
		if (ARR_NDIM(arr) != 1 ||
			ARR_HASNULL(arr) ||
			ARR_ELEMTYPE(arr) != CHAROID)
			elog(ERROR, "stxkind is not a 1-D char array");
		enabled = (char *) ARR_DATA_PTR(arr);
		for (i = 0; i < ARR_DIMS(arr)[0]; i++)
		{
			Assert((enabled[i] == STATS_EXT_NDISTINCT) ||
				   (enabled[i] == STATS_EXT_DEPENDENCIES) ||
				   (enabled[i] == STATS_EXT_MCV) ||
				   (enabled[i] == STATS_EXT_EXPRESSIONS));
			entry->types = lappend_int(entry->types, (int) enabled[i]);
		}

		/* decode expression (if any) */
		datum = SysCacheGetAttr(STATEXTOID, htup,
								Anum_pg_statistic_ext_stxexprs, &isnull);

		if (!isnull)
		{
			char *exprsString;

			exprsString = TextDatumGetCString(datum);
			exprs = (List *) stringToNode(exprsString);

			pfree(exprsString);

			/*
			 * Run the expressions through eval_const_expressions. This is not just an
			 * optimization, but is necessary, because the planner will be comparing
			 * them to similarly-processed qual clauses, and may fail to detect valid
			 * matches without this.  We must not use canonicalize_qual, however,
			 * since these aren't qual expressions.
			 */
			exprs = (List *) eval_const_expressions(NULL, (Node *) exprs);

			/* May as well fix opfuncids too */
			fix_opfuncids((Node *) exprs);
		}

		entry->exprs = exprs;

		result = lappend(result, entry);
	}

	systable_endscan(scan);

	return result;
}


/*
 * examine_attribute -- pre-analysis of a single column
 *
 * Determine whether the column is analyzable; if so, create and initialize
 * a VacAttrStats struct for it.  If not, return NULL.
 *
 * If index_expr isn't NULL, then we're trying to analyze an expression index,
 * and index_expr is the expression tree representing the column's data.
 */
static VacAttrStats *
examine_attribute(Node *expr)
{
	HeapTuple	typtuple;
	VacAttrStats *stats;
	int			i;
	bool		ok;

	/*
	 * Create the VacAttrStats struct.  Note that we only have a copy of the
	 * fixed fields of the pg_attribute tuple.
	 */
	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));

	/* fake the attribute */
	stats->attr = (Form_pg_attribute) palloc0(ATTRIBUTE_FIXED_PART_SIZE);
	stats->attr->attstattarget = -1;

	/*
	 * When analyzing an expression index, believe the expression tree's type
	 * not the column datatype --- the latter might be the opckeytype storage
	 * type of the opclass, which is not interesting for our purposes.  (Note:
	 * if we did anything with non-expression index columns, we'd need to
	 * figure out where to get the correct type info from, but for now that's
	 * not a problem.)	It's not clear whether anyone will care about the
	 * typmod, but we store that too just in case.
	 */
	stats->attrtypid = exprType(expr);
	stats->attrtypmod = exprTypmod(expr);
	stats->attrcollid = exprCollation(expr);

	typtuple = SearchSysCacheCopy1(TYPEOID,
								   ObjectIdGetDatum(stats->attrtypid));
	if (!HeapTupleIsValid(typtuple))
		elog(ERROR, "cache lookup failed for type %u", stats->attrtypid);
	stats->attrtype = (Form_pg_type) GETSTRUCT(typtuple);
	// stats->anl_context = anl_context;
	stats->tupattnum = InvalidAttrNumber;

	/*
	 * The fields describing the stats->stavalues[n] element types default to
	 * the type of the data being analyzed, but the type-specific typanalyze
	 * function can change them if it wants to store something else.
	 */
	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		stats->statypid[i] = stats->attrtypid;
		stats->statyplen[i] = stats->attrtype->typlen;
		stats->statypbyval[i] = stats->attrtype->typbyval;
		stats->statypalign[i] = stats->attrtype->typalign;
	}

	/*
	 * Call the type-specific typanalyze function.  If none is specified, use
	 * std_typanalyze().
	 */
	if (OidIsValid(stats->attrtype->typanalyze))
		ok = DatumGetBool(OidFunctionCall1(stats->attrtype->typanalyze,
										   PointerGetDatum(stats)));
	else
		ok = std_typanalyze(stats);

	if (!ok || stats->compute_stats == NULL || stats->minrows <= 0)
	{
		heap_freetuple(typtuple);
		pfree(stats->attr);
		pfree(stats);
		return NULL;
	}

	return stats;
}

/*
 * Using 'vacatts' of size 'nvacatts' as input data, return a newly built
 * VacAttrStats array which includes only the items corresponding to
 * attributes indicated by 'stxkeys'. If we don't have all of the per column
 * stats available to compute the extended stats, then we return NULL to indicate
 * to the caller that the stats should not be built.
 */
static VacAttrStats **
lookup_var_attr_stats(Relation rel, Bitmapset *attrs, List *exprs,
					  int nvacatts, VacAttrStats **vacatts)
{
	int			i = 0;
	int			x = -1;
	int			natts;
	VacAttrStats **stats;
	ListCell   *lc;

	natts = bms_num_members(attrs) + list_length(exprs);

	stats = (VacAttrStats **) palloc(natts * sizeof(VacAttrStats *));

	/* lookup VacAttrStats info for the requested columns (same attnum) */
	while ((x = bms_next_member(attrs, x)) >= 0)
	{
		int			j;

		stats[i] = NULL;
		for (j = 0; j < nvacatts; j++)
		{
			if (x == vacatts[j]->tupattnum)
			{
				stats[i] = vacatts[j];
				break;
			}
		}

		if (!stats[i])
		{
			/*
			 * Looks like stats were not gathered for one of the columns
			 * required. We'll be unable to build the extended stats without
			 * this column.
			 */
			pfree(stats);
			return NULL;
		}

		/*
		 * Sanity check that the column is not dropped - stats should have
		 * been removed in this case.
		 */
		Assert(!stats[i]->attr->attisdropped);

		i++;
	}

	/* also add info for expressions */
	foreach (lc, exprs)
	{
		Node *expr = (Node *) lfirst(lc);

		stats[i] = examine_attribute(expr);

		/*
		 * FIXME We need tuple descriptor later, and we just grab it from
		 * stats[0]->tupDesc (see e.g. statext_mcv_build). But as coded
		 * examine_attribute does not set that, so just grab it from the
		 * first vacatts element.
		 */
		stats[i]->tupDesc = vacatts[0]->tupDesc;

		i++;
	}

	return stats;
}

/*
 * statext_store
 *	Serializes the statistics and stores them into the pg_statistic_ext_data
 *	tuple.
 */
static void
statext_store(Oid statOid,
			  MVNDistinct *ndistinct, MVDependencies *dependencies,
			  MCVList *mcv, Datum exprs, VacAttrStats **stats)
{
	Relation	pg_stextdata;
	HeapTuple	stup,
				oldtup;
	Datum		values[Natts_pg_statistic_ext_data];
	bool		nulls[Natts_pg_statistic_ext_data];
	bool		replaces[Natts_pg_statistic_ext_data];

	pg_stextdata = table_open(StatisticExtDataRelationId, RowExclusiveLock);

	memset(nulls, true, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));
	memset(values, 0, sizeof(values));

	/*
	 * Construct a new pg_statistic_ext_data tuple, replacing the calculated
	 * stats.
	 */
	if (ndistinct != NULL)
	{
		bytea	   *data = statext_ndistinct_serialize(ndistinct);

		nulls[Anum_pg_statistic_ext_data_stxdndistinct - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_data_stxdndistinct - 1] = PointerGetDatum(data);
	}

	if (dependencies != NULL)
	{
		bytea	   *data = statext_dependencies_serialize(dependencies);

		nulls[Anum_pg_statistic_ext_data_stxddependencies - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_data_stxddependencies - 1] = PointerGetDatum(data);
	}
	if (mcv != NULL)
	{
		bytea	   *data = statext_mcv_serialize(mcv, stats);

		nulls[Anum_pg_statistic_ext_data_stxdmcv - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_data_stxdmcv - 1] = PointerGetDatum(data);
	}
	if (exprs != (Datum) 0)
	{
		nulls[Anum_pg_statistic_ext_data_stxdexpr - 1] = false;
		values[Anum_pg_statistic_ext_data_stxdexpr - 1] = exprs;
	}

	/* always replace the value (either by bytea or NULL) */
	replaces[Anum_pg_statistic_ext_data_stxdndistinct - 1] = true;
	replaces[Anum_pg_statistic_ext_data_stxddependencies - 1] = true;
	replaces[Anum_pg_statistic_ext_data_stxdmcv - 1] = true;
	replaces[Anum_pg_statistic_ext_data_stxdexpr - 1] = true;

	/* there should already be a pg_statistic_ext_data tuple */
	oldtup = SearchSysCache1(STATEXTDATASTXOID, ObjectIdGetDatum(statOid));
	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "cache lookup failed for statistics object %u", statOid);

	/* replace it */
	stup = heap_modify_tuple(oldtup,
							 RelationGetDescr(pg_stextdata),
							 values,
							 nulls,
							 replaces);
	ReleaseSysCache(oldtup);
	CatalogTupleUpdate(pg_stextdata, &stup->t_self, stup);

	heap_freetuple(stup);

	table_close(pg_stextdata, RowExclusiveLock);
}

/* initialize multi-dimensional sort */
MultiSortSupport
multi_sort_init(int ndims)
{
	MultiSortSupport mss;

	Assert(ndims >= 2);

	mss = (MultiSortSupport) palloc0(offsetof(MultiSortSupportData, ssup)
									 + sizeof(SortSupportData) * ndims);

	mss->ndims = ndims;

	return mss;
}

/*
 * Prepare sort support info using the given sort operator and collation
 * at the position 'sortdim'
 */
void
multi_sort_add_dimension(MultiSortSupport mss, int sortdim,
						 Oid oper, Oid collation)
{
	SortSupport ssup = &mss->ssup[sortdim];

	ssup->ssup_cxt = CurrentMemoryContext;
	ssup->ssup_collation = collation;
	ssup->ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(oper, ssup);
}

/* compare all the dimensions in the selected order */
int
multi_sort_compare(const void *a, const void *b, void *arg)
{
	MultiSortSupport mss = (MultiSortSupport) arg;
	SortItem   *ia = (SortItem *) a;
	SortItem   *ib = (SortItem *) b;
	int			i;

	for (i = 0; i < mss->ndims; i++)
	{
		int			compare;

		compare = ApplySortComparator(ia->values[i], ia->isnull[i],
									  ib->values[i], ib->isnull[i],
									  &mss->ssup[i]);

		if (compare != 0)
			return compare;
	}

	/* equal by default */
	return 0;
}

/* compare selected dimension */
int
multi_sort_compare_dim(int dim, const SortItem *a, const SortItem *b,
					   MultiSortSupport mss)
{
	return ApplySortComparator(a->values[dim], a->isnull[dim],
							   b->values[dim], b->isnull[dim],
							   &mss->ssup[dim]);
}

int
multi_sort_compare_dims(int start, int end,
						const SortItem *a, const SortItem *b,
						MultiSortSupport mss)
{
	int			dim;

	for (dim = start; dim <= end; dim++)
	{
		int			r = ApplySortComparator(a->values[dim], a->isnull[dim],
											b->values[dim], b->isnull[dim],
											&mss->ssup[dim]);

		if (r != 0)
			return r;
	}

	return 0;
}

int
compare_scalars_simple(const void *a, const void *b, void *arg)
{
	return compare_datums_simple(*(Datum *) a,
								 *(Datum *) b,
								 (SortSupport) arg);
}

int
compare_datums_simple(Datum a, Datum b, SortSupport ssup)
{
	return ApplySortComparator(a, false, b, false, ssup);
}

/* simple counterpart to qsort_arg */
void *
bsearch_arg(const void *key, const void *base, size_t nmemb, size_t size,
			int (*compar) (const void *, const void *, void *),
			void *arg)
{
	size_t		l,
				u,
				idx;
	const void *p;
	int			comparison;

	l = 0;
	u = nmemb;
	while (l < u)
	{
		idx = (l + u) / 2;
		p = (void *) (((const char *) base) + (idx * size));
		comparison = (*compar) (key, p, arg);

		if (comparison < 0)
			u = idx;
		else if (comparison > 0)
			l = idx + 1;
		else
			return (void *) p;
	}

	return NULL;
}

/*
 * build_attnums_array
 *		Transforms a bitmap into an array of AttrNumber values.
 *
 * This is used for extended statistics only, so all the attribute must be
 * user-defined. That means offsetting by FirstLowInvalidHeapAttributeNumber
 * is not necessary here (and when querying the bitmap).
 */
AttrNumber *
build_attnums_array(Bitmapset *attrs, int *numattrs)
{
	int			i,
				j;
	AttrNumber *attnums;
	int			num = bms_num_members(attrs);

	if (numattrs)
		*numattrs = num;

	/* build attnums from the bitmapset */
	attnums = (AttrNumber *) palloc(sizeof(AttrNumber) * num);
	i = 0;
	j = -1;
	while ((j = bms_next_member(attrs, j)) >= 0)
	{
		/*
		 * Make sure the bitmap contains only user-defined attributes. As
		 * bitmaps can't contain negative values, this can be violated in two
		 * ways. Firstly, the bitmap might contain 0 as a member, and secondly
		 * the integer value might be larger than MaxAttrNumber.
		 */
		Assert(AttrNumberIsForUserDefinedAttr(j));
		Assert(j <= MaxAttrNumber);

		attnums[i++] = (AttrNumber) j;

		/* protect against overflows */
		Assert(i <= num);
	}

	return attnums;
}

/*
 * build_sorted_items
 *		build a sorted array of SortItem with values from rows
 *
 * Note: All the memory is allocated in a single chunk, so that the caller
 * can simply pfree the return value to release all of it.
 */
SortItem *
build_sorted_items(int numrows, int *nitems, HeapTuple *rows, ExprInfo *exprs,
				   TupleDesc tdesc, MultiSortSupport mss,
				   int numattrs, AttrNumber *attnums)
{
	int			i,
				j,
				len,
				idx;
	int			nvalues = numrows * numattrs;

	SortItem   *items;
	Datum	   *values;
	bool	   *isnull;
	char	   *ptr;

	/* Compute the total amount of memory we need (both items and values). */
	len = numrows * sizeof(SortItem) + nvalues * (sizeof(Datum) + sizeof(bool));

	/* Allocate the memory and split it into the pieces. */
	ptr = palloc0(len);

	/* items to sort */
	items = (SortItem *) ptr;
	ptr += numrows * sizeof(SortItem);

	/* values and null flags */
	values = (Datum *) ptr;
	ptr += nvalues * sizeof(Datum);

	isnull = (bool *) ptr;
	ptr += nvalues * sizeof(bool);

	/* make sure we consumed the whole buffer exactly */
	Assert((ptr - (char *) items) == len);

	/* fix the pointers to Datum and bool arrays */
	idx = 0;
	for (i = 0; i < numrows; i++)
	{
		bool		toowide = false;

		items[idx].values = &values[idx * numattrs];
		items[idx].isnull = &isnull[idx * numattrs];

		/* load the values/null flags from sample rows */
		for (j = 0; j < numattrs; j++)
		{
			Datum		value;
			bool		isnull;
			int			attlen;

			if (attnums[j] <= MaxHeapAttributeNumber)
			{
				value = heap_getattr(rows[i], attnums[j], tdesc, &isnull);
				attlen = TupleDescAttr(tdesc, attnums[j] - 1)->attlen;
			}
			else
			{
				int	idx = (attnums[j] - MaxHeapAttributeNumber - 1);

				Assert((idx >= 0) && (idx < exprs->nexprs));

				value = exprs->values[idx][i];
				isnull = exprs->nulls[idx][i];

				attlen = get_typlen(exprs->types[idx]);
			}

			/*
			 * If this is a varlena value, check if it's too wide and if yes
			 * then skip the whole item. Otherwise detoast the value.
			 *
			 * XXX It may happen that we've already detoasted some preceding
			 * values for the current item. We don't bother to cleanup those
			 * on the assumption that those are small (below WIDTH_THRESHOLD)
			 * and will be discarded at the end of analyze.
			 */
			if ((!isnull) && (attlen == -1))
			{
				if (toast_raw_datum_size(value) > WIDTH_THRESHOLD)
				{
					toowide = true;
					break;
				}

				value = PointerGetDatum(PG_DETOAST_DATUM(value));
			}

			items[idx].values[j] = value;
			items[idx].isnull[j] = isnull;
		}

		if (toowide)
			continue;

		idx++;
	}

	/* store the actual number of items (ignoring the too-wide ones) */
	*nitems = idx;

	/* all items were too wide */
	if (idx == 0)
	{
		/* everything is allocated as a single chunk */
		pfree(items);
		return NULL;
	}

	/* do the sort, using the multi-sort */
	qsort_arg((void *) items, idx, sizeof(SortItem),
			  multi_sort_compare, mss);

	return items;
}

/*
 * has_stats_of_kind
 *		Check whether the list contains statistic of a given kind
 */
bool
has_stats_of_kind(List *stats, char requiredkind)
{
	ListCell   *l;

	foreach(l, stats)
	{
		StatisticExtInfo *stat = (StatisticExtInfo *) lfirst(l);

		if (stat->kind == requiredkind)
			return true;
	}

	return false;
}

/*
 * choose_best_statistics
 *		Look for and return statistics with the specified 'requiredkind' which
 *		have keys that match at least two of the given attnums.  Return NULL if
 *		there's no match.
 *
 * The current selection criteria is very simple - we choose the statistics
 * object referencing the most attributes in covered (and still unestimated
 * clauses), breaking ties in favor of objects with fewer keys overall.
 *
 * The clause_attnums is an array of bitmaps, storing attnums for individual
 * clauses. A NULL element means the clause is either incompatible or already
 * estimated.
 *
 * XXX If multiple statistics objects tie on both criteria, then which object
 * is chosen depends on the order that they appear in the stats list. Perhaps
 * further tiebreakers are needed.
 */
StatisticExtInfo *
choose_best_statistics(List *stats, char requiredkind,
					   Bitmapset **clause_attnums, Node **clause_exprs,
					   int nclauses)
{
	ListCell   *lc;
	StatisticExtInfo *best_match = NULL;
	int			best_num_matched = 2;	/* goal #1: maximize */
	int			best_match_keys = (STATS_MAX_DIMENSIONS + 1);	/* goal #2: minimize */

	foreach(lc, stats)
	{
		int			i;
		StatisticExtInfo *info = (StatisticExtInfo *) lfirst(lc);
		Bitmapset  *matched = NULL;
		int			num_matched;
		int			num_matched_exprs;
		int			numkeys;

		/* skip statistics that are not of the correct type */
		if (info->kind != requiredkind)
			continue;

		/*
		 * Collect attributes in remaining (unestimated) clauses fully covered
		 * by this statistic object.
		 */
		for (i = 0; i < nclauses; i++)
		{
			/* ignore incompatible/estimated clauses */
			if (!clause_attnums[i])
				continue;

			/* ignore clauses that are not covered by this object */
			if (!bms_is_subset(clause_attnums[i], info->keys))
				continue;

			matched = bms_add_members(matched, clause_attnums[i]);
		}

		num_matched = bms_num_members(matched);
		bms_free(matched);

		/*
		 * Collect expressions in remaining (unestimated) expressions, covered
		 * by an expression in this statistic object.
		 */
		num_matched_exprs = 0;
		for (i = 0; i < nclauses; i++)
		{
			ListCell *lc2;

			/* ignore incompatible/estimated expressions */
			if (!clause_exprs[i])
				continue;

			/* ignore expressions that are not covered by this object */
			foreach(lc2, info->exprs)
			{
				Node   *stat_expr = (Node *) lfirst(lc2);

				if (equal(clause_exprs[i], stat_expr))
				{
					num_matched_exprs++;
					break;
				}
			}
		}

		/*
		 * save the actual number of keys in the stats so that we can choose
		 * the narrowest stats with the most matching keys.
		 */
		numkeys = bms_num_members(info->keys);

		/*
		 * Use this object when it increases the number of matched clauses or
		 * when it matches the same number of attributes but these stats have
		 * fewer keys than any previous match.
		 */
		if (num_matched + num_matched_exprs > best_num_matched ||
			((num_matched + num_matched_exprs) == best_num_matched &&
			 numkeys < best_match_keys))
		{
			best_match = info;
			best_num_matched = num_matched + num_matched_exprs;
			best_match_keys = numkeys;
		}
	}

	return best_match;
}

/*
 * statext_is_compatible_clause_internal
 *		Determines if the clause is compatible with MCV lists.
 *
 * Does the heavy lifting of actually inspecting the clauses for
 * statext_is_compatible_clause. It needs to be split like this because
 * of recursion.  The attnums bitmap is an input/output parameter collecting
 * attribute numbers from all compatible clauses (recursively).
 */
static bool
statext_is_compatible_clause_internal(PlannerInfo *root, Node *clause,
									  Index relid, Bitmapset **attnums)
{
	/* Look inside any binary-compatible relabeling (as in examine_variable) */
	if (IsA(clause, RelabelType))
		clause = (Node *) ((RelabelType *) clause)->arg;

	/* plain Var references (boolean Vars or recursive checks) */
	if (IsA(clause, Var))
	{
		Var		   *var = (Var *) clause;

		/* Ensure var is from the correct relation */
		if (var->varno != relid)
			return false;

		/* we also better ensure the Var is from the current level */
		if (var->varlevelsup > 0)
			return false;

		/* Also skip system attributes (we don't allow stats on those). */
		if (!AttrNumberIsForUserDefinedAttr(var->varattno))
			return false;

		*attnums = bms_add_member(*attnums, var->varattno);

		return true;
	}

	/* (Var op Const) or (Const op Var) */
	if (is_opclause(clause))
	{
		RangeTblEntry *rte = root->simple_rte_array[relid];
		OpExpr	   *expr = (OpExpr *) clause;
		Var		   *var;

		/* Only expressions with two arguments are considered compatible. */
		if (list_length(expr->args) != 2)
			return false;

		/* Check if the expression has the right shape (one Var, one Const) */
		if (!examine_opclause_expression(expr, &var, NULL, NULL))
			return false;

		/*
		 * If it's not one of the supported operators ("=", "<", ">", etc.),
		 * just ignore the clause, as it's not compatible with MCV lists.
		 *
		 * This uses the function for estimating selectivity, not the operator
		 * directly (a bit awkward, but well ...).
		 */
		switch (get_oprrest(expr->opno))
		{
			case F_EQSEL:
			case F_NEQSEL:
			case F_SCALARLTSEL:
			case F_SCALARLESEL:
			case F_SCALARGTSEL:
			case F_SCALARGESEL:
				/* supported, will continue with inspection of the Var */
				break;

			default:
				/* other estimators are considered unknown/unsupported */
				return false;
		}

		/*
		 * If there are any securityQuals on the RTE from security barrier
		 * views or RLS policies, then the user may not have access to all the
		 * table's data, and we must check that the operator is leak-proof.
		 *
		 * If the operator is leaky, then we must ignore this clause for the
		 * purposes of estimating with MCV lists, otherwise the operator might
		 * reveal values from the MCV list that the user doesn't have
		 * permission to see.
		 */
		if (rte->securityQuals != NIL &&
			!get_func_leakproof(get_opcode(expr->opno)))
			return false;

		return statext_is_compatible_clause_internal(root, (Node *) var,
													 relid, attnums);
	}

	/* Var IN Array */
	if (IsA(clause, ScalarArrayOpExpr))
	{
		RangeTblEntry *rte = root->simple_rte_array[relid];
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) clause;
		Var		   *var;

		/* Only expressions with two arguments are considered compatible. */
		if (list_length(expr->args) != 2)
			return false;

		/* Check if the expression has the right shape (one Var, one Const) */
		if (!examine_clause_args(expr->args, &var, NULL, NULL))
			return false;

		/*
		 * If it's not one of the supported operators ("=", "<", ">", etc.),
		 * just ignore the clause, as it's not compatible with MCV lists.
		 *
		 * This uses the function for estimating selectivity, not the operator
		 * directly (a bit awkward, but well ...).
		 */
		switch (get_oprrest(expr->opno))
		{
			case F_EQSEL:
			case F_NEQSEL:
			case F_SCALARLTSEL:
			case F_SCALARLESEL:
			case F_SCALARGTSEL:
			case F_SCALARGESEL:
				/* supported, will continue with inspection of the Var */
				break;

			default:
				/* other estimators are considered unknown/unsupported */
				return false;
		}

		/*
		 * If there are any securityQuals on the RTE from security barrier
		 * views or RLS policies, then the user may not have access to all the
		 * table's data, and we must check that the operator is leak-proof.
		 *
		 * If the operator is leaky, then we must ignore this clause for the
		 * purposes of estimating with MCV lists, otherwise the operator might
		 * reveal values from the MCV list that the user doesn't have
		 * permission to see.
		 */
		if (rte->securityQuals != NIL &&
			!get_func_leakproof(get_opcode(expr->opno)))
			return false;

		return statext_is_compatible_clause_internal(root, (Node *) var,
													 relid, attnums);
	}

	/* AND/OR/NOT clause */
	if (is_andclause(clause) ||
		is_orclause(clause) ||
		is_notclause(clause))
	{
		/*
		 * AND/OR/NOT-clauses are supported if all sub-clauses are supported
		 *
		 * Perhaps we could improve this by handling mixed cases, when some of
		 * the clauses are supported and some are not. Selectivity for the
		 * supported subclauses would be computed using extended statistics,
		 * and the remaining clauses would be estimated using the traditional
		 * algorithm (product of selectivities).
		 *
		 * It however seems overly complex, and in a way we already do that
		 * because if we reject the whole clause as unsupported here, it will
		 * be eventually passed to clauselist_selectivity() which does exactly
		 * this (split into supported/unsupported clauses etc).
		 */
		BoolExpr   *expr = (BoolExpr *) clause;
		ListCell   *lc;

		foreach(lc, expr->args)
		{
			/*
			 * Had we found incompatible clause in the arguments, treat the
			 * whole clause as incompatible.
			 *
			 * XXX This fails for expressions, at the moment.
			 */
			if (!statext_is_compatible_clause_internal(root,
													   (Node *) lfirst(lc),
													   relid, attnums))
				return false;
		}

		return true;
	}

	/* Var IS NULL */
	if (IsA(clause, NullTest))
	{
		NullTest   *nt = (NullTest *) clause;

		/*
		 * Only simple (Var IS NULL) expressions supported for now. Maybe we
		 * could use examine_variable to fix this?
		 */
		if (!IsA(nt->arg, Var))
			return false;

		return statext_is_compatible_clause_internal(root, (Node *) (nt->arg),
													 relid, attnums);
	}

	return false;
}



/*
 * statext_extract_expression_internal
 *		FIXME
 *
 */
static Node *
statext_extract_expression_internal(PlannerInfo *root, Node *clause, Index relid)
{
	/* Look inside any binary-compatible relabeling (as in examine_variable) */
	if (IsA(clause, RelabelType))
		clause = (Node *) ((RelabelType *) clause)->arg;

	/* plain Var references (boolean Vars or recursive checks) */
	if (IsA(clause, Var))
	{
		Var		   *var = (Var *) clause;

		/* Ensure var is from the correct relation */
		if (var->varno != relid)
			return NULL;

		/* we also better ensure the Var is from the current level */
		if (var->varlevelsup > 0)
			return NULL;

		/* Also skip system attributes (we don't allow stats on those). */
		if (!AttrNumberIsForUserDefinedAttr(var->varattno))
			return NULL;

		// *attnums = bms_add_member(*attnums, var->varattno);
		return clause;
	}

	/* (Var op Const) or (Const op Var) */
	if (is_opclause(clause))
	{
		RangeTblEntry *rte = root->simple_rte_array[relid];
		OpExpr	   *expr = (OpExpr *) clause;
		Node	   *expr2 = NULL;

		/* Only expressions with two arguments are considered compatible. */
		if (list_length(expr->args) != 2)
			return NULL;

		/* Check if the expression has the right shape (one Expr, one Const) */
		if (!examine_opclause_expression2(expr, &expr2, NULL, NULL))
			return false;

		/*
		 * If it's not one of the supported operators ("=", "<", ">", etc.),
		 * just ignore the clause, as it's not compatible with MCV lists.
		 *
		 * This uses the function for estimating selectivity, not the operator
		 * directly (a bit awkward, but well ...).
		 */
		switch (get_oprrest(expr->opno))
		{
			case F_EQSEL:
			case F_NEQSEL:
			case F_SCALARLTSEL:
			case F_SCALARLESEL:
			case F_SCALARGTSEL:
			case F_SCALARGESEL:
				/* supported, will continue with inspection of the Var */
				break;

			default:
				/* other estimators are considered unknown/unsupported */
				return NULL;
		}

		/*
		 * If there are any securityQuals on the RTE from security barrier
		 * views or RLS policies, then the user may not have access to all the
		 * table's data, and we must check that the operator is leak-proof.
		 *
		 * If the operator is leaky, then we must ignore this clause for the
		 * purposes of estimating with MCV lists, otherwise the operator might
		 * reveal values from the MCV list that the user doesn't have
		 * permission to see.
		 */
		if (rte->securityQuals != NIL &&
			!get_func_leakproof(get_opcode(expr->opno)))
			return NULL;

		return expr2;
	}

	else if (IsA(clause, ScalarArrayOpExpr))
	{
		RangeTblEntry *rte = root->simple_rte_array[relid];
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) clause;
		Node	   *expr2 = NULL;

		/* Only expressions with two arguments are considered compatible. */
		if (list_length(expr->args) != 2)
			return NULL;

		/* Check if the expression has the right shape (one Expr, one Const) */
		if (!examine_clause_args2(expr->args, &expr2, NULL, NULL))
			return false;

		/*
		 * If there are any securityQuals on the RTE from security barrier
		 * views or RLS policies, then the user may not have access to all the
		 * table's data, and we must check that the operator is leak-proof.
		 *
		 * If the operator is leaky, then we must ignore this clause for the
		 * purposes of estimating with MCV lists, otherwise the operator might
		 * reveal values from the MCV list that the user doesn't have
		 * permission to see.
		 */
		if (rte->securityQuals != NIL &&
			!get_func_leakproof(get_opcode(expr->opno)))
			return NULL;

		return expr2;
	}

	/* AND/OR/NOT clause */
	if (is_andclause(clause) ||
		is_orclause(clause) ||
		is_notclause(clause))
	{
		/*
		 * AND/OR/NOT-clauses are supported if all sub-clauses are supported
		 *
		 * Perhaps we could improve this by handling mixed cases, when some of
		 * the clauses are supported and some are not. Selectivity for the
		 * supported subclauses would be computed using extended statistics,
		 * and the remaining clauses would be estimated using the traditional
		 * algorithm (product of selectivities).
		 *
		 * It however seems overly complex, and in a way we already do that
		 * because if we reject the whole clause as unsupported here, it will
		 * be eventually passed to clauselist_selectivity() which does exactly
		 * this (split into supported/unsupported clauses etc).
		 */
		BoolExpr   *expr = (BoolExpr *) clause;
		ListCell   *lc;

		foreach(lc, expr->args)
		{
			/*
			 * Had we found incompatible clause in the arguments, treat the
			 * whole clause as incompatible.
			 */
			if (!statext_extract_expression_internal(root,
													 (Node *) lfirst(lc),
													 relid))
				return NULL;
		}

		return clause;
	}

	/* Var IS NULL */
	if (IsA(clause, NullTest))
	{
		NullTest   *nt = (NullTest *) clause;

		/*
		 * Only simple (Var IS NULL) expressions supported for now. Maybe we
		 * could use examine_variable to fix this?
		 */
		if (!IsA(nt->arg, Var))
			return NULL;

		return statext_extract_expression_internal(root, (Node *) (nt->arg),
												   relid);
	}

	return NULL;
}

/*
 * statext_is_compatible_clause
 *		Determines if the clause is compatible with MCV lists.
 *
 * Currently, we only support three types of clauses:
 *
 * (a) OpExprs of the form (Var op Const), or (Const op Var), where the op
 * is one of ("=", "<", ">", ">=", "<=")
 *
 * (b) (Var IS [NOT] NULL)
 *
 * (c) combinations using AND/OR/NOT
 *
 * In the future, the range of supported clauses may be expanded to more
 * complex cases, for example (Var op Var).
 */
static bool
statext_is_compatible_clause(PlannerInfo *root, Node *clause, Index relid,
							 Bitmapset **attnums)
{
	RangeTblEntry *rte = root->simple_rte_array[relid];
	RestrictInfo *rinfo = (RestrictInfo *) clause;
	Oid			userid;

	if (!IsA(rinfo, RestrictInfo))
		return false;

	/* Pseudoconstants are not really interesting here. */
	if (rinfo->pseudoconstant)
		return false;

	/* clauses referencing multiple varnos are incompatible */
	if (bms_membership(rinfo->clause_relids) != BMS_SINGLETON)
		return false;

	/* Check the clause and determine what attributes it references. */
	if (!statext_is_compatible_clause_internal(root, (Node *) rinfo->clause,
											   relid, attnums))
		return false;

	/*
	 * Check that the user has permission to read all these attributes.  Use
	 * checkAsUser if it's set, in case we're accessing the table via a view.
	 */
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	if (pg_class_aclcheck(rte->relid, userid, ACL_SELECT) != ACLCHECK_OK)
	{
		/* Don't have table privilege, must check individual columns */
		if (bms_is_member(InvalidAttrNumber, *attnums))
		{
			/* Have a whole-row reference, must have access to all columns */
			if (pg_attribute_aclcheck_all(rte->relid, userid, ACL_SELECT,
										  ACLMASK_ALL) != ACLCHECK_OK)
				return false;
		}
		else
		{
			/* Check the columns referenced by the clause */
			int			attnum = -1;

			while ((attnum = bms_next_member(*attnums, attnum)) >= 0)
			{
				if (pg_attribute_aclcheck(rte->relid, attnum, userid,
										  ACL_SELECT) != ACLCHECK_OK)
					return false;
			}
		}
	}

	/* If we reach here, the clause is OK */
	return true;
}

/*
 * statext_extract_expression
 *		Determines if the clause is compatible with MCV lists.
 *
 * Currently, we only support three types of clauses:
 *
 * (a) OpExprs of the form (Var op Const), or (Const op Var), where the op
 * is one of ("=", "<", ">", ">=", "<=")
 *
 * (b) (Var IS [NOT] NULL)
 *
 * (c) combinations using AND/OR/NOT
 *
 * In the future, the range of supported clauses may be expanded to more
 * complex cases, for example (Var op Var).
 */
static Node *
statext_extract_expression(PlannerInfo *root, Node *clause, Index relid)
{
	RestrictInfo *rinfo = (RestrictInfo *) clause;
	Node		 *expr;

	if (!IsA(rinfo, RestrictInfo))
		return false;

	/* Pseudoconstants are not really interesting here. */
	if (rinfo->pseudoconstant)
		return false;

	/* clauses referencing multiple varnos are incompatible */
	if (bms_membership(rinfo->clause_relids) != BMS_SINGLETON)
		return false;

	/* Check the clause and determine what attributes it references. */
	expr = statext_extract_expression_internal(root, (Node *) rinfo->clause, relid);

	if (!expr)
		return NULL;

	/* FIXME do the same ACL check as in statext_is_compatible_clause */

	/* If we reach here, the clause is OK */
	return expr;
}

/*
 * statext_mcv_clauselist_selectivity
 *		Estimate clauses using the best multi-column statistics.
 *
 * Applies available extended (multi-column) statistics on a table. There may
 * be multiple applicable statistics (with respect to the clauses), in which
 * case we use greedy approach. In each round we select the best statistic on
 * a table (measured by the number of attributes extracted from the clauses
 * and covered by it), and compute the selectivity for the supplied clauses.
 * We repeat this process with the remaining clauses (if any), until none of
 * the available statistics can be used.
 *
 * One of the main challenges with using MCV lists is how to extrapolate the
 * estimate to the data not covered by the MCV list. To do that, we compute
 * not only the "MCV selectivity" (selectivities for MCV items matching the
 * supplied clauses), but also a couple of derived selectivities:
 *
 * - simple selectivity:  Computed without extended statistic, i.e. as if the
 * columns/clauses were independent
 *
 * - base selectivity:  Similar to simple selectivity, but is computed using
 * the extended statistic by adding up the base frequencies (that we compute
 * and store for each MCV item) of matching MCV items.
 *
 * - total selectivity: Selectivity covered by the whole MCV list.
 *
 * - other selectivity: A selectivity estimate for data not covered by the MCV
 * list (i.e. satisfying the clauses, but not common enough to make it into
 * the MCV list)
 *
 * Note: While simple and base selectivities are defined in a quite similar
 * way, the values are computed differently and are not therefore equal. The
 * simple selectivity is computed as a product of per-clause estimates, while
 * the base selectivity is computed by adding up base frequencies of matching
 * items of the multi-column MCV list. So the values may differ for two main
 * reasons - (a) the MCV list may not cover 100% of the data and (b) some of
 * the MCV items did not match the estimated clauses.
 *
 * As both (a) and (b) reduce the base selectivity value, it generally holds
 * that (simple_selectivity >= base_selectivity). If the MCV list covers all
 * the data, the values may be equal.
 *
 * So, (simple_selectivity - base_selectivity) is an estimate for the part
 * not covered by the MCV list, and (mcv_selectivity - base_selectivity) may
 * be seen as a correction for the part covered by the MCV list. Those two
 * statements are actually equivalent.
 *
 * Note: Due to rounding errors and minor differences in how the estimates
 * are computed, the inequality may not always hold. Which is why we clamp
 * the selectivities to prevent strange estimate (negative etc.).
 *
 * 'estimatedclauses' is an input/output parameter.  We set bits for the
 * 0-based 'clauses' indexes we estimate for and also skip clause items that
 * already have a bit set.
 */
static Selectivity
statext_mcv_clauselist_selectivity(PlannerInfo *root, List *clauses, int varRelid,
								   JoinType jointype, SpecialJoinInfo *sjinfo,
								   RelOptInfo *rel, Bitmapset **estimatedclauses)
{
	ListCell   *l;
	Bitmapset **list_attnums;	/* attnums extracted from the clause */
	Node	  **list_exprs;		/* expressions matched to any statistic */
	int			listidx;
	Selectivity sel = 1.0;

	/* check if there's any stats that might be useful for us. */
	if (!has_stats_of_kind(rel->statlist, STATS_EXT_MCV))
		return 1.0;

	list_attnums = (Bitmapset **) palloc(sizeof(Bitmapset *) *
										 list_length(clauses));

	list_exprs = (Node **) palloc(sizeof(Node *) * list_length(clauses));

	/*
	 * Pre-process the clauses list to extract the attnums seen in each item.
	 * We need to determine if there's any clauses which will be useful for
	 * selectivity estimations with extended stats. Along the way we'll record
	 * all of the attnums for each clause in a list which we'll reference
	 * later so we don't need to repeat the same work again. We'll also keep
	 * track of all attnums seen.
	 *
	 * We also skip clauses that we already estimated using different types of
	 * statistics (we treat them as incompatible).
	 */
	listidx = 0;
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);
		Bitmapset  *attnums = NULL;

		/* the clause is considered incompatible by default */
		list_attnums[listidx] = NULL;

		/* and it's also not covered exactly by the statistic */
		list_exprs[listidx] = NULL;

		/*
		 * First see if the clause is simple enough to be covered directly
		 * by the attributes. If not, see if there's at least one statistic
		 * object using the expression as-is.
		 */
		if (!bms_is_member(listidx, *estimatedclauses) &&
			statext_is_compatible_clause(root, clause, rel->relid, &attnums))
		{
			/* simple expression, covered through attnum(s) */
			list_attnums[listidx] = attnums;
		}
		else
		{
			ListCell *lc;
			Node	 *expr;

			expr = statext_extract_expression(root, clause, rel->relid);

			/* complex expression, search for statistic */
			foreach(lc, rel->statlist)
			{
				ListCell		   *lc2;
				StatisticExtInfo   *info = (StatisticExtInfo *) lfirst(lc);

				/* have we already matched the expression to a statistic? */
				Assert(!list_exprs[listidx]);

				/* no expressions in the statistic */
				if (!info->exprs)
					continue;

				/*
				 * Walk the expressions, see if all expressions extracted from
				 * the clause are covered by the extended statistic object.
				 */
				foreach (lc2, info->exprs)
				{
					Node   *stat_expr = (Node *) lfirst(lc2);

					if (equal(expr, stat_expr))
					{
						list_exprs[listidx] = expr;
						break;
					}

					if (list_exprs[listidx])
						break;
				}

				/* stop looking for another statistic */
				if (list_exprs[listidx])
					break;
			}
		}

		listidx++;
	}

	/* apply as many extended statistics as possible */
	while (true)
	{
		StatisticExtInfo *stat;
		List	   *stat_clauses;
		Selectivity simple_sel,
					mcv_sel,
					mcv_basesel,
					mcv_totalsel,
					other_sel,
					stat_sel;

		/* find the best suited statistics object for these attnums */
		stat = choose_best_statistics(rel->statlist, STATS_EXT_MCV,
									  list_attnums, list_exprs,
									  list_length(clauses));

		/*
		 * if no (additional) matching stats could be found then we've nothing
		 * to do
		 */
		if (!stat)
			break;

		/* Ensure choose_best_statistics produced an expected stats type. */
		Assert(stat->kind == STATS_EXT_MCV);

		/* now filter the clauses to be estimated using the selected MCV */
		stat_clauses = NIL;

		listidx = 0;
		foreach(l, clauses)
		{
			/*
			 * If the clause is compatible with the selected statistics, mark
			 * it as estimated and add it to the list to estimate. It may be
			 * either a simple clause, or an expression.
			 */
			if (list_attnums[listidx] != NULL &&
				bms_is_subset(list_attnums[listidx], stat->keys))
			{
				/* simple clause */
				stat_clauses = lappend(stat_clauses, (Node *) lfirst(l));
				*estimatedclauses = bms_add_member(*estimatedclauses, listidx);

				bms_free(list_attnums[listidx]);
				list_attnums[listidx] = NULL;
			}
			else
			{
				/* expression */
				ListCell *lc;

				foreach (lc, stat->exprs)
				{
					Node   *stat_expr = (Node *) lfirst(lc);

					if (equal(list_exprs[listidx], stat_expr))
					{
						stat_clauses = lappend(stat_clauses, (Node *) lfirst(l));
						*estimatedclauses = bms_add_member(*estimatedclauses, listidx);

						// bms_free(list_attnums[listidx]);
						list_exprs[listidx] = NULL;

						break;
					}
				}
			}

			listidx++;
		}

		/*
		 * First compute "simple" selectivity, i.e. without the extended
		 * statistics, and essentially assuming independence of the
		 * columns/clauses. We'll then use the various selectivities computed
		 * from MCV list to improve it.
		 */
		simple_sel = clauselist_selectivity_simple(root, stat_clauses, varRelid,
												   jointype, sjinfo, NULL);

		/*
		 * Now compute the multi-column estimate from the MCV list, along with
		 * the other selectivities (base & total selectivity).
		 */
		mcv_sel = mcv_clauselist_selectivity(root, stat, stat_clauses, varRelid,
											 jointype, sjinfo, rel,
											 &mcv_basesel, &mcv_totalsel);

		/* Estimated selectivity of values not covered by MCV matches */
		other_sel = simple_sel - mcv_basesel;
		CLAMP_PROBABILITY(other_sel);

		/* The non-MCV selectivity can't exceed the 1 - mcv_totalsel. */
		if (other_sel > 1.0 - mcv_totalsel)
			other_sel = 1.0 - mcv_totalsel;

		/*
		 * Overall selectivity is the combination of MCV and non-MCV
		 * estimates.
		 */
		stat_sel = mcv_sel + other_sel;
		CLAMP_PROBABILITY(stat_sel);

		/* Factor the estimate from this MCV to the overall estimate. */
		sel *= stat_sel;
	}

	return sel;
}

/*
 * statext_clauselist_selectivity
 *		Estimate clauses using the best multi-column statistics.
 */
Selectivity
statext_clauselist_selectivity(PlannerInfo *root, List *clauses, int varRelid,
							   JoinType jointype, SpecialJoinInfo *sjinfo,
							   RelOptInfo *rel, Bitmapset **estimatedclauses)
{
	Selectivity sel;

	/* First, try estimating clauses using a multivariate MCV list. */
	sel = statext_mcv_clauselist_selectivity(root, clauses, varRelid, jointype,
											 sjinfo, rel, estimatedclauses);

	/*
	 * Then, apply functional dependencies on the remaining clauses by calling
	 * dependencies_clauselist_selectivity.  Pass 'estimatedclauses' so the
	 * function can properly skip clauses already estimated above.
	 *
	 * The reasoning for applying dependencies last is that the more complex
	 * stats can track more complex correlations between the attributes, and
	 * so may be considered more reliable.
	 *
	 * For example, MCV list can give us an exact selectivity for values in
	 * two columns, while functional dependencies can only provide information
	 * about the overall strength of the dependency.
	 */
	sel *= dependencies_clauselist_selectivity(root, clauses, varRelid,
											   jointype, sjinfo, rel,
											   estimatedclauses);

	return sel;
}

/*
 * examine_opclause_expression
 *		Split expression into Var and Const parts.
 *
 * Attempts to match the arguments to either (Var op Const) or (Const op Var),
 * possibly with a RelabelType on top. When the expression matches this form,
 * returns true, otherwise returns false.
 *
 * Optionally returns pointers to the extracted Var/Const nodes, when passed
 * non-null pointers (varp, cstp and varonleftp). The varonleftp flag specifies
 * on which side of the operator we found the Var node.
 */
bool
examine_clause_args(List *args, Var **varp, Const **cstp, bool *varonleftp)
{
	Var		   *var;
	Const	   *cst;
	bool		varonleft;
	Node	   *leftop,
			   *rightop;

	/* enforced by statext_is_compatible_clause_internal */
	Assert(list_length(args) == 2);

	leftop = linitial(args);
	rightop = lsecond(args);

	/* strip RelabelType from either side of the expression */
	if (IsA(leftop, RelabelType))
		leftop = (Node *) ((RelabelType *) leftop)->arg;

	if (IsA(rightop, RelabelType))
		rightop = (Node *) ((RelabelType *) rightop)->arg;

	if (IsA(leftop, Var) && IsA(rightop, Const))
	{
		var = (Var *) leftop;
		cst = (Const *) rightop;
		varonleft = true;
	}
	else if (IsA(leftop, Const) && IsA(rightop, Var))
	{
		var = (Var *) rightop;
		cst = (Const *) leftop;
		varonleft = false;
	}
	else
		return false;

	/* return pointers to the extracted parts if requested */
	if (varp)
		*varp = var;

	if (cstp)
		*cstp = cst;

	if (varonleftp)
		*varonleftp = varonleft;

	return true;
}

bool
examine_clause_args2(List *args, Node **exprp, Const **cstp, bool *expronleftp)
{
	Node	   *expr;
	Const	   *cst;
	bool		expronleft;
	Node	   *leftop,
			   *rightop;

	/* enforced by statext_is_compatible_clause_internal */
	Assert(list_length(args) == 2);

	leftop = linitial(args);
	rightop = lsecond(args);

	/* strip RelabelType from either side of the expression */
	if (IsA(leftop, RelabelType))
		leftop = (Node *) ((RelabelType *) leftop)->arg;

	if (IsA(rightop, RelabelType))
		rightop = (Node *) ((RelabelType *) rightop)->arg;

	if (IsA(rightop, Const))
	{
		expr = (Node *) leftop;
		cst = (Const *) rightop;
		expronleft = true;
	}
	else if (IsA(leftop, Const))
	{
		expr = (Node *) rightop;
		cst = (Const *) leftop;
		expronleft = false;
	}
	else
		return false;

	/* return pointers to the extracted parts if requested */
	if (exprp)
		*exprp = expr;

	if (cstp)
		*cstp = cst;

	if (expronleftp)
		*expronleftp = expronleft;

	return true;
}

bool
examine_opclause_expression(OpExpr *expr, Var **varp, Const **cstp, bool *varonleftp)
{
	Var		   *var;
	Const	   *cst;
	bool		varonleft;
	Node	   *leftop,
			   *rightop;

	/* enforced by statext_is_compatible_clause_internal */
	Assert(list_length(expr->args) == 2);

	leftop = linitial(expr->args);
	rightop = lsecond(expr->args);

	/* strip RelabelType from either side of the expression */
	if (IsA(leftop, RelabelType))
		leftop = (Node *) ((RelabelType *) leftop)->arg;

	if (IsA(rightop, RelabelType))
		rightop = (Node *) ((RelabelType *) rightop)->arg;

	if (IsA(leftop, Var) && IsA(rightop, Const))
	{
		var = (Var *) leftop;
		cst = (Const *) rightop;
		varonleft = true;
	}
	else if (IsA(leftop, Const) && IsA(rightop, Var))
	{
		var = (Var *) rightop;
		cst = (Const *) leftop;
		varonleft = false;
	}
	else
		return false;

	/* return pointers to the extracted parts if requested */
	if (varp)
		*varp = var;

	if (cstp)
		*cstp = cst;

	if (varonleftp)
		*varonleftp = varonleft;

	return true;
}

bool
examine_opclause_expression2(OpExpr *expr, Node **exprp, Const **cstp, bool *expronleftp)
{
	Node	   *expr2;
	Const	   *cst;
	bool		expronleft;
	Node	   *leftop,
			   *rightop;

	/* enforced by statext_is_compatible_clause_internal */
	Assert(list_length(expr->args) == 2);

	leftop = linitial(expr->args);
	rightop = lsecond(expr->args);

	/* strip RelabelType from either side of the expression */
	if (IsA(leftop, RelabelType))
		leftop = (Node *) ((RelabelType *) leftop)->arg;

	if (IsA(rightop, RelabelType))
		rightop = (Node *) ((RelabelType *) rightop)->arg;

	if (IsA(rightop, Const))
	{
		expr2 = (Node *) leftop;
		cst = (Const *) rightop;
		expronleft = true;
	}
	else if (IsA(leftop, Const))
	{
		expr2 = (Node *) rightop;
		cst = (Const *) leftop;
		expronleft = false;
	}
	else
		return false;

	/* return pointers to the extracted parts if requested */
	if (exprp)
		*exprp = expr2;

	if (cstp)
		*cstp = cst;

	if (expronleftp)
		*expronleftp = expronleft;

	return true;
}


/*
 * Compute statistics about expressions of a relation.
 */
static void
compute_expr_stats(Relation onerel, double totalrows,
				   AnlExprData *exprdata, int nexprs,
				   HeapTuple *rows, int numrows)
{
	MemoryContext expr_context,
				old_context;
	int			ind,
				i;

	expr_context = AllocSetContextCreate(CurrentMemoryContext,
										 "Analyze Expression",
										 ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(expr_context);

	for (ind = 0; ind < nexprs; ind++)
	{
		AnlExprData *thisdata = &exprdata[ind];
		Node        *expr = thisdata->expr;
		TupleTableSlot *slot;
		EState	   *estate;
		ExprContext *econtext;
		Datum	   *exprvals;
		bool	   *exprnulls;
		ExprState  *exprstate;
		int			tcnt;

		/*
		 * Need an EState for evaluation of expressions.  Create it in
		 * the per-expression context to be sure it gets cleaned up at
		 * the bottom of the loop.
		 */
		estate = CreateExecutorState();
		econtext = GetPerTupleExprContext(estate);

		/* Set up expression evaluation state */
		exprstate = ExecPrepareExpr((Expr *) expr, estate);

		/* Need a slot to hold the current heap tuple, too */
		slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel),
										&TTSOpsHeapTuple);

		/* Arrange for econtext's scan tuple to be the tuple under test */
		econtext->ecxt_scantuple = slot;

		/* Compute and save index expression values */
		exprvals = (Datum *) palloc(numrows * sizeof(Datum));
		exprnulls = (bool *) palloc(numrows * sizeof(bool));

		tcnt = 0;
		for (i = 0; i < numrows; i++)
		{
			Datum	datum;
			bool	isnull;

			/*
			 * Reset the per-tuple context each time, to reclaim any cruft
			 * left behind by evaluating the predicate or index expressions.
			 */
			ResetExprContext(econtext);

			/* Set up for predicate or expression evaluation */
			ExecStoreHeapTuple(rows[i], slot, false);

			/*
			 * FIXME this probably leaks memory. Maybe we should use
			 * ExecEvalExprSwitchContext but then we need to copy the
			 * result somewhere else.
			 */
			datum = ExecEvalExpr(exprstate,
								 GetPerTupleExprContext(estate),
								 &isnull);
			if (isnull)
			{
				exprvals[tcnt] = (Datum) 0;
				exprnulls[tcnt] = true;
			}
			else
			{
				exprvals[tcnt] = (Datum) datum;
				exprnulls[tcnt] = false;
			}

			tcnt++;
		}

		/*
		 * Now we can compute the statistics for the expression columns.
		 */
		if (tcnt > 0)
		{
			// MemoryContextSwitchTo(col_context);
			VacAttrStats *stats = thisdata->vacattrstat;
			AttributeOpts *aopt =
				get_attribute_options(stats->attr->attrelid,
									  stats->attr->attnum);

			stats->exprvals = exprvals;
			stats->exprnulls = exprnulls;
			stats->rowstride = 1;
			stats->compute_stats(stats,
								 expr_fetch_func,
								 tcnt,
								 tcnt);

			/*
			 * If the n_distinct option is specified, it overrides the
			 * above computation.
			 */
			if (aopt != NULL && aopt->n_distinct != 0.0)
				stats->stadistinct = aopt->n_distinct;

			// MemoryContextResetAndDeleteChildren(col_context);
		}

		/* And clean up */
		// MemoryContextSwitchTo(expr_context);

		ExecDropSingleTupleTableSlot(slot);
		FreeExecutorState(estate);
		// MemoryContextResetAndDeleteChildren(expr_context);
	}

	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(expr_context);
}


/*
 * Fetch function for analyzing index expressions.
 *
 * We have not bothered to construct index tuples, instead the data is
 * just in Datum arrays.
 */
static Datum
expr_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull)
{
	int			i;

	/* exprvals and exprnulls are already offset for proper column */
	i = rownum * stats->rowstride;
	*isNull = stats->exprnulls[i];
	return stats->exprvals[i];
}

/*
 * Build analyze data for a list of expressions. As this is not tied
 * directly to a relation (table or index), we have to fake some of
 * the data.
 */
static AnlExprData *
build_expr_data(List *exprs)
{
	int				idx;
	int				nexprs = list_length(exprs);
	AnlExprData	   *exprdata;
	ListCell	   *lc;

	exprdata = (AnlExprData *) palloc0(nexprs * sizeof(AnlExprData));

	idx = 0;
	foreach (lc, exprs)
	{
		Node		   *expr = (Node *) lfirst(lc);
		AnlExprData	   *thisdata = &exprdata[idx];

		thisdata->expr = expr;
		thisdata->vacattrstat = (VacAttrStats *) palloc(sizeof(VacAttrStats));

		thisdata->vacattrstat = examine_expression(expr);
		idx++;
	}

	return exprdata;
}

/*
 * examine_expression -- pre-analysis of a single column
 *
 * Determine whether the column is analyzable; if so, create and initialize
 * a VacAttrStats struct for it.  If not, return NULL.
 */
static VacAttrStats *
examine_expression(Node *expr)
{
	HeapTuple	typtuple;
	VacAttrStats *stats;
	int			i;
	bool		ok;

	Assert(expr != NULL);

	/*
	 * Create the VacAttrStats struct.
	 */
	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));

	/*
	 * When analyzing an expression, believe the expression tree's type.
	 */
	stats->attrtypid = exprType(expr);
	stats->attrtypmod = exprTypmod(expr);

	/*
	 * XXX Do we need to do anything special about the collation, similar
	 * to what examine_attribute does for expression indexes?
	 */
	stats->attrcollid = exprCollation(expr);

	/*
	 * We don't have any pg_attribute for expressions, so let's fake
	 * something reasonable into attstattarget, which is the only thing
	 * std_typanalyze needs.
	 */
	stats->attr = (Form_pg_attribute) palloc(ATTRIBUTE_FIXED_PART_SIZE);

	/*
	 * FIXME we should probably get the target from the extended stats
	 * object, or something like that.
	 */
	stats->attr->attstattarget = default_statistics_target;

	/* initialize some basic fields */
	stats->attr->attrelid = InvalidOid;
	stats->attr->attnum = InvalidAttrNumber;
	stats->attr->atttypid = stats->attrtypid;

	typtuple = SearchSysCacheCopy1(TYPEOID,
								   ObjectIdGetDatum(stats->attrtypid));
	if (!HeapTupleIsValid(typtuple))
		elog(ERROR, "cache lookup failed for type %u", stats->attrtypid);
	stats->attrtype = (Form_pg_type) GETSTRUCT(typtuple);
	stats->anl_context = CurrentMemoryContext;	/* XXX should be using something else? */
	stats->tupattnum = InvalidAttrNumber;

	/*
	 * The fields describing the stats->stavalues[n] element types default to
	 * the type of the data being analyzed, but the type-specific typanalyze
	 * function can change them if it wants to store something else.
	 */
	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		stats->statypid[i] = stats->attrtypid;
		stats->statyplen[i] = stats->attrtype->typlen;
		stats->statypbyval[i] = stats->attrtype->typbyval;
		stats->statypalign[i] = stats->attrtype->typalign;
	}

	/*
	 * Call the type-specific typanalyze function.  If none is specified, use
	 * std_typanalyze().
	 */
	if (OidIsValid(stats->attrtype->typanalyze))
		ok = DatumGetBool(OidFunctionCall1(stats->attrtype->typanalyze,
										   PointerGetDatum(stats)));
	else
		ok = std_typanalyze(stats);

	if (!ok || stats->compute_stats == NULL || stats->minrows <= 0)
	{
		heap_freetuple(typtuple);
		pfree(stats);
		return NULL;
	}

	return stats;
}

/* form an array of pg_statistic rows (per update_attstats) */
static Datum
serialize_expr_stats(AnlExprData *exprdata, int nexprs)
{
	int			exprno;
	Oid			typOid;
	Relation	sd;

	ArrayBuildState *astate = NULL;

	sd = table_open(StatisticRelationId, RowExclusiveLock);

	/* lookup OID of composite type for pg_statistic */
	typOid = get_rel_type_id(StatisticRelationId);
	if (!OidIsValid(typOid))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"pg_statistic\" does not have a composite type")));

	for (exprno = 0; exprno < nexprs; exprno++)
	{
		int				i, k;
		VacAttrStats   *stats = exprdata[exprno].vacattrstat;

		Datum		values[Natts_pg_statistic];
		bool		nulls[Natts_pg_statistic];
		HeapTuple	stup;

		if (!stats->stats_valid)
		{
			astate = accumArrayResult(astate,
									  (Datum) 0,
									  true,
									  typOid,
									  CurrentMemoryContext);
			continue;
		}

		/*
		 * Construct a new pg_statistic tuple
		 */
		for (i = 0; i < Natts_pg_statistic; ++i)
		{
			nulls[i] = false;
		}

		values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(InvalidOid);
		values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(InvalidAttrNumber);
		values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(false);
		values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
		values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stats->stawidth);
		values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stats->stadistinct);
		i = Anum_pg_statistic_stakind1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = Int16GetDatum(stats->stakind[k]); /* stakindN */
		}
		i = Anum_pg_statistic_staop1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = ObjectIdGetDatum(stats->staop[k]);	/* staopN */
		}
		i = Anum_pg_statistic_stacoll1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = ObjectIdGetDatum(stats->stacoll[k]);	/* stacollN */
		}
		i = Anum_pg_statistic_stanumbers1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			int			nnum = stats->numnumbers[k];

			if (nnum > 0)
			{
				int			n;
				Datum	   *numdatums = (Datum *) palloc(nnum * sizeof(Datum));
				ArrayType  *arry;

				for (n = 0; n < nnum; n++)
					numdatums[n] = Float4GetDatum(stats->stanumbers[k][n]);
				/* XXX knows more than it should about type float4: */
				arry = construct_array(numdatums, nnum,
									   FLOAT4OID,
									   sizeof(float4), true, TYPALIGN_INT);
				values[i++] = PointerGetDatum(arry);	/* stanumbersN */
			}
			else
			{
				nulls[i] = true;
				values[i++] = (Datum) 0;
			}
		}
		i = Anum_pg_statistic_stavalues1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			if (stats->numvalues[k] > 0)
			{
				ArrayType  *arry;

				arry = construct_array(stats->stavalues[k],
									   stats->numvalues[k],
									   stats->statypid[k],
									   stats->statyplen[k],
									   stats->statypbyval[k],
									   stats->statypalign[k]);
				values[i++] = PointerGetDatum(arry);	/* stavaluesN */
			}
			else
			{
				nulls[i] = true;
				values[i++] = (Datum) 0;
			}
		}

		stup = heap_form_tuple(RelationGetDescr(sd), values, nulls);

		astate = accumArrayResult(astate,
								  heap_copy_tuple_as_datum(stup, RelationGetDescr(sd)),
								  false,
								  typOid,
								  CurrentMemoryContext);
	}

	table_close(sd, RowExclusiveLock);

	return makeArrayResult(astate, CurrentMemoryContext);
}


/*
 * Loads pg_statistic record from expression statistics for expression
 * identified by the supplied index.
 */
HeapTuple
statext_expressions_load(Oid stxoid, int idx)
{
	bool		isnull;
	Datum		value;
	HeapTuple	htup;
	ExpandedArrayHeader *eah;
	HeapTupleHeader td;
	HeapTupleData tmptup;
	HeapTuple	tup;

	htup = SearchSysCache1(STATEXTDATASTXOID, ObjectIdGetDatum(stxoid));
	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for statistics object %u", stxoid);

	value = SysCacheGetAttr(STATEXTDATASTXOID, htup,
							Anum_pg_statistic_ext_data_stxdexpr, &isnull);
	if (isnull)
		elog(ERROR,
			 "requested statistic kind \"%c\" is not yet built for statistics object %u",
			 STATS_EXT_DEPENDENCIES, stxoid);

	eah = DatumGetExpandedArray(value);

	deconstruct_expanded_array(eah);

	td = DatumGetHeapTupleHeader(eah->dvalues[idx]);

	/* Build a temporary HeapTuple control structure */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
	tmptup.t_data = td;

	tup = heap_copytuple(&tmptup);

	ReleaseSysCache(htup);

	return tup;
}

/*
 * Evaluate the expressions, so that we can use the results to build
 * all the requested statistics types. This matters especially for
 * expensive expressions, of course.
 */
static ExprInfo *
evaluate_expressions(Relation rel, List *exprs, int numrows, HeapTuple *rows)
{
	/* evaluated expressions */
	ExprInfo   *result;
	char	   *ptr;
	Size		len;

	int			i;
	int			idx;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	List	   *exprstates = NIL;
	int			nexprs = list_length(exprs);
	ListCell   *lc;

	/* allocate everything as a single chunk, so we can free it easily */
	len = MAXALIGN(sizeof(ExprInfo));
	len += MAXALIGN(sizeof(Oid) * nexprs);	/* types */
	len += MAXALIGN(sizeof(Oid) * nexprs);	/* collations */

	/* values */
	len += MAXALIGN(sizeof(Datum *) * nexprs);
	len += nexprs * MAXALIGN(sizeof(Datum) * numrows);

	/* values */
	len += MAXALIGN(sizeof(bool *) * nexprs);
	len += nexprs * MAXALIGN(sizeof(bool) * numrows);

	ptr = palloc(len);

	/* set the pointers */
	result = (ExprInfo *) ptr;
	ptr += sizeof(ExprInfo);

	/* types */
	result->types = (Oid *) ptr;
	ptr += MAXALIGN(sizeof(Oid) * nexprs);

	/* collations */
	result->collations = (Oid *) ptr;
	ptr += MAXALIGN(sizeof(Oid) * nexprs);

	/* values */
	result->values = (Datum **) ptr;
	ptr += MAXALIGN(sizeof(Datum *) * nexprs);

	/* nulls */
	result->nulls = (bool **) ptr;
	ptr += MAXALIGN(sizeof(bool *) * nexprs);

	for (i = 0; i < nexprs; i++)
	{
		result->values[i] = (Datum *) ptr;
		ptr += MAXALIGN(sizeof(Datum) * numrows);

		result->nulls[i] = (bool *) ptr;
		ptr += MAXALIGN(sizeof(bool) * numrows);
	}

	Assert((ptr - (char *) result) == len);

	result->nexprs = list_length(exprs);

	idx = 0;
	foreach (lc, exprs)
	{
		Node *expr = (Node *) lfirst(lc);

		result->types[idx] = exprType(expr);
		result->collations[idx] = exprCollation(expr);

		idx++;
	}

	/*
	 * Need an EState for evaluation of index expressions and
	 * partial-index predicates.  Create it in the per-index context to be
	 * sure it gets cleaned up at the bottom of the loop.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);

	/* Need a slot to hold the current heap tuple, too */
	slot = MakeSingleTupleTableSlot(RelationGetDescr(rel),
									&TTSOpsHeapTuple);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up expression evaluation state */
	exprstates = ExecPrepareExprList(exprs, estate);

	for (i = 0; i < numrows; i++)
	{
		/*
		 * Reset the per-tuple context each time, to reclaim any cruft
		 * left behind by evaluating the predicate or index expressions.
		 */
		ResetExprContext(econtext);

		/* Set up for predicate or expression evaluation */
		ExecStoreHeapTuple(rows[i], slot, false);

		idx = 0;
		foreach (lc, exprstates)
		{
			Datum	datum;
			bool	isnull;
			ExprState *exprstate = (ExprState *) lfirst(lc);

			/*
			 * FIXME this probably leaks memory. Maybe we should use
			 * ExecEvalExprSwitchContext but then we need to copy the
			 * result somewhere else.
			 */
			datum = ExecEvalExpr(exprstate,
								 GetPerTupleExprContext(estate),
								 &isnull);
			if (isnull)
			{
				result->values[idx][i] = (Datum) 0;
				result->nulls[idx][i] = true;
			}
			else
			{
				result->values[idx][i] = (Datum) datum;
				result->nulls[idx][i] = false;
			}

			idx++;
		}
	}

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);

	return result;
}
