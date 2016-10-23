/*-------------------------------------------------------------------------
 *
 * common.c
 *	  POSTGRES extended statistics
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/statistics/common.c
 *
 *-------------------------------------------------------------------------
 */

#include "common.h"

static VacAttrStats **lookup_var_attr_stats(int2vector *attrs,
					  int natts, VacAttrStats **vacattrstats);

static List *list_ext_stats(Oid relid);

static void update_ext_stats(Oid relid, MVNDistinct ndistinct,
					  int2vector *attrs, VacAttrStats **stats);


/*
 * Compute requested extended stats, using the rows sampled for the plain
 * (single-column) stats.
 *
 * This fetches a list of stats from pg_statistic_ext, computes the stats
 * and serializes them back into the catalog (as bytea values).
 */
void
build_ext_stats(Relation onerel, double totalrows,
				int numrows, HeapTuple *rows,
				int natts, VacAttrStats **vacattrstats)
{
	ListCell   *lc;
	List	   *stats;

	TupleDesc	tupdesc = RelationGetDescr(onerel);

	/* Fetch defined statistics from pg_statistic_ext, and compute them. */
	stats = list_ext_stats(RelationGetRelid(onerel));

	foreach(lc, stats)
	{
		int			j;
		StatisticExtInfo *stat = (StatisticExtInfo *) lfirst(lc);
		MVNDistinct	ndistinct = NULL;

		VacAttrStats **stats = NULL;
		int			numatts = 0;

		/* int2 vector of attnums the stats should be computed on */
		int2vector *attrs = stat->stakeys;

		/* see how many of the columns are not dropped */
		for (j = 0; j < attrs->dim1; j++)
			if (!tupdesc->attrs[attrs->values[j] - 1]->attisdropped)
				numatts += 1;

		/* if there are dropped attributes, build a filtered int2vector */
		if (numatts != attrs->dim1)
		{
			int16	   *tmp = palloc0(numatts * sizeof(int16));
			int			attnum = 0;

			for (j = 0; j < attrs->dim1; j++)
				if (!tupdesc->attrs[attrs->values[j] - 1]->attisdropped)
					tmp[attnum++] = attrs->values[j];

			pfree(attrs);
			attrs = buildint2vector(tmp, numatts);
		}

		/* filter only the interesting vacattrstats records */
		stats = lookup_var_attr_stats(attrs, natts, vacattrstats);

		/* check allowed number of dimensions */
		Assert((attrs->dim1 >= 2) && (attrs->dim1 <= STATS_MAX_DIMENSIONS));

		/* compute ndistinct coefficients */
		if (stat->ndist_enabled)
			ndistinct = build_ext_ndistinct(totalrows, numrows, rows, attrs, stats);

		/* store the statistics in the catalog */
		update_ext_stats(stat->mvoid, ndistinct, attrs, stats);
	}
}

/*
 * Lookup the VacAttrStats info for the selected columns, with indexes
 * matching the attrs vector (to make it easy to work with when
 * computing extended stats).
 */
static VacAttrStats **
lookup_var_attr_stats(int2vector *attrs, int natts, VacAttrStats **vacattrstats)
{
	int			i,
				j;
	int			numattrs = attrs->dim1;
	VacAttrStats **stats = (VacAttrStats **) palloc0(numattrs * sizeof(VacAttrStats *));

	/* lookup VacAttrStats info for the requested columns (same attnum) */
	for (i = 0; i < numattrs; i++)
	{
		stats[i] = NULL;
		for (j = 0; j < natts; j++)
		{
			if (attrs->values[i] == vacattrstats[j]->tupattnum)
			{
				stats[i] = vacattrstats[j];
				break;
			}
		}

		/*
		 * Check that we found the info, that the attnum matches and that
		 * there's the requested 'lt' operator and that the type is
		 * 'passed-by-value'.
		 */
		Assert(stats[i] != NULL);
		Assert(stats[i]->tupattnum == attrs->values[i]);

		/*
		 * FIXME This is rather ugly way to check for 'ltopr' (which is
		 * defined for 'scalar' attributes).
		 */
		Assert(((StdAnalyzeData *) stats[i]->extra_data)->ltopr != InvalidOid);
	}

	return stats;
}

/*
 * Fetch list of MV stats defined on a table, without the actual data
 * for histograms, MCV lists etc.
 */
static List *
list_ext_stats(Oid relid)
{
	Relation	indrel;
	SysScanDesc indscan;
	ScanKeyData skey;
	HeapTuple	htup;
	List	   *result = NIL;

	/* Prepare to scan pg_statistic_ext for entries having indrelid = this rel. */
	ScanKeyInit(&skey,
				Anum_pg_statistic_ext_starelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	indrel = heap_open(StatisticExtRelationId, AccessShareLock);
	indscan = systable_beginscan(indrel, StatisticExtRelidIndexId, true,
								 NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(indscan)))
	{
		StatisticExtInfo *info = makeNode(StatisticExtInfo);
		Form_pg_statistic_ext stats = (Form_pg_statistic_ext) GETSTRUCT(htup);

		info->mvoid = HeapTupleGetOid(htup);
		info->stakeys = buildint2vector(stats->stakeys.values, stats->stakeys.dim1);

		info->ndist_enabled = stats_are_enabled(htup, STATS_EXT_NDISTINCT);
		info->ndist_built = stats_are_built(htup, STATS_EXT_NDISTINCT);

		result = lappend(result, info);
	}

	systable_endscan(indscan);

	heap_close(indrel, AccessShareLock);

	/*
	 * TODO maybe save the list into relcache, as in RelationGetIndexList
	 * (which was used as an inspiration of this one)?.
	 */

	return result;
}

/*
 * update_ext_stats
 *	Serializes the statistics and stores them into the pg_statistic_ext tuple.
 */
static void
update_ext_stats(Oid mvoid, MVNDistinct ndistinct,
				int2vector *attrs, VacAttrStats **stats)
{
	HeapTuple	stup,
				oldtup;
	Datum		values[Natts_pg_statistic_ext];
	bool		nulls[Natts_pg_statistic_ext];
	bool		replaces[Natts_pg_statistic_ext];

	Relation	sd = heap_open(StatisticExtRelationId, RowExclusiveLock);

	memset(nulls, 1, Natts_pg_statistic_ext * sizeof(bool));
	memset(replaces, 0, Natts_pg_statistic_ext * sizeof(bool));
	memset(values, 0, Natts_pg_statistic_ext * sizeof(Datum));

	/*
	 * Construct a new pg_statistic_ext tuple - replace only the histogram and
	 * MCV list, depending whether it actually was computed.
	 */
	if (ndistinct != NULL)
	{
		bytea	   *data = serialize_ext_ndistinct(ndistinct);

		nulls[Anum_pg_statistic_ext_standistinct -1] = (data == NULL);
		values[Anum_pg_statistic_ext_standistinct-1] = PointerGetDatum(data);
	}

	/* always replace the value (either by bytea or NULL) */
	replaces[Anum_pg_statistic_ext_standistinct - 1] = true;

	/* always change the availability flags */
	nulls[Anum_pg_statistic_ext_stakeys - 1] = false;

	/* use the new attnums, in case we removed some dropped ones */
	replaces[Anum_pg_statistic_ext_stakeys - 1] = true;

	values[Anum_pg_statistic_ext_stakeys - 1] = PointerGetDatum(attrs);

	/* Is there already a pg_statistic_ext tuple for this attribute? */
	oldtup = SearchSysCache1(STATEXTOID,
							 ObjectIdGetDatum(mvoid));

	if (HeapTupleIsValid(oldtup))
	{
		/* Yes, replace it */
		stup = heap_modify_tuple(oldtup,
								 RelationGetDescr(sd),
								 values,
								 nulls,
								 replaces);
		ReleaseSysCache(oldtup);
		CatalogTupleUpdate(sd, &stup->t_self, stup);
	}
	else
		elog(ERROR, "invalid pg_statistic_ext record (oid=%d)", mvoid);

	heap_freetuple(stup);

	heap_close(sd, RowExclusiveLock);
}

/* multi-variate stats comparator */

/*
 * qsort_arg comparator for sorting Datums (MV stats)
 *
 * This does not maintain the tupnoLink array.
 */
int
compare_scalars_simple(const void *a, const void *b, void *arg)
{
	Datum		da = *(Datum *) a;
	Datum		db = *(Datum *) b;
	SortSupport ssup = (SortSupport) arg;

	return ApplySortComparator(da, false, db, false, ssup);
}

/*
 * qsort_arg comparator for sorting data when partitioning a MV bucket
 */
int
compare_scalars_partition(const void *a, const void *b, void *arg)
{
	Datum		da = ((ScalarItem *) a)->value;
	Datum		db = ((ScalarItem *) b)->value;
	SortSupport ssup = (SortSupport) arg;

	return ApplySortComparator(da, false, db, false, ssup);
}

/* initialize multi-dimensional sort */
MultiSortSupport
multi_sort_init(int ndims)
{
	MultiSortSupport mss;

	Assert(ndims >= 2);

	mss = (MultiSortSupport) palloc0(offsetof(MultiSortSupportData, ssup)
									 +sizeof(SortSupportData) * ndims);

	mss->ndims = ndims;

	return mss;
}

/*
 * add sort into for dimension 'dim' (index into vacattrstats) to mss,
 * at the position 'sortattr'
 */
void
multi_sort_add_dimension(MultiSortSupport mss, int sortdim,
						 int dim, VacAttrStats **vacattrstats)
{
	/* first, lookup StdAnalyzeData for the dimension (attribute) */
	SortSupportData ssup;
	StdAnalyzeData *tmp = (StdAnalyzeData *) vacattrstats[dim]->extra_data;

	Assert(mss != NULL);
	Assert(sortdim < mss->ndims);

	/* initialize sort support, etc. */
	memset(&ssup, 0, sizeof(ssup));
	ssup.ssup_cxt = CurrentMemoryContext;

	/* We always use the default collation for statistics */
	ssup.ssup_collation = DEFAULT_COLLATION_OID;
	ssup.ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(tmp->ltopr, &ssup);

	mss->ssup[sortdim] = ssup;
}

/* compare all the dimensions in the selected order */
int
multi_sort_compare(const void *a, const void *b, void *arg)
{
	int			i;
	SortItem   *ia = (SortItem *) a;
	SortItem   *ib = (SortItem *) b;

	MultiSortSupport mss = (MultiSortSupport) arg;

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

bool
stats_are_enabled(HeapTuple htup, char type)
{
	bool	retval;
	Datum	datum;
	bool	isnull;

	/* decoding the array value */
	int			i,
				nenabled;
	char	   *enabled;
	ArrayType  *enabledArray;

	/* see which statistics are enabled */
	datum = SysCacheGetAttr(STATEXTOID, htup,
							Anum_pg_statistic_ext_staenabled, &isnull);

	/* if there are no values in staenabled field, everything is enabled */
	if (isnull || (datum == PointerGetDatum(NULL)))
		return false;

	/*
	 * We expect the array to be a 1-D CHAR array; verify that. We don't
	 * need to use deconstruct_array() since the array data is just going
	 * to look like a C array of char values.
	 */
	enabledArray = DatumGetArrayTypeP(datum);

	if (ARR_NDIM(enabledArray) != 1 ||
		ARR_HASNULL(enabledArray) ||
		ARR_ELEMTYPE(enabledArray) != CHAROID)
		elog(ERROR, "enabled statistics (staenabled) is not a 1-D char array");

	nenabled = ARR_DIMS(enabledArray)[0];
	enabled = (char *) ARR_DATA_PTR(enabledArray);

	retval = false;
	for (i = 0; i < nenabled; i++)
	{
		if (enabled[i] == type)
		{
			retval = true;
			break;
		}
	}

	return retval;
}

bool
stats_are_built(HeapTuple htup, char type)
{
	bool	isnull;

	switch (type)
	{
		case STATS_EXT_NDISTINCT:
			SysCacheGetAttr(STATEXTOID, htup,
							Anum_pg_statistic_ext_standistinct, &isnull);
			break;

		default:
			elog(ERROR, "unexcpected statistics type requested: %d", type);
	}

	return (! isnull);
}
