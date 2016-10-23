/*-------------------------------------------------------------------------
 *
 * common.c
 *	  POSTGRES multivariate statistics
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/mvstats/common.c
 *
 *-------------------------------------------------------------------------
 */

#include "common.h"
#include "utils/array.h"

static VacAttrStats **lookup_var_attr_stats(int2vector *attrs,
					  int natts, VacAttrStats **vacattrstats);

static List *list_mv_stats(Oid relid);

/*
 * Compute requested multivariate stats, using the rows sampled for the
 * plain (single-column) stats.
 *
 * This fetches a list of stats from pg_mv_statistic, computes the stats
 * and serializes them back into the catalog (as bytea values).
 */
void
build_mv_stats(Relation onerel, double totalrows,
			   int numrows, HeapTuple *rows,
			   int natts, VacAttrStats **vacattrstats)
{
	ListCell   *lc;
	List	   *mvstats;

	TupleDesc	tupdesc = RelationGetDescr(onerel);

	/*
	 * Fetch defined MV groups from pg_mv_statistic, and then compute the MV
	 * statistics (histograms for now).
	 */
	mvstats = list_mv_stats(RelationGetRelid(onerel));

	foreach(lc, mvstats)
	{
		int			j;
		MVStatisticInfo *stat = (MVStatisticInfo *) lfirst(lc);
		MVNDistinct	ndistinct = NULL;
		MVDependencies deps = NULL;
		MCVList		mcvlist = NULL;
		int			numrows_filtered = 0;

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
		Assert((attrs->dim1 >= 2) && (attrs->dim1 <= MVSTATS_MAX_DIMENSIONS));

		/* compute ndistinct coefficients */
		if (stat->ndist_enabled)
			ndistinct = build_mv_ndistinct(totalrows, numrows, rows, attrs, stats);

		/* analyze functional dependencies between the columns */
		if (stat->deps_enabled)
			deps = build_mv_dependencies(numrows, rows, attrs, stats);

		/* build the MCV list */
		if (stat->mcv_enabled)
			mcvlist = build_mv_mcvlist(numrows, rows, attrs, stats, &numrows_filtered);

		/* store the statistics in the catalog */
		update_mv_stats(stat->mvoid, ndistinct, deps, mcvlist, attrs, stats);
	}
}

/*
 * Lookup the VacAttrStats info for the selected columns, with indexes
 * matching the attrs vector (to make it easy to work with when
 * computing multivariate stats).
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
list_mv_stats(Oid relid)
{
	Relation	indrel;
	SysScanDesc indscan;
	ScanKeyData skey;
	HeapTuple	htup;
	List	   *result = NIL;

	/* Prepare to scan pg_mv_statistic for entries having indrelid = this rel. */
	ScanKeyInit(&skey,
				Anum_pg_mv_statistic_starelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	indrel = heap_open(MvStatisticRelationId, AccessShareLock);
	indscan = systable_beginscan(indrel, MvStatisticRelidIndexId, true,
								 NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(indscan)))
	{
		MVStatisticInfo *info = makeNode(MVStatisticInfo);
		Form_pg_mv_statistic stats = (Form_pg_mv_statistic) GETSTRUCT(htup);

		info->mvoid = HeapTupleGetOid(htup);
		info->stakeys = buildint2vector(stats->stakeys.values, stats->stakeys.dim1);
		info->ndist_enabled = stats->ndist_enabled;
		info->ndist_built = stats->ndist_built;
		info->deps_enabled = stats->deps_enabled;
		info->deps_built = stats->deps_built;
		info->mcv_enabled = stats->mcv_enabled;
		info->mcv_built = stats->mcv_built;

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
 * Find attnims of MV stats using the mvoid.
 */
int2vector *
find_mv_attnums(Oid mvoid, Oid *relid)
{
	ArrayType  *arr;
	Datum		adatum;
	bool		isnull;
	HeapTuple	htup;
	int2vector *keys;

	/* Prepare to scan pg_mv_statistic for entries having indrelid = this rel. */
	htup = SearchSysCache1(MVSTATOID,
						   ObjectIdGetDatum(mvoid));

	/* XXX syscache contains OIDs of deleted stats (not invalidated) */
	if (!HeapTupleIsValid(htup))
		return NULL;

	/* starelid */
	adatum = SysCacheGetAttr(MVSTATOID, htup,
							 Anum_pg_mv_statistic_starelid, &isnull);
	Assert(!isnull);

	*relid = DatumGetObjectId(adatum);

	/* stakeys */
	adatum = SysCacheGetAttr(MVSTATOID, htup,
							 Anum_pg_mv_statistic_stakeys, &isnull);
	Assert(!isnull);

	arr = DatumGetArrayTypeP(adatum);

	keys = buildint2vector((int16 *) ARR_DATA_PTR(arr),
						   ARR_DIMS(arr)[0]);
	ReleaseSysCache(htup);

	/*
	 * TODO maybe save the list into relcache, as in RelationGetIndexList
	 * (which was used as an inspiration of this one)?.
	 */

	return keys;
}

void
update_mv_stats(Oid mvoid,
				MVNDistinct ndistinct, MVDependencies dependencies, MCVList mcvlist,
				int2vector *attrs, VacAttrStats **stats)
{
	HeapTuple	stup,
				oldtup;
	Datum		values[Natts_pg_mv_statistic];
	bool		nulls[Natts_pg_mv_statistic];
	bool		replaces[Natts_pg_mv_statistic];

	Relation	sd = heap_open(MvStatisticRelationId, RowExclusiveLock);

	memset(nulls, 1, Natts_pg_mv_statistic * sizeof(bool));
	memset(replaces, 0, Natts_pg_mv_statistic * sizeof(bool));
	memset(values, 0, Natts_pg_mv_statistic * sizeof(Datum));

	/*
	 * Construct a new pg_mv_statistic tuple - replace only the histogram and
	 * MCV list, depending whether it actually was computed.
	 */
	if (ndistinct != NULL)
	{
		bytea	   *data = serialize_mv_ndistinct(ndistinct);

		nulls[Anum_pg_mv_statistic_standist -1] = (data == NULL);
		values[Anum_pg_mv_statistic_standist-1] = PointerGetDatum(data);
	}

	if (dependencies != NULL)
	{
		nulls[Anum_pg_mv_statistic_stadeps - 1] = false;
		values[Anum_pg_mv_statistic_stadeps - 1]
			= PointerGetDatum(serialize_mv_dependencies(dependencies));
	}

	if (mcvlist != NULL)
	{
		bytea	   *data = serialize_mv_mcvlist(mcvlist, attrs, stats);

		nulls[Anum_pg_mv_statistic_stamcv - 1] = (data == NULL);
		values[Anum_pg_mv_statistic_stamcv - 1] = PointerGetDatum(data);
	}

	/* always replace the value (either by bytea or NULL) */
	replaces[Anum_pg_mv_statistic_standist - 1] = true;
	replaces[Anum_pg_mv_statistic_stadeps - 1] = true;
	replaces[Anum_pg_mv_statistic_stamcv - 1] = true;

	/* always change the availability flags */
	nulls[Anum_pg_mv_statistic_deps_built - 1] = false;
	nulls[Anum_pg_mv_statistic_ndist_built - 1] = false;
	nulls[Anum_pg_mv_statistic_mcv_built - 1] = false;

	nulls[Anum_pg_mv_statistic_stakeys - 1] = false;

	/* use the new attnums, in case we removed some dropped ones */
	replaces[Anum_pg_mv_statistic_deps_built - 1] = true;
	replaces[Anum_pg_mv_statistic_ndist_built - 1] = true;
	replaces[Anum_pg_mv_statistic_mcv_built - 1] = true;

	replaces[Anum_pg_mv_statistic_stakeys - 1] = true;

	values[Anum_pg_mv_statistic_ndist_built - 1] = BoolGetDatum(ndistinct != NULL);
	values[Anum_pg_mv_statistic_deps_built - 1] = BoolGetDatum(dependencies != NULL);
	values[Anum_pg_mv_statistic_mcv_built - 1] = BoolGetDatum(mcvlist != NULL);

	values[Anum_pg_mv_statistic_stakeys - 1] = PointerGetDatum(attrs);

	/* Is there already a pg_mv_statistic tuple for this attribute? */
	oldtup = SearchSysCache1(MVSTATOID,
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
		simple_heap_update(sd, &stup->t_self, stup);
	}
	else
		elog(ERROR, "invalid pg_mv_statistic record (oid=%d)", mvoid);

	/* update indexes too */
	CatalogUpdateIndexes(sd, stup);

	heap_freetuple(stup);

	heap_close(sd, RowExclusiveLock);
}


int
mv_get_index(AttrNumber varattno, int2vector *stakeys)
{
	int			i,
				idx = 0;

	for (i = 0; i < stakeys->dim1; i++)
	{
		if (stakeys->values[i] < varattno)
			idx += 1;
		else
			break;
	}
	return idx;
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
	return compare_datums_simple(*(Datum *) a,
								 *(Datum *) b,
								 (SortSupport) arg);
}

int
compare_datums_simple(Datum a, Datum b, SortSupport ssup)
{
	return ApplySortComparator(a, false, b, false, ssup);
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
