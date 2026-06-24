/*-------------------------------------------------------------------------
 *
 * lindp.c
 *	  A join_search_hook implementing LinDP++ (search-space linearization).
 *
 * This module provides an alternative to GEQO for planning queries with a
 * large number of relations.  Instead of a genetic algorithm, it uses the
 * "linearized dynamic programming" approach:
 *
 *	 1. The join graph is linearized with the IKKBZ algorithm, which produces
 *		a (near) optimal left-deep ordering of the relations under the ASI
 *		cost model (see Krishnamurthy/Boral/Zaniolo, and Ibaraki/Kameda).
 *
 *	 2. Dynamic programming is then run, but restricted to *contiguous*
 *		subsequences (intervals) of that linear order.  Within an interval all
 *		bushy splits are considered, so the result is the optimal plan whose
 *		relation sets form contiguous intervals of the linearization.  This is
 *		O(n^3) instead of the exponential cost of exhaustive DP, which is what
 *		makes it usable for many relations.  See:
 *			Radke & Neumann, "LinDP++: Generalizing Linearized DP to
 *			 Crossproducts and Non-Inner Joins" (BTW 2019).
 *			Birler, Radke & Neumann, adaptive LinDP improvements.
 *
 *	 3. The "adaptive" improvement keeps several candidate linearizations
 *		(seeded from different IKKBZ roots) and runs the interval DP for each,
 *		keeping the cheapest result.
 *
 * Correctness is guaranteed independently of plan quality: all candidate
 * joins are built with make_join_rel(), which enforces every join-order
 * restriction (outer joins, LATERAL, complex/multi-relation clauses, ...).
 * Splits that are not legal are simply skipped.  Cross products (joins of
 * relations from disconnected components of the join graph) are fully
 * supported.  If, for some query, the chosen linear order admits no legal
 * decomposition into the full relation set, the module transparently falls
 * back to the in-core standard_join_search().
 *
 * Copyright (c) 2024-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/lindp/lindp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "lindp.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/pathnodes.h"
#include "optimizer/geqo.h"
#include "optimizer/joininfo.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "utils/guc.h"

PG_MODULE_MAGIC_EXT(
					.name = "lindp3",
					.version = PG_VERSION
);

void		_PG_init(void);
void		_PG_fini(void);

/* GUC variables */
static bool lindp_enabled = true;
static bool lindp_adaptive = true;
static bool lindp_fallback = false;
static int	lindp_min_threshold = 12;
static int	lindp_max_threshold = 100;
static int	lindp_seeds = 5;

/* Saved hook value, for chaining / restore. */
static join_search_hook_type prev_join_search_hook = NULL;

static RelOptInfo *lindp_join_search(PlannerInfo *root, int levels_needed,
									 List *initial_rels);
static RelOptInfo *lindp_run(LinDPState *st, int levels_needed);


/*
 * Module load / unload
 */
void
_PG_init(void)
{
	DefineCustomBoolVariable("lindp3.enabled",
							 "Enables LinDP++ join order search.",
							 "When off, the installed hook delegates to the "
							 "in-core join search.",
							 &lindp_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("lindp3.fallback",
							 "Fallback to built-in join search if linearization fails.",
							 NULL,
							 &lindp_fallback,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("lindp3.min_threshold",
							"Minimum number of relations for using LinDP++.",
							"Join problems with fewer relations use exact "
							"dynamic programming (standard_join_search).",
							&lindp_min_threshold,
							12,
							2, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lindp3.max_threshold",
							"Maximum number of relations handled by LinDP++.",
							"Larger join problems fall back to the in-core join "
							"search.  0 means no limit.",
							&lindp_max_threshold,
							100,
							2, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("lindp3.adaptive",
							 "Enables adaptive multi-seed linearization.",
							 "When on, several IKKBZ linearizations are tried "
							 "and the cheapest resulting plan is kept.",
							 &lindp_adaptive,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("lindp3.seeds",
							"Number of linearizations tried per component.",
							"Only used when lindp.adaptive is on.",
							&lindp_seeds,
							5,
							1, 1000,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("lindp3");

	/* Install the hook (chaining to any previously installed one). */
	prev_join_search_hook = join_search_hook;
	join_search_hook = lindp_join_search;
}

void
_PG_fini(void)
{
	join_search_hook = prev_join_search_hook;
}


/*
 * Run the per-relation post-processing that standard_join_search() performs
 * after a joinrel is fully built: partitionwise paths, gather paths (except
 * for the topmost rel), set_cheapest(), and grouped paths.
 */
static void
lindp_finalize_joinrel(PlannerInfo *root, RelOptInfo *joinrel)
{
	bool		is_top_rel = bms_equal(joinrel->relids, root->all_query_rels);

	generate_partitionwise_join_paths(root, joinrel);

	if (!is_top_rel)
		generate_useful_gather_paths(root, joinrel, false);

	set_cheapest(joinrel);

	if (joinrel->grouped_rel != NULL && !is_top_rel)
	{
		generate_grouped_paths(root, joinrel->grouped_rel, joinrel);
		set_cheapest(joinrel->grouped_rel);
	}
}

/*
 * Build (or reuse) the join relation covering exactly the relations of the
 * interval [lo,hi] of the linear 'order', considering every contiguous split.
 *
 * best[a][b] holds the already-built joinrel for sub-interval [a,b] of the
 * same order (0-based).  Returns NULL if no legal split exists.
 *
 * If an equivalent joinrel has already been completely built (during a
 * previous seed or component), it is reused without adding duplicate paths.
 */
static RelOptInfo *
lindp_build_interval(LinDPState *st, int *order,
					 RelOptInfo ***best, int lo, int hi)
{
	PlannerInfo *root = st->root;
	Relids		relids = NULL;
	RelOptInfo *existing;
	RelOptInfo *joinrel = NULL;
	int			k;

	/* Compute the relid set of this interval. */
	for (k = lo; k <= hi; k++)
		relids = bms_add_members(relids, st->rels[order[k]]->relids);

	/* If we already finished this exact join earlier, just reuse it. */
	existing = find_join_rel(root, relids);
	if (existing != NULL && existing->cheapest_total_path != NULL)
	{
		bms_free(relids);
		return existing;
	}
	bms_free(relids);

	/*
	 * Try every contiguous split.  make_join_rel() builds the same joinrel
	 * object for the whole interval regardless of the split point, so each
	 * legal split simply contributes more candidate paths.
	 */
	for (k = lo; k < hi; k++)
	{
		RelOptInfo *l = best[lo][k];
		RelOptInfo *r = best[k + 1][hi];
		RelOptInfo *jr;

		if (l == NULL || r == NULL)
			continue;

		jr = make_join_rel(root, l, r);
		if (jr != NULL)
			joinrel = jr;
	}

	if (joinrel == NULL)
		return NULL;

	lindp_finalize_joinrel(root, joinrel);

	return joinrel;
}

/*
 * Run the linearized interval DP for one candidate order (a List of int of
 * global relation indexes).  Returns the joinrel covering all of them, or
 * NULL if no legal contiguous decomposition exists.
 */
static RelOptInfo *
lindp_solve_order(LinDPState *st, List *order)
{
	int			m = list_length(order);
	int		   *ord;
	RelOptInfo ***best;
	RelOptInfo *result;
	int			i,
				len;

	ord = (int *) palloc(m * sizeof(int));
	for (i = 0; i < m; i++)
		ord[i] = list_nth_int(order, i);

	/* best[a][b] = joinrel for interval [a,b] (0-based within this order) */
	best = (RelOptInfo ***) palloc(m * sizeof(RelOptInfo **));
	for (i = 0; i < m; i++)
		best[i] = (RelOptInfo **) palloc0(m * sizeof(RelOptInfo *));

	for (i = 0; i < m; i++)
		best[i][i] = st->rels[ord[i]];

	for (len = 2; len <= m; len++)
	{
		for (i = 0; i + len - 1 < m; i++)
		{
			int			j = i + len - 1;

			best[i][j] = lindp_build_interval(st, ord, best, i, j);
		}
	}

	result = best[0][m - 1];

	for (i = 0; i < m; i++)
		pfree(best[i]);
	pfree(best);
	pfree(ord);

	return result;
}

static int
ordercand_cmp(const void *a, const void *b)
{
	double		ca = ((const OrderCandidate *) a)->cost;
	double		cb = ((const OrderCandidate *) b)->cost;

	if (ca < cb)
		return -1;
	if (ca > cb)
		return 1;
	return 0;
}

/*
 * Solve one connected component: return the cheapest joinrel covering all of
 * its relations, or NULL on failure.
 */
static RelOptInfo *
lindp_solve_component(LinDPState *st, List *comp)
{
	int			compsz = list_length(comp);
	int			ncand;
	int			nkeep;
	OrderCandidate *cands;
	RelOptInfo *bestrel = NULL;
	ListCell   *lc;
	int			i;

	if (compsz == 1)
		return st->rels[linitial_int(comp)];

	/* Generate one IKKBZ order per possible root. */
	cands = (OrderCandidate *) palloc(compsz * sizeof(OrderCandidate));
	ncand = 0;
	foreach(lc, comp)
	{
		int			r = lfirst_int(lc);

		cands[ncand].order = ikkbz_order_for_root(st, comp, r,
												  &cands[ncand].cost);
		ncand++;
	}

	qsort(cands, ncand, sizeof(OrderCandidate), ordercand_cmp);

	/* How many linearizations to actually run the DP for. */
	nkeep = lindp_adaptive ? lindp_seeds : 1;
	if (nkeep > ncand)
		nkeep = ncand;

	for (i = 0; i < nkeep; i++)
	{
		RelOptInfo *rel;

		CHECK_FOR_INTERRUPTS();

		rel = lindp_solve_order(st, cands[i].order);
		if (rel == NULL)
			continue;
		if (bestrel == NULL ||
			rel->cheapest_total_path->total_cost <
			bestrel->cheapest_total_path->total_cost)
			bestrel = rel;
	}

	pfree(cands);

	return bestrel;
}


/*****************************************************************************
 *		Top-level driver
 *****************************************************************************/

/*
 * Partition the relations into connected components of the join graph and
 * return them as a List of Lists of int (global indexes).
 */
static List *
lindp_find_components(LinDPState *st)
{
	int			n = st->n;
	int		   *comp = (int *) palloc(n * sizeof(int));
	int		   *queue = (int *) palloc(n * sizeof(int));
	List	   *components = NIL;
	int			i;

	for (i = 0; i < n; i++)
		comp[i] = -1;

	for (i = 0; i < n; i++)
	{
		int			qhead = 0,
					qtail = 0;
		List	   *members;

		if (comp[i] != -1)
			continue;

		members = NIL;
		comp[i] = i;
		queue[qtail++] = i;
		while (qhead < qtail)
		{
			int			u = queue[qhead++];
			int			w;

			members = lappend_int(members, u);
			for (w = 0; w < n; w++)
			{
				if (comp[w] != -1)
					continue;
				if (!ikkbz_rels_connected(st, u, w))
					continue;
				comp[w] = i;
				queue[qtail++] = w;
			}
		}
		components = lappend(components, members);
	}

	pfree(comp);
	pfree(queue);
	return components;
}

/*
 * The actual LinDP++ search.  Returns the final joinrel, or NULL to request a
 * fallback to standard_join_search().
 */
static RelOptInfo *
lindp_run(LinDPState *st, int levels_needed)
{
	List	   *components = lindp_find_components(st);
	RelOptInfo *result = NULL;
	ListCell   *lc;

	foreach(lc, components)
	{
		List	   *comp = (List *) lfirst(lc);
		RelOptInfo *comprel = lindp_solve_component(st, comp);

		if (comprel == NULL)
			return NULL;		/* fall back */

		if (result == NULL)
			result = comprel;
		else
		{
			/* Join the components together with a cross product. */
			RelOptInfo *joinrel;
			RelOptInfo *existing;
			Relids		relids = bms_union(result->relids, comprel->relids);

			existing = find_join_rel(st->root, relids);
			bms_free(relids);
			if (existing != NULL && existing->cheapest_total_path != NULL)
				joinrel = existing;
			else
			{
				joinrel = make_join_rel(st->root, result, comprel);
				if (joinrel == NULL)
					return NULL;	/* fall back */
				lindp_finalize_joinrel(st->root, joinrel);
			}
			result = joinrel;
		}
	}

	/* The final relation must cover the whole query. */
	if (result == NULL ||
		!bms_equal(result->relids, st->root->all_query_rels) ||
		result->cheapest_total_path == NULL)
		return NULL;

	return result;
}

/*
 * Fallback to default join search, used if
 *
 * - join is too small (does not exceed the lindp.threshold)
 * - join is too large (exceeds the lindp.max_relations threshold)
 * - LinDP fails to find a valid plan
 *
 * We try geqo first (if enabled and the join is large enough to qualify), or
 * the full standard_join_search.
 *
 * XXX This is the same pattern as in make_rel_from_joinlist().
 */
static RelOptInfo *
fallback_default_search(PlannerInfo *root, int levels_needed, List *initial_rels)
{
	if (prev_join_search_hook)
		return prev_join_search_hook(root, levels_needed, initial_rels);
	else if (enable_geqo && levels_needed >= geqo_threshold)
		return geqo(root, levels_needed, initial_rels);
	else
		return standard_join_search(root, levels_needed, initial_rels);
}

/*
 * join_search_hook entry point.
 */
static RelOptInfo *
lindp_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)
{
	LinDPState	st;
	RelOptInfo *result;
	ListCell   *lc;
	int			i;

	/*
	 * Decide whether LinDP++ should handle this problem.  Small problems are
	 * better served by exact dynamic programming, and we honor an optional
	 * upper bound.  In all "decline" cases we use the in-core exhaustive
	 * search so that we never invoke GEQO ourselves.
	 */
	if (!lindp_enabled ||
		(levels_needed < lindp_min_threshold) ||
		(levels_needed > lindp_max_threshold))
	{
		return fallback_default_search(root, levels_needed, initial_rels);
	}

	/* Build working state. */
	st.root = root;
	st.n = levels_needed;
	st.rels = (RelOptInfo **) palloc(st.n * sizeof(RelOptInfo *));
	st.card = (double *) palloc(st.n * sizeof(double));
	st.parent = (int *) palloc(st.n * sizeof(int));
	st.trel = (double *) palloc(st.n * sizeof(double));
	st.children = (List **) palloc0(st.n * sizeof(List *));

	i = 0;
	foreach(lc, initial_rels)
	{
		RelOptInfo *rel = (RelOptInfo *) lfirst(lc);

		st.rels[i] = rel;
		st.card[i] = rel->rows;
		if (st.card[i] < 1.0)
			st.card[i] = 1.0;
		i++;
	}

	result = lindp_run(&st, levels_needed);

	if (result != NULL)
		return result;

	/*
	 * The linearization admitted no legal full decomposition for this query
	 * (e.g. an outer-join order restriction incompatible with the order).
	 * Fall back to the in-core exhaustive search, which is guaranteed to
	 * succeed.  Any join relations already built are simply reused.
	 */
	if (lindp_fallback)
		return fallback_default_search(root, levels_needed, initial_rels);
	else
		elog(ERROR, "LinDP linearization failed to find a valid plan (%d rels)",
			 levels_needed);
}
