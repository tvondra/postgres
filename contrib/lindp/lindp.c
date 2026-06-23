/*-------------------------------------------------------------------------
 *
 * lindp.c
 *	  Linearized dynamic-programming join search (LinDP) prototype.
 *
 * This module installs a join_search_hook that, for sufficiently large join
 * problems, replaces GEQO with a deterministic search-space linearization:
 * IKKBZ computes a good left-deep order and LinDP runs exact interval DP over
 * the contiguous sub-ranges of that order, building real join relations (and
 * thus the real PostgreSQL cost model and full set of paths) through
 * make_join_rel().
 *
 * Because every DP cell is a contiguous range of the linear order, the search
 * space is O(n^2) cells with O(n) splits each, i.e. O(n^3) make_join_rel()
 * calls, which scales to far more relations than exhaustive DP while still
 * producing bushy plans within the linearized neighbourhood.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/lindp/lindp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "fmgr.h"
#include "lindp.h"
#include "miscadmin.h"
#include "optimizer/geqo.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "utils/guc.h"

PG_MODULE_MAGIC_EXT(
					.name = "lindp",
					.version = PG_VERSION
);

/* GUCs */
static bool		lindp_enabled = true;
static bool		lindp_fallback = false;
static int		lindp_min_threshold = 12;
static int		lindp_max_threshold = 100;
static int		lindp_effort = -1;

/* Saved previous hook value, restored on unload. */
static join_search_hook_type prev_join_search_hook = NULL;

void		_PG_init(void);
void		_PG_fini(void);

/*
 * Run the cheapest-path finalization that standard_join_search() performs for
 * each joinrel: partitionwise paths, partial/gather paths (except the topmost
 * rel), grouped paths, and set_cheapest().
 */
static void
finalize_joinrel(PlannerInfo *root, RelOptInfo *joinrel)
{
	bool		is_top_rel = bms_equal(joinrel->relids, root->all_query_rels);

	generate_partitionwise_join_paths(root, joinrel);

	if (!is_top_rel)
		generate_useful_gather_paths(root, joinrel, false);

	set_cheapest(joinrel);

	if (joinrel->grouped_rel != NULL && !is_top_rel)
	{
		RelOptInfo *grouped_rel = joinrel->grouped_rel;

		Assert(IS_GROUPED_REL(grouped_rel));

		generate_grouped_paths(root, grouped_rel, joinrel);
		set_cheapest(grouped_rel);
	}
}

/*
 * The LinDP interval DP.
 *
 * memo[i][j] is the best join relation covering the contiguous range
 * order[i..j] of the linear order; it is NULL if no legal join order exists
 * for that range.  Larger ranges are built from all (left, right) splits at a
 * single point, exactly like a classic interval DP.  make_join_rel() caches
 * by relid set, so repeated splits accumulate paths on the same RelOptInfo;
 * we finalize each range once after all its splits have been considered.
 *
 * Returns the relation for the whole range, or NULL on failure.
 */
static RelOptInfo *
lindp_interval_dp(PlannerInfo *root, LinDpGraph *graph, int *order)
{
	int			n = graph->n;
	RelOptInfo ***memo;
	int			i,
				len;
	RelOptInfo *result;

	memo = palloc_array(RelOptInfo **, n);
	for (i = 0; i < n; i++)
	{
		memo[i] = palloc0_array(RelOptInfo *, n);
		/* Base case: single relation. */
		memo[i][i] = graph->rels[order[i]];
	}

	for (len = 2; len <= n; len++)
	{
		for (i = 0; i + len - 1 < n; i++)
		{
			int			j = i + len - 1;
			int			k;
			RelOptInfo *built = NULL;

			CHECK_FOR_INTERRUPTS();

			for (k = i; k < j; k++)
			{
				RelOptInfo *left = memo[i][k];
				RelOptInfo *right = memo[k + 1][j];
				RelOptInfo *jr;

				if (left == NULL || right == NULL)
					continue;

				/*
				 * make_join_rel() builds (or returns the cached) joinrel for
				 * the union and adds the paths for this particular split.  It
				 * returns NULL if this join is not legal (e.g. it would
				 * violate outer-join ordering).
				 */
				jr = make_join_rel(root, left, right);
				if (jr != NULL)
					built = jr;
			}

			if (built != NULL)
				finalize_joinrel(root, built);

			memo[i][j] = built;
		}
	}

	result = memo[0][n - 1];

	for (i = 0; i < n; i++)
		pfree(memo[i]);
	pfree(memo);

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
	root->initial_rels = initial_rels;

	if (prev_join_search_hook)
		return prev_join_search_hook(root, levels_needed, initial_rels);
	else if (enable_geqo && levels_needed >= geqo_threshold)
		return geqo(root, levels_needed, initial_rels);
	else
		return standard_join_search(root, levels_needed, initial_rels);
}

/*
 * The actual LinDP join order search hook, with a fallback to the current
 * join search algorithm (DP + GEQO).
 */
RelOptInfo *
lindp_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)
{
	LinDpGraph *graph;
	int		   *order;
	RelOptInfo *rel;

	/* This must hold for make_join_rel() to behave like during DP search. */
	Assert(root->join_rel_level == NULL);

	/*
	 * Disabled module, too small and too large problems: let the default search
	 * handle it.
	 */
	if (!lindp_enabled ||
		(levels_needed < lindp_min_threshold) ||
		(levels_needed > lindp_max_threshold))
	{
		return fallback_default_search(root, levels_needed, initial_rels);
	}

	/*
	 * Run the actual LinDP join search. First build the join graph, then
	 * run the IK/KBZ linearization, and then finally the interval DP on
	 * the seed order produced by the IK/KBZ.
	 *
	 * XXX Maybe we should return multiple orders from IK/KBZ and try the
	 * linearization on each? Could that help with failing to find a plan
	 * for some queries (i.e. we'd fail less often)?
	 */
	graph = lindp_build_graph(root, initial_rels);

	/* seed order from IKKBZ */
	order = lindp_ikkbz_order(graph, lindp_effort);

	/* run the interval DP using the seed order */
	rel = lindp_interval_dp(root, graph, order);

	/*
	 * The linearization can fail to find a legal complete order in the
	 * presence of strong outer-join or LATERAL ordering constraints. When
	 * that happens, fall back to the default search, which is guaranteed
	 * to succeed (or if it fails, it's no worse than without LinDP).
	 */
	if ((rel == NULL) && lindp_fallback)
		return fallback_default_search(root, levels_needed, initial_rels);
	else if (rel == NULL)
		elog(ERROR, "LinDP linearization failed to find a valid plan");

	return rel;
}

void
_PG_init(void)
{
	DefineCustomBoolVariable("lindp.enabled",
							 "Use linearized (IKKBZ + LinDP) join search.",
							 NULL,
							 &lindp_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("lindp.fallback",
							 "Fallback to built-in join search if linearization fails.",
							 NULL,
							 &lindp_fallback,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("lindp.min_threshold",
							"Minimum number of relations to trigger LinDP.",
							"Below this many relations the default join search "
							"(exhaustive dynamic programming) is used instead.",
							&lindp_min_threshold,
							12,
							2, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lindp.max_threshold",
							"Maximum number of relations LinDP will optimize.",
							"Above this many relations the default join search "
							"is used instead.",
							&lindp_max_threshold,
							100,
							2, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lindp.effort",
							"Linearization effort (higher tries more IKKBZ roots).",
							NULL,
							&lindp_effort,
							-1,
							-1, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("lindp");

	/* Install our join search hook, chaining any previous one. */
	prev_join_search_hook = join_search_hook;
	join_search_hook = lindp_join_search;
}

void
_PG_fini(void)
{
	join_search_hook = prev_join_search_hook;
}
