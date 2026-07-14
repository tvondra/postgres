/*-------------------------------------------------------------------------
 *
 * lindp.c
 *	  A join_search_hook implementing the "LinDP++" join ordering algorithm.
 *
 * This module installs a join_search_hook that replaces PostgreSQL's
 * standard dynamic-programming join search with the linearized dynamic
 * programming approach (LinDP++) described in three papers:
 *
 * [1] T. Neumann, B. Radke: "Adaptive Optimization of Very Large Join
 *     Queries", SIGMOD 2018.
 *     https://db.in.tum.de/~radke/papers/hugejoins.pdf
 *
 * [2] B. Radke, T. Neumann: "LinDP++: Generalizing Linearized DP to
 *     Crossproducs and Non-Inner Joins", BTW2019.
 *     https://db.in.tum.de/~radke/papers/lindp++.pdf
 *
 * [3] A. Birler, M. Stoian, T. Neumann: "Optimizing Linearized Join
 *     Enumeration by Adapting to the Query Structure", BTW2025.
 *     https://db.in.tum.de/~birler/papers/adaptivelindp.pdf
 *
 * The first two papers are foundational.  The first paper lays out the
 * complete join planning framework, and the LinDP algorithm as it's main
 * part.  Second paper extends the LinDP to support all join queries, by
 * using hypergraphs.  The third paper presents optimizations so that it
 * handles even larger problems.
 *
 * The implementation follows the approach laid out in the papers:
 *
 *	1. Build a hypergraph representing the join structure.  Vertices are the
 *	   relations to be joined; simple edges represent join predicates,
 *	   hyperedges capture the ordering restrictions imposed by outer joins
 *	   (and semi/anti joins), and synthetic "cross" edges connect otherwise
 *	   disconnected components so that cross products can be considered.
 *
 *	2. Linearize the hypergraph into a single sequence of relations using a
 *	   generalization of the IKKBZ algorithm to hypergraphs (Algorithm 2 in
 *	   paper [2]).  Outer-join hyperedges are handled by a recursive
 *	   precedence-graph decomposition: the vertex set is split at a hyperedge
 *	   into its two sides, each side is linearized recursively, and the
 *	   results are concatenated.  Simple, connected sub-problems are
 *	   linearized with the classic rank-based IKKBZ tree algorithm
 *	   (precedence tree + normalization).
 *
 *	3. Run a linearized dynamic program over the resulting order: a join is
 *	   only formed between two contiguous sub-sequences of the order.  This
 *	   restricts the classic O(2^n) DP to an O(n^3) interval DP while still
 *	   allowing bushy plans.  The interval DP naturally forms cross products
 *	   for splits that lack a join predicate, which -- together with the
 *	   cross edges added in step 1 -- realizes the paper's heuristic of
 *	   enriching the search space with cross products.
 *
 *	4. The linearization is only a heuristic, so we evaluate several seed
 *	   relations (roots) for the IKKBZ pass, keep the cheapest resulting
 *	   plan, and fall back to the standard join search whenever no legal
 *	   linearized plan can be produced.  The LinDP++ algorithm should find
 *	   a valid join order / plan for abitrary join queries, so a fallback
 *	   should not be needed.
 *
 * Building the actual join relations is delegated to make_join_rel(), which
 * enforces all join-legality rules.  As a result the plans produced here are
 * always valid; the algorithm above only decides *which* orders to consider.
 *
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/lindp/lindp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <float.h>
#include <limits.h>
#include <math.h>

#include "miscadmin.h"
#include "nodes/pathnodes.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/joininfo.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "utils/guc.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

/* GUC variables */
static bool lindp_enabled = true;
static bool lindp_debug = false;
static bool lindp_fallback_enabled = true;
static int	lindp_min_relations = 2;
static int	lindp_max_relations = 0;	/* 0 means "no limit" */
static int	lindp_seeds = 5;
static bool lindp_cross_products = true;

/* Saved previous hook, so several join-search plugins can coexist. */
static join_search_hook_type prev_join_search_hook = NULL;

/* A tiny positive number used to avoid division by zero in rank math. */
#define LINDP_EPSILON	1e-9

/*
 * A vertex of the join hypergraph: one of the relations to be joined.
 */
typedef struct LinDPVertex
{
	int			index;			/* position in the hypergraph, 0 .. nverts-1 */
	RelOptInfo *rel;			/* the relation itself */
	Relids		relids;			/* rel->relids (not owned) */
	double		rows;			/* estimated number of rows (>= 1) */
} LinDPVertex;

/*
 * A hyperedge induced by an outer/semi/anti join.  rhs_verts is the set of
 * vertex indices that lie syntactically inside the right-hand side of the
 * join, and lhs_verts the set syntactically inside the left-hand side.  A
 * vertex of the current sub-problem may belong to neither (it was contributed
 * by a join that syntactically encloses this one); such vertices are placed
 * after the two sides during the decomposition.  These edges drive the
 * recursive precedence-graph decomposition in the linearization step.
 */
typedef struct LinDPHyperEdge
{
	Bitmapset  *rhs_verts;		/* vertex indices fully inside the RHS */
	Bitmapset  *lhs_verts;		/* vertex indices fully inside the LHS */
} LinDPHyperEdge;

/*
 * The join hypergraph.
 */
typedef struct LinDPHypergraph
{
	int			nverts;
	LinDPVertex *verts;
	Bitmapset **adj;			/* adj[i] = simple-edge neighbours of vertex i */
	double	  **sel;			/* sel[i][j] = estimated join selectivity */
	List	   *hyperedges;		/* list of LinDPHyperEdge * (outer joins) */
} LinDPHypergraph;

/*
 * A "module" used by the IKKBZ tree algorithm.  A module is a contiguous
 * sub-sequence of vertices that the normalization step has decided must be
 * kept together, together with its accumulated ASI statistics.
 */
typedef struct LinDPModule
{
	List	   *seq;			/* list of vertex indices (int) */
	double		T;				/* product of per-relation size factors */
	double		C;				/* ASI cost of the sequence */
	double		rank;			/* (T - 1) / C */
} LinDPModule;

void		_PG_init(void);

static RelOptInfo *lindp_join_search(PlannerInfo *root, int levels_needed,
									 List *initial_rels);
static RelOptInfo *lindp_fallback(PlannerInfo *root, int levels_needed,
								  List *initial_rels);

static LinDPHypergraph *lindp_build_hypergraph(PlannerInfo *root,
											   const List *initial_rels);
static void lindp_print_hypergraph(LinDPHypergraph *hg);
static double lindp_edge_selectivity(PlannerInfo *root,
									 const RelOptInfo *rel1, const RelOptInfo *rel2);
static bool lindp_is_multicomp(const LinDPHypergraph *hg);

static int *lindp_linearize(const LinDPHypergraph *hg, int seed);
static int *lindp_linearize_set(const LinDPHypergraph *hg, const Bitmapset *vmask,
								int root_hint);
static int *lindp_ikkbz_chain(const LinDPHypergraph *hg, const Bitmapset *vmask,
							  int root_hint);
static Bitmapset *lindp_component(const LinDPHypergraph *hg, int start,
								  const Bitmapset *vmask);
static List *lindp_ikkbz_solve(const LinDPHypergraph *hg, int v, int parent,
							   const Bitmapset *vmask, Bitmapset **visited);
static List *lindp_merge_chains(List *chains);
static List *lindp_normalize_chain(List *chain);
static LinDPModule *lindp_make_module(const LinDPHypergraph *hg, int v, int parent);
static LinDPModule *lindp_combine_modules(const LinDPModule *a, const LinDPModule *b);

static RelOptInfo *lindp_run_dp(PlannerInfo *root, LinDPHypergraph *hg,
								int *order, int n, bool final);
static void lindp_finalize_joinrel(PlannerInfo *root, RelOptInfo *rel);

static int	lindp_pick_root(const LinDPHypergraph *hg, const Bitmapset *vmask,
							int root_hint);

/*
 * Module load callback.
 */
void
_PG_init(void)
{
	DefineCustomBoolVariable("lindp_hyper.enabled",
							 "Enables the LinDP++ join search hook.",
							 NULL,
							 &lindp_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("lindp_hyper.fallback",
							 "Fallback to stadard join search if linearization fails?",
							 NULL,
							 &lindp_fallback_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("lindp_hyper.debug",
							 "Print various internal debug info (as warnings)",
							 NULL,
							 &lindp_debug,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("lindp_hyper.min_relations",
							"Minimum number of relations for LinDP++ to engage.",
							"For fewer relations the standard join search is used.",
							&lindp_min_relations,
							2, 2, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lindp_hyper.max_relations",
							"Maximum number of relations for LinDP++ to engage.",
							"0 disables the limit.  Above the limit the standard "
							"join search (or GEQO) is used instead.",
							&lindp_max_relations,
							0, 0, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lindp_hyper.seeds",
							"Number of seed relations tried as IKKBZ roots.",
							"More seeds explore more linearizations.  0 means "
							"try every relation as a root.",
							&lindp_seeds,
							5, 0, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("lindp_hyper.cross_products",
							 "Enriches the search space with cross products.",
							 "When off, disconnected join problems fall back to "
							 "the standard join search.",
							 &lindp_cross_products,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("lindp_hyper");

	/* Install the hook, chaining to any previously installed hook. */
	prev_join_search_hook = join_search_hook;
	join_search_hook = lindp_join_search;
}

/*
 * Fall back to whatever join search would have run without this module.
 */
static RelOptInfo *
lindp_fallback(PlannerInfo *root, int levels_needed, List *initial_rels)
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
 * Choose the seed relations to try as IKKBZ roots.  We order the vertices
 * by ascending cardinality, since small relations make good roots, and
 * keep at most lindp.seeds of them.
 */
static int *
lindp_ikkbz_seeds(LinDPHypergraph *hg, int n, int nseeds)
{
	int		   *seedrels;

	seedrels = palloc_array(int, n);
	for (int i = 0; i < n; i++)
		seedrels[i] = i;

	/*
	 * Pick the seeds using a simple selection sort. It seems good enough,
	 * because we only ever expect a relatively small number of entries, and
	 * we simply compare the cardinalities, which is cheap.
	 *
	 * FIXME I think we could use (i < nseeds), to stop early.
	 */
	for (int i = 0; i < n; i++)
	{
		for (int j = i + 1; j < n; j++)
		{
			if (hg->verts[seedrels[j]].rows < hg->verts[seedrels[i]].rows)
			{
				int			tmp = seedrels[i];

				seedrels[i] = seedrels[j];
				seedrels[j] = tmp;
			}
		}
	}

	return seedrels;
}

/*
 * The join_search_hook entry point.
 */
static RelOptInfo *
lindp_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)
{
	LinDPHypergraph *hg;
	int			n = levels_needed;
	int			nseeds;
	int		   *seedrels;
	int		   *best_order = NULL;
	double		best_fitness = DBL_MAX;
	RelOptInfo *result;

	/* Decide whether LinDP++ should handle this join problem at all. */
	if (!lindp_enabled ||
		(n < lindp_min_relations) ||
		(lindp_max_relations > 0 && n > lindp_max_relations))
		return lindp_fallback(root, levels_needed, initial_rels);

	hg = lindp_build_hypergraph(root, initial_rels);

	if (lindp_debug)
		lindp_print_hypergraph(hg);

	/*
	 * If cross products are disabled and the join graph is disconnected, we
	 * cannot build a connected plan without them.  In that case fall back to
	 * the standard join search (which forms cross products when needed).
	 */
	if (!lindp_cross_products && lindp_is_multicomp(hg))
		return lindp_fallback(root, levels_needed, initial_rels);

	/*
	 * Choose the seed relations to try as IKKBZ roots.
	 */
	nseeds = (lindp_seeds <= 0) ? n : Min(lindp_seeds, n);
	seedrels = lindp_ikkbz_seeds(hg, n, nseeds);

	/*
	 * Phase 1: pick the best IKKBZ seeds / linearizations
	 *
	 * Pick the IKKBZ seed that is expected to generate the best final plan.
	 * We linearize and evaluate the resulting plan, and keep the cheapest
	 * linearization.
	 *
	 * We evaluate the plan in a private memory context, mirroring
	 * geqo_eval(). We must restore join_rel_list and join_rel_hash after each
	 * trial so the throw-away join relations do not leak.
	 *
	 * We run the DP with "final" set to true so that each candidate is costed
	 * exactly as it would be in the final build, including the Gather path on
	 * the top relation (see lindp_finalize_joinrel()).  Scoring seeds by a
	 * serial-only cost while the chosen plan is ultimately parallel would
	 * make the ranking inconsistent with the final plan cost, so that adding
	 * more seeds could pick a higher-cost plan.
	 *
	 * XXX There's a trade off between doing a cheap vs. accurate costing of
	 * the seeds. On the one hand we want to cost the seeds cheaply, so that
	 * we can evaluate more of them. But on the other hand we want the cost to
	 * be close to the "actual" cost, otherwise we can see weird effects. For
	 * example, with approximate costs, we may get a worse final plan after
	 * increasing the number of seeds (one of the additional seeds may seem
	 * cheaper, but end up with a worse "full" cost).
	 *
	 * We used to skip the finalization when costing the IKKBZ seeds, in which
	 * case we did not consider plans with a gather on top. We compared the
	 * plans using just serial-only costs, but maybe one plan could be made
	 * parallel.
	 *
	 * XXX This essentially does a pull planning for each IKKBZ seed, as it
	 * runs lindp_run_dp() with exactly the same parameters as later. It
	 * should still be relatively cheap, as the linearization fixes the join
	 * order, though. However, it means the later lindp_run_dp() call is
	 * entirely duplicate to what we already did when costing the seed. Maybe
	 * we could save the work somehow?
	 *
	 * XXX Maybe move to a separate function, just like lindp_ikkbz_seeds?
	 */
	for (int i = 0; i < nseeds; i++)
	{
		MemoryContext mycontext;
		MemoryContext oldcxt;
		int			savelength;
		struct HTAB *savehash;
		int		   *order;
		RelOptInfo *top;
		double		fitness;

		CHECK_FOR_INTERRUPTS();

		/*
		 * XXX maybe don't recreate the context over and over, and just reset
		 * it after each seed
		 */
		mycontext = AllocSetContextCreate(CurrentMemoryContext,
										  "LinDP",
										  ALLOCSET_DEFAULT_SIZES);
		oldcxt = MemoryContextSwitchTo(mycontext);

		savelength = list_length(root->join_rel_list);
		savehash = root->join_rel_hash;
		Assert(root->join_rel_level == NULL);
		root->join_rel_hash = NULL;

		/* derive the linearization (for the seed), and run the LinDP */
		order = lindp_linearize(hg, seedrels[i]);
		top = lindp_run_dp(root, hg, order, n, true);

		/* we use the cost of the cheapest total path as cost of the seed */
		if (top->cheapest_total_path != NULL)
			fitness = top->cheapest_total_path->total_cost;
		else
			fitness = DBL_MAX;

		/* Restore the planner's join relation bookkeeping. */
		root->join_rel_list = list_truncate(root->join_rel_list, savelength);
		root->join_rel_hash = savehash;

		MemoryContextSwitchTo(oldcxt);

		if (fitness < best_fitness)
		{
			best_fitness = fitness;

			/* Copy the winning order into the planner's context. */
			best_order = palloc_array(int, n);
			memcpy(best_order, order, n * sizeof(int));
		}

		MemoryContextDelete(mycontext);
	}

	/*
	 * No usable linearization found: fall back to the standard search / GEQO.
	 *
	 * With LinDP++ the fallback should not be needed, the algorithm should
	 * find a valid join order / plan for abitrary joins.
	 */
	if (best_order == NULL || best_fitness == DBL_MAX)
	{
		/* with fallback disabled, simply error out */
		if (!lindp_fallback_enabled)
			elog(ERROR, "LinDP linearization failed to find a valid plan");

		return lindp_fallback(root, levels_needed, initial_rels);
	}

	/*
	 * Phase 2: build a full plan for the best linearization
	 *
	 * This time do that in the regular memory context, etc.
	 */
	result = lindp_run_dp(root, hg, best_order, n, true);

	/*
	 * Defensive fallback if the final build somehow failed.
	 *
	 * XXX This should not really happen. lindp_run_dp should always produce a
	 * valid plan. Moreover we've already done this for this order earlier,
	 * and it produced a valid plan. Maybe no fallback here?
	 */
	if (result == NULL)
	{
		/* with fallback disabled, simply error out */
		if (!lindp_fallback_enabled)
			elog(ERROR, "LinDP linearization failed to find a valid plan");

		return lindp_fallback(root, levels_needed, initial_rels);
	}

	return result;
}

/*
 * Build the join hypergraph from the list of relations to be joined.
 */
static LinDPHypergraph *
lindp_build_hypergraph(PlannerInfo *root, const List *initial_rels)
{
	LinDPHypergraph *hg = palloc0_object(LinDPHypergraph);
	int			n = list_length(initial_rels);
	ListCell   *lc;
	int			i;

	hg->nverts = n;
	hg->verts = palloc0_array(LinDPVertex, n);
	hg->adj = palloc0_array(Bitmapset *, n);
	hg->sel = palloc0_array(double *, n);
	hg->hyperedges = NIL;

	/* vertices are relations to be joined */
	i = 0;
	foreach(lc, initial_rels)
	{
		RelOptInfo *rel = lfirst_node(RelOptInfo, lc);

		hg->verts[i].index = i;
		hg->verts[i].rel = rel;
		hg->verts[i].relids = rel->relids;
		hg->verts[i].rows = Max(rel->rows, 1.0);
		i++;
	}

	/* initialize the selectivity matrix to 1.0 */
	for (i = 0; i < n; i++)
	{
		hg->sel[i] = palloc_array(double, n);
		for (int j = 0; j < n; j++)
			hg->sel[i][j] = 1.0;
	}

	/* Simple edges: a join predicate (or order restriction) between rels. */
	for (i = 0; i < n; i++)
	{
		for (int j = i + 1; j < n; j++)
		{
			RelOptInfo *ri = hg->verts[i].rel;
			RelOptInfo *rj = hg->verts[j].rel;

			if (have_relevant_joinclause(root, ri, rj) ||
				have_join_order_restriction(root, ri, rj))
			{
				double		s = lindp_edge_selectivity(root, ri, rj);

				hg->adj[i] = bms_add_member(hg->adj[i], j);
				hg->adj[j] = bms_add_member(hg->adj[j], i);
				hg->sel[i][j] = s;
				hg->sel[j][i] = s;
			}
		}
	}

	/*
	 * Outer-join hyperedges.  For each outer/semi/anti join we record the set
	 * of vertices that are syntactically inside its right-hand side and the
	 * set inside its left-hand side.  This drives the recursive
	 * precedence-graph decomposition during linearization, ensuring the two
	 * sides of the join stay contiguous and correctly ordered.
	 */
	foreach(lc, root->join_info_list)
	{
		SpecialJoinInfo *sjinfo = lfirst_node(SpecialJoinInfo, lc);
		LinDPHyperEdge *he;
		Bitmapset  *rhs = NULL;
		Bitmapset  *lhs = NULL;

		if (sjinfo->jointype == JOIN_INNER)
			continue;

		for (i = 0; i < n; i++)
		{
			if (bms_is_subset(hg->verts[i].relids, sjinfo->syn_righthand))
				rhs = bms_add_member(rhs, i);
			else if (bms_is_subset(hg->verts[i].relids, sjinfo->syn_lefthand))
				lhs = bms_add_member(lhs, i);
		}

		/* Only useful if it actually splits the vertex set. */
		if (bms_is_empty(rhs) || bms_is_empty(lhs))
		{
			bms_free(rhs);
			bms_free(lhs);
			continue;
		}

		he = palloc0_object(LinDPHyperEdge);
		he->rhs_verts = rhs;
		he->lhs_verts = lhs;
		hg->hyperedges = lappend(hg->hyperedges, he);
	}

	return hg;
}

/*
 * Print the hypergraph (for debugging purposes).
 */
static void
lindp_print_hypergraph(LinDPHypergraph *hg)
{
	ListCell   *lc;
	StringInfoData str;

	initStringInfo(&str);

	/* vertices first, with relids */
	appendStringInfoString(&str, "vertices: {\n");
	for (int i = 0; i < hg->nverts; i++)
	{
		appendStringInfo(&str, "\t%d => [%s]\n", i, bmsToString(hg->verts[i].relids));
	}
	appendStringInfoString(&str, "}\n");

	/* adjacency matrix */
	appendStringInfoString(&str, "adjacency: {\n");
	for (int i = 0; i < hg->nverts; i++)
	{
		appendStringInfoString(&str, "\t");
		for (int j = 0; j < hg->nverts; j++)
		{
			if (j > 0)
				appendStringInfoString(&str, ", ");

			if (bms_is_member(j, hg->adj[i]))
				appendStringInfoString(&str, "1");
			else
				appendStringInfoString(&str, "0");
		}
		appendStringInfoString(&str, "\n");
	}
	appendStringInfoString(&str, "}\n");

	/* selectivity matrix */
	appendStringInfoString(&str, "selectivity: {\n");
	for (int i = 0; i < hg->nverts; i++)
	{
		appendStringInfoString(&str, "\t");
		for (int j = 0; j < hg->nverts; j++)
		{
			if (j > 0)
				appendStringInfoString(&str, ", ");

			appendStringInfo(&str, "%f", hg->sel[i][j]);
		}
		appendStringInfoString(&str, "\n");
	}
	appendStringInfoString(&str, "}\n");

	/* hyperedges */
	appendStringInfoString(&str, "hyperedges: {\n");
	foreach (lc, hg->hyperedges)
	{
		LinDPHyperEdge *edge = (LinDPHyperEdge *) lfirst(lc);

		appendStringInfo(&str, "\t[%s] => [%s]\n", bmsToString(edge->rhs_verts), bmsToString(edge->lhs_verts));
	}
	appendStringInfoString(&str, "}");

	elog(WARNING, "%s", str.data);
}

/*
 * Estimate the join selectivity between two relations from the join clauses
 * that connect them.  This is only used to rank relations during
 * linearization, so a rough estimate is fine.
 */
static double
lindp_edge_selectivity(PlannerInfo *root, const RelOptInfo *rel1, const RelOptInfo *rel2)
{
	Relids		joinrelids = bms_union(rel1->relids, rel2->relids);
	List	   *clauses = NIL;
	ListCell   *lc;
	double		sel;
	SpecialJoinInfo sjinfo;

	foreach(lc, rel1->joininfo)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		if (bms_is_subset(rinfo->clause_relids, joinrelids) &&
			bms_overlap(rinfo->clause_relids, rel1->relids) &&
			bms_overlap(rinfo->clause_relids, rel2->relids))
			clauses = lappend(clauses, rinfo);
	}

	bms_free(joinrelids);

	if (clauses == NIL)
	{
		/*
		 * No explicit clause found (for example the relations are connected
		 * only through an equivalence class or an order restriction).  Use a
		 * conservative default so such edges still rank ahead of pure cross
		 * products.
		 */
		return 0.1;
	}

	/*
	 * Build a dummy JOIN_INNER SpecialJoinInfo describing the join between
	 * the two relations.  Passing a non-NULL sjinfo is essential: with a NULL
	 * sjinfo, clause_selectivity() treats a join clause as a restriction
	 * clause, which both yields a bogus estimate here and (worse) caches that
	 * wrong selectivity in the RestrictInfo's norm_selec field, corrupting
	 * the real join cost estimates computed later for the chosen plan.
	 */
	init_dummy_sjinfo(&sjinfo, rel1->relids, rel2->relids);

	sel = clauselist_selectivity(root, clauses, 0, JOIN_INNER, &sjinfo);
	list_free(clauses);

	/* Keep the value strictly inside (0, 1]. */
	if (sel < 0)
		sel = 0;
	else if (sel > 1.0)
		sel = 1.0;

	return sel;
}

/*
 * Check if there are more than one connected components of the hypergraph
 * over its simple edges.
 */
static bool
lindp_is_multicomp(const LinDPHypergraph *hg)
{
	Bitmapset  *vmask = NULL;
	Bitmapset  *comp;
	int			compsize;

	for (int v = 0; v < hg->nverts; v++)
		vmask = bms_add_member(vmask, v);

	comp = lindp_component(hg, 0, NULL);

	compsize = bms_num_members(comp);

	bms_free(comp);

	return compsize < hg->nverts;
}

/*
 * Produce a full linearization of the hypergraph rooted at the given seed.
 *
 * Returns a palloc'd array of length hg->nverts containing a permutation of
 * the vertex indices, or NULL on failure.  The order respects the recursive
 * decomposition imposed by outer-join hyperedges.
 */
static int *
lindp_linearize(const LinDPHypergraph *hg, int seed)
{
	Bitmapset  *vmask = NULL;
	int		   *order;

	for (int i = 0; i < hg->nverts; i++)
		vmask = bms_add_member(vmask, i);

	order = lindp_linearize_set(hg, vmask, seed);

	bms_free(vmask);
	return order;
}

/*
 * Recursive precedence-graph decomposition at hyperedges.
 *
 * We split vmask at the "outermost" outer-join hyperedge, that is, the one
 * whose left- and right-hand sides together cover the most vertices of vmask.
 * The vertices are partitioned into three contiguous blocks that are each
 * linearized independently and concatenated in order:
 *
 *	  left-hand side, right-hand side, then any leftover vertices.
 *
 * The leftover block holds vertices that belong to neither side of the chosen
 * join; these were contributed by joins that syntactically enclose it (for
 * example an inner join sitting above a full join in the syntax tree), so they
 * must be joined *after* the enclosed outer join has been formed.  Keeping
 * each side -- and the leftover -- contiguous is exactly what lets the
 * interval DP find a legal parenthesization; placing the leftover after the
 * right-hand side is what prevents an enclosing relation from being
 * interleaved into the middle of a rigid outer-join chain.
 *
 * Splitting at the largest-coverage hyperedge first lets the recursion resolve
 * the nested hyperedges of a left-deep outer-join chain in order.  When no
 * hyperedge splits vmask, we fall through to the classic IKKBZ tree
 * linearization for the (simple) sub-problem.
 */
static int *
lindp_linearize_set(const LinDPHypergraph *hg, const Bitmapset *vmask, int root_hint)
{
	int			count = bms_num_members(vmask);
	ListCell   *lc;
	LinDPHyperEdge *best_edge = NULL;
	int			best_cover = 0;
	Bitmapset  *bestL = NULL;
	Bitmapset  *bestR = NULL;

	if (count <= 1)
	{
		int		   *order = palloc_array(int, 1);

		if (count == 1)
			order[0] = bms_singleton_member(vmask);
		return order;
	}

	/*
	 * Look for the hyperedge that covers the most of vmask while still
	 * leaving both of its sides non-empty within vmask (so that it genuinely
	 * splits the set).  Ties are broken by list order, which keeps the choice
	 * stable.
	 */
	foreach(lc, hg->hyperedges)
	{
		LinDPHyperEdge *he = (LinDPHyperEdge *) lfirst(lc);
		Bitmapset  *rv = bms_intersect(he->rhs_verts, vmask);
		Bitmapset  *lv = bms_intersect(he->lhs_verts, vmask);
		int			rsize = bms_num_members(rv);
		int			lsize = bms_num_members(lv);
		int			cover = rsize + lsize;

		if (rsize > 0 && lsize > 0 && cover <= count && cover > best_cover)
		{
			bms_free(bestR);
			bms_free(bestL);
			best_cover = cover;
			best_edge = he;
			bestR = rv;
			bestL = lv;
		}
		else
		{
			bms_free(rv);
			bms_free(lv);
		}
	}

	if (best_edge != NULL)
	{
		Bitmapset  *leftover = bms_difference(vmask, bestL);
		int		   *orderL;
		int		   *orderR;
		int		   *orderX = NULL;
		int			lenL = bms_num_members(bestL);
		int			lenR = bms_num_members(bestR);
		int			lenX = 0;
		int		   *order;
		int			rl = lindp_pick_root(hg, bestL, root_hint);
		int			rr = lindp_pick_root(hg, bestR, root_hint);

		leftover = bms_del_members(leftover, bestR);

		orderL = lindp_linearize_set(hg, bestL, rl);
		orderR = lindp_linearize_set(hg, bestR, rr);
		if (!bms_is_empty(leftover))
		{
			int			rx = lindp_pick_root(hg, leftover, root_hint);

			orderX = lindp_linearize_set(hg, leftover, rx);
			lenX = bms_num_members(leftover);
		}

		Assert(lenL + lenR + lenX == count);

		order = palloc_array(int, count);
		memcpy(order, orderL, lenL * sizeof(int));
		memcpy(order + lenL, orderR, lenR * sizeof(int));
		if (lenX > 0)
			memcpy(order + lenL + lenR, orderX, lenX * sizeof(int));

		bms_free(bestL);
		bms_free(bestR);
		bms_free(leftover);
		return order;
	}

	/* No splitting hyperedge: linearize the simple sub-problem. */
	return lindp_ikkbz_chain(hg, vmask, root_hint);
}

/*
 * Compute the connected component (over simple edges, restricted to vmask)
 * that contains "start".
 */
static Bitmapset *
lindp_component(const LinDPHypergraph *hg, int start, const Bitmapset *vmask)
{
	Bitmapset  *comp = bms_make_singleton(start);
	Bitmapset  *frontier = bms_make_singleton(start);

	while (!bms_is_empty(frontier))
	{
		int			v = bms_next_member(frontier, -1);
		int			c;

		frontier = bms_del_member(frontier, v);

		c = -1;
		while ((c = bms_next_member(hg->adj[v], c)) >= 0)
		{
			if (!bms_is_member(c, vmask) || bms_is_member(c, comp))
				continue;
			comp = bms_add_member(comp, c);
			frontier = bms_add_member(frontier, c);
		}
	}

	bms_free(frontier);
	return comp;
}

/*
 * Classic IKKBZ linearization of a (simple-edge) sub-problem.
 *
 * The sub-problem may consist of several connected components when cross
 * products are involved.  We linearize each component with the rank-based
 * IKKBZ tree algorithm and concatenate the components, placing the component
 * that contains the chosen root first.  Concatenating components is exactly
 * the "cross edge" treatment: the interval DP will join them with cross
 * products.
 */
static int *
lindp_ikkbz_chain(const LinDPHypergraph *hg, const Bitmapset *vmask, int root_hint)
{
	int			rootv = lindp_pick_root(hg, vmask, root_hint);
	int			total = bms_num_members(vmask);
	int		   *order = palloc_array(int, total);
	int			pos = 0;
	Bitmapset  *remaining = bms_copy(vmask);

	while (!bms_is_empty(remaining))
	{
		int			cstart;
		Bitmapset  *comp;
		Bitmapset  *visited = NULL;
		List	   *chain;
		ListCell   *lc;

		/* Linearize the component containing the root first, then the rest. */
		if (bms_is_member(rootv, remaining))
			cstart = rootv;
		else
			cstart = lindp_pick_root(hg, remaining, -1);

		comp = lindp_component(hg, cstart, remaining);
		chain = lindp_ikkbz_solve(hg, cstart, -1, comp, &visited);
		bms_free(visited);

		foreach(lc, chain)
		{
			LinDPModule *m = (LinDPModule *) lfirst(lc);
			ListCell   *lc2;

			foreach(lc2, m->seq)
				order[pos++] = lfirst_int(lc2);
		}

		remaining = bms_del_members(remaining, comp);
		bms_free(comp);
	}

	bms_free(remaining);

	Assert(pos == total);

	return order;
}

/*
 * Recursively linearize the precedence subtree rooted at vertex v (whose tree
 * parent is "parent", or -1 for the root), restricted to vmask, which must be
 * a single connected component.
 *
 * The precedence tree edges are the simple edges of the hypergraph, oriented
 * away from the root.  The join graph can contain cycles (for example an
 * equivalence class connects every relation to every other, producing a
 * clique), but IKKBZ requires a precedence *tree*.  We therefore build a DFS
 * spanning tree on the fly: "visited" accumulates every vertex already placed
 * in the tree, and any edge leading back to a visited vertex is skipped.
 * Without this, a cyclic graph would make the recursion loop forever.
 *
 * Returns a chain (list of LinDPModule *) headed by v.
 */
static List *
lindp_ikkbz_solve(const LinDPHypergraph *hg, int v, int parent, const Bitmapset *vmask,
				  Bitmapset **visited)
{
	List	   *childchains = NIL;
	List	   *merged;
	List	   *chain;
	int			c;

	*visited = bms_add_member(*visited, v);

	/* Recurse into every not-yet-visited simple-edge child of v in vmask. */
	c = -1;
	while ((c = bms_next_member(hg->adj[v], c)) >= 0)
	{
		if (c == parent || !bms_is_member(c, vmask) ||
			bms_is_member(c, *visited))
			continue;
		childchains = lappend(childchains,
							  lindp_ikkbz_solve(hg, c, v, vmask, visited));
	}

	merged = lindp_merge_chains(childchains);

	chain = list_make1(lindp_make_module(hg, v, parent));
	chain = list_concat(chain, merged);

	return lindp_normalize_chain(chain);
}

/*
 * Merge several independent child chains into one, ordering modules by
 * ascending rank.  Each input chain is normalized first; since the merge then
 * repeatedly takes the lowest-ranked available head, the result is globally
 * rank-sorted while preserving each chain's internal order.
 */
static List *
lindp_merge_chains(List *chains)
{
	List	   *result = NIL;
	int			nchains = list_length(chains);
	List	  **norm;
	ListCell  **cells;
	ListCell   *lc;
	int			i;

	if (nchains == 0)
		return NIL;

	norm = palloc_array(List *, nchains);
	cells = palloc_array(ListCell *, nchains);

	i = 0;
	foreach(lc, chains)
	{
		norm[i] = lindp_normalize_chain((List *) lfirst(lc));
		cells[i] = list_head(norm[i]);
		i++;
	}

	for (;;)
	{
		int			best = -1;
		double		best_rank = 0.0;

		for (i = 0; i < nchains; i++)
		{
			LinDPModule *m;

			if (cells[i] == NULL)
				continue;
			m = (LinDPModule *) lfirst(cells[i]);
			if (best < 0 || m->rank < best_rank)
			{
				best = i;
				best_rank = m->rank;
			}
		}

		if (best < 0)
			break;

		result = lappend(result, lfirst(cells[best]));
		cells[best] = lnext(norm[best], cells[best]);
	}

	pfree(cells);
	pfree(norm);
	return result;
}

/*
 * Normalize a chain: repeatedly merge any adjacent pair of modules whose
 * ranks are out of order (rank(a) > rank(b)) into a single compound module.
 * The result has non-decreasing ranks.
 */
static List *
lindp_normalize_chain(List *chain)
{
	bool		changed = true;

	while (changed)
	{
		ListCell   *lc;
		ListCell   *prev = NULL;

		changed = false;

		foreach(lc, chain)
		{
			if (prev != NULL)
			{
				LinDPModule *a = (LinDPModule *) lfirst(prev);
				LinDPModule *b = (LinDPModule *) lfirst(lc);

				if (a->rank > b->rank)
				{
					LinDPModule *combined = lindp_combine_modules(a, b);

					/* Replace a with the combined module, drop b. */
					lfirst(prev) = combined;
					chain = list_delete_cell(chain, lc);
					changed = true;
					break;
				}
			}
			prev = lc;
		}
	}

	return chain;
}

/*
 * Construct the singleton module for a single vertex.
 */
static LinDPModule *
lindp_make_module(const LinDPHypergraph *hg, int v, int parent)
{
	LinDPModule *m = palloc0_object(LinDPModule);
	double		n = hg->verts[v].rows;
	double		s = (parent < 0) ? 1.0 : hg->sel[parent][v];
	double		t = s * n;

	m->seq = list_make1_int(v);
	m->T = t;
	m->C = Max(t, LINDP_EPSILON);
	m->rank = (m->T - 1.0) / m->C;

	return m;
}

/*
 * Combine two adjacent modules a (first) and b (second) into one, using the
 * ASI cost recurrence T(ab) = T(a) T(b), C(ab) = C(a) + T(a) C(b).
 */
static LinDPModule *
lindp_combine_modules(const LinDPModule *a, const LinDPModule *b)
{
	LinDPModule *m = palloc0_object(LinDPModule);

	m->seq = list_concat_copy(a->seq, b->seq);
	m->T = a->T * b->T;
	m->C = a->C + a->T * b->C;
	m->rank = (m->T - 1.0) / Max(m->C, LINDP_EPSILON);

	return m;
}

/*
 * Pick a root vertex within vmask.  Prefer root_hint if it is a member,
 * otherwise choose the vertex with the smallest cardinality.
 */
static int
lindp_pick_root(const LinDPHypergraph *hg, const Bitmapset *vmask, int root_hint)
{
	int			best = -1;
	int			i;

	if (root_hint >= 0 && bms_is_member(root_hint, vmask))
		return root_hint;

	i = -1;
	while ((i = bms_next_member(vmask, i)) >= 0)
	{
		if (best < 0 || hg->verts[i].rows < hg->verts[best].rows)
			best = i;
	}

	return best;
}

/*
 * Linearized dynamic programming over the given order.
 *
 * best[i][j] holds the join relation for the contiguous sub-sequence
 * order[i..j].  A join is only formed between two adjacent sub-sequences, so
 * the DP runs in O(n^3) make_join_rel() calls while still allowing bushy
 * plans.  Splits without a join predicate yield cross-product joins, which is
 * how cross products enter the search.
 *
 * Every constructed join relation receives the full post-processing
 * (partitionwise paths, gather paths, grouped paths, set_cheapest) via
 * lindp_finalize_joinrel(), so the relations are costed exactly as
 * standard_join_search() would cost them.  This routine is meant to be called
 * once per seed linearization; because make_join_rel() reuses the canonical
 * join relation for a relid set, repeated calls simply accumulate additional
 * Pareto-optimal paths on the shared relations.
 *
 * Returns the join relation covering the whole order, or NULL if no legal
 * contiguous plan exists for this linearization.
 *
 * XXX At this point we only call lindp_run_dp with final=true, so maybe we
 * should ditch the argument entirely.
 */
static RelOptInfo *
lindp_run_dp(PlannerInfo *root, LinDPHypergraph *hg, int *order, int n, bool final)
{
	RelOptInfo **best = palloc0_array(RelOptInfo *, n * n);

	/* Length-1 intervals are the input relations themselves. */
	for (int i = 0; i < n; i++)
		best[i * n + i] = hg->verts[order[i]].rel;

	for (int len = 2; len <= n; len++)
	{
		CHECK_FOR_INTERRUPTS();

		for (int i = 0; i + len - 1 < n; i++)
		{
			int			j = i + len - 1;
			RelOptInfo *joinrel = NULL;
			int			k;

			for (k = i; k < j; k++)
			{
				RelOptInfo *lrel = best[i * n + k];
				RelOptInfo *rrel = best[(k + 1) * n + j];
				RelOptInfo *jr;

				if (lrel == NULL || rrel == NULL)
					continue;

				jr = make_join_rel(root, lrel, rrel);
				if (jr != NULL)
					joinrel = jr;
			}

			if (joinrel != NULL)
			{
				if (final)
					lindp_finalize_joinrel(root, joinrel);
				else
					set_cheapest(joinrel);
			}

			best[i * n + j] = joinrel;
		}
	}

	return best[n - 1];
}

/*
 * Run the per-joinrel post-processing that standard_join_search() performs at
 * the end of each level.  Mirrors the corresponding block in geqo_eval().
 */
static void
lindp_finalize_joinrel(PlannerInfo *root, RelOptInfo *rel)
{
	bool		is_top_rel = bms_equal(rel->relids, root->all_query_rels);

	/* Create paths for partitionwise joins. */
	generate_partitionwise_join_paths(root, rel);

	/*
	 * Consider gathering partial paths.  standard_join_search() skips this
	 * for the topmost rel, because the core planner gathers it later (once
	 * the final scan/join target is known) in
	 * apply_scanjoin_target_to_paths(). We do it here even for the topmost
	 * rel; that is harmless, because the core planner simply regenerates the
	 * Gather paths with the final target afterwards.  Gathering intermediate
	 * rels (including the topmost rel of a subproblem that feeds an enclosing
	 * join) is necessary so that a parallel path can become a relation's
	 * cheapest path and be used by higher joins, exactly as
	 * standard_join_search() does.
	 */
	generate_useful_gather_paths(root, rel, false);

	/* Find and save the cheapest paths for this rel. */
	set_cheapest(rel);

	/* Consider partial aggregation paths for the grouped relation. */
	if (rel->grouped_rel != NULL && !is_top_rel)
	{
		RelOptInfo *grouped_rel = rel->grouped_rel;

		Assert(IS_GROUPED_REL(grouped_rel));

		generate_grouped_paths(root, grouped_rel, rel);
		set_cheapest(grouped_rel);
	}
}
