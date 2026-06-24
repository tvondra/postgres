/*-------------------------------------------------------------------------
 *
 * lindp.c
 *	  Prototype join-order search based on search-space linearization.
 *
 * This module installs a join_search_hook that replaces GEQO (and, when
 * enabled, the regular dynamic-programming join search) with an approach
 * based on "search space linearization":
 *
 *	1. LinDP++ (Radke & Neumann, https://db.in.tum.de/~radke/papers/lindp++.pdf):
 *	   first compute a single high-quality *linear order* of the relations
 *	   using the IKKBZ polynomial algorithm (optimal for acyclic queries under
 *	   an ASI cost model), then run dynamic programming *restricted to that
 *	   linear order*, considering only (near-)contiguous subsequences as DP
 *	   subproblems.  This collapses the join search to polynomial time while
 *	   still using PostgreSQL's real path cost model and building full bushy
 *	   plans within the search window.
 *
 *	2. Adaptive LinDP (Birler et al.,
 *	   https://db.in.tum.de/~birler/papers/adaptivelindp.pdf): widen the DP
 *	   window adaptively, spending a bounded extra budget so that the search
 *	   recovers most of the optimal-DP quality lost by a hard linearization.
 *
 * The implementation is intentionally self-contained and lives in a contrib
 * module.  It only relies on the public planner entry points used by the
 * standard join search and by GEQO (make_join_rel(), set_cheapest(),
 * generate_partitionwise_join_paths(), generate_useful_gather_paths(),
 * generate_grouped_paths()), so no core planner files need to be modified.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/lindp2/lindp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <float.h>
#include <limits.h>
#include <math.h>

#include "lindp.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/joininfo.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/selfuncs.h"

PG_MODULE_MAGIC_EXT(
					.name = "lindp2",
					.version = PG_VERSION
);

void		_PG_init(void);
void		_PG_fini(void);

/* GUC variables */
static bool lindp_enabled = true;
static bool lindp_adaptive = true;
static bool lindp_fallback = false;
static int	lindp_min_relations = 8;
static int	lindp_max_relations = 64;
static int	lindp_window = 1;
static int	lindp_effort = 20000;
static int	lindp_seeds = 1;

/* Saved previous hook, so we can chain to it when disabled. */
static join_search_hook_type prev_join_search_hook = NULL;


static RelOptInfo *lindp_join_search(PlannerInfo *root, int levels_needed,
									 List *initial_rels);

/* ---------------------------------------------------------------------------
 * Module load/unload
 * ------------------------------------------------------------------------- */

void
_PG_init(void)
{
	DefineCustomBoolVariable("lindp2.enabled",
							 "Enables the LinDP++ linearized join search.",
							 NULL,
							 &lindp_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("lindp2.fallback",
							 "Fallback to built-in join search if linearization fails.",
							 NULL,
							 &lindp_fallback,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("lindp2.adaptive",
							 "Enables adaptive widening of the linearized DP window.",
							 NULL,
							 &lindp_adaptive,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("lindp2.min_threshold",
							"Minimum number of relations before LinDP++ engages.",
							"Below this, the standard join search is used so "
							"small queries stay optimal.",
							&lindp_min_relations,
							8, 2, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lindp2.max_threshold",
							"Maximum number of relations LinDP++ will handle.",
							"Above this, the standard join search is used.",
							&lindp_max_relations,
							64, 2, 1000,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lindp2.window_size",
							"Base window size for the linearized dynamic program.",
							"Number of allowed \"holes\" in a contiguous "
							"subsequence considered as a DP subproblem.  0 means "
							"pure contiguous-interval DP.",
							&lindp_window,
							1, 0, 16,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	/*
	 * XXX the effort is not the same thing as in lindp module (that's more like
	 * the seed here), we should unify that to make comparisons simpler.
	 */

	DefineCustomIntVariable("lindp2.effort",
							"Budget (in DP subproblems) for adaptive widening.",
							"When adaptive widening is enabled, the window is "
							"increased while the estimated number of DP "
							"subproblems stays below this value.",
							&lindp_effort,
							20000, 1, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lindp2.seeds",
							"Number of alternative linearizations to evaluate.",
							"Values greater than 1 run the DP on several "
							"IKKBZ linearizations and keep the cheapest plan.",
							&lindp_seeds,
							1, 1, 32,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("lindp2");

	/* Install our hook, chaining to any previous one. */
	prev_join_search_hook = join_search_hook;
	join_search_hook = lindp_join_search;
}

void
_PG_fini(void)
{
	join_search_hook = prev_join_search_hook;
}

static uint32
lindp_bms_hash(const void *key, Size keysize)
{
	Relids		r = *((const Relids *) key);

	return bms_hash_value(r);
}

static int
lindp_bms_match(const void *a, const void *b, Size keysize)
{
	Relids		ra = *((const Relids *) a);
	Relids		rb = *((const Relids *) b);

	return bms_equal(ra, rb) ? 0 : 1;
}

static RelOptInfo *
memo_lookup(LindpRun *run, Relids relids)
{
	LindpMemoEntry *entry;
	Relids		key = relids;

	entry = (LindpMemoEntry *) hash_search(run->memo, &key, HASH_FIND, NULL);
	return entry ? entry->rel : NULL;
}

static void
memo_insert(LindpRun *run, Relids relids, RelOptInfo *rel)
{
	LindpMemoEntry *entry;
	Relids		key = relids;
	bool		found;

	entry = (LindpMemoEntry *) hash_search(run->memo, &key, HASH_ENTER, &found);
	entry->relids = relids;
	entry->rel = rel;
}

/*
 * Finalize a freshly built joinrel exactly the way standard_join_search()
 * does for each level: partitionwise joins, gather paths (except the top
 * rel), set_cheapest, and grouped-rel paths (except the top rel).
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
		RelOptInfo *grouped_rel = joinrel->grouped_rel;

		Assert(IS_GROUPED_REL(grouped_rel));
		generate_grouped_paths(root, grouped_rel, joinrel);
		set_cheapest(grouped_rel);
	}
}

/*
 * Process one admissible subset given by its present positions.
 *
 * "present" is a sorted array of "npresent" linear positions in [lo, hi]
 * (with present[0] == lo and present[npresent-1] == hi).  We try every
 * positional split point and combine the two halves via make_join_rel(),
 * accumulating all resulting paths on the (single) joinrel, then finalize it
 * and memoize it.
 */
static void
lindp_process_subset(LindpRun *run, int lo, int hi,
					 const int *present, int npresent)
{
	PlannerInfo *root = run->root;
	Relids		relids = NULL;
	RelOptInfo *built = NULL;
	int			i;
	int			c;

	/* Compute the relid set for this subset. */
	for (i = 0; i < npresent; i++)
		relids = bms_add_members(relids, run->lin[present[i]]->relids);

	/* Already memoized (or it is a seeded singleton)?  Then nothing to do. */
	if (memo_lookup(run, relids) != NULL)
	{
		bms_free(relids);
		return;
	}

	/*
	 * Try every cut position c in [lo, hi-1].  The present positions <= c form
	 * the left subproblem and those > c form the right one; both are
	 * themselves admissible subsets that were processed earlier (smaller
	 * cardinality) and hence are in the memo if they were buildable.
	 */
	for (c = lo; c < hi; c++)
	{
		Relids		lrelids = NULL;
		Relids		rrelids = NULL;
		RelOptInfo *lrel;
		RelOptInfo *rrel;
		RelOptInfo *jr;
		int			nleft = 0;

		for (i = 0; i < npresent; i++)
		{
			if (present[i] <= c)
			{
				lrelids = bms_add_members(lrelids, run->lin[present[i]]->relids);
				nleft++;
			}
			else
				rrelids = bms_add_members(rrelids, run->lin[present[i]]->relids);
		}

		/* Need a non-trivial split. */
		if (nleft == 0 || nleft == npresent)
		{
			bms_free(lrelids);
			bms_free(rrelids);
			continue;
		}

		lrel = memo_lookup(run, lrelids);
		rrel = memo_lookup(run, rrelids);
		bms_free(lrelids);
		bms_free(rrelids);
		if (lrel == NULL || rrel == NULL)
			continue;

		if (run->budget-- <= 0)
		{
			run->failed = true;
			break;
		}

		jr = make_join_rel(root, lrel, rrel);
		if (jr != NULL)
			built = jr;			/* same object for every successful split */
	}

	if (built != NULL)
	{
		lindp_finalize_joinrel(root, built);
		memo_insert(run, relids, built);
	}
	else
		bms_free(relids);
}

/*
 * Enumerate, in non-decreasing cardinality order, all admissible subsets for
 * the given window and process each one.  An admissible subset is a contiguous
 * range [lo, hi] of linear positions, possibly with up to "window" interior
 * positions removed ("holes").  window == 0 yields pure contiguous-interval
 * DP (classic LinDP); larger windows admit near-contiguous, bushy subproblems
 * (LinDP++ / adaptive LinDP).
 *
 * Returns the joinrel covering all relations, or NULL on failure.
 */
static RelOptInfo *
lindp_run_dp(LindpRun *run, int window)
{
	int			n = run->n;
	int			card;
	int		   *present = palloc_array(int, n);

	/*
	 * Process subsets by increasing number of present positions so that, when
	 * we split a subset, both halves are already memoized.
	 */
	for (card = 2; card <= n && !run->failed; card++)
	{
		int			lo;

		for (lo = 0; lo + card - 1 < n && !run->failed; lo++)
		{
			int			minspan = card;		/* hi-lo+1 >= card */
			int			maxspan = Min(n - lo, card + window);
			int			span;

			for (span = minspan; span <= maxspan; span++)
			{
				int			hi = lo + span - 1;
				int			nholes = span - card;	/* interior positions to drop */

				/*
				 * Choose which "nholes" of the (span-2) interior positions are
				 * holes.  lo and hi are always present.  We enumerate all such
				 * combinations.
				 */
				int			interior = span - 2;

				if (nholes < 0 || nholes > interior)
					continue;
				if (nholes > window)
					continue;

				/* Enumerate combinations of interior positions to remove. */
				if (nholes == 0)
				{
					int			p;
					int			k = 0;

					for (p = lo; p <= hi; p++)
						present[k++] = p;
					lindp_process_subset(run, lo, hi, present, card);
				}
				else
				{
					/* combination of "nholes" positions out of [lo+1, hi-1] */
					int		   *hole = palloc_array(int, nholes);
					int			h;

					for (h = 0; h < nholes; h++)
						hole[h] = h; /* indices into interior list */

					for (;;)
					{
						int			p,
									k = 0,
									hi2 = 0;

						/* Build present[] = [lo..hi] minus the chosen holes. */
						for (p = lo; p <= hi; p++)
						{
							bool		ishole = false;

							if (p > lo && p < hi)
							{
								int			rel = p - (lo + 1);	/* interior idx */

								if (hi2 < nholes && hole[hi2] == rel)
								{
									ishole = true;
									hi2++;
								}
							}
							if (!ishole)
								present[k++] = p;
						}
						lindp_process_subset(run, lo, hi, present, card);

						/* advance the combination (lexicographic). */
						{
							int			t = nholes - 1;

							while (t >= 0 && hole[t] == interior - nholes + t)
								t--;
							if (t < 0)
								break;
							hole[t]++;
							for (h = t + 1; h < nholes; h++)
								hole[h] = hole[h - 1] + 1;
						}
					}
					pfree(hole);
				}
			}
		}
	}

	pfree(present);

	if (run->failed)
		return NULL;
	return memo_lookup(run, run->all_relids);
}

/*
 * Estimate the number of admissible subsets for a given window, used to pick
 * an adaptive window within the effort budget.  This is a cheap upper-bound
 * proxy (sum over spans of C(interior, holes)).
 */
static double
estimate_subset_count(int n, int window)
{
	double		total = 0.0;
	int			lo;

	for (lo = 0; lo < n; lo++)
	{
		int			span;

		for (span = 1; span <= n - lo; span++)
		{
			int			interior = span - 2;
			int			h;
			double		comb = 1.0;
			double		acc = 1.0;	/* C(interior, 0) */

			if (interior <= 0)
			{
				total += 1.0;
				continue;
			}
			/* sum_{h=0..min(window,interior)} C(interior, h) */
			for (h = 1; h <= window && h <= interior; h++)
			{
				comb = comb * (interior - h + 1) / h;
				acc += comb;
			}
			total += acc;
			if (total > 1.0e12)
				return total;	/* saturate */
		}
	}
	return total;
}

/*
 * Choose the effective window: start from the configured base window and, if
 * adaptive widening is enabled, increase it while the estimated number of DP
 * subproblems stays within the effort budget.
 */
static int
lindp_choose_window(int n)
{
	int			w = lindp_window;

	if (w > n - 2)
		w = Max(n - 2, 0);

	if (lindp_adaptive)
	{
		while (w < n - 2 &&
			   estimate_subset_count(n, w + 1) <= (double) lindp_effort)
			w++;
	}
	return w;
}

/*
 * Execute the DP for one linearization.  When "isolated" is true we run in a
 * throwaway memory context and restore root->join_rel_list / join_rel_hash
 * afterwards (so the run leaves no trace), returning only the resulting plan
 * cost via *cost.  When "isolated" is false we run for real in the current
 * context and return the top joinrel.
 */
static RelOptInfo *
lindp_eval_linearization(PlannerInfo *root, RelOptInfo **lin, int n,
						 Relids all_relids, int window,
						 bool isolated, Cost *cost)
{
	LindpRun	run;
	HASHCTL		hash_ctl;
	MemoryContext mycontext = NULL;
	MemoryContext oldcxt = NULL;
	int			savelength = 0;
	struct HTAB *savehash = NULL;
	RelOptInfo *top;
	int			i;

	if (isolated)
	{
		mycontext = AllocSetContextCreate(CurrentMemoryContext,
										  "LinDP",
										  ALLOCSET_DEFAULT_SIZES);
		oldcxt = MemoryContextSwitchTo(mycontext);
		savelength = list_length(root->join_rel_list);
		savehash = root->join_rel_hash;
		Assert(root->join_rel_level == NULL);
		root->join_rel_hash = NULL;
	}

	run.root = root;
	run.n = n;
	run.lin = lin;
	run.all_relids = all_relids;
	run.budget = (long) lindp_effort * 8;
	run.failed = false;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Relids);
	hash_ctl.entrysize = sizeof(LindpMemoEntry);
	hash_ctl.hash = lindp_bms_hash;
	hash_ctl.match = lindp_bms_match;
	hash_ctl.hcxt = CurrentMemoryContext;
	run.memo = hash_create("LinDP memo", 256L, &hash_ctl,
						   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	/* Seed the memo with the singleton nodes. */
	for (i = 0; i < n; i++)
		memo_insert(&run, lin[i]->relids, lin[i]);

	top = lindp_run_dp(&run, window);

	if (isolated)
	{
		if (top != NULL && top->cheapest_total_path != NULL)
			*cost = top->cheapest_total_path->total_cost;
		else
			*cost = DBL_MAX;

		/* Restore planner state and discard everything we built. */
		root->join_rel_list = list_truncate(root->join_rel_list, savelength);
		root->join_rel_hash = savehash;
		MemoryContextSwitchTo(oldcxt);
		MemoryContextDelete(mycontext);
		return NULL;
	}

	return top;
}

/*
 * lindp_solve_component
 *		Run the full LinDP search (IKKBZ linearization + windowed DP) on a
 *		single *connected* set of relations and return the resulting top
 *		joinrel for real, or NULL if the search could not build it.
 *
 * "comp_rels" must form a connected join graph (the caller decomposes the
 * problem into connected components first).
 */
static RelOptInfo *
lindp_solve_component(PlannerInfo *root, List *comp_rels)
{
	LinGraph   *g;
	int			n = list_length(comp_rels);
	int			window;
	int			nseeds;
	int		  **orders;
	int			norders;
	RelOptInfo **lin;
	Relids		all_relids = NULL;
	RelOptInfo *top;
	int			i;
	int			best = 0;

	/* A singleton component is already its own joinrel. */
	if (n == 1)
		return (RelOptInfo *) linitial(comp_rels);

	/* Build the join graph for this component. */
	g = lindp_build_graph(root, comp_rels);
	if (g == NULL)
		return NULL;

	/* Compute one or more IKKBZ linearizations. */
	nseeds = Min(lindp_seeds, n);
	orders = palloc_array(int *, nseeds);
	norders = ikkbz_compute_linearizations(g, nseeds, orders);

	/* Pick the effective DP window (adaptive widening within the budget). */
	window = lindp_choose_window(n);

	/* Precompute all_relids = union of every node's relids. */
	for (i = 0; i < n; i++)
		all_relids = bms_add_members(all_relids, g->rels[i]->relids);

	lin = palloc_array(RelOptInfo *, n);

	/*
	 * If there is a single linearization we can run it for real directly.
	 * Otherwise evaluate each in isolation, keep the cheapest, and re-run the
	 * winner for real so its joinrels persist for the rest of planning.
	 */
	if (norders > 1)
	{
		Cost		bestcost = DBL_MAX;

		for (i = 0; i < norders; i++)
		{
			Cost		cost;
			int			p;

			for (p = 0; p < n; p++)
				lin[p] = g->rels[orders[i][p]];
			lindp_eval_linearization(root, lin, n, all_relids, window,
									 true, &cost);
			if (cost < bestcost)
			{
				bestcost = cost;
				best = i;
			}
		}
		if (bestcost == DBL_MAX)
			return NULL;
	}

	/* Final, real run of the chosen linearization. */
	for (i = 0; i < n; i++)
		lin[i] = g->rels[orders[best][i]];
	top = lindp_eval_linearization(root, lin, n, all_relids, window,
								   false, NULL);

	return top;
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
 *
 * XXX This sometimes fails to find a valid n-way join in standard_join_search.
 * Could we be scribbling over some of the data while in the LinDP code?
 */
static RelOptInfo *
fallback_default_search(PlannerInfo *root, int levels_needed, List *initial_rels)
{
	root->initial_rels = initial_rels;

	/*
	 * lindp_eval_linearization may be leaving behind some inconsistent state
	 * behind (likely because of the last execution with isolated=false), which
	 * then causes failures in standard_join_search. So make sure we start with
	 * clean state.
	 *
	 * FIXME it'd be better if lindp_eval_linearization didn't leave garbage
	 * in the root context
	 */
	root->join_rel_list = NIL;
	root->join_rel_hash = NULL;

	if (prev_join_search_hook)
		return prev_join_search_hook(root, levels_needed, initial_rels);
	else if (enable_geqo && levels_needed >= geqo_threshold)
		return geqo(root, levels_needed, initial_rels);
	else
		return standard_join_search(root, levels_needed, initial_rels);
}

static RelOptInfo *
lindp_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)
{
	int		   *labels;
	int			ncomp;
	List	   *component_rels = NIL;
	int			c;
	int			i;

	/* When disabled, defer to the previous hook or the core search. */
	if (!lindp_enabled ||
		levels_needed < lindp_min_relations ||
		levels_needed > lindp_max_relations)
	{
		return fallback_default_search(root, levels_needed, initial_rels);
	}

	/* Decompose the join graph into connected components. */
	ncomp = lindp_compute_components(root, initial_rels, &labels);

	/*
	 * Connected graph: run LinDP directly on the whole problem.  We still keep
	 * a guaranteed-complete fallback for the (rare) case where LinDP cannot
	 * build the top joinrel within its budget.
	 */
	if (ncomp <= 1)
	{
		RelOptInfo *top;

		pfree(labels);
		top = lindp_solve_component(root, initial_rels);

		if ((top == NULL) && lindp_fallback)
			return fallback_default_search(root, levels_needed, initial_rels);
		else if (top == NULL)
			elog(ERROR, "LinDP linearization failed to find a valid plan (%d rels)",
				 levels_needed);

		return top;
	}

	/*
	 * Disconnected graph: rather than falling back to standard_join_search()
	 * for the whole problem, solve each connected component independently with
	 * LinDP, then combine the resulting per-component join relations with a
	 * standard join search.  Because there are no join clauses between
	 * components, that final search only has to enumerate the Cartesian
	 * products joining the components together.
	 */
	for (c = 0; c < ncomp; c++)
	{
		List	   *comp_rels = NIL;
		ListCell   *lc;
		RelOptInfo *crel;

		i = 0;
		foreach(lc, initial_rels)
		{
			if (labels[i] == c)
				comp_rels = lappend(comp_rels, lfirst(lc));
			i++;
		}

		crel = lindp_solve_component(root, comp_rels);

		/*
		 * If LinDP could not build this component (e.g. its budget was
		 * exhausted), fall back to the standard search for that component
		 * only, so the overall search still succeeds.
		 */
		if ((crel == NULL) && lindp_fallback)
			crel = fallback_default_search(root, list_length(comp_rels),
										   comp_rels);
		else if (crel == NULL)
			elog(ERROR, "LinDP linearization failed to find a valid plan for component (%d rels)",
				 list_length(comp_rels));

		component_rels = lappend(component_rels, crel);
		list_free(comp_rels);
	}

	pfree(labels);

	/* Combine the components (pure Cartesian products) with the core DP. */
	return standard_join_search(root, list_length(component_rels),
								component_rels);
}
