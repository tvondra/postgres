/*-------------------------------------------------------------------------
 *
 * joinhardness.c
 *	  Estimate the "hardness" (cost) of the dynamic-programming join search.
 *
 * The complexity of the exhaustive join order search (as implemented in the
 * standard_join_search) may be measured by the number of make_join_rel()
 * candidate pairs it considers.
 *
 * This number depends on the shape of the join graph, not merely on the
 * number of relations: an n-way chain is cubic, while an n-way clique or
 * star is exponential.
 *
 * That is why join_collapse_limit is such a poor way to judge complexity
 * of joins - joins with the same number of relations may have fundamentally
 * different complexity. The same issue applies to geqo_threshold. Those
 * values are too eager on large but sparse queries and too timid on dense
 * medium-sized ones. It's hard to pick a value that works for both.
 *
 * Per Moerkotte & Neumann (2006), the number of make_join_rel() candidates
 * equals the count of "connected subgraphs / connected complement" pairs
 * (#ccp) of the join graph.
 *
 * estimate_join_search_effort() computes an upper bound on #ccp, capped at
 * a caller-supplied budget (to allow aborting once we know the join search
 * is more complex).
 *
 * It first builds the "effective" join graph: with a vertex per initial
 * rel, and an edge between two rels whenever there is a join clause, an
 * EquivalenceClass-derived join clause, or a join-order restriction linking
 * them (have_relevant_joinclause() and have_join_order_restriction()
 * encapsulate exactly those tests).
 *
 * It then counts csg-cmp pairs with a budgeted DPccp enumeration that
 * aborts as soon as the accumulated work exceeds the budget. This makes the
 * estimate cheap even for catastrophic graphs (e.g. cliques), yet it is
 * exact (hence the estimate is as tight as possible) whenever the true
 * #ccp is below the budget.
 *
 * The estimate is monotone in the set of relations and edges, so it
 * composes naturally with recursive sub-joinlist planning in
 * make_rel_from_joinlist().
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/joinhardness.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "optimizer/joininfo.h"
#include "optimizer/paths.h"
#include "utils/timestamp.h"

/*
 * The effective join graph over the initial rels, indexed 0 .. n-1.
 * neighbors[i] holds the indexes adjacent to vertex i.
 */
typedef struct JoinGraph
{
	int			n;				/* number of vertices */
	Bitmapset **neighbors;		/* adjacency, one Bitmapset per vertex */
} JoinGraph;

/*
 * Mutable state for the recursive enumeration.
 *
 * "work" counts every connected subgraph and every emitted csg-cmp pair.
 * This is the quantity measuring join search complexity, and compared
 * against the budget so that the whole walk is bounded regardless of
 * graph shape.
 *
 * "ccp" counts only the csg-cmp pairs, which is the metric we report when
 * the enumeration completes without aborting.
 *
 * XXX The small subtle difference between "work" and "ccp" counters is
 * that "work" is incremented even for subgraphs for which we don't find
 * any suitable complement.
 */
typedef struct JoinEnumState
{
	JoinGraph  *graph;
	int64		budget;			/* abort once work exceeds this */
	int64		work;			/* csg + ccp emissions so far */
	int64		ccp;			/* csg-cmp pairs so far */
	bool		aborted;		/* set true once budget is exceeded */
} JoinEnumState;

static JoinGraph *build_join_graph(PlannerInfo *root, List *initial_rels);
static void free_join_graph(JoinGraph *graph);
static int64 join_ordering_upper_bound(int n, int64 budget);
static Bitmapset *neighborhood(JoinEnumState *state, Bitmapset *s, Bitmapset *x);
static void on_connected_subgraph(JoinEnumState *state, Bitmapset *s1);

static void enumerate_csg(JoinEnumState *state, Bitmapset *s, Bitmapset *x);
static void enumerate_csg_subsets(JoinEnumState *state, Bitmapset *s,
								  Bitmapset *x, Bitmapset *nbr,
								  int *members, int k, int idx,
								  Bitmapset **subset);
static void enumerate_cmp(JoinEnumState *state, Bitmapset *s1);

static void grow_cmp(JoinEnumState *state, Bitmapset *s1, Bitmapset *s2,
					 Bitmapset *x);
static void grow_cmp_subsets(JoinEnumState *state, Bitmapset *s1, Bitmapset *s2,
							 Bitmapset *x, Bitmapset *nbr,
							 int *members, int k, int idx,
							 Bitmapset **subset);


/*
 * estimate_join_search_effort
 *		Return an upper bound on the number of make_join_rel() candidate pairs
 *		(the #ccp metric) that standard_join_search() would consider for the
 *		given set of initial rels, capped at "budget".
 *
 * The returned value is in the range [0, budget].  It equals the exact #ccp
 * when that is <= budget; otherwise it equals budget, signalling that the join
 * search is at least as hard as the budget allows.  The computation never does
 * more than O(budget) units of enumeration work, so it is cheap to call even
 * for pathological join graphs.
 *
 * If the budget is set to 0, it's not enforced.
 */
int64
estimate_join_search_effort(PlannerInfo *root, List *initial_rels,
							int64 budget)
{
	JoinEnumState state;
	JoinGraph   *graph;
	int			n = list_length(initial_rels);
	int			i;
	int64		max_complexity;

	TimestampTz	ts_start,
				ts_end;
	uint64		microsec;

	ts_start = GetCurrentTimestamp();

	/* nothing to join */
	if (n <= 1)
		return 0;

	/*
	 * Calculate the (very loose) upper boundary from the number of relations.
	 * This grows very fast - at 6 tables it's ~30k, at 8 it's already ~17M.
	 * But for very small joins it allows us to do without the join graph.
	 */
	max_complexity = join_ordering_upper_bound(n, budget);
	if (max_complexity < budget)
		return max_complexity;

	/* build the adjacency matrix representing the graph */
	graph = build_join_graph(root, initial_rels);

	/* initialize the enumeration state */
	state.graph = graph;
	state.budget = budget;
	state.work = 0.0;
	state.ccp = 0.0;
	state.aborted = false;

	/*
	 * DPccp enumeration: seed a connected subgraph at every vertex, working
	 * from the highest index downwards. Forbidding all lower-or-equal
	 * indexes when growing from the seed guarantees that each connected
	 * subgraph is enumerated exactly once.
	 */
	for (i = graph->n - 1; i >= 0 && !state.aborted; i--)
	{
		Bitmapset  *s = bms_make_singleton(i);
		Bitmapset  *x = NULL;
		int			j;

		on_connected_subgraph(&state, s);

		if (!state.aborted)
		{
			/*
			 * Disallow all lower-or-equal vertices (i.e. those won't be
			 * part of the subgraphs enumerated in enumerate_csg).
			 */
			for (j = 0; j <= i; j++)
				x = bms_add_member(x, j);

			enumerate_csg(&state, s, x);
		}

		bms_free(s);
		bms_free(x);
	}

	free_join_graph(graph);

	ts_end = GetCurrentTimestamp();
	microsec = TimestampDifferenceMicroseconds(ts_start, ts_end);

	/*
	 * If aborted, return the budget (to show we aborted), otherwise return
	 * the number of ccp pairs.
	 */
	elog(WARNING, "estimate_join_search_effort: rels %d aborted %d ccp " INT64_FORMAT " budget " INT64_FORMAT " timing " INT64_FORMAT " us",
		 n, state.aborted, state.ccp, budget, microsec);

	return (state.aborted ? budget : state.ccp);
}

/*
 * join_ordering_upper_bound
 *		Compute n! * C(n-1), saturated at "budget".
 *
 * This is the maximum number of binary join orderings for n base rels, where
 * C(k) is the k-th Catalan number.  The value is an upper bound independent of
 * join graph shape, so if it is below the caller's budget then no DPccp search
 * is needed to prove the budget cannot be reached.
 */
static int64
join_ordering_upper_bound(int n, int64 budget)
{
	int64		result = 1.0;
	int			i;

	/* disable fast-path (useful for testing the full estimation logic) */
	if (!enable_join_search_fast)
		return budget;

	/* Compute n!, stopping once it reaches the only threshold we care about. */
	for (i = 2; i <= n; i++)
	{
		if ((budget != 0) && (result * i >= budget))
			return budget;

		result *= i;
	}

	/* Compute C(n-1) by recurrence: C_k = C_{k-1} * 2(2k-1)/(k+1). */
	for (i = 1; i < n; i++)
	{
		double		factor = 2.0 * (2.0 * (double) i - 1.0) /
			(double) (i + 1);

		if ((budget != 0) && (result >= budget / factor))
			return budget;

		result *= factor;
	}

	return result;
}

/*
 * build_join_graph
 *		Populate "graph" with one vertex per initial rel and an edge between
 *		every pair of rels that share a join clause, an EquivalenceClass join,
 *		or a join-order restriction.
 *
 * have_relevant_joinclause() already covers both ordinary join clauses and
 * EquivalenceClass-derived ones, so the resulting graph reflects every link
 * that standard_join_search() would actually use to combine two rels.
 */
static JoinGraph *
build_join_graph(PlannerInfo *root, List *initial_rels)
{
	JoinGraph *graph = palloc(sizeof(JoinGraph));
	int			n = list_length(initial_rels);
	RelOptInfo **rels = (RelOptInfo **) palloc(n * sizeof(RelOptInfo *));
	ListCell   *lc;
	int			i;
	int			j;

	i = 0;
	foreach(lc, initial_rels)
		rels[i++] = (RelOptInfo *) lfirst(lc);

	graph->n = n;
	graph->neighbors = (Bitmapset **) palloc0(n * sizeof(Bitmapset *));

	for (i = 0; i < n; i++)
	{
		for (j = i + 1; j < n; j++)
		{
			if (have_relevant_joinclause(root, rels[i], rels[j]) ||
				have_join_order_restriction(root, rels[i], rels[j]))
			{
				graph->neighbors[i] = bms_add_member(graph->neighbors[i], j);
				graph->neighbors[j] = bms_add_member(graph->neighbors[j], i);
			}
		}
	}

	pfree(rels);

	return graph;
}

/*
 * free_join_graph
 *		Release the adjacency storage built by build_join_graph().
 */
static void
free_join_graph(JoinGraph *graph)
{
	int			i;

	for (i = 0; i < graph->n; i++)
		bms_free(graph->neighbors[i]);

	pfree(graph->neighbors);
	pfree(graph);
}

/*
 * neighborhood
 *		Return the open neighborhood of vertex set "s".
 *
 * The open neighborhood is the union of the adjacency lists of its members,
 * minus "s" itself and minus the forbidden set "x".
 *
 * The caller owns and must free the result.
 */
static Bitmapset *
neighborhood(JoinEnumState *state, Bitmapset *s, Bitmapset *x)
{
	Bitmapset  *nbr = NULL;
	int			v = -1;

	while ((v = bms_next_member(s, v)) >= 0)
		nbr = bms_add_members(nbr, state->graph->neighbors[v]);

	nbr = bms_del_members(nbr, s);
	if (x != NULL)
		nbr = bms_del_members(nbr, x);

	return nbr;
}

/*
 * on_connected_subgraph
 *		Account for one connected subgraph S1 and enumerate the csg-cmp pairs
 *		that use it as the "left" side.
 */
static void
on_connected_subgraph(JoinEnumState *state, Bitmapset *s1)
{
	state->work += 1;
	if ((state->budget != 0) && (state->work > state->budget))
	{
		state->aborted = true;
		return;
	}

	enumerate_cmp(state, s1);
}

/*
 * enumerate_csg
 *		Recursively enumerate the connected subgraphs that extend "s" using
 *		only vertices outside the forbidden set "x".
 */
static void
enumerate_csg(JoinEnumState *state, Bitmapset *s, Bitmapset *x)
{
	Bitmapset  *nbr;
	Bitmapset  *subset = NULL;
	int		   *members;
	int			k;
	int			v;
	int			i;

	if (state->aborted)
		return;

	nbr = neighborhood(state, s, x);
	k = bms_num_members(nbr);
	if (k == 0)
	{
		bms_free(nbr);
		return;
	}

	members = (int *) palloc(k * sizeof(int));
	v = -1;
	i = 0;
	while ((v = bms_next_member(nbr, v)) >= 0)
		members[i++] = v;

	enumerate_csg_subsets(state, s, x, nbr, members, k, 0, &subset);

	pfree(members);
	bms_free(nbr);
}

/*
 * enumerate_csg_subsets
 *		Enumerate every non-empty subset of the neighborhood "nbr" (given as
 *		the member array members[0..k)).  For each subset S', the union S u S'
 *		is a connected subgraph: account for it and recurse to grow it further,
 *		forbidding the whole neighborhood to avoid duplicate enumerations.
 *
 * "subset" accumulates the members chosen so far; it is mutated in place and
 * restored on the way back up so a single buffer is reused across the walk.
 */
static void
enumerate_csg_subsets(JoinEnumState *state, Bitmapset *s, Bitmapset *x,
					  Bitmapset *nbr, int *members, int k, int idx,
					  Bitmapset **subset)
{
	if (state->aborted)
		return;

	if (idx == k)
	{
		Bitmapset  *snew;
		Bitmapset  *xnew;

		if (*subset == NULL)
			return;				/* skip the empty subset */

		snew = bms_union(s, *subset);
		on_connected_subgraph(state, snew);
		if (!state->aborted)
		{
			xnew = bms_union(x, nbr);
			enumerate_csg(state, snew, xnew);
			bms_free(xnew);
		}
		bms_free(snew);

		return;
	}

	/* Branch that excludes members[idx]. */
	enumerate_csg_subsets(state, s, x, nbr, members, k, idx + 1, subset);
	if (state->aborted)
		return;

	/* Branch that includes members[idx]. */
	*subset = bms_add_member(*subset, members[idx]);
	enumerate_csg_subsets(state, s, x, nbr, members, k, idx + 1, subset);
	*subset = bms_del_member(*subset, members[idx]);
}

/*
 * enumerate_cmp
 *		Enumerate every connected complement S2 for the connected subgraph S1,
 *		counting one csg-cmp pair per (S1, S2).
 *
 * To count each unordered pair exactly once we forbid all vertices whose index
 * is <= min(S1); that forces the global minimum of S1 u S2 to lie in S1.  The
 * complements are seeded from each neighbor of S1 in decreasing index order,
 * forbidding the lower-indexed neighbors as we go so that a complement reached
 * through several entry points is only generated once.
 */
static void
enumerate_cmp(JoinEnumState *state, Bitmapset *s1)
{
	int			mn = bms_next_member(s1, -1);
	Bitmapset  *x;
	Bitmapset  *nbr;
	int		   *members;
	int			k;
	int			v;
	int			i;

	if (state->aborted)
		return;

	/* x = S1 u {0 .. mn} */
	x = bms_copy(s1);
	for (i = 0; i <= mn; i++)
		x = bms_add_member(x, i);

	nbr = neighborhood(state, s1, x);
	k = bms_num_members(nbr);
	if (k == 0)
	{
		bms_free(nbr);
		bms_free(x);
		return;
	}

	members = (int *) palloc(k * sizeof(int));
	v = -1;
	i = 0;
	while ((v = bms_next_member(nbr, v)) >= 0)
		members[i++] = v;

	/* Seed complements from the highest-index neighbor downwards. */
	for (i = k - 1; i >= 0 && !state->aborted; i--)
	{
		Bitmapset  *s2;
		Bitmapset  *xrec;
		int			j;

		/* Count the pair (S1, {members[i]}). */
		state->work += 1;
		state->ccp += 1;

		if ((state->budget != 0) && (state->work > state->budget))
		{
			state->aborted = true;
			break;
		}

		/* Forbid x plus every lower-indexed neighbor (members[0 .. i-1]). */
		xrec = bms_copy(x);
		for (j = 0; j < i; j++)
			xrec = bms_add_member(xrec, members[j]);

		s2 = bms_make_singleton(members[i]);
		grow_cmp(state, s1, s2, xrec);

		bms_free(s2);
		bms_free(xrec);
	}

	pfree(members);
	bms_free(nbr);
	bms_free(x);
}

/*
 * grow_cmp
 *		Grow the connected complement "s2" of "s1" by every non-empty subset of
 *		its neighborhood, counting one csg-cmp pair for each enlarged
 *		complement.
 */
static void
grow_cmp(JoinEnumState *st, Bitmapset *s1, Bitmapset *s2, Bitmapset *x)
{
	Bitmapset  *nbr;
	Bitmapset  *subset = NULL;
	int		   *members;
	int			k;
	int			v;
	int			i;

	if (st->aborted)
		return;

	nbr = neighborhood(st, s2, x);
	k = bms_num_members(nbr);
	if (k == 0)
	{
		bms_free(nbr);
		return;
	}

	members = (int *) palloc(k * sizeof(int));
	v = -1;
	i = 0;
	while ((v = bms_next_member(nbr, v)) >= 0)
		members[i++] = v;

	grow_cmp_subsets(st, s1, s2, x, nbr, members, k, 0, &subset);

	pfree(members);
	bms_free(nbr);
}

/*
 * grow_cmp_subsets
 *		Subset-enumeration helper for grow_cmp(), mirroring
 *		enumerate_csg_subsets() but counting csg-cmp pairs as it enlarges the
 *		complement.
 */
static void
grow_cmp_subsets(JoinEnumState *state, Bitmapset *s1, Bitmapset *s2, Bitmapset *x,
				 Bitmapset *nbr, int *members, int k, int idx,
				 Bitmapset **subset)
{
	if (state->aborted)
		return;

	if (idx == k)
	{
		Bitmapset  *s2new;
		Bitmapset  *xnew;

		if (*subset == NULL)
			return;				/* skip the empty subset */

		state->work += 1;
		state->ccp += 1;

		if ((state->budget != 0) && (state->work > state->budget))
		{
			state->aborted = true;
			return;
		}

		s2new = bms_union(s2, *subset);
		xnew = bms_union(x, nbr);
		grow_cmp(state, s1, s2new, xnew);
		bms_free(s2new);
		bms_free(xnew);

		return;
	}

	/* Branch that excludes members[idx]. */
	grow_cmp_subsets(state, s1, s2, x, nbr, members, k, idx + 1, subset);

	if (state->aborted)
		return;

	/* Branch that includes members[idx]. */
	*subset = bms_add_member(*subset, members[idx]);
	grow_cmp_subsets(state, s1, s2, x, nbr, members, k, idx + 1, subset);
	*subset = bms_del_member(*subset, members[idx]);
}
