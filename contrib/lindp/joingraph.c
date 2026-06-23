/*-------------------------------------------------------------------------
 *
 * joingraph.c
 *	  Build the join graph used to seed the linearized join search.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/lindp/joingraph.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lindp.h"
#include "nodes/bitmapset.h"
#include "optimizer/joininfo.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"

/*
 * Estimate the join selectivity between two atomic nodes.
 *
 * We collect the restriction clauses from the outer rel's joininfo list that
 * mention the inner rel and are fully computable from the union of the two
 * relid sets, then run them through clauselist_selectivity().  This is only a
 * surrogate used to rank relations during linearization; the real cost model
 * is applied later when LinDP actually builds the join relations.
 *
 * XXX This likely ignores "complex" join clauses, referencing more than two
 * relations. That's unfortunate.
 */
static double
edge_selectivity(PlannerInfo *root, RelOptInfo *a, RelOptInfo *b)
{
	Relids		joinrelids;
	List	   *clauses = NIL;
	ListCell   *lc;
	double		sel;

	joinrelids = bms_union(a->relids, b->relids);

	foreach(lc, a->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/* Clause must be evaluable at this join and reference both sides. */
		if (!bms_is_subset(rinfo->required_relids, joinrelids))
			continue;
		if (!bms_overlap(rinfo->clause_relids, a->relids))
			continue;
		if (!bms_overlap(rinfo->clause_relids, b->relids))
			continue;

		clauses = lappend(clauses, rinfo);
	}

	if (clauses == NIL)
	{
		bms_free(joinrelids);
		return 1.0;
	}

	sel = clauselist_selectivity(root, clauses, 0, JOIN_INNER, NULL);

	list_free(clauses);
	bms_free(joinrelids);

	/* Clamp to a sane range. */
	if (sel <= 0.0)
		sel = 1.0e-10;
	if (sel > 1.0)
		sel = 1.0;

	return sel;
}

/*
 * Build the join graph over the supplied initial relations.
 */
LinDpGraph *
lindp_build_graph(PlannerInfo *root, List *initial_rels)
{
	LinDpGraph *graph = palloc_object(LinDpGraph);
	int			n = list_length(initial_rels);
	int			i,
				j;
	ListCell   *lc;

	graph->n = n;
	graph->rels = palloc_array(RelOptInfo *, n);
	graph->card = palloc_array(double, n);
	graph->adj = palloc_array(bool *, n);
	graph->sel = palloc_array(double *, n);

	i = 0;
	foreach(lc, initial_rels)
	{
		RelOptInfo *rel = (RelOptInfo *) lfirst(lc);

		graph->rels[i] = rel;
		graph->card[i] = Max(rel->rows, 1.0);
		i++;
	}

	for (i = 0; i < n; i++)
	{
		graph->adj[i] = palloc0_array(bool, n);
		graph->sel[i] = palloc_array(double, n);
		for (j = 0; j < n; j++)
			graph->sel[i][j] = 1.0;
	}

	/*
	 * An edge exists between two nodes when they share a relevant join clause
	 * or there is a join-order restriction forcing them together.  We compute
	 * the selectivity only for the join-clause case; restriction-only edges
	 * keep selectivity 1.0 but still count as connectivity.
	 */
	for (i = 0; i < n; i++)
	{
		for (j = i + 1; j < n; j++)
		{
			RelOptInfo *a = graph->rels[i];
			RelOptInfo *b = graph->rels[j];
			bool		connected = false;

			if (have_relevant_joinclause(root, a, b))
			{
				double		sel = edge_selectivity(root, a, b);

				graph->sel[i][j] = sel;
				graph->sel[j][i] = sel;
				connected = true;
			}
			else if (have_join_order_restriction(root, a, b))
			{
				connected = true;
			}

			graph->adj[i][j] = connected;
			graph->adj[j][i] = connected;
		}
	}

	return graph;
}

/*
 * Is the join graph connected?  A disconnected graph implies the query needs
 * one or more cross products; LinDP can still handle that, but the caller may
 * use this to decide whether to fall back to the default search.
 *
 * XXX We're not using this, currently. We should handle that automatically,
 * by splitting the graph into components, calculating linear order for each
 * component independently, and then doing standard_join_search with the
 * joinrels built from the components.
 */
bool
lindp_graph_is_connected(LinDpGraph *graph)
{
	int			n = graph->n;
	bool	   *seen;
	int		   *stack;
	int			top = 0;
	int			visited = 0;
	int			i;

	if (n <= 1)
		return true;

	seen = palloc0_array(bool, n);
	stack = palloc_array(int, n);

	stack[top++] = 0;
	seen[0] = true;

	while (top > 0)
	{
		int			cur = stack[--top];

		visited++;
		for (i = 0; i < n; i++)
		{
			if (!seen[i] && graph->adj[cur][i])
			{
				seen[i] = true;
				stack[top++] = i;
			}
		}
	}

	pfree(seen);
	pfree(stack);

	return visited == n;
}
