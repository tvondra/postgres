/*-------------------------------------------------------------------------
 *
 * ikkbz.c
 *	  IKKBZ linearization of the join graph.
 *
 * Given the join graph, IKKBZ produces an optimal left-deep join order for
 * an ASI-conforming surrogate cost (here a C_out style sum of intermediate
 * cardinalities).
 *
 * For a fixed root and a precedence (spanning) tree, the optimal linear
 * extension is obtained by repeatedly emitting, among all nodes whose
 * parent has already been emitted, the one with the smallest ASI rank
 * (the classic Monma-Sidney / IKKBZ result). We try several roots
 * and keep the order with the lowest surrogate cost (i.e. C_out, not the
 * traditional PostgreSQL cost).
 *
 * The resulting order is only a seed: LinDP re-costs it with the real
 * PostgreSQL cost model, so the surrogate need not match reality exactly.
 *
 * relevant papers:
 *
 * - Sequencing with series-parallel precedence constraints, C. L. Monma
 * and J. B. Sidney, Mathematics of Operations Research 4, 215 (1979)
 *
 * - On the optimal nesting order for computing N-relational joins,
 * T. Ibaraki and T. Kameda, ACM Trans. Database Syst. 9, 482 (1984)
 *
 * - Optimization of nonrecursive queries, R. Krishnamurthy, H. Boral, and
 * C. Zaniolo, Proceedings of the 12th International Conference on Very Large
 * Data Bases, VLDB ’86, pp. 128–137
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/lindp/ikkbz.c
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>

#include "postgres.h"

#include "lindp.h"

/*
 * Build a spanning forest of the join graph rooted at "root" via BFS
 * (breadth-first-search).
 *
 * parent[] is filled by setting the parent for each node reachable from the
 * root (-1 for other forest roots / unreachable components, which then become
 * additional roots so that disconnected graphs still linearize).
 *
 * XXX What if a node has multiple parent nodes, in a cyclic graph? That should
 * not matter, I think, at this point we only look for components, the join
 * orders will be explored later.
 */
static void
build_precedence_forest(LinDpGraph *graph, int root, int *parent)
{
	int			n = graph->n;
	int		   *queue = palloc_array(int, n);
	bool	   *visited = palloc0_array(bool, n);
	int			head,
				tail;
	int			i,
				start;

	/* initially nothing reachable */
	for (i = 0; i < n; i++)
		parent[i] = -1;

	/*
	 * Start BFS from the requested root, then sweep any nodes that were not
	 * reached (separate connected components) as their own forest roots, so
	 * that disconnected graphs still produce a complete linear order.  We
	 * visit "root" first, then nodes 0 .. n-1 in order for determinism.
	 */
	for (start = -1; start < n; start++)
	{
		int			s = (start < 0) ? root : start;

		/* skip nodes we already visited, */
		if (visited[s])
			continue;

		/*
		 * Haven't visited this node yet, might be a new (forest) root, leave
		 * the parent set to -1 (for now, we may set it later, when processing
		 * nodes in the queue).
		 */
		visited[s] = true;
		head = tail = 0;
		queue[tail++] = s;

		/*
		 * Process the queue in BFS manner - we'll be adding entries to the
		 * queue, but we never can have more than n elements in total (because
		 * then we visited all nodes in the graph).
		 */
		while (head < tail)
		{
			int			cur = queue[head++];

			/* find all nodes adjacent to 'cur' that we haven't visited yet */
			for (i = 0; i < n; i++)
			{
				if (visited[i] || !graph->adj[cur][i])
					continue;

				/* found new node, add it to the queue */
				visited[i] = true;
				parent[i] = cur;
				queue[tail++] = i;
			}
		}
	}

	pfree(visited);
	pfree(queue);
}

/*
 * ASI rank for a node given its parent in the precedence tree.
 *
 *   T = selectivity(parent, node) * card(node)
 *
 * is the multiplicative size factor contributed by joining "node". For the
 * surrogate C_out cost the per-node cost equals T, so
 *
 *   rank = (T - 1) / T
 *
 * Forest roots (no parent) use T = card(node).
 */
static double
asi_rank(LinDpGraph *graph, int node, int parent)
{
	double		t;

	if (parent < 0)
		t = graph->card[node];
	else
		t = graph->sel[parent][node] * graph->card[node];

	/* XXX is it even possible to get negative (or non-positive) "t"? */
	if (t <= 0.0)
		t = 1.0e-10;

	return (t - 1.0) / t;
}

/*
 * Produce the optimal linear extension of the precedence forest by greedily
 * emitting the free node with the smallest rank.
 *
 * XXX Linear extension is a total/linear ordering of the nodes, consistent
 * with the partial order imposed by the precendence forest.
 *
 * XXX I'm not sure this works correctly for graphs with multiple components.
 * We seem to be still processing those at once, constructing a single
 * linearization for the same graph. We should probably do that for every
 * component separately, no? And then combine those by doing cross-products
 * by standard_join_search.
 */
static void
linear_extension(LinDpGraph *graph, int *parent, int *order)
{
	int			n = graph->n;
	bool	   *placed = palloc0_array(bool, n);
	int			count = 0;
	int			i;

	/*
	 * In each round, find one node to add to the linearized order. We look
	 * for a free node with the lowest rank. Free means that it hasn't been
	 * placed yet, but it's parent was (and so we can place the node too).
	 */
	while (count < n)
	{
		int			best = -1;
		double		best_rank = 0.0;

		for (i = 0; i < n; i++)
		{
			/* already placed */
			if (placed[i])
				continue;

			/* free if root of forest or its parent is already placed */
			if (parent[i] >= 0 && !placed[parent[i]])
				continue;

			/* we have a free node, is it the cheapest one? */
			if (best < 0)
			{
				best = i;
				best_rank = asi_rank(graph, i, parent[i]);
			}
			else
			{
				double		r = asi_rank(graph, i, parent[i]);

				/*
				 * smallest rank wins; tie-break on node index for stability
				 *
				 * XXX The tie-break is implicit - we iterate from 0, so we
				 * use the first node with the same rank.
				 */
				if (r < best_rank)
				{
					best = i;
					best_rank = r;
				}
			}
		}

		Assert(best >= 0);
		order[count++] = best;
		placed[best] = true;
	}

	pfree(placed);
}

/*
 * Evaluate the C_out surrogate cost of a linear order, using the full
 * adjacency (not just the spanning tree) so that closing edges of cyclic
 * queries reduce the running size.
 */
static double
order_cost(LinDpGraph *graph, int *order)
{
	int			n = graph->n;
	double		running = 1.0;
	double		total = 0.0;
	int			step;

	for (step = 0; step < n; step++)
	{
		int			node = order[step];
		double		factor = graph->card[node];
		int			prev;

		for (prev = 0; prev < step; prev++)
		{
			int			pnode = order[prev];

			if (graph->adj[node][pnode])
				factor *= graph->sel[node][pnode];
		}

		running *= factor;
		if (step > 0)
			total += running;
	}

	return total;
}

/*
 * Compute the IKKBZ linear order, trying one or more roots depending on the
 * effort setting and returning the cheapest order under the surrogate cost.
 */
int *
lindp_ikkbz_order(LinDpGraph *graph, int effort)
{
	int			n = graph->n;
	int		   *best_order = palloc_array(int, n);
	int		   *order = palloc_array(int, n);
	int		   *parent = palloc_array(int, n);
	double		best_cost = 0.0;
	bool		have_best = false;
	int			root;
	int			roots_to_try;

	if (n == 1)
	{
		best_order[0] = 0;
		pfree(order);
		pfree(parent);
		return best_order;
	}

	/*
	 * The effort determines the number of relations to try as IKKBZ root.
	 *
	 * With low effort we only try the relations with the smallest cardinality,
	 * which is usually a good driving relation (but may not be).
	 *
	 * With effort -1 we try every relation as a root.
	 *
	 * FIXME We don't really use the lindp_effort as described, except for the
	 * case lindp_effort = 1. We should sort the nodes by cardinality first,
	 * and then process the first lindp_effort nodes with the lowest cardinality.
	 */
	if (effort <= 0)
		roots_to_try = n;
	else
		roots_to_try = effort;

	for (root = 0; root < n; root++)
	{
		double		cost;
		int			use_root = root;

		/*
		 * When trying just one root, start from the root with the lowest
		 * cardinality, which usually are good driving relations.
		 */
		if (roots_to_try == 1)
		{
			int			i;
			int			min_node = 0;

			for (i = 1; i < n; i++)
			{
				if (graph->card[i] < graph->card[min_node])
					min_node = i;
			}
			use_root = min_node;
		}

		build_precedence_forest(graph, use_root, parent);
		linear_extension(graph, parent, order);
		cost = order_cost(graph, order);

		if (!have_best || cost < best_cost)
		{
			memcpy(best_order, order, n * sizeof(int));
			best_cost = cost;
			have_best = true;
		}

		if (roots_to_try == 1)
			break;
	}

	pfree(order);
	pfree(parent);

	return best_order;
}
