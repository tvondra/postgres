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
 * edge_selectivity
 *		Approximate join selectivity between two nodes, used only to feed the
 *		IKKBZ rank function.  Returns a value in (0, 1].  Returns -1.0 if the
 *		two nodes have no relevant join clause (i.e. there is no graph edge).
 */
static double
edge_selectivity(PlannerInfo *root, RelOptInfo *a, RelOptInfo *b)
{
	Relids		joinrelids;
	List	   *clauses = NIL;
	ListCell   *lc;
	double		sel;

	if (!have_relevant_joinclause(root, a, b))
		return -1.0;

	joinrelids = bms_union(a->relids, b->relids);

	/*
	 * Gather the join clauses from a's joininfo that reference both a and b
	 * and nothing outside the pair.  This mirrors how the planner finds the
	 * restriction clauses applicable to a join of exactly these two rels.
	 */
	foreach(lc, a->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (bms_is_subset(rinfo->clause_relids, joinrelids) &&
			bms_overlap(rinfo->clause_relids, a->relids) &&
			bms_overlap(rinfo->clause_relids, b->relids))
			clauses = lappend(clauses, rinfo);
	}

	if (clauses != NIL)
	{
		sel = clauselist_selectivity(root, clauses, 0, JOIN_INNER, NULL);
		list_free(clauses);
	}
	else
	{
		/*
		 * Edge exists only via an equivalence class (or other mechanism not
		 * captured above).  Approximate an equijoin selectivity as the
		 * inverse of the larger input cardinality, which is the planner's
		 * usual fallback for a single equality.
		 */
		double		maxrows = Max(a->rows, b->rows);

		sel = (maxrows > 1.0) ? 1.0 / maxrows : 1.0;
	}

	bms_free(joinrelids);

	/* Clamp into a sane (0, 1] range. */
	if (sel <= 0.0)
		sel = 1.0e-10;
	if (sel > 1.0)
		sel = 1.0;
	return sel;
}

/*
 * build_graph
 *		Build the LinGraph from a connected set of rels.  Callers decompose the
 *		problem into connected components first, so the input is expected to be
 *		connected; as a safety net this still returns NULL (and the caller falls
 *		back) if the graph turns out not to be connected.
 */
LinGraph *
lindp_build_graph(PlannerInfo *root, List *initial_rels)
{
	LinGraph   *g = palloc0_object(LinGraph);
	int			n = list_length(initial_rels);
	int			i,
				j;
	ListCell   *lc;
	int		   *comp;			/* union-find parent */

	g->n = n;
	g->rels = palloc_array(RelOptInfo *, n);
	g->rows = palloc_array(double, n);
	g->edge = palloc0_array(bool, (Size) n * n);
	g->sel = palloc0_array(double, (Size) n * n);
	g->tree = palloc0_array(bool, (Size) n * n);

	i = 0;
	foreach(lc, initial_rels)
	{
		RelOptInfo *rel = (RelOptInfo *) lfirst(lc);

		g->rels[i] = rel;
		g->rows[i] = (rel->rows > 1.0) ? rel->rows : 1.0;
		i++;
	}

	/* Discover edges (symmetric) and selectivities. */
	for (i = 0; i < n; i++)
	{
		for (j = i + 1; j < n; j++)
		{
			double		sel = edge_selectivity(root, g->rels[i], g->rels[j]);

			if (sel > 0.0)
			{
				g->edge[i * n + j] = g->edge[j * n + i] = true;
				g->sel[i * n + j] = g->sel[j * n + i] = sel;
			}
		}
	}

	/* Connectivity check via union-find. */
	comp = palloc_array(int, n);
	for (i = 0; i < n; i++)
		comp[i] = i;
	for (i = 0; i < n; i++)
	{
		for (j = i + 1; j < n; j++)
		{
			if (g->edge[i * n + j])
			{
				int			ri = i,
							rj = j;

				while (comp[ri] != ri)
					ri = comp[ri];
				while (comp[rj] != rj)
					rj = comp[rj];
				if (ri != rj)
					comp[rj] = ri;
			}
		}
	}
	{
		int			root_comp = 0;
		bool		connected = true;

		while (comp[root_comp] != root_comp)
			root_comp = comp[root_comp];
		for (i = 0; i < n; i++)
		{
			int			ri = i;

			while (comp[ri] != ri)
				ri = comp[ri];
			if (ri != root_comp)
			{
				connected = false;
				break;
			}
		}
		pfree(comp);
		if (!connected)
			return NULL;
	}

	return g;
}

/*
 * compute_components
 *		Partition initial_rels into connected components of the join graph.
 *
 * Returns the number of components and, in *labels (palloc'd, one entry per
 * node in initial_rels order), the 0-based component id of each node.
 */
int
lindp_compute_components(PlannerInfo *root, List *initial_rels, int **labels)
{
	int			n = list_length(initial_rels);
	RelOptInfo **rels = palloc_array(RelOptInfo *, n);
	int		   *uf = palloc_array(int, n);
	int		   *comp = palloc_array(int, n);
	ListCell   *lc;
	int			i,
				j;
	int			ncomp = 0;

	i = 0;
	foreach(lc, initial_rels)
		rels[i++] = (RelOptInfo *) lfirst(lc);

	/* Union-find over join edges. */
	for (i = 0; i < n; i++)
		uf[i] = i;
	for (i = 0; i < n; i++)
	{
		for (j = i + 1; j < n; j++)
		{
			if (edge_selectivity(root, rels[i], rels[j]) > 0.0)
			{
				int			ri = i,
							rj = j;

				while (uf[ri] != ri)
					ri = uf[ri];
				while (uf[rj] != rj)
					rj = uf[rj];
				if (ri != rj)
					uf[rj] = ri;
			}
		}
	}

	/* Assign dense, 0-based component ids in first-seen order. */
	for (i = 0; i < n; i++)
		comp[i] = -1;
	for (i = 0; i < n; i++)
	{
		int			ri = i;

		while (uf[ri] != ri)
			ri = uf[ri];
		if (comp[ri] < 0)
			comp[ri] = ncomp++;
		comp[i] = comp[ri];
	}

	pfree(uf);
	pfree(rels);
	*labels = comp;
	return ncomp;
}
