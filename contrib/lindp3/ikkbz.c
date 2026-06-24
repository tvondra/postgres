#include "postgres.h"

#include "lindp.h"
#include "optimizer/joininfo.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"


static IkModule *
ikkbz_make_module(int item, double t)
{
	IkModule   *m = palloc_object(IkModule);

	if (t < LINDP_TINY)
		t = LINDP_TINY;

	m->items = list_make1_int(item);
	m->t = t;
	m->c = t;					/* C(single relation) = T */
	m->rank = (t - 1.0) / t;
	return m;
}

/* Concatenate module b after module a (a precedes b), returning a new one. */
static IkModule *
ikkbz_merge_modules(IkModule *a, IkModule *b)
{
	IkModule   *m = palloc_object(IkModule);

	m->items = list_concat_copy(a->items, b->items);
	m->t = a->t * b->t;
	m->c = a->c + a->t * b->c;
	if (m->c < LINDP_TINY)
		m->c = LINDP_TINY;
	m->rank = (m->t - 1.0) / m->c;
	return m;
}

/*
 * Normalize a chain of modules: contract adjacent modules whose ranks are out
 * of order, so that the resulting ranks are non-decreasing.  This enforces
 * the ASI precedence constraint.
 */
static List *
ikkbz_normalize_chain(List *chain)
{
	bool		changed = true;

	while (changed)
	{
		ListCell   *lc;

		changed = false;
		foreach(lc, chain)
		{
			ListCell   *next = lnext(chain, lc);
			IkModule   *a;
			IkModule   *b;

			if (next == NULL)
				break;
			a = (IkModule *) lfirst(lc);
			b = (IkModule *) lfirst(next);
			if (a->rank >= b->rank)
			{
				IkModule   *merged = ikkbz_merge_modules(a, b);

				/* Replace a and b with their contraction. */
				lfirst(lc) = merged;
				chain = list_delete_cell(chain, next);
				changed = true;
				break;
			}
		}
	}
	return chain;
}

/*
 * Merge several already-normalized chains into one by repeatedly taking the
 * front module with the smallest rank.  The result is itself rank-ordered.
 */
static List *
ikkbz_merge_chains(List *chains)
{
	List	   *result = NIL;
	int			nchains = list_length(chains);
	List	  **arr;
	ListCell  **pos;
	int			i;
	ListCell   *lc;

	if (nchains == 0)
		return NIL;

	arr = (List **) palloc(nchains * sizeof(List *));
	pos = (ListCell **) palloc(nchains * sizeof(ListCell *));
	i = 0;
	foreach(lc, chains)
	{
		arr[i] = (List *) lfirst(lc);
		pos[i] = list_head(arr[i]);
		i++;
	}

	for (;;)
	{
		int			best = -1;
		double		best_rank = 0;

		for (i = 0; i < nchains; i++)
		{
			IkModule   *m;

			if (pos[i] == NULL)
				continue;
			m = (IkModule *) lfirst(pos[i]);
			if (best < 0 || m->rank < best_rank)
			{
				best = i;
				best_rank = m->rank;
			}
		}
		if (best < 0)
			break;				/* all chains exhausted */

		result = lappend(result, lfirst(pos[best]));
		pos[best] = lnext(arr[best], pos[best]);
	}

	pfree(arr);
	pfree(pos);
	return result;
}

/*
 * Recursively build the descendant chain of node v in the precedence tree.
 * Returns the chain (List of IkModule *) of all descendants of v, in order.
 */
static List *
ikkbz_process(LinDPState *st, int v)
{
	List	   *childChains = NIL;
	ListCell   *lc;

	foreach(lc, st->children[v])
	{
		int			c = lfirst_int(lc);
		List	   *cdesc = ikkbz_process(st, c);
		List	   *chain;

		/* The child's full chain is the child itself followed by its desc. */
		chain = lcons(ikkbz_make_module(c, st->trel[c]), cdesc);
		chain = ikkbz_normalize_chain(chain);
		childChains = lappend(childChains, chain);
	}

	return ikkbz_merge_chains(childChains);
}

/*
 * Estimate the join selectivity between relations i and j, used only to rank
 * linearizations.  Returns a value in (0, 1].
 */
static double
ikkbz_pair_selectivity(LinDPState *st, int i, int j)
{
	PlannerInfo *root = st->root;
	RelOptInfo *ri = st->rels[i];
	RelOptInfo *rj = st->rels[j];
	Relids		joinrelids;
	List	   *clauses = NIL;
	List	   *eqclauses;
	ListCell   *lc;
	Selectivity sel;

	joinrelids = bms_union(ri->relids, rj->relids);

	/* Ordinary join clauses recorded on ri's joininfo. */
	foreach(lc, ri->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (bms_is_subset(rinfo->clause_relids, joinrelids) &&
			bms_overlap(rinfo->clause_relids, rj->relids) &&
			bms_overlap(rinfo->clause_relids, ri->relids))
			clauses = lappend(clauses, rinfo);
	}

	/* Equality clauses implied by equivalence classes. */
	eqclauses = generate_join_implied_equalities(root, joinrelids,
												 ri->relids, rj, NULL);
	clauses = list_concat(clauses, eqclauses);

	if (clauses == NIL)
	{
		bms_free(joinrelids);
		return 1.0;				/* cross product */
	}

	sel = clauselist_selectivity(root, clauses, 0, JOIN_INNER, NULL);

	bms_free(joinrelids);
	list_free(clauses);

	if (sel <= 0.0)
		sel = LINDP_TINY;
	if (sel > 1.0)
		sel = 1.0;
	return sel;
}

/*
 * Are relations i and j directly connected in the join graph?  This mirrors
 * the test join_search_one_level() uses to decide whether two relations may
 * be joined without a cross product.
 */
bool
ikkbz_rels_connected(LinDPState *st, int i, int j)
{
	return have_relevant_joinclause(st->root, st->rels[i], st->rels[j]) ||
		have_join_order_restriction(st->root, st->rels[i], st->rels[j]);
}

/*
 * Compute the IKKBZ linear order for one connected component rooted at
 * 'rootidx', plus its ASI cost (lower is better).  comp lists the component's
 * global relation indexes; adjacency is taken from ikkbz_rels_connected().
 *
 * Returns the order as a freshly allocated List of int (global indexes).
 */
List *
ikkbz_order_for_root(LinDPState *st, List *comp, int rootidx, double *cost_out)
{
	ListCell   *lc;
	int		   *queue;
	int			qhead = 0,
				qtail = 0;
	int			compsz = list_length(comp);
	List	   *chain;
	List	   *order;
	double		card_so_far;
	double		cost;
	int			idx;

	/* Reset precedence-tree scratch for the component. */
	foreach(lc, comp)
	{
		int			v = lfirst_int(lc);

		st->parent[v] = -1;
		st->children[v] = NIL;
		st->trel[v] = 0.0;
	}

	/* BFS over the connected component to build a spanning/precedence tree. */
	queue = (int *) palloc(compsz * sizeof(int));
	st->parent[rootidx] = rootidx;	/* mark root visited (self-parent) */
	st->trel[rootidx] = st->card[rootidx];
	queue[qtail++] = rootidx;

	while (qhead < qtail)
	{
		int			u = queue[qhead++];

		foreach(lc, comp)
		{
			int			w = lfirst_int(lc);

			if (st->parent[w] != -1)
				continue;		/* already in tree */
			if (!ikkbz_rels_connected(st, u, w))
				continue;
			st->parent[w] = u;
			st->children[u] = lappend_int(st->children[u], w);
			st->trel[w] = ikkbz_pair_selectivity(st, u, w) * st->card[w];
			if (st->trel[w] < LINDP_TINY)
				st->trel[w] = LINDP_TINY;
			queue[qtail++] = w;
		}
	}
	pfree(queue);

	st->parent[rootidx] = -1;	/* restore: root has no parent */

	/* Build the linear order: root, then its merged descendant chain. */
	chain = ikkbz_process(st, rootidx);

	order = list_make1_int(rootidx);
	foreach(lc, chain)
	{
		IkModule   *m = (IkModule *) lfirst(lc);
		ListCell   *lc2;

		foreach(lc2, m->items)
			order = lappend_int(order, lfirst_int(lc2));
	}

	/* Evaluate the ASI cost of this order. */
	card_so_far = st->card[rootidx];
	cost = 0.0;
	for (idx = 1; idx < list_length(order); idx++)
	{
		int			v = list_nth_int(order, idx);

		card_so_far *= st->trel[v];
		cost += card_so_far;
	}
	*cost_out = cost;

	return order;
}
