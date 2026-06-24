#include "postgres.h"

#include "lindp.h"

static void build_spanning_tree(LinGraph *g);

static IkkbzModule *
ikkbz_new_module(int rel, double t)
{
	IkkbzModule *m = palloc_object(IkkbzModule);

	m->cap = 4;
	m->rels = palloc_array(int, m->cap);
	m->rels[0] = rel;
	m->nrels = 1;
	m->T = t;
	m->C = t;
	/* rank = (T - 1) / C; guard against C == 0 */
	m->rank = (m->C > 0.0) ? (m->T - 1.0) / m->C : 0.0;
	return m;
}

/* Merge module b onto the end of module a (a := a ++ b). */
static void
ikkbz_merge(IkkbzModule *a, IkkbzModule *b)
{
	int			i;

	if (a->nrels + b->nrels > a->cap)
	{
		while (a->cap < a->nrels + b->nrels)
			a->cap *= 2;
		a->rels = repalloc(a->rels, a->cap * sizeof(int));
	}
	for (i = 0; i < b->nrels; i++)
		a->rels[a->nrels++] = b->rels[i];

	/* C(AB) = C(A) + T(A) * C(B); T(AB) = T(A) * T(B) */
	a->C = a->C + a->T * b->C;
	a->T = a->T * b->T;
	a->rank = (a->C > 0.0) ? (a->T - 1.0) / a->C : 0.0;
}

/*
 * Normalize a sequence of modules in place: repeatedly merge any adjacent
 * pair that violates the rank ordering (rank[i] > rank[i+1]).  When done, the
 * module ranks are non-decreasing, which is the ASI-optimal ordering subject
 * to the precedence implied by the construction order.  Returns the new
 * length.
 */
static int
ikkbz_normalize(IkkbzModule **seq, int len)
{
	bool		changed = true;

	while (changed)
	{
		int			i;

		changed = false;
		for (i = 0; i + 1 < len; i++)
		{
			if (seq[i]->rank > seq[i + 1]->rank)
			{
				int			k;

				ikkbz_merge(seq[i], seq[i + 1]);
				/* remove element i+1 */
				for (k = i + 1; k + 1 < len; k++)
					seq[k] = seq[k + 1];
				len--;
				changed = true;
				break;
			}
		}
	}
	return len;
}

/*
 * Recursively build the normalized module sequence for the subtree rooted at
 * "node" in the tree rooted at the IKKBZ root.  *out receives a palloc'd array
 * of module pointers; returns its length.
 */
static int
ikkbz_process(LinGraph *g, int node, int parent,
			  double *t, IkkbzModule ***out)
{
	int			n = g->n;
	int			c;
	int			nchildren = 0;
	int		  **childseq;
	int		   *childlen;
	int		   *childpos;
	IkkbzModule **merged;
	int			mergedlen = 0;
	IkkbzModule **seq;
	int			seqlen;
	int			i;

	/* Build normalized sequences for each child subtree. */
	childseq = palloc_array(int *, n);	/* misused as IkkbzModule** below */
	childlen = palloc0_array(int, n);
	childpos = palloc0_array(int, n);

	{
		IkkbzModule ***cseq = (IkkbzModule ***) childseq;

		for (c = 0; c < n; c++)
		{
			if (c != parent && g->tree[node * n + c])
			{
				IkkbzModule **cs;

				childlen[nchildren] = ikkbz_process(g, c, node, t, &cs);
				cseq[nchildren] = cs;
				nchildren++;
			}
		}

		/* k-way merge children by ascending module rank. */
		for (c = 0; c < nchildren; c++)
			mergedlen += childlen[c];
		merged = palloc_array(IkkbzModule *, Max(mergedlen, 1));
		mergedlen = 0;
		for (;;)
		{
			int			best = -1;
			double		bestrank = 0.0;

			for (c = 0; c < nchildren; c++)
			{
				if (childpos[c] < childlen[c])
				{
					double		r = cseq[c][childpos[c]]->rank;

					if (best < 0 || r < bestrank)
					{
						best = c;
						bestrank = r;
					}
				}
			}
			if (best < 0)
				break;
			merged[mergedlen++] = cseq[best][childpos[best]];
			childpos[best]++;
		}
	}

	/* Prepend this node as its own module, then normalize. */
	seq = palloc_array(IkkbzModule *, mergedlen + 1);
	seq[0] = ikkbz_new_module(node, t[node]);
	for (i = 0; i < mergedlen; i++)
		seq[i + 1] = merged[i];
	seqlen = ikkbz_normalize(seq, mergedlen + 1);

	pfree(childseq);
	pfree(childlen);
	pfree(childpos);
	pfree(merged);

	*out = seq;
	return seqlen;
}

/*
 * Run IKKBZ for a given root, producing a linear order (array of node
 * indices, caller-allocated, length g->n) and returning the ASI cost of that
 * order (lower is better).
 */
static double
ikkbz_for_root(LinGraph *g, int rootnode, int *order)
{
	int			n = g->n;
	double	   *t = palloc_array(double, n);
	int		   *parent = palloc_array(int, n);
	int		   *queue = palloc_array(int, n);
	int			qhead = 0,
				qtail = 0;
	IkkbzModule **seq;
	int			seqlen;
	int			pos = 0;
	int			i;
	double		totalC,
				totalT;

	/* BFS to assign parents and per-node t = sel(parent) * rows. */
	for (i = 0; i < n; i++)
		parent[i] = -2;			/* unvisited */
	parent[rootnode] = -1;
	t[rootnode] = g->rows[rootnode];
	queue[qtail++] = rootnode;
	while (qhead < qtail)
	{
		int			u = queue[qhead++];
		int			v;

		for (v = 0; v < n; v++)
		{
			if (g->tree[u * n + v] && parent[v] == -2)
			{
				parent[v] = u;
				t[v] = g->sel[u * n + v] * g->rows[v];
				queue[qtail++] = v;
			}
		}
	}

	seqlen = ikkbz_process(g, rootnode, -1, t, &seq);

	/* Flatten the modules into the output order. */
	for (i = 0; i < seqlen; i++)
	{
		int			k;

		for (k = 0; k < seq[i]->nrels; k++)
			order[pos++] = seq[i]->rels[k];
	}
	Assert(pos == n);

	/* Total ASI cost of the whole sequence (fold the modules together). */
	totalC = 0.0;
	totalT = 1.0;
	for (i = 0; i < seqlen; i++)
	{
		totalC = totalC + totalT * seq[i]->C;
		totalT = totalT * seq[i]->T;
	}

	pfree(t);
	pfree(parent);
	pfree(queue);
	return totalC;
}

/*
 * Compute up to nseeds distinct linearizations, ordered by ascending ASI
 * cost.  Each linearization is an int array of length g->n.  Returns the
 * number actually produced (>= 1).
 */
int
ikkbz_compute_linearizations(LinGraph *g, int nseeds, int **orders)
{
	int			n = g->n;
	int		   *cand = palloc_array(int, n);
	double	   *costs = palloc_array(double, n);
	int		  **allorders = palloc_array(int *, n);
	int			i,
				k,
				nout;

	build_spanning_tree(g);

	/* One IKKBZ run per candidate root. */
	for (i = 0; i < n; i++)
	{
		allorders[i] = palloc_array(int, n);
		costs[i] = ikkbz_for_root(g, i, allorders[i]);
		cand[i] = i;
	}

	/* Selection-sort roots by ascending ASI cost. */
	for (i = 0; i < n; i++)
	{
		int			best = i;

		for (k = i + 1; k < n; k++)
		{
			if (costs[cand[k]] < costs[cand[best]])
				best = k;
		}
		if (best != i)
		{
			int			tmp = cand[i];

			cand[i] = cand[best];
			cand[best] = tmp;
		}
	}

	/* Emit up to nseeds linearizations, skipping duplicates. */
	nout = 0;
	for (i = 0; i < n && nout < nseeds; i++)
	{
		int		   *o = allorders[cand[i]];
		bool		dup = false;

		for (k = 0; k < nout; k++)
		{
			int			p;
			bool		same = true;

			for (p = 0; p < n; p++)
			{
				if (orders[k][p] != o[p])
				{
					same = false;
					break;
				}
			}
			if (same)
			{
				dup = true;
				break;
			}
		}
		if (!dup)
			orders[nout++] = o;
	}

	pfree(cand);
	pfree(costs);
	pfree(allorders);
	return nout;
}

/*
 * build_spanning_tree
 *		Compute a spanning tree of the (connected) join graph using a
 *		minimum-selectivity Kruskal: edges with the strongest filtering
 *		(smallest selectivity) are preferred, which tends to keep the most
 *		informative join predicates in the tree the linearization is built on.
 */
typedef struct TreeEdge
{
	int			u;
	int			v;
	double		sel;
} TreeEdge;

static int
treeedge_cmp(const void *a, const void *b)
{
	const TreeEdge *ea = (const TreeEdge *) a;
	const TreeEdge *eb = (const TreeEdge *) b;

	if (ea->sel < eb->sel)
		return -1;
	if (ea->sel > eb->sel)
		return 1;
	return 0;
}

static void
build_spanning_tree(LinGraph *g)
{
	int			n = g->n;
	int			i,
				j,
				ne = 0;
	TreeEdge   *edges;
	int		   *uf;

	edges = palloc_array(TreeEdge, (Size) n * n);
	for (i = 0; i < n; i++)
	{
		for (j = i + 1; j < n; j++)
		{
			if (g->edge[i * n + j])
			{
				edges[ne].u = i;
				edges[ne].v = j;
				edges[ne].sel = g->sel[i * n + j];
				ne++;
			}
		}
	}

	qsort(edges, ne, sizeof(TreeEdge), treeedge_cmp);

	uf = palloc_array(int, n);
	for (i = 0; i < n; i++)
		uf[i] = i;

	for (i = 0; i < ne; i++)
	{
		int			ru = edges[i].u,
					rv = edges[i].v;

		while (uf[ru] != ru)
			ru = uf[ru];
		while (uf[rv] != rv)
			rv = uf[rv];
		if (ru != rv)
		{
			uf[rv] = ru;
			g->tree[edges[i].u * n + edges[i].v] = true;
			g->tree[edges[i].v * n + edges[i].u] = true;
		}
	}

	pfree(uf);
	pfree(edges);
}
