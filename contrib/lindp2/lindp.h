#ifndef LINDP_H
#define LINDP_H

#include "postgres.h"

#include "nodes/pathnodes.h"
#include "utils/hsearch.h"

/*
 * Internal representation of the join graph.  "Nodes" are the entries of
 * initial_rels (each a RelOptInfo, possibly already spanning several base
 * rels when it came from a sub-joinlist).  Edges encode whether two nodes
 * have a relevant join clause and an approximate join selectivity, used only
 * to drive the IKKBZ linearization (never the final cost model).
 */
typedef struct LinGraph
{
	int			n;				/* number of nodes */
	RelOptInfo **rels;			/* rels[i] = i'th initial rel */
	double	   *rows;			/* clamped cardinality of each node */
	bool	   *edge;			/* n*n adjacency matrix (edge[i*n+j]) */
	double	   *sel;			/* n*n selectivity matrix (valid if edge) */
	/* Spanning-tree adjacency (subset of edge), filled by build_spanning_tree */
	bool	   *tree;			/* n*n adjacency matrix of the spanning tree */
} LinGraph;

/* A module (compound node) used during IKKBZ normalization. */
typedef struct IkkbzModule
{
	int		   *rels;			/* node indices, in internal sequence order */
	int			nrels;
	int			cap;
	double		T;				/* product of t_i over the module */
	double		C;				/* ASI cost of the module's internal sequence */
	double		rank;			/* (T - 1) / C */
} IkkbzModule;

/* Memo entry mapping a relid set to its best joinrel (DP table). */
typedef struct LindpMemoEntry
{
	Relids		relids;			/* hash key (must be first) */
	RelOptInfo *rel;
} LindpMemoEntry;

/* Per-run DP state. */
typedef struct LindpRun
{
	PlannerInfo *root;
	int			n;				/* number of nodes */
	RelOptInfo **lin;			/* lin[pos] = rel at linear position pos */
	Relids		all_relids;		/* union of all node relids */
	HTAB	   *memo;			/* relids -> RelOptInfo* */
	long		budget;			/* remaining make_join_rel budget */
	bool		failed;			/* set when the budget was exhausted */
} LindpRun;

/* joingraph.c */
extern LinGraph *lindp_build_graph(PlannerInfo *root, List *initial_rels);
int lindp_compute_components(PlannerInfo *root, List *initial_rels, int **labels);

/* ikkbz.c */
int ikkbz_compute_linearizations(LinGraph *g, int nseeds, int **orders);

#endif							/* LINDP_H */
