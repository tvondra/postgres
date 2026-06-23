/*-------------------------------------------------------------------------
 *
 * lindp.h
 *	  Linearized join-order search (IKKBZ + LinDP) prototype.
 *
 * This test module installs a join_search_hook that replaces GEQO for
 * large join problems with a search-space linearization approach:
 *
 *	1. A join graph is built from the initial relations (joingraph.c).
 *	2. IKKBZ computes a good left-deep linear order under an ASI-conforming
 *	   surrogate cost (ikkbz.c).
 *	3. LinDP runs exact dynamic programming restricted to the contiguous
 *	   sub-ranges of that linear order, using the real PostgreSQL cost model
 *	   via make_join_rel() (lindp.c).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/lindp/lindp.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LINDP_H
#define LINDP_H

#include "nodes/pathnodes.h"

/*
 * Join graph over the initial relations.
 *
 * Each "node" is one entry of the initial_rels list passed to the join
 * search.  Note that an initial rel may itself already be a join relation
 * (when the jointree forces a sub-problem), so a node is identified by its
 * relid set rather than a single base relation.
 */
typedef struct LinDpGraph
{
	int			n;				/* number of nodes */
	RelOptInfo **rels;			/* node -> initial RelOptInfo (length n) */
	double	   *card;			/* per-node cardinality estimate (length n) */
	bool	  **adj;			/* adj[i][j]: relevant joinclause/restriction */
	double	  **sel;			/* sel[i][j]: edge selectivity (1.0 if none) */
} LinDpGraph;

/* joingraph.c */
extern LinDpGraph *lindp_build_graph(PlannerInfo *root, List *initial_rels);
extern bool lindp_graph_is_connected(LinDpGraph *graph);

/* ikkbz.c */
extern int *lindp_ikkbz_order(LinDpGraph *graph, int effort);

/* lindp.c */
extern RelOptInfo *lindp_join_search(PlannerInfo *root, int levels_needed,
									 List *initial_rels);

#endif							/* LINDP_H */
