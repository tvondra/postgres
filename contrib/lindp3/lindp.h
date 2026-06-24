#ifndef LINDP_H
#define LINDP_H

#include "postgres.h"

#include "nodes/pathnodes.h"

#define LINDP_TINY	1e-9

/*
 * Per-invocation working state.
 *
 * All "global index" values below are 0-based indexes into rels[].
 */
typedef struct LinDPState
{
	PlannerInfo *root;
	int			n;				/* number of initial relations */
	RelOptInfo **rels;			/* the initial relations, [0..n-1] */
	double	   *card;			/* cardinality estimate per relation */

	/* Spanning-tree / IKKBZ scratch (sized n) */
	int		   *parent;			/* precedence-tree parent (global idx) or -1 */
	double	   *trel;			/* IKKBZ T value per relation */
	List	  **children;		/* precedence-tree children (List of int) */
} LinDPState;

/*
 * A "module" (compound relation) used while merging chains in IKKBZ.
 *
 * The ASI cost model assigns to every sequence S of relations a value
 *		C(S)   = C(S1) + T(S1) * C(S2)			(for S = S1 S2)
 *		T(S)   = product of per-relation T values
 *		rank(S) = (T(S) - 1) / C(S)
 * and the optimal left-deep order (subject to the precedence constraints of
 * the rooted query tree) is obtained by sorting by rank, contracting adjacent
 * rank-violating modules.
 */
typedef struct IkModule
{
	List	   *items;			/* ordered List of int (global rel indexes) */
	double		t;				/* T(S) */
	double		c;				/* C(S) */
	double		rank;			/* (T-1)/C */
} IkModule;

/* For sorting candidate orders by ASI cost. */
typedef struct OrderCandidate
{
	List	   *order;
	double		cost;
} OrderCandidate;

/* ikkbz.c */
List *ikkbz_order_for_root(LinDPState *st, List *comp, int rootidx, double *cost_out);
bool ikkbz_rels_connected(LinDPState *st, int i, int j);

#endif							/* LINDP_H */
