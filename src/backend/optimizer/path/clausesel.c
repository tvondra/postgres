/*-------------------------------------------------------------------------
 *
 * clausesel.c
 *	  Routines to compute clause selectivities
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/clausesel.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/pg_operator.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/var.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/mvstats.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"


/*
 * Data structure for accumulating info about possible range-query
 * clause pairs in clauselist_selectivity.
 */
typedef struct RangeQueryClause
{
	struct RangeQueryClause *next;		/* next in linked list */
	Node	   *var;			/* The common variable of the clauses */
	bool		have_lobound;	/* found a low-bound clause yet? */
	bool		have_hibound;	/* found a high-bound clause yet? */
	Selectivity lobound;		/* Selectivity of a var > something clause */
	Selectivity hibound;		/* Selectivity of a var < something clause */
} RangeQueryClause;

static void addRangeClause(RangeQueryClause **rqlist, Node *clause,
			   bool varonleft, bool isLTsel, Selectivity s2);

#define		MV_CLAUSE_TYPE_FDEP		0x01

static bool clause_is_mv_compatible(PlannerInfo *root, Node *clause, Oid varRelid,
							 Index *relid, AttrNumber *attnum, SpecialJoinInfo *sjinfo);

static Bitmapset  *collect_mv_attnums(PlannerInfo *root, List *clauses,
									  Oid varRelid, Index *relid, SpecialJoinInfo *sjinfo);

static List *clauselist_apply_dependencies(PlannerInfo *root, List *clauses,
								Oid varRelid, List *stats,
								SpecialJoinInfo *sjinfo);

static bool has_stats(List *stats, int type);

static List * find_stats(PlannerInfo *root, List *clauses,
						 Oid varRelid, Index *relid);
 
static Bitmapset* fdeps_collect_attnums(List *stats);

static int	*make_idx_to_attnum_mapping(Bitmapset *attnums);
static int	*make_attnum_to_idx_mapping(Bitmapset *attnums);

static bool	*build_adjacency_matrix(List *stats, Bitmapset *attnums,
								int *idx_to_attnum, int *attnum_to_idx);

static void	multiply_adjacency_matrix(bool *matrix, int natts);

static List* fdeps_reduce_clauses(List *clauses,
								  Bitmapset *attnums, bool *matrix,
								  int *idx_to_attnum, int *attnum_to_idx,
								  Index relid);

static Bitmapset *fdeps_filter_clauses(PlannerInfo *root,
					 List *clauses, Bitmapset *deps_attnums,
					 List **reduced_clauses, List **deps_clauses,
					 Oid varRelid, Index *relid, SpecialJoinInfo *sjinfo);

static Bitmapset * get_varattnos(Node * node, Index relid);

/****************************************************************************
 *		ROUTINES TO COMPUTE SELECTIVITIES
 ****************************************************************************/

/*
 * clauselist_selectivity -
 *	  Compute the selectivity of an implicitly-ANDed list of boolean
 *	  expression clauses.  The list can be empty, in which case 1.0
 *	  must be returned.  List elements may be either RestrictInfos
 *	  or bare expression clauses --- the former is preferred since
 *	  it allows caching of results.
 *
 * See clause_selectivity() for the meaning of the additional parameters.
 *
 * Our basic approach is to take the product of the selectivities of the
 * subclauses.  However, that's only right if the subclauses have independent
 * probabilities, and in reality they are often NOT independent.  So,
 * we want to be smarter where we can.
 *
 * Currently, the only extra smarts we have is to recognize "range queries",
 * such as "x > 34 AND x < 42".  Clauses are recognized as possible range
 * query components if they are restriction opclauses whose operators have
 * scalarltsel() or scalargtsel() as their restriction selectivity estimator.
 * We pair up clauses of this form that refer to the same variable.  An
 * unpairable clause of this kind is simply multiplied into the selectivity
 * product in the normal way.  But when we find a pair, we know that the
 * selectivities represent the relative positions of the low and high bounds
 * within the column's range, so instead of figuring the selectivity as
 * hisel * losel, we can figure it as hisel + losel - 1.  (To visualize this,
 * see that hisel is the fraction of the range below the high bound, while
 * losel is the fraction above the low bound; so hisel can be interpreted
 * directly as a 0..1 value but we need to convert losel to 1-losel before
 * interpreting it as a value.  Then the available range is 1-losel to hisel.
 * However, this calculation double-excludes nulls, so really we need
 * hisel + losel + null_frac - 1.)
 *
 * If either selectivity is exactly DEFAULT_INEQ_SEL, we forget this equation
 * and instead use DEFAULT_RANGE_INEQ_SEL.  The same applies if the equation
 * yields an impossible (negative) result.
 *
 * A free side-effect is that we can recognize redundant inequalities such
 * as "x < 4 AND x < 5"; only the tighter constraint will be counted.
 *
 * Of course this is all very dependent on the behavior of
 * scalarltsel/scalargtsel; perhaps some day we can generalize the approach.
 *
 *
 * Multivariate statististics
 * --------------------------
 * This also uses multivariate stats to estimate combinations of
 * conditions, in a way (a) maximizing the estimate accuracy by using
 * as many stats as possible, and (b) minimizing the overhead,
 * especially when there are no suitable multivariate stats (so if you
 * are not using multivariate stats, there's no additional overhead).
 *
 * The following checks are performed (in this order), and the optimizer
 * falls back to regular stats on the first 'false'.
 *
 * NOTE: This explains how this works with all the patches applied, not
 *       just the functional dependencies.
 *
 * (0) check if there are multivariate stats on the relation
 *
 *     If no, just skip all the following steps (directly to the
 *     original code).
 *
 * (1) check how many attributes are there in conditions compatible
 *     with functional dependencies
 *
 *     Only simple equality clauses are considered compatible with
 *     functional dependencies (and that's unlikely to change, because
 *     that's the only case when functional dependencies are useful).
 *
 *     If there are no conditions that might be handled by multivariate
 *     stats, or if the conditions reference just a single column, it
 *     makes no sense to use functional dependencies, so skip to (4).
 *
 * (2) reduce the clauses using functional dependencies
 *
 *     This simply attempts to 'reduce' the clauses by applying functional
 *     dependencies. For example if there are two clauses:
 *
 *         WHERE (a = 1) AND (b = 2)
 *
 *     and we know that 'a' determines the value of 'b', we may remove
 *     the second condition (b = 2) when computing the selectivity.
 *     This is of course tricky - see mvstats/dependencies.c for details.
 *
 *     After the reduction, step (1) is to be repeated.
 *
 * (3) check how many attributes are there in conditions compatible
 *     with MCV lists and histograms
 *
 *     What conditions are compatible with multivariate stats is decided
 *     by clause_is_mv_compatible(). At this moment, only conditions
 *     of the form "column operator constant" (for simple comparison
 *     operators), IS [NOT] NULL and some AND/OR clauses are considered
 *     compatible with multivariate statistics.
 *
 *     Again, see clause_is_mv_compatible() for details.
 *
 * (4) check how many attributes are there in conditions compatible
 *     with MCV lists and histograms
 *
 *     If there are no conditions that might be handled by MCV lists
 *     or histograms, or if the conditions reference just a single
 *     column, it makes no sense to continue, so just skip to (7).
 *
 * (5) choose the stats matching the most columns
 *
 *     If there are multiple instances of multivariate statistics (e.g.
 *     built on different sets of columns), we choose the stats covering
 *     the most columns from step (1). It may happen that all available
 *     stats match just a single column - for example with conditions
 *
 *         WHERE a = 1 AND b = 2
 *
 *     and statistics built on (a,c) and (b,c). In such case just fall
 *     back to the regular stats because it makes no sense to use the
 *     multivariate statistics.
 *
 *     For more details about how exactly we choose the stats, see
 *     choose_mv_statistics().
 *
 * (6) use the multivariate stats to estimate matching clauses
 *
 * (7) estimate the remaining clauses using the regular statistics
 */
Selectivity
clauselist_selectivity(PlannerInfo *root,
					   List *clauses,
					   int varRelid,
					   JoinType jointype,
					   SpecialJoinInfo *sjinfo)
{
	Selectivity s1 = 1.0;
	RangeQueryClause *rqlist = NULL;
	ListCell   *l;

	/* processing mv stats */
	Oid			relid = InvalidOid;

	/* attributes in mv-compatible clauses */
	Bitmapset  *mvattnums = NULL;
	List	   *stats = NIL;

	/* use clauses (not conditions), because those are always non-empty */
	stats = find_stats(root, clauses, varRelid, &relid);

	/*
	 * If there's exactly one clause, then no use in trying to match up pairs,
	 * so just go directly to clause_selectivity().
	 */
	if (list_length(clauses) == 1)
		return clause_selectivity(root, (Node *) linitial(clauses),
								  varRelid, jointype, sjinfo);

	/*
	 * Check that there are some stats with functional dependencies
	 * built (by walking the stats list). We're going to find that
	 * anyway when trying to apply the functional dependencies, but
	 * this is probably a tad faster.
	 */
	if (has_stats(stats, MV_CLAUSE_TYPE_FDEP))
	{
		/* collect attributes referenced by mv-compatible clauses */
		mvattnums = collect_mv_attnums(root, clauses, varRelid, &relid, sjinfo);

		/*
		 * If there are mv-compatible clauses, referencing at least two
		 * different columns (otherwise it makes no sense to use mv stats),
		 * try to reduce the clauses using functional dependencies, and
		 * recollect the attributes from the reduced list.
		 *
		 * We don't need to select a single statistics for this - we can
		 * apply all the functional dependencies we have.
		 */
		if (bms_num_members(mvattnums) >= 2)
			clauses = clauselist_apply_dependencies(root, clauses, varRelid,
													stats, sjinfo);
	}

	/*
	 * Initial scan over clauses.  Anything that doesn't look like a potential
	 * rangequery clause gets multiplied into s1 and forgotten. Anything that
	 * does gets inserted into an rqlist entry.
	 */
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);
		RestrictInfo *rinfo;
		Selectivity s2;

		/* Always compute the selectivity using clause_selectivity */
		s2 = clause_selectivity(root, clause, varRelid, jointype, sjinfo);

		/*
		 * Check for being passed a RestrictInfo.
		 *
		 * If it's a pseudoconstant RestrictInfo, then s2 is either 1.0 or
		 * 0.0; just use that rather than looking for range pairs.
		 */
		if (IsA(clause, RestrictInfo))
		{
			rinfo = (RestrictInfo *) clause;
			if (rinfo->pseudoconstant)
			{
				s1 = s1 * s2;
				continue;
			}
			clause = (Node *) rinfo->clause;
		}
		else
			rinfo = NULL;

		/*
		 * See if it looks like a restriction clause with a pseudoconstant on
		 * one side.  (Anything more complicated than that might not behave in
		 * the simple way we are expecting.)  Most of the tests here can be
		 * done more efficiently with rinfo than without.
		 */
		if (is_opclause(clause) && list_length(((OpExpr *) clause)->args) == 2)
		{
			OpExpr	   *expr = (OpExpr *) clause;
			bool		varonleft = true;
			bool		ok;

			if (rinfo)
			{
				ok = (bms_membership(rinfo->clause_relids) == BMS_SINGLETON) &&
					(is_pseudo_constant_clause_relids(lsecond(expr->args),
													  rinfo->right_relids) ||
					 (varonleft = false,
					  is_pseudo_constant_clause_relids(linitial(expr->args),
													   rinfo->left_relids)));
			}
			else
			{
				ok = (NumRelids(clause) == 1) &&
					(is_pseudo_constant_clause(lsecond(expr->args)) ||
					 (varonleft = false,
					  is_pseudo_constant_clause(linitial(expr->args))));
			}

			if (ok)
			{
				/*
				 * If it's not a "<" or ">" operator, just merge the
				 * selectivity in generically.  But if it's the right oprrest,
				 * add the clause to rqlist for later processing.
				 */
				switch (get_oprrest(expr->opno))
				{
					case F_SCALARLTSEL:
						addRangeClause(&rqlist, clause,
									   varonleft, true, s2);
						break;
					case F_SCALARGTSEL:
						addRangeClause(&rqlist, clause,
									   varonleft, false, s2);
						break;
					default:
						/* Just merge the selectivity in generically */
						s1 = s1 * s2;
						break;
				}
				continue;		/* drop to loop bottom */
			}
		}

		/* Not the right form, so treat it generically. */
		s1 = s1 * s2;
	}

	/*
	 * Now scan the rangequery pair list.
	 */
	while (rqlist != NULL)
	{
		RangeQueryClause *rqnext;

		if (rqlist->have_lobound && rqlist->have_hibound)
		{
			/* Successfully matched a pair of range clauses */
			Selectivity s2;

			/*
			 * Exact equality to the default value probably means the
			 * selectivity function punted.  This is not airtight but should
			 * be good enough.
			 */
			if (rqlist->hibound == DEFAULT_INEQ_SEL ||
				rqlist->lobound == DEFAULT_INEQ_SEL)
			{
				s2 = DEFAULT_RANGE_INEQ_SEL;
			}
			else
			{
				s2 = rqlist->hibound + rqlist->lobound - 1.0;

				/* Adjust for double-exclusion of NULLs */
				s2 += nulltestsel(root, IS_NULL, rqlist->var,
								  varRelid, jointype, sjinfo);

				/*
				 * A zero or slightly negative s2 should be converted into a
				 * small positive value; we probably are dealing with a very
				 * tight range and got a bogus result due to roundoff errors.
				 * However, if s2 is very negative, then we probably have
				 * default selectivity estimates on one or both sides of the
				 * range that we failed to recognize above for some reason.
				 */
				if (s2 <= 0.0)
				{
					if (s2 < -0.01)
					{
						/*
						 * No data available --- use a default estimate that
						 * is small, but not real small.
						 */
						s2 = DEFAULT_RANGE_INEQ_SEL;
					}
					else
					{
						/*
						 * It's just roundoff error; use a small positive
						 * value
						 */
						s2 = 1.0e-10;
					}
				}
			}
			/* Merge in the selectivity of the pair of clauses */
			s1 *= s2;
		}
		else
		{
			/* Only found one of a pair, merge it in generically */
			if (rqlist->have_lobound)
				s1 *= rqlist->lobound;
			else
				s1 *= rqlist->hibound;
		}
		/* release storage and advance */
		rqnext = rqlist->next;
		pfree(rqlist);
		rqlist = rqnext;
	}

	return s1;
}

/*
 * addRangeClause --- add a new range clause for clauselist_selectivity
 *
 * Here is where we try to match up pairs of range-query clauses
 */
static void
addRangeClause(RangeQueryClause **rqlist, Node *clause,
			   bool varonleft, bool isLTsel, Selectivity s2)
{
	RangeQueryClause *rqelem;
	Node	   *var;
	bool		is_lobound;

	if (varonleft)
	{
		var = get_leftop((Expr *) clause);
		is_lobound = !isLTsel;	/* x < something is high bound */
	}
	else
	{
		var = get_rightop((Expr *) clause);
		is_lobound = isLTsel;	/* something < x is low bound */
	}

	for (rqelem = *rqlist; rqelem; rqelem = rqelem->next)
	{
		/*
		 * We use full equal() here because the "var" might be a function of
		 * one or more attributes of the same relation...
		 */
		if (!equal(var, rqelem->var))
			continue;
		/* Found the right group to put this clause in */
		if (is_lobound)
		{
			if (!rqelem->have_lobound)
			{
				rqelem->have_lobound = true;
				rqelem->lobound = s2;
			}
			else
			{

				/*------
				 * We have found two similar clauses, such as
				 * x < y AND x < z.
				 * Keep only the more restrictive one.
				 *------
				 */
				if (rqelem->lobound > s2)
					rqelem->lobound = s2;
			}
		}
		else
		{
			if (!rqelem->have_hibound)
			{
				rqelem->have_hibound = true;
				rqelem->hibound = s2;
			}
			else
			{

				/*------
				 * We have found two similar clauses, such as
				 * x > y AND x > z.
				 * Keep only the more restrictive one.
				 *------
				 */
				if (rqelem->hibound > s2)
					rqelem->hibound = s2;
			}
		}
		return;
	}

	/* No matching var found, so make a new clause-pair data structure */
	rqelem = (RangeQueryClause *) palloc(sizeof(RangeQueryClause));
	rqelem->var = var;
	if (is_lobound)
	{
		rqelem->have_lobound = true;
		rqelem->have_hibound = false;
		rqelem->lobound = s2;
	}
	else
	{
		rqelem->have_lobound = false;
		rqelem->have_hibound = true;
		rqelem->hibound = s2;
	}
	rqelem->next = *rqlist;
	*rqlist = rqelem;
}

/*
 * bms_is_subset_singleton
 *
 * Same result as bms_is_subset(s, bms_make_singleton(x)),
 * but a little faster and doesn't leak memory.
 *
 * Is this of use anywhere else?  If so move to bitmapset.c ...
 */
static bool
bms_is_subset_singleton(const Bitmapset *s, int x)
{
	switch (bms_membership(s))
	{
		case BMS_EMPTY_SET:
			return true;
		case BMS_SINGLETON:
			return bms_is_member(x, s);
		case BMS_MULTIPLE:
			return false;
	}
	/* can't get here... */
	return false;
}

/*
 * treat_as_join_clause -
 *	  Decide whether an operator clause is to be handled by the
 *	  restriction or join estimator.  Subroutine for clause_selectivity().
 */
static inline bool
treat_as_join_clause(Node *clause, RestrictInfo *rinfo,
					 int varRelid, SpecialJoinInfo *sjinfo)
{
	if (varRelid != 0)
	{
		/*
		 * Caller is forcing restriction mode (eg, because we are examining an
		 * inner indexscan qual).
		 */
		return false;
	}
	else if (sjinfo == NULL)
	{
		/*
		 * It must be a restriction clause, since it's being evaluated at a
		 * scan node.
		 */
		return false;
	}
	else
	{
		/*
		 * Otherwise, it's a join if there's more than one relation used. We
		 * can optimize this calculation if an rinfo was passed.
		 *
		 * XXX	Since we know the clause is being evaluated at a join, the
		 * only way it could be single-relation is if it was delayed by outer
		 * joins.  Although we can make use of the restriction qual estimators
		 * anyway, it seems likely that we ought to account for the
		 * probability of injected nulls somehow.
		 */
		if (rinfo)
			return (bms_membership(rinfo->clause_relids) == BMS_MULTIPLE);
		else
			return (NumRelids(clause) > 1);
	}
}


/*
 * clause_selectivity -
 *	  Compute the selectivity of a general boolean expression clause.
 *
 * The clause can be either a RestrictInfo or a plain expression.  If it's
 * a RestrictInfo, we try to cache the selectivity for possible re-use,
 * so passing RestrictInfos is preferred.
 *
 * varRelid is either 0 or a rangetable index.
 *
 * When varRelid is not 0, only variables belonging to that relation are
 * considered in computing selectivity; other vars are treated as constants
 * of unknown values.  This is appropriate for estimating the selectivity of
 * a join clause that is being used as a restriction clause in a scan of a
 * nestloop join's inner relation --- varRelid should then be the ID of the
 * inner relation.
 *
 * When varRelid is 0, all variables are treated as variables.  This
 * is appropriate for ordinary join clauses and restriction clauses.
 *
 * jointype is the join type, if the clause is a join clause.  Pass JOIN_INNER
 * if the clause isn't a join clause.
 *
 * sjinfo is NULL for a non-join clause, otherwise it provides additional
 * context information about the join being performed.  There are some
 * special cases:
 *	1. For a special (not INNER) join, sjinfo is always a member of
 *	   root->join_info_list.
 *	2. For an INNER join, sjinfo is just a transient struct, and only the
 *	   relids and jointype fields in it can be trusted.
 * It is possible for jointype to be different from sjinfo->jointype.
 * This indicates we are considering a variant join: either with
 * the LHS and RHS switched, or with one input unique-ified.
 *
 * Note: when passing nonzero varRelid, it's normally appropriate to set
 * jointype == JOIN_INNER, sjinfo == NULL, even if the clause is really a
 * join clause; because we aren't treating it as a join clause.
 */
Selectivity
clause_selectivity(PlannerInfo *root,
				   Node *clause,
				   int varRelid,
				   JoinType jointype,
				   SpecialJoinInfo *sjinfo)
{
	Selectivity s1 = 0.5;		/* default for any unhandled clause type */
	RestrictInfo *rinfo = NULL;
	bool		cacheable = false;

	if (clause == NULL)			/* can this still happen? */
		return s1;

	if (IsA(clause, RestrictInfo))
	{
		rinfo = (RestrictInfo *) clause;

		/*
		 * If the clause is marked pseudoconstant, then it will be used as a
		 * gating qual and should not affect selectivity estimates; hence
		 * return 1.0.  The only exception is that a constant FALSE may be
		 * taken as having selectivity 0.0, since it will surely mean no rows
		 * out of the plan.  This case is simple enough that we need not
		 * bother caching the result.
		 */
		if (rinfo->pseudoconstant)
		{
			if (!IsA(rinfo->clause, Const))
				return (Selectivity) 1.0;
		}

		/*
		 * If the clause is marked redundant, always return 1.0.
		 */
		if (rinfo->norm_selec > 1)
			return (Selectivity) 1.0;

		/*
		 * If possible, cache the result of the selectivity calculation for
		 * the clause.  We can cache if varRelid is zero or the clause
		 * contains only vars of that relid --- otherwise varRelid will affect
		 * the result, so mustn't cache.  Outer join quals might be examined
		 * with either their join's actual jointype or JOIN_INNER, so we need
		 * two cache variables to remember both cases.  Note: we assume the
		 * result won't change if we are switching the input relations or
		 * considering a unique-ified case, so we only need one cache variable
		 * for all non-JOIN_INNER cases.
		 */
		if (varRelid == 0 ||
			bms_is_subset_singleton(rinfo->clause_relids, varRelid))
		{
			/* Cacheable --- do we already have the result? */
			if (jointype == JOIN_INNER)
			{
				if (rinfo->norm_selec >= 0)
					return rinfo->norm_selec;
			}
			else
			{
				if (rinfo->outer_selec >= 0)
					return rinfo->outer_selec;
			}
			cacheable = true;
		}

		/*
		 * Proceed with examination of contained clause.  If the clause is an
		 * OR-clause, we want to look at the variant with sub-RestrictInfos,
		 * so that per-subclause selectivities can be cached.
		 */
		if (rinfo->orclause)
			clause = (Node *) rinfo->orclause;
		else
			clause = (Node *) rinfo->clause;
	}

	if (IsA(clause, Var))
	{
		Var		   *var = (Var *) clause;

		/*
		 * We probably shouldn't ever see an uplevel Var here, but if we do,
		 * return the default selectivity...
		 */
		if (var->varlevelsup == 0 &&
			(varRelid == 0 || varRelid == (int) var->varno))
		{
			/* Use the restriction selectivity function for a bool Var */
			s1 = boolvarsel(root, (Node *) var, varRelid);
		}
	}
	else if (IsA(clause, Const))
	{
		/* bool constant is pretty easy... */
		Const	   *con = (Const *) clause;

		s1 = con->constisnull ? 0.0 :
			DatumGetBool(con->constvalue) ? 1.0 : 0.0;
	}
	else if (IsA(clause, Param))
	{
		/* see if we can replace the Param */
		Node	   *subst = estimate_expression_value(root, clause);

		if (IsA(subst, Const))
		{
			/* bool constant is pretty easy... */
			Const	   *con = (Const *) subst;

			s1 = con->constisnull ? 0.0 :
				DatumGetBool(con->constvalue) ? 1.0 : 0.0;
		}
		else
		{
			/* XXX any way to do better than default? */
		}
	}
	else if (not_clause(clause))
	{
		/* inverse of the selectivity of the underlying clause */
		s1 = 1.0 - clause_selectivity(root,
								  (Node *) get_notclausearg((Expr *) clause),
									  varRelid,
									  jointype,
									  sjinfo);
	}
	else if (and_clause(clause))
	{
		/* share code with clauselist_selectivity() */
		s1 = clauselist_selectivity(root,
									((BoolExpr *) clause)->args,
									varRelid,
									jointype,
									sjinfo);
	}
	else if (or_clause(clause))
	{
		/*
		 * Selectivities for an OR clause are computed as s1+s2 - s1*s2 to
		 * account for the probable overlap of selected tuple sets.
		 *
		 * XXX is this too conservative?
		 */
		ListCell   *arg;

		s1 = 0.0;
		foreach(arg, ((BoolExpr *) clause)->args)
		{
			Selectivity s2 = clause_selectivity(root,
												(Node *) lfirst(arg),
												varRelid,
												jointype,
												sjinfo);

			s1 = s1 + s2 - s1 * s2;
		}
	}
	else if (is_opclause(clause) || IsA(clause, DistinctExpr))
	{
		OpExpr	   *opclause = (OpExpr *) clause;
		Oid			opno = opclause->opno;

		if (treat_as_join_clause(clause, rinfo, varRelid, sjinfo))
		{
			/* Estimate selectivity for a join clause. */
			s1 = join_selectivity(root, opno,
								  opclause->args,
								  opclause->inputcollid,
								  jointype,
								  sjinfo);
		}
		else
		{
			/* Estimate selectivity for a restriction clause. */
			s1 = restriction_selectivity(root, opno,
										 opclause->args,
										 opclause->inputcollid,
										 varRelid);
		}

		/*
		 * DistinctExpr has the same representation as OpExpr, but the
		 * contained operator is "=" not "<>", so we must negate the result.
		 * This estimation method doesn't give the right behavior for nulls,
		 * but it's better than doing nothing.
		 */
		if (IsA(clause, DistinctExpr))
			s1 = 1.0 - s1;
	}
	else if (IsA(clause, ScalarArrayOpExpr))
	{
		/* Use node specific selectivity calculation function */
		s1 = scalararraysel(root,
							(ScalarArrayOpExpr *) clause,
							treat_as_join_clause(clause, rinfo,
												 varRelid, sjinfo),
							varRelid,
							jointype,
							sjinfo);
	}
	else if (IsA(clause, RowCompareExpr))
	{
		/* Use node specific selectivity calculation function */
		s1 = rowcomparesel(root,
						   (RowCompareExpr *) clause,
						   varRelid,
						   jointype,
						   sjinfo);
	}
	else if (IsA(clause, NullTest))
	{
		/* Use node specific selectivity calculation function */
		s1 = nulltestsel(root,
						 ((NullTest *) clause)->nulltesttype,
						 (Node *) ((NullTest *) clause)->arg,
						 varRelid,
						 jointype,
						 sjinfo);
	}
	else if (IsA(clause, BooleanTest))
	{
		/* Use node specific selectivity calculation function */
		s1 = booltestsel(root,
						 ((BooleanTest *) clause)->booltesttype,
						 (Node *) ((BooleanTest *) clause)->arg,
						 varRelid,
						 jointype,
						 sjinfo);
	}
	else if (IsA(clause, CurrentOfExpr))
	{
		/* CURRENT OF selects at most one row of its table */
		CurrentOfExpr *cexpr = (CurrentOfExpr *) clause;
		RelOptInfo *crel = find_base_rel(root, cexpr->cvarno);

		if (crel->tuples > 0)
			s1 = 1.0 / crel->tuples;
	}
	else if (IsA(clause, RelabelType))
	{
		/* Not sure this case is needed, but it can't hurt */
		s1 = clause_selectivity(root,
								(Node *) ((RelabelType *) clause)->arg,
								varRelid,
								jointype,
								sjinfo);
	}
	else if (IsA(clause, CoerceToDomain))
	{
		/* Not sure this case is needed, but it can't hurt */
		s1 = clause_selectivity(root,
								(Node *) ((CoerceToDomain *) clause)->arg,
								varRelid,
								jointype,
								sjinfo);
	}
	else
	{
		/*
		 * For anything else, see if we can consider it as a boolean variable.
		 * This only works if it's an immutable expression in Vars of a single
		 * relation; but there's no point in us checking that here because
		 * boolvarsel() will do it internally, and return a suitable default
		 * selectivity if not.
		 */
		s1 = boolvarsel(root, clause, varRelid);
	}

	/* Cache the result if possible */
	if (cacheable)
	{
		if (jointype == JOIN_INNER)
			rinfo->norm_selec = s1;
		else
			rinfo->outer_selec = s1;
	}

#ifdef SELECTIVITY_DEBUG
	elog(DEBUG4, "clause_selectivity: s1 %f", s1);
#endif   /* SELECTIVITY_DEBUG */

	return s1;
}

/*
 * Collect attributes from mv-compatible clauses.
 */
static Bitmapset *
collect_mv_attnums(PlannerInfo *root, List *clauses, Oid varRelid,
				   Index *relid, SpecialJoinInfo *sjinfo)
{
	Bitmapset  *attnums = NULL;
	ListCell   *l;

	/*
	 * Walk through the clauses and identify the ones we can estimate
	 * using multivariate stats, and remember the relid/columns. We'll
	 * then cross-check if we have suitable stats, and only if needed
	 * we'll split the clauses into multivariate and regular lists.
	 *
	 * For now we're only interested in RestrictInfo nodes with nested
	 * OpExpr, using either a range or equality.
	 */
	foreach (l, clauses)
	{
		AttrNumber attnum;
		Node	   *clause = (Node *) lfirst(l);

		/* ignore the result for now - we only need the info */
		if (clause_is_mv_compatible(root, clause, varRelid, relid, &attnum, sjinfo))
			attnums = bms_add_member(attnums, attnum);
	}

	/*
	 * If there are not at least two attributes referenced by the clause(s),
	 * we can throw everything out (as we'll revert to simple stats).
	 */
	if (bms_num_members(attnums) <= 1)
	{
		if (attnums != NULL)
			pfree(attnums);
		attnums = NULL;
		*relid = InvalidOid;
	}

	return attnums;
}

/*
 * Determines whether the clause is compatible with multivariate stats,
 * and if it is, returns some additional information - varno (index
 * into simple_rte_array) and a bitmap of attributes. This is then
 * used to fetch related multivariate statistics.
 *
 * At this moment we only support basic conditions of the form
 *
 *     variable OP constant
 *
 * where OP is one of [=,<,<=,>=,>] (which is however determined by
 * looking at the associated function for estimating selectivity, just
 * like with the single-dimensional case).
 *
 * TODO Support 'OR clauses' - shouldn't be all that difficult to
 *      evaluate them using multivariate stats.
 */
static bool
clause_is_mv_compatible(PlannerInfo *root, Node *clause, Oid varRelid,
						Index *relid, AttrNumber *attnum, SpecialJoinInfo *sjinfo)
{

	if (IsA(clause, RestrictInfo))
	{
		RestrictInfo *rinfo = (RestrictInfo *) clause;

		/* Pseudoconstants are not really interesting here. */
		if (rinfo->pseudoconstant)
			return false;

		/* no support for OR clauses at this point */
		if (rinfo->orclause)
			return false;

		/* get the actual clause from the RestrictInfo (it's not an OR clause) */
		clause = (Node*)rinfo->clause;

		/* only simple opclauses are compatible with multivariate stats */
		if (! is_opclause(clause))
			return false;

		/* we don't support join conditions at this moment */
		if (treat_as_join_clause(clause, rinfo, varRelid, sjinfo))
			return false;

		/* is it 'variable op constant' ? */
		if (list_length(((OpExpr *) clause)->args) == 2)
		{
			OpExpr	   *expr = (OpExpr *) clause;
			bool		varonleft = true;
			bool		ok;

			ok = (bms_membership(rinfo->clause_relids) == BMS_SINGLETON) &&
				(is_pseudo_constant_clause_relids(lsecond(expr->args),
												rinfo->right_relids) ||
				(varonleft = false,
				is_pseudo_constant_clause_relids(linitial(expr->args),
												rinfo->left_relids)));

			if (ok)
			{
				Var * var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);

				/*
				 * Simple variables only - otherwise the planner_rt_fetch seems to fail
				 * (return NULL).
				 *
				 * TODO Maybe use examine_variable() would fix that?
				 */
				if (! (IsA(var, Var) && (varRelid == 0 || varRelid == var->varno)))
					return false;

				/*
				 * Only consider this variable if (varRelid == 0) or when the varno
				 * matches varRelid (see explanation at clause_selectivity).
				 *
				 * FIXME I suspect this may not be really necessary. The (varRelid == 0)
				 *       part seems to be enforced by treat_as_join_clause().
				 */
				if (! ((varRelid == 0) || (varRelid == var->varno)))
					return false;

				/* Also skip special varno values, and system attributes ... */
				if ((IS_SPECIAL_VARNO(var->varno)) || (! AttrNumberIsForUserDefinedAttr(var->varattno)))
					return false;

				*relid = var->varno;

				/*
				 * If it's not a "<" or ">" or "=" operator, just ignore the
				 * clause. Otherwise note the relid and attnum for the variable.
				 * This uses the function for estimating selectivity, ont the
				 * operator directly (a bit awkward, but well ...).
				 */
				switch (get_oprrest(expr->opno))
					{
						case F_EQSEL:
							*attnum = var->varattno;
							return true;
					}
			}
		}
	}

	return false;

}

/*
 * Performs reduction of clauses using functional dependencies, i.e.
 * removes clauses that are considered redundant. It simply walks
 * through dependencies, and checks whether the dependency 'matches'
 * the clauses, i.e. if there's a clause matching the condition. If yes,
 * all clauses matching the implied part of the dependency are removed
 * from the list.
 *
 * This simply looks at attnums references by the clauses, not at the
 * type of the operator (equality, inequality, ...). This may not be the
 * right way to do - it certainly works best for equalities, which is
 * naturally consistent with functional dependencies (implications).
 * It's not clear that other operators are handled sensibly - for
 * example for inequalities, like
 *
 *     WHERE (A >= 10) AND (B <= 20)
 *
 * and a trivial case where [A == B], resulting in symmetric pair of
 * rules [A => B], [B => A], it's rather clear we can't remove either of
 * those clauses.
 *
 * That only highlights that functional dependencies are most suitable
 * for label-like data, where using non-equality operators is very rare.
 * Using the common city/zipcode example, clauses like
 *
 *     (zipcode <= 12345)
 *
 * or
 *
 *     (cityname >= 'Washington')
 *
 * are rare. So restricting the reduction to equality should not harm
 * the usefulness / applicability.
 *
 * The other assumption is that this assumes 'compatible' clauses. For
 * example by using mismatching zip code and city name, this is unable
 * to identify the discrepancy and eliminates one of the clauses. The
 * usual approach (multiplying both selectivities) thus produces a more
 * accurate estimate, although mostly by luck - the multiplication
 * comes from assumption of statistical independence of the two
 * conditions (which is not not valid in this case), but moves the
 * estimate in the right direction (towards 0%).
 *
 * This might be somewhat improved by cross-checking the selectivities
 * against MCV and/or histogram.
 *
 * The implementation needs to be careful about cyclic rules, i.e. rules
 * like [A => B] and [B => A] at the same time. This must not reduce
 * clauses on both attributes at the same time.
 *
 * Technically we might consider selectivities here too, somehow. E.g.
 * when (A => B) and (B => A), we might use the clauses with minimum
 * selectivity.
 *
 * TODO Consider restricting the reduction to equality clauses. Or maybe
 *      use equality classes somehow?
 *
 * TODO Merge this docs to dependencies.c, as it's saying mostly the
 *      same things as the comments there.
 *
 * TODO Currently this is applied only to the top-level clauses, but
 *      maybe we could apply it to lists at subtrees too, e.g. to the
 *      two AND-clauses in
 *
 *          (x=1 AND y=2) OR (z=3 AND q=10)
 *
 */
static List *
clauselist_apply_dependencies(PlannerInfo *root, List *clauses,
							  Oid varRelid, List *stats,
							  SpecialJoinInfo *sjinfo)
{
	List	   *reduced_clauses = NIL;
	Index		relid;

	/*
	 * matrix of (natts x natts), 1 means x=>y
	 *
	 * This serves two purposes - first, it merges dependencies from all
	 * the statistics, second it makes generating all the transitive
	 * dependencies easier.
	 *
	 * We need to build this only for attributes from the dependencies,
	 * not for all attributes in the table.
	 *
	 * We can't do that only for attributes from the clauses, because we
	 * want to build transitive dependencies (including those going
	 * through attributes not listed in the stats).
	 *
	 * This only works for A=>B dependencies, not sure how to do that
	 * for complex dependencies.
	 */
	bool       *deps_matrix;
	int			deps_natts;	/* size of the matric */

	/* mapping attnum <=> matrix index */
	int		   *deps_idx_to_attnum;
	int		   *deps_attnum_to_idx;

	/* attnums in dependencies and clauses (and intersection) */
	List	   *deps_clauses   = NIL;
	Bitmapset  *deps_attnums   = NULL;
	Bitmapset  *clause_attnums = NULL;
	Bitmapset  *intersect_attnums = NULL;

	/*
	 * Is there at least one statistics with functional dependencies?
	 * If not, return the original clauses right away.
	 *
	 * XXX Isn't this pointless, thanks to exactly the same check in
	 *     clauselist_selectivity()? Can we trigger the condition here?
	 */
	if (! has_stats(stats, MV_CLAUSE_TYPE_FDEP))
		return clauses;

	/*
	 * Build the dependency matrix, i.e. attribute adjacency matrix,
	 * where 1 means (a=>b). Once we have the adjacency matrix, we'll
	 * multiply it by itself, to get transitive dependencies.
	 *
	 * Note: This is pretty much transitive closure from graph theory.
	 *
	 * First, let's see what attributes are covered by functional
	 * dependencies (sides of the adjacency matrix), and also a maximum
	 * attribute (size of mapping to simple integer indexes);
	 */
	deps_attnums = fdeps_collect_attnums(stats);

	/*
	 * Walk through the clauses - clauses that are (one of)
	 *
	 * (a) not mv-compatible
	 * (b) are using more than a single attnum
	 * (c) using attnum not covered by functional depencencies
	 *
	 * may be copied directly to the result. The interesting clauses are
	 * kept in 'deps_clauses' and will be processed later.
	 */
	clause_attnums = fdeps_filter_clauses(root, clauses, deps_attnums,
										  &reduced_clauses, &deps_clauses,
										  varRelid, &relid, sjinfo);

	/*
	 * we need at least two clauses referencing two different attributes
	 * referencing to do the reduction
	 */
	if ((list_length(deps_clauses) < 2) || (bms_num_members(clause_attnums) < 2))
	{
		bms_free(clause_attnums);
		list_free(reduced_clauses);
		list_free(deps_clauses);

		return clauses;
	}


	/*
	 * We need at least two matching attributes in the clauses and
	 * dependencies, otherwise we can't really reduce anything.
	 */
	intersect_attnums = bms_intersect(clause_attnums, deps_attnums);
	if (bms_num_members(intersect_attnums) < 2)
	{
		bms_free(clause_attnums);
		bms_free(deps_attnums);
		bms_free(intersect_attnums);

		list_free(deps_clauses);
		list_free(reduced_clauses);

		return clauses;
	}

	/*
	 * Build mapping between matrix indexes and attnums, and then the
	 * adjacency matrix itself.
	 */
	deps_idx_to_attnum = make_idx_to_attnum_mapping(deps_attnums);
	deps_attnum_to_idx = make_attnum_to_idx_mapping(deps_attnums);

	/* build the adjacency matrix */
	deps_matrix = build_adjacency_matrix(stats, deps_attnums,
										 deps_idx_to_attnum,
										 deps_attnum_to_idx);

	deps_natts = bms_num_members(deps_attnums);

	/*
	 * Multiply the matrix N-times (N = size of the matrix), so that we
	 * get all the transitive dependencies. That makes the next step
	 * much easier and faster.
	 *
	 * This is essentially an adjacency matrix from graph theory, and
	 * by multiplying it we get transitive edges. We don't really care
	 * about the exact number (number of paths between vertices) though,
	 * so we can do the multiplication in-place (we don't care whether
	 * we found the dependency in this round or in the previous one).
	 *
	 * Track how many new dependencies were added, and stop when 0, but
	 * we can't multiply more than N-times (longest path in the graph).
	 */
	multiply_adjacency_matrix(deps_matrix, deps_natts);

	/*
	 * Walk through the clauses, and see which other clauses we may
	 * reduce. The matrix contains all transitive dependencies, which
	 * makes this very fast.
	 *
	 * We have to be careful not to reduce the clause using itself, or
	 * reducing all clauses forming a cycle (so we have to skip already
	 * eliminated clauses).
	 *
	 * I'm not sure whether this guarantees finding the best solution,
	 * i.e. reducing the most clauses, but it probably does (thanks to
	 * having all the transitive dependencies).
	 */
	deps_clauses = fdeps_reduce_clauses(deps_clauses,
										deps_attnums, deps_matrix,
										deps_idx_to_attnum,
										deps_attnum_to_idx, relid);

	/* join the two lists of clauses */
	reduced_clauses = list_union(reduced_clauses, deps_clauses);

	pfree(deps_matrix);
	pfree(deps_idx_to_attnum);
	pfree(deps_attnum_to_idx);

	bms_free(deps_attnums);
	bms_free(clause_attnums);
	bms_free(intersect_attnums);

	return reduced_clauses;
}

static bool
has_stats(List *stats, int type)
{
	ListCell   *s;

	foreach (s, stats)
	{
		MVStatisticInfo	*stat = (MVStatisticInfo *)lfirst(s);

		if ((type & MV_CLAUSE_TYPE_FDEP) && stat->deps_built)
			return true;
	}

	return false;
}

/*
 * Determing relid (either from varRelid or from clauses) and then
 * lookup stats using the relid.
 */
static List *
find_stats(PlannerInfo *root, List *clauses, Oid varRelid, Index *relid)
{
	/* unknown relid by default */
	*relid = InvalidOid;

	/*
	 * First we need to find the relid (index info simple_rel_array).
	 * If varRelid is not 0, we already have it, otherwise we have to
	 * look it up from the clauses.
	 */
	if (varRelid != 0)
		*relid = varRelid;
	else
	{
		Relids	relids = pull_varnos((Node*)clauses);

		/*
		 * We only expect 0 or 1 members in the bitmapset. If there are
		 * no vars, we'll get empty bitmapset, otherwise we'll get the
		 * relid as the single member.
		 *
		 * FIXME For some reason we can get 2 relids here (e.g. \d in
		 *       psql does that).
		 */
		if (bms_num_members(relids) == 1)
			*relid = bms_singleton_member(relids);

		bms_free(relids);
	}

	/*
	 * if we found the relid, we can get the stats from simple_rel_array
	 *
	 * This only gets stats that are already built, because that's how
	 * we load it into RelOptInfo (see get_relation_info), but we don't
	 * detoast the whole stats yet. That'll be done later, after we
	 * decide which stats to use.
	 */
	if (*relid != InvalidOid)
		return root->simple_rel_array[*relid]->mvstatlist;

	return NIL;
}

static Bitmapset*
fdeps_collect_attnums(List *stats)
{
	ListCell *lc;
	Bitmapset *attnums = NULL;

	foreach (lc, stats)
	{
		int j;
		MVStatisticInfo *info = (MVStatisticInfo *)lfirst(lc);

		int2vector *stakeys = info->stakeys;

		/* skip stats without functional dependencies built */
		if (! info->deps_built)
			continue;

		for (j = 0; j < stakeys->dim1; j++)
			attnums = bms_add_member(attnums, stakeys->values[j]);
	}

	return attnums;
}


static int*
make_idx_to_attnum_mapping(Bitmapset *attnums)
{
	int		attidx = 0;
	int		attnum = -1;

	int	   *mapping = (int*)palloc0(bms_num_members(attnums) * sizeof(int));

	while ((attnum = bms_next_member(attnums, attnum)) >= 0)
		mapping[attidx++] = attnum;

	Assert(attidx == bms_num_members(attnums));

	return mapping;
}

static int*
make_attnum_to_idx_mapping(Bitmapset *attnums)
{
	int		attidx = 0;
	int		attnum = -1;
	int		maxattnum = -1;
	int	   *mapping;

	while ((attnum = bms_next_member(attnums, attnum)) >= 0)
		maxattnum = attnum;

	mapping = (int*)palloc0((maxattnum+1) * sizeof(int));

	attnum = -1;
	while ((attnum = bms_next_member(attnums, attnum)) >= 0)
		mapping[attnum] = attidx++;

	Assert(attidx == bms_num_members(attnums));

	return mapping;
}

static bool*
build_adjacency_matrix(List *stats, Bitmapset *attnums,
					   int *idx_to_attnum, int *attnum_to_idx)
{
	ListCell *lc;
	int		natts  = bms_num_members(attnums);
	bool   *matrix = (bool*)palloc0(natts * natts * sizeof(bool));

	foreach (lc, stats)
	{
		int j;
		MVStatisticInfo *stat = (MVStatisticInfo *)lfirst(lc);
		MVDependencies dependencies = NULL;

		/* skip stats without functional dependencies built */
		if (! stat->deps_built)
			continue;

		/* fetch and deserialize dependencies */
		dependencies = load_mv_dependencies(stat->mvoid);
		if (dependencies == NULL)
		{
			elog(WARNING, "failed to deserialize func deps %d", stat->mvoid);
			continue;
		}

		/* set matrix[a,b] to 'true' if 'a=>b' */
		for (j = 0; j < dependencies->ndeps; j++)
		{
			int aidx = attnum_to_idx[dependencies->deps[j]->a];
			int bidx = attnum_to_idx[dependencies->deps[j]->b];

			/* a=> b */
			matrix[aidx * natts + bidx] = true;
		}
	}

	return matrix;
}

static void
multiply_adjacency_matrix(bool *matrix, int natts)
{
	int i;

	for (i = 0; i < natts; i++)
	{
		int k, l, m;
		int nchanges = 0;

		/* k => l */
		for (k = 0; k < natts; k++)
		{
			for (l = 0; l < natts; l++)
			{
				/* we already have this dependency */
				if (matrix[k * natts + l])
					continue;

				/* we don't really care about the exact value, just 0/1 */
				for (m = 0; m < natts; m++)
				{
					if (matrix[k * natts + m] * matrix[m * natts + l])
					{
						matrix[k * natts + l] = true;
						nchanges += 1;
						break;
					}
				}
			}
		}

		/* no transitive dependency added here, so terminate */
		if (nchanges == 0)
			break;
	}
}

static List*
fdeps_reduce_clauses(List *clauses, Bitmapset *attnums, bool *matrix,
					int *idx_to_attnum, int *attnum_to_idx, Index relid)
{
	int i;
	ListCell *lc;
	List   *reduced_clauses = NIL;

	int			nmvclauses;	/* size of the arrays */
	bool	   *reduced;
	AttrNumber *mvattnums;
	Node	  **mvclauses;

	int			natts = bms_num_members(attnums);

	/*
	 * Preallocate space for all clauses (the list only containst
	 * compatible clauses at this point). This makes it somewhat easier
	 * to access the stats / attnums randomly.
	 *
	 * XXX This assumes each clause references exactly one Var, so the
	 *     arrays are sized accordingly - for functional dependencies
	 *     this is safe, because it only works with Var=Const.
	 */
	mvclauses = (Node**)palloc0(list_length(clauses) * sizeof(Node*));
	mvattnums = (AttrNumber*)palloc0(list_length(clauses) * sizeof(AttrNumber));
	reduced = (bool*)palloc0(list_length(clauses) * sizeof(bool));

	/* fill the arrays */
	nmvclauses = 0;
	foreach (lc, clauses)
	{
		Node * clause = (Node*)lfirst(lc);
		Bitmapset * attnums = get_varattnos(clause, relid);

		mvclauses[nmvclauses] = clause;
		mvattnums[nmvclauses] = bms_singleton_member(attnums);
		nmvclauses++;
	}

	Assert(nmvclauses == list_length(clauses));

	/* now try to reduce the clauses (using the dependencies) */
	for (i = 0; i < nmvclauses; i++)
	{
		int j;

		/* not covered by dependencies */
		if (! bms_is_member(mvattnums[i], attnums))
			continue;

		/* this clause was already reduced, so let's skip it */
		if (reduced[i])
			continue;

		/* walk the potentially 'implied' clauses */
		for (j = 0; j < nmvclauses; j++)
		{
			int aidx, bidx;

			/* not covered by dependencies */
			if (! bms_is_member(mvattnums[j], attnums))
				continue;

			aidx = attnum_to_idx[mvattnums[i]];
			bidx = attnum_to_idx[mvattnums[j]];

			/* can't reduce the clause by itself, or if already reduced */
			if ((i == j) || reduced[j])
				continue;

			/* mark the clause as reduced (if aidx => bidx) */
			reduced[j] = matrix[aidx * natts + bidx];
		}
	}

	/* now walk through the clauses, and keep only those not reduced */
	for (i = 0; i < nmvclauses; i++)
		if (! reduced[i])
			reduced_clauses = lappend(reduced_clauses, mvclauses[i]);

	pfree(reduced);
	pfree(mvclauses);
	pfree(mvattnums);

	return reduced_clauses;
}


static Bitmapset *
fdeps_filter_clauses(PlannerInfo *root,
					 List *clauses, Bitmapset *deps_attnums,
					 List **reduced_clauses, List **deps_clauses,
					 Oid varRelid, Index *relid, SpecialJoinInfo *sjinfo)
{
	ListCell *lc;
	Bitmapset *clause_attnums = NULL;

	foreach (lc, clauses)
	{
		AttrNumber	attnum;
		Node	   *clause = (Node *) lfirst(lc);

		if (! clause_is_mv_compatible(root, clause, varRelid, relid,
									  &attnum, sjinfo))

			/* clause incompatible with functional dependencies */
			*reduced_clauses = lappend(*reduced_clauses, clause);

		else if (! bms_is_member(attnum, deps_attnums))

			/* clause not covered by the dependencies */
			*reduced_clauses = lappend(*reduced_clauses, clause);

		else
		{
			*deps_clauses   = lappend(*deps_clauses, clause);
			clause_attnums = bms_add_member(clause_attnums, attnum);
		}
	}

	return clause_attnums;
}

/*
 * Pull varattnos from the clauses, similarly to pull_varattnos() but:
 *
 * (a) only get attributes for a particular relation (relid)
 * (b) ignore system attributes (we can't build stats on them anyway)
 *
 * This makes it possible to directly compare the result with attnum
 * values from pg_attribute etc.
 */
static Bitmapset *
get_varattnos(Node * node, Index relid)
{
	int			k;
	Bitmapset  *varattnos = NULL;
	Bitmapset  *result = NULL;

	/* get the varattnos */
	pull_varattnos(node, relid, &varattnos);

	k = -1;
	while ((k = bms_next_member(varattnos, k)) >= 0)
	{
		if (k + FirstLowInvalidHeapAttributeNumber > 0)
			result
				= bms_add_member(result,
								 k + FirstLowInvalidHeapAttributeNumber);
	}

	bms_free(varattnos);

	return result;
}
