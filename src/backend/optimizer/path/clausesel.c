/*-------------------------------------------------------------------------
 *
 * clausesel.c
 *	  Routines to compute clause selectivities
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
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
#include "catalog/pg_collation.h"
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
#define		MV_CLAUSE_TYPE_MCV		0x02
#define		MV_CLAUSE_TYPE_HIST		0x04

static bool clause_is_mv_compatible(PlannerInfo *root, Node *clause, Oid varRelid,
							 Index *relid, Bitmapset **attnums, SpecialJoinInfo *sjinfo,
							 int type);

static Bitmapset  *collect_mv_attnums(PlannerInfo *root, List *clauses,
									  Oid varRelid, Index *relid, SpecialJoinInfo *sjinfo,
									  int type);

static List *clauselist_apply_dependencies(PlannerInfo *root, List *clauses,
								Oid varRelid, List *stats,
								SpecialJoinInfo *sjinfo);

static MVStatisticInfo *choose_mv_statistics(List *mvstats, Bitmapset *attnums);

static List *clauselist_mv_split(PlannerInfo *root, SpecialJoinInfo *sjinfo,
								 List *clauses, Oid varRelid,
								 List **mvclauses, MVStatisticInfo *mvstats, int types);

static Selectivity clauselist_mv_selectivity(PlannerInfo *root,
						List *clauses, MVStatisticInfo *mvstats);
static Selectivity clauselist_mv_selectivity_mcvlist(PlannerInfo *root,
									List *clauses, MVStatisticInfo *mvstats,
									bool *fullmatch, Selectivity *lowsel);
static Selectivity clauselist_mv_selectivity_histogram(PlannerInfo *root,
									List *clauses, MVStatisticInfo *mvstats);

static int update_match_bitmap_mcvlist(PlannerInfo *root, List *clauses,
									int2vector *stakeys, MCVList mcvlist,
									int nmatches, char * matches,
									Selectivity *lowsel, bool *fullmatch,
									bool is_or);

static int update_match_bitmap_histogram(PlannerInfo *root, List *clauses,
									int2vector *stakeys,
									MVSerializedHistogram mvhist,
									int nmatches, char * matches,
									bool is_or);

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

/* used for merging bitmaps - AND (min), OR (max) */
#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

#define UPDATE_RESULT(m,r,isor)	\
	(m) = (isor) ? (MAX(m,r)) : (MIN(m,r))

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
		/*
		 * Collect attributes referenced by mv-compatible clauses (looking
		 * for clauses compatible with functional dependencies for now).
		 */
		mvattnums = collect_mv_attnums(root, clauses, varRelid, &relid, sjinfo,
									   MV_CLAUSE_TYPE_FDEP);

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
	 * Check that there are statistics with MCV list. If not, we don't
	 * need to waste time with the optimization.
	 */
	if (has_stats(stats, MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST))
	{
		/*
		 * Recollect attributes from mv-compatible clauses (maybe we've
		 * removed so many clauses we have a single mv-compatible attnum).
		 * From now on we're only interested in MCV-compatible clauses.
		 */
		mvattnums = collect_mv_attnums(root, clauses, varRelid, &relid, sjinfo,
									   (MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST));

		/*
		 * If there still are at least two columns, we'll try to select
		 * a suitable multivariate stats.
		 */
		if (bms_num_members(mvattnums) >= 2)
		{
			/* see choose_mv_statistics() for details */
			MVStatisticInfo *mvstat = choose_mv_statistics(stats, mvattnums);

			if (mvstat != NULL)	/* we have a matching stats */
			{
				/* clauses compatible with multi-variate stats */
				List	*mvclauses = NIL;

				/* split the clauselist into regular and mv-clauses */
				clauses = clauselist_mv_split(root, sjinfo, clauses,
										varRelid, &mvclauses, mvstat,
										(MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST));

				/* we've chosen the histogram to match the clauses */
				Assert(mvclauses != NIL);

				/* compute the multivariate stats */
				s1 *= clauselist_mv_selectivity(root, mvclauses, mvstat);
			}
		}
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
			/*
			 * A Var at the top of a clause must be a bool Var. This is
			 * equivalent to the clause reln.attribute = 't', so we compute
			 * the selectivity as if that is what we have.
			 */
			s1 = restriction_selectivity(root,
										 BooleanEqualOperator,
										 list_make2(var,
													makeBoolConst(true,
																  false)),
										 InvalidOid,
										 varRelid);
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
	else if (is_funcclause(clause))
	{
		/*
		 * This is not an operator, so we guess at the selectivity. THIS IS A
		 * HACK TO GET V4 OUT THE DOOR.  FUNCS SHOULD BE ABLE TO HAVE
		 * SELECTIVITIES THEMSELVES.       -- JMH 7/9/92
		 */
		s1 = (Selectivity) 0.3333333;
	}
#ifdef NOT_USED
	else if (IsA(clause, SubPlan) ||
			 IsA(clause, AlternativeSubPlan))
	{
		/*
		 * Just for the moment! FIX ME! - vadim 02/04/98
		 */
		s1 = (Selectivity) 0.5;
	}
#endif
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
 * Estimate selectivity for the list of MV-compatible clauses, using
 * using a MV statistics (combining a histogram and MCV list).
 *
 * This simply passes the estimation to the MCV list and then to the
 * histogram, if available.
 *
 * TODO Clamp the selectivity by min of the per-clause selectivities
 *      (i.e. the selectivity of the most restrictive clause), because
 *      that's the maximum we can ever get from ANDed list of clauses.
 *      This may probably prevent issues with hitting too many buckets
 *      and low precision histograms.
 *
 * TODO We may support some additional conditions, most importantly
 *      those matching multiple columns (e.g. "a = b" or "a < b").
 *      Ultimately we could track multi-table histograms for join
 *      cardinality estimation.
 *
 * TODO Further thoughts on processing equality clauses: Maybe it'd be
 *      better to look for stats (with MCV) covered by the equality
 *      clauses, because then we have a chance to find an exact match
 *      in the MCV list, which is pretty much the best we can do. We may
 *      also look at the least frequent MCV item, and use it as a upper
 *      boundary for the selectivity (had there been a more frequent
 *      item, it'd be in the MCV list).
 *
 * TODO There are several options for 'sanity clamping' the estimates.
 *
 *      First, if we have selectivities for each condition, then
 *
 *          P(A,B) <= MIN(P(A), P(B))
 *
 *      Because additional conditions (connected by AND) can only lower
 *      the probability.
 *
 *      So we can do some basic sanity checks using the single-variate
 *      stats (the ones we have right now).
 *
 *      Second, when we have multivariate stats with a MCV list, then
 *
 *      (a) if we have a full equality condition (one equality condition
 *          on each column) and we found a match in the MCV list, this is
 *          the selectivity (and it's supposed to be exact)
 *
 *      (b) if we have a full equality condition and we haven't found a
 *          match in the MCV list, then the selectivity is below the
 *          lowest selectivity in the MCV list
 *
 *      (c) if we have a equality condition (not full), we can still
 *          search the MCV for matches and use the sum of probabilities
 *          as a lower boundary for the histogram (if there are no
 *          matches in the MCV list, then we have no boundary)
 *
 *      Third, if there are multiple (combinations of) multivariate
 *      stats for a set of clauses, we may compute all of them and then
 *      somehow aggregate them - e.g. by choosing the minimum, median or
 *      average. The stats are susceptible to overestimation (because
 *      we take 50% of the bucket for partial matches). Some stats may
 *      give better estimates than others, but it's very difficult to
 *      say that in advance which one is the best (it depends on the
 *      number of buckets, number of additional columns not referenced
 *      in the clauses, type of condition etc.).
 *
 *      So we may compute them all and then choose a sane aggregation
 *      (minimum seems like a good approach). Of course, this may result
 *      in longer / more expensive estimation (CPU-wise), but it may be
 *      worth it.
 *
 *      It's possible to add a GUC choosing whether to do a 'simple'
 *      (using a single stats expected to give the best estimate) and
 *      'complex' (combining the multiple estimates).
 *
 *          multivariate_estimates = (simple|full)
 *
 *      Also, this might be enabled at a table level, by something like
 *
 *          ALTER TABLE ... SET STATISTICS (simple|full)
 *
 *      Which would make it possible to use this only for the tables
 *      where the simple approach does not work.
 *
 *      Also, there are ways to optimize this algorithmically. E.g. we
 *      may try to get an estimate from a matching MCV list first, and
 *      if we happen to get a "full equality match" we may stop computing
 *      the estimates from other stats (for this condition) because
 *      that's probably the best estimate we can really get.
 *
 * TODO When applying the clauses to the histogram/MCV list, we can do
 *      that from the most selective clauses first, because that'll
 *      eliminate the buckets/items sooner (so we'll be able to skip
 *      them without inspection, which is more expensive). But this
 *      requires really knowing the per-clause selectivities in advance,
 *      and that's not what we do now.
 */
static Selectivity
clauselist_mv_selectivity(PlannerInfo *root, List *clauses, MVStatisticInfo *mvstats)
{
	bool fullmatch = false;
	Selectivity s1 = 0.0, s2 = 0.0;

	/*
	 * Lowest frequency in the MCV list (may be used as an upper bound
	 * for full equality conditions that did not match any MCV item).
	 */
	Selectivity mcv_low = 0.0;

	/* TODO Evaluate simple 1D selectivities, use the smallest one as
	 *      an upper bound, product as lower bound, and sort the
	 *      clauses in ascending order by selectivity (to optimize the
	 *      MCV/histogram evaluation).
	 */

	/* Evaluate the MCV first. */
	s1 = clauselist_mv_selectivity_mcvlist(root, clauses, mvstats,
										   &fullmatch, &mcv_low);

	/*
	 * If we got a full equality match on the MCV list, we're done (and
	 * the estimate is pretty good).
	 */
	if (fullmatch && (s1 > 0.0))
		return s1;

	/* FIXME if (fullmatch) without matching MCV item, use the mcv_low
	 *       selectivity as upper bound */

	s2 = clauselist_mv_selectivity_histogram(root, clauses, mvstats);

	/* TODO clamp to <= 1.0 (or more strictly, when possible) */
	return s1 + s2;
}

/*
 * Collect attributes from mv-compatible clauses.
 */
static Bitmapset *
collect_mv_attnums(PlannerInfo *root, List *clauses, Oid varRelid,
				   Index *relid, SpecialJoinInfo *sjinfo, int types)
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
		Node	   *clause = (Node *) lfirst(l);

		/* ignore the result here - we only need the attnums */
		clause_is_mv_compatible(root, clause, varRelid, relid, &attnums,
								sjinfo, types);
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
 * We're looking for statistics matching at least 2 attributes,
 * referenced in the clauses compatible with multivariate statistics.
 * The current selection criteria is very simple - we choose the
 * statistics referencing the most attributes.
 *
 * If there are multiple statistics referencing the same number of
 * columns (from the clauses), the one with less source columns
 * (as listed in the ADD STATISTICS when creating the statistics) wins.
 * Other wise the first one wins.
 *
 * This is a very simple criteria, and has several weaknesses:
 *
 * (a) does not consider the accuracy of the statistics
 *
 *     If there are two histograms built on the same set of columns,
 *     but one has 100 buckets and the other one has 1000 buckets (thus
 *     likely providing better estimates), this is not currently
 *     considered.
 *
 * (b) does not consider the type of statistics
 *
 *     If there are three statistics - one containing just a MCV list,
 *     another one with just a histogram and a third one with both,
 *     this is not considered.
 *
 * (c) does not consider the number of clauses
 *
 *     As explained, only the number of referenced attributes counts,
 *     so if there are multiple clauses on a single attribute, this
 *     still counts as a single attribute.
 *
 * (d) does not consider type of condition
 *
 *     Some clauses may work better with some statistics - for example
 *     equality clauses probably work better with MCV lists than with
 *     histograms. But IS [NOT] NULL conditions may often work better
 *     with histograms (thanks to NULL-buckets).
 *
 * So for example with five WHERE conditions
 *
 *     WHERE (a = 1) AND (b = 1) AND (c = 1) AND (d = 1) AND (e = 1)
 *
 * and statistics on (a,b), (a,b,e) and (a,b,c,d), the last one will be
 * selected as it references the most columns.
 *
 * Once we have selected the multivariate statistics, we split the list
 * of clauses into two parts - conditions that are compatible with the
 * selected stats, and conditions are estimated using simple statistics.
 *
 * From the example above, conditions
 *
 *     (a = 1) AND (b = 1) AND (c = 1) AND (d = 1)
 *
 * will be estimated using the multivariate statistics (a,b,c,d) while
 * the last condition (e = 1) will get estimated using the regular ones.
 *
 * There are various alternative selection criteria (e.g. counting
 * conditions instead of just referenced attributes), but eventually
 * the best option should be to combine multiple statistics. But that's
 * much harder to do correctly.
 *
 * TODO Select multiple statistics and combine them when computing
 *      the estimate.
 *
 * TODO This will probably have to consider compatibility of clauses,
 *      because 'dependencies' will probably work only with equality
 *      clauses.
 */
static MVStatisticInfo *
choose_mv_statistics(List *stats, Bitmapset *attnums)
{
	int i;
	ListCell   *lc;

	MVStatisticInfo *choice = NULL;

	int current_matches = 1;						/* goal #1: maximize */
	int current_dims = (MVSTATS_MAX_DIMENSIONS+1);	/* goal #2: minimize */

	/*
	 * Walk through the statistics (simple array with nmvstats elements)
	 * and for each one count the referenced attributes (encoded in
	 * the 'attnums' bitmap).
	 */
	foreach (lc, stats)
	{
		MVStatisticInfo *info = (MVStatisticInfo *)lfirst(lc);

		/* columns matching this statistics */
		int matches = 0;

		int2vector * attrs = info->stakeys;
		int	numattrs = attrs->dim1;

		/* skip dependencies-only stats */
		if (! (info->mcv_built || info->hist_built))
			continue;

		/* count columns covered by the histogram */
		for (i = 0; i < numattrs; i++)
			if (bms_is_member(attrs->values[i], attnums))
				matches++;

		/*
		 * Use this statistics when it improves the number of matches or
		 * when it matches the same number of attributes but is smaller.
		 */
		if ((matches > current_matches) ||
			((matches == current_matches) && (current_dims > numattrs)))
		{
			choice = info;
			current_matches = matches;
			current_dims = numattrs;
		}
	}

	return choice;
}


/*
 * This splits the clauses list into two parts - one containing clauses
 * that will be evaluated using the chosen statistics, and the remaining
 * clauses (either non-mvcompatible, or not related to the histogram).
 */
static List *
clauselist_mv_split(PlannerInfo *root, SpecialJoinInfo *sjinfo,
					List *clauses, Oid varRelid, List **mvclauses,
					MVStatisticInfo *mvstats, int types)
{
	int i;
	ListCell *l;
	List	 *non_mvclauses = NIL;

	/* FIXME is there a better way to get info on int2vector? */
	int2vector * attrs = mvstats->stakeys;
	int	numattrs = mvstats->stakeys->dim1;

	Bitmapset *mvattnums = NULL;

	/* build bitmap of attributes covered by the stats, so we can
	 * do bms_is_subset later */
	for (i = 0; i < numattrs; i++)
		mvattnums = bms_add_member(mvattnums, attrs->values[i]);

	/* erase the list of mv-compatible clauses */
	*mvclauses = NIL;

	foreach (l, clauses)
	{
		bool		match = false;	/* by default not mv-compatible */
		Bitmapset	*attnums = NULL;
		Node	   *clause = (Node *) lfirst(l);

		if (clause_is_mv_compatible(root, clause, varRelid, NULL,
									&attnums, sjinfo, types))
		{
			/* are all the attributes part of the selected stats? */
			if (bms_is_subset(attnums, mvattnums))
				match = true;
		}

		/*
		 * The clause matches the selected stats, so put it to the list
		 * of mv-compatible clauses. Otherwise, keep it in the list of
		 * 'regular' clauses (that may be selected later).
		 */
		if (match)
			*mvclauses = lappend(*mvclauses, clause);
		else
			non_mvclauses = lappend(non_mvclauses, clause);
	}

	/*
	 * Perform regular estimation using the clauses incompatible
	 * with the chosen histogram (or MV stats in general).
	 */
	return non_mvclauses;

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
						Index *relid, Bitmapset **attnums, SpecialJoinInfo *sjinfo,
						int types)
{
	Relids clause_relids;
	Relids left_relids;
	Relids right_relids;

	if (IsA(clause, RestrictInfo))
	{
		RestrictInfo *rinfo = (RestrictInfo *) clause;

		/* Pseudoconstants are not really interesting here. */
		if (rinfo->pseudoconstant)
			return false;

		/* get the actual clause from the RestrictInfo (it's not an OR clause) */
		clause = (Node*)rinfo->clause;

		/* we don't support join conditions at this moment */
		if (treat_as_join_clause(clause, rinfo, varRelid, sjinfo))
			return false;

		clause_relids = rinfo->clause_relids;
		left_relids = rinfo->left_relids;
		right_relids = rinfo->right_relids;
	}
	else if (is_opclause(clause) && list_length(((OpExpr *) clause)->args) == 2)
	{
		left_relids = pull_varnos(get_leftop((Expr*)clause));
		right_relids = pull_varnos(get_rightop((Expr*)clause));

		clause_relids = bms_union(left_relids,
								  right_relids);
	}
	else
	{
		/* Not a binary opclause, so mark left/right relid sets as empty */
		left_relids = NULL;
		right_relids = NULL;
		/* and get the total relid set the hard way */
		clause_relids = pull_varnos((Node *) clause);
	}

	/*
	 * Only simple opclauses and IS NULL tests are compatible with
	 * multivariate stats at this point.
	 */
	if ((is_opclause(clause))
		&& (list_length(((OpExpr *) clause)->args) == 2))
	{
		OpExpr	   *expr = (OpExpr *) clause;
		bool		varonleft = true;
		bool		ok;

		/* is it 'variable op constant' ? */
		ok = (bms_membership(clause_relids) == BMS_SINGLETON) &&
			(is_pseudo_constant_clause_relids(lsecond(expr->args),
											  right_relids) ||
			(varonleft = false,
			is_pseudo_constant_clause_relids(linitial(expr->args),
											 left_relids)));

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

			/* Lookup info about the base relation (we need to pass the OID out) */
			if (relid != NULL)
				*relid = var->varno;

			/*
			 * If it's not a "<" or ">" or "=" operator, just ignore the
			 * clause. Otherwise note the relid and attnum for the variable.
			 * This uses the function for estimating selectivity, ont the
			 * operator directly (a bit awkward, but well ...).
			 */
			switch (get_oprrest(expr->opno))
				{
					case F_SCALARLTSEL:
					case F_SCALARGTSEL:
						/* not compatible with functional dependencies */
						if (types & (MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST))
						{
							*attnums = bms_add_member(*attnums, var->varattno);
							return (types & (MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST));
						}
						return false;

					case F_EQSEL:
						*attnums = bms_add_member(*attnums, var->varattno);
						return true;
				}
		}
	}
	else if (IsA(clause, NullTest)
			 && IsA(((NullTest*)clause)->arg, Var))
	{
		Var * var = (Var*)((NullTest*)clause)->arg;

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

		/* Lookup info about the base relation (we need to pass the OID out) */
		if (relid != NULL)
				*relid = var->varno;

		*attnums = bms_add_member(*attnums, var->varattno);

		return true;
	}
	else if (or_clause(clause) || and_clause(clause))
	{
		/*
		 * AND/OR-clauses are supported if all sub-clauses are supported
		 *
		 * TODO We might support mixed case, where some of the clauses
		 *      are supported and some are not, and treat all supported
		 *      subclauses as a single clause, compute it's selectivity
		 *      using mv stats, and compute the total selectivity using
		 *      the current algorithm.
		 *
		 * TODO For RestrictInfo above an OR-clause, we might use the
		 *      orclause with nested RestrictInfo - we won't have to
		 *      call pull_varnos() for each clause, saving time. 
		 */
		Bitmapset *tmp = NULL;
		ListCell *l;
		foreach (l, ((BoolExpr*)clause)->args)
		{
			if (! clause_is_mv_compatible(root, (Node*)lfirst(l),
						varRelid, relid, &tmp, sjinfo, types))
				return false;
		}

		/* add the attnums from the OR-clause to the set of attnums */
		*attnums = bms_join(*attnums, tmp);

		return true;
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

		if ((type & MV_CLAUSE_TYPE_MCV) && stat->mcv_built)
			return true;

		if ((type & MV_CLAUSE_TYPE_HIST) && stat->hist_built)
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
		Bitmapset *attnums = NULL;
		Node	   *clause = (Node *) lfirst(lc);

		if (! clause_is_mv_compatible(root, clause, varRelid, relid, &attnums,
									  sjinfo, MV_CLAUSE_TYPE_FDEP))

			/* clause incompatible with functional dependencies */
			*reduced_clauses = lappend(*reduced_clauses, clause);

		else if (bms_num_members(attnums) > 1)

			/*
			 * clause referencing multiple attributes (strange, should
			 * this be handled by clause_is_mv_compatible directly)
			 */
			*reduced_clauses = lappend(*reduced_clauses, clause);

		else if (! bms_is_member(bms_singleton_member(attnums), deps_attnums))

			/* clause not covered by the dependencies */
			*reduced_clauses = lappend(*reduced_clauses, clause);

		else
		{
			/* ok, clause compatible with existing dependencies */
			Assert(bms_num_members(attnums) == 1);

			*deps_clauses   = lappend(*deps_clauses, clause);
			clause_attnums = bms_add_member(clause_attnums,
										bms_singleton_member(attnums));
		}

		bms_free(attnums);
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

/*
 * Estimate selectivity of clauses using a MCV list.
 *
 * If there's no MCV list for the stats, the function returns 0.0.
 *
 * While computing the estimate, the function checks whether all the
 * columns were matched with an equality condition. If that's the case,
 * we can skip processing the histogram, as there can be no rows in
 * it with the same values - all the rows matching the condition are
 * represented by the MCV item. This can only happen with equality
 * on all the attributes.
 *
 * The algorithm works like this:
 *
 *   1) mark all items as 'match'
 *   2) walk through all the clauses
 *   3) for a particular clause, walk through all the items
 *   4) skip items that are already 'no match'
 *   5) check clause for items that still match
 *   6) sum frequencies for items to get selectivity
 *
 * The function also returns the frequency of the least frequent item
 * on the MCV list, which may be useful for clamping estimate from the
 * histogram (all items not present in the MCV list are less frequent).
 * This however seems useful only for cases with conditions on all
 * attributes.
 *
 * TODO This only handles AND-ed clauses, but it might work for OR-ed
 *      lists too - it just needs to reverse the logic a bit. I.e. start
 *      with 'no match' for all items, and mark the items as a match
 *      as the clauses are processed (and skip items that are 'match').
 */
static Selectivity
clauselist_mv_selectivity_mcvlist(PlannerInfo *root, List *clauses,
								  MVStatisticInfo *mvstats, bool *fullmatch,
								  Selectivity *lowsel)
{
	int i;
	Selectivity s = 0.0;
	Selectivity u = 0.0;

	MCVList mcvlist = NULL;
	int	nmatches = 0;

	/* match/mismatch bitmap for each MCV item */
	char * matches = NULL;

	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 2);

	/* there's no MCV list built yet */
	if (! mvstats->mcv_built)
		return 0.0;

	mcvlist = load_mv_mcvlist(mvstats->mvoid);

	Assert(mcvlist != NULL);
	Assert(mcvlist->nitems > 0);

	/* by default all the MCV items match the clauses fully */
	matches = palloc0(sizeof(char) * mcvlist->nitems);
	memset(matches, MVSTATS_MATCH_FULL, sizeof(char)*mcvlist->nitems);

	/* number of matching MCV items */
	nmatches = mcvlist->nitems;

	nmatches = update_match_bitmap_mcvlist(root, clauses,
										   mvstats->stakeys, mcvlist,
										   nmatches, matches,
										   lowsel, fullmatch, false);

	/* sum frequencies for all the matching MCV items */
	for (i = 0; i < mcvlist->nitems; i++)
	{
		/* used to 'scale' for MCV lists not covering all tuples */
		u += mcvlist->items[i]->frequency;

		if (matches[i] != MVSTATS_MATCH_NONE)
			s += mcvlist->items[i]->frequency;
	}

	pfree(matches);
	pfree(mcvlist);

	return s*u;
}

/*
 * Evaluate clauses using the MCV list, and update the match bitmap.
 *
 * The bitmap may be already partially set, so this is really a way to
 * combine results of several clause lists - either when computing
 * conditional probability P(A|B) or a combination of AND/OR clauses.
 *
 * TODO This works with 'bitmap' where each bit is represented as a char,
 *      which is slightly wasteful. Instead, we could use a regular
 *      bitmap, reducing the size to ~1/8. Another thing is merging the
 *      bitmaps using & and |, which might be faster than min/max.
 */
static int
update_match_bitmap_mcvlist(PlannerInfo *root, List *clauses,
						   int2vector *stakeys, MCVList mcvlist,
						   int nmatches, char * matches,
						   Selectivity *lowsel, bool *fullmatch,
						   bool is_or)
{
	int i;
	ListCell * l;

	Bitmapset *eqmatches = NULL;	/* attributes with equality matches */

	/* The bitmap may be partially built. */
	Assert(nmatches >= 0);
	Assert(nmatches <= mcvlist->nitems);
	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);
	Assert(mcvlist != NULL);
	Assert(mcvlist->nitems > 0);

	/* No possible matches (only works for AND-ded clauses) */
	if (((nmatches == 0) && (! is_or)) ||
		((nmatches == mcvlist->nitems) && is_or))
		return nmatches;

	/* frequency of the lowest MCV item */
	*lowsel = 1.0;

	/*
	 * Loop through the list of clauses, and for each of them evaluate
	 * all the MCV items not yet eliminated by the preceding clauses.
	 *
	 * FIXME This would probably deserve a refactoring, I guess. Unify
	 *       the two loops and put the checks inside, or something like
	 *       that.
	 */
	foreach (l, clauses)
	{
		Node * clause = (Node*)lfirst(l);

		/* if it's a RestrictInfo, then extract the clause */
		if (IsA(clause, RestrictInfo))
			clause = (Node*)((RestrictInfo*)clause)->clause;

		/* if there are no remaining matches possible, we can stop */
		if (((nmatches == 0) && (! is_or)) ||
			((nmatches == mcvlist->nitems) && is_or))
				break;

		/* it's either OpClause, or NullTest */
		if (is_opclause(clause))
		{
			OpExpr * expr = (OpExpr*)clause;
			bool		varonleft = true;
			bool		ok;

			/* operator */
			FmgrInfo		opproc;

			/* get procedure computing operator selectivity */
			RegProcedure	oprrest = get_oprrest(expr->opno);

			fmgr_info(get_opcode(expr->opno), &opproc);

			ok = (NumRelids(clause) == 1) &&
				 (is_pseudo_constant_clause(lsecond(expr->args)) ||
				 (varonleft = false,
				  is_pseudo_constant_clause(linitial(expr->args))));

			if (ok)
			{

				FmgrInfo	ltproc, gtproc;
				Var * var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);
				Const * cst = (varonleft) ? lsecond(expr->args) : linitial(expr->args);
				bool isgt = (! varonleft);

				/*
				 * TODO Fetch only when really needed (probably for equality only)
				 * TODO Technically either lt/gt is sufficient.
				 *
				 * FIXME The code in analyze.c creates histograms only for types
				 *       with enough ordering (by calling get_sort_group_operators).
				 *       Is this the same assumption, i.e. are we certain that we
				 *       get the ltproc/gtproc every time we ask? Or are there types
				 *       where get_sort_group_operators returns ltopr and here we
				 *       get nothing?
				 */
				TypeCacheEntry *typecache
					= lookup_type_cache(var->vartype,
										TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

				/* FIXME proper matching attribute to dimension */
				int idx = mv_get_index(var->varattno, stakeys);

				fmgr_info(get_opcode(typecache->lt_opr), &ltproc);
				fmgr_info(get_opcode(typecache->gt_opr), &gtproc);

				/*
				 * Walk through the MCV items and evaluate the current clause. We can
				 * skip items that were already ruled out, and terminate if there are
				 * no remaining MCV items that might possibly match.
				 */
				for (i = 0; i < mcvlist->nitems; i++)
				{
					bool mismatch = false;
					MCVItem item = mcvlist->items[i];

					/*
					 * find the lowest selectivity in the MCV
					 * FIXME Maybe not the best place do do this (in for all clauses).
					 */
					if (item->frequency < *lowsel)
						*lowsel = item->frequency;

					/*
					 * If there are no more matches (AND) or no remaining unmatched
					 * items (OR), we can stop processing this clause.
					 */
					if (((nmatches == 0) && (! is_or)) ||
						((nmatches == mcvlist->nitems) && is_or))
						break;

					/*
					 * For AND-lists, we can also mark NULL items as 'no match' (and
					 * then skip them). For OR-lists this is not possible.
					 */
					if ((! is_or) && item->isnull[idx])
						matches[i] = MVSTATS_MATCH_NONE;

					/* skip MCV items that were already ruled out */
					if ((! is_or) && (matches[i] == MVSTATS_MATCH_NONE))
						continue;
					else if (is_or && (matches[i] == MVSTATS_MATCH_FULL))
						continue;

					/* TODO consider bsearch here (list is sorted by values)
					 * TODO handle other operators too (LT, GT)
					 * TODO identify "full match" when the clauses fully
					 *      match the whole MCV list (so that checking the
					 *      histogram is not needed)
					 */
					if (oprrest == F_EQSEL)
					{
						/*
						 * We don't care about isgt in equality, because it does not
						 * matter whether it's (var = const) or (const = var).
						 */
						bool match = DatumGetBool(FunctionCall2Coll(&opproc,
															 DEFAULT_COLLATION_OID,
															 cst->constvalue,
															 item->values[idx]));

						if (match)
							eqmatches = bms_add_member(eqmatches, idx);

						mismatch = (! match);
					}
					else if (oprrest == F_SCALARLTSEL)	/* column < constant */
					{

						if (! isgt)	/* (var < const) */
						{
							/*
							 * First check whether the constant is below the lower boundary (in that
							 * case we can skip the bucket, because there's no overlap).
							 */
							mismatch = DatumGetBool(FunctionCall2Coll(&opproc,
																 DEFAULT_COLLATION_OID,
																 cst->constvalue,
																 item->values[idx]));

						} /* (get_oprrest(expr->opno) == F_SCALARLTSEL) */
						else	/* (const < var) */
						{
							/*
							 * First check whether the constant is above the upper boundary (in that
							 * case we can skip the bucket, because there's no overlap).
							 */
							mismatch = DatumGetBool(FunctionCall2Coll(&opproc,
																 DEFAULT_COLLATION_OID,
																 item->values[idx],
																 cst->constvalue));
						}
					}
					else if (oprrest == F_SCALARGTSEL)	/* column > constant */
					{

						if (! isgt)	/* (var > const) */
						{
							/*
							 * First check whether the constant is above the upper boundary (in that
							 * case we can skip the bucket, because there's no overlap).
							 */
							mismatch = DatumGetBool(FunctionCall2Coll(&opproc,
																 DEFAULT_COLLATION_OID,
																 cst->constvalue,
																 item->values[idx]));
						}
						else /* (const > var) */
						{
							/*
							 * First check whether the constant is below the lower boundary (in
							 * that case we can skip the bucket, because there's no overlap).
							 */
							mismatch = DatumGetBool(FunctionCall2Coll(&opproc,
																 DEFAULT_COLLATION_OID,
																 item->values[idx],
																 cst->constvalue));
						}

					} /* (get_oprrest(expr->opno) == F_SCALARGTSEL) */

					/* XXX The conditions on matches[i] are not needed, as we
					 *     skip MCV items that can't become true/false, depending
					 *     on the current flag. See beginning of the loop over
					 *     MCV items.
					 */

					if ((is_or) && (matches[i] == MVSTATS_MATCH_NONE) && (! mismatch))
					{
						/* OR - was MATCH_NONE, but will be MATCH_FULL */
						matches[i] = MVSTATS_MATCH_FULL;
						++nmatches;
						continue;
					}
					else if ((! is_or) && (matches[i] == MVSTATS_MATCH_FULL) && mismatch)
					{
						/* AND - was MATC_FULL, but will be MATCH_NONE */
						matches[i] = MVSTATS_MATCH_NONE;
						--nmatches;
						continue;
					}

				}
			}
		}
		else if (IsA(clause, NullTest))
		{
			NullTest * expr = (NullTest*)clause;
			Var * var = (Var*)(expr->arg);

			/* FIXME proper matching attribute to dimension */
			int idx = mv_get_index(var->varattno, stakeys);

			/*
			 * Walk through the MCV items and evaluate the current clause. We can
			 * skip items that were already ruled out, and terminate if there are
			 * no remaining MCV items that might possibly match.
			 */
			for (i = 0; i < mcvlist->nitems; i++)
			{
				MCVItem item = mcvlist->items[i];

				/*
				 * find the lowest selectivity in the MCV
				 * FIXME Maybe not the best place do do this (in for all clauses).
				 */
				if (item->frequency < *lowsel)
					*lowsel = item->frequency;

				/* if there are no more matches, we can stop processing this clause */
				if (nmatches == 0)
					break;

				/* skip MCV items that were already ruled out */
				if (matches[i] == MVSTATS_MATCH_NONE)
					continue;

				/* if the clause mismatches the MCV item, set it as MATCH_NONE */
				if (((expr->nulltesttype == IS_NULL) && (! mcvlist->items[i]->isnull[idx])) ||
					((expr->nulltesttype == IS_NOT_NULL) && (mcvlist->items[i]->isnull[idx])))
				{
						matches[i] = MVSTATS_MATCH_NONE;
						--nmatches;
				}
			}
		}
		else if (or_clause(clause) || and_clause(clause))
		{
			/* AND/OR clause, with all clauses compatible with the selected MV stat */

			int			i;
			BoolExpr   *orclause  = ((BoolExpr*)clause);
			List	   *orclauses = orclause->args;

			/* match/mismatch bitmap for each MCV item */
			int	or_nmatches = 0;
			char * or_matches = NULL;

			Assert(orclauses != NIL);
			Assert(list_length(orclauses) >= 2);

			/* number of matching MCV items */
			or_nmatches = mcvlist->nitems;

			/* by default none of the MCV items matches the clauses */
			or_matches = palloc0(sizeof(char) * or_nmatches);

			if (or_clause(clause))
			{
				/* OR clauses assume nothing matches, initially */
				memset(or_matches, MVSTATS_MATCH_NONE, sizeof(char)*or_nmatches);
				or_nmatches = 0;
			}
			else
			{
				/* AND clauses assume nothing matches, initially */
				memset(or_matches, MVSTATS_MATCH_FULL, sizeof(char)*or_nmatches);
			}

			/* build the match bitmap for the OR-clauses */
			or_nmatches = update_match_bitmap_mcvlist(root, orclauses,
									   stakeys, mcvlist,
									   or_nmatches, or_matches,
									   lowsel, fullmatch, or_clause(clause));

			/* merge the bitmap into the existing one*/
			for (i = 0; i < mcvlist->nitems; i++)
			{
				/*
				 * To AND-merge the bitmaps, a MIN() semantics is used.
				 * For OR-merge, use MAX().
				 *
				 * FIXME this does not decrease the number of matches
				 */
				UPDATE_RESULT(matches[i], or_matches[i], is_or);
			}

			pfree(or_matches);

		}
		else
		{
			elog(ERROR, "unknown clause type: %d", clause->type);
		}
	}

	/*
	 * If all the columns were matched by equality, it's a full match.
	 * In this case there can be just a single MCV item, matching the
	 * clause (if there were two, both would match the other one).
	 */
	*fullmatch = (bms_num_members(eqmatches) == mcvlist->ndimensions);

	/* free the allocated pieces */
	if (eqmatches)
		pfree(eqmatches);

	return nmatches;
}

/*
 * Estimate selectivity of clauses using a histogram.
 *
 * If there's no histogram for the stats, the function returns 0.0.
 *
 * The general idea of this method is similar to how MCV lists are
 * processed, except that this introduces the concept of a partial
 * match (MCV only works with full match / mismatch).
 *
 * The algorithm works like this:
 *
 *   1) mark all buckets as 'full match'
 *   2) walk through all the clauses
 *   3) for a particular clause, walk through all the buckets
 *   4) skip buckets that are already 'no match'
 *   5) check clause for buckets that still match (at least partially)
 *   6) sum frequencies for buckets to get selectivity
 *
 * Unlike MCV lists, histograms have a concept of a partial match. In
 * that case we use 1/2 the bucket, to minimize the average error. The
 * MV histograms are usually less detailed than the per-column ones,
 * meaning the sum is often quite high (thanks to combining a lot of
 * "partially hit" buckets).
 *
 * Maybe we could use per-bucket information with number of distinct
 * values it contains (for each dimension), and then use that to correct
 * the estimate (so with 10 distinct values, we'd use 1/10 of the bucket
 * frequency). We might also scale the value depending on the actual
 * ndistinct estimate (not just the values observed in the sample).
 *
 * Another option would be to multiply the selectivities, i.e. if we get
 * 'partial match' for a bucket for multiple conditions, we might use
 * 0.5^k (where k is the number of conditions), instead of 0.5. This
 * probably does not minimize the average error, though.
 *
 * TODO This might use a similar shortcut to MCV lists - count buckets
 *      marked as partial/full match, and terminate once this drop to 0.
 *      Not sure if it's really worth it - for MCV lists a situation like
 *      this is not uncommon, but for histograms it's not that clear.
 */
static Selectivity
clauselist_mv_selectivity_histogram(PlannerInfo *root, List *clauses,
									MVStatisticInfo *mvstats)
{
	int i;
	Selectivity s = 0.0;
	Selectivity u = 0.0;

	int		nmatches = 0;
	char   *matches = NULL;

	MVSerializedHistogram mvhist = NULL;

	/* there's no histogram */
	if (! mvstats->hist_built)
		return 0.0;

	/* There may be no histogram in the stats (check hist_built flag) */
	mvhist = load_mv_histogram(mvstats->mvoid);

	Assert (mvhist != NULL);
	Assert (clauses != NIL);
	Assert (list_length(clauses) >= 2);

	/*
	 * Bitmap of bucket matches (mismatch, partial, full). by default
	 * all buckets fully match (and we'll eliminate them).
	 */
	matches = palloc0(sizeof(char) * mvhist->nbuckets);
	memset(matches,  MVSTATS_MATCH_FULL, sizeof(char)*mvhist->nbuckets);

	nmatches = mvhist->nbuckets;

	/* build the match bitmap */
	update_match_bitmap_histogram(root, clauses,
								  mvstats->stakeys, mvhist,
								  nmatches, matches, false);

	/* now, walk through the buckets and sum the selectivities */
	for (i = 0; i < mvhist->nbuckets; i++)
	{
		/*
		 * Find out what part of the data is covered by the histogram,
		 * so that we can 'scale' the selectivity properly (e.g. when
		 * only 50% of the sample got into the histogram, and the rest
		 * is in a MCV list).
		 *
		 * TODO This might be handled by keeping a global "frequency"
		 *      for the whole histogram, which might save us some time
		 *      spent accessing the not-matching part of the histogram.
		 *      Although it's likely in a cache, so it's very fast.
		 */
		u += mvhist->buckets[i]->ntuples;

		if (matches[i] == MVSTATS_MATCH_FULL)
			s += mvhist->buckets[i]->ntuples;
		else if (matches[i] == MVSTATS_MATCH_PARTIAL)
			s += 0.5 * mvhist->buckets[i]->ntuples;
	}

	/* release the allocated bitmap and deserialized histogram */
	pfree(matches);
	pfree(mvhist);

	return s * u;
}

/*
 * Evaluate clauses using the histogram, and update the match bitmap.
 *
 * The bitmap may be already partially set, so this is really a way to
 * combine results of several clause lists - either when computing
 * conditional probability P(A|B) or a combination of AND/OR clauses.
 *
 * Note: This is not a simple bitmap in the sense that there are more
 *       than two possible values for each item - no match, partial
 *       match and full match. So we need 2 bits per item.
 *
 * TODO This works with 'bitmap' where each item is represented as a
 *      char, which is slightly wasteful. Instead, we could use a bitmap
 *      with 2 bits per item, reducing the size to ~1/4. By using values
 *      0, 1 and 3 (instead of 0, 1 and 2), the operations (merging etc.)
 *      might be performed just like for simple bitmap by using & and |,
 *      which might be faster than min/max.
 */
static int
update_match_bitmap_histogram(PlannerInfo *root, List *clauses,
							  int2vector *stakeys,
							  MVSerializedHistogram mvhist,
							  int nmatches, char * matches,
							  bool is_or)
{
	int i;
	ListCell * l;

	/*
	 * Used for caching function calls, only once per deduplicated value.
	 *
	 * We know may have up to (2 * nbuckets) values per dimension. It's
	 * probably overkill, but let's allocate that once for all clauses,
	 * to minimize overhead.
	 *
	 * Also, we only need two bits per value, but this allocates byte
	 * per value. Might be worth optimizing.
	 *
	 * 0x00 - not yet called
	 * 0x01 - called, result is 'false'
	 * 0x03 - called, result is 'true'
	 */
	char *callcache = palloc(mvhist->nbuckets);

	Assert(mvhist != NULL);
	Assert(mvhist->nbuckets > 0);
	Assert(nmatches >= 0);
	Assert(nmatches <= mvhist->nbuckets);

	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);

	/* loop through the clauses and do the estimation */
	foreach (l, clauses)
	{
		Node * clause = (Node*)lfirst(l);

		/* if it's a RestrictInfo, then extract the clause */
		if (IsA(clause, RestrictInfo))
			clause = (Node*)((RestrictInfo*)clause)->clause;

		/* it's either OpClause, or NullTest */
		if (is_opclause(clause))
		{
			OpExpr * expr = (OpExpr*)clause;
			bool		varonleft = true;
			bool		ok;

			FmgrInfo	opproc;			/* operator */
			fmgr_info(get_opcode(expr->opno), &opproc);

			/* reset the cache (per clause) */
			memset(callcache, 0, mvhist->nbuckets);

			ok = (NumRelids(clause) == 1) &&
				 (is_pseudo_constant_clause(lsecond(expr->args)) ||
				 (varonleft = false,
				  is_pseudo_constant_clause(linitial(expr->args))));

			if (ok)
			{
				FmgrInfo	ltproc;
				RegProcedure	oprrest = get_oprrest(expr->opno);

				Var * var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);
				Const * cst = (varonleft) ? lsecond(expr->args) : linitial(expr->args);
				bool isgt = (! varonleft);

				/*
				 * TODO Fetch only when really needed (probably for equality only)
				 *
				 * TODO Technically either lt/gt is sufficient.
				 *
				 * FIXME The code in analyze.c creates histograms only for types
				 *       with enough ordering (by calling get_sort_group_operators).
				 *       Is this the same assumption, i.e. are we certain that we
				 *       get the ltproc/gtproc every time we ask? Or are there types
				 *       where get_sort_group_operators returns ltopr and here we
				 *       get nothing?
				 */
				TypeCacheEntry *typecache
					= lookup_type_cache(var->vartype, TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR
																	   | TYPECACHE_GT_OPR);

				/* lookup dimension for the attribute */
				int idx = mv_get_index(var->varattno, stakeys);

				fmgr_info(get_opcode(typecache->lt_opr), &ltproc);

				/*
				 * Check this for all buckets that still have "true" in the bitmap
				 *
				 * We already know the clauses use suitable operators (because that's
				 * how we filtered them).
				 */
				for (i = 0; i < mvhist->nbuckets; i++)
				{
					bool tmp;
					MVSerializedBucket bucket = mvhist->buckets[i];

					/* histogram boundaries */
					Datum minval, maxval;

					/* values from the call cache */
					char mincached, maxcached;

					/*
					 * For AND-lists, we can also mark NULL buckets as 'no match'
					 * (and then skip them). For OR-lists this is not possible.
					 */
					if ((! is_or) && bucket->nullsonly[idx])
						matches[i] = MVSTATS_MATCH_NONE;

					/*
					 * Skip buckets that were already eliminated - this is impotant
					 * considering how we update the info (we only lower the match).
					 * We can't really do anything about the MATCH_PARTIAL buckets.
					 */
					if ((! is_or) && (matches[i] == MVSTATS_MATCH_NONE))
						continue;
					else if (is_or && (matches[i] == MVSTATS_MATCH_FULL))
						continue;

					/* lookup the values and cache of function calls */
					minval = mvhist->values[idx][bucket->min[idx]];
					maxval = mvhist->values[idx][bucket->max[idx]];

					mincached = callcache[bucket->min[idx]];
					maxcached = callcache[bucket->max[idx]];

					/*
					 * TODO Maybe it's possible to add here a similar optimization
					 *      as for the MCV lists:
					 * 
					 *      (nmatches == 0) && AND-list => all eliminated (FALSE)
					 *      (nmatches == N) && OR-list  => all eliminated (TRUE)
					 *
					 *      But it's more complex because of the partial matches.
					 */

					/*
					* If it's not a "<" or ">" or "=" operator, just ignore the
					* clause. Otherwise note the relid and attnum for the variable.
					*
					* TODO I'm really unsure the handling of 'isgt' flag (that is, clauses
					*      with reverse order of variable/constant) is correct. I wouldn't
					*      be surprised if there was some mixup. Using the lt/gt operators
					*      instead of messing with the opproc could make it simpler.
					*      It would however be using a different operator than the query,
					*      although it's not any shadier than using the selectivity function
					*      as is done currently.
					*
					* FIXME Once the min/max values are deduplicated, we can easily minimize
					*       the number of calls to the comparator (assuming we keep the
					*       deduplicated structure). See the note on compression at MVBucket
					*       serialize/deserialize methods.
					*/
					switch (oprrest)
					{
						case F_SCALARLTSEL:	/* column < constant */

							if (! isgt)	/* (var < const) */
							{
								/*
								 * First check whether the constant is below the lower boundary (in that
								 * case we can skip the bucket, because there's no overlap).
								 */
								if (! mincached)
								{
									tmp = DatumGetBool(FunctionCall2Coll(&opproc,
																		 DEFAULT_COLLATION_OID,
																		 cst->constvalue,
																		 minval));

									/*
									 * Update the cache, but with the inverse value, as we keep the
									 * cache for calls with (minval, constvalue).
									 */
									callcache[bucket->min[idx]] = (tmp) ? 0x01 : 0x03;
								}
								else
									tmp = !(mincached & 0x02);	/* get call result from the cache (inverse) */

								if (tmp)
								{
									/* no match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_NONE, is_or);
									continue;
								}

								/*
								 * Now check whether the upper boundary is below the constant (in that
								 * case it's a partial match).
								 */
								if (! maxcached)
								{
									tmp = DatumGetBool(FunctionCall2Coll(&opproc,
																		 DEFAULT_COLLATION_OID,
																		 cst->constvalue,
																		 maxval));

									/*
									 * Update the cache, but with the inverse value, as we keep the
									 * cache for calls with (minval, constvalue).
									 */
									callcache[bucket->max[idx]] = (tmp) ? 0x01 : 0x03;
								}
								else
									tmp = !(maxcached & 0x02);	/* extract the result (reverse) */

								if (tmp)	/* partial match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_PARTIAL, is_or);

							}
							else	/* (const < var) */
							{
								/*
								 * First check whether the constant is above the upper boundary (in that
								 * case we can skip the bucket, because there's no overlap).
								 */
								if (! maxcached)
								{
									tmp = DatumGetBool(FunctionCall2Coll(&opproc,
																		 DEFAULT_COLLATION_OID,
																		 maxval,
																		 cst->constvalue));

									/* Update the cache. */
									callcache[bucket->max[idx]] = (tmp) ? 0x03 : 0x01;
								}
								else
									tmp = (maxcached & 0x02);	/* extract the result */

								if (tmp)
								{
									/* no match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_NONE, is_or);
									continue;
								}

								/*
								 * Now check whether the lower boundary is below the constant (in that
								 * case it's a partial match).
								 */
								if (! mincached)
								{
									tmp = DatumGetBool(FunctionCall2Coll(&opproc,
																		 DEFAULT_COLLATION_OID,
																		 minval,
																		 cst->constvalue));

									/* Update the cache. */
									callcache[bucket->min[idx]] = (tmp) ? 0x03 : 0x01;
								}
								else
									tmp = (mincached & 0x02);	/* extract the result */

								if (tmp)	/* partial match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_PARTIAL, is_or);
							}
							break;

						case F_SCALARGTSEL:	/* column > constant */

							if (! isgt)	/* (var > const) */
							{
								/*
								 * First check whether the constant is above the upper boundary (in that
								 * case we can skip the bucket, because there's no overlap).
								 */
								if (! maxcached)
								{
									tmp = DatumGetBool(FunctionCall2Coll(&opproc,
																		 DEFAULT_COLLATION_OID,
																		 cst->constvalue,
																		 maxval));

									/*
									 * Update the cache, but with the inverse value, as we keep the
									 * cache for calls with (val, constvalue).
									 */
									callcache[bucket->max[idx]] = (tmp) ? 0x01 : 0x03;
								}
								else
									tmp = !(maxcached & 0x02);	/* extract the result */

								if (tmp)
								{
									/* no match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_NONE, is_or);
									continue;
								}

								/*
								 * Now check whether the lower boundary is below the constant (in that
								 * case it's a partial match).
								 */
								if (! mincached)
								{
									tmp = DatumGetBool(FunctionCall2Coll(&opproc,
																		 DEFAULT_COLLATION_OID,
																		 cst->constvalue,
																		 minval));

									/*
									 * Update the cache, but with the inverse value, as we keep the
									 * cache for calls with (val, constvalue).
									 */
									callcache[bucket->min[idx]] = (tmp) ? 0x01 : 0x03;
								}
								else
									tmp = !(mincached & 0x02);	/* extract the result */

								if (tmp)
									/* partial match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_PARTIAL, is_or);
							}
							else /* (const > var) */
							{
								/*
								 * First check whether the constant is below the lower boundary (in
								 * that case we can skip the bucket, because there's no overlap).
								 */
								if (! mincached)
								{
									tmp = DatumGetBool(FunctionCall2Coll(&opproc,
																		 DEFAULT_COLLATION_OID,
																		 minval,
																		 cst->constvalue));

									/* Update the cache. */
									callcache[bucket->min[idx]] = (tmp) ? 0x03 : 0x01;
								}
								else
									tmp = (mincached & 0x02);	/* extract the result */

								if (tmp)
								{
									/* no match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_NONE, is_or);
									continue;
								}

								/*
								 * Now check whether the upper boundary is below the constant (in that
								 * case it's a partial match).
								 */
								if (! maxcached)
								{
									tmp = DatumGetBool(FunctionCall2Coll(&opproc,
																		 DEFAULT_COLLATION_OID,
																		 maxval,
																		 cst->constvalue));

									/* Update the cache. */
									callcache[bucket->max[idx]] = (tmp) ? 0x03 : 0x01;
								}
								else
									tmp = (maxcached & 0x02);	/* extract the result */

								if (tmp)
									/* partial match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_PARTIAL, is_or);
							}
							break;

						case F_EQSEL:

							/*
							 * We only check whether the value is within the bucket, using the lt/gt
							 * operators fetched from type cache.
							 *
							 * TODO We'll use the default 50% estimate, but that's probably way off
							 *		if there are multiple distinct values. Consider tweaking this a
							 *		somehow, e.g. using only a part inversely proportional to the
							 *		estimated number of distinct values in the bucket.
							 *
							 * TODO This does not handle inclusion flags at the moment, thus counting
							 *		some buckets twice (when hitting the boundary).
							 *
							 * TODO Optimization is that if max[i] == min[i], it's effectively a MCV
							 *		item and we can count the whole bucket as a complete match (thus
							 *		using 100% bucket selectivity and not just 50%).
							 *
							 * TODO Technically some buckets may "degenerate" into single-value
							 *		buckets (not necessarily for all the dimensions) - maybe this
							 *		is better than keeping a separate MCV list (multi-dimensional).
							 *		Update: Actually, that's unlikely to be better than a separate
							 *		MCV list for two reasons - first, it requires ~2x the space
							 *		(because of storing lower/upper boundaries) and second because
							 *		the buckets are ranges - depending on the partitioning algorithm
							 *		it may not even degenerate into (min=max) bucket. For example the
							 *		the current partitioning algorithm never does that.
							 */
							if (! mincached)
							{
								tmp = DatumGetBool(FunctionCall2Coll(&ltproc,
																	 DEFAULT_COLLATION_OID,
																	 cst->constvalue,
																	 minval));

								/* Update the cache. */
								callcache[bucket->min[idx]] = (tmp) ? 0x03 : 0x01;
							}
							else
								tmp = (mincached & 0x02);	/* extract the result */

							if (tmp)
							{
								/* no match */
								UPDATE_RESULT(matches[i], MVSTATS_MATCH_NONE, is_or);
								continue;
							}

							if (! maxcached)
							{
								tmp = DatumGetBool(FunctionCall2Coll(&ltproc,
																	 DEFAULT_COLLATION_OID,
																	 maxval,
																	 cst->constvalue));

								/* Update the cache. */
								callcache[bucket->max[idx]] = (tmp) ? 0x03 : 0x01;
							}
							else
								tmp = (maxcached & 0x02);	/* extract the result */

							if (tmp)
							{
								/* no match */
								UPDATE_RESULT(matches[i], MVSTATS_MATCH_NONE, is_or);
								continue;
							}

							/* partial match */
							UPDATE_RESULT(matches[i], MVSTATS_MATCH_PARTIAL, is_or);

							break;
					}
				}
			}
		}
		else if (IsA(clause, NullTest))
		{
			NullTest * expr = (NullTest*)clause;
			Var * var = (Var*)(expr->arg);

			/* FIXME proper matching attribute to dimension */
			int idx = mv_get_index(var->varattno, stakeys);

			/*
			 * Walk through the buckets and evaluate the current clause. We can
			 * skip items that were already ruled out, and terminate if there are
			 * no remaining buckets that might possibly match.
			 */
			for (i = 0; i < mvhist->nbuckets; i++)
			{
				MVSerializedBucket bucket = mvhist->buckets[i];

				/*
				 * Skip buckets that were already eliminated - this is impotant
				 * considering how we update the info (we only lower the match)
				 */
				if ((! is_or) && (matches[i] == MVSTATS_MATCH_NONE))
					continue;
				else if (is_or && (matches[i] == MVSTATS_MATCH_FULL))
					continue;

				/* if the clause mismatches the MCV item, set it as MATCH_NONE */
				if ((expr->nulltesttype == IS_NULL)
					&& (! bucket->nullsonly[idx]))
					UPDATE_RESULT(matches[i], MVSTATS_MATCH_NONE, is_or);

				else if ((expr->nulltesttype == IS_NOT_NULL) &&
						 (bucket->nullsonly[idx]))
					UPDATE_RESULT(matches[i], MVSTATS_MATCH_NONE, is_or);
			}
		}
		else if (or_clause(clause) || and_clause(clause))
		{
			/* AND/OR clause, with all clauses compatible with the selected MV stat */

			int			i;
			BoolExpr   *orclause  = ((BoolExpr*)clause);
			List	   *orclauses = orclause->args;

			/* match/mismatch bitmap for each bucket */
			int	or_nmatches = 0;
			char * or_matches = NULL;

			Assert(orclauses != NIL);
			Assert(list_length(orclauses) >= 2);

			/* number of matching buckets */
			or_nmatches = mvhist->nbuckets;

			/* by default none of the buckets matches the clauses */
			or_matches = palloc0(sizeof(char) * or_nmatches);

			if (or_clause(clause))
			{
				/* OR clauses assume nothing matches, initially */
				memset(or_matches, MVSTATS_MATCH_NONE, sizeof(char)*or_nmatches);
				or_nmatches = 0;
			}
			else
			{
				/* AND clauses assume nothing matches, initially */
				memset(or_matches, MVSTATS_MATCH_FULL, sizeof(char)*or_nmatches);
			}

			/* build the match bitmap for the OR-clauses */
			or_nmatches = update_match_bitmap_histogram(root, orclauses,
										stakeys, mvhist,
										or_nmatches, or_matches, or_clause(clause));

			/* merge the bitmap into the existing one*/
			for (i = 0; i < mvhist->nbuckets; i++)
			{
				/*
				 * To AND-merge the bitmaps, a MIN() semantics is used.
				 * For OR-merge, use MAX().
				 *
				 * FIXME this does not decrease the number of matches
				 */
				UPDATE_RESULT(matches[i], or_matches[i], is_or);
			}

			pfree(or_matches);

		}
		else
			elog(ERROR, "unknown clause type: %d", clause->type);
	}

	/* free the call cache */
	pfree(callcache);

	return nmatches;
}
