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

#include "miscadmin.h"


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

static Selectivity clauselist_selectivity_or(PlannerInfo *root,
						  List *clauses,
						  int varRelid,
						  JoinType jointype,
						  SpecialJoinInfo *sjinfo,
						  List *conditions);

static void addRangeClause(RangeQueryClause **rqlist, Node *clause,
			   bool varonleft, bool isLTsel, Selectivity s2);

#define		MV_CLAUSE_TYPE_FDEP		0x01
#define		MV_CLAUSE_TYPE_MCV		0x02
#define		MV_CLAUSE_TYPE_HIST		0x04
#define		MV_CLAUSE_TYPE_NDIST	0x08

static bool clause_is_mv_compatible(Node *clause, Index relid, Bitmapset **attnums,
						int type);

static Bitmapset *collect_mv_attnums(List *clauses, Index relid, int type);

static int	count_mv_attnums(List *clauses, Index relid, int type);

static int	count_varnos(List *clauses, Index *relid);

static List *clauses_matching_statistic(List **clauses, MVStatisticInfo *statistic,
						   Index relid, int types, bool remove);

static List *clauselist_apply_dependencies(PlannerInfo *root, List *clauses,
							  Index relid, List *stats);

static Selectivity clauselist_mv_selectivity(PlannerInfo *root,
						  MVStatisticInfo *mvstats, List *clauses,
						  List *conditions, bool is_or);

static Selectivity clauselist_mv_selectivity_mcvlist(PlannerInfo *root,
								  MVStatisticInfo *mvstats,
								  List *clauses, List *conditions,
								  bool is_or, bool *fullmatch,
								  Selectivity *lowsel);
static Selectivity clauselist_mv_selectivity_histogram(PlannerInfo *root,
									MVStatisticInfo *mvstats,
									List *clauses, List *conditions,
									bool is_or);

static int update_match_bitmap_mcvlist(PlannerInfo *root, List *clauses,
							int2vector *stakeys, MCVList mcvlist,
							int nmatches, char *matches,
							Selectivity *lowsel, bool *fullmatch,
							bool is_or);

static int update_match_bitmap_histogram(PlannerInfo *root, List *clauses,
							  int2vector *stakeys,
							  MVSerializedHistogram mvhist,
							  int nmatches, char *matches,
							  bool is_or);

/*
 * Describes a combination of multiple statistics to cover attributes
 * referenced by the clauses. The array 'stats' (with nstats elements)
 * lists attributes (in the order as they are applied), and number of
 * clause attributes covered by this solution.
 *
 * choose_mv_statistics_exhaustive() uses this to track both the current
 * and the best solutions, while walking through the state of possible
 * combination.
 */
typedef struct mv_solution_t
{
	int			nclauses;		/* number of clauses covered */
	int			nconditions;	/* number of conditions covered */
	int			nstats;			/* number of stats applied */
	int		   *stats;			/* stats (in the apply order) */
} mv_solution_t;

static List *choose_mv_statistics(PlannerInfo *root, Index relid,
					 List *mvstats, List *clauses, List *conditions);

static bool has_stats(List *stats, int type);

static List *find_stats(PlannerInfo *root, Index relid);

static bool stats_type_matches(MVStatisticInfo *stat, int type);

int			mvstat_search_type = MVSTAT_SEARCH_GREEDY;

#define UPDATE_RESULT(m,r,isor) \
	(m) = (isor) ? (Max(m,r)) : (Min(m,r))


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
 * The first thing we try to do is applying multivariate statistics, in a way
 * that intends to minimize the overhead when there are no multivariate stats
 * on the relation. Thus we do several simple (and inexpensive) checks first,
 * to verify that suitable multivariate statistics exist.
 *
 * If we identify such multivariate statistics apply, we try to apply them.
 *
 * First we try to reduce the list of clauses by applying (soft) functional
 * dependencies, and then we try to estimate the selectivity of the reduced
 * list of clauses using the multivariate MCV list and histograms.
 *
 * Finally we remove the portion of clauses estimated using multivariate stats,
 * and process the rest of the clauses using the regular per-column stats.
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
 */
Selectivity
clauselist_selectivity(PlannerInfo *root,
					   List *clauses,
					   int varRelid,
					   JoinType jointype,
					   SpecialJoinInfo *sjinfo,
					   List *conditions)
{
	Selectivity s1 = 1.0;
	RangeQueryClause *rqlist = NULL;
	ListCell   *l;

	/* processing mv stats */
	Index		relid = InvalidOid;

	/* list of multivariate stats on the relation */
	List	   *stats = NIL;

	/*
	 * To fetch the statistics, we first need to determine the rel. Currently
	 * point we only support estimates of simple restrictions with all Vars
	 * referencing a single baserel. However set_baserel_size_estimates() sets
	 * varRelid=0 so we have to actually inspect the clauses by pull_varnos
	 * and see if there's just a single varno referenced.
	 */
	if ((count_varnos(clauses, &relid) == 1) && ((varRelid == 0) || (varRelid == relid)))
		stats = find_stats(root, relid);

	/*
	 * If there's exactly one clause, then no use in trying to match up pairs,
	 * or matching multivariate statistics, so just go directly to
	 * clause_selectivity().
	 */
	if (list_length(clauses) == 1)
		return clause_selectivity(root, (Node *) linitial(clauses),
								  varRelid, jointype, sjinfo, conditions);

	/*
	 * Apply functional dependencies, but first check that there are some
	 * stats with functional dependencies built (by simply walking the stats
	 * list), and that there are at two or more attributes referenced by
	 * clauses that may be reduced using functional dependencies.
	 *
	 * We would find that anyway when trying to actually apply the functional
	 * dependencies, but let's do the cheap checks first.
	 *
	 * After applying the functional dependencies we get the remainig clauses
	 * that need to be estimated by other types of stats (MCV, histograms
	 * etc).
	 */
	if (has_stats(stats, MV_CLAUSE_TYPE_FDEP) &&
		(count_mv_attnums(clauses, relid, MV_CLAUSE_TYPE_FDEP) >= 2))
	{
		clauses = clauselist_apply_dependencies(root, clauses, relid, stats);
	}

	/*
	 * Check that there are statistics with MCV list or histogram, and also
	 * the number of attributes covered by these types of statistics.
	 *
	 * If there are no such stats or not enough attributes, don't waste time
	 * with the multivariate code and simply skip to estimation using the
	 * regular per-column stats.
	 */
	if (has_stats(stats, MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST) &&
		(count_mv_attnums(clauses, relid,
						  MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST) >= 2))
	{
		ListCell   *s;

		/*
		 * Copy the conditions we got from the upper part of the expression
		 * tree so that we can add local conditions to it (we need to keep the
		 * original list intact, for sibling expressions - other expressions
		 * at the same level).
		 */
		List	   *conditions_local = list_copy(conditions);

		/* find the best combination of statistics */
		List	   *solution = choose_mv_statistics(root, relid, stats,
													clauses, conditions);

		/* FIXME we must not scribble over the original list */
		if (solution)
			clauses = list_copy(clauses);

		/*
		 * We have a good solution, which is merely a list of statistics that
		 * we need to apply. We'll apply the statistics one by one (in the
		 * order as they appear in the list), and for each statistic we'll
		 *
		 * (1) find clauses compatible with the statistic (and remove them
		 * from the list)
		 *
		 * (2) find local conditions compatible with the statistic
		 *
		 * (3) do the estimation P(clauses | conditions)
		 *
		 * (4) append the estimated clauses to local conditions
		 *
		 * continuously modify
		 */
		foreach(s, solution)
		{
			MVStatisticInfo *mvstat = (MVStatisticInfo *) lfirst(s);

			/* clauses compatible with the statistic we're applying right now */
			List	   *stat_clauses = NIL;
			List	   *stat_conditions = NIL;

			/*
			 * Find clauses and conditions matching the statistic - the
			 * clauses need to be removed from the list, while conditions
			 * should remain there (so that we can apply them repeatedly).
			 */
			stat_clauses
				= clauses_matching_statistic(&clauses, mvstat, relid,
									MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST,
											 true);

			stat_conditions
				= clauses_matching_statistic(&conditions_local, mvstat, relid,
									MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST,
											 false);

			/*
			 * If we got no clauses to estimate, we've done something wrong,
			 * either during the optimization, detecting compatible clause, or
			 * somewhere else.
			 *
			 * Also, we need at least two attributes in clauses and
			 * conditions.
			 */
			Assert(stat_clauses != NIL);
			Assert(count_mv_attnums(list_union(stat_clauses, stat_conditions),
					  relid, MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST) >= 2);

			/* compute the multivariate stats */
			s1 *= clauselist_mv_selectivity(root, mvstat,
											stat_clauses, stat_conditions,
											false);		/* AND */

			/*
			 * Add the new clauses to the local conditions, so that we can use
			 * them for the subsequent statistics. We only add the clauses,
			 * because the conditions are already there (or should be).
			 */
			conditions_local = list_concat(conditions_local, stat_clauses);
		}

		/* from now on, work only with the 'local' list of conditions */
		conditions = conditions_local;
	}

	/*
	 * If there's exactly one clause, then no use in trying to match up pairs,
	 * so just go directly to clause_selectivity().
	 */
	if (list_length(clauses) == 1)
		return s1 * clause_selectivity(root, (Node *) linitial(clauses),
									 varRelid, jointype, sjinfo, conditions);

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
		s2 = clause_selectivity(root, clause, varRelid, jointype, sjinfo,
								conditions);

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
 * Similar to clauselist_selectivity(), but for OR-clauses. We can't simply use
 * the same multi-statistic estimation logic for AND-clauses, at least not
 * directly, because there are a few key differences:
 *
 * - functional dependencies don't really apply to OR-clauses
 *
 * - clauselist_selectivity() is based on decomposing the selectivity into
 * a sequence of conditional probabilities (selectivities), but that can
 * be done only for AND-clauses
 *
 * We might invent a similar infrastructure for optimizing OR-clauses, doing
 * something similar to what clause_selectivity does for AND-clauses, but
 * luckily we know that each disjunctive normal form (aka OR-clause)
 *
 * (a OR b OR c)
 *
 * may be rewritten as an equivalent conjunctive normal form (aka AND-clause)
 * by using negation:
 *
 * NOT ((NOT a) AND (NOT b) AND (NOT c))
 *
 * And that's something we can pass to clauselist_selectivity and let it do
 * all the heavy lifting.
 */
static Selectivity
clauselist_selectivity_or(PlannerInfo *root,
						  List *clauses,
						  int varRelid,
						  JoinType jointype,
						  SpecialJoinInfo *sjinfo,
						  List *conditions)
{
	List	   *args = NIL;
	ListCell   *l;
	Expr	   *expr;

	/* build arguments for the AND-clause by negating args of the OR-clause */
	foreach(l, clauses)
		args = lappend(args, makeBoolExpr(NOT_EXPR, list_make1(lfirst(l)), -1));

	/* and then the actual OR-clause on the negated args */
	expr = makeBoolExpr(AND_EXPR, args, -1);

	/* instead of constructing NOT expression, just do (1.0 - s) */
	return 1.0 - clauselist_selectivity(root, list_make1(expr), varRelid,
										jointype, sjinfo, conditions);
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
				   SpecialJoinInfo *sjinfo,
				   List *conditions)
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
									  sjinfo,
									  conditions);
	}
	else if (and_clause(clause))
	{
		/* share code with clauselist_selectivity() */
		s1 = clauselist_selectivity(root,
									((BoolExpr *) clause)->args,
									varRelid,
									jointype,
									sjinfo,
									conditions);
	}
	else if (or_clause(clause))
	{
		/* just call to clauselist_selectivity_or() */
		s1 = clauselist_selectivity_or(root,
									   ((BoolExpr *) clause)->args,
									   varRelid,
									   jointype,
									   sjinfo,
									   conditions);
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
								sjinfo,
								conditions);
	}
	else if (IsA(clause, CoerceToDomain))
	{
		/* Not sure this case is needed, but it can't hurt */
		s1 = clause_selectivity(root,
								(Node *) ((CoerceToDomain *) clause)->arg,
								varRelid,
								jointype,
								sjinfo,
								conditions);
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
 * estimate selectivity of clauses using multivariate statistic
 *
 * Perform estimation of the clauses using a MCV list.
 *
 * This assumes all the clauses are compatible with the selected statistics
 * (e.g. only reference columns covered by the statistics, use supported
 * operator, etc.).
 *
 * TODO: We may support some additional conditions, most importantly those
 * matching multiple columns (e.g. "a = b" or "a < b").
 *
 * TODO: Clamp the selectivity by min of the per-clause selectivities (i.e. the
 * selectivity of the most restrictive clause), because that's the maximum
 * we can ever get from ANDed list of clauses. This may probably prevent
 * issues with hitting too many buckets and low precision histograms.
 *
 * TODO: We may remember the lowest frequency in the MCV list, and then later
 * use it as a upper boundary for the selectivity (had there been a more
 * frequent item, it'd be in the MCV list). This might improve cases with
 * low-detail histograms.
 *
 * TODO: We may also derive some additional boundaries for the selectivity from
 * the MCV list, because
 *
 * (a) if we have a "full equality condition" (one equality condition on
 * each column of the statistic) and we found a match in the MCV list,
 * then this is the final selectivity (and pretty accurate),
 *
 * (b) if we have a "full equality condition" and we haven't found a match
 * in the MCV list, then the selectivity is below the lowest frequency
 * found in the MCV list,
 *
 * TODO: When applying the clauses to the histogram/MCV list, we can do that
 * from the most selective clauses first, because that'll eliminate the
 * buckets/items sooner (so we'll be able to skip them without inspection,
 * which is more expensive). But this requires really knowing the per-clause
 * selectivities in advance, and that's not what we do now.
 */
static Selectivity
clauselist_mv_selectivity(PlannerInfo *root, MVStatisticInfo *mvstats,
						  List *clauses, List *conditions, bool is_or)
{
	bool		fullmatch = false;
	Selectivity s1 = 0.0,
				s2 = 0.0;

	/*
	 * Lowest frequency in the MCV list (may be used as an upper bound for
	 * full equality conditions that did not match any MCV item).
	 */
	Selectivity mcv_low = 0.0;

	/*
	 * TODO Evaluate simple 1D selectivities, use the smallest one as an upper
	 * bound, product as lower bound, and sort the clauses in ascending order
	 * by selectivity (to optimize the MCV/histogram evaluation).
	 */

	/* Evaluate the MCV first. */
	s1 = clauselist_mv_selectivity_mcvlist(root, mvstats,
										   clauses, conditions, is_or,
										   &fullmatch, &mcv_low);

	/*
	 * If we got a full equality match on the MCV list, we're done (and the
	 * estimate is pretty good).
	 */
	if (fullmatch && (s1 > 0.0))
		return s1;

	/*
	 * TODO if (fullmatch) without matching MCV item, use the mcv_low
	 * selectivity as upper bound
	 */

	s2 = clauselist_mv_selectivity_histogram(root, mvstats,
											 clauses, conditions, is_or);

	/* TODO clamp to <= 1.0 (or more strictly, when possible) */
	return s1 + s2;
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
get_varattnos(Node *node, Index relid)
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
 * Collect attributes from mv-compatible clauses.
 */
static Bitmapset *
collect_mv_attnums(List *clauses, Index relid, int types)
{
	Bitmapset  *attnums = NULL;
	ListCell   *l;

	/*
	 * Walk through the clauses and identify the ones we can estimate using
	 * multivariate stats, and remember the relid/columns. We'll then
	 * cross-check if we have suitable stats, and only if needed we'll split
	 * the clauses into multivariate and regular lists.
	 *
	 * For now we're only interested in RestrictInfo nodes with nested OpExpr,
	 * using either a range or equality.
	 */
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);

		/* ignore the result here - we only need the attnums */
		clause_is_mv_compatible(clause, relid, &attnums, types);
	}

	/*
	 * If there are not at least two attributes referenced by the clause(s),
	 * we can throw everything out (as we'll revert to simple stats).
	 */
	if (bms_num_members(attnums) <= 1)
	{
		bms_free(attnums);
		attnums = NULL;
	}

	return attnums;
}

/*
 * Count the number of attributes in clauses compatible with multivariate stats.
 */
static int
count_mv_attnums(List *clauses, Index relid, int type)
{
	int			c;
	Bitmapset  *attnums = collect_mv_attnums(clauses, relid, type);

	c = bms_num_members(attnums);

	bms_free(attnums);

	return c;
}

/*
 * Count varnos referenced in the clauses, and if there's a single varno then
 * return the index in 'relid'.
 */
static int
count_varnos(List *clauses, Index *relid)
{
	int			cnt;
	Bitmapset  *varnos = NULL;

	varnos = pull_varnos((Node *) clauses);
	cnt = bms_num_members(varnos);

	/* if there's a single varno in the clauses, remember it */
	if (bms_num_members(varnos) == 1)
		*relid = bms_singleton_member(varnos);

	bms_free(varnos);

	return cnt;
}

static List *
clauses_matching_statistic(List **clauses, MVStatisticInfo *statistic,
						   Index relid, int types, bool remove)
{
	int			i;
	Bitmapset  *stat_attnums = NULL;
	List	   *matching_clauses = NIL;
	ListCell   *lc;

	/* build attnum bitmapset for this statistics */
	for (i = 0; i < statistic->stakeys->dim1; i++)
		stat_attnums = bms_add_member(stat_attnums,
									  statistic->stakeys->values[i]);

	/*
	 * We can't use foreach here, because we may need to remove some of the
	 * clauses if (remove=true).
	 */
	lc = list_head(*clauses);
	while (lc)
	{
		Node	   *clause = (Node *) lfirst(lc);
		Bitmapset  *attnums = NULL;

		/* must advance lc before list_delete possibly pfree's it */
		lc = lnext(lc);

		/*
		 * skip clauses that are not compatible with stats (just leave them in
		 * the original list)
		 *
		 * XXX Perhaps this should check what stats are actually available in
		 * the statistics (not a big deal now, because MCV and histograms
		 * handle the same types of conditions).
		 */
		if (!clause_is_mv_compatible(clause, relid, &attnums, types))
		{
			bms_free(attnums);
			continue;
		}

		/* if the clause is covered by the statistic, add it to the list */
		if (bms_is_subset(attnums, stat_attnums))
		{
			matching_clauses = lappend(matching_clauses, clause);

			/* if remove=true, remove the matching item from the main list */
			if (remove)
				*clauses = list_delete_ptr(*clauses, clause);
		}

		bms_free(attnums);
	}

	bms_free(stat_attnums);

	return matching_clauses;
}

/*
 * Selects the best combination of multivariate statistics, in an exhaustive
 * way, where 'best' means:
 *
 * (a) covering the most attributes (referenced by clauses)
 * (b) using the least number of multivariate stats
 * (c) using the most conditions to exploit dependency
 *
 * Don't call this directly but through choose_mv_statistics(), which does some
 * additional tricks to minimize the runtime.
 *
 *
 * Algorithm
 * ---------
 * The algorithm is a recursive implementation of backtracking, with maximum
 * depth equal to the number of multi-variate statistics available on the table.
 * It actually explores all valid combinations of stats.
 *
 * Whenever it considers adding the next statistics, the clauses it matches are
 * divided into 'conditions' (clauses already matched by at least one previous
 * statistics) and clauses that are estimated.
 *
 * Then several checks are performed:
 *
 * (a) The statistics covers at least 2 columns, referenced in the estimated
 * clauses (otherwise multi-variate stats are useless).
 *
 * (b) The statistics covers at least 1 new column, i.e. column not refefenced
 * by the already used stats (and the new column has to be referenced by
 * the clauses, of couse). Otherwise the statistics would not add any new
 * information.
 *
 * There are some other sanity checks (e.g. stats must not be used twice etc.).
 *
 *
 * Weaknesses
 * ----------
 * The current implemetation uses a rather simple optimality criteria, so it may
 * not do the best choice when
 *
 * (a) There may be multiple solutions with the same number of covered
 * attributes and number of statistics (e.g. the same solution but with
 * statistics in a different order). It's unclear which solution in the best
 * one - in a sense all of them are equal.
 *
 * TODO: It might be possible to compute estimate for each of those solutions,
 * and then combine them to get the final estimate (e.g. by using average
 * or median).
 *
 * (b) Does not consider that some types of stats are a better match for some
 * types of clauses (e.g. MCV list is generally a better match for equality
 * conditions than a histogram).
 *
 * But maybe this is pointless - generally, each column is either a label
 * (it's not important whether because of the data type or how it's used),
 * or a value with ordering that makes sense. So either a MCV list is more
 * appropriate (labels) or a histogram (values with orderings).
 *
 * Now sure what to do with statistics on columns mixing both types of data
 * (some columns would work best with MCVs, some with histograms). Maybe we
 * could invent a new type of statistics combining MCV list and histogram
 * (keeping a small histogram for each MCV item, and a separate histogram
 * for values not on the MCV list).
 *
 * TODO: The algorithm should probably count number of Vars (not just attnums)
 * when computing the 'score' of each solution. Computing the ratio of
 * (num of all vars) / (num of condition vars) as a measure of how well
 * the solution uses conditions might be useful.
 */
static void
choose_mv_statistics_exhaustive(PlannerInfo *root, int step,
		   int nmvstats, MVStatisticInfo *mvstats, Bitmapset **stats_attnums,
				   int nclauses, Node **clauses, Bitmapset **clauses_attnums,
		  int nconditions, Node **conditions, Bitmapset **conditions_attnums,
						bool *cover_map, bool *condition_map, int *ruled_out,
								mv_solution_t *current, mv_solution_t **best)
{
	int			i,
				j;

	Assert(best != NULL);
	Assert((step == 0 && current == NULL) || (step > 0 && current != NULL));

	/* this may run for a long sime, so let's make it interruptible */
	CHECK_FOR_INTERRUPTS();

	if (current == NULL)
	{
		current = (mv_solution_t *) palloc0(sizeof(mv_solution_t));
		current->stats = (int *) palloc0(sizeof(int) * nmvstats);
		current->nstats = 0;
		current->nclauses = 0;
		current->nconditions = 0;
	}

	/*
	 * Now try to apply each statistics, matching at least two attributes,
	 * unless it's already used in one of the previous steps.
	 */
	for (i = 0; i < nmvstats; i++)
	{
		int			c;

		int			ncovered_clauses = 0;		/* number of covered clauses */
		int			ncovered_conditions = 0;	/* number of covered
												 * conditions */
		int			nattnums = 0;		/* number of covered attributes */

		Bitmapset  *all_attnums = NULL;

		/* skip statistics that were already used or eliminated */
		if (ruled_out[i] != -1)
			continue;

		/*
		 * See if we have clauses covered by this statistics, but not yet
		 * covered by any of the preceding onces.
		 */
		for (c = 0; c < nclauses; c++)
		{
			bool		covered = false;
			Bitmapset  *clause_attnums = clauses_attnums[c];
			Bitmapset  *tmp = NULL;

			/*
			 * If this clause is not covered by this stats, we can't use the
			 * stats to estimate that at all.
			 */
			if (!cover_map[i * nclauses + c])
				continue;

			/*
			 * Now we know we'll use this clause - either as a condition or as
			 * a new clause (the estimated one). So let's add the attributes
			 * to the attnums from all the clauses usable with this
			 * statistics.
			 */
			tmp = bms_union(all_attnums, clause_attnums);

			/* free the old bitmap */
			bms_free(all_attnums);
			all_attnums = tmp;

			/* let's see if it's covered by any of the previous stats */
			for (j = 0; j < step; j++)
			{
				/* already covered by the previous stats */
				if (cover_map[current->stats[j] * nclauses + c])
					covered = true;

				if (covered)
					break;
			}

			/* if already covered, continue with the next clause */
			if (covered)
			{
				ncovered_conditions += 1;
				continue;
			}

			/*
			 * OK, this clause is covered by this statistics (and not by any
			 * of the previous ones)
			 */
			ncovered_clauses += 1;
		}

		/* can't have more new clauses than original clauses */
		Assert(nclauses >= ncovered_clauses);
		Assert(ncovered_clauses >= 0);	/* mostly paranoia */

		nattnums = bms_num_members(all_attnums);

		/* free all the bitmapsets - we don't need them anymore */
		bms_free(all_attnums);

		all_attnums = NULL;

		/*
		 * See if we have clauses covered by this statistics, but not yet
		 * covered by any of the preceding onces.
		 */
		for (c = 0; c < nconditions; c++)
		{
			Bitmapset  *clause_attnums = conditions_attnums[c];
			Bitmapset  *tmp = NULL;

			/*
			 * If this clause is not covered by this stats, we can't use the
			 * stats to estimate that at all.
			 */
			if (!condition_map[i * nconditions + c])
				continue;

			/* count this as a condition */
			ncovered_conditions += 1;

			/*
			 * Now we know we'll use this clause - either as a condition or as
			 * a new clause (the estimated one). So let's add the attributes
			 * to the attnums from all the clauses usable with this
			 * statistics.
			 */
			tmp = bms_union(all_attnums, clause_attnums);

			/* free the old bitmap */
			bms_free(all_attnums);
			all_attnums = tmp;
		}

		/*
		 * Let's mark the statistics as 'ruled out' - either we'll use it (and
		 * proceed to the next step), or it's incompatible.
		 */
		ruled_out[i] = step;

		/*
		 * There are no clauses usable with this statistics (not already
		 * covered by aome of the previous stats).
		 *
		 * Similarly, if the clauses only use a single attribute, we can't
		 * really use that.
		 */
		if ((ncovered_clauses == 0) || (nattnums < 2))
			continue;

		/*
		 * TODO Not sure if it's possible to add a clause referencing only
		 * attributes already covered by previous stats? Introducing only some
		 * new dependency, not a new attribute. Couldn't come up with an
		 * example, though. Might be worth adding some assert.
		 */

		/*
		 * got a suitable statistics - let's update the current solution,
		 * maybe use it as the best solution
		 */
		current->nclauses += ncovered_clauses;
		current->nconditions += ncovered_conditions;
		current->nstats += 1;
		current->stats[step] = i;

		/*
		 * We can never cover more clauses, or use more stats that we actually
		 * have at the beginning.
		 */
		Assert(nclauses >= current->nclauses);
		Assert(nmvstats >= current->nstats);
		Assert(step < nmvstats);

		if (*best == NULL)
		{
			*best = (mv_solution_t *) palloc0(sizeof(mv_solution_t));
			(*best)->stats = (int *) palloc0(sizeof(int) * nmvstats);
			(*best)->nstats = 0;
			(*best)->nclauses = 0;
			(*best)->nconditions = 0;
		}

		/* see if it's better than the current 'best' solution */
		if ((current->nclauses > (*best)->nclauses) ||
			((current->nclauses == (*best)->nclauses) &&
			 ((current->nstats > (*best)->nstats))))
		{
			(*best)->nstats = current->nstats;
			(*best)->nclauses = current->nclauses;
			(*best)->nconditions = current->nconditions;
			memcpy((*best)->stats, current->stats, nmvstats * sizeof(int));
		}

		/*
		 * The recursion only makes sense if we haven't covered all the
		 * attributes (then adding stats is not really possible).
		 */
		if ((step + 1) < nmvstats)
			choose_mv_statistics_exhaustive(root, step + 1,
											nmvstats, mvstats, stats_attnums,
										  nclauses, clauses, clauses_attnums,
								 nconditions, conditions, conditions_attnums,
										 cover_map, condition_map, ruled_out,
											current, best);

		/* reset the last step */
		current->nclauses -= ncovered_clauses;
		current->nconditions -= ncovered_conditions;
		current->nstats -= 1;
		current->stats[step] = 0;

		/* mark the statistics as usable again */
		ruled_out[i] = -1;

		Assert(current->nclauses >= 0);
		Assert(current->nstats >= 0);
	}

	/* reset all statistics as 'incompatible' in this step */
	for (i = 0; i < nmvstats; i++)
		if (ruled_out[i] == step)
			ruled_out[i] = -1;

}

/*
 * Greedy search for a multivariate solution - a sequence of statistics covering
 * the clauses. This chooses the "best" statistics at each step, so the
 * resulting solution may not be the best solution globally, but this produces
 * the solution in only N steps (where N is the number of statistics), while
 * the exhaustive approach may have to walk through ~N! combinations (although
 * some of those are terminated early).
 *
 * See the comments at choose_mv_statistics_exhaustive() as this does the same
 * thing (but in a different way).
 *
 * Don't call this directly, but through choose_mv_statistics().
 *
 * TODO: There are probably other metrics we might use - e.g. using number of
 * columns (num_cond_columns / num_cov_columns), which might work better
 * with a mix of simple and complex clauses.
 *
 * TODO: Also the choice at the very first step should be handled in a special
 * way, because there will be 0 conditions at that moment, so there needs
 * to be some other criteria - e.g. using the simplest (or most complex?)
 * clause might be a good idea.
 *
 * TODO: We might also select multiple stats using different criteria, and branch
 * the search. This is however tricky, because if we choose k statistics at
 * each step, we get k^N branches to walk through (with N steps). That's
 * not really good with large number of stats (yet better than exhaustive
 * search).
 */
static void
choose_mv_statistics_greedy(PlannerInfo *root, int step,
		   int nmvstats, MVStatisticInfo *mvstats, Bitmapset **stats_attnums,
				   int nclauses, Node **clauses, Bitmapset **clauses_attnums,
		  int nconditions, Node **conditions, Bitmapset **conditions_attnums,
						bool *cover_map, bool *condition_map, int *ruled_out,
							mv_solution_t *current, mv_solution_t **best)
{
	int			i,
				j;
	int			best_stat = -1;
	double		gain,
				max_gain = -1.0;

	/*
	 * Bitmap tracking which clauses are already covered (by the previous
	 * statistics) and may thus serve only as a condition in this step.
	 */
	bool	   *covered_clauses = (bool *) palloc0(nclauses);

	/*
	 * Number of clauses and columns covered by each statistics - this
	 * includes both conditions and clauses covered by the statistics for the
	 * first time. The number of columns may count some columns repeatedly -
	 * if a column is shared by multiple clauses, it will be counted once for
	 * each clause (covered by the statistics). So with two clauses [(a=1 OR
	 * b=2),(a<2 OR c>1)] the column "a" will be counted twice (if both
	 * clauses are covered).
	 *
	 * The values for reduded statistics (that can't be applied) are not
	 * computed, because that'd be pointless.
	 */
	int		   *num_cov_clauses = (int *) palloc0(sizeof(int) * nmvstats);
	int		   *num_cov_columns = (int *) palloc0(sizeof(int) * nmvstats);

	/*
	 * Same as above, but this only includes clauses that are already covered
	 * by the previous stats (and the current one).
	 */
	int		   *num_cond_clauses = (int *) palloc0(sizeof(int) * nmvstats);
	int		   *num_cond_columns = (int *) palloc0(sizeof(int) * nmvstats);

	/*
	 * Number of attributes for each clause.
	 *
	 * TODO Might be computed in choose_mv_statistics() and then passed here,
	 * but then the function would not have the same signature as
	 * _exhaustive().
	 */
	int		   *attnum_counts = (int *) palloc0(sizeof(int) * nclauses);
	int		   *attnum_cond_counts = (int *) palloc0(sizeof(int) * nconditions);

	CHECK_FOR_INTERRUPTS();

	Assert(best != NULL);
	Assert((step == 0 && current == NULL) || (step > 0 && current != NULL));

	/* compute attributes (columns) for each clause */
	for (i = 0; i < nclauses; i++)
		attnum_counts[i] = bms_num_members(clauses_attnums[i]);

	/* compute attributes (columns) for each condition */
	for (i = 0; i < nconditions; i++)
		attnum_cond_counts[i] = bms_num_members(conditions_attnums[i]);

	/* see which clauses are already covered at this point (by previous stats) */
	for (i = 0; i < step; i++)
		for (j = 0; j < nclauses; j++)
			covered_clauses[j] |= (cover_map[current->stats[i] * nclauses + j]);

	/* which remaining statistics covers most clauses / uses most conditions? */
	for (i = 0; i < nmvstats; i++)
	{
		Bitmapset  *attnums_covered = NULL;
		Bitmapset  *attnums_conditions = NULL;

		/* skip stats that are already ruled out (either used or inapplicable) */
		if (ruled_out[i] != -1)
			continue;

		/* count covered clauses and conditions (for the statistics) */
		for (j = 0; j < nclauses; j++)
		{
			if (cover_map[i * nclauses + j])
			{
				Bitmapset  *attnums_new
				= bms_union(attnums_covered, clauses_attnums[j]);

				/* get rid of the old bitmap and keep the unified result */
				bms_free(attnums_covered);
				attnums_covered = attnums_new;

				num_cov_clauses[i] += 1;
				num_cov_columns[i] += attnum_counts[j];

				/* is the clause already covered (i.e. a condition)? */
				if (covered_clauses[j])
				{
					num_cond_clauses[i] += 1;
					num_cond_columns[i] += attnum_counts[j];
					attnums_new = bms_union(attnums_conditions,
											clauses_attnums[j]);

					bms_free(attnums_conditions);
					attnums_conditions = attnums_new;
				}
			}
		}

		/* if all covered clauses are covered by prev stats (thus conditions) */
		if (num_cov_clauses[i] == num_cond_clauses[i])
			ruled_out[i] = step;

		/* same if there are no new attributes */
		else if (bms_num_members(attnums_conditions) == bms_num_members(attnums_covered))
			ruled_out[i] = step;

		bms_free(attnums_covered);
		bms_free(attnums_conditions);

		/* if the statistics is inapplicable, try the next one */
		if (ruled_out[i] != -1)
			continue;

		/* now let's walk through conditions and count the covered */
		for (j = 0; j < nconditions; j++)
		{
			if (condition_map[i * nconditions + j])
			{
				num_cond_clauses[i] += 1;
				num_cond_columns[i] += attnum_cond_counts[j];
			}
		}

		/* otherwise see if this improves the interesting metrics */
		gain = num_cond_columns[i] / (double) num_cov_columns[i];

		if (gain > max_gain)
		{
			max_gain = gain;
			best_stat = i;
		}
	}

	/*
	 * Have we found a suitable statistics? Add it to the solution and try
	 * next step.
	 */
	if (best_stat != -1)
	{
		/* mark the statistics, so that we skip it in next steps */
		ruled_out[best_stat] = step;

		/* allocate current solution if necessary */
		if (current == NULL)
		{
			current = (mv_solution_t *) palloc0(sizeof(mv_solution_t));
			current->stats = (int *) palloc0(sizeof(int) * nmvstats);
			current->nstats = 0;
			current->nclauses = 0;
			current->nconditions = 0;
		}

		current->nclauses += num_cov_clauses[best_stat];
		current->nconditions += num_cond_clauses[best_stat];
		current->stats[step] = best_stat;
		current->nstats++;

		if (*best == NULL)
		{
			(*best) = (mv_solution_t *) palloc0(sizeof(mv_solution_t));
			(*best)->nstats = current->nstats;
			(*best)->nclauses = current->nclauses;
			(*best)->nconditions = current->nconditions;

			(*best)->stats = (int *) palloc0(sizeof(int) * nmvstats);
			memcpy((*best)->stats, current->stats, nmvstats * sizeof(int));
		}
		else
		{
			/* see if this is a better solution */
			double		current_gain = (double) current->nconditions / current->nclauses;
			double		best_gain = (double) (*best)->nconditions / (*best)->nclauses;

			if ((current_gain > best_gain) ||
				((current_gain == best_gain) && (current->nstats < (*best)->nstats)))
			{
				(*best)->nstats = current->nstats;
				(*best)->nclauses = current->nclauses;
				(*best)->nconditions = current->nconditions;
				memcpy((*best)->stats, current->stats, nmvstats * sizeof(int));
			}
		}

		/*
		 * The recursion only makes sense if we haven't covered all the
		 * attributes (then adding stats is not really possible).
		 */
		if ((step + 1) < nmvstats)
			choose_mv_statistics_greedy(root, step + 1,
										nmvstats, mvstats, stats_attnums,
										nclauses, clauses, clauses_attnums,
								 nconditions, conditions, conditions_attnums,
										cover_map, condition_map, ruled_out,
										current, best);

		/* reset the last step */
		current->nclauses -= num_cov_clauses[best_stat];
		current->nconditions -= num_cond_clauses[best_stat];
		current->nstats -= 1;
		current->stats[step] = 0;

		/* mark the statistics as usable again */
		ruled_out[best_stat] = -1;
	}

	/* reset all statistics eliminated in this step */
	for (i = 0; i < nmvstats; i++)
		if (ruled_out[i] == step)
			ruled_out[i] = -1;

	/* free everything allocated in this step */
	pfree(covered_clauses);
	pfree(attnum_counts);
	pfree(num_cov_clauses);
	pfree(num_cov_columns);
	pfree(num_cond_clauses);
	pfree(num_cond_columns);
}

/*
 * Remove clauses not covered by any of the available statistics
 *
 * This helps us to reduce the amount of work done in choose_mv_statistics()
 * by not having to deal with clauses that can't possibly be useful.
 */
static List *
filter_clauses(PlannerInfo *root, Index relid, int type,
			   List *stats, List *clauses, Bitmapset **attnums)
{
	ListCell   *c;
	ListCell   *s;

	/* results (list of compatible clauses, attnums) */
	List	   *rclauses = NIL;

	foreach(c, clauses)
	{
		Node	   *clause = (Node *) lfirst(c);
		Bitmapset  *clause_attnums = NULL;

		/*
		 * We do assume that thanks to previous checks, we should not run into
		 * clauses that are incompatible with multivariate stats here. We also
		 * need to collect the attnums for the clause.
		 *
		 * XXX Maybe turn this into an assert?
		 */
		if (!clause_is_mv_compatible(clause, relid, &clause_attnums, type))
			elog(ERROR, "should not get non-mv-compatible cluase");

		/* Is there a multivariate statistics covering the clause? */
		foreach(s, stats)
		{
			int			k,
						matches = 0;
			MVStatisticInfo *stat = (MVStatisticInfo *) lfirst(s);

			/* skip statistics not matching the required type */
			if (!stats_type_matches(stat, type))
				continue;

			/*
			 * see if all clause attributes are covered by the statistic
			 *
			 * We'll do that in the opposite direction, i.e. we'll see how
			 * many attributes of the statistic are referenced in the clause,
			 * and then compare the counts.
			 */
			for (k = 0; k < stat->stakeys->dim1; k++)
				if (bms_is_member(stat->stakeys->values[k], clause_attnums))
					matches += 1;

			/*
			 * If the number of matches is equal to attributes referenced by
			 * the clause, then the clause is covered by the statistic.
			 */
			if (bms_num_members(clause_attnums) == matches)
			{
				*attnums = bms_union(*attnums, clause_attnums);
				rclauses = lappend(rclauses, clause);
				break;
			}
		}

		bms_free(clause_attnums);
	}

	/* we can't have more compatible conditions than source conditions */
	Assert(list_length(clauses) >= list_length(rclauses));

	return rclauses;
}

/*
 * Remove statistics not covering any new clauses
 *
 * Statistics not covering any new clauses (conditions don't count) are not
 * really useful, so let's ignore them. Also, we need the statistics to
 * reference at least two different attributes (both in conditions and clauses
 * combined), and at least one of them in the clauses alone.
 *
 * This check might be made more strict by checking against individual clauses,
 * because by using the bitmapsets of all attnums we may actually use attnums
 * from clauses that are not covered by the statistics. For example, we may
 * have a condition
 *
 * (a=1 AND b=2)
 *
 * and a new clause
 *
 * (c=1 AND d=1)
 *
 * With only bitmapsets, statistics on [b,c] will pass through this (assuming
 * there are some statistics covering both clases).
 *
 * Parameters:
 *
 * - stats - list of statistics to filter
 * - new_attnums - attnums referenced in new clauses
 * - all_attnums - attnums referenced by contidions and new clauses combined
 *
 * Returns filtered list of statistics.
 *
 * TODO: Do the more strict check, i.e. walk through individual clauses and
 * conditions and only use those covered by the statistics.
 */
static List *
filter_stats(List *stats, Bitmapset *new_attnums, Bitmapset *all_attnums)
{
	ListCell   *s;
	List	   *stats_filtered = NIL;

	foreach(s, stats)
	{
		int			k;
		int			matches_new = 0,
					matches_all = 0;

		MVStatisticInfo *stat = (MVStatisticInfo *) lfirst(s);

		/* see how many attributes the statistics covers */
		for (k = 0; k < stat->stakeys->dim1; k++)
		{
			/* attributes from new clauses */
			if (bms_is_member(stat->stakeys->values[k], new_attnums))
				matches_new += 1;

			/* attributes from onditions */
			if (bms_is_member(stat->stakeys->values[k], all_attnums))
				matches_all += 1;
		}

		/* check we have enough attributes for this statistics */
		if ((matches_new >= 1) && (matches_all >= 2))
			stats_filtered = lappend(stats_filtered, stat);
	}

	/* we can't have more useful stats than we had originally */
	Assert(list_length(stats) >= list_length(stats_filtered));

	return stats_filtered;
}

static MVStatisticInfo *
make_stats_array(List *stats, int *nmvstats)
{
	int			i;
	ListCell   *l;

	MVStatisticInfo *mvstats = NULL;

	*nmvstats = list_length(stats);

	mvstats
		= (MVStatisticInfo *) palloc0((*nmvstats) * sizeof(MVStatisticInfo));

	i = 0;
	foreach(l, stats)
	{
		MVStatisticInfo *stat = (MVStatisticInfo *) lfirst(l);

		memcpy(&mvstats[i++], stat, sizeof(MVStatisticInfo));
	}

	return mvstats;
}

static Bitmapset **
make_stats_attnums(MVStatisticInfo *mvstats, int nmvstats)
{
	int			i,
				j;
	Bitmapset **stats_attnums = NULL;

	Assert(nmvstats > 0);

	/* build bitmaps of attnums for the stats (easier to compare) */
	stats_attnums = (Bitmapset **) palloc0(nmvstats * sizeof(Bitmapset *));

	for (i = 0; i < nmvstats; i++)
		for (j = 0; j < mvstats[i].stakeys->dim1; j++)
			stats_attnums[i]
				= bms_add_member(stats_attnums[i],
								 mvstats[i].stakeys->values[j]);

	return stats_attnums;
}


/*
 * Remove redundant statistics
 *
 * If there are multiple statistics covering the same set of columns (counting
 * only those referenced by clauses and conditions), we can apply one of those
 * anyway and further reduce the size of the optimization problem.
 *
 * Thus when redundant stats are detected, we keep the smaller one (the one with
 * fewer columns), based on the assumption that it's more accurate and also
 * faster to process. That may be untrue for two reasons - first, the accuracy
 * really depends on number of buckets/MCV items, not the number of columns.
 * Second, some types of statistics may work better for certain types of clauses
 * (e.g. MCV lists for equality conditions) etc.
 */
static List *
filter_redundant_stats(List *stats, List *clauses, List *conditions)
{
	int			i,
				j,
				nmvstats;

	MVStatisticInfo *mvstats;
	bool	   *redundant;
	Bitmapset **stats_attnums;
	Bitmapset  *varattnos;
	Index		relid;

	Assert(list_length(stats) > 0);
	Assert(list_length(clauses) > 0);

	/*
	 * We'll convert the list of statistics into an array now, because the
	 * reduction of redundant statistics is easier to do that way (we can mark
	 * previous stats as redundant, etc.).
	 */
	mvstats = make_stats_array(stats, &nmvstats);
	stats_attnums = make_stats_attnums(mvstats, nmvstats);

	/* by default, none of the stats is redundant (so palloc0) */
	redundant = palloc0(nmvstats * sizeof(bool));

	/*
	 * We only expect a single relid here, and also we should get the same
	 * relid from clauses and conditions (but we get it from clauses, because
	 * those are certainly non-empty).
	 */
	relid = bms_singleton_member(pull_varnos((Node *) clauses));

	/*
	 * Get the varattnos from both conditions and clauses.
	 *
	 * This skips system attributes, although that should be impossible thanks
	 * to previous filtering out of incompatible clauses.
	 *
	 * XXX Is that really true?
	 */
	varattnos = bms_union(get_varattnos((Node *) clauses, relid),
						  get_varattnos((Node *) conditions, relid));

	for (i = 1; i < nmvstats; i++)
	{
		/* intersect with current statistics */
		Bitmapset  *curr = bms_intersect(stats_attnums[i], varattnos);

		/* walk through 'previous' stats and check redundancy */
		for (j = 0; j < i; j++)
		{
			/* intersect with current statistics */
			Bitmapset  *prev;

			/* skip stats already identified as redundant */
			if (redundant[j])
				continue;

			prev = bms_intersect(stats_attnums[j], varattnos);

			switch (bms_subset_compare(curr, prev))
			{
				case BMS_EQUAL:

					/*
					 * Use the smaller one (hopefully more accurate). If both
					 * have the same size, use the first one.
					 */
					if (mvstats[i].stakeys->dim1 >= mvstats[j].stakeys->dim1)
						redundant[i] = TRUE;
					else
						redundant[j] = TRUE;

					break;

				case BMS_SUBSET1:		/* curr is subset of prev */
					redundant[i] = TRUE;
					break;

				case BMS_SUBSET2:		/* prev is subset of curr */
					redundant[j] = TRUE;
					break;

				case BMS_DIFFERENT:
					/* do nothing - keep both stats */
					break;
			}

			bms_free(prev);
		}

		bms_free(curr);
	}

	/* can't reduce all statistics (at least one has to remain) */
	Assert(nmvstats > 0);

	/* now, let's remove the reduced statistics from the arrays */
	list_free(stats);
	stats = NIL;

	for (i = 0; i < nmvstats; i++)
	{
		MVStatisticInfo *info;

		pfree(stats_attnums[i]);

		if (redundant[i])
			continue;

		info = makeNode(MVStatisticInfo);
		memcpy(info, &mvstats[i], sizeof(MVStatisticInfo));

		stats = lappend(stats, info);
	}

	pfree(mvstats);
	pfree(stats_attnums);
	pfree(redundant);

	return stats;
}

static Node **
make_clauses_array(List *clauses, int *nclauses)
{
	int			i;
	ListCell   *l;

	Node	  **clauses_array;

	*nclauses = list_length(clauses);
	clauses_array = (Node **) palloc0((*nclauses) * sizeof(Node *));

	i = 0;
	foreach(l, clauses)
		clauses_array[i++] = (Node *) lfirst(l);

	*nclauses = i;

	return clauses_array;
}

static Bitmapset **
make_clauses_attnums(PlannerInfo *root, Index relid,
					 int type, Node **clauses, int nclauses)
{
	int			i;
	Bitmapset **clauses_attnums
	= (Bitmapset **) palloc0(nclauses * sizeof(Bitmapset *));

	for (i = 0; i < nclauses; i++)
	{
		Bitmapset  *attnums = NULL;

		if (!clause_is_mv_compatible(clauses[i], relid, &attnums, type))
			elog(ERROR, "should not get non-mv-compatible clause");

		clauses_attnums[i] = attnums;
	}

	return clauses_attnums;
}

static bool *
make_cover_map(Bitmapset **stats_attnums, int nmvstats,
			   Bitmapset **clauses_attnums, int nclauses)
{
	int			i,
				j;
	bool	   *cover_map = (bool *) palloc0(nclauses * nmvstats);

	for (i = 0; i < nmvstats; i++)
		for (j = 0; j < nclauses; j++)
			cover_map[i * nclauses + j]
				= bms_is_subset(clauses_attnums[j], stats_attnums[i]);

	return cover_map;
}

/*
 * Chooses the combination of statistics, optimal for estimation of a particular
 * clause list.
 *
 * This only handles a 'preparation' shared by the exhaustive and greedy
 * implementations (see the previous methods), mostly trying to reduce the size
 * of the problem (eliminate clauses/statistics that can't be really used in
 * the solution).
 *
 * It also precomputes bitmaps for attributes covered by clauses and statistics,
 * so that we don't need to do that over and over in the actual optimizations
 * (as it's both CPU and memory intensive).
 *
 *
 * TODO: Another way to make the optimization problems smaller might be splitting
 * the statistics into several disjoint subsets, i.e. if we can split the
 * graph of statistics (after the elimination) into multiple components
 * (so that stats in different components share no attributes), we can do
 * the optimization for each component separately.
 *
 * TODO: If we could compute what is a "perfect solution" maybe we could
 * terminate the search after reaching ~90% of it? Say, if we knew that we
 * can cover 10 clauses and reuse 8 dependencies, maybe covering 9 clauses
 * and 7 dependencies would be OK?
 */
static List *
choose_mv_statistics(PlannerInfo *root, Index relid, List *stats,
					 List *clauses, List *conditions)
{
	int			i;
	mv_solution_t *best = NULL;
	List	   *result = NIL;

	int			nmvstats;
	MVStatisticInfo *mvstats;

	/* we only work with MCV lists and histograms here */
	int			type = (MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST);

	bool	   *clause_cover_map = NULL,
			   *condition_cover_map = NULL;
	int		   *ruled_out = NULL;

	/* build bitmapsets for all stats and clauses */
	Bitmapset **stats_attnums;
	Bitmapset **clauses_attnums;
	Bitmapset **conditions_attnums;

	int			nclauses,
				nconditions;
	Node	  **clauses_array;
	Node	  **conditions_array;

	/* copy lists, so that we can free them during elimination easily */
	clauses = list_copy(clauses);
	conditions = list_copy(conditions);
	stats = list_copy(stats);

	/*
	 * Reduce the optimization problem size as much as possible.
	 *
	 * Eliminate clauses and conditions not covered by any statistics, or
	 * statistics not matching at least two attributes (one of them has to be
	 * in a regular clause).
	 *
	 * It's possible that removing a statistics in one iteration eliminates
	 * clause in the next one, so we'll repeat this until we eliminate no
	 * clauses/stats in that iteration.
	 *
	 * This can only happen after eliminating a statistics - clauses are
	 * eliminated first, so statistics always reflect that.
	 */
	while (true)
	{
		List	   *tmp;

		Bitmapset  *compatible_attnums = NULL;
		Bitmapset  *condition_attnums = NULL;
		Bitmapset  *all_attnums = NULL;

		/*
		 * Clauses
		 *
		 * Walk through clauses and keep only those covered by at least one of
		 * the statistics we still have. We'll also keep info about attnums in
		 * clauses (without conditions) so that we can ignore stats covering
		 * just conditions (which is pointless).
		 */
		tmp = filter_clauses(root, relid, type,
							 stats, clauses, &compatible_attnums);

		/* discard the original list */
		list_free(clauses);
		clauses = tmp;

		/*
		 * Conditions
		 *
		 * Walk through clauses and keep only those covered by at least one of
		 * the statistics we still have. Also, collect bitmap of attributes so
		 * that we can make sure we add at least one new attribute (by
		 * comparing with clauses).
		 */
		if (conditions != NIL)
		{
			tmp = filter_clauses(root, relid, type,
								 stats, conditions, &condition_attnums);

			/* discard the original list */
			list_free(conditions);
			conditions = tmp;
		}

		/* get a union of attnums (from conditions and new clauses) */
		all_attnums = bms_union(compatible_attnums, condition_attnums);

		/*
		 * Statisitics
		 *
		 * Walk through statistics and only keep those covering at least one
		 * new attribute (excluding conditions) and at two attributes in both
		 * clauses and conditions.
		 */
		tmp = filter_stats(stats, compatible_attnums, all_attnums);

		/* if we've not eliminated anything, terminate */
		if (list_length(stats) == list_length(tmp))
			break;

		/* work only with filtered statistics from now */
		list_free(stats);
		stats = tmp;
	}

	/* only do the optimization if we have clauses/statistics */
	if ((list_length(stats) == 0) || (list_length(clauses) == 0))
		return NULL;

	/* remove redundant stats (stats covered by another stats) */
	stats = filter_redundant_stats(stats, clauses, conditions);

	/*
	 * TODO: We should sort the stats to make the order deterministic,
	 * otherwise we may get different estimates on different executions - if
	 * there are multiple "equally good" solutions, we'll keep the first
	 * solution we see.
	 *
	 * Sorting by OID probably is not the right solution though, because we'd
	 * like it to be somehow reproducible, irrespectedly of the order of ADD
	 * STATISTICS commands. So maybe statkeys?
	 */
	mvstats = make_stats_array(stats, &nmvstats);
	stats_attnums = make_stats_attnums(mvstats, nmvstats);

	/* collect clauses an bitmap of attnums */
	clauses_array = make_clauses_array(clauses, &nclauses);
	clauses_attnums = make_clauses_attnums(root, relid, type,
										   clauses_array, nclauses);

	/* collect conditions and bitmap of attnums */
	conditions_array = make_clauses_array(conditions, &nconditions);
	conditions_attnums = make_clauses_attnums(root, relid, type,
											  conditions_array, nconditions);

	/*
	 * Build bitmaps with info about which clauses/conditions are covered by
	 * each statistics (so that we don't need to call the bms_is_subset over
	 * and over again).
	 */
	clause_cover_map = make_cover_map(stats_attnums, nmvstats,
									  clauses_attnums, nclauses);

	condition_cover_map = make_cover_map(stats_attnums, nmvstats,
										 conditions_attnums, nconditions);

	ruled_out = (int *) palloc0(nmvstats * sizeof(int));

	/* no stats are ruled out by default */
	for (i = 0; i < nmvstats; i++)
		ruled_out[i] = -1;

	/* do the optimization itself */
	if (mvstat_search_type == MVSTAT_SEARCH_EXHAUSTIVE)
		choose_mv_statistics_exhaustive(root, 0,
										nmvstats, mvstats, stats_attnums,
									nclauses, clauses_array, clauses_attnums,
						   nconditions, conditions_array, conditions_attnums,
										clause_cover_map, condition_cover_map,
										ruled_out, NULL, &best);
	else
		choose_mv_statistics_greedy(root, 0,
									nmvstats, mvstats, stats_attnums,
									nclauses, clauses_array, clauses_attnums,
						   nconditions, conditions_array, conditions_attnums,
									clause_cover_map, condition_cover_map,
									ruled_out, NULL, &best);

	/* create a list of statistics from the array */
	if (best != NULL)
	{
		for (i = 0; i < best->nstats; i++)
		{
			MVStatisticInfo *info = makeNode(MVStatisticInfo);

			memcpy(info, &mvstats[best->stats[i]], sizeof(MVStatisticInfo));
			result = lappend(result, info);
		}

		pfree(best);
	}

	/* cleanup (maybe leave it up to the memory context?) */
	for (i = 0; i < nmvstats; i++)
		bms_free(stats_attnums[i]);

	for (i = 0; i < nclauses; i++)
		bms_free(clauses_attnums[i]);

	for (i = 0; i < nconditions; i++)
		bms_free(conditions_attnums[i]);

	pfree(stats_attnums);
	pfree(clauses_attnums);
	pfree(conditions_attnums);

	pfree(clauses_array);
	pfree(conditions_array);
	pfree(clause_cover_map);
	pfree(condition_cover_map);
	pfree(ruled_out);
	pfree(mvstats);

	list_free(clauses);
	list_free(conditions);
	list_free(stats);

	return result;
}

typedef struct
{
	int			types;			/* types of statistics ? */
	Index		varno;			/* relid we're interested in */
	Bitmapset  *varattnos;		/* attnums referenced by the clauses */
} mv_compatible_context;

/*
 * Recursive walker that checks compatibility of the clause with multivariate
 * statistics, and collects attnums from the Vars.
 *
 * XXX The original idea was to combine this with expression_tree_walker, but
 * I've been unable to make that work - seems that does not quite allow
 * checking the structure. Hence the explicit calls to the walker.
 */
static bool
mv_compatible_walker(Node *node, mv_compatible_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RestrictInfo))
	{
		RestrictInfo *rinfo = (RestrictInfo *) node;

		/* Pseudoconstants are not really interesting here. */
		if (rinfo->pseudoconstant)
			return true;

		/* clauses referencing multiple varnos are incompatible */
		if (bms_membership(rinfo->clause_relids) != BMS_SINGLETON)
			return true;

		/* check the clause inside the RestrictInfo */
		return mv_compatible_walker((Node *) rinfo->clause, (void *) context);
	}

	if (or_clause(node) || and_clause(node) || not_clause(node))
	{
		/*
		 * AND/OR/NOT-clauses are supported if all sub-clauses are supported
		 *
		 * TODO: We might support mixed case, where some of the clauses are
		 * supported and some are not, and treat all supported subclauses as a
		 * single clause, compute it's selectivity using mv stats, and compute
		 * the total selectivity using the current algorithm.
		 *
		 * TODO: For RestrictInfo above an OR-clause, we might use the
		 * orclause with nested RestrictInfo - we won't have to call
		 * pull_varnos() for each clause, saving time.
		 *
		 * TODO: Perhaps this needs a bit more thought for functional
		 * dependencies? Those don't quite work for NOT cases.
		 */
		BoolExpr   *expr = (BoolExpr *) node;
		ListCell   *lc;

		foreach(lc, expr->args)
		{
			if (mv_compatible_walker((Node *) lfirst(lc), context))
				return true;
		}

		return false;
	}

	if (IsA(node, NullTest))
	{
		NullTest   *nt = (NullTest *) node;

		/*
		 * Only simple (Var IS NULL) expressions supported for now. Maybe we
		 * could use examine_variable to fix this?
		 */
		if (!IsA(nt->arg, Var))
			return true;

		return mv_compatible_walker((Node *) (nt->arg), context);
	}

	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		/*
		 * Also, the variable needs to reference the right relid (this might
		 * be unnecessary given the other checks, but let's be sure).
		 */
		if (var->varno != context->varno)
			return true;

		/* Also skip system attributes (we don't allow stats on those). */
		if (!AttrNumberIsForUserDefinedAttr(var->varattno))
			return true;

		/* Seems fine, so let's remember the attnum. */
		context->varattnos = bms_add_member(context->varattnos, var->varattno);

		return false;
	}

	/*
	 * And finally the operator expressions - we only allow simple expressions
	 * with two arguments, where one is a Var and the other is a constant, and
	 * it's a simple comparison (which we detect using estimator function).
	 */
	if (is_opclause(node))
	{
		OpExpr	   *expr = (OpExpr *) node;
		Var		   *var;
		bool		varonleft = true;
		bool		ok;

		/*
		 * Only expressions with two arguments are considered compatible.
		 *
		 * XXX Possibly unnecessary (can OpExpr have different arg count?).
		 */
		if (list_length(expr->args) != 2)
			return true;

		/* see if it actually has the right */
		ok = (NumRelids((Node *) expr) == 1) &&
			(is_pseudo_constant_clause(lsecond(expr->args)) ||
			 (varonleft = false,
			  is_pseudo_constant_clause(linitial(expr->args))));

		/* unsupported structure (two variables or so) */
		if (!ok)
			return true;

		/*
		 * If it's not a "<" or ">" or "=" operator, just ignore the clause.
		 * Otherwise note the relid and attnum for the variable. This uses the
		 * function for estimating selectivity, ont the operator directly (a
		 * bit awkward, but well ...).
		 */
		switch (get_oprrest(expr->opno))
		{
			case F_EQSEL:
				/* equality conditions are compatible with all statistics */
				break;

			case F_SCALARLTSEL:
			case F_SCALARGTSEL:

				/* not compatible with functional dependencies */
				if (!(context->types & (MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST)))
					return true;	/* terminate */

				break;

			default:

				/* unknown estimator */
				return true;
		}

		var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);

		return mv_compatible_walker((Node *) var, context);
	}

	/* Node not explicitly supported, so terminate */
	return true;
}

/*
 * Determines whether the clause is compatible with multivariate stats,
 * and if it is, returns some additional information - varno (index
 * into simple_rte_array) and a bitmap of attributes. This is then
 * used to fetch related multivariate statistics.
 *
 * At this moment we only support basic conditions of the form
 *
 * variable OP constant
 *
 * where OP is one of [=,<,<=,>=,>] (which is however determined by
 * looking at the associated function for estimating selectivity, just
 * like with the single-dimensional case).
 *
 * TODO: Support 'OR clauses' - shouldn't be all that difficult to
 * evaluate them using multivariate stats.
 */
static bool
clause_is_mv_compatible(Node *clause, Index relid, Bitmapset **attnums, int types)
{
	mv_compatible_context context;

	context.types = types;
	context.varno = relid;
	context.varattnos = NULL;	/* no attnums */

	if (mv_compatible_walker(clause, (void *) &context))
		return false;

	/* remember the newly collected attnums */
	*attnums = bms_add_members(*attnums, context.varattnos);

	return true;
}


/*
 * Reduce clauses using functional dependencies
 */
static List *
fdeps_reduce_clauses(List *clauses, Index relid, Bitmapset *reduced_attnums)
{
	ListCell   *lc;
	List	   *reduced_clauses = NIL;

	foreach(lc, clauses)
	{
		Bitmapset  *attnums = NULL;
		Node	   *clause = (Node *) lfirst(lc);

		/* ignore clauses that are not compatible with functional dependencies */
		if (!clause_is_mv_compatible(clause, relid, &attnums, MV_CLAUSE_TYPE_FDEP))
			reduced_clauses = lappend(reduced_clauses, clause);

		/* for equality clauses, only keep those not on reduced attributes */
		if (!bms_is_subset(attnums, reduced_attnums))
			reduced_clauses = lappend(reduced_clauses, clause);
	}

	return reduced_clauses;
}

/*
 * decide which attributes are redundant (for equality clauses)
 *
 * We try to apply all functional dependencies available, and for each one we
 * check if it matches attnums from equality clauses, but only those not yet
 * reduced.
 *
 * XXX Not sure if the order in which we apply the dependencies matters.
 *
 * XXX We do not combine functional dependencies from separate stats. That is
 * if we have dependencies on [a,b] and [b,c], then we don't deduce a->c from
 * a->b and b->c. Computing such transitive closure is a possible future
 * improvement.
 */
static Bitmapset *
fdeps_reduce_attnums(List *stats, Bitmapset *attnums)
{
	ListCell   *lc;
	Bitmapset  *reduced = NULL;

	foreach(lc, stats)
	{
		int			i;
		MVDependencies dependencies = NULL;
		MVStatisticInfo *info = (MVStatisticInfo *) lfirst(lc);

		/* skip statistics without dependencies */
		if (!stats_type_matches(info, MV_CLAUSE_TYPE_FDEP))
			continue;

		/* fetch and deserialize dependencies */
		dependencies = load_mv_dependencies(info->mvoid);

		for (i = 0; i < dependencies->ndeps; i++)
		{
			int			j;
			bool		matched = true;
			MVDependency dep = dependencies->deps[i];

			/* we don't bother to break the loop early (only few attributes) */
			for (j = 0; j < dep->nattributes; j++)
			{
				if (!bms_is_member(dep->attributes[j], attnums))
					matched = false;

				if (bms_is_member(dep->attributes[j], reduced))
					matched = false;
			}

			/* if dependency applies, mark the last attribute as reduced */
			if (matched)
				reduced = bms_add_member(reduced,
									  dep->attributes[dep->nattributes - 1]);
		}
	}

	return reduced;
}

/*
 * reduce list of equality clauses using soft functional dependencies
 *
 * We simply walk through list of functional dependencies, and for each one we
 * check whether the dependency 'matches' the clauses, i.e. if there's a clause
 * matching the condition. If yes, we attempt to remove all clauses matching
 * the implied part of the dependency from the list.
 *
 * This only reduces equality clauses, and ignores all the other types. We might
 * extend it to handle IS NULL clause, in the future.
 *
 * We also assume the equality clauses are 'compatible'. For example we can't
 * identify when the clauses use a mismatching zip code and city name. In such
 * case the usual approach (product of selectivities) would produce a better
 * estimate, although mostly by chance.
 *
 * The implementation needs to be careful about cyclic dependencies, e.g. when
 *
 * (a -> b) and (b -> a)
 *
 * at the same time, which means there's 1:1 relationship between te columns.
 * In this case we must not reduce clauses on both attributes at the same time.
 *
 * TODO: Currently we only apply functional dependencies at the same level, but
 * maybe we could transfer the clauses from upper levels to the subtrees?
 * For example let's say we have (a->b) dependency, and condition
 *
 * (a=1) AND (b=2 OR c=3)
 *
 * Currently, we won't be able to perform any reduction, because we'll
 * consider (a=1) and (b=2 OR c=3) independently. But maybe we could pass
 * (a=1) into the other expression, and only check it against conditions
 * of the functional dependencies?
 *
 * In this case we'd end up with
 *
 * (a=1)
 *
 * as we'd consider (b=2) implied thanks to the rule, rendering the whole
 * OR clause valid.
 */
static List *
clauselist_apply_dependencies(PlannerInfo *root, List *clauses,
							  Index relid, List *stats)
{
	Bitmapset  *clause_attnums = NULL;
	Bitmapset  *reduced_attnums = NULL;

	/*
	 * Is there at least one statistics with functional dependencies? If not,
	 * return the original clauses right away.
	 *
	 * XXX Isn't this a bit pointless, thanks to exactly the same check in
	 * clauselist_selectivity()? Can we trigger the condition here?
	 */
	if (!has_stats(stats, MV_CLAUSE_TYPE_FDEP))
		return clauses;

	/* collect attnums from clauses compatible with dependencies (equality) */
	clause_attnums = collect_mv_attnums(clauses, relid, MV_CLAUSE_TYPE_FDEP);

	/* decide which attnums may be eliminated */
	reduced_attnums = fdeps_reduce_attnums(stats, clause_attnums);

	/*
	 * Walk through the clauses, and see which other clauses we may reduce.
	 */
	clauses = fdeps_reduce_clauses(clauses, relid, reduced_attnums);

	bms_free(clause_attnums);
	bms_free(reduced_attnums);

	return clauses;
}

/*
 * Check that there are stats with at least one of the requested types.
 */
static bool
stats_type_matches(MVStatisticInfo *stat, int type)
{
	if ((type & MV_CLAUSE_TYPE_FDEP) && stat->deps_built)
		return true;

	if ((type & MV_CLAUSE_TYPE_MCV) && stat->mcv_built)
		return true;

	if ((type & MV_CLAUSE_TYPE_HIST) && stat->hist_built)
		return true;

	if ((type & MV_CLAUSE_TYPE_NDIST) && stat->ndist_built)
		return true;

	return false;
}

/*
 * Check that there are stats with at least one of the requested types.
 */
static bool
has_stats(List *stats, int type)
{
	ListCell   *s;

	foreach(s, stats)
	{
		MVStatisticInfo *stat = (MVStatisticInfo *) lfirst(s);

		/* terminate if we've found at least one matching statistics */
		if (stats_type_matches(stat, type))
			return true;
	}

	return false;
}

/*
 * Lookups stats for a given baserel.
 */
static List *
find_stats(PlannerInfo *root, Index relid)
{
	Assert(root->simple_rel_array[relid] != NULL);

	return root->simple_rel_array[relid]->mvstatlist;
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
 * 1) mark all items as 'match'
 * 2) walk through all the clauses
 * 3) for a particular clause, walk through all the items
 * 4) skip items that are already 'no match'
 * 5) check clause for items that still match
 * 6) sum frequencies for items to get selectivity
 *
 * The function also returns the frequency of the least frequent item
 * on the MCV list, which may be useful for clamping estimate from the
 * histogram (all items not present in the MCV list are less frequent).
 * This however seems useful only for cases with conditions on all
 * attributes.
 *
 * TODO: This only handles AND-ed clauses, but it might work for OR-ed
 * lists too - it just needs to reverse the logic a bit. I.e. start
 * with 'no match' for all items, and mark the items as a match
 * as the clauses are processed (and skip items that are 'match').
 */
static Selectivity
clauselist_mv_selectivity_mcvlist(PlannerInfo *root, MVStatisticInfo *mvstats,
								  List *clauses, List *conditions, bool is_or,
								  bool *fullmatch, Selectivity *lowsel)
{
	int			i;
	Selectivity s = 0.0;
	Selectivity t = 0.0;
	Selectivity u = 0.0;

	MCVList		mcvlist = NULL;

	int			nmatches = 0;
	int			nconditions = 0;

	/* match/mismatch bitmap for each MCV item */
	char	   *matches = NULL;
	char	   *condition_matches = NULL;

	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);

	/* there's no MCV list built yet */
	if (!mvstats->mcv_built)
		return 0.0;

	mcvlist = load_mv_mcvlist(mvstats->mvoid);

	Assert(mcvlist != NULL);
	Assert(mcvlist->nitems > 0);

	/* number of matching MCV items */
	nmatches = mcvlist->nitems;
	nconditions = mcvlist->nitems;

	/*
	 * Bitmap of bucket matches (mismatch, partial, full).
	 *
	 * For AND clauses all buckets match (and we'll eliminate them). For OR
	 * clauses no  buckets match (and we'll add them).
	 *
	 * We only need to do the memset for AND clauses (for OR clauses it's
	 * already set correctly by the palloc0).
	 */
	matches = palloc0(sizeof(char) * nmatches);

	if (!is_or)					/* AND-clause */
		memset(matches, MVSTATS_MATCH_FULL, sizeof(char) * nmatches);

	/* Conditions are treated as AND clause, so match by default. */
	condition_matches = palloc0(sizeof(char) * nconditions);
	memset(condition_matches, MVSTATS_MATCH_FULL, sizeof(char) * nconditions);

	/*
	 * build the match bitmap for the conditions (conditions are always
	 * connected by AND)
	 */
	if (conditions != NIL)
		nconditions = update_match_bitmap_mcvlist(root, conditions,
												  mvstats->stakeys, mcvlist,
											  nconditions, condition_matches,
												  lowsel, fullmatch, false);

	/*
	 * build the match bitmap for the estimated clauses
	 *
	 * TODO This evaluates the clauses for all MCV items, even those ruled out
	 * by the conditions. The final result should be the same, but it might be
	 * faster.
	 */
	nmatches = update_match_bitmap_mcvlist(root, clauses,
										   mvstats->stakeys, mcvlist,
										   ((is_or) ? 0 : nmatches), matches,
										   lowsel, fullmatch, is_or);

	/* sum frequencies for all the matching MCV items */
	for (i = 0; i < mcvlist->nitems; i++)
	{
		/*
		 * Find out what part of the data is covered by the MCV list, so that
		 * we can 'scale' the selectivity properly (e.g. when only 50% of the
		 * sample items got into the MCV, and the rest is either in a
		 * histogram, or not covered by stats).
		 *
		 * TODO This might be handled by keeping a global "frequency" for the
		 * whole list, which might save us a bit of time spent on accessing
		 * the not-matching part of the MCV list. Although it's likely in a
		 * cache, so it's very fast.
		 */
		u += mcvlist->items[i]->frequency;

		/* skit MCV items not matching the conditions */
		if (condition_matches[i] == MVSTATS_MATCH_NONE)
			continue;

		if (matches[i] != MVSTATS_MATCH_NONE)
			s += mcvlist->items[i]->frequency;

		t += mcvlist->items[i]->frequency;
	}

	pfree(matches);
	pfree(condition_matches);
	pfree(mcvlist);

	/* no condition matches */
	if (t == 0.0)
		return (Selectivity) 0.0;

	return (s / t) * u;
}

/*
 * Evaluate clauses using the MCV list, and update the match bitmap.
 *
 * The bitmap may be already partially set, so this is really a way to
 * combine results of several clause lists - either when computing
 * conditional probability P(A|B) or a combination of AND/OR clauses.
 *
 * TODO: This works with 'bitmap' where each bit is represented as a char,
 * which is slightly wasteful. Instead, we could use a regular
 * bitmap, reducing the size to ~1/8. Another thing is merging the
 * bitmaps using & and |, which might be faster than min/max.
 */
static int
update_match_bitmap_mcvlist(PlannerInfo *root, List *clauses,
							int2vector *stakeys, MCVList mcvlist,
							int nmatches, char *matches,
							Selectivity *lowsel, bool *fullmatch,
							bool is_or)
{
	int			i;
	ListCell   *l;

	Bitmapset  *eqmatches = NULL;		/* attributes with equality matches */

	/* The bitmap may be partially built. */
	Assert(nmatches >= 0);
	Assert(nmatches <= mcvlist->nitems);
	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);
	Assert(mcvlist != NULL);
	Assert(mcvlist->nitems > 0);

	/* No possible matches (only works for AND-ded clauses) */
	if (((nmatches == 0) && (!is_or)) ||
		((nmatches == mcvlist->nitems) && is_or))
		return nmatches;

	/*
	 * find the lowest frequency in the MCV list
	 *
	 * We need to do that here, because we do various tricks in the following
	 * code - skipping items already ruled out, etc.
	 *
	 * XXX A loop is necessary because the MCV list is not sorted by
	 * frequency.
	 */
	*lowsel = 1.0;
	for (i = 0; i < mcvlist->nitems; i++)
	{
		MCVItem		item = mcvlist->items[i];

		if (item->frequency < *lowsel)
			*lowsel = item->frequency;
	}

	/*
	 * Loop through the list of clauses, and for each of them evaluate all the
	 * MCV items not yet eliminated by the preceding clauses.
	 */
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);

		/* if it's a RestrictInfo, then extract the clause */
		if (IsA(clause, RestrictInfo))
			clause = (Node *) ((RestrictInfo *) clause)->clause;

		/* if there are no remaining matches possible, we can stop */
		if (((nmatches == 0) && (!is_or)) ||
			((nmatches == mcvlist->nitems) && is_or))
			break;

		/* it's either OpClause, or NullTest */
		if (is_opclause(clause))
		{
			OpExpr	   *expr = (OpExpr *) clause;
			bool		varonleft = true;
			bool		ok;
			FmgrInfo	opproc;

			/* get procedure computing operator selectivity */
			RegProcedure oprrest = get_oprrest(expr->opno);

			fmgr_info(get_opcode(expr->opno), &opproc);

			ok = (NumRelids(clause) == 1) &&
				(is_pseudo_constant_clause(lsecond(expr->args)) ||
				 (varonleft = false,
				  is_pseudo_constant_clause(linitial(expr->args))));

			if (ok)
			{

				FmgrInfo	gtproc;
				Var		   *var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);
				Const	   *cst = (varonleft) ? lsecond(expr->args) : linitial(expr->args);
				bool		isgt = (!varonleft);

				TypeCacheEntry *typecache
				= lookup_type_cache(var->vartype, TYPECACHE_GT_OPR);

				/* FIXME proper matching attribute to dimension */
				int			idx = mv_get_index(var->varattno, stakeys);

				fmgr_info(get_opcode(typecache->gt_opr), &gtproc);

				/*
				 * Walk through the MCV items and evaluate the current clause.
				 * We can skip items that were already ruled out, and
				 * terminate if there are no remaining MCV items that might
				 * possibly match.
				 */
				for (i = 0; i < mcvlist->nitems; i++)
				{
					bool		mismatch = false;
					MCVItem		item = mcvlist->items[i];

					/*
					 * If there are no more matches (AND) or no remaining
					 * unmatched items (OR), we can stop processing this
					 * clause.
					 */
					if (((nmatches == 0) && (!is_or)) ||
						((nmatches == mcvlist->nitems) && is_or))
						break;

					/*
					 * For AND-lists, we can also mark NULL items as 'no
					 * match' (and then skip them). For OR-lists this is not
					 * possible.
					 */
					if ((!is_or) && item->isnull[idx])
						matches[i] = MVSTATS_MATCH_NONE;

					/* skip MCV items that were already ruled out */
					if ((!is_or) && (matches[i] == MVSTATS_MATCH_NONE))
						continue;
					else if (is_or && (matches[i] == MVSTATS_MATCH_FULL))
						continue;

					switch (oprrest)
					{
						case F_EQSEL:

							/*
							 * We don't care about isgt in equality, because
							 * it does not matter whether it's (var = const)
							 * or (const = var).
							 */
							mismatch = !DatumGetBool(FunctionCall2Coll(&opproc,
													   DEFAULT_COLLATION_OID,
															 cst->constvalue,
														 item->values[idx]));

							if (!mismatch)
								eqmatches = bms_add_member(eqmatches, idx);

							break;

						case F_SCALARLTSEL:		/* column < constant */
						case F_SCALARGTSEL:		/* column > constant */

							/*
							 * First check whether the constant is below the
							 * lower boundary (in that case we can skip the
							 * bucket, because there's no overlap).
							 */
							mismatch = DatumGetBool(FunctionCall2Coll(&opproc,
													   DEFAULT_COLLATION_OID,
															 cst->constvalue,
														 item->values[idx]));

							/* invert the result if isgt=true */
							mismatch = (isgt) ? (!mismatch) : mismatch;
							break;
					}

					/*
					 * XXX The conditions on matches[i] are not needed, as we
					 * skip MCV items that can't become true/false, depending
					 * on the current flag. See beginning of the loop over MCV
					 * items.
					 */

					if ((is_or) && (matches[i] == MVSTATS_MATCH_NONE) && (!mismatch))
					{
						/* OR - was MATCH_NONE, but will be MATCH_FULL */
						matches[i] = MVSTATS_MATCH_FULL;
						++nmatches;
						continue;
					}
					else if ((!is_or) && (matches[i] == MVSTATS_MATCH_FULL) && mismatch)
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
			NullTest   *expr = (NullTest *) clause;
			Var		   *var = (Var *) (expr->arg);

			/* FIXME proper matching attribute to dimension */
			int			idx = mv_get_index(var->varattno, stakeys);

			/*
			 * Walk through the MCV items and evaluate the current clause. We
			 * can skip items that were already ruled out, and terminate if
			 * there are no remaining MCV items that might possibly match.
			 */
			for (i = 0; i < mcvlist->nitems; i++)
			{
				MCVItem		item = mcvlist->items[i];

				/*
				 * if there are no more matches, we can stop processing this
				 * clause
				 */
				if (nmatches == 0)
					break;

				/* skip MCV items that were already ruled out */
				if (matches[i] == MVSTATS_MATCH_NONE)
					continue;

				/* if the clause mismatches the MCV item, set it as MATCH_NONE */
				if (((expr->nulltesttype == IS_NULL) && (!item->isnull[idx])) ||
				((expr->nulltesttype == IS_NOT_NULL) && (item->isnull[idx])))
				{
					matches[i] = MVSTATS_MATCH_NONE;
					--nmatches;
				}
			}
		}
		else if (or_clause(clause) || and_clause(clause) || not_clause(clause))
		{
			/*
			 * AND/OR clause, with all clauses compatible with the selected MV
			 * stat
			 */

			int			i;
			List	   *tmp_clauses = ((BoolExpr *) clause)->args;

			/* match/mismatch bitmap for each MCV item */
			int			tmp_nmatches = 0;
			char	   *tmp_matches = NULL;

			Assert(tmp_clauses != NIL);
			Assert((list_length(tmp_clauses) >= 2) || (not_clause(clause) && (list_length(tmp_clauses) == 1)));

			/* number of matching MCV items */
			tmp_nmatches = (or_clause(clause)) ? 0 : mcvlist->nitems;

			/* by default none of the MCV items matches the clauses */
			tmp_matches = palloc0(sizeof(char) * mcvlist->nitems);

			/* AND (and NOT) clauses assume everything matches, initially */
			if (!or_clause(clause))
				memset(tmp_matches, MVSTATS_MATCH_FULL, sizeof(char) * mcvlist->nitems);

			/* build the match bitmap for the OR-clauses */
			tmp_nmatches = update_match_bitmap_mcvlist(root, tmp_clauses,
													   stakeys, mcvlist,
												   tmp_nmatches, tmp_matches,
									   lowsel, fullmatch, or_clause(clause));

			/* merge the bitmap into the existing one */
			for (i = 0; i < mcvlist->nitems; i++)
			{
				/*
				 * if this is a NOT clause, we need to invert the results
				 * first
				 */
				if (not_clause(clause))
					tmp_matches[i] = (MVSTATS_MATCH_FULL - tmp_matches[i]);

				/*
				 * Merge the result into the bitmap (Min for AND, Max for OR).
				 *
				 * FIXME this does not decrease the number of matches
				 */
				UPDATE_RESULT(matches[i], tmp_matches[i], is_or);
			}

			pfree(tmp_matches);

		}
		else
			elog(ERROR, "unknown clause type: %d", clause->type);
	}

	/*
	 * If all the columns were matched by equality, it's a full match. In this
	 * case there can be just a single MCV item, matching the clause (if there
	 * were two, both would match the other one).
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
 * 1) mark all buckets as 'full match'
 * 2) walk through all the clauses
 * 3) for a particular clause, walk through all the buckets
 * 4) skip buckets that are already 'no match'
 * 5) check clause for buckets that still match (at least partially)
 * 6) sum frequencies for buckets to get selectivity
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
 * TODO: This might use a similar shortcut to MCV lists - count buckets
 * marked as partial/full match, and terminate once this drop to 0.
 * Not sure if it's really worth it - for MCV lists a situation like
 * this is not uncommon, but for histograms it's not that clear.
 */
static Selectivity
clauselist_mv_selectivity_histogram(PlannerInfo *root, MVStatisticInfo *mvstats,
								 List *clauses, List *conditions, bool is_or)
{
	int			i;
	Selectivity s = 0.0;
	Selectivity t = 0.0;
	Selectivity u = 0.0;

	int			nmatches = 0;
	int			nconditions = 0;
	char	   *matches = NULL;
	char	   *condition_matches = NULL;

	MVSerializedHistogram mvhist = NULL;

	/* there's no histogram */
	if (!mvstats->hist_built)
		return 0.0;

	/* There may be no histogram in the stats (check hist_built flag) */
	mvhist = load_mv_histogram(mvstats->mvoid);

	Assert(mvhist != NULL);
	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);

	nmatches = mvhist->nbuckets;
	nconditions = mvhist->nbuckets;

	/*
	 * Bitmap of bucket matches (mismatch, partial, full).
	 *
	 * For AND clauses all buckets match (and we'll eliminate them). For OR
	 * clauses no  buckets match (and we'll add them).
	 *
	 * We only need to do the memset for AND clauses (for OR clauses it's
	 * already set correctly by the palloc0).
	 */
	matches = palloc0(sizeof(char) * nmatches);

	if (!is_or)					/* AND-clause */
		memset(matches, MVSTATS_MATCH_FULL, sizeof(char) * nmatches);

	/* Conditions are treated as AND clause, so match by default. */
	condition_matches = palloc0(sizeof(char) * nconditions);
	memset(condition_matches, MVSTATS_MATCH_FULL, sizeof(char) * nconditions);

	/*
	 * build the match bitmap for the conditions (conditions are always
	 * connected by AND)
	 */
	if (conditions != NIL)
		update_match_bitmap_histogram(root, conditions,
									  mvstats->stakeys, mvhist,
									  nconditions, condition_matches, false);

	/*
	 * build the match bitmap for the estimated clauses
	 *
	 * TODO This evaluates the clauses for all buckets, even those ruled out
	 * by the conditions. The final result should be the same, but it might be
	 * faster.
	 */
	update_match_bitmap_histogram(root, clauses,
								  mvstats->stakeys, mvhist,
								  ((is_or) ? 0 : nmatches), matches,
								  is_or);

	/* now, walk through the buckets and sum the selectivities */
	for (i = 0; i < mvhist->nbuckets; i++)
	{
		float		coeff = 1.0;

		/*
		 * Find out what part of the data is covered by the histogram, so that
		 * we can 'scale' the selectivity properly (e.g. when only 50% of the
		 * sample got into the histogram, and the rest is in a MCV list).
		 *
		 * TODO This might be handled by keeping a global "frequency" for the
		 * whole histogram, which might save us some time spent accessing the
		 * not-matching part of the histogram. Although it's likely in a
		 * cache, so it's very fast.
		 */
		u += mvhist->buckets[i]->ntuples;

		/* skip buckets not matching the conditions */
		if (condition_matches[i] == MVSTATS_MATCH_NONE)
			continue;
		else if (condition_matches[i] == MVSTATS_MATCH_PARTIAL)
			coeff = 0.5;

		t += coeff * mvhist->buckets[i]->ntuples;

		if (matches[i] == MVSTATS_MATCH_FULL)
			s += coeff * mvhist->buckets[i]->ntuples;
		else if (matches[i] == MVSTATS_MATCH_PARTIAL)

			/*
			 * TODO If both conditions and clauses match partially, this will
			 * use 0.25 match - not sure if that's the right thing solution,
			 * but seems about right.
			 */
			s += coeff * 0.5 * mvhist->buckets[i]->ntuples;
	}

#ifdef DEBUG_MVHIST
	debug_histogram_matches(mvhist, matches);
#endif

	/* release the allocated bitmap and deserialized histogram */
	pfree(matches);
	pfree(condition_matches);
	pfree(mvhist);

	/* no condition matches */
	if (t == 0.0)
		return (Selectivity) 0.0;

	return (s / t) * u;
}

/* cached result of bucket boundary comparison for a single dimension */

#define HIST_CACHE_NOT_FOUND		0x00
#define HIST_CACHE_FALSE			0x01
#define HIST_CACHE_TRUE				0x03
#define HIST_CACHE_MASK				0x02

static char
bucket_contains_value(FmgrInfo ltproc, Datum constvalue,
					  Datum min_value, Datum max_value,
					  int min_index, int max_index,
					  bool min_include, bool max_include,
					  char *callcache)
{
	bool		a,
				b;

	char		min_cached = callcache[min_index];
	char		max_cached = callcache[max_index];

	/*
	 * First some quick checks on equality - if any of the boundaries equals,
	 * we have a partial match (so no need to call the comparator).
	 */
	if (((min_value == constvalue) && (min_include)) ||
		((max_value == constvalue) && (max_include)))
		return MVSTATS_MATCH_PARTIAL;

	/* Keep the values 0/1 because of the XOR at the end. */
	a = ((min_cached & HIST_CACHE_MASK) >> 1);
	b = ((max_cached & HIST_CACHE_MASK) >> 1);

	/*
	 * If result for the bucket lower bound not in cache, evaluate the
	 * function and store the result in the cache.
	 */
	if (!min_cached)
	{
		a = DatumGetBool(FunctionCall2Coll(&ltproc,
										   DEFAULT_COLLATION_OID,
										   constvalue, min_value));
		/* remember the result */
		callcache[min_index] = (a) ? HIST_CACHE_TRUE : HIST_CACHE_FALSE;
	}

	/* And do the same for the upper bound. */
	if (!max_cached)
	{
		b = DatumGetBool(FunctionCall2Coll(&ltproc,
										   DEFAULT_COLLATION_OID,
										   constvalue, max_value));
		/* remember the result */
		callcache[max_index] = (b) ? HIST_CACHE_TRUE : HIST_CACHE_FALSE;
	}

	return (a ^ b) ? MVSTATS_MATCH_PARTIAL : MVSTATS_MATCH_NONE;
}

static char
bucket_is_smaller_than_value(FmgrInfo opproc, Datum constvalue,
							 Datum min_value, Datum max_value,
							 int min_index, int max_index,
							 bool min_include, bool max_include,
							 char *callcache, bool isgt)
{
	char		min_cached = callcache[min_index];
	char		max_cached = callcache[max_index];

	/* Keep the values 0/1 because of the XOR at the end. */
	bool		a = ((min_cached & HIST_CACHE_MASK) >> 1);
	bool		b = ((max_cached & HIST_CACHE_MASK) >> 1);

	if (!min_cached)
	{
		a = DatumGetBool(FunctionCall2Coll(&opproc,
										   DEFAULT_COLLATION_OID,
										   min_value,
										   constvalue));
		/* remember the result */
		callcache[min_index] = (a) ? HIST_CACHE_TRUE : HIST_CACHE_FALSE;
	}

	if (!max_cached)
	{
		b = DatumGetBool(FunctionCall2Coll(&opproc,
										   DEFAULT_COLLATION_OID,
										   max_value,
										   constvalue));
		/* remember the result */
		callcache[max_index] = (b) ? HIST_CACHE_TRUE : HIST_CACHE_FALSE;
	}

	/*
	 * Now, we need to combine both results into the final answer, and we need
	 * to be careful about the 'isgt' variable which kinda inverts the
	 * meaning.
	 *
	 * First, we handle the case when each boundary returns different results.
	 * In that case the outcome can only be 'partial' match.
	 */
	if (a != b)
		return MVSTATS_MATCH_PARTIAL;

	/*
	 * When the results are the same, then it depends on the 'isgt' value.
	 * There are four options:
	 *
	 * isgt=false a=b=true	=> full match isgt=false a=b=false => empty
	 * isgt=true  a=b=true	=> empty isgt=true	a=b=false => full match
	 *
	 * We'll cheat a bit, because we know that (a=b) so we'll use just one of
	 * them.
	 */
	if (isgt)
		return (!a) ? MVSTATS_MATCH_FULL : MVSTATS_MATCH_NONE;
	else
		return (a) ? MVSTATS_MATCH_FULL : MVSTATS_MATCH_NONE;
}

/*
 * Evaluate clauses using the histogram, and update the match bitmap.
 *
 * The bitmap may be already partially set, so this is really a way to
 * combine results of several clause lists - either when computing
 * conditional probability P(A|B) or a combination of AND/OR clauses.
 *
 * Note: This is not a simple bitmap in the sense that there are more
 * than two possible values for each item - no match, partial
 * match and full match. So we need 2 bits per item.
 *
 * TODO: This works with 'bitmap' where each item is represented as a
 * char, which is slightly wasteful. Instead, we could use a bitmap
 * with 2 bits per item, reducing the size to ~1/4. By using values
 * 0, 1 and 3 (instead of 0, 1 and 2), the operations (merging etc.)
 * might be performed just like for simple bitmap by using & and |,
 * which might be faster than min/max.
 */
static int
update_match_bitmap_histogram(PlannerInfo *root, List *clauses,
							  int2vector *stakeys,
							  MVSerializedHistogram mvhist,
							  int nmatches, char *matches,
							  bool is_or)
{
	int			i;
	ListCell   *l;

	/*
	 * Used for caching function calls, only once per deduplicated value.
	 *
	 * We know may have up to (2 * nbuckets) values per dimension. It's
	 * probably overkill, but let's allocate that once for all clauses, to
	 * minimize overhead.
	 *
	 * Also, we only need two bits per value, but this allocates byte per
	 * value. Might be worth optimizing.
	 *
	 * 0x00 - not yet called 0x01 - called, result is 'false' 0x03 - called,
	 * result is 'true'
	 */
	char	   *callcache = palloc(mvhist->nbuckets);

	Assert(mvhist != NULL);
	Assert(mvhist->nbuckets > 0);
	Assert(nmatches >= 0);
	Assert(nmatches <= mvhist->nbuckets);

	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);

	/* loop through the clauses and do the estimation */
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);

		/* if it's a RestrictInfo, then extract the clause */
		if (IsA(clause, RestrictInfo))
			clause = (Node *) ((RestrictInfo *) clause)->clause;

		/* it's either OpClause, or NullTest */
		if (is_opclause(clause))
		{
			OpExpr	   *expr = (OpExpr *) clause;
			bool		varonleft = true;
			bool		ok;

			FmgrInfo	opproc; /* operator */

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
				RegProcedure oprrest = get_oprrest(expr->opno);

				Var		   *var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);
				Const	   *cst = (varonleft) ? lsecond(expr->args) : linitial(expr->args);
				bool		isgt = (!varonleft);

				TypeCacheEntry *typecache
				= lookup_type_cache(var->vartype, TYPECACHE_LT_OPR);

				/* lookup dimension for the attribute */
				int			idx = mv_get_index(var->varattno, stakeys);

				fmgr_info(get_opcode(typecache->lt_opr), &ltproc);

				/*
				 * Check this for all buckets that still have "true" in the
				 * bitmap
				 *
				 * We already know the clauses use suitable operators (because
				 * that's how we filtered them).
				 */
				for (i = 0; i < mvhist->nbuckets; i++)
				{
					char		res = MVSTATS_MATCH_NONE;

					MVSerializedBucket bucket = mvhist->buckets[i];

					/* histogram boundaries */
					Datum		minval,
								maxval;
					bool		mininclude,
								maxinclude;
					int			minidx,
								maxidx;

					/*
					 * For AND-lists, we can also mark NULL buckets as 'no
					 * match' (and then skip them). For OR-lists this is not
					 * possible.
					 */
					if ((!is_or) && bucket->nullsonly[idx])
						matches[i] = MVSTATS_MATCH_NONE;

					/*
					 * Skip buckets that were already eliminated - this is
					 * impotant considering how we update the info (we only
					 * lower the match). We can't really do anything about the
					 * MATCH_PARTIAL buckets.
					 */
					if ((!is_or) && (matches[i] == MVSTATS_MATCH_NONE))
						continue;
					else if (is_or && (matches[i] == MVSTATS_MATCH_FULL))
						continue;

					/* lookup the values and cache of function calls */
					minidx = bucket->min[idx];
					maxidx = bucket->max[idx];

					minval = mvhist->values[idx][bucket->min[idx]];
					maxval = mvhist->values[idx][bucket->max[idx]];

					mininclude = bucket->min_inclusive[idx];
					maxinclude = bucket->max_inclusive[idx];

					/*
					 * TODO Maybe it's possible to add here a similar
					 * optimization as for the MCV lists:
					 *
					 * (nmatches == 0) && AND-list => all eliminated (FALSE)
					 * (nmatches == N) && OR-list  => all eliminated (TRUE)
					 *
					 * But it's more complex because of the partial matches.
					 */

					/*
					 * If it's not a "<" or ">" or "=" operator, just ignore
					 * the clause. Otherwise note the relid and attnum for the
					 * variable.
					 *
					 * TODO I'm really unsure the handling of 'isgt' flag
					 * (that is, clauses with reverse order of
					 * variable/constant) is correct. I wouldn't be surprised
					 * if there was some mixup. Using the lt/gt operators
					 * instead of messing with the opproc could make it
					 * simpler. It would however be using a different operator
					 * than the query, although it's not any shadier than
					 * using the selectivity function as is done currently.
					 */
					switch (oprrest)
					{
						case F_SCALARLTSEL:		/* Var < Const */
						case F_SCALARGTSEL:		/* Var > Const */

							res = bucket_is_smaller_than_value(opproc, cst->constvalue,
															   minval, maxval,
															   minidx, maxidx,
													  mininclude, maxinclude,
															callcache, isgt);
							break;

						case F_EQSEL:

							/*
							 * We only check whether the value is within the
							 * bucket, using the lt operator, and we also
							 * check for equality with the boundaries.
							 */

							res = bucket_contains_value(ltproc, cst->constvalue,
														minval, maxval,
														minidx, maxidx,
													  mininclude, maxinclude,
														callcache);
							break;
					}

					UPDATE_RESULT(matches[i], res, is_or);

				}
			}
		}
		else if (IsA(clause, NullTest))
		{
			NullTest   *expr = (NullTest *) clause;
			Var		   *var = (Var *) (expr->arg);

			/* FIXME proper matching attribute to dimension */
			int			idx = mv_get_index(var->varattno, stakeys);

			/*
			 * Walk through the buckets and evaluate the current clause. We
			 * can skip items that were already ruled out, and terminate if
			 * there are no remaining buckets that might possibly match.
			 */
			for (i = 0; i < mvhist->nbuckets; i++)
			{
				MVSerializedBucket bucket = mvhist->buckets[i];

				/*
				 * Skip buckets that were already eliminated - this is
				 * impotant considering how we update the info (we only lower
				 * the match)
				 */
				if ((!is_or) && (matches[i] == MVSTATS_MATCH_NONE))
					continue;
				else if (is_or && (matches[i] == MVSTATS_MATCH_FULL))
					continue;

				/* if the clause mismatches the bucket, set it as MATCH_NONE */
				if ((expr->nulltesttype == IS_NULL)
					&& (!bucket->nullsonly[idx]))
					UPDATE_RESULT(matches[i], MVSTATS_MATCH_NONE, is_or);

				else if ((expr->nulltesttype == IS_NOT_NULL) &&
						 (bucket->nullsonly[idx]))
					UPDATE_RESULT(matches[i], MVSTATS_MATCH_NONE, is_or);
			}
		}
		else if (or_clause(clause) || and_clause(clause) || not_clause(clause))
		{
			/*
			 * AND/OR clause, with all clauses compatible with the selected MV
			 * stat
			 */

			int			i;
			List	   *tmp_clauses = ((BoolExpr *) clause)->args;

			/* match/mismatch bitmap for each bucket */
			int			tmp_nmatches = 0;
			char	   *tmp_matches = NULL;

			Assert(tmp_clauses != NIL);
			Assert((list_length(tmp_clauses) >= 2) || (not_clause(clause) && (list_length(tmp_clauses) == 1)));

			/* number of matching buckets */
			tmp_nmatches = (or_clause(clause)) ? 0 : mvhist->nbuckets;

			/* by default none of the buckets matches the clauses (OR clause) */
			tmp_matches = palloc0(sizeof(char) * mvhist->nbuckets);

			/* but AND (and NOT) clauses assume everything matches, initially */
			if (!or_clause(clause))
				memset(tmp_matches, MVSTATS_MATCH_FULL, sizeof(char) * mvhist->nbuckets);

			/* build the match bitmap for the OR-clauses */
			tmp_nmatches = update_match_bitmap_histogram(root, tmp_clauses,
														 stakeys, mvhist,
							   tmp_nmatches, tmp_matches, or_clause(clause));

			/* merge the bitmap into the existing one */
			for (i = 0; i < mvhist->nbuckets; i++)
			{
				/*
				 * if this is a NOT clause, we need to invert the results
				 * first
				 */
				if (not_clause(clause))
					tmp_matches[i] = (MVSTATS_MATCH_FULL - tmp_matches[i]);

				/*
				 * Merge the result into the bitmap (Min for AND, Max for OR).
				 *
				 * FIXME this does not decrease the number of matches
				 */
				UPDATE_RESULT(matches[i], tmp_matches[i], is_or);
			}

			pfree(tmp_matches);
		}
		else
			elog(ERROR, "unknown clause type: %d", clause->type);
	}

	pfree(callcache);

	return nmatches;
}
