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

static bool clause_is_mv_compatible(Node *clause, Index relid, Bitmapset **attnums,
						int type);

static Bitmapset *collect_mv_attnums(List *clauses, Index relid, int type);

static int	count_mv_attnums(List *clauses, Index relid, int type);

static int	count_varnos(List *clauses, Index *relid);

static List *clauselist_apply_dependencies(PlannerInfo *root, List *clauses,
							  Index relid, List *stats);

static MVStatisticInfo *choose_mv_statistics(List *mvstats, Bitmapset *attnums);

static List *clauselist_mv_split(PlannerInfo *root, Index relid,
					List *clauses, List **mvclauses,
					MVStatisticInfo *mvstats, int types);

static Selectivity clauselist_mv_selectivity(PlannerInfo *root,
						  List *clauses, MVStatisticInfo *mvstats);

static Selectivity clauselist_mv_selectivity_mcvlist(PlannerInfo *root,
								  List *clauses, MVStatisticInfo *mvstats,
								  bool *fullmatch, Selectivity *lowsel);

static int update_match_bitmap_mcvlist(PlannerInfo *root, List *clauses,
							int2vector *stakeys, MCVList mcvlist,
							int nmatches, char *matches,
							Selectivity *lowsel, bool *fullmatch,
							bool is_or);

static bool has_stats(List *stats, int type);

static List *find_stats(PlannerInfo *root, Index relid);

static bool stats_type_matches(MVStatisticInfo *stat, int type);


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
 * list of clauses using the multivariate MCV list.
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
					   SpecialJoinInfo *sjinfo)
{
	Selectivity s1 = 1.0;
	RangeQueryClause *rqlist = NULL;
	ListCell   *l;

	/* processing mv stats */
	Oid			relid = InvalidOid;

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
	 * so just go directly to clause_selectivity().
	 */
	if (list_length(clauses) == 1)
		return clause_selectivity(root, (Node *) linitial(clauses),
								  varRelid, jointype, sjinfo);

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
	if (has_stats(stats, MV_CLAUSE_TYPE_MCV) &&
		(count_mv_attnums(clauses, relid, MV_CLAUSE_TYPE_MCV) >= 2))
	{
		/* collect attributes from the compatible conditions */
		Bitmapset  *mvattnums = collect_mv_attnums(clauses, relid, MV_CLAUSE_TYPE_MCV);

		/* and search for the statistic covering the most attributes */
		MVStatisticInfo *mvstat = choose_mv_statistics(stats, mvattnums);

		if (mvstat != NULL)		/* we have a matching stats */
		{
			/* clauses compatible with multi-variate stats */
			List	   *mvclauses = NIL;

			/* split the clauselist into regular and mv-clauses */
			clauses = clauselist_mv_split(root, relid, clauses, &mvclauses,
										  mvstat, MV_CLAUSE_TYPE_MCV);

			/* we've chosen the histogram to match the clauses */
			Assert(mvclauses != NIL);

			/* compute the multivariate stats */
			s1 *= clauselist_mv_selectivity(root, mvclauses, mvstat);
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
clauselist_mv_selectivity(PlannerInfo *root, List *clauses, MVStatisticInfo *mvstats)
{
	bool		fullmatch = false;

	/*
	 * Lowest frequency in the MCV list (may be used as an upper bound for
	 * full equality conditions that did not match any MCV item).
	 */
	Selectivity mcv_low = 0.0;

	/*
	 * TODO: Evaluate simple 1D selectivities, use the smallest one as an
	 * upper bound, product as lower bound, and sort the clauses in ascending
	 * order by selectivity (to optimize the MCV/histogram evaluation).
	 */

	/* Evaluate the MCV selectivity */
	return clauselist_mv_selectivity_mcvlist(root, clauses, mvstats,
											 &fullmatch, &mcv_low);
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
		if (attnums != NULL)
			pfree(attnums);
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

/*
 * We're looking for statistics matching at least 2 attributes, referenced in
 * clauses compatible with multivariate statistics. The current selection
 * criteria is very simple - we choose the statistics referencing the most
 * attributes.
 *
 * If there are multiple statistics referencing the same number of columns
 * (from the clauses), the one with less source columns (as listed in the
 * ADD STATISTICS when creating the statistics) wins. Else the first one wins.
 *
 * This is a very simple criteria, and has several weaknesses:
 *
 * (a) does not consider the accuracy of the statistics
 *
 *	   If there are two histograms built on the same set of columns, but one
 *	   has 100 buckets and the other one has 1000 buckets (thus likely
 *	   providing better estimates), this is not currently considered.
 *
 * (b) does not consider the type of statistics
 *
 *	   If there are three statistics - one containing just a MCV list, another
 *	   one with just a histogram and a third one with both, we treat them equally.
 *
 * (c) does not consider the number of clauses
 *
 *	   As explained, only the number of referenced attributes counts, so if
 *	   there are multiple clauses on a single attribute, this still counts as
 *	   a single attribute.
 *
 * (d) does not consider type of condition
 *
 *	   Some clauses may work better with some statistics - for example equality
 *	   clauses probably work better with MCV lists than with histograms. But
 *	   IS [NOT] NULL conditions may often work better with histograms (thanks
 *	   to NULL-buckets).
 *
 * So for example with five WHERE conditions
 *
 *	   WHERE (a = 1) AND (b = 1) AND (c = 1) AND (d = 1) AND (e = 1)
 *
 * and statistics on (a,b), (a,b,e) and (a,b,c,d), the last one will be selected
 * as it references the most columns.
 *
 * Once we have selected the multivariate statistics, we split the list of
 * clauses into two parts - conditions that are compatible with the selected
 * stats, and conditions are estimated using simple statistics.
 *
 * From the example above, conditions
 *
 *	   (a = 1) AND (b = 1) AND (c = 1) AND (d = 1)
 *
 * will be estimated using the multivariate statistics (a,b,c,d) while the last
 * condition (e = 1) will get estimated using the regular ones.
 *
 * There are various alternative selection criteria (e.g. counting conditions
 * instead of just referenced attributes), but eventually the best option should
 * be to combine multiple statistics. But that's much harder to do correctly.
 *
 * TODO: Select multiple statistics and combine them when computing the estimate.
 *
 * TODO: This will probably have to consider compatibility of clauses, because
 * 'dependencies' will probably work only with equality clauses.
 */
static MVStatisticInfo *
choose_mv_statistics(List *stats, Bitmapset *attnums)
{
	int			i;
	ListCell   *lc;

	MVStatisticInfo *choice = NULL;

	int			current_matches = 1;	/* goal #1: maximize */
	int			current_dims = (MVSTATS_MAX_DIMENSIONS + 1);	/* goal #2: minimize */

	/*
	 * Walk through the statistics (simple array with nmvstats elements) and
	 * for each one count the referenced attributes (encoded in the 'attnums'
	 * bitmap).
	 */
	foreach(lc, stats)
	{
		MVStatisticInfo *info = (MVStatisticInfo *) lfirst(lc);

		/* columns matching this statistics */
		int			matches = 0;

		int2vector *attrs = info->stakeys;
		int			numattrs = attrs->dim1;

		/* skip dependencies-only stats */
		if (!info->mcv_built)
			continue;

		/* count columns covered by the histogram */
		for (i = 0; i < numattrs; i++)
			if (bms_is_member(attrs->values[i], attnums))
				matches++;

		/*
		 * Use this statistics when it improves the number of matches or when
		 * it matches the same number of attributes but is smaller.
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
 * This splits the clauses list into two parts - one containing clauses that
 * will be evaluated using the chosen statistics, and the remaining clauses
 * (either non-mvcompatible, or not related to the histogram).
 */
static List *
clauselist_mv_split(PlannerInfo *root, Index relid,
					List *clauses, List **mvclauses,
					MVStatisticInfo *mvstats, int types)
{
	int			i;
	ListCell   *l;
	List	   *non_mvclauses = NIL;

	/* FIXME is there a better way to get info on int2vector? */
	int2vector *attrs = mvstats->stakeys;
	int			numattrs = mvstats->stakeys->dim1;

	Bitmapset  *mvattnums = NULL;

	/* build bitmap of attributes, so we can do bms_is_subset later */
	for (i = 0; i < numattrs; i++)
		mvattnums = bms_add_member(mvattnums, attrs->values[i]);

	/* erase the list of mv-compatible clauses */
	*mvclauses = NIL;

	foreach(l, clauses)
	{
		bool		match = false;		/* by default not mv-compatible */
		Bitmapset  *attnums = NULL;
		Node	   *clause = (Node *) lfirst(l);

		if (clause_is_mv_compatible(clause, relid, &attnums, types))
		{
			/* are all the attributes part of the selected stats? */
			if (bms_is_subset(attnums, mvattnums))
				match = true;
		}

		/*
		 * The clause matches the selected stats, so put it to the list of
		 * mv-compatible clauses. Otherwise, keep it in the list of 'regular'
		 * clauses (that may be selected later).
		 */
		if (match)
			*mvclauses = lappend(*mvclauses, clause);
		else
			non_mvclauses = lappend(non_mvclauses, clause);
	}

	/*
	 * Perform regular estimation using the clauses incompatible with the
	 * chosen histogram (or MV stats in general).
	 */
	return non_mvclauses;

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
 *	   I've been unable to make that work - seems that does not quite allow
 *	   checking the structure. Hence the explicit calls to the walker.
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
				if (!(context->types & MV_CLAUSE_TYPE_MCV))
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
 *	   variable OP constant
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
 *	 1) mark all items as 'match'
 *	 2) walk through all the clauses
 *	 3) for a particular clause, walk through all the items
 *	 4) skip items that are already 'no match'
 *	 5) check clause for items that still match
 *	 6) sum frequencies for items to get selectivity
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
clauselist_mv_selectivity_mcvlist(PlannerInfo *root, List *clauses,
								  MVStatisticInfo *mvstats, bool *fullmatch,
								  Selectivity *lowsel)
{
	int			i;
	Selectivity s = 0.0;
	Selectivity u = 0.0;

	MCVList		mcvlist = NULL;
	int			nmatches = 0;

	/* match/mismatch bitmap for each MCV item */
	char	   *matches = NULL;

	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 2);

	/* there's no MCV list built yet */
	if (!mvstats->mcv_built)
		return 0.0;

	mcvlist = load_mv_mcvlist(mvstats->mvoid);

	Assert(mcvlist != NULL);
	Assert(mcvlist->nitems > 0);

	/* by default all the MCV items match the clauses fully */
	matches = palloc0(sizeof(char) * mcvlist->nitems);
	memset(matches, MVSTATS_MATCH_FULL, sizeof(char) * mcvlist->nitems);

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

	return s * u;
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
							if (isgt)
								mismatch = !DatumGetBool(FunctionCall2Coll(&opproc,
														   DEFAULT_COLLATION_OID,
															 cst->constvalue,
															item->values[idx]));
							else
								mismatch = !DatumGetBool(FunctionCall2Coll(&opproc,
														   DEFAULT_COLLATION_OID,
															 item->values[idx],
															  cst->constvalue));

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
		else if (or_clause(clause) || and_clause(clause))
		{
			/*
			 * AND/OR clause, with all clauses compatible with the selected MV
			 * stat
			 */

			int			i;
			BoolExpr   *orclause = ((BoolExpr *) clause);
			List	   *orclauses = orclause->args;

			/* match/mismatch bitmap for each MCV item */
			int			or_nmatches = 0;
			char	   *or_matches = NULL;

			Assert(orclauses != NIL);
			Assert(list_length(orclauses) >= 2);

			/* number of matching MCV items */
			or_nmatches = mcvlist->nitems;

			/* by default none of the MCV items matches the clauses */
			or_matches = palloc0(sizeof(char) * or_nmatches);

			if (or_clause(clause))
			{
				/* OR clauses assume nothing matches, initially */
				memset(or_matches, MVSTATS_MATCH_NONE, sizeof(char) * or_nmatches);
				or_nmatches = 0;
			}
			else
			{
				/* AND clauses assume nothing matches, initially */
				memset(or_matches, MVSTATS_MATCH_FULL, sizeof(char) * or_nmatches);
			}

			/* build the match bitmap for the OR-clauses */
			or_nmatches = update_match_bitmap_mcvlist(root, orclauses,
													  stakeys, mcvlist,
													  or_nmatches, or_matches,
									   lowsel, fullmatch, or_clause(clause));

			/* merge the bitmap into the existing one */
			for (i = 0; i < mcvlist->nitems; i++)
			{
				/*
				 * Merge the result into the bitmap (Min for AND, Max for OR).
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
