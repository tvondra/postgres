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

static bool clause_is_mv_compatible(PlannerInfo *root, Node *clause, Oid varRelid,
							 Index *relid, Bitmapset **attnums, SpecialJoinInfo *sjinfo,
							 int type);

static Bitmapset  *collect_mv_attnums(PlannerInfo *root, List *clauses,
									  Oid varRelid, Index *relid, SpecialJoinInfo *sjinfo,
									  int type);

static Bitmapset *clause_mv_get_attnums(PlannerInfo *root, Node *clause);

static List *clauselist_apply_dependencies(PlannerInfo *root, List *clauses,
								Oid varRelid, List *stats,
								SpecialJoinInfo *sjinfo);

static List *clauselist_mv_split(PlannerInfo *root, SpecialJoinInfo *sjinfo,
								 List *clauses, Oid varRelid,
								 List **mvclauses, MVStatisticInfo *mvstats, int types);

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
									int nmatches, char * matches,
									Selectivity *lowsel, bool *fullmatch,
									bool is_or);

static int update_match_bitmap_histogram(PlannerInfo *root, List *clauses,
									int2vector *stakeys,
									MVSerializedHistogram mvhist,
									int nmatches, char * matches,
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
typedef struct mv_solution_t {
	int		nclauses;		/* number of clauses covered */
	int		nconditions;	/* number of conditions covered */
	int		nstats;			/* number of stats applied */
	int	   *stats;			/* stats (in the apply order) */
} mv_solution_t;

static List *choose_mv_statistics(PlannerInfo *root,
								List *mvstats,
								List *clauses, List *conditions,
								Oid varRelid,
								SpecialJoinInfo *sjinfo);

static List *filter_clauses(PlannerInfo *root, Oid varRelid,
							SpecialJoinInfo *sjinfo, int type,
							List *stats, List *clauses,
							Bitmapset **attnums);

static List *filter_stats(List *stats, Bitmapset *new_attnums,
						  Bitmapset *all_attnums);

static Bitmapset **make_stats_attnums(MVStatisticInfo *mvstats,
									  int nmvstats);

static MVStatisticInfo *make_stats_array(List *stats, int *nmvstats);

static List* filter_redundant_stats(List *stats,
									List *clauses, List *conditions);

static Node** make_clauses_array(List *clauses, int *nclauses);

static Bitmapset ** make_clauses_attnums(PlannerInfo *root, Oid varRelid,
										 SpecialJoinInfo *sjinfo, int type,
										 Node **clauses, int nclauses);

static bool* make_cover_map(Bitmapset **stats_attnums, int nmvstats,
							Bitmapset **clauses_attnums, int nclauses);

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

int mvstat_search_type = MVSTAT_SEARCH_GREEDY;

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
					   SpecialJoinInfo *sjinfo,
					   List *conditions)
{
	Selectivity s1 = 1.0;
	RangeQueryClause *rqlist = NULL;
	ListCell   *l;

	/* processing mv stats */
	Index		relid = InvalidOid;

	/* attributes in mv-compatible clauses */
	Bitmapset  *mvattnums = NULL;
	List	   *stats = NIL;

	/* use clauses (not conditions), because those are always non-empty */
	stats = find_stats(root, clauses, varRelid, &relid);

	/*
	 * If there's exactly one clause, then no use in trying to match up
	 * pairs, or matching multivariate statistics, so just go directly
	 * to clause_selectivity().
	 */
	if (list_length(clauses) == 1)
		return clause_selectivity(root, (Node *) linitial(clauses),
								  varRelid, jointype, sjinfo, conditions);

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
	 * Check that there are statistics with MCV list or histogram.
	 * If not, we don't need to waste time with the optimization.
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
		 * a suitable combination of multivariate stats. If there are
		 * multiple combinations, we'll try to choose the best one.
		 * See choose_mv_statistics for more details.
		 */
		if (bms_num_members(mvattnums) >= 2)
		{
			int k;
			ListCell *s;

			/*
			 * Copy the list of conditions, so that we can build a list
			 * of local conditions (and keep the original intact, for
			 * the other clauses at the same level).
			 */
			List *conditions_local = list_copy(conditions);

			/* find the best combination of statistics */
			List *solution = choose_mv_statistics(root, stats,
												  clauses, conditions,
												  varRelid, sjinfo);

			/* we have a good solution (list of stats) */
			foreach (s, solution)
			{
				MVStatisticInfo *mvstat = (MVStatisticInfo *)lfirst(s);

				/* clauses compatible with multi-variate stats */
				List	*mvclauses = NIL;
				List	*mvclauses_new = NIL;
				List	*mvclauses_conditions = NIL;
				Bitmapset	*stat_attnums = NULL;

				/* build attnum bitmapset for this statistics */
				for (k = 0; k < mvstat->stakeys->dim1; k++)
					stat_attnums = bms_add_member(stat_attnums,
												  mvstat->stakeys->values[k]);

				/*
				 * Append the compatible conditions (passed from above)
				 * to mvclauses_conditions.
				 */
				foreach (l, conditions)
				{
					Node *c = (Node*)lfirst(l);
					Bitmapset *tmp = clause_mv_get_attnums(root, c);

					if (bms_is_subset(tmp, stat_attnums))
						mvclauses_conditions
							= lappend(mvclauses_conditions, c);

					bms_free(tmp);
				}

				/* split the clauselist into regular and mv-clauses
				 *
				 * We keep the list of clauses (we don't remove the
				 * clauses yet, because we want to use the clauses
				 * as conditions of other clauses).
				 *
				 * FIXME Do this only once, i.e. filter the clauses
				 *       once (selecting clauses covered by at least
				 *       one statistics) and then convert them into
				 *       smaller per-statistics lists of conditions
				 *       and estimated clauses.
				 */
				clauselist_mv_split(root, sjinfo, clauses,
										varRelid, &mvclauses, mvstat,
										(MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST));

				/*
				 * We've chosen the statistics to match the clauses, so
				 * each statistics from the solution should have at least
				 * one new clause (not covered by the previous stats).
				 */
				Assert(mvclauses != NIL);

				/*
				 * Mvclauses now contains only clauses compatible
				 * with the currently selected stats, but we have to
				 * split that into conditions (already matched by
				 * the previous stats), and the new clauses we need
				 * to estimate using this stats.
				 */
				foreach (l, mvclauses)
				{
					ListCell *p;
					bool covered = false;
					Node  *clause = (Node *) lfirst(l);
					Bitmapset *clause_attnums = clause_mv_get_attnums(root, clause);

					/*
					 * If already covered by previous stats, add it to
					 * conditions.
					 *
					 * TODO Maybe this could be relaxed a bit? Because
					 *      with complex and/or clauses, this might
					 *      mean no statistics actually covers such
					 *      complex clause.
					 */
					foreach (p, solution)
					{
						int k;
						Bitmapset  *stat_attnums = NULL;

						MVStatisticInfo *prev_stat
							= (MVStatisticInfo *)lfirst(p);

						/* break if we've ran into current statistic */
						if (prev_stat == mvstat)
							break;

						for (k = 0; k < prev_stat->stakeys->dim1; k++)
							stat_attnums = bms_add_member(stat_attnums,
														  prev_stat->stakeys->values[k]);

						covered = bms_is_subset(clause_attnums, stat_attnums);

						bms_free(stat_attnums);

						if (covered)
							break;
					}

					if (covered)
						mvclauses_conditions
							= lappend(mvclauses_conditions, clause);
					else
						mvclauses_new
							= lappend(mvclauses_new, clause);
				}

				/*
				 * We need at least one new clause (not just conditions).
				 */
				Assert(mvclauses_new != NIL);

				/* compute the multivariate stats */
				s1 *= clauselist_mv_selectivity(root, mvstat,
												mvclauses_new,
												mvclauses_conditions,
												false); /* AND */
			}

			/*
			 * And now finally remove all the mv-compatible clauses.
			 *
			 * This only repeats the same split as above, but this
			 * time we actually use the result list (and feed it to
			 * the next call).
			 */
			foreach (s, solution)
			{
				/* clauses compatible with multi-variate stats */
				List	*mvclauses = NIL;

				MVStatisticInfo *mvstat = (MVStatisticInfo *)lfirst(s);

				/* split the list into regular and mv-clauses */
				clauses = clauselist_mv_split(root, sjinfo, clauses,
										varRelid, &mvclauses, mvstat,
										(MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST));

				/*
				 * Add the clauses to the conditions (to be passed
				 * to regular clauses), irrespectedly whether it
				 * will be used as a condition or a clause here.
				 *
				 * We only keep the remaining conditions in the
				 * clauses (we keep what clauselist_mv_split returns)
				 * so we add each MV condition exactly once.
				 */
				conditions_local = list_concat(conditions_local, mvclauses);
			}

			/* from now on, work with the 'local' list of conditions */
			conditions = conditions_local;
		}
	}

	/*
	 * If there's exactly one clause, then no use in trying to match up
	 * pairs, so just go directly to clause_selectivity().
	 */
	if (list_length(clauses) == 1)
		return clause_selectivity(root, (Node *) linitial(clauses),
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
 * Similar to clauselist_selectivity(), but for OR-clauses. We can't
 * simply apply exactly the same logic as to AND-clauses, because there
 * are a few key differences:
 *
 *   - functional dependencies don't really apply to OR-clauses
 *
 *   - clauselist_selectivity() works by decomposing the selectivity
 *     into conditional selectivities (probabilities), but that can be
 *     done only for AND-clauses. That means problems with applying
 *     multiple statistics (and reusing clauses as conditions, etc.).
 *
 * We might invent a completely new set of functions here, resembling
 * clauselist_selectivity but adapting the ideas to OR-clauses.
 *
 * But luckily we know that each OR-clause
 *
 *     (a OR b OR c)
 *
 * may be rewritten as an equivalent AND-clause using negation:
 *
 *     NOT ((NOT a) AND (NOT b) AND (NOT c))
 *
 * And that's something we can pass to clauselist_selectivity.
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

	/* (NOT ...) */
	foreach (l, clauses)
		args = lappend(args, makeBoolExpr(NOT_EXPR, list_make1(lfirst(l)), -1));

	/* ((NOT ...) AND (NOT ...)) */
	expr = makeBoolExpr(AND_EXPR, args, -1);

	/* NOT (... AND ...) */
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
 *
 * TODO All this is based on the assumption that the statistics represent
 *      the necessary dependencies, i.e. that if two colunms are not in
 *      the same statistics, there's no dependency. If that's not the
 *      case, we may get misestimates, just like before. For example
 *      assume we have a table with three columns [a,b,c] with exactly
 *      the same values, and statistics on [a,b] and [b,c]. So somthing
 *      like this:
 *
 *          CREATE TABLE test AS SELECT i, i, i
                                  FROM generate_series(1,1000);
 *
 *          ALTER TABLE test ADD STATISTICS (mcv) ON (a,b);
 *          ALTER TABLE test ADD STATISTICS (mcv) ON (b,c);
 *
 *          ANALYZE test;
 *
 *          EXPLAIN ANALYZE SELECT * FROM test
 *                    WHERE (a < 10) AND (b < 20) AND (c < 10);
 *
 *      The problem here is that the only shared column between the two
 *      statistics is 'b' so the probability will be computed like this
 *
 *          P[(a < 10) & (b < 20) & (c < 10)]
 *             = P[(a < 10) & (b < 20)] * P[(c < 10) | (a < 10) & (b < 20)]
 *             = P[(a < 10) & (b < 20)] * P[(c < 10) | (b < 20)]
 *
 *      or like this
 *
 *          P[(a < 10) & (b < 20) & (c < 10)]
 *             = P[(b < 20) & (c < 10)] * P[(a < 10) | (b < 20) & (c < 10)]
 *             = P[(b < 20) & (c < 10)] * P[(a < 10) | (b < 20)]
 *
 *      In both cases the conditional probabilities will be evaluated as
 *      0.5, because they lack the other column (which would make it 1.0).
 *
 *      Theoretically it might be possible to transfer the dependency,
 *      e.g. by building bitmap for [a,b] and then combine it with [b,c]
 *      by doing something like this:
 *
 *          1) build bitmap on [a,b] using [(a<10) & (b < 20)]
 *          2) for each element in [b,c] check the bitmap
 *
 *      But that's certainly nontrivial - for example the statistics may
 *      be different (MCV list vs. histogram) and/or the items may not
 *      match (e.g. MCV items or histogram buckets will be built
 *      differently). Also, for one value of 'b' there might be multiple
 *      MCV items (because of the other column values) with different
 *      bitmap values (some will match, some won't) - so it's not exactly
 *      bitmap but a partial match.
 *
 *      Maybe a hash table with number of matches and mismatches (or
 *      maybe sums of frequencies) would work? The step (2) would then
 *      lookup the values and use that to weight the item somehow.
 * 
 *      Currently the only solution is to build statistics on all three
 *      columns.
 */
static Selectivity
clauselist_mv_selectivity(PlannerInfo *root, MVStatisticInfo *mvstats,
						  List *clauses, List *conditions, bool is_or)
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
	s1 = clauselist_mv_selectivity_mcvlist(root, mvstats,
										   clauses, conditions, is_or,
										   &fullmatch, &mcv_low);

	/*
	 * If we got a full equality match on the MCV list, we're done (and
	 * the estimate is pretty good).
	 */
	if (fullmatch && (s1 > 0.0))
		return s1;

	/* FIXME if (fullmatch) without matching MCV item, use the mcv_low
	 *       selectivity as upper bound */

	s2 = clauselist_mv_selectivity_histogram(root, mvstats,
											 clauses, conditions, is_or);

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
		bms_free(attnums);
		attnums = NULL;
		*relid = InvalidOid;
	}

	return attnums;
}

/*
 * Selects the best combination of multivariate statistics, in an
 * exhaustive way, where 'best' means:
 *
 * (a) covering the most attributes (referenced by clauses)
 * (b) using the least number of multivariate stats
 * (c) using the most conditions to exploit dependency
 *
 * There may be other optimality criteria, not considered in the initial
 * implementation (more on that 'weaknesses' section).
 *
 * This pretty much splits the probability of clauses (aka selectivity)
 * into a sequence of conditional probabilities, like this
 *
 *    P(A,B,C,D) = P(A,B) * P(C|A,B) * P(D|A,B,C)
 *
 * and removing the attributes not referenced by the existing stats,
 * under the assumption that there's no dependency (otherwise the DBA
 * would create the stats).
 *
 * The last criteria means that when we have the choice to compute like
 * this
 *
 *      P(A,B,C,D) = P(A,B,C) * P(D|B,C)
 *
 * or like this
 *
 *      P(A,B,C,D) = P(A,B,C) * P(D|C)
 *
 * we should use the first option, as that exploits more dependencies.
 *
 * The order of statistics in the solution implicitly determines the
 * order of estimation of clauses, because as we apply a statistics,
 * we always use it to estimate all the clauses covered by it (and
 * then we use those clauses as conditions for the next statistics).
 *
 * Don't call this directly but through choose_mv_statistics().
 *
 *
 * Algorithm
 * ---------
 * The algorithm is a recursive implementation of backtracking, with
 * maximum 'depth' equal to the number of multi-variate statistics
 * available on the table.
 *
 * It explores all the possible permutations of the stats.
 * 
 * Whenever it considers adding the next statistics, the clauses it
 * matches are divided into 'conditions' (clauses already matched by at
 * least one previous statistics) and clauses that are estimated.
 *
 * Then several checks are performed:
 *
 *  (a) The statistics covers at least 2 columns, referenced in the
 *      estimated clauses (otherwise multi-variate stats are useless).
 *
 *  (b) The statistics covers at least 1 new column, i.e. column not
 *      refefenced by the already used stats (and the new column has
 *      to be referenced by the clauses, of couse). Otherwise the
 *      statistics would not add any new information.
 *
 * There are some other sanity checks (e.g. that the stats must not be
 * used twice etc.).
 *
 * Finally the new solution is compared to the currently best one, and
 * if it's considered better, it's used instead.
 *
 *
 * Weaknesses
 * ----------
 * The current implemetation uses a somewhat simple optimality criteria,
 * suffering by the following weaknesses.
 *
 * (a) There may be multiple solutions with the same number of covered
 *     attributes and number of statistics (e.g. the same solution but
 *     with statistics in a different order). It's unclear which solution
 *     is the best one - in a sense all of them are equal.
 *
 * TODO It might be possible to compute estimate for each of those
 *      solutions, and then combine them to get the final estimate
 *      (e.g. by using average or median).
 *
 * (b) Does not consider that some types of stats are a better match for
 *     some types of clauses (e.g. MCV list is a good match for equality
 *     than a histogram).
 *
 *     XXX Maybe MCV is almost always better / more accurate?
 *
 *     But maybe this is pointless - generally, each column is either
 *     a label (it's not important whether because of the data type or
 *     how it's used), or a value with ordering that makes sense. So
 *     either a MCV list is more appropriate (labels) or a histogram
 *     (values with orderings).
 *
 *     Now sure what to do with statistics on columns mixing columns of
 *     both types - maybe it'd be beeter to invent a new type of stats
 *     combining MCV list and histogram (keeping a small histogram for
 *     each MCV item, and a separate histogram for values not on the
 *     MCV list). But that's not implemented at this moment.
 *
 * TODO The algorithm should probably count number of Vars (not just
 *      attnums) when computing the 'score' of each solution. Computing
 *      the ratio of (num of all vars) / (num of condition vars) as a
 *      measure of how well the solution uses conditions might be
 *      useful.
 */
static void
choose_mv_statistics_exhaustive(PlannerInfo *root, int step,
					int nmvstats, MVStatisticInfo *mvstats, Bitmapset ** stats_attnums,
					int nclauses, Node ** clauses, Bitmapset ** clauses_attnums,
					int nconditions, Node ** conditions, Bitmapset ** conditions_attnums,
					bool *cover_map, bool *condition_map, int *ruled_out,
					mv_solution_t *current, mv_solution_t **best)
{
	int i, j;

	Assert(best != NULL);
	Assert((step == 0 && current == NULL) || (step > 0 && current != NULL));

	CHECK_FOR_INTERRUPTS();

	if (current == NULL)
	{
		current = (mv_solution_t*)palloc0(sizeof(mv_solution_t));
		current->stats = (int*)palloc0(sizeof(int)*nmvstats);
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
		int c;

		int ncovered_clauses = 0;		/* number of covered clauses */
		int ncovered_conditions = 0;	/* number of covered conditions */
		int nattnums = 0;		/* number of covered attributes */

		Bitmapset  *all_attnums = NULL;
		Bitmapset  *new_attnums = NULL;

		/* skip statistics that were already used or eliminated */
		if (ruled_out[i] != -1)
			continue;

		/*
		 * See if we have clauses covered by this statistics, but not
		 * yet covered by any of the preceding onces.
		 */
		for (c = 0; c < nclauses; c++)
		{
			bool covered = false;
			Bitmapset *clause_attnums = clauses_attnums[c];
			Bitmapset *tmp = NULL;

			/*
			 * If this clause is not covered by this stats, we can't
			 * use the stats to estimate that at all.
			 */
			if (! cover_map[i * nclauses + c])
				continue;

			/*
			 * Now we know we'll use this clause - either as a condition
			 * or as a new clause (the estimated one). So let's add the
			 * attributes to the attnums from all the clauses usable with
			 * this statistics.
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
			 * OK, this clause is covered by this statistics (and not by
			 * any of the previous ones)
			 */
			ncovered_clauses += 1;

			/* add the attnums into attnums from 'new clauses' */
			// new_attnums = bms_union(new_attnums, clause_attnums);
		}

		/* can't have more new clauses than original clauses */
		Assert(nclauses >= ncovered_clauses);
		Assert(ncovered_clauses >= 0);	/* mostly paranoia */

		nattnums = bms_num_members(all_attnums);

		/* free all the bitmapsets - we don't need them anymore */
		bms_free(all_attnums);
		bms_free(new_attnums);

		all_attnums = NULL;
		new_attnums = NULL;

		/*
		 * See if we have clauses covered by this statistics, but not
		 * yet covered by any of the preceding onces.
		 */
		for (c = 0; c < nconditions; c++)
		{
			Bitmapset *clause_attnums = conditions_attnums[c];
			Bitmapset *tmp = NULL;

			/*
			 * If this clause is not covered by this stats, we can't
			 * use the stats to estimate that at all.
			 */
			if (! condition_map[i * nconditions + c])
				continue;

			/* count this as a condition */
			ncovered_conditions += 1;

			/*
			 * Now we know we'll use this clause - either as a condition
			 * or as a new clause (the estimated one). So let's add the
			 * attributes to the attnums from all the clauses usable with
			 * this statistics.
			 */
			tmp = bms_union(all_attnums, clause_attnums);

			/* free the old bitmap */
			bms_free(all_attnums);
			all_attnums = tmp;
		}

		/*
		 * Let's mark the statistics as 'ruled out' - either we'll use
		 * it (and proceed to the next step), or it's incompatible.
		 */
		ruled_out[i] = step;

		/*
		 * There are no clauses usable with this statistics (not already
		 * covered by aome of the previous stats).
		 *
		 * Similarly, if the clauses only use a single attribute, we
		 * can't really use that.
		 */
		if ((ncovered_clauses == 0) || (nattnums < 2))
			continue;

		/*
		 * TODO Not sure if it's possible to add a clause referencing
		 *      only attributes already covered by previous stats?
		 *      Introducing only some new dependency, not a new
		 *      attribute. Couldn't come up with an example, though.
		 *      Might be worth adding some assert.
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
		 * We can never cover more clauses, or use more stats that we
		 * actually have at the beginning.
		 */
		Assert(nclauses >= current->nclauses);
		Assert(nmvstats >= current->nstats);
		Assert(step < nmvstats);

		/* we can't get more conditions that clauses and conditions combined
		 *
		 * FIXME This assert does not work because we count the conditions
		 *       repeatedly (once for each statistics covering it).
		 */
		/* Assert((nconditions + nclauses) >= current->nconditions); */

		if (*best == NULL)
		{
			*best = (mv_solution_t*)palloc0(sizeof(mv_solution_t));
			(*best)->stats = (int*)palloc0(sizeof(int)*nmvstats);
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
			choose_mv_statistics_exhaustive(root, step+1,
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
 * Greedy search for a multivariate solution - a sequence of statistics
 * covering the clauses. This chooses the "best" statistics at each step,
 * so the resulting solution may not be the best solution globally, but
 * this produces the solution in only N steps (where N is the number of
 * statistics), while the exhaustive approach may have to walk through
 * ~N! combinations (although some of those are terminated early).
 *
 * See the comments at choose_mv_statistics_exhaustive() as this does
 * the same thing (but in a different way).
 *
 * Don't call this directly, but through choose_mv_statistics().
 *
 * TODO There are probably other metrics we might use - e.g. using
 *      number of columns (num_cond_columns / num_cov_columns), which
 *      might work better with a mix of simple and complex clauses.
 *
 * TODO Also the choice at the very first step should be handled
 *      in a special way, because there will be 0 conditions at that
 *      moment, so there needs to be some other criteria - e.g. using
 *      the simplest (or most complex?) clause might be a good idea.
 *
 * TODO We might also select multiple stats using different criteria,
 *      and branch the search. This is however tricky, because if we
 *      choose k statistics at each step, we get k^N branches to
 *      walk through (with N steps). That's not really good with
 *      large number of stats (yet better than exhaustive search).
 */
static void
choose_mv_statistics_greedy(PlannerInfo *root, int step,
					int nmvstats, MVStatisticInfo *mvstats, Bitmapset ** stats_attnums,
					int nclauses, Node ** clauses, Bitmapset ** clauses_attnums,
					int nconditions, Node ** conditions, Bitmapset ** conditions_attnums,
					bool *cover_map, bool *condition_map, int *ruled_out,
					mv_solution_t *current, mv_solution_t **best)
{
	int i, j;
	int best_stat = -1;
	double gain, max_gain = -1.0;

	/*
	 * Bitmap tracking which clauses are already covered (by the previous
	 * statistics) and may thus serve only as a condition in this step.
	 */
	bool *covered_clauses = (bool*)palloc0(nclauses);

	/*
	 * Number of clauses and columns covered by each statistics - this
	 * includes both conditions and clauses covered by the statistics for
	 * the first time. The number of columns may count some columns
	 * repeatedly - if a column is shared by multiple clauses, it will
	 * be counted once for each clause (covered by the statistics).
	 * So with two clauses [(a=1 OR b=2),(a<2 OR c>1)] the column "a"
	 * will be counted twice (if both clauses are covered).
	 *
	 * The values for reduded statistics (that can't be applied) are
	 * not computed, because that'd be pointless.
	 */
	int	*num_cov_clauses	= (int*)palloc0(sizeof(int) * nmvstats);
	int	*num_cov_columns	= (int*)palloc0(sizeof(int) * nmvstats);

	/*
	 * Same as above, but this only includes clauses that are already
	 * covered by the previous stats (and the current one).
	 */
	int	*num_cond_clauses	= (int*)palloc0(sizeof(int) * nmvstats);
	int	*num_cond_columns	= (int*)palloc0(sizeof(int) * nmvstats);

	/*
	 * Number of attributes for each clause.
	 *
	 * TODO Might be computed in choose_mv_statistics() and then passed
	 *      here, but then the function would not have the same signature
	 *      as _exhaustive().
	 */
	int *attnum_counts = (int*)palloc0(sizeof(int) * nclauses);
	int *attnum_cond_counts = (int*)palloc0(sizeof(int) * nconditions);

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
		Bitmapset *attnums_covered = NULL;
		Bitmapset *attnums_conditions = NULL;

		/* skip stats that are already ruled out (either used or inapplicable) */
		if (ruled_out[i] != -1)
			continue;

		/* count covered clauses and conditions (for the statistics) */
		for (j = 0; j < nclauses; j++)
		{
			if (cover_map[i * nclauses + j])
			{
				Bitmapset *attnums_new
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
		gain = num_cond_columns[i] / (double)num_cov_columns[i];

		if (gain > max_gain)
		{
			max_gain = gain;
			best_stat = i;
		}
	}

	/*
	 * Have we found a suitable statistics? Add it to the solution and
	 * try next step.
	 */
	if (best_stat != -1)
	{
		/* mark the statistics, so that we skip it in next steps */
		ruled_out[best_stat] = step;

		/* allocate current solution if necessary */
		if (current == NULL)
		{
			current = (mv_solution_t*)palloc0(sizeof(mv_solution_t));
			current->stats = (int*)palloc0(sizeof(int)*nmvstats);
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
			(*best) = (mv_solution_t*)palloc0(sizeof(mv_solution_t));
			(*best)->nstats = current->nstats;
			(*best)->nclauses = current->nclauses;
			(*best)->nconditions = current->nconditions;

			(*best)->stats = (int*)palloc0(sizeof(int)*nmvstats);
			memcpy((*best)->stats, current->stats, nmvstats * sizeof(int));
		}
		else
		{
			/* see if this is a better solution */
			double current_gain = (double)current->nconditions / current->nclauses;
			double best_gain    = (double)(*best)->nconditions / (*best)->nclauses;

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
			choose_mv_statistics_greedy(root, step+1,
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
 * Chooses the combination of statistics, optimal for estimation of
 * a particular clause list.
 *
 * This only handles a 'preparation' shared by the exhaustive and greedy
 * implementations (see the previous methods), mostly trying to reduce
 * the size of the problem (eliminate clauses/statistics that can't be
 * really used in the solution).
 *
 * It also precomputes bitmaps for attributes covered by clauses and
 * statistics, so that we don't need to do that over and over in the
 * actual optimizations (as it's both CPU and memory intensive).
 *
 * TODO This will probably have to consider compatibility of clauses,
 *      because 'dependencies' will probably work only with equality
 *      clauses.
 *
 * TODO Another way to make the optimization problems smaller might
 *      be splitting the statistics into several disjoint subsets, i.e.
 *      if we can split the graph of statistics (after the elimination)
 *      into multiple components (so that stats in different components
 *      share no attributes), we can do the optimization for each
 *      component separately.
 *
 * TODO If we could compute what is a "perfect solution" maybe we could
 *      terminate the search after reaching ~90% of it? Say, if we knew
 *      that we can cover 10 clauses and reuse 8 dependencies, maybe
 *      covering 9 clauses and 7 dependencies would be OK?
 */
static List*
choose_mv_statistics(PlannerInfo *root, List *stats,
					 List *clauses, List *conditions,
					 Oid varRelid, SpecialJoinInfo *sjinfo)
{
	int i;
	mv_solution_t *best = NULL;
	List *result = NIL;

	int nmvstats;
	MVStatisticInfo *mvstats;

	/* we only work with MCV lists and histograms here */
	int type = (MV_CLAUSE_TYPE_MCV | MV_CLAUSE_TYPE_HIST);

	bool   *clause_cover_map = NULL,
		   *condition_cover_map = NULL;
	int	   *ruled_out = NULL;

	/* build bitmapsets for all stats and clauses */
	Bitmapset **stats_attnums;
	Bitmapset **clauses_attnums;
	Bitmapset **conditions_attnums;

	int nclauses, nconditions;
	Node ** clauses_array;
	Node ** conditions_array;

	/* copy lists, so that we can free them during elimination easily */
	clauses = list_copy(clauses);
	conditions = list_copy(conditions);
	stats = list_copy(stats);

	/*
	 * Reduce the optimization problem size as much as possible.
	 *
	 * Eliminate clauses and conditions not covered by any statistics,
	 * or statistics not matching at least two attributes (one of them
	 * has to be in a regular clause).
	 *
	 * It's possible that removing a statistics in one iteration
	 * eliminates clause in the next one, so we'll repeat this until we
	 * eliminate no clauses/stats in that iteration.
	 *
	 * This can only happen after eliminating a statistics - clauses are
	 * eliminated first, so statistics always reflect that.
	 */
	while (true)
	{
		List	   *tmp;

		Bitmapset *compatible_attnums = NULL;
		Bitmapset *condition_attnums  = NULL;
		Bitmapset *all_attnums = NULL;

		/*
		 * Clauses
		 *
		 * Walk through clauses and keep only those covered by at least
		 * one of the statistics we still have. We'll also keep info
		 * about attnums in clauses (without conditions) so that we can
		 * ignore stats covering just conditions (which is pointless).
		 */
		tmp = filter_clauses(root, varRelid, sjinfo, type,
							 stats, clauses, &compatible_attnums);

		/* discard the original list */
		list_free(clauses);
		clauses = tmp;

		/*
		 * Conditions
		 *
		 * Walk through clauses and keep only those covered by at least
		 * one of the statistics we still have. Also, collect bitmap of
		 * attributes so that we can make sure we add at least one new
		 * attribute (by comparing with clauses).
		 */
		if (conditions != NIL)
		{
			tmp = filter_clauses(root, varRelid, sjinfo, type,
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
		 * Walk through statistics and only keep those covering at least
		 * one new attribute (excluding conditions) and at two attributes
		 * in both clauses and conditions.
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
	 * TODO We should sort the stats to make the order deterministic,
	 *      otherwise we may get different estimates on different
	 *      executions - if there are multiple "equally good" solutions,
	 *      we'll keep the first solution we see.
	 *
	 *      Sorting by OID probably is not the right solution though,
	 *      because we'd like it to be somehow reproducible,
	 *      irrespectedly of the order of ADD STATISTICS commands.
	 *      So maybe statkeys?
	 */
	mvstats = make_stats_array(stats, &nmvstats);
	stats_attnums = make_stats_attnums(mvstats, nmvstats);

	/* collect clauses an bitmap of attnums */
	clauses_array = make_clauses_array(clauses, &nclauses);
	clauses_attnums = make_clauses_attnums(root, varRelid, sjinfo, type,
										   clauses_array, nclauses);

	/* collect conditions and bitmap of attnums */
	conditions_array = make_clauses_array(conditions, &nconditions);
	conditions_attnums = make_clauses_attnums(root, varRelid, sjinfo, type,
										   conditions_array, nconditions);

	/*
	 * Build bitmaps with info about which clauses/conditions are
	 * covered by each statistics (so that we don't need to call the
	 * bms_is_subset over and over again).
	 */
	clause_cover_map = make_cover_map(stats_attnums, nmvstats,
									  clauses_attnums, nclauses);

	condition_cover_map	= make_cover_map(stats_attnums, nmvstats,
										 conditions_attnums, nconditions);

	ruled_out =  (int*)palloc0(nmvstats * sizeof(int));

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
	else if (or_clause(clause) || and_clause(clause) || not_clause(clause))
	{
		/*
		 * AND/OR/NOT-clauses are supported if all sub-clauses are supported
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
		 *
		 * TODO Perhaps this needs a bit more thought for functional
		 *      dependencies? Those don't quite work for NOT cases.
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


static Bitmapset *
clause_mv_get_attnums(PlannerInfo *root, Node *clause)
{
	Bitmapset * attnums = NULL;

	/* Extract clause from restrict info, if needed. */
	if (IsA(clause, RestrictInfo))
		clause = (Node*)((RestrictInfo*)clause)->clause;

	/*
	 * Only simple opclauses and IS NULL tests are compatible with
	 * multivariate stats at this point.
	 */
	if ((is_opclause(clause))
		&& (list_length(((OpExpr *) clause)->args) == 2))
	{
		OpExpr	   *expr = (OpExpr *) clause;

		if (IsA(linitial(expr->args), Var))
			attnums = bms_add_member(attnums,
							((Var*)linitial(expr->args))->varattno);
		else
			attnums = bms_add_member(attnums,
							((Var*)lsecond(expr->args))->varattno);
	}
	else if (IsA(clause, NullTest)
			 && IsA(((NullTest*)clause)->arg, Var))
	{
		attnums = bms_add_member(attnums,
							((Var*)((NullTest*)clause)->arg)->varattno);
	}
	else if (or_clause(clause) || and_clause(clause) || or_clause(clause))
	{
		ListCell *l;
		foreach (l, ((BoolExpr*)clause)->args)
		{
			attnums = bms_join(attnums,
						clause_mv_get_attnums(root, (Node*)lfirst(l)));
		}
	}

	return attnums;
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
clauselist_mv_selectivity_mcvlist(PlannerInfo *root, MVStatisticInfo *mvstats,
								  List *clauses, List *conditions, bool is_or,
								  bool *fullmatch, Selectivity *lowsel)
{
	int i;
	Selectivity s = 0.0;
	Selectivity t = 0.0;
	Selectivity u = 0.0;

	MCVList mcvlist = NULL;

	int	nmatches = 0;
	int	nconditions = 0;

	/* match/mismatch bitmap for each MCV item */
	char * matches = NULL;
	char * condition_matches = NULL;

	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);

	/* there's no MCV list built yet */
	if (! mvstats->mcv_built)
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
	 * For AND clauses all buckets match (and we'll eliminate them).
	 * For OR  clauses no  buckets match (and we'll add them).
	 *
	 * We only need to do the memset for AND clauses (for OR clauses
	 * it's already set correctly by the palloc0).
	 */
	matches = palloc0(sizeof(char) * nmatches);

	if (! is_or) /* AND-clause */
		memset(matches, MVSTATS_MATCH_FULL, sizeof(char)*nmatches);

	/* Conditions are treated as AND clause, so match by default. */
	condition_matches = palloc0(sizeof(char) * nconditions);
	memset(condition_matches, MVSTATS_MATCH_FULL, sizeof(char)*nconditions);

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
	 * TODO This evaluates the clauses for all MCV items, even those
	 *      ruled out by the conditions. The final result should be the
	 *      same, but it might be faster.
	 */
	nmatches = update_match_bitmap_mcvlist(root, clauses,
										   mvstats->stakeys, mcvlist,
										   ((is_or) ? 0 : nmatches), matches,
										   lowsel, fullmatch, is_or);

	/* sum frequencies for all the matching MCV items */
	for (i = 0; i < mcvlist->nitems; i++)
	{
		/*
		 * Find out what part of the data is covered by the MCV list,
		 * so that we can 'scale' the selectivity properly (e.g. when
		 * only 50% of the sample items got into the MCV, and the rest
		 * is either in a histogram, or not covered by stats).
		 *
		 * TODO This might be handled by keeping a global "frequency"
		 *      for the whole list, which might save us a bit of time
		 *      spent on accessing the not-matching part of the MCV list.
		 *      Although it's likely in a cache, so it's very fast.
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
		return (Selectivity)0.0;

	return (s / t) * u;
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
		else if (or_clause(clause) || and_clause(clause) || not_clause(clause))
		{
			/* AND/OR clause, with all clauses compatible with the selected MV stat */

			int			i;
			List	   *tmp_clauses = ((BoolExpr*)clause)->args;

			/* match/mismatch bitmap for each MCV item */
			int	tmp_nmatches = 0;
			char * tmp_matches = NULL;

			Assert(tmp_clauses != NIL);
			Assert((list_length(tmp_clauses) >= 2) || (not_clause(clause) && (list_length(tmp_clauses)==1)));

			/* number of matching MCV items */
			tmp_nmatches = (or_clause(clause)) ? 0 : mcvlist->nitems;

			/* by default none of the MCV items matches the clauses */
			tmp_matches = palloc0(sizeof(char) * mcvlist->nitems);

			/* AND (and NOT) clauses assume everything matches, initially */
			if (! or_clause(clause))
				memset(tmp_matches, MVSTATS_MATCH_FULL, sizeof(char)*mcvlist->nitems);

			/* build the match bitmap for the OR-clauses */
			tmp_nmatches = update_match_bitmap_mcvlist(root, tmp_clauses,
									   stakeys, mcvlist,
									   tmp_nmatches, tmp_matches,
									   lowsel, fullmatch, or_clause(clause));

			/* merge the bitmap into the existing one*/
			for (i = 0; i < mcvlist->nitems; i++)
			{
				/* if this is a NOT clause, we need to invert the results first */
				if (not_clause(clause))
					tmp_matches[i] = (MVSTATS_MATCH_FULL - tmp_matches[i]);

				/*
				 * To AND-merge the bitmaps, a MIN() semantics is used.
				 * For OR-merge, use MAX().
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
clauselist_mv_selectivity_histogram(PlannerInfo *root, MVStatisticInfo *mvstats,
									List *clauses, List *conditions, bool is_or)
{
	int i;
	Selectivity s = 0.0;
	Selectivity t = 0.0;
	Selectivity u = 0.0;

	int		nmatches = 0;
	int		nconditions = 0;
	char   *matches = NULL;
	char   *condition_matches = NULL;

	MVSerializedHistogram mvhist = NULL;

	/* there's no histogram */
	if (! mvstats->hist_built)
		return 0.0;

	/* There may be no histogram in the stats (check hist_built flag) */
	mvhist = load_mv_histogram(mvstats->mvoid);

	Assert (mvhist != NULL);
	Assert (clauses != NIL);
	Assert (list_length(clauses) >= 1);

	nmatches = mvhist->nbuckets;
	nconditions = mvhist->nbuckets;

	/*
	 * Bitmap of bucket matches (mismatch, partial, full).
	 *
	 * For AND clauses all buckets match (and we'll eliminate them).
	 * For OR  clauses no  buckets match (and we'll add them).
	 *
	 * We only need to do the memset for AND clauses (for OR clauses
	 * it's already set correctly by the palloc0).
	 */
	matches = palloc0(sizeof(char) * nmatches);

	if (! is_or) /* AND-clause */
		memset(matches, MVSTATS_MATCH_FULL, sizeof(char)*nmatches);

	/* Conditions are treated as AND clause, so match by default. */
	condition_matches = palloc0(sizeof(char)*nconditions);
	memset(condition_matches, MVSTATS_MATCH_FULL, sizeof(char)*nconditions);

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
	 * TODO This evaluates the clauses for all buckets, even those
	 *      ruled out by the conditions. The final result should be
	 *      the same, but it might be faster.
	 */
	update_match_bitmap_histogram(root, clauses,
								  mvstats->stakeys, mvhist,
								  ((is_or) ? 0 : nmatches), matches,
								  is_or);

	/* now, walk through the buckets and sum the selectivities */
	for (i = 0; i < mvhist->nbuckets; i++)
	{
		float coeff = 1.0;

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
			 * TODO If both conditions and clauses match partially, this
			 *      will use 0.25 match - not sure if that's the right
			 *      thing solution, but seems about right.
			 */
			s += coeff * 0.5 * mvhist->buckets[i]->ntuples;
	}

	/* release the allocated bitmap and deserialized histogram */
	pfree(matches);
	pfree(condition_matches);
	pfree(mvhist);

	/* no condition matches */
	if (t == 0.0)
		return (Selectivity)0.0;

	return (s / t) * u;
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
								 * Now check whether constant is below the upper boundary (in that
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

								if (tmp)
								{
									/* partial match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_PARTIAL, is_or);
									continue;
								}


								/*
								 * And finally check whether the whether the constant is above the the upper
								 * boundary (in that case it's a full match match).
								 *
								 * XXX We need to do this because of the OR clauses (which start with no
								 *     matches and we incrementally add more and more matches), but maybe
								 *     we don't need to do the check and can just do UPDATE_RESULT?
								 */
								tmp = DatumGetBool(FunctionCall2Coll(&opproc,
																	 DEFAULT_COLLATION_OID,
																	 maxval,
																	 cst->constvalue));

								if (tmp)
								{
									/* full match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_FULL, is_or);
								}

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

								if (tmp)
								{
									/* partial match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_PARTIAL, is_or);
									continue;
								}

								/*
								 * Now check whether the lower boundary is below the constant (in that
								 * case it's a partial match).
								 *
								 * XXX We need to do this because of the OR clauses (which start with no
								 *     matches and we incrementally add more and more matches), but maybe
								 *     we don't need to do the check and can just do UPDATE_RESULT?
								 */
								tmp = DatumGetBool(FunctionCall2Coll(&opproc,
																	 DEFAULT_COLLATION_OID,
																	 cst->constvalue,
																	 minval));

								if (tmp)
									/* partial match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_FULL, is_or);

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
								{
									/* partial match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_PARTIAL, is_or);
									continue;
								}

								/*
								 * Now check whether the lower boundary is below the constant (in that
								 * case it's a partial match).
								 *
								 * XXX We need to do this because of the OR clauses (which start with no
								 *     matches and we incrementally add more and more matches), but maybe
								 *     we don't need to do the check and can just do UPDATE_RESULT?
								 */
								tmp = DatumGetBool(FunctionCall2Coll(&opproc,
																	 DEFAULT_COLLATION_OID,
																	 minval,
																	 cst->constvalue));

								if (tmp)
									/* partial match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_FULL, is_or);

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
								{
									/* partial match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_PARTIAL, is_or);
									continue;
								}

								/*
								 * Now check whether the upper boundary is below the constant (in that
								 * case it's a partial match).
								 *
								 * XXX We need to do this because of the OR clauses (which start with no
								 *     matches and we incrementally add more and more matches), but maybe
								 *     we don't need to do the check and can just do UPDATE_RESULT?
								 */
								tmp = DatumGetBool(FunctionCall2Coll(&opproc,
																	 DEFAULT_COLLATION_OID,
																	 cst->constvalue,
																	 maxval));

								if (tmp)
									/* partial match */
									UPDATE_RESULT(matches[i], MVSTATS_MATCH_FULL, is_or);
									continue;

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
		else if (or_clause(clause) || and_clause(clause) || not_clause(clause))
		{
			/* AND/OR clause, with all clauses compatible with the selected MV stat */

			int			i;
			List	   *tmp_clauses = ((BoolExpr*)clause)->args;

			/* match/mismatch bitmap for each bucket */
			int	tmp_nmatches = 0;
			char * tmp_matches = NULL;

			Assert(tmp_clauses != NIL);
			Assert((list_length(tmp_clauses) >= 2) || (not_clause(clause) && (list_length(tmp_clauses)==1)));

			/* number of matching buckets */
			tmp_nmatches = (or_clause(clause)) ? 0 : mvhist->nbuckets;

			/* by default none of the buckets matches the clauses (OR clause) */
			tmp_matches = palloc0(sizeof(char) * mvhist->nbuckets);

			/* but AND (and NOT) clauses assume everything matches, initially */
			if (! or_clause(clause))
				memset(tmp_matches, MVSTATS_MATCH_FULL, sizeof(char)*mvhist->nbuckets);

			/* build the match bitmap for the OR-clauses */
			tmp_nmatches = update_match_bitmap_histogram(root, tmp_clauses,
										stakeys, mvhist,
										tmp_nmatches, tmp_matches, or_clause(clause));

			/* merge the bitmap into the existing one*/
			for (i = 0; i < mvhist->nbuckets; i++)
			{
				/* if this is a NOT clause, we need to invert the results first */
				if (not_clause(clause))
					tmp_matches[i] = (MVSTATS_MATCH_FULL - tmp_matches[i]);

				/*
				 * To AND-merge the bitmaps, a MIN() semantics is used.
				 * For OR-merge, use MAX().
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

#ifdef DEBUG_MVHIST
	debug_histogram_matches(mvhist, matches);
#endif

	return nmatches;
}

/*
 * Walk through clauses and keep only those covered by at least
 * one of the statistics.
 */
static List *
filter_clauses(PlannerInfo *root, Oid varRelid, SpecialJoinInfo *sjinfo,
			   int type, List *stats, List *clauses, Bitmapset **attnums)
{
	ListCell   *c;
	ListCell   *s;

	/* results (list of compatible clauses, attnums) */
	List	   *rclauses = NIL;

	foreach (c, clauses)
	{
		Node *clause = (Node*)lfirst(c);
		Bitmapset *clause_attnums = NULL;
		Index relid;

		/*
		 * The clause has to be mv-compatible (suitable operators etc.).
		 */
		if (! clause_is_mv_compatible(root, clause, varRelid,
							 &relid, &clause_attnums, sjinfo, type))
				elog(ERROR, "should not get non-mv-compatible cluase");

		/* is there a statistics covering this clause? */
		foreach (s, stats)
		{
			int k, matches = 0;
			MVStatisticInfo	*stat = (MVStatisticInfo *)lfirst(s);

			for (k = 0; k < stat->stakeys->dim1; k++)
			{
				if (bms_is_member(stat->stakeys->values[k],
								  clause_attnums))
					matches += 1;
			}

			/*
			 * The clause is compatible if all attributes it references
			 * are covered by the statistics.
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
 * Walk through statistics and only keep those covering at least
 * one new attribute (excluding conditions) and at two attributes
 * in both clauses and conditions.
 *
 * This check might be made more strict by checking against individual
 * clauses, because by using the bitmapsets of all attnums we may
 * actually use attnums from clauses that are not covered by the
 * statistics. For example, we may have a condition
 *
 *    (a=1 AND b=2)
 *
 * and a new clause
 *
 *    (c=1 AND d=1)
 *
 * With only bitmapsets, statistics on [b,c] will pass through this
 * (assuming there are some statistics covering both clases).
 *
 * TODO Do the more strict check.
 */
static List *
filter_stats(List *stats, Bitmapset *new_attnums, Bitmapset *all_attnums)
{
	ListCell   *s;
	List	   *stats_filtered = NIL;

	foreach (s, stats)
	{
		int k;
		int matches_new = 0,
			matches_all = 0;

		MVStatisticInfo	*stat = (MVStatisticInfo *)lfirst(s);

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
	int i;
	ListCell   *l;

	MVStatisticInfo *mvstats = NULL;
	*nmvstats = list_length(stats);

	mvstats
		= (MVStatisticInfo*)palloc0((*nmvstats) * sizeof(MVStatisticInfo));

	i = 0;
	foreach (l, stats)
	{
		MVStatisticInfo	*stat = (MVStatisticInfo *)lfirst(l);
		memcpy(&mvstats[i++], stat, sizeof(MVStatisticInfo));
	}

	return mvstats;
}

static Bitmapset **
make_stats_attnums(MVStatisticInfo *mvstats, int nmvstats)
{
	int			i, j;
	Bitmapset **stats_attnums = NULL;

	Assert(nmvstats > 0);

	/* build bitmaps of attnums for the stats (easier to compare) */
	stats_attnums = (Bitmapset **)palloc0(nmvstats * sizeof(Bitmapset*));

	for (i = 0; i < nmvstats; i++)
		for (j = 0; j < mvstats[i].stakeys->dim1; j++)
			stats_attnums[i]
				= bms_add_member(stats_attnums[i],
								 mvstats[i].stakeys->values[j]);

	return stats_attnums;
}


/*
 * Now let's remove redundant statistics, covering the same columns
 * as some other stats, when restricted to the attributes from
 * remaining clauses.
 *
 * If statistics S1 covers S2 (covers S2 attributes and possibly
 * some more), we can probably remove S2. What actually matters are
 * attributes from covered clauses (not all the attributes). This
 * might however prefer larger, and thus less accurate, statistics.
 *
 * When a redundancy is detected, we simply keep the smaller
 * statistics (less number of columns), on the assumption that it's
 * more accurate and faster to process. That might be incorrect for
 * two reasons - first, the accuracy really depends on number of
 * buckets/MCV items, not the number of columns. Second, we might
 * prefer MCV lists over histograms or something like that.
 */
static List*
filter_redundant_stats(List *stats, List *clauses, List *conditions)
{
	int i, j, nmvstats;

	MVStatisticInfo	   *mvstats;
	bool			   *redundant;
	Bitmapset		  **stats_attnums;
	Bitmapset		   *varattnos;
	Index				relid;

	Assert(list_length(stats) > 0);
	Assert(list_length(clauses) > 0);

	/*
	 * We'll convert the list of statistics into an array now, because
	 * the reduction of redundant statistics is easier to do that way
	 * (we can mark previous stats as redundant, etc.).
	 */
	mvstats = make_stats_array(stats, &nmvstats);
	stats_attnums = make_stats_attnums(mvstats, nmvstats);

	/* by default, none of the stats is redundant (so palloc0) */
	redundant = palloc0(nmvstats * sizeof(bool));

	/*
	 * We only expect a single relid here, and also we should get the
	 * same relid from clauses and conditions (but we get it from
	 * clauses, because those are certainly non-empty).
	 */
	relid = bms_singleton_member(pull_varnos((Node*)clauses));

	/*
	 * Get the varattnos from both conditions and clauses.
	 *
	 * This skips system attributes, although that should be impossible
	 * thanks to previous filtering out of incompatible clauses.
	 *
	 * XXX Is that really true?
	 */
	varattnos = bms_union(get_varattnos((Node*)clauses, relid),
						  get_varattnos((Node*)conditions, relid));

	for (i = 1; i < nmvstats; i++)
	{
		/* intersect with current statistics */
		Bitmapset *curr = bms_intersect(stats_attnums[i], varattnos);

		/* walk through 'previous' stats and check redundancy */
		for (j = 0; j < i; j++)
		{
			/* intersect with current statistics */
			Bitmapset *prev;

			/* skip stats already identified as redundant */
			if (redundant[j])
				continue;

			prev = bms_intersect(stats_attnums[j], varattnos);

			switch (bms_subset_compare(curr, prev))
			{
				case BMS_EQUAL:
					/*
					 * Use the smaller one (hopefully more accurate).
					 * If both have the same size, use the first one.
					 */
					if (mvstats[i].stakeys->dim1 >= mvstats[j].stakeys->dim1)
						redundant[i] = TRUE;
					else
						redundant[j] = TRUE;

					break;

				case BMS_SUBSET1: /* curr is subset of prev */
					redundant[i] = TRUE;
					break;

				case BMS_SUBSET2: /* prev is subset of curr */
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

static Node**
make_clauses_array(List *clauses, int *nclauses)
{
	int i;
	ListCell *l;

	Node** clauses_array;

	*nclauses = list_length(clauses);
	clauses_array = (Node **)palloc0((*nclauses) * sizeof(Node *));

	i = 0;
	foreach (l, clauses)
		clauses_array[i++] = (Node *)lfirst(l);

	*nclauses = i;

	return clauses_array;
}

static Bitmapset **
make_clauses_attnums(PlannerInfo *root, Oid varRelid, SpecialJoinInfo *sjinfo,
					 int type, Node **clauses, int nclauses)
{
	int			i;
	Index		relid;
	Bitmapset **clauses_attnums
		= (Bitmapset **)palloc0(nclauses * sizeof(Bitmapset *));

	for (i = 0; i < nclauses; i++)
	{
		Bitmapset * attnums = NULL;

		if (! clause_is_mv_compatible(root, clauses[i], varRelid,
									  &relid, &attnums, sjinfo, type))
			elog(ERROR, "should not get non-mv-compatible cluase");

		clauses_attnums[i] = attnums;
	}

	return clauses_attnums;
}

static bool*
make_cover_map(Bitmapset **stats_attnums, int nmvstats,
			   Bitmapset **clauses_attnums, int nclauses)
{
	int		i, j;
	bool   *cover_map	= (bool*)palloc0(nclauses * nmvstats);

	for (i = 0; i < nmvstats; i++)
		for (j = 0; j < nclauses; j++)
			cover_map[i * nclauses + j]
				= bms_is_subset(clauses_attnums[j], stats_attnums[i]);

	return cover_map;
}
