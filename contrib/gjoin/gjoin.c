/*
 * gjoin.c - implementation of custom scan access path
 *
 *
 * An experimental implementation of the GJoin access method, described
 * in a research paper:
 *
 * * A generalized join algorithm, Goetz Graefe
 *   https://dl.gi.de/items/db73bc60-a9df-4067-a426-0a174026099a
 *
 * The join path is implemented using a CustomJoin. The basic steps happen
 * in the following methods
 *
 * - gjoin_join_pathlist_hook - create CustomPath paths, mimicking the
 *   plain NestLoop join paths
 *
 * - gjoin_create_plan - crates CustomJoin plan, mimicking NestLoop
 *
 * - gjoin_create_plan_state - initializes runtime state of the plan
 *   during execution
 *
 * The executor methods are implemented by traditional several methods:
 *
 * - gjoin_BeginCustomJoin
 * - gjoin_ExecCustomJoin
 * - gjoin_EndCustomJoin
 * - gjoin_ReScanCustomJoin
 * - gjoin_ExplainCustomJoin
 *
 * XXX The paper implementation is based on splitting data into "pages",
 * and managing the memory based on that. But that's not what this code
 * does exactly, it relies on tuplesort (which does 8K pages internally,
 * but the API exposes tuples). So this code groups stuff into buffers
 * (or rather "batches"), instead of pages. So that the logic is similar
 * to what the paper says. But it's also a bit strange, and maybe it
 * could allow some optimizations (e.g. we could make join ranges more
 * focused, split pages join instead of requiring the range to be fully
 * covered, etc.).
 *
 *
 * Copyright (C) Tomas Vondra, 2025
 */

#include "postgres.h"

#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_opfamily.h"
#include "commands/explain_format.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "executor/nodeIndexscan.h"
#include "lib/pairingheap.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "nodes/supportnodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/paramassign.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/selfuncs.h"
#include "utils/tuplesort.h"

PG_MODULE_MAGIC;

static bool gjoin_enabled = false;

/* planner */
static void gjoin_join_pathlist_hook(PlannerInfo *root,
									 RelOptInfo *joinrel,
									 RelOptInfo *outerrel,
									 RelOptInfo *innerrel,
									 JoinType jointype,
									 JoinPathExtraData *extra);

static set_join_pathlist_hook_type prev_set_join_pathlist_hook = NULL;

static Path *create_gjoin_path(PlannerInfo *root, RelOptInfo *joinrel,
							   RelOptInfo *outerrel, RelOptInfo *innerrel,
							   JoinType jointype, JoinPathExtraData *extra);

/* executor */
static Plan *gjoin_create_plan(PlannerInfo *root,
							   RelOptInfo *rel,
							   CustomPath *best_path,
							   List *tlist,
							   List *clauses,
							   List *custom_plans);
static Node *gjoin_create_plan_state(CustomJoin *cjoin);

/* executor */
static void gjoin_BeginCustomJoin(CustomJoinState *node,
									   EState *estate,
									   int eflags);
static TupleTableSlot *gjoin_ExecCustomJoin(CustomJoinState *node);
static void gjoin_EndCustomJoin(CustomJoinState *node);
static void gjoin_ReScanCustomJoin(CustomJoinState *node);
static void gjoin_ExplainCustomJoin(CustomJoinState *node,
									List *ancestors,
									ExplainState *es);

static CustomPathMethods		gjoin_path_methods;
static CustomJoinMethods		gjoin_plan_methods;
static CustomJoinExecMethods	gjoin_exec_methods;

/*
 * A buffer of tuples, loaded from a relation.
 *
 * XXX we should probably keep an array of slots, to save on the tuple/slot
 * conversions when not needed
 */
typedef struct GJoinBuffer
{
	Size		space;
	int			ntuples;
	int			maxtuples;
	HeapTuple   *tuples;
} GJoinBuffer;

/*
 * An array of "runs" in the gjoin algorithm. Each run is sorted, so we
 * represent it as a tuplesort.
 *
 * XXX Maybe tuplesort is not the right abstraction, and writing into a
 * tuplesort from the beginning may be premature. The algorithm does allow
 * performing a hash join in some cases, in which case the sort is not
 * necessary. So maybe write to tuplestore first, and only flip into sorted
 * mode later? But we don't want to write everything twice (into tuplestore
 * and then shuffle everything into tuplesort), so maybe do that half-way
 * through?
 */
typedef struct GJoinRuns
{
	int					maxruns;
	int					nruns;
	int				   *ntuples;
	Tuplesortstate	  **runs;
} GJoinRuns;

/* XXX should be based on memory usage instead */
#define	MAX_SLOTS_PER_BUFFER 128

/*
 * A "buffer" of tuples. 
 *
 * A buffer (batch) of tuples loaded from one run. The buffers form a linked
 * list, so that a run can have multiple buffers loaded at a time.
 *
 * FIXME The min/max values should be arrays, for multi-column join clauses.
 */
typedef struct TupleBuffer
{
	/* number of keys in min/max arrays */
	int				nkeys;

	/* minimum values */
	bool			min_empty;		/* no lower bound */
	Datum		   *min_values;
	bool		   *min_isnull;

	/* maximum values */
	Datum		   *max_values;
	bool		   *max_isnull;

	int				maxslots;
	int				nslots;
	TupleTableSlot **slots;

	dlist_node		node;		/* doubly-linked list */
} TupleBuffer;

/*
 * Position in the currently loaded runs. The indexes determine the run, and
 * the slot in the current buffer (in the doubly-linked list).
 */
typedef struct GJoinPosition
{
	int		run;
	int		slot;
	TupleBuffer *buffer;
} GJoinPosition;

/*
 * Information needed for sorting the inner/outer side.
 *
 * FIXME gjoin_create_plan_state hard-codes a lot of the information. It
 * should be derived from the join conditions instead.
 *
 * FIXME The number of elements is stored in state->clauses.numcols.
 *
 * FIXME the name is a bit misleading, as it's used for the join clause
 * evaluation too, not just for the sort
 */
typedef struct GJoinClauseInfo
{
	int				nattnums;
	AttrNumber	   *attnums_inner;
	AttrNumber	   *attnums_outer;
	Oid			   *equality;		/* = operators */
	Oid			   *inequality;		/* < operators */
	Oid			   *collations;
	bool		   *nulls_first;
} GJoinClauseInfo;

/* entry in a priority queue, implemented using the pairing heap */
typedef struct QueueEntry
{
	pairingheap_node ph_node;
	int		run;
	/* FIXME this should be an array; for multi-column sort keys */
	Datum  *values;
} QueueEntry;

static int	priorityqueues_min_cmp(const pairingheap_node *a,
								  const pairingheap_node *b, void *arg);
static void priorityqueues_push(pairingheap *heap, int run, Datum *values);
static QueueEntry *priorityqueues_pop(pairingheap *heap);
static QueueEntry *priorityqueues_peek(pairingheap *heap);

/*
 * Phased of the gjoin state machine.
 *
 * XXX We should follow the logic that (R < S), and R = inner, S = outer.
 */
typedef enum GJoinPhase {
	GJOIN_INIT,			/* initial state */
	GJOIN_BUILD_INNER,	/* build runs for inner */
	GJOIN_BUILD_OUTER,	/* build runs for outer */
	GJOIN_INIT_INNER,	/* prepare inner runs for matching */
	GJOIN_INIT_OUTER,	/* prepare outer runs for matching */
	GJOIN_LOAD_INNER,	/* load a buffer of tuples for R */
	GJOIN_LOAD_OUTER,	/* load a buffer of tuples for S */
	GJOIN_NEXT_OUTER,	/* advance to the next outer tuple */
	GJOIN_NEXT_INNER,	/* advance to the next inner tuple */
	GJOIN_EVICT_INNER
} GJoinPhase;

/* ----------------
 *	 GJoinJoinState information
 *
 * ----------------
 */
typedef struct GJoinJoinState
{
	CustomJoinState	cstate;

	/* TODO */
	PlanState	   *outerstate;
	PlanState	   *innerstate;

	GJoinPhase		phase;

	/*
	 * sorted runs for inner/outer side
	 *
	 * We just use an array of tuplesorts. We could do our own merge sort, but
	 * that seems unlikely to beat the existing implementation.
	 *
	 * XXX We need to be careful about the number of files we're keeping
	 * open, to not end with the same memory "explosion" issue as hashjoin.
	 *
	 * XXX The annoying drawback is that this keeps work_mem for each sort,
	 * and that needs fixing. Actually, is that true? Maybe it's only for
	 * the sorting (but we do the one by one), not for reading the sorted
	 * data afterwards?
	 */
	struct {
		GJoinRuns	inner;
		GJoinRuns	outer;
	} runs;

	/*
	 * Buffer of tuples to be written to tuplesort. We don't write the tuples
	 * to the tuplesort right away, because maybe if it fits into work_mem
	 * we could do away without a sort.
	 *
	 * For ingesting tuples (when building the runs) we only need one buffer
	 * for each relation (In fact, maybe we could use a single buffer for
	 * both relations?). We will use multiple buffers during the matching
	 * during join later.
	 *
	 * XXX So we can use much larger buffer.
	 */
	 struct {
		GJoinBuffer	inner;
		GJoinBuffer	outer;
	} buffer;

	/*
	 * information for sorting the input data (derived from join clauses)
	 *
	 * XXX we should allow postponing the sort, in case we can do hash join
	 *
	 * XXX another thing is the sort may not be needed at all, if the input
	 * paths are already sorted
	 *
	 * XXX should handle expressions too, not just plain attnums
	 */
	GJoinClauseInfo	clauses;

	/*
	 * priority queues from the algorithm, described by the paper
	 *
	 * XXX Uses a simplified variant described in the paper on p. 6 (272),
	 * using the newest buffer for queue C (and not using D at all). That
	 * way we don't need to look ahead at the next page.
	 */
	struct {
		pairingheap *inner_grow;	/* R min(maxval) / newest buffer */
		pairingheap *inner_shrink;	/* R min(maxval) / oldest buffer */
		pairingheap *outer;			/* S min(maxval) / newest buffer */
	} queues;

	/*
	 * join range (calculated from R), determines which S buffer can be
	 * joined with currently loaded buffers.
	 *
	 * FIXME does not handle NULL values (it's not the same as not set)
	 */
	struct {
		Datum  *min_values;
		Datum  *max_values;
		bool   *min_value_set;
		bool   *max_value_set;
	} join_range;

	/*
	 * Buffers used during the actual join, when combining tuples from inner
	 * and outer side. Each run gets a doubly-linked list of buffers.
	 */
	dlist_head	   *buffers_inner;
	dlist_head	   *buffers_outer;

	GJoinPosition	pos_inner;
	GJoinPosition	pos_outer;

} GJoinJoinState;


void
_PG_init(void)
{
	DefineCustomBoolVariable(
		"gjoin.enabled",
		"whether to generate GJoin paths",
		NULL, &gjoin_enabled, false, PGC_USERSET, 0, NULL, NULL, NULL);

	/* custom-scan node */
	memset(&gjoin_path_methods, 0, sizeof(CustomPathMethods));
	gjoin_path_methods.CustomName   = "GJoin";
	gjoin_path_methods.PlanCustomPath = gjoin_create_plan;

	memset(&gjoin_plan_methods, 0, sizeof(CustomJoinMethods));
	gjoin_plan_methods.CustomName   = "GJoin";
	gjoin_plan_methods.CreateCustomJoinState = gjoin_create_plan_state;

	memset(&gjoin_exec_methods, 0, sizeof(CustomJoinExecMethods));
	gjoin_exec_methods.CustomName   = "GJoin";
	gjoin_exec_methods.BeginCustomJoin = gjoin_BeginCustomJoin;
	gjoin_exec_methods.ExecCustomJoin = gjoin_ExecCustomJoin;
	gjoin_exec_methods.EndCustomJoin = gjoin_EndCustomJoin;
	gjoin_exec_methods.ReScanCustomJoin = gjoin_ReScanCustomJoin;
	gjoin_exec_methods.ExplainCustomJoin = gjoin_ExplainCustomJoin;

	/* XXX no mark/restore for now */
	gjoin_exec_methods.MarkPosCustomJoin = NULL;
	gjoin_exec_methods.RestrPosCustomJoin = NULL;

	/* XXX no parallel execution */
	gjoin_exec_methods.EstimateDSMCustomJoin = NULL;
	gjoin_exec_methods.InitializeDSMCustomJoin = NULL;
	gjoin_exec_methods.ReInitializeDSMCustomJoin = NULL;
	gjoin_exec_methods.InitializeWorkerCustomJoin = NULL;
	gjoin_exec_methods.ShutdownCustomJoin = NULL;

	/* install hook to create the CustomScan paths */
	prev_set_join_pathlist_hook = set_join_pathlist_hook;
	set_join_pathlist_hook = gjoin_join_pathlist_hook;
}

/*
 * gjoin_join_pathlist_hook
 *		Create a GJoin path (represented by a CustomPath node).
 *
 * Does about the same thing as create_nestloop_path.
 */
static void
gjoin_join_pathlist_hook(PlannerInfo *root, RelOptInfo *joinrel,
						 RelOptInfo *outerrel, RelOptInfo *innerrel,
						 JoinType jointype, JoinPathExtraData *extra)
{
	Path   *path;

	if (prev_set_join_pathlist_hook)
		prev_set_join_pathlist_hook(root, joinrel, outerrel, innerrel,
									jointype, extra);

	/* if gjoin disabled, bail out */
	if (!gjoin_enabled)
		return;

	path = create_gjoin_path(root, joinrel, outerrel, innerrel, jointype, extra);
	add_path(joinrel, path);

	elog(DEBUG1, "consider GJoin path %p", path);
}

static bool
clause_is_compatible(Expr *clause)
{
	OpExpr *opclause;
	Var	   *var1,
		   *var2;
	TypeCacheEntry *typentry;

	/* we only care about (Var op Var) clauses */
	if (!IsA(clause, OpExpr))
		return false;

	opclause = (OpExpr *) clause;

	var1 = linitial(opclause->args);
	var2 = lsecond(opclause->args);

	if (!IsA(var1, Var) || !IsA(var2, Var))
		return false;

	/*
	 * Also require both sides of the clause to use the same data type.
	 *
	 * XXX Probably not strictly necessary, but it makes it easier to
	 * determine if the operator is equality etc.
	 */
	if (var1->vartype != var2->vartype)
		return false;

	/*
	 * Is the operator is an equality?
	 *
	 * XXX What's the right / generic way to do this? An operator may
	 * be in multiple opclasses, etc. For now just lookup the default
	 * btree opclass, and rely on that.
	 */
	typentry = lookup_type_cache(var1->vartype, TYPECACHE_EQ_OPR);
	if (opclause->opno != typentry->eq_opr)
		return false;

	return true;
}


/*
 * create_gjoin_path
 *		Create the CustomPath representing the GJoin.
 *
 * Returns NULL if GJoin is not supported for this join.
 *
 * XXX This should walk all paths for inner/outer path, and try to create
 * a gjoin for each combination, depending on the pathkeys etc. Some will
 * have "compatible" pathkeys, others will require a sort (in gjoin), etc.
 *
 * XXX We should also verify support for hashing for the data types. We
 * can still do GJoin without hashing, but without the hashjoin mode.
 *
 * XXX In principle, we should not generate too many paths here. The whole
 * point of gjoin is to move decisions to execution time, and having many
 * paths here means we'll need to cost them and pick cheapest one. That
 * goes against the gjoin idea.
 *
 * XXX This also needs to derive the right output pathkeys. A GJoin that
 * needs to produce pathkeys may have additional restrictions (e.g. it
 * can't do hashjoin mode easily, I think). So maybe we should produce
 * multiple paths, one with pathkeys, one without pathkeys.
 *
 * XXX So this should probably produce a list of paths, not just one path.
 *
 * XXX no parallelism support for now, but I guess it could be made to work
 * with parallelism, if the caches are shared, etc.
 *
 * XXX No support for CUSTOMPATH_SUPPORT_PROJECTION, could it be made to
 * work, maybe? Not sure how beneficial it is, really.
 *
 * XXX Cost this properly. The current costing is entirely bogus, to make
 * gjoin look like the best join path, but it likely skews planning of the
 * nodes above (because it looks absurdly cheap). We should at least set
 * the row count estimate properly. See the function comment for details
 * about cost model.
 *
 * XXX But the costing is a bit tricky, because gjoin is meant to be robust,
 * not cheap. So it'll mostly lose. And it's also for cases when we want
 * to deal with inaccurate estimates, hence why should a cost be a good
 * basis anyway? Maybe we should forcefully remove the "regular" paths in
 * gjoin_join_pathlist_hook, and keep just the GJoin?
 *
 * XXX Also should order so that (R < S). This is not really decided until
 * the execution, i.e. we can rethink that later. But let's try getting
 * that right during planning already.
 */
static Path *
create_gjoin_path(PlannerInfo *root, RelOptInfo *joinrel,
				  RelOptInfo *outerrel, RelOptInfo *innerrel,
				  JoinType jointype, JoinPathExtraData *extra)
{
	CustomPath	   *cpath;
	Relids			required_outer;
	ParamPathInfo  *param_info;
	List		   *restrict_clauses = extra->restrictlist;
	bool			has_usable_join_clause = false;
	ListCell	   *lc;

	/*
	 * pick inner/outer paths to join
	 *
	 * FIXME Any paths can be joined, but maybe the paths with cheapest
	 * startup are not the best ones. It might be better to get cheapest
	 * total paths and do sort. Or the paths may be sorted, and then we
	 * don't need to do additional sort.
	 */
	Path *outer_path = outerrel->cheapest_startup_path;
	Path *inner_path = innerrel->cheapest_startup_path;

	required_outer = calc_non_nestloop_required_outer(outer_path,
													  inner_path);

	/* FIXME validate required outer */

	param_info = get_joinrel_parampathinfo(root,
										   joinrel,
										   outer_path,
										   inner_path,
										   extra->sjinfo,
										   required_outer,
										   &restrict_clauses);

	/*
	 * Don't allow cartesian products (the following check would have the
	 * same effect, but it seems reasonable to do this cheap check now).
	 */
	if (restrict_clauses == NIL)
		return NULL;

	/*
	 * Make sure there's at least one join clause with (Var op Var), where
	 * the operator is equality. The remaining join clauses will be treated
	 * as join filters.
	 *
	 * XXX For now we support only joins with a single (var op var) clause.
	 *
	 * We should really support multiple join clauses, but we probably should
	 * stick to (var op var) for clauses, to keep things simple. Such clauses
	 * would be used by gjoin, and the rest would be turned into a filter.
	 */
	foreach (lc, restrict_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/* paranoia */
		Assert(IsA(rinfo, RestrictInfo));

		if (!clause_is_compatible(rinfo->clause))
			continue;

		/* found a join clause usable for gjoin */
		has_usable_join_clause = true;
		break;
	}

	/* no usable join equality clause found, can't build gjoin path */
	if (!has_usable_join_clause)
		return NULL;

 	/* build the path, fill the info */
	cpath = makeNode(CustomPath);
	cpath->path.pathtype		= T_CustomJoin;
	cpath->path.parent			= joinrel;
	cpath->path.pathtarget		= joinrel->reltarget;
	cpath->path.param_info		= param_info; /* FIXME */
	cpath->path.pathkeys		= NIL; /* FIXME derive output pathkeys */

	/* XXX no parallelism */
	cpath->path.parallel_aware	= false;
	cpath->path.parallel_safe	= false;
	cpath->path.parallel_workers = -1;

	/* XXX no support for CUSTOMPATH_SUPPORT_PROJECTION */
	cpath->flags				= 0;
	cpath->custom_paths			= NIL;
	cpath->methods				= &gjoin_path_methods;

	/* no custom path private info for now */
	cpath->custom_private = NIL;

	/* add the outer/inner paths into custom_paths */
	cpath->custom_paths = lappend(cpath->custom_paths, outer_path);
	cpath->custom_paths = lappend(cpath->custom_paths, inner_path);

	/*
	 * Remember the custom restrictinfos (all of them, both the clauses
	 * that work for gjoin, and those that will be treated as filters).
	 */
	cpath->custom_restrictinfo = restrict_clauses;

	/* XXX fake costing, to make gjoin look like the best join path */
	cpath->path.rows = joinrel->rows;
	cpath->path.startup_cost	= 0.0;
	cpath->path.total_cost		= 1.0;

	return (Path *) cpath;
}

/*
 * Transform the CustomPath gjoin path into a CustomJoin plan.
 *
 * gjoin_create_plan
 *		Transform the CustomPath gjoin path into a CustomScan plan.
 *
 * Most of the steps are common in other create_plan methods. We also need
 * to do CustomJoin stuff, to make setrefs do the right thing.
 */
static Plan *
gjoin_create_plan(PlannerInfo *root,
				  RelOptInfo *rel,
				  CustomPath *best_path,
				  List *tlist,
				  List *clauses,
				  List *custom_plans)
{
	CustomJoin *cjoin;
	Plan	   *outerplan,
			   *innerplan;
	// List	   *params = (List *) best_path->custom_private;
	ListCell   *lc;
	List	   *join_clauses = NIL;

	outerplan = (Plan *) list_nth(custom_plans, 0);
	innerplan = (Plan *) list_nth(custom_plans, 1);

	cjoin = makeNode(CustomJoin);

	/* copy_generic_path_info is static */
	cjoin->join.plan.disabled_nodes = best_path->path.disabled_nodes;
	cjoin->join.plan.startup_cost = best_path->path.startup_cost;
	cjoin->join.plan.total_cost = best_path->path.total_cost;
	cjoin->join.plan.plan_rows = best_path->path.rows;
	cjoin->join.plan.plan_width = best_path->path.pathtarget->width;
	cjoin->join.plan.parallel_aware = best_path->path.parallel_aware;
	cjoin->join.plan.parallel_safe = best_path->path.parallel_safe;

	/* copy the stuff not handled by copy_generic_path_info */
	cjoin->join.plan.targetlist = tlist;
	cjoin->join.plan.qual = NIL;
	cjoin->join.plan.lefttree = outerplan;
	cjoin->join.plan.righttree = innerplan;

	/* return the whole base relation tuple */
	cjoin->custom_join_tlist = tlist;

	/* set the CustomJoin stuff */
	cjoin->methods = &gjoin_plan_methods;
	cjoin->custom_private = NIL;
	cjoin->custom_plans = custom_plans;

	/*
	 * add expression from the target list, so that setrefs processes it
	 *
	 * XXX Do we actually need to add the expressions? If yes, can we add
	 * the whole tlist, without iterating the items?
	 */
	cjoin->custom_exprs = NIL;
	// foreach (lc, tlist)
	// {
	//	TargetEntry *te = (TargetEntry *) lfirst(lc);
	//	cjoin->custom_exprs = lappend(cjoin->custom_exprs, (Node *) te->expr);
	// }
 
	/*
	 * Handle the join clauses. We need to add them to custom_private (so that
	 * we can use that for the join later). And we also need to add it to
	 * custom_exprs, so that it gets processed by setrefs.
	 *
	 * XXX At the moment we only allow a single join clause (we don't build
	 * the path if there are multiple), but we expect to support more, so we
	 * build a list.
	 *
	 * XXX This is not actually working, correctly. First, setrefs will copy
	 * the expression (var), and adjust that - not the original var. That
	 * means the two lists will get out of sync - only the custom_exprs will
	 * be modified. The bigger issue is setrefs doesn't handle custom joins,
	 * and treats them exactly like custom scans. That means it varnos are
	 * replaced with INDEX_VAR in all vars, and so we lose the ability to
	 * distinguish the inner/outer side.
	 */
	foreach (lc, best_path->custom_restrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/* paranoia */
		Assert(IsA(rinfo, RestrictInfo));

		/*
		 * If it's a compatible clause (Var op Var), treat it as a join
		 * clause for the gjoin. Otherwise just use it as a regular filter
		 * clause for the join.
		 */
		if (clause_is_compatible(rinfo->clause))
		{
			join_clauses = lappend(join_clauses, rinfo->clause);
			cjoin->custom_exprs = lappend(cjoin->custom_exprs,
										  rinfo->clause);
			continue;
		}

		/*
		 * Treat it as a filter, not used for the gjoin algorithm itself.
		 *
		 * XXX We don't need to add it to custom_exprs, I think.
		 */
		cjoin->join.plan.qual = lappend(cjoin->join.plan.qual,
										rinfo->clause);
	}

	/* remember the join clauses, so that we can evalute it later */
	cjoin->custom_private = lappend(cjoin->custom_private, join_clauses);

	/* XXX Should we add qpqual too? Probably not. */

	return (Plan *) cjoin;
}

/*
 * gjoin_buffer_init
 *		initialize a buffer for tuples
 *
 * We only reset fields to "empty", we don't allocate any buffer yet.
 */
static void
gjoin_buffer_init(GJoinBuffer *buffer)
{
	buffer->tuples = NULL;
	buffer->ntuples = 0;
	buffer->maxtuples = 0;
	buffer->space = 0;
}

/*
 * gjoin_runs_init
 *		initialize runs of buffers
 *
 * We only reset fields to "empty", we don't allocate any buffer yet.
 */
static void
gjoin_runs_init(GJoinRuns *runs)
{
	runs->maxruns = 0;
	runs->nruns = 0;
	runs->ntuples = NULL;
	runs->runs = NULL;
}

/* close the runs - release the tuplesorts, etc. */
static void
gjoin_runs_close(GJoinRuns *runs)
{
	/*
	 * now also end the tuplesort, to prevent warnings about resources
	 *
	 * XXX this should happen much later, after the join
	 */
	for (int i = 0; i < runs->nruns; i++)
	{
		/* stop on the first non-initialized run */
		if (runs->runs[i] == NULL)
			break;

		tuplesort_end(runs->runs[i]);
	}
}

/*
 * gjoin_clauses_init
 *		initialize the equality / sort information
 *
 * This only allocates the space, does not set any of the values.
 */
static void
gjoin_clauses_init(GJoinClauseInfo *sort, int numcols)
{
	sort->nattnums = numcols;
	sort->attnums_inner = palloc_array(AttrNumber, numcols);
	sort->attnums_outer = palloc_array(AttrNumber, numcols);
	sort->equality = palloc_array(Oid, numcols);
	sort->inequality = palloc_array(Oid, numcols);
	sort->collations = palloc_array(Oid, numcols);
	sort->nulls_first = palloc_array(bool, numcols);
}

/*
 * gjoin_pos_reset
 *		reset gjoin position (as if before starting to process runs)
 */
static void
gjoin_ResetPosition(GJoinPosition *pos)
{
	pos->run = -1;
	pos->slot = -1;
	pos->buffer = NULL;
}

/*
 * gjoin_pos_is_reset
 *		returns true if the position is unset
 */
static bool
gjoin_PositionIsInvalid(GJoinPosition *pos)
{
	return (pos->run == -1) &&
		   (pos->slot == -1) &&
		   (pos->buffer == NULL);
}

/*
 * gjoin_create_plan_state
 *		Initialize state for executor of the smoothscan CustomScan.
 *
 * This does not need to be copied by the executor, so we don't need to
 * stash fields into lists etc.
 *
 * FIXME Most of the information is hard-coded and fake, working for a
 * singe fixed example. Needs to be derived from the actual join info.
 */
static Node *
gjoin_create_plan_state(CustomJoin *cjoin)
{
	GJoinJoinState *state;
	List		   *join_clauses = NIL;
	ListCell	   *lc;
	int				idx;

	/* makeNode to set tag etc, repalloc to get the right size */
	state = (GJoinJoinState *) makeNode(CustomJoinState);
	state = repalloc(state, sizeof(GJoinJoinState));

	state->cstate.methods = &gjoin_exec_methods;

	/* extract fields from the custom_private list */
	// join_clauses = list_nth(cjoin->custom_private, 0);
	join_clauses = cjoin->custom_exprs;

	/* must have valid lists */
	Assert(join_clauses != NIL);

	/* initialize the tuple buffers */
	gjoin_buffer_init(&state->buffer.inner);
	gjoin_buffer_init(&state->buffer.outer);

	/* initialize the runs */
	gjoin_runs_init(&state->runs.inner);
	gjoin_runs_init(&state->runs.outer);

	/* start by initializing the runs etc. */
	state->phase = GJOIN_INIT;

	/* one sort / equality key per join clause */
	gjoin_clauses_init(&state->clauses, list_length(join_clauses));

	/*
	 * Transform the join clause(s) into info we need for sorting and
	 * evaluating the join clauses later.
	 *
	 * XXX For now we require (Var op Var) clauses with equality, to keep
	 * the code simple. We can allow more complex conditions (e.g. with
	 * expressions) later.
	 */
	idx = 0;
	foreach (lc, join_clauses)
	{
		OpExpr *opclause = (OpExpr *) lfirst(lc);
		Var	   *var1,
			   *var2;
		TypeCacheEntry *typentry;

		AttrNumber	attnum_inner = InvalidAttrNumber,
					attnum_outer = InvalidAttrNumber;

		/* only allowed OpExpr clauses with two Vars earlier */
		Assert(IsA(opclause, OpExpr));

		var1 = linitial(opclause->args);
		var2 = lsecond(opclause->args);

		/* safety checks (should have been enforced earlier) */
		Assert(IsA(var1, Var) && IsA(var2, Var));
		Assert(var1->varno == INNER_VAR || var1->varno == OUTER_VAR);
		Assert(var2->varno == INNER_VAR || var2->varno == OUTER_VAR);
		Assert(var1->vartype == var2->vartype);

		/*
		 * XXX If the vars happen to be inversed, maybe we should use the GT
		 * operator instead of LT? But with both vars having the same type
		 * it does not matter.
		 */

		if (var1->varno == INNER_VAR)
			attnum_inner = var1->varattno;
		else
			attnum_outer = var1->varattno;

		if (var2->varno == INNER_VAR)
			attnum_inner = var2->varattno;
		else
			attnum_outer = var2->varattno;

		/* determine the operators */
		typentry = lookup_type_cache(var1->vartype,
									 TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR);

		Assert(opclause->opno == typentry->eq_opr);
		Assert(idx < state->clauses.nattnums);

		/* sort/equality info */
		state->clauses.attnums_inner[idx] = attnum_inner;
		state->clauses.attnums_outer[idx] = attnum_outer;
		state->clauses.equality[idx] = typentry->eq_opr;
		state->clauses.inequality[idx] = typentry->lt_opr;
		state->clauses.collations[idx] = opclause->opcollid;
		state->clauses.nulls_first[idx] = true;

		idx++;
	}

	/* did we get the expected number of elements in the two arrays? */
	Assert(idx == state->clauses.nattnums);

	state->buffers_inner = NULL;
	state->buffers_outer = NULL;

	return (Node *) state;
}

/* customjoin callbacks */
static void
gjoin_BeginCustomJoin(CustomJoinState *node,
					  EState *estate,
					  int eflags)
{
	GJoinJoinState *state = (GJoinJoinState *) node;
	CustomJoin *cjoin = (CustomJoin *) node->js.ps.plan;
	Plan *outerplan,
		 *innerplan;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &state->cstate.js.ps);

	/*
	 * Init the inner/outer subplan.
	 */
	outerplan = list_nth(cjoin->custom_plans, 0);
	innerplan = list_nth(cjoin->custom_plans, 1);

	state->outerstate = ExecInitNode(outerplan, estate, eflags);
	state->innerstate = ExecInitNode(innerplan, estate, eflags);

	state->cstate.custom_ps = lappend(state->cstate.custom_ps,
									  state->outerstate);
	state->cstate.custom_ps = lappend(state->cstate.custom_ps,
									  state->innerstate);

	/*
	 * Initialize the result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(&state->cstate.js.ps, &TTSOpsVirtual);
	ExecAssignProjectionInfo(&state->cstate.js.ps, NULL);

	ExecInitResultTypeTL(&state->cstate.js.ps);

	/*
	 * Initialize all child expressions - e.g. for join clauses and filter.
	 */
	state->cstate.js.ps.qual
		= ExecInitQual(cjoin->join.plan.qual, (PlanState *) state);

	/*
	 * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
	 * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
	 * references to nonexistent indexes.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * FIXME rest of init, if needed.
	 */
}

static void
gjoin_BuildRunsForRelation(GJoinJoinState *node, PlanState *state,
						   GJoinBuffer *buffer, GJoinRuns *runs,
						   GJoinClauseInfo *clauses, bool inner)
{
	TupleTableSlot *slot;
	bool		shouldFree;
	HeapTuple	tuple;
	TupleDesc	tdesc = ExecGetResultType(state);
	int			nextrun = 0;

	/* initialize the priority queues, described in the paper */

	/* queues for R */
	node->queues.inner_grow = pairingheap_allocate(priorityqueues_min_cmp, node);
	node->queues.inner_shrink = pairingheap_allocate(priorityqueues_min_cmp, node);

	/* queue for S (for the simplified variant with a single queue) */
	node->queues.outer = pairingheap_allocate(priorityqueues_min_cmp, node);

	/*
	 * Get all tuples from the node below the Hash node and insert into the
	 * hash table (or temp files).
	 */
	for (;;)
	{
		slot = ExecProcNode(state);
		if (TupIsNull(slot))
			break;

		/* XXX Do we need to materialize here? */
		tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);

		/*
		 * If we'd exceed the memory allowance, dump the current buffer into
		 * one of the tuplesorts, in a round-robin way.
		 *
		 * XXX We allow accumulating up to work_mem of tuples, because while
		 * building runs we only keep a single buffer in memory.
		 *
		 * XXX Why do we even need to process tuples in buffers while loading
		 * tuples into the tuplesorts? Well, we should probably load them
		 * into memory first, up to work_mem (or maybe work_mem/2), and only
		 * then start spilling to tuplesorts when it's clear it can't do the
		 * hashjoin-like execution. And we should try loading the other side
		 * too first, in case we can swap the sides.
		 */
		if (buffer->space + tuple->t_len > work_mem * 1024L)
		{
			Tuplesortstate *tuplesortstate;
			int				tuplesortopts = TUPLESORT_NONE;

			/* XXX we should keep the slot in the node state */
			TupleTableSlot *tmpslot;

			/* initialize the array of runs, if needed */
			if (runs->runs == NULL)
			{
				runs->maxruns = 32;	/* FIXME arbitrary number, needs to be set
										 * based on work_mem */
				runs->runs = palloc0_array(Tuplesortstate *, runs->maxruns);
				runs->ntuples = palloc0_array(int, runs->maxruns);
			}

			/* initialize the run, if needed */
			if ((tuplesortstate = runs->runs[nextrun]) == NULL)
			{
				Assert(runs->nruns == nextrun);

				tuplesortstate = tuplesort_begin_heap(tdesc,
													  clauses->nattnums,
													  (inner) ? clauses->attnums_inner : clauses->attnums_outer,
													  clauses->inequality,
													  clauses->collations,
													  clauses->nulls_first,
													  work_mem,
													  NULL,
													  tuplesortopts);
				runs->runs[nextrun] = tuplesortstate;
				runs->nruns++;
			}

			/* offload the tuples */
			tmpslot = MakeSingleTupleTableSlot(tdesc, &TTSOpsHeapTuple);
			for (int i = 0; i < buffer->ntuples; i++)
			{
				ExecStoreHeapTuple(buffer->tuples[i], tmpslot, true);
				tuplesort_puttupleslot(tuplesortstate, tmpslot);
			}
			ExecDropSingleTupleTableSlot(tmpslot);

			elog(DEBUG1, "gjoin_LoadInnerRelation %d space %lu",
				 buffer->ntuples, buffer->space);

			runs->ntuples[nextrun] += buffer->ntuples;

			buffer->space = 0;
			buffer->ntuples = 0;

			/* advance to the next run, round-robin way */
			nextrun = (nextrun + 1) % runs->maxruns;
		}

		/*
		 * Make sure there's space for adding a tuple to the buffer. Just double
		 * the array size if needed, as usual.
		 */
		if (buffer->ntuples == buffer->maxtuples)
		{
			if (buffer->ntuples == 0)
			{
				buffer->maxtuples = 64;
				buffer->tuples = palloc_array(HeapTuple, buffer->maxtuples);
			}
			else
			{
				buffer->maxtuples *= 2;
				buffer->tuples = repalloc_array(buffer->tuples, HeapTuple,
												buffer->maxtuples);
			}
		}

		/*
		 * ExecFetchSlotHeapTuple may return "physical tuple", in which case
		 * we need to copy it here, to prevent seeing garbage later
		 *
		 * FIXME why commented out?
		 */
		// if (!shouldFree)
			tuple = heap_copytuple(tuple);

		buffer->tuples[buffer->ntuples++] = tuple;
		buffer->space += tuple->t_len;
	}

	/*
	 * We're done with reading tuples from the table. If we had to dump any
	 * data into tuplesorts, dump the remaining tuples too.
	 *
	 * XXX Don't do this if we haven't spilled anything to disk, we should try
	 * doing an in-memory join (as if hashjoin) if possible.
	 */
	if (buffer->ntuples > 0)
	{
		Tuplesortstate *tuplesortstate;
		int				tuplesortopts = TUPLESORT_NONE;

		/* XXX we should keep the slot in the node state */
		TupleTableSlot *tmpslot;

		/* initialize the array of runs, if needed */
		if (runs->runs == NULL)
		{
			runs->maxruns = 32;	/* FIXME arbitrary number, needs to be set
									 * based on work_mem */
			runs->runs = palloc0_array(Tuplesortstate *, runs->maxruns);
			runs->ntuples = palloc0_array(int, runs->maxruns);
		}

		/* initialize the run, if needed */
		if ((tuplesortstate = runs->runs[nextrun]) == NULL)
		{
			Assert(nextrun == runs->nruns);

			tuplesortstate = tuplesort_begin_heap(tdesc,
												  clauses->nattnums,
												  (inner) ? clauses->attnums_inner : clauses->attnums_outer,
												  clauses->inequality,
												  clauses->collations,
												  clauses->nulls_first,
												  work_mem,
												  NULL,
												  tuplesortopts);
			runs->runs[nextrun] = tuplesortstate;
			runs->nruns++;
		}

		/* offload the tuples */
		tmpslot = MakeSingleTupleTableSlot(tdesc, &TTSOpsHeapTuple);
		for (int i = 0; i < buffer->ntuples; i++)
		{
			ExecStoreHeapTuple(buffer->tuples[i], tmpslot, true);
			tuplesort_puttupleslot(tuplesortstate, tmpslot);
		}
		ExecDropSingleTupleTableSlot(tmpslot);

		runs->ntuples[nextrun] += buffer->ntuples;

		elog(DEBUG1, "gjoin_LoadInnerRelation %d space %lu",
			 buffer->ntuples, buffer->space);

		buffer->space = 0;
		buffer->ntuples = 0;
	}

	elog(DEBUG1, "gjoin_BuildRunsForRelation %p SORT", state);

	/*
	 * Sort all the runs, one by one.
	 */
	for (int i = 0; i < runs->nruns; i++)
	{
		/* stop on the first non-initialized run */
		if (runs->runs[i] == NULL)
			break;

		elog(DEBUG1, "run %d tuples %d", i, runs->ntuples[i]);

		tuplesort_performsort(runs->runs[i]);
	}

	elog(DEBUG1, "gjoin_BuildRunsForRelation %p DONE", state);
}

/*
 * Initialize a batch of slots for tuples with the provided descriptor.
 *
 * XXX We use MinimalTuples, because that what tuplesort_gettupleslot uses
 */
static TupleBuffer *
tuple_buffer_init(TupleDesc tdesc, int nattnums)
{
	TupleBuffer *buffer;

	buffer = palloc0(sizeof(TupleBuffer));

	buffer->maxslots = MAX_SLOTS_PER_BUFFER;
	buffer->nslots = 0;
	buffer->slots = palloc_array(TupleTableSlot *, buffer->maxslots);

	for (int j = 0; j < buffer->maxslots; j++)
	{
		buffer->slots[j] = MakeSingleTupleTableSlot(tdesc, &TTSOpsMinimalTuple);
	}

	buffer->max_values = palloc_array(Datum, nattnums);
	buffer->min_values = palloc_array(Datum, nattnums);

	buffer->max_isnull = palloc_array(bool, nattnums);
	buffer->min_isnull = palloc_array(bool, nattnums);

	return buffer;
}

/*
 * Initialize runs for the inner relation (S).
 *
 * Loads one batch (~8KB) of tuples for each run generated for the relation.
 */
static dlist_head *
gjoin_InitRunsInner(GJoinJoinState *state, TupleDesc tdesc)
{
	GJoinRuns *runs = &state->runs.inner;

	/* allocate the array of buffer lists (list per run) */
	dlist_head *buffers = palloc_array(dlist_head, runs->nruns);

	/*
	 * Initialize batches of slots for all the runs, and load tuples from
	 * the tuplesorts into them.
	 */
	for (int i = 0; i < runs->nruns; i++)
	{
		TupleBuffer	   *buffer = tuple_buffer_init(tdesc, state->clauses.nattnums);

		while (tuplesort_gettupleslot(runs->runs[i], true, true,
									  buffer->slots[buffer->nslots],
									  NULL))
		{
			buffer->nslots++;

			/* stop after filling the last slot in the buffer */
			if (buffer->nslots == buffer->maxslots)
				break;
		}

		/* FIXME handle the case with nslots == 0, which can happen */
		Assert(buffer->nslots > 0);

		/* get the min/max values for the loaded chunks */
		if (buffer->nslots > 0)
		{
			/*
			 * initial buffers span from "negative infinity" (lowest value)
			 */
			buffer->min_empty = true;

			for (int j = 0; j < state->clauses.nattnums; j ++)
			{
				/*
				 * Use the value from the last tuple (because the data is sorted
				 * by this key).
				 */
				buffer->max_values[j]
					= slot_getattr(buffer->slots[buffer->nslots - 1],
								   state->clauses.attnums_inner[j],
								   &buffer->max_isnull[j]);

				// elog(WARNING, "buffer->max_values[%d] = %ld", j, buffer->max_values[j]);
			}

			/*
			 * Add the run to the grow/shrink priority queues (the paper calls
			 * those "A" and "B").
			 *
			 * We start with a single buffer per run, so it's both the oldest
			 * and newest loaded buffer. So add it to both queues with the same
			 * uppper limit.
			 */
			priorityqueues_push(state->queues.inner_grow, i, buffer->max_values);
			priorityqueues_push(state->queues.inner_shrink, i, buffer->max_values);
		}

		dlist_init(&buffers[i]);
		dlist_push_tail(&buffers[i], &buffer->node);
	}

	return buffers;
}

/*
 * Load one batch (~8KB) of tuples for each run of the outer relation (R).
 *
 * XXX We're not really checking the amount of memory, but the number of
 * slots in the batch.
 */
static dlist_head *
gjoin_InitRunsOuter(GJoinJoinState *state, TupleDesc tdesc)
{
	GJoinRuns *runs = &state->runs.outer;

	/* allocate the array of buffer lists (list per run) */
	dlist_head *buffers = palloc_array(dlist_head, runs->nruns);

	/*
	 * Initialize batches of slots for all the runs, and load tuples from
	 * the tuplesorts into them.
	 */
	for (int i = 0; i < runs->nruns; i++)
	{
		TupleBuffer	   *buffer = tuple_buffer_init(tdesc, state->clauses.nattnums);

		while (tuplesort_gettupleslot(runs->runs[i], true, true,
									  buffer->slots[buffer->nslots],
									  NULL))
		{
			buffer->nslots++;

			/* stop after filling the last slot */
			if (buffer->nslots == buffer->maxslots)
				break;
		}

		/* get the min/max for the loaded chunks */
		if (buffer->nslots > 0)
		{
			/*
			 * initial buffers span from "negative infinity" (lowest value)
			 */
			buffer->min_empty = true;

			for (int j = 0; j < state->clauses.nattnums; j ++)
			{
				/*
				 * Use the value from the last tuple (because the data is sorted
				 * by this key).
				 */
				buffer->max_values[j]
					= slot_getattr(buffer->slots[buffer->nslots - 1],
								   state->clauses.attnums_outer[j],
								   &buffer->max_isnull[j]);
			}

			/*
			 * Add the buffer to the priority queue "C", used to load new
			 * buffers for the outer relation.
			 *
			 * We use only a single priority queue to schedule both growth and
			 * eviction for S.
			 *
			 * XXX Maybe we should have both, to make it more memory-efficient?
			 * But for now simplicity matters more.
			 */
			priorityqueues_push(state->queues.outer, i, buffer->max_values);
		}

		dlist_init(&buffers[i]);
		dlist_push_tail(&buffers[i], &buffer->node);

		// elog(DEBUG1, "loaded inner buffer %p %d [%ld, %ld]",
		//	 buffer, i, buffer->min_value, buffer->max_value);
	}

	return buffers;
}

/* S */
static bool
gjoin_load_outer_buffer(GJoinJoinState *state, int run, TupleBuffer *buffer)
{
	/* reset, the buffer might be reused */
	buffer->nslots = 0;

	while (tuplesort_gettupleslot(state->runs.outer.runs[run], true, true,
								  buffer->slots[buffer->nslots],
								  NULL))
	{
		buffer->nslots++;

		/* stop after filling the last slot */
		if (buffer->nslots == buffer->maxslots)
			break;
	}

	/* no more tuples in this run */
	if (buffer->nslots == 0)
		return false;

	/* the minimum values are no longer empty */
	buffer->min_empty = false;

	/* calculate the buffer range (we know it's sorted) */
	for (int i = 0; i < state->clauses.nattnums; i++)
	{
		buffer->min_values[i] = slot_getattr(buffer->slots[0],
											 state->clauses.attnums_outer[i],
											 &buffer->max_isnull[i]);

		buffer->max_values[i] = slot_getattr(buffer->slots[buffer->nslots - 1],
											 state->clauses.attnums_outer[i],
											 &buffer->max_isnull[i]);
	}

	/* add the buffer to the priority queue for S */
	priorityqueues_push(state->queues.outer, run, buffer->max_values);

	/* also add the buffer to the run */
	dlist_push_tail(&state->buffers_outer[run],
					&buffer->node);

	return true;
}

/* R */
static bool
gjoin_load_inner_buffer(GJoinJoinState *state, int run, TupleBuffer *buffer)
{
	/* reset, the buffer might be reused */
	buffer->nslots = 0;

	elog(DEBUG1, "load buffer for run %d", run);

	while (tuplesort_gettupleslot(state->runs.inner.runs[run], true, true,
								  buffer->slots[buffer->nslots],
								  NULL))
	{
		buffer->nslots++;

		/* stop after filling the last slot */
		if (buffer->nslots == buffer->maxslots)
			break;
	}

	/* no more tuples in this run */
	if (buffer->nslots == 0)
		return false;

	/* the minimum values are no longer empty */
	buffer->min_empty = false;

	/* calculate the buffer range (we know it's sorted) */
	for (int i = 0; i < state->clauses.nattnums; i++)
	{
		buffer->min_values[i] = slot_getattr(buffer->slots[0],
											 state->clauses.attnums_inner[i],
											 &buffer->max_isnull[i]);

		buffer->max_values[i] = slot_getattr(buffer->slots[buffer->nslots - 1],
											 state->clauses.attnums_outer[i],
											 &buffer->max_isnull[i]);
	}

	/* add the buffer to the priority queue that manages growing */
	priorityqueues_push(state->queues.inner_grow, run, buffer->max_values);

	/* also add the buffer to the run */
	dlist_push_tail(&state->buffers_inner[run],
					&buffer->node);

	return true;
}

/*
 * Determine the join range covered by the given run (represented by
 * a list of tuple buffers).
 */
static void
gjoin_run_join_range(GJoinJoinState *state, dlist_head *run,
					 Datum **minvalues, Datum **maxvalues)
{
	TupleBuffer *buffer;
	dlist_iter	iter;

	dlist_foreach(iter, run)
	{
		buffer = dlist_container(TupleBuffer, node, iter.cur);
		// elog(DEBUG1, " > run %ld %ld", buffer->min_value, buffer->max_value);
	}

	buffer = dlist_head_element(TupleBuffer, node, run);
	*minvalues = buffer->min_values;

	elog(DEBUG1, "head %p", buffer);

	buffer = dlist_tail_element(TupleBuffer, node, run);
	*maxvalues = buffer->max_values;

	elog(DEBUG1, "tail %p", buffer);
}

/*
 * compare arrays of values
 *
 * FIXME use proper comparators for the given type, don't rely on
 * comparing the Datum values, it's bogus.
 */
static int
compare_values(GJoinJoinState *state, Datum *a, Datum *b)
{
	//elog(WARNING, "state->clauses.nattnums = %d", state->clauses.nattnums);
	for (int i = 0; i < state->clauses.nattnums; i++)
	{
		//elog(WARNING, "a[%d] = %ld", i, a[i]);
		//elog(WARNING, "b[%d] = %ld", i, b[i]);
		if (a[i] > b[i])
			return 1;
		else if (a[i] < b[i])
			return -1;
	}

	return 0;
}

/* Calculate the join range for all runs. */
static void
gjoin_CalculateJoinRange(GJoinJoinState *state)
{
	Datum  *min_values,
		   *max_values,
		   *run_min_values,
		   *run_max_values;

	bool	set = false;

	/* allocate once */
	min_values = palloc_array(Datum, state->clauses.nattnums);
	max_values = palloc_array(Datum, state->clauses.nattnums);
	run_min_values = palloc_array(Datum, state->clauses.nattnums);
	run_max_values = palloc_array(Datum, state->clauses.nattnums);

	for (int i = 0; i < state->runs.inner.nruns; i++)
	{
		/* no buffers loaded for the run (processed) */
		if (dlist_is_empty(&state->buffers_inner[i]))
			continue;

		gjoin_run_join_range(state, &state->buffers_inner[i],
							 &run_min_values, &run_max_values);

		/*
		 * If this is the first run with data, use the min/max values,
		 * otherwise do the actual comparison, and pick the smaller/larger
		 * one (lexicographically).
		 */
		if (!set)
		{
			memcpy(min_values, run_min_values,
				   sizeof(Datum) * state->clauses.nattnums);
			memcpy(max_values, run_max_values,
				   sizeof(Datum) * state->clauses.nattnums);
			continue;
		}

		/*
		 * Pick the larger (for minvalues) and smaller (for maxvalues).
		 */
		if (compare_values(state, min_values, run_min_values) < 0)
			memcpy(min_values, run_min_values,
				   sizeof(Datum) * state->clauses.nattnums);

		if (compare_values(state, min_values, run_min_values) > 0)
			memcpy(max_values, run_max_values,
				   sizeof(Datum) * state->clauses.nattnums);
	}

	/* FIXME copy the arrays, free the new allocated ones */

	state->join_range.min_values = min_values;
	state->join_range.max_values = max_values;

//	elog(DEBUG1, "join range = [%ld, %ld]",
//		 state->join_range.min_value,
//		 state->join_range.max_value);
}

static bool
check_join_clause(GJoinJoinState *state,
				 TupleTableSlot *outer, TupleTableSlot *inner)
{
	for (int i = 0; i < state->clauses.nattnums; i++)
	{
		Datum	a,
				b;
		bool	isnull;

		a = slot_getattr(outer, state->clauses.attnums_inner[i], &isnull);
		b = slot_getattr(inner, state->clauses.attnums_outer[i], &isnull);

		if (a != b)
			return false;
	}

	return true;
}

static TupleTableSlot *
gjoin_ExecCustomJoin(CustomJoinState *node)
{
	GJoinJoinState *state = (GJoinJoinState *) node;
	ExprContext *econtext;
	TupleTableSlot *slot;

	slot = state->cstate.js.ps.ps_ResultTupleSlot;

	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Perform the join - step through the state machine, etc.
	 */
	for (;;)
	{
		switch (state->phase)
		{
			case GJOIN_INIT:

				elog(DEBUG1, "GJOIN_INIT");

				/* First time through. Start by building runs for inner side. */
				state->phase = GJOIN_BUILD_INNER;
				break;

			case GJOIN_BUILD_INNER:

				elog(DEBUG1, "GJOIN_BUILD_INNER");

				/*
				 * Build runs for the inner relation. We assume the inner relation
				 * is smaller (it's what the paper calls R), so we start with it.
				 *
				 * XXX We should stop building the runs once it hits work_mem, try
				 * building runs on the outer relation, and then reconsider. Maybe
				 * the estimates were off and the outer relation is smaller, in
				 * which case it'd be better to flip the inner/outer relations for
				 * the sake of the algorithm. But we keep it simple for now.
				 */
				gjoin_BuildRunsForRelation(state,
										   state->innerstate,
										   &state->buffer.inner,
										   &state->runs.inner,
										   &state->clauses, true);

				/* build runs for the outer relation next */
				state->phase = GJOIN_BUILD_OUTER;
				break;

			case GJOIN_BUILD_OUTER:

				elog(DEBUG1, "GJOIN_BUILD_OUTER");

				/* Now build runs for the outer relation. */
				gjoin_BuildRunsForRelation(state,
										   state->outerstate,
										   &state->buffer.outer,
										   &state->runs.outer,
										   &state->clauses, false);

				/* prepare for reading tuples from the inner runs */
				state->phase = GJOIN_INIT_INNER;
				break;

			case GJOIN_INIT_INNER:

				elog(DEBUG1, "GJOIN_INIT_INNER");

				/* load a bufffer of tuples for each run of the inner relation */
				state->buffers_inner
					= gjoin_InitRunsInner(state,
										  ExecGetResultType(state->innerstate));

				gjoin_ResetPosition(&state->pos_inner);

				/*
				 * Calculate the current join range, defined as
				 *
				 * [Min(minvalue), Min(maxvalue)]
				 *
				 * over all buffers loaded from the runs of the inner relation.
				 * No buffers can be skipped when calculating this range.
				 */
				gjoin_CalculateJoinRange(state);

				/* next load buffers for the outer relation */
				state->phase = GJOIN_INIT_OUTER;
				break;

			case GJOIN_INIT_OUTER:

				elog(DEBUG1, "GJOIN_INIT_OUTER");

				/* load a bufffer of tuples for each run of the outer relation */
				state->buffers_outer
					= gjoin_InitRunsOuter(state,
										  ExecGetResultType(state->outerstate));

				gjoin_ResetPosition(&state->pos_outer);

				/* start by reading a tuple from the outer relation */
				state->phase = GJOIN_NEXT_OUTER;
				break;

			case GJOIN_NEXT_OUTER:

				elog(DEBUG1, "GJOIN_NEXT_OUTER");

				/*
				 * Advances to the next tuple in the outer relation (and possibly
				 * also loads the next buffer).
				 *
				 * FIXME should split the two things - one state for loading the
				 * next buffer, another for advancing to the next tuple slot
				 */

				{
					/*
					 * Are there are any outer buffers (S) that could be joined with
					 * the already loaded inner buffers? The whole outer buffer needs
					 * to be completely covered by "immediate join range" of R, which
					 * is the intersection of ranges for all runs.
					 *
					 * We only look at the first buffer in the run identified by the
					 * priority queue "C".
					 */
					TupleBuffer *buffer;

					/*
					 * If we don't have a buffer from S, get the next one from the
					 * queue "C" (determines in what order to load buffers for runs
					 * of the outer relation).
					 */
					if (gjoin_PositionIsInvalid(&state->pos_outer))
					{
						QueueEntry *entry;

						/* empty queue C means no more buffers in S, so terminate */
						if (pairingheap_is_empty(state->queues.outer))
							return NULL;

						/* don't remove the entry yet, GJOIN_LOAD_OUTER does that */
						entry = priorityqueues_peek(state->queues.outer);

						/* if we got an entry from queue, there must be a list */
						Assert(!dlist_is_empty(&state->buffers_outer[entry->run]));
						Assert(entry->run < state->runs.outer.nruns);

						/* init the outer position */
						state->pos_outer.run = entry->run;
						state->pos_outer.slot = -1;
					}

					/*
					 * XXX Maybe we could stash the buffer somewhere, so that
					 * we don't need to call dlist_head_element over and over
					 * (although it's likely cheap)? Can wait.
					 */
					buffer = dlist_head_element(TupleBuffer, node,
												&state->buffers_outer[state->pos_outer.run]);

					/*
					 * Is the whole buffer within the immediate join range?
					 * If not, we need to load some more pages for R (inner).
					 *
					 * FIXME Needs to use proper type comparators. Might be quite
					 * expensive, so we don't want to do that for every outer slot
					 * again, just once per buffer (and not for every tuple like
					 * happens now).
					 */
/*
 XXX We don't need to check the lower boundary, because if we get to
 load more buffers for inner relation, that only ever moves the upper
 boundary.
					if (!buffer->min_empty &&
						compare_values(state, state->join_range.min_values, buffer->min_values) > 0)
					{
						state->phase = GJOIN_LOAD_INNER;
						continue;
					}
*/

					/*
					 * FIXME this is not sufficient, because the join_range may be
					 * "incomplete", i.e. there may be more tuples with the same
					 * join key in the next buffer. So we need to either allow
					 * processing the outer buffer repeatedly, or make sure all the
					 * inner buffers are loaded at once (but then that needs more
					 * memory, and we may exceed work_mem). Or maybe we could peek
					 * at the next buffer in each run, and consider that when
					 * calculating the join range?
					 *
					 * In fact, we could calculate the join range knowing whether
					 * the upper boundary is inclusive or exclusive, and then we
					 * could process just the outer tuples that fall into that
					 * join range. But we still can't release the inner buffers,
					 * because the next outer buffer could need those.
					 */
					if (compare_values(state, state->join_range.max_values, buffer->max_values) < 0)
					{
						state->phase = GJOIN_LOAD_INNER;
						continue;
					}

					/* We can join this buffer, so advance to the next slot. */
					state->pos_outer.slot++;

					/*
					 * If we ran out of slots in this buffer, reset the position
					 * and request next buffer from the outer relation.
					 */
					if (state->pos_outer.slot >= buffer->nslots)
					{
						gjoin_ResetPosition(&state->pos_outer);
						state->phase = GJOIN_LOAD_OUTER;
						continue;
					}

					/*
					 * Got a valid outer tuple to join, so find all tuples on the
					 * inner side.
					 */
					gjoin_ResetPosition(&state->pos_inner);
					state->phase = GJOIN_NEXT_INNER;

					continue;
				}

			case GJOIN_NEXT_INNER:

				elog(DEBUG1, "GJOIN_NEXT_INNER");

				/*
				 * Advances to the next tuple in the inner relation (and possibly
				 * also loads the next buffer).
				 *
				 * FIXME should split the two things - one state for loading the
				 * next buffer, another for advancing to the next tuple slot
				 */

				{

					/*
					 * We have a slot from S (outer relation) to join, so walk loaded
					 * buffers from R (inner) and join them to the S tuple.
					 *
					 * XXX It should be possible to optimize this by first comparing
					 * the buffer range to the S range, and eliminate many of the
					 * buffers based on that.
					 */
					for (;;)
					{
						TupleBuffer	   *buffer_inner;
						TupleBuffer	   *buffer_outer;

						/* Have we ran out of runs? We're done. */
						if (state->pos_inner.run >= state->runs.inner.nruns)
							break;

						/* we've just start, so advance to first run */
						if (state->pos_inner.run == -1)
							state->pos_inner.run = 0;

						/*
						 * If there's no buffer yet, try to get the first buffer 
						 * from the selected run.
						 */
						if (state->pos_inner.buffer == NULL)
						{
							/* skip runs with no buffers (must have been finished) */
							if (dlist_is_empty(&state->buffers_inner[state->pos_inner.run]))
							{
								state->pos_inner.run++;
								continue;
							}

							/* FIXME don't look at the head only, need to walk all the buffers */
							buffer_inner = dlist_head_element(TupleBuffer, node,
															  &state->buffers_inner[state->pos_inner.run]);

							state->pos_inner.buffer = buffer_inner;
						}

						/* get the current inner buffer */
						buffer_inner = state->pos_inner.buffer;

						/* OK, time to join this R buffer. Advance to the next slot. */
						state->pos_inner.slot++;

						/* Have we ran out of slots? Move to the next buffer (or run). */
						if (state->pos_inner.slot >= buffer_inner->nslots)
						{
							/* if there's another buffer in this run, advance to it */
							if (dlist_has_next(&state->buffers_inner[state->pos_inner.run],
											   &buffer_inner->node))
							{
								dlist_node *next_node;
								next_node = dlist_next_node(&state->buffers_inner[state->pos_inner.run],
															&buffer_inner->node);
								buffer_inner = dlist_container(TupleBuffer, node, next_node);
								state->pos_inner.buffer = buffer_inner;
							}
							else
							{
								/* no buffer in this run, advance to the next run */
								state->pos_inner.buffer = NULL;
								state->pos_inner.run++;
							}
							state->pos_inner.slot = -1;
							continue;
						}

						buffer_outer = dlist_head_element(TupleBuffer, node,
														  &state->buffers_outer[state->pos_outer.run]);

						/*
						 * FIXME check that the two buffers overlap (not just the
						 * immediate join range, but the two smaller ranges)
						 */

						/* time to actually compare the two inner/outer tuples */
						{
							TupleTableSlot *outer = buffer_outer->slots[state->pos_outer.slot];
							TupleTableSlot *inner = buffer_inner->slots[state->pos_inner.slot];

							/* if the two tuples do not match, continue */
							if (!check_join_clause(state, outer, inner))
								continue;

							econtext->ecxt_innertuple = inner;
							econtext->ecxt_outertuple = outer;

							/*
							 * The rows seem to match the equality join clause (per
							 * the gjoin algoirthm itself), so check the additional
							 * join filters, if any.
							 */
							if (!ExecQual(state->cstate.js.ps.qual, econtext))
								continue;

							return ExecProject(node->js.ps.ps_ProjInfo);
						}
					}

					/* try to advance to the next outer tuple */
					state->phase = GJOIN_NEXT_OUTER;

					break;
				}

			case GJOIN_LOAD_INNER:

				elog(DEBUG1, "GJOIN_LOAD_INNER");

				{
					QueueEntry	   *entry;
					TupleBuffer	   *buffer;
					bool			loaded;

					/* FIXME we shouldn't bail out right away, there still
					 * may be some data to join (similarly to the beginning,
					 * we should use infinity for the highkey) */
					if (pairingheap_is_empty(state->queues.inner_grow))
						return NULL;

					/* get the next entry, remove it */
					entry = priorityqueues_pop(state->queues.inner_grow);

					buffer = tuple_buffer_init(ExecGetResultType(state->innerstate),
											   state->clauses.nattnums);

					/* load the next buffer from the run */
					loaded = gjoin_load_inner_buffer(state, entry->run, buffer);

//					if (loaded)
//						elog(DEBUG1, "loaded inner buffer %p %d [%ld, %ld]",
//							 buffer, entry->run, buffer->min_value, buffer->max_value);

					/* FIXME handle loaded=false */

					gjoin_ResetPosition(&state->pos_inner);
					gjoin_CalculateJoinRange(state);

					/* retry the join */
					state->phase =  GJOIN_NEXT_OUTER;
					break;
				}

			case GJOIN_LOAD_OUTER:

				elog(DEBUG1, "GJOIN_LOAD_OUTER");

				{
					/*
					 * FIXME This may free the buffer prematurely. We should
					 * only free it if it just got joined. And only then we
					 * should load the next one.
					 */

					QueueEntry	   *entry;
					TupleBuffer	   *buffer;
					bool			loaded;

					Assert(!pairingheap_is_empty(state->queues.outer));

					/* still don't remove the entry, we'll need it t */
					entry = priorityqueues_peek(state->queues.outer);

					buffer = dlist_head_element(TupleBuffer, node,
												&state->buffers_outer[entry->run]);

					/* unlink the buffer from the list */
					// dlist_delete(&(buffer->node));

					buffer = tuple_buffer_init(ExecGetResultType(state->outerstate),
											   state->clauses.nattnums);

					loaded = gjoin_load_outer_buffer(state, entry->run, buffer);

//					if (loaded)
//						elog(DEBUG1, "loaded outer buffer %p %d [%ld, %ld]",
//							 buffer, entry->run, buffer->min_value, buffer->max_value);

					/* FIXME handle loaded=false */

					/*
					 * FIXME we shouldn't be resetting the position all the
					 * way back, we know that the next key (at least in the
					 * same run in S) will be higher, so it can't start before
					 * the start of the current key. But it's more complex
					 * due to having multiple runs, so we'll need to remember
					 * the position per run.
					 */
					gjoin_ResetPosition(&state->pos_outer);

					/* retry the join */
					state->phase =  GJOIN_EVICT_INNER;
					break;
				}

			case GJOIN_EVICT_INNER:

				elog(DEBUG1, "GJOIN_EVICT_INNER");

				{
					/*
					 * FIXME This may free the outer buffer prematurely. We
					 * should only free it if it just got joined. And only
					 * then we should load the next one.
					 *
					 * But then we shouldn't get here at all, I guess. We
					 * should loading inner, and retry the buffer join. But
					 * now we always do load_inner -> load_outer.
					 */
					QueueEntry	   *entry;
					TupleBuffer	   *buffer;

					Assert(!pairingheap_is_empty(state->queues.outer));

					/* time to finally remove the entry, won't need it */
					entry = priorityqueues_pop(state->queues.outer);

					buffer = dlist_head_element(TupleBuffer, node,
												&state->buffers_outer[entry->run]);

					Assert(compare_values(state, entry->values, buffer->max_values) == 0);

					/* unlink the buffer from the list */
					dlist_delete(&(buffer->node));

					/*
					 * XXX no need to do anything about the outer queue,
					 * it's fed by loading new pages from S.
					 */

//					elog(DEBUG1, "evicting outer buffer %p %d [%ld, %ld] %ld",
//						 buffer, entry->run, buffer->min_value, buffer->max_value, entry->value);

					/*
					 * Try to evict buffers for inner relation, using the
					 * 'shrink' queue. The buffer has to be the head for
					 * each run (the queue only has run index), and we can
					 * evict it if the max_value is before the outer buffer.
					 */
					for (;;)
					{
						TupleBuffer *buffer_inner, *tail;
						QueueEntry *entry_inner;

						entry_inner = priorityqueues_peek(state->queues.inner_shrink);

						buffer_inner = dlist_head_element(TupleBuffer, node,
														  &state->buffers_inner[entry->run]);
						tail = dlist_tail_element(TupleBuffer, node,
												  &state->buffers_inner[entry->run]);

						/* can't evict, still may be needed to join */
						if (compare_values(state, entry_inner->values, entry->values) >= 0)
							break;

						/* also can't evict if it's the only buffer for 
						 * the run */
						if (buffer_inner == tail)
							break;

						/* ok, remove */
						priorityqueues_pop(state->queues.inner_shrink);

						/* FIXME if there are more buffers, for this run, have to
						 * add the next one (the oldest page remaining) to the
						 * shrink queue */

						if (dlist_has_next(&state->buffers_inner[entry_inner->run],
										   &buffer_inner->node))
						{
							TupleBuffer *tmp;
							dlist_node *next_node;
							next_node = dlist_next_node(&state->buffers_inner[entry_inner->run],
														&buffer_inner->node);
							tmp = dlist_container(TupleBuffer, node, next_node);
							priorityqueues_push(state->queues.inner_shrink, entry_inner->run, tmp->max_values);
						}

						/* unlink the buffer from the list */
						dlist_delete(&(buffer_inner->node));

//						elog(DEBUG1, "evicting inner buffer %p %d [%ld, %ld]",
//							 buffer_inner, entry_inner->run,
//							 buffer_inner->min_values, buffer_inner->max_values);
					}

					/* FIXME free the buffer tuples / memory */

					state->phase =  GJOIN_NEXT_OUTER;
					break;
				}

			default:
				elog(ERROR, "unrecognized gjoin state: %d",
					 (int) state->phase);
		}
	}

	return NULL;
}

static void
gjoin_EndCustomJoin(CustomJoinState *node)
{
	GJoinJoinState *state = (GJoinJoinState *) node;

	/* FIXME cleanup */
	gjoin_runs_close(&state->runs.inner);
	gjoin_runs_close(&state->runs.outer);

	/*
	 * clean up subtrees
	 */
	ExecEndNode(state->outerstate);
	ExecEndNode(state->innerstate);
}

static void
gjoin_ReScanCustomJoin(CustomJoinState *node)
{
	// GJoinJoinState *state = (GJoinJoinState *) node;

	/* FIXME rescan */
	elog(ERROR, "gjoin_ReScanCustomScan not implemented");
}

/*
 * Show a generic expression
 */
static void
show_expression(Node *node, const char *qlabel,
				PlanState *planstate, List *ancestors,
				ExplainState *es)
{
	List	   *context;
	char	   *exprstr;
	bool		useprefix = (es->rtable_size > 1 || es->verbose);

	/* Set up deparsing context */
	context = set_deparse_context_plan(es->deparse_cxt,
									   planstate->plan,
									   ancestors);

	/* Deparse the expression */
	exprstr = deparse_expression(node, context, useprefix, false);

	/* And add to es->str */
	ExplainPropertyText(qlabel, exprstr, es);
}

/*
 * XXX It's a bit strange this works, because join_clauses are not processed
 * by setrefs (and AFAIK the deparsing expects that?).
 */
static void
gjoin_ExplainCustomJoin(CustomJoinState *node,
						List *ancestors,
						ExplainState *es)
{
	// List	   *context;
	// char	   *exprstr;
	GJoinJoinState *state = (GJoinJoinState *) node;
	CustomJoin *cjoin = (CustomJoin *) node->js.ps.plan;
	List *join_clauses = list_nth(cjoin->custom_exprs, 0);

	/* FIXME show additional run-time information about the plan */
	show_expression((Node *) join_clauses, "Join Cond",
					(PlanState *) state, ancestors, es);
}

/*
 * Pairing heap provides getting topmost (greatest) element while we want to
 * calculate the minimum. That's why we invert the sort order.
 *
 * FIXME use proper type-specific comparator, this assumes integers
 */
static int
priorityqueues_min_cmp(const pairingheap_node *a, const pairingheap_node *b,
					   void *arg)
{
	QueueEntry *qea = (QueueEntry *) a;
	QueueEntry *qeb = (QueueEntry *) b;
	GJoinJoinState *state = (GJoinJoinState *) arg;

	int r = compare_values(state, qea->values, qeb->values);

	/* exchange argument order to invert the sort order */
	/* XXX ... or we could simply return (-r) */
	if (r < 0)
		return 1;
	else if (r > 0)
		return -1;
	else
		return 0;
}

/*
 * Helper function to push a tuple to the reorder queue.
 */
static void
priorityqueues_push(pairingheap *heap, int run, Datum *values)
{
	QueueEntry	*qe;

	/* FIXME don't use TopMemoryContext (see reorderqueues_push)  */
	MemoryContext oldContext = MemoryContextSwitchTo(TopMemoryContext);

	qe = (QueueEntry *) palloc(sizeof(QueueEntry));
	qe->run = run;
	qe->values = values;

	pairingheap_add(heap, &qe->ph_node);

	MemoryContextSwitchTo(oldContext);
}

/*
 * Helper function to pop the next tuple from the reorder queue.
 */
static QueueEntry *
priorityqueues_pop(pairingheap *heap)
{
	return (QueueEntry *) pairingheap_remove_first(heap);
}

/*
 * Helper function to peek at the next tuple in the reorder queue (without
 * removing it).
 */
static QueueEntry *
priorityqueues_peek(pairingheap *heap)
{
	return (QueueEntry *) pairingheap_first(heap);
}
