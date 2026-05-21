/*
 * gjoin.c - implementation of custom join access path
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
 * - create_gjoin_plan - crates CustomJoin plan, mimicking NestLoop
 *
 * - gjoin_CreatePlanState - initializes runtime state of the plan
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
 * does exactly. Instead it relies on tuplesort (which does 8K pages
 * internally, but the API exposes tuples). The code groups stuff into
 * batches, instead of pages. Other than that, the logic is similar to
 * what the paper describes.
 *
 * XXX I'm not sure if working with tuples (and not pages) changes the
 * algorithm in a fundamental way. I don't think it does, but working
 * with pages may be more efficient e.g. thanks to fewer comparisons.
 * OTOH it might allow other optimizations (e.g. the join ranges could be
 * more focused, split pages we're joining instead of requiring the
 * outer buffer to be fully covered, etc.).
 *
 * XXX Parallel variant - seems complex, but I think it would be possible
 * to coordinate the workers similarly to parallel hash join, i.e. workers
 * would build the inner runs, then would advance to the outer runs, and
 * then do the actual join - e.g. by each processing a different outer
 * batch. The runs would probably need to be loaded into shared memory,
 * though.
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
static Plan *create_gjoin_plan(PlannerInfo *root,
							   RelOptInfo *rel,
							   CustomPath *best_path,
							   List *tlist,
							   List *clauses,
							   List *custom_plans);
static Node *gjoin_CreatePlanState(CustomJoin *cjoin);

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

static CustomPathMethods gjoin_path_methods;
static CustomJoinMethods gjoin_plan_methods;
static CustomJoinExecMethods gjoin_exec_methods;


/*
 * An input buffer of tuples, loaded from a relation.
 *
 * We accumulate up to work_mem/2 worth of tuples in memory, hoping to
 * do a hashjoin-like in-memory join. When it becomes obvious we can't
 * fit either side into a hash table, we switch to mergejoin-like join
 * with sorted data.
 *
 * After we switch to the sorted variant, we stop using the buffers and
 * start feeding tuples directly into the tuplesorts.
 *
 * XXX We may need to keep both relations in memory at once, when trying
 * to figure out if we can build a hash table on either side. So we allow
 * each side to be work_mem/2, to not exceed work_mem in total. But maybe
 * we could allow up to work_mem for each side, or rather consider the
 * hash_multiplier (which is 2.0 by default).
 *
 * XXX Maybe we should keep an array of slots, to save on the tuple/slot
 * conversions when not needed.
 *
 * XXX Maybe use MinimalTuple instead of HeapTuple?
 */
typedef struct TupleBuffer
{
	MemoryContext cxt;			/* context batching this buffer */
	size_t		space;			/* space used by tuples */
	int			ntuples;		/* number of tuples */
	int			maxtuples;		/* maximum number of tuples */
	HeapTuple  *tuples;			/* tuple array (capacity maxtuples) */
}			TupleBuffer;

/*
 * An array of "runs" of batches in the g-join algorithm (paper uses "pages",
 * this code uses "batches"). But it's the same concept, mostly.
 *
 * Each run is sorted, so we represent it as a tuplesort.
 *
 * XXX Maybe tuplesort is not the right abstraction, and writing into a
 * tuplesort from the beginning may be premature. The algorithm does allow
 * performing a hash join in some cases, in which case the sort is not
 * necessary. So maybe write to tuplestore first, and only flip into sorted
 * mode later? But we don't want to write everything twice (into tuplestore
 * and then shuffle everything into tuplesort), so maybe do that half-way
 * through?
 */
typedef struct BatchRun
{
	int			ntuples;		/* number of tuples in this run */
	Tuplesortstate *tuplesort;	/* sorted tuples */
	dlist_head	batches;		/* batches loaded from the tuplesort */
}			BatchRun;

typedef struct BatchRuns
{
	int			nruns;			/* number of runs used */
	int			maxruns;		/* maximum number of runs */
	BatchRun   *runs;			/* array of runs */
}			BatchRuns;

/*
 * Maximum number of tuples/slots per batch during the join.
 *
 * XXX Batches should be sized based on memory usage (as in the paper), not
 * on the number of tuples.
 *
 * XXX Not sure what's the optimal batch size. Smaller batches allow better
 * control over memory usage. Larger batches allow efficient elimination
 * of not-matching batches, but then if batches match we have to check all
 * possible tuple combinations, which increases the cost. Surely there is
 * an analytic solution, but experiments would likely give an answer too.
 *
 * XXX Smaller batches also make cache_pos less efficient (or at least I
 * suspect so).
 *
 * XXX Would it make sense to have larger batches, and then build small
 * hash tables on them, to make finding matches more efficient?
 */
#define	MAX_BATCH_SIZE 128

/* XXX probably should use simplehash instead */
typedef struct HashEntry
{
	uint32		hashvalue;
	int32		slot;
} HashEntry;

/*
 * A small "batch" of tuples loaded from one sorted run. The batches form
 * a linked list, so that a run can have multiple buffers loaded at a time.
 * The batches loaded earlier are at the beginning of the list, so that
 * the oldest batch is at the head (and we can easily evict it).
 *
 * XXX Smaller batches allow more granular control of memory.
 *
 * XXX We should probably have a small "dense" memory context for each
 * batch. Or maybe we should use GenerationContext for all batches
 * combined? We can't be too strict about memory allocation, because
 * then what about large tuples (we could handle those as special case,
 * with one batch per large tuple).
 *
 * XXX We probably don't need the isnull arrays, because with inner joins
 * those tuples won't match anything. So we can probably just discard
 * those and not add them to the buffer/tuplesort at all?
 */
typedef struct Batch
{
	/* doubly-linked list of batches in each run */
	dlist_node	node;

	size_t		space;			/* space used by this batch */

	/* minimum values */
	bool		min_unbounded;	/* no lower bound */
	Datum	   *min_values;
	bool	   *min_isnull;

	/* maximum values */
	bool		max_unbounded;	/* no upper bound */
	Datum	   *max_values;
	bool	   *max_isnull;

	/* first/last batch in the run */
	/* XXX not needed, duplicate with unbounded flags */
	bool		is_first;		/* min_unbounded=true */
	bool		is_last;		/* max_unbounded=true */

	/*
	 * Cached slot position, used as starting position after advancing to
	 * the next outer tuple (to skip inner tuples that can't match).
	 */
	int			cache_pos;

	/* tuple slots in this batch */
	int			maxslots;
	int			nslots;
	uint32	   *hashes;
	HashEntry  *hashtable;
	TupleTableSlot **slots;
}			Batch;

/*
 * Position in currently loaded runs. The indexes determine the run, and
 * the slot in the current buffer (in the doubly-linked list).
 */
typedef struct JoinPosition
{
	int			run;
	Batch	   *batch;
	int			slot;
}			JoinPosition;

/*
 * Information extracted from the join clauses used by the g-join.
 *
 * This is used to sort the inner/outer side, evaluate the join clauses
 * when matching the tuples, etc.
 *
 * XXX For now this handles just (Var op Var) clauses, so it's enough to
 * work with attnums. But maybe it's OK even with expressions, if the
 * expression gets computed in tlist of the input path? Not sure.
 *
 * XXX nulls_first seems unnecessary, as we're handling just inner
 * joins, and those can't match NULLs.
 */
typedef struct JoinClauses
{
	int			nattnums;		/* number of clauses/keys */
	AttrNumber *attnums_inner;	/* attnums on inner side (R) */
	AttrNumber *attnums_outer;	/* attnums on outer side (S) */
	Oid		   *equality;		/* = operators (OIDs) */
	Oid		   *inequality;		/* < operators (OIDs)  */
	Oid		   *collations;		/* OIDs of collations */
	Oid		   *outfuncs;		/* OIDs of output functions */
	FmgrInfo   *cmp_info;		/* comparators */
	FmgrInfo   *hash_info;		/* hash functions */
	bool	   *nulls_first;	/* XXX unnecessary? */
}			JoinClauses;

/*
 * Phases of the gjoin state machine.
 *
 * We generally follow the logic that (R < S), and R = inner, S = outer.
 */
typedef enum JoinPhase
{
	GJOIN_INIT,					/* initial state */
	GJOIN_BUILD_INNER,			/* build runs for inner */
	GJOIN_BUILD_OUTER,			/* build runs for outer */
	GJOIN_INIT_INNER,			/* prepare inner runs for matching */
	GJOIN_INIT_OUTER,			/* prepare outer runs for matching */
	GJOIN_LOAD_INNER,			/* load a buffer of tuples for R */
	GJOIN_LOAD_OUTER,			/* load a buffer of tuples for S */
	GJOIN_NEXT_OUTER,			/* advance to the next outer tuple */
	GJOIN_NEXT_INNER,			/* advance to the next inner tuple */
	GJOIN_EVICT_INNER			/* evict buffer from the inner side */
}			JoinPhase;

typedef struct JoinStats
{
	int	batches_inner;	/* batches loaded from inner */
	int	batches_outer;	/* batches loaded from outer */
	int	batches_cross;	/* inner/outer batch combinations */

	int tuples_inner;	/* tuples fetched from inner */
	int	tuples_outer;	/* tuples fetched from outer */
	int	tuples_cross;	/* inner/outer tuple combinations */
} JoinStats;


/* ----------------
 *	 GJoinState information
 *
 *
 * ----------------
 */
typedef struct GJoinState
{
	CustomJoinState cstate;

	/* states of the inner/outer subplan */
	PlanState  *outerstate;
	PlanState  *innerstate;

	/* current phase of the join */
	JoinPhase	phase;

	/*
	 * Buffers of tuples loaded from inner/outer side, for the in-memory
	 * hash-join phase. If that doesn't work, we switch to the sorted mode
	 * with mergejoin-like algorithm (in which case the tuples get written to
	 * the tuplesorts, and the buffers are freed).
	 */
	struct
	{
		TupleBuffer inner;
		TupleBuffer outer;
	}			buffer;

	/*
	 * Runs of sorted tuple batches in the mergejoin-like join mode. Each run
	 * is represended by a separate tuplesort, and a bit of metadata. The
	 * batches are formed when reading tuples from the tuplesort.
	 *
	 * XXX We could do a custom merge sort, but it does not seem worth it (and
	 * it's unlikely to beat the optimized tuplesort). It might give us better
	 * control over memory, perhaps?
	 *
	 * XXX We need to be careful about the number of files we're keeping open,
	 * to not end with the same memory "explosion" issue as hashjoin. Could be
	 * a problem with very many runs (but the algorithm keeps the number of
	 * runs under control, to ensure memory limit).
	 *
	 * XXX The annoying drawback is that this keeps work_mem for each sort,
	 * and that needs fixing. Actually, is that true? Maybe it's only for the
	 * sorting (but we do the one by one), not for reading the sorted data
	 * afterwards?
	 */
	struct
	{
		BatchRuns	inner;
		BatchRuns	outer;
	}			runs;

	/*
	 * join clause information
	 *
	 * Information extracted from join clauses, used for hashing/sorting, and
	 * evaluating clauses, etc.
	 */
	JoinClauses clauses;

	/*
	 * priority queues from the algorithm, described by the paper
	 *
	 * XXX For now we use the simplified variant described in the paper on
	 * page. 6 (272), using the newest buffer for queue "C" (and not using "D"
	 * at all). That way we don't need to look ahead at the next page.
	 * Although, it wouldn't be too hard with the tuplesort.
	 */
	struct
	{
		pairingheap *inner_grow;	/* R min(maxval) / newest buffer */
		pairingheap *inner_shrink;	/* R min(maxval) / oldest buffer */
		pairingheap *outer;		/* S min(maxval) / newest buffer */
	}			queues;

	/*
	 * join range (derived from all current runs)
	 *
	 * Range of values from outer relation (S) that can be joined with the
	 * inner runs (from R). That is, the currently loaded inner runs are
	 * guarangeed to contain all existing tuples from this range.
	 *
	 * The boundaries are always non-inclusive, i.e. we can't join with the
	 * min/max boundary values because baches (in the same run) may share the
	 * min/max values. And we might have already discarded the first batch, or
	 * not loaded the following batch yet.
	 *
	 * The first/last batch in each run will have one boundary unset. If a run
	 * has a single batch, it will have neither bound. A join range is min/max
	 * unbounded if all runs are min/max unbounded.
	 *
	 * XXX Does not handle NULL values (and does not need to).
	 */
	struct
	{
		/* min/max boundary */
		Datum	   *min_values;
		Datum	   *max_values;

		/* min/max boundary not set (so unbounded) */
		bool		min_unbounded;
		bool		max_unbounded;
	}			join_range;

	JoinPosition pos_inner;
	JoinPosition pos_outer;

	/* stats about the join */
	JoinStats		stats;

}			GJoinState;


/*
 * priority queue entries
 *
 * The g-join is driven by a number of priority queues, determining which
 * run should load/evict a batch next. We implement those as pairing heaps,
 * sorted by the values of join keys.
 */
typedef struct QueueEntry
{
	pairingheap_node ph_node;	/* owning pairing heap */
	int			run;			/* run for this entry */
	Datum	   *values;			/* values of join keys */
}			QueueEntry;


/* helpers */
static bool join_clause_is_compatible(Expr *clause);
static void join_clauses_init(JoinClauses * sort, int numcols);

static void tuple_buffer_init(TupleBuffer * buffer);

static Batch * batch_init(TupleDesc tdesc, int nattnums);
static void batch_free(Batch *batch);
static void batch_runs_init(BatchRuns * runs);
static void batch_runs_close(BatchRuns * runs);
static void batch_run_init(BatchRun * run, Tuplesortstate *sort);

/* comparator used by all the priority queues */
static int	priorityqueues_min_cmp(const pairingheap_node *a,
								   const pairingheap_node *b, void *arg);

/* add/remove/peek at the next queue entry */
static void priorityqueues_push(pairingheap *heap, int run, Datum *values);
static QueueEntry * priorityqueues_pop(pairingheap *heap);
static QueueEntry * priorityqueues_peek(pairingheap *heap);


static void position_reset(JoinPosition * pos);
static bool position_is_invalid(JoinPosition * pos);

static void build_inner_runs(GJoinState * node);
static void build_outer_runs(GJoinState * node);

static void init_inner_runs(GJoinState * state);
static void init_outer_runs(GJoinState * state);
static bool load_outer_batch(GJoinState * state, int run, Batch * batch);
static bool load_inner_batch(GJoinState * state, int run, Batch * batch);

static bool batch_in_join_range(GJoinState * state, Batch * batch);
static int check_join_clause(GJoinState * state,
							 TupleTableSlot *outer, TupleTableSlot *inner);
static void update_join_range(GJoinState * state);
static int	compare_values(GJoinState * state, Datum *a, Datum *b);
static void join_range_for_run(GJoinState * state, dlist_head *run,
							   Datum **min_values, Datum **max_values,
							   bool *min_unbounded, bool *max_unbounded);
static bool batches_may_overlap(GJoinState *state, Batch *a, Batch *b);
static bool batch_can_evict_inner(GJoinState *state, Batch *batch_inner);
static void reset_cache_pos(GJoinState *state);

// #define GJOIN_DEBUG

#ifdef GJOIN_DEBUG
static void debug_print_batch(GJoinState *state, char *msg, Batch *batch);
static void debug_print_runs(GJoinState *state);
static void debug_print_join_range(GJoinState *state);
static void debug_print_values(GJoinState *state, char *msg,
							   bool unbounded, Datum *values);
#define DEBUG_LOG(...)	elog(LOG, __VA_ARGS__)
#else
#define debug_print_batch(a, b, c)
#define debug_print_runs(a)
#define debug_print_join_range(a)
#define debug_print_values(a, b, c)
#define DEBUG_LOG(...)
#endif

//#define HASHTABLE_CAPACITY(batch)		((batch)->nslots * 4)
#define HASHTABLE_CAPACITY(batch)		(MAX_BATCH_SIZE * 4)
#define HASHTABLE_STEP					41
#define HASHTABLE_SLOT_INDEX(batch, hashvalue)	((hashvalue) % HASHTABLE_CAPACITY(batch))
#define HASHTABLE_SLOT_EMPTY(batch, idx)	((batch)->hashtable[(idx)].slot == -1)


void
_PG_init(void)
{
	DefineCustomBoolVariable(
							 "gjoin.enabled",
							 "whether to generate GJoin paths",
							 NULL, &gjoin_enabled, false, PGC_USERSET, 0, NULL, NULL, NULL);

	/* custom-scan node */
	memset(&gjoin_path_methods, 0, sizeof(CustomPathMethods));
	gjoin_path_methods.CustomName = "GJoin";
	gjoin_path_methods.PlanCustomPath = create_gjoin_plan;

	memset(&gjoin_plan_methods, 0, sizeof(CustomJoinMethods));
	gjoin_plan_methods.CustomName = "GJoin";
	gjoin_plan_methods.CreateCustomJoinState = gjoin_CreatePlanState;

	memset(&gjoin_exec_methods, 0, sizeof(CustomJoinExecMethods));
	gjoin_exec_methods.CustomName = "GJoin";
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
	Path	   *path;

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
	CustomPath *cpath;
	Relids		required_outer;
	ParamPathInfo *param_info;
	List	   *restrict_clauses = extra->restrictlist;
	bool		has_usable_join_clause = false;
	ListCell   *lc;

	/*
	 * pick inner/outer paths to join
	 *
	 * FIXME Any paths can be joined, but maybe the paths with cheapest
	 * startup are not the best ones. It might be better to get cheapest total
	 * paths and do sort. Or the paths may be sorted, and then we don't need
	 * to do additional sort.
	 *
	 * XXX We might also consider using already-sorted paths (with any
	 * ordering matching the join keys), but I'm not sure it makes sense.
	 * With presorted paths we always pay some extra cost, even if we end
	 * up doing the in-memory hashjoin. Maybe that's fine, or maybe the
	 * extra cost is one of the problems (e.g. when it's provided by an
	 * indexscan that fetches many more rows / be very expensive). Maybe
	 * the right solution is to use the cheapest total path, but leverage
	 * the ordering if the path happens to be sorted? Or should we be
	 * defensive and always assume we'll be sorting? Still, the defensive
	 * approach may be to do sorting on our own.
	 */
	Path	   *outer_path = outerrel->cheapest_startup_path;
	Path	   *inner_path = innerrel->cheapest_startup_path;

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
	 * Don't allow cartesian products (the following check would have the same
	 * effect, but it seems reasonable to do this cheap check now).
	 */
	if (restrict_clauses == NIL)
		return NULL;

	/*
	 * Make sure there's at least one join clause with (Var op Var), where the
	 * operator is equality. The remaining join clauses will be treated as
	 * join filters.
	 *
	 * XXX For now we support only joins with a single (var op var) clause.
	 *
	 * We should really support multiple join clauses, but we probably should
	 * stick to (var op var) for clauses, to keep things simple. Such clauses
	 * would be used by gjoin, and the rest would be turned into a filter.
	 */
	foreach(lc, restrict_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/* paranoia */
		Assert(IsA(rinfo, RestrictInfo));

		if (!join_clause_is_compatible(rinfo->clause))
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
	cpath->path.pathtype = T_CustomJoin;
	cpath->path.parent = joinrel;
	cpath->path.pathtarget = joinrel->reltarget;
	cpath->path.param_info = param_info;	/* FIXME */
	cpath->path.pathkeys = NIL; /* FIXME derive output pathkeys */

	/* XXX no parallelism */
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = false;
	cpath->path.parallel_workers = -1;

	/* XXX no support for CUSTOMPATH_SUPPORT_PROJECTION */
	cpath->flags = 0;
	cpath->custom_paths = NIL;
	cpath->methods = &gjoin_path_methods;

	/* no custom path private info for now */
	cpath->custom_private = NIL;

	/* add the outer/inner paths into custom_paths */
	cpath->custom_paths = lappend(cpath->custom_paths, outer_path);
	cpath->custom_paths = lappend(cpath->custom_paths, inner_path);

	/*
	 * Remember the custom restrictinfos (all of them, both the clauses that
	 * work for gjoin, and those that will be treated as filters).
	 */
	cpath->custom_restrictinfo = restrict_clauses;

	/*
	 * XXX fake costing, to make gjoin look like the best join path
	 *
	 * XXX Instead, the set_join_pathlist_hook hook should discard the join
	 * paths this gjoin replaces.
	 */
	cpath->path.rows = joinrel->rows;
	cpath->path.startup_cost = 0.0;
	cpath->path.total_cost = 1.0;

	return (Path *) cpath;
}

/*
 * Transform the CustomPath gjoin path into a CustomJoin plan.
 *
 * create_gjoin_plan
 *		Transform the CustomPath gjoin path into a CustomScan plan.
 *
 * Most of the steps are common in other create_plan methods. We also need
 * to do CustomJoin stuff, to make setrefs do the right thing.
 */
static Plan *
create_gjoin_plan(PlannerInfo *root,
				  RelOptInfo *rel,
				  CustomPath *best_path,
				  List *tlist,
				  List *clauses,
				  List *custom_plans)
{
	CustomJoin *cjoin;
	Plan	   *outerplan,
			   *innerplan;

	/* List	   *params = (List *) best_path->custom_private; */
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
	cjoin->custom_plans = NIL;	/* setrefs operates on lefttree/righttree */

	/*
	 * XXX For now we assume there are only two plans passed through the
	 * custom_paths/custom_plans. If a custom join needs to use more paths,
	 * it needs to keep the other plans and set them to custom_plans.
	 *
	 * XXX Or maybe we should have a separate path type for custom joins?
	 * The CustomPath is very tailored for scans? So there would be a
	 * CustomJoinPath, embedding JoinPath, with separate left/right path.
	 */

	/*
	 * For now we use custom_exprs only to pass join clauses, so a simple list
	 * is enough. If we need to add more stuff to custom_exprs, it will need
	 * to be list of lists.
	 */
	cjoin->custom_exprs = NIL;

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
	foreach(lc, best_path->custom_restrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/* paranoia */
		Assert(IsA(rinfo, RestrictInfo));

		/*
		 * If it's a compatible clause (Var op Var), treat it as a join clause
		 * for the gjoin. Those are stored in custom_exprs.
		 */
		if (join_clause_is_compatible(rinfo->clause))
		{
			join_clauses = lappend(join_clauses, rinfo->clause);
			cjoin->custom_exprs = lappend(cjoin->custom_exprs,
										  rinfo->clause);
			continue;
		}

		/*
		 * Treat it as a filter, not used for the gjoin algorithm itself.
		 * Those can be added to the existing "qual" field.
		 *
		 * XXX We don't need to add it to custom_exprs, I think.
		 */
		cjoin->join.plan.qual = lappend(cjoin->join.plan.qual,
										rinfo->clause);
	}

	/* XXX Should we add qpqual too? Probably not. */

	return (Plan *) cjoin;
}

/*
 * gjoin_CreatePlanState
 *		Initialize state for executor of the smoothscan CustomScan.
 *
 * This does not need to be copied by the executor, so we don't need to
 * stash fields into lists etc.
 */
static Node *
gjoin_CreatePlanState(CustomJoin *cjoin)
{
	GJoinState *state;
	List	   *join_clauses = NIL;
	ListCell   *lc;
	int			idx;

	/* makeNode to set tag etc, repalloc to get the right size */
	state = (GJoinState *) makeNode(CustomJoinState);
	state = repalloc(state, sizeof(GJoinState));

	state->cstate.methods = &gjoin_exec_methods;

	/*
	 * join clauses are passes through the custom_exprs list (if we ever need
	 * to pass other expressions, we'll need to convert it into a list of
	 * lists)
	 */
	join_clauses = cjoin->custom_exprs;

	/* must have valid lists */
	Assert(join_clauses != NIL);

	/* initialize the tuple buffers */
	tuple_buffer_init(&state->buffer.inner);
	tuple_buffer_init(&state->buffer.outer);

	/* initialize the runs */
	batch_runs_init(&state->runs.inner);
	batch_runs_init(&state->runs.outer);

	/* start by initializing the runs etc. */
	state->phase = GJOIN_INIT;

	/* one sort / equality key per join clause */
	join_clauses_init(&state->clauses, list_length(join_clauses));

	/*
	 * Transform the join clause(s) into info we need for sorting and
	 * evaluating the join clauses later.
	 *
	 * XXX For now we require (Var op Var) clauses with equality, to keep the
	 * code simple. We can allow more complex conditions (e.g. with
	 * expressions) later.
	 */
	idx = 0;
	foreach(lc, join_clauses)
	{
		OpExpr	   *opclause = (OpExpr *) lfirst(lc);
		Var		   *var1,
				   *var2;
		TypeCacheEntry *typentry;
		bool		isvarlena;

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
		 * operator instead of LT? But with both vars having the same type it
		 * does not matter.
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
									 TYPECACHE_EQ_OPR |
									 TYPECACHE_LT_OPR |
									 TYPECACHE_CMP_PROC_FINFO |
									 TYPECACHE_HASH_PROC_FINFO);

		Assert(opclause->opno == typentry->eq_opr);
		Assert(idx < state->clauses.nattnums);

		/* sort/equality info */
		state->clauses.attnums_inner[idx] = attnum_inner;
		state->clauses.attnums_outer[idx] = attnum_outer;
		state->clauses.equality[idx] = typentry->eq_opr;
		state->clauses.inequality[idx] = typentry->lt_opr;
		state->clauses.cmp_info[idx] = typentry->cmp_proc_finfo;
		state->clauses.hash_info[idx] = typentry->hash_proc_finfo;
		state->clauses.collations[idx] = opclause->inputcollid;
		state->clauses.nulls_first[idx] = true;

		getTypeOutputInfo(var1->vartype,
						  &state->clauses.outfuncs[idx],
						  &isvarlena);

		idx++;
	}

	/* did we get the expected number of elements in the two arrays? */
	Assert(idx == state->clauses.nattnums);

	/* init the join stats */
	memset(&state->stats, 0, sizeof(JoinStats));

	return (Node *) state;
}

/* customjoin callbacks */
static void
gjoin_BeginCustomJoin(CustomJoinState *node,
					  EState *estate,
					  int eflags)
{
	GJoinState *state = (GJoinState *) node;
	CustomJoin *cjoin = (CustomJoin *) node->js.ps.plan;
	Plan	   *outerplan,
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
	outerplan = cjoin->join.plan.lefttree;
	innerplan = cjoin->join.plan.righttree;

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
	 * rest of init, if needed.
	 */
}

static TupleTableSlot *
gjoin_ExecCustomJoin(CustomJoinState *node)
{
	GJoinState *state = (GJoinState *) node;
	ExprContext *econtext = node->js.ps.ps_ExprContext;

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
				 * Build runs for the inner relation. We assume the inner
				 * relation (called "R" in the paper) is smaller, so we start
				 * with it (we might try loading it into memory and doing a
				 * hashjoin-like in-memory join).
				 *
				 * XXX We should stop building the runs once it hits work_mem,
				 * try building runs on the outer relation, and then
				 * reconsider. Maybe the estimates were off and the outer
				 * relation is smaller, in which case it'd be better to flip
				 * the inner/outer relations for the sake of the algorithm.
				 * But we keep it simple for now.
				 */
				build_inner_runs(state);

				/* build runs for the outer relation next */
				state->phase = GJOIN_BUILD_OUTER;
				break;

			case GJOIN_BUILD_OUTER:

				elog(DEBUG1, "GJOIN_BUILD_OUTER");

				/* Now build runs for the outer relation. */
				build_outer_runs(state);

				/* prepare for reading tuples from the inner runs */
				state->phase = GJOIN_INIT_INNER;
				break;

			case GJOIN_INIT_INNER:

				elog(DEBUG1, "GJOIN_INIT_INNER");

				/* load a bufffer of tuples for each run of the inner relation */
				init_inner_runs(state);

				position_reset(&state->pos_inner);

				/*
				 * Calculate the current join range, defined as
				 *
				 * [Min(minvalue), Min(maxvalue)]
				 *
				 * over all buffers loaded from the runs of the inner
				 * relation. No buffers can be skipped when calculating this
				 * range.
				 */
				update_join_range(state);

				/* next load buffers for the outer relation */
				state->phase = GJOIN_INIT_OUTER;
				break;

			case GJOIN_INIT_OUTER:

				elog(DEBUG1, "GJOIN_INIT_OUTER");

				/* load a bufffer of tuples for each run of the outer relation */
				init_outer_runs(state);

				position_reset(&state->pos_outer);

				/* start by reading a tuple from the outer relation */
				state->phase = GJOIN_NEXT_OUTER;
				break;

			case GJOIN_NEXT_OUTER:

				elog(DEBUG1, "GJOIN_NEXT_OUTER");

				/*
				 * Advances to the next tuple in the outer relation. If there
				 * are no more tuples in the current batch, initiate loading
				 * of the next batch.
				 *
				 * XXX This expects to already have the outer batch loaded
				 * (unless there are no more outer batches).
				 */

				{
					/*
					 * Are there are any outer batches (S) that could be
					 * joined with the already loaded inner batches?
					 *
					 * The whole outer batch needs to be covered by the
					 * "immediate join range" of R, which is th
					 * intersections of ranges for all valid runs.
					 *
					 * We only look at the first batch in the outer run
					 * identified by the priority queue "C" (per paper).
					 */
					Batch	   *batch;

					/*
					 * If we are not already processing a batch from S,
					 * get the next one from the queue "C" (determines in
					 * what order to load buffers for runs of the outer
					 * relation).
					 *
					 * This only tells us which run/batch to look at, the
					 * batch should have been already loaded from earlier
					 * (it happens after evicting the preceding batch).
					 * If there are no more batches, the queue is empty.
					 */
					if (position_is_invalid(&state->pos_outer))
					{
						QueueEntry *entry;
						BatchRun   *run;

						/*
						 * empty queue C means no more batches in S, so
						 * terminate - we can't return any more tuples.
						 */
						if (pairingheap_is_empty(state->queues.outer))
						{
#ifdef USE_ASSERT_CHECKING
							/* no more batches for outer runs */
							for (int i = 0; i < state->runs.outer.nruns; i++)
							{
								Assert(dlist_is_empty(&state->runs.outer.runs[i].batches));
							}
#endif
							return NULL;
						}

						/*
						 * Only peek at the entry, don't remove it from the
						 * queue yet. GJOIN_LOAD_OUTER does that when loading
						 * the next batch for this run.
						 *
						 * XXX The entry is for the *next* batch to load,
						 * after we evict the current one. not the one we
						 * are processing right now. That's how the algorithm
						 * works - we only keep a single outer batch for
						 * each run, I think. At least with the simpler
						 * variant of the algorithm, with three queues.
						 */
						entry = priorityqueues_peek(state->queues.outer);

						/* The entry should be for a valid run. */
						Assert(entry->run < state->runs.outer.nruns);

						/* init the outer position */
						state->pos_outer.run = entry->run;
						state->pos_outer.slot = -1;

						/* get the batch for the run */
						run = &state->runs.outer.runs[state->pos_outer.run];

						/* The selected outer run must have some batches. */
						Assert(!dlist_is_empty(&run->batches));

						state->pos_outer.batch
							= dlist_head_element(Batch, node, &run->batches);

						debug_print_batch(state, "processing outer batch",
										  state->pos_outer.batch);
					}

					Assert((state->pos_outer.run >= 0) &&
						   (state->pos_outer.run < state->runs.outer.nruns));

					/*
					 * XXX Maybe we could stash the buffer somewhere, so that
					 * we don't need to call dlist_head_element over and over
					 * (although it's likely cheap)? Can wait.
					 *
					 * XXX Actually, we already stash it into the position.
					 */
					batch = state->pos_outer.batch;

					/*
					 * Is the whole batch covered by the inner join range?
					 * If not, we need to load some more pages for R (inner).
					 *
					 * XXX We don't need to check the lower boundary. We
					 * never evict inner batches needed by current outer
					 * batches (and therefore no future batches either).
					 * So the lower boundary of the join range is fine.
					 *
					 * XXX But maybe we should still check even the lower
					 * boundary, at least in assert builds. Just to make
					 * sure we're not evicting buffers too early.
					 *
					 * For the upper boundary, we need to be careful about
					 * whether to include/exclude the boundary values. If
					 * there are duplicates, the following inner batch (not
					 * yet loaded) has the same join keys. We could peek
					 * at the next loaded tuple, but for now we just treat
					 * the boundaries as exclusive. This way we may load
					 * an extra batch, but good enough.
					 *
					 * For the last batch in each run we treat the boundary
					 * as "unbounded", i.e. covering any values.
					 */
					if (!batch_in_join_range(state, batch))
					{
						/* need to load more inner batches */
						state->phase = GJOIN_LOAD_INNER;
						continue;
					}

					/* dump current inner/outer runs */
					debug_print_runs(state);

					debug_print_join_range(state);

					/* We can join this batch, advance to the next slot. */
					state->pos_outer.slot++;

					/*
					 * If we ran out of slots in this batch, we need to
					 * load the next outer batch. Reset the position and
					 * request next batch from the outer relation.
					 */
					if (state->pos_outer.slot >= batch->nslots)
					{
						position_reset(&state->pos_outer);
						state->phase = GJOIN_LOAD_OUTER;
						continue;
					}

					/*
					 * Got a valid outer tuple to join. And we also have
					 * all the inner batches with possible join pairs. So
					 * match them.
					 */
					position_reset(&state->pos_inner);
					state->phase = GJOIN_NEXT_INNER;
					continue;
				}

			case GJOIN_NEXT_INNER:

				elog(DEBUG1, "GJOIN_NEXT_INNER");

				/*
				 * Find the next inner tuple matching the current outer one.
				 *
				 * XXX This expects to already have all needed internal
				 * batches loaded (unless there are no more batches).
				 *
				 *
				 * XXX An alternative idea - do a mini-mergejoin after
				 * advancing to the next inner batch, and then simply walk
				 * through the small result, without having to start from
				 * scratch for new outer tuples, etc. That would also make
				 * it unnecessary to do the hashing (although it could be
				 * helpful for types with expensive comparisons, maybe?).
				 */
				{

					/*
					 * We have a slot from S (outer relation) to join in
					 * pos_outer. Walk all batches loaded from R (inner)
					 * runs and join them to the S tuple.
					 *
					 * XXX It should be possible to optimize this by first
					 * comparing the inner batch range to the S range, and
					 * eliminate some of the batches based on that. Although,
					 * we probably should have evicted those already?
					 *
					 * FIXME But maybe we could remember the first matching
					 * inner slot for the outer tuple, and start from there
					 * for the next outer tuple. Because none of the earlier
					 * tuples can match (thanks to sorting).
					 */
					for (;;)
					{
						Batch	   *batch_inner;
						Batch	   *batch_outer;
						BatchRun   *run;
						int			nloops = 0;

						/* Have we ran out of runs? We're done. */
						if (state->pos_inner.run >= state->runs.inner.nruns)
							break;

						/* We've just started, so try the first inner run. */
						if (state->pos_inner.run == -1)
							state->pos_inner.run = 0;

						/* current batch run */
						run = &state->runs.inner.runs[state->pos_inner.run];

						/* get the current inner buffer */
						batch_inner = state->pos_inner.batch;

						/*
						 * If there's no current batch yet, get the first one for
						 * the selected run (if there's one).
						 */
						if (batch_inner == NULL)
						{
							/* skip runs with no more batches */
							if (dlist_is_empty(&run->batches))
							{
								state->pos_inner.run++;
								continue;
							}

							/* get first batch from the run */
							batch_inner = dlist_head_element(Batch, node,
															 &run->batches);
							state->pos_inner.batch = batch_inner;

							debug_print_batch(state, "processing inner batch",
											  state->pos_inner.batch);

							/* new combination of inner/outer batch */
							// state->stats.batches_cross++;

							Assert(state->pos_inner.slot == -1);
						}

						/*
						 * get the current outer batch
						 *
						 * We're always processing only the head batch from
						 * the current outer run.
						 */
						batch_outer = dlist_head_element(Batch, node,
														 &state->runs.outer.runs[state->pos_outer.run].batches);

						/* XXX We should simply use state->pos_outer.batch */
						Assert(state->pos_outer.batch != NULL);
						Assert(state->pos_outer.batch == batch_outer);

						/* Advance to the next slot in the current batch. */
						// state->pos_inner.slot++;
						/**/
						if (state->pos_inner.slot == -1)
						{
							state->pos_inner.slot
								= HASHTABLE_SLOT_INDEX(batch_inner,
													   batch_outer->hashes[state->pos_outer.slot]);
						}
						else
						{
							state->pos_inner.slot
								= (state->pos_inner.slot + HASHTABLE_STEP) % HASHTABLE_CAPACITY(batch_inner);
						}

						/* find the slot with a matching hashtable */
						for (;;)
						{
							uint32 inner_hash;

							/* empty slot, no point in looking for more matches */
							if (HASHTABLE_SLOT_EMPTY(batch_inner, state->pos_inner.slot))
							{
								state->pos_inner.slot = HASHTABLE_CAPACITY(batch_inner);
								break;
							}

							/* slot with a values, does the hash match? */
							inner_hash = batch_inner->hashes[batch_inner->hashtable[state->pos_inner.slot].slot];
							if (inner_hash == batch_outer->hashes[state->pos_outer.slot])
								break;

							/* not empty but not a match, try the next one */
							state->pos_inner.slot
								= (state->pos_inner.slot + HASHTABLE_STEP) % HASHTABLE_CAPACITY(batch_inner);

							/* too many loops, likely an infinite loop */
							nloops++;
							Assert(nloops <= batch_inner->nslots);
						}

						/*
						 * If we processed all slots, or if the two batches
						 * can't possibly overlap, move to the next batch
						 * in this run (or the next run).
						 *
						 * XXX We check that the two batches overlap - if
						 * we could skip a futile loop over all concurrent
						 * combinations of tuples, that would likely be a
						 * huge speedup.
						 *
						 * XXX I'm not sure if getting non-overlapping batches
						 * is inherent to the algorithm (how it calculates the
						 * join range / evicts inner batches), maybe just for
						 * the simplified variant with three queues. Or if
						 * that's some sort of thinko in this code.
						 *
						 * XXX The overlap is checked only on the first slot,
						 * not every time we get here.
						 */
						if ((state->pos_inner.slot >= HASHTABLE_CAPACITY(batch_inner)) ||
							((state->pos_inner.slot == 0) &&
							 !batches_may_overlap(state, batch_inner, batch_outer)))
						{
							DEBUG_LOG("inner batch processed, or batches can't overlap");

							/* advance to the next batch for this run */
							if (dlist_has_next(&run->batches, &batch_inner->node))
							{
								dlist_node *next_node;

								next_node = dlist_next_node(&run->batches,
															&batch_inner->node);
								batch_inner = dlist_container(Batch, node, next_node);

								/*
								 * Start at the position cached from matching
								 * the previous outer tuple (in the same outer
								 * batch). No earlier tuples can match, because
								 * the outer tuples are ordered the same way.
								 */
								state->pos_inner.batch = batch_inner;
								state->pos_inner.slot = batch_inner->cache_pos;
								// state->pos_inner.slot = -1;

								/* new combination of inner/outer batch */
								// state->stats.batches_cross++;
							}
							else
							{
								/* no more batches, try the next run */
								state->pos_inner.batch = NULL;
								state->pos_inner.run++;
								state->pos_inner.slot = -1;
							}

							continue;
						}

						{
							uint32 hash_outer = batch_outer->hashes[state->pos_outer.slot];
							int slot = batch_inner->hashtable[state->pos_inner.slot].slot;
							uint32 hash_inner = batch_inner->hashes[slot];

							Assert(hash_outer == hash_inner);
						}

						/*
						 * Actually try to join inner/outer tuples from the
						 * current inner/outer batches.
						 */
						{
							int		r;

							TupleTableSlot *outer = batch_outer->slots[state->pos_outer.slot];

							int slot = batch_inner->hashtable[state->pos_inner.slot].slot;
							uint32 hash_inner = batch_inner->hashes[slot];
							TupleTableSlot *inner = batch_inner->slots[slot];

							/* new tuple combination */
							state->stats.tuples_cross++;

							/* if the two tuples do not match, continue */
							r = check_join_clause(state, outer, inner);

							if (r != 0)
								continue;

//							if (r > 0)
//							{
//								/*
//								 * remember position for the next outer tuple
//								 *
//								 * XXX It's important to look for *smaller* value
//								 * on the inner side, not a match. Because there
//								 * may not be exact matches for a particular key.
//								 */
//								batch_inner->cache_pos = state->pos_inner.slot;
//								continue;
//							}
//							else if (r < 0)
//							{
//								/*
//								 * More tuples in this inner batch can't possibly
//								 * match, jump to the end (by pretending we got to
//								 * the last slot) and try the next inner batch.
//								 *
//								 * XXX Just terminating the inner walk cuts the
//								 * number of comparisons roughly in half (even
//								 * without the caching of starting position).
//								 */
//								state->pos_inner.slot = (batch_inner->nslots - 1);
//								continue;
//							}

							/*
							 * r=0, so the join clauses match
							 *
							 * We can't set the cache_pos here, because there
							 * might be multiple tuples with the same keys on
							 * either side of the join, and setting cache_pos
							 * here would point at the last inner one. So the
							 * next outer would skip the other matches.
							 */

							/*
							 * Continue by evaluating the filter (join clauses
							 * that are not recognized by the gjoin algorithm).
							 */
							econtext->ecxt_innertuple = inner;
							econtext->ecxt_outertuple = outer;

							/*
							 * The rows seem to match the equality join clause
							 * (per the gjoin algoirthm itself), so check the
							 * additional join filters, if any.
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
					QueueEntry *entry;
					Batch	   *batch;
					bool		loaded;

					/*
					 * FIXME we shouldn't bail out right away, there still may
					 * be some data to join (similarly to the beginning, we
					 * should use infinity for the highkey)
					 */
					if (pairingheap_is_empty(state->queues.inner_grow))
					{
						/* no more inner batches, try joining */
						state->phase = GJOIN_NEXT_OUTER;
						break;
					}

					/* get the next entry, remove it */
					entry = priorityqueues_pop(state->queues.inner_grow);

					batch = batch_init(ExecGetResultType(state->innerstate),
									   state->clauses.nattnums);

					/* load the next buffer from the run */
					loaded = load_inner_batch(state, entry->run, batch);

					/* FIXME account for loaded=false */
					state->stats.batches_inner++;
					state->stats.tuples_inner += batch->nslots;

					/*
					 * FIXME handle loaded=false
					 *
					 * Do we need to do something? It just means there are
					 * no more inner batches in a given run, no? But can
					 * that even happen? We checked the queue is not empty,
					 * so there should be a batch, no?
					 *
					 * XXX Actually, it can happen. Having an entry in
					 * the queue does not mean there are more batches in
					 * that run, it only says which run to try.
					 *
					 * But in that case, try again from another run (as
					 * long as there are entries in the growth queue).
					 */
					if (!loaded)
					{
						/* try loading from another inner run */
						continue;
					}

					position_reset(&state->pos_inner);
					update_join_range(state);

					/*
					 * Retry the join (if we need more inner batches to
					 * cover the join range, we'll get here again soon.
					 */
					state->phase = GJOIN_NEXT_OUTER;
					break;
				}

			case GJOIN_LOAD_OUTER:

				elog(DEBUG1, "GJOIN_LOAD_OUTER");

				{
					/*
					 * FIXME Can't this free the batches prematurely? We
					 * should only free batches that just got joined. And
					 * only then we should load the next one. I think it's
					 * fine, we only get here from GJOIN_NEXT_OUTER, after
					 * processing the last slot.
					 */

					QueueEntry *entry;
					Batch	   *batch;
					// BatchRun   *run;
					bool		loaded;

					Assert(!pairingheap_is_empty(state->queues.outer));

					/* don't remove the entry yet, we'll need it t */
					entry = priorityqueues_peek(state->queues.outer);

					/*
					 * XXX it's a bit strange, but we can't free the batch
					 * here, because it's used in GJOIN_EVICT_INNER.
					 */
					/* unlink the buffer from the list */
					// run = &state->runs.outer.runs[entry->run];
					// batch = dlist_head_element(Batch, node, &run->batches);
					// dlist_delete(&(batch->node));

					batch = batch_init(ExecGetResultType(state->outerstate),
									   state->clauses.nattnums);

					loaded = load_outer_batch(state, entry->run, batch);

					/* make sure we start from scratch in inner batches */
					reset_cache_pos(state);

					/* FIXME account for loaded=false */
					state->stats.batches_outer++;
					state->stats.tuples_outer += batch->nslots;

					/*
					 * FIXME Do we need to do anything when loaded=false?
					 * It means there are no more batches in the outer run.
					 */

					/*
					 * FIXME we shouldn't be resetting the position all the
					 * way back, we know that the next key (at least in the
					 * same run in S) will be higher, so it can't start before
					 * the start of the current key. But it's more complex due
					 * to having multiple runs, so we'll need to remember the
					 * position per run.
					 */
					position_reset(&state->pos_outer);

					/* retry the join */
					state->phase = GJOIN_EVICT_INNER;
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
					QueueEntry *entry_outer;
					Batch	   *batch_outer;
					BatchRun   *run_outer;

					Assert(!pairingheap_is_empty(state->queues.outer));

					/*
					 * Time to finally remove the processed outer batch, we
					 * won't need it anymore. We just remove it from the
					 * queue, but don't free it yet - we still need the
					 * max values to decide which inner batches are safe
					 * to evict.
					 */
					entry_outer = priorityqueues_pop(state->queues.outer);

					/* unlink the batch from the run */
					run_outer = &state->runs.outer.runs[entry_outer->run];
					batch_outer = dlist_head_element(Batch, node, &run_outer->batches);
					dlist_delete(&(batch_outer->node));

					/*
					 * paranoia: make sure the batch and queue entry match
					 *
					 * XXX Does this need to consider the batch may have
					 * unbounded range?
					 */
					Assert(compare_values(state, entry_outer->values,
												 batch_outer->max_values) == 0);

					debug_print_batch(state, "EVICT after outer batch", batch_outer);

					/*
					 * XXX no need to do anything about the outer queue,
					 * e.g. by loading the next batch. That happens in
					 * GJOIN_LOAD_OUTER. It's just the eviction that's
					 * done here.
					 */

					/*
					 * Walk the inner batches from the 'shrink' queue, and
					 * try to evict them. The buffer has to be the head for
					 * a run (the queue only has the run index, but we can't
					 * evict batches from the middle of a run). We can evict
					 * the batch, if it's before the outer batch.
					 */
					for (;;)
					{
						Batch	   *batch_inner;
						BatchRun   *run_inner;
						QueueEntry *entry_inner;

						/* peek - we don't know if we can evict it yet */
						entry_inner = priorityqueues_peek(state->queues.inner_shrink);

						run_inner = &state->runs.inner.runs[entry_inner->run];
						batch_inner = dlist_head_element(Batch, node,
														 &run_inner->batches);

						debug_print_batch(state, "EVICT consider inner batch", batch_inner);

						/* can't evict, some outer batch may need it
						 *
						 * XXX this does not seem right, it compares maximum value
						 * of the inner batch against maximum values of the next
						 * outer batch to load. But AFAICS that does not guarantee
						 * no outer batches will need it, and may evict the inner
						 * batch prematurely. Perhaps this is not quite right and
						 * differs from the paper?
						 */
						if (compare_values(state, entry_inner->values, entry_outer->values) >= 0)
						{
							DEBUG_LOG("EVICT can't evict inner batch, not old enough");
							break;
						}

						/*
						 * thorough check - compare the inner batch to all outer
						 * runs, and see if any of those still needs it.
						 *
						 * XXX This is a workaround for the insufficient check
						 * above, probably a sign of a bug / difference from
						 * what the paper does.
						 */
						if (!batch_can_evict_inner(state, batch_inner))
						{
							DEBUG_LOG("EVICT can't evict inner batch, not old enough (workaround)");
							break;
						}

						debug_print_batch(state, "EVICT evicting inner batch", batch_inner);

						/* can remove the batch, so pop from the queue */
						priorityqueues_pop(state->queues.inner_shrink);

						/*
						 * If there are more inner batches for this run, add the
						 * next one (the oldest one remaining) to the queue.
						 *
						 * XXX Per the check a couple lines back, we don't allow
						 * eviction of the last batch in a run, so this should
						 * always be the case.
						 */

						if (dlist_has_next(&run_inner->batches,
										   &batch_inner->node))
						{
							Batch	   *next_batch;
							dlist_node *next_node;

							next_node = dlist_next_node(&run_inner->batches,
														&batch_inner->node);
							next_batch = dlist_container(Batch, node, next_node);

							priorityqueues_push(state->queues.inner_shrink,
												entry_inner->run, next_batch->max_values);
						}

						/*
						 * unlink the batch from the list, free it
						 *
						 * XXX Maybe we could reuse the batches, so that we
						 * don't need to reinitialize the slots over and
						 * over again?
						 */
						dlist_delete(&(batch_inner->node));
						batch_free(batch_inner);
					}

					/* free the outer batch */
					batch_free(batch_outer);

					state->phase = GJOIN_NEXT_OUTER;
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
	GJoinState *state = (GJoinState *) node;

	/* FIXME cleanup */
	batch_runs_close(&state->runs.inner);
	batch_runs_close(&state->runs.outer);

	/*
	 * clean up subtrees
	 */
	ExecEndNode(state->outerstate);
	ExecEndNode(state->innerstate);
}

static void
gjoin_ReScanCustomJoin(CustomJoinState *node)
{
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
 * Show additional information in EXPLAIN.
 */
static void
gjoin_ExplainCustomJoin(CustomJoinState *node,
						List *ancestors,
						ExplainState *es)
{
	GJoinState *state = (GJoinState *) node;
	CustomJoin *cjoin = (CustomJoin *) node->js.ps.plan;
	List	   *join_clauses = cjoin->custom_exprs;
	StringInfoData str;

	initStringInfo(&str);

	/*
	 * FIXME Show additional run-time information about the plan (number of
	 * runs on each side, peak amount of memory used, ...)
	 */
	show_expression((Node *) join_clauses, "Join Cond",
					(PlanState *) state, ancestors, es);

	if (es->verbose && es->analyze)
	{
		resetStringInfo(&str);
		appendStringInfo(&str, "inner=%d outer=%d",
						 state->runs.inner.nruns,
						 state->runs.outer.nruns);
		ExplainPropertyText("Runs", str.data, es);

		resetStringInfo(&str);
		appendStringInfo(&str, "inner=%d outer=%d cross=%d",
						 state->stats.tuples_inner,
						 state->stats.tuples_outer,
						 state->stats.tuples_cross);
		ExplainPropertyText("Tuples", str.data, es);

		resetStringInfo(&str);
		appendStringInfo(&str, "inner=%d outer=%d cross=%d",
						 state->stats.batches_inner,
						 state->stats.batches_outer,
						 state->stats.batches_cross);
		ExplainPropertyText("Batches", str.data, es);
	}
}

/*
 * join_clause_is_compatible
 *		determine if a join clause can be processed by gjoin
 *
 * For now we only allow (Var op Var) clauses, where "op" is an equality,
 * and both Var nodes have the same data type.
 */
static bool
join_clause_is_compatible(Expr *clause)
{
	OpExpr	   *opclause;
	Var		   *var1,
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
	 * XXX What's the right / generic way to do this? An operator may be in
	 * multiple opclasses, etc. For now just lookup the default btree opclass,
	 * and rely on that.
	 */
	typentry = lookup_type_cache(var1->vartype, TYPECACHE_EQ_OPR);
	if (opclause->opno != typentry->eq_opr)
		return false;

	return true;
}

/*
 * join_clauses_init
 *		initialize the equality / sort information
 *
 * This only allocates the space, does not set any of the values.
 */
static void
join_clauses_init(JoinClauses * sort, int numcols)
{
	sort->nattnums = numcols;
	sort->attnums_inner = palloc_array(AttrNumber, numcols);
	sort->attnums_outer = palloc_array(AttrNumber, numcols);
	sort->equality = palloc_array(Oid, numcols);
	sort->inequality = palloc_array(Oid, numcols);
	sort->collations = palloc_array(Oid, numcols);
	sort->outfuncs = palloc_array(Oid, numcols);
	sort->nulls_first = palloc_array(bool, numcols);

	/* type-specific functions */
	sort->cmp_info = palloc_array(FmgrInfo, numcols);
	sort->hash_info = palloc_array(FmgrInfo, numcols);
}

/*
 * tuple_buffer_init
 *		initialize a buffer for tuples
 *
 * We only reset fields to "empty", we don't allocate any buffer yet.
 */
static void
tuple_buffer_init(TupleBuffer * buffer)
{
	buffer->tuples = NULL;
	buffer->ntuples = 0;
	buffer->maxtuples = 0;
	buffer->space = 0;
}

/*
 * batch_runs_init
 *		initialize runs of buffers
 *
 * We only reset fields to "empty", we don't allocate any buffer yet.
 */
static void
batch_runs_init(BatchRuns * runs)
{
	runs->maxruns = 0;
	runs->nruns = 0;
	runs->runs = NULL;
}

static void
batch_run_init(BatchRun * run, Tuplesortstate *sort)
{
	run->ntuples = 0;
	run->tuplesort = sort;
	dlist_init(&run->batches);
}

/* close the runs - release the tuplesorts, etc. */
static void
batch_runs_close(BatchRuns * runs)
{
	/*
	 * now also end the tuplesort, to prevent warnings about resources
	 *
	 * XXX this should happen much later, after the join
	 */
	for (int i = 0; i < runs->nruns; i++)
	{
		/* stop on the first non-initialized run */
		if (runs->runs[i].tuplesort == NULL)
			break;

		tuplesort_end(runs->runs[i].tuplesort);
	}
}


/*
 * position_reset
 *		reset gjoin position (as if before starting to process runs)
 */
static void
position_reset(JoinPosition * pos)
{
	pos->run = -1;
	pos->slot = -1;
	pos->batch = NULL;
}

/*
 * position_is_invalid
 *		returns true if the position is unset
 */
static bool
position_is_invalid(JoinPosition * pos)
{
	return (pos->run == -1) &&
		(pos->slot == -1) &&
		(pos->batch == NULL);
}

/*
 * buffer_flush_to_run
 *		dump tuples from a buffer into one of the runs
 *
 * The run is selected in a round-robin manner.
 */
static void
buffer_flush_to_run(TupleBuffer * buffer, BatchRuns * runs, int run,
					JoinClauses * clauses, AttrNumber *attnums,
					TupleDesc tdesc)
{
	Tuplesortstate *tuplesortstate;
	int			tuplesortopts = TUPLESORT_NONE;

	/* XXX we should keep the slot in the node state */
	TupleTableSlot *tmpslot;

	/* initialize the array of runs, if needed */
	if (runs->runs == NULL)
	{
		/*
		 * FIXME Set to an arbitrary number, needs to be set based on
		 * work_mem (per the paper).
		 *
		 * XXX Also, the optimal number of runs is a trade off. More runs
		 * on the inner side means more batches to check for each outer
		 * batch, and I suspect it makes the cache_pos less effective
		 * (because the outer batches are smaller, and cache_pos gets
		 * reset more often).
		 */
		runs->maxruns = 32;
		runs->runs = palloc0_array(BatchRun, runs->maxruns);

		for (int i = 0; i < runs->maxruns; i++)
		{
			tuplesortstate = tuplesort_begin_heap(tdesc,
												  clauses->nattnums,
												  attnums,
												  clauses->inequality,
												  clauses->collations,
												  clauses->nulls_first,
												  work_mem,
												  NULL,
												  tuplesortopts);

			batch_run_init(&runs->runs[i], tuplesortstate);
		}

		runs->nruns = runs->maxruns;
	}

	/* offload the tuples */
	tmpslot = MakeSingleTupleTableSlot(tdesc, &TTSOpsHeapTuple);
	for (int i = 0; i < buffer->ntuples; i++)
	{
		run = (i % runs->maxruns);
		runs->runs[run].ntuples += 1;

		tuplesortstate = runs->runs[run].tuplesort;

		ExecClearTuple(tmpslot);
		ExecStoreHeapTuple(buffer->tuples[i], tmpslot, true);
		tuplesort_puttupleslot(tuplesortstate, tmpslot);
	}
	ExecDropSingleTupleTableSlot(tmpslot);

	buffer->space = 0;
	buffer->ntuples = 0;
}

/*
 * Build runs from a given input (inner or outer).
 *
 * XXX Right now this hard-codes the number of runs as 32, and always spills
 * the buffers into the tuplesort (even if it would fit into memory).
 *
 * XXX We should try loading up to work_mem tuples, stop, and try loading
 * the other relation (in case it happens to be smaller). And only if both
 * are too large, switch to the sorted mode.
 */
static void
build_runs(GJoinState * node, PlanState *state,
		   TupleBuffer * buffer, BatchRuns * runs,
		   AttrNumber *attnums)
{
	TupleTableSlot *slot;
	bool		shouldFree;
	HeapTuple	tuple;
	TupleDesc	tdesc = ExecGetResultType(state);
	int			nextrun = 0;
	JoinClauses *clauses = &node->clauses;

	/*
	 * Get all tuples from the node below the Hash node and insert into the
	 * hash table (or temp files).
	 */
	for (;;)
	{
		slot = ExecProcNode(state);
		if (TupIsNull(slot))
			break;

		/* XXX Do we need to materialize the tuple here? */
		tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);

		/*
		 * If we'd exceed the memory allowance, dump the current buffer into
		 * one of the tuplesorts, in a round-robin way.
		 *
		 * XXX We allow accumulating up to work_mem of tuples, because while
		 * building runs we only keep a single buffer in memory.
		 *
		 * XXX Why do we even need to process tuples in buffers while loading
		 * tuples into the tuplesorts? Well, we should probably load them into
		 * memory first, up to work_mem (or maybe work_mem/2), and only then
		 * start spilling to tuplesorts when it's clear it can't do the
		 * hashjoin-like execution. And we should try loading the other side
		 * too first, in case we can swap the sides.
		 *
		 * XXX Maybe distribute the tuples to the runs randomly? That would
		 * make it more resilient to adversary cases, I think. Which is what
		 * some of the cases show in testing for "linear" data sets.
		 *
		 * XXX What if we sorted by hash value, and not by the keys? That
		 * might be cheaper to do the "merge join" between batches, but it
		 * would also mean we can't leverage presorted inputs.
		 */
		if (buffer->space + tuple->t_len > work_mem * 1024L)
		{
			/* flush buffer to a selected run */
			buffer_flush_to_run(buffer, runs, nextrun,
								clauses, attnums, tdesc);

			/* advance to the next run, round-robin way */
			nextrun = (nextrun + 1) % runs->maxruns;
		}

		/*
		 * Add the tuple to the Make sure there's space for adding a tuple to
		 * the buffer. Just double the array size if needed, as usual.
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
		 * we need to copy it here, to prevent seeing garbage later (after it
		 * gets freed for whatever reason).
		 */
		if (!shouldFree)
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
		/* flush buffer to a selected run */
		buffer_flush_to_run(buffer, runs, nextrun,
							clauses, attnums, tdesc);
	}

	/*
	 * Sort all the runs, one by one.
	 */
	for (int i = 0; i < runs->nruns; i++)
	{
		/* stop on the first non-initialized run */
		if (runs->runs[i].tuplesort == NULL)
			break;

		/*
		 * XXX the sorts could be parallelized quite easily, or maybe
		 * we could do each sort in a different worker?
		 */
		tuplesort_performsort(runs->runs[i].tuplesort);
	}
}

static void
build_inner_runs(GJoinState * state)
{
	build_runs(state,
			   state->innerstate,
			   &state->buffer.inner,
			   &state->runs.inner,
			   state->clauses.attnums_inner);
}

static void
build_outer_runs(GJoinState * state)
{
	build_runs(state,
			   state->outerstate,
			   &state->buffer.outer,
			   &state->runs.outer,
			   state->clauses.attnums_outer);
}

/*
 * Initialize a batch of slots for tuples with the provided descriptor.
 *
 * XXX We use MinimalTuples, because that what tuplesort_gettupleslot uses
 */
static Batch *
batch_init(TupleDesc tdesc, int nattnums)
{
	Batch	   *batch;

	batch = palloc(sizeof(Batch));

	batch->maxslots = MAX_BATCH_SIZE;
	batch->nslots = 0;
	batch->slots = palloc_array(TupleTableSlot *, batch->maxslots);
	batch->hashes = palloc_array(uint32, batch->maxslots);

	batch->cache_pos = -1;

	for (int j = 0; j < batch->maxslots; j++)
	{
		batch->slots[j] = MakeSingleTupleTableSlot(tdesc, &TTSOpsMinimalTuple);
	}

	batch->max_values = palloc_array(Datum, nattnums);
	batch->min_values = palloc_array(Datum, nattnums);

	batch->max_isnull = palloc_array(bool, nattnums);
	batch->min_isnull = palloc_array(bool, nattnums);

	batch->hashtable = NULL;

	return batch;
}

/*
 * release memory / slots associated with a batch
 *
 * XXX Maybe we could reuse the batches, so that we don't reinitialize the
 * batch over and over again (particularly creating the slots might be quite
 * expensive, I guess).
 */
static void
batch_free(Batch *batch)
{
	pfree(batch->max_values);
	pfree(batch->min_values);
	pfree(batch->min_isnull);
	pfree(batch->max_isnull);

	for (int j = 0; j < batch->maxslots; j++)
	{
		ExecDropSingleTupleTableSlot(batch->slots[j]);
	}

	pfree(batch->slots);
	pfree(batch->hashes);
	if (batch->hashtable)
		pfree(batch->hashtable);

	pfree(batch);
}

static void
batch_calculate_hashes(GJoinState *state, Batch *batch, AttrNumber *attnums)
{
	for (int i = 0; i < batch->nslots; i++)
	{
		uint32	hashvalue = 0;

		for (int j = 0; j < state->clauses.nattnums; j++)
		{
			Datum	value;
			bool	isnull;

			value = slot_getattr(batch->slots[i], attnums[j], &isnull);

			hashvalue |= DatumGetUInt32(FunctionCall1Coll(&state->clauses.hash_info[j],
														  state->clauses.collations[j],
														  value));
		}

		batch->hashes[i] = hashvalue;
	}
}

static void
batch_build_hashtable(GJoinState *state, Batch *batch)
{
	Assert(batch->hashtable == NULL);

	/* aim for 50% load factor (XXX probably should be power-of-2, with prime step) */
	batch->hashtable = palloc0_array(HashEntry, HASHTABLE_CAPACITY(batch));

	/* mark slots as empty */
	for (int i = 0; i < HASHTABLE_CAPACITY(batch); i++)
	{
		batch->hashtable[i].slot = -1;
	}

	/* add hashes to hash table */
	for (int i = 0; i < batch->nslots; i++)
	{
		int	idx = HASHTABLE_SLOT_INDEX(batch, batch->hashes[i]);

		/* find the next empty slot, linearly */
		while (!HASHTABLE_SLOT_EMPTY(batch, idx))
		{
			idx = (idx + HASHTABLE_STEP) % HASHTABLE_CAPACITY(batch);
		}

		batch->hashtable[idx].hashvalue = batch->hashes[i];
		batch->hashtable[idx].slot = i;
	}
}

/*
 * Initialize the inner runs, i.e. load the first batch of tuples for each
 * run of the inner relation (S).
 *
 * Also initializes the priority queues driving the growth/eviction of the
 * inner runs.
 *
 * Loads one batch (~8KB) of tuples for each run generated for the relation.
 */
static void
init_inner_runs(GJoinState * state)
{
	BatchRuns  *runs = &state->runs.inner;
	TupleDesc	tdesc = ExecGetResultType(state->innerstate);

	/* queues for R */
	state->queues.inner_grow = pairingheap_allocate(priorityqueues_min_cmp, state);
	state->queues.inner_shrink = pairingheap_allocate(priorityqueues_min_cmp, state);

	/*
	 * Initialize batches of slots for all the runs, and load tuples from the
	 * tuplesorts into them.
	 *
	 * XXX We size the batches by number of slots, but it should be driven by
	 * amount of memory used by the slots. But how do you calculate the slot
	 * size in an efficient way?
	 */
	for (int i = 0; i < runs->nruns; i++)
	{
		Batch	   *batch = batch_init(tdesc, state->clauses.nattnums);

		while (tuplesort_gettupleslot(runs->runs[i].tuplesort, true, true,
									  batch->slots[batch->nslots],
									  NULL))
		{
			batch->nslots++;

			/* stop after filling the last slot in the buffer */
			if (batch->nslots == batch->maxslots)
				break;
		}

		/*
		 * We should never see a run with no tuples, because then we don't
		 * create the run at all.
		 */
		Assert(batch->nslots > 0);

		Assert(runs->runs[i].ntuples >= batch->nslots);

		/* update the number of tuples remaining in the run */
		runs->runs[i].ntuples -= batch->nslots;

		/* is this the first/last batch in the run? */
		batch->is_first = true;
		batch->is_last = (runs->runs[i].ntuples == 0);

		/* get the min/max values for the loaded chunks */
		if (batch->nslots > 0)
		{
			/*
			 * initial buffers span from "negative infinity" (lowest value)
			 */
			batch->min_unbounded = true;
			batch->max_unbounded = false;	/* FIXME it can be the last batch */

			for (int j = 0; j < state->clauses.nattnums; j++)
			{
				/*
				 * Use the values from the first/last tuples (because the
				 * data is sorted that way).
				 */
				batch->min_values[j]
					= slot_getattr(batch->slots[0],
								   state->clauses.attnums_inner[j],
								   &batch->max_isnull[j]);

				batch->max_values[j]
					= slot_getattr(batch->slots[batch->nslots - 1],
								   state->clauses.attnums_inner[j],
								   &batch->max_isnull[j]);
			}

			batch_calculate_hashes(state, batch, state->clauses.attnums_inner);

			batch_build_hashtable(state, batch);

			/*
			 * Add the run to the grow/shrink priority queues (the paper calls
			 * those "A" and "B").
			 *
			 * We start with a single buffer per run, so it's both the oldest
			 * and newest loaded buffer. So add it to both queues with the
			 * same uppper limit.
			 */
			priorityqueues_push(state->queues.inner_grow, i, batch->max_values);
			priorityqueues_push(state->queues.inner_shrink, i, batch->max_values);
		}

		/* initialize the list of tuple batches for a run, add the batch */
		dlist_init(&runs->runs[i].batches);
		dlist_push_tail(&runs->runs[i].batches, &batch->node);

		/* account for the loaded batch */
		state->stats.batches_inner++;
		state->stats.tuples_inner += batch->nslots;

		debug_print_batch(state, "loaded inner batch (init)", batch);
	}
}

/*
 * Load one batch (~8KB) of tuples for each run of the outer relation (R).
 *
 * XXX We're not really checking the amount of memory, but the number of
 * slots in the batch.
 */
static void
init_outer_runs(GJoinState * state)
{
	BatchRuns  *runs = &state->runs.outer;
	TupleDesc	tdesc = ExecGetResultType(state->outerstate);

	/* queue for S (for the simplified variant with a single queue) */
	state->queues.outer = pairingheap_allocate(priorityqueues_min_cmp, state);

	/*
	 * Initialize batches of slots for all the runs, and load tuples from the
	 * tuplesorts into them.
	 */
	for (int i = 0; i < runs->nruns; i++)
	{
		Batch	   *batch = batch_init(tdesc, state->clauses.nattnums);

		while (tuplesort_gettupleslot(runs->runs[i].tuplesort, true, true,
									  batch->slots[batch->nslots],
									  NULL))
		{
			batch->nslots++;

			/* stop after filling the last slot */
			if (batch->nslots == batch->maxslots)
				break;
		}

		/*
		 * We should never see a run with no tuples, because then we don't
		 * create the run at all.
		 */
		Assert(batch->nslots > 0);

		Assert(runs->runs[i].ntuples >= batch->nslots);

		/* update the number of tuples remaining in the run */
		runs->runs[i].ntuples -= batch->nslots;

		/* get the min/max for the loaded chunks */
		if (batch->nslots > 0)
		{
			/*
			 * initial buffers span from "negative infinity" (lowest value)
			 */
			batch->min_unbounded = true;
			batch->max_unbounded = false;	/* FIXME it can be the last batch */

			for (int j = 0; j < state->clauses.nattnums; j++)
			{
				/*
				 * Use the values from the first/last tuples (because the
				 * data is sorted that way).
				 */
				batch->min_values[j]
					= slot_getattr(batch->slots[0],
								   state->clauses.attnums_outer[j],
								   &batch->max_isnull[j]);

				batch->max_values[j]
					= slot_getattr(batch->slots[batch->nslots - 1],
								   state->clauses.attnums_outer[j],
								   &batch->max_isnull[j]);
			}

			batch_calculate_hashes(state, batch, state->clauses.attnums_outer);

			/*
			 * Add the buffer to the priority queue "C", used to load new
			 * buffers for the outer relation.
			 *
			 * We use only a single priority queue to schedule both growth and
			 * eviction for S.
			 *
			 * XXX Maybe we should have both, to make it more
			 * memory-efficient? But for now simplicity matters more.
			 */
			priorityqueues_push(state->queues.outer, i, batch->max_values);
		}

		/* initialize the list of tuple batches for a run, add the batch */
		dlist_init(&runs->runs[i].batches);
		dlist_push_tail(&runs->runs[i].batches, &batch->node);

		/* account for the loaded batch */
		state->stats.batches_outer++;
		state->stats.tuples_outer += batch->nslots;

		debug_print_batch(state, "loaded outer batch (init)", batch);
	}
}

/* S */
static bool
load_outer_batch(GJoinState * state, int run, Batch * batch)
{
	/* reset, the buffer might be reused */
	batch->nslots = 0;

	while (tuplesort_gettupleslot(state->runs.outer.runs[run].tuplesort, true, true,
								  batch->slots[batch->nslots],
								  NULL))
	{
		batch->nslots++;

		/* stop after filling the last slot */
		if (batch->nslots == batch->maxslots)
			break;
	}

	/* no more tuples in this run */
	if (batch->nslots == 0)
	{
		Assert(state->runs.outer.runs[run].ntuples == 0);
		return false;
	}

	Assert(state->runs.outer.runs[run].ntuples >= batch->nslots);
	state->runs.outer.runs[run].ntuples -= batch->nslots;

	/* the minimum values are no longer empty */
	batch->min_unbounded = false;

	/* calculate the buffer range (we know it's sorted) */
	for (int i = 0; i < state->clauses.nattnums; i++)
	{
		batch->min_values[i] = slot_getattr(batch->slots[0],
											state->clauses.attnums_outer[i],
											&batch->max_isnull[i]);

		batch->max_values[i] = slot_getattr(batch->slots[batch->nslots - 1],
											state->clauses.attnums_outer[i],
											&batch->max_isnull[i]);
	}

	batch_calculate_hashes(state, batch, state->clauses.attnums_outer);

	/* add the buffer to the priority queue for S */
	priorityqueues_push(state->queues.outer, run, batch->max_values);

	/* also add the buffer to the run */
	dlist_push_tail(&state->runs.outer.runs[run].batches,
					&batch->node);

	debug_print_batch(state, "loaded outer batch", batch);

	return true;
}

/* R */
static bool
load_inner_batch(GJoinState * state, int run, Batch * batch)
{
	/* reset, the buffer might be reused */
	batch->nslots = 0;

	elog(DEBUG1, "load buffer for run %d", run);

	while (tuplesort_gettupleslot(state->runs.inner.runs[run].tuplesort, true, true,
								  batch->slots[batch->nslots],
								  NULL))
	{
		batch->nslots++;

		/* stop after filling the last slot */
		if (batch->nslots == batch->maxslots)
			break;
	}

	/* no more tuples in this run */
	if (batch->nslots == 0)
	{
		Assert(state->runs.inner.runs[run].ntuples == 0);
		return false;
	}

	Assert(state->runs.inner.runs[run].ntuples >= batch->nslots);
	state->runs.inner.runs[run].ntuples -= batch->nslots;

	/* the minimum values are no longer empty */
	batch->is_first = false;
	batch->is_last = (state->runs.inner.runs[run].ntuples == 0);
	batch->min_unbounded = false;
	batch->max_unbounded = batch->is_last;

	/*
	 * XXX I think is_first/is_last and the unbounded flags are quite
	 * redundant, we only need one of those.
	 *
	 * XXX Don't bother calculating max_values for an unbounded range.
	 */

	/* calculate the buffer range (we know it's sorted) */
	for (int i = 0; i < state->clauses.nattnums; i++)
	{
		batch->min_values[i] = slot_getattr(batch->slots[0],
											state->clauses.attnums_inner[i],
											&batch->max_isnull[i]);

		batch->max_values[i] = slot_getattr(batch->slots[batch->nslots - 1],
											state->clauses.attnums_inner[i],
											&batch->max_isnull[i]);
	}

	batch_calculate_hashes(state, batch, state->clauses.attnums_inner);

	batch_build_hashtable(state, batch);

	/* add the buffer to the priority queue that manages growing */
	priorityqueues_push(state->queues.inner_grow, run, batch->max_values);

	/* also add the buffer to the run */
	dlist_push_tail(&state->runs.inner.runs[run].batches,
					&batch->node);

	debug_print_batch(state, "loaded inner batch", batch);

	return true;
}

/*
 * Determine the join range covered by the given run (represented by
 * a list of tuple buffers).
 */
static void
join_range_for_run(GJoinState * state, dlist_head *run,
				   Datum **min_values, Datum **max_values,
				   bool *min_unbounded, bool *max_unbounded)
{
	Batch	   *buffer;
	dlist_iter	iter;

	dlist_foreach(iter, run)
	{
		buffer = dlist_container(Batch, node, iter.cur);

		/*
		 * elog(DEBUG1, " > run %ld %ld", buffer->min_value,
		 * buffer->max_value);
		 */
	}

	buffer = dlist_head_element(Batch, node, run);
	*min_values = buffer->min_values;
	*min_unbounded = buffer->is_first;

	elog(DEBUG1, "head %p", buffer);

	buffer = dlist_tail_element(Batch, node, run);
	*max_values = buffer->max_values;
	*max_unbounded = buffer->is_last;

	elog(DEBUG1, "tail %p", buffer);
}

/*
 * compare arrays of values
 *
 * FIXME use proper comparators for the given type, don't rely on
 * comparing the Datum values, it's bogus.
 */
static int
compare_values(GJoinState * state, Datum *a, Datum *b)
{
	/* elog(WARNING, "state->clauses.nattnums = %d", state->clauses.nattnums); */
	for (int i = 0; i < state->clauses.nattnums; i++)
	{
		int r = DatumGetInt32(FunctionCall2Coll(&state->clauses.cmp_info[i],
												state->clauses.collations[i],
												a[i], b[i]));

		/* equal valus, try the next value */
		if (r == 0)
			continue;

		return r;
	}

	return 0;
}

/* Calculate the join range for all runs. */
static void
update_join_range(GJoinState * state)
{
	Datum	   *min_values,
			   *max_values,
			   *run_min_values,
			   *run_max_values;
	bool		run_min_unbounded,
				run_max_unbounded;

	bool		all_min_unbounded = true,
				all_max_unbounded = true;

	bool		min_set = false,
				max_set = false;

	/* allocate once */
	min_values = palloc_array(Datum, state->clauses.nattnums);
	max_values = palloc_array(Datum, state->clauses.nattnums);
	run_min_values = palloc_array(Datum, state->clauses.nattnums);
	run_max_values = palloc_array(Datum, state->clauses.nattnums);

	for (int i = 0; i < state->runs.inner.nruns; i++)
	{
		/* no buffers loaded for the run (processed) */
		if (dlist_is_empty(&state->runs.inner.runs[i].batches))
			continue;

		join_range_for_run(state, &state->runs.inner.runs[i].batches,
						   &run_min_values, &run_max_values,
						   &run_min_unbounded, &run_max_unbounded);

		all_min_unbounded &= run_min_unbounded;
		all_max_unbounded &= run_max_unbounded;

		/*
		 * Pick the larger (for minvalues) and smaller (for maxvalues).
		 *
		 * Ignore unbounded values, as if there were no values.
		 */
		if (!run_min_unbounded)
		{
			if (!min_set)
				memcpy(min_values, run_min_values,
					   sizeof(Datum) * state->clauses.nattnums);
			else if (compare_values(state, min_values, run_min_values) < 0)
				memcpy(min_values, run_min_values,
					   sizeof(Datum) * state->clauses.nattnums);

			min_set = true;
		}

		if (!run_max_unbounded)
		{
			if (!max_set)
				memcpy(max_values, run_max_values,
					   sizeof(Datum) * state->clauses.nattnums);
			else if (compare_values(state, max_values, run_max_values) > 0)
				memcpy(max_values, run_max_values,
					   sizeof(Datum) * state->clauses.nattnums);
			max_set = true;
		}
	}

	/* FIXME copy the arrays, free the new allocated ones */

	state->join_range.min_values = (all_min_unbounded) ? NULL : min_values;
	state->join_range.max_values = (all_max_unbounded) ? NULL : max_values;

	state->join_range.min_unbounded = all_min_unbounded;
	state->join_range.max_unbounded = all_max_unbounded;

/* 	elog(DEBUG1, "join range = [%ld, %ld]", */
/* 		 state->join_range.min_value, */
/* 		 state->join_range.max_value); */
}

static int
check_join_clause(GJoinState * state,
				  TupleTableSlot *outer, TupleTableSlot *inner)
{
	for (int i = 0; i < state->clauses.nattnums; i++)
	{
		Datum		a,
					b;
		bool		isnull;
		int			r;

		a = slot_getattr(outer, state->clauses.attnums_inner[i], &isnull);
		b = slot_getattr(inner, state->clauses.attnums_outer[i], &isnull);

		r = DatumGetInt32(FunctionCall2Coll(&state->clauses.cmp_info[i],
											state->clauses.collations[i],
											a, b));

		if (r != 0)
			return r;
	}

	return 0;
}

static bool
batch_in_join_range(GJoinState * state, Batch * buffer)
{
	/* if we only have "last" buffers in each run, all buffers match */
	if (state->join_range.max_unbounded)
		return true;

	/* otherwise compare the upper boundary (non-inclusively) */
	return (compare_values(state, state->join_range.max_values, buffer->max_values) > 0);
}

/* quick elimination of batches that can't possibly have matching tuples */
static bool
batches_may_overlap(GJoinState *state, Batch *a, Batch *b)
{
	/* [a,A] < [b,B] */
	if (compare_values(state, a->max_values, b->min_values) < 0)
		return false;

	/* [b,B] < [a,A] */
	if (compare_values(state, b->max_values, a->min_values) < 0)
		return false;

	state->stats.batches_cross++;

	return true;
}

static bool
batch_can_evict_inner(GJoinState *state, Batch *batch_inner)
{
	for (int r = 0; r < state->runs.outer.nruns; r++)
	{
		Batch *batch;
		BatchRun *run = &state->runs.outer.runs[r];

		/* run with no batches, can ignore */
		if (dlist_is_empty(&run->batches))
			continue;

		batch = dlist_head_element(Batch, node, &run->batches);

		/*
		 * outer batch overlaps with the inner batch? can't evict
		 *
		 * XXX Do we need to check the other direction? I don't think so,
		 * because we won't consider evicting those batches, right?
		 */
		if (compare_values(state, batch->min_values, batch_inner->max_values) <= 0)
			return false;
	}

	return true;
}

/* reset the cache_pos in inner batches after advancing to a new outer batch */
static void
reset_cache_pos(GJoinState *state)
{
	for (int r = 0; r < state->runs.inner.nruns; r++)
	{
		dlist_iter	iter;
		BatchRun *run = &state->runs.inner.runs[r];

		dlist_foreach(iter, &run->batches)
		{
			Batch *batch = dlist_container(Batch, node, iter.cur);
			batch->cache_pos = -1;
		}
	}
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
	GJoinState *state = (GJoinState *) arg;

	int			r = compare_values(state, qea->values, qeb->values);

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
	QueueEntry *qe;

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


#ifdef GJOIN_DEBUG
static void
debug_format_values(GJoinState *state, StringInfo str,
					bool unbounded, Datum *values)
{
	if (unbounded)
		appendStringInfoString(str, "(");
	else
		appendStringInfoString(str, "[");

	for (int i = 0; i < state->clauses.nattnums; i++)
	{
		char *tmp;

		tmp = OidOutputFunctionCall(state->clauses.outfuncs[i],
									values[i]);

		if (i > 0)
			appendStringInfoString(str, ", ");

		appendStringInfoString(str, tmp);
		pfree(tmp);
	}

	if (unbounded)
		appendStringInfoString(str, ")");
	else
		appendStringInfoString(str, "]");
}

static void
debug_print_batch(GJoinState *state, char *msg, Batch *batch)
{
	StringInfoData	str;

	initStringInfo(&str);

	appendStringInfoString(&str, "min ");
	debug_format_values(state, &str,
						batch->min_unbounded, batch->min_values);

	appendStringInfoString(&str, " max ");
	debug_format_values(state, &str,
						batch->max_unbounded, batch->max_values);

	elog(LOG, "%s (batch %p): %s", msg, batch, str.data);

	pfree(str.data);
}

static void
debug_print_runs(GJoinState *state)
{
	StringInfoData str;

	initStringInfo(&str);

	/* inner runs */
	elog(LOG, "========================================================");
	elog(LOG, "inner runs count=%d", state->runs.inner.nruns);

	for (int r = 0; r < state->runs.inner.nruns; r++)
	{
		dlist_iter	iter;

		resetStringInfo(&str);
		appendStringInfo(&str, "  inner run %d batches", r);

		dlist_foreach(iter, &state->runs.inner.runs[r].batches)
		{
			Batch *batch = dlist_container(Batch, node, iter.cur);

			appendStringInfoString(&str, " {");
			debug_format_values(state, &str,
								batch->min_unbounded, batch->min_values);

			appendStringInfoString(&str, ", ");
			debug_format_values(state, &str,
								batch->max_unbounded, batch->max_values);

			appendStringInfoString(&str, "}");
		}

		elog(LOG, "%s", str.data);
	}

	elog(LOG, "--------------------------------------------------------");

	/* outer runs */
	elog(LOG, "outer runs count=%d", state->runs.outer.nruns);

	for (int r = 0; r < state->runs.outer.nruns; r++)
	{
		dlist_iter	iter;

		resetStringInfo(&str);
		appendStringInfo(&str, "  outer run %d batches", r);

		dlist_foreach(iter, &state->runs.outer.runs[r].batches)
		{
			Batch *batch = dlist_container(Batch, node, iter.cur);

			appendStringInfoString(&str, " {");
			debug_format_values(state, &str,
								batch->min_unbounded, batch->min_values);

			appendStringInfoString(&str, ", ");
			debug_format_values(state, &str,
								batch->max_unbounded, batch->max_values);

			appendStringInfoString(&str, "}");
		}

		elog(LOG, "%s", str.data);
	}

	elog(LOG, "========================================================");
}

static void
debug_print_join_range(GJoinState *state)
{
	StringInfoData str;

	initStringInfo(&str);
	appendStringInfo(&str, "  join range {");

	if (state->join_range.min_unbounded)
		appendStringInfoString(&str, "(NULL)");
	else
		debug_format_values(state, &str,
							state->join_range.min_unbounded,
							state->join_range.min_values);

	appendStringInfoString(&str, ", ");

	if (state->join_range.max_unbounded)
		appendStringInfoString(&str, "(NULL)");
	else
		debug_format_values(state, &str,
							state->join_range.max_unbounded,
							state->join_range.max_values);

	appendStringInfo(&str, "}");

	elog(LOG, "%s", str.data);
}

static void
debug_print_values(GJoinState *state, char *msg, bool unbounded, Datum *values)
{
	StringInfoData str;

	initStringInfo(&str);
	appendStringInfo(&str, "values (%s)", msg);

	debug_format_values(state, &str, unbounded, values);

	elog(LOG, "%s", str.data);
}

#endif
