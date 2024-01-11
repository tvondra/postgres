/*-------------------------------------------------------------------------
 *
 * execPrefetch.c
 *	  routines for inserting index tuples and enforcing unique and
 *	  exclusion constraints.
 *
 * ExecInsertIndexTuples() is the main entry point.  It's called after
 * inserting a tuple to the heap, and it inserts corresponding index tuples
 * into all indexes.  At the same time, it enforces any unique and
 * exclusion constraints:
 *
 * Unique Indexes
 * --------------
 *
 * Enforcing a unique constraint is straightforward.  When the index AM
 * inserts the tuple to the index, it also checks that there are no
 * conflicting tuples in the index already.  It does so atomically, so that
 * even if two backends try to insert the same key concurrently, only one
 * of them will succeed.  All the logic to ensure atomicity, and to wait
 * for in-progress transactions to finish, is handled by the index AM.
 *
 * If a unique constraint is deferred, we request the index AM to not
 * throw an error if a conflict is found.  Instead, we make note that there
 * was a conflict and return the list of indexes with conflicts to the
 * caller.  The caller must re-check them later, by calling index_insert()
 * with the UNIQUE_CHECK_EXISTING option.
 *
 * Exclusion Constraints
 * ---------------------
 *
 * Exclusion constraints are different from unique indexes in that when the
 * tuple is inserted to the index, the index AM does not check for
 * duplicate keys at the same time.  After the insertion, we perform a
 * separate scan on the index to check for conflicting tuples, and if one
 * is found, we throw an error and the transaction is aborted.  If the
 * conflicting tuple's inserter or deleter is in-progress, we wait for it
 * to finish first.
 *
 * There is a chance of deadlock, if two backends insert a tuple at the
 * same time, and then perform the scan to check for conflicts.  They will
 * find each other's tuple, and both try to wait for each other.  The
 * deadlock detector will detect that, and abort one of the transactions.
 * That's fairly harmless, as one of them was bound to abort with a
 * "duplicate key error" anyway, although you get a different error
 * message.
 *
 * If an exclusion constraint is deferred, we still perform the conflict
 * checking scan immediately after inserting the index tuple.  But instead
 * of throwing an error if a conflict is found, we return that information
 * to the caller.  The caller must re-check them later by calling
 * check_exclusion_constraint().
 *
 * Speculative insertion
 * ---------------------
 *
 * Speculative insertion is a two-phase mechanism used to implement
 * INSERT ... ON CONFLICT DO UPDATE/NOTHING.  The tuple is first inserted
 * to the heap and update the indexes as usual, but if a constraint is
 * violated, we can still back out the insertion without aborting the whole
 * transaction.  In an INSERT ... ON CONFLICT statement, if a conflict is
 * detected, the inserted tuple is backed out and the ON CONFLICT action is
 * executed instead.
 *
 * Insertion to a unique index works as usual: the index AM checks for
 * duplicate keys atomically with the insertion.  But instead of throwing
 * an error on a conflict, the speculatively inserted heap tuple is backed
 * out.
 *
 * Exclusion constraints are slightly more complicated.  As mentioned
 * earlier, there is a risk of deadlock when two backends insert the same
 * key concurrently.  That was not a problem for regular insertions, when
 * one of the transactions has to be aborted anyway, but with a speculative
 * insertion we cannot let a deadlock happen, because we only want to back
 * out the speculatively inserted tuple on conflict, not abort the whole
 * transaction.
 *
 * When a backend detects that the speculative insertion conflicts with
 * another in-progress tuple, it has two options:
 *
 * 1. back out the speculatively inserted tuple, then wait for the other
 *	  transaction, and retry. Or,
 * 2. wait for the other transaction, with the speculatively inserted tuple
 *	  still in place.
 *
 * If two backends insert at the same time, and both try to wait for each
 * other, they will deadlock.  So option 2 is not acceptable.  Option 1
 * avoids the deadlock, but it is prone to a livelock instead.  Both
 * transactions will wake up immediately as the other transaction backs
 * out.  Then they both retry, and conflict with each other again, lather,
 * rinse, repeat.
 *
 * To avoid the livelock, one of the backends must back out first, and then
 * wait, while the other one waits without backing out.  It doesn't matter
 * which one backs out, so we employ an arbitrary rule that the transaction
 * with the higher XID backs out.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execIndexing.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"
#include "utils/spccache.h"

/* index prefetching - probably should be somewhere else, outside indexam */

/*
 * Cache of recently prefetched blocks, organized as a hash table of
 * small LRU caches. Doesn't need to be perfectly accurate, but we
 * aim to make false positives/negatives reasonably low.
 */
typedef struct IndexPrefetchCacheEntry {
	BlockNumber		block;
	uint64			request;
} IndexPrefetchCacheEntry;

/*
 * Size of the cache of recently prefetched blocks - shouldn't be too
 * small or too large. 1024 seems about right, it covers ~8MB of data.
 * It's somewhat arbitrary, there's no particular formula saying it
 * should not be higher/lower.
 *
 * The cache is structured as an array of small LRU caches, so the total
 * size needs to be a multiple of LRU size. The LRU should be tiny to
 * keep linear search cheap enough.
 *
 * XXX Maybe we could consider effective_cache_size or something?
 */
#define		PREFETCH_LRU_SIZE		8
#define		PREFETCH_LRU_COUNT		128
#define		PREFETCH_CACHE_SIZE		(PREFETCH_LRU_SIZE * PREFETCH_LRU_COUNT)

/*
 * Used to detect sequential patterns (and disable prefetching).
 *
 * XXX seems strange to have two separate values
 */
#define		PREFETCH_QUEUE_HISTORY			8
#define		PREFETCH_SEQ_PATTERN_BLOCKS		4

typedef struct IndexPrefetch
{
	/*
	 * XXX We need to disable this in some cases (e.g. when using index-only
	 * scans, we don't want to prefetch pages). Or maybe we should prefetch
	 * only pages that are not all-visible, that'd be even better.
	 */
	int			prefetchTarget;	/* how far we should be prefetching */
	int			prefetchMaxTarget;	/* maximum prefetching distance */
	int			prefetchReset;	/* reset to this distance on rescan */
	bool		prefetchDone;	/* did we get all TIDs from the index? */

	/* runtime statistics */
	uint64		countAll;		/* all prefetch requests */
	uint64		countPrefetch;	/* actual prefetches */
	uint64		countSkipSequential;
	uint64		countSkipCached;

	/*
	 * If a callback is specified, it may store global state (for all TIDs).
	 * For example VM buffer may be kept during IOS. This is similar to the
	 * data field in IndexPrefetchEntry, but that's per-TID.
	 */
	void	   *data;

	/*
	 * Callback to customize the prefetch (decide which block need to be
	 * prefetched, etc.)
	 */
	IndexPrefetchNextCB		next_cb;
	IndexPrefetchCleanupCB	cleanup_cb;

	/*
	 * Queue of TIDs to prefetch.
	 *
	 * XXX Sizing for MAX_IO_CONCURRENCY may be overkill, but it seems simpler
	 * than dynamically adjusting for custom values.
	 */
	IndexPrefetchEntry	queueItems[MAX_IO_CONCURRENCY];
	uint64			queueIndex;	/* next TID to prefetch */
	uint64			queueStart;	/* first valid TID in queue */
	uint64			queueEnd;	/* first invalid (empty) TID in queue */

	/*
	 * A couple of last prefetched blocks, used to check for certain access
	 * pattern and skip prefetching - e.g. for sequential access).
	 *
	 * XXX Separate from the main queue, because we only want to compare the
	 * block numbers, not the whole TID. In sequential access it's likely we
	 * read many items from each page, and we don't want to check many items
	 * (as that is much more expensive).
	 */
	BlockNumber		blockItems[PREFETCH_QUEUE_HISTORY];
	uint64			blockIndex;	/* index in the block (points to the first
								 * empty entry)*/

	/*
	 * Cache of recently prefetched blocks, organized as a hash table of
	 * small LRU caches.
	 */
	uint64				prefetchReqNumber;
	IndexPrefetchCacheEntry	prefetchCache[PREFETCH_CACHE_SIZE];

} IndexPrefetch;


#define PREFETCH_QUEUE_INDEX(a)	((a) % (MAX_IO_CONCURRENCY))
#define PREFETCH_QUEUE_EMPTY(p)	((p)->queueEnd == (p)->queueIndex)
#define PREFETCH_ENABLED(p)		((p) && ((p)->prefetchMaxTarget > 0))
#define PREFETCH_FULL(p)		((p)->queueEnd - (p)->queueIndex == (p)->prefetchTarget)
#define PREFETCH_DONE(p)		((p) && ((p)->prefetchDone && PREFETCH_QUEUE_EMPTY(p)))

/* XXX easy to confuse with PREFETCH_ACTIVE */
#define PREFETCH_ACTIVE(p)		(PREFETCH_ENABLED(p) && !(p)->prefetchDone)
#define PREFETCH_BLOCK_INDEX(v)	((v) % PREFETCH_QUEUE_HISTORY)

int index_heap_prefetch_target(Relation heapRel, double plan_rows, bool allow_prefetch);

/*
 * index_prefetch_is_sequential
 *		Track the block number and check if the I/O pattern is sequential,
 *		or if the same block was just prefetched.
 *
 * Prefetching is cheap, but for some access patterns the benefits are small
 * compared to the extra overhead. In particular, for sequential access the
 * read-ahead performed by the OS is very effective/efficient. Doing more
 * prefetching is just increasing the costs.
 *
 * This tries to identify simple sequential patterns, so that we can skip
 * the prefetching request. This is implemented by having a small queue
 * of block numbers, and checking it before prefetching another block.
 *
 * We look at the preceding PREFETCH_SEQ_PATTERN_BLOCKS blocks, and see if
 * they are sequential. We also check if the block is the same as the last
 * request (which is not sequential).
 *
 * Note that the main prefetch queue is not really useful for this, as it
 * stores TIDs while we care about block numbers. Consider a sorted table,
 * with a perfectly sequential pattern when accessed through an index. Each
 * heap page may have dozens of TIDs, but we need to check block numbers.
 * We could keep enough TIDs to cover enough blocks, but then we also need
 * to walk those when checking the pattern (in hot path).
 *
 * So instead, we maintain a small separate queue of block numbers, and we use
 * this instead.
 *
 * Returns true if the block is in a sequential pattern (and so should not be
 * prefetched), or false (not sequential, should be prefetched).
 *
 * XXX The name is a bit misleading, as it also adds the block number to the
 * block queue and checks if the block is the same as the last one (which
 * does not require a sequential pattern).
 */
static bool
index_prefetch_is_sequential(IndexPrefetch *prefetch, BlockNumber block)
{
	int			idx;

	/*
	 * If the block queue is empty, just store the block and we're done (it's
	 * neither a sequential pattern, neither recently prefetched block).
	 */
	if (prefetch->blockIndex == 0)
	{
		prefetch->blockItems[PREFETCH_BLOCK_INDEX(prefetch->blockIndex)] = block;
		prefetch->blockIndex++;
		return false;
	}

	/*
	 * Check if it's the same as the immediately preceding block. We don't
	 * want to prefetch the same block over and over (which would happen for
	 * well correlated indexes).
	 *
	 * In principle we could rely on index_prefetch_add_cache doing this using
	 * the full cache, but this check is much cheaper and we need to look at
	 * the preceding block anyway, so we just do it.
	 *
	 * XXX Notice we haven't added the block to the block queue yet, and there
	 * is a preceding block (i.e. blockIndex-1 is valid).
	 */
	if (prefetch->blockItems[PREFETCH_BLOCK_INDEX(prefetch->blockIndex - 1)] == block)
		return true;

	/*
	 * Add the block number to the queue.
	 *
	 * We do this before checking if the pattern, because we want to know
	 * about the block even if we end up skipping the prefetch. Otherwise we'd
	 * not be able to detect longer sequential pattens - we'd skip one block
	 * but then fail to skip the next couple blocks even in a perfect
	 * sequential pattern. This ocillation might even prevent the OS
	 * read-ahead from kicking in.
	 */
	prefetch->blockItems[PREFETCH_BLOCK_INDEX(prefetch->blockIndex)] = block;
	prefetch->blockIndex++;

	/*
	 * Check if the last couple blocks are in a sequential pattern. We look
	 * for a sequential pattern of PREFETCH_SEQ_PATTERN_BLOCKS (4 by default),
	 * so we look for patterns of 5 pages (40kB) including the new block.
	 *
	 * XXX Perhaps this should be tied to effective_io_concurrency somehow?
	 *
	 * XXX Could it be harmful that we read the queue backwards? Maybe memory
	 * prefetching works better for the forward direction?
	 */
	for (int i = 1; i < PREFETCH_SEQ_PATTERN_BLOCKS; i++)
	{
		/*
		 * Are there enough requests to confirm a sequential pattern? We only
		 * consider something to be sequential after finding a sequence of
		 * PREFETCH_SEQ_PATTERN_BLOCKS blocks.
		 *
		 * FIXME Better to move this outside the loop.
		 */
		if (prefetch->blockIndex < i)
			return false;

		/*
		 * Calculate index of the earlier block (we need to do -1 as we
		 * already incremented the index when adding the new block to the
		 * queue).
		 */
		idx = PREFETCH_BLOCK_INDEX(prefetch->blockIndex - i - 1);

		/*
		 * For a sequential pattern, blocks "k" step ago needs to have block
		 * number by "k" smaller compared to the current block.
		 */
		if (prefetch->blockItems[idx] != (block - i))
			return false;
	}

	return true;
}

/*
 * index_prefetch_add_cache
 *		Add a block to the cache, check if it was recently prefetched.
 *
 * We don't want to prefetch blocks that we already prefetched recently. It's
 * cheap but not free, and the overhead may have measurable impact.
 *
 * This check needs to be very cheap, even with fairly large caches (hundreds
 * of entries, see PREFETCH_CACHE_SIZE).
 *
 * A simple queue would allow expiring the requests, but checking if it
 * contains a particular block prefetched would be expensive (linear search).
 * Another option would be a simple hash table, which has fast lookup but
 * does not allow expiring entries cheaply.
 *
 * The cache does not need to be perfect, we can accept false
 * positives/negatives, as long as the rate is reasonably low. We also need
 * to expire entries, so that only "recent" requests are remembered.
 *
 * We use a hybrid cache that is organized as many small LRU caches. Each
 * block is mapped to a particular LRU by hashing (so it's a bit like a
 * hash table). The LRU caches are tiny (e.g. 8 entries), and the expiration
 * happens at the level of a single LRU (by tracking only the 8 most recent requests).
 *
 * This allows quick searches and expiration, but with false negatives (when a
 * particular LRU has too many collisions, we may evict entries that are more
 * recent than some other LRU).
 *
 * For example, imagine 128 LRU caches, each with 8 entries - that's 1024
 * prefetch request in total (these are the default parameters.)
 *
 * The recency is determined using a prefetch counter, incremented every
 * time we end up prefetching a block. The counter is uint64, so it should
 * not wrap (125 zebibytes, would take ~4 million years at 1GB/s).
 *
 * To check if a block was prefetched recently, we calculate hash(block),
 * and then linearly search if the tiny LRU has entry for the same block
 * and request less than PREFETCH_CACHE_SIZE ago.
 *
 * At the same time, we either update the entry (for the queried block) if
 * found, or replace the oldest/empty entry.
 *
 * If the block was not recently prefetched (i.e. we want to prefetch it),
 * we increment the counter.
 *
 * Returns true if the block was recently prefetched (and thus we don't
 * need to prefetch it again), or false (should do a prefetch).
 *
 * XXX It's a bit confusing these return values are inverse compared to
 * what index_prefetch_is_sequential does.
 */
static bool
index_prefetch_add_cache(IndexPrefetch *prefetch, BlockNumber block)
{
	IndexPrefetchCacheEntry *entry;

	/* map the block number the the LRU */
	int			lru = hash_uint32(block) % PREFETCH_LRU_COUNT;

	/* age/index of the oldest entry in the LRU, to maybe use */
	uint64		oldestRequest = PG_UINT64_MAX;
	int			oldestIndex = -1;

	/*
	 * First add the block to the (tiny) top-level LRU cache and see if it's
	 * part of a sequential pattern. In this case we just ignore the block and
	 * don't prefetch it - we expect read-ahead to do a better job.
	 *
	 * XXX Maybe we should still add the block to the hybrid cache, in case we
	 * happen to access it later? That might help if we first scan a lot of
	 * the table sequentially, and then randomly. Not sure that's very likely
	 * with index access, though.
	 */
	if (index_prefetch_is_sequential(prefetch, block))
	{
		prefetch->countSkipSequential++;
		return true;
	}

	/*
	 * See if we recently prefetched this block - we simply scan the LRU
	 * linearly. While doing that, we also track the oldest entry, so that we
	 * know where to put the block if we don't find a matching entry.
	 */
	for (int i = 0; i < PREFETCH_LRU_SIZE; i++)
	{
		entry = &prefetch->prefetchCache[lru * PREFETCH_LRU_SIZE + i];

		/* Is this the oldest prefetch request in this LRU? */
		if (entry->request < oldestRequest)
		{
			oldestRequest = entry->request;
			oldestIndex = i;
		}

		/*
		 * If the entry is unused (identified by request being set to 0),
		 * we're done. Notice the field is uint64, so empty entry is
		 * guaranteed to be the oldest one.
		 */
		if (entry->request == 0)
			continue;

		/* Is this entry for the same block as the current request? */
		if (entry->block == block)
		{
			bool		prefetched;

			/*
			 * Is the old request sufficiently recent? If yes, we treat the
			 * block as already prefetched.
			 *
			 * XXX We do add the cache size to the request in order not to
			 * have issues with uint64 underflows.
			 */
			prefetched = ((entry->request + PREFETCH_CACHE_SIZE) >= prefetch->prefetchReqNumber);

			/* Update the request number. */
			entry->request = ++prefetch->prefetchReqNumber;

			prefetch->countSkipCached += (prefetched) ? 1 : 0;

			return prefetched;
		}
	}

	/*
	 * We didn't find the block in the LRU, so store it either in an empty
	 * entry, or in the "oldest" prefetch request in this LRU.
	 */
	Assert((oldestIndex >= 0) && (oldestIndex < PREFETCH_LRU_SIZE));

	/* FIXME do a nice macro */
	entry = &prefetch->prefetchCache[lru * PREFETCH_LRU_SIZE + oldestIndex];

	entry->block = block;
	entry->request = ++prefetch->prefetchReqNumber;

	/* not in the prefetch cache */
	return false;
}

/*
 * index_prefetch
 *		Prefetch the TID, unless it's sequential or recently prefetched.
 *
 * XXX Some ideas how to auto-tune the prefetching, so that unnecessary
 * prefetching does not cause significant regressions (e.g. for nestloop
 * with inner index scan). We could track number of rescans and number of
 * items (TIDs) actually returned from the scan. Then we could calculate
 * rows / rescan and use that to clamp prefetch target.
 *
 * That'd help with cases when a scan matches only very few rows, far less
 * than the prefetchTarget, because the unnecessary prefetches are wasted
 * I/O. Imagine a LIMIT on top of index scan, or something like that.
 *
 * Another option is to use the planner estimates - we know how many rows we're
 * expecting to fetch (on average, assuming the estimates are reasonably
 * accurate), so why not to use that?
 *
 * Of course, we could/should combine these two approaches.
 *
 * XXX The prefetching may interfere with the patch allowing us to evaluate
 * conditions on the index tuple, in which case we may not need the heap
 * tuple. Maybe if there's such filter, we should prefetch only pages that
 * are not all-visible (and the same idea would also work for IOS), but
 * it also makes the indexing a bit "aware" of the visibility stuff (which
 * seems a somewhat wrong). Also, maybe we should consider the filter selectivity
 * (if the index-only filter is expected to eliminate only few rows, then
 * the vm check is pointless). Maybe this could/should be auto-tuning too,
 * i.e. we could track how many heap tuples were needed after all, and then
 * we would consider this when deciding whether to prefetch all-visible
 * pages or not (matters only for regular index scans, not IOS).
 *
 * XXX Maybe we could/should also prefetch the next index block, e.g. stored
 * in BTScanPosData.nextPage.
 *
 * XXX Could we tune the cache size based on execution statistics? We have
 * a cache of limited size (PREFETCH_CACHE_SIZE = 1024 by default), but
 * how do we know it's the right size? Ideally, we'd have a cache large
 * enough to track actually cached blocks. If the OS caches 10240 pages,
 * then we may do 90% of prefetch requests unnecessarily. Or maybe there's
 * a lot of contention, blocks are evicted quickly, and 90% of the blocks
 * in the cache are not actually cached anymore? But we do have a concept
 * of sequential request ID (PrefetchCacheEntry->request), which gives us
 * information about "age" of the last prefetch. Now it's used only when
 * evicting entries (to keep the more recent one), but maybe we could also
 * use it when deciding if the page is cached. Right now any block that's
 * in the cache is considered cached and not prefetched, but maybe we could
 * have "max age", and tune it based on feedback from reading the blocks
 * later. For example, if we find the block in cache and decide not to
 * prefetch it, but then later find we have to do I/O, it means our cache
 * is too large. And we could "reduce" the maximum age (measured from the
 * current prefetchReqNumber value), so that only more recent blocks would
 * be considered cached. Not sure about the opposite direction, where we
 * decide to prefetch a block - AFAIK we don't have a way to determine if
 * I/O was needed or not in this case (so we can't increase the max age).
 * But maybe we could di that somehow speculatively, i.e. increase the
 * value once in a while, and see what happens.
 */
static void
index_prefetch_heap_page(IndexScanDesc scan, IndexPrefetch *prefetch, IndexPrefetchEntry *entry)
{
	BlockNumber block = ItemPointerGetBlockNumber(&entry->tid);

	/*
	 * No heap relation means bitmap index scan, which does prefetching at the
	 * bitmap heap scan, so no prefetch here (we can't do it anyway, without
	 * the heap)
	 *
	 * XXX But in this case we should have prefetchMaxTarget=0, because in
	 * index_bebinscan_bitmap() we disable prefetching. So maybe we should
	 * just check that.
	 *
	 * XXX Comment/check seems obsolete.
	 */
	if (!prefetch)
		return;

	/*
	 * If we got here, prefetching is enabled and it's a node that supports
	 * prefetching (i.e. it can't be a bitmap index scan).
	 *
	 * XXX Comment/check seems obsolete.
	 */
	Assert(scan->heapRelation);

	/*
	 * Do not prefetch the same block over and over again,
	 *
	 * This happens e.g. for clustered or naturally correlated indexes (fkey
	 * to a sequence ID). It's not expensive (the block is in page cache
	 * already, so no I/O), but it's not free either.
	 */
	if (!index_prefetch_add_cache(prefetch, block))
	{
		prefetch->countPrefetch++;

		PrefetchBuffer(scan->heapRelation, MAIN_FORKNUM, block);
		pgBufferUsage.blks_prefetches++;
	}

	prefetch->countAll++;
}

/*
 * index_prefetch_tids
 *		Fill the prefetch queue and issue necessary prefetch requests.
 */
static void
index_prefetch_tids(IndexScanDesc scan, IndexPrefetch *prefetch, ScanDirection direction)
{
	/*
	 * If the prefetching is still active (i.e. enabled and we still
	 * haven't finished reading TIDs from the scan), read enough TIDs into
	 * the queue until we hit the current target.
	 */
	if (PREFETCH_ACTIVE(prefetch))
	{
		/*
		 * Ramp up the prefetch distance incrementally.
		 *
		 * Intentionally done as first, before reading the TIDs into the
		 * queue, so that there's always at least one item. Otherwise we
		 * might get into a situation where we start with target=0 and no
		 * TIDs loaded.
		 */
		prefetch->prefetchTarget = Min(prefetch->prefetchTarget + 1,
									   prefetch->prefetchMaxTarget);

		/*
		 * Now read TIDs from the index until the queue is full (with
		 * respect to the current prefetch target).
		 */
		while (!PREFETCH_FULL(prefetch))
		{
			IndexPrefetchEntry *entry
				= prefetch->next_cb(scan, direction, prefetch->data);

			/* no more entries in this index scan */
			if (entry == NULL)
			{
				prefetch->prefetchDone = true;
				return;
			}

			Assert(ItemPointerEquals(&entry->tid, &scan->xs_heaptid));

			/* store the entry and then maybe issue the prefetch request */
			prefetch->queueItems[PREFETCH_QUEUE_INDEX(prefetch->queueEnd++)] = *entry;

			/* issue the prefetch request? */
			if (entry->prefetch)
				index_prefetch_heap_page(scan, prefetch, entry);
		}
	}
}

/*
 * index_prefetch_get_entry
 *		Get the next entry from the prefetch queue (or from the index directly).
 *
 * If prefetching is enabled, get next entry from the prefetch queue (unless
 * queue is empty). With prefetching disabled, read an entry directly from the
 * index scan.
 *
 * XXX not sure this correctly handles xs_heap_continue - see index_getnext_slot,
 * maybe nodeIndexscan needs to do something more to handle this? Although, that
 * should be in the indexscan next_cb callback, probably.
 *
 * XXX If xs_heap_continue=true, we need to return the last TID.
 */
static IndexPrefetchEntry *
index_prefetch_get_entry(IndexScanDesc scan, IndexPrefetch *prefetch, ScanDirection direction)
{
	IndexPrefetchEntry *entry = NULL;

	/*
	 * With prefetching enabled (even if we already finished reading
	 * all TIDs from the index scan), we need to return a TID from the
	 * queue. Otherwise, we just get the next TID from the scan
	 * directly.
	 */
	if (PREFETCH_ENABLED(prefetch))
	{
		/* Did we reach the end of the scan and the queue is empty? */
		if (PREFETCH_DONE(prefetch))
			return NULL;

		entry = palloc(sizeof(IndexPrefetchEntry));

		entry->tid = prefetch->queueItems[PREFETCH_QUEUE_INDEX(prefetch->queueIndex)].tid;
		entry->data = prefetch->queueItems[PREFETCH_QUEUE_INDEX(prefetch->queueIndex)].data;

		prefetch->queueIndex++;

		scan->xs_heaptid = entry->tid;
	}
	else				/* not prefetching, just do the regular work  */
	{
		ItemPointer tid;

		/* Time to fetch the next TID from the index */
		tid = index_getnext_tid(scan, direction);

		/* If we're out of index entries, we're done */
		if (tid == NULL)
			return NULL;

		Assert(ItemPointerEquals(tid, &scan->xs_heaptid));

		entry = palloc(sizeof(IndexPrefetchEntry));

		entry->tid = scan->xs_heaptid;
		entry->data = NULL;
	}

	return entry;
}

int
index_heap_prefetch_target(Relation heapRel, double plan_rows, bool allow_prefetch)
{
	/*
	 * XXX No prefetching for direct I/O.
	 *
	 * XXX Shouldn't we do prefetching even for direct I/O? We would only pretend
	 * doing it now, ofc, because we'd not do posix_fadvise(), but once the code
	 * starts loading into shared buffers, that'd work.
	 */
	if ((io_direct_flags & IO_DIRECT_DATA) == 0)
		return 0;

	/* disable prefetching for cursors etc. */
	if (!allow_prefetch)
		return 0;

	/*
	 * Determine number of heap pages to prefetch for this index. This is
	 * essentially just effective_io_concurrency for the table (or the
	 * tablespace it's in).
	 *
	 * XXX Should this also look at plan.plan_rows and maybe cap the target
	 * to that? Pointless to prefetch more than we expect to use. Or maybe
	 * just reset to that value during prefetching, after reading the next
	 * index page (or rather after rescan)?
	 *
	 * XXX Maybe reduce the value with parallel workers?
	 */
	return Min(get_tablespace_io_concurrency(heapRel->rd_rel->reltablespace),
			   plan_rows);
}

IndexPrefetch *
IndexPrefetchAlloc(IndexPrefetchNextCB next_cb, IndexPrefetchCleanupCB cleanup_cb,
				   int prefetch_max, void *data)
{
	IndexPrefetch *prefetch = palloc0(sizeof(IndexPrefetch));

	prefetch->queueIndex = 0;
	prefetch->queueStart = 0;
	prefetch->queueEnd = 0;

	prefetch->prefetchTarget = 0;
	prefetch->prefetchMaxTarget = prefetch_max;

	/*
	 * Customize the prefetch to also check visibility map and keep
	 * the result so that IOS does not need to repeat it.
	 */
	prefetch->next_cb = next_cb;
	prefetch->cleanup_cb = cleanup_cb;
	prefetch->data = data;

	return prefetch;
}

IndexPrefetchEntry *
IndexPrefetchNext(IndexScanDesc scan, IndexPrefetch *prefetch, ScanDirection direction)
{
	/* Do prefetching (if requested/enabled). */
	index_prefetch_tids(scan, prefetch, direction);

	/* Read the TID from the queue (or directly from the index). */
	return index_prefetch_get_entry(scan, prefetch, direction);
}

void
IndexPrefetchReset(IndexScanDesc scan, IndexPrefetch *state)
{
	if (!state)
		return;

	state->queueIndex = 0;
	state->queueStart = 0;
	state->queueEnd = 0;

	state->prefetchDone = false;
	state->prefetchTarget = 0;
}

/* XXX print some debug info */
void
IndexPrefetchStats(IndexScanDesc scan, IndexPrefetch *state)
{
	if (!state)
		return;

	elog(LOG, "index prefetch stats: requests " UINT64_FORMAT " prefetches " UINT64_FORMAT " (%f) skip cached " UINT64_FORMAT " sequential " UINT64_FORMAT,
		 state->countAll,
		 state->countPrefetch,
		 state->countPrefetch * 100.0 / state->countAll,
		 state->countSkipCached,
		 state->countSkipSequential);
}

void
IndexPrefetchEnd(IndexScanDesc scan, IndexPrefetch *state)
{
	if (!state)
		return;

	if (!state->cleanup_cb)
		return;

	state->cleanup_cb(scan, state->data);
}
