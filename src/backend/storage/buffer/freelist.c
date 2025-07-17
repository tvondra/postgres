/*-------------------------------------------------------------------------
 *
 * freelist.c
 *	  routines for managing the buffer pool's replacement strategy.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/freelist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sched.h>
#include <sys/sysinfo.h>

#ifdef USE_LIBNUMA
#include <numa.h>
#include <numaif.h>
#endif

#include "pgstat.h"
#include "port/atomics.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"

#define INT_ACCESS_ONCE(var)	((int)(*((volatile int *)&(var))))

/*
 * Represents one freelist partition.
 */
typedef struct BufferStrategyFreelist
{
	/* Spinlock: protects the values below */
	slock_t		freelist_lock;

	/*
	 * XXX Not sure why this needs to be aligned like this. Need to ask
	 * Andres.
	 */
	int			firstFreeBuffer __attribute__((aligned(64)));	/* Head of list of
																 * unused buffers */

	/* Number of buffers consumed from this list. */
	uint64		consumed;
}			BufferStrategyFreelist;

/*
 * Information about one partition of the ClockSweep (on a subset of buffers).
 *
 * XXX Should be careful to align this to cachelines, etc.
 */
typedef struct
{
	/* Spinlock: protects the values below */
	slock_t		clock_sweep_lock;

	/* range for this clock weep partition */
	int32		firstBuffer;
	int32		numBuffers;

	/*
	 * Clock sweep hand: index of next buffer to consider grabbing. Note that
	 * this isn't a concrete buffer - we only ever increase the value. So, to
	 * get an actual buffer, it needs to be used modulo NBuffers.
	 *
	 * XXX firstBuffer + (nextVictimBuffer % numBuffers)
	 */
	pg_atomic_uint32 nextVictimBuffer;

	/*
	 * Statistics.  These counters should be wide enough that they can't
	 * overflow during a single bgwriter cycle.
	 */
	uint32		completePasses; /* Complete cycles of the clock sweep */
	pg_atomic_uint32 numBufferAllocs;	/* Buffers allocated since last reset */
} ClockSweep;

/*
 * The minimum number of clocksweep partitions. We always want at least this
 * number of partitions, even on a single NUMA node, as it helps with contention
 * for buffers. But with multiple NUMA nodes, we want a separate partition per
 * NUMA node. But we may get more, as we still want at least the minimum.
 *
 * With multiple partitions per NUMA node, we pick the partition based on CPU.
 *
 * XXX Should be synchronized with freelist partitions, probably. If we still
 * have freelists.
 */
#define MIN_CLOCKSWEEP_PARTITIONS		4

/*
 * The shared freelist control information.
 */
typedef struct
{
	/* Spinlock: protects the values below */
	slock_t		buffer_strategy_lock;

	/*
	 * Bgworker process to be notified upon activity or -1 if none. See
	 * StrategyNotifyBgWriter.
	 *
	 * XXX Not sure why this needs to be aligned like this. Need to ask
	 * Andres. Also, shouldn't the alignment be specified after, like for
	 * "consumed"?
	 */
	int			__attribute__((aligned(64))) bgwprocno;

	/*
	 * clocksweep partitions
	 *
	 * XXX Isn't num_sweeps_partitions_groups equal to strategy_nnodes?
	 */
	int			num_sweeps_partitions;
	int			num_sweeps_partitions_groups;
	int			num_sweeps_partitions_per_group;
	ClockSweep *sweeps;

	BufferStrategyFreelist freelists[FLEXIBLE_ARRAY_MEMBER];
} BufferStrategyControl;

/* Pointers to shared state */
static BufferStrategyControl *StrategyControl = NULL;

/*
 * XXX shouldn't this be in BufferStrategyControl? Probably not, we need to
 * calculate it during sizing, and perhaps it could change before the memory
 * gets allocated (so we need to remember the values).
 *
 * XXX We should probably have a fixed number of partitions, and map the
 * NUMA nodes to them, somehow (i.e. each node would get some subset of
 * partitions). Similar to NUM_LOCK_PARTITIONS.
 *
 * XXX We don't use the ncpus, really.
 */
static int	strategy_ncpus;
static int	strategy_nnodes;

/*
 * Private (non-shared) state for managing a ring of shared buffers to re-use.
 * This is currently the only kind of BufferAccessStrategy object, but someday
 * we might have more kinds.
 */
typedef struct BufferAccessStrategyData
{
	/* Overall strategy type */
	BufferAccessStrategyType btype;
	/* Number of elements in buffers[] array */
	int			nbuffers;

	/*
	 * Index of the "current" slot in the ring, ie, the one most recently
	 * returned by GetBufferFromRing.
	 */
	int			current;

	/*
	 * Array of buffer numbers.  InvalidBuffer (that is, zero) indicates we
	 * have not yet selected a buffer for this ring slot.  For allocation
	 * simplicity this is palloc'd together with the fixed fields of the
	 * struct.
	 */
	Buffer		buffers[FLEXIBLE_ARRAY_MEMBER];
}			BufferAccessStrategyData;


/* Prototypes for internal functions */
static BufferDesc *GetBufferFromRing(BufferAccessStrategy strategy,
									 uint32 *buf_state);
static void AddBufferToRing(BufferAccessStrategy strategy,
							BufferDesc *buf);
static ClockSweep *ChooseClockSweep(void);

/*
 * ClockSweepTick - Helper routine for StrategyGetBuffer()
 *
 * Move the clock hand one buffer ahead of its current position and return the
 * id of the buffer now under the hand.
 */
static inline uint32
ClockSweepTick(void)
{
	uint32		victim;
	ClockSweep *sweep = ChooseClockSweep();

	/*
	 * Atomically move hand ahead one buffer - if there's several processes
	 * doing this, this can lead to buffers being returned slightly out of
	 * apparent order.
	 */
	victim =
		pg_atomic_fetch_add_u32(&sweep->nextVictimBuffer, 1);

	if (victim >= sweep->numBuffers)
	{
		uint32		originalVictim = victim;

		/* always wrap what we look up in BufferDescriptors */
		victim = victim % sweep->numBuffers;

		/*
		 * If we're the one that just caused a wraparound, force
		 * completePasses to be incremented while holding the spinlock. We
		 * need the spinlock so StrategySyncStart() can return a consistent
		 * value consisting of nextVictimBuffer and completePasses.
		 */
		if (victim == 0)
		{
			uint32		expected;
			uint32		wrapped;
			bool		success = false;

			expected = originalVictim + 1;

			while (!success)
			{
				/*
				 * Acquire the spinlock while increasing completePasses. That
				 * allows other readers to read nextVictimBuffer and
				 * completePasses in a consistent manner which is required for
				 * StrategySyncStart().  In theory delaying the increment
				 * could lead to an overflow of nextVictimBuffers, but that's
				 * highly unlikely and wouldn't be particularly harmful.
				 */
				SpinLockAcquire(&sweep->clock_sweep_lock);

				wrapped = expected % sweep->numBuffers;

				success = pg_atomic_compare_exchange_u32(&sweep->nextVictimBuffer,
														 &expected, wrapped);
				if (success)
					sweep->completePasses++;
				SpinLockRelease(&sweep->clock_sweep_lock);
			}
		}
	}

	/* XXX buffer IDs are 1-based, we're calculating 0-based indexes */
	Assert(BufferIsValid(1 + sweep->firstBuffer + (victim % sweep->numBuffers)));

	return sweep->firstBuffer + victim;
}

/*
 * ChooseClockSweep
 *		pick a clocksweep partition based on NUMA node and CPU
 *
 * The number of clocksweep partitions may not match the number of NUMA
 * nodes, but it should not be lower. Each partition should be mapped to
 * a single NUMA node, but a node may have multiple partitions. If there
 * are multiple partitions per node (all nodes have the same number of
 * partitions), we pick the partition using CPU.
 *
 * XXX We might also use PID instead of CPU, to make it more stable?
 *
 * XXX Maybe we should do both the total and "per group" counts a power of
 * two? That'd allow using shifts instead of divisions in the calculation,
 * and that's cheaper. But how would that deal with odd number of nodes?
 */
static ClockSweep *
ChooseClockSweep(void)
{
	int			rc;
	unsigned	cpu;
	unsigned	node;
	int			index;

	Assert(StrategyControl->num_sweeps_partitions_groups == strategy_nnodes);

	Assert(StrategyControl->num_sweeps_partitions ==
		   (strategy_nnodes * StrategyControl->num_sweeps_partitions_per_group));

	/*
	 * freelist is partitioned, so determine the CPU/NUMA node, and pick a
	 * list based on that.
	 */
	rc = getcpu(&cpu, &node);
	if (rc != 0)
		elog(ERROR, "getcpu failed: %m");

	/*
	 * XXX We should't get nodes that we haven't considered while building
	 * the partitions. Maybe if we allow this (e.g. due to support adjusting
	 * the NUMA stuff at runtime), we should just do our best to minimize
	 * the conflicts somehow. But it'll make the mapping harder, so for now
	 * we ignore it.
	 */
	if (node > strategy_nnodes)
		elog(ERROR, "node out of range: %d > %u", cpu, strategy_nnodes);

	/*
	 * Find the partition. If we have a single partition per node, we can
	 * calculate the index directly from node. Otherwise we need to do two
	 * steps, using node and then cpu.
	 */
	if (StrategyControl->num_sweeps_partitions_per_group == 1)
	{
		index = (node % StrategyControl->num_sweeps_partitions);
	}
	else
	{
		int		index_group,
				index_part;

		/* two steps - calculate group from node, partition from cpu */
		index_group = (node % StrategyControl->num_sweeps_partitions_groups);
		index_part = (cpu % StrategyControl->num_sweeps_partitions_per_group);

		index = (index_group * StrategyControl->num_sweeps_partitions_per_group)
				+ index_part;
	}

	return &StrategyControl->sweeps[index];
}

/*
 * ChooseFreeList
 *		Pick the buffer freelist to use, depending on the CPU and NUMA node.
 *
 * Without partitioned freelists (numa_partition_freelist=false), there's only
 * a single freelist, so use that.
 *
 * With partitioned freelists, we have multiple ways how to pick the freelist
 * for the backend:
 *
 * - one freelist per CPU, use the freelist for CPU the task executes on
 *
 * - one freelist per NUMA node, use the freelist for node task executes on
 *
 * - use fixed number of freelists, map processes to lists based on PID
 *
 * There may be some other strategies, not sure. The important thing is this
 * needs to be refrecled during initialization, i.e. we need to create the
 * right number of lists.
 */
static BufferStrategyFreelist *
ChooseFreeList(void)
{
	unsigned	cpu;
	unsigned	node;
	int			rc;

	int			freelist_idx;

	/* freelist not partitioned, return the first (and only) freelist */
	if (!numa_partition_freelist)
		return &StrategyControl->freelists[0];

	/*
	 * freelist is partitioned, so determine the CPU/NUMA node, and pick a
	 * list based on that.
	 */
	rc = getcpu(&cpu, &node);
	if (rc != 0)
		elog(ERROR, "getcpu failed: %m");

	/*
	 * FIXME This doesn't work well if CPUs are excluded from being run or
	 * offline. In that case we end up not using some freelists at all, but
	 * not sure if we need to worry about that. Probably not for now. But
	 * could that change while the system is running?
	 *
	 * XXX Maybe we should somehow detect changes to the list of CPUs, and
	 * rebuild the lists if that changes? But that seems expensive.
	 */
	if (node > strategy_nnodes)
		elog(ERROR, "node out of range: %d > %u", cpu, strategy_nnodes);

	/*
	 * Pick the freelist, based on CPU, NUMA node or process PID. This matches
	 * how we built the freelists above.
	 *
	 * XXX Can we rely on some of the values (especially strategy_nnodes) to
	 * be a power-of-2? Then we could replace the modulo with a mask, which is
	 * likely more efficient.
	 */
	freelist_idx = node % strategy_nnodes;

	return &StrategyControl->freelists[freelist_idx];
}

/*
 * have_free_buffer -- a lockless check to see if there is a free buffer in
 *					   buffer pool.
 *
 * If the result is true that will become stale once free buffers are moved out
 * by other operations, so the caller who strictly want to use a free buffer
 * should not call this.
 */
bool
have_free_buffer(void)
{
	for (int i = 0; i < strategy_nnodes; i++)
	{
		if (StrategyControl->freelists[i].firstFreeBuffer >= 0)
			return true;
	}

	return false;
}

/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	BufferAlloc(). The only hard requirement BufferAlloc() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	strategy is a BufferAccessStrategy object, or NULL for default strategy.
 *
 *	To ensure that no one else can pin the buffer before we do, we must
 *	return the buffer with the buffer header spinlock still held.
 */
BufferDesc *
StrategyGetBuffer(BufferAccessStrategy strategy, uint32 *buf_state, bool *from_ring)
{
	BufferDesc *buf;
	int			bgwprocno;
	int			trycounter;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */
	BufferStrategyFreelist *freelist;

	*from_ring = false;

	/*
	 * If given a strategy object, see whether it can select a buffer. We
	 * assume strategy objects don't need buffer_strategy_lock.
	 */
	if (strategy != NULL)
	{
		buf = GetBufferFromRing(strategy, buf_state);
		if (buf != NULL)
		{
			*from_ring = true;
			return buf;
		}
	}

	/*
	 * If asked, we need to waken the bgwriter. Since we don't want to rely on
	 * a spinlock for this we force a read from shared memory once, and then
	 * set the latch based on that value. We need to go through that length
	 * because otherwise bgwprocno might be reset while/after we check because
	 * the compiler might just reread from memory.
	 *
	 * This can possibly set the latch of the wrong process if the bgwriter
	 * dies in the wrong moment. But since PGPROC->procLatch is never
	 * deallocated the worst consequence of that is that we set the latch of
	 * some arbitrary process.
	 */
	bgwprocno = INT_ACCESS_ONCE(StrategyControl->bgwprocno);
	if (bgwprocno != -1)
	{
		/* reset bgwprocno first, before setting the latch */
		StrategyControl->bgwprocno = -1;

		/*
		 * Not acquiring ProcArrayLock here which is slightly icky. It's
		 * actually fine because procLatch isn't ever freed, so we just can
		 * potentially set the wrong process' (or no process') latch.
		 */
		SetLatch(&ProcGlobal->allProcs[bgwprocno].procLatch);
	}

	/*
	 * We count buffer allocation requests so that the bgwriter can estimate
	 * the rate of buffer consumption.  Note that buffers recycled by a
	 * strategy object are intentionally not counted here.
	 */
	pg_atomic_fetch_add_u32(&ChooseClockSweep()->numBufferAllocs, 1);

	/*
	 * First check, without acquiring the lock, whether there's buffers in the
	 * freelist. Since we otherwise don't require the spinlock in every
	 * StrategyGetBuffer() invocation, it'd be sad to acquire it here -
	 * uselessly in most cases. That obviously leaves a race where a buffer is
	 * put on the freelist but we don't see the store yet - but that's pretty
	 * harmless, it'll just get used during the next buffer acquisition.
	 *
	 * If there's buffers on the freelist, acquire the spinlock to pop one
	 * buffer of the freelist. Then check whether that buffer is usable and
	 * repeat if not.
	 *
	 * Note that the freeNext fields are considered to be protected by the
	 * buffer_strategy_lock not the individual buffer spinlocks, so it's OK to
	 * manipulate them without holding the spinlock.
	 */
	freelist = ChooseFreeList();
	if (freelist->firstFreeBuffer >= 0)
	{
		while (true)
		{
			/* Acquire the spinlock to remove element from the freelist */
			SpinLockAcquire(&freelist->freelist_lock);

			if (freelist->firstFreeBuffer < 0)
			{
				SpinLockRelease(&freelist->freelist_lock);
				break;
			}

			buf = GetBufferDescriptor(freelist->firstFreeBuffer);
			Assert(buf->freeNext != FREENEXT_NOT_IN_LIST);

			/* Unconditionally remove buffer from freelist */
			freelist->firstFreeBuffer = buf->freeNext;
			buf->freeNext = FREENEXT_NOT_IN_LIST;

			/* increment number of buffers we consumed from this list */
			freelist->consumed++;

			/*
			 * Release the lock so someone else can access the freelist while
			 * we check out this buffer.
			 */
			SpinLockRelease(&freelist->freelist_lock);

			/*
			 * If the buffer is pinned or has a nonzero usage_count, we cannot
			 * use it; discard it and retry.  (This can only happen if VACUUM
			 * put a valid buffer in the freelist and then someone else used
			 * it before we got to it.  It's probably impossible altogether as
			 * of 8.3, but we'd better check anyway.)
			 */
			local_buf_state = LockBufHdr(buf);
			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
				&& BUF_STATE_GET_USAGECOUNT(local_buf_state) == 0)
			{
				if (strategy != NULL)
					AddBufferToRing(strategy, buf);
				*buf_state = local_buf_state;
				return buf;
			}
			UnlockBufHdr(buf, local_buf_state);
		}
	}

	/*
	 * Nothing on the freelist, so run the "clock sweep" algorithm
	 *
	 * XXX Should we also make this NUMA-aware, to only access buffers from
	 * the same NUMA node? That'd probably mean we need to make the clock
	 * sweep NUMA-aware, perhaps by having multiple clock sweeps, each for a
	 * subset of buffers. But that also means each process could "sweep" only
	 * a fraction of buffers, even if the other buffers are better candidates
	 * for eviction. Would that also mean we'd have multiple bgwriters, one
	 * for each node, or would one bgwriter handle all of that?
	 */
	trycounter = NBuffers;
	for (;;)
	{
		buf = GetBufferDescriptor(ClockSweepTick());

		/*
		 * If the buffer is pinned or has a nonzero usage_count, we cannot use
		 * it; decrement the usage_count (unless pinned) and keep scanning.
		 */
		local_buf_state = LockBufHdr(buf);

		if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
		{
			if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
			{
				local_buf_state -= BUF_USAGECOUNT_ONE;

				trycounter = NBuffers;
			}
			else
			{
				/* Found a usable buffer */
				if (strategy != NULL)
					AddBufferToRing(strategy, buf);
				*buf_state = local_buf_state;
				return buf;
			}
		}
		else if (--trycounter == 0)
		{
			/*
			 * We've scanned all the buffers without making any state changes,
			 * so all the buffers are pinned (or were when we looked at them).
			 * We could hope that someone will free one eventually, but it's
			 * probably better to fail than to risk getting stuck in an
			 * infinite loop.
			 */
			UnlockBufHdr(buf, local_buf_state);
			elog(ERROR, "no unpinned buffers available");
		}
		UnlockBufHdr(buf, local_buf_state);
	}
}

/*
 * StrategyFreeBuffer: put a buffer on the freelist
 *
 * XXX This calls ChooseFreeList() again, and it might return the freelist to
 * a different freelist than it was taken from (either by a different backend,
 * or perhaps even the same backend running on a different CPU). Is that good?
 * Maybe we should try to balance this somehow, e.g. by choosing a random list,
 * the shortest one, or something like that? But that breaks the whole idea of
 * having freelists with buffers from a particular NUMA node.
 */
void
StrategyFreeBuffer(BufferDesc *buf)
{
	BufferStrategyFreelist *freelist;

	freelist = ChooseFreeList();

	SpinLockAcquire(&freelist->freelist_lock);

	/*
	 * It is possible that we are told to put something in the freelist that
	 * is already in it; don't screw up the list if so.
	 */
	if (buf->freeNext == FREENEXT_NOT_IN_LIST)
	{
		buf->freeNext = freelist->firstFreeBuffer;
		freelist->firstFreeBuffer = buf->buf_id;
	}

	SpinLockRelease(&freelist->freelist_lock);
}

/*
 * StrategySyncStart -- prepare for sync of all partitions
 *
 * Determine the number of clocksweep partitions, and calculate the recent
 * buffers allocs (as a sum of all the partitions). This allows BgBufferSync
 * to calculate average number of allocations per partition for the next
 * sync cycle.
 */
void
StrategySyncPrepare(int *num_parts, uint32 *num_buf_alloc)
{
	*num_buf_alloc = 0;
	*num_parts = StrategyControl->num_sweeps_partitions;

	/*
	 * We lock the partitions one by one, so not exacly in sync, but that
	 * should be fine. We're only looking for heuristics anyway.
	 */
	for (int i = 0; i < StrategyControl->num_sweeps_partitions; i++)
	{
		ClockSweep *sweep = &StrategyControl->sweeps[i];

		SpinLockAcquire(&sweep->clock_sweep_lock);
		if (num_buf_alloc)
		{
			*num_buf_alloc += pg_atomic_exchange_u32(&sweep->numBufferAllocs, 0);
		}
		SpinLockRelease(&sweep->clock_sweep_lock);
	}
}

/*
 * StrategySyncStart -- tell BgBufferSync where to start syncing
 *
 * The result is the buffer index of the best buffer to sync first.
 * BgBufferSync() will proceed circularly around the buffer array from there.
 *
 * In addition, we return the completed-pass count (which is effectively
 * the higher-order bits of nextVictimBuffer) and the count of recent buffer
 * allocs if non-NULL pointers are passed.  The alloc count is reset after
 * being read.
 *
 * XXX This probably either needs to consider all the clocksweep partitions,
 * or look at them one by one. It likely needs to get a partition index
 * (instead of looking at the "current" partition).
 *
 * XXX And it probably needs to sum the numBufferAllocs from all partitions,
 * as the per-partition counts don't quite "match" buffers allocated from
 * that particular partition, we need the global count and average.
 *
 * XXX But we now reset it, and we can't do that if we get partitions one by
 * one (we'd reset on the first one, then what?). Maybe do that in a separate
 * call, before doing the partitions?
 */
int
StrategySyncStart(int partition, uint32 *complete_passes,
				  int *first_buffer, int *num_buffers)
{
	uint32		nextVictimBuffer;
	int			result;
	ClockSweep *sweep = &StrategyControl->sweeps[partition];

	Assert((partition >= 0) && (partition < StrategyControl->num_sweeps_partitions));

	SpinLockAcquire(&sweep->clock_sweep_lock);
	nextVictimBuffer = pg_atomic_read_u32(&sweep->nextVictimBuffer);
	result = nextVictimBuffer % sweep->numBuffers;

	*first_buffer = sweep->firstBuffer;
	*num_buffers = sweep->numBuffers;

	if (complete_passes)
	{
		*complete_passes = sweep->completePasses;

		/*
		 * Additionally add the number of wraparounds that happened before
		 * completePasses could be incremented. C.f. ClockSweepTick().
		 */
		*complete_passes += nextVictimBuffer / sweep->numBuffers;
	}
	SpinLockRelease(&sweep->clock_sweep_lock);

	/* XXX buffer IDs start at 1, we're calculating 0-based indexes */
	Assert(BufferIsValid(1 + sweep->firstBuffer + result));

	return sweep->firstBuffer + result;
}

/*
 * StrategyNotifyBgWriter -- set or clear allocation notification latch
 *
 * If bgwprocno isn't -1, the next invocation of StrategyGetBuffer will
 * set that latch.  Pass -1 to clear the pending notification before it
 * happens.  This feature is used by the bgwriter process to wake itself up
 * from hibernation, and is not meant for anybody else to use.
 */
void
StrategyNotifyBgWriter(int bgwprocno)
{
	/*
	 * We acquire buffer_strategy_lock just to ensure that the store appears
	 * atomic to StrategyGetBuffer.  The bgwriter should call this rather
	 * infrequently, so there's no performance penalty from being safe.
	 */
	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
	StrategyControl->bgwprocno = bgwprocno;
	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
}

/* prints some debug info / stats about freelists at shutdown */
static void
freelist_before_shmem_exit(int code, Datum arg)
{
	for (int node = 0; node < strategy_nnodes; node++)
	{
		BufferStrategyFreelist *freelist = &StrategyControl->freelists[node];
		uint64		remain = 0;
		uint64		actually_free = 0;
		int			cur = freelist->firstFreeBuffer;

		while (cur >= 0)
		{
			uint32		local_buf_state;
			BufferDesc *buf;

			buf = GetBufferDescriptor(cur);

			remain++;

			local_buf_state = LockBufHdr(buf);

			if (!(local_buf_state & BM_TAG_VALID))
				actually_free++;

			UnlockBufHdr(buf, local_buf_state);

			cur = buf->freeNext;
		}
		elog(LOG, "freelist %d, firstF: %d: consumed: %lu, remain: %lu, actually free: %lu",
			 node,
			 freelist->firstFreeBuffer,
			 freelist->consumed,
			 remain, actually_free);
	}
}

/*
 * StrategyShmemSize
 *
 * estimate the size of shared memory used by the freelist-related structures.
 *
 * Note: for somewhat historical reasons, the buffer lookup hashtable size
 * is also determined here.
 */
Size
StrategyShmemSize(void)
{
	Size		size = 0;

	/* FIXME */
#ifdef USE_LIBNUMA
	strategy_ncpus = numa_num_task_cpus();
	strategy_nnodes = numa_num_task_nodes();
#else
	strategy_ncpus = 1;
	strategy_nnodes = 1;
#endif

	/* size of lookup hash table ... see comment in StrategyInitialize */
	size = add_size(size, BufTableShmemSize(NBuffers + NUM_BUFFER_PARTITIONS));

	/* size of the shared replacement strategy control block */
	size = add_size(size, MAXALIGN(offsetof(BufferStrategyControl, freelists)));

	/*
	 * Allocate one frelist per CPU. We might use per-node freelists, but the
	 * assumption is the number of CPUs is less than number of NUMA nodes.
	 *
	 * FIXME This assumes the we have more CPUs than NUMA nodes, which seems
	 * like a safe assumption. But maybe we should calculate how many elements
	 * we actually need, depending on the GUC? Not a huge amount of memory.
	 */
	size = add_size(size, MAXALIGN(mul_size(sizeof(BufferStrategyFreelist),
											strategy_nnodes)));

	/*
	 * Size the clocksweep partitions. At least one partition per NUMA node,
	 * but at least MIN_CLOCKSWEEP_PARTITIONS partitions in total.
	 */
	{
		int		num_per_node = 1;

		while (num_per_node * strategy_nnodes < MIN_CLOCKSWEEP_PARTITIONS)
			num_per_node++;

		size = add_size(size, MAXALIGN(mul_size(sizeof(ClockSweep),
												strategy_nnodes * num_per_node)));
	}

	return size;
}

/*
 * StrategyInitialize -- initialize the buffer cache replacement
 *		strategy.
 *
 * Assumes: All of the buffers are already built into a linked list.
 *		Only called by postmaster and only during initialization.
 */
void
StrategyInitialize(bool init)
{
	bool		found;
	int			buffers_per_node;

	int			num_sweeps_per_node;
	int			num_sweeps;
	char	   *ptr;

	/*
	 * Initialize the shared buffer lookup hashtable.
	 *
	 * Since we can't tolerate running out of lookup table entries, we must be
	 * sure to specify an adequate table size here.  The maximum steady-state
	 * usage is of course NBuffers entries, but BufferAlloc() tries to insert
	 * a new entry before deleting the old.  In principle this could be
	 * happening in each partition concurrently, so we could need as many as
	 * NBuffers + NUM_BUFFER_PARTITIONS entries.
	 */
	InitBufTable(NBuffers + NUM_BUFFER_PARTITIONS);

	/*
	 * XXX repeat the clocksweep sizing from StrategyShmemSize, maybe we should
	 * add a function for this.
	 */
	{
		num_sweeps_per_node = 1;

		while (num_sweeps_per_node * strategy_nnodes < MIN_CLOCKSWEEP_PARTITIONS)
			num_sweeps_per_node++;

		num_sweeps = strategy_nnodes * num_sweeps_per_node;
	}

	/*
	 * Get or create the shared strategy control block
	 */
	StrategyControl = (BufferStrategyControl *)
		ShmemInitStruct("Buffer Strategy Status",
						MAXALIGN(offsetof(BufferStrategyControl, freelists)) +
						MAXALIGN(sizeof(BufferStrategyFreelist) * strategy_nnodes) +
						MAXALIGN(sizeof(ClockSweep) * num_sweeps),
						&found);

	if (!found)
	{
		int32	numBuffers = NBuffers / num_sweeps;

		while (numBuffers * num_sweeps < NBuffers)
			numBuffers++;

		Assert(numBuffers * num_sweeps == NBuffers);

		/*
		 * Only done once, usually in postmaster
		 */
		Assert(init);

		/* register callback to dump some stats on exit */
		before_shmem_exit(freelist_before_shmem_exit, 0);

		SpinLockInit(&StrategyControl->buffer_strategy_lock);

		/* have to point the sweeps array to right after the freelists */
		ptr = (char *) StrategyControl +
				MAXALIGN(offsetof(BufferStrategyControl, freelists)) +
				MAXALIGN(sizeof(BufferStrategyFreelist) * strategy_nnodes);
		StrategyControl->sweeps = (ClockSweep *) ptr;

		/* initialize the partitioned clocksweep */
		StrategyControl->num_sweeps_partitions = num_sweeps;
		StrategyControl->num_sweeps_partitions_groups = strategy_nnodes;
		StrategyControl->num_sweeps_partitions_per_group = num_sweeps_per_node;

		/* Initialize the clock sweep pointers (for all partitions) */
		for (int i = 0; i < num_sweeps; i++)
		{
			SpinLockInit(&StrategyControl->sweeps[i].clock_sweep_lock);

			pg_atomic_init_u32(&StrategyControl->sweeps[i].nextVictimBuffer, 0);

			/*
			 * XXX this is probably not quite right, because if NBuffers is not
			 * a perfect multiple of numBuffers, the last partition will have
			 * numBuffers set too high. The freelists track this by tracking the
			 * remaining number of buffers.
			 */
			StrategyControl->sweeps[i].numBuffers = numBuffers;
			StrategyControl->sweeps[i].firstBuffer = (numBuffers * i);

			/* Clear statistics */
			StrategyControl->sweeps[i].completePasses = 0;
			pg_atomic_init_u32(&StrategyControl->sweeps[i].numBufferAllocs, 0);
		}

		/* No pending notification */
		StrategyControl->bgwprocno = -1;

		/*
		 * Rebuild the freelist - right now all buffers are in one huge list,
		 * we want to rework that into multiple lists. Start by initializing
		 * the strategy to have empty lists.
		 */
		for (int nfreelist = 0; nfreelist < strategy_nnodes; nfreelist++)
		{
			BufferStrategyFreelist *freelist;

			freelist = &StrategyControl->freelists[nfreelist];

			freelist->firstFreeBuffer = FREENEXT_END_OF_LIST;

			SpinLockInit(&freelist->freelist_lock);
		}

		/* buffers per CPU (also used for PID partitioning) */
		buffers_per_node = (NBuffers / strategy_nnodes);

		elog(LOG, "NBuffers: %d, nodes %d, ncpus: %d, divide: %d, remain: %d",
			 NBuffers, strategy_nnodes, strategy_ncpus,
			 buffers_per_node, NBuffers - (strategy_nnodes * buffers_per_node));

		/*
		 * Walk through the buffers, add them to the correct list. Walk from
		 * the end, because we're adding the buffers to the beginning.
		 */
		for (int i = NBuffers - 1; i >= 0; i--)
		{
			BufferDesc *buf = GetBufferDescriptor(i);
			BufferStrategyFreelist *freelist;
			int			belongs_to = 0; /* first freelist by default */

			/*
			 * Split the freelist into partitions, if needed (or just keep the
			 * freelist we already built in BufferManagerShmemInit().
			 */

			/* determine NUMA node for buffer */
			belongs_to = BufferGetNode(i);

			/* add to the right freelist */
			freelist = &StrategyControl->freelists[belongs_to];

			buf->freeNext = freelist->firstFreeBuffer;
			freelist->firstFreeBuffer = i;
		}
	}
	else
		Assert(!init);
}


/* ----------------------------------------------------------------
 *				Backend-private buffer ring management
 * ----------------------------------------------------------------
 */


/*
 * GetAccessStrategy -- create a BufferAccessStrategy object
 *
 * The object is allocated in the current memory context.
 */
BufferAccessStrategy
GetAccessStrategy(BufferAccessStrategyType btype)
{
	int			ring_size_kb;

	/*
	 * Select ring size to use.  See buffer/README for rationales.
	 *
	 * Note: if you change the ring size for BAS_BULKREAD, see also
	 * SYNC_SCAN_REPORT_INTERVAL in access/heap/syncscan.c.
	 */
	switch (btype)
	{
		case BAS_NORMAL:
			/* if someone asks for NORMAL, just give 'em a "default" object */
			return NULL;

		case BAS_BULKREAD:
			{
				int			ring_max_kb;

				/*
				 * The ring always needs to be large enough to allow some
				 * separation in time between providing a buffer to the user
				 * of the strategy and that buffer being reused. Otherwise the
				 * user's pin will prevent reuse of the buffer, even without
				 * concurrent activity.
				 *
				 * We also need to ensure the ring always is large enough for
				 * SYNC_SCAN_REPORT_INTERVAL, as noted above.
				 *
				 * Thus we start out a minimal size and increase the size
				 * further if appropriate.
				 */
				ring_size_kb = 256;

				/*
				 * There's no point in a larger ring if we won't be allowed to
				 * pin sufficiently many buffers.  But we never limit to less
				 * than the minimal size above.
				 */
				ring_max_kb = GetPinLimit() * (BLCKSZ / 1024);
				ring_max_kb = Max(ring_size_kb, ring_max_kb);

				/*
				 * We would like the ring to additionally have space for the
				 * configured degree of IO concurrency. While being read in,
				 * buffers can obviously not yet be reused.
				 *
				 * Each IO can be up to io_combine_limit blocks large, and we
				 * want to start up to effective_io_concurrency IOs.
				 *
				 * Note that effective_io_concurrency may be 0, which disables
				 * AIO.
				 */
				ring_size_kb += (BLCKSZ / 1024) *
					io_combine_limit * effective_io_concurrency;

				if (ring_size_kb > ring_max_kb)
					ring_size_kb = ring_max_kb;
				break;
			}
		case BAS_BULKWRITE:
			ring_size_kb = 16 * 1024;
			break;
		case BAS_VACUUM:
			ring_size_kb = 2048;
			break;

		default:
			elog(ERROR, "unrecognized buffer access strategy: %d",
				 (int) btype);
			return NULL;		/* keep compiler quiet */
	}

	return GetAccessStrategyWithSize(btype, ring_size_kb);
}

/*
 * GetAccessStrategyWithSize -- create a BufferAccessStrategy object with a
 *		number of buffers equivalent to the passed in size.
 *
 * If the given ring size is 0, no BufferAccessStrategy will be created and
 * the function will return NULL.  ring_size_kb must not be negative.
 */
BufferAccessStrategy
GetAccessStrategyWithSize(BufferAccessStrategyType btype, int ring_size_kb)
{
	int			ring_buffers;
	BufferAccessStrategy strategy;

	Assert(ring_size_kb >= 0);

	/* Figure out how many buffers ring_size_kb is */
	ring_buffers = ring_size_kb / (BLCKSZ / 1024);

	/* 0 means unlimited, so no BufferAccessStrategy required */
	if (ring_buffers == 0)
		return NULL;

	/* Cap to 1/8th of shared_buffers */
	ring_buffers = Min(NBuffers / 8, ring_buffers);

	/* NBuffers should never be less than 16, so this shouldn't happen */
	Assert(ring_buffers > 0);

	/* Allocate the object and initialize all elements to zeroes */
	strategy = (BufferAccessStrategy)
		palloc0(offsetof(BufferAccessStrategyData, buffers) +
				ring_buffers * sizeof(Buffer));

	/* Set fields that don't start out zero */
	strategy->btype = btype;
	strategy->nbuffers = ring_buffers;

	return strategy;
}

/*
 * GetAccessStrategyBufferCount -- an accessor for the number of buffers in
 *		the ring
 *
 * Returns 0 on NULL input to match behavior of GetAccessStrategyWithSize()
 * returning NULL with 0 size.
 */
int
GetAccessStrategyBufferCount(BufferAccessStrategy strategy)
{
	if (strategy == NULL)
		return 0;

	return strategy->nbuffers;
}

/*
 * GetAccessStrategyPinLimit -- get cap of number of buffers that should be pinned
 *
 * When pinning extra buffers to look ahead, users of a ring-based strategy are
 * in danger of pinning too much of the ring at once while performing look-ahead.
 * For some strategies, that means "escaping" from the ring, and in others it
 * means forcing dirty data to disk very frequently with associated WAL
 * flushing.  Since external code has no insight into any of that, allow
 * individual strategy types to expose a clamp that should be applied when
 * deciding on a maximum number of buffers to pin at once.
 *
 * Callers should combine this number with other relevant limits and take the
 * minimum.
 */
int
GetAccessStrategyPinLimit(BufferAccessStrategy strategy)
{
	if (strategy == NULL)
		return NBuffers;

	switch (strategy->btype)
	{
		case BAS_BULKREAD:

			/*
			 * Since BAS_BULKREAD uses StrategyRejectBuffer(), dirty buffers
			 * shouldn't be a problem and the caller is free to pin up to the
			 * entire ring at once.
			 */
			return strategy->nbuffers;

		default:

			/*
			 * Tell caller not to pin more than half the buffers in the ring.
			 * This is a trade-off between look ahead distance and deferring
			 * writeback and associated WAL traffic.
			 */
			return strategy->nbuffers / 2;
	}
}

/*
 * FreeAccessStrategy -- release a BufferAccessStrategy object
 *
 * A simple pfree would do at the moment, but we would prefer that callers
 * don't assume that much about the representation of BufferAccessStrategy.
 */
void
FreeAccessStrategy(BufferAccessStrategy strategy)
{
	/* don't crash if called on a "default" strategy */
	if (strategy != NULL)
		pfree(strategy);
}

/*
 * GetBufferFromRing -- returns a buffer from the ring, or NULL if the
 *		ring is empty / not usable.
 *
 * The bufhdr spin lock is held on the returned buffer.
 */
static BufferDesc *
GetBufferFromRing(BufferAccessStrategy strategy, uint32 *buf_state)
{
	BufferDesc *buf;
	Buffer		bufnum;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */


	/* Advance to next ring slot */
	if (++strategy->current >= strategy->nbuffers)
		strategy->current = 0;

	/*
	 * If the slot hasn't been filled yet, tell the caller to allocate a new
	 * buffer with the normal allocation strategy.  He will then fill this
	 * slot by calling AddBufferToRing with the new buffer.
	 */
	bufnum = strategy->buffers[strategy->current];
	if (bufnum == InvalidBuffer)
		return NULL;

	/*
	 * If the buffer is pinned we cannot use it under any circumstances.
	 *
	 * If usage_count is 0 or 1 then the buffer is fair game (we expect 1,
	 * since our own previous usage of the ring element would have left it
	 * there, but it might've been decremented by clock sweep since then). A
	 * higher usage_count indicates someone else has touched the buffer, so we
	 * shouldn't re-use it.
	 */
	buf = GetBufferDescriptor(bufnum - 1);
	local_buf_state = LockBufHdr(buf);
	if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
		&& BUF_STATE_GET_USAGECOUNT(local_buf_state) <= 1)
	{
		*buf_state = local_buf_state;
		return buf;
	}
	UnlockBufHdr(buf, local_buf_state);

	/*
	 * Tell caller to allocate a new buffer with the normal allocation
	 * strategy.  He'll then replace this ring element via AddBufferToRing.
	 */
	return NULL;
}

/*
 * AddBufferToRing -- add a buffer to the buffer ring
 *
 * Caller must hold the buffer header spinlock on the buffer.  Since this
 * is called with the spinlock held, it had better be quite cheap.
 */
static void
AddBufferToRing(BufferAccessStrategy strategy, BufferDesc *buf)
{
	strategy->buffers[strategy->current] = BufferDescriptorGetBuffer(buf);
}

/*
 * Utility function returning the IOContext of a given BufferAccessStrategy's
 * strategy ring.
 */
IOContext
IOContextForStrategy(BufferAccessStrategy strategy)
{
	if (!strategy)
		return IOCONTEXT_NORMAL;

	switch (strategy->btype)
	{
		case BAS_NORMAL:

			/*
			 * Currently, GetAccessStrategy() returns NULL for
			 * BufferAccessStrategyType BAS_NORMAL, so this case is
			 * unreachable.
			 */
			pg_unreachable();
			return IOCONTEXT_NORMAL;
		case BAS_BULKREAD:
			return IOCONTEXT_BULKREAD;
		case BAS_BULKWRITE:
			return IOCONTEXT_BULKWRITE;
		case BAS_VACUUM:
			return IOCONTEXT_VACUUM;
	}

	elog(ERROR, "unrecognized BufferAccessStrategyType: %d", strategy->btype);
	pg_unreachable();
}

/*
 * StrategyRejectBuffer -- consider rejecting a dirty buffer
 *
 * When a nondefault strategy is used, the buffer manager calls this function
 * when it turns out that the buffer selected by StrategyGetBuffer needs to
 * be written out and doing so would require flushing WAL too.  This gives us
 * a chance to choose a different victim.
 *
 * Returns true if buffer manager should ask for a new victim, and false
 * if this buffer should be written and re-used.
 */
bool
StrategyRejectBuffer(BufferAccessStrategy strategy, BufferDesc *buf, bool from_ring)
{
	/* We only do this in bulkread mode */
	if (strategy->btype != BAS_BULKREAD)
		return false;

	/* Don't muck with behavior of normal buffer-replacement strategy */
	if (!from_ring ||
		strategy->buffers[strategy->current] != BufferDescriptorGetBuffer(buf))
		return false;

	/*
	 * Remove the dirty buffer from the ring; necessary to prevent infinite
	 * loop if all ring members are dirty.
	 */
	strategy->buffers[strategy->current] = InvalidBuffer;

	return true;
}
