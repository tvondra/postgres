/*-------------------------------------------------------------------------
 *
 * freelist.c
 *	  routines for managing the buffer pool's replacement strategy.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/freelist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_LIBNUMA
#include <sched.h>
#endif

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
 * XXX needed for make ClockSweep fixed-size, should be tied to the number
 * of buffer partitions (bufmgr.c already has MAX_CLOCKSWEEP_PARTITIONS, so
 * at least set it to the same value).
 */
#define MAX_BUFFER_PARTITIONS		32

/*
 * Coefficient used to combine the old and new balance coefficients, using
 * weighted average. The higher the value, the more the old value affects the
 * result.
 *
 * XXX Doesn't this invalidate the interpretation as a probability to allocate
 * from a given partition? Does it still sum to 100%?
 */
#define CLOCKSWEEP_HISTORY_COEFF	0.5

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
	 * clock-sweep hand: index of next buffer to consider grabbing. Note that
	 * this isn't a concrete buffer - we only ever increase the value. So, to
	 * get an actual buffer, it needs to be used modulo NBuffers.
	 *
	 * XXX This is relative to firstBuffer, so needs to be offset properly.
	 *
	 * XXX firstBuffer + (nextVictimBuffer % numBuffers)
	 */
	pg_atomic_uint32 nextVictimBuffer;

	/*
	 * Statistics.  These counters should be wide enough that they can't
	 * overflow during a single bgwriter cycle.
	 */
	uint32		completePasses; /* Complete cycles of the clock-sweep */
	pg_atomic_uint32 numBufferAllocs;	/* Buffers allocated since last reset */

	/*
	 * Buffers that should have been allocated in this partition (but might
	 * have been redirected to keep allocations balanced).
	 */
	pg_atomic_uint32 numRequestedAllocs;

	/* running total of allocs */
	pg_atomic_uint64 numTotalAllocs;
	pg_atomic_uint64 numTotalRequestedAllocs;

	/*
	 * Weights to balance buffer allocations for all the partitions. Each
	 * partition gets a vector of weights 0-100, determining what fraction
	 * of buffers to allocate from that particular. So [75, 15, 5, 5] would
	 * mean 75% allocations should go from partition 0, 15% from partition
	 * 1, and 5% from partitions 2&3. Each partition gets a different vector
	 * of weights.
	 *
	 * XXX Allocate a fixed-length array, to simplify working with array of
	 * the structs, etc.
	 */
	uint8		balance[MAX_BUFFER_PARTITIONS];
} ClockSweep;

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
	 */
	int			bgwprocno;
	// the _attribute_ does not work on Windows, it seems
	//int			__attribute__((aligned(64))) bgwprocno;

	/* info about freelist partitioning */
	int			num_nodes;		/* effectively number of NUMA nodes */
	int			num_partitions;
	int			num_partitions_per_node;

	/* clocksweep partitions */
	ClockSweep	sweeps[FLEXIBLE_ARRAY_MEMBER];
} BufferStrategyControl;

/* Pointers to shared state */
static BufferStrategyControl *StrategyControl = NULL;

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
									 uint64 *buf_state);
static void AddBufferToRing(BufferAccessStrategy strategy,
							BufferDesc *buf);
static ClockSweep *ChooseClockSweep(bool balance);

/*
 * clocksweep allocation balancing
 *
 * To balance allocations from clocksweep partitions, each partition gets
 * a set of "weights" determining the fraction of allocations to redirect
 * to other partitions.
 *
 * We could do that based on a random number generator, but that seems too
 * expensive. So instead we simply treat the probabilities as a budget, i.e.
 * a number of allocations to serve from that partition, before moving to
 * the next partition (in a round-robin manner).
 *
 * This is very simple/cheap, and over many allocations it has the same
 * effect. For periods of low activity it may diverge, but that does not
 * matter much (we care about high-activity periods much more).
 *
 * We intentionally keep the "budget" fairly low, with the sum for a given
 * partition 100. That means we get to the same partition after only 100
 * allocations, keeping it more balances. It wouldn't be hard to make the
 * budgets higher (say, to match the expected number of allocations, i.e.
 * about the average number of allocations from the past interval).
 */
static int clocksweep_partition_optimal = -1;
static int clocksweep_partition = -1;
static int clocksweep_count = 0;

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
	ClockSweep *sweep = ChooseClockSweep(true);

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
 * calculate_partition_index
 *		calculate the buffer / clock-sweep partition to use
 *
 * With libnuma, use the NUMA node and CPU to pick the partition. Otherwise
 * use just PID instead of CPU (we assume everything is a single NUMA node).
 */
static int
calculate_partition_index(void)
{
	int		cpu,
			node,
			index;

	/*
	 * The buffers are partitioned, so determine the CPU/NUMA node, and pick a
	 * partition based on that.
	 *
	 * Without NUMA assume everything is a single NUMA node, and we pick the
	 * partition based on PID (we may not have sched_getcpu).
	 */
#ifdef USE_LIBNUMA
	cpu = sched_getcpu();

	if (cpu < 0)
		elog(ERROR, "sched_getcpu failed: %m");

	node = numa_node_of_cpu(cpu);
#else
	cpu = MyProcPid;
	node = 0;
#endif

	Assert(StrategyControl->num_partitions ==
		   (StrategyControl->num_nodes * StrategyControl->num_partitions_per_node));

	/*
	 * XXX We should't get nodes that we haven't considered while building the
	 * partitions. Maybe if we allow this (e.g. due to support adjusting the
	 * NUMA stuff at runtime), we should just do our best to minimize the
	 * conflicts somehow. But it'll make the mapping harder, so for now we
	 * ignore it.
	 */
	if (node > StrategyControl->num_nodes)
		elog(ERROR, "node out of range: %d > %u", cpu, StrategyControl->num_nodes);

	/*
	 * Find the partition. If we have a single partition per node, we can
	 * calculate the index directly from node. Otherwise we need to do two
	 * steps, using node and then cpu.
	 */
	if (StrategyControl->num_partitions_per_node == 1)
	{
		/* fast-path */
		index = (node % StrategyControl->num_partitions);
	}
	else
	{
		int			index_group,
					index_part;

		/* two steps - calculate group from node, partition from cpu */
		index_group = (node % StrategyControl->num_nodes);
		index_part = (cpu % StrategyControl->num_partitions_per_node);

		index = (index_group * StrategyControl->num_partitions_per_node)
			+ index_part;
	}

	return index;
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
 * XXX Maybe we should do both the total and "per group" counts a power of
 * two? That'd allow using shifts instead of divisions in the calculation,
 * and that's cheaper. But how would that deal with odd number of nodes?
 */
static ClockSweep *
ChooseClockSweep(bool balance)
{
	/* What's the "optimal" partition? */
	int		index = calculate_partition_index();
	ClockSweep *sweep = &StrategyControl->sweeps[index];

	/*
	 * Did we migrate to a different core / NUMA node, affecting the
	 * clocksweep partition we should use? Switch to that partition.
	 */
	if (clocksweep_partition_optimal != index)
	{
		clocksweep_partition_optimal = index;
		clocksweep_partition = index;
		clocksweep_count = sweep->balance[index];
	}

	/* we should have a valid partition */
	Assert(clocksweep_partition_optimal != -1);
	Assert(clocksweep_partition != -1);

	/*
	 * If rebalancing is enabled, use the weights to redirect the allocations
	 * to match the desired distribution. We do that by using the partitions
	 * in a round-robin way, after allocating the "weight" of allocations
	 * from each partitions.
	 */
	if (balance)
	{
		/*
		 * Ran out of allocations from the current partition? Move to the
		 * next partition with non-zero weight, and use the weight as a
		 * budget for allocations.
		 */
		while (clocksweep_count == 0)
		{
			clocksweep_partition
				= (clocksweep_partition + 1) % StrategyControl->num_partitions;

			Assert((clocksweep_partition >= 0) &&
				   (clocksweep_partition < StrategyControl->num_partitions));

			clocksweep_count = sweep->balance[clocksweep_partition];
		}

		/* account for the allocation - take it from the budget */
		--clocksweep_count;

		/* account for the alloc in the "optimal" (original) partition */
		pg_atomic_fetch_add_u32(&sweep->numRequestedAllocs, 1);
	}

	return &StrategyControl->sweeps[clocksweep_partition];
}

/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	GetVictimBuffer(). The only hard requirement GetVictimBuffer() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	strategy is a BufferAccessStrategy object, or NULL for default strategy.
 *
 *	It is the callers responsibility to ensure the buffer ownership can be
 *	tracked via TrackNewBufferPin().
 *
 *	The buffer is pinned and marked as owned, using TrackNewBufferPin(),
 *	before returning.
 */
BufferDesc *
StrategyGetBuffer(BufferAccessStrategy strategy, uint64 *buf_state, bool *from_ring)
{
	BufferDesc *buf;
	int			bgwprocno;
	int			trycounter;

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
		SetLatch(&GetPGProcByNumber(bgwprocno)->procLatch);
	}

	/*
	 * We count buffer allocation requests so that the bgwriter can estimate
	 * the rate of buffer consumption.  Note that buffers recycled by a
	 * strategy object are intentionally not counted here.
	 */
	pg_atomic_fetch_add_u32(&ChooseClockSweep(false)->numBufferAllocs, 1);

	/*
	 * Use the "clock sweep" algorithm to find a free buffer
	 *
	 * XXX Note that ClockSweepTick() is NUMA-aware, i.e. it only looks at
	 * buffers from a single partition, aligned with the NUMA node. That means
	 * it only accesses buffers from the same NUMA node.
	 *
	 * XXX That also means each process "sweeps" only a fraction of buffers,
	 * even if the other buffers are better candidates for eviction. Maybe
	 * there should be some logic to "steal" buffers from other freelists or
	 * other nodes?
	 *
	 * XXX Would that also mean we'd have multiple bgwriters, one for each
	 * node, or would one bgwriter handle all of that?
	 *
	 * XXX This only searches a single partition, which can result in "no
	 * unpinned buffers available" even if there are buffers in other
	 * partitions. Should be fixed by falling back to other partitions if
	 * needed.
	 *
	 * XXX Also, the trycounter should not be set to NBuffers, but to buffer
	 * count for that one partition. In fact, this should not call ClockSweepTick
	 * for every iteration. The call is likely quite expensive (does a lot
	 * of stuff), and also may return a different partition on each call.
	 * We should just do it once, and then do the for(;;) loop. And then
	 * maybe advance to the next partition, until we scan through all of them.
	 */
	trycounter = NBuffers;
	for (;;)
	{
		uint64		old_buf_state;
		uint64		local_buf_state;

		buf = GetBufferDescriptor(ClockSweepTick());

		/*
		 * Check whether the buffer can be used and pin it if so. Do this
		 * using a CAS loop, to avoid having to lock the buffer header.
		 */
		old_buf_state = pg_atomic_read_u64(&buf->state);
		for (;;)
		{
			local_buf_state = old_buf_state;

			/*
			 * If the buffer is pinned or has a nonzero usage_count, we cannot
			 * use it; decrement the usage_count (unless pinned) and keep
			 * scanning.
			 */

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
			{
				if (--trycounter == 0)
				{
					/*
					 * We've scanned all the buffers without making any state
					 * changes, so all the buffers are pinned (or were when we
					 * looked at them). We could hope that someone will free
					 * one eventually, but it's probably better to fail than
					 * to risk getting stuck in an infinite loop.
					 */
					elog(ERROR, "no unpinned buffers available");
				}
				break;
			}

			/* See equivalent code in PinBuffer() */
			if (unlikely(local_buf_state & BM_LOCKED))
			{
				old_buf_state = WaitBufHdrUnlocked(buf);
				continue;
			}

			if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
			{
				local_buf_state -= BUF_USAGECOUNT_ONE;

				if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
												   local_buf_state))
				{
					trycounter = NBuffers;
					break;
				}
			}
			else
			{
				/* pin the buffer if the CAS succeeds */
				local_buf_state += BUF_REFCOUNT_ONE;

				if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
												   local_buf_state))
				{
					/* Found a usable buffer */
					if (strategy != NULL)
						AddBufferToRing(strategy, buf);
					*buf_state = local_buf_state;

					TrackNewBufferPin(BufferDescriptorGetBuffer(buf));

					return buf;
				}
			}
		}
	}
}

/*
 * StrategySyncBalance
 *		update partition weights, to balance the buffer allocations
 *
 * We want to give preference to allocating buffers on the same NUMA node,
 * but that might lead to imbalance - a single process would only use a
 * fraction of shared buffers. We don't want that, we want to utilize the
 * whole shared buffers. The number of allocations in each partition may
 * also change over time, so we need to adapt to that.
 *
 * To allow this "adaptive balancing", each partition has a set of weights,
 * determining what fraction of allocations to direct to other partitions.
 * For simplicity the coefficients are integers 0-100, expressing the
 * percentage of allocations redirected to that partition.
 *
 * Consider for example weights [50, 25, 25, 0] for one of 4 partitions.
 * This means 50% of allocations will be redirected to partition 0, 25%
 * to partitions 1 and 2, and no allocations will go to partition 3.
 *
 * To calculate these weights, assume we know the number of allocations
 * requested for each partition in the past interval. We can use this to
 * calculate weights for the following interval, aiming to allocate the
 * same (fair share) number of buffers from each partition.
 *
 * Note: This is based on number of allocations "originating" in a given
 * partition. If an allocation is requested in a partition A, it's counted
 * as allocation for A, even if it gets redirected to some other partition.
 * The patch addes a new counter to track this.
 *
 * The main observation is that partitions get divided into two groups,
 * depending on whether the number allocations is higher or lower than the
 * target average. But the "total delta" for these two groups is the
 * same, i.e. sum(abs(allocs - avg_allocs)) is the same. Therefore, the
 * task is to "distribute" the excess allocations between the partitions
 * with not enough allocations.
 *
 * Partitions with (nallocs < avg_nallocs) don't redirect any allocations.
 *
 * Partitions with (nallocs > avg_nallocs) redirect the extra allocations,
 * with each target allocation getting a proportional part (with respect
 * to the total delta).
 *
 * XXX In principle we might do without the new "requestedAllocs" counter,
 * but we'd need to solve the matrix equation Ax=b, with [A,b] known
 * (weights and allocs), and calculate x (requested allocs). But it's not
 * quite clear this'd always have a solution.
 */
void
StrategySyncBalance(void)
{
	/* snapshot of allocs for partitions */
	uint32	allocs[MAX_BUFFER_PARTITIONS];

	uint32	total_allocs = 0,	/* total number of allocations */
			avg_allocs,			/* average allocations (per partition) */
			delta_allocs = 0;	/* sum of allocs above average */

	/*
	 * Collect the number of allocations requested in the past interval.
	 * While at it, reset the counter to start the new interval.
	 *
	 * We lock the partitions one by one, so this is not exactly consistent
	 * snapshot of the counts, and the resets happen before we update the
	 * weights too. But we're only looking for heuristics anyway, so this
	 * should be good enough.
	 *
	 * A similar issue applies to the counter reset - we haven't updated
	 * the weights yet. Should be fine, we'll simply consider this in the
	 * next balancing cycle.
	 *
	 * XXX Does this need to worry about the completePasses too?
	 */
	for (int i = 0; i < StrategyControl->num_partitions; i++)
	{
		ClockSweep *sweep = &StrategyControl->sweeps[i];

		/* no need for a spinlock */
		allocs[i] = pg_atomic_exchange_u32(&sweep->numRequestedAllocs, 0);

		/* add the allocs to running total */
		pg_atomic_fetch_add_u64(&sweep->numTotalRequestedAllocs, allocs[i]);

		total_allocs += allocs[i];
	}

	/*
	 * Calculate the "fair share" of allocations per partition.
	 *
	 * XXX The last partition could be smaller, in which case it should be
	 * expected to handle fewer allocations. So this should be a weighted
	 * average. But for now a simple average is good enough.
	 */
	avg_allocs = (total_allocs / StrategyControl->num_partitions);

	/*
	 * Calculate the "delta" from balanced state, i.e. how many allocations
	 * we'd need to redistribute.
	 */
	for (int i = 0; i < StrategyControl->num_partitions; i++)
	{
		if (allocs[i] > avg_allocs)
			delta_allocs += (allocs[i] - avg_allocs);
	}

	/*
	 * Skip the rebalancing when there's not enough activity. In this case
	 * we just keep the current weights.
	 *
	 * XXX The threshold of 100 allocation is pretty arbitrary.
	 *
	 * XXX Maybe a better strategy would be to slowly return to the default
	 * weights, with each partition allocation only from itself?
	 *
	 * XXX Maybe we shouldn't even reset the counters in this case? But it
	 * should not matter, if the activity is low.
	 */
	if (avg_allocs < 100)
	{
		elog(DEBUG1, "rebalance skipped: not enough allocations (allocs: %u)",
			 avg_allocs);
		return;
	}

	/*
	 * Likewise, skip rebalancing if the misbalance is not significant. We
	 * consider it acceptable if the amount of allocations we'd need to
	 * redistribute is less than 10% of the average.
	 *
	 * XXX Again, these threshold are rather arbitrary.
	 */
	if (delta_allocs < (avg_allocs * 0.1))
	{
		elog(DEBUG1, "rebalance skipped: delta within limit (delta: %u, threshold: %u)",
			 delta_allocs, (uint32) (avg_allocs * 0.1));
		return;
	}

	/*
	 * Got to do the rebalancing. Go through the partitions, and for each
	 * partition decide if it gets to redirect or receive allocations.
	 *
	 * If a partition has fewer than average allocations, it won't redirect
	 * any allocations to other partitions. So it only has a single non-zero
	 * weight, and that's for itself.
	 *
	 * If a parttion has more than average allocations, it won't receive
	 * any redirected allocations. Instead, the excess allocations are
	 * redirected to the other partitions.
	 *
	 * The redistribution is "proportional" - if the excess allocations of
	 * a partition represent 10% of the "delta", then each partition that
	 * needs more allocations will get 10% of the gap from this one.
	 *
	 * XXX We should add hysteresis, to "dampen" the changes, and make
	 * sure it does not oscillate too much.
	 *
	 * XXX Ideally, the alternative partitions to use first would be the
	 * other partitions for the same node (if any).
	 */
	for (int i = 0; i < StrategyControl->num_partitions; i++)
	{
		ClockSweep *sweep = &StrategyControl->sweeps[i];
		uint8		balance[MAX_BUFFER_PARTITIONS];

		/* lock, we're going to modify the balance weights */
		SpinLockAcquire(&sweep->clock_sweep_lock);

		/* reset the weights to start from scratch */
		memset(balance, 0, sizeof(uint8) * MAX_BUFFER_PARTITIONS);

		/* does this partition has fewer or more than avg_allocs? */
		if (allocs[i] < avg_allocs)
		{
			/* fewer - don't redirect any allocations elsewhere */
			balance[i] = 100;
		}
		else
		{
			/*
			 * more - redistribute the excess allocations
			 *
			 * Each "target" partition (with less than avg_allocs) should get
			 * a fraction proportional to (excess/delta) from this one.
			 */

			/* fraction of the "total" delta */
			double	delta_frac = (allocs[i] - avg_allocs) * 1.0 / delta_allocs;

			/* keep just enough allocations to meet the target */
			balance[i] = (100.0 * avg_allocs / allocs[i]);

			/* redirect the extra allocations */
			for (int j = 0; j < StrategyControl->num_partitions; j++)
			{
				/* How many allocations to receive from i-th partition? */
				uint32	receive_allocs = delta_frac * (avg_allocs - allocs[j]);

				/* ignore partitions that don't need additional allocations */
				if (allocs[j] > avg_allocs)
					continue;

				/* fraction to redirect */
				balance[j] = (100.0 * receive_allocs / allocs[i]) + 0.5;
			}
		}

		/* combine the old and new weights (hysteresis) */
		for (int j = 0; j < MAX_BUFFER_PARTITIONS; j++)
		{
			sweep->balance[j]
				= CLOCKSWEEP_HISTORY_COEFF * sweep->balance[j] +
				  (1.0 - CLOCKSWEEP_HISTORY_COEFF) * balance[j];
		}

		SpinLockRelease(&sweep->clock_sweep_lock);
	}
}

/*
 * StrategySyncPrepare -- prepare for sync of all partitions
 *
 * Determine the number of clocksweep partitions, and calculate the recent
 * buffers allocs (as a sum of all the partitions). This allows BgBufferSync
 * to calculate average number of allocations per partition for the next
 * sync cycle.
 *
 * In addition it returns the count of recent buffer allocs, which is a total
 * summed from all partitions. The alloc counts are reset after being read,
 * as the partitions are walked.
 */
void
StrategySyncPrepare(int *num_parts, uint32 *num_buf_alloc)
{
	*num_buf_alloc = 0;
	*num_parts = StrategyControl->num_partitions;

	/*
	 * We lock the partitions one by one, so not exacly in sync, but that
	 * should be fine. We're only looking for heuristics anyway.
	 */
	for (int i = 0; i < StrategyControl->num_partitions; i++)
	{
		ClockSweep *sweep = &StrategyControl->sweeps[i];

		/* XXX we don't need the spinlock to read atomics, no? */
		SpinLockAcquire(&sweep->clock_sweep_lock);
		if (num_buf_alloc)
		{
			uint32	allocs = pg_atomic_exchange_u32(&sweep->numBufferAllocs, 0);

			/* include the count in the running total */
			pg_atomic_fetch_add_u64(&sweep->numTotalAllocs, allocs);

			*num_buf_alloc += allocs;
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
 * the higher-order bits of nextVictimBuffer).
 *
 * This only considers a single clocksweep partition, as BgBufferSync looks
 * at them one by one.
 */
int
StrategySyncStart(int partition, uint32 *complete_passes,
				  int *first_buffer, int *num_buffers)
{
	uint32		nextVictimBuffer;
	int			result;
	ClockSweep *sweep = &StrategyControl->sweeps[partition];

	Assert((partition >= 0) && (partition < StrategyControl->num_partitions));

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
	int			num_partitions;

	BufferPartitionParams(&num_partitions, NULL);

	/* size of lookup hash table ... see comment in StrategyInitialize */
	size = add_size(size, BufTableShmemSize(NBuffers + NUM_BUFFER_PARTITIONS));

	/* size of the shared replacement strategy control block */
	size = add_size(size, MAXALIGN(sizeof(BufferStrategyControl)));

	/* size of clocksweep partitions (at least one per NUMA node) */
	size = add_size(size, MAXALIGN(mul_size(sizeof(ClockSweep),
											num_partitions)));

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

	int			num_nodes;
	int			num_partitions;
	int			num_partitions_per_node;

	num_partitions = BufferPartitionCount();
	num_nodes = BufferPartitionNodes();

	/* always a multiple of NUMA nodes */
	Assert(num_partitions % num_nodes == 0);

	num_partitions_per_node = (num_partitions / num_nodes);

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
	 * Get or create the shared strategy control block
	 */
	StrategyControl = (BufferStrategyControl *)
		ShmemInitStruct("Buffer Strategy Status",
						MAXALIGN(offsetof(BufferStrategyControl, sweeps)) +
						MAXALIGN(sizeof(ClockSweep) * num_partitions),
						&found);

	if (!found)
	{
		/*
		 * Only done once, usually in postmaster
		 */
		Assert(init);

		SpinLockInit(&StrategyControl->buffer_strategy_lock);

		/* Initialize the clock sweep pointers (for all partitions) */
		for (int i = 0; i < num_partitions; i++)
		{
			int			node,
						num_buffers,
						first_buffer,
						last_buffer;

			SpinLockInit(&StrategyControl->sweeps[i].clock_sweep_lock);

			pg_atomic_init_u32(&StrategyControl->sweeps[i].nextVictimBuffer, 0);

			/* get info about the buffer partition */
			BufferPartitionGet(i, &node, &num_buffers,
							   &first_buffer, &last_buffer);

			/*
			 * FIXME This may not quite right, because if NBuffers is not a
			 * perfect multiple of numBuffers, the last partition will have
			 * numBuffers set too high. buf_init handles this by tracking the
			 * remaining number of buffers, and not overflowing.
			 */
			StrategyControl->sweeps[i].numBuffers = num_buffers;
			StrategyControl->sweeps[i].firstBuffer = first_buffer;

			/* Clear statistics */
			StrategyControl->sweeps[i].completePasses = 0;
			pg_atomic_init_u32(&StrategyControl->sweeps[i].numBufferAllocs, 0);
			pg_atomic_init_u32(&StrategyControl->sweeps[i].numRequestedAllocs, 0);
			pg_atomic_init_u64(&StrategyControl->sweeps[i].numTotalAllocs, 0);
			pg_atomic_init_u64(&StrategyControl->sweeps[i].numTotalRequestedAllocs, 0);

			/*
			 * Initialize the weights - start by allocating 100% buffers from
			 * the current node / partition.
			 */
			for (int j = 0; j < MAX_BUFFER_PARTITIONS; j++)
			{
				if (i == j)
					StrategyControl->sweeps[i].balance[i] = 100;
				else
					StrategyControl->sweeps[i].balance[j] = 0;
			}
		}

		/* No pending notification */
		StrategyControl->bgwprocno = -1;

		/* initialize the partitioned clocksweep */
		StrategyControl->num_partitions = num_partitions;
		StrategyControl->num_nodes = num_nodes;
		StrategyControl->num_partitions_per_node = num_partitions_per_node;
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
 * The buffer is pinned and marked as owned, using TrackNewBufferPin(), before
 * returning.
 */
static BufferDesc *
GetBufferFromRing(BufferAccessStrategy strategy, uint64 *buf_state)
{
	BufferDesc *buf;
	Buffer		bufnum;
	uint64		old_buf_state;
	uint64		local_buf_state;	/* to avoid repeated (de-)referencing */


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

	buf = GetBufferDescriptor(bufnum - 1);

	/*
	 * Check whether the buffer can be used and pin it if so. Do this using a
	 * CAS loop, to avoid having to lock the buffer header.
	 */
	old_buf_state = pg_atomic_read_u64(&buf->state);
	for (;;)
	{
		local_buf_state = old_buf_state;

		/*
		 * If the buffer is pinned we cannot use it under any circumstances.
		 *
		 * If usage_count is 0 or 1 then the buffer is fair game (we expect 1,
		 * since our own previous usage of the ring element would have left it
		 * there, but it might've been decremented by clock-sweep since then).
		 * A higher usage_count indicates someone else has touched the buffer,
		 * so we shouldn't re-use it.
		 */
		if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0
			|| BUF_STATE_GET_USAGECOUNT(local_buf_state) > 1)
			break;

		/* See equivalent code in PinBuffer() */
		if (unlikely(local_buf_state & BM_LOCKED))
		{
			old_buf_state = WaitBufHdrUnlocked(buf);
			continue;
		}

		/* pin the buffer if the CAS succeeds */
		local_buf_state += BUF_REFCOUNT_ONE;

		if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
										   local_buf_state))
		{
			*buf_state = local_buf_state;

			TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
			return buf;
		}
	}

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

void
ClockSweepPartitionGetInfo(int idx,
						 uint32 *complete_passes, uint32 *next_victim_buffer,
						 uint64 *buffer_total_allocs, uint32 *buffer_allocs,
						 uint64 *buffer_total_req_allocs, uint32 *buffer_req_allocs,
						 int **weights)
{
	ClockSweep *sweep = &StrategyControl->sweeps[idx];

	Assert((idx >= 0) && (idx < StrategyControl->num_partitions));

	/* get the clocksweep stats */
	*complete_passes = sweep->completePasses;

	/* calculate the actual buffer ID */
	*next_victim_buffer = pg_atomic_read_u32(&sweep->nextVictimBuffer);
	*next_victim_buffer = sweep->firstBuffer + (*next_victim_buffer % sweep->numBuffers);

	*buffer_total_allocs = pg_atomic_read_u64(&sweep->numTotalAllocs);
	*buffer_allocs = pg_atomic_read_u32(&sweep->numBufferAllocs);

	*buffer_total_req_allocs = pg_atomic_read_u64(&sweep->numTotalRequestedAllocs);
	*buffer_req_allocs = pg_atomic_read_u32(&sweep->numRequestedAllocs);

	/* return the weights in a newly allocated array */
	*weights = palloc_array(int, StrategyControl->num_partitions);
	for (int i = 0; i < StrategyControl->num_partitions; i++)
	{
		(*weights)[i] = (int) sweep->balance[i];
	}
}
