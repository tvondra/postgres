/*-------------------------------------------------------------------------
 *
 * buf_init.c
 *	  buffer manager initialization routines
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_init.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_LIBNUMA
#include <numa.h>
#include <numaif.h>
#endif

#include "port/pg_numa.h"
#include "storage/aio.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/proclist.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/guc_hooks.h"
#include "utils/varlena.h"

BufferDescPadded *BufferDescriptors;
char	   *BufferBlocks;
ConditionVariableMinimallyPadded *BufferIOCVArray;
WritebackContext BackendWritebackContext;
CkptSortItem *CkptBufferIds;

/*
 * Minimum number of buffer partitions, no matter the number of NUMA nodes.
 */
#define MIN_BUFFER_PARTITIONS	4

/* Array of structs with information about buffer ranges */
BufferPartitions *BufferPartitionsArray = NULL;

static void buffer_partitions_prepare(void);
static void buffer_partitions_init(void);

/* number of NUMA nodes (as returned by numa_num_configured_nodes) */
static int	numa_nodes = -1;	/* number of nodes when sizing */
static Size numa_page_size = 0; /* page used to size partitions */
static bool numa_can_partition = false; /* can map to NUMA nodes? */
static int	numa_buffers_per_node = -1; /* buffers per node */
static int	numa_partitions = 0;	/* total (multiple of nodes) */

/*
 * Data Structures:
 *		buffers live in a freelist and a lookup data structure.
 *
 *
 * Buffer Lookup:
 *		Two important notes.  First, the buffer has to be
 *		available for lookup BEFORE an IO begins.  Otherwise
 *		a second process trying to read the buffer will
 *		allocate its own copy and the buffer pool will
 *		become inconsistent.
 *
 * Buffer Replacement:
 *		see freelist.c.  A buffer cannot be replaced while in
 *		use either by data manager or during IO.
 *
 *
 * Synchronization/Locking:
 *
 * IO_IN_PROGRESS -- this is a flag in the buffer descriptor.
 *		It must be set when an IO is initiated and cleared at
 *		the end of the IO.  It is there to make sure that one
 *		process doesn't start to use a buffer while another is
 *		faulting it in.  see WaitIO and related routines.
 *
 * refcount --	Counts the number of processes holding pins on a buffer.
 *		A buffer is pinned during IO and immediately after a BufferAlloc().
 *		Pins must be released before end of transaction.  For efficiency the
 *		shared refcount isn't increased if an individual backend pins a buffer
 *		multiple times. Check the PrivateRefCount infrastructure in bufmgr.c.
 */


/*
 * Initialize shared buffer pool
 *
 * This is called once during shared-memory initialization (either in the
 * postmaster, or in a standalone backend).
 */
void
BufferManagerShmemInit(void)
{
	bool		foundBufs,
				foundDescs,
				foundIOCV,
				foundBufCkpt,
				foundParts;
	Size		buffer_align;

	/*
	 * Determine the memory page size used to partition shared buffers over
	 * the available NUMA nodes.
	 *
	 * XXX We have to call prepare again, because with EXEC_BACKEND we may not
	 * see the values already calculated in BufferManagerShmemSize().
	 *
	 * XXX We need to be careful to get the same value when calculating the
	 * and then later when initializing the structs after allocation, or to not
	 * depend on that value too much. Before the allocation we don't know if we
	 * get huge pages, so we just have to assume we do.
	 */
	buffer_partitions_prepare();

	/*
	 * With NUMA we need to ensure the buffers are properly aligned not just
	 * to PG_IO_ALIGN_SIZE, but also to memory page size. NUMA works on page
	 * granularity, and we don't want a buffer to get split to multiple nodes
	 * (when spanning multiple memory pages).
	 *
	 * We also don't want to interfere with other parts of shared memory,
	 * which could easily happen with huge pages (e.g. with data stored before
	 * buffers).
	 *
	 * We do this by aligning to the larger of the two values (we know both
	 * are power-of-two values, so the larger value is automatically a
	 * multiple of the lesser one).
	 *
	 * XXX Maybe there's a way to use less alignment?
	 *
	 * XXX Maybe with (numa_page_size > PG_IO_ALIGN_SIZE), we don't need to
	 * align to numa_page_size? Especially for very large huge pages (e.g. 1GB)
	 * that doesn't seem quite worth it. Maybe we should simply align to
	 * BLCKSZ, so that buffers don't get split? Still, we might interfere with
	 * other stuff stored in shared memory that we want to allocate on a
	 * particular NUMA node (e.g. ProcArray).
	 *
	 * XXX Maybe with "too large" huge pages we should just not do this, or
	 * maybe do this only for sufficiently large areas (e.g. shared buffers,
	 * but not ProcArray).
	 */
	buffer_align = Max(numa_page_size, PG_IO_ALIGN_SIZE);

	/* one page is a multiple of the other */
	Assert(((numa_page_size % PG_IO_ALIGN_SIZE) == 0) ||
		   ((PG_IO_ALIGN_SIZE % numa_page_size) == 0));

	/* allocate the partition registry first */
	BufferPartitionsArray = (BufferPartitions *)
		ShmemInitStruct("Buffer Partitions",
						offsetof(BufferPartitions, partitions) +
						mul_size(sizeof(BufferPartition), numa_partitions),
						&foundParts);

	/*
	 * Align descriptors to a cacheline boundary, and memory page.
	 *
	 * We want to distribute both to NUMA nodes, so that each buffer and it's
	 * descriptor are on the same NUMA node. So we align both the same way.
	 *
	 * XXX The memory page is always larger than cacheline, so the cacheline
	 * reference is a bit unnecessary.
	 *
	 * XXX In principle we only need to do this with NUMA, otherwise we could
	 * still align just to cacheline, as before.
	 */
	BufferDescriptors = (BufferDescPadded *)
		TYPEALIGN(buffer_align,
				  ShmemInitStruct("Buffer Descriptors",
								  NBuffers * sizeof(BufferDescPadded) + buffer_align,
								  &foundDescs));

	/* Align buffer pool on IO page size boundary. */
	BufferBlocks = (char *)
		TYPEALIGN(buffer_align,
				  ShmemInitStruct("Buffer Blocks",
								  NBuffers * (Size) BLCKSZ + buffer_align,
								  &foundBufs));

	/* Align condition variables to cacheline boundary. */
	BufferIOCVArray = (ConditionVariableMinimallyPadded *)
		ShmemInitStruct("Buffer IO Condition Variables",
						NBuffers * sizeof(ConditionVariableMinimallyPadded),
						&foundIOCV);

	/*
	 * The array used to sort to-be-checkpointed buffer ids is located in
	 * shared memory, to avoid having to allocate significant amounts of
	 * memory at runtime. As that'd be in the middle of a checkpoint, or when
	 * the checkpointer is restarted, memory allocation failures would be
	 * painful.
	 */
	CkptBufferIds = (CkptSortItem *)
		ShmemInitStruct("Checkpoint BufferIds",
						NBuffers * sizeof(CkptSortItem), &foundBufCkpt);

	if (foundDescs || foundBufs || foundIOCV || foundBufCkpt)
	{
		/* should find all of these, or none of them */
		Assert(foundDescs && foundBufs && foundIOCV && foundBufCkpt);
		/* note: this path is only taken in EXEC_BACKEND case */
	}
	else
	{
		int			i;

		/*
		 * Initialize buffer partitions, including moving memory to different
		 * NUMA nodes (if enabled by GUC).
		 */
		buffer_partitions_init();

		/*
		 * Initialize all the buffer headers.
		 */
		for (i = 0; i < NBuffers; i++)
		{
			BufferDesc *buf = GetBufferDescriptor(i);

			ClearBufferTag(&buf->tag);

			pg_atomic_init_u64(&buf->state, 0);
			buf->wait_backend_pgprocno = INVALID_PROC_NUMBER;

			buf->buf_id = i;

			pgaio_wref_clear(&buf->io_wref);

			proclist_init(&buf->lock_waiters);
			ConditionVariableInit(BufferDescriptorGetIOCV(buf));
		}
	}

	/* Init other shared buffer-management stuff */
	StrategyInitialize(!foundDescs);

	/* Initialize per-backend file flush context */
	WritebackContextInit(&BackendWritebackContext,
						 &backend_flush_after);
}

/*
 * BufferManagerShmemSize
 *
 * compute the size of shared memory for the buffer pool including
 * data pages, buffer descriptors, hash tables, etc.
 *
 * XXX Called before allocation, so we don't know if huge pages get used yet.
 * So we need to assume huge pages get used, and use get_memory_page_size()
 * to calculate the largest possible memory page.
 */
Size
BufferManagerShmemSize(void)
{
	Size		size = 0;

	/* calculate partition info for buffers */
	buffer_partitions_prepare();

	/* size of buffer descriptors */
	size = add_size(size, mul_size(NBuffers, sizeof(BufferDescPadded)));
	/* to allow aligning buffer descriptors */
	size = add_size(size, Max(numa_page_size, PG_IO_ALIGN_SIZE));

	/* size of data pages, plus alignment padding */
	size = add_size(size, Max(numa_page_size, PG_IO_ALIGN_SIZE));
	size = add_size(size, mul_size(NBuffers, BLCKSZ));

	/*
	 * Extra alignment, so that the partitions are whole memory pages (we
	 * may need to pad the last one, so one page is enough). Without this
	 * we may get mbind() failures in pg_numa_move_to_node().
	 */
	size = add_size(size, Max(numa_page_size, PG_IO_ALIGN_SIZE));

	/* size of stuff controlled by freelist.c */
	size = add_size(size, StrategyShmemSize());

	/* size of I/O condition variables */
	size = add_size(size, mul_size(NBuffers,
								   sizeof(ConditionVariableMinimallyPadded)));
	/* to allow aligning the above */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of checkpoint sort array in bufmgr.c */
	size = add_size(size, mul_size(NBuffers, sizeof(CkptSortItem)));

	/* account for registry of NUMA partitions */
	size = add_size(size, MAXALIGN(offsetof(BufferPartitions, partitions) +
								   mul_size(sizeof(BufferPartition), numa_partitions)));

	return size;
}

/*
 * Calculate the NUMA node for a given buffer.
 */
int
BufferGetNode(Buffer buffer)
{
	/* not NUMA partitioning */
	if (numa_buffers_per_node == -1)
		return 0;

	/* no NUMA-aware partitioning */
	if ((numa_flags & NUMA_BUFFERS) == 0)
		return 0;

	return (buffer / numa_buffers_per_node);
}

/*
 * buffer_partitions_prepare
 *		Calculate parameters for partitioning buffers.
 *
 * We want to split the shared buffers into multiple partitions, of roughly
 * the same size. This is meant to serve multiple purposes. We want to map
 * the partitions to different NUMA nodes, to balance memory usage, and
 * allow partitioning some data structures built on top of buffers, to give
 * preference to local access (buffers on the same NUMA node). This applies
 * mostly to freelists and clocksweep.
 *
 * We may want to use partitioning even on non-NUMA systems, or when running
 * on a single NUMA node. Partitioning the freelist/clocksweep is beneficial
 * even without the NUMA effects.
 *
 * So we try to always build at least 4 partitions (MIN_BUFFER_PARTITIONS)
 * in total, or at least one partition per NUMA node. We always create the
 * same number of partitions per NUMA node.
 *
 * Some examples:
 *
 * - non-NUMA system (or 1 NUMA node): 4 partitions for the single node
 *
 * - 2 NUMA nodes: 4 partitions, 2 for each node
 *
 * - 3 NUMA nodes: 6 partitions, 2 for each node
 *
 * - 4+ NUMA nodes: one partition per node
 *
 * NUMA works on the memory-page granularity, which determines the smallest
 * amount of memory we can allocate to single node. This is determined by
 * how many BufferDescriptors fit onto a single memory page, so this depends
 * on huge page support. With 2MB huge pages (typical on x86 Linux), this is
 * 32768 buffers (256MB). With regular 4kB pages, it's 64 buffers (512KB).
 *
 * Note: This is determined before the allocation, i.e. we don't know if the
 * allocation got to use huge pages. So unless huge_pages=off we assume we're
 * using huge pages.
 *
 * This minimal size requirement only matters for the per-node amount of
 * memory, not for the individual partitions. The partitions for the same
 * node are a contiguous chunk of memory, which can be split arbitrarily,
 * it's independent of the NUMA granularity.
 *
 * XXX This patch only implements placing the buffers onto different NUMA
 * nodes. The freelist/clocksweep partitioning is implemented in separate
 * patches earlier in the patch series. Those patches however use the same
 * buffer partition registry, to align the partitions.
 *
 *
 * XXX This needs to consider the minimum chunk size, i.e. we can't split
 * buffers beyond some point, at some point it gets we run into the size of
 * buffer descriptors. Not sure if we should give preference to one of these
 * (probably at least print a warning).
 *
 * XXX We want to do this even with numa_buffers_interleave=false, so that the
 * other patches can do their partitioning. But in that case we don't need to
 * enforce the min chunk size (probably)?
 *
 * XXX We need to only call this once, when sizing the memory. But at that
 * point we don't know if we get to use huge pages or not (unless when huge
 * pages are disabled). We'll proceed as if the huge pages were used, and we
 * may have to use larger partitions. Maybe there's some sort of fallback,
 * but for now we simply disable the NUMA partitioning - it simply means the
 * shared buffers are too small.
 *
 * XXX We don't need to make each partition a multiple of min_partition_size.
 * That's something we need to do for a node (because NUMA works at granularity
 * of pages), but partitions for a single node can split that arbitrarily.
 * Although keeping the sizes power-of-two would allow calculating everything
 * as shift/mask, without expensive division/modulo operations.
 */
static void
buffer_partitions_prepare(void)
{
	/*
	 * Minimum number of buffers we can allocate to a NUMA node (determined by
	 * how many BufferDescriptors fit onto a memory page).
	 */
	int			min_node_buffers;

	/*
	 * Maximum number of nodes we can split shared buffers to, assuming each
	 * node gets the smallest allocatable chunk (the last node can get a
	 * smaller amount of memory, not the full chunk).
	 */
	int			max_nodes;

	/*
	 * How many partitions to create per node. Could be more than 1 for small
	 * number of nodes (of non-NUMA systems).
	 */
	int			num_partitions_per_node;

	/* bail out if already initialized (calculate only once) */
	if (numa_nodes != -1)
		return;

	/* XXX only gives us the number, the nodes may not be 0, 1, 2, ... */
#if USE_LIBNUMA
	numa_nodes = numa_num_configured_nodes();
#else
	/* without NUMA, assume there's just one node */
	numa_nodes = 1;
#endif

	/* we should never get here without at least one NUMA node */
	Assert(numa_nodes > 0);

	/*
	 * XXX A bit weird. Do we need to worry about postmaster? Could this even
	 * run outside postmaster? I don't think so.
	 *
	 * XXX Another issue is we may get different values than when sizing the
	 * the memory, because at that point we didn't know if we get huge pages,
	 * so we assumed we will. Shouldn't cause crashes, but we might allocate
	 * shared memory and then not use some of it (because of the alignment
	 * that we don't actually need). Not sure about better way, good for now.
	 */
	numa_page_size = pg_numa_page_size();

	/* make sure the chunks will align nicely */
	Assert(BLCKSZ % sizeof(BufferDescPadded) == 0);
	Assert(numa_page_size % sizeof(BufferDescPadded) == 0);
	Assert(((BLCKSZ % numa_page_size) == 0) || ((numa_page_size % BLCKSZ) == 0));

	/*
	 * The minimum number of buffers we can allocate from a single node, using
	 * the memory page size (determined by buffer descriptors). NUMA allocates
	 * memory in pages, and we need to do that for both buffers and
	 * descriptors at the same time.
	 *
	 * In practice the BLCKSZ doesn't really matter, because it's much larger
	 * than BufferDescPadded, so the result is determined buffer descriptors.
	 */
	min_node_buffers = (numa_page_size / sizeof(BufferDescPadded));

	/*
	 * Maximum number of nodes (each getting min_node_buffers) we can handle
	 * given the current shared buffers size. The last node is allowed to be
	 * smaller (half of the other nodes).
	 */
	max_nodes = (NBuffers + (min_node_buffers / 2)) / min_node_buffers;

	/*
	 * Can we actually do NUMA partitioning with these settings? If we can't
	 * handle the current number of nodes, then no.
	 *
	 * XXX This shouldn't be a big issue in practice. NUMA systems typically
	 * run with large shared buffers, which also makes the imbalance issues
	 * fairly significant (it's quick to rebalance 128MB, much slower to do
	 * that for 256GB).
	 */
	numa_can_partition = true;	/* assume we can allocate to nodes */
	if (numa_nodes > max_nodes)
	{
		elog(NOTICE, "shared buffers too small for %d nodes (max nodes %d)",
			 numa_nodes, max_nodes);
		numa_can_partition = false;
	}
	else if ((numa_flags & NUMA_BUFFERS) == 0)
	{
		elog(NOTICE, "NUMA-partitioning of buffers disabled");
		numa_can_partition = false;
	}

	/*
	 * We know we can partition to the desired number of nodes, now it's time
	 * to figure out how many partitions we need per node. We simply add
	 * partitions per node until we reach MIN_BUFFER_PARTITIONS.
	 *
	 * XXX Maybe we should make sure to keep the actual partition size a power
	 * of 2, to make the calculations simpler (shift instead of mod).
	 */
	num_partitions_per_node = 1;

	while (numa_nodes * num_partitions_per_node < MIN_BUFFER_PARTITIONS)
		num_partitions_per_node++;

	/* now we know the total number of partitions */
	numa_partitions = (numa_nodes * num_partitions_per_node);

	/*
	 * Finally, calculate how many buffers we'll assign to a single NUMA node.
	 * If we have only a single node, or when we can't partition for some
	 * reason, just take a "fair share" of buffers. This can happen for a
	 * number of reasons - missing NUMA support, partitioning of buffers not
	 * enabled, or not enough buffers for this many nodes.
	 *
	 * We still build partitions, because we want to allow partitioning of
	 * the clock-sweep later.
	 *
	 * The number of buffers for each partition is calculated later, once we
	 * have allocated the shared memory (because that's where we store it).
	 *
	 * XXX In both cases the last node can get fewer buffers.
	 */
	if (!numa_can_partition)
	{
		numa_buffers_per_node = (NBuffers + (numa_nodes - 1)) / numa_nodes;
	}
	else
	{
		numa_buffers_per_node = min_node_buffers;
		while (numa_buffers_per_node * numa_nodes < NBuffers)
			numa_buffers_per_node += min_node_buffers;

		/* the last node should get at least some buffers */
		Assert(NBuffers - (numa_nodes - 1) * numa_buffers_per_node > 0);
	}

	elog(DEBUG1, "NUMA: buffers %d partitions %d num_nodes %d per_node %d buffers_per_node %d (min %d)",
		 NBuffers, numa_partitions, numa_nodes, num_partitions_per_node,
		 numa_buffers_per_node, min_node_buffers);
}

/*
 * Sanity checks of buffers partitions - there must be no gaps, it must cover
 * the whole range of buffers, etc.
 */
static void
AssertCheckBufferPartitions(void)
{
#ifdef USE_ASSERT_CHECKING
	int			num_buffers = 0;

	Assert(BufferPartitionsArray->npartitions > 0);

	for (int i = 0; i < BufferPartitionsArray->npartitions; i++)
	{
		BufferPartition *part = &BufferPartitionsArray->partitions[i];

		/*
		 * We can get a single-buffer partition, if the sizing forces the last
		 * partition to be just one buffer. But it's unlikely (and
		 * undesirable).
		 */
		Assert(part->first_buffer <= part->last_buffer);
		Assert((part->last_buffer - part->first_buffer + 1) == part->num_buffers);

		num_buffers += part->num_buffers;

		/*
		 * The first partition needs to start on buffer 0. Later partitions
		 * need to be contiguous, without skipping any buffers.
		 */
		if (i == 0)
		{
			Assert(part->first_buffer == 0);
		}
		else
		{
			BufferPartition *prev = &BufferPartitionsArray->partitions[i - 1];

			Assert((part->first_buffer - 1) == prev->last_buffer);
		}

		/* the last partition needs to end on buffer (NBuffers - 1) */
		if (i == (BufferPartitionsArray->npartitions - 1))
		{
			Assert(part->last_buffer == (NBuffers - 1));
		}
	}

	Assert(num_buffers == NBuffers);
#endif
}

/*
 * buffer_partitions_init
 *		Initialize array of buffer partitions.
 */
static void
buffer_partitions_init(void)
{
	int			remaining_buffers = NBuffers;
	int			buffer = 0;
	int			parts_per_node = (numa_partitions / numa_nodes);
	char	   *buffers_ptr,
			   *descriptors_ptr;

	BufferPartitionsArray->npartitions = numa_partitions;
	BufferPartitionsArray->nnodes = numa_nodes;

	for (int n = 0; n < numa_nodes; n++)
	{
		/* buffers this node should get (last node can get fewer) */
		int			node_buffers = Min(remaining_buffers, numa_buffers_per_node);

		/* split node buffers netween partitions (last one can get fewer) */
		int			part_buffers = (node_buffers + (parts_per_node - 1)) / parts_per_node;

		remaining_buffers -= node_buffers;

		Assert((node_buffers > 0) && (node_buffers <= NBuffers));
		Assert((n >= 0) && (n < numa_nodes));

		for (int p = 0; p < parts_per_node; p++)
		{
			int			idx = (n * parts_per_node) + p;
			BufferPartition *part = &BufferPartitionsArray->partitions[idx];
			int			num_buffers = Min(node_buffers, part_buffers);

			Assert((idx >= 0) && (idx < numa_partitions));
			Assert((buffer >= 0) && (buffer < NBuffers));
			Assert((num_buffers > 0) && (num_buffers <= part_buffers));

			/* XXX we should get the actual node ID from the mask */
			if (numa_can_partition)
				part->numa_node = n;
			else
				part->numa_node = -1;

			part->num_buffers = num_buffers;
			part->first_buffer = buffer;
			part->last_buffer = buffer + (num_buffers - 1);

			elog(DEBUG1, "NUMA: buffer %d node %d partition %d buffers %d first %d last %d", idx, n, p, num_buffers, buffer, buffer + (num_buffers - 1));

			buffer += num_buffers;
			node_buffers -= part_buffers;
		}
	}

	AssertCheckBufferPartitions();

	/*
	 * With buffers interleaving disabled (or can't partition, because of
	 * shared buffers being too small), we're done.
	 */
	if (((numa_flags & NUMA_BUFFERS) == 0) || !numa_can_partition)
		return;

	/*
	 * Assign chunks of buffers and buffer descriptors to the available NUMA
	 * nodes. We can't use the regular interleaving, because with regular
	 * memory pages (smaller than BLCKSZ) we'd split all buffers to multiple
	 * NUMA nodes. And we don't want that.
	 *
	 * But even with huge pages it seems like a good idea to not map pages
	 * one by one.
	 *
	 * So we always assign a larger contiguous chunk of buffers to the same
	 * NUMA node, as calculated by choose_chunk_buffers(). We try to keep the
	 * chunks large enough to work both for buffers and buffer descriptors,
	 * but not too large. See the comments at choose_chunk_buffers() for
	 * details.
	 *
	 * Thanks to the earlier alignment (to memory page etc.), we know the
	 * buffers won't get split, etc.
	 *
	 * This also makes it easier / straightforward to calculate which NUMA
	 * node a buffer belongs to (it's a matter of divide + mod). See
	 * BufferGetNode().
	 *
	 * We need to account for partitions being of different length, when the
	 * NBuffers is not nicely divisible. To do that we keep track of the start
	 * of the next partition.
	 *
	 * We always map all partitions for the same node at once, so that we
	 * don't need to worry about alignment of memory pages that get split
	 * between partitions (we only worry about min_node_buffers for whole
	 * NUMA nodes, not for individual partitions).
	 */
	buffers_ptr = BufferBlocks;
	descriptors_ptr = (char *) BufferDescriptors;

	for (int n = 0; n < numa_nodes; n++)
	{
		char	   *startptr,
				   *endptr;
		int			num_buffers = 0;

		/* sum buffers in all partitions for this node */
		for (int p = 0; p < parts_per_node; p++)
		{
			int		pidx = (n * parts_per_node + p);
			BufferPartition *part = &BufferPartitionsArray->partitions[pidx];

			Assert(part->numa_node == n);

			num_buffers += part->num_buffers;
		}

		/* first map buffers */
		startptr = buffers_ptr;
		endptr = startptr + ((Size) num_buffers * BLCKSZ);

		/*
		 * Make sure the partition is a multiple of memory page, so that we
		 * don't get mbind failures in move_to_node calls. This matters only
		 * for the last partition, the earlier ones should be always sized
		 * as multiples of pages.
		 */
		endptr = (char *) TYPEALIGN(numa_page_size, endptr);

		buffers_ptr = endptr;	/* start of the next partition */

		elog(DEBUG1, "NUMA: buffer_partitions_init: %d => buffers %d start %p end %p (size %zd)",
			 n, num_buffers, startptr, endptr, (endptr - startptr));

		pg_numa_move_to_node(startptr, endptr, n);

		/* now do the same for buffer descriptors */
		startptr = descriptors_ptr;
		endptr = startptr + ((Size) num_buffers * sizeof(BufferDescPadded));

		/*
		 * Make sure the partition is a multiple of memory page, so that we
		 * don't get mbind failures in move_to_node calls. This matters only
		 * for the last partition, the earlier ones should be always sized
		 * as multiples of pages.
		 */
		endptr = (char *) TYPEALIGN(numa_page_size, endptr);

		descriptors_ptr = endptr;

		elog(DEBUG1, "NUMA: buffer_partitions_init: %d => descriptors %d start %p end %p (size %zd)",
			 n, num_buffers, startptr, endptr, (endptr - startptr));

		pg_numa_move_to_node(startptr, endptr, n);
	}

	/* we should have consumed the arrays exactly */
	Assert(buffers_ptr <= (char *) TYPEALIGN(numa_page_size, BufferBlocks + (Size) NBuffers * BLCKSZ));
	Assert(descriptors_ptr == (char *) TYPEALIGN(numa_page_size, (char *) BufferDescriptors + (Size) NBuffers * sizeof(BufferDescPadded)));
}

int
BufferPartitionCount(void)
{
	return BufferPartitionsArray->npartitions;
}

int
BufferPartitionNodes(void)
{
	return BufferPartitionsArray->nnodes;
}

void
BufferPartitionGet(int idx, int *node, int *num_buffers,
				   int *first_buffer, int *last_buffer)
{
	if ((idx >= 0) && (idx < BufferPartitionsArray->npartitions))
	{
		BufferPartition *part = &BufferPartitionsArray->partitions[idx];

		*node = part->numa_node;
		*num_buffers = part->num_buffers;
		*first_buffer = part->first_buffer;
		*last_buffer = part->last_buffer;

		return;
	}

	elog(ERROR, "invalid partition index");
}

/* return parameters before the partitions are initialized (during sizing) */
void
BufferPartitionParams(int *num_partitions, int *num_nodes)
{
	if (num_partitions)
		*num_partitions = numa_partitions;

	if (num_nodes)
		*num_nodes = numa_nodes;
}

/* XXX the GUC hooks should probably be somewhere else? */
bool
check_debug_numa(char **newval, void **extra, GucSource source)
{
	bool		result = true;
	int			flags;

#if USE_LIBNUMA == 0
	if (strcmp(*newval, "") != 0)
	{
		GUC_check_errdetail("\"%s\" is not supported on this platform.",
							"debug_numa");
		result = false;
	}
	flags = 0;
#else
	List	   *elemlist;
	ListCell   *l;
	char	   *rawstring;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	if (!SplitGUCList(rawstring, ',', &elemlist))
	{
		GUC_check_errdetail("Invalid list syntax in parameter \"%s\".",
							"debug_numa");
		pfree(rawstring);
		list_free(elemlist);
		return false;
	}

	flags = 0;
	foreach(l, elemlist)
	{
		char	   *item = (char *) lfirst(l);

		if (pg_strcasecmp(item, "buffers") == 0)
			flags |= NUMA_BUFFERS;
		else
		{
			GUC_check_errdetail("Invalid option \"%s\".", item);
			result = false;
			break;
		}
	}

	pfree(rawstring);
	list_free(elemlist);
#endif

	if (!result)
		return result;

	/* Save the flags in *extra, for use by assign_debug_io_direct */
	*extra = guc_malloc(LOG, sizeof(int));
	if (!*extra)
		return false;
	*((int *) *extra) = flags;

	return result;
}

void
assign_debug_numa(const char *newval, void *extra)
{
	int		   *flags = (int *) extra;

	numa_flags = *flags;
}
