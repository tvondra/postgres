/*-------------------------------------------------------------------------
 *
 * buf_init.c
 *	  buffer manager initialization routines
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
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
#include "storage/pg_shmem.h"
#include "storage/proc.h"

BufferDescPadded *BufferDescriptors;
char	   *BufferBlocks;
ConditionVariableMinimallyPadded *BufferIOCVArray;
WritebackContext BackendWritebackContext;
CkptSortItem *CkptBufferIds;


static Size get_memory_page_size(void);
static int choose_chunk_items(int NBuffers, Size mem_page_size, int num_nodes);
static void pg_numa_interleave_memory(char *startptr, char *endptr,
									  Size mem_page_size, Size chunk_size,
									  int num_nodes);

/* number of buffers allocated on the same NUMA node */
static int numa_chunk_items = -1;

/* number of NUMA nodes (as returned by numa_num_configured_nodes) */
static int numa_nodes = -1;


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
				foundBufCkpt;
	Size		mem_page_size;
	Size		buffer_align;

	/*
	 * XXX A bit weird. Do we need to worry about postmaster? Could this
	 * even run outside postmaster? I don't think so.
	 *
	 * XXX Another issue is we may get different values than when sizing the
	 * the memory, because at that point we didn't know if we get huge pages,
	 * so we assumed we will. Shouldn't cause crashes, but we might allocate
	 * shared memory and then not use some of it. Not sure about better way.
	 */
	if (IsUnderPostmaster)
		mem_page_size = pg_get_shmem_pagesize();
	else
		mem_page_size = get_memory_page_size();

	/*
	 * With NUMA we need to ensure the buffers are properly aligned not
	 * just to PG_IO_ALIGN_SIZE, but also to memory page size, because
	 * NUMA works on page granularity, and we don't want a buffer to get
	 * split to multiple nodes (when using multiple memory pages).
	 *
	 * We also don't want to interfere with other parts of shared memory,
	 * which could easily happen with huge pages (e.g. with data stored
	 * before buffers).
	 *
	 * We do this by aligning to the larger of the two values (we know
	 * both are power-of-two values, so the larger value is automatically
	 * a multiple of the lesser one).
	 *
	 * XXX Maybe there's a way to use less alignment?
	 *
	 * XXX Maybe with (mem_page_size > PG_IO_ALIGN_SIZE), we don't need to
	 * align to mem_page_size? Especially for very large huge pages (e.g. 1GB)
	 * that doesn't seem quite worth it. Maybe we should simply align to
	 * BLCKSZ, so that buffers don't get split? Still, we might interfere
	 * with other stuff stored in shared memory that we want to allocate on
	 * a particular NUMA node (e.g. ProcArray).
	 *
	 * XXX Maybe with "too large" huge pages we should just not do this, or
	 * maybe do this only for sufficiently large areas (e.g. shared buffers,
	 * but not ProcArray).
	 */
	buffer_align = Max(mem_page_size, PG_IO_ALIGN_SIZE);

	/* one page is a multiple of the other */
	Assert(((mem_page_size % PG_IO_ALIGN_SIZE) == 0) ||
		   ((PG_IO_ALIGN_SIZE % mem_page_size) == 0));

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
		 * Assign chunks of buffers and buffer descriptors to the available
		 * NUMA nodes. We can't use the regular interleaving, because with
		 * regular memory pages (smaller than BLCKSZ) we'd split all buffers
		 * to multiple NUMA nodes. And we don't want that.
		 *
		 * But even with huge pages it seems like a good idea to not have
		 * mapping for each page.
		 *
		 * So we always assign a larger contiguous chunk of buffers to the
		 * same NUMA node, as calculated by choose_chunk_items(). We try
		 * to keep the chunks large enough to work both for buffers and
		 * buffer descriptors, but not too large. See the comments at
		 * choose_chunk_items() for details.
		 *
		 * Thanks to the earlier alignment (to memory page etc.), we know
		 * the buffers won't get split, etc.
		 *
		 * This also makes it easier / straightforward to calculate which
		 * NUMA node a buffer belongs to (it's a matter of divide + mod).
		 * See BufferGetNode().
		 */
		if (numa_buffers_interleave)
		{
			char   *startptr,
				   *endptr;
			Size	chunk_size;

			numa_nodes = numa_num_configured_nodes();

			numa_chunk_items
				= choose_chunk_items(NBuffers, mem_page_size, numa_nodes);

			elog(LOG, "BufferManagerShmemInit num_nodes %d chunk_items %d",
				 numa_nodes, numa_chunk_items);

			/* first map buffers */
			startptr = BufferBlocks;
			endptr = startptr + ((Size) NBuffers) * BLCKSZ;
			chunk_size = (numa_chunk_items * BLCKSZ);

			pg_numa_interleave_memory(startptr, endptr,
									  mem_page_size,
									  chunk_size,
									  numa_nodes);

			/* now do the same for buffer descriptors */
			startptr = (char *) BufferDescriptors;
			endptr = startptr + ((Size) NBuffers) * sizeof(BufferDescPadded);
			chunk_size = (numa_chunk_items * sizeof(BufferDescPadded));

			pg_numa_interleave_memory(startptr, endptr,
									  mem_page_size,
									  chunk_size,
									  numa_nodes);
		}

		/*
		 * Initialize all the buffer headers.
		 */
		for (i = 0; i < NBuffers; i++)
		{
			BufferDesc *buf = GetBufferDescriptor(i);

			ClearBufferTag(&buf->tag);

			pg_atomic_init_u32(&buf->state, 0);
			buf->wait_backend_pgprocno = INVALID_PROC_NUMBER;

			buf->buf_id = i;

			pgaio_wref_clear(&buf->io_wref);

			/*
			 * Initially link all the buffers together as unused. Subsequent
			 * management of this list is done by freelist.c.
			 */
			buf->freeNext = i + 1;

			LWLockInitialize(BufferDescriptorGetContentLock(buf),
							 LWTRANCHE_BUFFER_CONTENT);

			ConditionVariableInit(BufferDescriptorGetIOCV(buf));
		}

		/* Correct last entry of linked list */
		GetBufferDescriptor(NBuffers - 1)->freeNext = FREENEXT_END_OF_LIST;
	}

	/*
	 * As this point we have all the buffers in a single long freelist. With
	 * freelist partitioning we rebuild them in StrategyInitialize.
	 */

	/* Init other shared buffer-management stuff */
	StrategyInitialize(!foundDescs);

	/* Initialize per-backend file flush context */
	WritebackContextInit(&BackendWritebackContext,
						 &backend_flush_after);
}

/*
 * Determine the size of memory page.
 *
 * XXX This is a bit tricky, because the result depends at which point we call
 * this. Before the allocation we don't know if we succeed in allocating huge
 * pages - but we have to size everything for the chance that we will. And then
 * if the huge pages fail (with 'huge_pages=try'), we'll use the regular memory
 * pages. But at that point we can't adjust the sizing.
 *
 * XXX Maybe with huge_pages=try we should do the sizing twice - first with
 * huge pages, and if that fails, then without them. But not for this patch.
 * Up to this point there was no such dependency on huge pages.
 */
static Size
get_memory_page_size(void)
{
	Size		os_page_size;
	Size		huge_page_size;

#ifdef WIN32
	SYSTEM_INFO sysinfo;

	GetSystemInfo(&sysinfo);
	os_page_size = sysinfo.dwPageSize;
#else
	os_page_size = sysconf(_SC_PAGESIZE);
#endif

	/* assume huge pages get used, unless HUGE_PAGES_OFF */
	if (huge_pages_status != HUGE_PAGES_OFF)
		GetHugePageSize(&huge_page_size, NULL);
	else
		huge_page_size = 0;

	return Max(os_page_size, huge_page_size);
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
	Size		mem_page_size;

	/* XXX why does IsUnderPostmaster matter? */
	if (IsUnderPostmaster)
		mem_page_size = pg_get_shmem_pagesize();
	else
		mem_page_size = get_memory_page_size();

	/* size of buffer descriptors */
	size = add_size(size, mul_size(NBuffers, sizeof(BufferDescPadded)));
	/* to allow aligning buffer descriptors */
	size = add_size(size, Max(mem_page_size, PG_IO_ALIGN_SIZE));

	/* size of data pages, plus alignment padding */
	size = add_size(size, Max(mem_page_size, PG_IO_ALIGN_SIZE));
	size = add_size(size, mul_size(NBuffers, BLCKSZ));

	/* size of stuff controlled by freelist.c */
	size = add_size(size, StrategyShmemSize());

	/* size of I/O condition variables */
	size = add_size(size, mul_size(NBuffers,
								   sizeof(ConditionVariableMinimallyPadded)));
	/* to allow aligning the above */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of checkpoint sort array in bufmgr.c */
	size = add_size(size, mul_size(NBuffers, sizeof(CkptSortItem)));

	return size;
}

/*
 * choose_chunk_items
 *		choose the number of buffers allocated to a NUMA node at once
 *
 * We don't map shared buffers to NUMA nodes one by one, but in larger chunks.
 * This is both for efficiency reasons (fewer mappings), and also because we
 * want to map buffer descriptors too - and descriptors are much smaller. So
 * we pick a number that's high enough for descriptors, but not too high (now
 * there's a hardcoded limit of 1GB).
 *
 * We also want to keep buffers somehow evenly distributed on nodes, with
 * about NBuffers/nodes per node. So we don't use chunks larger than this,
 * to keep it as fair as possible (the chunk size is a possible difference
 * between memory allocated to different NUMA nodes).
 *
 * But it's theoretically possible shared buffers are so small this is not
 * possible (i.e. it's less than chunk_size). But sensible NUMA systems will
 * use a lot of memory, much more than 1GB / node, so this is unlikely. In
 * most cases the chunk size will be 1GB.
 *
 * So we simply print a warning and that's it.
 */
static int
choose_chunk_items(int NBuffers, Size mem_page_size, int num_nodes)
{
	int	num_items;
	int	max_items;

	/* make sure the chunks will align nicely */
	Assert(BLCKSZ % sizeof(BufferDescPadded) == 0);
	Assert(mem_page_size % sizeof(BufferDescPadded) == 0);
	Assert(((BLCKSZ % mem_page_size) == 0) || ((mem_page_size % BLCKSZ) == 0));

	/*
	 * The minimum number of items to fill a memory page with descriptors and
	 * blocks. The NUMA allocates memory in pages, and we need to do that for
	 * both buffers and descriptors.
	 *
	 * In practice the BLCKSZ doesn't really matter, because it's much larger
	 * than BufferDescPadded, so the result is determined buffer descriptors.
	 * But it's clearer this way.
	 */
	num_items = Max(mem_page_size / sizeof(BufferDescPadded),
					mem_page_size / BLCKSZ);

	/*
	 * What's the largest chunk to use? NBuffers/num_nodes is the max we can
	 * do, larger chunks would result in imbalance (some nodes getting much
	 * fewer buffers). But we limit that to 1/2, to make it more even. And
	 * we don't make chunks larger than 1GB.
	 *
	 * XXX Some of these limits are a bit arbitrary.
	 */
	max_items = Min((NBuffers / num_nodes) / 2,			/* 1/2 of fair share */
					(1024L * 1024L * 1024L) / BLCKSZ);	/* 1GB of blocks */

	/* Did we already exceed the maximum share? */
	if (num_items > max_items)
		elog(WARNING, "choose_chunk_items: chunk items exceeds max (%d > %d)",
			 num_items, max_items);

	/* grow the chunk size until we hit the limit. */
	while (2 * num_items <= max_items)
		num_items *= 2;

	return num_items;
}

/*
 * Calculate the NUMA node for a given buffer.
 */
int
BufferGetNode(Buffer buffer)
{
	/* not NUMA interleaving */
	if (numa_chunk_items == -1)
		return -1;

	return (buffer / numa_chunk_items) % numa_nodes;
}

/* batches for numa_move_pages */
#define BUFFERS_NUMA_BATCH	128

/*
 * pg_numa_interleave_memory
 *		move memory to different NUMA nodes in larger chunks
 *
 * startptr - start of the region (should be aligned to page size)
 * endptr - end of the region (doesn't need to be aligned)
 * mem_page_size - size of the memory page size
 * chunk_size - size of the chunk to move to a single node (should be multiple
 *              of page size
 * num_nodes - number of nodes to allocate memory to
 *
 * XXX Maybe this should use numa_tonode_memory and numa_police_memory instead?
 * That might be more efficient than numa_move_pages, as it works on larger
 * chunks of memory, not individual system pages, I think.
 */
static void
pg_numa_interleave_memory(char *startptr, char *endptr,
						  Size mem_page_size, Size chunk_size,
						  int num_nodes)
{
	volatile uint64 touch pg_attribute_unused();
	char   *ptr = startptr;

	/* batch calls to numa_move_pages for efficiency reasons */
	void   *ptrs[BUFFERS_NUMA_BATCH];
	int		status[BUFFERS_NUMA_BATCH];
	int		nodes[BUFFERS_NUMA_BATCH];
	int		nptrs = 0;

	/*
	 * Walk the memory pages in the range, and determine the node for each one.
	 * numa_move_pages is fairly expensive, so batch the calls for efficiency.
	 */
	while (ptr < endptr)
	{
		/*
		 * What NUMA node does this range belong to? Each chunk should go to
		 * the same NUMA node, in a round-robin manner.
		 */
		int	node = ((ptr - startptr) / chunk_size) % num_nodes;

		/*
		 * Nope, we have the first buffer from the next memory page,
		 * and we'll set NUMA node for it (and all pages up to the
		 * next buffer). The buffer should align with the memory page,
		 * thanks to the buffer_align earlier.
		 */
		Assert((int64) ptr % mem_page_size == 0);

		/* move_pages fails for pages not mapped in this process */
		pg_numa_touch_mem_if_required(touch, ptr);

		ptrs[nptrs] = ptr;
		nodes[nptrs] = node;

		nptrs++;

		/* if batch is full actually move the memory to the correct NUMA node */
		if (nptrs == BUFFERS_NUMA_BATCH)
		{
			int r = numa_move_pages(0, nptrs, ptrs, nodes, status, 0);

			if (r != 0)
				elog(WARNING, "failed to move page %p res %d", ptr, r);

			nptrs = 0;
		}

		ptr += mem_page_size;
	}

	/* there may be a couple pages in the buffer */
	if (nptrs > 0)
	{
		int r = numa_move_pages(0, nptrs, ptrs, nodes, status, 0);

		if (r != 0)
			elog(WARNING, "failed to move page %p res %d", ptr, r);

		nptrs = 0;
	}

	/* should have processed all chunks */
	Assert(ptr == endptr);
}
