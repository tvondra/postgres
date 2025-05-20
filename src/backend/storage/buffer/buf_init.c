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

static int buffer_get_numa_node(Buffer buffer);

static int numa_chunk_items = -1;
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

	/*
	 * Ensure buffers are properly aligned both for PG_IO_ALIGN_SIZE and
	 * memory page size, so that buffers don't get "split" to multiple
	 * memory pages unnecessarily.
	 */
	Size		mem_page_size;
	Size		buffer_align;

	if (IsUnderPostmaster)
		mem_page_size = pg_get_shmem_pagesize();
	else
		mem_page_size = get_memory_page_size();

	/*
	 * XXX Maybe with (mem_page_size > PG_IO_ALIGN_SIZE), we don't need to
	 * align to mem_page_size? Especially for very large huge pages (e.g. 1GB)
	 * that doesn't seem quite worth it. Maybe we should simply align to
	 * BLCKSZ, so that buffers don't get split?
	 */
	buffer_align = Max(mem_page_size, PG_IO_ALIGN_SIZE);

	/* one page is a multiple of the other */
	Assert(((mem_page_size % PG_IO_ALIGN_SIZE) == 0) ||
		   ((PG_IO_ALIGN_SIZE % mem_page_size) == 0));

	/* Align descriptors to a cacheline boundary. */
	/*
	 * XXX Should we partition buffer descriptors to NUMA nodes too? It would
	 * need to be partitioned the same way as buffers, i.e. the buffer and
	 * descriptor should be on the same NUMA node.
	 *
	 * XXX Use TYPEALIGN() to make sure each part starts at the beginning of
	 * a memory page.
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
		 * Before we start touching the memory, interleave the memory to
		 * available NUMA nodes. The regular interleaving may not work as
		 * the memory page size could be smaller than block size, in which
		 * case a buffer might get located on multiple NUMA nodes, and we
		 * don't want that.
		 *
		 * With huge pages it'd be less of a problem - some buffers might
		 * get split on multiple memory pages (and thus NUMA nodes), but
		 * blocks are much smaller than huge pages so it would be just a
		 * tiny fraction (might still affect performance, not sure).
		 *
		 * We try to be nice and keep buffers on a single NUMA node. To
		 * ensure it's possible (while still interleaving nodes), we align
		 * the start of buffers to a memory page. Otherwise we'd align to
		 * PG_IO_ALIGN_SIZE, and buffers might be still be "shifted" and
		 * split unnecessarily.
		 *
		 * XXX Maybe instead of trying to interleave the buffers like this,
		 * it might be better to simply split the memory into contiguous
		 * chunks of equal size, and move them to nodes 0, 1, 2, ... So
		 * a bit like interleaving, but with much larger chunks. That
		 * would make it easier to calculate the node the buffer belongs
		 * (or should belong) to. Is there any particular benefit from
		 * interleaving page by page?
		 *
		 * XXX Update comment to reflect that we're not processing buffers
		 * one by one, but chunks.
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
			startptr = (BufferBlocks + ((Size) 0) * BLCKSZ);
			endptr = (BufferBlocks + ((Size) NBuffers) * BLCKSZ);
			chunk_size = (numa_chunk_items * BLCKSZ);

			pg_numa_interleave_memory(startptr, endptr,
									  mem_page_size,
									  chunk_size,
									  numa_nodes);

			/* now do the same for buffer descriptors */
			startptr = (char *) (BufferDescriptors + ((Size) 0) * sizeof(BufferDescPadded));
			endptr = (char *) (BufferDescriptors + ((Size) NBuffers) * sizeof(BufferDescPadded));
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

			elog(WARNING, "buffer %d node %d", i, buffer_get_numa_node(i));

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
	if (huge_pages == HUGE_PAGES_ON || huge_pages == HUGE_PAGES_TRY)
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

static void
pg_numa_interleave_memory(char *startptr, char *endptr,
						  Size mem_page_size, Size chunk_size,
						  int num_nodes)
{
	volatile uint64 touch pg_attribute_unused();
	char   *ptr = startptr;

	/*
	 * FIXME this still works page-by-page, we should use larger chunks.
	 */
	while (ptr < endptr)
	{
		int	status;
		int	r;
		int	node;

		/*
		 * What NUMA node does this range belong to? Each chunk should go to
		 * the same NUMA node, in a round-robin manner.
		 */
		node = ((ptr - startptr) / chunk_size) % num_nodes;

		/*
		 * Nope, we have the first buffer from the next memory page,
		 * and we'll set NUMA node for it (and all pages up to the
		 * next buffer). The buffer should align with the memory page,
		 * thanks to the buffer_align earlier.
		 */
		Assert((int64) ptr % mem_page_size == 0);

		/* move_pages fails for pages not mapped in this process */
		pg_numa_touch_mem_if_required(touch, ptr);

		/* now actually move the memory to the correct NUMA node */
		r = numa_move_pages(0, 1, (void **) &ptr, &node, &status, 0);

		if (r != 0)
			elog(WARNING, "failed to move page %p to node %d", ptr, node);

		ptr += mem_page_size;
	}

	/* should have processed all chunks */
	Assert(ptr == endptr);
}

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
	 * The minimum number of items to fill memory page with descriptors and
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

	elog(LOG, "choose_chunk_items: chunk items %d", num_items);

	return num_items;
}

static int
buffer_get_numa_node(Buffer buffer)
{
	/* not NUMA interleaving */
	if (numa_chunk_items == -1)
		return -1;

	return (buffer / numa_chunk_items) % numa_nodes;
}
