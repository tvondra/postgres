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

BufferDescPadded *BufferDescriptors;
char	   *BufferBlocks;
ConditionVariableMinimallyPadded *BufferIOCVArray;
WritebackContext BackendWritebackContext;
CkptSortItem *CkptBufferIds;


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
	Size		mem_page_size = pg_get_shmem_pagesize();
	Size		buffer_align = Max(mem_page_size, PG_IO_ALIGN_SIZE);

	/* one page is a multiple of the other */
	Assert(((mem_page_size % PG_IO_ALIGN_SIZE) == 0) ||
		   ((PG_IO_ALIGN_SIZE % mem_page_size) == 0));

	/* Align descriptors to a cacheline boundary. */
	BufferDescriptors = (BufferDescPadded *)
		ShmemInitStruct("Buffer Descriptors",
						NBuffers * sizeof(BufferDescPadded),
						&foundDescs);

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
		 */
		if (numa_buffers_interleave)
		{
			char   *next_page = NULL;
			int		node = 0;
			int		num_nodes = numa_num_configured_nodes();
			volatile uint64 touch pg_attribute_unused();

			for (i = 0; i < NBuffers; i++)
			{
				/* pointer to this buffer */
				char *buffptr = (BufferBlocks + ((Size) (i)) * BLCKSZ);
				char *tmp;

				/*
				 * Skip buffers we already moved to a NUMA node with the last
				 * memory page. Happens if page size > block size.
				 */
				if (buffptr < next_page)
					continue;

				/*
				 * Nope, we have the first buffer from the next memory page,
				 * and we'll set NUMA node for it (and all pages up to the
				 * next buffer). The buffer should align with the memory page,
				 * thanks to the buffer_align earlier.
				 */
				Assert(buffptr % mem_page_size == 0);

				tmp = buffptr;
				while (tmp < buffptr + BLCKSZ)
				{
					int status;
					int r;

					/* move_pages fails for pages not mapped in this process */
					pg_numa_touch_mem_if_required(touch, tmp);

					r = numa_move_pages(0, 1, (void **) &tmp, &node, &status, 0);

					if (r != 0)
						elog(WARNING, "failed to move page %p to node %d", tmp, node);

					tmp += mem_page_size;
				}

				/* we're done with this buffer/page, so advance to next node */
				node = (node + 1) % num_nodes;

				/* remember the first unmoved page */
				next_page = tmp;
			}
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

	Assert(IsUnderPostmaster);

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

	/* size of buffer descriptors */
	size = add_size(size, mul_size(NBuffers, sizeof(BufferDescPadded)));
	/* to allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of data pages, plus alignment padding */
	size = add_size(size, get_memory_page_size());
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
