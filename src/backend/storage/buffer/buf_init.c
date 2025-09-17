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

#include "storage/aio.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
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

/* *
 * number of buffer partitions */
#define NUM_CLOCK_SWEEP_PARTITIONS	4

/* Array of structs with information about buffer ranges */
BufferPartitions *BufferPartitionsArray = NULL;

static void buffer_partitions_init(void);

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

	/* allocate the partition registry first */
	BufferPartitionsArray = (BufferPartitions *)
		ShmemInitStruct("Buffer Partitions",
						offsetof(BufferPartitions, partitions) +
						mul_size(sizeof(BufferPartition), NUM_CLOCK_SWEEP_PARTITIONS),
						&foundParts);

	/* Align descriptors to a cacheline boundary. */
	BufferDescriptors = (BufferDescPadded *)
		ShmemInitStruct("Buffer Descriptors",
						NBuffers * sizeof(BufferDescPadded),
						&foundDescs);

	/* Align buffer pool on IO page size boundary. */
	BufferBlocks = (char *)
		TYPEALIGN(PG_IO_ALIGN_SIZE,
				  ShmemInitStruct("Buffer Blocks",
								  NBuffers * (Size) BLCKSZ + PG_IO_ALIGN_SIZE,
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

		/* Initialize buffer partitions (calculate buffer ranges). */
		buffer_partitions_init();

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

			LWLockInitialize(BufferDescriptorGetContentLock(buf),
							 LWTRANCHE_BUFFER_CONTENT);

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
	size = add_size(size, PG_IO_ALIGN_SIZE);
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

	/* account for registry of NUMA partitions */
	size = add_size(size, MAXALIGN(offsetof(BufferPartitions, partitions) +
								   mul_size(sizeof(BufferPartition), NUM_CLOCK_SWEEP_PARTITIONS)));

	return size;
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

	/* number of buffers per partition (make sure to not overflow) */
	int			part_buffers
		= ((int64) NBuffers + (NUM_CLOCK_SWEEP_PARTITIONS - 1)) / NUM_CLOCK_SWEEP_PARTITIONS;

	BufferPartitionsArray->npartitions = NUM_CLOCK_SWEEP_PARTITIONS;

	for (int n = 0; n < BufferPartitionsArray->npartitions; n++)
	{
		BufferPartition *part = &BufferPartitionsArray->partitions[n];

		/* buffers this partition should get (last partition can get fewer) */
		int			num_buffers = Min(remaining_buffers, part_buffers);

		remaining_buffers -= num_buffers;

		Assert((num_buffers > 0) && (num_buffers <= part_buffers));
		Assert((buffer >= 0) && (buffer < NBuffers));

		part->num_buffers = num_buffers;
		part->first_buffer = buffer;
		part->last_buffer = buffer + (num_buffers - 1);

		buffer += num_buffers;
	}

	AssertCheckBufferPartitions();
}

int
BufferPartitionCount(void)
{
	return BufferPartitionsArray->npartitions;
}

void
BufferPartitionGet(int idx, int *num_buffers,
				   int *first_buffer, int *last_buffer)
{
	if ((idx >= 0) && (idx < BufferPartitionsArray->npartitions))
	{
		BufferPartition *part = &BufferPartitionsArray->partitions[idx];

		*num_buffers = part->num_buffers;
		*first_buffer = part->first_buffer;
		*last_buffer = part->last_buffer;

		return;
	}

	elog(ERROR, "invalid partition index");
}

