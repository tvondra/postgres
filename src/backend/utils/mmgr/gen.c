/*-------------------------------------------------------------------------
 *
 * gen.c
 *	  Generational allocator definitions.
 *
 * GEN is a custom MemoryContext implementation designed for cases of
 * chunks with similar lifespan.
 *
 * Portions Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/gen.c
 *
 *
 *	This memory context is based on the assumption that the allocated chunks
 *	have similar lifespan, i.e. that chunks allocated close from each other
 *	(by time) will also be freed in close proximity, and mostly in the same
 *	order. This is typical for various queue-like use cases, i.e. when tuples
 *	are constructed, processed and then thrown away.
 *
 *	The memory context uses a very simple approach to free space management.
 *	Instead of a complex freelist (like AllocSet), each block tracks a number
 *	of allocated and freed chunks. The space release by freed chunks is not
 *	reused, and once all chunks are freed (i.e. when nallocated == nfreed),
 *	the whole block is thrown away. When the allocated chunks have similar
 *	lifespan, this works very well and is extremely cheap.
 *
 *	The current implementation uses a fixed block size - maybe it should adapt
 *	similar approach as AllocSet, with min/max block size limits. It however
 *	uses dedicated blocks for oversized chunks, just like AllocSet.
 *
 *	XXX It might be possible to improve this by keeping a small freelist for
 *	only a small number of recent blocks, but it's not clear it's worth the
 *	additional complexity.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/memdebug.h"
#include "utils/memutils.h"


#define GEN_BLOCKHDRSZ	MAXALIGN(sizeof(GenBlockData))
#define GEN_CHUNKHDRSZ	MAXALIGN(sizeof(GenChunkData))

/* Portion of GEN_CHUNKHDRSZ examined outside slab.c. */
#define GEN_CHUNK_PUBLIC	\
	(offsetof(GenChunkData, size) + sizeof(Size))

/* Portion of GEN_CHUNKHDRSZ excluding trailing padding. */
#ifdef MEMORY_CONTEXT_CHECKING
#define GEN_CHUNK_USED	\
	(offsetof(GenChunkData, requested_size) + sizeof(Size))
#else
#define GEN_CHUNK_USED	\
	(offsetof(GenChunkData, size) + sizeof(Size))
#endif

typedef struct GenBlockData *GenBlock;		/* forward reference */
typedef struct GenChunkData *GenChunk;

typedef void *GenPointer;

/*
 * GenContext is a self-tuning version of SlabContext.
 */
typedef struct GenContext
{
	MemoryContextData header;	/* Standard memory-context fields */

	/* generational context parameters */
	Size		blockSize;		/* block size */

	GenBlock	block;			/* current (most recently allocated) block */
	GenBlock	blocks;			/* list of blocks */

} GenContext;

typedef GenContext *Gen;

/*
 * GenBlockData
 *		A GenBlock is the unit of memory that is obtained by gen.c
 *		from malloc().  It contains one or more GenChunks, which are
 *		the units requested by palloc() and freed by pfree().  GenChunks
 *		cannot be returned to malloc() individually, instead pfree()
 *		updates a free counter on a block and when all chunks on a block
 *		are freed the whole block is returned to malloc().
 *
 *		GenBlockData is the header data for a block --- the usable space
 *		within the block begins at the next alignment boundary.
 */
typedef struct GenBlockData
{
	Gen			gen;			/* context owning this block */
	GenBlock	prev;			/* previous block in block list */
	GenBlock	next;			/* next block in block list */
	int			nchunks;		/* number of chunks in the block */
	int			nfree;			/* number of free chunks */
	char	   *freeptr;		/* start of free space in this block */
	char	   *endptr;			/* end of space in this block */
}	GenBlockData;

/*
 * GenChunk
 *		The prefix of each piece of memory in an GenBlock
 *
 * NB: this MUST match StandardChunkHeader as defined by utils/memutils.h.
 * However it's possible to add fields in front of the StandardChunkHeader
 * fields, which is used to add pointer to the block owning a chunk.
 */
typedef struct GenChunkData
{
	/* block owning this chunk */
	void	   *block;
	/* gen is pointer to the Gen context owning this chunk (and block) */
	void	   *gen;
	/* size is always the size of the usable space in the chunk */
	Size		size;
#ifdef MEMORY_CONTEXT_CHECKING
	/* when debugging memory usage, also store actual requested size */
	/* this is zero in a free chunk */
	Size		requested_size;
#endif
}	GenChunkData;


/*
 * GenIsValid
 *		True iff set is valid allocation set.
 */
#define GenIsValid(set) PointerIsValid(set)

#define GenPointerGetChunk(ptr)	\
					((GenChunk)(((char *)(ptr)) - GEN_CHUNKHDRSZ))
#define GenChunkGetPointer(chk)	\
					((GenPointer)(((char *)(chk)) + GEN_CHUNKHDRSZ))

/*
 * These functions implement the MemoryContext API for Gen contexts.
 */
static void *GenAlloc(MemoryContext context, Size size);
static void GenFree(MemoryContext context, void *pointer);
static void *GenRealloc(MemoryContext context, void *pointer, Size size);
static void GenInit(MemoryContext context);
static void GenReset(MemoryContext context);
static void GenDelete(MemoryContext context);
static Size GenGetChunkSpace(MemoryContext context, void *pointer);
static bool GenIsEmpty(MemoryContext context);
static void GenStats(MemoryContext context, int level, bool print,
			  MemoryContextCounters *totals);

#ifdef MEMORY_CONTEXT_CHECKING
static void GenCheck(MemoryContext context);
#endif

/*
 * This is the virtual function table for Slab contexts.
 */
static MemoryContextMethods GenMethods = {
	GenAlloc,
	GenFree,
	GenRealloc,
	GenInit,
	GenReset,
	GenDelete,
	GenGetChunkSpace,
	GenIsEmpty,
	GenStats
#ifdef MEMORY_CONTEXT_CHECKING
	,GenCheck
#endif
};

/* ----------
 * Debug macros
 * ----------
 */
#ifdef HAVE_ALLOCINFO
#define GenFreeInfo(_cxt, _chunk) \
			fprintf(stderr, "GenFree: %s: %p, %d\n", \
				(_cxt)->header.name, (_chunk), (_chunk)->size)
#define GenAllocInfo(_cxt, _chunk) \
			fprintf(stderr, "GenAlloc: %s: %p, %d\n", \
				(_cxt)->header.name, (_chunk), (_chunk)->size)
#else
#define GenFreeInfo(_cxt, _chunk)
#define GenAllocInfo(_cxt, _chunk)
#endif

#ifdef CLOBBER_FREED_MEMORY

/* Wipe freed memory for debugging purposes */
static void
wipe_mem(void *ptr, size_t size)
{
	VALGRIND_MAKE_MEM_UNDEFINED(ptr, size);
	memset(ptr, 0x7F, size);
	VALGRIND_MAKE_MEM_NOACCESS(ptr, size);
}
#endif

#ifdef MEMORY_CONTEXT_CHECKING
static void
set_sentinel(void *base, Size offset)
{
	char	   *ptr = (char *) base + offset;

	VALGRIND_MAKE_MEM_UNDEFINED(ptr, 1);
	*ptr = 0x7E;
	VALGRIND_MAKE_MEM_NOACCESS(ptr, 1);
}

static bool
sentinel_ok(const void *base, Size offset)
{
	const char *ptr = (const char *) base + offset;
	bool		ret;

	VALGRIND_MAKE_MEM_DEFINED(ptr, 1);
	ret = *ptr == 0x7E;
	VALGRIND_MAKE_MEM_NOACCESS(ptr, 1);

	return ret;
}
#endif

#ifdef RANDOMIZE_ALLOCATED_MEMORY

/*
 * Fill a just-allocated piece of memory with "random" data.  It's not really
 * very random, just a repeating sequence with a length that's prime.  What
 * we mainly want out of it is to have a good probability that two palloc's
 * of the same number of bytes start out containing different data.
 *
 * The region may be NOACCESS, so make it UNDEFINED first to avoid errors as
 * we fill it.  Filling the region makes it DEFINED, so make it UNDEFINED
 * again afterward.  Whether to finally make it UNDEFINED or NOACCESS is
 * fairly arbitrary.  UNDEFINED is more convenient for AllocSetRealloc(), and
 * other callers have no preference.
 */
static void
randomize_mem(char *ptr, size_t size)
{
	static int	save_ctr = 1;
	size_t		remaining = size;
	int			ctr;

	ctr = save_ctr;
	VALGRIND_MAKE_MEM_UNDEFINED(ptr, size);
	while (remaining-- > 0)
	{
		*ptr++ = ctr;
		if (++ctr > 251)
			ctr = 1;
	}
	VALGRIND_MAKE_MEM_UNDEFINED(ptr - size, size);
	save_ctr = ctr;
}
#endif   /* RANDOMIZE_ALLOCATED_MEMORY */


/*
 * Public routines
 */


/*
 * GenContextCreate
 *		Create a new Gen context.
 */
MemoryContext
GenContextCreate(MemoryContext parent,
				 const char *name,
				 Size blockSize)
{
	Gen	set;

	/*
	 * First, validate allocation parameters.  (If we're going to throw an
	 * error, we should do so before the context is created, not after.)  We
	 * somewhat arbitrarily enforce a minimum 1K block size, mostly because
	 * that's what AllocSet does.
	 */
	if (blockSize != MAXALIGN(blockSize) ||
		blockSize < 1024 ||
		!AllocHugeSizeIsValid(blockSize))
		elog(ERROR, "invalid blockSize for memory context: %zu",
			 blockSize);

	/* Do the type-independent part of context creation */
	set = (Gen) MemoryContextCreate(T_GenContext,
									sizeof(GenContext),
									&GenMethods,
									parent,
									name);

	set->blockSize = blockSize;
	set->block = NULL;
	set->blocks = NULL;

	return (MemoryContext) set;
}

/*
 * GenInit
 *		Context-type-specific initialization routine. Simply delegate the
 *		child contexts.
 */
static void
GenInit(MemoryContext context)
{
	/* nothing to do here */
}

/*
 * GenReset
 *		Frees all memory which is allocated in the given set.
 *
 * The code simply frees all the blocks in the context - we don't keep any
 * keeper blocks or anything like that.
 */
static void
GenReset(MemoryContext context)
{
	Gen	set = (Gen) context;
	GenBlock block = set->blocks;

	while (block) {
		GenBlock next = block->next;
		free(block);
		block = next;
	}

	set->block = NULL;
	set->blocks = NULL;
}

/*
 * GenDelete
 *		Frees all memory which is allocated in the given set, in preparation
 *		for deletion of the set. We simply call GenReset() which does all the
 *		dirty work.
 */
static void
GenDelete(MemoryContext context)
{
	/* just reset (although not really necessary) */
	GenReset(context);
}

/*
 * GenAlloc
 *		Returns pointer to allocated memory of given size or NULL if
 *		request could not be completed; memory is added to the set.
 *
 * No request may exceed:
 *		MAXALIGN_DOWN(SIZE_MAX) - GEN_BLOCKHDRSZ - GEN_CHUNKHDRSZ
 * All callers use a much-lower limit.
 */
static void *
GenAlloc(MemoryContext context, Size size)
{
	Gen			set = (Gen) context;
	GenBlock	block;
	GenChunk	chunk;
	Size		chunk_size = MAXALIGN(size);

	/* is it an over-sized chunk? if yes, allocate special block */
	if (chunk_size > set->blockSize / 8)
	{
		Size blksize = chunk_size + GEN_BLOCKHDRSZ + GEN_CHUNKHDRSZ;
		block = (GenBlock) malloc(blksize);
		if (block == NULL)
			return NULL;

		block->gen = set;

		/* block with a single (used) chunk */
		block->nchunks = 1;
		block->nfree = 0;

		block->next = NULL;
		block->prev = NULL;

		/* the block is completely full */
		block->freeptr = block->endptr = ((char *) block) + blksize;

		chunk = (GenChunk) (((char *) block) + GEN_BLOCKHDRSZ);
		chunk->gen = set;
		chunk->size = chunk_size;

#ifdef MEMORY_CONTEXT_CHECKING
		/* Valgrind: Will be made NOACCESS below. */
		chunk->requested_size = size;
		/* set mark to catch clobber of "unused" space */
		if (size < chunk_size)
			set_sentinel(GenChunkGetPointer(chunk), size);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* fill the allocated space with junk */
		randomize_mem((char *) GenChunkGetPointer(chunk), size);
#endif

		/* add the block to the list of allocated blocks */
		block->next = set->blocks;

		if (set->blocks != NULL)
			set->blocks->prev = block;

		set->blocks = block;

		GenAllocInfo(set, chunk);

		/*
		 * Chunk header public fields remain DEFINED.  The requested
		 * allocation itself can be NOACCESS or UNDEFINED; our caller will
		 * soon make it UNDEFINED.  Make extra space at the end of the chunk,
		 * if any, NOACCESS.
		 */
		VALGRIND_MAKE_MEM_NOACCESS((char *) chunk + GEN_CHUNK_PUBLIC,
						 chunk_size + GEN_CHUNKHDRSZ - GEN_CHUNK_PUBLIC);

		return GenChunkGetPointer(chunk);
	}

	/*
	 * Not an over-sized chunk. Is there enough space on the current block?
	 * If not, allocate a new "regular" block.
	 */
	block = set->block;

	if ((block == NULL) ||
		(block->endptr - block->freeptr) < GEN_CHUNKHDRSZ + chunk_size)
	{
		Size	blksize = set->blockSize;

		block = (GenBlock) malloc(blksize);

		if (block == NULL)
			return NULL;

		block->nchunks = 0;
		block->nfree = 0;
		block->gen = set;

		block->next = NULL;
		block->prev = NULL;

		block->freeptr = ((char *) block) + GEN_BLOCKHDRSZ;
		block->endptr = ((char *) block) + blksize;

		/* Mark unallocated space NOACCESS. */
		VALGRIND_MAKE_MEM_NOACCESS(block->freeptr,
								   blksize - GEN_BLOCKHDRSZ);

		/* add it to the doubly-linked list of blocks */
		block->next = set->blocks;

		if (set->blocks != NULL)
			set->blocks->prev = block;

		set->blocks = block;

		/* and use it as the current allocation block */
		set->block = block;
	}

	/* we're supposed to have a block with enough free space now */
	Assert(block != NULL);
	Assert((block->endptr - block->freeptr) >= GEN_CHUNKHDRSZ + chunk_size);

	chunk = (GenChunk)block->freeptr;

	block->nchunks += 1;
	block->freeptr += (GEN_CHUNKHDRSZ + chunk_size);

	chunk->gen = set;
	chunk->block = block;
	chunk->size = chunk_size;

#ifdef MEMORY_CONTEXT_CHECKING
	/* Valgrind: Free list requested_size should be DEFINED. */
	chunk->requested_size = size;
	VALGRIND_MAKE_MEM_NOACCESS(&chunk->requested_size,
							   sizeof(chunk->requested_size));
	/* set mark to catch clobber of "unused" space */
	if (size < chunk->size)
		set_sentinel(GenChunkGetPointer(chunk), size);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
	/* fill the allocated space with junk */
	randomize_mem((char *) GenChunkGetPointer(chunk), size);
#endif

	GenAllocInfo(set, chunk);
	return GenChunkGetPointer(chunk);
}

/*
 * GenFree
 *		Update number of chunks on the block, and if all chunks on the block
 *		are freeed then discard the block.
 */
static void
GenFree(MemoryContext context, void *pointer)
{
	Gen			set = (Gen)context;
	GenChunk	chunk = GenPointerGetChunk(pointer);
	GenBlock	block = chunk->block;

#ifdef MEMORY_CONTEXT_CHECKING
	VALGRIND_MAKE_MEM_DEFINED(&chunk->requested_size,
							  sizeof(chunk->requested_size));
	/* Test for someone scribbling on unused space in chunk */
	if (chunk->requested_size < chunk->size)
		if (!sentinel_ok(pointer, chunk->requested_size))
			elog(WARNING, "detected write past chunk end in %s %p",
				 set->header.name, chunk);
#endif

#ifdef CLOBBER_FREED_MEMORY
	wipe_mem(pointer, chunk->size);
#endif

#ifdef MEMORY_CONTEXT_CHECKING
	/* Reset requested_size to 0 in chunks that are on freelist */
	chunk->requested_size = 0;
#endif

	block->nfree += 1;

	Assert(block->nchunks > 0);
	Assert(block->nfree <= block->nchunks);

	/* If there are still allocated chunks on the block, we're done. */
	if (block->nfree < block->nchunks)
		return;

	/*
	 * The block is empty, so let's get rid of it. First remove it from the
	 * list of blocks, then return it to malloc().
	 */
	if (block->prev != NULL)
		block->prev->next = block->next;

	if (block->next != NULL)
		block->next->prev = block->prev;

	if (set->blocks == block)
		set->blocks = block->next;

	/* Also make sure the block is not marked as the current block. */
	if (set->block == block)
		set->block = NULL;

	free(block);
}

/*
 * GenRealloc
 *		When handling repalloc, we simply allocate a new chunk, copy the data
 *		and discard the old one. The only exception is when the new size fits
 *		into the old chunk - in that case we just update chunk header.
 */
static void *
GenRealloc(MemoryContext context, void *pointer, Size size)
{
	Gen			set = (Gen) context;
	GenChunk	chunk = GenPointerGetChunk(pointer);
	Size		oldsize = chunk->size;
	GenPointer	newPointer;

#ifdef MEMORY_CONTEXT_CHECKING
	VALGRIND_MAKE_MEM_DEFINED(&chunk->requested_size,
							  sizeof(chunk->requested_size));
	/* Test for someone scribbling on unused space in chunk */
	if (chunk->requested_size < oldsize)
		if (!sentinel_ok(pointer, chunk->requested_size))
			elog(WARNING, "detected write past chunk end in %s %p",
				 set->header.name, chunk);
#endif

	/*
	 * Maybe the allocated area already is >= the new size.  (In particular,
	 * we always fall out here if the requested size is a decrease.)
	 *
	 * XXX GEN context is not using doubling (unlike AllocSet), so most
	 * repalloc() calls will end up in the palloc/memcpy/pfree branch. So
	 * perhaps we should mark this with unlikely().
	 */
	if (oldsize >= size)
	{
#ifdef MEMORY_CONTEXT_CHECKING
		Size		oldrequest = chunk->requested_size;

#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* We can only fill the extra space if we know the prior request */
		if (size > oldrequest)
			randomize_mem((char *) pointer + oldrequest,
						  size - oldrequest);
#endif

		chunk->requested_size = size;
		VALGRIND_MAKE_MEM_NOACCESS(&chunk->requested_size,
								   sizeof(chunk->requested_size));

		/*
		 * If this is an increase, mark any newly-available part UNDEFINED.
		 * Otherwise, mark the obsolete part NOACCESS.
		 */
		if (size > oldrequest)
			VALGRIND_MAKE_MEM_UNDEFINED((char *) pointer + oldrequest,
										size - oldrequest);
		else
			VALGRIND_MAKE_MEM_NOACCESS((char *) pointer + size,
									   oldsize - size);

		/* set mark to catch clobber of "unused" space */
		if (size < oldsize)
			set_sentinel(pointer, size);
#else							/* !MEMORY_CONTEXT_CHECKING */

		/*
		 * We don't have the information to determine whether we're growing
		 * the old request or shrinking it, so we conservatively mark the
		 * entire new allocation DEFINED.
		 */
		VALGRIND_MAKE_MEM_NOACCESS(pointer, oldsize);
		VALGRIND_MAKE_MEM_DEFINED(pointer, size);
#endif

		return pointer;
	}

	/* allocate new chunk */
	newPointer = GenAlloc((MemoryContext) set, size);

	/* leave immediately if request was not completed */
	if (newPointer == NULL)
		return NULL;

	/*
	 * GenSetAlloc() just made the region NOACCESS.  Change it to
	 * UNDEFINED for the moment; memcpy() will then transfer definedness
	 * from the old allocation to the new.  If we know the old allocation,
	 * copy just that much.  Otherwise, make the entire old chunk defined
	 * to avoid errors as we copy the currently-NOACCESS trailing bytes.
	 */
	VALGRIND_MAKE_MEM_UNDEFINED(newPointer, size);
#ifdef MEMORY_CONTEXT_CHECKING
	oldsize = chunk->requested_size;
#else
	VALGRIND_MAKE_MEM_DEFINED(pointer, oldsize);
#endif

	/* transfer existing data (certain to fit) */
	memcpy(newPointer, pointer, oldsize);

	/* free old chunk */
	GenFree((MemoryContext) set, pointer);

	return newPointer;
}

/*
 * GenGetChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 */
static Size
GenGetChunkSpace(MemoryContext context, void *pointer)
{
	GenChunk	chunk = GenPointerGetChunk(pointer);

	return chunk->size + GEN_CHUNKHDRSZ;
}

/*
 * GenIsEmpty
 *		Is an Gen empty of any allocated space?
 */
static bool
GenIsEmpty(MemoryContext context)
{
	Gen set = (Gen)context;

	return (set->blocks == NULL);
}

/*
 * GenStats
 *		Compute stats about memory consumption of an Gen.
 *
 * level: recursion level (0 at top level); used for print indentation.
 * print: true to print stats to stderr.
 * totals: if not NULL, add stats about this Slab into *totals.
 *
 * FIXME not really implemented yet
 */
static void
GenStats(MemoryContext context, int level, bool print,
			  MemoryContextCounters *totals)
{
	Gen		set = (Gen) context;

	if (print)
	{
		int			i;

		for (i = 0; i < level; i++)
			fprintf(stderr, "  ");
		fprintf(stderr, "%s\n", set->header.name);
	}
}


#ifdef MEMORY_CONTEXT_CHECKING

/*
 * GenCheck
 *		Walk through chunks and check consistency of memory.
 *
 * NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
 * find yourself in an infinite loop when trouble occurs, because this
 * routine will be entered again when elog cleanup tries to release memory!
 */
static void
GenCheck(MemoryContext context)
{
	// FIXME
}

#endif   /* MEMORY_CONTEXT_CHECKING */
