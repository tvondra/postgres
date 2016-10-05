/*-------------------------------------------------------------------------
 *
 * slab.c
 *	  SLAB allocator definitions.
 *
 * SLAB is a custom memory context MemoryContext implementation designed for
 * cases of equally-sized objects.
 *
 *
 * Portions Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/slab.c
 *
 *
 *	The constant allocation size allows significant simplification and various
 *	optimizations that are not possible in AllocSet. Firstly, we can get rid
 *	of the doubling and carve the blocks into chunks of exactly the right size
 *	(plus alignment), now wasting memory.
 *
 *	The information about free chunks is maintained both at the block level and
 *	global (context) level. This is possible as the chunk size (and thus also
 *	the number of chunks per block) is fixed.
 *
 *	Each block includes a simple bitmap tracking which chunks are used/free.
 *	This makes it trivial to check if all chunks on the block are free, and
 *	eventually free the whole block (which is almost impossible with AllocSet,
 *	as it stores free chunks from all blocks in a single global freelist).
 *
 *	At the context level, we use 'freelist' array to track blocks grouped by
 *	number of free chunks. For example freelist[0] is a list of completely full
 *	blocks, freelist[1] is a block with a single free chunk, etc.
 *
 *	This also allows various optimizations - for example when searching for
 *	free chunk, we the allocator reuses space from the most full blocks first,
 *	in the hope that some of the less full blocks will get completely empty
 *	(and returned back to the OS).
 *
 *	For each block, we maintain pointer to the first free chunk - this is quite
 *	cheap and allows us to skip all the preceding used chunks, eliminating
 *	a significant number of lookups in many common usage patters. In the worst
 *	case this performs as if the pointer was not maintained.
 *
 *
 *	About CLOBBER_FREED_MEMORY:
 *
 *	If this symbol is defined, all freed memory is overwritten with 0x7F's.
 *	This is useful for catching places that reference already-freed memory.
 *
 *	About MEMORY_CONTEXT_CHECKING:
 *
 *	Since we usually round request sizes up to the next power of 2, there
 *	is often some unused space immediately after a requested data area.
 *	Thus, if someone makes the common error of writing past what they've
 *	requested, the problem is likely to go unnoticed ... until the day when
 *	there *isn't* any wasted space, perhaps because of different memory
 *	alignment on a new platform, or some other effect.  To catch this sort
 *	of problem, the MEMORY_CONTEXT_CHECKING option stores 0x7E just beyond
 *	the requested space whenever the request is less than the actual chunk
 *	size, and verifies that the byte is undamaged when the chunk is freed.
 *
 *
 *	About USE_VALGRIND and Valgrind client requests:
 *
 *	Valgrind provides "client request" macros that exchange information with
 *	the host Valgrind (if any).  Under !USE_VALGRIND, memdebug.h stubs out
 *	currently-used macros.
 *
 *	When running under Valgrind, we want a NOACCESS memory region both before
 *	and after the allocation.  The chunk header is tempting as the preceding
 *	region, but mcxt.c expects to able to examine the standard chunk header
 *	fields.  Therefore, we use, when available, the requested_size field and
 *	any subsequent padding.  requested_size is made NOACCESS before returning
 *	a chunk pointer to a caller.  However, to reduce client request traffic,
 *	it is kept DEFINED in chunks on the free list.
 *
 *	The rounded-up capacity of the chunk usually acts as a post-allocation
 *	NOACCESS region.  If the request consumes precisely the entire chunk,
 *	there is no such region; another chunk header may immediately follow.  In
 *	that case, Valgrind will not detect access beyond the end of the chunk.
 *
 *	See also the cooperating Valgrind client requests in mcxt.c.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/memdebug.h"
#include "utils/memutils.h"


#define SLAB_BLOCKHDRSZ	MAXALIGN(sizeof(SlabBlockData))
#define SLAB_CHUNKHDRSZ	MAXALIGN(sizeof(SlabChunkData))

/* Portion of SLAB_CHUNKHDRSZ examined outside slab.c. */
#define SLAB_CHUNK_PUBLIC	\
	(offsetof(SlabChunkData, size) + sizeof(Size))

/* Portion of SLAB_CHUNKHDRSZ excluding trailing padding. */
#ifdef MEMORY_CONTEXT_CHECKING
#define SLAB_CHUNK_USED	\
	(offsetof(SlabChunkData, requested_size) + sizeof(Size))
#else
#define SLAB_CHUNK_USED	\
	(offsetof(SlabChunkData, size) + sizeof(Size))
#endif

typedef struct SlabBlockData *SlabBlock;		/* forward reference */
typedef struct SlabChunkData *SlabChunk;

/*
 * SlabPointer
 *		Aligned pointer which may be a member of an allocation set.
 */
typedef void *SlabPointer;

/*
 * SlabContext is a specialized implementation of MemoryContext.
 */
typedef struct SlabContext
{
	MemoryContextData header;	/* Standard memory-context fields */
	/* Allocation parameters for this context: */
	Size		chunkSize;		/* chunk size */
	Size		fullChunkSize;	/* chunk size including header and alignment */
	Size		blockSize;		/* block size */
	int			chunksPerBlock;	/* number of chunks per block */
	int			minFreeCount;	/* min number of free chunks in any block */
	int			nblocks;		/* number of blocks allocated */
	bool		autodestruct;	/* destruct after freeing the last block */
	/* Info about storage allocated in this context: */
	SlabBlock	freelist[1];	/* free lists (block-level) */
} SlabContext;

typedef SlabContext *Slab;

/*
 * SlabBlockData
 *		Structure of a single block in SLAB allocator.
 *
 * slab: context owning this block
 * prev, next: used for doubly-linked list of blocks in global freelist
 * nfree: number of free chunks in this block
 * firstFreeChunk: pointer to the first free chunk
 * bitmapptr: pointer to the free bitmap (tracking free chunks)
 */
typedef struct SlabBlockData
{
	Slab		slab;			/* slab that owns this block */
	SlabBlock	prev;			/* previous block in slab's block list */
	SlabBlock	next;			/* next block in slab's blocks list */
	int			nfree;			/* number of free chunks */
	int			firstFreeChunk;	/* index of the first free chunk in the block */
	char	   *bitmapptr;		/* pointer to free bitmap */
}	SlabBlockData;

/*
 * SlabChunk
 *		The prefix of each piece of memory in an SlabBlock
 *
 * NB: this MUST match StandardChunkHeader as defined by utils/memutils.h.
 * However it's possible to fields in front of the StandardChunkHeader fields,
 * which is used to add pointer to the block.
 */
typedef struct SlabChunkData
{
	/* block owning this chunk */
	void	   *block;
	/* slab is the owning slab context */
	void	   *slab;
	/* size is always the size of the usable space in the chunk */
	Size		size;
#ifdef MEMORY_CONTEXT_CHECKING
	/* when debugging memory usage, also store actual requested size */
	/* this is zero in a free chunk */
	Size		requested_size;
#endif
}	SlabChunkData;


/*
 * SlabIsValid
 *		True iff set is valid allocation set.
 */
#define SlabIsValid(set) PointerIsValid(set)

#define SlabPointerGetChunk(ptr)	\
					((SlabChunk)(((char *)(ptr)) - SLAB_CHUNKHDRSZ))
#define SlabChunkGetPointer(chk)	\
					((SlabPointer)(((char *)(chk)) + SLAB_CHUNKHDRSZ))

/*
 * These functions implement the MemoryContext API for Slab contexts.
 */
static void *SlabAlloc(MemoryContext context, Size size);
static void SlabFree(MemoryContext context, void *pointer);
static void *SlabRealloc(MemoryContext context, void *pointer, Size size);
static void SlabInit(MemoryContext context);
static void SlabReset(MemoryContext context);
static void SlabDelete(MemoryContext context);
static Size SlabGetChunkSpace(MemoryContext context, void *pointer);
static bool SlabIsEmpty(MemoryContext context);
static void SlabStats(MemoryContext context, int level, bool print,
			  MemoryContextCounters *totals);

#ifdef MEMORY_CONTEXT_CHECKING
static void SlabCheck(MemoryContext context);
#endif

/*
 * This is the virtual function table for Slab contexts.
 */
static MemoryContextMethods SlabMethods = {
	SlabAlloc,
	SlabFree,
	SlabRealloc,
	SlabInit,
	SlabReset,
	SlabDelete,
	SlabGetChunkSpace,
	SlabIsEmpty,
	SlabStats
#ifdef MEMORY_CONTEXT_CHECKING
	,SlabCheck
#endif
};

/* ----------
 * Debug macros
 * ----------
 */
#ifdef HAVE_ALLOCINFO
#define SlabFreeInfo(_cxt, _chunk) \
			fprintf(stderr, "SlabFree: %s: %p, %d\n", \
				(_cxt)->header.name, (_chunk), (_chunk)->size)
#define SlabAllocInfo(_cxt, _chunk) \
			fprintf(stderr, "SlabAlloc: %s: %p, %d\n", \
				(_cxt)->header.name, (_chunk), (_chunk)->size)
#else
#define SlabFreeInfo(_cxt, _chunk)
#define SlabAllocInfo(_cxt, _chunk)
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
 * fairly arbitrary.  UNDEFINED is more convenient for SlabRealloc(), and
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
 * SlabContextCreate
 *		Create a new Slab context.
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (for debugging --- string will be copied)
 * blockSize: allocation block size
 * chunkSize: allocation chunk size
 */
MemoryContext
SlabContextCreate(MemoryContext parent,
					  const char *name,
					  Size blockSize,
					  Size chunkSize)
{
	int		chunksPerBlock;
	Size	fullChunkSize;
	Slab	set;

	/* chunk, including SLAB header (both addresses nicely aligned) */
	fullChunkSize = MAXALIGN(sizeof(SlabChunkData) + MAXALIGN(chunkSize));

	/* make sure the block can store at least one chunk (with 1B for a bitmap)? */
	if (blockSize - sizeof(SlabChunkData) < fullChunkSize + 1)
		elog(ERROR, "block size %ld for slab is too small for chunks %ld",
					blockSize, chunkSize);

	/* so how many chunks can we fit into a block, including header and bitmap? */
	chunksPerBlock
		=  (8 * (blockSize - sizeof(SlabBlockData)) - 7) / (8 * fullChunkSize + 1);

	/* if we can't fit at least one chunk into the block, we're hosed */
	Assert(chunksPerBlock > 0);

	/* make sure the chunks (and bitmap) actually fit on the block  */
	Assert(fullChunkSize * chunksPerBlock + ((chunksPerBlock + 7) / 8) + sizeof(SlabBlockData) <= blockSize);

	/* Do the type-independent part of context creation */
	set = (Slab) MemoryContextCreate(T_SlabContext,
									 /* allocate context and freelist at once */
									 (offsetof(SlabContext, freelist) + sizeof(SlabChunk) * (chunksPerBlock + 1)),
									 &SlabMethods,
									 parent,
									 name);

	set->blockSize = blockSize;
	set->chunkSize = chunkSize;
	set->fullChunkSize = fullChunkSize;
	set->chunksPerBlock = chunksPerBlock;
	set->nblocks = 0;
	set->minFreeCount = 0;
	set->autodestruct = false;

	return (MemoryContext) set;
}

/*
 * SlabInit
 *		Context-type-specific initialization routine. SlabContext does not
 *		need anything extra, at this moment.
 */
static void
SlabInit(MemoryContext context)
{
	/*
	 * Since MemoryContextCreate already zeroed the context node, we don't
	 * have to do anything here: it's already OK.
	 */
}

/*
 * SlabReset
 *		Frees all memory which is allocated in the given set.
 *
 * The code simply frees all the blocks in the context - we don't keep any
 * keeper blocks or anything like that.
 */
static void
SlabReset(MemoryContext context)
{
	int		i;
	Slab	set = (Slab) context;

	AssertArg(SlabIsValid(set));

#ifdef MEMORY_CONTEXT_CHECKING
	/* Check for corruption and leaks before freeing */
	SlabCheck(context);
#endif

	/* walk over freelists and free the blocks */
	for (i = 0; i <= set->chunksPerBlock; i++)
	{
		SlabBlock block = set->freelist[i];
		set->freelist[i] = NULL;

		while (block != NULL)
		{
			SlabBlock	next = block->next;

			/* Normal case, release the block */
#ifdef CLOBBER_FREED_MEMORY
			wipe_mem(block, set->blockSize);
#endif
			free(block);
			set->nblocks--;

			block = next;
		}
	}

	set->minFreeCount = 0;
	set->autodestruct = false;

	Assert(set->nblocks == 0);
}

/*
 * SlabDelete
 *		Frees all memory which is allocated in the given set, in preparation
 *		for deletion of the set. We simply call SlabReset().
 */
static void
SlabDelete(MemoryContext context)
{
	/* just reset the context */
	SlabReset(context);
}

/* operations on the freelist - adding/removing/moving blocks */
static void
remove_from_freelist(Slab set, SlabBlock block, int nfree_old)
{
	/* either it has a previous block, or it's the first block in list */
	if (block->prev)
		block->prev->next = block->next;
	else
		set->freelist[nfree_old] = block->next;

	/* if it has a next block, update it too */
	if (block->next)
		block->next->prev = block->prev;

	block->prev = NULL;
	block->next = NULL;
}

static void
add_to_freelist(Slab set, SlabBlock block)
{
	/* otherwise add it to the proper freelist bin */
	if (set->freelist[block->nfree])
		set->freelist[block->nfree]->prev = block;

	block->next = set->freelist[block->nfree];
	set->freelist[block->nfree] = block;
}

static void
move_in_freelist(Slab set, SlabBlock block, int nfree_old)
{
	remove_from_freelist(set, block, nfree_old);
	add_to_freelist(set, block);
}


/*
 * SlabAlloc
 *		Returns pointer to allocated memory of given size or NULL if
 *		request could not be completed; memory is added to the set.
 *
 * No request may exceed:
 *		MAXALIGN_DOWN(SIZE_MAX) - SLAB_BLOCKHDRSZ - SLAB_CHUNKHDRSZ
 * All callers use a much-lower limit.
 */
static void *
SlabAlloc(MemoryContext context, Size size)
{
	Slab	set = (Slab) context;
	SlabBlock	block;
	SlabChunk	chunk;
	int			idx;

	AssertArg(SlabIsValid(set));

	Assert((set->minFreeCount >= 0) && (set->minFreeCount < set->chunksPerBlock));

	/* make sure we only allow correct request size */
	if (size != set->chunkSize)
		elog(ERROR, "unexpected alloc chunk size %ld (expected %ld)",
					size, set->chunkSize);

	/*
	 * If there are no free chunks in any existing block, create a new block
	 * and put it to the last freelist bucket.
	 */
	if (set->minFreeCount == 0)
	{
		block = (SlabBlock)malloc(set->blockSize);

		if (block == NULL)
			return NULL;

		block->slab = set;
		block->nfree = set->chunksPerBlock;
		block->prev = NULL;
		block->next = NULL;
		block->firstFreeChunk = 0;

		/* the free bitmap is placed at the end */
		block->bitmapptr
			= ((char *) block) + set->blockSize - ((set->chunksPerBlock + 7) / 8);

		/* we need to reset the free bitmap */
		memset(block->bitmapptr, 0, ((set->chunksPerBlock + 7) / 8));

		/*
		 * And add it to the last freelist with all chunks empty (we know
		 * there are no blocks in the freelist, otherwise we wouldn't need
		 * a new block.
		 */
		set->freelist[set->chunksPerBlock] = block;
		set->minFreeCount = set->chunksPerBlock;
		set->nblocks += 1;
	}

	/* grab the block from the freelist (even the new block is there) */
	block = set->freelist[set->minFreeCount];

	/* make sure we actually got a valid block, with matching nfree */
	Assert(block != NULL);
	Assert(set->minFreeCount == block->nfree);
	Assert(block->nfree > 0);

	Assert((char*)block < block->bitmapptr);
	Assert((char*)block + set->blockSize > block->bitmapptr);

	/* we know the first free chunk */
	idx = block->firstFreeChunk;

	/* make sure the chunk index is valid, and that it's marked as empty */
	Assert((idx >= 0) && (idx < set->chunksPerBlock));
	Assert(!((block->bitmapptr[idx/8] & (0x01 << (idx % 8)))));

	/* mark the chunk as used (set 1 to the bit) */
	block->bitmapptr[idx/8] |= (0x01 << (idx % 8));

	/* compute the chunk location block start (after the block header) */
	chunk = (SlabChunk) ((char*)block + sizeof(SlabBlockData)
									  + (idx * set->fullChunkSize));

	/*
	 * update the block nfree count, and also the minFreeCount as we've
	 * decreased nfree for a block with the minimum count
	 */
	block->nfree--;
	set->minFreeCount = block->nfree;

	/* but we need to find the next one, for the next alloc call (unless the
	 * block just got full, in that case simply set it to -1 */
	if (block->nfree == 0)
		block->firstFreeChunk = set->chunksPerBlock;
	else
	{
		/* look for the next free chunk in the block, after the first one */
		while ((++block->firstFreeChunk) < set->chunksPerBlock)
		{
			int byte = block->firstFreeChunk / 8;
			int bit  = block->firstFreeChunk % 8;

			/* stop when you find 0 (unused chunk) */
			if (! (block->bitmapptr[byte] & (0x01 << bit)))
				break;
		}

		/* must have found the free chunk */
		Assert(block->firstFreeChunk != set->chunksPerBlock);
	}

	/* move the block to the right place in the freelist */
	move_in_freelist(set, block, (block->nfree + 1));

	/* but if the minimum is 0, we need to look for a new one */
	if (set->minFreeCount == 0)
		for (idx = 1; idx <= set->chunksPerBlock; idx++)
			if (set->freelist[idx])
			{
				set->minFreeCount = idx;
				break;
			}

	if (set->minFreeCount == set->chunksPerBlock)
		set->minFreeCount = 0;

	/* Prepare to initialize the chunk header. */
	VALGRIND_MAKE_MEM_UNDEFINED(chunk, SLAB_CHUNK_USED);

	chunk->slab = (void *) set;
	chunk->block = (void *) block;
	chunk->size = MAXALIGN(size);

#ifdef MEMORY_CONTEXT_CHECKING
	chunk->requested_size = size;
	VALGRIND_MAKE_MEM_NOACCESS(&chunk->requested_size,
							   sizeof(chunk->requested_size));
	/* set mark to catch clobber of "unused" space */
	if (size < chunk->size)
		set_sentinel(SlabChunkGetPointer(chunk), size);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
	/* fill the allocated space with junk */
	randomize_mem((char *) SlabChunkGetPointer(chunk), size);
#endif

	SlabAllocInfo(set, chunk);
	return SlabChunkGetPointer(chunk);
}

/*
 * SlabFree
 *		Frees allocated memory; memory is removed from the set.
 */
static void
SlabFree(MemoryContext context, void *pointer)
{
	int		idx;
	Slab	set = (Slab) context;
	SlabChunk	chunk = SlabPointerGetChunk(pointer);
	SlabBlock	block = chunk->block;

	SlabFreeInfo(set, chunk);

#ifdef MEMORY_CONTEXT_CHECKING
	VALGRIND_MAKE_MEM_DEFINED(&chunk->requested_size,
							  sizeof(chunk->requested_size));
	/* Test for someone scribbling on unused space in chunk */
	if (chunk->requested_size < chunk->size)
		if (!sentinel_ok(pointer, chunk->requested_size))
			elog(WARNING, "detected write past chunk end in %s %p",
				 set->header.name, chunk);
#endif

	/* compute index wrt to block start */
	idx = ((char*)chunk - ((char*)block + sizeof(SlabBlockData))) / set->fullChunkSize;

	Assert((block->bitmapptr[idx/8] & (0x01 << (idx % 8))));

	/* mark the chunk as unused (set 0 to the bit), and update block nfree count */
	block->bitmapptr[idx/8] ^= (0x01 << (idx % 8));
	block->nfree++;
	block->firstFreeChunk = Min(block->firstFreeChunk, idx);

	Assert(block->nfree > 0);
	Assert(block->nfree <= set->chunksPerBlock);

#ifdef CLOBBER_FREED_MEMORY
	wipe_mem(pointer, chunk->size);
#endif

#ifdef MEMORY_CONTEXT_CHECKING
	/* Reset requested_size to 0 in chunks that are on freelist */
	chunk->requested_size = 0;
#endif

	/* now decide what to do with the block */

	/*
	 * See if we need to update the minFreeCount field for the set - we only
	 * need to do that if the block had that number of free chunks before we
	 * freed one. In that case, we check if there still are blocks with that
	 * number of free chunks - we can simply check if the chunk has siblings.
	 * Otherwise we simply increment the value by one, as the new block is
	 * still the one with minimum free chunks (even without the one chunk).
	 */
	if (set->minFreeCount == (block->nfree-1))
		if ((block->prev == NULL) && (block->next == NULL)) /* no other blocks */
		{
			/* but if we made the block entirely free, we'll free it */
			if (block->nfree == set->chunksPerBlock)
				set->minFreeCount = 0;
			else
				set->minFreeCount++;
		}

	/* remove the block from a freelist */
	remove_from_freelist(set, block, block->nfree-1);

	/* If the block is now completely empty, free it. */
	if (block->nfree == set->chunksPerBlock)
	{
		free(block);
		set->nblocks--;
	}
	else
		add_to_freelist(set, block);

	Assert(set->nblocks >= 0);

	/*
	 * If we've just released the last block in the context, destruct it.
	 *
	 * XXX But don't do that if the context has children.
	 */
	if (set->autodestruct && (set->nblocks == 0) && (context->firstchild == NULL))
		MemoryContextDelete(context);
}

/*
 * SlabRealloc
 *		As Slab is designed for allocating equally-sized chunks of memory, it
 *		can't really do an actual realloc. However we try to be gentle and
 *		allow calls with exactly the same size as in that case we can simply
 *		return the same chunk. When the size differs, we fail with assert
 *		failure or return NULL.
 *
 *	XXX We might be even gentler and allow cases when (size < chunkSize).
 */
static void *
SlabRealloc(MemoryContext context, void *pointer, Size size)
{
	Slab	set = (Slab)context;

	/* can't do actual realloc with slab, but let's try to be gentle */
	if (size == set->chunkSize)
		return pointer;

	elog(ERROR, "slab allocator does not support realloc()");
}

/*
 * SlabGetChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 */
static Size
SlabGetChunkSpace(MemoryContext context, void *pointer)
{
	SlabChunk	chunk = SlabPointerGetChunk(pointer);

	return chunk->size + SLAB_CHUNKHDRSZ;
}

/*
 * SlabIsEmpty
 *		Is an Slab empty of any allocated space?
 */
static bool
SlabIsEmpty(MemoryContext context)
{
	Slab		set = (Slab)context;
	return (set->nblocks == 0);
}

/*
 * SlabStats
 *		Compute stats about memory consumption of an Slab.
 *
 * level: recursion level (0 at top level); used for print indentation.
 * print: true to print stats to stderr.
 * totals: if not NULL, add stats about this Slab into *totals.
 */
static void
SlabStats(MemoryContext context, int level, bool print,
			  MemoryContextCounters *totals)
{
	Slab		set = (Slab) context;
	Size		nblocks = 0;
	Size		freechunks = 0;
	Size		totalspace = 0;
	Size		freespace = 0;
	int			i;

	for (i = 0; i <= set->chunksPerBlock; i++)
	{
		SlabBlock block = set->freelist[i];
		while (block != NULL)
		{
			nblocks++;
			totalspace += set->blockSize;
			freespace += set->fullChunkSize * block->nfree;
			freechunks += block->nfree;
			block = block->next;
		}
	}

	if (print)
	{
		int			i;

		for (i = 0; i < level; i++)
			fprintf(stderr, "  ");
		fprintf(stderr,
			"%s: %zu total in %zd blocks; %zu free (%zd chunks); %zu used; autodestruct %d\n",
				set->header.name, totalspace, nblocks, freespace, freechunks,
				totalspace - freespace, set->autodestruct);
	}

	if (totals)
	{
		totals->nblocks += nblocks;
		totals->freechunks += freechunks;
		totals->totalspace += totalspace;
		totals->freespace += freespace;
	}
}

void
SlabAutodestruct(MemoryContext context)
{
	Slab	set = (Slab)context;

	Assert(IsA(set, SlabContext));

	set->autodestruct = true;
}

#ifdef MEMORY_CONTEXT_CHECKING

/*
 * SlabCheck
 *		Walk through chunks and check consistency of memory.
 *
 * NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
 * find yourself in an infinite loop when trouble occurs, because this
 * routine will be entered again when elog cleanup tries to release memory!
 */
static void
SlabCheck(MemoryContext context)
{
	int i;
	Slab	slab = (Slab) context;
	char	   *name = slab->header.name;

	for (i = 0; i <= slab->chunksPerBlock; i++)
	{
		int j, nfree;
		SlabBlock block = slab->freelist[i];

		/* no entries in this freelist slot */
		if (! block)
			continue;

		if (block->slab != (void *) slab)
			elog(WARNING, "problem in slab %s: bogus slab link in block %p",
				 name, block);

		/*
		 * Make sure the number of free chunks (in the block header) matches
		 * position in the freelist.
		 */
		if (block->nfree != i)
			elog(WARNING, "problem in slab %s: number of free chunks %d in block %p does not match freelist %d",
				 name, block->nfree, block, i);

		/*
		 * Now walk through the chunks, count the free ones and also perform
		 * some additional checks for the used ones.
		 */

		nfree = 0;
		for (j = 0; j <= slab->chunksPerBlock; j++)
		{
			/* non-zero bit in the bitmap means chunk the chunk is used */
			if ((block->bitmapptr[j/8] & (0x01 << (j % 8))) != 0)
			{
				SlabChunk chunk = (SlabChunk) ((char*)block + sizeof(SlabBlockData)
								  + (j * slab->fullChunkSize));

				VALGRIND_MAKE_MEM_DEFINED(&chunk->requested_size,
										  sizeof(chunk->requested_size));

				/* we're in a no-freelist branch */
				VALGRIND_MAKE_MEM_NOACCESS(&chunk->requested_size,
										   sizeof(chunk->requested_size));

				/* chunks have both block and slab pointers, so check both of them */

				if (chunk->block != block)
					elog(WARNING, "problem in slab %s: bogus block link in block %p, chunk %p",
						 name, block, chunk);

				if (chunk->slab != slab)
					elog(WARNING, "problem in slab %s: bogus slab link in block %p, chunk %p",
						 name, block, chunk);

				/* now make sure the chunk size is correct */
				if (chunk->size != MAXALIGN(slab->chunkSize))
					elog(WARNING, "problem in slab %s: bogus chunk size in block %p, chunk %p",
						 name, block, chunk);

#ifdef MEMORY_CONTEXT_CHECKING
				/* now make sure the chunk size is correct */
				if (chunk->size != slab->chunkSize)
					elog(WARNING, "problem in slab %s: bogus chunk requested size in block %p, chunk %p",
						 name, block, chunk);

				/* there might be sentinel (thanks to alignment) */
				if (chunk->requested_size < chunk->size &&
					!sentinel_ok(chunk, SLAB_CHUNKHDRSZ + chunk->requested_size))
					elog(WARNING, "problem in slab %s: detected write past chunk end in block %p, chunk %p",
						 name, block, chunk);
#endif
			}
			else
				/* free chunk */
				nfree += 1;
		}

		/*
		 * Make sure we got the expected number of free chunks (as tracked in
		 * the block header).
		 */
		if (nfree != block->nfree)
			elog(WARNING, "problem in slab %s: number of free chunks %d in block %p does not match bitmap %d",
				 name, block->nfree, block, nfree);
	}
}

#endif   /* MEMORY_CONTEXT_CHECKING */
