/*-------------------------------------------------------------------------
 *
 * slab.c
 *	  SLAB allocator definitions.
 *
 * SLAB is a custom MemoryContext implementation designed for cases of
 * equally-sized objects.
 *
 *
 * Portions Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/slab.c
 *
 *
 *	The constant allocation size allows significant simplification and various
 *	optimizations. The blocks are carved into chunks of exactly the right size
 *	(plus alignment), not wasting any memory.
 *
 *	The information about free chunks is maintained both at the block level and
 *	global (context) level. This is possible as the chunk size (and thus also
 *	the number of chunks per block) is fixed.
 *
 *	On each block, free chunks are tracked in a simple linked list. Contents
 *	of free chunks is replaced with an index of the next free chunk, forming
 *	a very simple linked list. Each block also contains a counter of free
 *	chunks. Combined with the local block-level freelist, it makes it trivial
 *	to eventually free the whole block.
 *
 *	At the context level, we use 'freelist' to track blocks ordered by number
 *	of free chunks, starting with blocks having a single allocated chunk, and
 *	with completely full blocks on the tail.
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
 *	We cache indexes of the first empty chunk on each block (firstFreeChunk),
 *	and freelist index for blocks with least free chunks (minFreeChunks), so
 *	that we don't have to search the freelist and block on every SlabAlloc()
 *	call, which is quite expensive.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "lib/ilist.h"


#define SLAB_BLOCKHDRSZ MAXALIGN(sizeof(SlabBlockData))
#define SLAB_CHUNKHDRSZ MAXALIGN(sizeof(SlabChunkData))

/* Portion of SLAB_CHUNKHDRSZ examined outside slab.c. */
#define SLAB_CHUNK_PUBLIC	\
	(offsetof(SlabChunkData, size) + sizeof(Size))

/* Portion of SLAB_CHUNKHDRSZ excluding trailing padding. */
#ifdef MEMORY_CONTEXT_CHECKING
#define SLAB_CHUNK_USED \
	(offsetof(SlabChunkData, requested_size) + sizeof(Size))
#else
#define SLAB_CHUNK_USED \
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
	int			chunksPerBlock; /* number of chunks per block */
	int			minFreeChunks;	/* min number of free chunks in any block */
	int			nblocks;		/* number of blocks allocated */
	/* blocks with free space, grouped by number of free chunks: */
	dlist_head	freelist[FLEXIBLE_ARRAY_MEMBER];
}	SlabContext;

typedef SlabContext *Slab;

/*
 * SlabBlockData
 *		Structure of a single block in SLAB allocator.
 *
 * node: doubly-linked list of blocks in global freelist
 * nfree: number of free chunks in this block
 * firstFreeChunk: index of the first free chunk
 */
typedef struct SlabBlockData
{
	dlist_node	node;			/* doubly-linked list */
	int			nfree;			/* number of free chunks */
	int			firstFreeChunk; /* index of the first free chunk in the block */
}	SlabBlockData;

/*
 * SlabChunk
 *		The prefix of each piece of memory in an SlabBlock
 */
typedef struct SlabChunkData
{
	/* block owning this chunk */
	void	   *block;

	/* include StandardChunkHeader because mcxt.c expects that */
	StandardChunkHeader header;

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
#define SlabBlockGetChunk(set, block, idx)	\
					((SlabChunk) ((char *) (block) + sizeof(SlabBlockData)	\
					+ (idx * set->fullChunkSize)))
#define SlabBlockStart(block)	\
				((char *) block + sizeof(SlabBlockData))
#define SlabChunkIndex(set, block, chunk)	\
				(((char *) chunk - SlabBlockStart(block)) / set->fullChunkSize)
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
			fprintf(stderr, "SlabFree: %s: %p, %lu\n", \
				(_cxt)->header.name, (_chunk), (_chunk)->size)
#define SlabAllocInfo(_cxt, _chunk) \
			fprintf(stderr, "SlabAlloc: %s: %p, %lu\n", \
				(_cxt)->header.name, (_chunk), (_chunk)->size)
#else
#define SlabFreeInfo(_cxt, _chunk)
#define SlabAllocInfo(_cxt, _chunk)
#endif


/*
 * SlabContextCreate
 *		Create a new Slab context.
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (for debugging --- string will be copied)
 * blockSize: allocation block size
 * chunkSize: allocation chunk size
 *
 * The chunkSize may not exceed:
 *		MAXALIGN_DOWN(SIZE_MAX) - SLAB_BLOCKHDRSZ - SLAB_CHUNKHDRSZ
 *
 */
MemoryContext
SlabContextCreate(MemoryContext parent,
				  const char *name,
				  Size blockSize,
				  Size chunkSize)
{
	int			chunksPerBlock;
	Size		fullChunkSize;
	Size		freelistSize;
	Slab		set;

	/* chunk, including SLAB header (both addresses nicely aligned) */
	fullChunkSize = MAXALIGN(sizeof(SlabChunkData) + MAXALIGN(chunkSize));

	/* Make sure the block can store at least one chunk. */
	if (blockSize - sizeof(SlabBlockData) < fullChunkSize)
		elog(ERROR, "block size %ld for slab is too small for %ld chunks",
			 blockSize, chunkSize);

	/* Compute maximum number of chunks per block */
	chunksPerBlock = (blockSize - sizeof(SlabBlockData)) / fullChunkSize;

	/* The freelist starts with 0, ends with chunksPerBlock. */
	freelistSize = sizeof(dlist_head) * (chunksPerBlock + 1);

	/* if we can't fit at least one chunk into the block, we're hosed */
	Assert(chunksPerBlock > 0);

	/* make sure the chunks actually fit on the block	*/
	Assert((fullChunkSize * chunksPerBlock) + sizeof(SlabBlockData) <= blockSize);

	/* Do the type-independent part of context creation */
	set = (Slab) MemoryContextCreate(T_SlabContext,
									 (offsetof(SlabContext, freelist) + freelistSize),
									 &SlabMethods,
									 parent,
									 name);

	set->blockSize = blockSize;
	set->chunkSize = chunkSize;
	set->fullChunkSize = fullChunkSize;
	set->chunksPerBlock = chunksPerBlock;
	set->nblocks = 0;
	set->minFreeChunks = 0;

	return (MemoryContext) set;
}

/*
 * SlabInit
 *		Context-type-specific initialization routine.
 */
static void
SlabInit(MemoryContext context)
{
	int			i;
	Slab		set = (Slab) context;

	/* initialize the freelist slots */
	for (i = 0; i < (set->chunksPerBlock + 1); i++)
		dlist_init(&set->freelist[i]);
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
	int			i;
	Slab		set = (Slab) context;

	AssertArg(SlabIsValid(set));

#ifdef MEMORY_CONTEXT_CHECKING
	/* Check for corruption and leaks before freeing */
	SlabCheck(context);
#endif

	/* walk over freelists and free the blocks */
	for (i = 0; i <= set->chunksPerBlock; i++)
	{
		dlist_mutable_iter miter;

		dlist_foreach_modify(miter, &set->freelist[i])
		{
			SlabBlock	block = dlist_container(SlabBlockData, node, miter.cur);

			dlist_delete(miter.cur);

#ifdef CLOBBER_FREED_MEMORY
			wipe_mem(block, set->blockSize);
#endif
			free(block);
			set->nblocks--;
		}
	}

	set->minFreeChunks = 0;

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

/*
 * SlabAlloc
 *		Returns pointer to allocated memory of given size or NULL if
 *		request could not be completed; memory is added to the set.
 */
static void *
SlabAlloc(MemoryContext context, Size size)
{
	Slab		set = (Slab) context;
	SlabBlock	block;
	SlabChunk	chunk;
	int			idx;

	AssertArg(SlabIsValid(set));

	Assert((set->minFreeChunks >= 0) && (set->minFreeChunks < set->chunksPerBlock));

	/* make sure we only allow correct request size */
	if (size != set->chunkSize)
		elog(ERROR, "unexpected alloc chunk size %ld (expected %ld)",
			 size, set->chunkSize);

	/*
	 * If there are no free chunks in any existing block, create a new block
	 * and put it to the last freelist bucket.
	 *
	 * (set->minFreeChunks == 0) means there are no blocks with free chunks,
	 * thanks to how minFreeChunks is updated at the end of SlabAlloc().
	 */
	if (set->minFreeChunks == 0)
	{
		block = (SlabBlock) malloc(set->blockSize);

		if (block == NULL)
			return NULL;

		block->nfree = set->chunksPerBlock;
		block->firstFreeChunk = 0;

		/*
		 * Put all the chunks on a freelist. Walk the chunks and point each
		 * one to the next one.
		 */
		for (idx = 0; idx < set->chunksPerBlock; idx++)
		{
			chunk = SlabBlockGetChunk(set, block, idx);
			*(int32 *)SlabChunkGetPointer(chunk) = (idx + 1);
		}

		/*
		 * And add it to the last freelist with all chunks empty.
		 *
		 * XXX We know there are no blocks in the freelist, otherwise we
		 * wouldn't need a new block.
		 */
		Assert(dlist_is_empty(&set->freelist[set->chunksPerBlock]));

		dlist_push_head(&set->freelist[set->chunksPerBlock], &block->node);

		set->minFreeChunks = set->chunksPerBlock;
		set->nblocks += 1;
	}

	/* grab the block from the freelist (even the new block is there) */
	block = dlist_head_element(SlabBlockData, node,
							   &set->freelist[set->minFreeChunks]);

	/* make sure we actually got a valid block, with matching nfree */
	Assert(block != NULL);
	Assert(set->minFreeChunks == block->nfree);
	Assert(block->nfree > 0);

	/* we know index of the first free chunk in the block */
	idx = block->firstFreeChunk;

	/* make sure the chunk index is valid, and that it's marked as empty */
	Assert((idx >= 0) && (idx < set->chunksPerBlock));

	/* compute the chunk location block start (after the block header) */
	chunk = SlabBlockGetChunk(set, block, idx);

	/*
	 * Update the block nfree count, and also the minFreeChunks as we've
	 * decreased nfree for a block with the minimum number of free chunks
	 * (because that's how we chose the block).
	 */
	block->nfree--;
	set->minFreeChunks = block->nfree;

	/*
	 * Remove the chunk from the freelist head. The index of the next free
	 * chunk is stored in the chunk itself.
	 */
	block->firstFreeChunk = *(int32 *)SlabChunkGetPointer(chunk);

	Assert(block->firstFreeChunk >= 0);
	Assert(block->firstFreeChunk <= set->chunksPerBlock);

	Assert(((block->nfree != 0) && (block->firstFreeChunk < set->chunksPerBlock)) ||
		   ((block->nfree == 0) && (block->firstFreeChunk == set->chunksPerBlock)));

	/* move the whole block to the right place in the freelist */
	dlist_delete(&block->node);
	dlist_push_head(&set->freelist[block->nfree], &block->node);

	/*
	 * And finally update minFreeChunks, i.e. the index to the block with the
	 * lowest number of free chunks. We only need to do that when the block
	 * got full (otherwise we know the current block is the right one). We'll
	 * simply walk the freelist until we find a non-empty entry.
	 */
	if (set->minFreeChunks == 0)
	{
		for (idx = 1; idx <= set->chunksPerBlock; idx++)
		{
			if (dlist_is_empty(&set->freelist[idx]))
				continue;

			/* found a non-empty freelist */
			set->minFreeChunks = idx;
			break;
		}
	}

	if (set->minFreeChunks == set->chunksPerBlock)
		set->minFreeChunks = 0;

	/* Prepare to initialize the chunk header. */
	VALGRIND_MAKE_MEM_UNDEFINED(chunk, SLAB_CHUNK_USED);

	chunk->block = (void *) block;

	chunk->header.context = (MemoryContext) set;
	chunk->header.size = MAXALIGN(size);

#ifdef MEMORY_CONTEXT_CHECKING
	chunk->header.requested_size = size;
	VALGRIND_MAKE_MEM_NOACCESS(&chunk->header.requested_size,
							   sizeof(chunk->header.requested_size));
	/* set mark to catch clobber of "unused" space */
	if (size < chunk->header.size)
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
	int			idx;
	Slab		set = (Slab) context;
	SlabChunk	chunk = SlabPointerGetChunk(pointer);
	SlabBlock	block = chunk->block;

	SlabFreeInfo(set, chunk);

#ifdef MEMORY_CONTEXT_CHECKING
	VALGRIND_MAKE_MEM_DEFINED(&chunk->header.requested_size,
							  sizeof(chunk->header.requested_size));
	/* Test for someone scribbling on unused space in chunk */
	if (chunk->header.requested_size < chunk->header.size)
		if (!sentinel_ok(pointer, chunk->header.requested_size))
			elog(WARNING, "detected write past chunk end in %s %p",
				 set->header.name, chunk);
#endif

	/* compute index of the chunk with respect to block start */
	idx = SlabChunkIndex(set, block, chunk);

	/* mark the chunk as unused (zero the bit), and update block nfree count */
	*(int32 *)pointer = block->firstFreeChunk;
	block->firstFreeChunk = idx;
	block->nfree++;

	Assert(block->nfree > 0);
	Assert(block->nfree <= set->chunksPerBlock);

#ifdef CLOBBER_FREED_MEMORY
	/* XXX don't wipe the int32 index, used for block-level freelist */
	wipe_mem((char *)pointer + sizeof(int32), chunk->header.size - sizeof(int32));
#endif

#ifdef MEMORY_CONTEXT_CHECKING
	/* Reset requested_size to 0 in chunks that are on freelist */
	chunk->header.requested_size = 0;
#endif

	/* remove the block from a freelist */
	dlist_delete(&block->node);

	/*
	 * See if we need to update the minFreeChunks field for the set - we only
	 * need to do that if there the block had that number of free chunks
	 * before we freed one. In that case, we check if there still are blocks
	 * in the original freelist and we either keep the current value (if there
	 * still are blocks) or increment it by one (the new block is still the
	 * one with minimum free chunks).
	 *
	 * The one exception is when the block will get completely free - in that
	 * case we will free it, se we can't use it for minFreeChunks. It however
	 * means there are no more blocks with free chunks.
	 */
	if (set->minFreeChunks == (block->nfree - 1))
	{
		/* Have we removed the last chunk from the freelist? */
		if (dlist_is_empty(&set->freelist[set->minFreeChunks]))
		{
			/* but if we made the block entirely free, we'll free it */
			if (block->nfree == set->chunksPerBlock)
				set->minFreeChunks = 0;
			else
				set->minFreeChunks++;
		}
	}

	/* If the block is now completely empty, free it. */
	if (block->nfree == set->chunksPerBlock)
	{
		free(block);
		set->nblocks--;
	}
	else
		dlist_push_head(&set->freelist[block->nfree], &block->node);

	Assert(set->nblocks >= 0);
}

/*
 * SlabRealloc
 *		As Slab is designed for allocating equally-sized chunks of memory, it
 *		can't really do an actual realloc.
 *
 * We try to be gentle and allow calls with exactly the same size as in that
 * case we can simply return the same chunk. When the size differs, we fail
 * with assert failure or return NULL.
 *
 * We might be even support cases with (size < chunkSize). That however seems
 * rather pointless - Slab is meant for chunks of constant size, and moreover
 * realloc is usually used to enlarge the chunk.
 *
 * XXX Perhaps we should not be gentle at all and simply fails in all cases,
 * to eliminate the (mostly pointless) uncertainty.
 */
static void *
SlabRealloc(MemoryContext context, void *pointer, Size size)
{
	Slab		set = (Slab) context;

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

	return chunk->header.size + SLAB_CHUNKHDRSZ;
}

/*
 * SlabIsEmpty
 *		Is an Slab empty of any allocated space?
 */
static bool
SlabIsEmpty(MemoryContext context)
{
	Slab		set = (Slab) context;

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
		dlist_iter	iter;

		dlist_foreach(iter, &set->freelist[i])
		{
			SlabBlock	block = dlist_container(SlabBlockData, node, iter.cur);

			nblocks++;
			totalspace += set->blockSize;
			freespace += set->fullChunkSize * block->nfree;
			freechunks += block->nfree;
		}
	}

	if (print)
	{
		for (i = 0; i < level; i++)
			fprintf(stderr, "  ");
		fprintf(stderr,
			"Slab: %s: %zu total in %zd blocks; %zu free (%zd chunks); %zu used\n",
				set->header.name, totalspace, nblocks, freespace, freechunks,
				totalspace - freespace);
	}

	if (totals)
	{
		totals->nblocks += nblocks;
		totals->freechunks += freechunks;
		totals->totalspace += totalspace;
		totals->freespace += freespace;
	}
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
	int			i;
	Slab		slab = (Slab) context;
	char	   *name = slab->header.name;
	char	   *freechunks;

	Assert(slab->chunksPerBlock > 0);

	/* bitmap of free chunks on a block */
	freechunks = palloc(slab->chunksPerBlock * sizeof(bool));

	/* walk all the freelists */
	for (i = 0; i <= slab->chunksPerBlock; i++)
	{
		int			j,
					nfree;
		dlist_iter	iter;

		/* walk all blocks on this freelist */
		dlist_foreach(iter, &slab->freelist[i])
		{
			int			idx;
			SlabBlock	block = dlist_container(SlabBlockData, node, iter.cur);

			/*
			 * Make sure the number of free chunks (in the block header) matches
			 * position in the freelist.
			 */
			if (block->nfree != i)
				elog(WARNING, "problem in slab %s: number of free chunks %d in block %p does not match freelist %d",
					 name, block->nfree, block, i);

			/* reset the bitmap of free chunks for this block */
			memset(freechunks, 0, (slab->chunksPerBlock * sizeof(bool)));
			idx = block->firstFreeChunk;

			/*
			 * Now walk through the chunks, count the free ones and also perform
			 * some additional checks for the used ones. As the chunk freelist
			 * is stored within the chunks themselves, we have to walk through
			 * the chunks and construct our own bitmap.
			 */

			nfree = 0;
			while (idx < slab->chunksPerBlock)
			{
				SlabChunk	chunk;

				/* count the chunk as free, add it to the bitmap */
				nfree++;
				freechunks[idx] = true;

				/* read index of the next free chunk */
				chunk = SlabBlockGetChunk(slab, block, idx);
				idx = *(int32 *)SlabChunkGetPointer(chunk);
			}

			for (j = 0; j < slab->chunksPerBlock; j++)
			{
				/* non-zero bit in the bitmap means chunk the chunk is used */
				if (! freechunks[j])
				{
					SlabChunk	chunk = SlabBlockGetChunk(slab, block, j);

					VALGRIND_MAKE_MEM_DEFINED(&chunk->header.requested_size,
										   sizeof(chunk->header.requested_size));

					/* we're in a no-freelist branch */
					VALGRIND_MAKE_MEM_NOACCESS(&chunk->header.requested_size,
										   sizeof(chunk->header.requested_size));

					/* chunks have both block and slab pointers, so check both */
					if (chunk->block != block)
						elog(WARNING, "problem in slab %s: bogus block link in block %p, chunk %p",
							 name, block, chunk);

					if (chunk->header.context != (MemoryContext) slab)
						elog(WARNING, "problem in slab %s: bogus slab link in block %p, chunk %p",
							 name, block, chunk);

					/* now make sure the chunk size is correct */
					if (chunk->header.size != MAXALIGN(slab->chunkSize))
						elog(WARNING, "problem in slab %s: bogus chunk size in block %p, chunk %p",
							 name, block, chunk);

					/* now make sure the chunk size is correct */
					if (chunk->header.requested_size != slab->chunkSize)
						elog(WARNING, "problem in slab %s: bogus chunk requested size in block %p, chunk %p",
							 name, block, chunk);

					/* there might be sentinel (thanks to alignment) */
					if (chunk->header.requested_size < chunk->header.size &&
						!sentinel_ok(chunk, SLAB_CHUNKHDRSZ + chunk->header.requested_size))
						elog(WARNING, "problem in slab %s: detected write past chunk end in block %p, chunk %p",
							 name, block, chunk);
				}
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
}

#endif   /* MEMORY_CONTEXT_CHECKING */
