/*-------------------------------------------------------------------------
 *
 * genslab.c
 *	  Generational SLAB allocator definitions.
 *
 * An extension of the SLAB allocator relaxing the fixed-size limitation by
 * using a generational design.
 *
 *
 * Portions Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/genslab.c
 *
 *
 *	The simple SLAB allocator only allows allocating chunks with exactly the
 *	same size. That only works for some special cases, e.g. when the context
 *	is only used for instances of a single structure with fixed size.
 * 
 *	This implementation tries to relax this restriction by treating the chunk
 *	size as an upper boundary, and using a regular AllocSet context to serve
 *	requests for larger pieces of memory.
 *
 *	Furthermore, instead of using a single SLAB context (fixing the maximum
 *	chunk size) it's possible to automatically tune the chunk size based on
 *	past allocations. This is done by replacing the single SLAB context with
 *	a sequence of contexts (with only the last one used for allocations).
 *
 *	This works particularly well when we can't predict the size of the
 *	objects easily, but we know that the size is unlikely to vary too much.
 *	It also works quite nicely when the memory is freed in about the same
 *	sequence as it was allocated, because the old SLAB contexts will get
 *	empty and freed automatically (one of the benefits of SLAB contexts).
 *
 *	A good example is ReorderBuffer - the tuples tend to be of about the
 *	same size, and freed in roughly the same sequence as allocated.
 *
 *	In a sense, this delegates the allocation to actual implementations,
 *	which also handle CLOBBER_FREED_MEMORY and MEMORY_CONTEXT_CHECKING.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/memdebug.h"
#include "utils/memutils.h"


/*
 * GenSlabContext is a self-tuning version of SlabContext.
 */
typedef struct GenSlabContext
{
	MemoryContextData header;	/* Standard memory-context fields */

	MemoryContext	slab;
	MemoryContext	aset;

	/* SLAB parameters */
	Size		blockSize;		/* block size */
	Size		chunkSize;		/* chunk size */

	/* counters used for tuning chunk size */

	Size		nbytes;			/* bytes allocated (as requested) */
	int			nallocations;	/* number of allocations */
	int			maxallocations;	/* self-tune after number of allocations */

} GenSlabContext;

typedef GenSlabContext *GenSlab;

/*
 * These functions implement the MemoryContext API for GenSlab contexts.
 */
static void *GenSlabAlloc(MemoryContext context, Size size);
static void GenSlabFree(MemoryContext context, void *pointer);
static void *GenSlabRealloc(MemoryContext context, void *pointer, Size size);
static void GenSlabInit(MemoryContext context);
static void GenSlabReset(MemoryContext context);
static void GenSlabDelete(MemoryContext context);
static Size GenSlabGetChunkSpace(MemoryContext context, void *pointer);
static bool GenSlabIsEmpty(MemoryContext context);
static void GenSlabStats(MemoryContext context, int level, bool print,
			  MemoryContextCounters *totals);

#ifdef MEMORY_CONTEXT_CHECKING
static void GenSlabCheck(MemoryContext context);
#endif

/*
 * This is the virtual function table for Slab contexts.
 */
static MemoryContextMethods GenSlabMethods = {
	GenSlabAlloc,
	GenSlabFree,
	GenSlabRealloc,
	GenSlabInit,
	GenSlabReset,
	GenSlabDelete,
	GenSlabGetChunkSpace,
	GenSlabIsEmpty,
	GenSlabStats
#ifdef MEMORY_CONTEXT_CHECKING
	,GenSlabCheck
#endif
};


/*
 * Public routines
 */


/*
 * GenSlabContextCreate
 *		Create a new GenSlab context.
 */
MemoryContext
GenSlabContextCreate(MemoryContext parent,
					  const char *name,
					  Size blockSize,
					  Size chunkSize,
					  int maxAllocations)
{
	GenSlab	set;

	/* Do the type-independent part of context creation */
	set = (GenSlab) MemoryContextCreate(T_GenSlabContext,
										sizeof(GenSlabContext),
										&GenSlabMethods,
										parent,
										name);

	/* the default context */
	set->slab = SlabContextCreate((MemoryContext)set,
								  "slab",
								  blockSize,
								  chunkSize);

	/*
	 * TODO Maybe we could set the parameters so that all requests exceeding
	 * the SLAB chunk size (and thus falling through to the AllocSet) also
	 * exceed allocChunkLimit and thus get allocated using malloc(). That's
	 * more expensive, but vast majority of requests should be handled by
	 * the SLAB context anyway. And chunks over allocChunkLimit are freed
	 * immediately, which is also nice.
	 */
	set->aset = AllocSetContextCreate((MemoryContext)set,
									 "oversized",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);

	set->blockSize = blockSize;
	set->nbytes = 0;
	set->nallocations = 0;
	set->maxallocations = maxAllocations;

	return (MemoryContext) set;
}

/*
 * GenSlabInit
 *		Context-type-specific initialization routine. Simply delegate the
 *		child contexts.
 */
static void
GenSlabInit(MemoryContext context)
{
	GenSlab set = (GenSlab)context;

	set->nallocations = 0;
	set->nbytes = 0;
}

/*
 * GenSlabReset
 *		Frees all memory which is allocated in the given set. We also get
 *		rid of all the old SLAB generations and only keep the current one.
 *
 * The code simply frees all the blocks in the context - we don't keep any
 * keeper blocks or anything like that.
 */
static void
GenSlabReset(MemoryContext context)
{
	GenSlab	set = (GenSlab) context;

	set->nallocations = 0;
	set->nbytes = 0;
}

/*
 * GenSlabDelete
 *		Frees all memory which is allocated in the given set, in preparation
 *		for deletion of the set. We don't really need to do anything special
 *		as MemoryContextDelete deletes child contexts automatically.
 */
static void
GenSlabDelete(MemoryContext context)
{
	/* just reset (although not really necessary) */
	GenSlabReset(context);
}

/*
 * GenSlabAlloc
 *		Returns pointer to allocated memory of given size or NULL if
 *		request could not be completed; memory is added to the set.
 *
 * No request may exceed:
 *		MAXALIGN_DOWN(SIZE_MAX) - SLAB_BLOCKHDRSZ - SLAB_CHUNKHDRSZ
 * All callers use a much-lower limit.
 */
static void *
GenSlabAlloc(MemoryContext context, Size size)
{
	GenSlab	set = (GenSlab) context;

	/* do we need to auto-tune the SLAB chunk size */
	if (set->nallocations > set->maxallocations)
	{
		/*
		 * TODO we could also assume the requests follow normal distribution,
		 * computing stddev and then computing a chosen percentile (e.g. 0.95).
		 * For now we simply use 1.5x the average, as it's simple.
		 */

		/* compute the new chunk size */
		set->chunkSize = (1.5 * set->nbytes) / set->nallocations;

		/*
		 * We don't allow chunk size to exceed ALLOCSET_SEPARATE_THRESHOLD
		 * (8kB), as in that case AllocSet uses a separately, with constant
		 * allocation overhead.
		 */
		set->chunkSize = Min(set->chunkSize, ALLOCSET_SEPARATE_THRESHOLD);

		/* mark the old Slab context for autodestruction (and replace it) */
		SlabAutodestruct(set->slab);

		set->slab = SlabContextCreate((MemoryContext)set,
									  "slab",
									  set->blockSize,
									  set->chunkSize);

		/* reset the counters */
		set->nallocations = 0;
		set->nbytes = 0;
	}

	/* increment the auto-tuning counters */
	set->nallocations += 1;
	set->nbytes += size;

	if (size <= set->chunkSize)
		return MemoryContextAlloc(set->slab, set->chunkSize);
	else
		return MemoryContextAlloc(set->aset, size);
}

/*
 * GenSlabFree
 *		As the memory is actually allocated in other contexts, we should
 *		never really get here.
 *
 * FIXME Although someone could call MemoryContextFree directly.
 */
static void
GenSlabFree(MemoryContext context, void *pointer)
{
	return pfree(pointer);
}

/*
 * GenSlabRealloc
 *		As the memory is actually allocated in other contexts, we should
 *		never really get here.
 *
 * FIXME Although someone could call MemoryContextRealloc directly.
 */
static void *
GenSlabRealloc(MemoryContext context, void *pointer, Size size)
{
	return repalloc(pointer, size);
}

/*
 * GenSlabGetChunkSpace
 *		As the memory is actually allocated in other contexts, we should
 *		never really get here.
 *
 * FIXME Although someone could call MemoryContextGetChunkSpace directly.
 */
static Size
GenSlabGetChunkSpace(MemoryContext context, void *pointer)
{
	return GetMemoryChunkSpace(pointer);
}

/*
 * GenSlabIsEmpty
 *		Is an GenSlab empty of any allocated space?
 *
 * TODO This does not really work, as MemoryContextIsEmpty returns false if
 * 		there are any children, and GenSlab always has at least two.
 */
static bool
GenSlabIsEmpty(MemoryContext context)
{
	/* */
	return true;
}

/*
 * GenSlabStats
 *		Compute stats about memory consumption of an GenSlab.
 *
 * level: recursion level (0 at top level); used for print indentation.
 * print: true to print stats to stderr.
 * totals: if not NULL, add stats about this Slab into *totals.
 */
static void
GenSlabStats(MemoryContext context, int level, bool print,
			  MemoryContextCounters *totals)
{
	GenSlab		set = (GenSlab) context;

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
 * GenSlabCheck
 *		Walk through chunks and check consistency of memory.
 *
 * NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
 * find yourself in an infinite loop when trouble occurs, because this
 * routine will be entered again when elog cleanup tries to release memory!
 */
static void
GenSlabCheck(MemoryContext context)
{
	
}

#endif   /* MEMORY_CONTEXT_CHECKING */
