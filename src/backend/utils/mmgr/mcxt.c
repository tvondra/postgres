/*-------------------------------------------------------------------------
 *
 * mcxt.c
 *	  POSTGRES memory context management code.
 *
 * This module handles context management operations that are independent
 * of the particular kind of context being operated on.  It calls
 * context-type-specific operations via the function pointers in a
 * context's MemoryContextMethods struct.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/mcxt.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "utils/fmgrprotos.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/memutils_internal.h"
#include "utils/memutils_memorychunk.h"


static void BogusFree(void *pointer);
static void *BogusRealloc(void *pointer, Size size);
static MemoryContext BogusGetChunkContext(void *pointer);
static Size BogusGetChunkSpace(void *pointer);

/*****************************************************************************
 *	  GLOBAL MEMORY															 *
 *****************************************************************************/

static const MemoryContextMethods mcxt_methods[] = {
	/* aset.c */
	[MCTX_ASET_ID].alloc = AllocSetAlloc,
	[MCTX_ASET_ID].free_p = AllocSetFree,
	[MCTX_ASET_ID].realloc = AllocSetRealloc,
	[MCTX_ASET_ID].reset = AllocSetReset,
	[MCTX_ASET_ID].delete_context = AllocSetDelete,
	[MCTX_ASET_ID].get_chunk_context = AllocSetGetChunkContext,
	[MCTX_ASET_ID].get_chunk_space = AllocSetGetChunkSpace,
	[MCTX_ASET_ID].is_empty = AllocSetIsEmpty,
	[MCTX_ASET_ID].stats = AllocSetStats,
#ifdef MEMORY_CONTEXT_CHECKING
	[MCTX_ASET_ID].check = AllocSetCheck,
#endif

	/* generation.c */
	[MCTX_GENERATION_ID].alloc = GenerationAlloc,
	[MCTX_GENERATION_ID].free_p = GenerationFree,
	[MCTX_GENERATION_ID].realloc = GenerationRealloc,
	[MCTX_GENERATION_ID].reset = GenerationReset,
	[MCTX_GENERATION_ID].delete_context = GenerationDelete,
	[MCTX_GENERATION_ID].get_chunk_context = GenerationGetChunkContext,
	[MCTX_GENERATION_ID].get_chunk_space = GenerationGetChunkSpace,
	[MCTX_GENERATION_ID].is_empty = GenerationIsEmpty,
	[MCTX_GENERATION_ID].stats = GenerationStats,
#ifdef MEMORY_CONTEXT_CHECKING
	[MCTX_GENERATION_ID].check = GenerationCheck,
#endif

	/* slab.c */
	[MCTX_SLAB_ID].alloc = SlabAlloc,
	[MCTX_SLAB_ID].free_p = SlabFree,
	[MCTX_SLAB_ID].realloc = SlabRealloc,
	[MCTX_SLAB_ID].reset = SlabReset,
	[MCTX_SLAB_ID].delete_context = SlabDelete,
	[MCTX_SLAB_ID].get_chunk_context = SlabGetChunkContext,
	[MCTX_SLAB_ID].get_chunk_space = SlabGetChunkSpace,
	[MCTX_SLAB_ID].is_empty = SlabIsEmpty,
	[MCTX_SLAB_ID].stats = SlabStats,
#ifdef MEMORY_CONTEXT_CHECKING
	[MCTX_SLAB_ID].check = SlabCheck,
#endif

	/* alignedalloc.c */
	[MCTX_ALIGNED_REDIRECT_ID].alloc = NULL,	/* not required */
	[MCTX_ALIGNED_REDIRECT_ID].free_p = AlignedAllocFree,
	[MCTX_ALIGNED_REDIRECT_ID].realloc = AlignedAllocRealloc,
	[MCTX_ALIGNED_REDIRECT_ID].reset = NULL,	/* not required */
	[MCTX_ALIGNED_REDIRECT_ID].delete_context = NULL,	/* not required */
	[MCTX_ALIGNED_REDIRECT_ID].get_chunk_context = AlignedAllocGetChunkContext,
	[MCTX_ALIGNED_REDIRECT_ID].get_chunk_space = AlignedAllocGetChunkSpace,
	[MCTX_ALIGNED_REDIRECT_ID].is_empty = NULL, /* not required */
	[MCTX_ALIGNED_REDIRECT_ID].stats = NULL,	/* not required */
#ifdef MEMORY_CONTEXT_CHECKING
	[MCTX_ALIGNED_REDIRECT_ID].check = NULL,	/* not required */
#endif


	/*
	 * Unused (as yet) IDs should have dummy entries here.  This allows us to
	 * fail cleanly if a bogus pointer is passed to pfree or the like.  It
	 * seems sufficient to provide routines for the methods that might get
	 * invoked from inspection of a chunk (see MCXT_METHOD calls below).
	 */

	[MCTX_UNUSED1_ID].free_p = BogusFree,
	[MCTX_UNUSED1_ID].realloc = BogusRealloc,
	[MCTX_UNUSED1_ID].get_chunk_context = BogusGetChunkContext,
	[MCTX_UNUSED1_ID].get_chunk_space = BogusGetChunkSpace,

	[MCTX_UNUSED2_ID].free_p = BogusFree,
	[MCTX_UNUSED2_ID].realloc = BogusRealloc,
	[MCTX_UNUSED2_ID].get_chunk_context = BogusGetChunkContext,
	[MCTX_UNUSED2_ID].get_chunk_space = BogusGetChunkSpace,

	[MCTX_UNUSED3_ID].free_p = BogusFree,
	[MCTX_UNUSED3_ID].realloc = BogusRealloc,
	[MCTX_UNUSED3_ID].get_chunk_context = BogusGetChunkContext,
	[MCTX_UNUSED3_ID].get_chunk_space = BogusGetChunkSpace,

	[MCTX_UNUSED4_ID].free_p = BogusFree,
	[MCTX_UNUSED4_ID].realloc = BogusRealloc,
	[MCTX_UNUSED4_ID].get_chunk_context = BogusGetChunkContext,
	[MCTX_UNUSED4_ID].get_chunk_space = BogusGetChunkSpace,
};

/*
 * CurrentMemoryContext
 *		Default memory context for allocations.
 */
MemoryContext CurrentMemoryContext = NULL;

/*
 * Standard top-level contexts. For a description of the purpose of each
 * of these contexts, refer to src/backend/utils/mmgr/README
 */
MemoryContext TopMemoryContext = NULL;
MemoryContext ErrorContext = NULL;
MemoryContext PostmasterContext = NULL;
MemoryContext CacheMemoryContext = NULL;
MemoryContext MessageContext = NULL;
MemoryContext TopTransactionContext = NULL;
MemoryContext CurTransactionContext = NULL;

/* This is a transient link to the active portal's memory context: */
MemoryContext PortalContext = NULL;

static void MemoryContextCallResetCallbacks(MemoryContext context);
static void MemoryContextStatsInternal(MemoryContext context, int level,
									   bool print, int max_children,
									   MemoryContextCounters *totals,
									   bool print_to_stderr);
static void MemoryContextStatsPrint(MemoryContext context, void *passthru,
									const char *stats_string,
									bool print_to_stderr);

/*
 * You should not do memory allocations within a critical section, because
 * an out-of-memory error will be escalated to a PANIC. To enforce that
 * rule, the allocation functions Assert that.
 */
#define AssertNotInCriticalSection(context) \
	Assert(CritSectionCount == 0 || (context)->allowInCritSection)

/*
 * Call the given function in the MemoryContextMethods for the memory context
 * type that 'pointer' belongs to.
 */
#define MCXT_METHOD(pointer, method) \
	mcxt_methods[GetMemoryChunkMethodID(pointer)].method

/*
 * GetMemoryChunkMethodID
 *		Return the MemoryContextMethodID from the uint64 chunk header which
 *		directly precedes 'pointer'.
 */
static inline MemoryContextMethodID
GetMemoryChunkMethodID(const void *pointer)
{
	uint64		header;

	/*
	 * Try to detect bogus pointers handed to us, poorly though we can.
	 * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
	 * allocated chunk.
	 */
	Assert(pointer == (const void *) MAXALIGN(pointer));

	/* Allow access to the uint64 header */
	VALGRIND_MAKE_MEM_DEFINED((char *) pointer - sizeof(uint64), sizeof(uint64));

	header = *((const uint64 *) ((const char *) pointer - sizeof(uint64)));

	/* Disallow access to the uint64 header */
	VALGRIND_MAKE_MEM_NOACCESS((char *) pointer - sizeof(uint64), sizeof(uint64));

	return (MemoryContextMethodID) (header & MEMORY_CONTEXT_METHODID_MASK);
}

/*
 * GetMemoryChunkHeader
 *		Return the uint64 chunk header which directly precedes 'pointer'.
 *
 * This is only used after GetMemoryChunkMethodID, so no need for error checks.
 */
static inline uint64
GetMemoryChunkHeader(const void *pointer)
{
	uint64		header;

	/* Allow access to the uint64 header */
	VALGRIND_MAKE_MEM_DEFINED((char *) pointer - sizeof(uint64), sizeof(uint64));

	header = *((const uint64 *) ((const char *) pointer - sizeof(uint64)));

	/* Disallow access to the uint64 header */
	VALGRIND_MAKE_MEM_NOACCESS((char *) pointer - sizeof(uint64), sizeof(uint64));

	return header;
}

/*
 * Support routines to trap use of invalid memory context method IDs
 * (from calling pfree or the like on a bogus pointer).  As a possible
 * aid in debugging, we report the header word along with the pointer
 * address (if we got here, there must be an accessible header word).
 */
static void
BogusFree(void *pointer)
{
	elog(ERROR, "pfree called with invalid pointer %p (header 0x%016llx)",
		 pointer, (unsigned long long) GetMemoryChunkHeader(pointer));
}

static void *
BogusRealloc(void *pointer, Size size)
{
	elog(ERROR, "repalloc called with invalid pointer %p (header 0x%016llx)",
		 pointer, (unsigned long long) GetMemoryChunkHeader(pointer));
	return NULL;				/* keep compiler quiet */
}

static MemoryContext
BogusGetChunkContext(void *pointer)
{
	elog(ERROR, "GetMemoryChunkContext called with invalid pointer %p (header 0x%016llx)",
		 pointer, (unsigned long long) GetMemoryChunkHeader(pointer));
	return NULL;				/* keep compiler quiet */
}

static Size
BogusGetChunkSpace(void *pointer)
{
	elog(ERROR, "GetMemoryChunkSpace called with invalid pointer %p (header 0x%016llx)",
		 pointer, (unsigned long long) GetMemoryChunkHeader(pointer));
	return 0;					/* keep compiler quiet */
}


/*****************************************************************************
 *	  EXPORTED ROUTINES														 *
 *****************************************************************************/


/*
 * MemoryContextInit
 *		Start up the memory-context subsystem.
 *
 * This must be called before creating contexts or allocating memory in
 * contexts.  TopMemoryContext and ErrorContext are initialized here;
 * other contexts must be created afterwards.
 *
 * In normal multi-backend operation, this is called once during
 * postmaster startup, and not at all by individual backend startup
 * (since the backends inherit an already-initialized context subsystem
 * by virtue of being forked off the postmaster).  But in an EXEC_BACKEND
 * build, each process must do this for itself.
 *
 * In a standalone backend this must be called during backend startup.
 */
void
MemoryContextInit(void)
{
	Assert(TopMemoryContext == NULL);

	/*
	 * First, initialize TopMemoryContext, which is the parent of all others.
	 */
	TopMemoryContext = AllocSetContextCreate((MemoryContext) NULL,
											 "TopMemoryContext",
											 ALLOCSET_DEFAULT_SIZES);

	/*
	 * Not having any other place to point CurrentMemoryContext, make it point
	 * to TopMemoryContext.  Caller should change this soon!
	 */
	CurrentMemoryContext = TopMemoryContext;

	/*
	 * Initialize ErrorContext as an AllocSetContext with slow growth rate ---
	 * we don't really expect much to be allocated in it. More to the point,
	 * require it to contain at least 8K at all times. This is the only case
	 * where retained memory in a context is *essential* --- we want to be
	 * sure ErrorContext still has some memory even if we've run out
	 * elsewhere! Also, allow allocations in ErrorContext within a critical
	 * section. Otherwise a PANIC will cause an assertion failure in the error
	 * reporting code, before printing out the real cause of the failure.
	 *
	 * This should be the last step in this function, as elog.c assumes memory
	 * management works once ErrorContext is non-null.
	 */
	ErrorContext = AllocSetContextCreate(TopMemoryContext,
										 "ErrorContext",
										 8 * 1024,
										 8 * 1024,
										 8 * 1024);
	MemoryContextAllowInCriticalSection(ErrorContext, true);
}

/*
 * MemoryContextReset
 *		Release all space allocated within a context and delete all its
 *		descendant contexts (but not the named context itself).
 */
void
MemoryContextReset(MemoryContext context)
{
	Assert(MemoryContextIsValid(context));

	/* save a function call in common case where there are no children */
	if (context->firstchild != NULL)
		MemoryContextDeleteChildren(context);

	/* save a function call if no pallocs since startup or last reset */
	if (!context->isReset)
		MemoryContextResetOnly(context);
}

/*
 * MemoryContextResetOnly
 *		Release all space allocated within a context.
 *		Nothing is done to the context's descendant contexts.
 */
void
MemoryContextResetOnly(MemoryContext context)
{
	Assert(MemoryContextIsValid(context));

	/* Nothing to do if no pallocs since startup or last reset */
	if (!context->isReset)
	{
		MemoryContextCallResetCallbacks(context);

		/*
		 * If context->ident points into the context's memory, it will become
		 * a dangling pointer.  We could prevent that by setting it to NULL
		 * here, but that would break valid coding patterns that keep the
		 * ident elsewhere, e.g. in a parent context.  So for now we assume
		 * the programmer got it right.
		 */

		context->methods->reset(context);
		context->isReset = true;
		VALGRIND_DESTROY_MEMPOOL(context);
		VALGRIND_CREATE_MEMPOOL(context, 0, false);
	}
}

/*
 * MemoryContextResetChildren
 *		Release all space allocated within a context's descendants,
 *		but don't delete the contexts themselves.  The named context
 *		itself is not touched.
 */
void
MemoryContextResetChildren(MemoryContext context)
{
	MemoryContext child;

	Assert(MemoryContextIsValid(context));

	for (child = context->firstchild; child != NULL; child = child->nextchild)
	{
		MemoryContextResetChildren(child);
		MemoryContextResetOnly(child);
	}
}

/*
 * MemoryContextDelete
 *		Delete a context and its descendants, and release all space
 *		allocated therein.
 *
 * The type-specific delete routine removes all storage for the context,
 * but we have to recurse to handle the children.
 * We must also delink the context from its parent, if it has one.
 */
void
MemoryContextDelete(MemoryContext context)
{
	Assert(MemoryContextIsValid(context));
	/* We had better not be deleting TopMemoryContext ... */
	Assert(context != TopMemoryContext);
	/* And not CurrentMemoryContext, either */
	Assert(context != CurrentMemoryContext);

	/* save a function call in common case where there are no children */
	if (context->firstchild != NULL)
		MemoryContextDeleteChildren(context);

	/*
	 * It's not entirely clear whether 'tis better to do this before or after
	 * delinking the context; but an error in a callback will likely result in
	 * leaking the whole context (if it's not a root context) if we do it
	 * after, so let's do it before.
	 */
	MemoryContextCallResetCallbacks(context);

	/*
	 * We delink the context from its parent before deleting it, so that if
	 * there's an error we won't have deleted/busted contexts still attached
	 * to the context tree.  Better a leak than a crash.
	 */
	MemoryContextSetParent(context, NULL);

	/*
	 * Also reset the context's ident pointer, in case it points into the
	 * context.  This would only matter if someone tries to get stats on the
	 * (already unlinked) context, which is unlikely, but let's be safe.
	 */
	context->ident = NULL;

	context->methods->delete_context(context);

	VALGRIND_DESTROY_MEMPOOL(context);
}

/*
 * MemoryContextDeleteChildren
 *		Delete all the descendants of the named context and release all
 *		space allocated therein.  The named context itself is not touched.
 */
void
MemoryContextDeleteChildren(MemoryContext context)
{
	Assert(MemoryContextIsValid(context));

	/*
	 * MemoryContextDelete will delink the child from me, so just iterate as
	 * long as there is a child.
	 */
	while (context->firstchild != NULL)
		MemoryContextDelete(context->firstchild);
}

/*
 * MemoryContextRegisterResetCallback
 *		Register a function to be called before next context reset/delete.
 *		Such callbacks will be called in reverse order of registration.
 *
 * The caller is responsible for allocating a MemoryContextCallback struct
 * to hold the info about this callback request, and for filling in the
 * "func" and "arg" fields in the struct to show what function to call with
 * what argument.  Typically the callback struct should be allocated within
 * the specified context, since that means it will automatically be freed
 * when no longer needed.
 *
 * There is no API for deregistering a callback once registered.  If you
 * want it to not do anything anymore, adjust the state pointed to by its
 * "arg" to indicate that.
 */
void
MemoryContextRegisterResetCallback(MemoryContext context,
								   MemoryContextCallback *cb)
{
	Assert(MemoryContextIsValid(context));

	/* Push onto head so this will be called before older registrants. */
	cb->next = context->reset_cbs;
	context->reset_cbs = cb;
	/* Mark the context as non-reset (it probably is already). */
	context->isReset = false;
}

/*
 * MemoryContextCallResetCallbacks
 *		Internal function to call all registered callbacks for context.
 */
static void
MemoryContextCallResetCallbacks(MemoryContext context)
{
	MemoryContextCallback *cb;

	/*
	 * We pop each callback from the list before calling.  That way, if an
	 * error occurs inside the callback, we won't try to call it a second time
	 * in the likely event that we reset or delete the context later.
	 */
	while ((cb = context->reset_cbs) != NULL)
	{
		context->reset_cbs = cb->next;
		cb->func(cb->arg);
	}
}

/*
 * MemoryContextSetIdentifier
 *		Set the identifier string for a memory context.
 *
 * An identifier can be provided to help distinguish among different contexts
 * of the same kind in memory context stats dumps.  The identifier string
 * must live at least as long as the context it is for; typically it is
 * allocated inside that context, so that it automatically goes away on
 * context deletion.  Pass id = NULL to forget any old identifier.
 */
void
MemoryContextSetIdentifier(MemoryContext context, const char *id)
{
	Assert(MemoryContextIsValid(context));
	context->ident = id;
}

/*
 * MemoryContextSetParent
 *		Change a context to belong to a new parent (or no parent).
 *
 * We provide this as an API function because it is sometimes useful to
 * change a context's lifespan after creation.  For example, a context
 * might be created underneath a transient context, filled with data,
 * and then reparented underneath CacheMemoryContext to make it long-lived.
 * In this way no special effort is needed to get rid of the context in case
 * a failure occurs before its contents are completely set up.
 *
 * Callers often assume that this function cannot fail, so don't put any
 * elog(ERROR) calls in it.
 *
 * A possible caller error is to reparent a context under itself, creating
 * a loop in the context graph.  We assert here that context != new_parent,
 * but checking for multi-level loops seems more trouble than it's worth.
 */
void
MemoryContextSetParent(MemoryContext context, MemoryContext new_parent)
{
	Assert(MemoryContextIsValid(context));
	Assert(context != new_parent);

	/* Fast path if it's got correct parent already */
	if (new_parent == context->parent)
		return;

	/* Delink from existing parent, if any */
	if (context->parent)
	{
		MemoryContext parent = context->parent;

		if (context->prevchild != NULL)
			context->prevchild->nextchild = context->nextchild;
		else
		{
			Assert(parent->firstchild == context);
			parent->firstchild = context->nextchild;
		}

		if (context->nextchild != NULL)
			context->nextchild->prevchild = context->prevchild;
	}

	/* And relink */
	if (new_parent)
	{
		Assert(MemoryContextIsValid(new_parent));
		context->parent = new_parent;
		context->prevchild = NULL;
		context->nextchild = new_parent->firstchild;
		if (new_parent->firstchild != NULL)
			new_parent->firstchild->prevchild = context;
		new_parent->firstchild = context;
	}
	else
	{
		context->parent = NULL;
		context->prevchild = NULL;
		context->nextchild = NULL;
	}
}

/*
 * MemoryContextAllowInCriticalSection
 *		Allow/disallow allocations in this memory context within a critical
 *		section.
 *
 * Normally, memory allocations are not allowed within a critical section,
 * because a failure would lead to PANIC.  There are a few exceptions to
 * that, like allocations related to debugging code that is not supposed to
 * be enabled in production.  This function can be used to exempt specific
 * memory contexts from the assertion in palloc().
 */
void
MemoryContextAllowInCriticalSection(MemoryContext context, bool allow)
{
	Assert(MemoryContextIsValid(context));

	context->allowInCritSection = allow;
}

/*
 * GetMemoryChunkContext
 *		Given a currently-allocated chunk, determine the MemoryContext that
 *		the chunk belongs to.
 */
MemoryContext
GetMemoryChunkContext(void *pointer)
{
	return MCXT_METHOD(pointer, get_chunk_context) (pointer);
}

/*
 * GetMemoryChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 *
 * This is useful for measuring the total space occupied by a set of
 * allocated chunks.
 */
Size
GetMemoryChunkSpace(void *pointer)
{
	return MCXT_METHOD(pointer, get_chunk_space) (pointer);
}

/*
 * MemoryContextGetParent
 *		Get the parent context (if any) of the specified context
 */
MemoryContext
MemoryContextGetParent(MemoryContext context)
{
	Assert(MemoryContextIsValid(context));

	return context->parent;
}

/*
 * MemoryContextIsEmpty
 *		Is a memory context empty of any allocated space?
 */
bool
MemoryContextIsEmpty(MemoryContext context)
{
	Assert(MemoryContextIsValid(context));

	/*
	 * For now, we consider a memory context nonempty if it has any children;
	 * perhaps this should be changed later.
	 */
	if (context->firstchild != NULL)
		return false;
	/* Otherwise use the type-specific inquiry */
	return context->methods->is_empty(context);
}

/*
 * Find the memory allocated to blocks for this memory context. If recurse is
 * true, also include children.
 */
Size
MemoryContextMemAllocated(MemoryContext context, bool recurse)
{
	Size		total = context->mem_allocated;

	Assert(MemoryContextIsValid(context));

	if (recurse)
	{
		MemoryContext child;

		for (child = context->firstchild;
			 child != NULL;
			 child = child->nextchild)
			total += MemoryContextMemAllocated(child, true);
	}

	return total;
}

/*
 * MemoryContextStats
 *		Print statistics about the named context and all its descendants.
 *
 * This is just a debugging utility, so it's not very fancy.  However, we do
 * make some effort to summarize when the output would otherwise be very long.
 * The statistics are sent to stderr.
 */
void
MemoryContextStats(MemoryContext context)
{
	/* A hard-wired limit on the number of children is usually good enough */
	MemoryContextStatsDetail(context, 100, true);
}

/*
 * MemoryContextStatsDetail
 *
 * Entry point for use if you want to vary the number of child contexts shown.
 *
 * If print_to_stderr is true, print statistics about the memory contexts
 * with fprintf(stderr), otherwise use ereport().
 */
void
MemoryContextStatsDetail(MemoryContext context, int max_children,
						 bool print_to_stderr)
{
	MemoryContextCounters grand_totals;

	memset(&grand_totals, 0, sizeof(grand_totals));

	MemoryContextStatsInternal(context, 0, true, max_children, &grand_totals, print_to_stderr);

	if (print_to_stderr)
		fprintf(stderr,
				"Grand total: %zu bytes in %zu blocks; %zu free (%zu chunks); %zu used\n",
				grand_totals.totalspace, grand_totals.nblocks,
				grand_totals.freespace, grand_totals.freechunks,
				grand_totals.totalspace - grand_totals.freespace);
	else

		/*
		 * Use LOG_SERVER_ONLY to prevent the memory contexts from being sent
		 * to the connected client.
		 *
		 * We don't buffer the information about all memory contexts in a
		 * backend into StringInfo and log it as one message.  That would
		 * require the buffer to be enlarged, risking an OOM as there could be
		 * a large number of memory contexts in a backend.  Instead, we log
		 * one message per memory context.
		 */
		ereport(LOG_SERVER_ONLY,
				(errhidestmt(true),
				 errhidecontext(true),
				 errmsg_internal("Grand total: %zu bytes in %zu blocks; %zu free (%zu chunks); %zu used",
								 grand_totals.totalspace, grand_totals.nblocks,
								 grand_totals.freespace, grand_totals.freechunks,
								 grand_totals.totalspace - grand_totals.freespace)));
}

/*
 * MemoryContextStatsInternal
 *		One recursion level for MemoryContextStats
 *
 * Print this context if print is true, but in any case accumulate counts into
 * *totals (if given).
 */
static void
MemoryContextStatsInternal(MemoryContext context, int level,
						   bool print, int max_children,
						   MemoryContextCounters *totals,
						   bool print_to_stderr)
{
	MemoryContextCounters local_totals;
	MemoryContext child;
	int			ichild;

	Assert(MemoryContextIsValid(context));

	/* Examine the context itself */
	context->methods->stats(context,
							print ? MemoryContextStatsPrint : NULL,
							(void *) &level,
							totals, print_to_stderr);

	/*
	 * Examine children.  If there are more than max_children of them, we do
	 * not print the rest explicitly, but just summarize them.
	 */
	memset(&local_totals, 0, sizeof(local_totals));

	for (child = context->firstchild, ichild = 0;
		 child != NULL;
		 child = child->nextchild, ichild++)
	{
		if (ichild < max_children)
			MemoryContextStatsInternal(child, level + 1,
									   print, max_children,
									   totals,
									   print_to_stderr);
		else
			MemoryContextStatsInternal(child, level + 1,
									   false, max_children,
									   &local_totals,
									   print_to_stderr);
	}

	/* Deal with excess children */
	if (ichild > max_children)
	{
		if (print)
		{
			if (print_to_stderr)
			{
				int			i;

				for (i = 0; i <= level; i++)
					fprintf(stderr, "  ");
				fprintf(stderr,
						"%d more child contexts containing %zu total in %zu blocks; %zu free (%zu chunks); %zu used\n",
						ichild - max_children,
						local_totals.totalspace,
						local_totals.nblocks,
						local_totals.freespace,
						local_totals.freechunks,
						local_totals.totalspace - local_totals.freespace);
			}
			else
				ereport(LOG_SERVER_ONLY,
						(errhidestmt(true),
						 errhidecontext(true),
						 errmsg_internal("level: %d; %d more child contexts containing %zu total in %zu blocks; %zu free (%zu chunks); %zu used",
										 level,
										 ichild - max_children,
										 local_totals.totalspace,
										 local_totals.nblocks,
										 local_totals.freespace,
										 local_totals.freechunks,
										 local_totals.totalspace - local_totals.freespace)));
		}

		if (totals)
		{
			totals->nblocks += local_totals.nblocks;
			totals->freechunks += local_totals.freechunks;
			totals->totalspace += local_totals.totalspace;
			totals->freespace += local_totals.freespace;
		}
	}
}

/*
 * MemoryContextStatsPrint
 *		Print callback used by MemoryContextStatsInternal
 *
 * For now, the passthru pointer just points to "int level"; later we might
 * make that more complicated.
 */
static void
MemoryContextStatsPrint(MemoryContext context, void *passthru,
						const char *stats_string,
						bool print_to_stderr)
{
	int			level = *(int *) passthru;
	const char *name = context->name;
	const char *ident = context->ident;
	char		truncated_ident[110];
	int			i;

	/*
	 * It seems preferable to label dynahash contexts with just the hash table
	 * name.  Those are already unique enough, so the "dynahash" part isn't
	 * very helpful, and this way is more consistent with pre-v11 practice.
	 */
	if (ident && strcmp(name, "dynahash") == 0)
	{
		name = ident;
		ident = NULL;
	}

	truncated_ident[0] = '\0';

	if (ident)
	{
		/*
		 * Some contexts may have very long identifiers (e.g., SQL queries).
		 * Arbitrarily truncate at 100 bytes, but be careful not to break
		 * multibyte characters.  Also, replace ASCII control characters, such
		 * as newlines, with spaces.
		 */
		int			idlen = strlen(ident);
		bool		truncated = false;

		strcpy(truncated_ident, ": ");
		i = strlen(truncated_ident);

		if (idlen > 100)
		{
			idlen = pg_mbcliplen(ident, idlen, 100);
			truncated = true;
		}

		while (idlen-- > 0)
		{
			unsigned char c = *ident++;

			if (c < ' ')
				c = ' ';
			truncated_ident[i++] = c;
		}
		truncated_ident[i] = '\0';

		if (truncated)
			strcat(truncated_ident, "...");
	}

	if (print_to_stderr)
	{
		for (i = 0; i < level; i++)
			fprintf(stderr, "  ");
		fprintf(stderr, "%s: %s%s\n", name, stats_string, truncated_ident);
	}
	else
		ereport(LOG_SERVER_ONLY,
				(errhidestmt(true),
				 errhidecontext(true),
				 errmsg_internal("level: %d; %s: %s%s",
								 level, name, stats_string, truncated_ident)));
}

/*
 * MemoryContextCheck
 *		Check all chunks in the named context.
 *
 * This is just a debugging utility, so it's not fancy.
 */
#ifdef MEMORY_CONTEXT_CHECKING
void
MemoryContextCheck(MemoryContext context)
{
	MemoryContext child;

	Assert(MemoryContextIsValid(context));

	context->methods->check(context);
	for (child = context->firstchild; child != NULL; child = child->nextchild)
		MemoryContextCheck(child);
}
#endif

/*
 * MemoryContextCreate
 *		Context-type-independent part of context creation.
 *
 * This is only intended to be called by context-type-specific
 * context creation routines, not by the unwashed masses.
 *
 * The memory context creation procedure goes like this:
 *	1.  Context-type-specific routine makes some initial space allocation,
 *		including enough space for the context header.  If it fails,
 *		it can ereport() with no damage done.
 *	2.	Context-type-specific routine sets up all type-specific fields of
 *		the header (those beyond MemoryContextData proper), as well as any
 *		other management fields it needs to have a fully valid context.
 *		Usually, failure in this step is impossible, but if it's possible
 *		the initial space allocation should be freed before ereport'ing.
 *	3.	Context-type-specific routine calls MemoryContextCreate() to fill in
 *		the generic header fields and link the context into the context tree.
 *	4.  We return to the context-type-specific routine, which finishes
 *		up type-specific initialization.  This routine can now do things
 *		that might fail (like allocate more memory), so long as it's
 *		sure the node is left in a state that delete will handle.
 *
 * node: the as-yet-uninitialized common part of the context header node.
 * tag: NodeTag code identifying the memory context type.
 * method_id: MemoryContextMethodID of the context-type being created.
 * parent: parent context, or NULL if this will be a top-level context.
 * name: name of context (must be statically allocated).
 *
 * Context routines generally assume that MemoryContextCreate can't fail,
 * so this can contain Assert but not elog/ereport.
 */
void
MemoryContextCreate(MemoryContext node,
					NodeTag tag,
					MemoryContextMethodID method_id,
					MemoryContext parent,
					const char *name)
{
	/* Creating new memory contexts is not allowed in a critical section */
	Assert(CritSectionCount == 0);

	/* Initialize all standard fields of memory context header */
	node->type = tag;
	node->isReset = true;
	node->methods = &mcxt_methods[method_id];
	node->parent = parent;
	node->firstchild = NULL;
	node->mem_allocated = 0;
	node->prevchild = NULL;
	node->name = name;
	node->ident = NULL;
	node->reset_cbs = NULL;

	/* OK to link node into context tree */
	if (parent)
	{
		node->nextchild = parent->firstchild;
		if (parent->firstchild != NULL)
			parent->firstchild->prevchild = node;
		parent->firstchild = node;
		/* inherit allowInCritSection flag from parent */
		node->allowInCritSection = parent->allowInCritSection;
	}
	else
	{
		node->nextchild = NULL;
		node->allowInCritSection = false;
	}

	VALGRIND_CREATE_MEMPOOL(node, 0, false);
}

/*
 * MemoryContextAlloc
 *		Allocate space within the specified context.
 *
 * This could be turned into a macro, but we'd have to import
 * nodes/memnodes.h into postgres.h which seems a bad idea.
 */
void *
MemoryContextAlloc(MemoryContext context, Size size)
{
	void	   *ret;

	Assert(MemoryContextIsValid(context));
	AssertNotInCriticalSection(context);

	if (!AllocSizeIsValid(size))
		elog(ERROR, "invalid memory alloc request size %zu", size);

	context->isReset = false;

	ret = context->methods->alloc(context, size);
	if (unlikely(ret == NULL))
	{
		MemoryContextStats(TopMemoryContext);

		/*
		 * Here, and elsewhere in this module, we show the target context's
		 * "name" but not its "ident" (if any) in user-visible error messages.
		 * The "ident" string might contain security-sensitive data, such as
		 * values in SQL commands.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed on request of size %zu in memory context \"%s\".",
						   size, context->name)));
	}

	VALGRIND_MEMPOOL_ALLOC(context, ret, size);

	return ret;
}

/*
 * MemoryContextAllocZero
 *		Like MemoryContextAlloc, but clears allocated memory
 *
 *	We could just call MemoryContextAlloc then clear the memory, but this
 *	is a very common combination, so we provide the combined operation.
 */
void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	void	   *ret;

	Assert(MemoryContextIsValid(context));
	AssertNotInCriticalSection(context);

	if (!AllocSizeIsValid(size))
		elog(ERROR, "invalid memory alloc request size %zu", size);

	context->isReset = false;

	ret = context->methods->alloc(context, size);
	if (unlikely(ret == NULL))
	{
		MemoryContextStats(TopMemoryContext);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed on request of size %zu in memory context \"%s\".",
						   size, context->name)));
	}

	VALGRIND_MEMPOOL_ALLOC(context, ret, size);

	MemSetAligned(ret, 0, size);

	return ret;
}

/*
 * MemoryContextAllocExtended
 *		Allocate space within the specified context using the given flags.
 */
void *
MemoryContextAllocExtended(MemoryContext context, Size size, int flags)
{
	void	   *ret;

	Assert(MemoryContextIsValid(context));
	AssertNotInCriticalSection(context);

	if (!((flags & MCXT_ALLOC_HUGE) != 0 ? AllocHugeSizeIsValid(size) :
		  AllocSizeIsValid(size)))
		elog(ERROR, "invalid memory alloc request size %zu", size);

	context->isReset = false;

	ret = context->methods->alloc(context, size);
	if (unlikely(ret == NULL))
	{
		if ((flags & MCXT_ALLOC_NO_OOM) == 0)
		{
			MemoryContextStats(TopMemoryContext);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail("Failed on request of size %zu in memory context \"%s\".",
							   size, context->name)));
		}
		return NULL;
	}

	VALGRIND_MEMPOOL_ALLOC(context, ret, size);

	if ((flags & MCXT_ALLOC_ZERO) != 0)
		MemSetAligned(ret, 0, size);

	return ret;
}

/*
 * HandleLogMemoryContextInterrupt
 *		Handle receipt of an interrupt indicating logging of memory
 *		contexts.
 *
 * All the actual work is deferred to ProcessLogMemoryContextInterrupt(),
 * because we cannot safely emit a log message inside the signal handler.
 */
void
HandleLogMemoryContextInterrupt(void)
{
	InterruptPending = true;
	LogMemoryContextPending = true;
	/* latch will be set by procsignal_sigusr1_handler */
}

/*
 * ProcessLogMemoryContextInterrupt
 * 		Perform logging of memory contexts of this backend process.
 *
 * Any backend that participates in ProcSignal signaling must arrange
 * to call this function if we see LogMemoryContextPending set.
 * It is called from CHECK_FOR_INTERRUPTS(), which is enough because
 * the target process for logging of memory contexts is a backend.
 */
void
ProcessLogMemoryContextInterrupt(void)
{
	LogMemoryContextPending = false;

	/*
	 * Use LOG_SERVER_ONLY to prevent this message from being sent to the
	 * connected client.
	 */
	ereport(LOG_SERVER_ONLY,
			(errhidestmt(true),
			 errhidecontext(true),
			 errmsg("logging memory contexts of PID %d", MyProcPid)));

	/*
	 * When a backend process is consuming huge memory, logging all its memory
	 * contexts might overrun available disk space. To prevent this, we limit
	 * the number of child contexts to log per parent to 100.
	 *
	 * As with MemoryContextStats(), we suppose that practical cases where the
	 * dump gets long will typically be huge numbers of siblings under the
	 * same parent context; while the additional debugging value from seeing
	 * details about individual siblings beyond 100 will not be large.
	 */
	MemoryContextStatsDetail(TopMemoryContext, 100, false);
}

void *
palloc(Size size)
{
	/* duplicates MemoryContextAlloc to avoid increased overhead */
	void	   *ret;
	MemoryContext context = CurrentMemoryContext;

	Assert(MemoryContextIsValid(context));
	AssertNotInCriticalSection(context);

	if (!AllocSizeIsValid(size))
		elog(ERROR, "invalid memory alloc request size %zu", size);

	context->isReset = false;

	ret = context->methods->alloc(context, size);
	if (unlikely(ret == NULL))
	{
		MemoryContextStats(TopMemoryContext);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed on request of size %zu in memory context \"%s\".",
						   size, context->name)));
	}

	VALGRIND_MEMPOOL_ALLOC(context, ret, size);

	return ret;
}

void *
palloc0(Size size)
{
	/* duplicates MemoryContextAllocZero to avoid increased overhead */
	void	   *ret;
	MemoryContext context = CurrentMemoryContext;

	Assert(MemoryContextIsValid(context));
	AssertNotInCriticalSection(context);

	if (!AllocSizeIsValid(size))
		elog(ERROR, "invalid memory alloc request size %zu", size);

	context->isReset = false;

	ret = context->methods->alloc(context, size);
	if (unlikely(ret == NULL))
	{
		MemoryContextStats(TopMemoryContext);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed on request of size %zu in memory context \"%s\".",
						   size, context->name)));
	}

	VALGRIND_MEMPOOL_ALLOC(context, ret, size);

	MemSetAligned(ret, 0, size);

	return ret;
}

void *
palloc_extended(Size size, int flags)
{
	/* duplicates MemoryContextAllocExtended to avoid increased overhead */
	void	   *ret;
	MemoryContext context = CurrentMemoryContext;

	Assert(MemoryContextIsValid(context));
	AssertNotInCriticalSection(context);

	if (!((flags & MCXT_ALLOC_HUGE) != 0 ? AllocHugeSizeIsValid(size) :
		  AllocSizeIsValid(size)))
		elog(ERROR, "invalid memory alloc request size %zu", size);

	context->isReset = false;

	ret = context->methods->alloc(context, size);
	if (unlikely(ret == NULL))
	{
		if ((flags & MCXT_ALLOC_NO_OOM) == 0)
		{
			MemoryContextStats(TopMemoryContext);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail("Failed on request of size %zu in memory context \"%s\".",
							   size, context->name)));
		}
		return NULL;
	}

	VALGRIND_MEMPOOL_ALLOC(context, ret, size);

	if ((flags & MCXT_ALLOC_ZERO) != 0)
		MemSetAligned(ret, 0, size);

	return ret;
}

/*
 * MemoryContextAllocAligned
 *		Allocate 'size' bytes of memory in 'context' aligned to 'alignto'
 *		bytes.
 *
 * Currently, we align addresses by requesting additional bytes from the
 * MemoryContext's standard allocator function and then aligning the returned
 * address by the required alignment.  This means that the given MemoryContext
 * must support providing us with a chunk of memory that's larger than 'size'.
 * For allocators such as Slab, that's not going to work, as slab only allows
 * chunks of the size that's specified when the context is created.
 *
 * 'alignto' must be a power of 2.
 * 'flags' may be 0 or set the same as MemoryContextAllocExtended().
 */
void *
MemoryContextAllocAligned(MemoryContext context,
						  Size size, Size alignto, int flags)
{
	MemoryChunk *alignedchunk;
	Size		alloc_size;
	void	   *unaligned;
	void	   *aligned;

	/* wouldn't make much sense to waste that much space */
	Assert(alignto < (128 * 1024 * 1024));

	/* ensure alignto is a power of 2 */
	Assert((alignto & (alignto - 1)) == 0);

	/*
	 * If the alignment requirements are less than what we already guarantee
	 * then just use the standard allocation function.
	 */
	if (unlikely(alignto <= MAXIMUM_ALIGNOF))
		return MemoryContextAllocExtended(context, size, flags);

	/*
	 * We implement aligned pointers by simply allocating enough memory for
	 * the requested size plus the alignment and an additional "redirection"
	 * MemoryChunk.  This additional MemoryChunk is required for operations
	 * such as pfree when used on the pointer returned by this function.  We
	 * use this redirection MemoryChunk in order to find the pointer to the
	 * memory that was returned by the MemoryContextAllocExtended call below.
	 * We do that by "borrowing" the block offset field and instead of using
	 * that to find the offset into the owning block, we use it to find the
	 * original allocated address.
	 *
	 * Here we must allocate enough extra memory so that we can still align
	 * the pointer returned by MemoryContextAllocExtended and also have enough
	 * space for the redirection MemoryChunk.  Since allocations will already
	 * be at least aligned by MAXIMUM_ALIGNOF, we can subtract that amount
	 * from the allocation size to save a little memory.
	 */
	alloc_size = size + PallocAlignedExtraBytes(alignto);

#ifdef MEMORY_CONTEXT_CHECKING
	/* ensure there's space for a sentinel byte */
	alloc_size += 1;
#endif

	/* perform the actual allocation */
	unaligned = MemoryContextAllocExtended(context, alloc_size, flags);

	/* set the aligned pointer */
	aligned = (void *) TYPEALIGN(alignto, (char *) unaligned +
								 sizeof(MemoryChunk));

	alignedchunk = PointerGetMemoryChunk(aligned);

	/*
	 * We set the redirect MemoryChunk so that the block offset calculation is
	 * used to point back to the 'unaligned' allocated chunk.  This allows us
	 * to use MemoryChunkGetBlock() to find the unaligned chunk when we need
	 * to perform operations such as pfree() and repalloc().
	 *
	 * We store 'alignto' in the MemoryChunk's 'value' so that we know what
	 * the alignment was set to should we ever be asked to realloc this
	 * pointer.
	 */
	MemoryChunkSetHdrMask(alignedchunk, unaligned, alignto,
						  MCTX_ALIGNED_REDIRECT_ID);

	/* double check we produced a correctly aligned pointer */
	Assert((void *) TYPEALIGN(alignto, aligned) == aligned);

#ifdef MEMORY_CONTEXT_CHECKING
	alignedchunk->requested_size = size;
	/* set mark to catch clobber of "unused" space */
	set_sentinel(aligned, size);
#endif

	/* Mark the bytes before the redirection header as noaccess */
	VALGRIND_MAKE_MEM_NOACCESS(unaligned,
							   (char *) alignedchunk - (char *) unaligned);

	/* Disallow access to the redirection chunk header. */
	VALGRIND_MAKE_MEM_NOACCESS(alignedchunk, sizeof(MemoryChunk));

	return aligned;
}

/*
 * palloc_aligned
 *		Allocate 'size' bytes returning a pointer that's aligned to the
 *		'alignto' boundary.
 *
 * Currently, we align addresses by requesting additional bytes from the
 * MemoryContext's standard allocator function and then aligning the returned
 * address by the required alignment.  This means that the given MemoryContext
 * must support providing us with a chunk of memory that's larger than 'size'.
 * For allocators such as Slab, that's not going to work, as slab only allows
 * chunks of the size that's specified when the context is created.
 *
 * 'alignto' must be a power of 2.
 * 'flags' may be 0 or set the same as MemoryContextAllocExtended().
 */
void *
palloc_aligned(Size size, Size alignto, int flags)
{
	return MemoryContextAllocAligned(CurrentMemoryContext, size, alignto, flags);
}

/*
 * pfree
 *		Release an allocated chunk.
 */
void
pfree(void *pointer)
{
#ifdef USE_VALGRIND
	MemoryContextMethodID method = GetMemoryChunkMethodID(pointer);
	MemoryContext context = GetMemoryChunkContext(pointer);
#endif

	MCXT_METHOD(pointer, free_p) (pointer);

#ifdef USE_VALGRIND
	if (method != MCTX_ALIGNED_REDIRECT_ID)
		VALGRIND_MEMPOOL_FREE(context, pointer);
#endif
}

/*
 * repalloc
 *		Adjust the size of a previously allocated chunk.
 */
void *
repalloc(void *pointer, Size size)
{
#ifdef USE_VALGRIND
	MemoryContextMethodID method = GetMemoryChunkMethodID(pointer);
#endif
#if defined(USE_ASSERT_CHECKING) || defined(USE_VALGRIND)
	MemoryContext context = GetMemoryChunkContext(pointer);
#endif
	void	   *ret;

	if (!AllocSizeIsValid(size))
		elog(ERROR, "invalid memory alloc request size %zu", size);

	AssertNotInCriticalSection(context);

	/* isReset must be false already */
	Assert(!context->isReset);

	ret = MCXT_METHOD(pointer, realloc) (pointer, size);
	if (unlikely(ret == NULL))
	{
		MemoryContext cxt = GetMemoryChunkContext(pointer);

		MemoryContextStats(TopMemoryContext);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed on request of size %zu in memory context \"%s\".",
						   size, cxt->name)));
	}

#ifdef USE_VALGRIND
	if (method != MCTX_ALIGNED_REDIRECT_ID)
		VALGRIND_MEMPOOL_CHANGE(context, pointer, ret, size);
#endif

	return ret;
}

/*
 * repalloc_extended
 *		Adjust the size of a previously allocated chunk,
 *		with HUGE and NO_OOM options.
 */
void *
repalloc_extended(void *pointer, Size size, int flags)
{
#if defined(USE_ASSERT_CHECKING) || defined(USE_VALGRIND)
	MemoryContext context = GetMemoryChunkContext(pointer);
#endif
	void	   *ret;

	if (!((flags & MCXT_ALLOC_HUGE) != 0 ? AllocHugeSizeIsValid(size) :
		  AllocSizeIsValid(size)))
		elog(ERROR, "invalid memory alloc request size %zu", size);

	AssertNotInCriticalSection(context);

	/* isReset must be false already */
	Assert(!context->isReset);

	ret = MCXT_METHOD(pointer, realloc) (pointer, size);
	if (unlikely(ret == NULL))
	{
		if ((flags & MCXT_ALLOC_NO_OOM) == 0)
		{
			MemoryContext cxt = GetMemoryChunkContext(pointer);

			MemoryContextStats(TopMemoryContext);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail("Failed on request of size %zu in memory context \"%s\".",
							   size, cxt->name)));
		}
		return NULL;
	}

	VALGRIND_MEMPOOL_CHANGE(context, pointer, ret, size);

	return ret;
}

/*
 * repalloc0
 *		Adjust the size of a previously allocated chunk and zero out the added
 *		space.
 */
void *
repalloc0(void *pointer, Size oldsize, Size size)
{
	void	   *ret;

	/* catch wrong argument order */
	if (unlikely(oldsize > size))
		elog(ERROR, "invalid repalloc0 call: oldsize %zu, new size %zu",
			 oldsize, size);

	ret = repalloc(pointer, size);
	memset((char *) ret + oldsize, 0, (size - oldsize));
	return ret;
}

/*
 * MemoryContextAllocHuge
 *		Allocate (possibly-expansive) space within the specified context.
 *
 * See considerations in comment at MaxAllocHugeSize.
 */
void *
MemoryContextAllocHuge(MemoryContext context, Size size)
{
	void	   *ret;

	Assert(MemoryContextIsValid(context));
	AssertNotInCriticalSection(context);

	if (!AllocHugeSizeIsValid(size))
		elog(ERROR, "invalid memory alloc request size %zu", size);

	context->isReset = false;

	ret = context->methods->alloc(context, size);
	if (unlikely(ret == NULL))
	{
		MemoryContextStats(TopMemoryContext);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed on request of size %zu in memory context \"%s\".",
						   size, context->name)));
	}

	VALGRIND_MEMPOOL_ALLOC(context, ret, size);

	return ret;
}

/*
 * repalloc_huge
 *		Adjust the size of a previously allocated chunk, permitting a large
 *		value.  The previous allocation need not have been "huge".
 */
void *
repalloc_huge(void *pointer, Size size)
{
	/* this one seems not worth its own implementation */
	return repalloc_extended(pointer, size, MCXT_ALLOC_HUGE);
}

/*
 * MemoryContextStrdup
 *		Like strdup(), but allocate from the specified context
 */
char *
MemoryContextStrdup(MemoryContext context, const char *string)
{
	char	   *nstr;
	Size		len = strlen(string) + 1;

	nstr = (char *) MemoryContextAlloc(context, len);

	memcpy(nstr, string, len);

	return nstr;
}

char *
pstrdup(const char *in)
{
	return MemoryContextStrdup(CurrentMemoryContext, in);
}

/*
 * pnstrdup
 *		Like pstrdup(), but append null byte to a
 *		not-necessarily-null-terminated input string.
 */
char *
pnstrdup(const char *in, Size len)
{
	char	   *out;

	len = strnlen(in, len);

	out = palloc(len + 1);
	memcpy(out, in, len);
	out[len] = '\0';

	return out;
}

/*
 * Make copy of string with all trailing newline characters removed.
 */
char *
pchomp(const char *in)
{
	size_t		n;

	n = strlen(in);
	while (n > 0 && in[n - 1] == '\n')
		n--;
	return pnstrdup(in, n);
}

/*
 * Memory Pools
 *
 * Contexts may get memory either directly from the OS (libc) through malloc
 * calls, but that has non-trivial overhead, depending on the allocation size
 * and so on. And we tend to allocate fairly large amounts of memory, because
 * contexts allocate blocks (starting with 1kB, quickly growing by doubling).
 * A lot of hot paths also allocate pieces of memory exceeding the size limit
 * and being allocated as a separate block.
 *
 * The contexts may cache the memory by keeping chunks, but it's limited to a
 * single memory context (as AllocSet freelist), and only for the lifetime of
 * a particular context instance. When the memory is reset/deleted, all the
 * blocks are freed and retuned to the OS (libc).
 *
 * There's a rudimentary cache of memory contexts blocks, but this only keeps
 * the keeper blocks, not any other blocks that may be needed.
 *
 * Memory pools are attempt to improve this by establishing a cache of blocks
 * shared by all the memory contexts. A memory pool allocates blocks larger
 * than 1kB, with doubling (1kB, 2kB, 4kB, ...). All the allocations come
 * from memory contexts, and are either regular blocks (also starting at 1kB)
 * or oversized chunks (a couple kB or larger). This means the lower limit
 * is reasonable - there should be no smaller allocations.
 *
 * There's no explicit upper size limit - whatever could be used by palloc()
 * can be requested from the pool. However, only blocks up to 8MB may be
 * cached by the pool - larger allocations are not kept after pfree().
 *
 * To make the reuse possible, the blocks are grouped into size clasess the
 * same way AllocSet uses for chunks. There are 14 size classes, starting
 * at 1kB and ending at 8MB.
 *
 * This "rouding" applies even to oversized chunks. So e.g. allocating 27kB
 * will allocate a 32kB block. This wastes memory, but it means the block
 * may be reused by "regular" allocations. The amount of wasted memory could
 * be reduced by using size classes with smaller steps, but that reduces the
 * likelihood of reusing the block.
 */


#define MEMPOOL_MIN_BLOCK	1024L				/* smallest cached block */
#define MEMPOOL_MAX_BLOCK	(8*1024L*1024L)		/* largest cached block */
#define MEMPOOL_SIZES		14					/* 1kB -> 8MB */

/*
 * Maximum amount of memory to keep in cache for all size buckets. Sets a
 * safety limit limit set on the blocks kept in the *cached* part of the
 * pool. Each bucket starts with the same amount of memory (1/14 of this)
 * and then we adapt the cache depending on cache hits/misses.
 */
#define MEMPOOL_SIZE_MAX	(128*1024L*1024L)

/*
 * Maximum number of blocks kept for the whole memory pool. This is used
 * only to allocate the entries, so we assume all are in the smallest size
 * bucket.
 */
#define MEMPOOL_MAX_BLOCKS	(MEMPOOL_SIZE_MAX / MEMPOOL_MIN_BLOCK)

/*
 * How often to rebalance the memory pool buckets (number of allocations).
 * This is a tradeoff between the pool being adaptive and more overhead.
 */
#define	MEMPOOL_REBALANCE_DISTANCE		25000

/*
 * To enable debug logging for the memory pool code, build with -DMEMPOOL_DEBUG.
 */
#ifdef MEMPOOL_DEBUG

#undef MEMPOOL_DEBUG
#define	MEMPOOL_RANDOMIZE(ptr, size)	memset((ptr), 0x7f, (size))
#define MEMPOOL_DEBUG(...)	fprintf (stderr, __VA_ARGS__)

#else

#define MEMPOOL_DEBUG(...)
#define MEMPOOL_RANDOMIZE(ptr, size)

#endif	/* MEMPOOL_DEBUG */


/*
 * Entries for a simple linked list of blocks to reuse.
 */
typedef struct MemPoolEntry
{
	void   *ptr;	/* allocated block (NULL in empty entries) */
	struct	MemPoolEntry *next;
} MemPoolEntry;

/*
 * Information about allocations of blocks of a certain size. We track both the
 * number of currently cached blocks, and also the number of allocated 
 */
typedef struct MemPoolBucket
{
	int				nhits;			/* allocation cache hits */
	int				nmisses;		/* allocation cache misses */
	int				nallocated;		/* number of currently allocated blocks */
	int				maxallocated;	/* max number of allocated blocks */
	int				ncached;		/* number of free blocks (entry list) */
	int				maxcached;		/* max number of free blocks to cache */
	MemPoolEntry   *entry;
} MemPoolBucket;

/*
 * MemPool - memory pool, caching allocations between memory contexts
 *
 * cache - stores free-d blocks that may be reused for future allocations,
 * each slot is a list of MemPoolEntry elements using the "entries"
 *
 * entries - pre-allocated entries for the freelists, used by cache lists
 *
 * freelist - list of free cache entries (not used by the cache lists)
 *
 * The meaning of the freelist is somewhat inverse - when a block of freed,
 * we need to add it to the cache. Hence we get an entry from the freelist,
 * and add it to the cache. So a free-d block removes and entry from the
 * mempool freelist.
 */
typedef struct MemPool
{
	/* LIFO cache of free-d blocks of eligible sizes (1kB - 1MB, doubled) */
	MemPoolBucket	cache[MEMPOOL_SIZES];

	/* pre-allocated entries for cache of free-d blocks */
	MemPoolEntry	entries[MEMPOOL_SIZES * MEMPOOL_MAX_BLOCKS];

	/* head of freelist (entries from the array) */
	MemPoolEntry   *freelist;

	/* memory limit / accounting */
	int64 mem_allowed;
	int64 mem_allocated;
	int64 mem_cached;
	int64 num_requests;
} MemPool;

static MemPool *pool = NULL;

static void
AssertCheckMemPool(MemPool *p)
{
#ifdef ASSERT_CHECKING
	int	nused = 0;
	int	nfree = 0;
	int64	mem_cached = 0;
	Size	block_size = MEMPOOL_MIN_BLOCK;

	Assert(p->mem_allocated >= 0);
	Assert(p->mem_cached >= 0);

	/* count the elements in the various cache buckets */
	for (int i = 0; i < MEMPOOL_SIZES; i++)
	{
		int	count = 0;

		Assert(p->cache[i].ncached >= 0);
		Assert(p->cache[i].ncached <= p->cache[i].maxcached);

		entry = p->cache[i].entry;

		while (entry)
		{
			Assert(entry->ptr);

			entry = entry->next;
			count++;
		}

		Assert(count == p->cache[i].ncached);

		nused += count;
		mem_cached += (count * block_size);

		block_size *= 2;
	}

	/* now count the elements in the freelist */
	entry = p->freelist;
	while (entry)
	{
		nfree++;
		entry = entry->next;
	}

	Assert(nfree + nused == MEMPOOL_SIZES * MEMPOOL_MAX_BLOCKS);
	Assert(mem_cached == p->mem_cached);
#endif
}

static void MemoryPoolRebalanceBuckets(void);
static void MemoryPoolEnforceSizeLimit(Size request_size, int index);

/*
 * MemoryPoolInit
 *		initialize the global memory pool
 *
 * Initialize the overall memory pool structure, and also link all entries
 * into a freelist.
 */
static void
MemoryPoolInit(void)
{
	Size	size = MEMPOOL_MIN_BLOCK;

	/* bail out if already initialized */
	if (pool)
		return;

	/* allocate the basic structure */
	pool = malloc(sizeof(MemPool));
	memset(pool, 0, sizeof(MemPool));

	/* initialize the frelist - put all entries to the list */
	pool->freelist = &pool->entries[0];

	for (int i = 0; i < (MEMPOOL_SIZES * MEMPOOL_MAX_BLOCKS - 1); i++)
	{
		if (i < (MEMPOOL_SIZES * MEMPOOL_MAX_BLOCKS - 1))
			pool->entries[i].next = &pool->entries[i+1];
		else
			pool->entries[i].next = NULL;
	}

	/* set default maximum counts of entries for each size class */
	for (int i = 0; i < MEMPOOL_SIZES; i++)
	{
		pool->cache[i].maxcached = (MEMPOOL_SIZE_MAX / MEMPOOL_SIZES / size);
		size *= 2;
	}

	AssertCheckMemPool(pool);
}

/*
 * MemoryPoolEntrySize
 *		calculate the size of the block to allocate for a given request size
 *
 * The request sizes are grouped into pow(2,n) classes, starting at 1kB and
 * ending at 8MB. Which means there are 14 size classes.
 */
static Size
MemoryPoolEntrySize(Size size)
{
	Size	result;

	/*
	 * We shouldn't really get many malloc() for such small elements through
	 * memory contexts, so just use the smallest block.
	 */
	if (size < MEMPOOL_MIN_BLOCK)
		return MEMPOOL_MIN_BLOCK;

	/*
	 * We can get various large allocations - we don't want to cache those,
	 * not waste space on doubling them, so just allocate them directly.
	 * Maybe the limit should be separate/lower, like 1MB.
	 */
	if (size > MEMPOOL_MAX_BLOCK)
		return size;

	/*
	 * Otherwise just calculate the first block larger than the request.
	 *
	 * XXX Maybe there's a better way to calculate this? The number of loops
	 * should be very low, though (less than MEMPOOL_SIZES, i.e. 14).
	 */
	result = MEMPOOL_MIN_BLOCK;
	while (size > result)
		result *= 2;

	MEMPOOL_DEBUG("%d MempoolEntrySize %lu => %lu\n", getpid(), size, result);

	/* the block size has to be sufficient for the requested size */
	Assert(size <= result);

	return result;
}

/*
 * MemoryPoolEntryIndex
 *		Calculate the cache index for a given entry size.
 *
 * XXX Always called right after MemoryPoolEntrySize, so maybe it should be
 * merged into a single function, so that the loop happens only once.
 */
static int
MemoryPoolEntryIndex(Size size)
{
	int		blockIndex = 0;
	Size	blockSize = MEMPOOL_MIN_BLOCK;

	/* is size possibly in cache? */
	if (size < MEMPOOL_MIN_BLOCK || size > MEMPOOL_MAX_BLOCK)
		return -1;

	/* calculate where to maybe cache the entry */
	while (blockSize <= MEMPOOL_MAX_BLOCK)
	{
		Assert(size >= blockSize);

		if (size == blockSize)
		{
			Assert(blockIndex < MEMPOOL_SIZES);
			return blockIndex;
		}

		blockIndex++;
		blockSize *= 2;
	}

	/* not eligible for caching after all */
	return -1;
}

/*
 * Check that the entry size is valid and matches the class index - if smaller
 * than 8MB, it needs to be in one of the valid classes.
 */
static void
AssertCheckEntrySize(Size size, int cacheIndex)
{
#ifdef USE_ASSERT_CHECKING
	int	blockSize = MEMPOOL_MIN_BLOCK;
	int	blockIndex = 0;

	Assert(cacheIndex >= -1 && cacheIndex < MEMPOOL_SIZES);

	/* all sizes in the valid range should be in one of the slots */
	if (cacheIndex == -1)
		Assert(size < MEMPOOL_MIN_BLOCK || size > MEMPOOL_MAX_BLOCK);
	else
	{
		/* calculate the block size / index for the given size */
		while (size > blockSize)
		{
			blockSize *= 2;
			blockIndex++;
		}

		Assert(size == blockSize);
		Assert(cacheIndex == blockIndex);
	}
#endif
}

/*
 * MemoryPoolAlloc
 *		Allocate a block from the memory pool.
 *
 * The block may come either from cache - if available - or from malloc().
 */
void *
MemoryPoolAlloc(Size size)
{
	int	index;
	void *ptr;

	MemoryPoolInit();

	pool->num_requests++;

	MemoryPoolRebalanceBuckets();

	/* maybe override the requested size */
	size = MemoryPoolEntrySize(size);
	index = MemoryPoolEntryIndex(size);

	/* cross-check the size and index */
	AssertCheckEntrySize(size, index);

	/* try to enforce the memory limit */
	MemoryPoolEnforceSizeLimit(size, index);

	/* Is the block eligible to be in the cache? Or is it too large/small? */
	if (index >= 0)
	{
		MemPoolEntry *entry = pool->cache[index].entry;

		/*
		 * update the number of allocated chunks, and the high watermark
		 *
		 * We do this even if there's no entry in the cache.
		 */
		pool->cache[index].nallocated++;
		pool->cache[index].maxallocated = Max(pool->cache[index].nallocated,
											  pool->cache[index].maxallocated);

		/*
		 * If we have a cached block for this size, we're done. Remove it
		 * from the cache and return the entry to the freelist.
		 */
		if (entry != NULL)
		{
			/* remember the pointer (we'll reset the entry) */
			ptr = entry->ptr;
			entry->ptr = NULL;

			/* remove the entry from the cache */
			pool->cache[index].entry = entry->next;
			pool->cache[index].ncached--;

			/* return the entry to the freelist */
			entry->next = pool->freelist;
			pool->freelist = entry;

			MEMPOOL_RANDOMIZE(ptr, size);
			MEMPOOL_DEBUG("%d MemoryPoolAlloc %lu => %d %p HIT\n", getpid(), size, index, ptr);

			/* update memory accounting */
			Assert(pool->mem_cached >= size);

			pool->mem_cached -= size;
			pool->mem_allocated += size;

			pool->cache[index].nhits++;

			AssertCheckMemPool(pool);

			return ptr;
		}

		pool->cache[index].nmisses++;
	}

	/*
	 * Either too small/large for the cache, or there's no available block of
	 * the right size.
	 */
	ptr = malloc(size);

	MEMPOOL_RANDOMIZE(ptr, size);
	MEMPOOL_DEBUG("%d MemoryPoolAlloc %lu => %d %p MISS\n", getpid(), size, index, ptr);

	/* update memory accounting */
	pool->mem_allocated += size;

	/* maybe we should track the number of over-sized allocations too? */
	// pool->cache_misses++;

	return ptr;
}

/*
 * MemoryPoolShouldCache
 *		Should we put the entry into cache at the given index?
 */
static bool
MemoryPoolShouldCache(Size size, int index)
{
	MemPoolBucket  *entry = &pool->cache[index];

	/* not in any pool bucket */
	if (index == -1)
		return false;

	/*
	 * Bail out if no freelist entries.
	 *
	 * XXX This shouldn't be possible, as we size the freeslist as if all classes
	 * could have the maximum number of entries (but the actual number grops to
	 * 1/2 with each size class).
	 */
	if (!pool->freelist)
		return false;

	/* Memory limit is set, and we'd exceed it? Don't cache. */
	if ((pool->mem_allowed > 0) &&
		(pool->mem_allocated + pool->mem_cached + size > pool->mem_allowed))
		return false;

	/* Did we already reach the maximum size of the size class? */
	return (entry->ncached < entry->maxcached);
}

void
MemoryPoolFree(void *pointer, Size size)
{
	int	index = 0;

	MemoryPoolInit();

	/*
	 * Override the requested size (provided by the memory context), calculate
	 * the appropriate size class index.
	 */
	size = MemoryPoolEntrySize(size);
	index = MemoryPoolEntryIndex(size);

	AssertCheckEntrySize(size, index);

	/* check that we've correctly accounted for this block during allocation */
	Assert(pool->mem_allocated >= size);

	/* update the number of allocated blocks (if eligible for cache) */
	if (index != -1)
		pool->cache[index].nallocated--;

	/*
	 * Should we cache this entry? Do we have entries for the freelist, and
	 * do we have free space in the size class / memory pool as a whole?
	 */
	if (MemoryPoolShouldCache(size, index))
	{
		MemPoolEntry *entry;

		entry = pool->freelist;
		pool->freelist = entry->next;

		/* add the entry to the cache, update number of entries in this bucket */
		entry->next = pool->cache[index].entry;
		pool->cache[index].entry = entry;
		pool->cache[index].ncached++;

		entry->ptr = pointer;

		MEMPOOL_RANDOMIZE(pointer, size);
		MEMPOOL_DEBUG("%d MemoryPoolFree %lu => %d %p ADD\n", getpid(), size, index, pointer);

		/* update accounting */
		pool->mem_cached += size;
		pool->mem_allocated -= size;

		AssertCheckMemPool(pool);

		return;
	}

	MEMPOOL_RANDOMIZE(pointer, size);
	MEMPOOL_DEBUG("%d MemoryPoolFree %lu => %d FULL\n", getpid(), size, index);

	/* update accounting */
	pool->mem_allocated -= size;

	free(pointer);
}

/*
 * MemoryPoolRealloc
 *		reallocate a previously allocated block
 *
 * XXX Maybe this should use the cache too. Right now we just call realloc()
 * after updating the cache counters. And maybe it should enforce the memory
 * limit, just like we do in MemoryPoolAlloc().
 */
void *
MemoryPoolRealloc(void *pointer, Size oldsize, Size newsize)
{
	void *ptr;

	int		oldindex,
			newindex;

	MemoryPoolInit();

	oldsize = MemoryPoolEntrySize(oldsize);
	newsize = MemoryPoolEntrySize(newsize);

	/* XXX Maybe if (oldsize >= newsize) we don't need to do anything? */

	oldindex = MemoryPoolEntryIndex(oldsize);
	newindex = MemoryPoolEntryIndex(newsize);

	if (oldindex != -1)
		pool->cache[oldindex].nallocated--;

	if (newindex != -1)
	{
		pool->cache[newindex].nallocated++;
		pool->cache[newindex].maxallocated = Max(pool->cache[newindex].nallocated,
												 pool->cache[newindex].maxallocated);
	}

	MEMPOOL_DEBUG("%d MemoryPoolRealloc old %lu => %p\n", getpid(), oldsize, pointer);

	ptr = realloc(pointer, newsize);

	MEMPOOL_DEBUG("%d MemoryPoolRealloc new %lu => %p\n", getpid(), newsize, ptr);

	/* update accounting */
	Assert(pool->mem_allocated >= oldsize);

	pool->mem_allocated -= oldsize;
	pool->mem_allocated += newsize;

	AssertCheckMemPool(pool);

	return ptr;
}

static void
MemoryPoolRebalanceBuckets(void)
{
	Size	block_size;
	int64	redistribute_bytes;
	int64	assigned_bytes = 0;
	int64	num_total_misses = 0;

	/* only do this once every MEMPOOL_ADJUST_DISTANCE allocations */
	if (pool->num_requests < MEMPOOL_REBALANCE_DISTANCE)
		return;

#ifdef MEMPOOL_DEBUG
	MEMPOOL_DEBUG("%d mempool rebalance requests %ld allowed %ld allocated %ld cached %ld\n",
				  getpid(), pool->num_requests,
				  pool->mem_allowed, pool->mem_allocated, pool->mem_cached);

	/* print some info about cache hit ratio, but only once in a while */
	for (int i = 0; i < MEMPOOL_SIZES; i++)
	{
		MEMPOOL_DEBUG("%d mempool rebalance bucket %d hit %d miss %d (%.1f%%) maxcached %d cached %d maxallocated %d allocated %d\n",
					  getpid(), i, pool->cache[i].nhits, pool->cache[i].nmisses,
					  pool->cache[i].nhits * 100.0 / Max(1, pool->cache[i].nhits + pool->cache[i].nmisses),
					  pool->cache[i].maxcached, pool->cache[i].ncached,
					  pool->cache[i].maxallocated, pool->cache[i].nallocated);
	}
#endif

	/*
	 * Are there buckets with cache that is unnecessarily large? That is, with
	 * (ncached + nallocated > maxallocated). If yes, we release half of that
	 * and put that into a budget that we can redistribute.
	 *
	 * XXX We release half to somewhat dampen the changes over time.
	 */
	block_size = MEMPOOL_MIN_BLOCK;
	for (int i = 0; i < MEMPOOL_SIZES; i++)
	{
		/*
		 * If the cache is large enough to serve all allocations, try making it
		 * a bit smaller and cut half the extra space (and maybe also free the
		 * unnecessary blocks).
		 */
		if (pool->cache[i].maxcached > pool->cache[i].maxallocated)
		{
			int	nentries;

			pool->cache[i].maxcached
				= (pool->cache[i].maxcached + pool->cache[i].maxallocated) / 2;

			nentries = (pool->cache[i].ncached + pool->cache[i].nallocated);
			nentries -= pool->cache[i].maxcached;

			/* release enough entries from the cache */
			while (nentries > 0)
			{
				MemPoolEntry *entry = pool->cache[i].entry;

				pool->cache[i].entry = entry->next;
				pool->cache[i].ncached--;

				free(entry->ptr);
				entry->ptr = NULL;

				/* add the entry to the freelist */
				entry->next = pool->freelist;
				pool->freelist = entry;

				Assert(pool->mem_cached >= block_size);

				/* update accounting */
				pool->mem_cached -= block_size;

				nentries--;
			}
		}

		/* remember how many misses we saw in the undersized buckets */
		num_total_misses += pool->cache[i].nmisses;

		/* remember how much space we already allocated to this bucket */
		assigned_bytes += (pool->cache[i].maxcached * block_size);

		/* double the block size */
		block_size = (block_size << 1);
	}

	/*
	 * How much memory we can redistribute? Start with the memory limit,
	 * and subtract the space currently allocated and assigned to cache.
	 */
	redistribute_bytes = Max(pool->mem_allowed, MEMPOOL_SIZE_MAX);
	redistribute_bytes -= (pool->mem_allocated);
	redistribute_bytes -= assigned_bytes;

	/*
	 * Make sure it's not negative (might happen if there's a lot of
	 * allocated memory).
	 */
	redistribute_bytes = Max(0, redistribute_bytes);

	MEMPOOL_DEBUG("%d mempool rebalance can redistribute %ld bytes, allocated %ld bytes, assigned %ld bytes, total misses %ld\n",
				  getpid(), redistribute_bytes, pool->mem_allocated, assigned_bytes, num_total_misses);

	/*
	 * Redistribute the memory based on the number of misses, and reset the
	 * various counters, so that the next round begins afresh.
	 */
	if (redistribute_bytes > 0)
	{
		block_size = MEMPOOL_MIN_BLOCK;
		for (int i = 0; i < MEMPOOL_SIZES; i++)
		{
			int64	nbytes;
			int		nentries;

			/* Are we missing entries in cache for this slot? */
			if (pool->cache[i].maxcached < pool->cache[i].maxallocated)
			{
				int nmissing = (pool->cache[i].maxallocated - pool->cache[i].maxcached);

				/*
				 * How many entries we can add to this size bucket, based on the number
				 * of cache misses?
				 */
				nbytes = redistribute_bytes * pool->cache[i].nmisses / Max(1, num_total_misses);
				nentries = (nbytes / block_size);

				/* But don't add more than we need. */
				nentries = Min(nentries, nmissing);

				pool->cache[i].maxcached += nentries;
				assigned_bytes += nentries * block_size;
			}

			/* double the block size */
			block_size = (block_size << 1);
		}
	}

	MEMPOOL_DEBUG("%d mempool rebalance done allocated %ld bytes, assigned %ld bytes\n",
				  getpid(), pool->mem_allocated, assigned_bytes);

	/*
	 * If we still have some memory, redistribute it uniformly.
	 */
	redistribute_bytes = Max(pool->mem_allowed, MEMPOOL_SIZE_MAX);
	redistribute_bytes -= (pool->mem_allocated);
	redistribute_bytes -= assigned_bytes;

	/*
	 * Make sure it's not negative (might happen if there's a lot of
	 * allocated memory).
	 */
	redistribute_bytes = Max(0, redistribute_bytes);

	MEMPOOL_DEBUG("%d mempool rebalance remaining bytes %ld, allocated %ld bytes, assigned %ld bytes\n",
				  getpid(), redistribute_bytes, pool->mem_allocated, assigned_bytes);

	block_size = MEMPOOL_MIN_BLOCK;
	for (int i = 0; i < MEMPOOL_SIZES; i++)
	{
		int	nentries = (redistribute_bytes / MEMPOOL_SIZES / block_size);

		pool->cache[i].maxcached += nentries;

		/* also reset the various counters */
		pool->cache[i].maxallocated = pool->cache[i].nallocated;
		pool->cache[i].nhits = 0;
		pool->cache[i].nmisses = 0;

		/* double the block size */
		block_size = (block_size << 1);
	}

	MEMPOOL_DEBUG("%d mempool rebalance done\n", getpid());

#ifdef MEMPOOL_DEBUG
	/* print some info about cache hit ratio, but only once in a while */
	block_size = MEMPOOL_MIN_BLOCK;
	assigned_bytes = 0;
	for (int i = 0; i < MEMPOOL_SIZES; i++)
	{
		MEMPOOL_DEBUG("%d mempool rebalance bucket %d maxcached %d cached %d maxallocated %d allocated %d\n",
					  getpid(), i,
					  pool->cache[i].maxcached, pool->cache[i].ncached,
					  pool->cache[i].maxallocated, pool->cache[i].nallocated);

		assigned_bytes += (pool->cache[i].maxcached * block_size);

		/* double the block size */
		block_size = (block_size << 1);
	}
	MEMPOOL_DEBUG("%d mempool rebalance allocated %ld assigned %ld (total %ld kB)\n",
				  getpid(), pool->mem_allocated, assigned_bytes,
				  (pool->mem_allocated + assigned_bytes) / 1024L);
#endif

	/* start new rebalance period */
	pool->num_requests = 0;
}

/*
 * MemoryPoolEnforceMaxCounts
 *		
 *
 */
static void
MemoryPoolEnforceMaxCounts(void)
{
	Size	block_size = MEMPOOL_MAX_BLOCK;

	/* nothing cached, so can't release anything */
	if (pool->mem_cached == 0)
		return;

	/* Is it even eligible to be in the cache? */
	for (int i = MEMPOOL_SIZES - 1; i >= 0; i--)
	{
		while (pool->cache[i].entry)
		{
			MemPoolEntry *entry = pool->cache[i].entry;

			/* we're within the limit, bail out */
			if (pool->cache[i].ncached <= pool->cache[i].maxcached)
				break;

			pool->cache[i].entry = entry->next;
			pool->cache[i].ncached--;

			free(entry->ptr);
			entry->ptr = NULL;

			/* add the entry to the freelist */
			entry->next = pool->freelist;
			pool->freelist = entry;

			Assert(pool->mem_cached >= block_size);

			/* update accounting */
			pool->mem_cached -= block_size;
		}

		/* double the block size */
		block_size = (block_size << 1);
	}

	MEMPOOL_DEBUG("%d MemoryPoolEnforceMaxCounts allocated %ld cached %ld\n",
				  getpid(), pool->mem_allocated, pool->mem_cached);

	AssertCheckMemPool(pool);
}

/*
 * MemoryPoolEnforceSizeLimit
 *		Release cached blocks to allow allocating a block of a given size.
 *
 * If actually freeing blocks is needed, we free more of them, so that we don't
 * need to do that too often. We free at least 2x the amount of space we need,
 * or 25% of the limit, whichever is larger.
 *
 * We free memory from the largest blocks, because that's likely to free memory
 * the fastest. And we don't alocate those very often.
 *
 * XXX Maybe we should free memory in the smaller classes too, so that we don't
 * end up keeping many unnecessary old blocks, while trashing the large class.
 */
static void
MemoryPoolEnforceSizeLimit(Size request_size, int index)
{
	int64	threshold,
			needtofree;

	Size	block_size = MEMPOOL_MAX_BLOCK;

	/* no memory limit set */
	if (pool->mem_allowed == 0)
		return;

	/* nothing cached, so can't release anything */
	if (pool->mem_cached == 0)
		return;

	/*
	 * With the new request, would we exceed the memory limit? we need
	 * to count both the allocated and cached memory.
	 *
	 * XXX In principle the block may be already available in cache, in which
	 * case we don't need to add it to the allocated + cached figure.
	 */
	if (pool->mem_allocated + pool->mem_cached + request_size <= pool->mem_allowed)
		return;

	/*
	 * How much we need to release? we don't want to allocate just enough
	 * for the one request, but a bit more, to prevent trashing.
	 */
	threshold = Min(Max(0, pool->mem_allowed - 2 * request_size),
					pool->mem_allowed * 0.75);

	Assert((threshold >= 0) && (threshold < pool->mem_allowed));

	/*
	 * How much we need to free, to get under the theshold? Can't free more
	 * than we have in the cache, though.
	 *
	 * XXX One we free at least this amount of memory, we're done.
	 */
	needtofree = (pool->mem_allocated + pool->mem_cached + request_size) - threshold;
	needtofree = Min(needtofree, pool->mem_cached);

	MEMPOOL_DEBUG("%d MemoryPoolMaybeShrink total %ld cached %ld threshold %ld needtofree %ld\n",
				  getpid(), pool->mem_allocated + pool->mem_cached, pool->mem_cached, threshold, needtofree);

	/* Is it even eligible to be in the cache? */
	for (int i = MEMPOOL_SIZES - 1; i >= 0; i--)
	{
		/* did we free enough memory? */
		if (needtofree <= 0)
			break;

		while (pool->cache[i].entry)
		{
			MemPoolEntry *entry = pool->cache[i].entry;

			pool->cache[i].entry = entry->next;
			pool->cache[i].ncached--;

			free(entry->ptr);
			entry->ptr = NULL;

			/* add the entry to the freelist */
			entry->next = pool->freelist;
			pool->freelist = entry;

			needtofree -= block_size;

			/* did we free enough memory? */
			if (needtofree <= 0)
				break;
		}

		block_size = (block_size >> 1);
	}

	MEMPOOL_DEBUG("%d MemoryPoolEnforceMemoryLimit allocated %ld cached %ld needtofree %ld\n",
				  getpid(), pool->mem_allocated, pool->mem_cached, needtofree);

	AssertCheckMemPool(pool);
}

/*
 * MemoryPoolSetSizeLimit
 *		Set size limit for the memory pool.
 */
void
MemoryPoolSetSizeLimit(int64 size)
{
	Size	blksize = MEMPOOL_MIN_BLOCK;
	Size	maxsize;

	Assert(pool);
	Assert(size >= 0);

	pool->mem_allowed = size;

	/* also update the max number of entries for each class size */

	if (size > 0)
		maxsize = size / MEMPOOL_SIZES;
	else
		maxsize = MEMPOOL_SIZE_MAX;

	for (int i = 0; i < MEMPOOL_SIZES; i++)
	{
		pool->cache[i].maxcached = (maxsize / blksize);
		blksize *= 2;
	}

	/* enforce the updated maxcached limit */
	MemoryPoolEnforceMaxCounts();

	/* also enforce the general memory limit  */
	MemoryPoolEnforceSizeLimit(0, -1);
}

/*
 * MemoryPoolGetSizeAndCounts
 *		
 */
void
MemoryPoolGetSizeAndCounts(int64 *mem_allowed, int64 *mem_allocated, int64 *mem_cached,
						   int64 *cache_hits, int64 *cache_misses)
{
	Assert(pool);

	*mem_allowed = pool->mem_allowed;
	*mem_allocated = pool->mem_allocated;
	*mem_cached = pool->mem_cached;

	*cache_hits = 0;
	*cache_misses = 0;

	for (int i = 0; i < MEMPOOL_SIZES; i++)
	{
		*cache_hits += pool->cache[i].nhits;
		*cache_misses += pool->cache[i].nmisses;
	}
}
