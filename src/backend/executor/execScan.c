/*-------------------------------------------------------------------------
 *
 * execScan.c
 *	  This code provides support for generalized relation scans. ExecScan
 *	  is passed a node and a pointer to a function to "do the right thing"
 *	  and return a tuple from the relation. ExecScan then does the tedious
 *	  stuff - checking the qualification and projecting the tuple
 *	  appropriately.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execScan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/xxhash.h"

static bool ExecScanGetFilterHashValue(HashFilterReferenceState *ref,
									   ExprContext *econtext,
									   bool keep_nulls,
									   uint64 *hashvalue);

static bool ExecHashFilterContainsValue(HashFilterReferenceState *ref,
										ExprContext *econtext);

/*
 * ExecScanFetch -- check interrupts & fetch next potential tuple
 *
 * This routine is concerned with substituting a test tuple if we are
 * inside an EvalPlanQual recheck.  If we aren't, just execute
 * the access method's next-tuple routine.
 */
static inline TupleTableSlot *
ExecScanFetch(ScanState *node,
			  ExecScanAccessMtd accessMtd,
			  ExecScanRecheckMtd recheckMtd)
{
	EState	   *estate = node->ps.state;

	CHECK_FOR_INTERRUPTS();

	if (estate->es_epq_active != NULL)
	{
		EPQState   *epqstate = estate->es_epq_active;

		/*
		 * We are inside an EvalPlanQual recheck.  Return the test tuple if
		 * one is available, after rechecking any access-method-specific
		 * conditions.
		 */
		Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;

		if (scanrelid == 0)
		{
			/*
			 * This is a ForeignScan or CustomScan which has pushed down a
			 * join to the remote side.  The recheck method is responsible not
			 * only for rechecking the scan/join quals but also for storing
			 * the correct tuple in the slot.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			if (!(*recheckMtd) (node, slot))
				ExecClearTuple(slot);	/* would not be returned by scan */
			return slot;
		}
		else if (epqstate->relsubs_done[scanrelid - 1])
		{
			/*
			 * Return empty slot, as we already performed an EPQ substitution
			 * for this relation.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			/* Return empty slot, as we already returned a tuple */
			return ExecClearTuple(slot);
		}
		else if (epqstate->relsubs_slot[scanrelid - 1] != NULL)
		{
			/*
			 * Return replacement tuple provided by the EPQ caller.
			 */

			TupleTableSlot *slot = epqstate->relsubs_slot[scanrelid - 1];

			Assert(epqstate->relsubs_rowmark[scanrelid - 1] == NULL);

			/* Mark to remember that we shouldn't return more */
			epqstate->relsubs_done[scanrelid - 1] = true;

			/* Return empty slot if we haven't got a test tuple */
			if (TupIsNull(slot))
				return NULL;

			/* Check if it meets the access-method conditions */
			if (!(*recheckMtd) (node, slot))
				return ExecClearTuple(slot);	/* would not be returned by
												 * scan */
			return slot;
		}
		else if (epqstate->relsubs_rowmark[scanrelid - 1] != NULL)
		{
			/*
			 * Fetch and return replacement tuple using a non-locking rowmark.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			/* Mark to remember that we shouldn't return more */
			epqstate->relsubs_done[scanrelid - 1] = true;

			if (!EvalPlanQualFetchRowMark(epqstate, scanrelid, slot))
				return NULL;

			/* Return empty slot if we haven't got a test tuple */
			if (TupIsNull(slot))
				return NULL;

			/* Check if it meets the access-method conditions */
			if (!(*recheckMtd) (node, slot))
				return ExecClearTuple(slot);	/* would not be returned by
												 * scan */
			return slot;
		}
	}

	/*
	 * Run the node-type-specific access method function to get the next tuple
	 */
	return (*accessMtd) (node);
}

/*
 * hash_filter_lookup
 *		Lookup filter by filterId in the plan-level executor registry.
 *
 * The filter may not exist yet, depending on whether the Hash node was already
 * initialized. If the filter does not exist, we treat that matching everything.
 */
static HashFilterState *
hash_filter_lookup(EState *estate, HashFilterReferenceState *refstate)
{
	ListCell *lc;

	if (refstate->filter)
		return refstate->filter;

	foreach (lc, estate->es_filters)
	{
		HashFilterState *filterstate = (HashFilterState *) lfirst(lc);

		if (filterstate->filterId == refstate->filterId)
		{
			refstate->filter = filterstate;
			return filterstate;
		}
	}
	return NULL;
}

/*
 * ExecFilters
 *		Chech if the tuple matches the pushed-down filters.
 */
static bool
ExecFilters(ScanState *node, ExprContext *econtext)
{
	ListCell *lc;
	List *filters;

	filters = node->ss_Filters;

	foreach (lc, filters)
	{
		HashFilterReferenceState *refstate = (HashFilterReferenceState *) lfirst(lc);
		HashFilterState *filterstate = hash_filter_lookup(node->ps.state, refstate);

		if (!filterstate)
			continue;

		if (!filterstate->built)
			continue;

		if (!ExecHashFilterContainsValue(refstate, econtext))
			return false;
	}

	return true;
}

/* ----------------------------------------------------------------
 *		ExecScan
 *
 *		Scans the relation using the 'access method' indicated and
 *		returns the next qualifying tuple.
 *		The access method returns the next tuple and ExecScan() is
 *		responsible for checking the tuple returned against the qual-clause.
 *
 *		A 'recheck method' must also be provided that can check an
 *		arbitrary tuple of the relation against any qual conditions
 *		that are implemented internal to the access method.
 *
 *		Conditions:
 *		  -- the "cursor" maintained by the AMI is positioned at the tuple
 *			 returned previously.
 *
 *		Initial States:
 *		  -- the relation indicated is opened for scanning so that the
 *			 "cursor" is positioned before the first qualifying tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecScan(ScanState *node,
		 ExecScanAccessMtd accessMtd,	/* function returning a tuple */
		 ExecScanRecheckMtd recheckMtd)
{
	ExprContext *econtext;
	ExprState  *qual;
	ProjectionInfo *projInfo;
	List *filters;

	/*
	 * Fetch data from node
	 */
	qual = node->ps.qual;
	projInfo = node->ps.ps_ProjInfo;
	econtext = node->ps.ps_ExprContext;
	filters = node->ss_Filters;

	/* interrupt checks are in ExecScanFetch */

	/*
	 * If we have neither a qual or pushed-down filter to check nor a projection
	 * to do, just skip all the overhead and return the raw scan tuple.
	 */
	if (!qual && !projInfo && !filters)
	{
		ResetExprContext(econtext);
		return ExecScanFetch(node, accessMtd, recheckMtd);
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * get a tuple from the access method.  Loop until we obtain a tuple that
	 * passes the qualification.
	 */
	for (;;)
	{
		TupleTableSlot *slot;

		slot = ExecScanFetch(node, accessMtd, recheckMtd);

		/*
		 * if the slot returned by the accessMtd contains NULL, then it means
		 * there is nothing more to scan so we just return an empty slot,
		 * being careful to use the projection result slot so it has correct
		 * tupleDesc.
		 */
		if (TupIsNull(slot))
		{
			if (projInfo)
				return ExecClearTuple(projInfo->pi_state.resultslot);
			else
				return slot;
		}

		/*
		 * place the current tuple into the expr context
		 */
		econtext->ecxt_scantuple = slot;

		/*
		 * check that the current tuple satisfies the qual-clause and filte (if
		 * any was pushed down)
		 *
		 * check for non-null qual here to avoid a function call to ExecQual()
		 * when the qual is null ... saves only a few cycles, but they add up
		 * ...
		 */
		if ((qual == NULL || ExecQual(qual, econtext)) &&
			(filters == NIL || ExecFilters(node, econtext)))
		{
			/*
			 * Found a satisfactory scan tuple.
			 */
			if (projInfo)
			{
				/*
				 * Form a projection tuple, store it in the result tuple slot
				 * and return it.
				 */
				return ExecProject(projInfo);
			}
			else
			{
				/*
				 * Here, we aren't projecting, so just return scan tuple.
				 */
				return slot;
			}
		}
		else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);
	}
}

/*
 * ExecAssignScanProjectionInfo
 *		Set up projection info for a scan node, if necessary.
 *
 * We can avoid a projection step if the requested tlist exactly matches
 * the underlying tuple type.  If so, we just set ps_ProjInfo to NULL.
 * Note that this case occurs not only for simple "SELECT * FROM ...", but
 * also in most cases where there are joins or other processing nodes above
 * the scan node, because the planner will preferentially generate a matching
 * tlist.
 *
 * The scan slot's descriptor must have been set already.
 */
void
ExecAssignScanProjectionInfo(ScanState *node)
{
	Scan	   *scan = (Scan *) node->ps.plan;
	TupleDesc	tupdesc = node->ss_ScanTupleSlot->tts_tupleDescriptor;

	ExecConditionalAssignProjectionInfo(&node->ps, tupdesc, scan->scanrelid);
}

/*
 * ExecAssignScanProjectionInfoWithVarno
 *		As above, but caller can specify varno expected in Vars in the tlist.
 */
void
ExecAssignScanProjectionInfoWithVarno(ScanState *node, int varno)
{
	TupleDesc	tupdesc = node->ss_ScanTupleSlot->tts_tupleDescriptor;

	ExecConditionalAssignProjectionInfo(&node->ps, tupdesc, varno);
}

/*
 * ExecScanReScan
 *
 * This must be called within the ReScan function of any plan node type
 * that uses ExecScan().
 */
void
ExecScanReScan(ScanState *node)
{
	EState	   *estate = node->ps.state;

	/*
	 * We must clear the scan tuple so that observers (e.g., execCurrent.c)
	 * can tell that this plan node is not positioned on a tuple.
	 */
	ExecClearTuple(node->ss_ScanTupleSlot);

	/* Rescan EvalPlanQual tuple if we're inside an EvalPlanQual recheck */
	if (estate->es_epq_active != NULL)
	{
		EPQState   *epqstate = estate->es_epq_active;
		Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;

		if (scanrelid > 0)
			epqstate->relsubs_done[scanrelid - 1] = false;
		else
		{
			Bitmapset  *relids;
			int			rtindex = -1;

			/*
			 * If an FDW or custom scan provider has replaced the join with a
			 * scan, there are multiple RTIs; reset the epqScanDone flag for
			 * all of them.
			 */
			if (IsA(node->ps.plan, ForeignScan))
				relids = ((ForeignScan *) node->ps.plan)->fs_base_relids;
			else if (IsA(node->ps.plan, CustomScan))
				relids = ((CustomScan *) node->ps.plan)->custom_relids;
			else
				elog(ERROR, "unexpected scan node: %d",
					 (int) nodeTag(node->ps.plan));

			while ((rtindex = bms_next_member(relids, rtindex)) >= 0)
			{
				Assert(rtindex > 0);
				epqstate->relsubs_done[rtindex - 1] = false;
			}
		}
	}
}

/*
 * ExecScanGetFilterHashValue
 *		Calculate a hash value for the tuple in the scan slot.
 *
 * We'll then check the presence of this hash value in the bloom filter.
 */
static bool
ExecScanGetFilterHashValue(HashFilterReferenceState *ref,
						   ExprContext *econtext,
						   bool keep_nulls,
						   uint64 *hashvalue)
{
	uint64		hashkey = 0;
	FmgrInfo   *hashfunctions;
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;
	HashFilterState *filter = ref->filter;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	hashfunctions = filter->hashfunctions;

	/* XXX use expressions from the reference, with adjusted varnos etc. */
	foreach(hk, ref->clauses)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/* combine successive hashkeys by rotating */
		hashkey = pg_rotate_left32(hashkey, 1);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull);

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (filter->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
			/* FIXME should we ignore NULL values altogether? what about the
			 * keep_nulls flag? */
		}
		else
		{
			/* Compute the hash function */
			uint64		hkey;

			if (filter->types[i] == INT4OID)
				hkey = XXH3_64bits(&keyval, sizeof(Datum));
			else
				hkey = DatumGetUInt32(FunctionCall1Coll(&hashfunctions[i], filter->collations[i], keyval));

			hashkey ^= hkey;
		}

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	*hashvalue = hashkey;
	return true;
}

/*
 * ExecScanGetFilterGetValues
 *		Extract values from the scan tuple.
 *
 * FIXME Probably does not handle NULLs correctly, needs a separate isnull
 * array, or something like that?
 */
static bool
ExecScanGetFilterGetValues(HashFilterReferenceState *ref,
						   ExprContext *econtext,
						   bool keep_nulls,
						   Datum *values)
{
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;
	HashFilterState *filter = ref->filter;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/* XXX use expressions from the reference, with adjusted varnos etc. */
	foreach(hk, ref->clauses)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull);

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (filter->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
			/* FIXME should we ignore NULL values altogether? what about the
			 * keep_nulls flag? */
		}
		else
			/* FIXME probably needs to copy the value using datumCopy? */
			values[i] = keyval;

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	return true;
}

/*
 * ExecHashFilterContainsHash
 *		Check if the tuple matches the Bloom filter.
 */
static bool
ExecHashFilterContainsHash(HashFilterReferenceState *refstate, ExprContext *econtext)
{
	int			i;
	uint64		h1,
				h2;
	uint64		hashvalue = 0;
	HashFilterState *filter = refstate->filter;

	ExecScanGetFilterHashValue(refstate, econtext, false, &hashvalue);

	Assert(filter->filter_type == HashFilterBloom);

	/* compute the hashes, used for the bloom filter */
	// xxhash = XXH3_128bits(&hashvalue, sizeof(uint64));
	// h1 = xxhash.low64 % filter->nbits;
	// h2 = xxhash.high64 % filter->nbits;

	h1 = ((uint32) hashvalue) % filter->nbits;
	h2 = (hashvalue >> 32) % filter->nbits;

	/* compute the requested number of hashes */
	for (i = 0; i < filter->nhashes; i++)
	{
		/* h1 + h2 + f(i) */
		uint32		h = (h1 + i * h2) % filter->nbits;
		uint32		byte = (h / 8);
		uint32		bit = (h % 8);

		/* if the bit is not set, the value is not there */
		if (!(filter->data[byte] & (0x01 << bit)))
			return false;
	}

	filter->nhits++;

	/* all hashes found in bloom filter */
	return true;
}

/*
 * Simple comparator of Datum arrays.
 *
 * FIXME This only works for byval types, needs to check byref types too. That
 * requires looking up comparators for types etc.
 */
static int
filter_comparator(const void *a, const void *b, void *c)
{
	Size	len = * (Size *) c;

	return memcmp(a, b, len);
}

/*
 * ExecHashFilterContainsExact
 *		Check if the filter (in 'exact' mode) contains exact value.
 *
 * FIXME This assumes all the types allow sorting, but that may not be true.
 * In that case this should just do linear search.
 */
static bool
ExecHashFilterContainsExact(HashFilterReferenceState *refstate, ExprContext *econtext)
{
	HashFilterState *filter = refstate->filter;
	Datum	   *values;
	Size		entrysize = sizeof(Datum) * list_length(refstate->clauses);
	char	   *ptr;

	Assert(filter->filter_type == HashFilterExact);

	values = palloc(entrysize);

	ExecScanGetFilterGetValues(refstate, econtext, false, values);

	/* FIXME wrong, needs to use the proper comparator, not memcmp() */
	ptr = bsearch_arg(values, filter->data, filter->nvalues, entrysize, filter_comparator, &entrysize);

	if (ptr != NULL)
		filter->nhits++;

	pfree(values);

	/* all hashes found in bloom filter */
	return (ptr != NULL);
}

/*
 * ExecHashFilterContainsExact
 *		Check the filter - either in exact or hashed mode, as needed.
 */
static bool
ExecHashFilterContainsValue(HashFilterReferenceState *refstate, ExprContext *econtext)
{
	HashFilterState *filter = refstate->filter;

	filter->nqueries++;

	if (filter->filter_type == HashFilterExact)
		return ExecHashFilterContainsExact(refstate, econtext);
	else
		return ExecHashFilterContainsHash(refstate, econtext);
}
