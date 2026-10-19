/*-------------------------------------------------------------------------
 *
 * nodeHash.h
 *	  prototypes for nodeHash.c
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeHash.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEHASH_H
#define NODEHASH_H

#include "access/parallel.h"
#include "executor/hashjoin.h"
#include "lib/bloomfilter.h"
#include "nodes/execnodes.h"

struct SharedHashJoinBatch;

extern HashState *ExecInitHash(Hash *node, EState *estate, int eflags);
extern Node *MultiExecHash(HashState *node);
extern void ExecEndHash(HashState *node);
extern void ExecReScanHash(HashState *node);

extern HashJoinTable ExecHashTableCreate(HashState *state);
extern void ExecParallelHashTableAlloc(HashJoinTable hashtable,
									   int batchno);
extern void ExecHashTableDestroy(HashJoinTable hashtable);
extern void ExecHashTableDetach(HashJoinTable hashtable);
extern void ExecHashTableDetachBatch(HashJoinTable hashtable);
extern void ExecParallelHashTableSetCurrentBatch(HashJoinTable hashtable,
												 int batchno);

extern void ExecHashTableInsert(HashJoinTable hashtable,
								TupleTableSlot *slot,
								uint32 hashvalue);
extern void ExecParallelHashTableInsert(HashJoinTable hashtable,
										TupleTableSlot *slot,
										uint32 hashvalue);
extern void ExecParallelHashTableInsertCurrentBatch(HashJoinTable hashtable,
													TupleTableSlot *slot,
													uint32 hashvalue);
extern void ExecHashGetBucketAndBatch(HashJoinTable hashtable,
									  uint32 hashvalue,
									  int *bucketno,
									  int *batchno);
extern bool ExecScanHashBucket(HashJoinState *hjstate, ExprContext *econtext);
extern bool ExecParallelScanHashBucket(HashJoinState *hjstate, ExprContext *econtext);
extern void ExecPrepHashTableForUnmatched(HashJoinState *hjstate);
extern bool ExecParallelPrepHashTableForUnmatched(HashJoinState *hjstate);
extern bool ExecScanHashTableForUnmatched(HashJoinState *hjstate,
										  ExprContext *econtext);
extern bool ExecParallelScanHashTableForUnmatched(HashJoinState *hjstate,
												  ExprContext *econtext);
extern void ExecHashTableReset(HashJoinTable hashtable);
extern void ExecHashTableResetMatchFlags(HashJoinTable hashtable);
extern void ExecChooseHashTableSize(double ntuples, int tupwidth, bool useskew,
									bool try_combined_hash_mem,
									int parallel_workers,
									size_t *space_allowed,
									int *numbuckets,
									int *numbatches,
									int *num_skew_mcvs);
extern int	ExecHashGetSkewBucket(HashJoinTable hashtable, uint32 hashvalue);
extern Tuplestorestate *ExecHashBuildNullTupleStore(HashJoinTable hashtable);
extern void ExecHashEstimate(HashState *node, ParallelContext *pcxt);
extern void ExecHashInitializeDSM(HashState *node, ParallelContext *pcxt);
extern void ExecHashInitializeWorker(HashState *node, ParallelWorkerContext *pwcxt);
extern void ExecHashRetrieveInstrumentation(HashState *node);
extern void ExecShutdownHash(HashState *node);
extern void ExecHashBloomRememberFilter(HashJoinTable hashtable,
										bloom_filter *filter);
extern void ExecHashAccumInstrumentation(HashInstrumentation *instrument,
										 HashJoinTable hashtable);

/* out-of-line part of ExecHashBloomReject() below, not for direct use */
extern bool ExecHashBloomProbeFilter(HashJoinTable hashtable, uint32 hashvalue);

/*
 * ExecHashBloomReject
 *		Should this hash value (for an outer tuple) be rejected?
 *
 * Returns true if a Bloom filter is in use and it proves that the given hash
 * value (and therefore the outer tuple) cannot match any inner tuple.
 *
 * This is called for every outer tuple, so the common case of a hash table
 * without a filter is handled here, inline, and only hash tables that do
 * have a filter pay for the out-of-line call that probes it.
 *
 * The caller is responsible for probing each outer tuple only once.  Tuples
 * written to a batch file have already been checked (either while processing
 * batch 0 of a serial join, or while partitioning the outer relation of a
 * Parallel Hash join), so there's no point in checking them again when they
 * are read back.  Doing so would waste cycles and distort the probe counters
 * used to drive the adaptive sampling.
 */
static inline bool
ExecHashBloomReject(HashJoinTable hashtable, uint32 hashvalue)
{
	/* If there's no filter, all tuples should pass. */
	if (hashtable->bloomFilter == NULL)
		return false;

	return ExecHashBloomProbeFilter(hashtable, hashvalue);
}

#endif							/* NODEHASH_H */
