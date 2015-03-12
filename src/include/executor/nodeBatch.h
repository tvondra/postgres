/*-------------------------------------------------------------------------
 *
 * nodeBatch.h
 *
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeSeqscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEBUFFSCAN_H
#define NODEBUFFSCAN_H

#include "nodes/execnodes.h"

extern BatchState *ExecInitBatch(Batch *node, EState *estate, int eflags);
extern TupleTableSlot *ExecBatch(BatchState *node);
extern void ExecEndBatch(BatchState *node);
extern void ExecReScanBatch(BatchState *node);

#endif   /* NODEBUFFSCAN_H */
