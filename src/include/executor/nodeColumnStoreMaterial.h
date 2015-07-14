/*-------------------------------------------------------------------------
 *
 * nodeColumnStoreMaterial.h
 *
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeColumnStoreMaterial.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODECOLSTOREMATERIAL_H
#define NODECOLSTOREMATERIAL_H

#include "nodes/execnodes.h"

extern ColumnStoreMaterialState *ExecInitColumnStoreMaterial(ColumnStoreMaterial *node, EState *estate, int eflags);
extern TupleTableSlot *ExecColumnStoreMaterial(ColumnStoreMaterialState *node);
extern void ExecEndColumnStoreMaterial(ColumnStoreMaterialState *node);
extern void ExecColumnStoreMaterialMarkPos(ColumnStoreMaterialState *node);
extern void ExecColumnStoreMaterialRestrPos(ColumnStoreMaterialState *node);
extern void ExecReScanColumnStoreMaterial(ColumnStoreMaterialState *node);

#endif   /* NODECOLSTOREMATERIAL_H */
