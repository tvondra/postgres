/*-------------------------------------------------------------------------
 *
 * nodeFullMaterial.h
 *
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeFullMaterial.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEFULLMATERIAL_H
#define NODEFULLMATERIAL_H

#include "nodes/execnodes.h"

extern FullMaterialState *ExecInitFullMaterial(FullMaterial *node, EState *estate, int eflags);
extern void ExecEndFullMaterial(FullMaterialState *node);
extern void ExecFullMaterialMarkPos(FullMaterialState *node);
extern void ExecFullMaterialRestrPos(FullMaterialState *node);
extern void ExecReScanFullMaterial(FullMaterialState *node);

#endif							/* NODEFULLMATERIAL_H */
