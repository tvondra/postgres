/*-------------------------------------------------------------------------
 *
 * cubes.h
 *		Changeset and cube management commands (create/drop cube).
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/cubes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CUBE_H
#define CUBE_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern ObjectAddress CreateChangeSet(ChangeSetStmt *stmt);
extern ObjectAddress CreateCube(CubeStmt *stmt);

#endif   /* CUBE_H */
