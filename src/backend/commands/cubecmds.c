/*-------------------------------------------------------------------------
 *
 * cubes.c
 *	  Commands to manipulate changesets and cubes
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/cubes.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_cube.h"
#include "catalog/pg_changeset.h"
#include "commands/cubes.h"

/*
 * CREATE CHANGESET
 */
ObjectAddress
CreateChangeSet(ChangeSetStmt *stmt)
{
	return InvalidObjectAddress;
}

/*
 * CREATE CUBE
 */
ObjectAddress
CreateCube(CubeStmt *stmt)
{
	return InvalidObjectAddress;
}
