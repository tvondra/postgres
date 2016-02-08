/*-------------------------------------------------------------------------
 *
 * changeset.h
 *	  prototypes for catalog/changeset.c.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/changeset.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHANGESET_H
#define CHANGESET_H

#include "catalog/objectaddress.h"
#include "nodes/execnodes.h"

extern Oid changeset_create(Relation heapRelation,
			 const char *chsetRelationName,
			 ChangeSetInfo *chsetInfo,
			 List *chsetColNames,
			 Oid tableSpaceId,
			 Datum reloptions,
			 bool if_not_exists);

ChangeSetInfo * BuildChangeSetInfo(Relation changeset);

#endif   /* CHANGESET_H */
