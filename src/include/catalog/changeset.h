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

/* changeset operation type */
#define CHANGESET_INSERT		'I'
#define CHANGESET_DELETE		'D'

extern Oid changeset_create(Relation heapRelation,
			 const char *chsetRelationName,
			 ChangeSetInfo *chsetInfo,
			 Oid tableSpaceId,
			 Datum reloptions,
			 bool if_not_exists);

extern void changeset_drop(Oid chsetId);

ChangeSetInfo * BuildChangeSetInfo(Relation changeset);

void FormChangeSetDatum(ChangeSetInfo *chsetInfo,
				   char changeType,
				   TupleTableSlot *slot,
				   Datum *values,
				   bool *isnull);

void FormChangeSetDatum2(ChangeSetInfo *chsetInfo,
				   char changeType,
				   TupleDesc desc,
				   HeapTuple tup,
				   Datum *values,
				   bool *isnull);				   

extern Oid cube_create(Relation heapRelation,
			Relation changesetRelation,
			const char *cubeRelationName,
			CubeInfo *cubeInfo,
			Oid *typeObjectId,
			Oid *collationObjectId,
			Oid *classObjectId,
			List *cubeColNames,
			Oid tableSpaceId,
			Datum reloptions,
			bool if_not_exists);

extern void cube_drop(Oid cubeId);

CubeInfo * BuildCubeInfo(Relation cube);

#endif   /* CHANGESET_H */
