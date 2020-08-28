/*
 * fdwxact.h
 *
 * PostgreSQL global transaction manager
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/access/fdwxact.h
 */
#ifndef FDWXACT_H
#define FDWXACT_H

#include "access/xact.h"
#include "foreign/foreign.h"

/* Flag passed to FDW transaction management APIs */
#define FDWXACT_FLAG_ONEPHASE		0x01	/* transaction can commit/rollback
											 * without preparation */
#define FDWXACT_FLAG_PARALLEL_WORKER	0x02	/* is parallel worker? */

/* State data for foreign transaction resolution, passed to FDW callbacks */
typedef struct FdwXactInfo
{
	ForeignServer	*server;
	UserMapping		*usermapping;

	int	flags;			/* OR of FDWXACT_FLAG_xx flags */
} FdwXactInfo;

/* Function declarations */
extern void AtEOXact_FdwXact(bool isCommit, bool is_parallel_worker);
extern void AtPrepare_FdwXact(void);

#endif /* FDWXACT_H */
