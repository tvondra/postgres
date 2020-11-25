/*
 * fdwxact.h
 *
 * PostgreSQL foreign transaction manager definitions
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/access/fdwxact.h
 */
#ifndef FDWXACT_H
#define FDWXACT_H

#include "access/xact.h"
#include "access/fdwxact_xlog.h"
#include "foreign/foreign.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/s_lock.h"

/* Flag passed to FDW transaction management APIs */
#define FDWXACT_FLAG_ONEPHASE		0x01	/* transaction can commit/rollback
											 * without preparation */
#define FDWXACT_FLAG_PARALLEL_WORKER	0x02	/* is parallel worker? */

/* Enum for foreign_twophase_commit parameter */
typedef enum
{
	FOREIGN_TWOPHASE_COMMIT_DISABLED,	/* disable foreign twophase commit */
	FOREIGN_TWOPHASE_COMMIT_REQUIRED	/* all foreign servers have to support
										 * twophase commit */
}			ForeignTwophaseCommitLevel;

/* Enum to track the status of foreign transaction */
typedef enum
{
	FDWXACT_STATUS_INVALID = 0,
	FDWXACT_STATUS_PREPARING,	/* foreign transaction is being prepared */
	FDWXACT_STATUS_PREPARED,	/* foreign transaction is prepared */
	FDWXACT_STATUS_COMMITTING,	/* foreign prepared transaction is committed */
	FDWXACT_STATUS_ABORTING		/* foreign prepared transaction is aborted */
} FdwXactStatus;

/*
 * Shared memory state of a single foreign transaction.
 */
typedef struct FdwXactStateData *FdwXactState;
typedef struct FdwXactStateData
{
	FdwXactState		fdwxact_free_next;	/* Next free FdwXactState entry */

	/* Information relevant with foreign transaction */
	FdwXactStateOnDiskData data;

	/* Foreign transaction status */
	FdwXactStatus status;
	slock_t		mutex;			/* protect the above field */

	/*
	 * Note that we need to keep track of two LSNs for each FdwXactState. We keep
	 * track of the start LSN because this is the address we must use to read
	 * state data back from WAL when committing a FdwXactState. We keep track of
	 * the end LSN because that is the LSN we need to wait for prior to
	 * commit.
	 *
	 * XXX Should be "checkpointing" instead of "committing", no?
	 */
	XLogRecPtr	insert_start_lsn;	/* XLOG offset of inserting this entry
									 * start */
	XLogRecPtr	insert_end_lsn; /* XLOG offset of inserting this entry end */

	bool		valid;			/* has the entry been complete and written to
								 * file? */
	BackendId	locking_backend;	/* backend currently working on the fdw xact */
	bool		ondisk;			/* true if prepare state file is on disk */
	bool		inredo;			/* true if entry was added via xlog_redo */
} FdwXactStateData;

/* State data for foreign transaction resolution, passed to FDW callbacks */
typedef struct FdwXactInfo
{
	ForeignServer	*server;
	UserMapping		*usermapping;

	int	flags;			/* OR of FDWXACT_FLAG_xx flags */
	char   *identifier;
} FdwXactInfo;

/* GUC parameters */
extern int	max_prepared_foreign_xacts;
extern int	max_foreign_xact_resolvers;
extern int	foreign_xact_resolution_retry_interval;
extern int	foreign_xact_resolver_timeout;
extern int	foreign_twophase_commit;

/* Function declarations */
extern void PreCommit_FdwXact(bool is_parallel_worker);
extern void AtEOXact_FdwXact(bool isCommit, bool is_parallel_worker);
extern Size FdwXactShmemSize(void);
extern void FdwXactShmemInit(void);
extern void AtPrepare_FdwXact(void);
extern void FdwXactLaunchResolvers(void);
extern int CountFdwXactsForUserMapping(Oid umid);
extern int CountFdwXactsForDB(Oid dbid);
extern void FdwXactLaunchResolversForXid(TransactionId xid);
extern void CheckPointFdwXacts(XLogRecPtr redo_horizon);
extern void ResolveOneFdwXact(FdwXactState fdwxact);
extern void RestoreFdwXactData(void);
extern void RecoverFdwXacts(void);
extern TransactionId PrescanFdwXacts(TransactionId oldestActiveXid);

extern int	FdwXactCount(void);
extern FdwXactState FdwXactGetState(int i);

#endif /* FDWXACT_H */
