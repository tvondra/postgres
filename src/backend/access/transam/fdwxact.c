/*-------------------------------------------------------------------------
 *
 * fdwxact.c
 *		PostgreSQL global transaction manager for foreign servers.
 *
 * This module contains the code for managing transactions started on foreign
 * servers.
 *
 * An FDW that implements both commit and rollback APIs can request to register
 * the foreign transaction participant by FdwXactRegisterEntry() to participate
 * it to a group of distributed tranasction.  The registered foreign transactions
 * are identified by user mapping OID.  On commit and rollback, the global
 * transaction manager calls corresponding FDW API to end the foreign
 * tranasctions.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/fdwxact.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/fdwxact.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_user_mapping.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

/* Initial size of the hash table */
#define FDWXACT_HASH_SIZE	64

/* Check the FdwXactEntry supports commit (and rollback) callbacks */
#define ServerSupportTransactionCallback(fdwent) \
	(((FdwXactEntry *)(fdwent))->commit_foreign_xact_fn != NULL)

/*
 * Structure to bundle the foreign transaction participant.
 *
 * Participants are identified by user mapping OID, rather than pair of
 * user OID and server OID. See README.fdwxact for the discussion.
 */
typedef struct FdwXactEntry
{
	/* user mapping OID, hash key (must be first) */
	Oid			umid;

	ForeignServer *server;
	UserMapping *usermapping;

	/* Callbacks for foreign transaction */
	CommitForeignTransaction_function commit_foreign_xact_fn;
	RollbackForeignTransaction_function rollback_foreign_xact_fn;
} FdwXactEntry;

/*
 * Foreign transaction participants involved in the current transaction.
 * A member of participants must support both commit and rollback APIs
 * (i.g., ServerSupportTransactionCallback() is true).
 */
static HTAB *FdwXactParticipants = NULL;

/* Check the current transaction has at least one fdwxact participant */
#define HasFdwXactParticipant() \
	(FdwXactParticipants != NULL && \
	 hash_get_num_entries(FdwXactParticipants) > 0)

static void EndFdwXactEntry(FdwXactEntry *fdwent, bool isCommit,
							bool is_parallel_worker);
static void RemoveFdwXactEntry(Oid umid);

/*
 * Register the given foreign transaction participant identified by the
 * given user mapping OID as a participant of the transaction.
 */
void
FdwXactRegisterEntry(UserMapping *usermapping)
{
	FdwXactEntry *fdwent;
	FdwRoutine *routine;
	Oid			umid;
	MemoryContext old_ctx;
	bool		found;

	Assert(IsTransactionState());

	if (FdwXactParticipants == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(FdwXactEntry);

		FdwXactParticipants = hash_create("fdw xact participants",
										  FDWXACT_HASH_SIZE,
										  &ctl, HASH_ELEM | HASH_BLOBS);
	}

	umid = usermapping->umid;
	fdwent = hash_search(FdwXactParticipants, (void *) &umid, HASH_ENTER, &found);

	if (found)
		return;

	/*
	 * The participant information needs to live until the end of the
	 * transaction where syscache is not available, so we save them in
	 * TopTransactionContext.
	 */
	old_ctx = MemoryContextSwitchTo(TopTransactionContext);

	fdwent->usermapping = GetUserMapping(usermapping->userid, usermapping->serverid);
	fdwent->server = GetForeignServer(usermapping->serverid);

	/*
	 * Foreign server managed by the transaction manager must implement
	 * transaction callbacks.
	 */
	routine = GetFdwRoutineByServerId(usermapping->serverid);
	if (!routine->CommitForeignTransaction)
		ereport(ERROR,
				(errmsg("cannot register foreign server not supporting transaction callback")));

	fdwent->commit_foreign_xact_fn = routine->CommitForeignTransaction;
	fdwent->rollback_foreign_xact_fn = routine->RollbackForeignTransaction;

	MemoryContextSwitchTo(old_ctx);
}

/* Remove the foreign transaction from FdwXactParticipants */
void
FdwXactUnregisterEntry(UserMapping *usermapping)
{
	Assert(IsTransactionState());
	RemoveFdwXactEntry(usermapping->umid);
}

/*
 * Remove an FdwXactEntry identified by the given user mapping id from the
 * hash table.
 */
static void
RemoveFdwXactEntry(Oid umid)
{
	(void) hash_search(FdwXactParticipants, (void *) &umid, HASH_REMOVE, NULL);
}

/*
 * Commit or rollback all foreign transactions.
 */
void
AtEOXact_FdwXact(bool isCommit, bool is_parallel_worker)
{
	FdwXactEntry *fdwent;
	HASH_SEQ_STATUS scan;

	/* If there are no foreign servers involved, we have no business here */
	if (!HasFdwXactParticipant())
		return;

	hash_seq_init(&scan, FdwXactParticipants);
	while ((fdwent = (FdwXactEntry *) hash_seq_search(&scan)))
	{
		Assert(ServerSupportTransactionCallback(fdwent));

		/* Commit or rollback foreign transaction */
		EndFdwXactEntry(fdwent, isCommit, is_parallel_worker);

		/*
		 * Remove the entry so that we don't recursively process this foreign
		 * transaction.
		 */
		RemoveFdwXactEntry(fdwent->umid);
	}

	Assert(!HasFdwXactParticipant());
}

/*
 * The routine for committing or rolling back the given transaction participant.
 */
static void
EndFdwXactEntry(FdwXactEntry *fdwent, bool isCommit, bool is_parallel_worker)
{
	FdwXactInfo finfo;

	Assert(ServerSupportTransactionCallback(fdwent));

	finfo.server = fdwent->server;
	finfo.usermapping = fdwent->usermapping;
	finfo.flags = FDWXACT_FLAG_ONEPHASE |
		((is_parallel_worker) ? FDWXACT_FLAG_PARALLEL_WORKER : 0);

	if (isCommit)
	{
		fdwent->commit_foreign_xact_fn(&finfo);
		elog(DEBUG1, "successfully committed the foreign transaction for user mapping %u",
			 fdwent->umid);
	}
	else
	{
		fdwent->rollback_foreign_xact_fn(&finfo);
		elog(DEBUG1, "successfully rolled back the foreign transaction for user mapping %u",
			 fdwent->umid);
	}
}

/*
 * This function is called at PREPARE TRANSACTION.  Since we don't support
 * preparing foreign transactions for now, raise an error if the local transaction
 * has any foreign transaction.
 */
void
AtPrepare_FdwXact(void)
{
	if (HasFdwXactParticipant())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a transaction that has operated on foreign tables")));
}
