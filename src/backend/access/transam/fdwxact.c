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
 * in a distributed transaction.  Registered foreign transactions are identified
 * by user mapping OID.  On commit and rollback, the global transaction manager
 * (i.e. the backend initiating the distributed transaction) calls corresponding
 * FDW API methods to end the foreign transactions.
 *
 *
 * XXX I think this should document the overall architecture, i.e. when we talk
 * about transaction manager what does that mean? The commit message also uses
 * the term "foreign transaction manager" which seems a bit unclear to me. What
 * does a "participant" mean? Is it different from "entry"? Maybe we should call
 * the function "FdwXactRegisterParticipant" instead? And same for FdwXactEntry
 * which should be FdwXactParticipant, I guess.
 *
 * XXX Worth mentioning we can't rely on syscache, which may not be available
 * while processing commit, so FdwXactParticipants copies some of the data.
 *
 * XXX Maybe we should have a separate memory context for FdwXactParticipants,
 * not TopTransactionContext. We're not leaking anything, but it'd make it
 * clearer what consumes memory. Not sure.
 *
 * To achieve commit among all foreign servers atomically, the global transaction
 * manager supports two-phase commit protocol, which is a type of atomic commitment
 * protocol. We WAL log the foreign transaction state so foreign transaction state
 * is crash-safe.
 *
 * FOREIGN TRANSACTION RESOLUTION
 *
 * At PREPARE TRANSACTION, we prepare all transactions on foreign servers by executing
 * PrepareForeignTransaction() API for each foreign transaction regardless of data on
 * the foreign server having been modified.  At COMMIT PREPARED and ROLLBACK PREPARED,
 * we commit or rollback only the local transaction but not do anything for involved
 * foreign transactions.  The prepared foreign transactinos are resolved by a resolver
 * process asynchronously.  Also, users can use pg_resolve_foreign_xact() SQL function
 * that resolve a foreign transaction manually.
 *
 * LOCKING
 *
 * Whenever a foreign transaction is processed, the corresponding FdwXactState
 * entry is updated. To avoid holding the lock during transaction processing
 * which may take an unpredictable time the in-memory data of foreign
 * transaction follows a locking model based on the following linked concepts:
 *
 * * A process who is going to work on the foreign transaction needs to set
 *	 locking_backend of the FdwXactState entry, which prevents the entry from being
 *	 updated and removed by concurrent processes.
 * * All FdwXactState fields except for status are protected by FdwXactLock.  The
 *   status is protected by its mutex.
 *
 * RECOVERY
 *
 * During replay WAL and replication FdwXactCtl also holds information about
 * active prepared foreign transaction that haven't been moved to disk yet.
 *
 * Replay of fdwxact records happens by the following rules:
 *
 * * At the beginning of recovery, pg_fdwxacts is scanned once, filling FdwXactState
 *	 with entries marked with fdwxact->inredo and fdwxact->ondisk.	FdwXactState file
 *	 data older than the XID horizon of the redo position are discarded.
 * * On PREPARE redo, the foreign transaction is added to FdwXactCtl->xacts.
 *	 We set fdwxact->inredo to true for such entries.
 * * On Checkpoint we iterate through FdwXactCtl->xacts entries that
 *	 have fdwxact->inredo set and are behind the redo_horizon.	We save
 *	 them to disk and then set fdwxact->ondisk to true.
 * * On resolution we delete the entry from FdwXactCtl->xacts.  If
 *	 fdwxact->ondisk is true, the corresponding entry from the disk is
 *	 additionally deleted.
 * * RecoverFdwXacts() and PrescanFdwXacts() have been modified to go through
 *	 fdwxact->inredo entries that have not made it to disk.
 *
 * These replay rules are borrowed from twophase.c
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/fdwxact.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/fdwxact.h"
#include "access/fdwxact_resolver.h"
#include "access/fdwxact_launcher.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/twophase.h"
#include "access/resolver_internal.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/pg_user_mapping.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/syncrep.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/procarray.h"
#include "storage/sinvaladt.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/* Directory where the foreign prepared transaction files will reside */
#define FDWXACTS_DIR "pg_fdwxact"

/* Initial size of the hash table */
#define FDWXACT_HASH_SIZE	64

/* Check the FdwXactEntry supports commit (and rollback) callbacks */
// XXX should be ServerSupportsTransactionCallback?
#define ServerSupportTransactionCallback(fdwent) \
	(((FdwXactEntry *)(fdwent))->commit_foreign_xact_fn != NULL)

/* Check the FdwXactEntry is capable of two-phase commit  */
#define ServerSupportTwophaseCommit(fdwent) \
	(((FdwXactEntry *)(fdwent))->prepare_foreign_xact_fn != NULL)

/*
 * Name of foreign prepared transaction file is 8 bytes xid and
 * user mapping OID separated by '_'.
 *
 * Since FdwXactState is identified by user mapping OID and it's unique
 * within a distributed transaction, the name is fairly enough to
 * ensure uniqueness.
 */
#define FDWXACT_FILE_NAME_LEN (8 + 1 + 8)
#define FdwXactStateFilePath(path, xid, umid)	\
	snprintf(path, MAXPGPATH, FDWXACTS_DIR "/%08X_%08X", \
			 xid, umid)

/*
 * Structure to bundle the foreign transaction participant.
 *
 * Participants are identified by user mapping OID, rather than pair of
 * user OID and server OID. See README.fdwxact for the discussion.
 *
 * XXX Unfortunately the README.fdwxact file is only added much later,
 * not in this part of the patch series.
 */
typedef struct FdwXactEntry
{
	/* user mapping OID, hash key (must be first) */
	Oid			umid;

	ForeignServer *server;
	UserMapping *usermapping;

	/*
	 * Pointer to a FdwXactState entry in the global array. NULL if the entry
	 * is not inserted yet but this is registered as a participant.
	 */
	FdwXactState fdwxact;

	/* Callbacks for foreign transaction */
	CommitForeignTransaction_function commit_foreign_xact_fn;
	RollbackForeignTransaction_function rollback_foreign_xact_fn;
	PrepareForeignTransaction_function prepare_foreign_xact_fn;
} FdwXactEntry;

/*
 * Shared memory layout for maintaining foreign prepared transaction entries.
 * Adding or removing FdwXactState entry needs to hold FdwXactLock in exclusive mode,
 * and iterating fdwXacts needs that in shared mode.
 *
 * XXX I suggest we include the FdwXactState struct here, and only expose
 * a simple forward declaration in fdwxact.h. No need to expose the internal
 * stuff there, I think.
 *
 * XXX I think we've stopped doing the Data + pointer pairs some time ago.
 * It still exists in old code, but we don't use it for new stuff.
 *
 * XXX Explain that the empty entries are organized in a linked list, to
 * quickly pick the next free entry. Maybe there should be a function doing
 * thorough consistency with asserts (AssertCheckFdwXactCtl or something),
 * and for the local stuff too.
 *
 * XXX Apparently, this stores FdwXactState, but then also FdwXactStateData
 * right after that. At least that's what FdwXactShmemSize/FdwXactShmemInit
 * seems to do. Not sure why? Why not to store just FdwXactStateData array?
 * After all, FdwXactState is merely a pointer to FdwXactStateData, so what
 * would we get from this?
 */
typedef struct
{
	/* Head of linked list of free FdwXactStateData structs */
	FdwXactState	free_fdwxacts;

	/* Number of valid foreign transaction entries */
	int	num_xacts;

	/* Upto max_prepared_foreign_xacts entries in the array */
	FdwXactState	xacts[FLEXIBLE_ARRAY_MEMBER];	/* Variable length array */
} FdwXactCtlData;

/* Pointer to the shared memory holding the foreign transactions data */
static FdwXactCtlData *FdwXactCtl = NULL;

/*
 * The current distributed transaction state.  Members of participants
 * must support at least both commit and rollback APIs
 * (i.g., ServerSupportTransactionCallback() is true).
 *
 * XXX Should be "e.g." instead of "i.g."? Also, clearly Entry is the
 * same as Participants.
 *
 * XXX I wonder if we should have a separate memory context for this,
 * if only to make it clearer what consumes the memory.
 */
typedef struct DistributedXactStateData
{
	bool		local_prepared; /* will (did) we prepare the local transaction? */

	/* Statistics of participants */
	int			nparticipants_no_twophase;	/* how many participants doesn't
											 * support two-phase commit
											 * protocol? */

	HTAB	   *participants;	/* foreign transaction participants (FdwXactEntry) */
	List	   *serveroids_uniq;	/* list of unique server OIDs in
									 * participants */
} DistributedXactStateData;

static DistributedXactStateData DistributedXactState = {
	.local_prepared = false,
	.nparticipants_no_twophase = 0,
	.participants = NULL,
	.serveroids_uniq = NIL,
};

/* Check the current transaction has at least one fdwxact participant */
/*
 * XXX But this does not check if there's at least one participant - it
 * actually counts all the participants. But I guess it's fairly cheap,
 * as it just gets a counter from a freelist (see hash_get_num_entries).
 */
#define HasFdwXactParticipant() \
	(DistributedXactState.participants != NULL && \
	 hash_get_num_entries(DistributedXactState.participants) > 0)

/* Keep track of registering process exit call back. */
static bool fdwXactExitRegistered = false;

/* Guc parameter */
int			max_prepared_foreign_xacts = 0;
int			max_foreign_xact_resolvers = 0;

/*
 * XXX "End" seems a bit confusing, because it suggest the entry is a
 * process that was "started" in some sense. But it's not, it's just
 * a participant, and we need to deal with the remote transaction. So
 * maybe it should be ResolveFdwXactParticipant() or something. Not
 * sure.
 */
static void EndFdwXactEntry(FdwXactEntry *fdwent, bool isCommit,
							bool is_parallel_worker);

/*
 * XXX It's a bit strange that FdwXactRegisterEntry() gets UserMapping,
 * but here we only get the umid. Maybe the parameter should be the
 * same in both cases, to make the internal API more consistent.
 *
 * XXX Also, why doesn't this get FdwXactEntry pointer, just like the
 * "EndFdwXactEntry" function?
 */
static void RemoveFdwXactEntry(Oid umid);

static char *getFdwXactIdentifier(FdwXactEntry *fdwent, TransactionId xid);
static void ForgetAllParticipants(void);
static void FdwXactLaunchResolvers(void);

static void PrepareAllFdwXacts(TransactionId xid);
static XLogRecPtr FdwXactInsertEntry(TransactionId xid, FdwXactEntry *fdwent,
									 char *identifier);
static void AtProcExit_FdwXact(int code, Datum arg);
static void FdwXactComputeRequiredXmin(void);
static FdwXactStatus FdwXactGetTransactionFate(TransactionId xid);
static void FdwXactRedoAdd(char *buf, XLogRecPtr start_lsn, XLogRecPtr end_lsn);
static void FdwXactRedoRemove(TransactionId xid, Oid umid, bool givewarning);
static void XlogReadFdwXactData(XLogRecPtr lsn, char **buf, int *len);
static char *ProcessFdwXactBuffer(TransactionId xid, Oid umid,
								  XLogRecPtr insert_start_lsn, bool fromdisk);
static char *ReadFdwXactStateFile(TransactionId xid, Oid umid);
static void RemoveFdwXactStateFile(TransactionId xid, Oid umid, bool giveWarning);
static void RecreateFdwXactFile(TransactionId xid, Oid umid, void *content, int len);

static FdwXactState insert_fdwxact(Oid dbid, TransactionId xid, Oid umid, Oid serverid,
								   Oid owner, char *identifier);
static void remove_fdwxact(FdwXactState fdwxact);
static List *find_fdwxacts(TransactionId xid, Oid umid, Oid dbid);
static FdwXactState get_fdwxact_with_check(TransactionId xid, Oid umid,
										   bool check_two_phase);
static void pg_foreign_xact_callback(int code, Datum arg);

/*
 * Calculates the size of shared memory allocated for maintaining foreign
 * prepared transaction entries.
 */
Size
FdwXactShmemSize(void)
{
	Size		size;

	/* Size for foreign transaction information array */
	size = offsetof(FdwXactCtlData, xacts);
	size = add_size(size, mul_size(max_prepared_foreign_xacts,
								   sizeof(FdwXactState)));
	size = MAXALIGN(size);
	size = add_size(size, mul_size(max_prepared_foreign_xacts,
								   sizeof(FdwXactStateData)));

	return size;
}

/*
 * Initialization of shared memory for maintaining foreign prepared transaction
 * entries. The shared memory layout is defined in definition of FdwXactCtlData
 * structure.
 */
void
FdwXactShmemInit(void)
{
	bool		found;

	FdwXactCtl = ShmemInitStruct("Foreign transactions table",
								 FdwXactShmemSize(),
								 &found);
	if (!IsUnderPostmaster)
	{
		FdwXactState fdwxacts;
		int			cnt;

		Assert(!found);
		FdwXactCtl->free_fdwxacts = NULL;
		FdwXactCtl->num_xacts = 0;

		/* Initialize the linked list of free FDW transactions */
		fdwxacts = (FdwXactState)
			((char *) FdwXactCtl +
			 MAXALIGN(offsetof(FdwXactCtlData, xacts) +
					  sizeof(FdwXactState) * max_prepared_foreign_xacts));

		for (cnt = 0; cnt < max_prepared_foreign_xacts; cnt++)
		{
			fdwxacts[cnt].status = FDWXACT_STATUS_INVALID;
			fdwxacts[cnt].fdwxact_free_next = FdwXactCtl->free_fdwxacts;
			FdwXactCtl->free_fdwxacts = &fdwxacts[cnt];
			SpinLockInit(&fdwxacts[cnt].mutex);
		}
	}
	else
	{
		Assert(FdwXactCtl);
		Assert(found);
	}
}

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

	if (DistributedXactState.participants == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(FdwXactEntry);

		DistributedXactState.participants = hash_create("fdw xact participants",
														FDWXACT_HASH_SIZE,
														&ctl, HASH_ELEM | HASH_BLOBS);
	}

	umid = usermapping->umid;
	fdwent = hash_search(DistributedXactState.participants,
						 (void *) &umid, HASH_ENTER, &found);

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
	 *
	 * XXX Shouldn't this check all callbacks, including e.g. rollback?
	 * GetFdwRoutine does check both callbacks, but only as an assert,
	 * while this does elog(ERROR).
	 */
	routine = GetFdwRoutineByServerId(usermapping->serverid);
	if (!routine->CommitForeignTransaction)
		ereport(ERROR,
				(errmsg("cannot register foreign server not supporting transaction callback")));

	fdwent->fdwxact = NULL;
	fdwent->commit_foreign_xact_fn = routine->CommitForeignTransaction;
	fdwent->rollback_foreign_xact_fn = routine->RollbackForeignTransaction;
	fdwent->prepare_foreign_xact_fn = routine->PrepareForeignTransaction;

	MemoryContextSwitchTo(old_ctx);

	/* Update statistics */
	if (!ServerSupportTwophaseCommit(fdwent))
		DistributedXactState.nparticipants_no_twophase++;

	Assert(DistributedXactState.nparticipants_no_twophase <=
		   hash_get_num_entries(DistributedXactState.participants));
}

/* Remove the foreign transaction from the current participants */
// XXX Seems entirely unused. Why do we need this?
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
// XXX Maybe this should check a matching entry was found, etc?
static void
RemoveFdwXactEntry(Oid umid)
{
	FdwXactEntry *fdwent;

	Assert(DistributedXactState.participants != NULL);
	fdwent = hash_search(DistributedXactState.participants, (void *) &umid,
						 HASH_REMOVE, NULL);

	if (fdwent)
	{
		/* Update statistics */
		if (!ServerSupportTwophaseCommit(fdwent))
			DistributedXactState.nparticipants_no_twophase--;

		Assert(DistributedXactState.nparticipants_no_twophase <=
			   hash_get_num_entries(DistributedXactState.participants));
	}
}

/*
 * Commit or rollback all foreign transactions.
 *
 * XXX This probably deserves a better commit. Clearly, we don't do
 * any commits here, just aborts, so the comment is wrong.
 */
void
AtEOXact_FdwXact(bool isCommit, bool is_parallel_worker)
{
	/* If there are no foreign servers involved, we have no business here */
	if (!HasFdwXactParticipant())
		return;

	Assert(!RecoveryInProgress());

	if (!isCommit)
	{
		HASH_SEQ_STATUS scan;
		FdwXactEntry *fdwent;

		/* Rollback foreign transactions in the participant list */
		hash_seq_init(&scan, DistributedXactState.participants);
		while ((fdwent = (FdwXactEntry *) hash_seq_search(&scan)))
		{
			FdwXactState fdwxact = fdwent->fdwxact;
			int			status;

			/*
			 * If this foreign transaction is not prepared yet, end the
			 * foreign transaction in one-phase.
			 */
			if (!fdwxact)
			{
				Assert(ServerSupportTransactionCallback(fdwent));
				EndFdwXactEntry(fdwent, false, is_parallel_worker);

				/*
				 * Remove FdwXactState entry to prevent processing again in a
				 * recursive error case.
				 */
				RemoveFdwXactEntry(fdwent->umid);
				continue;
			}

			/*
			 * If the foreign transaction has FdwXactState entry, the foreign
			 * transaction might have been prepared.  We rollback the foreign
			 * transaction anyway to end the current transaction if the status
			 * is in-progress.  Since the transaction might have been already
			 * prepared on the foreign we set the status to aborting and leave
			 * it.
			 *
			 * xxx So is this a synchronous abort? Are we waiting for a response?
			 * In principle we don't need to do that (unlike for commit).
			 */
			SpinLockAcquire(&(fdwxact->mutex));
			status = fdwxact->status;
			fdwxact->status = FDWXACT_STATUS_ABORTING;
			SpinLockRelease(&(fdwxact->mutex));

			if (status == FDWXACT_STATUS_PREPARING)
				EndFdwXactEntry(fdwent, isCommit, is_parallel_worker);
		}
	}

	/* Unlock all participants */
	/* XXX Unlock? The function name says "forget". This however leaves
	 * behind the entries in shared memory, maybe the comment should
	 * mention that. */
	ForgetAllParticipants();

	/*
	 * Launch the resolver processes if we failed to prepare the local
	 * transaction after preparing the foreign transactions.  In this case, we
	 * need to rollback the prepared transaction on the foreign servers.
	 *
	 * XXX Hmmm, would it make sense to try launching the resolvers earlier?
	 * Although it we're not waiting for them, that may not be needed.
	 *
	 * XXX But we call this from PrepareTransaction - does that mean we will
	 * try resolving the transaction over and over, until it eventually gets
	 * committed/rolledback locally? Maybe we should really poke the resolvers
	 * only from FinishPreparedTransaction()? Although, we already call
	 * FdwXactLaunchResolversForXid() from there ...
	 */
	if (DistributedXactState.local_prepared && !isCommit)
		FdwXactLaunchResolvers();

	/* Reset all fields */
	DistributedXactState.local_prepared = false;
	DistributedXactState.nparticipants_no_twophase = 0;
	list_free(DistributedXactState.serveroids_uniq);
	DistributedXactState.serveroids_uniq = NIL;
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
	finfo.identifier = NULL;	/* XXX Shouldn't we pfree this? */

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
 * Prepare foreign transactions by PREPARE TRANSACTION command.
 *
 * In case where an error happens during parparing a foreign transaction we
 * change to rollback.  See AtEOXact_FdwXact() for details.
 */
void
AtPrepare_FdwXact(void)
{
	TransactionId xid;

	/* If there are no foreign servers involved, we have no business here */
	if (!HasFdwXactParticipant())
		return;

	/*
	 * Check if there is a server that doesn't support two-phase commit. All
	 * involved servers need to support two-phase commit as we're going to
	 * prepare all of them.
	 */
	if (DistributedXactState.nparticipants_no_twophase > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a distributed transaction that has operated on a foreign server not supporting two-phase commit protocol")));

	/*
	 * Make sure the local transaction has a proper transaction ID. We will
	 * use the local transaction to determine the result of the distributed
	 * one (e.g. during retry) so we need a proper commit/rollback record.
	 */
	xid = GetTopTransactionId();

	/*
	 * Mark the local transaction will be prepared before actually preparing
	 * any foreign trasactions so that in a case where an error happens during
	 * preparing a foreign transaction or preparing the local transaction, we can
	 * launch a resolver to rollback already-prepared foreign transactions.
	 *
	 * XXX I'm not sure I understand this. Why do we need this flag? Surely
	 * if any prepare fails, we know we need to rollback the transactions?
	 * In principle, we the backend could crash/exit, in which case we lose
	 * the flag anyway, right?
	 */
	DistributedXactState.local_prepared = true;

	PrepareAllFdwXacts(xid);
}

/*
 * Pre-commit processing for foreign transactions. We commit those foreign
 * transactions with one-phase.
 */
void
PreCommit_FdwXact(bool is_parallel_worker)
{
	HASH_SEQ_STATUS scan;
	FdwXactEntry *fdwent;

	/*
	 * If there is no foreign server involved or all foreign transactions are
	 * already prepared (see AtPrepare_FdwXact()), we have no business here.
	 *
	 * XXX This doesn't explain why we check the local_prepared flag too.
	 */
	if (!HasFdwXactParticipant() || DistributedXactState.local_prepared)
		return;

	/* XXX Why isn't this assert at the beginning? */
	Assert(!RecoveryInProgress());

	/* Commit all foreign transactions in the participant list */
	hash_seq_init(&scan, DistributedXactState.participants);
	while ((fdwent = (FdwXactEntry *) hash_seq_search(&scan)))
	{
		Assert(ServerSupportTransactionCallback(fdwent));

		/*
		 * Commit the foreign transaction and remove itself from the hash
		 * table so that we don't try to abort already-closed transaction.
		 */
		EndFdwXactEntry(fdwent, true, is_parallel_worker);
		RemoveFdwXactEntry(fdwent->umid);
	}
}

/*
 * Functions to count the number of FdwXactState entries associated with
 * the given argument.
 */
int
CountFdwXactsForUserMapping(Oid umid)
{
	List	   *res;

	LWLockAcquire(FdwXactLock, LW_SHARED);
	res = find_fdwxacts(InvalidTransactionId, umid, InvalidOid);
	LWLockRelease(FdwXactLock);

	return list_length(res);
}

int
CountFdwXactsForDB(Oid dbid)
{
	List	   *res;

	LWLockAcquire(FdwXactLock, LW_SHARED);
	res = find_fdwxacts(InvalidTransactionId, InvalidOid, dbid);
	LWLockRelease(FdwXactLock);

	return list_length(res);

}

/*
 * We must fsync the foreign transaction state file that is valid or generated
 * during redo and has a inserted LSN <= the checkpoint's redo horizon.
 *
 * XXX Why only inserted_LSN <= redo horizon? We don't ant extra records for
 * future transactions, right?
 *
 * Foreign transaction entries (and thus the corresponding files) are expected
 * to be very short-lived. By executing this function at the end, we might have
 * fewer files to fsync, thus reducing the amount of I/O. This is similar to
 * CheckPointTwoPhase().
 *
 * This is deliberately run as late as possible in the checkpoint sequence,
 * because FdwXacts ordinarily have short lifespans, and so it is quite
 * possible that FdwXactStates that were valid at checkpoint start will no longer
 * exist if we wait a little bit. With typical checkpoint settings this
 * will be about 3 minutes for an online checkpoint, so as a result we
 * expect that there will be no FdwXactStates that need to be copied to disk.
 *
 * XXX This just repeats (with a bit more detail) the preceding paragraph, no?
 *
 * XXX This fails to mention that the information is read from WAL, which may
 * be quite significant source of I/O (perhaps worse than the write/fsync).
 *
 * If a FdwXactState remains valid across multiple checkpoints, it will already
 * be on disk so we don't bother to repeat that write.
 */
void
CheckPointFdwXacts(XLogRecPtr redo_horizon)
{
	int			cnt;
	int			serialized_fdwxacts = 0;

	if (max_prepared_foreign_xacts == 0)
		return;					/* nothing to do */

	/*
	 * We are expecting there to be zero FdwXactState that need to be copied
	 * to disk, so we perform all I/O while holding FdwXactLock for
	 * simplicity. This presents any new foreign xacts from preparing while
	 * this occurs, which shouldn't be a problem since the presence of
	 * long-lived prepared foreign xacts indicated the transaction manager
	 * isn't active.
	 *
	 * It's also possible to move I/O out of the lock, but on every error we
	 * should check whether somebody committed our transaction in different
	 * backend. Let's leave this optimisation for future, if somebody will
	 * spot that this place cause bottleneck.
	 *
	 * Note that it isn't possible for there to be a FdwXactState with a
	 * insert_end_lsn set prior to the last checkpoint yet is marked invalid,
	 * because of the efforts with delayChkpt.
	 */
	LWLockAcquire(FdwXactLock, LW_SHARED);
	for (cnt = 0; cnt < FdwXactCtl->num_xacts; cnt++)
	{
		FdwXactState fdwxact = FdwXactCtl->xacts[cnt];

		if ((fdwxact->valid || fdwxact->inredo) &&
			!fdwxact->ondisk &&
			fdwxact->insert_end_lsn <= redo_horizon)
		{
			char	   *buf;
			int			len;

			XlogReadFdwXactData(fdwxact->insert_start_lsn, &buf, &len);

			/* XXX Why "recreate"? Does that mean we're simply discarding old
			 * file, which might be e.g. corrupted after a crash? */
			RecreateFdwXactFile(fdwxact->data.xid, fdwxact->data.umid, buf, len);
			fdwxact->ondisk = true;
			fdwxact->insert_start_lsn = InvalidXLogRecPtr;
			fdwxact->insert_end_lsn = InvalidXLogRecPtr;
			pfree(buf);
			serialized_fdwxacts++;
		}
	}

	LWLockRelease(FdwXactLock);

	/*
	 * Flush unconditionally the parent directory to make any information
	 * durable on disk.	 FdwXactState files could have been removed and those
	 * removals need to be made persistent as well as any files newly created.
	 */
	fsync_fname(FDWXACTS_DIR, true);

	if (log_checkpoints && serialized_fdwxacts > 0)
		ereport(LOG,
				(errmsg_plural("%u foreign transaction state file was written "
							   "for long-running prepared transactions",
							   "%u foreign transaction state files were written "
							   "for long-running prepared transactions",
							   serialized_fdwxacts,
							   serialized_fdwxacts)));
}

/*
 * PrepareAllFdwXacts
 *		Prepare and WAL-log foreign transactions.
 *
 * The basic strategy is to create all FdwXactState entries with WAL-logging,
 * wait for those WAL records to be replicated if synchronous replication is
 * enabled, and then prepare foreign transactions by calling
 * PrepareForeignTransaction FDW callback functions.  There are two points
 * here: (a) writing WAL records before preparing foreign transactions, and
 * (b) waiting for those records to replicated before preparing foreign
 * transactions.
 *
 * You might think that we can itereate over foreign transaction while preparing
 * the foreign transaction and writing WAL record one by one.  But this doesn't
 * work if the server crashes in the middle of those operations. We will end
 * up losing foreign prepared transaction information if the server crashes
 * and/or the failover happens.  Therefore, we need point (a) because otherwise
 * we will lost the prepared transaction on the foreign server and will not be
 * able to resolve it after the server crash.  Hence  persist first then prepare.
 * Point (b) guarantees that foreign transaction information are not lost even
 * if the failover happens.
 *
 * XXX Well, we might do the wait for flush / sync replica after each record,
 * but that seems like a pretty inefficient thing. Better to WAL-log everything
 * and then wait just once, I guess. Or do I not understand the failure modes?
 *
 * XXX What seems annoying is that we issue all the prepares synchronously, while
 * we could parallelize that quite easily by issuing the PREPARE TRANSACTION for
 * all nodes, and only then waiting for all the replies. But that does require
 * a different commit/rollback callback API - with a start/stop phase.
 */
static void
PrepareAllFdwXacts(TransactionId xid)
{
	FdwXactEntry *fdwent;
	XLogRecPtr	flush_lsn = InvalidXLogRecPtr;
	bool	canceled;
	HASH_SEQ_STATUS scan;

	Assert(TransactionIdIsValid(xid));

	/* Persist all foreign transaction entries */
	hash_seq_init(&scan, DistributedXactState.participants);
	while ((fdwent = (FdwXactEntry *) hash_seq_search(&scan)))
	{
		char	   *identifier;

		Assert(ServerSupportTwophaseCommit(fdwent));

		CHECK_FOR_INTERRUPTS();

		/* Get prepared transaction identifier */
		identifier = getFdwXactIdentifier(fdwent, xid);
		Assert(identifier);

		/*
		 * Insert the foreign transaction entry with the
		 * FDWXACT_STATUS_PREPARING status. Registration persists this
		 * information to the disk and logs (that way relaying it on standby).
		 * Thus in case we loose connectivity to the foreign server or crash
		 * ourselves, we will remember that we might have prepared transaction
		 * on the foreign server and try to resolve it when connectivity is
		 * restored or after crash recovery.
		 */
		flush_lsn = FdwXactInsertEntry(xid, fdwent, identifier);
	}

	/* We only get here when there are transactions to prepare, so the LSN
	 * should have been set. */
	Assert(flush_lsn != InvalidXLogRecPtr);

	/*
	 * XXX Pretty sure we should do a flush here, before starting the syncrep
	 * wait. FdwXactInsertEntry doesn't do the flush (and it should not, IMO)
	 * and other SyncRepWaitForLSN callers (like RecordTransactionCommit) do
	 * call XLogFlush. Otherwise we might wait for a long time, if there's not
	 * much other WAL activity.
	 */

	HOLD_INTERRUPTS();

	/* Wait for all WAL records to be replicated, if necessary */
	canceled = SyncRepWaitForLSN(flush_lsn, false);

	RESUME_INTERRUPTS();

	/*
	 * XXX: dirty hack to invoke an interruption that was absorbed.
	 * SyncRepWaitForLSN() is aimed to be used at the point where we never be
	 * able to change to rollback.  But at this state, preparing foreign
	 * transactions at pre-commit phase, we still are able to change to rollback.
	 * So if the waits canceled by an interruption, we error out. Note that
	 * SyncRepWaitForLSN() might have been set ProcDiePending too.
	 */
	if (canceled)
	{
		QueryCancelPending = true;
		ProcessInterrupts();
	}

	/* Prepare all foreign transactions */
	hash_seq_init(&scan, DistributedXactState.participants);
	while ((fdwent = (FdwXactEntry *) hash_seq_search(&scan)))
	{
		FdwXactInfo finfo;
		FdwXactState fdwxact = fdwent->fdwxact;

		/*
		 * Prepare the foreign transaction.  Between FdwXactInsertEntry call
		 * till this backend hears acknowledge from foreign server, the
		 * backend may abort the local transaction (say, because of a signal).
		 *
		 * XXX Can we just do this without holding the mutex?
		 */
		finfo.server = fdwent->server;
		finfo.usermapping = fdwent->usermapping;
		finfo.flags = 0;
		finfo.identifier = pstrdup(fdwxact->data.identifier);
		fdwent->prepare_foreign_xact_fn(&finfo);

		/* succeeded, update status */
		SpinLockAcquire(&fdwxact->mutex);
		fdwent->fdwxact->status = FDWXACT_STATUS_PREPARED;
		SpinLockRelease(&fdwxact->mutex);

		pfree(finfo.identifier);
	}
}

/*
 * Return a null-terminated foreign transaction identifier.  We generate an
 * unique identifier with the format
 * "fx_<random number>_<xid>_<umid> whose length is less than FDWXACT_ID_MAX_LEN.
 *
 * Returned string value is used to identify foreign transaction. The
 * identifier should not be same as any other concurrent prepared transaction
 * identifier.
 *
 * To make the foreign transaction ID unique, we should ideally use something
 * like UUID, which gives unique ids with high probability, but that may be
 * expensive here and UUID extension which provides the function to generate
 * UUID is not part of the core code.
 *
 * XXX I'm not even sure we can just generte arbitrary ID, to be honest, and
 * this part of the API may need rethinking. I'm no expert in XA, but from
 * cursory reading of the spec [1] (p. 19-20) it seems the "official" format
 * is not arbitrary and different from what we have. So it seems possible
 * other databases may not allow/understand "our" format.
 *
 * XXX But even worse, do all XA resources allow specifying a custom XID?
 * Maybe they allow calling PREPARE and only then you get the XID so that
 * you can commit it? I did check Oracle and it does not seem to allow
 * specifying GLOBAL_TRAN_ID, which is always in the format
 *
 *    global_db_name.db_hex_id.local_tran_id
 *
 * so entirely different from what we have. There's TRAN_COMMENT so maybe
 * we could use that?
 *
 * XXX However, it might be better to allow the "prepare" callback to allow
 * returning the XID, instead of having to generate it in advance. Of course,
 * that does not address the case where we fail right after issuing prepare,
 * but maybe we can WAL-log at least some identifying information (which would
 * help us to resolve the XACT later)?
 *
 * XXX Also, the XA spec suggests the XID can be up to 128 bytes (well, plus a
 * "long" format field), but this allows 200B. And then later GetPrepareId is
 * documented to allow only 64 bytes (NAMEDATALEN). Shouldn't this match?
 *
 * [1] https://pubs.opengroup.org/onlinepubs/009680699/toc.pdf
 * [2] https://docs.oracle.com/cd/E18283_01/server.112/e17120/ds_txnman003.htm
 */
static char *
getFdwXactIdentifier(FdwXactEntry *fdwent, TransactionId xid)
{
	char		buf[FDWXACT_ID_MAX_LEN] = {0};

	snprintf(buf, FDWXACT_ID_MAX_LEN, "fx_%ld_%u_%u", Abs(random()),
			 xid, fdwent->umid);

	return pstrdup(buf);
}

/*
 * This function inserts a new FdwXactState entry to the global array with
 * WAL-logging. The new entry is held by the backend who inserted.
 *
 * XXX I wonder if we might track / WAL-log other info too. For example, if
 * we WAL-log enough info to open a connection, we could use the same resolver
 * for all databases (because we would not require access to per-db objects
 * like foreign servers / user mapping etc.). But maybe that would not work
 * because of invalidations etc.?
 *
 * XXX It seems pretty confusing that we have a number of "insert" functions
 * like FdwXactInsertEntry, insert_fdwxact (and same fore "remove" funcs),
 * without clear indication *where* are we adding the stuff.
 */
static XLogRecPtr
FdwXactInsertEntry(TransactionId xid, FdwXactEntry *fdwent, char *identifier)
{
	FdwXactStateOnDiskData *fdwxact_file_data;
	FdwXactState fdwxact;
	Oid			owner;
	int			data_len;

	/* on first call, register the exit hook */
	if (!fdwXactExitRegistered)
	{
		before_shmem_exit(AtProcExit_FdwXact, 0);
		fdwXactExitRegistered = true;
	}

	/*
	 * Enter the foreign transaction into the shared memory structure.
	 */
	owner = GetUserId();
	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	fdwxact = insert_fdwxact(MyDatabaseId, xid, fdwent->umid,
							 fdwent->usermapping->serverid, owner, identifier);
	fdwxact->locking_backend = MyBackendId;
	LWLockRelease(FdwXactLock);

	/* Update FdwXactEntry */
	fdwent->fdwxact = fdwxact;

	/* Remember server's oid where we prepared the transaction */
	DistributedXactState.serveroids_uniq =
		list_append_unique_oid(DistributedXactState.serveroids_uniq,
							   fdwent->usermapping->serverid);

	/*
	 * Prepare to write the entry to a file. Also add xlog entry. The contents
	 * of the xlog record are same as what is written to the file.
	 *
	 * XXX This is misleading, because we're not actually writing anything
	 * to a file (particularly not to the xact state file, which is generated
	 * by a checkpoint). We're only generating the data to be written to WAL.
	 */
	data_len = offsetof(FdwXactStateOnDiskData, identifier);
	data_len = data_len + strlen(identifier) + 1;
	data_len = MAXALIGN(data_len);
	fdwxact_file_data = (FdwXactStateOnDiskData *) palloc0(data_len);
	memcpy(fdwxact_file_data, &(fdwxact->data), data_len);

	START_CRIT_SECTION();

	/* See note in RecordTransactionCommit */
	Assert((MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0);
	MyProc->delayChkptFlags |= DELAY_CHKPT_START;

	/* Add the entry in the xlog and save LSN for checkpointer */
	XLogBeginInsert();
	XLogRegisterData((char *) fdwxact_file_data, data_len);
	fdwxact->insert_end_lsn = XLogInsert(RM_FDWXACT_ID, XLOG_FDWXACT_INSERT);

	/* If we crash now, we have prepared: WAL replay will fix things */

	/* Store record's start location to read that later on CheckPoint */
	fdwxact->insert_start_lsn = ProcLastRecPtr;

	/* File is written completely, checkpoint can proceed with syncing */
	fdwxact->valid = true;

	/* Checkpoint can process now */
	MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;

	END_CRIT_SECTION();

	pfree(fdwxact_file_data);
	return fdwxact->insert_end_lsn;
}

/*
 * Insert a new entry for a given foreign transaction identified by transaction
 * id, foreign server and user mapping, into the shared memory array. Caller
 * must hold FdwXactLock in exclusive mode.
 *
 * If the entry already exists, the function raises an error.
 *
 * XXX I don't understand why we need the duplicate check. Maybe that should be
 * an assert instead?
 *
 * XXX But if we do the sequential scan to check duplicity, doesn't that mean
 * we don't need the freelist? Surely we can remember the first empty entry.
 */
static FdwXactState
insert_fdwxact(Oid dbid, TransactionId xid, Oid umid, Oid serverid, Oid owner,
			   char *identifier)
{
	FdwXactState fdwxact;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

	/* Check for duplicated foreign transaction entry */
	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		fdwxact = FdwXactCtl->xacts[i];
		if (fdwxact->valid &&
			fdwxact->data.xid == xid &&
			fdwxact->data.umid == umid)
			ereport(ERROR,
					(errmsg("could not insert a foreign transaction entry"),
					 errdetail("Duplicate entry with transaction id %u, user mapping id %u exists.",
							   xid, umid)));
	}

	/*
	 * Get a next free foreign transaction entry. Raise error if there are
	 * none left.
	 */
	if (!FdwXactCtl->free_fdwxacts)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("maximum number of foreign transactions reached"),
				 errhint("Increase max_prepared_foreign_transactions: \"%d\".",
						 max_prepared_foreign_xacts)));
	}

	fdwxact = FdwXactCtl->free_fdwxacts;
	FdwXactCtl->free_fdwxacts = fdwxact->fdwxact_free_next;

	/* Insert the entry to shared memory array */
	Assert(FdwXactCtl->num_xacts < max_prepared_foreign_xacts);
	FdwXactCtl->xacts[FdwXactCtl->num_xacts++] = fdwxact;

	fdwxact->status = FDWXACT_STATUS_PREPARING;
	fdwxact->data.xid = xid;
	fdwxact->data.dbid = dbid;
	fdwxact->data.umid = umid;
	fdwxact->data.serverid = serverid;
	fdwxact->data.owner = owner;
	strlcpy(fdwxact->data.identifier, identifier, FDWXACT_ID_MAX_LEN);
	fdwxact->data.identifier[strlen(identifier)] = '\0';

	fdwxact->insert_start_lsn = InvalidXLogRecPtr;
	fdwxact->insert_end_lsn = InvalidXLogRecPtr;
	fdwxact->locking_backend = InvalidBackendId;
	fdwxact->valid = false;
	fdwxact->ondisk = false;
	fdwxact->inredo = false;

	return fdwxact;
}

/*
 * Remove the foreign prepared transaction entry from shared memory.
 * Caller must hold FdwXactLock in exclusive mode.
 *
 * XXX This fails to mention it's also removes the xact state file, which
 * seems like a fairly important thing.
 *
 */
static void
remove_fdwxact(FdwXactState fdwxact)
{
	int			i;

	Assert(fdwxact != NULL);
	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

	elog(DEBUG2, "remove fdwxact entry id %s", fdwxact->data.identifier);

	if (!RecoveryInProgress())
	{
		xl_fdwxact_remove record;
		XLogRecPtr	recptr;

		/* Fill up the log record before releasing the entry */
		record.xid = fdwxact->data.xid;
		record.umid = fdwxact->data.umid;

		/*
		 * Now writing FdwXactState data to WAL. We have to set delayChkpt
		 * here, otherwise a checkpoint starting immediately after the WAL
		 * record is inserted could complete without fsync'ing our state file.
		 * (This is essentially the same kind of race condition as the
		 * COMMIT-to-clog-write case that RecordTransactionCommit uses
		 * delayChkpt for; see notes there.)
		 */
		START_CRIT_SECTION();

		Assert((MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0);
		MyProc->delayChkptFlags |= DELAY_CHKPT_START;

		/*
		 * Log that we are removing the foreign transaction entry and remove
		 * the file from the disk as well.
		 */
		XLogBeginInsert();
		XLogRegisterData((char *) &record, sizeof(xl_fdwxact_remove));
		recptr = XLogInsert(RM_FDWXACT_ID, XLOG_FDWXACT_REMOVE);

		/* Always flush, since we're about to remove the FdwXact state file */
		/*
		 * XXX Do we actually need/want to do this flush now? Firstly, the
		 * fdwxact may not be on disk, so maybe we can check that. Secondly,
		 * what if we're processing multiple entries - can't we do just one
		 * flush later for all of them, and then go and remove all the files?
		 */
		XLogFlush(recptr);

		/* Now we can mark ourselves as out of the commit critical section */
		MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;

		END_CRIT_SECTION();
	}

	/* Search the slot where this entry resided */
	for (i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		if (FdwXactCtl->xacts[i] == fdwxact)
			break;
	}

	Assert(i < FdwXactCtl->num_xacts);

	/* Clean up any files we may have left */
	if (fdwxact->ondisk)
		RemoveFdwXactStateFile(fdwxact->data.xid, fdwxact->data.umid, true);

	/* Remove the entry from active array */
	FdwXactCtl->num_xacts--;
	FdwXactCtl->xacts[i] = FdwXactCtl->xacts[FdwXactCtl->num_xacts];

	/* Put it back into free list */
	fdwxact->fdwxact_free_next = FdwXactCtl->free_fdwxacts;
	FdwXactCtl->free_fdwxacts = fdwxact;

	/* Reset informations */
	fdwxact->status = FDWXACT_STATUS_INVALID;
	fdwxact->locking_backend = InvalidBackendId;
	fdwxact->valid = false;
	fdwxact->ondisk = false;
	fdwxact->inredo = false;
}

/*
 * When the process exits, unlock all the entries.
 *
 * XXX This does far more than just "unlocking" entries. It also starts the
 * resolvers, and it should briefly explain what happens to remaining xacts.
 */
static void
AtProcExit_FdwXact(int code, Datum arg)
{
	ForgetAllParticipants();
	FdwXactLaunchResolvers();
}

/*
 * Unlock all foreign transaction participants.  If we left foreign transaction,
 * update the oldest xmin of unresolved transaction to prevent the local
 * transaction id of such unresolved foreign transaction from begin truncated.
 * Returns the number of remaining foreign transactions.  We return the unique
 * list of foreign servers' oids so that the caller request to launch the resolver
 * using it.
 */
static void
ForgetAllParticipants(void)
{
	FdwXactEntry *fdwent;
	HASH_SEQ_STATUS scan;
	int			nremaining = 0;

	if (!HasFdwXactParticipant())
		return;

	hash_seq_init(&scan, DistributedXactState.participants);
	while ((fdwent = (FdwXactEntry *) hash_seq_search(&scan)))
	{
		FdwXactState fdwxact = fdwent->fdwxact;

		if (fdwxact)
		{
			/*
			 * XXX Is it legal to access the locking_backend without
			 * acquiring the lock first?
			 */
			Assert(fdwxact->locking_backend == MyBackendId);

			/* Unlock the foreign transaction entry */
			LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
			fdwxact->locking_backend = InvalidBackendId;
			LWLockRelease(FdwXactLock);

			nremaining++;
		}

		/* Remove from the participants list */
		RemoveFdwXactEntry(fdwent->umid);
	}

	/*
	 * If we leave any FdwXactState entries, update the oldest local
	 * transaction of unresolved distributed transaction.
	 */
	if (nremaining > 0)
		FdwXactComputeRequiredXmin();

	Assert(!HasFdwXactParticipant());
}

static void
FdwXactLaunchResolvers(void)
{
	if (list_length(DistributedXactState.serveroids_uniq) > 0)
		LaunchOrWakeupFdwXactResolver(DistributedXactState.serveroids_uniq);
}

/*
 * Launch or wake up the resolver to resolve the given transaction.
 *
 * XXX The naming here seems a bit confused whether we're dealing with
 * a single server or multiple ones. Presumably, we may need multiple
 * resolvers - one for each remote server. So we collect the OIDs, but
 * then we call LaunchOrWakeupFdwXactResolver, which talks about one
 * server.
 *
 * XXX In fact, shouldn't this be resolver per-database? And even that
 * seems a bit strange, maybe we should allow multiple resolvers for
 * a single DB?
 */
void
FdwXactLaunchResolversForXid(TransactionId xid)
{
	List	   *serveroids = NIL;

	if (max_prepared_foreign_xacts == 0)
		return;					/* nothing to do */

	LWLockAcquire(FdwXactLock, LW_SHARED);
	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		FdwXactState fdwxact = FdwXactCtl->xacts[i];

		if (!fdwxact->valid)
			continue;

		/* Collect server oids associated with the given transaction */
		if (fdwxact->data.xid == xid)
			serveroids = list_append_unique_oid(serveroids, fdwxact->data.serverid);
	}
	LWLockRelease(FdwXactLock);

	/* Exit if there is no servers to launch */
	if (serveroids == NIL)
		return;

	LaunchOrWakeupFdwXactResolver(serveroids);
	list_free(serveroids);
}

/*
 * Commit or rollback one prepared foreign transaction, and remove FdwXactState
 * entry.
 */
void
ResolveOneFdwXact(FdwXactState fdwxact)
{
	FdwXactInfo finfo;
	FdwRoutine *routine;

	/* The FdwXactState entry must be held by me */
	Assert(fdwxact != NULL);
	Assert(fdwxact->locking_backend == MyBackendId);
	Assert(fdwxact->status == FDWXACT_STATUS_PREPARED ||
		   fdwxact->status == FDWXACT_STATUS_COMMITTING ||
		   fdwxact->status == FDWXACT_STATUS_ABORTING);

	/* Set whether we do commit or abort if not set yet */
	if (fdwxact->status == FDWXACT_STATUS_PREPARED)
	{
		FdwXactStatus new_status;

		new_status = FdwXactGetTransactionFate(fdwxact->data.xid);
		Assert(new_status == FDWXACT_STATUS_COMMITTING ||
			   new_status == FDWXACT_STATUS_ABORTING);

		/* Update the status */
		SpinLockAcquire(&fdwxact->mutex);
		fdwxact->status = new_status;
		SpinLockRelease(&fdwxact->mutex);
	}

	routine = GetFdwRoutineByServerId(fdwxact->data.serverid);

	/* Prepare the foreign transaction information to pass to API */
	finfo.server = GetForeignServer(fdwxact->data.serverid);
	finfo.usermapping = GetUserMapping(fdwxact->data.owner, fdwxact->data.serverid);
	finfo.flags = 0;
	finfo.identifier = fdwxact->data.identifier;

	if (fdwxact->status == FDWXACT_STATUS_COMMITTING)
	{
		routine->CommitForeignTransaction(&finfo);
		elog(DEBUG1, "successfully committed the prepared foreign transaction %s",
			 fdwxact->data.identifier);
	}
	else
	{
		routine->RollbackForeignTransaction(&finfo);
		elog(DEBUG1, "successfully rolled back the prepared foreign transaction %s",
			 fdwxact->data.identifier);
	}

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	remove_fdwxact(fdwxact);
	LWLockRelease(FdwXactLock);
}

/*
 * Compute the oldest xmin across all unresolved foreign transactions
 * and store it in the ProcArray.
 */
static void
FdwXactComputeRequiredXmin(void)
{
	TransactionId agg_xmin = InvalidTransactionId;

	Assert(FdwXactCtl != NULL);

	LWLockAcquire(FdwXactLock, LW_SHARED);

	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		FdwXactState fdwxact = FdwXactCtl->xacts[i];

		if (!fdwxact->valid)
			continue;

		Assert(TransactionIdIsValid(fdwxact->data.xid));

		/*
		 * We can exclude entries that are marked as either committing or
		 * aborting and its state file is on disk since such entries no longer
		 * need to lookup its transaction status from the commit log.
		 */
		if (!TransactionIdIsValid(agg_xmin) ||
			TransactionIdPrecedes(fdwxact->data.xid, agg_xmin) ||
			(fdwxact->ondisk &&
			 (fdwxact->status == FDWXACT_STATUS_COMMITTING ||
			  fdwxact->status == FDWXACT_STATUS_ABORTING)))
			agg_xmin = fdwxact->data.xid;
	}

	LWLockRelease(FdwXactLock);

	ProcArraySetFdwXactUnresolvedXmin(agg_xmin);
}


/*
 * Return whether the foreign transaction associated with the given transaction
 * id should be committed or rolled back according to the result of the local
 * transaction.
 */
static FdwXactStatus
FdwXactGetTransactionFate(TransactionId xid)
{
	/*
	 * If the local transaction is already committed, commit prepared foreign
	 * transaction.
	 */
	if (TransactionIdDidCommit(xid))
		return FDWXACT_STATUS_COMMITTING;

	/*
	 * If the local transaction is already aborted, abort prepared foreign
	 * transactions.
	 */
	else if (TransactionIdDidAbort(xid))
		return FDWXACT_STATUS_ABORTING;

	/*
	 * The local transaction is not in progress but the foreign transaction is
	 * not prepared on the foreign server. This can happen when transaction
	 * failed after registered this entry but before actual preparing on the
	 * foreign server. So let's assume it aborted.
	 */
	else if (!TransactionIdIsInProgress(xid))
		return FDWXACT_STATUS_ABORTING;

	/*
	 * The Local transaction is in progress and foreign transaction is about
	 * to be committed or aborted.	Raise an error anyway since we cannot
	 * determine the fate of this foreign transaction according to the local
	 * transaction whose fate is also not determined.
	 */
	elog(ERROR,
		 "cannot resolve the foreign transaction associated with in-process transaction");

	pg_unreachable();
}


/*
 * Recreates a foreign transaction state file. This is used in WAL replay
 * and during checkpoint creation.
 *
 * Note: content and len don't include CRC.
 *
 * XXX Why is it OK to discard the existing content of the file?
 */
void
RecreateFdwXactFile(TransactionId xid, Oid umid, void *content, int len)
{
	char		path[MAXPGPATH];
	pg_crc32c	statefile_crc;
	int			fd;

	/* Recompute CRC */
	INIT_CRC32C(statefile_crc);
	COMP_CRC32C(statefile_crc, content, len);
	FIN_CRC32C(statefile_crc);

	FdwXactStateFilePath(path, xid, umid);

	fd = OpenTransientFile(path, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);

	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not recreate foreign transaction state file \"%s\": %m",
						path)));

	/* Write content and CRC */
	pgstat_report_wait_start(WAIT_EVENT_FDWXACT_FILE_WRITE);
	if (write(fd, content, len) != len)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write foreign transaction state file: %m")));
	}
	if (write(fd, &statefile_crc, sizeof(pg_crc32c)) != sizeof(pg_crc32c))
	{
		if (errno == 0)
			errno = ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write foreign transaction state file: %m")));
	}
	pgstat_report_wait_end();

	/*
	 * We must fsync the file because the end-of-replay checkpoint will not do
	 * so, there being no FDWXACT in shared memory yet to tell it to.
	 */
	pgstat_report_wait_start(WAIT_EVENT_FDWXACT_FILE_SYNC);
	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync foreign transaction state file: %m")));
	pgstat_report_wait_end();

	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close foreign transaction file: %m")));
}


/* Apply the redo log for a foreign transaction */
void
fdwxact_redo(XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_FDWXACT_INSERT)
	{
		/*
		 * Add fdwxact entry and set start/end lsn of the WAL record in
		 * FdwXactState entry.
		 */
		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		FdwXactRedoAdd(XLogRecGetData(record), record->ReadRecPtr,
					   record->EndRecPtr);
		LWLockRelease(FdwXactLock);
	}
	else if (info == XLOG_FDWXACT_REMOVE)
	{
		xl_fdwxact_remove *record = (xl_fdwxact_remove *) rec;

		/* Delete FdwXactState entry and file if exists */
		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		FdwXactRedoRemove(record->xid, record->umid, false);
		LWLockRelease(FdwXactLock);
	}
	else
		elog(ERROR, "invalid log type %d in foreign transaction log record", info);

	return;
}

/*
 * Scan the shared memory entries of FdwXactState and determine the range of valid
 * XIDs present.  This is run during database startup, after we have completed
 * reading WAL.	 ShmemVariableCache->nextXid has been set to one more than
 * the highest XID for which evidence exists in WAL.
 *
 * On corrupted two-phase files, fail immediately.	Keeping around broken
 * entries and let replay continue causes harm on the system, and a new
 * backup should be rolled in.
 *
 * Our other responsibility is to update and return the oldest valid XID
 * among the distributed transactions. This is needed to synchronize pg_subtrans
 * startup properly.
 */
TransactionId
PrescanFdwXacts(TransactionId oldestActiveXid)
{
	FullTransactionId nextXid = ShmemVariableCache->nextXid;
	TransactionId origNextXid = XidFromFullTransactionId(nextXid);
	TransactionId result = origNextXid;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		FdwXactState fdwxact = FdwXactCtl->xacts[i];
		char	   *buf;

		buf = ProcessFdwXactBuffer(fdwxact->data.xid, fdwxact->data.umid,
								   fdwxact->insert_start_lsn, fdwxact->ondisk);

		if (buf == NULL)
			continue;

		if (TransactionIdPrecedes(fdwxact->data.xid, result))
			result = fdwxact->data.xid;

		pfree(buf);
	}
	LWLockRelease(FdwXactLock);

	return result;
}

/*
 * Scan pg_fdwxact and fill FdwXactState depending on the on-disk data.
 * This is called once at the beginning of recovery, saving any extra
 * lookups in the future.  FdwXactState files that are newer than the
 * minimum XID horizon are discarded on the way.
 */
void
RestoreFdwXactData(void)
{
	DIR		   *cldir;
	struct dirent *clde;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	cldir = AllocateDir(FDWXACTS_DIR);
	while ((clde = ReadDir(cldir, FDWXACTS_DIR)) != NULL)
	{
		/*
		 * XXX shouldn't we error out if these checks fail? Suggests there
		 * is garbage in the pg_fdwxact directory.
		 */
		if (strlen(clde->d_name) == FDWXACT_FILE_NAME_LEN &&
			strspn(clde->d_name, "0123456789ABCDEF_") == FDWXACT_FILE_NAME_LEN)
		{
			TransactionId xid;
			Oid			umid;
			char	   *buf;

			sscanf(clde->d_name, "%08x_%08x", &xid, &umid);

			/* Read fdwxact data from disk */
			buf = ProcessFdwXactBuffer(xid, umid, InvalidXLogRecPtr,
									   true);

			/* XXX We're reading the data from file, so how could this be NULL? */
			if (buf == NULL)
				continue;

			/* Add this entry into the table of foreign transactions */
			FdwXactRedoAdd(buf, InvalidXLogRecPtr, InvalidXLogRecPtr);
		}
	}

	LWLockRelease(FdwXactLock);
	FreeDir(cldir);
}

/*
 * Scan the shared memory entries of FdwXactState and validate them.
 *
 * This is run at the end of recovery, but before we allow backends to write
 * WAL.
 *
 * XXX It's a bit confusing that we have both RestoreFdwXacts and RecoverFdwXacts.
 */
void
RecoverFdwXacts(void)
{
	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		FdwXactState fdwxact = FdwXactCtl->xacts[i];
		char	   *buf;

		buf = ProcessFdwXactBuffer(fdwxact->data.xid, fdwxact->data.umid,
								   fdwxact->insert_start_lsn, fdwxact->ondisk);

		if (buf == NULL)
			continue;

		ereport(LOG,
				(errmsg("recovering foreign prepared transaction %s from shared memory",
						fdwxact->data.identifier)));

		/* recovered, so reset the flag for entries generated by redo */
		fdwxact->inredo = false;
		fdwxact->valid = true;
		pfree(buf);
	}
	LWLockRelease(FdwXactLock);
}

/*
 * Store pointer to the start/end of the WAL record along with the xid in
 * a fdwxact entry in shared memory FdwXactData structure.
 */
static void
FdwXactRedoAdd(char *buf, XLogRecPtr start_lsn, XLogRecPtr end_lsn)
{
	FdwXactStateOnDiskData *fdwxact_data = (FdwXactStateOnDiskData *) buf;
	FdwXactState fdwxact;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));
	Assert(RecoveryInProgress());

	/*
	 * Add this entry into the table of foreign transactions. The status of
	 * the transaction is set as preparing, since we do not know the exact
	 * status right now. Resolver will set it later based on the status of
	 * local transaction which prepared this foreign transaction.
	 */
	fdwxact = insert_fdwxact(fdwxact_data->dbid, fdwxact_data->xid,
							 fdwxact_data->umid, fdwxact_data->serverid,
							 fdwxact_data->owner, fdwxact_data->identifier);

	elog(DEBUG2, "added fdwxact entry in shared memory for foreign transaction, db %u xid %u user mapping %u owner %u id %s",
		 fdwxact_data->dbid, fdwxact_data->xid,
		 fdwxact_data->umid, fdwxact_data->owner,
		 fdwxact_data->identifier);

	/*
	 * Set status as PREPARED, since we do not know the xact status right now.
	 * We will set it later based on the status of local transaction that
	 * prepared this fdwxact entry.
	 */
	fdwxact->status = FDWXACT_STATUS_PREPARED;
	fdwxact->insert_start_lsn = start_lsn;
	fdwxact->insert_end_lsn = end_lsn;
	fdwxact->inredo = true;		/* added in redo */
	fdwxact->valid = false;
	fdwxact->ondisk = XLogRecPtrIsInvalid(start_lsn);
}

/*
 * Remove the corresponding fdwxact entry from FdwXactCtl. Also remove
 * FdwXactState file if a foreign transaction was saved via an earlier checkpoint.
 * We could not found the FdwXactState entry in the case where a crash recovery
 * starts from the point where is after added but before removed the entry.
 *
 * XXX So can the issue actually happen? If we start from LSN where we already
 * added the entry, shouldn't RestoreFdwXactData already added the entry?
 *
 * XXX The givewarning parameter is unused, so maybe the comment is obsolete?
 */
static void
FdwXactRedoRemove(TransactionId xid, Oid umid, bool givewarning)
{
	FdwXactState fdwxact;
	int			i;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));
	Assert(RecoveryInProgress());

	for (i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		fdwxact = FdwXactCtl->xacts[i];

		if (fdwxact->data.xid == xid && fdwxact->data.umid == umid)
			break;
	}

	/* XXX What if we haven't found a matching entry? Shouldn't that be
	 * actually an error? */
	if (i >= FdwXactCtl->num_xacts)
		return;

	/* Clean up entry and any files we may have left */
	remove_fdwxact(fdwxact);

	elog(DEBUG2, "removed fdwxact entry from shared memory for foreign transaction %s",
		 fdwxact->data.identifier);
}

/*
 * Reads foreign transaction data from xlog. During checkpoint this data will
 * be moved to fdwxact files and ReadFdwXactStateFile should be used instead.
 *
 * Note clearly that this function accesses WAL during normal operation, similarly
 * to the way WALSender or Logical Decoding would do. It does not run during
 * crash recovery or standby processing.
 *
 * XXX Isn't it quite inefficient that we allocate xlogreader over and over,
 * for each xact? Do we do that for each xact that we find in the WAL, even if
 * we remove the entry shortly after?
 */
static void
XlogReadFdwXactData(XLogRecPtr lsn, char **buf, int *len)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;

	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &read_local_xlog_page,
											   .segment_open = &wal_segment_open,
											   .segment_close = &wal_segment_close),
									NULL);

	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating an XLog reading processor.")));

	XLogBeginRead(xlogreader, lsn);
	record = XLogReadRecord(xlogreader, &errormsg);

	if (record == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read foreign transaction state from xlog at %X/%X",
						LSN_FORMAT_ARGS(lsn))));

	if (XLogRecGetRmid(xlogreader) != RM_FDWXACT_ID ||
		(XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK) != XLOG_FDWXACT_INSERT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("expected foreign transaction state data is not present in xlog at %X/%X",
						LSN_FORMAT_ARGS(lsn))));

	if (len != NULL)
		*len = XLogRecGetDataLen(xlogreader);

	*buf = palloc(sizeof(char) * XLogRecGetDataLen(xlogreader));
	memcpy(*buf, XLogRecGetData(xlogreader), sizeof(char) * XLogRecGetDataLen(xlogreader));

	XLogReaderFree(xlogreader);
}

/*
 * Given a transaction id, userid and serverid read it either from disk
 * or read it directly via shmem xlog record pointer using the provided
 * "insert_start_lsn".
 */
static char *
ProcessFdwXactBuffer(TransactionId xid, Oid umid, XLogRecPtr insert_start_lsn,
					 bool fromdisk)
{
	TransactionId origNextXid =
	XidFromFullTransactionId(ShmemVariableCache->nextXid);
	char	   *buf;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

	if (!fromdisk)
		Assert(!XLogRecPtrIsInvalid(insert_start_lsn));

	/* Reject XID if too new */
	if (TransactionIdFollowsOrEquals(xid, origNextXid))
	{
		if (fromdisk)
		{
			ereport(WARNING,
					(errmsg("removing future fdwxact state file for xid %u and user mapping %u",
							xid, umid)));
			RemoveFdwXactStateFile(xid, umid, true);
		}
		else
		{
			ereport(WARNING,
					(errmsg("removing future fdwxact state from memory for xid %u and user mapping %u",
							xid, umid)));
			FdwXactRedoRemove(xid, umid, true);
		}
		return NULL;
	}

	if (fromdisk)
	{
		/* Read and validate file */
		buf = ReadFdwXactStateFile(xid, umid);
	}
	else
	{
		/* Read xlog data */
		XlogReadFdwXactData(insert_start_lsn, &buf, NULL);
	}

	return buf;
}

/*
 * Read and validate the foreign transaction state file.
 *
 * If it looks OK (has a valid magic number and CRC), return the palloc'd
 * contents of the file, issuing an error when finding corrupted data.
 * This state can be reached when doing recovery.
 */
static char *
ReadFdwXactStateFile(TransactionId xid, Oid umid)
{
	char		path[MAXPGPATH];
	int			fd;
	FdwXactStateOnDiskData *fdwxact_file_data;
	struct stat stat;
	uint32		crc_offset;
	pg_crc32c	calc_crc;
	pg_crc32c	file_crc;
	char	   *buf;
	int			r;

	FdwXactStateFilePath(path, xid, umid);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open FDW transaction state file \"%s\": %m",
						path)));

	/*
	 * Check file length.  We can determine a lower bound pretty easily. We
	 * set an upper bound to avoid palloc() failure on a corrupt file, though
	 * we can't guarantee that we won't get an out of memory error anyway,
	 * even on a valid file.
	 */
	if (fstat(fd, &stat))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat FDW transaction state file \"%s\": %m",
						path)));

	if (stat.st_size < (offsetof(FdwXactStateOnDiskData, identifier) +
						sizeof(pg_crc32c)) ||
		stat.st_size > MaxAllocSize)

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("too large FDW transaction state file \"%s\": %m",
						path)));

	crc_offset = stat.st_size - sizeof(pg_crc32c);
	if (crc_offset != MAXALIGN(crc_offset))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("incorrect alignment of CRC offset for file \"%s\"",
						path)));

	/*
	 * Ok, slurp in the file.
	 */
	buf = (char *) palloc(stat.st_size);
	fdwxact_file_data = (FdwXactStateOnDiskData *) buf;

	/* Slurp the file */
	pgstat_report_wait_start(WAIT_EVENT_FDWXACT_FILE_READ);
	r = read(fd, buf, stat.st_size);
	if (r != stat.st_size)
	{
		if (r < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", path)));
		else
			ereport(ERROR,
					(errmsg("could not read file \"%s\": read %d of %zu",
							path, r, (Size) stat.st_size)));
	}
	pgstat_report_wait_end();

	if (CloseTransientFile(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));

	/*
	 * Check the CRC.
	 */
	INIT_CRC32C(calc_crc);
	COMP_CRC32C(calc_crc, buf, crc_offset);
	FIN_CRC32C(calc_crc);

	file_crc = *((pg_crc32c *) (buf + crc_offset));

	if (!EQ_CRC32C(calc_crc, file_crc))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("calculated CRC checksum does not match value stored in file \"%s\"",
						path)));

	/* Check if the contents is an expected data */
	fdwxact_file_data = (FdwXactStateOnDiskData *) buf;
	if (fdwxact_file_data->xid != xid ||
		fdwxact_file_data->umid != umid)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid foreign transaction state file \"%s\"",
						path)));

	return buf;
}

/*
 * Remove the foreign transaction file for given entry.
 *
 * If giveWarning is false, do not complain about file-not-present;
 * this is an expected case during WAL replay.
 *
 * XXX Shouldn't this fsync the directory, to make the unlink persistent?
 * Although, maybe it's fine if we find old files during recovery?
 */
static void
RemoveFdwXactStateFile(TransactionId xid, Oid umid, bool giveWarning)
{
	char		path[MAXPGPATH];

	FdwXactStateFilePath(path, xid, umid);
	if (unlink(path) < 0 && (errno != ENOENT || giveWarning))
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not remove foreign transaction state file \"%s\": %m",
						path)));
}

/*
 * Return the list of FdwXactState entries that match at least one given criteria
 * that is not invalid.  The caller must hold FdwXactLock.
 *
 * XXX I guess this should say "list of valid entries matching any criteria". But
 * it's a bit misleading, because we never supply more than just a single param.
 * So it could just as well say "matching all criteria" considering we ignore
 * InvalidOid and InvalidTransactionId.
 *
 * XXX It's a bit pointless to return List, because we don't use it at all. In
 * fact, we only use this to check if any matching entries exist. So we might
 * just as well return "true" or "false" on the first matching entry.
 */
static List *
find_fdwxacts(TransactionId xid, Oid umid, Oid dbid)
{
	List	   *res = NIL;

	Assert(LWLockHeldByMe(FdwXactLock));

	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		FdwXactState fdwxact = FdwXactCtl->xacts[i];
		bool		match = false;

		if (!fdwxact->valid)
			continue;

		/* xid */
		if (TransactionIdIsValid(xid) && xid == fdwxact->data.xid)
			match = true;

		/* umid */
		if (OidIsValid(umid) && umid == fdwxact->data.umid)
			match = true;

		/* dbid */
		if (OidIsValid(dbid) && dbid == fdwxact->data.dbid)
			match = true;

		if (match)
			res = lappend(res, fdwxact);
	}

	return res;
}

/*
 * Get FdwXact entry and do some sanity checks. If check_twophase_xact is true, we
 * also check if the given xid is prepared.  The caller must hold FdwXactLock.
 */
static FdwXactState
get_fdwxact_with_check(TransactionId xid, Oid umid, bool check_twophase_xact)
{
	FdwXactState fdwxact = NULL;
	Oid			myuserid;

	Assert(LWLockHeldByMe(FdwXactLock));

	/* Look for FdwXactState entry that matches the given xid and umid */
	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		FdwXactState fx = FdwXactCtl->xacts[i];

		if (fx->valid && fx->data.xid == xid && fx->data.umid == umid)
		{
			fdwxact = fx;
			break;
		}
	}

	/* not found */
	if (!fdwxact)
		return NULL;

	/* check if belonging to another database */
	if (fdwxact->data.dbid != MyDatabaseId)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign transaction belongs to another database"),
				 errhint("Connect to the database where the transaction was created to finish it.")));

	/* permission check */
	myuserid = GetUserId();
	if (myuserid != fdwxact->data.owner && !superuser_arg(myuserid))
		ereport(ERROR,
				(errmsg("permission denied to resolve prepared foreign transaction"),
				 errhint("Must be superuser or the user that prepared the transaction")));

	/* check if the entry is being processed by someone */
	if (fdwxact->locking_backend != InvalidBackendId)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign transaction with transaction identifier \"%s\" is busy",
						fdwxact->data.identifier)));

	if (check_twophase_xact && TwoPhaseExists(fdwxact->data.xid))
	{
		/*
		 * the entry's local transaction is prepared. Since we cannot know the
		 * fate of the local transaction, we cannot resolve this foreign
		 * transaction.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot resolve foreign transaction with identifier \"%s\" whose local transaction is in-progress",
						fdwxact->data.identifier),
				 errhint("Do COMMIT PREPARED or ROLLBACK PREPARED")));
	}

	return fdwxact;
}

/* Error cleanup callback for pg_foreign_resolve/remove_xact */
static void
pg_foreign_xact_callback(int code, Datum arg)
{
	FdwXactState fdwxact = (FdwXactState) DatumGetPointer(arg);

	if (fdwxact->valid)
	{
		Assert(fdwxact->locking_backend == MyBackendId);

		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		fdwxact->locking_backend = InvalidBackendId;
		LWLockRelease(FdwXactLock);
	}
}

/* Built in functions */

/*
 * Structure to hold and iterate over the foreign transactions to be displayed
 * by the built-in functions.
 */
typedef struct
{
	FdwXactState fdwxacts;
	int			num_xacts;
	int			cur_xact;
}			WorkingStatus;

Datum
pg_foreign_xacts(PG_FUNCTION_ARGS)
{
#define PG_PREPARED_FDWXACTS_COLS	7
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(FdwXactLock, LW_SHARED);
	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		FdwXactState fdwxact = FdwXactCtl->xacts[i];
		FdwXactStatus status;
		char	   *xact_status;
		Datum		values[PG_PREPARED_FDWXACTS_COLS];
		bool		nulls[PG_PREPARED_FDWXACTS_COLS];

		if (!fdwxact->valid)
			continue;

		memset(nulls, 0, sizeof(nulls));

		SpinLockAcquire(&fdwxact->mutex);
		status = fdwxact->status;
		SpinLockRelease(&fdwxact->mutex);

		values[0] = TransactionIdGetDatum(fdwxact->data.xid);
		values[1] = ObjectIdGetDatum(fdwxact->data.umid);
		values[2] = ObjectIdGetDatum(fdwxact->data.owner);
		values[3] = ObjectIdGetDatum(fdwxact->data.dbid);

		switch (status)
		{
			case FDWXACT_STATUS_PREPARING:
				xact_status = "preparing";
				break;
			case FDWXACT_STATUS_PREPARED:
				xact_status = "prepared";
				break;
			case FDWXACT_STATUS_COMMITTING:
				xact_status = "committing";
				break;
			case FDWXACT_STATUS_ABORTING:
				xact_status = "aborting";
				break;
			default:
				xact_status = "unknown";
				break;
		}

		values[4] = CStringGetTextDatum(xact_status);
		values[5] = CStringGetTextDatum(fdwxact->data.identifier);

		if (fdwxact->locking_backend != InvalidBackendId)
		{
			PGPROC	   *locker = BackendIdGetProc(fdwxact->locking_backend);

			values[6] = Int32GetDatum(locker->pid);
		}
		else
			nulls[6] = true;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	LWLockRelease(FdwXactLock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * Built-in SQL function to resolve a prepared foreign transaction.
 */
Datum
pg_resolve_foreign_xact(PG_FUNCTION_ARGS)
{
	TransactionId xid = DatumGetTransactionId(PG_GETARG_DATUM(0));
	Oid			umid = PG_GETARG_OID(1);
	FdwXactState fdwxact;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	fdwxact = get_fdwxact_with_check(xid, umid, true);

	/* lock it */
	fdwxact->locking_backend = MyBackendId;

	LWLockRelease(FdwXactLock);

	/*
	 * Resolve the foreign transaction.  We ensure unlocking FdwXact entry at
	 * an error or an interruption.
	 *
	 * XXX we assume that an interruption doesn't happen between locking
	 * FdwXact entry and registering the callback, especially in
	 * LWLockRelease().
	 */
	PG_ENSURE_ERROR_CLEANUP(pg_foreign_xact_callback,
							(Datum) PointerGetDatum(fdwxact));
	{
		ResolveOneFdwXact(fdwxact);
	}
	PG_END_ENSURE_ERROR_CLEANUP(pg_foreign_xact_callback,
								(Datum) PointerGetDatum(fdwxact));

	PG_RETURN_BOOL(true);
}

/*
 * Built-in function to remove a prepared foreign transaction entry without
 * resolution. The function gives a way to forget about such prepared
 * transaction in case: the foreign server where it is prepared is no longer
 * available, the user which prepared this transaction needs to be dropped.
 *
 * XXX The XA specification [1] actually has xa_forget, so maybe this should
 * be called pg_forget_foreign_xact?
 *
 * [1] https://pubs.opengroup.org/onlinepubs/009680699/toc.pdf
 */
Datum
pg_remove_foreign_xact(PG_FUNCTION_ARGS)
{
	TransactionId xid = DatumGetTransactionId(PG_GETARG_DATUM(0));
	Oid			umid = PG_GETARG_OID(1);
	FdwXactState fdwxact;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	fdwxact = get_fdwxact_with_check(xid, umid, false);

	/* Clean up entry and any files we may have left */
	remove_fdwxact(fdwxact);

	LWLockRelease(FdwXactLock);

	PG_RETURN_BOOL(true);
}

extern int
FdwXactCount(void)
{
	return FdwXactCtl->num_xacts;
}

extern FdwXactState
FdwXactGetState(int i)
{
	return FdwXactCtl->xacts[i];
}
