/*-------------------------------------------------------------------------
 *
 * resolver.c
 *
 * The foreign transaction resolver background worker resolves in-doubt
 * foreign transactions, foreign transactions participate to a distributed
 * transaction but aren't being processed anyone.  A resolver process is
 * launched per foreign server by foreign transaction launcher.
 *
 * Normal termination is by SIGTERM, which instructs the resolver process
 * to exit(0) at the next convenient moment. Emergency termination is by
 * SIGQUIT; like any backend. The resolver process also terminate by timeouts
 * only if there is no pending foreign transactions on the database waiting
 * to be resolved.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/fdwxact_resolver.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/fdwxact.h"
#include "access/fdwxact_resolver.h"
#include "access/fdwxact_launcher.h"
#include "access/resolver_internal.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

/* max sleep time between cycles (3min) */
#define DEFAULT_NAPTIME_PER_CYCLE 180000L

/* GUC parameters */
int			foreign_xact_resolution_retry_interval;
int			foreign_xact_resolver_timeout = 60 * 1000;

FdwXactResolverCtlData *FdwXactResolverCtl;

static void FdwXactResolverLoop(void);
static long FdwXactResolverComputeSleepTime(TimestampTz now);
static void FdwXactResolverCheckTimeout(TimestampTz now);

static void FdwXactResolverOnExit(int code, Datum arg);
static void FdwXactResolverDetach(void);
static void FdwXactResolverAttach(int slot);
static void FdwXactResolverProcessInDoubtXacts(void);

/* The list of currently holding FdwXact entries. */
static List *heldFdwXactEntries = NIL;

static TimestampTz last_resolution_time = -1;

/*
 * Detach the resolver and cleanup the resolver info.
 */
static void
FdwXactResolverDetach(void)
{
	LWLockAcquire(FdwXactResolverLock, LW_EXCLUSIVE);

	MyFdwXactResolver->pid = InvalidPid;
	MyFdwXactResolver->in_use = false;
	MyFdwXactResolver->dbid = InvalidOid;
	MyFdwXactResolver->serverid = InvalidOid;

	LWLockRelease(FdwXactResolverLock);

	/*
	 * Force to send remaining WAL statistics to the stats collector at
	 * process exit.
	 *
	 * Since pgstat_report_wal is invoked with 'force' is false in main loop
	 * to avoid overloading to the stats collector, there may exist unsent
	 * stats counters for the WAL writer.
	 */
	pgstat_report_wal(true);
}

/*
 * Cleanup up foreign transaction resolver info and releas the holding
 * FdwXactState entries.
 */
static void
FdwXactResolverOnExit(int code, Datum arg)
{
	ListCell   *lc;

	FdwXactResolverDetach();

	/* Release the held foreign transaction entries */
	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	foreach(lc, heldFdwXactEntries)
	{
		FdwXactState fdwxact = (FdwXactState) lfirst(lc);

		if (fdwxact->valid && fdwxact->locking_backend == MyBackendId)
			fdwxact->locking_backend = InvalidBackendId;
	}
	LWLockRelease(FdwXactLock);
}

/*
 * Attach to a slot.
 */
static void
FdwXactResolverAttach(int slot)
{
	/* Block concurrent access */
	LWLockAcquire(FdwXactResolverLock, LW_EXCLUSIVE);

	Assert(slot >= 0 && slot < max_foreign_xact_resolvers);
	MyFdwXactResolver = &FdwXactResolverCtl->resolvers[slot];

	if (!MyFdwXactResolver->in_use)
	{
		LWLockRelease(FdwXactResolverLock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("foreign transaction resolver slot %d is empty, cannot attach",
						slot)));
	}

	Assert(OidIsValid(MyFdwXactResolver->dbid));
	Assert(OidIsValid(MyFdwXactResolver->serverid));

	MyFdwXactResolver->pid = MyProcPid;
	MyFdwXactResolver->latch = &MyProc->procLatch;

	before_shmem_exit(FdwXactResolverOnExit, (Datum) 0);

	LWLockRelease(FdwXactResolverLock);
}

/* Foreign transaction resolver entry point */
void
FdwXactResolverMain(Datum main_arg)
{
	int			slot = DatumGetInt32(main_arg);
	char	   *datname;
	ForeignServer *server;

	/* Attach to a slot */
	FdwXactResolverAttach(slot);

	/* Establish signal handlers */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnectionByOid(MyFdwXactResolver->dbid, InvalidOid, 0);

	StartTransactionCommand();
	datname = get_database_name(MyFdwXactResolver->dbid);
	server = GetForeignServer(MyFdwXactResolver->serverid);
	ereport(LOG,
			(errmsg("foreign transaction resolver for server \"%s\" on database \"%s\" has started",
					server->servername, datname)));
	pfree(datname);
	pfree(server);
	CommitTransactionCommand();

	/* Initialize stats to a sanish value */
	last_resolution_time = GetCurrentTimestamp();

	/* Run the main loop */
	FdwXactResolverLoop();

	proc_exit(0);
}

/*
 * Fdwxact resolver main loop
 */
static void
FdwXactResolverLoop(void)
{
	/* Enter main loop */
	for (;;)
	{
		TimestampTz now;
		int			rc;
		long		sleep_time = DEFAULT_NAPTIME_PER_CYCLE;

		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Resolve in-doubt transactions if any  */
		FdwXactResolverProcessInDoubtXacts();

		now = GetCurrentTimestamp();
		FdwXactResolverCheckTimeout(now);
		sleep_time = FdwXactResolverComputeSleepTime(now);

		/* Send WAL statistics to the stats collector */
		pgstat_report_wal(false);

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   sleep_time,
					   WAIT_EVENT_FDWXACT_RESOLVER_MAIN);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}

/*
 * Check whether there have been foreign transactions by the backend within
 * foreign_xact_resolver_timeout and shutdown if not.
 */
static void
FdwXactResolverCheckTimeout(TimestampTz now)
{
	TimestampTz timeout;
	ForeignServer *server;

	if (foreign_xact_resolver_timeout == 0)
		return;

	timeout = TimestampTzPlusMilliseconds(last_resolution_time,
										  foreign_xact_resolver_timeout);

	if (now < timeout)
		return;

	/* Reached timeout, exit */
	StartTransactionCommand();
	server = GetForeignServer(MyFdwXactResolver->serverid);
	ereport(LOG,
			(errmsg("foreign transaction resolver for server \"%s\" on database \"%s\" will stop because the timeout",
					server->servername,
					get_database_name(MyDatabaseId))));
	CommitTransactionCommand();
	FdwXactResolverDetach();
	proc_exit(0);
}

/*
 * Compute how long we should sleep by the next cycle. We can sleep until the time
 * out.
 */
static long
FdwXactResolverComputeSleepTime(TimestampTz now)
{
	long		sleeptime = DEFAULT_NAPTIME_PER_CYCLE;

	if (foreign_xact_resolver_timeout > 0)
	{
		TimestampTz timeout;

		/* Compute relative time until wakeup. */
		timeout = TimestampTzPlusMilliseconds(last_resolution_time,
											  foreign_xact_resolver_timeout);
		sleeptime = TimestampDifferenceMilliseconds(now, timeout);
	}

	return sleeptime;
}

bool
IsFdwXactResolver(void)
{
	return MyFdwXactResolver != NULL;
}

/*
 * Process in-doubt foreign transactions.
 */
static void
FdwXactResolverProcessInDoubtXacts(void)
{
	ListCell   *lc;

	Assert(heldFdwXactEntries == NIL);

	/* Hold all in-doubt foreign transactions */
	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	for (int i = 0; i < FdwXactCount(); i++)
	{
		FdwXactState fdwxact = FdwXactGetState(i);

		if (fdwxact->valid &&
			fdwxact->locking_backend == InvalidBackendId &&
			fdwxact->data.dbid == MyFdwXactResolver->dbid &&
			fdwxact->data.serverid == MyFdwXactResolver->serverid &&
			!TwoPhaseExists(fdwxact->data.xid))
		{
			fdwxact->locking_backend = MyBackendId;
			heldFdwXactEntries = lappend(heldFdwXactEntries, fdwxact);
		}
	}
	LWLockRelease(FdwXactLock);

	foreach(lc, heldFdwXactEntries)
	{
		FdwXactState fdwxact = (FdwXactState) lfirst(lc);

		/*
		 * Resolve one foreign transaction. ResolveOneFdwXact() releases and
		 * removes FdwXactState entry after resolution.
		 */
		StartTransactionCommand();
		ResolveOneFdwXact(fdwxact);
		CommitTransactionCommand();
	}

	if (list_length(heldFdwXactEntries) > 0)
		last_resolution_time = GetCurrentTimestamp();

	list_free(heldFdwXactEntries);
	heldFdwXactEntries = NIL;
}
