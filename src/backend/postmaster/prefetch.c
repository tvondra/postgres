/*-------------------------------------------------------------------------
 *
 * prefetch.c
 *
 * PostgreSQL Integrated Prefetch Daemon
 *
 * The prefetch system is structured in two different kinds of processes: the
 * prefetch launcher and the prefetch worker.  The launcher is an
 * always-running process, started by the postmaster when the prefetch_workers
 * GUC parameter is set to non-zero value.  The launcher schedules prefetch
 * workers to be started when appropriate.  The workers are the processes
 * which execute the actual I/O requests, received from a shared queue.
 *
 * The prefetch launcher cannot start the worker processes by itself,
 * because doing so would cause robustness issues (namely, failure to shut
 * them down on exceptional conditions, and also, since the launcher is
 * connected to shared memory and is thus subject to corruption there, it is
 * not as robust as the postmaster).  So it leaves that task to the postmaster.
 *
 * There is an prefetch shared memory area, where the launcher stores
 * information for the worker (at the moment there's just a single queue of
 * requests, but in the future we may support multiple separate queues etc.)
 * When it wants a new worker to start, it sets a flag in shared memory and
 * sends a signal to the postmaster.  Then postmaster knows nothing more than
 * it must start a prefetch worker; so it forks a new child, which turns into
 * a worker.  This new process connects to shared memory, and there it can
 * inspect the information that the launcher has set up.
 *
 * If the fork() call fails in the postmaster, it sets a flag in the shared
 * memory area, and sends a signal to the launcher.  The launcher, upon
 * noticing the flag, can try starting the worker again by resending the
 * signal.  Note that the failure can only be transient (fork failure due to
 * high load, memory pressure, too many processes, etc); more permanent
 * problems, are detected later in the worker and dealt with just by having
 * the worker exit normally.  The launcher will launch a new worker again
 * later, per schedule.
 *
 * When the worker is done prefetching it sends SIGUSR2 to the launcher.  The
 * launcher then wakes up and is able to launch another worker, if the schedule
 * is so tight that a new worker is needed immediately.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/prefetch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <sys/time.h>
#include <unistd.h>

#include "access/xact.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/prefetch.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "storage/smgr.h"
#include "storage/buf_internals.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"


/*
 * GUC parameters
 */
bool		async_prefetch_enabled;
bool		async_prefetch_buffers;
int			async_prefetch_workers;
int			async_prefetch_naptime;

/* Flags to tell if we are in an prefetch process */
static bool am_prefetch_launcher = false;
static bool am_prefetch_worker = false;

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGUSR2 = false;
static volatile sig_atomic_t got_SIGTERM = false;

/* Memory context for long-lived data */
static MemoryContext PrefetchMemCxt;

/*-------------
 * This struct holds information about a single worker's whereabouts.  We keep
 * an array of these in shared memory, sized according to prefetch_workers.
 *
 * wi_links		entry into free list or running list
 * wi_proc		pointer to PGPROC of the running worker, NULL if not started
 * wi_launchtime Time at which this worker was launched
 *
 * All fields are protected by PrefetchLock.
 *-------------
 */
typedef struct WorkerInfoData
{
	dlist_node	wi_links;
	PGPROC	   *wi_proc;
	TimestampTz wi_launchtime;
} WorkerInfoData;

typedef struct WorkerInfoData *WorkerInfo;

/*
 * Possible signals received by the launcher from remote processes.  These are
 * stored atomically in shared memory so that other processes can set them
 * without locking.
 */
typedef enum
{
	PrefetchForkFailed,			/* failed trying to start a worker */
	PrefetchNumSignals			/* must be last */
}			PrefetchSignal;

/*
 * Length of the prefetch queue. 2048 seems like a reasonable value,
 * although it might be insufficient for multi-tablespace setups etc.
 */
#define	QUEUE_SIZE		2048

/*-------------
 * The main prefetch shmem struct.  On shared memory we store this main
 * struct and the array of WorkerInfo structs.  This struct keeps:
 *
 * prefetch_signal			set by other processes to indicate various conditions
 * prefetch_launcherpid		the PID of the prefetch launcher
 * prefetch_freeWorkers		the WorkerInfo freelist
 * prefetch_runningWorkers	the WorkerInfo non-free queue
 * prefetch_startingWorker	pointer to WorkerInfo currently being started (cleared
 * 							by the worker itself as soon as it's up and running)
 * prefetch_requests_start	first request in the queue
 * prefetch_requests_count	number of valid requests in the queue
 * prefetch_requests		an array of prefetch requests
 *
 * This struct is protected by PrefetchLock, except for prefetch_signal.
 *-------------
 */
typedef struct
{
	sig_atomic_t prefetch_signal[PrefetchNumSignals];
	pid_t		prefetch_launcherpid;
	dlist_head	prefetch_freeWorkers;
	dlist_head	prefetch_runningWorkers;
	WorkerInfo	prefetch_startingWorker;

	/* locks */
	slock_t		prefetch_stats_lck;		/* protects the stats fields */
	slock_t		prefetch_queue_lck;		/* protects the queue fields */

	/* prefetch queue statistics */
	uint64		prefetch_stats_nsubmitted;
	uint64		prefetch_stats_nprocessed;
	uint64		prefetch_stats_nskipped;
	uint64		prefetch_stats_nerrors;
	uint64		prefetch_stats_nfull;

	/* used to notify about new requests and available space in queue */
	ConditionVariable requests_cv; /* signaled after adding new requests */
	ConditionVariable free_cv;	/* signaled after removing requests */

	int			prefetch_queue_start;	/* index of first request */
	int			prefetch_queue_count;	/* number of queued requests */
	BufferTag	prefetch_queue[QUEUE_SIZE];
} PrefetchShmemStruct;

static PrefetchShmemStruct *PrefetchShmem;

/* Pointer to my own WorkerInfo, valid on each worker */
static WorkerInfo MyWorkerInfo = NULL;

/* PID of launcher, valid only in worker while shutting down */
int			PrefetchLauncherPid = 0;

#ifdef EXEC_BACKEND
static pid_t prefetch_launcher_forkexec(void);
static pid_t prefetch_worker_forkexec(void);
#endif
NON_EXEC_STATIC void PrefetchWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();
NON_EXEC_STATIC void PrefetchLauncherMain(int argc, char *argv[]) pg_attribute_noreturn();

static void launch_worker(TimestampTz now);

static void FreeWorkerInfo(int code, Datum arg);

static void prefetch_sighup_handler(SIGNAL_ARGS);
static void prefetch_launcher_sigusr2_handler(SIGNAL_ARGS);
static void prefetch_launcher_sigterm_handler(SIGNAL_ARGS);

static void do_prefetch(void);

/********************************************************************
 *					  PREFETCH LAUNCHER CODE
 ********************************************************************/

#ifdef EXEC_BACKEND
/*
 * forkexec routine for the prefetch launcher process.
 *
 * Format up the arglist, then fork and exec.
 */
static pid_t
prefetch_launcher_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkprefetchlauncher";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}

/*
 * We need this set from the outside, before InitProcess is called
 */
void
PrefetchLauncherIAm(void)
{
	am_prefetch_launcher = true;
}
#endif

/*
 * Main entry point for prefetch launcher process, to be called from the
 * postmaster.
 */
int
StartPrefetchLauncher(void)
{
	pid_t		PrefetchPID;

#ifdef EXEC_BACKEND
	switch ((PrefetchPID = prefetch_launcher_forkexec()))
#else
	switch ((PrefetchPID = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork prefetch launcher process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			PrefetchLauncherMain(0, NULL);
			break;
#endif
		default:
			return (int) PrefetchPID;
	}

	/* shouldn't get here */
	return 0;
}

/*
 * Main loop for the prefetch launcher process.
 */
NON_EXEC_STATIC void
PrefetchLauncherMain(int argc, char *argv[])
{
	sigjmp_buf	local_sigjmp_buf;

	am_prefetch_launcher = true;

	/* Identify myself via ps */
	init_ps_display(pgstat_get_backend_desc(B_PREFETCH_LAUNCHER), "", "", "");

	ereport(DEBUG1,
			(errmsg("prefetch launcher started")));

	if (PostAuthDelay)
		pg_usleep(PostAuthDelay * 1000000L);

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers.  We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 */
	pqsignal(SIGHUP, prefetch_sighup_handler);
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, prefetch_launcher_sigterm_handler);

	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts();		/* establishes SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, prefetch_launcher_sigusr2_handler);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/* Early initialization */
	BaseInit();

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif

	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL, false);

	SetProcessingMode(NormalProcessing);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.
	 */
	PrefetchMemCxt = AllocSetContextCreate(TopMemoryContext,
										   "Prefetch Launcher",
										   ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(PrefetchMemCxt);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * This code is a stripped down version of PostgresMain error recovery.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Forget any pending QueryCancel or timeout request */
		disable_all_timeouts(false);
		QueryCancelPending = false; /* second to avoid race condition */

		/* Report the error to the server log */
		EmitErrorReport();

		/* Abort the current transaction in order to recover */
		AbortCurrentTransaction();

		/*
		 * Release any other resources, for the case where we were not in a
		 * transaction.
		 */
		LWLockReleaseAll();
		pgstat_report_wait_end();
		AbortBufferIO();
		UnlockBuffers();
		/* this is probably dead code, but let's be safe: */
		if (AuxProcessResourceOwner)
			ReleaseAuxProcessResources(false);
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(PrefetchMemCxt);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(PrefetchMemCxt);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/* if in shutdown mode, no need for anything further; just go away */
		if (got_SIGTERM)
			goto shutdown;

		/*
		 * Sleep at least 1 second after any error.  We don't want to be
		 * filling the error logs as fast as we can.
		 */
		pg_usleep(1000000L);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/* must unblock signals before calling rebuild_database_list */
	PG_SETMASK(&UnBlockSig);

	/*
	 * In emergency mode, just start a worker (unless shutdown was requested)
	 * and go away.
	 */
	if (!PrefetchActive())
	{
		if (!got_SIGTERM)
			launch_worker(GetCurrentTimestamp());
		proc_exit(0);			/* done */
	}

	PrefetchShmem->prefetch_launcherpid = MyProcPid;

	/* loop until shutdown request */
	while (!got_SIGTERM)
	{
		TimestampTz current_time = 0;
		bool		can_launch;

		/*
		 * Wait until naptime expires or we get some type of signal (all the
		 * signal handlers will wake us by calling SetLatch).
		 */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 1000L, WAIT_EVENT_PREFETCH_MAIN);

		ResetLatch(MyLatch);

		/* Process sinval catchup interrupts that happened while sleeping */
		ProcessCatchupInterrupt();

		/* the normal shutdown case */
		if (got_SIGTERM)
			break;

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);

			/* shutdown requested in config file? */
			if (!PrefetchActive())
				break;
		}

		/*
		 * a worker finished, or postmaster signalled failure to start a
		 * worker
		 */
		if (got_SIGUSR2)
		{
			got_SIGUSR2 = false;

			if (PrefetchShmem->prefetch_signal[PrefetchForkFailed])
			{
				/*
				 * If the postmaster failed to start a new worker, we sleep
				 * for a little while and resend the signal.  The new worker's
				 * state is still in memory, so this is sufficient.  After
				 * that, we restart the main loop.
				 *
				 * XXX should we put a limit to the number of times we retry?
				 * I don't think it makes much sense, because a future start
				 * of a worker will continue to fail in the same way.
				 */
				PrefetchShmem->prefetch_signal[PrefetchForkFailed] = false;
				pg_usleep(1000000L);	/* 1s */
				SendPostmasterSignal(PMSIGNAL_START_PREFETCH_WORKER);
				continue;
			}
		}

		/*
		 * There are some conditions that we need to check before trying to
		 * start a worker.  First, we need to make sure that there is a worker
		 * slot available.  Second, we need to make sure that no other worker
		 * failed while starting up.
		 */

		current_time = GetCurrentTimestamp();
		LWLockAcquire(PrefetchLock, LW_SHARED);

		can_launch = !dlist_is_empty(&PrefetchShmem->prefetch_freeWorkers);

		if (PrefetchShmem->prefetch_startingWorker != NULL)
		{
			int			waittime;
			WorkerInfo	worker = PrefetchShmem->prefetch_startingWorker;

			/*
			 * We can't launch another worker when another one is still
			 * starting up (or failed while doing so), so just sleep for a bit
			 * more; that worker will wake us up again as soon as it's ready.
			 * We will only wait prefetch_naptime seconds (up to a maximum
			 * of 60 seconds) for this to happen however.  Problems detected by
			 * the postmaster (like fork() failure) are also reported and handled
			 * differently.  The only problems that may cause this code to
			 * fire are errors in the earlier sections of PrefetchWorkerMain,
			 * before the worker removes the WorkerInfo from the
			 * startingWorker pointer.
			 */
			waittime = Min(async_prefetch_naptime, 60) * 1000;
			if (TimestampDifferenceExceeds(worker->wi_launchtime, current_time,
										   waittime))
			{
				LWLockRelease(PrefetchLock);
				LWLockAcquire(PrefetchLock, LW_EXCLUSIVE);

				/*
				 * No other process can put a worker in starting mode, so if
				 * startingWorker is still INVALID after exchanging our lock,
				 * we assume it's the same one we saw above (so we don't
				 * recheck the launch time).
				 */
				if (PrefetchShmem->prefetch_startingWorker != NULL)
				{
					worker = PrefetchShmem->prefetch_startingWorker;
					worker->wi_proc = NULL;
					worker->wi_launchtime = 0;
					dlist_push_head(&PrefetchShmem->prefetch_freeWorkers,
									&worker->wi_links);
					PrefetchShmem->prefetch_startingWorker = NULL;
					elog(WARNING, "worker took too long to start; canceled");
				}
			}
			else
				can_launch = false;
		}
		LWLockRelease(PrefetchLock);	/* either shared or exclusive */

		/* if we can't do anything, just go back to sleep */
		if (!can_launch)
			continue;

		/* We're OK to start a new worker */
		launch_worker(current_time);
	}

	/* Normal exit from the prefetch launcher is here */
shutdown:
	ereport(DEBUG1,
			(errmsg("prefetch launcher shutting down")));
	PrefetchShmem->prefetch_launcherpid = 0;

	proc_exit(0);				/* done */
}


/*
 * launch_worker
 *
 * Wrapper for starting a worker from the launcher. 
 */
static void
launch_worker(TimestampTz now)
{
	WorkerInfo	worker;
	dlist_node *wptr;

	/* return quickly when there are no free workers */
	LWLockAcquire(PrefetchLock, LW_SHARED);
	if (dlist_is_empty(&PrefetchShmem->prefetch_freeWorkers))
	{
		LWLockRelease(PrefetchLock);
		return;
	}
	LWLockRelease(PrefetchLock);

	/*
	 * Get a worker entry from the freelist.  We checked above, so there
	 * really should be a free slot.
	 */
	LWLockAcquire(PrefetchLock, LW_EXCLUSIVE);

	wptr = dlist_pop_head_node(&PrefetchShmem->prefetch_freeWorkers);

	worker = dlist_container(WorkerInfoData, wi_links, wptr);
	worker->wi_proc = NULL;
	worker->wi_launchtime = GetCurrentTimestamp();

	PrefetchShmem->prefetch_startingWorker = worker;

	LWLockRelease(PrefetchLock);

	SendPostmasterSignal(PMSIGNAL_START_PREFETCH_WORKER);
}

/*
 * Called from postmaster to signal a failure to fork a process to become
 * worker.  The postmaster should kill(SIGUSR2) the launcher shortly
 * after calling this function.
 */
void
PrefetchWorkerFailed(void)
{
	PrefetchShmem->prefetch_signal[PrefetchForkFailed] = true;
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
prefetch_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGUSR2: a worker is up and running, or just finished, or failed to fork */
static void
prefetch_launcher_sigusr2_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGUSR2 = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGTERM: time to die */
static void
prefetch_launcher_sigterm_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


/********************************************************************
 *					  PREFETCH WORKER CODE
 ********************************************************************/

#ifdef EXEC_BACKEND
/*
 * forkexec routines for the prefetch worker.
 *
 * Format up the arglist, then fork and exec.
 */
static pid_t
prefetch_worker_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkprefetchworker";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}

/*
 * We need this set from the outside, before InitProcess is called
 */
void
PrefetchWorkerIAm(void)
{
	am_prefetch_worker = true;
}
#endif

/*
 * Main entry point for prefetch worker process.
 *
 * This code is heavily based on pgarch.c, q.v.
 */
int
StartPrefetchWorker(void)
{
	pid_t		worker_pid;

#ifdef EXEC_BACKEND
	switch ((worker_pid = prefetch_worker_forkexec()))
#else
	switch ((worker_pid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork prefetch worker process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			PrefetchWorkerMain(0, NULL);
			break;
#endif
		default:
			return (int) worker_pid;
	}

	/* shouldn't get here */
	return 0;
}

/*
 * PrefetchWorkerMain
 */
NON_EXEC_STATIC void
PrefetchWorkerMain(int argc, char *argv[])
{
	sigjmp_buf	local_sigjmp_buf;

	am_prefetch_worker = true;

	/* Identify myself via ps */
	init_ps_display(pgstat_get_backend_desc(B_PREFETCH_WORKER), "", "", "");

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers.  We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 */
	pqsignal(SIGHUP, prefetch_sighup_handler);

	/*
	 * SIGINT is used to signal canceling the current block prefetch; SIGTERM
	 * means abort and exit cleanly, and SIGQUIT means abandon ship.
	 */
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, die);
	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts();		/* establishes SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/* Early initialization */
	BaseInit();

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * We can now go away.  Note that because we called InitProcess, a
		 * callback was registered to do ProcKill, which will clean up
		 * necessary state.
		 */
		proc_exit(0);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	PG_SETMASK(&UnBlockSig);

	/*
	 * Get the info about the database we're going to work on.
	 */
	LWLockAcquire(PrefetchLock, LW_EXCLUSIVE);

	/*
	 * beware of startingWorker being INVALID; this should normally not
	 * happen, but if a worker fails after forking and before this, the
	 * launcher might have decided to remove it from the queue and start
	 * again.
	 */
	if (PrefetchShmem->prefetch_startingWorker != NULL)
	{
		MyWorkerInfo = PrefetchShmem->prefetch_startingWorker;
		MyWorkerInfo->wi_proc = MyProc;

		/* insert into the running list */
		dlist_push_head(&PrefetchShmem->prefetch_runningWorkers,
						&MyWorkerInfo->wi_links);

		/*
		 * remove from the "starting" pointer, so that the launcher can start
		 * a new worker if required
		 */
		PrefetchShmem->prefetch_startingWorker = NULL;
		LWLockRelease(PrefetchLock);

		on_shmem_exit(FreeWorkerInfo, 0);

		/* wake up the launcher */
		if (PrefetchShmem->prefetch_launcherpid != 0)
			kill(PrefetchShmem->prefetch_launcherpid, SIGUSR2);
	}
	else
	{
		/* no worker entry for me, go away */
		elog(WARNING, "prefetch worker started without a worker entry");
		LWLockRelease(PrefetchLock);
	}

	/* do the actual work */
	do_prefetch();

	/*
	 * The launcher will be notified of my death in ProcKill, *if* we managed
	 * to get a worker slot at all
	 */

	/* All done, go away */
	proc_exit(0);
}

/*
 * Return a WorkerInfo to the free list
 */
static void
FreeWorkerInfo(int code, Datum arg)
{
	if (MyWorkerInfo != NULL)
	{
		LWLockAcquire(PrefetchLock, LW_EXCLUSIVE);

		/*
		 * Wake the launcher up so that he can launch a new worker immediately
		 * if required.  We only save the launcher's PID in local memory here;
		 * the actual signal will be sent when the PGPROC is recycled.  Note
		 * that we always do this, so that the launcher can rebalance the cost
		 * limit setting of the remaining workers.
		 *
		 * We somewhat ignore the risk that the launcher changes its PID
		 * between us reading it and the actual kill; we expect ProcKill to be
		 * called shortly after us, and we assume that PIDs are not reused too
		 * quickly after a process exits.
		 */
		PrefetchLauncherPid = PrefetchShmem->prefetch_launcherpid;

		dlist_delete(&MyWorkerInfo->wi_links);
		MyWorkerInfo->wi_proc = NULL;
		MyWorkerInfo->wi_launchtime = 0;
		dlist_push_head(&PrefetchShmem->prefetch_freeWorkers,
						&MyWorkerInfo->wi_links);
		/* not mine anymore */
		MyWorkerInfo = NULL;

		/*
		 * now that we're inactive, cause a rebalancing of the surviving
		 * workers
		 */
		LWLockRelease(PrefetchLock);
	}
}

/*
 * The workers don't grab requests one by one, but in small chunks, to
 * reduce lock contention. We must not use too large chunks though, as
 * it changes ordering of the requests.
 */
#define MAX_PREFETCH_WORKER_REQUESTS	8

/*
 * TODO do the actual prefetching
 */
static void
do_prefetch(void)
{
	/*
	 * get a bunch of prefetch requests
	 */
	for (;;)
	{
		int			nrequests = 0,
					nerrors = 0,
					start,
					count;
		BufferTag	requests[MAX_PREFETCH_WORKER_REQUESTS];

		CHECK_FOR_INTERRUPTS();

		ConditionVariablePrepareToSleep(&PrefetchShmem->requests_cv);
		for (;;)
		{
			SpinLockAcquire(&PrefetchShmem->prefetch_queue_lck);

			/* shorter variables for convenience */
			start = PrefetchShmem->prefetch_queue_start;
			count = PrefetchShmem->prefetch_queue_count;

			/* XXX Maybe do a memcpy instead? */
			while ((nrequests < MAX_PREFETCH_WORKER_REQUESTS) && (count > 0))
			{
				requests[nrequests++] = PrefetchShmem->prefetch_queue[start];

				start = (start + 1) % QUEUE_SIZE;
				count--;
			}

			/* store modified values */
			PrefetchShmem->prefetch_queue_start = start;
			PrefetchShmem->prefetch_queue_count = count;

			SpinLockRelease(&PrefetchShmem->prefetch_queue_lck);

			if (nrequests > 0)
				break;

			/* otherwise sleep for a while and wait for new requests */
			ConditionVariableSleep(&PrefetchShmem->requests_cv,
								   WAIT_EVENT_PREFETCH_IDLE);
		}
		ConditionVariableCancelSleep();

		/*
		 * If we got requests, notify others there's free space in the queue
		 * now, and do the actual prefetching.
		 */
		if (nrequests > 0)
		{
			int i;
			int		nprefetched = 0,
					nskipped = 0;

			/* XXX maybe do ConditionVariableSignal instead? */
			ConditionVariableBroadcast(&PrefetchShmem->free_cv);

			for (i = 0; i < nrequests; i++)
			{
				BufferTag	tag;
				bool		prefetch = true;

				/* for convenience */
				tag = requests[i];

				/* optionally inspect shared buffers */
				if (async_prefetch_buffers)
				{
					uint32		hash;
					LWLock	   *partitionLock;
					int			buf_id;

					/* determine its hash code and partition lock ID */
					hash = BufTableHashCode(&tag);
					partitionLock = BufMappingPartitionLock(hash);

					/* see if the block is in the buffer pool already */
					LWLockAcquire(partitionLock, LW_SHARED);
					buf_id = BufTableLookup(&tag, hash);
					LWLockRelease(partitionLock);

					/* prefetch only if not already in shared buffers */
					prefetch = (buf_id < 0);
				}

				/* initiate prefetch */
				if (prefetch)
				{
					SMgrRelation	reln = smgropen(tag.rnode, InvalidBackendId);

					smgrprefetch(reln, tag.forkNum, tag.blockNum);
					nprefetched++;
				}
				else
					nskipped++;
			}

			SpinLockAcquire(&PrefetchShmem->prefetch_stats_lck);

			PrefetchShmem->prefetch_stats_nprocessed += nprefetched;
			PrefetchShmem->prefetch_stats_nskipped += nskipped;
			PrefetchShmem->prefetch_stats_nerrors += nerrors;

			SpinLockRelease(&PrefetchShmem->prefetch_stats_lck);
		}
	}
}

/*
 * PrefetchActive
 *		Check GUC vars and report whether the prefetch should be running.
 */
bool
PrefetchActive(void)
{
	return (async_prefetch_workers > 0);
}

/*
 * IsPrefetch functions
 *		Return whether this is either a launcher process or a worker process.
 */
bool
IsPrefetchLauncherProcess(void)
{
	return am_prefetch_launcher;
}

bool
IsPrefetchWorkerProcess(void)
{
	return am_prefetch_worker;
}


/*
 * PrefetchShmemSize
 *		Compute space needed for prefetch-related shared memory
 */
Size
PrefetchShmemSize(void)
{
	Size		size;

	/*
	 * Need the fixed struct and the array of WorkerInfoData.
	 */
	size = sizeof(PrefetchShmemStruct);
	size = MAXALIGN(size);
	size = add_size(size, mul_size(async_prefetch_workers,
								   sizeof(WorkerInfoData)));
	return size;
}

/*
 * PrefetchShmemInit
 *		Allocate and initialize prefetch-related shared memory
 */
void
PrefetchShmemInit(void)
{
	bool		found;

	PrefetchShmem
		= (PrefetchShmemStruct *) ShmemInitStruct("Prefetch Data",
												  PrefetchShmemSize(),
												  &found);

	if (!IsUnderPostmaster)
	{
		WorkerInfo	worker;
		int			i;

		Assert(!found);

		/* generic child process fields */
		PrefetchShmem->prefetch_launcherpid = 0;
		dlist_init(&PrefetchShmem->prefetch_freeWorkers);
		dlist_init(&PrefetchShmem->prefetch_runningWorkers);
		PrefetchShmem->prefetch_startingWorker = NULL;

		/* queue of prefetch requests */
		ConditionVariableInit(&PrefetchShmem->requests_cv);
		ConditionVariableInit(&PrefetchShmem->free_cv);

		SpinLockInit(&PrefetchShmem->prefetch_queue_lck);
		SpinLockInit(&PrefetchShmem->prefetch_stats_lck);

		/* (count == 0) means there are no requests */
		PrefetchShmem->prefetch_queue_start = 0;
		PrefetchShmem->prefetch_queue_count = 0;

		memset(PrefetchShmem->prefetch_queue, 0,
			   sizeof(BufferTag) * QUEUE_SIZE);

		worker = (WorkerInfo) ((char *) PrefetchShmem +
							   MAXALIGN(sizeof(PrefetchShmemStruct)));

		/* initialize the WorkerInfo free list */
		for (i = 0; i < async_prefetch_workers; i++)
			dlist_push_head(&PrefetchShmem->prefetch_freeWorkers,
							&worker[i].wi_links);
	}
	else
		Assert(found);
}

static int
SubmitPrefetchRequests(int nrequests, BufferTag *requests, bool nowait)
{
	int	nsubmitted = 0,
		nqueuefull = 0;

	/*
	 * Submit the requests.
	 */
	ConditionVariablePrepareToSleep(&PrefetchShmem->free_cv);
	for (;;)
	{
		int			start,
					count;

		SpinLockAcquire(&PrefetchShmem->prefetch_queue_lck);

		start = PrefetchShmem->prefetch_queue_start;
		count = PrefetchShmem->prefetch_queue_count;

		while ((count < QUEUE_SIZE) && (nsubmitted < nrequests))
		{
			int qidx = (start + count) % QUEUE_SIZE;

			PrefetchShmem->prefetch_queue[qidx] = requests[nsubmitted++];
			count++;
		}

		PrefetchShmem->prefetch_queue_count = count;

		SpinLockRelease(&PrefetchShmem->prefetch_queue_lck);

		if (nsubmitted < nrequests)
			nqueuefull++;

		/*
		 * Bail of if we've submitted all the requests, of when called
		 * with (nowait = true).
		 */
		if ((nsubmitted == nrequests) || nowait)
			break;

		/* otherwise sleep for a while */
		ConditionVariableSleep(&PrefetchShmem->free_cv,
							   WAIT_EVENT_PREFETCH_QUEUE);
	}
	ConditionVariableCancelSleep();

	/* notify we have submitted new requests */
	/* XXX maybe do ConditionVariableSignal instead? */
	if (nsubmitted > 0)
		ConditionVariableBroadcast(&PrefetchShmem->requests_cv);

	/* update stats */
	SpinLockAcquire(&PrefetchShmem->prefetch_stats_lck);

	PrefetchShmem->prefetch_stats_nsubmitted += nsubmitted;
	PrefetchShmem->prefetch_stats_nfull += nqueuefull;

	SpinLockRelease(&PrefetchShmem->prefetch_stats_lck);

	return nsubmitted;
}

void
PrefetchQueueInit(struct PrefetchQueue *requests)
{
	Assert(requests);

	requests->nrequests = 0;
}

void
PrefetchQueueAdd(PrefetchQueue *requests, RelFileNode rnode,
				 ForkNumber forkNum, BlockNumber blockNum)
{
	Assert(requests);
	Assert(requests->nrequests < MAX_PREFETCH_REQUESTS);

	requests->requests[requests->nrequests].rnode = rnode;
	requests->requests[requests->nrequests].forkNum = forkNum;
	requests->requests[requests->nrequests].blockNum = blockNum;

	requests->nrequests++;

	/* when the local queue is full, send it to prefetcher */
	if (requests->nrequests == MAX_PREFETCH_REQUESTS)
		PrefetchQueueFlush(requests);
}

void
PrefetchQueueFlush(PrefetchQueue *requests)
{
	Assert(requests);

	if (requests->nrequests == 0)
		return;

	/* send the requests with nowait=true, so that we don't get blocked
	 * in case the prefetcher workers fail, get stuck or something */
	SubmitPrefetchRequests(requests->nrequests, requests->requests, true);
}

Datum
pg_stat_get_prefetch_submitted(PG_FUNCTION_ARGS)
{
	int64	r;

	/* update stats */
	SpinLockAcquire(&PrefetchShmem->prefetch_stats_lck);

	r = PrefetchShmem->prefetch_stats_nsubmitted;

	SpinLockRelease(&PrefetchShmem->prefetch_stats_lck);

	PG_RETURN_INT64(r);
}

Datum
pg_stat_get_prefetch_processed(PG_FUNCTION_ARGS)
{
	int64	r;

	/* update stats */
	SpinLockAcquire(&PrefetchShmem->prefetch_stats_lck);

	r = PrefetchShmem->prefetch_stats_nprocessed;

	SpinLockRelease(&PrefetchShmem->prefetch_stats_lck);

	PG_RETURN_INT64(r);
}

Datum
pg_stat_get_prefetch_skipped(PG_FUNCTION_ARGS)
{
	int64	r;

	/* update stats */
	SpinLockAcquire(&PrefetchShmem->prefetch_stats_lck);

	r = PrefetchShmem->prefetch_stats_nskipped;

	SpinLockRelease(&PrefetchShmem->prefetch_stats_lck);

	PG_RETURN_INT64(r);
}

Datum
pg_stat_get_prefetch_failed(PG_FUNCTION_ARGS)
{
	int64	r;

	/* update stats */
	SpinLockAcquire(&PrefetchShmem->prefetch_stats_lck);

	r = PrefetchShmem->prefetch_stats_nerrors;

	SpinLockRelease(&PrefetchShmem->prefetch_stats_lck);

	PG_RETURN_INT64(r);
}

Datum
pg_stat_get_prefetch_queue_full(PG_FUNCTION_ARGS)
{
	int64	r;

	/* update stats */
	SpinLockAcquire(&PrefetchShmem->prefetch_stats_lck);

	r = PrefetchShmem->prefetch_stats_nfull;

	SpinLockRelease(&PrefetchShmem->prefetch_stats_lck);

	PG_RETURN_INT64(r);
}

Datum
pg_stat_reset_prefetch_stats(PG_FUNCTION_ARGS)
{
	/* update stats */
	SpinLockAcquire(&PrefetchShmem->prefetch_stats_lck);

	PrefetchShmem->prefetch_stats_nsubmitted = 0;
	PrefetchShmem->prefetch_stats_nprocessed = 0;
	PrefetchShmem->prefetch_stats_nskipped = 0;
	PrefetchShmem->prefetch_stats_nerrors = 0;
	PrefetchShmem->prefetch_stats_nfull = 0;

	SpinLockRelease(&PrefetchShmem->prefetch_stats_lck);

	PG_RETURN_VOID();
}
