/*-------------------------------------------------------------------------
 *
 * walprefetcher.c
 *
 * Replaying WAL is done by single process, it may cause slow recovery time
 * cause lag between master and replica.
 *
 * Prefetcher trieds to preload in OS file cache blocks, referenced by WAL 
 * records to speedup recovery
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/walprefetcher.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "access/heapam_xlog.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/walprefetcher.h"
#include "replication/walreceiver.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/buf_internals.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/memutils.h"

#define KB (1024LL)
/* #define DEBUG_PREFETCH 1 */

#if DEBUG_PREFETCH
#define LOG_LEVEL LOG
#else
#define LOG_LEVEL DEBUG1
#endif

/*
 * GUC parameters
 */
int			WalPrefetchMinLead = 0;
int			WalPrefetchMaxLead = 0;
int			WalPrefetchPollInterval = 1000;
bool 		WalPrefetchEnabled = false;

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t shutdown_requested = false;

/* Signal handlers */
static void WpfQuickDie(SIGNAL_ARGS);
static void WpfSigHupHandler(SIGNAL_ARGS);
static void WpfShutdownHandler(SIGNAL_ARGS);
static void WpfSigusr1Handler(SIGNAL_ARGS);

/*
 * Main entry point for walprefetcher background worker
 */
void
WalPrefetcherMain()
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext walprefetcher_context;
	int rc;

	pqsignal(SIGHUP, WpfSigHupHandler); /* set flag to read config file */
	pqsignal(SIGINT, WpfShutdownHandler);	/* request shutdown */
	pqsignal(SIGTERM, WpfShutdownHandler);	/* request shutdown */
	pqsignal(SIGQUIT, WpfQuickDie);	/* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, WpfSigusr1Handler);
	pqsignal(SIGUSR2, SIG_IGN); /* not used */

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	walprefetcher_context = AllocSetContextCreate(TopMemoryContext,
											  "Wal Prefetcher",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(walprefetcher_context);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * This code is heavily based on bgwriter.c, q.v.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		pgstat_report_wait_end();
		AtEOXact_Files(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(walprefetcher_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(walprefetcher_context);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Loop forever
	 */
	for (;;)
	{
		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		/*
		 * Process any requests or signals received recently.
		 */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		if (shutdown_requested)
		{
			/* Normal exit from the walprefetcher is here */
			proc_exit(0);		/* done */
		}

		if (WalPrefetchEnabled)
			WalPrefetch(InvalidXLogRecPtr);

		/*
		 * Sleep until we are signaled
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH,
					   -1,
					   WAIT_EVENT_WAL_PREFETCHER_MAIN);

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			exit(1);
	}
}


/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */

/*
 * WpfQuickDie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
static void
WpfQuickDie(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);

	/*
	 * We DO NOT want to run proc_exit() callbacks -- we're here because
	 * shared memory may be corrupted, so we don't want to try to clean up our
	 * transaction.  Just nail the windows shut and get out of town.  Now that
	 * there's an atexit callback to prevent third-party code from breaking
	 * things by calling exit() directly, we have to reset the callbacks
	 * explicitly to make this work as intended.
	 */
	on_exit_reset();

	/*
	 * Note we do exit(2) not exit(0).  This is to force the postmaster into a
	 * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	exit(2);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
WpfSigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
	SetLatch(MyLatch);
}

/* SIGTERM: set flag to exit normally */
static void
WpfShutdownHandler(SIGNAL_ARGS)
{
	shutdown_requested = true;
	SetLatch(MyLatch);
}

/* SIGUSR1: used for latch wakeups */
static void
WpfSigusr1Handler(SIGNAL_ARGS)
{
	latch_sigusr1_handler();
}

/*
 * Now wal prefetch code itself. 
 */
static int
WalReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr,
			int reqLen, XLogRecPtr targetRecPtr, char *cur_page,
			TimeLineID *pageTLI);

#define FILE_HASH_SIZE  1009     /* Size of opened files hash */
#define STAT_REFRESH_PERIOD 1024 /* Refresh backend status rate */

/*
 * Block LRU hash table is used to keep information about most recently prefetched blocks.
 */
typedef struct BlockHashEntry
{
	struct BlockHashEntry* next;
	struct BlockHashEntry* prev;
	struct BlockHashEntry* collision;
	BufferTag tag;
	uint32 hash;
} BlockHashEntry;

static BlockHashEntry** block_hash_table;
static size_t block_hash_size;
static size_t block_hash_used;
static BlockHashEntry lru = {&lru, &lru};
static TimeLineID replay_timeline;

/*
 * Yet another L2-list implementation
 */
static void
unlink_block(BlockHashEntry* entry)
{
	entry->next->prev = entry->prev;
	entry->prev->next = entry->next;
}

static void
link_block_after(BlockHashEntry* head, BlockHashEntry* entry)
{
	entry->next = head->next;
	entry->prev = head;
	head->next->prev = entry;
	head->next = entry;
}

/*
 * Put block in LRU hash or link it to the head of LRU list. Returns true if block was not present in hash, false otherwise.
 */
static bool
put_block_in_cache(BufferTag* tag)
{
	uint32 hash;
	BlockHashEntry* entry;

	hash = BufTableHashCode(tag) % block_hash_size;
	for (entry = block_hash_table[hash]; entry != NULL; entry = entry->collision)
	{
		if (BUFFERTAGS_EQUAL(entry->tag, *tag))
		{
			unlink_block(entry);
			link_block_after(&lru, entry);
			return false;
		}
	}
	if (block_hash_size == block_hash_used)
	{
		BlockHashEntry* victim = lru.prev;
		BlockHashEntry** epp = &block_hash_table[victim->hash];
		while (*epp != victim)
			epp = &(*epp)->collision;
		*epp = (*epp)->collision;
		unlink_block(victim);
		entry = victim;
	}
	else
	{
		entry = (BlockHashEntry*)palloc(sizeof(BlockHashEntry));
		block_hash_used += 1;
	}
	entry->tag = *tag;
	entry->hash = hash;
	entry->collision = block_hash_table[hash];
	block_hash_table[hash] = entry;
	link_block_after(&lru, entry);

	return true;
}

/*
 * Hash of of opened files. It seems to be simpler to maintain own cache rather than provide SMgrRelation for smgr functions.
 */
typedef struct FileHashEntry
{
    BufferTag tag;
	File file;
} FileHashEntry;

static FileHashEntry file_hash_table[FILE_HASH_SIZE];

static File
WalOpenFile(BufferTag* tag)
{
	BufferTag segment_tag = *tag;
	uint32 hash;
	char* path;
	File file;

	/* Transform block number into segment number */
	segment_tag.blockNum /= RELSEG_SIZE;
	hash = BufTableHashCode(&segment_tag) % FILE_HASH_SIZE;

	if (BUFFERTAGS_EQUAL(file_hash_table[hash].tag, segment_tag))
		return file_hash_table[hash].file;

	path = relpathperm(tag->rnode, tag->forkNum);
	if (segment_tag.blockNum > 0)
	{
		char* fullpath = psprintf("%s.%d", path, segment_tag.blockNum);
		pfree(path);
		path = fullpath;
	}
	file = PathNameOpenFile(path, O_RDONLY | PG_BINARY);

	if (file >= 0)
	{
		elog(LOG_LEVEL, "WAL_PREFETCH: open file %s", path);
		if (file_hash_table[hash].tag.rnode.dbNode != 0)
			FileClose(file_hash_table[hash].file);

		file_hash_table[hash].file = file;
		file_hash_table[hash].tag = segment_tag;
	}
	pfree(path);
	return file;
}

/*
 * Our backend doesn't receive any notifications about WAL progress, so we have to use sleep
 * to wait until requested information is available
 */
static void
WalWaitWAL(void)
{
	int rc;
	CHECK_FOR_INTERRUPTS();
	rc = WaitLatch(MyLatch,
				   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
				   WalPrefetchPollInterval,
				   WAIT_EVENT_WAL_PREFETCHER_MAIN);
	/*
	 * Emergency bailout if postmaster has died.  This is to avoid the
	 * necessity for manual cleanup of all postmaster children.
	 */
	if (rc & WL_POSTMASTER_DEATH)
		exit(1);

}

/*
 * Main function: perform prefetch of blocks referenced by WAL records starting from given LSN or from WAL replay position if lsn=0
 */
void
WalPrefetch(XLogRecPtr lsn)
{
	XLogReaderState *xlogreader;
	long n_prefetched = 0;
	long n_fpw = 0;
	long n_cached= 0;
	long n_initialized = 0;

	/* Dirty hack: prevent recovery conflict */
	MyPgXact->xmin = InvalidTransactionId;

	memset(file_hash_table, 0, sizeof file_hash_table);

	free(block_hash_table);
	block_hash_size = effective_cache_size;
	block_hash_table = (BlockHashEntry**)calloc(block_hash_size, sizeof(BlockHashEntry*));
	block_hash_used = 0;

	xlogreader = XLogReaderAllocate(wal_segment_size, &WalReadPage, NULL);

	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	if (lsn == InvalidXLogRecPtr)
		lsn = GetXLogReplayRecPtr(NULL); /* Start with replay LSN */

	while (!shutdown_requested)
	{
		char *errormsg;
		int	block_id;
		XLogRecPtr replay_lsn = GetXLogReplayRecPtr(&replay_timeline);
		XLogRecord *record;

		/*
		 * If current position is behind current replay LSN, then move it forward: we do not want to perform useless job and prefetch
		 * blocks for already processed WAL records
		 */
		if (lsn != InvalidXLogRecPtr || replay_lsn + WalPrefetchMinLead*KB >= xlogreader->EndRecPtr)
		{
			XLogRecPtr prefetch_lsn = replay_lsn != InvalidXLogRecPtr
				? XLogFindNextRecord(xlogreader, Max(lsn, replay_lsn) + WalPrefetchMinLead*KB) : InvalidXLogRecPtr;
			if (prefetch_lsn == InvalidXLogRecPtr)
			{
				elog(LOG_LEVEL, "WAL_PREFETCH: wait for new WAL records at LSN %llx: replay lsn %llx, prefetched %ld, cached %ld, fpw %ld, initialized %ld",
					 (long long)xlogreader->EndRecPtr, (long long)replay_lsn, n_prefetched, n_cached, n_fpw, n_initialized);
				WalWaitWAL();
				continue;
			}
			lsn = prefetch_lsn;
		}
		/*
		 * Now opposite check: if prefetch goes too far from replay position, then suspend it for a while
		 */
		if (WalPrefetchMaxLead != 0 && replay_lsn + WalPrefetchMaxLead*KB < xlogreader->EndRecPtr)
		{
			elog(LOG_LEVEL, "WAL_PREFETCH: wait for recovery at LSN %llx, replay LSN %llx",
				 (long long)xlogreader->EndRecPtr, (long long)replay_lsn);
			WalWaitWAL();
			continue;
		}

		record = XLogReadRecord(xlogreader, lsn, &errormsg);

		if (record != NULL)
		{
			lsn = InvalidXLogRecPtr; /* continue with next record */

			/* Loop through blocks referenced by this WAL record */
			for (block_id = 0; block_id <= xlogreader->max_block_id; block_id++)
			{
				BufferTag tag;
				File      file;

				if (!XLogRecGetBlockTag(xlogreader, block_id, &tag.rnode, &tag.forkNum, &tag.blockNum))
					continue;

				/* Check if block already prefetched */
				if (!put_block_in_cache(&tag))
					continue;

				/* Check if block is cached in shared buffers */
				if (IsBlockCached(&tag))
				{
					n_cached += 1;
					continue;
				}

				/* Do not prefetch full pages */
				if (XLogRecHasBlockImage(xlogreader, block_id))
				{
					n_fpw += 1;
					continue;
				}

				/* Ignore initialized pages */
				if (XLogRecGetRmid(xlogreader) == RM_HEAP_ID
					&& (XLogRecGetInfo(xlogreader) & XLOG_HEAP_INIT_PAGE))
				{
					n_initialized += 1;
					continue;
				}

				file = WalOpenFile(&tag);
				if (file >= 0)
				{
					off_t offs = (off_t) BLCKSZ * (tag.blockNum % ((BlockNumber) RELSEG_SIZE));
					int rc;
#if DEBUG_PREFETCH
					instr_time start, stop;
					INSTR_TIME_SET_CURRENT(start);
#endif
					rc = FilePrefetch(file, offs, BLCKSZ, WAIT_EVENT_DATA_FILE_PREFETCH);
					if (rc != 0)
						elog(ERROR, "WAL_PREFETCH: failed to prefetch file: %m");
					else if (++n_prefetched % STAT_REFRESH_PERIOD == 0)
					{
						char buf[1024];
						sprintf(buf, "Prefetch %ld blocks at LSN %llx, replay LSN %llx",
								n_prefetched, (long long)xlogreader->ReadRecPtr, (long long)replay_lsn);
						pgstat_report_activity(STATE_RUNNING, buf);
						elog(DEBUG1, "%s", buf);
					}
#if DEBUG_PREFETCH
					INSTR_TIME_SET_CURRENT(stop);
					INSTR_TIME_SUBTRACT(stop,start);
					elog(LOG, "WAL_PREFETCH: %x/%x prefetch block %d fork %d of relation %d at LSN %llx, replay LSN %llx (%u usec), %ld prefetched, %ld cached, %ld fpw, %ld initialized",
						 XLogRecGetRmid(xlogreader), XLogRecGetInfo(xlogreader),
						 tag.blockNum, tag.forkNum, tag.rnode.relNode, (long long)xlogreader->ReadRecPtr, (long long)replay_lsn,
						 (int)INSTR_TIME_GET_MICROSEC(stop), n_prefetched, n_cached, n_fpw, n_initialized);
#endif
				}
				else
					elog(LOG, "WAL_PREFETCH: file segment doesn't exists");
			}
		}
		else
		{
			elog(LOG, "WAL_PREFETCH: wait for valid record at LSN %llx, replay_lsn %llx: %s",
				 (long long)xlogreader->EndRecPtr, (long long)replay_lsn, errormsg);
			WalWaitWAL();
		}
	}
}

/*
 * Almost copy of read_local_xlog_page from xlogutils.c, but it reads until flush position of WAL receiver, rather then replay position.
 */
static int
WalReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr,
			int reqLen, XLogRecPtr targetRecPtr, char *cur_page,
			TimeLineID *pageTLI)
{
	XLogRecPtr	read_upto,
				loc;
	int			count;

	loc = targetPagePtr + reqLen;

	/* Loop waiting for xlog to be available if necessary */
	while (1)
	{
		/*
		 * If we perform recovery at startup then read until end of WAL,
		 * otherwise if there is active WAL receiver at replica, read until the end of received data,
		 * if there is no active wal recevier, then just sleep.
		 */
		read_upto =	WalRcv->walRcvState == WALRCV_STOPPED
			? RecoveryInProgress() ? (XLogRecPtr)-1 : InvalidXLogRecPtr
			: WalRcv->receivedUpto;
		*pageTLI = replay_timeline;

		if (loc <= read_upto)
			break;

		elog(LOG_LEVEL, "WAL_PREFETCH: wait for new WAL records at LSN %llx, read up to lsn %llx",
			 (long long)loc, (long long)read_upto);
		WalWaitWAL();
		CHECK_FOR_INTERRUPTS();
		if (shutdown_requested)
			return -1;
	}

	if (targetPagePtr + XLOG_BLCKSZ <= read_upto)
	{
		/*
		 * more than one block available; read only that block, have caller
		 * come back if they need more.
		 */
		count = XLOG_BLCKSZ;
	}
	else if (targetPagePtr + reqLen > read_upto)
	{
		/* not enough data there */
		return -1;
	}
	else
	{
		/* enough bytes available to satisfy the request */
		count = read_upto - targetPagePtr;
	}

	/*
	 * Even though we just determined how much of the page can be validly read
	 * as 'count', read the whole page anyway. It's guaranteed to be
	 * zero-padded up to the page boundary if it's incomplete.
	 */
	XLogRead(cur_page, state->wal_segment_size, *pageTLI, targetPagePtr, XLOG_BLCKSZ);


	/* number of valid bytes in the buffer */
	return count;
}
