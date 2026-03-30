/* -------------------------------------------------------------------------
 *
 * pgstat_io.c
 *	  Implementation of IO statistics.
 *
 * This file contains the implementation of IO statistics. It is kept separate
 * from pgstat.c to enforce the line between the statistics access / storage
 * implementation and the details about individual types of statistics.
 *
 * Copyright (c) 2021-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_io.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/instrument.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"

static PgStat_PendingIO PendingIOStats;
static bool have_iostats = false;

PgStat_IOWorkerStats PendingIOWorkerStats = {0};

/*
 * Check that stats have not been counted for any combination of IOObject,
 * IOContext, and IOOp which are not tracked for the passed-in BackendType. If
 * stats are tracked for this combination and IO times are non-zero, counts
 * should be non-zero.
 *
 * The passed-in PgStat_BktypeIO must contain stats from the BackendType
 * specified by the second parameter. Caller is responsible for locking the
 * passed-in PgStat_BktypeIO, if needed.
 */
bool
pgstat_bktype_io_stats_valid(PgStat_BktypeIO *backend_io,
							 BackendType bktype)
{
	for (int io_object = 0; io_object < IOOBJECT_NUM_TYPES; io_object++)
	{
		for (int io_context = 0; io_context < IOCONTEXT_NUM_TYPES; io_context++)
		{
			for (int io_op = 0; io_op < IOOP_NUM_TYPES; io_op++)
			{
				/* we do track it */
				if (pgstat_tracks_io_op(bktype, io_object, io_context, io_op))
				{
					/* ensure that if IO times are non-zero, counts are > 0 */
					if (backend_io->times[io_object][io_context][io_op] != 0 &&
						backend_io->counts[io_object][io_context][io_op] <= 0)
						return false;

					continue;
				}

				/* we don't track it, and it is not 0 */
				if (backend_io->counts[io_object][io_context][io_op] != 0)
					return false;
			}
		}
	}

	return true;
}

void
pgstat_count_io_op(IOObject io_object, IOContext io_context, IOOp io_op,
				   uint32 cnt, uint64 bytes)
{
	Assert((unsigned int) io_object < IOOBJECT_NUM_TYPES);
	Assert((unsigned int) io_context < IOCONTEXT_NUM_TYPES);
	Assert(pgstat_is_ioop_tracked_in_bytes(io_op) || bytes == 0);
	Assert(pgstat_tracks_io_op(MyBackendType, io_object, io_context, io_op));

	PendingIOStats.counts[io_object][io_context][io_op] += cnt;
	PendingIOStats.bytes[io_object][io_context][io_op] += bytes;

	/* Add the per-backend counts */
	pgstat_count_backend_io_op(io_object, io_context, io_op, cnt, bytes);

	have_iostats = true;
	pgstat_report_fixed = true;
}

/*
 * Initialize the internal timing for an IO operation, depending on an
 * IO timing GUC.
 */
instr_time
pgstat_prepare_io_time(bool track_io_guc)
{
	instr_time	io_start;

	if (track_io_guc)
		INSTR_TIME_SET_CURRENT(io_start);
	else
	{
		/*
		 * There is no need to set io_start when an IO timing GUC is disabled.
		 * Initialize it to zero to avoid compiler warnings and to let
		 * pgstat_count_io_op_time() know that timings should be ignored.
		 */
		INSTR_TIME_SET_ZERO(io_start);
	}

	return io_start;
}

/*
 * Like pgstat_count_io_op() except it also accumulates time.
 *
 * The calls related to pgstat_count_buffer_*() are for pgstat_database.  As
 * pg_stat_database only counts block read and write times, these are done for
 * IOOP_READ, IOOP_WRITE and IOOP_EXTEND.
 *
 * pgBufferUsage is used for EXPLAIN.  pgBufferUsage has write and read stats
 * for shared, local and temporary blocks.  pg_stat_io does not track the
 * activity of temporary blocks, so these are ignored here.
 */
void
pgstat_count_io_op_time(IOObject io_object, IOContext io_context, IOOp io_op,
						instr_time start_time, uint32 cnt, uint64 bytes)
{
	if (!INSTR_TIME_IS_ZERO(start_time))
	{
		instr_time	io_time;

		INSTR_TIME_SET_CURRENT(io_time);
		INSTR_TIME_SUBTRACT(io_time, start_time);

		if (io_object != IOOBJECT_WAL)
		{
			if (io_op == IOOP_WRITE || io_op == IOOP_EXTEND)
			{
				pgstat_count_buffer_write_time(INSTR_TIME_GET_MICROSEC(io_time));
				if (io_object == IOOBJECT_RELATION)
					INSTR_TIME_ADD(pgBufferUsage.shared_blk_write_time, io_time);
				else if (io_object == IOOBJECT_TEMP_RELATION)
					INSTR_TIME_ADD(pgBufferUsage.local_blk_write_time, io_time);
			}
			else if (io_op == IOOP_READ)
			{
				pgstat_count_buffer_read_time(INSTR_TIME_GET_MICROSEC(io_time));
				if (io_object == IOOBJECT_RELATION)
					INSTR_TIME_ADD(pgBufferUsage.shared_blk_read_time, io_time);
				else if (io_object == IOOBJECT_TEMP_RELATION)
					INSTR_TIME_ADD(pgBufferUsage.local_blk_read_time, io_time);
			}
		}

		INSTR_TIME_ADD(PendingIOStats.pending_times[io_object][io_context][io_op],
					   io_time);

		/* Add the per-backend count */
		pgstat_count_backend_io_op_time(io_object, io_context, io_op,
										io_time);
	}

	pgstat_count_io_op(io_object, io_context, io_op, cnt, bytes);
}

PgStat_IO *
pgstat_fetch_stat_io(void)
{
	pgstat_snapshot_fixed(PGSTAT_KIND_IO);

	return &pgStatLocal.snapshot.io;
}

/*
 * Simpler wrapper of pgstat_io_flush_cb()
 */
void
pgstat_flush_io(bool nowait)
{
	(void) pgstat_io_flush_cb(nowait);
}

/*
 * Flush out locally pending IO statistics
 *
 * If no stats have been recorded, this function returns false.
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise, return false.
 */
bool
pgstat_io_flush_cb(bool nowait)
{
	LWLock	   *bktype_lock;
	PgStat_BktypeIO *bktype_shstats;

	if (!have_iostats)
		return false;

	bktype_lock = &pgStatLocal.shmem->io.locks[MyBackendType];
	bktype_shstats =
		&pgStatLocal.shmem->io.stats.stats[MyBackendType];

	if (!nowait)
		LWLockAcquire(bktype_lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(bktype_lock, LW_EXCLUSIVE))
		return true;

	for (int io_object = 0; io_object < IOOBJECT_NUM_TYPES; io_object++)
	{
		for (int io_context = 0; io_context < IOCONTEXT_NUM_TYPES; io_context++)
		{
			for (int io_op = 0; io_op < IOOP_NUM_TYPES; io_op++)
			{
				instr_time	time;

				bktype_shstats->counts[io_object][io_context][io_op] +=
					PendingIOStats.counts[io_object][io_context][io_op];

				bktype_shstats->bytes[io_object][io_context][io_op] +=
					PendingIOStats.bytes[io_object][io_context][io_op];

				time = PendingIOStats.pending_times[io_object][io_context][io_op];

				bktype_shstats->times[io_object][io_context][io_op] +=
					INSTR_TIME_GET_MICROSEC(time);
			}
		}
	}

	Assert(pgstat_bktype_io_stats_valid(bktype_shstats, MyBackendType));

	LWLockRelease(bktype_lock);

	memset(&PendingIOStats, 0, sizeof(PendingIOStats));

	have_iostats = false;

	return false;
}

const char *
pgstat_get_io_context_name(IOContext io_context)
{
	switch (io_context)
	{
		case IOCONTEXT_BULKREAD:
			return "bulkread";
		case IOCONTEXT_BULKWRITE:
			return "bulkwrite";
		case IOCONTEXT_INIT:
			return "init";
		case IOCONTEXT_NORMAL:
			return "normal";
		case IOCONTEXT_VACUUM:
			return "vacuum";
	}

	elog(ERROR, "unrecognized IOContext value: %d", io_context);
	pg_unreachable();
}

const char *
pgstat_get_io_object_name(IOObject io_object)
{
	switch (io_object)
	{
		case IOOBJECT_RELATION:
			return "relation";
		case IOOBJECT_TEMP_RELATION:
			return "temp relation";
		case IOOBJECT_WAL:
			return "wal";
	}

	elog(ERROR, "unrecognized IOObject value: %d", io_object);
	pg_unreachable();
}

void
pgstat_io_init_shmem_cb(void *stats)
{
	PgStatShared_IO *stat_shmem = (PgStatShared_IO *) stats;

	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
		LWLockInitialize(&stat_shmem->locks[i], LWTRANCHE_PGSTATS_DATA);
}

void
pgstat_io_reset_all_cb(TimestampTz ts)
{
	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
	{
		LWLock	   *bktype_lock = &pgStatLocal.shmem->io.locks[i];
		PgStat_BktypeIO *bktype_shstats = &pgStatLocal.shmem->io.stats.stats[i];

		LWLockAcquire(bktype_lock, LW_EXCLUSIVE);

		/*
		 * Use the lock in the first BackendType's PgStat_BktypeIO to protect
		 * the reset timestamp as well.
		 */
		if (i == 0)
			pgStatLocal.shmem->io.stats.stat_reset_timestamp = ts;

		memset(bktype_shstats, 0, sizeof(*bktype_shstats));
		LWLockRelease(bktype_lock);
	}
}

void
pgstat_io_snapshot_cb(void)
{
	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
	{
		LWLock	   *bktype_lock = &pgStatLocal.shmem->io.locks[i];
		PgStat_BktypeIO *bktype_shstats = &pgStatLocal.shmem->io.stats.stats[i];
		PgStat_BktypeIO *bktype_snap = &pgStatLocal.snapshot.io.stats[i];

		LWLockAcquire(bktype_lock, LW_SHARED);

		/*
		 * Use the lock in the first BackendType's PgStat_BktypeIO to protect
		 * the reset timestamp as well.
		 */
		if (i == 0)
			pgStatLocal.snapshot.io.stat_reset_timestamp =
				pgStatLocal.shmem->io.stats.stat_reset_timestamp;

		/* using struct assignment due to better type safety */
		*bktype_snap = *bktype_shstats;
		LWLockRelease(bktype_lock);
	}
}

/*
* IO statistics are not collected for all BackendTypes.
*
* The following BackendTypes do not participate in the cumulative stats
* subsystem or do not perform IO on which we currently track:
* - Dead-end backend because it is not connected to shared memory and
*   doesn't do any IO
* - Syslogger because it is not connected to shared memory
* - Archiver because most relevant archiving IO is delegated to a
*   specialized command or module
*
* Function returns true if BackendType participates in the cumulative stats
* subsystem for IO and false if it does not.
*
* When adding a new BackendType, also consider adding relevant restrictions to
* pgstat_tracks_io_object() and pgstat_tracks_io_op().
*/
bool
pgstat_tracks_io_bktype(BackendType bktype)
{
	/*
	 * List every type so that new backend types trigger a warning about
	 * needing to adjust this switch.
	 */
	switch (bktype)
	{
		case B_INVALID:
		case B_DEAD_END_BACKEND:
		case B_ARCHIVER:
		case B_LOGGER:
			return false;

		case B_AUTOVAC_LAUNCHER:
		case B_AUTOVAC_WORKER:
		case B_BACKEND:
		case B_BG_WORKER:
		case B_BG_WRITER:
		case B_CHECKPOINTER:
		case B_IO_WORKER:
		case B_SLOTSYNC_WORKER:
		case B_STANDALONE_BACKEND:
		case B_STARTUP:
		case B_WAL_RECEIVER:
		case B_WAL_SENDER:
		case B_WAL_SUMMARIZER:
		case B_WAL_WRITER:
			return true;
	}

	return false;
}

/*
 * Some BackendTypes do not perform IO on certain IOObjects or in certain
 * IOContexts. Some IOObjects are never operated on in some IOContexts. Check
 * that the given BackendType is expected to do IO in the given IOContext and
 * on the given IOObject and that the given IOObject is expected to be operated
 * on in the given IOContext.
 */
bool
pgstat_tracks_io_object(BackendType bktype, IOObject io_object,
						IOContext io_context)
{
	bool		no_temp_rel;

	/*
	 * Some BackendTypes should never track IO statistics.
	 */
	if (!pgstat_tracks_io_bktype(bktype))
		return false;

	/*
	 * Currently, IO on IOOBJECT_WAL objects can only occur in the
	 * IOCONTEXT_NORMAL and IOCONTEXT_INIT IOContexts.
	 */
	if (io_object == IOOBJECT_WAL &&
		(io_context != IOCONTEXT_NORMAL &&
		 io_context != IOCONTEXT_INIT))
		return false;

	/*
	 * Currently, IO on temporary relations can only occur in the
	 * IOCONTEXT_NORMAL IOContext.
	 */
	if (io_context != IOCONTEXT_NORMAL &&
		io_object == IOOBJECT_TEMP_RELATION)
		return false;

	/*
	 * In core Postgres, only regular backends and WAL Sender processes
	 * executing queries will use local buffers and operate on temporary
	 * relations. Parallel workers will not use local buffers (see
	 * InitLocalBuffers()); however, extensions leveraging background workers
	 * have no such limitation, so track IO on IOOBJECT_TEMP_RELATION for
	 * BackendType B_BG_WORKER.
	 */
	no_temp_rel = bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER ||
		bktype == B_CHECKPOINTER || bktype == B_AUTOVAC_WORKER ||
		bktype == B_STANDALONE_BACKEND || bktype == B_STARTUP ||
		bktype == B_WAL_SUMMARIZER || bktype == B_WAL_WRITER ||
		bktype == B_WAL_RECEIVER;

	if (no_temp_rel && io_context == IOCONTEXT_NORMAL &&
		io_object == IOOBJECT_TEMP_RELATION)
		return false;

	/*
	 * Some BackendTypes only perform IO under IOOBJECT_WAL, hence exclude all
	 * rows for all the other objects for these.
	 */
	if ((bktype == B_WAL_SUMMARIZER || bktype == B_WAL_RECEIVER ||
		 bktype == B_WAL_WRITER) && io_object != IOOBJECT_WAL)
		return false;

	/*
	 * Some BackendTypes do not currently perform any IO in certain
	 * IOContexts, and, while it may not be inherently incorrect for them to
	 * do so, excluding those rows from the view makes the view easier to use.
	 */
	if ((bktype == B_CHECKPOINTER || bktype == B_BG_WRITER) &&
		(io_context == IOCONTEXT_BULKREAD ||
		 io_context == IOCONTEXT_BULKWRITE ||
		 io_context == IOCONTEXT_VACUUM))
		return false;

	if (bktype == B_AUTOVAC_LAUNCHER && io_context == IOCONTEXT_VACUUM)
		return false;

	if ((bktype == B_AUTOVAC_WORKER || bktype == B_AUTOVAC_LAUNCHER) &&
		io_context == IOCONTEXT_BULKWRITE)
		return false;

	return true;
}

/*
 * Some BackendTypes will never do certain IOOps and some IOOps should not
 * occur in certain IOContexts or on certain IOObjects. Check that the given
 * IOOp is valid for the given BackendType in the given IOContext and on the
 * given IOObject. Note that there are currently no cases of an IOOp being
 * invalid for a particular BackendType only within a certain IOContext and/or
 * only on a certain IOObject.
 */
bool
pgstat_tracks_io_op(BackendType bktype, IOObject io_object,
					IOContext io_context, IOOp io_op)
{
	bool		strategy_io_context;

	/* if (io_context, io_object) will never collect stats, we're done */
	if (!pgstat_tracks_io_object(bktype, io_object, io_context))
		return false;

	/*
	 * Some BackendTypes will not do certain IOOps.
	 */
	if (bktype == B_BG_WRITER &&
		(io_op == IOOP_READ || io_op == IOOP_EVICT || io_op == IOOP_HIT))
		return false;

	if (bktype == B_CHECKPOINTER &&
		((io_object != IOOBJECT_WAL && io_op == IOOP_READ) ||
		 (io_op == IOOP_EVICT || io_op == IOOP_HIT)))
		return false;

	if ((bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER ||
		 bktype == B_CHECKPOINTER) && io_op == IOOP_EXTEND)
		return false;

	/*
	 * Some BackendTypes do not perform reads with IOOBJECT_WAL.
	 */
	if (io_object == IOOBJECT_WAL && io_op == IOOP_READ &&
		(bktype == B_WAL_RECEIVER || bktype == B_BG_WRITER ||
		 bktype == B_AUTOVAC_LAUNCHER || bktype == B_AUTOVAC_WORKER ||
		 bktype == B_WAL_WRITER))
		return false;

	/*
	 * Temporary tables are not logged and thus do not require fsync'ing.
	 * Writeback is not requested for temporary tables.
	 */
	if (io_object == IOOBJECT_TEMP_RELATION &&
		(io_op == IOOP_FSYNC || io_op == IOOP_WRITEBACK))
		return false;

	/*
	 * Some IOOps are not valid in certain IOContexts and some IOOps are only
	 * valid in certain contexts.
	 */
	if (io_context == IOCONTEXT_BULKREAD && io_op == IOOP_EXTEND)
		return false;

	strategy_io_context = io_context == IOCONTEXT_BULKREAD ||
		io_context == IOCONTEXT_BULKWRITE || io_context == IOCONTEXT_VACUUM;

	/*
	 * IOOP_REUSE is only relevant when a BufferAccessStrategy is in use.
	 */
	if (!strategy_io_context && io_op == IOOP_REUSE)
		return false;

	/*
	 * IOOBJECT_WAL IOObject will not do certain IOOps depending on IOContext.
	 */
	if (io_object == IOOBJECT_WAL && io_context == IOCONTEXT_INIT &&
		!(io_op == IOOP_WRITE || io_op == IOOP_FSYNC))
		return false;

	if (io_object == IOOBJECT_WAL && io_context == IOCONTEXT_NORMAL &&
		!(io_op == IOOP_WRITE || io_op == IOOP_READ || io_op == IOOP_FSYNC))
		return false;

	/*
	 * IOOP_FSYNC IOOps done by a backend using a BufferAccessStrategy are
	 * counted in the IOCONTEXT_NORMAL IOContext. See comment in
	 * register_dirty_segment() for more details.
	 */
	if (strategy_io_context && io_op == IOOP_FSYNC)
		return false;


	return true;
}


/*
 * Report IO worker queue statistics
 */
void
pgstat_report_io_worker(void)
{
	PgStatShared_IOWorker *stats_shmem = &pgStatLocal.shmem->ioworker;

	Assert(!pgStatLocal.shmem->is_shutdown);
	pgstat_assert_is_up();

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid unnecessarily modifying the stats entry.
	 */
	if (pg_memory_is_all_zeros(&PendingIOWorkerStats,
							   sizeof(struct PgStat_IOWorkerStats)))
		return;

	pgstat_begin_changecount_write(&stats_shmem->changecount);

#define IOWORKER_ACC(fld) stats_shmem->stats.fld += PendingIOWorkerStats.fld
	IOWORKER_ACC(io_count);
	IOWORKER_ACC(io_bytes);
	IOWORKER_ACC(num_sync_full);
	IOWORKER_ACC(num_sync_lock);
#undef IOWORKER_ACC

	pgstat_end_changecount_write(&stats_shmem->changecount);

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&PendingIOWorkerStats, 0, sizeof(PendingIOWorkerStats));
}


/*
 * Support function for the SQL-callable pgstat* functions. Returns
 * a pointer to the ioworker statistics struct.
 */
PgStat_IOWorkerStats *
pgstat_fetch_stat_io_worker(void)
{
	pgstat_snapshot_fixed(PGSTAT_KIND_IOWORKER);

	return &pgStatLocal.snapshot.ioworker;
}

/*
 * Flush out locally pending ioworker statistics
 *
 * If no stats have been recorded, this function returns false.
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise, return false.
 */
bool
pgstat_ioworker_flush_cb(bool nowait)
{
	PgStatShared_IOWorker *stats_shmem = &pgStatLocal.shmem->ioworker;
	LWLock *lock = &stats_shmem->lock;
	bool		lock_not_acquired = false;

	// FIXME if iomethod != worker?
	// if (!have_lockstats)
	//	return false;

	if (!nowait)
		LWLockAcquire(lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(lock, LW_EXCLUSIVE))
	{
		lock_not_acquired = true;
		goto cleanup;
	}

	elog(WARNING, "pgstat_ioworker_flush_cb pending count %ld full %ld lock %ld",
		PendingIOWorkerStats.io_count,
		PendingIOWorkerStats.num_sync_full,
		PendingIOWorkerStats.num_sync_lock);

#define IOWORKER_ACC(fld) stats_shmem->stats.fld += PendingIOWorkerStats.fld
	IOWORKER_ACC(io_count);
	IOWORKER_ACC(io_bytes);
	IOWORKER_ACC(num_sync_full);
	IOWORKER_ACC(num_sync_lock);
#undef IOWORKER_ACC

	LWLockRelease(lock);

cleanup:

	memset(&PendingIOWorkerStats, 0, sizeof(PendingIOWorkerStats));

	return lock_not_acquired;
}

void
pgstat_ioworker_init_shmem_cb(void *stats)
{
	PgStatShared_IOWorker *stats_shmem = (PgStatShared_IOWorker *) stats;

	LWLockInitialize(&stats_shmem->lock, LWTRANCHE_PGSTATS_DATA);
}

void
pgstat_ioworker_reset_all_cb(TimestampTz ts)
{
	PgStatShared_IOWorker *stats_shmem = &pgStatLocal.shmem->ioworker;

	/* see explanation above PgStatShared_BgWriter for the reset protocol */
	LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	pgstat_copy_changecounted_stats(&stats_shmem->reset_offset,
									&stats_shmem->stats,
									sizeof(stats_shmem->stats),
									&stats_shmem->changecount);
	stats_shmem->stats.stat_reset_timestamp = ts;
	LWLockRelease(&stats_shmem->lock);
}

void
pgstat_ioworker_snapshot_cb(void)
{
	PgStatShared_IOWorker *stats_shmem = &pgStatLocal.shmem->ioworker;
	PgStat_IOWorkerStats *reset_offset = &stats_shmem->reset_offset;
	PgStat_IOWorkerStats reset;

	pgstat_copy_changecounted_stats(&pgStatLocal.snapshot.ioworker,
									&stats_shmem->stats,
									sizeof(stats_shmem->stats),
									&stats_shmem->changecount);

	LWLockAcquire(&stats_shmem->lock, LW_SHARED);
	memcpy(&reset, reset_offset, sizeof(stats_shmem->stats));
	LWLockRelease(&stats_shmem->lock);

	/* compensate by reset offsets */
#define IOWORKER_COMP(fld) pgStatLocal.snapshot.ioworker.fld -= reset.fld;
	IOWORKER_COMP(io_count);
	IOWORKER_COMP(io_bytes);
	IOWORKER_COMP(num_sync_full);
	IOWORKER_COMP(num_sync_lock);
#undef IOWORKER_COMP
}
