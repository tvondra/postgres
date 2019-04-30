/*-------------------------------------------------------------------------
 *
 * prefetch.h
 *	  header file for integrated prefetch daemon
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/prefetch.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREFETCH_H
#define PREFETCH_H

#include "storage/block.h"
#include "storage/buf_internals.h"

/* GUC variables */
extern int			prefetch_workers;
extern int			prefetch_naptime;

/* prefetch launcher PID, only valid when worker is shutting down */
extern int	PrefetchLauncherPid;

/* Status inquiry functions */
extern bool PrefetchActive(void);
extern bool IsPrefetchLauncherProcess(void);
extern bool IsPrefetchWorkerProcess(void);

#define IsAnyPrefetchProcess() \
	(IsPrefetchLauncherProcess() || IsPrefetchWorkerProcess())

/* Functions to start autovacuum process, called from postmaster */
extern void prefetch_init(void);
extern int	StartPrefetchLauncher(void);
extern int	StartPrefetchWorker(void);

/* called from postmaster when a worker could not be forked */
extern void PrefetchWorkerFailed(void);

#ifdef EXEC_BACKEND
extern void PrefetchLauncherMain(int argc, char *argv[]) pg_attribute_noreturn();
extern void PrefetchWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();
extern void PrefetchWorkerIAm(void);
extern void PrefetchLauncherIAm(void);
#endif

/* shared memory stuff */
extern Size PrefetchShmemSize(void);
extern void PrefetchShmemInit(void);

/* used to submit requests */
extern int SubmitPrefetchRequests(int nrequests, BufferTag *requests, bool nowait);

#endif							/* PREFETCH_H */
