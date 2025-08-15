/*-------------------------------------------------------------------------
 *
 * datachecksumsworker.h
 *	  header file for data checksum helper background worker
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/datachecksumsworker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATACHECKSUMSWORKER_H
#define DATACHECKSUMSWORKER_H

/* Shared memory */
extern Size DataChecksumsWorkerShmemSize(void);
extern void DataChecksumsWorkerShmemInit(void);

/* Possible operations the Datachecksumsworker can perform */
typedef enum DataChecksumsWorkerOperation
{
	ENABLE_DATACHECKSUMS,
	DISABLE_DATACHECKSUMS,
	/* TODO: VERIFY_DATACHECKSUMS, */
}			DataChecksumsWorkerOperation;

/*
 * Possible states for a database entry which has been processed. Exported
 * here since we want to be able to reference this from injection point tests.
 */
typedef enum
{
	DATACHECKSUMSWORKER_SUCCESSFUL = 0,
	DATACHECKSUMSWORKER_ABORTED,
	DATACHECKSUMSWORKER_FAILED,
	DATACHECKSUMSWORKER_RETRYDB,
} DataChecksumsWorkerResult;

/* Start the background processes for enabling or disabling checksums */
void		StartDataChecksumsWorkerLauncher(DataChecksumsWorkerOperation op,
											 int cost_delay,
											 int cost_limit,
											 bool fast);

/* Background worker entrypoints */
void		DataChecksumsWorkerLauncherMain(Datum arg);
void		DataChecksumsWorkerMain(Datum arg);

#endif							/* DATACHECKSUMSWORKER_H */
