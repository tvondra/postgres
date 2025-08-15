/*--------------------------------------------------------------------------
 *
 * test_checksums.c
 *		Test data checksums
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_checksums/test_checksums.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/datachecksumsworker.h"
#include "storage/latch.h"
#include "utils/injection_point.h"
#include "utils/wait_event.h"

#define USEC_PER_SEC    1000000


PG_MODULE_MAGIC;

extern PGDLLEXPORT void dc_delay_barrier(const char *name, const void *private_data, void *arg);
extern PGDLLEXPORT void dc_fail_database(const char *name, const void *private_data, void *arg);
extern PGDLLEXPORT void dc_dblist(const char *name, const void *private_data, void *arg);
extern PGDLLEXPORT void dc_fake_temptable(const char *name, const void *private_data, void *arg);

/*
 * Test for delaying emission of procsignalbarriers.
 */
void
dc_delay_barrier(const char *name, const void *private_data, void *arg)
{
	(void) name;
	(void) private_data;

	(void) WaitLatch(MyLatch,
					 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					 (3 * 1000),
					 WAIT_EVENT_PG_SLEEP);
}

PG_FUNCTION_INFO_V1(dcw_inject_delay_barrier);
Datum
dcw_inject_delay_barrier(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	bool		attach = PG_GETARG_BOOL(0);

	if (attach)
		InjectionPointAttach("datachecksums-enable-checksums-delay",
							 "test_checksums",
							 "dc_delay_barrier",
							 NULL,
							 0);
	else
		InjectionPointDetach("datachecksums-enable-checksums-delay");
#else
	elog(ERROR,
		 "test is not working as intended when injection points are disabled");
#endif
	PG_RETURN_VOID();
}

void
dc_fail_database(const char *name, const void *private_data, void *arg)
{
	static bool first_pass = true;
	DataChecksumsWorkerResult *res = (DataChecksumsWorkerResult *) arg;

	if (first_pass)
		*res = DATACHECKSUMSWORKER_FAILED;
	first_pass = false;
}

PG_FUNCTION_INFO_V1(dcw_inject_fail_database);
Datum
dcw_inject_fail_database(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	bool		attach = PG_GETARG_BOOL(0);

	if (attach)
		InjectionPointAttach("datachecksumsworker-fail-db",
							 "test_checksums",
							 "dc_fail_database",
							 NULL,
							 0);
	else
		InjectionPointDetach("datachecksumsworker-fail-db");
#else
	elog(ERROR,
		 "test is not working as intended when injection points are disabled");
#endif
	PG_RETURN_VOID();
}

/*
 * Test to remove an entry from the Databaselist to force re-processing since
 * not all databases could be processed in the first iteration of the loop.
 */
void
dc_dblist(const char *name, const void *private_data, void *arg)
{
	static bool first_pass = true;
	List	   *DatabaseList = (List *) arg;

	if (first_pass)
		DatabaseList = list_delete_last(DatabaseList);
	first_pass = false;
}

PG_FUNCTION_INFO_V1(dcw_prune_dblist);
Datum
dcw_prune_dblist(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	bool		attach = PG_GETARG_BOOL(0);

	if (attach)
		InjectionPointAttach("datachecksumsworker-initial-dblist",
							 "test_checksums",
							 "dc_dblist",
							 NULL,
							 0);
	else
		InjectionPointDetach("datachecksumsworker-initial-dblist");
#else
	elog(ERROR,
		 "test is not working as intended when injection points are disabled");
#endif
	PG_RETURN_VOID();
}

/*
 * Test to force waiting for existing temptables.
 */
void
dc_fake_temptable(const char *name, const void *private_data, void *arg)
{
	static bool first_pass = true;
	int		   *numleft = (int *) arg;

	if (first_pass)
		*numleft = 1;
	first_pass = false;
}

PG_FUNCTION_INFO_V1(dcw_fake_temptable);
Datum
dcw_fake_temptable(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	bool		attach = PG_GETARG_BOOL(0);

	if (attach)
		InjectionPointAttach("datachecksumsworker-fake-temptable-wait",
							 "test_checksums",
							 "dc_fake_temptable",
							 NULL,
							 0);
	else
		InjectionPointDetach("datachecksumsworker-fake-temptable-wait");
#else
	elog(ERROR,
		 "test is not working as intended when injection points are disabled");
#endif
	PG_RETURN_VOID();
}
