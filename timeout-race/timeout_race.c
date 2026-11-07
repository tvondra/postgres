/*
 * timeout_race.c
 *
 * Busy-wait past statement_timeout without CHECK_FOR_INTERRUPTS().
 *
 * statement_timeout is not cleared when it fires before the statement
 * finishes.  disable_statement_timeout() only resets the indicator while the
 * timeout is still active, and a timeout that has already fired is not
 * active.  If the statement never checks for interrupts, it completes
 * successfully and the next statement is cancelled with
 * "canceling statement due to statement timeout".
 *
 * SQL-level loops (pg_sleep, plpgsql) check for interrupts, so they cancel
 * themselves instead of leaking the cancel.  This procedure does not.
 *
 * Build: see run.sh.
 */

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(spin_ms);
PG_FUNCTION_INFO_V1(spin_ms_check);

static void
spin_until(int32 ms, bool check)
{
	TimestampTz until;

	if (ms < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("spin time must not be negative")));

	until = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), ms);
	while (GetCurrentTimestamp() < until)
	{
		if (check)
			CHECK_FOR_INTERRUPTS();
	}
}

/* Overrun statement_timeout and do not notice the cancel. */
Datum
spin_ms(PG_FUNCTION_ARGS)
{
	spin_until(PG_GETARG_INT32(0), false);
	PG_RETURN_VOID();
}

/* Same wait, but cancel this statement when the timeout fires. */
Datum
spin_ms_check(PG_FUNCTION_ARGS)
{
	spin_until(PG_GETARG_INT32(0), true);
	PG_RETURN_VOID();
}
