-- Reproducer for a statement_timeout cancel leaking onto the next command.
--
-- Requires the timeout_race module (see run.sh).  psql variable so is the
-- absolute path of timeout_race.so.
--
-- A statement that finishes after statement_timeout has fired, without
-- checking for interrupts, completes successfully.  disable_statement_timeout()
-- does not clear the fired indicator, so the server reports
-- "canceling statement due to statement timeout" for the following command.

\set ON_ERROR_STOP on

CREATE OR REPLACE PROCEDURE spin_ms(integer)
	LANGUAGE C AS :'so', 'spin_ms';

CREATE OR REPLACE PROCEDURE spin_ms_check(integer)
	LANGUAGE C AS :'so', 'spin_ms_check';

\set ON_ERROR_STOP off
\pset footer off

\echo
\echo '--- control: interrupt checks cancel the spinning statement ---'
SET statement_timeout = '100ms';
CALL spin_ms_check(500);
SELECT 'control next statement survived' AS control_next;

\echo
\echo '--- race: no interrupt checks, the next statement is cancelled ---'
SET statement_timeout = '100ms';
CALL spin_ms(500);
SELECT 'race next statement survived' AS race_next;

\echo
\echo '--- after the error, the indicator is cleared ---'
SET statement_timeout = 0;
SELECT 'session recovered' AS recovered;
