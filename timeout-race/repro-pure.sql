-- Pure-SQL reproducer for a statement_timeout cancel leaking onto the next
-- command.  Needs no C code, extensions or superuser privileges: any role
-- allowed to create temporary objects (granted to PUBLIC by default) can run
-- it, e.g. "psql -X -f repro-pure.sql".  See run-pure.sh.
--
-- A statement that finishes after statement_timeout has fired, without
-- checking for interrupts, completes successfully.
-- disable_statement_timeout() does not clear the fired indicator, so the
-- server reports "canceling statement due to statement timeout" for the
-- following command, without executing it.
--
-- lpad() builds its result without CHECK_FOR_INTERRUPTS(), while repeat()
-- checks for interrupts in its loop.  The length is read from a custom GUC,
-- so that the planner can't fold the calls into constants.  Neither CALL of
-- an empty SQL procedure nor FETCH 1 from a cursor checks for interrupts
-- after evaluating the expression.  (A DO block does not work: PL/pgSQL
-- checks for interrupts before the implicit RETURN ending the block.)
--
-- psql variable len sets the string length; lpad() must take well over the
-- 50ms timeout.

\if :{?len}
\else
\set len 50000000
\endif

\set ON_ERROR_STOP on
SET repro.len = :'len';
CREATE PROCEDURE pg_temp.sink(integer) LANGUAGE sql AS '';
CREATE TEMP TABLE t (x integer);
\set ON_ERROR_STOP off
\pset footer off
\timing on

\echo
\echo '--- control: repeat() checks for interrupts, the CALL is cancelled ---'
SET statement_timeout = '50ms';
CALL pg_temp.sink(length(repeat('x', current_setting('repro.len')::int)));
SELECT 'control next statement survived' AS control_next;

\echo
\echo '--- race (CALL): the CALL succeeds, the INSERT is cancelled ---'
SET statement_timeout = '50ms';
CALL pg_temp.sink(length(lpad('', current_setting('repro.len')::int, 'x')));
INSERT INTO t VALUES (1);
SET statement_timeout = 0;
SELECT count(*) AS rows_inserted FROM t;

\echo
\echo '--- race (cursor, no objects): FETCH succeeds, COMMIT is cancelled ---'
SET statement_timeout = '50ms';
BEGIN;
DECLARE c CURSOR FOR
	SELECT length(lpad('', current_setting('repro.len')::int, 'x')) AS len;
FETCH 1 FROM c;
COMMIT;
ROLLBACK;

\echo
\echo '--- after the error, the indicator is cleared ---'
SET statement_timeout = 0;
SELECT 'session recovered' AS recovered;
