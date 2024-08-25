/* contrib/pg_visibility/pg_visibility--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_visibility UPDATE TO '1.3'" to load this file. \quit

-- Show visibility map information for each block in a relation.
CREATE FUNCTION pg_visibility_map_sample(regclass,
								  nblocks int,
								  all_visible boolean,
								  all_frozen boolean,
								  blkno OUT bigint,
								  all_visible OUT boolean,
								  all_frozen OUT boolean,
								  lsn OUT pg_lsn)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_visibility_map_sample'
LANGUAGE C;


-- Allow use of monitoring functions by pg_monitor members
GRANT EXECUTE ON FUNCTION pg_visibility_map_sample(regclass, int, boolean, boolean) TO pg_stat_scan_tables;
