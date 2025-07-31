/* contrib/pg_buffercache/pg_buffercache--1.6--1.7.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION pg_buffercache UPDATE TO '1.7'" to load this file. \quit

-- Register the new functions.
CREATE OR REPLACE FUNCTION pg_buffercache_partitions()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'pg_buffercache_partitions'
LANGUAGE C PARALLEL SAFE;

-- Create a view for convenient access.
CREATE VIEW pg_buffercache_partitions AS
	SELECT P.* FROM pg_buffercache_partitions() AS P
	(partition integer,
	 numa_node integer, num_buffers integer, first_buffer integer, last_buffer integer,
	 buffers_consumed bigint, buffers_remain bigint, buffers_free bigint,
	 complete_passes bigint, buffer_total_allocs bigint, buffer_req_allocs bigint,
	 buffer_allocs bigint, next_victim_buffer integer);

-- Don't want these to be available to public.
REVOKE ALL ON FUNCTION pg_buffercache_partitions() FROM PUBLIC;
REVOKE ALL ON pg_buffercache_partitions FROM PUBLIC;

GRANT EXECUTE ON FUNCTION pg_buffercache_partitions() TO pg_monitor;
GRANT SELECT ON pg_buffercache_partitions TO pg_monitor;

-- Register the new functions.
CREATE OR REPLACE FUNCTION pg_buffercache_pgproc()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'pg_buffercache_pgproc'
LANGUAGE C PARALLEL SAFE;

-- Create a view for convenient access.
CREATE VIEW pg_buffercache_pgproc AS
	SELECT P.* FROM pg_buffercache_pgproc() AS P
	(partition integer,
	 numa_node integer, num_procs integer, pgproc_ptr bigint, fastpath_ptr bigint);

-- Don't want these to be available to public.
REVOKE ALL ON FUNCTION pg_buffercache_pgproc() FROM PUBLIC;
REVOKE ALL ON pg_buffercache_pgproc FROM PUBLIC;

GRANT EXECUTE ON FUNCTION pg_buffercache_pgproc() TO pg_monitor;
GRANT SELECT ON pg_buffercache_pgproc TO pg_monitor;
