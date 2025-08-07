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
	(partition integer,			-- partition index
	 numa_node integer,			-- NUMA node of the partitioon
	 num_buffers integer,		-- number of buffers in the partition
	 first_buffer integer,		-- first buffer of partition
	 last_buffer integer,		-- last buffer of partition

	 -- freelists
	 list_consumed bigint,		-- buffers consumed from a freelist
	 list_remain bigint,		-- buffers left in a freelist
	 list_free bigint,			-- number of free buffers

	 -- clocksweep counters
	 num_passes bigint,			-- clocksweep passes
	 next_buffer integer,		-- next victim buffer for clocksweep
	 total_allocs bigint,		-- handled allocs (running total)
	 num_allocs bigint,			-- handled allocs (current cycle)
	 total_req_allocs bigint,	-- requested allocs (running total)
	 num_req_allocs bigint,		-- handled allocs (current cycle)
	 weights int[]);			-- balancing weights

-- Don't want these to be available to public.
REVOKE ALL ON FUNCTION pg_buffercache_partitions() FROM PUBLIC;
REVOKE ALL ON pg_buffercache_partitions FROM PUBLIC;

GRANT EXECUTE ON FUNCTION pg_buffercache_partitions() TO pg_monitor;
GRANT SELECT ON pg_buffercache_partitions TO pg_monitor;
