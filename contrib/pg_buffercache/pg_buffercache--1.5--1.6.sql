/* contrib/pg_buffercache/pg_buffercache--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_buffercache" to load this file. \quit

-- Register the new functions.
CREATE OR REPLACE FUNCTION pg_buffercache_pages()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'pg_buffercache_pages'
LANGUAGE C PARALLEL SAFE;

CREATE OR REPLACE FUNCTION pg_buffercache_numa_pages()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'pg_buffercache_numa_pages'
LANGUAGE C PARALLEL SAFE;

-- Create a view for convenient access.
CREATE OR REPLACE VIEW pg_buffercache AS
	SELECT P.* FROM pg_buffercache_pages() AS P
	(bufferid integer, relfilenode oid, reltablespace oid, reldatabase oid,
	 relforknumber int2, relblocknumber int8, isdirty bool, usagecount int2,
	 pinning_backends int4);

CREATE OR REPLACE VIEW pg_buffercache_numa AS
	SELECT P.* FROM pg_buffercache_numa_pages() AS P
	(bufferid integer, relfilenode oid, reltablespace oid, reldatabase oid,
	 relforknumber int2, relblocknumber int8, isdirty bool, usagecount int2,
	 pinning_backends int4, node_id int4);

-- Don't want these to be available to public.
REVOKE ALL ON FUNCTION pg_buffercache_pages() FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_buffercache_numa_pages() FROM PUBLIC;
REVOKE ALL ON pg_buffercache FROM PUBLIC;
REVOKE ALL ON pg_buffercache_numa FROM PUBLIC;

GRANT EXECUTE ON FUNCTION pg_buffercache_pages() TO pg_monitor;
GRANT EXECUTE ON FUNCTION pg_buffercache_numa_pages() TO pg_monitor;
GRANT SELECT ON pg_buffercache TO pg_monitor;
GRANT SELECT ON pg_buffercache_numa TO pg_monitor;
