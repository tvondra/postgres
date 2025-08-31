/* src/test/modules/test_checksums/test_checksums--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_checksums" to load this file. \quit

CREATE FUNCTION dcw_inject_delay_barrier(attach boolean DEFAULT true)
	RETURNS pg_catalog.void
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION dcw_inject_fail_database(attach boolean DEFAULT true)
	RETURNS pg_catalog.void
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION dcw_prune_dblist(attach boolean DEFAULT true)
	RETURNS pg_catalog.void
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION dcw_fake_temptable(attach boolean DEFAULT true)
	RETURNS pg_catalog.void
	AS 'MODULE_PATHNAME' LANGUAGE C;
