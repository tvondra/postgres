CREATE TABLE hash_bloom_fact (id int, did int, padding text);
CREATE TABLE hash_bloom_dimension (id int, r float, padding text);

-- fact is 10x the dimension size
SELECT setseed(0); -- stabilize random() output
INSERT INTO hash_bloom_fact SELECT i, 1 + mod(i, 10000), md5(i::text) FROM generate_series(1,100000) s(i);
INSERT INTO hash_bloom_dimension SELECT i, random(), md5(i::text) FROM generate_series(1,10000) s(i);

VACUUM ANALYZE hash_bloom_fact;
VACUUM ANALYZE hash_bloom_dimension;

-- Helper functions for the tests below.
--
-- hash_bloom_count() runs a query returning a single count twice, with Bloom
-- filters disabled and enabled, so that the two results can be compared.
--
-- hash_bloom_toggle() opens a cursor for a query with the given
-- enable_hashjoin_bloom setting, fetches the first row (which builds the
-- hash table), flips the setting, and returns the total number of rows.
--
-- hash_bloom_stats() inspects the EXPLAIN ANALYZE output of a query to see
-- whether the hash join was batched, whether it had to add batches while
-- building the hash table, and whether it built a Bloom filter that rejected
-- at least some outer tuples.  Only the structure of the output is checked,
-- because the exact probe counts depend on the false positives, and so on
-- the hash values and the byte order.
--
-- hash_bloom_node() returns the Hash node of the EXPLAIN ANALYZE output as
-- json, for tests that need to look at individual counters.
--
-- hash_bloom_spill() returns the number of temporary blocks written by a
-- query with Bloom filters disabled and enabled.
CREATE FUNCTION hash_bloom_find_node(node json, node_type text)
RETURNS json LANGUAGE plpgsql
AS
$$
DECLARE
  x json;
  child json;
BEGIN
  IF node->>'Node Type' = node_type THEN
    RETURN node;
  ELSE
    FOR child IN SELECT json_array_elements(node->'Plans')
    LOOP
      x := hash_bloom_find_node(child, node_type);
      IF x IS NOT NULL THEN
        RETURN x;
      END IF;
    END LOOP;
    RETURN NULL;
  END IF;
END;
$$;

CREATE FUNCTION hash_bloom_stats(query text,
                                 OUT batched bool,
                                 OUT grew bool,
                                 OUT bloom_used bool,
                                 OUT bloom_rejects bool)
RETURNS SETOF record LANGUAGE plpgsql
AS
$$
DECLARE
  whole_plan json;
  hash_node json;
BEGIN
  FOR whole_plan IN
    EXECUTE 'EXPLAIN (ANALYZE, VERBOSE, TIMING OFF, SUMMARY OFF, BUFFERS OFF, FORMAT JSON) ' || query
  LOOP
    hash_node := hash_bloom_find_node(json_extract_path(whole_plan, '0', 'Plan'), 'Hash');
    batched := (hash_node->>'Original Hash Batches')::int > 1;
    grew := (hash_node->>'Hash Batches')::int > (hash_node->>'Original Hash Batches')::int;
    bloom_used := (hash_node->'Bloom Filter') IS NOT NULL;
    bloom_rejects := bloom_used AND
      (hash_node->'Bloom Filter'->>'Matches')::bigint < (hash_node->'Bloom Filter'->>'Probes')::bigint;
    RETURN NEXT;
  END LOOP;
END;
$$;

CREATE FUNCTION hash_bloom_node(query text)
RETURNS json LANGUAGE plpgsql
AS
$$
DECLARE
  whole_plan json;
BEGIN
  EXECUTE 'EXPLAIN (ANALYZE, VERBOSE, TIMING OFF, SUMMARY OFF, BUFFERS OFF, FORMAT JSON) ' || query
    INTO whole_plan;
  RETURN hash_bloom_find_node(json_extract_path(whole_plan, '0', 'Plan'), 'Hash');
END;
$$;

CREATE FUNCTION hash_bloom_spill(query text,
                                 OUT bloom_off bigint,
                                 OUT bloom_on bigint)
RETURNS record LANGUAGE plpgsql
AS
$$
DECLARE
  whole_plan json;
BEGIN
  SET LOCAL enable_hashjoin_bloom = off;
  EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY OFF, FORMAT JSON) ' || query
    INTO whole_plan;
  bloom_off := (hash_bloom_find_node(json_extract_path(whole_plan, '0', 'Plan'), 'Hash Join')->>'Temp Written Blocks')::bigint;
  SET LOCAL enable_hashjoin_bloom = on;
  EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY OFF, FORMAT JSON) ' || query
    INTO whole_plan;
  bloom_on := (hash_bloom_find_node(json_extract_path(whole_plan, '0', 'Plan'), 'Hash Join')->>'Temp Written Blocks')::bigint;
END;
$$;

CREATE FUNCTION hash_bloom_count(query text,
                                 OUT bloom_off bigint,
                                 OUT bloom_on bigint)
RETURNS record LANGUAGE plpgsql
AS
$$
BEGIN
  SET LOCAL enable_hashjoin_bloom = off;
  EXECUTE query INTO bloom_off;
  SET LOCAL enable_hashjoin_bloom = on;
  EXECUTE query INTO bloom_on;
END;
$$;

CREATE FUNCTION hash_bloom_toggle(query text, enabled bool)
RETURNS bigint LANGUAGE plpgsql
AS
$$
DECLARE
  c refcursor;
  first bigint;
  rest bigint;
BEGIN
  PERFORM set_config('enable_hashjoin_bloom', enabled::text, true);
  OPEN c FOR EXECUTE query;
  MOVE c;
  GET DIAGNOSTICS first = ROW_COUNT;
  PERFORM set_config('enable_hashjoin_bloom', (NOT enabled)::text, true);
  MOVE ALL FROM c;
  GET DIAGNOSTICS rest = ROW_COUNT;
  CLOSE c;
  RETURN first + rest;
END;
$$;
--
-- hash_bloom_explain() runs EXPLAIN ANALYZE of a query and replaces the
-- numbers in the output that depend on the platform (memory usage, numbers
-- of buckets and batches), on the hash values (false positives, and so the
-- lookup, probe and match counts) or on the number of workers actually
-- launched (row counts and loops), leaving only the structure of the plan.
-- Batch counts are kept only as far as needed to tell single-batch joins
-- from batched ones.
CREATE FUNCTION hash_bloom_explain(query text, verbose_mode bool DEFAULT false)
RETURNS SETOF text LANGUAGE plpgsql
AS
$$
DECLARE
  ln text;
BEGIN
  FOR ln IN
    EXECUTE 'EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF, BUFFERS OFF, SUMMARY OFF'
            || CASE WHEN verbose_mode THEN ', VERBOSE' ELSE '' END || ') ' || query
  LOOP
    ln := regexp_replace(ln, 'actual rows=\d+(\.\d+)? loops=\d+', 'actual rows=N loops=N');
    ln := regexp_replace(ln, 'Workers Launched: \d+', 'Workers Launched: N');
    ln := regexp_replace(ln, '(Buckets|Memory Usage|Size|Hash Functions|Hash Lookups|Probes|Matches): \d+\w*', '\1: N', 'g');
    ln := regexp_replace(ln, 'Batches: ([2-9]|\d\d+)', 'Batches: N', 'g');
    ln := regexp_replace(ln, '\(originally ([2-9]|\d\d+)\)', '(originally N)', 'g');
    ln := regexp_replace(ln, 'Rate: \d+\.\d+%', 'Rate: N%', 'g');
    RETURN NEXT ln;
  END LOOP;
END;
$$;

-- no parallel queries for now, force hashjoins
SET max_parallel_workers_per_gather = 0;
SET enable_nestloop = off;
SET enable_mergejoin = off;
SET work_mem = '512kB';

-- non-selective in-memory hash join does not use Bloom filters

SET enable_hashjoin_bloom = off;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id)$$, true);

SET enable_hashjoin_bloom = on;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id)$$, true);

-- a selective in-memory join uses a filter (after 1000 lookups)

SET enable_hashjoin_bloom = off;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5$$, true);

SET enable_hashjoin_bloom = on;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5$$, true);

-- force batching
SET work_mem = '128kB';

-- batched join always creates a Bloom filter, but then disables it if
-- not selective enough

SET enable_hashjoin_bloom = off;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id)$$, true);

SET enable_hashjoin_bloom = on;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id)$$, true);

-- batched join always creates a Bloom filter, and keeps using it if
-- selective enough

SET enable_hashjoin_bloom = off;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5$$, true);

SET enable_hashjoin_bloom = on;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5$$, true);

-- An outer relation where most rows have no match in the dimension: 1% of
-- the keys are NULL, 9% match (900 distinct dimension ids), 90% don't.
CREATE TABLE hash_bloom_probe (id int, did int, padding text);
INSERT INTO hash_bloom_probe
  SELECT i,
         CASE WHEN i % 100 = 0 THEN NULL
              WHEN i % 10 = 0 THEN 1 + mod(i, 10000)
              ELSE 10000 + i END,
         md5(i::text)
  FROM generate_series(1,100000) s(i);
VACUUM ANALYZE hash_bloom_probe;

-- A copy of the dimension whose size the planner underestimates, so that
-- the hash table starts with a single batch and has to add batches while
-- it is being built.
CREATE TABLE hash_bloom_underestimated AS SELECT * FROM hash_bloom_dimension;
ALTER TABLE hash_bloom_underestimated SET (autovacuum_enabled = 'false');
ANALYZE hash_bloom_underestimated;
UPDATE pg_class SET reltuples = 500 WHERE relname = 'hash_bloom_underestimated';

-- a join with no matching rows at all builds (and uses) a filter too
SET work_mem = '512kB';
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id) WHERE p.did > 10000
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id) WHERE p.did > 10000
$$);

-- Outer tuples rejected by the filter must still be null-extended by outer
-- joins and counted by anti joins, and NULL keys must not match anything.
-- First in a single batch (filter built adaptively), then with batching
-- (filter built up front, skew buckets used for the common outer keys).
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p FULL JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_dimension d LEFT JOIN hash_bloom_probe p ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p WHERE EXISTS (SELECT 1 FROM hash_bloom_dimension d WHERE p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p WHERE NOT EXISTS (SELECT 1 FROM hash_bloom_dimension d WHERE p.did = d.id)
$$);

SET work_mem = '128kB';
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p FULL JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_dimension d LEFT JOIN hash_bloom_probe p ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p WHERE EXISTS (SELECT 1 FROM hash_bloom_dimension d WHERE p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p WHERE NOT EXISTS (SELECT 1 FROM hash_bloom_dimension d WHERE p.did = d.id)
$$);

-- When a single-batch hash table has to add batches while being built, the
-- filter is built at that point (from the tuples already in the hash table)
-- and the remaining tuples are added to it as they arrive.
SET work_mem = '64kB';
SET hash_mem_multiplier = 1.0;
EXPLAIN (COSTS OFF)
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_underestimated d ON (p.did = d.id);
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_underestimated d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_underestimated d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_underestimated d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p WHERE NOT EXISTS (SELECT 1 FROM hash_bloom_underestimated d WHERE p.did = d.id)
$$);

-- no filter is built at that point with Bloom filters disabled
SET enable_hashjoin_bloom = off;
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_underestimated d ON (p.did = d.id)
$$);
SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_underestimated d ON (p.did = d.id);
SET enable_hashjoin_bloom = on;

RESET hash_mem_multiplier;
SET work_mem = '128kB';

-- Changing enable_hashjoin_bloom while a join is running has no effect on
-- it, the decision is made when the hash table is created.  Both cursors
-- use a batched join; the first one must not build a filter after the fact,
-- the second one keeps using the one it has.
SELECT hash_bloom_toggle($$
  SELECT p.id FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)
$$, false);
SELECT hash_bloom_toggle($$
  SELECT p.id FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id)
$$, true);

-- Rescans.  When the inner side depends on the parameter, the hash table
-- (and the filter) is rebuilt for each rescan, and only some of the hash
-- tables may build a filter.  The statistics should reflect the filter no
-- matter which of the rescans built it.
SET work_mem = '512kB';
EXPLAIN (COSTS OFF)
  SELECT t.thr, (SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < t.thr)
    FROM (VALUES (1.0), (0.5)) t(thr);
SELECT t.thr, (SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < t.thr)
  FROM (VALUES (1.0), (0.5)) t(thr);
SELECT * FROM hash_bloom_stats($$
  SELECT t.thr, (SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < t.thr)
    FROM (VALUES (1.0), (0.5)) t(thr)
$$);
SELECT * FROM hash_bloom_stats($$
  SELECT t.thr, (SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < t.thr)
    FROM (VALUES (0.5), (1.0)) t(thr)
$$);

-- When only the outer side depends on the parameter, a single-batch hash
-- table (and its filter) is reused by the rescans.
EXPLAIN (COSTS OFF)
  SELECT v.x, (SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id) WHERE p.id > v.x)
    FROM (VALUES (0), (50000), (99990)) v(x);
SELECT v.x, (SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id) WHERE p.id > v.x)
  FROM (VALUES (0), (50000), (99990)) v(x);
SELECT * FROM hash_bloom_stats($$
  SELECT v.x, (SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id) WHERE p.id > v.x)
    FROM (VALUES (0), (50000), (99990)) v(x)
$$);

-- the same with batching, which rebuilds the hash table for each rescan
SET work_mem = '128kB';
SELECT v.x, (SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id) WHERE p.id > v.x)
  FROM (VALUES (0), (50000), (99990)) v(x);
SELECT * FROM hash_bloom_stats($$
  SELECT v.x, (SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id) WHERE p.id > v.x)
    FROM (VALUES (0), (50000), (99990)) v(x)
$$);

-- test parallel hash joins
SET work_mem = '512kB';
SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

ALTER TABLE hash_bloom_fact SET (parallel_workers = 2);

-- non-selective in-memory hash join does not use Bloom filters

SET enable_hashjoin_bloom = off;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id)$$);

SET enable_hashjoin_bloom = on;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id)$$);

-- a selective in-memory join uses a filter (after 1000 lookups)

SET enable_hashjoin_bloom = off;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5$$);

SET enable_hashjoin_bloom = on;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5$$);

-- force batching
SET work_mem = '128kB';

-- batched join always creates a Bloom filter, but then disables it if
-- not selective enough

SET enable_hashjoin_bloom = off;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id)$$);

SET enable_hashjoin_bloom = on;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id)$$);

-- batched join always creates a Bloom filter, and keeps using it if
-- selective enough

SET enable_hashjoin_bloom = off;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5$$);

SET enable_hashjoin_bloom = on;
SELECT * FROM hash_bloom_explain($$SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5$$);

-- Parallel-aware hash joins (Parallel Hash) build and probe the filter
-- cooperatively.  Only the shape of the plans and the results are checked,
-- as the rest depends on the number of workers that were launched.
ALTER TABLE hash_bloom_dimension SET (parallel_workers = 2);
ALTER TABLE hash_bloom_probe SET (parallel_workers = 2);
ALTER TABLE hash_bloom_underestimated SET (parallel_workers = 2);
SET enable_parallel_hash = on;
SET work_mem = '512kB';

-- a non-selective in-memory join does not use a filter, a selective one does
EXPLAIN (COSTS OFF)
SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id);
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id)
$$);
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5
$$);

-- outer and anti joins in a single batch (filter built adaptively)
EXPLAIN (COSTS OFF)
SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id);
EXPLAIN (COSTS OFF)
SELECT count(*) FROM hash_bloom_probe p FULL JOIN hash_bloom_dimension d ON (p.did = d.id);
EXPLAIN (COSTS OFF)
SELECT count(*) FROM hash_bloom_probe p WHERE NOT EXISTS (SELECT 1 FROM hash_bloom_dimension d WHERE p.did = d.id);
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p FULL JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_dimension d LEFT JOIN hash_bloom_probe p ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p WHERE EXISTS (SELECT 1 FROM hash_bloom_dimension d WHERE p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p WHERE NOT EXISTS (SELECT 1 FROM hash_bloom_dimension d WHERE p.did = d.id)
$$);

-- The same with batching (filter built up front).  A parallel hash table
-- may use the memory of all the participants, so it takes a smaller limit
-- to make it batch.
SET work_mem = '64kB';
SET hash_mem_multiplier = 1.0;
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p FULL JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_dimension d LEFT JOIN hash_bloom_probe p ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p WHERE EXISTS (SELECT 1 FROM hash_bloom_dimension d WHERE p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p WHERE NOT EXISTS (SELECT 1 FROM hash_bloom_dimension d WHERE p.did = d.id)
$$);

-- a single-batch hash table that has to add batches while being built
EXPLAIN (COSTS OFF)
SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_underestimated d ON (p.did = d.id);
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_underestimated d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_underestimated d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_underestimated d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p WHERE NOT EXISTS (SELECT 1 FROM hash_bloom_underestimated d WHERE p.did = d.id)
$$);

-- no filter is built at that point with Bloom filters disabled
SET enable_hashjoin_bloom = off;
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_underestimated d ON (p.did = d.id)
$$);
SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_underestimated d ON (p.did = d.id);
SET enable_hashjoin_bloom = on;
RESET hash_mem_multiplier;

-- Rescans of a parallel hash join, which rebuild the hash table (and the
-- filter) every time.  First in a single batch, then with batching.
SET work_mem = '512kB';
SET enable_nestloop = on;
SET enable_material = off;
EXPLAIN (COSTS OFF)
SELECT v.x, ss.id FROM (VALUES (10), (15), (99990)) v(x)
  LEFT JOIN (SELECT p.id FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)) ss
  ON v.x < ss.id + 1 AND v.x > ss.id - 1;
SELECT v.x, ss.id FROM (VALUES (10), (15), (99990)) v(x)
  LEFT JOIN (SELECT p.id FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)) ss
  ON v.x < ss.id + 1 AND v.x > ss.id - 1;
SELECT * FROM hash_bloom_stats($$
  SELECT v.x, ss.id FROM (VALUES (10), (15), (99990)) v(x)
    LEFT JOIN (SELECT p.id FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)) ss
    ON v.x < ss.id + 1 AND v.x > ss.id - 1
$$);
SET work_mem = '64kB';
SET hash_mem_multiplier = 1.0;
EXPLAIN (COSTS OFF)
SELECT v.x, ss.id FROM (VALUES (10), (15), (99990)) v(x)
  LEFT JOIN (SELECT p.id FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)) ss
  ON v.x < ss.id + 1 AND v.x > ss.id - 1;
SELECT v.x, ss.id FROM (VALUES (10), (15), (99990)) v(x)
  LEFT JOIN (SELECT p.id FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)) ss
  ON v.x < ss.id + 1 AND v.x > ss.id - 1;
SELECT * FROM hash_bloom_stats($$
  SELECT v.x, ss.id FROM (VALUES (10), (15), (99990)) v(x)
    LEFT JOIN (SELECT p.id FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)) ss
    ON v.x < ss.id + 1 AND v.x > ss.id - 1
$$);
RESET hash_mem_multiplier;
SET enable_nestloop = off;
RESET enable_material;

-- A serial plan that debug_parallel_query = regress puts under a Gather runs
-- in a worker whose details EXPLAIN hides, but the statistics the worker
-- collected must still be reported.
ALTER TABLE hash_bloom_dimension SET (parallel_workers = 0);
ALTER TABLE hash_bloom_probe SET (parallel_workers = 0);
SET work_mem = '512kB';
SET debug_parallel_query = regress;
EXPLAIN (COSTS OFF)
SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id);
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
SELECT * FROM hash_bloom_count($$
  SELECT count(*) FROM hash_bloom_probe p LEFT JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
RESET debug_parallel_query;

-- Regression tests for bugs found during review.  Each block says what it
-- guards against.  The counters are only visible in EXPLAIN ANALYZE, which
-- runs the plan in a single ExecutorRun(), so how the counters survive the
-- shutdown at the end of a run (cursors, SPI) cannot be tested here.

-- serial joins again
SET max_parallel_workers_per_gather = 0;
SET work_mem = '512kB';

-- #6: an outer tuple with several hash-equal inner tuples that all fail the
-- join filter was counted as a match once per inner tuple.  Every fact row
-- with did <= 2000 finds five hash-equal rows in hash_bloom_dups, none of
-- which passes the join filter: exactly 20000 matches.  The over-count also
-- made the match rate look like 100%, which prevented the adaptive filter
-- from being built.
CREATE TABLE hash_bloom_dups AS
  SELECT d.id, d.r FROM hash_bloom_dimension d, generate_series(1, 5) WHERE d.id <= 2000;
VACUUM ANALYZE hash_bloom_dups;
EXPLAIN (COSTS OFF)
SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dups d ON (f.did = d.id AND d.r > f.id);
SELECT (n->>'Hash Matches')::bigint AS hash_matches,
       (n->'Bloom Filter') IS NOT NULL AS bloom_used
  FROM hash_bloom_node($$
    SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dups d ON (f.did = d.id AND d.r > f.id)
  $$) n;

-- #21: the fields that carry the parameters of a destroyed hash table's
-- filter to EXPLAIN were not initialized.  With the filter disabled nothing
-- assigns them, and a hash table rebuilt for each rescan is likely to be
-- allocated where the previous one was freed, so garbage was reported (on
-- assert-enabled builds, the pattern pfree() leaves behind).
SET enable_hashjoin_bloom = off;
SELECT * FROM hash_bloom_stats($$
  SELECT t.thr, (SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < t.thr)
    FROM (VALUES (1.0), (0.5)) t(thr)
$$);
SET enable_hashjoin_bloom = on;

-- #9: outer tuples rejected by the filter must not be written to the batch
-- files.  Most of hash_bloom_probe has no match, so a batched join with the
-- filter writes much less temporary data than one without.  The serial
-- join always did this, and is the baseline for the parallel one below.
SET work_mem = '128kB';
SELECT bloom_on > 0 AS spills, bloom_on * 2 < bloom_off AS filter_reduces_spill
  FROM hash_bloom_spill($$
    SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)
  $$);

-- parallel joins again
SET max_parallel_workers_per_gather = 2;
ALTER TABLE hash_bloom_dimension SET (parallel_workers = 2);
ALTER TABLE hash_bloom_probe SET (parallel_workers = 2);
SET work_mem = '64kB';
SET hash_mem_multiplier = 1.0;

-- #8: with parallel_leader_participation = off the leader never touches the
-- hash table, and EXPLAIN reported no Bloom filter at all, because only the
-- leader's statistics decided whether to print it.  (When no worker can be
-- launched the leader runs the join itself, and the test passes trivially.)
SET parallel_leader_participation = off;
SELECT * FROM hash_bloom_stats($$
  SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)
$$);
RESET parallel_leader_participation;

-- #9: a batched parallel hash join partitions the whole outer relation into
-- shared batch files before probing, and used to write the tuples the filter
-- rejects too.
SELECT bloom_on > 0 AS spills, bloom_on * 2 < bloom_off AS filter_reduces_spill
  FROM hash_bloom_spill($$
    SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)
  $$);

-- #21: the same for a parallel rescan, which rebuilds the hash table too
SET enable_hashjoin_bloom = off;
SET enable_nestloop = on;
SET enable_material = off;
SELECT * FROM hash_bloom_stats($$
  SELECT v.x, ss.id FROM (VALUES (10), (15), (99990)) v(x)
    LEFT JOIN (SELECT p.id FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)) ss
    ON v.x < ss.id + 1 AND v.x > ss.id - 1
$$);
SET enable_hashjoin_bloom = on;
SET enable_nestloop = off;
RESET enable_material;

-- #23: a parallel rescan frees the shared filter before the leader
-- accumulates the statistics of the hash table being destroyed, so the
-- filter built by an earlier scan was lost when the last one built none.
-- The inner side is restricted by an InitPlan that depends on the outer
-- row: the first scan hashes a tenth of the dimension (the first lookups
-- find no match, so a filter is built adaptively), the second all of it
-- (every lookup matches, no filter).  The statistics reported by workers
-- are kept across rescans, so the leader has to probe alone for the loss to
-- be visible: max_parallel_workers = 0 leaves the plan unchanged but
-- launches no worker.  The reversed order, which ends with a filter, is the
-- control.
SET work_mem = '512kB';
RESET hash_mem_multiplier;
SET max_parallel_workers = 0;
EXPLAIN (COSTS OFF)
SELECT t.i, (SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.id > (SELECT 9000 * (2 - t.i)))
  FROM (VALUES (1), (2)) t(i);
SELECT t.i, (SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.id > (SELECT 9000 * (2 - t.i)))
  FROM (VALUES (1), (2)) t(i);
SELECT * FROM hash_bloom_stats($$
  SELECT t.i, (SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.id > (SELECT 9000 * (2 - t.i)))
    FROM (VALUES (1), (2)) t(i)
$$);
SELECT * FROM hash_bloom_stats($$
  SELECT t.i, (SELECT count(*) FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.id > (SELECT 9000 * (t.i - 1)))
    FROM (VALUES (1), (2)) t(i)
$$);

-- #29: the participant that built the shared adaptive filter published it
-- for the others but did not start probing it itself until its next window
-- of lookups, so its first 1000 outer tuples after the build went to the
-- hash table for nothing.  Probing alone, the leader must use the filter for
-- every outer tuple after the window that builds it, exactly like the serial
-- join: hash_bloom_probe has 99000 non-NULL keys, 1000 are looked up before
-- the filter exists and 98000 are probed after.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id);
SELECT (n->'Bloom Filter'->>'Probes')::bigint AS bloom_probes,
       (n->>'Hash Lookups')::bigint - (n->'Bloom Filter'->>'Matches')::bigint AS lookups_before_filter
  FROM hash_bloom_node($$
    SELECT count(*) FROM hash_bloom_probe p JOIN hash_bloom_dimension d ON (p.did = d.id)
  $$) n;
RESET max_parallel_workers;

DROP TABLE hash_bloom_fact;
DROP TABLE hash_bloom_dimension;
DROP TABLE hash_bloom_probe;
DROP TABLE hash_bloom_underestimated;
DROP TABLE hash_bloom_dups;
DROP FUNCTION hash_bloom_count(text);
DROP FUNCTION hash_bloom_toggle(text, bool);
DROP FUNCTION hash_bloom_stats(text);
DROP FUNCTION hash_bloom_node(text);
DROP FUNCTION hash_bloom_spill(text);
DROP FUNCTION hash_bloom_find_node(json, text);
DROP FUNCTION hash_bloom_explain(text, bool);
