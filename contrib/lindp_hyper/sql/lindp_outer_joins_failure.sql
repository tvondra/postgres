-- ============================================================================
-- Self-contained reproducer: a 6-way join the lindp join_search_hook cannot
-- linearize, so it falls back to standard_join_search.
--
-- Run against a server with the `lindp` test module available:
--     psql -f this_file.sql
-- ============================================================================

LOAD 'lindp_hyper';

SET lindp_hyper.enabled        = on;
SET lindp_hyper.fallback       = off;  -- error out if linearization failed
SET lindp_hyper.min_relations  = 3;    -- low, so a 6-way join uses LinDP (not GEQO/DP)
SET lindp_hyper.seeds          = 1;    -- try every IKKBZ root
SET geqo                       = off;
SET join_collapse_limit        = 100;   -- keep the whole join flat (one search problem)
SET from_collapse_limit        = 100;
SET client_min_messages  = notice;

DROP TABLE IF EXISTS t1, t2, t3, t4, t5, t6 CASCADE;

CREATE TABLE t1 (id int, x int, y int);
CREATE TABLE t2 (id int, x int, y int);
CREATE TABLE t3 (id int, x int, y int);
CREATE TABLE t4 (id int, x int, y int);
CREATE TABLE t5 (id int, x int, y int);
CREATE TABLE t6 (id int, x int, y int);

-- Skewed cardinalities are essential: they make IKKBZ's C_out surrogate order
-- the relations in a sequence that interleaves a large table INTO the middle of
-- the rigid FULL JOIN chain, so no contiguous-range parenthesization is legal.
INSERT INTO t1 SELECT g, g, g FROM generate_series(1, 5)      g;   -- tiny
INSERT INTO t2 SELECT g, g, g FROM generate_series(1, 4)      g;   -- tiny
INSERT INTO t3 SELECT g, g, g FROM generate_series(1, 120000) g;   -- large
INSERT INTO t4 SELECT g, g, g FROM generate_series(1, 80000)  g;   -- large
INSERT INTO t5 SELECT g, g, g FROM generate_series(1, 90000)  g;   -- large
INSERT INTO t6 SELECT g, g, g FROM generate_series(1, 50000)  g;   -- large
ANALYZE;

-- The query.
--
-- The left-deep chain of three FULL JOINs (t1 ⟗ t2 ⟗ t3 ⟗ t4) is RIGID:
-- full outer joins are neither commutative nor freely re-associatable, so the
-- prefix MUST be built strictly left-to-right (t4 may only join once t1,t2,t3
-- are already together).  The trailing inner JOIN t5 and LEFT JOIN t6 flatten
-- everything into ONE 6-way join-search problem, so the hook runs over all 6.
--
-- IKKBZ orders the relations (by cardinality) as:  t3 t2 t4 t6 t1 t5
-- which wedges t4/t6 between the chain members, so {t1,t2,t3} is never a
-- contiguous interval -> the interval DP cell for the whole range is NULL ->
-- lindp_join_search() returns NULL and falls back to standard_join_search().
EXPLAIN (COSTS OFF)
SELECT *
FROM (((((t1 FULL JOIN t2 ON t2.y = t1.y)
            FULL JOIN t3 ON t3.id = t1.id)
            FULL JOIN t4 ON t4.x  = t2.x)
            JOIN      t5 ON t5.id = t3.id)
            LEFT JOIN t6 ON t6.x  = t4.x);

-- Correctness check: the plan produced via the fallback must match the result
-- of the default search (lindp disabled).  Both return 90000.
SET lindp_hyper.enabled = on;
SET lindp_hyper.fallback = on;

EXPLAIN (COSTS OFF)
SELECT *
FROM (((((t1 FULL JOIN t2 ON t2.y = t1.y)
            FULL JOIN t3 ON t3.id = t1.id)
            FULL JOIN t4 ON t4.x  = t2.x)
            JOIN      t5 ON t5.id = t3.id)
            LEFT JOIN t6 ON t6.x  = t4.x);

SET lindp_hyper.enabled = off;

EXPLAIN (COSTS OFF)
SELECT *
FROM (((((t1 FULL JOIN t2 ON t2.y = t1.y)
            FULL JOIN t3 ON t3.id = t1.id)
            FULL JOIN t4 ON t4.x  = t2.x)
            JOIN      t5 ON t5.id = t3.id)
            LEFT JOIN t6 ON t6.x  = t4.x);

DROP TABLE t1, t2, t3, t4, t5, t6 CASCADE;
