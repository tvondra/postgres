--
-- Example 5: a LATERAL chain hanging off the nullable side of a LEFT JOIN
--
-- A self-contained test case that builds its own tables and runs a query that
-- mixes an inner join, a LEFT JOIN, and a two-step chain of LATERAL subqueries.
-- Relation "t1" is inner-joined to "t2" and LEFT JOINed to "t3"; the first
-- LATERAL subquery "s" references the nullable side "t3", the second LATERAL
-- subquery "u" references "s", and both are finally tied back to the preserved
-- side "t1".  The LATERAL dependencies pin "s"/"u" after the nullable side of
-- the outer join while the inner-joined "t2" must stay next to "t1", so the
-- selectivity-driven IKKBZ linear order interleaves relations the outer join
-- and the lateral chain require to stay together.  No contiguous (window = 0)
-- split of the linear order is therefore a legal join, the pure-interval DP
-- cannot build the top join relation, and the module falls back to
-- standard_join_search() for the component, which still returns a correct plan.
--
-- (The "OFFSET 0" optimization fences keep the subqueries from being pulled up
-- into the parent query, preserving the laterality.)
--
-- The test verifies that, despite engaging the fallback, the LinDP++ search
-- returns exactly the same rows the standard dynamic-programming join search
-- would.
--
LOAD 'lindp2';

-- Make the search engage on modestly sized joins and stay deterministic.
SET lindp2.enabled = on;
SET lindp2.fallback = false;
SET lindp2.min_threshold = 3;
SET lindp2.max_threshold = 100;
SET lindp2.window_size = 0;
SET lindp2.adaptive = off;
SET lindp2.seeds = 1;
SET geqo = off;

-- Tables wired up with join keys.  The skewed row counts (large hub and large
-- laterally-referenced relation, small inner relations) are what drive the
-- IKKBZ order into the shape that defeats the contiguous-interval DP.
CREATE TABLE ex5_t1 (id int, k int, v int);
CREATE TABLE ex5_t2 (id int, k int, v int);
CREATE TABLE ex5_t3 (id int, k int, v int);
CREATE TABLE ex5_t4 (id int, k int, v int);
CREATE TABLE ex5_t5 (id int, k int, v int);

INSERT INTO ex5_t1 SELECT g, g % 500, g FROM generate_series(0, 999) g;
INSERT INTO ex5_t2 SELECT g, g % 2,   g FROM generate_series(0, 4)   g;
INSERT INTO ex5_t3 SELECT g, g % 2,   g FROM generate_series(0, 4)   g;
INSERT INTO ex5_t4 SELECT g, g % 2,   g FROM generate_series(0, 4)   g;
INSERT INTO ex5_t5 SELECT g, g % 500, g FROM generate_series(0, 999) g;

ANALYZE ex5_t1, ex5_t2, ex5_t3, ex5_t4, ex5_t5;

-- The example query: an inner join plus a LEFT JOIN feeding a chain of two
-- LATERAL subqueries that reference the nullable side of the outer join.
EXPLAIN (COSTS OFF)
SELECT ex5_t1.id AS t1, ex5_t2.id AS t2, ex5_t3.id AS t3,
       s.id AS s, u.id AS u
FROM ex5_t1
    JOIN ex5_t2 ON ex5_t2.id = ex5_t1.id
    LEFT JOIN ex5_t3 ON ex5_t3.id = ex5_t1.id,
    LATERAL (SELECT x.id, x.k FROM ex5_t5 x
             WHERE x.v = ex5_t3.v OFFSET 0) s,
    LATERAL (SELECT y.id FROM ex5_t4 y
             WHERE y.v = s.k OFFSET 0) u
WHERE u.id = ex5_t1.id AND s.id = ex5_t1.id;

-- Show a deterministic slice of the actual result for illustration.
EXPLAIN (COSTS OFF)
SELECT ex5_t1.id AS t1, ex5_t2.id AS t2, ex5_t3.id AS t3,
       s.id AS s, u.id AS u
FROM ex5_t1
    JOIN ex5_t2 ON ex5_t2.id = ex5_t1.id
    LEFT JOIN ex5_t3 ON ex5_t3.id = ex5_t1.id,
    LATERAL (SELECT x.id, x.k FROM ex5_t5 x
             WHERE x.v = ex5_t3.v OFFSET 0) s,
    LATERAL (SELECT y.id FROM ex5_t4 y
             WHERE y.v = s.k OFFSET 0) u
WHERE u.id = ex5_t1.id AND s.id = ex5_t1.id
ORDER BY t1
LIMIT 5;

DROP TABLE ex5_t1, ex5_t2, ex5_t3, ex5_t4, ex5_t5;
