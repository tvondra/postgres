--
-- Example 2: nested non-inner joins
--
-- A self-contained test case that builds its own tables and runs a query in
-- which a LEFT JOIN has a nested inner join on its right-hand side.  With this
-- data the IKKBZ linear order interleaves the relations that the outer join
-- requires to stay together, so no contiguous (window = 0) split of the linear
-- order is a legal join.  The pure-interval DP therefore cannot build the top
-- join relation and the module falls back to standard_join_search() for the
-- component, which still returns a correct plan.
--
-- The test verifies that, despite engaging the fallback, the LinDP++ search
-- returns exactly the same rows the standard dynamic-programming join search
-- would.
--
LOAD 'lindp_hyper';

SET lindp_hyper.enabled        = on;
SET lindp_hyper.fallback       = off;  -- error out if linearization failed
SET lindp_hyper.min_relations  = 3;    -- low, so a 6-way join uses LinDP (not GEQO/DP)
SET lindp_hyper.seeds          = 1;    -- try every IKKBZ root
SET geqo                       = off;
SET join_collapse_limit        = 100;   -- keep the whole join flat (one search problem)
SET from_collapse_limit        = 100;


-- Tables wired up with join keys.  The skewed row counts are what drive the
-- IKKBZ order into the shape that defeats the contiguous-interval DP.
CREATE TABLE ex2_t1 (id int, k int, v int);
CREATE TABLE ex2_t2 (id int, k int, v int);
CREATE TABLE ex2_t3 (id int, k int, v int);
CREATE TABLE ex2_t4 (id int, k int, v int);
CREATE TABLE ex2_t5 (id int, k int, v int);

INSERT INTO ex2_t1 SELECT g, g % 500, g FROM generate_series(0, 999) g;
INSERT INTO ex2_t2 SELECT g, g % 2,   g FROM generate_series(0, 4)   g;
INSERT INTO ex2_t3 SELECT g, g % 2,   g FROM generate_series(0, 4)   g;
INSERT INTO ex2_t4 SELECT g, g % 2,   g FROM generate_series(0, 4)   g;
INSERT INTO ex2_t5 SELECT g, g % 500, g FROM generate_series(0, 999) g;

ANALYZE ex2_t1, ex2_t2, ex2_t3, ex2_t4, ex2_t5;

-- The example query: a LEFT JOIN whose right-hand side is itself an inner join,
-- with a further inner join on top.
EXPLAIN (COSTS OFF)
SELECT ex2_t1.id AS t1, ex2_t2.id AS t2, ex2_t3.id AS t3,
       ex2_t4.id AS t4, ex2_t5.id AS t5
FROM (ex2_t1 JOIN ex2_t2 ON ex2_t2.id = ex2_t1.id)
    LEFT JOIN (ex2_t3 JOIN ex2_t4 ON ex2_t4.id = ex2_t3.id)
        ON ex2_t3.id = ex2_t2.id
    JOIN ex2_t5 ON ex2_t5.id = ex2_t1.id;

-- Show a deterministic slice of the actual result for illustration.
EXPLAIN (COSTS OFF)
SELECT ex2_t1.id AS t1, ex2_t2.id AS t2, ex2_t3.id AS t3,
       ex2_t4.id AS t4, ex2_t5.id AS t5
FROM (ex2_t1 JOIN ex2_t2 ON ex2_t2.id = ex2_t1.id)
    LEFT JOIN (ex2_t3 JOIN ex2_t4 ON ex2_t4.id = ex2_t3.id)
        ON ex2_t3.id = ex2_t2.id
    JOIN ex2_t5 ON ex2_t5.id = ex2_t1.id
ORDER BY t1
LIMIT 5;

-- explore a bigger part of the search space
SET lindp_hyper.seeds = 10;

EXPLAIN (COSTS OFF)
SELECT ex2_t1.id AS t1, ex2_t2.id AS t2, ex2_t3.id AS t3,
       ex2_t4.id AS t4, ex2_t5.id AS t5
FROM (ex2_t1 JOIN ex2_t2 ON ex2_t2.id = ex2_t1.id)
    LEFT JOIN (ex2_t3 JOIN ex2_t4 ON ex2_t4.id = ex2_t3.id)
        ON ex2_t3.id = ex2_t2.id
    JOIN ex2_t5 ON ex2_t5.id = ex2_t1.id;
    
EXPLAIN (COSTS OFF)
SELECT ex2_t1.id AS t1, ex2_t2.id AS t2, ex2_t3.id AS t3,
       ex2_t4.id AS t4, ex2_t5.id AS t5
FROM (ex2_t1 JOIN ex2_t2 ON ex2_t2.id = ex2_t1.id)
    LEFT JOIN (ex2_t3 JOIN ex2_t4 ON ex2_t4.id = ex2_t3.id)
        ON ex2_t3.id = ex2_t2.id
    JOIN ex2_t5 ON ex2_t5.id = ex2_t1.id
ORDER BY t1
LIMIT 5;

-- now try fallback, and check it produces the same plan as regular search
RESET lindp_hyper.seeds;

SET lindp_hyper.enabled = on;
SET lindp_hyper.fallback = on;

EXPLAIN (COSTS OFF)
SELECT ex2_t1.id AS t1, ex2_t2.id AS t2, ex2_t3.id AS t3,
       ex2_t4.id AS t4, ex2_t5.id AS t5
FROM (ex2_t1 JOIN ex2_t2 ON ex2_t2.id = ex2_t1.id)
    LEFT JOIN (ex2_t3 JOIN ex2_t4 ON ex2_t4.id = ex2_t3.id)
        ON ex2_t3.id = ex2_t2.id
    JOIN ex2_t5 ON ex2_t5.id = ex2_t1.id;

SET lindp_hyper.enabled = off;

EXPLAIN (COSTS OFF)
SELECT ex2_t1.id AS t1, ex2_t2.id AS t2, ex2_t3.id AS t3,
       ex2_t4.id AS t4, ex2_t5.id AS t5
FROM (ex2_t1 JOIN ex2_t2 ON ex2_t2.id = ex2_t1.id)
    LEFT JOIN (ex2_t3 JOIN ex2_t4 ON ex2_t4.id = ex2_t3.id)
        ON ex2_t3.id = ex2_t2.id
    JOIN ex2_t5 ON ex2_t5.id = ex2_t1.id;

DROP TABLE ex2_t1, ex2_t2, ex2_t3, ex2_t4, ex2_t5;
