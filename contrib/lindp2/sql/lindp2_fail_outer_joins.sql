--
-- Example 1: non-inner joins (LEFT + RIGHT)
--
-- A self-contained test case that builds its own tables and runs a join whose
-- graph mixes a LEFT JOIN and a RIGHT JOIN around a common hub relation.  With
-- this data the selectivity-driven IKKBZ linear order interleaves the two
-- sides of the outer joins, so no contiguous (window = 0) split of the linear
-- order is a legal join.  The pure-interval DP therefore cannot build the top
-- join relation and the module falls back to standard_join_search() for the
-- component, which still returns a correct plan.
--
-- The test verifies that, despite engaging the fallback, the LinDP++ search
-- returns exactly the same rows the standard dynamic-programming join search
-- would.
--
LOAD 'lindp2';

SET lindp2.enabled = on;
SET lindp2.min_threshold = 2;
SET lindp2.fallback = off;
SET geqo = off;


-- Tables wired up with join keys.  The skewed row counts (large hub and large
-- RIGHT-joined relation, small inner relations) are what drive the IKKBZ order
-- into the shape that defeats the contiguous-interval DP.
CREATE TABLE ex1_t1 (id int, k int, v int);
CREATE TABLE ex1_t2 (id int, k int, v int);
CREATE TABLE ex1_t3 (id int, k int, v int);
CREATE TABLE ex1_t4 (id int, k int, v int);
CREATE TABLE ex1_t5 (id int, k int, v int);

INSERT INTO ex1_t1 SELECT g, g % 500, g FROM generate_series(0, 999) g;
INSERT INTO ex1_t2 SELECT g, g % 2,   g FROM generate_series(0, 4)   g;
INSERT INTO ex1_t3 SELECT g, g % 2,   g FROM generate_series(0, 4)   g;
INSERT INTO ex1_t4 SELECT g, g % 2,   g FROM generate_series(0, 4)   g;
INSERT INTO ex1_t5 SELECT g, g % 500, g FROM generate_series(0, 999) g;

ANALYZE ex1_t1, ex1_t2, ex1_t3, ex1_t4, ex1_t5;

-- The example query: an inner-join star with a LEFT JOIN and a RIGHT JOIN
-- mixed in.
EXPLAIN (COSTS OFF)
SELECT ex1_t1.id AS t1, ex1_t2.id AS t2, ex1_t3.id AS t3,
       ex1_t4.id AS t4, ex1_t5.id AS t5
FROM ex1_t1
    JOIN ex1_t2 ON ex1_t2.id = ex1_t1.id
    JOIN ex1_t3 ON ex1_t3.id = ex1_t1.id
    LEFT JOIN ex1_t4 ON ex1_t4.id = ex1_t1.id
    RIGHT JOIN ex1_t5 ON ex1_t5.id = ex1_t1.id;

-- Show a deterministic slice of the actual result for illustration.
EXPLAIN (COSTS OFF)
SELECT ex1_t1.id AS t1, ex1_t2.id AS t2, ex1_t3.id AS t3,
       ex1_t4.id AS t4, ex1_t5.id AS t5
FROM ex1_t1
    JOIN ex1_t2 ON ex1_t2.id = ex1_t1.id
    JOIN ex1_t3 ON ex1_t3.id = ex1_t1.id
    LEFT JOIN ex1_t4 ON ex1_t4.id = ex1_t1.id
    RIGHT JOIN ex1_t5 ON ex1_t5.id = ex1_t1.id
ORDER BY t5
LIMIT 5;

-- explore a bigger part of the search space
SET lindp2.seeds = 10;
SET lindp2.adaptive = on;

EXPLAIN (COSTS OFF)
SELECT ex1_t1.id AS t1, ex1_t2.id AS t2, ex1_t3.id AS t3,
       ex1_t4.id AS t4, ex1_t5.id AS t5
FROM ex1_t1
    JOIN ex1_t2 ON ex1_t2.id = ex1_t1.id
    JOIN ex1_t3 ON ex1_t3.id = ex1_t1.id
    LEFT JOIN ex1_t4 ON ex1_t4.id = ex1_t1.id
    RIGHT JOIN ex1_t5 ON ex1_t5.id = ex1_t1.id;

EXPLAIN (COSTS OFF)
SELECT ex1_t1.id AS t1, ex1_t2.id AS t2, ex1_t3.id AS t3,
       ex1_t4.id AS t4, ex1_t5.id AS t5
FROM ex1_t1
    JOIN ex1_t2 ON ex1_t2.id = ex1_t1.id
    JOIN ex1_t3 ON ex1_t3.id = ex1_t1.id
    LEFT JOIN ex1_t4 ON ex1_t4.id = ex1_t1.id
    RIGHT JOIN ex1_t5 ON ex1_t5.id = ex1_t1.id
ORDER BY t5
LIMIT 5;

-- now try fallback, and check it produces the same plan as regular search
RESET lindp2.seeds;
RESET lindp2.adaptive;

SET lindp2.enabled = on;
SET lindp2.fallback = on;

EXPLAIN (COSTS OFF)
SELECT ex1_t1.id AS t1, ex1_t2.id AS t2, ex1_t3.id AS t3,
       ex1_t4.id AS t4, ex1_t5.id AS t5
FROM ex1_t1
    JOIN ex1_t2 ON ex1_t2.id = ex1_t1.id
    JOIN ex1_t3 ON ex1_t3.id = ex1_t1.id
    LEFT JOIN ex1_t4 ON ex1_t4.id = ex1_t1.id
    RIGHT JOIN ex1_t5 ON ex1_t5.id = ex1_t1.id;

SET lindp2.enabled = off;

EXPLAIN (COSTS OFF)
SELECT ex1_t1.id AS t1, ex1_t2.id AS t2, ex1_t3.id AS t3,
       ex1_t4.id AS t4, ex1_t5.id AS t5
FROM ex1_t1
    JOIN ex1_t2 ON ex1_t2.id = ex1_t1.id
    JOIN ex1_t3 ON ex1_t3.id = ex1_t1.id
    LEFT JOIN ex1_t4 ON ex1_t4.id = ex1_t1.id
    RIGHT JOIN ex1_t5 ON ex1_t5.id = ex1_t1.id;

DROP TABLE ex1_t1, ex1_t2, ex1_t3, ex1_t4, ex1_t5;
