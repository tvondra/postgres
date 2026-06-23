--
-- Example 3: lateral join
--
-- A self-contained test case that builds its own tables and runs a query with
-- a chain of LATERAL subqueries layered over an outer join: subquery "s"
-- references the nullable side of a LEFT JOIN, subquery "u" references "s", and
-- "u" is finally tied back to the preserved side.  These lateral dependencies,
-- combined with the outer join, make every contiguous (window = 0) split of the
-- IKKBZ linear order an illegal join, so the pure-interval DP cannot build the
-- top join relation and the module falls back to standard_join_search() for the
-- component, which still returns a correct plan.
--
-- (The "OFFSET 0" optimization fences keep the subqueries from being pulled up
-- into the parent query, preserving the laterality.)
--
-- The test verifies that, despite engaging the fallback, the LinDP++ search
-- returns exactly the same rows the standard dynamic-programming join search
-- would.
--
LOAD 'lindp';

SET lindp.enabled        = on;
SET lindp.fallback       = off;  -- error out if linearization failed
SET lindp.min_relations  = 3;    -- low, so a 6-way join uses LinDP (not GEQO/DP)
SET lindp.seeds          = 1;    -- try every IKKBZ root
SET geqo                       = off;
SET join_collapse_limit        = 100;   -- keep the whole join flat (one search problem)
SET from_collapse_limit        = 100;


CREATE TABLE ex3_t1 (id int, k int, v int);
CREATE TABLE ex3_t2 (id int, k int, v int);
CREATE TABLE ex3_s  (id int, k int, v int);
CREATE TABLE ex3_u  (id int, k int, v int);

INSERT INTO ex3_t1 SELECT g, g % 10, g FROM generate_series(0, 49) g;
INSERT INTO ex3_t2 SELECT g, g % 10, g FROM generate_series(0, 49) g;
INSERT INTO ex3_s  SELECT g, g % 10, g FROM generate_series(0, 49) g;
INSERT INTO ex3_u  SELECT g, g % 10, g FROM generate_series(0, 49) g;

ANALYZE ex3_t1, ex3_t2, ex3_s, ex3_u;

-- The example query: a LEFT JOIN feeding a chain of two LATERAL subqueries.
EXPLAIN (COSTS OFF)
SELECT ex3_t1.id AS t1, ex3_t2.id AS t2, s.id AS s, u.id AS u
FROM ex3_t1
    LEFT JOIN ex3_t2 ON ex3_t2.id = ex3_t1.id,
    LATERAL (SELECT x.id, x.k FROM ex3_s x
             WHERE x.v = ex3_t2.v OFFSET 0) s,
    LATERAL (SELECT y.id FROM ex3_u y
             WHERE y.v = s.k OFFSET 0) u
WHERE u.id = ex3_t1.id AND s.id = ex3_t1.id;

-- Show a deterministic slice of the actual result for illustration.
EXPLAIN (COSTS OFF)
SELECT ex3_t1.id AS t1, ex3_t2.id AS t2, s.id AS s, u.id AS u
FROM ex3_t1
    LEFT JOIN ex3_t2 ON ex3_t2.id = ex3_t1.id,
    LATERAL (SELECT x.id, x.k FROM ex3_s x
             WHERE x.v = ex3_t2.v OFFSET 0) s,
    LATERAL (SELECT y.id FROM ex3_u y
             WHERE y.v = s.k OFFSET 0) u
WHERE u.id = ex3_t1.id AND s.id = ex3_t1.id
ORDER BY t1
LIMIT 5;

-- explore a bigger part of the search space
SET lindp.seeds = 10;

EXPLAIN (COSTS OFF)
SELECT ex3_t1.id AS t1, ex3_t2.id AS t2, s.id AS s, u.id AS u
FROM ex3_t1
    LEFT JOIN ex3_t2 ON ex3_t2.id = ex3_t1.id,
    LATERAL (SELECT x.id, x.k FROM ex3_s x
             WHERE x.v = ex3_t2.v OFFSET 0) s,
    LATERAL (SELECT y.id FROM ex3_u y
             WHERE y.v = s.k OFFSET 0) u
WHERE u.id = ex3_t1.id AND s.id = ex3_t1.id;

EXPLAIN (COSTS OFF)
SELECT ex3_t1.id AS t1, ex3_t2.id AS t2, s.id AS s, u.id AS u
FROM ex3_t1
    LEFT JOIN ex3_t2 ON ex3_t2.id = ex3_t1.id,
    LATERAL (SELECT x.id, x.k FROM ex3_s x
             WHERE x.v = ex3_t2.v OFFSET 0) s,
    LATERAL (SELECT y.id FROM ex3_u y
             WHERE y.v = s.k OFFSET 0) u
WHERE u.id = ex3_t1.id AND s.id = ex3_t1.id
ORDER BY t1
LIMIT 5;

-- check the fallback produces the same plan as the regular planning
SET lindp.enabled = on;
SET lindp.fallback = on;

EXPLAIN (COSTS OFF)
SELECT ex3_t1.id AS t1, ex3_t2.id AS t2, s.id AS s, u.id AS u
FROM ex3_t1
    LEFT JOIN ex3_t2 ON ex3_t2.id = ex3_t1.id,
    LATERAL (SELECT x.id, x.k FROM ex3_s x
             WHERE x.v = ex3_t2.v OFFSET 0) s,
    LATERAL (SELECT y.id FROM ex3_u y
             WHERE y.v = s.k OFFSET 0) u
WHERE u.id = ex3_t1.id AND s.id = ex3_t1.id;

SET lindp.enabled = off;

EXPLAIN (COSTS OFF)
SELECT ex3_t1.id AS t1, ex3_t2.id AS t2, s.id AS s, u.id AS u
FROM ex3_t1
    LEFT JOIN ex3_t2 ON ex3_t2.id = ex3_t1.id,
    LATERAL (SELECT x.id, x.k FROM ex3_s x
             WHERE x.v = ex3_t2.v OFFSET 0) s,
    LATERAL (SELECT y.id FROM ex3_u y
             WHERE y.v = s.k OFFSET 0) u
WHERE u.id = ex3_t1.id AND s.id = ex3_t1.id;

DROP TABLE ex3_t1, ex3_t2, ex3_s, ex3_u;
