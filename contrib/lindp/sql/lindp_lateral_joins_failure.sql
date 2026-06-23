-- ===========================================================================
-- A 7-relation query mixing FULL JOINs and a LATERAL join whose flattened
-- 6-way sub-problem the lindp join_search_hook cannot linearize, so it falls
-- back to standard_join_search.  Run against a server with the `lindp` module.
-- ===========================================================================
LOAD 'lindp';
SET lindp.enabled       = on;
SET lindp.fallback      = off;
SET lindp.min_threshold = 3;     -- low, so the 6-way search uses LinDP
SET lindp.effort        = -1;
SET geqo                = off;
SET join_collapse_limit = 100;   -- keep the join tree flat (one search)
SET from_collapse_limit = 100;

DROP TABLE IF EXISTS la, lb, lc, ld, le, lf, lg CASCADE;
CREATE TABLE la (id int, x int, y int);
CREATE TABLE lb (id int, x int, y int);
CREATE TABLE lc (id int, x int, y int);
CREATE TABLE ld (id int, x int, y int);
CREATE TABLE le (id int, x int, y int);
CREATE TABLE lf (id int, x int, y int);
CREATE TABLE lg (id int, x int, y int);

-- Skewed cardinalities make IKKBZ's C_out order disagree with the order the
-- FULL JOIN chain forces.
INSERT INTO la SELECT g,g,g FROM generate_series(1,5)      g;
INSERT INTO lb SELECT g,g,g FROM generate_series(1,80000)  g;
INSERT INTO lc SELECT g,g,g FROM generate_series(1,90000)  g;
INSERT INTO ld SELECT g,g,g FROM generate_series(1,6)      g;
INSERT INTO le SELECT g,g,g FROM generate_series(1,120000) g;
INSERT INTO lf SELECT g,g,g FROM generate_series(1,3)      g;
INSERT INTO lg SELECT g,g,g FROM generate_series(1,4)      g;
ANALYZE;

-- The LATERAL join (ld correlated to lc) sits inside a left-deep chain of
-- FULL JOINs.  The whole left-deep tree flattens into ONE 6-way join-search
-- problem; the outer-join ordering constraints make the IKKBZ linear order
-- non-realizable as contiguous intervals, so lindp_interval_dp() returns NULL
-- and the module falls back to standard_join_search.
EXPLAIN (COSTS OFF)
SELECT *
FROM (((((( la FULL JOIN lb ON lb.y = la.y)
               FULL JOIN lc ON lc.x = la.x)
               JOIN LATERAL (SELECT * FROM ld WHERE ld.x = lc.x) ld ON true)
               FULL JOIN le ON le.y = lb.y)
               JOIN lf ON lf.x = le.x)
               FULL JOIN lg ON lg.y = lf.y);

-- should plain without lindp
SET lindp.enabled = off;

EXPLAIN (COSTS OFF)
SELECT *
FROM (((((( la FULL JOIN lb ON lb.y = la.y)
               FULL JOIN lc ON lc.x = la.x)
               JOIN LATERAL (SELECT * FROM ld WHERE ld.x = lc.x) ld ON true)
               FULL JOIN le ON le.y = lb.y)
               JOIN lf ON lf.x = le.x)
               FULL JOIN lg ON lg.y = lf.y);

-- Correctness: LinDP with fallback plan matches the default search (both return 4).
SET lindp.enabled = on;
SET lindp.fallback = on;

EXPLAIN (COSTS OFF)
SELECT *
FROM (((((( la FULL JOIN lb ON lb.y = la.y)
               FULL JOIN lc ON lc.x = la.x)
               JOIN LATERAL (SELECT * FROM ld WHERE ld.x = lc.x) ld ON true)
               FULL JOIN le ON le.y = lb.y)
               JOIN lf ON lf.x = le.x)
               FULL JOIN lg ON lg.y = lf.y);

SET lindp.enabled = off;

EXPLAIN (COSTS OFF)
SELECT *
FROM (((((( la FULL JOIN lb ON lb.y = la.y)
               FULL JOIN lc ON lc.x = la.x)
               JOIN LATERAL (SELECT * FROM ld WHERE ld.x = lc.x) ld ON true)
               FULL JOIN le ON le.y = lb.y)
               JOIN lf ON lf.x = le.x)
               FULL JOIN lg ON lg.y = lf.y);

DROP TABLE la, lb, lc, ld, le, lf, lg CASCADE;
