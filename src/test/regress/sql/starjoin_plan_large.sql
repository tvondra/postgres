CREATE TABLE starjoin_plan_p1 (id int PRIMARY KEY, v int, w int);
CREATE TABLE starjoin_plan_p2 (id int PRIMARY KEY, v int, w int);
CREATE TABLE starjoin_plan_p3 (id int PRIMARY KEY, v int, w int);

INSERT INTO starjoin_plan_p1 SELECT i, i % 100, i FROM generate_series(1,100000) i;
INSERT INTO starjoin_plan_p2 SELECT i, i % 100, i FROM generate_series(1,100000) i;
INSERT INTO starjoin_plan_p3 SELECT i, i % 100, i FROM generate_series(1,100000) i;

CREATE TABLE starjoin_plan_pf (id int PRIMARY KEY,
  d1 int NOT NULL REFERENCES starjoin_plan_p1,
  d2 int NOT NULL REFERENCES starjoin_plan_p2,
  d3 int NOT NULL REFERENCES starjoin_plan_p3,
  x int);

INSERT INTO starjoin_plan_pf SELECT i, 1 + (i * 7) % 100000, 1 + (i * 13) % 100000, 1 + (i * 17) % 100000, i % 1000
  FROM generate_series(1,2000000) i;

VACUUM ANALYZE starjoin_plan_p1, starjoin_plan_p2, starjoin_plan_p3, starjoin_plan_pf;

SET max_parallel_workers_per_gather = 0;

-- Q1: extra (non-FK) join qual between fact and the last dimension
SET enable_starjoin_ordering = on;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM starjoin_plan_pf AS pf
  JOIN starjoin_plan_p1 AS p1 ON pf.d1 = p1.id
  JOIN starjoin_plan_p2 AS p2 ON pf.d2 = p2.id
  JOIN starjoin_plan_p3 AS p3 ON pf.d3 = p3.id AND pf.x = p3.w;

SET enable_starjoin_ordering = off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM starjoin_plan_pf AS pf
  JOIN starjoin_plan_p1 AS p1 ON pf.d1 = p1.id
  JOIN starjoin_plan_p2 AS p2 ON pf.d2 = p2.id
  JOIN starjoin_plan_p3 AS p3 ON pf.d3 = p3.id AND pf.x = p3.w;

-- Q2: OR clause from which extract_restriction_or_clauses() derives a restriction on p3
SET enable_starjoin_ordering = on;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM starjoin_plan_pf AS pf
  JOIN starjoin_plan_p1 AS p1 ON pf.d1 = p1.id
  JOIN starjoin_plan_p2 AS p2 ON pf.d2 = p2.id
  JOIN starjoin_plan_p3 AS p3 ON pf.d3 = p3.id
 WHERE (p3.w = 1 AND pf.x = 1) OR (p3.w = 2 AND pf.x = 2);

SET enable_starjoin_ordering = off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM starjoin_plan_pf AS pf
  JOIN starjoin_plan_p1 AS p1 ON pf.d1 = p1.id
  JOIN starjoin_plan_p2 AS p2 ON pf.d2 = p2.id
  JOIN starjoin_plan_p3 AS p3 ON pf.d3 = p3.id
 WHERE (p3.w = 1 AND pf.x = 1) OR (p3.w = 2 AND pf.x = 2);

DROP TABLE starjoin_plan_pf, starjoin_plan_p1, starjoin_plan_p2, starjoin_plan_p3;
