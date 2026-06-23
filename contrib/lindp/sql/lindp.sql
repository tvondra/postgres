-- Tests for the lindp (IKKBZ + LinDP) join search prototype.
LOAD 'lindp';

-- Use a low threshold so small join problems exercise LinDP, and keep the
-- search deterministic and easy to read.
SET lindp.min_threshold = 3;
SET lindp.effort = 10;
SET geqo = off;
SET join_collapse_limit = 100;
SET from_collapse_limit = 100;

CREATE TABLE lindp_t0 (a int, b int);
CREATE TABLE lindp_t1 (a int, b int);
CREATE TABLE lindp_t2 (a int, b int);
CREATE TABLE lindp_t3 (a int, b int);
CREATE TABLE lindp_t4 (a int, b int);

-- Chain query: t0 - t1 - t2 - t3 - t4
EXPLAIN (COSTS OFF)
SELECT * FROM lindp_t0, lindp_t1, lindp_t2, lindp_t3, lindp_t4
WHERE lindp_t0.a = lindp_t1.a
  AND lindp_t1.b = lindp_t2.a
  AND lindp_t2.b = lindp_t3.a
  AND lindp_t3.b = lindp_t4.a;

-- Star query: t0 in the center joined to all others
EXPLAIN (COSTS OFF)
SELECT * FROM lindp_t0, lindp_t1, lindp_t2, lindp_t3, lindp_t4
WHERE lindp_t0.a = lindp_t1.a
  AND lindp_t0.a = lindp_t2.a
  AND lindp_t0.a = lindp_t3.a
  AND lindp_t0.a = lindp_t4.a;

-- Clique-ish query: many edges (cyclic)
EXPLAIN (COSTS OFF)
SELECT * FROM lindp_t0, lindp_t1, lindp_t2, lindp_t3
WHERE lindp_t0.a = lindp_t1.a
  AND lindp_t1.a = lindp_t2.a
  AND lindp_t2.a = lindp_t3.a
  AND lindp_t3.a = lindp_t0.a
  AND lindp_t0.b = lindp_t2.b;

-- Cross product (disconnected join graph): t0,t1 connected, t2 separate
EXPLAIN (COSTS OFF)
SELECT * FROM lindp_t0, lindp_t1, lindp_t2
WHERE lindp_t0.a = lindp_t1.a;

-- Outer joins: legal order is constrained; LinDP must produce a valid plan
EXPLAIN (COSTS OFF)
SELECT * FROM lindp_t0
LEFT JOIN lindp_t1 ON lindp_t0.a = lindp_t1.a
LEFT JOIN lindp_t2 ON lindp_t1.b = lindp_t2.a
LEFT JOIN lindp_t3 ON lindp_t2.b = lindp_t3.a;

-- Determinism: planning the same query twice yields the same plan (the two
-- EXPLAIN outputs below must be identical).
EXPLAIN (COSTS OFF)
SELECT * FROM lindp_t0, lindp_t1, lindp_t2, lindp_t3, lindp_t4
WHERE lindp_t0.a = lindp_t1.a
  AND lindp_t1.b = lindp_t2.a
  AND lindp_t2.b = lindp_t3.a
  AND lindp_t3.b = lindp_t4.a;
EXPLAIN (COSTS OFF)
SELECT * FROM lindp_t0, lindp_t1, lindp_t2, lindp_t3, lindp_t4
WHERE lindp_t0.a = lindp_t1.a
  AND lindp_t1.b = lindp_t2.a
  AND lindp_t2.b = lindp_t3.a
  AND lindp_t3.b = lindp_t4.a;

-- Correctness vs default search: results must match regardless of optimizer.
INSERT INTO lindp_t0 SELECT g, g % 5 FROM generate_series(1, 50) g;
INSERT INTO lindp_t1 SELECT g, g % 5 FROM generate_series(1, 50) g;
INSERT INTO lindp_t2 SELECT g, g % 5 FROM generate_series(1, 50) g;
INSERT INTO lindp_t3 SELECT g, g % 5 FROM generate_series(1, 50) g;
ANALYZE lindp_t0, lindp_t1, lindp_t2, lindp_t3;

SET lindp.enabled = on;
CREATE TEMP TABLE lindp_res_on AS
SELECT count(*) AS c, sum(lindp_t0.a) AS s
FROM lindp_t0, lindp_t1, lindp_t2, lindp_t3
WHERE lindp_t0.a = lindp_t1.a
  AND lindp_t1.a = lindp_t2.a
  AND lindp_t2.a = lindp_t3.a;

SET lindp.enabled = off;
CREATE TEMP TABLE lindp_res_off AS
SELECT count(*) AS c, sum(lindp_t0.a) AS s
FROM lindp_t0, lindp_t1, lindp_t2, lindp_t3
WHERE lindp_t0.a = lindp_t1.a
  AND lindp_t1.a = lindp_t2.a
  AND lindp_t2.a = lindp_t3.a;

SELECT (a.c = b.c AND a.s = b.s) AS results_match
FROM lindp_res_on a, lindp_res_off b;

DROP TABLE lindp_t0, lindp_t1, lindp_t2, lindp_t3, lindp_t4;
