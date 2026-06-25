--
-- Tests for the LinDP++ join search hook (contrib/lindp).
--
-- The hook must produce join plans that are equivalent to those of the
-- standard join search: the query results must be identical regardless of
-- which join-ordering algorithm is used.  These tests build a few join
-- queries covering inner joins, outer joins, and cross products, and check
-- that LinDP++ produces correct results and engages the hook.
--

LOAD 'lindp_hyper';

-- Make plans deterministic and force the hook to engage for small joins.
SET lindp_hyper.enabled = on;
SET lindp_hyper.min_relations = 2;
SET max_parallel_workers_per_gather = 0;
SET enable_material = off;

CREATE TABLE lindp_a (id int, b_id int, val int);
CREATE TABLE lindp_b (id int, c_id int, val int);
CREATE TABLE lindp_c (id int, d_id int, val int);
CREATE TABLE lindp_d (id int, val int);

INSERT INTO lindp_a SELECT g, g % 50, g FROM generate_series(1, 200) g;
INSERT INTO lindp_b SELECT g, g % 30, g FROM generate_series(1, 100) g;
INSERT INTO lindp_c SELECT g, g % 20, g FROM generate_series(1, 60) g;
INSERT INTO lindp_d SELECT g, g FROM generate_series(1, 40) g;

ANALYZE lindp_a;
ANALYZE lindp_b;
ANALYZE lindp_c;
ANALYZE lindp_d;

-- A chain of inner joins.
CREATE TABLE lindp_r1 AS
SELECT a.id AS aid, b.id AS bid, c.id AS cid, d.id AS did
FROM lindp_a a
  JOIN lindp_b b ON a.b_id = b.id
  JOIN lindp_c c ON b.c_id = c.id
  JOIN lindp_d d ON c.d_id = d.id;

-- The same query with the hook disabled must yield the same rows.
SET lindp_hyper.enabled = off;
CREATE TABLE lindp_r2 AS
SELECT a.id AS aid, b.id AS bid, c.id AS cid, d.id AS did
FROM lindp_a a
  JOIN lindp_b b ON a.b_id = b.id
  JOIN lindp_c c ON b.c_id = c.id
  JOIN lindp_d d ON c.d_id = d.id;
SET lindp_hyper.enabled = on;

-- No difference in either direction.
SELECT count(*) AS only_in_lindp
FROM (SELECT * FROM lindp_r1 EXCEPT SELECT * FROM lindp_r2) s;
SELECT count(*) AS only_in_standard
FROM (SELECT * FROM lindp_r2 EXCEPT SELECT * FROM lindp_r1) s;

-- Outer joins: the hyperedge decomposition must keep the join legal.
CREATE TABLE lindp_oj1 AS
SELECT a.id AS aid, b.id AS bid, c.id AS cid, d.id AS did
FROM lindp_a a
  LEFT JOIN lindp_b b ON a.b_id = b.id
  LEFT JOIN lindp_c c ON b.c_id = c.id
  JOIN lindp_d d ON a.id = d.id;

SET lindp_hyper.enabled = off;
CREATE TABLE lindp_oj2 AS
SELECT a.id AS aid, b.id AS bid, c.id AS cid, d.id AS did
FROM lindp_a a
  LEFT JOIN lindp_b b ON a.b_id = b.id
  LEFT JOIN lindp_c c ON b.c_id = c.id
  JOIN lindp_d d ON a.id = d.id;
SET lindp_hyper.enabled = on;

SELECT count(*) AS only_in_lindp
FROM (SELECT * FROM lindp_oj1 EXCEPT SELECT * FROM lindp_oj2) s;
SELECT count(*) AS only_in_standard
FROM (SELECT * FROM lindp_oj2 EXCEPT SELECT * FROM lindp_oj1) s;

-- A full join nested inside inner joins.
SELECT count(*) FROM (
  SELECT a.id
  FROM lindp_a a
    FULL JOIN lindp_b b ON a.b_id = b.id
    JOIN lindp_d d ON a.id = d.id
) s;

-- Cross product: a disconnected join graph.  With lindp_hyper.cross_products on the
-- hook handles it; the result is the full Cartesian product.
SELECT count(*) AS cross_count
FROM lindp_c c, lindp_d d;

-- With cross products disabled the hook falls back, but results are unchanged.
SET lindp_hyper.cross_products = off;
SELECT count(*) AS cross_count_fallback
FROM lindp_c c, lindp_d d;
SET lindp_hyper.cross_products = on;
-- The plan for the inner-join chain is a valid 4-way join (3 join nodes),
-- regardless of which physical join operators get chosen.
CREATE FUNCTION lindp_count_joins(query text) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
  line text;
  njoins int := 0;
BEGIN
  FOR line IN EXECUTE 'EXPLAIN (COSTS OFF) ' || query
  LOOP
    IF line ~ 'Join|Nested Loop' THEN
      njoins := njoins + 1;
    END IF;
  END LOOP;
  RETURN njoins;
END;
$$;

SELECT lindp_count_joins($q$
SELECT *
FROM lindp_a a
  JOIN lindp_b b ON a.b_id = b.id
  JOIN lindp_c c ON b.c_id = c.id
  JOIN lindp_d d ON c.d_id = d.id
$q$) AS njoins;

-- A join whose predicates all reference the same key form a single
-- equivalence class, so the planner derives a join clause between *every*
-- pair of relations: the simple-edge join graph is a clique (it has cycles),
-- not a tree.  IKKBZ requires a precedence tree, so the linearization must
-- build a spanning tree over the cycle; otherwise it loops forever.  Mixing
-- in outer joins exercises the same path through the hyperedge decomposition.
CREATE TABLE lindp_k1 (id int, val int);
CREATE TABLE lindp_k2 (id int, val int);
CREATE TABLE lindp_k3 (id int, val int);
CREATE TABLE lindp_k4 (id int, val int);
CREATE TABLE lindp_k5 (id int, val int);

INSERT INTO lindp_k1 SELECT g, g FROM generate_series(0, 199) g;
INSERT INTO lindp_k2 SELECT g, g FROM generate_series(0, 4) g;
INSERT INTO lindp_k3 SELECT g, g FROM generate_series(0, 4) g;
INSERT INTO lindp_k4 SELECT g, g FROM generate_series(0, 4) g;
INSERT INTO lindp_k5 SELECT g, g FROM generate_series(0, 199) g;

ANALYZE lindp_k1;
ANALYZE lindp_k2;
ANALYZE lindp_k3;
ANALYZE lindp_k4;
ANALYZE lindp_k5;

SET lindp_hyper.seeds = 10;

-- This query planned (and ran) without looping once the spanning tree is built.
SELECT lindp_count_joins($q$
SELECT *
FROM lindp_k1
  JOIN lindp_k2 ON lindp_k2.id = lindp_k1.id
  JOIN lindp_k3 ON lindp_k3.id = lindp_k1.id
  LEFT JOIN lindp_k4 ON lindp_k4.id = lindp_k1.id
  RIGHT JOIN lindp_k5 ON lindp_k5.id = lindp_k1.id
$q$) AS clique_njoins;

SELECT count(*) AS clique_count
FROM lindp_k1
  JOIN lindp_k2 ON lindp_k2.id = lindp_k1.id
  JOIN lindp_k3 ON lindp_k3.id = lindp_k1.id
  LEFT JOIN lindp_k4 ON lindp_k4.id = lindp_k1.id
  RIGHT JOIN lindp_k5 ON lindp_k5.id = lindp_k1.id;

RESET lindp_hyper.seeds;

-- A left-deep chain of FULL JOINs with an inner join spliced above one of
-- them.  The inner-join relation (lindp_j5) attaches to the left-hand side of a
-- full join but, because it is contributed by an enclosing join, it must be
-- joined only *after* that full join is formed.  With skewed cardinalities,
-- IKKBZ's cost surrogate is tempted to order lindp_j5 into the middle of the rigid
-- full-join chain, where no contiguous-interval parenthesization is legal.
-- The hyperedge decomposition must therefore keep each side of every full
-- join contiguous and place the enclosing relation after it; otherwise the
-- linearization fails and the hook silently falls back.
CREATE TABLE lindp_j1 (id int, x int, y int);
CREATE TABLE lindp_j2 (id int, x int, y int);
CREATE TABLE lindp_j3 (id int, x int, y int);
CREATE TABLE lindp_j4 (id int, x int, y int);
CREATE TABLE lindp_j5 (id int, x int, y int);
CREATE TABLE lindp_j6 (id int, x int, y int);

INSERT INTO lindp_j1 SELECT g, g, g FROM generate_series(1, 5)   g;
INSERT INTO lindp_j2 SELECT g, g, g FROM generate_series(1, 4)   g;
INSERT INTO lindp_j3 SELECT g, g, g FROM generate_series(1, 120) g;
INSERT INTO lindp_j4 SELECT g, g, g FROM generate_series(1, 80)  g;
INSERT INTO lindp_j5 SELECT g, g, g FROM generate_series(1, 90)  g;
INSERT INTO lindp_j6 SELECT g, g, g FROM generate_series(1, 50)  g;

ANALYZE lindp_j1;
ANALYZE lindp_j2;
ANALYZE lindp_j3;
ANALYZE lindp_j4;
ANALYZE lindp_j5;
ANALYZE lindp_j6;

SET lindp_hyper.seeds = 10;

-- A valid linearization exists (the natural left-deep order), so the hook
-- must build the 5-join plan itself rather than fall back.
SELECT lindp_count_joins($q$
SELECT *
FROM (((((lindp_j1 FULL JOIN lindp_j2 ON lindp_j2.y = lindp_j1.y)
              FULL JOIN lindp_j3 ON lindp_j3.id = lindp_j1.id)
              FULL JOIN lindp_j4 ON lindp_j4.x  = lindp_j2.x)
              JOIN      lindp_j5 ON lindp_j5.id = lindp_j3.id)
              LEFT JOIN lindp_j6 ON lindp_j6.x  = lindp_j4.x)
$q$) AS fulljoin_njoins;

SELECT count(*) AS fulljoin_count
FROM (((((lindp_j1 FULL JOIN lindp_j2 ON lindp_j2.y = lindp_j1.y)
              FULL JOIN lindp_j3 ON lindp_j3.id = lindp_j1.id)
              FULL JOIN lindp_j4 ON lindp_j4.x  = lindp_j2.x)
              JOIN      lindp_j5 ON lindp_j5.id = lindp_j3.id)
              LEFT JOIN lindp_j6 ON lindp_j6.x  = lindp_j4.x);

RESET lindp_hyper.seeds;

DROP FUNCTION lindp_count_joins(text);
DROP TABLE lindp_r1, lindp_r2, lindp_oj1, lindp_oj2;
DROP TABLE lindp_a, lindp_b, lindp_c, lindp_d;
DROP TABLE lindp_k1, lindp_k2, lindp_k3, lindp_k4, lindp_k5;
DROP TABLE lindp_j1, lindp_j2, lindp_j3, lindp_j4, lindp_j5, lindp_j6;
