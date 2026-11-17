CREATE TABLE starjoin_planning_d0 (id int PRIMARY KEY, v int);
CREATE TABLE starjoin_planning_d1 (id int PRIMARY KEY, v int);
CREATE TABLE starjoin_planning_d2 (id int PRIMARY KEY, v int);
CREATE TABLE starjoin_planning_d3 (id int PRIMARY KEY, v int);
CREATE TABLE starjoin_planning_d4 (id int PRIMARY KEY, v int);
CREATE TABLE starjoin_planning_d5 (id int PRIMARY KEY, v int);
CREATE TABLE starjoin_planning_d6 (id int PRIMARY KEY, v int);
CREATE TABLE starjoin_planning_d7 (id int PRIMARY KEY, v int);

CREATE TABLE starjoin_planning_f (id int PRIMARY KEY,
  d0 int NOT NULL REFERENCES starjoin_planning_d0,
  d1 int NOT NULL REFERENCES starjoin_planning_d1,
  d2 int NOT NULL REFERENCES starjoin_planning_d2,
  d3 int NOT NULL REFERENCES starjoin_planning_d3,
  d4 int NOT NULL REFERENCES starjoin_planning_d4,
  d5 int NOT NULL REFERENCES starjoin_planning_d5,
  d6 int NOT NULL REFERENCES starjoin_planning_d6,
  d7 int NOT NULL REFERENCES starjoin_planning_d7,
  x int);

-- A: explicit join order, sub-problem has fact + non-prefix dims
SET join_collapse_limit = 1;
EXPLAIN (COSTS OFF) SELECT * FROM starjoin_planning_d1 AS d1 JOIN (starjoin_planning_f AS f JOIN starjoin_planning_d2 AS d2 ON f.d2 = d2.id JOIN starjoin_planning_d3 AS d3 ON f.d3 = d3.id) ON f.d1 = d1.id;

-- B: explicit join order, sub-problem has dims but no fact
SET enable_starjoin_ordering = on;
EXPLAIN (COSTS OFF) SELECT * FROM starjoin_planning_f AS f JOIN (starjoin_planning_d1 AS d1 CROSS JOIN starjoin_planning_d2 AS d2) ON f.d1 = d1.id AND f.d2 = d2.id;

SET enable_starjoin_ordering = off;
EXPLAIN (COSTS OFF) SELECT * FROM starjoin_planning_f AS f JOIN (starjoin_planning_d1 AS d1 CROSS JOIN starjoin_planning_d2 AS d2) ON f.d1 = d1.id AND f.d2 = d2.id;

RESET enable_starjoin_ordering;
RESET join_collapse_limit;

-- C: default settings, 9 rels, join_collapse_limit (8) splits the problem
EXPLAIN (COSTS OFF) SELECT count(*) FROM starjoin_planning_d0 AS d0 JOIN (starjoin_planning_f AS f
  JOIN starjoin_planning_d1 AS d1 ON f.d1 = d1.id
  JOIN starjoin_planning_d2 AS d2 ON f.d2 = d2.id
  JOIN starjoin_planning_d3 AS d3 ON f.d3 = d3.id
  JOIN starjoin_planning_d4 AS d4 ON f.d4 = d4.id
  JOIN starjoin_planning_d5 AS d5 ON f.d5 = d5.id
  JOIN starjoin_planning_d6 AS d6 ON f.d6 = d6.id
  JOIN starjoin_planning_d7 AS d7 ON f.d7 = d7.id) ON f.d0 = d0.id;

-- D: default settings, view + outer dimension
CREATE VIEW starjoin_planning_star AS
SELECT f.*, d1.v AS v1, d2.v AS v2, d3.v AS v3, d4.v AS v4, d5.v AS v5, d6.v AS v6, d7.v AS v7
  FROM starjoin_planning_f AS f JOIN starjoin_planning_d1 AS d1 ON f.d1 = d1.id
                                JOIN starjoin_planning_d2 AS d2 ON f.d2 = d2.id
                                JOIN starjoin_planning_d3 AS d3 ON f.d3 = d3.id
                                JOIN starjoin_planning_d4 AS d4 ON f.d4 = d4.id
                                JOIN starjoin_planning_d5 AS d5 ON f.d5 = d5.id
                                JOIN starjoin_planning_d6 AS d6 ON f.d6 = d6.id
                                JOIN starjoin_planning_d7 AS d7 ON f.d7 = d7.id;

EXPLAIN (COSTS OFF) SELECT count(*) FROM starjoin_planning_d0 AS d0 JOIN starjoin_planning_star s ON s.d0 = d0.id;

SET enable_starjoin_ordering = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM starjoin_planning_d0 AS d0 JOIN starjoin_planning_star s ON s.d0 = d0.id;
RESET enable_starjoin_ordering;

-- E: default settings, comma FROM list with a pulled-up subquery (from_collapse_limit)
EXPLAIN (COSTS OFF) SELECT count(*) FROM starjoin_planning_f AS f, starjoin_planning_d0 AS d0,
  starjoin_planning_d1 AS d1, starjoin_planning_d2 AS d2, starjoin_planning_d3 AS d3,
  starjoin_planning_d4 AS d4, starjoin_planning_d5 AS d5,
  (SELECT d6.id AS i6, d7.id AS i7 FROM starjoin_planning_d6 AS d6, starjoin_planning_d7 AS d7) s
 WHERE f.d0 = d0.id AND f.d1 = d1.id AND f.d2 = d2.id AND f.d3 = d3.id
   AND f.d4 = d4.id AND f.d5 = d5.id AND f.d6 = s.i6 AND f.d7 = s.i7;

-- F: from_collapse_limit, the fact is joined to a dimension (with a higher
-- relid than the other dimensions) in a separate subproblem
SET from_collapse_limit = 3;
EXPLAIN (COSTS OFF) SELECT count(*) FROM starjoin_planning_d1 AS d1, starjoin_planning_d2 AS d2,
  starjoin_planning_f AS f JOIN starjoin_planning_d3 AS d3 ON f.d3 = d3.id
 WHERE f.d1 = d1.id AND f.d2 = d2.id;
RESET from_collapse_limit;

-- G: same, but with default settings (9 rels exceed from_collapse_limit)
EXPLAIN (COSTS OFF) SELECT count(*) FROM starjoin_planning_d0 AS d0,
  starjoin_planning_d1 AS d1, starjoin_planning_d2 AS d2, starjoin_planning_d3 AS d3,
  starjoin_planning_d4 AS d4, starjoin_planning_d5 AS d5, starjoin_planning_d6 AS d6,
  starjoin_planning_f AS f JOIN starjoin_planning_d7 AS d7 ON f.d7 = d7.id
 WHERE f.d0 = d0.id AND f.d1 = d1.id AND f.d2 = d2.id AND f.d3 = d3.id
   AND f.d4 = d4.id AND f.d5 = d5.id AND f.d6 = d6.id;

-- H: join_collapse_limit, right-nested explicit joins
SET join_collapse_limit = 3;
EXPLAIN (COSTS OFF) SELECT count(*) FROM starjoin_planning_d1 AS d1
  JOIN (starjoin_planning_d2 AS d2
        JOIN (starjoin_planning_f AS f
              JOIN starjoin_planning_d3 AS d3 ON f.d3 = d3.id
              JOIN starjoin_planning_d4 AS d4 ON f.d4 = d4.id)
        ON f.d2 = d2.id)
  ON f.d1 = d1.id;
RESET join_collapse_limit;

DROP VIEW starjoin_planning_star;
DROP TABLE starjoin_planning_f, starjoin_planning_d0, starjoin_planning_d1,
  starjoin_planning_d2, starjoin_planning_d3, starjoin_planning_d4,
  starjoin_planning_d5, starjoin_planning_d6, starjoin_planning_d7;
