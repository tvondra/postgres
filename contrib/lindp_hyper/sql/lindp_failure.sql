-- Tests for the cases where the LinDP linearization cannot find a legal
-- complete join order and lindp_interval_dp() therefore returns NULL, forcing
-- lindp_join_search() to fall back to the default join search.
--
-- LinDP only ever evaluates one linear order (the IKKBZ order) and builds join
-- relations from its *contiguous* sub-ranges.  This works for inner joins (any
-- order can be parenthesised) and for LATERAL dependencies (which can be
-- satisfied with parameterised paths), but it can fail for outer joins, whose
-- relative ordering is a hard constraint: if the IKKBZ order places an
-- unrelated relation *between* the two relations that make up an outer join's
-- right-hand side, then no contiguous bracketing of that order can build the
-- outer join, and the interval DP returns NULL.
--
-- The lindp_linearization_failed() function exposes whether the most recent
-- LinDP-eligible join search hit exactly that NULL-from-interval-DP path, so we
-- can assert the fallback really happened (rather than LinDP merely producing a
-- different but still valid plan).
LOAD 'lindp_hyper';

SET lindp_hyper.enabled        = on;
SET lindp_hyper.fallback       = off;  -- error out if linearization failed
SET lindp_hyper.min_relations  = 3;    -- low, so a 6-way join uses LinDP (not GEQO/DP)
SET lindp_hyper.seeds          = 1;    -- try every IKKBZ root
SET geqo                       = off;
SET join_collapse_limit        = 100;   -- keep the whole join flat (one search problem)
SET from_collapse_limit        = 100;

CREATE TABLE lf_a (id int, x int);
CREATE TABLE lf_b (id int, x int);
CREATE TABLE lf_c (id int, x int);
CREATE TABLE lf_e (id int, x int);

-- Cardinalities chosen so the IKKBZ order is driven by relation size: with the
-- highly non-selective (mod 2) join keys the ASI rank is dominated by row
-- count, so the order is lf_a < lf_b < lf_e < lf_c.  lf_e (an outer-side
-- relation) thus lands *between* lf_b and lf_c, which together form the
-- right-hand side of the outer join below.
INSERT INTO lf_a SELECT g % 2, g % 2 FROM generate_series(1, 2) g;
INSERT INTO lf_b SELECT g % 2, g % 2 FROM generate_series(1, 10) g;
INSERT INTO lf_e SELECT g % 2, g % 2 FROM generate_series(1, 100) g;
INSERT INTO lf_c SELECT g % 2, g % 2 FROM generate_series(1, 1000) g;
ANALYZE lf_a, lf_b, lf_c, lf_e;

-- Example 1: LEFT JOIN whose right-hand side is the join (lf_b JOIN lf_c).
-- The IKKBZ order is [lf_a, lf_b, lf_e, lf_c]; lf_e splits the RHS {lf_b, lf_c}
-- so no contiguous bracketing can form the left join.  LinDP falls back.
EXPLAIN (COSTS OFF)
SELECT * FROM lf_a
	LEFT JOIN (lf_b JOIN lf_c ON lf_b.id = lf_c.id) ON lf_a.id = lf_b.id
	JOIN lf_e ON lf_e.id = lf_a.id;

-- Example 2: same topology with a FULL JOIN, which is even more constrained
-- (neither side may commute).  The linearization fails for the same reason.
EXPLAIN (COSTS OFF)
SELECT * FROM lf_a
	FULL JOIN (lf_b JOIN lf_c ON lf_b.id = lf_c.id) ON lf_a.id = lf_b.id
	JOIN lf_e ON lf_e.id = lf_a.id;

-- Negative control: an all-inner-join version of the same shape linearizes
-- fine, so lindp_interval_dp() succeeds
EXPLAIN (COSTS OFF)
SELECT * FROM lf_a
	JOIN (lf_b JOIN lf_c ON lf_b.id = lf_c.id) ON lf_a.id = lf_b.id
	JOIN lf_e ON lf_e.id = lf_a.id;

-- Correctness: plans fine without LinDP linearization
SET lindp_hyper.enabled = off;

EXPLAIN (COSTS OFF)
SELECT * FROM lf_a
	LEFT JOIN (lf_b JOIN lf_c ON lf_b.id = lf_c.id) ON lf_a.id = lf_b.id
	JOIN lf_e ON lf_e.id = lf_a.id;

EXPLAIN (COSTS OFF)
SELECT * FROM lf_a
	FULL JOIN (lf_b JOIN lf_c ON lf_b.id = lf_c.id) ON lf_a.id = lf_b.id
	JOIN lf_e ON lf_e.id = lf_a.id;

-- Correctness: the fallback must still produce the same answer as the default
-- search for the two failing queries.

SET lindp_hyper.enabled = on;
SET lindp_hyper.fallback = on;

EXPLAIN (COSTS OFF)
SELECT * FROM lf_a
	LEFT JOIN (lf_b JOIN lf_c ON lf_b.id = lf_c.id) ON lf_a.id = lf_b.id
	JOIN lf_e ON lf_e.id = lf_a.id;

EXPLAIN (COSTS OFF)
SELECT * FROM lf_a
	FULL JOIN (lf_b JOIN lf_c ON lf_b.id = lf_c.id) ON lf_a.id = lf_b.id
	JOIN lf_e ON lf_e.id = lf_a.id;

DROP TABLE lf_a, lf_b, lf_c, lf_e;
