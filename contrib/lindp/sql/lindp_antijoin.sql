-- Another linearization-failure case for LinDP, this time *without* any outer
-- join in the SQL text and without LATERAL: a plain NOT EXISTS subquery.
--
-- A NOT EXISTS (or EXISTS) subquery is flattened by the planner into an
-- anti-join (semi-join), which carries the very same rigid ordering constraint
-- as an outer join: the anti-join's right-hand side must be formed as a single
-- unit and joined as a whole, it cannot commute with or be interleaved by
-- unrelated relations.  LinDP only ever evaluates one linear order (the IKKBZ
-- order) and builds join relations from its *contiguous* sub-ranges, so if that
-- order wedges an unrelated relation between the two relations that make up the
-- anti-join's right-hand side, no contiguous bracketing can build the anti-join
-- and lindp_interval_dp() returns NULL, forcing the fall back to the default
-- join search.
--
-- This complements lindp_fallback.sql (which uses explicit LEFT/FULL JOINs):
-- it shows the same linearization limitation arising from a query that contains
-- no JOIN keyword for the problematic join at all, and uses no LATERAL.
LOAD 'lindp';

SET lindp.enabled        = on;
SET lindp.fallback       = off;  -- error out if linearization failed
SET lindp.min_relations  = 3;    -- low, so a 6-way join uses LinDP (not GEQO/DP)
SET lindp.seeds          = 1;    -- try every IKKBZ root
SET geqo                       = off;
SET join_collapse_limit        = 100;   -- keep the whole join flat (one search problem)
SET from_collapse_limit        = 100;

CREATE TABLE aj_a (id int, x int);
CREATE TABLE aj_b (id int, x int);
CREATE TABLE aj_c (id int, x int);
CREATE TABLE aj_e (id int, x int);

-- Indexes on the join keys (part of the self-contained schema; they also give
-- the planner a non-trivial set of paths to cost).
CREATE INDEX aj_a_id ON aj_a (id);
CREATE INDEX aj_b_id ON aj_b (id);
CREATE INDEX aj_c_id ON aj_c (id);
CREATE INDEX aj_e_id ON aj_e (id);

-- Cardinalities chosen so the IKKBZ order is driven by relation size: with the
-- highly non-selective (mod 2) join keys the ASI rank is dominated by row
-- count, so the order is aj_a < aj_b < aj_e < aj_c.  aj_e (joined only to the
-- driving relation aj_a) thus lands *between* aj_b and aj_c, which together form
-- the right-hand side of the anti-join below.
INSERT INTO aj_a SELECT g % 2, g % 2 FROM generate_series(1, 2) g;
INSERT INTO aj_b SELECT g % 2, g % 2 FROM generate_series(1, 10) g;
INSERT INTO aj_e SELECT g % 2, g % 2 FROM generate_series(1, 100) g;
INSERT INTO aj_c SELECT g % 2, g % 2 FROM generate_series(1, 1000) g;
ANALYZE aj_a, aj_b, aj_c, aj_e;

-- The failing query: NOT EXISTS becomes an anti-join whose right-hand side is
-- the join (aj_b JOIN aj_c).  The IKKBZ order is [aj_a, aj_b, aj_e, aj_c]; aj_e
-- splits the anti-join RHS {aj_b, aj_c}, so no contiguous bracketing can form
-- the anti-join.  LinDP falls back to the default search.
EXPLAIN (COSTS OFF)
SELECT * FROM aj_a JOIN aj_e ON aj_e.id = aj_a.id
WHERE NOT EXISTS (SELECT 1 FROM aj_b JOIN aj_c ON aj_b.id = aj_c.id
				  WHERE aj_b.id = aj_a.id);

-- The same shape with EXISTS (a semi-join) fails for the same reason.
EXPLAIN (COSTS OFF)
SELECT * FROM aj_a JOIN aj_e ON aj_e.id = aj_a.id
WHERE EXISTS (SELECT 1 FROM aj_b JOIN aj_c ON aj_b.id = aj_c.id
			  WHERE aj_b.id = aj_a.id);

-- Negative control: an all-inner-join version of the same shape linearizes
-- fine, so lindp_interval_dp() succeeds and the flag is false.
EXPLAIN (COSTS OFF)
SELECT * FROM aj_a
	JOIN aj_e ON aj_e.id = aj_a.id
	JOIN (aj_b JOIN aj_c ON aj_b.id = aj_c.id) ON aj_b.id = aj_a.id;

-- Correctness: the fallback must still produce the same answer as the default
-- search for the failing anti-join query.
SET lindp.fallback = on;
SET lindp.enabled = on;

EXPLAIN (COSTS OFF)
SELECT * FROM aj_a JOIN aj_e ON aj_e.id = aj_a.id
WHERE NOT EXISTS (SELECT 1 FROM aj_b JOIN aj_c ON aj_b.id = aj_c.id
				  WHERE aj_b.id = aj_a.id);

EXPLAIN (COSTS OFF)
SELECT * FROM aj_a JOIN aj_e ON aj_e.id = aj_a.id
WHERE EXISTS (SELECT 1 FROM aj_b JOIN aj_c ON aj_b.id = aj_c.id
			  WHERE aj_b.id = aj_a.id);

SET lindp.enabled = off;

EXPLAIN (COSTS OFF)
SELECT * FROM aj_a JOIN aj_e ON aj_e.id = aj_a.id
WHERE NOT EXISTS (SELECT 1 FROM aj_b JOIN aj_c ON aj_b.id = aj_c.id
				  WHERE aj_b.id = aj_a.id);

EXPLAIN (COSTS OFF)
SELECT * FROM aj_a JOIN aj_e ON aj_e.id = aj_a.id
WHERE EXISTS (SELECT 1 FROM aj_b JOIN aj_c ON aj_b.id = aj_c.id
			  WHERE aj_b.id = aj_a.id);

DROP TABLE aj_a, aj_b, aj_c, aj_e;
