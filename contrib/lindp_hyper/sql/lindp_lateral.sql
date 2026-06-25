-- A linearization-failure case for LinDP that uses a LATERAL join.
--
-- LinDP only ever evaluates one linear order (the IKKBZ order) and builds join
-- relations from its *contiguous* sub-ranges.  A plain LATERAL dependency on a
-- single relation never defeats this, because make_join_rel() can satisfy it
-- with a parameterized path (the referenced value is passed in from an outer
-- relation that is joined later).  The interesting case is a LATERAL derived
-- table on the right-hand side of an *outer* join: the LEFT JOIN makes the whole
-- correlated subquery a rigid unit that must be joined as a block, exactly like
-- any other outer-join right-hand side.  If the IKKBZ order then wedges an
-- unrelated relation into the middle of that block, no contiguous bracketing of
-- the order can form the outer join and lindp_interval_dp() returns NULL,
-- forcing the fall back to the default join search.
--
-- This complements lindp_fallback.sql (explicit LEFT/FULL JOIN of a plain
-- sub-select) and lindp_antijoin.sql (NOT EXISTS / anti-join): here the
-- right-hand side of the outer join is a LATERAL-correlated derived table
-- (LEFT JOIN LATERAL (... WHERE ll_b.id = ll_a.id) ON true), so the query
-- genuinely depends on LATERAL.
LOAD 'lindp_hyper';

SET lindp_hyper.enabled        = on;
SET lindp_hyper.fallback       = off;  -- error out if linearization failed
SET lindp_hyper.min_relations  = 3;    -- low, so a 6-way join uses LinDP (not GEQO/DP)
SET lindp_hyper.seeds          = 1;    -- try every IKKBZ root
SET geqo                       = off;
SET join_collapse_limit        = 100;   -- keep the whole join flat (one search problem)
SET from_collapse_limit        = 100;

CREATE TABLE ll_a (id int, x int);
CREATE TABLE ll_b (id int, x int);
CREATE TABLE ll_c (id int, x int);
CREATE TABLE ll_e (id int, x int);

-- Indexes on the join keys (part of the self-contained schema; they also give
-- the planner a non-trivial set of paths to cost).
CREATE INDEX ll_a_id ON ll_a (id);
CREATE INDEX ll_b_id ON ll_b (id);
CREATE INDEX ll_c_id ON ll_c (id);
CREATE INDEX ll_e_id ON ll_e (id);

-- Cardinalities chosen so the IKKBZ order is driven by relation size: with the
-- highly non-selective (mod 2) join keys the ASI rank is dominated by row
-- count, so the order is ll_a < ll_b < ll_e < ll_c.  ll_e (joined only to the
-- driving relation ll_a) thus lands *between* ll_b and ll_c, which together form
-- the right-hand side of the LATERAL left join below.
INSERT INTO ll_a SELECT g % 2, g % 2 FROM generate_series(1, 2) g;
INSERT INTO ll_b SELECT g % 2, g % 2 FROM generate_series(1, 10) g;
INSERT INTO ll_e SELECT g % 2, g % 2 FROM generate_series(1, 100) g;
INSERT INTO ll_c SELECT g % 2, g % 2 FROM generate_series(1, 1000) g;
ANALYZE ll_a, ll_b, ll_c, ll_e;

-- The failing query: a LEFT JOIN LATERAL whose correlated derived table is the
-- join (ll_b JOIN ll_c), so the outer join's right-hand side is {ll_b, ll_c}.
-- The IKKBZ order is [ll_a, ll_b, ll_e, ll_c]; ll_e splits the right-hand side
-- {ll_b, ll_c}, so no contiguous bracketing can form the left join.  LinDP
-- falls back to the default search.
EXPLAIN (COSTS OFF)
SELECT * FROM ll_a
	LEFT JOIN LATERAL (SELECT ll_b.id AS bid, ll_c.x AS cx
					   FROM ll_b JOIN ll_c ON ll_b.id = ll_c.id
					   WHERE ll_b.id = ll_a.id) ss ON true
	JOIN ll_e ON ll_e.id = ll_a.id;

-- Negative control: the same shape with an *inner* LATERAL join linearizes
-- fine (the correlated derived table is no longer a rigid outer-join block, so
-- any contiguous bracketing of the order is legal), so the flag is false.
EXPLAIN (COSTS OFF)
SELECT * FROM ll_a
	JOIN LATERAL (SELECT ll_b.id AS bid, ll_c.x AS cx
				  FROM ll_b JOIN ll_c ON ll_b.id = ll_c.id
				  WHERE ll_b.id = ll_a.id) ss ON true
	JOIN ll_e ON ll_e.id = ll_a.id;

-- Correctness: the fallback must still produce the same answer as the default
-- search for the failing LATERAL query.

SET lindp_hyper.enabled = on;
SET lindp_hyper.fallback = on;

EXPLAIN (COSTS OFF)
SELECT * FROM ll_a
	LEFT JOIN LATERAL (SELECT ll_b.id AS bid, ll_c.x AS cx
					   FROM ll_b JOIN ll_c ON ll_b.id = ll_c.id
					   WHERE ll_b.id = ll_a.id) ss ON true
	JOIN ll_e ON ll_e.id = ll_a.id;

SET lindp_hyper.enabled = off;

EXPLAIN (COSTS OFF)
SELECT * FROM ll_a
	LEFT JOIN LATERAL (SELECT ll_b.id AS bid, ll_c.x AS cx
					   FROM ll_b JOIN ll_c ON ll_b.id = ll_c.id
					   WHERE ll_b.id = ll_a.id) ss ON true
	JOIN ll_e ON ll_e.id = ll_a.id;

DROP TABLE ll_a, ll_b, ll_c, ll_e;
