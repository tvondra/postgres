-- Another linearization-failure case for LinDP, built from a *combination* of
-- an OUTER join and a LATERAL join: a LEFT JOIN LATERAL whose correlated
-- derived table itself contains an inner LATERAL join.
--
-- LinDP only ever evaluates one linear order (the IKKBZ order) and builds join
-- relations from its *contiguous* sub-ranges.  A plain LATERAL dependency never
-- defeats this on its own, because make_join_rel() can satisfy it with a
-- parameterized path.  The interesting case is a LATERAL derived table on the
-- right-hand side of an *outer* join: the LEFT JOIN makes the whole correlated
-- subquery a rigid unit that must be joined as a block.  Here that block is
-- itself a two-relation LATERAL join (nl_b cross-correlated with nl_c via an
-- inner LATERAL), so the query genuinely depends on *both* an outer join and a
-- nested LATERAL.  If the IKKBZ order wedges an unrelated relation into the
-- middle of that {nl_b, nl_c} block, no contiguous bracketing of the order can
-- form the outer join and lindp_interval_dp() returns NULL, forcing the fall
-- back to the default join search.
--
-- This complements lindp_lateral.sql, where the right-hand side of the LEFT
-- JOIN LATERAL is a plain inner JOIN; here the right-hand side is a nested
-- LATERAL join, exercising the OUTER+LATERAL combination.  Using an inner
-- LATERAL (rather than an inner JOIN) inside the block keeps the same
-- cardinality estimates, so the IKKBZ order is still driven by relation size.
LOAD 'lindp_hyper';

SET lindp_hyper.enabled        = on;
SET lindp_hyper.fallback       = off;  -- error out if linearization failed
SET lindp_hyper.min_relations  = 3;    -- low, so a 6-way join uses LinDP (not GEQO/DP)
SET lindp_hyper.seeds          = 1;    -- try every IKKBZ root
SET geqo                       = off;
SET join_collapse_limit        = 100;   -- keep the whole join flat (one search problem)
SET from_collapse_limit        = 100;

CREATE TABLE nl_a (id int, x int);
CREATE TABLE nl_b (id int, x int);
CREATE TABLE nl_c (id int, x int);
CREATE TABLE nl_e (id int, x int);

-- Indexes on the join keys (part of the self-contained schema; they also give
-- the planner a non-trivial set of paths to cost).
CREATE INDEX nl_a_id ON nl_a (id);
CREATE INDEX nl_b_id ON nl_b (id);
CREATE INDEX nl_c_id ON nl_c (id);
CREATE INDEX nl_e_id ON nl_e (id);

-- Cardinalities chosen so the IKKBZ order is driven by relation size: with the
-- highly non-selective (mod 2) join keys the ASI rank is dominated by row
-- count, so the order is nl_a < nl_b < nl_e < nl_c.  nl_e (joined only to the
-- driving relation nl_a) thus lands *between* nl_b and nl_c, which together
-- form the right-hand side of the nested LATERAL left join below.
INSERT INTO nl_a SELECT g % 2, g % 2 FROM generate_series(1, 2) g;
INSERT INTO nl_b SELECT g % 2, g % 2 FROM generate_series(1, 10) g;
INSERT INTO nl_e SELECT g % 2, g % 2 FROM generate_series(1, 100) g;
INSERT INTO nl_c SELECT g % 2, g % 2 FROM generate_series(1, 1000) g;
ANALYZE nl_a, nl_b, nl_c, nl_e;

-- The failing query: a LEFT JOIN LATERAL whose correlated derived table is a
-- nested LATERAL join (nl_b cross-correlated with nl_c), so the outer join's
-- right-hand side is {nl_b, nl_c}.  The IKKBZ order is [nl_a, nl_b, nl_e,
-- nl_c]; nl_e splits the right-hand side {nl_b, nl_c}, so no contiguous
-- bracketing can form the left join.  LinDP falls back to the default search.
EXPLAIN (COSTS OFF)
SELECT * FROM nl_a
	LEFT JOIN LATERAL (SELECT nl_b.id AS bid, ss2.cx
					   FROM nl_b,
							LATERAL (SELECT nl_c.x AS cx FROM nl_c
									 WHERE nl_c.id = nl_b.id) ss2
					   WHERE nl_b.id = nl_a.id) ss ON true
	JOIN nl_e ON nl_e.id = nl_a.id;

-- Negative control: the same shape with an *inner* LATERAL join (no outer join)
-- linearizes fine, because the correlated derived table is no longer a rigid
-- outer-join block, so any contiguous bracketing of the order is legal and the
-- flag is false.
EXPLAIN (COSTS OFF)
SELECT * FROM nl_a
	JOIN LATERAL (SELECT nl_b.id AS bid, ss2.cx
				  FROM nl_b,
					   LATERAL (SELECT nl_c.x AS cx FROM nl_c
								WHERE nl_c.id = nl_b.id) ss2
				  WHERE nl_b.id = nl_a.id) ss ON true
	JOIN nl_e ON nl_e.id = nl_a.id;

-- Correctness: the fallback must still produce the same answer as the default
-- search for the failing nested-LATERAL query.
SET lindp_hyper.enabled = on;
SET lindp_hyper.fallback = on;

EXPLAIN (COSTS OFF)
SELECT * FROM nl_a
	LEFT JOIN LATERAL (SELECT nl_b.id AS bid, ss2.cx
					   FROM nl_b,
							LATERAL (SELECT nl_c.x AS cx FROM nl_c
									 WHERE nl_c.id = nl_b.id) ss2
					   WHERE nl_b.id = nl_a.id) ss ON true
	JOIN nl_e ON nl_e.id = nl_a.id;

SET lindp_hyper.enabled = off;

EXPLAIN (COSTS OFF)
SELECT * FROM nl_a
	LEFT JOIN LATERAL (SELECT nl_b.id AS bid, ss2.cx
					   FROM nl_b,
							LATERAL (SELECT nl_c.x AS cx FROM nl_c
									 WHERE nl_c.id = nl_b.id) ss2
					   WHERE nl_b.id = nl_a.id) ss ON true
	JOIN nl_e ON nl_e.id = nl_a.id;

DROP TABLE nl_a, nl_b, nl_c, nl_e;
