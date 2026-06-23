--
-- Self-contained reproducer: a query whose IKKBZ *linearization* admits no
-- legal contiguous decomposition, so contrib/lindp transparently falls back
-- to the in-core standard_join_search().  No LATERAL joins are used.
--
-- Why it fails:
--   All five relations are joined on the same key, so the join graph is a
--   clique (one equivalence class id1 = id2 = id3 = id4 = id5).  IKKBZ
--   produces a single left-deep order of the relations ranked purely by
--   selectivity, ignoring the outer-join restrictions.  The query mixes a
--   LEFT JOIN (lf4 on the nullable side) with a RIGHT JOIN (lf5 forces the
--   whole left input onto the nullable side).  For the linear order IKKBZ
--   picks, no contiguous split [a..k][k+1..b] yields a join that
--   make_join_rel() accepts: every split would commute or re-associate across
--   one of the two outer joins, which is illegal.  build_interval() therefore
--   returns NULL for the full set and lindp falls back.
--
-- The result is still correct (the fallback guarantees it); this script just
-- demonstrates that the linearization step itself cannot solve the query.
-- With lindp.debug = on a NOTICE is emitted on the fallback.
--
LOAD 'lindp';

SET lindp.enabled        = on;
SET lindp.fallback       = off;  -- error out if linearization failed
SET lindp.min_relations  = 3;    -- low, so a 6-way join uses LinDP (not GEQO/DP)
SET lindp.seeds          = 1;    -- try every IKKBZ root
SET geqo                       = off;
SET join_collapse_limit        = 100;   -- keep the whole join flat (one search problem)
SET from_collapse_limit        = 100;

CREATE TABLE lf1 (id int, val int);
CREATE TABLE lf2 (id int, val int);
CREATE TABLE lf3 (id int, val int);
CREATE TABLE lf4 (id int, val int);
CREATE TABLE lf5 (id int, val int);

INSERT INTO lf1 SELECT g, g % 10 FROM generate_series(1, 1000) g;
INSERT INTO lf2 SELECT g, g % 10 FROM generate_series(1,  100) g;
INSERT INTO lf3 SELECT g, g % 10 FROM generate_series(1,   50) g;
INSERT INTO lf4 SELECT g, g % 10 FROM generate_series(1,   20) g;
INSERT INTO lf5 SELECT g, g % 10 FROM generate_series(1,   10) g;

CREATE INDEX lf1_id ON lf1 (id);
CREATE INDEX lf2_id ON lf2 (id);
CREATE INDEX lf3_id ON lf3 (id);
CREATE INDEX lf4_id ON lf4 (id);
CREATE INDEX lf5_id ON lf5 (id);

ANALYZE lf1, lf2, lf3, lf4, lf5;

-- The failing query.  Expect a NOTICE: "lindp: linearization failed ...".
SELECT count(*) AS lindp_rows
  FROM lf1
       JOIN  lf2 ON lf1.id = lf2.id
       JOIN  lf3 ON lf2.id = lf3.id
       LEFT  JOIN lf4 ON lf2.id = lf4.id
       RIGHT JOIN lf5 ON lf2.id = lf5.id;

-- Sanity check: the in-core search produces exactly the same result, proving
-- the fallback preserves correctness.
SET lindp3.enabled = off;
SELECT count(*) AS incore_rows
  FROM lf1
       JOIN  lf2 ON lf1.id = lf2.id
       JOIN  lf3 ON lf2.id = lf3.id
       LEFT  JOIN lf4 ON lf2.id = lf4.id
       RIGHT JOIN lf5 ON lf2.id = lf5.id;

DROP TABLE lf1, lf2, lf3, lf4, lf5;
