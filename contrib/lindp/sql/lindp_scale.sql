-- Scalability test for the lindp (IKKBZ + LinDP) join search prototype.
-- Builds a large chain join (more relations than would be practical for
-- exhaustive DP) and verifies LinDP produces a single valid plan joining all
-- relations, deterministically, with results matching the default search.
LOAD 'lindp';
SET lindp.min_threshold = 3;
SET lindp.effort = 10;
SET geqo = off;
SET join_collapse_limit = 100;
SET from_collapse_limit = 100;

-- Create N chain tables lindp_s0 .. lindp_s(N-1).
DO $$
DECLARE
  n int := 16;
  i int;
BEGIN
  FOR i IN 0 .. n - 1 LOOP
    EXECUTE format('CREATE TABLE lindp_s%s (a int, b int)', i);
    EXECUTE format('INSERT INTO lindp_s%s SELECT g, g %% 7 FROM generate_series(1, 30) g', i);
    EXECUTE format('ANALYZE lindp_s%s', i);
  END LOOP;
END $$;

-- Build a chain query text: s0.b=s1.a AND s1.b=s2.a AND ...
CREATE FUNCTION lindp_chain_query(n int) RETURNS text AS $$
DECLARE
  fromlist text := '';
  wherelist text := '';
  i int;
BEGIN
  FOR i IN 0 .. n - 1 LOOP
    IF i > 0 THEN
      fromlist := fromlist || ', ';
      wherelist := wherelist || format(' AND lindp_s%s.b = lindp_s%s.a', i - 1, i);
    END IF;
    fromlist := fromlist || format('lindp_s%s', i);
  END LOOP;
  RETURN format('SELECT count(*) FROM %s WHERE true%s', fromlist, wherelist);
END $$ LANGUAGE plpgsql;

-- Summarize a plan compactly: number of scans and join nodes.  For an
-- N-relation chain this is scans=N, joins=N-1 regardless of tree shape, which
-- proves LinDP built a complete, valid plan over all relations.
CREATE FUNCTION lindp_plan_summary(q text) RETURNS text AS $$
DECLARE
  ln text;
  joins int := 0;
  scans int := 0;
BEGIN
  FOR ln IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
    IF ln LIKE '%Join%' OR ln LIKE '%Nested Loop%' THEN
      joins := joins + 1;
    END IF;
    IF ln LIKE '%Seq Scan%' THEN
      scans := scans + 1;
    END IF;
  END LOOP;
  RETURN format('scans=%s joins=%s', scans, joins);
END $$ LANGUAGE plpgsql;

-- 16-way chain with LinDP enabled.
SET lindp.enabled = on;
SELECT lindp_plan_summary(lindp_chain_query(16));

-- Determinism: the summary is identical on a second planning pass.
SELECT lindp_plan_summary(lindp_chain_query(16));

-- Same query with LinDP disabled (default search) must also join all 16.
SET lindp.enabled = off;
SELECT lindp_plan_summary(lindp_chain_query(16));

-- Result correctness across the two optimizers.
SET lindp.enabled = on;
SELECT count(*) AS lindp_on FROM lindp_s0, lindp_s1, lindp_s2, lindp_s3, lindp_s4,
  lindp_s5, lindp_s6, lindp_s7
WHERE lindp_s0.b = lindp_s1.a AND lindp_s1.b = lindp_s2.a AND lindp_s2.b = lindp_s3.a
  AND lindp_s3.b = lindp_s4.a AND lindp_s4.b = lindp_s5.a AND lindp_s5.b = lindp_s6.a
  AND lindp_s6.b = lindp_s7.a \gset

SET lindp.enabled = off;
SELECT count(*) AS lindp_off FROM lindp_s0, lindp_s1, lindp_s2, lindp_s3, lindp_s4,
  lindp_s5, lindp_s6, lindp_s7
WHERE lindp_s0.b = lindp_s1.a AND lindp_s1.b = lindp_s2.a AND lindp_s2.b = lindp_s3.a
  AND lindp_s3.b = lindp_s4.a AND lindp_s4.b = lindp_s5.a AND lindp_s5.b = lindp_s6.a
  AND lindp_s6.b = lindp_s7.a \gset

SELECT :lindp_on = :lindp_off AS results_match;

-- Cleanup
DROP FUNCTION lindp_chain_query(int);
DROP FUNCTION lindp_plan_summary(text);
DO $$
DECLARE
  i int;
BEGIN
  FOR i IN 0 .. 15 LOOP
    EXECUTE format('DROP TABLE lindp_s%s', i);
  END LOOP;
END $$;
