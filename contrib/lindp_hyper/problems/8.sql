-- much worse plan:
-- exhaustive:  Aggregate  (cost=4299074237.71..4299074237.72 rows=1 width=8)
-- geqo:        Aggregate  (cost=123957843187.73..123957843187.74 rows=1 width=8)
-- lindp:       Aggregate  (cost=10151826643651.85..10151826643651.86 rows=1 width=8)

DO $$
BEGIN
    FOR i IN 0..5 LOOP
        EXECUTE format('DROP TABLE IF EXISTS a%s', i);
        EXECUTE format('DROP TABLE IF EXISTS b%s', i);

        EXECUTE format('CREATE TABLE a%s (k int, m int)', i);
        EXECUTE format('CREATE TABLE b%s (k int, m int)', i);

        EXECUTE format('INSERT INTO a%s SELECT (g %% 20) + 1, g '
                       'FROM generate_series(1, 20000) g', i);
        EXECUTE format('INSERT INTO b%s SELECT (g %% 20) + 1, g '
                       'FROM generate_series(1, 20000) g', i);
    END LOOP;
END
$$;
ANALYZE;

SET geqo                = on;
SET geqo_threshold      = 12;
SET geqo_effort         = 5;
SET geqo_pool_size      = 0;
SET geqo_generations    = 0;
SET geqo_selection_bias = 2.0;
SET geqo_seed           = 0;

SET join_collapse_limit = 20;
SET from_collapse_limit = 20;

\timing

-- 1. The exhaustive optimum, for comparison (standard_join_search).
SET geqo = off;
EXPLAIN (COSTS ON, TIMING OFF, SUMMARY ON)
SELECT count(*)
  FROM a0, a1, a2, a3, a4, a5, b0, b1, b2, b3, b4, b5
 WHERE a0.k = a1.k AND a1.k = a2.k AND a2.k = a3.k
   AND a3.k = a4.k AND a4.k = a5.k AND a5.k = b0.k
   AND b0.k = b1.k AND b1.k = b2.k AND b2.k = b3.k
   AND b3.k = b4.k AND b4.k = b5.k
   AND a0.m = b0.m AND a1.m = b1.m AND a2.m = b2.m
   AND a3.m = b3.m AND a4.m = b4.m AND a5.m = b5.m;

-- 2. The same query under the default GEQO parameters: a much costlier plan.
SET geqo = on;
EXPLAIN (COSTS ON, TIMING OFF, SUMMARY ON)
SELECT count(*)
  FROM a0, a1, a2, a3, a4, a5, b0, b1, b2, b3, b4, b5
 WHERE a0.k = a1.k AND a1.k = a2.k AND a2.k = a3.k
   AND a3.k = a4.k AND a4.k = a5.k AND a5.k = b0.k
   AND b0.k = b1.k AND b1.k = b2.k AND b2.k = b3.k
   AND b3.k = b4.k AND b4.k = b5.k
   AND a0.m = b0.m AND a1.m = b1.m AND a2.m = b2.m
   AND a3.m = b3.m AND a4.m = b4.m AND a5.m = b5.m;

-- 3. lindp_hyper
LOAD 'lindp_hyper';
SET lindp_hyper.min_relations = 2;
SET lindp_hyper.max_relations = 64;

SET geqo = off;
EXPLAIN (COSTS ON, TIMING OFF, SUMMARY ON)
SELECT count(*)
  FROM a0, a1, a2, a3, a4, a5, b0, b1, b2, b3, b4, b5
 WHERE a0.k = a1.k AND a1.k = a2.k AND a2.k = a3.k
   AND a3.k = a4.k AND a4.k = a5.k AND a5.k = b0.k
   AND b0.k = b1.k AND b1.k = b2.k AND b2.k = b3.k
   AND b3.k = b4.k AND b4.k = b5.k
   AND a0.m = b0.m AND a1.m = b1.m AND a2.m = b2.m
   AND a3.m = b3.m AND a4.m = b4.m AND a5.m = b5.m;
