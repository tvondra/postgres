-- not really a problem
-- geqo:  Nested Loop  (cost=27.63..497986734694888.06 rows=238418579101562496 width=416)
-- lindp: Nested Loop  (cost=60.51..389268119271205.75 rows=238418579101562496 width=416)

DO $$
BEGIN
    FOR i IN 0..23 LOOP
        EXECUTE format('DROP TABLE IF EXISTS t%s', i);
        EXECUTE format('CREATE TABLE t%s (id int, a int, b int, c int)', i);
        -- A few rows so the EXPLAIN'd plan is actually executable/correct.
        EXECUTE format('INSERT INTO t%s SELECT g, g %% 4, g %% 5, g %% 6 '
                       'FROM generate_series(1, 20) g', i);
        EXECUTE format('ANALYZE t%s', i);
    END LOOP;
END
$$;

SET geqo = on;
SET geqo_threshold = 100;
SET geqo_effort = 5;
-- SET geqo_effort = 10;
-- SET geqo_pool_size = 30000;
-- SET geqo_generations = 30000;
SET join_collapse_limit = 100;
SET from_collapse_limit = 100;
-- Deterministic GEQO runs.
SET geqo_seed = 0;

\timing on

-- The query.
--
-- The 24 base tables form a single equivalence class on column "a"
-- (t0.a = t1.a = ... = t23.a).  Equivalence-class machinery derives a join
-- clause between *every* pair of those tables, so the join graph is a complete
-- graph (a 24-clique).  A clique is the worst case for GEQO's gimme_tree()
-- heuristic: every relation looks "desirable" to join with every other one, so
-- the clumping heuristic can merge in almost any order and each fitness
-- evaluation has to cost out a large, dense set of intermediate joinrels.
--
-- On top of the clique, 8 LEFT JOIN LATERAL subqueries each reference two
-- different base tables.  Because they sit on the inner side of an outer join,
-- they impose join-order restrictions (the two referenced tables must be joined
-- before the lateral scan can be performed).  Many of the random join orders
-- GEQO tries are therefore invalid and get thrown away, which both forces GEQO
-- to generate extra candidate tours and makes every gimme_tree() call do more
-- work.
--
EXPLAIN
SELECT *
  FROM t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11,
       t12, t13, t14, t15, t16, t17, t18, t19, t20, t21, t22, t23
     LEFT JOIN LATERAL (SELECT l0.id FROM t10 l0
                        WHERE l0.b = t10.b AND l0.c = t4.c)  s0 ON true
     LEFT JOIN LATERAL (SELECT l1.id FROM t12 l1
                        WHERE l1.b = t12.b AND l1.c = t20.c) s1 ON true
     LEFT JOIN LATERAL (SELECT l2.id FROM t1 l2
                        WHERE l2.b = t1.b  AND l2.c = t2.c)  s2 ON true
     LEFT JOIN LATERAL (SELECT l3.id FROM t17 l3
                        WHERE l3.b = t17.b AND l3.c = t3.c)  s3 ON true
     LEFT JOIN LATERAL (SELECT l4.id FROM t11 l4
                        WHERE l4.b = t11.b AND l4.c = t18.c) s4 ON true
     LEFT JOIN LATERAL (SELECT l5.id FROM t1 l5
                        WHERE l5.b = t1.b  AND l5.c = t16.c) s5 ON true
     LEFT JOIN LATERAL (SELECT l6.id FROM t6 l6
                        WHERE l6.b = t6.b  AND l6.c = t1.c)  s6 ON true
     LEFT JOIN LATERAL (SELECT l7.id FROM t2 l7
                        WHERE l7.b = t2.b  AND l7.c = t13.c) s7 ON true
  WHERE t0.a = t1.a   AND t1.a = t2.a   AND t2.a = t3.a   AND t3.a = t4.a
    AND t4.a = t5.a   AND t5.a = t6.a   AND t6.a = t7.a   AND t7.a = t8.a
    AND t8.a = t9.a   AND t9.a = t10.a  AND t10.a = t11.a AND t11.a = t12.a
    AND t12.a = t13.a AND t13.a = t14.a AND t14.a = t15.a AND t15.a = t16.a
    AND t16.a = t17.a AND t17.a = t18.a AND t18.a = t19.a AND t19.a = t20.a
    AND t20.a = t21.a AND t21.a = t22.a AND t22.a = t23.a;

LOAD 'lindp_hyper';
SET lindp_hyper.min_relations = 2;
SET lindp_hyper.max_relations = 64;
SET geqo = off;

EXPLAIN
SELECT *
  FROM t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11,
       t12, t13, t14, t15, t16, t17, t18, t19, t20, t21, t22, t23
     LEFT JOIN LATERAL (SELECT l0.id FROM t10 l0
                        WHERE l0.b = t10.b AND l0.c = t4.c)  s0 ON true
     LEFT JOIN LATERAL (SELECT l1.id FROM t12 l1
                        WHERE l1.b = t12.b AND l1.c = t20.c) s1 ON true
     LEFT JOIN LATERAL (SELECT l2.id FROM t1 l2
                        WHERE l2.b = t1.b  AND l2.c = t2.c)  s2 ON true
     LEFT JOIN LATERAL (SELECT l3.id FROM t17 l3
                        WHERE l3.b = t17.b AND l3.c = t3.c)  s3 ON true
     LEFT JOIN LATERAL (SELECT l4.id FROM t11 l4
                        WHERE l4.b = t11.b AND l4.c = t18.c) s4 ON true
     LEFT JOIN LATERAL (SELECT l5.id FROM t1 l5
                        WHERE l5.b = t1.b  AND l5.c = t16.c) s5 ON true
     LEFT JOIN LATERAL (SELECT l6.id FROM t6 l6
                        WHERE l6.b = t6.b  AND l6.c = t1.c)  s6 ON true
     LEFT JOIN LATERAL (SELECT l7.id FROM t2 l7
                        WHERE l7.b = t2.b  AND l7.c = t13.c) s7 ON true
  WHERE t0.a = t1.a   AND t1.a = t2.a   AND t2.a = t3.a   AND t3.a = t4.a
    AND t4.a = t5.a   AND t5.a = t6.a   AND t6.a = t7.a   AND t7.a = t8.a
    AND t8.a = t9.a   AND t9.a = t10.a  AND t10.a = t11.a AND t11.a = t12.a
    AND t12.a = t13.a AND t13.a = t14.a AND t14.a = t15.a AND t15.a = t16.a
    AND t16.a = t17.a AND t17.a = t18.a AND t18.a = t19.a AND t19.a = t20.a
    AND t20.a = t21.a AND t21.a = t22.a AND t22.a = t23.a;
