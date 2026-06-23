-- not a huge problem
-- a large star join, geqo takes ~200ms vhile lindp takes 600ms

DO $$
BEGIN
  FOR i IN 1..32 LOOP
    EXECUTE format('DROP TABLE IF EXISTS t%s', i);
    EXECUTE format('CREATE TABLE t%s(id int PRIMARY KEY, v int)', i);
    EXECUTE format('INSERT INTO t%s VALUES (1, 1)', i);
  END LOOP;
END $$;

ANALYZE;

SET geqo = on;
SET geqo_threshold = 2;
SET join_collapse_limit = 64;
SET from_collapse_limit = 64;
-- Make GEQO do enough chromosome evaluations to exceed 10s on typical builds.
-- SET geqo_pool_size = 1024;
-- SET geqo_generations = 100000;
SET geqo_effort = 5;
SET geqo_seed = 0.5;

\timing on

EXPLAIN
SELECT count(*)
FROM t1
JOIN t2  ON t1.id  = t2.id
JOIN t3  ON t2.id  = t3.id
JOIN t4  ON t3.id  = t4.id
JOIN t5  ON t4.id  = t5.id
JOIN t6  ON t5.id  = t6.id
JOIN t7  ON t6.id  = t7.id
JOIN t8  ON t7.id  = t8.id
JOIN t9  ON t8.id  = t9.id
JOIN t10 ON t9.id  = t10.id
JOIN t11 ON t10.id = t11.id
JOIN t12 ON t11.id = t12.id
JOIN t13 ON t12.id = t13.id
JOIN t14 ON t13.id = t14.id
JOIN t15 ON t14.id = t15.id
JOIN t16 ON t15.id = t16.id
JOIN t17 ON t16.id = t17.id
JOIN t18 ON t17.id = t18.id
JOIN t19 ON t18.id = t19.id
JOIN t20 ON t19.id = t20.id
JOIN t21 ON t20.id = t21.id
JOIN t22 ON t21.id = t22.id
JOIN t23 ON t22.id = t23.id
JOIN t24 ON t23.id = t24.id
JOIN t25 ON t24.id = t25.id
JOIN t26 ON t25.id = t26.id
JOIN t27 ON t26.id = t27.id
JOIN t28 ON t27.id = t28.id
JOIN t29 ON t28.id = t29.id
JOIN t30 ON t29.id = t30.id
JOIN t31 ON t30.id = t31.id
JOIN t32 ON t31.id = t32.id;

LOAD 'lindp';
SET lindp.min_relations = 2;
SET lindp.max_relations = 64;
SET geqo = off;

EXPLAIN
SELECT count(*)
FROM t1
JOIN t2  ON t1.id  = t2.id
JOIN t3  ON t2.id  = t3.id
JOIN t4  ON t3.id  = t4.id
JOIN t5  ON t4.id  = t5.id
JOIN t6  ON t5.id  = t6.id
JOIN t7  ON t6.id  = t7.id
JOIN t8  ON t7.id  = t8.id
JOIN t9  ON t8.id  = t9.id
JOIN t10 ON t9.id  = t10.id
JOIN t11 ON t10.id = t11.id
JOIN t12 ON t11.id = t12.id
JOIN t13 ON t12.id = t13.id
JOIN t14 ON t13.id = t14.id
JOIN t15 ON t14.id = t15.id
JOIN t16 ON t15.id = t16.id
JOIN t17 ON t16.id = t17.id
JOIN t18 ON t17.id = t18.id
JOIN t19 ON t18.id = t19.id
JOIN t20 ON t19.id = t20.id
JOIN t21 ON t20.id = t21.id
JOIN t22 ON t21.id = t22.id
JOIN t23 ON t22.id = t23.id
JOIN t24 ON t23.id = t24.id
JOIN t25 ON t24.id = t25.id
JOIN t26 ON t25.id = t26.id
JOIN t27 ON t26.id = t27.id
JOIN t28 ON t27.id = t28.id
JOIN t29 ON t28.id = t29.id
JOIN t30 ON t29.id = t30.id
JOIN t31 ON t30.id = t31.id
JOIN t32 ON t31.id = t32.id;
