-- geqo ~130 seconds, 850MB
-- lindp: OOM (~8GB)

SET geqo = on;
SET geqo_threshold = 12;
SET geqo_effort = 5;
SET geqo_pool_size = 0;
SET geqo_generations = 0;
SET geqo_selection_bias = 2.0;
SET geqo_seed = 0;
SET join_collapse_limit = 32;
SET from_collapse_limit = 32;
SET enable_partitionwise_join = on;

DROP SCHEMA IF EXISTS geqo_stress CASCADE;
CREATE SCHEMA geqo_stress;
SET search_path = geqo_stress;

DO $$
DECLARE
    rel int;
    part int;
BEGIN
    FOR rel IN 1..32 LOOP
        EXECUTE format('DROP TABLE IF EXISTS t%s', rel);
        EXECUTE format('CREATE TABLE t%s (p int, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int) PARTITION BY RANGE (p)', rel);

        FOR part IN 0..95 LOOP
            EXECUTE format('CREATE TABLE t%s_%s PARTITION OF t%s FOR VALUES FROM (%s) TO (%s)', rel, part, rel, part, part + 1);
        END LOOP;
    END LOOP;
END
$$;

ANALYZE;


\timing

EXPLAIN (COSTS OFF)
SELECT *
FROM t1 r1
JOIN t2 r2
  ON (r1.p = r2.p AND r1.c3 = r2.c4)
JOIN t3 r3
  ON (r2.p = r3.p AND r2.c4 = r3.c5)
JOIN t4 r4
  ON (r3.p = r4.p AND r3.c5 = r4.c6)
JOIN t5 r5
  ON (r4.p = r5.p AND r4.c6 = r5.c1)
JOIN t6 r6
  ON (r5.p = r6.p AND r5.c1 = r6.c2)
JOIN t7 r7
  ON (r6.p = r7.p AND r6.c2 = r7.c3)
JOIN t8 r8
  ON (r7.p = r8.p AND r7.c3 = r8.c4)
JOIN t9 r9
  ON (r8.p = r9.p AND r8.c4 = r9.c5)
JOIN t10 r10
  ON (r9.p = r10.p AND r9.c5 = r10.c6)
JOIN t11 r11
  ON (r10.p = r11.p AND r10.c6 = r11.c1)
JOIN t12 r12
  ON (r11.p = r12.p AND r11.c1 = r12.c2)
JOIN t13 r13
  ON (r12.p = r13.p AND r12.c2 = r13.c3)
JOIN t14 r14
  ON (r13.p = r14.p AND r13.c3 = r14.c4)
JOIN t15 r15
  ON (r14.p = r15.p AND r14.c4 = r15.c5)
JOIN t16 r16
  ON (r15.p = r16.p AND r15.c5 = r16.c6)
JOIN t17 r17
  ON (r16.p = r17.p AND r16.c6 = r17.c1)
JOIN t18 r18
  ON (r17.p = r18.p AND r17.c1 = r18.c2)
JOIN t19 r19
  ON (r18.p = r19.p AND r18.c2 = r19.c3)
JOIN t20 r20
  ON (r19.p = r20.p AND r19.c3 = r20.c4)
JOIN t21 r21
  ON (r20.p = r21.p AND r20.c4 = r21.c5)
JOIN t22 r22
  ON (r21.p = r22.p AND r21.c5 = r22.c6)
JOIN t23 r23
  ON (r22.p = r23.p AND r22.c6 = r23.c1)
JOIN t24 r24
  ON (r23.p = r24.p AND r23.c1 = r24.c2)
JOIN t25 r25
  ON (r24.p = r25.p AND r24.c2 = r25.c3)
JOIN t26 r26
  ON (r25.p = r26.p AND r25.c3 = r26.c4)
JOIN t27 r27
  ON (r26.p = r27.p AND r26.c4 = r27.c5)
JOIN t28 r28
  ON (r27.p = r28.p AND r27.c5 = r28.c6)
JOIN t29 r29
  ON (r28.p = r29.p AND r28.c6 = r29.c1)
JOIN t30 r30
  ON (r29.p = r30.p AND r29.c1 = r30.c2)
JOIN t31 r31
  ON (r30.p = r31.p AND r30.c2 = r31.c3)
JOIN t32 r32
  ON (r31.p = r32.p AND r31.c3 = r32.c4);


LOAD 'lindp_hyper';
SET lindp_hyper.min_relations = 2;
SET lindp_hyper.max_relations = 64;
SET geqo = off;


EXPLAIN (COSTS OFF)
SELECT *
FROM t1 r1
JOIN t2 r2
  ON (r1.p = r2.p AND r1.c3 = r2.c4)
JOIN t3 r3
  ON (r2.p = r3.p AND r2.c4 = r3.c5)
JOIN t4 r4
  ON (r3.p = r4.p AND r3.c5 = r4.c6)
JOIN t5 r5
  ON (r4.p = r5.p AND r4.c6 = r5.c1)
JOIN t6 r6
  ON (r5.p = r6.p AND r5.c1 = r6.c2)
JOIN t7 r7
  ON (r6.p = r7.p AND r6.c2 = r7.c3)
JOIN t8 r8
  ON (r7.p = r8.p AND r7.c3 = r8.c4)
JOIN t9 r9
  ON (r8.p = r9.p AND r8.c4 = r9.c5)
JOIN t10 r10
  ON (r9.p = r10.p AND r9.c5 = r10.c6)
JOIN t11 r11
  ON (r10.p = r11.p AND r10.c6 = r11.c1)
JOIN t12 r12
  ON (r11.p = r12.p AND r11.c1 = r12.c2)
JOIN t13 r13
  ON (r12.p = r13.p AND r12.c2 = r13.c3)
JOIN t14 r14
  ON (r13.p = r14.p AND r13.c3 = r14.c4)
JOIN t15 r15
  ON (r14.p = r15.p AND r14.c4 = r15.c5)
JOIN t16 r16
  ON (r15.p = r16.p AND r15.c5 = r16.c6)
JOIN t17 r17
  ON (r16.p = r17.p AND r16.c6 = r17.c1)
JOIN t18 r18
  ON (r17.p = r18.p AND r17.c1 = r18.c2)
JOIN t19 r19
  ON (r18.p = r19.p AND r18.c2 = r19.c3)
JOIN t20 r20
  ON (r19.p = r20.p AND r19.c3 = r20.c4)
JOIN t21 r21
  ON (r20.p = r21.p AND r20.c4 = r21.c5)
JOIN t22 r22
  ON (r21.p = r22.p AND r21.c5 = r22.c6)
JOIN t23 r23
  ON (r22.p = r23.p AND r22.c6 = r23.c1)
JOIN t24 r24
  ON (r23.p = r24.p AND r23.c1 = r24.c2)
JOIN t25 r25
  ON (r24.p = r25.p AND r24.c2 = r25.c3)
JOIN t26 r26
  ON (r25.p = r26.p AND r25.c3 = r26.c4)
JOIN t27 r27
  ON (r26.p = r27.p AND r26.c4 = r27.c5)
JOIN t28 r28
  ON (r27.p = r28.p AND r27.c5 = r28.c6)
JOIN t29 r29
  ON (r28.p = r29.p AND r28.c6 = r29.c1)
JOIN t30 r30
  ON (r29.p = r30.p AND r29.c1 = r30.c2)
JOIN t31 r31
  ON (r30.p = r31.p AND r30.c2 = r31.c3)
JOIN t32 r32
  ON (r31.p = r32.p AND r31.c3 = r32.c4);
