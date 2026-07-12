-- geqo finishes in ~60 seconds, using ~500MB of memory
-- lindp finishes in ~90 seconds, but needs 4.5GB of memory
-- partitionwise join

DO $$
DECLARE
  r int;
  p int;
BEGIN
  FOR r IN 1..32 LOOP
    EXECUTE format('DROP TABLE IF EXISTS p%s CASCADE', r);
    EXECUTE format(
      'CREATE TABLE p%s (i int NOT NULL, j int) PARTITION BY RANGE (i)',
      r
    );

    FOR p IN 0..127 LOOP
      EXECUTE format(
        'CREATE TABLE p%s_%s PARTITION OF p%s FOR VALUES FROM (%s) TO (%s)',
        r, p, r, p, p + 1
      );
    END LOOP;
  END LOOP;
END $$;

ANALYZE;

SET geqo = on;
SET geqo_threshold = 2;
SET geqo_effort = 5;
SET geqo_pool_size = 0;
SET geqo_generations = 0;
SET geqo_selection_bias = 2.0;
SET geqo_seed = 0;

SET join_collapse_limit = 32;
SET from_collapse_limit = 32;
SET enable_partitionwise_join = on;
SET jit = off;

\timing on

EXPLAIN
SELECT 1
FROM p1 a1, p2 a2, p3 a3, p4 a4, p5 a5, p6 a6, p7 a7, p8 a8,
     p9 a9, p10 a10, p11 a11, p12 a12, p13 a13, p14 a14, p15 a15, p16 a16,
     p17 a17, p18 a18, p19 a19, p20 a20, p21 a21, p22 a22, p23 a23, p24 a24,
     p25 a25, p26 a26, p27 a27, p28 a28, p29 a29, p30 a30, p31 a31, p32 a32
WHERE a1.i = a2.i  AND a1.i = a3.i  AND a1.i = a4.i  AND a1.i = a5.i
  AND a1.i = a6.i  AND a1.i = a7.i  AND a1.i = a8.i  AND a1.i = a9.i
  AND a1.i = a10.i AND a1.i = a11.i AND a1.i = a12.i AND a1.i = a13.i
  AND a1.i = a14.i AND a1.i = a15.i AND a1.i = a16.i AND a1.i = a17.i
  AND a1.i = a18.i AND a1.i = a19.i AND a1.i = a20.i AND a1.i = a21.i
  AND a1.i = a22.i AND a1.i = a23.i AND a1.i = a24.i AND a1.i = a25.i
  AND a1.i = a26.i AND a1.i = a27.i AND a1.i = a28.i AND a1.i = a29.i
  AND a1.i = a30.i AND a1.i = a31.i AND a1.i = a32.i;


LOAD 'lindp_hyper';
SET lindp_hyper.min_relations = 2;
SET lindp_hyper.max_relations = 64;
SET geqo = off;

EXPLAIN
SELECT 1
FROM p1 a1, p2 a2, p3 a3, p4 a4, p5 a5, p6 a6, p7 a7, p8 a8,
     p9 a9, p10 a10, p11 a11, p12 a12, p13 a13, p14 a14, p15 a15, p16 a16,
     p17 a17, p18 a18, p19 a19, p20 a20, p21 a21, p22 a22, p23 a23, p24 a24,
     p25 a25, p26 a26, p27 a27, p28 a28, p29 a29, p30 a30, p31 a31, p32 a32
WHERE a1.i = a2.i  AND a1.i = a3.i  AND a1.i = a4.i  AND a1.i = a5.i
  AND a1.i = a6.i  AND a1.i = a7.i  AND a1.i = a8.i  AND a1.i = a9.i
  AND a1.i = a10.i AND a1.i = a11.i AND a1.i = a12.i AND a1.i = a13.i
  AND a1.i = a14.i AND a1.i = a15.i AND a1.i = a16.i AND a1.i = a17.i
  AND a1.i = a18.i AND a1.i = a19.i AND a1.i = a20.i AND a1.i = a21.i
  AND a1.i = a22.i AND a1.i = a23.i AND a1.i = a24.i AND a1.i = a25.i
  AND a1.i = a26.i AND a1.i = a27.i AND a1.i = a28.i AND a1.i = a29.i
  AND a1.i = a30.i AND a1.i = a31.i AND a1.i = a32.i;
