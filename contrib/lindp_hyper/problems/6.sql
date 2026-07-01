-- geqo: ~60 seconds
-- lindp: ~120 seconds

SET geqo = on;
SET geqo_threshold     = 12;      -- 32 rels > 12, so GEQO is used
SET geqo_effort        = 5;
SET geqo_pool_size     = 0;       -- 0 => derived from effort (=> 250)
SET geqo_generations   = 0;       -- 0 => derived from pool size (=> 250)
SET geqo_selection_bias = 2.0;
SET geqo_seed          = 0;

SET join_collapse_limit = 100;
SET from_collapse_limit = 100;

SET enable_partitionwise_join = on;

-- ---------------------------------------------------------------------------
-- 32 identically range-partitioned tables (96 partitions each), all joined
-- on the partition key so the join is partitionwise-eligible.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    nt    int := 32;   -- number of tables (the task allows up to 32)
    npart int := 96;   -- partitions per table -> drives planning time
BEGIN
    FOR i IN 0 .. nt - 1 LOOP
        EXECUTE format('DROP TABLE IF EXISTS pt%s', i);
        EXECUTE format('CREATE TABLE pt%s (id int, a int, b int) PARTITION BY RANGE (id)', i);
        FOR j IN 0 .. npart - 1 LOOP
            EXECUTE format('CREATE TABLE pt%s_%s PARTITION OF pt%s FOR VALUES FROM (%s) TO (%s)',
                           i, j, i, j * 10, (j + 1) * 10);
        END LOOP;
        EXECUTE format('INSERT INTO pt%s SELECT g, g %% 50, g %% 30 FROM generate_series(0, %s) g',
                       i, npart * 10 - 1);
    END LOOP;
END $$;

ANALYZE;

\timing

-- ---------------------------------------------------------------------------
-- The challenging query: a plain 32-way equi-join on the partition key.
-- It is correct and cheap to *execute*; it is GEQO's planning that is slow.
-- The EXPLAIN below typically takes ~15-20 seconds to plan (well over 10s).
-- ---------------------------------------------------------------------------
EXPLAIN (COSTS off, SUMMARY off)
SELECT 1
FROM pt0
    JOIN pt1  ON pt1.id  = pt0.id
    JOIN pt2  ON pt2.id  = pt0.id
    JOIN pt3  ON pt3.id  = pt0.id
    JOIN pt4  ON pt4.id  = pt0.id
    JOIN pt5  ON pt5.id  = pt0.id
    JOIN pt6  ON pt6.id  = pt0.id
    JOIN pt7  ON pt7.id  = pt0.id
    JOIN pt8  ON pt8.id  = pt0.id
    JOIN pt9  ON pt9.id  = pt0.id
    JOIN pt10 ON pt10.id = pt0.id
    JOIN pt11 ON pt11.id = pt0.id
    JOIN pt12 ON pt12.id = pt0.id
    JOIN pt13 ON pt13.id = pt0.id
    JOIN pt14 ON pt14.id = pt0.id
    JOIN pt15 ON pt15.id = pt0.id
    JOIN pt16 ON pt16.id = pt0.id
    JOIN pt17 ON pt17.id = pt0.id
    JOIN pt18 ON pt18.id = pt0.id
    JOIN pt19 ON pt19.id = pt0.id
    JOIN pt20 ON pt20.id = pt0.id
    JOIN pt21 ON pt21.id = pt0.id
    JOIN pt22 ON pt22.id = pt0.id
    JOIN pt23 ON pt23.id = pt0.id
    JOIN pt24 ON pt24.id = pt0.id
    JOIN pt25 ON pt25.id = pt0.id
    JOIN pt26 ON pt26.id = pt0.id
    JOIN pt27 ON pt27.id = pt0.id
    JOIN pt28 ON pt28.id = pt0.id
    JOIN pt29 ON pt29.id = pt0.id
    JOIN pt30 ON pt30.id = pt0.id
    JOIN pt31 ON pt31.id = pt0.id;


LOAD 'lindp_hyper';
SET lindp_hyper.min_relations = 2;
SET lindp_hyper.max_relations = 64;
SET geqo = off;

EXPLAIN (COSTS off, SUMMARY off)
SELECT 1
FROM pt0
    JOIN pt1  ON pt1.id  = pt0.id
    JOIN pt2  ON pt2.id  = pt0.id
    JOIN pt3  ON pt3.id  = pt0.id
    JOIN pt4  ON pt4.id  = pt0.id
    JOIN pt5  ON pt5.id  = pt0.id
    JOIN pt6  ON pt6.id  = pt0.id
    JOIN pt7  ON pt7.id  = pt0.id
    JOIN pt8  ON pt8.id  = pt0.id
    JOIN pt9  ON pt9.id  = pt0.id
    JOIN pt10 ON pt10.id = pt0.id
    JOIN pt11 ON pt11.id = pt0.id
    JOIN pt12 ON pt12.id = pt0.id
    JOIN pt13 ON pt13.id = pt0.id
    JOIN pt14 ON pt14.id = pt0.id
    JOIN pt15 ON pt15.id = pt0.id
    JOIN pt16 ON pt16.id = pt0.id
    JOIN pt17 ON pt17.id = pt0.id
    JOIN pt18 ON pt18.id = pt0.id
    JOIN pt19 ON pt19.id = pt0.id
    JOIN pt20 ON pt20.id = pt0.id
    JOIN pt21 ON pt21.id = pt0.id
    JOIN pt22 ON pt22.id = pt0.id
    JOIN pt23 ON pt23.id = pt0.id
    JOIN pt24 ON pt24.id = pt0.id
    JOIN pt25 ON pt25.id = pt0.id
    JOIN pt26 ON pt26.id = pt0.id
    JOIN pt27 ON pt27.id = pt0.id
    JOIN pt28 ON pt28.id = pt0.id
    JOIN pt29 ON pt29.id = pt0.id
    JOIN pt30 ON pt30.id = pt0.id
    JOIN pt31 ON pt31.id = pt0.id;
