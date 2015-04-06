-- data type passed by value
CREATE TABLE mcv_list (
    a INT,
    b INT,
    c INT
);

-- unknown column
ALTER TABLE mcv_list ADD STATISTICS (mcv) ON (unknown_column);

-- single column
ALTER TABLE mcv_list ADD STATISTICS (mcv) ON (a);

-- single column, duplicated
ALTER TABLE mcv_list ADD STATISTICS (mcv) ON (a, a);

-- two columns, one duplicated
ALTER TABLE mcv_list ADD STATISTICS (mcv) ON (a, a, b);

-- unknown option
ALTER TABLE mcv_list ADD STATISTICS (unknown_option) ON (a, b, c);

-- missing MCV statistics
ALTER TABLE mcv_list ADD STATISTICS (dependencies, max_mcv_items 200) ON (a, b, c);

-- invalid mcv_max_items value / too low
ALTER TABLE mcv_list ADD STATISTICS (mcv, max_mcv_items 10) ON (a, b, c);

-- invalid mcv_max_items value / too high
ALTER TABLE mcv_list ADD STATISTICS (mcv, max_mcv_items 10000) ON (a, b, c);

-- correct command
ALTER TABLE mcv_list ADD STATISTICS (mcv) ON (a, b, c);

-- random data
INSERT INTO mcv_list
     SELECT mod(i, 111), mod(i, 123), mod(i, 23) FROM generate_series(1,10000) s(i);

ANALYZE mcv_list;

SELECT mcv_enabled, mcv_built, pg_mv_stats_mcvlist_info(stamcv)
  FROM pg_mv_statistic WHERE starelid = 'mcv_list'::regclass;

TRUNCATE mcv_list;

-- a => b, a => c, b => c
INSERT INTO mcv_list
     SELECT i/10, i/100, i/200 FROM generate_series(1,10000) s(i);

ANALYZE mcv_list;

SELECT mcv_enabled, mcv_built, pg_mv_stats_mcvlist_info(stamcv)
  FROM pg_mv_statistic WHERE starelid = 'mcv_list'::regclass;

TRUNCATE mcv_list;

-- a => b, a => c
INSERT INTO mcv_list
     SELECT i/10, i/150, i/200 FROM generate_series(1,10000) s(i);

ANALYZE mcv_list;

SELECT mcv_enabled, mcv_built, pg_mv_stats_mcvlist_info(stamcv)
  FROM pg_mv_statistic WHERE starelid = 'mcv_list'::regclass;

TRUNCATE mcv_list;

-- check explain (expect bitmap index scan, not plain index scan)
INSERT INTO mcv_list
     SELECT i/10000, i/20000, i/40000 FROM generate_series(1,1000000) s(i);
CREATE INDEX mcv_idx ON mcv_list (a, b);

ANALYZE mcv_list;

SELECT mcv_enabled, mcv_built, pg_mv_stats_mcvlist_info(stamcv)
  FROM pg_mv_statistic WHERE starelid = 'mcv_list'::regclass;

EXPLAIN (COSTS off)
 SELECT * FROM mcv_list WHERE a = 10 AND b = 5;

DROP TABLE mcv_list;

-- varlena type (text)
CREATE TABLE mcv_list (
    a TEXT,
    b TEXT,
    c TEXT
);

ALTER TABLE mcv_list ADD STATISTICS (mcv) ON (a, b, c);

-- random data
INSERT INTO mcv_list
     SELECT mod(i, 111), mod(i, 123), mod(i, 23) FROM generate_series(1,10000) s(i);

ANALYZE mcv_list;

SELECT mcv_enabled, mcv_built, pg_mv_stats_mcvlist_info(stamcv)
  FROM pg_mv_statistic WHERE starelid = 'mcv_list'::regclass;

TRUNCATE mcv_list;

-- a => b, a => c, b => c
INSERT INTO mcv_list
     SELECT i/10, i/100, i/200 FROM generate_series(1,10000) s(i);

ANALYZE mcv_list;

SELECT mcv_enabled, mcv_built, pg_mv_stats_mcvlist_info(stamcv)
  FROM pg_mv_statistic WHERE starelid = 'mcv_list'::regclass;

TRUNCATE mcv_list;

-- a => b, a => c
INSERT INTO mcv_list
     SELECT i/10, i/150, i/200 FROM generate_series(1,10000) s(i);
ANALYZE mcv_list;

SELECT mcv_enabled, mcv_built, pg_mv_stats_mcvlist_info(stamcv)
  FROM pg_mv_statistic WHERE starelid = 'mcv_list'::regclass;

TRUNCATE mcv_list;

-- check explain (expect bitmap index scan, not plain index scan)
INSERT INTO mcv_list
     SELECT i/10000, i/20000, i/40000 FROM generate_series(1,1000000) s(i);
CREATE INDEX mcv_idx ON mcv_list (a, b);
ANALYZE mcv_list;

SELECT mcv_enabled, mcv_built, pg_mv_stats_mcvlist_info(stamcv)
  FROM pg_mv_statistic WHERE starelid = 'mcv_list'::regclass;

EXPLAIN (COSTS off)
 SELECT * FROM mcv_list WHERE a = '10' AND b = '5';

TRUNCATE mcv_list;

-- check explain (expect bitmap index scan, not plain index scan) with NULLs
INSERT INTO mcv_list
     SELECT
       (CASE WHEN i/10000 = 0 THEN NULL ELSE i/10000 END),
       (CASE WHEN i/20000 = 0 THEN NULL ELSE i/20000 END),
       (CASE WHEN i/40000 = 0 THEN NULL ELSE i/40000 END)
     FROM generate_series(1,1000000) s(i);
ANALYZE mcv_list;

SELECT mcv_enabled, mcv_built, pg_mv_stats_mcvlist_info(stamcv)
  FROM pg_mv_statistic WHERE starelid = 'mcv_list'::regclass;

EXPLAIN (COSTS off)
 SELECT * FROM mcv_list WHERE a IS NULL AND b IS NULL;

DROP TABLE mcv_list;

-- NULL values (mix of int and text columns)
CREATE TABLE mcv_list (
    a INT,
    b TEXT,
    c INT,
    d TEXT
);

ALTER TABLE mcv_list ADD STATISTICS (mcv) ON (a, b, c, d);

INSERT INTO mcv_list
     SELECT
         mod(i, 100),
         (CASE WHEN mod(i, 200) = 0 THEN NULL ELSE mod(i,200) END),
         mod(i, 400),
         (CASE WHEN mod(i, 300) = 0 THEN NULL ELSE mod(i,600) END)
     FROM generate_series(1,10000) s(i);

ANALYZE mcv_list;

SELECT mcv_enabled, mcv_built, pg_mv_stats_mcvlist_info(stamcv)
  FROM pg_mv_statistic WHERE starelid = 'mcv_list'::regclass;

DROP TABLE mcv_list;
