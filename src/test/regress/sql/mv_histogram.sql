-- data type passed by value
CREATE TABLE mv_histogram (
    a INT,
    b INT,
    c INT
);

-- unknown column
CREATE STATISTICS s7 ON mv_histogram (unknown_column) WITH (histogram);

-- single column
CREATE STATISTICS s7 ON mv_histogram (a) WITH (histogram);

-- single column, duplicated
CREATE STATISTICS s7 ON mv_histogram (a, a) WITH (histogram);

-- two columns, one duplicated
CREATE STATISTICS s7 ON mv_histogram (a, a, b) WITH (histogram);

-- unknown option
CREATE STATISTICS s7 ON mv_histogram (a, b, c) WITH (unknown_option);

-- missing histogram statistics
CREATE STATISTICS s7 ON mv_histogram (a, b, c) WITH (dependencies, max_buckets=200);

-- invalid max_buckets value / too low
CREATE STATISTICS s7 ON mv_histogram (a, b, c) WITH (mcv, max_buckets=10);

-- invalid max_buckets value / too high
CREATE STATISTICS s7 ON mv_histogram (a, b, c) WITH (mcv, max_buckets=100000);

-- correct command
CREATE STATISTICS s7 ON mv_histogram (a, b, c) WITH (histogram);

-- random data (no functional dependencies)
INSERT INTO mv_histogram
     SELECT mod(i, 111), mod(i, 123), mod(i, 23) FROM generate_series(1,10000) s(i);

ANALYZE mv_histogram;

SELECT hist_enabled, hist_built
  FROM pg_mv_statistic WHERE starelid = 'mv_histogram'::regclass;

TRUNCATE mv_histogram;

-- a => b, a => c, b => c
INSERT INTO mv_histogram
     SELECT i/10, i/100, i/200 FROM generate_series(1,10000) s(i);

ANALYZE mv_histogram;

SELECT hist_enabled, hist_built
  FROM pg_mv_statistic WHERE starelid = 'mv_histogram'::regclass;

TRUNCATE mv_histogram;

-- a => b, a => c
INSERT INTO mv_histogram
     SELECT i/10, i/150, i/200 FROM generate_series(1,10000) s(i);
ANALYZE mv_histogram;

SELECT hist_enabled, hist_built
  FROM pg_mv_statistic WHERE starelid = 'mv_histogram'::regclass;

TRUNCATE mv_histogram;

-- check explain (expect bitmap index scan, not plain index scan)
INSERT INTO mv_histogram
     SELECT i/10000, i/20000, i/40000 FROM generate_series(1,1000000) s(i);
CREATE INDEX hist_idx ON mv_histogram (a, b);
ANALYZE mv_histogram;

SELECT hist_enabled, hist_built
  FROM pg_mv_statistic WHERE starelid = 'mv_histogram'::regclass;

EXPLAIN (COSTS off)
 SELECT * FROM mv_histogram WHERE a = 10 AND b = 5;

DROP TABLE mv_histogram;

-- varlena type (text)
CREATE TABLE mv_histogram (
    a TEXT,
    b TEXT,
    c TEXT
);

CREATE STATISTICS s8 ON mv_histogram (a, b, c) WITH (histogram);

-- random data (no functional dependencies)
INSERT INTO mv_histogram
     SELECT mod(i, 111), mod(i, 123), mod(i, 23) FROM generate_series(1,10000) s(i);

ANALYZE mv_histogram;

SELECT hist_enabled, hist_built
  FROM pg_mv_statistic WHERE starelid = 'mv_histogram'::regclass;

TRUNCATE mv_histogram;

-- a => b, a => c, b => c
INSERT INTO mv_histogram
     SELECT i/10, i/100, i/200 FROM generate_series(1,10000) s(i);

ANALYZE mv_histogram;

SELECT hist_enabled, hist_built
  FROM pg_mv_statistic WHERE starelid = 'mv_histogram'::regclass;

TRUNCATE mv_histogram;

-- a => b, a => c
INSERT INTO mv_histogram
     SELECT i/10, i/150, i/200 FROM generate_series(1,10000) s(i);
ANALYZE mv_histogram;

SELECT hist_enabled, hist_built
  FROM pg_mv_statistic WHERE starelid = 'mv_histogram'::regclass;

TRUNCATE mv_histogram;

-- check explain (expect bitmap index scan, not plain index scan)
INSERT INTO mv_histogram
     SELECT i/10000, i/20000, i/40000 FROM generate_series(1,1000000) s(i);
CREATE INDEX hist_idx ON mv_histogram (a, b);
ANALYZE mv_histogram;

SELECT hist_enabled, hist_built
  FROM pg_mv_statistic WHERE starelid = 'mv_histogram'::regclass;

EXPLAIN (COSTS off)
 SELECT * FROM mv_histogram WHERE a = '10' AND b = '5';

TRUNCATE mv_histogram;

-- check explain (expect bitmap index scan, not plain index scan) with NULLs
INSERT INTO mv_histogram
     SELECT
       (CASE WHEN i/10000 = 0 THEN NULL ELSE i/10000 END),
       (CASE WHEN i/20000 = 0 THEN NULL ELSE i/20000 END),
       (CASE WHEN i/40000 = 0 THEN NULL ELSE i/40000 END)
     FROM generate_series(1,1000000) s(i);
ANALYZE mv_histogram;

SELECT hist_enabled, hist_built
  FROM pg_mv_statistic WHERE starelid = 'mv_histogram'::regclass;

EXPLAIN (COSTS off)
 SELECT * FROM mv_histogram WHERE a IS NULL AND b IS NULL;

DROP TABLE mv_histogram;

-- NULL values (mix of int and text columns)
CREATE TABLE mv_histogram (
    a INT,
    b TEXT,
    c INT,
    d TEXT
);

CREATE STATISTICS s9 ON mv_histogram (a, b, c, d) WITH (histogram);

INSERT INTO mv_histogram
     SELECT
         mod(i, 100),
         (CASE WHEN mod(i, 200) = 0 THEN NULL ELSE mod(i,200) END),
         mod(i, 400),
         (CASE WHEN mod(i, 300) = 0 THEN NULL ELSE mod(i,600) END)
     FROM generate_series(1,10000) s(i);

ANALYZE mv_histogram;

SELECT hist_enabled, hist_built
  FROM pg_mv_statistic WHERE starelid = 'mv_histogram'::regclass;

DROP TABLE mv_histogram;
