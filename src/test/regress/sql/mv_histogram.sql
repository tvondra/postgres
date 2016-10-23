-- data type passed by value
CREATE TABLE mv_histogram (
    a INT,
    b INT,
    c INT
);

-- unknown column
CREATE STATISTICS s7 WITH (histogram) ON (unknown_column) FROM mv_histogram;

-- single column
CREATE STATISTICS s7 WITH (histogram) ON (a) FROM mv_histogram;

-- single column, duplicated
CREATE STATISTICS s7 WITH (histogram) ON (a, a) FROM mv_histogram;

-- two columns, one duplicated
CREATE STATISTICS s7 WITH (histogram) ON (a, a, b) FROM mv_histogram;

-- unknown option
CREATE STATISTICS s7 WITH (unknown_option) ON (a, b, c) FROM mv_histogram;

-- correct command
CREATE STATISTICS s7 WITH (histogram) ON (a, b, c) FROM mv_histogram;

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
     SELECT i/100, i/200, i/400 FROM generate_series(1,30000) s(i);
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

CREATE STATISTICS s8 WITH (histogram) ON (a, b, c) FROM mv_histogram;

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
     SELECT i/100, i/200, i/400 FROM generate_series(1,30000) s(i);
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
       (CASE WHEN i/100 = 0 THEN NULL ELSE i/100 END),
       (CASE WHEN i/200 = 0 THEN NULL ELSE i/200 END),
       (CASE WHEN i/400 = 0 THEN NULL ELSE i/400 END)
     FROM generate_series(1,30000) s(i);
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

CREATE STATISTICS s9 WITH (histogram) ON (a, b, c, d) FROM mv_histogram;

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
