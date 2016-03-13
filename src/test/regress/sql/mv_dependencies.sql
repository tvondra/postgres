-- data type passed by value
CREATE TABLE functional_dependencies (
    a INT,
    b INT,
    c INT
);

-- unknown column
CREATE STATISTICS s1 ON functional_dependencies (unknown_column) WITH (dependencies);

-- single column
CREATE STATISTICS s1 ON functional_dependencies (a) WITH (dependencies);

-- single column, duplicated
CREATE STATISTICS s1 ON functional_dependencies (a,a) WITH (dependencies);

-- two columns, one duplicated
CREATE STATISTICS s1 ON functional_dependencies (a, a, b) WITH (dependencies);

-- unknown option
CREATE STATISTICS s1 ON functional_dependencies (a, b, c) WITH (unknown_option);

-- correct command
CREATE STATISTICS s1 ON functional_dependencies (a, b, c) WITH (dependencies);

-- random data (no functional dependencies)
INSERT INTO functional_dependencies
     SELECT mod(i, 111), mod(i, 123), mod(i, 23) FROM generate_series(1,10000) s(i);

ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, pg_mv_stats_dependencies_show(stadeps)
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

TRUNCATE functional_dependencies;

-- a => b, a => c, b => c
INSERT INTO functional_dependencies
     SELECT i/10, i/100, i/200 FROM generate_series(1,10000) s(i);

ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, pg_mv_stats_dependencies_show(stadeps)
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

TRUNCATE functional_dependencies;

-- a => b, a => c
INSERT INTO functional_dependencies
     SELECT i/10, i/150, i/200 FROM generate_series(1,10000) s(i);
ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, pg_mv_stats_dependencies_show(stadeps)
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

TRUNCATE functional_dependencies;

-- check explain (expect bitmap index scan, not plain index scan)
INSERT INTO functional_dependencies
     SELECT i/10000, i/20000, i/40000 FROM generate_series(1,1000000) s(i);
CREATE INDEX fdeps_idx ON functional_dependencies (a, b);
ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, pg_mv_stats_dependencies_show(stadeps)
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

EXPLAIN (COSTS off)
 SELECT * FROM functional_dependencies WHERE a = 10 AND b = 5;

DROP TABLE functional_dependencies;

-- varlena type (text)
CREATE TABLE functional_dependencies (
    a TEXT,
    b TEXT,
    c TEXT
);

CREATE STATISTICS s2 ON functional_dependencies (a, b, c) WITH (dependencies);

-- random data (no functional dependencies)
INSERT INTO functional_dependencies
     SELECT mod(i, 111), mod(i, 123), mod(i, 23) FROM generate_series(1,10000) s(i);

ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, pg_mv_stats_dependencies_show(stadeps)
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

TRUNCATE functional_dependencies;

-- a => b, a => c, b => c
INSERT INTO functional_dependencies
     SELECT i/10, i/100, i/200 FROM generate_series(1,10000) s(i);

ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, pg_mv_stats_dependencies_show(stadeps)
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

TRUNCATE functional_dependencies;

-- a => b, a => c
INSERT INTO functional_dependencies
     SELECT i/10, i/150, i/200 FROM generate_series(1,10000) s(i);
ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, pg_mv_stats_dependencies_show(stadeps)
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

TRUNCATE functional_dependencies;

-- check explain (expect bitmap index scan, not plain index scan)
INSERT INTO functional_dependencies
     SELECT i/10000, i/20000, i/40000 FROM generate_series(1,1000000) s(i);
CREATE INDEX fdeps_idx ON functional_dependencies (a, b);
ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, pg_mv_stats_dependencies_show(stadeps)
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

EXPLAIN (COSTS off)
 SELECT * FROM functional_dependencies WHERE a = '10' AND b = '5';

DROP TABLE functional_dependencies;

-- NULL values (mix of int and text columns)
CREATE TABLE functional_dependencies (
    a INT,
    b TEXT,
    c INT,
    d TEXT
);

CREATE STATISTICS s3 ON functional_dependencies (a, b, c, d) WITH (dependencies);

INSERT INTO functional_dependencies
     SELECT
         mod(i, 100),
         (CASE WHEN mod(i, 200) = 0 THEN NULL ELSE mod(i,200) END),
         mod(i, 400),
         (CASE WHEN mod(i, 300) = 0 THEN NULL ELSE mod(i,600) END)
     FROM generate_series(1,10000) s(i);

ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, pg_mv_stats_dependencies_show(stadeps)
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

DROP TABLE functional_dependencies;
