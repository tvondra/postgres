-- data type passed by value
CREATE TABLE functional_dependencies (
    a INT,
    b INT,
    c INT
);

-- unknown column
CREATE STATISTICS s1 WITH (dependencies) ON (unknown_column) FROM functional_dependencies;

-- single column
CREATE STATISTICS s1 WITH (dependencies) ON (a) FROM functional_dependencies;

-- single column, duplicated
CREATE STATISTICS s1 WITH (dependencies) ON (a,a) FROM functional_dependencies;

-- two columns, one duplicated
CREATE STATISTICS s1 WITH (dependencies) ON (a, a, b) FROM functional_dependencies;

-- correct command
CREATE STATISTICS s1 WITH (dependencies) ON (a, b, c) FROM functional_dependencies;

-- random data (no functional dependencies)
INSERT INTO functional_dependencies
     SELECT mod(i, 111), mod(i, 123), mod(i, 23) FROM generate_series(1,10000) s(i);

ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, stadeps
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

TRUNCATE functional_dependencies;

-- a => b, a => c, b => c
INSERT INTO functional_dependencies
     SELECT i/10, i/100, i/200 FROM generate_series(1,10000) s(i);

ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, stadeps
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

TRUNCATE functional_dependencies;

-- a => b, a => c
INSERT INTO functional_dependencies
     SELECT i/10, i/150, i/200 FROM generate_series(1,10000) s(i);
ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, stadeps
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

TRUNCATE functional_dependencies;

-- a => b, a => c, b => c
INSERT INTO functional_dependencies
     SELECT i/10000, i/20000, i/40000 FROM generate_series(1,1000000) s(i);
ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, stadeps
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

DROP TABLE functional_dependencies;

-- varlena type (text)
CREATE TABLE functional_dependencies (
    a TEXT,
    b TEXT,
    c TEXT
);

CREATE STATISTICS s2 WITH (dependencies) ON (a, b, c) FROM functional_dependencies;

-- random data (no functional dependencies)
INSERT INTO functional_dependencies
     SELECT mod(i, 111), mod(i, 123), mod(i, 23) FROM generate_series(1,10000) s(i);

ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, stadeps
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

TRUNCATE functional_dependencies;

-- a => b, a => c, b => c
INSERT INTO functional_dependencies
     SELECT i/10, i/100, i/200 FROM generate_series(1,10000) s(i);

ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, stadeps
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

TRUNCATE functional_dependencies;

-- a => b, a => c
INSERT INTO functional_dependencies
     SELECT i/10, i/150, i/200 FROM generate_series(1,10000) s(i);
ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, stadeps
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

TRUNCATE functional_dependencies;

-- a => b, a => c, b => c
INSERT INTO functional_dependencies
     SELECT i/10000, i/20000, i/40000 FROM generate_series(1,1000000) s(i);
ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, stadeps
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

DROP TABLE functional_dependencies;

-- NULL values (mix of int and text columns)
CREATE TABLE functional_dependencies (
    a INT,
    b TEXT,
    c INT,
    d TEXT
);

CREATE STATISTICS s3 WITH (dependencies) ON (a, b, c, d) FROM functional_dependencies;

INSERT INTO functional_dependencies
     SELECT
         mod(i, 100),
         (CASE WHEN mod(i, 200) = 0 THEN NULL ELSE mod(i,200) END),
         mod(i, 400),
         (CASE WHEN mod(i, 300) = 0 THEN NULL ELSE mod(i,600) END)
     FROM generate_series(1,10000) s(i);

ANALYZE functional_dependencies;

SELECT deps_enabled, deps_built, stadeps
  FROM pg_mv_statistic WHERE starelid = 'functional_dependencies'::regclass;

DROP TABLE functional_dependencies;
