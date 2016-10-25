-- data type passed by value
CREATE TABLE ndistinct (
    a INT,
    b INT,
    c INT,
    d INT
);

-- unknown column
CREATE STATISTICS s10 WITH (ndistinct) ON (unknown_column) FROM ndistinct;

-- single column
CREATE STATISTICS s10 WITH (ndistinct) ON (a) FROM ndistinct;

-- single column, duplicated
CREATE STATISTICS s10 WITH (ndistinct) ON (a,a) FROM ndistinct;

-- two columns, one duplicated
CREATE STATISTICS s10 WITH (ndistinct) ON (a, a, b) FROM ndistinct;

-- correct command
CREATE STATISTICS s10 WITH (ndistinct) ON (a, b, c) FROM ndistinct;

-- perfectly correlated groups
INSERT INTO ndistinct
     SELECT i/100, i/100, i/100 FROM generate_series(1,10000) s(i);

ANALYZE ndistinct;

SELECT ndist_enabled, ndist_built, length(standist)
  FROM pg_mv_statistic WHERE starelid = 'ndistinct'::regclass;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c, d;

TRUNCATE TABLE ndistinct;

-- partially correlated groups
INSERT INTO ndistinct
     SELECT i/50, i/100, i/200 FROM generate_series(1,10000) s(i);

ANALYZE ndistinct;

SELECT ndist_enabled, ndist_built, length(standist)
  FROM pg_mv_statistic WHERE starelid = 'ndistinct'::regclass;

EXPLAIN
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b;

EXPLAIN
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c;

EXPLAIN
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c, d;

EXPLAIN
 SELECT COUNT(*) FROM ndistinct GROUP BY b, c, d;

EXPLAIN
 SELECT COUNT(*) FROM ndistinct GROUP BY a, d;

DROP TABLE ndistinct;
