-- data type passed by value
CREATE TABLE multi_stats (
    a INT,
    b INT,
    c INT,
    d INT,
    e INT,
    f INT,
    g INT,
    h INT
);

-- MCV list on (a,b)
CREATE STATISTICS m1 WITH (mcv) ON (a, b) FROM multi_stats;

-- histogram on (c,d)
CREATE STATISTICS m2 WITH (histogram) ON (c, d) FROM multi_stats;

-- functional dependencies on (e,f)
CREATE STATISTICS m3 WITH (dependencies) ON (e, f) FROM multi_stats;

-- ndistinct coefficients on (g,h)
CREATE STATISTICS m4 WITH (ndistinct) ON (g, h) FROM multi_stats;

-- perfectly correlated groups
INSERT INTO multi_stats
SELECT
    i, i/2,      -- MCV
    i, i + j,    -- histogram
    k, k/2,      -- dependencies
    l/5, l/10    -- ndistinct
FROM (
    SELECT
        mod(x, 13)   AS i,
        mod(x, 17)   AS j,
        mod(x, 11)   AS k,
        mod(x, 51)   AS l
    FROM generate_series(1,30000) AS s(x)
) foo;

ANALYZE multi_stats;

EXPLAIN SELECT * FROM multi_stats
         WHERE (a = 8) AND (b = 4) AND (c >= 3) AND (d <= 10);

EXPLAIN SELECT * FROM multi_stats
         WHERE (a = 8) AND (b = 4) AND (e = 10) AND (f = 5);

EXPLAIN SELECT * FROM multi_stats
         WHERE (a = 8) AND (b = 4) AND (e = 10) AND (f = 5);

EXPLAIN SELECT * FROM multi_stats
         WHERE (a = 8) AND (b = 4) AND (g = 2) AND (h = 1);

EXPLAIN SELECT * FROM multi_stats
         WHERE (a = 8) AND (b = 4) AND
               (c >= 3) AND (d <= 10) AND
               (e = 10) AND (f = 5);

DROP TABLE multi_stats;
