LOAD 'gjoin';
SET gjoin.enabled = false;

SET gjoin.batch_size = 1024;
SET gjoin.max_runs = 8;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

DROP TABLE IF EXISTS results;
CREATE TABLE results(rows int, ndistinct INT, dataset TEXT, work_mem INT, join_algo TEXT, run INT, timing NUMERIC);

CREATE OR REPLACE FUNCTION query_timing(sql TEXT, runs INT DEFAULT 5) RETURNS numeric AS $$
DECLARE
    v_timing numeric;
    v_start_time FLOAT;
    v_rows   BIGINT;
BEGIN

    v_start_time := extract(epoch from clock_timestamp());

    EXECUTE 'SELECT COUNT(*) FROM (' || sql || ')' INTO v_rows;
    --EXECUTE sql || ' offset 1000000000' INTO v_rows;

    v_timing := extract(epoch from clock_timestamp()) - v_start_time;

    RETURN v_timing;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION joins_timing(rows INT, ndistinct INT, dataset TEXT, work_mem INT, sql TEXT, runs INT DEFAULT 5) RETURNS void AS $$
DECLARE
    v_timing NUMERIC := 0;
BEGIN

    PERFORM set_config('work_mem', work_mem::text, false);

    -- no parallelism for fair comparison
    PERFORM set_config('max_parallel_workers_per_gather', '0', false);

    -- disable all joins
    PERFORM set_config('enable_nestloop', '0', false);
    PERFORM set_config('enable_hashjoin', '0', false);
    PERFORM set_config('enable_mergejoin', '0', false);
    PERFORM set_config('gjoin.enabled', '0', false);

    PERFORM set_config('enable_nestloop', '1', false);
    FOR v_run IN 1..runs LOOP
        v_timing := query_timing(sql);
        INSERT INTO results (rows, ndistinct, dataset, work_mem, join_algo, run, timing) VALUES (rows, ndistinct, dataset, work_mem, 'nestloop', v_run, v_timing);
    END LOOP;
    PERFORM set_config('enable_nestloop', '0', false);

    PERFORM set_config('enable_hashjoin', '1', false);
    FOR v_run IN 1..runs LOOP
        v_timing := query_timing(sql);
        INSERT INTO results (rows, ndistinct, dataset, work_mem, join_algo, run, timing) VALUES (rows, ndistinct, dataset, work_mem, 'hashjoin', v_run, v_timing);
    END LOOP;
    PERFORM set_config('enable_hashjoin', '0', false);

    PERFORM set_config('enable_mergejoin', '1', false);
    FOR v_run IN 1..runs LOOP
        v_timing := query_timing(sql);
        INSERT INTO results (rows, ndistinct, dataset, work_mem, join_algo, run, timing) VALUES (rows, ndistinct, dataset, work_mem, 'mergejoin', v_run, v_timing);
    END LOOP;
    PERFORM set_config('enable_mergejoin', '0', false);

    PERFORM set_config('gjoin.enabled', '1', false);
    FOR v_run IN 1..runs LOOP
        v_timing := query_timing(sql);
        INSERT INTO results (rows, ndistinct, dataset, work_mem, join_algo, run, timing) VALUES (rows, ndistinct, dataset, work_mem, 'gjoin', v_run, v_timing);
    END LOOP;
    PERFORM set_config('gjoin.enabled', '0', false);

END;
$$ LANGUAGE plpgsql;

-- 10k

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);
CREATE TABLE t3 (e INT, f INT);

INSERT INTO t1 SELECT i, i FROM generate_series(1,10000) s(i);
INSERT INTO t2 SELECT i, i FROM generate_series(1,10000) s(i);
INSERT INTO t3 SELECT i, i FROM generate_series(1,10000) s(i);

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);
CREATE INDEX ON t3 (e);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;
VACUUM ANALYZE t3;

SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '10000', 'linear', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '10000', 'linear_3', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c) JOIN t3 ON (t1.b = t3.e)');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);
CREATE TABLE t3 (e INT, f INT);

INSERT INTO t1 SELECT i, i FROM generate_series(1,10000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i, i FROM generate_series(1,10000) s(i) ORDER BY random();
INSERT INTO t3 SELECT i, i FROM generate_series(1,10000) s(i) ORDER BY random();

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);
CREATE INDEX ON t3 (e);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;
VACUUM ANALYZE t3;

SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '10000', 'random', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '10000', 'random_3', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c) JOIN t3 ON (t1.b = t3.e)');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);
CREATE TABLE t3 (e INT, f INT);

INSERT INTO t1 SELECT i/10, i/10 FROM generate_series(1,10000) s(i);
INSERT INTO t2 SELECT i/10, i/10 FROM generate_series(1,10000) s(i);
INSERT INTO t3 SELECT i/10, i/10 FROM generate_series(1,10000) s(i);

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);
CREATE INDEX ON t3 (e);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;
VACUUM ANALYZE t3;

SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'linear', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'linear_3', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c) JOIN t3 ON (t1.b = t3.e)');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);
CREATE TABLE t3 (e INT, f INT);

INSERT INTO t1 SELECT i/10, i/10 FROM generate_series(1,10000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i/10, i/10 FROM generate_series(1,10000) s(i) ORDER BY random();
INSERT INTO t3 SELECT i/10, i/10 FROM generate_series(1,10000) s(i) ORDER BY random();

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);
CREATE INDEX ON t3 (e);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;
VACUUM ANALYZE t3;

SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random_3', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c) JOIN t3 ON (t1.b = t3.e)');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

-- 100k

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);
CREATE TABLE t3 (e INT, f INT);

INSERT INTO t1 SELECT i, i FROM generate_series(1,100000) s(i);
INSERT INTO t2 SELECT i, i FROM generate_series(1,100000) s(i);
INSERT INTO t3 SELECT i, i FROM generate_series(1,100000) s(i);

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);
CREATE INDEX ON t3 (e);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;
VACUUM ANALYZE t3;

SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random_3', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c) JOIN t3 ON (t1.b = t3.e)');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);
CREATE TABLE t3 (e INT, f INT);

INSERT INTO t1 SELECT i, i FROM generate_series(1,100000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i, i FROM generate_series(1,100000) s(i) ORDER BY random();
INSERT INTO t3 SELECT i, i FROM generate_series(1,100000) s(i) ORDER BY random();

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);
CREATE INDEX ON t3 (e);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;
VACUUM ANALYZE t3;

SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random_3', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c) JOIN t3 ON (t1.b = t3.e)');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);
CREATE TABLE t3 (e INT, f INT);

INSERT INTO t1 SELECT i/10, i/10 FROM generate_series(1,100000) s(i);
INSERT INTO t2 SELECT i/10, i/10 FROM generate_series(1,100000) s(i);
INSERT INTO t3 SELECT i/10, i/10 FROM generate_series(1,100000) s(i);

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);
CREATE INDEX ON t3 (e);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;
VACUUM ANALYZE t3;

SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random_3', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c) JOIN t3 ON (t1.b = t3.e)');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);
CREATE TABLE t3 (e INT, f INT);

INSERT INTO t1 SELECT i/10, i/10 FROM generate_series(1,100000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i/10, i/10 FROM generate_series(1,100000) s(i) ORDER BY random();
INSERT INTO t3 SELECT i/10, i/10 FROM generate_series(1,100000) s(i) ORDER BY random();

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);
CREATE INDEX ON t3 (e);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;
VACUUM ANALYZE t3;

SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random_3', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c) JOIN t3 ON (t1.b = t3.e)');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

-- 1M

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);
CREATE TABLE t3 (e INT, f INT);

INSERT INTO t1 SELECT i, i FROM generate_series(1,1000000) s(i);
INSERT INTO t2 SELECT i, i FROM generate_series(1,1000000) s(i);
INSERT INTO t3 SELECT i, i FROM generate_series(1,1000000) s(i);

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);
CREATE INDEX ON t3 (e);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;
VACUUM ANALYZE t3;

SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random_3', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c) JOIN t3 ON (t1.b = t3.e)');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);
CREATE TABLE t3 (e INT, f INT);

INSERT INTO t1 SELECT i, i FROM generate_series(1,1000000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i, i FROM generate_series(1,1000000) s(i) ORDER BY random();
INSERT INTO t3 SELECT i, i FROM generate_series(1,1000000) s(i) ORDER BY random();

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);
CREATE INDEX ON t3 (e);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;
VACUUM ANALYZE t3;

SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random_3', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c) JOIN t3 ON (t1.b = t3.e)');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);
CREATE TABLE t3 (e INT, f INT);

INSERT INTO t1 SELECT i/10, i/10 FROM generate_series(1,1000000) s(i);
INSERT INTO t2 SELECT i/10, i/10 FROM generate_series(1,1000000) s(i);
INSERT INTO t3 SELECT i/10, i/10 FROM generate_series(1,1000000) s(i);

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);
CREATE INDEX ON t3 (e);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;
VACUUM ANALYZE t3;

SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random_3', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c) JOIN t3 ON (t1.b = t3.e)');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);
CREATE TABLE t3 (e INT, f INT);

INSERT INTO t1 SELECT i/10, i/10 FROM generate_series(1,1000000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i/10, i/10 FROM generate_series(1,1000000) s(i) ORDER BY random();
INSERT INTO t3 SELECT i/10, i/10 FROM generate_series(1,1000000) s(i) ORDER BY random();

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);
CREATE INDEX ON t3 (e);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;
VACUUM ANALYZE t3;

SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT * FROM unnest(ARRAY[64, 256, 1024, 4096, 16384, 65536]) AS work_mem, LATERAL joins_timing('10000', '1000', 'random_3', work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c) JOIN t3 ON (t1.b = t3.e)');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

-- generate report with results
WITH results AS (
    SELECT
        rows,
        ndistinct,
        dataset,
        work_mem,
        ROUND(AVG(1000 * timing) FILTER (WHERE join_algo = 'nestloop'),2) AS nestloop_avg,
        ROUND(STDDEV(1000 * timing) FILTER (WHERE join_algo = 'nestloop'),2) AS nestloop_std,
        ROUND(AVG(1000 * timing) FILTER (WHERE join_algo = 'hashjoin'),2) AS hashjoin_avg,
        ROUND(STDDEV(1000 * timing) FILTER (WHERE join_algo = 'hashjoin'),2) AS hashjoin_std,
        ROUND(AVG(1000 * timing) FILTER (WHERE join_algo = 'mergejoin'),2) AS mergejoin_avg,
        ROUND(STDDEV(1000 * timing) FILTER (WHERE join_algo = 'mergejoin'),2) AS mergejoin_std,
        ROUND(AVG(1000 * timing) FILTER (WHERE join_algo = 'gjoin'),2) AS gjoin_avg,
        ROUND(STDDEV(1000 * timing) FILTER (WHERE join_algo = 'gjoin'),2) AS gjoin_std
    FROM results
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4
)
SELECT
    (CASE WHEN show_rows THEN rows ELSE NULL END) AS rows,
    (CASE WHEN show_ndistinct OR show_rows THEN ndistinct ELSE NULL END) AS ndistinct,
    (CASE WHEN show_dataset OR show_ndistinct OR show_rows THEN dataset ELSE NULL END) AS dataset,
    (CASE WHEN show_work_mem OR show_dataset OR show_ndistinct OR show_rows THEN work_mem ELSE NULL END) AS work_mem,
    nestloop_avg, hashjoin_avg, mergejoin_avg, gjoin_avg,
    nestloop_std, hashjoin_std, mergejoin_std, gjoin_std
FROM (
    SELECT
        (CASE WHEN LAG(rows,1) OVER (ORDER BY 1, 2, 3, 4) != rows OR LAG(rows,1) OVER (ORDER BY 1, 2, 3, 4) IS NULL THEN true ELSE false END) AS show_rows,
        (CASE WHEN LAG(ndistinct,1) OVER (ORDER BY 1, 2, 3, 4) != ndistinct OR LAG(ndistinct,1) OVER (ORDER BY 1, 2, 3, 4) IS NULL THEN true ELSE false END) AS show_ndistinct,
        (CASE WHEN LAG(dataset,1) OVER (ORDER BY 1, 2, 3, 4) != dataset OR LAG(dataset,1) OVER (ORDER BY 1, 2, 3, 4) IS NULL THEN true ELSE false END) AS show_dataset,
        (CASE WHEN LAG(work_mem,1) OVER (ORDER BY 1, 2, 3, 4) != work_mem OR LAG(work_mem,1) OVER (ORDER BY 1, 2, 3, 4) IS NULL THEN true ELSE false END) AS show_work_mem,
        rows, ndistinct, dataset, work_mem,
        nestloop_avg, hashjoin_avg, mergejoin_avg, gjoin_avg,
        nestloop_std, hashjoin_std, mergejoin_std, gjoin_std
    FROM
        results
);
