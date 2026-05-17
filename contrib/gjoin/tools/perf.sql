LOAD 'gjoin';
SET gjoin.enabled = false;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE OR REPLACE FUNCTION query_timing(sql TEXT, runs INT DEFAULT 1) RETURNS INT AS $$
DECLARE
    v_timing FLOAT;
    v_start_time FLOAT;
    v_rows   BIGINT;
BEGIN

    v_start_time := extract(epoch from clock_timestamp());

    FOR r IN 1..runs LOOP
        EXECUTE 'SELECT COUNT(*) FROM (' || sql || ')' INTO v_rows;
    END LOOP;

    v_timing := extract(epoch from clock_timestamp()) - v_start_time;

    RETURN ((v_timing * 1000) / runs)::int;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION joins_timing(work_mem TEXT, sql TEXT, OUT nestloop_timing INT, OUT hashjoin_timing INT, OUT mergejoin_timing INT, OUT gjoin_timing INT) RETURNS record AS $$
BEGIN

    PERFORM set_config('work_mem', work_mem, false);

    -- no parallelism for fair comparison
    PERFORM set_config('max_parallel_workers_per_gather', '0', false);

    -- disable all joins
    PERFORM set_config('enable_nestloop', '0', false);
    PERFORM set_config('enable_hashjoin', '0', false);
    PERFORM set_config('enable_mergejoin', '0', false);
    PERFORM set_config('gjoin.enabled', '0', false);

    PERFORM set_config('enable_nestloop', '1', false);
    nestloop_timing := query_timing(sql);
    PERFORM set_config('enable_nestloop', '0', false);

    PERFORM set_config('enable_hashjoin', '1', false);
    hashjoin_timing := query_timing(sql);
    PERFORM set_config('enable_hashjoin', '0', false);

    PERFORM set_config('enable_mergejoin', '1', false);
    mergejoin_timing := query_timing(sql);
    PERFORM set_config('enable_mergejoin', '0', false);

    PERFORM set_config('gjoin.enabled', '1', false);
    gjoin_timing := query_timing(sql);
    PERFORM set_config('gjoin.enabled', '0', false);

END;
$$ LANGUAGE plpgsql;

-- 10k

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

INSERT INTO t1 SELECT i, 100000 * random() FROM generate_series(1,10000) s(i);
INSERT INTO t2 SELECT i, 100000 * random() FROM generate_series(1,10000) s(i);

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT * FROM unnest(ARRAY['64kB', '256kB', '1MB', '4MB', '16MB', '64MB']) AS work_mem, LATERAL joins_timing(work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');

DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

INSERT INTO t1 SELECT i, 100000 * random() FROM generate_series(1,10000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i, 100000 * random() FROM generate_series(1,10000) s(i) ORDER BY random();

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT * FROM unnest(ARRAY['64kB', '256kB', '1MB', '4MB', '16MB', '64MB']) AS work_mem, LATERAL joins_timing(work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');

DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

INSERT INTO t1 SELECT i/10, 100000 * random() FROM generate_series(1,10000) s(i);
INSERT INTO t2 SELECT i/10, 100000 * random() FROM generate_series(1,10000) s(i);

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT * FROM unnest(ARRAY['64kB', '256kB', '1MB', '4MB', '16MB', '64MB']) AS work_mem, LATERAL joins_timing(work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');

DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

INSERT INTO t1 SELECT i/10, 100000 * random() FROM generate_series(1,10000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i/10, 100000 * random() FROM generate_series(1,10000) s(i) ORDER BY random();

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT * FROM unnest(ARRAY['64kB', '256kB', '1MB', '4MB', '16MB', '64MB']) AS work_mem, LATERAL joins_timing(work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');

DROP TABLE t1;
DROP TABLE t2;

-- 100k

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

INSERT INTO t1 SELECT i, 100000 * random() FROM generate_series(1,100000) s(i);
INSERT INTO t2 SELECT i, 100000 * random() FROM generate_series(1,100000) s(i);

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT * FROM unnest(ARRAY['64kB', '256kB', '1MB', '4MB', '16MB', '64MB']) AS work_mem, LATERAL joins_timing(work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');

DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

INSERT INTO t1 SELECT i, 100000 * random() FROM generate_series(1,100000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i, 100000 * random() FROM generate_series(1,100000) s(i) ORDER BY random();

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT * FROM unnest(ARRAY['64kB', '256kB', '1MB', '4MB', '16MB', '64MB']) AS work_mem, LATERAL joins_timing(work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');

DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

INSERT INTO t1 SELECT i/10, 100000 * random() FROM generate_series(1,100000) s(i);
INSERT INTO t2 SELECT i/10, 100000 * random() FROM generate_series(1,100000) s(i);

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT * FROM unnest(ARRAY['64kB', '256kB', '1MB', '4MB', '16MB', '64MB']) AS work_mem, LATERAL joins_timing(work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');

DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

INSERT INTO t1 SELECT i/10, 100000 * random() FROM generate_series(1,100000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i/10, 100000 * random() FROM generate_series(1,100000) s(i) ORDER BY random();

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT * FROM unnest(ARRAY['64kB', '256kB', '1MB', '4MB', '16MB', '64MB']) AS work_mem, LATERAL joins_timing(work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');

DROP TABLE t1;
DROP TABLE t2;

-- 1M

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

INSERT INTO t1 SELECT i, 100000 * random() FROM generate_series(1,1000000) s(i);
INSERT INTO t2 SELECT i, 100000 * random() FROM generate_series(1,1000000) s(i);

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT * FROM unnest(ARRAY['64kB', '256kB', '1MB', '4MB', '16MB', '64MB']) AS work_mem, LATERAL joins_timing(work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');

DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

INSERT INTO t1 SELECT i, 100000 * random() FROM generate_series(1,1000000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i, 100000 * random() FROM generate_series(1,1000000) s(i) ORDER BY random();

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT * FROM unnest(ARRAY['64kB', '256kB', '1MB', '4MB', '16MB', '64MB']) AS work_mem, LATERAL joins_timing(work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');

DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

INSERT INTO t1 SELECT i/10, 100000 * random() FROM generate_series(1,1000000) s(i);
INSERT INTO t2 SELECT i/10, 100000 * random() FROM generate_series(1,1000000) s(i);

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT * FROM unnest(ARRAY['64kB', '256kB', '1MB', '4MB', '16MB', '64MB']) AS work_mem, LATERAL joins_timing(work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');

DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

INSERT INTO t1 SELECT i/10, 100000 * random() FROM generate_series(1,1000000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i/10, 100000 * random() FROM generate_series(1,1000000) s(i) ORDER BY random();

CREATE INDEX ON t1 (a);
CREATE INDEX ON t2 (c);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT * FROM unnest(ARRAY['64kB', '256kB', '1MB', '4MB', '16MB', '64MB']) AS work_mem, LATERAL joins_timing(work_mem, 'SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');

DROP TABLE t1;
DROP TABLE t2;
