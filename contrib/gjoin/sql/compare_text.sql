LOAD 'gjoin';
SET gjoin.enabled = false;

CREATE TABLE t1 (a TEXT, b TEXT);
CREATE TABLE t2 (c TEXT, d TEXT);

CREATE OR REPLACE FUNCTION compare_joins(sql TEXT) RETURNS bool AS $$
DECLARE
    v_custom_rows INT;
    v_rows        INT;
BEGIN

    PERFORM set_config('gjoin.enabled', '1', false);
    EXECUTE 'SELECT COUNT(*) FROM (' || sql || ')' INTO v_custom_rows;

    PERFORM set_config('gjoin.enabled', '0', false);
    EXECUTE 'SELECT COUNT(*) FROM (' || sql || ')' INTO v_rows;

    IF v_rows != v_custom_rows THEN
        RAISE WARNING '% %', v_rows, v_custom_rows;
    END IF;

    RETURN (v_rows = v_custom_rows);
END;
$$ LANGUAGE plpgsql;


INSERT INTO t1 SELECT i, md5(random()::text) FROM generate_series(1,10000) s(i);
INSERT INTO t2 SELECT i, md5(random()::text) FROM generate_series(1,10000) s(i);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c AND t1.b < t2.d)');


TRUNCATE t1;
TRUNCATE t2;

INSERT INTO t1 SELECT i, md5(random()::text) FROM generate_series(1,10000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i, md5(random()::text) FROM generate_series(1,10000) s(i) ORDER BY random();

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c AND t1.b < t2.d)');


TRUNCATE t1;
TRUNCATE t2;

INSERT INTO t1 SELECT mod(i,1000), md5(random()::text) FROM generate_series(1,10000) s(i);
INSERT INTO t2 SELECT mod(i,1000), md5(random()::text) FROM generate_series(1,10000) s(i);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c AND t1.b < t2.d)');


TRUNCATE t1;
TRUNCATE t2;

INSERT INTO t1 SELECT mod(i,1000), md5(random()::text) FROM generate_series(1,10000) s(i) ORDER BY random();
INSERT INTO t2 SELECT mod(i,1000), md5(random()::text) FROM generate_series(1,10000) s(i) ORDER BY random();

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c AND t1.b < t2.d)');


TRUNCATE t1;
TRUNCATE t2;

INSERT INTO t1 SELECT mod(i,100), md5(random()::text) FROM generate_series(1,10000) s(i);
INSERT INTO t2 SELECT mod(i,100), md5(random()::text) FROM generate_series(1,10000) s(i);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c AND t1.b < t2.d)');


TRUNCATE t1;
TRUNCATE t2;

INSERT INTO t1 SELECT mod(i,100), md5(random()::text) FROM generate_series(1,10000) s(i) ORDER BY random();
INSERT INTO t2 SELECT mod(i,100), md5(random()::text) FROM generate_series(1,10000) s(i) ORDER BY random();

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c AND t1.b < t2.d)');


DROP TABLE t1;
DROP TABLE t2;
