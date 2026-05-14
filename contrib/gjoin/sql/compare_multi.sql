LOAD 'gjoin';
SET gjoin.enabled = false;

CREATE TABLE t1 (a1 INT, a2 INT, a3 INT);
CREATE TABLE t2 (b1 INT, b2 INT, b3 INT);

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


INSERT INTO t1 SELECT i, i, 1000 * random() FROM generate_series(1,10000) s(i);
INSERT INTO t2 SELECT i, i, 1000 * random() FROM generate_series(1,10000) s(i);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a1 = t2.b1 AND t1.a2 = t2.b2)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a1 = t2.b1 AND t1.a2 = t2.b2 AND t1.a3 < t2.b3)');


TRUNCATE t1;
TRUNCATE t2;

INSERT INTO t1 SELECT i, i, 1000 * random() FROM generate_series(1,10000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i, i, 1000 * random() FROM generate_series(1,10000) s(i) ORDER BY random();

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a1 = t2.b1 AND t1.a2 = t2.b2)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a1 = t2.b1 AND t1.a2 = t2.b2 AND t1.a3 < t2.b3)');


TRUNCATE t1;
TRUNCATE t2;

INSERT INTO t1 SELECT mod(i,17), mod(i, 37), 1000 * random() FROM generate_series(1,10000) s(i);
INSERT INTO t2 SELECT mod(i,19), mod(i, 31), 1000 * random() FROM generate_series(1,10000) s(i);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a1 = t2.b1 AND t1.a2 = t2.b2)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a1 = t2.b1 AND t1.a2 = t2.b2 AND t1.a3 < t2.b3)');


TRUNCATE t1;
TRUNCATE t2;

INSERT INTO t1 SELECT mod(i,17), mod(i, 37), 1000 * random() FROM generate_series(1,10000) s(i) ORDER BY random();
INSERT INTO t2 SELECT mod(i,19), mod(i, 31), 1000 * random() FROM generate_series(1,10000) s(i) ORDER BY random();

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a1 = t2.b1 AND t1.a2 = t2.b2)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a1 = t2.b1 AND t1.a2 = t2.b2 AND t1.a3 < t2.b3)');


TRUNCATE t1;
TRUNCATE t2;

INSERT INTO t1 SELECT mod(i,17), mod(i, 37), 1000 * random() FROM generate_series(1,10000) s(i);
INSERT INTO t2 SELECT mod(i,17), mod(i, 37), 1000 * random() FROM generate_series(1,10000) s(i);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a1 = t2.b1 AND t1.a2 = t2.b2)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a1 = t2.b1 AND t1.a2 = t2.b2 AND t1.a3 < t2.b3)');


TRUNCATE t1;
TRUNCATE t2;

INSERT INTO t1 SELECT mod(i,17), mod(i, 37), 1000 * random() FROM generate_series(1,10000) s(i) ORDER BY random();
INSERT INTO t2 SELECT mod(i,17), mod(i, 37), 1000 * random() FROM generate_series(1,10000) s(i) ORDER BY random();

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a1 = t2.b1 AND t1.a2 = t2.b2)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a1 = t2.b1 AND t1.a2 = t2.b2 AND t1.a3 < t2.b3)');


DROP TABLE t1;
DROP TABLE t2;
