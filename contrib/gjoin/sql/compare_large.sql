LOAD 'gjoin';
SET gjoin.enabled = false;
SET work_mem = '128kB';

CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (c INT, d INT);

CREATE OR REPLACE FUNCTION compare_joins(sql TEXT) RETURNS bool AS $$
DECLARE
    v_custom_rows BIGINT;
    v_rows        BIGINT;
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


INSERT INTO t1 SELECT i, 100000 * random() FROM generate_series(1,1000000) s(i);
INSERT INTO t2 SELECT i, 100000 * random() FROM generate_series(1,1000000) s(i);

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c AND t1.b < t2.d)');


TRUNCATE t1;
TRUNCATE t2;

INSERT INTO t1 SELECT i, 100000 * random() FROM generate_series(1,1000000) s(i) ORDER BY random();
INSERT INTO t2 SELECT i, 100000 * random() FROM generate_series(1,1000000) s(i) ORDER BY random();

VACUUM ANALYZE t1;
VACUUM ANALYZE t2;

SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c)');
SELECT compare_joins('SELECT * FROM t1 JOIN t2 ON (t1.a = t2.c AND t1.b < t2.d)');


DROP TABLE t1;
DROP TABLE t2;
