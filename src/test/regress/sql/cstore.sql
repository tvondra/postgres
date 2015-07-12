-- create two fake cstore AM instances (no implementation)
INSERT INTO pg_cstore_am VALUES ('bar1', 1, 1, 1, 1);
INSERT INTO pg_cstore_am VALUES ('bar2', 1, 1, 1, 1);

-- column-level column store definition

-- unknown name of a column store AM
CREATE TABLE test_columnar_single_missing (
    a INT,
    b INT COLUMN STORE foo USING bar,
    c INT
);

-- conflicting column store name (same AM)
CREATE TABLE test_columnar_single_conflict (
    a INT,
    b INT COLUMN STORE foo USING bar1,
    c INT COLUMN STORE foo USING bar1,
    d INT
);

-- conflicting column store name (different AM)
CREATE TABLE test_columnar_single_conflict2 (
    a INT,
    b INT COLUMN STORE foo USING bar1,
    c INT COLUMN STORE foo USING bar2,
    d INT
);

-- correct definition (single store) 
CREATE TABLE test_columnar_single_ok (
    a INT,
    b INT COLUMN STORE foo1 USING bar1,
    c INT
);

-- check contents of the catalogs
SELECT cstname, cstnatts, cstatts
  FROM pg_cstore
 WHERE cstrelid = 'test_columnar_single_ok'::regclass;

-- basic pg_class attributes
SELECT relnamespace, reltype, reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_single_ok'::regclass)
 ORDER BY oid;

-- pg_class flags
SELECT relnatts, relchecks, relhasoids, relhaspkey, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relispopulated
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_single_ok'::regclass)
 ORDER BY oid;
 
-- pg_attribute entries (for the cstore)
SELECT attnum, attname, atttypid, attstattarget
  FROM pg_attribute WHERE attrelid IN (
    SELECT cststoreid
      FROM pg_cstore
     WHERE cstname = 'test_columnar_single_ok')
 ORDER BY attrelid, attnum;

-- correct definition (two stores, same AM) 
CREATE TABLE test_columnar_single_ok2 (
    a INT,
    b INT COLUMN STORE foo1 USING bar1,
    c INT COLUMN STORE foo2 USING bar1,
    d INT
);

-- check contents of the catalogs
SELECT cstname, cstnatts, cstatts
  FROM pg_cstore
 WHERE cstrelid = 'test_columnar_single_ok2'::regclass;

-- basic pg_class attributes
SELECT relnamespace, reltype, reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_single_ok2'::regclass)
 ORDER BY oid;

-- pg_class flags
SELECT relnatts, relchecks, relhasoids, relhaspkey, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relispopulated
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_single_ok2'::regclass)
 ORDER BY oid;
 
-- pg_attribute entries (for the cstore)
SELECT attnum, attname, atttypid, attstattarget
  FROM pg_attribute WHERE attrelid IN (
    SELECT cststoreid
      FROM pg_cstore
     WHERE cstname = 'test_columnar_single_ok2')
 ORDER BY attrelid, attnum;

-- correct definition (two stores, different AMs)
CREATE TABLE test_columnar_single_ok3 (
    a INT,
    b INT COLUMN STORE foo1 USING bar1,
    c INT COLUMN STORE foo2 USING bar1,
    d INT
);

-- check contents of the catalogs
SELECT cstname, cstnatts, cstatts
  FROM pg_cstore
 WHERE cstrelid = 'test_columnar_single_ok3'::regclass;

-- basic pg_class attributes
SELECT relnamespace, reltype, reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_single_ok3'::regclass)
 ORDER BY oid;

-- pg_class flags
SELECT relnatts, relchecks, relhasoids, relhaspkey, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relispopulated
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_single_ok3'::regclass)
 ORDER BY oid;
 
-- pg_attribute entries (for the cstore)
SELECT attnum, attname, atttypid, attstattarget
  FROM pg_attribute WHERE attrelid IN (
    SELECT cststoreid
      FROM pg_cstore
     WHERE cstname = 'test_columnar_single_ok3')
 ORDER BY attrelid, attnum;


-- table-level column store definition

-- unknown name of a column store AM
CREATE TABLE test_columnar_multi_missing (
    a INT,
    b INT,
    c INT,
    d INT,
    COLUMN STORE foo USING bar (b,c)
);

-- conflicting column store name (same AM)
CREATE TABLE test_columnar_multi_conflict (
    a INT,
    b INT,
    c INT,
    d INT,
    COLUMN STORE foo USING bar1 (a,b),
    COLUMN STORE foo USING bar1 (c,d)
);

-- conflicting column store name (different AM)
CREATE TABLE test_columnar_multi_conflict2 (
    a INT,
    b INT,
    c INT,
    d INT,
    COLUMN STORE foo USING bar1 (a,b),
    COLUMN STORE foo USING bar2 (c,d)
);

-- overlapping list of columns
CREATE TABLE test_columnar_multi_conflict3 (
    a INT,
    b INT,
    c INT,
    d INT,
    COLUMN STORE foo1 USING bar1 (a,b),
    COLUMN STORE foo2 USING bar2 (b,c,d)
);

-- correct definition (single store) 
CREATE TABLE test_columnar_multi_ok (
    a INT,
    b INT,
    c INT,
    COLUMN STORE foo USING bar1 (a,b)
);

-- check contents of the catalogs
SELECT cstname, cstnatts, cstatts
  FROM pg_cstore
 WHERE cstrelid = 'test_columnar_multi_ok'::regclass;

-- basic pg_class attributes
SELECT relnamespace, reltype, reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_multi_ok'::regclass)
 ORDER BY oid;

-- pg_class flags
SELECT relnatts, relchecks, relhasoids, relhaspkey, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relispopulated
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_multi_ok'::regclass)
 ORDER BY oid;
 
-- pg_attribute entries (for the cstore)
SELECT attnum, attname, atttypid, attstattarget
  FROM pg_attribute WHERE attrelid IN (
    SELECT cststoreid
      FROM pg_cstore
     WHERE cstname = 'test_columnar_multi_ok')
 ORDER BY attrelid, attnum;

-- correct definition (two stores, same AM) 
CREATE TABLE test_columnar_multi_ok2 (
    a INT,
    b INT,
    c INT,
    d INT,
    COLUMN STORE foo1 USING bar1 (a,b),
    COLUMN STORE foo2 USING bar1 (c,d)
);

-- check contents of the catalogs
SELECT cstname, cstnatts, cstatts
  FROM pg_cstore
 WHERE cstrelid = 'test_columnar_multi_ok2'::regclass;

-- basic pg_class attributes
SELECT relnamespace, reltype, reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_multi_ok2'::regclass)
 ORDER BY oid;

-- pg_class flags
SELECT relnatts, relchecks, relhasoids, relhaspkey, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relispopulated
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_multi_ok2'::regclass)
 ORDER BY oid;
 
-- pg_attribute entries (for the cstore)
SELECT attnum, attname, atttypid, attstattarget
  FROM pg_attribute WHERE attrelid IN (
    SELECT cststoreid
      FROM pg_cstore
     WHERE cstname = 'test_columnar_multi_ok2')
 ORDER BY attrelid, attnum;

-- correct definition (two stores, different AMs)
CREATE TABLE test_columnar_multi_ok3 (
    a INT,
    b INT,
    c INT,
    d INT,
    COLUMN STORE foo1 USING bar1 (a,b),
    COLUMN STORE foo2 USING bar2 (d)
);

-- check contents of the catalogs
SELECT cstname, cstnatts, cstatts
  FROM pg_cstore
 WHERE cstrelid = 'test_columnar_multi_ok3'::regclass;

-- basic pg_class attributes
SELECT relnamespace, reltype, reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_multi_ok3'::regclass)
 ORDER BY oid;

-- pg_class flags
SELECT relnatts, relchecks, relhasoids, relhaspkey, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relispopulated
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_multi_ok3'::regclass)
 ORDER BY oid;
 
-- pg_attribute entries (for the cstore)
SELECT attnum, attname, atttypid, attstattarget
  FROM pg_attribute WHERE attrelid IN (
    SELECT cststoreid
      FROM pg_cstore
     WHERE cstname = 'test_columnar_multi_ok3')
 ORDER BY attrelid, attnum;


-- combination of column-level and table-level column stores

-- conflicting column store name (same AM)
CREATE TABLE test_columnar_multi_conflict (
    a INT,
    b INT COLUMN STORE foo USING bar1,
    c INT,
    d INT,
    COLUMN STORE foo USING bar1 (c,d)
);

-- conflicting column store name (different AM)
CREATE TABLE test_columnar_combi_conflict2 (
    a INT,
    b INT COLUMN STORE foo USING bar1,
    c INT,
    d INT,
    COLUMN STORE foo USING bar2 (c,d)
);

-- overlapping list of columns
CREATE TABLE test_columnar_combi_conflict3 (
    a INT,
    b INT COLUMN STORE foo USING bar1,
    c INT,
    d INT,
    COLUMN STORE foo2 USING bar2 (b,c,d)
);

-- correct definition (two stores, same AM) 
CREATE TABLE test_columnar_combi_ok (
    a INT,
    b INT COLUMN STORE foo USING bar1,
    c INT,
    d INT,
    COLUMN STORE foo2 USING bar1 (c,d)
);

-- check contents of the catalogs
SELECT cstname, cstnatts, cstatts
  FROM pg_cstore
 WHERE cstrelid = 'test_columnar_combi_ok'::regclass;

-- basic pg_class attributes
SELECT relnamespace, reltype, reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_combi_ok'::regclass)
 ORDER BY oid;

-- pg_class flags
SELECT relnatts, relchecks, relhasoids, relhaspkey, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relispopulated
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_combi_ok'::regclass)
 ORDER BY oid;
 
-- pg_attribute entries (for the cstore)
SELECT attnum, attname, atttypid, attstattarget
  FROM pg_attribute WHERE attrelid IN (
    SELECT cststoreid
      FROM pg_cstore
     WHERE cstname = 'test_columnar_combi_ok')
 ORDER BY attrelid, attnum;

-- correct definition (two stores, different AMs)
CREATE TABLE test_columnar_combi_ok2 (
    a INT,
    b INT COLUMN STORE foo USING bar1,
    c INT,
    d INT,
    COLUMN STORE foo2 USING bar2 (d)
);

-- check contents of the catalogs
SELECT cstname, cstnatts, cstatts
  FROM pg_cstore
 WHERE cstrelid = 'test_columnar_combi_ok2'::regclass;

-- basic pg_class attributes
SELECT relnamespace, reltype, reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_combi_ok2'::regclass)
 ORDER BY oid;

-- pg_class flags
SELECT relnatts, relchecks, relhasoids, relhaspkey, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relispopulated
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'test_columnar_combi_ok2'::regclass)
 ORDER BY oid;
 
-- pg_attribute entries (for the cstore)
SELECT attnum, attname, atttypid, attstattarget
  FROM pg_attribute WHERE attrelid IN (
    SELECT cststoreid
      FROM pg_cstore
     WHERE cstname = 'test_columnar_combi_ok2')
 ORDER BY attrelid, attnum;

-- test cleanup
CREATE TABLE cstore_oids AS SELECT oid FROM pg_class WHERE relkind = 'C';

DROP TABLE test_columnar_single_ok;
DROP TABLE test_columnar_single_ok2;
DROP TABLE test_columnar_single_ok3;

DROP TABLE test_columnar_multi_ok;
DROP TABLE test_columnar_multi_ok2;
DROP TABLE test_columnar_multi_ok3;

DROP TABLE test_columnar_combi_ok;
DROP TABLE test_columnar_combi_ok2;

-- should return 0
SELECT COUNT(*) FROM pg_class WHERE OID IN (SELECT oid FROM cstore_oids);
SELECT COUNT(*) FROM pg_cstore WHERE cststoreid IN (SELECT oid FROM cstore_oids);
SELECT COUNT(*) FROM pg_attribute WHERE attrelid IN (SELECT oid FROM cstore_oids);

DROP TABLE cstore_oids;

-- delete the fake cstore AM records
DELETE FROM pg_cstore_am;
