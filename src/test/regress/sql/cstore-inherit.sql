-- create two fake cstore AM instances (no implementation)
INSERT INTO pg_cstore_am VALUES ('bar11', 1, 1, 1, 1);
INSERT INTO pg_cstore_am VALUES ('bar12', 1, 1, 1, 1);

-- parent table with two column stores
CREATE TABLE parent_table (
    a INT,
    b INT COLUMN STORE foo1 USING bar11,
    c INT,
    d INT,
    e INT,
    COLUMN STORE foo2 USING bar12 (d,e)
);

-- check contents of the catalogs
SELECT cstname, cstnatts, cstatts
  FROM pg_cstore
 WHERE cstrelid = 'parent_table'::regclass
 ORDER BY cstname;

-- basic pg_class attributes
SELECT relnamespace, reltype, reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'parent_table'::regclass)
 ORDER BY oid;

-- pg_class flags
SELECT relnatts, relchecks, relhasoids, relhaspkey, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relispopulated
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'parent_table'::regclass)
 ORDER BY oid;
 
-- pg_attribute entries (for the cstore)
SELECT cstname, attnum, attname, atttypid, attstattarget
  FROM pg_attribute JOIN pg_cstore ON (attrelid = cststoreid)
 WHERE cstrelid = 'parent_table'::regclass
 ORDER BY cstname, attnum;

-- child table with two separate column stores
CREATE TABLE child_table_1 (
    f INT,
    g INT COLUMN STORE foo1c USING bar11,
    h INT,
    i INT,
    COLUMN STORE foo2c USING bar12(h,i)
) INHERITS (parent_table);

-- check contents of the catalogs
SELECT cstname, cstnatts, cstatts
  FROM pg_cstore
 WHERE cstrelid = 'child_table_1'::regclass
 ORDER BY cstname;

-- basic pg_class attributes
SELECT relnamespace, reltype, reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'child_table_1'::regclass)
 ORDER BY oid;

-- pg_class flags
SELECT relnatts, relchecks, relhasoids, relhaspkey, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relispopulated
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'child_table_1'::regclass)
 ORDER BY oid;
 
-- pg_attribute entries (for the cstore)
SELECT cstname, attnum, attname, atttypid, attstattarget
  FROM pg_attribute JOIN pg_cstore ON (attrelid = cststoreid)
 WHERE cstrelid = 'child_table_1'::regclass
 ORDER BY cstname, attnum;

-- child table with two column stores - one modifying, one redefining the parent
CREATE TABLE child_table_2 (
    f INT,
    g INT COLUMN STORE foo1c USING bar11, -- new column store
    h INT,
    i INT,
    COLUMN STORE foo2c USING bar12(b,h,i) -- redefines the parent colstore
) INHERITS (parent_table);

-- check contents of the catalogs
SELECT cstname, cstnatts, cstatts
  FROM pg_cstore
 WHERE cstrelid = 'child_table_2'::regclass
 ORDER BY cstname;

-- basic pg_class attributes
SELECT relnamespace, reltype, reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'child_table_2'::regclass)
 ORDER BY oid;

-- pg_class flags
SELECT relnatts, relchecks, relhasoids, relhaspkey, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relispopulated
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'child_table_2'::regclass)
 ORDER BY oid;
 
-- pg_attribute entries (for the cstore)
SELECT cstname, attnum, attname, atttypid, attstattarget
  FROM pg_attribute JOIN pg_cstore ON (attrelid = cststoreid)
 WHERE cstrelid = 'child_table_2'::regclass
 ORDER BY cstname, attnum;

-- child table with a single column store of the whole table
CREATE TABLE child_table_3 (
    f INT,
    g INT,
    h INT,
    i INT,
    COLUMN STORE foo1 USING bar11(a,b,c,d,e,f,g,h,i)
) INHERITS (parent_table);

-- check contents of the catalogs
SELECT cstname, cstnatts, cstatts
  FROM pg_cstore
 WHERE cstrelid = 'child_table_3'::regclass
 ORDER BY cstname;

-- basic pg_class attributes
SELECT relnamespace, reltype, reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'child_table_3'::regclass)
 ORDER BY oid;

-- pg_class flags
SELECT relnatts, relchecks, relhasoids, relhaspkey, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relispopulated
  FROM pg_class WHERE oid IN (SELECT cststoreid FROM pg_cstore WHERE cstrelid = 'child_table_3'::regclass)
 ORDER BY oid;
 
-- pg_attribute entries (for the cstore)
SELECT cstname, attnum, attname, atttypid, attstattarget
  FROM pg_attribute JOIN pg_cstore ON (attrelid = cststoreid)
 WHERE cstrelid = 'child_table_3'::regclass
 ORDER BY cstname, attnum;

-- test cleanup
CREATE TABLE cstore_oids AS
SELECT cststoreid
  FROM pg_cstore JOIN pg_class ON (pg_cstore.cstrelid = pg_class.oid)
 WHERE relname IN ('parent_table', 'child_table_1', 'child_table_2', 'child_table_3');

CREATE TABLE cstore_oids_2 AS
SELECT pg_class.oid
  FROM pg_class JOIN cstore_oids ON (pg_class.oid = cstore_oids.cststoreid);

DROP TABLE child_table_1;
DROP TABLE child_table_2;
DROP TABLE child_table_3;
DROP TABLE parent_table;

-- should return 0
SELECT COUNT(*) FROM pg_class WHERE oid IN (SELECT cststoreid FROM cstore_oids);
SELECT COUNT(*) FROM pg_class WHERE oid IN (SELECT oid FROM cstore_oids_2);

SELECT COUNT(*) FROM pg_cstore WHERE cststoreid IN (SELECT oid FROM cstore_oids);

SELECT COUNT(*) FROM pg_attribute WHERE attrelid IN (SELECT cststoreid FROM cstore_oids);
SELECT COUNT(*) FROM pg_attribute WHERE attrelid IN (SELECT oid FROM cstore_oids_2);

DROP TABLE cstore_oids;
DROP TABLE cstore_oids_2;

-- delete the fake cstore AM records
DELETE FROM pg_cstore_am;
