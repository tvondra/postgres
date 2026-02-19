# Tests that an INSERT on referencing table correctly fails when
# the referenced value disappears due to a concurrent update
setup
{
  CREATE TABLE parent (
    parent_key int PRIMARY KEY,
    aux   text NOT NULL
  );

  CREATE TABLE child (
    child_key int PRIMARY KEY,
    parent_key int8 NOT NULL REFERENCES parent
  );

  INSERT INTO parent VALUES (1, 'foo');
}

teardown
{
  DROP TABLE parent, child;
}

session s1
setup  { BEGIN; }
step s1i { INSERT INTO child VALUES (1, 1); }
step s1c { COMMIT; }
step s1s { SELECT * FROM child; }

session s2
setup  { BEGIN; }
step s2ukey { UPDATE parent SET parent_key = 2 WHERE parent_key = 1; }
step s2uaux { UPDATE parent SET aux = 'bar' WHERE parent_key = 1; }
step s2ukey2 { UPDATE parent SET parent_key = 1 WHERE parent_key = 2; }
step s2c { COMMIT; }
step s2s { SELECT * FROM parent; }

# fail
permutation s2ukey s1i s2c s1c s2s s1s
# ok
permutation s2uaux s1i s2c s1c s2s s1s
# ok
permutation s2ukey s1i s2ukey2 s2c s1c s2s s1s
