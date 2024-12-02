# index-only-scan visibility test
#
# This test checks that index-only scans don't return incorrect results
# when vacuum marks pages as all-visible while a cursor is active.
#
setup
{
    -- by using a low fillfactor and a wide tuple we can get multiple blocks
    -- with just few rows
    CREATE TABLE ios_vis_test (a int NOT NULL, b int not null, pad char(1024) default '')
    WITH (AUTOVACUUM_ENABLED = false, FILLFACTOR = 10);

    INSERT INTO ios_vis_test SELECT g.i, g.i FROM generate_series(1, 10) g(i);

    CREATE INDEX ios_vis_test_a ON ios_vis_test (a);
}

teardown
{
    DROP TABLE ios_vis_test;
}


session s1

# Force an index-only scan, where possible:
setup {
    SET enable_bitmapscan = false;
    SET enable_indexonlyscan = true;
    SET enable_indexscan = true;
}

step s1_begin { BEGIN; }
step s1_commit { COMMIT; }

step s1_prepare {
    DECLARE foo NO SCROLL CURSOR FOR SELECT a FROM ios_vis_test WHERE a > 0;
}

step s1_fetch_1 {
    FETCH FROM foo;
}

step s1_fetch_all {
    FETCH ALL FROM foo;
}


session s2

# Don't delete row 1 so we have a row for the cursor to "rest" on.
step s2_mod
{
  DELETE FROM ios_vis_test WHERE a > 1;
}

# Disable truncation, as otherwise we'll just wait for a timeout while trying
# to acquire the lock
step s2_vacuum  { VACUUM (TRUNCATE false) ios_vis_test; }

permutation
  # Vacuum first, to ensure VM exists, otherwise the bitmapscan will consider
  # VM to be size 0, due to caching. Can't do that in setup because
  s2_vacuum

  # delete nearly all rows, to make issue visible
  s2_mod
  # create a cursor
  s1_begin
  s1_prepare

  # fetch one row from the cursor, that ensures the index scan portion is done
  # before the vacuum in the next step
  s1_fetch_1

  # with the bug this vacuum will mark pages as all-visible that the scan in
  # the next step then considers all-visible, despite all rows from those
  # pages having been removed.
  s2_vacuum
  # if this returns any rows, we're busted
  s1_fetch_all

  s1_commit
