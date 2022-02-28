# Copyright (c) 2022, PostgreSQL Global Development Group

# Test partial-column publication of tables
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 27;

# setup

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->append_conf('postgresql.conf',
	qq(max_logical_replication_workers = 6));
$node_subscriber->start;

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

# setup tables on both nodes

# tab1: simple 1:1 replication
$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE tab1 (a int PRIMARY KEY, "B" int, c int)
));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE tab1 (a int PRIMARY KEY, "B" int, c int)
));

# tab2: replication from regular to table with fewer columns
$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE tab2 (a int PRIMARY KEY, b varchar, c int);
));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE tab2 (a int PRIMARY KEY, b varchar)
));

# tab3: simple 1:1 replication with weird column names
$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE tab3 ("a'" int PRIMARY KEY, "B" varchar, "c'" int)
));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE tab3 ("a'" int PRIMARY KEY, "c'" int)
));

# test_part: partitioned tables, with partitioning (including multi-level
# partitioning, and fewer columns on the subscriber)
$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE test_part (a int PRIMARY KEY, b text, c timestamptz) PARTITION BY LIST (a);
	CREATE TABLE test_part_1_1 PARTITION OF test_part FOR VALUES IN (1,2,3);
	CREATE TABLE test_part_2_1 PARTITION OF test_part FOR VALUES IN (4,5,6) PARTITION BY LIST (a);
	CREATE TABLE test_part_2_2 PARTITION OF test_part_2_1 FOR VALUES IN (4,5);
));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE test_part (a int PRIMARY KEY, b text) PARTITION BY LIST (a);
	CREATE TABLE test_part_1_1 PARTITION OF test_part FOR VALUES IN (1,2,3);
	CREATE TABLE test_part_2_1 PARTITION OF test_part FOR VALUES IN (4,5,6) PARTITION BY LIST (a);
	CREATE TABLE test_part_2_2 PARTITION OF test_part_2_1 FOR VALUES IN (4,5);
));

# tab4: table with user-defined enum types
$node_publisher->safe_psql('postgres', qq(
	CREATE TYPE test_typ AS ENUM ('blue', 'red');
	CREATE TABLE tab4 (a INT PRIMARY KEY, b test_typ, c int, d text);
));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TYPE test_typ AS ENUM ('blue', 'red');
	CREATE TABLE tab4 (a INT PRIMARY KEY, b test_typ, d text);
));


# TEST: create publication and subscription for some of the tables with
# column lists
$node_publisher->safe_psql('postgres', qq(
	CREATE PUBLICATION pub1
	   FOR TABLE tab1 (a, "B"), tab3 ("a'", "c'"), test_part (a, b), tab4 (a, b, d)
	  WITH (publish_via_partition_root = 'true');
));

# check that we got the right prattrs values for the publication in the
# pg_publication_rel catalog (order by relname, to get stable ordering)
my $result = $node_publisher->safe_psql('postgres', qq(
	SELECT relname, prattrs
	FROM pg_publication_rel pb JOIN pg_class pc ON(pb.prrelid = pc.oid)
	ORDER BY relname
));

is($result, qq(tab1|1 2
tab3|1 3
tab4|1 2 4
test_part|1 2), 'publication relation updated');

# create subscription for the publication, wait for sync to complete
$node_subscriber->safe_psql('postgres', qq(
	CREATE SUBSCRIPTION sub1 CONNECTION '$publisher_connstr' PUBLICATION pub1
));

$node_publisher->wait_for_catchup('sub1');

# TEST: insert data into the tables, and see we got replication of just
# the filtered columns
$node_publisher->safe_psql('postgres', qq(
	INSERT INTO tab1 VALUES (1, 2, 3);
	INSERT INTO tab1 VALUES (4, 5, 6);
));

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO tab3 VALUES (1, 2, 3);
	INSERT INTO tab3 VALUES (4, 5, 6);
));

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO tab4 VALUES (1, 'red', 3, 'oh my');
	INSERT INTO tab4 VALUES (2, 'blue', 4, 'hello');
));

# replication of partitioned table
$node_publisher->safe_psql('postgres', qq(
	INSERT INTO test_part VALUES (1, 'abc', '2021-07-04 12:00:00');
	INSERT INTO test_part VALUES (2, 'bcd', '2021-07-03 11:12:13');
	INSERT INTO test_part VALUES (4, 'abc', '2021-07-04 12:00:00');
	INSERT INTO test_part VALUES (5, 'bcd', '2021-07-03 11:12:13');
));

# wait for catchup before checking the subscriber
$node_publisher->wait_for_catchup('sub1');

# tab1: only (a,b) is replicated
$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab1");
is($result, qq(1|2|
4|5|), 'insert on column tab1.c is not replicated');

# tab3: only (a,c) is replicated
$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab3");
is($result, qq(1|3
4|6), 'insert on column tab3.b is not replicated');

# tab4: only (a,b,d) is replicated
$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab4");
is($result, qq(1|red|oh my
2|blue|hello), 'insert on column tab4.c is not replicated');

# test_part: (a,b) is replicated
$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM test_part");
is($result, qq(1|abc
2|bcd
4|abc
5|bcd), 'insert on column test_part.c columns is not replicated');

# TEST: do some updated on some of the tables, both on columns included
# in the column list and other

# tab1: update of replicated column
$node_publisher->safe_psql('postgres',
	qq(UPDATE tab1 SET "B" = 2 * "B" where a = 1));

# tab1: update of non-replicated column
$node_publisher->safe_psql('postgres',
	qq(UPDATE tab1 SET c = 2*c where a = 4));

# tab3: update of non-replicated
$node_publisher->safe_psql('postgres',
	qq(UPDATE tab3 SET "B" = "B" || ' updated' where "a'" = 4));

# tab3: update of replicated column
$node_publisher->safe_psql('postgres',
	qq(UPDATE tab3 SET "c'" = 2 * "c'" where "a'" = 1));

# tab4
$node_publisher->safe_psql('postgres',
	qq(UPDATE tab4 SET b = 'blue', c = c * 2, d = d || ' updated' where a = 1));

# tab4
$node_publisher->safe_psql('postgres',
	qq(UPDATE tab4 SET b = 'red', c = c * 2, d = d || ' updated' where a = 2));

$node_publisher->wait_for_catchup('sub1');

$result = $node_subscriber->safe_psql('postgres',
	qq(SELECT * FROM tab1 ORDER BY a));
is($result,
qq(1|4|
4|5|), 'only update on column tab1.b is replicated');

$result = $node_subscriber->safe_psql('postgres',
	qq(SELECT * FROM tab3 ORDER BY "a'"));
is($result,
qq(1|6
4|6), 'only update on column tab3.c is replicated');

$result = $node_subscriber->safe_psql('postgres',
	qq(SELECT * FROM tab4 ORDER BY a));

is($result, qq(1|blue|oh my updated
2|red|hello updated), 'update on column tab4.c is not replicated');


# TEST: add table with a column list, insert data, replicate

# insert some data before adding it to the publication
$node_publisher->safe_psql('postgres', qq(
	INSERT INTO tab2 VALUES (1, 'abc', 3);
));

$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION pub1 ADD TABLE tab2 (a, b)");

$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub1 REFRESH PUBLICATION");

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO tab2 VALUES (2, 'def', 6);
));

$node_publisher->wait_for_catchup('sub1');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab2 ORDER BY a");
is($result, qq(1|abc
2|def), 'insert on column tab2.c is not replicated');

$node_publisher->safe_psql('postgres', qq(
	UPDATE tab2 SET c = 5 where a = 1;
	UPDATE tab2 SET b = 'xyz' where a = 2;
));

$node_publisher->wait_for_catchup('sub1');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab2 ORDER BY a");
is($result, qq(1|abc
2|xyz), 'update on column tab2.c is not replicated');


# TEST: add a table to two publications with different column lists, and
# create a single subscription replicating both publications
$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE tab5 (a int PRIMARY KEY, b int, c int, d int);
	CREATE PUBLICATION pub2 FOR TABLE tab5 (a, b);
	CREATE PUBLICATION pub3 FOR TABLE tab5 (a, d);
));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE tab5 (a int PRIMARY KEY, b int, d int);
));

$node_subscriber->safe_psql('postgres', qq(
	ALTER SUBSCRIPTION sub1 SET PUBLICATION pub2, pub3
));

$node_publisher->wait_for_catchup('sub1');

# TEST: insert data and make sure all the columns (union of the columns lists)
# were replicated
$node_publisher->safe_psql('postgres', "INSERT INTO tab5 VALUES (1, 11, 111, 1111)");
$node_publisher->safe_psql('postgres', "INSERT INTO tab5 VALUES (2, 22, 222, 2222)");

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM tab5 ORDER BY a"),
   qq(1|11|1111
2|22|2222),
   'overlapping publications with overlapping column lists');

# and finally, set the column filter to ALL for one of the publications,
# which means replicating all columns (removing the column filter), but
# first add the missing column to the table on subscriber
$node_publisher->safe_psql('postgres', qq(
	ALTER PUBLICATION pub3 ALTER TABLE tab5 SET COLUMNS ALL;
));

$node_subscriber->safe_psql('postgres', qq(
	ALTER SUBSCRIPTION sub1 REFRESH PUBLICATION;
	ALTER TABLE tab5 ADD COLUMN c INT;
));

$node_publisher->safe_psql('postgres', "INSERT INTO tab5 VALUES (3, 33, 333, 3333)");

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM tab5 ORDER BY a"),
   qq(1|11|1111|
2|22|2222|
3|33|3333|333),
   'overlapping publications with overlapping column lists');

# TEST: create a table with a column filter, then change the replica
# identity by replacing a primary key (but use a different column in
# the column filter)
$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE tab6 (a int PRIMARY KEY, b int, c int, d int);
	CREATE PUBLICATION pub4 FOR TABLE tab6 (a, b);
));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE tab6 (a int PRIMARY KEY, b int, c int, d int);
));

$node_subscriber->safe_psql('postgres', qq(
	ALTER SUBSCRIPTION sub1 SET PUBLICATION pub4
));

$node_publisher->wait_for_catchup('sub1');

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO tab6 VALUES (1, 22, 333, 4444);
	UPDATE tab6 SET b = b * 2, c = c * 3, d = d * 4 WHERE a = 1;
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM tab6 ORDER BY a"),
   "1|44||", 'replication with the original primary key');

# now redefine the constraint - move the primary key to a different column
# (which is still covered by the column list, though)

$node_publisher->safe_psql('postgres', qq(
	ALTER TABLE tab6 DROP CONSTRAINT tab6_pkey;
	ALTER TABLE tab6 ADD PRIMARY KEY (b);
));

# we need to do the same thing on the subscriber
# XXX What would happen if this happens before the publisher ALTER? Or
# interleaved, somehow? But that seems unrelated to column lists.
$node_subscriber->safe_psql('postgres', qq(
	ALTER TABLE tab6 DROP CONSTRAINT tab6_pkey;
	ALTER TABLE tab6 ADD PRIMARY KEY (b);
));

$node_subscriber->safe_psql('postgres', qq(
	ALTER SUBSCRIPTION sub1 REFRESH PUBLICATION
));

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO tab6 VALUES (2, 55, 666, 8888);
	UPDATE tab6 SET b = b * 2, c = c * 3, d = d * 4 WHERE a = 2;
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM tab6 ORDER BY a"),
   qq(1|44||
2|110||),
   'replication with the modified primary key');


# TEST: create a table with a column filter, then change the replica
# identity by replacing a primary key with a key on multiple columns
# (all of them covered by the column filter)
$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE tab7 (a int PRIMARY KEY, b int, c int, d int);
	CREATE PUBLICATION pub5 FOR TABLE tab7 (a, b);
));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE tab7 (a int PRIMARY KEY, b int, c int, d int);
));

$node_subscriber->safe_psql('postgres', qq(
	ALTER SUBSCRIPTION sub1 SET PUBLICATION pub5
));

$node_publisher->wait_for_catchup('sub1');

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO tab7 VALUES (1, 22, 333, 4444);
	UPDATE tab7 SET b = b * 2, c = c * 3, d = d * 4 WHERE a = 1;
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM tab7 ORDER BY a"),
   "1|44||", 'replication with the original primary key');

# now redefine the constraint - move the primary key to a different column
# (which is not covered by the column list)
$node_publisher->safe_psql('postgres', qq(
	ALTER TABLE tab7 DROP CONSTRAINT tab7_pkey;
	ALTER TABLE tab7 ADD PRIMARY KEY (a, b);
));

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO tab7 VALUES (2, 55, 666, 8888);
	UPDATE tab7 SET b = b * 2, c = c * 3, d = d * 4 WHERE a = 2;
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM tab7 ORDER BY a"),
   qq(1|44||
2|110||),
   'replication with the modified primary key');

# now switch the primary key again to another columns not covered by the
# column filter, but also generate writes between the drop and creation
# of the new constraint

$node_publisher->safe_psql('postgres', qq(
	ALTER TABLE tab7 DROP CONSTRAINT tab7_pkey;
	INSERT INTO tab7 VALUES (3, 33, 999, 7777);
	-- update/delete is not allowed for tables without RI
	ALTER TABLE tab7 ADD PRIMARY KEY (b, a);
	UPDATE tab7 SET b = b * 2, c = c * 3, d = d * 4 WHERE a = 3;
	DELETE FROM tab7 WHERE a = 1;
));

$node_publisher->safe_psql('postgres', qq(
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM tab7 ORDER BY a"),
   qq(2|110||
3|66||),
   'replication with the modified primary key');


# TEST: partitioned tables (with publish_via_partition_root = false)
# and replica identity. The (leaf) partitions may have different RI, so
# we need to check the partition RI (with respect to the column filter)
# while attaching the partition.

# First, let's create a partitioned table with two partitions, each with
# a different RI, but a column filter not covering all those RI.

$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE test_part_a (a int, b int, c int) PARTITION BY LIST (a);

	CREATE TABLE test_part_a_1 PARTITION OF test_part_a FOR VALUES IN (1);
	ALTER TABLE test_part_a_1 ADD PRIMARY KEY (a);
	ALTER TABLE test_part_a_1 REPLICA IDENTITY USING INDEX test_part_a_1_pkey;

	CREATE TABLE test_part_a_2 PARTITION OF test_part_a FOR VALUES IN (2);
	ALTER TABLE test_part_a_2 ADD PRIMARY KEY (b);
	ALTER TABLE test_part_a_2 REPLICA IDENTITY USING INDEX test_part_a_2_pkey;
));

# do the same thing on the subscriber (with the opposite column order)
$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE test_part_a (b int, a int) PARTITION BY LIST (a);

	CREATE TABLE test_part_a_1 PARTITION OF test_part_a FOR VALUES IN (1);
	ALTER TABLE test_part_a_1 ADD PRIMARY KEY (a);
	ALTER TABLE test_part_a_1 REPLICA IDENTITY USING INDEX test_part_a_1_pkey;

	CREATE TABLE test_part_a_2 PARTITION OF test_part_a FOR VALUES IN (2);
	ALTER TABLE test_part_a_2 ADD PRIMARY KEY (b);
	ALTER TABLE test_part_a_2 REPLICA IDENTITY USING INDEX test_part_a_2_pkey;
));

# create a publication replicating just the column "a", which is not enough
# for the second partition
$node_publisher->safe_psql('postgres', qq(
	CREATE PUBLICATION pub6 FOR TABLE test_part_a (b, a) WITH (publish_via_partition_root = true);
	ALTER PUBLICATION pub6 ADD TABLE test_part_a_1 (a);
	ALTER PUBLICATION pub6 ADD TABLE test_part_a_2 (b);
));

# add the publication to our subscription, wait for sync to complete
$node_subscriber->safe_psql('postgres', qq(
	ALTER SUBSCRIPTION sub1 SET PUBLICATION pub6
));

$node_publisher->wait_for_catchup('sub1');

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO test_part_a VALUES (1, 3);
	INSERT INTO test_part_a VALUES (2, 4);
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT a, b FROM test_part_a ORDER BY a, b"),
   qq(1|3
2|4),
   'partitions with different replica identities not replicated correctly');


# This time start with a column filter covering RI for all partitions, but
# then update the column filter to not cover column "b" (needed by the
# second partition)

$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE test_part_b (a int, b int) PARTITION BY LIST (a);

	CREATE TABLE test_part_b_1 PARTITION OF test_part_b FOR VALUES IN (1,3);
	ALTER TABLE test_part_b_1 ADD PRIMARY KEY (a);
	ALTER TABLE test_part_b_1 REPLICA IDENTITY USING INDEX test_part_b_1_pkey;

	CREATE TABLE test_part_b_2 PARTITION OF test_part_b FOR VALUES IN (2,4);
	ALTER TABLE test_part_b_2 ADD PRIMARY KEY (b);
	ALTER TABLE test_part_b_2 REPLICA IDENTITY USING INDEX test_part_b_2_pkey;
));

# do the same thing on the subscriber
$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE test_part_b (a int, b int) PARTITION BY LIST (a);

	CREATE TABLE test_part_b_1 PARTITION OF test_part_b FOR VALUES IN (1,3);
	ALTER TABLE test_part_b_1 ADD PRIMARY KEY (a);
	ALTER TABLE test_part_b_1 REPLICA IDENTITY USING INDEX test_part_b_1_pkey;

	CREATE TABLE test_part_b_2 PARTITION OF test_part_b FOR VALUES IN (2,4);
	ALTER TABLE test_part_b_2 ADD PRIMARY KEY (b);
	ALTER TABLE test_part_b_2 REPLICA IDENTITY USING INDEX test_part_b_2_pkey;
));

# create a publication replicating both columns, which is sufficient for
# both partitions
$node_publisher->safe_psql('postgres', qq(
	CREATE PUBLICATION pub7 FOR TABLE test_part_b (a, b) WITH (publish_via_partition_root = true);
));

# add the publication to our subscription, wait for sync to complete
$node_subscriber->safe_psql('postgres', qq(
	ALTER SUBSCRIPTION sub1 SET PUBLICATION pub7
));

$node_publisher->wait_for_catchup('sub1');

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO test_part_b VALUES (1, 1);
	INSERT INTO test_part_b VALUES (2, 2);
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM test_part_b ORDER BY a, b"),
   qq(1|1
2|2),
   'partitions with different replica identities not replicated correctly');


# TEST: This time start with a column filter covering RI for all partitions,
# but then update RI for one of the partitions to not be covered by the
# column filter anymore.

$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE test_part_c (a int, b int, c int) PARTITION BY LIST (a);

	CREATE TABLE test_part_c_1 PARTITION OF test_part_c FOR VALUES IN (1,3);
	ALTER TABLE test_part_c_1 ADD PRIMARY KEY (a);
	ALTER TABLE test_part_c_1 REPLICA IDENTITY USING INDEX test_part_c_1_pkey;

	CREATE TABLE test_part_c_2 PARTITION OF test_part_c FOR VALUES IN (2,4);
	ALTER TABLE test_part_c_2 ADD PRIMARY KEY (b);
	ALTER TABLE test_part_c_2 REPLICA IDENTITY USING INDEX test_part_c_2_pkey;
));

# do the same thing on the subscriber
$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE test_part_c (a int, b int, c int) PARTITION BY LIST (a);

	CREATE TABLE test_part_c_1 PARTITION OF test_part_c FOR VALUES IN (1,3);
	ALTER TABLE test_part_c_1 ADD PRIMARY KEY (a);
	ALTER TABLE test_part_c_1 REPLICA IDENTITY USING INDEX test_part_c_1_pkey;

	CREATE TABLE test_part_c_2 PARTITION OF test_part_c FOR VALUES IN (2,4);
	ALTER TABLE test_part_c_2 ADD PRIMARY KEY (b);
	ALTER TABLE test_part_c_2 REPLICA IDENTITY USING INDEX test_part_c_2_pkey;
));

# create a publication replicating data through partition root, with a column
# filter on the root, and then add the partitions one by one with separate
# column filters (but those are not applied)
$node_publisher->safe_psql('postgres', qq(
	CREATE PUBLICATION pub8 FOR TABLE test_part_c WITH (publish_via_partition_root = false);
	ALTER PUBLICATION pub8 ADD TABLE test_part_c_1 (a,c);
	ALTER PUBLICATION pub8 ADD TABLE test_part_c_2 (a,b);
));

# add the publication to our subscription, wait for sync to complete
$node_subscriber->safe_psql('postgres', qq(
	DROP SUBSCRIPTION sub1;
	CREATE SUBSCRIPTION sub1 CONNECTION '$publisher_connstr' PUBLICATION pub8;
));

$node_publisher->wait_for_catchup('sub1');

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO test_part_c VALUES (1, 3, 5);
	INSERT INTO test_part_c VALUES (2, 4, 6);
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM test_part_c ORDER BY a, b"),
   qq(1||5
2|4|),
   'partitions with different replica identities not replicated correctly');


# create a publication not replicating data through partition root, without
# a column filter on the root, and then add the partitions one by one with
# separate column filters
$node_publisher->safe_psql('postgres', qq(
	DROP PUBLICATION pub8;
	CREATE PUBLICATION pub8 FOR TABLE test_part_c WITH (publish_via_partition_root = false);
	ALTER PUBLICATION pub8 ADD TABLE test_part_c_1 (a);
	ALTER PUBLICATION pub8 ADD TABLE test_part_c_2 (a,b);
));

# add the publication to our subscription, wait for sync to complete
$node_subscriber->safe_psql('postgres', qq(
	ALTER SUBSCRIPTION sub1 REFRESH PUBLICATION;
	TRUNCATE test_part_c;
));

$node_publisher->wait_for_catchup('sub1');

$node_publisher->safe_psql('postgres', qq(
	TRUNCATE test_part_c;
	INSERT INTO test_part_c VALUES (1, 3, 5);
	INSERT INTO test_part_c VALUES (2, 4, 6);
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM test_part_c ORDER BY a, b"),
   qq(1||
2|4|),
   'partitions with different replica identities not replicated correctly');


# TEST: Start with a single partition, with RI compatible with the column
# filter, and then attach a partition with incompatible RI.

$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE test_part_d (a int, b int) PARTITION BY LIST (a);

	CREATE TABLE test_part_d_1 PARTITION OF test_part_d FOR VALUES IN (1,3);
	ALTER TABLE test_part_d_1 ADD PRIMARY KEY (a);
	ALTER TABLE test_part_d_1 REPLICA IDENTITY USING INDEX test_part_d_1_pkey;
));

# do the same thing on the subscriber (in fact, create both partitions right
# away, no need to delay that)
$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE test_part_d (a int, b int) PARTITION BY LIST (a);

	CREATE TABLE test_part_d_1 PARTITION OF test_part_d FOR VALUES IN (1,3);
	ALTER TABLE test_part_d_1 ADD PRIMARY KEY (a);
	ALTER TABLE test_part_d_1 REPLICA IDENTITY USING INDEX test_part_d_1_pkey;

	CREATE TABLE test_part_d_2 PARTITION OF test_part_d FOR VALUES IN (2,4);
	ALTER TABLE test_part_d_2 ADD PRIMARY KEY (a);
	ALTER TABLE test_part_d_2 REPLICA IDENTITY USING INDEX test_part_d_2_pkey;
));

# create a publication replicating both columns, which is sufficient for
# both partitions
$node_publisher->safe_psql('postgres', qq(
	CREATE PUBLICATION pub9 FOR TABLE test_part_d (a) WITH (publish_via_partition_root = true);
));

# add the publication to our subscription, wait for sync to complete
$node_subscriber->safe_psql('postgres', qq(
	ALTER SUBSCRIPTION sub1 SET PUBLICATION pub9
));

$node_publisher->wait_for_catchup('sub1');

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO test_part_d VALUES (1, 1);
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM test_part_d ORDER BY a, b"),
   qq(1|),
   'partitions with different replica identities not replicated correctly');

# TEST: With a table included in multiple publications, we should use a
# union of the column filters. So with column filters (a,b) and (a,c) we
# should replicate (a,b,c).

$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE test_mix_1 (a int PRIMARY KEY, b int, c int);
	CREATE PUBLICATION pub_mix_1 FOR TABLE test_mix_1 (a, b);
	CREATE PUBLICATION pub_mix_2 FOR TABLE test_mix_1 (a, c);
));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE test_mix_1 (a int PRIMARY KEY, b int, c int);
	ALTER SUBSCRIPTION sub1 SET PUBLICATION pub_mix_1, pub_mix_2;
));

$node_publisher->wait_for_catchup('sub1');

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO test_mix_1 VALUES (1, 2, 3);
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM test_mix_1"),
   qq(1|2|3),
   'a mix of publications should use a union of column filter');


# TEST: With a table included in multiple publications, we should use a
# union of the column filters. If any of the publications is FOR ALL
# TABLES, we should replicate all columns.

# drop unnecessary tables, so as not to interfere with the FOR ALL TABLES
$node_publisher->safe_psql('postgres', qq(
	DROP TABLE tab1, tab2, tab3, tab4, tab5, tab6, tab7, test_mix_1,
			   test_part, test_part_a, test_part_b, test_part_c, test_part_d;
));

$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE test_mix_2 (a int PRIMARY KEY, b int, c int);
	CREATE PUBLICATION pub_mix_3 FOR TABLE test_mix_2 (a, b);
	CREATE PUBLICATION pub_mix_4 FOR ALL TABLES;
));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE test_mix_2 (a int PRIMARY KEY, b int, c int);
	ALTER SUBSCRIPTION sub1 SET PUBLICATION pub_mix_3, pub_mix_4;
	ALTER SUBSCRIPTION sub1 REFRESH PUBLICATION;
));

$node_publisher->wait_for_catchup('sub1');

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO test_mix_2 VALUES (1, 2, 3);
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM test_mix_2"),
   qq(1|2|3),
   'a mix of publications should use a union of column filter');


# TEST: With a table included in multiple publications, we should use a
# union of the column filters. If any of the publications is FOR ALL
# TABLES IN SCHEMA, we should replicate all columns.

$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE test_mix_3 (a int PRIMARY KEY, b int, c int);
	CREATE PUBLICATION pub_mix_5 FOR TABLE test_mix_3 (a, b);
	CREATE PUBLICATION pub_mix_6 FOR ALL TABLES IN SCHEMA public;
));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE test_mix_3 (a int PRIMARY KEY, b int, c int);
	ALTER SUBSCRIPTION sub1 SET PUBLICATION pub_mix_5, pub_mix_6;
));

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO test_mix_3 VALUES (1, 2, 3);
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM test_mix_3"),
   qq(1|2|3),
   'a mix of publications should use a union of column filter');


# TEST: Check handling of publish_via_partition_root - if a partition is
# published through partition root, we should only apply the column filter
# defined for the whole table (not the partitions) - both during the initial
# sync and when replicating changes. This is what we do for row filters.

$node_publisher->safe_psql('postgres', qq(
	CREATE TABLE test_root (a int PRIMARY KEY, b int, c int) PARTITION BY RANGE (a);
	CREATE TABLE test_root_1 PARTITION OF test_root FOR VALUES FROM (1) TO (10);
	CREATE TABLE test_root_2 PARTITION OF test_root FOR VALUES FROM (10) TO (20);

	CREATE PUBLICATION pub_root_true FOR TABLE test_root (a) WITH (publish_via_partition_root = true);
));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE test_root (a int PRIMARY KEY, b int, c int) PARTITION BY RANGE (a);
	CREATE TABLE test_root_1 PARTITION OF test_root FOR VALUES FROM (1) TO (10);
	CREATE TABLE test_root_2 PARTITION OF test_root FOR VALUES FROM (10) TO (20);
));

$node_subscriber->safe_psql('postgres', qq(
	ALTER SUBSCRIPTION sub1 SET PUBLICATION pub_root_true;
));

$node_publisher->safe_psql('postgres', qq(
	INSERT INTO test_root VALUES (1, 2, 3);
	INSERT INTO test_root VALUES (10, 20, 30);
));

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres',"SELECT * FROM test_root ORDER BY a, b, c"),
   qq(1||
10||),
   'publication via partition root applies column filter');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');

done_testing();
