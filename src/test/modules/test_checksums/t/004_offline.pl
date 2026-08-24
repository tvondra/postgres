
# Copyright (c) 2026, PostgreSQL Global Development Group

# Test suite for testing enabling data checksums offline from various states
# of checksum processing
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;

use DataChecksums::Utils;

sub check_cluster
{
	my ($node, $state, $name) = @_;

	# Run a dummy query just to make sure we can read back some data
	my $res =
	  $node->safe_psql('postgres', "SELECT count(*) FROM t WHERE a > 1");
	is($res, '999', 'ensure checksummed pages can be read back on ' . $name);
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, 0);
	unlike(
		$log,
		qr/page verification failed,.+\d$/m,
		"no checksum validation errors in $name log (during WAL recovery)");
}

# Initialize node with checksums disabled.
my $node = PostgreSQL::Test::Cluster->new('offline_node');
$node->init(no_data_checksums => 1);
$node->start;

# Make sure pg_control_init reports the initial state as disabled
my $result = $node->safe_psql('postgres',
	'SELECT data_page_checksum_version FROM pg_control_init();');
is($result, '0', 'ensure pg_control_init reports disabled state');

# Create some content to have un-checksummed data in the cluster
$node->safe_psql('postgres',
	"CREATE TABLE t AS SELECT generate_series(1,10000) AS a;");

# Ensure that checksums are disabled
test_checksum_state($node, 'off');

# Enable checksums offline using pg_checksums
$node->stop;
$node->checksum_enable_offline;
$node->start;

# Ensure that checksums are enabled
test_checksum_state($node, 'on');

# Since offline checksums don't issue a checkpoint like online checksums, the
# first call to pg_control_checkpoint will show the state as off even though
# checksums are enabled.  After a CHECKPOINT, pg_control_checkpoint shall
# return 1.
$result = $node->safe_psql('postgres',
	'SELECT data_page_checksum_version FROM pg_control_checkpoint();');
is($result, '0', 'latest checkpoint will still see off state');
$node->safe_psql('postgres', 'CHECKPOINT;');
$result = $node->safe_psql('postgres',
	'SELECT data_page_checksum_version FROM pg_control_checkpoint();');
is($result, '1', 'latest checkpoint will now see on state');

# Make sure pg_control_init still reports the initial state as disabled even
# though the current state has changed.
$result = $node->safe_psql('postgres',
	'SELECT data_page_checksum_version FROM pg_control_init();');
is($result, '0', 'ensure pg_control_init still reports disabled state');

# Run a dummy query just to make sure we can read back some data
$result = $node->safe_psql('postgres', "SELECT count(*) FROM t WHERE a > 1");
is($result, '9999', 'ensure checksummed pages can be read back');

# Disable checksums offline again using pg_checksums
$node->stop;
$node->checksum_disable_offline;
$node->start;

# Ensure that checksums are disabled
test_checksum_state($node, 'off');

# Create a barrier for checksum enablement to block on, in this case a pre-
# existing temporary table which is kept open while processing is started. We
# can accomplish this by setting up an interactive psql process which keeps the
# temporary table created as we enable checksums in another psql process.

my $bsession = $node->background_psql('postgres');
$bsession->query_safe('CREATE TEMPORARY TABLE tt (a integer);');

# In another session, make sure we can see the blocking temp table but start
# processing anyways and check that we are blocked with a proper wait event.
$result = $node->safe_psql('postgres',
	"SELECT relpersistence FROM pg_catalog.pg_class WHERE relname = 'tt';");
is($result, 't', 'ensure we can see the temporary table');

# Enable, but stop waiting at inprogress-on since it will sit there until the
# above temporary table is removed.
enable_data_checksums($node, wait => 'inprogress-on');

# Turn the cluster off and enable checksums offline, then start back up.
# Stop the cluster before exiting the background session since otherwise
# checksums might have time to get enabled before shutting down the cluster.
$node->stop('fast');
$bsession->quit;
$node->checksum_enable_offline;
$node->start;

# Ensure that checksums are now enabled even though processing wasn't
# restarted
test_checksum_state($node, 'on');

# Run a dummy query just to make sure we can read back some data
$result = $node->safe_psql('postgres', "SELECT count(*) FROM t WHERE a > 1");
is($result, '9999', 'ensure checksummed pages can be read back');

$node->stop;
$node->clean_node;

#############################################################################
# Testing offline data checksum operations in a replicated cluster
#

# --------------------------------------------------------------------------
# First test a successful offline checksum enable sequence where checksums
# are enabled in a cluster after having been initdb'd without checksums
my $node_primary = PostgreSQL::Test::Cluster->new('offline_primary');
$node_primary->init(allows_streaming => 1, no_data_checksums => 1);
$node_primary->start;
my $slotname = 'physical_slot';
$node_primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('$slotname');");
my $backup_name = 'backup_one';
$node_primary->backup($backup_name);

my $node_standby = PostgreSQL::Test::Cluster->new('offline_standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->append_conf('postgresql.conf',
	"primary_slot_name = '$slotname'");
$node_standby->start;

$node_primary->safe_psql('postgres',
	"CREATE TABLE t AS SELECT generate_series(1,1000) AS a;");
$node_primary->wait_for_catchup($node_standby, 'replay',
	$node_primary->lsn('insert'));

# All nodes should have checksums turned off
test_checksum_state($node_primary, 'off');
test_checksum_state($node_standby, 'off');

# Enable checksums on all nodes offline
$node_standby->stop;
$node_primary->stop;
$node_primary->checksum_enable_offline;
$node_standby->checksum_enable_offline;

# Verify the inprogress state in the controlfile
my ($stdout, $stderr) =
  run_command([ 'pg_controldata', $node_primary->data_dir ]);
like(
	$stdout,
	qr/Data page checksum version:\s+4/,
	'Checksum in pg_control on primary is INPROGRESS_ON_OFFLINE');
($stdout, $stderr) =
  run_command([ 'pg_controldata', $node_standby->data_dir ]);
like(
	$stdout,
	qr/Data page checksum version:\s+4/,
	'Checksum in pg_control on standby is INPROGRESS_ON_OFFLINE');

# All nodes should now have checksums turned on after enabling offline
$node_primary->start;
$node_standby->start;
$node_primary->wait_for_catchup($node_standby, 'replay');
test_checksum_state($node_primary, 'on');
test_checksum_state($node_standby, 'on');

# Test retrieving data and read the logs to make sure
check_cluster($node_primary, 'on', 'primary');
check_cluster($node_standby, 'on', 'standby');

$node_standby->stop;
$node_primary->stop;

# Verify the on state in the controlfile
($stdout, $stderr) =
  run_command([ 'pg_controldata', $node_primary->data_dir ]);
like(
	$stdout,
	qr/Data page checksum version:\s+1/,
	'Checksum in pg_control on primary is on');
($stdout, $stderr) =
  run_command([ 'pg_controldata', $node_standby->data_dir ]);
like(
	$stdout,
	qr/Data page checksum version:\s+1/,
	'Checksum in pg_control on standby is on');

# --------------------------------------------------------------------------
# Test creating a mismatched cluster where the standby has checksums disabled
# while they remain on on the primary
$node_standby->checksum_disable_offline;
$node_primary->start;
test_checksum_state($node_primary, 'on');
my $a = $node_standby->start(fail_ok => 1);
is($a, 0, "Standby could not start");
my $log = PostgreSQL::Test::Utils::slurp_file($node_standby->logfile, 0);
unlike(
	$log,
	qr/page verification failed,.+\d$/m,
	"no checksum validation errors in standby log (during WAL recovery)");
$node_standby->clean_node;

# --------------------------------------------------------------------------
# Recreate the standby from a primary base backup and make sure it has the
# same state as the primary.  Then do a disable/enable cycle online and then
# disable checksums offline on the primary.  When restarted, the standby must
# fail to start.
$backup_name = 'backup_two';
$node_primary->backup($backup_name);
$node_standby = PostgreSQL::Test::Cluster->new('offline_standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->append_conf('postgresql.conf',
	"primary_slot_name = '$slotname'");
$node_standby->start;
$node_primary->wait_for_catchup($node_standby, 'replay');

test_checksum_state($node_primary, 'on');
test_checksum_state($node_standby, 'on');

check_cluster($node_primary, 'on', 'primary');
check_cluster($node_standby, 'on', 'standby');

disable_data_checksums($node_primary, wait => 1);
test_checksum_state($node_primary, 'off');
$node_primary->wait_for_catchup($node_standby, 'replay');
test_checksum_state($node_standby, 'off');

enable_data_checksums($node_primary, wait => 'on');
test_checksum_state($node_primary, 'on');
$node_primary->wait_for_catchup($node_standby, 'replay');
test_checksum_state($node_standby, 'on');

$node_primary->stop;
$node_standby->stop;

$node_primary->checksum_disable_offline;
$node_primary->start;
test_checksum_state($node_primary, 'off');
my $status = $node_standby->start(fail_ok => 1);
is($status, 0, 'Standby fails with mismatched checksum state, p:off/s:on');

$node_primary->stop;
$node_primary->clean_node;
$node_standby->clean_node;

# --------------------------------------------------------------------------
# Re-initialize node with checksums disabled and then create a standby where
# checksums are enabled offline with pg_checksums.  The primary has in this
# test never had checksums enabled at any point.
$node_primary = PostgreSQL::Test::Cluster->new('offline_node');
$node_primary->init(allows_streaming => 1, no_data_checksums => 1);
$node_primary->start;
$node_primary->safe_psql('postgres',
	"CREATE TABLE t AS SELECT generate_series(1,10000) AS a;");
$node_primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('$slotname');");

# Create the standby
$backup_name = "backup_three";
$node_primary->backup($backup_name);
$node_standby = PostgreSQL::Test::Cluster->new('offline_standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->append_conf('postgresql.conf',
	"primary_slot_name = '$slotname'");
$node_standby->start;
$node_primary->wait_for_catchup($node_standby, 'replay');

# Now enable data checksums on the standby offline and restart, the node should
# now refuse to start due to mismatched data checksums state with the primary
$node_standby->stop;
$node_standby->checksum_enable_offline;
$status = $node_standby->start(fail_ok => 1);
is($status, 0, 'Standby fails with mismatched checksum state p:off/s:on');

# Test cleanup
$node_standby->clean_node;
$node_primary->stop;

done_testing();
