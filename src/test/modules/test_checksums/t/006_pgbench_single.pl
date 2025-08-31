
# Copyright (c) 2025, PostgreSQL Global Development Group

# Test suite for testing enabling data checksums in an online cluster with
# concurrent activity via pgbench runs

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;

use DataChecksums::Utils;

my $node_primary;
my $node_primary_loglocation = 0;

# The number of full test iterations which will be performed. The exact number
# of tests performed and the wall time taken is non-deterministic as the test
# performs a lot of randomized actions, but 50 iterations will be a long test
# run regardless.
my $TEST_ITERATIONS = 2000;

# Variables which record the current state of the cluster
my $data_checksum_state = 'off';

my $pgbench_primary = undef;

# Variables holding state for managing the cluster and aux processes in
# various ways
my @stop_modes = ();
my ($pgb_primary_stdin, $pgb_primary_stdout, $pgb_primary_stderr) =
  ('', '', '');

if (!$ENV{PG_TEST_EXTRA} || $ENV{PG_TEST_EXTRA} !~ /\bchecksum_extended\b/)
{
	plan skip_all => 'Extended tests not enabled';
}

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Helper for retrieving a binary value with random distribution for deciding
# whether to turn things off during testing.
sub cointoss
{
	return int(rand() < 0.5);
}

# Helper for injecting random sleeps here and there in the testrun. The sleep
# duration wont be predictable in order to avoid sleep patterns that manage to
# avoid race conditions and timing bugs.
sub random_sleep
{
	return if cointoss;
	sleep(int(rand(3)));
}

# Start a pgbench run in the background against the server specified via the
# port passed as parameter
sub background_rw_pgbench
{
	my ($port, $stdin, $stdout, $stderr) = @_;

	if ($pgbench_primary)
	{
		$pgbench_primary->finish;
	}

	$pgbench_primary = IPC::Run::start(
		[ 'pgbench', '-p', $port, '-T', '600', '-c', '10', 'postgres' ],
		'<' => '/dev/null',
		'>' => '/dev/null',
		'2>' => '/dev/null',
		IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default));
}

# Invert the state of data checksums in the cluster, if data checksums are on
# then disable them and vice versa. Also performs proper validation of the
# before and after state.
sub flip_data_checksums
{
	test_checksum_state($node_primary, $data_checksum_state);

	if ($data_checksum_state eq 'off')
	{
		# Coin-toss to see if we are injecting a retry due to a temptable
		$node_primary->safe_psql('postgres',
			'SELECT dcw_fake_temptable(true);')
		  if cointoss();

		# log LSN right before we start changing checksums
		my $result = $node_primary->safe_psql('postgres', "SELECT pg_current_wal_lsn()");
		print (">> LSN before: " . $result . "\n");

		# Ensure that the primary switches to "inprogress-on"
		enable_data_checksums($node_primary, wait => 'inprogress-on');

		random_sleep();

		# Wait for checksums enabled on the primary
		wait_for_checksum_state($node_primary, 'on');

		# log LSN right after the primary flips checksums to "on"
		$result = $node_primary->safe_psql('postgres', "SELECT pg_current_wal_lsn()");
		print (">> LSN after: " . $result . "\n");

		random_sleep();

		$node_primary->safe_psql('postgres',
			'SELECT dcw_fake_temptable(false);');
		$data_checksum_state = 'on';
	}
	elsif ($data_checksum_state eq 'on')
	{
		random_sleep();

		# log LSN right before we start changing checksums
		my $result = $node_primary->safe_psql('postgres', "SELECT pg_current_wal_lsn()");
		print (">> LSN before: " . $result . "\n");

		disable_data_checksums($node_primary);

		# Wait for checksums disabled on the primary
		wait_for_checksum_state($node_primary, 'off');

		# log LSN right after the primary flips checksums to "off"
		$result = $node_primary->safe_psql('postgres', "SELECT pg_current_wal_lsn()");
		print (">> LSN after: " . $result . "\n");

		random_sleep();

		$data_checksum_state = 'off';
	}
	else
	{
		# This should only happen due to programmer error when hacking on the
		# test code, but since that might pass subtly by let's ensure it gets
		# caught with a test error if so.
		is(1, 0, 'data_checksum_state variable has invalid state');
	}
}

# Prepare an array with pg_ctl stop modes which we later can randomly select
# from in order to stop the cluster in some way.
for (my $i = 1; $i <= 100; $i++)
{
	if (int(rand($i * 2)) > $i)
	{
		push(@stop_modes, "immediate");
	}
	else
	{
		push(@stop_modes, "fast");
	}
}

# Create and start a cluster with one node
$node_primary = PostgreSQL::Test::Cluster->new('main');
$node_primary->init(allows_streaming => 1, no_data_checksums => 1);
# max_connections need to be bumped in order to accomodate for pgbench clients
# and log_statement is dialled down since it otherwise will generate enormous
# amounts of logging. Page verification failures are still logged.
$node_primary->append_conf(
	'postgresql.conf',
	qq[
max_connections = 30
log_statement = none
]);
$node_primary->start;
$node_primary->safe_psql('postgres', 'CREATE EXTENSION test_checksums;');
# Create some content to have un-checksummed data in the cluster
$node_primary->safe_psql('postgres',
	"CREATE TABLE t AS SELECT generate_series(1, 100000) AS a;");

$node_primary->command_ok([ 'pgbench', '-i', '-s', '100', '-q', 'postgres' ]);

# Start the test suite with pgbench running.
background_rw_pgbench(
	$node_primary->port, $pgb_primary_stdin,
	$pgb_primary_stdout, $pgb_primary_stderr);

my $primary_shutdown_clean = 0;

# Main test suite. This loop will start a pgbench run on the cluster and while
# that's running flip the state of data checksums concurrently. It will then
# randomly restart thec cluster (in fast or immediate) mode and then check for
# the desired state.  The idea behind doing things randomly is to stress out
# any timing related issues by subjecting the cluster for varied workloads.
# A TODO is to generate a trace such that any test failure can be traced to
# its order of operations for debugging.
for (my $i = 0; $i < $TEST_ITERATIONS; $i++)
{
	if (!$node_primary->is_alive)
	{
		# If data checksums are enabled, take the opportunity to verify them
		# while the cluster is offline (but only if stopped in a clean way,
		# not after immediate shutdown)
		#$node_primary->checksum_verify_offline()
		#  unless $data_checksum_state eq 'off' or !$primary_shutdown_clean;

		random_sleep();

		# start, to do recovery, and stop
		$node_primary->start;
		$node_primary->stop('fast');

		# Since the log isn't being written to now, parse the log and check
		# for instances of checksum verification failures.
		my $log = PostgreSQL::Test::Utils::slurp_file($node_primary->logfile,
			$node_primary_loglocation);
		unlike(
			$log,
			qr/page verification failed/,
			"no checksum validation errors in primary log (during WAL recovery)");
		$node_primary_loglocation = -s $node_primary->logfile;

		$node_primary->start;

		# Start a pgbench in the background against the primary
		background_rw_pgbench($node_primary->port, 0, $pgb_primary_stdin,
			$pgb_primary_stdout, $pgb_primary_stderr);
	}

	$node_primary->safe_psql('postgres', "UPDATE t SET a = a + 1;");

	flip_data_checksums();
	random_sleep();
	my $result = $node_primary->safe_psql('postgres',
		"SELECT count(*) FROM t WHERE a > 1");
	is($result, '100000', 'ensure data pages can be read back on primary');

	random_sleep();

	# Potentially powercycle the cluster (the nodes independently)
	# XXX should maybe try stopping nodes in the opposite order too?
	if (cointoss())
	{
		my $mode = $stop_modes[ int(rand(100)) ];
		$node_primary->stop($mode);
		$primary_shutdown_clean = ($mode eq 'fast');

		PostgreSQL::Test::Utils::system_log("pg_controldata", $node_primary->data_dir);

		my $log = PostgreSQL::Test::Utils::slurp_file($node_primary->logfile,
			$node_primary_loglocation);
		unlike(
			$log,
			qr/page verification failed/,
			"no checksum validation errors in primary log (outside WAL recovery)");
		$node_primary_loglocation = -s $node_primary->logfile;
	}

	random_sleep();
}

# make sure the nodes are running
if (!$node_primary->is_alive)
{
	$node_primary->start;
}

# Testrun is over, ensure that data reads back as expected and perform a final
# verification of the data checksum state.
my $result =
  $node_primary->safe_psql('postgres', "SELECT count(*) FROM t WHERE a > 1");
is($result, '100000', 'ensure data pages can be read back on primary');
test_checksum_state($node_primary, $data_checksum_state);

# Perform one final pass over the logs and hunt for unexpected errors
my $log = PostgreSQL::Test::Utils::slurp_file($node_primary->logfile,
	$node_primary_loglocation);
unlike(
	$log,
	qr/page verification failed/,
	"no checksum validation errors in primary log");
$node_primary_loglocation = -s $node_primary->logfile;

$node_primary->teardown_node;

done_testing();
