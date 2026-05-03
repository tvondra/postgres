
# Copyright (c) 2026, PostgreSQL Global Development Group

# Test suite for testing enabling data checksums in an online cluster with
# injection point tests injecting failures into the processing

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;

use DataChecksums::Utils;

# This test suite is expensive, or very expensive, to execute.  There are two
# PG_TEST_EXTRA options for running it, "checksum" for a pared-down test suite
# an "checksum_extended" for the full suite.  The full suite can run for hours
# on slow or constrained systems.
my $extended = undef;
if ($ENV{PG_TEST_EXTRA})
{
	$extended = 1 if ($ENV{PG_TEST_EXTRA} =~ /\bchecksum_extended\b/);
	plan skip_all => 'Expensive data checksums test disabled'
	  unless ($ENV{PG_TEST_EXTRA} =~ /\bchecksum(_extended)?\b/);
}

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# ---------------------------------------------------------------------------
# Test cluster setup
#

# Initiate testcluster
my $node = PostgreSQL::Test::Cluster->new('injection_node');
$node->init(no_data_checksums => 1);
$node->append_conf('postgresql.conf', 'checkpoint_timeout = 60s');
$node->start;

# Set up test environment
$node->safe_psql('postgres', 'CREATE EXTENSION test_checksums;');
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

my $pgbench = undef;
my $scalefactor = ($extended ? 10 : 1);
my $node_loglocation = 0;

$node->command_ok(
	[
		'pgbench', '-p', $node->port, '-i',
		'-s', $scalefactor, '-q', 'postgres'
	]);

# Start a pgbench run in the background against the server specified via the
# port passed as parameter.
sub background_rw_pgbench
{
	my $port = shift;

	# If a previous pgbench is still running, start by shutting it down.
	$pgbench->finish if $pgbench;

	my $clients = 1;
	my $runtime = 2;

	if ($extended)
	{
		# Randomize the number of pgbench clients a bit (range 1-16)
		$clients = 1 + int(rand(15));
		$runtime = 600;
	}
	my @cmd = ('pgbench', '-p', $port, '-T', $runtime, '-c', $clients);

	# Randomize whether we spawn connections or not
	push(@cmd, '-C') if ($extended && cointoss);
	# Finally add the database name to use
	push(@cmd, 'postgres');

	$pgbench = IPC::Run::start(
		\@cmd,
		'<' => '/dev/null',
		'>' => '/dev/null',
		'2>' => '/dev/null',
		IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default));
}

sub attach_injection_point
{
	my ($node, $point) = @_;
	note('attaching injection point: ' . $point);
	$node->safe_psql('postgres',
		"SELECT injection_points_attach('" . $point . "','wait');"
	);
}

sub wait_injection_point
{
	my ($node, $point) = @_;

	note("waiting for the injection point to be hit");
	$node->poll_query_until(
		'postgres',
		"SELECT COUNT(*) FROM pg_catalog.pg_stat_activity WHERE wait_event = '" . $point . "'",
		'1');
}

sub wakeup_injection_point
{
	my ($node, $point) = @_;

	note('waking the init injection point: ' . $point);
	$node->safe_psql('postgres',
		"SELECT injection_points_wakeup('" . $point . "');"
	);

	note("detaching the init injection point");
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('" . $point . "');");
}

# Test behavior with a checksum transitions and a concurrent checkpoint,
# followed by a crash.
#
# The test puts the instance into the initial checksum state $start,
# triggers a checksum change that pauses on a first injection point. Then
# a checkpoint is performed, and the checksum change proceeds either to
# a second injection point or finishes.
#
# Then the instance get restarted in immediate mode to simulate failure,
# and the final checksum state (after recovery) is validated against the
# expected value. The server log is checked for checksum failures.
#
# While the checksum change is happening, there's a r/w pgbench running in
# the background, to generate writes.
#
# arguments:
#
# - start  - start checksum state (enabled/disabled)
# - change - checksum change to initiate
# - point1 - injection point before checkpoint
# - point2 - injection point after checkpoint
# - final  - expected checksum state at the end
#
# XXX Some of the injection points are in a critical section, which does
# not allow memory allocations etc. INJECTION_POINT_LOAD/_CACHED handles
# just private memory allocation, but 'wait' requires a shmem allocation.
# To deal with that, we setup an injection point $init outside a critical
# section to initialize the shmem stuff needed by 'wait'.
#
# XXX We could also validate the checksums using pg_checksums, if the
# state is 'on'.
sub test_checksum_transition
{
	my ($start, $change, $init, $point1, $point2, $final) = @_;

	my $initstr = 'undef';
	my $point2str = 'undef';

	# print the current test instructions, both into TAP output and into
	# the server log, to make correlation easier

	$initstr = $init if defined($init);
	$point2str = $point2 if defined($point2);

	$node->safe_psql('postgres',
		"SELECT '========== " . $start . " / " . $change . " / " . $initstr . " / " . $point1 . " / " . $point2str . " / " . $final . " =========='");

	note($start . " / " . $change . " / " . $initstr . " / " . $point1 . " / " . $point2str . " / " . $final);

	# Start the test suite with pgbench running.
	background_rw_pgbench($node->port);

	# put the cluster into the initial checksum state, synchronously
	note('changing checksums into initial state: ' . $start);
	enable_data_checksums($node, wait => 'on', fast => 'true') if ($start eq 'enabled');
	disable_data_checksums($node, wait => 'off', fast => 'true') if ($start eq 'disabled');

	 # Wait on an injection point outside a critical section, to initialize
	 # the shmem (which can't be done in critical section).
	attach_injection_point($node, $init) if defined($init);

	# Wait on the two injection points, to pause the checksum change.
	attach_injection_point($node, $point1);

	# The second injection point is optional, so only attach it then.
	attach_injection_point($node, $point2) if defined($point2);

	# Trigger the checksum change, asynchronously
	note("triggering checksum change: " . $change);
	enable_data_checksums($node, fast => 'false') if ($change eq 'enable');
	disable_data_checksums($node, fast => 'false') if ($change eq 'disable');

	# Handle the initial injection point - wait, wakeup and detatch. This
	# initializes the shmem for the 'wait' action.
	wait_injection_point($node, $init) if defined($init);
	wakeup_injection_point($node, $init) if defined($init);

	# Wait for the first injection point to be hit by the state change.
	wait_injection_point($node, $point1);

	# The checksum state change is paused on the first injection point.
	# Perform the checkpoint (synchronously).
	note('checkpoint');
	$node->safe_psql('postgres', "CHECKPOINT");

	# Wake the injection point, so that the first change can proceed.
	wakeup_injection_point($node, $point1);

	# Either wait for the second injection point - if defined, or for the
	# checksum change to complete.

	if (defined($point2))
	{
		wait_injection_point($node, $point2);
	}
	else
	{
		# Wait until there are no ongoing checksum changes, which we determine
		# by looking for a checksum launcher process.
		note('wait for the checksum launcher to exit');
		$node->poll_query_until('postgres',
				"SELECT count(*) = 0 "
			  . "FROM pg_catalog.pg_stat_activity "
			  . "WHERE backend_type = 'datachecksum launcher';");
		note('checksum launcher exited');
	}

	# restart the cluster, in immediate mode, to simulate a crash
	$node->stop('immediate');
	$node->start;

	# Does the final checksum state match the expected state?
	test_checksum_state($node, $final);

	# Since the log isn't being written to now, parse the log and check
	# for instances of checksum verification failures.
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile,
		$node_loglocation);
	unlike(
		$log,
		qr/page verification failed,.+\d$/,
		"no checksum validation errors in primary log (during WAL recovery)"
	);
	$node_loglocation = -s $node->logfile;
}

# concurrent enable + checkpoint, different injection points in the "enable" process
test_checksum_transition('disabled', 'enable', undef, 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-after-xlog', 'off');
test_checksum_transition('disabled', 'enable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-after-xlog', 'datachecksums-enable-inprogress-checksums-after-xlogctl', 'off');
test_checksum_transition('disabled', 'enable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-after-xlogctl', 'datachecksums-enable-inprogress-checksums-after-controlfile', 'off');
test_checksum_transition('disabled', 'enable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-after-controlfile', 'datachecksums-enable-inprogress-checksums-before-barrier-wait', 'off');
test_checksum_transition('disabled', 'enable', undef, 'datachecksums-enable-inprogress-checksums-before-barrier-wait', 'datachecksums-enable-inprogress-checksums-end', 'off');
test_checksum_transition('disabled', 'enable', undef, 'datachecksums-enable-inprogress-checksums-end', 'datachecksums-enable-checksums-start', 'off');
test_checksum_transition('disabled', 'enable', undef, 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-after-xlog', 'on');
test_checksum_transition('disabled', 'enable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-after-xlog', 'datachecksums-enable-checksums-after-xlogctl', 'off');
test_checksum_transition('disabled', 'enable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-after-xlogctl', 'datachecksums-enable-checksums-after-controlfile', 'on');
test_checksum_transition('disabled', 'enable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-after-controlfile', 'datachecksums-enable-checksums-before-checkpoint', 'on');
test_checksum_transition('disabled', 'enable', undef, 'datachecksums-enable-checksums-before-checkpoint', 'datachecksums-enable-checksums-before-barrier-wait', 'on');
test_checksum_transition('disabled', 'enable', undef, 'datachecksums-enable-checksums-before-barrier-wait', 'datachecksums-enable-checksums-end', 'on');
test_checksum_transition('disabled', 'enable', undef, 'datachecksums-enable-checksums-end', undef, 'on');

# concurrent disable + checkpoint, different injection points in the "disable" process
test_checksum_transition('enabled', 'disable', undef, 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-inprogress-checksums-after-xlog', 'off');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-inprogress-checksums-after-xlog', 'datachecksums-disable-inprogress-checksums-after-xlogctl', 'on');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-inprogress-checksums-after-xlogctl', 'datachecksums-disable-inprogress-checksums-after-controlfile', 'off');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-inprogress-checksums-after-controlfile', 'datachecksums-disable-inprogress-checksums-before-barrier-wait', 'off');
test_checksum_transition('enabled', 'disable', undef, 'datachecksums-disable-inprogress-checksums-before-barrier-wait', 'datachecksums-disable-checksums-start', 'off');
test_checksum_transition('enabled', 'disable', undef, 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-after-xlog', 'off');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-after-xlog', 'datachecksums-disable-checksums-after-xlogctl', 'off');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-after-xlogctl', 'datachecksums-disable-checksums-after-controlfile', 'off');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-after-controlfile', 'datachecksums-disable-checksums-before-checkpoint', 'off');
test_checksum_transition('enabled', 'disable', undef, 'datachecksums-disable-checksums-before-checkpoint', 'datachecksums-disable-checksums-before-barrier-wait', 'off');
test_checksum_transition('enabled', 'disable', undef, 'datachecksums-disable-checksums-before-barrier-wait', 'datachecksums-disable-checksums-end', 'off');
test_checksum_transition('enabled', 'disable', undef, 'datachecksums-disable-checksums-end', undef, 'off');

$node->stop;
done_testing();
