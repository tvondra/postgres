
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

# Test checksum transition. The function has these arguments:
#
# - start checksum state (enabled/disabled)
# - first - first checksum change
# - second - second checksum change
# - point - injection point the first change should wait on
# - final - expected checksum state at the end
#
# The test puts the instance into the initial checksum state, triggers two
# checksum changes, and verifies the final state is as expected. The first
# state change is paused on a selected injection point, and unpaused after
# the second change gets initiated.
#
# The injection point is triggered only by the datachecksum launcher, and
# there can be only one such process. So there's no risk of hitting the
# injection point by both changes.
sub test_checksum_transition
{
	my ($start, $first, $second, $init, $point, $final) = @_;

	my $initstr = 'undef';
	$initstr = $init if defined($init);

	$node->safe_psql('postgres',
		"SELECT '========== " . $start . " / " . $first . " / " . $second . " / " . $initstr . " / " . $point . " / " . $final . " =========='");

	note($start . " / " . $first . " / " . $second . " / " . $initstr . " / " . $point . " / " . $final);

	note('changing checksums into initial state: ' . $start);

	enable_data_checksums($node, wait => 'on') if ($start eq 'enabled');
	disable_data_checksums($node, wait => 'off') if ($start eq 'disabled');

	 # If the first injection point is not 'start', then do a wait first,
	 # to initialize the shmem (which can't be done in critical section).
	 # Wait on an init point first, before the critical section, to force
	 # initializing the shmem outside.
	if (defined($init))
	{
		note('attaching injection point: ' . $init);
		$node->safe_psql('postgres',
			"SELECT injection_points_attach('" . $init . "','wait');"
		);
	}

	note('attaching injection point: ' . $point);
	$node->safe_psql('postgres',
		"SELECT injection_points_attach('" . $point . "','wait');"
	);

	note("triggering first checksum change: " . $first);

	enable_data_checksums($node) if ($first eq 'enable');
	disable_data_checksums($node) if ($first eq 'disable');

	# wakeup and detach the init point, to cleanup
	if (defined($init))
	{
		note("waiting for the init injection point to be hit");
		$node->poll_query_until(
			'postgres',
			"SELECT COUNT(*) FROM pg_catalog.pg_stat_activity WHERE wait_event = '" . $init . "'",
			'1');

		note('waking the init injection point: ' . $init);
		$node->safe_psql('postgres',
			"SELECT injection_points_wakeup('" . $init . "');"
		);

		note("detaching the init injection point");
		$node->safe_psql('postgres',
			"SELECT injection_points_detach('" . $init . "');");
	}

	note("waiting for the injection point to be hit");
	$node->poll_query_until(
		'postgres',
		"SELECT COUNT(*) FROM pg_catalog.pg_stat_activity WHERE wait_event = '" . $point . "'",
		'1');

	note("triggering second checksum change: " . $second);

	enable_data_checksums($node) if ($second eq 'enable');
	disable_data_checksums($node) if ($second eq 'disable');

	note("waking and detaching injection point");
	$node->safe_psql('postgres',
		"SELECT injection_points_wakeup('" . $point . "');");

	note("detaching injection point");
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('" . $point . "');");

	note('wait for the checksum launcher to exit');
	$node->poll_query_until('postgres',
			"SELECT count(*) = 0 "
		  . "FROM pg_catalog.pg_stat_activity "
		  . "WHERE backend_type = 'datachecksum launcher';");

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

# Start the test suite with pgbench running.
background_rw_pgbench($node->port);

test_checksum_transition('disabled', 'enable', 'disable', undef, 'datachecksums-enable-inprogress-checksums-start', 'off');
test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-after-xlog', 'off');
test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-after-xlogctl', 'off');
test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-after-controlfile', 'off');
test_checksum_transition('disabled', 'enable', 'disable', undef, 'datachecksums-enable-inprogress-checksums-before-barrier-wait', 'off');
test_checksum_transition('disabled', 'enable', 'disable', undef, 'datachecksums-enable-inprogress-checksums-end', 'off');
test_checksum_transition('disabled', 'enable', 'disable', undef, 'datachecksums-enable-checksums-start', 'off');
test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-after-xlog', 'off');
test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-after-xlogctl', 'off');
test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-after-controlfile', 'off');
test_checksum_transition('disabled', 'enable', 'disable', undef, 'datachecksums-enable-checksums-before-checkpoint', 'off');
test_checksum_transition('disabled', 'enable', 'disable', undef, 'datachecksums-enable-checksums-before-barrier-wait', 'off');
test_checksum_transition('disabled', 'enable', 'disable', undef, 'datachecksums-enable-checksums-end', 'off');

test_checksum_transition('disabled', 'enable', 'enable', undef, 'datachecksums-enable-inprogress-checksums-start', 'on');
test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-after-xlog', 'on');
test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-after-xlogctl', 'on');
test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-after-controlfile', 'on');
test_checksum_transition('disabled', 'enable', 'enable', undef, 'datachecksums-enable-inprogress-checksums-before-barrier-wait', 'on');
test_checksum_transition('disabled', 'enable', 'enable', undef, 'datachecksums-enable-inprogress-checksums-end', 'on');
test_checksum_transition('disabled', 'enable', 'enable', undef, 'datachecksums-enable-checksums-start', 'on');
test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-after-xlog', 'on');
test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-after-xlogctl', 'on');
test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-after-controlfile', 'on');
test_checksum_transition('disabled', 'enable', 'enable', undef, 'datachecksums-enable-checksums-before-checkpoint', 'on');
test_checksum_transition('disabled', 'enable', 'enable', undef, 'datachecksums-enable-checksums-before-barrier-wait', 'on');
test_checksum_transition('disabled', 'enable', 'enable', undef, 'datachecksums-enable-checksums-end', 'on');

test_checksum_transition('enabled', 'disable', 'disable', undef, 'datachecksums-disable-inprogress-checksums-start', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-inprogress-checksums-after-xlog', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-inprogress-checksums-after-xlogctl', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-inprogress-checksums-after-controlfile', 'off');
test_checksum_transition('enabled', 'disable', 'disable', undef, 'datachecksums-disable-inprogress-checksums-before-barrier-wait', 'off');
test_checksum_transition('enabled', 'disable', 'disable', undef, 'datachecksums-disable-checksums-start', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-after-xlog', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-after-xlogctl', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-after-controlfile', 'off');
test_checksum_transition('enabled', 'disable', 'disable', undef, 'datachecksums-disable-checksums-before-checkpoint', 'off');
test_checksum_transition('enabled', 'disable', 'disable', undef, 'datachecksums-disable-checksums-before-barrier-wait', 'off');
test_checksum_transition('enabled', 'disable', 'disable', undef, 'datachecksums-disable-checksums-end', 'off');

test_checksum_transition('enabled', 'disable', 'enable', undef, 'datachecksums-disable-inprogress-checksums-start', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-inprogress-checksums-after-xlog', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-inprogress-checksums-after-xlogctl', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-inprogress-checksums-after-controlfile', 'on');
test_checksum_transition('enabled', 'disable', 'enable', undef, 'datachecksums-disable-inprogress-checksums-before-barrier-wait', 'on');
test_checksum_transition('enabled', 'disable', 'enable', undef, 'datachecksums-disable-checksums-start', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-after-xlog', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-after-xlogctl', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-after-controlfile', 'on');
test_checksum_transition('enabled', 'disable', 'enable', undef, 'datachecksums-disable-checksums-before-checkpoint', 'on');
test_checksum_transition('enabled', 'disable', 'enable', undef, 'datachecksums-disable-checksums-before-barrier-wait', 'on');
test_checksum_transition('enabled', 'disable', 'enable', undef, 'datachecksums-disable-checksums-end', 'on');

$node->stop;
done_testing();
