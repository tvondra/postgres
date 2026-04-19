
# Copyright (c) 2026, PostgreSQL Global Development Group

# Test suite for testing enabling data checksums in an online cluster with
# injection point tests injecting failures into the processing

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Algorithm::Combinatorics qw(combinations);

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

# make sure we don't hit checkpoints unless desired
$node->append_conf(
	'postgresql.conf',
	qq[
checkpoint_timeout = 1h
max_wal_size = 32GB
]);
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

# Start a checkpoint in the background against the server specified via the
# port passed as parameter.
sub background_checkpoint
{
	my $port = shift;

	my @cmd = ('psql', '-p', $port, '-c', 'checkpoint', 'postgres');

	$pgbench = IPC::Run::start(
		\@cmd,
		'<' => '/dev/null',
		'>' => '/dev/null',
		'2>' => '/dev/null',
		IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default));
}

# Test checksum transition concurrent with a checkpoint.
#
# The function has these arguments:
#
# - start checksum state (enabled/disabled)
# - change - checksum change to initiate
# - point1 - injection point before checkpoint
# - point2 - injection point after checkpoint
# - final - expected checksum state at the end
#
# The test puts the instance into the initial checksum state, triggers a
# checksum change that pauses on a selected injection point. Then performs
# a checkpoint, unpauses the change so that it proceeds to a second
# injection point.
#
# Then the instance is restarted in immediate mode to simulate failure,
# and the final checksum state is validated against the expected value.
# The server log is checked for checksum failures.
sub test_checksum_sequence
{
	my ($start, $change, $init, $stop, $final, @sequence) = @_;

	# Start the test suite with pgbench running.
	background_rw_pgbench($node->port);

	note(@sequence);

	$node->safe_psql('postgres',
		"SELECT '========== " . $start . " / " . $change . " / (" . @sequence . ") / " . $init . " / " . $stop . " / " . $final . " =========='");

	note($start . " / " . $change . " / (" . @sequence . ") / " . $init . " / " . $stop . " / " . $final);

	note('changing checksums into initial state: ' . $start);

	enable_data_checksums($node, wait => 'on') if ($start eq 'enabled');
	disable_data_checksums($node, wait => 'off') if ($start eq 'disabled');

	# attach the initial injection point
	note('attaching injection point: ' . $init);
	$node->safe_psql('postgres',
		"SELECT injection_points_attach('" . $init . "','wait');"
	);

	# when we're at it, attach the last injection point
	note('attaching injection point: ' . $stop);
	$node->safe_psql('postgres',
		"SELECT injection_points_attach('" . $stop . "','wait');"
	);

	# do a checkpoint, so that we don't hit the next one at some
	# arbitrary time
	$node->safe_psql('postgres', "CHECKPOINT");

	note("triggering checksum change: " . $change);

	enable_data_checksums($node) if ($change eq 'enable');
	disable_data_checksums($node) if ($change eq 'disable');

	# wait for the initial injection point
	note("waiting for the injection point $init to be hit");
	$node->poll_query_until(
		'postgres',
		"SELECT COUNT(*) FROM pg_catalog.pg_stat_activity WHERE wait_event = '" . $init . "'",
		'1');

	# now attach all the regular injection points
	my $n = @sequence;
	for ($a = 0; $a < $n; $a++)
	{
		my $point = $sequence[$a];
		note('attaching injection point: ' . $point);
		$node->safe_psql('postgres',
			"SELECT injection_points_attach('" . $point . "','wait');"
		);
	}

	# initiate a background checkpoint
	background_checkpoint($node->port);

	# wakeup the initial injection point, to start the main part
	note('waking the injection point: ' . $init);
	$node->safe_psql('postgres',
		"SELECT injection_points_wakeup('" . $init . "');"
	);

	note("detaching the injection point: " . $init);
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('" . $init . "');");

	# wait for regular injection points in sequence, wake and detach them
	for ($a = 0; $a < $n; $a++)
	{
		my $point = $sequence[$a];

		note("waiting for the injection point $point to be hit");
		$node->poll_query_until(
			'postgres',
			"SELECT COUNT(*) FROM pg_catalog.pg_stat_activity WHERE wait_event = '" . $point . "'",
			'1');

		# detach before wakeup, so that we can't hit it again right away
		note("detaching the injection point: " . $point);
		$node->safe_psql('postgres',
			"SELECT injection_points_detach('" . $point . "');");

		note('waking the injection point: ' . $point);
		$node->safe_psql('postgres',
			"SELECT injection_points_wakeup('" . $point . "');"
		);
	}

	# wait for the stop injection point
	note("waiting for the injection point $stop to be hit");
	$node->poll_query_until(
		'postgres',
		"SELECT COUNT(*) FROM pg_catalog.pg_stat_activity WHERE wait_event = '" . $stop . "'",
		'1');

	$node->stop('immediate');
	$node->start;

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

my @checksum_points = qw(datachecksums-enable-inprogress-checksums-start
						 datachecksums-enable-inprogress-checksums-after-xlog
						 datachecksums-enable-inprogress-checksums-after-xlogctl
						 datachecksums-enable-inprogress-checksums-after-controlfile
						 datachecksums-enable-inprogress-checksums-before-barrier-wait
						 datachecksums-enable-inprogress-checksums-end
						 datachecksums-enable-checksums-start
						 datachecksums-enable-checksums-after-xlog
						 datachecksums-enable-checksums-after-xlogctl
						 datachecksums-enable-checksums-after-controlfile
						 datachecksums-enable-checksums-before-checkpoint
						 datachecksums-enable-checksums-before-barrier-wait
						 datachecksums-enable-checksums-end);

my @checkpoint_points = qw(create-checkpoint-initial
						   create-checkpoint-run
						   checkpoint-before-xlogctl-checksums
						   checkpoint-before-redo-position
						   checkpoint-after-redo-position
						   checkpoint-before-redo-wal
						   checkpoint-after-redo-wal
						   checkpoint-before-old-wal-removal);

my @points = undef;

## checksums INPROGRESS ON

note('TEST INPROGRESS-ON/1');
@points = qw(datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @points);

note('TEST INPROGRESS-ON/2');
@points = qw(create-checkpoint-initial
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @points);

note('TEST INPROGRESS-ON/3');
@points = qw(create-checkpoint-initial
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @points);

note('TEST INPROGRESS-ON/4');
@points = qw(create-checkpoint-initial
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @points);

note('TEST INPROGRESS-ON/5');
@points = qw(create-checkpoint-initial
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile);
test_checksum_sequence('disabled', 'enable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @points);

note('TEST INPROGRESS-ON/6');
@points = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @points);

## checksums ON

note('TEST ON/1');
@points = qw(datachecksums-enable-checksums-before-xlog
			 datachecksums-enable-checksums-after-xlog
			 datachecksums-enable-checksums-after-xlogctl
			 datachecksums-enable-checksums-after-controlfile
			 create-checkpoint-initial
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-end', 'on', @points);

note('TEST ON/2');
@points = qw(create-checkpoint-initial
			 datachecksums-enable-checksums-before-xlog
			 datachecksums-enable-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-enable-checksums-after-xlogctl
			 datachecksums-enable-checksums-after-controlfile);
test_checksum_sequence('disabled', 'enable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-end', 'on', @points);

note('TEST ON/3');
@points = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 datachecksums-enable-checksums-before-xlog
			 datachecksums-enable-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-enable-checksums-after-xlogctl
			 datachecksums-enable-checksums-after-controlfile);
test_checksum_sequence('disabled', 'enable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-end', 'on', @points);

note('TEST ON/4');
@points = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 datachecksums-enable-checksums-before-xlog
			 datachecksums-enable-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-enable-checksums-after-xlogctl
			 datachecksums-enable-checksums-after-controlfile);
test_checksum_sequence('disabled', 'enable', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-end', 'on', @points);


## checksums INPROGRESS OFF

note('TEST INPROGRESS-OFF/1');
@points = qw(datachecksums-disable-inprogress-checksums-before-xlog
			 datachecksums-disable-inprogress-checksums-after-xlog
			 datachecksums-disable-inprogress-checksums-after-xlogctl
			 datachecksums-disable-inprogress-checksums-after-controlfile
			 datachecksums-disable-inprogress-checksums-before-checkpoint
			 create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-inprogress-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-checksums-start', 'off', @points);

note('TEST INPROGRESS-OFF/2');
@points = qw(create-checkpoint-initial
			 datachecksums-disable-inprogress-checksums-before-xlog
			 datachecksums-disable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-inprogress-checksums-after-xlogctl
			 datachecksums-disable-inprogress-checksums-after-controlfile
			 datachecksums-disable-inprogress-checksums-before-checkpoint
			 datachecksums-disable-inprogress-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-checksums-start', 'off', @points);

note('TEST INPROGRESS-OFF/3');
@points = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 datachecksums-disable-inprogress-checksums-before-xlog
			 datachecksums-disable-inprogress-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-inprogress-checksums-after-xlogctl
			 datachecksums-disable-inprogress-checksums-after-controlfile
			 datachecksums-disable-inprogress-checksums-before-checkpoint
			 datachecksums-disable-inprogress-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-checksums-start', 'off', @points);

note('TEST INPROGRESS-OFF/4');
@points = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 datachecksums-disable-inprogress-checksums-before-xlog
			 datachecksums-disable-inprogress-checksums-after-xlog
			 datachecksums-disable-inprogress-checksums-after-xlogctl
			 datachecksums-disable-inprogress-checksums-after-controlfile
			 datachecksums-disable-inprogress-checksums-before-checkpoint
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-inprogress-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-checksums-start', 'off', @points);

## checksums OFF

note('TEST OFF/1');
@points = qw(datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @points);

note('TEST OFF/2');
@points = qw(create-checkpoint-initial
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @points);

note('TEST OFF/3');
@points = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @points);

note('TEST OFF/4');
@points = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @points);

note('TEST OFF/5');
@points = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @points);

note('TEST OFF/6');
@points = qw(create-checkpoint-initial
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 datachecksums-disable-checksums-after-xlogctl
			 checkpoint-before-redo-position
			 checkpoint-after-redo-position
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @points);

$node->stop;
done_testing();
