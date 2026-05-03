
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

	# detach before wakeup, so that we can't hit it right away
	note("detaching the init injection point");
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('" . $point . "');");

	note('waking the init injection point: ' . $point);
	$node->safe_psql('postgres',
		"SELECT injection_points_wakeup('" . $point . "');"
	);
}

# Test behavior with a checksum transitions and a concurrent checkpoint,
# followed by a crash.
#
# The test puts the instance into the initial checksum state $start,
# initiates a checksum change and waits for it to pause on the $init
# injection point. Then a checkpoint is started (in the background), and
# the two processes go through a list of injection points to interleave
# them in a particular way.
#
# After reaching a $stop injection point, the cluster get restarted in an
# immediate mode to simulate failure. The checksum state (after recovery)
# is validated against the expected value. The server log is checked for
# checksum failures.
#
# While the checksum change is happening, there's a r/w pgbench running in
# the background, to generate writes.
#
# arguments:
#
# - start  - start checksum state (enabled/disabled)
# - change - checksum change to initiate
# - init   - initial injection point to wait on
# - stop   - injection point to wait on before restarting
# - final  - expected checksum state at the end
# - steps  - a list of injection points to wait on / unpause
#
# XXX Some of the injection points are in a critical section, which does
# not allow memory allocations etc. INJECTION_POINT_LOAD/_CACHED handles
# just private memory allocation, but 'wait' requires a shmem allocation.
# To deal with that, we first need to pause on an injection point outside
# a critical section to initialize the shmem stuff needed by 'wait'.
#
# XXX We could also validate the checksums using pg_checksums, if the
# state is 'on'.
#
# XXX The steps are not entirely deterministic, because when unpausing
# an injection point, we don't know if the other process is already
# paused on the following point.
#
sub test_checksum_sequence
{
	my ($start, $change, $fast, $init, $stop, $final, @steps) = @_;

	# print the current test instructions, both into TAP output and into
	# the server log, to make correlation easier

	$node->safe_psql('postgres',
		"SELECT '========== " . $start . " / " . $change . " / (" . @steps . ") / " . $init . " / " . $stop . " / " . $final . " =========='");

	note($start . " / " . $change . " / (" . @steps . ") / " . $init . " / " . $stop . " / " . $final);

	# Start the test suite with pgbench running.
	background_rw_pgbench($node->port);

	# put the cluster into the initial checksum state, synchronously
	note('changing checksums into initial state: ' . $start);
	enable_data_checksums($node, wait => 'on', fast => 'true') if ($start eq 'enabled');
	disable_data_checksums($node, wait => 'off', fast => 'true') if ($start eq 'disabled');

	# attach the initial injection point
	attach_injection_point($node, $init);

	# when we're at it, attach the stop injection point
	attach_injection_point($node, $stop);

	# Do a checkpoint first, so that we don't accidentaly start the next
	# checkpoint at an unecpected time. We want the next checkpoint to be
	# the background one.
	$node->safe_psql('postgres', "CHECKPOINT");

	# Trigger the checksum change, asynchronously
	note("triggering checksum change: " . $change);
	enable_data_checksums($node, fast => $fast) if ($change eq 'enable');
	disable_data_checksums($node, fast => $fast) if ($change eq 'disable');

	# wait for the initial injection point
	wait_injection_point($node, $init);

	# now attach all the regular injection points
	my $n = @steps;
	for ($a = 0; $a < $n; $a++)
	{
		my $point = $steps[$a];
		attach_injection_point($node, $point);
	}

	# initiate a background checkpoint
	background_checkpoint($node->port);

	# wakeup the initial injection point, to start the main part
	wakeup_injection_point($node, $init);

	# wait for regular injection points in sequence, wake and detach them
	for ($a = 0; $a < $n; $a++)
	{
		my $point = $steps[$a];

		wait_injection_point($node, $point);
		wakeup_injection_point($node, $point);
	}

	# wait for the stop injection point
	wait_injection_point($node, $stop);

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

my @steps = undef;

## checksums INPROGRESS ON

note('TEST INPROGRESS-ON/1/fast');
@steps = qw(datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'true', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @steps);

note('TEST INPROGRESS-ON/1/spread');
@steps = qw(datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'false', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @steps);

note('TEST INPROGRESS-ON/2/fast');
@steps = qw(create-checkpoint-initial
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'true', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @steps);

note('TEST INPROGRESS-ON/2/spread');
@steps = qw(create-checkpoint-initial
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'false', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @steps);

note('TEST INPROGRESS-ON/3/false');
@steps = qw(create-checkpoint-initial
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'true', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @steps);

note('TEST INPROGRESS-ON/3/spread');
@steps = qw(create-checkpoint-initial
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'false', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @steps);

note('TEST INPROGRESS-ON/4/fast');
@steps = qw(create-checkpoint-initial
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'true', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @steps);

note('TEST INPROGRESS-ON/4/spread');
@steps = qw(create-checkpoint-initial
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'false', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @steps);

note('TEST INPROGRESS-ON/5/fast');
@steps = qw(create-checkpoint-initial
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile);
test_checksum_sequence('disabled', 'enable', 'true', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @steps);

note('TEST INPROGRESS-ON/5/spread');
@steps = qw(create-checkpoint-initial
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile);
test_checksum_sequence('disabled', 'enable', 'false', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @steps);

note('TEST INPROGRESS-ON/6/fast');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'true', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @steps);

note('TEST INPROGRESS-ON/6/spread');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 datachecksums-enable-inprogress-checksums-before-xlog
			 datachecksums-enable-inprogress-checksums-after-xlog
			 datachecksums-enable-inprogress-checksums-after-xlogctl
			 datachecksums-enable-inprogress-checksums-after-controlfile
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'false', 'datachecksums-enable-inprogress-checksums-start', 'datachecksums-enable-inprogress-checksums-end', 'off', @steps);

## checksums ON

note('TEST ON/1/fast');
@steps = qw(datachecksums-enable-checksums-before-xlog
			 datachecksums-enable-checksums-after-xlog
			 datachecksums-enable-checksums-after-xlogctl
			 datachecksums-enable-checksums-after-controlfile
			 create-checkpoint-initial
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'true', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-end', 'on', @steps);

note('TEST ON/1/spread');
@steps = qw(datachecksums-enable-checksums-before-xlog
			 datachecksums-enable-checksums-after-xlog
			 datachecksums-enable-checksums-after-xlogctl
			 datachecksums-enable-checksums-after-controlfile
			 create-checkpoint-initial
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal);
test_checksum_sequence('disabled', 'enable', 'false', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-end', 'on', @steps);

note('TEST ON/2/fast');
@steps = qw(create-checkpoint-initial
			 datachecksums-enable-checksums-before-xlog
			 datachecksums-enable-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-enable-checksums-after-xlogctl
			 datachecksums-enable-checksums-after-controlfile);
test_checksum_sequence('disabled', 'enable', 'true', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-end', 'on', @steps);

note('TEST ON/2/spread');
@steps = qw(create-checkpoint-initial
			 datachecksums-enable-checksums-before-xlog
			 datachecksums-enable-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-enable-checksums-after-xlogctl
			 datachecksums-enable-checksums-after-controlfile);
test_checksum_sequence('disabled', 'enable', 'false', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-end', 'on', @steps);

note('TEST ON/3/fast');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 datachecksums-enable-checksums-before-xlog
			 datachecksums-enable-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-enable-checksums-after-xlogctl
			 datachecksums-enable-checksums-after-controlfile);
test_checksum_sequence('disabled', 'enable', 'true', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-end', 'on', @steps);

note('TEST ON/3/spread');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 datachecksums-enable-checksums-before-xlog
			 datachecksums-enable-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-enable-checksums-after-xlogctl
			 datachecksums-enable-checksums-after-controlfile);
test_checksum_sequence('disabled', 'enable', 'false', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-end', 'on', @steps);

note('TEST ON/4/fast');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 datachecksums-enable-checksums-before-xlog
			 datachecksums-enable-checksums-after-xlog
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-enable-checksums-after-xlogctl
			 datachecksums-enable-checksums-after-controlfile);
test_checksum_sequence('disabled', 'enable', 'true', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-end', 'on', @steps);

note('TEST ON/4/spread');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 datachecksums-enable-checksums-before-xlog
			 datachecksums-enable-checksums-after-xlog
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-enable-checksums-after-xlogctl
			 datachecksums-enable-checksums-after-controlfile);
test_checksum_sequence('disabled', 'enable', 'false', 'datachecksums-enable-checksums-start', 'datachecksums-enable-checksums-end', 'on', @steps);


## checksums INPROGRESS OFF

note('TEST INPROGRESS-OFF/1/fast');
@steps = qw(datachecksums-disable-inprogress-checksums-before-xlog
			 datachecksums-disable-inprogress-checksums-after-xlog
			 datachecksums-disable-inprogress-checksums-after-xlogctl
			 datachecksums-disable-inprogress-checksums-after-controlfile
			 datachecksums-disable-inprogress-checksums-before-checkpoint
			 create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-inprogress-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'true', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-checksums-start', 'off', @steps);

note('TEST INPROGRESS-OFF/1/spread');
@steps = qw(datachecksums-disable-inprogress-checksums-before-xlog
			 datachecksums-disable-inprogress-checksums-after-xlog
			 datachecksums-disable-inprogress-checksums-after-xlogctl
			 datachecksums-disable-inprogress-checksums-after-controlfile
			 datachecksums-disable-inprogress-checksums-before-checkpoint
			 create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-inprogress-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'false', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-checksums-start', 'off', @steps);

note('TEST INPROGRESS-OFF/2/fast');
@steps = qw(create-checkpoint-initial
			 datachecksums-disable-inprogress-checksums-before-xlog
			 datachecksums-disable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-inprogress-checksums-after-xlogctl
			 datachecksums-disable-inprogress-checksums-after-controlfile
			 datachecksums-disable-inprogress-checksums-before-checkpoint
			 datachecksums-disable-inprogress-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'true', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-checksums-start', 'off', @steps);

note('TEST INPROGRESS-OFF/2/spread');
@steps = qw(create-checkpoint-initial
			 datachecksums-disable-inprogress-checksums-before-xlog
			 datachecksums-disable-inprogress-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-inprogress-checksums-after-xlogctl
			 datachecksums-disable-inprogress-checksums-after-controlfile
			 datachecksums-disable-inprogress-checksums-before-checkpoint
			 datachecksums-disable-inprogress-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'false', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-checksums-start', 'off', @steps);

note('TEST INPROGRESS-OFF/3/fast');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 datachecksums-disable-inprogress-checksums-before-xlog
			 datachecksums-disable-inprogress-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-inprogress-checksums-after-xlogctl
			 datachecksums-disable-inprogress-checksums-after-controlfile
			 datachecksums-disable-inprogress-checksums-before-checkpoint
			 datachecksums-disable-inprogress-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'true', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-checksums-start', 'off', @steps);

note('TEST INPROGRESS-OFF/3/spread');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 datachecksums-disable-inprogress-checksums-before-xlog
			 datachecksums-disable-inprogress-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-inprogress-checksums-after-xlogctl
			 datachecksums-disable-inprogress-checksums-after-controlfile
			 datachecksums-disable-inprogress-checksums-before-checkpoint
			 datachecksums-disable-inprogress-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'false', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-checksums-start', 'off', @steps);

note('TEST INPROGRESS-OFF/4/fast');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 datachecksums-disable-inprogress-checksums-before-xlog
			 datachecksums-disable-inprogress-checksums-after-xlog
			 datachecksums-disable-inprogress-checksums-after-xlogctl
			 datachecksums-disable-inprogress-checksums-after-controlfile
			 datachecksums-disable-inprogress-checksums-before-checkpoint
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-inprogress-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'true', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-checksums-start', 'off', @steps);

note('TEST INPROGRESS-OFF/4/spread');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 datachecksums-disable-inprogress-checksums-before-xlog
			 datachecksums-disable-inprogress-checksums-after-xlog
			 datachecksums-disable-inprogress-checksums-after-xlogctl
			 datachecksums-disable-inprogress-checksums-after-controlfile
			 datachecksums-disable-inprogress-checksums-before-checkpoint
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-inprogress-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'false', 'datachecksums-disable-inprogress-checksums-start', 'datachecksums-disable-checksums-start', 'off', @steps);

## checksums OFF

note('TEST OFF/1/fast');
@steps = qw(datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'true', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @steps);

note('TEST OFF/1/spread');
@steps = qw(datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'false', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @steps);

note('TEST OFF/2/fast');
@steps = qw(create-checkpoint-initial
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'true', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @steps);

note('TEST OFF/2/spread');
@steps = qw(create-checkpoint-initial
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'false', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @steps);

note('TEST OFF/3/fast');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'true', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @steps);

note('TEST OFF/3/spread');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'false', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @steps);

note('TEST OFF/4/fast');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'true', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @steps);

note('TEST OFF/4/spread');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'false', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @steps);

note('TEST OFF/5/fast');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'true', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @steps);

note('TEST OFF/5/spread');
@steps = qw(create-checkpoint-initial
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 checkpoint-before-redo-checksums
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-disable-checksums-after-xlogctl
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'false', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @steps);

note('TEST OFF/6/fast');
@steps = qw(create-checkpoint-initial
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 datachecksums-disable-checksums-after-xlogctl
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'true', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @steps);

note('TEST OFF/6/spread');
@steps = qw(create-checkpoint-initial
			 datachecksums-disable-checksums-before-xlog
			 datachecksums-disable-checksums-after-xlog
			 checkpoint-before-redo
			 checkpoint-before-xlogctl-checksums
			 checkpoint-after-xlogctl-checksums
			 datachecksums-disable-checksums-after-xlogctl
			 checkpoint-before-redo-checksums
			 checkpoint-before-redo-wal
			 checkpoint-after-redo-wal
			 datachecksums-disable-checksums-after-controlfile
			 datachecksums-disable-checksums-before-checkpoint
			 checkpoint-before-old-wal-removal
			 datachecksums-disable-checksums-before-barrier-wait);
test_checksum_sequence('enabled', 'disable', 'false', 'datachecksums-disable-checksums-start', 'datachecksums-disable-checksums-end', 'off', @steps);

$node->stop;
done_testing();
