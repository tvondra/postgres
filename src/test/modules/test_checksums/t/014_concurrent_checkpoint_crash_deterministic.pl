
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

# Run a SQL in the background against the server specified via the
# port passed as parameter.
sub background_sql
{
	my ($port, $sql) = @_;

	my @cmd = ('psql', '-p', $port, '-c', $sql, 'postgres');

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
# triggers a checksum change. A concurrent checkpoint is performed, and
# and the test steps through the checksum change and the checkpoint in
# a deterministic way.
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
# - name   - name of the test sequence (for easy identification)
# - start  - start checksum state (enabled/disabled)
# - change - checksum change to initiate
# - final  - expected checksum state at the end
# - steps  - steps to go through (wait, wakeup, sql)
#
# XXX This is similar to 012_concurrent_checkpoint_crash, except that the
# checkpoint happens asynchronously (in the background), and the steps
# step through the injection points in a deterministic way.
#
# XXX Some of the injection points are in a critical section, which does
# not allow memory allocations etc. INJECTION_POINT_LOAD/_CACHED handles
# just private memory allocation, but 'wait' requires a shmem allocation.
# To deal with that, we setup an injection point $init outside a critical
# section to initialize the shmem stuff needed by 'wait'. This needs to
# be done for individual processes (e.g. the checkpointer needs to init a
# 'wait' point too, it's not enough to init one in the checksum worker).
#
# XXX We could also validate the checksums using pg_checksums, if the
# state is 'on'.
sub test_checksum_sequence
{
	my ($name, $start, $change, $final, @steps) = @_;

	# Start the test suite with pgbench running.
	background_rw_pgbench($node->port);

	# print the current test instructions, both into TAP output and into
	# the server log, to make correlation easier

	note($name);
	$node->safe_psql('postgres',
		"SELECT '========== " . $name . " =========='");

	# put the cluster into the initial checksum state, synchronously
	note('changing checksums into initial state: ' . $start);
	enable_data_checksums($node, wait => 'on', fast => 'true') if ($start eq 'enabled');
	disable_data_checksums($node, wait => 'off', fast => 'true') if ($start eq 'disabled');

	# attach all the injection points mentioned in 'wait' steps
	my $n = @steps;
	my $s;
	note('processing ' . $n . ' steps');
	for ($s = 0; $s < $n; $s++)
	{
		my @step = $steps[$s];
		my $action = $steps[$s][0];
		my $value = $steps[$s][1];

		note('step ' . $s . ' action ' . $action . ' / ' . $value);
		if ($action eq 'wait')
		{
			attach_injection_point($node, $value);
		}
	}

	# Trigger the checksum change, asynchronously
	note("triggering checksum change: " . $change);
	enable_data_checksums($node, fast => 'false') if ($change eq 'enable');
	disable_data_checksums($node, fast => 'false') if ($change eq 'disable');

	# now process all the steps - wait, wakeup, sql, etc.
	$n = @steps;
	note('processing ' . $n . ' steps');
	for ($s = 0; $s < $n; $s++)
	{
		my @step = $steps[$s];
		my $action = $steps[$s][0];
		my $value = $steps[$s][1];

		note('step ' . $s . ' action ' . $action . ' / ' . $value);
		if ($action eq 'wait')
		{
			wait_injection_point($node, $value);
		}
		elsif ($action eq 'wakeup')
		{
			wakeup_injection_point($node, $value);
		}
		elsif ($action eq 'sql')
		{
			note('sql: ' . $value);

			# initiate a background sql
			background_sql($node->port, $value);
		}
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

# sequence of steps to perform, each step is defined as an array of actions
#
# - type of an action (wait, wakeup, sql)
# - injection point or SQL command
#
# after processing all commands, the instance gets killed / shut down with
# immediate mode
my @steps = undef;

# CHECKSUMS-ON-1
#
# checkpoint after: datachecksums-enable-checksums-after-xlog
# crash after: datachecksums-enable-checksums-after-xlogctl
# checkpoint completes
#
# steps:
# 1) checksums:    write XLOG2_CHECKSUMS to WAL
# 2) start checkpoint
# 3) checkpointer: read XLogCtl->data_checksum_version
# 4) checksums:    update XLogCtl->data_checksum_version
# 5) checkpointer: write CHECKPOINT_REDO
# 6) checkpointer: complete the checkpoint
# 7) crash
@steps = (
    ['wait', 'datachecksums-enable-checksums-start'],	# initialize the wait
    ['wakeup', 'datachecksums-enable-checksums-start'],
    ['wait', 'datachecksums-enable-checksums-after-xlog'],
    ['sql', 'checkpoint'],
    ['wait', 'create-checkpoint-initial'],				# initialize the wait
    ['wakeup', 'create-checkpoint-initial'],
    ['wait', 'checkpoint-before-redo-wal'],
    ['wakeup', 'datachecksums-enable-checksums-after-xlog'],
    ['wait', 'datachecksums-enable-checksums-after-xlogctl'],
    ['wakeup', 'checkpoint-before-redo-wal'],
    ['wait', 'checkpoint-before-old-wal-removal'],
    ['wakeup', 'checkpoint-before-old-wal-removal']
);

test_checksum_sequence('CHECKSUMS-ON-1', 'disabled', 'enable', 'off', @steps);

# CHECKSUMS-ON-2
#
# checkpoint after: datachecksums-enable-checksums-after-xlog
# crash after: datachecksums-enable-checksums-after-xlogctl
# checkpoint completes
#
# steps:
# 1) checksums:    write XLOG2_CHECKSUMS to WAL
# 2) start checkpoint
# 3) checksums:    update XLogCtl->data_checksum_version
# 4) checkpointer: read XLogCtl->data_checksum_version
# 5) checkpointer: write CHECKPOINT_REDO
# 6) checkpointer: complete the checkpoint
# 7) crash
@steps = (
    ['wait', 'datachecksums-enable-checksums-start'],	# initialize the wait
    ['wakeup', 'datachecksums-enable-checksums-start'],
    ['wait', 'datachecksums-enable-checksums-after-xlog'],
    ['sql', 'checkpoint'],
    ['wait', 'create-checkpoint-initial'],				# initialize the wait
    ['wakeup', 'datachecksums-enable-checksums-after-xlog'],
    ['wakeup', 'create-checkpoint-initial'],
    ['wait', 'checkpoint-before-xlogctl-checksums'],
    ['wait', 'datachecksums-enable-checksums-after-xlogctl'],
    ['wakeup', 'checkpoint-before-xlogctl-checksums'],
    ['wait', 'checkpoint-before-old-wal-removal'],
    ['wakeup', 'checkpoint-before-old-wal-removal']
);

test_checksum_sequence('CHECKSUMS-ON-2', 'disabled', 'enable', 'off', @steps);

# CHECKSUMS-ON-3
#
# checkpoint happens after checksum worker updates XLogCtl
@steps = (
    ['wait', 'datachecksums-enable-checksums-start'],	# initialize the wait
    ['wakeup', 'datachecksums-enable-checksums-start'],
    ['wait', 'datachecksums-enable-checksums-after-xlogctl'],
    ['sql', 'checkpoint'],
    ['wait', 'create-checkpoint-initial'],				# initialize the wait
    ['wakeup', 'create-checkpoint-initial'],
    ['wait', 'checkpoint-before-redo-checksums'],
    ['wakeup', 'checkpoint-before-redo-checksums'],
    ['wait', 'checkpoint-before-old-wal-removal'],
    ['wakeup', 'checkpoint-before-old-wal-removal']
);

test_checksum_sequence('CHECKSUMS-ON-3', 'disabled', 'enable', 'off', @steps);

## FIXME do similar sequences for the opposite direction (enabled -> disabled)

$node->stop;
done_testing();
