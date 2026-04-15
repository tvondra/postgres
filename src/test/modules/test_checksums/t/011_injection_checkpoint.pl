
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

sub test_checksum_transition
{
	my ($start, $change, $point, $final) = @_;

	# Start the test suite with pgbench running.
	background_rw_pgbench($node->port);

	$node->safe_psql('postgres',
		"SELECT '========== " . $start . " / " . $change . " / " . $point . " / " . $final . " =========='");

	note($start . " / " . $change . " / " . $point . " / " . $final);

	note('changing checksums into initial state: ' . $start);

	enable_data_checksums($node, wait => 'on') if ($start eq 'enabled');
	disable_data_checksums($node, wait => 'off') if ($start eq 'disabled');

	note('attaching injection point: ' . $point);
	$node->safe_psql('postgres',
		"SELECT injection_points_attach('" . $point . "','wait');"
	);

	note("triggering checksum change: " . $change);

	enable_data_checksums($node) if ($change eq 'enable');
	disable_data_checksums($node) if ($change eq 'disable');

	note("waiting for the injection point to be hit");
	$node->poll_query_until(
		'postgres',
		"SELECT COUNT(*) FROM pg_catalog.pg_stat_activity WHERE wait_event = '" . $point . "'",
		'1');

	note('checkpoint');
	$node->safe_psql('postgres', "CHECKPOINT");

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

	$node->stop(stopmode());
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

test_checksum_transition('disabled', 'enable', 'datachecksums-enable-inprogress-checksums-delay', 'on');
test_checksum_transition('disabled', 'enable', 'datachecksums-enable-inprogress-checksums-after-xlogctl', 'on');
test_checksum_transition('disabled', 'enable', 'datachecksums-enable-inprogress-checksums-after-controlfile', 'on');
test_checksum_transition('disabled', 'enable', 'datachecksums-enable-checksums-delay', 'on');
test_checksum_transition('disabled', 'enable', 'datachecksums-enable-checksums-after-xlogctl', 'on');
test_checksum_transition('disabled', 'enable', 'datachecksums-enable-checksums-after-controlfile', 'on');
test_checksum_transition('disabled', 'enable', 'datachecksums-enable-checksums-after-checkpoint', 'on');

test_checksum_transition('enabled', 'disable', 'datachecksums-disable-inprogress-checksums-delay', 'off');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-inprogress-checksums-after-xlogctl', 'off');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-inprogress-checksums-after-controlfile', 'off');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-inprogress-checksums-after-checkpoint', 'off');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-checksums-delay', 'off');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-checksums-after-xlogctl', 'off');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-checksums-after-controlfile', 'off');
test_checksum_transition('enabled', 'disable', 'datachecksums-disable-checksums-after-checkpoint', 'off');

$node->stop;
done_testing();
