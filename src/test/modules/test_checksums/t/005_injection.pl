
# Copyright (c) 2025, PostgreSQL Global Development Group

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

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# ---------------------------------------------------------------------------
# Test cluster setup
#

# Initiate testcluster
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(no_data_checksums => 1);
$node->start;

# Set up test environment
$node->safe_psql('postgres', 'CREATE EXTENSION test_checksums;');

# ---------------------------------------------------------------------------
# Inducing failures in processing

# Force enabling checksums to fail by marking one of the databases as having
# failed in processing.
disable_data_checksums($node, wait => 1);
$node->safe_psql('postgres', 'SELECT dcw_inject_fail_database(true);');
enable_data_checksums($node, wait => 'off');
$node->safe_psql('postgres', 'SELECT dcw_inject_fail_database(false);');

# Force the enable checksums processing to make multiple passes by removing
# one database from the list in the first pass.  This will simulate a CREATE
# DATABASE during processing.  Doing this via fault injection makes the test
# not be subject to exact timing.
$node->safe_psql('postgres', 'SELECT dcw_prune_dblist(true);');
enable_data_checksums($node, wait => 'on');

# ---------------------------------------------------------------------------
# Timing and retry related tests
#
SKIP:
{
	skip 'Data checksum delay tests not enabled in PG_TEST_EXTRA', 4
	  if (!$ENV{PG_TEST_EXTRA}
		|| $ENV{PG_TEST_EXTRA} !~ /\bchecksum_extended\b/);

	# Inject a delay in the barrier for enabling checksums
	disable_data_checksums($node, wait => 1);
	$node->safe_psql('postgres', 'SELECT dcw_inject_delay_barrier();');
	enable_data_checksums($node, wait => 'on');

	# Fake the existence of a temporary table at the start of processing, which
	# will force the processing to wait and retry in order to wait for it to
	# disappear.
	disable_data_checksums($node, wait => 1);
	$node->safe_psql('postgres', 'SELECT dcw_fake_temptable(true);');
	enable_data_checksums($node, wait => 'on');
}

$node->stop;
done_testing();
