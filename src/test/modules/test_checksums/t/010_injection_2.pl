
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

sub test_checksum_transition
{
	my ($start, $first, $second, $point, $final) = @_;

	$node->safe_psql('postgres',
		"SELECT '========== " . $start . " / " . $first . " / " . $second . " / " . $point . " / " . $final . " =========='");

	note($start . " / " . $first . " / " . $second . " / " . $point . " / " . $final);

	note('changing checksums into initial state: ' . $start);

	enable_data_checksums($node, wait => 'on') if ($start eq 'enabled');
	disable_data_checksums($node, wait => 'off') if ($start eq 'disabled');

	note('attaching injection point: ' . $point);
	$node->safe_psql('postgres',
		"SELECT injection_points_attach('" . $point . "','wait');"
	);

	note("triggering first checksum change: " . $first);

	enable_data_checksums($node) if ($first eq 'enable');
	disable_data_checksums($node) if ($first eq 'disable');

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
}

test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-inprogress-checksums-delay', 'off');
test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-inprogress-checksums-after-xlogctl', 'off');
test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-inprogress-checksums-after-controlfile', 'off');
test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-checksums-delay', 'off');
test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-checksums-after-xlogctl', 'off');
test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-checksums-after-controlfile', 'off');
test_checksum_transition('disabled', 'enable', 'disable', 'datachecksums-enable-checksums-after-checkpoint', 'off');

test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-inprogress-checksums-delay', 'on');
test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-inprogress-checksums-after-xlogctl', 'on');
test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-inprogress-checksums-after-controlfile', 'on');
test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-checksums-delay', 'on');
test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-checksums-after-xlogctl', 'on');
test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-checksums-after-controlfile', 'on');
test_checksum_transition('disabled', 'enable', 'enable', 'datachecksums-enable-checksums-after-checkpoint', 'on');

test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-inprogress-checksums-delay', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-inprogress-checksums-after-xlogctl', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-inprogress-checksums-after-controlfile', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-inprogress-checksums-after-checkpoint', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-checksums-delay', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-checksums-after-xlogctl', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-checksums-after-controlfile', 'off');
test_checksum_transition('enabled', 'disable', 'disable', 'datachecksums-disable-checksums-after-checkpoint', 'off');

test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-inprogress-checksums-delay', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-inprogress-checksums-after-xlogctl', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-inprogress-checksums-after-controlfile', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-inprogress-checksums-after-checkpoint', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-checksums-delay', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-checksums-after-xlogctl', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-checksums-after-controlfile', 'on');
test_checksum_transition('enabled', 'disable', 'enable', 'datachecksums-disable-checksums-after-checkpoint', 'on');

$node->stop;
done_testing();
