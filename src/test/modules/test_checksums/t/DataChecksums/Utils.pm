
# Copyright (c) 2025, PostgreSQL Global Development Group

=pod

=head1 NAME

DataChecksums::Utils - Utility functions for testing data checksums in a running cluster

=head1 SYNOPSIS

  use PostgreSQL::Test::Cluster;
  use DataChecksums::Utils qw( .. );

  # Create, and start, a new cluster
  my $node = PostgreSQL::Test::Cluster->new('primary');
  $node->init;
  $node->start;

  test_checksum_state($node, 'off');

  enable_data_checksums($node);

  wait_for_checksum_state($node, 'on');


=cut

package DataChecksums::Utils;

use strict;
use warnings FATAL => 'all';
use Exporter 'import';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

our @EXPORT = qw(
  test_checksum_state
  wait_for_checksum_state
  enable_data_checksums
  disable_data_checksums
);

=pod

=head1 METHODS

=over

=item test_checksum_state(node, state)

Test that the current value of the data checksum GUC in the server running
at B<node> matches B<state>.  If the values differ, a test failure is logged.
Returns True if the values match, otherwise False.

=cut

sub test_checksum_state
{
	my ($postgresnode, $state) = @_;

	my $result = $postgresnode->safe_psql('postgres',
		"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
	);
	is($result, $state, 'ensure checksums are set to ' . $state);
	return $result eq $state;
}

=item wait_for_checksum_state(node, state)

Test the value of the data checksum GUC in the server running at B<node>
repeatedly until it matches B<state> or times out.  Processing will run for
$PostgreSQL::Test::Utils::timeout_default seconds before timing out.  If the
values differ when the process times out, False is returned and a test failure
is logged, otherwise True.

=cut

sub wait_for_checksum_state
{
	my ($postgresnode, $state) = @_;

	my $res = $postgresnode->poll_query_until(
		'postgres',
		"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
		$state);
	is($res, 1, 'ensure data checksums are transitioned to ' . $state);
	return $res == 1;
}

=item enable_data_checksums($node, %params)

Function for enabling data checksums in the cluster running at B<node>.

=over

=item cost_delay

The C<cost_delay> to use when enabling data checksums, default is 0.

=item cost_limit

The C<cost_limit> to use when enabling data checksums, default is 100.

=item fast

If set to C<true> an immediate checkpoint will be issued after data
checksums are enabled.  Setting this to false will lead to slower tests.
The default is true.

=item wait

If defined, the function will wait for the state defined in this parameter,
waiting timing out, before returning.  The function will wait for
$PostgreSQL::Test::Utils::timeout_default seconds before timing out.

=back

=cut

sub enable_data_checksums
{
	my $postgresnode = shift;
	my %params = @_;

	# Set sane defaults for the parameters
	$params{cost_delay} = 0 unless (defined($params{cost_delay}));
	$params{cost_limit} = 100 unless (defined($params{cost_limit}));
	$params{fast} = 'true' unless (defined($params{fast}));

	note("enable_data_checksums delay ", $params{cost_delay},
		 " limit ", $params{cost_limit},
		 " fast ", $params{fast});

	my $query = <<'EOQ';
SELECT pg_enable_data_checksums(%s, %s, %s);
EOQ

	$postgresnode->safe_psql(
		'postgres',
		sprintf($query,
			$params{cost_delay}, $params{cost_limit}, $params{fast}));

	wait_for_checksum_state($postgresnode, $params{wait})
	  if (defined($params{wait}));
}

=item disable_data_checksums($node, %params)

Function for disabling data checksums in the cluster running at B<node>.

=over

=item wait

If defined, the function will wait for the state to turn to B<off>, or
waiting timing out, before returning.  The function will wait for
$PostgreSQL::Test::Utils::timeout_default seconds before timing out.
Unlike in C<enable_data_checksums> the value of the parameter is discarded.

=back

=cut

sub disable_data_checksums
{
	my $postgresnode = shift;
	my %params = @_;

	# Set sane defaults for the parameters
	$params{fast} = 'true' unless (defined($params{fast}));

	note("disable_data_checksums fast ", $params{fast});

	my $query = <<'EOQ';
SELECT pg_disable_data_checksums(%s);
EOQ

	$postgresnode->safe_psql('postgres', sprintf($query, $params{fast}));

	wait_for_checksum_state($postgresnode, 'off') if (defined($params{wait}));
}

=pod

=back

=cut

1;
