#!/usr/bin/bash

OUTDIR=$(date +%Y%m%d-%H%M)
DATADIR_PRIMARY=$OUTDIR/data-primary
DATADIR_STANDBY=$OUTDIR/data-standby
PGCTLTIMEOUT=3600
WALDIR=$(pwd)/$OUTDIR/wal

killall -9 postgres
pgrep -f pgbench | xargs kill

mkdir -p $OUTDIR/control $OUTDIR/wal

# init primary
pg_ctl -D $DATADIR_PRIMARY init
echo 'wal_level = logical' >> $DATADIR_PRIMARY/postgresql.conf 2>&1
echo "log_line_prefix = '%n %m [%p] [%b:%a] [%c:%l] [%s] [%v/%x] '" >> $DATADIR_PRIMARY/postgresql.conf 2>&1
#echo "log_min_duration_statement = 0" >> $DATADIR_PRIMARY/postgresql.conf 2>&1
echo "checkpoint_timeout = '1min'" >> $DATADIR_PRIMARY/postgresql.conf 2>&1
echo "archive_mode = 'on'" >> $DATADIR_PRIMARY/postgresql.conf 2>&1
echo "archive_command = 'cp %p $WALDIR/.tmp && mv $WALDIR/.tmp $WALDIR/%f'" >> $DATADIR_PRIMARY/postgresql.conf 2>&1

pg_ctl -D $DATADIR_PRIMARY -l $OUTDIR/pg-primary.log start

# init a replica
pg_basebackup -D $DATADIR_STANDBY -c fast -R -C -S replica
echo "port = 5433" >> $DATADIR_STANDBY/postgresql.conf 2>&1
pg_ctl -D $DATADIR_STANDBY -l $OUTDIR/pg-standby.log start

# lock a file (fd 100), to communicate with child scripts
exec 100>run.lock || exit 1
flock 100 || exit 1

function primary_pgbench()
{
	r=$1
	db=$2

	m=$((RANDOM % 2))
	c=$((RANDOM % 4 + 1))
	d=$((RANDOM % 10 + 10))

	if [ "$m" == "0" ]; then
		echo `date` "starting pgbench on primary, $c clients"
		pgbench -c $c -P 1 -T $d $db >> $OUTDIR/pgbench-primary.log 2>&1 &
	else
		echo `date` "starting pgbench on primary (-C), $c clients"
		pgbench -C -c $c -P 1 -T $d $db >> $OUTDIR/pgbench-primary.log 2>&1 &
	fi
}

function standby_catch_up()
{
	r=$1

	while /bin/true; do

		d=$(psql -t -A postgres -c "select (pg_current_wal_lsn() - replay_lsn) from pg_stat_replication")
		x=$(psql -t -A postgres -c "select (pg_current_wal_lsn() - replay_lsn) < 16384 from pg_stat_replication")

		if [ "$x" == "t" ]; then
			break
		fi

		echo `date` "loop $r waiting for standby to catch up ($d bytes)"
		sleep 1

	done
}

function standby_pgbench()
{
	r=$1
	db=$2

	m=$((RANDOM % 2))
	c=$((RANDOM % 4 + 1))
	d=$((RANDOM % 10 + 10))

	if [ "$m" == "0" ]; then
		echo `date` "starting pgbench on standby, $c clients"
		pgbench -n -S -p 5433 -c $c -P 1 -T $d $db >> $OUTDIR/pgbench-standby.log 2>&1 &
	else
		echo `date` "starting pgbench on standby (-C), $c clients"
		pgbench -C -n -S -p 5433 -c $c -P 1 -T $d $db >> $OUTDIR/pgbench-standby.log 2>&1 &
	fi
}

function random_sleep()
{
	r=$1
	sleep=$2

	x=$((RANDOM % sleep + 1))
	echo `date` "loop $r sleeping for $x seconds"
	sleep $x
}

function wait_for_pgbench()
{
	r=$1

	while /bin/true; do
		c=$(pgrep -f pgbench | wc -l)

		if [ "$c" == "0" ]; then
			break
		fi

		echo `date` "loop $r wait for pgbench to end, count $c"
		sleep 1
	done
}


# small test
sleep=10
m=fast
# default is 'on', so start with disable
s="disable"

./random-checkpoints.sh > $OUTDIR/checkpoints.log 2>&1 &

# run multiple sessions triggering checksum changes
./random-changes.sh > $OUTDIR/changes-1.log 2>&1 &
./random-changes.sh > $OUTDIR/changes-2.log 2>&1 &
./random-changes.sh > $OUTDIR/changes-3.log 2>&1 &

# 100 loops of the primary restarts
for r in $(seq 1 200); do

	x=$((RANDOM % 10))
	db="test_$x"

	scale=$((RANDOM % 40 + 10))

	# maybe create a new DB with random scale
	dropdb --if-exists $db
	createdb $db
	pgbench -i -s $scale $db >> $OUTDIR/pgbench-init.log 2>&1

	# wait for the standy to catch up
	echo `date` "loop $r wait for standby to catch up"
	standby_catch_up $r

	# run pgbench in the background
	echo `date` "loop $r pgbench scale $scale"

	# run pgbench on primary/standby in the background
	primary_pgbench $r $db
	standby_pgbench $r $db

	# wait for pgbench to complete
	wait_for_pgbench $r

	# wait for the standy to catch up
	echo `date` "loop $r wait for standby to catch up"
	standby_catch_up $r

done
