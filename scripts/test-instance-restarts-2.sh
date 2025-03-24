#!/usr/bin/bash

OUTDIR=$(date +%Y%m%d-%H%M)
DATADIR_PRIMARY=$OUTDIR/data-primary
DATADIR_STANDBY=$OUTDIR/data-standby
PGCTLTIMEOUT=3600
WALDIR=$(pwd)/$OUTDIR/wal

killall -9 postgres
pgrep -f pgbench | xargs kill
#rm -Rf $DATADIR_PRIMARY $DATADIR_STANDBY primary.running standby.running

mkdir -p $OUTDIR/control $OUTDIR/wal

# init primary
pg_ctl -D $DATADIR_PRIMARY init
echo 'wal_level = logical' >> $DATADIR_PRIMARY/postgresql.conf 2>&1
echo "log_line_prefix = '%n %m [%p] [%b:%a] [%c:%l] [%s] [%v/%x] '" >> $DATADIR_PRIMARY/postgresql.conf 2>&1
#echo "log_min_duration_statement = 0" >> $DATADIR_PRIMARY/postgresql.conf 2>&1
echo "checkpoint_timeout = '1min'" >> $DATADIR_PRIMARY/postgresql.conf 2>&1
echo "archive_mode = 'on'" >> $DATADIR_PRIMARY/postgresql.conf 2>&1
# echo "archive_command = 'cp %p $WALDIR/.tmp && mv $WALDIR/.tmp $WALDIR/%f'" >> $DATADIR_PRIMARY/postgresql.conf 2>&1

pg_ctl -D $DATADIR_PRIMARY -l $OUTDIR/pg-primary.log start
touch $OUTDIR/primary.running

# init a replica
pg_basebackup -D $DATADIR_STANDBY -c fast -R -C -S replica
echo "port = 5433" >> $DATADIR_STANDBY/postgresql.conf 2>&1
pg_ctl -D $DATADIR_STANDBY -l $OUTDIR/pg-standby.log start
touch $OUTDIR/standby.running

function primary_start()
{
	r=$1

	if [ -f $OUTDIR/primary.running ]; then
		return
	fi

	touch $OUTDIR/primary.running

	echo `date` "loop $r start primary"

	while /bin/true; do
		ts=$(date +%s)
		echo `date` "loop $r start primary: $ts"

		pg_controldata $DATADIR_PRIMARY > $OUTDIR/control/primary.$ts

		pg_ctl -D $DATADIR_PRIMARY -l $OUTDIR/pg-primary.log start
		x=$?

		if [ "$x" == "0" ]; then
			break
		fi

		echo `date` "loop $r retry start primary"
		sleep 1
	done

	echo `date` "loop $r primary started"
}

function primary_stop()
{
	r=$1

	# stop the primary in some way (or not at all)
	m=$((RANDOM % 3))

	if [ "$m" == "0" ]; then
		lsn=$(psql postgres -t -A -c "select pg_current_wal_lsn()")
		ts=$(date +%s)
		echo `date` "stopping primary / immediate $ts / $lsn"
		pg_ctl -D $DATADIR_PRIMARY -m immediate stop
		pg_controldata $DATADIR_PRIMARY > $OUTDIR/control/primary.$ts
		rm $OUTDIR/primary.running
	elif [ "$m" == "1" ]; then
		lsn=$(psql postgres -t -A -c "select pg_current_wal_lsn()")
		ts=$(date +%s)
		echo `date` "stopping primary / fast $ts / $lsn"
		pg_ctl -D $DATADIR_PRIMARY -m fast stop
		pg_controldata $DATADIR_PRIMARY > $OUTDIR/control/primary.$ts
		rm $OUTDIR/primary.running
	else
		echo `date` "not stopping primary"
	fi
}

function primary_verify_checksums()
{
	r=$1
	s=$2

	m=$((RANDOM % 2))

	if [ "$m" == "0" ]; then
		echo `date` "stopping primary / immediate"
		pg_ctl -D $DATADIR_PRIMARY -m immediate stop
	else
		echo `date` "stopping primary / fast"
		pg_ctl -D $DATADIR_PRIMARY -m fast stop
	fi

	if [ "$s" == "enable" ] && [ "$m" != "0" ]; then
		echo `date` "primary / verify checksums"
		pg_checksums -c $DATADIR_PRIMARY >> $OUTDIR/checksums-primary.log 2>&1
	fi

	echo `date` "starting primary"
	pg_ctl -D $DATADIR_PRIMARY -l $OUTDIR/pg-primary.log start
}

function primary_pgbench()
{
	r=$1
	db=$2

	m=$((RANDOM % 2))
	c=$((RANDOM % 4 + 1))

	if [ "$m" == "0" ]; then
		echo `date` "starting pgbench on primary, $c clients"
		pgbench -c $c -P 1 -T 3600 $db >> $OUTDIR/pgbench-primary.log 2>&1 &
	else
		echo `date` "starting pgbench on primary (-C), $c clients"
		pgbench -C -c $c -P 1 -T 3600 $db >> $OUTDIR/pgbench-primary.log 2>&1 &
	fi
}

function standby_start()
{
	r=$1

	if [ -f $OUTDIR/standby.running ]; then
		return
	fi

	touch $OUTDIR/standby.running

	echo `date` "loop $r start standby"

	while /bin/true; do
		ts=$(date +%s)
		echo `date` "loop $r start standby: $ts"

		pg_controldata $DATADIR_STANDBY > $OUTDIR/control/standby.$ts

		pg_ctl -D $DATADIR_STANDBY -l $OUTDIR/pg-standby.log start
		x=$?

		if [ "$x" == "0" ]; then
			break
		fi

		echo `date` "loop $r retry start standby"
		sleep 1
	done

	echo `date` "loop $r standby started"
}

function standby_stop()
{
	r=$1

	# stop the standby in some way (or not at all)
	m=$((RANDOM % 3))

	if [ "$m" == "0" ]; then
		lsn=$(psql postgres -t -p 5433 -A -c "select pg_last_wal_replay_lsn()")
		ts=$(date +%s)
		echo `date` "stopping standby / immediate $ts / $lsn"
		pg_ctl -D $DATADIR_STANDBY -m immediate stop
		pg_controldata $DATADIR_STANDBY > $OUTDIR/control/standby.$ts
		rm $OUTDIR/standby.running
	elif [ "$m" == "1" ]; then
		lsn=$(psql postgres -t -p 5433 -A -c "select pg_last_wal_replay_lsn()")
		ts=$(date +%s)
		echo `date` "stopping standby / fast $ts / $lsn"
		pg_ctl -D $DATADIR_STANDBY -m fast stop
		pg_controldata $DATADIR_STANDBY > $OUTDIR/control/standby.$ts
		rm $OUTDIR/standby.running
	else
		echo `date` "not stopping standby"
	fi
}

function standby_verify_checksums()
{
	r=$1
	s=$2

	m=$((RANDOM % 2))

	if [ "$m" == "0" ]; then
		echo `date` "stopping standby / immediate"
		pg_ctl -D $DATADIR_STANDBY -m immediate stop
	else
		echo `date` "stopping standby / fast"
		pg_ctl -D $DATADIR_STANDBY -m fast stop
	fi

	if [ "$s" == "enable" ] && [ "$m" != "0" ]; then
		echo `date` "standby / verify checksums"
		pg_checksums -c $DATADIR_STANDBY >> $OUTDIR/checksums-standby.log 2>&1
	fi

	echo `date` "starting standby"
	pg_ctl -D $DATADIR_STANDBY -l $OUTDIR/pg-standby.log start
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

	if [ "$m" == "0" ]; then
		echo `date` "starting pgbench on standby, $c clients"
		pgbench -n -S -p 5433 -c $c -P 1 -T 3600 $db >> $OUTDIR/pgbench-standby.log 2>&1 &
	else
		echo `date` "starting pgbench on standby (-C), $c clients"
		pgbench -C -n -S -p 5433 -c $c -P 1 -T 3600 $db >> $OUTDIR/pgbench-standby.log 2>&1 &
	fi
}

function wait_for_checksum_state()
{
	r=$1
	db=$2
	state=$3

	touch $OUTDIR/write.pause

	while /bin/true; do

		c=$(psql -t -A $db -c "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums'")

		psql $db -c "SELECT * FROM pg_stat_progress_data_checksums"

		if [ "$state" == "enable" ] && [ "$c" == "on" ]; then
			break;
		elif [ "$state" == "disable" ] && [ "$c" == "off" ]; then
			break;
		fi

		echo `date` "loop $r checksum state: $c (sleeping)"
		sleep 1

	done

	rm $OUTDIR/write.pause
}

function change_checksums()
{
	r=$1
	db=$2
	state=$3

	fast="false"
	x=$((RANDOM % 2))
	if [ "$x" == "0" ]; then
		fast="true"
	fi

	if [ "$state" == "enable" ]; then
		echo `date` "loop $r enabling checksums fast $fast"
		psql $db -c "select pg_current_wal_lsn(), pg_enable_data_checksums(fast := $fast)"
	else
		echo `date` "loop $r disabling checksums fast $fast"
		psql $db -c "select pg_current_wal_lsn(), pg_disable_data_checksums(fast := $fast)"
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

# small test
sleep=10
m=fast
# default is 'on', so start with disable
s="disable"

for d in $(seq 0 9); do
	./pgbench-primary.sh $OUTDIR "test_$d" 4 > $OUTDIR/pgbench-primary-$d.log 2>&1 &
	./pgbench-standby.sh $OUTDIR "test_$d" 4 > $OUTDIR/pgbench-standby-$d.log 2>&1 &
done

# 100 loops of the primary restarts
for r in $(seq 1 200); do

	x=$((RANDOM % 10))
	db="test_$x"

	scale=$((RANDOM % 40 + 10))

	# maybe create a new DB with random scale
	dropdb --if-exists $db
	createdb $db
	pgbench -i -s $scale $db >> $OUTDIR/pgbench-init.log 2>&1

	# run pgbench in the background
	echo `date` "loop $r pgbench scale $scale"

	# sleep for a bit
	random_sleep $r $sleep

	# start the checksums change
	change_checksums $r $db $s

	# sleep for a bit
	random_sleep $r $sleep

	psql $db -c "SELECT * FROM pg_stat_progress_data_checksums"

	# stop the primary/standby in some way
	primary_stop $r
	standby_stop $r

	# start the primary/stadby again
	primary_start $r
	standby_start $r

	# sleep for a bit
	random_sleep $r $sleep

	# start the checksums change (again, to restart the workers)
	change_checksums $r $db $s

	# wait for the checksums to get enabled/disabled
	echo `date` "loop $r waiting for checksums to change in the instance"
	wait_for_checksum_state $r $db $s

	# stop primary/standby, verify checksums and start again
	echo `date` "loop $r verify checksums on primary"
	primary_verify_checksums $r $s

	echo `date` "loop $r verify checksums on standby"
	standby_verify_checksums $r $s

	# wait for standy to catch up
	echo `date` "loop $r wait for standby to catch up"
	standby_catch_up $r

	# flip the state for the next loop
	if [ "$s" == "disable" ]; then
		s="enable"
	else
		s="disable"
	fi

done
