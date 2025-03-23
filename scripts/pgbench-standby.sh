#!/usr/bin/env bash

OUTDIR=$1
db=$2
CLIENTS=$3

while /bin/true; do

	m=$((RANDOM % 2))
	c=$((RANDOM % CLIENTS + 1))

	if [ "$m" == "0" ]; then
		echo `date` "starting pgbench on primary, $c clients"
		pgbench -n -S -p 5433 -j $c -c $c -P 1 -T 1 $db
	else
		echo `date` "starting pgbench on primary (-C), $c clients"
		pgbench -n -S -p 5433 -C -j $c -c $c -P 1 -T 1 $db
	fi

done
