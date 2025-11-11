#!/usr/bin/env bash

for t in 1 3; do

	dropdb --if-exists test
	createdb test
	psql test < create-$t.sql > /dev/null 2>&1

	for m in a b; do

		for o in off on; do

			sed "s/OPT/$o/" select-$t$m.sql > script.sql

			tps=$(pgbench -n -f script.sql -T 10 test | grep 'tps = ' | awk '{print $3}')

			echo $t $m $o $tps

		done

	done

done
