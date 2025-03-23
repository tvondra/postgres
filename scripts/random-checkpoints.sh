#!/usr/bin/env bash

exec 100>run.lock || exit 1

while /bin/true; do

	s=$((RANDOM % 5 + 1))
	sleep $s

	flock -n 100
	r=$?

	# if the flock worked, the master script is not running, so exit too
	if [ "$r" == "0" ]; then
		break
	fi

	echo `date` "checkpoint"
	psql postgres -c "checkpoint"

done
