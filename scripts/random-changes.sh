#!/usr/bin/env bash

exec 100>run.lock || exit 1

while /bin/true; do

	flock -n 100
	r=$?

	echo "r = $r"

	# if the flock worked, the master script is not running, so exit too
	if [ "$r" == "0" ]; then
		break
	fi

	sleep $s

	m=$((RANDOM % 2))
	if [ "$m" == "0" ]; then

		m=$((RANDOM % 2))
		fast="false"

		if [ "$m" = "0" ]; then
			fast="true"
		fi

		echo `date` "pg_enable_data_checksums(fast := $fast)"
		psql postgres -c "select pg_enable_data_checksums(fast := $fast)"
	else
		echo `date` "pg_disable_data_checksums"
		psql postgres -c "select pg_disable_data_checksums()"
	fi

	s=$((RANDOM % 5 + 1))
	sleep $s

done
