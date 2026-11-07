#!/bin/sh
# Compile timeout_race.so, start a temporary server, and run repro.sql.
# Exits 0 when the leaked statement_timeout cancel is reproduced.
#
# Set PG_CONFIG to pick the PostgreSQL installation, and CC to override the
# compiler it reports.
set -eu

cd "$(dirname "$0")"

if [ -n "${PG_CONFIG:-}" ]; then
	:
elif command -v pg_config >/dev/null 2>&1; then
	PG_CONFIG=pg_config
elif [ -x /home/tomas/builds/master/bin/pg_config ]; then
	PG_CONFIG=/home/tomas/builds/master/bin/pg_config
else
	echo "pg_config not found; set PG_CONFIG" >&2
	exit 2
fi

BINDIR=$("$PG_CONFIG" --bindir)
INC=$("$PG_CONFIG" --includedir-server)
CC=${CC:-$("$PG_CONFIG" --cc)}
CFLAGS=$("$PG_CONFIG" --cflags)

# CC may be a command with arguments, e.g. "ccache gcc".
# shellcheck disable=SC2086
$CC -fPIC -shared $CFLAGS -I"$INC" -o timeout_race.so timeout_race.c

D=$(mktemp -d "${TMPDIR:-/tmp}/timeout-race.XXXXXX")
PORT=$((20000 + $$ % 20000))
cleanup() {
	"$BINDIR/pg_ctl" -D "$D" stop -m immediate >/dev/null 2>&1 || true
	rm -rf "$D"
}
trap cleanup EXIT INT TERM

"$BINDIR/initdb" -D "$D" -N --no-instructions >/dev/null
"$BINDIR/pg_ctl" -D "$D" -o "-k $D -p $PORT -c listen_addresses=" \
	-l "$D/server.log" start >/dev/null

OUT=$("$BINDIR/psql" -X -h "$D" -p "$PORT" -d postgres \
	-v so="$PWD/timeout_race.so" -f repro.sql 2>&1) || true
printf '%s\n' "$OUT"

printf '%s\n' "$OUT" | grep -q 'control next statement survived' || {
	echo "FAIL: control statement did not survive" >&2
	exit 1
}
printf '%s\n' "$OUT" | grep -q 'race next statement survived' && {
	echo "FAIL: next statement was not cancelled; race not reproduced" >&2
	exit 1
}
printf '%s\n' "$OUT" | grep -q 'canceling statement due to statement timeout' || {
	echo "FAIL: expected statement-timeout error was not reported" >&2
	exit 1
}
printf '%s\n' "$OUT" | grep -q 'session recovered' || {
	echo "FAIL: session did not recover after the leaked cancel" >&2
	exit 1
}

# The error found above may come from the control CALL alone.  Check that
# CALL spin_ms() completed and the error was reported after it.  repro.sql
# prints the race banner immediately before CALL spin_ms().
race=$(printf '%s\n' "$OUT" | sed -n '/--- race:/,$p')
printf '%s\n' "$race" | grep -q '^CALL$' || {
	echo "FAIL: race CALL did not complete" >&2
	exit 1
}
printf '%s\n' "$race" | grep -q 'canceling statement due to statement timeout' || {
	echo "FAIL: leaked cancel was not reported on the statement after CALL spin_ms" >&2
	exit 1
}

echo "REPRODUCED: CALL spin_ms() completed, and the next statement was cancelled by statement_timeout"
