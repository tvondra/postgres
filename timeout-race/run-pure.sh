#!/bin/sh
# Start a temporary server and run repro-pure.sql as a non-superuser role.
# Exits 0 when the leaked statement_timeout cancel is reproduced.
#
# Set BINDIR (or PG_CONFIG) to pick the PostgreSQL installation, and LEN to
# override the string length used by repro-pure.sql.
set -eu

cd "$(dirname "$0")"

if [ -n "${BINDIR:-}" ]; then
	:
elif [ -n "${PG_CONFIG:-}" ]; then
	BINDIR=$("$PG_CONFIG" --bindir)
elif command -v pg_config >/dev/null 2>&1; then
	BINDIR=$(pg_config --bindir)
elif [ -x /home/tomas/builds/master/bin/pg_config ]; then
	BINDIR=/home/tomas/builds/master/bin
else
	echo "pg_config not found; set BINDIR or PG_CONFIG" >&2
	exit 2
fi

SQL=repro-pure.sql
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

"$BINDIR/psql" -X -q -h "$D" -p "$PORT" -d postgres \
	-c "CREATE ROLE repro LOGIN NOSUPERUSER"
super=$("$BINDIR/psql" -X -At -h "$D" -p "$PORT" -d postgres -U repro \
	-c "SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
[ "$super" = f ] || {
	echo "FAIL: role repro is a superuser" >&2
	exit 1
}

"$BINDIR/postgres" --version
OUT=$("$BINDIR/psql" -X -h "$D" -p "$PORT" -d postgres -U repro \
	${LEN:+-v len="$LEN"} -f "$SQL" 2>&1) || true
printf '%s\n' "$OUT"

# psql prefixes errors with the script line, which tells which statement
# received the statement-timeout error.
line() {
	n=$(grep -n -F -- "$1" "$SQL" | cut -d: -f1)
	[ "$(printf '%s\n' "$n" | grep -c .)" -eq 1 ] || {
		echo "expected one line containing \"$1\" in $SQL" >&2
		exit 2
	}
	echo "$n"
}
err_at() {
	printf '%s\n' "$OUT" | grep -q -F -- \
		"psql:$SQL:$1: ERROR:  canceling statement due to statement timeout"
}
fail() {
	echo "FAIL: $*" >&2
	exit 1
}

ctrl_call=$(line "CALL pg_temp.sink(length(repeat(")
race_call=$(line "CALL pg_temp.sink(length(lpad(")
race_insert=$(line "INSERT INTO t VALUES (1);")
fetch=$(line "FETCH 1 FROM c;")
commit=$(line "COMMIT;")

err_at "$ctrl_call" ||
	fail "control CALL was not cancelled; increase LEN"
printf '%s\n' "$OUT" | grep -q 'control next statement survived' ||
	fail "control: the next statement did not survive"
err_at "$race_call" &&
	fail "race CALL was cancelled itself"
err_at "$race_insert" ||
	fail "INSERT after the race CALL was not cancelled; increase LEN"
printf '%s\n' "$OUT" | grep -A2 '^ rows_inserted' | grep -q -x ' *0' ||
	fail "the cancelled INSERT inserted a row"
err_at "$fetch" &&
	fail "race FETCH was cancelled itself"
err_at "$commit" ||
	fail "COMMIT after the race FETCH was not cancelled; increase LEN"
printf '%s\n' "$OUT" | grep -q 'session recovered' ||
	fail "session did not recover after the leaked cancel"

echo "REPRODUCED as non-superuser: CALL and FETCH completed after" \
	"statement_timeout fired; the next INSERT and COMMIT were cancelled"
