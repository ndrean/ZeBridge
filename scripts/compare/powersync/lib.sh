#!/bin/bash
# Shared by the comparison's scripts: the settings, the repository root, the database guard. Prints no secret.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
[ -f "$HERE/settings" ] || { echo "copy settings.example to settings and fill it in" >&2; exit 2; }
. "$HERE/settings"
: "${CMP_HOST:?settings: CMP_HOST}" "${CMP_DB_ENV:?settings: CMP_DB_ENV}" "${CMP_DB_SYSID:?settings: CMP_DB_SYSID}"
case "$CMP_DB_ENV" in /*) DB_ENV_FILE="$CMP_DB_ENV" ;; *) DB_ENV_FILE="$ROOT/$CMP_DB_ENV" ;; esac
SEC="$HERE/secrets"
mkdir -p -m 700 "$SEC"

ssh_h() { ssh -o BatchMode=yes -o ConnectTimeout=15 "$CMP_HOST" "$@"; }
# psql's errors can echo a connection URL: replace any with <url>.
redact() { sed 's/postgres[a-z]*:\/\/[^ ]*/<url>/g'; }

# The database's variables, then the guard: the administrator URL must reach the database named in `settings`.
db_env() {
  set -a; . "$DB_ENV_FILE"; set +a
  ADMIN="${SB_ADMIN_URL:-${ADMIN_DATABASE_URL:-}}"
  [ -n "$ADMIN" ] || { echo "no SB_ADMIN_URL or ADMIN_DATABASE_URL in $CMP_DB_ENV" >&2; exit 2; }
  local got
  got="$(psql "$ADMIN" -X -tA -c 'SELECT system_identifier FROM pg_control_system()' 2>/dev/null || true)"
  [ "$got" = "$CMP_DB_SYSID" ] || { echo "WRONG DATABASE: $CMP_DB_ENV reaches '${got:-nothing}', settings say '$CMP_DB_SYSID'; refusing" >&2; exit 2; }
}
admin_sql() { psql "$ADMIN" -X -q -v ON_ERROR_STOP=1 "$@" 2>&1 | redact; }

# sensor_events' row count, sampled every second while a write crowd runs: `count_rate start`, then `count_rate stop`
# waits until the count stops rising (the drain) and prints what count_rate.py makes of the samples.
count_rate() {
  if [ "$1" = start ]; then
    COUNT_LOG="$SEC/count.log"; : > "$COUNT_LOG"
    ( while :; do echo "$(python3 -c 'import time; print(time.time())') $(psql "$ADMIN" -X -tA -c 'SELECT count(*) FROM sensor_events' 2>/dev/null)" >> "$COUNT_LOG"; sleep 1; done ) &
    COUNT_PID=$!
  else
    local last=-1 now
    while :; do now=$(tail -1 "$COUNT_LOG" | cut -d' ' -f2); [ "$now" = "$last" ] && break; last=$now; sleep 3; done
    { kill "$COUNT_PID" && wait "$COUNT_PID"; } 2>/dev/null || true
    python3 "$HERE/count_rate.py" "$COUNT_LOG"
    rm -f "$COUNT_LOG"
  fi
}
