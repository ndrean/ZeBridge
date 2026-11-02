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
