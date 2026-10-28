#!/usr/bin/env bash
# Stop the processes scripts/linux/up.sh started.
#
#   scripts/linux/down.sh            # stop PostgreSQL and nats-server, keep the data and the keys
#   scripts/linux/down.sh --clean    # also remove the data, the minted keys and the credentials
#
# --clean is destructive and deliberately not the default: it deletes the database and the JetStream
# data, and the nsc keys that sign every credential (the .creds files in scripts/native/creds/ cannot
# be reproduced without them, so they go too).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
STATE="${ZB_LINUX_STATE:-$HERE/state}"
NATS_PID="$ROOT/scripts/native/nats-server.pid"

if [ -s "$NATS_PID" ] && kill -0 "$(cat "$NATS_PID")" 2>/dev/null; then
  echo "[linux] stopping nats-server"
  kill "$(cat "$NATS_PID")"
  for _ in $(seq 1 40); do kill -0 "$(cat "$NATS_PID")" 2>/dev/null || break; sleep 0.25; done
fi
rm -f "$NATS_PID"

PGDATA=""
if [ -r "$STATE/pg.env" ]; then
  # shellcheck disable=SC1091
  . "$STATE/pg.env"
  if "$HERE/pg_ctl" status; then
    echo "[linux] stopping postgres"
    "$HERE/pg_ctl" stop -m fast || echo "[linux] postgres did not stop cleanly" >&2
  fi
fi

if [ "${1:-}" = "--clean" ]; then
  echo "[linux] removing the data, scripts/linux/state/ and scripts/native/creds/ — the next up.sh mints new ones"
  [ -n "$PGDATA" ] && rm -rf "$PGDATA"
  rm -rf "$STATE" "$ROOT/scripts/native/creds"
fi
