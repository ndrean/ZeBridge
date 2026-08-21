#!/usr/bin/env bash
# Stop the native PostgreSQL + NATS pair `up.sh` starts.
#
# Plain `down.sh` stops both processes and leaves postgres-data/ and nats-data/ in
# place — same as `docker compose down` (no `-v`), so the next `up.sh` resumes from
# where this left off.
#
# `down.sh --clean` also deletes postgres-data/ and nats-data/ — same as
# `docker compose down -v`. This is the "leave a clean state" step for a repeatable
# benchmark run: no leftover WAL retained by a stale replication slot, no accumulated
# JetStream data from a prior test. It is destructive and deliberately not the
# default — ask before assuming a `-v`-shaped wipe is wanted, the way this project's
# actual data volume got wiped once already this session by assuming exactly that.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PGBIN="/opt/homebrew/opt/postgresql@18/bin"
PGDATA="$ROOT/postgres-data"
NATSDATA="$ROOT/nats-data"
NATS_PIDFILE="$ROOT/scripts/native/nats-server.pid"

if [ -s "$PGDATA/PG_VERSION" ]; then
  echo "[native] stopping postgres"
  "$PGBIN/pg_ctl" -D "$PGDATA" stop -m fast 2>/dev/null || true
fi

if [ -f "$NATS_PIDFILE" ]; then
  echo "[native] stopping nats-server"
  kill "$(cat "$NATS_PIDFILE")" 2>/dev/null || true
  rm -f "$NATS_PIDFILE"
fi

if [ "${1:-}" = "--clean" ]; then
  echo "[native] wiping postgres-data/ and nats-data/ — next up.sh starts fresh"
  rm -rf "$PGDATA" "$NATSDATA"
fi
