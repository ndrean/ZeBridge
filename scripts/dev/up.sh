#!/usr/bin/env bash
# Start the containerized scenario stack (scripts/dev/compose.yml) and make it ready:
# PostgreSQL and NATS up and healthy, the NATS credentials minted (first run), the init SQL
# applied, the publication created. Safe to run again: every step checks before it acts.
#
#   scripts/dev/up.sh
#
# Needs Docker with the compose plugin, a built bridge (zig build -Doptimize=ReleaseFast: its
# --init-sql output is what is applied) and .env.bridge with DATABASE_READER_URL,
# DATABASE_WRITER_URL and BRIDGE_CDC_PUBLICATION pointing at 127.0.0.1:5432.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BRIDGE="${ZB_BRIDGE_BIN:-$ROOT/zig-out/bin/bridge}"
dc() { docker compose -f "$HERE/compose.yml" "$@"; }

[ -x "$BRIDGE" ] || { echo "[dev] no bridge at $BRIDGE — build it first: zig build -Doptimize=ReleaseFast" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "[dev] the Docker daemon is not reachable" >&2; exit 1; }

# The bootstrap container writes as root and hands the files back to this user.
export ZB_DEV_UID="$(id -u)" ZB_DEV_GID="$(id -g)"
mkdir -p "$HERE/state" "$ROOT/scripts/native/creds"

echo "[dev] starting postgres and nats (first run: pulls images, mints the NATS credentials)"
dc up -d

# `up --wait` treats the one-shot bootstrap that exits 0 as a failure on some compose versions,
# so wait for the two servers by their health, and report the bootstrap by its exit code.
wait_healthy() {
  local name="$1" status
  for _ in $(seq 1 90); do
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$name" 2>/dev/null || echo missing)"
    [ "$status" = healthy ] && return 0
    sleep 1
  done
  echo "[dev] $name is not healthy (status: $status) — docker logs $name" >&2
  return 1
}
wait_healthy zb-dev-postgres
if [ "$(docker inspect -f '{{.State.ExitCode}}' zb-dev-nats-bootstrap 2>/dev/null || echo 1)" != 0 ]; then
  echo "[dev] the credential bootstrap failed — docker logs zb-dev-nats-bootstrap" >&2
  exit 1
fi
wait_healthy zb-dev-nats

# The init SQL is the bridge's own (--init-sql, from the same environment the bridge will read),
# applied through the container's psql: nothing but Docker is needed on this machine for it.
set -a
# shellcheck disable=SC1091
. "$ROOT/.env.bridge"
set +a
: "${BRIDGE_CDC_PUBLICATION:?set BRIDGE_CDC_PUBLICATION in .env.bridge — there is no default}"
psql_c() { dc exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d postgres "$@"; }

echo "[dev] applying the init SQL"
"$BRIDGE" --init-sql | psql_c -q >/dev/null

if [ "$(psql_c -Atc "SELECT count(*) FROM pg_publication WHERE pubname = '$BRIDGE_CDC_PUBLICATION'")" = 0 ]; then
  echo "[dev] creating publication $BRIDGE_CDC_PUBLICATION"
  psql_c -qc "SELECT step, status FROM public.zebridge_create_publication('$BRIDGE_CDC_PUBLICATION')" >/dev/null
fi

cat <<EOF

[dev] up. PostgreSQL 127.0.0.1:5432, NATS 127.0.0.1:4222 (operator mode), credentials in scripts/native/creds/.
Next, from this directory, with .env.bridge sourced and the scenarios' venv:
  scripts/reprovision.py                       # the baseline tables, catalogue rows and tenant mappings
  zig-out/bin/bridge                           # the long-running bridge on :27434 (the live group)
  scripts/scenarios/run.py all
Stop with scripts/dev/down.sh.
EOF
