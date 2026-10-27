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
if [ -z "${BRIDGE_CDC_PUBLICATION:-}" ]; then
  # Not in the env file: the publication is named by whoever runs, never guessed (NOTES §10ad).
  echo "[dev] BRIDGE_CDC_PUBLICATION is not set — there is no default. Pass the name:" >&2
  echo "        BRIDGE_CDC_PUBLICATION=zb_pub scripts/dev/up.sh" >&2
  exit 1
fi
psql_c() { dc exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d postgres "$@"; }

# The native NATS has a TLS listener and the env file is written for it (tls://); this server is
# plain. A tls:// URL against it ends in `TlsRecordOverflow`: the server answers the ClientHello
# with the plain-text `-ERR 'Authentication Error'`, which the TLS client reads as an enormous record.
case "${NATS_URL:-}" in
  tls://*) echo "[dev] note: the env file's NATS_URL is tls://, this NATS is plain — use the nats:// URL printed below" >&2 ;;
esac

echo "[dev] applying the init SQL"
"$BRIDGE" --init-sql | psql_c -q >/dev/null

if [ "$(psql_c -Atc "SELECT count(*) FROM pg_publication WHERE pubname = '$BRIDGE_CDC_PUBLICATION'")" = 0 ]; then
  echo "[dev] creating publication $BRIDGE_CDC_PUBLICATION"
  psql_c -qc "SELECT step, status FROM public.zebridge_create_publication('$BRIDGE_CDC_PUBLICATION')" >/dev/null
fi

cat <<EOF

[dev] up. PostgreSQL 127.0.0.1:5432, NATS 127.0.0.1:4222 (operator mode), credentials in scripts/native/creds/.
Next, from the repository root.

Once (the scenarios' Python environment: nats-py, nkeys, msgpack, duckdb, boto3):
  python3 -m venv scripts/scenarios/.venv
  scripts/scenarios/.venv/bin/pip install -r scripts/scenarios/requirements.txt

Every terminal (reprovision.py and run.py are run by the venv's Python, so activate it):
  source scripts/scenarios/.venv/bin/activate
  set -a && . ./.env.bridge && set +a && export BRIDGE_CDC_PUBLICATION=$BRIDGE_CDC_PUBLICATION
  export NATS_URL=nats://127.0.0.1:4222         # after the line above: this NATS has no TLS (tls:// gives TlsRecordOverflow)

Then:
  scripts/reprovision.py                       # the baseline tables, catalogue rows and tenant mappings
  zig-out/bin/bridge --pub \$BRIDGE_CDC_PUBLICATION   # the long-running bridge on :27434 (the live group), in its own terminal
  scripts/scenarios/run.py all                 # in another terminal, same environment
Stop with scripts/dev/down.sh (--clean wipes the volumes, the keys and the credentials).
EOF
