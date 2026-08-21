#!/usr/bin/env bash
# Native (no-Docker, no-VM) PostgreSQL + NATS JetStream, for the throughput/CPU
# comparisons that need Docker/OrbStack's I/O virtualization out of the picture
# entirely — see README.md's "measured throughput" section for why that gap matters.
#
# Idempotent: safe to run again against an already-initialized data dir (just starts
# the two servers). For a genuinely clean slate (fresh WAL, fresh replication slots,
# fresh JetStream streams — no accumulated state from a prior run), use `down.sh --clean`
# first, matching `docker compose down -v` in spirit.
#
# Data lives in postgres-data/ and nats-data/ at the repo root (already .gitignore'd —
# these directories predate this script). Only the *config* — postgresql's startup
# flags below, and nats-server.conf, rendered from .env.admin's existing dev
# credentials — is committed. .env.admin/.env.bridge are themselves tracked in this
# repo with throwaway "_changeme" dev credentials, so a rendered nats-server.conf
# carries no more exposure than what's already there.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PGBIN="/opt/homebrew/opt/postgresql@18/bin"
PGDATA="$ROOT/postgres-data"
PGPORT="${NATIVE_PG_PORT:-5432}"
NATSDATA="$ROOT/nats-data"
NATSCONF="$ROOT/scripts/native/nats-server.conf"
NATS_LOG="$ROOT/scripts/native/nats-server.log"
PG_LOG="$ROOT/scripts/native/postgres.log"

# ─── PostgreSQL ───────────────────────────────────────────────────────────────
# Flags match docker-compose.full.yml's postgres-primary `command:` exactly, so a
# native run and a Docker run are configured identically — only the virtualization
# layer differs.
PG_FLAGS=(
  -c wal_level=logical
  -c max_replication_slots=10
  -c max_wal_senders=10
  -c max_slot_wal_keep_size=10GB
  -c wal_sender_timeout=300s
  -c logical_decoding_work_mem=256MB
  -c wal_buffers=64MB
  -c commit_delay=1000
  -c commit_siblings=5
)

FRESH_PG=0
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  FRESH_PG=1
  echo "[native] initdb → $PGDATA"
  "$PGBIN/initdb" -D "$PGDATA" -U postgres --auth=trust >/dev/null
fi

echo "[native] starting postgres on :$PGPORT"
"$PGBIN/pg_ctl" -D "$PGDATA" -l "$PG_LOG" -o "-p $PGPORT ${PG_FLAGS[*]}" start
"$PGBIN/pg_isready" -h 127.0.0.1 -p "$PGPORT" >/dev/null

if [ "$FRESH_PG" = "1" ]; then
  echo "[native] applying init.core.template.sql + init.write.template.sql"
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env.admin"
  set +a
  export PG_HOST=127.0.0.1
  export PG_PORT="$PGPORT"
  "$PGBIN/psql" -h 127.0.0.1 -p "$PGPORT" -U postgres -d postgres \
    -c "ALTER USER postgres WITH PASSWORD '${PG_PASSWORD}';" >/dev/null
  cat "$ROOT/init.core.template.sql" "$ROOT/init.write.template.sql" \
    | envsubst \
    | "$PGBIN/psql" -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PGPORT" -U postgres -d postgres >/dev/null
  echo "[native] postgres initialized"
fi

# ─── NATS JetStream ───────────────────────────────────────────────────────────
FRESH_NATS=0
if [ ! -d "$NATSDATA/jetstream" ]; then
  FRESH_NATS=1
fi

mkdir -p "$NATSDATA"
echo "[native] starting nats-server on :4222"
nohup nats-server -js -c "$NATSCONF" > "$NATS_LOG" 2>&1 &
echo $! > "$ROOT/scripts/native/nats-server.pid"
for _ in $(seq 1 30); do
  curl -s -o /dev/null localhost:8222/varz && break
  sleep 0.5
done

if [ "$FRESH_NATS" = "1" ]; then
  echo "[native] creating streams + KV buckets from topology.json"
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env.admin"
  set +a
  NATS_URL=127.0.0.1:4222
  echo "$NATS_NKEY_SEED" > "$ROOT/scripts/native/seed.txt"
  SEED="$ROOT/scripts/native/seed.txt"

  CDC_PREFIX=$(jq -r '.subjects.cdc_prefix' "$ROOT/topology.json")
  INIT_STREAM=$(jq -r '.streams.init' "$ROOT/topology.json")
  INIT_PREFIX=$(jq -r '.subjects.init_prefix' "$ROOT/topology.json")
  REQUESTS_STREAM=$(jq -r '.streams.requests' "$ROOT/topology.json")
  REQUESTS_PREFIX=$(jq -r '.subjects.snapshot_request' "$ROOT/topology.json")
  MUTATIONS_STREAM=$(jq -r '.streams.mutations' "$ROOT/topology.json")
  MUTATIONS_PREFIX=$(jq -r '.subjects.mutations_prefix' "$ROOT/topology.json")
  MUTATION_ERROR_PREFIX=$(jq -r '.subjects.mutation_error_prefix' "$ROOT/topology.json")
  MUTATION_ACK_PREFIX=$(jq -r '.subjects.mutation_ack_prefix' "$ROOT/topology.json")
  SCHEMA_KV=$(jq -r '.kv.schemas' "$ROOT/topology.json")
  SNAPSHOT_KV=$(jq -r '.kv.snapshots' "$ROOT/topology.json")
  TENANTS_KV=$(jq -r '.kv.tenants' "$ROOT/topology.json")
  TENANT_PREFIX=$(jq -r '.cdc_streams.tenant_prefix' "$ROOT/topology.json")
  PUBLIC_STREAM=$(jq -r '.cdc_streams.public' "$ROOT/topology.json")
  # CDC-family streams outlive INIT by a day on purpose: snapshot generation isn't
  # instant (queued behind other tables, no timeout), so CDC events for writes right
  # after a snapshot's LSN must not age out before the snapshot itself does — see
  # README's "The NATS streams and buckets" for the full reasoning.
  SNAP_RET="${SNAP_RET_SECONDS:-603000}"

  for T in $(jq -r '.tenants[]' "$ROOT/topology.json"); do
    UPPER=$(echo "$T" | tr '[:lower:]' '[:upper:]')
    nats --server "$NATS_URL" --nkey "$SEED" stream add "$TENANT_PREFIX$UPPER" \
      --subjects="$CDC_PREFIX.$T.>" --storage=file --retention=limits --max-age=8d \
      --max-msgs=10000000 --max-bytes=10G --replicas=1 --compression s2 --defaults >/dev/null
  done

  PUBLIC_SUBJECTS=$(jq -r --arg p "$CDC_PREFIX" --arg o "$OPEN_TENANT" \
    '[.public_tables[] | $p + "." + . + ".>"] + [$p + "." + $o + ".>"] | join(",")' "$ROOT/topology.json")
  nats --server "$NATS_URL" --nkey "$SEED" stream add "$PUBLIC_STREAM" \
    --subjects="$PUBLIC_SUBJECTS" --storage=file --retention=limits --max-age=8d \
    --max-msgs=10000000 --max-bytes=10G --replicas=1 --compression s2 --defaults >/dev/null

  nats --server "$NATS_URL" --nkey "$SEED" stream add "$INIT_STREAM" \
    --subjects="$INIT_PREFIX.>" --storage=file --retention=limits --max-age=7d \
    --max-msgs=10000000 --max-bytes=8G --replicas=1 --compression s2 --defaults >/dev/null

  nats --server "$NATS_URL" --nkey "$SEED" stream add "$REQUESTS_STREAM" \
    --subjects="$REQUESTS_PREFIX.>" --storage=file --retention=limits \
    --max-msgs-per-subject=1 --discard=new --discard-per-subject \
    --max-age="${SNAP_RET}s" --replicas=1 --defaults >/dev/null

  nats --server "$NATS_URL" --nkey "$SEED" stream add "$MUTATIONS_STREAM" \
    --subjects="$MUTATIONS_PREFIX.>,$MUTATION_ERROR_PREFIX.>,$MUTATION_ACK_PREFIX.>" \
    --storage=file --retention=limits --max-age=7d --max-bytes=1G --replicas=1 --defaults >/dev/null

  nats --server "$NATS_URL" --nkey "$SEED" kv add "$SCHEMA_KV" --history=10 --replicas=1 >/dev/null
  nats --server "$NATS_URL" --nkey "$SEED" kv add "$SNAPSHOT_KV" --history=1 --replicas=1 >/dev/null
  # PROTOCOL.md "The Connection Flow" §Step 0. Not yet fed by a PG trigger — seed by
  # hand to test the NATS side alone:
  #   nats --server "$NATS_URL" --nkey "$SEED" kv put "$TENANTS_KV" alice acme
  #   nats --server "$NATS_URL" --nkey "$SEED" kv put "$TENANTS_KV" bob   globex
  nats --server "$NATS_URL" --nkey "$SEED" kv add "$TENANTS_KV" --history=1 --replicas=1 >/dev/null
  echo "[native] streams + KV buckets created"
fi

cat <<EOF

[native] up. Point the bridge at:
  DATABASE_URL="postgres://bridge_reader:bridge_password_changeme@127.0.0.1:${PGPORT}/postgres"
  DATABASE_WRITER_URL="postgres://bridge_writer:writer_password_changeme@127.0.0.1:${PGPORT}/postgres"
  NATS_URL="nats://127.0.0.1:4222"
  NATS_NKEY_SEED=<from .env.admin>
  ZB_PSQL="$PGBIN/psql -h 127.0.0.1 -p ${PGPORT} -U postgres"

Tear down: scripts/native/down.sh (add --clean to also wipe postgres-data/ and nats-data/)
EOF
