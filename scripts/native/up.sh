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
  -c wal_sender_timeout=300s
  -c logical_decoding_work_mem=256MB
  -c wal_buffers=64MB
  -c commit_delay=1000
  -c commit_siblings=5
  -c checkpoint_timeout=15min
  -c wal_compression=zstd
)
# NOTES §10gk: initdb's defaults (128MB buffers, 1GB of WAL between checkpoints) made
# the 100k firehose a disk benchmark — the client backend wrote its own evicted pages,
# 687 checkpoints were forced by WAL volume, and full-page writes doubled the WAL.
# ZB_PG_BENCH=1 sizes the cluster for that benchmark (16 GB Mac). Not the default:
# shared_buffers is never given back while the server runs, so after a few big runs
# PostgreSQL held 3.7 GB of RAM, and max_wal_size kept 3.6 GB of recycled WAL on disk.
if [ "${ZB_PG_BENCH:-0}" = "1" ]; then
  PG_FLAGS+=(-c shared_buffers=4GB -c max_wal_size=16GB)
else
  PG_FLAGS+=(-c shared_buffers=512MB -c max_wal_size=2GB)
fi

FRESH_PG=0
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  FRESH_PG=1
  echo "[native] initdb → $PGDATA"
  "$PGBIN/initdb" -D "$PGDATA" -U postgres --auth=trust >/dev/null
fi

echo "[native] starting postgres on :$PGPORT"
"$PGBIN/pg_ctl" -D "$PGDATA" -l "$PG_LOG" -o "-p $PGPORT ${PG_FLAGS[*]}" start
"$PGBIN/pg_isready" -h 127.0.0.1 -p "$PGPORT" >/dev/null
# ⚠️ Not a `-c` flag: a command-line setting beats postgresql.auto.conf, so nothing
# short of a restart could change it — scripts/scenarios/slot_loss.py lowers it with
# ALTER SYSTEM + pg_reload_conf to invalidate a slot on purpose, and an operator may
# need the same knob live. Set once here; ALTER SYSTEM wins from then on.
# (two statements: ALTER SYSTEM refuses to run inside a transaction block)
"$PGBIN/psql" -h 127.0.0.1 -p "$PGPORT" -U postgres -d postgres -qc "ALTER SYSTEM SET max_slot_wal_keep_size = '10GB'" >/dev/null
"$PGBIN/psql" -h 127.0.0.1 -p "$PGPORT" -U postgres -d postgres -qc "SELECT pg_reload_conf()" >/dev/null

if [ "$FRESH_PG" = "1" ]; then
  echo "[native] applying init.core.template.sql + init.write.template.sql"
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env.admin"
  # ⚠️ The role credentials are NOT declared twice. `.env.bridge` owns them —
  # they are the bridge's own identity — and init.sql's `CREATE ROLE … PASSWORD`
  # needs the same secret in parts. Deriving beats duplicating: a password
  # written in two files drifts silently, and the failure lands at the next
  # bridge restart as an authentication error naming the ROLE, not the file.
  # shellcheck disable=SC1091
  source "$ROOT/.env.bridge"
  set +a
  eval "$(python3 "$ROOT/scripts/zb-derive-env.py")"
  export PG_HOST=127.0.0.1
  export PG_PORT="$PGPORT"
  "$PGBIN/psql" -h 127.0.0.1 -p "$PGPORT" -U postgres -d postgres \
    -c "ALTER USER postgres WITH PASSWORD '${PG_PASSWORD}';" >/dev/null
  cat "$ROOT/init.core.template.sql" "$ROOT/init.write.template.sql" \
    | envsubst \
    | "$PGBIN/psql" -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PGPORT" -U postgres -d postgres >/dev/null

  # The templates create NO publication (NOTES §10ad) — the name used to be
  # substituted into them, which made it a second spelling of the bridge's own
  # --pub with nothing checking that the two agreed. Creating one is an explicit
  # act now, and this function is the only supported way to do it: it also
  # attaches the three internal tables (ddl_events, gc_watermark, user_tenants)
  # that a hand-made `CREATE PUBLICATION` silently left out.
  : "${BRIDGE_CDC_PUBLICATION:?set it in .env.bridge — there is no default}"
  echo "[native] creating publication $BRIDGE_CDC_PUBLICATION"
  "$PGBIN/psql" -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PGPORT" -U postgres -d postgres \
    -c "SELECT step, status, detail FROM public.zebridge_create_publication('$BRIDGE_CDC_PUBLICATION')"
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
  echo "[native] creating streams + KV buckets from grammar.json"
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env.admin"
  set +a
  NATS_URL=127.0.0.1:4222
  echo "$NATS_BRIDGE_NKEY_SEED" > "$ROOT/scripts/native/seed.txt"
  SEED="$ROOT/scripts/native/seed.txt"

  CDC_PREFIX=$(jq -r '.subjects.cdc_prefix' "$ROOT/src/grammar.json")
  MUTATIONS_STREAM=$(jq -r '.streams.mutations' "$ROOT/src/grammar.json")
  VERDICTS_STREAM=$(jq -r '.streams.verdicts' "$ROOT/src/grammar.json")
  MUTATIONS_PREFIX=$(jq -r '.subjects.mutations_prefix' "$ROOT/src/grammar.json")
  MUTATION_ERROR_PREFIX=$(jq -r '.subjects.mutation_error_prefix' "$ROOT/src/grammar.json")
  MUTATION_ACK_PREFIX=$(jq -r '.subjects.mutation_ack_prefix' "$ROOT/src/grammar.json")
  SCHEMA_KV=$(jq -r '.kv.schemas' "$ROOT/src/grammar.json")
  TENANTS_KV=$(jq -r '.kv.tenants' "$ROOT/src/grammar.json")
  GENERATIONS_KV=$(jq -r '.generations.kv' "$ROOT/src/grammar.json")
  LIVE_KV=$(jq -r '.kv.live // "live"' "$ROOT/src/grammar.json")
  # CDC_<TENANT> and CDC_PUBLIC are no longer created here: the BRIDGE reconciles
  # them at boot — tenants from zebridge_user_tenants, the public subject set from
  # zebridge_catalogue. The catalogue is the config.

  # §10fk: the anti-flood cap is the STREAM's, not the bridge's. The subject carries
  # the principal (mutation.<principal>.<table>.<op>), so a per-subject cap is a
  # per-principal backlog: past it that principal's publish is refused at the door
  # (its PubAck errors, the client keeps the entry and retries after its grace) and
  # nobody else notices. `discard=new` refuses rather than evicting — with the old
  # policy a flood evicted the oldest: honest queued writes, stored verdicts, dead
  # letters. Workqueue retention deletes a mutation once the bridge acks it, so the
  # cap counts what is QUEUED; with `limits` it would count every write of the last
  # max-age, a quota per two hours. Dead letters have no consumer and stay until
  # max-age. The bridge declares its rate on /status.
  MUTATION_BACKLOG_PER_PRINCIPAL="${MUTATION_BACKLOG_PER_PRINCIPAL:-5000}"
  nats --server "$NATS_URL" --nkey "$SEED" stream add "$MUTATIONS_STREAM" \
    --subjects="$MUTATIONS_PREFIX.>,$MUTATION_ERROR_PREFIX.>" \
    --storage=file --retention=work --max-age=2h --max-bytes=1G --replicas=1 \
    --discard=new --max-msgs-per-subject="$MUTATION_BACKLOG_PER_PRINCIPAL" --discard-per-subject --defaults >/dev/null
  # Verdicts (mutation_ack.<principal>.<msg_id>, and the ban mutation_ack.<p>.revoked)
  # live in their OWN stream. In MUTATIONS they filled the 1 GiB that incoming writes
  # need: ~193 B each, so ~4.6 min of 20k writes/s, and then `discard new` refused
  # every write until verdicts aged out (NOTES §10jj). Here `discard old` is right: a
  # verdict pushed out early costs a client that was offline that long one replay,
  # which the ingress judges idempotently. One message per subject: a msg_id is judged
  # once, and a new ban replaces the old one.
  nats --server "$NATS_URL" --nkey "$SEED" stream add "$VERDICTS_STREAM" \
    --subjects="$MUTATION_ACK_PREFIX.>" \
    --storage=file --retention=limits --max-age=2h --max-bytes=1G --replicas=1 \
    --discard=old --max-msgs-per-subject=1 --allow-direct --defaults >/dev/null
  # 2h, not days: a mutation is consumed within seconds, and its verdict is kept only for
  # a client that went offline between the send and the reply — on reconnect it collects
  # the verdict, or, past this window, REPLAYS the write, which the ingress judges
  # idempotently (NOTES §10dy: a write that already landed is accepted again). Acks
  # retained for a week were 30 messages a second of dead weight under the wasp.

  nats --server "$NATS_URL" --nkey "$SEED" kv add "$SCHEMA_KV" --history=10 --replicas=1 >/dev/null
  # PROTOCOL.md "The Connection Flow" §Step 0. Not yet fed by a PG trigger — seed by
  # hand to test the NATS side alone:
  #   nats --server "$NATS_URL" --nkey "$SEED" kv put "$TENANTS_KV" alice acme
  #   nats --server "$NATS_URL" --nkey "$SEED" kv put "$TENANTS_KV" bob   globex
  nats --server "$NATS_URL" --nkey "$SEED" kv add "$TENANTS_KV" --history=1 --replicas=1 >/dev/null
  # Chain manifests for the generation producer (NOTES.md §1.13). --history=1: only the
  # current manifest is ever read. The CLI enables direct gets, which the per-tenant
  # DIRECT.GET grants in nats-server.conf depend on. The per-tenant OBJ_gen-<tenant>
  # object buckets are NOT pre-created here: the producer provisions them at runtime,
  # the same shape dyntenant streams have.
  nats --server "$NATS_URL" --nkey "$SEED" kv add "$GENERATIONS_KV" --history=1 --replicas=1 >/dev/null
  # Fleet heartbeats (NOTES §10dc): last value per key, and a TTL — a client that
  # stops beating drops out by itself. The bridge creates this too if it is missing.
  nats --server "$NATS_URL" --nkey "$SEED" kv add "$LIVE_KV" --history=1 --ttl=90s --replicas=1 >/dev/null
  echo "[native] streams + KV buckets created"
fi

cat <<EOF

[native] up. Point the bridge at:
  DATABASE_READER_URL="postgres://bridge_reader:bridge_password_changeme@127.0.0.1:${PGPORT}/postgres"
  DATABASE_WRITER_URL="postgres://bridge_writer:writer_password_changeme@127.0.0.1:${PGPORT}/postgres"
  NATS_URL="nats://127.0.0.1:4222"
  NATS_BRIDGE_NKEY_SEED=<from .env.admin>
  ZB_PSQL="$PGBIN/psql -h 127.0.0.1 -p ${PGPORT} -U postgres"

Tear down: scripts/native/down.sh (add --clean to also wipe postgres-data/ and nats-data/)
EOF
