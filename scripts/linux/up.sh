#!/usr/bin/env bash
# The scenarios' stack on a Linux machine without Docker: PostgreSQL (logical replication) and NATS in
# operator mode as plain processes, the credentials minted by nsc, the init SQL applied. The same stack as
# scripts/dev/compose.yml (same flags, same operator-mode configuration, same credentials), for a sandbox, a CI
# runner or a workstation where containers are not an option. On a Mac use scripts/dev/up.sh.
#
#   scripts/linux/up.sh            # start, mint creds on first run, apply the init SQL
#   scripts/linux/down.sh          # stop (--clean also removes the data, the keys and the credentials)
#
# Needs: PostgreSQL server binaries (initdb, pg_ctl, psql: the postgresql package; ZB_PGBIN names the directory
# if they are not on PATH or in /usr/lib/postgresql/*/bin), nats-server >= 2.15, the nats and nsc command-line
# tools, a built bridge (zig build -Doptimize=ReleaseFast: its --init-sql output is what is applied) and .env.bridge
# with BRIDGE_CDC_PUBLICATION. Run as root (a container, a sandbox) PostgreSQL is started as the `postgres` user:
# it refuses to run as root.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
STATE="${ZB_LINUX_STATE:-$HERE/state}"
BRIDGE="${ZB_BRIDGE_BIN:-$ROOT/zig-out/bin/bridge}"
PGPORT="${ZB_DEV_PG_PORT:-5432}"
NATS_PID="$ROOT/scripts/native/nats-server.pid"   # where chaos.py, nats_outage.py and matrix.py look for it

die() { echo "[linux] $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "$1 is not on PATH — $2"; }

[ -x "$BRIDGE" ] || die "no bridge at $BRIDGE — build it first: zig build -Doptimize=ReleaseFast"
need nats-server "install nats-server >= 2.15 (https://github.com/nats-io/nats-server/releases)"
need nats "install the NATS CLI (https://github.com/nats-io/natscli/releases)"
need nsc "install nsc (https://github.com/nats-io/nsc/releases)"
need curl "install curl"

# ── which PostgreSQL ────────────────────────────────────────────────────────
PGBIN="${ZB_PGBIN:-}"
if [ -z "$PGBIN" ]; then
  if command -v pg_ctl >/dev/null 2>&1; then PGBIN="$(dirname "$(command -v pg_ctl)")"
  else PGBIN="$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -V | tail -1 || true)"; fi
fi
[ -n "$PGBIN" ] && [ -x "$PGBIN/pg_ctl" ] || die "no PostgreSQL server binaries — install the postgresql package or set ZB_PGBIN"
pg_major() { "$1/pg_ctl" --version 2>/dev/null | grep -oE '[0-9]+' | head -1 || true; }
# The first pg_ctl on PATH can be an old one (a Homebrew postgresql@14 beside the distribution's 16): unless ZB_PGBIN said which,
# a PATH server older than 15 gives way to the newest distribution install that is not.
if [ -z "${ZB_PGBIN:-}" ] && [ "$(pg_major "$PGBIN")" -lt 15 ] 2>/dev/null; then
  alt="$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -V | tail -1 || true)"
  if [ -n "$alt" ] && [ -x "$alt/pg_ctl" ] && [ "$(pg_major "$alt")" -ge 15 ] 2>/dev/null; then
    echo "[linux] the pg_ctl on PATH is PostgreSQL $(pg_major "$PGBIN"); using $alt instead"
    PGBIN="$alt"
  fi
fi
# The init SQL reads pg_publication_rel.prqual (row filters), which PostgreSQL 15 added: an older server fails half way through
# the SQL with "column pr.prqual does not exist". The first pg_ctl on PATH is not always the newest installed.
PG_MAJOR="$(pg_major "$PGBIN")"
[ "${PG_MAJOR:-0}" -ge 15 ] || die "PostgreSQL ${PG_MAJOR:-?} at $PGBIN is too old (15 or newer): set ZB_PGBIN to a newer one, e.g. ZB_PGBIN=/usr/lib/postgresql/16/bin, and run down.sh --clean"
# PostgreSQL refuses to run as root: its data directory then belongs to the `postgres` user, and (that user
# cannot traverse most home directories) lives outside the repository.
PG_OS_USER=""
if [ "$(id -u)" = 0 ]; then
  id postgres >/dev/null 2>&1 || die "running as root needs a 'postgres' operating-system user (the postgresql package creates it)"
  PG_OS_USER=postgres
  PGDATA="${ZB_PGDATA:-/var/lib/zebridge-dev/pgdata}"
else
  PGDATA="${ZB_PGDATA:-$STATE/pgdata}"
fi
PG_LOG="$STATE/postgres.log"
as_pg() { if [ -n "$PG_OS_USER" ]; then runuser -u "$PG_OS_USER" -- "$@"; else "$@"; fi; }

# The flags of scripts/dev/compose.yml (and scripts/native/up.sh). max_slot_wal_keep_size is NOT here: a
# command-line setting beats ALTER SYSTEM, and slot_loss.py lowers it live.
PG_OPTS="-p $PGPORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=/tmp -c wal_level=logical -c max_replication_slots=10 \
-c max_wal_senders=10 -c wal_sender_timeout=300s -c logical_decoding_work_mem=256MB -c wal_buffers=64MB -c commit_delay=1000 \
-c commit_siblings=5 -c shared_buffers=${ZB_PG_SHARED_BUFFERS:-512MB} -c max_wal_size=2GB"
# compose.yml says wal_compression=zstd, which needs a server built with zstd: `on` is accepted by every build
PG_OPTS="$PG_OPTS -c wal_compression=on"

mkdir -p "$STATE" "$ROOT/scripts/native/creds" "$(dirname "$NATS_PID")"
if [ -n "$PG_OS_USER" ]; then
  mkdir -p "$(dirname "$PGDATA")"; chown "$PG_OS_USER" "$(dirname "$PGDATA")"
  touch "$PG_LOG"; chown "$PG_OS_USER" "$PG_LOG"
fi
# What scripts/linux/pg_ctl needs to stop and start this cluster the way the restart scenarios do.
{
  printf 'PGBIN=%q\nPGDATA=%q\nPG_LOG=%q\nPG_OS_USER=%q\nPG_OPTS=%q\nPGPORT=%q\n' "$PGBIN" "$PGDATA" "$PG_LOG" "$PG_OS_USER" "$PG_OPTS" "$PGPORT"
} > "$STATE/pg.env"

FRESH_PG=0
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  FRESH_PG=1
  echo "[linux] initdb → $PGDATA"
  # C locale, as compose.yml: standby.py starts a COPY of this database with another PostgreSQL, and a locale
  # name that only one system spells (en_US.utf8 / en_US.UTF-8) makes it refuse to start.
  as_pg "$PGBIN/initdb" -D "$PGDATA" -U postgres --auth=trust --locale=C --encoding=UTF8 >/dev/null
fi

if ! as_pg "$PGBIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; then
  echo "[linux] starting postgres on :$PGPORT"
  as_pg "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PG_LOG" -o "$PG_OPTS" -t 90 -w start >/dev/null \
    || { echo "[linux] postgres did not start — last lines of $PG_LOG:" >&2; tail -15 "$PG_LOG" >&2; exit 1; }
fi
export PGOPTIONS="${PGOPTIONS:-} -c client_min_messages=warning"   # the init SQL is idempotent and says so, at length
PSQL=("$PGBIN/psql" -h 127.0.0.1 -p "$PGPORT" -U postgres -d postgres)
"$PGBIN/pg_isready" -h 127.0.0.1 -p "$PGPORT" >/dev/null || die "postgres does not answer on :$PGPORT"
if [ "$FRESH_PG" = 1 ]; then
  # Not a `-c` flag, for the reason above (two statements: ALTER SYSTEM refuses a transaction block).
  "${PSQL[@]}" -qc "ALTER SYSTEM SET max_slot_wal_keep_size = '10GB'" >/dev/null
  "${PSQL[@]}" -qc "SELECT pg_reload_conf()" >/dev/null
fi

# ── NATS in operator mode ───────────────────────────────────────────────────
echo "[linux] minting the NATS credentials (nsc)"
ZB_STATE_DIR="$STATE" bash "$ROOT/scripts/native/jwt-bootstrap.sh" > "$STATE/jwt-bootstrap.log" 2>&1 \
  || { echo "[linux] the credential bootstrap failed — tail of $STATE/jwt-bootstrap.log:" >&2; tail -15 "$STATE/jwt-bootstrap.log" >&2; exit 1; }

# scripts/dev/nats-server.conf with its two container paths made local: the JetStream directory, and the
# fragment `nsc generate config` wrote beside this file.
mkdir -p "$STATE/nats-js"
sed -e "s#store_dir: \"/data\"#store_dir: \"$STATE/nats-js\"#" \
    -e 's#include "state/resolver.conf"#include "resolver.conf"#' \
    "$ROOT/scripts/dev/nats-server.conf" > "$STATE/nats-server.conf"

nats_alive() { [ -s "$NATS_PID" ] && kill -0 "$(cat "$NATS_PID")" 2>/dev/null; }
if nats_alive; then
  # The bootstrap added a signing key and rewrote resolver.conf: a server that was already up keeps the
  # account JWT it loaded (the enrolled clients' JWTs would be refused). A reload reads it again.
  kill -HUP "$(cat "$NATS_PID")"
else
  echo "[linux] starting nats-server on :4222"
  # (cd first, apart from the `&`: `cd X && cmd &` backgrounds the whole list, and $! is then a wrapper shell, not the server)
  (cd "$ROOT" || exit 1; nohup nats-server -c "$STATE/nats-server.conf" > "$STATE/nats-server.log" 2>&1 < /dev/null & echo $! > "$NATS_PID")
fi
for _ in $(seq 1 60); do curl -s -o /dev/null http://127.0.0.1:8222/healthz && break; sleep 0.5; done
curl -s -o /dev/null http://127.0.0.1:8222/healthz || { echo "[linux] nats-server did not come up — tail of $STATE/nats-server.log:" >&2; tail -15 "$STATE/nats-server.log" >&2; exit 1; }

# ── the init SQL and the publication ────────────────────────────────────────
set -a
# shellcheck disable=SC1091
. "$ROOT/.env.bridge"
set +a
if [ -z "${BRIDGE_CDC_PUBLICATION:-}" ]; then
  echo "[linux] BRIDGE_CDC_PUBLICATION is not set — there is no default. Pass the name:" >&2
  echo "        BRIDGE_CDC_PUBLICATION=zb_pub scripts/linux/up.sh" >&2
  exit 1
fi
echo "[linux] applying the init SQL"
"$BRIDGE" --init-sql | "${PSQL[@]}" -v ON_ERROR_STOP=1 -q >/dev/null
if [ "$("${PSQL[@]}" -Atc "SELECT count(*) FROM pg_publication WHERE pubname = '$BRIDGE_CDC_PUBLICATION'")" = 0 ]; then
  echo "[linux] creating publication $BRIDGE_CDC_PUBLICATION"
  "${PSQL[@]}" -v ON_ERROR_STOP=1 -qc "SELECT step, status FROM public.zebridge_create_publication('$BRIDGE_CDC_PUBLICATION')" >/dev/null
fi

cat <<EOF

[linux] up. PostgreSQL 127.0.0.1:$PGPORT ($PGBIN), NATS 127.0.0.1:4222 (operator mode), credentials in scripts/native/creds/.
The database is EMPTY. The next steps (the exports, reprovision.py, the bridge, the groups) are in scripts/linux/README.md,
from its step 3. In every terminal, after the activation and env-file lines of that step:

     export NATS_URL=nats://127.0.0.1:4222
     export ZB_PGBIN="$PGBIN" ZB_PG_CTL="$HERE/pg_ctl" ZB_JWT_STATE="$STATE"
     # (no ZB_NATS_CONTAINER: the scenarios signal the nats-server process, whose pid is in scripts/native/nats-server.pid)

Stop with scripts/linux/down.sh (--clean wipes the data, the keys and the credentials).
EOF
