#!/bin/bash
# The ZeBridge side, on the comparison host. It needs the release's `zebridge` and `libzb` packages there
# (/usr/bin/bridge, libzbcore.so), nats-server, psql and Python 3; nothing ZeBridge runs as a service there.
#   zebridge.sh up             a temporary stack in ~/zbcmp/zb: its own nats-server (127.0.0.1:14222, JetStream
#                              domain `cmp`), mutation admission and the bridge (4 ingress lanes, HTTP 127.0.0.1:27534)
#   zebridge.sh creds [N]      N client identities p000.. (default 50) in tenant `cmp`, minted from the stack's
#                              operator, mapped on the database; the clients' files to ~/zbcmp/cl (needs the
#                              scenarios' venv: scripts/scenarios/README.md)
#   zebridge.sh run PHASE ...  one phase of zb_cmp.py on the host: seed N | roundtrip S | burst N | writes P M
#   zebridge.sh tools          a Python venv on the host (~/zbcmp/venv): the `zebridge` package at the installed
#                              libzb's version, nats-py, nkeys, msgpack; for the two crowd commands below
#   zebridge.sh ingress N RATE SECONDS
#                              the write crowd: scripts/bench/ingress_load.py, N principals (one NATS connection
#                              each, after `creds N`), RATE writes/s offered in all, into sensor_events; each
#                              verdict timed, and the rows PostgreSQL committed counted before and after
#   zebridge.sh enroll N       N enrolled identities for storm.py, through the temporary bridge's /enroll
#   zebridge.sh storm N [MARKERS]
#                              the sync crowd: storm/storm.py, N libzb clients seeding storm_items at once, then
#                              MARKERS seconds of one write a second timed to every client
#   zebridge.sh down           stop what `up` started (by its pid files, nothing else)
#   zebridge.sh clean          down, then remove ~/zbcmp/zb, ~/zbcmp/cl, ~/zbcmp/venv and the replicas in /tmp
# Prints no secret.
. "$(dirname "$0")/lib.sh"

case "${1:-}" in
up)
  # Only what the bridge needs from the database's file, in a 0600 file that is copied and deleted here.
  (umask 077; grep -E '^(DATABASE_READER_URL|DATABASE_WRITER_URL|BRIDGE_CDC_PUBLICATION|BRIDGE_CDC_SLOT|GENERATIONS_ENABLED|BASE_BUF)=' "$DB_ENV_FILE" > "$SEC/env.db")
  ssh_h 'for p in ~/zbcmp/zb/*.pid; do [ -f "$p" ] && kill "$(cat "$p")" 2>/dev/null; done; rm -rf ~/zbcmp/zb && mkdir -p -m 700 ~/zbcmp/zb'
  scp -q "$SEC/env.db" "$CMP_HOST:zbcmp/zb/env.db" && rm -f "$SEC/env.db"
  ssh_h 'bash -s' <<'REMOTE'
set -e
cd ~/zbcmp/zb && chmod 600 env.db
/usr/bin/bridge --version | head -1
/usr/bin/bridge --init-nats operator --dir ~/zbcmp/zb/id --js-domain cmp > init-nats.log 2>&1 || { tail -5 init-nats.log; exit 1; }
conf=$(cat id/nats-server.conf)
printf '%s\nhost: "127.0.0.1"\nport: 14222\njetstream { domain: cmp, store_dir: "%s/js" }\n' "${conf%%$'\n'port:*}" "$HOME/zbcmp/zb" > nats.conf
nohup "$(command -v nats-server)" -c nats.conf > nats.log 2>&1 &
echo $! > nats.pid
for i in $(seq 30); do grep -q "Server is ready" nats.log && break; sleep 0.5; done
grep -q "Server is ready" nats.log && echo "nats-server ready on 127.0.0.1:14222" || { tail -5 nats.log; exit 1; }
cat > run.env <<EOF
NATS_URL=nats://127.0.0.1:14222
NATS_CREDS=$HOME/zbcmp/zb/id/creds/bridge.creds
NATS_JS_DOMAIN=cmp
NATS_TLS_CA=
MUTATION_ADMISSION=1
ZB_INGRESS_LANES=4
GENERATION_CADENCE_SECONDS=30
LOG_LEVEL=info
ENROLL_NATS_URL=nats://127.0.0.1:14222
EOF
chmod 600 run.env
( set -a; . ./env.db; . ./id/.env.nats; . ./run.env; set +a; exec nohup /usr/bin/bridge --mutation-ingress ) > admission.log 2>&1 &
echo $! > admission.pid
for i in $(seq 30); do grep -q "mutation admission ready" admission.log && break; sleep 1; done
grep -q "mutation admission ready" admission.log && echo "admission ready" || { tail -5 admission.log; exit 1; }
# id/.env.nats carries the client signing key: the bridge serves /enroll (storm.py enroll).
( set -a; . ./env.db; . ./id/.env.nats; . ./run.env; set +a; ZB_FEED_RESTART=1 exec nohup /usr/bin/bridge --pub "$BRIDGE_CDC_PUBLICATION" --slot "$BRIDGE_CDC_SLOT" --port 27534 ) > bridge.log 2>&1 &
echo $! > bridge.pid
for i in $(seq 90); do grep -q "Replication started successfully" bridge.log && break; sleep 1; done
grep -q "Replication started successfully" bridge.log && echo "bridge replicating" || { grep -E "error" bridge.log | tail -5; exit 1; }
# A client that connects before the table's first snapshot waits for it: a cold seed would time the wait.
# Any generation that carries a full: g1 on a new database, a later one (`delta+full`) when this NATS is new but the
# database already recorded a chain.
snap="g[0-9]+ for 'cmp'/'storm_items': [a-z+]*full"
for i in $(seq 300); do grep -qE "$snap" bridge.log && break; sleep 1; done
grep -oE "$snap.*" bridge.log | head -1 | grep . || { echo "no snapshot of storm_items after 300 s"; exit 1; }
echo "errors so far: $(grep -c 'error(' bridge.log admission.log | paste -sd' ' -)"
REMOTE
  ;;
creds)
  db_env
  N="${2:-50}"
  rm -rf "$SEC/creds" && mkdir -m 700 "$SEC/creds"
  trap 'rm -f "$SEC/creds/operator.store"' EXIT   # the operator's seeds: deleted whatever happens
  (umask 077; ssh_h 'cat ~/zbcmp/zb/id/operator.store' > "$SEC/creds/operator.store")
  ( cd "$ROOT/scripts/scenarios" && . .venv/bin/activate && python3 - "$SEC/creds" "$N" <<'EOF'
import os, pathlib, sys, nkeys
sys.path.insert(0, ".")
from rotate_client_key import client_creds
d = pathlib.Path(sys.argv[1])
keys = dict(l.split("=", 1) for l in (d / "operator.store").read_text().splitlines() if "=" in l and not l.startswith("#"))
account = nkeys.from_seed(keys["ACCOUNT_SEED"].encode()).public_key.decode()
for i in range(int(sys.argv[2])):
    p = f"p{i:03d}"
    fd = os.open(d / f"{p}.creds", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.write(fd, client_creds(keys["SK_CLIENT_SEED"], account, p, "cmp").encode()); os.close(fd)
print("creds:", len(list(d.glob("p*.creds"))))
EOF
  )
  rm -f "$SEC/creds/operator.store"
  admin_sql -c "INSERT INTO zebridge_user_tenants (principal, tenant_id) SELECT 'p' || lpad(i::text, 3, '0'), 'cmp' FROM generate_series(0, $N - 1) i ON CONFLICT DO NOTHING"
  (umask 077; grep -E '^(SB_ADMIN_URL|ADMIN_DATABASE_URL)=' "$DB_ENV_FILE" > "$SEC/admin.env")
  ssh_h 'rm -rf ~/zbcmp/cl/creds && mkdir -p -m 700 ~/zbcmp/cl/creds'
  scp -q "$SEC/admin.env" "$CMP_HOST:zbcmp/cl/admin.env" && rm -f "$SEC/admin.env"
  scp -q "$HERE/zb_cmp.py" "$CMP_HOST:zbcmp/cl/"
  (cd "$SEC/creds" && COPYFILE_DISABLE=1 tar --no-xattrs -czf - p*.creds) | ssh_h 'tar -C ~/zbcmp/cl/creds -xzf -'
  ssh_h 'chmod 600 ~/zbcmp/cl/admin.env ~/zbcmp/cl/creds/*; echo "creds on the host: $(ls ~/zbcmp/cl/creds | wc -l)"'
  ;;
run)
  shift
  ssh_h "cd ~/zbcmp/cl && ZB_CMP_LIB=\$(ls /usr/lib/*/libzbcore.so | head -1) ZB_CMP_CREDS=\$HOME/zbcmp/cl/creds ZB_CMP_ADMIN_ENV=\$HOME/zbcmp/cl/admin.env python3 zb_cmp.py $* 2>run.err; echo \"(client log: ~/zbcmp/cl/run.err, \$(grep -ciE 'error\\(|refused|rejected' run.err) error lines)\""
  ;;
tools)
  ssh_h 'set -e; v=$(dpkg-query -W -f "\${Version}" libzb); python3 -m venv ~/zbcmp/venv
    ~/zbcmp/venv/bin/pip install -q --disable-pip-version-check "zebridge==$v" nats-py nkeys msgpack
    echo "venv: zebridge $(~/zbcmp/venv/bin/python -c "import importlib.metadata as m; print(m.version(\"zebridge\"))"), nats-py, nkeys, msgpack"'
  ;;
ingress)
  N="${2:?N}" RATE="${3:?RATE}" SECS="${4:?SECONDS}"
  db_env
  scp -q "$ROOT/scripts/bench/ingress_load.py" "$CMP_HOST:zbcmp/cl/"
  before=$(psql "$ADMIN" -X -tA -c "SELECT count(*) FROM sensor_events" 2>&1 | redact)
  t0=$(date +%s)
  ssh_h "cd ~/zbcmp/cl && ~/zbcmp/venv/bin/python ingress_load.py --url nats://127.0.0.1:14222 --creds \$HOME/zbcmp/cl/creds/p000.creds --creds-dir \$HOME/zbcmp/cl/creds \
    --principals \$(seq -f 'p%03g' 0 $((N - 1)) | paste -sd, -) --tenant cmp --variant zb1 --workers $N --rate $RATE --seconds $SECS 2>ingress.err | tail -1"
  # Committed is PostgreSQL's own count, not the verdicts: wait until it stops rising.
  last=-1; while :; do now=$(psql "$ADMIN" -X -tA -c "SELECT count(*) FROM sensor_events" 2>&1 | redact); [ "$now" = "$last" ] && break; last=$now; sleep 3; done
  echo "committed: $((last - before)) rows, $(( (last - before) / ($(date +%s) - t0 - 3) )) rows/s over the whole run, offered $RATE/s for $SECS s"
  ;;
enroll)
  N="${2:?N}"
  db_env
  (umask 077; psql "$ADMIN" -X -tA -c "SELECT zebridge_invite('s' || lpad(i::text, 3, '0'), 'cmp', interval '1 day') FROM generate_series(0, $N - 1) i" 2>/dev/null > "$SEC/codes.txt")
  [ "$(wc -l < "$SEC/codes.txt" | tr -d ' ')" = "$N" ] || { echo "invites: expected $N, got $(wc -l < "$SEC/codes.txt")" >&2; rm -f "$SEC/codes.txt"; exit 1; }
  scp -q "$SEC/codes.txt" "$ROOT/storm/storm.py" "$CMP_HOST:zbcmp/cl/" && rm -f "$SEC/codes.txt"
  ssh_h 'cd ~/zbcmp/cl && chmod 600 codes.txt && ~/zbcmp/venv/bin/python storm.py enroll codes.txt ids --bridge http://127.0.0.1:27534 2>enroll.err | tail -1; rm -f codes.txt; chmod 700 ids'
  ;;
storm)
  N="${2:?N}" M="${3:-30}"
  ssh_h "cd ~/zbcmp/cl && ~/zbcmp/venv/bin/python storm.py run ids $N --bridge http://127.0.0.1:27534 --nats nats://127.0.0.1:14222 --tenant cmp --markers $M 2>storm.err"
  ;;
down)
  ssh_h 'for p in ~/zbcmp/zb/*.pid; do [ -f "$p" ] && kill "$(cat "$p")" 2>/dev/null && echo "stopped $(basename "$p" .pid)"; rm -f "$p"; done; true'
  ;;
clean)
  "$0" down
  ssh_h 'sudo rm -rf ~/zbcmp/zb ~/zbcmp/cl ~/zbcmp/venv /tmp/zbcmp-* /tmp/storm-*; rmdir ~/zbcmp 2>/dev/null; ls ~/zbcmp 2>/dev/null || true'
  rm -rf "$SEC/creds"
  ;;
*) sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
