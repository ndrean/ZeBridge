#!/bin/bash
# The PowerSync side, on the comparison host. It needs Docker there; the images are pulled on first use.
#   powersync.sh roles           the database side (ps_roles.py), after `db.sh init` and `db.sh table`
#   powersync.sh up              the client's files to ~/zbcmp/psc (npm install in node:22), then the three
#                                containers (ps_service.py), then a check that the tenant policy applies
#   powersync.sh run PHASE ...   one phase of client/ps_cmp.mjs in a fresh node:22 container (4 GB cap), with a
#                                watchdog that kills it if the host's available memory falls under 1.5 GB:
#                                seed N | roundtrip S | burst N | writes P M
#   powersync.sh crowd sync N MARKERS | crowd writes N RATE SECONDS
#                                the crowd (client/ps_crowd.mjs), in the same container and watchdog: N stream
#                                readers, or N apps uploading to PostgREST (rows committed counted before and after)
#   powersync.sh down            remove the containers
#   powersync.sh clean           down, remove ~/zbcmp/ps, ~/zbcmp/psc and the images
# Each `run` starts with empty replicas: the container's /tmp goes with it.
# Prints no secret.
. "$(dirname "$0")/lib.sh"

case "${1:-}" in
roles)
  db_env
  CMP_ADMIN_URL="$ADMIN" python3 "$HERE/ps_roles.py"
  ;;
up)
  for f in jwt.key admin.uri replication.uri api.uri store.pw; do [ -s "$SEC/$f" ] || { echo "no secrets/$f: run powersync.sh roles first" >&2; exit 2; }; done
  # Every `up` starts a fresh service and storage: the last one's slot would hold WAL, and `db.sh table` recreates
  # the tables, which takes them out of the publication and drops their grants.
  db_env
  psql "$ADMIN" -X -tA -c "SELECT 'dropped old slot ' || slot_name || pg_drop_replication_slot(slot_name)::text FROM pg_replication_slots WHERE slot_name LIKE 'powersync%' AND NOT active" 2>&1 | redact | grep -v '^$' || true
  admin_sql -c "ALTER PUBLICATION powersync SET TABLE public.storm_items, public.sensor_events" -c "GRANT SELECT ON public.storm_items, public.sensor_events TO powersync_role"
  ssh_h 'mkdir -p -m 700 ~/zbcmp/psc/secrets'
  scp -q "$HERE/client/ps_cmp.mjs" "$HERE/client/ps_crowd.mjs" "$HERE/client/package.json" "$CMP_HOST:zbcmp/psc/"
  scp -q "$SEC/jwt.key" "$SEC/admin.uri" "$CMP_HOST:zbcmp/psc/secrets/"
  ssh_h 'chmod 600 ~/zbcmp/psc/secrets/*; sudo docker run --rm --network host -v $HOME/zbcmp/psc:/w -w /w node:22 bash -c "npm install --no-audit --no-fund --loglevel=error >/dev/null 2>&1; echo npm install rc=\$?"' 2>&1 | grep "npm install"
  CMP_HOST="$CMP_HOST" python3 "$HERE/ps_service.py"
  # The tenant policy must apply: a mapped principal writes, an unmapped one is refused (42501).
  # The service serves nothing until its first checkpoint, which needs the replication stream to move: the policy
  # check's accepted insert moves it. Then wait until one reader gets a stream, so no timed run includes the start.
  ssh_h 'for i in $(seq 120); do
      code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 2 -X POST http://127.0.0.1:18080/sync/stream \
        -H "content-type: application/json" -H "accept: application/x-ndjson" \
        -H "authorization: Token $(python3 -c "import base64,hashlib,hmac,json,time,sys; k=open(sys.argv[1]).read().strip(); b=lambda x: base64.urlsafe_b64encode(x).rstrip(b\"=\").decode(); n=int(time.time()); h=b(json.dumps({\"alg\":\"HS256\",\"kid\":\"cmp-key\",\"typ\":\"JWT\"}).encode())+\".\"+b(json.dumps({\"sub\":\"ready\",\"aud\":\"powersync-cmp\",\"iat\":n,\"exp\":n+600}).encode()); print(h+\".\"+b(hmac.new(base64.urlsafe_b64decode(k+\"=\"*(-len(k)%4)),h.encode(),hashlib.sha256).digest()))" ~/zbcmp/psc/secrets/jwt.key)" \
        -d "{\"buckets\":[],\"include_checksum\":true,\"raw_data\":true,\"client_id\":\"ready\"}" || true)
      [ "$code" = 200 ] && { echo "sync stream ready after ${i} s"; exit 0; }; sleep 1; done; echo "sync stream not ready after 120 s (last HTTP $code)"; exit 1' &
  READY=$!
  ssh_h 'for p in p000 nobody; do printf "policy check, x-principal %s: " $p; curl -s -o /dev/null -w "%{http_code}\n" -X POST http://127.0.0.1:13000/storm_items -H "content-type: application/json" -H "prefer: return=minimal" -H "x-principal: $p" -d "[{\"id\":\"policy-$p\",\"tenant_id\":\"cmp\",\"note\":\"check\"}]"; done; echo "(expected: 201, then 401)"'
  wait "$READY"
  rows=$(psql "$ADMIN" -X -tA -c "SELECT count(*) FROM storm_items" 2>&1 | redact)
  ssh_h "docker run --rm --network host -v \$HOME/zbcmp/psc:/w -w /w node:22 node ps_crowd.mjs ready $rows"
  ;;
run|crowd)
  script=ps_cmp.mjs; [ "$1" = crowd ] && script=ps_crowd.mjs
  shift
  if [ "$script" = ps_crowd.mjs ] && [ "${1:-}" = writes ]; then db_env; before=$(psql "$ADMIN" -X -tA -c "SELECT count(*) FROM sensor_events" 2>&1 | redact); fi
  ssh_h "bash -s -- $script $*" <<'REMOTE'
(while sleep 2; do a=$(awk '/MemAvailable/{print int($2/1024)}' /proc/meminfo); echo "$a" >> /tmp/zbcmp-mem.log
  [ "$a" -lt 1500 ] && { sudo docker kill zbcmp-psclient >/dev/null 2>&1; echo "WATCHDOG: killed the client at $a MB available"; }
  sudo docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' zbcmp-powersync zbcmp-psclient 2>/dev/null | tr -d '%' >> /tmp/zbcmp-cpu.log; done) &
W=$!
script=$1; shift
sudo docker run --rm --name zbcmp-psclient --network host --memory 4g --ulimit nofile=65536 -v "$HOME/zbcmp/psc:/w" -w /w node:22 node "$script" "$@" 2>&1 \
  | grep -vE "Could not apply checkpoint|^ +at " | uniq -c | sed -E 's/^ +1 //'
kill $W
echo "(lowest available memory: $(sort -n /tmp/zbcmp-mem.log | head -1) MB; peak CPU in % of one core: the service $(awk '$1=="zbcmp-powersync"{print $2}' /tmp/zbcmp-cpu.log | sort -n | tail -1), the crowd process $(awk '$1=="zbcmp-psclient"{print $2}' /tmp/zbcmp-cpu.log | sort -n | tail -1))"; rm -f /tmp/zbcmp-mem.log /tmp/zbcmp-cpu.log
for c in zbcmp-powersync zbcmp-psstore zbcmp-postgrest; do sudo docker inspect $c --format "$c restarts={{.RestartCount}} oom={{.State.OOMKilled}}" 2>/dev/null; done
REMOTE
  if [ -n "${before:-}" ]; then
    last=-1; while :; do now=$(psql "$ADMIN" -X -tA -c "SELECT count(*) FROM sensor_events" 2>&1 | redact); [ "$now" = "$last" ] && break; last=$now; sleep 3; done
    echo "committed: $((last - before)) rows in sensor_events"
  fi
  ;;
down)
  ssh_h 'sudo docker rm -f zbcmp-powersync zbcmp-postgrest zbcmp-psstore zbcmp-psclient >/dev/null 2>&1; sudo docker ps -a --filter name=zbcmp- --format "{{.Names}}" | grep . || echo "no comparison container left"'
  ;;
clean)
  "$0" down
  ssh_h 'sudo docker rmi -f journeyapps/powersync-service:latest postgrest/postgrest:latest postgres:17 node:22 >/dev/null 2>&1; sudo rm -rf ~/zbcmp/ps ~/zbcmp/psc; rmdir ~/zbcmp 2>/dev/null; ls ~/zbcmp 2>/dev/null || true'
  ;;
*) sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
