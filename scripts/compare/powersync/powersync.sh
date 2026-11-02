#!/bin/bash
# The PowerSync side, on the comparison host. It needs Docker there; the images are pulled on first use.
#   powersync.sh roles           the database side (ps_roles.py), after `db.sh init` and `db.sh table`
#   powersync.sh up              the client's files to ~/zbcmp/psc (npm install in node:22), then the three
#                                containers (ps_service.py), then a check that the tenant policy applies
#   powersync.sh run PHASE ...   one phase of client/ps_cmp.mjs in a fresh node:22 container (2 GB cap), with a
#                                watchdog that kills it if the host's available memory falls under 1.5 GB:
#                                seed N | roundtrip S | burst N | writes P M
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
  ssh_h 'mkdir -p -m 700 ~/zbcmp/psc/secrets'
  scp -q "$HERE/client/ps_cmp.mjs" "$HERE/client/package.json" "$CMP_HOST:zbcmp/psc/"
  scp -q "$SEC/jwt.key" "$SEC/admin.uri" "$CMP_HOST:zbcmp/psc/secrets/"
  ssh_h 'chmod 600 ~/zbcmp/psc/secrets/*; sudo docker run --rm --network host -v $HOME/zbcmp/psc:/w -w /w node:22 bash -c "npm install --no-audit --no-fund --loglevel=error >/dev/null 2>&1; echo npm install rc=\$?"' 2>&1 | grep "npm install"
  CMP_HOST="$CMP_HOST" python3 "$HERE/ps_service.py"
  # The tenant policy must apply: a mapped principal writes, an unmapped one is refused (42501).
  ssh_h 'for p in p00 nobody; do printf "policy check, x-principal %s: " $p; curl -s -o /dev/null -w "%{http_code}\n" -X POST http://127.0.0.1:13000/storm_items -H "content-type: application/json" -H "prefer: return=minimal" -H "x-principal: $p" -d "[{\"id\":\"policy-$p\",\"tenant_id\":\"cmp\",\"note\":\"check\"}]"; done; echo "(expected: 201, then 401)"'
  ;;
run)
  shift
  ssh_h "bash -s -- $*" <<'REMOTE'
(while sleep 2; do a=$(awk '/MemAvailable/{print int($2/1024)}' /proc/meminfo); echo "$a" >> /tmp/zbcmp-mem.log
  [ "$a" -lt 1500 ] && { sudo docker kill zbcmp-psclient >/dev/null 2>&1; echo "WATCHDOG: killed the client at $a MB available"; }; done) &
W=$!
sudo docker run --rm --name zbcmp-psclient --network host --memory 2g -v "$HOME/zbcmp/psc:/w" -w /w node:22 node ps_cmp.mjs "$@" 2>&1 \
  | grep -vE "Could not apply checkpoint|^ +at " | uniq -c | sed -E 's/^ +1 //'
kill $W
echo "(lowest available memory: $(sort -n /tmp/zbcmp-mem.log | head -1) MB)"; rm -f /tmp/zbcmp-mem.log
for c in zbcmp-powersync zbcmp-psstore zbcmp-postgrest; do sudo docker inspect $c --format "$c restarts={{.RestartCount}} oom={{.State.OOMKilled}}" 2>/dev/null; done
REMOTE
  ;;
down)
  ssh_h 'sudo docker rm -f zbcmp-powersync zbcmp-postgrest zbcmp-psstore zbcmp-psclient >/dev/null 2>&1; sudo docker ps -a --filter name=zbcmp- --format "{{.Names}}" | grep . || echo "no comparison container left"'
  ;;
clean)
  "$0" down
  ssh_h 'sudo docker rmi -f journeyapps/powersync-service:latest postgrest/postgrest:latest postgres:17 node:22 >/dev/null 2>&1; sudo rm -rf ~/zbcmp/ps ~/zbcmp/psc; rmdir ~/zbcmp 2>/dev/null; ls ~/zbcmp 2>/dev/null || true'
  ;;
*) sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
