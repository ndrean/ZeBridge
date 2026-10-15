#!/bin/sh
# Post-release smoke test, after every registry has the version (npm approved):
#   smoke/run.sh 0.2.1
# Installs the bridge from APT and each client from its registry, enrolls each with an
# invite, writes one row each, checks PostgreSQL has them and that each client receives a
# row inserted in PostgreSQL afterwards; checks the Maven and React Native packages carry
# libzb for every target. Leaves nothing behind.
set -u
[ $# -eq 1 ] || { echo "usage: $0 <version>"; exit 2; }
export ZB_VERSION="$1"
cd "$(dirname "$0")"
DC="docker compose -f compose.yml"
trap '$DC --profile clients down -v --remove-orphans >/dev/null 2>&1' EXIT

echo "▶ zebridge $ZB_VERSION: building (APT, npm, PyPI, pub.dev, Maven)"
$DC --profile clients build --pull -q || { echo "✗ build: an install failed (above)"; exit 1; }
$DC up -d --wait bridge || { $DC logs setup bridge | tail -40; echo "✗ the APT bridge did not start"; exit 1; }
$DC logs setup | grep "smoke setup"

failed=""
$DC run --rm packages || failed=" packages"

psql() { $DC exec -T postgres psql -U postgres -d app -v ON_ERROR_STOP=1 -tAc "$1"; }

# Each client: enroll, seed, write and get its verdict, say "ready"; then a row inserted
# here must reach it, which only the live CDC tail can carry.
for c in node python dart; do
  cid=$($DC run -d "$c") || { failed="$failed $c"; continue; }
  i=0
  until docker logs "$cid" 2>&1 | grep -q '^ready'; do
    i=$((i + 1))
    if [ "$i" -gt 120 ] || [ "$(docker inspect -f '{{.State.Running}}' "$cid")" != true ]; then break; fi
    sleep 1
  done
  psql "INSERT INTO smoke_items (id, tenant_id, note) VALUES ('live-$c', 'smoke', 'inserted by run.sh')" >/dev/null
  code=$(docker wait "$cid")
  docker logs "$cid" 2>&1 | grep -E '^(✓|✗)|rror' || docker logs "$cid" 2>&1 | tail -20
  docker rm "$cid" >/dev/null
  [ "$code" = 0 ] || failed="$failed $c"
done

rows=$(psql "SELECT string_agg(id, ' ' ORDER BY id) FROM smoke_items WHERE tenant_id = 'smoke'")
echo "PostgreSQL has: $rows"
for c in node python dart; do
  case " $rows " in *" from-$c "*) ;; *) case "$failed" in *"$c"*) ;; *) failed="$failed $c(row)";; esac;; esac
done
if [ -n "$failed" ]; then
  $DC logs bridge | tail -30
  echo "✗ smoke $ZB_VERSION failed:$failed"; exit 1
fi
echo "✓ smoke $ZB_VERSION: APT bridge, Node, Python, Dart (Linux); Maven and React Native packages. Phones: by hand (README)."
