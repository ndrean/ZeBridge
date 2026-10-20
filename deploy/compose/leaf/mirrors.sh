#!/bin/sh
# Shared by Docker and native Ansible. Discovery never authorizes deletion.
set -eu
if [ -z "${LEAF_JS_DOMAIN:-}" ]; then
  echo "forwarding leaf: no local mirrors"
  exec sleep 2147483647
fi
: "${LEAF_NAME:?set the reserved leaf namespace}"
: "${LEAF_TENANTS:?an explicit tenant list is required}"
: "${LEAF_HOST:?set the leaf TLS hostname}"
: "${HUB_JS_DOMAIN:?set the hub JetStream domain}"
KEEPER_CREDS=${KEEPER_CREDS:-/hub/keeper.creds}
for token in "$LEAF_NAME" "$LEAF_JS_DOMAIN" "$HUB_JS_DOMAIN" $LEAF_TENANTS; do
  case "$token" in ''|*[!A-Za-z0-9_-]*) echo "invalid leaf policy token"; exit 1;; esac
done
[ -s "$KEEPER_CREDS" ] || { echo "keeper credentials missing"; exit 1; }
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
trap 'exit 0' TERM INT
N() {
  if [ -n "${NATS_TLS_CA:-}" ]; then set -- --tlsca "$NATS_TLS_CA" "$@"; fi
  nats --timeout "${NATS_REQUEST_TIMEOUT:-5s}" --inbox-prefix "\$ZB.KEEPER.$LEAF_NAME" \
  -s "tls://$LEAF_HOST:4222" --creds "$KEEPER_CREDS" "$@"; }
info() { N request --raw "$1.STREAM.INFO.$2" '{}' 2>/dev/null; }
valid_info() { jq -e '.config.name != null and .error == null' >/dev/null; }
create() {
  name=$1
  if N request --raw "\$JS.API.STREAM.CREATE.$name" "$(cat "$work/config.json")" >"$work/create.json" 2>/dev/null &&
      valid_info <"$work/create.json"; then
    echo "local seeding stream created: $name"
  else
    echo "could not create $name; existing streams were retained" >&2
  fi
}
reconcile() {
  # Known explicit names, rather than a global, possibly partial stream listing.
  # An unavailable hub, a missing origin or a bad JSON response leaves caches intact.
  tenants=$(printf '%s\n' _default $LEAF_TENANTS | sort -u)
  printf '%s\n' "$tenants" | jq -R . | jq -s --arg api "\$JS.$HUB_JS_DOMAIN.API" \
    --arg deliver "\$ZB.MIRROR.$LEAF_NAME" \
    '[.[] | {name:("KV_generations_leaf_" + .), external:{api:$api, deliver:$deliver}}]' >"$work/sources.json" || return 1
  projections_ready=yes
  for tenant in $tenants; do
    if ! info "\$JS.$HUB_JS_DOMAIN.API" "KV_generations_leaf_$tenant" >"$work/origin.json" ||
        ! valid_info <"$work/origin.json"; then
      projections_ready=no
      echo "manifest projection unavailable for $tenant; retained local streams" >&2
    fi
    stream="OBJ_gen-$tenant"
    if ! info "\$JS.$HUB_JS_DOMAIN.API" "$stream" >"$work/origin.json" ||
        ! valid_info <"$work/origin.json"; then
      continue
    fi
    if ! info '$JS.API' "$stream" >"$work/local.json"; then continue; fi
    if jq -e '.error.err_code == 10059' "$work/local.json" >/dev/null; then
      jq --arg name "$stream" --arg api "\$JS.$HUB_JS_DOMAIN.API" --arg deliver "\$ZB.MIRROR.$LEAF_NAME" \
        '.config | del(.subjects,.sources,.mirror,.metadata,.consumer_limits)
         | .name=$name | .mirror_direct=false
         | .mirror={name:$name,external:{api:$api,deliver:$deliver}}' \
         "$work/origin.json" >"$work/config.json" || return 1
      create "$stream"
    elif ! jq -e --arg name "$stream" --arg api "\$JS.$HUB_JS_DOMAIN.API" --arg deliver "\$ZB.MIRROR.$LEAF_NAME" \
        '.error == null and .config.mirror.name == $name and .config.mirror.external.api == $api
         and .config.mirror.external.deliver == $deliver and .config.mirror_direct == false' "$work/local.json" >/dev/null; then
      echo "$stream has an unexpected configuration; explicit cache migration is required" >&2
      return 1
    fi
  done
  if [ "$projections_ready" = yes ]; then
    if ! info '$JS.API' KV_generations >"$work/local.json"; then return; fi
    if jq -e '.error.err_code == 10059' "$work/local.json" >/dev/null; then
      jq -n --slurpfile sources "$work/sources.json" \
        '{name:"KV_generations",subjects:["$KV.generations.>"],storage:"file",allow_direct:true,
          max_bytes:67108864,max_msgs_per_subject:1,allow_rollup_hdrs:true,sources:$sources[0]}' >"$work/config.json" || return 1
      create KV_generations
    elif ! jq -e --slurpfile sources "$work/sources.json" \
        '.error == null and .config.mirror == null and .config.max_bytes > 0 and .config.max_bytes <= 67108864
         and ([.config.sources[].name] | sort) == ([$sources[0][].name] | sort)
         and all(.config.sources[]; .external == $sources[0][0].external)' "$work/local.json" >/dev/null; then
      echo "KV_generations is a legacy or differently scoped cache; migrate it explicitly" >&2
      return 1
    fi
  fi
}
while :; do
  reconcile || echo "mirror reconciliation incomplete; no stream deleted" >&2
  [ "${ZB_MIRROR_ONCE:-0}" = 1 ] && break
  sleep 30 & wait $!
done
