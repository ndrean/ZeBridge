#!/usr/bin/env bash
# ZeBridge operator/JWT bootstrap (NOTES.md endgame: the signing-key endpoint).
#
# Creates, idempotently-ish (wipe STORE to redo): an operator, the SYS account,
# the ZEBRIDGE account (JetStream enabled), one SCOPED signing key carrying the
# whole client permission shape as a template ({{name()}} / {{tag(tenant)}}),
# one service signing key, the bridge user (service) and the demo principals
# (client role, tenant tag). Outputs .creds files and a mem-resolver conf
# fragment — the artifacts are plain files: IaC-portable, container or host.
#
# ⚠️ nsc lowercases tags, and templates substitute literally — which is why the
# runtime stream names are `CDC_<tenant>` with the tenant AS-IS (lowercase),
# never upper-cased. The bridge's reconciler and the conf were changed together.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export NSC_HOME="$ROOT/scripts/native/nsc-store"          # keys + JWTs live here
export XDG_DATA_HOME="$NSC_HOME/data"
export XDG_CONFIG_HOME="$NSC_HOME/config"
CREDS="$ROOT/scripts/native/creds"
mkdir -p "$CREDS"

nsc add operator --name ZeBridgeOp --sys 2>/dev/null || echo "operator exists"
nsc add account ZEBRIDGE 2>/dev/null || echo "account exists"

# JetStream limits: the account must carry them or every JS call is refused.
nsc edit account ZEBRIDGE --js-mem-storage -1 --js-disk-storage -1 \
    --js-streams -1 --js-consumer -1 >/dev/null

# ── the SERVICE signing key: the bridge (and admin tooling) ──────────────────
SK_SERVICE=$(nsc edit account ZEBRIDGE --sk generate 2>&1 | grep -o 'A[A-Z0-9]\{55\}' | tail -1)
nsc edit signing-key --account ZEBRIDGE --sk "$SK_SERVICE" --role service \
    --allow-pub ">" --allow-sub ">" >/dev/null
echo "service signing key: $SK_SERVICE"

# ── the CLIENT signing key: the WHOLE per-principal grant block, ONCE ────────
SK_CLIENT=$(nsc edit account ZEBRIDGE --sk generate 2>&1 | grep -o 'A[A-Z0-9]\{55\}' | tail -1)
nsc edit signing-key --account ZEBRIDGE --sk "$SK_CLIENT" --role client \
    --allow-pub "mutation.{{name()}}.>" \
    --allow-pub "\$JS.API.INFO" \
    --allow-pub "\$JS.API.CONSUMER.CREATE.CDC_{{tag(tenant)}}" \
    --allow-pub "\$JS.API.CONSUMER.CREATE.CDC_{{tag(tenant)}}.>" \
    --allow-pub "\$JS.API.CONSUMER.CREATE.CDC_PUBLIC" \
    --allow-pub "\$JS.API.CONSUMER.CREATE.CDC_PUBLIC.>" \
    --allow-pub "\$JS.API.CONSUMER.CREATE.KV_schemas" \
    --allow-pub "\$JS.API.CONSUMER.CREATE.KV_schemas.>" \
    --allow-pub "\$JS.API.CONSUMER.CREATE.KV_generations" \
    --allow-pub "\$JS.API.CONSUMER.CREATE.KV_generations.>" \
    --allow-pub "\$JS.API.CONSUMER.INFO.CDC_{{tag(tenant)}}.>" \
    --allow-pub "\$JS.API.CONSUMER.INFO.CDC_PUBLIC.>" \
    --allow-pub "\$JS.API.CONSUMER.INFO.KV_schemas.>" \
    --allow-pub "\$JS.API.CONSUMER.INFO.KV_generations.>" \
    --allow-pub "\$JS.API.CONSUMER.MSG.NEXT.CDC_{{tag(tenant)}}.>" \
    --allow-pub "\$JS.API.CONSUMER.MSG.NEXT.CDC_PUBLIC.>" \
    --allow-pub "\$JS.API.CONSUMER.MSG.NEXT.KV_schemas.>" \
    --allow-pub "\$JS.API.CONSUMER.MSG.NEXT.KV_generations.>" \
    --allow-pub "\$JS.API.STREAM.INFO.CDC_{{tag(tenant)}}" \
    --allow-pub "\$JS.API.STREAM.INFO.CDC_PUBLIC" \
    --allow-pub "\$JS.API.STREAM.INFO.KV_schemas" \
    --allow-pub "\$JS.API.STREAM.INFO.KV_generations" \
    --allow-pub "\$JS.API.STREAM.INFO.KV_tenants" \
    --allow-pub "\$JS.API.STREAM.MSG.GET.KV_schemas" \
    --allow-pub "\$JS.API.DIRECT.GET.KV_schemas.>" \
    --allow-pub "\$JS.API.DIRECT.GET.KV_generations.\$KV.generations.{{tag(tenant)}}.>" \
    --allow-pub "\$JS.API.DIRECT.GET.KV_generations.\$KV.generations._default.>" \
    --allow-pub "\$JS.API.DIRECT.GET.KV_tenants.\$KV.tenants.{{name()}}" \
    --allow-pub "\$JS.API.STREAM.INFO.OBJ_gen-{{tag(tenant)}}" \
    --allow-pub "\$JS.API.DIRECT.GET.OBJ_gen-{{tag(tenant)}}.>" \
    --allow-pub "\$JS.API.STREAM.MSG.GET.OBJ_gen-{{tag(tenant)}}" \
    --allow-pub "\$JS.API.CONSUMER.CREATE.OBJ_gen-{{tag(tenant)}}" \
    --allow-pub "\$JS.API.CONSUMER.CREATE.OBJ_gen-{{tag(tenant)}}.>" \
    --allow-pub "\$JS.API.CONSUMER.INFO.OBJ_gen-{{tag(tenant)}}.>" \
    --allow-pub "\$JS.API.CONSUMER.MSG.NEXT.OBJ_gen-{{tag(tenant)}}.>" \
    --allow-pub "\$JS.API.STREAM.INFO.OBJ_gen-_default" \
    --allow-pub "\$JS.API.DIRECT.GET.OBJ_gen-_default.>" \
    --allow-pub "\$JS.API.STREAM.MSG.GET.OBJ_gen-_default" \
    --allow-pub "\$JS.API.CONSUMER.CREATE.OBJ_gen-_default" \
    --allow-pub "\$JS.API.CONSUMER.CREATE.OBJ_gen-_default.>" \
    --allow-pub "\$JS.API.CONSUMER.INFO.OBJ_gen-_default.>" \
    --allow-pub "\$JS.API.CONSUMER.MSG.NEXT.OBJ_gen-_default.>" \
    --allow-pub "\$JS.ACK.>" \
    --allow-sub "mutation_ack.{{name()}}.>" \
    --allow-sub "cdc.{{tag(tenant)}}.>" \
    --allow-sub "cdc._default.>" \
    --allow-sub "\$KV.schemas.>" \
    --allow-sub "\$KV.generations.>" \
    --allow-sub "_INBOX.>" >/dev/null
echo "client signing key:  $SK_CLIENT"

# ── users: one mint per principal — THIS is the whole onboarding now ─────────
nsc add user --account ZEBRIDGE --name bridge -K "$SK_SERVICE" 2>/dev/null || echo "bridge user exists"
for spec in alice:acme bob:globex mary:globex nina:tango omar:kilo; do
  u="${spec%%:*}"; t="${spec##*:}"
  nsc add user --account ZEBRIDGE --name "$u" -K "$SK_CLIENT" --tag "tenant:$t" 2>/dev/null || echo "$u exists"
done

# ── the auditor: zbdoctor's own principal, read-only by construction ────────
#
# ⚠️ Minted WITHOUT -K, i.e. signed by the account IDENTITY key. Both signing
# keys above are ROLE-SCOPED, and a scoped signing key REPLACES whatever
# permissions the user JWT carries — so a "narrow" auditor minted under one
# would silently inherit its template (`>` for the service key) and be able to
# read every tenant's data. Signed by the identity key, the user's own
# permissions are what the server enforces.
#
# The grant set is the MEASURED minimum for scripts/zbdoctor.py (NOTES §10y),
# narrowed by removing grants until a gate went red:
#   INFO          the account probe every JetStream client issues first
#   STREAM.NAMES  the topology gate (does CDC_<tenant> exist?)
#   STREAM.INFO.> stream state, and the object-store meta lookups
#   DIRECT.GET.>  per-key KV reads and chain objects
#   _INBOX.>      request/reply replies — the grant everyone forgets
#
# ⚠️ NOT granted: CONSUMER.CREATE. A credential that can create a consumer can
# READ that stream, which is precisely what an auditor must not do. `nats kv ls`
# needs it, so zbdoctor asks "does key X exist?" per key instead of listing —
# that design choice is what keeps this grant set this small.
nsc add user --account ZEBRIDGE --name zbdoctor \
    --allow-pub "\$JS.API.INFO" \
    --allow-pub "\$JS.API.STREAM.NAMES" \
    --allow-pub "\$JS.API.STREAM.INFO.>" \
    --allow-pub "\$JS.API.DIRECT.GET.>" \
    --allow-sub "_INBOX.>" 2>/dev/null || echo "zbdoctor exists"

for u in bridge alice bob mary nina omar zbdoctor; do
  nsc generate creds --account ZEBRIDGE --name "$u" > "$CREDS/$u.creds"
done
chmod 600 "$CREDS"/*.creds

# ── the server side: operator + preloaded accounts, one fragment ─────────────
nsc generate config --mem-resolver --config-file "$ROOT/scripts/native/resolver.conf" --force
echo; echo "── store ──"; nsc describe account ZEBRIDGE | head -30
echo "── creds ──"; ls -la "$CREDS"
