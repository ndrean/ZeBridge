#!/usr/bin/env python3
"""Merge-on-stale over a jsonb column — the CRDT ladder's top rung, measured.

Row-level LWW guarantees replicas CONVERGE; it cannot guarantee both writers'
INTENTS survive. This scenario stages the difference on `test_types.metadata`
(jsonb), no schema changes, no protocol changes — the whole experiment is
app-level, which is the point (§10cq's ladder: sparse columns → PG-function
verbs → CRDT-shaped merge; the sync layer stays row-dumb).

Leg 1, BLIND REPLACE (the motivation): two writers each replace the whole doc
with only their own key. LWW picks a winner; the loser's key is GONE. This leg
asserts the loss HAPPENS — it is the documented price of stateful overwrite,
not a bug.

Leg 2, MAP-OF-LWW-REGISTERS with merge-on-stale: the doc is {key: {v, t, w}}.
Each writer's round merges its pending register into the last doc it has SEEN
(via its own CDC tail — the echo is the merge input, exactly as a real client
would), stamps a fresh row version, writes. On `stale`: wait for the winner's
echo, merge into IT, write again. Registers win per-KEY by (t, w) — writer
identity breaks ties. Asserted: the final PostgreSQL doc holds EVERY key both
writers ever wrote (zero lost operations), the contested key holds its per-key
winner, and the re-mutate rounds are BOUNDED (convergence terminates — merge
is what turns the write war into a settlement).

⚠️ Both writers share one principal (same tenant, same row) but carry distinct
client_ids — the row-level tiebreak — while the REGISTER tiebreak is the app's
own `w` field. Two conflict layers, deliberately: the row race decides who
must merge; the register race decides which VALUE survives a same-key write.

Usage:  python scripts/scenarios/crdt.py [table]   (NATS_CREDS=<client>.creds or ZB_PRINCIPAL)
"""

import asyncio
import json
import sys
import uuid
from datetime import datetime, timezone

import msgpack
import zb

TABLE = sys.argv[1] if len(sys.argv) > 1 else "test_types"
ROUNDS = 8
VERDICT_WAIT_S = 10.0
ECHO_WAIT_S = 15.0


def iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def now_iso():
    return iso(datetime.now(timezone.utc))


def merge(a: dict, b: dict) -> dict:
    """Per-key LWW-register union: higher (t, w) wins the key; every key survives."""
    out = dict(a)
    for k, reg in b.items():
        cur = out.get(k)
        if cur is None or (reg.get("t", ""), reg.get("w", "")) > (cur.get("t", ""), cur.get("w", "")):
            out[k] = reg
    return out


async def main():
    failed = 0
    who = zb.require_principal()
    version_col = zb.require_rules(TABLE, "version")["version"]
    tenant = zb.psql(
        f"SELECT tenant_id FROM zebridge_user_tenants WHERE principal='{who}' LIMIT 1"
    ).strip()
    required = [
        c for c in zb.psql(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{TABLE}' AND is_nullable='NO' AND column_default IS NULL"
        ).splitlines() if c
    ]

    nc = await zb.connect_as(who)
    js = nc.jetstream()
    verdicts: dict[str, dict] = {}
    ack_sub = await zb.subscribe(nc, f"{zb.TOPOLOGY['subjects']['mutation_ack_prefix']}.{who}.*")

    # each writer's view of the row — fed by the CDC echo, the way a client's is
    seen: dict[str, dict] = {"doc": {}, "version": ""}
    cdc_sub = await zb.subscribe(nc, f"{zb.TOPOLOGY['subjects']['cdc_prefix']}.{tenant}.{TABLE}.>")
    uid = str(uuid.uuid4())

    async def collect_acks():
        async for m in ack_sub.messages:
            try:
                verdicts[m.subject.rsplit(".", 1)[-1]] = zb.decode(m.data)
            except Exception:  # noqa: BLE001
                pass

    async def collect_cdc():
        async for m in cdc_sub.messages:
            try:
                payload = zb.decode(m.data)
            except Exception:  # noqa: BLE001
                continue
            for ev in payload if isinstance(payload, list) else [payload]:
                d = ev.get("data") or {} if isinstance(ev, dict) else {}
                if d.get("uid") != uid:
                    continue
                v = d.get(version_col) or ""
                if v > seen["version"]:
                    seen["version"] = v
                    raw = d.get("metadata")
                    try:
                        seen["doc"] = json.loads(raw) if isinstance(raw, str) else (raw or {})
                    except Exception:  # noqa: BLE001
                        seen["doc"] = {}

    tasks = [asyncio.create_task(collect_acks()), asyncio.create_task(collect_cdc())]
    await asyncio.sleep(0.5)

    async def write(client_id: str, doc: dict, version: str) -> dict:
        data = {"uid": uid, "metadata": doc, "some_text": f"crdt {client_id}", "inserted_at": version}
        for c in required:
            data.setdefault(c, tenant if c == "tenant_id" else version)
        msg_id = f"crdt-{uuid.uuid4().hex[:12]}"
        await js.publish(
            zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, TABLE, "insert"),
            msgpack.packb({"key": {"uid": uid}, "data": data, "version": version,
                           "client_id": client_id}),
            headers={"Nats-Msg-Id": msg_id},
        )
        deadline = asyncio.get_event_loop().time() + VERDICT_WAIT_S
        while msg_id not in verdicts and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.1)
        return verdicts.get(msg_id) or {}

    def stored_doc() -> dict:
        raw = zb.psql(f"SELECT metadata FROM public.{TABLE} WHERE uid='{uid}'").strip()
        return json.loads(raw) if raw else {}

    # ── leg 1: blind replace — the write war's single round, and its price ────
    v1 = await write("c-alpha", {"alpha": {"v": 1}}, now_iso())
    await asyncio.sleep(0.3)
    v2 = await write("c-beta", {"beta": {"v": 2}}, now_iso())
    await asyncio.sleep(1.0)
    doc = stored_doc()
    if v1.get("status") == "accepted" and v2.get("status") == "accepted" \
            and "beta" in doc and "alpha" not in doc:
        zb.ok("blind full-doc replace: both writes ACCEPTED, and alpha's key is GONE — "
              "row-level LWW converged perfectly and still lost an intent. The price, demonstrated")
    else:
        zb.bad(f"the blind leg should lose alpha's key to beta's replace: {doc} "
               f"(v1={v1.get('status')}, v2={v2.get('status')})")
        failed += 1

    # ── leg 2: merge-on-stale — every key survives, the war becomes a settlement ─
    remutates = {"c-red": 0, "c-blue": 0}

    async def writer(client_id: str, keys: list[str]):
        # STATE-based semantics, the part the first run taught: a writer ships the
        # union of ALL its own registers on every write, not just the newest delta.
        # Merging only seen+delta lost every un-echoed key to the other writer's
        # accepted overwrites — a write war with zero stales, because the row
        # version was always fresh while the echo lagged. Your state rides along,
        # so a laggy echo costs nothing.
        mine: dict = {}
        for i, key in enumerate(keys):
            mine[key] = {"v": f"{client_id}:{i}", "t": now_iso(), "w": client_id}
            for _attempt in range(6):
                base_version = seen["version"]
                doc_out = merge(dict(seen["doc"]), mine)
                verdict = await write(client_id, doc_out, now_iso())
                if verdict.get("status") == "accepted":
                    break
                if verdict.get("status") != "stale":
                    zb.bad(f"{client_id}: unexpected verdict {verdict}")
                    return False
                # the winner's echo is the merge input — wait for it, then retry
                remutates[client_id] += 1
                deadline = asyncio.get_event_loop().time() + ECHO_WAIT_S
                while seen["version"] <= base_version and asyncio.get_event_loop().time() < deadline:
                    await asyncio.sleep(0.1)
            else:
                zb.bad(f"{client_id}: key {key} never landed after 6 attempts")
                return False
        # RECONCILE until the observed doc CONTAINS my state — the actual CRDT
        # convergence condition. Stopping at "accepted" is not enough: with fresh
        # clocks every overwrite is accepted, so merge-on-stale never fires, and
        # whoever writes LAST with a lagging echo erases the other unanswered
        # (measured: zero stales, all of red's keys gone). Merge is monotone —
        # registers only gain — so this loop reaches a fixed point.
        for _ in range(10):
            await asyncio.sleep(0.8)
            merged = merge(dict(seen["doc"]), mine)
            if merged == seen["doc"]:
                return True
            remutates[client_id] += 1
            await write(client_id, merged, now_iso())
        zb.bad(f"{client_id}: no fixed point after 10 reconcile rounds")
        return False

    red_keys = [f"red{i}" for i in range(ROUNDS)] + ["shared"]
    blue_keys = [f"blue{i}" for i in range(ROUNDS)] + ["shared"]
    ok_red, ok_blue = await asyncio.gather(writer("c-red", red_keys), writer("c-blue", blue_keys))
    if not (ok_red and ok_blue):
        failed += 1

    await asyncio.sleep(2.0)
    doc = stored_doc()
    want = set(red_keys) | set(blue_keys) | {"beta"}
    got = set(doc.keys())
    missing = want - got
    if missing:
        zb.bad(f"lost operations: {sorted(missing)} never made the final doc — the merge leaked")
        failed += 1
    else:
        zb.ok(f"zero lost operations: all {len(want)} keys from both writers survived "
              f"{sum(remutates.values())} stale-merge round(s) — the claim plain LWW cannot make")
    shared = doc.get("shared", {})
    if shared.get("w") in ("c-red", "c-blue"):
        zb.ok(f"the contested key settled by its own register tiebreak: shared → {shared.get('w')} "
              f"({shared.get('v')}) — per-KEY LWW inside a per-ROW LWW world")
    else:
        zb.bad(f"contested key in a strange state: {shared}")
        failed += 1
    if all(n <= 10 for n in remutates.values()):
        zb.ok(f"convergence TERMINATED: re-mutates bounded ({remutates}) — merge is what "
              "turns the write war into a settlement")
    else:
        zb.bad(f"unbounded ping-pong: {remutates}")
        failed += 1

    # teardown the product's way
    zb.psql(f"UPDATE public.{TABLE} SET deleted_at = now(), {version_col} = now() WHERE uid='{uid}'", quiet=True)
    for t in tasks:
        t.cancel()
    await nc.close()
    print("\nPASS" if not failed else f"\nFAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)
