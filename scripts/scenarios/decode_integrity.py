#!/usr/bin/env python3
"""Row values survive the zero-copy decode path, in both CDC and snapshots.

2026-08-20: `decodeTuple`/`decodeBinColumnData` (src/pgoutput.zig) stopped copying a
column's TEXT/VARCHAR/BPCHAR/JSON/BYTEA/JSONB/NUMERIC/enum-passthrough bytes out of the
WAL or COPY-binary buffer and started aliasing them directly, on the reasoning that the
buffer and the decoded value share one arena and nothing resets it until this row's
value has already been consumed — packed into the CDC ring buffer
(event_processor.zig), or encoded into a snapshot chunk (pg_copy_binary.zig). Zig unit
tests confirmed the two call sites this was actually changed for, under
`std.testing.allocator` (which panics on a double-free), but that only proves the
*reasoning* is internally consistent — not that the *assumption* (this buffer, this
allocator, this lifetime) still holds against a live WAL stream and a live COPY, where
the arena resets on a schedule this test does not control.

A single narrow row cannot exercise the failure mode this guards against: the risk is
an alias that outlives what it should, which needs *many* rows decoded between one arena
reset and the next — an aliasing bug shows up as row N reading back as row N+1's bytes
(or garbage from after the arena was reused), not as an error anywhere. So this seeds N
rows in ONE transaction, wide enough that the WAL-message arena has to grow mid-batch,
each carrying `idx` baked into every variable-length value so a misattributed row is
visibly wrong rather than accidentally identical — and matches by primary key, not by
delivery order, so a reordering bug cannot hide as a values bug or vice versa.

Two acts, same fixture:

  1. CDC — subscribe to `cdc.>`, run the seed INSERT, and check every event's `data`
     against PostgreSQL.
  2. snapshot — request a snapshot, replay its chunks, and check the same way.

NUMERIC is compared by decimal value, not by string: pg_copy_binary.zig pads a snapshot
NUMERIC to its declared scale to match PostgreSQL's own `::text` (see `padNumeric`'s
doc comment), but the CDC path does not, so a byte-string comparison would fail on a
padding difference that is not a bug. Decimal equality still catches a wrong value.

⚠️ `decode_fixture` is a table this script invents. Its catalogue row is written
BEFORE the probe bridge starts: the bridge derives the public set from
`zebridge_catalogue` at boot and reconciles CDC_PUBLIC's subject filter itself, so
the row IS the registration — the `nats stream edit` half this scenario used to do
by hand (and once lost several hours to, when it was skipped and every publish
timed out unacked) is now the bridge's own boot step, and check 0 asserts it
happened rather than performing it.

Usage:  python scripts/scenarios/decode_integrity.py [row_count]

Seeds and drops its own `decode_fixture` table and its NATS subject registration. Needs
DATABASE_URL (it starts a bridge).
"""

import asyncio
import decimal
import json
import os
import pathlib
import subprocess
import sys

import zb
from nats.js.api import ConsumerConfig, DeliverPolicy

TABLE = "decode_fixture"
KIND_TYPE = "decode_fixture_kind"
PUB = os.environ.get("BRIDGE_CDC_PUBLICATION", "my_pub")
SCRATCH = pathlib.Path(os.environ.get("TMPDIR", "/tmp"))
CDC_PUBLIC_STREAM = "CDC_PUBLIC"
FIXTURE_SUBJECT = f"cdc.{TABLE}.>"


# Comfortably past one page, so a run of these forces the WAL-message arena
# (bridge.zig's `arena.reset(.retain_capacity)`, once per WAL message — not per row) to
# grow mid-transaction rather than settle into one steady allocation reused untouched.
LABEL_BYTES = 2048


def seed(n: int):
    """One transaction, N rows, every variable-length column carrying `idx`."""
    zb.psql(
        f"DROP TABLE IF EXISTS public.{TABLE}; "
        f"DROP TYPE IF EXISTS {KIND_TYPE}; "
        f"CREATE TYPE {KIND_TYPE} AS ENUM ('alpha', 'beta', 'gamma'); "
        f"CREATE TABLE public.{TABLE} ("
        "  id uuid PRIMARY KEY, idx int NOT NULL, label text NOT NULL, "
        "  doc jsonb NOT NULL, amount numeric(20,8) NOT NULL, "
        f"  kind {KIND_TYPE} NOT NULL); "
        f"ALTER PUBLICATION {PUB} ADD TABLE public.{TABLE};",
        quiet=True,
    )
    zb.psql(
        f"INSERT INTO public.{TABLE} (id, idx, label, doc, amount, kind) "
        "SELECT md5(('decode-fixture-' || i)::text)::uuid, i, "
        f"  'row-' || i || '-' || repeat(chr(65 + (i % 26)), {LABEL_BYTES}), "
        "  jsonb_build_object('idx', i, 'nested', "
        "    jsonb_build_object('tag', 'v' || i, 'list', ARRAY[i, i + 1, i + 2])), "
        "  (i || '.1')::numeric, "
        f"  (ARRAY['alpha','beta','gamma'])[1 + (i % 3)]::{KIND_TYPE} "
        f"FROM generate_series(1, {n}) i;",
        quiet=True,
    )


def expected_rows() -> dict[int, dict]:
    """idx -> {label, doc, amount, kind}, straight from PostgreSQL."""
    out = {}
    for line in zb.psql(
        f"SELECT idx, label, doc::text, amount::text, kind FROM public.{TABLE} ORDER BY idx",
        quiet=True,
    ).splitlines():
        if not line:
            continue
        idx, label, doc, amount, kind = line.split("|", 4)
        out[int(idx)] = {
            "label": label,
            "doc": json.loads(doc),
            "amount": decimal.Decimal(amount),
            "kind": kind,
        }
    return out


def check_rows(got: dict[int, dict], expected: dict[int, dict], source: str) -> int:
    """Compare decoded rows against PostgreSQL. Returns the failure count."""
    failed = 0
    missing = expected.keys() - got.keys()
    extra = got.keys() - expected.keys()
    if missing:
        zb.bad(f"{source}: {len(missing)} row(s) never arrived (e.g. idx={sorted(missing)[:5]})")
        failed += 1
    if extra:
        zb.bad(f"{source}: {len(extra)} unexpected row(s) (e.g. idx={sorted(extra)[:5]})")
        failed += 1

    wrong = []
    for idx in got.keys() & expected.keys():
        g, e = got[idx], expected[idx]
        if g["label"] != e["label"]:
            wrong.append((idx, "label", g["label"][:40], e["label"][:40]))
        elif g["doc"] != e["doc"]:
            wrong.append((idx, "doc", g["doc"], e["doc"]))
        elif g["amount"] != e["amount"]:
            wrong.append((idx, "amount", g["amount"], e["amount"]))
        elif g["kind"] != e["kind"]:
            wrong.append((idx, "kind", g["kind"], e["kind"]))

    if wrong:
        failed += 1
        zb.bad(f"{source}: {len(wrong)} row(s) decoded wrong — the signature of an alias "
               "outliving the buffer it points into")
        for idx, col, got_v, exp_v in wrong[:5]:
            print(f"    idx={idx} {col}: got {got_v!r}, expected {exp_v!r}")
    elif not missing and not extra:
        zb.ok(f"{source}: all {len(expected)} rows decoded byte-for-byte correct "
              "(label, jsonb, numeric, enum)")
    return failed


async def collect_cdc(nc, n: int, timeout: float) -> dict[int, dict]:
    """Subscribe to cdc.>, seed, and collect every decode_fixture INSERT — batched or not."""
    sub = await nc.subscribe("cdc.>")
    got: dict[int, dict] = {}

    async def drain():
        async for m in sub.messages:
            payload = zb.decode(m.data)
            events = payload if isinstance(payload, list) else [payload]
            for ev in events:
                if ev.get("table") != TABLE or ev.get("operation") != "INSERT":
                    continue
                d = ev.get("data") or {}
                if "idx" not in d:
                    continue
                got[d["idx"]] = {
                    "label": d.get("label"),
                    "doc": json.loads(d["doc"]) if isinstance(d.get("doc"), str) else d.get("doc"),
                    "amount": decimal.Decimal(d["amount"]),
                    "kind": d.get("kind"),
                }

    task = asyncio.create_task(drain())
    await asyncio.sleep(0.5)
    seed(n)

    deadline = asyncio.get_event_loop().time() + timeout
    while len(got) < n and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.5)

    task.cancel()
    await sub.unsubscribe()
    return got


async def collect_snapshot(nc, columns: list[str]) -> dict[int, dict]:
    """Request a snapshot of TABLE and decode every chunk into idx -> row."""
    js = nc.jetstream()
    kv_snap = await js.key_value(zb.TOPOLOGY["kv"]["snapshots"])

    # ⚠️ Everything on the snapshot path is tenant-scoped since §1.12 — the KV key, the
    # request subject, the chunk filter, and the stream itself. `decode_fixture` is a
    # *public* table (zebridge_public_tables), so its tenant is the open tenant, and its
    # chunks live on INIT_PUBLIC. The old shape (`snapshot.request.<table>`, stream
    # "INIT") produced a Publish Violation and a consumer on a stream that does not
    # exist — NOTES.md §1.16.
    tenant = zb.TOPOLOGY["open_tenant"]
    snap_key = f"{tenant}.{TABLE}"

    async def descriptor():
        try:
            return zb.decode((await kv_snap.get(snap_key)).value)
        except Exception:  # noqa: BLE001
            return None

    before = await descriptor()
    stale_id = before["snapshot_id"] if before else None

    req = zb.subject(zb.TOPOLOGY["subjects"]["snapshot_request"], tenant, TABLE)
    try:
        await js.publish(req, b"")
    except Exception:  # noqa: BLE001
        pass  # a request already in flight is fine — read whatever descriptor lands

    desc = None
    for _ in range(30):
        desc = await descriptor()
        if desc and desc["snapshot_id"] != stale_id:
            break
        desc = None
        await asyncio.sleep(1)
    if not desc:
        sys.exit("no fresh snapshot descriptor appeared — is the bridge running?")

    # Same resolution as snapshot.py: `streams.init` ("INIT") is a generic label, not a
    # real stream — only INIT_PUBLIC / INIT_<TENANT> exist.
    init_stream = zb.TOPOLOGY["init_streams"]["public"]
    filt = f"init.snap.{tenant}.{TABLE}.{desc['snapshot_id']}.>"
    sub = await js.pull_subscribe(
        filt, durable=None, stream=init_stream,
        config=ConsumerConfig(deliver_policy=DeliverPolicy.ALL, filter_subject=filt),
    )

    idx_col, label_col, doc_col, amount_col, kind_col = (
        columns.index(c) for c in ("idx", "label", "doc", "amount", "kind")
    )

    got: dict[int, dict] = {}
    while True:
        try:
            batch = await sub.fetch(50, timeout=2)
        except Exception:  # noqa: BLE001
            break
        if not batch:
            break
        for msg in batch:
            for row in zb.decode(msg.data):
                # Every snapshot column is text (pg_copy_binary.zig's renderValue), idx
                # included — unlike the CDC path, where msgpack carries it as an int.
                idx = int(row[idx_col])
                got[idx] = {
                    "label": row[label_col],
                    "doc": json.loads(row[doc_col]) if isinstance(row[doc_col], str) else row[doc_col],
                    "amount": decimal.Decimal(str(row[amount_col])),
                    "kind": row[kind_col],
                }
            await msg.ack()
    return got


async def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 400
    failed = 0

    running = subprocess.run(["pgrep", "-f", "zig-out/bin/bridge"], capture_output=True, text=True)
    if running.stdout.strip():
        sys.exit(
            "another bridge is already running — this scenario starts its own so the CDC "
            "subscription sees exactly this run's inserts.\n    pkill -f zig-out/bin/bridge"
        )

    # ONE declaration makes a public table reachable now: its catalogue row,
    # written BEFORE the probe boots. The bridge derives the public set from
    # `zebridge_catalogue` at boot (grammar.json no longer carries a table list)
    # and reconciles CDC_PUBLIC's subject filter to it — the temp-topology and
    # stream-edit machinery this scenario used to need are both gone.
    zb.psql(
        "INSERT INTO public.zebridge_catalogue (tbl, public_reason) VALUES "
        f"('{TABLE}', 'decode_integrity.py fixture') ON CONFLICT (tbl) DO NOTHING",
        quiet=True)

    try:
        with zb.Bridge(SCRATCH / "decode_integrity.log") as br:
            if not br.wait_for_log("Replication started successfully", timeout=60):
                sys.exit(f"could not start a bridge — see {SCRATCH / 'decode_integrity.log'}")

            info = zb.nats_cli("stream", "info", CDC_PUBLIC_STREAM, "--json")
            bound = json.loads(info.stdout)["config"]["subjects"] if info.returncode == 0 else []
            if FIXTURE_SUBJECT not in bound:
                sys.exit(f"boot reconciliation did not bind {FIXTURE_SUBJECT} (bound: {bound})")
            print(f"0. boot reconciliation bound {FIXTURE_SUBJECT} in {CDC_PUBLIC_STREAM}")

            nc = await zb.connect()
            try:
                print(f"1. CDC path ({n} rows, one transaction)")
                got_cdc = await collect_cdc(nc, n, timeout=30)
                expected = expected_rows()
                failed += check_rows(got_cdc, expected, "CDC")

                print(f"\n2. snapshot path ({n} rows)")
                columns = zb.psql(
                    f"SELECT attname FROM pg_attribute WHERE attrelid='public.{TABLE}'::regclass "
                    "AND attnum>0 AND NOT attisdropped ORDER BY attnum"
                ).splitlines()
                got_snap = await collect_snapshot(nc, columns)
                failed += check_rows(got_snap, expected, "snapshot")
            finally:
                await nc.close()
    finally:
        # CDC_PUBLIC keeps the fixture subject until the NEXT bridge boot reconciles
        # it away — restart your bridge after this scenario.
        zb.psql(f"DROP TABLE IF EXISTS public.{TABLE}", quiet=True)
        zb.psql(f"DROP TYPE IF EXISTS {KIND_TYPE}", quiet=True)
        zb.psql(
            "DELETE FROM public.zebridge_catalogue WHERE public_reason = "
            "'decode_integrity.py fixture'",
            quiet=True,
        )

    return 1 if failed else 0


zb.run(main)
