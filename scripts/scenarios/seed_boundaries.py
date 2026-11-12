#!/usr/bin/env python3
"""Snapshot boundaries on a private database and JetStream.

A committed delta must include an update whose transaction was open across the
preceding cut. Fresh native and TS replicas then pause at a plain full's durable
anchor. The next two commits update and delete known rows. Both replicas must
replay those messages on the same anchor and match PostgreSQL.
"""
import asyncio
from datetime import datetime, timezone
import os
import pathlib
import select
import subprocess
import tempfile
import time

import msgpack
import zstandard

from recreate_recovery import Client, TsClient, Stack, Js, T, FAULT_LIBRARY, FAULT_BUILD


def require(ok, message):
    if not ok:
        raise AssertionError(message)
    print(f"  ✓ {message}", flush=True)


async def wait_for(read, ready, what, seconds=75):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        value = await read()
        if ready(value):
            return value
        await asyncio.sleep(.15)
    raise AssertionError(f"timed out waiting for {what}; last value {value!r}")


async def document(js, manifest, step):
    bucket = await js.js.object_store(manifest["bucket"])
    blob = (await bucket.get(step["object"])).data
    if blob.startswith(b"\x28\xb5\x2f\xfd"):
        blob = zstandard.ZstdDecompressor().decompress(blob, max_output_size=1 << 30)
    data = msgpack.unpackb(blob, raw=False, strict_map_key=False)
    return [dict(zip(data["columns"], row, strict=True)) for row in data["rows"]]


async def across_cut(s, js):
    table, tenant = "sb_open", "t_a"

    async def manifest():
        return await js.kv(js.gen, f"{tenant}.{table}")

    await wait_for(manifest, bool, "the initial generation")
    with subprocess.Popen([*s.psql, "-d", s.name], stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, text=True) as session:
        try:
            session.stdin.write(f"BEGIN; UPDATE {table} SET note='across cut', updated_at=now() "
                                "WHERE id='1' RETURNING id;\n")
            session.stdin.flush()
            if not select.select([session.stdout], [], [], 10)[0]:
                raise RuntimeError("held PostgreSQL transaction did not report its UPDATE")
            require(session.stdout.readline().strip() == "1", "the open transaction has updated row 1")
            before = (await manifest())["gen"]
            s.sql(f"INSERT INTO {table} (id,tenant_id,note) VALUES ('trigger','{tenant}','cut')")
            held = await wait_for(manifest, lambda m: m and m["gen"] > before, "a cut before COMMIT")
            step = next((d for d in held["deltas"] if d["gen"] == held["gen"]), held["full"])
            cut_rows = await document(js, held, step)
            require(any(r["id"] == "trigger" for r in cut_rows)
                    and not any(r["note"] == "across cut" for r in cut_rows),
                    "the cut sees the trigger row while the held update remains invisible")
            # Give the next delta a newer row even when a broken floor misses row 1.
            session.stdin.write(f"INSERT INTO {table} (id,tenant_id,note,updated_at) "
                                f"VALUES ('after','{tenant}','after',clock_timestamp()); COMMIT;\n")
        finally:
            session.stdin.close()
            try:
                session.wait(timeout=10)
            except subprocess.TimeoutExpired:
                session.kill()
                session.wait()
                raise
        require(session.returncode == 0, "the held transaction committed")
    wanted = held["gen"] + 1
    later = await wait_for(manifest, lambda m: m and m["gen"] >= wanted, "a cut after COMMIT")
    delta = next((d for d in later["deltas"] if d["gen"] == wanted), None)
    require(later["full"]["gen"] < wanted and delta, f"generation {wanted} is a delta over the earlier base")
    rows = await document(js, later, delta)
    require([r["note"] for r in rows if r["id"] == "1"] == ["across cut"],
            "the first delta after COMMIT contains the update held across the previous cut")


async def overlap(s, js, tmp):
    tenant = "t_b"
    stream = "CDC_t_b"
    # The private stream is quiet until the two deliberately separate commits.
    await wait_for(lambda: js.stream(stream), lambda x: x.get("last", 0) > 0, "baseline insert CDC")
    baseline_seq = (await js.stream(stream))["last"]
    # Ask for a full whose own sequence includes the baseline INSERT.
    s.sql(f"SELECT count(*) FROM zebridge_reseed('public.{T}'::regclass)")
    epoch = int(s.sql(f"SELECT seed_epoch FROM zebridge_catalogue WHERE tbl='{T}'"))
    pre = s.sql(f"SELECT id,note FROM {T} ORDER BY id")
    require(pre == "drop|old\nkeep|old\nupdate|old", "known snapshot rows exist in PostgreSQL")
    m = await wait_for(lambda: js.manifest(tenant),
                       lambda m: m and m["cutoff_seq"] == baseline_seq and m["gen"] == m["full"]["gen"]
                       and m["seed_epoch"] == epoch,
                       "the reseeded full at the baseline insert")
    cut = m["cutoff_seq"]
    require(m["full"]["gen"] == m["gen"] and (await js.stream(stream))["last"] == cut,
            f"the full is cut at the current end of {stream} ({cut})")
    require(sorted((r["id"], r["note"]) for r in await document(js, m, m["full"])) ==
            [("drop", "old"), ("keep", "old"), ("update", "old")], "the full object holds the baseline")
    clients = [Client(s, "native", "bob", tenant, lib=FAULT_LIBRARY), TsClient(s, "ts", "bob", tenant)]
    hold = tmp / "hold"
    hold.touch()
    try:
        for c in clients:
            c.start({"ZB_TEST_SEED_COMMIT_PAUSE_FILE": str(hold)})

        async def paused():
            for c in clients:
                if c.exited() is not None:
                    raise RuntimeError(f"{c.name} exited; see {c.log}")
            return [f"test: seed anchor committed for {T}" in c.text() for c in clients]

        await wait_for(paused, all, "both processes at the seed commit")
        anchors = [c.anchors() for c in clients]
        for c, anchor in zip(clients, anchors):
            require(len(anchor) == 1 and anchor[0]["anchor_seq"] == cut and
                    anchor[0]["watermark"] == m["cutoff_version"] and c.rows() == dict(line.split("|") for line in pre.splitlines()),
                    f"{c.name} paused with the selected snapshot committed")
        require(s.sql(f"UPDATE {T} SET note='after cut',updated_at=now() WHERE id='update' RETURNING id") == "update",
                "post-cut update committed")
        require(s.sql(f"DELETE FROM {T} WHERE id='drop' RETURNING id") == "drop", "post-cut delete committed")
        await wait_for(lambda: js.stream(stream), lambda x: x.get("last", 0) >= cut + 2, "both CDC messages")
        for seq, op, key in ((cut + 1, "UPDATE", "update"), (cut + 2, "DELETE", "drop")):
            msg = await js.js.get_msg(stream, seq=seq)
            event = msgpack.unpackb(msg.data, raw=False)
            require(event["table"] == T and event["operation"].upper() == op and event["data"]["id"] == key,
                    f"{stream} sequence {seq} carries {op} of {key}")
        for c in clients:
            require(c.rows() == {"drop": "old", "keep": "old", "update": "old"} and c.position(stream) <= cut,
                    f"{c.name} has neither change while paused")
    finally:
        hold.unlink(missing_ok=True)

    truth = dict(row.split("|") for row in s.sql(f"SELECT id,note FROM {T}").splitlines())
    require(truth == {"keep": "old", "update": "after cut"}, "PostgreSQL holds the expected final rows")
    for c, anchor in zip(clients, anchors):
        async def replayed():
            return c.position(stream), c.rows()
        await wait_for(replayed, lambda v: v[0] >= cut + 2 and v[1] == truth, f"{c.name} replay")
        require(c.anchors() == anchor, f"{c.name} replayed the update and delete on its original anchor")
        columns = "id, tenant_id, note, updated_at"
        pg = [row.split("|") for row in s.sql(f"SELECT {columns} FROM {T} ORDER BY id").splitlines()]
        def normalized(rows):
            return [(key, tn, note, datetime.fromisoformat(stamp).astimezone(timezone.utc))
                    for key, tn, note, stamp in rows]
        require(normalized(c._q(f"SELECT {columns} FROM {T} ORDER BY id")) == normalized(pg),
                f"{c.name} equals PostgreSQL on every row and column")


async def run():
    if not FAULT_LIBRARY.exists() or b"ZB_TEST_SEED_COMMIT_PAUSE_FILE" not in FAULT_LIBRARY.read_bytes():
        raise RuntimeError(f"rebuild the fault library: {FAULT_BUILD}")
    with tempfile.TemporaryDirectory(prefix="zb_sb_") as d:
        tmp = pathlib.Path(d)
        s = Stack(tmp, "seed_boundaries", 5, {"GENERATION_CHECKPOINT_SECONDS": "0", "GENERATION_CHAIN_DEPTH": "64", "CDC_MAX_AGE_SECONDS": "600"},
                  pg_port=int(os.environ.get("ZB_PG_PORT", "5432")))
        js = None
        try:
            s.start({"alice": "t_a", "bob": "t_b"})
            for table in ("sb_open", T):
                s.sql(f"CREATE TABLE {table} (id text PRIMARY KEY, tenant_id text NOT NULL, note text,"
                      " updated_at timestamptz NOT NULL DEFAULT now());"
                      f"SELECT count(*) FROM zebridge_enable('public.{table}', tenant_col=>'tenant_id', writable=>false,"
                      " version_col=>'updated_at', generations=>true,"
                      " publication=>'rr_pub', dry_run=>false)")
            s.sql("INSERT INTO sb_open (id,tenant_id,note) SELECT i::text,'t_a','old' FROM generate_series(1,200) i")
            s.bridge_up()
            s.sql(f"INSERT INTO {T} (id,tenant_id,note) SELECT id,'t_b','old' FROM unnest(ARRAY['update','drop','keep']) id")
            js = await Js().open(s)
            require(await js.tenants_ready(["alice", "bob"]), "tenant mappings are available")
            await across_cut(s, js)
            await overlap(s, js, tmp)
        finally:
            try:
                if js: await js.close()
            finally:
                s.stop()


if __name__ == "__main__":
    asyncio.run(run())
