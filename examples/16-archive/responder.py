#!/usr/bin/env python3
"""The responder: one tenant's readings, answered from Parquet plus the live stream.

    manifest (NATS KV) ── files ──▶ local cache ──┐
                                                   ├─▶ DuckDB view `readings` ─▶ zb.serve handlers
    stream READINGS after covers_up_to ─▶ hot tail ┘

It holds nothing durable. At start it reads the manifest, fetches the Parquet files it names
(once: they never change) and follows the stream from `covers_up_to + 1` into the hot tail.
When the archiver moves the manifest, the new files join the view and the hot rows they
cover leave it: a reading is counted once, from Parquet or from the tail, never both.

Questions, on query.<tenant>.<name>:

    count                         {"rows", "archived", "hot", "covers_up_to", "files"}
    per_minute {"kind", "minutes": 10}   {"columns", "rows"}: count and average per minute
    latest     {"kind"}           {"columns", "rows"}: each sensor's last reading

    NATS_URL=… NATS_CREDS=<stream reader> python3 examples/16-archive/responder.py \
        --tenant acme --creds acme-responder.creds
"""
import argparse, asyncio, os, pathlib, sys, threading

import duckdb
import nats
from nats.js.api import ConsumerConfig, DeliverPolicy

import common
from zebridge import ZeBridge


class Readings:
    """The DuckDB side: Parquet files + hot tail, behind one view, under one lock."""

    def __init__(self, cache: pathlib.Path):
        self.cache = cache
        self.db = duckdb.connect()
        self.lock = threading.Lock()
        self.files: list[pathlib.Path] = []
        self.covers = 0
        self.archived = 0
        with self.lock:
            self.db.execute(f"CREATE TABLE hot ({common.COLUMNS})")
            self.view()

    def view(self):
        # Resent readings (same sensor, kind, sensor time) count once, the latest one kept.
        parquet = f"SELECT * FROM read_parquet({[str(f) for f in self.files]!r})" if self.files else \
            f"SELECT * FROM hot WHERE false"
        self.db.execute(f"""CREATE OR REPLACE VIEW readings AS
            SELECT * FROM ({parquet} UNION ALL SELECT * FROM hot WHERE stream_seq > {self.covers})
            QUALIFY row_number() OVER (PARTITION BY sensor_id, kind, sensor_ts ORDER BY stream_seq DESC) = 1""")

    def manifest(self, m: dict):
        files = common.fetch_files(m["files"], self.cache)   # outside the lock: may download
        with self.lock:
            self.files, self.covers = files, m["covers_up_to"]
            self.archived = sum(f["rows"] for f in m["files"])
            self.db.execute(f"DELETE FROM hot WHERE stream_seq <= {self.covers}")
            self.view()

    def add(self, rows: list[dict]):
        with self.lock:
            self.db.executemany(
                "INSERT INTO hot VALUES (?, ?, ?, ?, make_timestamp(? * 1000)::TIMESTAMPTZ, make_timestamp(? * 1000)::TIMESTAMPTZ, ?)",
                [(r["tenant"], r["sensor_id"], r["kind"], r["value"], r["sensor_ms"], r["received_ms"], r["stream_seq"]) for r in rows])

    def query(self, sql: str, params=()) -> dict:
        with self.lock:
            cur = self.db.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = [[v.isoformat() if hasattr(v, "isoformat") else v for v in r] for r in cur.fetchall()]
        return {"columns": cols, "rows": rows}


async def follow(r: Readings, tenant: str, ready: threading.Event):
    """Manifest first, then the stream after it; the manifest's watch moves the boundary."""
    nc = await nats.connect(os.environ.get("NATS_URL", "nats://127.0.0.1:4222"), user_credentials=os.environ.get("NATS_CREDS"))
    js = nc.jetstream()
    kv = await js.key_value(common.KV)
    m, _ = await common.read_manifest(kv, tenant)
    if m:
        await asyncio.to_thread(r.manifest, m)
    info = await js.stream_info(common.STREAM)
    start = max(r.covers + 1, info.state.first_seq)
    if info.state.messages and info.state.first_seq > r.covers + 1 and m:
        print(f"⚠️ the stream starts at {info.state.first_seq}, the archive covers up to {r.covers}: a hole", flush=True)
    sub = await js.subscribe(common.subject_filter(tenant), stream=common.STREAM, ordered_consumer=True,
                             config=ConsumerConfig(deliver_policy=DeliverPolicy.BY_START_SEQUENCE, opt_start_seq=start))
    watch = await kv.watch(common.manifest_key(tenant), include_history=False)
    print(f"responder {tenant}: {len(r.files)} file(s) up to {r.covers}, the stream from {start}", flush=True)
    ready.set()

    async def manifests():
        async for e in watch:
            if e is not None and e.value:
                import json
                await asyncio.to_thread(r.manifest, json.loads(e.value))
                print(f"manifest: {len(r.files)} file(s), covers up to {r.covers}", flush=True)

    asyncio.create_task(manifests())
    batch = []
    while True:
        try:
            msg = await sub.next_msg(timeout=0.2)
            row = common.reading(msg)
            if row is not None:
                batch.append(row)
            if len(batch) < 1000:
                continue
        except nats.errors.TimeoutError:
            pass
        if batch:
            r.add(batch)
            batch = []


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tenant", required=True)
    ap.add_argument("--creds", required=True, help="a responder's creds (bridge --mint-responder): answers the questions")
    ap.add_argument("--queue", default="readings")
    ap.add_argument("--cache", default="/var/cache/zb-readings", help="the Parquet files, fetched once")
    a = ap.parse_args()

    r = Readings(pathlib.Path(a.cache))
    ready = threading.Event()
    threading.Thread(target=lambda: asyncio.run(follow(r, a.tenant, ready)), daemon=True, name="follow").start()
    if not ready.wait(60):
        sys.exit("the manifest or the stream did not answer in 60 s")

    def own(tenant: str):
        if tenant != a.tenant:   # one responder per tenant: never answer another's data
            raise PermissionError(f"this responder serves {a.tenant}, not {tenant}")

    def count(q: dict, tenant: str):
        own(tenant)
        rows, hot = r.query(f"SELECT count(*), count(*) FILTER (WHERE stream_seq > {r.covers}) FROM readings")["rows"][0]
        return {"rows": rows, "archived": rows - hot, "hot": hot, "covers_up_to": r.covers, "files": len(r.files)}

    def per_minute(q: dict, tenant: str):
        own(tenant)
        return r.query("SELECT date_trunc('minute', sensor_ts) AS minute, count(*) AS n, round(avg(value), 3) AS avg "
                       "FROM readings WHERE kind = ? AND sensor_ts >= now() - to_minutes(?) GROUP BY 1 ORDER BY 1",
                       (q["kind"], int(q.get("minutes", 10))))

    def latest(q: dict, tenant: str):
        own(tenant)
        return r.query("SELECT sensor_id, arg_max(value, sensor_ts) AS value, max(sensor_ts) AS sensor_ts "
                       "FROM readings WHERE kind = ? GROUP BY 1 ORDER BY 1", (q["kind"],))

    with ZeBridge(nats_url=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"), creds_path=a.creds,
                  db_path=str(pathlib.Path(a.cache) / "zb.sqlite3"), client_id=f"readings-{a.tenant}", heartbeat_ms=0) as zb:
        n = zb.serve([a.tenant], {"count": count, "per_minute": per_minute, "latest": latest}, queue=a.queue)
        print(f"answering count, per_minute, latest on query.{a.tenant}.<name> ({n} subject(s))", flush=True)
        zb.wait()


if __name__ == "__main__":
    main()
