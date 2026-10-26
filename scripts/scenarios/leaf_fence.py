#!/usr/bin/env python3
"""Leaf-sourced recovery, live: a fence taken from a seeding leaf's chain (recovery model §6).

    scripts/scenarios/leaf_fence.py [--client libzb|ts|both] [--rows 2000] [--interval-ms 200]

A private stack: a disposable database, a hub (JetStream domain `hub`), a seeding leaf (`leaf`)
linked to it, one bridge on the hub. The leaf carries the seeding caches deploy/compose/leaf/mirrors.sh
makes (the per-tenant manifest projections the bridge prepares, and OBJ_gen-<tenant> mirrored); the
clients connect to the LEAF with `seedDomain: "leaf"`. Authentication is off: the link's grants and the
keeper's credentials are leaf_security.py's subject, the data path is this one's.

A client behind the leaf follows a table under load; a seed-epoch move (zebridge_reseed) cuts a chain
behind its position — the case a busy table makes common. Two cases per client:

  from_leaf         the leaf holds the new chain (manifest and objects) before the client acts on it:
                    the client FENCES FROM THE LEAF'S CHAIN ("[from the seeding leaf]"), its objects
                    read from the leaf (libzb: ZB_MEASURE says `source: seed`), the cursor at the
                    chain's cut, the rows exact, the anchor current. Before this, a leaf chain behind
                    the position was refused and the hub's chain fenced.
  leaf_missing      the leaf's manifest is there but its object cache is gone (two mirrors; one lags):
                    nothing is prepared from the leaf — the client says so, and fences the HUB's chain.
  leaf_stale        the leaf's manifest names an earlier incarnation of the CDC stream (a copy left from
                    before a recreate): its cut cannot be proven replayable on the hub's stream. That is
                    NOT retention expiry, and it must not stall recovery: the leaf's refusal sends the
                    client to the hub's chain, which is fenced. (libzb used to return success there and
                    never ask the hub.)

The client's schema watch is held (libzb ZB_TEST_SCHEMA_PAUSE_FILE, a fault build; zb-client-ts a
`before_decision` hold file) until the new chain is on the leaf, and the tails keep applying meanwhile:
the position runs ahead of the chain's cut.

Needs nats-server, psql with a disposable database, a ReleaseFast bridge, and for libzb the fault build
(cd libzb && zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults).
"""
import argparse, asyncio, json, os, pathlib, re, subprocess, sys, tempfile, time, urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import nats
import zb
import recreate_recovery as rr
from recreate_recovery import Js, Load, Stack, TsClient, check, digest, note

LEAF_NAME = "lf-fence"
T = rr.T


class LeafStack(Stack):
    """The private stack with a hub, a seeding leaf and no NATS authentication."""

    def start(self, tenants):
        self.sql(f'CREATE DATABASE "{self.name}"', "postgres")
        r, w = self.name + "_r", self.name + "_w"
        self.env = dict(os.environ, DATABASE_READER_URL=f"postgres://{r}:x@127.0.0.1:5432/{self.name}",
                        DATABASE_WRITER_URL=f"postgres://{w}:x@127.0.0.1:5432/{self.name}", TARGET_DB=self.name,
                        BRIDGE_CDC_PUBLICATION="rr_pub")
        self.sql(subprocess.run([str(rr.BRIDGE), "--init-sql"], env=self.env, capture_output=True, text=True, check=True).stdout)
        self.sql("".join(f"INSERT INTO zebridge_user_tenants VALUES ('{p}', '{t}');" for p, t in tenants.items()))
        hub, leaf, link, hub_http, leaf_http = (rr.port() for _ in range(5))
        for name, listen, http, extra in (
                ("hub", hub, hub_http, f'leafnodes {{ listen: "127.0.0.1:{link}", no_advertise: true }}'),
                ("leaf", leaf, leaf_http, f'leafnodes {{ remotes: [{{ url: "nats://127.0.0.1:{link}" }}] }}')):
            (self.tmp / f"{self.name}-{name}.conf").write_text(
                f'listen: 127.0.0.1:{listen}\nhttp: 127.0.0.1:{http}\nmax_payload: 8388608\n'
                f'jetstream {{ domain: {name}, store_dir: "{self.tmp}/{self.name}-{name}-js" }}\n{extra}\n')
            self.spawn(name, ["nats-server", "-c", str(self.tmp / f"{self.name}-{name}.conf")], dict(os.environ))
            self.wait_log(name, "Server is ready")
        self.url, self.leaf_url, self.leaf_http = f"nats://127.0.0.1:{hub}", f"nats://127.0.0.1:{leaf}", leaf_http
        self.bridge_creds = None
        self.creds = {p: "" for p in tenants}
        for _ in range(100):
            if json.load(urllib.request.urlopen(f"http://127.0.0.1:{leaf_http}/leafz", timeout=3)).get("leafnodes", 0): break
            time.sleep(0.1)
        else:
            sys.exit("the leaf never linked to the hub")
        env = {k: v for k, v in self.env.items() if k not in ("NATS_CREDS", "NATS_BRIDGE_NKEY_SEED", "ZB_SIGNING_SEED")}
        self.env = dict(env, NATS_URL=self.url, NATS_JS_DOMAIN="hub", NATS_TLS_CA="", MUTATION_ADMISSION="0", GENERATIONS_ENABLED="1",
                        GENERATION_CADENCE_SECONDS=str(self.cadence), BASE_BUF="12", LOG_LEVEL="info", **self.cdc_env)

    async def caches(self, tenants):
        """The leaf's seeding caches, as mirrors.sh makes them, once the hub has a chain."""
        r = subprocess.run([str(rr.BRIDGE), "--mutation-ingress", "--prepare-projections"], capture_output=True, text=True, timeout=60,
                           env=dict(self.env, ZB_LEAF_TENANTS=" ".join(tenants), NATS_JS_DOMAIN="hub"))
        if r.returncode:
            sys.exit(f"--prepare-projections failed:\n{(r.stdout + r.stderr)[-1500:]}")
        hub = await nats.connect(self.url, connect_timeout=5)
        leaf = await nats.connect(self.leaf_url, connect_timeout=5)
        try:
            async def req(nc, subject, body):
                return json.loads((await nc.request(subject, json.dumps(body).encode(), timeout=15)).data)
            external = {"api": "$JS.hub.API", "deliver": f"$ZB.MIRROR.{LEAF_NAME}"}
            self.mirror_config = {}
            open_t = os.environ.get("OPEN_TENANT", "_default")
            for tenant in sorted({*tenants, open_t}):
                origin = await req(hub, f"$JS.API.STREAM.INFO.OBJ_gen-{tenant}", {})
                if "config" not in origin:
                    sys.exit(f"hub has no OBJ_gen-{tenant} yet: {origin}")
                config = {k: v for k, v in origin["config"].items() if k not in ("subjects", "sources", "mirror", "metadata", "consumer_limits")}
                config |= {"name": f"OBJ_gen-{tenant}", "mirror_direct": False, "mirror": {"name": f"OBJ_gen-{tenant}", "external": external}}
                self.mirror_config[tenant] = config
                out = await req(leaf, f"$JS.API.STREAM.CREATE.OBJ_gen-{tenant}", config)
                if "error" in out:
                    sys.exit(f"leaf cache OBJ_gen-{tenant}: {out['error']}")
            out = await req(leaf, "$JS.API.STREAM.CREATE.KV_generations", {
                "name": "KV_generations", "subjects": ["$KV.generations.>"], "storage": "file", "allow_direct": True,
                "max_bytes": 64 << 20, "max_msgs_per_subject": 1, "allow_rollup_hdrs": True,
                "sources": [{"name": f"KV_generations_leaf_{t}", "external": external} for t in sorted({*tenants, open_t})]})
            if "error" in out:
                sys.exit(f"leaf cache KV_generations: {out['error']}")
        finally:
            await hub.close(); await leaf.close()

    async def drop_object_cache(self, tenant):
        """The leaf's object cache for a tenant gone (its manifest cache kept): a lagging mirror."""
        leaf = await nats.connect(self.leaf_url, connect_timeout=5)
        try:
            out = json.loads((await leaf.request(f"$JS.API.STREAM.DELETE.OBJ_gen-{tenant}", b"", timeout=15)).data)
            return bool(out.get("success"))
        finally:
            await leaf.close()

    async def restore_object_cache(self, tenant):
        leaf = await nats.connect(self.leaf_url, connect_timeout=5)
        try:
            out = json.loads((await leaf.request(f"$JS.API.STREAM.CREATE.OBJ_gen-{tenant}", json.dumps(self.mirror_config[tenant]).encode(), timeout=15)).data)
            return "error" not in out
        finally:
            await leaf.close()

    async def doctor_manifest(self, tenant, change):
        """The leaf's cached manifest rewritten (the hub's stays): what a stale mirror holds."""
        leaf = await nats.connect(self.leaf_url, connect_timeout=5)
        try:
            js = leaf.jetstream(domain="leaf")
            subject = f"$KV.{zb.kv_bucket('generations')}.{tenant}.{T}"
            msg = await js.get_last_msg("KV_generations", subject, direct=True)
            await js.publish(subject, json.dumps(change(json.loads(msg.data))).encode())
        finally:
            await leaf.close()

    async def leaf_manifest(self, tenant):
        leaf = await nats.connect(self.leaf_url, connect_timeout=5)
        try:
            msg = await leaf.jetstream(domain="leaf").get_last_msg("KV_generations", f"$KV.{zb.kv_bucket('generations')}.{tenant}.{T}", direct=True)
            return json.loads(msg.data)
        except Exception:
            return None
        finally:
            await leaf.close()


class LeafClient(rr.Client):
    """A libzb client that connects to the LEAF and seeds from it."""

    def start(self, env=None):
        pathlib.Path(self.db + ".stop").unlink(missing_ok=True)
        opts = dict(natsUrl=self.stack.leaf_url, principal=self.principal, jsDomain="hub", seedDomain="leaf",
                    clientId=f"rr-{self.name}", dbPath=self.db, tables=[T], heartbeatMs=0,
                    **({"seedChunkRows": self.chunk_rows} if self.chunk_rows else {}))
        self.p = subprocess.Popen([sys.executable, rr.__file__, "--worker", json.dumps(opts), "--lib", self.lib], stdout=subprocess.DEVNULL,
                                  stderr=open(self.log, "a"), env=dict(os.environ, ZB_MEASURE="1", **(env or {})))


class LeafTsClient(TsClient):
    def start(self, env=None):
        pathlib.Path(self.db + ".stop").unlink(missing_ok=True)
        args = ["node", "--experimental-strip-types", "--no-warnings", str(rr.HARNESS), self.stack.leaf_url, "-", self.db, self.principal, T,
                str(self.chunk_rows or 0)]
        self.p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=open(self.log, "a"), cwd=zb.ROOT / "zb-client-ts",
                                  env=dict(os.environ, ZB_SEED_DOMAIN="leaf", **(env or {})))


async def run(a, tmp, ts):
    kind = "ts" if ts else "libzb"
    hooks = (b"ZB_TEST_SCHEMA_PAUSE_FILE",)
    if ts:
        if not check(f"the harness exists ({rr.HARNESS})", rr.HARNESS.exists()):
            return
    elif not check(f"the leaf cases run on a fault-hook build ({rr.FAULT_LIBRARY})", rr.FAULT_LIBRARY.exists() and all(h in rr.FAULT_LIBRARY.read_bytes() for h in hooks)):
        note(f"build it: {rr.FAULT_BUILD}")
        return
    s = LeafStack(tmp, "leafts" if ts else "leaf", a.cadence, {})
    try:
        s.start({"pa": "t_a"})
        s.create_table({"t_a": a.rows}, "a")
        s.bridge_up()
        js = await Js().open(s)
        check("tenant mappings published before any client connects", await js.tenants_ready(s.creds))
        t0 = time.monotonic()
        while time.monotonic() - t0 < a.cadence + 60 and not await js.manifest("t_a"): await asyncio.sleep(1)
        check(f"a chain on the hub ({round(time.monotonic() - t0, 1)} s)", await js.manifest("t_a") is not None)
        await s.caches(["t_a"])
        t0 = time.monotonic()
        while time.monotonic() - t0 < 60 and not await s.leaf_manifest("t_a"): await asyncio.sleep(1)
        check(f"the leaf holds the manifest ({round(time.monotonic() - t0, 1)} s)", await s.leaf_manifest("t_a") is not None)
        base = {k for k in s.truth("t_a") if k.startswith("at_a-")}

        async def until(pred, timeout):
            t = time.monotonic()
            while time.monotonic() - t < timeout:
                r = pred()
                if asyncio.iscoroutine(r): r = await r
                if r: return round(time.monotonic() - t, 1)
                await asyncio.sleep(1)
            return None

        async def recovered(c):
            ok, _ = await rr.anchor_state(c, js, {"t_a"})
            return ok and base <= (c.rows() or {}).keys()

        def fence_cut(c):
            m = re.findall(r"fence prepared — chain g\d+ cut at (\d+)", c.text())
            return int(m[-1]) if m else None

        def exact(c):
            return digest(c.rows() or {}) == digest(s.truth("t_a"))

        async def leaf_current():
            m, ep = await s.leaf_manifest("t_a") or {}, await js.epoch()
            return ep is not None and m.get("seed_epoch") == ep and ep > epoch0

        for name in ("from_leaf", "leaf_missing", "leaf_stale"):
            load = Load(s, "t_a", "c", a.interval_ms / 1000)
            c = (LeafTsClient(s, f"{name}_{kind}", "pa", "t_a", chunk_rows=200) if ts
                 else LeafClient(s, f"{name}_{kind}", "pa", "t_a", chunk_rows=200, lib=rr.FAULT_LIBRARY))
            c.start()
            check(f"{name}: seeded and anchored through the leaf ({await until(lambda: recovered(c), 150)} s)", await recovered(c))
            if not ts:
                check(f"{name}: the first seed read the leaf's objects", c.count('"m":"chain".*"source":"seed"') > 0 and c.count('"m":"chain".*"source":"hub"') == 0)
            c.stop()
            pause = pathlib.Path(f"/tmp/zb-rr-leaf-{name}-{kind}.pause")
            pause.unlink(missing_ok=True)
            c.start(({"ZB_TEST_FENCE_HOLD_FILE": str(pause)} if ts else {"ZB_TEST_SCHEMA_PAUSE_FILE": str(pause)}))
            await asyncio.sleep(5)   # caught up with the load again
            pause.touch()
            epoch0 = await js.epoch()
            s.sql(f"SELECT count(*) FROM zebridge_reseed('public.{T}'::regclass)")
            note(f"{name}: the new chain on the leaf after {await until(leaf_current, 90)} s")
            if name == "leaf_missing":
                check(f"{name}: the leaf's object cache dropped (its manifest kept)", await s.drop_object_cache("t_a"))
            if name == "leaf_stale":
                await s.doctor_manifest("t_a", lambda m: {**m, "cdc_stream_created": "2020-01-01T00:00:00.000000Z"})
                m = await s.leaf_manifest("t_a")
                check(f"{name}: the leaf's manifest now names an earlier incarnation of the stream", m.get("cdc_stream_created", "").startswith("2020"))
            await asyncio.sleep(4)   # the tails go on applying: the position runs ahead of the chain's cut
            pause.unlink(missing_ok=True)
            took = await until(lambda: fence_cut(c) is not None, 90)
            check(f"{name}: a fence is taken ({took} s, cut {fence_cut(c)})", took is not None)
            if name == "from_leaf":
                check(f"{name}: from the seeding leaf's chain", "[from the seeding leaf]" in c.text())
                # (while the new chain is still on its way to the leaf a poll may fall back to the hub and wait: only a FENCE counts)
                check(f"{name}: every fence prepared was the leaf's ({c.count('fence prepared')} of {c.text().count('[from the seeding leaf]')})",
                      c.count("fence prepared") > 0 and c.count("fence prepared") == c.text().count("[from the seeding leaf]"))
                if not ts:
                    last = re.findall(r'"m":"chain".*?"source":"(seed|hub)"', c.text())
                    check(f"{name}: the fence's objects came from the leaf (chain sources {last})", bool(last) and last[-1] == "seed")
            else:
                check(f"{name}: nothing was prepared from the leaf, and the client went to the hub",
                      "[from the seeding leaf]" not in c.text() and "seeding from the hub" in c.text())
            secs = await until(lambda: recovered(c), 120)
            check(f"{name}: recovered under a current anchor while writes continue ({secs} s)", secs is not None)
            load.end()
            ok = await until(lambda: exact(c), 120)
            check(f"{name}: exact against PostgreSQL ({ok} s)", ok is not None)
            c.stop()
            if name == "leaf_missing":
                await s.restore_object_cache("t_a")
        await js.close()
    finally:
        s.stop()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--client", choices=("libzb", "ts", "both"), default="both")
    ap.add_argument("--rows", type=int, default=2000)
    ap.add_argument("--interval-ms", type=int, default=200)
    ap.add_argument("--cadence", type=int, default=120)
    a = ap.parse_args()
    with tempfile.TemporaryDirectory(prefix="zb-leaf-") as d:
        for ts in ([False] if a.client == "libzb" else [True] if a.client == "ts" else [False, True]):
            print(f"── leaf fence, {'zb-client-ts' if ts else 'libzb'}", flush=True)
            try:
                asyncio.run(run(a, pathlib.Path(d), ts))
            except Exception as e:
                check(f"raised: {type(e).__name__}: {str(e)[:400]}", False)
    print(f"  · logs: /tmp/zb_rrleaf_*.log, /tmp/zb_rrleafts_*.log; {'FAILED ' + str(rr.failed) + ' check(s)' if rr.failed else 'clean'}")
    return 1 if rr.failed else 0


if __name__ == "__main__":
    sys.exit(main())
