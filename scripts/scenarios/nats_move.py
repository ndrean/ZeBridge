#!/usr/bin/env python3
"""A bridge moved to another NATS on the same slot rebuilds its chains (§10kz).

Moving a deployment (a bigger VPS, a new NATS) keeps the database and the slot, so
`zebridge_generations` still records the chains built in the OLD server's object store.
The producer must not trust those records where this NATS holds no matching manifest:
it builds a full for every such pair, and stays quiet when the manifests match.

One bridge, its own publication `zb_nm` (tables nm_pub, public, and nm_ten, rows in tenants
acme and globex), slot `zb_nm`, port 9099, on a NATS of its own (a generated `--init-nats
operator` config on port 14226). The same server config is started on two data
directories, so the operator and the bridge's credentials stay the same:

  1. NATS 1, fresh: the first chains are built, and nothing is forced (nothing recorded);
  2. NATS 2, empty, same slot: one forced full per recorded pair, and every pair's
     manifest on NATS 2 names PostgreSQL's latest generation and a full object that is
     there;
  3. back to NATS 1, whose manifests are one generation behind: one forced full per pair
     again, and the manifests match once more;
  4. restarted on NATS 1: nothing forced, nothing built.

Usage:  set -a; . ./.env.bridge; set +a; python scripts/scenarios/nats_move.py
"""
import asyncio
import json
import os
import subprocess
import tempfile
import time

import nats
import zb

ADMIN_URL = "postgres://postgres@127.0.0.1:5432/postgres"
BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
NATS_PORT = 14226
NATS_URL = f"nats://127.0.0.1:{NATS_PORT}"
PORT = 9099
PUB = SLOT = "zb_nm"
TABLES = ("nm_pub", "nm_ten")
KV = zb.TOPOLOGY["generations"]["kv"]
FORCED = "this NATS holds no manifest"
# The pairs with rows. nm_ten also gets a chain for every other tenant that has a stream.
REQUIRED = {("_default", "nm_pub"), ("acme", "nm_ten"), ("globex", "nm_ten")}


def setup_db():
    zb.psql(f"SELECT * FROM zebridge_create_publication('{PUB}')")
    zb.psql("CREATE TABLE IF NOT EXISTS public.nm_pub (id int PRIMARY KEY, v int, "
            "updated_at timestamptz NOT NULL DEFAULT now())")
    zb.psql("CREATE TABLE IF NOT EXISTS public.nm_ten (id int PRIMARY KEY, tenant_id text NOT NULL, v int, "
            "updated_at timestamptz NOT NULL DEFAULT now())")
    for t, scope in (("nm_pub", "public_reason => 'nats_move test'"), ("nm_ten", "tenant_col => 'tenant_id'")):
        out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{t}', {scope}, "
                      f"version_col => 'updated_at', publication => '{PUB}', dry_run => false)")
        if "ERROR" in out:
            raise SystemExit(f"zebridge_enable {t}: {out}")
    zb.psql("INSERT INTO public.nm_pub (id, v) VALUES (1, 1), (2, 2)")
    zb.psql("INSERT INTO public.nm_ten (id, tenant_id, v) VALUES (1, 'acme', 1), (2, 'globex', 2)")


def teardown_db():
    subprocess.run([str(BRIDGE), "--drop-slot", SLOT], env=dict(os.environ, ADMIN_DATABASE_URL=ADMIN_URL),
                   capture_output=True)
    for t in TABLES:
        zb.psql(f"DROP TABLE IF EXISTS public.{t}", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{t}'", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_generations WHERE tbl = '{t}'", quiet=True)
    zb.psql(f"DROP PUBLICATION IF EXISTS {PUB}", quiet=True)


def generate(tmp: str) -> tuple[str, str, dict]:
    """The generated config, and a copy of it on a second, empty data directory."""
    subprocess.run([str(BRIDGE), "--init-nats", "operator"], cwd=tmp, capture_output=True, check=True)
    gen = os.path.join(tmp, "zb-nats")
    conf1 = os.path.join(gen, "nats-server.conf")
    text = open(conf1).read().replace("port: 4222", f"port: {NATS_PORT}") \
        .replace("port: 8222", "port: 18226").replace("port: 8080", "port: 18084")
    open(conf1, "w").write(text)
    store1 = os.path.join(gen, "nats-data")
    conf2 = os.path.join(tmp, "nats-server-2.conf")
    open(conf2, "w").write(text.replace(store1, os.path.join(tmp, "nats-data-2")))
    genv = dict(line.split("=", 1) for line in open(os.path.join(gen, "." + "env.nats")).read().splitlines()
                if "=" in line and not line.startswith("#"))
    return conf1, conf2, genv


def start_nats(conf: str) -> subprocess.Popen:
    ns = subprocess.Popen(["nats-server", "-c", conf], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and subprocess.run(["nc", "-z", "127.0.0.1", str(NATS_PORT)],
                                                         capture_output=True).returncode:
        time.sleep(0.2)
    return ns


def start_bridge(log: str, genv: dict) -> subprocess.Popen:
    env = dict(os.environ, NATS_URL=NATS_URL, NATS_CREDS=genv["NATS_CREDS"], NATS_TLS_CA="", NATS_JS_DOMAIN="",
               ZB_SIGNING_SEED=genv["ZB_SIGNING_SEED"], ZB_ACCOUNT_PUB=genv["ZB_ACCOUNT_PUB"],
               GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="300", LOG_LEVEL="info")
    return subprocess.Popen([str(BRIDGE), "--pub", PUB, "--slot", SLOT, "--port", str(PORT)],
                            env=env, stdout=subprocess.DEVNULL, stderr=open(log, "w"))


def stop(p: subprocess.Popen | None):
    if p is None:
        return
    p.terminate()
    try:
        p.wait(timeout=15)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


def recorded() -> dict[tuple[str, str], int]:
    """(tenant, table) → the latest generation PostgreSQL records."""
    out = zb.psql("SELECT tenant || '|' || tbl || '|' || max(gen) FROM public.zebridge_generations "
                  f"WHERE tbl IN ({', '.join(repr(t) for t in TABLES)}) AND retired_at IS NULL GROUP BY tenant, tbl")
    pairs = {}
    for line in out.splitlines():
        tenant, tbl, gen = line.split("|")
        pairs[(tenant, tbl)] = int(gen)
    return pairs


async def manifests(creds: str, pairs) -> dict[tuple[str, str], tuple[int | None, bool]]:
    """(tenant, table) → (the manifest's generation, whether its full object is in the
    store); (None, False) when this NATS holds no manifest for the pair."""
    nc = await nats.connect(NATS_URL, user_credentials=creds)
    js = nc.jetstream()
    out = {}
    try:
        try:
            kv = await js.key_value(KV)
        except Exception:  # noqa: BLE001
            return {p: (None, False) for p in pairs}
        for tenant, tbl in pairs:
            try:
                man = json.loads((await kv.get(f"{tenant}.{tbl}")).value)
            except Exception:  # noqa: BLE001
                out[(tenant, tbl)] = (None, False)
                continue
            try:
                store = await js.object_store(man["bucket"])
                await store.get_info(man["full"]["object"])
                full_there = True
            except Exception:  # noqa: BLE001
                full_there = False
            out[(tenant, tbl)] = (man.get("gen"), full_there)
    finally:
        await nc.close()
    return out


def wait_matched(creds: str, timeout: float = 90) -> tuple[dict, dict]:
    """Until every recorded pair's manifest names PostgreSQL's latest generation."""
    deadline = time.monotonic() + timeout
    while True:
        pairs = recorded()
        mans = asyncio.run(manifests(creds, pairs))
        if (pairs and all(mans[p][0] == g and mans[p][1] for p, g in pairs.items())) or time.monotonic() > deadline:
            return pairs, mans
        time.sleep(1)


def forced(log: str) -> int:
    """Forced fulls for this scenario's tables. Others are forced too, and rightly: the
    internal zebridge_gc_watermark is in every publication, and its chain record is
    shared with the dev bridge, whose chain lives in the dev NATS."""
    return sum(1 for line in open(log, errors="replace") if FORCED in line
               and any(f"'/'{t}'" in line for t in TABLES))


def builds(log: str) -> int:
    return sum(1 for line in open(log, errors="replace") if "🧬 g" in line and " for '" in line
               and any(f"'/'{t}'" in line for t in TABLES))


def main() -> int:
    failed = 0

    def check(label: str, cond: bool, detail: str):
        nonlocal failed
        (zb.ok if cond else zb.bad)(f"{label}: {detail}")
        failed += 0 if cond else 1

    def matched(label: str, pairs: dict, mans: dict):
        bad = {f"{t}.{tb}": mans[(t, tb)] for (t, tb), g in pairs.items() if mans[(t, tb)] != (g, True)}
        missing = REQUIRED - set(pairs)
        check(label, not bad and not missing,
              f"{len(pairs)} pair(s), every manifest names PostgreSQL's latest generation and its full is in the store"
              + (f" — off: {bad}" if bad else "") + (f" — no chain recorded: {sorted(missing)}" if missing else ""))

    tmp = tempfile.mkdtemp(prefix="zb-nats-move-")
    ns = br = None
    logs = []
    try:
        teardown_db()
        setup_db()
        conf1, conf2, genv = generate(tmp)
        creds = genv["NATS_CREDS"]

        def run_on(conf: str, step: int) -> str:
            nonlocal ns, br
            stop(br)
            stop(ns)
            ns = start_nats(conf)
            log = os.path.join(tmp, f"bridge_{step}.log")
            logs.append(log)
            br = start_bridge(log, genv)
            return log

        # ── 1. NATS 1, fresh ─────────────────────────────────────────────────────
        log = run_on(conf1, 1)
        pairs, mans = wait_matched(creds)
        matched("1. first chains", pairs, mans)
        check("1. nothing forced", forced(log) == 0,
              f"{forced(log)} forced full(s) on a NATS with nothing recorded before it")
        gen1 = dict(pairs)

        # ── 2. NATS 2, empty, the same slot ─────────────────────────────────────
        log = run_on(conf2, 2)
        pairs, mans = wait_matched(creds)
        check("2. forced fulls", forced(log) == len(gen1),
              f"{forced(log)} forced full(s) for {len(gen1)} pair(s) recorded on NATS 1")
        matched("2. chains on the new NATS", pairs, mans)

        # ── 3. back to NATS 1, one generation behind ────────────────────────────
        log = run_on(conf1, 3)
        pairs, mans = wait_matched(creds)
        check("3. forced fulls", forced(log) == len(gen1),
              f"{forced(log)} forced full(s): NATS 1's manifests named g{sorted(set(gen1.values()))}, "
              f"PostgreSQL had moved on")
        matched("3. chains back on NATS 1", pairs, mans)

        # ── 4. restarted on the same NATS ───────────────────────────────────────
        log = run_on(conf1, 4)
        started = False
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not started:
            started = "Generation producer started" in open(log, errors="replace").read()
            time.sleep(0.5)
        time.sleep(10)  # the first tick runs at once; give it time to decide every pair
        check("4. restart", started and forced(log) == 0 and builds(log) == 0,
              f"producer {'started' if started else 'NOT started'}, {forced(log)} forced full(s), "
              f"{builds(log)} build(s) on an unchanged NATS")
    finally:
        stop(br)
        stop(ns)
        teardown_db()
        for i, src in enumerate(logs, 1):
            subprocess.run(["cp", src, f"/tmp/zb_nats_move_{i}.log"])
        subprocess.run(["rm", "-rf", tmp])
    print()
    if failed:
        zb.bad(f"{failed} check(s) failed")
        return 1
    zb.ok("a bridge moved to another NATS on the same slot rebuilds every chain, and a restart rebuilds none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
