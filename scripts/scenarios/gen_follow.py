#!/usr/bin/env python3
"""Counted ids against the chain: do clients that come and go converge, and how fast?

    python scripts/scenarios/gen_follow.py [--seconds 200] [--rate 200] [--noise 500]     (manual; ~6 min)

An emitter inserts ids 1, 2, 3… into a tenant-scoped table, `gen_ids` (tenant `acme`, plus
ONE `_default` row: the shared rows a tenant table's clients follow on CDC_PUBLIC), while a
public table, `gen_noise`, fills CDC_PUBLIC. The CDC streams keep 60 s of events and the
producer cuts every 20 s, so pruning starts within the first minute.

The oracle is the count: at any moment the truth is "ids 1..N". A replica is right when
count(*) = count(DISTINCT id) = max - min + 1 and the set equals PostgreSQL's.

Four libzb clients, each its own replica and process (its log stays its own), all mapped
to `acme`, on a schedule relative to the emitter's start:
  fresh   opens mid-load and stays
  short   offline 30 s — inside the stream's window: resumes from the tail
  long    offline 120 s — past the window: a real gap, healed from a chain
  alice   online the longest, then reconnects 5 s after going away, once CDC_PUBLIC has
          pruned past her position on it (the shared cut covers it: GENERATION.md)

Per client and per (re)connect: how long `connect`+`sync` took, how long until its max(id)
reached PostgreSQL's (a catch-up over 10 s is flagged), and from libzb's own log the seeds,
the refusals (`predates replica`), the gaps healed and the prunes met. At the end every
client must equal PostgreSQL.

Isolated like burst_tls.py: a scratch database (rendered from the templates), a scratch
nats-server without auth, the bridge on its own slot and port. Nothing of the dev stack is
written to, but the scratch slot's WAL is the dev cluster's: keep a disk guard on.

Needs nats-server, the nats CLI, envsubst, psql, a ReleaseFast bridge and libzb, and
`.env.admin` and `.env.bridge` sourced (the role URLs and the template variables).
"""
import argparse, ctypes, json, os, pathlib, signal, subprocess, sys, tempfile, threading, time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

ROOT = pathlib.Path(__file__).resolve().parents[2]
LIBZB = ROOT / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if sys.platform == "darwin" else "libzbcore.so")
TABLE, NOISE, TENANT = "gen_ids", "gen_noise", "acme"
CADENCE_S, CDC_AGE_S, CDC_MSGS = 20, 60, 20_000
SLOW_S = 10.0
# name → (client, [(open, close)]) in seconds from the emitter's start; close None = stay to
# the end. The zb-client-ts ones join at 75 s, once CDC_PUBLIC has pruned (60 s window): a
# browser joining a long-lived hub meets a stream whose first message is far past 1.
SCHEDULE = {
    "fresh": ("libzb", [(60, None)]),
    "short": ("libzb", [(15, 60), (90, None)]),
    "long": ("libzb", [(15, 40), (160, None)]),
    "alice": ("libzb", [(5, 150), (155, None)]),
    "ts_bob": ("ts", [(75, 120), (150, None)]),
    "ts_alice": ("ts", [(75, 170), (175, None)]),
}
NODE_DIR = pathlib.Path(os.environ.get("ZB_NODE_DIR", ROOT / "examples" / "04-node-consumer"))  # another zb-client-ts revision
# libzb's own words, counted per client (libzb/src/client.zig)
MARKS = {
    # zb-client-ts (examples/04-node-consumer, the library's log on stderr)
    "gap detected": "Gap detected!",
    "refused (predates this replica)": "predates this replica",
    "waiting for a chain": "waiting up to",
    # libzb
    "seeded": "seeded ",
    "refused (predates replica)": "chain predates replica",
    "chain behind the stream": "predates the stream",
    "gap healed": "gap healed",
    "pruned under the drain": "pruned under",
}


class Live:
    """After catch-up, once a second: how far behind PostgreSQL's newest id the replica is
    (ids, and seconds at the emitter's rate), and its holes: (max - min + 1) - count over the
    live rows, which the contiguous oracle keeps at 0 for a consistent replica."""

    def __init__(self, rate, say=None):
        self.rate, self.lags, self.holes, self.at, self.say, self.last = max(1, rate), [], 0, 0.0, say, None

    def sample(self, pg_max, l):
        if time.time() - self.at < 1 or "max" not in l:
            return
        self.at = time.time()
        self.lags.append(max(0, pg_max - l["max"]))
        holes = (l["max"] - l["min"] + 1) - l["count"] if l["count"] else 0
        self.holes = max(self.holes, holes)
        # A timeline of the replica while it has holes (and the sample after): when they
        # open and close, to line up with the client's own log.
        if self.say and (holes or self.last):
            self.say(event="holes", holes=holes, min=l["min"], max=l["max"], count=l["count"], wall=round(time.time(), 3))
        self.last = holes

    def report(self):
        if not self.lags:
            return {}
        xs = sorted(self.lags)
        q = lambda f: xs[min(len(xs) - 1, int(f * len(xs)))]
        return {"live_samples": len(xs), "lag_p50_s": round(q(.5) / self.rate, 2), "lag_p99_s": round(q(.99) / self.rate, 2),
                "lag_max_s": round(xs[-1] / self.rate, 2), "holes_max": self.holes}


def final_truth(done_file) -> dict:
    """PostgreSQL's live rows when the emitter stopped (JSON; a bare count from older runs)."""
    text = pathlib.Path(done_file).read_text()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        n = int(text)
        return {"count": n, "min": 1, "max": n, "notes": None}


def frame(i, note) -> bytes:
    """One row, framed: `id:N` for a NULL note, `id:S<bytes>:<note>` otherwise. The length
    prefix keeps NULL apart from '' and lets a note hold any byte, tabs and newlines included.
    PG_DIGEST_SQL builds the same frames on the server."""
    return f"{i}:N".encode() if note is None else f"{i}:S{len(note.encode())}:".encode() + note.encode()


# The exact comparison: sha256 over the ordered framed rows (an aggregate like
# sum(length(note)) can hide offsetting errors). PostgreSQL hashes its own rows, so no
# text output sits between the two sides.
# One digest per tenant a replica holds: acme's rows and the shared (open-tenant) rows.
PG_DIGEST_SQL = (f"SELECT count(*), encode(sha256(coalesce(string_agg(convert_to(id::text || ':' || "
                 f"CASE WHEN note IS NULL THEN 'N' ELSE 'S' || octet_length(note) || ':' || note END, 'UTF8'), '' ORDER BY id), '')), 'hex') "
                 f"FROM public.{TABLE} WHERE tenant_id = '{{tenant}}' AND deleted_at IS NULL")
SHARED_N = 2000  # shared rows -1..-SHARED_N besides row 0; updated and tombstoned under load
SHARED_SQL = f"SELECT CASE WHEN note IS NULL THEN 'N' ELSE 'S' || note END FROM {TABLE} WHERE id = 0 AND tenant_id = '{{open}}'"


def replica_exact(db) -> dict:
    """The replica's ordered rows and its shared row, read from its SQLite file."""
    import sqlite3
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=10)
    try:
        import hashlib
        out = {}
        for key, tenant in (("", TENANT), ("open_", os.environ.get("OPEN_TENANT", "_default"))):
            h, n = hashlib.sha256(), 0
            for i, note in con.execute(f"SELECT id, note FROM {TABLE} WHERE tenant_id = ? ORDER BY id", (tenant,)):
                h.update(frame(i, note))
                n += 1
            out[key + "digest"], out[key + "rows"] = h.hexdigest(), n
        shared = con.execute(SHARED_SQL.format(open=os.environ.get("OPEN_TENANT", "_default"))).fetchone()
        return out | {"shared": shared[0] if shared else None}
    finally:
        con.close()


def catchup_targets(url: str) -> dict:
    """Per stream this client tails, the seq of the last message its filter matches, read
    just before a leg connects: the fixed end of that leg's CDC catch-up (0: nothing to
    catch up). Ongoing traffic after it is live following, not catch-up."""
    out = {}
    open_t = os.environ.get("OPEN_TENANT", "_default")
    for stream, subj in ((f"CDC_{TENANT}", f"cdc.{TENANT}.{TABLE}.>"), ("CDC_PUBLIC", f"cdc.{open_t}.{TABLE}.>")):
        r = subprocess.run(["nats", "--server", url, "req", f"$JS.hub.API.STREAM.MSG.GET.{stream}", json.dumps({"last_by_subj": subj})],
                           capture_output=True, text=True, timeout=15)
        try:
            response = json.loads(r.stdout)
            if r.returncode != 0:
                raise ValueError("CLI request failed")
            # Only JetStream's explicit "no message found" answer means no work.
            # A missing stream, denied request or broken CLI is not an empty stream.
            if response.get("error", {}).get("err_code") == 10037 and response["error"].get("code") == 404:
                out[stream] = 0
                continue
            if "error" in response:
                raise ValueError("request failed")
            seq = response["message"]["seq"]
            if isinstance(seq, bool) or not isinstance(seq, int) or seq <= 0:
                raise ValueError("invalid message sequence")
            out[stream] = seq
        except (json.JSONDecodeError, ValueError, KeyError, TypeError, AttributeError) as error:
            raise RuntimeError(f"Cannot measure {stream}: target lookup failed (exit {r.returncode}): {r.stdout[-500:]} {r.stderr[-500:]}") from error
    return out


def measure_env(targets: dict) -> dict:
    return {"ZB_MEASURE": "1", "ZB_CATCHUP_TARGETS": ",".join(f"{k}={v}" for k, v in targets.items())}


def pg_time_ms(v: str):
    """A PostgreSQL timestamptz's text (the chain's cutoff_version) as epoch ms."""
    import datetime
    try:
        t = v.replace(" ", "T")
        if t[-3] in "+-" and t[-3:].lstrip("+-").isdigit():
            t += ":00"
        return datetime.datetime.fromisoformat(t).timestamp() * 1000
    except (ValueError, IndexError):
        return None


def hub_gen_at(fresh: list, tenant: str, t_ms: int):
    """The generation the hub's manifest named when a chain was read: the first 1 s sample
    at or after `t_ms`. A chain is never newer than the hub, so the sample before it can be
    stale; this one can overstate "behind" by one generation across a change, never hide it."""
    for row in fresh:
        if row["t_ms"] >= t_ms and row.get(f"hub_{tenant}") is not None:
            return row[f"hub_{tenant}"]
    return None


def seeding_report(name: str, events: list, logtext: str, fresh: list = ()) -> list:
    """Each leg's reconnect work from the clients' ZB_MEASURE lines: the chain (full and
    deltas apart: objects, bytes, fetch / inflate / decode / apply), its age at connect,
    and the CDC catch-up to the targets fixed before connecting, per stream. Download and
    apply overlap on a streamed step, so step times are reported, never summed into the
    total; `target_catchup_s` ends when newest id and every stream target are reached.
    This is delivery/application accounting, not readiness: held events can remain."""
    recs = []
    for line in logtext.splitlines():
        i = line.find("measure {")
        if i >= 0:
            try:
                recs.append(json.loads(line[i + 8:]))
            except json.JSONDecodeError:
                pass
    legs = [e for e in events if e["event"] == "leg"]
    caught = {e["leg"]: e for e in events if e["event"] == "caught_up"}
    out = []
    for k, leg in enumerate(legs):
        lo, hi = leg["start_ms"], (legs[k + 1]["start_ms"] if k + 1 < len(legs) else float("inf"))
        mine = [r for r in recs if lo <= r.get("t0_ms", r.get("at_ms", 0)) < hi]
        chains = [r for r in mine if r["m"] == "chain"]
        steps = [r for r in mine if r["m"] == "step"]
        ups = {r["stream"]: r for r in mine if r["m"] == "catchup"}
        c = caught.get(leg["leg"])
        ends = [lo + c["catchup_s"] * 1000] if c else []
        streams = {}
        for stream, target in leg["targets"].items():
            u = ups.get(stream)
            if target == 0:
                streams[stream] = {"target": 0}
            elif u:
                ends.append(u["at_ms"])
                streams[stream] = {k2: u.get(k2) for k2 in ("target", "via", "messages", "bytes", "events", "applied", "gated", "held", "failed")} | {"reached_s": round((u["at_ms"] - lo) / 1000, 2)}
            else:
                streams[stream] = {"target": target, "reached_s": None}
        unreached = any(v.get("reached_s", 0) is None for v in streams.values())
        target_ms = None if (unreached or not c) else max(ends)
        row = {"client": name, "leg": leg["leg"], "cold": leg["cold"], "targets": leg["targets"],
               "target_catchup_s": None if target_ms is None else round((target_ms - lo) / 1000, 2),
               "chains": [], "streams": streams}
        for ch in chains:
            cut = pg_time_ms(ch.get("cutoff_version", ""))
            # A chain started after the initial targets were reached is a reseed while following
            # live (a gap), not reconnect work: its age is taken at its own start.
            live = target_ms is not None and ch["t0_ms"] > target_ms
            at = ch["t0_ms"] if live else lo
            hub_gen = hub_gen_at(fresh, ch.get("tenant", TENANT), ch["t0_ms"])
            row["chains"].append({**{k2: ch.get(k2) for k2 in ("tenant", "source", "gen", "full_gen", "fulls", "deltas", "rows", "bytes", "fetch_ms", "inflate_ms", "decode_ms", "apply_ms", "streamed", "cutoff_seq", "shared_cutoff_seq")},
                                  "hub_gen": hub_gen, "gens_behind_hub": (hub_gen - ch["gen"]) if hub_gen is not None and ch.get("gen") is not None else None,
                                  "start_s": round((ch["t0_ms"] - lo) / 1000, 2), "ms": ch["t1_ms"] - ch["t0_ms"], "live_reseed": live,
                                  "age_s": round((at - cut) / 1000, 1) if cut else None})
        row["steps"] = [{k2: st.get(k2) for k2 in ("kind", "object", "bytes", "rows", "ms", "fetch_ms", "inflate_ms", "decode_ms", "apply_ms", "streamed")}
                        | {"live_reseed": target_ms is not None and st["t0_ms"] > target_ms} for st in steps]
        out.append(row)
    return out


def print_seeding(rows: list):
    def mb(b):
        return f"{(b or 0) / 1048576:.2f} MB"
    for r in rows:
        print(f"  leg {r['leg']}{' (cold join)' if r['cold'] else ''}: CDC targets and newest ID reached after {r['target_catchup_s']} s (held work may remain); targets {r['targets']}")
        if not [ch for ch in r["chains"] if not ch["live_reseed"]]:
            print("    no chain applied at connect: resumed from the stored position")
        for ch in r["chains"]:
            when = f"RESEED while live at {ch['start_s']} s, age {ch['age_s']} s then" if ch["live_reseed"] else f"age {ch['age_s']} s at connect"
            behind = "" if ch.get("gens_behind_hub") is None else f", hub at g{ch['hub_gen']} ({ch['gens_behind_hub']} behind)"
            print(f"    chain {ch.get('tenant', '')} g{ch['gen']} from {ch.get('source') or '?'} (full g{ch['full_gen']} ×{ch['fulls']}, {ch['deltas']} delta(s)){behind}, {when}: "
                  f"{ch['rows']} rows, {mb(ch['bytes'])}, {ch['ms']} ms from {ch['start_s']} s "
                  f"[fetch {ch['fetch_ms']}, inflate {ch['inflate_ms']}, decode {ch['decode_ms']}, apply {ch['apply_ms']} ms; {ch['streamed']} streamed]")
        for st in r["steps"]:
            print(f"      {st['kind']:5} {st['object']}: {st['rows']} rows, {mb(st['bytes'])}, {st['ms']} ms"
                  + ("" if st["streamed"] else f" [fetch {st['fetch_ms']}, inflate {st['inflate_ms']}, decode {st['decode_ms']}, apply {st['apply_ms']}]"))
        for stream, v in r["streams"].items():
            if v["target"] == 0:
                print(f"    {stream}: nothing to catch up")
            elif v.get("reached_s") is None:
                print(f"    {stream}: target {v['target']} NOT reached")
            else:
                print(f"    {stream}: target {v['target']} reached at {v['reached_s']} s via {v['via']} — {v['messages']} msg(s){f" ({mb(v['bytes'])})" if v.get('bytes') is not None else ''}, {v['events']} event(s): "
                      f"{v['applied']} applied, {v['gated']} gated, {v['held']} held" + (f", {v['failed']} failed" if v.get("failed") else ""))


OPEN_RETRY_S = 300
CUT_CLIENTS = ("cut_libzb", "cut_ts")  # with --cut-at: they join while the leaf is cut off
HUB_PORT, LEAF_PORT, LINK_PORT, LINK_PROXY, CLIENT_PROXY, HUB_HTTP, LEAF_HTTP = 14272, 14273, 14274, 14275, 14276, 14282, 14283
LEAF_NAME = "lf-gen"


class Topology:
    """A hub (JetStream domain `hub`) and a seeding leaf (`leaf`), unauthenticated: the link's
    grants and the keeper's credentials are leaf_security.py's subject, the data path is this
    one's. The leaf's link to the hub always runs through netem.py, so it can be slowed and cut;
    with --via hub the clients reach the hub through the same delay, so in both modes the hub is
    the far end. The leaf's caches are the ones deploy/compose/leaf/mirrors.sh creates: its
    KV_generations sourced from the hub's per-tenant projections (made by the bridge's own
    --prepare-projections), and OBJ_gen-<tenant> mirrored."""

    def __init__(self, tmp: pathlib.Path, via: str, delay_ms: float, mbit: float):
        self.tmp, self.via, self.delay_ms, self.mbit = tmp, via, delay_ms, mbit
        self.procs: dict = {}
        self.bridge_pid = None

    def spawn(self, name, args):
        self.procs[name] = subprocess.Popen(args, stdout=(self.tmp / f"{name}.log").open("w"), stderr=subprocess.STDOUT)

    def wait_log(self, name, text, timeout=20):
        end = time.time() + timeout
        while time.time() < end:
            if text in (self.tmp / f"{name}.log").read_text(errors="replace"):
                return
            time.sleep(0.1)
        sys.exit(f"{name}: no '{text}'")

    def start(self):
        for name, port, http, extra in (
                # no_advertise: otherwise the hub tells the leaf its listen address, and a leaf
                # that loses the relayed link redials the hub directly, around the cut and the delay.
                ("hub", HUB_PORT, HUB_HTTP, f'leafnodes {{ listen: "127.0.0.1:{LINK_PORT}", no_advertise: true }}'),
                ("leaf", LEAF_PORT, LEAF_HTTP, f'leafnodes {{ remotes: [{{ url: "nats://127.0.0.1:{LINK_PROXY}" }}] }}')):
            (self.tmp / f"{name}.conf").write_text(
                f'listen: 127.0.0.1:{port}\nhttp: 127.0.0.1:{http}\nmax_payload: 8388608\n'
                f'jetstream {{ domain: {name}, store_dir: "{self.tmp}/{name}-js", max_file_store: 20G }}\n{extra}\n')
            self.spawn(name, ["nats-server", "-c", str(self.tmp / f"{name}.conf")])
            self.wait_log(name, "Server is ready")
        netem = [sys.executable, str(pathlib.Path(__file__).with_name("netem.py")), "--delay-ms", str(self.delay_ms), "--rate-mbit", str(self.mbit)]
        self.spawn("link", netem + ["--listen", str(LINK_PROXY), "--to", f"127.0.0.1:{LINK_PORT}"])
        self.wait_log("link", "listening")
        self.far = self.via == "hub" and (self.delay_ms or self.mbit)
        if self.far:
            self.spawn("clientlink", netem + ["--listen", str(CLIENT_PROXY), "--to", f"127.0.0.1:{HUB_PORT}"])
            self.wait_log("clientlink", "listening")
        self.hub_url = f"nats://127.0.0.1:{HUB_PORT}"
        self.client_url = f"nats://127.0.0.1:{LEAF_PORT}" if self.via == "leaf" else f"nats://127.0.0.1:{CLIENT_PROXY if self.far else HUB_PORT}"
        cli = ["nats", "--server", self.hub_url]
        for args in (["kv", "add", "schemas", "--history=10", "--replicas=1"], ["kv", "add", "tenants", "--history=1", "--replicas=1"],
                     ["kv", "add", "generations", "--history=1", "--replicas=1"], ["kv", "add", "live", "--history=1", "--ttl=90s", "--replicas=1"],
                     ["stream", "add", "MUTATIONS", "--subjects=mutation.>,mutation_error.>", "--storage=file",
                      "--retention=work", "--max-age=2h", "--max-bytes=1G", "--replicas=1", "--discard=new", "--defaults"],
                     ["stream", "add", "VERDICTS", "--subjects=mutation_ack.>", "--storage=file", "--retention=limits",
                      "--max-age=2h", "--max-bytes=1G", "--replicas=1", "--discard=old", "--max-msgs-per-subject=1",
                      "--allow-direct", "--defaults"]):
            subprocess.run(cli + args, capture_output=True, text=True)
        for _ in range(100):
            if self.varz(True, "leafz").get("leafnodes", 0):
                break
            time.sleep(0.1)
        else:
            sys.exit("the leaf never linked to the hub")

    def varz(self, leaf=False, endpoint="varz"):
        import urllib.request
        return json.load(urllib.request.urlopen(f"http://127.0.0.1:{LEAF_HTTP if leaf else HUB_HTTP}/{endpoint}", timeout=3))

    def req(self, server_url, subject, body) -> dict:
        r = subprocess.run(["nats", "--server", server_url, "req", subject, json.dumps(body)], capture_output=True, text=True, timeout=15)
        try:
            return json.loads(r.stdout)
        except json.JSONDecodeError:
            return {"error": {"description": (r.stdout + r.stderr)[-300:]}}

    def caches(self, bridge_env: dict, bridge_bin) -> None:
        """The leaf's seeding caches, as mirrors.sh makes them, once the hub has a chain."""
        r = subprocess.run([str(bridge_bin), "--mutation-ingress", "--prepare-projections"], capture_output=True, text=True, timeout=60,
                           env=bridge_env | {"ZB_LEAF_TENANTS": TENANT, "NATS_JS_DOMAIN": "hub"})
        if r.returncode:
            sys.exit(f"--prepare-projections failed:\n{(r.stdout + r.stderr)[-1500:]}")
        leaf_url = f"nats://127.0.0.1:{LEAF_PORT}"
        external = {"api": "$JS.hub.API", "deliver": f"$ZB.MIRROR.{LEAF_NAME}"}
        open_t = os.environ.get("OPEN_TENANT", "_default")
        for tenant in (TENANT, open_t):
            origin = self.req(self.hub_url, f"$JS.API.STREAM.INFO.OBJ_gen-{tenant}", {})
            if "config" not in origin:
                sys.exit(f"hub has no OBJ_gen-{tenant} yet: {origin}")
            config = {k: v for k, v in origin["config"].items() if k not in ("subjects", "sources", "mirror", "metadata", "consumer_limits")}
            config |= {"name": f"OBJ_gen-{tenant}", "mirror_direct": False, "mirror": {"name": f"OBJ_gen-{tenant}", "external": external}}
            out = self.req(leaf_url, f"$JS.API.STREAM.CREATE.OBJ_gen-{tenant}", config)
            if "error" in out:
                sys.exit(f"leaf cache OBJ_gen-{tenant}: {out['error']}")
        out = self.req(leaf_url, "$JS.API.STREAM.CREATE.KV_generations", {
            "name": "KV_generations", "subjects": ["$KV.generations.>"], "storage": "file", "allow_direct": True,
            "max_bytes": 64 << 20, "max_msgs_per_subject": 1, "allow_rollup_hdrs": True,
            "sources": [{"name": f"KV_generations_leaf_{t}", "external": external} for t in sorted({TENANT, open_t})]})
        if "error" in out:
            sys.exit(f"leaf cache KV_generations: {out['error']}")

    def manifest_gen(self, leaf: bool, tenant: str):
        """The generation a domain's manifest for (tenant, gen_ids) names right now."""
        r = subprocess.run(["nats", "--server", self.hub_url, "--js-domain", "leaf" if leaf else "hub", "kv", "get", "generations",
                            f"{tenant}.{TABLE}", "--raw"], capture_output=True, text=True, timeout=10)
        try:
            return json.loads(r.stdout).get("gen")
        except json.JSONDecodeError:
            return None

    def usage(self) -> dict:
        """Per server: bytes out/in and messages out (cumulative), and CPU seconds."""
        import burst_tls as bt
        out = {}
        for name in ("hub", "leaf"):
            v = self.varz(name == "leaf")
            out[name] = {"out_bytes": v["out_bytes"], "in_bytes": v["in_bytes"], "out_msgs": v["out_msgs"],
                         "cpu_s": bt.cpu_seconds(self.procs[name].pid)}
        if self.bridge_pid:
            out["hub"]["bridge_cpu_s"] = bt.cpu_seconds(self.bridge_pid)
        return out

    def signal(self, name, sig):
        self.procs[name].send_signal(sig)

    def stop(self):
        for p in self.procs.values():
            if p.poll() is None:
                p.terminate()


def exact_now(db, truth) -> bool:
    """The replica equals PostgreSQL row for row (the done file's digests), read-only."""
    want = truth.get("exact")
    if not want:
        return True
    try:
        have = replica_exact(db)
    except Exception:
        return False
    return all(have.get(k) == v for k, v in want.items())


def matches(l, truth) -> bool:
    return (l.get("count") == l.get("distinct") == truth["count"] and l.get("min") == truth["min"] and l.get("max") == truth["max"]
            and (truth.get("notes") is None or l.get("notes") == truth["notes"]))


# ── the worker: one client, one process ────────────────────────────────────────
def ts_worker(a) -> int:
    """One zb-client-ts client (Node) through its schedule: a process per leg on the same
    replica file — going offline is the process ending. `ready` is printed once connect()
    returns, so the time to it is the connect a browser waits on."""
    import sqlite3
    import burst_tls as bt

    def say(**kv):
        print(json.dumps({"t": round(time.time() - a.t0, 1), **kv}), flush=True)

    def pg_max() -> int:
        return int(bt.psql(f"SELECT coalesce(max(id), 0) FROM public.{TABLE} WHERE tenant_id = '{TENANT}'").stdout.strip() or 0)

    def local() -> dict:
        try:
            con = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True, timeout=2)
            try:
                n, d, lo, hi, notes = con.execute(f"SELECT count(*), count(DISTINCT id), coalesce(min(id), 0), coalesce(max(id), 0), coalesce(sum(length(note)), 0) FROM {TABLE} WHERE tenant_id = '{TENANT}'").fetchone()
                return {"count": n, "distinct": d, "min": lo, "max": hi, "notes": notes}
            finally:
                con.close()
        except Exception as e:
            return {"error": str(e)}

    # Node resolves "localhost" to ::1 first; the scratch server listens on 127.0.0.1 only.
    env = dict(os.environ, NATS_URL=a.url.replace("localhost", "127.0.0.1"), ZB_DB=a.db, ZB_TABLES=TABLE, ZB_PRINCIPAL=f"gen_{a.name}",
               ZB_JS_DOMAIN="hub", ZB_SEED_DOMAIN="leaf" if a.via == "leaf" else "")
    env.pop("ZB_CREDS", None)
    for i, (t_open, t_close) in enumerate(json.loads(a.schedule)):
        while time.time() < a.t0 + t_open:
            time.sleep(0.2)
        target = pg_max()
        targets = catchup_targets(a.hub_url)  # from the hub itself: a cut leaf must not read as 'nothing to catch up'
        t_c = time.time()
        say(event="leg", leg=i, start_ms=int(t_c * 1000), targets=targets, cold=i == 0)
        # A host retries a refused open (the hub unreachable behind a cut leaf, say): every
        # 5 s for up to OPEN_RETRY_S, each refusal said; the leg's clock keeps running.
        while True:
            proc = subprocess.Popen(["node", "--experimental-strip-types", "follow-worker.ts"], cwd=NODE_DIR, env=env | measure_env(targets),
                                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=sys.stderr, text=True)
            first = proc.stdout.readline()
            try:
                ready = json.loads(first).get("ready")
            except json.JSONDecodeError:
                ready = False
            if ready:
                break
            proc.kill()
            if time.time() - t_c > OPEN_RETRY_S:
                say(event="open_failed", leg=i, said=first.strip()[:160]); return 1
            say(event="open_retry", leg=i, said=first.strip()[:160])
            time.sleep(5)
        connect_s = time.time() - t_c
        caught = None
        live = Live(a.rate, say)
        end = (a.t0 + t_close) if t_close is not None else None
        while True:
            now = time.time()
            if caught is None:
                l = local()
                if l.get("max", -1) >= target:
                    caught = now - t_c
                    say(event="caught_up", leg=i, target=target, connect_s=round(connect_s, 2), catchup_s=round(caught, 2),
                        holes=(l["max"] - l["min"] + 1) - l["count"])
                elif now - t_c > 300:
                    say(event="never_caught_up", leg=i, target=target, local=l); break
            elif not pathlib.Path(a.done_file).exists():
                live.sample(pg_max(), local())
            if end is not None and now >= end:
                break
            if end is None and pathlib.Path(a.done_file).exists() and caught is not None:
                break
            time.sleep(0.25)
        if t_close is not None:
            say(event="offline", leg=i, poll_errors=0, **live.report())
            proc.terminate()
            try: proc.wait(timeout=20)
            except subprocess.TimeoutExpired: proc.kill()
            continue
        while not pathlib.Path(a.done_file).exists():
            live.sample(pg_max(), local())
            time.sleep(0.5)
        truth = final_truth(a.done_file)
        t_f = time.time()
        while time.time() - t_f < 180:
            if matches(local(), truth) and exact_now(a.db, truth):
                break
            time.sleep(0.5)
        say(event="final", local=local(), pg=truth, converge_s=round(time.time() - t_f, 1), poll_errors=0, **live.report())
        proc.terminate()
        try: proc.wait(timeout=20)
        except subprocess.TimeoutExpired: proc.kill()
    return 0


def worker(a) -> int:
    """One libzb client through its schedule; JSON lines on stdout, libzb's log on stderr."""
    if a.kind == "ts":
        return ts_worker(a)
    import burst_tls as bt
    lib = ctypes.CDLL(str(LIBZB))
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, extra in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + extra

    def take(ptr):
        try: return json.loads(ctypes.string_at(ptr).decode())
        finally: lib.zb_free(ptr)

    def say(**kv):
        print(json.dumps({"t": round(time.time() - a.t0, 1), **kv}), flush=True)

    def pg_max() -> int:
        out = bt.psql(f"SELECT coalesce(max(id), 0) FROM public.{TABLE} WHERE tenant_id = '{TENANT}'").stdout.strip()
        return int(out or 0)

    def local(h) -> dict:
        r = take(lib.zb_client_query(h, f"SELECT count(*), count(DISTINCT id), coalesce(min(id), 0), coalesce(max(id), 0), coalesce(sum(length(note)), 0) FROM {TABLE} WHERE tenant_id = '{TENANT}'".encode(), b"[]"))
        if "error" in r:
            return {"error": r["error"]}
        n, d, lo, hi, notes = (int(x) for x in r["rows"][0])
        return {"count": n, "distinct": d, "min": lo, "max": hi, "notes": notes}

    sched = json.loads(a.schedule)
    for i, (t_open, t_close) in enumerate(sched):
        while time.time() < a.t0 + t_open:
            time.sleep(0.2)
        target = pg_max()
        targets = catchup_targets(a.hub_url)  # from the hub itself: a cut leaf must not read as 'nothing to catch up'
        os.environ.update(measure_env(targets))  # libzb reads them at each connect
        t_c = time.time()
        say(event="leg", leg=i, start_ms=int(t_c * 1000), targets=targets, cold=i == 0)
        opts = json.dumps({"natsUrl": a.url, "dbPath": a.db, "tables": [TABLE], "heartbeatMs": 0, "jsDomain": "hub",
                           **({"seedDomain": "leaf"} if a.via == "leaf" else {}),
                           "clientId": f"gen-{a.name}", "principal": f"gen_{a.name}"}).encode()
        while not (h := lib.zb_client_connect(opts)):  # the host retries, as for TS above
            if time.time() - t_c > OPEN_RETRY_S:
                say(event="open_failed", leg=i); return 1
            say(event="open_retry", leg=i)
            time.sleep(5)
        synced = take(lib.zb_client_sync(h))
        connect_s = time.time() - t_c
        caught = None
        poll_errors = 0
        live = Live(a.rate, say)
        end = (a.t0 + t_close) if t_close is not None else None
        while True:
            p = take(lib.zb_client_poll(h, 500))
            if p.get("error"):
                poll_errors += 1
            now = time.time()
            if caught is None:
                l = local(h)
                if l.get("max", -1) >= target:
                    caught = now - t_c
                    say(event="caught_up", leg=i, target=target, connect_s=round(connect_s, 2), catchup_s=round(caught, 2),
                        sync_error=synced.get("error"), holes=(l["max"] - l["min"] + 1) - l["count"])
                elif now - t_c > 300:
                    say(event="never_caught_up", leg=i, target=target, local=l); break
            elif not (a.done_file and pathlib.Path(a.done_file).exists()):
                live.sample(pg_max(), local(h))
            if end is not None and now >= end:
                break
            if end is None and a.done_file and pathlib.Path(a.done_file).exists() and caught is not None:
                break
        if t_close is not None:
            say(event="offline", leg=i, poll_errors=poll_errors, **live.report())
            lib.zb_client_close(h)
            continue
        # The last leg: once the emitter is done, poll until this replica equals PostgreSQL.
        while not pathlib.Path(a.done_file).exists():
            take(lib.zb_client_poll(h, 500))
            live.sample(pg_max(), local(h))
        truth = final_truth(a.done_file)
        t_f = time.time()
        while time.time() - t_f < 180:
            take(lib.zb_client_poll(h, 500))
            if matches(local(h), truth) and exact_now(a.db, truth):
                break
        say(event="final", local=local(h), pg=truth, converge_s=round(time.time() - t_f, 1), poll_errors=poll_errors, **live.report())
        lib.zb_client_close(h)
    return 0


# ── the harness ────────────────────────────────────────────────────────────────
def setup_tables(bt) -> None:
    open_tenant = os.environ.get("OPEN_TENANT", "_default")
    bt.psql(f"""CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, tenant_id text NOT NULL, note text,
        updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)""")
    r = bt.psql(f"SELECT step, status, detail FROM zebridge_enable('public.{TABLE}'::regclass, tenant_col => 'tenant_id', "
                f"tombstone_col => 'deleted_at', publication => '{bt.PUB}', dry_run => false) WHERE status = 'ERROR'")
    if r.stdout.strip() or r.returncode != 0:
        sys.exit(f"enable {TABLE} refused: {r.stdout}{r.stderr}")
    bt.psql(f"CREATE TABLE public.{NOISE} (id bigint PRIMARY KEY, payload text, updated_at timestamptz NOT NULL DEFAULT now())")
    r = bt.psql(f"SELECT step, status, detail FROM zebridge_enable('public.{NOISE}'::regclass, public_reason => 'gen_follow noise', "
                f"publication => '{bt.PUB}', dry_run => false) WHERE status = 'ERROR'")
    if r.stdout.strip() or r.returncode != 0:
        sys.exit(f"enable {NOISE} refused: {r.stdout}{r.stderr}")
    # the clients' principals, all in acme (the scratch NATS has no auth: the mapping is the identity)
    values = ", ".join(f"('gen_{n}', '{TENANT}')" for n in [*SCHEDULE, *CUT_CLIENTS])
    bt.psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) VALUES {values}")
    # the one shared row, before anything else
    bt.psql(f"INSERT INTO public.{TABLE} (id, tenant_id, note) VALUES (0, '{open_tenant}', 'shared')")
    # Shared rows that change under load, so CDC_PUBLIC carries catch-up work too.
    bt.psql(f"INSERT INTO public.{TABLE} (id, tenant_id, note) SELECT -g, '{open_tenant}', 's' FROM generate_series(1, {SHARED_N}) g")


DELETE_AFTER_S = 30


def emitter_sql(path: pathlib.Path, seconds: int, rate: int, noise: int, deletes: int = 0, updates: int = 0,
                shared_updates: int = 0, shared_deletes: int = 0) -> None:
    """One psql session: each second `rate` new ids in acme and `noise` public rows, paced on
    the wall clock from the session's own start. `updates`: the previous second's first ids
    get a note whose length changes each second (the final oracle sums note lengths).
    `deletes`: from DELETE_AFTER_S on, the oldest live ids are tombstoned, so the live set
    stays one contiguous range and holes stay measurable."""
    lines = ["SELECT set_config('zb.gen_t0', extract(epoch from clock_timestamp())::text, false);"]
    for s in range(seconds):
        lines.append(f"INSERT INTO public.{TABLE} (id, tenant_id, note) SELECT g, '{TENANT}', 'n' FROM generate_series({s * rate + 1}, {(s + 1) * rate}) g;")
        if updates and s > 0:
            lo = (s - 1) * rate + 1
            lines.append(f"UPDATE public.{TABLE} SET note = repeat('u', {s % 9 + 2}), updated_at = now() WHERE tenant_id = '{TENANT}' AND id BETWEEN {lo} AND {lo + min(updates, rate) - 1};")
        if deletes and s >= DELETE_AFTER_S:
            k = s - DELETE_AFTER_S
            lines.append(f"UPDATE public.{TABLE} SET deleted_at = now(), updated_at = now() WHERE tenant_id = '{TENANT}' AND id BETWEEN {k * deletes + 1} AND {(k + 1) * deletes};")
        # Shared rows: a window that walks down from -1 is updated each second, and the
        # oldest (-SHARED_N upwards) are tombstoned, so CDC_PUBLIC carries both.
        open_t = os.environ.get("OPEN_TENANT", "_default")
        if shared_updates:
            hi = -((s * shared_updates) % SHARED_N) - 1
            lines.append(f"UPDATE public.{TABLE} SET note = repeat('v', {s % 7 + 2}), updated_at = now() WHERE tenant_id = '{open_t}' AND id BETWEEN {max(hi - shared_updates + 1, -SHARED_N)} AND {hi};")
        if shared_deletes and (s + 1) * shared_deletes <= SHARED_N // 2:
            lines.append(f"UPDATE public.{TABLE} SET deleted_at = now(), updated_at = now() WHERE tenant_id = '{open_t}' AND id BETWEEN {-SHARED_N + s * shared_deletes} AND {-SHARED_N + (s + 1) * shared_deletes - 1};")
        if noise:
            lines.append(f"INSERT INTO public.{NOISE} (id, payload) SELECT g, 'x' FROM generate_series({s * noise + 1}, {(s + 1) * noise}) g;")
        lines.append(f"SELECT pg_sleep(greatest(0, current_setting('zb.gen_t0')::float8 + {s + 1} - extract(epoch from clock_timestamp())));")
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seconds", type=int, default=200)
    ap.add_argument("--rate", type=int, default=200, help="new ids per second in acme")
    ap.add_argument("--noise", type=int, default=500, help="public rows per second (prunes CDC_PUBLIC)")
    ap.add_argument("--update-rate", type=int, default=0, help="ids updated per second (the previous second's)")
    ap.add_argument("--delete-rate", type=int, default=0, help=f"oldest live ids tombstoned per second, from {DELETE_AFTER_S} s")
    ap.add_argument("--shared-update-rate", type=int, default=20, help="shared (open-tenant) rows updated per second")
    ap.add_argument("--shared-delete-rate", type=int, default=2, help=f"shared rows tombstoned per second, up to half of {SHARED_N}")
    ap.add_argument("--via", choices=("hub", "leaf"), default="hub", help="clients connect to the hub, or to a seeding leaf")
    ap.add_argument("--link-delay-ms", type=float, default=0, help="one-way delay to the hub (the leaf's link, or the clients' path with --via hub)")
    ap.add_argument("--link-mbit", type=float, default=0, help="bandwidth to the hub, Mbit/s per direction (0: unlimited)")
    ap.add_argument("--cut-at", type=float, help="seconds into the load: cut the leaf's link to the hub (--via leaf)")
    ap.add_argument("--cut-for", type=float, default=40, help="seconds the link stays cut")
    ap.add_argument("--hub-url", help=argparse.SUPPRESS)
    ap.add_argument("--worker", help=argparse.SUPPRESS)
    ap.add_argument("--t0", type=float, help=argparse.SUPPRESS)
    ap.add_argument("--url", help=argparse.SUPPRESS)
    ap.add_argument("--db", help=argparse.SUPPRESS)
    ap.add_argument("--name", help=argparse.SUPPRESS)
    ap.add_argument("--kind", default="libzb", help=argparse.SUPPRESS)
    ap.add_argument("--schedule", help=argparse.SUPPRESS)
    ap.add_argument("--done-file", help=argparse.SUPPRESS)
    a = ap.parse_args()
    if a.worker:
        return worker(a)

    import burst_tls as bt
    if not LIBZB.exists():
        sys.exit(f"{LIBZB} missing: cd libzb && zig build -Doptimize=ReleaseFast")
    bt.keep_awake()
    watch = bt.SleepWatch()
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="gen-follow-"))
    bt.setup_database()
    setup_tables(bt)
    topo = Topology(tmp, a.via, a.link_delay_ms, a.link_mbit)
    topo.start()
    url = topo.hub_url
    env = {k: v for k, v in os.environ.items() if k not in ("NATS_BRIDGE_NKEY_SEED", "NATS_CREDS", "ZB_SIGNING_SEED", "NATS_TLS_CA")}
    env.update({
        "DATABASE_READER_URL": bt.db_url(os.environ["DATABASE_READER_URL"], bt.DB),
        "DATABASE_WRITER_URL": bt.db_url(os.environ["DATABASE_WRITER_URL"], bt.DB),
        "NATS_URL": url, "NATS_JS_DOMAIN": "hub", "LOG_LEVEL": "info",
        "GENERATIONS_ENABLED": "true", "GENERATION_CADENCE_SECONDS": str(CADENCE_S),
        "CDC_MAX_AGE_SECONDS": str(CDC_AGE_S), "CDC_MAX_MSGS": str(CDC_MSGS),
    })
    blog = tmp / "bridge.log"
    bridge = subprocess.Popen([str(bt.BRIDGE), "--pub", bt.PUB, "--slot", bt.SLOT, "--port", str(bt.HTTP_PORT)],
                              env=env, stdout=blog.open("w"), stderr=subprocess.STDOUT)
    topo.bridge_pid = bridge.pid
    workers: dict = {}
    try:
        for _ in range(300):
            if "Replication started" in blog.read_text(errors="replace"):
                break
            if bridge.poll() is not None:
                sys.exit(f"the bridge exited:\n{blog.read_text(errors='replace')[-1500:]}")
            time.sleep(0.2)
        else:
            sys.exit(f"the bridge never became ready:\n{blog.read_text(errors='replace')[-1500:]}")
        time.sleep(CADENCE_S + 5)  # a first chain of every pair, the shared row in it
        if a.via == "leaf":
            topo.caches(env, bt.BRIDGE)
        # Mirror freshness, sampled each second: the generation each domain's manifest names.
        fresh: list = []
        stop_sampler = threading.Event()

        def sampler():
            while not stop_sampler.is_set():
                row = {"t_ms": int(time.time() * 1000)}
                try:
                    row["linked"] = topo.varz(True, "leafz").get("leafnodes", 0) > 0
                except OSError:
                    row["linked"] = None
                for tenant in (TENANT, os.environ.get("OPEN_TENANT", "_default")):
                    row[f"hub_{tenant}"] = topo.manifest_gen(False, tenant)
                    if a.via == "leaf":
                        row[f"leaf_{tenant}"] = topo.manifest_gen(True, tenant)
                fresh.append(row)
                stop_sampler.wait(1)
        threading.Thread(target=sampler, daemon=True).start()
        usage0 = topo.usage()
        load = tmp / "emit.sql"
        emitter_sql(load, a.seconds, a.rate, a.noise, a.delete_rate, a.update_rate, a.shared_update_rate, a.shared_delete_rate)
        done_file = tmp / "done"
        t0 = time.time()
        emitter = subprocess.Popen([bt.PSQL, bt.db_url(bt.ADMIN_URL, bt.DB), "-q", "-f", str(load)],
                                   stdout=subprocess.DEVNULL, stderr=(tmp / "emit.err").open("w"))
        schedule = dict(SCHEDULE)
        if a.cut_at is not None:
            if a.via != "leaf":
                sys.exit("--cut-at cuts the leaf's link: it needs --via leaf")
            # Two clients join while the leaf is cut off: its mirrors are frozen and the hub
            # is out of reach through it. They must wait, not report a replica they lack.
            schedule |= {name: (name.split("_")[1], [(a.cut_at + 5, None)]) for name in CUT_CLIENTS}

            def cutter():
                time.sleep(max(0, t0 + a.cut_at - time.time()))
                topo.signal("link", signal.SIGUSR1)
                print(f"link cut at {time.time() - t0:.1f} s", flush=True)
                time.sleep(a.cut_for)
                topo.signal("link", signal.SIGUSR2)
                print(f"link restored at {time.time() - t0:.1f} s", flush=True)
            threading.Thread(target=cutter, daemon=True).start()
        for name, (kind, sched) in schedule.items():
            wdir = tmp / name
            wdir.mkdir()
            workers[name] = (subprocess.Popen(
                [sys.executable, __file__, "--worker", "1", "--t0", str(t0), "--url", topo.client_url, "--hub-url", topo.hub_url, "--via", a.via, "--db", str(wdir / "replica.sqlite3"),
                 "--name", name, "--kind", kind, "--schedule", json.dumps(sched), "--done-file", str(done_file), "--rate", str(a.rate)],
                stdout=(wdir / "events.jsonl").open("w"), stderr=(wdir / "libzb.log").open("w"), env=os.environ.copy()), wdir)
        emitter.wait()
        usage1 = topo.usage()
        n, lo, hi, notes = (int(x) for x in bt.psql(f"SELECT count(*), coalesce(min(id), 0), coalesce(max(id), 0), coalesce(sum(length(note)), 0) "
                                                     f"FROM public.{TABLE} WHERE tenant_id = '{TENANT}' AND deleted_at IS NULL").stdout.strip().split("|"))
        # The exact truth: PostgreSQL's ordered live rows, and its shared row. In the done
        # file too, so each worker waits for its replica to equal it, not just acme's counts:
        # a stream still healing at the end must not read as converged.
        pg_n, pg_digest = bt.psql(PG_DIGEST_SQL.format(tenant=TENANT)).stdout.strip().split("|")
        pg_n = int(pg_n)
        pg_open_n, pg_open_digest = bt.psql(PG_DIGEST_SQL.format(tenant=os.environ.get("OPEN_TENANT", "_default"))).stdout.strip().split("|")
        pg_open_n = int(pg_open_n)
        pg_shared = bt.psql(SHARED_SQL.format(open=os.environ.get("OPEN_TENANT", "_default")).replace(f"FROM {TABLE}", f"FROM public.{TABLE}")).stdout.strip()
        done_file.write_text(json.dumps({"count": n, "min": lo, "max": hi, "notes": notes,
                                         "exact": {"digest": pg_digest, "rows": pg_n, "open_digest": pg_open_digest, "open_rows": pg_open_n, "shared": pg_shared}}))
        print(f"emitter done: {hi} ids in {TENANT}, {n} live, over {time.time() - t0:.0f} s ({a.rate}/s, updates {a.update_rate}/s, deletes {a.delete_rate}/s, noise {a.noise}/s)")
        exits = {}
        for name, (p, _) in workers.items():
            try:
                exits[name] = p.wait(timeout=600)
            except subprocess.TimeoutExpired:
                p.kill()
                exits[name] = "timeout"

        # ── the report ──
        failed = 0
        stop_sampler.set()
        print(f"via {a.via}, link {a.link_delay_ms} ms one way, {a.link_mbit or 'unlimited'} Mbit/s")
        for srv in ("hub", "leaf"):
            d = {k: usage1[srv][k] - usage0[srv][k] for k in usage0[srv]}
            print(f"  {srv}: out {d['out_bytes'] / 1048576:.1f} MB ({d['out_msgs']} msgs), in {d['in_bytes'] / 1048576:.1f} MB, nats-server CPU {d['cpu_s']:.1f} s"
                  + (f", bridge CPU {d['bridge_cpu_s']:.1f} s" if "bridge_cpu_s" in d else "") + " over the load")
        print("usage " + json.dumps({"via": a.via, "delay_ms": a.link_delay_ms, "mbit": a.link_mbit,
                                     **{srv: {k: usage1[srv][k] - usage0[srv][k] for k in usage0[srv]} for srv in usage0}}))
        if a.via == "leaf":
            for tenant in (TENANT, os.environ.get("OPEN_TENANT", "_default")):
                rows = [r for r in fresh if r.get(f"hub_{tenant}") is not None]
                behind = [r[f"hub_{tenant}"] - (r.get(f"leaf_{tenant}") or 0) for r in rows]
                print(f"  leaf manifest freshness {tenant}: {len(rows)} samples, behind the hub in {sum(b > 0 for b in behind)}, most {max(behind, default=0)} generation(s)")
            down = [r for r in fresh if r.get("linked") is False]
            print(f"  leaf link down in {len(down)} of {len(fresh)} samples"
                  + (f", from t={(down[0]['t_ms'] / 1000 - t0):.0f} s to t={(down[-1]['t_ms'] / 1000 - t0):.0f} s" if down else ""))
        print(f"PostgreSQL: {pg_n} live rows in {TENANT}, digest {pg_digest[:12]}; {pg_open_n} live shared rows, digest {pg_open_digest[:12]}; row 0 {pg_shared!r}")
        for name, (_, wdir) in workers.items():
            events = [json.loads(l) for l in (wdir / "events.jsonl").read_text().splitlines() if l.strip()]
            # A client passes only by exiting cleanly with exactly one final result whose
            # replica equals PostgreSQL row for row, shared row included: a crash or a timeout
            # before `final` must fail the run, not vanish from it.
            finals = [e for e in events if e["event"] == "final"]
            try:
                exact = replica_exact(wdir / "replica.sqlite3")
            except Exception as err:
                exact = {"error": str(err)}
            exact_ok = (exact.get("digest") == pg_digest and exact.get("rows") == pg_n and exact.get("shared") == pg_shared
                        and exact.get("open_digest") == pg_open_digest and exact.get("open_rows") == pg_open_n)
            if exits.get(name) != 0 or len(finals) != 1 or not exact_ok:
                failed += 1
                print(f"\n✗ {name}: exit {exits.get(name)}, {len(finals)} final result(s), exact rows "
                      f"{'match' if exact_ok else 'DIFFER: ' + json.dumps({k: (v[:12] if k == 'digest' else v) for k, v in exact.items()})}")
            else:
                print(f"\n✓ {name}: exit 0, one final result, {exact['rows']} acme + {exact['open_rows']} shared rows equal PostgreSQL's in order (id, note), row 0 present")
            logtext = (wdir / "libzb.log").read_text(errors="replace")
            counts = {k: logtext.count(v) for k, v in MARKS.items()}
            print(f"\n{name} ({schedule[name][0]})  schedule {schedule[name][1]}")
            seeding = seeding_report(name, events, logtext, fresh)
            if name in CUT_CLIENTS:
                # Joined while the leaf was cut off: reaching the newest id before the link
                # came back would mean a replica reported complete that cannot be.
                restored = a.cut_at + a.cut_for
                caught = [e for e in events if e["event"] == "caught_up"]
                retries = sum(e["event"] == "open_retry" for e in events)
                if caught and caught[0]["t"] < restored:
                    failed += 1
                    print(f"  ✗ caught up at t={caught[0]['t']} s, before the link was restored at {restored} s")
                elif caught:
                    print(f"  ✓ waited through the cut: {retries} refused open(s), caught up at t={caught[0]['t']} s, {caught[0]['t'] - restored:.1f} s after the restore at {restored} s")
            print_seeding(seeding)
            for row in seeding:
                print("seeding " + json.dumps(row))
            for e in events:
                if e["event"] == "caught_up":
                    flag = "  ⚠️ slow" if e["catchup_s"] > SLOW_S else ""
                    print(f"  t={e['t']:>6}s leg {e['leg']}: connect{'' if schedule[name][0] == 'ts' else '+sync'} {e['connect_s']} s{'  ⚠️ slow connect' if e['connect_s'] > SLOW_S else ''}, caught up with max(id) {e['target']} after {e['catchup_s']} s, holes {e.get('holes')}{flag}")
                elif e["event"] == "offline" and e.get("live_samples"):
                    print(f"         live until offline: lag p50 {e['lag_p50_s']} s, p99 {e['lag_p99_s']} s, max {e['lag_max_s']} s, holes max {e['holes_max']}")
                elif e["event"] == "final":
                    l, truth = e["local"], e["pg"] if isinstance(e["pg"], dict) else {"count": e["pg"], "min": 1, "max": e["pg"], "notes": None}
                    ok = matches(l, truth)
                    failed += 0 if ok else 1
                    if e.get("live_samples"):
                        print(f"  live: lag p50 {e['lag_p50_s']} s, p99 {e['lag_p99_s']} s, max {e['lag_max_s']} s, holes max {e['holes_max']} ({e['live_samples']} samples)")
                    print(f"  final: {'✓' if ok else '✗'} count {l.get('count')} distinct {l.get('distinct')} min {l.get('min')} max {l.get('max')} notes {l.get('notes')} "
                          f"(PostgreSQL {truth}), converged in {e['converge_s']} s, poll errors {e['poll_errors']}")
                elif e["event"] in ("never_caught_up", "open_failed"):
                    failed += 1
                    print(f"  ✗ {e}")
            print("  log: " + ", ".join(f"{k} {v}" for k, v in counts.items() if v or schedule[name][0] == "libzb" and k in ("seeded", "gap healed")))
        slept = watch.slept_s()
        if slept > 5:
            print(f"\n⚠️ INVALID: the machine slept {slept:.0f} s during the run")
        print(f"\nrun directory: {tmp}")
        return 1 if failed else 0
    finally:
        for p, _ in workers.values():
            if p.poll() is None:
                p.kill()
        bridge.terminate()
        try:
            bridge.wait(timeout=20)
        except subprocess.TimeoutExpired:
            bridge.kill()
        topo.stop()
        bt.drop_scratch()


if __name__ == "__main__":
    sys.exit(main())
