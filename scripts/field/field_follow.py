#!/usr/bin/env python3
"""gen_follow on the real hub: clients near a leaf, the hub far, a busy table, exact replicas.

    field_follow.py --via leaf     # the clients dial the Canada leaf, chains from its mirrors
    field_follow.py --via hub      # the same clients dial the hub directly (94 ms away)

The clients run on the Canada VPS (field_worker.py over ssh, libzb 0.2.7 through its C ABI);
the load comes from this Mac into Supabase's storm_items (tenant `storm`, guarded: deletes
are tombstones by contract, so the delta path may apply). One minute of idle counters first
(the demo and the airports responder share the hub), then five clients through one schedule:

    steady  connected throughout
    cold    joins one minute in, from nothing
    away1   offline for one minute    (inside one cadence: usually CDC)
    away4   offline for four minutes  (one or two cuts while away: the delta path)
    away8   offline for eight minutes (past the 6 min retention at a 120 s cadence: a gap)

At the end, PostgreSQL's framed digest of the tenant's live rows (ORDER BY id COLLATE "C",
SQLite's byte order) goes to the VPS and every replica must equal it. The report: each leg
(chain, steps, catch-up per stream, delta choices: gen_follow's), the server counters (hub
nats-server and bridge CPU, hub and leaf bytes), and the exact check.

Before the first run: the setup in docs/plans/field-measurements.md (venv, libzb, invites).
Needs .env.supabase (SB_ADMIN_URL) in the repository root.

    field_follow.py --target eval --via hub   # the compose-hub evaluation (docs/plans/compose-hub-evaluation.md)

--target eval: the hub is the evaluation VPS (compose or systemd, the same counters), its
database the Dublin project (.env.supa.dublin), and the clients run on this Mac
(~/zb-field-eval: invites/, ids/, runs/), so they take no CPU from the hub measured.
"""
import argparse, json, os, pathlib, re, subprocess, sys, time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "scenarios"))
import gen_follow  # noqa: E402  seeding_report, print_seeding

HUB, CANADA = "debian@141.95.95.12", "debian@51.222.37.223"
LEAF_NATS, HUB_NATS = "tls://t-nats.zebridge.eu:4222", "tls://nats.zebridge.eu:4222"
BRIDGE_URL, ENV_FILE, JS_DOMAIN = "https://bridge.zebridge.eu", ".env.supabase", "hub"
SEED_DOMAIN, TENANT, TABLE = "scratch", "storm", "storm_items"
REMOTE = "field"   # ~/field on the VPS: venv/, lib/libzbcore.so, invites/<name>.code, ids/, runs/
LOCAL = False      # the clients on this Mac (--target eval), not over ssh


def use_eval():
    """--target eval: the hub is the evaluation VPS, the clients run here."""
    global HUB, CANADA, HUB_NATS, BRIDGE_URL, ENV_FILE, JS_DOMAIN, REMOTE, LOCAL
    HUB, CANADA = "debian@51.222.37.223", "local"
    HUB_NATS, BRIDGE_URL = "tls://t-nats.zebridge.eu:4222", "https://t-bridge.zebridge.eu"
    ENV_FILE, JS_DOMAIN, REMOTE, LOCAL = ".env.supa.dublin", "", "zb-field-eval", True
SCHEDULE = {
    "steady": [[20, None]],
    "cold": [[60, None]],
    "away1": [[20, 300], [360, None]],
    "away4": [[20, 300], [540, None]],
    "away8": [[20, 240], [720, None]],
}
ROLES = ["steady"] * 6 + ["cold", "away1", "away4", "away8"]  # the extra clients, one in ten each


def extra_clients(n):
    """Clients beyond the five measured ones: c006…c<n>, their role by position, one
    schedule per role (SCHEDULE's), unmeasured, ten to a process."""
    return [{"name": f"c{i:03d}", "schedule": SCHEDULE[ROLES[i % 10]]} for i in range(6, n + 1)]


DIGEST_SQL = (f"SELECT count(*), encode(sha256(coalesce(string_agg(convert_to(id || ':' || "
              f"CASE WHEN note IS NULL THEN 'N' ELSE 'S' || octet_length(note) || ':' || note END, 'UTF8'), '' ORDER BY id COLLATE \"C\"), '')), 'hex') "
              f"FROM public.{TABLE} WHERE tenant_id = '{TENANT}' AND deleted_at IS NULL")


def ssh(host, cmd, check=True, input=None):
    # "local": the same command in a shell here, from the home directory as ssh would be.
    if host == "local":
        r = subprocess.run(["sh", "-c", cmd], capture_output=True, text=True, input=input, cwd=os.path.expanduser("~"))
    else:
        # -n unless there is input: ssh then never waits on this process's stdin.
        r = subprocess.run(["ssh", "-o", "BatchMode=yes"] + ([] if input is not None else ["-n"]) + [host, cmd],
                           capture_output=True, text=True, input=input)
    if check and r.returncode:
        sys.exit(f"ssh {host}: {cmd[:80]}… failed:\n{r.stderr[-800:]}")
    return r.stdout


def supabase_env():
    env = dict(os.environ)
    for line in (ROOT / ENV_FILE).read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def psql(env, sql):
    r = subprocess.run(["psql", env["SB_ADMIN_URL"], "-X", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql], capture_output=True, text=True, env=env)
    if r.returncode:
        sys.exit(f"psql failed: {r.stderr[-500:]}")
    return r.stdout.strip()


def load_sql(run, seconds, ins, upd, dele, delete_after=30):
    """One session: each second `ins` new rows of this run, the previous second's first `upd`
    rows updated, and from `delete_after` the oldest `dele` of this run's rows tombstoned;
    paced on the wall clock. Only this run's ids change, the tenant's base rows stay."""
    key = lambda n: f"'f{run}-' || lpad(g::text, 7, '0')"
    ids = lambda lo, hi: f"(SELECT {key(0)} FROM generate_series({lo}, {hi}) g)"
    out = ["SELECT set_config('zb.t0', extract(epoch from clock_timestamp())::text, false);"]
    for s in range(seconds):
        out.append(f"INSERT INTO public.{TABLE} (id, tenant_id, note) SELECT {key(0)}, '{TENANT}', 'n' FROM generate_series({s * ins + 1}, {(s + 1) * ins}) g;")
        if upd and s > 0:
            lo = (s - 1) * ins + 1
            out.append(f"UPDATE public.{TABLE} SET note = repeat('u', {s % 9 + 2}) WHERE id IN {ids(lo, lo + min(upd, ins) - 1)};")
        if dele and s >= delete_after:
            k = s - delete_after
            out.append(f"UPDATE public.{TABLE} SET deleted_at = now() WHERE id IN {ids(k * dele + 1, (k + 1) * dele)};")
        out.append(f"SELECT pg_sleep(greatest(0, current_setting('zb.t0')::float8 + {s + 1} - extract(epoch from clock_timestamp())));")
    return "\n".join(out) + "\n"


def counters():
    """Hub: nats-server, bridge, admission and Docker CPU seconds, nats bytes/messages. Leaf:
    bytes/messages. The same commands for a systemd hub and a compose one: a container's
    processes are the host's, found by name; /varz by curl, or inside the container when the
    monitoring port is not published."""
    def cpu(host, pid_cmd):
        t = ssh(host, f"ps -o times= -p $({pid_cmd})", check=False).strip()
        return int(t) if t.isdigit() else None
    varz = ("curl -sf 127.0.0.1:8222/varz || sudo docker exec $(sudo docker ps -qf label=com.docker.compose.service=nats) "
            "wget -qO- http://127.0.0.1:8222/varz")
    hub = json.loads(ssh(HUB, varz))
    # Docker's own cost, which a systemd hub does not pay: the daemons, each container's
    # shim, and docker-proxy relaying the published ports (4222, 80, 443).
    dk = ssh(HUB, "ps -C dockerd,containerd,containerd-shim-runc-v2,docker-proxy -o times= | awk '{s+=$1} END {print s+0}'",
             check=False).strip()
    # The whole machine's busy CPU (/proc/stat: user, nice, system, irq, softirq, steal), in
    # seconds: what no process accounts for, the kernel's NAT and connection tracking for
    # Docker's published ports above all, counts here. Hundredths of a second per tick.
    host = ssh(HUB, "awk '/^cpu /{print ($2+$3+$4+$7+$8+$9)/100}' /proc/stat", check=False).strip()
    # The leaf's traffic, exactly: the hub's own count of its link to the Canada leaf
    # (/leafz, per leaf connection; "out" = hub → leaf). Docker's per-container counters
    # disagreed with themselves (2026-10-22: +30 MB in, +310 MB out for a leaf relaying ~300 MB).
    leafz = {} if LOCAL else json.loads(ssh(HUB, "curl -s '127.0.0.1:8222/leafz'"))
    link = next((l for l in leafz.get("leafs", []) if l.get("ip") == CANADA.split("@")[1]), {})
    leaf = {"out_bytes": link.get("out_bytes"), "in_bytes": link.get("in_bytes"), "out_msgs": link.get("out_msgs")}
    return {"t": time.time(),
            "hub": {"out_bytes": hub["out_bytes"], "in_bytes": hub["in_bytes"], "out_msgs": hub["out_msgs"],
                    "nats_cpu_s": cpu(HUB, "pidof nats-server"),
                    "bridge_cpu_s": cpu(HUB, "pgrep -xf '(/usr/bin/)?bridge'"),
                    "admission_cpu_s": cpu(HUB, "pgrep -xf '(/usr/bin/)?bridge --mutation-ingress'"),
                    "docker_cpu_s": int(dk) if dk.isdigit() else None,
                    "host_cpu_s": round(float(host), 1) if host else None},
            "link_to_leaf": {"out_bytes": leaf["out_bytes"], "in_bytes": leaf["in_bytes"], "out_msgs": leaf["out_msgs"]},
            "leaf": {"nats_cpu_s": None if LOCAL else cpu(CANADA, "pidof nats-server")}}


def delta(a, b):
    out = {}
    for side in ("hub", "link_to_leaf", "leaf"):
        out[side] = {k: (b[side][k] - a[side][k]) if a[side][k] is not None and b[side][k] is not None else None for k in a[side]}
    out["seconds"] = round(b["t"] - a["t"], 1)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--via", choices=("leaf", "hub"), required=True)
    ap.add_argument("--target", choices=("field", "eval"), default="field",
                    help="field: the production hub, clients on the Canada VPS; eval: the evaluation VPS, clients here")
    ap.add_argument("--seconds", type=int, default=900)
    ap.add_argument("--inserts", type=int, default=200)
    ap.add_argument("--updates", type=int, default=50)
    ap.add_argument("--deletes", type=int, default=50)
    ap.add_argument("--out", default=str(ROOT / "docs" / "bench" / "field"))
    ap.add_argument("--clients", type=int, default=5, help="5 measured clients, plus c006…cN (unmeasured, ten per process)")
    ap.add_argument("--enroll-only", action="store_true", help="redeem the extra clients' invites, keep their identities, and stop")
    ap.add_argument("--enroll-parallel", type=int, default=8, help="with --enroll-only: how many redeem at once")
    a = ap.parse_args()
    if a.target == "eval":
        if a.via == "leaf":
            sys.exit("--target eval has no leaf: --via hub")
        use_eval()

    run = time.strftime("%m%d%H%M") + a.via[0] + ("e" if LOCAL else "")
    out = pathlib.Path(a.out) / run
    out.mkdir(parents=True, exist_ok=True)
    env = supabase_env()
    # Preflight: the VPS has what the clients need.
    extra = extra_clients(a.clients)
    names = list(SCHEDULE) + [c["name"] for c in extra]
    missing = ssh(CANADA, f"cd ~/{REMOTE} && for f in venv/bin/python lib/libzbcore.so {' '.join(f'invites/{n}.code' for n in names)}; do "
                          f"[ -e $f ] || [ -e ids/$(basename $f .code).identity ] || echo $f; done", check=False).split()
    if missing:
        sys.exit(f"missing on the VPS: {missing} — see docs/plans/field-measurements.md")
    worker = str(pathlib.Path(__file__).with_name("field_worker.py"))
    if LOCAL:
        subprocess.run(["cp", worker, os.path.expanduser(f"~/{REMOTE}/field_worker.py")], check=True)
    else:
        subprocess.run(["scp", "-q", worker, f"{CANADA}:{REMOTE}/field_worker.py"], check=True)
    ssh(CANADA, f"mkdir -p {REMOTE}/ids {REMOTE}/runs/{run} && chmod 700 {REMOTE}/ids")
    if a.enroll_only:
        ssh(CANADA, f"cat > {REMOTE}/runs/{run}/enroll.json", input=json.dumps(extra))
        out_text = ssh(CANADA, f"cd ~/{REMOTE} && venv/bin/python field_worker.py --batch runs/{run}/enroll.json --enroll-only "
                               f"--lib lib/libzbcore.so --db 'runs/{run}/{{name}}.sqlite3' --identity 'ids/{{name}}.identity' "
                               f"--invite-file 'invites/{{name}}.code' --nats {HUB_NATS} --hub-nats {HUB_NATS} --bridge {BRIDGE_URL} --js-domain '{JS_DOMAIN}' --t0 0 --done-file /dev/null --enroll-parallel {a.enroll_parallel} "
                               f"2> runs/{run}/enroll.log; rm -f runs/{run}/c*.sqlite3*", check=False)
        rows = [json.loads(l) for l in out_text.splitlines() if l.strip()]
        summary = next((r for r in rows if r.get("summary")), {})
        rows = [r for r in rows if not r.get("summary")]
        from collections import Counter
        done = [r for r in rows if r["enroll"] == "enrolled"]
        print(f"{Counter(r['enroll'] for r in rows)} in {summary.get('seconds')} s, {summary.get('parallel')} at once"
              + (f"; {len(done) / summary['seconds']:.1f} enrollments/s" if done and summary.get("seconds") else ""))
        refusals = Counter()
        for r in rows:
            refusals.update(r.get("refusals", {}))
        for why, n in refusals.most_common(5):
            print(f"  refused {n}×: {why or '(no reason given)'}")
        for r in rows:
            if r["enroll"] == "failed":
                print("  failed:", r)
        return 0 if all(r["enroll"] != "failed" for r in rows) else 1

    print(f"run {run}: via {a.via}, {a.seconds} s of load ({a.inserts} inserts, {a.updates} updates, {a.deletes} deletes /s)", flush=True)
    c_idle0 = counters()
    time.sleep(60)
    c_idle1 = counters()
    t0 = time.time() + 15
    nats = LEAF_NATS if a.via == "leaf" else HUB_NATS
    seed = f"--seed-domain {SEED_DOMAIN}" if a.via == "leaf" else ""
    # All five in one ssh call, each detached in its own session (setsid -f): a backgrounded
    # process that still held the session kept `ssh` from returning, and the launch stalled
    # after the first worker (2026-10-22).
    launches = []
    for name, sched in SCHEDULE.items():
        launches.append(f"setsid -f venv/bin/python field_worker.py --name {name} --lib lib/libzbcore.so "
                        f"--db runs/{run}/{name}.sqlite3 --identity ids/{name}.identity --invite-file invites/{name}.code "
                        f"--nats {nats} --hub-nats {HUB_NATS} --bridge {BRIDGE_URL} --js-domain '{JS_DOMAIN}' {seed} --tenant {TENANT} --schedule '{json.dumps(sched)}' --t0 {t0} "
                        f"--done-file runs/{run}/done > runs/{run}/{name}.events 2> runs/{run}/{name}.log < /dev/null")
    for k in range(0, len(extra), 10):
        group = extra[k:k + 10]
        ssh(CANADA, f"cat > {REMOTE}/runs/{run}/batch{k // 10}.json", input=json.dumps(group))
        launches.append(f"setsid -f venv/bin/python field_worker.py --batch runs/{run}/batch{k // 10}.json --lib lib/libzbcore.so "
                        f"--db 'runs/{run}/{{name}}.sqlite3' --identity 'ids/{{name}}.identity' --invite-file 'invites/{{name}}.code' "
                        f"--nats {nats} --hub-nats {HUB_NATS} --bridge {BRIDGE_URL} --js-domain '{JS_DOMAIN}' {seed} --tenant {TENANT} --t0 {t0} "
                        f"--done-file runs/{run}/done > runs/{run}/batch{k // 10}.events 2> runs/{run}/batch{k // 10}.log < /dev/null")
    if LOCAL:
        # No setsid on macOS: each worker in its own session through Popen, detached.
        for cmd in launches:
            subprocess.Popen(["sh", "-c", cmd.removeprefix("setsid -f ")], cwd=os.path.expanduser(f"~/{REMOTE}"),
                             start_new_session=True, stdin=subprocess.DEVNULL)
        time.sleep(1)
        print("workers running: " + ssh(CANADA, "pgrep -f field_worker.py | wc -l").strip(), flush=True)
    else:
        print("workers running: " + ssh(CANADA, f"cd ~/{REMOTE} && " + " && ".join(launches) + f" && sleep 1 && pgrep -fc field_worker.py").strip(), flush=True)
    while time.time() < t0:
        time.sleep(0.5)
    c0 = counters()
    sql = out / "load.sql"
    sql.write_text(load_sql(run, a.seconds, a.inserts, a.updates, a.deletes))
    load = subprocess.run(["psql", env["SB_ADMIN_URL"], "-X", "-q", "-v", "ON_ERROR_STOP=1", "-f", str(sql)],
                          capture_output=True, text=True, env=env)
    c1 = counters()
    if load.returncode:
        print(f"⚠️ the load stopped early: {load.stderr[-400:]}")
    n, digest = psql(env, DIGEST_SQL).split("|")
    # Atomically: a worker polling for the file must never read it half written.
    ssh(CANADA, f"cat > {REMOTE}/runs/{run}/done.tmp && mv {REMOTE}/runs/{run}/done.tmp {REMOTE}/runs/{run}/done",
        input=json.dumps({"digest": digest, "rows": int(n)}))
    print(f"load done after {time.time() - t0:.0f} s: {n} live rows in {TENANT}, digest {digest[:12]}", flush=True)
    end = time.time() + 420
    while time.time() < end:
        finals = ssh(CANADA, f"cat {REMOTE}/runs/{run}/*.events | grep -c '\"final\"'", check=False).strip()
        if finals == str(len(names)):
            break
        time.sleep(10)
    c2 = counters()
    # The replicas first: each worker already compared its own to PostgreSQL's digest, and a
    # hundred of them, each with its WAL, filled this Mac's disk (2026-10-24: 9.9 GB, and
    # the copy below doubled it). Then the logs, events and report alone.
    ssh(CANADA, f"rm -f {REMOTE}/runs/{run}/*.sqlite3 {REMOTE}/runs/{run}/*.sqlite3-wal {REMOTE}/runs/{run}/*.sqlite3-shm", check=False)
    if LOCAL:
        subprocess.run(["cp", "-R", os.path.expanduser(f"~/{REMOTE}/runs/{run}/."), str(out)], check=True)
    else:
        subprocess.run(["scp", "-q", "-r", f"{CANADA}:{REMOTE}/runs/{run}/", str(out.parent)], check=True)

    # ── the report ──
    failed = 0
    print(f"\nidle baseline, {delta(c_idle0, c_idle1)['seconds']} s: {json.dumps(delta(c_idle0, c_idle1))}")
    print(f"during the load: {json.dumps(delta(c0, c1))}")
    print(f"load to the end: {json.dumps(delta(c0, c2))}")
    for name in SCHEDULE:
        ev_path, log_path = out / f"{name}.events", out / f"{name}.log"
        events = [json.loads(l) for l in ev_path.read_text().splitlines() if l.strip()] if ev_path.exists() else []
        finals = [e for e in events if e["event"] == "final"]
        ok = len(finals) == 1 and finals[0].get("exact")
        failed += not ok
        print(f"\n{'✓' if ok else '✗'} {name}: schedule {SCHEDULE[name]} — "
              + (f"exact ({finals[0]['rows']} rows), converged in {finals[0]['converge_s']} s" if ok
                 else f"{len(finals)} final result(s): {finals[0] if finals else 'none'}"))
        rows = gen_follow.seeding_report(name, events, log_path.read_text(errors="replace") if log_path.exists() else "")
        gen_follow.print_seeding(rows)
        for r in rows:
            print("seeding " + json.dumps(r))
    if extra:
        finals = []
        for ev in sorted(out.glob("batch*.events")):
            finals += [json.loads(l) for l in ev.read_text().splitlines() if '"final"' in l]
        exact = [f for f in finals if f.get("exact")]
        failed += len(extra) - len(exact)
        conv = sorted(f["converge_s"] for f in exact)
        print(f"\n{'✓' if len(exact) == len(extra) else '✗'} {len(extra)} unmeasured clients: {len(exact)} exact, "
              f"{len(finals) - len(exact)} not, {len(extra) - len(finals)} without a final result"
              + (f"; converged after the load in {conv[0]}–{conv[-1]} s (median {conv[len(conv) // 2]})" if conv else ""))
    print(f"\nlogs: {out}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
