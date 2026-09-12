#!/usr/bin/env python3
"""The streaming seed (§10fh): libzb with `seedStreaming` holds one chunk of rows,
never the inflated chain object — measured on the 3 M-row test_types chain.

  set -a && . ./.env.bridge && set +a
  scripts/scenarios/.venv/bin/python scripts/scenarios/seed_stream.py [--table test_types] [--principal bob]

Needs NATS with a chain for the table (the bridge need not be running: a seed reads
the manifest and the objects, nothing else). Two seeds into fresh SQLite files, each
in its own process so its peak RSS is its own: the whole-object path, then the
streaming path. What is checked: both replicas hold the same rows (count and a
checksum over every column), the streaming seed wrote its watermark, and its peak
RSS is a fraction of the other's — the inflated document is what the streaming
path never holds. Times are printed; the streaming apply is expected slower (the
sort is per chunk, not global).
"""
import argparse, json, os, resource, subprocess, sys, time
import zb

FAILED = []


def check(msg, cond):
    (zb.ok if cond else zb.bad)(msg)
    if not cond: FAILED.append(msg)


def child(table, principal, streaming):
    from clients import Lib, fresh_sqlite
    db = f"/tmp/zb-seed-{'stream' if streaming else 'whole'}.sqlite3"; fresh_sqlite(db)
    t0 = time.monotonic()
    em = Lib(db, [table], f"py-seed-{'stream' if streaming else 'whole'}", principal=principal, seed_streaming=streaming)
    # the seed runs inside a poll once the manifest is usable (a chain that predates
    # the stream waits for the producer's next generation)
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline and not em.q(f"SELECT 1 FROM {table} LIMIT 1"): em.poll(500)
    elapsed = time.monotonic() - t0
    cols = em.cols(table)
    expr = " + ".join(f"length(coalesce(CAST(\"{c}\" AS TEXT), ''))" for c in cols)
    n, chk = em.q(f"SELECT count(*), total({expr}) FROM {table}")[0]
    wm = em.q(f"SELECT watermark FROM _zbz_generations WHERE tbl = '{table}'")
    em.close()
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1 << 20)
    print(json.dumps({"rows": n, "checksum": chk, "elapsed_s": round(elapsed, 1), "peak_mb": round(peak), "watermark": wm[0][0] if wm else None}))


def run_child(table, principal, streaming):
    r = subprocess.run([sys.executable, __file__, "--child", "--table", table, "--principal", principal] + (["--streaming"] if streaming else []),
                       capture_output=True, text=True, env=os.environ)
    lines = [l for l in r.stdout.splitlines() if l.startswith("{")]
    if r.returncode != 0 or not lines:
        print(r.stderr[-1500:]); return None
    return json.loads(lines[-1])


async def main(table, principal):
    whole = run_child(table, principal, False)
    check(f"whole-object seed: {whole}", whole is not None and whole["rows"] > 0)
    stream = run_child(table, principal, True)
    check(f"streaming seed: {stream}", stream is not None and stream["rows"] > 0)
    if not whole or not stream: return 1
    check(f"both replicas hold the same rows ({whole['rows']:,}) and checksum", whole["rows"] == stream["rows"] and abs(whole["checksum"] - stream["checksum"]) < 1)
    check(f"the streaming seed wrote its watermark ({stream['watermark']})", bool(stream["watermark"]) and stream["watermark"] == whole["watermark"])
    ratio = stream["peak_mb"] / max(whole["peak_mb"], 1)
    check(f"peak RSS: whole {whole['peak_mb']} MB, streaming {stream['peak_mb']} MB ({ratio:.0%}); time: whole {whole['elapsed_s']} s, streaming {stream['elapsed_s']} s", ratio < 0.5)
    return 1 if FAILED else 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="test_types"); ap.add_argument("--principal", default=os.environ.get("ZB_PRINCIPAL", "bob"))
    ap.add_argument("--child", action="store_true"); ap.add_argument("--streaming", action="store_true")
    a = ap.parse_args()
    if a.child:
        child(a.table, a.principal, a.streaming); sys.exit(0)
    sys.exit(zb.run(lambda: main(a.table, a.principal)))
