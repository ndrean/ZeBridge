#!/usr/bin/env python3
"""The firehose (§10eu): rows straight into PostgreSQL at a rate no client can emit —
one INSERT of `--rate` rows per second, then an UPDATE of the previous second's rows —
so the bridge, the streams, the chain and the clients meet a table that GROWS at
thousands of rows a second. The wasp (`drip.py`) runs alongside for the three-way
invariant on its own rows; this script only pushes and reports what PostgreSQL gave.

  set -a && . ./.env.bridge && set +a
  scripts/scenarios/.venv/bin/python scripts/scenarios/inject.py --rate 10000 --minutes 10 [--table crazy_1]

Rows are `grow-<n>` on one tenant, never deleted: the fixture the run leaves behind is
the point — a fresh client's seed of it is the last measurement.
"""
import argparse, subprocess, sys, time
import zb

T = "test_types"


def psql(sql):
    r = subprocess.run(zb.PSQL.split() + ["-X", "-q", "-A", "-t", "-c", sql], capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  ✗ psql: {r.stderr.strip()[:200]}", flush=True)
    return r.stdout


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=10_000, help="rows inserted per second")
    ap.add_argument("--update", type=float, default=1.0, help="share of the previous second's rows updated (0..1)")
    ap.add_argument("--minutes", type=float, default=10)
    ap.add_argument("--tenant", default="globex")
    ap.add_argument("--table", default="test_types", help="a table shaped like test_types")
    a = ap.parse_args()
    global T
    T = a.table
    start = int(psql(f"SELECT COALESCE(max(substr(some_text, 6)::bigint), 0) FROM {T} WHERE some_text LIKE 'grow-%'").strip() or 0) + 1
    n0 = int(psql(f"SELECT count(*) FROM {T}").strip() or 0)
    print(f"  · firehose: {a.rate} inserts/s + {a.update:.0%} updates of the previous second, {a.minutes} min on {a.tenant}/{T}; the table has {n0} rows", flush=True)
    t0 = time.monotonic(); prev = []; sec = 0; ins_ms = upd_ms = 0.0; inserted = updated = 0; last_report = t0
    while time.monotonic() - t0 < a.minutes * 60:
        tick = time.monotonic()
        lo = start + sec * a.rate
        t = time.monotonic()
        out = psql(
            f"INSERT INTO {T} (uid, age, temperature, price, is_true, some_text, tags, matrix, metadata, tenant_id, last_writer, inserted_at, updated_at) "
            f"SELECT gen_random_uuid(), i % 90, 20 + (i % 100) / 10.0, ((i % 1000) + 0.5)::numeric, i % 2 = 0, format('grow-%s', lpad(i::text, 10, '0')), "
            f"ARRAY['grow', format('s-{sec}')], ARRAY[[i, 1], [2, 3]], jsonb_build_object('src', 'grow', 'i', i), '{a.tenant}', 'firehose', now(), now() "
            f"FROM generate_series({lo}, {lo + a.rate - 1}) AS i RETURNING uid")
        ins_ms += (time.monotonic() - t) * 1000
        uids = [l for l in out.split("\n") if l]
        inserted += len(uids)
        if prev and a.update > 0:
            k = int(len(prev) * a.update)
            t = time.monotonic()
            psql(f"UPDATE {T} SET age = age + 1, updated_at = now() WHERE uid = ANY(ARRAY[{','.join(chr(39) + u + chr(39) for u in prev[:k])}]::uuid[])")
            upd_ms += (time.monotonic() - t) * 1000
            updated += k
        prev = uids; sec += 1
        if time.monotonic() - last_report >= 30:
            el = time.monotonic() - t0
            print(f"  · {el/60:.1f} min: {inserted} inserted ({inserted/el:.0f}/s), {updated} updated ({updated/el:.0f}/s); per second: insert {ins_ms/max(sec,1):.0f} ms, update {upd_ms/max(sec,1):.0f} ms", flush=True)
            last_report = time.monotonic()
        left = 1.0 - (time.monotonic() - tick)
        if left > 0: time.sleep(left)
    el = time.monotonic() - t0
    n1 = int(psql(f"SELECT count(*) FROM {T}").strip() or 0)
    print(f"  · done: {inserted} inserted, {updated} updated in {el/60:.1f} min ({inserted/el:.0f}/s, {updated/el:.0f}/s); the table has {n1} rows, {psql(f'SELECT pg_size_pretty(pg_total_relation_size({chr(39)}{T}{chr(39)}))').strip()}", flush=True)


if __name__ == "__main__":
    sys.exit(main() or 0)
