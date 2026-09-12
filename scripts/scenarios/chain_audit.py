#!/usr/bin/env python3
"""The chain audit (§10eu): does what sits in the object store mirror PostgreSQL?

Every other check of the chain goes THROUGH a client — a replica diffed against the
table, counts, the object's SHA-256. This one opens the objects themselves: the
manifest from the KV bucket, the full and every delta it names, decompressed with
the dictionary the manifest names, msgpack-decoded, and every cell compared with
PostgreSQL rendered in the wire's own shapes (ISO `Z` timestamps, numeric text with
its scale, PostgreSQL's array and jsonb text, real booleans).

  set -a && . ./.env.bridge && set +a
  scripts/scenarios/.venv/bin/python scripts/scenarios/chain_audit.py --tenant globex --table test_types

Two passes. Per object: a row whose version column is still at or under the object's
cutoff has not changed since the cut, and every cell must match; a row the table
changed after the cut is only checked for presence. A full holds the rows live at
its snapshot — a transaction that STARTED before the cutoff (`now()` is the
transaction's start) but committed after the snapshot is not in it, and the next
delta's clamp margin (`version_future_tolerance`) is what carries it; the audit
tolerates such a row within the margin and the second pass proves the claim: the
whole chain replayed — full, then each delta with the client's version-guarded
upsert — must equal the table at the chain's last cutoff.

One shape difference the audit normalises: arrays ride as JSON text
(`["grow","s-1"]`, §10ey) and are compared with PostgreSQL's `to_json` rendering,
quotes stripped on both sides (the wire keeps numeric elements as strings).
"""
import argparse, json, pathlib, sqlite3, struct, subprocess, sys, tempfile, time
import msgpack, zstandard
import zb

TS_FMT = "YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\""
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
SEP = "\x1f"
MARGIN_S = 5  # config.Sync.version_future_tolerance


def psql_rows(sql):
    r = subprocess.run(zb.PSQL.split() + ["-X", "-q", "-A", "-t", "-F", SEP, "-P", "null=\\N", "-c", sql], capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  ✗ psql: {r.stderr.strip()[:300]}", flush=True); sys.exit(1)
    return [l.split(SEP) for l in r.stdout.split("\n") if l]


def render(col, udt):
    """The SELECT expression that renders `col` the way the chain carries it."""
    if udt == "timestamptz": return f"to_char({col} AT TIME ZONE 'UTC', '{TS_FMT}')"
    if udt == "timestamp": return f"to_char({col}, 'YYYY-MM-DD\"T\"HH24:MI:SS.US')"
    if udt == "date": return f"to_char({col}, 'YYYY-MM-DD')"
    if udt == "bool": return f"CASE WHEN {col} THEN 't' ELSE 'f' END"
    # PostGIS renders geometry::text as UPPERCASE EWKB hex; the wire carries the bytes.
    if udt in ("geometry", "geography"): return f"encode(ST_AsEWKB({col}), 'hex')"
    # §10ey: arrays ride as JSON text; PostgreSQL's to_json renders the same list, with
    # numbers bare where the wire keeps numerics as strings — quotes stripped on both sides.
    if udt.startswith("_"): return f"replace(to_json({col})::text, '\"', '')"
    return f"{col}::text"


def f32_text(x):
    """pgvector's rendering of a float32: the shortest decimal that reads back to it."""
    packed = struct.pack("<f", x)
    for p in range(1, 10):
        s = f"{x:.{p}g}"
        if struct.pack("<f", float(s)) == packed: return s
    return repr(x)


def cell_text(v, udt):
    """A decoded chain cell as the text PostgreSQL renders — the comparison key."""
    if v is None: return "\\N"
    if isinstance(v, bool): return "t" if v else "f"
    if isinstance(v, bytes):
        # §10fg: pgvector and bit(n) ride as normalised BLOBs; PostgreSQL's ::text is
        # pgvector's own form, so the bytes are rendered the way pgvector prints them.
        if udt == "vector": return "[" + ",".join(f32_text(f) for f in struct.unpack(f"<{len(v) // 4}f", v)) + "]"
        if udt == "halfvec": return "[" + ",".join(f32_text(f) for f in struct.unpack(f"<{len(v) // 2}e", v)) + "]"
        if udt == "sparsevec":
            dim, nnz = struct.unpack_from("<II", v)
            idx = struct.unpack_from(f"<{nnz}I", v, 8); vals = struct.unpack_from(f"<{nnz}f", v, 8 + 4 * nnz)
            return "{" + ",".join(f"{i + 1}:{f32_text(x)}" for i, x in zip(idx, vals)) + "}/" + str(dim)
        if udt.startswith("bit"):
            n = int(udt[4:-1]) if udt.startswith("bit(") else len(v) * 8
            return "".join("1" if (v[i >> 3] >> (7 - (i & 7))) & 1 else "0" for i in range(n))
        return v.hex() if udt in ("geometry", "geography") else "\\x" + v.hex()
    if isinstance(v, float): return repr(v)
    s = str(v)
    return s.replace('"', "") if udt.startswith("_") else s


def same(chain_v, pg_text, udt):
    if chain_v is None or pg_text == "\\N": return chain_v is None and pg_text == "\\N"
    if udt in ("float4", "float8"):
        try:
            a, b = float(chain_v), float(pg_text)
        except (TypeError, ValueError):
            return False
        return abs(a - b) <= 1e-6 * max(1.0, abs(b))
    return cell_text(chain_v, udt) == pg_text


def ts_lit(iso_z):
    """A wire timestamp back to a timestamptz literal."""
    return "'" + iso_z.replace("T", " ").replace("Z", "+00") + "'::timestamptz"


def fetch_object(bucket, name, into):
    path = into / name
    r = zb.nats_cli("object", "get", bucket, name, "--output", str(path))
    if r.returncode != 0:
        print(f"  ✗ nats object get {bucket} {name}: {r.stderr.strip()[:200]}", flush=True)
        return None
    return path.read_bytes()


def inflate(blob, dict_bytes):
    if not blob.startswith(ZSTD_MAGIC): return blob
    if dict_bytes:
        try:
            return zstandard.ZstdDecompressor(dict_data=zstandard.ZstdCompressionDict(dict_bytes)).decompress(blob, max_output_size=1 << 31)
        except zstandard.ZstdError:
            pass
    return zstandard.ZstdDecompressor().decompress(blob, max_output_size=1 << 31)


class Replica:
    """The client's apply rule in SQLite: every cell as its wire text, an upsert
    guarded by the version column."""

    def __init__(self, columns, pk, vcol):
        self.db = sqlite3.connect(":memory:")
        self.columns, self.pk, self.vcol = columns, pk, vcol
        cols = ", ".join(f'"{c}" TEXT' for c in columns)
        self.db.execute(f'CREATE TABLE r ({cols}, PRIMARY KEY ({", ".join(pk)}))')
        sets = ", ".join(f'"{c}" = excluded."{c}"' for c in columns if c not in pk)
        self.sql = (f'INSERT INTO r VALUES ({", ".join("?" for _ in columns)}) ON CONFLICT({", ".join(pk)}) DO UPDATE SET {sets} '
                    f'WHERE excluded."{vcol}" >= r."{vcol}"')

    def apply(self, columns, rows, cols_udt):
        # The object's column order may differ from the table's: map by name.
        order = [columns.index(c) for c in self.columns]
        self.db.executemany(self.sql, ([cell_text(r[i], cols_udt[self.columns[j]]) for j, i in enumerate(order)] for r in rows))
        self.db.commit()

    def sorted_rows(self):
        return self.db.execute(f'SELECT * FROM r ORDER BY {", ".join(self.pk)}')


def audit_object(kind, gen, doc, cols_udt, pk, tenant, tenant_col, tcol, ins_col, table):
    columns = doc["columns"]; rows = doc["rows"]; vcol = doc.get("version_column")
    cutoff = doc["cutoff"]; prev = doc.get("prev_cutoff")
    missing_cols = [c for c in cols_udt if c not in columns]; extra_cols = [c for c in columns if c not in cols_udt]
    if missing_cols or extra_cols:
        print(f"  ✗ {kind} g{gen}: columns differ from PostgreSQL — object lacks {missing_cols}, carries {extra_cols}")
    ci = {c: i for i, c in enumerate(columns)}
    pk_i = [ci[k] for k in pk]
    by_key = {tuple(cell_text(r[i], cols_udt[columns[i]]) for i in pk_i): r for r in rows}
    dup = len(rows) - len(by_key)
    vi = ci[vcol]
    versions = [r[vi] for r in rows if r[vi] is not None]
    lo = min(versions) if versions else None

    # The expected set, with the version and the birth column alongside for the verdicts.
    sel = ", ".join(render(c, cols_udt[c]) for c in columns)
    born = f", {render(ins_col, cols_udt[ins_col])}" if ins_col else ", NULL"
    if kind == "full":
        live_at_cut = f"({tcol} IS NULL OR {tcol} > '{cutoff}'::timestamptz)" if tcol else "TRUE"
        existed = f"AND {ins_col} <= '{cutoff}'::timestamptz" if ins_col else f"AND {vcol} <= '{cutoff}'::timestamptz"
        where = f"{tenant_col} = '{tenant}' AND {live_at_cut} {existed}"
        expect_note = "live rows at the cutoff" + (" (by inserted_at)" if ins_col else f" (by {vcol})")
    else:
        lo_lit = ts_lit(lo) if lo else f"'{prev}'::timestamptz"
        where = f"{tenant_col} = '{tenant}' AND {vcol} >= {lo_lit} AND {vcol} <= '{cutoff}'::timestamptz"
        expect_note = f"rows whose {vcol} lies in the delta's range"
    t0 = time.monotonic()
    pg = psql_rows(f"SELECT {sel}{born} FROM {table} WHERE {where}")
    pg_ms = int((time.monotonic() - t0) * 1000)
    cutoff_wire = cutoff.replace(" ", "T").replace("+00", "Z")
    pg_by_key = {tuple(r[i] for i in pk_i): r for r in pg}

    exact = changed = missing = late = 0
    col_bad = {}
    examples = []
    for key, pr in pg_by_key.items():
        cr = by_key.get(key)
        pg_version = pr[vi]
        if cr is None:
            # Not in the object. Inside the clamp margin before the cutoff, a batch
            # that committed after the snapshot: the next delta carries it (proven by
            # the replay). Older than that is a hole.
            stamp = pr[-1] if ins_col else pg_version
            if stamp != "\\N" and within_margin(stamp, cutoff_wire):
                late += 1
            else:
                missing += 1
                if len(examples) < 5: examples.append(f"PG row {key} (version {pg_version}) is not in the object")
            continue
        if pg_version > cutoff_wire:
            changed += 1
            continue
        # The object holds an OLDER version of the row than the table, and the table's
        # version is inside the margin before the cutoff: an UPDATE that started before
        # the cutoff and committed after the snapshot — the next delta re-carries it.
        if cell_text(cr[vi], cols_udt[vcol]) < pg_version and within_margin(pg_version, cutoff_wire):
            late += 1
            continue
        bad = [c for c, i in ci.items() if not same(cr[i], pr[i], cols_udt[c])]
        if not bad:
            exact += 1
        else:
            for c in bad:
                col_bad[c] = col_bad.get(c, 0) + 1
                if len(examples) < 5:
                    i = ci[c]; examples.append(f"{key} {c}: object {cell_text(cr[i], cols_udt[c])[:60]!r} vs PG {pr[i][:60]!r}")
    # Rows the object holds that the expected set does not: the version moved past the
    # cutoff since, or the row was tombstoned/reaped — presence in the table, not cells.
    extra_keys = [k for k in by_key if k not in pg_by_key]
    gone = 0
    if extra_keys:
        sample = extra_keys[:50000]
        lits = ",".join("(" + ",".join("'" + v.replace("'", "''") + "'" for v in k) + ")" for k in sample)
        pk_list = ", ".join(pk)
        present = psql_rows(f"SELECT {pk_list} FROM {table} WHERE ({pk_list}) IN ({lits})")
        present_keys = {tuple(r) for r in present}
        gone = sum(1 for k in sample if k not in present_keys)
        changed += len(extra_keys) - gone
    ok = missing == 0 and not col_bad and dup == 0 and gone == 0 and not missing_cols and not extra_cols
    mark = "✓" if ok else "✗"
    print(f"  {mark} {kind} g{gen}: {len(rows)} row(s) in the object; PostgreSQL holds {len(pg)} {expect_note} ({pg_ms} ms) — {exact} cell-exact, {changed} changed since the cut, "
          f"{late} committed late (inside the {MARGIN_S} s margin, the next delta's), {missing} MISSING, {gone} in the object but gone from the table, {dup} duplicate key(s)", flush=True)
    for c, n in sorted(col_bad.items(), key=lambda x: -x[1]): print(f"      column {c}: {n} mismatch(es)")
    for e in examples: print(f"      · {e}")
    return ok


def within_margin(stamp_wire, cutoff_wire):
    """Both ISO `Z` wire timestamps; true when `stamp` is at most MARGIN_S before the cutoff."""
    from datetime import datetime, timedelta
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    try:
        s, c = datetime.strptime(stamp_wire, fmt), datetime.strptime(cutoff_wire, fmt)
    except ValueError:
        return False
    return c - timedelta(seconds=MARGIN_S) <= s <= c


def replay_check(rep, last_cutoff, cols_udt, pk, tenant, tenant_col, tcol, table):
    """The chain replayed against the table at its last cutoff."""
    columns = rep.columns
    sel = ", ".join(render(c, cols_udt[c]) for c in columns)
    vcol = rep.vcol
    t0 = time.monotonic()
    pg = psql_rows(f"SELECT {sel} FROM {table} WHERE {tenant_col} = '{tenant}' AND {vcol} <= '{last_cutoff}'::timestamptz ORDER BY {', '.join(pk)}")
    pg_ms = int((time.monotonic() - t0) * 1000)
    pk_i = [columns.index(k) for k in pk]
    t_i = columns.index(tcol) if tcol and tcol in columns else None
    rows = rep.sorted_rows()
    exact = tomb_ok = missing = extra = 0
    col_bad = {}
    examples = []
    cur = next(rows, None)
    for pr in pg:
        key = tuple(pr[i] for i in pk_i)
        while cur is not None and tuple(cur[i] for i in pk_i) < key:
            # A replica row the table has not got at this cutoff: born after the chain's
            # end, or a wire-shape difference in the key. Counted, five shown.
            extra += 1
            if len(examples) < 5: examples.append(f"replica row {tuple(cur[i] for i in pk_i)} is not in the table at the cutoff")
            cur = next(rows, None)
        pg_tomb = t_i is not None and pr[t_i] != "\\N"
        if cur is None or tuple(cur[i] for i in pk_i) != key:
            if pg_tomb:
                tomb_ok += 1  # a row tombstoned before the full: rightly absent
            else:
                missing += 1
                if len(examples) < 5: examples.append(f"PG row {key} is not in the replica")
            continue
        if pg_tomb and cur[t_i] != "\\N":
            tomb_ok += 1
        else:
            bad = [c for i, c in enumerate(columns) if not same_text(cur[i], pr[i], cols_udt[c])]
            if not bad: exact += 1
            for c in bad:
                col_bad[c] = col_bad.get(c, 0) + 1
                if len(examples) < 5:
                    i = columns.index(c); examples.append(f"{key} {c}: replica {cur[i][:60]!r} vs PG {pr[i][:60]!r}")
        cur = next(rows, None)
    while cur is not None:
        extra += 1; cur = next(rows, None)
    ok = missing == 0 and extra == 0 and not col_bad
    print(f"  {'✓' if ok else '✗'} the chain replayed: {len(pg)} table row(s) at the last cutoff ({pg_ms} ms) — {exact} cell-exact, {tomb_ok} tombstoned on both sides, "
          f"{missing} MISSING from the replica, {extra} in the replica only", flush=True)
    for c, n in sorted(col_bad.items(), key=lambda x: -x[1]): print(f"      column {c}: {n} mismatch(es)")
    for e in examples: print(f"      · {e}")
    return ok


def same_text(a, b, udt):
    if udt in ("float4", "float8") and a != "\\N" and b != "\\N":
        try:
            return abs(float(a) - float(b)) <= 1e-6 * max(1.0, abs(float(b)))
        except ValueError:
            return False
    return a == b


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tenant", default="globex"); ap.add_argument("--table", default="test_types")
    ap.add_argument("--only", choices=["full", "deltas", "all"], default="all")
    ap.add_argument("--no-replay", action="store_true", help="skip the whole-chain replay")
    a = ap.parse_args()
    raw = zb.kv_get("generations", f"{a.tenant}.{a.table}")
    if not raw:
        print(f"  ✗ no manifest for {a.tenant}.{a.table}"); return 1
    m = json.loads(raw)
    bucket = m["bucket"]
    print(f"  · manifest g{m['gen']} on {bucket}: full g{m['full']['gen']} (cutoff {m['full']['cutoff']}), {len(m['deltas'])} delta(s), cutoff_seq {m.get('cutoff_seq')} on {m.get('cdc_stream')}")

    # §10ff: the columns the publication carries — a column outside the table's column
    # list is not on the wire, so it is not in the chain either.
    # a bit(n) column carries its length — the wire pads to a byte, the text form does not
    cols = zb.psql(f"SELECT string_agg(column_name || ':' || udt_name || CASE WHEN udt_name = 'bit' THEN '(' || character_maximum_length || ')' ELSE '' END, ',' ORDER BY ordinal_position) FROM information_schema.columns c WHERE table_name = '{a.table}' "
                   f"AND COALESCE((SELECT bool_and(attnames IS NULL OR c.column_name = ANY(attnames)) FROM pg_publication_tables WHERE tablename = '{a.table}'), true)", quiet=True).strip()
    cols_udt = dict(c.split(":") for c in cols.split(","))
    pk = zb.psql(f"SELECT string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema WHERE tc.table_name = '{a.table}' AND tc.constraint_type = 'PRIMARY KEY'", quiet=True).strip().split(",")
    cat = zb.psql(f"SELECT tenant_col || '|' || coalesce(tombstone_col, '') FROM zebridge_catalogue WHERE tbl = '{a.table}'", quiet=True).strip()
    tenant_col, tcol = cat.split("|") if cat else ("tenant_id", "")
    ins_col = "inserted_at" if "inserted_at" in cols_udt else None
    print(f"  · {len(cols_udt)} column(s), key ({', '.join(pk)}), tenant column {tenant_col}, tombstone column {tcol or '(none)'}")

    ls = zb.nats_cli("object", "ls", bucket)
    names = [l.split("│")[1].strip() for l in ls.stdout.splitlines() if "│" in l and f"{a.table}-g" in l]
    referenced = {m["full"]["object"]} | {d["object"] for d in m["deltas"]} | {d["dict"] for d in m["deltas"] if d.get("dict")}
    kinds = {}
    for n in names: kinds[n.rsplit("-", 1)[1]] = kinds.get(n.rsplit("-", 1)[1], 0) + 1
    orphans = [n for n in names if n not in referenced]
    print(f"  · bucket holds {len(names)} object(s) of {a.table}: {kinds}; {len(referenced)} referenced by the manifest, {len(orphans)} unreferenced" + (f" (e.g. {', '.join(orphans[:4])})" if orphans else ""))

    into = pathlib.Path(tempfile.mkdtemp(prefix="zb_chain_audit_"))
    dicts = {}
    ok = True
    rep = None if a.no_replay else Replica(list(cols_udt), pk, m["version_column"])
    full_gen = m["full"]["gen"]
    try:
        objects = []
        if a.only in ("full", "all"): objects.append(("full", m["full"]))
        if a.only in ("deltas", "all"): objects += [("delta", d) for d in m["deltas"]]
        for kind, o in objects:
            dn = o.get("dict")
            if dn and dn not in dicts:
                db = fetch_object(bucket, dn, into)
                dicts[dn] = inflate(db, None) if db else None
            t0 = time.monotonic()
            blob = fetch_object(bucket, o["object"], into)
            if blob is None: ok = False; continue
            fetch_ms = int((time.monotonic() - t0) * 1000); t0 = time.monotonic()
            doc = msgpack.unpackb(inflate(blob, dicts.get(dn)), raw=False)
            print(f"  · {o['object']}: {len(blob)} bytes in the store (fetched in {fetch_ms} ms), dictionary {dn or '(none)'}, {len(doc['rows'])} row(s) decoded in {int((time.monotonic() - t0) * 1000)} ms", flush=True)
            assert doc["kind"] == kind and doc["gen"] == o["gen"], (doc["kind"], doc["gen"], o)
            ok &= audit_object(kind, o["gen"], doc, cols_udt, pk, a.tenant, tenant_col, tcol, ins_col, a.table)
            # The replay applies the full and the deltas AFTER it, in order — a client's path.
            if rep is not None and (kind == "full" or o["gen"] > full_gen):
                t0 = time.monotonic()
                rep.apply(doc["columns"], doc["rows"], cols_udt)
                print(f"    replayed into the SQLite replica in {int((time.monotonic() - t0) * 1000)} ms", flush=True)
            del doc, blob
    finally:
        for p in into.iterdir(): p.unlink()
        into.rmdir()
    if rep is not None and a.only == "all":
        ok &= replay_check(rep, m["cutoff_version"], cols_udt, pk, a.tenant, tenant_col, tcol, a.table)
    print("  " + ("✓ the chain mirrors PostgreSQL" if ok else "✗ the chain does NOT mirror PostgreSQL — see above"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main() or 0)
