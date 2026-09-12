#!/usr/bin/env python3
"""PostgreSQL → SQLite → DuckDB: an analytical job over a synced replica.

The bridge keeps a SQLite file current (libzb, one table or many); DuckDB attaches
that file and runs the analytical query. PostgreSQL is never touched by the job, and
the replica keeps syncing while DuckDB reads it — SQLite's WAL lets the two coexist.

  set -a && . ./.env.bridge && set +a          # NATS_URL and the creds directory
  pip install duckdb
  ZB_PRINCIPAL=bob ZB_TABLES=test_types python examples/07-duckdb/analyze.py

Env: NATS_URL (nats://127.0.0.1:4222), ZB_PRINCIPAL (bob), ZB_CREDS (scripts/native/creds/<principal>.creds),
ZB_DB (/tmp/zb-duckdb.sqlite3), ZB_TABLES (test_types), ZB_LIB (libzb/zig-out/lib/libzbcore.*).

The C card, in twenty lines of ctypes: open, sync (seeds the tables from the chain),
poll (applies what arrived), query, close. Every string in and out is JSON.
"""
import ctypes, json, os, pathlib, sys, time

ROOT = pathlib.Path(__file__).resolve().parents[2]
LIB = os.environ.get("ZB_LIB") or str(ROOT / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if sys.platform == "darwin" else "libzbcore.so"))
PRINCIPAL = os.environ.get("ZB_PRINCIPAL", "bob")
CREDS = os.environ.get("ZB_CREDS") or str(ROOT / "scripts" / "native" / "creds" / f"{PRINCIPAL}.creds")
DB = os.environ.get("ZB_DB", "/tmp/zb-duckdb.sqlite3")
TABLES = os.environ.get("ZB_TABLES", "test_types").split(",")
# The server address only: the creds file is the credential, userinfo in the URL would win over it.
_url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
NATS_URL = _url.split("://")[0] + "://" + _url.split("://", 1)[1].rsplit("@", 1)[-1]


class Replica:
    def __init__(self):
        self.lib = lib = ctypes.CDLL(LIB)
        lib.zb_free.argtypes = [ctypes.c_void_p]
        lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
        lib.zb_client_close.argtypes = [ctypes.c_uint64]
        for name, args in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
            f = getattr(lib, "zb_client_" + name); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + args
        opts = {"url": NATS_URL, "credsPath": CREDS, "dbPath": DB, "principal": PRINCIPAL, "clientId": "duckdb-job", "tables": TABLES, "heartbeatMs": 0}
        self.h = lib.zb_client_open(json.dumps(opts).encode())
        if not self.h: sys.exit("libzb open failed (NATS_URL, creds, library path?)")

    def _take(self, p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: self.lib.zb_free(p)

    def sync(self): return self._take(self.lib.zb_client_sync(self.h))
    def poll(self, wait_ms=200): return self._take(self.lib.zb_client_poll(self.h, wait_ms))
    def query(self, sql, params=()): return self._take(self.lib.zb_client_query(self.h, sql.encode(), json.dumps(list(params)).encode()))
    def close(self): self.lib.zb_client_close(self.h)


def main():
    import duckdb
    t0 = time.monotonic()
    rep = Replica()
    r = rep.sync()
    if r.get("error"): sys.exit(f"sync failed: {r['error']}")
    # The seed lands inside sync when the chain exists; a table enabled seconds ago may
    # have no chain yet — poll until the rows are there.
    for _ in range(600):
        counts = {t: rep.query(f"SELECT count(*) FROM {t}").get("rows", [[0]])[0][0] for t in TABLES}
        if all(counts.values()): break
        rep.poll(500)
    print(f"replica ready in {time.monotonic() - t0:.1f} s on tenant {r.get('tenant')}: " + ", ".join(f"{t} {n:,} rows" for t, n in counts.items()), flush=True)

    # DuckDB reads the SQLite file in place. READ_ONLY: the replica keeps syncing meanwhile.
    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{DB}' AS replica (TYPE sqlite, READ_ONLY)")
    t = TABLES[0]
    t1 = time.monotonic()
    rows = con.execute(f"""
        SELECT age, count(*) AS n,
               round(avg(temperature), 2) AS avg_temp,
               sum(CAST(price AS DECIMAL(20, 8))) AS revenue
        FROM replica.{t}
        WHERE deleted_at IS NULL
        GROUP BY age ORDER BY n DESC, age LIMIT 5
    """).fetchall()
    print(f"DuckDB, {t} by age ({(time.monotonic() - t1) * 1000:.0f} ms): " + "; ".join(f"age {a}: {n:,} rows, avg temp {tmp}, revenue {rev}" for a, n, tmp, rev in rows), flush=True)
    t2 = time.monotonic()
    total = con.execute(f"SELECT count(*), min(inserted_at), max(inserted_at) FROM replica.{t}").fetchone()
    print(f"DuckDB, whole table ({(time.monotonic() - t2) * 1000:.0f} ms): {total[0]:,} rows, inserted between {total[1]} and {total[2]}", flush=True)

    # The replica moved on while DuckDB read it: one more poll, and the file is current.
    rep.poll(200)
    rep.close()


if __name__ == "__main__":
    main()
