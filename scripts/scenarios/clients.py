"""The two clients a migration scenario drives side by side (NOTES §10dg):
`Lib` — libzb through its C ABI, host-driven (every observation is a poll);
`Node` — zb-client-ts in a Node process (`examples/04-node-consumer/query-worker.ts`),
event-driven, answering SQL over stdin/stdout. `both()` polls the first and asks
both until a predicate holds on each."""
import ctypes, json, os, select, subprocess, sys, time
import zb

NODE_DIR = zb.ROOT / "examples" / "04-node-consumer"
LIBZB = zb.ROOT / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if sys.platform == "darwin" else "libzbcore.so")


def fresh_sqlite(path):
    for f in (path, path + "-wal", path + "-shm"):
        try: os.remove(f)
        except FileNotFoundError: pass


class Lib:
    """The libzb client, host-driven: every observation is a poll."""
    def __init__(self, db, tables, client_id="py-scenario", principal="omar", creds=None, db_url=None):
        if not LIBZB.exists():
            sys.exit(f"{LIBZB} missing — cd libzb && zig build -Doptimize=ReleaseFast")
        self.lib = lib = ctypes.CDLL(str(LIBZB))
        lib.zb_free.argtypes = [ctypes.c_void_p]
        lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
        lib.zb_client_close.argtypes = [ctypes.c_uint64]
        # ⚠️ Every verb's types, at open: a `flush` called before any `mutate` used to run
        # with ctypes' default int return — a 64-bit pointer cut to 32 bits, then read as
        # a C string (SIGSEGV in the host, blamed on the library for an hour).
        for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                     ("flush", [ctypes.c_uint64]), ("mutate", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]),
                     ("mutate_at", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p])):
            f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a
        lib.zb_client_wipe.restype, lib.zb_client_wipe.argtypes = ctypes.c_int, [ctypes.c_uint64]
        self.h = lib.zb_client_open(json.dumps({
            "url": zb.nats_server(), "credsPath": str(creds) if creds else zb.creds_for(principal), "dbPath": db,
            "principal": principal, "clientId": client_id, "tables": list(tables), "heartbeatMs": 0,
            **({"dbUrl": db_url} if db_url else {})}).encode())
        if not self.h: sys.exit("libzb open failed")
        r = self.take(lib.zb_client_sync(self.h))
        self.sync_error = r.get("error")
        if self.sync_error and self.sync_error != "Revoked": sys.exit(f"libzb sync failed: {self.sync_error}")
        self.tenant = r.get("tenant")
    def take(self, p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: self.lib.zb_free(p)
    def poll(self, wait_ms=300):
        """One poll; the report dict, or {"error": name} once the connection is gone."""
        self.last_poll = self.take(self.lib.zb_client_poll(self.h, wait_ms))
        return self.last_poll
    def q(self, sql, params=()):
        r = self.take(self.lib.zb_client_query(self.h, sql.encode(), json.dumps(list(params)).encode()))
        if "error" in r: raise RuntimeError(r["error"])
        return r["rows"]
    def cols(self, t): return [r[0] for r in self.q(f"SELECT name FROM pragma_table_info('{t}')")]
    def mutate(self, table, op, key, values=None, version=None):
        """`version` stamps the write explicitly — a slow clock, in one argument. libzb
        sends at once (one path for every send), so this is how a write is made late."""
        lib = self.lib
        args = (self.h, table.encode(), op.encode(), json.dumps(key).encode(), json.dumps(values).encode() if values is not None else None)
        if version is None: return self.take(lib.zb_client_mutate(*args))
        return self.take(lib.zb_client_mutate_at(*args, version.encode()))
    def flush(self, wait_ms=2000):
        return self.take(self.lib.zb_client_flush(self.h, wait_ms))
    def close(self): self.lib.zb_client_close(self.h)
    def wipe(self):
        """The explicit wipe (§10dl): close and delete the replica files."""
        return self.lib.zb_client_wipe(self.h)


class Node:
    """The zb-client-ts client, event-driven, behind query-worker.ts."""
    def __init__(self, db, log="/tmp/zb_node_worker.log", principal="omar", creds=None):
        env = dict(os.environ, ZB_DB=db, ZB_PRINCIPAL=principal, NATS_URL=zb.nats_server())
        if creds: env["ZB_CREDS"] = str(creds)
        self.log = open(log, "w")
        self.p = subprocess.Popen(["node", "--experimental-strip-types", "query-worker.ts"], cwd=NODE_DIR, env=env,
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log, text=True)
        line = self.p.stdout.readline()
        try:
            first = json.loads(line)
        except json.JSONDecodeError:
            self.p.wait(timeout=10); self.log.close()
            tail = open(log).read().splitlines()[-3:]
            raise RuntimeError(f"node worker did not come up ({line.strip()[:80]!r}); last log lines: {tail}")
        self.tenant = first.get("tenant")
        self.connect_error = first.get("error")
    def q(self, sql, params=()):
        self.p.stdin.write(json.dumps({"sql": sql, "params": list(params)}) + "\n"); self.p.stdin.flush()
        # a silent worker is a finding, not a hang: 60 s, then the scenario says so
        if not select.select([self.p.stdout], [], [], 60)[0]:
            raise RuntimeError("node worker silent for 60 s")
        line = self.p.stdout.readline()
        if not line: raise RuntimeError("node worker exited")
        r = json.loads(line)
        if "error" in r: raise RuntimeError(r["error"])
        return [list(row.values()) for row in r["rows"]]
    def cols(self, t): return [r[0] for r in self.q(f"SELECT name FROM pragma_table_info('{t}')")]
    def mutate(self, table, op, key, values=None, version=None):
        """`version` stamps the write explicitly — a slow clock, in one argument."""
        self.p.stdin.write(json.dumps({"mutate": {"table": table, "op": op, "key": key, "values": values, "version": version}}) + "\n"); self.p.stdin.flush()
        if not select.select([self.p.stdout], [], [], 60)[0]:
            raise RuntimeError("node worker silent for 60 s")
        r = json.loads(self.p.stdout.readline())
        if "error" in r: raise RuntimeError(r["error"])
        return r["rows"][0]
    def _op(self, req):
        self.p.stdin.write(json.dumps(req) + "\n"); self.p.stdin.flush()
        if not select.select([self.p.stdout], [], [], 60)[0]:
            raise RuntimeError("node worker silent for 60 s")
        r = json.loads(self.p.stdout.readline())
        if "error" in r: raise RuntimeError(r["error"])
        return r["rows"][0]
    def disconnect(self):
        """Hang up the socket (§10dp): writes made afterwards queue in the outbox."""
        return self._op({"disconnect": True})
    def connect(self):
        """Reconnect: catch up on CDC, then flush the outbox."""
        return self._op({"connect": True})
    def wipe(self):
        """The explicit wipe (§10dl): the worker closes its client, deletes the files, exits."""
        self.p.stdin.write('{"wipe": true}\n'); self.p.stdin.flush()
        line = self.p.stdout.readline(); self.p.wait(timeout=15); self.log.close()
        return "wiped" in line
    def close(self):
        try:
            self.p.stdin.write('{"close": true}\n'); self.p.stdin.flush(); self.p.wait(timeout=15)
        except Exception:
            self.p.kill()
        self.log.close()


def both(py, nd, pred, budget=60):
    """Poll libzb and ask both replicas until `pred(client)` holds for each; seconds or None."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        py.poll()
        try:
            if pred(py) and pred(nd): return round(time.monotonic() - t0, 2)
        except Exception:
            pass
        time.sleep(0.2)
    return None


