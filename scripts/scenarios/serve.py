#!/usr/bin/env python3
"""`serve` in both client libraries: a responder is a client that answers (NOTES §10hp).

Until now a service was an application: a second NATS connection, a thread, and a lock
between the two. `serve` moves that into the libraries. libzb subscribes
`query.<tenant>.<name>` in a queue group on the client's OWN connection, hands the
questions to the host in `poll`'s report and sends the answers with `reply`;
zb-client-ts does the same behind async handlers. This scenario runs one responder of
each KIND at once, in ONE queue group, and asks them.

  A  a libzb responder answers a question about its own replica, and the answer equals
     PostgreSQL;
  B  a zb-client-ts responder answers the same question with the same number;
  C  in one queue group the asks are SHARED — both labels answer some of them, which is
     how a region scales and fails over;
  D  a handler that throws answers an ERROR, not a timeout: a service that says no is
     not a service that looks dead;
  E  the role boundary holds: a CLIENT credential may ask but may NOT subscribe a query
     subject, so nobody can pose as a service (§10hk);
  F  an answer too large for one NATS message (§10hq) travels as an OBJECT in the asking
     tenant's bucket and arrives WHOLE, both ways round: a libzb responder answering a
     zb-client-ts asker, and the reverse. A raw NATS client sees the envelope, which is
     what proves the object path was taken rather than a big message squeezing through.
     Answers are compressed before inline-or-object is decided (§10ip), so the large
     answer's rows carry hex digests (816 KB compressed) and the raw asker reads zstd.

Usage:  scripts/scenarios/.venv/bin/python scripts/scenarios/serve.py
"""
import asyncio, ctypes, hashlib, json, os, pathlib, subprocess, sys, tempfile, time
from compression import zstd  # Python 3.14: answers travel as zstd frames (§10ip)

import nats
import zb

REPO = pathlib.Path(__file__).resolve().parents[2]
LIBZB = REPO / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if sys.platform == "darwin" else "libzbcore.so")
TABLE = "memo"
QUEUE = "serveprobe"
TENANT = "_default"
ASKS = 20


class ZigResponder:
    """A libzb client that serves: open, sync, serve, then one loop of poll and reply."""

    def __init__(self, db: str, label: str):
        self.label = label
        self.lib = lib = ctypes.CDLL(str(LIBZB))
        lib.zb_free.argtypes = [ctypes.c_void_p]
        lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
        lib.zb_client_close.argtypes = [ctypes.c_uint64]
        for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                     ("serve", [ctypes.c_char_p]), ("reply", [ctypes.c_uint64, ctypes.c_char_p])):
            f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a
        self.h = lib.zb_client_connect(json.dumps({
            "natsUrl": zb.nats_server(), "credsPath": zb.creds_for("pois"), "principal": "pois",
            "dbPath": db, "tables": [TABLE], "clientId": f"serve-{label}", "heartbeatMs": 0}).encode())
        if not self.h:
            sys.exit("libzb open failed")
        self.take(lib.zb_client_sync(self.h))
        r = self.take(lib.zb_client_serve(self.h, json.dumps({"tenants": [TENANT], "queries": ["count", "zigbig"], "queue": QUEUE}).encode()))
        if "error" in r:
            sys.exit(f"libzb serve refused: {r['error']}")
        self.serving = r["serving"]
        self.answered = 0

    def take(self, p):
        try:
            return json.loads(ctypes.string_at(p).decode())
        finally:
            self.lib.zb_free(p)

    def turn(self, wait_ms=50):
        """One loop turn: poll, answer what came."""
        rep = self.take(self.lib.zb_client_poll(self.h, wait_ms))
        for q in rep.get("requests", []):
            t0 = time.time()
            if q["name"] == "zigbig":
                # §10hq: deliberately past one message — the library turns it into an object.
                n = int((q.get("payload") or {}).get("rows", 20000))
                # §10ip: answers are compressed before inline-or-object is decided, so
                # a "too large" answer must stay large compressed. Hex digests shrink by
                # about half; "x" * 40 shrank 20:1 and every answer went inline.
                ans = {"rows": [[i, f"row {i} " + hashlib.sha256(str(i).encode()).hexdigest()] for i in range(n)], "count": n, "answered_by": self.label}
            else:
                rows = self.take(self.lib.zb_client_query(self.h, f"SELECT count(*) AS n FROM {TABLE}".encode(), b"[]"))
                ans = {"table": TABLE, "count": rows["rows"][0][0] if rows.get("rows") else None, "answered_by": self.label}
            ans["ms"] = round((time.time() - t0) * 1000, 1)
            self.take(self.lib.zb_client_reply(self.h, q["id"], json.dumps(ans).encode()))
            self.answered += 1

    def close(self):
        self.lib.zb_client_close(self.h)


async def ask(nc, name: str, payload: dict, timeout=5.0):
    r = await nc.request(f"query.{TENANT}.{name}", json.dumps(payload).encode(), timeout=timeout)
    # A raw asker reads both forms (§10ip): JSON, or a zstd frame of it (magic 28 b5 2f fd).
    data = zstd.decompress(r.data) if r.data[:4] == b"\x28\xb5\x2f\xfd" else r.data
    return json.loads(data)


async def main() -> int:
    failed = 0
    if not LIBZB.exists():
        sys.exit(f"{LIBZB} missing — cd libzb && zig build -Doptimize=ReleaseFast")
    live = int(zb.psql(f"SELECT count(*) FROM public.{TABLE}", quiet=True).strip() or "0")
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb-serve-"))

    zig = ZigResponder(str(tmp / "zig.sqlite3"), "zig")
    zb.ok(f"libzb serves {zig.serving} subject(s) in queue group {QUEUE!r} on its own connection")

    # The zb-client-ts responder, as a separate process — the other library, same group.
    env_ts = dict(os.environ, ZB_LABEL="ts", ZB_TABLES=TABLE, ZB_QUEUE=QUEUE, ZB_TENANTS=TENANT,
                  ZB_DB=str(tmp / "ts.sqlite3"), NATS_URL=zb.nats_server())
    ts = subprocess.Popen(["pnpm", "serve"], cwd=REPO / "examples" / "04-node-consumer",
                          env=env_ts, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        deadline = time.time() + 60
        while time.time() < deadline:
            line = ts.stdout.readline()
            if "serving as" in line:
                zb.ok(f"zb-client-ts serves too: {line.strip()}")
                break
            if ts.poll() is not None:
                zb.bad(f"the TS responder died: {line.strip()}")
                return failed + 1
        else:
            zb.bad("the TS responder never announced itself")
            return failed + 1

        nc = await nats.connect(zb.nats_server(), user_credentials=zb.creds_for("omar"), inbox_prefix=b"_INBOX.omar")
        # One turn before the first ask so the libzb responder is listening, then a turn
        # between asks: the host owns the loop, which is the whole point of the shape.
        zig.turn()
        seen: dict[str, int] = {}
        counts: set = set()
        for _ in range(ASKS):
            fut = asyncio.create_task(ask(nc, "count", {"table": TABLE}))
            for _ in range(12):                    # drive the libzb loop while the ask is in flight
                if fut.done():
                    break
                zig.turn(20)
                await asyncio.sleep(0)
            try:
                a = await asyncio.wait_for(fut, timeout=6)
            except Exception as e:
                zb.bad(f"an ask went unanswered: {type(e).__name__}")
                failed += 1
                continue
            seen[a.get("answered_by", "?")] = seen.get(a.get("answered_by", "?"), 0) + 1
            counts.add(a.get("count"))

        if counts == {live}:
            zb.ok(f"every answer equals PostgreSQL: {TABLE} holds {live} row(s), from both replicas")
        else:
            zb.bad(f"answers disagree with PostgreSQL ({live}): {counts}")
            failed += 1
        if seen.get("zig", 0) > 0:
            zb.ok(f"the libzb responder answered {seen['zig']} of {ASKS}")
        else:
            zb.bad("the libzb responder answered nothing")
            failed += 1
        if seen.get("ts", 0) > 0:
            zb.ok(f"the zb-client-ts responder answered {seen['ts']} of {ASKS}")
        else:
            zb.bad("the zb-client-ts responder answered nothing")
            failed += 1
        if seen.get("zig", 0) and seen.get("ts", 0):
            zb.ok(f"one queue group, two libraries, the asks shared: {seen}")

        # D: a handler that throws answers an error (the TS responder owns `boom`)
        try:
            a = await ask(nc, "boom", {})
            if isinstance(a, dict) and a.get("error"):
                zb.ok(f"a throwing handler answers an error, not a timeout: {a['error'][:60]}")
            else:
                zb.bad(f"the throwing handler answered {a}")
                failed += 1
        except Exception as e:
            zb.bad(f"the throwing handler timed out instead of answering: {type(e).__name__}")
            failed += 1

        # E: the role boundary — a CLIENT may ask, and may not listen
        violations: list = []
        async def on_error(e):
            violations.append(str(e).lower())
        client = await nats.connect(zb.nats_server(), user_credentials=zb.creds_for("omar"),
                                    error_cb=on_error, inbox_prefix=b"_INBOX.omar")
        await client.subscribe(f"query.{TENANT}.count")
        await client.flush()
        await asyncio.sleep(0.5)
        await client.close()
        if any("query" in v for v in violations):
            zb.ok("a client credential may ASK but may not SUBSCRIBE a query subject — nobody poses as a service")
        else:
            zb.bad("a client credential subscribed a query subject")
            failed += 1

        # ── F. an answer too large for one message ──────────────────────────
        BIG = 20000
        # F1: a RAW NATS client sees the envelope — the object path was taken
        fut = asyncio.create_task(ask(nc, "zigbig", {"rows": BIG}, timeout=20))
        for _ in range(400):
            if fut.done():
                break
            zig.turn(20)
            await asyncio.sleep(0)
        raw = await asyncio.wait_for(fut, timeout=20)
        env = raw.get("zb_object") if isinstance(raw, dict) else None
        if env and env.get("bucket") == f"res-{TENANT}" and env.get("bytes", 0) > 262_144:
            zb.ok(f"a raw client sees the ENVELOPE: {env['bytes']:,} bytes in {env['bucket']} — past one message, so it went as an object")
        else:
            zb.bad(f"the large answer did not travel as an object: {str(raw)[:120]}")
            failed += 1

        # F2: zb-client-ts ASKS the libzb responder and resolves it whole
        # An asker is a CLIENT principal: a responder may answer and may not ask (§10hk).
        ask_env = dict(env_ts, ZB_PRINCIPAL="omar", ZB_ASK="zigbig", ZB_ASK_PAYLOAD=json.dumps({"rows": BIG}),
                       ZB_DB=str(tmp / "ts-ask.sqlite3"))
        asker = subprocess.Popen(["pnpm", "serve"], cwd=REPO / "examples" / "04-node-consumer",
                                 env=ask_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        line, deadline = "", time.time() + 90
        while time.time() < deadline:
            zig.turn(20)                      # the libzb responder must be driven meanwhile
            if asker.poll() is not None and not line:
                break
            import select
            if select.select([asker.stdout], [], [], 0.05)[0]:
                got = asker.stdout.readline()
                if got.startswith("ASKED "):
                    line = got[len("ASKED "):].strip()
                    break
        asker.terminate()
        try:
            got = json.loads(line) if line else {}
        except Exception:
            got = {}
        if got.get("rows") == BIG and got.get("envelope_resolved"):
            zb.ok(f"zb-client-ts resolved a libzb responder's large answer whole: {got['rows']:,} rows in {got.get('ms')} ms")
        else:
            zb.bad(f"the TS asker did not resolve the large answer: {line[:140] or '(no line)'}")
            failed += 1

        # F3: the bucket expires on its own — nobody sweeps a result store
        js = nc.jetstream()
        try:
            si = await js.stream_info(f"OBJ_res-{TENANT}")
            # nats-py reports max_age in SECONDS (it converts the wire's nanoseconds).
            age_s = float(si.config.max_age or 0)
            if age_s > 0:
                zb.ok(f"the answer bucket expires on its own: OBJ_res-{TENANT} max_age {age_s:.0f} s")
            else:
                zb.bad(f"OBJ_res-{TENANT} has no max_age — answers would accumulate for ever")
                failed += 1
        except Exception as e:
            zb.bad(f"the answer bucket is missing: {type(e).__name__}")
            failed += 1

        await nc.close()
    finally:
        ts.terminate()
        try:
            ts.wait(timeout=10)
        except Exception:
            ts.kill()
        zig.close()
    return failed


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
