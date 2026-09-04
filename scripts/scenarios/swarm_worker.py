#!/usr/bin/env python3
"""One libzb client in the 100-strong soak (swarm.py, NOTES §10cp).

Same 5 s CRUD cycle as the Node worker (see swarm-worker.ts for the shape):
three test_types INSERTs, an UPDATE of the last, a soft DELETE of the first,
with an orders INSERT/UPDATE/physical-DELETE folded onto the first three ticks.
One mutation tick per second; poll() drives the CDC apply between ticks; the
outbox absorbs every outage. At the end: drain, settle, report whole-replica
checksums as JSON.

Env: ZB_PRINCIPAL ZB_WORKER_ID ZB_DB ZB_DURATION_S ZB_SETTLE_S ZB_REPORT
     ZB_USER_IDS(csv) NATS_URL
"""
import ctypes
import hashlib
import json
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "libzb", "python"))
import _env  # noqa: E402

P = os.environ.get("ZB_PRINCIPAL", "omar")
WID = os.environ.get("ZB_WORKER_ID", "0")
DB = os.environ.get("ZB_DB", f"/tmp/zb-swarm-py-{WID}.sqlite3")
DURATION_S = float(os.environ.get("ZB_DURATION_S", "3600"))
SETTLE_S = float(os.environ.get("ZB_SETTLE_S", "240"))
REPORT = os.environ.get("ZB_REPORT", f"/tmp/zb-swarm-report-py-{WID}.json")
USER_IDS = [int(x) for x in os.environ.get("ZB_USER_IDS", "").split(",") if x]

lib = _env.load_lib()
lib.zb_free.argtypes = [ctypes.c_void_p]
lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
lib.zb_client_close.argtypes = [ctypes.c_uint64]
for fn, a in [("sync", []), ("poll", [ctypes.c_uint64]), ("flush", [ctypes.c_uint64]),
              ("query", [ctypes.c_char_p, ctypes.c_char_p]),
              ("mutate", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p])]:
    f = getattr(lib, "zb_client_" + fn)
    f.restype = ctypes.c_void_p
    f.argtypes = [ctypes.c_uint64] + a


def take(p):
    if not p:
        return {}
    try:
        return json.loads(ctypes.string_at(p).decode())
    finally:
        lib.zb_free(p)


def query(h, sql, params=None):
    """libzb answers {columns: [...], rows: [[...]]} — reshape to dicts; surface errors."""
    r = take(lib.zb_client_query(h, sql.encode(), json.dumps(params or []).encode()))
    if not isinstance(r, dict) or "rows" not in r:
        counters["queryErrors"].append(str(r)[:120])
        return []
    cols = r.get("columns", [])
    return [dict(zip(cols, row)) for row in r["rows"]]


counters = {"sent": 0, "sendErrors": 0, "byError": {}, "queryErrors": [], "pollErrors": {}}


def send(h, table, op, key, values=None):
    r = take(lib.zb_client_mutate(h, table.encode(), op.encode(), json.dumps(key).encode(),
                                  json.dumps(values).encode() if values is not None else None))
    if "msgId" in r:
        counters["sent"] += 1
    else:
        counters["sendErrors"] += 1
        name = str(r.get("error", r))[:60]
        counters["byError"][name] = counters["byError"].get(name, 0) + 1


def main():
    creds = os.path.join(_env.CREDS_DIR, f"{P}.creds")
    h = lib.zb_client_open(json.dumps({
        "url": os.environ.get("NATS_URL", "nats://127.0.0.1:4222"),
        "credsPath": creds, "grammarPath": _env.GRAMMAR, "dbPath": DB,
        "principal": P, "clientId": f"py-swarm-{WID}",
        "tables": ["users", "orders", "test_types"]}).encode())
    if not h:
        json.dump({"worker": WID, "fatal": "open failed"}, open(REPORT, "w"))
        return 1
    tenant = ""
    try:
        s = take(lib.zb_client_sync(h))
        tenant = s.get("tenant") or ""
        if not tenant:
            json.dump({"worker": WID, "fatal": f"sync gave no tenant: {s}"}, open(REPORT, "w"))
            return 1

        t0 = time.monotonic()
        trio, order, tick, users_ready, applied_total = [], None, 0, False, 0
        while time.monotonic() - t0 < DURATION_S:
            tick_start = time.monotonic()
            i = tick % 5
            now = time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime())
            cyc = tick // 5
            if i == 0:
                trio = [str(uuid.uuid4())]
                send(h, "test_types", "INSERT", {"uid": trio[0]},
                     {"uid": trio[0], "tenant_id": tenant,
                      "some_text": f"w{WID} c{cyc} a", "inserted_at": now, "updated_at": now})
                if not users_ready:
                    r = query(h, "SELECT count(*) AS n FROM users")
                    users_ready = bool(r) and r[0]["n"] > 0
                if USER_IDS and users_ready:
                    order = str(uuid.uuid4())
                    send(h, "orders", "INSERT", {"uid": order},
                         {"uid": order, "user_id": USER_IDS[tick % len(USER_IDS)],
                          "label": f"w{WID} o{tick}", "inserted_at": now, "updated_at": now})
            elif i == 1:
                trio.append(str(uuid.uuid4()))
                send(h, "test_types", "INSERT", {"uid": trio[1]},
                     {"uid": trio[1], "tenant_id": tenant,
                      "some_text": f"w{WID} c{cyc} b", "inserted_at": now, "updated_at": now})
                if order:
                    send(h, "orders", "UPDATE", {"uid": order}, {"label": f"w{WID} o{tick} touched", "updated_at": now})
            elif i == 2:
                trio.append(str(uuid.uuid4()))
                send(h, "test_types", "INSERT", {"uid": trio[2]},
                     {"uid": trio[2], "tenant_id": tenant,
                      "some_text": f"w{WID} c{cyc} c", "inserted_at": now, "updated_at": now})
                if order:
                    send(h, "orders", "DELETE", {"uid": order})
                    order = None
            elif i == 3:
                send(h, "test_types", "UPDATE", {"uid": trio[2]}, {"some_text": f"w{WID} touched", "updated_at": now})
            else:
                send(h, "test_types", "DELETE", {"uid": trio[0]})
            take(lib.zb_client_flush(h, 200))
            tick += 1
            if tick % 15 == 0:
                print(f"[w{WID}] tick {tick} sent={counters['sent']} errs={counters['sendErrors']} "
                      f"applied_total={applied_total}", flush=True)
            # poll returns EARLY when a batch applied (by design) — keep polling
            # until this tick's second is spent, so the cadence holds under load
            deadline_t = tick_start + 1.0
            while True:
                left = deadline_t - time.monotonic()
                if left <= 0.05:
                    if left > 0:
                        time.sleep(left)
                    break
                r = take(lib.zb_client_poll(h, int(left * 1000) - 30))
                if isinstance(r, dict):
                    applied_total += r.get("applied", 0) or 0
                if isinstance(r, dict) and "error" in r:
                    e = str(r["error"])[:60]
                    counters["pollErrors"][e] = counters["pollErrors"].get(e, 0) + 1
                    time.sleep(min(left, 0.5))

        # settle: outbox to zero, then let the last CDC fan-out land
        outbox = -1
        deadline = time.monotonic() + SETTLE_S
        while time.monotonic() < deadline:
            take(lib.zb_client_flush(h, 1000))
            take(lib.zb_client_poll(h, 500))
            r = query(h, "SELECT count(*) AS n FROM _zebridge_outbox")
            outbox = r[0]["n"] if r else -1
            if outbox == 0:
                break
        end = time.monotonic() + 10
        while time.monotonic() < end:
            take(lib.zb_client_poll(h, 500))

        def digest(sql):
            uids = [str(r["uid"]) for r in query(h, sql)]
            return {"count": len(uids), "md5": hashlib.md5(",".join(uids).encode()).hexdigest()}

        json.dump({
            "worker": WID, "kind": "py-sqlite", "principal": P, "tenant": tenant,
            "ticks": tick, **counters, "outboxLeft": outbox,
            "test_types": digest("SELECT uid FROM test_types WHERE deleted_at IS NULL "
                                 f"AND tenant_id = '{tenant}' ORDER BY uid"),
            "orders": digest("SELECT uid FROM orders ORDER BY uid"),
        }, open(REPORT, "w"))
        return 0
    finally:
        lib.zb_client_close(h)


if __name__ == "__main__":
    sys.exit(main())
