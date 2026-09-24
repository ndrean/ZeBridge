#!/usr/bin/env python3
"""Two editors, one route, no lost move — the cooperative document over LWW (NOTES §10ho).

`routes.doc` is a jsonb map of registers, `{"start": {v, t, w}, "end": {v, t, w}}`.
Two libzb clients, two principals of two tenants, edit the ONE demo row at once: one
keeps moving the start, the other the end, then both move the end. Each write ships the
union of the writer's own registers merged into the document it last saw (the local
replica's row, fed by the CDC echo), and each editor reconciles until what it observes
contains what it wrote — the rule crdt.py proved on a scratch column, here on the map's
own table, through the client library the phone uses, with the merge from the library
(`zb_call("mergeRegisters")`), not reimplemented.

Asserted:
  A  every move survives: after the war, PostgreSQL's document holds both editors'
     last positions of the ends they own — the row-level LWW underneath lost none;
  B  the contested end holds the (t, w) winner, and both replicas agree on it;
  C  both replicas equal PostgreSQL's document — convergence, not just acceptance;
  D  the reconcile rounds are bounded.

Usage:  scripts/scenarios/.venv/bin/python scripts/scenarios/route_crdt.py
"""
import ctypes, json, os, pathlib, sys, tempfile, time
from datetime import datetime, timezone

import zb
from clients import Lib

ROUTE_ID = "11111111-1111-4111-8111-111111111111"
ROUNDS = 6


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class Editor:
    """A libzb client editing the route: its own registers, the row it sees, the merge from the library."""

    def __init__(self, principal: str, client_id: str, tmp: pathlib.Path):
        self.who, self.client_id = principal, client_id
        self.lib = Lib(str(tmp / f"{principal}.sqlite3"), ["routes"], client_id=client_id, principal=principal)
        l = self.lib.lib
        l.zb_call.restype, l.zb_call.argtypes = ctypes.c_void_p, [ctypes.c_char_p, ctypes.c_char_p]
        self.mine: dict = {}
        self.rewrites = 0

    def merge(self, a: dict, b: dict) -> dict:
        return self.lib.take(self.lib.lib.zb_call(b"mergeRegisters", json.dumps({"a": a, "b": b}).encode()))

    def seen(self) -> dict:
        rows = self.lib.take(self.lib.lib.zb_client_query(self.lib.h, b"SELECT doc FROM routes WHERE id = ?", json.dumps([ROUTE_ID]).encode()))["rows"]
        raw = rows[0][0] if rows else None
        return json.loads(raw) if isinstance(raw, str) and raw else (raw or {})

    def move(self, key: str, lat: float, lng: float):
        self.mine[key] = {"v": {"lat": lat, "lng": lng}, "t": now_iso(), "w": self.client_id}
        self.write()

    def write(self):
        doc = self.merge(self.seen(), self.mine)
        r = self.lib.take(self.lib.lib.zb_client_mutate(self.lib.h, b"routes", b"UPDATE", json.dumps({"id": ROUTE_ID}).encode(), json.dumps({"doc": doc}).encode()))
        if "error" in r:
            zb.bad(f"{self.who}: mutate refused: {r}")
        self.rewrites += 1

    def poll(self):
        self.lib.take(self.lib.lib.zb_client_flush_outbox(self.lib.h, 200))
        return self.lib.take(self.lib.lib.zb_client_poll(self.lib.h, 200))

    def reconcile(self) -> bool:
        """Observed ⊇ mine? If not, write the union again. True when settled."""
        seen = self.seen()
        merged = self.merge(seen, self.mine)
        if merged == seen:
            return True
        self.write()
        return False


def main() -> int:
    failed = 0
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb-route-crdt-"))
    zb.psql(f"UPDATE public.routes SET doc = '{{}}'::jsonb, updated_at = now() WHERE id = '{ROUTE_ID}'", quiet=True)
    a = Editor("omar", "c-phone", tmp)     # kilo
    b = Editor("alice", "c-browser", tmp)  # acme
    for e in (a, b):
        for _ in range(5):
            e.poll()
    # ── the war: a moves start, b moves end, interleaved, no waiting for echoes ──
    t0 = time.time()
    for i in range(ROUNDS):
        a.move("start", 47.2000 + i * 0.001, -1.5500)
        b.move("end", 47.2100, -1.5400 - i * 0.001)
        a.poll(); b.poll()
    # ── then both move the SAME end: one register, one winner ──
    a.move("end", 47.2999, -1.5999)
    b.move("end", 47.2888, -1.5888)
    # ── reconcile to a fixed point, both sides ──
    settled = {a.who: False, b.who: False}
    for _round in range(12):
        a.poll(); b.poll()
        settled[a.who] = a.reconcile()
        settled[b.who] = b.reconcile()
        if all(settled.values()):
            break
        time.sleep(0.5)
    for _ in range(6):
        a.poll(); b.poll(); time.sleep(0.3)
    war_ms = round((time.time() - t0) * 1000)

    pg = json.loads(zb.psql(f"SELECT doc FROM public.routes WHERE id = '{ROUTE_ID}'", quiet=True).strip() or "{}")
    # A: every move survives — each editor's last own position of the end it OWNED
    want_start, want_end_b = a.mine["start"], b.mine["end"]
    if pg.get("start") == want_start:
        zb.ok(f"{a.who}'s last start survived the war ({want_start['v']})")
    else:
        zb.bad(f"{a.who}'s start lost: PostgreSQL holds {pg.get('start')} — wanted {want_start}"); failed += 1
    # B: the contested end holds the (t, w) winner
    winner = a.merge({"end": a.mine["end"]}, {"end": want_end_b})["end"]
    if pg.get("end") == winner:
        zb.ok(f"the contested end holds the (t, w) winner: {winner['w']} at {winner['v']}")
    else:
        zb.bad(f"the contested end is {pg.get('end')} — the (t, w) winner was {winner}"); failed += 1
    # C: both replicas equal PostgreSQL
    for e in (a, b):
        if e.seen() == pg:
            zb.ok(f"{e.who}'s replica equals PostgreSQL's document ({len(pg)} register(s))")
        else:
            zb.bad(f"{e.who}'s replica differs from PostgreSQL: {e.seen()} vs {pg}"); failed += 1
    # D: bounded
    if all(settled.values()) and a.rewrites + b.rewrites <= 2 * (ROUNDS + 1) + 12:
        zb.ok(f"settled: {a.rewrites} + {b.rewrites} writes for {2 * (ROUNDS + 1)} moves, {war_ms} ms end to end")
    else:
        zb.bad(f"not settled or unbounded: {settled}, {a.rewrites} + {b.rewrites} writes"); failed += 1
    for e in (a, b):
        e.lib.lib.zb_client_close(e.lib.h)
    return failed


if __name__ == "__main__":
    sys.exit(main())
