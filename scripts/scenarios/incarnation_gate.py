#!/usr/bin/env python3
"""The bridge's incarnation gate, with explicitly stamped payloads (no client library).

A write stamped `incarnation: n` belongs to incarnation n of its table. The bridge checks
the stamp in the write's own transaction, under a lock DROP must wait for
(`zebridge_private.lock_incarnations`), and an existing receipt always wins. Checked on a
private stack (own database, NATS, admission and bridge), on the fault build so the gate's
window can be held open (`ZB_TEST_FAULT=incarnation_pause` while a file exists):

  A  legacy and malformed: no field is applied unchecked; "7" and 0 are refused by
     admission at the door (InvalidIncarnation, nothing stored, no receipt; in direct mode
     the bridge records the same refusal); a current stamp is accepted. The descriptor has no `incarnation`
     while the switch is off.
  B  ordering 1, the write first: held past the gate, a DROP in another session waits in
     pg_locks until the write commits.
  C  receipts win: after the recreate, the identical retry (past the stream's duplicate
     window) is answered `accepted` from its receipt; the same ID with the new stamp is
     MessageIdReused; the replacement row keeps its exact value.
  D  ordering 2, the recreate first: an old stamp is rejected TableIncarnationChanged and the
     replacement row keeps its exact value; a stamp on a table dropped with no replacement
     is rejected too.
  E  one batch: a stale write, a current write on the same table and a write on another
     table. Only the stale one is refused; the batch path held (no per-write replay).
  F  parked release: an ordered write parked behind its predecessor, its table recreated
     meanwhile, is rejected when released.
  G  retryable: the writer without EXECUTE on the gate, or no incarnation recorded, leaves no
     receipt and no rejection; once fixed, the write is applied.
  H  preflight: a bridge whose writer may not run the gate does not start.
  I  advertising: with the switch on and the bridge restarted, the descriptor carries the
     table's current incarnation.
  J  an all-unstamped batch (switch off) commits as one batch, with no per-write replay.
  K  identity unknown while advertised: the incarnation row gone and the descriptor
     republished, it says `incarnation: null` and `writable: false`, and a stamped write is
     retried, not judged; the row restored and republished, advertising recovers and the
     write is applied.
  L  the policy unreadable (not "off"): descriptors withhold writes the same way, and
     recover once it can be read.
  M  a reload through one table republishes the other's descriptor when its advert
     changed: the switch off, then on, then the policy unreadable and readable again, each
     triggered by reseeding ig_t only, observed on ig_u.
  N  a cold start whose catalogue read fails (the reader may not read zebridge_catalogue)
     with the switch on: no writable descriptor goes out without `incarnation`. Observed:
     every table is refused (no rules, so no CDC subject) and published as a suspension,
     which carries no write contract; so this pins the outcome but does not exercise the
     load's default (`unknown`), which the catalogue unit test pins. Once the catalogue
     reads again, advertising recovers.

Needs a PostgreSQL superuser on 127.0.0.1:5432, nats-server and the fault build of the
bridge (`zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults`).
"""
import asyncio, json, os, pathlib, subprocess, sys, tempfile, time, uuid

import msgpack
import nats
import nats.js.errors
import zb
from recreate_recovery import Stack, Js

ROOT = pathlib.Path(__file__).resolve().parents[2]
FAULT_BRIDGE = ROOT / "zig-out-faults" / "bin" / "bridge"
PRINCIPAL, TENANT, CLIENT = "igp", "igt", "igc"
T, U, V = "ig_t", "ig_u", "ig_v"
OLD = "2026-01-01 00:00:00+00"  # a replacement row older than any write: LWW cannot hide a wrong target

failed = 0


def check(label, cond, detail=""):
    global failed
    (zb.ok if cond else zb.bad)(label + (f" — {detail}" if detail else ""))
    if not cond: failed += 1
    return cond


class Gate:
    def __init__(self, stack, tmp):
        self.s, self.tmp = stack, tmp
        self.ack = zb.TOPOLOGY["subjects"]["mutation_ack_prefix"]
        self.pause = tmp / "incarnation.pause"

    # ── database ──
    def create(self, table, rows=()):
        self.s.sql(f"CREATE TABLE public.{table} (uid uuid PRIMARY KEY, title text, tenant_id text NOT NULL, "
                   "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)")
        self.s.sql(f"SELECT count(*) FROM zebridge_enable('public.{table}'::regclass, writable => true, tenant_col => 'tenant_id', "
                   "version_col => 'updated_at', tombstone_col => 'deleted_at', publication => 'rr_pub', dry_run => false)")
        for uid, title, at in rows:
            self.s.sql(f"INSERT INTO public.{table} (uid, title, tenant_id, updated_at) VALUES ('{uid}', '{title}', '{TENANT}', {at})")

    def row(self, table, uid):
        return self.s.sql(f"SELECT title FROM public.{table} WHERE uid = '{uid}'")

    def inc(self, table):
        return int(self.s.sql(f"SELECT incarnation FROM public.zebridge_incarnations WHERE tbl = '{table}'") or 0)

    def receipt(self, mid):
        r = self.s.sql(f"SELECT coalesce(outcome->>'status','') || '|' || coalesce(outcome->>'reason','') FROM zebridge_private.receipts WHERE message_id = '{mid}'")
        return tuple(r.split("|", 1)) if r else None

    def version(self):
        return self.s.sql("SELECT to_char((clock_timestamp() + interval '1 second') AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")

    # ── NATS ──
    async def open(self):
        self.nc = await nats.connect(self.s.url, user_credentials=self.s.creds[PRINCIPAL], inbox_prefix=f"_INBOX.{PRINCIPAL}".encode())
        self.js = self.nc.jetstream(domain="hub")
        self.look = await Js().open(self.s)

    async def short_dedup(self):
        """Every mutation stream's duplicate window to one second: a retry then reaches the
        bridge's receipt lookup instead of being dropped by JetStream."""
        names = []
        for info in await self.look.js.streams_info():
            subjects = " ".join(info.config.subjects or [])
            if "mutation" in subjects and info.config.duplicate_window:
                info.config.duplicate_window = 1.0
                await self.look.js.update_stream(info.config)
                names.append(info.config.name)
        return names

    def mid(self, op, table):
        return f"zb1-t{int(time.time() * 1000)}-{op}-{CLIENT}-{table}-k1-{uuid.uuid4().hex}-v"

    def payload(self, uid, title, stamp=..., ordered=None, op="UPDATE"):
        p = {"key": {"uid": uid}, "data": {"uid": uid, "title": title, "tenant_id": TENANT}, "version": self.version(), "client_id": CLIENT}
        if stamp is not ...: p["incarnation"] = stamp
        if ordered is not None: p.update(ordering=1, predecessor=ordered or None)
        return msgpack.packb(p)

    async def send(self, table, mid, body, op="update"):
        """Publish and subscribe to its verdicts. Returns (subscription, held subscription, puback)."""
        sub = await self.nc.subscribe(f"{self.ack}.{PRINCIPAL}.{mid}")
        held = await self.nc.subscribe(f"{self.ack}.{PRINCIPAL}.{mid}.held")
        await self.nc.flush()
        subject = zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], PRINCIPAL, table) + f".{op}"
        ack = await self.js.publish(subject, body, headers={"Nats-Msg-Id": mid}, timeout=5)
        return sub, held, ack

    async def verdict(self, sub, timeout=20):
        try:
            m = await sub.next_msg(timeout=timeout)
        except Exception:
            return None
        d = zb.decode(m.data)
        return (d.get("status"), d.get("reason") or "")

    async def descriptor(self, table):
        return await self.look.kv(self.look.schemas, table)


def bridge_log(stack, label):
    return open(stack.log(label), errors="replace").read()


async def run(tmp):
    stack = Stack(tmp, "ig", 30, {"ZB_TEST_FAULT": "incarnation_pause"})
    stack.bridge_bin = FAULT_BRIDGE
    g = Gate(stack, tmp)
    try:
        stack.start({PRINCIPAL: TENANT})
        stack.env.update(MUTATION_ADMISSION="1", ZB_TEST_PAUSE_FILE=str(g.pause))
        writer = stack.name + "_w"
        r_a, r_hold, r_w = (str(uuid.uuid4()) for _ in range(3))
        g.create(T, [(r_a, "original", "now()"), (r_hold, "original", "now()"), (r_w, "original", "now()")])
        r_u = str(uuid.uuid4())
        g.create(U, [(r_u, "original", "now()")])
        stack.spawn("admission", [str(FAULT_BRIDGE), "--mutation-ingress"], stack.env)
        stack.wait_log("admission", "mutation admission ready")
        bridge = stack.bridge_up()
        await g.open()
        check("the mutation streams take a 1 s duplicate window", bool(await g.short_dedup()))
        inc_t1 = g.inc(T)
        check(f"{T} starts at incarnation {inc_t1}", inc_t1 >= 1)

        # ── A ──
        d = None
        end = time.monotonic() + 30
        while time.monotonic() < end and not isinstance(d, dict):
            d = await g.descriptor(T)
            if not isinstance(d, dict): await asyncio.sleep(0.5)
        check("A: switch off: the descriptor has no `incarnation`", isinstance(d, dict) and "incarnation" not in d, str(sorted(d)) if isinstance(d, dict) else str(d))
        for label, stamp, want in (("no field (legacy)", ..., ("accepted", "")),
                                   ('"7"', "7", "refused"),
                                   ("0", 0, "refused"),
                                   ("current", inc_t1, ("accepted", ""))):
            mid = g.mid("UPDATE", T)
            title = f"A {label}"
            if want == "refused":
                try:
                    await g.send(T, mid, g.payload(r_a, title, stamp)); why = "stored"
                except nats.js.errors.BadRequestError as e:
                    why = e.description or ""
                check(f"A: stamp {label}: refused by admission (InvalidIncarnation), no receipt, row unchanged",
                      "InvalidIncarnation" in why and g.receipt(mid) is None and g.row(T, r_a) != title, why)
                continue
            sub, _, _ = await g.send(T, mid, g.payload(r_a, title, stamp))
            got = await g.verdict(sub)
            check(f"A: stamp {label}: {want[0]} {want[1]}".rstrip(), got == want and g.receipt(mid) == want, f"verdict {got}, receipt {g.receipt(mid)}")

        # ── J: an all-unstamped batch, the switch off ──
        stack.bridge_down(bridge)
        unstamped = [(T, g.mid("UPDATE", T), g.payload(r_a, "J unstamped a")),
                     (T, g.mid("UPDATE", T), g.payload(r_w, "J unstamped w")),
                     (U, g.mid("UPDATE", U), g.payload(r_u, "J unstamped u"))]
        subs = [(await g.send(t, m, b))[0] for t, m, b in unstamped]
        bridge = stack.bridge_up()
        results = [await g.verdict(x) for x in subs]
        check("J: three unstamped writes, all accepted", results == [("accepted", "")] * 3, str(results))
        check("J: they committed as one batch (no per-write replay)", "replayed one write at a time" not in bridge_log(stack, bridge))

        # ── B: the write holds the gate's lock, a DROP waits for it ──
        g.pause.touch()
        mid_hold = g.mid("UPDATE", T)
        sub_hold, _, _ = await g.send(T, mid_hold, g.payload(r_hold, "held past the gate", inc_t1))
        end = time.monotonic() + 20
        while time.monotonic() < end and "ZB_TEST_FAULT incarnation_pause fired" not in bridge_log(stack, bridge):
            await asyncio.sleep(0.05)
        paused = "ZB_TEST_FAULT incarnation_pause fired" in bridge_log(stack, bridge)
        check("B: the write is held between the gate and its statement", paused)
        drop = subprocess.Popen([*stack.psql, "-d", stack.name, "-c", f"DROP TABLE public.{T}"], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        await asyncio.sleep(1.5)
        waiting = stack.sql(f"SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation WHERE c.relname = '{T}' "
                            "AND l.mode = 'AccessExclusiveLock' AND NOT l.granted")
        check("B: the DROP waits on the gate's lock (pg_locks: AccessExclusiveLock not granted)", drop.poll() is None and waiting == "1", f"waiting {waiting}")
        g.pause.unlink()
        got = await g.verdict(sub_hold)
        drop.wait(timeout=30)
        check("B: the held write commits first, then the DROP completes", got == ("accepted", "") and drop.returncode == 0,
              f"verdict {got}, drop rc {drop.returncode}")
        # The recreate: same keys, older versions, distinctive values.
        g.create(T, [(r_a, "replacement a", f"'{OLD}'"), (r_hold, "replacement hold", f"'{OLD}'"), (r_w, "replacement w", f"'{OLD}'")])
        inc_t2 = g.inc(T)
        check(f"B: the recreate moved {T} to incarnation {inc_t2}", inc_t2 == inc_t1 + 1)

        # ── C: an existing receipt wins. W commits on incarnation 2, the table is recreated
        #    again, and W is retried byte for byte past the duplicate window. ──
        mid_w = g.mid("UPDATE", T)
        body_w = g.payload(r_w, "W committed before the recreate", inc_t2)
        sub_w, _, _ = await g.send(T, mid_w, body_w)
        check("C: W committed on incarnation " + str(inc_t2), await g.verdict(sub_w) == ("accepted", ""))
        g.s.sql(f"DROP TABLE public.{T}")
        g.create(T, [(r_a, "replacement a", f"'{OLD}'"), (r_w, "replacement w", f"'{OLD}'")])
        inc_t3 = g.inc(T)
        await asyncio.sleep(1.5)
        sub_r, _, puback = await g.send(T, mid_w, body_w)
        got = await g.verdict(sub_r)
        check("C: the identical retry reached the bridge (not a JetStream duplicate)", not getattr(puback, "duplicate", False))
        check("C: the retry is answered from W's receipt (accepted), not judged again", got == ("accepted", "") and g.receipt(mid_w) == ("accepted", ""), f"verdict {got}")
        check("C: the replacement row keeps its exact value", g.row(T, r_w) == "replacement w", g.row(T, r_w))
        await asyncio.sleep(1.5)
        sub_c, _, _ = await g.send(T, mid_w, g.payload(r_w, "W again, new stamp", inc_t3))
        got = await g.verdict(sub_c)
        check("C: the same ID with another stamp is a fingerprint conflict (indeterminate MessageIdReused)",
              got == ("indeterminate", "MessageIdReused") and g.row(T, r_w) == "replacement w", f"verdict {got}, row {g.row(T, r_w)}")

        # ── D: the recreate first ──
        mid_x = g.mid("UPDATE", T)
        sub_x, _, _ = await g.send(T, mid_x, g.payload(r_a, "an edit of the old row a", inc_t2))
        got = await g.verdict(sub_x)
        check("D: an old stamp is rejected TableIncarnationChanged, durably", got == ("rejected", "TableIncarnationChanged") and g.receipt(mid_x) == got, f"verdict {got}")
        check("D: the replacement row keeps its exact value", g.row(T, r_a) == "replacement a", g.row(T, r_a))
        r_v = str(uuid.uuid4())
        g.create(V, [(r_v, "original", "now()")])
        inc_v = g.inc(V)
        g.s.sql(f"DROP TABLE public.{V}")
        mid_v = g.mid("UPDATE", V)
        sub_v, _, _ = await g.send(V, mid_v, g.payload(r_v, "an edit of a dropped table", inc_v))
        got = await g.verdict(sub_v)
        check("D: a table dropped with no replacement: rejected TableIncarnationChanged", got == ("rejected", "TableIncarnationChanged"), f"verdict {got}")

        # ── E: one batch, three writes ──
        r_new = str(uuid.uuid4())
        g.s.sql(f"INSERT INTO public.{T} (uid, title, tenant_id) VALUES ('{r_new}', 'original', '{TENANT}')")
        stack.bridge_down(bridge)
        batch = [(T, g.mid("UPDATE", T), g.payload(r_a, "stale in a batch", inc_t2), ("rejected", "TableIncarnationChanged")),
                 (T, g.mid("UPDATE", T), g.payload(r_new, "current in a batch", inc_t3), ("accepted", "")),
                 (U, g.mid("UPDATE", U), g.payload(r_u, "other table in a batch", g.inc(U)), ("accepted", ""))]
        subs = [(await g.send(t, m, b))[0] for t, m, b, _ in batch]
        bridge = stack.bridge_up()
        results = [await g.verdict(s) for s in subs]
        check("E: in one batch the stale write is refused and the two valid ones commit",
              [r for r in results] == [w for *_, w in batch], str(results))
        check("E: rows: the replacement kept, the current write applied, the other table applied",
              (g.row(T, r_a), g.row(T, r_new), g.row(U, r_u)) == ("replacement a", "current in a batch", "other table in a batch"))
        check("E: the batch path held (no per-write replay)", "replayed one write at a time" not in bridge_log(stack, bridge))

        # ── F: a parked write released after its table was recreated ──
        r_f = str(uuid.uuid4())
        g.s.sql(f"INSERT INTO public.{T} (uid, title, tenant_id) VALUES ('{r_f}', 'original', '{TENANT}')")
        mid_p = g.mid("UPDATE", U)
        mid_c = g.mid("UPDATE", T)
        sub_child, held_child, _ = await g.send(T, mid_c, g.payload(r_f, "child of P", g.inc(T), ordered=mid_p))
        held = await g.verdict(held_child, timeout=20)
        check("F: the child is parked behind its predecessor", held is not None, str(held))
        g.s.sql(f"DROP TABLE public.{T}")
        g.create(T, [(r_f, "replacement f", f"'{OLD}'")])
        sub_parent, _, _ = await g.send(U, mid_p, g.payload(r_u, "parent P", g.inc(U), ordered=""))
        check("F: the predecessor commits", await g.verdict(sub_parent) == ("accepted", ""))
        got = await g.verdict(sub_child, timeout=40)
        check("F: the released child is rejected TableIncarnationChanged", got == ("rejected", "TableIncarnationChanged"), f"verdict {got}")
        check("F: its replacement row keeps its value", g.row(T, r_f) == "replacement f", g.row(T, r_f))

        # ── G: failures that must stay retryable ──
        g.s.sql(f"REVOKE EXECUTE ON FUNCTION zebridge_private.lock_incarnations(text[]) FROM {writer}")
        mid_g = g.mid("UPDATE", U)
        sub_g, _, _ = await g.send(U, mid_g, g.payload(r_u, "after a permission fix", g.inc(U)))
        early = await g.verdict(sub_g, timeout=4)
        check("G: without EXECUTE on the gate: no rejection, no receipt (retried)", early in (None, ("failed", "IncarnationUnavailable")) and g.receipt(mid_g) is None, f"verdict {early}")
        g.s.sql(f"GRANT EXECUTE ON FUNCTION zebridge_private.lock_incarnations(text[]) TO {writer}")
        got = early if early == ("accepted", "") else await g.verdict(sub_g, timeout=30)
        check("G: once granted, the write is applied", got == ("accepted", "") and g.row(U, r_u) == "after a permission fix", f"verdict {got}")
        saved = g.s.sql(f"SELECT incarnation || ',' || incarnation_relid::text || ',' || seed_epoch FROM public.zebridge_incarnations WHERE tbl = '{U}'")
        g.s.sql(f"DELETE FROM public.zebridge_incarnations WHERE tbl = '{U}'")
        mid_k = g.mid("UPDATE", U)
        sub_k, _, _ = await g.send(U, mid_k, g.payload(r_u, "after the record came back", int(saved.split(",")[0])))
        early = await g.verdict(sub_k, timeout=4)
        check("G: no incarnation recorded: no rejection, no receipt (retried)", early in (None, ("failed", "IncarnationUnavailable")) and g.receipt(mid_k) is None, f"verdict {early}")
        inc_s, relid_s, epoch_s = saved.split(",")
        g.s.sql(f"INSERT INTO public.zebridge_incarnations (tbl, incarnation_relid, incarnation, seed_epoch) VALUES ('{U}', {relid_s}, {inc_s}, {epoch_s})")
        got = await g.verdict(sub_k, timeout=30)
        check("G: once recorded again, the write is applied", got == ("accepted", ""), f"verdict {got}")

        # ── H: preflight ──
        stack.bridge_down(bridge)
        g.s.sql(f"REVOKE EXECUTE ON FUNCTION zebridge_private.lock_incarnations(text[]) FROM {writer}")
        # The receipts check runs as the mutation listener starts, after replication is up:
        # wait for the refusal itself, then for the process to exit with it.
        refused, label = False, None
        try:
            label = stack.bridge_up(ready="FATAL: the writer role may not use the mutation receipts")
            p = stack.procs[label]
            p.wait(timeout=30)
            refused = p.returncode != 0 and "ReceiptsNotInstalled" in bridge_log(stack, label)
        except (RuntimeError, subprocess.TimeoutExpired):
            refused = False
        finally:
            if label and label in stack.procs: stack.bridge_down(label)
        check("H: a bridge whose writer may not run the gate refuses to start (ReceiptsNotInstalled)", refused)
        g.s.sql(f"GRANT EXECUTE ON FUNCTION zebridge_private.lock_incarnations(text[]) TO {writer}")

        # ── I: advertising ──
        g.s.sql("UPDATE zebridge_private.write_policy SET incarnation_stamps = true WHERE singleton")
        bridge = stack.bridge_up()
        d = None
        end = time.monotonic() + 30
        while time.monotonic() < end:
            d = await g.descriptor(T)
            if isinstance(d, dict) and "incarnation" in d: break
            await asyncio.sleep(0.5)
        check(f"I: switch on: the descriptor carries {T}'s current incarnation ({g.inc(T)})", isinstance(d, dict) and d.get("incarnation") == g.inc(T), str(d.get("incarnation") if isinstance(d, dict) else d))

        async def descriptor_where(pred, label):
            end, d = time.monotonic() + 30, None
            while time.monotonic() < end:
                d = await g.descriptor(T)
                if isinstance(d, dict) and pred(d): return d
                await asyncio.sleep(0.5)
            return d

        def republish():
            g.s.sql(f"SELECT zebridge_reseed('{T}')")

        # ── K: identity unknown while advertised ──
        saved = g.s.sql(f"SELECT incarnation || ',' || incarnation_relid::text FROM public.zebridge_incarnations WHERE tbl = '{T}'")
        inc_s, relid_s = saved.split(",")
        epoch0 = (await g.descriptor(T) or {}).get("seed_epoch", 0)
        g.s.sql(f"DELETE FROM public.zebridge_incarnations WHERE tbl = '{T}'")
        republish()
        d = await descriptor_where(lambda d: d.get("seed_epoch", 0) > epoch0, "republished")
        check("K: identity unknown, advertised: the descriptor says incarnation null and writes withheld",
              isinstance(d, dict) and "incarnation" in d and d["incarnation"] is None and d.get("writable") is False,
              str({k: d.get(k) for k in ("seed_epoch", "incarnation", "writable")} if isinstance(d, dict) else d))
        r_k = str(uuid.uuid4())
        g.s.sql(f"INSERT INTO public.{T} (uid, title, tenant_id) VALUES ('{r_k}', 'original', '{TENANT}')")
        mid_k2 = g.mid("UPDATE", T)
        sub_k2, _, _ = await g.send(T, mid_k2, g.payload(r_k, "K stamped while unknown", int(inc_s)))
        early = await g.verdict(sub_k2, timeout=4)
        check("K: a stamped write meanwhile is retried, not judged (no receipt)", early in (None, ("failed", "IncarnationUnavailable")) and g.receipt(mid_k2) is None, f"verdict {early}")
        g.s.sql(f"UPDATE public.zebridge_incarnations SET incarnation = {inc_s}, incarnation_relid = {relid_s} WHERE tbl = '{T}'")
        epoch1 = d.get("seed_epoch", 0) if isinstance(d, dict) else 0
        republish()
        d = await descriptor_where(lambda d: d.get("seed_epoch", 0) > epoch1, "recovered")
        check(f"K: restored and republished: advertising recovers (incarnation {inc_s}, writes not withheld)",
              isinstance(d, dict) and d.get("incarnation") == int(inc_s) and d.get("writable") is not False,
              str({k: d.get(k) for k in ("seed_epoch", "incarnation", "writable")} if isinstance(d, dict) else d))
        got = await g.verdict(sub_k2, timeout=30)
        check("K: the stamped write is applied once the identity is known", got == ("accepted", "") and g.row(T, r_k) == "K stamped while unknown", f"verdict {got}")

        # ── L: the policy unreadable is not "off" ──
        reader = stack.name + "_r"
        g.s.sql(f"REVOKE EXECUTE ON FUNCTION public.zebridge_incarnation_stamps() FROM {reader}, {writer}")
        epoch2 = d.get("seed_epoch", 0) if isinstance(d, dict) else 0
        republish()
        d = await descriptor_where(lambda d: d.get("seed_epoch", 0) > epoch2, "unreadable")
        check("L: the policy unreadable: writes withheld (incarnation null, writable false), not advertised as off",
              isinstance(d, dict) and "incarnation" in d and d["incarnation"] is None and d.get("writable") is False,
              str({k: d.get(k) for k in ("seed_epoch", "incarnation", "writable")} if isinstance(d, dict) else d))
        g.s.sql(f"GRANT EXECUTE ON FUNCTION public.zebridge_incarnation_stamps() TO {reader}, {writer}")
        epoch3 = d.get("seed_epoch", 0) if isinstance(d, dict) else 0
        republish()
        d = await descriptor_where(lambda d: d.get("seed_epoch", 0) > epoch3, "readable")
        check("L: readable again: advertising recovers", isinstance(d, dict) and d.get("incarnation") == g.inc(T) and d.get("writable") is not False,
              str({k: d.get(k) for k in ("seed_epoch", "incarnation", "writable")} if isinstance(d, dict) else d))

        # ── M: a reload through ig_t republishes ig_u when its advert changed ──
        def summary(d):
            return {k: d.get(k, "absent") for k in ("incarnation", "writable", "suspended", "reason")} if isinstance(d, dict) else d

        async def u_where(pred):
            end, d = time.monotonic() + 30, None
            while time.monotonic() < end:
                d = await g.descriptor(U)
                if isinstance(d, dict) and pred(d): return True, d
                await asyncio.sleep(0.5)
            return False, d

        inc_u = g.inc(U)
        ok, d = await u_where(lambda d: d.get("incarnation") == inc_u)
        check(f"M: before: {U} advertises incarnation {inc_u}", ok, str(summary(d)))
        steps = (("the switch off", "UPDATE zebridge_private.write_policy SET incarnation_stamps = false WHERE singleton",
                  lambda d: "incarnation" not in d and d.get("writable") is not False),
                 ("the switch on", "UPDATE zebridge_private.write_policy SET incarnation_stamps = true WHERE singleton",
                  lambda d: d.get("incarnation") == inc_u),
                 ("the policy unreadable", f"REVOKE EXECUTE ON FUNCTION public.zebridge_incarnation_stamps() FROM {reader}, {writer}",
                  lambda d: "incarnation" in d and d["incarnation"] is None and d.get("writable") is False),
                 ("the policy readable", f"GRANT EXECUTE ON FUNCTION public.zebridge_incarnation_stamps() TO {reader}, {writer}",
                  lambda d: d.get("incarnation") == inc_u and d.get("writable") is not False))
        for label, sql, want in steps:
            g.s.sql(sql)
            republish()  # through ig_t only
            ok, d = await u_where(want)
            check(f"M: {label}, reload through {T}: {U}'s descriptor follows", ok, str(summary(d)))

        # ── N: a cold start whose catalogue read fails, the switch on ──
        stack.bridge_down(bridge)
        g.s.sql(f"REVOKE SELECT ON public.zebridge_catalogue FROM {reader}")
        bucket = g.look.schemas
        for table in (T, U):
            try: await g.look.js.purge_stream(f"KV_{bucket}", subject=f"$KV.{bucket}.{table}")
            except Exception: pass
        cold, label_n = None, None
        try:
            label_n = stack.bridge_up()
            end = time.monotonic() + 30
            while time.monotonic() < end:
                cold = {t: await g.descriptor(t) for t in (T, U)}
                if all(isinstance(v, dict) for v in cold.values()): break
                await asyncio.sleep(0.5)
        except RuntimeError as e:
            cold = str(e)[-300:]
        invites = isinstance(cold, dict) and any(isinstance(v, dict) and "incarnation" not in v and v.get("writable") is True for v in cold.values())
        check("N: catalogue unreadable at boot, switch on: no writable descriptor without `incarnation` (tables suspended)",
              isinstance(cold, dict) and all(isinstance(v, dict) for v in cold.values()) and not invites,
              str({t: summary(v) for t, v in cold.items()} if isinstance(cold, dict) else cold))
        if label_n and label_n in stack.procs: stack.bridge_down(label_n)
        g.s.sql(f"GRANT SELECT ON public.zebridge_catalogue TO {reader}")
        bridge = stack.bridge_up()
        ok, d = await u_where(lambda d: d.get("incarnation") == g.inc(U) and d.get("writable") is not False)
        check("N: the catalogue readable again: advertising recovers", ok, str(summary(d)))
        await g.nc.close(); await g.look.nc.close()
    finally:
        stack.stop()
    return 1 if failed else 0


def main():
    if not FAULT_BRIDGE.exists():
        print(f"{FAULT_BRIDGE} missing: zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults"); return 2
    with tempfile.TemporaryDirectory(prefix="zb-incarnation-gate-") as tmp:
        return asyncio.run(run(pathlib.Path(tmp)))


if __name__ == "__main__":
    sys.exit(main())
