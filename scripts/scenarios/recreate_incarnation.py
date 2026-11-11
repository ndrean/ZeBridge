#!/usr/bin/env python3
"""A queued write belongs to the table incarnation it was made against.

A client edits row K while offline; meanwhile the table is dropped and re-created under
its name, with a row of the same key K and a distinguishable value. The replacement row
is OLDER than the edit (`updated_at` in the past, as rows restored from a backup are), so
last-writer-wins cannot hide a write that reaches the wrong incarnation.

  case 1  offline UPDATE of K across a recreate. Contract: the bridge records `rejected`
          / `TableIncarnationChanged` for the original ID, the app sees exactly one
          terminal outcome, the replacement value survives, the outbox and undo queue
          drain, the replica equals PostgreSQL.
  case 4  the control: `zebridge_reseed` moves the seed epoch, the incarnation stays. The
          same offline UPDATE is eligible: applied, outbox drained, replica exact.

Each case runs on both clients, one at a time: libzb through a relay that cuts its NATS
link (its ordinary reconnect path, schema history included), zb-client-ts hung up.

Exit status (it starts in `known_red`):
  1  the known defects: both controls pass; zb-client-ts reports the write applied and
     PostgreSQL holds the old edit on the replacement row; libzb shows one of two
     signatures for the original ID, depending on what its reconnect does first:
       applied   the outbox flush first: the receipt is `accepted`, the app sees
                 `applied`, PostgreSQL holds the old edit on the replacement row;
       discarded the DROP first: the discard is logged, the ID leaves its outbox, no
                 receipt, no terminal outcome through the observation window, and
                 PostgreSQL keeps the replacement value;
       silent    applied to replacement, no app outcome after DROP discard: the receipt is
                 `accepted`, PostgreSQL holds the old edit, schema replay logged the discard,
                 the ID left the outbox and the queues drained, and the app recorded no
                 terminal outcome through the observation window. (The logs do not say when
                 the wire verdict arrived; the signature does not assume it.)
     The contract must hold whichever comes first; one signature suffices.
  0  the contract, on both clients (case 1 above).
  2  setup incomplete: a case did not run, or a precondition (the write queued offline,
     the replacement older than the edit) could not be established.
  3  anything else, a failed control or only one client fixed included.
"""
import json, os, re, signal, socket, subprocess, sys, tempfile, time, uuid
import zb
from clients import Lib, Node, fresh_sqlite

LOG = "/tmp/zb_recreate_incarnation_bridge.log"
NODE_LOG = "/tmp/zb_recreate_incarnation_node.log"
RELAY_LOG = "/tmp/zb_recreate_incarnation_relay.log"
T = "ri_rows"
PUB = zb.publication()
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"generations => true, publication => '{PUB}', dry_run => false")
REPLACEMENT = "replacement row"
RESTORED_AT = "2026-01-01 00:00:00+00"
RESTORED_MS = 1767225600000  # RESTORED_AT in epoch milliseconds
OBSERVE_S = 20  # after the outbox drains, how long the app is watched for a late outcome
DISCARD = re.compile(rf"{T}: \d+ queued write\(s\) discarded — the table was dropped")


def until(pred, budget, step=0.1):
    end = time.monotonic() + budget
    while time.monotonic() < end:
        if pred(): return True
        time.sleep(step)
    return False


def create():
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY, title text, tenant_id varchar(255) NOT NULL, "
            "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)")
    out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE})")
    if "error" in out.lower(): raise RuntimeError(f"zebridge_enable: {out}")


def teardown():
    zb.forget_table(T)
    for sql in (f"DROP TABLE IF EXISTS public.{T}", f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'",
                f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'"):
        zb.psql(sql, quiet=True)


def incarnation():
    r = zb.psql(f"SELECT incarnation || '|' || seed_epoch FROM public.zebridge_incarnations WHERE tbl = '{T}'")
    inc, ep = (r or "0|0").split("|")
    return int(inc), int(ep)


def pg_title(uid):
    return zb.psql(f"SELECT title FROM public.{T} WHERE uid = '{uid}'")


def receipt(mid):
    """The bridge's receipt for a message ID: (status, reason), or None."""
    r = zb.psql(f"SELECT coalesce(outcome->>'status', '') || '|' || coalesce(outcome->>'reason', '') "
                f"FROM zebridge_private.receipts WHERE message_id = '{mid}'")
    return tuple(r.split("|", 1)) if r else None


def created_ms(mid):
    """The creation time a structured message ID carries (`zb1-t<ms>-…`)."""
    m = re.match(r"zb1-t(\d+)-", mid or "")
    return int(m.group(1)) if m else None


class Relay:
    """netem.py between libzb and NATS: SIGUSR1 cuts every connection, SIGUSR2 restores."""
    def __init__(self):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0)); self.port = s.getsockname()[1]
        target = zb.nats_server().split("://")[-1]
        self.log = open(RELAY_LOG, "w")
        self.p = subprocess.Popen([sys.executable, str(zb.ROOT / "scripts" / "scenarios" / "netem.py"),
                                   "--listen", str(self.port), "--to", target], stdout=self.log, stderr=subprocess.STDOUT)
        if not until(lambda: "listening" in open(RELAY_LOG).read(), 10): raise RuntimeError("relay did not start")
        self.url = f"nats://127.0.0.1:{self.port}"
    def cut(self): self.p.send_signal(signal.SIGUSR1)
    def restore(self): self.p.send_signal(signal.SIGUSR2)
    def close(self):
        self.p.terminate()
        try: self.p.wait(timeout=5)
        except subprocess.TimeoutExpired: self.p.kill()
        self.log.close()


class Side:
    """One client: how it goes offline, queues, comes back, and what it reports."""
    def __init__(self, kind, relay):
        self.kind = kind
        self.db = f"/tmp/zb-ri-{kind}.sqlite3"; fresh_sqlite(self.db)
        self.err = None
        if kind == "libzb":
            # libzb logs on this process's stderr: captured for the positive evidence of a
            # discard, and copied back to the scenario log when the client closes.
            self.err = tempfile.NamedTemporaryFile("w+", prefix="zb_ri_libzb_", suffix=".log", delete=False)
            sys.stderr.flush(); self.saved = os.dup(2); os.dup2(self.err.fileno(), 2)
            self.relay = relay
            self.c = Lib(self.db, [T], "py-recreate-incarnation", nats_url=relay.url)
        else:
            self.c = Node(self.db, NODE_LOG)
        self.tenant = self.c.tenant
        self.undo = "_zbz_undo" if kind == "libzb" else "_zebridge_undo"
    def log_text(self):
        if self.kind == "libzb":
            self.err.flush(); return open(self.err.name, errors="replace").read()
        return open(NODE_LOG, errors="replace").read()
    def poll(self):
        if self.kind == "libzb": self.c.flush(0); self.c.poll(200)
    def offline(self):
        if self.kind == "libzb": self.relay.cut(); time.sleep(0.5)
        else: self.c.disconnect()
    def online(self):
        if self.kind == "libzb": self.relay.restore()
        else: self.c.connect()
    def update(self, uid, title):
        r = self.c.mutate(T, "UPDATE", {"uid": uid}, {"title": title}) if self.kind == "libzb" else \
            self.c.mutate(T, "UPDATE", {"uid": uid}, {"title": title}, with_id=True)
        return r.get("msgId") or r.get("writeId")
    def queued(self, mid):
        return self.c.q("SELECT count(*) FROM _zebridge_outbox WHERE msg_id = ?", (mid,))[0][0]
    def drained(self):
        return self.c.q("SELECT count(*) FROM _zebridge_outbox")[0][0] == 0 and \
               self.c.q(f"SELECT count(*) FROM {self.undo} WHERE mode <> 'accepted'")[0][0] == 0
    def local_title(self, uid):
        rows = self.c.q(f"SELECT title FROM {T} WHERE uid = ?", (uid,))
        return rows[0][0] if rows else None
    def outcomes(self, uid, mid):
        """Terminal outcomes the application saw for this write: (outcome, reason)."""
        if self.kind == "libzb":
            return [(o.get("outcome"), o.get("reason") or "") for m, o in self.c.outcomes.items() if m == mid]
        out = []
        for line in open(NODE_LOG, errors="replace"):
            if "[VERDICT-API]" not in line: continue
            try: v = json.loads(line.split("[VERDICT-API]", 1)[1])
            except ValueError: continue
            if v.get("table") == T and v.get("key", {}).get("uid") == uid:
                out.append((v.get("outcome"), v.get("reason") or ""))
        return out
    def seeded(self):
        anchor = "_zbz_generations" if self.kind == "libzb" else "_zebridge_generations"
        self.poll()
        try: return self.c.q(f"SELECT count(*) FROM {anchor} WHERE tbl = '{T}'")[0][0] >= 1
        except RuntimeError: return False
    def matches_pg(self):
        self.poll()
        want = zb.psql(f"SELECT string_agg(uid::text || '=' || coalesce(title, ''), ',' ORDER BY uid) FROM public.{T} WHERE deleted_at IS NULL") or ""
        try: rows = self.c.q(f"SELECT uid, title FROM {T} ORDER BY uid")
        except RuntimeError: return False
        return ",".join(f"{u}={t or ''}" for u, t in rows) == want
    def restart(self):
        """Close and reopen on the same replica file (the outbox survives on disk)."""
        try: self.c.close()
        except Exception: pass
        if self.kind == "libzb":
            self.c = Lib(self.db, [T], "py-recreate-incarnation", nats_url=self.relay.url)
        else:
            self.c = Node(self.db, NODE_LOG)
    def close(self):
        try: self.c.close()
        except Exception: pass
        if self.err:
            sys.stderr.flush(); os.dup2(self.saved, 2); os.close(self.saved)
            sys.stderr.write(self.log_text()); sys.stderr.flush()
            self.err.close(); os.unlink(self.err.name)


def run_case(side, change, label):
    """Seed row K, go offline, queue an UPDATE of K, change the table, come back, watch.
    Returns the facts the classifier reads, or {"setup": why} when a precondition failed."""
    uid = str(uuid.uuid4())
    zb.psql(f"INSERT INTO public.{T} (uid, title, tenant_id) VALUES ('{uid}', 'original', '{side.tenant}')")
    if not until(lambda: (side.poll(), side.local_title(uid))[1] == "original", 60):
        return {"setup": "row K never reached the replica"}
    before = incarnation()
    log_from = len(side.log_text())
    side.offline()
    edit = f"edited offline by {side.kind}"
    mid = side.update(uid, edit)
    if not mid or side.queued(mid) != 1:
        return {"setup": f"the edit was not queued offline (id {mid!r})"}
    change(uid, side.tenant)
    after = incarnation()
    side.online()
    left = until(lambda: (side.poll(), side.queued(mid) == 0)[1], 90, 0.3)
    # The observation window: a late outcome (a slow verdict) would show up here.
    end = time.monotonic() + OBSERVE_S
    while time.monotonic() < end: side.poll(); time.sleep(0.3)
    converged = until(side.matches_pg, 60, 0.3)
    facts = {"id": mid, "edit": edit, "before": before, "after": after, "pg": pg_title(uid),
             "left_outbox": left, "drained": side.drained(), "converged": converged,
             "outcomes": side.outcomes(uid, mid), "receipt": receipt(mid),
             "discard_logged": bool(DISCARD.search(side.log_text()[log_from:])),
             "edit_newer_than_replacement": (created_ms(mid) or 0) > RESTORED_MS}
    print(f"  {side.kind} {label}: {json.dumps(facts)}", flush=True)
    return facts


def classify(results):
    """The exit status, under the rule in the module docstring; prints each client's result."""
    for kind in ("libzb", "ts"):
        if kind not in results: print(f"  {kind}: did not run"); return 2
        for label, f in results[kind].items():
            if "setup" in f: print(f"  {kind} {label}: setup incomplete — {f['setup']}"); return 2
        rec = results[kind]["case 1"]
        if not rec["edit_newer_than_replacement"]:
            print(f"  {kind} case 1: setup incomplete — the replacement is not older than the edit"); return 2

    def control(f):
        return (f["after"][0] == f["before"][0] and f["after"][1] > f["before"][1] and f["pg"] == f["edit"]
                and f["left_outbox"] and f["converged"])

    def fixed(f):
        return (f["after"][0] == f["before"][0] + 1 and f["receipt"] == ("rejected", "TableIncarnationChanged")
                and f["outcomes"] == [("rejected", "TableIncarnationChanged")] and f["pg"] == REPLACEMENT
                and f["drained"] and f["converged"])

    def ts_defect(f):
        return f["after"][0] == f["before"][0] + 1 and ("applied", "") in f["outcomes"] and f["pg"] == f["edit"]

    def libzb_signature(f):
        """Which known libzb defect this run shows, or None."""
        if f["after"][0] != f["before"][0] + 1: return None
        if f["receipt"] == ("accepted", "") and ("applied", "") in f["outcomes"] and f["pg"] == f["edit"]:
            return "applied"
        if (f["discard_logged"] and f["left_outbox"] and f["receipt"] is None and f["outcomes"] == []
                and f["pg"] == REPLACEMENT):
            return "discarded"
        if (f["receipt"] == ("accepted", "") and f["pg"] == f["edit"] and f["discard_logged"] and f["left_outbox"]
                and f["drained"] and f["outcomes"] == []):
            return "applied to replacement, no app outcome after DROP discard"
        return None

    controls = {k: control(results[k]["case 4"]) for k in ("libzb", "ts")}
    lib_sig = libzb_signature(results["libzb"]["case 1"])
    state = {"libzb": "fixed" if fixed(results["libzb"]["case 1"]) else f"defect ({lib_sig})" if lib_sig else "unmatched",
             "ts": "fixed" if fixed(results["ts"]["case 1"]) else "defect (applied)" if ts_defect(results["ts"]["case 1"]) else "unmatched"}
    for k in ("libzb", "ts"):
        print(f"  {k}: reseed control {'passes' if controls[k] else 'FAILS'}; recreate case: {state[k]}", flush=True)
    if not all(controls.values()): return 3
    if all(s == "fixed" for s in state.values()): return 0
    if all(s.startswith("defect") for s in state.values()):
        print(f"known defects reproduced: zb-client-ts applied the old edit to the replacement row; "
              f"libzb signature: {lib_sig}")
        return 1
    return 3


def main():
    def recreate(uid, tenant):
        zb.psql(f"DROP TABLE public.{T}")
        create()
        # Older than the offline edit, as rows restored from a backup are: a version
        # comparison alone then lets the old edit win.
        zb.psql(f"INSERT INTO public.{T} (uid, title, tenant_id, updated_at) VALUES ('{uid}', '{REPLACEMENT}', '{tenant}', '{RESTORED_AT}')")

    def reseed(uid, tenant):
        zb.psql(f"SELECT zebridge_reseed('{T}')")

    teardown()
    relay = None
    results = {}
    try:
        create()
        # The contract holds where the bridge asks for stamps: the switch on for this run.
        zb.psql("UPDATE zebridge_private.write_policy SET incarnation_stamps = true WHERE singleton")
        relay = Relay()
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                print("bridge never started its producer"); print(bridge.text()[-1500:]); return 2
            for kind in ("libzb", "ts"):
                side = Side(kind, relay)
                try:
                    if not until(side.seeded, 120, 0.3):
                        results[kind] = {"case 4": {"setup": f"{T} never seeded"}}; continue
                    results[kind] = {"case 4": run_case(side, reseed, "case 4 (reseed control)"),
                                     "case 1": run_case(side, recreate, "case 1 (recreate)")}
                finally:
                    side.close()
    finally:
        if relay: relay.close()
        zb.psql("UPDATE zebridge_private.write_policy SET incarnation_stamps = false WHERE singleton", quiet=True)
        teardown()
    return classify(results)


if __name__ == "__main__":
    # An exception is not a verdict: Python's own exit 1 would read as "known defect
    # reproduced" in known_red. It is an unexpected result (3), with its traceback.
    try:
        sys.exit(main())
    except Exception:
        import traceback; traceback.print_exc(); sys.exit(3)
