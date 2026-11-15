#!/usr/bin/env python3
"""Pending offline edits survive a fresh snapshot, and an edit of a row the snapshot no
longer has ends as `row_deleted`, never as a resurrected row.

A phone edits rows offline; while it is away, one of them is deleted upstream and its
position falls out of the stream's retention. On reconnect it takes the gap → re-seed
path: a base replaces the table's rows, but the outbox is a separate table, so the
queued edits must outlive the replacement and be laid again over the seeded rows.

Per client (libzb through a relay that cuts its NATS link, zb-client-ts hung up):

  1. rows X and Y are seeded on the replica
  2. offline: the client queues an UPDATE of X and an UPDATE of Y; a stray row is written
     into the replica file, which only a base (wipe and reload) removes — a delta does not
  3. upstream: X is deleted (tombstone), Z is inserted, `zebridge_reseed` asks for a base
     (a client gets one when no delta covers its absence, which a test cannot wait for),
     and the tenant stream is purged past the client's position (retention, made instant)
  4. the bridge is STOPPED, then the client comes back: it re-seeds while no verdict can
     arrive, so the base is applied with both edits still pending, whichever order the
     client flushes and catches up in
       - Z present and the stray row gone (a base replaced the rows), both edits still in
         the outbox
       - Y shows the offline edit (re-applied over the seed)
       - X is absent: an UPDATE laid over a row the snapshot lacks invents nothing
  5. the bridge restarts:
       - Y's edit → `applied`; X's → `deleted` (row_deleted: the UPDATE matched no live row)
       - PostgreSQL: X still tombstoned with its original title, Y edited
       - outbox and undo queue empty, the replica equals PostgreSQL

    scripts/scenarios/run.py owns -k offline_reseed_delete

Exit codes: 0 pass, 1 a check failed, 2 the setup could not be established.
"""
import json, os, sqlite3, sys, time, uuid
import zb
from clients import Lib, Node, fresh_sqlite
from recreate_incarnation import Relay, until

LOG = "/tmp/zb_offline_reseed_delete_bridge.log"
NODE_LOG = "/tmp/zb_offline_reseed_delete_node.log"
T = "ord_rows"
PUB = zb.publication()
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"generations => true, publication => '{PUB}', dry_run => false")
BRIDGE_ENV = dict(GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5")


class Setup(Exception):
    """A precondition could not be established: exit 2, not a failed check."""


def teardown():
    zb.forget_table(T)
    for sql in (f"DROP TABLE IF EXISTS public.{T}", f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'",
                f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'"):
        zb.psql(sql, quiet=True)


class Side:
    """One client: how it goes offline, comes back, and what it reports."""
    def __init__(self, kind, relay):
        self.kind, self.relay = kind, relay
        self.db = f"/tmp/zb-ord-{kind}.sqlite3"; fresh_sqlite(self.db)
        if kind == "libzb":
            self.c = Lib(self.db, [T], "py-offline-reseed-delete", nats_url=relay.url)
            self.log = None
        else:
            # T only, as libzb: the purge leaves the tenant's other chains behind the stream,
            # and with the producer stopped their wait would hold connect().
            os.environ["ZB_TABLES"] = T
            self.c = Node(self.db, NODE_LOG)
        self.tenant = self.c.tenant
        self.undo = "_zbz_undo" if kind == "libzb" else "_zebridge_undo"
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
    def title(self, uid):
        try: rows = self.c.q(f"SELECT title FROM {T} WHERE uid = ?", (uid,))
        except RuntimeError: return None
        return rows[0][0] if rows else None
    def queued(self, *mids):
        return sum(self.c.q("SELECT count(*) FROM _zebridge_outbox WHERE msg_id = ?", (m,))[0][0] for m in mids)
    def drained(self):
        return self.c.q("SELECT count(*) FROM _zebridge_outbox")[0][0] == 0 and \
               self.c.q(f"SELECT count(*) FROM {self.undo} WHERE mode <> 'accepted'")[0][0] == 0
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
    def outcome(self, uid, mid):
        """The terminal outcome the application saw for this write, or None."""
        if self.kind == "libzb":
            o = self.c.outcome(mid)
            return o.get("outcome") if o else None
        for line in open(NODE_LOG, errors="replace"):
            if "[VERDICT-API]" not in line: continue
            try: v = json.loads(line.split("[VERDICT-API]", 1)[1])
            except ValueError: continue
            if v.get("table") == T and (v.get("key") or {}).get("uid") == uid:
                return v.get("outcome")
        return None
    def close(self):
        try: self.c.close()
        except Exception: pass


def manifest(tenant):
    m = zb.kv_get("generations", f"{tenant}.{T}")
    return json.loads(m) if m else {}


def base_covers(tenant, epoch, seq, budget):
    """The newest generation is a base of a later seed epoch, cut at or past `seq`."""
    def ready():
        m = manifest(tenant)
        return (int(m.get("seed_epoch") or 0) > epoch and (m.get("full") or {}).get("gen") == m.get("gen")
                and int(m.get("cutoff_seq") or 0) >= seq)
    return until(ready, budget, 1)


def stream_state(stream):
    return json.loads(zb.nats_cli("stream", "info", stream, "-j").stdout)["state"]


def run(kind, relay, check):
    """Steps 1–5 for one client. Bridges are started and stopped here."""
    side = None
    try:
        with zb.Bridge(LOG + f".{kind}.1", **BRIDGE_ENV) as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                raise Setup(f"bridge never started its producer: {bridge.text()[-500:]}")
            side = Side(kind, relay)
            if not until(side.seeded, 120, 0.3): raise Setup(f"{T} never seeded on {kind}")
            stream = zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + side.tenant
            x, y, z, stray = (str(uuid.uuid4()) for _ in range(4))
            zb.psql(f"INSERT INTO public.{T} (uid, title, tenant_id) VALUES ('{x}', 'x original', '{side.tenant}'), "
                    f"('{y}', 'y original', '{side.tenant}')")
            if not until(lambda: (side.poll(), side.title(x) == "x original" and side.title(y) == "y original")[1], 60, 0.3):
                raise Setup("rows X and Y never reached the replica")

            # 2. offline edits
            side.offline()
            mx = side.update(x, f"x edited offline by {kind}")
            my = side.update(y, f"y edited offline by {kind}")
            if not (mx and my and side.queued(mx, my) == 2): raise Setup(f"the edits were not queued offline ({mx!r}, {my!r})")

            # A row only a base removes: a delta leaves an unknown key alone, a base wipes and reloads.
            # Written in the file (the client's query is read-only), while the client is idle.
            with sqlite3.connect(side.db, timeout=10) as db:
                db.execute(f"INSERT INTO {T} (uid, title, tenant_id, updated_at) VALUES (?, 'stray', ?, '2026-01-01 00:00:00+00')",
                           (stray, side.tenant))
            if side.title(stray) != "stray": raise Setup("the stray local row was not written")

            # 3. upstream: delete X, insert Z, cut a base, purge past the client. A base comes
            # when no delta covers the absence; a test cannot age the deltas out, so
            # zebridge_reseed asks for it.
            epoch = int(manifest(side.tenant).get("seed_epoch") or 0)
            seq0 = stream_state(stream)["last_seq"]
            zb.psql(f"UPDATE public.{T} SET deleted_at = now(), updated_at = now() WHERE uid = '{x}'")
            zb.psql(f"INSERT INTO public.{T} (uid, title, tenant_id) VALUES ('{z}', 'z inserted while away', '{side.tenant}')")
            zb.psql(f"SELECT zebridge_reseed('{T}')")
            if not until(lambda: stream_state(stream)["last_seq"] >= seq0 + 2, 30): raise Setup(f"{stream} never received the changes")
            last = stream_state(stream)["last_seq"]
            if not base_covers(side.tenant, epoch, last, 90): raise Setup(f"no base past epoch {epoch} cut at seq {last}")
            r = zb.nats_cli("stream", "purge", stream, "--seq", str(last), "-f")
            if r.returncode != 0: raise Setup(f"purge refused (run as the bridge): {r.stderr.strip()[:120]}")
            first = stream_state(stream)["first_seq"]
            print(f"  {kind}: X deleted, Z inserted, chain cut ≥ {last}, {stream} purged to first_seq {first}", flush=True)

        # 4. back online with no bridge: the re-seed lands while both edits wait for a verdict
        side.online()
        if not until(lambda: (side.poll(), side.title(z) is not None)[1], 90, 0.3):
            raise Setup(f"{kind} never re-seeded after the gap (Z absent)")
        check(f"§4 {kind}: a base replaced the rows (Z present, the stray local row gone: {side.title(stray)!r})",
              side.title(stray) is None)
        check(f"§4 {kind}: both edits still in the outbox ({side.queued(mx, my)}/2)", side.queued(mx, my) == 2)
        check(f"§4 {kind}: Y shows the offline edit, re-applied over the seed: {side.title(y)!r}",
              side.title(y) == f"y edited offline by {kind}")
        check(f"§4 {kind}: X absent from the replica, not resurrected by its queued UPDATE: {side.title(x)!r}",
              side.title(x) is None)

        # 5. the bridge returns: verdicts
        with zb.Bridge(LOG + f".{kind}.2", **BRIDGE_ENV) as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                raise Setup(f"bridge never restarted: {bridge.text()[-500:]}")
            got = {}
            until(lambda: (side.poll(), got.update(x=side.outcome(x, mx), y=side.outcome(y, my)), all(got.values()))[2], 60, 0.3)
            check(f"§5 {kind}: Y's edit → {got.get('y')}, X's edit → {got.get('x')}",
                  got.get("y") == "applied" and got.get("x") == "deleted")
            pg_x = zb.psql(f"SELECT title || '|' || (deleted_at IS NOT NULL) FROM public.{T} WHERE uid = '{x}'")
            pg_y = zb.psql(f"SELECT title FROM public.{T} WHERE uid = '{y}'")
            check(f"§5 {kind}: PostgreSQL X = {pg_x!r} (tombstoned, original title), Y = {pg_y!r}",
                  pg_x == "x original|true" and pg_y == f"y edited offline by {kind}")
            drained = until(lambda: (side.poll(), side.drained())[1], 30, 0.3)
            check(f"§5 {kind}: outbox and undo queue empty", drained)
            check(f"§5 {kind}: replica equals PostgreSQL", until(side.matches_pg, 60, 0.3))
    finally:
        if side: side.close()


def main():
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario stops and starts its own")
    failed = 0
    def check(label, cond):
        nonlocal failed
        (zb.ok if cond else zb.bad)(time.strftime("%H:%M:%S ") + label)
        failed += not cond
        sys.stdout.flush()

    teardown()
    relay = None
    try:
        zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY, title text, tenant_id varchar(255) NOT NULL, "
                "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)")
        out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE})")
        if "error" in out.lower(): print(f"zebridge_enable: {out}"); return 2
        relay = Relay()
        for kind in ("libzb", "ts"):
            run(kind, relay, check)
    except Setup as e:
        zb.bad(f"setup incomplete: {e}"); return 2
    finally:
        if relay: relay.close()
        teardown()
    print("PASS" if not failed else f"FAIL ({failed})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
