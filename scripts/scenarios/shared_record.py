"""Example 15, played mechanically (examples/15-shared-record/README.md).

Three editors of one tenant — alice, bob, carol, each a zb-client-ts client with its own
replica and its own offline switch — and omar, in another tenant. The README's timeline
runs twice: on row A through the five plain columns (the library's rebase), on row B
through the five registers of `doc` (PostgreSQL's register merge, `register_cols`, and
the page's re-merge after a stale write — the same logic as web/src/main.ts, built on
the survival kit: mutate, onVerdict, pending, stamp, mergeRegisters).

The timeline, in order (no clock: each step waits until everyone online agrees):
   1. alice: access          2. bob goes offline       3. alice: notes
   4. carol goes offline; carol: rating 2; alice: rating 4
   5. alice: contact         6. bob, still offline: hazard — stamped after alice's last write
   7. bob back online        8. alice: rating 5        9. carol back online
  10. carol: contact        11. omar writes the row from his tenant

What must hold, on both rows: PostgreSQL ends at access, notes and rating 5 by alice,
hazard by bob, contact by carol; carol's offline rating is reported LOST; bob's late write
lands without taking alice's fields (B: every register keeps its writer); every replica
equals PostgreSQL; every outbox is empty; omar's write changes nothing and his replica
never holds the rows.
"""
import json, os, sys, time, uuid
import zb
from clients import Node, fresh_sqlite

LOG = "/tmp/zb_shared_record_bridge.log"
T = "mig_survey"
PUB = zb.publication()
COLS = ["access", "hazard", "contact", "rating", "notes"]
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', register_cols => ARRAY['doc']::name[], generations => true, "
          f"publication => '{PUB}', dry_run => false")
TENANT = "kilo"                      # the dev stack's 'omar' credentials: the three editors' tenant
OUTSIDER = "alice"                   # credentials of another tenant (acme): omar, in the README


class Editor:
    """One editor, as the page does it: columns written as they are; a register merged
    into the doc this replica holds, kept in `mine` until its write settles, merged once
    more into the newer row when a doc write is refused."""

    def __init__(self, name, principal=None):
        self.name = name
        self.log = f"/tmp/zb_shared_record_{name}.log"
        self.db = f"/tmp/zb-shared-record-{name}.sqlite3"
        fresh_sqlite(self.db)
        self.node = Node(self.db, self.log, principal=principal or "omar")
        self.online = True
        self.mine = {}          # register -> {v, t, w}
        self.carried = {}       # version -> the registers that doc write carried
        self.seen = 0           # verdict lines already handled
        self.lost = []          # (row, what) the editor was told it lost
        self.outcomes = []      # (version, outcome, columns)

    def doc(self, uid):
        r = self.node.q(f"SELECT doc FROM {T} WHERE uid = ?", [uid])
        if not r: return None
        d = r[0][0]
        return json.loads(d) if isinstance(d, str) else d

    def row(self, uid):
        r = self.node.q(f"SELECT {', '.join(COLS)}, doc FROM {T} WHERE uid = ?", [uid])
        if not r: return None
        vals = dict(zip(COLS, r[0][:5]))
        d = r[0][5]
        vals["doc"] = json.loads(d) if isinstance(d, str) else d
        return vals

    def write_col(self, uid, col, value):
        return self.node.mutate(T, "UPDATE", {"uid": uid}, {col: value})["version"]

    def write_reg(self, uid, key, value):
        self.mine[key] = {"v": value, "t": self.node.stamp(), "w": self.name}
        return self._write_doc(uid)

    def _write_doc(self, uid):
        doc = self.node.merge(self.doc(uid) or {}, self.mine)
        v = self.node.mutate(T, "UPDATE", {"uid": uid}, {"doc": doc})["version"]
        self.carried[v] = {k: dict(r) for k, r in self.mine.items()}
        return v

    def offline(self):
        self.node.disconnect(); self.online = False

    def back(self):
        self.node.connect(); self.online = True

    def handle_verdicts(self, uid_b):
        """The page's onVerdict, for the verdicts that arrived since last time."""
        lines = [l for l in open(self.log) if l.startswith("[VERDICT-API] ")]
        for line in lines[self.seen:]:
            v = json.loads(line[len("[VERDICT-API] "):])
            self.outcomes.append((v["version"], v["outcome"], v["columns"]))
            regs = self.carried.pop(v["version"], None)
            if v["outcome"] in ("applied", "rebased") and "doc" in v["columns"] and regs:
                if v["outcome"] == "rebased" and v.get("rebasedAs"): self.carried[v["rebasedAs"]] = regs
                else:
                    for k, r in regs.items():
                        if self.mine.get(k, {}).get("t") == r["t"]: self.mine.pop(k)
            elif v["outcome"] == "lost" and "doc" in v["columns"] and self.mine:
                held = self.doc(uid_b) or {}
                for k, r in list(self.mine.items()):
                    h = held.get(k)
                    if h and (h.get("t", ""), h.get("w", "")) > (r.get("t", ""), r.get("w", "")):
                        self.lost.append(("B", k)); self.mine.pop(k)
                if self.mine: self._write_doc(uid_b)
            elif v["outcome"] == "lost":
                self.lost.append(("A", ",".join(v.get("lostColumns") or v["columns"])))
        self.seen = len(lines)

    def close(self):
        self.node.close()


def pg_row(uid):
    out = zb.psql(f"SELECT json_build_object({', '.join(f'{c!r}, {c}' for c in COLS)}, 'doc', doc)::text FROM {T} WHERE uid = '{uid}'").strip()
    return json.loads(out) if out else None


def main():
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")
    failed = 0

    def check(label, ok):
        nonlocal failed
        (zb.ok if ok else zb.bad)(label)
        if not ok: failed += 1

    def teardown():
        zb.forget_table(T)
        for sql in (f"DROP TABLE IF EXISTS public.{T}", f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'",
                    f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'"):
            zb.psql(sql, quiet=True)

    teardown()
    zb.psql(f"""CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        access text, hazard text, contact text, rating integer, notes text,
        doc jsonb NOT NULL DEFAULT '{{}}'::jsonb,
        tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)""")
    eds = {}
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE})")
            check(f"zebridge_enable({T}, register_cols => doc): registers done", "registers:done" in out and "error" not in out.lower())
            row_a, row_b = str(uuid.uuid4()), str(uuid.uuid4())
            zb.psql(f"INSERT INTO {T} (uid, tenant_id) VALUES ('{row_a}', '{TENANT}'), ('{row_b}', '{TENANT}')")
            # Only this table: the default ('*') seeds the whole tenant per client, and four
            # of them exhaust the account's consumer limit.
            os.environ["ZB_TABLES"] = T
            for name in ("alice", "bob", "carol"): eds[name] = Editor(name)
            eds["omar"] = Editor("omar", principal=OUTSIDER)
            alice, bob, carol, omar = eds["alice"], eds["bob"], eds["carol"], eds["omar"]

            def settle(budget=60):
                """Until every online editor has an empty outbox, every verdict handled,
                and a replica equal to PostgreSQL on both rows."""
                t0 = time.monotonic()
                while time.monotonic() - t0 < budget:
                    for e in (alice, bob, carol): e.handle_verdicts(row_b)
                    on = [e for e in (alice, bob, carol) if e.online]
                    want = {u: pg_row(u) for u in (row_a, row_b)}
                    if all(e.node.pending() == 0 and not e.carried for e in on) and \
                       all(e.row(u) == want[u] for e in on for u in (row_a, row_b)):
                        return round(time.monotonic() - t0, 2)
                    time.sleep(0.2)
                return None

            dt = settle(120)
            check(f"step 0: the three editors hold both rows ({dt} s)", dt is not None)

            def step(label, fn):
                fn()
                dt = settle()
                check(f"{label} ({dt} s)", dt is not None)

            both = lambda f: (lambda: (f(row_a, "A"), f(row_b, "B")))
            def edit(e, key, value):
                return both(lambda uid, part: e.write_col(uid, key, value) if part == "A" else e.write_reg(uid, key, value))

            step("step 1: alice writes access", edit(alice, "access", "gate code 4421"))
            step("step 2: bob goes offline", bob.offline)
            step("step 3: alice writes notes", edit(alice, "notes", "the new warehouse"))
            def s4():
                carol.offline()
                edit(carol, "rating", 2)()
                edit(alice, "rating", 4)()
            step("step 4: carol goes offline and writes rating 2; alice writes rating 4", s4)
            step("step 5: alice writes contact", edit(alice, "contact", "M. Dupont"))
            step("step 6: bob, still offline, writes hazard — after alice's last write", edit(bob, "hazard", "asbestos roof"))
            check(f"step 6: bob's two writes wait in his outbox: pending() = {bob.node.pending()}", bob.node.pending() == 2)
            step("step 7: bob comes back", bob.back)
            step("step 8: alice writes rating 5", edit(alice, "rating", 5))
            step("step 9: carol comes back", carol.back)
            step("step 10: carol writes contact", edit(carol, "contact", "M. Safety"))

            # ── the outcome ──
            want = {"access": "gate code 4421", "hazard": "asbestos roof", "contact": "M. Safety", "rating": 5, "notes": "the new warehouse"}
            a, b = pg_row(row_a), pg_row(row_b)
            check(f"A: PostgreSQL holds {{{', '.join(f'{c}={a[c]!r}' for c in COLS)}}}", {c: a[c] for c in COLS} == want)
            check(f"B: PostgreSQL's doc holds {{{', '.join(f'{c}={b['doc'].get(c, {}).get('v')!r}' for c in COLS)}}}",
                  {c: b["doc"].get(c, {}).get("v") for c in COLS} == want)
            writers = {c: b["doc"].get(c, {}).get("w") for c in COLS}
            check(f"B: each register keeps its writer: {writers}",
                  writers == {"access": "alice", "hazard": "bob", "contact": "carol", "rating": "alice", "notes": "alice"})
            check(f"carol was told she lost rating, on both rows: {carol.lost}", ("A", "rating") in carol.lost and ("B", "rating") in carol.lost)
            check(f"nobody else lost anything: alice {alice.lost}, bob {bob.lost}", not alice.lost and not bob.lost)

            # ── omar, from another tenant ──
            before = pg_row(row_b)
            omar.node.mutate(T, "UPDATE", {"uid": row_b}, {"rating": 1})
            time.sleep(3)
            check(f"omar's write changed nothing: rating still {pg_row(row_b)['rating']}", pg_row(row_b) == before)
            check(f"omar's replica never held the rows: {omar.node.q(f'SELECT count(*) FROM {T}')[0][0]} row(s)",
                  omar.node.q(f"SELECT count(*) FROM {T}")[0][0] == 0)
            omar.handle_verdicts(row_b)
            zb.ok(f"omar's verdict: {[o for _, o, _ in omar.outcomes]}")
            for e in eds.values(): e.close()
            eds = {}
            teardown()
    finally:
        for e in eds.values():
            try: e.close()
            except Exception: pass
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
