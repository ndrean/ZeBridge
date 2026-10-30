#!/usr/bin/env python3
"""A table dropped and re-created under its name: the field shape of 2026-10-24 (compose
run A: 46 of 100 replicas not exact, 9,374 D2 refusals), as three independent checks.
Each runs on its own private stack: a disposable database (with its own slot) on the
shared cluster, its own nats-server, its own bridge. Nothing else writes there.

Notation: p a client's stored position, f the stream's first retained sequence, c the
newest manifest's cut. A retention gap is f > p + 1; a chain splices when c >= f - 1,
so then c > p and D2 (`fullPredatesReplica`: c < p) cannot refuse it.

  --check 1  scheduling and epochs. Three tenant pairs, generous retention. Bridge #1
             builds the first chains and stops; bridge #2 starts with no producer history
             (the field: the bridge had restarted 30 s before the reset) and runs all
             that is measured. The reset drops and re-creates the table, the load starts.
             Asserts ONE seed-epoch increment, and per pair a chain matching the current
             epoch within --first-chain-s. A client that followed through and a client
             that joins after must be exact AND anchored under the current epoch.
  --check 2  a recognised retention gap. Small CDC caps, one bridge, no recreate. A
             client closes, the stream prunes past it (f > p + 1), it returns: exact and
             anchored. Reports whether the chain spliced at once (c >= f - 1) or waited.
  --check 3  an incomplete replica with an advanced cursor — the suspected D2 stalemate.
             Small caps; bridge #1/#2 as in check 1; reset the way only the producer's
             backstop sees (event triggers disabled), since the DDL path now cuts a chain
             at once; the load prunes the new
             incarnation's first messages (its base rows) while no chain exists. A cold
             client joins then. Establishes whether it advances its cursor past the
             missing prefix, then whether the chain that follows repairs it or D2
             refuses it for good. No fault is injected.

  --check 4  the fence's boundaries, the load running, a seed-epoch move behind a live
             follower each time (test hooks in libzb): a crash after a committed chunk and
             a crash before the anchor's commit leave the replica unseeded with its cursor
             at the cut, and reopened it recovers; retention overtaking the cut while the
             fence stages refuses the commit, and a newer chain recovers it; an interrupted
             fence is still owed on reopening (its cursor now AT the cut), so its retry is
             verified too: pruned during the retry, it stays incomplete; an unreadable
             shared route refuses the fence instead of leaving the route out. The hooks
             exist only in a fault build (libzb: -Dtest-faults=true -p zig-out-faults);
             the check refuses to run without one, and fails if the ordinary library
             carries any hook.

  --check 12 check 4 with a zb-client-ts client (scripts/scenarios/fence_client.mts): the same
             cases, the same assertions, on the TypeScript replica's tables. The fault points
             are the harness's (the library's `faultHook` option); the library reads no
             environment for them, and the check fails if it does. No PostgreSQL-storage cases:
             that is libzb's `dbUrl`.

  --check 13 the applied frontier: two tables ride one tenant route, a seed-epoch move on ONE of
             them, the load running on both. The fence must be taken (the old refusal, D2,
             left the table unseeded while writes went on) and the other table, which already
             holds the events the rewind replays, must stay exact: an insert and its later
             tombstone replayed over a row the table has already deleted would bring it back
             (no surviving version rejects an insert the tombstone followed). Live: the fence
             is taken, both tables exact, events skipped through the frontier. Crash after a
             committed chunk: the frontier is durable (position at the cut, frontier above it),
             and reopened the replay skips what the table holds. Fault build, as check 4.

  --check 14 check 13 with a zb-client-ts client (fence_client.mts), two tables: the same cases, the
             same assertions. The hold that lets the position run ahead of the new chain's cut is
             the harness's (`faultHook` at `before_decision`, while a file exists).

  --check 15 a destructive replacement that is not a fence. A replica is closed; the stream moves on and
             a seed-epoch move cuts a chain AHEAD of its position (a plain seed: no cursor moves). It
             reopens with a crash after the first chunk of the full — the baseline is already deleted, the
             rest of the rows are not in. The replica must be durably incomplete: the owed record written
             and the anchor cleared with that first chunk, the position where it was. Reopened again, it
             recovers (anchored under the current epoch, exact). Before, such a failure left the old
             anchor over half-replaced rows and nothing owed; the CDC suffix cannot restore the rows the
             first chunk deleted. Fault build, as check 4.
  --check 16 check 15 with a zb-client-ts client.

  --check 5  check 1 with the bridge down during the recreate: PostgreSQL observes it (the
             DDL triggers), the restarted bridge publishes it — one transition, one epoch
             move, every pair's chain within --first-chain-s of the restart.
  --check 6  check 1 with the event triggers disabled for the recreate (a restore): the
             producer's OID check is the only observer — one transition, one epoch move,
             every pair's chain within a cadence (its only signal) plus 30 s.

  --check 7  a stale observation (fault-hook bridge): in SQL, two recreates then the two
             earlier OIDs submitted change nothing; through the producer, an observation
             held across another recreate is rejected — one transition, one epoch move,
             the recorded OID the live table's; nothing is built from the stale OID, the
             bookkeeping records the live OID only, and the next tick keeps the chain.
  --check 8  the DROP cleanup barrier (fault-hook bridge): the cleanup reads the old
             manifest and holds, the table is re-created and its chains published, the
             cleanup resumes — every pair keeps the new manifest and its objects.
  --check 9  the boot cleanup barrier (fault-hook bridge): a bridge boots with the table
             gone, reads its absence and holds BEFORE reading the manifests; the table is
             re-created and the running producer publishes its chain; the cleanup resumes
             — the new chain stays (its bound is the incarnation read with the absence).
  --check 10 a replacement behind a catalogue removal: a replica seeds and closes; the
             catalogue entry is deleted, the table dropped and re-created EMPTY, then
             enabled again. The seed epoch must still move above the replica's (the
             replacement emits no CDC, so no stream gap can tell it): the returning
             replica empties and anchors under the new epoch.
  --check 11 observation SQL errors: bounded transient retries and a permanent failure
             preserve the existing chain (manifest, bookkeeping, objects, epoch); restoring
             the function recovers the replacement, one epoch move.

Regressions enforced (failing on main as of 2026-10-25, by design): check 1 — V2 one epoch
increment per recreate, V3 a usable chain within --first-chain-s, V4 the live follower
re-anchored; check 3 — V8 no certified position past a missing prefix, and recovery
while writes continue. Check 2 lacks two variants: a gap over events the client had
applied (p > 0 before the pause), and a newest cut below f - 1 that forces a wait.
(V-numbers: docs/plans/recovery-model.md, local.)

Logs: /tmp/zb_rr<check>_* (bridges, nats, one per client); a failed replica keeps its
missing/extra/differing ids in /tmp/zb_rr<check>_<client>.diff.json.
"""
import argparse, asyncio, ctypes, hashlib, json, os, pathlib, re, signal, sqlite3, subprocess, sys, tempfile, threading, time, uuid
from contextlib import closing
import nats, nkeys
import zb
from hub_fallback_stage2 import BRIDGE, LIBRARY, port

# Check 4's library: the fence's fault hooks compiled in (an ordinary build has none).
FAULT_LIBRARY = pathlib.Path(os.environ.get("ZB_FAULT_LIBZB", zb.ROOT / "libzb/zig-out-faults/lib" / LIBRARY.name))
FAULT_BUILD = "cd libzb && zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults"
# Checks 7 and 8: the bridge's own fault hooks (ZB_TEST_OBSERVE_SLEEP_MS, ZB_TEST_PRUNE_SLEEP_MS).
FAULT_BRIDGE = pathlib.Path(os.environ.get("ZB_FAULT_BRIDGE_BIN", zb.ROOT / "zig-out-faults/bin/bridge"))
FAULT_BRIDGE_BUILD = "zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults"
from rotate_client_key import client_creds

T = "rr_items"
failed = 0


def check(label, cond):
    global failed
    (zb.ok if cond else zb.bad)(f"{time.strftime('%H:%M:%S')} {label}")
    if not cond: failed += 1
    sys.stdout.flush()
    return cond


def note(text):
    print(f"  · {text}", flush=True)


def frame(key, val):
    return f"{key}:N".encode() if val is None else f"{key}:S{len(val.encode())}:".encode() + val.encode()


def digest(rows):
    h = hashlib.sha256()
    for k in sorted(rows): h.update(frame(k, rows[k]))
    return h.hexdigest()


# ── the private stack ────────────────────────────────────────────────────────

class Stack:
    def __init__(self, tmp, tag, cadence, cdc_env):
        self.tmp, self.tag, self.cadence, self.cdc_env = tmp, tag, cadence, cdc_env
        self.name = "zb_rr_" + uuid.uuid4().hex[:8]
        self.psql = [zb._psql_binary(), "-X", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-Atq"]
        self.procs, self.bridges = {}, 0
        self.bridge_bin = BRIDGE
        self.owned = []   # loads and clients: stop() ends them, whatever interrupted the check

    def sql(self, statement, db=None):
        r = subprocess.run([*self.psql, "-d", db or self.name], input=statement, capture_output=True, text=True)
        if r.returncode: raise RuntimeError(r.stderr[-1500:])
        return r.stdout.strip()

    def log(self, label):
        return f"/tmp/zb_rr{self.tag}_{label}.log"

    def spawn(self, label, args, env):
        self.procs[label] = subprocess.Popen(args, env=env, stdout=open(self.log(label), "w"), stderr=subprocess.STDOUT)

    def wait_log(self, label, text, timeout=90):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if text in open(self.log(label), errors="replace").read(): return
            if self.procs[label].poll() is not None: break
            time.sleep(0.2)
        raise RuntimeError(f"{label}: no '{text}'\n" + open(self.log(label), errors="replace").read()[-1500:])

    def start(self, tenants):
        self.sql(f'CREATE DATABASE "{self.name}"', "postgres")
        r, w = self.name + "_r", self.name + "_w"
        self.env = dict(os.environ, DATABASE_READER_URL=f"postgres://{r}:x@127.0.0.1:5432/{self.name}",
                        DATABASE_WRITER_URL=f"postgres://{w}:x@127.0.0.1:5432/{self.name}", TARGET_DB=self.name,
                        BRIDGE_CDC_PUBLICATION="rr_pub")
        self.sql(subprocess.run([str(BRIDGE), "--init-sql"], env=self.env, capture_output=True, text=True, check=True).stdout)
        self.sql("".join(f"INSERT INTO zebridge_user_tenants VALUES ('{p}', '{t}');" for p, t in tenants.items()))
        ident = self.tmp / f"{self.name}-id"
        subprocess.run([str(BRIDGE), "--init-nats", "operator", "--dir", str(ident), "--js-domain", "hub"], capture_output=True, check=True)
        keys = dict(l.split("=", 1) for l in (ident / "operator.store").read_text().splitlines() if "=" in l and not l.startswith("#"))
        account = nkeys.from_seed(keys["ACCOUNT_SEED"].encode()).public_key.decode()
        self.creds = {}
        for p, t in tenants.items():
            path = self.tmp / f"{self.name}-{p}.creds"
            path.write_text(client_creds(keys["SK_CLIENT_SEED"], account, p, t))
            self.creds[p] = str(path)
        conf = (ident / "nats-server.conf").read_text()
        tcp = port()
        (self.tmp / f"{self.name}.conf").write_text(conf[:conf.index("\nport:")] + f'\nhost: "127.0.0.1"\nport: {tcp}\njetstream {{ domain: hub, store_dir: "{self.tmp}/{self.name}-js" }}\n')
        self.spawn("nats", ["nats-server", "-c", str(self.tmp / f"{self.name}.conf")], dict(os.environ))
        self.wait_log("nats", "Server is ready")
        self.url, self.bridge_creds = f"nats://127.0.0.1:{tcp}", str(ident / "creds/bridge.creds")
        self.env = dict(self.env, NATS_URL=self.url, NATS_CREDS=self.bridge_creds, NATS_JS_DOMAIN="hub", NATS_TLS_CA="",
                        MUTATION_ADMISSION="0", GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS=str(self.cadence),
                        BASE_BUF="12", LOG_LEVEL="info", **self.cdc_env)

    def bridge_up(self, ready="Replication started successfully"):
        """A new bridge process: a new producer with no history. The first one starts the
        feed on the private slot; later ones resume it. `ready`: the log line awaited."""
        self.bridges += 1
        env = dict(self.env, **({"ZB_FEED_RESTART": "1"} if self.bridges == 1 else {}))
        label = f"bridge{self.bridges}"
        self.spawn(label, [str(self.bridge_bin), "--pub", "rr_pub", "--slot", self.name + "_slot", "--port", str(port())], env)
        self.wait_log(label, ready)
        return label

    def bridge_down(self, label):
        p = self.procs.pop(label)
        p.terminate()
        try: p.wait(timeout=20)
        except subprocess.TimeoutExpired: p.kill()

    def replica_url(self, label):
        """A PostgreSQL database for one libzb replica, dropped with the stack."""
        db = f"{self.name}_{label}"
        self.sql(f'CREATE DATABASE "{db}"', "postgres")
        self.replicas = getattr(self, "replicas", []) + [db]
        return f"postgres://postgres@127.0.0.1:5432/{db}"

    def stop(self):
        if getattr(self, "stopped", False): return
        self.stopped = True
        # Every client asked at once, then waited on: one at a time, an interrupted check
        # spent up to 30 s per client before the stack itself went.
        for o in self.owned:
            if isinstance(o, Client) and o.p and o.p.poll() is None:
                pathlib.Path(o.db + ".stop").touch()
        for o in self.owned:
            try: o.end() if isinstance(o, Load) else o.stop()
            except Exception: pass
        for p in self.procs.values():
            if p.poll() is None: p.terminate()
        for p in self.procs.values():
            try: p.wait(timeout=15)
            except subprocess.TimeoutExpired: p.kill()
        subprocess.run([*self.psql, "-d", "postgres"], capture_output=True, text=True,
                       input=f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{self.name}_slot'; "
                             f'DROP DATABASE IF EXISTS "{self.name}" WITH (FORCE); ' + "".join(f'DROP DATABASE IF EXISTS "{r}" WITH (FORCE); ' for r in getattr(self, "replicas", [])) + f'DROP ROLE IF EXISTS "{self.name}_r"; DROP ROLE IF EXISTS "{self.name}_w";')

    # fixture
    def create_table(self, rows_by_tenant, prefix):
        self.sql(f"CREATE TABLE public.{T} (id text PRIMARY KEY, tenant_id text NOT NULL, note text, "
                 f"updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz);"
                 f"SELECT count(*) FROM zebridge_enable('public.{T}'::regclass, writable => true, tenant_col => 'tenant_id', "
                 f"version_col => 'updated_at', tombstone_col => 'deleted_at', publication => 'rr_pub', dry_run => false);")
        for t, n in rows_by_tenant.items():
            self.sql(f"INSERT INTO public.{T} (id, tenant_id, note) SELECT '{prefix}{t}-' || lpad(i::text, 6, '0'), '{t}', repeat(md5(i::text), 4) "
                     f"FROM generate_series(1, {n}) i;")

    def create_table_raw(self, rows_by_tenant, prefix):
        """The table re-created the way a restore does it: no zebridge_enable, only put back
        in the publication — nothing but the producer's OID check can observe it."""
        self.sql(f"CREATE TABLE public.{T} (id text PRIMARY KEY, tenant_id text NOT NULL, note text, "
                 f"updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz);"
                 # A restore keeps the table's row-level security AND its policies: RLS
                 # alone (no policy) hid every row from the producer's reader — an empty
                 # full, and the replicas emptied with it. The same helper zebridge_enable
                 # uses, which observes no incarnation.
                 f"SELECT public.zebridge_scope_reads_by_tenant('public.{T}'::regclass, 'tenant_id');"
                 f"ALTER PUBLICATION rr_pub ADD TABLE public.{T};")
        for t, n in rows_by_tenant.items():
            self.sql(f"INSERT INTO public.{T} (id, tenant_id, note) SELECT '{prefix}{t}-' || lpad(i::text, 6, '0'), '{t}', repeat(md5(i::text), 4) "
                     f"FROM generate_series(1, {n}) i;")

    def incarnation(self):
        return int(self.sql(f"SELECT incarnation FROM public.zebridge_incarnations WHERE tbl = '{T}'") or 0)

    def recorded(self):
        """(incarnation, seed_epoch, recorded OID, live OID) from the catalogue and pg_class."""
        r = self.sql(f"SELECT i.incarnation, c.seed_epoch, coalesce(i.incarnation_relid::text, ''), "
                     f"coalesce(to_regclass('public.{T}')::oid::text, '') FROM public.zebridge_catalogue c "
                     f"JOIN public.zebridge_incarnations i ON i.tbl = c.tbl WHERE c.tbl = '{T}'")
        inc, ep, rec, live = r.split("|")
        return int(inc), int(ep), rec, live

    def chain_state(self):
        """Per tenant: (newest live gen, the OIDs its live bookkeeping rows recorded)."""
        out = {}
        for line in self.sql(f"SELECT tenant, max(gen), string_agg(DISTINCT relid::text, ',') FROM public.zebridge_generations "
                             f"WHERE tbl = '{T}' AND retired_at IS NULL GROUP BY tenant").splitlines():
            t, g, oids = line.split("|")
            out[t] = (int(g), set(oids.split(",")) if oids else set())
        return out

    def live_oid(self):
        return self.sql(f"SELECT coalesce(to_regclass('public.{T}')::oid::text, '')")

    def drop_table(self):
        self.sql(f"DROP TABLE public.{T};")

    def truth(self, tenant):
        out = self.sql(f"SELECT id, coalesce(note, E'\\\\N') FROM public.{T} WHERE tenant_id = '{tenant}' AND deleted_at IS NULL")
        rows = {}
        for line in out.splitlines():
            k, v = line.split("|", 1)
            rows[k] = None if v == "\\N" else v
        return rows


class Load:
    """3 inserts, 1 update, 1 tombstone per tick in one tenant, each statement its own
    transaction (many small CDC messages)."""
    def __init__(self, stack, tenant, prefix, interval, table=None):
        self.table = table or T
        self.stop, self.ticks = threading.Event(), 0
        self.p = subprocess.Popen([*stack.psql, "-d", stack.name, "-v", "ON_ERROR_STOP=0"], stdin=subprocess.PIPE,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        self.tenant, self.prefix, self.interval = tenant, prefix, interval
        self.t = threading.Thread(target=self.run, daemon=True); self.t.start()
        stack.owned.append(self)

    def run(self):
        i, w = 0, self.p.stdin.write
        while not self.stop.is_set():
            t0 = time.monotonic()
            w(f"INSERT INTO public.{self.table} (id, tenant_id, note) VALUES " + ", ".join(f"('{self.prefix}-{i + k:06d}', '{self.tenant}', 'n{i}')" for k in range(3)) + ";\n")
            if i >= 3: w(f"UPDATE public.{self.table} SET note = 'u{i}', updated_at = now() WHERE id = '{self.prefix}-{i - 2:06d}';\n")
            if i >= 6: w(f"UPDATE public.{self.table} SET deleted_at = now(), updated_at = now() WHERE id = '{self.prefix}-{i - 6:06d}';\n")
            self.p.stdin.flush()
            i += 3; self.ticks += 1
            left = self.interval - (time.monotonic() - t0)
            if left > 0: time.sleep(left)

    def end(self):
        if self.stop.is_set(): return
        self.stop.set(); self.t.join(timeout=10)
        try:
            self.p.stdin.close(); self.p.wait(timeout=60)
        except Exception:
            self.p.kill()


class Js:
    """Read-only looks at the private NATS, with the bridge's creds."""
    async def open(self, stack):
        self.nc = await nats.connect(stack.url, user_credentials=stack.bridge_creds, connect_timeout=5)
        self.js = self.nc.jetstream(domain="hub")
        self.gen, self.schemas = zb.kv_bucket("generations"), zb.kv_bucket("schemas")
        return self

    async def kv(self, bucket, key):
        try:
            m = await self.js.get_msg(f"KV_{bucket}", subject=f"$KV.{bucket}.{key}", direct=True)
            if (m.headers or {}).get("KV-Operation") in ("DEL", "PURGE"): return None
            return json.loads(m.data)
        except Exception:
            return None

    async def tenants_ready(self, principals, timeout=60):
        """$KV.tenants.<principal> for each: a client that connects before its mapping is
        published skips every tenant-scoped table for good (measured: check 2, first run)."""
        bucket, t0 = zb.kv_bucket("tenants"), time.monotonic()
        missing = list(principals)
        while missing and time.monotonic() - t0 < timeout:
            missing = [p for p in missing if await self.kv(bucket, p) is None]
            if missing: await asyncio.sleep(0.5)
        return not missing

    async def epoch(self):
        d = await self.kv(self.schemas, T)
        return d.get("seed_epoch", 0) if isinstance(d, dict) else None

    async def manifest(self, tenant):
        return await self.kv(self.gen, f"{tenant}.{T}")

    async def stream(self, name):
        try:
            s = (await self.js.stream_info(name)).state
            return {"first": s.first_seq, "last": s.last_seq, "messages": s.messages}
        except Exception:
            return {}

    async def close(self):
        await self.nc.close()


# ── one libzb client per process ─────────────────────────────────────────────

class Client:
    STOP_WAIT = 30   # seconds a process gets to see its stop file before it is killed

    def __init__(self, stack, name, principal, tenant, chunk_rows=None, lib=None, pg_url=None):
        """`pg_url`: the replica on PostgreSQL storage (libzb's `dbUrl`) instead of SQLite;
        the db path then only names the process's stop file."""
        self.stack, self.name, self.principal, self.tenant, self.chunk_rows = stack, name, principal, tenant, chunk_rows
        self.lib, self.pg_url = str(lib or LIBRARY), pg_url
        self.tables = [T]   # the tables this client follows (the applied-frontier scenario follows two)
        self.db = f"/tmp/zb-rr{stack.tag}-{name}.sqlite3"
        self.log = stack.log(name)
        for s in ("", "-wal", "-shm", ".stop"): pathlib.Path(self.db + s).unlink(missing_ok=True)
        open(self.log, "w").close()
        self.p = None
        stack.owned.append(self)

    def start(self, env=None):
        """`env`: test hooks for this process only (ZB_TEST_FENCE_*). A previous process that did not exit (an abort
        that never came) is stopped first: two clients applying the same stream to one replica interleave and leave it
        inexact (NOTES "The joint fence": the third-table mismatch was that)."""
        self.stop()
        pathlib.Path(self.db + ".stop").unlink(missing_ok=True)
        opts = dict(natsUrl=self.stack.url, credsPath=self.stack.creds[self.principal], principal=self.principal, jsDomain="hub",
                    clientId=f"rr-{self.name}", dbPath=self.db, tables=self.tables, heartbeatMs=0,
                    **({"seedChunkRows": self.chunk_rows} if self.chunk_rows else {}),
                    **({"dbUrl": self.pg_url} if self.pg_url else {}))
        self.p = subprocess.Popen([sys.executable, __file__, "--worker", json.dumps(opts), "--lib", self.lib], stdout=subprocess.DEVNULL,
                                  stderr=open(self.log, "a"), env=dict(os.environ, **(env or {})))

    def exited(self):
        """The process ended on its own (a test abort): its return code, else None."""
        return self.p.poll() if self.p else None

    def text(self):
        return open(self.log, errors="replace").read()

    def stop(self):
        if not self.p: return
        if self.p.poll() is not None:
            self.p = None
            return
        pathlib.Path(self.db + ".stop").touch()
        try: self.p.wait(timeout=self.STOP_WAIT)
        except subprocess.TimeoutExpired:
            # A killed process still holds the replica until the kernel has reaped it: the replacement must not start before.
            self.p.kill()
            self.p.wait()
        self.p = None

    def _q(self, sql, args=()):
        if self.pg_url:
            return self._pg(sql, args)
        try:
            with closing(sqlite3.connect(f"file:{self.db}?mode=ro", uri=True, timeout=2)) as con:
                return con.execute(sql, args).fetchall()
        except sqlite3.Error:
            return None

    def _pg(self, sql, args):
        """The same reads on a PostgreSQL replica, through psql. The arguments are this
        scenario's own names (table, tenant, stream): quoted, never interpolated raw."""
        for v in args:
            sql = sql.replace("?", "'" + str(v).replace("'", "''") + "'", 1)
        r = subprocess.run([zb._psql_binary(), "-X", "-At", "-F", "\x1f", "-P", "null=\\N", "-d", self.pg_url, "-c", sql],
                           capture_output=True, text=True)
        if r.returncode:
            return None
        def cell(x):
            return None if x == "\\N" else int(x) if x.lstrip("-").isdigit() else x
        return [tuple(cell(x) for x in line.split("\x1f")) for line in r.stdout.splitlines()]

    def rows(self):
        r = self._q(f"SELECT id, note FROM {T} WHERE tenant_id = ?", (self.tenant,))
        return None if r is None else dict(r)

    def position(self, stream):
        r = self._q("SELECT last_seq FROM _zbz_stream_seq WHERE stream = ?", (stream,))
        return r[0][0] if r else 0

    def created(self, stream):
        r = self._q("SELECT created FROM _zbz_stream_seq WHERE stream = ?", (stream,))
        return r[0][0] if r else None

    def fence_pending(self):
        r = self._q("SELECT count(*) FROM _zbz_fence WHERE tbl = ?", (T,))
        return bool(r and r[0][0])

    def anchors(self):
        cols = ("tenant", "seed_epoch", "anchor_stream", "anchor_seq", "shared_seq", "watermark")
        r = self._q(f"SELECT {', '.join(cols)} FROM _zbz_generations WHERE tbl = ?", (T,))
        return [dict(zip(cols, row)) for row in r] if r else []

    def count(self, pattern):
        return len(re.findall(pattern, open(self.log, errors="replace").read()))


HARNESS = zb.ROOT / "scripts/scenarios/fence_client.mts"


class TsClient(Client):
    """The same client from the scenario's side, a zb-client-ts process: its replica tables are
    `_zebridge_*`, keyed by table alone (one tenant per table), and its answers are mapped onto
    the names the checks read."""
    def start(self, env=None):
        self.stop()   # never two processes on one replica (Client.start)
        pathlib.Path(self.db + ".stop").unlink(missing_ok=True)
        args = ["node", "--experimental-strip-types", "--no-warnings", str(HARNESS), self.stack.url, self.stack.creds[self.principal],
                self.db, self.principal, ",".join(self.tables), str(self.chunk_rows or 0)]
        self.p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=open(self.log, "a"), env=dict(os.environ, **(env or {})),
                                  cwd=zb.ROOT / "zb-client-ts")

    def position(self, stream):
        r = self._q("SELECT last_seq FROM _zebridge_stream_seq WHERE stream = ?", (stream,))
        return r[0][0] if r else 0

    def created(self, stream):
        r = self._q("SELECT created FROM _zebridge_stream_seq WHERE stream = ?", (stream,))
        return r[0][0] if r else None

    def fence_pending(self):
        r = self._q("SELECT count(*) FROM _zebridge_fence WHERE tbl = ?", (T,))
        return bool(r and r[0][0])

    def anchors(self):
        # A TypeScript replica is seeded when its generations row exists. A chain cut on an empty
        # stream ships no sequence (cut 0), and the replica then records neither a stream nor a
        # sequence where libzb records the stream and 0: read as that, the cut is 0 on the own route.
        r = self._q("SELECT seed_epoch, seed_stream, seed_seq, shared_seed_seq, watermark FROM _zebridge_generations WHERE tbl = ?", (T,))
        return [dict(tenant=self.tenant, seed_epoch=e, anchor_stream=st or f"CDC_{self.tenant}", anchor_seq=sq if sq is not None else 0,
                     shared_seq=sh, watermark=w) for e, st, sq, sh, w in r] if r else []


def worker(opts_json, lib_path):
    opts = json.loads(opts_json)
    lib = ctypes.CDLL(lib_path or str(LIBRARY))
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, a in (("sync", []), ("poll", [ctypes.c_uint64])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a
    h = lib.zb_client_connect(json.dumps(opts).encode())
    if not h: sys.exit("libzb open failed")
    lib.zb_free(lib.zb_client_sync(h))
    stop = pathlib.Path(opts["dbPath"] + ".stop")
    while not stop.exists():
        lib.zb_free(lib.zb_client_poll(h, 200))
    lib.zb_client_close(h)


def grade(stack, clients, budget):
    """Each client against PostgreSQL (the load has stopped), within `budget` s."""
    want = {c.tenant: stack.truth(c.tenant) for c in clients}
    done, t0 = {}, time.monotonic()
    while time.monotonic() - t0 < budget and len(done) < len(clients):
        for c in clients:
            have = c.rows()
            if c.name not in done and have is not None and digest(have) == digest(want[c.tenant]):
                done[c.name] = round(time.monotonic() - t0, 1)
        time.sleep(2)
    result = {}
    for c in clients:
        have, w = c.rows() or {}, want[c.tenant]
        miss, extra = sorted(w.keys() - have.keys()), sorted(have.keys() - w.keys())
        diff = sorted(k for k in w.keys() & have.keys() if w[k] != have[k])
        result[c.name] = dict(exact=c.name in done, s=done.get(c.name), rows=len(have), want=len(w), missing=miss, extra=extra, differing=diff)
        if c.name not in done:
            pathlib.Path(f"/tmp/zb_rr{stack.tag}_{c.name}.diff.json").write_text(json.dumps({"missing": miss, "extra": extra, "differing": diff}))
    return result


async def anchor_state(c, js, pairs):
    """(ok, why): an anchor for exactly the expected pairs, each under the current epoch, on
    its own route's stream with the stream identity the manifest names, and the client's
    positions at or past its cuts (own and shared). A cut of 0 is valid; absence is not."""
    epoch, rows = await js.epoch(), c.anchors()
    got = {r.get("tenant") for r in rows}
    if got != set(pairs):
        return False, f"pairs {sorted(got)} (expected {sorted(pairs)})"
    for r in rows:
        t, m = r.get("tenant"), await js.manifest(r.get("tenant")) or {}
        stream, cut = r.get("anchor_stream"), r.get("anchor_seq")
        if r.get("seed_epoch") != epoch:
            return False, f"{t}: epoch {r.get('seed_epoch')} (current {epoch})"
        if stream != f"CDC_{t}" or cut is None:
            return False, f"{t}: anchor {stream!r} at {cut!r}"
        if m.get("cdc_stream_created") and c.created(stream) != m["cdc_stream_created"]:
            return False, f"{t}: stream identity {c.created(stream)!r} != the manifest's {m['cdc_stream_created']!r}"
        if c.position(stream) < cut:
            return False, f"{t}: position {c.position(stream)} below its cut {cut}"
        shared = r.get("shared_seq")
        if shared is not None and m.get("shared_cdc_stream") and c.position(m["shared_cdc_stream"]) < shared:
            return False, f"{t}: {m['shared_cdc_stream']} position {c.position(m['shared_cdc_stream'])} below its shared cut {shared}"
    return True, f"{[{k: r.get(k) for k in ('tenant', 'seed_epoch', 'anchor_stream', 'anchor_seq', 'shared_seq')} for r in rows]}"


async def report(c, g, js, pairs):
    ev = f"D2 refusals {c.count('refusing the full')}, gap waits {c.count('gap stays open')}, seeded {c.count('seeded [0-9]+ row')}"
    if g["exact"]:
        check(f"{c.name}: exact in {g['s']} s — {ev}", True)
    else:
        check(f"{c.name}: NOT exact — {g['rows']} rows of {g['want']}: {len(g['missing'])} missing, {len(g['extra'])} extra, "
              f"{len(g['differing'])} differing — {ev}", False)
    ok, why = await anchor_state(c, js, pairs)
    check(f"{c.name}: anchored for {sorted(pairs)} under the current epoch — {why}", ok)


async def wait_manifests(js, tenants, timeout):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if all([await js.manifest(t) for t in tenants]): return round(time.monotonic() - t0, 1)
        await asyncio.sleep(1)
    return None


# ── the checks ───────────────────────────────────────────────────────────────

async def check1(a, tmp, mode="ddl"):
    """mode ddl (check 1): the DDL triggers observe the recreate; restart (check 5): the
    bridge is down while they do, and publishes after its restart; backstop (check 6): the
    triggers are disabled for the recreate (a restore), so only the producer's OID check
    observes it — bounded by the cadence, its only signal."""
    rows = {"t_a": a.rows, "t_b": a.rows // 10, "t_c": a.rows // 10}
    s = Stack(tmp, {"ddl": "1", "restart": "5", "backstop": "6"}[mode], a.cadence, {})
    try:
        s.start({"pa": "t_a", "pb": "t_b", "pc": "t_c"})
        s.create_table(rows, "a")
        b1 = s.bridge_up()
        js = await Js().open(s)
        check("tenant mappings published before any client connects", await js.tenants_ready(s.creds))
        secs = await wait_manifests(js, rows, a.cadence + 60)
        check(f"first incarnation: a chain for each of {len(rows)} pairs ({secs} s)", secs is not None)
        s.bridge_down(b1)
        b2 = s.bridge_up()
        note(f"{b2}: a new producer, no history (the field's restart before the reset)")
        live = Client(s, "live", "pa", "t_a"); live.start()
        g = grade(s, [live], 120)
        check(f"live exact on the first incarnation ({g['live']['s']} s)", g["live"]["exact"])
        e0, inc0 = await js.epoch(), s.incarnation()

        if mode == "restart":
            s.bridge_down(b2)
            note("bridge down: the recreate is observed in PostgreSQL, published after the restart")
        if mode == "backstop":
            s.sql("ALTER EVENT TRIGGER zebridge_ddl_trigger DISABLE; ALTER EVENT TRIGGER zebridge_drop_trigger DISABLE;")
        s.drop_table(); await asyncio.sleep(5)
        if mode == "backstop":
            s.create_table_raw(rows, "b")
            s.sql("ALTER EVENT TRIGGER zebridge_ddl_trigger ENABLE; ALTER EVENT TRIGGER zebridge_drop_trigger ENABLE;")
            note(f"recreated with the event triggers disabled: incarnation {inc0} -> {s.incarnation()} before any producer build")
        else:
            s.create_table(rows, "b")
        if mode == "restart":
            await asyncio.sleep(3)
            s.bridge_up()
        t_create = time.monotonic()
        load = Load(s, "t_a", "c", a.interval_ms / 1000)
        bound = a.first_chain_s if mode != "backstop" else a.cadence + 30
        epochs, usable, late = [e0], {}, None
        while time.monotonic() - t_create < a.cadence + 90 and len(usable) < len(rows):
            e = await js.epoch()
            if e is not None and e != epochs[-1]:
                epochs.append(e)
                usable.clear()   # a chain under the previous epoch is no longer usable
            for t in rows:
                m = await js.manifest(t)
                if t not in usable and m and m.get("cutoff_seq") is not None and e is not None and e > e0 and m.get("seed_epoch") == e:
                    usable[t] = round(time.monotonic() - t_create, 1)
            await asyncio.sleep(1)
        t_watch = time.monotonic()
        while time.monotonic() - t_watch < 30:   # a late bump shows here
            e = await js.epoch()
            if e is not None and e != epochs[-1]: epochs.append(e)
            await asyncio.sleep(1)
        late = Client(s, "late", "pa", "t_a"); late.start()
        await asyncio.sleep(30)
        load.end()
        final = await js.epoch()
        check(f"one incarnation transition for one recreate: {inc0} -> {s.incarnation()}", s.incarnation() == inc0 + 1)
        check(f"one seed-epoch increment for one recreate: {' -> '.join(map(str, epochs))}", final == e0 + 1 and len(epochs) == 2)
        since = "the CREATE" if mode == "ddl" else "the restart" if mode == "restart" else "the CREATE (backstop: the producer's next tick)"
        for t in rows:
            check(f"pair {t}: a chain under the current epoch within {bound} s of {since}: {usable.get(t)} s (cadence {a.cadence} s)",
                  usable.get(t) is not None and usable[t] <= bound)
        g = grade(s, [live, late], 2 * a.cadence + 60)
        for c in (live, late): await report(c, g[c.name], js, {"t_a"})
        await js.close()
    finally:
        s.stop()


async def check2(a, tmp):
    s = Stack(tmp, "2", a.cadence, {"CDC_MAX_MSGS": str(a.max_msgs)})
    try:
        s.start({"pa": "t_a"})
        s.create_table({"t_a": a.rows}, "a")
        s.bridge_up()
        js = await Js().open(s)
        check("tenant mappings published before any client connects", await js.tenants_ready(s.creds))
        check(f"a chain ({await wait_manifests(js, ['t_a'], a.cadence + 60)} s)", await js.manifest("t_a") is not None)
        away = Client(s, "away", "pa", "t_a"); away.start()
        g = grade(s, [away], 120)
        check(f"away exact before the pause ({g['away']['s']} s)", g["away"]["exact"])
        away.stop()
        p = away.position("CDC_t_a")
        load = Load(s, "t_a", "c", a.interval_ms / 1000)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 900 and (await js.stream("CDC_t_a")).get("first", 0) <= p + 1:
            await asyncio.sleep(2)
        st, m = await js.stream("CDC_t_a"), await js.manifest("t_a") or {}
        f, c = st.get("first", 0), m.get("cutoff_seq") or 0
        check(f"a recognised gap: f {f} > p {p} + 1 after {round(time.monotonic() - t0)} s", f > p + 1)
        note(f"at the return: c {c} {'>= f - 1: the chain splices at once' if c >= f - 1 else '< f - 1: the chain cannot splice, the client must wait'}")
        away.start()
        await asyncio.sleep(30)
        load.end()
        g = grade(s, [away], 2 * a.cadence + 60)
        await report(away, g["away"], js, {"t_a"})
        note(f"away's log: {away.count('gap healed')} gap healed, {away.count('gap stays open')} gap waits")
        await js.close()
    finally:
        s.stop()


async def check3(a, tmp):
    s = Stack(tmp, "3", a.cadence, {"CDC_MAX_MSGS": str(a.max_msgs)})
    try:
        s.start({"pa": "t_a"})
        s.create_table({"t_a": a.rows}, "a")
        b1 = s.bridge_up()
        js = await Js().open(s)
        check("tenant mappings published before any client connects", await js.tenants_ready(s.creds))
        check(f"first incarnation's chain ({await wait_manifests(js, ['t_a'], a.cadence + 60)} s)", await js.manifest("t_a") is not None)
        s.bridge_down(b1)
        s.bridge_up()
        e0 = await js.epoch()

        # The window with no usable chain: since the incarnation is observed in the DDL's
        # transaction, the DDL path cuts a chain within a second, so the recreate goes the
        # way only the producer's backstop sees it (event triggers disabled, a restore) —
        # a window of up to a cadence, the one in which R2 must still hold.
        s.sql("ALTER EVENT TRIGGER zebridge_ddl_trigger DISABLE; ALTER EVENT TRIGGER zebridge_drop_trigger DISABLE;")
        s.drop_table(); await asyncio.sleep(5)
        s.create_table_raw({"t_a": a.rows}, "b")
        s.sql("ALTER EVENT TRIGGER zebridge_ddl_trigger ENABLE; ALTER EVENT TRIGGER zebridge_drop_trigger ENABLE;")
        t_create = time.monotonic()
        await asyncio.sleep(3)
        prefix_end = (await js.stream("CDC_t_a")).get("last", 0)   # the base rows' messages end here
        load = Load(s, "t_a", "c", a.interval_ms / 1000)

        def usable(m, e):
            return bool(m and m.get("cutoff_seq") is not None and e is not None and e > e0 and m.get("seed_epoch") == e)
        while time.monotonic() - t_create < a.cadence + 60:
            st = await js.stream("CDC_t_a")
            if st.get("first", 0) > prefix_end + 1: break
            if usable(await js.manifest("t_a"), await js.epoch()): break
            await asyncio.sleep(1)
        st = await js.stream("CDC_t_a")
        chain_first = usable(await js.manifest("t_a"), await js.epoch())
        pruned = st.get("first", 0) > prefix_end + 1
        if not check(f"the base rows' messages (to {prefix_end}) pruned while no usable chain exists: f {st.get('first')} after "
                     f"{round(time.monotonic() - t_create)} s", pruned and not chain_first):
            note("inconclusive: the chain came first (or nothing pruned); raise the load or the cadence")
        cold = Client(s, "cold", "pa", "t_a"); cold.start()
        t_join = time.monotonic()
        base = {k for k in s.truth("t_a") if k.startswith("bt_a-")}
        seen = []
        while time.monotonic() - t_join < a.cadence + 60:
            pos, have = cold.position("CDC_t_a"), cold.rows() or {}
            m, e = await js.manifest("t_a"), await js.epoch()
            seen.append((round(time.monotonic() - t_join), pos, len(base & have.keys()), m.get("cutoff_seq") if m else None, e))
            if usable(m, e) and time.monotonic() - t_join > 20: break
            await asyncio.sleep(2)
        adv = [x for x in seen if x[1] > prefix_end and x[2] < len(base)]
        note(f"diagnostic: {'violation observed' if adv else 'no violation'} — first sample past the prefix ({prefix_end}) "
             f"while missing base rows: {adv[0] if adv else None} (t, p, base rows held, c, epoch)")
        check(f"R2: cold never certifies a position past the missing prefix ({prefix_end}) while base rows are missing "
              f"({len(adv)} of {len(seen)} samples did)", not adv)
        # While the writes go on: the unchanged base rows back, and a valid current anchor.
        t_rec, recovered, why = time.monotonic(), None, ""
        while time.monotonic() - t_rec < a.recover_s:
            have = cold.rows() or {}
            ok, why = await anchor_state(cold, js, {"t_a"})
            if ok and base <= have.keys():
                recovered = round(time.monotonic() - t_rec, 1); break
            await asyncio.sleep(2)
        check(f"while writes continue: base rows recovered and a current anchor within {a.recover_s} s of a usable chain: "
              f"{recovered} s — {why}", recovered is not None)
        load.end()
        g = grade(s, [cold], 2 * a.cadence + 60)
        await report(cold, g["cold"], js, {"t_a"})
        missing_base = len(set(g["cold"]["missing"]) & base)
        note(f"cold misses {missing_base} of the {len(base)} base rows; position {cold.position('CDC_t_a')}, newest cut "
             f"{((await js.manifest('t_a')) or {}).get('cutoff_seq')}; trace (t, p, base held, c, epoch): {seen[:3]} … {seen[-2:]}")
        await js.close()
    finally:
        s.stop()


async def check4(a, tmp, ts=False):
    """The fence's crash and retention boundaries, one client per case, the load running.
    A seed-epoch move (zebridge_reseed) gives a prompt chain cut behind a live follower.
    ts: the same cases with a zb-client-ts client (check 12)."""
    hooks = (b"ZB_TEST_FENCE_ABORT", b"ZB_TEST_FENCE_SLEEP_MS", b"ZB_TEST_SHARED_UNREADABLE", b"ZB_TEST_SCHEMA_PAUSE_FILE")
    if ts:
        # The fault points belong to the harness: the library reads no environment for them.
        lib_src = (zb.ROOT / "zb-client-ts/src/libzb.ts").read_bytes()
        if not check("the TypeScript library reads no fault variable (its faultHook is a config option)", not any(h in lib_src for h in hooks) and b"ZB_TEST_FENCE" not in lib_src):
            return
        if not check(f"the harness exists ({HARNESS})", HARNESS.exists()):
            return
    else:
        if not check(f"the ordinary library carries no fault hook ({LIBRARY})", LIBRARY.exists() and not any(h in LIBRARY.read_bytes() for h in hooks)):
            return
        if not check(f"check 4 runs on a fault-hook build ({FAULT_LIBRARY})", FAULT_LIBRARY.exists() and all(h in FAULT_LIBRARY.read_bytes() for h in hooks)):
            note(f"build it: {FAULT_BUILD}")
            return
    s = Stack(tmp, "12" if ts else "4", a.cadence, {})
    try:
        s.start({"pa": "t_a"})
        s.create_table({"t_a": a.rows}, "a")
        s.bridge_up()
        js = await Js().open(s)
        check("tenant mappings published before any client connects", await js.tenants_ready(s.creds))
        check(f"a chain ({await wait_manifests(js, ['t_a'], a.cadence + 60)} s)", await js.manifest("t_a") is not None)
        load = Load(s, "t_a", "c", a.interval_ms / 1000)
        base = {k for k in s.truth("t_a") if k.startswith("at_a-")}

        async def until(pred, timeout):
            t0 = time.monotonic()
            while time.monotonic() - t0 < timeout:
                r = pred()
                if asyncio.iscoroutine(r): r = await r
                if r: return round(time.monotonic() - t0, 1)
                await asyncio.sleep(1)
            return None

        async def recovered(c):
            ok, _ = await anchor_state(c, js, {"t_a"})
            return ok and base <= (c.rows() or {}).keys()

        def fence_cut(c):
            m = re.findall(r"fence prepared — chain g\d+ cut at (\d+)", c.text())
            return int(m[-1]) if m else None

        def reseed():
            s.sql(f"SELECT count(*) FROM zebridge_reseed('public.{T}'::regclass)")

        clients = []
        cases = (("abort_chunk", {"ZB_TEST_FENCE_ABORT": "after_chunk"}),
                 ("retry_overtaken", {"ZB_TEST_FENCE_ABORT": "after_chunk"}),
                 ("abort_commit", {"ZB_TEST_FENCE_ABORT": "before_commit"}),
                 ("overtaken", {"ZB_TEST_FENCE_SLEEP_MS": "8000"}),
                 ("shared_unknown", {"ZB_TEST_SHARED_UNREADABLE": "1"}),
                 ("pg_abort_commit", {"ZB_TEST_FENCE_ABORT": "before_commit"}),
                 ("pg_retry_overtaken", {"ZB_TEST_FENCE_ABORT": "after_chunk"}))
        for name, env in cases:
            # pg_*: the same cases on PostgreSQL storage (one database per case's client), libzb's `dbUrl`.
            if ts and name.startswith("pg_"): continue
            pg = s.replica_url(name) if name.startswith("pg_") else None
            c = (TsClient(s, name, "pa", "t_a", chunk_rows=200) if ts
                 else Client(s, name, "pa", "t_a", chunk_rows=200, lib=FAULT_LIBRARY, pg_url=pg))
            clients.append(c)
            c.start()
            check(f"{name}: seeded and anchored before the case ({await until(lambda: recovered(c), 120)} s)", await recovered(c))
            c.stop()
            c.start(env)
            await asyncio.sleep(5)   # caught up with the load again: its position runs ahead of any new cut
            reseed()
            if name in ("retry_overtaken", "pg_retry_overtaken"):
                # Two attempts: a crash after a chunk; the reopened client's retry stages
                # the same fence and the stream prunes past the cut meanwhile. The retry's
                # cursor sits AT the cut — a fence only because PREPARE left it owed.
                rc = await until(lambda: c.exited() is not None, 90)
                cut = fence_cut(c)
                check(f"{name}: attempt 1 aborted after a chunk ({c.exited()}): fence owed {c.fence_pending()}, anchors {c.anchors()}, "
                      f"position {c.position('CDC_t_a')}, cut {cut}",
                      rc is not None and c.fence_pending() and c.anchors() == [] and c.position("CDC_t_a") == cut)
                held_before = c.text().count("fence holding")
                c.start({"ZB_TEST_FENCE_SLEEP_MS": "8000"})
                held = await until(lambda: c.text().count("fence holding") > held_before, 90)
                cut2 = fence_cut(c)
                check(f"{name}: attempt 2 is a fence again, staging (cut {cut2}, cursor {c.position('CDC_t_a')}) — {held} s",
                      held is not None and cut2 is not None)
                if cut2 is not None:
                    await js.js.purge_stream("CDC_t_a", seq=cut2 + 5)
                    refused = await until(lambda: c.text().count("fence not committed") > 0, 30)
                    check(f"{name}: attempt 2 not committed after the stream moved past the cut ({refused} s): fence owed {c.fence_pending()}, "
                          f"anchors {c.anchors()}, position {c.position('CDC_t_a')}",
                          refused is not None and c.fence_pending() and c.anchors() == [] and c.position("CDC_t_a") == cut2)
                secs = await until(lambda: recovered(c), 120)
                check(f"{name}: recovered from a newer chain, the fence no longer owed ({secs} s, owed {c.fence_pending()})",
                      secs is not None and not c.fence_pending())
            elif name.startswith("abort") or name == "pg_abort_commit":
                rc = await until(lambda: c.exited() is not None, 90)
                check(f"{name}: the client aborted at the fence ({c.exited()})", rc is not None)
                cut = fence_cut(c)
                check(f"{name}: after the crash — unseeded, cursor at the cut: anchors {c.anchors()}, position {c.position('CDC_t_a')}, cut {cut}",
                      cut is not None and c.anchors() == [] and c.position("CDC_t_a") == cut)
                c.start()
                secs = await until(lambda: recovered(c), 90)
                check(f"{name}: reopened — recovered with a current anchor while writes continue ({secs} s)", secs is not None)
            elif name == "overtaken":
                held = await until(lambda: "fence holding" in c.text(), 90)
                cut = fence_cut(c)
                check(f"overtaken: the fence is staging (cut {cut}) — {held} s", held is not None and cut is not None)
                if cut is not None:
                    await js.js.purge_stream("CDC_t_a", seq=cut + 5)   # retention past the cut, mid-recovery
                    st = await js.stream("CDC_t_a")
                    note(f"overtaken: purged CDC_t_a below {cut + 5}: first {st.get('first')}")
                    refused = await until(lambda: "fence not committed" in c.text(), 30)
                    check(f"overtaken: not committed after the stream moved past the cut ({refused} s): anchors {c.anchors()}, position {c.position('CDC_t_a')}",
                          refused is not None and c.anchors() == [] and c.position("CDC_t_a") == cut)
                secs = await until(lambda: recovered(c), 120)
                check(f"overtaken: recovered from a newer chain while writes continue ({secs} s)", secs is not None)
            else:
                t0 = time.monotonic()
                await until(lambda: c.count("its coverage on CDC_PUBLIC is unknown") > 0, 60)
                await asyncio.sleep(10)
                check(f"shared_unknown: the fence refused with the shared route unreadable — refusals {c.count('its coverage on CDC_PUBLIC is unknown')}, "
                      f"prepared {c.count('fence prepared')}, anchors {c.anchors()}",
                      c.count("its coverage on CDC_PUBLIC is unknown") > 0 and c.count("fence prepared") == 0 and c.anchors() == [])
                c.stop()
                c.start()
                secs = await until(lambda: recovered(c), 90)
                check(f"shared_unknown: reopened with the route readable — recovered while writes continue ({secs} s)", secs is not None)
        load.end()
        g = grade(s, clients, 2 * a.cadence + 60)
        for c in clients: await report(c, g[c.name], js, {"t_a"})
        await js.close()
    finally:
        s.stop()


T2 = "rr_items2"


async def check13(a, tmp, ts=False):
    """The applied frontier, live: two tables on one tenant route, one of them fenced.
    ts: the same cases with a zb-client-ts client (check 14)."""
    hooks = (b"ZB_TEST_FENCE_ABORT", b"ZB_TEST_FENCE_SLEEP_MS", b"ZB_TEST_SHARED_UNREADABLE", b"ZB_TEST_SCHEMA_PAUSE_FILE")
    if ts:
        lib_src = (zb.ROOT / "zb-client-ts/src/libzb.ts").read_bytes()
        if not check("the TypeScript library reads no fault variable (its faultHook is a config option)",
                     not any(h in lib_src for h in hooks) and b"ZB_TEST_FENCE" not in lib_src):
            return
        if not check(f"the harness exists ({HARNESS})", HARNESS.exists()):
            return
    elif not check(f"check 13 runs on a fault-hook build ({FAULT_LIBRARY})", FAULT_LIBRARY.exists() and all(h in FAULT_LIBRARY.read_bytes() for h in hooks)):
        note(f"build it: {FAULT_BUILD}")
        return
    s = Stack(tmp, "14" if ts else "13", a.cadence, {})
    try:
        s.start({"pa": "t_a"})
        s.create_table({"t_a": a.rows}, "a")
        s.sql(f"CREATE TABLE public.{T2} (id text PRIMARY KEY, tenant_id text NOT NULL, note text, "
              f"updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz);"
              f"SELECT count(*) FROM zebridge_enable('public.{T2}'::regclass, writable => true, tenant_col => 'tenant_id', "
              f"version_col => 'updated_at', tombstone_col => 'deleted_at', publication => 'rr_pub', dry_run => false);"
              f"INSERT INTO public.{T2} (id, tenant_id, note) SELECT 'bt_a-' || lpad(i::text, 6, '0'), 't_a', repeat(md5(i::text), 4) "
              f"FROM generate_series(1, {a.rows}) i;")
        s.bridge_up()
        js = await Js().open(s)
        check("tenant mappings published before any client connects", await js.tenants_ready(s.creds))
        async def manifests():
            return all([await js.kv(js.gen, f"t_a.{t}") for t in (T, T2)])
        t0 = time.monotonic()
        while time.monotonic() - t0 < a.cadence + 60 and not await manifests(): await asyncio.sleep(1)
        check(f"a chain for each table ({round(time.monotonic() - t0, 1)} s)", await manifests())
        base = {T: {k for k in s.truth("t_a") if k.startswith("at_a-")}}

        def table_rows(c, table):
            return dict(c._q(f"SELECT id, note FROM {table} WHERE tenant_id = ?", (c.tenant,)) or [])

        def truth_of(table):
            rows = {}
            for line in s.sql(f"SELECT id, coalesce(note, E'\\\\N') FROM public.{table} WHERE tenant_id = 't_a' AND deleted_at IS NULL").splitlines():
                k, v = line.split("|", 1)
                rows[k] = None if v == "\\N" else v
            return rows

        async def until(pred, timeout):
            t0 = time.monotonic()
            while time.monotonic() - t0 < timeout:
                r = pred()
                if asyncio.iscoroutine(r): r = await r
                if r: return round(time.monotonic() - t0, 1)
                await asyncio.sleep(1)
            return None

        def seeded(c):
            held = {r[0] for r in (c._q(f"SELECT tbl FROM {'_zebridge_generations' if ts else '_zbz_generations'}") or [])}
            return held == {T, T2}

        async def recovered(c):
            ok, _ = await anchor_state(c, js, {"t_a"})
            return ok and seeded(c) and base[T] <= table_rows(c, T).keys()

        def frontier_rows(c):
            return c._q(f"SELECT tbl, stream, seq FROM {'_zebridge_frontier' if ts else '_zbz_frontier'}") or []

        def skipped(c):
            m = re.findall(r"(\d+) event\(s\) skipped so far", c.text())
            return int(m[-1]) if m else 0

        def exact(c):
            return all(digest(table_rows(c, t)) == digest(truth_of(t)) for t in (T, T2))

        def fence_cut(c):
            m = re.findall(r"fence prepared — chain g\d+ cut at (\d+)", c.text())
            return int(m[-1]) if m else None

        def reseed():
            s.sql(f"SELECT count(*) FROM zebridge_reseed('public.{T}'::regclass)")

        clients = []
        # There used to be a first case here, "frontier_reseeded": a CDC_PUBLIC the client had never read a message of
        # counted as gapped for ever (stored 0), so every pass re-seeded each table with a shared route and the other
        # table got a chain in the fence's own pass and lost its frontier (exact, no skip expected). A stream recorded
        # as born empty is not a gap any more (core.streamHasGap, NOTES 2026-10-28), so that case would have been the
        # next one; the frontier that a chain drops is pinned by fence-apply.test.ts. Re-seeding both tables on purpose
        # is another matter: HANDOFF.md. CDC_PUBLIC stays empty here: the cases run on a quiet stream.
        for name, env in (("frontier_live", {}), ("frontier_crash", {"ZB_TEST_FENCE_ABORT": "after_chunk"})):
            # One client and one load per case: a client left running would follow the next case's
            # epoch move too. The two tables together write about what check 4's one does.
            loads = [Load(s, "t_a", "c", 2 * a.interval_ms / 1000, table=T), Load(s, "t_a", "d", 2 * a.interval_ms / 1000, table=T2)]
            c = (TsClient(s, name, "pa", "t_a", chunk_rows=200) if ts
                 else Client(s, name, "pa", "t_a", chunk_rows=200, lib=FAULT_LIBRARY))
            c.tables = [T, T2]
            clients.append(c)
            c.start()
            check(f"{name}: both tables seeded and anchored before the case ({await until(lambda: recovered(c), 150)} s)", await recovered(c))
            c.stop()
            # The schema watch is held while the test says so: the chain under the new epoch is
            # ready before this client looks at it, and the tails go on applying meanwhile — its
            # position runs ahead of the chain's cut by as long as the hold lasts.
            pause = pathlib.Path(f"/tmp/zb-rr13-{name}.pause")
            pause.unlink(missing_ok=True)
            c.start(dict(env, **{"ZB_TEST_FENCE_HOLD_FILE" if ts else "ZB_TEST_SCHEMA_PAUSE_FILE": str(pause)}))
            await asyncio.sleep(5)   # caught up with the load again
            pause.touch()
            epoch0 = await js.epoch()
            reseed()
            async def new_chain():
                m = await js.kv(js.gen, f"t_a.{T}") or {}
                ep = await js.epoch()
                return ep is not None and ep > epoch0 and m.get("seed_epoch") == ep
            note(f"{name}: the chain under the new epoch after {await until(new_chain, 90)} s")
            await asyncio.sleep(4)
            check(f"{name}: held back, the client has not acted on it ({c.count('seed epoch [0-9]+ -> [0-9]+')} epoch notices)",
                  c.count("seed epoch [0-9]+ -> [0-9]+") == 0)
            pause.unlink(missing_ok=True)
            if name != "frontier_crash":
                took = await until(lambda: fence_cut(c) is not None, 90)
                check(f"{name}: the fence is taken with {T2} on the same route ({took} s, cut {fence_cut(c)}) — not refused: refusals {c.count('refusing the full')}",
                      took is not None and c.count("refusing the full") == 0)
                check(f"{name}: the log says {T2} keeps its applied frontier", "keep their applied frontier" in c.text())
                secs = await until(lambda: recovered(c), 120)
                check(f"{name}: {T} recovered under a current anchor while writes continue ({secs} s)", secs is not None)
            else:
                rc = await until(lambda: c.exited() is not None, 90)
                cut = fence_cut(c)
                fr = frontier_rows(c)
                pos = c.position("CDC_t_a")
                check(f"{name}: the client aborted after a chunk ({c.exited()}); position {pos} at the cut {cut}, fence owed {c.fence_pending()}",
                      rc is not None and cut is not None and pos == cut and c.fence_pending())
                check(f"{name}: the frontier is durable — {fr}",
                      len(fr) == 1 and fr[0][0] == T2 and fr[0][1] == "CDC_t_a" and fr[0][2] > pos)
                c.start()
                secs = await until(lambda: recovered(c), 120)
                check(f"{name}: reopened — {T} recovered while writes continue ({secs} s)", secs is not None)
            for l in loads: l.end()
            ok = await until(lambda: exact(c), 150)
            check(f"{name}: BOTH tables exact against PostgreSQL ({ok} s) — {T}: {len(table_rows(c, T))} of {len(truth_of(T))}, "
                  f"{T2}: {len(table_rows(c, T2))} of {len(truth_of(T2))}", ok is not None)
            if ok is None:
                for t in (T, T2):
                    have, want = table_rows(c, t), truth_of(t)
                    note(f"{t}: missing {sorted(want.keys() - have.keys())[:5]}, extra {sorted(have.keys() - want.keys())[:5]}, "
                         f"differing {sorted(k for k in want.keys() & have.keys() if want[k] != have[k])[:5]}")
            check(f"{name}: events were skipped through the frontier ({skipped(c)}) and it is spent ({frontier_rows(c)})",
                  skipped(c) > 0 and frontier_rows(c) == [])
            c.stop()
        await js.close()
    finally:
        s.stop()


T3 = "rr_items3"


async def check19(a, tmp, ts=False):
    """The joint fence, live: two tables of one tenant route reseeded together while the replica's position is past both
    chains' cuts, and a third table untouched (it takes inserts, updates and tombstones throughout). Each table's anchor
    goes with the epoch move, so neither fence can stay the other's untouched table: before the joint fence they refused
    each other for ever (NOTES "The joint fence"). Cases: both orders of the two reseeds; the client aborts
    after a chunk and reopens; the second table's full is deleted from the object store before the client acts, so its
    replacement is unavailable until it is reseeded again. Always: no refusal, every table exact against PostgreSQL.
    ts: the same cases with a zb-client-ts client (check 20)."""
    hooks = (b"ZB_TEST_FENCE_ABORT", b"ZB_TEST_FENCE_SLEEP_MS", b"ZB_TEST_SHARED_UNREADABLE", b"ZB_TEST_SCHEMA_PAUSE_FILE")
    if ts:
        if not check(f"the harness exists ({HARNESS})", HARNESS.exists()):
            return
    elif not check(f"check 19 runs on a fault-hook build ({FAULT_LIBRARY})", FAULT_LIBRARY.exists() and all(h in FAULT_LIBRARY.read_bytes() for h in hooks)):
        note(f"build it: {FAULT_BUILD}")
        return
    s = Stack(tmp, "20" if ts else "19", a.cadence, {})
    tables = [T, T2, T3]
    try:
        s.start({"pa": "t_a"})
        s.create_table({"t_a": a.rows}, "a")
        for extra in (T2, T3):
            s.sql(f"CREATE TABLE public.{extra} (id text PRIMARY KEY, tenant_id text NOT NULL, note text, "
                  f"updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz);"
                  f"SELECT count(*) FROM zebridge_enable('public.{extra}'::regclass, writable => true, tenant_col => 'tenant_id', "
                  f"version_col => 'updated_at', tombstone_col => 'deleted_at', publication => 'rr_pub', dry_run => false);"
                  f"INSERT INTO public.{extra} (id, tenant_id, note) SELECT 'bt_a-' || lpad(i::text, 6, '0'), 't_a', repeat(md5(i::text), 4) "
                  f"FROM generate_series(1, {a.rows}) i;")
        s.bridge_up()
        js = await Js().open(s)
        check("tenant mappings published before any client connects", await js.tenants_ready(s.creds))

        async def manifests():
            return all([await js.kv(js.gen, f"t_a.{t}") for t in tables])
        t0 = time.monotonic()
        while time.monotonic() - t0 < a.cadence + 60 and not await manifests(): await asyncio.sleep(1)
        check(f"a chain for each table ({round(time.monotonic() - t0, 1)} s)", await manifests())
        base = {k for k in s.truth("t_a") if k.startswith("at_a-")}

        def table_rows(c, table):
            return dict(c._q(f"SELECT id, note FROM {table} WHERE tenant_id = ?", (c.tenant,)) or [])

        def truth_of(table):
            rows = {}
            for line in s.sql(f"SELECT id, coalesce(note, E'\\\\N') FROM public.{table} WHERE tenant_id = 't_a' AND deleted_at IS NULL").splitlines():
                k, v = line.split("|", 1)
                rows[k] = None if v == "\\N" else v
            return rows

        async def until(pred, timeout):
            t0 = time.monotonic()
            while time.monotonic() - t0 < timeout:
                r = pred()
                if asyncio.iscoroutine(r): r = await r
                if r: return round(time.monotonic() - t0, 1)
                await asyncio.sleep(1)
            return None

        def anchored(c):
            return {r[0] for r in (c._q(f"SELECT tbl FROM {'_zebridge_generations' if ts else '_zbz_generations'}") or [])}

        async def recovered(c):
            ok, _ = await anchor_state(c, js, {"t_a"})
            return ok and anchored(c) == set(tables) and base <= table_rows(c, T).keys()

        def exact(c):
            return all(digest(table_rows(c, t)) == digest(truth_of(t)) for t in tables)

        async def seed_epoch(table):
            d = await js.kv(js.schemas, table)
            return d.get("seed_epoch", 0) if isinstance(d, dict) else None

        def reseed(table):
            s.sql(f"SELECT count(*) FROM zebridge_reseed('public.{table}'::regclass)")

        async def new_chain(table, epoch0):
            m = await js.kv(js.gen, f"t_a.{table}") or {}
            ep = await seed_epoch(table)
            return ep is not None and ep > epoch0 and m.get("seed_epoch") == ep

        clients = []
        cases = (("joint_live", [T, T2], {}, False, False), ("joint_swapped", [T2, T], {}, False, False),
                 ("joint_crash", [T, T2], {"ZB_TEST_FENCE_ABORT": "after_chunk"}, False, False), ("joint_unavailable", [T, T2], {}, True, False),
                 # The client dies right after the FIRST participant committed; the stream then drops the events after the first
                 # cut(s) while it is down. The group has to come back from the replica: the routes parked before any tail, the
                 # earlier participant proven again before they open.
                 ("joint_restart", [T, T2], {"ZB_TEST_FENCE_ABORT": "after_commit"}, False, True))
        for name, order, env, unavailable, restart in cases:
            loads = [Load(s, "t_a", p, 2 * a.interval_ms / 1000, table=t) for p, t in zip("cde", tables)]
            c = (TsClient(s, name, "pa", "t_a", chunk_rows=200) if ts
                 else Client(s, name, "pa", "t_a", chunk_rows=200, lib=FAULT_LIBRARY))
            c.tables = tables
            clients.append(c)
            c.start()
            check(f"{name}: all three tables seeded and anchored before the case ({await until(lambda: recovered(c), 150)} s)", await recovered(c))
            c.stop()
            # Held back while the test says so: both chains under the new epochs are ready before this client looks at
            # them, and the tails go on applying meanwhile — its position runs ahead of both cuts.
            pause = pathlib.Path(f"/tmp/zb-rr19-{name}.pause")
            pause.unlink(missing_ok=True)
            c.start(dict(env, **{"ZB_TEST_FENCE_HOLD_FILE" if ts else "ZB_TEST_SCHEMA_PAUSE_FILE": str(pause)}))
            await asyncio.sleep(5)
            pause.touch()
            before = {t: await seed_epoch(t) for t in order}
            for t in order:
                reseed(t)
                await asyncio.sleep(2)   # the second table's cut is taken later than the first's: different cuts
            got = [await until(lambda t=t: new_chain(t, before[t]), 90) for t in order]
            note(f"{name}: the chains under the new epochs after {got} s")
            await asyncio.sleep(4)
            check(f"{name}: held back, the client has not acted on them ({c.count('seed epoch [0-9]+ -> [0-9]+')} epoch notices)",
                  c.count("seed epoch [0-9]+ -> [0-9]+") == 0)
            if unavailable:
                m = await js.kv(js.gen, f"t_a.{T2}") or {}
                try:
                    store = await js.js.object_store(m["bucket"])
                    await store.delete(m["full"]["object"])
                    note(f"{name}: {T2}'s full {m['full']['object']} deleted from {m['bucket']}")
                except Exception as e:
                    check(f"{name}: could not delete {T2}'s full: {e}", False)
            pos_before = c.position("CDC_t_a")
            pause.unlink(missing_ok=True)
            if unavailable:
                await asyncio.sleep(25)
                # Two safe states: nothing was touched, or what was is owed. Either way the pair is not trusted (an anchor
                # is only written when its chain was applied) and the third table is.
                check(f"{name}: {T2}'s replacement is unavailable: the pair is not trusted (anchors {sorted(anchored(c))}; recovery owed: {c.fence_pending()})",
                      anchored(c) != set(tables) and T3 in anchored(c))
                reseed(T2)   # a new full under a new epoch
                note(f"{name}: {T2} reseeded again")
            elif env:
                rc = await until(lambda: c.exited() is not None, 90)
                pos = c.position("CDC_t_a")
                what = "after the first participant committed" if restart else "after a chunk"
                owed_any = int((c._q(f"SELECT count(*) FROM {'_zebridge_fence' if ts else '_zbz_fence'}") or [[0]])[0][0])
                check(f"{name}: the client aborted {what} ({c.exited()}); position {pos} moved back from {pos_before}, owed records {owed_any}",
                      rc is not None and pos is not None and pos_before is not None and pos < pos_before and owed_any > 0)
                if restart:
                    ms = [await js.kv(js.gen, f"t_a.{t}") or {} for t in tables[:2]]
                    cuts = [m.get("cutoff_seq") for m in ms]
                    if all(isinstance(x, int) for x in cuts):
                        await js.js.purge_stream("CDC_t_a", seq=max(cuts))
                        st = await js.stream("CDC_t_a") or {}
                        note(f"{name}: purged CDC_t_a up to {max(cuts)} (cuts {cuts}): first {st.get('first')}")
                    mark = len(c.text())
                c.start()
            secs = await until(lambda: recovered(c), 150)
            check(f"{name}: all three recovered under current anchors while writes continue ({secs} s)", secs is not None)
            if restart:
                after = c.text()[mark:]
                restored = after.find("joint fence unfinished")
                decided = min([i for i in (after.find("joint fence complete"), after.find("joint fence not released")) if i >= 0] or [-1])
                tail = after.find("tail consumer")
                check(f"{name}: the restart parks the routes of the unfinished group before anything else ({restored >= 0}); "
                      f"the group is decided (complete or an earlier participant owed again) before a tail opens (restore at {restored}, decided at {decided}, first tail at {tail})",
                      restored >= 0 and decided >= 0 and (tail < 0 or restored < tail and decided < tail))
            check(f"{name}: no table was refused for another's missing coverage ({c.count('refusing the full')} refusal(s))",
                  c.count("refusing the full") == 0 or unavailable)
            for l in loads: l.end()
            ok = await until(lambda: exact(c), 150)
            check(f"{name}: ALL tables exact against PostgreSQL ({ok} s) — " + ", ".join(f"{t}: {len(table_rows(c, t))} of {len(truth_of(t))}" for t in tables), ok is not None)
            if ok is None:
                for t in tables:
                    have, want = table_rows(c, t), truth_of(t)
                    note(f"{t}: missing {sorted(want.keys() - have.keys())[:5]}, extra {sorted(have.keys() - want.keys())[:5]}, "
                         f"differing {sorted(k for k in want.keys() & have.keys() if want[k] != have[k])[:5]}")
            c.stop()
        await js.close()
    finally:
        s.stop()


async def check15(a, tmp, ts=False):
    """A destructive plain replacement that fails after its first chunk is durably owed."""
    hooks = (b"ZB_TEST_FENCE_ABORT", b"ZB_TEST_FENCE_SLEEP_MS", b"ZB_TEST_SHARED_UNREADABLE", b"ZB_TEST_SCHEMA_PAUSE_FILE")
    if ts:
        if not check(f"the harness exists ({HARNESS})", HARNESS.exists()):
            return
    elif not check(f"check 15 runs on a fault-hook build ({FAULT_LIBRARY})", FAULT_LIBRARY.exists() and all(h in FAULT_LIBRARY.read_bytes() for h in hooks)):
        note(f"build it: {FAULT_BUILD}")
        return
    s = Stack(tmp, "16" if ts else "15", a.cadence, {})
    try:
        s.start({"pa": "t_a"})
        s.create_table({"t_a": a.rows}, "a")
        s.bridge_up()
        js = await Js().open(s)
        check("tenant mappings published before any client connects", await js.tenants_ready(s.creds))
        check(f"a chain ({await wait_manifests(js, ['t_a'], a.cadence + 60)} s)", await js.manifest("t_a") is not None)
        load = Load(s, "t_a", "c", a.interval_ms / 1000)
        base = {k for k in s.truth("t_a") if k.startswith("at_a-")}

        async def until(pred, timeout):
            t0 = time.monotonic()
            while time.monotonic() - t0 < timeout:
                r = pred()
                if asyncio.iscoroutine(r): r = await r
                if r: return round(time.monotonic() - t0, 1)
                await asyncio.sleep(1)
            return None

        async def recovered(c):
            ok, _ = await anchor_state(c, js, {"t_a"})
            return ok and base <= (c.rows() or {}).keys()

        c = (TsClient(s, "plain_abort", "pa", "t_a", chunk_rows=200) if ts
             else Client(s, "plain_abort", "pa", "t_a", chunk_rows=200, lib=FAULT_LIBRARY))
        c.start()
        check(f"seeded and anchored before the case ({await until(lambda: recovered(c), 120)} s)", await recovered(c))
        c.stop()
        pos0 = c.position("CDC_t_a")
        await asyncio.sleep(4)   # the stream moves on while the replica is closed
        epoch0 = await js.epoch()
        s.sql(f"SELECT count(*) FROM zebridge_reseed('public.{T}'::regclass)")

        async def new_chain():
            m, ep = await js.manifest("t_a") or {}, await js.epoch()
            return ep is not None and ep > epoch0 and m.get("seed_epoch") == ep
        note(f"the chain under the new epoch after {await until(new_chain, 90)} s (its cut is ahead of the replica's position {pos0})")
        c.start({"ZB_TEST_FENCE_ABORT": "after_chunk"})
        rc = await until(lambda: c.exited() is not None, 90)
        check(f"the reopened replica aborted after the first chunk of the full ({c.exited()})", rc is not None and c.exited() == 70)
        check(f"it is durably incomplete: owed {c.fence_pending()}, anchors {c.anchors()}, position {c.position('CDC_t_a')} (was {pos0}, a plain seed moves no cursor)",
              c.fence_pending() and c.anchors() == [] and c.position("CDC_t_a") == pos0)
        c.start()
        secs = await until(lambda: recovered(c), 120)
        check(f"reopened: recovered under a current anchor while writes continue, nothing owed ({secs} s, owed {c.fence_pending()})",
              secs is not None and not c.fence_pending())
        load.end()
        g = grade(s, [c], 2 * a.cadence + 60)
        await report(c, g[c.name], js, {"t_a"})
        await js.close()
    finally:
        s.stop()


async def check17(a, tmp, ts=False):
    """A stream that has never held a message is not a gap (core.streamHasGap, `born_empty`). CDC_PUBLIC of a fresh stack
    is empty; a client that records it at its first sight and reopens must not re-seed its tables because its position
    on it is 0. A message the client has not read is still a gap: the rule gives up nothing but the empty case."""
    if ts and not check(f"the harness exists ({HARNESS})", HARNESS.exists()):
        return
    s = Stack(tmp, "18" if ts else "17", a.cadence, {})
    seeds = lambda c: len(re.findall(r"seeded \d+ row\(s\) from chain|Seeded \S+ from generation chain", c.text()))
    try:
        s.start({"pa": "t_a"})
        s.create_table({"t_a": a.rows // 10}, "a")
        s.bridge_up()
        js = await Js().open(s)
        check("tenant mappings published before any client connects", await js.tenants_ready(s.creds))
        check(f"a chain ({await wait_manifests(js, ['t_a'], a.cadence + 60)} s)", await js.manifest("t_a") is not None)
        pub = await js.stream("CDC_PUBLIC")
        check(f"CDC_PUBLIC has never held a message ({pub})", bool(pub) and pub.get("last") == 0)

        async def until(pred, timeout):
            t0 = time.monotonic()
            while time.monotonic() - t0 < timeout:
                r = pred()
                if asyncio.iscoroutine(r): r = await r
                if r: return round(time.monotonic() - t0, 1)
                await asyncio.sleep(1)
            return None

        async def seeded(c):
            ok, _ = await anchor_state(c, js, {"t_a"})
            return ok

        c = TsClient(s, "quiet", "pa", "t_a") if ts else Client(s, "quiet", "pa", "t_a")
        c.start()
        check(f"seeded and anchored ({await until(lambda: seeded(c), 120)} s)", await seeded(c))
        n1 = seeds(c)
        c.stop(); c.p = None
        for k in range(2):
            c.start()
            await until(lambda: seeded(c), 60)
            await asyncio.sleep(10)   # the gap pass runs at connect
            check(f"reopen {k + 1}: nothing re-seeded because a stream that never held a message has position 0 (seeds {n1} -> {seeds(c)})", seeds(c) == n1)
            c.stop(); c.p = None
        # now the stream holds a message the client has not read: the position 0 is a gap again
        s.sql(f"INSERT INTO public.{T} (id, tenant_id, note) VALUES ('pub-1', '_default', 'open')")
        for _ in range(30):
            pub = await js.stream("CDC_PUBLIC")
            if pub.get("last", 0) > 0: break
            await asyncio.sleep(1)
        check(f"the open-tenant row reached CDC_PUBLIC ({pub})", pub.get("last", 0) > 0)
        c.start()
        await until(lambda: seeded(c), 60)
        await asyncio.sleep(10)
        check(f"reopened with a message the client never read: it re-seeds, as it always did (seeds {n1} -> {seeds(c)})", seeds(c) > n1)
        c.stop()
        await js.close()
    finally:
        s.stop()


BRIDGE_HOOKS = (b"ZB_TEST_OBSERVE_SLEEP_MS", b"ZB_TEST_PRUNE_SLEEP_MS", b"ZB_TEST_BOOT_PRUNE_SLEEP_MS")


def fault_bridge_ready():
    if not check(f"checks 7-9 run on a fault-hook bridge ({FAULT_BRIDGE})",
                 FAULT_BRIDGE.exists() and all(h in FAULT_BRIDGE.read_bytes() for h in BRIDGE_HOOKS)):
        note(f"build it: {FAULT_BRIDGE_BUILD}")
        return False
    return check(f"the ordinary bridge carries none of the hooks ({BRIDGE})",
                 not any(h in BRIDGE.read_bytes() for h in BRIDGE_HOOKS))


async def check7(a, tmp):
    """A stale observation: an OID read before another recreate changes nothing."""
    if not fault_bridge_ready(): return
    rows = {"t_a": a.rows // 10, "t_b": a.rows // 10}
    s = Stack(tmp, "7", a.cadence, {"ZB_TEST_OBSERVE_SLEEP_MS": "20000"})
    s.bridge_bin = FAULT_BRIDGE
    try:
        s.start({"pa": "t_a", "pb": "t_b"})
        s.create_table(rows, "a")
        # ── the rule itself, in SQL: two recreates, then the two earlier OIDs submitted
        x = s.live_oid()
        s.drop_table(); s.create_table(rows, "b"); y = s.live_oid()
        s.drop_table(); s.create_table(rows, "c"); z = s.live_oid()
        before = s.recorded()
        for stale in (x, y):
            s.sql(f"SELECT * FROM public.zebridge_observe_incarnation('{T}', {stale}::oid)")
        after = s.recorded()
        check(f"SQL: the earlier OIDs {x} and {y} observed after the table became {z} change nothing: {before} -> {after}",
              before == after and after[2] == after[3] == z)
        # ── through the producer: its observation held across another recreate
        b1 = s.bridge_up()
        js = await Js().open(s)
        check("tenant mappings published", await js.tenants_ready(s.creds))
        check(f"chains ({await wait_manifests(js, rows, a.cadence + 60)} s)", all([await js.manifest(t) for t in rows]))
        inc0, ep0, _, _ = s.recorded()
        # a recreate only the producer sees (event triggers off): it reads the OID, holds
        s.sql("ALTER EVENT TRIGGER zebridge_ddl_trigger DISABLE; ALTER EVENT TRIGGER zebridge_drop_trigger DISABLE;")
        s.drop_table(); await asyncio.sleep(2); s.create_table_raw(rows, "d")
        s.sql("ALTER EVENT TRIGGER zebridge_ddl_trigger ENABLE; ALTER EVENT TRIGGER zebridge_drop_trigger ENABLE;")
        unseen = s.live_oid()
        t0 = time.monotonic()
        while time.monotonic() - t0 < a.cadence + 60 and "observation of oid" not in open(s.log(b1), errors="replace").read():
            await asyncio.sleep(1)
        held = "observation of oid" in open(s.log(b1), errors="replace").read()
        check(f"the producer read oid {unseen} and holds its observation ({round(time.monotonic() - t0)} s)", held)
        # another recreate, through the DDL path, while the observation is held
        s.drop_table(); await asyncio.sleep(2); s.create_table(rows, "e")
        current = s.live_oid()
        await asyncio.sleep(30)   # the held observation is applied meanwhile
        inc1, ep1, rec1, live1 = s.recorded()
        check(f"one transition and one epoch move for the observed recreate, none for the stale one: incarnation {inc0} -> {inc1}, "
              f"epoch {ep0} -> {ep1}", inc1 == inc0 + 1 and ep1 == ep0 + 1)
        check(f"the recorded OID is the live table's ({rec1} == {live1} == {current}), not the stale {unseen}", rec1 == live1 == current)
        text = open(s.log(b1), errors="replace").read()
        check("the producer built nothing from the stale OID: it said so and asked for a fresh pass",
              "is no longer the table's" in text)
        # The chains now standing: rebuilt ONCE for the live incarnation (from the chain
        # built before the stale read), from the live OID alone — then kept by the next tick.
        t_r = time.monotonic()
        while time.monotonic() - t_r < a.cadence + 60:
            if all([((await js.manifest(t)) or {}).get("incarnation") == inc1 for t in rows]): break
            await asyncio.sleep(1)
        await asyncio.sleep(5)
        before, mark = s.chain_state(), len(open(s.log(b1), errors="replace").read())
        check(f"every pair's live bookkeeping records the live OID {current} only: {before}",
              set(before) == set(rows) and all(oids == {current} for _, oids in before.values()))
        await asyncio.sleep(a.cadence + 30)   # at least one more tick
        after = s.chain_state()
        later = open(s.log(b1), errors="replace").read()[mark:]
        swept = [l for l in later.splitlines() if "rr_items" in l and ("re-created under its name" in l or "built under incarnation" in l or "starting at g1" in l)]
        check(f"the following tick keeps the chains: no sweep ({len(swept)}), no generation restart ({before} -> {after})",
              not swept and all(after.get(t, (0, set()))[0] >= g for t, (g, _) in before.items()))
        late = Client(s, "late", "pa", "t_a"); late.start()
        g = grade(s, [late], a.cadence + 90)
        await report(late, g["late"], js, {"t_a"})
        await js.close()
    finally:
        s.stop()


async def check8(a, tmp):
    """The DROP cleanup reads the old manifest, the replacement is published, the cleanup
    resumes: the replacement's manifest and objects survive."""
    if not fault_bridge_ready(): return
    rows = {"t_a": a.rows // 10, "t_b": a.rows // 10}
    # The hold blocks the WAL thread — the CREATE, and the kick it would give, wait behind
    # it — so the replacement can only come from the producer's own tick, on its own
    # thread: the race itself. A cadence well inside the hold puts a tick there.
    cadence, hold_s = 20, 35
    s = Stack(tmp, "8", cadence, {"ZB_TEST_PRUNE_SLEEP_MS": str(hold_s * 1000)})
    s.bridge_bin = FAULT_BRIDGE
    try:
        s.start({"pa": "t_a", "pb": "t_b"})
        s.create_table(rows, "a")
        b1 = s.bridge_up()
        js = await Js().open(s)
        check("tenant mappings published", await js.tenants_ready(s.creds))
        check(f"chains ({await wait_manifests(js, rows, cadence + 60)} s)", all([await js.manifest(t) for t in rows]))
        inc0 = s.incarnation()
        s.drop_table()
        t0 = time.monotonic()
        while time.monotonic() - t0 < 30 and "prune of 'rr_items' held" not in open(s.log(b1), errors="replace").read():
            await asyncio.sleep(0.2)
        check(f"the cleanup read the old manifest and holds ({round(time.monotonic() - t0, 1)} s)",
              "prune of 'rr_items' held" in open(s.log(b1), errors="replace").read())
        s.create_table(rows, "b")    # the DDL path: a new incarnation, a prompt chain
        inc1 = s.incarnation()
        published = {}
        t1 = time.monotonic()
        while time.monotonic() - t1 < hold_s - 3 and len(published) < len(rows):
            for t in rows:
                m = await js.manifest(t)
                if m and m.get("incarnation") == inc1: published[t] = round(time.monotonic() - t1, 1)
            await asyncio.sleep(0.5)
        check(f"the replacement chains were published while the cleanup held: {published} (incarnation {inc0} -> {inc1})", len(published) == len(rows))
        t2 = time.monotonic()
        while time.monotonic() - t2 < hold_s + 20 and "drop prune for 'rr_items'" not in open(s.log(b1), errors="replace").read():
            await asyncio.sleep(0.5)
        line = [l for l in open(s.log(b1), errors="replace").read().splitlines() if "drop prune for 'rr_items'" in l]
        note(f"cleanup: {line[-1][line[-1].index('drop prune'):][:160] if line else 'no line'}")
        for t in rows:
            m = await js.manifest(t) or {}
            names = [m.get("full", {}).get("object")] + [d.get("object") for d in m.get("deltas", [])]
            store = await js.js.object_store(f"gen-{t}")
            present = []
            for n in names:
                if not n: continue
                try:
                    await store.get_info(n); present.append(n)
                except Exception:
                    pass
            check(f"pair {t}: after the cleanup, the manifest is incarnation {m.get('incarnation')} (current {inc1}) and its {len([n for n in names if n])} "
                  f"object(s) exist ({len(present)})", m.get("incarnation") == inc1 and len(present) == len([n for n in names if n]) and present)
        late = Client(s, "late", "pa", "t_a"); late.start()
        g = grade(s, [late], 120)
        await report(late, g["late"], js, {"t_a"})
        await js.close()
    finally:
        s.stop()


async def check9(a, tmp):
    """The boot cleanup barrier: a bridge boots with a table gone, reads its absence, and
    holds before reading the manifests; the table is re-created and the running producer
    publishes its chain; the cleanup resumes — the new chain stays."""
    if not fault_bridge_ready(): return
    rows = {"t_a": a.rows // 10, "t_b": a.rows // 10}
    cadence, hold_s = 20, 35   # a producer tick inside the hold: the only publisher there
    s = Stack(tmp, "9", cadence, {})
    s.bridge_bin = FAULT_BRIDGE
    try:
        s.start({"pa": "t_a", "pb": "t_b"})
        s.create_table(rows, "a")
        b1 = s.bridge_up()
        js = await Js().open(s)
        check("tenant mappings published", await js.tenants_ready(s.creds))
        check(f"chains ({await wait_manifests(js, rows, cadence + 60)} s)", all([await js.manifest(t) for t in rows]))
        s.bridge_down(b1)
        s.drop_table()   # while no bridge runs: the next one's boot finds the schema key alone
        inc0 = s.incarnation()
        s.env["ZB_TEST_BOOT_PRUNE_SLEEP_MS"] = str(hold_s * 1000)
        b2 = s.bridge_up(ready="boot cleanup of 'rr_items' held")
        del s.env["ZB_TEST_BOOT_PRUNE_SLEEP_MS"]
        check("the boot cleanup read the table's absence and holds before its manifests", True)
        s.create_table(rows, "b")
        inc1 = s.incarnation()
        published, t1 = {}, time.monotonic()
        while time.monotonic() - t1 < hold_s - 3 and len(published) < len(rows):
            for t in rows:
                m = await js.manifest(t)
                if m and m.get("incarnation") == inc1: published[t] = round(time.monotonic() - t1, 1)
            await asyncio.sleep(0.5)
        check(f"the replacement chains were published while the boot cleanup held: {published} (incarnation {inc0} -> {inc1})",
              len(published) == len(rows))
        s.wait_log(b2, "drop prune for 'rr_items'", hold_s + 30)
        line = [l for l in open(s.log(b2), errors="replace").read().splitlines() if "drop prune for 'rr_items'" in l]
        note(f"boot cleanup: {line[-1][line[-1].index('drop prune'):][:170] if line else 'no line'}")
        for t in rows:
            m = await js.manifest(t) or {}
            names = [n for n in [m.get("full", {}).get("object")] + [d.get("object") for d in m.get("deltas", [])] if n]
            store = await js.js.object_store(f"gen-{t}")
            present = []
            for n in names:
                try:
                    await store.get_info(n); present.append(n)
                except Exception:
                    pass
            check(f"pair {t}: after the boot cleanup, the manifest is incarnation {m.get('incarnation')} (current {inc1}) and its "
                  f"{len(names)} object(s) exist ({len(present)})", m.get("incarnation") == inc1 and names and len(present) == len(names))
        s.wait_log(b2, "Replication started successfully", 60)
        late = Client(s, "late", "pa", "t_a"); late.start()
        g = grade(s, [late], 120)
        await report(late, g["late"], js, {"t_a"})
        await js.close()
    finally:
        s.stop()


async def check10(a, tmp):
    """A replacement behind a catalogue removal reaches a replica that missed it all."""
    s = Stack(tmp, "10", a.cadence, {})
    try:
        s.start({"pa": "t_a"})
        # One ordinary recreate first: the replica then holds epoch 1, the value a catalogue
        # row inserted afresh comes back to once its re-enable moves it (0 -> 1).
        s.create_table({"t_a": a.rows}, "z")
        s.drop_table(); s.create_table({"t_a": a.rows}, "a")
        s.bridge_up()
        js = await Js().open(s)
        check("tenant mapping published", await js.tenants_ready(s.creds))
        check(f"a chain ({await wait_manifests(js, ['t_a'], a.cadence + 60)} s), epoch {s.recorded()[1]}", bool(await js.manifest("t_a")))
        old = Client(s, "old", "pa", "t_a"); old.start()
        g = grade(s, [old], 120)
        check(f"the replica is exact on the first table ({g['old']['rows']} rows, {g['old']['s']} s)", g["old"]["exact"])
        old.stop()
        held = {r["seed_epoch"] for r in old.anchors()}
        inc0, ep0, _, _ = s.recorded()
        s.sql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}';")
        s.drop_table()
        await asyncio.sleep(3)
        s.create_table({"t_a": 0}, "b")   # enabled again, and empty: nothing on the CDC stream
        inc1, ep1, _, _ = s.recorded()
        check(f"the replacement is a new incarnation ({inc0} -> {inc1}) and the epoch moves above the replica's "
              f"{sorted(held)}: {ep0} -> {ep1}", inc1 > inc0 and all(ep1 > e for e in held))
        t0 = time.monotonic()
        while time.monotonic() - t0 < a.cadence + 60:
            m = await js.manifest("t_a") or {}
            if m.get("incarnation") == inc1 and m.get("seed_epoch") == ep1 and await js.epoch() == ep1: break
            await asyncio.sleep(1)
        check(f"the descriptor and the chain carry the new epoch ({round(time.monotonic() - t0, 1)} s)",
              await js.epoch() == ep1 and (await js.manifest("t_a") or {}).get("seed_epoch") == ep1)
        old.start()
        g = grade(s, [old], a.cadence + 60)
        await report(old, g["old"], js, {"t_a"})
        # The epoch is what told it, not a stream gap or the chain's plan: its own line.
        moved = [f"seed epoch {e} -> {ep1}" for e in held]
        check(f"the replica re-seeded on the epoch move ({', '.join(moved)})", all(m in old.text() for m in moved))
        # The same table catalogued again (no recreate) keeps its epoch: no re-seed for it.
        old.stop()
        s.sql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}';"
              f"SELECT count(*) FROM zebridge_enable('public.{T}'::regclass, writable => true, tenant_col => 'tenant_id', "
              f"version_col => 'updated_at', tombstone_col => 'deleted_at', publication => 'rr_pub', dry_run => false);")
        inc2, ep2, _, _ = s.recorded()
        check(f"the same table catalogued again keeps its identity: incarnation {inc1} -> {inc2}, epoch {ep1} -> {ep2}",
              (inc2, ep2) == (inc1, ep1))
        await js.close()
    finally:
        s.stop()


async def check11(a, tmp):
    """Observation SQL failures cannot authorize a sweep or publication."""
    s = Stack(tmp, "11", 20, {})
    try:
        s.start({"pa": "t_a"})
        s.create_table({"t_a": a.rows // 10}, "a")
        b = s.bridge_up()
        js = await Js().open(s)
        check("tenant mapping published", await js.tenants_ready(s.creds))
        check("initial chain exists", await wait_manifests(js, ["t_a"], 80) is not None)
        before = await js.manifest("t_a")
        state = s.chain_state()
        store = await js.js.object_store("gen-t_a")

        async def objects():
            try:
                return sorted(o.name for o in await store.list())
            except Exception:
                return []
        objs, ep0 = await objects(), s.recorded()[1]
        # A real SQL error from the observation entry point, on a disposable database.
        # The wrapper retains the production function and its role boundaries.
        s.sql("""
            CREATE TABLE zebridge_private.observation_fault (code text NOT NULL);
            INSERT INTO zebridge_private.observation_fault VALUES ('40001');
            ALTER FUNCTION public.zebridge_observe_incarnation(text, oid) RENAME TO im_observe_real;
            CREATE FUNCTION public.zebridge_observe_incarnation(p_tbl text, p_relid oid)
            RETURNS TABLE (incarnation bigint, status text) LANGUAGE plpgsql SECURITY DEFINER
            SET search_path = pg_catalog, public AS $$
            DECLARE fault text;
            BEGIN
                SELECT code INTO fault FROM zebridge_private.observation_fault;
                RAISE EXCEPTION 'observation regression fault' USING ERRCODE = fault;
            END; $$;
            REVOKE ALL ON FUNCTION public.zebridge_observe_incarnation(text, oid) FROM PUBLIC;
        """ + f"GRANT EXECUTE ON FUNCTION public.zebridge_observe_incarnation(text, oid) TO {s.name}_r, {s.name}_w;")
        s.sql("ALTER EVENT TRIGGER zebridge_ddl_trigger DISABLE; ALTER EVENT TRIGGER zebridge_drop_trigger DISABLE;")
        s.drop_table(); s.create_table_raw({"t_a": a.rows // 10}, "b")
        s.sql("ALTER EVENT TRIGGER zebridge_ddl_trigger ENABLE; ALTER EVENT TRIGGER zebridge_drop_trigger ENABLE;")
        # No DDL observation has run: only the producer can discover the replacement.
        s.wait_log(b, "IncarnationObservationRetryable", timeout=80)
        text = pathlib.Path(s.log(b)).read_text(errors="replace")
        check("serialization failure retried twice, then stopped", text.count("SQLSTATE 40001") == 3
              and text.count("retrying incarnation observation") == 2)
        check("failed observation leaves manifest, bookkeeping, objects and epoch unchanged",
              await js.manifest("t_a") == before and s.chain_state() == state and await objects() == objs
              and s.recorded()[1] == ep0)
        check("failed observation never sweeps or starts a replacement chain",
              "sweeping this pair's chain" not in text)
        s.sql("UPDATE zebridge_private.observation_fault SET code = '42501';")
        s.wait_log(b, "SQLSTATE 42501", timeout=60)
        await asyncio.sleep(1)
        text = pathlib.Path(s.log(b)).read_text(errors="replace")
        check("permanent failure is logged once, without immediate retry", text.count("SQLSTATE 42501") == 1)
        check("permanent failure also preserves the existing chain, its objects and the epoch",
              await js.manifest("t_a") == before and s.chain_state() == state and await objects() == objs
              and s.recorded()[1] == ep0)
        s.sql("DROP FUNCTION public.zebridge_observe_incarnation(text, oid); "
              "ALTER FUNCTION public.im_observe_real(text, oid) RENAME TO zebridge_observe_incarnation;")
        deadline = time.monotonic() + 80
        inc0 = before["incarnation"]
        while time.monotonic() < deadline:
            m = await js.manifest("t_a") or {}
            if m.get("incarnation", 0) > inc0:
                break
            await asyncio.sleep(0.5)
        check("a later successful observation publishes the new incarnation", m.get("incarnation", 0) == inc0 + 1)
        ep1 = s.recorded()[1]
        check(f"the epoch moved once, for the replacement only ({ep0} -> {ep1}), and the chain carries it",
              ep1 == ep0 + 1 and m.get("seed_epoch") == ep1)
        late = Client(s, "late", "pa", "t_a"); late.start()
        g = grade(s, [late], 90)
        await report(late, g["late"], js, {"t_a"})
        await js.close()
    finally:
        s.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--worker")
    ap.add_argument("--lib")
    ap.add_argument("--check", default="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18")
    ap.add_argument("--cadence", type=int, default=120)
    ap.add_argument("--max-msgs", type=int, default=1000, help="CDC_MAX_MSGS for checks 2 and 3 (the bridge's minimum is 1000)")
    ap.add_argument("--rows", type=int, default=2000)
    ap.add_argument("--interval-ms", type=int, default=100)
    ap.add_argument("--first-chain-s", type=int, default=20)
    ap.add_argument("--recover-s", type=int, default=60, help="check 3: recovery while the load still runs")
    a = ap.parse_args()
    if a.worker: return worker(a.worker, a.lib)
    signal.signal(signal.SIGTERM, signal.default_int_handler)   # torn down like an interrupt
    with tempfile.TemporaryDirectory(prefix="zb-rr-") as d:
        for n in (int(x) for x in a.check.split(",")):
            print(f"── check {n}", flush=True)
            try:
                asyncio.run({1: check1, 2: check2, 3: check3, 4: check4,
                             5: lambda a_, d_: check1(a_, d_, "restart"), 6: lambda a_, d_: check1(a_, d_, "backstop"),
                             7: check7, 8: check8, 9: check9, 10: check10, 11: check11,
                             12: lambda a_, d_: check4(a_, d_, ts=True), 13: check13,
                             14: lambda a_, d_: check13(a_, d_, ts=True), 15: check15,
                             16: lambda a_, d_: check15(a_, d_, ts=True),
                             17: check17, 18: lambda a_, d_: check17(a_, d_, ts=True),
                             19: check19, 20: lambda a_, d_: check19(a_, d_, ts=True)}[n](a, pathlib.Path(d)))
            except Exception as e:
                check(f"check {n} raised: {type(e).__name__}: {str(e)[:400]}", False)
    print(f"  · logs: /tmp/zb_rr<check>_*.log; {'FAILED ' + str(failed) + ' check(s)' if failed else 'clean'}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main() or 0)
