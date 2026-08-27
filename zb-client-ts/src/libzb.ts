/// libzb.ts — the ZeBridge client core, extracted from App.tsx (NOTES.md §10).
///
/// "Theater around a subscription": App.tsx keeps the theater; this file keeps the
/// subscription — schema watch, chain-first seeding with the snapshot fallback, CDC
/// apply, the LWW write path (outbox, optimistic apply, verdicts, echo-confirm), and
/// the change doorbell. App.tsx is this module's FIRST consumer, so the browser demo
/// is the regression test for the extraction; a headless Node consumer is the second.
///
/// This is also the TypeScript ancestor of the sans-I/O wasm eater: transport (the
/// "speaker": this file's NATS calls) and decision logic (the "eater": plans, guards,
/// verdict transitions) are kept separable on purpose, so the later split is a
/// refactor, not a rewrite.
///
/// The consumer contract (§10, the index card):
///   query      — arbitrary SELECTs against the local replica; the replica IS the API
///   mutate     — three verbs, a 1:1 constructor for the wire message; no SQL parsed
///   onChange   — the doorbell; the app re-queries
///
/// Browser-tier write guard: this API exports no write path except `mutate()`. The raw
/// `sql` handle stays public for the SQL console and for people deliberately off the
/// path (one sqlocal connection — OPFS sync handles are exclusive, so a second
/// read-only connection is not available here the way `libzebridge` native has one).

import { wsconnect, headers, credsAuthenticator } from '@nats-io/nats-core';
import type { NatsConnection } from '@nats-io/nats-core';
import { jetstream, jetstreamManager, DeliverPolicy } from '@nats-io/jetstream';
import { Kvm } from '@nats-io/kv';
import { Objm } from '@nats-io/obj';
import { decode, encode } from '@msgpack/msgpack';
import type { Storage, StorageFactory, Exec as StorageExec } from './storage.ts';
import { browserStorage } from './browser-storage.ts';
import { v7 as uuidv7 } from 'uuid';

export interface ZeBridgeConfig {
  natsUrl: string;
  principal: string;
  password?: string;
  /// Operator/JWT mode: the CONTENT of a .creds file (user JWT + nkey seed).
  /// When set it wins over user/password — the JWT carries the permissions
  /// (scoped signing key), so no server conf names this principal at all.
  creds?: string;
  grammar: any;   // the parsed grammar.json — wire names only; the consumer imports and passes it
  durable?: boolean;
  /// The two seams (NOTES §10). Defaults are the browser: sqlocal/OPFS storage
  /// and a NATS WebSocket dial. A Node host injects better-sqlite3 + TCP
  /// (`zb-client-ts/node`); any other host brings its own pair.
  storage?: StorageFactory;
  connect?: (opts: any) => Promise<NatsConnection>;
}

export type TableState = {
  pkCols: string[];
  columns: string[];
  tombstoneColumn: string | null;
  tenantColumn: string | null;
  lsn: number;
  /// The seed gate's PRIMARY anchor (finding 7, NOTES §10i): the CDC stream's
  /// last_seq captured by the producer AT CHAIN BUILD TIME, and which stream it
  /// belongs to. Stream sequence is commit-ordered and monotonic — lsn is NOT
  /// (a transaction that begins early and commits late delivers late with a
  /// LOWER lsn), so gating on lsn silently dropped in-flight transactions.
  seedSeq?: number;
  seedStream?: string;
};

export type Phase = 'connected' | 'migrated' | 'snapshot' | 'cdc';
export type ConnStatus = 'connected' | 'disconnected' | 'connecting';

interface BucketEntry {
  key: string;
  operation: 'PUT' | 'DEL' | 'PURGE';
  value: Uint8Array;
  delta: number;
}

/// How long to wait for a snapshot descriptor before re-requesting. Must exceed a
/// realistic snapshot for the largest table, and stay well below the REQUESTS stream's
/// max_age (retrying at the last moment is the same as not retrying).
const SNAPSHOT_WAIT_MS = 60_000;
const SNAPSHOT_REQUEST_ATTEMPTS = 5;

/// ⚠️ RETIRED 2026-08-27 — snapshot-on-demand seeding (NOTES §10g/§10h; README
/// roadmap "Retire the snapshot path"). Seeding is cron-based generations now:
/// the producer queries PG on GENERATION_CADENCE_SECONDS and pushes chains to
/// object storage; a client pulls the chain and never asks PG-adjacent machinery
/// for a bespoke dump. The legacy path is GATED, not erased, because it earned
/// three findings in one day — a stale descriptor that DELETEd 5,000 correct rows,
/// the SNAP_RET throttle deadlocking an orphaned table for its whole 7-day window,
/// and 5x60s of head-of-line blocking — and the code is the documentation of them.
/// Flip to true only to study that behaviour; never in production.
const LEGACY_SNAPSHOT_SEEDING: boolean = false;
/// How long a client waits for the producer to publish a usable chain before
/// declaring the table unseedable. Covers a fresh table between two cadence ticks;
/// a table still chainless after this fails LOUDLY and seeds on the next connect.
const GENERATION_WAIT_MS = 90_000;
const GENERATION_POLL_MS = 10_000;
/// Derived from bridge-side max_deliver × retry sleep + batch window + slack; see the
/// verdict-timeout discussion in PROTOCOL §7 — a guess kept in step by hand until it
/// rides in the schema descriptor.
const WRITE_TIMEOUT_MS = 10_000;

/// Watch a KV bucket through a plain pull consumer instead of `kv.watch()` — the
/// ordered-push consumer `kv.watch()` uses leaks a consumer per reset under this
/// server's grant set (NOTES.md §1.14). Same semantics the callers rely on:
/// LastPerSubject replays current values first; `delta === 0` marks the replay's end.
async function watchBucket(
  js: any,
  bucket: string,
  filterKey: string = '>',
): Promise<{ pending: number; entries: AsyncIterable<BucketEntry>; stop: () => void }> {
  const stream = `KV_${bucket}`;
  const prefix = `$KV.${bucket}.`;
  const jsm = await js.jetstreamManager();
  const ci = await jsm.consumers.add(stream, {
    deliver_policy: DeliverPolicy.LastPerSubject,
    filter_subject: `${prefix}${filterKey}`,
    ack_policy: 'none',
  });
  const consumer = await js.consumers.get(stream, ci.name);
  const iter = await consumer.consume();
  const entries = (async function* () {
    for await (const m of iter) {
      const op = m.headers?.get('KV-Operation') || 'PUT';
      yield {
        key: m.subject.substring(prefix.length),
        operation: (op === 'DEL' || op === 'PURGE' ? op : 'PUT') as BucketEntry['operation'],
        value: m.data,
        delta: m.info.pending,
      };
    }
  })();
  return { pending: ci.num_pending ?? 0, entries, stop: () => { try { iter.stop(); } catch { /* closed */ } } };
}

/// Wait for one KV value (snapshot descriptor), or null on timeout — "not yet" is an
/// ordinary outcome here, not an exception. The watch is always torn down.
/// `rejectId` is the descriptor this caller has ALREADY refused.
///
/// ⚠️ Without it this function hands back the very descriptor the caller just
/// rejected. A KV watch replays the current value first, so the sequence
/// "judge the cached descriptor orphaned -> request a fresh one -> wait for a
/// descriptor" resolves instantly with the SAME stale one — and replaying it
/// starts with DELETE FROM. Measured: a table holding 5,000 correct rows was
/// emptied that way (NOTES §10g). Skipping the rejected snapshot_id is what makes
/// "requesting a fresh one instead" mean it.
async function waitForDescriptor(js: any, bucket: string, key: string, timeoutMs: number, rejectId?: string): Promise<any | null> {
  let watch: Awaited<ReturnType<typeof watchBucket>> | null = null;
  try {
    watch = await watchBucket(js, bucket, key);
    return await Promise.race([
      (async () => {
        for await (const entry of watch.entries) {
          if (entry.operation === 'DEL' || entry.operation === 'PURGE') continue;
          let d: any;
          try { d = decode(entry.value); } catch { d = JSON.parse(new TextDecoder().decode(entry.value)); }
          // Keep waiting: this is the one we already refused.
          if (rejectId && d?.snapshot_id === rejectId) continue;
          return d;
        }
        return null;
      })(),
      new Promise<null>((resolve) => setTimeout(() => resolve(null), timeoutMs)),
    ]);
  } catch {
    return null;
  } finally {
    watch?.stop();
  }
}

/// PG text-mode timestamptz (UTC — the producer pins its snapshot txn there) → the CDC
/// wire shape. String surgery, microseconds preserved (`Date` would truncate to ms).
/// The version guard compares AS STRINGS: `' '` sorts before `'T'`, so unnormalized
/// chain values would lose every comparison against CDC-written ones (NOTES.md §1.13).
const pgTsToWire = (v: any): any =>
  typeof v === 'string' && /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?\+00(:00)?$/.test(v)
    ? v.replace(' ', 'T').replace(/\+00(:00)?$/, 'Z')
    : v;

/// `pg_lsn` text (`0/C5793FD0`) → the numeric WAL position CDC events carry.
const lsnToNumber = (lsn: string): number => {
  const [hi, lo] = String(lsn).split('/');
  return parseInt(hi, 16) * 0x100000000 + parseInt(lo, 16);
};

const td = new TextDecoder();
type Exec = (q: string, ...params: any[]) => Promise<any[]>;

/// Assemble a .creds file's text from a JWT and a seed — what the enrollment
/// flow holds after the mint responds (the seed never crossed the wire; the app
/// generated the pair itself).
/// Is this failure "the parent is not here YET" rather than "this row is wrong"?
///
/// THREE distinct messages, and only the first one says what you would expect —
/// all measured against SQLite:
///
///   FOREIGN KEY constraint failed        the parent ROW is missing
///   no such table: main.<parent>         the parent TABLE has not been created yet
///                                        (FK resolution is lazy at DDL, strict at
///                                        DML, so a child can exist before its
///                                        parent when schemas arrive out of order)
///   foreign key mismatch - "c" ref "p"   the parent KEY is not the PK and has no
///                                        UNIQUE index — a schema-porting fault,
///                                        NOT a missing parent
///
/// The first two are worth holding and retrying: a later batch supplies what is
/// missing. The third never resolves by waiting — the parent key will not become
/// unique — so it is matched here only to be reported distinctly rather than
/// retried forever. Matching messages is unavoidable: sqlocal and better-sqlite3
/// surface different error CLASSES for the same fault.
function foreignKeyFailureKind(e: unknown): 'missing-parent' | 'mismatch' | null {
  const m = String((e as any)?.message ?? e);
  if (/foreign key mismatch/i.test(m)) return 'mismatch';
  if (/FOREIGN KEY constraint failed/i.test(m)) return 'missing-parent';
  if (/no such table: /i.test(m)) return 'missing-parent';
  return null;
}

export function credsFileText(jwt: string, seed: string): string {
  return `-----BEGIN NATS USER JWT-----\n${jwt}\n------END NATS USER JWT------\n\n` +
         `-----BEGIN USER NKEY SEED-----\n${seed}\n------END USER NKEY SEED------\n`;
}

/// The JWT's own name claim — the creds are AUTHORITATIVE for the principal:
/// the payload is plain base64 (only the signature is cryptographic), and the
/// server expands permissions from THIS name, never from what the app believes.
export function principalFromCreds(creds: string): string | null {
  const m = creds.match(/BEGIN NATS USER JWT-+\s*\n([^\n]+)/);
  if (!m) return null;
  try {
    const payload = m[1].split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
    return JSON.parse(atob(payload)).name ?? null;
  } catch { return null; }
}

export class ZeBridge {
  public readonly dbName: string;
  public sql: StorageExec;
  public transaction: Storage['transaction'];
  public deleteDatabaseFile: Storage['deleteDatabaseFile'];

  private nc: NatsConnection | null = null;
  private storage: Storage;

  private syncedTables = new Map<string, TableState>();
  private failed = new Set<string>();
  private suspendedMap = new Map<string, string>();
  private globalSyncState: { lsn: number; seq: Record<string, number> } = { lsn: 0, seq: {} };
  private pendingEvents: { table: string; ev: any }[] = [];
  private pendingWrites = new Map<string, { table: string; id: number | string; at: number }>();

  private tenantValue = '';
  /// Per instance, like the replica (see App.tsx's CLIENT_ID history): the LWW
  /// tiebreaker and msg-id prefix. Must move INTO the replica when durable outboxes
  /// need identity to survive a reload (NOTES.md §1.9).
  private clientIdValue = `c-${crypto.randomUUID().slice(0, 8)}`;
  private lastVersion = '';

  private outboxInitPromise: Promise<void>;
  private resolveOutboxInit!: () => void;
  private resyncing = false;
  private naturallyConnected = true;

  private tableListeners: Record<string, Set<(ev?: any) => void>> = {};
  private eventListeners = new Set<(table: string, ev: any) => void>();
  private anyChangeListeners = new Set<() => void>();
  private logHandlers = new Set<(topic: string, data: any, level: string) => void>();
  private phaseHandlers = new Set<(p: Phase) => void>();
  private suspendedHandlers = new Set<(table: string, reason: string | null) => void>();
  private statusHandlers = new Set<(s: ConnStatus) => void>();

  private fkHeld: { id?: number; table: string; ev: any }[] = [];
  private sweepId?: ReturnType<typeof setInterval>;
  private rttIntervalId?: ReturnType<typeof setInterval>;
  private recountTimer?: ReturnType<typeof setTimeout>;

  private config: ZeBridgeConfig;

  constructor(config: ZeBridgeConfig) {
    this.config = config;
    // The creds win over the passed principal — kills the mismatch class where
    // config says bob but the JWT says omar (every publish would just bounce).
    if (config.creds) {
      const fromJwt = principalFromCreds(config.creds);
      if (fromJwt && fromJwt !== config.principal) config.principal = fromJwt;
    }
    // A fresh OPFS file per load by default — the project's own clean-room dev
    // convention; `durable` opts into the stable per-principal name that makes the
    // outbox meaningful across reloads.
    this.dbName = config.durable
      ? `zebridge_${config.principal}.sqlite3`
      : `zebridge_${Date.now()}.sqlite3`;
    this.storage = (config.storage ?? browserStorage)(this.dbName);
    this.sql = this.storage.exec;
    this.transaction = (fn) => this.storage.transaction(fn);
    this.deleteDatabaseFile = () => this.storage.deleteDatabaseFile();
    this.outboxInitPromise = new Promise((resolve) => { this.resolveOutboxInit = resolve; });
  }

  // ─── the index card ───────────────────────────────────────────────────────

  /// Arbitrary SQL against the local replica. Reads are the intended use; a write
  /// here is local theater the feed will overwrite — real writes go through mutate().
  public async query(sqlText: string, ...params: any[]): Promise<any[]> {
    return this.run(sqlText, ...params);
  }

  /// The blessed write path: a 1:1 constructor for the wire message
  /// `mutation.<principal>.<table>.<verb>` — no SQL is ever parsed. Returns the
  /// version stamped on the write so callers can mirror it into value columns.
  public async mutate(
    table: string,
    op: 'INSERT' | 'UPDATE' | 'DELETE',
    key: Record<string, unknown>,
    values?: Record<string, unknown>,
    opts?: { version?: string },
  ): Promise<{ version: string }> {
    const state = this.syncedTables.get(table);
    if (!state || !state.pkCols.length) throw new Error(`table ${table} is not synced or has no primary key`);
    const version = opts?.version ?? this.newVersion();
    const id = state.pkCols.map((c) => key[c]).join('|');
    const payload: Record<string, unknown> = { key, version, client_id: this.clientIdValue };
    if (op !== 'DELETE') payload.data = values ?? {};
    await this.rawMutation(table, op, id, version, payload);
    return { version };
  }

  public onChange(table: string, cb: (ev?: any) => void): () => void {
    if (!this.tableListeners[table]) this.tableListeners[table] = new Set();
    this.tableListeners[table].add(cb);
    return () => this.tableListeners[table].delete(cb);
  }

  // ─── the rest of the public surface ──────────────────────────────────────

  /// Every applied event, with its table — the hook for verb badges and per-table
  /// console logging. High-volume: keep handlers cheap.
  public onTableEvent(cb: (table: string, ev: any) => void): () => void {
    this.eventListeners.add(cb);
    return () => this.eventListeners.delete(cb);
  }

  /// Debounced "something changed somewhere" (250ms) — the recount trigger.
  public onAnyChange(cb: () => void): () => void {
    this.anyChangeListeners.add(cb);
    return () => this.anyChangeListeners.delete(cb);
  }

  public onLog(cb: (topic: string, data: any, level: string) => void): () => void {
    this.logHandlers.add(cb);
    return () => this.logHandlers.delete(cb);
  }

  public onPhase(cb: (p: Phase) => void): () => void {
    this.phaseHandlers.add(cb);
    return () => this.phaseHandlers.delete(cb);
  }

  public onSuspended(cb: (table: string, reason: string | null) => void): () => void {
    this.suspendedHandlers.add(cb);
    return () => this.suspendedHandlers.delete(cb);
  }

  public onStatus(cb: (s: ConnStatus) => void): () => void {
    this.statusHandlers.add(cb);
    return () => this.statusHandlers.delete(cb);
  }

  public uuid(): string { return uuidv7(); }
  public get tenant(): string { return this.tenantValue; }
  public get clientId(): string { return this.clientIdValue; }
  /// Events held back waiting for a schema newer than they are.
  public get heldCount(): number { return this.pendingEvents.length; }
  /// Events held waiting for a PARENT ROW — a foreign key whose target has not
  /// arrived yet, which happens when one PostgreSQL transaction is split across
  /// batches. Non-zero for long is a real signal: the parent never came.
  public get fkHeldCount(): number { return this.fkHeld.length; }
  /// The durable hold queue, oldest first — what is waiting and for how long. A row
  /// with a high `attempts` is a parent that is never coming, which is a real
  /// condition worth seeing rather than expiring away.
  public async inbox(): Promise<any[]> {
    return this.run(`SELECT id, tbl, lsn, reason, held_at, attempts FROM _zebridge_inbox ORDER BY id`);
  }
  public tableNames(): string[] { return [...this.syncedTables.keys()]; }
  public tableState(table: string): TableState | undefined { return this.syncedTables.get(table); }
  public suspendedTables(): Record<string, string> { return Object.fromEntries(this.suspendedMap); }

  /// Debug snapshot: stored positions and per-table lsn — `zb.state()` in the console.
  public syncState() {
    return {
      global: { ...this.globalSyncState },
      tables: Object.fromEntries([...this.syncedTables].map(([t, v]) => [t, { lsn: v.lsn, columns: v.columns.length }])),
      failed: [...this.failed],
    };
  }

  /// Render "now" as a version value — the exact shape CDC echoes back for a
  /// timestamptz column (ISO, six digits, trailing Z), strictly increasing within
  /// this instance so same-millisecond edits never tie against themselves.
  public newVersion(): string {
    let candidate = new Date().toISOString().replace('Z', '') + '000Z';
    if (candidate <= this.lastVersion) {
      const micros = (parseInt(this.lastVersion.slice(-7, -1), 10) + 1) % 1000000;
      candidate = `${this.lastVersion.slice(0, -7)}${String(micros).padStart(6, '0')}Z`;
    }
    this.lastVersion = candidate;
    return candidate;
  }

  // ─── lifecycle ────────────────────────────────────────────────────────────

  public async connect(): Promise<void> {
    await this.initSyncState();

    if (this.nc) {
      try { await this.nc.close(); } catch { /* already closed */ }
      this.nc = null;
    }

    try {
      this.emitStatus('connecting');
      this.appendLog('SYS', `Connecting to NATS at ${this.config.natsUrl}...`);

      const dial = this.config.connect ?? wsconnect;
      this.nc = await dial({
        servers: this.config.natsUrl,
        ...(this.config.creds
          ? { authenticator: credsAuthenticator(new TextEncoder().encode(this.config.creds)) }
          : { user: this.config.principal, pass: this.config.password }),
        reconnect: true,
        maxReconnectAttempts: -1,
      });

      this.emitStatus('connected');
      this.reach('connected');
      this.appendLog('SYS', `Connected to NATS as '${this.config.principal}'`);
      await this.loadHeldEvents();
      await this.watchSchemas();
      await this.watchVerdicts();
      await this.resolveTenant();
      await this.subscribeStreams();
      this.reach('cdc');

      // Anything queued while disconnected goes out now — including writes made in a
      // previous page load, which is the whole point of persisting them.
      void this.flushOutbox();

      // `reconnect: true` restores the connection without coming back through this
      // function — the status loop is what re-syncs after a blip. Safe to re-run
      // subscribeStreams: the gap check compares persisted positions, so a normal
      // reconnect just resumes CDC (upserts idempotent even if consumers double up).
      void (async () => {
        try {
          for await (const st of this.nc!.status()) {
            const t = String((st as any).type);
            if (t === 'reconnect') {
              this.emitStatus('connected');
              if (this.resyncing) continue;
              this.resyncing = true;
              this.appendLog('SYS', 'NATS reconnected — flushing outbox and re-syncing streams', 'INFO');
              void this.flushOutbox();
              void this.subscribeStreams().finally(() => { this.resyncing = false; });
            } else if (t === 'disconnect') {
              this.emitStatus('disconnected');
            }
          }
        } catch { /* connection closed; nothing to watch */ }
      })();

      this.sweepId = setInterval(() => this.sweepPendingWrites(), 1000);
      this.rttIntervalId = setInterval(() => void this.pollNatsRtt(), 10_000);
    } catch (err) {
      this.emitStatus('disconnected');
      this.appendLog('SYS', `Connection failed: ${err}`, 'ERROR');
      throw err;
    }
  }

  public async close(): Promise<void> {
    clearInterval(this.sweepId);
    clearInterval(this.rttIntervalId);
    clearTimeout(this.recountTimer);
    if (this.nc) {
      await this.nc.close();
      this.nc = null;
    }
  }

  // ─── internals ────────────────────────────────────────────────────────────

  private run: Exec = (q, ...params) => this.sql(q, ...params);

  private appendLog(topic: string, data: any, level = 'INFO') {
    for (const cb of this.logHandlers) cb(topic, data, level);
  }

  private reach(p: Phase) {
    for (const cb of this.phaseHandlers) cb(p);
  }

  private emitStatus(s: ConnStatus) {
    for (const cb of this.statusHandlers) cb(s);
  }

  private scheduleRecount() {
    clearTimeout(this.recountTimer);
    this.recountTimer = setTimeout(() => {
      for (const cb of this.anyChangeListeners) cb();
    }, 250);
  }

  private triggerChange(table: string, ev?: any) {
    // Per-event hooks (verb badges, per-table logging) only fire WITH an event —
    // a revert or seed notifies "something changed" without one, and handlers must
    // not be handed undefined (measured: markVerb crashed on ev.operation).
    if (ev !== undefined) {
      for (const cb of this.eventListeners) cb(table, ev);
    }
    if (this.tableListeners[table]) {
      for (const cb of this.tableListeners[table]) cb(ev);
    }
    this.scheduleRecount();
  }

  private async initSyncState() {
    await this.run(`
      CREATE TABLE IF NOT EXISTS _zebridge_sync (
        id INTEGER PRIMARY KEY,
        global_last_lsn INTEGER,
        global_last_seq INTEGER
      );
    `);
    // One row per stream: JetStream sequences are per stream, and a single global
    // number would corrupt both (resuming one stream from the other's position).
    await this.run(`
      CREATE TABLE IF NOT EXISTS _zebridge_stream_seq (
        stream TEXT PRIMARY KEY,
        last_seq INTEGER NOT NULL
      );
    `);
    // Generation watermarks (NOTES.md §1.13): the client tracks WATERMARKS, never gen
    // numbers — a gen is an object-naming detail; the cutoff is what deltas chain on.
    await this.run(`
      CREATE TABLE IF NOT EXISTS _zebridge_generations (
        tbl TEXT PRIMARY KEY,
        watermark TEXT NOT NULL,
        cutoff_lsn INTEGER NOT NULL
      );
    `);
    await this.createOutboxTable();
    this.resolveOutboxInit();
    await this.run(`INSERT OR IGNORE INTO _zebridge_sync (id, global_last_lsn, global_last_seq) VALUES (1, 0, 0)`);
    const res = await this.run(`SELECT global_last_lsn FROM _zebridge_sync WHERE id = 1`);
    if (res.length > 0) this.globalSyncState.lsn = res[0].global_last_lsn ?? 0;
    for (const r of await this.run(`SELECT stream, last_seq FROM _zebridge_stream_seq`)) {
      this.globalSyncState.seq[(r as any).stream] = (r as any).last_seq ?? 0;
    }
  }

  // ── outbox (PROTOCOL.md §7.1) ──────────────────────────────────────────────

  private async createOutboxTable() {
    await this.run(`
      CREATE TABLE IF NOT EXISTS _zebridge_inbox (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        tbl      TEXT    NOT NULL,
        lsn      INTEGER NOT NULL,
        ev       TEXT    NOT NULL,
        reason   TEXT    NOT NULL,
        held_at  INTEGER NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0
      )
    `);
    await this.run(`CREATE INDEX IF NOT EXISTS _zebridge_inbox_tbl ON _zebridge_inbox (tbl, lsn)`);
    await this.run(`
      CREATE TABLE IF NOT EXISTS _zebridge_outbox (
        msg_id     TEXT PRIMARY KEY,
        subject    TEXT NOT NULL,
        payload    TEXT NOT NULL,
        tbl        TEXT NOT NULL,
        row_id     TEXT NOT NULL,
        before     TEXT,
        created_at INTEGER NOT NULL,
        attempts   INTEGER NOT NULL DEFAULT 0
      )
    `);
  }

  private async outboxPut(
    r: { msgId: string; subject: string; payload: unknown; table: string; id: string | number; before: unknown },
    exec: Exec = this.run,
  ) {
    await this.outboxInitPromise;
    await exec(
      `INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, before, created_at, attempts)
       VALUES (?, ?, ?, ?, ?, ?, ?, 0)
       ON CONFLICT(msg_id) DO UPDATE SET attempts = _zebridge_outbox.attempts + 1`,
      r.msgId, r.subject, JSON.stringify(r.payload), r.table, String(r.id),
      r.before == null ? null : JSON.stringify(r.before), Date.now(),
    );
  }

  private async outboxDrop(msgId: string) {
    await this.outboxInitPromise;
    await this.run(`DELETE FROM _zebridge_outbox WHERE msg_id = ?`, msgId);
  }

  /// Public for the console (`zb.outbox()`): a queue you only inspect when something
  /// has already gone wrong should be inspectable.
  public async outboxAll(): Promise<any[]> {
    await this.outboxInitPromise;
    return this.run(`SELECT * FROM _zebridge_outbox ORDER BY created_at`);
  }

  /// Undo an optimistic apply when a verdict says the write cannot land ('rejected' →
  /// restore the before-image; 'row_deleted' → the row is confirmed gone server-side).
  /// Guarded: only if the row still shows exactly what our own write applied.
  private async revertOptimisticWrite(msgId: string, mode: 'restore' | 'delete') {
    await this.outboxInitPromise;
    const rows = await this.run(`SELECT tbl, payload, before FROM _zebridge_outbox WHERE msg_id = ?`, msgId);
    const entry = rows[0];
    if (!entry) return;

    const table: string = entry.tbl;
    const sent = JSON.parse(entry.payload);
    const before = entry.before ? JSON.parse(entry.before) : null;
    const state = this.syncedTables.get(table);
    const keyObj = sent.key as Record<string, unknown> | undefined;

    if (!state?.pkCols.length || !keyObj) {
      await this.run(`DELETE FROM _zebridge_outbox WHERE msg_id = ?`, msgId);
      return;
    }

    const where = state.pkCols.map((c) => `"${c}" = ?`).join(' AND ');
    const pkVals = state.pkCols.map((c) => keyObj[c]);

    await this.transaction(async (txExec) => {
      const current = await txExec(`SELECT * FROM ${table} WHERE ${where}`, ...pkVals);
      const currentRow = current[0] ?? null;

      // SQLite stores booleans as 0/1 and objects as JSON text — normalize the SENT
      // side to storage shape before comparing, or a row with any boolean column
      // never matches and the revert declines forever (the oversize test's ghost).
      const asStored = (v: unknown): string =>
        typeof v === 'boolean' ? (v ? '1' : '0')
        : v !== null && typeof v === 'object' ? JSON.stringify(v)
        : String(v);
      const stillOurs = sent.data
        ? currentRow != null && Object.entries(sent.data as Record<string, unknown>)
            .every(([k, v]) => String(currentRow[k]) === asStored(v))
        : currentRow == null;

      if (stillOurs) {
        if (mode === 'delete') {
          if (currentRow) await txExec(`DELETE FROM ${table} WHERE ${where}`, ...pkVals);
        } else if (before == null) {
          if (currentRow) await txExec(`DELETE FROM ${table} WHERE ${where}`, ...pkVals);
        } else if (currentRow == null) {
          const cols = Object.keys(before);
          const placeholders = cols.map(() => '?').join(', ');
          await txExec(
            `INSERT INTO ${table} (${cols.map((c) => `"${c}"`).join(', ')}) VALUES (${placeholders})`,
            ...cols.map((c) => before[c]),
          );
        } else {
          const cols = Object.keys(before).filter((c) => !state.pkCols.includes(c));
          if (cols.length) {
            const setClause = cols.map((c) => `"${c}" = ?`).join(', ');
            await txExec(`UPDATE ${table} SET ${setClause} WHERE ${where}`, ...cols.map((c) => before[c]), ...pkVals);
          }
        }
      }

      await txExec(`DELETE FROM _zebridge_outbox WHERE msg_id = ?`, msgId);
    });
    this.triggerChange(table);
  }

  // ── schemas ────────────────────────────────────────────────────────────────

  private watchSchemas(): Promise<void> | undefined {
    if (!this.nc) return;
    try {
      return (async () => {
        const watch = await watchBucket(jetstream(this.nc!), this.config.grammar.kv.schemas);
        this.appendLog('SCHEMA', `Watching KV bucket "${this.config.grammar.kv.schemas}" for all tables...`, 'WATCH');

        return new Promise<void>((resolve) => {
          let initialized = false;
          if (watch.pending === 0) { initialized = true; this.reach('migrated'); resolve(); }

          void (async () => {
            for await (const entry of watch.entries) {
              // `delta === 0` marks the last entry of the replay, but it has NOT been
              // applied yet — resolve in the finally, once the migration below landed,
              // or subscribeStreams snapshots syncedTables while the last table is
              // still being created and never seeds it (measured live).
              const isLastOfInitialReplay = !initialized && (!entry || entry.delta === 0);
              try {
                if (!entry || !entry.key) continue;

                if (entry.operation === 'DEL' || entry.operation === 'PURGE') {
                  await this.dropLocalTable(entry.key, 'KV key removed');
                  continue;
                }

                // Schema is always JSON, never msgpack — a fixed bridge-side rule.
                const val: any = JSON.parse(td.decode(entry.value));

                if (val?.dropped === true) {
                  await this.dropLocalTable(entry.key, `tombstone @ lsn ${val.lsn ?? '?'}`);
                  continue;
                }

                if (val?.suspended === true) {
                  // NOTES.md §1.6c: the bridge refuses the table upstream; local rows
                  // stay frozen and valid, writes are refused client-side.
                  const reason = String(val.reason ?? 'suspended');
                  this.suspendedMap.set(entry.key, reason);
                  for (const cb of this.suspendedHandlers) cb(entry.key, reason);
                  this.appendLog('SCHEMA', `${entry.key} suspended upstream (${reason})`, 'WARNING');
                  continue;
                }

                if (val?.sqlite?.columns) {
                  if (this.suspendedMap.delete(entry.key)) {
                    for (const cb of this.suspendedHandlers) cb(entry.key, null);
                  }
                  await this.applySchema(entry.key, val);
                }
              } finally {
                if (isLastOfInitialReplay) {
                  initialized = true;
                  resolve();
                }
              }
            }
          })();
        });
      })();
    } catch (err) {
      this.appendLog('SCHEMA', `Watch failed: ${err}`, 'ERROR');
      return undefined;
    }
  }

  private async dropLocalTable(table: string, reason: string) {
    try {
      await this.pruneInboxDropped(table);
      await this.run(`DROP VIEW IF EXISTS ${table}_view;`);
      await this.run(`DROP TABLE IF EXISTS ${table};`);
      this.syncedTables.delete(table);
      // Queued writes for a dropped table can never apply — the server would only
      // answer row_deleted (or worse, land on an unrelated table that later reuses
      // the name). Discard them LOUDLY: a silent queue that drains into a void is
      // exactly what the outbox exists to prevent.
      try {
        const q = await this.run(`SELECT count(*) AS k FROM _zebridge_outbox WHERE tbl = ?`, table);
        const k = q?.[0]?.k ?? 0;
        if (k > 0) {
          await this.run(`DELETE FROM _zebridge_outbox WHERE tbl = ?`, table);
          this.appendLog('SCHEMA', `${k} queued write(s) for dropped table "${table}" discarded — surface this to the user`, 'WARNING');
        }
      } catch { /* outbox not initialized yet — nothing queued */ }
      this.appendLog('SCHEMA', `Dropped local table "${table}" (${reason})`, 'DROP');
      this.scheduleRecount();
    } catch (err) {
      this.appendLog('SCHEMA', `Drop of ${table} failed: ${err}`, 'ERROR');
    }
  }

  private async applySchema(table: string, val: any) {
    // `pk_columns` is authoritative; `pk` is the legacy single-column form.
    const pkCols: string[] = Array.isArray(val.pk_columns) ? val.pk_columns : val.pk ? [val.pk] : [];
    const cols: { name: string; type: string; required?: boolean }[] = val.sqlite.columns;
    // Dialect-neutral, at the root: CREATE [UNIQUE] INDEX is the same statement here
    // and in PGlite/local Postgres, so one list serves every consumer shape (§10c).
    const indexes: { name: string; unique?: boolean; columns: string[] }[] =
      Array.isArray(val.indexes) ? val.indexes : [];
    // Already filtered upstream to constraints SQLite can satisfy — parent key is
    // the parent's PK or has a ported UNIQUE index (NOTES §10d).
    const foreignKeys: { name: string; columns: string[]; references: string; parent_columns: string[] }[] =
      Array.isArray(val.foreign_keys) ? val.foreign_keys : [];
    const lsn: number = typeof val.lsn === 'number' ? val.lsn : 0;
    const tombstoneColumn: string | null = typeof val.tombstone_column === 'string' ? val.tombstone_column : null;
    const tenantColumn: string | null = typeof val.tenant_column === 'string' ? val.tenant_column : null;
    const names = cols.map((c) => c.name);

    const existing = this.syncedTables.get(table);
    // Renames land BEFORE the add/remove diff — a renamed column is neither.
    const renames: [string, string][] = existing
      ? Object.entries((val.renamed ?? {}) as Record<string, string>)
          .filter(([to, from]) => existing.columns.includes(from) && !existing.columns.includes(to))
          .map(([to, from]) => [from, to])
      : [];

    const effectiveCols = existing
      ? existing.columns.map((n) => renames.find(([from]) => from === n)?.[1] ?? n)
      : [];
    const added = existing ? names.filter((n) => !effectiveCols.includes(n)) : [];
    const removed = existing ? effectiveCols.filter((n) => !names.includes(n)) : [];

    const inlinePk = pkCols.length === 1;
    const isPk = (name: string) => pkCols.includes(name);
    // `required` is NOT NULL with no DEFAULT, published in both dialects. Honouring
    // it makes a bad optimistic write fail HERE instead of round-tripping: before
    // this, mutate() applied locally and came back
    // `null value in column "inserted_at" violates not-null constraint` — the
    // replica accepted a row PostgreSQL would not (NOTES §10c).
    //
    // Tolerant of absence: a bridge older than the field publishes no `required`,
    // which reads as nullable — the same table this built before it existed.
    const ddl = (c: { name: string; type: string; required?: boolean }) =>
      `"${c.name}" ${c.type}` +
      (isPk(c.name) || c.required ? ' NOT NULL' : '') +
      (inlinePk && c.name === pkCols[0] ? ' PRIMARY KEY' : '');
    // ⚠️ SQLite has no ALTER TABLE ADD CONSTRAINT, so a foreign key can only be
    // declared INSIDE CREATE TABLE. That is why an FK change forces a rebuild below
    // while an index change is a cheap CREATE/DROP.
    const fkClauses = foreignKeys
      .filter((f) => f?.references && Array.isArray(f.columns) && f.columns.length &&
                     Array.isArray(f.parent_columns) && f.parent_columns.length === f.columns.length)
      .map((f) =>
        `, FOREIGN KEY (${f.columns.map((c) => `"${c}"`).join(', ')})` +
        ` REFERENCES ${f.references} (${f.parent_columns.map((c) => `"${c}"`).join(', ')})`,
      )
      .join('');
    const tableConstraint =
      (pkCols.length > 1 ? `, PRIMARY KEY (${pkCols.map((c) => `"${c}"`).join(', ')})` : '') + fkClauses;

    const rebuildPreservingData = async (why: string) => {
      const tmp = `${table}__migrating`;
      await this.run(`DROP TABLE IF EXISTS ${tmp};`);
      await this.run(`CREATE TABLE ${tmp} (${cols.map(ddl).join(', ')}${tableConstraint});`);
      if (existing) {
        const common = names.filter((n) => existing.columns.includes(n)).map((n) => `"${n}"`);
        if (common.length) {
          await this.run(`INSERT INTO ${tmp} (${common.join(', ')}) SELECT ${common.join(', ')} FROM ${table};`);
        }
      }
      await this.run(`DROP TABLE IF EXISTS ${table};`);
      await this.run(`ALTER TABLE ${tmp} RENAME TO ${table};`);
      this.appendLog('SCHEMA', `${table}: rebuilt preserving common columns (${why}), lsn=${lsn}`, 'MIGRATE');
    };

    try {
      if (!existing) {
        await this.run(`DROP TABLE IF EXISTS ${table};`);
        await this.run(`CREATE TABLE ${table} (${cols.map(ddl).join(', ')}${tableConstraint});`);
        this.appendLog('SCHEMA', `${table}: created (first sight), lsn=${lsn}`, 'MIGRATE');
      } else if (added.length === 0 && removed.length === 0 && renames.length === 0 &&
                 !(await this.foreignKeysDiffer(table, fkClauses))) {
        this.syncedTables.set(table, { pkCols, columns: names, lsn, tombstoneColumn, tenantColumn });
        this.reach('migrated');
        this.scheduleRecount();
        // ⚠️ NOT a no-op path for indexes. Adding an index in PostgreSQL changes no
        // column, so the republish that carries it lands EXACTLY here — returning
        // without syncing would make `CREATE INDEX` upstream a silent no-op forever.
        await this.syncIndexes(table, indexes);
        return; // identical schema, e.g. a boot republish
      } else {
        // The view goes FIRST (§1.17): DROP COLUMN re-validates every schema object
        // referencing the table, and the stale view kills the ALTER.
        await this.run(`DROP VIEW IF EXISTS ${table}_view;`);
        for (const [from, to] of renames) {
          await this.run(`ALTER TABLE ${table} RENAME COLUMN "${from}" TO "${to}";`);
        }
        try {
          for (const name of removed) {
            await this.run(`ALTER TABLE ${table} DROP COLUMN "${name}";`);
          }
          for (const name of added) {
            const type = cols.find((c) => c.name === name)!.type;
            await this.run(`ALTER TABLE ${table} ADD COLUMN "${name}" ${type};`);
          }
          if (await this.foreignKeysDiffer(table, fkClauses)) {
            // ALTER cannot add or drop a constraint in SQLite; only a rebuild can.
            await rebuildPreservingData('foreign keys changed');
          }
        } catch (alterErr) {
          await rebuildPreservingData(`ALTER refused: ${alterErr}`);
        }
      }

      const EXCLUDE_FROM_VIEW = ['uid', 'inserted_at', 'updated_at', 'metadata'];
      const viewCols = names.filter((n) => !EXCLUDE_FROM_VIEW.includes(n)).map((n) => `"${n}"`).join(', ');
      await this.run(`DROP VIEW IF EXISTS ${table}_view;`);
      if (viewCols) await this.run(`CREATE VIEW ${table}_view AS SELECT ${viewCols} FROM ${table};`);

      // After every shape change, because a rebuild DROPs the table and takes its
      // indexes with it.
      await this.syncIndexes(table, indexes);

      this.syncedTables.set(table, { pkCols, columns: names, lsn, tombstoneColumn, tenantColumn });
      // Both registration paths mark the phase — a strip that lies is worse than none.
      this.reach('migrated');
      this.scheduleRecount();

      await this.drainPending(table);
    } catch (err) {
      this.appendLog('SCHEMA', `Applying schema for ${table} failed: ${err}`, 'ERROR');
    }
  }

  /// Does the stored table's DDL disagree with the foreign keys we now want?
  ///
  /// Compared against the recorded CREATE TABLE text because SQLite keeps no
  /// queryable "expected constraints" — `foreign_key_list` reports what IS declared,
  /// and comparing that back to clauses is more fragile than comparing the clauses
  /// themselves. A mismatch forces `rebuildPreservingData`, since ALTER cannot add
  /// or drop a constraint.
  private async foreignKeysDiffer(table: string, fkClauses: string): Promise<boolean> {
    try {
      const rows = await this.run(`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, table);
      const ddl: string = rows?.[0]?.sql ?? '';
      if (!ddl) return false;                       // no table yet: the create path handles it
      const hasAny = /FOREIGN KEY/i.test(ddl);
      if (!fkClauses) return hasAny;                // wanted none, has some
      // Normalise whitespace; the stored DDL is our own generated text.
      const want = fkClauses.replace(/^,\s*/, '').replace(/\s+/g, ' ').trim();
      return !ddl.replace(/\s+/g, ' ').includes(want);
    } catch {
      return false;
    }
  }

  /// Bring the replica's secondary indexes in line with the published list.
  ///
  /// Without these the replica answers every predicate with a sequential scan, which
  /// quietly undercuts the whole promise of querying it directly. The bridge sends
  /// only what translates (no partial, expression or non-btree indexes), so this is a
  /// straight create/drop against the names it names.
  ///
  /// Drops indexes we hold that are no longer published — an index removed upstream
  /// should not linger, costing writes for a query nobody makes. `sqlite_`-prefixed
  /// entries are SQLite's own (the implicit PK index) and are never touched.
  private async syncIndexes(table: string, indexes: { name: string; unique?: boolean; columns: string[] }[]) {
    try {
      const rows = await this.run(
        `SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND name NOT LIKE 'sqlite_%'`,
        table,
      );
      const have = new Set<string>((rows ?? []).map((r: any) => r.name));
      const want = new Set(indexes.map((i) => i.name));

      for (const stale of have) {
        if (!want.has(stale)) {
          await this.run(`DROP INDEX IF EXISTS "${stale}";`);
          this.appendLog('SCHEMA', `${table}: dropped index ${stale} (no longer published)`, 'MIGRATE');
        }
      }
      let created = 0;
      for (const ix of indexes) {
        if (!ix?.name || !Array.isArray(ix.columns) || !ix.columns.length) continue;
        if (have.has(ix.name)) continue;
        const cols = ix.columns.map((c) => `"${c}"`).join(', ');
        await this.run(`CREATE ${ix.unique ? 'UNIQUE ' : ''}INDEX IF NOT EXISTS "${ix.name}" ON ${table} (${cols});`);
        created++;
      }
      if (created) {
        this.appendLog('SCHEMA', `${table}: ${created} index(es) created`, 'MIGRATE');
      }
    } catch (err) {
      // An index is a performance object: failing to build one must never stop a
      // table from syncing. Loud, but not fatal.
      this.appendLog('SCHEMA', `${table}: index sync failed (queries will scan): ${err}`, 'ERROR');
    }
  }

  private async drainPending(table: string) {
    if (!this.pendingEvents.length) return;
    const mine = this.pendingEvents.filter((p) => p.table === table);
    if (!mine.length) return;
    for (let i = this.pendingEvents.length - 1; i >= 0; i--) {
      if (this.pendingEvents[i].table === table) this.pendingEvents.splice(i, 1);
    }
    this.appendLog('CDC', `Replaying ${mine.length} held event(s) for ${table}`, 'DRAIN');
    for (const p of mine) await this.applyEvent(p.table, p.ev);
  }

  /// A batch failed as a unit — replay it event by event so one bad row cannot take
  /// the rest with it. Anything failing a FOREIGN KEY check is HELD, not dropped:
  /// its parent may simply be in a later batch, which happens whenever a single
  /// PostgreSQL transaction is larger than the bridge's ring and gets split across
  /// batches (NOTES.md finding 5).
  private async applyBatchIsolated(streamName: string, toApply: { table: string; ev: any }[], batchErr: string) {
    let applied = 0, held = 0, failed = 0;
    for (const { table, ev } of toApply) {
      try {
        await this.transaction(async (txExec) => {
          await txExec(`PRAGMA defer_foreign_keys = ON;`);
          await this.applyEvent(table, ev, txExec);
        });
        applied++;
      } catch (e) {
        const kind = foreignKeyFailureKind(e);
        if (kind === 'missing-parent') {
          await this.holdEvent(table, ev, String(e));
          held++;
        } else if (kind === 'mismatch') {
          // Waiting cannot fix this: the parent key is not the PK and carries no
          // UNIQUE index, so the constraint should never have been ported.
          failed++;
          this.appendLog('CDC', `DROPPED ${ev?.operation} on ${table}: ${e} — the ported FK is unsatisfiable, its parent key needs a UNIQUE index (NOTES §10d)`, 'ERROR');
        } else {
          failed++;
          this.appendLog('CDC', `DROPPED ${ev?.operation} on ${table} (lsn ${ev?.lsn}): ${e}`, 'ERROR');
        }
      }
    }
    this.appendLog(
      'SYS',
      `${streamName} batch of ${toApply.length} failed as a unit (${batchErr}) — isolated replay: ` +
        `${applied} applied, ${held} held for a missing parent, ${failed} dropped`,
      failed ? 'ERROR' : 'WARNING',
    );
  }

  private async deleteHeld(rows: { id?: number }[], exec: Exec = this.run) {
    const ids = rows.map((r) => r.id).filter((i): i is number => i != null);
    if (!ids.length) return;
    await exec(`DELETE FROM _zebridge_inbox WHERE id IN (${ids.map(() => '?').join(',')})`, ...ids);
  }

  /// Persist a held event and remember it. In memory ALONE it was lost on restart
  /// while already ACKed to JetStream — so it would never be redelivered either.
  /// Silent, permanent divergence, the same family as findings 4 and 5.
  ///
  /// `id` is AUTOINCREMENT: arrival order is apply order, and a restart must
  /// replay them in the order they were received, not by lsn (which is NOT
  /// monotonic in delivery order — NOTES §10f).
  private async holdEvent(table: string, ev: any, reason: string) {
    const lsn = typeof ev?.lsn === 'number' ? ev.lsn : 0;
    try {
      const r = await this.run(
        `INSERT INTO _zebridge_inbox (tbl, lsn, ev, reason, held_at) VALUES (?,?,?,?,?) RETURNING id`,
        table, lsn, JSON.stringify(ev), reason.slice(0, 300), Date.now(),
      );
      this.fkHeld.push({ id: r?.[0]?.id, table, ev });
    } catch (err) {
      // Never lose the event to a bookkeeping failure — hold it in memory at least.
      this.fkHeld.push({ table, ev });
      this.appendLog('CDC', `held event could not be persisted (${err}) — it survives only until restart`, 'ERROR');
    }
  }

  /// Reload holds after a restart, in arrival order.
  private async loadHeldEvents() {
    try {
      const rows = await this.run(`SELECT id, tbl, ev FROM _zebridge_inbox ORDER BY id`);
      if (!rows?.length) return;
      for (const r of rows) {
        try { this.fkHeld.push({ id: r.id, table: r.tbl, ev: JSON.parse(r.ev) }); } catch { /* unreadable row */ }
      }
      this.appendLog('SYS', `${this.fkHeld.length} held event(s) restored from the inbox — awaiting their parents`, 'INFO');
    } catch { /* table absent on an older replica */ }
  }

  /// PRUNING. Four points, and deliberately no fifth:
  ///
  ///   applied      — the row is done; delete it. The primary path.
  ///   re-seeded    — a seed is a new baseline at a watermark, so anything at or
  ///                  below it is ALREADY in the seeded data. Rows ABOVE it are
  ///                  NOT, and must survive: they were acked, so CDC will never
  ///                  redeliver them. Hence `lsn <= watermark`, never a blanket
  ///                  delete by table.
  ///   table dropped— the table is gone; its holds are meaningless.
  ///   ...and NOT by age. A parent that never arrives is a REAL condition (a
  ///   constraint whose parent row the client may not read), and expiring the row
  ///   would silently discard data — the exact failure class this whole table
  ///   exists to end. It stays, it is counted, and it is loud.
  private async pruneInboxSeeded(table: string, watermarkLsn: number) {
    try {
      await this.run(`DELETE FROM _zebridge_inbox WHERE tbl = ? AND lsn <= ?`, table, watermarkLsn);
      this.fkHeld = this.fkHeld.filter((h) => !(h.table === table && (h.ev?.lsn ?? 0) <= watermarkLsn));
    } catch { /* nothing held */ }
  }

  private async pruneInboxDropped(table: string) {
    try {
      const q = await this.run(`SELECT count(*) AS k FROM _zebridge_inbox WHERE tbl = ?`, table);
      const k = q?.[0]?.k ?? 0;
      if (k > 0) {
        this.appendLog('CDC', `${table}: discarding ${k} held event(s) — the table was dropped upstream`, 'WARNING');
        await this.run(`DELETE FROM _zebridge_inbox WHERE tbl = ?`, table);
      }
      this.fkHeld = this.fkHeld.filter((h) => h.table !== table);
    } catch { /* nothing held */ }
  }

  /// Retry events held for a missing parent. Called after every batch, because the
  /// parent arrives in a LATER batch when a big transaction was split — which makes
  /// a cross-batch split self-healing rather than a silent hole.
  private async retryFkHeld(streamName: string) {
    if (!this.fkHeld.length) return;
    const pending = this.fkHeld;
    this.fkHeld = [];
    let applied = 0;

    // ⚠️ BULK FIRST, and this is the whole difference between converging and not.
    // Retrying one-transaction-per-event after every batch is O(n * batches): with
    // 15,000 held across 51 batches that is ~765,000 transactions, which never
    // finishes and starves the very batches that carry the missing parents —
    // measured, the held set sat at 15,000 with 0 resolved while the parent table
    // stopped advancing entirely. One deferred transaction retries the whole set at
    // once, and once the parents have landed that is a single COMMIT.
    try {
      await this.transaction(async (txExec) => {
        await txExec(`PRAGMA defer_foreign_keys = ON;`);
        for (const { table, ev } of pending) await this.applyEvent(table, ev, txExec);
        // Same transaction as the apply: an event is either applied AND forgotten,
        // or neither. A crash between the two would replay it forever or lose it.
        await this.deleteHeld(pending, txExec);
      });
      applied = pending.length;
    } catch {
      // Some are still orphaned. NOW it is worth isolating, because only the ones
      // that individually fail go back on the queue.
      for (const held of pending) {
        const { table, ev } = held;
        try {
          await this.transaction(async (txExec) => {
            await txExec(`PRAGMA defer_foreign_keys = ON;`);
            await this.applyEvent(table, ev, txExec);
            await this.deleteHeld([held], txExec);
          });
          applied++;
        } catch (e) {
          if (foreignKeyFailureKind(e) === 'missing-parent') {
            this.fkHeld.push(held);            // still waiting; try again next batch
            if (held.id != null) {
              try { await this.run(`UPDATE _zebridge_inbox SET attempts = attempts + 1 WHERE id = ?`, held.id); } catch { /* best effort */ }
            }
          } else {
            this.appendLog('CDC', `DROPPED held ${ev?.operation} on ${table}: ${e}`, 'ERROR');
            await this.deleteHeld([held]);
          }
        }
      }
    }
    if (applied) {
      this.appendLog('SYS', `${streamName}: ${applied} held event(s) applied once their parent arrived` +
        (this.fkHeld.length ? `, ${this.fkHeld.length} still waiting` : ''), 'INFO');
    }
  }

  // ── CDC apply ──────────────────────────────────────────────────────────────

  /// `ev.optimistic` marks a synthetic event from our own mutate(): lsn is a sentinel
  /// so the gate never blocks it, and neither the echo-pop nor resume bookkeeping run.
  private async applyEvent(table: string, ev: any, exec: Exec = this.run, seed = false) {
    const state = this.syncedTables.get(table);
    if (!state || !ev?.data) return;
    this.triggerChange(table, ev);

    // Strictly `<`, never `<=`: the first post-snapshot commit carries the watermark
    // LSN itself — skipping it loses exactly one row per snapshot (measured).
    // Re-applying is free (the upsert converges); skipping is permanent.
    //
    // ⚠️ `seed` bypasses the gate entirely. The schema descriptor is re-published at
    // every bridge restart with the CURRENT WAL LSN, so on a fresh replica
    // `state.lsn` starts far ahead of any stored snapshot's descriptor LSN — and
    // this line silently dropped EVERY snapshot-replayed row while the replay
    // counter kept counting ("3 rows applied", table empty — found live in the
    // enrollment demo; chain seeding survived only because it writes outside this
    // path). Seeding IS the baseline: it must land unconditionally, and the
    // caller re-anchors state.lsn to the snapshot's own watermark afterwards.
    if (!seed) {
      if (typeof state.seedSeq === 'number' && state.seedStream) {
        // ── finding 7's fix (NOTES §10i) ──
        // Drop only what the chain provably contains: an event published at or
        // before cutoff_seq was decoded from a transaction that COMMITTED before
        // the producer's snapshot began (capture precedes BEGIN), so the snapshot
        // saw it. An in-flight transaction commits later, publishes later, and its
        // seq exceeds the cutoff even though its row lsns may be LOWER — measured:
        // inflight-A, seq 62129 > cutoff, lsn below it, lost under the lsn gate.
        // Events from OTHER streams and optimistic locals carry no matching
        // seq/stream and pass untouched.
        if (ev.stream === state.seedStream && typeof ev.seq === 'number' && ev.seq <= state.seedSeq) return;
      } else if (ev.lsn < state.lsn) {
        // Legacy manifest without cutoff_seq (an older bridge): the lsn gate, with
        // its known in-flight blind spot, is still better than no gate at all.
        return;
      }
    }

    const op = ev.operation;

    if (op === 'INSERT' || op === 'UPDATE') {
      const keys = Object.keys(ev.data);
      const unknown = keys.filter((k) => !state.columns.includes(k) && !k.startsWith('old.'));
      if (unknown.length) {
        this.pendingEvents.push({ table, ev });
        this.appendLog('CDC', `Holding ${op} on ${table}: unknown column(s) [${unknown.join(', ')}] — awaiting schema newer than lsn ${state.lsn}`, 'HOLD');
        this.scheduleRecount();
        return;
      }

      // A changed primary key arrives as an UPDATE with `old.*` — delete the old key
      // first, or the row lives on under both keys forever (measured live).
      if (state.pkCols.length) {
        const oldPk = state.pkCols.map((c) => ev.data[`old.${c}`]);
        const newPk = state.pkCols.map((c) => ev.data[c]);
        const keyChanged =
          oldPk.every((v) => v !== undefined && v !== null) &&
          oldPk.some((v, i) => v !== newPk[i]);
        if (keyChanged) {
          const where = state.pkCols.map((c) => `"${c}" = ?`).join(' AND ');
          try {
            await exec(`DELETE FROM ${table} WHERE ${where}`, ...oldPk);
            this.appendLog('CDC', `Key change on ${table}: ${JSON.stringify(oldPk)} → ${JSON.stringify(newPk)}`, 'REKEY');
          } catch (err) {
            this.appendLog('SQLITE', `Key-change cleanup on ${table} failed: ${err}`, 'ERROR');
          }
        }
      }

      const dataKeys = keys.filter((k) => !k.startsWith('old.'));
      const values = dataKeys.map((k) => ev.data[k]).map((v) =>
        v !== null && typeof v === 'object' ? JSON.stringify(v) : v,
      );
      const columns = dataKeys.map((k) => `"${k}"`).join(', ');
      const placeholders = dataKeys.map(() => '?').join(', ');
      const updates = dataKeys
        .filter((k) => !state.pkCols.includes(k))
        .map((k) => `"${k}" = excluded."${k}"`)
        .join(', ');

      let query = `INSERT INTO ${table} (${columns}) VALUES (${placeholders})`;
      if (state.pkCols.length && updates) {
        const conflict = state.pkCols.map((c) => `"${c}"`).join(', ');
        query += ` ON CONFLICT(${conflict}) DO UPDATE SET ${updates}`;
      }

      try {
        await exec(query, ...values);
      } catch (err) {
        this.appendLog('SQLITE', `UPSERT on ${table} failed: ${err}`, 'ERROR');
      }
    } else if (op === 'DELETE') {
      // Every key column must be present: a partial composite key would match more
      // rows than PostgreSQL deleted.
      const pkVals = state.pkCols.map((c) => ev.data[c]);
      if (state.pkCols.length && pkVals.every((v) => v !== undefined && v !== null)) {
        const where = state.pkCols.map((c) => `"${c}" = ?`).join(' AND ');
        try {
          await exec(`DELETE FROM ${table} WHERE ${where}`, ...pkVals);
        } catch (err) {
          this.appendLog('SQLITE', `DELETE on ${table} failed: ${err}`, 'ERROR');
        }
      }
    }

    // The echo is the success signal: a successful write produces no verdict (§7.0) —
    // the CDC echo of our own key is what pops the outbox entry.
    if (!ev.optimistic && state.pkCols.length && this.pendingWrites.size) {
      const echoedKey = state.pkCols.map((c) => String(ev.data?.[c])).join('|');
      for (const [msgId, w] of this.pendingWrites) {
        if (w.table !== table || String(w.id) !== echoedKey) continue;
        this.pendingWrites.delete(msgId);
        void this.outboxDrop(msgId);
        this.appendLog(table, `confirmed by CDC echo after ${Date.now() - w.at}ms`, 'CONFIRMED');
      }
    }

    const stream: string = ev.stream ?? '';
    const streamSeq = stream ? (this.globalSyncState.seq[stream] ?? 0) : 0;
    if (!ev.optimistic && ((ev.lsn ?? 0) > this.globalSyncState.lsn || (ev.seq ?? 0) > streamSeq)) {
      this.globalSyncState.lsn = Math.max(this.globalSyncState.lsn, ev.lsn ?? 0);
      if (stream) this.globalSyncState.seq[stream] = Math.max(streamSeq, ev.seq ?? 0);
      try {
        await exec(`UPDATE _zebridge_sync SET global_last_lsn = ? WHERE id = 1`, this.globalSyncState.lsn);
        if (stream) {
          await exec(
            `INSERT INTO _zebridge_stream_seq (stream, last_seq) VALUES (?, ?)
             ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq`,
            stream, this.globalSyncState.seq[stream],
          );
        }
      } catch (e) {
        this.appendLog('SQLITE', `Failed to update sync state: ${e}`, 'ERROR');
      }
    }
  }

  // ── stream/tenant routing ──────────────────────────────────────────────────

  /// One stream per tenant plus the shared public one — the stream NAME is the read
  /// boundary (a filter_subject is reader-chosen, not a permission).
  private cdcStreams(): string[] {
    const cfg = this.config.grammar.cdc_streams;
    if (!cfg) return [this.config.grammar.streams.cdc];
    // $KV.tenants is the runtime truth, NOT grammar.json's tenant list: a tenant
    // born after the file was written (dyntenant) has real streams the client must
    // read. The one tenant with no stream of its own is the OPEN tenant — checked
    // explicitly (the old list-membership gate silently ignored dynamic tenants;
    // its original job was only to avoid the CDC__DEFAULT ghost stream).
    const open = this.config.grammar.open_tenant || '_default';
    if (!this.tenantValue || this.tenantValue === open) return [cfg.public];
    return [`${cfg.tenant_prefix}${this.tenantValue}`, cfg.public];
  }

  private cdcStreamForTenant(tenant: string): string {
    const cfg = this.config.grammar.cdc_streams;
    if (!cfg) return this.config.grammar.streams.cdc;
    const open = this.config.grammar.open_tenant || '_default';
    if (!tenant || tenant === open) return cfg.public;
    return `${cfg.tenant_prefix}${tenant}`;
  }

  /// The tenant token THIS table's descriptors/manifests live under: the client's own
  /// tenant when the table is tenant-scoped, the open tenant otherwise — so every
  /// principal converges on one shared entry for tenant-agnostic tables.
  private effectiveTenantFor(table: string): string {
    const state = this.syncedTables.get(table);
    if (state?.tenantColumn) return this.tenantValue;
    return this.config.grammar.open_tenant || this.tenantValue;
  }

  private initStream(tenant: string): string {
    const cfg = this.config.grammar.init_streams;
    if (!cfg) return this.config.grammar.streams.init;
    const open = this.config.grammar.open_tenant || '_default';
    if (!tenant || tenant === open) return cfg.public;
    return `${cfg.tenant_prefix}${tenant}`;
  }

  private async resolveTenant() {
    if (!this.nc) return;
    try {
      const kvm = new Kvm(this.nc);
      // allow_direct must be explicit: Kvm.open never asks the server, and the grant
      // covers ONLY the per-key Direct Get path (measured — see App.tsx history).
      const kv = await kvm.open(this.config.grammar.kv.tenants, { allow_direct: true });
      const entry = await kv.get(this.config.principal);
      if (entry) {
        let val: string;
        try { val = decode(entry.value) as string; } catch { val = td.decode(entry.value); }
        this.tenantValue = val;
        this.appendLog('SYS', `Resolved tenant for '${this.config.principal}': ${val}`, 'INFO');
      } else {
        this.appendLog('SYS', `No tenant mapping for '${this.config.principal}' — public-only reads`, 'INFO');
      }
    } catch (err) {
      this.appendLog('SYS', `Tenant resolution failed: ${err}`, 'ERROR');
    }
  }

  // ── seeding: generations first, snapshots as the fallback ─────────────────

  /// Seed or catch up one table from its delta-generation chain (NOTES.md §1.13).
  /// Returns false for every "not this way" outcome — the snapshot path is the
  /// fallback, not an error handler. Watermark-based walk, guarded upsert, one
  /// manifest re-read on a 404 mid-walk. Never throws.
  private async applyGenerations(table: string): Promise<boolean> {
    const GEN = this.config.grammar.generations;
    if (!GEN || !this.nc) return false;
    const state = this.syncedTables.get(table);
    if (!state || !state.pkCols.length) return false;

    const tenantForTable = this.effectiveTenantFor(table);
    const key = `${tenantForTable}.${table}`;
    const readManifest = async (): Promise<any | null> => {
      try {
        const kvm = new Kvm(this.nc!);
        const kv = await kvm.open(GEN.kv, { allow_direct: true });
        const entry = await kv.get(key);
        return entry ? JSON.parse(td.decode(entry.value)) : null; // manifest is JSON
      } catch (e) { this.appendLog('SYS', `${table}: chain manifest unreadable: ${e}`, 'ERROR'); return null; }
    };
    let manifest = await readManifest();
    if (!manifest?.full?.object) return false;

    let os: any;
    try { os = await new Objm(this.nc).open(manifest.bucket); } catch (e) { this.appendLog('SYS', `${table}: chain bucket ${manifest.bucket} unreachable: ${e}`, 'ERROR'); return false; }
    const fetchDoc = async (name: string): Promise<any | null> => {
      try {
        const blob = await os.getBlob(name);
        return blob ? (decode(blob) as any) : null; // objects are msgpack
      } catch (e) { this.appendLog('SYS', `${table}: chain object ${name} unreadable: ${e}`, 'ERROR'); return null; }
    };

    let watermark: string | null = null;
    try {
      const r = await this.run(`SELECT watermark FROM _zebridge_generations WHERE tbl = ?`, table);
      watermark = r[0]?.watermark ?? null;
    } catch { /* fresh replica */ }

    const planFrom = (man: any): { name: string; kind: 'full' | 'delta' }[] => {
      const deltas: any[] = man.deltas ?? [];
      const applicable = watermark ? deltas.filter((d) => d.cutoff > watermark!) : deltas;
      const reaches = watermark != null &&
        (applicable.length === 0 || applicable[0].prev_cutoff <= watermark);
      if (reaches) return applicable.map((d) => ({ name: d.object, kind: 'delta' as const }));
      return [
        { name: man.full.object, kind: 'full' as const },
        ...deltas.filter((d) => d.gen > man.full.gen)
                 .map((d) => ({ name: d.object, kind: 'delta' as const })),
      ];
    };

    const applyPlan = async (plan: { name: string; kind: string }[]): Promise<number | null> => {
      let applied = 0;
      for (const step of plan) {
        const doc = await fetchDoc(step.name);
        if (!doc) return null; // pruned under us — caller re-reads
        const cols: string[] = doc.columns ?? [];
        if (!cols.length || !cols.every((c) => state.columns.includes(c))) {
          this.appendLog('SYS', `Generation ${step.name} for ${table} references columns the local schema lacks — falling back to snapshot`, 'WARNING');
          return null;
        }
        const vcol: string = doc.version_column ?? manifest.version_column;
        const colList = cols.map((c) => `"${c}"`).join(', ');
        const ph = cols.map(() => '?').join(', ');
        const conflict = state.pkCols.map((c) => `"${c}"`).join(', ');
        const sets = cols.filter((c) => !state.pkCols.includes(c))
                         .map((c) => `"${c}" = excluded."${c}"`).join(', ');
        let q = `INSERT INTO ${table} (${colList}) VALUES (${ph})`;
        q += sets
          ? ` ON CONFLICT(${conflict}) DO UPDATE SET ${sets}` +
            (vcol && cols.includes(vcol) ? ` WHERE excluded."${vcol}" > ${table}."${vcol}"` : '')
          : ` ON CONFLICT(${conflict}) DO NOTHING`;
        await this.transaction(async (txExec) => {
          // A full replaces the baseline wholesale; the DELETE shares the transaction
          // so a crash mid-apply cannot leave an empty table.
          if (step.kind === 'full') await txExec(`DELETE FROM ${table}`);
          for (const row of doc.rows) {
            await txExec(q, ...row.map((v: any) =>
              v !== null && typeof v === 'object' ? JSON.stringify(v) : pgTsToWire(v)));
          }
        });
        applied += doc.rows.length;
      }
      return applied;
    };

    let applied = await applyPlan(planFrom(manifest));
    if (applied === null) {
      // Pruned between manifest read and fetch: re-read ONCE, restart from ITS full —
      // overlap, never a gap; a second failure falls back to the snapshot path.
      manifest = await readManifest();
      if (!manifest?.full?.object) return false;
      watermark = null;
      applied = await applyPlan(planFrom(manifest));
      if (applied === null) return false;
    }

    state.lsn = lsnToNumber(manifest.cutoff_lsn);
    if (typeof manifest.cutoff_seq === 'number' && manifest.cutoff_seq > 0 && manifest.cdc_stream) {
      state.seedSeq = manifest.cutoff_seq;
      state.seedStream = manifest.cdc_stream;
    }
    await this.pruneInboxSeeded(table, state.lsn);
    await this.run(
      `INSERT INTO _zebridge_generations (tbl, watermark, cutoff_lsn) VALUES (?, ?, ?)
       ON CONFLICT(tbl) DO UPDATE SET watermark = excluded.watermark, cutoff_lsn = excluded.cutoff_lsn`,
      table, manifest.cutoff_version, state.lsn,
    );
    this.scheduleRecount();
    this.appendLog('SYS', `Seeded ${table} from generation chain g${manifest.gen} (${applied} row(s), watermark ${manifest.cutoff_version} @ ${manifest.cutoff_lsn})`, 'INFO');
    return true;
  }

  /// Is a cached snapshot descriptor still safe to trust, or has the CDC stream aged
  /// past its watermark (orphaned descriptor — the failure `descriptorStillFresh` was
  /// built for)? Unverifiable cases resolve to "fresh" — a lookup failure must not
  /// force every table to re-snapshot.
  private async descriptorStillFresh(js: any, tenant: string, desc: any): Promise<boolean> {
    if (desc?.lsn == null) return true;
    const streamName = this.cdcStreamForTenant(tenant);
    try {
      const jsm = await js.jetstreamManager();
      const info = await jsm.streams.info(streamName);
      if (info.state.messages === 0) return false;
      if (info.state.first_seq <= 1) return true;
      const ci = await jsm.consumers.add(streamName, {
        deliver_policy: DeliverPolicy.StartSequence,
        opt_start_seq: info.state.first_seq,
      });
      const consumer = await js.consumers.get(streamName, ci.name);
      const batch = await consumer.fetch({ max_messages: 1, expires: 3000 });
      let oldestLsn: number | null = null;
      for await (const m of batch) {
        try {
          const decoded = decode(m.data);
          const ev = Array.isArray(decoded) ? decoded[0] : decoded;
          oldestLsn = ev?.lsn ?? null;
        } catch { /* undecodable — treat as unknown */ }
      }
      if (oldestLsn == null) return true;
      return oldestLsn <= desc.lsn;
    } catch {
      return true;
    }
  }

  // ── the main orchestration: gap check → seed → CDC ────────────────────────

  private async subscribeStreams() {
    if (!this.nc) return;
    const js = jetstream(this.nc);
    const jsm = await jetstreamManager(this.nc);

    // 1. Gap detection — asked of EVERY stream this client reads: a gap in any of
    // them means missing rows, and checking only one looks like an empty table.
    try {
      let gap = false;
      const gapDetail: string[] = [];
      for (const streamName of this.cdcStreams()) {
        const info = await jsm.streams.info(streamName);
        const firstSeq = info.state.first_seq;
        const local = this.globalSyncState.seq[streamName] ?? 0;
        if (local === 0 || (firstSeq > 0 && local < firstSeq - 1)) {
          gap = true;
          gapDetail.push(`${streamName}: local ${local}, stream first ${firstSeq}`);
        }
      }

      if (!gap) {
        this.appendLog('SYS', 'No CDC gap — resuming from stored positions, no seeding needed', 'INFO');
        this.reach('snapshot');
      } else {
        this.appendLog('SYS', `Gap detected or first run! ${gapDetail.join('; ')}. Seeding required!`, 'WARNING');

        let snapKv: any;
        try {
          const kvm = new Kvm(this.nc!);
          // Per-tenant grant covers ONLY the exact-key Direct Get path (see history).
          snapKv = await kvm.open(this.config.grammar.kv.snapshots, { allow_direct: true });
        } catch { /* no snapshot bucket access — generations may still seed */ }

        const tenanted = !!this.config.grammar.init_streams;
        const tablesToSeed = new Set(this.syncedTables.keys());
        const seedPromises: Promise<void>[] = [];

        // Off for the duration of the bulk load — see the re-arm below. A no-op
        // inside a transaction, which is why it is here and not in a seed step.
        try { await this.run(`PRAGMA foreign_keys = OFF;`); } catch { /* engine without it */ }

        // ⚠️ ONE TABLE PER PROMISE, and that is the point.
        //
        // This used to be a sequential `for` that awaited each table's whole
        // request/retry cycle inline — so a single table that could not be seeded
        // blocked every table AFTER it for 5 attempts x 60 s. Measured: `orders`
        // never seeded at all and looked broken, when in fact `test_types` sat ahead
        // of it in the loop, orphaned and throttled, and `orders` was never reached.
        // Five minutes of head-of-line blocking presenting as data loss.
        //
        // A table that cannot seed is ITS OWN failure (`this.failed`), never a
        // reason to starve the rest.
        for (const table of tablesToSeed) seedPromises.push((async () => {
          const tenantForTable = tenanted ? this.effectiveTenantFor(table) : '';
          const snapKey = tenanted ? `${tenantForTable}.${table}` : table;

          // Generations are the ONLY seeding path (NOTES.md §1.13, §10h): the
          // producer builds once on a cadence, every client catches up on deltas.
          if (await this.applyGenerations(table)) {
            this.reach('snapshot');
            return;
          }

          if (!LEGACY_SNAPSHOT_SEEDING) {
            // No usable chain yet — the ordinary case is a table created between two
            // cadence ticks. Wait for the producer rather than demanding a bespoke
            // dump: polling the chain is idempotent and cannot rewind anything,
            // which is precisely what the retired path below could not promise.
            this.appendLog('SYS', `${table}: no generation chain yet — waiting up to ${GENERATION_WAIT_MS / 1000}s for the producer (builds every GENERATION_CADENCE_SECONDS)`, 'INFO');
            const deadline = Date.now() + GENERATION_WAIT_MS;
            while (Date.now() < deadline) {
              await new Promise((r) => setTimeout(r, GENERATION_POLL_MS));
              if (await this.applyGenerations(table)) {
                this.reach('snapshot');
                return;
              }
            }
            // Unseeded is NOT the same as synced: following CDC against an unseeded
            // table diverges silently — strictly worse than being visibly absent.
            this.syncedTables.delete(table);
            this.failed.add(table);
            this.scheduleRecount();
            this.appendLog('SYS', `Giving up on ${table}: no generation chain after ${GENERATION_WAIT_MS / 1000}s — NOT following CDC for it. Check the producer (GENERATIONS_ENABLED, its cadence, and the gen-<tenant> object store); the table seeds on the next connect once a chain exists. Snapshot-on-demand is retired (NOTES §10h).`, 'ERROR');
            return;
          }

          // ─────────────────────────────────────────────────────────────────────
          // Everything below is the RETIRED path, reachable only via the gate above.
          // ─────────────────────────────────────────────────────────────────────
          let desc: any = null;
          let rejectedId: string | undefined;
          if (snapKv) {
            try {
              const entry = await snapKv.get(snapKey);
              if (entry) {
                const candidate = decode(entry.value) as any; // descriptor is always msgpack
                if (await this.descriptorStillFresh(js, tenantForTable, candidate)) {
                  desc = candidate;
                } else {
                  rejectedId = candidate?.snapshot_id;
                  this.appendLog('SYS', `Cached snapshot for ${table} is orphaned (CDC no longer covers its watermark) — requesting a fresh one instead`, 'WARNING');
                }
              }
            } catch { /* no cached descriptor */ }
          }

          if (!desc && snapKv) {
            const reqSubject = tenanted
              ? `${this.config.grammar.subjects.snapshot_request}.${tenantForTable}.${table}`
              : `${this.config.grammar.subjects.snapshot_request}.${table}`;

            // PROTOCOL.md §6 "no answer at all": JetStream publish (503 = already
            // queued, which is what a retry needs to know), bounded wait, re-request.
            for (let attempt = 1; attempt <= SNAPSHOT_REQUEST_ATTEMPTS && !desc; attempt++) {
              try {
                await js.publish(reqSubject, new Uint8Array(0));
                this.appendLog('SYS', `Snapshot requested for ${table} (attempt ${attempt}/${SNAPSHOT_REQUEST_ATTEMPTS})`, 'INFO');
              } catch (e: any) {
                this.appendLog('SYS', `Request for ${table} refused (${e?.message ?? e}) — a snapshot is already pending, waiting for it`, 'INFO');
              }
              desc = await waitForDescriptor(js, this.config.grammar.kv.snapshots, snapKey, SNAPSHOT_WAIT_MS, rejectedId);
              if (!desc) {
                this.appendLog('SYS', `No snapshot for ${table} after ${SNAPSHOT_WAIT_MS / 1000}s — the request may have expired unread; re-requesting`, 'WARNING');
              }
            }

            if (!desc) {
              // Unseeded is NOT the same as synced: following CDC against an unseeded
              // table diverges silently — strictly worse than being visibly absent.
              //
              // ⚠️ Note what this does NOT do: fall back to the descriptor rejected
              // above. "No fresh snapshot available" must never degrade into "replay
              // a stale one", because replaying begins with DELETE FROM — the answer
              // to not knowing is to keep what we have and be loud, never to rewind
              // (NOTES §10g).
              this.syncedTables.delete(table);
              this.failed.add(table);
              this.scheduleRecount();
              this.appendLog('SYS', `Giving up on ${table} after ${SNAPSHOT_REQUEST_ATTEMPTS} attempts — NOT following CDC for it, the local copy would silently diverge. If the table was added to the publication after the bridge started, restart the bridge.`, 'ERROR');
            }
          }

          if (desc) {
            this.appendLog('SYS', `Snapshot metadata ready for ${table} (LSN ${desc.lsn}, ${desc.row_count ?? '?'} rows). Replaying...`, 'INFO');
            this.reach('snapshot');
            await this.replaySnapshot(js, jsm, table, desc, tenanted, tenantForTable);
          }
        })().catch((e) => {
          // One table's failure is one table's failure.
          this.failed.add(table);
          this.appendLog('SYS', `Seeding ${table} failed: ${e}`, 'ERROR');
        }));

        await Promise.all(seedPromises);

        // ⚠️ Re-arm referential integrity, and CHECK what the bulk load produced.
        // Seeding runs every table CONCURRENTLY (the Promise.all above), so with
        // enforcement on, a child's rows hit the constraint before its parent's have
        // landed — measured: `orders` seeded from a chain holding 6,500 rows applied
        // NOTHING, silently, because `users` was still loading beside it. A seed is a
        // bulk load of an ALREADY-CONSISTENT snapshot: enforcing order during it is
        // both unnecessary and actively wrong. The standard SQLite bulk-load shape —
        // off during load, on after, then verify.
        try {
          await this.run(`PRAGMA foreign_keys = ON;`);
          const bad = await this.run(`PRAGMA foreign_key_check;`);
          if (bad?.length) {
            this.appendLog('SYS', `⚠️ ${bad.length} foreign key violation(s) survive seeding — the seeded set is not self-consistent`, 'ERROR');
          }
        } catch { /* engine without the pragma */ }
        if (this.failed.size > 0) {
          this.appendLog('SYS', `Seeding done, but ${this.failed.size} table(s) could not be seeded and are excluded: ${[...this.failed].join(', ')}`, 'WARNING');
        } else {
          this.appendLog('SYS', `All required tables seeded successfully!`, 'INFO');
        }
      }
    } catch (e) {
      this.appendLog('SYS', `Failed to resolve gap and seed tables: ${e}`, 'ERROR');
    }

    // 2. CDC consumers, ONLY AFTER seeding is resolved — one consumer per stream,
    // because a consumer belongs to exactly one stream and the stream is the ACL
    // boundary. No filter: narrowing here would re-introduce a reader-chosen boundary.
    try {
      for (const streamName of this.cdcStreams()) {
        const setupStart = performance.now();
        const last = this.globalSyncState.seq[streamName] ?? 0;
        const ci = await jsm.consumers.add(streamName, {
          deliver_policy: last > 0 ? DeliverPolicy.StartSequence : DeliverPolicy.All,
          opt_start_seq: last > 0 ? last + 1 : undefined,
        });
        const consumer = await js.consumers.get(streamName, ci.name);
        const setupMs = Math.round(performance.now() - setupStart);
        this.appendLog('SYS', `CDC consumer on ${streamName} (from seq ${last || 'all'}, ${ci.num_pending ?? '?'} messages pending, consumer setup took ${setupMs}ms)`, 'INFO');

        const iter = await consumer.consume();
        void (async () => {
          let processedSinceStart = 0;
          let caughtUpLogged = ci.num_pending === 0;
          const progressEvery = 2000;

          // Batched into ONE transaction per flush: N autocommits each pay OPFS
          // commit/fsync, one transaction of N pays it once. Messages are acked only
          // after their batch's transaction committed.
          const BATCH_SIZE = 100;
          const BATCH_MS = 200;
          let batch: { table: string; ev: any }[] = [];
          let batchMsgs: any[] = [];
          let flushTimer: ReturnType<typeof setTimeout> | null = null;

          const flushBatch = async () => {
            if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
            if (!batch.length) return;
            const toApply = batch;
            const toAck = batchMsgs;
            batch = [];
            batchMsgs = [];
            try {
              await this.transaction(async (txExec) => {
                // PROTOCOL.md §4's FK rule in executable form: enforcement waits for
                // this batch's COMMIT, so a child arriving before its parent inside
                // one batch cannot fail the apply.
                await txExec(`PRAGMA defer_foreign_keys = ON;`);
                for (const { table, ev } of toApply) {
                  await this.applyEvent(table, ev, txExec);
                }
              });
            } catch (err) {
              // ⚠️ This used to log and fall through to the ack below, so ONE bad event
              // silently discarded the other 99 and the replica still reported itself
              // caught up. Measured 2026-08-26: six batches × 100 events dropped that
              // way during the Node-consumer work, from a single adapter bug.
              //
              // Isolate instead: replay the batch ONE EVENT AT A TIME, each in its own
              // transaction, so only the genuinely bad event is affected.
              await this.applyBatchIsolated(streamName, toApply, String(err));
            }
            // Held events are accounted for (retried after later batches), so acking
            // here is correct — what must never happen again is acking events that
            // were neither applied nor held.
            for (const m of toAck) m.ack();
            await this.retryFkHeld(streamName);
          };

          for await (const msg of iter) {
            processedSinceStart++;
            if (processedSinceStart % progressEvery === 0) {
              this.appendLog('SYS', `${streamName} catch-up: ${processedSinceStart} messages processed so far, at seq ${msg.seq}`, 'INFO');
            }
            if (!caughtUpLogged && ci.num_pending != null && processedSinceStart >= ci.num_pending) {
              caughtUpLogged = true;
              const totalMs = Math.round(performance.now() - setupStart);
              this.appendLog('SYS', `${streamName} caught up (${processedSinceStart} messages, ${totalMs}ms total since consumer setup started) — now live`, 'INFO');
            }
            let decoded: any;
            try {
              decoded = decode(msg.data); // CDC events are always msgpack
            } catch (err) {
              // Caught, not thrown: an uncaught throw in this fire-and-forget IIFE
              // silently ends CDC for the whole stream.
              this.appendLog('SYS', `CDC event on ${streamName} failed to decode (seq ${msg.seq}): ${err} — skipping this message`, 'ERROR');
              msg.ack();
              continue;
            }
            const events = Array.isArray(decoded) ? decoded : [decoded];

            for (const ev of events) {
              ev.seq = msg.seq;
              ev.stream = streamName;
              // cdc.<tenant>.<table>.<op> has the table at [2]; cdc.<table>.<op> at [1].
              const parts = msg.subject.split('.');
              const table = ev?.table || (parts.length >= 4 ? parts[2] : parts[1]);
              this.appendLog(msg.subject, ev, ev?.operation || 'CDC');
              if (table) batch.push({ table, ev });
            }
            batchMsgs.push(msg);

            if (batch.length >= BATCH_SIZE) {
              await flushBatch();
            } else if (!flushTimer) {
              flushTimer = setTimeout(() => { void flushBatch(); }, BATCH_MS);
            }
          }
          await flushBatch();
        })();
      }
    } catch (e) {
      this.appendLog('SYS', `Failed to start CDC consumer: ${e}`, 'ERROR');
    }
  }

  /// Replay one snapshot's chunks off the INIT stream. Failures exclude the table
  /// (same "unseeded is not synced" rule) but never abandon other tables' replays.
  private async replaySnapshot(js: any, jsm: any, table: string, desc: any, tenanted: boolean, tenantForTable: string): Promise<void> {
    try {
      // ⚠️ A SNAPSHOT MUST NEVER MOVE THIS TABLE BACKWARDS.
      //
      // Replaying starts with DELETE FROM, so a stale descriptor does not merely
      // fail to help — it DESTROYS a correct replica. Measured in a clean room: a
      // populated `orders` (5,000 rows) was emptied by replaying a descriptor taken
      // when the table was empty. The path there is entirely ordinary: the client
      // judged its cached snapshot orphaned, asked for a fresh one, the SNAP_RET
      // throttle REFUSED it ("maximum messages per subject exceeded — a snapshot is
      // already pending"), and it fell back to the pending descriptor, which was
      // older than its own position. 0 rows applied, 5,000 rows gone.
      //
      // The rule is finding 4's, on the client side: resume forward, never rewind.
      // A snapshot older than data we already hold is not a baseline, it is a
      // regression. Comparing against `state.lsn` alone will not do — on a FRESH
      // replica that starts far ahead of any descriptor (see the gate in
      // applyEvent) — so the test is "do we actually hold rows this descriptor
      // predates?". An empty table has nothing to lose and always seeds.
      const descLsn = typeof desc?.lsn === 'number' ? desc.lsn : 0;
      const held = (await this.run(`SELECT COUNT(*) AS n FROM ${table}`))?.[0]?.n ?? 0;
      if (held > 0 && descLsn > 0 && descLsn < this.globalSyncState.lsn) {
        this.appendLog(
          'SYS',
          `REFUSED a stale snapshot for ${table}: descriptor @ lsn ${descLsn} is behind this replica (${this.globalSyncState.lsn}) ` +
            `and the table holds ${held} row(s) — replaying it would DELETE them and restore an older state. Keeping what we have; CDC carries on.`,
          'ERROR',
        );
        return;
      }

      await this.run(`DELETE FROM ${table}`);

      const stream = tenanted ? this.initStream(tenantForTable) : this.config.grammar.streams.init;
      const filterSubject = tenanted
        ? `${this.config.grammar.subjects.init_prefix}.snap.${tenantForTable}.${table}.${desc.snapshot_id}.>`
        : `${this.config.grammar.subjects.init_prefix}.snap.${table}.${desc.snapshot_id}.>`;
      const ci = await jsm.consumers.add(stream, {
        filter_subject: filterSubject,
        deliver_policy: DeliverPolicy.All,
      });
      const replayConsumer = await js.consumers.get(stream, ci.name);

      let done = false;
      let snapshotColumns: string[] | null = null;
      let rowsApplied = 0;

      while (!done) {
        // 5s, not 1s: a tighter window missed the first pull's heartbeat on a
        // freshly-created consumer (measured).
        const batch = await replayConsumer.fetch({ max_messages: 100, expires: 5000 }).catch(() => null);
        if (!batch) break;

        let receivedCount = 0;
        for await (const msg of batch) {
          receivedCount++;
          let chunkDecoded: any;
          try {
            chunkDecoded = decode(msg.data); // snapshot chunks are always msgpack
          } catch (err) {
            this.appendLog('SYS', `Snapshot chunk for ${table} failed to decode (seq ${msg.seq}): ${err} — skipping this message`, 'ERROR');
            msg.ack();
            continue;
          }

          const state = this.syncedTables.get(table);

          if (chunkDecoded && typeof chunkDecoded === 'object' && Array.isArray(chunkDecoded.schema)) {
            snapshotColumns = chunkDecoded.schema;
            this.appendLog('SYS', `Received snapshot schema for ${table}: ${snapshotColumns!.join(', ')}`, 'INFO');
          } else if (state && Array.isArray(chunkDecoded)) {
            const cols = snapshotColumns || state.columns;
            for (const rowVals of chunkDecoded) {
              const rowObj: any = {};
              cols.forEach((col: string, i: number) => { rowObj[col] = rowVals[i]; });
              await this.applyEvent(table, { table, operation: 'INSERT', data: rowObj, lsn: desc.lsn }, this.run, true);
              rowsApplied++;
            }
          } else if (state && chunkDecoded.operation === 'snapshot' && chunkDecoded.data) {
            for (const row of chunkDecoded.data) {
              await this.applyEvent(table, { table, operation: 'INSERT', data: row, lsn: desc.lsn }, this.run, true);
              rowsApplied++;
            }
          }
          msg.ack();
        }

        if (receivedCount === 0) done = true;
      }

      const state = this.syncedTables.get(table);
      if (state) {
        state.lsn = desc.lsn;
        await this.pruneInboxSeeded(table, desc.lsn);
      }
      const rowCountNote = desc.row_count != null && desc.row_count !== rowsApplied
        ? ` ⚠️ expected ${desc.row_count} rows per the descriptor`
        : '';
      this.appendLog('SYS', `Replay finished for ${table} (Snapshot ID: ${desc.snapshot_id}, ${rowsApplied} rows applied${rowCountNote})`, 'INFO');
    } catch (err) {
      this.syncedTables.delete(table);
      this.failed.add(table);
      this.scheduleRecount();
      this.appendLog('SYS', `Replay of ${table} failed: ${err} — NOT following CDC for it, the local copy would silently diverge.`, 'ERROR');
    }
  }

  // ── the write path (PROTOCOL.md §7) ───────────────────────────────────────

  private async watchVerdicts() {
    if (!this.nc) return;
    const sub = this.nc.subscribe(`mutation_ack.${this.config.principal}.>`);
    void (async () => {
      for await (const m of sub) {
        const prefix = `mutation_ack.${this.config.principal}.`;
        const msgId = m.subject.startsWith(prefix) ? m.subject.slice(prefix.length) : m.subject;
        const verdict = JSON.parse(new TextDecoder().decode(m.data));
        const pending = this.pendingWrites.get(msgId);
        const where = pending ? `${pending.table}#${pending.id}` : '(not from this session)';

        // 'failed' is the ONE status that is not definitive (§7.1: keep and retry).
        const definitive = verdict.status !== 'failed';
        if (definitive) this.pendingWrites.delete(msgId);
        if (definitive && verdict.status !== 'rejected' && verdict.status !== 'row_deleted') {
          void this.outboxDrop(msgId);
        }

        switch (verdict.status) {
          case 'accepted':
            // Nothing applied here: state arrives over CDC, never from a verdict.
            if (verdict.reason === 'version_clamped') {
              this.appendLog(m.subject, `${where}: accepted, but the version was clamped to ${verdict.version} — this client's clock is ahead of the database`, 'WARNING');
            } else {
              this.appendLog(m.subject, { ...verdict, write: where }, 'VERDICT');
            }
            break;
          case 'stale':
            // Pop and do NOT hand-revert: the winning row arrives via CDC.
            this.appendLog(m.subject, `${where}: a newer version won — dropping this edit, the winning row arrives via CDC`, 'INFO');
            break;
          case 'row_deleted':
            // Nothing is coming via CDC to correct this one — revert by hand (§1.6d).
            void this.revertOptimisticWrite(msgId, 'delete');
            this.appendLog(m.subject, `${where}: the row was deleted elsewhere, so this edit cannot be applied — reverting the local copy. Surface this to the user rather than dropping it silently.`, 'ERROR');
            break;
          case 'rejected':
            void this.revertOptimisticWrite(msgId, 'restore');
            this.appendLog(m.subject, `${where}: refused permanently (${verdict.reason}${verdict.sqlstate ? ` / SQLSTATE ${verdict.sqlstate}` : ''}) — ${verdict.detail || 'no detail'} — reverting the local copy`, 'ERROR');
            break;
          case 'failed':
            this.appendLog(m.subject, `${where}: failed after the bridge's delivery limit (${verdict.reason}) — kept for retry`, 'WARNING');
            break;
          default:
            this.pendingWrites.set(msgId, pending ?? { table: '?', id: '?', at: Date.now() });
            this.appendLog(m.subject, { ...verdict, write: where, note: 'unknown status — kept pending' }, 'ERROR');
        }
      }
    })();
  }

  private sweepPendingWrites() {
    const now = Date.now();
    for (const [msgId, w] of this.pendingWrites) {
      if (now - w.at < WRITE_TIMEOUT_MS) continue;
      this.pendingWrites.delete(msgId);
      // No echo and no verdict: report as unconfirmed, not denied — the two are
      // indistinguishable from here and only one is the client's fault.
      this.appendLog(msgId, `no echo and no verdict within ${WRITE_TIMEOUT_MS}ms — unconfirmed (${w.table})`, 'ERROR');
    }
  }

  /// The low-level write: publish one mutation and record it as pending. Public as an
  /// escape hatch for demos that deliberately send broken payloads (no key, wrong
  /// grants) — mutate() is the blessed path and builds the payload correctly.
  public async rawMutation(table: string, op: string, id: string | number, version: string, payload: Record<string, unknown>) {
    if (!this.nc) return;

    // A suspended table has no CDC path: an optimistic write here would never get a
    // confirming or correcting echo — permanently. Refused client-side.
    const suspendReason = this.suspendedMap.get(table);
    if (suspendReason) {
      this.appendLog(table, `write refused: table is suspended upstream (${suspendReason}) — no CDC echo would ever confirm it`, 'ERROR');
      return;
    }

    const subject = `mutation.${this.config.principal}.${table}.${op.toLowerCase()}`;
    // Dots stripped: the msg_id becomes subject tokens on mutation_ack, and the
    // version carries fractional seconds. The version stays IN the id: a second edit
    // to the same row is a different write; a retry of the same edit is not.
    const subjectSafe = (v: string) => v.replace(/[.*>\s]/g, '-');
    const msgId = subjectSafe(`${this.clientIdValue}-${table}-${id}-${version}`);
    const h = headers();
    h.set('Nats-Msg-Id', msgId);
    this.pendingWrites.set(msgId, { table, id, at: Date.now() });

    // Outbox insert and optimistic apply in ONE transaction (§7.1), persisted BEFORE
    // the publish: a duplicate is collapsed by dedup, a loss is unrecoverable.
    try {
      await this.transaction(async (txExec) => {
        const state = this.syncedTables.get(table);
        let before: any = null;
        if (state?.pkCols.length) {
          const keyObj = (payload as any).key as Record<string, unknown> | undefined;
          if (keyObj) {
            const where = state.pkCols.map((c) => `"${c}" = ?`).join(' AND ');
            const pkVals = state.pkCols.map((c) => keyObj[c]);
            const existing = await txExec(`SELECT * FROM ${table} WHERE ${where}`, ...pkVals);
            before = existing[0] ?? null;
          }
        }
        await this.outboxPut({ msgId, subject, payload, table, id, before }, txExec);
        await this.applyEvent(table, {
          table,
          operation: op,
          data: op === 'DELETE' ? (payload as any).key : (payload as any).data,
          lsn: Number.MAX_SAFE_INTEGER,
          optimistic: true,
        }, txExec);
      });
    } catch (err) {
      this.pendingWrites.delete(msgId);
      this.appendLog(subject, `optimistic apply failed, write not sent: ${err}`, 'ERROR');
      return;
    }

    // JetStream publish, not core: the PubAck proves durability (not application —
    // the verdict/echo decide that), and `duplicate: true` is a success.
    try {
      const ack = await jetstream(this.nc).publish(subject, encode(payload), { headers: h });
      this.appendLog(subject, { ...payload, _ack: { seq: ack.seq, duplicate: ack.duplicate } }, 'MUTATION OUT');
    } catch (err) {
      this.appendLog(subject, `not accepted by JetStream: ${err}`, 'ERROR');
    }
  }

  /// Republish everything still in the outbox — what makes it an outbox rather than a
  /// log. Safe to run repeatedly: original msg ids make replays idempotent. Entries
  /// are only popped by a verdict or a CDC echo (§7.1), never on send failure.
  public async flushOutbox() {
    let rows: any[] = [];
    try {
      rows = await this.outboxAll();
    } catch (err) {
      this.appendLog('OUTBOX', `could not be read: ${err}`, 'ERROR');
      return;
    }
    if (!rows.length) return;
    this.appendLog('OUTBOX', `replaying ${rows.length} unconfirmed write(s)`, 'INFO');
    for (const r of rows) {
      if (!this.nc) return;
      try {
        const h = headers();
        h.set('Nats-Msg-Id', r.msg_id);
        this.pendingWrites.set(r.msg_id, { table: r.tbl, id: r.row_id, at: Date.now() });
        const ack = await jetstream(this.nc).publish(r.subject, encode(JSON.parse(r.payload)), { headers: h });
        this.appendLog('OUTBOX', `replayed ${r.msg_id} (seq ${ack.seq}${ack.duplicate ? ', duplicate — already landed' : ''})`, 'INFO');
      } catch (err) {
        this.appendLog('OUTBOX', `replay of ${r.msg_id} failed, kept for next connection: ${err}`, 'ERROR');
      }
    }
  }

  /// A real PING against a possibly-lying transport: a frozen server can leave the
  /// WebSocket believing it is open with no 'disconnect' ever fired. Acts only on
  /// transitions — the recovery transition is the one nc.status() might never report.
  private async pollNatsRtt() {
    if (!this.nc) return;
    try {
      await Promise.race([
        this.nc.rtt(),
        new Promise<never>((_, reject) => setTimeout(() => reject(new Error('rtt timeout')), 3000)),
      ]);
      if (!this.naturallyConnected) {
        this.naturallyConnected = true;
        this.emitStatus('connected');
        this.appendLog('SYS', 'NATS rtt check recovered — re-syncing (nc.status() never reported this)', 'INFO');
        if (!this.resyncing) {
          this.resyncing = true;
          void this.flushOutbox();
          void this.subscribeStreams().finally(() => { this.resyncing = false; });
        }
      }
    } catch (err) {
      if (this.naturallyConnected) {
        this.naturallyConnected = false;
        this.emitStatus('disconnected');
        this.appendLog('SYS', `NATS rtt check failed — connection is not actually answering: ${err}`, 'WARNING');
      }
    }
  }
}
