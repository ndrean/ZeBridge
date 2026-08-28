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

import { natsTransport } from './transport.ts';
import type { Transport, TransportConnection } from './transport.ts';
import { decode, encode } from '@msgpack/msgpack';
import type { Storage, StorageFactory, Exec as StorageExec } from './storage.ts';
import { browserStorage } from './browser-storage.ts';
import { v7 as uuidv7 } from 'uuid';
import {
  seedGateDrops, planFromManifest, fullPredatesReplica as coreFullPredates,
  scopeSeeding, advancePosition, foreignKeyFailureKind, lsnToNumber, pgTsToWire,
  planKeyChange, planUpsert, planDelete, chainUpsertSql, chainRowParams,
  fkClausesFor, createTableSteps, rebuildSteps, diffColumns,
  mutationSubject, mutationMsgId, mutationKeyId, mutationPayload, optimisticEvent,
  normalizeVersion, maxVersion, hlcVersion,
  fkTextDiffers, viewSteps, indexSyncPlan, outboxWatermarkGate,
} from './core.ts';
import type { PlanStep } from './core.ts';

export interface ZeBridgeConfig {
  /// Replace the whole wire layer (NATS factories, headers, wire constants) —
  /// the transport seam (transport.ts). Default: the @nats-io libraries.
  transport?: Transport;
  natsUrl: string;
  principal: string;
  password?: string;
  /// Chain objects may arrive as zstd frames (detected by the 4-byte magic —
  /// no manifest field, so mixed old/new chains keep working). Node decodes
  /// via node:zlib automatically; a BROWSER host must supply this hook (the
  /// web consumer passes fzstd's decompress). Absent where needed, seeding
  /// fails LOUDLY naming the fix — never by feeding zstd bytes to msgpack.
  zstdDecompress?: (b: Uint8Array, dict?: Uint8Array) => Uint8Array | Promise<Uint8Array>;
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
  connect?: (opts: any) => Promise<TransportConnection>;
}

export type TableState = {
  pkCols: string[];
  columns: string[];
  tombstoneColumn: string | null;
  tenantColumn: string | null;
  /// The table's LWW version column (from the schema payload) — read to feed
  /// the HLC floor; the guard itself runs in SQL and in PG.
  versionColumn?: string | null;
  lsn: number;
  /// The seed gate's PRIMARY anchor (finding 7, NOTES §10i): the CDC stream's
  /// last_seq captured by the producer AT CHAIN BUILD TIME, and which stream it
  /// belongs to. Stream sequence is commit-ordered and monotonic — lsn is NOT
  /// (a transaction that begins early and commits late delivers late with a
  /// LOWER lsn), so gating on lsn silently dropped in-flight transactions.
  seedSeq?: number;
  seedStream?: string;
  /// FINDING 10: the lsn fallback gate must compare against a lsn that a SEED set —
  /// never `state.lsn`, which the boot schema-republish advances to the WAL head, so
  /// after any bridge restart every replayed data event carried an older lsn and was
  /// dropped as "already seeded" (measured: two rows consumed, accounted, and absent).
  seedLsn?: number;
};

/// 'snapshot' means "seeded" — the name predates the retirement of
/// snapshot-on-demand and is kept for UI compatibility.
export type Phase = 'connected' | 'migrated' | 'snapshot' | 'cdc';
export type ConnStatus = 'connected' | 'disconnected' | 'connecting';

interface BucketEntry {
  key: string;
  operation: 'PUT' | 'DEL' | 'PURGE';
  value: Uint8Array;
  delta: number;
}

/// Snapshot-on-demand seeding is DELETED (2026-08-27, NOTES §10p) — generations
/// are the only seed path. Its three findings (a stale descriptor that DELETEd
/// 5,000 correct rows, the SNAP_RET throttle deadlock, head-of-line blocking)
/// live in NOTES §10g/§10h.
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
    deliver_policy: 'last_per_subject', // NATS wire constant (transport.DELIVER_POLICY)
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

// pgTsToWire and lsnToNumber live in core.ts (§10s).
const td = new TextDecoder();
type Exec = (q: string, ...params: any[]) => Promise<any[]>;

/// Assemble a .creds file's text from a JWT and a seed — what the enrollment
/// flow holds after the mint responds (the seed never crossed the wire; the app
/// generated the pair itself).
// foreignKeyFailureKind lives in core.ts (§10s) — the three measured SQLite messages.

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

/// The version an outbox entry carries, dug out of the envelope it stored.
///
/// The payload is the mutation envelope as JSON (`{version, key, data}`), so the
/// version is a field of it rather than a column — the outbox table deliberately does
/// not duplicate it, and a copy would be one more thing to drift.
function outboxVersionOf(row: { payload?: string }): string | null {
  try {
    const v = JSON.parse(row.payload ?? '').version;
    return typeof v === 'string' && v.length ? v : null;
  } catch {
    return null;
  }
}

export class ZeBridge {
  public readonly dbName: string;
  public sql: StorageExec;
  public transaction: Storage['transaction'];
  public deleteDatabaseFile: Storage['deleteDatabaseFile'];

  private nc: TransportConnection | null = null;
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
  private readonly transport: Transport;
  private lastVersion = '';
  private dictCache = new Map<string, Uint8Array>();
  /// The HLC floor (§10q): the newest version this client has OBSERVED —
  /// CDC events' version column, chain cutoff_version. newVersion() stamps
  /// strictly above it, so a slow clock cannot lose to a row already seen.
  private hlcFloor = '';

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
    this.transport = config.transport ?? natsTransport;
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
    const id = mutationKeyId(state.pkCols, key);
    const payload = mutationPayload(op, key, values, version, this.clientIdValue);
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
    // core.hlcVersion (§10q): the wall clock floored by the newest version
    // seen arriving — a slow clock lifts to just past the observed floor, and
    // arrival time never becomes the comparator (that would punish offline).
    this.lastVersion = hlcVersion(new Date().toISOString(), this.lastVersion, this.hlcFloor);
    return this.lastVersion;
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

      const dial = this.config.connect ?? this.transport.connect;
      this.nc = await dial({
        servers: this.config.natsUrl,
        ...(this.config.creds
          ? { authenticator: this.transport.credsAuthenticator(new TextEncoder().encode(this.config.creds)) }
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
    await this.run(`CREATE TABLE IF NOT EXISTS _zebridge_dicts (name TEXT PRIMARY KEY, bytes BLOB NOT NULL)`); // §10x dictionary cache
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

  /// The one row of `zebridge_gc_watermark`, read from THIS client's own replica.
  ///
  /// No extra subscription and no request path: the table arrives over CDC like any
  /// other, which is the whole point of publishing it (§10as). Returns null whenever
  /// the answer is not known — the table is not replicated here, has no row yet, or
  /// the read failed — and `outboxWatermarkGate` treats null as "refuse nothing".
  ///
  /// ⚠️ Deployments exist where this table is absent (it is only in a publication if
  /// the DBA put it there), so a missing table is an ordinary state, not an error.
  public async gcWatermark(): Promise<string | null> {
    try {
      const rows = await this.run(`SELECT watermark FROM zebridge_gc_watermark LIMIT 1`);
      const w = rows?.[0]?.watermark;
      return typeof w === 'string' && w.length ? w : null;
    } catch {
      return null;
    }
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
        const watch = await watchBucket(this.transport.jetstream(this.nc!), this.config.grammar.kv.schemas);
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
    const versionColumn: string | null = typeof val.version_column === 'string' ? val.version_column : null;
    const names = cols.map((c) => c.name);

    // ── FINDING 9 (the destroyer behind §10j): "first sight" must be decided by
    // the DATABASE, not this in-memory map. `syncedTables` is empty in every fresh
    // process, so a durable replica's every reconnect took the "first sight" path —
    // DROP TABLE + CREATE — wiping the data while the durable bookkeeping
    // (watermarks, stream positions) survived and testified that everything was
    // fine. Measured in a clean room: every run logged `created (first sight)` for
    // every table; a reconnect with no gap left users=0/salaries=0 because nothing
    // re-seeded what the drop had just emptied; and one run kept its users only
    // because the salaries FOREIGN KEY blocked the DROP. This single defect is the
    // vanished-cx-users and the 3750-of-4500 of §10j.
    let existing = this.syncedTables.get(table);
    if (!existing) {
      try {
        const phys: any[] = (await this.run(`PRAGMA table_info("${table}")`)) ?? [];
        if (phys.length) {
          existing = {
            columns: phys.map((c: any) => c.name),
            pkCols: phys.filter((c: any) => c.pk > 0).sort((a: any, b: any) => a.pk - b.pk).map((c: any) => c.name),
            lsn: 0,
            tombstoneColumn: null,
            tenantColumn: null,
          };
        }
      } catch { /* engine without the pragma — behaves as before */ }
    }
    // core.diffColumns (§10s 2b): rename-aware — a hinted rename is neither
    // added nor removed; an unhinted one degrades to add+remove (§1.2).
    const { renames, added, removed } = diffColumns(
      existing ? existing.columns : null, names, (val.renamed ?? {}) as Record<string, string>);

    // DDL text and constraints come from core (columnDdl/fkClausesFor, §10s 2b).
    // SQLite has no ALTER TABLE ADD CONSTRAINT, so an FK change forces a rebuild
    // below while an index change stays a cheap CREATE/DROP.
    const fkClauses = fkClausesFor(foreignKeys);

    const rebuildPreservingData = async (why: string) => {
      // Schema surgery on an already-consistent copy: with foreign_keys ON, the
      // DROP of a referenced parent is refused outright (measured: users, blocked
      // by salaries' FK). Off for the surgery, back on after — the data is copied,
      // not changed.
      try { await this.run(`PRAGMA foreign_keys = OFF;`); } catch { /* no pragma */ }
      for (const st of rebuildSteps(table, cols, pkCols, foreignKeys, existing ? existing.columns : [])) {
        await this.run(st.sql, ...st.params);
      }
      try { await this.run(`PRAGMA foreign_keys = ON;`); } catch { /* no pragma */ }
      this.appendLog('SCHEMA', `${table}: rebuilt preserving common columns (${why}), lsn=${lsn}`, 'MIGRATE');
    };

    try {
      if (!existing) {
        for (const st of createTableSteps(table, cols, pkCols, foreignKeys)) {
          await this.run(st.sql, ...st.params);
        }
        this.appendLog('SCHEMA', `${table}: created (first sight), lsn=${lsn}`, 'MIGRATE');
      } else if (added.length === 0 && removed.length === 0 && renames.length === 0 &&
                 !(await this.foreignKeysDiffer(table, fkClauses))) {
        this.syncedTables.set(table, { pkCols, columns: names, lsn, tombstoneColumn, tenantColumn, versionColumn });
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

      for (const st of viewSteps(table, names)) await this.run(st.sql, ...st.params);

      // After every shape change, because a rebuild DROPs the table and takes its
      // indexes with it.
      await this.syncIndexes(table, indexes);

      this.syncedTables.set(table, { pkCols, columns: names, lsn, tombstoneColumn, tenantColumn, versionColumn });
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
      return fkTextDiffers(rows?.[0]?.sql ?? '', fkClauses); // the pure compare lives in core (§10s 2b)
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
      const plan = indexSyncPlan(table, (rows ?? []).map((r: any) => r.name), indexes);
      for (const d of plan.drops) {
        await this.run(d.sql, ...d.params);
        this.appendLog('SCHEMA', `${table}: dropped index (no longer published): ${d.sql}`, 'MIGRATE');
      }
      for (const c of plan.creates) await this.run(c.sql, ...c.params);
      if (plan.creates.length) {
        this.appendLog('SCHEMA', `${table}: ${plan.creates.length} index(es) created`, 'MIGRATE');
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
    // The decision is core.seedGateDrops — findings 7 and 10 as one pure rule
    // (seq-primary because commit-ordered; strict-< lsn fallback anchored only
    // by a seed), pinned executable in fixtures/core-fixtures.json.
    if (!seed && seedGateDrops(ev, state)) return;

    // Feed the HLC floor (§10q) from every arriving row's version column —
    // observed remote versions, never our own optimistic stamps.
    if (!ev.optimistic && state.versionColumn) {
      const seen = ev.data?.[state.versionColumn];
      if (typeof seen === 'string') {
        this.hlcFloor = maxVersion(this.hlcFloor, normalizeVersion(pgTsToWire(seen)));
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

      // The statements come from core.ts (§10s 2a): the key-change delete
      // (a changed PK arrives as an UPDATE with old.* — measured: the row
      // lived under both keys) and the idempotent upsert, fixture-pinned.
      const kc = planKeyChange(table, state.pkCols, ev.data);
      if (kc) {
        try {
          await exec(kc.sql, ...kc.params);
          this.appendLog('CDC', `Key change on ${table}: ${JSON.stringify(kc.oldKey)} → ${JSON.stringify(kc.newKey)}`, 'REKEY');
        } catch (err) {
          this.appendLog('SQLITE', `Key-change cleanup on ${table} failed: ${err}`, 'ERROR');
        }
      }

      const up = planUpsert(table, state.pkCols, ev.data);
      try {
        await exec(up.sql, ...up.params);
      } catch (err) {
        this.appendLog('SQLITE', `UPSERT on ${table} failed: ${err}`, 'ERROR');
      }
    } else if (op === 'DELETE') {
      // core.planDelete: null on a partial composite key — deleting on it would
      // match MORE rows than PostgreSQL did.
      const del = planDelete(table, state.pkCols, ev.data);
      if (del) {
        try {
          await exec(del.sql, ...del.params);
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
  ///
  /// ⚠️ Returns null when the table is tenant-scoped and this principal resolved NO
  /// tenant. That is a real state, not an error: `resolveTenant()` already logs
  /// "No tenant mapping for 'x' — public-only reads" when `$KV.tenants.<principal>`
  /// is absent. What used to happen next was that this returned `''` anyway, and the
  /// caller built the manifest key `'' + '.' + table` — so NATS refused
  /// `.counter_tenant` and the client reported `chain manifest unreadable: invalid
  /// key`, blaming the key syntax for a missing roster entry. Measured against the
  /// compose stack 2026-08-28, where `zebridge_user_tenants` is empty.
  private effectiveTenantFor(table: string): string | null {
    const state = this.syncedTables.get(table);
    if (state?.tenantColumn) return this.tenantValue || null;
    return this.config.grammar.open_tenant || this.tenantValue || null;
  }

  /// zstd frame magic: 28 B5 2F FD. Our msgpack docs always start with a map
  /// marker, so the sniff is unambiguous and needs no wire-format field.
  private async maybeZstd(b: Uint8Array, dict?: Uint8Array): Promise<Uint8Array> {
    if (b.length < 4 || b[0] !== 0x28 || b[1] !== 0xb5 || b[2] !== 0x2f || b[3] !== 0xfd) return b;
    if (this.config.zstdDecompress) return await this.config.zstdDecompress(b, dict);
    // Node default, written so a BROWSER tsconfig/bundler never sees the node
    // module: globalThis probe + Function-constructed dynamic import.
    const proc = (globalThis as any).process;
    if (proc?.versions?.node) {
      const dynImport = new Function('m', 'return import(m)') as (m: string) => Promise<any>;
      const zlib = await dynImport('node:zlib');
      return new Uint8Array(zlib.zstdDecompressSync(b, dict ? { dictionary: dict } : undefined));
    }
    throw new Error('chain object is zstd-compressed: pass config.zstdDecompress (e.g. fzstd.decompress in the browser)');
  }

  private async resolveTenant() {
    if (!this.nc) return;
    try {
      // allow_direct must be explicit: the KV open never asks the server, and the
      // grant covers ONLY the per-key Direct Get path (measured — App.tsx history).
      const kv = await this.transport.kv(this.nc, this.config.grammar.kv.tenants, { allow_direct: true });
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
    if (tenantForTable === null) {
      // Say what is actually wrong. A tenant-scoped table is unreadable by a
      // principal with no tenant — there is no chain to find, and no key that could
      // name one.
      this.appendLog(
        'SYS',
        `${table} is tenant-scoped and '${this.config.principal}' has no tenant ` +
          `(no ${this.config.grammar.kv.tenants}.${this.config.principal} entry) — skipping. ` +
          `Add the principal to zebridge_user_tenants.`,
        'WARN',
      );
      return false;
    }
    const key = `${tenantForTable}.${table}`;
    const readManifest = async (): Promise<any | null> => {
      try {
        const kv = await this.transport.kv(this.nc!, GEN.kv, { allow_direct: true });
        const entry = await kv.get(key);
        return entry ? JSON.parse(td.decode(entry.value)) : null; // manifest is JSON
      } catch (e) { this.appendLog('SYS', `${table}: chain manifest unreadable: ${e}`, 'ERROR'); return null; }
    };
    let manifest = await readManifest();
    if (!manifest?.full?.object) return false;

    let os: any;
    try { os = await this.transport.objectStore(this.nc, manifest.bucket); } catch (e) { this.appendLog('SYS', `${table}: chain bucket ${manifest.bucket} unreachable: ${e}`, 'ERROR'); return false; }
    // §10x: a delta names the dictionary it was compressed with. Fetched once
    // per era from the same bucket, kept in memory AND in _zebridge_dicts so a
    // reconnect does not re-download it; immutable by name, so never stale.
    const dictFor = async (name: string): Promise<Uint8Array> => {
      const cached = this.dictCache.get(name);
      if (cached) return cached;
      try {
        const rows = await this.run(`SELECT bytes FROM _zebridge_dicts WHERE name = ?`, name);
        if (rows?.[0]?.bytes) { const d = new Uint8Array(rows[0].bytes); this.dictCache.set(name, d); return d; }
      } catch { /* table absent on an older replica — fetch below */ }
      const d = await os.getBlob(name);
      if (!d) throw new Error(`dictionary ${name} missing from ${manifest.bucket}`);
      this.dictCache.set(name, d);
      try { await this.run(`INSERT OR REPLACE INTO _zebridge_dicts (name, bytes) VALUES (?, ?)`, name, d); } catch { /* best effort */ }
      return d;
    };
    const fetchDoc = async (name: string, dictName?: string): Promise<any | null> => {
      try {
        let blob = await os.getBlob(name);
        if (!blob) return null;
        const dict = dictName ? await dictFor(dictName) : undefined;
        blob = await this.maybeZstd(blob, dict); // §10w: sniffed by magic, mixed chains fine
        return decode(blob) as any; // objects are msgpack
      } catch (e) { this.appendLog('SYS', `${table}: chain object ${name} unreadable: ${e}`, 'ERROR'); return null; }
    };

    let watermark: string | null = null;
    try {
      const r = await this.run(`SELECT watermark FROM _zebridge_generations WHERE tbl = ?`, table);
      watermark = r[0]?.watermark ?? null;
    } catch { /* fresh replica */ }

    // The walk itself is core.planFromManifest; `watermark` is read at call
    // time (it resets to null on a mid-walk manifest re-read).
    const planFrom = (man: any) => planFromManifest(man, watermark);

    const applyPlan = async (plan: PlanStep[]): Promise<number | null> => {
      let applied = 0;
      for (const step of plan) {
        const doc = await fetchDoc(step.name, step.dict);
        if (!doc) return null; // pruned under us — caller re-reads
        const cols: string[] = doc.columns ?? [];
        if (!cols.length || !cols.every((c) => state.columns.includes(c))) {
          this.appendLog('SYS', `Generation ${step.name} for ${table} references columns the local schema lacks — falling back to snapshot`, 'WARNING');
          return null;
        }
        const vcol: string = doc.version_column ?? manifest.version_column;
        // core.chainUpsertSql: version-guarded when the object carries the
        // table's version column — LWW holds during seeding too.
        const q = chainUpsertSql(table, cols, state.pkCols,
          vcol && cols.includes(vcol) ? vcol : null);
        await this.transaction(async (txExec) => {
          // A full replaces the baseline wholesale; the DELETE shares the transaction
          // so a crash mid-apply cannot leave an empty table.
          if (step.kind === 'full') await txExec(`DELETE FROM ${table}`);
          for (const row of doc.rows) {
            await txExec(q, ...chainRowParams(row));
          }
        });
        applied += doc.rows.length;
      }
      return applied;
    };

    // D2's destruction guard is core.fullPredatesReplica (NOTES §10n/§10s);
    // this wrapper only supplies the stored position and the log line.
    const fullPredatesReplica = (man: any, plan: PlanStep[]): boolean => {
      const pos = this.globalSyncState.seq[man.cdc_stream] ?? 0;
      if (!coreFullPredates(man, plan, pos)) return false;
      this.appendLog('SYS', `${table}: chain g${man.gen} predates this replica (cutoff seq ${man.cutoff_seq} < applied ${pos} on ${man.cdc_stream}) — a full replay would destroy newer rows; waiting for a newer build`, 'WARNING');
      return true;
    };

    let plan = planFrom(manifest);
    if (fullPredatesReplica(manifest, plan)) return false;
    let applied = await applyPlan(plan);
    if (applied === null) {
      // Pruned between manifest read and fetch: re-read ONCE, restart from ITS full —
      // overlap, never a gap; a second failure falls back to the snapshot path.
      manifest = await readManifest();
      if (!manifest?.full?.object) return false;
      watermark = null;
      plan = planFrom(manifest);
      if (fullPredatesReplica(manifest, plan)) return false;
      applied = await applyPlan(plan);
      if (applied === null) return false;
    }

    state.lsn = lsnToNumber(manifest.cutoff_lsn);
    state.seedLsn = state.lsn; // the ONE place the legacy data gate may anchor to (finding 10)
    if (typeof manifest.cutoff_seq === 'number' && manifest.cutoff_seq > 0 && manifest.cdc_stream) {
      state.seedSeq = manifest.cutoff_seq;
      state.seedStream = manifest.cdc_stream;
    }
    // The chain's cutoff_version is an observed version watermark: floor the HLC
    // with it so a freshly seeded slow-clock client stamps above its own seed.
    if (typeof manifest.cutoff_version === 'string') {
      this.hlcFloor = maxVersion(this.hlcFloor, normalizeVersion(pgTsToWire(manifest.cutoff_version)));
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

  // ── the main orchestration: gap check → seed → CDC ────────────────────────

  private async subscribeStreams() {
    if (!this.nc) return;
    const js = this.transport.jetstream(this.nc);
    const jsm = await this.transport.jetstreamManager(this.nc);

    // 1. Gap detection — asked of EVERY stream this client reads: a gap in any of
    // them means missing rows, and checking only one looks like an empty table.
    try {
      const streamGaps: Record<string, { firstSeq: number; stored: number }> = {};
      for (const streamName of this.cdcStreams()) {
        const info = await jsm.streams.info(streamName);
        streamGaps[streamName] = {
          firstSeq: info.state.first_seq,
          stored: this.globalSyncState.seq[streamName] ?? 0,
        };
      }

      // D2 (NOTES §10n): seeding is SCOPED. A gap on one stream re-seeds only
      // the tables ROUTED to that stream; every other table resumes from its
      // stored position untouched — a mobile client reconnecting with one stale
      // stream must not rebuild its whole replica. A table with no generations
      // watermark has never been seeded at all (enabled between two connects,
      // or a brand-new replica) and seeds regardless of its stream's health.
      let seededBefore = new Set<string>();
      try {
        const rows: any[] = (await this.run(`SELECT tbl FROM _zebridge_generations`)) ?? [];
        seededBefore = new Set(rows.map((r: any) => r.tbl));
      } catch { /* fresh replica */ }
      const tableRoutes: Record<string, { route: string; seeded: boolean }> = {};
      for (const table of this.syncedTables.keys()) {
        // Same null as above: a tenant-scoped table this principal cannot route is
        // left out of the seeding decision entirely rather than routed to a stream
        // name built from an empty token.
        const t = this.effectiveTenantFor(table);
        if (t === null) continue;
        tableRoutes[table] = {
          route: this.cdcStreamForTenant(t),
          seeded: seededBefore.has(table),
        };
      }
      // The decision is core.scopeSeeding (D2, §10n) — gapped streams plus
      // never-seeded tables, everything else untouched.
      const scoped = scopeSeeding(streamGaps, tableRoutes);
      const gapDetail = scoped.gapped.map(
        (g) => `${g}: local ${streamGaps[g].stored}, stream first ${streamGaps[g].firstSeq}`);
      const tablesToSeed = new Set(scoped.tablesToSeed);
      const gap = tablesToSeed.size > 0;

      if (!gap) {
        this.appendLog('SYS', 'No CDC gap — resuming from stored positions, no seeding needed', 'INFO');
        this.reach('snapshot');
      } else {
        const untouched = this.syncedTables.size - tablesToSeed.size;
        this.appendLog('SYS',
          `${gapDetail.length ? `Gap detected or first run! ${gapDetail.join('; ')}. ` : ''}` +
          `Seeding ${tablesToSeed.size} table(s) [${[...tablesToSeed].join(', ')}]` +
          `${untouched > 0 ? `; ${untouched} table(s) resume untouched` : ''}`, 'WARNING');

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
          // Generations are the ONLY seeding path (NOTES.md §1.13, §10h): the
          // producer builds once on a cadence, every client catches up on deltas.
          if (await this.applyGenerations(table)) {
            this.reach('snapshot');
            return;
          }

          {
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
            this.appendLog('SYS', `Giving up on ${table}: no generation chain after ${GENERATION_WAIT_MS / 1000}s — NOT following CDC for it. Check the producer (GENERATIONS_ENABLED, its cadence, and the gen-<tenant> object store); the table seeds on the next connect once a chain exists. Snapshot-on-demand is gone (NOTES §10h/§10p).`, 'ERROR');
            return;
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
        // A stream that is fully consumed (or empty) delivers no message, so the
        // per-batch persist below never fires — record its CURRENT tail now, or a
        // quiet stream reads as `local 0` forever and re-seeds every reconnect.
        try {
          const sinfo = await jsm.streams.info(streamName);
          const tail = sinfo?.state?.last_seq ?? 0;
          if (tail > (this.globalSyncState.seq[streamName] ?? 0) && (this.globalSyncState.seq[streamName] ?? 0) === 0) {
            // Only when we hold NO position: a stored position must never jump
            // forward past unconsumed messages. Zero means "fresh or gated-all",
            // and in both cases seeding has just covered everything at or before
            // the tail we are about to record.
            this.globalSyncState.seq[streamName] = tail;
            await this.run(
              `INSERT INTO _zebridge_stream_seq (stream, last_seq) VALUES (?, ?)
               ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq`,
              streamName, tail,
            );
          }
        } catch { /* stream info unavailable — the per-batch persist still covers it */ }
        const last = this.globalSyncState.seq[streamName] ?? 0;
        const ci = await jsm.consumers.add(streamName, {
          deliver_policy: last > 0 ? this.transport.deliverPolicy.byStartSequence : this.transport.deliverPolicy.all,
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

            // ── ADVANCE THE STREAM POSITION FOR EVERY DELIVERED MESSAGE ──
            // It used to advance only inside applyEvent, whose early returns (the
            // seed gate, the FK hold, the schema hold) skipped it — so a stream whose
            // delivered events were all gated or held NEVER persisted a position,
            // read as `local 0` on the next connect, and forced a FULL RE-SEED on
            // every reconnect (measured in a clean room: run 1 applied everything
            // and stream_seq still lacked CDC_PUBLIC, because its one event was
            // correctly gate-dropped; run 2 then gap-detected and re-seeded with no
            // new data). Delivery + accounting IS the position: an applied event is
            // in the tables, a gated one is provably in the seeded chain, a held one
            // is durably in the inbox — none of them needs redelivery.
            const maxSeq = advancePosition(0, toAck.map((msg) => msg.seq ?? 0));
            if (maxSeq > (this.globalSyncState.seq[streamName] ?? 0)) {
              this.globalSyncState.seq[streamName] = maxSeq;
              try {
                await this.run(
                  `INSERT INTO _zebridge_stream_seq (stream, last_seq) VALUES (?, ?)
                   ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq`,
                  streamName, maxSeq,
                );
              } catch (e) {
                this.appendLog('SQLITE', `Failed to persist ${streamName} position: ${e}`, 'ERROR');
              }
            }
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

    // Subject and idempotency id come from core (§10s 2c): the version stays
    // IN the id — a second edit to the same row is a different write; a retry
    // of the same edit is not.
    const subject = mutationSubject(this.config.principal, table, op);
    const msgId = mutationMsgId(this.clientIdValue, table, id, version);
    const h = this.transport.headers();
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
        await this.applyEvent(table, optimisticEvent(table, op, payload), txExec);
      });
    } catch (err) {
      this.pendingWrites.delete(msgId);
      this.appendLog(subject, `optimistic apply failed, write not sent: ${err}`, 'ERROR');
      return;
    }

    // ⚠️ The GC watermark gates the FIRST send too, not only replays.
    //
    // Publishing straight from here left a hole: the gate lives in `flushOutbox`, so a
    // write's first send skipped it entirely and only a later replay was ever checked.
    // Found in the Zig port's first live test — the gate printed its refusal while the
    // row was already in PostgreSQL — and the same shape was here.
    //
    // Normally invisible, because a fresh write is stamped `now` and the watermark is
    // in the past. It bites exactly where it matters: a lagging clock, or a client
    // whose queued write is being sent for the first time long after it was made.
    const wm = await this.gcWatermark();
    if (outboxWatermarkGate([{ msgId, version: (payload as any)?.version ?? null }], wm).refuse.length) {
      await this.revertOptimisticWrite(msgId, 'restore');
      await this.outboxDrop(msgId);
      this.appendLog(
        subject,
        `${table}[${id}] predates the GC watermark (${wm}) and CANNOT be sent: its tombstone ` +
          `has been reaped, so sending it would resurrect a deleted row (PROTOCOL §MUST 6). ` +
          `The local copy has been reverted — this edit is lost.`,
        'ERROR',
      );
      return;
    }

    // JetStream publish, not core: the PubAck proves durability (not application —
    // the verdict/echo decide that), and `duplicate: true` is a success.
    try {
      const ack = await this.transport.jetstream(this.nc).publish(subject, encode(payload), { headers: h });
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

    // ── the GC watermark gate (PROTOCOL.md §MUST 6) ─────────────────────────
    //
    // Runs BEFORE the first publish, not per-entry inside the loop: one read of the
    // watermark for the whole flush, and an entry that must not be sent is never
    // sent even if a later one fails.
    const watermark = await this.gcWatermark();
    const gate = outboxWatermarkGate(
      rows.map((r: any) => ({ msgId: r.msg_id, version: outboxVersionOf(r) })),
      watermark,
    );
    if (gate.refuse.length) {
      const refused = new Set(gate.refuse);
      for (const r of rows.filter((x: any) => refused.has(x.msg_id))) {
        // Same handling as a `rejected` verdict, and for the same reason: this write
        // will never be sent, so the optimistic copy is a divergence. Restore the
        // before-image and say so — the user's edit is being dropped, and PROTOCOL
        // §SHOULD 2's rule (surface it, never silently discard) is exactly this case.
        await this.revertOptimisticWrite(r.msg_id, 'restore');
        await this.outboxDrop(r.msg_id);
        this.appendLog(
          'OUTBOX',
          `${r.tbl}[${r.row_id}] ${r.msg_id} was queued before the GC watermark ` +
            `(${watermark}) and CANNOT be sent: the tombstone that would have overruled ` +
            `it has been reaped, so sending it would resurrect a deleted row ` +
            `(PROTOCOL §MUST 6). The local copy has been reverted — this edit is lost.`,
          'ERROR',
        );
      }
      rows = rows.filter((x: any) => !refused.has(x.msg_id));
      if (!rows.length) return;
    }

    this.appendLog('OUTBOX', `replaying ${rows.length} unconfirmed write(s)`, 'INFO');
    for (const r of rows) {
      if (!this.nc) return;
      try {
        const h = this.transport.headers();
        h.set('Nats-Msg-Id', r.msg_id);
        this.pendingWrites.set(r.msg_id, { table: r.tbl, id: r.row_id, at: Date.now() });
        const ack = await this.transport.jetstream(this.nc).publish(r.subject, encode(JSON.parse(r.payload)), { headers: h });
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
