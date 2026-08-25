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

import { wsconnect, NatsConnection, headers } from '@nats-io/nats-core';
import { jetstream, jetstreamManager, DeliverPolicy } from '@nats-io/jetstream';
import { Kvm } from '@nats-io/kv';
import { Objm } from '@nats-io/obj';
import { decode, encode } from '@msgpack/msgpack';
import { SQLocal } from 'sqlocal';
import { v7 as uuidv7 } from 'uuid';

export interface ZeBridgeConfig {
  natsUrl: string;
  principal: string;
  password?: string;
  grammar: any;   // the parsed grammar.json — wire names only; the consumer imports and passes it
  durable?: boolean;
}

export type TableState = {
  pkCols: string[];
  columns: string[];
  tombstoneColumn: string | null;
  tenantColumn: string | null;
  lsn: number;
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
async function waitForDescriptor(js: any, bucket: string, key: string, timeoutMs: number): Promise<any | null> {
  let watch: Awaited<ReturnType<typeof watchBucket>> | null = null;
  try {
    watch = await watchBucket(js, bucket, key);
    return await Promise.race([
      (async () => {
        for await (const entry of watch.entries) {
          if (entry.operation === 'DEL' || entry.operation === 'PURGE') continue;
          try { return decode(entry.value); } catch { return JSON.parse(new TextDecoder().decode(entry.value)); }
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

export class ZeBridge {
  public readonly dbName: string;
  public sql: SQLocal['sql'];
  public transaction: SQLocal['transaction'];
  public deleteDatabaseFile: SQLocal['deleteDatabaseFile'];

  private nc: NatsConnection | null = null;
  private sqlocal: SQLocal;

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

  private sweepId?: ReturnType<typeof setInterval>;
  private rttIntervalId?: ReturnType<typeof setInterval>;
  private recountTimer?: ReturnType<typeof setTimeout>;

  constructor(private config: ZeBridgeConfig) {
    // A fresh OPFS file per load by default — the project's own clean-room dev
    // convention; `durable` opts into the stable per-principal name that makes the
    // outbox meaningful across reloads.
    this.dbName = config.durable
      ? `zebridge_${config.principal}.sqlite3`
      : `zebridge_${Date.now()}.sqlite3`;
    this.sqlocal = new SQLocal(this.dbName);
    this.sql = this.sqlocal.sql;
    this.transaction = this.sqlocal.transaction;
    this.deleteDatabaseFile = this.sqlocal.deleteDatabaseFile;
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

      this.nc = await wsconnect({
        servers: this.config.natsUrl,
        user: this.config.principal,
        pass: this.config.password,
        reconnect: true,
        maxReconnectAttempts: -1,
      });

      this.emitStatus('connected');
      this.reach('connected');
      this.appendLog('SYS', `Connected to NATS over WebSockets as '${this.config.principal}'`);
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

  private run: Exec = (q, ...params) => (this.sql as any)(q, ...params);

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

    await this.transaction(async (tx) => {
      const txExec: Exec = (q, ...p) => (tx.sql as any)(q, ...p);
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
    const cols: { name: string; type: string }[] = val.sqlite.columns;
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
    const ddl = (c: { name: string; type: string }) =>
      `"${c.name}" ${c.type}` +
      (isPk(c.name) ? ' NOT NULL' : '') +
      (inlinePk && c.name === pkCols[0] ? ' PRIMARY KEY' : '');
    const tableConstraint = pkCols.length > 1 ? `, PRIMARY KEY (${pkCols.map((c) => `"${c}"`).join(', ')})` : '';

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
      } else if (added.length === 0 && removed.length === 0 && renames.length === 0) {
        this.syncedTables.set(table, { pkCols, columns: names, lsn, tombstoneColumn, tenantColumn });
        this.reach('migrated');
        this.scheduleRecount();
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
        } catch (alterErr) {
          await rebuildPreservingData(`ALTER refused: ${alterErr}`);
        }
      }

      const EXCLUDE_FROM_VIEW = ['uid', 'inserted_at', 'updated_at', 'metadata'];
      const viewCols = names.filter((n) => !EXCLUDE_FROM_VIEW.includes(n)).map((n) => `"${n}"`).join(', ');
      await this.run(`DROP VIEW IF EXISTS ${table}_view;`);
      if (viewCols) await this.run(`CREATE VIEW ${table}_view AS SELECT ${viewCols} FROM ${table};`);

      this.syncedTables.set(table, { pkCols, columns: names, lsn, tombstoneColumn, tenantColumn });
      // Both registration paths mark the phase — a strip that lies is worse than none.
      this.reach('migrated');
      this.scheduleRecount();

      await this.drainPending(table);
    } catch (err) {
      this.appendLog('SCHEMA', `Applying schema for ${table} failed: ${err}`, 'ERROR');
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

  // ── CDC apply ──────────────────────────────────────────────────────────────

  /// `ev.optimistic` marks a synthetic event from our own mutate(): lsn is a sentinel
  /// so the gate never blocks it, and neither the echo-pop nor resume bookkeeping run.
  private async applyEvent(table: string, ev: any, exec: Exec = this.run) {
    const state = this.syncedTables.get(table);
    if (!state || !ev?.data) return;
    this.triggerChange(table, ev);

    // Strictly `<`, never `<=`: the first post-snapshot commit carries the watermark
    // LSN itself — skipping it loses exactly one row per snapshot (measured).
    // Re-applying is free (the upsert converges); skipping is permanent.
    if (ev.lsn < state.lsn) return;

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
    return [`${cfg.tenant_prefix}${this.tenantValue.toUpperCase()}`, cfg.public];
  }

  private cdcStreamForTenant(tenant: string): string {
    const cfg = this.config.grammar.cdc_streams;
    if (!cfg) return this.config.grammar.streams.cdc;
    const open = this.config.grammar.open_tenant || '_default';
    if (!tenant || tenant === open) return cfg.public;
    return `${cfg.tenant_prefix}${tenant.toUpperCase()}`;
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
    return `${cfg.tenant_prefix}${tenant.toUpperCase()}`;
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
      } catch { return null; }
    };
    let manifest = await readManifest();
    if (!manifest?.full?.object) return false;

    let os: any;
    try { os = await new Objm(this.nc).open(manifest.bucket); } catch { return false; }
    const fetchDoc = async (name: string): Promise<any | null> => {
      try {
        const blob = await os.getBlob(name);
        return blob ? (decode(blob) as any) : null; // objects are msgpack
      } catch { return null; }
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
        await this.transaction(async (tx) => {
          const txExec: Exec = (qq, ...p) => (tx.sql as any)(qq, ...p);
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

        for (const table of tablesToSeed) {
          const tenantForTable = tenanted ? this.effectiveTenantFor(table) : '';
          const snapKey = tenanted ? `${tenantForTable}.${table}` : table;

          // Delta generations first (NOTES.md §1.13): the producer builds once on a
          // cadence, every client catches up on deltas. Any "not this way" outcome
          // falls through to the snapshot request path unchanged.
          if (await this.applyGenerations(table)) {
            this.reach('snapshot');
            continue;
          }

          let desc: any = null;
          if (snapKv) {
            try {
              const entry = await snapKv.get(snapKey);
              if (entry) {
                const candidate = decode(entry.value); // descriptor is always msgpack
                if (await this.descriptorStillFresh(js, tenantForTable, candidate)) {
                  desc = candidate;
                } else {
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
              desc = await waitForDescriptor(js, this.config.grammar.kv.snapshots, snapKey, SNAPSHOT_WAIT_MS);
              if (!desc) {
                this.appendLog('SYS', `No snapshot for ${table} after ${SNAPSHOT_WAIT_MS / 1000}s — the request may have expired unread; re-requesting`, 'WARNING');
              }
            }

            if (!desc) {
              // Unseeded is NOT the same as synced: following CDC against an unseeded
              // table diverges silently — strictly worse than being visibly absent.
              this.syncedTables.delete(table);
              this.failed.add(table);
              this.scheduleRecount();
              this.appendLog('SYS', `Giving up on ${table} after ${SNAPSHOT_REQUEST_ATTEMPTS} attempts — NOT following CDC for it, the local copy would silently diverge. If the table was added to the publication after the bridge started, restart the bridge.`, 'ERROR');
            }
          }

          if (desc) {
            this.appendLog('SYS', `Snapshot metadata ready for ${table} (LSN ${desc.lsn}, ${desc.row_count ?? '?'} rows). Replaying...`, 'INFO');
            this.reach('snapshot');
            seedPromises.push(this.replaySnapshot(js, jsm, table, desc, tenanted, tenantForTable));
          }
        }

        await Promise.all(seedPromises);
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
              await this.transaction(async (tx) => {
                const txExec: Exec = (q, ...p) => (tx.sql as any)(q, ...p);
                // PROTOCOL.md §4's FK rule in executable form: enforcement waits for
                // this batch's COMMIT, so a child arriving before its parent inside
                // one batch cannot fail the apply.
                await txExec(`PRAGMA defer_foreign_keys = ON;`);
                for (const { table, ev } of toApply) {
                  await this.applyEvent(table, ev, txExec);
                }
              });
            } catch (err) {
              this.appendLog('SYS', `${streamName} batch of ${toApply.length} event(s) failed to apply: ${err}`, 'ERROR');
            }
            for (const m of toAck) m.ack();
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
              await this.applyEvent(table, { table, operation: 'INSERT', data: rowObj, lsn: desc.lsn });
              rowsApplied++;
            }
          } else if (state && chunkDecoded.operation === 'snapshot' && chunkDecoded.data) {
            for (const row of chunkDecoded.data) {
              await this.applyEvent(table, { table, operation: 'INSERT', data: row, lsn: desc.lsn });
              rowsApplied++;
            }
          }
          msg.ack();
        }

        if (receivedCount === 0) done = true;
      }

      const state = this.syncedTables.get(table);
      if (state) state.lsn = desc.lsn;
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
      await this.transaction(async (tx) => {
        const txExec: Exec = (q, ...p) => (tx.sql as any)(q, ...p);
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
