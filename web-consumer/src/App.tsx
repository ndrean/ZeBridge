import { createSignal, onCleanup, For } from 'solid-js';
import { wsconnect, NatsConnection, headers } from '@nats-io/nats-core';
import { jetstream, jetstreamManager, DeliverPolicy } from '@nats-io/jetstream';
import { Kvm } from '@nats-io/kv';
import { decode, encode } from '@msgpack/msgpack';
import topology from '../../topology.json';
import { SQLocal } from 'sqlocal';
// v7, not v4 (`crypto.randomUUID()`). Both are collision-safe; v7 puts a 48-bit
// millisecond timestamp in the high bits, so keys sort by creation time and index inserts
// stay append-only instead of scattering across the B-tree.
//
// A dependency rather than twelve lines of bit-shifting, because this file is reference
// code: a consumer author should read `v7()` and know exactly what to reach for, not have
// to audit a hand-rolled version nibble before trusting it. PostgreSQL 18 has `uuidv7()`
// natively — but that is the *server* minting the key, which is what an edge-writable
// table must not depend on (PROTOCOL.md §7.2).
import { v7 as uuidv7 } from 'uuid';

const NATS_URL = 'ws://localhost:8080';

/// The principal this client writes as — the second token of every mutation subject,
/// and the NATS user it authenticates as. The two must be the same string: the server
/// grants `publish: ["mutation.alice.>"]` to the user `alice`, so a mismatch is not a
/// silent downgrade — every write is rejected with a permissions violation.
///
/// This is what makes PROTOCOL.md §7.1 true rather than aspirational. Verified against
/// the server in `nats-server.conf.template`: publishing `mutation.bob.…` as alice is
/// refused by NATS before the bridge ever sees it, so the bridge can trust the token
/// without checking anything.
const PRINCIPAL = 'alice';
const PASSWORD = 's3cret';

/// ⚠️ A password in a browser bundle is **enforced, not secret**. Anyone with devtools
/// reads it and can then write as `alice` from curl. What this buys is real and worth
/// having — alice cannot write as bob, cannot forge a `cdc.>` event, cannot overwrite a
/// schema in KV — but it authenticates *the bundle*, not *the person*.
///
/// The endpoint for a browser is a short-lived credential minted per session by your own
/// auth server after it authenticates the user, handed over as a JWT. The permission
/// shape is identical, so nothing above changes when that lands. Server-side consumers
/// (Go, Python) have no such gap: user/password is the right answer there today.
///
/// Note the bridge's nkey is **gone from this file**. That was the real hole: the client
/// held the bridge's own credential, so every permission below was moot.
// ⚠️ A **new OPFS file on every page load**, which is why "Clear site data" appears to do
// nothing and why files accumulate: nothing ever reuses or removes the previous one, and
// OPFS is not what that devtools button clears. `zb.orphans()` / `zb.purge()` below make
// that visible and reclaimable, so incognito is not the only way to get a clean run.
//
// It also means the replica is **rebuilt from snapshot + CDC on every load**, so there is
// no durable outbox — and therefore nothing for `Nats-Msg-Id` to be idempotent *across*
// yet. A stable name is what turns the replica durable; that is a design decision, not a
// cleanup, so it is left alone here.
const DB_NAME = `zebridge_${Date.now()}.sqlite3`;
const { sql, deleteDatabaseFile } = new SQLocal(DB_NAME);


const td = new TextDecoder();

// Console handle for inspecting the local replica directly, e.g.
//
//   await zb.q('SELECT id, name FROM users ORDER BY id')
//   await zb.count('users')
//   zb.state()                       // stored seq/lsn and per-table lsn
//   await zb.orphans()               // every past load's database still in OPFS
//   await zb.purge()                 // remove all but this one
//   await zb.reset()                 // delete this one and reload clean
//
// The database lives in the browser's OPFS under a per-session name, so there is no file
// to open with the sqlite3 CLI and no way to query it except through this worker. Exposed
// deliberately rather than left to a breakpoint: verifying a sync bug means comparing the
// replica against Postgres row by row, and "which rows arrived" is the question that
// distinguishes a correct run from a lucky one.
declare global {
  interface Window { zb: any }
}
if (typeof window !== 'undefined') {
  window.zb = {
    db: DB_NAME,
    /**
     * Mint a UUID v7 — the key an edge-writable table wants (PROTOCOL.md §7.4).
     *
     * ⚠️ Not usable against `users`: it has  `bigint` primary keys, 
     * the column type rejects it. It needs a table keyed `uuid` — migration in §7.4.
     * on the contrary, `test_types` has.
     */
    uuid: () => uuidv7(),
    /**
     * Delete *this* session's database file, then reload into a fresh one.
     *
     * The reason "Clear site data" looks broken: it does not clear OPFS. Nothing in
     * devtools does. This is the button that actually works.
     */
    reset: async () => {
      await deleteDatabaseFile();
      location.reload();
    },
    /** Every zebridge database left in OPFS, newest last — one per past page load. */
    orphans: async () => {
      const root = await navigator.storage.getDirectory();
      const names: string[] = [];
      for await (const name of (root as any).keys()) {
        if (typeof name === 'string' && name.startsWith('zebridge_')) names.push(name);
      }
      return names.sort();
    },
    /** Remove every zebridge database except the one this page is using. */
    purge: async () => {
      const root = await navigator.storage.getDirectory();
      const removed: string[] = [];
      for (const name of await window.zb.orphans()) {
        if (name === DB_NAME) continue;
        // OPFS keeps -journal / -wal siblings; removeEntry on the base name leaves those,
        // so failures here are expected and skipped rather than aborting the sweep.
        try {
          await root.removeEntry(name);
          removed.push(name);
        } catch { /* held by another tab, or already gone */ }
      }
      return { removed: removed.length, kept: DB_NAME };
    },
    /** Run any SQL against the local replica. Returns rows. */
    q: (text: string) => sql([text] as any),
    count: async (table: string) => (await sql([`SELECT COUNT(*) AS n FROM ${table}`] as any))[0],
    /** Every table the client believes it is syncing, with the LSN it has applied to. */
    state: () => ({
      global: { ...globalSyncState },
      tables: Object.fromEntries([...syncedTables].map(([t, v]) => [t, { lsn: v.lsn, columns: v.columns.length }])),
      failed: [...failedTables],
    }),
    /** Primary keys present locally — the list to diff against Postgres. */
    ids: async (table: string, col = 'id') =>
      (await sql([`SELECT ${col} FROM ${table} ORDER BY ${col}`] as any)).map((r: any) => r[col]),
  };
}

// Columns hidden from the *_view convenience views (still stored in the table).
const EXCLUDE_FROM_VIEW = ['uid', 'inserted_at', 'updated_at', 'metadata'];

type TableState = {
  /**
   * The table's key columns. One entry for a normal table, several for a composite
   * primary key. Stored as a list rather than a string because every use of it —
   * PRIMARY KEY, ON CONFLICT, DELETE — generalises cleanly, whereas a string forces
   * a second code path the moment a table has a two-column key.
   */
  pkCols: string[];
  /** Column names the local table currently has. */
  columns: string[];
  /**
   * The table's tombstone column, from the schema descriptor, or null when deletes on this
   * table are physical.
   *
   * Published by the bridge (`tombstone_column`) precisely so a client does not have to
   * guess: the same table may soft-delete in one deployment and not in another, since it
   * is a `SYNC_RULES` decision rather than a schema one.
   */
  tombstoneColumn: string | null;
  /** WAL LSN this schema is valid from. Events at or below it predate the change. */
  lsn: number;
};

/// How long to wait for a snapshot descriptor before re-requesting.
///
/// Must exceed a realistic snapshot for the largest table, or a client re-requests work
/// that was about to arrive. Must be well below the REQUESTS stream's max_age, or the
/// retry lands after the window has already dropped the first request — retrying at the
/// last moment is the same as not retrying.
const SNAPSHOT_WAIT_MS = 60_000;
const SNAPSHOT_REQUEST_ATTEMPTS = 5;

/// Wait for this table's snapshot descriptor, or give up after `timeoutMs`.
///
/// Returns null on timeout rather than throwing: "not yet" is an ordinary outcome here,
/// not an exception. The watch is always torn down — an abandoned KV watch leaks a
/// subscription per attempt, and this is on the retry path.
async function waitForDescriptor(kv: any, table: string, timeoutMs: number): Promise<any | null> {
  let iter: any = null;
  try {
    iter = await kv.watch({ key: table });
    return await Promise.race([
      (async () => {
        for await (const entry of iter) {
          if (entry.operation === "DEL" || entry.operation === "PURGE") continue;
          try { return decode(entry.value); } catch { return JSON.parse(new TextDecoder().decode(entry.value)); }
        }
        return null;
      })(),
      new Promise<null>((resolve) => setTimeout(() => resolve(null), timeoutMs)),
    ]);
  } catch {
    return null;
  } finally {
    try { iter?.stop?.(); } catch { /* already closed */ }
  }
}

/// Tables that could not be seeded. Held out of `syncedTables` so no CDC event is applied
/// to a local table that has no baseline — an unseeded replica must stay visibly absent
/// rather than quietly partial.
const failedTables = new Set<string>();

// Non-reactive state for sync tracking
const syncedTables = new Map<string, TableState>();
let globalSyncState = { lsn: 0, seq: 0 };

/**
 * CDC events that arrived referencing columns the local schema does not have yet.
 * The bridge publishes the schema before the dependent row, but KV and CDC are two
 * independent subscriptions here, so the row can still win the race locally. Holding
 * it until a newer schema lands is what turns the bridge's ordering guarantee into
 * something this client actually honours.
 */
const pendingEvents: { table: string; ev: any }[] = [];

let nc: NatsConnection | null = null;



export default function App() {
  const [status, setStatus] = createSignal<'connected' | 'disconnected' | 'connecting'>('disconnected');
  const [dbState, setDbState] = createSignal<Record<string, { columns: string[], rows: any[], count: number }>>({});
  const [pendingCount, setPendingCount] = createSignal(0);
  const [tableCount, setTableCount] = createSignal(0);
  const [suspended, setSuspended] = createSignal<Record<string, string>>({});

  const appendLog = (topic: string, data: any, opType = '') => {
    if (['INSERT', 'UPDATE', 'DELETE', 'snapshot', 'CDC'].includes(opType)) {
      return; // Skip high-volume CDC events to save the UI thread
    }
    const timestamp = new Date().toLocaleTimeString();
    const bodyStr = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
    console.log(`[${timestamp}] ${topic} ${opType}:`, bodyStr);
  };

  const refreshLocalDb = async () => {
    if (!syncedTables.size) {
      setDbState({});
      return;
    }
    const newState: Record<string, { columns: string[], rows: any[], count: number }> = {};
    for (const [table, state] of syncedTables) {
      try {
        // ⚠️ Soft-deleted rows are still **here**, and must be filtered out.
        //
        // A delete on a tombstoned table arrives as `cdc.<table>.update` with the tombstone
        // set — because that is literally what happened in PostgreSQL — so the applier
        // takes the UPSERT branch and the row stays in the local replica carrying its
        // `deleted_at`. That is correct and deliberate: the row has to survive locally for
        // an offline edit to be overruled rather than resurrecting it (§7.5). But a query
        // that does not filter shows the user rows they deleted.
        //
        // The bridge does not send a later `delete` for these — the sweeper's reap is
        // suppressed, since the client already acted on the tombstone update. So this
        // filter is not an optimisation; without it a deleted row is visible forever.
        //
        // Discovered from the tombstone column in the schema descriptor, not hardcoded:
        // a table without one deletes physically, its rows are gone from SQLite, and
        // adding `WHERE deleted_at IS NULL` to it would fail on a column that does not
        // exist.
        const tombstone = state.tombstoneColumn;
        const liveOnly = tombstone ? ` WHERE "${tombstone}" IS NULL` : '';

        const countRes = await sql(`SELECT COUNT(*) as count FROM ${table}${liveOnly}`);
        const order = state.pkCols.length
          ? state.pkCols.map((c) => `"${c}" ASC`).join(', ')
          : 'rowid ASC';
        const rows = await sql(`SELECT * FROM ${table}${liveOnly} ORDER BY ${order} LIMIT 100`);
        newState[table] = {
          columns: state.columns,
          rows: rows,
          count: countRes[0]?.count ?? 0,
        };
      } catch (err) {
        // ignore
      }
    }
    setDbState(newState);
  };


  /// Writes published but not yet accounted for, keyed by the `Nats-Msg-Id` sent with
  /// them. An entry leaves either because a verdict named it or because it timed out.
  ///
  /// This is the seed of an outbox, not an outbox: it lives in memory, so it does not
  /// survive a reload. A real one belongs in the replica alongside the rows, in the same
  /// transaction as the optimistic write (PROTOCOL.md §7.1) — which is work that arrives
  /// with optimistic apply, not before it.
  const pendingWrites = new Map<string, { table: string; id: number | string; at: number }>();

  /// Subscribe to this principal's write verdicts, and apply PROTOCOL.md §7.1.
  ///
  /// **Every** write the bridge reaches a conclusion about is answered here — that is
  /// §7.4b, and it is what makes "pop the queue only on a definitive reply" implementable.
  /// It was not always so: the bridge once published a verdict *only on failure*, and a
  /// client obeying §7.1 literally could therefore never pop a **successful** write. It
  /// would resend forever — idempotently, thanks to `Nats-Msg-Id`, and so invisibly: no
  /// error, no duplicate row, just an outbox that never drained.
  ///
  /// ⚠️ The three success statuses are not interchangeable, and the row count cannot tell
  /// them apart — two of the three are *zero rows affected*. The bridge classifies them by
  /// reading the row's state in the same transaction as the write; a client must not try
  /// to re-derive them.
  const watchVerdicts = async () => {
    if (!nc) return;
    const sub = nc.subscribe(`mutation_ack.${PRINCIPAL}.>`);
    (async () => {
      for await (const m of sub) {
        // The subject's last token is the msg_id, and it may itself contain dots — so it
        // is everything after the principal, not `split('.')[2]`.
        const prefix = `mutation_ack.${PRINCIPAL}.`;
        const msgId = m.subject.startsWith(prefix) ? m.subject.slice(prefix.length) : m.subject;
        const verdict = JSON.parse(new TextDecoder().decode(m.data));
        const pending = pendingWrites.get(msgId);
        const where = pending ? `${pending.table}#${pending.id}` : '(not from this session)';

        // `failed` is the ONE status that is not definitive: the bridge exhausted its
        // deliveries for a reason that may not recur. §7.1 says keep it and retry, so the
        // entry stays and `sweepPendingWrites` decides when to give up. Everything else
        // pops — including `stale` and `rejected`, where resending cannot change anything.
        const definitive = verdict.status !== 'failed';
        if (definitive) pendingWrites.delete(msgId);

        switch (verdict.status) {
          case 'accepted':
            // ⚠️ Nothing is applied here. State arrives over CDC, never from a verdict
            // (§7.1: "treat the reply as a verdict, not as data") — keeping one path for
            // state is what stops this client having two sources of truth.
            if (verdict.reason === 'version_clamped') {
              // The version sent was ahead of the database's clock and was capped, so the
              // value this client believes it wrote does not exist anywhere. §7.3 says
              // adopt what came back. Nothing to adopt *into* here — every local row comes
              // from CDC — but a client keeping optimistic rows must overwrite its own.
              appendLog(
                m.subject,
                `${where}: accepted, but the version was clamped to ${verdict.version} — this client's clock is ahead of the database`,
                'WARNING',
              );
            } else {
              appendLog(m.subject, { ...verdict, write: where }, 'VERDICT');
            }
            break;

          case 'stale':
            // ⚠️ Pop, and do **not** hand-revert. A newer version won; the winning row is
            // already on its way over CDC, and reverting locally would fight the feed and
            // briefly show the user a row the database does not have.
            appendLog(
              m.subject,
              `${where}: a newer version won — dropping this edit, the winning row arrives via CDC`,
              'INFO',
            );
            break;

          case 'row_deleted':
            // The one case last-write-wins cannot decide for the user: their edit targeted
            // a row someone else deleted. §7.1 SHOULD-2 — surface it rather than silently
            // discarding it.
            appendLog(
              m.subject,
              `${where}: the row was deleted elsewhere, so this edit cannot be applied. Surface this to the user rather than dropping it.`,
              'ERROR',
            );
            break;

          case 'rejected':
            // PostgreSQL refused, and would refuse the same bytes again — a constraint, a
            // missing grant, a bad type. Retrying is not a strategy; the fix is a schema
            // change, a grant, or different data.
            appendLog(
              m.subject,
              `${where}: refused permanently (${verdict.reason}${verdict.sqlstate ? ` / SQLSTATE ${verdict.sqlstate}` : ''}) — ${verdict.detail || 'no detail'}`,
              'ERROR',
            );
            break;

          case 'failed':
            appendLog(
              m.subject,
              `${where}: failed after the bridge's delivery limit (${verdict.reason}) — kept for retry`,
              'WARNING',
            );
            break;

          default:
            // An unknown status is not something to guess at: keep the entry so the
            // timeout reports it rather than popping on a reply this client cannot read.
            pendingWrites.set(msgId, pending ?? { table: '?', id: '?', at: Date.now() });
            appendLog(m.subject, { ...verdict, write: where, note: 'unknown status — kept pending' }, 'ERROR');
        }
      }
    })();
  };

  /// Anything still pending after this long is treated as unconfirmed.
  ///
  /// Now that every write is answered (§7.4b), reaching this timeout means something is
  /// genuinely wrong rather than merely unremarkable: the bridge died before publishing,
  /// the verdict was undeliverable, or ingress is not running. Before the reply channel
  /// covered successes, silence was the *normal* outcome and this timeout was the only
  /// thing that ever closed a pending entry.
  ///
  /// Derived, not chosen: `max_deliver` (5) × the bridge's 1s retry sleep, plus the CDC
  /// batch window (500ms), plus slack. ⚠️ Every term is bridge-side configuration this
  /// client cannot see, so raising `max_deliver` server-side silently makes this number
  /// wrong and starts producing false failures. It belongs in the schema descriptor next
  /// to `version_column`; until it is there, this is a guess that has to be kept in step
  /// by hand.
  const WRITE_TIMEOUT_MS = 10_000;

  const sweepPendingWrites = () => {
    const now = Date.now();
    for (const [msgId, w] of pendingWrites) {
      if (now - w.at < WRITE_TIMEOUT_MS) continue;
      pendingWrites.delete(msgId);
      // Not necessarily a failure — a verdict may simply be unreachable (the bridge died
      // before publishing one). Reported as unconfirmed rather than denied, because the
      // two are indistinguishable from here and only one of them is the client's fault.
      // Reaching here now means genuinely nothing came back: no CDC echo (which would
      // have confirmed it above) and no verdict (which would have explained it). The
      // remaining cause is the one nothing can report — a bridge that died before saying
      // anything — so it is reported as unconfirmed, not as denied.
      appendLog(msgId, `no echo and no verdict within ${WRITE_TIMEOUT_MS}ms — unconfirmed (${w.table})`, 'ERROR');
    }
  };

  const initSyncState = async () => {
    await sql(`
      CREATE TABLE IF NOT EXISTS _zebridge_sync (
        id INTEGER PRIMARY KEY,
        global_last_lsn INTEGER,
        global_last_seq INTEGER
      );
    `);
    await sql(`INSERT OR IGNORE INTO _zebridge_sync (id, global_last_lsn, global_last_seq) VALUES (1, 0, 0)`);
    const res = await sql(`SELECT global_last_lsn, global_last_seq FROM _zebridge_sync WHERE id = 1`);
    if (res.length > 0) {
      globalSyncState.lsn = res[0].global_last_lsn ?? 0;
      globalSyncState.seq = res[0].global_last_seq ?? 0;
    }
  };

  const initNats = async () => {
    console.log('App.tsx: initNats called!');
    await initSyncState();

    if (nc) {
      try { await nc.close(); } catch {}
      nc = null;
    }

    try {
      setStatus('connecting');
      appendLog('SYS', `Connecting to NATS at ${NATS_URL}...`);

      nc = await wsconnect({
        servers: NATS_URL,
        user: PRINCIPAL,
        pass: PASSWORD,
        reconnect: true,
        maxReconnectAttempts: -1
      });

      setStatus('connected');
      appendLog('SYS', `Connected to NATS over WebSockets as '${PRINCIPAL}'`);
      await watchSchemas();
      await watchVerdicts();
      await subscribeStreams();
    } catch (err) {
      setStatus('disconnected');
      appendLog('SYS', `Connection failed: ${err}`);
    }
  };

  // ---------------------------------------------------------------------------
  // Schema handling
  // ---------------------------------------------------------------------------

  /**
   * Watch the schemas KV bucket for every table, not one table on a button press.
   *
   * The bridge pushes a schema whenever DDL runs, so migrations should be driven by
   * that push. watch() also replays current values on start, which doubles as the
   * initial bootstrap — one code path for "catch up" and "keep up".
   */
  const watchSchemas = async () => {
    if (!nc) return;
    try {
      const kvm = new Kvm(nc);
      const kv = await kvm.open(topology.kv.schemas);
      const watcher = await kv.watch();
      appendLog('SCHEMA', `Watching KV bucket "${topology.kv.schemas}" for all tables...`, 'WATCH');

      return new Promise<void>((resolve) => {
        let initialized = false;

        (async () => {
          for await (const entry of watcher) {
            // ⚠️ `delta === 0` marks the last entry of the initial replay, but this entry
            // has NOT been applied yet — the migration below is still to come, and it
            // awaits SQL. Resolving here would let `subscribeStreams()` run and snapshot
            // `syncedTables.keys()` while the final table is still being created, so that
            // table is never requested.
            //
            // Observed live: `users` snapshotted, `test_types` silently skipped, and the
            // client still logged "All required snapshots replayed successfully". It had
            // rows only because the CDC stream happened to still hold every insert from
            // seq 1 — with any rotation the table would have been quietly empty.
            //
            // So the flag is *recorded* here and acted on in the `finally` below, once
            // this entry is fully applied.
            const isLastOfInitialReplay = !initialized && (!entry || entry.delta === 0);
            try {
            if (!entry || !entry.key) continue;

            // A deleted/purged key means the table is gone upstream.
            if (entry.operation === 'DEL' || entry.operation === 'PURGE') {
              await dropLocalTable(entry.key, 'KV key removed');
              continue;
            }

            let val: any;
            try { val = decode(entry.value); } catch { val = JSON.parse(td.decode(entry.value)); }
            if (val?.schema && typeof val.schema === 'string') val = JSON.parse(val.schema);

            // Tombstone: the bridge publishes {"dropped":true} rather than removing the
            // key, so a client arriving after the DROP still learns the table is gone.
            if (val?.dropped === true) {
              await dropLocalTable(entry.key, `tombstone @ lsn ${val.lsn ?? '?'}`);
              continue;
            }

            // Suspension
            if (val?.suspended === true) {
              setSuspended((prev) => ({ ...prev, [entry.key]: val.reason ?? 'unknown' }));
              continue;
            }

            // A normal schema after a suspension is the recovery signal
            setSuspended((prev) => {
              if (!(entry.key in prev)) return prev;
              appendLog('SCHEMA', `Table "${entry.key}" resumed upstream — replication restored`, 'MIGRATE');
              const { [entry.key]: _removed, ...rest } = prev;
              return rest;
            });

            if (val?.sqlite?.columns) {
              console.log(`[SCHEMA PAYLOAD] ${entry.key}:`, val);
              await applySchema(entry.key, val);
            }
            } finally {
              // After the entry is applied, whichever branch it took — including the
              // `continue`s above, which is why this is a `finally` and not a trailing
              // statement.
              if (isLastOfInitialReplay) {
                initialized = true;
                resolve();
              }
            }
          }
        })();
      });
    } catch (err) {
      appendLog('SCHEMA', `Watch failed: ${err}`, 'ERROR');
    }
  };

  const dropLocalTable = async (table: string, reason: string) => {
    try {
      await sql(`DROP VIEW IF EXISTS ${table}_view;`);
      await sql(`DROP TABLE IF EXISTS ${table};`);
      syncedTables.delete(table);
      setTableCount(syncedTables.size);
      appendLog('SCHEMA', `Dropped local table "${table}" (${reason})`, 'DROP');
    } catch (err) {
      appendLog('SCHEMA', `Drop of ${table} failed: ${err}`, 'ERROR');
    }
  };

  /**
   * Apply a schema to the local database.
   *
   * Additive changes use ALTER TABLE ADD COLUMN so existing rows survive. That matters
   * for the mid-stream evolution test: rebuilding the table on every schema push would
   * wipe the local replica, making "the client survived" trivially true and hiding any
   * data actually lost.
   */
  const applySchema = async (table: string, val: any) => {
    // `pk_columns` is authoritative and always present; `pk` is the legacy
    // single-column form and is null for a composite key. Falling back keeps this
    // working against a bridge that predates pk_columns.
    const pkCols: string[] = Array.isArray(val.sqlite.pk_columns)
      ? val.sqlite.pk_columns
      : val.sqlite.pk
        ? [val.sqlite.pk]
        : [];
    const cols: { name: string; type: string }[] = val.sqlite.columns;
    const lsn: number = typeof val.lsn === 'number' ? val.lsn : 0;

    // Which column marks a row deleted, or null when this table deletes physically. The
    // bridge publishes it because it is a SYNC_RULES decision, not a schema one — the same
    // table can soft-delete in one deployment and not in another, so a client that guessed
    // from the column list would be wrong half the time.
    const tombstoneColumn: string | null =
      typeof val.tombstone_column === 'string' ? val.tombstone_column : null;
    const names = cols.map((c) => c.name);

    const existing = syncedTables.get(table);
    const added = existing ? names.filter((n) => !existing.columns.includes(n)) : [];
    const removed = existing ? existing.columns.filter((n) => !names.includes(n)) : [];

    // A single-column key can be declared inline; a composite one must be a
    // table-level constraint, so the column DDL stays bare and the constraint is
    // appended after the column list.
    //
    // ⚠️ `NOT NULL` on every key column, because **SQLite does not imply it and PostgreSQL
    // does.** Verified: `PRIMARY KEY ("a","b")` accepts `(NULL, NULL)`, and accepts it
    // *twice* — NULL never equals NULL, so the constraint does not dedupe. The same is
    // true of a single `TEXT PRIMARY KEY` (a uuid key). That is not a cosmetic
    // divergence: the CDC applier upserts with `ON CONFLICT (<pk>) DO UPDATE`, and a
    // conflict target that never matches turns a replayed chunk into duplicate rows —
    // silently, in a replica whose whole job is to agree with the source.
    //
    // ⚠️ It is *ignored* for a single `INTEGER PRIMARY KEY`, which SQLite treats as an
    // alias for the rowid: a missing or NULL id is assigned `max(rowid)+1` rather than
    // rejected, and no NOT NULL changes that. So a local write that forgets the key does
    // not fail — it invents one, and that phantom row later collides with a real server
    // row of the same id. Emitted anyway (harmless, and it documents the intent), but the
    // guarantee has to come from the writer always supplying the key.
    const inlinePk = pkCols.length === 1;
    const isPk = (name: string) => pkCols.includes(name);
    const ddl = (c: { name: string; type: string }) =>
      `"${c.name}" ${c.type}` +
      (isPk(c.name) ? ' NOT NULL' : '') +
      (inlinePk && c.name === pkCols[0] ? ' PRIMARY KEY' : '');
    const tableConstraint =
      pkCols.length > 1 ? `, PRIMARY KEY (${pkCols.map((c) => `"${c}"`).join(', ')})` : '';

    /**
     * Recreate the table and carry over every column the two schemas share.
     *
     * Used only for changes SQLite cannot do in place (type changes, or dropping a
     * PK/UNIQUE/indexed column, which SQLite refuses). It is NOT a data reset:
     * dropping the table outright would discard rows the new schema can still hold,
     * and would force a needless re-seed. Re-seeding is for a CDC gap, not for DDL.
     */
    const rebuildPreservingData = async (why: string) => {
      const tmp = `${table}__migrating`;
      await sql(`DROP TABLE IF EXISTS ${tmp};`);
      await sql(`CREATE TABLE ${tmp} (${cols.map(ddl).join(', ')}${tableConstraint});`);

      if (existing) {
        const common = names.filter((n) => existing.columns.includes(n)).map((n) => `"${n}"`);
        if (common.length) {
          await sql(`INSERT INTO ${tmp} (${common.join(', ')}) SELECT ${common.join(', ')} FROM ${table};`);
        }
      }
      await sql(`DROP TABLE IF EXISTS ${table};`);
      await sql(`ALTER TABLE ${tmp} RENAME TO ${table};`);
      appendLog('SCHEMA', `${table}: rebuilt preserving common columns (${why}), lsn=${lsn}`, 'MIGRATE');
    };

    try {
      if (!existing) {
        await sql(`DROP TABLE IF EXISTS ${table};`);
        await sql(`CREATE TABLE ${table} (${cols.map(ddl).join(', ')}${tableConstraint});`);
        appendLog('SCHEMA', `${table}: created (first sight), lsn=${lsn}`, 'MIGRATE');
      } else if (added.length === 0 && removed.length === 0) {
        syncedTables.set(table, { pkCols, columns: names, lsn, tombstoneColumn });
        return; // identical schema, e.g. a boot republish
      } else {
        // Apply in place where SQLite allows it. DROP COLUMN exists since 3.35 and
        // preserves every remaining row — a removed column is not a reason to discard
        // the replica. SQLite still refuses to drop a PK/UNIQUE/indexed column, so
        // fall back to the copy-based rebuild rather than assuming it worked.
        try {
          for (const name of removed) {
            await sql(`ALTER TABLE ${table} DROP COLUMN "${name}";`);
          }
          for (const name of added) {
            const type = cols.find((c) => c.name === name)!.type;
            await sql(`ALTER TABLE ${table} ADD COLUMN "${name}" ${type};`);
          }
          const parts = [
            added.length ? `+[${added.join(', ')}]` : null,
            removed.length ? `-[${removed.join(', ')}]` : null,
          ].filter(Boolean).join(' ');
          appendLog('SCHEMA', `${table}: ${parts} via ALTER (rows preserved), lsn=${lsn}`, 'MIGRATE');
        } catch (alterErr) {
          await rebuildPreservingData(`ALTER refused: ${alterErr}`);
        }
      }

      // (Re)build the convenience view against the current column set.
      const viewCols = names.filter((n) => !EXCLUDE_FROM_VIEW.includes(n)).map((n) => `"${n}"`).join(', ');
      await sql(`DROP VIEW IF EXISTS ${table}_view;`);
      if (viewCols) await sql(`CREATE VIEW ${table}_view AS SELECT ${viewCols} FROM ${table};`);

      syncedTables.set(table, { pkCols, columns: names, lsn, tombstoneColumn });
      setTableCount(syncedTables.size);

      await drainPending(table);
    } catch (err) {
      appendLog('SCHEMA', `Applying schema for ${table} failed: ${err}`, 'ERROR');
    }
  };

  // ---------------------------------------------------------------------------
  // CDC handling
  // ---------------------------------------------------------------------------

  /** Replay events that were held back waiting for this table's schema to catch up. */
  const drainPending = async (table: string) => {
    if (!pendingEvents.length) return;
    const mine = pendingEvents.filter((p) => p.table === table);
    if (!mine.length) return;

    for (let i = pendingEvents.length - 1; i >= 0; i--) {
      if (pendingEvents[i].table === table) pendingEvents.splice(i, 1);
    }
    setPendingCount(pendingEvents.length);
    appendLog('CDC', `Replaying ${mine.length} held event(s) for ${table}`, 'DRAIN');
    for (const p of mine) await applyEvent(p.table, p.ev);
  };

  const applyEvent = async (table: string, ev: any) => {
    const state = syncedTables.get(table);
    if (!state || !ev?.data) return;

    // The LSN gate: skip events the snapshot already contains.
    //
    // ⚠️ Strictly `<`, never `<=`. A snapshot's watermark is `pg_current_wal_lsn()` taken
    // at BEGIN, and the very next commit is stamped with **that same LSN** — measured:
    // watermark 25472600, first post-snapshot event 25472600. With `<=` that row is
    // discarded although the snapshot never contained it, so every snapshot silently
    // loses exactly one row: the first written after it.
    //
    // The boundary is genuinely ambiguous — a row at the watermark may or may not be in
    // the snapshot — so the tie is broken by which mistake costs more. Re-applying is
    // free: the write below is `INSERT … ON CONFLICT DO UPDATE`, so a duplicate converges
    // to the same row. Skipping is permanent.
    if (ev.lsn < state.lsn) {
      return;
    }

    const op = ev.operation;

    if (op === 'INSERT' || op === 'UPDATE') {
      const keys = Object.keys(ev.data);

      // The row references a column we do not have yet: the schema push has not been
      // processed. Hold it rather than dropping it or writing a partial row.
      const unknown = keys.filter((k) => !state.columns.includes(k) && !k.startsWith('old.'));
      if (unknown.length) {
        pendingEvents.push({ table, ev });
        setPendingCount(pendingEvents.length);
        appendLog('CDC', `Holding ${op} on ${table}: unknown column(s) [${unknown.join(', ')}] — awaiting schema newer than lsn ${state.lsn}`, 'HOLD');
        return;
      }

      // ── A changed primary key arrives as an UPDATE, not as delete + insert ──────
      //
      // The upsert below writes the row under its *new* key, which inserts rather than
      // updates — so without this the row under the old key stays in the replica forever
      // and the table permanently disagrees with PostgreSQL. Verified live:
      // `UPDATE test_types SET id = 888001 WHERE id = 777006` left PostgreSQL with one
      // row and the replica with two.
      //
      // ⚠️ `old.*` does appear on a `REPLICA IDENTITY DEFAULT` table — only when the key
      // changes, which is exactly when it is needed, because the old key is the only way
      // to locate the row. (PROTOCOL.md said otherwise until this was measured.)
      //
      // The delete comes first so that a key change *onto* an existing key still
      // converges rather than colliding.
      if (state.pkCols.length) {
        const oldPk = state.pkCols.map((c) => ev.data[`old.${c}`]);
        const newPk = state.pkCols.map((c) => ev.data[c]);
        const keyChanged =
          oldPk.every((v) => v !== undefined && v !== null) &&
          oldPk.some((v, i) => v !== newPk[i]);
        if (keyChanged) {
          const where = state.pkCols.map((c) => `"${c}" = ?`).join(' AND ');
          try {
            await sql(`DELETE FROM ${table} WHERE ${where}`, ...oldPk);
            appendLog('CDC', `Key change on ${table}: ${JSON.stringify(oldPk)} → ${JSON.stringify(newPk)}`, 'REKEY');
          } catch (err) {
            appendLog('SQLITE', `Key-change cleanup on ${table} failed: ${err}`, 'ERROR');
          }
        }
      }

      // `old.*` entries describe the previous row; they are not columns of this table and
      // must not reach the INSERT.
      const dataKeys = keys.filter((k) => !k.startsWith('old.'));
      const values = dataKeys.map((k) => ev.data[k]).map((v) =>
        v !== null && typeof v === 'object' ? JSON.stringify(v) : v
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
        await sql(query, ...values);
      } catch (err) {
        appendLog('SQLITE', `UPSERT on ${table} failed: ${err}`, 'ERROR');
      }
    } else if (op === 'DELETE') {
      // With REPLICA IDENTITY DEFAULT the delete carries the key columns and nulls
      // elsewhere, which is all a delete-by-key needs. Every key column must be
      // present: deleting on a partial composite key would match more rows than
      // PostgreSQL deleted.
      const pkVals = state.pkCols.map((c) => ev.data[c]);
      if (state.pkCols.length && pkVals.every((v) => v !== undefined && v !== null)) {
        const where = state.pkCols.map((c) => `"${c}" = ?`).join(' AND ');
        try {
          await sql(`DELETE FROM ${table} WHERE ${where}`, ...pkVals);
        } catch (err) {
          appendLog('SQLITE', `DELETE on ${table} failed: ${err}`, 'ERROR');
        }
      }
    }

    // ── The echo is the success signal ───────────────────────────────────────────
    //
    // A write that succeeds produces **no verdict** — that is deliberate (§7.0: state
    // arrives through CDC, and the row carries the key this client minted, so nothing
    // else has to say "it worked"). Which means the echo has to be what clears the
    // pending entry: without this, every *successful* write was swept as "unconfirmed"
    // ten seconds later, and the only writes that ever resolved cleanly were the ones
    // that failed.
    //
    // Matching on the primary key, because that is the one thing the client chose and the
    // echo carries back. It also means an echo of *someone else's* write to the same row
    // clears our entry — which is correct: the row reached the state we can observe, and
    // whether our version or theirs won is a question LWW already answered.
    if (state.pkCols.length && pendingWrites.size) {
      const echoedKey = state.pkCols.map((c) => String(ev.data?.[c])).join('|');
      for (const [msgId, w] of pendingWrites) {
        if (w.table !== table || String(w.id) !== echoedKey) continue;
        pendingWrites.delete(msgId);
        appendLog(table, `confirmed by CDC echo after ${Date.now() - w.at}ms`, 'CONFIRMED');
      }
    }

    if ((ev.lsn ?? 0) > globalSyncState.lsn || (ev.seq ?? 0) > globalSyncState.seq) {
      globalSyncState.lsn = Math.max(globalSyncState.lsn, ev.lsn ?? 0);
      globalSyncState.seq = Math.max(globalSyncState.seq, ev.seq ?? 0);
      try {
        await sql(
          `UPDATE _zebridge_sync SET global_last_lsn = ?, global_last_seq = ? WHERE id = 1`,
          globalSyncState.lsn,
          globalSyncState.seq
        );
      } catch (e) {
        appendLog('SQLITE', `Failed to update sync state: ${e}`, 'ERROR');
      }
    }
  };

  const subscribeStreams = async () => {
    if (!nc) return;
    const js = jetstream(nc);
    const jsm = await jetstreamManager(nc);

    // 1. Gap Detection
    try {
      const streamInfo = await jsm.streams.info(topology.streams.cdc);
      const firstSeq = streamInfo.state.first_seq;
      if (globalSyncState.seq === 0 || (firstSeq > 0 && globalSyncState.seq < firstSeq - 1)) {
        appendLog('SYS', `Gap detected or first run! Local seq: ${globalSyncState.seq}, Stream first seq: ${firstSeq}. Snapshots required!`, 'WARNING');
          
          let snapKv;
          try { 
            const kvm = new Kvm(nc!);
            snapKv = await kvm.open(topology.kv.snapshots); 
          } catch { /* ignore */ }

          const tablesToSnap = new Set(syncedTables.keys());
          const snapshotPromises = [];

          for (const table of tablesToSnap) {
            let desc: any = null;
            if (snapKv) {
              try {
                const entry = await snapKv.get(table);
                if (entry) {
                  try { desc = decode(entry.value); } catch { desc = JSON.parse(td.decode(entry.value)); }
                }
              } catch (e) {}
            }

            if (!desc && snapKv) {
              const reqSubject = topology.subjects.snapshot_request.includes('{[table]s}')
                ? topology.subjects.snapshot_request.replace('{[table]s}', table)
                : `${topology.subjects.snapshot_request}.${table}`;

              // PROTOCOL.md §6, "The fourth case: no answer at all".
              //
              // Two changes from the naive version, and each fixes a way of waiting
              // forever:
              //
              // 1. **JetStream publish, not core.** A core `nc.publish` is fire-and-forget:
              //    when the one-request-per-table window is already occupied the broker
              //    drops the message and the client is told nothing. `js.publish` surfaces
              //    the 503, which is not an error here — it means "yours is already
              //    queued", which is exactly what a retry needs to know.
              //
              // 2. **A bounded wait.** Snapshots are served one at a time, and a request
              //    waiting its turn ages against the REQUESTS stream's max_age. If it
              //    expires unread there is no error and no chunks — the bridge never saw
              //    it. Only a timeout can distinguish "still working" from "gone".
              //
              // Retrying is safe in both directions: an expired request is replaced
              // (accepted), a live one is refused (503). Neither can start a second
              // snapshot.
              for (let attempt = 1; attempt <= SNAPSHOT_REQUEST_ATTEMPTS && !desc; attempt++) {
                try {
                  await js.publish(reqSubject, new Uint8Array(0));
                  appendLog('SYS', `Snapshot requested for ${table} (attempt ${attempt}/${SNAPSHOT_REQUEST_ATTEMPTS})`, 'INFO');
                } catch (e: any) {
                  // 503 = the window is occupied, i.e. a request for this table is already
                  // pending — ours or another client's. Wait for its result rather than
                  // treating it as a failure; the descriptor it produces serves everyone.
                  appendLog('SYS', `Request for ${table} refused (${e?.message ?? e}) — a snapshot is already pending, waiting for it`, 'INFO');
                }

                desc = await waitForDescriptor(snapKv, table, SNAPSHOT_WAIT_MS);
                if (!desc) {
                  appendLog('SYS', `No snapshot for ${table} after ${SNAPSHOT_WAIT_MS / 1000}s — the request may have expired unread; re-requesting`, 'WARNING');
                }
              }

              if (!desc) {
                // ⚠️ Unseeded is NOT the same as synced, and the difference is silent.
                //
                // Dropping it from `syncedTables` stops CDC being applied to a table that
                // was never seeded. Otherwise the client follows `cdc.users.*` against an
                // empty local table: INSERTs land, UPDATEs and DELETEs match nothing, and
                // the replica is quietly wrong with no error anywhere — strictly worse
                // than the hang this retry loop replaced.
                //
                // The usual cause is not a slow bridge: `monitored_tables` is read once at
                // boot, so a table added to the publication after the bridge started is
                // refused a snapshot until it restarts.
                syncedTables.delete(table);
                failedTables.add(table);
                appendLog('SYS', `Giving up on ${table} after ${SNAPSHOT_REQUEST_ATTEMPTS} attempts — NOT following CDC for it, the local copy would silently diverge. If the table was added to the publication after the bridge started, restart the bridge.`, 'ERROR');
              }
            }

            if (desc) {
              appendLog('SYS', `Snapshot metadata ready for ${table} (LSN ${desc.lsn}). Replaying...`, 'INFO');
              
              const pullPromise = (async () => {
                await sql(`DELETE FROM ${table}`);
                
                const ci = await jsm.consumers.add(topology.streams.init, {
                  filter_subject: `init.snap.${table}.${desc.snapshot_id}.>`,
                  deliver_policy: DeliverPolicy.All,
                });
                const replayConsumer = await js.consumers.get(topology.streams.init, ci.name);
                
                // Use fetch to reliably pull all chunks currently in the stream for this snapshot
                let done = false;
                let snapshotColumns: string[] | null = null;

                while (!done) {
                  const batch = await replayConsumer.fetch({ max_messages: 100, expires: 1000 }).catch(() => null);
                  if (!batch) break;
                  
                  let receivedCount = 0;
                  for await (const msg of batch) {
                    receivedCount++;
                    let chunkDecoded: any;
                    try { chunkDecoded = decode(msg.data); } catch { chunkDecoded = JSON.parse(td.decode(msg.data)); }
                    
                    const state = syncedTables.get(table);

                    if (chunkDecoded && typeof chunkDecoded === 'object' && Array.isArray(chunkDecoded.schema)) {
                      snapshotColumns = chunkDecoded.schema;
                      appendLog('SYS', `Received snapshot schema for ${table}: ${snapshotColumns!.join(', ')}`, 'INFO');
                    } else if (state && Array.isArray(chunkDecoded)) {
                      // Array of row arrays
                      const cols = snapshotColumns || state.columns;
                      for (const rowVals of chunkDecoded) {
                        const rowObj: any = {};
                        cols.forEach((col: string, i: number) => { rowObj[col] = rowVals[i]; });
                        await applyEvent(table, { table, operation: 'INSERT', data: rowObj, lsn: desc.lsn });
                      }
                    } else if (state && chunkDecoded.operation === 'snapshot' && chunkDecoded.data) {
                      // Legacy JSON format fallback
                      for (const row of chunkDecoded.data) {
                        await applyEvent(table, { table, operation: 'INSERT', data: row, lsn: desc.lsn });
                      }
                    }
                    msg.ack();
                  }
                  
                  // If we didn't receive any messages in this fetch window, the stream is exhausted
                  if (receivedCount === 0) {
                    done = true;
                  }
                }
                
                const state = syncedTables.get(table);
                if (state) state.lsn = desc.lsn;
                appendLog('SYS', `Replay finished for ${table} (Snapshot ID: ${desc.snapshot_id})`, 'INFO');
              })();
              snapshotPromises.push(pullPromise);
            }
          }

          await Promise.all(snapshotPromises);
          if (failedTables.size > 0) {
            // Not "successfully". A summary that reports success while a table is missing
            // is the line an operator trusts instead of reading the errors above it.
            appendLog('SYS', `Snapshots replayed, but ${failedTables.size} table(s) could not be seeded and are excluded: ${[...failedTables].join(', ')}`, 'WARNING');
          } else {
            appendLog('SYS', `All required snapshots replayed successfully!`, 'INFO');
          }
        }
    } catch (e) {
      appendLog('SYS', `Failed to resolve gap and replay snapshots: ${e}`, 'ERROR');
    }

    // 2. Start JetStream Consumer ONLY AFTER snapshots are resolved!
    try {
      console.log('App.tsx: Starting CDC consumer on subject:', `${topology.subjects.cdc_prefix}.>`);
      const ci = await jsm.consumers.add(topology.streams.cdc, {
        filter_subject: `${topology.subjects.cdc_prefix}.>`,
        deliver_policy: globalSyncState.seq > 0 ? DeliverPolicy.StartSequence : DeliverPolicy.All,
        opt_start_seq: globalSyncState.seq > 0 ? globalSyncState.seq + 1 : undefined,
      });
      const consumer = await js.consumers.get(topology.streams.cdc, ci.name);

      console.log('App.tsx: CDC Consumer started! Iterating messages...');
      const iter = await consumer.consume();
      (async () => {
        for await (const msg of iter) {
          let decoded: any;
          try { decoded = decode(msg.data); } catch { decoded = JSON.parse(td.decode(msg.data)); }
          const events = Array.isArray(decoded) ? decoded : [decoded];

          for (const ev of events) {
            ev.seq = msg.seq;
            const table = ev?.table || msg.subject.split('.')[1];
            appendLog(msg.subject, ev, ev?.operation || 'CDC');
            if (table) await applyEvent(table, ev);
          }
          msg.ack();
        }
      })();
    } catch (e) {
      appendLog('SYS', `Failed to start CDC consumer: ${e}`, 'ERROR');
    }


  };

  // ---------------------------------------------------------------------------
  // UI actions
  // ---------------------------------------------------------------------------



/// The id sent as `client_id` and embedded in `Nats-Msg-Id`. A fixed, obviously-fake
/// number: easy to spot in a log, and nothing depends on it being unique yet.
///
/// It is a constant rather than something generated or stored because the replica is
/// rebuilt on every page load (see `DB_NAME`), so there is no durable outbox — nothing
/// queued survives a reload for a msg_id to be idempotent *across*. And the bridge does
/// not compare `client_id` yet, so it breaks no ties either.
///
/// ⚠️ Two changes make this wrong, and both are on the roadmap:
///
///  - **A durable replica** (a stable `DB_NAME`). Then a mutation queued before a crash
///    is retried after it, and the msg_id must be the same across that boundary — so the
///    id has to be stored *in the replica*, sharing its lifetime exactly. Not
///    `localStorage`: "Clear site data" clears that but not OPFS, so the queue would
///    outlive the identity and the retry would write the row twice.
///  - **`client_id` becoming a real tiebreaker.** Then every client sharing this constant
///    means ties cannot be broken between them, and two replicas can stay divergent.
const CLIENT_ID = '1234567890123456';

  /// Render "now" as a value for the table's version column.
  ///
  /// The shape is **exactly what CDC echoes back**, verified against a live round trip:
  ///
  ///     timestamptz  →  2026-08-18T04:57:10.827000Z     (ISO, six digits, trailing Z)
  ///     timestamp    →  2026-08-17T18:10:27.818000      (the same, without the Z)
  ///
  /// ⚠️ So the rendering follows the **column type**, and migrating the column changes the
  /// wire format of every value in it. `test_types.updated_at` was naive until it moved to
  /// `timestamptz`; the `Z` appeared with it. PostgreSQL parses either form, so the bridge
  /// keeps working and only a client's own string comparisons quietly stop agreeing with
  /// the feed — which is the kind of break nothing reports.
  ///
  /// ⚠️ Neither shape is how PostgreSQL *prints* the column: `psql` shows
  /// `2026-08-18 04:57:10.827+00`, space-separated. Matching the database's own output
  /// produces a value the bridge never emits.
  ///
  /// `toISOString()` is the right base because it is always UTC — the property that
  /// matters most, since LWW compares these across clients and a local-time writer wins or
  /// loses on its offset with nothing to show for it.
  ///
  /// The `000` tail extends milliseconds to microseconds: `toISOString()` stops at
  /// milliseconds and a tie is *rejected* by `<`, so two edits to one row inside the same
  /// millisecond would silently drop one. The counter only guarantees values are strictly
  /// increasing within this session. Finer than microseconds would be pointless — the
  /// column keeps six digits and rounds the rest away before LWW ever sees them.
  let lastVersion = '';
  const versionNow = (): string => {
    // "2026-08-18T04:57:10.827Z" → "2026-08-18T04:57:10.827000Z"
    let candidate = new Date().toISOString().replace('Z', '') + '000Z';
    if (candidate <= lastVersion) {
      // Step the microsecond tail, keeping the trailing Z in place.
      const micros = (parseInt(lastVersion.slice(-7, -1), 10) + 1) % 1000000;
      candidate = `${lastVersion.slice(0, -7)}${String(micros).padStart(6, '0')}Z`;
    }
    lastVersion = candidate;
    return candidate;
  };

  const publishMutation = async () => {
    if (!nc) return;
    const table = 'test_types';
    const op = 'INSERT';
    // The client mints the key. `test_types.uid` is `uuid` with no sequence behind it, so
    // there is no shared allocator to contend over: a client-chosen key and a
    // server-chosen one coexist with no bookkeeping (PROTOCOL.md §7.2).
    //
    // v7 rather than `crypto.randomUUID()`'s v4 — same collision safety, but the leading
    // 48-bit timestamp keeps index inserts append-only instead of scattering them across
    // the B-tree.
    //
    // This replaces a `9_000_000_000 + random` band, which was a workaround for the table's
    // former `bigserial` key: an explicit id does not advance the sequence, so every edge
    // write planted a duplicate-key collision the application would hit later.
    const id = uuidv7();

    const version = versionNow();

    // The envelope is PROTOCOL.md §7.2. `table` and `operation` are deliberately absent:
    // they come from the subject, and a payload copy is ignored rather than merged — so
    // including them would only invite the two to disagree.
    //
    // This previously sent `primary_key` and `hlc`, which the bridge refused as
    // MissingPrimaryKey / MissingVersion. The names are not sniffed or aliased.
    const payload = {
      key: { uid: id },
      data: {
        uid: id,
        some_text: 'Manual Button Simulator!',
        age: 42,
        price: 99.99,
        is_true: true,
        // The version column is part of the row, so it must carry the same value as
        // `version` — that is the value LWW will store and compare against next time.
        updated_at: version,
        // ⚠️ Required, and nothing in the protocol says so. `inserted_at` is NOT NULL
        // with no DEFAULT, so an INSERT omitting it fails — and the schema descriptor
        // published to KV carries only column *names and types*, never nullability or
        // defaults, so a client cannot discover this. Worse, the failure is classified
        // transient: it is retried to max_deliver and only then dead-lettered, so the
        // symptom is a write that silently never lands rather than a clear rejection.
        //
        // Sending it on every upsert also rewrites the creation time on an update, which
        // is wrong in general — a real client would send it only on insert.
        inserted_at: version
      },
      version,
      client_id: CLIENT_ID
    };

    await sendMutation(table, op, id, version, payload);
  };

  /// Write to `users`, which is **outbound-only**: it has no `bridge_writer` grant.
  ///
  /// Expect a pause of about **four seconds**, then a verdict. `permission denied` is a
  /// SQL error, and the bridge cannot tell one from a dropped connection, so it gets the
  /// full retry budget — five attempts, each after a 1s sleep — before the bridge
  /// concludes it is out of attempts and says why:
  ///
  /// (Measured at +4.4s. Redelivery is immediate because the bridge NAKs explicitly; the
  /// consumer's 30s `ack_wait` only applies when a message is abandoned without a NAK,
  /// i.e. when the bridge dies holding it.)
  ///
  ///     mutation_ack.alice.<msg_id>
  ///     {"status":"failed","reason":"MutationFailed","sqlstate":"42501",
  ///      "detail":"ERROR:  permission denied for table users ","seq":N}
  ///
  /// The delay is the interesting part, not a defect: it is what a *genuinely* transient
  /// failure gets to recover in. Before verdicts existed this ended in silence — no row,
  /// no error, no echo — which is the failure mode the whole path was built to remove.
  const publishToReadOnlyTable = async () => {
    if (!nc) return;
    const version = versionNow();
    // `users.id` is a bigserial, so a client cannot mint it safely (§7.2). Irrelevant
    // here — the grant refuses the write long before the key could matter — but it is why
    // this table is the read-only fixture in the first place.
    const id = Math.floor(Date.now() / 1000);
    await sendMutation('users', 'INSERT', id, version, {
      key: { id },
      data: { id, name: 'should not land', updated_at: version, inserted_at: version },
      version,
      client_id: CLIENT_ID,
    });
  };

  /// Send a payload with no `key`, to see the *other* verdict path.
  ///
  /// `MissingPrimaryKey` is classified permanent — the same bytes fail identically every
  /// time — so it is dead-lettered on the **first** delivery and the verdict arrives
  /// immediately:
  ///
  ///     {"status":"rejected","reason":"MissingPrimaryKey","sqlstate":"","seq":N}
  ///
  /// `status` is what separates the two: `rejected` means "do not send this again",
  /// `failed` means "the bridge tried five times and gave up". A client's queue should
  /// treat them differently — one is a bug in the write, the other may be worth retrying
  /// later.
  const publishMalformed = async () => {
    if (!nc) return;
    const version = versionNow();
    const id = uuidv7();
    await sendMutation('test_types', 'INSERT', id, version, {
      // No `key` at all. The bridge reads the key from here and nowhere else — it does not
      // fall back to `data`, because a partial or inferred key would make ON CONFLICT
      // match a different row than the client meant.
      data: { uid: id, some_text: 'malformed on purpose', updated_at: version, inserted_at: version },
      version,
      client_id: CLIENT_ID,
    });
  };

  /// Publish one mutation and record it as pending. Shared by the three buttons so they
  /// differ only in *what* they send — which is the point of having three.
  const sendMutation = async (
    table: string,
    op: string,
    id: string | number,
    version: string,
    payload: Record<string, unknown>,
  ) => {
    if (!nc) return;
    // mutation.<principal>.<table>.<operation>. The principal must match the NATS user
    // we connected as — the server's allow-list is what makes this token trustworthy.
    const subject = `mutation.${PRINCIPAL}.${table}.${op.toLowerCase()}`;
    // `msg_id` is a header, not a payload field (§7.2): JetStream deduplicates on
    // Nats-Msg-Id, so a retry after a timeout is idempotent rather than a second write.
    // The correlation token. It is the client's own choice — which is what makes it
    // matchable against a queued write without having stored a server-assigned number —
    // and it is what the bridge echoes back in the verdict subject.
    //
    // ⚠️ **Dots are stripped, because this becomes part of a NATS subject.** The version
    // carries fractional seconds (`…T05:19:35.413000Z`), so an unsanitised msg_id made
    // `mutation_ack.alice.<id>` four tokens instead of three. That happened to work — the
    // subscription uses `>`, and the handler slices after the prefix rather than splitting
    // — but it silently breaks anything matching a single token (`mutation_ack.alice.*`),
    // and a value with two adjacent dots or a trailing one is an *invalid subject* the
    // publish would reject outright, losing the verdict with only a bridge-side log.
    //
    // It cannot escape the principal either way: the pattern is
    // `mutation_ack.<principal>.<msg_id>`, so a crafted msg_id can only add tokens after
    // `alice`, never climb above it. So this is an availability concern, not a security
    // one — but a verdict that cannot be delivered is the failure this whole path exists
    // to remove.
    //
    // The version stays in the id: it is what makes a *second* edit to the same row a
    // different write, so JetStream dedup collapses a retry without collapsing two edits.
    const subjectSafe = (v: string) => v.replace(/[.*>\s]/g, '-');
    const msgId = subjectSafe(`${CLIENT_ID}-${table}-${id}-${version}`);
    const h = headers();
    h.set('Nats-Msg-Id', msgId);
    pendingWrites.set(msgId, { table, id, at: Date.now() });

    // **JetStream publish, not core** — the same rule the snapshot request above already
    // follows, and the write path needs it more.
    //
    // `nc.publish` is fire-and-forget: if MUTATIONS is down, or the subject does not match
    // its filter, the message is dropped and the client is told nothing. `js.publish`
    // waits for a **PubAck** — `{stream, seq}` — so a write is known to be durable before
    // the outbox pops it (§7.1: "pop the queue only on a definitive reply"). Without it
    // there is no definitive anything: a client cannot distinguish accepted from vanished.
    //
    // The PubAck also reports `duplicate: true` when `Nats-Msg-Id` matched inside the
    // stream's dedup window. That is a *success*, not an error — it means a retry after a
    // timeout was correctly collapsed into the original write, which is exactly the signal
    // an idempotent outbox needs to pop without writing the row twice.
    //
    // ⚠️ It acknowledges **persistence, not application**. A PubAck means "the bridge will
    // see this", not "this was written" — the row may still be refused by PostgreSQL for a
    // missing grant, or lose to a newer version, or find no row at all.
    //
    // Which is why the PubAck is *not* what pops the outbox. The verdict on
    // `mutation_ack.<principal>.<msg_id>` is (§7.4b), and `watchVerdicts` above is what
    // reads it. The PubAck only proves the write is durable enough to be worth waiting for
    // a verdict about.
    try {
      // Cheap wrapper over the connection, not a second connection — the `js` inside
      // subscribeStreams is scoped there, and sharing it would mean hoisting a mutable
      // binding for no gain.
      const ack = await jetstream(nc).publish(subject, encode(payload), { headers: h });
      appendLog(subject, { ...payload, _ack: { seq: ack.seq, duplicate: ack.duplicate } }, 'MUTATION OUT');
    } catch (err) {
      // No PubAck: the mutation is *not* in the stream. A real outbox keeps the entry and
      // retries — the msg_id makes that safe even if the message did land and only the
      // acknowledgement was lost.
      appendLog(subject, `not accepted by JetStream: ${err}`, 'ERROR');
    }
  };

  initNats();

    const intervalId = setInterval(refreshLocalDb, 1000);
    const sweepId = setInterval(sweepPendingWrites, 1000);

    onCleanup(() => {
      clearInterval(intervalId);
      clearInterval(sweepId);
      if (nc) nc.close();
    });

    return (
      <>
        <header>
          <h1>ZeBridge CDC Web Consumer</h1>
          <div class="status-bar">
            <span class={`badge ${status()}`}>{status().toUpperCase()}</span>
            <span id="server-url">{NATS_URL}</span>
            <span id="sync-state">tables: {tableCount()} · held events: {pendingCount()}</span>
          </div>
        </header>

        <For each={Object.entries(suspended())}>
          {([table, reason]) => (
            <div class="suspended-banner">
              ⏸ <strong>{table}</strong> is suspended upstream ({reason}). Local rows are frozen and
              still valid, but no new events or snapshots will arrive until a primary key is added.
            </div>
          )}
        </For>

        <div class="controls">
          <button onClick={refreshLocalDb} style="background: #2e7d32;">Refresh Local DB</button>
          <button onClick={publishMutation} style="background: #e65100;">Push Mutation (test_types)</button>
          <button onClick={publishToReadOnlyTable} style="background: #6a1b9a;">Push to users — no grant (verdict in ~4s)</button>
          <button onClick={publishMalformed} style="background: #b71c1c;">Push malformed — no key (verdict now)</button>
        </div>

        <main>
          <h3>Local Database State</h3>
          <div class="tables-container">
            <For each={Object.entries(dbState())}>
              {([tableName, data]) => (
                <div class="table-view">
                  <h4>{tableName} <span class="row-count">({data.count} rows)</span></h4>
                  {data.rows.length === 0 ? (
                    <p>No rows in table.</p>
                  ) : (
                    <table>
                      <thead>
                        <tr>
                          <For each={data.columns}>
                            {(col) => <th>{col}</th>}
                          </For>
                        </tr>
                      </thead>
                      <tbody>
                        <For each={data.rows}>
                          {(row) => (
                            <tr>
                              <For each={data.columns}>
                                {(col) => <td>{String(row[col])}</td>}
                              </For>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  )}
                </div>
              )}
            </For>
          </div>
        </main>
      </>
    );
}
