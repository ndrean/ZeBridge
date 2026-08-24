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
///
/// ⚠️ Read from the environment so two dev servers can run as two different principals:
///
///     VITE_PRINCIPAL=alice VITE_PASSWORD=s3cret pnpm dev --port 5173
///     VITE_PRINCIPAL=bob   VITE_PASSWORD=s3cret pnpm dev --port 5174
///
/// The name must exist as a NATS user with a matching `mutation.<principal>.>` grant, or
/// the connection is refused at authentication — before any of this code runs.
const PRINCIPAL = (import.meta.env.VITE_PRINCIPAL as string | undefined) ?? 'alice';
const PASSWORD = (import.meta.env.VITE_PASSWORD as string | undefined) ?? 's3cret';

/// Opt in to a stable, per-principal OPFS file (`zebridge_<principal>.sqlite3`) instead of
/// a fresh one every page load. Off by default — the timestamped name is the project's own
/// dev/test convention (incognito only, clean room every run) and this must not weaken it.
///
///     VITE_DURABLE=1 VITE_PRINCIPAL=alice VITE_PASSWORD=s3cret pnpm dev --port 5173
///
/// Durability is what makes the outbox below meaningful: a row queued for send has to
/// survive the reload that might follow it, and a file that is deleted every load cannot
/// do that regardless of what table lives in it.
const DURABLE = ['1', 'true'].includes((import.meta.env.VITE_DURABLE as string | undefined) ?? '');

/// The tenant subtree this client reads, when the deployment routes CDC by tenant.
///
/// ⚠️ **RLS does not bound reads.** It bounds what a principal may write and what a
/// snapshot may select; what stops one tenant *reading* another's rows is this subject
/// plus the matching NATS permission. A client granted `cdc.>` sees every tenant however
/// perfect the policies are — which is why this is asked of NATS, not guessed.
///
/// Resolved live in `resolveTenant()` from `$KV.tenants.<principal>`
/// (PROTOCOL.md "The Connection Flow", Step 0) rather than a build-time constant.
/// A build-time `VITE_TENANT` used to sit here — the failure mode NOTES.md §1.12 part 3
/// describes: nothing catches `VITE_TENANT=globex` on alice's dev server, and she
/// connects, authenticates, and looks like an idle database. The KV bucket is asked
/// fresh on every connect instead, so a reassignment in `zebridge_user_tenants` takes
/// effect on the next connect with no client rebuild.
///
/// Empty means either an untenanted deployment (`cdc_streams` absent from
/// topology.json) or a principal with no tenant-scoped tables — public-only reads.
let TENANT = '';

/// ⚠️ There is deliberately no PUBLIC_TABLES list here any more. Tables with no tenant
/// column are carried by the CDC_PUBLIC *stream*, whose subject list is declared in
/// topology.json and applied by nats-init — server-side, where a grant can bound it. The
/// client used to name them in a `filter_subjects` array, which worked and was not safe:
/// a reader-chosen filter cannot be a security boundary (see cdcStreams()).

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
// ⚠️ A **new OPFS file on every page load** by default, which is why "Clear site data"
// appears to do nothing and why files accumulate: nothing ever reuses or removes the
// previous one, and OPFS is not what that devtools button clears. `zb.orphans()` /
// `zb.purge()` below make that visible and reclaimable, so incognito is not the only way
// to get a clean run.
//
// `VITE_DURABLE` (above) is the opt-in past this: a stable, per-principal name instead of
// one stamped with `Date.now()`. Off by default, the replica is rebuilt from snapshot + CDC
// on every load — a clean room per test run, unchanged from before the outbox existed.
const DB_NAME = DURABLE ? `zebridge_${PRINCIPAL}.sqlite3` : `zebridge_${Date.now()}.sqlite3`;
const { sql, deleteDatabaseFile, transaction } = new SQLocal(DB_NAME);

/// The write outbox — mutations sent but not yet confirmed — lives in the **same** database
/// as the replica, not a second one.
///
/// ⚠️ It has to: the whole point is that the optimistic row and the intent-to-send commit
/// **together** (PROTOCOL.md §7.1), in one `sql.transaction`. Two separate `SQLocal`
/// instances are two separate Workers with no shared transaction — a first version of this
/// lived in its own `zebridge_outbox_<principal>.sqlite3` file, which could never be atomic
/// with the data write no matter how carefully the two calls were sequenced. Fixed by moving
/// it here; the risk that motivated a separate file in the first place — an outbox needing
/// to survive a timestamped, wiped-every-load `DB_NAME` — is what `VITE_DURABLE` now solves
/// instead, at the source, rather than by moving the outbox somewhere the reload can't reach.
///
/// `payload` is stored as JSON, not as the MessagePack bytes actually published. The
/// payload is plain JSON-safe data (`{data, version, client_id}`), so the round trip is
/// lossless, and JSON is inspectable from `zb.outbox()` — which matters for a queue whose
/// contents you only look at when something has already gone wrong.
///
/// `_zebridge_outbox` (PROTOCOL.md §7.1) — not a name this file invented: it is one of
/// the client's three bookkeeping tables, alongside `_zebridge_sync` and
/// `_zebridge_stream_seq`, and the `_zebridge_` prefix is what keeps all three from ever
/// colliding with a genuinely replicated table of the same name. All three are created
/// in one place, `initSyncState()`, which runs before anything else in `initNats()` —
/// not three independent init paths that happen to agree. This module-scope promise is
/// resolved from there; every outbox helper below awaits it rather than creating the
/// table itself.
let resolveOutboxInit: () => void;
const outboxInit = new Promise<void>((resolve) => { resolveOutboxInit = resolve; });

/// Creates `_zebridge_outbox` (and migrates an older file that predates a column).
/// Called once, from `initSyncState()` — see the doc comment on `outboxInit` above for
/// why it lives there rather than running itself.
const createOutboxTable = async () => {
  await sql`
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
  `;
  // A durable (`VITE_DURABLE`) file created before `before` existed lacks the column.
  // SQLite has no `ADD COLUMN IF NOT EXISTS`, so the failure (duplicate column) is the
  // signal that it is already there — not an error worth surfacing.
  try {
    await sql`ALTER TABLE _zebridge_outbox ADD COLUMN before TEXT`;
  } catch { /* already has it */ }
};

/// `before` is the row's full previous state (JSON, or null when this write is an
/// INSERT — there was no previous row), captured in the same transaction as the
/// optimistic apply. It exists to make a `rejected`/`row_deleted` verdict revertible —
/// see `revertOptimisticWrite` — rather than leaving whatever was guessed sitting in the
/// replica forever (NOTES.md §1.9/§1.6d).
///
/// `exec` defaults to the module-level `sql`, but every call site that needs this in the
/// *same* transaction as an optimistic apply passes `tx.sql` instead — see `sendMutation`.
const outboxPut = async (r: {
  msgId: string; subject: string; payload: unknown; table: string; id: string | number;
  before: unknown;
}, exec: typeof sql = sql) => {
  await outboxInit;
  await exec(
    `INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, before, created_at, attempts)
     VALUES (?, ?, ?, ?, ?, ?, ?, 0)
     ON CONFLICT(msg_id) DO UPDATE SET attempts = _zebridge_outbox.attempts + 1`,
    r.msgId, r.subject, JSON.stringify(r.payload), r.table, String(r.id),
    r.before == null ? null : JSON.stringify(r.before), Date.now(),
  );
};

/// Remove a write from the outbox. Called only on a **definitive** outcome — a verdict, or
/// the CDC echo — never on a publish timeout: §7.1 pops the queue on a definitive reply,
/// and "no answer yet" is not one.
const outboxDrop = async (msgId: string) => {
  await outboxInit;
  await sql`DELETE FROM _zebridge_outbox WHERE msg_id = ${msgId}`;
};

const outboxAll = async (): Promise<any[]> => {
  await outboxInit;
  return sql`SELECT * FROM _zebridge_outbox ORDER BY created_at`;
};

/// Undo an optimistic write that the bridge has definitively told us did not happen —
/// `rejected` (PostgreSQL refused it) or `row_deleted` (it targeted a row someone else
/// removed). Neither gets a correcting CDC event: nothing changed server-side, so there
/// is nothing for the replica to converge to on its own (NOTES.md §1.9/§1.6d).
///
/// Reads the outbox row itself for `tbl`/`payload`/`before` rather than trusting
/// in-memory `pendingWrites` — a write replayed by `flushOutbox` after a reload has no
/// in-memory entry, but the durable outbox row is exactly what this needs.
///
/// `mode` distinguishes what "correct" means for the two verdicts:
/// - `'restore'` (rejected): our write never applied, so the row should look exactly
///   like it did before we touched it — `before`.
/// - `'delete'` (row_deleted): the row is confirmed gone server-side. Restoring
///   `before` would resurrect data that no longer exists anywhere; the only correct
///   local state is "no row".
///
/// ⚠️ **Guarded, not unconditional.** Something else may have touched the row between
/// the optimistic apply and this verdict arriving — a real CDC event from another
/// client, or a second optimistic write from this same client. Reverting blind would
/// clobber state that is *more* correct than what we're about to write. So this only
/// acts if the row still shows exactly what our own write applied; otherwise it just
/// drops the outbox entry and leaves the (now independently-explained) row alone —
/// the same behaviour as before this existed.
const revertOptimisticWrite = async (msgId: string, mode: 'restore' | 'delete') => {
  await outboxInit;
  const rows = await sql(`SELECT tbl, payload, before FROM _zebridge_outbox WHERE msg_id = ?`, msgId);
  const entry = rows[0];
  if (!entry) return;

  const table: string = entry.tbl;
  const sent = JSON.parse(entry.payload);
  const before = entry.before ? JSON.parse(entry.before) : null;
  const state = syncedTables.get(table);
  const keyObj = sent.key as Record<string, unknown> | undefined;

  if (!state?.pkCols.length || !keyObj) {
    await sql`DELETE FROM _zebridge_outbox WHERE msg_id = ${msgId}`;
    return;
  }

  const where = state.pkCols.map((c) => `"${c}" = ?`).join(' AND ');
  const pkVals = state.pkCols.map((c) => keyObj[c]);

  await transaction(async (tx) => {
    const current = await tx.sql(`SELECT * FROM ${table} WHERE ${where}`, ...pkVals);
    const currentRow = current[0] ?? null;

    // "Still ours": the row is exactly what our optimistic apply left behind — an
    // INSERT/UPDATE's row matches `sent.data` on every column it set; a DELETE's
    // absence is itself the match.
    const stillOurs = sent.data
      ? currentRow != null && Object.entries(sent.data as Record<string, unknown>)
          .every(([k, v]) => String(currentRow[k]) === String(v))
      : currentRow == null;

    if (stillOurs) {
      if (mode === 'delete') {
        if (currentRow) await tx.sql(`DELETE FROM ${table} WHERE ${where}`, ...pkVals);
      } else if (before == null) {
        // Our write was an INSERT; there was no row before it.
        if (currentRow) await tx.sql(`DELETE FROM ${table} WHERE ${where}`, ...pkVals);
      } else if (currentRow == null) {
        // Our write was a DELETE that got rejected; restore the row it removed.
        const cols = Object.keys(before);
        const placeholders = cols.map(() => '?').join(', ');
        await tx.sql(
          `INSERT INTO ${table} (${cols.map((c) => `"${c}"`).join(', ')}) VALUES (${placeholders})`,
          ...cols.map((c) => before[c]),
        );
      } else {
        // Our write was an UPDATE that got rejected; restore its previous columns.
        const cols = Object.keys(before).filter((c) => !state.pkCols.includes(c));
        if (cols.length) {
          const setClause = cols.map((c) => `"${c}" = ?`).join(', ');
          await tx.sql(`UPDATE ${table} SET ${setClause} WHERE ${where}`, ...cols.map((c) => before[c]), ...pkVals);
        }
      }
    }

    await tx.sql(`DELETE FROM _zebridge_outbox WHERE msg_id = ?`, msgId);
  });
};

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
  /**
   * The table's tenant column, from the schema descriptor (`tenant_column`), or null for
   * a table every principal reads and writes the same content of — system tables and
   * genuinely public business tables alike, not just `topology.json`'s `public_tables`
   * list, which only names user-schema tables.
   *
   * Drives which token a snapshot request/descriptor/stream lookup uses for this specific
   * table: the client's own `TENANT` when set, `topology.open_tenant` when null. Without
   * this, every table used the client's own tenant unconditionally, so a tenant-agnostic
   * table like `users` ended up with one redundant cached snapshot per principal instead
   * of every principal converging on the one the bridge already stores under
   * `topology.open_tenant`.
   */
  tenantColumn: string | null;
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

/// A KV entry as `watchBucket` yields it — the four fields the callers read from
/// `@nats-io/kv`'s own `KvEntry`, and nothing else.
interface BucketEntry {
  key: string;
  operation: 'PUT' | 'DEL' | 'PURGE';
  value: Uint8Array;
  /// Messages still queued behind this one: `0` marks the last entry of the initial
  /// replay, exactly like `KvEntry.delta`.
  delta: number;
}

/// Watch a KV bucket through a plain pull consumer instead of `kv.watch()`.
///
/// ⚠️ `kv.watch()` is the wrong primitive under this server config, and the reason is in
/// `@nats-io/jetstream`, not here. It hands `getPushConsumer` a config carrying
/// `filter_subject`/`deliver_policy`, which `isOrderedPushConsumerOptions` classifies as
/// an **ordered** consumer — so the stable `KV_WATCHER_*` name it picks is discarded for
/// `oc_<nuid>_1`, and every reset (a missed heartbeat pair, a delivery-sequence gap)
/// deletes the old consumer and creates `_2`, `_3`, … The server withholds
/// `$JS.API.CONSUMER.DELETE` from clients on purpose (see `nats-server.conf.template`),
/// so the delete is denied and the abandoned consumers pile up until their 5-minute
/// inactivity threshold — measured live at one new consumer per watcher per reset
/// (NOTES.md §1.14).
///
/// A pull consumer has no reset path: a missed heartbeat just means the next pull. It is
/// also the primitive the CDC path already uses, and it needs only grants a principal
/// already holds (`CONSUMER.CREATE`/`INFO`/`MSG.NEXT` on `KV_<bucket>`). Ephemeral, so it
/// expires on its own once `stop()` is called or the connection drops — nothing to delete.
///
/// Same semantics the callers relied on: `LastPerSubject` replays the current value of
/// every matching key first, then streams updates; `delta === 0` marks the end of the
/// replay. An empty bucket yields nothing and reports `pending: 0` up front, which
/// `kv.watch()` never did — the caller can resolve instead of waiting forever.
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

/// Wait for this snapshot descriptor (KV key, already `<tenant>.<table>` when tenants are
/// declared), or give up after `timeoutMs`.
///
/// Returns null on timeout rather than throwing: "not yet" is an ordinary outcome here,
/// not an exception. The watch is always torn down — an abandoned KV watch leaks a
/// subscription per attempt, and this is on the retry path.
async function waitForDescriptor(js: any, bucket: string, key: string, timeoutMs: number): Promise<any | null> {
  let watch: Awaited<ReturnType<typeof watchBucket>> | null = null;
  try {
    watch = await watchBucket(js, bucket, key);
    return await Promise.race([
      (async () => {
        for await (const entry of watch.entries) {
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
    watch?.stop();
  }
}

/// Tables that could not be seeded. Held out of `syncedTables` so no CDC event is applied
/// to a local table that has no baseline — an unseeded replica must stay visibly absent
/// rather than quietly partial.
const failedTables = new Set<string>();

// Non-reactive state for sync tracking
const syncedTables = new Map<string, TableState>();

/// ⚠️ `syncedTables` is a plain Map, so Solid cannot track it. `<For each={[...keys()]}>`
/// read it once at mount — when it is still empty, because tables are discovered
/// asynchronously from the KV schema watch — and never re-ran, so the Replica table
/// rendered no rows at all.
///
/// This signal mirrors the key set and is what the view iterates. Every `set`/`delete`
/// on the map must be followed by `refreshTableNames()`; the map stays the source of
/// truth for state, this is only the render order.
const [syncedTableNames, setSyncedTableNames] = createSignal<string[]>([]);
const refreshTableNames = () => setSyncedTableNames([...syncedTables.keys()].sort());
/// ⚠️ `seq` is **per stream**, not global. CDC is split into one stream per tenant plus a
/// shared public one, and JetStream sequences are per stream — `CDC_ACME` seq 5 and
/// `CDC_PUBLIC` seq 3 are unrelated numbers. A single `seq` with `Math.max` over both would
/// corrupt each: resuming one stream from the other's position silently skips or replays.
/// `lsn` stays global — it is PostgreSQL's WAL position and means the same thing everywhere.
let globalSyncState: { lsn: number; seq: Record<string, number> } = { lsn: 0, seq: {} };

/// Which CDC streams this client reads. One per tenant plus the shared public stream — the
/// stream name is the read boundary, because a `filter_subject` is chosen by the reader and
/// is not a permission (see nats-server.conf.template).
///
/// ⚠️ Checked against the declared tenant list, not just truthiness: `TENANT` can resolve
/// to a real value that still isn't a *tenant* — the open tenant (`_default`, john's own
/// mapping) has no `CDC_<TENANT>` stream of its own, only `CDC_PUBLIC`. Truthiness alone
/// built `CDC__DEFAULT`, a stream that was never created; `jsm.streams.info` on it throws.
const cdcStreams = (): string[] => {
  const cfg = (topology as any).cdc_streams;
  if (!cfg) return [topology.streams.cdc];                  // untenanted deployment
  const tenants: string[] = (topology as any).tenants || [];
  if (!TENANT || !tenants.includes(TENANT)) return [cfg.public];  // open tenant, or no mapping
  return [`${cfg.tenant_prefix}${TENANT.toUpperCase()}`, cfg.public];
};

/// The ONE CDC stream that would carry a given tenant token's writes — not the two-stream
/// watch list `cdcStreams()` returns, but the single stream a cached snapshot descriptor's
/// own writes actually land on. Takes the tenant explicitly rather than reading the global
/// `TENANT`, because the *table* decides which token its own descriptor uses (see
/// `effectiveTenantFor`) — a tenant-agnostic table's descriptor lives under
/// `topology.open_tenant`, not necessarily this client's own tenant.
const cdcStreamForTenant = (tenant: string): string => {
  const cfg = (topology as any).cdc_streams;
  if (!cfg) return topology.streams.cdc;
  const tenants: string[] = (topology as any).tenants || [];
  if (!tenant || !tenants.includes(tenant)) return cfg.public;
  return `${cfg.tenant_prefix}${tenant.toUpperCase()}`;
};

/// Is a cached snapshot descriptor still safe to trust, or does the CDC stream that would
/// carry this tenant's writes no longer reach back to (or before) the descriptor's own LSN
/// watermark?
///
/// ⚠️ Nothing currently checks this. A client accepts ANY existing `$KV.snapshots` entry
/// unconditionally, however old — the only trigger for a fresh generation is *no*
/// descriptor existing at all. Measured live: `users`' descriptor sat at a 1-row watermark
/// while Postgres held 4, because nothing ever re-requested it, and a since-purged CDC
/// history meant the gap between that watermark and "now" was unrecoverable — the client
/// would silently stay wrong forever. This is what should have caught it: if the CDC
/// stream's own oldest still-available message postdates the descriptor's LSN, some
/// writes between the snapshot and "now" are already gone, and the descriptor must be
/// treated as if it did not exist.
///
/// An unverifiable case (the stream has 0 messages right now, with an existing descriptor)
/// is treated as unsafe rather than assumed fine — the only two explanations for that
/// combination are "nothing written since boot" (retrying costs one harmless
/// regeneration) and "everything aged out from under this exact descriptor" (the failure
/// this exists to catch) — the empty stream alone cannot tell them apart, and a wrong wrong
/// answer here is a silently incomplete replica, not an error anywhere.
const descriptorStillFresh = async (js: any, tenant: string, desc: any): Promise<boolean> => {
  if (desc?.lsn == null) return true; // nothing to compare against — accept as before
  const streamName = cdcStreamForTenant(tenant); // handles the untenanted-deployment case itself
  try {
    const jsm = await js.jetstreamManager();
    const info = await jsm.streams.info(streamName);
    if (info.state.messages === 0) return false; // can't verify — see doc comment above
    if (info.state.first_seq <= 1) return true; // full history intact, nothing could age out
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
      } catch { /* leave null — see the "can't read it" fallback below */ }
    }
    if (oldestLsn == null) return true; // couldn't read it — don't block on an unknown
    return oldestLsn <= desc.lsn;
  } catch {
    return true; // a lookup failure should not itself force every table to re-snapshot
  }
};

/// Which INIT stream a snapshot for the current tenant lives on — one stream per request,
/// unlike `cdcStreams()`'s two-stream watch, since a snapshot request/replay targets a
/// single tenant at a time. `INIT_<TENANT>` for a declared tenant, `INIT_PUBLIC` for the
/// open tenant and public-only reads alike, or the legacy `INIT` stream when tenants
/// aren't declared at all — same fallback shape as `cdcStreams()`.
const initStream = (tenant: string): string => {
  const cfg = (topology as any).init_streams;
  if (!cfg) return topology.streams.init;                   // untenanted deployment
  const tenants: string[] = (topology as any).tenants || [];
  if (!tenant || !tenants.includes(tenant)) return cfg.public;  // open tenant, or no mapping
  return `${cfg.tenant_prefix}${tenant.toUpperCase()}`;
};

/// Which tenant token a specific table's snapshot request/descriptor/stream lookup should
/// use — the client's own `TENANT` for a table `TENANT_RULES` actually scopes, or
/// `topology.open_tenant` for one it doesn't. Without this every table used the client's
/// own tenant unconditionally, so a tenant-agnostic table (`users`, any system table)
/// ended up with one redundant cached snapshot per principal instead of every principal
/// converging on the single one the bridge already stores under the open tenant.
const effectiveTenantFor = (table: string): string => {
  const state = syncedTables.get(table);
  if (state?.tenantColumn) return TENANT;
  return (topology as any).open_tenant || TENANT; // fall back to TENANT if topology.json predates this field
};

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
  const [pendingCount, setPendingCount] = createSignal(0);
  const [suspended, setSuspended] = createSignal<Record<string, string>>({});

  /// Guards the resync (subscribeStreams + flushOutbox) from running twice at once.
  ///
  /// Shared between two independent triggers, not local to either: `nc.status()`'s own
  /// 'reconnect' event, and the active `nc.rtt()` poll below — a server freeze (`kill
  /// -STOP`) can leave the WebSocket object believing it is still connected, with no
  /// 'disconnect'/'reconnect' event ever firing, since nothing at that layer detected a
  /// failure to report. `/bridge/health` already polls actively every 10s for exactly this
  /// reason (see its own comment); the NATS side only ever reacted to events until this.
  let resyncing = false;

  /// The four things that must happen before a replica is trustworthy, in order.
  ///
  /// Shown as a strip rather than a log line because the interesting question is never
  /// "what is happening now" but "how far did it get" — a client stuck between `migrated`
  /// and `snapshot` looks identical to a healthy one in a log tail.
  const [phase, setPhase] = createSignal<Record<string, boolean>>({
    connected: false, migrated: false, snapshot: false, cdc: false,
  });
  const reach = (p: string) => setPhase((prev) => ({ ...prev, [p]: true }));

  /// The bridge's own view of itself, polled through the Vite proxy (see vite.config.ts —
  /// it cannot be fetched directly under COEP).
  const [health, setHealth] = createSignal<'up' | 'down' | 'unknown'>('unknown');

  /// The tenant this client reads, mirroring `TENANT` for the UI. Set once, by
  /// `resolveTenant()` — not inferred from local rows, which would need a row to
  /// already have arrived and so could never help decide what to subscribe to in the
  /// first place.
  const [tenant, setTenant] = createSignal<string>('—');

  /// Per-table row counts, and the last CDC verb, for the flash.
  const [counts, setCounts] = createSignal<Record<string, number>>({});
  /// The counter tables' own `value` column — not a row count, so it rides alongside
  /// `counts` rather than in it, but on the exact same reactive trigger (`recount()`,
  /// debounced off CDC apply) so both stay in sync with no separate polling mechanism.
  const [counterValues, setCounterValues] = createSignal<Record<string, number>>({});
  const [lastVerb, setLastVerb] = createSignal<Record<string, string>>({});
  /// Which tables log their events to the console. Off by default: a burst from the
  /// emitter is noisy enough to hide everything else.
  const [logging, setLogging] = createSignal<Record<string, boolean>>({});

  const appendLog = (topic: string, data: any, opType = '') => {
    if (['INSERT', 'UPDATE', 'DELETE', 'snapshot', 'CDC'].includes(opType)) {
      return; // Skip high-volume CDC events to save the UI thread
    }
    const timestamp = new Date().toLocaleTimeString();
    const bodyStr = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
    console.log(`[${timestamp}] ${topic} ${opType}:`, bodyStr);
  };

  /// Recount every synced table. Counts only — the row grid it replaced queried 100 rows
  /// per table per second and rendered them, which is most of what made the UI thread the
  /// bottleneck during a burst.
  const recount = async () => {
    const next: Record<string, number> = {};
    for (const [table, state] of syncedTables) {
      try {
        // Soft-deleted rows are still present locally and must not be counted — same rule
        // the row query used, and for the same reason (§7.5).
        const liveOnly = state.tombstoneColumn ? ` WHERE "${state.tombstoneColumn}" IS NULL` : '';
        const r = await sql(`SELECT COUNT(*) as count FROM ${table}${liveOnly}`);
        next[table] = r[0]?.count ?? 0;
      } catch { /* table not ready yet */ }
    }
    setCounts(next);

    const nextCounters: Record<string, number> = {};
    for (const table of ['counter_public', 'counter_tenant']) {
      if (!syncedTables.has(table)) continue;
      try {
        const r = await sql(`SELECT value FROM ${table} LIMIT 1`);
        nextCounters[table] = r[0]?.value ?? 0;
      } catch { /* table not ready yet */ }
    }
    setCounterValues(nextCounters);
  };

  /// ⚠️ Debounced, and triggered by CDC — **not** by the click that sent a mutation.
  ///
  /// A write travels client → NATS → bridge → PostgreSQL → CDC → here, so the local row
  /// appears when the event is *applied*, not when the PubAck returns. Counting in a
  /// `.finally` on the publish would read the number from before the write, every time.
  /// Recounting on apply is also what makes the emitter's writes — which this client never
  /// clicked — show up at all.
  let recountTimer: number | undefined;
  const scheduleRecount = () => {
    clearTimeout(recountTimer);
    recountTimer = setTimeout(recount, 250) as unknown as number;
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
        // `rejected`/`row_deleted` pop the outbox as part of `revertOptimisticWrite`
        // (below) — it needs the row still there to know what to undo. Every other
        // definitive status pops it here, unchanged.
        if (definitive && verdict.status !== 'rejected' && verdict.status !== 'row_deleted') {
          void outboxDrop(msgId);
        }

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
            //
            // ⚠️ Nothing is coming via CDC to correct this one. `stale` is silently fine —
            // the winning row arrives and overwrites the optimistic guess. So this reverts
            // by hand: the row is confirmed gone server-side, so the only correct local
            // state is "no row" — restoring the pre-write snapshot would resurrect data
            // that no longer exists anywhere (NOTES.md §1.6d). Guarded: only if the row
            // still shows exactly what our own write applied — see `revertOptimisticWrite`.
            void revertOptimisticWrite(msgId, 'delete');
            appendLog(
              m.subject,
              `${where}: the row was deleted elsewhere, so this edit cannot be applied — reverting the local copy. Surface this to the user rather than dropping it silently.`,
              'ERROR',
            );
            break;

          case 'rejected':
            // PostgreSQL refused, and would refuse the same bytes again — a constraint, a
            // missing grant, a bad type. Retrying is not a strategy; the fix is a schema
            // change, a grant, or different data.
            //
            // ⚠️ Same caveat as row_deleted: nothing changed server-side, so no CDC event
            // is coming to correct what this write optimistically applied. Reverted by
            // hand to the pre-write snapshot captured in the outbox — guarded the same way
            // (NOTES.md §1.6d).
            void revertOptimisticWrite(msgId, 'restore');
            appendLog(
              m.subject,
              `${where}: refused permanently (${verdict.reason}${verdict.sqlstate ? ` / SQLSTATE ${verdict.sqlstate}` : ''}) — ${verdict.detail || 'no detail'} — reverting the local copy`,
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

  /// Creates the client's three bookkeeping tables (PROTOCOL.md §7.1) and loads resume
  /// state from two of them. All three, together, here — not one init path per table
  /// that happens to agree with the other two. This runs first in `initNats()`, before
  /// the connection even opens, so nothing that needs them can race their creation.
  const initSyncState = async () => {
    await sql(`
      CREATE TABLE IF NOT EXISTS _zebridge_sync (
        id INTEGER PRIMARY KEY,
        global_last_lsn INTEGER,
        global_last_seq INTEGER
      );
    `);
    // One row per stream. The legacy single `global_last_seq` column is left in place and
    // unused: it cannot express two streams, and dropping it would break a replica written
    // by an older client.
    await sql(`
      CREATE TABLE IF NOT EXISTS _zebridge_stream_seq (
        stream TEXT PRIMARY KEY,
        last_seq INTEGER NOT NULL
      );
    `);
    await createOutboxTable();
    resolveOutboxInit();
    await sql(`INSERT OR IGNORE INTO _zebridge_sync (id, global_last_lsn, global_last_seq) VALUES (1, 0, 0)`);
    const res = await sql(`SELECT global_last_lsn, global_last_seq FROM _zebridge_sync WHERE id = 1`);
    if (res.length > 0) {
      globalSyncState.lsn = res[0].global_last_lsn ?? 0;
    }
    for (const r of await sql(`SELECT stream, last_seq FROM _zebridge_stream_seq`)) {
      globalSyncState.seq[(r as any).stream] = (r as any).last_seq ?? 0;
    }
  };

  /// PROTOCOL.md "The Connection Flow", Step 0: ask NATS who this principal's tenant
  /// is, rather than trusting a build-time constant. `$KV.tenants.<principal>` holds a
  /// single value, kept current by a trigger on `zebridge_user_tenants` — resolved
  /// fresh here on every connect, never cached across page loads or reconnects, which
  /// is what lets a tenant reassignment take effect without rebuilding this client.
  ///
  /// No entry is a normal outcome, not an error: a public-only principal, or an
  /// untenanted deployment. `TENANT` stays empty and `cdcStreams()` falls back
  /// accordingly.
  const resolveTenant = async () => {
    if (!nc) return;
    try {
      const kvm = new Kvm(nc);
      // ⚠️ `allow_direct` must be passed explicitly. `Kvm.open()` binds to an existing
      // bucket without ever asking the server whether its stream actually has
      // `allow_direct` set — `Bucket.bind()` just trusts this option, defaulting to
      // `false` when omitted. Omitting it here sent `get()` down the non-direct
      // `$JS.API.STREAM.MSG.GET.KV_tenants` path, selecting the key from the request
      // BODY — which is exactly the path `nats-server.conf.template` deliberately does
      // NOT grant, because it cannot be scoped per key. Measured: a Permissions
      // Violation on that exact subject, even though the stream itself already has
      // `allow_direct: true` (confirmed via `nats stream info KV_tenants`).
      const kv = await kvm.open(topology.kv.tenants, { allow_direct: true });
      const entry = await kv.get(PRINCIPAL);
      if (entry) {
        // A bare tenant name, not a JSON/msgpack payload like the schema and snapshot
        // buckets — `decode()` is tried first only so a future bridge-side encoding
        // change is not a silent break here too.
        let val: string;
        try { val = decode(entry.value) as string; } catch { val = td.decode(entry.value); }
        TENANT = val;
        setTenant(TENANT);
        appendLog('SYS', `Resolved tenant for '${PRINCIPAL}': ${TENANT}`, 'INFO');
      } else {
        appendLog('SYS', `No tenant mapping for '${PRINCIPAL}' — public-only reads`, 'INFO');
      }
    } catch (err) {
      appendLog('SYS', `Tenant resolution failed: ${err}`, 'ERROR');
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
      reach('connected');
      appendLog('SYS', `Connected to NATS over WebSockets as '${PRINCIPAL}'`);
      await watchSchemas();
      await watchVerdicts();
      await resolveTenant();
      await subscribeStreams();
      reach('cdc');

      // Anything queued while disconnected goes out now — including writes made in a
      // previous page load, which is the whole point of persisting them.
      void flushOutbox();

      // ⚠️ `reconnect: true` means the library restores the connection without coming back
      // through this function, so a flush placed only above would run once per page load
      // and never after a network blip. Watching the status is what covers the case the
      // outbox exists for.
      // ⚠️ A mobile/PWA client has no page refresh to fall back on — this loop is the
      // *only* thing that ever re-syncs it. Measured live: after a reconnect, CDC events
      // stopped arriving with no error anywhere, and only a full page reload brought the
      // replica current again. flushOutbox() alone doesn't fix that — it only resends
      // queued writes, it does nothing about the CDC consumers a stale connection left
      // behind. subscribeStreams() is safe to re-run here: its own gap check compares the
      // *persisted* last-processed sequence against what the stream currently retains, so
      // a normal reconnect (still within retention) just resumes CDC from where it left
      // off — cheap, no re-snapshot, no duplicate data applied (upserts are idempotent
      // even if it were). `resyncing` guards against overlapping calls if `reconnect`
      // events fire in a burst.
      //
      // ⚠️ Not fully verified: whether nats.js's own `reconnect: true` already transparently
      // resumes the *previous* call's consumers once the connection comes back, which would
      // make this create a second, redundant set alongside them rather than a clean
      // replacement. Harmless if so (upserts are idempotent, just wasted work), but worth
      // watching across repeated reconnects rather than assuming either way. Shared with
      // the active `nc.rtt()` poll below, not local here — see that declaration's comment.
      void (async () => {
        try {
          for await (const st of nc!.status()) {
            const t = String((st as any).type);
            if (t === 'reconnect') {
              setStatus('connected');
              if (resyncing) continue;
              resyncing = true;
              appendLog('SYS', 'NATS reconnected — flushing outbox and re-syncing streams', 'INFO');
              void flushOutbox();
              void subscribeStreams().finally(() => { resyncing = false; });
            } else if (t === 'disconnect') {
              setStatus('disconnected');
            }
          }
        } catch { /* connection closed; nothing to watch */ }
      })();
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
      const watch = await watchBucket(jetstream(nc), topology.kv.schemas);
      appendLog('SCHEMA', `Watching KV bucket "${topology.kv.schemas}" for all tables...`, 'WATCH');

      return new Promise<void>((resolve) => {
        let initialized = false;
        // Nothing published yet: there is no "last entry of the replay" to wait for.
        if (watch.pending === 0) { initialized = true; resolve(); }

        (async () => {
          for await (const entry of watch.entries) {
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

            // Schema is always JSON, never msgpack — a fixed bridge-side rule, not a
            // per-deployment choice (NOTES.md/PROTOCOL.md). No decode() attempt, no
            // double-unwrap: both the boot/DDL path and the on-demand init.schema
            // responder now emit the identical {table, pg, sqlite, pk, pk_columns} shape.
            const val: any = JSON.parse(td.decode(entry.value));

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
      refreshTableNames();
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
    // single-column form and is null for a composite key. Both live at the payload
    // ROOT — the key is a fact about the table, not the SQLite dialect (it used to sit
    // inside `val.sqlite`; moved while there is no installed base to keep a duplicate
    // alive for).
    const pkCols: string[] = Array.isArray(val.pk_columns)
      ? val.pk_columns
      : val.pk
        ? [val.pk]
        : [];
    const cols: { name: string; type: string }[] = val.sqlite.columns;
    const lsn: number = typeof val.lsn === 'number' ? val.lsn : 0;

    // Which column marks a row deleted, or null when this table deletes physically. The
    // bridge publishes it because it is a SYNC_RULES decision, not a schema one — the same
    // table can soft-delete in one deployment and not in another, so a client that guessed
    // from the column list would be wrong half the time.
    const tombstoneColumn: string | null =
      typeof val.tombstone_column === 'string' ? val.tombstone_column : null;
    // Which column scopes this table by tenant, or null for a table every principal
    // shares the same content of. See TableState.tenantColumn's own comment for why this
    // has to come from the bridge rather than `topology.json`'s `public_tables` list.
    const tenantColumn: string | null =
      typeof val.tenant_column === 'string' ? val.tenant_column : null;
    const names = cols.map((c) => c.name);

    const existing = syncedTables.get(table);

    // RENAME COLUMN hints from the bridge (§1.2): `val.renamed` maps new name → old
    // name, derived from pg_attribute.attnum (which survives a rename) by the DDL
    // trigger. Applied as a local RENAME below so existing values survive; without the
    // hint a rename reads as drop+add and every value of the column resets to NULL.
    // Only pairs the local table can actually honour: old name present, new name not.
    const renames: [string, string][] = existing
    ? Object.entries((val.renamed ?? {}) as Record<string, string>)
        .filter(([to, from]) => existing.columns.includes(from) && !existing.columns.includes(to))
        .map(([to, from]) => [from, to])
    : [];
    // The diff is computed as if the renames had already happened, so a renamed column
    // is "unchanged" rather than one removed + one added.
    const effectiveCols = existing
      ? existing.columns.map((n) => renames.find(([from]) => from === n)?.[1] ?? n)
      : [];
    const added = existing ? names.filter((n) => !effectiveCols.includes(n)) : [];
    const removed = existing ? effectiveCols.filter((n) => !names.includes(n)) : [];

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
      } else if (added.length === 0 && removed.length === 0 && renames.length === 0) {
        syncedTables.set(table, { pkCols, columns: names, lsn, tombstoneColumn, tenantColumn });
        refreshTableNames();
        reach('migrated');
        return; // identical schema, e.g. a boot republish
      } else {
        // ⚠️ The view goes first. SQLite's `ALTER TABLE … DROP COLUMN` re-validates every
        // schema object that references the table, and the copy-based fallback drops and
        // renames the table under it — with `<table>_view` still defined, both fail with
        // `error in view users_view: no such table: main.users` (NOTES.md §1.17). It is
        // recreated against the new column set below, so nothing is lost by dropping it
        // early; `ADD COLUMN` only worked because it never touches a referenced column.
        await sql(`DROP VIEW IF EXISTS ${table}_view;`);
        // Renames first, so the add/drop loops below and the copy-based fallback (which
        // matches columns by name) both see the post-rename names. SQLite has RENAME
        // COLUMN since 3.25.
        for (const [from, to] of renames) {
          await sql(`ALTER TABLE ${table} RENAME COLUMN "${from}" TO "${to}";`);
        }
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
            renames.length ? `~[${renames.map(([f, t]) => `${f}→${t}`).join(', ')}]` : null,
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

      syncedTables.set(table, { pkCols, columns: names, lsn, tombstoneColumn, tenantColumn });
      refreshTableNames();
      // ⚠️ Both registration paths mark the phase. Marking only one left `migrated`
      // grey on a client that took the other, while `snapshot` went green after it —
      // a strip that lies is worse than no strip.
      reach('migrated');

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

  /// INS green, UP orange, DEL red — held for 500ms, then cleared.
  ///
  /// ⚠️ A soft delete arrives as an **UPDATE** with the tombstone set (§7.5), so it is
  /// shown as DEL rather than UP: the verb a user cares about is the intent, not the SQL.
  /// The last CDC verb for a table, held until the next event replaces it.
  ///
  /// ⚠️ Deliberately persistent, not a flash. It used to clear after 500 ms, which meant
  /// the column was blank almost all the time: on a quiet table you had to be looking at
  /// the exact half-second an event landed to see anything at all. Holding the last verb
  /// makes the row answer "what happened here, and what kind of change was it?" at any
  /// moment, which is the question it exists to answer.
  const markVerb = (table: string, ev: any) => {
    const state = syncedTables.get(table);
    const tombstoned = state?.tombstoneColumn && ev?.data?.[state.tombstoneColumn] != null;
    const verb = tombstoned ? 'DEL'
      : ev.operation === 'INSERT' ? 'INS'
      : ev.operation === 'DELETE' ? 'DEL'
      : 'UP';
    setLastVerb((prev) => ({ ...prev, [table]: verb }));
    if (logging()[table]) console.log(`[${table}] ${verb}`, ev.data);
  };

  /// `exec` defaults to the module-level `sql`; a batched CDC flush or an optimistic apply
  /// passes `tx.sql` instead, so several events land in one SQLite transaction rather than
  /// one round trip each — see the CDC consume loop and `sendMutation`.
  ///
  /// `ev.optimistic` marks a synthetic event built from a mutation this client is about to
  /// send, not a real CDC/snapshot row: `lsn` is a sentinel (`Number.MAX_SAFE_INTEGER`, so
  /// the gate below never blocks it — `state.lsn` itself is only ever written by real
  /// snapshot/CDC application, never read back from an optimistic call, so this cannot
  /// corrupt what a later real event is compared against) and the echo-pop block at the
  /// bottom must not run for it — that block exists to recognise *our own write coming back*,
  /// and an optimistic call is the write going out, not coming back.
  const applyEvent = async (table: string, ev: any, exec: typeof sql = sql) => {
    const state = syncedTables.get(table);
    if (!state || !ev?.data) return;
    markVerb(table, ev);
    scheduleRecount();

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
            await exec(`DELETE FROM ${table} WHERE ${where}`, ...oldPk);
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
        await exec(query, ...values);
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
          await exec(`DELETE FROM ${table} WHERE ${where}`, ...pkVals);
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
    if (!ev.optimistic && state.pkCols.length && pendingWrites.size) {
      const echoedKey = state.pkCols.map((c) => String(ev.data?.[c])).join('|');
      for (const [msgId, w] of pendingWrites) {
        if (w.table !== table || String(w.id) !== echoedKey) continue;
        pendingWrites.delete(msgId);
        void outboxDrop(msgId);
        appendLog(table, `confirmed by CDC echo after ${Date.now() - w.at}ms`, 'CONFIRMED');
      }
    }

    // ⚠️ Optimistic events never reach here for resume bookkeeping — `ev.lsn` is a
    // sentinel (`Number.MAX_SAFE_INTEGER`), not a real WAL position. Letting it through
    // would set `globalSyncState.lsn` to the sentinel permanently: every later *real* CDC
    // event compares `< MAX_SAFE_INTEGER` and the resume position would never advance
    // again.
    const stream: string = ev.stream ?? "";
    const streamSeq = stream ? (globalSyncState.seq[stream] ?? 0) : 0;
    if (!ev.optimistic && ((ev.lsn ?? 0) > globalSyncState.lsn || (ev.seq ?? 0) > streamSeq)) {
      globalSyncState.lsn = Math.max(globalSyncState.lsn, ev.lsn ?? 0);
      if (stream) globalSyncState.seq[stream] = Math.max(streamSeq, ev.seq ?? 0);
      try {
        await exec(`UPDATE _zebridge_sync SET global_last_lsn = ? WHERE id = 1`, globalSyncState.lsn);
        if (stream) {
          await exec(
            `INSERT INTO _zebridge_stream_seq (stream, last_seq) VALUES (?, ?)
             ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq`,
            stream, globalSyncState.seq[stream]
          );
        }
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
      // ⚠️ Asked of EVERY stream this client reads, not one. A gap in any of them means the
      // replica is missing rows, and checking only the tenant stream would miss a public
      // table that aged out — the failure would look like a table that is simply empty.
      let gap = false;
      const gapDetail: string[] = [];
      for (const streamName of cdcStreams()) {
        const info = await jsm.streams.info(streamName);
        const firstSeq = info.state.first_seq;
        const local = globalSyncState.seq[streamName] ?? 0;
        if (local === 0 || (firstSeq > 0 && local < firstSeq - 1)) {
          gap = true;
          gapDetail.push(`${streamName}: local ${local}, stream first ${firstSeq}`);
        }
      }
      if (gap) {
        appendLog('SYS', `Gap detected or first run! ${gapDetail.join('; ')}. Snapshots required!`, 'WARNING');
          
          let snapKv;
          try {
            const kvm = new Kvm(nc!);
            // ⚠️ `allow_direct` must be passed explicitly — `Kvm.open()`/`Bucket.bind()`
            // defaults it to `false` without ever asking the server, sending get()/watch()
            // down the unscopable `$JS.API.STREAM.MSG.GET.KV_snapshots` path instead of
            // Direct Get. Unlike `$KV.schemas` (deliberately wholesale-granted — schema
            // disclosure stays unscoped by design), `$KV.snapshots` grants ONLY the
            // exact-key Direct Get path per tenant, so the wholesale path is a genuine
            // Permissions Violation here, not a fallback. Same lesson as `resolveTenant()`'s
            // `$KV.tenants` open above, never applied to this bucket until now — see
            // NOTES.md §1.12 part 3.
            snapKv = await kvm.open(topology.kv.snapshots, { allow_direct: true });
          } catch { /* ignore */ }

          // Tenant-scoped iff topology.json declares `init_streams` — the same signal
          // `initStream()` and `cdcStreams()` gate on. When true, every snapshot subject
          // and KV key below carries `TENANT` as its own token (PROTOCOL.md "The
          // Connection Flow" §2): `snapshot.request.<tenant>.<table>`,
          // `$KV.snapshots.<tenant>.<table>`, `init.snap.<tenant>.<table>.<snapshot_id>.>`.
          const tenanted = !!(topology as any).init_streams;

          const tablesToSnap = new Set(syncedTables.keys());
          const snapshotPromises = [];

          for (const table of tablesToSnap) {
            // `TENANT` for a table `TENANT_RULES` actually scopes; `topology.open_tenant`
            // for one it doesn't (system tables, tenant-agnostic business tables) — every
            // client converges on the same shared entry for the latter instead of each
            // caching its own redundant copy.
            const tenantForTable = tenanted ? effectiveTenantFor(table) : '';
            const snapKey = tenanted ? `${tenantForTable}.${table}` : table;
            let desc: any = null;
            if (snapKv) {
              try {
                const entry = await snapKv.get(snapKey);
                if (entry) {
                  const candidate = decode(entry.value); // descriptor is always msgpack, never JSON
                  if (await descriptorStillFresh(js, tenantForTable, candidate)) {
                    desc = candidate;
                  } else {
                    appendLog('SYS', `Cached snapshot for ${table} is orphaned (CDC no longer covers its watermark) — requesting a fresh one instead`, 'WARNING');
                  }
                }
              } catch (e) {}
            }

            if (!desc && snapKv) {
              const reqSubject = tenanted
                ? `${topology.subjects.snapshot_request}.${tenantForTable}.${table}`
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

                desc = await waitForDescriptor(js, topology.kv.snapshots, snapKey, SNAPSHOT_WAIT_MS);
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
                refreshTableNames();
                failedTables.add(table);
                appendLog('SYS', `Giving up on ${table} after ${SNAPSHOT_REQUEST_ATTEMPTS} attempts — NOT following CDC for it, the local copy would silently diverge. If the table was added to the publication after the bridge started, restart the bridge.`, 'ERROR');
              }
            }

            if (desc) {
              appendLog('SYS', `Snapshot metadata ready for ${table} (LSN ${desc.lsn}, ${desc.row_count ?? '?'} rows). Replaying...`, 'INFO');
              reach('snapshot');
              
              const pullPromise = (async () => {
                // ⚠️ The whole body is wrapped, not just the `fetch()` calls below.
                // `fetch()`'s own `.catch()` only guards *starting* a pull — the
                // `for await (const msg of batch)` iteration is a separate async
                // iterator that can itself throw mid-stream (`JetStreamError:
                // heartbeats missed`) if the pull's internal heartbeat is missed even
                // once, which a short `expires` window makes more likely on the very
                // first pull against a freshly-created consumer. Uncaught, that throw
                // used to propagate out of this IIFE, reject the whole
                // `Promise.all(snapshotPromises)` below, and abandon every *other*
                // table's replay too — not just the one that hit the hiccup.
                try {
                  await sql(`DELETE FROM ${table}`);

                  const stream = tenanted ? initStream(tenantForTable) : topology.streams.init;
                  const filterSubject = tenanted
                    ? `${topology.subjects.init_prefix}.snap.${tenantForTable}.${table}.${desc.snapshot_id}.>`
                    : `${topology.subjects.init_prefix}.snap.${table}.${desc.snapshot_id}.>`;
                  const ci = await jsm.consumers.add(stream, {
                    filter_subject: filterSubject,
                    deliver_policy: DeliverPolicy.All,
                  });
                  const replayConsumer = await js.consumers.get(stream, ci.name);

                  // Use fetch to reliably pull all chunks currently in the stream for this snapshot
                  let done = false;
                  let snapshotColumns: string[] | null = null;
                  let rowsApplied = 0;

                  while (!done) {
                    // ⚠️ 5s, not 1s. A 1s window left too little margin for the first
                    // pull's heartbeat on a consumer that had just been created —
                    // measured: `JetStreamError: heartbeats missed` on an otherwise
                    // healthy connection, purely from the timing being tight.
                    const batch = await replayConsumer.fetch({ max_messages: 100, expires: 5000 }).catch(() => null);
                    if (!batch) break;

                    let receivedCount = 0;
                    for await (const msg of batch) {
                      receivedCount++;
                      let chunkDecoded: any;
                      try {
                        chunkDecoded = decode(msg.data); // snapshot chunks are always msgpack, never JSON
                      } catch (err) {
                        // ⚠️ Caught and logged, not left to throw. The bridge only ever
                        // produces msgpack here (verified), but an uncaught throw inside
                        // this `for await` would silently kill the whole replay loop for
                        // this table — no more chunks processed, no error visible anywhere
                        // except an easy-to-miss unhandled rejection. A corrupt/unexpected
                        // message should skip itself loudly, not take the table down with it.
                        appendLog('SYS', `Snapshot chunk for ${table} failed to decode (seq ${msg.seq}): ${err} — skipping this message`, 'ERROR');
                        msg.ack();
                        continue;
                      }

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
                          rowsApplied++;
                        }
                      } else if (state && chunkDecoded.operation === 'snapshot' && chunkDecoded.data) {
                        // Legacy JSON format fallback
                        for (const row of chunkDecoded.data) {
                          await applyEvent(table, { table, operation: 'INSERT', data: row, lsn: desc.lsn });
                          rowsApplied++;
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
                  // Compares against the descriptor's own row_count — a mismatch here means
                  // the snapshot under- or over-delivered, which is exactly the kind of thing
                  // that otherwise only shows up later as "the count looks wrong" with no clue
                  // why.
                  const rowCountNote = desc.row_count != null && desc.row_count !== rowsApplied
                    ? ` ⚠️ expected ${desc.row_count} rows per the descriptor`
                    : '';
                  appendLog('SYS', `Replay finished for ${table} (Snapshot ID: ${desc.snapshot_id}, ${rowsApplied} rows applied${rowCountNote})`, 'INFO');
                } catch (err) {
                  // Same "unseeded is not the same as synced" rule as the no-descriptor
                  // case above: a table that failed mid-replay must not go on receiving
                  // CDC against a partial or absent local copy.
                  syncedTables.delete(table);
                  refreshTableNames();
                  failedTables.add(table);
                  appendLog('SYS', `Replay of ${table} failed: ${err} — NOT following CDC for it, the local copy would silently diverge.`, 'ERROR');
                }
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
      // ⚠️ **One consumer per stream, because a consumer belongs to exactly one stream.**
      // CDC is split per tenant (CDC_ACME, CDC_GLOBEX) plus a shared CDC_PUBLIC for tables
      // with no tenant column. That split is a security boundary, not organisation: a
      // subject permission governs core SUBSCRIBE only — a JetStream consumer never
      // subscribes to the subject it filters on, so `cdc.acme.>` in the allow-list could not
      // stop a client creating a consumer on a shared stream filtered at another tenant.
      // Measured before the split: alice pulled `cdc.globex.test_types.insert` with her own
      // credentials (`scripts/scenarios/crosstenant.py`).
      //
      // ⚠️ This replaces a single `filter_subjects: [cdc.<tenant>.>, cdc.users.>]` consumer.
      // That worked and was not safe — the filter was doing the job a grant should do.
      //
      // ⚠️ No ordering guarantee ACROSS streams, and none is needed: they carry disjoint
      // tables, so no row is described by both. Ordering *within* a table still comes from
      // its own stream.
      for (const streamName of cdcStreams()) {
        // ⚠️ Timed, not guessed. This project's own rule: measure before assuming which
        // part of a "few seconds" delay is actually slow (the consumer round trip, message
        // delivery, or applying them) rather than fixing whichever one sounds plausible.
        const setupStart = performance.now();
        const last = globalSyncState.seq[streamName] ?? 0;
        const ci = await jsm.consumers.add(streamName, {
          // No filter: the stream itself is now the filter. Narrowing further here would
          // only re-introduce a reader-chosen boundary.
          deliver_policy: last > 0 ? DeliverPolicy.StartSequence : DeliverPolicy.All,
          opt_start_seq: last > 0 ? last + 1 : undefined,
        });
        const consumer = await js.consumers.get(streamName, ci.name);
        const setupMs = Math.round(performance.now() - setupStart);
        // `num_pending` is how many messages this consumer has yet to deliver — the real
        // backlog size, not just "from seq all". A fresh client on a stream carrying old
        // burst-test history (CDC_PUBLIC has held 67k+ leftover messages before) silently
        // has to iterate all of it before reaching anything current; without this number
        // that looks identical to "nothing is happening" from the log alone.
        appendLog('SYS', `CDC consumer on ${streamName} (from seq ${last || 'all'}, ${ci.num_pending ?? '?'} messages pending, consumer setup took ${setupMs}ms)`, 'INFO');

        const iter = await consumer.consume();
        void (async () => {
          // Throttled, not per-message — appendLog already skips INSERT/UPDATE/DELETE/CDC
          // individually for exactly this reason (protecting the UI thread on a burst), but
          // that leaves catch-up on a large backlog completely invisible: no error, no
          // progress, indistinguishable from actually being stuck. This logs every 2000
          // messages regardless of opType, so "still working, N/pending processed" is
          // visible without reintroducing a log line per row.
          let processedSinceStart = 0;
          let caughtUpLogged = ci.num_pending === 0;
          const progressEvery = 2000;

          // ⚠️ Batched into one `sql.transaction`, not one autocommitting `sql()` call per
          // event. Measured this session: 9–43 CDC messages — genuinely tiny — still took
          // multiple seconds to replay. `SQLocal`'s `tx.sql()` is still one Worker round trip
          // per statement even inside a transaction (checked its own source —
          // `transactionKey` only wraps the calls in one BEGIN/COMMIT, it does not merge the
          // postMessage calls themselves), so this does not cut round-trip count. What it
          // does cut is per-write commit/fsync overhead to OPFS — N autocommitting
          // statements each pay that, one transaction of N pays it once — which is the
          // well-known reason "wrap many writes in one transaction" is a real SQLite win
          // regardless of the transport underneath. If round-trip count itself turns out to
          // still matter after this, `TransactionHandle.batch` sends multiple statements in
          // one message and would need `applyEvent` restructured to return statements rather
          // than execute them — not done here, this pass only threads through `exec`.
          // Flushing every `BATCH_SIZE` events, or every `BATCH_MS` if fewer have arrived,
          // mirrors why `batch_publisher.zig`'s `flushLoop` batches on the bridge side, same
          // shape, client side. Messages are only ack'd after their batch's transaction has
          // actually committed — acking first and losing the write between the two would
          // be worse than the seconds this replaces.
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
              await transaction(async (tx) => {
                for (const { table, ev } of toApply) {
                  await applyEvent(table, ev, tx.sql);
                }
              });
            } catch (err) {
              appendLog('SYS', `${streamName} batch of ${toApply.length} event(s) failed to apply: ${err}`, 'ERROR');
            }
            for (const m of toAck) m.ack();
          };

          for await (const msg of iter) {
            processedSinceStart++;
            if (processedSinceStart % progressEvery === 0) {
              appendLog('SYS', `${streamName} catch-up: ${processedSinceStart} messages processed so far, at seq ${msg.seq}`, 'INFO');
            }
            // The backlog moment has no other marker otherwise — the consumer just keeps
            // running forever, live and catch-up look identical from outside this loop.
            if (!caughtUpLogged && ci.num_pending != null && processedSinceStart >= ci.num_pending) {
              caughtUpLogged = true;
              const totalMs = Math.round(performance.now() - setupStart);
              appendLog('SYS', `${streamName} caught up (${processedSinceStart} messages, ${totalMs}ms total since consumer setup started) — now live`, 'INFO');
            }
            let decoded: any;
            try {
              decoded = decode(msg.data); // CDC events are always msgpack, never JSON
            } catch (err) {
              // ⚠️ Caught and logged, not left to throw. This `for await` runs inside a
              // fire-and-forget `void (async () => {...})()` — the outer try/catch around
              // consumer setup cannot reach an error thrown in here, since this IIFE isn't
              // awaited. An uncaught throw would silently end CDC consumption for this
              // *entire stream* (every table on it) with nothing visible but an
              // easy-to-miss unhandled rejection — not just skip the one bad message.
              appendLog('SYS', `CDC event on ${streamName} failed to decode (seq ${msg.seq}): ${err} — skipping this message`, 'ERROR');
              msg.ack();
              continue;
            }
            const events = Array.isArray(decoded) ? decoded : [decoded];

            for (const ev of events) {
              ev.seq = msg.seq;
              // Which stream this came from — sequences are per stream, so advancing the
              // wrong one silently skips or replays.
              ev.stream = streamName;
              // cdc.<tenant>.<table>.<op> has the table at [2]; the bare
              // cdc.<table>.<op> has it at [1]. Taking [1] unconditionally yielded the
              // *tenant* for routed subjects — harmless only while ev.table is present.
              const parts = msg.subject.split('.');
              const table = ev?.table || (parts.length >= 4 ? parts[2] : parts[1]);
              appendLog(msg.subject, ev, ev?.operation || 'CDC');
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
const CLIENT_ID = `c-${crypto.randomUUID().slice(0, 8)}`;
// ⚠️ Per **tab**, not per user. `PRINCIPAL` is the account NATS authenticated; this is the
// writer, and last-write-wins breaks an equal-version tie on it (§7.3). Two tabs sharing
// one constant — which is what this was — cannot have a tie broken between them, so two
// replicas of the same row can stay divergent forever with no error. That is exactly the
// two-window demo.
//
// Per-load is consistent with the replica, which is also per-load (see DB_NAME). The
// moment the replica becomes durable this has to move into it — see NOTES.md §1.9, which
// explains why `localStorage` is the wrong home.

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


  /// The three buttons on `test_types`. Each sends one mutation and returns immediately —
  /// ⚠️ **the count does not update here**. It updates when the CDC echo is applied
  /// (`scheduleRecount`), because that is when the row actually exists locally. Counting
  /// in a `.finally` on the publish would read the number from before the write.
  const insertRandom = async () => {
    if (!nc) return;
    const id = uuidv7();
    const version = versionNow();
    await sendMutation('test_types', 'INSERT', id, version, {
      key: { uid: id },
      data: {
        uid: id,
        some_text: `random ${Math.floor(Math.random() * 10_000)}`,
        age: Math.floor(Math.random() * 90),
        is_true: Math.random() > 0.5,
        // `TENANT` is resolved in `resolveTenant()` before this client subscribes to
        // anything (PROTOCOL.md Step 0), so it is already known by the time a write can
        // happen — unlike the old row-inferred value, there is no cold-start gap here.
        tenant_id: TENANT || undefined,
        updated_at: version,
        inserted_at: version,
      },
      version,
      client_id: CLIENT_ID,
    });
  };

  /// The most recently touched live row, read from the local replica.
  ///
  /// ⚠️ `deleted_at IS NULL` — a soft-deleted row is still here (§7.5), and updating one
  /// would be writing to something the user already deleted.
  const lastLiveUid = async (): Promise<string | null> => {
    try {
      const r = await sql(
        `SELECT uid FROM test_types WHERE deleted_at IS NULL ORDER BY updated_at DESC LIMIT 1`,
      );
      return r[0]?.uid ?? null;
    } catch {
      return null;
    }
  };

  const updateLast = async () => {
    if (!nc) return;
    const uid = await lastLiveUid();
    if (!uid) return appendLog('SYS', 'nothing live to update', 'WARNING');
    const version = versionNow();
    await sendMutation('test_types', 'UPDATE', uid, version, {
      key: { uid },
      data: { uid, some_text: `updated ${new Date().toLocaleTimeString()}`, updated_at: version },
      version,
      client_id: CLIENT_ID,
    });
  };

  /// ⚠️ A delete from the edge is a **soft** delete (§7.5): it arrives back as an UPDATE
  /// setting the tombstone, not as a CDC delete. That is why the verb reads the
  /// tombstone rather than the operation.
  const deleteLast = async () => {
    if (!nc) return;
    const uid = await lastLiveUid();
    if (!uid) return appendLog('SYS', 'nothing live to delete', 'WARNING');
    const version = versionNow();
    await sendMutation('test_types', 'DELETE', uid, version, {
      key: { uid },
      version,
      client_id: CLIENT_ID,
    });
  };

  /// +1/-1 on the one row a counter table holds. No tombstone, no delete button — a
  /// counter only ever has an INSERT (first click, no row yet) or an UPDATE (every click
  /// after). The two must send different columns: INSERT needs `inserted_at` to satisfy
  /// the column's NOT NULL with no default; UPDATE must NOT resend it — the exact
  /// NOT-NULL-satisfying-only-what-changed lesson `applyUpdate` exists for, except here
  /// it falls out naturally from only including the field when it's actually needed.
  const bumpCounter = async (table: 'counter_public' | 'counter_tenant', delta: number) => {
    if (!nc) return;
    let row: { uid: string; value: number } | null = null;
    try {
      const r = await sql(`SELECT uid, value FROM ${table} LIMIT 1`);
      row = r[0] ? { uid: r[0].uid, value: r[0].value } : null;
    } catch {
      /* table not ready yet */
    }

    const id = row?.uid ?? uuidv7();
    const nextValue = (row?.value ?? 0) + delta;
    const version = versionNow();
    const data: Record<string, unknown> = { uid: id, value: nextValue, updated_at: version };
    if (table === 'counter_tenant') data.tenant_id = TENANT || undefined;
    if (!row) data.inserted_at = version;

    await sendMutation(table, row ? 'UPDATE' : 'INSERT', id, version, {
      key: { uid: id },
      data,
      version,
      client_id: CLIENT_ID,
    });
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

    // A suspended table (`$KV.schemas` said `suspended: true` — NOTES.md §1.6c) has no
    // CDC path: the bridge drops every event for it at the source, refused or not. An
    // optimistic write here would apply locally and then never get a confirming or
    // correcting echo — permanently, not just until a retry — so it is refused
    // client-side instead of ever being queued.
    const suspendReason = suspended()[table];
    if (suspendReason) {
      appendLog(table, `write refused: table is suspended upstream (${suspendReason}) — no CDC echo would ever confirm it`, 'ERROR');
      return;
    }

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

    // ⚠️ Persisted **before** the publish, not after. If the tab dies between the two, a
    // stored write that never reached JetStream is replayed harmlessly on the next load
    // (the msg_id makes it idempotent); a write that reached JetStream but was never
    // stored is simply lost. Ordering it this way trades a possible duplicate — which
    // dedup collapses — for a possible loss, which nothing can recover.
    //
    // The outbox insert and the optimistic apply happen in **one transaction** (PROTOCOL.md
    // §7.1) — either both land or neither does, so there is never a visible row with no
    // queued intent behind it, or a queued intent with nothing shown. `DELETE`'s `payload`
    // carries `key`, never `data` (there is nothing to upsert), so the synthetic event reads
    // from whichever one applies. `lsn: Number.MAX_SAFE_INTEGER` + `optimistic: true` are
    // `applyEvent`'s own contract for this — see its doc comment.
    try {
      await transaction(async (tx) => {
        // Snapshot the row exactly as it stands right now, in the same transaction as
        // the optimistic apply — so a later revert restores what was really there, not
        // whatever the replica happened to hold by the time the verdict arrives.
        const state = syncedTables.get(table);
        let before: any = null;
        if (state?.pkCols.length) {
          const keyObj = (payload as any).key as Record<string, unknown> | undefined;
          if (keyObj) {
            const where = state.pkCols.map((c) => `"${c}" = ?`).join(' AND ');
            const pkVals = state.pkCols.map((c) => keyObj[c]);
            const existing = await tx.sql(`SELECT * FROM ${table} WHERE ${where}`, ...pkVals);
            before = existing[0] ?? null;
          }
        }
        await outboxPut({ msgId, subject, payload, table, id, before }, tx.sql);
        await applyEvent(table, {
          table,
          operation: op,
          data: op === 'DELETE' ? (payload as any).key : (payload as any).data,
          lsn: Number.MAX_SAFE_INTEGER,
          optimistic: true,
        }, tx.sql);
      });
    } catch (err) {
      // Nothing was queued and nothing was shown — safe to stop here rather than publish a
      // write this client cannot itself account for.
      pendingWrites.delete(msgId);
      appendLog(subject, `optimistic apply failed, write not sent: ${err}`, 'ERROR');
      return;
    }

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

  /// Republish everything still in the outbox.
  ///
  /// This is what makes the queue an outbox rather than a log: writes made while the
  /// connection was down — or made and then abandoned when the tab closed — are sent when
  /// there is somewhere to send them.
  ///
  /// ⚠️ Safe to run repeatedly, and that is load-bearing. Each entry keeps its **original
  /// `Nats-Msg-Id`**, so JetStream dedup collapses a replay of a write that did land into
  /// the original (`duplicate: true` on the PubAck, which is a success). Minting a fresh
  /// id per attempt would defeat that and write the row twice.
  ///
  /// Entries are not dropped on failure — only a verdict or a CDC echo pops the queue
  /// (§7.1). A write that cannot be sent stays for the next connection.
  const flushOutbox = async () => {
    let rows: any[] = [];
    try {
      rows = await outboxAll();
    } catch (err) {
      appendLog('OUTBOX', `could not be read: ${err}`, 'ERROR');
      return;
    }
    if (!rows.length) return;
    appendLog('OUTBOX', `replaying ${rows.length} unconfirmed write(s)`, 'INFO');
    for (const r of rows) {
      if (!nc) return;
      try {
        const h = headers();
        h.set('Nats-Msg-Id', r.msg_id);
        // Re-track it so the verdict and echo handlers can still pop it by msg_id; without
        // this a replayed write would be confirmed by nothing and swept as unconfirmed.
        pendingWrites.set(r.msg_id, { table: r.tbl, id: r.row_id, at: Date.now() });
        const ack = await jetstream(nc).publish(r.subject, encode(JSON.parse(r.payload)), { headers: h });
        appendLog('OUTBOX', `replayed ${r.msg_id} (seq ${ack.seq}${ack.duplicate ? ', duplicate — already landed' : ''})`, 'INFO');
      } catch (err) {
        appendLog('OUTBOX', `replay of ${r.msg_id} failed, kept for next connection: ${err}`, 'ERROR');
      }
    }
  };

  if (typeof window !== 'undefined') {
    window.zb.outbox = outboxAll;
    window.zb.flushOutbox = () => flushOutbox();
  }

  initNats();

    // ⚠️ No longer a 1-second poll of every row. Counts are recomputed when CDC lands
    // (`scheduleRecount`), so an idle client does no work at all; this interval only asks
    // the bridge how it is doing.
    void recount();

    /// ⚠️ Fetched through the Vite proxy (`/bridge/*`), never at `127.0.0.1:9090` directly.
    /// This page sets `Cross-Origin-Embedder-Policy: require-corp` for OPFS, and under COEP
    /// a cross-origin response needs CORS *and* CORP headers — which the bridge does not
    /// send, and should not, since the same server exposes an unauthenticated `/metrics`.
    let healthWarned = false;
    const pollHealth = async () => {
      try {
        const r = await fetch('/bridge/health', { signal: AbortSignal.timeout(3000) });
        setHealth(r.ok ? 'up' : 'down');
        healthWarned = false;
      } catch (err: any) {
        // The bridge being unreachable is not the same as NATS being down — they fail
        // independently, which is the point of showing both badges.
        setHealth('down');
        // Once per outage, not once per poll: the reason matters and the repetition does
        // not. `ERR_INTERNET_DISCONNECTED` here while NATS stays connected is almost always
        // DevTools' Network tab set to Offline — it blocks fetch but leaves an open
        // WebSocket alive, which is exactly that combination.
        if (!healthWarned) {
          healthWarned = true;
          appendLog('SYS', `bridge /health unreachable: ${err?.message ?? err}`, 'WARNING');
        }
      }
    };
    void pollHealth();                                   // immediately, not in 10s
    const intervalId = setInterval(pollHealth, 10_000);
    const sweepId = setInterval(sweepPendingWrites, 1000);

    /// The NATS-side counterpart to `pollHealth` above — same reason, same shape: `status`
    /// was previously updated only by `nc.status()`'s own event stream, which reacts to
    /// what the WebSocket *reports* about itself, not to what actually is true. A frozen
    /// `nats-server` (measured live: `kill -STOP`) can leave the WebSocket object believing
    /// it is still open, with no 'disconnect' event ever fired to react to — `nc.rtt()`
    /// sends a real PING and waits for a PONG, so it fails when the connection genuinely
    /// isn't answering, whether or not the transport layer has noticed yet.
    ///
    /// Only acts on a **transition**, not every poll: repeatedly re-triggering a resync
    /// while already known-down would just queue redundant `subscribeStreams()` calls
    /// behind the `resyncing` guard for no benefit. The transition back to healthy is what
    /// matters — that is the moment `nc.status()` might never announce on its own, so it is
    /// the one this poll exists to catch.
    let naturallyConnected = true;
    const pollNatsRtt = async () => {
      if (!nc) return;
      try {
        await Promise.race([
          nc.rtt(),
          new Promise<never>((_, reject) => setTimeout(() => reject(new Error('rtt timeout')), 3000)),
        ]);
        if (!naturallyConnected) {
          naturallyConnected = true;
          setStatus('connected');
          appendLog('SYS', 'NATS rtt check recovered — re-syncing (nc.status() never reported this)', 'INFO');
          if (!resyncing) {
            resyncing = true;
            void flushOutbox();
            void subscribeStreams().finally(() => { resyncing = false; });
          }
        }
      } catch (err) {
        if (naturallyConnected) {
          naturallyConnected = false;
          setStatus('disconnected');
          appendLog('SYS', `NATS rtt check failed — connection is not actually answering: ${err}`, 'WARNING');
        }
      }
    };
    void pollNatsRtt();
    const rttIntervalId = setInterval(pollNatsRtt, 10_000);

    onCleanup(() => {
      clearInterval(intervalId);
      clearInterval(sweepId);
      clearInterval(rttIntervalId);
      if (nc) nc.close();
    });

    return (
      <>
        <header>
          <h1>ZeBridge CDC Web Consumer</h1>
          <div class="status-bar">
            <span class={`badge ${status()}`}>{status().toUpperCase()}</span>
            <span id="server-url">{NATS_URL}</span>
            <span class={`badge ${health() === 'up' ? 'connected' : 'disconnected'}`}>
              bridge {health()}
            </span>
          </div>

          {/* The startup state machine. Each cell greens once and stays green — the useful
              signal is how FAR it got, which a log tail cannot show at a glance. */}
          <ul class="phases">
            <For each={[
              ['connected', 'NATS connected'],
              ['migrated', 'schema migrated'],
              ['snapshot', 'snapshot replayed'],
              ['cdc', 'CDC active'],
            ]}>
              {([key, label]) => (
                <li classList={{ done: phase()[key] }}>{label}</li>
              )}
            </For>
          </ul>
        </header>

        <For each={Object.entries(suspended())}>
          {([table, reason]) => (
            <div class="suspended-banner">
              ⏸ <strong>{table}</strong> is suspended upstream ({reason}). Local rows are frozen and
              still valid, but no new events or snapshots will arrive, and writes are refused
              client-side, until the shape is fixed.
            </div>
          )}
        </For>

        <main>
          <h3>Replica</h3>
          <p id="sync-state">
            principal <strong>{PRINCIPAL}</strong> · client <strong>{CLIENT_ID}</strong>
            {' '}· tenant <strong>{tenant()}</strong>
            {' '}· held {pendingCount()}
          </p>
          <table class="tables-summary">
            <thead>
              <tr><th>table</th><th>rows</th><th>last CDC</th><th>log</th><th>actions</th></tr>
            </thead>
            <tbody>
              <For each={syncedTableNames()}>
                {(t) => (
                  <tr>
                    <td><strong>{t}</strong></td>
                    <td class="count">{counts()[t] ?? 0}</td>
                    {/* Last CDC verb, held until the next: green INS, orange UP, red DEL. */}
                    <td>
                      <span class={`verb ${lastVerb()[t] ? 'verb-' + lastVerb()[t] : ''}`}>
                        {lastVerb()[t] || ''}
                      </span>
                    </td>
                    <td>
                      <input
                        type="checkbox"
                        checked={!!logging()[t]}
                        onChange={(e) =>
                          setLogging((prev) => ({ ...prev, [t]: e.currentTarget.checked }))
                        }
                      />
                    </td>
                    <td>
                      {t === 'test_types' ? (
                        <span class="row-actions">
                          <button onClick={() => void insertRandom()}>INS</button>
                          <button onClick={() => void updateLast()}>UP</button>
                          <button onClick={() => void deleteLast()}>DEL</button>
                        </span>
                      ) : (
                        <em class="readonly">read-only</em>
                      )}
                    </td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>

          <div class="controls">
            <button onClick={recount} style="background: #37474f;">Recount</button>
          </div>

          {/* One row per demo, not one flat button strip — the three test_types buttons
              are a set (accepted / refused by grant / refused by shape), and the two
              counters are the offline-first walkthrough fixture: same +/- shape, one
              public (every principal shares the row), one tenant-scoped (RLS-filtered,
              one row per tenant) — see the schema's own `tenant_column` for which is
              which, not a name convention. */}
          <div class="controls">
            <span>test_types:</span>
            <button onClick={() => void insertRandom()} style="background: #2e7d32;">Push — accepted</button>
            <button onClick={() => void updateLast()} style="background: #1565c0;">Update last row</button>
            <button onClick={() => void deleteLast()} style="background: #ef6c00;">Delete last row</button>
          </div>

          <div class="controls">
            <span>counter (public):</span>
            <button onClick={() => void bumpCounter('counter_public', -1)} style="background: #ef6c00;">-</button>
            <strong>{counterValues()['counter_public'] ?? 0}</strong>
            <button onClick={() => void bumpCounter('counter_public', 1)} style="background: #2e7d32;">+</button>
          </div>

          <div class="controls">
            <span>counter (tenant):</span>
            <button onClick={() => void bumpCounter('counter_tenant', -1)} style="background: #ef6c00;">-</button>
            <strong>{counterValues()['counter_tenant'] ?? 0}</strong>
            <button onClick={() => void bumpCounter('counter_tenant', 1)} style="background: #2e7d32;">+</button>
          </div>

          <div class="controls">
            <button onClick={publishToReadOnlyTable} style="background: #6a1b9a;">Push to users — refused</button>
            <button onClick={publishMalformed} style="background: #b71c1c;">Push malformed — verdict now</button>
          </div>
        </main>
      </>
    );
}
