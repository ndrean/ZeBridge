import { createSignal, onCleanup, For } from 'solid-js';
import { wsconnect, NatsConnection, nkeyAuthenticator } from '@nats-io/nats-core';
import { jetstream, jetstreamManager, DeliverPolicy } from '@nats-io/jetstream';
import { Kvm } from '@nats-io/kv';
import { decode, encode } from '@msgpack/msgpack';
import topology from '../../topology.json';
import { SQLocal } from 'sqlocal';

const NATS_URL = 'ws://localhost:8080';
const NKEY_SEED = 'SUAPSL67RKOUDZFREHHDWUXDXLYZKEHMWEXMIUC35Z4Z2LXWP55SWVJS4Q';
const { sql } = new SQLocal(`zebridge_${Date.now()}.sqlite3`);
const td = new TextDecoder();

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
  /** WAL LSN this schema is valid from. Events at or below it predate the change. */
  lsn: number;
};

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
        const countRes = await sql(`SELECT COUNT(*) as count FROM ${table}`);
        const order = state.pkCols.length
          ? state.pkCols.map((c) => `"${c}" ASC`).join(', ')
          : 'rowid ASC';
        const rows = await sql(`SELECT * FROM ${table} ORDER BY ${order} LIMIT 100`);
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

      const encoder = new TextEncoder();
      nc = await wsconnect({
        servers: NATS_URL,
        authenticator: nkeyAuthenticator(encoder.encode(NKEY_SEED)),
        reconnect: true,
        maxReconnectAttempts: -1
      });

      setStatus('connected');
      appendLog('SYS', 'Connected to NATS over WebSockets using NKEY!');
      await watchSchemas();
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
            if (!initialized && (!entry || entry.delta === 0)) {
              initialized = true;
              resolve();
            }
            
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
    const names = cols.map((c) => c.name);

    const existing = syncedTables.get(table);
    const added = existing ? names.filter((n) => !existing.columns.includes(n)) : [];
    const removed = existing ? existing.columns.filter((n) => !names.includes(n)) : [];

    // A single-column key can be declared inline; a composite one must be a
    // table-level constraint, so the column DDL stays bare and the constraint is
    // appended after the column list.
    const inlinePk = pkCols.length === 1;
    const ddl = (c: { name: string; type: string }) =>
      `"${c.name}" ${c.type}${inlinePk && c.name === pkCols[0] ? ' PRIMARY KEY' : ''}`;
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
        syncedTables.set(table, { pkCols, columns: names, lsn });
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

      syncedTables.set(table, { pkCols, columns: names, lsn });
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

    // The LSN Gate: ignore stale events replayed from the stream if the table 
    // has already advanced past them (e.g. via a recent snapshot).
    if (ev.lsn <= state.lsn) {
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

      const values = Object.values(ev.data).map((v) =>
        v !== null && typeof v === 'object' ? JSON.stringify(v) : v
      );
      const columns = keys.map((k) => `"${k}"`).join(', ');
      const placeholders = keys.map(() => '?').join(', ');
      const updates = keys
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

            if (!desc) {
              // Request snapshot and wait for KV descriptor
              const reqSubject = topology.subjects.snapshot_request.includes('{[table]s}')
                ? topology.subjects.snapshot_request.replace('{[table]s}', table)
                : `${topology.subjects.snapshot_request}.${table}`;
              nc.publish(reqSubject, new Uint8Array(0));
              appendLog('SYS', `Published snapshot request for ${table}. Waiting for generation...`, 'INFO');

              if (snapKv) {
                const watchP = new Promise<any>(async (resolve) => {
                  const iter = await snapKv!.watch({ key: table });
                  for await (const entry of iter) {
                    if (entry.operation === "DEL" || entry.operation === "PURGE") continue;
                    try { desc = decode(entry.value); } catch { desc = JSON.parse(td.decode(entry.value)); }
                    resolve(desc);
                    break;
                  }
                });
                desc = await watchP;
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
          appendLog('SYS', `All required snapshots replayed successfully!`, 'INFO');
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



  const publishMutation = () => {
    if (!nc) return;
    const table = 'test_types';
    const op = 'INSERT';
    const id = Math.floor(Math.random() * 10000) + 1000;
    const payload = {
      table,
      operation: op,
      primary_key: { id },
      data: {
        id,
        some_text: 'Manual Button Simulator!',
        age: 42,
        price: 99.99,
        is_true: true
      },
      hlc: `${Date.now()}-0001`,
      msg_id: `mut-${Math.random().toString(36).substring(2, 9)}`
    };

    nc.publish(`mutation.${table}.${op.toLowerCase()}`, encode(payload));
    appendLog(`mutation.${table}.${op.toLowerCase()}`, payload, 'MUTATION OUT');
  };

  initNats();

    const intervalId = setInterval(refreshLocalDb, 1000);

    onCleanup(() => {
      clearInterval(intervalId);
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
          <button onClick={publishMutation} style="background: #e65100;">Push Mutation to Bridge</button>
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
