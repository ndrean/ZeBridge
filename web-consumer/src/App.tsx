import { createSignal, onCleanup, For } from 'solid-js';
import { connect, NatsConnection, StringCodec, nkeyAuthenticator } from 'nats.ws';
import { decode, encode } from '@msgpack/msgpack';
import topology from '../../topology.json';
import { SQLocal } from 'sqlocal';

const NATS_URL = 'ws://localhost:8080';
const NKEY_SEED = 'SUAPSL67RKOUDZFREHHDWUXDXLYZKEHMWEXMIUC35Z4Z2LXWP55SWVJS4Q';
const { sql } = new SQLocal('zebridge.sqlite3');
const sc = StringCodec();

// Columns hidden from the *_view convenience views (still stored in the table).
const EXCLUDE_FROM_VIEW = ['uid', 'inserted_at', 'updated_at', 'metadata'];

type TableState = {
  pk: string;
  /** Column names the local table currently has. */
  columns: string[];
  /** WAL LSN this schema is valid from. Events at or below it predate the change. */
  lsn: number;
};

// Non-reactive state for sync tracking
const syncedTables = new Map<string, TableState>();

/**
 * CDC events that arrived referencing columns the local schema does not have yet.
 * The bridge publishes the schema before the dependent row, but KV and CDC are two
 * independent subscriptions here, so the row can still win the race locally. Holding
 * it until a newer schema lands is what turns the bridge's ordering guarantee into
 * something this client actually honours.
 */
const pendingEvents: { table: string; ev: any }[] = [];

let nc: NatsConnection | null = null;

type LogEntry = {
  id: number;
  timestamp: string;
  topic: string;
  opType: string;
  bodyStr: string;
};

export default function App() {
  const [status, setStatus] = createSignal<'connected' | 'disconnected' | 'connecting'>('disconnected');
  const [logs, setLogs] = createSignal<LogEntry[]>([]);
  const [pendingCount, setPendingCount] = createSignal(0);
  const [tableCount, setTableCount] = createSignal(0);
  let logIdCounter = 0;

  const appendLog = (topic: string, data: any, opType = '') => {
    const timestamp = new Date().toLocaleTimeString();
    const bodyStr = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
    setLogs((prev) => [
      ...prev,
      { id: logIdCounter++, timestamp, topic, opType, bodyStr }
    ]);
  };

  const initNats = async () => {
    if (nc) {
      try { await nc.close(); } catch {}
      nc = null;
    }

    try {
      setStatus('connecting');
      appendLog('SYS', `Connecting to NATS at ${NATS_URL}...`);

      const encoder = new TextEncoder();
      nc = await connect({
        servers: NATS_URL,
        authenticator: nkeyAuthenticator(encoder.encode(NKEY_SEED)),
        reconnect: true,
        maxReconnectAttempts: -1
      });

      setStatus('connected');
      appendLog('SYS', 'Connected to NATS over WebSockets using NKEY!');
      subscribeStreams();
      watchSchemas();
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
      const js = nc.jetstream();
      const kv = await js.views.kv(topology.kv.schemas);
      const watcher = await kv.watch();
      appendLog('SCHEMA', `Watching KV bucket "${topology.kv.schemas}" for all tables...`, 'WATCH');

      (async () => {
        for await (const entry of watcher) {
          // A deleted/purged key means the table is gone upstream.
          if (entry.operation === 'DEL' || entry.operation === 'PURGE') {
            await dropLocalTable(entry.key, 'KV key removed');
            continue;
          }

          let val: any;
          try { val = decode(entry.value); } catch { val = JSON.parse(sc.decode(entry.value)); }
          if (val?.schema && typeof val.schema === 'string') val = JSON.parse(val.schema);

          // Tombstone: the bridge publishes {"dropped":true} rather than removing the
          // key, so a client arriving after the DROP still learns the table is gone.
          if (val?.dropped === true) {
            await dropLocalTable(entry.key, `tombstone @ lsn ${val.lsn ?? '?'}`);
            continue;
          }

          if (val?.sqlite?.columns) await applySchema(entry.key, val);
        }
      })();
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
    const pkName: string = val.sqlite.pk;
    const cols: { name: string; type: string }[] = val.sqlite.columns;
    const lsn: number = typeof val.lsn === 'number' ? val.lsn : 0;
    const names = cols.map((c) => c.name);

    const existing = syncedTables.get(table);
    const added = existing ? names.filter((n) => !existing.columns.includes(n)) : [];
    const removed = existing ? existing.columns.filter((n) => !names.includes(n)) : [];
    const additiveOnly = !!existing && removed.length === 0 && added.length > 0;

    try {
      if (additiveOnly) {
        for (const name of added) {
          const type = cols.find((c) => c.name === name)!.type;
          await sql(`ALTER TABLE ${table} ADD COLUMN "${name}" ${type};`);
        }
        appendLog('SCHEMA', `${table}: +[${added.join(', ')}] via ALTER (rows preserved), lsn=${lsn}`, 'MIGRATE');
      } else if (existing && added.length === 0 && removed.length === 0) {
        syncedTables.set(table, { pk: pkName, columns: names, lsn });
        return; // identical schema, e.g. a boot republish
      } else {
        const columns = cols
          .map((c) => `"${c.name}" ${c.type}${c.name === pkName ? ' PRIMARY KEY' : ''}`)
          .join(', ');
        await sql(`DROP TABLE IF EXISTS ${table};`);
        await sql(`CREATE TABLE IF NOT EXISTS ${table} (${columns});`);
        const why = existing ? `columns removed: [${removed.join(', ')}]` : 'first sight';
        appendLog('SCHEMA', `${table}: rebuilt (${why}), lsn=${lsn}`, 'MIGRATE');
      }

      // (Re)build the convenience view against the current column set.
      const viewCols = names.filter((n) => !EXCLUDE_FROM_VIEW.includes(n)).map((n) => `"${n}"`).join(', ');
      await sql(`DROP VIEW IF EXISTS ${table}_view;`);
      if (viewCols) await sql(`CREATE VIEW ${table}_view AS SELECT ${viewCols} FROM ${table};`);

      syncedTables.set(table, { pk: pkName, columns: names, lsn });
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
      const updates = keys.filter((k) => k !== state.pk).map((k) => `"${k}" = excluded."${k}"`).join(', ');

      let query = `INSERT INTO ${table} (${columns}) VALUES (${placeholders})`;
      if (state.pk && updates) query += ` ON CONFLICT("${state.pk}") DO UPDATE SET ${updates}`;

      try {
        await sql(query, ...values);
      } catch (err) {
        appendLog('SQLITE', `UPSERT on ${table} failed: ${err}`, 'ERROR');
      }
    } else if (op === 'DELETE') {
      // With REPLICA IDENTITY DEFAULT the delete carries the PK and nulls elsewhere,
      // which is all a delete-by-key needs.
      const pkVal = ev.data[state.pk];
      if (pkVal !== undefined && pkVal !== null) {
        try {
          await sql(`DELETE FROM ${table} WHERE "${state.pk}" = ?`, pkVal);
        } catch (err) {
          appendLog('SQLITE', `DELETE on ${table} failed: ${err}`, 'ERROR');
        }
      }
    }
  };

  const subscribeStreams = () => {
    if (!nc) return;

    const cdcSub = nc.subscribe(`${topology.subjects.cdc_prefix}.>`);
    (async () => {
      for await (const msg of cdcSub) {
        let decoded: any;
        try { decoded = decode(msg.data); } catch { decoded = sc.decode(msg.data); }
        // A batch message is an array; a single event is an object.
        const events = Array.isArray(decoded) ? decoded : [decoded];

        for (const ev of events) {
          const table = ev?.table || msg.subject.split('.')[1];
          if (table) await applyEvent(table, ev);
        }
      }
    })();

    const initSub = nc.subscribe(`${topology.subjects.init_prefix}.>`);
    (async () => {
      for await (const msg of initSub) {
        let decoded: any;
        try { decoded = decode(msg.data); } catch { decoded = sc.decode(msg.data); }
        appendLog(msg.subject, decoded, 'INIT');
      }
    })();
  };

  // ---------------------------------------------------------------------------
  // UI actions
  // ---------------------------------------------------------------------------

  const queryLocalDb = async () => {
    if (!syncedTables.size) {
      appendLog('SQLITE', 'No tables synced yet — waiting for a schema push.', 'ERROR');
      return;
    }
    for (const [table, state] of syncedTables) {
      try {
        const countRes = await sql(`SELECT COUNT(*) as count FROM ${table}`);
        const lastRes = await sql(`SELECT * FROM ${table} ORDER BY "${state.pk}" DESC LIMIT 1`);
        appendLog(
          'SQLITE',
          JSON.stringify(
            { table, schema_lsn: state.lsn, columns: state.columns, row_count: countRes[0]?.count ?? 0, last_row: lastRes[0] ?? null },
            null,
            2
          ),
          'QUERY'
        );
      } catch (err) {
        appendLog('SQLITE', `Query on ${table} failed: ${err}`, 'ERROR');
      }
    }
  };

  const publishMutation = () => {
    if (!nc) return;
    const table = 'test_types';
    const payload = {
      table,
      operation: 'INSERT',
      data: {
        id: Math.floor(Math.random() * 10000) + 1000,
        some_text: 'Manual Button Simulator!',
        age: 42,
        price: 99.99,
        is_true: 1,
        tags: ['manual', 'test'],
        matrix: [[1, 2], [3, 4]]
      },
      lsn: 9999999,
      msg_id: `sim-${Math.random().toString(36).substring(2, 9)}`
    };

    nc.publish(`cdc.${table}.insert`, encode(payload));
    appendLog(`cdc.${table}.insert`, payload, 'SIMULATE');
  };

  initNats();

  onCleanup(() => {
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

      <div class="controls">
        <button onClick={queryLocalDb} style="background: #2e7d32;">Query Local DB</button>
        <button onClick={publishMutation} style="background: #8e24aa;">Simulate CDC Event</button>
        <button onClick={() => setLogs([])}>Clear Logs</button>
      </div>

      <main>
        <h3>Live Event Logs ({logs().length})</h3>
        <div class="log-container">
          <For each={logs()}>
            {(log) => (
              <div class="log-entry">
                <span class="time">[{log.timestamp}]</span>{' '}
                <span class="topic">{log.topic}</span>{' '}
                <span class={`op-${log.opType.toLowerCase()}`}>{log.opType}</span>
                <br />
                {log.bodyStr}
              </div>
            )}
          </For>
        </div>
      </main>
    </>
  );
}
