import { createSignal, onCleanup, For } from 'solid-js';
import { connect, NatsConnection, StringCodec, nkeyAuthenticator } from 'nats.ws';
import { decode, encode } from '@msgpack/msgpack';
import topology from '../../topology.json';
import { SQLocal } from 'sqlocal';

const NATS_URL = 'ws://localhost:8080';
const NKEY_SEED = 'SUAPSL67RKOUDZFREHHDWUXDXLYZKEHMWEXMIUC35Z4Z2LXWP55SWVJS4Q';
const { sql } = new SQLocal('zebridge.sqlite3');
const sc = StringCodec();

// Non-reactive state for sync tracking
const syncedTables = new Map<string, { pk: string }>();
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
    } catch (err) {
      setStatus('disconnected');
      appendLog('SYS', `Connection failed: ${err}`);
    }
  };

  const subscribeStreams = () => {
    if (!nc) return;

    // 1. Subscribe to CDC events
    const cdcSub = nc.subscribe(`${topology.subjects.cdc_prefix}.>`);
    (async () => {
      for await (const msg of cdcSub) {
        let decoded: any;
        try { decoded = decode(msg.data); } catch { decoded = sc.decode(msg.data); }
        const events = Array.isArray(decoded) ? decoded : [decoded];
        
        for (const ev of events) {
          const op = ev?.operation || 'CDC';
          
          if (!Array.isArray(decoded)) {
            // Only log if it wasn't a batch, to avoid double logging
            // Or log each event individually
          }

          const table = ev?.table || msg.subject.split('.')[1];
          if (table && syncedTables.has(table) && ev?.data) {
            const schema = syncedTables.get(table)!;
            
            if (op === 'INSERT' || op === 'UPDATE') {
              const keys = Object.keys(ev.data);
              const values = Object.values(ev.data).map(v => 
                (v !== null && typeof v === 'object') ? JSON.stringify(v) : v
              );
              
              const columns = keys.map(k => `"${k}"`).join(', ');
              const placeholders = keys.map(() => '?').join(', ');
              const updates = keys.filter(k => k !== schema.pk).map(k => `"${k}" = excluded."${k}"`).join(', ');

              let query = `INSERT INTO ${table} (${columns}) VALUES (${placeholders})`;
              if (schema.pk) {
                query += ` ON CONFLICT("${schema.pk}") DO UPDATE SET ${updates}`;
              }

              try {
                await sql(query, ...values);
              } catch (err) {
                appendLog('SQLITE', `UPSERT failed: ${err}`, 'ERROR');
              }
            } else if (op === 'DELETE') {
              const pkVal = ev.data[schema.pk];
              if (pkVal !== undefined) {
                try {
                  await sql(`DELETE FROM ${table} WHERE "${schema.pk}" = ?`, pkVal);
                } catch (err) {
                  appendLog('SQLITE', `DELETE failed: ${err}`, 'ERROR');
                }
              }
            }
          }
        }
      }
    })();

    // 2. Subscribe to INIT snapshot events
    const initSub = nc.subscribe(`${topology.subjects.init_prefix}.>`);
    (async () => {
      for await (const msg of initSub) {
        let decoded: any;
        try { decoded = decode(msg.data); } catch { decoded = sc.decode(msg.data); }
        appendLog(msg.subject, decoded, 'INIT');
      }
    })();
  };

  const syncSqlite = async () => {
    if (!nc) return;
    try {
      appendLog('SQLITE', 'Fetching schema from NATS KV...');
      const js = nc.jetstream();
      const kv = await js.views.kv(topology.kv.schemas);
      
      const table = 'test_types';
      const entry = await kv.get(table);
      if (!entry) {
        appendLog('SQLITE', `Schema for ${table} not found in KV!`, 'ERROR');
        return;
      }

      let val: any;
      try { val = decode(entry.value); } catch { val = JSON.parse(entry.string()); }
      
      if (val?.schema && typeof val.schema === 'string') {
        val = JSON.parse(val.schema);
      }
      
      if (val?.sqlite?.columns) {
        const pkName = val.sqlite.pk;
        const columns = val.sqlite.columns.map((c: any) => {
          const isPk = c.name === pkName ? ' PRIMARY KEY' : '';
          return `"${c.name}" ${c.type}${isPk}`;
        }).join(', ');
        
        const dropTableQuery = `DROP TABLE IF EXISTS ${table};`;
        const createTableQuery = `CREATE TABLE IF NOT EXISTS ${table} (${columns});`;
        appendLog('SQLITE', `Executing: ${dropTableQuery} then ${createTableQuery}`, 'MIGRATE');
        
        await sql(dropTableQuery);
        await sql(createTableQuery);
        syncedTables.set(table, { pk: pkName });
        appendLog('SQLITE', `Table ${table} is now ready in local WASM SQLite!`, 'READY');
      }
    } catch (err) {
      appendLog('SQLITE', `Sync failed: ${err}`, 'ERROR');
    }
  };

  const queryLocalDb = async () => {
    try {
      const table = 'test_types';
      if (!syncedTables.has(table)) {
        appendLog('SQLITE', `Table ${table} is not synced yet!`, 'ERROR');
        return;
      }
      
      // We use id DESC because it's the primary key we extracted earlier!
      const countRes = await sql(`SELECT COUNT(*) as count FROM ${table}`);
      const count = countRes[0]?.count || 0;
      
      const lastRowRes = await sql(`SELECT * FROM ${table} ORDER BY id DESC LIMIT 1`);
      const lastRow = lastRowRes[0] || null;
      
      appendLog('SQLITE', JSON.stringify({ row_count: count, last_inserted: lastRow }, null, 2), 'QUERY');
    } catch (err) {
      appendLog('SQLITE', `Query failed: ${err}`, 'ERROR');
    }
  };

  // Mount effect
  initNats();

  onCleanup(() => {
    if (nc) nc.close();
  });

  const publishMutation = () => {
    if (!nc) return;
    const table = 'test_types';
    const payload = {
      table,
      operation: 'INSERT',
      data: {
        id: Math.floor(Math.random() * 10000) + 1000,
        some_text: "Manual Button Simulator!",
        age: 42,
        price: 99.99,
        is_true: 1,
        tags: ["manual", "test"],
        matrix: [[1, 2], [3, 4]]
      },
      lsn: 9999999,
      msg_id: `sim-${Math.random().toString(36).substring(2, 9)}`
    };

    const encoded = encode(payload);
    // Publish directly to the CDC stream! 
    // This bypasses PostgreSQL and bounces straight off NATS back into our subscriber loop!
    nc.publish(`cdc.${table}.insert`, encoded);
    appendLog(`cdc.${table}.insert`, payload, 'SIMULATE');
  };

  return (
    <>
      <header>
        <h1>ZeBridge CDC Web Consumer</h1>
        <div class="status-bar">
          <span class={`badge ${status()}`}>{status().toUpperCase()}</span>
          <span id="server-url">{NATS_URL}</span>
        </div>
      </header>

      <div class="controls">
        <button onClick={syncSqlite} style="background: #0277bd;">Sync SQLite</button>
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
