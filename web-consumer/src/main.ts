import { connect, NatsConnection, StringCodec, KvEntry } from 'nats.ws';
import { decode } from '@msgpack/msgpack';

const NATS_URL = 'ws://localhost:8080';
const NATS_USER = 'bridge_user';
const NATS_PASS = 'bridge_secure_password';

let nc: NatsConnection | null = null;
const sc = StringCodec();

const statusBadge = document.getElementById('status-badge')!;
const logOutput = document.getElementById('log-output')!;
const btnFetchSchema = document.getElementById('btn-fetch-schema')!;
const btnRequestSnapshot = document.getElementById('btn-request-snapshot')!;
const btnClearLogs = document.getElementById('btn-clear-logs')!;

function updateStatus(state: 'connected' | 'disconnected' | 'connecting') {
  statusBadge.className = `badge ${state}`;
  statusBadge.textContent = state.toUpperCase();
}

function appendLog(topic: string, data: any, opType?: string) {
  const div = document.createElement('div');
  div.className = 'log-entry';

  const timestamp = new Date().toLocaleTimeString();
  const opClass = opType ? `op-${opType.toLowerCase()}` : '';

  let bodyStr = '';
  if (typeof data === 'string') {
    bodyStr = data;
  } else {
    bodyStr = JSON.stringify(data, null, 2);
  }

  div.innerHTML = `<span class="time">[${timestamp}]</span> <span class="topic">${topic}</span> <span class="${opClass}">${opType || ''}</span>\n${bodyStr}`;

  logOutput.appendChild(div);
  logOutput.scrollTop = logOutput.scrollHeight;
}

async function initNats() {
  try {
    updateStatus('connecting');
    appendLog('SYS', `Connecting to NATS at ${NATS_URL}...`);

    nc = await connect({
      servers: NATS_URL,
      user: NATS_USER,
      pass: NATS_PASS,
      reconnect: true,
      maxReconnectAttempts: -1
    });

    updateStatus('connected');
    appendLog('SYS', 'Connected to NATS over WebSockets!');

    subscribeStreams();
  } catch (err) {
    updateStatus('disconnected');
    appendLog('SYS', `Connection failed: ${err}`);
  }
}

async function subscribeStreams() {
  if (!nc) return;

  // 1. Subscribe to CDC events
  const cdcSub = nc.subscribe('cdc.>');
  (async () => {
    for await (const msg of cdcSub) {
      let decoded: any;
      try {
        decoded = decode(msg.data);
      } catch {
        decoded = sc.decode(msg.data);
      }
      const op = decoded?.operation || 'CDC';
      appendLog(msg.subject, decoded, op);
    }
  })();

  // 2. Subscribe to INIT snapshot events
  const initSub = nc.subscribe('init.>');
  (async () => {
    for await (const msg of initSub) {
      let decoded: any;
      try {
        decoded = decode(msg.data);
      } catch {
        decoded = sc.decode(msg.data);
      }
      appendLog(msg.subject, decoded, 'INIT');
    }
  })();
}

// Button actions
btnFetchSchema.addEventListener('click', async () => {
  if (!nc) return;
  try {
    const js = nc.jetstream();
    const kv = await js.views.kv('schemas');
    const entry: KvEntry | null = await kv.get('test_types');
    if (entry) {
      let val: any;
      try { val = decode(entry.value); } catch { val = entry.string(); }
      appendLog('KV:schemas', val, 'SCHEMA');
    } else {
      appendLog('KV:schemas', 'Key "test_types" not found in schemas KV');
    }
  } catch (err) {
    appendLog('KV:schemas', `Fetch failed: ${err}`);
  }
});

btnRequestSnapshot.addEventListener('click', () => {
  if (!nc) return;
  nc.publish('snapshot.request.test_types', new Uint8Array(0));
  appendLog('SNAPSHOT', 'Published snapshot request for table: test_types');
});

btnClearLogs.addEventListener('click', () => {
  logOutput.innerHTML = '';
});

// Start connection
initNats();
