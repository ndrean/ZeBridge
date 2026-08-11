import { connect, NatsConnection, StringCodec, KvEntry } from 'nats.ws';
import { decode, encode } from '@msgpack/msgpack';

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

const selectTable = document.getElementById('select-table') as HTMLSelectElement;
const selectOp = document.getElementById('select-op') as HTMLSelectElement;
const fieldsUsers = document.getElementById('fields-users')!;
const fieldsTestTypes = document.getElementById('fields-test_types')!;
const btnPublishMutation = document.getElementById('btn-publish-mutation')!;

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

// Table selection toggle
selectTable.addEventListener('change', () => {
  if (selectTable.value === 'users') {
    fieldsUsers.style.display = 'flex';
    fieldsTestTypes.style.display = 'none';
  } else {
    fieldsUsers.style.display = 'none';
    fieldsTestTypes.style.display = 'flex';
  }
});

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

btnPublishMutation.addEventListener('click', () => {
  if (!nc) {
    appendLog('SYS', 'Cannot publish mutation: NATS not connected');
    return;
  }

  const table = selectTable.value;
  const op = selectOp.value;

  let idVal: string;
  let dataFields: Record<string, any> = {};

  if (table === 'users') {
    idVal = (document.getElementById('user-id') as HTMLInputElement).value;
    dataFields = {
      name: (document.getElementById('user-name') as HTMLInputElement).value,
      email: (document.getElementById('user-email') as HTMLInputElement).value,
    };
  } else {
    idVal = (document.getElementById('tt-id') as HTMLInputElement).value;
    const uidVal = (document.getElementById('tt-uid') as HTMLInputElement).value;
    const ageVal = (document.getElementById('tt-age') as HTMLInputElement).value;
    const tempVal = (document.getElementById('tt-temp') as HTMLInputElement).value;
    const priceVal = (document.getElementById('tt-price') as HTMLInputElement).value;
    const tagsStr = (document.getElementById('tt-tags') as HTMLInputElement).value;
    const matrixStr = (document.getElementById('tt-matrix') as HTMLInputElement).value;
    const metaStr = (document.getElementById('tt-metadata') as HTMLTextAreaElement).value;

    let parsedTags: any = tagsStr;
    let parsedMatrix: any = matrixStr;
    let parsedMetadata: any = metaStr;

    try { parsedTags = JSON.parse(tagsStr); } catch { /* keep raw */ }
    try { parsedMatrix = JSON.parse(matrixStr); } catch { /* keep raw */ }
    try { parsedMetadata = JSON.parse(metaStr); } catch { /* keep raw */ }

    dataFields = {
      uid: uidVal || undefined,
      age: ageVal !== '' ? Number(ageVal) : 30,
      temperature: tempVal !== '' ? Number(tempVal) : 36.6,
      price: priceVal || '123.4500',
      is_true: (document.getElementById('tt-is_true') as HTMLInputElement).checked,
      some_text: (document.getElementById('tt-text') as HTMLInputElement).value,
      tags: parsedTags,
      matrix: parsedMatrix,
      metadata: parsedMetadata,
    };
    if (op !== 'INSERT' || idVal !== '') {
      dataFields.id = Number(idVal);
    }
  }

  const primaryKey: Record<string, any> = {};
  if (idVal !== '') {
    primaryKey.id = Number(idVal);
  }

  const payload = {
    table,
    operation: op,
    primary_key: primaryKey,
    data: dataFields,
    hlc: `${Date.now()}-0001`,
    msg_id: `mut-${Math.random().toString(36).substring(2, 9)}`
  };

  const subject = `mutation.${table}.${op.toLowerCase()}`;
  const encoded = encode(payload);

  nc.publish(subject, encoded);
  appendLog(subject, payload, 'MUTATION OUT');
});

// Start connection
initNats();

