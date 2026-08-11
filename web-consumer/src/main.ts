import { connect, NatsConnection, StringCodec, KvEntry, jwtAuthenticator, nkeyAuthenticator } from 'nats.ws';
import { decode, encode } from '@msgpack/msgpack';

const NATS_URL = 'ws://localhost:8080';

let nc: NatsConnection | null = null;
const sc = StringCodec();

const statusBadge = document.getElementById('status-badge')!;
const logOutput = document.getElementById('log-output')!;
const btnFetchSchema = document.getElementById('btn-fetch-schema')!;
const btnRequestSnapshot = document.getElementById('btn-request-snapshot')!;
const btnClearLogs = document.getElementById('btn-clear-logs')!;

const authModeSelect = document.getElementById('auth-mode') as HTMLSelectElement;
const authUserpassFields = document.getElementById('auth-userpass-fields')!;
const authNkeyFields = document.getElementById('auth-nkey-fields')!;
const authJwtFields = document.getElementById('auth-jwt-fields')!;
const btnReconnect = document.getElementById('btn-reconnect')!;

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

// Auth UI Toggle
authModeSelect.addEventListener('change', () => {
  const mode = authModeSelect.value;
  authUserpassFields.style.display = mode === 'userpass' ? 'flex' : 'none';
  authNkeyFields.style.display = mode === 'nkey' ? 'flex' : 'none';
  authJwtFields.style.display = mode === 'jwt' ? 'flex' : 'none';
});

async function initNats() {
  if (nc) {
    try {
      await nc.close();
    } catch { /* ignore */ }
    nc = null;
  }

  try {
    updateStatus('connecting');
    const mode = authModeSelect.value;
    appendLog('SYS', `Connecting to NATS at ${NATS_URL} (auth=${mode})...`);

    const encoder = new TextEncoder();

    if (mode === 'nkey') {
      const seedStr = (document.getElementById('auth-nkey-seed') as HTMLInputElement).value.trim();
      if (!seedStr) throw new Error('NKEY Seed is required for NKEY auth mode');
      nc = await connect({
        servers: NATS_URL,
        authenticator: nkeyAuthenticator(encoder.encode(seedStr)),
        reconnect: true,
        maxReconnectAttempts: -1
      });
    } else if (mode === 'jwt') {
      const jwtStr = (document.getElementById('auth-jwt-token') as HTMLTextAreaElement).value.trim();
      const seedStr = (document.getElementById('auth-jwt-seed') as HTMLInputElement).value.trim();
      if (!jwtStr || !seedStr) throw new Error('Both JWT token and NKEY Seed are required for JWT auth mode');
      nc = await connect({
        servers: NATS_URL,
        authenticator: jwtAuthenticator(jwtStr, encoder.encode(seedStr)),
        reconnect: true,
        maxReconnectAttempts: -1
      });
    } else {
      const userStr = (document.getElementById('auth-user') as HTMLInputElement).value.trim();
      const passStr = (document.getElementById('auth-pass') as HTMLInputElement).value.trim();
      nc = await connect({
        servers: NATS_URL,
        user: userStr,
        pass: passStr,
        reconnect: true,
        maxReconnectAttempts: -1
      });
    }

    updateStatus('connected');
    appendLog('SYS', `Connected to NATS over WebSockets using ${mode.toUpperCase()} authentication!`);

    subscribeStreams();
  } catch (err) {
    updateStatus('disconnected');
    appendLog('SYS', `Connection failed: ${err}`);
  }
}

btnReconnect.addEventListener('click', () => {
  initNats();
});

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

const userIdInput = document.getElementById('user-id') as HTMLInputElement;
const ttIdInput = document.getElementById('tt-id') as HTMLInputElement;
const ttUidInput = document.getElementById('tt-uid') as HTMLInputElement;

function updateFieldStates() {
  const op = selectOp.value;
  const isInsert = op === 'INSERT';
  const isDelete = op === 'DELETE';

  // ID fields are disabled for INSERT (Postgres sequence assigns ID)
  userIdInput.disabled = isInsert;
  ttIdInput.disabled = isInsert;

  if (isInsert) {
    userIdInput.placeholder = 'Auto (PG)';
    ttIdInput.placeholder = 'Auto (PG)';
    ttUidInput.placeholder = 'Auto (PG)';
  } else {
    userIdInput.placeholder = 'ID (Required)';
    ttIdInput.placeholder = 'ID (Required)';
    ttUidInput.placeholder = 'uid (UUID)';
    if (!userIdInput.value) userIdInput.value = '1';
    if (!ttIdInput.value) ttIdInput.value = '12';
  }

  // Non-ID fields are disabled for DELETE (only PK required)
  const usersInputs = fieldsUsers.querySelectorAll('input:not(#user-id)');
  usersInputs.forEach((el) => { (el as HTMLInputElement).disabled = isDelete; });

  const ttInputs = fieldsTestTypes.querySelectorAll('input:not(#tt-id), textarea');
  ttInputs.forEach((el) => { (el as HTMLInputElement | HTMLTextAreaElement).disabled = isDelete; });
}

selectTable.addEventListener('change', () => {
  if (selectTable.value === 'users') {
    fieldsUsers.style.display = 'flex';
    fieldsTestTypes.style.display = 'none';
  } else {
    fieldsUsers.style.display = 'none';
    fieldsTestTypes.style.display = 'flex';
  }
  updateFieldStates();
});

selectOp.addEventListener('change', updateFieldStates);

// Initial state check
updateFieldStates();

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

