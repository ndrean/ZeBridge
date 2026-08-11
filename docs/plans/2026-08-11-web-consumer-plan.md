# Web Consumer Browser Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a clean, readable Vite + Vanilla TypeScript browser client in `/web-consumer` that connects to NATS JetStream via WebSockets (`ws://localhost:8080`), decodes `INIT` snapshots and `CDC` events (`INSERT`/`UPDATE`/`DELETE`), and appends formatted event logs into an overflow scroll container.

**Architecture:** Update NATS configuration to enable WebSockets on port 8080 and expose it in Docker Compose. Scaffold `/web-consumer` using Vite + pnpm, connect to NATS via `nats.ws`, decode MessagePack/JSON payloads, and render a minimal, clear UI log.

**Tech Stack:** Vite, Vanilla TypeScript, `nats.ws`, `@msgpack/msgpack`, Docker Compose.

## Global Constraints

- NATS WebSocket port: `8080` (no TLS in development)
- NATS User: `${NATS_BRIDGE_USER}` (default: `bridge_user`)
- NATS Password: `${NATS_BRIDGE_PASSWORD}` (default: `bridge_secure_password`)
- Output path for web app: `/web-consumer`
- UI Styling: Minimalist, readable, un-cluttered CSS (no Tailwind required)

---

### Task 1: Enable NATS WebSockets in Configuration & Docker Compose

**Files:**
- Modify: `nats-server.conf.template:1-16`
- Modify: `docker-compose.full.yml:82-99`

**Interfaces:**
- Consumes: NATS Server config template & Docker Compose ports
- Produces: NATS WebSocket listener exposed on host port `8080` (`ws://localhost:8080`)

- [ ] **Step 1: Update `nats-server.conf.template` to enable WebSockets**

```conf
port: 4222
http_port: 8222

websocket {
  port: 8080
  no_tls: true
}

authorization {
  users: [
    { user: "${NATS_BRIDGE_USER}", password: "${NATS_BRIDGE_PASSWORD}" }
  ]
}

jetstream {
  store_dir: "/data"
}
```

- [ ] **Step 2: Update `docker-compose.full.yml` to expose port 8080 on `nats-server`**

```yaml
  nats-server:
    image: nats:latest
    container_name: nats-server
    depends_on:
      nats-config-gen:
        condition: service_completed_successfully
    ports:
      - "4222:4222" # Client TCP connections
      - "8222:8222" # HTTP monitoring
      - "8080:8080" # WebSocket connections
```

- [ ] **Step 3: Test Docker Compose configuration syntax**

Run: `docker compose -f docker-compose.full.yml --env-file .env.prod config --quiet`
Expected: Exit code 0.

- [ ] **Step 4: Commit Infrastructure Changes**

```bash
git add nats-server.conf.template docker-compose.full.yml
git commit -m "feat(telemetry): enable NATS WebSocket port 8080 for browser clients"
```

---

### Task 2: Scaffold `/web-consumer` App Scaffolding

**Files:**
- Create: `web-consumer/package.json`
- Create: `web-consumer/index.html`
- Create: `web-consumer/vite.config.ts`
- Create: `web-consumer/src/style.css`

**Interfaces:**
- Consumes: `nats.ws`, `@msgpack/msgpack`
- Produces: Runnable Vite app structure serving `index.html`

- [ ] **Step 1: Create `web-consumer/package.json`**

```json
{
  "name": "web-consumer",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc && vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "@msgpack/msgpack": "^3.0.0-beta.1",
    "nats.ws": "^1.29.1"
  },
  "devDependencies": {
    "typescript": "^5.7.2",
    "vite": "^6.0.0"
  }
}
```

- [ ] **Step 2: Create `web-consumer/index.html`**

```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>ZeBridge Web Consumer</title>
    <link rel="stylesheet" href="/src/style.css" />
  </head>
  <body>
    <div id="app">
      <header>
        <h1>ZeBridge CDC Web Consumer</h1>
        <div class="status-bar">
          <span id="status-badge" class="badge disconnected">DISCONNECTED</span>
          <span id="server-url">ws://localhost:8080</span>
        </div>
      </header>

      <div class="controls">
        <button id="btn-fetch-schema">Fetch Schemas</button>
        <button id="btn-request-snapshot">Request Snapshot</button>
        <button id="btn-clear-logs">Clear Logs</button>
      </div>

      <main>
        <h3>Live Event Logs</h3>
        <div id="log-output" class="log-container"></div>
      </main>
    </div>
    <script type="module" src="/src/main.ts"></script>
  </body>
</html>
```

- [ ] **Step 3: Create `web-consumer/src/style.css` (Minimal & Readable)**

```css
body {
  font-family: system-ui, -apple-system, sans-serif;
  margin: 0;
  padding: 1rem;
  background: #121212;
  color: #e0e0e0;
}

#app {
  max-width: 900px;
  margin: 0 auto;
}

header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid #333;
  padding-bottom: 0.5rem;
}

.status-bar {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.badge {
  padding: 0.25rem 0.5rem;
  border-radius: 4px;
  font-size: 0.8rem;
  font-weight: bold;
}

.badge.connected { background: #1b5e20; color: #81c784; }
.badge.disconnected { background: #b71c1c; color: #ef9a9a; }
.badge.connecting { background: #e65100; color: #ffcc80; }

.controls {
  margin: 1rem 0;
  display: flex;
  gap: 0.5rem;
}

button {
  background: #2a2a2a;
  color: #fff;
  border: 1px solid #444;
  padding: 0.5rem 1rem;
  border-radius: 4px;
  cursor: pointer;
}

button:hover {
  background: #3a3a3a;
}

.log-container {
  height: 450px;
  overflow-y: auto;
  background: #000;
  border: 1px solid #333;
  border-radius: 4px;
  padding: 0.75rem;
  font-family: monospace;
  font-size: 0.85rem;
  white-space: pre-wrap;
}

.log-entry {
  margin-bottom: 0.5rem;
  padding-bottom: 0.5rem;
  border-bottom: 1px solid #1a1a1a;
}

.log-entry .topic { color: #64b5f6; font-weight: bold; }
.log-entry .op-insert { color: #81c784; }
.log-entry .op-update { color: #ffb74d; }
.log-entry .op-delete { color: #e57373; }
.log-entry .op-init { color: #ba68c8; }
```

- [ ] **Step 4: Create `web-consumer/vite.config.ts`**

```ts
import { defineConfig } from 'vite';

export default defineConfig({
  server: {
    port: 5173
  }
});
```

- [ ] **Step 5: Install dependencies in `/web-consumer`**

Run: `cd web-consumer && pnpm install`
Expected: `node_modules` installed successfully.

- [ ] **Step 6: Commit Scaffolding**

```bash
git add web-consumer/
git commit -m "scaffold(web-consumer): initial Vite package setup"
```

---

### Task 3: Implement NATS WebSocket Connection & JetStream Consumer Logic

**Files:**
- Create: `web-consumer/src/main.ts`

**Interfaces:**
- Consumes: `nats.ws`, `@msgpack/msgpack`
- Produces: Interactive client connecting to NATS over WebSockets, displaying incoming `CDC` / `INIT` event logs, and fetching KV schemas / requesting snapshots.

- [ ] **Step 1: Create `web-consumer/src/main.ts`**

```ts
import { connect, NatsConnection, StringCodec, Kvp } from 'nats.ws';
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
    const entry: Kvp | null = await kv.get('test_types');
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
```

- [ ] **Step 2: Test TypeScript compilation**

Run: `cd web-consumer && pnpm run build`
Expected: TypeScript compiles clean with zero errors.

- [ ] **Step 3: Commit Application Logic**

```bash
git add web-consumer/src/main.ts
git commit -m "feat(web-consumer): implement NATS WebSocket client and event logger"
```
