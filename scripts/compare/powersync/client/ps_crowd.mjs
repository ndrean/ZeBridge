// The PowerSync side of the crowd (../README.md, "A crowd of 500"): many clients in one process, without the SDK,
// which holds a SQLite database and about 150 MB per client. Run by powersync.sh in a node:22 container.
//
//   node ps_crowd.mjs sync N MARKERS     N readers of /sync/stream (NDJSON), each timed from one shared start to
//                                        its first complete checkpoint; then MARKERS seconds of one marker row a
//                                        second (through PostgREST), each timed to every reader. A reader keeps
//                                        no replica: it counts the operations it is sent, which is the service's
//                                        share of a sync, not a client's apply.
//   node ps_crowd.mjs ready ROWS         one reader, retried until a complete checkpoint carries ROWS operations:
//                                        the service has the table in its storage (powersync.sh up runs it, so
//                                        no timed run includes the service's own first copy)
//   node ps_crowd.mjs writes N RATE SECONDS
//                                        N simulated apps, RATE writes/s offered in all, into sensor_events. Each
//                                        app uploads as uploadData does: one request at a time, up to 200 queued
//                                        rows per request, to PostgREST as its own principal; each write timed
//                                        from its creation to the 201 that covers it.
//
// Service 127.0.0.1:18080, PostgREST 127.0.0.1:13000; ./secrets/jwt.key signs the readers' tokens.
import { createHmac, randomBytes, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { BSON } from 'bson';

// The API processes (ps_service.py): reader i connects to the (i mod n)th, as a round-robin load balancer would.
const API_PORTS = (process.env.API_PORTS ?? '18081,18082,18083,18084,18085').split(',');
const SERVICE = (i) => `http://127.0.0.1:${API_PORTS[i % API_PORTS.length]}`, REST = 'http://127.0.0.1:13000';
const KEY = readFileSync('./secrets/jwt.key', 'utf8').trim();
const b64u = (b) => Buffer.from(b).toString('base64url');
const principal = (i) => `p${String(i).padStart(3, '0')}`;
const pct = (a, f) => { const s = [...a].sort((x, y) => x - y); return s.length ? s[Math.min(s.length - 1, Math.floor(f * s.length))] : NaN; };
const ms = (v) => Math.round(v);
// RFC 9562 version 7: time-ordered keys, as ingress_load.py sends, so both crowds append to the key's index alike.
const uuid7 = () => {
  const b = randomBytes(16), t = Date.now();
  for (let i = 0; i < 6; i++) b[i] = Math.floor(t / 2 ** (8 * (5 - i))) & 0xff;
  b[6] = (b[6] & 0x0f) | 0x70; b[8] = (b[8] & 0x3f) | 0x80;
  const h = b.toString('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rssMb = () => Math.round(process.memoryUsage().rss / 1048576);

function token(user) {
  const now = Math.floor(Date.now() / 1000);
  const head = b64u(JSON.stringify({ alg: 'HS256', kid: 'cmp-key', typ: 'JWT' }));
  const body = b64u(JSON.stringify({ sub: user, aud: 'powersync-cmp', iat: now, exp: now + 3600 }));
  return `${head}.${body}.${createHmac('sha256', Buffer.from(KEY, 'base64url')).update(`${head}.${body}`).digest('base64url')}`;
}

async function post(path, rows, who) {
  const r = await fetch(`${REST}/${path}`, {
    method: 'POST', body: JSON.stringify(rows),
    headers: { 'content-type': 'application/json', prefer: 'resolution=merge-duplicates,return=minimal', 'x-principal': who },
  });
  if (!r.ok) throw new Error(`PostgREST ${r.status}: ${(await r.text()).slice(0, 160)}`);
}

// ── sync ──────────────────────────────────────────────────────────────────────
// Readers run PER_PROCESS to a process (default 25), all from one shared start, so that no single Node thread
// decoding the streams becomes the limit (storm.py does the same with 60 libzb clients per process). Times are
// wall-clock milliseconds, so the processes' numbers compare.
const PER = Number(process.env.PER_PROCESS ?? 25);

async function reader(j, gi, startEpoch, st) {
  const res = await fetch(`${SERVICE(gi)}/sync/stream`, {
    method: 'POST',
    // What the SDK asks for: BSON preferred, NDJSON otherwise; the service picks.
    headers: { 'content-type': 'application/json', authorization: `Token ${token(`crowd-${gi}`)}`, accept: 'application/vnd.powersync.bson-stream;q=0.9,application/x-ndjson;q=0.8' },
    body: JSON.stringify({ buckets: [], include_checksum: true, raw_data: true, client_id: `crowd-${gi}`, streams: { include_defaults: true, subscriptions: [] } }),
    signal: st.abort.signal,
  });
  if (!res.ok) throw new Error(`sync/stream ${res.status}: ${(await res.text()).slice(0, 160)}`);
  st.bson = res.headers.get('content-type') === 'application/vnd.powersync.bson-stream';
  const dec = new TextDecoder();
  let buf = '', bin = Buffer.alloc(0);
  for await (const chunk of res.body) {
    const msgs = [];
    if (st.bson) {
      // Concatenated BSON documents, each starting with its int32 length.
      bin = bin.length ? Buffer.concat([bin, chunk]) : Buffer.from(chunk);
      while (bin.length >= 4 && bin.length >= bin.readInt32LE(0)) {
        const len = bin.readInt32LE(0), doc = bin.subarray(0, len);
        bin = bin.subarray(len);
        // Light (the crowd): the first field's name says what the document is (a type byte, then the name). A data
        // document is counted and scanned for markers, as zb_crowd.py scans ZeBridge's, never decoded; only the
        // small control documents (checkpoint, checkpoint_complete…) are.
        if (st.light && doc.toString('latin1', 5, 9) === 'data' && doc[9] === 0) {
          st.bytes[j] += len;
          // A marker's id appears twice in its operation (object_id and the row's id): one arrival per document.
          const ids = new Set();
          for (let at = doc.indexOf(st.needle); at >= 0; at = doc.indexOf(st.needle, at + 1)) ids.add(doc.toString('latin1', at, at + st.needle.length + 4));
          for (const id of ids) (st.arrivals[id] ??= []).push(Date.now());
          continue;
        }
        msgs.push(BSON.deserialize(doc));
      }
    } else {
      buf += dec.decode(chunk, { stream: true });
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) { const line = buf.slice(0, nl); buf = buf.slice(nl + 1); if (line) msgs.push(JSON.parse(line)); }
    }
    for (const m of msgs) {
      if (m.data) {
        st.ops[j] += m.data.data?.length ?? 0;
        for (const op of m.data.data ?? []) {
          if (!op.object_id?.startsWith('m-')) continue;
          if (!st.arrivals[op.object_id]) st.arrivals[op.object_id] = [];
          st.arrivals[op.object_id].push(Date.now());
        }
      } else if (m.checkpoint_complete && st.synced[j] === null) {
        st.synced[j] = (Date.now() - startEpoch) / 1000;
      }
    }
  }
}

// One process's readers, [offset, offset + count): reports when they are synced, then their markers when told to stop.
async function syncWorker(offset, count, startEpoch, tag) {
  (await import('node:events')).setMaxListeners(0);   // every reader's fetch listens on the one abort signal
  const st = { abort: new AbortController(), synced: Array(count).fill(null), ops: Array(count).fill(0), bytes: Array(count).fill(0),
    retries: 0, last: '', bson: false, arrivals: {}, light: true, needle: Buffer.from(tag, 'latin1') };
  process.on('message', async (m) => {
    if (m !== 'stop') return;
    process.send({ type: 'arrivals', arrivals: st.arrivals });
    st.abort.abort(); setTimeout(() => process.exit(0), 200);
  });
  await sleep(Math.max(0, startEpoch - Date.now()));
  for (let j = 0; j < count; j++) {
    // A reader the service refuses (no checkpoint yet, overloaded) retries after 5 s, as the SDK does; its
    // time to synced keeps counting from the shared start.
    void (async () => {
      while (!st.abort.signal.aborted && Date.now() - startEpoch < 900_000) {
        try { await reader(j, offset + j, startEpoch, st); return; } catch (e) {
          if (e.name === 'AbortError') return;
          st.retries++; st.last = `reader ${offset + j}: ${e.message}`; await sleep(5000);   // the SDK's retryDelayMs
        }
      }
    })();
  }
  while (st.synced.some((s) => s === null) && Date.now() - startEpoch < 900_000) await sleep(200);
  process.send({ type: 'synced', synced: st.synced, ops: st.ops, bytes: st.bytes, retries: st.retries, last: st.last, bson: st.bson, rss: rssMb() });
}

async function sync(n, markers) {
  const { fork } = await import('node:child_process');
  const startEpoch = Date.now() + 3000 + n * 4;   // time for the processes to load
  const tag = `m-${randomUUID().slice(0, 6)}-`;
  const workers = [];
  for (let off = 0; off < n; off += PER) {
    const w = fork(new URL(import.meta.url).pathname, ['sync-worker', String(off), String(Math.min(PER, n - off)), String(startEpoch), tag], { stdio: ['ignore', 'inherit', 'inherit', 'ipc'] });
    workers.push({ w, synced: new Promise((r) => w.on('message', (m) => m.type === 'synced' && r(m))), arrivals: new Promise((r) => w.on('message', (m) => m.type === 'arrivals' && r(m.arrivals))) });
  }
  const parts = await Promise.all(workers.map((x) => x.synced));
  const synced = parts.flatMap((p) => p.synced), ops = parts.flatMap((p) => p.ops), ok = synced.filter((s) => s !== null);
  const retries = parts.reduce((a, p) => a + p.retries, 0), last = parts.map((p) => p.last).find(Boolean) ?? '';
  console.log(`powersync crowd sync, ${n} readers in ${workers.length} process(es): synced ${ok.length}/${n}, median ${pct(ok, 0.5).toFixed(2)} s, p95 ${pct(ok, 0.95).toFixed(2)} s, slowest ${Math.max(...ok).toFixed(2)} s; ${(pct(parts.flatMap((p) => p.bytes), 0.5) / 1048576).toFixed(2)} MB of data documents per reader (${parts[0].bson ? 'BSON' : 'NDJSON'}, not decoded); ${retries} retried connections${retries ? ` (last: ${last.slice(0, 120)})` : ''}; ${Math.round(parts.reduce((a, p) => a + p.rss, 0) / n)} MB per reader`);
  const sent = new Map(), errors = [];
  for (let k = 0; k < markers; k++) {
    const id = `${tag}${String(k).padStart(4, '0')}`, t = Date.now();
    sent.set(id, t);
    try { await post('storm_items', [{ id, tenant_id: 'cmp', note: 'marker' }], principal(0)); } catch (e) { errors.push(`marker ${k}: ${e.message}`); }
    await sleep(Math.max(0, 1000 - (Date.now() - t)));
  }
  await sleep(5000);
  for (const x of workers) x.w.send('stop');
  const all = await Promise.all(workers.map((x) => x.arrivals));
  const lat = [];
  let complete = 0;
  for (const [id, t] of sent) {
    const got = all.flatMap((a) => a[id] ?? []);
    for (const at of got) lat.push(at - t);
    if (got.length >= n) complete++;
  }
  if (markers) console.log(`powersync crowd markers: ${markers} sent, ${complete} reached all ${n} readers; one write to a reader median ${ms(pct(lat, 0.5))} ms, p95 ${ms(pct(lat, 0.95))} ms, slowest ${ms(Math.max(...lat))} ms (n=${lat.length})`);
  if (errors.length) console.log(`errors: ${errors.length}, first: ${errors.slice(0, 3).join(' | ')}`);
}

// ── writes ────────────────────────────────────────────────────────────────────
async function writes(n, rate, seconds) {
  const period = (n / rate) * 1000, start = performance.now(), until = start + seconds * 1000;
  const lat = [], errors = [];
  let made = 0, acked = 0, lastAck = start;
  const app = async (i) => {
    const queue = [];
    let producing = true, k = 0, t0 = start + Math.random() * period;
    const produce = (async () => {
      for (;;) {
        const due = t0 + k * period;
        if (due >= until) break;
        await sleep(Math.max(0, due - performance.now()));
        k++; made++;
        queue.push({ at: performance.now(), row: { event_id: uuid7(), tenant_id: 'cmp', sensor_id: i, kind: 'temperature', value: 21 + Math.random(), updated_at: new Date().toISOString() } });
      }
      producing = false;
    })();
    // uploadData: one request at a time, up to 200 queued rows each.
    while (producing || queue.length) {
      if (!queue.length) { await sleep(5); continue; }
      const batch = queue.splice(0, 200);
      try {
        await post('sensor_events', batch.map((w) => w.row), principal(i));
        const now = performance.now();
        for (const w of batch) lat.push(now - w.at);
        acked += batch.length; lastAck = now;
      } catch (e) {
        errors.push(e.message); queue.unshift(...batch); await sleep(1000);   // retried, as the SDK does
        if (performance.now() > until + 120_000) break;
      }
    }
    await produce;
  };
  await Promise.all(Array.from({ length: n }, (_, i) => app(i)));
  const span = (lastAck - start) / 1000;
  console.log(JSON.stringify({ system: 'powersync', apps: n, offered_per_s: rate, seconds, made, acked, http_errors: errors.length,
    acked_per_s: Math.round(acked / span), drain_s: +((lastAck - until) / 1000).toFixed(2),
    ack_p50_ms: ms(pct(lat, 0.5)), ack_p90_ms: ms(pct(lat, 0.9)), ack_p99_ms: ms(pct(lat, 0.99)), process_mb: rssMb(),
    first_error: errors[0] }));
}

async function ready(rows) {
  const start = Date.now();
  for (;;) {
    const st = { abort: new AbortController(), synced: [null], ops: [0], arrivals: {} };
    const done = reader(0, 0, start, st).catch(() => {});
    while (st.synced[0] === null && Date.now() - start < 600_000) await sleep(200);
    st.abort.abort(); await done;
    if (st.ops[0] >= rows) { console.log(`service ready: ${st.ops[0]} operations in its first complete checkpoint, after ${((Date.now() - start) / 1000).toFixed(1)} s`); return; }
    if (Date.now() - start > 600_000) { console.log(`service not ready after 600 s (${st.ops[0]} of ${rows} operations)`); process.exit(1); }
    await sleep(2000);
  }
}

const [mode, a1, a2, a3] = process.argv.slice(2);
if (mode === 'ready') await ready(Number(a1));
else if (mode === 'sync-worker') { await syncWorker(Number(a1), Number(a2), Number(a3), process.argv[6]); await new Promise(() => {}); }
else if (mode === 'sync') await sync(Number(a1), Number(a2 ?? 0));
else if (mode === 'writes') await writes(Number(a1), Number(a2), Number(a3));
else { console.error('usage: ps_crowd.mjs sync N MARKERS | writes N RATE SECONDS'); process.exit(2); }
process.exit(0);
