// The PowerSync side of the crowd (../README.md, "A crowd of 500"): many clients in one process, without the SDK,
// which holds a SQLite database and about 150 MB per client. Run by powersync.sh in a node:22 container.
//
//   node ps_crowd.mjs sync N MARKERS     N readers of /sync/stream (NDJSON), each timed from one shared start to
//                                        its first complete checkpoint; then MARKERS seconds of one marker row a
//                                        second (through PostgREST), each timed to every reader. A reader keeps
//                                        no replica: it counts the operations it is sent, which is the service's
//                                        share of a sync, not a client's apply.
//   node ps_crowd.mjs writes N RATE SECONDS
//                                        N simulated apps, RATE writes/s offered in all, into sensor_events. Each
//                                        app uploads as uploadData does: one request at a time, up to 200 queued
//                                        rows per request, to PostgREST as its own principal; each write timed
//                                        from its creation to the 201 that covers it.
//
// Service 127.0.0.1:18080, PostgREST 127.0.0.1:13000; ./secrets/jwt.key signs the readers' tokens.
import { createHmac, randomBytes, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';

const SERVICE = 'http://127.0.0.1:18080', REST = 'http://127.0.0.1:13000';
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
async function reader(i, start, seen, state) {
  const res = await fetch(`${SERVICE}/sync/stream`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Token ${token(`crowd-${i}`)}`, accept: 'application/x-ndjson' },
    body: JSON.stringify({ buckets: [], include_checksum: true, raw_data: true, client_id: `crowd-${i}`, streams: { include_defaults: true, subscriptions: [] } }),
    signal: state.abort.signal,
  });
  if (!res.ok) throw new Error(`sync/stream ${res.status}: ${(await res.text()).slice(0, 160)}`);
  const dec = new TextDecoder();
  let buf = '';
  for await (const chunk of res.body) {
    buf += dec.decode(chunk, { stream: true });
    let nl;
    while ((nl = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
      if (!line) continue;
      const m = JSON.parse(line);
      if (m.data) {
        state.ops[i] += m.data.data?.length ?? 0;
        for (const op of m.data.data ?? []) if (op.object_id?.startsWith('m-')) seen.get(op.object_id)?.set(i, performance.now());
      } else if (m.checkpoint_complete && state.synced[i] === null) {
        state.synced[i] = (performance.now() - start) / 1000;
      }
    }
  }
}

async function sync(n, markers) {
  const state = { abort: new AbortController(), synced: Array(n).fill(null), ops: Array(n).fill(0), errors: [] };
  const seen = new Map();   // marker id -> Map(reader -> arrival)
  const start = performance.now();
  const runs = Array.from({ length: n }, (_, i) => reader(i, start, seen, state).catch((e) => {
    if (e.name !== 'AbortError') state.errors.push(`reader ${i}: ${e.message}`);
  }));
  while (state.synced.some((s) => s === null) && performance.now() - start < 300_000 && state.errors.length < n) await sleep(200);
  const ok = state.synced.filter((s) => s !== null);
  console.log(`powersync crowd sync, ${n} readers: synced ${ok.length}/${n}, median ${pct(ok, 0.5).toFixed(2)} s, p95 ${pct(ok, 0.95).toFixed(2)} s, slowest ${Math.max(...ok).toFixed(2)} s; ops per reader ${pct(state.ops, 0.5)}; process ${rssMb()} MB`);
  const tag = randomUUID().slice(0, 6), sent = new Map();
  for (let k = 0; k < markers; k++) {
    const id = `m-${tag}-${String(k).padStart(4, '0')}`, t = performance.now();
    seen.set(id, new Map()); sent.set(id, t);
    try { await post('storm_items', [{ id, tenant_id: 'cmp', note: 'marker' }], principal(0)); } catch (e) { state.errors.push(`marker ${k}: ${e.message}`); }
    await sleep(Math.max(0, 1000 - (performance.now() - t)));
  }
  await sleep(5000);
  const lat = [];
  let complete = 0;
  for (const [id, t] of sent) {
    const got = seen.get(id);
    for (const at of got.values()) lat.push(at - t);
    if (got.size === n) complete++;
  }
  if (markers) console.log(`powersync crowd markers: ${markers} sent, ${complete} reached all ${n} readers; one write to a reader median ${ms(pct(lat, 0.5))} ms, p95 ${ms(pct(lat, 0.95))} ms, slowest ${ms(Math.max(...lat))} ms`);
  if (state.errors.length) console.log(`errors: ${state.errors.length}, first: ${state.errors.slice(0, 3).join(' | ')}`);
  state.abort.abort();
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

const [mode, a1, a2, a3] = process.argv.slice(2);
if (mode === 'sync') await sync(Number(a1), Number(a2 ?? 0));
else if (mode === 'writes') await writes(Number(a1), Number(a2), Number(a3));
else { console.error('usage: ps_crowd.mjs sync N MARKERS | writes N RATE SECONDS'); process.exit(2); }
process.exit(0);
