// The PowerSync side of the comparison (../README.md), the same phases as zb_cmp.py, run on the comparison host in
// a node:22 container (host network) by powersync.sh.
// Service 127.0.0.1:18080, writes through PostgREST 127.0.0.1:13000 (uploadData: CRUD batches of up to 200 → one
// bulk upsert). Secrets: ./secrets/jwt.key, ./secrets/admin.uri (the administrator, for the burst).
//   node ps_cmp.mjs seed N | roundtrip S | burst N | writes P M
import { PowerSyncDatabase, Schema, Table, column } from '@powersync/node';
import { createHmac, randomUUID } from 'node:crypto';
import { readFileSync, rmSync } from 'node:fs';
import { fork } from 'node:child_process';
import pg from 'pg';

const SERVICE = 'http://127.0.0.1:18080', REST = 'http://127.0.0.1:13000';
const KEY = readFileSync('./secrets/jwt.key', 'utf8').trim();
const schema = new Schema({ storm_items: new Table({ tenant_id: column.text, note: column.text, updated_at: column.text }) });
const b64u = (b) => Buffer.from(b).toString('base64url');

function token(user) {
  const now = Math.floor(Date.now() / 1000);
  const head = b64u(JSON.stringify({ alg: 'HS256', kid: 'cmp-key', typ: 'JWT' }));
  const body = b64u(JSON.stringify({ sub: user, aud: 'powersync-cmp', iat: now, exp: now + 3600 }));
  const sig = createHmac('sha256', Buffer.from(KEY, 'base64url')).update(`${head}.${body}`).digest('base64url');
  return `${head}.${body}.${sig}`;
}

class Connector {
  // shortcut: the principal travels in a plain header (PostgREST does not verify it); the policy's cost is the same.
  constructor(user, onUploaded, principal) { this.user = user; this.onUploaded = onUploaded; this.principal = principal; }
  async fetchCredentials() { return { endpoint: SERVICE, token: token(this.user) }; }
  async uploadData(db) {
    for (;;) {
      const batch = await db.getCrudBatch(200);
      if (!batch) return;
      const rows = batch.crud.filter((e) => e.op === 'PUT').map((e) => ({ id: e.id, ...e.opData }));
      if (rows.length) {
        const r = await fetch(`${REST}/storm_items`, {
          method: 'POST', body: JSON.stringify(rows),
          headers: { 'content-type': 'application/json', prefer: 'resolution=merge-duplicates,return=minimal', 'x-principal': this.principal },
        });
        if (!r.ok) throw new Error(`PostgREST ${r.status}: ${(await r.text()).slice(0, 200)}`);
      }
      await batch.complete();
      this.onUploaded?.(rows.map((x) => x.id));
    }
  }
}

async function open(i, keep, onUploaded) {
  const file = `/tmp/pscmp-${i}.db`;
  if (!keep) for (const s of ['', '-wal', '-shm']) rmSync(file + s, { force: true });
  const db = new PowerSyncDatabase({ schema, database: { dbFilename: `pscmp-${i}.db`, dbLocation: '/tmp' } });
  // No wait between upload rounds: ZeBridge's outbox sends at once, so the round trips compare the same thing.
  await db.connect(new Connector(`user-${i}`, onUploaded, `p${String(i).padStart(2, '0')}`), { crudUploadThrottleMs: 0 });
  return db;
}
const count = async (db, where = 'TRUE') => (await db.get(`SELECT count(*) AS n FROM storm_items WHERE ${where}`)).n;
const median = (a) => { const s = [...a].sort((x, y) => x - y); return s[Math.floor(s.length / 2)]; };
const pct = (a, f) => { const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(f * s.length))]; };

// ── one child process per client for seed and writes, like zb_cmp.py's processes ──
if (process.argv[2] === 'child') {
  const [, , , role, i, arg] = process.argv;
  if (role === 'seed') {
    const t = performance.now();
    const db = await open(Number(i), arg === 'warm');
    await db.waitForFirstSync();
    while ((await count(db)) < 10000 && performance.now() - t < 120000) await new Promise((r) => setTimeout(r, 20));
    process.send({ s: (performance.now() - t) / 1000, n: await count(db) });
    await db.close();
  } else if (role === 'writes') {
    const m = Number(arg), done = new Map();
    const db = await open(Number(i), true, (ids) => { const now = Date.now(); for (const id of ids) done.set(id, now); });
    await db.waitForFirstSync();
    const sent = new Map(), tag = `w${i}-${randomUUID().slice(0, 6)}`;
    for (let k = 0; k < m; k++) {
      const id = `${tag}-${String(k).padStart(5, '0')}`;
      sent.set(id, Date.now());
      await db.execute('INSERT INTO storm_items (id, tenant_id, note) VALUES (?, ?, ?)', [id, 'cmp', 'write']);
    }
    const deadline = Date.now() + 180000;
    while ([...sent.keys()].some((id) => !done.has(id)) && Date.now() < deadline) await new Promise((r) => setTimeout(r, 20));
    const lat = [...sent].filter(([id]) => done.has(id)).map(([id, t]) => done.get(id) - t);
    process.send({ n: lat.length, sent: sent.size, first: Math.min(...sent.values()), last: Math.max(...[...sent.keys()].filter((id) => done.has(id)).map((id) => done.get(id))), lat });
    await db.close();
  }
  process.exit(0);
}

async function parallel(n, role, arg) {
  return Promise.all(Array.from({ length: n }, (_, i) => new Promise((resolve) => {
    const c = fork(new URL(import.meta.url).pathname, ['child', role, String(i), String(arg)], { stdio: ['ignore', 'ignore', 'inherit', 'ipc'] });
    c.on('message', resolve); c.on('exit', (code) => resolve({ error: `client ${i} exited ${code}` }));
  })));
}

const [phase, a1, a2] = process.argv.slice(2);
if (phase === 'seed') {
  for (const keep of ['cold', 'warm']) {
    const rs = await parallel(Number(a1), 'seed', keep);
    const ok = rs.filter((r) => r.n >= 10000).map((r) => r.s);
    if (!ok.length) { console.log('seed results:', JSON.stringify(rs).slice(0, 400)); continue; }
    console.log(`powersync seed ${keep}, ${a1} clients: synced ${ok.length}/${a1}, median ${median(ok).toFixed(2)} s, slowest ${Math.max(...ok).toFixed(2)} s`);
  }
} else if (phase === 'roundtrip') {
  const w = await open(0, true), r = await open(1, true);
  await w.waitForFirstSync(); await r.waitForFirstSync();
  const lats = [], tag = randomUUID().slice(0, 6);
  for (let k = 0; k < Number(a1); k++) {
    const id = `m-${tag}-${String(k).padStart(3, '0')}`, t = performance.now();
    await w.execute('INSERT INTO storm_items (id, tenant_id, note) VALUES (?, ?, ?)', [id, 'cmp', 'marker']);
    while (performance.now() - t < 10000) {
      if (await count(r, `id = '${id}'`)) { lats.push(performance.now() - t); break; }
      await new Promise((res) => setTimeout(res, 5));
    }
    await new Promise((res) => setTimeout(res, Math.max(0, 1000 - (performance.now() - t))));
  }
  console.log(`powersync round trip (client write -> PostgREST -> PostgreSQL -> other client), ${lats.length}/${a1}: median ${median(lats).toFixed(0)} ms, p95 ${pct(lats, 0.95).toFixed(0)} ms`);
  await w.close(); await r.close();
} else if (phase === 'burst') {
  const c = await open(0, true); await c.waitForFirstSync();
  const admin = new pg.Client({ connectionString: readFileSync('./secrets/admin.uri', 'utf8').trim(), ssl: { rejectUnauthorized: false } });
  await admin.connect();
  const tag = 'b' + randomUUID().slice(0, 6), n = Number(a1), t0 = performance.now();
  await admin.query(`INSERT INTO storm_items (id, tenant_id, note) SELECT '${tag}-' || lpad(i::text, 6, '0'), 'cmp', repeat(md5(i::text), 6) FROM generate_series(1, ${n}) i`);
  const tc = performance.now();
  while ((await count(c, `id LIKE '${tag}-%'`)) < n && performance.now() - tc < 180000) await new Promise((r) => setTimeout(r, 100));
  console.log(`powersync burst: insert ${((tc - t0) / 1000).toFixed(2)} s; commit -> client holds ${(await count(c, `id LIKE '${tag}-%'`)).toLocaleString()}/${n.toLocaleString()} rows: ${((performance.now() - tc) / 1000).toFixed(2)} s`);
  await admin.end(); await c.close();
} else if (phase === 'writes') {
  const p = Number(a1), m = Number(a2);
  const rs = await parallel(p, 'writes', m);
  for (const r of rs) if (r.error) console.log('ERROR', r.error);
  const lat = rs.flatMap((r) => r.lat ?? []), n = rs.reduce((s, r) => s + (r.n ?? 0), 0);
  const first = Math.min(...rs.map((r) => r.first ?? Infinity)), last = Math.max(...rs.map((r) => r.last ?? 0));
  console.log(`powersync writes, ${p} clients x ${m}: ${n.toLocaleString()}/${rs.reduce((s, r) => s + (r.sent ?? 0), 0).toLocaleString()} uploaded in ${((last - first) / 1000).toFixed(2)} s -> ${Math.round(n / ((last - first) / 1000)).toLocaleString()} writes/s; insert -> upload acknowledged median ${median(lat)} ms, p95 ${pct(lat, 0.95)} ms`);
}
process.exit(0);
