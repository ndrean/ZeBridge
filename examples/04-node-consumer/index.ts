/// Consumer #2: a Node microservice on the SAME core as the browser demo.
///
/// This example exists to prove the two seams (NOTES.md §10). The browser
/// supplies sqlocal/OPFS storage and a WebSocket dial; this supplies
/// better-sqlite3 and a TCP dial. Everything between — schema watch, chain-first
/// seeding, CDC apply, the whole write path — is the same code. If a sync rule
/// had leaked into the browser adapter, this file would not work.
///
///   pnpm start                      (seed, follow CDC, write once, report)
///   ZB_ENGINE=pglite pnpm start     (the same, on PostgreSQL-in-process — PGlite,
///                                    persisted under /tmp; the dialect seam, §10bc)
///
/// Env: NATS_URL, ZB_PRINCIPAL, ZB_CREDS, ZB_TABLE, ZB_DB, ZB_ENGINE (sqlite|pglite).
import { readFileSync } from 'node:fs';
import { ZeBridge } from 'zb-client-ts';
import { nodeStorage, nodeConnect } from 'zb-client-ts/node';
import { makePgliteStorage } from 'zb-client-ts/pglite';

const REPO = new URL('../../', import.meta.url).pathname;
const PRINCIPAL = process.env.ZB_PRINCIPAL ?? 'omar';
const CREDS = readFileSync(process.env.ZB_CREDS ?? `${REPO}scripts/native/creds/${PRINCIPAL}.creds`, 'utf8');
const GRAMMAR = JSON.parse(readFileSync(`${REPO}grammar.json`, 'utf8'));
// ⚠️ Not `counter_public`: this example used to INSERT a fresh random uid there on
// every run, which is exactly how that table accumulated 27 stray counter rows
// (§10bc). `test_types` is the table built to take a full, typed row — and on the
// PostgreSQL engine the arrays and the jsonb below arrive as real arrays and jsonb.
const TABLE = process.env.ZB_TABLE ?? 'test_types';
const ENGINE = (process.env.ZB_ENGINE ?? 'sqlite') as 'sqlite' | 'pglite';
const DB = process.env.ZB_DB ?? `/tmp/zb-node-${PRINCIPAL}-${Date.now()}.sqlite3`;

const zb = new ZeBridge({
  natsUrl: process.env.NATS_URL ?? 'nats://127.0.0.1:4222',
  principal: PRINCIPAL,
  creds: CREDS,
  grammar: GRAMMAR,
  durable: true,
  // ── the two seams ──
  storage: ENGINE === 'pglite' ? makePgliteStorage({ persist: true, dataDir: '/tmp' }) : nodeStorage,
  connect: nodeConnect,
});

// Only the lifecycle lines: CDC fires one log per event and would bury the report.
zb.onLog((topic: string, data: any, level: string) => {
  if (topic === 'CDC') return;
  console.log(`[${topic} ${level}] ${typeof data === 'string' ? data : JSON.stringify(data)}`);
});

const deadline = (ms: number) => new Promise((r) => setTimeout(r, ms));

console.log(`▶ node consumer: principal=${PRINCIPAL} engine=${ENGINE} db=${DB}`);
await zb.connect();

// Seeding and the CDC catch-up are driven by connect(); give them a moment to land.
await deadline(8000);

const tables: string[] = zb.tableNames?.() ?? [];
console.log(`\n📚 tables materialized locally: ${tables.length ? tables.join(', ') : '(none)'}`);
console.log(`🏷  tenant resolved: ${zb.tenant || '(none)'}`);

for (const t of tables.slice(0, 12)) {
  try {
    const [{ n }] = await zb.query(`SELECT COUNT(*) AS n FROM ${t}`);
    console.log(`   ${t.padEnd(24)} ${n} row(s)`);
  } catch (e: any) {
    console.log(`   ${t.padEnd(24)} query failed: ${e.message}`);
  }
}

// ── the write path, through the same mutate() the browser uses ──
const before = (await zb.query(`SELECT COUNT(*) AS n FROM ${TABLE}`))[0].n;
const uid = zb.uuid();
const version = zb.newVersion();
console.log(`\n✍  mutate(): INSERT ${TABLE} uid=${uid}`);
// A full row (§7's asymmetry: the local upsert's INSERT arm needs every NOT NULL).
const row0 = TABLE === 'test_types'
  ? { uid, some_text: 'written by the node consumer', age: 42, is_true: true,
      tags: ['node', ENGINE, 'with,comma'], matrix: [[1, 2], [3, 4]],
      metadata: { source: 'node-consumer', engine: ENGINE }, price: '1234.56789012', temperature: 36.6,
      tenant_id: zb.tenant || undefined, updated_at: version, inserted_at: version }
  : { uid, value: 4242, updated_at: version, inserted_at: version };
await zb.mutate(TABLE, 'INSERT', { uid }, row0, { version });

await deadline(4000);
const after = (await zb.query(`SELECT COUNT(*) AS n FROM ${TABLE}`))[0].n;
const row = (await zb.query(`SELECT * FROM ${TABLE} WHERE uid = ?`, uid))[0];
const outbox = await zb.outboxAll();

console.log(`\n── result ──────────────────────────────`);
console.log(`rows ${before} → ${after}`);
console.log(`round-tripped row: ${row ? JSON.stringify(row) : 'NOT FOUND'}`);
console.log(`outbox drained: ${Array.isArray(outbox) ? outbox.length === 0 : outbox} (${Array.isArray(outbox) ? outbox.length : '?'} pending)`);
const ok = !!row && Array.isArray(outbox) && outbox.length === 0;
console.log(ok ? '\n✅ PASS — the Node host drives the same core end to end' : '\n❌ FAIL');

await zb.close();
process.exit(ok ? 0 : 1);
