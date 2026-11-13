// Run against a dev stack AFTER setup.sql. Creates a new test document; optional
// compaction retires only its updates. Replicas have distinct client IDs.
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { ZeBridge } from '@zebridge/client/node';
import { ZeBridgeYjsProvider, TABLE } from '../src/provider.ts';
import { Client } from 'pg';
import { compactDocument } from '../scripts/compaction.ts';

if (!process.env.NATS_URL) throw new Error('Set NATS_URL for the running test stack');
const dir = mkdtempSync(join(tmpdir(), 'zb-yjs-live-'));
const documentId = crypto.randomUUID();
const failures: Error[] = [];
const clients: ZeBridge[] = [];
const providers: ZeBridgeYjsProvider[] = [];
const makeClient = (name: string) => {
  const c = new ZeBridge({
    natsUrl: process.env.NATS_URL,
    principal: process.env.ZB_TEST_PRINCIPAL ?? 'alice',
    credsPath: process.env.NATS_CREDS,
    jsDomain: process.env.ZB_JS_DOMAIN,
    dbPath: join(dir, `${name}.sqlite3`),
    tables: [TABLE], heartbeatMs: 0,
  });
  c.onLog((topic, data, level) => { if (level === 'ERROR') console.error(topic, data); });
  clients.push(c);
  return c;
};
const open = async (c: ZeBridge) => {
  const p = new ZeBridgeYjsProvider(c, documentId, c.tenant, (e) => failures.push(e));
  providers.push(p); await p.start(); return p;
};
const until = async (label: string, fn: () => Promise<boolean>) => {
  const deadline = Date.now() + 45_000;
  while (Date.now() < deadline) {
    if (failures.length) throw failures[0];
    if (await fn()) return;
    await sleep(100);
  }
  throw new Error(`Timed out: ${label}`);
};

try {
  const a = makeClient('alice'), b = makeClient('bob');
  await a.connect(); await b.connect();
  assert.ok(a.tenant); assert.equal(a.tenant, b.tenant);
  const alice = await open(a), bob = await open(b);
  alice.text.insert(0, 'Hello 🌍'); await alice.flush();
  await until('initial CDC delivery', async () => bob.text.toString() === 'Hello 🌍' && await a.pending() === 0);
  await a.close(); await b.close();
  alice.text.insert(5, ' Alice'); bob.text.insert(5, ' Bob');
  await alice.flush(); await bob.flush();
  assert.equal(await a.pending(), 1); assert.equal(await b.pending(), 1);

  // Re-open Alice's file with a fresh client, exercising persisted outbox recovery.
  alice.destroy();
  // Offline writes reacquire the replica lock; release it before handing the file
  // to a new owner, just as exiting the old process would do.
  await a.close();
  const reopened = makeClient('alice');
  await reopened.connect();
  const restored = await open(reopened);
  await b.connect();
  bob.refresh();
  await until('offline edits merged and acknowledged', async () =>
    restored.text.toString() === bob.text.toString() &&
    restored.text.toString().includes('Alice') && restored.text.toString().includes('Bob') &&
    await reopened.pending() === 0 && await b.pending() === 0);

  const rows = await reopened.query('SELECT update_data FROM yjs_updates WHERE document_id = ?', documentId);
  assert.equal(rows.length, 3, 'one immutable row per edit, no remote echo writes');
  assert.ok(rows.every((r) => r.update_data instanceof Uint8Array));
  // A completely fresh replica loads from the server, not either editor's local history.
  const fresh = makeClient('fresh'); await fresh.connect();
  const loaded = await open(fresh);
  assert.equal(loaded.text.toString(), restored.text.toString());

  if (process.env.YJS_MAINTENANCE_DATABASE_URL) {
    // Bob has an old CRDT state plus an unsent edit while the accepted history is
    // compacted. Alice stays connected, and a new replica has only the replacement.
    await b.close();
    bob.text.insert(bob.text.length, ' after compaction'); await bob.flush();
    const maintenance = new Client({ connectionString: process.env.YJS_MAINTENANCE_DATABASE_URL });
    try {
      await maintenance.connect();
      const result = await compactDocument(maintenance, a.tenant, documentId, 2);
      assert.equal(result.retired, 3);
    } finally { await maintenance.end(); }
    await until('compacted log replicated', async () =>
      (await reopened.query('SELECT id FROM yjs_updates WHERE document_id=? AND deleted_at IS NULL', documentId)).length === 1);
    assert.equal(restored.text.toString(), loaded.text.toString(), 'retiring SQL rows does not delete document text');
    const checkpointClient = makeClient('checkpoint'); await checkpointClient.connect();
    const checkpoint = await open(checkpointClient);
    assert.equal(checkpoint.text.toString(), restored.text.toString());
    assert.equal((await checkpointClient.query('SELECT id FROM yjs_updates WHERE document_id=? AND deleted_at IS NULL', documentId)).length, 1);
    await b.connect(); bob.refresh();
    await until('old offline edits merged with checkpoint', async () =>
      bob.text.toString() === restored.text.toString() && checkpoint.text.toString() === restored.text.toString() &&
      restored.text.toString().endsWith(' after compaction') && await b.pending() === 0);
    assert.equal((await reopened.query('SELECT id FROM yjs_updates WHERE document_id=? AND deleted_at IS NULL', documentId)).length, 2);
  }
  assert.deepEqual(failures, []);
  console.log(JSON.stringify({ documentId, text: loaded.text.toString(),
    activeUpdates: (await reopened.query('SELECT id FROM yjs_updates WHERE document_id=? AND deleted_at IS NULL', documentId)).length,
    compaction: !!process.env.YJS_MAINTENANCE_DATABASE_URL, replicas: dir }));
} finally {
  for (const p of providers) p.destroy();
  for (const c of clients) await c.close();
}
