// Run against a dev stack AFTER setup.sql. Creates a new test document; optional
// compaction retires only its updates. Replicas have distinct client IDs.
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { ZeBridge } from '@zebridge/client/node';
import * as Y from 'yjs';
import { ZeBridgeYjsProvider, TABLE } from '../src/provider.ts';
import { Client } from 'pg';
import { compactDocument } from '../scripts/compaction.ts';

if (!process.env.NATS_URL) throw new Error('Set NATS_URL for the running test stack');
const dir = mkdtempSync(join(tmpdir(), 'zb-yjs-live-'));
const documentId = crypto.randomUUID();
const failures: Error[] = [];
const refusals: string[] = [];
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
  const p = new ZeBridgeYjsProvider(c, documentId, c.tenant, (e) => failures.push(e), (d) => refusals.push(d));
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
  // Compaction from the editor, through mutate: Bob, offline, compacts the same confirmed rows
  // as Alice. Alice's is applied; Bob's then finds its inputs retired and is refused, not fatal.
  const active = (c: ZeBridge) => c.query('SELECT id, replaces FROM yjs_updates WHERE document_id=? AND deleted_at IS NULL', documentId);
  const compacted = async (p: ZeBridgeYjsProvider) => {
    let r = await p.compact();
    // Rows whose echo is still owed are not confirmed yet; they become eligible shortly.
    for (let i = 0; !r.compacted && i < 50; i++) { await sleep(100); r = await p.compact(); }
    assert.ok(r.compacted, `nothing compacted: ${(r as { reason?: string }).reason}`); return r;
  };
  const before = (await active(reopened)).length;
  await b.close();
  const bobs = await compacted(bob);
  const verdicts: string[] = [];
  reopened.onVerdict((v) => { if (v.table === TABLE) verdicts.push(`${v.key.id}:${v.outcome}`); });
  const alices = await compacted(restored);
  assert.equal(alices.replaced, before);
  await until('editor compaction applied and replicated', async () => {
    const rows = await active(reopened);
    return verdicts.includes(`${alices.id}:applied`) && rows.length === 1 && rows[0].id === alices.id && rows[0].replaces === null;
  });
  await b.connect(); bob.refresh();
  await until('the second compaction refused and reported', async () => refusals.length === 1 && await b.pending() === 0);
  assert.equal(bob.text.toString(), restored.text.toString(), 'a refused compaction loses nothing');
  assert.deepEqual((await active(b)).map((r) => r.id), [alices.id]);
  const after = makeClient('after-editor-compaction'); await after.connect();
  assert.equal((await open(after)).text.toString(), restored.text.toString(), 'a fresh replica loads the one merged row');
  if (process.env.YJS_MAINTENANCE_DATABASE_URL) {
    // A replacement whose ID is taken by a newer row: the bridge's version guard declines the
    // upsert, and the inputs must stay active (they would be retired with nothing replacing them).
    restored.text.insert(0, '> '); await restored.flush();
    restored.text.insert(restored.text.length, ' <'); await restored.flush();
    await until('two more edits confirmed', async () => (await active(reopened)).length === 3 && await reopened.pending() === 0);
    const admin = new Client({ connectionString: process.env.YJS_MAINTENANCE_DATABASE_URL });
    const taken = crypto.randomUUID();
    try {
      await admin.connect();
      const inputs = (await admin.query('SELECT id, update_data FROM yjs_updates WHERE document_id=$1 AND deleted_at IS NULL', [documentId])).rows;
      await admin.query(`INSERT INTO yjs_updates (id, document_id, tenant_id, update_data, updated_at)
        VALUES ($1, $2, $3, $4, now() + interval '1 day')`, [taken, documentId, a.tenant, Buffer.from(Y.encodeStateAsUpdate(new Y.Doc()))]);
      await until('the newer row replicated', async () => (await active(reopened)).some((r) => r.id === taken));
      const outcome: string[] = [];
      reopened.onVerdict((v) => { if (v.table === TABLE && String(v.key.id) === taken) outcome.push(v.outcome); });
      await reopened.mutateWithId(TABLE, 'INSERT', { id: taken }, {
        id: taken, document_id: documentId, tenant_id: a.tenant,
        update_data: Y.mergeUpdates(inputs.map((r) => new Uint8Array(r.update_data))),
        replaces: JSON.stringify(inputs.map((r) => r.id)),
      }, { independent: true });
      await until('the declined replacement settled', async () => outcome.length > 0 && await reopened.pending() === 0);
      assert.notEqual(outcome[0], 'applied');
      const still = (await admin.query('SELECT id FROM yjs_updates WHERE document_id=$1 AND deleted_at IS NULL', [documentId])).rows.map((r) => r.id);
      assert.deepEqual(still.sort(), [...inputs.map((r) => r.id), taken].sort(), 'no input retired by a declined replacement');
      const witness = makeClient('after-declined'); await witness.connect();
      assert.equal((await open(witness)).text.toString(), restored.text.toString(), 'a fresh replica still has every edit');
      console.log(JSON.stringify({ declinedReplacement: { verdict: outcome[0], inputsKept: inputs.length } }));
    } finally { await admin.end(); }
  }
  console.log(JSON.stringify({ editorCompaction: { replaced: alices.replaced, bytesBefore: alices.bytesBefore,
    bytesAfter: alices.bytesAfter, refused: refusals[0], bobReplaced: bobs.replaced } }));

  assert.deepEqual(failures, []);
  console.log(JSON.stringify({ documentId, text: loaded.text.toString(),
    activeUpdates: (await reopened.query('SELECT id FROM yjs_updates WHERE document_id=? AND deleted_at IS NULL', documentId)).length,
    compaction: !!process.env.YJS_MAINTENANCE_DATABASE_URL, replicas: dir }));
} finally {
  for (const p of providers) p.destroy();
  for (const c of clients) await c.close();
}
