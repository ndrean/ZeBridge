// Run only against a disposable stack with setup.sql installed. This test runs
// the actual sweeper with shortened retention, scoped to yjs_updates.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { Client } from 'pg';
import * as Y from 'yjs';
import { compactDocument } from '../scripts/compaction.ts';

const adminUrl = process.env.YJS_MAINTENANCE_DATABASE_URL;
const writerUrl = process.env.DATABASE_WRITER_URL;
if (!adminUrl || !writerUrl) throw new Error('Set YJS_MAINTENANCE_DATABASE_URL and DATABASE_WRITER_URL for a disposable test database');
const exec = promisify(execFile);
const open = async (connectionString = adminUrl) => {
  const c = new Client({ connectionString }); await c.connect(); return c;
};
const insert = async (c: Client, doc: string, update: Uint8Array, tenant = 'acme') => {
  const id = randomUUID();
  await c.query('INSERT INTO yjs_updates (id, document_id, tenant_id, update_data) VALUES ($1,$2,$3,$4)',
    [id, doc, tenant, Buffer.from(update)]);
  return id;
};
const active = async (c: Client, doc: string, tenant = 'acme') => (await c.query(
  'SELECT * FROM yjs_updates WHERE document_id=$1 AND tenant_id=$2 AND deleted_at IS NULL', [doc, tenant])).rows;
const seed = async (c: Client, doc: string, count = 4) => {
  const ydoc = new Y.Doc(), updates: Uint8Array[] = [];
  ydoc.on('update', (u: Uint8Array) => updates.push(u));
  for (let i = 0; i < count; i++) {
    ydoc.getText('content').insert(i, String(i));
    await insert(c, doc, updates.at(-1)!);
  }
  return { ydoc, updates };
};
const textOf = (rows: any[]) => {
  const doc = new Y.Doc();
  try { for (const row of rows) Y.applyUpdate(doc, row.update_data); return doc.getText('content').toString(); }
  finally { doc.destroy(); }
};
// Observe a real transaction boundary without adding test hooks to the compactor.
const intercept = (c: Client, after: (sql: string) => Promise<void>) => ({
  query: async (sql: string, values?: any[]) => {
    const result = await c.query(sql, values); await after(sql); return result;
  },
}) as Client;

test('compaction preserves document state, scopes, and merges old offline edits', async () => {
  const c = await open(), id = randomUUID(), other = randomUUID();
  const { ydoc, updates } = await seed(c, id);
  const offline = new Y.Doc();
  try {
    Y.applyUpdate(offline, Y.mergeUpdates(updates));
    offline.getText('content').insert(0, 'offline ');
    await insert(c, other, updates[0]); await insert(c, id, updates[0], 'globex');
    const result = await compactDocument(c, 'acme', id, 2);
    assert.equal(result.compacted, true); assert.equal(result.retired, 4);
    const rows = await active(c, id);
    assert.equal(rows.length, 1); assert.equal(textOf(rows), ydoc.getText('content').toString());
    assert.equal((await active(c, other)).length, 1); assert.equal((await active(c, id, 'globex')).length, 1);
    const retired = (await c.query('SELECT * FROM yjs_updates WHERE document_id=$1 AND deleted_at IS NOT NULL', [id])).rows;
    assert.equal(retired.length, 4);
    assert.ok(retired.every((r) => Date.now() - new Date(r.deleted_at).getTime() < 10_000));
    // The merged replacement can initialize a new replica AND merge into an old one.
    Y.applyUpdate(offline, rows[0].update_data);
    await insert(c, id, Y.encodeStateAsUpdate(offline));
    await compactDocument(c, 'acme', id, 2);
    assert.equal(textOf(await active(c, id)), 'offline 0123');
    assert.equal((await compactDocument(c, 'acme', id, 2)).compacted, false);
  } finally { ydoc.destroy(); offline.destroy(); await c.end(); }
});

test('an update arriving after the compaction read remains active', async () => {
  const c = await open(), writer = await open(), id = randomUUID();
  const { ydoc, updates } = await seed(c, id);
  ydoc.getText('content').insert(4, 'tail');
  let arrived = false;
  try {
    const wrapped = intercept(c, async (sql) => {
      if (sql.startsWith('SELECT id, update_data') && !arrived) {
        arrived = true; await insert(writer, id, updates.at(-1)!);
      }
    });
    await compactDocument(wrapped, 'acme', id, 2);
    const rows = await active(c, id);
    assert.equal(rows.length, 2); assert.equal(textOf(rows), '0123tail');
  } finally { ydoc.destroy(); await c.end(); await writer.end(); }
});

test('concurrent compactors serialize and a failed retirement rolls back its replacement', async () => {
  const c = await open(), second = await open(), id = randomUUID();
  const { ydoc, updates } = await seed(c, id);
  try {
    const results = await Promise.all([compactDocument(c, 'acme', id, 2), compactDocument(second, 'acme', id, 2)]);
    assert.equal(results.filter((r) => r.compacted).length, 1);
    assert.equal((await active(c, id)).length, 1);
    await insert(c, id, updates[0]);
    const wrapped = intercept(c, async (sql) => {
      if (sql.startsWith('UPDATE public.yjs_updates')) await c.query('SELECT 1/0');
    });
    await assert.rejects(compactDocument(wrapped, 'acme', id, 2), /division by zero/);
    assert.equal((await active(c, id)).length, 2, 'original active inputs survive rollback');
    assert.equal(Number((await c.query('SELECT count(*) AS n FROM yjs_updates WHERE document_id=$1', [id])).rows[0].n), 6);
  } finally { ydoc.destroy(); await c.end(); await second.end(); }
});

test('writer cannot change payloads, retire, revive, or bypass the guard with a session flag', async () => {
  const c = await open(), edge = await open(writerUrl), id = randomUUID();
  const { ydoc } = await seed(c, id, 2);
  try {
    await edge.query("SELECT set_config('zb.principal','alice',false), set_config('zb.tenant','acme',false)");
    const row = (await active(c, id))[0];
    await assert.rejects(edge.query('UPDATE yjs_updates SET deleted_at=now() WHERE id=$1', [row.id]), /Only the Yjs table owner/);
    await edge.query("SELECT set_config('yjs.compacting','true',false)");
    await assert.rejects(edge.query('UPDATE yjs_updates SET deleted_at=now() WHERE id=$1', [row.id]), /Only the Yjs table owner/);
    await assert.rejects(edge.query("UPDATE yjs_updates SET update_data=decode('00','hex') WHERE id=$1", [row.id]), /same document and bytes/);
    await assert.rejects(edge.query('DELETE FROM yjs_updates WHERE id=$1', [row.id]), /immutable/);
    await assert.rejects(edge.query('INSERT INTO yjs_updates (id,document_id,tenant_id,update_data,deleted_at) VALUES ($1,$2,$3,$4,now())',
      [randomUUID(), id, 'acme', row.update_data]), /must be active/);
    await compactDocument(c, 'acme', id, 2);
    await assert.rejects(c.query('UPDATE yjs_updates SET deleted_at=NULL WHERE id=$1', [row.id]), /Only the Yjs table owner/);
    await edge.query("SELECT set_config('zb.principal','zb_sweeper',false)");
    const live = (await active(c, id))[0];
    await assert.rejects(edge.query('DELETE FROM yjs_updates WHERE id=$1', [live.id]), /immutable/);
  } finally { ydoc.destroy(); await c.end(); await edge.end(); }
});

test('existing bridge_sweeper retains fresh retirements and reaps them after retention', async () => {
  const c = await open(), id = randomUUID();
  const { ydoc } = await seed(c, id, 2);
  const sweep = async (retention: string) => {
    const env: NodeJS.ProcessEnv = { ...process.env, DATABASE_WRITER_URL: writerUrl, SWEEP_ONLY_TABLES: 'yjs_updates',
      GC_THRESHOLD_MS: retention, GC_ALLOW_SHORT_THRESHOLD: '1' };
    delete env.GC_DRY_RUN;
    await exec(process.env.YJS_TEST_SWEEPER ?? fileURLToPath(new URL('../../../../zig-out/bin/bridge_sweeper', import.meta.url)),
      ['--once'], { env, timeout: 15_000 });
  };
  try {
    await compactDocument(c, 'acme', id, 2);
    await sweep('604800000');
    assert.equal(Number((await c.query('SELECT count(*) AS n FROM yjs_updates WHERE document_id=$1', [id])).rows[0].n), 3);
    await sleep(1100);
    await sweep('1000');
    assert.equal(Number((await c.query('SELECT count(*) AS n FROM yjs_updates WHERE document_id=$1', [id])).rows[0].n), 1);
    assert.equal(textOf(await active(c, id)), '01');
    // Restore the test database's advertised retention, without moving its floor backwards.
    await sweep('604800000');
  } finally { ydoc.destroy(); await c.end(); }
});

// Client compaction: what a `mutate` INSERT with `replaces` does, run here directly as the writer.
const asAlice = async () => {
  const edge = await open(writerUrl);
  await edge.query("SELECT set_config('zb.principal','alice',false), set_config('zb.tenant','acme',false)");
  return edge;
};
const replace = (c: Client, doc: string, inputs: any[], replaces: unknown = JSON.stringify(inputs.map((r) => r.id)), id = randomUUID()) =>
  c.query('INSERT INTO yjs_updates (id, document_id, tenant_id, update_data, replaces) VALUES ($1,$2,$3,$4,$5)',
    [id, doc, 'acme', Buffer.from(Y.mergeUpdates(inputs.map((r) => r.update_data))), replaces]).then(() => id);
const refused = (p: Promise<unknown>, message: RegExp) =>
  assert.rejects(p, (e: any) => e.code === '23514' && message.test(e.message));

test('a writer compaction retires exactly its inputs and stores no list', async () => {
  const c = await open(), edge = await asAlice(), id = randomUUID();
  const { ydoc } = await seed(c, id, 4);
  try {
    const rows = await active(c, id);
    const kept = rows.pop();
    const replacement = await replace(edge, id, rows);
    const now = await active(c, id);
    assert.deepEqual(now.map((r) => r.id).sort(), [replacement, kept.id].sort());
    assert.equal(now.find((r) => r.id === replacement).replaces, null, 'the list is an input, never stored');
    assert.equal(textOf(now), ydoc.getText('content').toString());
    const retired = (await c.query('SELECT id FROM yjs_updates WHERE document_id=$1 AND deleted_at IS NOT NULL', [id])).rows;
    assert.deepEqual(retired.map((r) => r.id).sort(), rows.map((r) => r.id).sort());
  } finally { ydoc.destroy(); await c.end(); await edge.end(); }
});

test('a writer compaction is refused outside its document, twice, or malformed, and changes nothing', async () => {
  const c = await open(), edge = await asAlice(), id = randomUUID(), other = randomUUID();
  const { ydoc, updates } = await seed(c, id, 3);
  try {
    const rows = await active(c, id);
    const foreignTenant = { id: await insert(c, id, updates[0], 'globex'), update_data: updates[0] };
    const foreignDoc = { id: await insert(c, other, updates[0]), update_data: updates[0] };
    const outside = /missing, already retired, or outside/;
    await refused(replace(edge, id, [rows[0], foreignTenant]), outside);
    await refused(replace(edge, id, [rows[0], foreignDoc]), outside);
    await refused(replace(edge, id, [rows[0], { id: randomUUID(), update_data: updates[0] }]), outside);
    const self = randomUUID();
    await refused(replace(edge, id, rows, JSON.stringify([rows[0].id, self]), self), /distinct and exclude/);
    await refused(replace(edge, id, rows, JSON.stringify([rows[0].id, rows[0].id])), /distinct and exclude/);
    for (const bad of ['', '  ', 'not json', '{"a":1}', '["x"]', '[1]']) await refused(replace(edge, id, rows, bad), /JSON array of update IDs/);
    await refused(replace(edge, id, rows, '[]'), /1 to 256/);
    await refused(replace(edge, id, rows, JSON.stringify(Array.from({ length: 257 }, randomUUID))), /1 to 256/);
    assert.equal((await active(c, id)).length, 3, 'every refusal left the inputs active and added nothing');
    await replace(edge, id, rows);
    await refused(replace(edge, id, rows), outside); // the same inputs again: already retired
    // Stored rows never carry a list; setting one later is an edit of an immutable update.
    await assert.rejects(edge.query('UPDATE yjs_updates SET replaces=$2 WHERE id=$1', [(await active(c, id))[0].id, '[]']), /same document and bytes/);
    await assert.rejects(c.query('UPDATE yjs_updates SET replaces=$2 WHERE id=$1', [foreignDoc.id, '[]']), /same document and bytes/);
  } finally { ydoc.destroy(); await c.end(); await edge.end(); }
});

test('two writers compacting overlapping updates: one wins, the other is refused, no deadlock', async () => {
  const c = await open(), a = await asAlice(), b = await asAlice(), id = randomUUID();
  const { ydoc } = await seed(c, id, 6);
  try {
    const rows = await active(c, id);
    // Opposite input orders and overlapping sets; the per-document lock makes the second see the first.
    const results = await Promise.allSettled([replace(a, id, rows.slice(0, 4)), replace(b, id, rows.slice(2).reverse())]);
    assert.equal(results.filter((r) => r.status === 'fulfilled').length, 1);
    const loser = results.find((r) => r.status === 'rejected') as PromiseRejectedResult;
    assert.equal(loser.reason.code, '23514');
    assert.equal(textOf(await active(c, id)), ydoc.getText('content').toString());
  } finally { ydoc.destroy(); await c.end(); await a.end(); await b.end(); }
});

test('a replacement the version guard declines retires nothing (the bridge upsert)', async () => {
  const c = await open(), edge = await asAlice(), id = randomUUID();
  const { ydoc } = await seed(c, id, 3);
  try {
    const rows = await active(c, id);
    // The replacement's ID is already taken by a newer row of this document.
    const taken = randomUUID();
    await c.query(`INSERT INTO yjs_updates (id, document_id, tenant_id, update_data, updated_at)
      VALUES ($1, $2, 'acme', $3, now() + interval '1 day')`, [taken, id, Buffer.from(Y.encodeStateAsUpdate(new Y.Doc()))]);
    // The statement the bridge sends for an INSERT (mutation_listener.zig: ON CONFLICT … WHERE version <).
    const declined = await edge.query(`INSERT INTO yjs_updates (id, document_id, tenant_id, update_data, replaces, updated_at)
      VALUES ($1, $2, 'acme', $3, $4, now())
      ON CONFLICT (id) DO UPDATE SET document_id = EXCLUDED.document_id, tenant_id = EXCLUDED.tenant_id,
        update_data = EXCLUDED.update_data, replaces = EXCLUDED.replaces, updated_at = EXCLUDED.updated_at
      WHERE yjs_updates.updated_at IS NULL OR yjs_updates.updated_at < EXCLUDED.updated_at`,
    [taken, id, Buffer.from(Y.mergeUpdates(rows.map((r) => r.update_data))), JSON.stringify(rows.map((r) => r.id))]);
    assert.equal(declined.rowCount, 0, 'the guard declined: nothing inserted or updated');
    const now = await active(c, id);
    assert.equal(now.length, 4, 'every input is still active');
    assert.equal(textOf(now), ydoc.getText('content').toString(), 'a fresh reader still reconstructs the text');
  } finally { ydoc.destroy(); await c.end(); await edge.end(); }
});
