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
