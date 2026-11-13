import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import * as Y from 'yjs';
import type { ZeBridge, Verdict } from '@zebridge/client';
import { ZeBridgeYjsProvider, TABLE } from '../src/provider.ts';

// SQLite replicas exercise the adapter with real binary storage. Transport is
// controlled here so updates can be delayed, duplicated, and delivered backwards.
function replica() {
  const db = new DatabaseSync(':memory:');
  db.exec(`CREATE TABLE yjs_updates (id TEXT PRIMARY KEY, document_id TEXT, tenant_id TEXT,
    update_data BLOB, deleted_at TEXT)`);
  const changes = new Set<() => void>();
  const verdicts = new Set<(v: Verdict) => void>();
  let writes = 0;
  let refuse = false;
  const insert = (row: any) => {
    db.prepare('INSERT OR IGNORE INTO yjs_updates VALUES (?, ?, ?, ?, NULL)')
      .run(row.id, row.document_id, row.tenant_id, row.update_data);
    for (const fn of changes) fn();
  };
  const client: Pick<ZeBridge, 'query' | 'mutateWithId' | 'onChange' | 'onVerdict'> = {
    query: async (sql, ...params) => db.prepare(sql).all(...params),
    mutateWithId: async (table, op, key, values, opts) => {
      assert.equal(table, TABLE); assert.equal(op, 'INSERT');
      assert.equal(key.id, values!.id); assert.equal(opts?.independent, true);
      assert.ok(values!.update_data instanceof Uint8Array);
      if (refuse) throw new Error('disk full: nothing queued');
      writes++; insert(values);
      return { version: String(writes), writeId: String(key.id) };
    },
    onChange: (_table, fn) => { changes.add(fn); return () => { changes.delete(fn); }; },
    onVerdict: (fn) => { verdicts.add(fn); return () => { verdicts.delete(fn); }; },
  };
  return {
    db, client, insert, rows: () => db.prepare('SELECT * FROM yjs_updates ORDER BY rowid').all(),
    writes: () => writes, refuse: () => { refuse = true; },
    verdict: (v: Verdict) => { for (const fn of verdicts) fn(v); },
  };
}

test('offline concurrent text edits converge after reversed, duplicate delivery and reload', async () => {
  const a = replica(), b = replica();
  const errors: Error[] = [];
  const alice = new ZeBridgeYjsProvider(a.client, 'doc', 'acme', (e) => errors.push(e));
  const bob = new ZeBridgeYjsProvider(b.client, 'doc', 'acme', (e) => errors.push(e));
  await alice.start(); await bob.start();
  try {
    alice.text.insert(0, 'Hello 🌍'); await alice.flush();
    for (const row of a.rows()) b.insert(row);
    await bob.flush();
    assert.equal(b.writes(), 0, 'remote updates are not uploaded again');

    // Both edit the same position without seeing the other. Bob also deletes
    // existing text; multiple dependent updates exercise out-of-order import.
    alice.text.insert(5, ' Alice');
    bob.text.insert(5, ' Bob');
    bob.text.delete(0, 1);
    await alice.flush(); await bob.flush();
    assert.notEqual(alice.text.toString(), bob.text.toString());
    const all = [...a.rows(), ...b.rows()].reverse();
    for (const row of [...all, ...all]) { a.insert(row); b.insert(row); }
    await alice.flush(); await bob.flush();
    assert.equal(alice.text.toString(), bob.text.toString());
    assert.match(alice.text.toString(), /Alice/); assert.match(alice.text.toString(), /Bob/);
    assert.equal(a.rows().length, 4); assert.equal(b.rows().length, 4);
    assert.equal(a.writes(), 2); assert.equal(b.writes(), 2);

    const reload = new ZeBridgeYjsProvider(a.client, 'doc', 'acme', (e) => errors.push(e));
    await reload.start();
    assert.equal(reload.text.toString(), alice.text.toString());
    assert.equal(a.writes(), 2, 'loading persisted history produces no new writes');
    reload.destroy();
    assert.deepEqual(errors, []);
  } finally { alice.destroy(); bob.destroy(); a.db.close(); b.db.close(); }
});

test('only the selected document and tenant are imported', async () => {
  const r = replica();
  const other = new Y.Doc(); other.getText('content').insert(0, 'private');
  const bytes = Y.encodeStateAsUpdate(other);
  r.insert({ id: '1', document_id: 'other-doc', tenant_id: 'acme', update_data: bytes });
  r.insert({ id: '2', document_id: 'doc', tenant_id: 'globex', update_data: bytes });
  const provider = new ZeBridgeYjsProvider(r.client, 'doc', 'acme', assert.fail);
  try { await provider.start(); assert.equal(provider.text.toString(), ''); }
  finally { provider.destroy(); other.destroy(); r.db.close(); }
});

test('a compacted replacement preserves live text and an old offline editor can still merge', async () => {
  const a = replica(), b = replica();
  const alice = new ZeBridgeYjsProvider(a.client, 'doc', 'acme', assert.fail);
  const bob = new ZeBridgeYjsProvider(b.client, 'doc', 'acme', assert.fail);
  await alice.start(); await bob.start();
  try {
    alice.text.insert(0, 'Hello'); await alice.flush();
    b.insert(a.rows()[0]); await bob.flush();
    bob.text.insert(5, ' offline'); await bob.flush();
    alice.text.insert(5, ' online'); await alice.flush();
    const merged = Y.mergeUpdates(a.rows().map((r) => r.update_data as Uint8Array));
    a.db.exec("UPDATE yjs_updates SET deleted_at='retired'");
    alice.refresh(); await alice.flush();
    assert.equal(alice.text.toString(), 'Hello online', 'retirement is not a Yjs deletion');
    const checkpoint = { id: 'checkpoint', document_id: 'doc', tenant_id: 'acme', update_data: merged };
    a.insert(checkpoint); await alice.flush();
    b.db.prepare("UPDATE yjs_updates SET deleted_at='retired' WHERE id=?").run(a.rows()[0].id);
    b.insert(checkpoint); await bob.flush();
    a.insert(b.rows().find((r) => r.id !== 'checkpoint' && r.deleted_at === null)!);
    await alice.flush();
    assert.equal(alice.text.toString(), bob.text.toString());
    assert.match(alice.text.toString(), /online/); assert.match(alice.text.toString(), /offline/);
    const reloaded = new ZeBridgeYjsProvider(a.client, 'doc', 'acme', assert.fail);
    await reloaded.start();
    assert.equal(reloaded.text.toString(), alice.text.toString());
    reloaded.destroy();
    assert.equal(a.writes(), 2); assert.equal(b.writes(), 1, 'no re-uploads of merged replacements');
  } finally { alice.destroy(); bob.destroy(); a.db.close(); b.db.close(); }
});

test('a failed durable write stops further uploads and retains text for recovery', async () => {
  const r = replica();
  const errors: Error[] = [];
  const provider = new ZeBridgeYjsProvider(r.client, 'doc', 'acme', (e) => errors.push(e));
  await provider.start();
  try {
    r.refuse(); provider.text.insert(0, 'keep this text');
    await assert.rejects(provider.flush(), /disk full/);
    provider.text.insert(provider.text.length, '!');
    await assert.rejects(provider.flush(), /disk full/);
    assert.equal(provider.text.toString(), 'keep this text!');
    assert.equal(errors.length, 1); assert.equal(r.rows().length, 0);
  } finally { provider.destroy(); r.db.close(); }
});

test('a rejected update surfaces recovery instead of silently continuing a divergent document', async () => {
  const r = replica();
  const errors: Error[] = [];
  const provider = new ZeBridgeYjsProvider(r.client, 'doc', 'acme', (e) => errors.push(e));
  await provider.start();
  try {
    provider.text.insert(0, 'save me'); await provider.flush();
    r.verdict({ table: TABLE, version: '1', key: { id: r.rows()[0].id },
      columns: ['update_data'], outcome: 'rejected', reason: 'permission denied' });
    await assert.rejects(provider.flush(), /Copy your text/);
    provider.text.insert(provider.text.length, '!');
    assert.equal(r.writes(), 1); assert.equal(errors.length, 1);
  } finally { provider.destroy(); r.db.close(); }
});
