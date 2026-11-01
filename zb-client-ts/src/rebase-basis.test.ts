// A stale UPDATE is rebased only when the winner left its columns alone, judged against the
// AUTHORITATIVE winner and the row as the user saw it (`basis`), never against the displayed
// row. shared_record: after a reseed re-applied carol's pending rating 2 over alice's 5, the
// rebase read carol's own overlay as "the winner changed rating", matched it to her edit, and
// re-sent rating 2 as a fresh write that overwrote alice's newer value.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { ZeBridge } from './libzb.ts';
import { sqliteDialect } from './dialect.ts';

const V = (s: number) => `2026-11-01T16:10:${String(s).padStart(2, '0')}.000000Z`;

/// The replica after a reseed re-applied one pending edit: `visible` is the row shown (the
/// overlay), `before` the seeded row under it, `basis` what the user saw when editing.
function world(visible: Record<string, unknown>, before: Record<string, unknown>, basis: Record<string, unknown>, edit: Record<string, unknown>) {
  const db = new DatabaseSync(':memory:');
  db.exec(`CREATE TABLE item (id TEXT PRIMARY KEY, rating INTEGER, notes TEXT, updated_at TEXT)`);
  db.exec(`CREATE TABLE _zebridge_outbox (msg_id TEXT PRIMARY KEY, subject TEXT, payload TEXT, tbl TEXT, row_id TEXT,
    before TEXT, basis TEXT, created_at INTEGER, attempts INTEGER DEFAULT 0, published INTEGER DEFAULT 1, optimistic TEXT, retry_at INTEGER DEFAULT 0)`);
  db.prepare('INSERT INTO item VALUES (?, ?, ?, ?)').run(visible.id as string, visible.rating as any, visible.notes as any, visible.updated_at as string);
  db.prepare('INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, before, basis, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 0)')
    .run('m1', 'mutation.carol.item.update', JSON.stringify({ key: { id: '1' }, data: { id: '1', ...edit }, version: edit.updated_at }),
      'item', '1', JSON.stringify(before), JSON.stringify(basis));
  const resent: any[] = [], verdicts: any[] = [];
  const c: any = Object.create(ZeBridge.prototype);
  Object.assign(c, {
    outboxInitPromise: Promise.resolve(), rebase: new Map(), verdictHandlers: new Set([(v: any) => verdicts.push(v)]),
    syncedTables: new Map([['item', { pkCols: ['id'], versionColumn: 'updated_at' }]]),
    run: async (sql: string, ...args: any[]) => { const st = db.prepare(sql); return st.columns().length ? st.all(...args) : (st.run(...args), []); },
    appendLog: () => {},
    mutate: async (_t: string, _op: string, _k: any, values: any) => { resent.push(values); return { version: V(59) }; },
  });
  return { c, db, resent, verdicts };
}

test("carol's pending edit re-applied over a reseed is judged against alice's winner: lost, not re-sent", async () => {
  // carol saw rating null; alice's winner (seeded) wrote 5; carol's overlay shows her 2 with her version.
  const { c, db, resent, verdicts } = world(
    { id: '1', rating: 2, notes: 'n', updated_at: V(5) },      // displayed: carol's overlay
    { id: '1', rating: 5, notes: 'n', updated_at: V(30) },     // before: alice's seeded winner
    { id: '1', rating: null, notes: 'n', updated_at: V(1) },   // basis: what carol saw
    { rating: 2, updated_at: V(5) },
  );
  await c.holdForRebase('m1'); await c.tryRebase('m1');
  assert.deepEqual(resent, [], 'a stale edit on a column the winner changed must not be re-sent');
  assert.equal(verdicts[0]?.outcome, 'lost');
  assert.deepEqual(verdicts[0]?.lostColumns, ['rating']);
  db.close();
});

test('a disjoint edit re-applied over a reseed is still rebased', async () => {
  // the winner changed notes only; carol's rating edit does not conflict with it.
  const { c, db, resent, verdicts } = world(
    { id: '1', rating: 2, notes: 'w', updated_at: V(5) },
    { id: '1', rating: null, notes: 'w', updated_at: V(30) },
    { id: '1', rating: null, notes: 'n', updated_at: V(1) },
    { rating: 2, updated_at: V(5) },
  );
  await c.holdForRebase('m1'); await c.tryRebase('m1');
  assert.equal(resent.length, 1, 'a disjoint edit is re-sent onto the newer row');
  assert.equal(verdicts[0]?.outcome, 'rebased');
  db.close();
});

test('without a reseed (the winner overwrote the overlay) the decision is unchanged', async () => {
  // the CDC winner replaced carol's copy: rating 5 shown, carol saw null, edited 2 → lost.
  const { c, db, resent, verdicts } = world(
    { id: '1', rating: 5, notes: 'n', updated_at: V(30) },
    { id: '1', rating: null, notes: 'n', updated_at: V(1) },
    { id: '1', rating: null, notes: 'n', updated_at: V(1) },
    { rating: 2, updated_at: V(5) },
  );
  await c.holdForRebase('m1'); await c.tryRebase('m1');
  assert.deepEqual(resent, []);
  assert.equal(verdicts[0]?.outcome, 'lost');
  db.close();
});

test('two pending edits on one row: the confirmed row under both overlays is the winner', async () => {
  // carol's older edit (rating 2) is being rebased; her later edit (notes) is still queued and
  // its overlay is what the row shows. The seeded winner (alice's rating 5) is under both.
  const { c, db, resent, verdicts } = world(
    { id: '1', rating: 2, notes: 'x', updated_at: V(7) },      // displayed: carol's SECOND overlay
    { id: '1', rating: 5, notes: 'n', updated_at: V(30) },     // first edit's before: alice's seeded winner
    { id: '1', rating: null, notes: 'n', updated_at: V(1) },   // basis: what carol saw
    { rating: 2, updated_at: V(5) },
  );
  db.prepare('INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, before, basis, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 1)')
    .run('m2', 'mutation.carol.item.update', JSON.stringify({ key: { id: '1' }, data: { id: '1', notes: 'x', updated_at: V(7) }, version: V(7) }),
      'item', '1', JSON.stringify({ id: '1', rating: 2, notes: 'n', updated_at: V(5) }), JSON.stringify({ id: '1', rating: null, notes: 'n', updated_at: V(1) }));
  await c.holdForRebase('m1'); await c.tryRebase('m1');
  assert.deepEqual(resent, [], 'the later overlay is not the winner');
  assert.equal(verdicts[0]?.outcome, 'lost');
  db.close();
});

test('an outbox from before the basis column keeps the basis it had, whatever a later reseed does', async () => {
  // The upgrade: `before` is what carol saw; the migration copies it into `basis` at once; a
  // reseed then rewrites `before` to alice's seeded row and lays carol's edit over it.
  const db = new DatabaseSync(':memory:');
  db.exec(`CREATE TABLE item (id TEXT PRIMARY KEY, rating INTEGER, notes TEXT, updated_at TEXT)`);
  db.exec(`CREATE TABLE _zebridge_outbox (msg_id TEXT PRIMARY KEY, subject TEXT NOT NULL, payload TEXT NOT NULL, tbl TEXT NOT NULL,
    row_id TEXT NOT NULL, before TEXT, created_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0)`);
  db.prepare('INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, before, created_at) VALUES (?, ?, ?, ?, ?, ?, 0)')
    .run('m1', 'mutation.carol.item.update', JSON.stringify({ key: { id: '1' }, data: { id: '1', rating: 2, updated_at: V(5) }, version: V(5) }),
      'item', '1', JSON.stringify({ id: '1', rating: null, notes: 'n', updated_at: V(1) }));
  const resent: any[] = [], verdicts: any[] = [];
  const c: any = Object.create(ZeBridge.prototype);
  const run = async (sql: string, ...args: any[]) => { const st = db.prepare(sql); return st.columns().length ? st.all(...args) : (st.run(...args), []); };
  Object.assign(c, {
    outboxInitPromise: Promise.resolve(), rebase: new Map(), verdictHandlers: new Set([(v: any) => verdicts.push(v)]),
    syncedTables: new Map([['item', { pkCols: ['id'], versionColumn: 'updated_at' }]]), dialect: sqliteDialect,
    run, appendLog: () => {}, mutate: async (_t: string, _o: string, _k: any, v: any) => { resent.push(v); return { version: V(59) }; },
  });
  await c.createOutboxTable();
  db.prepare('UPDATE _zebridge_outbox SET before = ? WHERE msg_id = ?').run(JSON.stringify({ id: '1', rating: 5, notes: 'n', updated_at: V(30) }), 'm1');
  db.prepare('INSERT INTO item VALUES (?, ?, ?, ?)').run('1', 2, 'n', V(5));
  await c.holdForRebase('m1'); await c.tryRebase('m1');
  assert.deepEqual(resent, [], 'judged against the basis copied at the upgrade, not the refreshed before');
  assert.equal(verdicts[0]?.outcome, 'lost');
  db.close();
});

test('two stale edits on one row both settle: the confirmed row outlives the first decision', async () => {
  // edit 1 (rating 2) is lost to alice's seeded 5; edit 2 (notes) does not conflict. Once edit 1
  // has left the outbox, the seeded winner must still be found for edit 2, or it waits for ever.
  const { c, db, resent, verdicts } = world(
    { id: '1', rating: 2, notes: 'x', updated_at: V(7) },
    { id: '1', rating: 5, notes: 'n', updated_at: V(30) },
    { id: '1', rating: null, notes: 'n', updated_at: V(1) },
    { rating: 2, updated_at: V(5) },
  );
  db.prepare('INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, before, basis, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 1)')
    .run('m2', 'mutation.carol.item.update', JSON.stringify({ key: { id: '1' }, data: { id: '1', notes: 'x', updated_at: V(7) }, version: V(7) }),
      'item', '1', JSON.stringify({ id: '1', rating: 2, notes: 'n', updated_at: V(5) }), JSON.stringify({ id: '1', rating: null, notes: 'n', updated_at: V(1) }));
  const drop = (id: string) => db.prepare('DELETE FROM _zebridge_outbox WHERE msg_id = ?').run(id);
  await c.holdForRebase('m1'); drop('m1'); await c.tryRebase('m1');
  assert.equal(verdicts[0]?.outcome, 'lost');
  await c.holdForRebase('m2'); drop('m2'); await c.tryRebase('m2');
  assert.equal(verdicts.length, 2, 'edit 2 is decided, not held for ever');
  assert.equal(verdicts[1]?.outcome, 'rebased');
  assert.deepEqual(resent.map((v) => v.notes), ['x']);
  db.close();
});
