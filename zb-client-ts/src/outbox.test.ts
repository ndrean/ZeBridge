import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { ZeBridge } from './libzb.ts';
import { makePgliteStorage } from './pglite-storage.ts';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

function fixture(count = 300) {
  const db = new DatabaseSync(':memory:');
  db.exec(`CREATE TABLE _zebridge_outbox (msg_id TEXT PRIMARY KEY, subject TEXT, payload TEXT, tbl TEXT, row_id TEXT,
    attempts INTEGER DEFAULT 0, created_at INTEGER, published INTEGER DEFAULT 1, retry_at INTEGER DEFAULT 0)`);
  for (let i = 0; i < count; i++) db.prepare('INSERT INTO _zebridge_outbox(msg_id,subject,payload,tbl,row_id,created_at) VALUES (?,?,?,?,?,?)')
    .run(`id${i}`, 'mutation.alice.item.insert', '{}', 'item', String(i), i);
  const gets: string[] = [], sends: string[] = [];
  let managers = 0, fail = false, stored = '', holding = false;
  const held: Array<() => void> = [];
  const make = () => {
    const c: any = Object.create(ZeBridge.prototype);
    const exec = async (sql: string, ...args: any[]) => {
      const st = db.prepare(sql); return st.columns().length ? st.all(...args) : (st.run(...args), []);
    };
    let queue = Promise.resolve();
    Object.assign(c, {
      nc: {}, outboxInitPromise: Promise.resolve(), outboxWork: null, outboxWake: false, outboxWindow: 128,
      nextOutboxRetry: 0, outboxReconnect: 0, outboxRetryEpoch: 0, holdUntil: 0, pendingWrites: new Map(), syncedTables: new Map(), verdictHandlers: new Set(),
      config: { principal: 'alice', grammar: {} }, run: exec,
      transaction: (fn: any) => {
        const w = queue.then(async () => {
          db.exec('BEGIN'); try { await fn(exec); db.exec('COMMIT'); } catch (e) { db.exec('ROLLBACK'); throw e; }
        }); queue = w.catch(() => {}); return w;
      },
      appendLog: () => {}, gcWatermark: async () => null, ackPrefix: () => 'mutation_ack',
      transport: {
        headers: () => new Map(),
        jetstreamManager: async () => {
          managers++;
          return { direct: { getMessage: async (_s: string, opts: any) => {
            gets.push(opts.last_by_subj.split('.').pop());
            if (stored) return {subject: opts.last_by_subj, data: new TextEncoder().encode(stored)};
            throw new Error('404 not found');
          } } };
        },
        jetstream: () => ({publish: async (_s: string, _b: Uint8Array, opts: any) => {
          sends.push(opts.headers.get('Nats-Msg-Id')); const seq = sends.length;
          if (holding) await new Promise<void>((r) => held.push(r)); // the ack, released by the test
          if (fail) throw new Error('hub unavailable'); return {seq};
        }}),
      },
    }); return c;
  };
  return {db, gets, sends, make, managers: () => managers, fail: (v: boolean) => {fail=v;}, stored: (v: string) => {stored=v;},
    hold: (v: boolean) => {holding=v;}, held: () => held.length, release: () => {for (const r of held.splice(0)) r();}};
}

test('PubAcks do not free slots; a verdict immediately clocks the next send', async () => {
  const f=fixture(), c=f.make();
  try {
    await c.recoverOutbox(); assert.equal(f.sends.length,128); assert.equal(f.managers(),1);
    await c.recoverOutbox(); assert.equal(f.sends.length,128);
    await c.handleVerdict('mutation_ack.alice.id0',new TextEncoder().encode('{"status":"accepted"}'));
    await c.outboxWork;
    assert.equal(f.sends.length,129); assert.equal(f.sends.at(-1),'id128');
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM _zebridge_outbox WHERE retry_at > 0').get()!.n,128);
  } finally {c.nc=null; await c.outboxWork; f.db.close();}
});

test('mutate does not wait for PubAcks; a recovery pass waits for its probe, then once for the window', async () => {
  // One write per round trip capped a client at 26 writes/s 23 ms from its server. The acks
  // here never resolve until the test releases them, so waiting and not waiting are told apart.
  const f=fixture(128), c=f.make();
  const tick = () => new Promise((r) => setTimeout(r, 10));
  try {
    f.hold(true);
    await Promise.race([c.flushOutboxPass(false), new Promise((_, no) => setTimeout(() => no(new Error("mutate's pass waited for its PubAcks")), 500))]);
    assert.equal(f.sends.length,128); assert.equal(f.held(),128); // returned with every ack unresolved
    f.release();
    const g=fixture(128), d=g.make(); g.hold(true);
    let done=false; const p=d.recoverOutbox().then(() => {done=true;});
    await tick(); assert.equal(g.sends.length,1); assert.equal(done,false); // the probe, waiting for its ack
    g.release(); await tick();
    assert.equal(g.sends.length,128); assert.equal(g.held(),127); assert.equal(done,false); // the burst, its acks pending
    g.release(); await p; assert.equal(done,true);
    g.db.close();
  } finally {f.release(); c.nc=null; await c.outboxWork; f.db.close();}
});

test('restart preserves reservations; only due IDs retry, with exponential backoff', async () => {
  const f=fixture(), c=f.make();
  try {
    await c.recoverOutbox(); const reopened=f.make(); await reopened.recoverOutbox(); assert.equal(f.sends.length,128);
    f.db.exec("UPDATE _zebridge_outbox SET retry_at=1 WHERE msg_id='id7'"); await reopened.recoverOutbox();
    assert.equal(f.sends.length,129); assert.equal(f.sends.at(-1),'id7');
    const r=f.db.prepare("SELECT attempts,retry_at FROM _zebridge_outbox WHERE msg_id='id7'").get()!;
    assert.equal(r.attempts,2); assert.ok(Number(r.retry_at)>=Date.now()+9000);
  } finally {c.nc=null; await c.outboxWork; f.db.close();}
});

test('an ambiguous failed publish keeps its slot and leaves peers eligible', async () => {
  const f=fixture(), c=f.make();
  try {
    f.fail(true); await c.recoverOutbox(); assert.deepEqual(f.sends,['id0']);
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM _zebridge_outbox WHERE retry_at>0').get()!.n,128);
    f.fail(false); await c.recoverOutbox(); assert.equal(f.sends.length,128); assert.equal(f.sends.at(-1),'id127');
    assert.equal(f.db.prepare("SELECT attempts FROM _zebridge_outbox WHERE msg_id='id128'").get()!.attempts,0);
  } finally {c.nc=null; await c.outboxWork; f.db.close();}
});

test('stored definitive verdicts drain many windows without publishing or a timer', async () => {
  const f=fixture(1000), c=f.make();
  try {
    f.stored('{"status":"accepted"}'); await c.recoverOutbox();
    while (f.db.prepare('SELECT count(*) AS n FROM _zebridge_outbox').get()!.n) await new Promise(setImmediate);
    assert.equal(f.gets.length,1000); assert.equal(f.sends.length,0);
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM _zebridge_outbox').get()!.n,0);
  } finally {c.nc=null; await c.outboxWork; f.db.close();}
});

test('live retry_after holds sends; a retained failed verdict cannot renew it forever', async () => {
  const f=fixture(1), c=f.make();
  try {
    const failed='{"status":"failed","reason":"rate_limited","retry_after_ms":1000}';
    await c.handleVerdict('mutation_ack.alice.id0',new TextEncoder().encode(failed));
    await c.recoverOutbox(); assert.equal(f.sends.length,0);
    c.holdUntil=0; f.stored(failed); await c.recoverOutbox(); assert.deepEqual(f.sends,['id0']); assert.equal(c.holdUntil,0);
  } finally {c.nc=null; await c.outboxWork; f.db.close();}
});

test('a reconnect retries the existing cohort without opening another window', async () => {
  const f=fixture(), c=f.make();
  try {
    await c.recoverOutbox();
    c.outboxReconnect++;
    await c.recoverOutbox();
    assert.equal(f.sends.length,256);
    assert.equal(new Set(f.sends).size,128);
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM _zebridge_outbox WHERE retry_at>0').get()!.n,128);
  } finally {c.nc=null; await c.outboxWork; f.db.close();}
});

test('a CDC echo before its verdict does not swallow onVerdict: either order reports the write once', async () => {
  // shared_record: alice's accepted doc write was CONFIRMED by its echo, which dropped the
  // outbox row; the verdict then found nothing ("not from this session") and onVerdict never fired.
  const version = '2026-11-01T00:00:00.000000Z';
  const settle = () => new Promise((r) => setTimeout(r, 20));
  for (const order of ['verdict, then echo', 'echo, then verdict']) {
    const f = fixture(1), c = f.make();
    const seen: any[] = [];
    try {
      c.verdictHandlers = new Set([(v: any) => seen.push(v)]);
      f.db.prepare('UPDATE _zebridge_outbox SET payload = ?, row_id = ? WHERE msg_id = ?')
        .run(JSON.stringify({ key: { id: '0' }, data: { id: '0', rating: 5, updated_at: version }, version }), '0', 'id0');
      const state = { pkCols: ['id'], versionColumn: 'updated_at' };
      c.syncedTables.set('item', state);
      c.pendingWrites.set('id0', { table: 'item', id: '0', at: Date.now(), version });
      const verdict = () => c.handleVerdict('mutation_ack.alice.id0', new TextEncoder().encode('{"status":"accepted"}'));
      const echo = async () => { c.confirmEcho('item', state, { data: { id: '0', updated_at: version } }); await settle(); };
      if (order.startsWith('verdict')) { await verdict(); await echo(); } else { await echo(); await verdict(); }
      await settle();
      assert.equal(seen.length, 1, `${order}: ${seen.length} onVerdict call(s)`);
      assert.equal(seen[0].outcome, 'applied', order);
      assert.deepEqual(seen[0].columns, ['id', 'rating'], order);
      assert.equal(f.db.prepare('SELECT count(*) AS n FROM _zebridge_outbox').get()!.n, 0, `${order}: the outbox row is gone`);
    } finally { c.nc = null; f.db.close(); }
  }
});

test('an echo racing its verdict, then a duplicate verdict: onVerdict fires once', async () => {
  // Both paths read the outbox before either deleted it: the verdict reported the write, the
  // echo kept a description, and a duplicate verdict consumed it and reported again.
  const version = '2026-11-01T00:00:00.000000Z';
  const f = fixture(1), c = f.make();
  const seen: any[] = [];
  try {
    c.verdictHandlers = new Set([(v: any) => seen.push(v)]);
    f.db.prepare('UPDATE _zebridge_outbox SET payload = ?, row_id = ? WHERE msg_id = ?')
      .run(JSON.stringify({ key: { id: '0' }, data: { id: '0', rating: 5, updated_at: version }, version }), '0', 'id0');
    const state = { pkCols: ['id'], versionColumn: 'updated_at' };
    c.syncedTables.set('item', state);
    c.pendingWrites.set('id0', { table: 'item', id: '0', at: Date.now(), version });
    const accepted = () => c.handleVerdict('mutation_ack.alice.id0', new TextEncoder().encode('{"status":"accepted"}'));
    c.confirmEcho('item', state, { data: { id: '0', updated_at: version } });
    await accepted();
    await new Promise((r) => setTimeout(r, 20));
    await accepted();
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(seen.length, 1, `${seen.length} onVerdict call(s) for one write`);
  } finally { c.nc = null; f.db.close(); }
});

test('a verdict whose settlement fails is retried by its next delivery, and reported once', async () => {
  const version = '2026-11-01T00:00:00.000000Z';
  const f = fixture(1), c = f.make();
  const seen: any[] = [];
  try {
    c.verdictHandlers = new Set([(v: any) => seen.push(v)]);
    f.db.prepare('UPDATE _zebridge_outbox SET payload = ?, row_id = ? WHERE msg_id = ?')
      .run(JSON.stringify({ key: { id: '0' }, data: { id: '0', rating: 5, updated_at: version }, version }), '0', 'id0');
    c.syncedTables.set('item', { pkCols: ['id'], versionColumn: 'updated_at' });
    c.pendingWrites.set('id0', { table: 'item', id: '0', at: Date.now(), version });
    const real = c.outboxDrop.bind(c);
    let failures = 1;
    c.outboxDrop = async (id: string) => { if (failures-- > 0) throw new Error('database is locked'); return real(id); };
    const accepted = () => c.handleVerdict('mutation_ack.alice.id0', new TextEncoder().encode('{"status":"accepted"}'));
    await assert.rejects(accepted());
    await accepted();
    assert.equal(seen.length, 1, `${seen.length} onVerdict call(s)`);
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM _zebridge_outbox').get()!.n, 0, 'the retry removed the outbox row');
  } finally { c.nc = null; f.db.close(); }
});

test('outboxStatus: the oldest waiting write against the GC floor, warned once an hour under a day', async () => {
  const db = new DatabaseSync(':memory:');
  db.exec(`CREATE TABLE _zebridge_outbox (msg_id TEXT PRIMARY KEY, payload TEXT)`);
  db.exec(`CREATE TABLE zebridge_gc_watermark (watermark TEXT)`);
  const logs: string[] = [];
  const c: any = Object.create(ZeBridge.prototype);
  Object.assign(c, {
    outboxInitPromise: Promise.resolve(),
    run: async (sql: string, ...args: any[]) => { const st = db.prepare(sql); return st.columns().length ? st.all(...args) : (st.run(...args), []); },
    appendLog: (_src: string, text: string, level: string) => logs.push(`${level} ${text}`),
  });
  try {
    assert.deepEqual(await c.outboxStatus(), { pending: 0, floorMarginMs: null }, 'nothing waits');
    const add = db.prepare('INSERT INTO _zebridge_outbox (msg_id, payload) VALUES (?, ?)');
    add.run('a', JSON.stringify({ version: '2026-08-01T05:00:00.000000Z' }));
    add.run('b', JSON.stringify({ version: '2026-08-01T02:00:00.000000Z' }));
    add.run('c', '{}');
    assert.deepEqual(await c.outboxStatus(), { pending: 3, floorMarginMs: null }, 'no floor received yet');
    db.prepare('INSERT INTO zebridge_gc_watermark (watermark) VALUES (?)').run('2026-08-01T00:00:00Z');
    assert.deepEqual(await c.outboxStatus(), { pending: 3, floorMarginMs: 7_200_000 }, 'the oldest, b, is two hours ahead');
    assert.deepEqual(logs, ['WARNING outbox: the oldest waiting write is 2.0 h from the GC floor — once the floor reaches it, it is refused (PredatesGcWatermark)']);
    await c.outboxStatus();
    assert.equal(logs.length, 1, 'not again within the hour');
    c.floorWarnedAt = Date.now() - 3_600_000;
    db.exec(`UPDATE zebridge_gc_watermark SET watermark = '2026-07-01T00:00:00Z'`);
    assert.equal((await c.outboxStatus()).floorMarginMs, 2_685_600_000, 'thirty-one days and two hours');
    assert.equal(logs.length, 1, 'no warning while more than a day remains');
  } finally { db.close(); }
});

// A refused write's local undo is owed durably (`_zebridge_undo`) and settled per row: a
// foreign key that refuses it while a dependent row is still here keeps it owed, never throws
// (that ended Node), and is retried after a verdict, a change or a restart. The twin of
// libzb's "undo: …" tests.
function undoFixture(path = ':memory:') {
  const db = new DatabaseSync(path);
  db.exec(`PRAGMA foreign_keys = ON;
    CREATE TABLE IF NOT EXISTS _zebridge_outbox (msg_id TEXT PRIMARY KEY, subject TEXT, payload TEXT, tbl TEXT, row_id TEXT, before TEXT, created_at INTEGER, optimistic TEXT, authoritative INTEGER);
    CREATE TABLE IF NOT EXISTS _zebridge_undo (msg_id TEXT PRIMARY KEY, tbl TEXT NOT NULL, row_id TEXT NOT NULL, payload TEXT NOT NULL, before TEXT, optimistic TEXT,
      created_at INTEGER NOT NULL, authoritative INTEGER, mode TEXT NOT NULL, since INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS p (id TEXT PRIMARY KEY, title TEXT);
    CREATE TABLE IF NOT EXISTS c (id TEXT PRIMARY KEY, pid TEXT NOT NULL REFERENCES p(id));`);
  const c: any = Object.create(ZeBridge.prototype);
  const logs: string[] = [];
  c.run = async (sql: string, ...args: any[]) => {
    const stmt = db.prepare(sql);
    return stmt.columns().length ? stmt.all(...args) : (stmt.run(...args), []);
  };
  c.transaction = async (fn: any) => {
    db.exec('BEGIN');
    try { await fn(c.run); db.exec('COMMIT'); } catch (e) { db.exec('ROLLBACK'); throw e; }
  };
  const table = (cols: string[]) => ({ pkCols: ['id'], columns: cols, tombstoneColumn: null });
  Object.assign(c, {
    outboxInitPromise: Promise.resolve(), appendLog: (_t: string, m: string) => logs.push(m),
    eventListeners: [], tableListeners: {}, scheduleRecount: () => {}, undoOwed: true,
    syncedTables: new Map([['p', table(['id', 'title'])], ['c', table(['id', 'pid'])]]),
    // The optimistic apply, reduced to the upsert it is for these two tables.
    applyEvent: async (t: string, ev: any, exec: any) => {
      const cols = Object.keys(ev.data);
      await exec(`INSERT INTO ${t} (${cols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})
        ON CONFLICT (id) DO UPDATE SET ${cols.map((k) => `${k} = excluded.${k}`).join(', ')}`, ...cols.map((k) => ev.data[k]));
    },
  });
  // A write as `mutate` leaves it: before-image read, optimistic event (key merged into the
  // data) applied, outbox entry queued with the SPARSE wire data.
  const write = async (msg: string, t: 'p' | 'c', op: string, id: string, col: string, val: string) => {
    const before = db.prepare(`SELECT * FROM ${t} WHERE id = ?`).get(id) ?? null;
    const authoritative = await c.beforeIsAuthoritative(t, c.syncedTables.get(t), id, c.run);
    const ev = { operation: op, data: { id, [col]: val } };
    await c.applyEvent(t, ev, c.run);
    db.prepare('INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, before, created_at, optimistic, authoritative) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
      .run(msg, 's', JSON.stringify({ operation: op, key: { id }, data: { [col]: val } }), t, id, before == null ? null : JSON.stringify(before), c.nextCreated(), JSON.stringify(ev), authoritative ? 1 : 0);
  };
  const has = (t: string, id: string) => db.prepare(`SELECT 1 FROM ${t} WHERE id = ?`).all(id).length === 1;
  const title = (id: string) => (db.prepare('SELECT title FROM p WHERE id = ?').get(id) as any)?.title ?? null;
  return { db, c, logs, write, has, title };
}

test('undo: a refused parent\'s undo waits for its child\'s in either order, survives a restart, and spares a row that changed', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zb-undo-'));
  const path = `${dir}/r.sqlite3`;
  const { db, c, logs, write, has, title } = undoFixture(path);

  // Parent first (dependent_writes.py): refused while the child is here, owed, unblocked by it.
  await write('p1', 'p', 'INSERT', 'p1', 'title', 'refused');
  await write('r1', 'c', 'INSERT', 'r1', 'pid', 'p1');
  await c.revertOptimisticWrite('p1', 'restore');
  assert.ok(has('p', 'p1'));
  assert.equal(await c.undoing(), 1);
  assert.ok(logs.some((m) => m.includes('local undo of p1 is blocked (FOREIGN KEY constraint failed)')));
  await c.revertOptimisticWrite('r1', 'restore');
  assert.ok(!has('p', 'p1') && !has('c', 'r1'));
  assert.equal(await c.undoing(), 0);

  // Child first, then a duplicate verdict: nothing blocks, nothing is owed twice.
  await write('p2', 'p', 'INSERT', 'p2', 'title', 'refused');
  await write('r2', 'c', 'INSERT', 'r2', 'pid', 'p2');
  await c.revertOptimisticWrite('r2', 'restore');
  await c.revertOptimisticWrite('p2', 'restore');
  await c.revertOptimisticWrite('p2', 'restore');
  assert.ok(!has('p', 'p2'));
  assert.equal(await c.undoing(), 0);

  // A newer value lands before the retry (CDC): the late undo leaves the row alone.
  await write('p3', 'p', 'INSERT', 'p3', 'title', 'refused');
  await write('r3', 'c', 'INSERT', 'r3', 'pid', 'p3');
  await c.revertOptimisticWrite('p3', 'restore');
  db.exec(`UPDATE p SET title = 'newer' WHERE id = 'p3'; DELETE FROM c WHERE id = 'r3'; DELETE FROM _zebridge_outbox WHERE msg_id = 'r3'`);
  await c.runUndos();
  assert.equal(title('p3'), 'newer');
  assert.equal(await c.undoing(), 0);

  // A restart while the undo is owed: still owed after reopening, done once the child is gone.
  await write('p4', 'p', 'INSERT', 'p4', 'title', 'refused');
  await write('r4', 'c', 'INSERT', 'r4', 'pid', 'p4');
  await c.revertOptimisticWrite('p4', 'restore');
  db.close();
  const again = undoFixture(path);
  assert.equal(await again.c.undoing(), 1);
  again.db.exec(`DELETE FROM c WHERE id = 'r4'`);
  await again.c.runUndos();
  assert.ok(!again.has('p', 'p4'));
  assert.equal(await again.c.undoing(), 0);
  again.db.close();
  rmSync(dir, { recursive: true, force: true });
});

test('undo: two refused writes on one row leave no trace in either order; a sparse UPDATE is restored', async () => {
  const { c, write, has, title } = undoFixture();
  await write('a1', 'p', 'INSERT', 'x1', 'title', 'A');
  await write('b1', 'p', 'UPDATE', 'x1', 'title', 'B');
  await c.revertOptimisticWrite('a1', 'restore');
  assert.equal(title('x1'), 'B'); // B still pending, rebuilt over A's base (no row)
  await c.revertOptimisticWrite('b1', 'restore');
  assert.ok(!has('p', 'x1'));

  await write('a2', 'p', 'INSERT', 'x2', 'title', 'A');
  await write('b2', 'p', 'UPDATE', 'x2', 'title', 'B');
  await c.revertOptimisticWrite('b2', 'restore');
  assert.equal(title('x2'), 'A');
  await c.revertOptimisticWrite('a2', 'restore');
  assert.ok(!has('p', 'x2'));
  assert.equal(await c.undoing(), 0);

  const { db, c: c2, write: write2, title: title2 } = undoFixture();
  db.exec(`INSERT INTO p (id, title) VALUES ('x3', 'old')`);
  await write2('u3', 'p', 'UPDATE', 'x3', 'title', 'new');
  await c2.revertOptimisticWrite('u3', 'restore');
  assert.equal(title2('x3'), 'old');
});

test('undo: a change (CDC) retries an owed undo without another verdict', async () => {
  const { db, c, write, has } = undoFixture();
  await write('p1', 'p', 'INSERT', 'p1', 'title', 'refused');
  await write('r1', 'c', 'INSERT', 'r1', 'pid', 'p1');
  await c.revertOptimisticWrite('p1', 'restore');
  assert.equal(await c.undoing(), 1);
  // CDC removes the child (its write resolved without a refusal of its own).
  db.exec(`DELETE FROM _zebridge_outbox WHERE msg_id = 'r1'; DELETE FROM c WHERE id = 'r1'`);
  c.triggerChange('c');
  await new Promise((r) => setTimeout(r, 20));
  assert.ok(!has('p', 'p1'));
  assert.equal(await c.undoing(), 0);
});

test('undo: a reseed settles the owed undos of the rows it replaced; a pending write then rolls back to the seeded row', async () => {
  const { db, c, write, title } = undoFixture();
  db.exec(`INSERT INTO p (id, title) VALUES ('x', 'old')`);
  await write('ua', 'p', 'UPDATE', 'x', 'title', 'a');
  await write('ub', 'p', 'UPDATE', 'x', 'title', 'b');
  // Refused; its undo not run yet.
  db.exec(`INSERT INTO _zebridge_undo (msg_id, tbl, row_id, payload, before, optimistic, created_at, mode, since)
    SELECT msg_id, tbl, row_id, payload, before, optimistic, created_at, 'restore', 0 FROM _zebridge_outbox WHERE msg_id = 'ua';
    DELETE FROM _zebridge_outbox WHERE msg_id = 'ua'`);
  db.exec(`UPDATE p SET title = 'srv' WHERE id = 'x'`); // a full replaced the rows
  await c.reapplyPending('p');
  assert.equal(title('x'), 'b');
  assert.equal(await c.undoing(), 0);
  await c.revertOptimisticWrite('ub', 'restore');
  assert.equal(title('x'), 'srv');
});

test('undo: a server value that landed between two refused edits survives both, in either order', async () => {
  const { db, c, write, title } = undoFixture();
  for (const [x, first, second] of [['x1', 'x1a', 'x1b'], ['x2', 'x2b', 'x2a']]) {
    db.prepare(`INSERT INTO p (id, title) VALUES (?, 'original')`).run(x);
    await write(`${x}a`, 'p', 'UPDATE', x, 'title', 'A');
    db.prepare(`UPDATE p SET title = 'server' WHERE id = ?`).run(x); // CDC
    await write(`${x}b`, 'p', 'UPDATE', x, 'title', 'B');
    await c.revertOptimisticWrite(first, 'restore');
    await c.revertOptimisticWrite(second, 'restore');
    assert.equal(title(x), 'server');
    assert.equal(await c.undoing(), 0);
  }
});

test('outbox: the first write queued on a fresh PGlite replica does not wait on itself', async () => {
  const storage = makePgliteStorage()('zb-first-write');
  const c: any = Object.create(ZeBridge.prototype);
  Object.assign(c, { run: storage.exec, transaction: storage.transaction, dialect: storage.dialect, outboxInitPromise: Promise.resolve() });
  try {
    await c.createOutboxTable();
    const put = storage.transaction(async (tx) => {
      await c.outboxPut({ msgId: 'm1', subject: 's', payload: { key: { id: 1 } }, table: 'item', id: 1, before: null, optimistic: {}, authoritative: true }, tx);
    });
    const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('the queued write waited on its own transaction')), 5_000));
    await Promise.race([put, timeout]);
    assert.equal(Number((await storage.exec(`SELECT count(*) AS n FROM _zebridge_outbox`))[0].n), 1);
  } finally {
    await storage.deleteDatabaseFile?.();
  }
});
