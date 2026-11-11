import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { sqliteDialect } from './dialect.ts';
import { ZeBridge, OrderingRefusal } from './libzb.ts';
import { messageIdCreatedMs, messageIdOfClient, mutationMsgId } from './core.ts';
import { makePgliteStorage } from './pglite-storage.ts';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

function fixture(count = 300) {
  const db = new DatabaseSync(':memory:');
  db.exec(`CREATE TABLE _zebridge_outbox (msg_id TEXT PRIMARY KEY, subject TEXT, payload TEXT, tbl TEXT, row_id TEXT,
    attempts INTEGER DEFAULT 0, created_at INTEGER, published INTEGER DEFAULT 1, retry_at INTEGER DEFAULT 0, before TEXT, optimistic TEXT, authoritative INTEGER, incarnation INTEGER, other_incarnation INTEGER NOT NULL DEFAULT 0)`);
  db.exec(`CREATE TABLE _zebridge_undo (msg_id TEXT PRIMARY KEY,tbl TEXT,row_id TEXT,payload TEXT,before TEXT,optimistic TEXT,created_at INTEGER,authoritative INTEGER,mode TEXT,since INTEGER,attempts INTEGER DEFAULT 0)`);
  db.exec(`CREATE TABLE _zebridge_base(tbl TEXT NOT NULL,row_id TEXT NOT NULL,at INTEGER NOT NULL,data TEXT,PRIMARY KEY(tbl,row_id))`);
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
    CREATE TABLE IF NOT EXISTS _zebridge_outbox (msg_id TEXT PRIMARY KEY, subject TEXT, payload TEXT, tbl TEXT, row_id TEXT, before TEXT, created_at INTEGER, optimistic TEXT, authoritative INTEGER, attempts INTEGER DEFAULT 0, published INTEGER DEFAULT 0, retry_at INTEGER DEFAULT 0, incarnation INTEGER, other_incarnation INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS _zebridge_undo (msg_id TEXT PRIMARY KEY, tbl TEXT NOT NULL, row_id TEXT NOT NULL, payload TEXT NOT NULL, before TEXT, optimistic TEXT,
      created_at INTEGER NOT NULL, authoritative INTEGER, mode TEXT NOT NULL, since INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS _zebridge_base(tbl TEXT NOT NULL,row_id TEXT NOT NULL,at INTEGER NOT NULL,data TEXT,PRIMARY KEY(tbl,row_id));
    CREATE TABLE IF NOT EXISTS _zebridge_stamp (tbl TEXT PRIMARY KEY, incarnation INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS _zebridge_fence (tbl TEXT PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS _zebridge_generations (tbl TEXT PRIMARY KEY, watermark TEXT, cutoff_lsn INTEGER, seed_epoch INTEGER NOT NULL DEFAULT 0, seed_seq INTEGER, seed_stream TEXT, shared_seed_seq INTEGER);
    CREATE TABLE IF NOT EXISTS _zebridge_reconcile(msg_id TEXT PRIMARY KEY,tbl TEXT NOT NULL,reason TEXT,version TEXT DEFAULT '',data TEXT,routes TEXT DEFAULT '',targets TEXT DEFAULT '',since INTEGER,state TEXT DEFAULT 'pending');
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
    dialect: sqliteDialect, baselineActive: !!db.prepare('SELECT 1 FROM _zebridge_base LIMIT 1').get(), ledgerActive: true,
    outboxInitPromise: Promise.resolve(), appendLog: (_t: string, m: string) => logs.push(m),
    eventListeners: [], tableListeners: {}, scheduleRecount: () => {}, undoOwed: true,
    syncedTables: new Map([['p', table(['id', 'title'])], ['c', table(['id', 'pid'])]]),
    // The optimistic apply, reduced for these tables: an UPDATE of a present row updates it
    // (an upsert's INSERT would trip a NOT NULL column it does not carry), otherwise an upsert.
    applyEvent: async (t: string, ev: any, exec: any) => {
      if (ev.operation === 'DELETE') { await exec(`DELETE FROM ${t} WHERE id = ?`, ev.data.id); return; }
      const cols = Object.keys(ev.data);
      if (ev.operation === 'UPDATE' && (await exec(`SELECT 1 FROM ${t} WHERE id = ?`, ev.data.id)).length) {
        await exec(`UPDATE ${t} SET ${cols.map((k) => `${k} = ?`).join(', ')} WHERE id = ?`, ...cols.map((k) => ev.data[k]), ev.data.id);
        return;
      }
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
    const at = c.nextCreated();
    c.ledgerKeys?.include(t, id); c.ledgerChanged(); // as `outboxPut`: no base copy until CDC brings one
    db.prepare('INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, before, created_at, optimistic, authoritative) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
      .run(msg, 's', JSON.stringify({ operation: op, key: { id }, data: { [col]: val } }), t, id, before == null ? null : JSON.stringify(before), at, JSON.stringify(ev), authoritative ? 1 : 0);
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
  // The base is no row, and B (still pending) has nothing to update, upstream either: no
  // partial row is made from its sparse data.
  assert.ok(!has('p', 'x1'));
  assert.equal((await c.run(`SELECT 1 FROM _zebridge_outbox WHERE msg_id = 'b1'`)).length, 1);
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


test('undo: an accepted middle write survives surrounding refusals and restart (verdict or echo)', async () => {
  for (const restart of [false, true]) for (const reverse of [false, true]) for (const echo of [false, true]) {
    const dir = mkdtempSync(join(tmpdir(), 'zb-middle-'));
    const path = join(dir, 'r.sqlite3');
    let f = undoFixture(path);
    f.db.exec("INSERT INTO p VALUES ('x', 'original')");
    await f.write('a', 'p', 'UPDATE', 'x', 'title', 'A');
    f.db.exec("UPDATE p SET title = 'server' WHERE id = 'x'");
    await f.write('b', 'p', 'UPDATE', 'x', 'title', 'B');
    await f.write('c', 'p', 'UPDATE', 'x', 'title', 'C');
    f.c.wakeOutbox = () => {};
    if (echo) {
      f.c.outboxWrite = async () => ({table: 'p', version: 'v', key: {id: 'x'}, columns: ['title']});
      await f.c.settleByEcho('b', {table: 'p', version: 'v'});
    } else {
      // Through the verdict dispatch, as an `accepted` verdict arrives.
      Object.assign(f.c, {config: {principal: 'alice'}, ackPrefix: () => 'mutation_ack', pendingWrites: new Map(), emitVerdict: () => {}});
      await f.c.handleVerdict('mutation_ack.alice.b', new TextEncoder().encode('{"status":"accepted"}'));
    }
    if (restart) { f.db.close(); f = undoFixture(path); }
    for (const mid of reverse ? ['c', 'a'] : ['a', 'c']) await f.c.revertOptimisticWrite(mid, 'restore');
    assert.equal(f.title('x'), 'B', `restart=${restart}, reverse=${reverse}, echo=${echo}`);
    assert.equal(await f.c.undoing(), 0);
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM _zebridge_undo').get()!.n, 0, 'settled history is reclaimed');
    await new Promise((r) => setTimeout(r, 10)); f.db.close(); rmSync(dir, {recursive: true, force: true});
  }
});

test('undo: PGlite compares typed values and reopens with a blocked undo', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zb-pg-undo-'));
  const open = async () => {
    const storage = makePgliteStorage({persist: true, dataDir: dir})('replica');
    const c: any = Object.create(ZeBridge.prototype);
    Object.assign(c, {run: storage.exec, transaction: storage.transaction, dialect: storage.dialect,
      outboxInitPromise: Promise.resolve(), undoOwed: true, appendLog: () => {}, triggerChange: () => {},
      syncedTables: new Map([['p', {pkCols: ['id'], columns: ['id','title','updated_at','flag','amount','doc'],
        versionColumn: 'updated_at', tombstoneColumn: null}]])});
    await c.createOutboxTable();
    return {storage, c};
  };
  let f = await open();
  try {
    await f.storage.exec('CREATE TABLE p(id TEXT PRIMARY KEY,title TEXT,updated_at TIMESTAMPTZ,flag BOOLEAN,amount NUMERIC,doc JSONB)');
    await f.storage.exec('CREATE TABLE child(id TEXT PRIMARY KEY,pid TEXT REFERENCES p(id))');
    const before = {id:'x', title:'original', updated_at:'2026-11-09T00:00:00.000000Z', flag:false, amount:'1.00', doc:'{"a":1}'};
    const data = {id:'x', title:'refused', updated_at:'2026-11-09T00:00:01.000000Z', flag:true, amount:'2.00', doc:'{"a":2}'};
    await f.storage.exec('INSERT INTO p VALUES(?,?,?,?,?,?)', ...Object.values(data));
    await f.storage.exec("INSERT INTO child VALUES('child','x')");
    // A refused INSERT requires a DELETE, blocked by the child until after reopening.
    await f.storage.transaction(async tx => await f.c.outboxPut({msgId:'a', subject:'s', payload:{operation:'INSERT',key:{id:'x'},data},
      table:'p', id:'x', before:null, optimistic:{operation:'INSERT',data}, authoritative:true},tx));
    await f.c.revertOptimisticWrite('a','restore');
    assert.equal(await f.c.undoing(), 1);
    await f.storage.close!();
    f = await open();
    await f.storage.exec('DELETE FROM child');
    await f.c.runUndos();
    assert.equal((await f.storage.exec('SELECT * FROM p')).length, 0);
    assert.equal(await f.c.undoing(), 0);
    // A refused UPDATE restores its typed before-image rather than skipping it.
    await f.storage.exec('INSERT INTO p VALUES(?,?,?,?,?,?)', ...Object.values(data));
    await f.storage.transaction(async tx => await f.c.outboxPut({msgId:'u', subject:'s', payload:{operation:'UPDATE',key:{id:'x'},data},
      table:'p', id:'x', before, optimistic:{operation:'UPDATE',data}, authoritative:true},tx));
    await f.c.revertOptimisticWrite('u','restore');
    assert.equal((await f.storage.exec('SELECT title FROM p'))[0].title,'original');
  } finally {
    await f.storage.close?.().catch(() => {});
    rmSync(dir, {recursive: true, force: true});
  }
});


test('undo: CASCADE waits for independent children, including grandchildren and reopen', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zb-cascade-'));
  const path = join(dir, 'r.sqlite3');
  let f = undoFixture(path);
  f.db.exec(`DROP TABLE c; CREATE TABLE c(id TEXT PRIMARY KEY,pid TEXT NOT NULL REFERENCES p(id) ON DELETE CASCADE);
    CREATE TABLE g(id TEXT PRIMARY KEY,pid TEXT NOT NULL REFERENCES c(id) ON DELETE CASCADE)`);
  const followGrandchild = () => f.c.syncedTables.set('g', {pkCols:['id'], columns:['id','pid'], tombstoneColumn:null});
  followGrandchild();
  try {
    await f.write('p', 'p', 'INSERT', 'p', 'title', 'refused');
    await f.write('c', 'c', 'INSERT', 'c', 'pid', 'p');
    await f.write('g', 'g' as any, 'INSERT', 'g', 'pid', 'c');
    await f.c.revertOptimisticWrite('p', 'restore');
    assert.ok(f.has('p','p') && f.has('c','c') && f.has('g','g'), 'parent refusal must not cancel or cascade-delete independent descendants');
    assert.equal(await f.c.undoing(), 1);
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM _zebridge_outbox').get()!.n, 2);
    await new Promise(r=>setTimeout(r,10)); f.db.close();
    f=undoFixture(path); followGrandchild();
    await f.c.runUndos();
    assert.ok(f.has('c','c') && f.has('g','g'));
    await f.c.revertOptimisticWrite('c','restore');
    assert.ok(f.has('p','p') && f.has('c','c') && f.has('g','g'));
    assert.equal(await f.c.undoing(),2);
    await f.c.revertOptimisticWrite('g','restore');
    assert.ok(!f.has('p','p') && !f.has('c','c') && !f.has('g','g'));
    assert.equal(await f.c.undoing(),0);
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM _zebridge_outbox').get()!.n,0);
    assert.equal(f.db.prepare('PRAGMA foreign_key_check').all().length,0);
  } finally { await new Promise(r=>setTimeout(r,10)); f.db.close(); rmSync(dir,{recursive:true,force:true}); }
});

test('undo: a temporary parent rebuild must not cascade; an unfollowed child is protected too', async () => {
  const f=undoFixture();
  f.db.exec('DROP TABLE c; CREATE TABLE c(id TEXT PRIMARY KEY,pid TEXT REFERENCES p(id) ON DELETE CASCADE)');
  try {
    await f.write('a','p','INSERT','p','title','A');
    await f.write('b','p','UPDATE','p','title','B');
    await f.write('c','c','INSERT','c','pid','p');
    // A's rebuild deletes and re-inserts p, which used to lose c even though p survived.
    await f.c.revertOptimisticWrite('a','restore');
    assert.equal(f.title('p'),'B'); assert.ok(f.has('c','c')); assert.equal(await f.c.undoing(),1);
    // This child is not followed: safety must use the physical schema, not syncedTables.
    f.c.syncedTables.delete('c');
    await f.c.revertOptimisticWrite('b','restore');
    assert.ok(f.has('p','p') && f.has('c','c')); assert.equal(await f.c.undoing(),2);
    f.db.exec('DELETE FROM c'); // authoritative removal, no dependent verdict
    f.c.triggerChange('c'); await new Promise(r=>setTimeout(r,20));
    assert.ok(!f.has('p','p')); assert.equal(await f.c.undoing(),0);
  } finally { await new Promise(r=>setTimeout(r,10)); f.db.close(); }
});

test('undo: CASCADE composite implicit keys, PGlite and self references', async () => {
  for (const postgres of [false,true]) {
    const sqlite=postgres?null:new DatabaseSync(':memory:');
    const storage=postgres?makePgliteStorage()('zb-cascade-pg'):null;
    const exec=storage?.exec ?? (async(q:string,...p:any[])=>{const st=sqlite!.prepare(q);return st.columns().length?st.all(...p):(st.run(...p),[])});
    const transaction=storage?.transaction ?? (async(fn:any)=>{sqlite!.exec('BEGIN');try{await fn(exec);sqlite!.exec('COMMIT')}catch(e){sqlite!.exec('ROLLBACK');throw e}});
    const c:any=Object.create(ZeBridge.prototype);
    Object.assign(c,{run:exec,transaction,dialect:storage?.dialect??sqliteDialect,outboxInitPromise:Promise.resolve(),undoOwed:true,appendLog:()=>{},triggerChange:()=>{},
      syncedTables:new Map([['p',{pkCols:['region','id'],columns:['region','id','title'],tombstoneColumn:null}]])});
    try {
      if(sqlite) sqlite.exec('PRAGMA foreign_keys=ON');
      await c.createOutboxTable();
      await exec('CREATE TABLE p(region TEXT,id TEXT,title TEXT,PRIMARY KEY(region,id))');
      // Omitting parent columns tests primary-key resolution; the child is deliberately unfollowed.
      await exec('CREATE TABLE child(id TEXT PRIMARY KEY,region TEXT,pid TEXT,FOREIGN KEY(region,pid) REFERENCES p ON DELETE CASCADE)');
      await exec("INSERT INTO p VALUES('other','p','other'),('own','p','refused')");
      await exec("INSERT INTO child VALUES('other','other','p'),('own','own','p')");
      const key={region:'own',id:'p'},data={...key,title:'refused'};
      await transaction(async(tx:any)=>await c.outboxPut({msgId:'p',subject:'s',payload:{operation:'INSERT',key,data},table:'p',id:'own|p',before:null,optimistic:{operation:'INSERT',data},authoritative:true},tx));
      await c.revertOptimisticWrite('p','restore');
      assert.equal(await c.undoing(),1,`composite blocker postgres=${postgres}`);
      assert.equal((await exec('SELECT * FROM child')).length,2);
      if (postgres) {
        // A cascade runs with RI privileges: a filtered SELECT must not prove the child absent.
        await exec('CREATE ROLE undo_cascade_hidden NOSUPERUSER');
        await exec('GRANT USAGE ON SCHEMA public TO undo_cascade_hidden');
        await exec('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO undo_cascade_hidden');
        await exec('ALTER TABLE child ENABLE ROW LEVEL SECURITY');
        await exec('CREATE POLICY hidden ON child USING (false)');
        await exec('SET ROLE undo_cascade_hidden');
        assert.equal((await exec('SELECT * FROM child')).length,0);
        await c.runUndos();
        assert.equal((await exec("SELECT * FROM p WHERE region='own'")).length,1,'hidden children are unknown, not absent');
        assert.equal(await c.undoing(),1);
        await exec('RESET ROLE');
        assert.equal((await exec('SELECT * FROM child')).length,2);
      }
      await exec("DELETE FROM child WHERE id='own'"); await c.runUndos();
      assert.equal((await exec("SELECT * FROM p WHERE region='own'")).length,0);
      assert.equal((await exec('SELECT * FROM child')).length,1,'an unrelated same-id parent must not block or be removed');
      assert.equal(await c.undoing(),0);
      await exec('DROP TABLE child'); await exec('DROP TABLE p');
      await exec('CREATE TABLE p(id TEXT PRIMARY KEY,title TEXT,pid TEXT REFERENCES p(id) ON DELETE CASCADE)');
      c.syncedTables.set('p',{pkCols:['id'],columns:['id','title','pid'],tombstoneColumn:null});
      await exec("INSERT INTO p VALUES('self','refused','self')");
      const self={id:'self',title:'refused',pid:'self'};
      await transaction(async(tx:any)=>await c.outboxPut({msgId:'self',subject:'s',payload:{operation:'INSERT',key:{id:'self'},data:self},table:'p',id:'self',before:null,optimistic:{operation:'INSERT',data:self},authoritative:true},tx));
      await c.revertOptimisticWrite('self','restore');
      assert.equal((await exec('SELECT * FROM p')).length,0,'self reference is the same row, not collateral damage');
      assert.equal(await c.undoing(),0);
    } finally { if(storage) await storage.close?.(); else sqlite!.close(); }
  }
});

test('undo: accepted history goes when the row\'s last write settles otherwise', async () => {
  const f = undoFixture();
  f.c.wakeOutbox = () => {};
  try {
    f.db.exec("INSERT INTO p VALUES ('x', 'original')");
    await f.write('a', 'p', 'UPDATE', 'x', 'title', 'A');
    await f.write('b', 'p', 'UPDATE', 'x', 'title', 'B');
    await f.write('c', 'p', 'UPDATE', 'x', 'title', 'C');
    await f.c.outboxDrop('b', true);
    const kept = () => f.db.prepare('SELECT count(*) AS n FROM _zebridge_undo').get()!.n;
    assert.equal(kept(), 1);
    await f.c.outboxDrop('a'); // stale, indeterminate…: settled without history of its own
    assert.equal(kept(), 1, 'c is still pending on the row');
    await f.c.outboxDrop('c');
    assert.equal(kept(), 0, 'nothing left on the row: its history goes');
    assert.equal(f.title('x'), 'C');
  } finally { f.db.close(); }
});

test('undo: a refused INSERT\'s rebuild does not replay a pending sparse UPDATE onto the absent row (NOT NULL column)', async () => {
  // dependent_writes.py §5: the replay's upsert of {id, title} failed NOT NULL.
  const f = undoFixture();
  f.db.exec('DROP TABLE c; DROP TABLE p; CREATE TABLE p (id TEXT PRIMARY KEY, title TEXT, tenant TEXT NOT NULL)');
  await f.write('i', 'p', 'INSERT', 'x', 'tenant', 't1');
  await f.write('u', 'p', 'UPDATE', 'x', 'title', 'edited');
  await f.c.revertOptimisticWrite('i', 'restore');
  assert.equal(await f.c.undoing(), 0);
  assert.ok(!f.has('p', 'x'));
  const pend: any[] = await f.c.run(`SELECT before, authoritative FROM _zebridge_outbox WHERE msg_id = 'u'`);
  assert.deepEqual(pend.map((r) => [r.before, Number(r.authoritative)]), [[null, 1]]); // left for its own verdict
});

test('undo: a rebuild that replays a pending DELETE waits for a CASCADE child, then settles', async () => {
  const f = undoFixture();
  f.db.exec('DROP TABLE c; CREATE TABLE c(id TEXT PRIMARY KEY,pid TEXT REFERENCES p(id) ON DELETE CASCADE)');
  try {
    f.db.exec("INSERT INTO p VALUES ('x', 'original')");
    await f.write('a', 'p', 'UPDATE', 'x', 'title', 'A');
    await f.write('d', 'p', 'DELETE', 'x', 'title', 'A');
    // A child received with the keys off (a seed's bulk load): the parent is absent locally
    // (the pending DELETE), and the rebuild brings it back before replaying that DELETE.
    f.db.exec("PRAGMA foreign_keys = OFF; INSERT INTO c VALUES ('kid', 'x'); PRAGMA foreign_keys = ON;");
    await f.c.revertOptimisticWrite('a', 'restore');
    assert.ok(f.has('c', 'kid'), 'the replayed DELETE must not cascade into the child');
    assert.equal(await f.c.undoing(), 1);
    f.db.exec("DELETE FROM c WHERE id = 'kid'"); // CDC
    await f.c.runUndos();
    assert.equal(await f.c.undoing(), 0);
    assert.ok(!f.has('p', 'x'), 'the pending DELETE still shows');
  } finally { await new Promise((r) => setTimeout(r, 10)); f.db.close(); }
});

test('undo: history left by an indeterminate write goes at the next undo pass', async () => {
  const f = undoFixture();
  f.c.wakeOutbox = () => {};
  try {
    f.db.exec("INSERT INTO p VALUES ('x', 'original')");
    await f.write('a', 'p', 'UPDATE', 'x', 'title', 'A');
    await f.write('b', 'p', 'UPDATE', 'x', 'title', 'B');
    await f.c.outboxDrop('b', true);
    f.db.exec(`CREATE TABLE IF NOT EXISTS _zebridge_reconcile (msg_id TEXT PRIMARY KEY, tbl TEXT, reason TEXT, version TEXT, data TEXT, since INTEGER, state TEXT)`);
    assert.equal(await f.c.settleIndeterminate('a', 'x', null), true);
    await f.c.runUndos();
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM _zebridge_undo').get()!.n, 0);
  } finally { f.db.close(); }
});

test('undo: PGlite compares an array column in the engine\'s form', async () => {
  const storage = makePgliteStorage()('zb-undo-array');
  const c: any = Object.create(ZeBridge.prototype);
  Object.assign(c, {run: storage.exec, transaction: storage.transaction, dialect: storage.dialect, outboxInitPromise: Promise.resolve(),
    undoOwed: true, appendLog: () => {}, triggerChange: () => {},
    applyEvent: async (_t: string, ev: any, exec: any) => { await exec('UPDATE p SET tags = ? WHERE id = ?', `{${ev.data.tags.join(',')}}`, ev.data.id); },
    syncedTables: new Map([['p', {pkCols: ['id'], columns: ['id', 'tags'], tombstoneColumn: null}]])});
  try {
    await c.createOutboxTable();
    await storage.exec('CREATE TABLE p(id TEXT PRIMARY KEY, tags TEXT[])');
    await storage.exec("INSERT INTO p VALUES ('x', '{refused,edit}')");
    const data = {id: 'x', tags: ['refused', 'edit']};
    await storage.transaction(async (tx) => await c.outboxPut({msgId: 'u', subject: 's', payload: {operation: 'UPDATE', key: {id: 'x'}, data},
      table: 'p', id: 'x', before: {id: 'x', tags: '{original}'}, optimistic: {operation: 'UPDATE', data}, authoritative: true}, tx));
    await c.revertOptimisticWrite('u', 'restore');
    assert.deepEqual((await storage.exec('SELECT tags FROM p'))[0].tags, ['original'], 'an own row with an array column is still recognised as own');
    assert.equal(await c.undoing(), 0);
  } finally { await storage.close?.(); }
});


test('undo: accepted middle UPDATE survives an absent baseline and restart in both refusal orders', async () => {
  for (const restart of [false,true]) for (const reverse of [false,true]) for (const echo of [false,true]) {
    const dir=mkdtempSync(join(tmpdir(),'zb-absent-middle-')), path=join(dir,'r.sqlite3');
    let f=undoFixture(path); f.c.wakeOutbox=()=>{};
    try {
      await f.write('a','p','INSERT','x','title','A');
      await f.write('b','p','UPDATE','x','title','B');
      await f.write('c','p','UPDATE','x','title','C');
      if (echo) {
        f.c.outboxWrite=async()=>({table:'p',version:'v',key:{id:'x'},columns:['title']});
        await f.c.settleByEcho('b',{table:'p',version:'v'});
      } else await f.c.outboxDrop('b',true);
      if(restart) { await new Promise(r=>setTimeout(r,10)); f.db.close(); f=undoFixture(path); }
      for(const mid of reverse?['c','a']:['a','c']) await f.c.revertOptimisticWrite(mid,'restore');
      assert.equal(f.title('x'),'B',`absent base restart=${restart}, reverse=${reverse}, echo=${echo}`);
      assert.equal(await f.c.undoing(),0);
      assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_outbox').get()!.n,0);
      assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_undo').get()!.n,0);
    } finally { await new Promise(r=>setTimeout(r,10)); f.db.close(); rmSync(dir,{recursive:true,force:true}); }
  }
});

test('undo: sparse accepted history keeps an obligation across restart until an authoritative seed', async () => {
  for(const required of [false,true]) {
    const dir=mkdtempSync(join(tmpdir(),'zb-absent-sparse-')), path=join(dir,'r.sqlite3');
    let f=undoFixture(path); f.c.wakeOutbox=()=>{};
    try {
      f.db.exec(`DROP TABLE c; DROP TABLE p; CREATE TABLE p(id TEXT PRIMARY KEY,title TEXT,tenant TEXT ${required?'NOT NULL':''})`);
      await f.write('a','p','INSERT','x','tenant','local');
      await f.write('b','p','UPDATE','x','title','B');
      await f.write('c','p','UPDATE','x','title','C');
      await f.c.outboxDrop('b',true);
      await f.c.revertOptimisticWrite('a','restore');
      assert.equal(await f.c.undoing(),1,`missing column required=${required}: do not discard accepted history`);
      assert.equal(f.title('x'),'C','an unsuccessful rebuild is rolled back');
      assert.equal(f.db.prepare("SELECT count(*) n FROM _zebridge_undo WHERE mode='accepted'").get()!.n,1);
      await new Promise(r=>setTimeout(r,10)); f.db.close(); f=undoFixture(path);
      await f.c.runUndos();
      assert.equal(await f.c.undoing(),1);
      // A full now supplies B's actual row, including the column the sparse UPDATE never carried.
      f.db.prepare("UPDATE p SET title='server B',tenant='server' WHERE id='x'").run();
      await f.c.reapplyPending('p');
      assert.equal(f.title('x'),'C');
      await f.c.revertOptimisticWrite('c','restore');
      assert.deepEqual({...f.db.prepare("SELECT * FROM p WHERE id='x'").get()}, {id:'x',title:'server B',tenant:'server'});
      assert.equal(await f.c.undoing(),0);
      assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_outbox').get()!.n,0);
      assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_undo').get()!.n,0);
    } finally { await new Promise(r=>setTimeout(r,10)); f.db.close(); rmSync(dir,{recursive:true,force:true}); }
  }
});

test('undo: PGlite accepted UPDATE from an absent base preserves a complete row or keeps recovery owed', async () => {
  for(const partial of [false,true]) {
    const storage=makePgliteStorage()('zb-absent-accepted-pg');
    const c:any=Object.create(ZeBridge.prototype);
    Object.assign(c,{run:storage.exec,transaction:storage.transaction,dialect:storage.dialect,outboxInitPromise:Promise.resolve(),
      undoOwed:true,wakeOutbox:()=>{},triggerChange:()=>{},appendLog:()=>{},
      syncedTables:new Map([['p',{pkCols:['id'],columns:partial?['id','title','tenant']:['id','title'],tombstoneColumn:null}]]),
      applyEvent:async(_t:string,ev:any,exec:any)=>{
        const cols=Object.keys(ev.data);
        if(ev.operation==='UPDATE'&&(await exec('SELECT 1 FROM p WHERE id=?',ev.data.id)).length) {
          await exec(`UPDATE p SET ${cols.map(k=>`"${k}"=?`).join(',')} WHERE id=?`,...cols.map(k=>ev.data[k]),ev.data.id);
        } else await exec(`INSERT INTO p(${cols.join(',')}) VALUES(${cols.map(()=>'?').join(',')}) ON CONFLICT(id) DO UPDATE SET ${cols.map(k=>`"${k}"=excluded."${k}"`).join(',')}`,...cols.map(k=>ev.data[k]));
      }});
    try {
      await c.createOutboxTable();
      await storage.exec(`CREATE TABLE p(id TEXT PRIMARY KEY,title TEXT${partial?',tenant TEXT':''})`);
      for(const [id,op,title] of [['a','INSERT','A'],['b','UPDATE','B'],['c','UPDATE','C']]) {
        await storage.transaction(async tx=>{
          const before=(await tx("SELECT * FROM p WHERE id='x'"))[0]??null;
          const authoritative=await c.beforeIsAuthoritative('p',c.syncedTables.get('p'),'x',tx);
          const data=partial&&op==='INSERT'?{id:'x',title,tenant:'local'}:{id:'x',title};
          const ev={operation:op,data};
          await c.outboxPut({msgId:id,subject:'s',payload:{operation:op,key:{id:'x'},data:{title}},table:'p',id:'x',before,optimistic:ev,authoritative},tx);
          await c.applyEvent('p',ev,tx);
        });
      }
      await c.outboxDrop('b',true); await c.revertOptimisticWrite('a','restore');
      if(partial) {
        assert.equal(await c.undoing(),1,'missing nullable data is not supplied by a default');
        assert.equal((await storage.exec('SELECT title FROM p'))[0].title,'C');
        await storage.exec("UPDATE p SET title='server B',tenant='server'");
        await c.reapplyPending('p');
      }
      await c.revertOptimisticWrite('c','restore');
      assert.equal((await storage.exec('SELECT title FROM p'))[0].title,partial?'server B':'B');
      assert.equal(await c.undoing(),0);
      assert.equal(Number((await storage.exec('SELECT count(*) n FROM _zebridge_outbox'))[0].n),0);
    } finally { await storage.close?.(); }
  }
});

test('undo: the confirmed baseline is the ledger\'s authoritative before-image, across overlays and reopen', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zb-confirmed-base-'));
  const path = join(dir, 'replica.sqlite3');
  let f = undoFixture(path); f.c.wakeOutbox = () => {};
  try {
    f.db.exec("INSERT INTO p VALUES('x','server')");
    await f.write('a', 'p', 'UPDATE', 'x', 'title', 'A');
    await f.write('b', 'p', 'UPDATE', 'x', 'title', 'B');
    f.db.exec("UPDATE p SET title='new server' WHERE id='x'");
    await f.write('c', 'p', 'UPDATE', 'x', 'title', 'C');
    // Lazy: no copy is written for a write; c's authoritative before-image holds "new server".
    assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_base').get()!.n, 0);
    await new Promise(r => setTimeout(r, 10)); f.db.close(); f = undoFixture(path);
    for (const id of ['a', 'b', 'c']) await f.c.revertOptimisticWrite(id, 'restore');
    assert.equal(f.title('x'), 'new server');
    await f.c.runUndos();
    assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_base').get()!.n, 0);
  } finally { await new Promise(r => setTimeout(r, 10)); f.db.close(); rmSync(dir, {recursive: true, force: true}); }
});

test('undo: an UPDATE omitting a column merges the trusted server copy; explicit NULL overwrites; no trusted copy recovers', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zb-toast-merge-'));
  const path = join(dir, 'replica.sqlite3');
  let f = undoFixture(path); f.c.wakeOutbox = () => {};
  const spec = { pkCols: ['id'], columns: ['id', 'title', 'body'], tombstoneColumn: null };
  const base = (id: string) => { const r: any = f.db.prepare('SELECT data FROM _zebridge_base WHERE row_id=?').get(id); return r ? JSON.parse(r.data) : null; };
  try {
    f.db.exec("DROP TABLE c; DROP TABLE p; CREATE TABLE p(id TEXT PRIMARY KEY,title TEXT,body TEXT); INSERT INTO p VALUES('x','server','big')");
    f.c.syncedTables.set('p', spec);
    await f.write('a', 'p', 'UPDATE', 'x', 'title', 'mine');
    assert.equal(base('x'), null, 'lazy: no copy for a write');
    // Another writer's UPDATE of the title, `body` unchanged (omitted by pgoutput), across a restart.
    await new Promise(r => setTimeout(r, 10)); f.db.close(); f = undoFixture(path); f.c.wakeOutbox = () => {};
    f.c.syncedTables.set('p', spec);
    await f.c.recordAuthoritative(f.c.run, 'p', spec, { operation: 'UPDATE', data: { id: 'x', title: 'theirs' } });
    f.db.exec("UPDATE p SET title='theirs' WHERE id='x'"); // what the CDC apply itself writes
    assert.deepEqual(base('x'), { id: 'x', title: 'theirs', body: 'big' });
    await f.c.recordAuthoritative(f.c.run, 'p', spec, { operation: 'UPDATE', data: { id: 'x', title: null } });
    f.db.exec("UPDATE p SET title=NULL WHERE id='x'");
    assert.deepEqual(base('x'), { id: 'x', title: null, body: 'big' }, 'explicit NULL overwrites, omission preserves');
    assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_reconcile').get()!.n, 0);
    await f.c.revertOptimisticWrite('a', 'restore');
    assert.deepEqual({ ...f.db.prepare("SELECT * FROM p WHERE id='x'").get() }, { id: 'x', title: null, body: 'big' });
    // No trusted copy: a key whose only write is not authoritative (its before is an overlay).
    await f.write('y1', 'p', 'INSERT', 'y', 'title', 'one');
    await f.write('y2', 'p', 'UPDATE', 'y', 'title', 'two');
    f.db.exec("DELETE FROM _zebridge_outbox WHERE msg_id='y1'");
    await f.c.recordAuthoritative(f.c.run, 'p', spec, { operation: 'UPDATE', data: { id: 'y', title: 's' } });
    assert.equal(f.db.prepare("SELECT reason FROM _zebridge_reconcile").get()!.reason, 'UndoNeedsBaseline');
  } finally { await new Promise(r => setTimeout(r, 10)); f.db.close(); rmSync(dir, {recursive: true, force: true}); }
});

test('undo: no base copy for a write\'s own echo or for a key with no writes', async () => {
  const f = undoFixture();
  const spec = { pkCols: ['id'], columns: ['id', 'title', 'v', 'origin'], tombstoneColumn: null, versionColumn: 'v', tiebreakColumn: 'origin' };
  f.db.prepare("INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, created_at, authoritative) VALUES ('m','s',?,'p','x',1,1)")
    .run(JSON.stringify({ operation: 'INSERT', key: { id: 'x' }, data: { title: 't' }, version: '2026-11-09T10:00:00.000000Z', client_id: 'deviceA' }));
  await f.c.recordAuthoritative(f.c.run, 'p', spec, { operation: 'INSERT', data: { id: 'x', title: 't', v: '2026-11-09T10:00:00.000000Z', origin: 'deviceA' } });
  await f.c.recordAuthoritative(f.c.run, 'p', spec, { operation: 'INSERT', data: { id: 'z', title: 't', v: '2026-11-09T10:00:00.000000Z', origin: 'deviceA' } });
  assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_base').get()!.n, 0);
  // Someone else's row on the same key (another stamp) is kept.
  await f.c.recordAuthoritative(f.c.run, 'p', spec, { operation: 'UPDATE', data: { id: 'x', title: 'o', v: '2026-11-09T11:00:00.000000Z', origin: 'deviceB' } });
  assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_base').get()!.n, 1);
  f.db.close();
});

test('undo: missing accepted baseline requests durable recovery after restart without another write', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zb-baseline-recovery-'));
  const path = join(dir, 'replica.sqlite3');
  let f = undoFixture(path); f.c.wakeOutbox = () => {};
  try {
    f.db.exec('DROP TABLE c; DROP TABLE p; CREATE TABLE p(id TEXT PRIMARY KEY,title TEXT,tenant TEXT NOT NULL)');
    await f.write('a','p','INSERT','x','tenant','local');
    await f.write('b','p','UPDATE','x','title','B');
    await f.write('c','p','UPDATE','x','title','C');
    await f.c.outboxDrop('b', true);
    await f.c.revertOptimisticWrite('a','restore');
    await f.c.revertOptimisticWrite('c','restore');
    assert.equal(await f.c.pending(), 0);
    const marker = f.db.prepare('SELECT msg_id,reason,state FROM _zebridge_reconcile').get()!;
    assert.equal(marker.reason, 'UndoNeedsBaseline');
    assert.equal(marker.state, 'pending');
    assert.equal(await f.c.undoing(), 2);
    await new Promise(r => setTimeout(r,10)); f.db.close(); f = undoFixture(path);
    assert.deepEqual(f.db.prepare('SELECT msg_id,reason,state FROM _zebridge_reconcile').get(), marker);
    // Connected recovery is scheduled by the owed undo itself, with no new CDC or
    // verdict. The actual fenced orchestration and network are covered live in §7.
    let requested = 0;
    f.c.nc = {};
    f.c.reconcile = async () => { requested++; };
    await f.c.runUndos();
    assert.equal(requested, 1);
    assert.equal(await f.c.pending(), 0);
    assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_reconcile').get()!.n, 1);
    f.c.nc = null;
  } finally { await new Promise(r => setTimeout(r,10)); f.db.close(); rmSync(dir, {recursive:true,force:true}); }
});

test('undo: authoritative CDC matching an overlay survives refusals and restart on single and bulk paths', async () => {
  for (const bulk of [false, true]) {
    const dir = mkdtempSync(join(tmpdir(), 'zb-cdc-baseline-'));
    const path = join(dir,'replica.sqlite3');
    let f = undoFixture(path); f.c.wakeOutbox = () => {};
    try {
      f.db.exec("INSERT INTO p VALUES('x','server')");
      await f.write('a','p','UPDATE','x','title','A');
      await f.write('b','p','UPDATE','x','title','B');
      await f.write('c','p','UPDATE','x','title','C');
      await f.c.outboxDrop('b',true);
      Object.assign(f.c,{ dialect:sqliteDialect, frontiers:new Map(), rebase:new Map(), config:{bulkStatement:'json_each'},
        bulkStats:{single:0,bulked:0,statements:0,fallbacks:0}, advanceGlobal:async()=>{}, confirmEcho:()=>{} });
      const ev = {operation:'UPDATE', data:{id:'x',title:'C'}};
      if (bulk) {
        await f.c.applyBatchPlanned([{table:'p',ev}, {table:'p',ev:{operation:'INSERT',data:{id:'y',title:'other'}}}],f.c.run);
        assert.ok(f.c.bulkStats.bulked > 0, 'exercise the actual bulk path');
      } else await (ZeBridge.prototype as any).applyEvent.call(f.c,'p',ev,f.c.run);
      assert.equal(JSON.parse(f.db.prepare('SELECT data FROM _zebridge_base WHERE row_id=?').get('x')!.data as string).title,'C');
      const savedAt = Number(f.db.prepare('SELECT at FROM _zebridge_base WHERE row_id=?').get('x')!.at);
      await new Promise(r=>setTimeout(r,10)); f.db.close(); f=undoFixture(path);
      await f.c.createOutboxTable();
      assert.ok(f.c.lastCreated >= savedAt, 'a restart retains the ordering floor of confirmed CDC');
      await f.c.revertOptimisticWrite('a','restore');
      await f.c.revertOptimisticWrite('c','restore');
      assert.equal(f.title('x'),'C',`confirmed CDC survives the identical overlay: bulk=${bulk}`);
      assert.equal(await f.c.undoing(),0);
      assert.equal(await f.c.pending(),0);
      await f.write('d','p','UPDATE','x','title','D');
      // A row omitting a column (an unchanged TOAST value) takes it from the trusted server
      // copy (d's authoritative before-image), never from the displayed overlay "D".
      await f.c.recordAuthoritative(f.c.run,'p',f.c.syncedTables.get('p'),{operation:'UPDATE',data:{id:'x'}});
      assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_reconcile').get()!.n,0);
      assert.equal(JSON.parse(f.db.prepare('SELECT data FROM _zebridge_base WHERE row_id=?').get('x')!.data as string).title,'C','merged from the trusted copy, not the overlay');
    } finally { await new Promise(r=>setTimeout(r,10)); f.db.close(); rmSync(dir,{recursive:true,force:true}); }
  }
});

test('undo: stale empty-ledger refresh cannot disable tracking for a newly committed write', async () => {
 const f=undoFixture(); f.c.wakeOutbox=()=>{}; f.c.pendingWrites=new Map();
 await f.c.createOutboxTable();
 let release!:()=>void, entered!:()=>void;
 const gate=new Promise<void>(r=>release=r), arrived=new Promise<void>(r=>entered=r);
 const run=f.c.run;
 f.c.run=async(sql:string,...args:any[])=>{
   const rows=await run(sql,...args);
   if(sql.startsWith('SELECT 1 AS x WHERE EXISTS')) {entered();await gate;}
   return rows;
 };
 try {
   await f.write('old','p','INSERT','old','title','old');
   const dropping=f.c.outboxDrop('old',true);
   await arrived;
   await f.c.transaction(async (tx:any)=> {
     await f.c.outboxPut({msgId:'new',subject:'s',payload:{key:{id:'new'},data:{title:'new'}},table:'p',id:'new',before:null,optimistic:{operation:'INSERT',data:{id:'new',title:'new'}},authoritative:true},tx);
     await f.c.applyEvent('p',{operation:'INSERT',data:{id:'new',title:'new'}},tx);
   });
   assert.equal(f.c.ledgerActive,true);
   release(); await dropping;
   assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_outbox').get()!.n,1);
   const tracking=f.c.ledgerActive;
   const event={operation:'INSERT',data:{id:'new',title:'new'}};
   await f.c.recordAuthoritative(f.c.run,'p',f.c.syncedTables.get('p'),event);
   await f.c.applyEvent('p',event,f.c.run);
   await f.c.revertOptimisticWrite('new','restore');
   assert.equal(f.has('p','new'),true,`new authoritative row must survive rejection; ledgerActive was ${tracking}`);
 } finally {release();f.db.close();}
});


test('undo: equal-stamp peer CDC survives a rejection and restart; owed entries never count as pending echoes', async () => {
  for (const restart of [false,true]) for (const mode of ['pending','owed','unknown-writer']) {
    const dir=mkdtempSync(join(tmpdir(),'zb-writer-proof-')), path=join(dir,'r.sqlite3');
    let f=undoFixture(path); f.c.wakeOutbox=()=>{};
    const version='2026-11-09T10:00:00.000000Z';
    const spec={pkCols:['id'],columns:['id','title','v','origin','body'],tombstoneColumn:null,versionColumn:'v',tiebreakColumn:mode==='unknown-writer'?null:'origin'};
    try {
      f.db.exec("DROP TABLE c; DROP TABLE p; CREATE TABLE p(id TEXT PRIMARY KEY,title TEXT,v TEXT,origin TEXT,body TEXT); INSERT INTO p VALUES('x','original','older','old writer','old body')");
      f.c.syncedTables.set('p',spec);
      await f.write('a','p','UPDATE','x','title','mine');
      f.db.prepare('UPDATE _zebridge_outbox SET payload=?,optimistic=? WHERE msg_id=?').run(
        JSON.stringify({operation:'UPDATE',key:{id:'x'},data:{title:'mine'},version,client_id:'deviceA'}),
        JSON.stringify({operation:'UPDATE',data:{id:'x',title:'mine',v:version},optimistic:true}), 'a');
      f.db.prepare("UPDATE p SET v=? WHERE id='x'").run(version);
      if(mode==='owed') f.db.exec("INSERT INTO _zebridge_undo(msg_id,tbl,row_id,payload,before,optimistic,created_at,authoritative,mode,since) SELECT msg_id,tbl,row_id,payload,before,optimistic,created_at,authoritative,'restore',0 FROM _zebridge_outbox; DELETE FROM _zebridge_outbox");
      const origin=mode==='owed'?'deviceA':'deviceB';
      await f.c.recordAuthoritative(f.c.run,'p',spec,{operation:'UPDATE',data:{id:'x',title:'mine',v:version,origin,body:'peer body'}});
      f.db.prepare("UPDATE p SET body='peer body',origin=? WHERE id='x'").run(origin);
      assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_base').get()!.n,1,mode);
      if(restart) {await new Promise(r=>setTimeout(r,10));f.db.close();f=undoFixture(path);f.c.syncedTables.set('p',spec);}
      if(mode==='owed') await f.c.runUndos(); else await f.c.revertOptimisticWrite('a','restore');
      assert.equal(f.db.prepare("SELECT body FROM p WHERE id='x'").get()!.body,'peer body',`${mode}, restart=${restart}`);
      assert.equal(await f.c.undoing(),0);
    } finally {await new Promise(r=>setTimeout(r,10));f.db.close();rmSync(dir,{recursive:true,force:true});}
  }
});

test('undo: verified pending echoes skip copies with either PostgreSQL or wire timestamp spelling', async () => {
  for(const stamp of ['2026-11-09T10:00:00.000000Z','2026-11-09 10:00:00+00']) {
    const f=undoFixture();
    try {
      const spec={pkCols:['id'],columns:['id','title','v','origin'],tombstoneColumn:null,versionColumn:'v',tiebreakColumn:'origin'};
      f.c.ledgerActive=true;
      f.db.prepare("INSERT INTO _zebridge_outbox(msg_id,subject,payload,tbl,row_id,created_at,authoritative) VALUES('m','s',?,'p','x',1,1)")
        .run(JSON.stringify({key:{id:'x'},data:{title:'mine'},version:'2026-11-09T10:00:00.000000Z',client_id:'deviceA'}));
      await f.c.recordAuthoritative(f.c.run,'p',spec,{operation:'UPDATE',data:{id:'x',title:'mine',v:stamp,origin:'deviceA'}});
      assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_base').get()!.n,0,stamp);
    } finally {f.db.close();}
  }
});

test('undo: an insert completing after an empty refresh re-enables tracking', async () => {
  const f=undoFixture(); f.c.wakeOutbox=()=>{};
  let release!:()=>void, arrived!:()=>void;
  const gate=new Promise<void>(r=>release=r), waiting=new Promise<void>(r=>arrived=r);
  try {
    await f.c.createOutboxTable();
    const run=f.c.run;
    f.c.run=async(sql:string,...args:any[])=>{
      if(sql.startsWith('INSERT INTO _zebridge_outbox')) {arrived();await gate;}
      return run(sql,...args);
    };
    const putting=f.c.transaction((tx:any)=>f.c.outboxPut({msgId:'new',subject:'s',payload:{key:{id:'x'}},table:'p',id:'x',before:null,optimistic:{operation:'INSERT',data:{id:'x',title:'new'}},authoritative:true},tx));
    await waiting;
    await f.c.refreshLedgerActive();
    release(); await putting;
    assert.equal(f.c.ledgerActive,true);
    assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_outbox').get()!.n,1);
  } finally {release();f.db.close();}
});

test('undo: independent empty reads during and across commit cannot switch tracking off', async () => {
  for(const delayedRead of [false,true]) {
    const dir=mkdtempSync(join(tmpdir(),'zb-ledger-commit-')), path=join(dir,'r.sqlite3');
    const f=undoFixture(path); f.c.wakeOutbox=()=>{};
    f.db.exec('PRAGMA journal_mode=WAL');
    const reader=new DatabaseSync(path,{readOnly:true});
    let commit!:()=>void, inserted!:()=>void, readDone!:()=>void, deliver!:()=>void;
    const gate=new Promise<void>(r=>commit=r), waiting=new Promise<void>(r=>inserted=r);
    const readStarted=new Promise<void>(r=>readDone=r), readGate=new Promise<void>(r=>deliver=r);
    try {
      await f.c.createOutboxTable();
      const run=f.c.run;
      f.c.run=async(sql:string,...args:any[])=>{
        if(!sql.startsWith('SELECT 1 AS x WHERE EXISTS'))return run(sql,...args);
        const rows=reader.prepare(sql).all(...args);readDone();
        if(delayedRead)await readGate;
        return rows;
      };
      f.c.transaction=async(fn:any)=>{
        f.db.exec('BEGIN');
        try {await fn(run);inserted();await gate;f.db.exec('COMMIT');}
        catch(e){f.db.exec('ROLLBACK');throw e;}
      };
      const putting=f.c.ledgerTransaction((tx:any)=>f.c.outboxPut({msgId:'new',subject:'s',payload:{key:{id:'x'}},table:'p',id:'x',before:null,optimistic:{operation:'INSERT',data:{id:'x',title:'new'}},authoritative:true},tx));
      await waiting;
      assert.equal(reader.prepare('SELECT count(*) n FROM _zebridge_outbox').get()!.n,0,'the separate reader cannot see an uncommitted insert');
      const refreshing=f.c.refreshLedgerActive();await readStarted;
      if(delayedRead) {commit();await putting;deliver();await refreshing;}
      else {await refreshing;assert.equal(f.c.ledgerActive,true,'tracking remains conservative until commit');commit();await putting;}
      assert.equal(f.c.ledgerActive,true,`delayed read=${delayedRead}`);
      assert.equal(reader.prepare('SELECT count(*) n FROM _zebridge_outbox').get()!.n,1);
    } finally {commit();deliver();reader.close();f.db.close();rmSync(dir,{recursive:true,force:true});}
  }
});

// The CDC ledger filter (libzb's "CDC ledger filter …" tests). A probe is the ledger lookup in
// `recordAuthoritative`; a key the filter rules out must not reach SQL.
function probing(f: ReturnType<typeof undoFixture>) {
  const probes: string[] = [];
  const exec = async (sql: string, ...a: any[]) => {
    if (sql.includes('0 AS pending FROM _zebridge_undo')) probes.push(`${a[0]}/${a[1]}`);
    return f.c.run(sql, ...a);
  };
  const cdc = (t: string, id: string) => f.c.recordAuthoritative(exec, t, f.c.syncedTables.get(t), { operation: 'UPDATE', data: { id, title: 'peer' } });
  return { probes, cdc };
}

test('undo: the CDC ledger filter skips unrelated keys and captures outbox and undo overlaps across reopen', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zb-ledger-keys-')), path = join(dir, 'r.sqlite3');
  let f = undoFixture(path); f.c.wakeOutbox = () => {};
  try {
    await f.c.createOutboxTable();
    for (const id of ['pending', 'owed'])
      await f.c.outboxPut({ msgId: id, subject: 's', payload: { key: { id } }, table: 'p', id, before: null, optimistic: { operation: 'INSERT', data: { id, title: id } }, authoritative: true });
    // As `revertOptimisticWrite` moves it: the key goes with the row.
    f.db.exec("INSERT INTO _zebridge_undo (msg_id,tbl,row_id,payload,before,optimistic,created_at,authoritative,mode,since) SELECT msg_id,tbl,row_id,payload,before,optimistic,created_at,authoritative,'restore',1 FROM _zebridge_outbox WHERE msg_id='owed'; DELETE FROM _zebridge_outbox WHERE msg_id='owed'");
    assert.equal(f.c.ledgerKeys.mayContain('p', 'pending'), true);
    assert.equal(f.c.ledgerKeys.mayContain('p', 'owed'), true);
    f.db.close(); f = undoFixture(path); f.c.wakeOutbox = () => {};
    await f.c.createOutboxTable();
    const { probes, cdc } = probing(f);
    // The same row ID on another table is a miss too.
    for (const [t, id] of [['p', 'other'], ['c', 'owed'], ['p', 'owed'], ['p', 'pending']]) await cdc(t, id);
    assert.deepEqual(probes, ['p/owed', 'p/pending']);
    assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_base').get()!.n, 2);
  } finally { f.db.close(); rmSync(dir, { recursive: true, force: true }); }
});

test('undo: the CDC ledger filter never trusts a truncated startup scan', async () => {
  const f = undoFixture();
  try {
    await f.c.createOutboxTable();
    // Repeated writes to one key fill the startup scan before it reaches undo: the few
    // distinct keys of that prefix cannot certify a miss.
    f.db.prepare("WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i < ?) INSERT INTO _zebridge_outbox (msg_id,subject,payload,tbl,row_id,created_at) SELECT 'm'||i,'s','{}','p','repeat',i FROM n").run(1024);
    f.db.exec("INSERT INTO _zebridge_undo (msg_id,tbl,row_id,payload,created_at,mode,since) VALUES ('z','p','owed','{}',1,'restore',1)");
    await f.c.createOutboxTable();
    const { probes, cdc } = probing(f);
    await cdc('p', 'owed');
    assert.deepEqual(probes, ['p/owed']);
  } finally { f.db.close(); }
});

test('undo: the CDC ledger filter falls back to SQL at capacity and is trusted again once the ledger empties', async () => {
  const f = undoFixture();
  try {
    await f.c.createOutboxTable();
    const keys = f.c.ledgerKeys;
    assert.equal(keys.mayContain('p', 'unseen'), false);
    for (let i = 0; i <= 1024; i++) keys.include('p', String(i));
    assert.equal(keys.mayContain('p', 'unseen'), true);
    await f.c.refreshLedgerActive(); // the ledger is empty: tracking off, the set starts over
    assert.equal(f.c.ledgerActive, false);
    assert.equal(keys.mayContain('p', 'unseen'), false);
  } finally { f.db.close(); }
});

// The ordered envelope (libzb's "ordering: …" tests): a write names the newest unsettled ordered
// write, fixed when made and sent inside the payload; a `held` notice only delays the replay.
function orderFixture(path: string) {
  const f = undoFixture(path);
  try { f.db.exec('ALTER TABLE _zebridge_outbox ADD COLUMN basis TEXT'); } catch { /* reopened */ }
  Object.assign(f.c, {
    suspendedMap: new Map(), config: { principal: 'omar', grammar: { subjects: {} } }, clientIdValue: 'c-1',
    clockOffsetMs: () => 0, triggerChange: () => {}, pendingWrites: new Map(), nc: null,
  });
  f.c.syncedTables.get('p').ordering = 1;
  const mutate = async (t: 'p' | 'c', op: string, id: string, version: string) => {
    const values = t === 'p' ? { title: 't' } : { pid: 'x' };
    const before = new Set((f.db.prepare('SELECT msg_id FROM _zebridge_outbox').all() as any[]).map((r) => r.msg_id));
    await f.c.rawMutation(t, op, id, version, { key: { id }, version, client_id: 'c-1', ...(op === 'DELETE' ? {} : { data: values }) });
    const added = (f.db.prepare('SELECT msg_id FROM _zebridge_outbox').all() as any[]).map((r) => r.msg_id).filter((m) => !before.has(m));
    assert.equal(added.length, 1, `one outbox entry per write: ${f.logs.join(' | ')}`);
    return added[0] as string;
  };
  const link = (mid: string) => {
    const r = f.db.prepare('SELECT ordered, predecessor, payload FROM _zebridge_outbox WHERE msg_id = ?').get(mid) as any;
    return { ordered: r.ordered === 1, column: r.predecessor ?? null, payload: JSON.parse(r.payload) };
  };
  return { ...f, mutate, link };
}

test('ordering: each ordered write names the newest unsettled one, fixed at creation, across settlement and reopen; an old outbox stays unordered', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zb-order-'));
  const path = `${dir}/r.sqlite3`;
  const f = orderFixture(path);
  // An outbox from before the ordered envelope: its entry stays unordered once migrated.
  f.db.prepare(`INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, created_at) VALUES ('old-1', 's', '{}', 'p', 'y', 0)`).run();
  await f.c.addOutboxOrdering();
  assert.deepEqual({ ...(f.db.prepare(`SELECT ordered, predecessor FROM _zebridge_outbox WHERE msg_id = 'old-1'`).get() as any) }, { ordered: 0, predecessor: null });

  const m1 = await f.mutate('p', 'INSERT', 'x', '2026-11-09T10:00:00.000001Z');
  assert.deepEqual([f.link(m1).ordered, f.link(m1).column, f.link(m1).payload.predecessor, f.link(m1).payload.ordering], [true, null, null, 1]);
  const m2 = await f.mutate('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000002Z');
  assert.equal(f.link(m2).column, m1);
  assert.equal(f.link(m2).payload.predecessor, m1);
  // A table the bridge offers no ordering for: no field, and never a predecessor.
  const m3 = await f.mutate('c', 'INSERT', 'k', '2026-11-09T10:00:00.000003Z');
  assert.deepEqual([f.link(m3).ordered, f.link(m3).column, 'ordering' in f.link(m3).payload, 'predecessor' in f.link(m3).payload], [false, null, false, false]);
  const m4 = await f.mutate('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000004Z');
  assert.equal(f.link(m4).column, m2);

  // m4 settles: the next write names m2, the newest still unsettled; links never change.
  await f.c.outboxDrop(m4, true);
  const m5 = await f.mutate('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000005Z');
  assert.equal(f.link(m5).column, m2);
  assert.equal(f.link(m2).payload.predecessor, m1);
  f.db.close();

  // Reopen: the tail is the outbox itself, so the chain continues where it was.
  const g = orderFixture(path);
  g.c.lastCreated = Number((g.db.prepare('SELECT max(created_at) AS m FROM _zebridge_outbox').get() as any).m);
  const m6 = await g.mutate('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000006Z');
  assert.equal(g.link(m6).column, m5);
  g.db.close();
});

test('ordering: held moves only the replay deadline; never settles, never shortens, ignored once settled', async () => {
  const f = orderFixture(':memory:');
  await f.c.addOutboxOrdering();
  const m1 = await f.mutate('p', 'INSERT', 'x', '2026-11-09T10:00:00.000001Z');
  const sent = Date.now() + 1000; // published: a replay deadline is set
  f.db.prepare('UPDATE _zebridge_outbox SET retry_at = ? WHERE msg_id = ?').run(sent, m1);
  const seen: any[] = [];
  f.c.verdictHandlers = new Set([(v: any) => seen.push(v)]);
  const logs: [string, string][] = [];
  f.c.appendLog = (_t: string, d: any, level: string) => logs.push([level, String(d)]);
  const held = (body: string) => f.c.handleVerdict(`mutation_ack.omar.${m1}.held`, new TextEncoder().encode(body));
  const due = () => Number((f.db.prepare('SELECT retry_at FROM _zebridge_outbox WHERE msg_id = ?').get(m1) as any)?.retry_at);
  assert.equal(await held('{"status":"held","lease_ms":30000}'), false, 'not definitive');
  const first = due();
  assert.ok(first >= sent + 28_000, `replay moved out (${first - sent} ms)`);
  assert.deepEqual(logs, [['DEBUG', `${m1} held behind its predecessor, replay due in 30000 ms`]], 'the words of libzb\'s debug line');
  // An outbox that cannot be updated: said, and the notice is dropped (libzb's warn line), not thrown at the verdict loop.
  const run = f.c.run;
  f.c.run = async () => { throw new Error('disk I/O error'); };
  assert.equal(await held('{"status":"held","lease_ms":60000}'), false);
  f.c.run = run;
  assert.deepEqual(logs[1], ['WARNING', 'a held notice was not applied (disk I/O error)']);
  assert.equal(due(), first, 'nothing changed');
  await held('{"status":"held","lease_ms":10}');
  await held('{"status":"held"}');
  assert.equal(due(), first, 'a shorter lease or none changes nothing');
  assert.equal(seen.length, 0, 'held reports no outcome');
  // Terminal first, then a delayed held: the outcome stands and nothing comes back.
  await f.c.outboxDrop(m1, true);
  await held('{"status":"held","lease_ms":30000}');
  assert.equal((f.db.prepare('SELECT count(*) AS n FROM _zebridge_outbox').get() as any).n, 0);
  f.db.close();
});

test('ordering: a successor\'s ID is never dated before its predecessor\'s, after a clock step back and across reopen', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zb-order-clock-'));
  const path = `${dir}/r.sqlite3`;
  const f = orderFixture(path);
  await f.c.addOutboxOrdering();
  const t = (id: string) => messageIdCreatedMs(id)!;
  const m1 = await f.mutate('p', 'INSERT', 'x', '2026-11-09T10:00:00.000001Z');
  f.c.clockOffsetMs = () => -3_600_000; // the bridge's clock re-estimated an hour back
  const m2 = await f.mutate('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000002Z');
  assert.equal(t(m2), t(m1), 'equal, not earlier, not invented');
  assert.equal(f.link(m2).payload.predecessor, m1);
  const u = await f.mutate('c', 'INSERT', 'k', '2026-11-09T10:00:00.000003Z');
  assert.ok(t(u) < t(m1) - 3_000_000, 'an unordered write keeps its own clock');
  f.db.close();

  const g = orderFixture(path);
  g.c.lastCreated = Number((g.db.prepare('SELECT max(created_at) AS m FROM _zebridge_outbox').get() as any).m);
  g.c.clockOffsetMs = () => -7_200_000; // reopened with an offset two hours back
  const m3 = await g.mutate('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000004Z');
  assert.equal(t(m3), t(m1));
  assert.equal(g.link(m3).column, m2);
  const ids = (g.db.prepare('SELECT msg_id FROM _zebridge_outbox WHERE ordered = 1 ORDER BY created_at').all() as any[]).map((r) => r.msg_id);
  assert.deepEqual(ids, [m1, m2, m3], 'what was queued is unchanged');
  assert.equal(g.link(m1).payload.predecessor, null);
  assert.equal(g.link(m2).payload.predecessor, m1);
  g.db.close();
});

// Ordering modes (libzb's "ordering modes: …" tests): `after` names one earlier write of this
// client, `independent` has none and leaves the tail. A client ID that needs escaping.
function modesFixture(path: string) {
  const f = orderFixture(path);
  f.c.clientIdValue = 'phone-1.a';
  const verdicts: any[] = [];
  f.c.verdictHandlers = new Set([(v: any) => verdicts.push(v)]);
  const write = (t: 'p' | 'c', op: string, id: string, version: string, opts?: any) =>
    f.c.enqueueMutation(t, op, id, version, { key: { id }, version, client_id: 'phone-1.a', ...(op === 'DELETE' ? {} : { data: t === 'p' ? { title: version } : { pid: 'x' } }) }, opts);
  const count = () => (f.db.prepare('SELECT count(*) AS n FROM _zebridge_outbox').get() as any).n as number;
  return { ...f, write, count, verdicts };
}

test('ordering modes: after names its predecessor, independent leaves the tail, across reopen (escaped client)', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zb-modes-'));
  const path = `${dir}/r.sqlite3`;
  const f = modesFixture(path);
  await f.c.addOutboxOrdering();
  const P = (await f.write('p', 'INSERT', 'x', '2026-11-09T10:00:00.000001Z')).writeId;
  assert.ok(messageIdOfClient(P, 'phone-1.a') && P.includes('-phone~2D1~2Ea-'), P);
  const B = (await f.write('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000002Z', { after: P })).writeId;
  const C = (await f.write('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000003Z', { after: P })).writeId;
  assert.equal(f.link(B).column, P);
  assert.equal(f.link(C).column, P, 'C names P although B was the tail when C was made');
  assert.equal(f.link(C).payload.predecessor, P);
  assert.ok(messageIdCreatedMs(C)! >= messageIdCreatedMs(P)!);
  const I = (await f.write('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000004Z', { independent: true })).writeId;
  assert.deepEqual([f.link(I).ordered, f.link(I).column, 'ordering' in f.link(I).payload, 'predecessor' in f.link(I).payload], [false, null, false, false]);
  f.db.close();

  // Reopen with all four still queued: the links hold, and a default write follows C, not I.
  const g = modesFixture(path);
  g.c.lastCreated = Number((g.db.prepare('SELECT max(created_at) AS m FROM _zebridge_outbox').get() as any).m);
  assert.deepEqual([g.link(B).column, g.link(C).column, g.link(I).column], [P, P, null]);
  const D = (await g.write('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000005Z')).writeId;
  assert.equal(g.link(D).column, C, 'the tail after the independent write is still C');
  g.db.close();
  rmSync(dir, { recursive: true, force: true });
});

test('ordering modes: after a write already gone from the outbox is accepted and dated no earlier than it', async () => {
  const f = modesFixture(':memory:');
  await f.c.addOutboxOrdering();
  const P = (await f.write('p', 'INSERT', 'x', '2026-11-09T10:00:00.000001Z')).writeId;
  await f.c.outboxDrop(P, true);
  f.c.clockOffsetMs = () => -3_600_000; // the clock an hour behind P's time
  const C = (await f.write('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000002Z', { after: P })).writeId;
  assert.equal(f.link(C).column, P);
  assert.equal(messageIdCreatedMs(C), messageIdCreatedMs(P), 'equal, not earlier');
  f.db.close();
});

test('ordering modes: every misuse refuses before anything is written, and names what is wrong', async () => {
  const f = modesFixture(':memory:');
  await f.c.addOutboxOrdering();
  const P = (await f.write('p', 'INSERT', 'x', '2026-11-09T10:00:00.000001Z')).writeId;
  const row = () => ({ ...(f.db.prepare(`SELECT title FROM p WHERE id = 'x'`).get() as any) });
  const shown = row(), queued = f.count();
  const refuses = async (label: string, run: () => Promise<unknown>, words: RegExp) => {
    await assert.rejects(run, (e: any) => e instanceof OrderingRefusal && words.test(e.message), label);
    assert.equal(f.count(), queued, `${label}: nothing queued`);
    assert.deepEqual(row(), shown, `${label}: the displayed row is intact`);
  };
  await refuses('no ordering offered', () => f.write('c', 'INSERT', 'k', '2026-11-09T10:00:00.000002Z', { after: P }), /^after needs ordering: the bridge does not offer it for 'c'$/);
  await refuses('malformed ID', () => f.write('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000003Z', { after: 'legacy-id' }), /^after: 'legacy-id' is not a write of this client$/);
  // Truncated: the client token is right, but no table, key or version: no client made it.
  await refuses('truncated ID', () => f.write('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000003Z', { after: `zb1-t${Date.now()}-INSERT-phone~2D1~2Ea` }), /is not a write of this client/);
  for (const bad of [`zb1-t1-UPDATE-phone~2D1~2Ea-p-k2-x-v`, `zb1-t1-UPDATE-phone~2D1~2Ea-p-k1-x~2-v`, `zb1-t1-MERGE-phone~2D1~2Ea-p-k1-x-v`, `zb1-t1-UPDATE-phone~2D1~2Ea-p-k1-x-v-extra`, `zb1-t1-UPDATE-phone~2D1~2Ea--k1-x-v`])
    assert.equal(messageIdOfClient(bad, 'phone-1.a'), false, bad);
  assert.equal(messageIdOfClient(mutationMsgId(1, 'phone-1.a', 'p', 'DELETE', ['a+b', 'null'], 'v'), 'phone-1.a'), true, 'a composite key');
  // Canonical only: `~72` decodes to `r`, which the generator writes literally.
  const genuine = mutationMsgId(1787824800123, 'phone-1.a', 'rows', 'UPDATE', ['7'], 'v');
  assert.equal(messageIdOfClient(genuine, 'phone-1.a'), true);
  assert.equal(messageIdOfClient(genuine.replace('-rows-', '-~72ows-'), 'phone-1.a'), false, 'a non-canonical spelling of the same table');
  // The time spelled canonically: t0 is a time, t00 and a zero-padded t0123 are not (libzb's same cases).
  assert.equal(messageIdOfClient(mutationMsgId(0, 'phone-1.a', 'rows', 'UPDATE', ['7'], 'v'), 'phone-1.a'), true, 't0');
  assert.equal(messageIdOfClient('zb1-t00-UPDATE-phone~2D1~2Ea-rows-k1-7-v', 'phone-1.a'), false, 't00');
  const t123 = mutationMsgId(123, 'phone-1.a', 'rows', 'UPDATE', ['7'], 'v');
  assert.equal(messageIdOfClient(t123, 'phone-1.a'), true);
  assert.equal(messageIdOfClient(t123.replace('-t123-', '-t0123-'), 'phone-1.a'), false, 'zero-padded');
  // Every ID the generator makes from awkward inputs validates (libzb's same cases).
  for (const [client, table, v] of [['phone 1~x', 'my-table.v2', '2026-11-11T10:00:00.123456Z'], ['é+ü', 't', 'a~b'], ['c:1_2', 'rows', '-']])
    assert.equal(messageIdOfClient(mutationMsgId(1787824800123, client, table, 'INSERT', [v, '', '+'], v), client), true, `${client} ${table} ${v}`);
  const foreign = mutationMsgId(Date.now(), 'phone-2', 'p', 'UPDATE', ['x'], 'v');
  await refuses('another client', () => f.write('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000004Z', { after: foreign }), /is not a write of this client/);
  await refuses('both options', () => f.write('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000005Z', { after: P, independent: true }), /^after and independent cannot be combined$/);
  // Self: P's own version, operation and key, the clock behind P: the built ID is P's. P is
  // in flight (sent, tracked for its echo): the refusal must leave that tracking alone.
  f.c.pendingWrites.set(P, { table: 'p', id: 'x', at: Date.now(), version: '2026-11-09T10:00:00.000001Z' });
  f.c.clockOffsetMs = () => -3_600_000;
  await refuses('self-dependency', () => f.write('p', 'INSERT', 'x', '2026-11-09T10:00:00.000001Z', { after: P }), /^after: a write cannot depend on itself/);
  assert.equal((f.db.prepare('SELECT attempts FROM _zebridge_outbox WHERE msg_id = ?').get(P) as any).attempts, 0, "P's own entry is untouched");
  assert.ok(f.c.pendingWrites.has(P), "P's session tracking survives the refusal");
  // P's echo still settles it, as for any write in flight.
  f.c.confirmEcho('p', f.c.syncedTables.get('p'), { operation: 'INSERT', data: { id: 'x', title: '2026-11-09T10:00:00.000001Z' } });
  assert.ok(!f.c.pendingWrites.has(P), 'its echo found it');
  const gone = () => !f.db.prepare('SELECT 1 FROM _zebridge_outbox WHERE msg_id = ?').get(P);
  for (let i = 0; i < 100 && !gone(); i++) await new Promise((r) => setTimeout(r, 10));
  assert.ok(gone(), 'and settled it: the outbox entry is gone');
  f.db.close();
});

test('ordering modes: ordering withdrawn while the call waits refuses after, with nothing written', async () => {
  const f = modesFixture(':memory:');
  await f.c.addOutboxOrdering();
  const P = (await f.write('p', 'INSERT', 'x', '2026-11-09T10:00:00.000001Z')).writeId;
  const queued = f.count();
  const shown = { ...(f.db.prepare(`SELECT title FROM p WHERE id = 'x'`).get() as any) };
  // A descriptor update lands during the floor read: the table no longer offers ordering.
  f.c.gcWatermark = async () => { f.c.syncedTables.get('p').ordering = 0; return null; };
  await assert.rejects(f.write('p', 'UPDATE', 'x', '2026-11-09T10:00:00.000002Z', { after: P }), (e: any) => e instanceof OrderingRefusal && /^after needs ordering: the bridge does not offer it for 'p'$/.test(e.message));
  assert.equal(f.count(), queued, 'no outbox entry');
  assert.deepEqual({ ...(f.db.prepare(`SELECT title FROM p WHERE id = 'x'`).get() as any) }, shown, 'no optimistic change');
  f.db.close();
});

test('ordering modes: an ID only for a write durably queued; mutate() keeps its shape and its single verdict', async () => {
  const f = modesFixture(':memory:');
  await f.c.addOutboxOrdering();
  Object.assign(f.c, { initializeStorage: async () => {}, newVersion: () => '2026-11-09T10:00:00.000009Z' });
  // Offline: queued, and the returned ID is the stored one.
  const r = await f.c.mutateWithId('p', 'insert', { id: 'y' }, { title: 'y' });
  assert.deepEqual(Object.keys(r).sort(), ['version', 'writeId']);
  assert.ok((f.db.prepare('SELECT 1 FROM _zebridge_outbox WHERE msg_id = ?').get(r.writeId)));
  assert.deepEqual(await f.c.mutate('p', 'update', { id: 'y' }, { title: 'z' }, { version: '2026-11-09T10:00:00.000010Z' }), { version: '2026-11-09T10:00:00.000010Z' });
  // An empty stamp would make an ID no `after` can name: refused before anything is queued.
  const queuedBefore = f.count();
  await assert.rejects(f.c.mutateWithId('p', 'update', { id: 'y' }, { title: 'e' }, { version: '' }), /^Error: a write's version cannot be empty$/);
  assert.equal(f.count(), queuedBefore);
  // A local transaction that rolls back queues nothing: mutateWithId rejects.
  const apply = f.c.applyEvent;
  f.c.applyEvent = async () => { throw new Error('disk full'); };
  const before = f.count();
  await assert.rejects(f.c.mutateWithId('p', 'update', { id: 'y' }, { title: 'w' }, { version: '2026-11-09T10:00:00.000011Z' }), /^Error: write not queued: NotQueued/);
  assert.equal(f.count(), before);
  f.c.applyEvent = apply;
  // The GC floor at enqueue: mutate() reports rejected / PredatesGcWatermark once; mutateWithId rejects; nothing applied or queued.
  f.c.gcWatermark = async () => '2099-01-01T00:00:00.000000Z';
  assert.deepEqual(await f.c.mutate('p', 'update', { id: 'y' }, { title: 'v' }, { version: '2026-11-09T10:00:00.000012Z' }), { version: '2026-11-09T10:00:00.000012Z' });
  assert.deepEqual(f.verdicts.map((v) => [v.outcome, v.reason]), [['rejected', 'PredatesGcWatermark']], 'one notification');
  await assert.rejects(f.c.mutateWithId('p', 'update', { id: 'y' }, { title: 'u' }, { version: '2026-11-09T10:00:00.000013Z' }), /write not queued: PredatesGcWatermark/);
  assert.equal(f.verdicts.length, 2);
  assert.equal(f.count(), before);
  assert.equal((f.db.prepare(`SELECT title FROM p WHERE id = 'y'`).get() as any).title, 'z', 'the displayed row is the last queued write');
  f.db.close();
});

test('ordering modes: the GC floor is still checked at send time, for a floor that moved while offline', async () => {
  const f = modesFixture(':memory:');
  await f.c.addOutboxOrdering();
  const P = (await f.write('p', 'INSERT', 'x', '2026-11-09T10:00:00.000001Z')).writeId;
  const C = (await f.write('p', 'UPDATE', 'x', '2026-11-09T10:00:05.000000Z', { after: P })).writeId;
  // Offline the floor passes P's version, not C's; then the client reconnects and flushes.
  f.c.gcWatermark = async () => '2026-11-09T10:00:02.000000Z';
  const sends: string[] = [];
  Object.assign(f.c, {
    nc: {}, outboxWork: null, outboxWake: false, outboxWindow: 128, nextOutboxRetry: 0, outboxReconnect: 0, outboxRetryEpoch: 0, holdUntil: 0,
    ackPrefix: () => 'mutation_ack', revertOptimisticWrite: async () => {},
    transport: { headers: () => new Map(), jetstream: () => ({ publish: async (_s: string, _b: Uint8Array, o: any) => { sends.push(o.headers.get('Nats-Msg-Id')); return { seq: sends.length }; } }) },
  });
  await f.c.flushOutboxPass();
  assert.deepEqual(f.verdicts.filter((v) => v.reason === 'PredatesGcWatermark').length, 1, 'P refused once');
  assert.ok(!f.db.prepare('SELECT 1 FROM _zebridge_outbox WHERE msg_id = ?').get(P), 'P never sent, dropped');
  assert.deepEqual(sends, [C], 'C is sent, still naming P: the documented limit, resolved by the bridge');
  assert.equal(f.link(C).payload.predecessor, P);
  f.db.close();
});

// ── A queued write belongs to its table incarnation (libzb's "incarnation: …" tests) ──
async function incarnationFixture() {
  const f = orderFixture(':memory:');
  await f.c.addOutboxOrdering();
  f.db.exec("INSERT INTO p (id, title) VALUES ('x', 'orig')");
  const st = f.c.syncedTables.get('p');
  const verdicts: any[] = [];
  Object.assign(f.c, { emitVerdict: (v: any) => verdicts.push(v), ackPrefix: () => 'mutation_ack', reconcile: async () => {} });
  let n = 0;
  const write = (title: string, id = 'x') => {
    const version = `2026-11-12T10:00:00.00000${++n}Z`;
    return f.c.enqueueMutation('p', 'UPDATE', id, version, { key: { id }, version, client_id: 'c-1', data: { title } }, undefined);
  };
  const outbox = () => (f.db.prepare('SELECT msg_id, incarnation, other_incarnation, payload FROM _zebridge_outbox ORDER BY created_at').all() as any[]);
  return { ...f, st, write, outbox, verdicts };
}

test('incarnation: a withheld or unwritable descriptor refuses a new edit before anything is applied or queued; writable null refuses nothing', async () => {
  const f = await incarnationFixture();
  try {
    for (const [label, patch, reason] of [
      ['writes withheld', { advertised: true, incarnation: null, writable: true }, 'IncarnationUnknown'],
      ['writable false', { advertised: false, incarnation: null, writable: false }, 'TableNotWritable'],
      ['writes withheld, writable null', { advertised: true, incarnation: null, writable: null }, 'IncarnationUnknown'],
    ] as const) {
      Object.assign(f.st, patch);
      assert.deepEqual(await f.write(`refused: ${label}`), { refused: reason }, label);
      assert.equal(f.title('x'), 'orig', `${label}: the row is untouched`);
      assert.equal(f.outbox().length, 0, `${label}: nothing queued`);
    }
    Object.assign(f.st, { advertised: false, incarnation: null, writable: null });
    const r = await f.write('legacy');
    assert.ok('writeId' in r, `writable null and no advert: an ordinary, unstamped write — got ${JSON.stringify(r)}; ${f.logs.slice(-3).join(' | ')}`);
    assert.equal(f.title('x'), 'legacy');
    assert.equal(f.outbox()[0].incarnation, null);
    assert.equal('incarnation' in JSON.parse(f.outbox()[0].payload), false);
  } finally { f.db.close(); }
});

test('incarnation: a stamped write carries the scope stamp; seeded before stamps adopts at the same epoch; a replacement in progress refuses', async () => {
  const f = await incarnationFixture();
  try {
    Object.assign(f.st, { advertised: true, incarnation: 5, writable: true, seedEpoch: 3 });
    // No stamp, no anchor: the identity of the rows cannot be stated.
    assert.deepEqual(await f.write('unknown'), { refused: 'IncarnationUnknown' });
    // An anchor at the descriptor's epoch: seeded before stamps, adopted and recorded.
    f.db.exec("INSERT INTO _zebridge_generations (tbl, watermark, seed_epoch) VALUES ('p', 'w', 3)");
    await f.write('adopted');
    assert.equal(f.outbox()[0].incarnation, 5);
    assert.equal(JSON.parse(f.outbox()[0].payload).incarnation, 5);
    assert.equal((f.db.prepare("SELECT incarnation FROM _zebridge_stamp WHERE tbl = 'p'").get() as any).incarnation, 5);
    // The scope's stamp wins over a descriptor that moved on: the rows are not replaced yet.
    f.db.exec("UPDATE _zebridge_stamp SET incarnation = 4 WHERE tbl = 'p'");
    await f.write('still the old rows');
    assert.equal(f.outbox()[1].incarnation, 4);
    // A replacement in progress (an owed record): refused, nothing written.
    f.db.exec("INSERT INTO _zebridge_fence (tbl) VALUES ('p')");
    assert.deepEqual(await f.write('mid-replacement'), { refused: 'ReplacementIncomplete' });
    assert.equal(f.outbox().length, 2);
    assert.equal(f.title('x'), 'still the old rows');
  } finally { f.db.close(); }
});

test('incarnation: a new stamp marks older writes; they are never replayed, and their verdict settles them once without touching the new row', async () => {
  const f = await incarnationFixture();
  try {
    Object.assign(f.st, { advertised: true, incarnation: 4, writable: true });
    f.db.exec("INSERT INTO _zebridge_stamp (tbl, incarnation) VALUES ('p', 4)");
    await f.write('old edit');
    const [old] = f.outbox();
    // The table was re-created; its seed installs incarnation 5 over a row of the same key.
    await f.c.transaction((tx: any) => f.c.installStamp(tx, 'p', { incarnation: 5 }));
    assert.equal(f.outbox()[0].other_incarnation, 1, 'the old write is marked as another incarnation\'s');
    // The new row has the same key AND the same value the old edit wrote: a check of values
    // alone would take it for the old write's own copy and revert it.
    f.db.exec("UPDATE p SET title = 'old edit' WHERE id = 'x'");
    f.db.exec("UPDATE p SET title = 'replacement' WHERE id = 'x'");
    await f.c.reapplyPending('p');
    assert.equal(f.title('x'), 'replacement', 'not replayed over the new rows');
    f.db.exec("UPDATE p SET title = 'old edit' WHERE id = 'x'");
    // Its verdict: settled and reported once; nothing local reverted.
    await f.c.handleVerdictOnce(`mutation_ack.omar.${old.msg_id}`, new TextEncoder().encode(JSON.stringify({ status: 'rejected', reason: 'TableIncarnationChanged' })));
    assert.equal(f.outbox().length, 0);
    assert.equal(f.title('x'), 'old edit', 'the new row, equal in key and value to the old edit, is untouched');
    assert.equal(f.db.prepare('SELECT count(*) n FROM _zebridge_undo').get()!.n, 0, 'no undo owed against the new row');
    assert.deepEqual(f.verdicts.map((v) => [v.outcome, v.reason]), [['rejected', 'TableIncarnationChanged']]);
  } finally { f.db.close(); }
});

test('incarnation: a dropped table keeps its stamped writes for the bridge to settle and discards unstamped ones', async () => {
  const f = await incarnationFixture();
  try {
    await f.write('legacy');                                   // no advert: unstamped
    Object.assign(f.st, { advertised: true, incarnation: 4, writable: true });
    f.db.exec("INSERT INTO _zebridge_stamp (tbl, incarnation) VALUES ('p', 4)");
    await f.write('stamped');
    await f.c.discardOutbox('p', 'dropped');
    assert.deepEqual(f.outbox().map((r) => [r.incarnation, r.other_incarnation]), [[4, 1]]);
  } finally { f.db.close(); }
});
