import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { ZeBridge } from './libzb.ts';

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
