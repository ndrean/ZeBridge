import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { ZeBridge } from './libzb.ts';

test('automatic outbox pages bound network work, reuse a manager and rotate a failed head', async () => {
  const db = new DatabaseSync(':memory:');
  try {
    db.exec(`CREATE TABLE _zebridge_outbox (
      msg_id TEXT PRIMARY KEY, subject TEXT, payload TEXT, tbl TEXT, row_id TEXT,
      attempts INTEGER DEFAULT 0, created_at INTEGER, published INTEGER DEFAULT 1)`);
    for (let i = 0; i < 24; i++) {
      db.prepare('INSERT INTO _zebridge_outbox(msg_id,subject,payload,tbl,row_id,created_at) VALUES (?,?,?,?,?,?)')
        .run(`id${i}`, 'mutation.alice.item.insert', '{}', 'item', String(i), i);
    }
    const gets: string[] = [], sends: string[] = [];
    let managers = 0, fail = true;
    const make = () => {
      const c: any = Object.create(ZeBridge.prototype);
      Object.assign(c, {
        nc: {}, outboxInitPromise: Promise.resolve(), outboxWork: null,
        holdUntil: 0, pendingWrites: new Map(), config: { principal: 'alice', grammar: {} },
        run: async (sql: string, ...args: any[]) => {
          const st = db.prepare(sql);
          return st.columns().length ? st.all(...args) : (st.run(...args), []);
        },
        appendLog: () => {}, gcWatermark: async () => null, ackPrefix: () => 'mutation_ack',
        transport: {
          headers: () => new Map(),
          jetstreamManager: async () => {
            managers++;
            return { direct: { getMessage: async (_stream: string, opts: any) => {
              gets.push(opts.last_by_subj.split('.').pop());
              throw new Error('404 not found');
            } } };
          },
          jetstream: () => ({ publish: async (_subject: string, _bytes: Uint8Array, opts: any) => {
            sends.push(opts.headers.get('Nats-Msg-Id'));
            if (fail) throw new Error('hub unavailable');
            return { seq: sends.length };
          } }),
        },
      });
      return c;
    };
    await make().flushOutboxPass(true);
    assert.deepEqual(sends, ['id0'], 'a publish failure stops the automatic pass');
    assert.deepEqual(gets, ['id0']);
    fail = false; sends.length = gets.length = managers = 0;
    // A replacement shell uses the persisted counters, not an in-memory cursor.
    await make().flushOutboxPass(true);
    assert.deepEqual(sends, Array.from({length: 8}, (_, i) => `id${i + 1}`));
    assert.equal(gets.length, 8);
    assert.equal(managers, 1, 'one INFO/account check per pass, not per ID');
    assert.equal(db.prepare('SELECT count(*) AS n FROM _zebridge_outbox WHERE attempts = 1').get()!.n, 9);
    sends.length = gets.length = managers = 0;
    await make().flushOutbox();
    assert.equal(sends.length, 24, 'explicit flush keeps its full-pass contract');
    assert.equal(gets.length, 24);
    assert.equal(managers, 1);
  } finally { db.close(); }
});
