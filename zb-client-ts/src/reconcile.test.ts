import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { encode } from '@msgpack/msgpack';
import { ZeBridge } from './libzb.ts';
import { Measure } from './measure.ts';
import { loadCore } from './wasm-core.ts';

// Exercise the actual orchestration methods, with SQLite and a controllable NATS seam.
function fixture(path = ':memory:') {
  const db = new DatabaseSync(path);
  db.exec(`
    CREATE TABLE IF NOT EXISTS _zebridge_reconcile (
      msg_id TEXT PRIMARY KEY, tbl TEXT, state TEXT, routes TEXT,
      targets TEXT DEFAULT '', data TEXT, since INTEGER);
    CREATE TABLE IF NOT EXISTS _zebridge_generations (tbl TEXT);
    CREATE TABLE IF NOT EXISTS _zebridge_stream_seq (stream TEXT PRIMARY KEY, last_seq INTEGER, created TEXT);
    CREATE TABLE IF NOT EXISTS item (id INTEGER, value TEXT);
  `);
  const c: any = Object.create(ZeBridge.prototype);
  const outcomes: any[] = [];
  c.run = async (sql: string, ...args: any[]) => {
    const stmt = db.prepare(sql);
    return stmt.columns().length ? stmt.all(...args) : (stmt.run(...args), []);
  };
  c.transaction = async (fn: any) => {
    db.exec('BEGIN');
    try { await fn(c.run); db.exec('COMMIT'); } catch (e) { db.exec('ROLLBACK'); throw e; }
  };
  const seq = Object.fromEntries(db.prepare('SELECT stream, last_seq FROM _zebridge_stream_seq').all().map((r) => [String(r.stream), Number(r.last_seq)]));
  Object.assign(c, {
    outboxInitPromise: Promise.resolve(), reconcileLoaded: false, streamWork: Promise.resolve(),
    globalSyncState: { seq }, hubOnly: new Set(), healedRoutes: new Set(),
    tailGen: new Map(), tailStops: new Map(), tailFilters: new Map(), streamsSubscribed: false, measure: new Measure(() => {}), syncedTables: new Map([['item', {}]]), ondemandSet: new Set(),
    streamCreated: new Map([['CDC_acme', 'same']]), restarted: new Set(), failed: new Set(), nc: {},
    transport: {
      jetstreamManager: async () => ({ streams: { info: async () => ({ state: { last_seq: 12 } }) } }),
      deliverPolicy: { byStartSequence: 'by_start_sequence', all: 'all' },
    },
    jsOpts: () => ({ domain: 'hub' }), appendLog: () => {}, emitVerdict: (e: any) => outcomes.push(e),
    cdcStreams: () => ['CDC_acme'], effectiveTenantFor: () => 'acme', cdcStreamForTenant: () => 'CDC_acme',
    reach: () => {}, cdcFilters: () => ['cdc.acme.item.>'], config: { grammar: {}, cdcBatchEvents: 20000 },
    bulkStats: { bulked: 0, statements: 0, single: 0, fallbacks: 0 }, lastPending: {},
    dialect: { deferForeignKeys: async () => {} }, applyBatchPlanned: async () => {},
    triggerChange: () => {}, retryFkHeld: async () => {},
  });
  const mark = (state: string, routes = 'CDC_acme', targets = '') => {
    db.prepare('INSERT INTO _zebridge_reconcile (msg_id,tbl,state,routes,targets,data,since) VALUES (?,?,?,?,?,?,?)')
      .run('one', 'item', state, routes, targets, '{"msgId":"one"}', 1);
  };
  return { db, c, outcomes, mark };
}

test('reconciliation retains every marker when any tail target is unavailable', async () => {
  for (const state of ['rewound', 'healed']) {
    const { db, c, outcomes, mark } = fixture();
    try {
      mark(state, 'CDC_acme,CDC_b');
      c.transport.jetstreamManager = async () => ({ streams: { info: async (route: string) => {
        if (route === 'CDC_b') throw new Error('request timed out');
        return { state: { last_seq: 0 } };
      } } });
      for (let i = 0; i < 2; i++) assert.equal(await c.reconcileClear(), true);
      assert.equal(db.prepare('SELECT count(*) AS n FROM _zebridge_reconcile').get()!.n, 1);
      assert.equal(outcomes.length, 0);
      c.transport.jetstreamManager = async () => ({ streams: { info: async () => ({ state: { last_seq: 0 } }) } });
      await c.reconcileClear();
      assert.equal(db.prepare('SELECT targets FROM _zebridge_reconcile').get()!.targets, 'CDC_acme:0,CDC_b:0');
      await c.reconcileClear();
      assert.equal(outcomes[0].outcome, 'reconciled');
    } finally { db.close(); }
  }
});

test('an unavailable route does not stall independent markers in the same pass', async () => {
  const { db, c, outcomes, mark } = fixture();
  try {
    mark('rewound', 'CDC_bad');
    db.prepare('INSERT INTO _zebridge_reconcile (msg_id,tbl,state,routes,targets,data,since) VALUES (?,?,?,?,?,?,?)')
      .run('healthy', 'item', 'rewound', 'CDC_acme', '', '{"msgId":"healthy"}', 2);
    c.transport.jetstreamManager = async () => ({ streams: { info: async (route: string) => {
      if (route === 'CDC_bad') throw new Error('request timed out');
      return { state: { last_seq: 0 } };
    } } });
    assert.equal(await c.reconcileClear(), true);
    assert.equal(db.prepare("SELECT state FROM _zebridge_reconcile WHERE msg_id = 'healthy'").get()!.state, 'healed');
    assert.equal(await c.reconcileClear(), true);
    assert.deepEqual(outcomes.map((o) => [o.msgId, o.outcome]), [['healthy', 'reconciled']]);
    assert.equal(db.prepare('SELECT msg_id FROM _zebridge_reconcile').get()!.msg_id, 'one');
  } finally { db.close(); }
});

test('parked markers do not re-arm reconciliation timers', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  for (const ondemand of [true, false]) {
    const { db, c, mark } = fixture();
    try {
      mark('pending', '');
      if (ondemand) c.ondemandSet.add('item'); else c.syncedTables.clear();
      let passes = 0;
      const rewind = c.reconcileRewind.bind(c);
      c.reconcileRewind = async () => { passes++; await rewind(); };
      await c.reconcile();
      t.mock.timers.tick(10_000);
      for (let i = 0; i < 8; i++) await new Promise(setImmediate);
      assert.equal(passes, 1);
      assert.equal(await c.reconciling(), 1);
    } finally { c.nc = null; db.close(); }
  }
});

test('on-demand and unfollowed markers stay unresolved, including older empty-route states', async () => {
  for (const [state, routes] of [['pending', ''], ['rewound', ''], ['healed', ''], ['rewound', 'CDC_acme'], ['healed', 'CDC_acme']]) {
    const { db, c, outcomes, mark } = fixture();
    try {
      mark(state, routes, state === 'healed' ? 'CDC_acme:12' : ''); c.ondemandSet.add('item');
      db.exec("INSERT INTO _zebridge_stream_seq VALUES ('CDC_acme', 0, 'same')");
      db.exec("INSERT INTO item VALUES (1, 'optimistic')");
      await c.reconcileRewind(); await c.reconcileClear(); await c.reconcileClear();
      assert.equal(db.prepare('SELECT count(*) AS n FROM _zebridge_reconcile').get()!.n, 1);
      assert.equal(db.prepare('SELECT value FROM item').get()!.value, 'optimistic');
      assert.equal(outcomes.length, 0);
      c.syncedTables.clear(); c.ondemandSet.clear();
      await c.reconcileRewind(); await c.reconcileClear();
      assert.equal(outcomes.length, 0);
      c.syncedTables.set('item', {});
      await c.reconcileRewind();
      assert.equal(db.prepare('SELECT state FROM _zebridge_reconcile').get()!.state, 'rewound');
      assert.equal(db.prepare('SELECT last_seq FROM _zebridge_stream_seq').get()!.last_seq, 0);
      assert.equal(c.hubOnly.has('CDC_acme'), true);
    } finally { db.close(); }
  }
});

test('pending, rewound and healed reconciliation states survive a database reopen', async () => {
  for (const state of ['pending', 'rewound', 'healed']) {
    const dir = mkdtempSync(join(tmpdir(), 'zb-reconcile-unit-'));
    try {
      const path = join(dir, 'replica.sqlite3');
      let f = fixture(path);
      f.mark(state, state === 'pending' ? '' : 'CDC_acme', state === 'healed' ? 'CDC_acme:12' : '');
      f.db.prepare('INSERT INTO _zebridge_stream_seq VALUES (?, ?, ?)').run('CDC_acme', state === 'pending' ? 10 : state === 'rewound' ? 0 : 11, 'same');
      f.db.close();
      f = fixture(path);
      try {
        await f.c.reconcileRewind();
        if (state !== 'healed') {
          assert.equal(f.c.hubOnly.has('CDC_acme'), true);
          await f.c.reconcileClear();
          assert.equal(f.outcomes.length, 0);
          // The gap path's successful seed is the event allowed to release this route.
          f.c.healedRoutes.add('CDC_acme');
        }
        await f.c.reconcileClear();
        assert.equal(f.outcomes.length, 0);
        f.c.globalSyncState.seq.CDC_acme = 12;
        await f.c.reconcileClear();
        assert.equal(f.outcomes[0].outcome, 'reconciled');
      } finally { f.db.close(); }
    } finally { rmSync(dir, { recursive: true, force: true }); }
  }
});

test('a new marker cannot reuse a route heal that preceded its watermark removal', async () => {
  const { db, c, mark } = fixture();
  try {
    mark('pending', '');
    db.exec("INSERT INTO _zebridge_stream_seq VALUES ('CDC_acme', 11, 'same')");
    c.globalSyncState.seq.CDC_acme = 11;
    c.hubOnly.add('CDC_acme'); c.healedRoutes.add('CDC_acme');
    await c.reconcileRewind();
    await c.reconcileClear();
    assert.equal(c.globalSyncState.seq.CDC_acme, 0);
    assert.equal(c.hubOnly.has('CDC_acme'), true);
    assert.equal(db.prepare('SELECT state FROM _zebridge_reconcile').get()!.state, 'rewound');
  } finally { db.close(); }
});

test('rewind fences buffered timers and waits for active flushes and idle checks', async (t) => {
  await loadCore(readFileSync(new URL('../wasm/zb_core.wasm', import.meta.url)));
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval'] });
  let clock = Date.now();
  t.mock.method(Date, 'now', () => clock);
  for (const mode of ['buffered', 'flush', 'idle']) {
    const inFlight = mode === 'flush';
    const idle = mode === 'idle';
    const { db, c, mark } = fixture();
    try {
      c.syncedTables.clear(); c.globalSyncState.seq.CDC_acme = 10;
      db.exec("INSERT INTO _zebridge_stream_seq VALUES ('CDC_acme', 10, 'same')");
      let wake: ((r: IteratorResult<any>) => void) | undefined;
      let sent = idle;
      const iter = {
        [Symbol.asyncIterator]() { return this; },
        next: () => {
          if (!sent) {
            sent = true;
            return Promise.resolve({ done: false, value: { seq: 11, subject: 'cdc.acme.item.insert', data: encode({ table: 'item', operation: 'INSERT' }), info: { pending: inFlight ? 0 : 1, deliverySequence: 1 }, ack() {} } });
          }
          return new Promise<IteratorResult<any>>((r) => { wake = r; });
        },
        stop() { wake?.({ done: true, value: undefined }); },
      };
      let release!: () => void;
      let applying = false;
      const manager = {
        streams: { info: async () => ({ created: 'same', state: { first_seq: 1, last_seq: 11 } }) },
        consumers: {
          add: async () => ({ name: 'old', num_pending: 2 }),
          info: async () => {
            applying = true;
            await new Promise<void>((r) => { release = r; });
            return { num_pending: 0, num_ack_pending: 0, delivered: { consumer_seq: 0, stream_seq: 0 } };
          },
        },
      };
      c.transport.jetstreamManager = async () => manager;
      c.transport.jetstream = () => ({ consumers: { get: async () => ({ consume: async () => iter }) } });
      c.applyBatchPlanned = async () => { applying = true; await new Promise<void>((r) => { release = r; }); };
      await c.subscribeStreams();
      if (idle) { clock += 30_000; t.mock.timers.tick(30_000); }
      for (let i = 0; i < 8; i++) await new Promise(setImmediate);
      assert.equal(applying, inFlight || idle);
      c.syncedTables.set('item', {}); mark('pending', '');
      const rewind = c.reconcileRewind();
      for (let i = 0; i < 8; i++) await new Promise(setImmediate);
      if (inFlight || idle) {
        assert.equal(db.prepare('SELECT last_seq FROM _zebridge_stream_seq').get()!.last_seq, 10);
        release();
      }
      await rewind;
      t.mock.timers.tick(200);
      for (let i = 0; i < 8; i++) await new Promise(setImmediate);
      assert.equal(db.prepare('SELECT last_seq FROM _zebridge_stream_seq').get()!.last_seq, 0);
      assert.equal(c.globalSyncState.seq.CDC_acme, 0);
      assert.equal(c.hubOnly.has('CDC_acme'), true);
      if (!inFlight && !idle) assert.equal(applying, false);
    } finally { c.nc = null; db.close(); }
  }
});

test('a stream with no followed table gets no tail consumer', async () => {
  await loadCore(readFileSync(new URL('../wasm/zb_core.wasm', import.meta.url)));
  const { db, c } = fixture();
  try {
    // On-demand only: the table is known but nothing on the stream is followed. The
    // real filter function decides (the fixture's stub always names a subject).
    c.cdcFilters = ZeBridge.prototype['cdcFilters' as keyof ZeBridge];
    c.ondemandSet.add('item');
    c.globalSyncState.seq.CDC_acme = 10;
    db.exec("INSERT INTO _zebridge_stream_seq VALUES ('CDC_acme', 10, 'same')");
    let added = 0;
    c.transport.jetstreamManager = async () => ({
      streams: { info: async () => ({ created: 'same', state: { first_seq: 1, last_seq: 11 } }) },
      consumers: { add: async () => { added++; return { name: 'x', num_pending: 0 }; } },
    });
    c.transport.jetstream = () => ({ consumers: { get: async () => ({ consume: async () => ({ [Symbol.asyncIterator]() { return this; }, next: () => new Promise(() => {}), stop() {} }) }) } });
    await c.subscribeStreams();
    assert.equal(added, 0);
    assert.equal(c.tailFilters.has('CDC_acme'), false);
  } finally { c.nc = null; db.close(); }
});

test('a reseed caused by a shared-stream gap rewinds the table\'s own stream to the chain\'s cut', async () => {
  await loadCore(readFileSync(new URL('../wasm/zb_core.wasm', import.meta.url)));
  const { db, c } = fixture();
  try {
    // A tenant table: own rows on CDC_acme (no gap: stored 20, stream 1..25), open-tenant rows
    // on CDC_PUBLIC (a real gap: stored 5, the stream now starts at 10).
    c.cdcFilters = ZeBridge.prototype['cdcFilters' as keyof ZeBridge];
    c.cdcStreams = () => ['CDC_acme', 'CDC_PUBLIC'];
    c.cdcStreamForTenant = (t: string) => (t === '_default' ? 'CDC_PUBLIC' : 'CDC_acme');
    c.config = { grammar: { cdc_streams: { public: 'CDC_PUBLIC', tenant_prefix: 'CDC_' }, open_tenant: '_default' }, cdcBatchEvents: 20000 };
    c.tenantValue = 'acme';
    c.syncedTables = new Map([['item', { tenantColumn: 'tenant_id', pkCols: ['id'] }]]);
    c.streamCreated = new Map([['CDC_acme', 'same'], ['CDC_PUBLIC', 'same']]);
    c.globalSyncState.seq = { CDC_acme: 20, CDC_PUBLIC: 5 };
    db.exec("INSERT INTO _zebridge_stream_seq VALUES ('CDC_acme', 20, 'same'), ('CDC_PUBLIC', 5, 'same')");
    db.exec("INSERT INTO _zebridge_generations VALUES ('item')");
    c.dialect = { setForeignKeys: async () => {}, foreignKeyViolations: async () => 0 };
    // The chain: its cut on CDC_acme (12) is older than the stored position (20).
    c.applyGenerations = async (table: string) => {
      const st = c.syncedTables.get(table);
      Object.assign(st, { seedSeq: 12, seedStream: 'CDC_acme', sharedSeedSeq: 15, seedLsn: 1 });
      return true;
    };
    const starts: Record<string, number | undefined> = {};
    const info: Record<string, any> = { CDC_acme: { first_seq: 1, last_seq: 25 }, CDC_PUBLIC: { first_seq: 10, last_seq: 30 } };
    c.transport.jetstreamManager = async () => ({
      streams: { info: async (name: string) => ({ created: 'same', state: info[name] }) },
      consumers: { add: async (name: string, cfg: any) => { starts[name] = cfg.opt_start_seq; return { name: 'x', num_pending: 0 }; } },
    });
    c.transport.jetstream = () => ({ consumers: { get: async () => ({ consume: async () => ({ [Symbol.asyncIterator]() { return this; }, next: () => new Promise(() => {}), stop() {} }) }) } });
    await c.subscribeStreams();
    assert.equal(starts.CDC_acme, 13, 'the own stream replays from the chain\'s cut, not from the stored position');
    assert.equal(starts.CDC_PUBLIC, 16, 'the gapped shared stream resumes at the shared cut');
    assert.equal(db.prepare("SELECT last_seq FROM _zebridge_stream_seq WHERE stream = 'CDC_acme'").get()!.last_seq, 12);
  } finally {
    for (const stream of [...c.tailStops.keys()]) await c.stopTail(stream); // their idle timers
    c.nc = null; db.close();
  }
});
