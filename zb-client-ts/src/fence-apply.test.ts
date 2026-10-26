// The fence through the real apply path (libzb.ts applyGenerationsFrom): a chain cut BEHIND
// the replica's position replaces the rows and moves the cursors back to its cuts, in the
// recovery model's order — PREPARE, replace, VERIFY, COMMIT. The same cases libzb's
// recreate_recovery.py check 4 runs live, here deterministically: SQLite on a file (a
// "crash" is a thrown fault and a new client on the same file), a stubbed NATS.
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
import { sqliteDialect } from './dialect.ts';

const OWN = 'CDC_acme';
const PUB = 'CDC_PUBLIC';
const VERSION = (n: number) => `2026-10-26T07:00:${String(n).padStart(2, '0')}.000000Z`;

/// The world a client reads: stream facts, the manifest and the chain objects. Shared by the
/// clients of one test, as the real hub is shared by a client and its reopened self.
class World {
  streams: Record<string, { created: string; first: number; last: number } | null> = {
    [OWN]: { created: 'own-1', first: 1, last: 700 },
    [PUB]: { created: 'pub-1', first: 1, last: 200 },
  };
  objects = new Map<string, Uint8Array>();
  manifests: Record<string, any> = {};
  /// The `item` table's manifest (most tests have only that table).
  get manifest(): any { return this.manifests.item ?? null; }
  set manifest(m: any) { this.manifests.item = m; }
  /// A chain of `rows` rows (ids 1..rows, note `new`), cut at `cut` on the own route and
  /// `sharedCut` on CDC_PUBLIC; the full alone.
  publish(gen: number, rows: number, cut: number, sharedCut: number | null = 40, table = 'item') {
    const name = `${table}-i1-g${gen}-full`;
    this.objects.set(name, encode({
      columns: ['id', 'tenant_id', 'note', 'updated_at'], version_column: 'updated_at', kind: 'full', gen,
      rows: Array.from({ length: rows }, (_, i) => [i + 1, 'acme', `new-${gen}`, VERSION(10)]),
    }));
    this.manifests[table] = {
      gen, bucket: 'gen-acme', version_column: 'updated_at', seed_epoch: 0,
      full: { gen, object: name, cutoff: VERSION(10), sorted: true }, deltas: [], checkpoints: [],
      cutoff_version: VERSION(10), cutoff_lsn: '0/1000',
      cdc_stream: OWN, cutoff_seq: cut, cdc_stream_created: 'own-1',
      ...(sharedCut === null ? {} : { shared_cdc_stream: PUB, shared_cutoff_seq: sharedCut, shared_cdc_stream_created: 'pub-1' }),
    };
  }
}

type Hook = (point: string, c: any) => void | Promise<void>;

/// A client on `path`, fed by `world`. `hook` is the harness's fault point.
function client(path: string, world: World, opts: { hook?: Hook; others?: string[] } = {}) {
  const db = new DatabaseSync(path);
  db.exec(`
    CREATE TABLE IF NOT EXISTS _zebridge_stream_seq (stream TEXT PRIMARY KEY, last_seq INTEGER NOT NULL, created TEXT);
    CREATE TABLE IF NOT EXISTS _zebridge_generations (tbl TEXT PRIMARY KEY, watermark TEXT NOT NULL, cutoff_lsn INTEGER NOT NULL, seed_epoch INTEGER NOT NULL DEFAULT 0, seed_seq INTEGER, seed_stream TEXT, shared_seed_seq INTEGER);
    CREATE TABLE IF NOT EXISTS _zebridge_fence (tbl TEXT PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS _zebridge_sync (id INTEGER PRIMARY KEY, global_last_lsn INTEGER, global_last_seq INTEGER);
    INSERT OR IGNORE INTO _zebridge_sync VALUES (1, 0, 0);
    CREATE TABLE IF NOT EXISTS _zebridge_frontier (tbl TEXT NOT NULL, stream TEXT NOT NULL, created TEXT NOT NULL, seq INTEGER NOT NULL, PRIMARY KEY (tbl, stream));
    CREATE TABLE IF NOT EXISTS item (id INTEGER PRIMARY KEY, tenant_id TEXT, note TEXT, updated_at TEXT);
    CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, tenant_id TEXT, note TEXT, updated_at TEXT);
    CREATE TABLE IF NOT EXISTS catalogue (id INTEGER PRIMARY KEY, tenant_id TEXT, note TEXT, updated_at TEXT);
  `);
  const c: any = Object.create(ZeBridge.prototype);
  const log: string[] = [];
  c.run = async (sql: string, ...args: any[]) => {
    const stmt = db.prepare(sql);
    return stmt.columns().length ? stmt.all(...args) : (stmt.run(...args), []);
  };
  // One connection: transactions run one after the other, as the real driver's do.
  let txChain: Promise<unknown> = Promise.resolve();
  c.transaction = (fn: any) => {
    const work = txChain.then(async () => {
      db.exec('BEGIN');
      try { await fn(c.run); db.exec('COMMIT'); } catch (e) { db.exec('ROLLBACK'); throw e; }
    });
    txChain = work.catch(() => {});
    return work;
  };
  const seq = Object.fromEntries(db.prepare('SELECT stream, last_seq FROM _zebridge_stream_seq').all().map((r) => [String(r.stream), Number(r.last_seq)]));
  const syncedTables = new Map<string, any>([['item', {
    pkCols: ['id'], columns: ['id', 'tenant_id', 'note', 'updated_at'], tenantColumn: 'tenant_id', versionColumn: 'updated_at', seedEpoch: 0,
  }]]);
  for (const t of opts.others ?? []) syncedTables.set(t, {
    pkCols: ['id'], columns: ['id', 'tenant_id', 'note', 'updated_at'], tenantColumn: 'tenant_id', versionColumn: 'updated_at', seedEpoch: 0,
  });
  const streamCreated = new Map<string, string>(db.prepare('SELECT stream, created FROM _zebridge_stream_seq WHERE created IS NOT NULL').all().map((r) => [String(r.stream), String(r.created)]));
  const passes: number[] = [];
  Object.assign(c, {
    nc: {}, tenantValue: 'acme', syncedTables, ondemandSet: new Set(), seedInFlight: new Map(),
    globalSyncState: { seq }, tailGen: new Map(), tailStops: new Map(), tailFilters: new Map(), fenced: new Map(),
    streamWork: Promise.resolve(), streamsSubscribed: true, restarted: new Set(), failed: new Set(), hubOnly: new Set(),
    applyingByChoice: new Set(), healedRoutes: new Set(), streamCreated, fkHeld: [], hlcFloor: '', frontiers: new Map(), frontierGated: 0,
    pendingWrites: new Map(), rebase: new Map(), bulkStats: { single: 0, statements: 0, bulked: 0, fallbacks: 0 },
    measure: new Measure(() => {}), dialect: sqliteDialect, storage: {}, platform: {},
    config: {
      principal: 'p', seedChunkRows: 2, bulkStatement: 'json_each', faultHook: opts.hook ? (point: string) => opts.hook!(point, c) : undefined,
      grammar: {
        generations: { kv: 'generations', bucket_prefix: 'gen-' }, kv: { tenants: 'tenants' }, streams: { cdc: 'CDC' },
        cdc_streams: { public: PUB, tenant_prefix: 'CDC_' }, open_tenant: '_default',
      },
    },
    transport: {
      kv: async () => ({ get: async (key: string) => {
        const m = world.manifests[String(key).split('.').pop() as string];
        return m ? { operation: 'PUT', value: new TextEncoder().encode(JSON.stringify(m)) } : null;
      } }),
      objectStore: async () => ({}),
      jetstreamManager: async () => ({ streams: { info: async (name: string) => {
        const st = world.streams[name];
        if (!st) throw new Error(`no stream ${name}`);
        return { created: st.created, state: { first_seq: st.first, last_seq: st.last } };
      } } }),
    },
    jsOpts: () => ({ domain: 'hub' }), appendLog: (_t: string, m: string) => { log.push(m); },
    objectBlob: async (_os: unknown, _bucket: string, name: string) => world.objects.get(name) ?? null,
    seedProgress: () => {}, triggerChange: () => {}, reapplyPending: async () => {}, drainPending: async () => {},
    subscribeStreams: async () => { passes.push(1); }, reach: () => {},
  });
  return { c, db, log, passes, world, close: () => db.close() };
}

/// A replica holding old rows and an anchor, the position ahead of the new chain's cuts:
/// the follower the fence exists for.
function follower(path: string, world: World, seededOthers: string[] = []) {
  const t = client(path, world);
  for (const o of seededOthers) t.db.exec(`INSERT INTO _zebridge_generations VALUES ('${o}', '${VERSION(1)}', 1, 0, 470, '${OWN}', 70)`);
  t.db.exec(`
    INSERT INTO item VALUES (91,'acme','old','${VERSION(1)}'),(92,'acme','old','${VERSION(1)}'),(93,'acme','old','${VERSION(1)}');
    INSERT INTO _zebridge_stream_seq VALUES ('${OWN}', 500, 'own-1'), ('${PUB}', 90, 'pub-1');
    INSERT INTO _zebridge_generations VALUES ('item', '${VERSION(1)}', 1, 0, 480, '${OWN}', 80);
  `);
  t.close();
}

const rows = (db: DatabaseSync) => db.prepare('SELECT id, note FROM item ORDER BY id').all().map((r) => `${r.id}:${r.note}`);
const pos = (db: DatabaseSync, s: string) => db.prepare('SELECT last_seq FROM _zebridge_stream_seq WHERE stream = ?').get(s)?.last_seq;
const anchor = (db: DatabaseSync) => db.prepare('SELECT seed_seq, seed_stream, shared_seed_seq FROM _zebridge_generations WHERE tbl = ?').get('item');
const owed = (db: DatabaseSync) => (db.prepare('SELECT count(*) AS n FROM _zebridge_fence').get() as any).n;
const wanted = (gen: number, n: number) => Array.from({ length: n }, (_, i) => `${i + 1}:new-${gen}`);

async function withDir(fn: (path: string) => Promise<void>) {
  await loadCore(readFileSync(new URL('../wasm/zb_core.wasm', import.meta.url)));
  const dir = mkdtempSync(join(tmpdir(), 'zb-fence-'));
  try { await fn(join(dir, 'replica.db')); } finally { rmSync(dir, { recursive: true, force: true }); }
}

/// The process dies: a thrown fault, nothing in memory carried over.
const crash = (at: string): Hook => (point) => { if (point === at) throw new Error(`crash at ${point}`); };

test('a chain cut behind the position fences: rows replaced, cursors at the cuts, anchor committed, nothing owed', async () => {
  for (const inPass of [false, true]) await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    const t = client(path, w);
    try {
      const stops: string[] = [];
      t.c.tailStops.set(OWN, async () => { stops.push(OWN); });
      t.c.tailStops.set(PUB, async () => { stops.push(PUB); });
      assert.equal(await t.c.applyGenerations('item', inPass), true);
      assert.deepEqual(rows(t.db), wanted(2, 5), 'the old rows are gone, the chain\'s are in');
      assert.equal(pos(t.db, OWN), 300, 'the own cursor moved back to the cut');
      assert.equal(pos(t.db, PUB), 40, 'the shared cursor too: it was past the shared cut');
      assert.deepEqual({ ...anchor(t.db) }, { seed_seq: 300, seed_stream: OWN, shared_seed_seq: 40 });
      assert.equal(owed(t.db), 0, 'the owed record is removed with the anchor');
      assert.deepEqual([...stops].sort(), [OWN, PUB].sort(), 'the tails on both routes were stopped first');
      assert.equal(t.c.fenced.size, 0, 'the routes are released');
      assert.equal(t.c.syncedTables.get('item').unseeded, false);
      assert.equal(t.c.syncedTables.get('item').seedSeq, 300);
      assert.equal(t.c.globalSyncState.seq[OWN], 300);
      assert.ok(t.log.some((m) => /fence prepared — chain g2 cut at 300/.test(m)));
      assert.ok(t.log.some((m) => /fenced — chain g2/.test(m)));
      // From a poll loop the tails are reopened by a pass of their own; from a pass, by its step 2.
      assert.equal(t.passes.length, inPass ? 0 : 1);
    } finally { t.close(); }
  });
});

test('a chain that does not predate the position is no fence: anchor committed, cursors untouched', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 600, 150); // ahead of the follower (500 / 90)
    follower(path, w);
    const t = client(path, w);
    try {
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.deepEqual(rows(t.db), wanted(2, 5));
      assert.equal(pos(t.db, OWN), 500, 'the position stays: the tail forwards it, not the seed');
      assert.equal(owed(t.db), 0);
      assert.ok(!t.log.some((m) => /fence prepared/.test(m)));
      assert.deepEqual({ ...anchor(t.db) }, { seed_seq: 600, seed_stream: OWN, shared_seed_seq: 150 });
    } finally { t.close(); }
  });
});

test('crash after a committed chunk: unseeded, cursor at the cut, the fence owed; reopened, it recovers', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    let first = true;
    const t = client(path, w, { hook: (point) => { if (point === 'after_chunk' && first) { first = false; throw new Error('crash at after_chunk'); } } });
    await assert.rejects(() => t.c.applyGenerations('item'), /crash at after_chunk/);
    // The process is gone. What is on disk:
    assert.equal(anchor(t.db), undefined, 'no anchor: the table is unseeded');
    assert.equal(owed(t.db), 1, 'the fence is owed');
    assert.equal(pos(t.db, OWN), 300, 'the cursor sits at the cut');
    assert.equal(pos(t.db, PUB), 40);
    assert.deepEqual(rows(t.db), ['1:new-2', '2:new-2'], 'part of the replacement committed, the old rows are gone: incomplete, and said so by the missing anchor');
    t.close();
    // Reopened: the cursor is AT the cut, so the chain no longer "predates" — a fence only because it is owed.
    const r = client(path, w);
    try {
      assert.equal(await r.c.fenceOwed('item'), true);
      assert.equal(await r.c.applyGenerations('item'), true);
      assert.deepEqual(rows(r.db), wanted(2, 5));
      assert.equal(owed(r.db), 0);
      assert.deepEqual({ ...anchor(r.db) }, { seed_seq: 300, seed_stream: OWN, shared_seed_seq: 40 });
      assert.ok(r.log.some((m) => /fence prepared/.test(m)), 'the retry is a fence again');
    } finally { r.close(); }
  });
});

test('crash before the anchor commits: rows replaced, no anchor, cursor at the cut; reopened, it recovers', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    const t = client(path, w, { hook: crash('before_commit') });
    await assert.rejects(() => t.c.applyGenerations('item'), /crash at before_commit/);
    assert.deepEqual(rows(t.db), wanted(2, 5), 'the replacement finished');
    assert.equal(anchor(t.db), undefined, 'but no anchor: the commit never happened');
    assert.equal(owed(t.db), 1);
    assert.equal(pos(t.db, OWN), 300);
    t.close();
    const r = client(path, w);
    try {
      assert.equal(await r.c.applyGenerations('item'), true);
      assert.equal(owed(r.db), 0);
      assert.equal((anchor(r.db) as any).seed_seq, 300);
    } finally { r.close(); }
  });
});

test('retention overtaking a staged fence: not committed, still owed, parked; a newer chain recovers it', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    // While the rows are replaced, the stream is pruned past the cut.
    const t = client(path, w, { hook: (point) => { if (point === 'before_verify') w.streams[OWN]!.first = 305; } });
    try {
      assert.equal(await t.c.applyGenerations('item'), false);
      assert.ok(t.log.some((m) => /fence not committed — CDC_acme no longer holds the events after the cut 300/.test(m)));
      assert.equal(anchor(t.db), undefined, 'no anchor');
      assert.equal(owed(t.db), 1, 'the fence is still owed');
      assert.equal(pos(t.db, OWN), 300, 'the cursor stays at the cut');
      assert.equal(t.c.syncedTables.get('item').unseeded, true, 'its events are held');
      assert.equal(t.c.fenced.get(OWN), 'item', 'the route stays parked: no tail meanwhile');
      // The old chain is now behind the stream: nothing to do but wait for a newer one.
      assert.equal(await t.c.applyGenerations('item'), false);
      assert.ok(t.log.some((m) => /predates the stream/.test(m)));
      // A newer chain, cut inside what the stream holds.
      w.publish(3, 6, 350);
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.deepEqual(rows(t.db), wanted(3, 6));
      assert.equal(owed(t.db), 0);
      assert.equal((anchor(t.db) as any).seed_seq, 350);
      assert.equal(t.c.fenced.size, 0, 'released with the commit');
    } finally { t.close(); }
  });
});

test('two attempts: a crash after a chunk, then the retry overtaken — still owed, recovered from a newer chain', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    const a = client(path, w, { hook: crash('after_chunk') });
    await assert.rejects(() => a.c.applyGenerations('item'), /crash/);
    a.close();
    // Attempt 2: the reopened client stages the same fence and the stream moves past the cut.
    const b = client(path, w, { hook: (point) => { if (point === 'before_verify') w.streams[OWN]!.first = 310; } });
    try {
      assert.equal(await b.c.applyGenerations('item'), false);
      assert.ok(b.log.some((m) => /fence prepared/.test(m)), 'attempt 2 is a fence again');
      assert.ok(b.log.some((m) => /fence not committed/.test(m)));
      assert.equal(owed(b.db), 1);
      assert.equal(anchor(b.db), undefined);
      assert.equal(pos(b.db, OWN), 300);
      w.publish(3, 4, 400);
      assert.equal(await b.c.applyGenerations('item'), true);
      assert.equal(owed(b.db), 0);
      assert.deepEqual(rows(b.db), wanted(3, 4));
    } finally { b.close(); }
  });
});

test('a shared route that cannot be read refuses the fence: nothing prepared, the old state intact', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    const t = client(path, w, { hook: (point) => { if (point === 'shared_lookup') throw new Error('unreadable'); } });
    try {
      assert.equal(await t.c.applyGenerations('item'), false);
      assert.ok(t.log.some((m) => /its coverage on CDC_PUBLIC is unknown/.test(m)));
      assert.ok(!t.log.some((m) => /fence prepared/.test(m)));
      assert.equal(owed(t.db), 0, 'no obligation was created');
      assert.deepEqual(rows(t.db), ['91:old', '92:old', '93:old'], 'the rows are untouched');
      assert.equal(pos(t.db, OWN), 500);
      assert.equal(pos(t.db, PUB), 90);
      assert.equal((anchor(t.db) as any).seed_seq, 480, 'the anchor is kept');
      assert.equal(t.c.fenced.size, 0);
    } finally { t.close(); }
    // The route readable again: recovered.
    const r = client(path, w);
    try {
      assert.equal(await r.c.applyGenerations('item'), true);
      assert.deepEqual(rows(r.db), wanted(2, 5));
    } finally { r.close(); }
  });
});

test('another table on the routes that is not seeded: the old refusal (D2) stands and nothing changes', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w); // orders has no anchor: its coverage is unknown
    const t = client(path, w, { others: ['orders'] });
    try {
      assert.equal(await t.c.applyGenerations('item'), false);
      assert.ok(t.log.some((m) => /refusing the full \(D2\): orders is not seeded, so its coverage on CDC_acme is unknown/.test(m)));
      assert.equal(owed(t.db), 0);
      assert.deepEqual(rows(t.db), ['91:old', '92:old', '93:old']);
      assert.equal(pos(t.db, OWN), 500);
      assert.equal(frontierRows(t.db).length, 0);
    } finally { t.close(); }
  });
});

const frontierRows = (db: DatabaseSync) => db.prepare('SELECT tbl, stream, created, seq FROM _zebridge_frontier ORDER BY tbl, stream').all().map((r) => ({ ...r }));
const ev = (table: string, id: number, stream: string, seq: number, note: string, ver = 30) =>
  ({ table, operation: 'INSERT', data: { id, tenant_id: 'acme', note, updated_at: VERSION(ver) }, stream, seq, lsn: 0 });

test('another seeded table on the routes stays untouched: its frontier is written with the cursors, the events it holds are skipped on replay', async () => {
  for (const planned of [false, true]) await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w, ['orders']);
    const t = client(path, w, { others: ['orders'] });
    try {
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.deepEqual(rows(t.db), wanted(2, 5));
      assert.equal(pos(t.db, OWN), 300);
      // The fenced table is replaced; the untouched one keeps its rows and gets a frontier at
      // each rewound route's position as it was, on the identity the chain was cut on.
      assert.deepEqual(frontierRows(t.db), [
        { tbl: 'orders', stream: PUB, created: 'pub-1', seq: 90 },
        { tbl: 'orders', stream: OWN, created: 'own-1', seq: 500 },
      ]);
      assert.deepEqual([...t.c.frontiers.get('orders')!].sort((a, b) => a.seq - b.seq), [{ stream: PUB, seq: 90 }, { stream: OWN, seq: 500 }]);
      assert.equal(t.c.frontiers.has('item'), false, 'the fenced table has none of its own');
      assert.ok(t.log.some((m) => /keep their applied frontier/.test(m)));
      // The replay: orders already holds everything up to 500 on the own route, 90 on the shared one.
      const replay = [ev('orders', 1, OWN, 301, 'held'), ev('orders', 2, OWN, 500, 'held'), ev('orders', 3, OWN, 501, 'new'), ev('orders', 4, PUB, 90, 'held'), ev('orders', 5, PUB, 91, 'new')];
      if (planned) await t.c.applyBatchPlanned(replay.map((e) => ({ table: 'orders', ev: e })), t.c.run);
      else for (const e of replay) await t.c.applyEvent('orders', e);
      assert.deepEqual(t.db.prepare('SELECT id, note FROM orders ORDER BY id').all().map((r) => `${r.id}:${r.note}`), ['3:new', '5:new'], 'only past its frontier does the table take events');
      assert.equal(t.c.frontierGated, 3);
      // The fenced table itself replays its events (it has no frontier): a replayed event lands.
      await t.c.applyEvent('item', ev('item', 9, OWN, 305, 'replayed', 40));
      assert.equal(t.db.prepare('SELECT note FROM item WHERE id = 9').get()?.note, 'replayed');
    } finally { t.close(); }
  });
});

test('a frontier is spent when the position passes it, and gone from the replica with it', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w, ['orders']);
    const t = client(path, w, { others: ['orders'] });
    try {
      assert.equal(await t.c.applyGenerations('item'), true);
      await t.c.advanceGlobal(t.c.run, 0, OWN, 499);
      assert.equal(frontierRows(t.db).length, 2, 'below the frontier: kept');
      await t.c.advanceGlobal(t.c.run, 0, OWN, 500);
      assert.deepEqual(frontierRows(t.db).map((r) => r.stream), [PUB], 'the own route\'s is spent, the shared one is another stream');
      assert.deepEqual(t.c.frontiers.get('orders'), [{ stream: PUB, seq: 90 }]);
      await t.c.advanceGlobal(t.c.run, 0, PUB, 120);
      assert.equal(frontierRows(t.db).length, 0);
      assert.equal(t.c.frontiers.has('orders'), false);
    } finally { t.close(); }
  });
});

test('the frontier survives a crash after PREPARE and is read back by the reopened client; a stream recreated since kills it', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w, ['orders']);
    const t = client(path, w, { others: ['orders'], hook: crash('after_chunk') });
    try {
      await assert.rejects(() => t.c.applyGenerations('item'), /crash at after_chunk/);
      assert.equal(owed(t.db), 1);
      assert.equal(pos(t.db, OWN), 300);
    } finally { t.close(); }
    // Reopened: the frontier is above the cursor it guards, still there to skip the replay.
    const r = client(path, w, { others: ['orders'] });
    try {
      await r.c.loadFrontiers();
      assert.deepEqual([...r.c.frontiers.get('orders')].sort((a, b) => a.seq - b.seq), [{ stream: PUB, seq: 90 }, { stream: OWN, seq: 500 }]);
      assert.equal(await r.c.applyGenerations('item'), true, 'the owed fence recovers');
      assert.equal(owed(r.db), 0);
      assert.deepEqual(frontierRows(r.db).map((x) => Number(x.seq)).sort((a, b) => a - b), [90, 500], 'the retry (its cursor at the cut) does not lower them');
    } finally { r.close(); }
    // Another incarnation of the route since: what was taken on the old one means nothing.
    const g = client(path, w, { others: ['orders'] });
    try {
      g.db.exec(`UPDATE _zebridge_stream_seq SET created = 'own-2' WHERE stream = '${OWN}'`);
      g.c.streamCreated.set(OWN, 'own-2');
      await g.c.loadFrontiers();
      assert.deepEqual(g.c.frontiers.get('orders'), [{ stream: PUB, seq: 90 }]);
      assert.deepEqual(frontierRows(g.db).map((x) => x.stream), [PUB], 'the dead row is deleted');
    } finally { g.close(); }
  });
});

test('a stream whose position was stored on another incarnation than the chain\'s cut: the other table cannot stay untouched, the fence is refused', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w, ['orders']);
    const t = client(path, w, { others: ['orders'] });
    try {
      t.c.streamCreated.set(OWN, 'own-0'); // the stored position's identity is not the manifest's
      assert.equal(await t.c.applyGenerations('item'), false);
      assert.ok(t.log.some((m) => /orders stays untouched on CDC_acme only if its position and the chain's cut were taken on the same stream incarnation/.test(m)));
      assert.equal(owed(t.db), 0);
      assert.equal(pos(t.db, OWN), 500);
      assert.deepEqual(rows(t.db), ['91:old', '92:old', '93:old']);
    } finally { t.close(); }
  });
});

test('a chain applied to a table drops the frontier it held before its first step', async () => {
  for (const gone of [false, true]) await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 600); // cut past the position: no fence, a plain seed
    if (gone) w.objects.delete(w.manifest.full.object); // pruned under us: no step is ever applied
    follower(path, w);
    const t = client(path, w);
    // A frontier a fence on another table left for this one.
    t.db.exec(`INSERT INTO _zebridge_frontier VALUES ('item', '${OWN}', 'own-1', 520)`);
    t.c.frontiers.set('item', [{ stream: OWN, seq: 520 }]);
    try {
      assert.equal(await t.c.applyGenerations('item'), !gone);
      assert.deepEqual(rows(t.db), gone ? ['91:old', '92:old', '93:old'] : wanted(2, 5));
      assert.equal(frontierRows(t.db).length, 0, 'gone from the replica');
      assert.equal(t.c.frontiers.has('item'), false);
    } finally { t.close(); }
  });
});

test('an on-demand table on the route does not count as a dependent', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    const t = client(path, w, { others: ['catalogue'] });
    try {
      t.c.ondemandSet.add('catalogue');
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.equal(owed(t.db), 0);
    } finally { t.close(); }
  });
});

test('a fence is not taken from a leaf\'s chain: refused, nothing prepared, the hub is asked', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    const t = client(path, w);
    try {
      t.c.config.seedDomain = 'leaf';
      t.c.config.jsDomain = 'hub';
      const seen: string[] = [];
      const real = t.c.transport.kv;
      t.c.transport.kv = async (nc: unknown, bucket: string, o: unknown, jsOpts: any) => { seen.push(jsOpts?.domain ?? 'hub'); return real(nc, bucket, o, jsOpts); };
      assert.equal(await t.c.applyGenerations('item'), true, 'the leaf attempt is refused, the hub attempt is fenced');
      assert.deepEqual(seen.slice(0, 2), ['leaf', 'hub']);
      assert.ok(t.log.some((m) => /not fencing from a leaf's chain/.test(m)));
      assert.equal(owed(t.db), 0);
      assert.deepEqual(rows(t.db), wanted(2, 5));
    } finally { t.close(); }
  });
});

test('a chain object pruned under the fence: the manifest is re-read and its proofs taken again', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    // The first manifest names an object that is gone; the re-read finds a newer chain.
    const gone = w.manifest;
    w.objects.delete(gone.full.object);
    const t = client(path, w);
    const real = t.c.transport.kv;
    let reads = 0;
    t.c.transport.kv = async (...a: any[]) => {
      const kv = await real(...a);
      return { get: async (k: string) => { if (++reads === 2) w.publish(3, 4, 350); return kv.get(k); } };
    };
    try {
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.ok(reads >= 2, 'the manifest was read again');
      assert.deepEqual(rows(t.db), wanted(3, 4));
      assert.equal(owed(t.db), 0);
      assert.equal((anchor(t.db) as any).seed_seq, 350, 'anchored on the re-read chain\'s cut, not the first one\'s');
    } finally { t.close(); }
  });
});

test('PREPARE is one transaction: a failure in it leaves the cursor, the anchor, the rows and the routes as they were', async () => {
  for (const inPass of [false, true]) await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    const t = client(path, w);
    // The cursor write fails inside PREPARE.
    const run = t.c.run;
    let failing = true;
    t.c.transaction = async (fn: any) => {
      t.db.exec('BEGIN');
      try {
        await fn(async (sql: string, ...args: any[]) => {
          if (failing && /INSERT INTO _zebridge_stream_seq/.test(sql)) throw new Error('disk full');
          return run(sql, ...args);
        });
        t.db.exec('COMMIT');
      } catch (e) { t.db.exec('ROLLBACK'); throw e; }
    };
    const stops: string[] = [];
    for (const s of [OWN, PUB]) t.c.tailStops.set(s, async () => { stops.push(s); });
    try {
      await assert.rejects(() => t.c.applyGenerations('item', inPass), /disk full/);
      assert.equal(owed(t.db), 0, 'no obligation without the rest of PREPARE');
      assert.equal((anchor(t.db) as any).seed_seq, 480, 'the anchor is still there');
      assert.equal(pos(t.db, OWN), 500);
      assert.deepEqual(rows(t.db), ['91:old', '92:old', '93:old']);
      // The memory the transaction does not own: no route is left parked for a fence that
      // was never prepared, the in-memory position and anchor are untouched, and the
      // tails that were stopped for it come back (a pass reopens its own in its step 2).
      assert.equal(t.c.fenced.size, 0, 'no route stays parked');
      assert.equal(t.c.globalSyncState.seq[OWN], 500);
      assert.equal(t.c.syncedTables.get('item').unseeded, undefined, 'the table was not marked unseeded');
      assert.deepEqual([...stops].sort(), [OWN, PUB].sort(), 'the tails were stopped for the PREPARE');
      assert.equal(t.passes.length, inPass ? 0 : 1, 'a poll loop\'s failed PREPARE asks for the tails to be reopened');
      // And the next attempt, the disk fixed, is the fence it was meant to be.
      failing = false;
      assert.equal(await t.c.applyGenerations('item', inPass), true);
      assert.deepEqual(rows(t.db), wanted(2, 5));
      assert.equal(pos(t.db, OWN), 300);
      assert.equal(owed(t.db), 0);
      assert.equal(t.c.fenced.size, 0);
    } finally { t.close(); }
  });
});

test('a failed PREPARE gives back a parking an earlier fence of the table still holds', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    const t = client(path, w);
    // An earlier fence of this table, interrupted: the own route is parked for it.
    t.c.fenced.set(OWN, 'item');
    t.c.transaction = async () => { throw new Error('disk full'); };
    try {
      await assert.rejects(() => t.c.applyGenerations('item', true), /disk full/);
      assert.deepEqual([...t.c.fenced], [[OWN, 'item']], 'its parking stays; only this attempt\'s own is undone');
    } finally { t.close(); }
  });
});

/// A deferred, to hold a step at a chosen point.
const deferred = () => { let go!: () => void; const p = new Promise<void>((r) => { go = r; }); return { p, go }; };
const within = <T>(ms: number, p: Promise<T>, what: string) =>
  Promise.race([p, new Promise<never>((_, rej) => setTimeout(() => rej(new Error(`deadlock: ${what} still pending after ${ms} ms`)), ms))]);

test('a stream pass that waits for a background fence does not deadlock on the stream queue (the op reaches PREPARE after the pass joined it)', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    const t = client(path, w);
    try {
      // The background op (a poll loop's applyGenerations) is held just before its decision.
      const atDecision = deferred();
      const release = deferred();
      const owedOrig = t.c.fenceOwed;
      t.c.fenceOwed = async (tb: string) => { atDecision.go(); await release.p; return owedOrig.call(t.c, tb); };
      const background = t.c.applyGenerations('item');
      await atDecision.p;
      // A pass now holds the stream queue and wants the same table seeded: it joins the op.
      const pass = t.c.changeStreams(async () => { assert.equal(await t.c.applyGenerations('item', true), true); });
      await new Promise((r) => setImmediate(r));
      release.go();
      await within(2000, pass, 'the pass');
      assert.equal(await within(2000, background, 'the background op'), true);
      assert.deepEqual(rows(t.db), wanted(2, 5));
      assert.equal(pos(t.db, OWN), 300);
      assert.equal(owed(t.db), 0);
      assert.equal(t.c.fenced.size, 0);
    } finally { t.close(); }
  });
});

test('a stream pass that joins a background fence already queued for PREPARE lends it the queue', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w);
    const t = client(path, w);
    try {
      // The pass holds the queue first (busy with something else), and the background op
      // reaches PREPARE meanwhile: it queues behind the pass.
      const joinNow = deferred();
      const pass = t.c.changeStreams(async () => {
        await joinNow.p;
        assert.equal(await t.c.applyGenerations('item', true), true);
      });
      const background = t.c.applyGenerations('item');
      // Let the background op get as far as its queued PREPARE (it needs nothing the pass holds before that).
      await new Promise((r) => setTimeout(r, 50));
      assert.equal(typeof t.c.seedInFlight.get('item')?.run.lend, 'function', 'the op waits in the queue for its turn');
      assert.equal(owed(t.db), 0, 'the fence is still waiting for the queue: nothing prepared yet');
      joinNow.go();
      await within(2000, pass, 'the pass');
      assert.equal(await within(2000, background, 'the background op'), true);
      assert.deepEqual(rows(t.db), wanted(2, 5));
      assert.equal(pos(t.db, OWN), 300);
      assert.equal(owed(t.db), 0);
      assert.equal(t.c.fenced.size, 0);
    } finally { t.close(); }
  });
});

/// Two fences at once on one route: the item chain cut at 100 and the orders chain cut at 200,
/// the position at 500. `inPass`: both run as a stream pass runs its tables (concurrently, under
/// the queue it holds); otherwise as two poll loops (both queued).
for (const inPass of [false, true]) {
  test(`overlapping fences on one route (${inPass ? 'in a pass' : 'background'}): the later cut never moves the cursor forward`, async () => {
    await withDir(async (path) => {
      const w = new World();
      w.publish(2, 5, 100, 40, 'item');
      w.publish(2, 4, 200, 40, 'orders');
      follower(path, w, ['orders']);
      let arrived = 0;
      const both = deferred();
      // Both are decided against the same position unless the decision is taken again inside.
      const t = client(path, w, { others: ['orders'], hook: async (point) => {
        if (point !== 'before_decision') return;
        if (++arrived === 2) both.go();
        await both.p;
      } });
      try {
        const low = t.c.applyGenerations('item', inPass);
        const high = t.c.applyGenerations('orders', inPass);
        const [a, b] = await within(5000, Promise.all([low, high]), 'the two fences');
        assert.equal(a, true);
        assert.equal(b, true, 'the later cut is a plain seed: it does not predate the position the earlier fence left');
        assert.equal(pos(t.db, OWN), 100, 'the cursor is at the LOWER cut: the table seeded at 100 needs 101..200');
        assert.equal(t.c.globalSyncState.seq[OWN], 100);
        assert.deepEqual(rows(t.db), wanted(2, 5));
        assert.equal(owed(t.db), 0);
        assert.equal(t.c.fenced.size, 0);
        assert.deepEqual(t.db.prepare('SELECT tbl, seed_seq FROM _zebridge_generations ORDER BY tbl').all().map((r) => `${r.tbl}:${r.seed_seq}`), ['item:100', 'orders:200']);
      } finally { t.close(); }
    });
  });

  test(`overlapping fences on one route (${inPass ? 'in a pass' : 'background'}): a fence behind a table still being fenced waits, and takes its turn after`, async () => {
    await withDir(async (path) => {
      const w = new World();
      w.publish(2, 5, 100, 40, 'item');
      w.publish(2, 4, 200, 40, 'orders');
      follower(path, w, ['orders']);
      let arrived = 0;
      const both = deferred();
      const t = client(path, w, { others: ['orders'], hook: async (point) => {
        if (point !== 'before_decision') return;
        if (++arrived === 2) both.go();
        await both.p;
      } });
      try {
        // orders first (cut 200), then item (cut 100, behind the position orders left at 200).
        const high = t.c.applyGenerations('orders', inPass);
        const low = t.c.applyGenerations('item', inPass);
        await within(5000, Promise.all([high, low]), 'the two fences');
        // Whatever the interleaving, the cursor is at the lowest cut and the owed record is clear
        // once both have been through; a refused one is asked again.
        if (!(await t.c.applyGenerations('item', inPass))) assert.fail('the retry is a fence');
        assert.equal(pos(t.db, OWN), 100);
        assert.equal(owed(t.db), 0);
        assert.equal(t.c.fenced.size, 0);
        assert.equal(t.db.prepare('SELECT count(*) AS n FROM _zebridge_generations').get()?.n, 2, 'both tables are anchored');
      } finally { t.close(); }
    });
  });
}

test('frontier load fails closed: a failed read keeps what memory had, and throws', async () => {
  await withDir(async (path) => {
    const w = new World();
    follower(path, w, ['orders']);
    const t = client(path, w, { others: ['orders'] });
    try {
      t.db.exec(`INSERT INTO _zebridge_frontier VALUES ('orders', '${OWN}', 'own-1', 500)`);
      await t.c.loadFrontiers();
      assert.deepEqual(t.c.frontiers.get('orders'), [{ stream: OWN, seq: 500 }]);
      const real = t.c.run;
      t.c.run = async (sql: string, ...a: any[]) => { if (/FROM _zebridge_frontier f JOIN/.test(sql)) throw new Error('read failed'); return real(sql, ...a); };
      await assert.rejects(() => t.c.loadFrontiers(), /read failed/);
      assert.deepEqual(t.c.frontiers.get('orders'), [{ stream: OWN, seq: 500 }], 'not "no frontier"');
    } finally { t.close(); }
  });
});

test('a PREPARE that committed but could not read its frontiers back leaves the routes parked; the owed fence\'s retry restores them', async () => {
  for (const inPass of [false, true]) await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300);
    follower(path, w, ['orders']);
    const t = client(path, w, { others: ['orders'] });
    try {
      const real = t.c.run;
      let failing = true;
      t.c.run = async (sql: string, ...a: any[]) => { if (failing && /FROM _zebridge_frontier f JOIN/.test(sql)) throw new Error('read failed'); return real(sql, ...a); };
      await assert.rejects(() => t.c.applyGenerations('item', inPass), /read failed/);
      // Durable: the cursors, the owed record and the frontiers; memory: the table unseeded, the
      // routes parked — no pass or tail applies a replayed event to a table that already holds it.
      assert.equal(owed(t.db), 1);
      assert.equal(pos(t.db, OWN), 300);
      assert.equal(frontierRows(t.db).length, 2);
      assert.equal(t.c.syncedTables.get('item').unseeded, true);
      assert.deepEqual([...t.c.fenced].sort(), [[OWN, 'item'], [PUB, 'item']].sort());
      assert.ok(t.log.some((m) => /could not be read back/.test(m)));
      // The retry (the fence is still owed) reads them, completes, and releases the routes.
      failing = false;
      assert.equal(await t.c.applyGenerations('item', inPass), true);
      assert.equal(t.c.fenced.size, 0);
      assert.equal(owed(t.db), 0);
      assert.deepEqual([...t.c.frontiers.get('orders')].sort((a: any, b: any) => a.seq - b.seq), [{ stream: PUB, seq: 90 }, { stream: OWN, seq: 500 }]);
      await t.c.applyEvent('orders', ev('orders', 1, OWN, 301, 'held'));
      assert.equal(t.db.prepare('SELECT count(*) AS n FROM orders').get()?.n, 0, 'the covered event is not applied');
    } finally { t.close(); }
  });
});

test('dropping a table\'s frontier fails closed: the chain is not applied over a frontier that could not be dropped', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 600); // a plain seed
    follower(path, w);
    const t = client(path, w);
    t.db.exec(`INSERT INTO _zebridge_frontier VALUES ('item', '${OWN}', 'own-1', 520)`);
    t.c.frontiers.set('item', [{ stream: OWN, seq: 520 }]);
    try {
      const real = t.c.run;
      t.c.run = async (sql: string, ...a: any[]) => { if (/^DELETE FROM _zebridge_frontier WHERE tbl = \?/.test(sql.trim())) throw new Error('disk full'); return real(sql, ...a); };
      await assert.rejects(() => t.c.applyGenerations('item'), /disk full/);
      assert.deepEqual(rows(t.db), ['91:old', '92:old', '93:old'], 'no step ran');
      assert.deepEqual(t.c.frontiers.get('item'), [{ stream: OWN, seq: 520 }], 'memory and replica still agree');
      assert.equal(frontierRows(t.db).length, 1);
    } finally { t.close(); }
  });
});
