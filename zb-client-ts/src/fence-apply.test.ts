// The fence through the real apply path (libzb.ts applyGenerationsFrom): a chain cut BEHIND
// the replica's position replaces the rows and moves the cursors back to its cuts, in the
// recovery model's order — PREPARE, replace, VERIFY, COMMIT. The same cases libzb's
// recreate_recovery.py check 4 runs live, here deterministically: SQLite on a file (a
// "crash" is a thrown fault and a new client on the same file), a stubbed NATS.
import { test, mock } from 'node:test';
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
  /// A seeding leaf's mirrors, when the test has one: its own manifests and objects.
  leaf: World | null = null;
  view(domain?: string): World { return domain === 'leaf' && this.leaf ? this.leaf : this; }
  /// A chain object read waits for this (a slow download), per object name.
  hold?: (name: string) => Promise<void> | undefined;
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
    CREATE TABLE IF NOT EXISTS _zebridge_fence (tbl TEXT PRIMARY KEY, joint INTEGER);
    CREATE TABLE IF NOT EXISTS _zebridge_joint_done (tbl TEXT NOT NULL, stream TEXT NOT NULL, cut INTEGER NOT NULL, created TEXT NOT NULL, PRIMARY KEY (tbl, stream));
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
    globalSyncState: { seq }, tailGen: new Map(), tailStops: new Map(), tailFilters: new Map(), fenced: new Map(), replacing: new Map(), jointRestored: false,
    streamWork: Promise.resolve(), streamsSubscribed: true, restarted: new Set(), failed: new Set(), hubOnly: new Set(),
    applyingByChoice: new Set(), healedRoutes: new Set(), streamCreated, streamBornEmpty: new Set(), fkHeld: [], hlcFloor: '', frontiers: new Map(), frontierGated: 0,
    lateSeeds: new Set<string>(), reseedKicks: new Map(), fkHolds: 0, scheduleRecount: () => {},
    waits: [] as string[], waitForChain(tb: string) { (this as any).waits.push(tb); }, pendingWrites: new Map(), rebase: new Map(), bulkStats: { single: 0, statements: 0, bulked: 0, fallbacks: 0 },
    measure: new Measure(() => {}), dialect: sqliteDialect, storage: {}, platform: {},
    config: {
      principal: 'p', seedChunkRows: 2, bulkStatement: 'json_each', faultHook: opts.hook ? (point: string) => opts.hook!(point, c) : undefined,
      grammar: {
        generations: { kv: 'generations', bucket_prefix: 'gen-' }, kv: { tenants: 'tenants' }, streams: { cdc: 'CDC' },
        cdc_streams: { public: PUB, tenant_prefix: 'CDC_' }, open_tenant: '_default',
      },
    },
    transport: {
      kv: async (_nc: unknown, _bucket: string, _o: unknown, jsOpts: any) => ({ get: async (key: string) => {
        const m = world.view(jsOpts?.domain).manifests[String(key).split('.').pop() as string];
        return m ? { operation: 'PUT', value: new TextEncoder().encode(JSON.stringify(m)) } : null;
      } }),
      objectStore: async (_nc: unknown, _bucket: string, jsOpts: any) => ({
        info: async (name: string) => {
          const o = world.view(jsOpts?.domain).objects.get(name);
          return o ? { size: o.length, deleted: false } : null;
        },
      }),
      jetstreamManager: async () => ({ streams: { info: async (name: string) => {
        const st = world.streams[name];
        if (!st) throw new Error(`no stream ${name}`);
        return { created: st.created, state: { first_seq: st.first, last_seq: st.last } };
      } } }),
    },
    jsOpts: () => ({ domain: 'hub' }), appendLog: (_t: string, m: string) => { log.push(m); },
    objectBlob: async (_os: unknown, _bucket: string, name: string, jsOpts?: any) => { await world.hold?.(name); return world.view(jsOpts?.domain).objects.get(name) ?? null; },
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
      assert.deepEqual([...t.c.fenced.get(OWN)], ['item'], 'the route stays parked: no tail meanwhile');
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
    w.publish(2, 5, 600, 150); // cut past the position: no fence, a plain seed
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

/// A client behind a seeding leaf: the leaf's mirrors hold `leafCut`'s chain (note `leaf`), the hub a
/// newer one (note `hub`). Records which domain every read went to.
function behindLeaf(path: string, leafCut: number, hubCut = 450) {
  const w = new World();
  w.leaf = new World();
  w.leaf.publish(2, 5, leafCut);
  w.publish(5, 4, hubCut);
  follower(path, w);
  const t = client(path, w);
  t.c.config.seedDomain = 'leaf';
  t.c.config.jsDomain = 'hub';
  const seen: string[] = [];
  const real = t.c.transport.kv;
  t.c.transport.kv = async (nc: unknown, bucket: string, o: unknown, jsOpts: any) => { seen.push(jsOpts?.domain ?? 'hub'); return real(nc, bucket, o, jsOpts); };
  return { w, t, seen };
}

test('a fence is taken from a seeding leaf\'s chain: the leaf\'s objects, the hub\'s proofs, the hub never asked for the chain', async () => {
  await withDir(async (path) => {
    const { t, seen } = behindLeaf(path, 300);
    try {
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.deepEqual(seen, ['leaf'], 'only the leaf was asked for a manifest');
      assert.deepEqual(rows(t.db), wanted(2, 5), 'the leaf\'s rows, though the hub has a newer chain (an older generation on a leaf stays usable)');
      assert.equal(pos(t.db, OWN), 300, 'the cursor is at the LEAF chain\'s cut: the events after it replay from the hub');
      assert.deepEqual({ ...anchor(t.db) }, { seed_seq: 300, seed_stream: OWN, shared_seed_seq: 40 });
      assert.equal(owed(t.db), 0);
      assert.ok(t.log.some((m) => /fence prepared — chain g2 cut at 300.*\[from the seeding leaf\]/.test(m)));
    } finally { t.close(); }
  });
});

test('a leaf whose manifest is ahead of its objects: nothing is prepared, the hub\'s chain is fenced', async () => {
  await withDir(async (path) => {
    const { w, t, seen } = behindLeaf(path, 300);
    try {
      w.leaf!.objects.clear(); // the manifest mirrored, the objects not yet
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.deepEqual(seen.slice(0, 2), ['leaf', 'hub']);
      assert.ok(t.log.some((m) => /the seeding leaf's manifest names item-i1-g2-full, which its cache does not hold yet/.test(m)));
      assert.ok(!t.log.some((m) => /\[from the seeding leaf\]/.test(m)));
      assert.deepEqual(rows(t.db), wanted(5, 4), 'the hub\'s chain');
      assert.equal(pos(t.db, OWN), 450);
      assert.equal(owed(t.db), 0);
    } finally { t.close(); }
  });
});

test('a leaf chain whose suffix the hub no longer holds is not fenced: the hub\'s chain is', async () => {
  await withDir(async (path) => {
    const { w, t } = behindLeaf(path, 300);
    try {
      w.streams[OWN]!.first = 400; // the hub's retention is past the leaf's cut + 1; the hub's own cut (450) is fine
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.ok(!t.log.some((m) => /\[from the seeding leaf\]/.test(m)), 'no fence prepared from the stale leaf chain');
      assert.deepEqual(rows(t.db), wanted(5, 4));
      assert.equal(pos(t.db, OWN), 450);
    } finally { t.close(); }
  });
});

test('a fence from a leaf interrupted after a chunk is owed: the reopened client recovers it, from the leaf', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.leaf = new World();
    w.leaf.publish(2, 5, 300);
    w.publish(5, 4, 450);
    follower(path, w);
    const first = client(path, w, { hook: crash('after_chunk') });
    first.c.config.seedDomain = 'leaf'; first.c.config.jsDomain = 'hub';
    try {
      await assert.rejects(() => first.c.applyGenerations('item'), /crash at after_chunk/);
      assert.equal(owed(first.db), 1);
      assert.equal(pos(first.db, OWN), 300);
    } finally { first.close(); }
    const second = client(path, w);
    second.c.config.seedDomain = 'leaf'; second.c.config.jsDomain = 'hub';
    try {
      assert.equal(await second.c.applyGenerations('item'), true, 'the retry is a fence again, with every proof');
      assert.equal(owed(second.db), 0);
      assert.deepEqual(rows(second.db), wanted(2, 5));
      assert.equal(pos(second.db, OWN), 300);
    } finally { second.close(); }
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
    t.c.fenced.set(OWN, new Set(['item']));
    t.c.transaction = async () => { throw new Error('disk full'); };
    try {
      await assert.rejects(() => t.c.applyGenerations('item', true), /disk full/);
      assert.deepEqual([...t.c.fenced].map(([k, v]) => [k, [...v]]), [[OWN, ['item']]], 'its parking stays; only this attempt\'s own is undone');
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
      assert.deepEqual([...t.c.fenced].map(([k, v]) => [k, [...v]]).sort(), [[OWN, ['item']], [PUB, ['item']]].sort());
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
    w.publish(2, 5, 600, 150); // a plain seed
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

/// Two chain replacements on one route whose downloads and commits overlap: while either is still
/// writing rows, no consumer may reopen on the route — a live event applied now is deleted by the
/// full, and the cursor, already past it, never replays it (a permanent hole).
for (const inPass of [false, true]) {
  const how = inPass ? 'in a pass' : 'background';
  const setup = async (path: string, plainFirst: boolean) => {
    const w = new World();
    // The fence: item cut at 100, behind the position 500. The other: a full that is a plain seed,
    // cut at 200 (decided after the fence moved the cursor to 100) or at 600 (ahead of the position).
    w.publish(2, 5, 100, 40, 'item');
    w.publish(2, 4, plainFirst ? 600 : 200, plainFirst ? 150 : 40, 'orders');
    follower(path, w, ['orders']);
    const slow = deferred();
    w.hold = (name) => (name.startsWith('orders') ? slow.p : undefined); // its download stays paused
    let arrived = 0;
    const both = deferred();
    const fail = { on: false };
    const t = client(path, w, { others: ['orders'], hook: async (point) => {
      if (point === 'after_chunk' && fail.on) throw new Error('crash at after_chunk');
      if (point !== 'before_decision') return;
      if (++arrived === 2) both.go();
      await both.p;
    } });
    return { w, t, slow, fail };
  };
  /// A real stream pass (step 2 opens the tails) against a stubbed hub, started and not awaited: a
  /// table it finds unseeded joins the replacement in flight and waits for it, as it would.
  const startPass = (t: any) => {
    let added = 0;
    const real = t.c.transport.jetstreamManager;
    t.c.transport.jetstreamManager = async (...a: any[]) => ({
      ...(await real(...a)),
      consumers: { add: async () => { added++; return { name: 'x', num_pending: 0 }; } },
    });
    t.c.transport.deliverPolicy = { byStartSequence: 'by_start_sequence', all: 'all' };
    t.c.transport.jetstream = () => ({ consumers: { get: async () => ({ consume: async () => ({ [Symbol.asyncIterator]() { return this; }, next: () => new Promise(() => {}), stop() {} }) }) } });
    t.c.subscribeStreamsOnce = (ZeBridge.prototype as any).subscribeStreamsOnce;
    const done = t.c.subscribeStreamsOnce().then(async () => { for (const stream of [...t.c.tailStops.keys()]) await t.c.stopTail(stream); });
    return { opened: () => added, done };
  };
  const parkedBy = (t: any, stream: string) => [...(t.c.fenced.get(stream) ?? [])].sort();

  test(`a fence that finishes while another chain replacement is still downloading does not release the route (${how}, the replacement converted to a plain seed)`, async () => {
    await withDir(async (path) => {
      const { t, slow } = await setup(path, false);
      try {
        const fence = t.c.applyGenerations('item', inPass);
        const plain = t.c.applyGenerations('orders', inPass);
        assert.equal(await within(5000, fence, 'the fence'), true, 'the fence is through: rows replaced, anchored');
        // orders is a plain seed now (its cut 200 is past the cursor 100 the fence left), its download paused.
        for (let i = 0; i < 50 && !t.c.replacing.get(OWN)?.has('orders'); i++) await new Promise((r) => setImmediate(r));
        assert.ok(t.c.replacing.get(OWN)?.has('orders'), 'the plain seed is recorded on the route');
        assert.equal(t.c.routeParked(OWN), true, 'the route stays parked: a consumer reopening at 101 would apply the live event the full then deletes');
        assert.deepEqual(parkedBy(t, OWN), ['orders']);
        assert.equal(t.c.routeParked(PUB), true, 'the shared route too');
        assert.equal(pos(t.db, OWN), 100);
        // A pass now — the poll loop's, the reconnect's — opens no consumer on either route.
        const pass = startPass(t);
        await new Promise((r) => setTimeout(r, 100));
        assert.equal(pass.opened(), 0, 'no consumer opens while the replacement writes rows');
        // The download completes: the replacement ends and only then the route opens (a poll loop asks for the tails).
        const before = t.passes.length;
        slow.go();
        assert.equal(await within(5000, plain, 'the plain seed'), true);
        await within(5000, pass.done, 'the pass');
        assert.equal(t.c.fenced.size, 0, 'released with the last owner');
        assert.equal(pass.opened(), 0, 'that pass found the routes parked and left them');
        const later = startPass(t);
        await within(5000, later.done, 'the next pass');
        assert.ok(later.opened() > 0, 'the next pass opens the consumers (the stub is not what held them back)');
        assert.equal(t.c.replacing.get(OWN)?.size ?? 0, 0);
        assert.equal(t.passes.length - before, inPass ? 0 : 1, 'the tails are asked for when the route opens');
        assert.equal(pos(t.db, OWN), 100, 'the cursor is still at the lower cut: nothing moved it meanwhile');
        assert.deepEqual(rows(t.db), wanted(2, 5));
      } finally { slow.go(); t.close(); }
    });
  });

  test(`a plain replacement that fails after its first chunk leaves the table owed and unseeded, and its retry recovers (${how})`, async () => {
    await withDir(async (path) => {
      const { w, t, slow, fail } = await setup(path, true);
      // orders holds a stable baseline the full's first chunk deletes.
      t.db.exec(`INSERT INTO orders VALUES (91,'acme','old','${VERSION(1)}'),(92,'acme','old','${VERSION(1)}'),(93,'acme','old','${VERSION(1)}')`);
      try {
        const plain = t.c.applyGenerations('orders', inPass); // decided plain (cut 600, ahead of the position)
        const fence = t.c.applyGenerations('item', inPass);
        assert.equal(await within(5000, fence, 'the fence'), true, 'the fence is through and has handed the parking to the plain replacement');
        assert.equal(t.c.routeParked(OWN), true);
        // The download completes, its first chunk commits (the baseline is deleted), the second fails.
        fail.on = true;
        slow.go();
        await assert.rejects(() => within(5000, plain, 'the plain seed'), /crash at after_chunk/);
        const owedTables = t.db.prepare('SELECT tbl FROM _zebridge_fence').all().map((r) => r.tbl);
        assert.deepEqual(owedTables, ['orders'], 'durably owed');
        assert.equal(t.db.prepare("SELECT count(*) AS n FROM _zebridge_generations WHERE tbl = 'orders'").get()?.n, 0, 'the old anchor is gone: orders is not seeded');
        assert.equal(t.db.prepare('SELECT count(*) AS n FROM orders WHERE note = \'old\'').get()?.n, 0, 'the baseline was deleted by the first chunk');
        assert.equal(t.c.syncedTables.get('orders').unseeded, true, 'its events are held, not applied to half-replaced rows');
        assert.deepEqual(t.c.waits, ['orders'], 'a retry loop runs for it');
        // The retry (what that loop does) is a fence retry with every proof, and completes it.
        fail.on = false;
        assert.equal(await t.c.applyGenerations('orders', false), true);
        assert.equal(t.db.prepare('SELECT count(*) AS n FROM _zebridge_fence').get()?.n, 0);
        assert.equal(t.db.prepare("SELECT count(*) AS n FROM _zebridge_generations WHERE tbl = 'orders'").get()?.n, 1, 'anchored again');
        assert.equal(t.db.prepare('SELECT count(*) AS n FROM orders').get()?.n, 4, 'the chain\'s rows');
        assert.equal(t.c.syncedTables.get('orders').unseeded, false);
        assert.equal(t.c.fenced.size, 0);
        assert.equal(pos(t.db, OWN), 100, 'the cursor stayed at the lower cut');
        void w;
      } finally { slow.go(); t.close(); }
    });
  });

  test(`a plain seed already downloading when a fence parks and commits on its route keeps the parking (${how})`, async () => {
    await withDir(async (path) => {
      const { t, slow } = await setup(path, true);
      try {
        const plain = t.c.applyGenerations('orders', inPass); // first: decided plain (cut 600, ahead of the position)
        const fence = t.c.applyGenerations('item', inPass);
        assert.equal(await within(5000, fence, 'the fence'), true);
        assert.equal(t.c.routeParked(OWN), true, 'the fence committed, the plain seed is still writing');
        assert.deepEqual(parkedBy(t, OWN), ['orders']);
        const pass = startPass(t);
        await new Promise((r) => setTimeout(r, 100));
        assert.equal(pass.opened(), 0, 'no consumer opens while the replacement writes rows');
        assert.equal(pos(t.db, OWN), 100);
        slow.go();
        assert.equal(await within(5000, plain, 'the plain seed'), true);
        await within(5000, pass.done, 'the pass');
        assert.equal(t.c.fenced.size, 0);
        assert.equal(pos(t.db, OWN), 100);
      } finally { slow.go(); t.close(); }
    });
  });
}

test('a plain seed that is refused or fails leaves the routes it held', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 600, 150); // a plain seed ...
    follower(path, w);
    w.objects.delete(w.manifest.full.object); // ... whose chain object is gone: no step ever applies
    const t = client(path, w);
    try {
      t.c.fenced.set(OWN, new Set(['orders'])); // another table's fence holds the route
      assert.equal(await t.c.applyGenerations('item'), false);
      assert.equal(t.c.replacing.get(OWN)?.size ?? 0, 0);
      assert.deepEqual([...t.c.fenced.get(OWN)], ['orders'], 'the parking the other owner holds is not this one\'s to release; this one joined it and left it');
    } finally { t.close(); }
  });
});

test('a plain full that fails after its first chunk is owed too: no overlap needed', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 600, 150); // a plain seed: the cut is ahead of the position
    follower(path, w);
    let armed = true;
    const t = client(path, w, { hook: async (point) => { if (point === 'after_chunk' && armed) throw new Error('crash at after_chunk'); } });
    try {
      await assert.rejects(() => t.c.applyGenerations('item'), /crash at after_chunk/);
      assert.equal(owed(t.db), 1);
      assert.equal(t.db.prepare("SELECT count(*) AS n FROM _zebridge_generations WHERE tbl = 'item'").get()?.n, 0, 'the anchor is cleared with the first chunk\'s owed record');
      assert.equal(t.c.syncedTables.get('item').unseeded, true);
      assert.deepEqual(t.c.waits, ['item']);
      assert.equal(pos(t.db, OWN), 500, 'a plain seed moves no cursor');
      armed = false;
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.deepEqual(rows(t.db), wanted(2, 5));
      assert.equal(owed(t.db), 0);
      assert.equal((anchor(t.db) as any).seed_seq, 600);
    } finally { t.close(); }
  });
});

/// The decision to take a full as plain is made BEFORE its download, and the download is slow: the tails
/// keep applying CDC meanwhile. An event past the cut, applied to the rows and counted by the cursor, is
/// deleted by the full and never replays — unless the position is read again, with the tails stopped,
/// before the first chunk, and the full becomes a fence.
for (const inPass of [false, true]) {
  test(`CDC passing the cut while a plain full downloads makes it a fence: the cursor goes back, the event replays (${inPass ? 'in a pass' : 'background'})`, async () => {
    await withDir(async (path) => {
      const w = new World();
      w.publish(2, 5, 600, 150); // ahead of the position 500: a plain seed when decided
      follower(path, w);
      const slow = deferred();
      w.hold = () => slow.p;
      const t = client(path, w);
      try {
        const stops: string[] = [];
        t.c.tailStops.set(OWN, async () => { stops.push(OWN); });
        const apply = t.c.applyGenerations('item', inPass);
        for (let i = 0; i < 50 && !t.c.replacing.get(OWN)?.has('item'); i++) await new Promise((r) => setImmediate(r));
        assert.ok(t.c.replacing.get(OWN)?.has('item'), 'decided plain, the download is paused');
        assert.equal(pos(t.db, OWN), 500);
        // The live tail applies the event at 650 to the rows and moves the cursor past the cut.
        t.db.exec(`INSERT INTO item VALUES (650,'acme','live','${VERSION(20)}')`);
        t.db.exec(`UPDATE _zebridge_stream_seq SET last_seq = 650 WHERE stream = '${OWN}'`);
        t.c.globalSyncState.seq[OWN] = 650;
        slow.go();
        assert.equal(await within(5000, apply, 'the seed'), true);
        assert.ok(stops.length > 0, 'the tail was stopped before the rows were replaced');
        assert.deepEqual(rows(t.db), wanted(2, 5), 'the chain\'s rows replaced the old ones, the live row included');
        assert.equal(pos(t.db, OWN), 600, 'the cursor is back at the cut: the event at 650 replays on top of the chain');
        assert.equal(t.c.globalSyncState.seq[OWN], 600);
        assert.equal((anchor(t.db) as any).seed_seq, 600);
        assert.equal(owed(t.db), 0, 'committed, nothing owed');
        assert.equal(t.c.fenced.size, 0, 'the route is released with the commit');
        assert.ok(t.log.some((m) => /the position passed the cut while the full downloaded/.test(m)));
        assert.ok(t.log.some((m) => /fenced — chain g2/.test(m)), 'it went through VERIFY and COMMIT like any fence');
      } finally { t.close(); }
    });
  });
}

/// The shared route (CDC_PUBLIC, the open-tenant rows) has its own position and its own cut. Only IT can be past
/// its cut while the own route is behind: the two streams are read independently. A full deletes the rows back to
/// both cuts, so the shared cursor must go back too — or the events between never replay.
const sharedBehind = (path: string, w: World) => {
  follower(path, w);
  const f = client(path, w);
  f.db.exec(`UPDATE _zebridge_stream_seq SET last_seq = 30 WHERE stream = '${PUB}'`); // the shared cut is 40: behind it
  f.close();
};

test('only the shared route is past its cut when the full is decided: a fence, the own cursor stays', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 600, 40); // own 500 < 600; shared 90 > 40
    follower(path, w);
    const t = client(path, w);
    try {
      t.c.tailStops.set(OWN, async () => {});
      t.c.tailStops.set(PUB, async () => {});
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.deepEqual(rows(t.db), wanted(2, 5));
      assert.equal(pos(t.db, OWN), 500, 'the own route is not behind its cut: its cursor stays');
      assert.equal(pos(t.db, PUB), 40, 'the shared cursor went back to its cut: the events after it replay');
      assert.deepEqual({ ...anchor(t.db) }, { seed_seq: 600, seed_stream: OWN, shared_seed_seq: 40 });
      assert.equal(owed(t.db), 0);
      assert.ok(t.log.some((m) => /fence prepared — chain g2 cut at 600 on CDC_acme \(position 500, not behind it\)/.test(m)), t.log.join('\n'));
    } finally { t.close(); }
  });
});

for (const inPass of [false, true]) {
  test(`CDC passing the SHARED cut while a plain full downloads makes it a fence: the shared cursor goes back, the event replays (${inPass ? 'in a pass' : 'background'})`, async () => {
    await withDir(async (path) => {
      const w = new World();
      w.publish(2, 5, 600, 40);
      sharedBehind(path, w); // own 500 < cut 600, shared 30 < cut 40: a plain full when decided
      const slow = deferred();
      w.hold = () => slow.p;
      const t = client(path, w);
      try {
        t.c.tailStops.set(OWN, async () => {});
        t.c.tailStops.set(PUB, async () => {});
        const apply = t.c.applyGenerations('item', inPass);
        for (let i = 0; i < 50 && !t.c.replacing.get(OWN)?.has('item'); i++) await new Promise((r) => setImmediate(r));
        assert.ok(t.c.replacing.get(OWN)?.has('item'), 'decided plain, the download is paused');
        assert.equal(pos(t.db, PUB), 30);
        // The shared tail applies a row at 95 and its cursor passes the shared cut.
        t.db.exec(`INSERT INTO item VALUES (95,'acme','shared-live','${VERSION(20)}')`);
        t.db.exec(`UPDATE _zebridge_stream_seq SET last_seq = 95 WHERE stream = '${PUB}'`);
        t.c.globalSyncState.seq[PUB] = 95;
        slow.go();
        assert.equal(await within(5000, apply, 'the seed'), true);
        assert.deepEqual(rows(t.db), wanted(2, 5), 'the chain\'s rows replaced the old ones, the shared live row included');
        assert.equal(pos(t.db, PUB), 40, 'the shared cursor is back at the shared cut: the event at 95 replays on top of the chain');
        assert.equal(t.c.globalSyncState.seq[PUB], 40);
        assert.equal(pos(t.db, OWN), 500, 'the own route never passed its cut');
        assert.deepEqual({ ...anchor(t.db) }, { seed_seq: 600, seed_stream: OWN, shared_seed_seq: 40 });
        assert.equal(owed(t.db), 0);
        assert.equal(t.c.fenced.size, 0, 'both routes are released with the commit');
        assert.ok(t.log.some((m) => /the position passed the cut while the full downloaded — fence prepared/.test(m)));
        assert.ok(t.log.some((m) => /fenced — chain g2/.test(m)), 'VERIFY and COMMIT like any fence');
      } finally { t.close(); }
    });
  });
}

test('CDC passing the shared cut during the download when the shared route cannot be rewound: nothing replaced', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 600, 40);
    sharedBehind(path, w);
    const slow = deferred();
    w.hold = () => slow.p;
    const t = client(path, w, { others: ['orders'] }); // orders rides CDC_PUBLIC and has no anchor: its coverage is unknown
    try {
      t.c.tailStops.set(PUB, async () => {});
      const apply = t.c.applyGenerations('item');
      for (let i = 0; i < 50 && !t.c.replacing.get(OWN)?.has('item'); i++) await new Promise((r) => setImmediate(r));
      t.db.exec(`UPDATE _zebridge_stream_seq SET last_seq = 95 WHERE stream = '${PUB}'`);
      t.c.globalSyncState.seq[PUB] = 95;
      slow.go();
      assert.equal(await within(5000, apply, 'the seed'), false, 'waits: nothing is destroyed');
      assert.deepEqual(rows(t.db), ['91:old', '92:old', '93:old']);
      assert.equal(owed(t.db), 0);
      assert.equal(pos(t.db, PUB), 95);
      assert.ok((anchor(t.db) as any), 'the anchor is intact');
    } finally { t.close(); }
  });
});

test('a plain full whose position stays behind the cut is owed as before, and the tails are given back', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 600, 150);
    follower(path, w);
    const t = client(path, w);
    try {
      t.c.tailStops.set(OWN, async () => {});
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.deepEqual(rows(t.db), wanted(2, 5));
      assert.equal(pos(t.db, OWN), 500, 'a plain seed moves no cursor');
      assert.equal((anchor(t.db) as any).seed_seq, 600);
      assert.equal(owed(t.db), 0);
      assert.ok(t.passes.length >= 1, 'the tails stopped for the decision are asked for again');
      assert.equal(t.log.some((m) => /the position passed the cut/.test(m)), false);
    } finally { t.close(); }
  });
});

test('CDC passing the cut during the download when the fence would be refused: nothing is replaced, the old rows and anchor stay', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 600, 150);
    follower(path, w); // orders has no anchor: its coverage on the route is unknown (D2 refuses a rewind)
    const slow = deferred();
    w.hold = () => slow.p;
    const t = client(path, w, { others: ['orders'] });
    try {
      t.c.tailStops.set(OWN, async () => {});
      const apply = t.c.applyGenerations('item');
      for (let i = 0; i < 50 && !t.c.replacing.get(OWN)?.has('item'); i++) await new Promise((r) => setImmediate(r));
      t.db.exec(`UPDATE _zebridge_stream_seq SET last_seq = 650 WHERE stream = '${OWN}'`);
      t.c.globalSyncState.seq[OWN] = 650;
      slow.go();
      assert.equal(await within(5000, apply, 'the seed'), false, 'waits: nothing is destroyed');
      assert.deepEqual(rows(t.db), ['91:old', '92:old', '93:old']);
      assert.equal(owed(t.db), 0);
      assert.equal(pos(t.db, OWN), 650);
      assert.ok((anchor(t.db) as any), 'the anchor is intact');
    } finally { t.close(); }
  });
});

/// The waits for a chain run on the clock: one failed attempt must not be the last. A throw out of an
/// attempt (the hub unreachable for a moment, a half-replaced table that fails again) used to end the
/// loop, leave the table unseeded with its events held, and nothing asking for its chain any more.
const ticks = async (ms: number, n: number) => {
  for (let i = 0; i < n; i++) {
    for (let k = 0; k < 20; k++) await new Promise((r) => setImmediate(r));
    mock.timers.tick(ms);
  }
  for (let k = 0; k < 20; k++) await new Promise((r) => setImmediate(r));
};

test('the wait for a chain survives an attempt that throws, and asks again until it lands', async () => {
  await withDir(async (path) => {
    mock.timers.enable({ apis: ['setTimeout'] });
    const w = new World();
    const t = client(path, w);
    try {
      t.c.waitForChain = (ZeBridge.prototype as any).waitForChain;
      let calls = 0;
      t.c.applyGenerations = async () => { if (++calls <= 2) throw new Error('hub unreachable'); return true; };
      t.c.waitForChain('item');
      assert.equal(t.c.syncedTables.get('item').unseeded, true);
      await ticks(15_000, 4);
      assert.equal(calls, 3, 'two throws, then the attempt that landed');
      assert.equal(t.c.syncedTables.get('item').unseeded, false);
      assert.equal(t.c.lateSeeds.size, 0);
      assert.equal(t.log.filter((m) => /late seed attempt failed: Error: hub unreachable/.test(m)).length, 2);
    } finally { t.close(); mock.timers.reset(); }
  });
});

test('a replacement that throws again during the wait asks for another loop: the one running goes on', async () => {
  await withDir(async (path) => {
    mock.timers.enable({ apis: ['setTimeout'] });
    const w = new World();
    w.publish(2, 5, 600, 150);
    follower(path, w);
    let armed = true;
    const t = client(path, w, { hook: async (point) => { if (point === 'after_chunk' && armed) throw new Error('crash at after_chunk'); } });
    try {
      t.c.waitForChain = (ZeBridge.prototype as any).waitForChain;
      // The full fails after its first chunk: owed, so the failure asks for the wait (applyGenerationsFrom).
      await assert.rejects(() => t.c.applyGenerations('item'), /crash at after_chunk/);
      assert.equal(t.c.lateSeeds.has('item'), true, 'the wait is running');
      // The automatic retry fails the same way: it asks for a wait again, and the table is already in
      // lateSeeds — the exception must not take the running loop with it.
      await ticks(15_000, 2);
      assert.equal(t.c.lateSeeds.has('item'), true, 'still waiting after a second failure');
      assert.equal(t.c.syncedTables.get('item').unseeded, true);
      armed = false;
      await ticks(15_000, 3);
      assert.equal(t.c.syncedTables.get('item').unseeded, false, 'the next attempt seeds it');
      assert.deepEqual(rows(t.db), wanted(2, 5));
      assert.equal(owed(t.db), 0);
      assert.equal(t.c.lateSeeds.size, 0);
    } finally { t.close(); mock.timers.reset(); }
  });
});

test('a re-seed wait survives an attempt that throws too', async () => {
  await withDir(async (path) => {
    mock.timers.enable({ apis: ['setTimeout'] });
    const w = new World();
    const t = client(path, w);
    try {
      let calls = 0;
      t.c.applyGenerations = async () => { if (++calls <= 2) throw new Error('hub unreachable'); return true; };
      t.c.kickReseed = (ZeBridge.prototype as any).kickReseed;
      t.c.kickReseed('item');
      await ticks(15_000, 4);
      assert.equal(calls, 3);
      assert.equal(t.c.reseedKicks.size, 0, 'the loop ended because the chain landed');
    } finally { t.close(); mock.timers.reset(); }
  });
});

test('a delta-only plan writes no owed record: it is not destructive', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 600, 150);
    // the same chain as a delta over the replica's watermark: no full step in the plan
    w.manifest.deltas = [{ gen: 2, object: w.manifest.full.object, cutoff: VERSION(10), sorted: true }];
    w.manifest.full = { ...w.manifest.full, cutoff: VERSION(0) };
    follower(path, w);
    const t = client(path, w);
    try {
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.equal(t.db.prepare('SELECT count(*) AS n FROM _zebridge_fence').get()?.n, 0);
    } finally { t.close(); }
  });
});

// ── The joint fence: several tables of one route reseeded together ────────────────────────────────────
//
// A seed-epoch move drops each table's anchor as its descriptor arrives, so reseeding two tables of one tenant
// leaves BOTH unseeded while the position is past both chains' cuts. Each fence needs the other table seeded (its
// coverage is what the applied frontier certifies), so each refuses the other for as long as it goes on
// (HANDOFF "Observed, not investigated"). The joint fence replaces the participants together: every unseeded table
// the rewound routes carry takes part, the routes stay parked and the participants durably owed until all of them
// completed and the hub's proofs still hold, each is anchored at ITS OWN cut while the cursors go to the smallest,
// and the seeded tables stay untouched under applied frontiers. These are the cases the mechanism must pass;
// they are written against the per-table entry point a pass uses, so they do not name the coordination.

const ordersRows = (db: DatabaseSync) => db.prepare('SELECT id, note FROM orders ORDER BY id').all().map((r) => `${r.id}:${r.note}`);
const catalogueRows = (db: DatabaseSync) => db.prepare('SELECT id, note FROM catalogue ORDER BY id').all().map((r) => `${r.id}:${r.note}`);
const anchorOf = (db: DatabaseSync, tbl: string) => db.prepare('SELECT seed_seq, seed_stream, shared_seed_seq FROM _zebridge_generations WHERE tbl = ?').get(tbl);
const seededTbl = (db: DatabaseSync, tbl: string) => anchorOf(db, tbl) !== undefined;
const wantedOrders = (gen: number, n: number) => Array.from({ length: n }, (_, i) => `${i + 1}:new-${gen}`);

/// item and orders both lost their anchor (the epoch move), the position (500 / 90) is past every cut, and
/// catalogue is seeded: it holds row 1, and row 7 was inserted at 410 and deleted at 420 on the own route.
function pairFollower(path: string, world: World) {
  const t = client(path, world, { others: ['orders', 'catalogue'] });
  t.db.exec(`
    INSERT INTO item VALUES (91,'acme','old','${VERSION(1)}'),(92,'acme','old','${VERSION(1)}'),(93,'acme','old','${VERSION(1)}');
    INSERT INTO orders VALUES (81,'acme','old-o','${VERSION(1)}'),(82,'acme','old-o','${VERSION(1)}');
    INSERT INTO catalogue VALUES (1,'acme','kept','${VERSION(1)}');
    INSERT INTO _zebridge_stream_seq VALUES ('${OWN}', 500, 'own-1'), ('${PUB}', 90, 'pub-1');
    INSERT INTO _zebridge_generations VALUES ('catalogue', '${VERSION(1)}', 1, 0, 470, '${OWN}', 70);
  `);
  t.close();
}

/// What a pass does with unseeded tables: each asks for its chain, again while any progress was made.
async function seedPair(t: ReturnType<typeof client>, tables = ['item', 'orders']) {
  for (let round = 0; round < 4 && !tables.every((x) => seededTbl(t.db, x)); round++) {
    let progress = false;
    for (const x of tables) if (!seededTbl(t.db, x) && await t.c.applyGenerations(x, true)) progress = true;
    if (!progress) break;
  }
}

/// The end state of a recovered pair, whatever the order of the cuts or the road taken.
function assertJoint(t: ReturnType<typeof client>, cuts: { item: number; orders: number }, shared: { item: number; orders: number }) {
  assert.deepEqual(rows(t.db), wanted(2, 5), 'item replaced');
  assert.deepEqual(ordersRows(t.db), wantedOrders(2, 4), 'orders replaced');
  assert.deepEqual(catalogueRows(t.db), ['1:kept'], 'catalogue untouched');
  assert.equal(pos(t.db, OWN), Math.min(cuts.item, cuts.orders), 'the own cursor at the smallest cut');
  assert.equal(pos(t.db, PUB), Math.min(shared.item, shared.orders), 'the shared cursor at the smallest cut');
  assert.deepEqual({ ...anchorOf(t.db, 'item') }, { seed_seq: cuts.item, seed_stream: OWN, shared_seed_seq: shared.item }, 'item anchored at its own cut');
  assert.deepEqual({ ...anchorOf(t.db, 'orders') }, { seed_seq: cuts.orders, seed_stream: OWN, shared_seed_seq: shared.orders }, 'orders anchored at its own cut');
  assert.equal(owed(t.db), 0, 'nothing owed');
  assert.equal(t.c.fenced.size, 0, 'the routes are released');
  assert.deepEqual(frontierRows(t.db).map((r) => `${r.tbl}:${r.stream}:${r.seq}`), [`catalogue:${PUB}:90`, `catalogue:${OWN}:500`], 'only the untouched table has frontiers, at the positions it held');
}

const orderings: [string, { item: number; orders: number }, { item: number; orders: number }][] = [
  ['item cut first', { item: 300, orders: 350 }, { item: 40, orders: 60 }],
  ['orders cut first', { item: 350, orders: 300 }, { item: 60, orders: 40 }],
];

test('two tables reseeded together are fenced together: each at its own cut, the cursors at the smallest, a third table untouched', async () => {
  for (const [name, cuts, shared] of orderings) await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, cuts.item, shared.item, 'item');
    w.publish(2, 4, cuts.orders, shared.orders, 'orders');
    pairFollower(path, w);
    const t = client(path, w, { others: ['orders', 'catalogue'] });
    try {
      await seedPair(t);
      assert.ok(!t.log.some((m) => /refusing the full \(D2\)/.test(m)), `${name}: no mutual refusal`);
      assertJoint(t, cuts, shared);
      // The untouched table's insert at 410 (row 7, deleted at 420) replays from the smallest cut: its frontier skips it.
      await t.c.applyEvent('catalogue', ev('catalogue', 7, OWN, 410, 'resurrected'));
      assert.deepEqual(catalogueRows(t.db), ['1:kept'], `${name}: the row deleted at 420 is not recreated by the replay of its insert`);
      await t.c.applyEvent('catalogue', ev('catalogue', 8, OWN, 501, 'new'));
      assert.deepEqual(catalogueRows(t.db), ['1:kept', '8:new'], 'past its frontier the table takes events');
    } finally { t.close(); }
  });
});

test('the second replacement cannot be had: nothing is released, the recovery stays durably incomplete, and a later attempt completes it', async () => {
  for (const missing of ['item', 'orders']) await withDir(async (path) => {
    const w = new World();
    const cuts = { item: 300, orders: 350 }, shared = { item: 40, orders: 60 };
    w.publish(2, 5, cuts.item, shared.item, 'item');
    w.publish(2, 4, cuts.orders, shared.orders, 'orders');
    pairFollower(path, w);
    const gone = w.objects.get(`${missing}-i1-g2-full`)!;
    w.objects.delete(`${missing}-i1-g2-full`);
    const t = client(path, w, { others: ['orders', 'catalogue'] });
    try {
      await seedPair(t);
      assert.ok(!(seededTbl(t.db, 'item') && seededTbl(t.db, 'orders')), 'not both anchored');
      // Two safe states: nothing was changed (the participants' objects were checked before PREPARE), or what was
      // changed is recorded as incomplete and the routes stay parked. Nothing in between.
      const untouched = Number(pos(t.db, OWN)) === 500 && Number(pos(t.db, PUB)) === 90 && rows(t.db).join() === '91:old,92:old,93:old' && ordersRows(t.db).join() === '81:old-o,82:old-o' && owed(t.db) === 0;
      assert.ok(untouched || (owed(t.db) >= 1 && t.c.fenced.size > 0), `${missing} unavailable: untouched, or durably incomplete with the routes parked`);
      assert.ok(Number(pos(t.db, OWN)) <= 500, 'the cursor never moved forward');
      assert.equal(catalogueRows(t.db).join(), '1:kept', 'the untouched table kept its rows');
    } finally { t.close(); }
    w.objects.set(`${missing}-i1-g2-full`, gone);
    const r = client(path, w, { others: ['orders', 'catalogue'] });
    try {
      await seedPair(r);
      assertJoint(r, cuts, shared);
    } finally { r.close(); }
  });
});

test('a crash at any point of a joint fence — before a verify, before a commit, after a chunk of the second table — recovers on restart', async () => {
  const points: [string, number][] = [['after_chunk', 1], ['after_chunk', 4], ['after_chunk', 5], ['before_verify', 1], ['before_verify', 2], ['before_commit', 1], ['before_commit', 2]];
  for (const [ordering, cuts, shared] of orderings) for (const [point, nth] of points) await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, cuts.item, shared.item, 'item');
    w.publish(2, 4, cuts.orders, shared.orders, 'orders');
    pairFollower(path, w);
    let seen = 0;
    const crashed = client(path, w, { others: ['orders', 'catalogue'], hook: (p) => { if (p === point && ++seen === nth) throw new Error(`crash at ${p} #${nth}`); } });
    try {
      await seedPair(crashed).catch(() => {});
      // Whatever was done: never a table anchored while the other still holds its old rows under a cursor past its cut.
      if (seen >= nth) {
        assert.ok(owed(crashed.db) >= 1 || (seededTbl(crashed.db, 'item') && seededTbl(crashed.db, 'orders')), `${ordering} / ${point} #${nth}: incomplete is recorded, or complete`);
        assert.ok(Number(pos(crashed.db, OWN)) <= 500, 'the cursor never moved forward');
      }
    } finally { crashed.close(); }
    const r = client(path, w, { others: ['orders', 'catalogue'] });
    try {
      await seedPair(r);
      assertJoint(r, cuts, shared);
    } finally { r.close(); }
  });
});

test('the retention overtaking a cut before the release keeps every participant unreleased; restored, the restart completes', async () => {
  await withDir(async (path) => {
    const w = new World();
    const cuts = { item: 300, orders: 350 }, shared = { item: 40, orders: 60 };
    w.publish(2, 5, cuts.item, shared.item, 'item');
    w.publish(2, 4, cuts.orders, shared.orders, 'orders');
    pairFollower(path, w);
    let verifies = 0;
    const t = client(path, w, { others: ['orders', 'catalogue'], hook: (p) => { if (p === 'before_verify' && ++verifies === 2) w.streams[OWN]!.first = 400; } });
    try {
      await seedPair(t);
      assert.ok(owed(t.db) >= 1, 'recovery is durably incomplete: the stream no longer holds the events after the cuts');
      assert.ok(t.c.fenced.size > 0, 'no route is released');
      assert.ok(!(seededTbl(t.db, 'item') && seededTbl(t.db, 'orders')), 'the pair is not trusted');
    } finally { t.close(); }
    w.streams[OWN]!.first = 1;
    const r = client(path, w, { others: ['orders', 'catalogue'] });
    try {
      await seedPair(r);
      assertJoint(r, cuts, shared);
    } finally { r.close(); }
  });
});

test('a table with no chain cannot take part: the refusal stands and nothing changes', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300, 40, 'item');
    pairFollower(path, w); // orders has no manifest: a promised replacement is not coverage
    const t = client(path, w, { others: ['orders', 'catalogue'] });
    try {
      assert.equal(await t.c.applyGenerations('item'), false);
      assert.ok(t.log.some((m) => /refusing the full \(D2\): orders is not seeded/.test(m)));
      assert.equal(owed(t.db), 0);
      assert.deepEqual(rows(t.db), ['91:old', '92:old', '93:old']);
      assert.equal(pos(t.db, OWN), 500);
    } finally { t.close(); }
  });
});

test('between participants the routes stay parked and the tails stay closed; the last commit releases them', async () => {
  for (const [name, cuts, shared] of orderings) await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, cuts.item, shared.item, 'item');
    w.publish(2, 4, cuts.orders, shared.orders, 'orders');
    pairFollower(path, w);
    const t = client(path, w, { others: ['orders', 'catalogue'] });
    try {
      assert.equal(await t.c.applyGenerations('item'), true);
      assert.ok(seededTbl(t.db, 'item') && !seededTbl(t.db, 'orders'), `${name}: the first participant is anchored, the second is not`);
      assert.equal(owed(t.db), 1, `${name}: the second participant is still owed`);
      assert.ok(t.c.fenced.get(OWN)?.has('orders'), `${name}: the route is parked for the participant still to come`);
      assert.ok(t.c.routeParked(OWN) && t.c.routeParked(PUB), `${name}: both routes are parked: no pass opens a tail on them`);
      assert.deepEqual(ordersRows(t.db), ['81:old-o', '82:old-o'], 'its rows are untouched until its turn');
      assert.equal(await t.c.applyGenerations('orders'), true);
      assertJoint(t, cuts, shared);
    } finally { t.close(); }
  });
});

test('a joint fence is not prepared over a chain that cannot be applied: nothing changes', async () => {
  for (const missing of ['item', 'orders']) await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300, 40, 'item');
    w.publish(2, 4, 350, 60, 'orders');
    pairFollower(path, w);
    w.objects.delete(`${missing}-i1-g2-full`);
    const t = client(path, w, { others: ['orders', 'catalogue'] });
    try {
      const other = missing === 'item' ? 'orders' : 'item';
      assert.equal(await t.c.applyGenerations(other), false);
      assert.equal(owed(t.db), 0, `${missing} missing: no obligation was created`);
      assert.equal(pos(t.db, OWN), 500);
      assert.equal(pos(t.db, PUB), 90);
      assert.equal(t.c.fenced.size, 0);
      assert.equal(frontierRows(t.db).length, 0);
    } finally { t.close(); }
  });
});

test('a participant whose cut the retention overtook while the others finished is owed again, and nothing is released', async () => {
  await withDir(async (path) => {
    const w = new World();
    const cuts = { item: 300, orders: 350 }, shared = { item: 40, orders: 60 };
    w.publish(2, 5, cuts.item, shared.item, 'item');
    w.publish(2, 4, cuts.orders, shared.orders, 'orders');
    pairFollower(path, w);
    let verifies = 0;
    // item commits first (its verify is the first); the stream then drops the events after item's cut but keeps orders'.
    const t = client(path, w, { others: ['orders', 'catalogue'], hook: (p) => { if (p === 'before_verify' && ++verifies === 2) w.streams[OWN]!.first = 330; } });
    try {
      await seedPair(t);
      assert.ok(seededTbl(t.db, 'orders'), 'orders committed: its own cut is still retained');
      assert.ok(!seededTbl(t.db, 'item'), 'item is owed again: the events after its cut are gone');
      assert.ok(owed(t.db) >= 1);
      assert.ok(t.c.fenced.size > 0, 'the routes are not released');
      assert.ok(t.log.some((m) => /item.*no longer holds the events after (its|the) cut/.test(m)), 'it says why');
    } finally { t.close(); }
    w.streams[OWN]!.first = 1;
    const r = client(path, w, { others: ['orders', 'catalogue'] });
    try {
      await seedPair(r);
      assertJoint(r, cuts, shared);
    } finally { r.close(); }
  });
});

test('a joint fence survives a restart: A committed, B unavailable, the process restarts, retention overtakes A\'s cut — the routes stay parked and A is proven again before they are released', async () => {
  for (const [name, cuts, shared] of orderings) await withDir(async (path) => {
    // The one whose cut the stream will drop is the first to commit; the other is the one that fails.
    const first = cuts.item < cuts.orders ? 'item' : 'orders', second = first === 'item' ? 'orders' : 'item';
    const dropTo = Math.min(cuts.item, cuts.orders) + 30; // past the first's cut, not past the second's
    const w = new World();
    w.publish(2, 5, cuts.item, shared.item, 'item');
    w.publish(2, 4, cuts.orders, shared.orders, 'orders');
    pairFollower(path, w);
    const gone = w.objects.get(`${second}-i1-g2-full`)!;
    // The second one's object disappears after the joint PREPARE (its plan was checked before it): the first one commits, the
    // second cannot be had.
    let verifies = 0;
    const t = client(path, w, { others: ['orders', 'catalogue'], hook: (p) => { if (p === 'before_verify' && ++verifies === 1) w.objects.delete(`${second}-i1-g2-full`); } });
    try {
      assert.equal(await t.c.applyGenerations(first, true), true, `${name}: ${first} commits`);
      assert.equal(await t.c.applyGenerations(second, true), false, `${name}: ${second} cannot be had`);
      assert.ok(seededTbl(t.db, first) && !seededTbl(t.db, second));
      assert.equal(owed(t.db), 1);
      assert.equal(Number((t.db.prepare('SELECT count(*) AS n FROM _zebridge_joint_done WHERE tbl = ?').get(first) as any).n) > 0, true, `${name}: ${first}'s proofs are in the replica`);
    } finally { t.close(); }

    // The process restarts. ${second} is still unavailable.
    const r = client(path, w, { others: ['orders', 'catalogue'] });
    try {
      await r.c.ensureJointRestored();
      assert.ok(r.c.routeParked(OWN) && r.c.routeParked(PUB), `${name}: the restart parks the routes again, before anything else happens`);
      assert.ok(r.c.fenced.get(OWN)?.has(second), `${name}: for the participant that is still owed`);
      // Retention overtakes the first one's cut while the second is still unavailable.
      w.streams[OWN]!.first = dropTo;
      assert.equal(await r.c.applyGenerations(second, true), false);
      assert.ok(r.c.routeParked(OWN), `${name}: still parked`);
      // The second one comes back: it commits, and as the last participant proves the first again — which fails.
      w.objects.set(`${second}-i1-g2-full`, gone);
      assert.equal(await r.c.applyGenerations(second, true), true);
      assert.ok(seededTbl(r.db, second), `${name}: ${second} committed`);
      assert.ok(!seededTbl(r.db, first), `${name}: ${first} is owed again: the stream no longer holds the events after its cut`);
      assert.equal(owed(r.db), 1);
      assert.ok(r.c.routeParked(OWN), `${name}: nothing was released`);
      assert.ok(r.log.some((m) => new RegExp(`${first}: joint fence not released`).test(m)));
    } finally { r.close(); }

    // Retention restored, one more restart: the owed one is replaced, the group ends.
    w.streams[OWN]!.first = 1;
    const q = client(path, w, { others: ['orders', 'catalogue'] });
    try {
      await q.c.ensureJointRestored();
      assert.ok(q.c.routeParked(OWN), `${name}: parked again after the second restart`);
      await seedPair(q);
      assertJoint(q, cuts, shared);
      assert.equal(Number((q.db.prepare('SELECT count(*) AS n FROM _zebridge_joint_done').get() as any).n), 0, `${name}: no proof is left behind`);
    } finally { q.close(); }
  });
});

test('a restart without the group would open the tails under the participant still owed: ensureJointRestored is what parks them', async () => {
  await withDir(async (path) => {
    const w = new World();
    w.publish(2, 5, 300, 40, 'item');
    w.publish(2, 4, 350, 60, 'orders');
    pairFollower(path, w);
    const t = client(path, w, { others: ['orders', 'catalogue'], hook: (p) => { if (p === 'after_chunk') throw new Error('crash'); } });
    try { await t.c.applyGenerations('item', true).catch(() => {}); } finally { t.close(); }
    // Crashed after the joint PREPARE: both are owed and flagged; nothing else is recorded.
    const r = client(path, w, { others: ['orders', 'catalogue'] });
    try {
      assert.equal(r.c.routeParked(OWN), false, 'a fresh process knows nothing yet');
      await r.c.ensureJointRestored();
      assert.ok(r.c.routeParked(OWN) && r.c.routeParked(PUB));
      assert.deepEqual([...r.c.fenced.get(OWN)!].sort(), ['item', 'orders']);
    } finally { r.close(); }
  });
});
