/// The conformance runner: every case in ../fixtures/core-fixtures.json against
/// the TS core. A port (Zig, …) writes its own thin runner over the SAME file —
/// the fixtures are the spec, this file is just plumbing.
import { test } from 'node:test';
import { encode, decode, decodeMulti } from '@msgpack/msgpack';
import { parseChainHead, chainStageSql, msgpackScan, msgpackDecodeScanned } from './core.ts';
import { sha256 } from 'js-sha256';
import { cdcValue, pgEngineValues, isBytes, pgArrayValues, sortRowsByKey, chainBulkSql, chainChunkJson, vecColsOf, vecLiteral, pgVectorValues } from './core.ts';

test('a chunk in one statement through json_each, version-guarded (§10fc)', () => {
  const sql = chainBulkSql('t', ['uid', 'n', 'updated_at'], ['uid'], 'updated_at');
  assert.equal(sql, `INSERT INTO t ("uid", "n", "updated_at") SELECT json_extract(value, '$[0]'), json_extract(value, '$[1]'), json_extract(value, '$[2]') FROM json_each(?) WHERE true ON CONFLICT("uid") DO UPDATE SET "n" = excluded."n", "updated_at" = excluded."updated_at" WHERE excluded."updated_at" > t."updated_at"`);
  assert.ok(chainBulkSql('t', ['uid'], ['uid'], null).endsWith('DO NOTHING'));
});

test('a chunk with a BLOB column in one statement: bytes back through unhex, text and null as they are', async () => {
  const { default: Database } = await import('better-sqlite3');
  const db = new Database(':memory:');
  db.exec('CREATE TABLE t (uid TEXT PRIMARY KEY, geom BLOB, n INTEGER, updated_at TEXT)');
  const ewkb = new Uint8Array([1, 1, 0, 0, 32, 230, 16, 0, 0, 0, 0, 0, 0, 0, 0, 255]);
  const rows = [
    ['a', ewkb, 1, '2026-10-05T00:00:00.000000Z'],
    ['b', null, 2, '2026-10-05T00:00:00.000000Z'],
    ['c', 'not bytes', 3, '2026-10-05T00:00:00.000000Z'],   // a BLOB column holding text loses nothing
    ['d', new Uint8Array(0), 4, '2026-10-05T00:00:00.000000Z'],
  ];
  const cols = ['uid', 'geom', 'n', 'updated_at'];
  db.prepare(chainBulkSql('t', cols, ['uid'], 'updated_at', [1])).run(chainChunkJson(rows, [1]));
  const got = db.prepare('SELECT uid, geom, typeof(geom) AS ty, n FROM t ORDER BY uid').all() as any[];
  assert.deepEqual(Buffer.from(got[0].geom), Buffer.from(ewkb));
  assert.equal(got[0].ty, 'blob');
  assert.equal(got[1].geom, null);
  assert.equal(got[2].geom, 'not bytes');
  assert.equal(got[3].ty, 'blob');
  assert.equal(got[3].geom.length, 0);
  assert.equal(got[3].n, 4);
  // the staging insert takes the same picks
  db.exec('CREATE TEMP TABLE s (uid, geom, n, updated_at)');
  db.prepare(chainStageSql('temp.s', cols, [1])).run(chainChunkJson(rows, [1]));
  assert.deepEqual(Buffer.from((db.prepare("SELECT geom FROM temp.s WHERE uid = 'a'").get() as any).geom), Buffer.from(ewkb));
  // and a table with no BLOB column binds exactly what it did before
  assert.equal(chainChunkJson([[1, 'x']]), JSON.stringify([[1, 'x']]));
});

test('chain rows sort by their key cell, stable for the rest (§10fb)', () => {
  const rows = [['c', 1], ['a', 2], ['b', 3]];
  assert.deepEqual(sortRowsByKey(rows, 0).map((r) => r[0]), ['a', 'b', 'c']);
  assert.deepEqual(sortRowsByKey([[3, 'x'], [1, 'y'], [2, 'z']], 0).map((r) => r[0]), [1, 2, 3]);
  assert.deepEqual(sortRowsByKey(rows, -1), rows);
  assert.deepEqual(rows.map((r) => r[0]), ['c', 'a', 'b']); // the input is untouched
});

test('pgvector wire shapes render as pgvector text on a PostgreSQL engine (§10fg)', () => {
  assert.equal(vecLiteral('vector', new Uint8Array([0, 0, 0x80, 0x3f, 0, 0, 0x20, 0xc0])), '[1,-2.5]');
  assert.equal(vecLiteral('halfvec', new Uint8Array([0x00, 0x3e, 0x00, 0xbc])), '[1.5,-1]');
  assert.equal(vecLiteral('sparsevec', new Uint8Array([5, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0x3f])), '{3:0.5}/5');
  assert.equal(vecLiteral('bit', new Uint8Array([0xa5]), 8), '10100101');
  assert.equal(vecLiteral('bit', new Uint8Array([0xa0]), 3), '101');
  assert.throws(() => vecLiteral('vector', new Uint8Array([1, 2, 3])));
  const cols = vecColsOf([{ name: 'emb', type: 'vector(3)' }, { name: 'b3', type: 'bit(3)' }, { name: 'vb', type: 'bit varying(12)' }, { name: 'n', type: 'integer' }]);
  assert.deepEqual(cols, [{ name: 'emb', kind: 'vector', bits: 0 }, { name: 'b3', kind: 'bit', bits: 3 }]);
  const out = pgVectorValues({ emb: new Uint8Array([0, 0, 0x80, 0x3f]), b3: new Uint8Array([0xa0]), n: 1, vb: '1010' }, cols);
  assert.deepEqual(out, { emb: '[1]', b3: '101', n: 1, vb: '1010' });
});

test('JSON array text becomes the PostgreSQL literal for array columns only (§10ey)', () => {
  const data = { tags: '["a","b c",null]', matrix: '[[1,2],[3,4]]', note: '[not an array column]', n: 3 };
  const out = pgArrayValues(data, ['tags', 'matrix']);
  assert.equal(out.tags, '{a,"b c",NULL}');
  assert.equal(out.matrix, '{{1,2},{3,4}}');
  assert.equal(out.note, '[not an array column]');
  assert.equal(out.n, 3);
  assert.equal(pgArrayValues({ tags: null }, ['tags']).tags, null);
  assert.equal(pgArrayValues({ tags: ['x'] }, ['tags']).tags, '{x}');
});

test('bytes stay bytes on every bind path (§10ex)', () => {
  const b = new Uint8Array([0, 255, 254, 65]);
  assert.equal(isBytes(b), true);
  assert.equal(isBytes('x'), false);
  assert.equal(cdcValue(b), b);
  assert.equal(chainRowParams([b, { a: 1 }, null])[0], b);
  assert.equal(chainRowParams([b, { a: 1 }, null])[1], '{"a":1}');
  assert.equal(pgEngineValues({ tile: b, tags: ['a'] }).tile, b);
  assert.equal(pgEngineValues({ tile: b, tags: ['a'] }).tags, '{a}');
});
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import {
  normalizeVersion, hlcVersion,
  nextVersion, subjectSafeToken, buildMutation,
  columnDdl, fkClausesFor, createTableSteps, rebuildSteps, diffColumns,
  fkTextDiffers, viewSteps, indexSyncPlan,
  planKeyChange, planUpsert, planUpdate, planExists, planDelete, pgArrayLiteral, chainUpsertSql, chainRowParams,
  planCdcBulk,
  seedGateDrops, tombstoned, planFromManifest, fullPredatesReplica, tableSet, mergeRegisters,
  advancePosition, foreignKeyFailureKind, pgTsToWire, lsnToNumber,
  outboxWatermarkGate,
  heartbeatPayload,
  keyShape, typeShape, retypedColumns, isReadOnlySql,
} from './core.ts';

import { loadCore, scopeSeeding, caughtUpPosition, streamResume } from './wasm-core.ts';

const here = dirname(fileURLToPath(import.meta.url));
// The rules libzb owns run from the module this package ships (wasm-core.ts).
await loadCore(readFileSync(join(here, '..', 'wasm', 'zb_core.wasm')));
const fx = JSON.parse(readFileSync(join(here, '..', 'fixtures', 'core-fixtures.json'), 'utf8'));

// §10dq: the package's grammar is the bridge's, byte for byte. The bridge embeds
// src/grammar.json; this copy ships inside the package because a file outside its
// root cannot. A drift here is a protocol fork, and this is where it turns red.
test('grammar: the packaged copy is byte-identical to src/grammar.json', () => {
  const packaged = readFileSync(join(here, 'grammar.json'), 'utf8');
  const source = readFileSync(join(here, '..', '..', 'src', 'grammar.json'), 'utf8');
  assert.equal(packaged, source);
});

// §10lx: the shipped core is libzb's current build. Rebuilt but not copied (`pnpm wasm`)
// is a client running yesterday's rules; checked when the build output is there.
const built = join(here, '..', '..', 'libzb', 'zig-out', 'wasm', 'zb_core.wasm');
test('wasm: the packaged core is byte-identical to libzb\'s build', { skip: !existsSync(built) && 'libzb not built (zig build wasm-core)' }, () => {
  assert.ok(readFileSync(join(here, '..', 'wasm', 'zb_core.wasm')).equals(readFileSync(built)));
});

for (const c of fx.heartbeat) {
  test(`heartbeat: ${c.name}`, () => assert.equal(heartbeatPayload(c.principal, c.tenant, c.ts, c.seqs, c.pending ?? {}), c.out));
}
for (const c of fx.seedGate) {
  test(`seedGate: ${c.name}`, () => assert.equal(seedGateDrops(c.ev, c.anchor), c.drops));
}
for (const c of fx.tombstoned) {
  test(`tombstoned: ${c.name}`, () => assert.equal(tombstoned(c.tombstoneColumn, c.data), c.drops));
}
for (const c of fx.chainPlan) {
  test(`chainPlan: ${c.name}`, () => assert.deepEqual(planFromManifest(c.manifest, c.watermark), c.plan));
}
for (const c of fx.outboxWatermark) {
  test(`outboxWatermark: ${c.name}`, () =>
    assert.deepEqual(outboxWatermarkGate(c.entries, c.watermark), { send: c.send, refuse: c.refuse }));
}
for (const c of fx.fullPredates) {
  test(`fullPredates: ${c.name}`, () =>
    assert.equal(fullPredatesReplica(c.manifest, c.plan, c.storedSeq), c.predates));
}
for (const c of fx.scope) {
  test(`scope: ${c.name}`, () => {
    const r = scopeSeeding(c.streams, c.tables);
    assert.deepEqual(r.gapped.sort(), [...c.gapped].sort());
    assert.deepEqual(r.tablesToSeed.sort(), [...c.tablesToSeed].sort());
  });
}
for (const c of fx.position) {
  test(`position: ${c.name}`, () => assert.equal(advancePosition(c.stored, c.batch), c.next));
}
for (const c of fx.streamResume) {
  test(`streamResume: ${c.name}`, () => assert.deepEqual(streamResume(c.stored, c.firstSeq, c.cuts), { to: c.to, blocked: c.blocked }));
}
for (const c of fx.caughtUp) {
  test(`caughtUp: ${c.name}`, () => assert.equal(caughtUpPosition(c.pos, c.lastSeq, c), c.next));
}
for (const c of fx.fkKind) {
  test(`fkKind: ${c.name}`, () => assert.equal(foreignKeyFailureKind(new Error(c.message)), c.kind));
}
for (const c of fx.pgTsToWire) {
  test(`pgTsToWire: ${c.name}`, () => assert.equal(pgTsToWire(c.in), c.out));
}
for (const c of fx.lsnToNumber) {
  test(`lsnToNumber: ${c.name}`, () => assert.equal(lsnToNumber(c.in), c.out));
}

for (const c of fx.keyChange) {
  test(`keyChange: ${c.name}`, () =>
    assert.deepEqual(planKeyChange(c.table, c.pkCols, c.data), c.step));
}
for (const c of fx.upsert) {
  test(`upsert: ${c.name}`, () =>
    assert.deepEqual(planUpsert(c.table, c.pkCols, c.data), c.step));
}
for (const c of fx.pgArrayLiteral) {
  test(`pgArrayLiteral: ${c.name}`, () => assert.equal(pgArrayLiteral(c.in), c.out));
}
for (const c of fx.update) {
  test(`update: ${c.name}`, () => assert.deepEqual(planUpdate(c.table, c.pkCols, c.data), c.plan));
}
for (const c of fx.exists) {
  test(`exists: ${c.name}`, () => assert.deepEqual(planExists(c.table, c.pkCols, c.data), c.plan));
}
for (const c of fx.delete) {
  test(`delete: ${c.name}`, () =>
    assert.deepEqual(planDelete(c.table, c.pkCols, c.data), c.step));
}
for (const c of fx.chainUpsert) {
  test(`chainUpsert: ${c.name}`, () =>
    assert.equal(chainUpsertSql(c.table, c.cols, c.pkCols, c.versionCol), c.sql));
}
for (const c of fx.chainRowParams) {
  test(`chainRowParams: ${c.name}`, () =>
    assert.deepEqual(chainRowParams(c.row), c.params));
}
for (const c of fx.cdcBulk) {
  test(`cdcBulk: ${c.name}`, () =>
    assert.deepEqual(planCdcBulk(c.engine, c.tables, c.events), c.segments));
}

for (const c of fx.columnDdl) {
  test(`columnDdl: ${c.name}`, () => assert.equal(columnDdl(c.col, c.pkCols), c.ddl));
}
for (const c of fx.fkClauses) {
  test(`fkClauses: ${c.name}`, () => assert.equal(fkClausesFor(c.fks), c.text));
}
for (const c of fx.createTable) {
  test(`createTable: ${c.name}`, () =>
    assert.deepEqual(createTableSteps(c.table, c.cols, c.pkCols, c.fks, c.strict ? { strict: true } : {}), c.steps));
}
for (const c of fx.rebuildSteps) {
  test(`rebuildSteps: ${c.name}`, () =>
    assert.deepEqual(rebuildSteps(c.table, c.cols, c.pkCols, c.fks, c.existing, c.strict ? { strict: true } : {}), c.steps));
}
for (const c of fx.mergeRegisters) {
  test(`mergeRegisters: ${c.name}`, () => assert.deepEqual(mergeRegisters(c.a, c.b), c.want));
}
for (const c of fx.tableSet) {
  test(`tableSet: ${c.name}`, () => assert.deepEqual(tableSet(c.tables ?? null, c.ondemand ?? null, c.keys ?? []), c.want));
}
for (const c of fx.readOnlySql) {
  test(`readOnlySql: ${c.name}`, () => assert.equal(isReadOnlySql(c.sql), c.allowed));
}
for (const c of fx.shape) {
  test(`shape: ${c.name}`, () => {
    assert.equal(keyShape(c.pkCols, c.cols), c.key);
    assert.equal(typeShape(c.cols), c.types);
  });
}
for (const c of fx.retyped) {
  test(`retyped: ${c.name}`, () => assert.deepEqual(retypedColumns(c.stored, c.cols), c.out));
}
for (const c of fx.diffColumns) {
  test(`diffColumns: ${c.name}`, () =>
    assert.deepEqual(diffColumns(c.existing, c.wanted, c.renamed), c.out));
}
for (const c of fx.fkDiffer) {
  test(`fkDiffer: ${c.name}`, () => assert.equal(fkTextDiffers(c.ddl, c.want), c.differs));
}
for (const c of fx.viewSteps) {
  test(`viewSteps: ${c.name}`, () => assert.deepEqual(viewSteps(c.table, c.names), c.steps));
}
for (const c of fx.indexPlan) {
  test(`indexPlan: ${c.name}`, () =>
    assert.deepEqual(indexSyncPlan(c.table, c.have, c.want), { drops: c.drops, creates: c.creates }));
}

for (const c of fx.nextVersion) {
  test(`nextVersion: ${c.name}`, () => assert.equal(nextVersion(c.now, c.last), c.out));
}
for (const c of fx.subjectSafe) {
  test(`subjectSafe: ${c.name}`, () => assert.equal(subjectSafeToken(c.in), c.out));
}
for (const c of fx.envelope) {
  test(`envelope: ${c.name}`, () => {
    if (c.throws) assert.throws(() => buildMutation(c.args), new RegExp(c.throws));
    else assert.deepEqual(buildMutation(c.args), c.out);
  });
}

for (const c of fx.normalizeVersion) {
  test(`normalizeVersion: ${c.name}`, () => assert.equal(normalizeVersion(c.in), c.out));
}
for (const c of fx.hlcVersion) {
  test(`hlcVersion: ${c.name}`, () => assert.equal(hlcVersion(c.now, c.last, c.floor), c.out));
}

// §10ix: the chain document head, read by hand so the rows can be decoded as a stream.
test('parseChainHead: a document the msgpack encoder wrote (fixmap, fixarray)', () => {
  const doc = encode({ columns: ['uid', 'age'], rows: [[1, 'x'], [2, 'y']], gen: 7, kind: 'full', cutoff: 'c' });
  const h = parseChainHead(doc);
  assert.ok(h);
  assert.deepEqual(h.columns, ['uid', 'age']);
  assert.equal(h.nrows, 2);
  // everything after the head is one value per row, then the tail's keys and values
  const rest = [...decodeMulti(doc.subarray(h.offset))];
  assert.deepEqual(rest.slice(0, 2), [[1, 'x'], [2, 'y']]);
  assert.deepEqual(rest.slice(2), ['gen', 7, 'kind', 'full', 'cutoff', 'c']);
});

test('parseChainHead: the producer\'s spelling — array32 row count, a str8 column name', () => {
  const name = 'a'.repeat(40);                       // > 31 bytes: str8, not fixstr
  const head = new Uint8Array([
    0x86,                                             // fixmap(6)
    0xa7, ...Buffer.from('columns'),
    0x91, 0xd9, name.length, ...Buffer.from(name),    // fixarray(1) [ str8 name ]
    0xa4, ...Buffer.from('rows'),
    0xdd, ...new Uint8Array(new Uint32Array([3_055_002]).buffer).reverse(), // array32, big-endian
  ]);
  const h = parseChainHead(head);
  assert.ok(h);
  assert.deepEqual(h.columns, [name]);
  assert.equal(h.nrows, 3_055_002);
  assert.equal(h.offset, head.length);
});

test('parseChainHead: short of the head is null (buffer more), not a chain document is null (fall back)', () => {
  const doc = encode({ columns: ['uid'], rows: [[1]] });
  for (let n = 0; n < doc.length; n++) {
    // every prefix that stops before the row-count header must ask for more
    const h = parseChainHead(doc.subarray(0, n));
    if (h) { assert.ok(n >= 12, `parsed from only ${n} bytes`); break; }
  }
  assert.equal(parseChainHead(encode([1, 2, 3])), null);
  assert.equal(parseChainHead(encode({ rows: [], columns: [] })), null); // keys in the wrong order
});

test('chainStageSql: the bulk picks into a keyless stage, no conflict clause', () => {
  const sql = chainStageSql('_zb_seed_stage', ['uid', 'age']);
  assert.equal(sql, `INSERT INTO _zb_seed_stage ("uid", "age") SELECT json_extract(value, '$[0]'), json_extract(value, '$[1]') FROM json_each(?)`);
  assert.ok(!/ON CONFLICT/.test(sql));
});

// §10ix: the streaming digest a browser or a phone uses in place of node:crypto. The
// object store's digest is SHA-256 over the whole object; the streamed read folds chunks
// in as they pass. This pins that js-sha256's incremental result IS Web Crypto's.
test('streaming SHA-256 (js-sha256, chunked) equals crypto.subtle.digest (one shot)', async () => {
  const bytes = new Uint8Array(300_007);
  for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 2654435761) >>> 24;
  const h = sha256.create();
  for (let o = 0; o < bytes.length; o += 131_072) h.update(bytes.subarray(o, Math.min(o + 131_072, bytes.length)));
  const chunked = new Uint8Array(h.arrayBuffer());
  const oneShot = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  assert.deepEqual(chunked, oneShot);
});


// §10ja: the streamed seed decodes a chunk of rows at once; the scanner must count
// exactly the complete values at ANY cut, or a row is lost or decoded half.
test('msgpackScan: at every cut, only complete values count — every type and size class', () => {
  const vals: unknown[] = [0, 127, -1, -32, -33, 128, 255, 256, 65535, 65536, 2 ** 32, -(2 ** 31) - 1, 1.5, 2 ** 53 - 1,
    null, true, false, '', 'a'.repeat(31), 'b'.repeat(32), 'c'.repeat(255), 'd'.repeat(256), 'e'.repeat(70_000),
    new Uint8Array(3), new Uint8Array(300), new Uint8Array(70_000), new Date(1e12), new Date(1_700_000_000_000), new Date(-1e12),
    [], [1, [2, [3, {}]]], Array.from({ length: 20 }, (_, i) => i), Array.from({ length: 70_000 }, () => 1),
    { a: 1, b: [2, 'x'], c: { d: null } }, Object.fromEntries(Array.from({ length: 20 }, (_, i) => ['k' + i, i])),
    ['row', 42, 3.25, null, 'z'.repeat(40), new Uint8Array(16)]];
  const parts = vals.map((v) => encode(v));
  const bounds: number[] = [];
  let off = 0;
  for (const p of parts) { off += p.length; bounds.push(off); }
  const all = new Uint8Array(off);
  let o = 0;
  for (const p of parts) { all.set(p, o); o += p.length; }
  const cuts = new Set<number>();
  for (let c = 0; c <= all.length; c += c < 2000 ? 1 : 97) cuts.add(c);
  for (const b of bounds) for (const d of [-1, 0, 1]) if (b + d >= 0 && b + d <= all.length) cuts.add(b + d);
  for (const cut of cuts) {
    const { end, count } = msgpackScan(all.subarray(0, cut), 1e9);
    const expect = bounds.filter((b) => b <= cut).length;
    assert.equal(count, expect, `cut ${cut}`);
    assert.equal(end, expect ? bounds[expect - 1] : 0, `cut ${cut}`);
  }
  assert.deepEqual(msgpackScan(all, 3), { end: bounds[2], count: 3 });
  const { end, count } = msgpackScan(all, 1e9);
  assert.deepEqual(msgpackDecodeScanned(decode, all, end, count), vals.map((v) => decode(encode(v))));
  assert.throws(() => msgpackScan(new Uint8Array([0xc1]), 1), /not a type/);
});

test('a stored identity is usable with creds and a NATS URL for its platform', async () => {
  const { identityUsable } = await import('./libzb.ts');
  const creds = '-----BEGIN NATS USER JWT-----\nx\n------END NATS USER JWT------';
  const both = JSON.stringify({ creds, nats_url: 'tls://n:4222', nats_ws_url: 'wss://w' });
  const tcpOnly = JSON.stringify({ creds, nats_url: 'tls://n:4222' });
  assert.equal(identityUsable(both, undefined, true), true);
  assert.equal(identityUsable(tcpOnly, undefined, false), true);
  // a browser needs the websocket URL: a TCP-only identity is not enough, unless the app passes one
  assert.equal(identityUsable(tcpOnly, undefined, true), false);
  assert.equal(identityUsable(tcpOnly, 'wss://app-given', true), true);
  // no creds, or text that does not parse: not usable
  assert.equal(identityUsable(JSON.stringify({ nats_ws_url: 'wss://w' }), undefined, true), false);
  assert.equal(identityUsable('{not json', undefined, true), false);
});
