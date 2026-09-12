/// The conformance runner: every case in ../fixtures/core-fixtures.json against
/// the TS core. A port (Zig, …) writes its own thin runner over the SAME file —
/// the fixtures are the spec, this file is just plumbing.
import { test } from 'node:test';
import { cdcValue, pgEngineValues, isBytes, pgArrayValues, sortRowsByKey, chainBulkSql } from './core.ts';

test('a chunk in one statement through json_each, version-guarded (§10fc)', () => {
  const sql = chainBulkSql('t', ['uid', 'n', 'updated_at'], ['uid'], 'updated_at');
  assert.equal(sql, `INSERT INTO t ("uid", "n", "updated_at") SELECT json_extract(value, '$[0]'), json_extract(value, '$[1]'), json_extract(value, '$[2]') FROM json_each(?) WHERE true ON CONFLICT("uid") DO UPDATE SET "n" = excluded."n", "updated_at" = excluded."updated_at" WHERE excluded."updated_at" > t."updated_at"`);
  assert.ok(chainBulkSql('t', ['uid'], ['uid'], null).endsWith('DO NOTHING'));
});

test('chain rows sort by their key cell, stable for the rest (§10fb)', () => {
  const rows = [['c', 1], ['a', 2], ['b', 3]];
  assert.deepEqual(sortRowsByKey(rows, 0).map((r) => r[0]), ['a', 'b', 'c']);
  assert.deepEqual(sortRowsByKey([[3, 'x'], [1, 'y'], [2, 'z']], 0).map((r) => r[0]), [1, 2, 3]);
  assert.deepEqual(sortRowsByKey(rows, -1), rows);
  assert.deepEqual(rows.map((r) => r[0]), ['c', 'a', 'b']); // the input is untouched
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
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import {
  normalizeVersion, hlcVersion,
  nextVersion, subjectSafeToken, buildMutation,
  columnDdl, fkClausesFor, createTableSteps, rebuildSteps, diffColumns,
  fkTextDiffers, viewSteps, indexSyncPlan,
  planKeyChange, planUpsert, planUpdate, planExists, planDelete, pgArrayLiteral, chainUpsertSql, chainRowParams,
  seedGateDrops, tombstoned, planFromManifest, fullPredatesReplica, scopeSeeding,
  advancePosition, foreignKeyFailureKind, pgTsToWire, lsnToNumber,
  outboxWatermarkGate,
  heartbeatPayload,
  keyShape, typeShape, retypedColumns, isReadOnlySql,
} from './core.ts';

const here = dirname(fileURLToPath(import.meta.url));
const fx = JSON.parse(readFileSync(join(here, '..', 'fixtures', 'core-fixtures.json'), 'utf8'));

// §10dq: the package's grammar is the bridge's, byte for byte. The bridge embeds
// src/grammar.json; this copy ships inside the package because a file outside its
// root cannot. A drift here is a protocol fork, and this is where it turns red.
test('grammar: the packaged copy is byte-identical to src/grammar.json', () => {
  const packaged = readFileSync(join(here, 'grammar.json'), 'utf8');
  const source = readFileSync(join(here, '..', '..', 'src', 'grammar.json'), 'utf8');
  assert.equal(packaged, source);
});

for (const c of fx.heartbeat) {
  test(`heartbeat: ${c.name}`, () => assert.equal(heartbeatPayload(c.principal, c.tenant, c.ts, c.seqs), c.out));
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

for (const c of fx.columnDdl) {
  test(`columnDdl: ${c.name}`, () => assert.equal(columnDdl(c.col, c.pkCols), c.ddl));
}
for (const c of fx.fkClauses) {
  test(`fkClauses: ${c.name}`, () => assert.equal(fkClausesFor(c.fks), c.text));
}
for (const c of fx.createTable) {
  test(`createTable: ${c.name}`, () =>
    assert.deepEqual(createTableSteps(c.table, c.cols, c.pkCols, c.fks), c.steps));
}
for (const c of fx.rebuildSteps) {
  test(`rebuildSteps: ${c.name}`, () =>
    assert.deepEqual(rebuildSteps(c.table, c.cols, c.pkCols, c.fks, c.existing), c.steps));
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
