/// The conformance runner: every case in ../fixtures/core-fixtures.json against
/// the TS core. A port (Zig, …) writes its own thin runner over the SAME file —
/// the fixtures are the spec, this file is just plumbing.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import {
  nextVersion, subjectSafeToken, buildMutation,
  columnDdl, fkClausesFor, createTableSteps, rebuildSteps, diffColumns,
  fkTextDiffers, viewSteps, indexSyncPlan,
  planKeyChange, planUpsert, planDelete, chainUpsertSql, chainRowParams,
  seedGateDrops, planFromManifest, fullPredatesReplica, scopeSeeding,
  advancePosition, foreignKeyFailureKind, pgTsToWire, lsnToNumber,
} from './core.ts';

const here = dirname(fileURLToPath(import.meta.url));
const fx = JSON.parse(readFileSync(join(here, '..', 'fixtures', 'core-fixtures.json'), 'utf8'));

for (const c of fx.seedGate) {
  test(`seedGate: ${c.name}`, () => assert.equal(seedGateDrops(c.ev, c.anchor), c.drops));
}
for (const c of fx.chainPlan) {
  test(`chainPlan: ${c.name}`, () => assert.deepEqual(planFromManifest(c.manifest, c.watermark), c.plan));
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
  test(`envelope: ${c.name}`, () => assert.deepEqual(buildMutation(c.args), c.out));
}
