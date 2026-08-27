/// The conformance runner: every case in ../fixtures/core-fixtures.json against
/// the TS core. A port (Zig, …) writes its own thin runner over the SAME file —
/// the fixtures are the spec, this file is just plumbing.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import {
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
