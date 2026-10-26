import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Measure, MeasureAttempt } from './measure.ts';
import { ZeBridge } from './libzb.ts';
import { loadCore } from './wasm-core.ts';
import { readFileSync } from 'node:fs';

test('rolled-back measurement attempts do not leak holds into committed totals', () => {
  const first = {}, second = {};
  const aborted = new MeasureAttempt([{ ev: first, gated: false }, { ev: second, gated: false }]);
  aborted.held(first);
  // Discard the entire failed attempt; a successful retry starts from its own outcomes.
  const committed = new MeasureAttempt([{ ev: first, gated: false }, { ev: second, gated: false }]);
  committed.held(first);
  committed.held(first);
  committed.failed(second);
  assert.deepEqual(committed.snapshot(), { messages: 0, bytes: 0, events: 2, applied: 0, gated: 0, held: 1, failed: 1 });
});

test('isolated replay counts only final outcomes, including a commit-time failure', async () => {
  await loadCore(readFileSync(new URL('../wasm/zb_core.wasm', import.meta.url)));
  const oldMeasure = process.env.ZB_MEASURE, oldTargets = process.env.ZB_CATCHUP_TARGETS;
  process.env.ZB_MEASURE = '1'; process.env.ZB_CATCHUP_TARGETS = 'CDC_acme=4';
  try {
    const lines: string[] = [];
    const c: any = Object.create(ZeBridge.prototype);
    c.measure = new Measure((line) => lines.push(line));
    c.frontiers = new Map();
    c.syncedTables = new Map([['item', {}], ['seeded', { seedSeq: 4, seedStream: 'CDC_acme' }]]);
    c.appendLog = () => {}; c.triggerChange = () => {};
    c.dialect = { deferForeignKeys: async () => {} };
    let failCommit = true;
    c.transaction = async (fn: any) => {
      await fn(async () => []);
      if (failCommit) { failCommit = false; throw new Error('FOREIGN KEY constraint failed'); }
    };
    c.applyEvent = async (_table: string, ev: any, _exec: any, _seed: boolean, attempt: MeasureAttempt) => {
      if (ev.seq === 1) attempt?.held(ev); // tentative hold, then COMMIT fails
      if (ev.seq === 2) throw new Error('bad SQL');
    };
    c.holdEvent = async (_table: string, ev: any, _reason: string, attempt: MeasureAttempt) => attempt?.held(ev);
    const entries = [1, 2, 3, 4, 5].map((seq) => ({ table: seq === 4 ? 'seeded' : 'item', ev: { stream: 'CDC_acme', seq, data: {} } }));
    const counts = await c.applyBatchIsolated('CDC_acme', entries, 'batch failed');
    assert.deepEqual(counts, { messages: 0, bytes: 0, events: 4, applied: 1, gated: 1, held: 1, failed: 1 });
    c.measure.add('CDC_acme', counts);
    c.measure.reached('CDC_acme', 'position', 4);
    const record = JSON.parse(lines[0].slice('measure '.length));
    assert.equal(record.failed, 1);
    assert.equal(record.applied, 1);
    assert.equal(record.held, 1);
  } finally {
    if (oldMeasure === undefined) delete process.env.ZB_MEASURE; else process.env.ZB_MEASURE = oldMeasure;
    if (oldTargets === undefined) delete process.env.ZB_CATCHUP_TARGETS; else process.env.ZB_CATCHUP_TARGETS = oldTargets;
  }
});
