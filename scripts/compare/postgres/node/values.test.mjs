import assert from 'node:assert/strict';
import test from 'node:test';
import { utcTimestamp } from './values.mjs';

test('preserves timestamp microseconds instead of truncating to Date milliseconds', () => {
  assert.equal(utcTimestamp('2026-01-01 00:00:00.123456+00'), '2026-01-01T00:00:00.123456Z');
});

test('normalizes missing and shortened fractions to the same six-digit representation', () => {
  assert.equal(utcTimestamp('2026-01-01 00:00:00+00'), '2026-01-01T00:00:00.000000Z');
  assert.equal(utcTimestamp('2026-01-01 00:00:00.1+00:00'), '2026-01-01T00:00:00.100000Z');
});

test('refuses non-UTC input rather than silently changing the instant', () => {
  assert.throws(() => utcTimestamp('2026-01-01 00:00:00.123456+01'));
});
