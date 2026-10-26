// The fence's decision (fence.ts): the same conditions libzb's client.zig applies inline.
import test from 'node:test';
import assert from 'node:assert/strict';
import { decideFence, routeHolds, soleDependent, verifyRoutes, type FenceFacts } from './fence.ts';

/// A follower ahead of a new cut on its own route, alone on it, nothing shared.
const base: FenceFacts = {
  predates: true, pending: false,
  ownStream: 'CDC_acme', ownPos: 500, ownCut: 300, soleOwn: true, ownReplayable: true,
  sharedStream: null, manifestSharedStream: '', sharedCut: null, sharedProven: false, sharedPos: 0, soleShared: true,
};
const withShared = (over: Partial<FenceFacts> = {}): FenceFacts => ({
  ...base, sharedStream: 'CDC_PUBLIC', manifestSharedStream: 'CDC_PUBLIC', sharedCut: 40, sharedProven: true, sharedPos: 90, ...over,
});

test('a chain that does not predate the replica, with nothing owed, is no fence', () => {
  assert.deepEqual(decideFence({ ...base, predates: false }), { kind: 'none' });
});

test('a chain cut behind the position, alone on its route: a fence back to the cut', () => {
  assert.deepEqual(decideFence(base), { kind: 'fence', ownTo: 300, sharedTo: null });
});

test('the cursor never moves forward: a cut past the position keeps the position', () => {
  // An owed retry whose cursor already sits at an older cut than the new chain's.
  assert.deepEqual(decideFence({ ...base, predates: false, pending: true, ownPos: 120, ownCut: 300 }), { kind: 'fence', ownTo: 120, sharedTo: null });
});

test('another table on the own route: the old refusal (D2) stands', () => {
  const d = decideFence({ ...base, soleOwn: false });
  assert.equal(d.kind, 'refuse');
  assert.match((d as { why: string }).why, /other tables ride CDC_acme/);
});

test('an owed fence whose cursor is at the cut needs no sole-table scope on the own route', () => {
  // predates is false (the cursor sits at the cut): a retry rewinds nothing there.
  assert.deepEqual(decideFence({ ...base, predates: false, pending: true, ownPos: 300, soleOwn: false }), { kind: 'fence', ownTo: 300, sharedTo: null });
});

test('a cut that cannot be proven replayable refuses, owed or not', () => {
  const d = decideFence({ ...base, ownReplayable: false });
  assert.equal(d.kind, 'refuse');
  assert.match((d as { why: string }).why, /cannot be proven replayable/);
  const owed = decideFence({ ...base, predates: false, pending: true, ownReplayable: false });
  assert.equal(owed.kind, 'refuse');
  assert.equal((owed as { owed: boolean }).owed, true);
});

test('a refusal says whether a fence is still owed', () => {
  assert.equal((decideFence({ ...base, soleOwn: false }) as { owed: boolean }).owed, false);
  assert.equal((decideFence({ ...base, soleOwn: false, pending: true }) as { owed: boolean }).owed, true);
});

test('a shared route: its coverage must be proven, and unknown coverage refuses', () => {
  // Past the shared cut, the shared route rewinds too.
  assert.deepEqual(decideFence(withShared()), { kind: 'fence', ownTo: 300, sharedTo: 40 });
  // At or before the shared cut there is nothing to rewind there, but it is still proven.
  assert.deepEqual(decideFence(withShared({ sharedPos: 40 })), { kind: 'fence', ownTo: 300, sharedTo: null });
  const unknown = decideFence(withShared({ sharedCut: null }));
  assert.equal(unknown.kind, 'refuse');
  assert.match((unknown as { why: string }).why, /coverage on CDC_PUBLIC is unknown/);
});

test('a shared cut on an unproven incarnation, or on another stream, is refused, never left out', () => {
  assert.equal(decideFence(withShared({ sharedProven: false })).kind, 'refuse');
  assert.equal(decideFence(withShared({ manifestSharedStream: 'CDC_OTHER' })).kind, 'refuse');
});

test('another table on the shared route refuses only when the shared route must rewind', () => {
  assert.equal(decideFence(withShared({ soleShared: false })).kind, 'refuse');
  // Nothing to rewind on the shared route: the other tables there are not touched.
  assert.deepEqual(decideFence(withShared({ soleShared: false, sharedPos: 30 })), { kind: 'fence', ownTo: 300, sharedTo: null });
});

test('VERIFY: the same incarnation and the event after the cut still held', () => {
  const route = { stream: 'CDC_acme', cut: 300, created: '2026-10-26T07:00:00Z' };
  assert.equal(routeHolds(route, { created: route.created, firstSeq: 301 }), true);
  assert.equal(routeHolds(route, { created: route.created, firstSeq: 1 }), true);
  // Retention overtook the cut while the rows were replaced: the event after it is gone.
  assert.equal(routeHolds(route, { created: route.created, firstSeq: 302 }), false);
  // Another incarnation (the stream was recreated), or the stream unreadable.
  assert.equal(routeHolds(route, { created: '2026-10-26T08:00:00Z', firstSeq: 1 }), false);
  assert.equal(routeHolds(route, null), false);
});

test('VERIFY names the first route that does not hold, and checks every route', () => {
  const routes = [
    { stream: 'CDC_acme', cut: 300, created: 'a' },
    { stream: 'CDC_PUBLIC', cut: 40, created: 'b' },
  ];
  const seen: string[] = [];
  const ok = verifyRoutes(routes, (s) => { seen.push(s); return { created: s === 'CDC_acme' ? 'a' : 'b', firstSeq: 1 }; });
  assert.equal(ok, null);
  assert.deepEqual(seen, ['CDC_acme', 'CDC_PUBLIC']);
  const bad = verifyRoutes(routes, (s) => (s === 'CDC_PUBLIC' ? null : { created: 'a', firstSeq: 1 }));
  assert.equal(bad?.stream, 'CDC_PUBLIC');
});

test('soleDependent: another table on the own or the shared route, and routes not followed', () => {
  const routes: Record<string, { route: string; sharedRoute?: string } | null> = {
    orders: { route: 'CDC_acme', sharedRoute: 'CDC_PUBLIC' },
    items: { route: 'CDC_acme' },
    prices: { route: 'CDC_PUBLIC' },
    notes: null, // on demand: not followed
    elsewhere: { route: 'CDC_globex', sharedRoute: 'CDC_PUBLIC' },
  };
  const tables = Object.keys(routes);
  const of = (t: string) => routes[t] ?? null;
  // orders shares CDC_acme with items.
  assert.equal(soleDependent('orders', ['CDC_acme'], tables, of), false);
  // On CDC_PUBLIC alone: prices rides it and elsewhere shares it.
  assert.equal(soleDependent('orders', ['CDC_PUBLIC'], tables, of), false);
  // A table alone on its stream; empty stream names are ignored.
  assert.equal(soleDependent('elsewhere', ['CDC_globex', ''], tables, of), true);
  assert.equal(soleDependent('notes', ['CDC_acme'], ['notes'], of), true);
});
