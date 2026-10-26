// The fence's decision (fence.ts): the same conditions libzb's client.zig applies inline.
import test from 'node:test';
import assert from 'node:assert/strict';
import { decideFence, fenceScope, frontierDrops, routeHolds, verifyRoutes, type FenceFacts } from './fence.ts';

/// A follower ahead of a new cut on its own route, the other tables on it fine to leave untouched, nothing shared.
const base: FenceFacts = {
  predates: true, pending: false,
  ownStream: 'CDC_acme', ownPos: 500, ownCut: 300, ownScopeRefusal: null, ownReplayable: true,
  sharedStream: null, manifestSharedStream: '', sharedCut: null, sharedProven: false, sharedPos: 0, sharedScopeRefusal: null,
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

test('other tables on the own route that cannot stay untouched: the old refusal (D2) stands, with the scope\'s reason', () => {
  const d = decideFence({ ...base, ownScopeRefusal: 'items is not seeded, so its coverage on CDC_acme is unknown' });
  assert.equal(d.kind, 'refuse');
  assert.match((d as { why: string }).why, /refusing the full \(D2\): items is not seeded/);
});

test('an owed fence whose cursor is at the cut needs no sole-table scope on the own route', () => {
  // predates is false (the cursor sits at the cut): a retry rewinds nothing there.
  assert.deepEqual(decideFence({ ...base, predates: false, pending: true, ownPos: 300, ownScopeRefusal: 'x' }), { kind: 'fence', ownTo: 300, sharedTo: null });
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
  assert.equal((decideFence({ ...base, ownScopeRefusal: 'x' }) as { owed: boolean }).owed, false);
  assert.equal((decideFence({ ...base, ownScopeRefusal: 'x', pending: true }) as { owed: boolean }).owed, true);
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

test('a scope refusal on the shared route matters only when the shared route must rewind', () => {
  assert.equal(decideFence(withShared({ sharedScopeRefusal: 'x' })).kind, 'refuse');
  // Nothing to rewind on the shared route: the other tables there are not touched.
  assert.deepEqual(decideFence(withShared({ sharedScopeRefusal: 'x', sharedPos: 30 })), { kind: 'fence', ownTo: 300, sharedTo: null });
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

test('fenceScope: the other tables on a route stay untouched when seeded; an unseeded one refuses', () => {
  const routes: Record<string, { route: string; sharedRoute?: string } | null> = {
    orders: { route: 'CDC_acme', sharedRoute: 'CDC_PUBLIC' },
    items: { route: 'CDC_acme', sharedRoute: 'CDC_PUBLIC' },
    prices: { route: 'CDC_PUBLIC' },
    notes: null, // on demand: not followed
    elsewhere: { route: 'CDC_globex', sharedRoute: 'CDC_PUBLIC' },
  };
  const tables = Object.keys(routes);
  const of = (t: string) => routes[t] ?? null;
  const all = () => true;
  // orders rewinds CDC_acme: items rides it, and keeps its rows.
  assert.deepEqual(fenceScope('orders', ['CDC_acme'], tables, of, all), { refused: null, untouched: [{ table: 'items', stream: 'CDC_acme' }] });
  // CDC_PUBLIC: items and elsewhere share it, prices rides it; each is listed once.
  const pub = fenceScope('orders', ['CDC_PUBLIC'], tables, of, all);
  assert.equal(pub.refused, null);
  assert.deepEqual(pub.untouched.map((u) => u.table).sort(), ['elsewhere', 'items', 'prices']);
  // A table alone on its stream, or with nothing else near: nothing to leave untouched.
  assert.deepEqual(fenceScope('elsewhere', ['CDC_globex'], tables, of, all), { refused: null, untouched: [] });
  assert.deepEqual(fenceScope('notes', ['CDC_acme'], ['notes'], of, all), { refused: null, untouched: [] });
  // Empty stream names match nothing.
  assert.deepEqual(fenceScope('orders', ['', ''], tables, of, all), { refused: null, untouched: [] });
  // An unseeded table's coverage is unknown: the fence is refused, whichever route it rides.
  const refused = fenceScope('orders', ['CDC_acme'], tables, of, (t) => t !== 'items');
  assert.match(refused.refused ?? '', /items is not seeded, so its coverage on CDC_acme is unknown/);
  assert.deepEqual(refused.untouched, []);
  // A table that is not followed is not asked about at all.
  assert.equal(fenceScope('orders', ['CDC_acme'], ['orders', 'notes'], of, () => false).refused, null);
});

test('frontierDrops: an event at or below its table\'s frontier on ITS stream; local writes never', () => {
  const fr = [{ stream: 'CDC_acme', seq: 500 }, { stream: 'CDC_PUBLIC', seq: 90 }];
  assert.equal(frontierDrops({ stream: 'CDC_acme', seq: 500 }, fr), true);
  assert.equal(frontierDrops({ stream: 'CDC_acme', seq: 301 }, fr), true);
  assert.equal(frontierDrops({ stream: 'CDC_acme', seq: 501 }, fr), false);
  // Another stream's frontier says nothing about this one.
  assert.equal(frontierDrops({ stream: 'CDC_PUBLIC', seq: 91 }, fr), false);
  assert.equal(frontierDrops({ stream: 'CDC_PUBLIC', seq: 90 }, fr), true);
  assert.equal(frontierDrops({ stream: 'CDC_other', seq: 1 }, fr), false);
  // No frontier, no stream, no sequence: not gated.
  assert.equal(frontierDrops({ stream: 'CDC_acme', seq: 1 }, undefined), false);
  assert.equal(frontierDrops({ stream: 'CDC_acme', seq: 1 }, []), false);
  assert.equal(frontierDrops({ seq: 1 }, fr), false);
  assert.equal(frontierDrops({ stream: 'CDC_acme' }, fr), false);
});
