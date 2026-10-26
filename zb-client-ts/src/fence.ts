/// The fence: replacing a replica's rows with a chain cut BEHIND the replica's position,
/// without losing the events in between.
///
/// A follower stays ahead of every new cut, so refusing such a chain (D2, core.ts's
/// `fullPredatesReplica`) left it without a seed for as long as writes went on. The fence
/// takes the chain and moves the routes' cursors back to its cuts, in the recovery model's
/// order (libzb's client.zig, the same order):
///
///   PREPARE  one transaction before any row changes: the pair durably unseeded (its
///            generations row gone), the routes' cursors at the cuts (never forward), and
///            the fence recorded as OWED (`_zebridge_fence`). The tails on those routes
///            stop, the table holds its events.
///   replace  the chain's steps, window by window.
///   VERIFY   fresh stream facts for every route the table depends on: the same
///            incarnation, and the event after each cut still held. Otherwise no anchor:
///            the table stays unseeded with its cursor at the cut, the fence still owed.
///   COMMIT   the anchor (the generations row) and the owed record's removal, one
///            transaction. Only then the in-memory anchor.
///
/// A fence interrupted before its commit is still owed: the retry is a fence too, with
/// every proof, though its cursor now sits at the cut and the chain no longer "predates"
/// it. This file is the DECISION, as pure functions (the I/O is libzb.ts); libzb's
/// client.zig makes the same decision inline.

/// What the caller learned before any row changes.
export interface FenceFacts {
  /// core.fullPredatesReplica: a full in the plan is cut behind the replica's position.
  predates: boolean;
  /// An earlier fence of this table was interrupted before its commit (`_zebridge_fence`).
  pending: boolean;
  /// The table's own route, the replica's position on it, and the chain's cut on it.
  ownStream: string;
  ownPos: number;
  ownCut: number;
  /// Only this table rides the own route: a rewind would replay no other table's events.
  soleOwn: boolean;
  /// The own route's cut is replayable: the stream was read, on the incarnation the chain
  /// was cut on, and the seed gate is not zeroed (`gateSeq === cutoff_seq`).
  ownReplayable: boolean;
  /// The table's shared route (CDC_PUBLIC, for a tenant table's open-tenant rows), or null.
  sharedStream: string | null;
  /// The shared stream the manifest names, and its cut on it: null when the stream could
  /// not be read, or the cut was taken on another incarnation (nothing is proved there).
  manifestSharedStream: string;
  sharedCut: number | null;
  /// The shared cut is proven: both incarnations known and equal.
  sharedProven: boolean;
  sharedPos: number;
  /// Only this table rides the shared route.
  soleShared: boolean;
}

export type FenceDecision =
  /// Not a fence: the chain does not predate the replica and nothing is owed.
  | { kind: 'none' }
  /// A fence: the cursors go to `ownTo` (and `sharedTo` when the shared route rewinds too).
  | { kind: 'fence'; ownTo: number; sharedTo: number | null }
  /// Refused: the old refusal (D2) stands. `why` is the log line; `owed` says a fence is
  /// still owed, so the caller keeps asking.
  | { kind: 'refuse'; why: string; owed: boolean };

export function decideFence(f: FenceFacts): FenceDecision {
  if (!f.predates && !f.pending) return { kind: 'none' };
  const owed = f.pending;
  // A rewind of the own route (the chain behind the cursor) needs the sole-table scope; a
  // pending retry already at or before this cut rewinds nothing there.
  if (f.predates && !f.soleOwn) {
    return { kind: 'refuse', owed, why: `chain predates replica — refusing the full (D2): other tables ride ${f.ownStream}, and a rewind would replay their events` };
  }
  if (!f.ownReplayable) {
    return { kind: 'refuse', owed, why: `chain predates replica — refusing the full (D2): its cut on ${f.ownStream} cannot be proven replayable (stream unread, or another incarnation)` };
  }
  // A table that rides a shared route depends on it: under the fence its coverage there
  // must be PROVEN. Unknown coverage refuses the fence; it is never a route left out.
  let sharedTo: number | null = null;
  if (f.sharedStream !== null) {
    if (f.sharedCut === null) {
      return { kind: 'refuse', owed, why: `chain predates replica — refusing the full (D2): its coverage on ${f.sharedStream} is unknown (no certificate, the stream unread, or another incarnation)` };
    }
    if (!f.sharedProven || f.sharedStream !== f.manifestSharedStream) {
      return { kind: 'refuse', owed, why: `chain predates replica — refusing the full (D2): its shared cut on ${f.sharedStream} cannot be proven replayable` };
    }
    if (f.sharedPos > f.sharedCut) {
      if (!f.soleShared) {
        return { kind: 'refuse', owed, why: `chain predates replica — refusing the full (D2): other tables ride ${f.sharedStream}, and its rewind would replay their events` };
      }
      sharedTo = f.sharedCut;
    }
  }
  return { kind: 'fence', ownTo: Math.min(f.ownPos, f.ownCut), sharedTo };
}

/// A route the fence depends on, as the VERIFY step checks it.
export interface FenceRoute {
  stream: string;
  cut: number;
  /// The incarnation the chain was cut on ('' when the manifest carries none).
  created: string;
}

/// VERIFY, one route: fresh facts from the stream. Same incarnation, and the event after
/// the cut still held. A stream that could not be read fails the route.
export function routeHolds(route: FenceRoute, now: { created: string; firstSeq: number } | null): boolean {
  if (!now) return false;
  return now.created === route.created && now.firstSeq <= route.cut + 1;
}

/// Every route holds, or the name of the first that does not.
export function verifyRoutes(routes: FenceRoute[], facts: (stream: string) => { created: string; firstSeq: number } | null): FenceRoute | null {
  for (const r of routes) if (!routeHolds(r, facts(r.stream))) return r;
  return null;
}

/// The tables whose routes include any of `streams`, other than `table`: the ones a
/// rewind of those streams would replay events onto. `routesOf` gives a table's own route
/// and its shared route (CDC_PUBLIC for a tenant table), null for a table not followed
/// (on-demand, or no tenant for this principal).
export function soleDependent(
  table: string,
  streams: readonly string[],
  tables: Iterable<string>,
  routesOf: (t: string) => { route: string; sharedRoute?: string } | null,
): boolean {
  const wanted = streams.filter((s) => s.length > 0);
  for (const t of tables) {
    if (t === table) continue;
    const r = routesOf(t);
    if (!r) continue;
    for (const s of wanted) {
      if (r.route === s) return false;
      if (r.sharedRoute && r.sharedRoute === s) return false;
    }
  }
  return true;
}
