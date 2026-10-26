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
///
/// A rewind replays a route's events to EVERY table on it. The tables the fence leaves
/// untouched already hold those events, so each gets an APPLIED FRONTIER (the recovery
/// model's §1): the route's position when the fence was prepared, kept with the identity of
/// the stream it was taken on, and replay skips their events at or below it. Version guards
/// alone cannot: an insert at 11 and a tombstone at 12 applied, a rewind to 10 — the insert
/// at 11 recreates a row no surviving version rejects. The frontier is spent when the
/// position passes it, gone when the table is replaced by a chain or the stream restarts.
/// A table whose coverage is unknown (not seeded) cannot stay untouched: the fence is refused.

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
  /// Why the other tables on the own route cannot stay untouched (`fenceScope`), or null.
  ownScopeRefusal: string | null;
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
  /// The same for the shared route.
  sharedScopeRefusal: string | null;
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
  // A rewind of the own route (the chain behind the cursor) needs the other tables on it to
  // stay untouched; a pending retry already at or before this cut rewinds nothing there.
  if (f.predates && f.ownScopeRefusal !== null) {
    return { kind: 'refuse', owed, why: `chain predates replica — refusing the full (D2): ${f.ownScopeRefusal}` };
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
      if (f.sharedScopeRefusal !== null) {
        return { kind: 'refuse', owed, why: `chain predates replica — refusing the full (D2): ${f.sharedScopeRefusal}` };
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

/// A table's applied frontier on one route: it holds every event of `stream` up to `seq`.
export interface Frontier {
  stream: string;
  seq: number;
}

/// The frontier gate: an event at or below its table's frontier on its stream is already
/// in the table. (An event with no stream or sequence — a local write — is never gated.)
export function frontierDrops(ev: { stream?: string; seq?: number }, frontiers: readonly Frontier[] | undefined): boolean {
  if (!frontiers?.length || typeof ev.seq !== 'number' || !ev.stream) return false;
  for (const f of frontiers) if (f.stream === ev.stream && ev.seq <= f.seq) return true;
  return false;
}

/// A table left untouched by a fence, with the route whose events it already holds.
export interface Untouched {
  table: string;
  stream: string;
}

/// The fence's scope rule (the recovery model's §3, R3): which OTHER tables ride any of
/// `streams`, and can they stay untouched? Each gets an applied frontier at the route's
/// position. That needs its coverage to be KNOWN: a table that is not seeded has none, and
/// the fence is refused. `routesOf` gives a table's own route and its shared route
/// (CDC_PUBLIC for a tenant table), null for a table not followed (on-demand, or no tenant
/// for this principal), which rides nothing. An empty stream name matches nothing.
export function fenceScope(
  table: string,
  streams: readonly string[],
  tables: Iterable<string>,
  routesOf: (t: string) => { route: string; sharedRoute?: string } | null,
  seeded: (t: string) => boolean,
): { refused: string | null; untouched: Untouched[] } {
  const wanted = streams.filter((s) => s.length > 0);
  const untouched: Untouched[] = [];
  for (const t of tables) {
    if (t === table) continue;
    const r = routesOf(t);
    if (!r) continue;
    for (const s of wanted) {
      if (r.route !== s && !(r.sharedRoute && r.sharedRoute === s)) continue;
      if (!seeded(t)) return { refused: `${t} is not seeded, so its coverage on ${s} is unknown`, untouched: [] };
      if (!untouched.some((u) => u.table === t && u.stream === s)) untouched.push({ table: t, stream: s });
    }
  }
  return { refused: null, untouched };
}
