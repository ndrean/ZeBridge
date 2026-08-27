/// The sans-I/O core (NOTES §10s). Every function here is PURE — no NATS, no
/// SQLite, no clock, no logging. This module is the part of the client that a
/// port reimplements: the conformance fixtures in ../fixtures/core-fixtures.json
/// are the spec, and a Zig (or any other) core is correct exactly when it passes
/// them. Findings 7, 9, 10 and the D1/D2 work all live here as executable rules
/// rather than as prose.
///
/// The I/O shells (the NATS pump and the storage adapter in libzb.ts) call in;
/// nothing here calls out.

// ─── shared shapes ───────────────────────────────────────────────────────────

/// What a seed anchors on a table (set ONLY by applyGenerations — finding 10).
export type SeedAnchor = {
  /// Primary gate (finding 7): the CDC stream's commit-ordered sequence,
  /// captured by the producer BEFORE the chain build.
  seedSeq?: number;
  seedStream?: string;
  /// Legacy fallback for manifests without cutoff_seq. ⚠️ Must be a lsn a SEED
  /// set — never a schema event's lsn (finding 10: the boot schema-republish
  /// advances to the WAL head and would eat every replayed event).
  seedLsn?: number;
};

export type CoreEvent = { lsn?: number; seq?: number; stream?: string };

export type ManifestDelta = { object: string; cutoff: string; prev_cutoff: string; gen: number };
export type ChainManifest = {
  gen: number;
  full?: { object: string; gen: number } | null;
  deltas?: ManifestDelta[];
  cutoff_seq?: number;
  cdc_stream?: string;
};
export type PlanStep = { name: string; kind: 'full' | 'delta' };

// ─── the seed gate (findings 7 and 10) ───────────────────────────────────────

/// Should this CDC event be DROPPED as already contained in the table's seed?
///
/// Primary rule: stream sequence, because it is commit-ordered — lsn is NOT
/// (a transaction that begins early and commits late delivers late with a
/// LOWER lsn; measured, NOTES §10i). `<=` is safe in seq-space: no in-flight
/// transaction can land below the cutoff.
///
/// Fallback (legacy manifests only): STRICTLY less-than on the seed's lsn —
/// `<=` loses exactly one row per seed (the next commit is stamped with the
/// watermark's own lsn). No anchor at all → never drop: duplicates are
/// absorbed by the idempotent LWW upsert; a dropped row is gone forever.
export function seedGateDrops(ev: CoreEvent, anchor: SeedAnchor): boolean {
  if (typeof anchor.seedSeq === 'number' && anchor.seedStream) {
    return ev.stream === anchor.seedStream && typeof ev.seq === 'number' && ev.seq <= anchor.seedSeq;
  }
  return typeof anchor.seedLsn === 'number' && typeof ev.lsn === 'number' && ev.lsn < anchor.seedLsn;
}

// ─── chain planning (§10n) ───────────────────────────────────────────────────

/// The walk a client applies from a chain manifest, given its stored watermark
/// (the last applied cutoff_version, or null on a fresh table): deltas-only
/// when they reach the watermark, otherwise the full plus every delta after it.
export function planFromManifest(man: ChainManifest, watermark: string | null): PlanStep[] {
  const deltas: ManifestDelta[] = man.deltas ?? [];
  const applicable = watermark ? deltas.filter((d) => d.cutoff > watermark) : deltas;
  const reaches = watermark != null &&
    (applicable.length === 0 || applicable[0].prev_cutoff <= watermark);
  if (reaches) return applicable.map((d) => ({ name: d.object, kind: 'delta' as const }));
  if (!man.full) return [];
  return [
    { name: man.full.object, kind: 'full' as const },
    ...deltas.filter((d) => d.gen > man.full!.gen)
             .map((d) => ({ name: d.object, kind: 'delta' as const })),
  ];
}

/// D2's destruction guard: a chain-full is DELETE FROM + replay, so a chain
/// whose cutoff_seq is below the replica's stored position for that stream
/// would destroy rows the resumed CDC will never re-deliver — and cannot close
/// the gap being seeded either (the gap sits ABOVE the position it fails to
/// reach). Delta-only plans are upserts and need no gate; legacy manifests
/// (no cutoff_seq) stay ungated — lsn is not comparable across commits.
export function fullPredatesReplica(
  man: ChainManifest,
  plan: PlanStep[],
  storedSeqForStream: number,
): boolean {
  if (!plan.some((step) => step.kind === 'full')) return false;
  if (typeof man.cutoff_seq !== 'number' || man.cutoff_seq <= 0 || !man.cdc_stream) return false;
  return man.cutoff_seq < storedSeqForStream;
}

// ─── the gap rule and seeding scope (D2, §10n) ───────────────────────────────

export type StreamGap = { firstSeq: number; stored: number };

/// Per-stream, never per-table (the abandoned-table paradox). `stored === 0` is
/// the fresh-client case; `< firstSeq - 1` means the stream pruned past the
/// stored position. `stored === firstSeq - 1` is NOT a gap: the very next
/// message needed is the oldest one still held.
export function streamHasGap(g: StreamGap): boolean {
  return g.stored === 0 || (g.firstSeq > 0 && g.stored < g.firstSeq - 1);
}

/// Seeding is SCOPED: a gap on one stream re-seeds only the tables ROUTED to
/// that stream, plus tables never seeded at all (no generations watermark —
/// a brand-new replica, or a table enabled between two connects). Everything
/// else resumes untouched — a mobile client reconnecting with one stale
/// stream must not rebuild its whole replica.
export function scopeSeeding(
  streams: Record<string, StreamGap>,
  tables: Record<string, { route: string; seeded: boolean }>,
): { gapped: string[]; tablesToSeed: string[] } {
  const gapped = Object.entries(streams)
    .filter(([, g]) => streamHasGap(g))
    .map(([name]) => name);
  const gappedSet = new Set(gapped);
  const tablesToSeed = Object.entries(tables)
    .filter(([, t]) => gappedSet.has(t.route) || !t.seeded)
    .map(([name]) => name);
  return { gapped, tablesToSeed };
}

// ─── position accounting (D1, §10m) ──────────────────────────────────────────

/// Delivery + accounting IS the position: an applied event is in the tables, a
/// gated one is provably in the seeded chain, a held one is durably in the FK
/// inbox — all three account for the message. The position never moves
/// backwards, and an empty batch leaves it alone.
export function advancePosition(stored: number, batchSeqs: number[]): number {
  return batchSeqs.reduce((m, s) => Math.max(m, s ?? 0), stored);
}

// ─── FK failure classification (§10h) ────────────────────────────────────────

/// Is this failure "the parent is not here YET" rather than "this row is wrong"?
/// Three distinct SQLite messages, all measured:
///   FOREIGN KEY constraint failed   → the parent ROW is missing (hold + retry)
///   no such table: <parent>         → the parent TABLE is not created yet
///                                     (FK resolution is lazy at DDL, strict at DML)
///   foreign key mismatch            → the schema itself is wrong (drop loudly)
export function foreignKeyFailureKind(e: unknown): 'missing-parent' | 'mismatch' | null {
  const m = String((e as any)?.message ?? e);
  if (/foreign key mismatch/i.test(m)) return 'mismatch';
  if (/FOREIGN KEY constraint failed/i.test(m)) return 'missing-parent';
  if (/no such table: /i.test(m)) return 'missing-parent';
  return null;
}

// ─── wire-shape helpers ──────────────────────────────────────────────────────

/// PG text-mode timestamptz (UTC) → the CDC wire shape. String surgery,
/// microseconds preserved (`Date` would truncate to ms). The version guard
/// compares AS STRINGS: `' '` sorts before `'T'`, so unnormalized chain values
/// would lose every comparison against CDC-written ones (NOTES §1.13).
export const pgTsToWire = (v: any): any =>
  typeof v === 'string' && /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?\+00(:00)?$/.test(v)
    ? v.replace(' ', 'T').replace(/\+00(:00)?$/, 'Z')
    : v;

/// `pg_lsn` text (`0/C5793FD0`) → the numeric WAL position CDC events carry.
export const lsnToNumber = (lsn: string): number => {
  const [hi, lo] = String(lsn).split('/');
  return parseInt(hi, 16) * 0x100000000 + parseInt(lo, 16);
};
