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

export type ManifestDelta = { object: string; cutoff: string; prev_cutoff: string; gen: number; sorted?: boolean };
export type ManifestCheckpoint = { object: string; gen: number; lower?: string; cutoff: string; sorted?: boolean };
export type ChainManifest = {
  gen: number;
  /// The whole table. `base` is the incremental chain's name for it; `full` is the same
  /// thing under the older name, and a manifest may carry either.
  full?: { object: string; gen: number; cutoff?: string; sorted?: boolean } | null;
  base?: { object: string; gen: number; cutoff?: string; sorted?: boolean } | null;
  /// Rows whose version moved inside each checkpoint's window, tombstones included.
  checkpoints?: ManifestCheckpoint[];
  deltas?: ManifestDelta[];
  cutoff_seq?: number;
  cdc_stream?: string;
  /// The `created` of the stream incarnation `cutoff_seq` was read on: the gate
  /// anchors only while the client reads that incarnation (a recreated stream
  /// restarts its numbering).
  cdc_stream_created?: string;
  /// `zebridge_gc_watermark` as it stood at the cut: nothing soft-deleted before it is
  /// guaranteed to still exist. A replica older than this cannot catch up incrementally,
  /// because a row may have been deleted AND reaped while it was away and no artifact
  /// carries an absence — only the base does (PROTOCOL §7.5).
  gc_watermark?: string;
};
/// A checkpoint applies exactly as a delta does (version-guarded upserts, tombstones
/// delete by key); only the base wipes and reloads, which is why it keeps the name the
/// clients already act on.
/// `sorted`: the object's rows are in primary-key order (§10ja) — applied without
/// staging or sorting. Present only when true.
export type PlanStep = { name: string; kind: 'full' | 'delta' | 'checkpoint'; sorted?: boolean };

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
/// PROTOCOL §7.5: a soft delete reaches a client as an update that SETS the tombstone
/// column, and the physical reap that follows is never forwarded — so a row whose
/// tombstone is present and not null is removed by the client, on seed, on CDC and on
/// its own optimistic apply alike. A table without a tombstone column is never
/// tombstoned: its deletes are physical DELETE events. Pinned in fixtures/tombstoned.
export function tombstoned(tombstoneColumn: string | null, data: unknown): boolean {
  if (!tombstoneColumn || data === null || typeof data !== 'object') return false;
  const v = (data as Record<string, unknown>)[tombstoneColumn];
  return v !== undefined && v !== null;
}

export function seedGateDrops(ev: CoreEvent, anchor: SeedAnchor): boolean {
  // §10ja: an event that carries its stream sequence is gated by the sequence anchor
  // ALONE. Without one on its stream — a chain cut on an empty, brand-new stream ships
  // no cutoff_seq — it applies: a transaction in flight during the cut has lsns below
  // the cutoff and commits after the snapshot (finding 7), so the lsn gate dropped the
  // first 2,342 rows of a 10,000-row insert under the firehose. Applying twice is what
  // the upsert absorbs; dropping is a hole. libzb's shell already did this.
  if (typeof ev.seq === 'number' && ev.stream) {
    return typeof anchor.seedSeq === 'number' && anchor.seedStream === ev.stream && ev.seq <= anchor.seedSeq;
  }
  return typeof anchor.seedLsn === 'number' && typeof ev.lsn === 'number' && ev.lsn < anchor.seedLsn;
}

// ─── chain planning (§10n) ───────────────────────────────────────────────────

/// The walk a client applies from a chain manifest, given its stored watermark
/// (the last applied cutoff_version, or null on a fresh table): deltas-only
/// when they reach the watermark, otherwise the full plus every delta after it.
export function planFromManifest(man: ChainManifest, watermark: string | null): PlanStep[] {
  const deltas: ManifestDelta[] = man.deltas ?? [];
  const checkpoints: ManifestCheckpoint[] = man.checkpoints ?? [];
  const base = man.base ?? man.full ?? null;
  const step = (d: ManifestDelta): PlanStep => ({ name: d.object, kind: 'delta', ...(d.sorted ? { sorted: true } : {}) });
  const ckptStep = (c: ManifestCheckpoint): PlanStep => ({ name: c.object, kind: 'checkpoint', ...(c.sorted ? { sorted: true } : {}) });
  const applicable = watermark ? deltas.filter((d) => d.cutoff > watermark) : deltas;

  // The base, then everything the base does not already carry. One path, three callers.
  const fromBase = (): PlanStep[] => {
    if (!base) return [];
    return [
      { name: base.object, kind: 'full' as const, ...(base.sorted ? { sorted: true } : {}) },
      ...checkpoints.filter((c) => c.gen > base.gen).map(ckptStep),
      ...deltas.filter((d) => d.gen > base.gen).map(step),
    ];
  };

  // ⚠️ FIRST, before any arithmetic on cutoffs: a replica older than the gc watermark
  // cannot be caught up incrementally at all. A row deleted while it was away may have
  // been REAPED since — the tombstone that would have carried the delete is gone from
  // every checkpoint and delta, and only a wipe-and-reload can remove the row from this
  // replica (the sweeper cannot reap a tombstone newer than the watermark, which is what
  // makes the other branches sound).
  if (watermark != null && man.gc_watermark != null && watermark < man.gc_watermark) return fromBase();

  // "Reaches" = the chain continues from where this replica stands: either the first
  // applicable delta starts at or before the watermark, or there is nothing newer AND
  // the chain's base itself is not newer than the watermark. The second half is the
  // part that was missing: a chain REBUILT after the watermark (a fresh g1 full, no
  // deltas — what a feed restart produces, NOTES §10bm) is not "already applied", it
  // is unreachable, and the walk must start from its base.
  const reaches = watermark != null && (
    applicable.length > 0
      ? applicable[0].prev_cutoff <= watermark
      : (base == null || base.cutoff == null || base.cutoff <= watermark));  // a legacy full without a cutoff is taken as reached, as the Zig core does
  if (reaches) return applicable.map(step);

  // The deltas do not reach it, but the checkpoints may: apply every checkpoint whose
  // window ends after the watermark, then the deltas. Overlap between the two is
  // harmless — rows are version-guarded upserts and tombstones delete by key — so the
  // cheap rule is "everything newer than the watermark", not "exactly the gap".
  if (watermark != null && checkpoints.length > 0) {
    const oldest = checkpoints[0];
    const covered = (oldest.lower ?? '') <= watermark;
    if (covered) {
      return [
        ...checkpoints.filter((c) => c.cutoff > watermark).map(ckptStep),
        ...applicable.map(step),
      ];
    }
  }
  return fromBase();
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

export type StreamGap = { firstSeq: number; stored: number; lastSeq?: number };

/// Per-stream, never per-table (the abandoned-table paradox). `stored === 0` is
/// the fresh-client case; `< firstSeq - 1` means the stream pruned past the
/// stored position. `stored === firstSeq - 1` is NOT a gap: the very next
/// message needed is the oldest one still held.
/// Three shapes of "the stream no longer continues from where I stopped":
///   never here (`stored === 0`); the tail I need was retained away
///   (`stored < firstSeq - 1`); and the stream RESTARTED under me — my position is
///   beyond its last sequence (`stored > lastSeq`). The third is what a lost
///   replication slot looks like from a client: WAL the bridge never saw leaves no
///   hole in the stream's numbering, so the bridge recreates the CDC streams on a new
///   slot (NOTES §10bm) and this is the only trace a client can read.
export function streamHasGap(g: StreamGap): boolean {
  if (g.stored === 0) return true;
  if (g.firstSeq > 0 && g.stored < g.firstSeq - 1) return true;
  if (typeof g.lastSeq === 'number' && g.lastSeq >= 0 && g.stored > g.lastSeq) return true;
  return false;
}

/// Seeding is SCOPED: a gap on one stream re-seeds only the tables ROUTED to
/// that stream, plus tables never seeded at all (no generations watermark —
/// a brand-new replica, or a table enabled between two connects). Everything
/// else resumes untouched — a mobile client reconnecting with one stale
/// stream must not rebuild its whole replica.
export function scopeSeeding(
  streams: Record<string, StreamGap>,
  tables: Record<string, { route: string; sharedRoute?: string; seeded: boolean }>,
): { gapped: string[]; tablesToSeed: string[] } {
  const gapped = Object.entries(streams)
    .filter(([, g]) => streamHasGap(g))
    .map(([name]) => name);
  const gappedSet = new Set(gapped);
  // ⚠️ TWO routes, for a tenant-scoped table. Its own rows ride `CDC_<tenant>`, but its
  // OPEN-TENANT rows — the shared ones every tenant may read (`zb_reader_all` admits
  // `tenant_col = <open tenant>`, and the producer's chain carries them because it reads
  // under that policy) — ride `CDC_PUBLIC`. Scoping such a table to its tenant stream
  // alone meant a gap on CDC_PUBLIC re-seeded the public TABLES and left every
  // tenant-scoped table's shared rows silently stale (NOTES §10bq).
  const tablesToSeed = Object.entries(tables)
    .filter(([, t]) => gappedSet.has(t.route) ||
                       (t.sharedRoute != null && gappedSet.has(t.sharedRoute)) ||
                       !t.seeded)
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

/// §10ja: how far a CAUGHT-UP consumer has read. A consumer filtered to this client's
/// tables never sees the other tables' messages, so its position stays at the last
/// message it was handed (0 on a stream none of its tables writes to) and the next
/// launch reads "position 0, stream first 9632" as a gap. With nothing pending it has
/// seen every message for its subjects up to the stream's end: that is the position.
/// ⚠️ `lastSeq` must be read BEFORE the consumer's info — then `numPending = 0` covers
/// it — and nothing handed over past `pos`, nothing unacked, says all it was handed is
/// applied. `deliveredCount` is the consumer's own sequence: a consumer that delivered
/// nothing still reports `delivered` = its start - 1 (measured: 9631 on a fresh one,
/// position 0), which is no message in flight. Any doubt keeps `pos`; never backwards.
/// §10jh: never over a PRUNED range — `firstSeq` past `pos + 1` means the stream dropped
/// messages after the position before this client saw them, and a filtered consumer then
/// looks caught up. Stay; the gap rule heals it from the chain. `firstSeq` absent or 0:
/// unknown, the old rule (libzb core.caughtUpPosition, the same fixtures).
export function caughtUpPosition(pos: number, lastSeq: number, c: { numPending: number; numAckPending: number; deliveredCount: number; delivered: number; firstSeq?: number }): number {
  if (lastSeq <= pos) return pos;
  if ((c.firstSeq ?? 0) > pos + 1) return pos;
  if (c.numPending !== 0 || c.numAckPending !== 0 || (c.deliveredCount > 0 && c.delivered > pos)) return pos;
  return lastSeq;
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

// ─── the apply SQL builders (§10s increment 2a) ──────────────────────────────
//
// The exact statements a replica executes for one CDC event and for one chain
// row. Pure string/param construction — the shell owns exec, transactions,
// logging and error classification. A port producing byte-identical SQL and
// params is applying events exactly like this client.

export type SqlStep = { sql: string; params: any[] };
export type KeyChangeStep = SqlStep & { oldKey: any[]; newKey: any[] };

/// §10ex: a bytea (or EWKB) cell arrives as msgpack `bin`, decoded as a Uint8Array.
/// It is an object to `typeof`, and must never be stringified: it binds as a BLOB.
export const isBytes = (v: any): v is Uint8Array => v instanceof Uint8Array;

/// One CDC value → one bound parameter: structured values travel as JSON text
/// (SQLite has no object affinity); bytes bind as bytes; everything else binds
/// as-is (the CDC wire already normalizes timestamps).
export const cdcValue = (v: any): any =>
  isBytes(v) ? v : v !== null && typeof v === 'object' ? JSON.stringify(v) : v;

/// A JS array as PostgreSQL's array-literal TEXT: `{a,b}`, nested `{{1,2},{3}}`,
/// elements quoted when they need it (comma, brace, quote, backslash, whitespace,
/// empty, or the word NULL), `null` elements as NULL. The form PostgreSQL's array
/// input function reads — and the form the CDC wire already carries for arrays, so a
/// replica on a PostgreSQL engine sees its own optimistic write exactly as it will
/// see the echo. Measured without it: `malformed array literal: ["web","push-…"]` —
/// `cdcValue`'s JSON is right for a jsonb column and wrong for `text[]`.
/// Pinned in fixtures/pgArrayLiteral.
export function pgArrayLiteral(v: any[]): string {
  const elem = (x: any): string => {
    if (x === null || x === undefined) return 'NULL';
    if (Array.isArray(x)) return pgArrayLiteral(x);
    if (typeof x === 'object') return quote(JSON.stringify(x));
    if (typeof x === 'boolean') return x ? 't' : 'f';
    if (typeof x === 'number') return String(x);
    return quote(String(x));
  };
  const quote = (s: string): string =>
    s === '' || /[\s,{}"\\]/.test(s) || s.toUpperCase() === 'NULL'
      ? `"${s.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`
      : s;
  return `{${v.map(elem).join(',')}}`;
}

/// §10fb: a chain step's rows in primary-key order before they are applied. A chain
/// comes in the table's physical order — random uuids — and each insert then lands
/// on a random b-tree page; sorted, the inserts append (libzb §10ez: the apply
/// halved). Strings compare by code unit, numbers numerically; anything else keeps
/// its place (the sort is stable). A copy of the row references, not of the rows.
/// §10ix: the head of a chain document, read by hand. The producer's layout
/// (generation_producer.zig `docHead`) is a map whose first two entries are
/// `columns` (an array of names) and `rows` (an array whose COUNT is written before
/// its elements) — so a client can learn the columns and the row count from the
/// first few hundred bytes and decode every value after `offset` as one row, without
/// holding the document. Null when `b` does not yet reach the end of the head (buffer
/// more) or is not a chain document at all (the caller falls back). Accepts every
/// msgpack spelling of a map, string and array header the producer could emit.
export function parseChainHead(b: Uint8Array): { columns: string[]; nrows: number; offset: number } | null {
  let p = 0;
  const need = (n: number) => p + n <= b.length;
  const dv = () => new DataView(b.buffer, b.byteOffset, b.byteLength);
  const str = (): string | null => {
    if (!need(1)) return null;
    const t = b[p]; let len: number, hl: number;
    if (t >= 0xa0 && t <= 0xbf) { len = t & 0x1f; hl = 1; }
    else if (t === 0xd9) { if (!need(2)) return null; len = b[p + 1]; hl = 2; }
    else if (t === 0xda) { if (!need(3)) return null; len = dv().getUint16(p + 1); hl = 3; }
    else return null;
    if (!need(hl + len)) return null;
    const s = new TextDecoder().decode(b.subarray(p + hl, p + hl + len)); p += hl + len; return s;
  };
  const arr = (): number | null => {
    if (!need(1)) return null; const t = b[p];
    if (t >= 0x90 && t <= 0x9f) { p += 1; return t & 0x0f; }
    if (t === 0xdc) { if (!need(3)) return null; const n = dv().getUint16(p + 1); p += 3; return n; }
    if (t === 0xdd) { if (!need(5)) return null; const n = dv().getUint32(p + 1); p += 5; return n; }
    return null;
  };
  if (!need(1)) return null;
  const t = b[p];
  if (t >= 0x80 && t <= 0x8f) p += 1;
  else if (t === 0xde) { if (!need(3)) return null; p += 3; }
  else return null;
  if (str() !== 'columns') return null;
  const ncols = arr(); if (ncols === null) return null;
  const columns: string[] = [];
  for (let i = 0; i < ncols; i++) { const s = str(); if (s === null) return null; columns.push(s); }
  if (str() !== 'rows') return null;
  const nrows = arr(); if (nrows === null) return null;
  return { columns, nrows, offset: p };
}

/// §10ja: how many COMPLETE msgpack values `b` holds from its start (at most `max`),
/// and where the last one ends — without decoding them. The streamed seed decodes a
/// chunk of rows at once with it: one `decode` per chunk instead of one `await` per row
/// through an async generator, which was most of the seed's JS time on Hermes (~130 µs
/// a row on the iPhone 12). A value cut by the end of `b` is not counted: the caller
/// keeps the bytes from `end` and appends the next chunk.
export function msgpackScan(b: Uint8Array, max: number): { end: number; count: number } {
  const n = b.length;
  const dv = new DataView(b.buffer, b.byteOffset, b.byteLength);
  let end = 0;
  let count = 0;
  while (count < max) {
    let p = end;
    let need = 1; // values still to skip at any depth: an array adds its length, a map twice
    while (need > 0) {
      if (p >= n) return { end, count };
      const t = b[p];
      need--;
      let size: number; // header + payload, once the header is known to be in `b`
      if (t <= 0x7f || t >= 0xe0) size = 1; // fixint
      else if (t <= 0x8f) { need += 2 * (t & 0x0f); size = 1; } // fixmap
      else if (t <= 0x9f) { need += t & 0x0f; size = 1; } // fixarray
      else if (t <= 0xbf) size = 1 + (t & 0x1f); // fixstr
      else {
        switch (t) {
          case 0xc0: case 0xc2: case 0xc3: size = 1; break; // nil, false, true
          case 0xcc: case 0xd0: size = 2; break;
          case 0xcd: case 0xd1: size = 3; break;
          case 0xce: case 0xd2: case 0xca: size = 5; break;
          case 0xcf: case 0xd3: case 0xcb: size = 9; break;
          case 0xd4: size = 3; break; case 0xd5: size = 4; break; case 0xd6: size = 6; break; // fixext 1/2/4
          case 0xd7: size = 10; break; case 0xd8: size = 18; break; // fixext 8/16
          case 0xc4: case 0xd9: if (p + 2 > n) return { end, count }; size = 2 + b[p + 1]; break; // bin8, str8
          case 0xc5: case 0xda: if (p + 3 > n) return { end, count }; size = 3 + dv.getUint16(p + 1); break;
          case 0xc6: case 0xdb: if (p + 5 > n) return { end, count }; size = 5 + dv.getUint32(p + 1); break;
          case 0xc7: if (p + 2 > n) return { end, count }; size = 3 + b[p + 1]; break; // ext8
          case 0xc8: if (p + 3 > n) return { end, count }; size = 4 + dv.getUint16(p + 1); break;
          case 0xc9: if (p + 5 > n) return { end, count }; size = 6 + dv.getUint32(p + 1); break;
          case 0xdc: if (p + 3 > n) return { end, count }; need += dv.getUint16(p + 1); size = 3; break; // array16
          case 0xdd: if (p + 5 > n) return { end, count }; need += dv.getUint32(p + 1); size = 5; break;
          case 0xde: if (p + 3 > n) return { end, count }; need += 2 * dv.getUint16(p + 1); size = 3; break; // map16
          case 0xdf: if (p + 5 > n) return { end, count }; need += 2 * dv.getUint32(p + 1); size = 5; break;
          default: throw new Error(`msgpack: byte 0x${t.toString(16)} is not a type`); // 0xc1
        }
      }
      p += size;
    }
    if (p > n) return { end, count }; // the last payload runs past `b`
    end = p;
    count++;
  }
  return { end, count };
}

/// The `count` values `msgpackScan` found in `b[0, end)`, decoded in ONE call: prefixed
/// with an array32 header they are a single array. The copy is a memcpy; a generator
/// per value is what this replaces.
export function msgpackDecodeScanned(decode: (b: Uint8Array) => unknown, b: Uint8Array, end: number, count: number): any[] {
  const h = new Uint8Array(5 + end);
  h[0] = 0xdd;
  new DataView(h.buffer).setUint32(1, count);
  h.set(b.subarray(0, end), 5);
  return decode(h) as any[];
}

export function sortRowsByKey(rows: any[][], keyIdx: number): any[][] {
  if (keyIdx < 0 || rows.length < 2) return rows;
  const out = rows.slice();
  out.sort((x, y) => {
    const a = x[keyIdx], b = y[keyIdx];
    if (typeof a === 'string' && typeof b === 'string') return a < b ? -1 : a > b ? 1 : 0;
    if (typeof a === 'number' && typeof b === 'number') return a - b;
    return 0;
  });
  return out;
}

/// §10ey: the wire carries an array as JSON text (`["a","b"]`). A PostgreSQL engine
/// binds an array column from PostgreSQL's literal, so on apply the JSON text of each
/// array column becomes that literal — `pgArrayLiteral` of the parsed list. A value
/// that is not JSON-array text (already a literal, NULL, a scalar) is left alone.
export function pgArrayValues(data: Record<string, any>, arrayCols: readonly string[]): Record<string, any> {
  if (!arrayCols.length) return data;
  const out: Record<string, any> = { ...data };
  for (const k of arrayCols) {
    const v = out[k];
    if (typeof v === 'string' && v.startsWith('[')) {
      try { out[k] = pgArrayLiteral(JSON.parse(v)); } catch { /* not JSON: leave it */ }
    } else if (Array.isArray(v)) out[k] = pgArrayLiteral(v);
  }
  return out;
}

/// §10fg: the pgvector wire shapes — the bridge's normalised BLOBs: `vector` as
/// little-endian float32s, `halfvec` as little-endian float16s, `sparsevec` as u32 dim,
/// u32 nnz, nnz u32 indices (0-based), nnz float32s, `bit` as packed bits MSB first.
/// A SQLite replica stores them as they are (sqlite-vec reads them); a PostgreSQL
/// engine binds pgvector's text form, which `vecLiteral` renders. `bits` is a bit(n)
/// column's declared length (0 when unknown): the wire pads to a byte, bit(3) refuses eight.
export type VecKind = 'vector' | 'halfvec' | 'sparsevec' | 'bit';
export type VecCol = { name: string; kind: VecKind; bits: number };

/// The pgvector/bit columns of a descriptor's `pg` block, with a bit(n)'s length.
export function vecColsOf(cols: readonly { name: string; type?: string }[]): VecCol[] {
  const out: VecCol[] = [];
  for (const c of cols) {
    if (typeof c.type !== 'string') continue;
    const bare = c.type.split('(')[0];
    if (bare !== 'vector' && bare !== 'halfvec' && bare !== 'sparsevec' && bare !== 'bit') continue;
    const m = bare === 'bit' ? /\((\d+)\)/.exec(c.type) : null;
    out.push({ name: c.name, kind: bare, bits: m ? Number(m[1]) : 0 });
  }
  return out;
}

const halfToNumber = (h: number): number => {
  const s = h >> 15 ? -1 : 1, e = (h >> 10) & 0x1f, f = h & 0x3ff;
  if (e === 0) return s * f * 2 ** -24;
  if (e === 31) return f ? NaN : s * Infinity;
  return s * (1 + f / 1024) * 2 ** (e - 15);
};

export function vecLiteral(kind: VecKind, bytes: Uint8Array, bits = 0): string {
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (kind === 'vector' || kind === 'halfvec') {
    const size = kind === 'vector' ? 4 : 2;
    if (bytes.byteLength % size) throw new Error(`${kind}: ${bytes.byteLength} bytes is not a whole number of elements`);
    const parts: string[] = [];
    for (let i = 0; i < bytes.byteLength; i += size) parts.push(String(size === 4 ? dv.getFloat32(i, true) : halfToNumber(dv.getUint16(i, true))));
    return `[${parts.join(',')}]`;
  }
  if (kind === 'sparsevec') {
    if (bytes.byteLength < 8) throw new Error('sparsevec: no header');
    const dim = dv.getUint32(0, true), nnz = dv.getUint32(4, true);
    if (bytes.byteLength !== 8 + nnz * 8) throw new Error(`sparsevec: ${bytes.byteLength} bytes for nnz ${nnz}`);
    const parts: string[] = [];
    for (let k = 0; k < nnz; k++) parts.push(`${dv.getUint32(8 + k * 4, true) + 1}:${dv.getFloat32(8 + nnz * 4 + k * 4, true)}`);
    return `{${parts.join(',')}}/${dim}`;
  }
  const all = bytes.byteLength * 8;
  const n = bits > 0 && bits < all ? bits : all;
  let s = '';
  for (let i = 0; i < n; i++) s += (bytes[i >> 3] >> (7 - (i & 7))) & 1 ? '1' : '0';
  return s;
}

/// On a PostgreSQL engine, the bytes of each pgvector/bit column become the text form.
export function pgVectorValues(data: Record<string, any>, vecCols: readonly VecCol[]): Record<string, any> {
  if (!vecCols.length) return data;
  const out: Record<string, any> = { ...data };
  for (const vc of vecCols) {
    const v = out[vc.name];
    if (isBytes(v)) out[vc.name] = vecLiteral(vc.kind, v, vc.bits);
  }
  return out;
}

/// The payload of a LOCAL write as a PostgreSQL engine must bind it: arrays as
/// array literals, plain objects as JSON (jsonb reads that), scalars unchanged. CDC
/// events need none of this — the wire is already in these forms.
export function pgEngineValues(data: Record<string, any>): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(data)) {
    out[k] = isBytes(v) ? v : Array.isArray(v) ? pgArrayLiteral(v) : v !== null && typeof v === 'object' ? JSON.stringify(v) : v;
  }
  return out;
}

/// A changed primary key arrives as an UPDATE with `old.*` — the old key must
/// be deleted first, or the row lives on under both keys forever (measured
/// live). Null when the event carries no complete, actually-different old key.
export function planKeyChange(
  table: string,
  pkCols: string[],
  data: Record<string, any>,
): KeyChangeStep | null {
  if (!pkCols.length) return null;
  const oldKey = pkCols.map((c) => data[`old.${c}`]);
  const newKey = pkCols.map((c) => data[c]);
  const changed =
    oldKey.every((v) => v !== undefined && v !== null) &&
    oldKey.some((v, i) => v !== newKey[i]);
  if (!changed) return null;
  const where = pkCols.map((c) => `"${c}" = ?`).join(' AND ');
  return { sql: `DELETE FROM ${table} WHERE ${where}`, params: oldKey, oldKey, newKey };
}

/// The CDC upsert: INSERT .. ON CONFLICT(pk) DO UPDATE over the non-key
/// columns. A table whose every column is in the key gets DO NOTHING — a
/// redelivered insert must converge, not throw (the §7.1 idempotency promise;
/// the chain path always had this and the CDC path now matches it). A keyless
/// table gets a plain INSERT (it is refused upstream anyway, §9).
export function planUpsert(
  table: string,
  pkCols: string[],
  data: Record<string, any>,
): SqlStep {
  const dataKeys = Object.keys(data).filter((k) => !k.startsWith('old.'));
  const params = dataKeys.map((k) => cdcValue(data[k]));
  const columns = dataKeys.map((k) => `"${k}"`).join(', ');
  const placeholders = dataKeys.map(() => '?').join(', ');
  const updates = dataKeys
    .filter((k) => !pkCols.includes(k))
    .map((k) => `"${k}" = excluded."${k}"`)
    .join(', ');
  let sql = `INSERT INTO ${table} (${columns}) VALUES (${placeholders})`;
  if (pkCols.length) {
    const conflict = pkCols.map((c) => `"${c}"`).join(', ');
    sql += updates
      ? ` ON CONFLICT(${conflict}) DO UPDATE SET ${updates}`
      : ` ON CONFLICT(${conflict}) DO NOTHING`;
  }
  return { sql, params };
}

/// The CDC delete. Null when any key column is absent: a partial composite key
/// would match MORE rows than PostgreSQL deleted — skipping is the only safe
/// answer (the row converges on the next seed).
/// The UPDATE-shaped plan for a row that EXISTS locally: only the columns the
/// payload carries are touched, so a partial payload is fine. Null when there is no
/// key, the key is incomplete, or nothing but the key was sent (nothing to set).
///
/// ⚠️ Why this exists next to planUpsert. The upsert is one statement for both
/// arms, and SQLite evaluates the INSERT arm first: a partial UPDATE payload on an
/// existing row failed the INSERT's NOT NULL check before the conflict was ever
/// resolved (measured: `NOT NULL constraint failed: test_types.tenant_id` on every
/// browser `UP`, corrected only by the CDC echo; libzb's soak hit the same). So the
/// shell asks `planExists` first and applies THIS when the row is there; the upsert
/// remains the plan for a row that is not. Pinned in fixtures/update.
export function planUpdate(
  table: string,
  pkCols: string[],
  data: Record<string, any>,
): SqlStep | null {
  if (!pkCols.length) return null;
  const key = pkCols.map((c) => data[c]);
  if (!key.every((v) => v !== undefined && v !== null)) return null;
  const setKeys = Object.keys(data).filter((k) => !k.startsWith('old.') && !pkCols.includes(k));
  if (!setKeys.length) return null;
  const sets = setKeys.map((k) => `"${k}" = ?`).join(', ');
  const where = pkCols.map((c) => `"${c}" = ?`).join(' AND ');
  return { sql: `UPDATE ${table} SET ${sets} WHERE ${where}`, params: [...setKeys.map((k) => cdcValue(data[k])), ...key] };
}

/// "Is this row here?" — `SELECT 1 … LIMIT 1` by the full key, or null when the key
/// is incomplete (then nothing can be looked up and the caller falls back to upsert).
export function planExists(
  table: string,
  pkCols: string[],
  data: Record<string, any>,
): SqlStep | null {
  if (!pkCols.length) return null;
  const key = pkCols.map((c) => data[c]);
  if (!key.every((v) => v !== undefined && v !== null)) return null;
  const where = pkCols.map((c) => `"${c}" = ?`).join(' AND ');
  return { sql: `SELECT 1 FROM ${table} WHERE ${where} LIMIT 1`, params: key };
}

export function planDelete(
  table: string,
  pkCols: string[],
  data: Record<string, any>,
): SqlStep | null {
  if (!pkCols.length) return null;
  const params = pkCols.map((c) => data[c]);
  if (!params.every((v) => v !== undefined && v !== null)) return null;
  const where = pkCols.map((c) => `"${c}" = ?`).join(' AND ');
  return { sql: `DELETE FROM ${table} WHERE ${where}`, params };
}

/// The chain-row upsert: like planUpsert but column-list driven (chain objects
/// carry rows as arrays), and version-GUARDED when the table has a version
/// column the object carries — a chain row must never overwrite a NEWER value
/// CDC already applied (LWW holds during seeding too).
export function chainUpsertSql(
  table: string,
  cols: string[],
  pkCols: string[],
  versionCol: string | null,
): string {
  const colList = cols.map((c) => `"${c}"`).join(', ');
  const ph = cols.map(() => '?').join(', ');
  const conflict = pkCols.map((c) => `"${c}"`).join(', ');
  const sets = cols.filter((c) => !pkCols.includes(c))
                   .map((c) => `"${c}" = excluded."${c}"`).join(', ');
  let sql = `INSERT INTO ${table} (${colList}) VALUES (${ph})`;
  sql += sets
    ? ` ON CONFLICT(${conflict}) DO UPDATE SET ${sets}` +
      (versionCol ? ` WHERE excluded."${versionCol}" > ${table}."${versionCol}"` : '')
    : ` ON CONFLICT(${conflict}) DO NOTHING`;
  return sql;
}

/// §10fc: a whole chunk of chain rows in ONE statement — the rows as a JSON array
/// text bound once, exploded by SQLite's own `json_each`, each cell picked with
/// `json_extract` (a JSON string is TEXT, a number INTEGER or REAL, true/false 1/0,
/// null NULL, a nested value its JSON text — the shapes `chainRowParams` binds).
/// The conflict clause is the upsert's, so the version guard holds row by row. The
/// `WHERE true` is SQLite's disambiguation of INSERT … SELECT … ON CONFLICT.
/// Not for a table with a BLOB column (JSON has no bytes) nor for PostgreSQL.
/// §10ix: the staging insert of a streamed FULL on SQLite — the same `json_extract`
/// picks as `chainBulkSql`, into a keyless TEMP table, with no conflict clause: a heap
/// append, no sort, no b-tree. The real table is filled once at the end, `SELECT …
/// ORDER BY <pk>`, so SQLite's external sorter puts the rows in key order and the
/// b-tree is built sequentially — the property the buffered path had from sorting the
/// whole document in memory, without holding it.
export function chainStageSql(stage: string, cols: string[]): string {
  const colList = cols.map((c) => `"${c}"`).join(', ');
  const picks = cols.map((_, i) => `json_extract(value, '$[${i}]')`).join(', ');
  return `INSERT INTO ${stage} (${colList}) SELECT ${picks} FROM json_each(?)`;
}

export function chainBulkSql(
  table: string,
  cols: string[],
  pkCols: string[],
  versionCol: string | null,
): string {
  const colList = cols.map((c) => `"${c}"`).join(', ');
  const picks = cols.map((_, i) => `json_extract(value, '$[${i}]')`).join(', ');
  const conflict = pkCols.map((c) => `"${c}"`).join(', ');
  const sets = cols.filter((c) => !pkCols.includes(c))
                   .map((c) => `"${c}" = excluded."${c}"`).join(', ');
  let sql = `INSERT INTO ${table} (${colList}) SELECT ${picks} FROM json_each(?) WHERE true`;
  sql += sets
    ? ` ON CONFLICT(${conflict}) DO UPDATE SET ${sets}` +
      (versionCol ? ` WHERE excluded."${versionCol}" > ${table}."${versionCol}"` : '')
    : ` ON CONFLICT(${conflict}) DO NOTHING`;
  return sql;
}

/// One chain row → bound parameters: structured values as JSON, text-mode
/// timestamps normalized to the CDC wire shape so the version guard compares
/// like against like (NOTES §1.13).
export const chainRowParams = (row: any[]): any[] =>
  row.map((v) => (isBytes(v) ? v : v !== null && typeof v === 'object' ? JSON.stringify(v) : pgTsToWire(v)));

// ─── the CDC bulk planner (§10gp, the plan's "bulk CDC apply") ──────────────
//
// A batch of CDC events is applied one statement per event; a chain chunk is one
// statement per chunk (`chainBulkSql`), and runs ~3–5× faster. This planner says
// which events of a batch may share ONE upsert and which must take the per-event
// path — the six decisions `applyEvent` makes before it writes, as a pure rule the
// two shells implement identically. Pinned in fixtures/cdcBulk.

export type CdcBulkTable = {
  /// The replica table's OWN columns — what the shell recorded after the migration,
  /// never the descriptor's list on its own (§10gp: the reverted probe experiment
  /// judged "whole payload" against the descriptor and lost 28 of 60 batches).
  columns: string[];
  pkCols: string[];
  tombstoneColumn?: string | null;
  /// Waiting for its chain: every event holds (§10et), none is bulked.
  unseeded?: boolean;
  /// The seed gate's anchor; absent → nothing is gated.
  anchor?: SeedAnchor;
  /// JSON has no bytes: a table with a BLOB column takes the per-event path, as the
  /// chain does.
  blobCols?: string[];
};
export type CdcBulkEvent = {
  table: string;
  operation: string;
  data?: Record<string, any> | null;
  seq?: number;
  stream?: string;
  lsn?: number;
  optimistic?: boolean;
};
export type CdcSegment =
  /// ONE statement for `rows` (in `cols` order, the shell binds `JSON.stringify(rows)`);
  /// `events` are the batch indexes it stands for, so the shell can still feed the HLC
  /// floor, confirm echoes and notify per event. On failure the shell applies those
  /// events one at a time — a bad row costs a retry, never a silent drop.
  | { kind: 'bulk'; table: string; cols: string[]; sql: string; rows: any[][]; events: number[] }
  /// The per-event path, with the decision that sent it there.
  | { kind: 'single'; event: number; why: string }
  /// Already contained in the seed (`seedGateDrops`), or nothing to apply.
  | { kind: 'drop'; event: number; why: string };

/// The CDC bulk upsert: `chainBulkSql` WITHOUT the version guard. Two updates of one
/// row inside a single PostgreSQL transaction carry the same version, and a guard
/// would drop the second; the stream's order is the truth, and SQLite applies the
/// SELECT's rows in order, so the last occurrence of a key wins — exactly sequential
/// application (measured on 3.53: a key written three times ends with the third row,
/// even when its stamp is OLDER).
export function cdcBulkSql(table: string, cols: string[], pkCols: string[]): string {
  return chainBulkSql(table, cols, pkCols, null);
}

const sameSet = (a: readonly string[], b: readonly string[]): boolean =>
  a.length === b.length && a.every((x) => b.includes(x));

/// Segments in execution order. A table's run of eligible events with one column set
/// is one segment; a per-event event of the SAME table closes its run first (the
/// order within a table is the stream's), events of other tables do not (the batch is
/// one transaction with FK checks deferred, so tables may interleave). A gated event
/// closes nothing: dropping it is a no-op wherever it falls.
///
/// Eligible: a followed, seeded table on SQLite or DuckDB (§10hg: DuckDB's shell
/// executes a segment through its appender and one set-based upsert) with no BLOB column; a CDC (not
/// optimistic) INSERT or UPDATE that is not a tombstone, not a key change, carries a
/// complete key and only known columns — and an UPDATE only when it carries EVERY
/// column of the table: a partial UPDATE on an existing row fails the upsert's INSERT
/// arm on NOT NULL before the conflict resolves (`planUpdate`'s reason to exist), so it
/// keeps the probe-then-UPDATE path. Everything else is `single`, named.
export function planCdcBulk(
  engine: string,
  tables: Record<string, CdcBulkTable>,
  events: readonly CdcBulkEvent[],
): CdcSegment[] {
  const out: CdcSegment[] = [];
  const open = new Map<string, { table: string; cols: string[]; sql: string; rows: any[][]; events: number[] }>();
  const close = (table: string) => {
    const g = open.get(table);
    if (!g) return;
    open.delete(table);
    out.push({ kind: 'bulk', ...g });
  };
  events.forEach((ev, i) => {
    const t = tables[ev.table];
    const single = (why: string) => { close(ev.table); out.push({ kind: 'single', event: i, why }); };
    if (!t) { out.push({ kind: 'single', event: i, why: 'not-followed' }); return; }
    const data = ev.data;
    if (!data || typeof data !== 'object') { out.push({ kind: 'drop', event: i, why: 'no-data' }); return; }
    if (ev.optimistic) return single('optimistic');
    if (t.unseeded) return single('unseeded');
    if (t.anchor && seedGateDrops(ev, t.anchor)) { out.push({ kind: 'drop', event: i, why: 'gate' }); return; }
    if (engine !== 'sqlite' && engine !== 'duckdb') return single('engine');
    // JSON has no bytes, so a BLOB column keeps SQLite's json_each path per row; DuckDB's
    // appender binds bytes, so there it is not a reason (§10hi).
    if (engine === 'sqlite' && t.blobCols?.length) return single('blob-table');
    if (ev.operation === 'DELETE') return single('delete');
    if (tombstoned(t.tombstoneColumn ?? null, data)) return single('tombstone');
    if (ev.operation !== 'INSERT' && ev.operation !== 'UPDATE') return single('operation');
    const keys = Object.keys(data).filter((k) => !k.startsWith('old.'));
    if (keys.some((k) => !t.columns.includes(k))) return single('unknown-column');
    if (planKeyChange(ev.table, t.pkCols, data)) return single('key-change');
    if (!t.pkCols.length) return single('keyless');
    if (t.pkCols.some((c) => data[c] === undefined || data[c] === null)) return single('incomplete-key');
    if (engine === 'sqlite' && keys.some((k) => isBytes(data[k]))) return single('bytes');
    if (ev.operation === 'UPDATE' && !sameSet(keys, t.columns)) return single('partial-update');
    let g = open.get(ev.table);
    if (g && !sameSet(g.cols, keys)) { close(ev.table); g = undefined; }
    if (!g) {
      g = { table: ev.table, cols: keys, sql: cdcBulkSql(ev.table, keys, t.pkCols), rows: [], events: [] };
      open.set(ev.table, g);
    }
    g.rows.push(g.cols.map((c) => data[c]));
    g.events.push(i);
  });
  for (const table of [...open.keys()]) close(table);
  return out;
}

// ─── the schema migration planner (§10s increment 2b — finding 9's home) ────
//
// Everything applySchema DECIDES, as pure functions over data the shell
// fetches: the incoming descriptor, the existing column list (from
// syncedTables or PRAGMA table_info — never from memory alone, finding 9),
// the stored CREATE TABLE text, and the existing index names. The shell
// executes, logs, and owns the runtime ALTER→rebuild fallback (that decision
// is error-driven, not plannable).

export type SchemaColumn = { name: string; type: string; required?: boolean; default?: string };
export type SchemaIndex = { name: string; unique?: boolean; columns: string[] };
export type SchemaForeignKey = { name?: string; columns: string[]; references: string; parent_columns: string[] };

/// One column's DDL. `required` is NOT NULL with no DEFAULT, published in both
/// dialects — honouring it makes a bad optimistic write fail locally instead of
/// round-tripping to a PostgreSQL refusal (NOTES §10c). Tolerant of absence: an
/// older bridge publishes no `required`, which reads as nullable.
export function columnDdl(c: SchemaColumn, pkCols: string[]): string {
  const inlinePk = pkCols.length === 1;
  return `"${c.name}" ${c.type}` +
    (pkCols.includes(c.name) || c.required ? ' NOT NULL' : '') +
    (c.default != null && c.default !== '' ? ` DEFAULT ${c.default}` : '') +
    (inlinePk && c.name === pkCols[0] ? ' PRIMARY KEY' : '');
}

/// The FOREIGN KEY table-constraint text. ⚠️ SQLite has no ALTER TABLE ADD
/// CONSTRAINT — an FK lives only inside CREATE TABLE, which is why an FK change
/// forces a rebuild while an index change is a cheap CREATE/DROP. Malformed
/// entries (missing parents, arity mismatch) are dropped, not guessed at.
/// `deferrable` (PostgreSQL engines): declare each FK `DEFERRABLE INITIALLY
/// IMMEDIATE`, so `SET CONSTRAINTS ALL DEFERRED` can hold the check to COMMIT the way
/// SQLite's `PRAGMA defer_foreign_keys` does. Off by default — the fixtures pin the
/// SQLite text, and SQLite would reject the clause.
/// `strict` (§10fi): SQLite's `STRICT` tables — a value that is not of the column's
/// declared type is refused at the bind instead of stored as whatever arrived. The
/// type table is exact now (every PostgreSQL type maps to one of INTEGER, REAL, TEXT,
/// BLOB), so a refusal is a bug surfacing, never a legitimate value. SQLite only;
/// PostgreSQL types its columns itself.
export type DdlOptions = { deferrable?: boolean; strict?: boolean };

export function fkClausesFor(foreignKeys: SchemaForeignKey[], opts: DdlOptions = {}): string {
  return foreignKeys
    .filter((f) => f?.references && Array.isArray(f.columns) && f.columns.length &&
                   Array.isArray(f.parent_columns) && f.parent_columns.length === f.columns.length)
    .map((f) =>
      `, FOREIGN KEY (${f.columns.map((c) => `"${c}"`).join(', ')})` +
      ` REFERENCES ${f.references} (${f.parent_columns.map((c) => `"${c}"`).join(', ')})` +
      (opts.deferrable ? ' DEFERRABLE INITIALLY IMMEDIATE' : ''))
    .join('');
}

function tableBody(cols: SchemaColumn[], pkCols: string[], foreignKeys: SchemaForeignKey[], opts: DdlOptions = {}): string {
  const constraint =
    (pkCols.length > 1 ? `, PRIMARY KEY (${pkCols.map((c) => `"${c}"`).join(', ')})` : '') +
    fkClausesFor(foreignKeys, opts);
  return `${cols.map((c) => columnDdl(c, pkCols)).join(', ')}${constraint}`;
}

/// First sight — which, after finding 9, means the table is PHYSICALLY absent.
export function createTableSteps(
  table: string, cols: SchemaColumn[], pkCols: string[], foreignKeys: SchemaForeignKey[], opts: DdlOptions = {},
): SqlStep[] {
  return [
    { sql: `DROP TABLE IF EXISTS ${table};`, params: [] },
    { sql: `CREATE TABLE ${table} (${tableBody(cols, pkCols, foreignKeys, opts)})${opts.strict ? ' STRICT' : ''};`, params: [] },
  ];
}

/// The rebuild sequence (tmp → copy common columns → swap). The shell wraps it
/// in PRAGMA foreign_keys OFF/ON: with FK enforcement on, the DROP of a
/// referenced parent is refused outright (measured — users, blocked by
/// salaries' FK). The data is copied, not changed.
export function rebuildSteps(
  table: string, cols: SchemaColumn[], pkCols: string[], foreignKeys: SchemaForeignKey[],
  existingColumns: string[], opts: DdlOptions = {},
): SqlStep[] {
  const tmp = `${table}__migrating`;
  const steps: SqlStep[] = [
    { sql: `DROP TABLE IF EXISTS ${tmp};`, params: [] },
    { sql: `CREATE TABLE ${tmp} (${tableBody(cols, pkCols, foreignKeys, opts)})${opts.strict ? ' STRICT' : ''};`, params: [] },
  ];
  const kept = cols.filter((c) => existingColumns.includes(c.name));
  const common = kept.map((c) => `"${c.name}"`);
  // A STRICT target refuses a value of another type where affinity used to convert
  // it (a re-typed column: TEXT '1.5' into REAL), so the copy casts to the new type.
  const select = opts.strict ? kept.map((c) => `CAST("${c.name}" AS ${c.type})`) : common;
  if (common.length) {
    steps.push({ sql: `INSERT INTO ${tmp} (${common.join(', ')}) SELECT ${select.join(', ')} FROM ${table};`, params: [] });
  }
  steps.push({ sql: `DROP TABLE IF EXISTS ${table};`, params: [] });
  steps.push({ sql: `ALTER TABLE ${tmp} RENAME TO ${table};`, params: [] });
  return steps;
}

/// The column diff, rename-aware. A rename hint counts only when its source
/// still exists and its target does not — anything else degrades to add+remove
/// (the §1.2 rename gap: without a hint the values are lost, by protocol).
/// Renames land BEFORE the add/remove diff — a renamed column is neither.
export function diffColumns(
  existingColumns: string[] | null,
  wantedNames: string[],
  renamed: Record<string, string>,
): { renames: [string, string][]; added: string[]; removed: string[] } {
  if (!existingColumns) return { renames: [], added: [], removed: [] };
  const renames: [string, string][] = Object.entries(renamed)
    .filter(([to, from]) => existingColumns.includes(from) && !existingColumns.includes(to))
    .map(([to, from]) => [from, to]);
  const effective = existingColumns.map((n) => renames.find(([from]) => from === n)?.[1] ?? n);
  return {
    renames,
    added: wantedNames.filter((n) => !effective.includes(n)),
    removed: effective.filter((n) => !wantedNames.includes(n)),
  };
}

// ─── query() is read-only (§10di) ──────────────────────────────────────────
//
// libzb answers `query()` on a second SQLite connection opened READONLY, so no
// write of any kind — data or bookkeeping (`_zebridge_outbox`, positions, the
// shape record) — can come through the API. The TypeScript client has that
// connection on Node (node.ts) and a single handle in the browser and on PGlite,
// where this rule stands in: the statement must READ. Comments and string
// literals are blanked first, so a value containing "delete" is not a write;
// a second statement after a `;`, a CTE feeding DML, or a pragma that SETS
// something is.

const WRITE_WORDS = /\b(insert|update|delete|replace|drop|alter|create|attach|detach|vacuum|reindex|truncate)\b/;

/// §10hn: which tables a client holds, and how — the ONE rule both clients follow.
///
///   tables      the tables to seed and tail: a list, or '*' for every published table
///   ondemand    the tables held for their schema only — no seed, no tail; rows arrive
///               only through `ingest` answering the client's own requests
///   keys        every published table (the schemas bucket's keys) — consulted for '*'
///
/// The result's `follow` is what gets seeded and tailed, `ondemand` what does not.
/// Absent both: nothing is held — a client declares what it wants, or says '*'. A
/// table in both lists is on-demand: on-demand wins. Names are deduplicated, and a
/// declared name the bucket does not know yet is kept (its schema may arrive later).
export function tableSet(
  tables: string[] | '*' | null | undefined,
  ondemand: string[] | null | undefined,
  keys: readonly string[],
): { follow: string[]; ondemand: string[] } {
  const od: string[] = [];
  for (const t of ondemand ?? []) if (t && !od.includes(t)) od.push(t);
  const odSet = new Set(od);
  const follow: string[] = [];
  const src: readonly string[] = tables === '*' ? keys : (tables ?? []);
  for (const t of src) if (t && !odSet.has(t) && !follow.includes(t)) follow.push(t);
  return { follow, ondemand: od };
}

/// §10ho: a map of LWW registers, merged. A register is `{v, t, w}`: a value, the
/// writer's stamp (RFC 3339 UTC, string-ordered), the writer. Per key the higher (t, w)
/// wins, so every key either side ever wrote survives; equal stamps break on the writer.
/// Commutative, associative, idempotent — the union only gains, which is why a writer
/// that ships all of its own registers on every write converges (crdt.py, §10cr). The
/// row underneath stays plain LWW: the row race decides who must merge, this decides
/// which value survives. Not a register (no `t`) is treated as the oldest.
export type Register = { v: unknown; t?: string; w?: string };
export function mergeRegisters(a: Record<string, Register>, b: Record<string, Register>): Record<string, Register> {
  const out: Record<string, Register> = { ...a };
  for (const [k, reg] of Object.entries(b ?? {})) {
    const cur = out[k];
    if (!cur || (reg.t ?? '') > (cur.t ?? '') || ((reg.t ?? '') === (cur.t ?? '') && (reg.w ?? '') > (cur.w ?? ''))) out[k] = reg;
  }
  return out;
}

export function isReadOnlySql(sql: string): boolean {
  let s = sql.replace(/\/\*[\s\S]*?\*\//g, ' ').replace(/--[^\n]*/g, ' ');
  s = s.replace(/'(?:[^']|'')*'/g, "''").replace(/"(?:[^"]|"")*"/g, '""').toLowerCase().trim();
  if (!s) return false;
  s = s.replace(/;\s*$/, '');
  if (s.includes(';')) return false;
  const first = s.match(/^[a-z_]+/)?.[0] ?? '';
  if (!['select', 'with', 'explain', 'values', 'pragma'].includes(first)) return false;
  if (WRITE_WORDS.test(s)) return false;
  if (first === 'pragma' && s.includes('=')) return false;
  return true;
}

// ─── the shape record (§10dg: re-key and re-type) ────────────────────────
//
// `diffColumns` sees NAMES only. A primary-key column whose type changed
// (bigserial → uuid), or a pk that gained a column, has the same names before and
// after — and a replica cannot ALTER its way there: SQLite's INTEGER PRIMARY KEY
// is a rowid alias that refuses a uuid, and no engine re-keys rows in place. So
// each replica records the shape it BUILT (its own descriptor's pk + dialect
// types, not a physical introspection whose type text differs per engine), and
// the next descriptor is compared with that record: key shape moved → the table
// is rebuilt EMPTY and re-seeded; a non-key type moved → rebuilt/ALTERed keeping
// the rows. Canonical JSON so the record compares byte-for-byte.

/// The pk columns, in pk order, with the dialect type each carries: `[["id","INTEGER"]]`.
/// A pk column the descriptor does not list (malformed) is skipped, not guessed.
export function keyShape(pkCols: string[], cols: SchemaColumn[]): string {
  const pairs: [string, string][] = [];
  for (const pk of pkCols) {
    const c = cols.find((x) => x.name === pk);
    if (c) pairs.push([c.name, c.type]);
  }
  return JSON.stringify(pairs);
}

/// Every column with its type, sorted by name (a DROP+ADD reorders attnums; that
/// is not a type change): `[["id","INTEGER"],["name","TEXT"]]`.
export function typeShape(cols: SchemaColumn[]): string {
  const pairs: [string, string][] = cols.map((c): [string, string] => [c.name, c.type]);
  pairs.sort((x, y) => (x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0));
  return JSON.stringify(pairs);
}

/// Columns present in BOTH the stored type shape and the descriptor whose type
/// text differs. Added/removed columns belong to `diffColumns`; an absent or
/// unreadable record reads as "nothing known" — never as "everything changed".
export function retypedColumns(storedTypeShape: string | null, cols: SchemaColumn[]): string[] {
  if (!storedTypeShape) return [];
  let stored: unknown;
  try { stored = JSON.parse(storedTypeShape); } catch { return []; }
  if (!Array.isArray(stored)) return [];
  const before = new Map<string, string>();
  for (const e of stored) {
    if (Array.isArray(e) && e.length === 2 && typeof e[0] === 'string' && typeof e[1] === 'string') before.set(e[0], e[1]);
  }
  return cols.filter((c) => before.has(c.name) && before.get(c.name) !== c.type).map((c) => c.name);
}

/// Does the stored CREATE TABLE text disagree with the FK clauses now wanted?
/// Text-compared because SQLite keeps no queryable "expected constraints", and
/// the stored DDL is our own generated text. Empty ddl → false (no table yet:
/// the create path owns it).
/// §10fi: a SQLite table created before STRICT existed is rebuilt once (rows kept).
/// Empty ddl → false: no table yet, the create path owns it.
export function strictMissing(ddl: string): boolean {
  return !!ddl && !/\)\s*STRICT\s*;?\s*$/i.test(ddl.trim());
}

export function fkTextDiffers(ddl: string, fkClauses: string): boolean {
  if (!ddl) return false;
  const hasAny = /FOREIGN KEY/i.test(ddl);
  if (!fkClauses) return hasAny;
  const want = fkClauses.replace(/^,\s*/, '').replace(/\s+/g, ' ').trim();
  return !ddl.replace(/\s+/g, ' ').includes(want);
}

/// The app-facing view: the replica's columns minus the plumbing ones. All
/// columns excluded → no view at all.
export const VIEW_EXCLUDED_COLUMNS = ['uid', 'inserted_at', 'updated_at', 'metadata'];
export function viewSteps(table: string, names: string[]): SqlStep[] {
  const viewCols = names.filter((n) => !VIEW_EXCLUDED_COLUMNS.includes(n)).map((n) => `"${n}"`).join(', ');
  const steps: SqlStep[] = [{ sql: `DROP VIEW IF EXISTS ${table}_view;`, params: [] }];
  if (viewCols) steps.push({ sql: `CREATE VIEW ${table}_view AS SELECT ${viewCols} FROM ${table};`, params: [] });
  return steps;
}

/// Bring the replica's secondary indexes in line with the published list:
/// create what is missing, drop what is no longer published (an index removed
/// upstream must not linger, costing writes for a query nobody makes).
/// `have` arrives pre-filtered of sqlite_% internals. Malformed entries skip.
export function indexSyncPlan(
  table: string, have: string[], want: SchemaIndex[],
): { drops: SqlStep[]; creates: SqlStep[] } {
  const wantNames = new Set(want.map((i) => i.name));
  const haveSet = new Set(have);
  const drops = have.filter((n) => !wantNames.has(n))
    .map((n) => ({ sql: `DROP INDEX IF EXISTS "${n}";`, params: [] }));
  const creates = want
    .filter((ix) => ix?.name && Array.isArray(ix.columns) && ix.columns.length && !haveSet.has(ix.name))
    .map((ix) => ({
      sql: `CREATE ${ix.unique ? 'UNIQUE ' : ''}INDEX IF NOT EXISTS "${ix.name}" ON ${table} (${ix.columns.map((c) => `"${c}"`).join(', ')});`,
      params: [],
    }));
  return { drops, creates };
}

// ─── the mutate() envelope (§10s increment 2c) ───────────────────────────────
//
// The 1:1 construction of one wire write: subject, idempotency id, payload,
// and the synthetic optimistic event applied locally. No SQL is ever parsed —
// mutate() is a constructor, not a query language. Pure: the clock and the
// last-version state stay in the shell.

/// The next version stamp: wall-clock ISO time widened to microseconds, bumped
/// past the last issued stamp when the clock has not moved (or moved backwards)
/// — a client's own versions are strictly monotonic even inside one millisecond.
/// (This is also where the §10q HLC candidate would land: feed `nowIso` the max
/// of the wall clock and the newest version seen via CDC.)
export function nextVersion(nowIso: string, lastVersion: string): string {
  let candidate = nowIso.replace('Z', '') + '000Z';
  if (candidate <= lastVersion) {
    const micros = (parseInt(lastVersion.slice(-7, -1), 10) + 1) % 1000000;
    candidate = `${lastVersion.slice(0, -7)}${String(micros).padStart(6, '0')}Z`;
  }
  return candidate;
}

/// NATS-subject-safe token: dots, wildcards and whitespace become dashes. The
/// msg_id rides as subject tokens on mutation_ack, and the version carries
/// fractional seconds — unescaped, one write's ack would fan out as a wildcard.
export const subjectSafeToken = (v: string): string => v.replace(/[.*>\s]/g, '-');

/// `prefix` is grammar.json's `subjects.mutations_prefix`; the shell passes it, the
/// fixtures rely on the protocol default.
/// PROTOCOL §9: the fleet heartbeat a client writes to `$KV.live.<tenant>.<principal>`.
/// Byte-identical across cores: stream keys sorted bytewise (JS default sort on ASCII
/// names is bytewise), fixed key order, integers verbatim. Pinned in fixtures/heartbeat.
export function heartbeatPayload(principal: string, tenant: string, ts: number, seqs: Record<string, number>): string {
  const streams: Record<string, number> = {};
  for (const k of Object.keys(seqs).sort()) streams[k] = seqs[k];
  return JSON.stringify({ principal, tenant, ts, streams });
}

export function mutationSubject(principal: string, table: string, op: string, prefix = 'mutation'): string {
  return `${prefix}.${principal}.${table}.${op.toLowerCase()}`;
}

/// The idempotency id. The version stays IN the id: a second edit to the same
/// row is a different write; a retry of the same edit is not.
export function mutationMsgId(clientId: string, table: string, id: string | number, version: string): string {
  return subjectSafeToken(`${clientId}-${table}-${id}-${version}`);
}

/// The row id mutate() reports and the outbox tracks: the PK values joined with
/// '|' (composite keys welcome — the echo-confirm joins the same way).
export function mutationKeyId(pkCols: string[], key: Record<string, unknown>): string {
  return pkCols.map((c) => key[c]).join('|');
}

/// The wire payload (PROTOCOL §7.4): key + version + client_id, plus `data`
/// for everything but DELETE — a delete is expressed by its key alone.
export function mutationPayload(
  op: 'INSERT' | 'UPDATE' | 'DELETE',
  key: Record<string, unknown>,
  values: Record<string, unknown> | undefined,
  version: string,
  clientId: string,
): Record<string, unknown> {
  const payload: Record<string, unknown> = { key, version, client_id: clientId };
  if (op !== 'DELETE') {
    // §10dv: a key column in `values` must equal the key. An UPDATE cannot move a row —
    // the ingress refuses it (`KeyChange`) — and the optimistic apply must not pretend
    // it can. Refused here, before an outbox row exists: rename = delete + create.
    for (const k of Object.keys(key)) {
      if (values && k in values && String(values[k]) !== String(key[k])) {
        throw new Error(`KeyChange: ${op} cannot change key column "${k}" (${String(key[k])} → ${String(values[k])}) — delete and re-create`);
      }
    }
    payload.data = values ?? {};
  }
  return payload;
}

/// The synthetic event the optimistic local apply runs through the SAME
/// applyEvent path as CDC: DELETE carries its key as data, lsn is pinned at
/// MAX_SAFE_INTEGER (an optimistic row must never lose to any gate), and the
/// `optimistic` flag keeps it out of position accounting and echo-confirm.
///
/// INSERT and UPDATE carry the KEY merged into the data (§10ds). The wire payload
/// stays sparse — `data` is only the columns the edit sets — but the local apply
/// addresses a row by its key like every CDC event does: without it an UPDATE that
/// did not repeat the key could not find its row and fell into the upsert's INSERT
/// arm, which failed NOT NULL on the key. The verdict and the echo were right, the
/// optimistic copy was not.
export function optimisticEvent(table: string, op: string, payload: Record<string, unknown>): Record<string, unknown> {
  const key = (payload as any).key as Record<string, unknown>;
  return {
    table,
    operation: op,
    data: op === 'DELETE' ? key : { ...key, ...((payload as any).data ?? {}) },
    lsn: Number.MAX_SAFE_INTEGER,
    optimistic: true,
  };
}

/// The whole envelope in one call — what a port implements first.
export function buildMutation(args: {
  principal: string; clientId: string; table: string;
  op: 'INSERT' | 'UPDATE' | 'DELETE';
  key: Record<string, unknown>; values?: Record<string, unknown>;
  pkCols: string[]; version: string;
  mutationsPrefix?: string;
}): { subject: string; msgId: string; id: string; payload: Record<string, unknown>; optimistic: Record<string, unknown> } {
  const id = mutationKeyId(args.pkCols, args.key);
  const payload = mutationPayload(args.op, args.key, args.values, args.version, args.clientId);
  return {
    subject: mutationSubject(args.principal, args.table, args.op, args.mutationsPrefix),
    msgId: mutationMsgId(args.clientId, args.table, id, args.version),
    id,
    payload,
    optimistic: optimisticEvent(args.table, args.op, payload),
  };
}

// ─── the hybrid logical clock (§10q, built §10s) ─────────────────────────────
//
// LWW on client-stamped time has one real hole: a device with a SLOW clock
// loses its own edits to rows it has just seen. The fix is the standard HLC
// move — the version stamp is the wall clock FLOORED by the newest version the
// client has observed (CDC events\' version column, chain cutoff_version), so a
// device that has seen the current row can never stamp below it. Arrival time
// never becomes the comparator (that would punish offline edits, §10q); the
// floor only lifts a lagging clock to just past what was already seen.

/// Canonical wire version: exactly six fractional digits. PG text output trims
/// trailing zeros (`.68582+00`), and mixed widths break both string comparison
/// (`.5Z` > `.50001Z` lexicographically, < numerically) and nextVersion\'s
/// fixed-width micro arithmetic. Non-timestamp strings pass through untouched.
export function normalizeVersion(v: string): string {
  const m = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$/.exec(v);
  if (!m) return v;
  return `${m[1]}.${(m[2] ?? '').padEnd(6, '0').slice(0, 6)}Z`;
}

export const maxVersion = (a: string, b: string): string => (b > a ? b : a);

export type OutboxCandidate = { msgId: string; version: string | null };
export type OutboxGate = { send: string[]; refuse: string[] };

/// PROTOCOL.md §MUST 6 — the GC watermark bounds how long a queued write stays
/// SENDABLE, and this decides which of an outbox's entries have passed it.
///
/// A mutation older than the watermark cannot be applied safely. The tombstone that
/// would have overruled it has already been reaped, so the server has nothing left to
/// compare against and the write lands as a resurrection of a row someone deleted.
/// That is the one failure LWW cannot catch on its own: every version comparison the
/// bridge could make has been discarded.
///
/// Deliberately conservative in both directions, because both unknowns are common and
/// neither is evidence of danger:
///
///   * no watermark (null) — the client has never received the one row of
///     `zebridge_gc_watermark`, e.g. it is not published in this deployment. Nothing
///     is known, so nothing is refused; refusing here would break every client of a
///     deployment that simply never enabled GC.
///   * no version on an entry — the server's own version guard is still in front of
///     it, and refusing a write we cannot judge would lose an edit for no reason.
///
/// The comparison is `<=`: a mutation stamped exactly AT the watermark is refused,
/// because the watermark is the oldest tombstone still standing — anything at or
/// before it may already have been reaped. String comparison after `normalizeVersion`,
/// the same rule the chain planner uses on cutoffs (§7.2 fixes the wire format, so
/// lexicographic order is chronological order once the widths match).
export function outboxWatermarkGate(
  entries: OutboxCandidate[],
  watermark: string | null,
): OutboxGate {
  if (!watermark) return { send: entries.map((e) => e.msgId), refuse: [] };
  const mark = normalizeVersion(watermark);
  const send: string[] = [];
  const refuse: string[] = [];
  for (const e of entries) {
    if (e.version && normalizeVersion(e.version) <= mark) refuse.push(e.msgId);
    else send.push(e.msgId);
  }
  return { send, refuse };
}


/// The HLC stamp: strictly after BOTH this client\'s own last stamp and the
/// newest version it has seen arrive. With an accurate clock this is exactly
/// the wall time; with a slow one it is the observed floor plus one microsecond.
export function hlcVersion(nowIso: string, lastVersion: string, seenFloor: string): string {
  return nextVersion(nowIso, maxVersion(lastVersion, seenFloor));
}
