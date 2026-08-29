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

export type ManifestDelta = { object: string; cutoff: string; prev_cutoff: string; gen: number; dict?: string };
export type ChainManifest = {
  gen: number;
  full?: { object: string; gen: number } | null;
  deltas?: ManifestDelta[];
  cutoff_seq?: number;
  cdc_stream?: string;
};
/// `dict` names the dictionary object a delta was compressed with (§10x) —
/// carried through so the applier fetches it before decoding.
export type PlanStep = { name: string; kind: 'full' | 'delta'; dict?: string };

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
  const step = (d: ManifestDelta): PlanStep => ({ name: d.object, kind: 'delta', ...(d.dict ? { dict: d.dict } : {}) });
  if (reaches) return applicable.map(step);
  if (!man.full) return [];
  return [
    { name: man.full.object, kind: 'full' as const },
    ...deltas.filter((d) => d.gen > man.full!.gen).map(step),
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

// ─── the apply SQL builders (§10s increment 2a) ──────────────────────────────
//
// The exact statements a replica executes for one CDC event and for one chain
// row. Pure string/param construction — the shell owns exec, transactions,
// logging and error classification. A port producing byte-identical SQL and
// params is applying events exactly like this client.

export type SqlStep = { sql: string; params: any[] };
export type KeyChangeStep = SqlStep & { oldKey: any[]; newKey: any[] };

/// One CDC value → one bound parameter: structured values travel as JSON text
/// (SQLite has no object affinity); everything else binds as-is (the CDC wire
/// already normalizes timestamps).
export const cdcValue = (v: any): any =>
  v !== null && typeof v === 'object' ? JSON.stringify(v) : v;

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

/// The payload of a LOCAL write as a PostgreSQL engine must bind it: arrays as
/// array literals, plain objects as JSON (jsonb reads that), scalars unchanged. CDC
/// events need none of this — the wire is already in these forms.
export function pgEngineValues(data: Record<string, any>): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(data)) {
    out[k] = Array.isArray(v) ? pgArrayLiteral(v) : v !== null && typeof v === 'object' ? JSON.stringify(v) : v;
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

/// One chain row → bound parameters: structured values as JSON, text-mode
/// timestamps normalized to the CDC wire shape so the version guard compares
/// like against like (NOTES §1.13).
export const chainRowParams = (row: any[]): any[] =>
  row.map((v) => (v !== null && typeof v === 'object' ? JSON.stringify(v) : pgTsToWire(v)));

// ─── the schema migration planner (§10s increment 2b — finding 9's home) ────
//
// Everything applySchema DECIDES, as pure functions over data the shell
// fetches: the incoming descriptor, the existing column list (from
// syncedTables or PRAGMA table_info — never from memory alone, finding 9),
// the stored CREATE TABLE text, and the existing index names. The shell
// executes, logs, and owns the runtime ALTER→rebuild fallback (that decision
// is error-driven, not plannable).

export type SchemaColumn = { name: string; type: string; required?: boolean };
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
export type DdlOptions = { deferrable?: boolean };

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
    { sql: `CREATE TABLE ${table} (${tableBody(cols, pkCols, foreignKeys, opts)});`, params: [] },
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
    { sql: `CREATE TABLE ${tmp} (${tableBody(cols, pkCols, foreignKeys, opts)});`, params: [] },
  ];
  const common = cols.map((c) => c.name).filter((n) => existingColumns.includes(n)).map((n) => `"${n}"`);
  if (common.length) {
    steps.push({ sql: `INSERT INTO ${tmp} (${common.join(', ')}) SELECT ${common.join(', ')} FROM ${table};`, params: [] });
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

/// Does the stored CREATE TABLE text disagree with the FK clauses now wanted?
/// Text-compared because SQLite keeps no queryable "expected constraints", and
/// the stored DDL is our own generated text. Empty ddl → false (no table yet:
/// the create path owns it).
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

export function mutationSubject(principal: string, table: string, op: string): string {
  return `mutation.${principal}.${table}.${op.toLowerCase()}`;
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
  if (op !== 'DELETE') payload.data = values ?? {};
  return payload;
}

/// The synthetic event the optimistic local apply runs through the SAME
/// applyEvent path as CDC: DELETE carries its key as data, lsn is pinned at
/// MAX_SAFE_INTEGER (an optimistic row must never lose to any gate), and the
/// `optimistic` flag keeps it out of position accounting and echo-confirm.
export function optimisticEvent(table: string, op: string, payload: Record<string, unknown>): Record<string, unknown> {
  return {
    table,
    operation: op,
    data: op === 'DELETE' ? (payload as any).key : (payload as any).data,
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
}): { subject: string; msgId: string; id: string; payload: Record<string, unknown>; optimistic: Record<string, unknown> } {
  const id = mutationKeyId(args.pkCols, args.key);
  const payload = mutationPayload(args.op, args.key, args.values, args.version, args.clientId);
  return {
    subject: mutationSubject(args.principal, args.table, args.op),
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
