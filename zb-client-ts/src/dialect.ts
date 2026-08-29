/// The dialect seam: what the shell has to say DIFFERENTLY to SQLite and to
/// PostgreSQL (PGlite in the browser, a local Postgres in a service).
///
/// The core's SQL is already portable — `INSERT … ON CONFLICT … DO UPDATE …
/// WHERE excluded."v" > t."v"`, quoted identifiers, `?` placeholders that an adapter
/// rewrites — and the CDC wire carries PostgreSQL's own text forms (`{a,b}`,
/// `{{1,2}}`, JSON, `t`/`f`, ISO `Z`), which SQLite stores as text and PostgreSQL
/// parses natively. What is NOT portable is the shell's housekeeping: how a table
/// is introspected, how FK enforcement is paused for a bulk load, how a constraint
/// is deferred inside a transaction, the conflict-ignore spelling for the
/// bookkeeping tables, a blob type, an auto-increment key, and — the one that
/// matters most — WHICH block of the schema descriptor carries the column types.
/// The bridge publishes both (`sqlite` and `pg`, NOTES §10c); until 2026-08-29 no
/// client read `pg`.
///
/// ⚠️ 64-bit integers. SQLite's INTEGER is 64-bit; PostgreSQL's is 32-bit. The
/// bookkeeping tables store LSNs (~1e10) and millisecond timestamps (~1.7e12) in
/// "INTEGER" columns, which is fine in SQLite and an overflow in PostgreSQL —
/// `int64` is the type name to use for those, and the PostgreSQL adapter must hand
/// int8 back as a JS number (see pglite-storage.ts), or `JSON.stringify` of the
/// sync state throws on a BigInt.

import type { Exec } from './storage.ts';

export type PhysicalColumn = { name: string; pk: number }; // pk: 1-based position in the PK, 0 = not part of it

export interface Dialect {
  readonly name: 'sqlite' | 'postgres';
  /// The descriptor block whose column types this engine speaks.
  readonly schemaBlock: 'sqlite' | 'pg';
  readonly blobType: string;
  readonly int64: string;
  readonly autoincrementPk: string;
  /// Whether table FKs must be declared DEFERRABLE for `deferForeignKeys` to work.
  readonly deferrableForeignKeys: boolean;

  /// The table as it physically exists (finding 9: the DATABASE answers, never a map).
  tableInfo(exec: Exec, table: string): Promise<PhysicalColumn[]>;
  /// The stored CREATE TABLE text, or null on an engine that keeps none — then an
  /// FK change cannot be detected by text compare and the rebuild path is skipped.
  tableDdl(exec: Exec, table: string): Promise<string | null>;
  /// Secondary index names the shell may drop — never the engine's own (the PK's).
  indexNames(exec: Exec, table: string): Promise<string[]>;
  /// Session-level FK enforcement, for the bulk load of an already-consistent set.
  setForeignKeys(exec: Exec, on: boolean): Promise<void>;
  /// Inside a transaction: check FKs at COMMIT, so a child may precede its parent.
  deferForeignKeys(tx: Exec): Promise<void>;
  /// Violations after a bulk load; -1 when the engine cannot answer.
  foreignKeyViolations(exec: Exec): Promise<number>;
  /// `INSERT … ` that ignores a conflicting row / replaces it. `?` placeholders.
  insertIgnore(table: string, cols: string[], conflictCols: string[]): string;
  insertReplace(table: string, cols: string[], conflictCols: string[]): string;
  /// Replace the table's FOREIGN KEYs in place, for engines that can ALTER a
  /// constraint. Absent (SQLite) → the shell rebuilds the table instead, which is
  /// the only way there — and which PostgreSQL would refuse for a referenced parent.
  alterForeignKeys?(exec: Exec, table: string, fks: ForeignKeySpec[]): Promise<void>;
}

export type ForeignKeySpec = { columns: string[]; references: string; parent_columns: string[] };

const ph = (n: number) => Array.from({ length: n }, () => '?').join(', ');
const q = (cols: string[]) => cols.map((c) => `"${c}"`).join(', ');

export const sqliteDialect: Dialect = {
  name: 'sqlite',
  schemaBlock: 'sqlite',
  blobType: 'BLOB',
  int64: 'INTEGER',
  autoincrementPk: 'INTEGER PRIMARY KEY AUTOINCREMENT',
  deferrableForeignKeys: false,
  async tableInfo(exec, table) {
    const phys: any[] = (await exec(`PRAGMA table_info("${table}")`)) ?? [];
    return phys.map((c) => ({ name: c.name, pk: c.pk ?? 0 }));
  },
  async tableDdl(exec, table) {
    const rows = await exec(`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, table);
    return rows?.[0]?.sql ?? '';
  },
  async indexNames(exec, table) {
    const rows = await exec(
      `SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND name NOT LIKE 'sqlite_%'`, table);
    return (rows ?? []).map((r: any) => r.name);
  },
  async setForeignKeys(exec, on) { await exec(`PRAGMA foreign_keys = ${on ? 'ON' : 'OFF'};`); },
  async deferForeignKeys(tx) { await tx(`PRAGMA defer_foreign_keys = ON;`); },
  async foreignKeyViolations(exec) { return ((await exec(`PRAGMA foreign_key_check;`)) ?? []).length; },
  insertIgnore: (t, cols) => `INSERT OR IGNORE INTO ${t} (${q(cols)}) VALUES (${ph(cols.length)})`,
  insertReplace: (t, cols) => `INSERT OR REPLACE INTO ${t} (${q(cols)}) VALUES (${ph(cols.length)})`,
};

export const postgresDialect: Dialect = {
  name: 'postgres',
  schemaBlock: 'pg',
  blobType: 'BYTEA',
  int64: 'BIGINT',
  autoincrementPk: 'BIGSERIAL PRIMARY KEY',
  deferrableForeignKeys: true,
  async tableInfo(exec, table) {
    // Regular columns in attnum order; the PK position comes from the primary
    // index's column vector. `to_regclass` answers NULL for an absent table rather
    // than erroring, which is the "first sight" case this exists for.
    const rows = await exec(
      `SELECT a.attname AS name,
              COALESCE(array_position(i.indkey::int2[], a.attnum), 0) AS pk
         FROM pg_attribute a
         LEFT JOIN pg_index i ON i.indrelid = a.attrelid AND i.indisprimary
        WHERE a.attrelid = to_regclass(?) AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum`, table);
    return (rows ?? []).map((r: any) => ({ name: r.name, pk: Number(r.pk ?? 0) }));
  },
  /// PostgreSQL keeps no CREATE TABLE text; what the shell compares is the FK
  /// clause text, so synthesize exactly the form `core.fkClausesFor` emits (with the
  /// DEFERRABLE suffix this dialect always requests) from `pg_constraint`, and the
  /// text compare in `core.fkTextDiffers` needs no engine branch. An absent table
  /// answers '' — the create path owns it.
  async tableDdl(exec, table) {
    const fks = await pgForeignKeys(exec, table);
    if (fks === null) return '';
    return 'CREATE TABLE ' + table + ' (' + fks.map((f) =>
      `, FOREIGN KEY (${q(f.columns)}) REFERENCES ${f.references} (${q(f.parent_columns)}) DEFERRABLE INITIALLY IMMEDIATE`).join('') + ')';
  },
  async indexNames(exec, table) {
    // Secondary indexes only: not the PK's, not one backing a constraint.
    const rows = await exec(
      `SELECT c.relname AS name
         FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE i.indrelid = to_regclass(?) AND NOT i.indisprimary
          AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid)`, table);
    return (rows ?? []).map((r: any) => r.name);
  },
  // ⚠️ `replica` skips FK (and other) triggers for the session; nothing is re-checked
  // when it goes back to `origin`. Same contract as SQLite's PRAGMA — the bulk load is
  // of an already-consistent snapshot — and `foreignKeyViolations` below is the
  // post-load check that PostgreSQL does not offer as a pragma.
  async setForeignKeys(exec, on) { await exec(`SET session_replication_role = ${on ? 'origin' : 'replica'}`); },
  async deferForeignKeys(tx) { await tx(`SET CONSTRAINTS ALL DEFERRED`); },
  /// PostgreSQL has no `foreign_key_check`, and `session_replication_role = replica`
  /// re-checks nothing on the way back to `origin` — so this asks the catalogue for
  /// every FK and counts the children with no parent, one anti-join per constraint.
  /// Same answer SQLite's pragma gives; a few catalogue reads more.
  async foreignKeyViolations(exec) {
    const fks: any[] = (await exec(
      `SELECT c.conname,
              c.conrelid::regclass::text  AS child,
              c.confrelid::regclass::text AS parent,
              (SELECT array_agg(a.attname ORDER BY k.ord)
                 FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
                 JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum) AS cols,
              (SELECT array_agg(a.attname ORDER BY k.ord)
                 FROM unnest(c.confkey) WITH ORDINALITY AS k(attnum, ord)
                 JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.attnum) AS pcols
         FROM pg_constraint c
        WHERE c.contype = 'f' AND c.connamespace = 'public'::regnamespace`)) ?? [];
    let total = 0;
    for (const fk of fks) {
      const cols: string[] = fk.cols ?? [];
      const pcols: string[] = fk.pcols ?? [];
      if (!cols.length || cols.length !== pcols.length) continue;
      const notNull = cols.map((c) => `ch."${c}" IS NOT NULL`).join(' AND ');
      const join = cols.map((c, i) => `p."${pcols[i]}" = ch."${c}"`).join(' AND ');
      const rows = await exec(
        `SELECT count(*) AS n FROM ${fk.child} ch WHERE ${notNull} AND NOT EXISTS (SELECT 1 FROM ${fk.parent} p WHERE ${join})`);
      total += Number(rows?.[0]?.n ?? 0);
    }
    return total;
  },
  insertIgnore: (t, cols) =>
    `INSERT INTO ${t} (${q(cols)}) VALUES (${ph(cols.length)}) ON CONFLICT DO NOTHING`,
  insertReplace: (t, cols, conflictCols) =>
    `INSERT INTO ${t} (${q(cols)}) VALUES (${ph(cols.length)}) ON CONFLICT (${q(conflictCols)}) DO UPDATE SET ` +
    cols.filter((c) => !conflictCols.includes(c)).map((c) => `"${c}" = EXCLUDED."${c}"`).join(', '),
  /// Drop every FK the table holds, add every FK wanted — in place, data untouched.
  /// ⚠️ Not a rebuild: `DROP TABLE` of a referenced parent is refused by PostgreSQL
  /// regardless of `session_replication_role`, which is about triggers, not
  /// dependencies. ALTER is the native path, and it keeps the table's identity.
  async alterForeignKeys(exec, table, fks) {
    const have: any[] = (await exec(
      `SELECT conname FROM pg_constraint WHERE contype = 'f' AND conrelid = to_regclass(?)`, table)) ?? [];
    for (const c of have) await exec(`ALTER TABLE ${table} DROP CONSTRAINT "${c.conname}"`);
    for (const f of fks) {
      await exec(`ALTER TABLE ${table} ADD FOREIGN KEY (${q(f.columns)}) REFERENCES ${f.references} (${q(f.parent_columns)}) DEFERRABLE INITIALLY IMMEDIATE`);
    }
  },
};

/// The table's FKs from the catalogue, in definition order; null when the table
/// does not exist.
async function pgForeignKeys(exec: Exec, table: string): Promise<ForeignKeySpec[] | null> {
  const exists = await exec(`SELECT to_regclass(?) IS NOT NULL AS ok`, table);
  if (!exists?.[0]?.ok) return null;
  const rows: any[] = (await exec(
    `SELECT c.confrelid::regclass::text AS parent,
            (SELECT array_agg(a.attname ORDER BY k.ord)
               FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
               JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum) AS cols,
            (SELECT array_agg(a.attname ORDER BY k.ord)
               FROM unnest(c.confkey) WITH ORDINALITY AS k(attnum, ord)
               JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.attnum) AS pcols
       FROM pg_constraint c
      WHERE c.contype = 'f' AND c.conrelid = to_regclass(?)
      ORDER BY c.oid`, table)) ?? [];
  return rows.map((r) => ({ columns: r.cols ?? [], references: r.parent, parent_columns: r.pcols ?? [] }));
}

/// `?` → `$1, $2, …` for engines that number their parameters. Skips quoted
/// strings and quoted identifiers, so a literal `?` inside them is left alone.
export function numberPlaceholders(sql: string): string {
  let out = '';
  let n = 0;
  let quote: string | null = null;
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (quote) {
      out += ch;
      if (ch === quote) quote = null;
    } else if (ch === "'" || ch === '"') {
      quote = ch;
      out += ch;
    } else if (ch === '?') {
      out += `$${++n}`;
    } else {
      out += ch;
    }
  }
  return out;
}
