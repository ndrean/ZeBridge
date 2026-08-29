/// PGlite storage: PostgreSQL in the browser (WASM), in memory by default — a
/// fresh database per load, the same clean-room convention as the OPFS SQLite
/// adapter. "PG to PG": the CDC wire carries PostgreSQL's own text forms, so here
/// `text[]` is a real array, `jsonb` a real document, `numeric(20,8)` keeps its
/// eight decimals — nothing is flattened to text on the way in.
///
/// The serialization contract (storage.ts) is met the way sqlocal meets it: PGlite
/// is one connection with one internal queue, so `exec` and `transaction` calls
/// from the core's several lanes cannot interleave inside the engine.
///
/// ⚠️ Parsers, deliberately narrowed. PGlite parses result cells by type OID. Two
/// defaults would change the client's semantics and are overridden here:
///   int8 → BigInt: the sync state (lsn, seq) lives in BIGINT bookkeeping columns
///     (dialect.ts), and a BigInt breaks `JSON.stringify` and mixed arithmetic —
///     parsed to Number, exact up to 2^53, which an LSN or a stream seq is.
///   timestamptz / timestamp → Date: the version guard compares versions as TEXT
///     everywhere else (SQLite stores the wire string), and a Date would turn a
///     before-image into an object the revert cannot bind — kept as the text
///     PostgreSQL renders.
///   numeric → Number: precision loss on the one column type that exists to avoid
///     it — kept as text.
import { PGlite, types } from '@electric-sql/pglite';
import type { Exec, StorageFactory } from './storage.ts';
import { numberPlaceholders, postgresDialect } from './dialect.ts';

const asText = (x: string | null) => x;

export const pgliteStorage: StorageFactory = (_dbName) => {
  // ⚠️ ONE options argument, no `dataDir`. PGlite's constructor is
  // `(dataDir?: string, options?)`, but its body reads `typeof dataDir == "string"
  // ? {dataDir, ...options} : options = dataDir` — so `new PGlite(undefined, opts)`
  // silently DISCARDS opts (measured: every timestamptz came back as a Date with the
  // parsers below apparently in place). An object as the first argument is the form
  // that keeps them. No dataDir → in-memory.
  const pg = new PGlite({
    parsers: {
      [types.INT8]: (x: string | null) => (x === null ? null : Number(x)),
      [types.TIMESTAMPTZ]: asText,
      [types.TIMESTAMP]: asText,
      [types.DATE]: asText,
      [types.NUMERIC]: asText,
    },
  });
  // `?` → `$n` once per statement text; `undefined` binds as NULL (the storage
  // contract — better-sqlite3 refuses it, sqlocal coerces, PGlite must agree).
  const bind = (params: any[]) => params.map((p) => (p === undefined ? null : p));
  const exec: Exec = async (q, ...params) => (await pg.query(numberPlaceholders(q), bind(params))).rows as any[];
  return {
    dialect: postgresDialect,
    exec,
    transaction: (fn) =>
      pg.transaction(async (tx) => {
        const txExec: Exec = async (q, ...p) => (await tx.query(numberPlaceholders(q), bind(p))).rows as any[];
        await fn(txExec);
      }),
    // In-memory: closing IS deleting.
    deleteDatabaseFile: () => pg.close(),
  };
};
