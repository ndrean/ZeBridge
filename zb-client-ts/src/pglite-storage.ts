/// PGlite storage: PostgreSQL in the browser (WASM). "PG to PG": the CDC wire
/// carries PostgreSQL's own text forms, so here `text[]` is a real array, `jsonb` a
/// real document, `numeric(20,8)` keeps its eight decimals — nothing is flattened
/// to text on the way in.
///
/// Two modes, chosen by the host (`makePgliteStorage`):
///   in memory  (default) a fresh database per load — the clean-room convention.
///   persist    `idb://<dbName>`: PGlite's IndexedDB-backed filesystem, which works
///              on the main thread (the OPFS access-handle pool needs a worker). The
///              durable replica + outbox across reloads, for the same reason the
///              SQLite adapter has a stable per-principal file behind `durable`.
///
/// The serialization contract (storage.ts) is met the way sqlocal meets it: PGlite
/// is one connection with one internal queue, so `exec` and `transaction` calls
/// from the core's several lanes cannot interleave inside the engine. That single
/// owned connection is also the write-path lock the README asks about: the app gets
/// `query()` and `mutate()`, never the handle.
///
/// ⚠️ Parsers, deliberately narrowed. PGlite parses result cells by type OID. Three
/// defaults would change the client's semantics and are overridden here:
///   int8 → BigInt: the sync state (lsn, seq) lives in BIGINT bookkeeping columns
///     (dialect.ts), and a BigInt breaks `JSON.stringify` and mixed arithmetic —
///     parsed to Number, exact up to 2^53, which an LSN or a stream seq is.
///   timestamptz / timestamp / date → Date: the version guard compares versions as
///     TEXT everywhere else (SQLite stores the wire string), and a Date would turn a
///     before-image into an object the revert cannot bind — kept as the text
///     PostgreSQL renders.
///   numeric → Number: precision loss on the one column type that exists to avoid
///     it — kept as text.
import { PGlite, types } from '@electric-sql/pglite';
import type { Exec, StorageFactory } from './storage.ts';
import { numberPlaceholders, postgresDialect } from './dialect.ts';

const asText = (x: string | null) => x;

export type PgliteStorageOptions = {
  /// Keep the database across reloads (`idb://<dbName>`); default in memory.
  persist?: boolean;
};

export function makePgliteStorage(opts: PgliteStorageOptions = {}): StorageFactory {
  return (dbName) => {
    // ⚠️ ONE options argument. PGlite's constructor is `(dataDir?: string, options?)`,
    // but its body reads `typeof dataDir == "string" ? {dataDir, ...options} :
    // options = dataDir` — so `new PGlite(undefined, opts)` silently DISCARDS opts
    // (measured: every timestamptz came back as a Date with the parsers below
    // apparently in place). An object as the first argument keeps them; `dataDir`
    // inside it selects persistence.
    const idbName = `/pglite/${dbName}`;
    const pg = new PGlite({
      ...(opts.persist ? { dataDir: `idb://${dbName}` } : {}),
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
      deleteDatabaseFile: async () => {
        await pg.close();
        // In memory, closing IS deleting. Persisted: drop the IndexedDB database
        // PGlite keeps under `/pglite/<name>` — best effort, the way sqlocal's
        // deleteDatabaseFile is.
        if (opts.persist && typeof indexedDB !== 'undefined') {
          await new Promise<void>((resolve) => {
            const req = indexedDB.deleteDatabase(idbName);
            req.onsuccess = req.onerror = req.onblocked = () => resolve();
          });
        }
      },
    };
  };
}

/// The in-memory default, for hosts that want a factory without options.
export const pgliteStorage: StorageFactory = makePgliteStorage();
