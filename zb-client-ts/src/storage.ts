/// The storage seam (NOTES.md §10). The core never talks to a database driver —
/// it talks to THIS. One call shape (`Exec`), one transaction shape (an Exec
/// scoped to the transaction), one lifecycle verb. The browser default lives in
/// browser-storage.ts (sqlocal/OPFS); Node's adapter in node.ts (better-sqlite3).
/// Keeping drivers behind a factory is also the §10 lesson made structural: a
/// driver that spawns a worker (sqlocal) makes its URL resolution the HOST
/// bundler's problem — so the host, not the core, chooses the driver.

export type Exec = (q: string, ...params: any[]) => Promise<any[]>;

export interface Storage {
  exec: Exec;
  /// Runs fn inside ONE transaction; the passed Exec is scoped to it.
  /// Rolls back if fn throws, commits otherwise.
  ///
  /// ⚠️ CONTRACT: the adapter MUST serialize. The core drives several lanes at
  /// once (one CDC consumer per stream, plus the write path), so `transaction`
  /// and `exec` can be called concurrently — and an adapter that lets two
  /// transactions interleave gets `cannot start a transaction within a
  /// transaction` and drops whole batches. sqlocal satisfied this invisibly
  /// (one worker, one queue), which is exactly why it went unstated until a
  /// second adapter appeared. If your driver does not serialize, wrap it (see
  /// node.ts).
  transaction(fn: (tx: Exec) => Promise<void>): Promise<void>;
  deleteDatabaseFile(): Promise<void>;
  /// What the engine speaks (dialect.ts). Absent means SQLite — the two adapters
  /// that predate the seam. An adapter over PostgreSQL (PGlite, a local server)
  /// MUST say so: the shell picks the descriptor's `pg` block, BIGINT bookkeeping
  /// and `session_replication_role` from it.
  dialect?: Dialect;
  /// §10ix: whether the engine's temp store spills to a real disk. When true, a
  /// streamed FULL is STAGED — windows appended to a TEMP table, the real table
  /// filled once by `INSERT … SELECT … ORDER BY pk`, SQLite's external sorter doing
  /// the ordering on disk (measured: 43 s, 786 MB peak for 3M rows, but ~3× the table
  /// on disk while it runs). Absent or false, each window is sorted and applied on
  /// its own: bounded in memory AND disk, but each window scatters across the whole
  /// b-tree — in Chrome over OPFS that crawled to 2k rows/s past 750k rows. Node, a
  /// phone, AND the browser say true: sqlite-wasm's OPFS VFS spills TEMP and the
  /// sorter to OPFS files once `temp_store = FILE` is set (browser-storage.ts) —
  /// 3M rows in 156 s in Chrome, 2.3 GB of OPFS transiently. Keep false only for an
  /// engine whose temp store is memory with no way out.
  spillsTemp?: boolean;
  /// §10ix: the temp files a heavy operation may leave behind, and their removal.
  /// sqlite-wasm's OPFS VFS names its temp files (the TEMP database, the sorter's
  /// spill) 16 random letters in the origin's root and tries to remove them on close
  /// — and quietly fails to: 1.3 GB of orphans after a 3M-row seed in Chrome, gone
  /// only when someone removed them by hand. `tempFiles` says what such files exist
  /// now; `sweepTemp` removes the ones that appeared since and are closed (an open
  /// one refuses removal and is left alone), and returns the bytes reclaimed. A
  /// storage over a real filesystem, where the OS honours delete-on-close, leaves
  /// both undefined.
  tempFiles?(): Promise<Set<string>>;
  sweepTemp?(before: Set<string>): Promise<number>;
  /// An Exec that CANNOT write (§10di): a second connection opened read-only, the
  /// libzb design. `query()` runs on it when present; when absent (one handle —
  /// the browser's OPFS, PGlite) the shell guards `query()` by statement shape
  /// (`core.isReadOnlySql`) instead. Either way the application never reaches the
  /// bookkeeping (`_zebridge_outbox`, positions, the shape record) through the API.
  readOnly?: Exec;
}

import type { Dialect } from './dialect.ts';

/// The host hands the core a factory, not an instance: the core owns the DB
/// NAME (per-principal, or per-load in the browser dev convention).
///
/// ⚠️ CONTRACT: an adapter MUST enable `foreign_keys`. SQLite defaults it OFF and
/// it is PER CONNECTION, and our two adapters disagreed on it by accident —
/// better-sqlite3 turns it on when it opens a database, sqlocal never sets it.
/// Left alone, the same core over the same data would ENFORCE referential
/// integrity in Node and IGNORE it in the browser.
///
/// ⚠️ CONTRACT: value BINDING is semantics too. A JS boolean binds as 0/1 and
/// `undefined` binds as NULL, whatever the engine natively accepts — sqlocal
/// does this implicitly, better-sqlite3 refuses both and must coerce (node.ts).
/// Left unaligned, the same event applies in one adapter and errors in the other.
///
/// The line to hold: an adapter may choose pragmas that trade PERFORMANCE
/// (`journal_mode`, `synchronous`, cache size) as its engine sees fit, but pragmas
/// that change SEMANTICS belong to the contract, because a consumer must not have
/// to know which adapter it is running on to know what its data means.
export type StorageFactory = (dbName: string) => Storage;
