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
