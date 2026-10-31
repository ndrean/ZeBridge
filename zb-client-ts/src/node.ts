/// Node adapter: better-sqlite3 storage + TCP transport (@nats-io/transport-node).
/// Import from '@zebridge/client/node'. The two deps are the HOST's to install
/// (optional peers) — a browser bundle never sees this file.
import Database from 'better-sqlite3';
import { connect as tcpConnect } from '@nats-io/transport-node';
import { bindEndpointTls } from './node-tls.ts';
import { realpathSync } from 'node:fs';
import { unlink } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';
import type { Exec, StorageFactory } from './storage.ts';

/// The single-owner lock of a file-backed replica: a SQLite `BEGIN EXCLUSIVE` held on `<replica>.lock`,
/// the same lock libzb takes (storage.zig `ReplicaLock`), so the two clients exclude each other.
/// Nonblocking, dropped by the OS when the process dies, and never taken by a reader.
/// The lock's identity is the replica's canonical location, not the path as given: a symlink to the file,
/// a symlinked directory or a relative spelling must reach the same `.lock` (one owner on real.sqlite3 and a
/// second on its symlink were both let in). A file that exists resolves whole; one not created yet resolves
/// its directory and keeps its name. The same rule as libzb's `ReplicaLock`, so the two clients still exclude
/// each other. Not unified: hardlinks, and names that differ only by case on a case-insensitive disk.
export const canonicalReplica = (p: string): string => {
  try { return realpathSync.native(p); }
  catch (e: any) {
    if (e?.code !== 'ENOENT') throw e;
    const abs = resolve(p);
    return join(realpathSync.native(dirname(abs)), basename(abs));
  }
};

export const replicaLock = (dbName: string) => {
  let held: Database.Database | null = null;
  return {
    acquire() {
      if (held || dbName === '' || dbName === ':memory:') return;
      const l = new Database(`${canonicalReplica(dbName)}.lock`, { timeout: 0 });
      try { l.exec('BEGIN EXCLUSIVE'); }
      catch (e: any) {
        l.close();
        if (e?.code !== 'SQLITE_BUSY' && e?.code !== 'SQLITE_LOCKED') throw e;
        throw Object.assign(new Error(`ReplicaAlreadyInUse: ${dbName} is held by another synchronization owner`), { code: 'ReplicaAlreadyInUse' });
      }
      held = l;
    },
    release() { held?.close(); held = null; },
  };
};

/// The replica's connection with the contract pragmas, closed again if one of them throws.
const openConfigured = (dbName: string): Database.Database => {
  const db = new Database(dbName);
  try {
    // 16 KB pages for a replica THIS open creates (an existing file keeps its own; the
    // pragma is a no-op once in WAL mode). §10ja, 2026-09-26: a random-key live insert
    // reads one index leaf per probe, and a 16 KB leaf holds four times the keys of a
    // 4 KB one — libzb's cold 300k-insert drain into a 4.5M-row replica went 22.7 → 9.8 s.
    db.pragma('page_size = 16384');
    db.pragma('journal_mode = WAL');
    // §10he: the commit's fsync and the automatic checkpoint every 1,000 WAL pages were a
    // third of libzb's CDC thread on the firehose replica; the same defaults apply here.
    // NORMAL in WAL mode stays consistent through a crash and may lose the last
    // transactions, which a replica re-fetches from the stream.
    db.pragma('synchronous = NORMAL');
    db.pragma('wal_autocheckpoint = 10000');
    // better-sqlite3 already defaults this ON, which is exactly why it is spelled
    // out: an invariant that holds by a dependency's default is one upgrade away
    // from not holding, and the browser adapter had the opposite default.
    db.pragma('foreign_keys = ON');
  } catch (e) { db.close(); throw e; }
  return db;
};

export const nodeStorage: StorageFactory = (dbName) => {
  const lock = replicaLock(dbName);
  lock.acquire();   // before the file is touched: a second owner is refused with the first one's state intact
  // An open or a pragma that throws must give the replica up: the lock is held for this process, and a retry
  // after the cause is fixed was told ReplicaAlreadyInUse by its own failed attempt.
  let db: Database.Database;
  try { db = openConfigured(dbName); }
  catch (e) { lock.release(); throw e; }

  // §10gp: prepared statements, kept by their text. better-sqlite3 compiles the SQL on
  // every `prepare`, and the CDC path runs the same handful of statements — one per
  // (table, operation, column set) — thousands of times a second. libzb's storage has
  // had this cache for as long; this side was paying the compile on every event. Cleared
  // wholesale past the cap: a replica has few shapes, and a DDL change makes new text.
  const stmts = new Map<string, any>();
  const prepared = (text: string) => {
    let st = stmts.get(text);
    if (st === undefined) {
      if (stmts.size >= 512) stmts.clear();
      st = db.prepare(text);
      stmts.set(text, st);
    }
    return st;
  };

  const exec: Exec = async (q, ...params) => {
    const text = q.trim().replace(/;\s*$/, '');
    if (/^PRAGMA\b/i.test(text)) {
      const r = db.pragma(text.replace(/^PRAGMA\s+/i, ''));
      return Array.isArray(r) ? r : [];
    }
    const stmt = prepared(text);
    // Binding is SEMANTICS, so it belongs to the contract (storage.ts): sqlocal's
    // wasm SQLite binds JS booleans as 0/1 and undefined as NULL; better-sqlite3
    // REFUSES both ("can only bind numbers, strings, bigints, buffers, and
    // null") — measured: the optimistic apply and every boolean-carrying CDC
    // echo failed on Node while the same rows applied fine in the browser.
    const bound = params.map((p) =>
      p === undefined ? null : typeof p === 'boolean' ? (p ? 1 : 0) : p);
    return stmt.reader ? stmt.all(...bound) : (stmt.run(...bound), []);
  };

  // The storage contract: transactions must serialize. better-sqlite3 is a
  // single synchronous connection with no queue of its own, and the core runs
  // several lanes concurrently (a CDC consumer per stream + the write path), so
  // two overlapping transactions would interleave their BEGINs and lose whole
  // batches to `cannot start a transaction within a transaction`. A promise
  // chain is the whole fix: each transaction waits for the previous to settle.
  let queue: Promise<void> = Promise.resolve();

  // §10di: the application's `query()` answers on a SECOND connection, opened
  // read-only — enforcement by the engine, not by convention (the libzb design).
  // Opened lazily, after the first connection created the file and its WAL.
  let ro: Database.Database | null = null;
  const readOnly: Exec = async (q, ...params) => {
    if (!ro) {
      ro = new Database(dbName, { readonly: true, fileMustExist: true });
      ro.pragma('foreign_keys = ON');
    }
    const text = q.trim().replace(/;\s*$/, '');
    if (/^PRAGMA\b/i.test(text)) {
      const r = ro.pragma(text.replace(/^PRAGMA\s+/i, ''));
      return Array.isArray(r) ? r : [];
    }
    const stmt = ro.prepare(text);
    const bound = params.map((p) =>
      p === undefined ? null : typeof p === 'boolean' ? (p ? 1 : 0) : p);
    return stmt.reader ? stmt.all(...bound) : (stmt.run(...bound), []);
  };

  return {
    exec,
    spillsTemp: true, // better-sqlite3 on a real filesystem: the sorter spills to disk
    readOnly,
    // better-sqlite3's own .transaction() is synchronous-only; the core's apply
    // paths are async. One connection, sequential use → manual BEGIN/COMMIT.
    transaction: (fn) => {
      const run = queue.then(async () => {
        await exec('BEGIN IMMEDIATE');
        try { await fn(exec); await exec('COMMIT'); }
        catch (e) { await exec('ROLLBACK'); throw e; }
      });
      queue = run.catch(() => {});   // a failed transaction must not wedge the lane
      return run;
    },
    release: () => lock.release(),
    reacquire: () => lock.acquire(),
    deleteDatabaseFile: async () => {
      if (ro) { ro.close(); ro = null; }
      db.close();
      lock.release();
      for (const f of [dbName, dbName + '-wal', dbName + '-shm']) {
        try { await unlink(f); } catch { /* absent is fine */ }
      }
    },
  };
};

/// TCP dial with the same option shape the core passes to its default (wsconnect).
export const nodeConnect = (opts: any) => tcpConnect(bindEndpointTls(opts));
