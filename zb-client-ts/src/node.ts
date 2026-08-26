/// Node adapter: better-sqlite3 storage + TCP transport (@nats-io/transport-node).
/// Import from 'zb-client-ts/node'. The two deps are the HOST's to install
/// (optional peers) — a browser bundle never sees this file.
import Database from 'better-sqlite3';
import { connect as tcpConnect } from '@nats-io/transport-node';
import { unlink } from 'node:fs/promises';
import type { Exec, StorageFactory } from './storage.ts';

export const nodeStorage: StorageFactory = (dbName) => {
  const db = new Database(dbName);
  db.pragma('journal_mode = WAL');
  // better-sqlite3 already defaults this ON, which is exactly why it is spelled
  // out: an invariant that holds by a dependency's default is one upgrade away
  // from not holding, and the browser adapter had the opposite default.
  db.pragma('foreign_keys = ON');

  const exec: Exec = async (q, ...params) => {
    const text = q.trim().replace(/;\s*$/, '');
    if (/^PRAGMA\b/i.test(text)) {
      const r = db.pragma(text.replace(/^PRAGMA\s+/i, ''));
      return Array.isArray(r) ? r : [];
    }
    const stmt = db.prepare(text);
    return stmt.reader ? stmt.all(...params) : (stmt.run(...params), []);
  };

  // The storage contract: transactions must serialize. better-sqlite3 is a
  // single synchronous connection with no queue of its own, and the core runs
  // several lanes concurrently (a CDC consumer per stream + the write path), so
  // two overlapping transactions would interleave their BEGINs and lose whole
  // batches to `cannot start a transaction within a transaction`. A promise
  // chain is the whole fix: each transaction waits for the previous to settle.
  let queue: Promise<void> = Promise.resolve();

  return {
    exec,
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
    deleteDatabaseFile: async () => {
      db.close();
      for (const f of [dbName, dbName + '-wal', dbName + '-shm']) {
        try { await unlink(f); } catch { /* absent is fine */ }
      }
    },
  };
};

/// TCP dial with the same option shape the core passes to its default (wsconnect).
export const nodeConnect = (opts: any) => tcpConnect(opts);
