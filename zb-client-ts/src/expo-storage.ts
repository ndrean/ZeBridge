/// The storage adapter for React Native: `expo-sqlite` behind the client's own
/// `Storage` contract (storage.ts), used by the react-native entry. The Node adapter
/// (node.ts) does the same job over better-sqlite3.
///
/// ⚠️ THE CONTRACT IS SERIALISATION. The core drives several lanes at once (one CDC
/// consumer per stream, plus the write path), so `transaction` and `exec` can be
/// called concurrently. An adapter that lets two transactions interleave gets
/// "cannot start a transaction within a transaction" and drops whole batches. The
/// promise chain below is the whole fix, exactly as in node.ts.
import * as SQLite from 'expo-sqlite';
import type { Exec, Storage, StorageFactory } from './storage.ts';

/// Binding is SEMANTICS, not a detail, which is why node.ts spells it out too:
/// expo-sqlite binds null, numbers, strings and Uint8Array. A JS boolean and
/// `undefined` are refused, and the optimistic apply carries both.
const bind = (params: any[]) =>
  params.map((p) => (p === undefined ? null : typeof p === 'boolean' ? (p ? 1 : 0) : p));

export const expoStorage: StorageFactory = (dbName: string): Storage => {
  // Opened lazily: the factory is synchronous, expo-sqlite is not.
  let dbp: Promise<SQLite.SQLiteDatabase> | null = null;
  const open = () => {
    if (!dbp) {
      dbp = SQLite.openDatabaseAsync(dbName).then(async (db) => {
        // The same pragmas the Node adapter sets, and for the same reasons: WAL for
        // concurrent readers, NORMAL because a replica re-fetches what a crash loses,
        // and foreign_keys spelled out rather than inherited from a default.
        // §10ik: FIRST, before anything can take a lock. Without it SQLite waits for a
        // held write lock FOREVER, and a lock left by a process killed mid-seed turns
        // the next launch into a silent hang — `connect()` never resolving, nothing
        // logged, indistinguishable from a network problem. Five seconds then an error
        // is always better than waiting for ever.
        await db.execAsync('PRAGMA busy_timeout = 5000');
        // 16 KB pages for a replica this open creates (see node.ts; a no-op on an
        // existing file). iOS's own page size, and a quarter of the index reads on a
        // random-key live insert (§10ja).
        await db.execAsync('PRAGMA page_size = 16384');
        await db.execAsync('PRAGMA journal_mode = WAL');
        await db.execAsync('PRAGMA synchronous = NORMAL');
        await db.execAsync('PRAGMA foreign_keys = ON');
        return db;
      });
    }
    return dbp;
  };

  const run: Exec = async (q, ...params) => {
    const db = await open();
    const text = q.trim().replace(/;\s*$/, '');
    // A PRAGMA that reads answers rows; one that sets does not.
    if (/^PRAGMA\b/i.test(text)) {
      if (/=/.test(text)) { await db.execAsync(text); return []; }
      return await db.getAllAsync(text);
    }
    const bound = bind(params);
    // `getAllAsync` on a statement that returns nothing is harmless and saves
    // guessing which arm a statement belongs to from its text.
    if (/^\s*(SELECT|WITH|EXPLAIN|VALUES)\b/i.test(text)) {
      return await db.getAllAsync(text, bound);
    }
    await db.runAsync(text, bound);
    return [];
  };

  let queue: Promise<void> = Promise.resolve();

  // ⚠️ Every statement in flight, so that closing waits for them. expo-sqlite's
  // `closeAsync` frees the connection while a statement may still be reading its rows
  // on the native queue — measured 2026-09-25: "wipe & seed again" during a 3M-row
  // `count(DISTINCT uid)` crashed the app (SIGSEGV in expo-sqlite's `columnName`, on a
  // freed handle). Closing refuses new statements, then waits for these.
  const inflight = new Set<Promise<unknown>>();
  let closing = false;
  const exec: Exec = (q, ...params) => {
    if (closing) return Promise.reject(new Error('zb-client-ts: the replica is being closed'));
    const p = run(q, ...params);
    inflight.add(p);
    const done = () => { inflight.delete(p); };
    p.then(done, done);
    return p;
  };

  return {
    exec,
    spillsTemp: true, // expo-sqlite on the device's filesystem: a sort spills to disk, so a full is staged (§10ix)
    // No `readOnly`: expo-sqlite has no second read-only handle, so the shell guards
    // `query()` by statement shape instead (core.isReadOnlySql) — the same choice the
    // browser's OPFS and the PGlite adapters make.
    transaction: (fn) => {
      const run = queue.then(async () => {
        await exec('BEGIN IMMEDIATE');
        try {
          await fn(exec);
          await exec('COMMIT');
        } catch (e) {
          await exec('ROLLBACK');
          throw e;
        }
      });
      queue = run.catch(() => {}); // a failed transaction must not wedge the lane
      return run;
    },
    deleteDatabaseFile: async () => {
      closing = true;
      try {
        await Promise.allSettled([...inflight]);
        await queue; // a transaction between two of its statements
        if (dbp) {
          const db = await dbp;
          await db.closeAsync();
          dbp = null;
        }
        try {
          await SQLite.deleteDatabaseAsync(dbName);
        } catch {
          /* absent is fine */
        }
      } finally {
        closing = false;
      }
    },
  };
};
