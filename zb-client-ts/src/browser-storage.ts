/// Browser storage: sqlocal over OPFS. One connection (OPFS sync handles are
/// exclusive) — which is also the browser-tier write guard: the package exports
/// no write path except mutate(), and this single connection is the library's.
import { SQLocal } from 'sqlocal';
import type { Exec, StorageFactory } from './storage.ts';

export const browserStorage: StorageFactory = (dbName) => {
  const sqlocal = new SQLocal(dbName);
  const exec: Exec = (q, ...params) => (sqlocal.sql as any)(q, ...params);
  // Semantics, not performance — see the contract in storage.ts. sqlocal sets no
  // pragma of its own, so without this the browser silently ignores every foreign
  // key while Node enforces them. Fire-and-forget: the first real statement is
  // queued behind it on the same connection.
  void exec(`PRAGMA foreign_keys = ON;`);
  return {
    exec,
    transaction: (fn) =>
      sqlocal.transaction(async (tx) => {
        const txExec: Exec = (q, ...p) => (tx.sql as any)(q, ...p);
        await fn(txExec);
      }) as Promise<void>,
    deleteDatabaseFile: () => sqlocal.deleteDatabaseFile(),
  };
};
