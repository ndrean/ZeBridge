/// Browser storage: sqlocal over OPFS. One connection (OPFS sync handles are
/// exclusive) — which is also the browser-tier write guard: the package exports
/// no write path except mutate(), and this single connection is the library's.
import { SQLocal } from 'sqlocal';
import type { Exec, StorageFactory } from './storage.ts';

export const browserStorage: StorageFactory = (dbName) => {
  const sqlocal = new SQLocal(dbName);
  const exec: Exec = (q, ...params) => (sqlocal.sql as any)(q, ...params);
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
