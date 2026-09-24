/// Browser storage: sqlocal over OPFS. One connection (OPFS sync handles are
/// exclusive) — which is also the browser-tier write guard: the package exports
/// no write path except mutate(), and this single connection is the library's.
import { SQLocal } from 'sqlocal';
import type { Exec, StorageFactory } from './storage.ts';

/// sqlite-wasm's temp-file shape: `randomFilename()` in its OPFS VFS, 16 characters
/// of [a-zA-Z0-9], no extension, in the origin's root. A database has a `.sqlite3`
/// name; nothing else of ours looks like this.
const TEMP_SHAPE = /^[A-Za-z0-9]{16}$/;
const opfsTempCandidates = async (root: FileSystemDirectoryHandle): Promise<[string, FileSystemFileHandle][]> => {
  const out: [string, FileSystemFileHandle][] = [];
  for await (const [name, h] of (root as any).entries() as AsyncIterable<[string, FileSystemHandle]>) {
    if (h.kind === 'file' && TEMP_SHAPE.test(name)) out.push([name, h as FileSystemFileHandle]);
  }
  return out;
};
const tempFiles = async (): Promise<Set<string>> => {
  const root = await navigator.storage.getDirectory();
  return new Set((await opfsTempCandidates(root)).map(([name]) => name));
};
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
/// Remove the temp-shaped files not in `before` that are closed — an open one throws
/// (NoModificationAllowedError) and is left alone after a few tries. Bytes reclaimed.
const sweepOrphans = async (before: Set<string>, sized?: Map<string, number>): Promise<number> => {
  const root = await navigator.storage.getDirectory();
  const found = sized ?? new Map<string, number>();
  for (const [name, h] of await opfsTempCandidates(root)) {
    if (before.has(name)) continue;
    if (!found.has(name)) found.set(name, (await h.getFile()).size);
    // Backed off over ~6 s: on a loaded machine the VFS releases a closed file's
    // handle later than half a second (measured 2026-09-24: 1.3 GB of temp files still
    // in the origin when the facts were read, with the 5 × 100 ms of the first cut).
    for (let attempt = 0; attempt < 12; attempt++) {
      try { await root.removeEntry(name); break; } catch { await sleep(100 * (attempt + 1)); }
    }
  }
  // Reclaimed = what is gone now, whoever removed it — the VFS's delete-on-close
  // often wins once the temp database is closed, and that counts.
  const left = new Set((await opfsTempCandidates(root)).map(([name]) => name));
  let bytes = 0;
  for (const [name, size] of found) if (!left.has(name)) bytes += size;
  return bytes;
};

export const browserStorage: StorageFactory = (dbName) => {
  const sqlocal = new SQLocal(dbName);
  const exec: Exec = (q, ...params) => (sqlocal.sql as any)(q, ...params);
  // Semantics, not performance — see the contract in storage.ts. sqlocal sets no
  // pragma of its own, so without this the browser silently ignores every foreign
  // key while Node enforces them. Fire-and-forget: the first real statement is
  // queued behind it on the same connection.
  void exec(`PRAGMA foreign_keys = ON;`);
  // §10ix, measured 2026-09-24: the sqlite-wasm build is TEMP_STORE=2 (temp in memory
  // unless told otherwise), and with `temp_store = FILE` its OPFS VFS puts TEMP tables
  // and the sorter's spill into OPFS files (a 300 MB TEMP table became a 319 MB file
  // in the origin's root). That is what lets the seed take the staged path here —
  // TEMP heap append, one INSERT … SELECT … ORDER BY pk — instead of a sorted window
  // at a time into a scattered b-tree, which crawled past 900k rows in Chrome.
  void exec(`PRAGMA temp_store = FILE;`);
  // Orphans of an earlier session (a reload mid-seed leaves 1.3 GB behind) — swept
  // once at open, before this connection has any temp file of its own to confuse
  // with them. Best effort, and an open file refuses removal anyway.
  void sweepOrphans(new Set()).catch(() => undefined);
  return {
    exec,
    spillsTemp: true,
    // ⚠️ sqlocal rolls back on an error in the body and rethrows — the ROLLBACK's error,
    // if it fails. It does when SQLite already rolled the transaction back itself
    // (NOMEM, FULL, IOERR: the auto-rollback class), so the one message that says
    // what happened is replaced by "cannot rollback - no transaction is active"
    // (seen 2026-09-24, 06-large-table at 350k rows). The body's error is kept here
    // and thrown in preference.
    transaction: async (fn) => {
      let inner: unknown;
      try {
        await sqlocal.transaction(async (tx) => {
          const txExec: Exec = (q, ...p) => (tx.sql as any)(q, ...p);
          try { await fn(txExec); } catch (e) { inner = e; throw e; }
        });
      } catch (e) {
        if (inner !== undefined && inner !== e) throw inner;
        throw e;
      }
    },
    deleteDatabaseFile: () => sqlocal.deleteDatabaseFile(),
    tempFiles,
    // The TEMP database stays open after the stage is dropped, and the VFS's own
    // delete-on-close races Chrome's release of the sync handle (it lost that race
    // for a 665 MB file, kept it the next time). Changing `temp_store` closes the
    // temp b-tree outright ("all existing temporary tables … are immediately
    // deleted" — nothing of ours lives there by then), and the removal retries the
    // same race a few times.
    sweepTemp: async (before) => {
      const root = await navigator.storage.getDirectory();
      const sized = new Map<string, number>();
      for (const [name, h] of await opfsTempCandidates(root)) if (!before.has(name)) sized.set(name, (await h.getFile()).size);
      await exec(`PRAGMA temp_store = MEMORY;`);
      await exec(`PRAGMA temp_store = FILE;`);
      return sweepOrphans(before, sized);
    },
  };
};
