// One synchronization owner per file-backed replica: same process, another process, and a crash.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import Database from 'better-sqlite3';
import { nodeStorage } from './node.ts';

const dir = mkdtempSync(join(tmpdir(), 'zb-lock-'));
test.after(() => rmSync(dir, { recursive: true, force: true }));
const refused = (f: () => unknown) => assert.throws(f, { code: 'ReplicaAlreadyInUse' });

test('a second owner in the same process is refused, and takes over after release', () => {
  const db = join(dir, 'same.sqlite3');
  const first = nodeStorage(db);
  refused(() => nodeStorage(db));
  first.release!();
  const second = nodeStorage(db);   // free again
  refused(() => first.reacquire!());   // close() then connect() on the first client is refused while another owns it
  second.release!();
  first.reacquire!();
  first.release!();
});

test('a reader is not an owner: the replica stays readable while it is owned', async () => {
  const db = join(dir, 'read.sqlite3');
  const owner = nodeStorage(db);
  await owner.exec('CREATE TABLE t (a INTEGER)');
  await owner.exec('INSERT INTO t VALUES (7)');
  const reader = new Database(db, { readonly: true, fileMustExist: true });
  assert.deepEqual(reader.prepare('SELECT a FROM t').all(), [{ a: 7 }]);
  reader.close();
  owner.release!();
});

// A child process that owns the replica until it is killed. Resolves once the lock is held.
const child = (db: string) => new Promise<ReturnType<typeof spawn>>((resolve, reject) => {
  const code = `import('./src/node.ts').then(m => { m.nodeStorage(${JSON.stringify(db)}); console.log('held'); setInterval(() => {}, 1000); })`;
  const p = spawn(process.execPath, ['--experimental-strip-types', '--no-warnings', '-e', code], { stdio: ['ignore', 'pipe', 'inherit'] });
  p.stdout!.once('data', () => resolve(p));
  p.once('error', reject);
  p.once('exit', (c) => reject(new Error(`child exited ${c} before holding the lock`)));
});

test('another process is refused while it owns the replica, and a crash releases the lock', async () => {
  const db = join(dir, 'proc.sqlite3');
  const p = await child(db);
  p.removeAllListeners('exit');
  refused(() => nodeStorage(db));
  const gone = new Promise((r) => p.once('exit', r));
  p.kill('SIGKILL');   // no release(), no close(): the OS drops the lock with the process
  await gone;
  nodeStorage(db).release!();
});
