// Direct SELECT -> prepared INSERT baseline for Node server/worker applications.
import { readFileSync, existsSync } from 'node:fs';
import { performance } from 'node:perf_hooks';
import pg from 'pg';
import Cursor from 'pg-cursor';
import Database from 'better-sqlite3';
import { utcTimestamp } from './values.mjs';

const [configPath, dbPath] = process.argv.slice(2);
const config = JSON.parse(readFileSync(configPath, 'utf8'));
if (existsSync(dbPath)) throw new Error('Refusing to reuse a local database');
const packages = Object.fromEntries(['pg', 'pg-cursor', 'better-sqlite3'].map(name =>
  [name, JSON.parse(readFileSync(new URL(`./node_modules/${name}/package.json`, import.meta.url), 'utf8')).version]));

// Match the Python importer: type conversion happens within the timed fetch.
const types = {
  getTypeParser(oid, format) {
    return oid === 1184 && format !== 'binary' ? utcTimestamp : pg.types.getTypeParser(oid, format);
  },
};

const started = performance.now();
const client = new pg.Client({ connectionString: config.pg_url, connectionTimeoutMillis: 10000 });
let db;
let result;
try {
  await client.connect();
  const connected = performance.now();
  db = new Database(dbPath);
  for (const pragma of ['page_size = 16384', 'journal_mode = WAL', 'synchronous = NORMAL',
    'wal_autocheckpoint = 10000', 'foreign_keys = ON', 'cache_size = -131072']) db.pragma(pragma);
  for (const ddl of config.ddl) db.exec(ddl);
  const insert = db.prepare('INSERT INTO seed_items VALUES (?, ?, ?, ?, ?, ?)');
  const insertMany = db.transaction(rows => {
    for (const row of rows) insert.run(row);
  });
  const created = performance.now();
  await client.query('BEGIN READ ONLY');
  await client.query("SET LOCAL TIME ZONE 'UTC'");
  await client.query("SELECT set_config('zb.tenant', $1, true)", ['seedbench']);
  const cursor = client.query(new Cursor(
    'SELECT id, tenant_id, note, quantity, updated_at, deleted_at FROM public.seed_items '
      + 'WHERE tenant_id = $1 ORDER BY id', ['seedbench'], { rowMode: 'array', types }));
  try {
    while (true) {
      const rows = await cursor.read(config.batch_rows);
      if (rows.length === 0) break;
      insertMany(rows);
    }
  } finally {
    await cursor.close();
  }
  const loaded = performance.now();
  const count = db.prepare('SELECT count(*) AS n FROM seed_items').get().n;
  const ready = performance.now();
  if (count !== config.expected.rows) throw new Error(`Expected ${config.expected.rows} rows, got ${count}`);
  result = {
    seconds: (ready - started) / 1000,
    phases_seconds: {
      connect: (connected - started) / 1000,
      create: (created - connected) / 1000,
      select_and_insert: (loaded - created) / 1000,
      ready_query: (ready - loaded) / 1000,
    },
    engine_sqlite_version: db.prepare('SELECT sqlite_version() AS v').get().v,
    node_version: process.version,
    packages,
  };
} finally {
  // Like the other workers, close and verification are outside the timer.
  db?.close();
  await client.end();
}
console.log(JSON.stringify(result));
