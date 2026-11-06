import { createHash } from 'node:crypto'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import { createCollection } from '@tanstack/db'
import { electricCollectionOptions } from '@tanstack/electric-db-collection'
import {
  createNodeSQLitePersistence,
  persistedCollectionOptions,
} from '@tanstack/node-db-sqlite-persistence'

const COUNT = Number(process.env.COMPARE_ROWS ?? 50_000)
const N = Number(process.argv[2] ?? 1)
const BASE = process.env.ELECTRIC_COMPARE_URL ?? 'http://127.0.0.1:30001/v1/shape'
const TOKEN = process.env.ELECTRIC_COMPARE_TOKEN
const STATE = process.env.ELECTRIC_COMPARE_STATE ?? path.join(os.tmpdir(), 'electric-read-compare')
const TIMEOUT_MS = Number(process.env.ELECTRIC_COMPARE_TIMEOUT_MS ?? 180_000)
if (!TOKEN) throw new Error('Set ELECTRIC_COMPARE_TOKEN (generate it with token.mjs)')
if (!Number.isInteger(N) || N < 1 || N > 100) throw new Error('Client count must be between 1 and 100')

function expectedDigest() {
  const hash = createHash('sha256')
  for (let i = 1; i <= COUNT; i++) {
    const id = `row-${String(i).padStart(5, '0')}`
    const note = createHash('md5').update(String(i)).digest('hex').repeat(6)
    hash.update(`${id}\0cmp\0${note}\n`)
  }
  return hash.digest('hex')
}

function digestRows(rows) {
  const hash = createHash('sha256')
  for (const row of rows.sort((a, b) => a.id.localeCompare(b.id))) {
    hash.update(`${row.id}\0${row.tenant_id}\0${row.note}\n`)
  }
  return hash.digest('hex')
}

function removeLocalFiles(file) {
  for (const suffix of ['', '-wal', '-shm']) {
    try { fs.unlinkSync(file + suffix) } catch (e) { if (e.code !== 'ENOENT') throw e }
  }
}

function readPersistedRowCount(database) {
  const dataTables = database.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name GLOB 'c_*'").all()
  return dataTables.reduce((total, { name }) =>
    total + database.prepare(`SELECT count(*) AS n FROM \"${name}\"`).get().n, 0)
}

async function oneClient(i) {
  const file = path.join(STATE, `tanstack-electric-${i}.sqlite3`)
  fs.mkdirSync(STATE, { recursive: true })
  removeLocalFiles(file)

  const started = performance.now()
  const database = new Database(file)
  database.pragma('journal_mode = WAL')
  const persistence = createNodeSQLitePersistence({ database })

  // No runtime item schema is supplied here. Electric sends row objects;
  // TanStack still needs a primary-key accessor for collection identity.
  const collection = createCollection(persistedCollectionOptions({
    ...electricCollectionOptions({
      id: 'storm_items',
      getKey: row => row.id,
      shapeOptions: {
        url: BASE,
        headers: { authorization: `Bearer ${TOKEN}` },
        params: {
          table: 'public.storm_items',
          where: 'tenant_id = $1',
          params: ['cmp'],
          replica: 'full',
        },
      },
    }),
    persistence,
    schemaVersion: 1,
  }))

  let timeout
  try {
    await Promise.race([
      collection.preload(),
      new Promise((_, reject) => {
        timeout = setTimeout(() => reject(new Error(`client ${i}: timed out after ${TIMEOUT_MS} ms`)), TIMEOUT_MS)
      }),
    ])
    const rows = Array.from(collection.state.values())
    const sha256 = digestRows(rows)
    if (rows.length !== COUNT || sha256 !== expectedDigest()) {
      throw new Error(`client ${i}: local verification mismatch, rows=${rows.length}, sha256=${sha256}`)
    }
    const deadline = started + TIMEOUT_MS
    let persistedRows = readPersistedRowCount(database)
    while (persistedRows < COUNT && performance.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 10))
      persistedRows = readPersistedRowCount(database)
    }
    if (persistedRows !== COUNT) {
      throw new Error(`client ${i}: SQLite contains ${persistedRows} persisted rows, expected ${COUNT}`)
    }
    const seconds = (performance.now() - started) / 1000
    return { client: i, seconds, rows: rows.length, persisted_rows: persistedRows, sha256, file }
  } finally {
    clearTimeout(timeout)
    await collection.cleanup()
    database.close()
  }
}

const allStarted = performance.now()
const results = await Promise.all(Array.from({ length: N }, (_, i) => oneClient(i)))
const wallSeconds = (performance.now() - allStarted) / 1000
const times = results.map(r => r.seconds).sort((a, b) => a - b)
const mid = Math.floor(times.length / 2)
const median = times.length % 2 ? times[mid] : (times[mid - 1] + times[mid]) / 2
const slowest = times.at(-1)

console.log(JSON.stringify({
  system: 'electric-tanstack-sqlite', phase: 'cold-seed', clients: N,
  rows_per_client: COUNT, median_seconds: Number(median.toFixed(3)),
  slowest_seconds: Number(slowest.toFixed(3)), wall_seconds: Number(wallSeconds.toFixed(3)),
  verified: results.every(r => r.sha256 === results[0].sha256),
  row_sha256: results[0].sha256,
  clients_result: results.map(({ file, ...result }) => result),
}, null, 2))
