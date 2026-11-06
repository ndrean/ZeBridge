import { createHash } from 'node:crypto'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import Database from 'better-sqlite3'
import { ShapeStream } from '@electric-sql/client'

const COUNT = Number(process.env.COMPARE_ROWS ?? 50_000)
const N = Number(process.argv[2] ?? 1)
const BASE = process.env.ELECTRIC_COMPARE_URL ?? 'http://127.0.0.1:30001/v1/shape'
const TOKEN = process.env.ELECTRIC_COMPARE_TOKEN
const STATE = process.env.ELECTRIC_COMPARE_STATE ?? path.join(os.tmpdir(), 'electric-read-compare')
const TIMEOUT_MS = Number(process.env.ELECTRIC_COMPARE_TIMEOUT_MS ?? 180_000)
if (!TOKEN) throw new Error('Set ELECTRIC_COMPARE_TOKEN (generate it with token.mjs)')
if (!Number.isInteger(N) || N < 1 || N > 100) throw new Error('Client count must be between 1 and 100')

fs.mkdirSync(STATE, { recursive: true })

function expectedDigest() {
  const hash = createHash('sha256')
  for (let i = 1; i <= COUNT; i++) {
    const id = `row-${String(i).padStart(5, '0')}`
    const note = createHash('md5').update(String(i)).digest('hex').repeat(6)
    hash.update(`${id}\0cmp\0${note}\n`)
  }
  return hash.digest('hex')
}
const EXPECTED_DIGEST = expectedDigest()

function localDigest(db) {
  const hash = createHash('sha256')
  const rows = db.prepare('SELECT id, tenant_id, note FROM storm_items ORDER BY id').iterate()
  let n = 0
  for (const row of rows) {
    hash.update(`${row.id}\0${row.tenant_id}\0${row.note}\n`)
    n++
  }
  return { n, sha256: hash.digest('hex') }
}

function removeLocalFiles(file) {
  for (const suffix of ['', '-wal', '-shm']) {
    try { fs.unlinkSync(file + suffix) } catch (e) { if (e.code !== 'ENOENT') throw e }
  }
}

function applyMessage(db, statements, message) {
  const headers = message.headers ?? {}
  const operation = headers.operation ?? headers.action
  if (headers.control) return

  const value = message.value ?? {}
  let id = value.id
  if (id === undefined && message.key !== undefined) {
    if (typeof message.key === 'object' && message.key !== null) id = message.key.id
    else {
      try {
        const decoded = JSON.parse(message.key)
        id = typeof decoded === 'object' && decoded !== null ? decoded.id : decoded
      } catch { id = message.key }
    }
  }
  if (id === undefined || id === null) throw new Error(`Missing primary key in ${JSON.stringify(message)}`)

  if (operation === 'delete') {
    statements.delete.run(String(id))
    return
  }
  if (operation === 'insert') {
    statements.insert.run(value)
    return
  }
  if (operation === 'update') {
    const allowed = new Set(['tenant_id', 'note', 'updated_at', 'deleted_at'])
    const fields = Object.keys(value).filter(k => allowed.has(k))
    if (fields.length === 0) return
    const assignments = fields.map(k => `"${k}" = @${k}`).join(', ')
    const key = fields.join(',')
    let statement = statements.updates.get(key)
    if (!statement) {
      statement = db.prepare(`UPDATE storm_items SET ${assignments} WHERE id = @id`)
      statements.updates.set(key, statement)
    }
    const result = statement.run({ ...value, id: String(id) })
    if (result.changes !== 1) throw new Error(`Update for missing local row ${id}`)
    return
  }
  throw new Error(`Unsupported Electric operation ${String(operation)}`)
}

async function oneClient(i) {
  const file = path.join(STATE, `electric-${i}.sqlite3`)
  removeLocalFiles(file)
  const started = performance.now()
  const db = new Database(file)
  db.pragma('journal_mode = WAL')
  db.exec(`CREATE TABLE storm_items (
    id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, note TEXT,
    updated_at TEXT NOT NULL, deleted_at TEXT
  )`)
  const statements = {
    insert: db.prepare(`INSERT INTO storm_items (id, tenant_id, note, updated_at, deleted_at)
      VALUES (@id, @tenant_id, @note, @updated_at, @deleted_at)
      ON CONFLICT(id) DO UPDATE SET tenant_id=excluded.tenant_id, note=excluded.note,
        updated_at=excluded.updated_at, deleted_at=excluded.deleted_at`),
    delete: db.prepare('DELETE FROM storm_items WHERE id = ?'),
    updates: new Map(),
  }
  const applyBatch = db.transaction(messages => {
    for (const message of messages) applyMessage(db, statements, message)
  })

  let unsubscribe
  let timeout
  let done = false
  const result = new Promise((resolve, reject) => {
    timeout = setTimeout(() => reject(new Error(`client ${i}: timed out after ${TIMEOUT_MS} ms`)), TIMEOUT_MS)
    const stream = new ShapeStream({
      url: BASE,
      headers: { authorization: `Bearer ${TOKEN}` },
      params: {
        table: 'public.storm_items',
        where: 'tenant_id = $1',
        params: ['cmp'],
        replica: 'full',
      },
      // This runner measures a one-shot initial snapshot and unsubscribes after
      // all rows arrive; it does not need an open live SSE connection.
      liveSse: false,
      ...(process.env.ELECTRIC_COMPARE_TRACE ? {
        fetchClient: async (url, init) => {
          console.error(`client ${i}: GET ${url}`)
          const response = await fetch(url, init)
          console.error(`client ${i}: HTTP ${response.status} ${response.headers.get('content-type')} offset=${response.headers.get('electric-offset')}`)
          return response
        },
      } : {}),
      onError: error => {
        if (!done) reject(error)
      },
    })
    unsubscribe = stream.subscribe(messages => {
      if (done) return
      try {
        if (process.env.ELECTRIC_COMPARE_TRACE) console.error(`client ${i}: received ${messages.length} messages`)
        applyBatch(messages)
        if (messages.some(m => m.headers?.control === 'up-to-date')) {
          const count = db.prepare('SELECT count(*) AS n FROM storm_items').get().n
          if (process.env.ELECTRIC_COMPARE_TRACE) console.error(`client ${i}: up-to-date, local rows=${count}`)
          // Electric can send an intermediate up-to-date while a shape is paged.
          // Only the expected materialized row count ends the initial seed timer.
          if (count === COUNT) {
            done = true
            clearTimeout(timeout)
            unsubscribe()
            resolve({ client: i, seconds: (performance.now() - started) / 1000 })
          }
        }
      } catch (error) {
        done = true
        clearTimeout(timeout)
        unsubscribe()
        reject(error)
      }
    }, error => {
      if (!done) reject(error)
    })
  })

  try {
    const timed = await result
    return { ...timed, file }
  } finally {
    done = true
    clearTimeout(timeout)
    if (unsubscribe) unsubscribe()
    db.close()
  }
}

if (fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  const allStarted = performance.now()
  const timedResults = await Promise.all(Array.from({ length: N }, (_, i) => oneClient(i)))
  const seedWallSeconds = (performance.now() - allStarted) / 1000
  const results = timedResults.map(result => {
    const db = new Database(result.file, { readonly: true })
    try {
      const actual = localDigest(db)
      if (actual.n !== COUNT || actual.sha256 !== EXPECTED_DIGEST) {
        throw new Error(`client ${result.client}: local verification mismatch: ${JSON.stringify(actual)}`)
      }
      return { client: result.client, seconds: result.seconds, rows: actual.n, sha256: actual.sha256 }
    } finally { db.close() }
  })
  const times = results.map(r => r.seconds).sort((a, b) => a - b)
  const mid = Math.floor(times.length / 2)
  const median = times.length % 2 ? times[mid] : (times[mid - 1] + times[mid]) / 2
  const slowest = times.at(-1)
  console.log(JSON.stringify({
    system: 'electric', phase: 'cold-seed', clients: N, rows_per_client: COUNT,
    median_seconds: Number(median.toFixed(3)), slowest_seconds: Number(slowest.toFixed(3)),
    wall_seconds: Number(seedWallSeconds.toFixed(3)),
    verified: results.every(r => r.sha256 === results[0].sha256),
    row_sha256: results[0].sha256,
    clients_result: results,
  }, null, 2))
}
