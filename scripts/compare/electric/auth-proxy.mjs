import { createHmac, timingSafeEqual } from 'node:crypto'
import { createServer } from 'node:http'

const PORT = 3001
const ELECTRIC_URL = process.env.ELECTRIC_URL ?? 'http://electric:3000/v1/shape'
const JWT_SECRET = process.env.ELECTRIC_COMPARE_JWT_SECRET
const TENANT = process.env.ELECTRIC_COMPARE_TENANT ?? 'cmp'
const SUBJECT = process.env.ELECTRIC_COMPARE_SUBJECT ?? 'compare-reader'
const ISSUER = process.env.ELECTRIC_COMPARE_ISSUER ?? 'electric-read-compare'

if (!JWT_SECRET || Buffer.byteLength(JWT_SECRET) < 32) {
  throw new Error('ELECTRIC_COMPARE_JWT_SECRET must be at least 32 bytes')
}

function decodePart(part) {
  return JSON.parse(Buffer.from(part, 'base64url').toString('utf8'))
}

function authorize(req) {
  const match = /^Bearer ([A-Za-z0-9._-]+)$/.exec(req.headers.authorization ?? '')
  if (!match) return false

  const parts = match[1].split('.')
  if (parts.length !== 3) return false
  let header, claims
  try {
    header = decodePart(parts[0])
    claims = decodePart(parts[1])
  } catch {
    return false
  }
  if (header.alg !== 'HS256' || header.typ !== 'JWT') return false

  const expected = createHmac('sha256', JWT_SECRET)
    .update(`${parts[0]}.${parts[1]}`)
    .digest()
  const actual = Buffer.from(parts[2], 'base64url')
  if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) return false

  const now = Math.floor(Date.now() / 1000)
  const audience = Array.isArray(claims.aud) ? claims.aud : [claims.aud]
  return claims.sub === SUBJECT && claims.iss === ISSUER &&
    audience.includes(ISSUER) && Number.isFinite(claims.exp) && claims.exp > now &&
    (!claims.nbf || claims.nbf <= now)
}

// Pass only Electric's stream-resumption controls. The client cannot choose a
// table, filter, columns, or subset snapshot through this proxy.
const protocolParams = new Set([
  'offset', 'handle', 'live', 'live_sse', 'log', 'replica', 'cursor', 'source_id',
])
const requestHeaders = ['accept', 'if-none-match', 'last-event-id']
const responseHeadersToSkip = new Set([
  'connection', 'content-encoding', 'content-length', 'keep-alive',
  'transfer-encoding', 'upgrade',
])

const server = createServer(async (req, res) => {
  if (req.method === 'GET' && req.url === '/healthz') {
    res.writeHead(200, { 'content-type': 'text/plain' }).end('ok')
    return
  }
  if (req.url?.split('?')[0] !== '/v1/shape') {
    res.writeHead(404).end('not found')
    return
  }
  if (req.method !== 'GET') {
    res.writeHead(405, { allow: 'GET' }).end('GET only')
    return
  }
  if (!authorize(req)) {
    res.writeHead(401, { 'www-authenticate': 'Bearer' }).end('unauthorized')
    return
  }

  const incoming = new URL(req.url, 'http://proxy.invalid')
  const upstream = new URL(ELECTRIC_URL)
  for (const key of protocolParams) {
    for (const value of incoming.searchParams.getAll(key)) upstream.searchParams.append(key, value)
  }
  upstream.searchParams.set('table', 'public.storm_items')
  upstream.searchParams.set('where', 'tenant_id = $1')
  upstream.searchParams.set('params[1]', TENANT)

  const headers = new Headers()
  for (const name of requestHeaders) {
    const value = req.headers[name]
    if (value) headers.set(name, value)
  }

  const abort = new AbortController()
  res.on('close', () => {
    if (!res.writableEnded) abort.abort()
  })

  try {
    const upstreamResponse = await fetch(upstream, { headers, signal: abort.signal })
    const outgoingHeaders = {}
    for (const [name, value] of upstreamResponse.headers) {
      if (!responseHeadersToSkip.has(name.toLowerCase())) outgoingHeaders[name] = value
    }
    res.writeHead(upstreamResponse.status, outgoingHeaders)
    if (!upstreamResponse.body) return res.end()
    for await (const chunk of upstreamResponse.body) {
      if (!res.write(chunk)) await new Promise(resolve => res.once('drain', resolve))
    }
    res.end()
  } catch (error) {
    if (abort.signal.aborted) return
    if (!res.headersSent) res.writeHead(502, { 'content-type': 'text/plain' })
    res.end('Electric upstream unavailable')
  }
})

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Electric comparison auth proxy listening on ${PORT}; tenant=${TENANT}`)
})
