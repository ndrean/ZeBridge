import { createHmac } from 'node:crypto'

const secret = process.env.ELECTRIC_COMPARE_JWT_SECRET
if (!secret || Buffer.byteLength(secret) < 32) {
  throw new Error('Set ELECTRIC_COMPARE_JWT_SECRET to at least 32 bytes')
}

const issuer = 'electric-read-compare'
const now = Math.floor(Date.now() / 1000)
const encode = value => Buffer.from(JSON.stringify(value)).toString('base64url')
const head = encode({ alg: 'HS256', typ: 'JWT' })
const payload = encode({
  sub: 'compare-reader',
  iss: issuer,
  aud: issuer,
  iat: now,
  exp: now + 3600,
})
const body = `${head}.${payload}`
const signature = createHmac('sha256', secret).update(body).digest('base64url')
process.stdout.write(`${body}.${signature}\n`)
