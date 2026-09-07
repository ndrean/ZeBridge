/// A ZeBridge client that answers SQL over stdin/stdout — the Node half of the
/// two-client migration scenario (scripts/scenarios/migrate_both.py, NOTES §10dg).
///
/// One JSON object per line in:  {"sql": "...", "params": [...]} or {"mutate": {table, op, key, values}}  → one line out:
/// {"rows": [...]} or {"error": "..."}. A line {"close": true} ends the process.
/// The client is durable and follows every schema key, exactly like a service
/// would; the scenario drives PostgreSQL and asks this replica what it holds.
///
/// Env: NATS_URL ZB_PRINCIPAL ZB_DB ZB_ENGINE (sqlite|pglite)
import { createInterface } from 'node:readline';
import { mkdirSync, readFileSync } from 'node:fs';
import { ZeBridge } from 'zb-client-ts';
import { nodeStorage, nodeConnect } from 'zb-client-ts/node';
import { makePgliteStorage } from 'zb-client-ts/pglite';

const REPO = new URL('../../', import.meta.url).pathname;
const PRINCIPAL = process.env.ZB_PRINCIPAL ?? 'omar';
const ENGINE = (process.env.ZB_ENGINE ?? 'sqlite') as 'sqlite' | 'pglite';
const DB = process.env.ZB_DB ?? `/tmp/zb-query-worker-${process.pid}.sqlite3`;
if (ENGINE === 'pglite') mkdirSync(DB, { recursive: true });

const zb = new ZeBridge({
  natsUrl: process.env.NATS_URL ?? 'nats://127.0.0.1:4222',
  principal: PRINCIPAL,
  creds: readFileSync(process.env.ZB_CREDS ?? `${REPO}scripts/native/creds/${PRINCIPAL}.creds`, 'utf8'),
  heartbeatMs: 0,
  durable: true,
  storage: ENGINE === 'pglite' ? makePgliteStorage({ persist: true, dataDir: DB }) : (_: string) => nodeStorage(DB),
  connect: nodeConnect,
});
// Lifecycle lines to stderr (the scenario greps them); CDC is one line per event.
zb.onLog((t: string, d: any, level: string) => {
  if (t === 'CDC' || t.startsWith('cdc.')) return;  // one line per event would dwarf the seed itself
  console.error(`[${t} ${level}] ${typeof d === 'string' ? d : JSON.stringify(d)}`.slice(0, 300));
});

// A refused connect (a revoked principal, §10dm) is reported, not fatal: the local
// replica can still be queried and, explicitly, wiped.
try {
  await zb.connect();
  console.log(JSON.stringify({ ready: true, tenant: zb.tenant }));
} catch (e: any) {
  console.log(JSON.stringify({ ready: false, error: String(e?.message ?? e) }));
}

const rl = createInterface({ input: process.stdin });
for await (const line of rl) {
  if (!line.trim()) continue;
  let req: any;
  try { req = JSON.parse(line); } catch { console.log(JSON.stringify({ error: 'bad json' })); continue; }
  if (req.close) break;
  if (req.wipe) { await zb.wipe(); console.log(JSON.stringify({ wiped: true })); process.exit(0); }
  try {
    if (req.mutate) {
      // {"mutate": {"table", "op", "key", "values"}} → the blessed write path
      const m = req.mutate;
      // an explicit version models a slow clock (§10do): the stamp is the caller's
      const r = await zb.mutate(m.table, m.op, m.key, m.values, m.version ? { version: m.version } : undefined);
      console.log(JSON.stringify({ rows: [r] }));
      continue;
    }
    const rows = await zb.query(req.sql, ...(req.params ?? []));
    console.log(JSON.stringify({ rows }));
  } catch (e: any) {
    console.log(JSON.stringify({ error: String(e?.message ?? e) }));
  }
}
await zb.close();
process.exit(0);
