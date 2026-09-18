/// §10gn: the TS client as a pure follower, for the firehose harness
/// (`firehose_tls.py --ts-client-at`). It connects, seeds what the chain gives it and
/// follows CDC; the harness reads the replica's SQLite file directly and compares it with
/// PostgreSQL. No stdin protocol — query-worker.ts is the one that answers SQL.
///
/// Env: NATS_URL, ZB_DB, ZB_TABLES (comma list), ZB_PRINCIPAL, ZB_CREDS (optional: the
/// scratch nats-server of a benchmark has no auth, and the creds file does not exist).
import { existsSync, readFileSync } from 'node:fs';
import { ZeBridge } from 'zb-client-ts';
import { nodeStorage, nodeConnect } from 'zb-client-ts/node';

const DB = process.env.ZB_DB ?? `/tmp/zb-follow-${process.pid}.sqlite3`;
const credsPath = process.env.ZB_CREDS;
const creds = credsPath && existsSync(credsPath) ? readFileSync(credsPath, 'utf8') : undefined;

const zb = new ZeBridge({
  natsUrl: process.env.NATS_URL ?? 'nats://127.0.0.1:4222',
  principal: process.env.ZB_PRINCIPAL ?? 'follower',
  bulkCdc: process.env.ZB_BULK_CDC !== '0',
  bulkStatement: process.env.ZB_BULK_STMT === 'json_each' ? 'json_each' : 'rows',
  cdcBatchEvents: process.env.ZB_BATCH_EVENTS ? Number(process.env.ZB_BATCH_EVENTS) : undefined,
  creds,
  heartbeatMs: 0,
  durable: true,
  tables: process.env.ZB_TABLES ? process.env.ZB_TABLES.split(',').map((t) => t.trim()).filter(Boolean) : undefined,
  storage: (_: string) => nodeStorage(DB),
  connect: nodeConnect,
});
zb.onLog((t: string, d: any, level: string) => {
  if (t === 'CDC' || t.startsWith('cdc.')) return; // one line per event would dwarf the run
  console.error(`[${t} ${level}] ${typeof d === 'string' ? d : JSON.stringify(d)}`.slice(0, 300));
});

try {
  await zb.connect();
  console.log(JSON.stringify({ ready: true, tenant: zb.tenant }));
} catch (e: any) {
  console.log(JSON.stringify({ ready: false, error: String(e?.message ?? e) }));
  process.exit(1);
}
// Follow until the harness stops us.
process.on('SIGTERM', () => process.exit(0));
setInterval(() => {}, 1 << 30);
