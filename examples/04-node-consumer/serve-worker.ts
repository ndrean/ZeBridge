/// A responder in zb-client-ts (§10hp): a client that answers questions about its own
/// replica. `zb.serve` subscribes `query.<tenant>.<name>` in a queue group on the
/// client's own connection; the handlers are this file's, everything else is the
/// library's. The libzb twin is `examples/08-map/poi_service.py`.
///
///   ZB_PRINCIPAL=pois ZB_TABLES=memo ZB_LABEL=ts pnpm serve
///
/// Needs a RESPONDER credential: a client principal may ASK (`query.<tenant>.<name>`)
/// but may not subscribe there, which is the point of the role (§10hk).
import { readFileSync } from 'node:fs';
import { ZeBridge } from 'zb-client-ts';
import { nodeStorage, nodeConnect } from 'zb-client-ts/node';

const REPO = new URL('../../', import.meta.url).pathname;
const PRINCIPAL = process.env.ZB_PRINCIPAL ?? 'pois';
const LABEL = process.env.ZB_LABEL ?? 'ts';
const TABLES = (process.env.ZB_TABLES ?? 'memo').split(',').map((t) => t.trim()).filter(Boolean);
const TENANTS = (process.env.ZB_TENANTS ?? '_default').split(',').map((t) => t.trim()).filter(Boolean);

const zb = new ZeBridge({
  natsUrl: process.env.NATS_URL ?? 'nats://127.0.0.1:4222',
  principal: PRINCIPAL,
  creds: readFileSync(process.env.ZB_CREDS ?? `${REPO}scripts/native/creds/${PRINCIPAL}.creds`, 'utf8'),
  tables: TABLES,
  heartbeatMs: 0,
  storage: (_: string) => nodeStorage(process.env.ZB_DB ?? `/tmp/zb-serve-${PRINCIPAL}-${Date.now()}.sqlite3`),
  connect: nodeConnect,
});

await zb.connect();

await zb.serve({
  tenants: TENANTS,
  queue: process.env.ZB_QUEUE ?? 'demo',
  handlers: {
    /// How many live rows a table holds here, answered from THIS replica.
    count: async (q: any) => {
      const table = String(q?.table ?? TABLES[0]);
      if (!TABLES.includes(table)) return { error: `not replicated here: ${table}`, tables: TABLES };
      const t0 = performance.now();
      const n = (await zb.query(`SELECT count(*) AS n FROM ${table}`))[0]?.n ?? 0;
      return { table, count: Number(n), ms: Math.round((performance.now() - t0) * 10) / 10, answered_by: LABEL };
    },
    /// The payload back, to measure the round trip and prove the envelope.
    echo: (q: any) => ({ echo: q, answered_by: LABEL }),
    /// Deliberately throws: the asker must get an error, not a timeout.
    boom: () => { throw new Error('boom, on purpose'); },
  },
});

console.log(`serving as ${PRINCIPAL} (${LABEL}), tables ${TABLES.join(',')}`);
process.on('SIGTERM', () => { void zb.close().then(() => process.exit(0)); });
await new Promise(() => {});
