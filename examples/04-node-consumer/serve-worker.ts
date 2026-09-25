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
  dbPath: process.env.ZB_DB ?? `/tmp/zb-serve-${PRINCIPAL}-${Date.now()}.sqlite3`,
});

await zb.connect();

// §10hq: `ZB_ASK=<name>` turns this into an ASKER for one question, printing what came
// back — used by `scripts/scenarios/serve.py` to prove that THIS library resolves an
// answer that travelled as an object, not only that it can send one.
//
// ⚠️ An asker is a CLIENT principal, never the responder one: a responder may answer
// and may NOT ask (§10hk), so `ZB_PRINCIPAL=omar ZB_ASK=…` is the shape. It does not
// serve at all — it asks once and leaves.
if (process.env.ZB_ASK) {
  const payload = JSON.parse(process.env.ZB_ASK_PAYLOAD ?? '{}');
  const t0 = performance.now();
  const ans: any = await zb.request(`query.${TENANTS[0]}.${process.env.ZB_ASK}`, payload, 20_000);
  console.log(`ASKED ${JSON.stringify({
    rows: Array.isArray(ans?.rows) ? ans.rows.length : null,
    count: ans?.count ?? null,
    answered_by: ans?.answered_by ?? null,
    envelope_resolved: !('zb_object' in (ans ?? {})),
    // §10hu: how it travelled, from the library rather than from a guess.
    transport: ans?.zb_transport ?? null,
    ms: Math.round(performance.now() - t0),
  })}`);
  await zb.close();
  process.exit(0);
}

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
    /// §10hq: an answer deliberately too large for one message. It becomes an object in
    /// the asking tenant's bucket, and the asker sees an ordinary answer.
    big: (q: any) => {
      const n = Number(q?.rows ?? 20000);
      return { rows: Array.from({ length: n }, (_, i) => [i, `row ${i} ${'x'.repeat(40)}`]), count: n, answered_by: LABEL };
    },
  },
});

console.log(`serving as ${PRINCIPAL} (${LABEL}), tables ${TABLES.join(',')}`);

process.on('SIGTERM', () => { void zb.close().then(() => process.exit(0)); });
await new Promise(() => {});
