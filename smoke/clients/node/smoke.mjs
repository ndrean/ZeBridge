// Smoke: the published @zebridge/client enrolls with an invite, seeds smoke_items, writes
// one row and waits for PostgreSQL's verdict, then prints "ready" and waits for the row
// run.sh inserts in PostgreSQL (it can only arrive by the live CDC tail). Exit 0 on both.
import { ZeBridge } from '@zebridge/client';

const NAME = 'node';
const fail = (msg) => { console.log(`✗ ${NAME}: ${msg}`); process.exit(1); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const zb = new ZeBridge({
  bridgeUrl: process.env.ZB_BRIDGE_URL, invite: process.env.ZB_INVITE,
  tables: ['smoke_items'], dbPath: `/tmp/smoke-${NAME}.db`,
});
const verdict = new Promise((resolve) => zb.onVerdict(resolve));
await zb.connect();
const until = Date.now() + 30_000;
while ((await zb.query("SELECT 1 AS one FROM smoke_items WHERE id = 'seed'")).length === 0) {
  if (Date.now() > until) fail('the seed row never arrived');
  await sleep(500);
}
const id = `from-${NAME}`;
await zb.mutate('smoke_items', 'INSERT', { id },
  { id, tenant_id: zb.tenant, note: `written by ${NAME}`, updated_at: new Date().toISOString() });
const v = await Promise.race([verdict, sleep(30_000).then(() => null)]);
if (!v) fail('no verdict in 30 s');
if (v.outcome !== 'applied') fail(`verdict ${JSON.stringify(v)}`);
console.log('ready');
const live = Date.now() + 30_000;
while ((await zb.query('SELECT 1 AS one FROM smoke_items WHERE id = ?', `live-${NAME}`)).length === 0) {
  if (Date.now() > live) fail("PostgreSQL's live row never arrived (the CDC tail)");
  await sleep(500);
}
console.log(`✓ ${NAME}: enrolled, seeded, write applied, live change received (tenant ${zb.tenant})`);
await zb.close?.();
process.exit(0);
