/// One PROCESS in the swarm, hosting one or more ZeBridge clients
/// (scripts/scenarios/swarm.py, NOTES §10cp). Each client keeps its own NATS
/// connection, consumers, outbox and replica file — grouping amortizes only the
/// ~70 MB V8 baseline, so 100 logical clients fit a 16 GB machine.
///
/// The 5 s CRUD cycle, one mutation tick per second, orders folded onto the
/// first three ticks so test_types grows +2 live rows per cycle per client:
///
///   t+0  test_types INSERT A     + orders INSERT O
///   t+1  test_types INSERT B     + orders UPDATE O
///   t+2  test_types INSERT C     + orders DELETE O   (physical — no tombstone)
///   t+3  test_types UPDATE C
///   t+4  test_types DELETE A     (soft — tombstone)
///
/// Env: ZB_CLIENTS_SPEC — JSON [{wid, principal, tenant, db, engine, report}, …]
///      ZB_DURATION_S ZB_SETTLE_S ZB_USER_IDS(csv) NATS_URL
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';
import { ZeBridge } from 'zb-client-ts';
import { nodeStorage, nodeConnect } from 'zb-client-ts/node';
import { makePgliteStorage } from 'zb-client-ts/pglite';

const REPO = new URL('../../', import.meta.url).pathname;
const SPEC: { wid: string; principal: string; tenant: string; db: string; engine: string; report: string }[] =
  JSON.parse(process.env.ZB_CLIENTS_SPEC ?? '[]');
const DURATION_S = Number(process.env.ZB_DURATION_S ?? 3600);
const SETTLE_S = Number(process.env.ZB_SETTLE_S ?? 240);
const USER_IDS = (process.env.ZB_USER_IDS ?? '').split(',').filter(Boolean).map(Number);
const SUPPLIERS = (process.env.ZB_SUPPLIERS ?? '').split(',').filter((x) => x.includes('|'))
  .map((x) => x.split('|', 2) as [string, string]);
const GRAMMAR = JSON.parse(readFileSync(`${REPO}src/grammar.json`, 'utf8'));
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function runClient(c: (typeof SPEC)[number], stagger: number): Promise<void> {
  const counters = { sent: 0, sendErrors: 0, rejections: 0, byError: {} as Record<string, number> };
  if (c.engine === 'pglite') mkdirSync(c.db, { recursive: true });
  const zb = new ZeBridge({
    natsUrl: process.env.NATS_URL ?? 'nats://127.0.0.1:4222',
    principal: c.principal,
    creds: readFileSync(`${REPO}scripts/native/creds/${c.principal}.creds`, 'utf8'),
    grammar: GRAMMAR,
    storage: c.engine === 'pglite'
      ? makePgliteStorage({ persist: true, dataDir: c.db })
      : (_: string) => nodeStorage(c.db),
    connect: nodeConnect,
  });
  zb.onLog((t: string, d: any, level: string) => {
    const s = typeof d === 'string' ? d : JSON.stringify(d);
    if (/"status":"rejected"|"status":"failed"/.test(s)) {
      counters.rejections += 1;
      if (counters.rejections <= 3) console.error(`[${c.wid}] ${t}: ${s}`.slice(0, 200));
    } else if (level === 'error' || t === 'SYS') {
      // SYS carries the load-bearing lifecycle lines — seeded gen, consumer floor,
      // re-sync — without which a divergence postmortem is pure divination (§10cp).
      console.error(`[${c.wid}] ${t}: ${s}`.slice(0, 220));
    }
  });

  await zb.connect();
  const seedDeadline = Date.now() + 180_000;
  while (Date.now() < seedDeadline) {
    const t = zb.tableNames();
    if (t.includes('test_types') && t.includes('orders')) break;
    await sleep(1000);
  }
  if (!zb.tableNames().includes('test_types')) {
    writeFileSync(c.report, JSON.stringify({ worker: c.wid, fatal: 'test_types never synced' }));
    await zb.close();
    return;
  }

  const send = async (table: string, op: 'INSERT' | 'UPDATE' | 'DELETE',
                      key: Record<string, unknown>, values?: Record<string, unknown>) => {
    try {
      await zb.mutate(table, op, key, values);
      counters.sent += 1;
    } catch (e: any) {
      counters.sendErrors += 1;
      const name = String(e?.message ?? e).slice(0, 60);
      counters.byError[name] = (counters.byError[name] ?? 0) + 1;
    }
  };

  await sleep(stagger);
  const t0 = Date.now();
  let trio: string[] = [];
  let order: string | null = null;
  let tick = 0;
  let webReady = false;
  const myClient = randomUUID();
  while ((Date.now() - t0) / 1000 < DURATION_S) {
    const tickStart = Date.now();
    const i = tick % 5;
    const now = new Date().toISOString();
    if (i === 0) {
      trio = [randomUUID()];
      await send('test_types', 'INSERT', { uid: trio[0] },
        { uid: trio[0], tenant_id: c.tenant, some_text: `${c.wid} c${Math.floor(tick / 5)} a`, inserted_at: now });
      if (!webReady) {
        try {
          if (Number((await zb.query('SELECT COUNT(*) n FROM sw_suppliers'))[0].n) > 0) {
            await send('sw_clients', 'INSERT', { uid: myClient },
              { uid: myClient, label: `client of ${c.wid}`, inserted_at: now });
            await send('sw_suppliers', 'INSERT', { country: 'wland', name: `sup-${c.wid}` },
              { country: 'wland', name: `sup-${c.wid}`, rating: 0, inserted_at: now });
            webReady = true;
          }
        } catch {}
      } else if (SUPPLIERS.length) {
        order = randomUUID();
        const [sc, sn] = SUPPLIERS[tick % SUPPLIERS.length];
        await send('sw_orders', 'INSERT', { uid: order },
          { uid: order, client_id: myClient, supplier_country: sc, supplier_name: sn,
            label: `${c.wid} o${tick}`, inserted_at: now });
      }
    } else if (i === 1) {
      trio.push(randomUUID());
      await send('test_types', 'INSERT', { uid: trio[1] },
        { uid: trio[1], tenant_id: c.tenant, some_text: `${c.wid} c${Math.floor(tick / 5)} b`, inserted_at: now });
      if (order) {
        // the composite-FK re-point: BOTH columns move in one UPDATE
        const [sc, sn] = SUPPLIERS[(tick + 3) % SUPPLIERS.length];
        await send('sw_orders', 'UPDATE', { uid: order }, { supplier_country: sc, supplier_name: sn });
      }
    } else if (i === 2) {
      trio.push(randomUUID());
      await send('test_types', 'INSERT', { uid: trio[2] },
        { uid: trio[2], tenant_id: c.tenant, some_text: `${c.wid} c${Math.floor(tick / 5)} c`, inserted_at: now });
      if (order) { await send('sw_orders', 'DELETE', { uid: order }); order = null; }
    } else if (i === 3) {
      await send('test_types', 'UPDATE', { uid: trio[2] }, { some_text: `${c.wid} touched` });
      if (webReady && Math.floor(tick / 5) % 3 === 0) {
        // composite-KEY update of the 1-side: key is {country, name}
        await send('sw_suppliers', 'UPDATE', { country: 'wland', name: `sup-${c.wid}` }, { rating: tick });
      }
    } else {
      await send('test_types', 'DELETE', { uid: trio[0] });
    }
    tick += 1;
    const elapsed = Date.now() - tickStart;
    if (elapsed < 1000) await sleep(1000 - elapsed);
  }

  // Settle: the outbox must drain (every write definitively acked) before the
  // replica is compared against anyone.
  const settleDeadline = Date.now() + SETTLE_S * 1000;
  let outbox = -1;
  while (Date.now() < settleDeadline) {
    try {
      outbox = Number((await zb.query('SELECT COUNT(*) n FROM _zebridge_outbox'))[0].n);
    } catch { outbox = -1; }
    if (outbox === 0) break;
    await sleep(2000);
  }
  await sleep(10_000); // let the last CDC fan-out land locally

  const digest = async (sql: string) => {
    const rows = await zb.query(sql);
    const uids = rows.map((r: any) => String(r.uid));
    return { count: uids.length, md5: createHash('md5').update(uids.join(',')).digest('hex') };
  };
  const report = {
    worker: c.wid, kind: `node-${c.engine}`, principal: c.principal, tenant: c.tenant,
    ticks: tick, ...counters, outboxLeft: outbox,
    test_types: await digest(
      `SELECT uid FROM test_types WHERE deleted_at IS NULL AND tenant_id = '${c.tenant}' ORDER BY uid`),
    orders: await digest('SELECT uid FROM orders ORDER BY uid'),
    sw_orders: await digest('SELECT uid FROM sw_orders ORDER BY uid'),
    sw_clients: await digest('SELECT uid FROM sw_clients ORDER BY uid'),
    sw_suppliers: await digest(`SELECT country || '|' || name AS uid FROM sw_suppliers ORDER BY uid`),
    orphans: Number((await zb.query(
      `SELECT (SELECT COUNT(*) FROM sw_orders o LEFT JOIN sw_clients c ON c.uid = o.client_id WHERE c.uid IS NULL)
            + (SELECT COUNT(*) FROM sw_orders o LEFT JOIN sw_suppliers s
               ON s.country = o.supplier_country AND s.name = o.supplier_name WHERE s.country IS NULL) AS n`))[0].n),
  };
  writeFileSync(c.report, JSON.stringify(report));
  await zb.close();
}

await Promise.all(SPEC.map((c, i) =>
  runClient(c, i * 200).catch((e) => {
    writeFileSync(c.report, JSON.stringify({ worker: c.wid, fatal: String(e).slice(0, 200) }));
  })));
process.exit(0);
