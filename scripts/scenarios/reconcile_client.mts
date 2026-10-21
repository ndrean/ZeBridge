// Real TS client for reconcile.py: one JSON command per stdin line, one JSON answer per
// stdout line. Verdicts are collected as they arrive and handed back by `state`.
import { createInterface } from 'node:readline';
import { ZeBridge } from '../../zb-client-ts/src/entry-node.ts';

const [, , url, creds, db, principal, table] = process.argv;
const verdicts: unknown[] = [];
let client: ZeBridge | null = null;

const open = async (ondemand = false) => {
  client = new ZeBridge({ natsUrl: url, credsPath: creds, dbPath: db, principal, jsDomain: 'hub', tables: ondemand ? [] : [table], ondemandTables: ondemand ? [table] : [], heartbeatMs: 0 });
  client.onVerdict((v) => verdicts.push(v));
  client.onLog((topic, data, level) => console.error(JSON.stringify({ topic, data, level })));
  await client.connect();
};

const answer = async (cmd: any): Promise<unknown> => {
  switch (cmd.cmd) {
    case 'open': await open(cmd.ondemand); return { ok: true };
    case 'close': await client?.close(); client = null; return { ok: true };
    case 'mutate': return await client!.mutate(cmd.table, cmd.op, cmd.key, cmd.values);
    case 'flush': await client!.flushOutbox(); return { ok: true };
    case 'state': return {
      verdicts: verdicts.splice(0),
      outbox: (await client!.outboxAll()).map((r: any) => r.msg_id),
      reconciling: await client!.reconciling(),
    };
    case 'sql': return await client!.query(cmd.sql);
    default: throw new Error(`unknown command ${cmd.cmd}`);
  }
};

for await (const line of createInterface({ input: process.stdin })) {
  if (!line.trim()) continue;
  try {
    console.log(JSON.stringify({ ok: true, value: await answer(JSON.parse(line)) }));
  } catch (e) {
    console.log(JSON.stringify({ ok: false, error: String(e) }));
  }
}
process.exit(0);
