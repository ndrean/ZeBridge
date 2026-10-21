// Multi-client outbox storm worker: real clients, automatic recovery only.
import { readFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { ZeBridge } from '../../zb-client-ts/src/entry-node.ts';
const options = JSON.parse(readFileSync(process.argv[2], 'utf8'));
const clients: ZeBridge[] = [];
const inflight = new Map<ZeBridge, Map<number, {op: string; since: number}>>();
const errors: string[] = [];
try {
  await Promise.all(options.map(async (o: any) => {
    const c = new ZeBridge(o);
    clients.push(c);
    const active = new Map<number, {op: string; since: number}>();
    inflight.set(c, active);
    let serial = 0;
    for (const op of ['collectMissedVerdicts', 'outboxMarkPublished', 'flushOutboxOnce', 'handleVerdict']) {
      const original = (c as any)[op].bind(c);
      (c as any)[op] = async (...args: any[]) => {
        const id = ++serial;
        active.set(id, {op, since: performance.now()});
        try { return await original(...args); } finally { active.delete(id); }
      };
    }
    c.onLog((topic, data, level) => {
      if (level === 'ERROR') console.error(JSON.stringify({ principal: o.principal, topic, data }));
    });
    try { await c.connect(); } catch (e) { errors.push(String(e)); }
  }));
  console.log(JSON.stringify({ ready: clients.length - errors.length, errors }));
  for await (const line of createInterface({ input: process.stdin })) {
    const cmd = JSON.parse(line);
    if (cmd === 'close') break;
    const pending = await Promise.all(clients.map(async (c) => Number((await c.sql('SELECT count(*) AS n FROM _zebridge_outbox'))[0].n)));
    console.log(JSON.stringify({ pending, active: clients.map(c => ({
      principal: (c as any).config.principal, connected: (c as any).naturallyConnected,
      closed: (c as any).nc?.isClosed(), busy: !!(c as any).outboxWork,
      work: [...inflight.get(c)!.values()].map(x => ({op:x.op, seconds: Math.round((performance.now()-x.since)/1000)})),
    })) }));
  }
} finally {
  await Promise.all(clients.map((c) => c.close()));
  // Breaking readline's iterator leaves the pipe resumed. Close the worker's
  // input too, so an open controller pipe cannot masquerade as a client leak.
  process.stdin.destroy();
}
