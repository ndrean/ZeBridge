// Real TS client over TCP or WebSocket; controlled by the stage-3 Python fixture.
import { createInterface } from 'node:readline';
import { readFileSync } from 'node:fs';
import { nodeConnect } from '../../zb-client-ts/src/node.ts';
import { wsconnect } from '../../zb-client-ts/node_modules/@nats-io/nats-core/lib/mod.js';
import { ZeBridge } from '../../zb-client-ts/src/entry-node.ts';
import { currentPlatform, registerPlatform } from '../../zb-client-ts/src/platform.ts';

const [optionsPath, mode, ca] = process.argv.slice(2);
const options = JSON.parse(readFileSync(optionsPath, 'utf8'));
if (mode === 'ws') registerPlatform({ ...currentPlatform(), natsOverWebSocket: true, connect: wsconnect });
let nc: any;
let dialOptions: any;
const client = new ZeBridge({
  ...options,
  connect: async (opts: any) => {
    dialOptions = opts;
    nc = await (mode === 'ws' ? wsconnect(opts) : nodeConnect({ ...opts, tls: { caFile: ca } }));
    return nc;
  },
});
const send = (value: unknown) => console.log(JSON.stringify(value));
client.onLog((topic, data, level) => console.error(JSON.stringify({ topic, data, level })));
async function snapshot() {
  return {
    connected: !!nc?.protocol?.connected,
    server: nc?.getServer(),
    pool: nc?.getServers().map((s: any) => s.listen),
    implicit: nc?.getServers().filter((s: any) => s.gossiped).length,
    max_payload: nc?.info?.max_payload,
    dial: { servers: dialOptions.servers, noRandomize: dialOptions.noRandomize, ignoreClusterUpdates: dialOptions.ignoreClusterUpdates },
    rows: await client.sql('SELECT id, v FROM s2_row ORDER BY id'),
    outbox: await client.sql('SELECT msg_id, hex(payload) AS payload FROM _zebridge_outbox ORDER BY msg_id'),
  };
}
try {
  await client.connect();
  send(await snapshot());
  for await (const line of createInterface({ input: process.stdin })) {
    const command = JSON.parse(line);
    if (command.cmd === 'close') break;
    if (command.cmd === 'write') await client.mutate('s2_row', 'INSERT', { id: command.id }, { v: command.v });
    if (command.cmd === 'flush') await (client as any).flushOutbox();
    if (command.cmd === 'renew') await (client as any).renewNow(true);
    if (command.cmd === 'info') {
      // Exercise the installed NATS INFO handler, including an asynchronous advert.
      nc.protocol.processInfo(new TextEncoder().encode(JSON.stringify({ ...nc.info, connect_urls: ['127.0.0.1:1'] })));
    }
    send(await snapshot());
  }
} finally {
  await client.close();
}
