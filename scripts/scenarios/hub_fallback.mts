// Real TS client for hub_fallback.py. The transport wrappers observe domain
// selection while delegating every operation to the installed NATS libraries.
import assert from 'node:assert/strict';
import { ZeBridge, natsTransport } from '../../zb-client-ts/src/entry-node.ts';
import type { JetStreamOpts, Transport } from '../../zb-client-ts/src/transport.ts';

const [, , url, creds, db, seedDomain, streaming] = process.argv;
const calls: { kind: string; name: string; domain: string }[] = [];
let expectedChunks = 0, yieldedChunks = 0, streamedBeforeComplete = false;
const record = (kind: string, name: string, js?: JetStreamOpts) => {
  calls.push({ kind, name, domain: js?.domain ?? '' });
};
const transport: Transport = {
  ...natsTransport,
  kv: (nc, bucket, opts, js) => {
    record('kv', bucket, js);
    return natsTransport.kv(nc, bucket, opts, js);
  },
  objectStore: async (nc, bucket, js) => {
    record('object', bucket, js);
    const store = await natsTransport.objectStore(nc, bucket, js);
    const info = store.info.bind(store);
    store.info = async (...args: any[]) => {
      const result = await info(...args);
      if (result) expectedChunks = result.chunks;
      return result;
    };
    return store;
  },
  jetstream: (nc, js) => {
    const client = natsTransport.jetstream(nc, js);
    const consumers = client.consumers;
    const get = consumers.get.bind(consumers);
    consumers.get = async (...args: any[]) => {
      record('consumer', String(args[0]), js);
      const consumer = await get(...args);
      if (String(args[0]).startsWith('OBJ_')) {
        yieldedChunks = 0;
        const fetch = consumer.fetch.bind(consumer);
        consumer.fetch = async (...fetchArgs: any[]) => {
          const messages = await fetch(...fetchArgs);
          const iterator = messages[Symbol.asyncIterator].bind(messages);
          messages[Symbol.asyncIterator] = async function* () {
            for await (const msg of { [Symbol.asyncIterator]: iterator }) {
              yieldedChunks++;
              yield msg;
            }
          };
          return messages;
        };
      }
      return consumer;
    };
    return client;
  },
  jetstreamManager: (nc, js) => {
    record('manager', '', js);
    return natsTransport.jetstreamManager(nc, js);
  },
};
const client = new ZeBridge({
  natsUrl: url, credsPath: creds, dbPath: db,
  principal: 'lf-edge1.alice', jsDomain: 'hub',
  ...(seedDomain === '-' ? {} : { seedDomain }),
  tables: ['seed_probe'], heartbeatMs: 0, transport,
  seedStreaming: streaming === 'stream', seedStreamingAboveBytes: 1,
  seedChunkRows: 64,
});
client.onLog((topic, data, level) => console.error(JSON.stringify({ topic, data, level })));
client.onSeedProgress((progress) => {
  if (progress.applied > 0 && yieldedChunks < expectedChunks) streamedBeforeComplete = true;
});
try {
  await client.connect();
  const [seeded] = await client.sql('SELECT count(*) AS n, sum(n) AS total FROM seed_probe');
  assert.equal(seeded.n, 2048);
  assert.equal(seeded.total, 2048 * 2047 / 2);
  // No seed is accepted on row count alone: every column must survive the chain.
  const [bad] = await client.sql("SELECT count(*) AS n FROM seed_probe WHERE tenant_id != 'acme' OR length(body) != 160 OR updated_at != '2026-10-20T00:00:00.000000Z'");
  assert.equal(bad.n, 0);
  const [gate] = await client.sql('SELECT seed_seq, seed_stream, watermark FROM _zebridge_generations WHERE tbl = ?', 'seed_probe');
  assert.equal(gate.seed_stream, 'CDC_acme');
  assert.equal(gate.seed_seq, 1);
  assert.equal(gate.watermark, '2026-10-20T00:00:00.000000Z');
  assert.equal(streamedBeforeComplete, streaming === 'stream', 'streaming applies rows before receiving the complete object');
  await client.request('query.acme.seed_probe_ready', {});
  const end = Date.now() + 5000;
  let value = 0;
  do {
    const [row] = await client.sql("SELECT n FROM seed_probe WHERE uid = '0000'");
    value = row.n;
    if (value === 7) break;
    await new Promise((resolve) => setTimeout(resolve, 20));
  } while (Date.now() < end);
  assert.equal(value, 7, 'hub CDC still applies after mirrored seeding');
  console.log(JSON.stringify({ seeded: seeded.n, cdc: value, calls, streamedBeforeComplete }));
} finally {
  await client.close();
}
