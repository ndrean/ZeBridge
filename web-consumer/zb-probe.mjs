// Permissions probe — answers "what does the client actually need to be allowed to do?"
// empirically, instead of by reading client-library source and guessing.
//
//   node zb-probe.mjs        (from web-consumer/, so the @nats-io packages resolve)
//
// It replays App.tsx's startup sequence against a server and prints ok/FAIL per step.
// Written after a permissions block that looked complete failed at runtime with
// `Permissions Violation for Publish to "$JS.API.INFO"` — a subject no amount of reading
// the allow-list would have suggested, because it is issued by `jetstreamManager()`
// before any feature is used. Two more ($JS.API.CONSUMER.INFO.*) were missing for the
// same reason.
//
// To discover a *new* client's requirements: grant the test user `publish = ">"`,
// subscribe as an admin to `$JS.API.>`, run this, and read off the subjects actually
// used. Then narrow the allow-list to exactly those and re-run — every step must pass.
//
// `jsm.consumers.delete` is expected to FAIL against the real config: App.tsx never
// deletes a consumer, and the grant is withheld on purpose. See nats-server.conf.template.

import { wsconnect } from '@nats-io/nats-core';
import { jetstream, jetstreamManager, DeliverPolicy } from '@nats-io/jetstream';
import { Kvm } from '@nats-io/kv';

const step = async (name, fn) => {
  try { await fn(); console.log(`ok    ${name}`); }
  catch (e) { console.log(`FAIL  ${name}: ${e.message}`); }
};

const nc = await wsconnect({ servers: 'ws://localhost:18080', user: 'alice', pass: 's3cret' });
console.log('connected');
const js = jetstream(nc);
let jsm;
await step('jetstreamManager()', async () => { jsm = await jetstreamManager(nc); });
await step('Kvm.open(schemas)+watch', async () => {
  const kv = await new Kvm(nc).open('schemas'); const w = await kv.watch();
  setTimeout(() => w.stop(), 300);
});
await step('Kvm.open(snapshots)+get', async () => {
  const kv = await new Kvm(nc).open('snapshots'); await kv.get('users');
});
await step('kv.watch({key})', async () => {
  const kv = await new Kvm(nc).open('schemas'); const w = await kv.watch({ key: 'users' });
  setTimeout(() => w.stop(), 300);
});
await step('jsm.streams.info(CDC)', async () => { await jsm.streams.info('CDC'); });
await step('js.publish(snapshot.request.users)', async () => { await js.publish('snapshot.request.users', new Uint8Array()); });
await step('nc.publish(mutation.alice.users.insert)', async () => { nc.publish('mutation.alice.users.insert', new Uint8Array([1])); await nc.flush(); });
let ci;
await step('jsm.consumers.add(INIT)', async () => {
  ci = await jsm.consumers.add('INIT', { filter_subject: 'init.snap.users.abc.>', deliver_policy: DeliverPolicy.All, ack_policy: 'explicit' });
});
await step('js.consumers.get(INIT)+fetch+ack', async () => {
  const c = await js.consumers.get('INIT', ci.name);
  const b = await c.fetch({ max_messages: 10, expires: 1000 });
  for await (const m of b) { m.ack(); }
});
let cc;
await step('jsm.consumers.add(CDC)', async () => {
  cc = await jsm.consumers.add('CDC', { deliver_policy: DeliverPolicy.New, ack_policy: 'explicit' });
});
await step('js.consumers.get(CDC)+consume', async () => {
  const c = await js.consumers.get('CDC', cc.name);
  const it = await c.consume({ max_messages: 10 });
  setTimeout(() => it.stop(), 500);
  for await (const m of it) { m.ack(); }
});
await step('jsm.consumers.delete(INIT)', async () => { await jsm.consumers.delete('INIT', ci.name); });
await new Promise(r => setTimeout(r, 800));
await nc.drain();
console.log('done');
