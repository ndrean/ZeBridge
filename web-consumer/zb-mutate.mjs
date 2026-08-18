// Ingress probe — publishes one mutation with the §7.2 envelope and reports what came
// back: dead letters, and whether a CDC echo followed.
//
//   node zb-mutate.mjs [id]     (from web-consumer/, so the @nats-io packages resolve)
//
// The point is that "no dead letter" is not success. A mutation that is well-formed but
// hits a SQL error is retried silently, so the only proof it landed is the CDC echo and
// the row itself. This probe was written while chasing exactly that: an accepted envelope
// that produced no row because `bridge_writer` had no GRANT on the table.
//
// Companion to zb-probe.mjs, which covers the permissions side.

// Publishes one mutation using the §7.2 envelope, then reports what came back.
import { wsconnect, headers } from '@nats-io/nats-core';
import { encode, decode } from '@msgpack/msgpack';

const CLIENT_ID = '1234567890123456';
const table = 'test_types';
const id = Number(process.argv[2] ?? 777001);
const d = new Date(); const p = (n,w=2)=>String(n).padStart(w,'0');
const version = d.toISOString().replace('Z','') + '000';  // the shape CDC echoes back

const nc = await wsconnect({ servers: 'ws://localhost:8080', user: 'alice', pass: 's3cret' });
const errs = [];
(async () => { for await (const m of nc.subscribe('mutation_error.>')) errs.push([m.subject, new TextDecoder().decode(m.data)]); })();
const cdc = [];
(async () => { for await (const m of nc.subscribe('cdc.>')) cdc.push(m.subject); })();
await new Promise(r => setTimeout(r, 300));

const payload = { key: { id }, data: { id, some_text: 'envelope probe', age: 42, price: 99.99, is_true: true, updated_at: version, inserted_at: version }, version, client_id: CLIENT_ID };
const h = headers(); h.set('Nats-Msg-Id', `${CLIENT_ID}-${table}-${id}-${version}`);
nc.publish(`mutation.alice.${table}.insert`, encode(payload), { headers: h });
await nc.flush();
console.log(`published id=${id} version=${version}`);
await new Promise(r => setTimeout(r, 2500));
console.log(errs.length ? `dead letters: ${JSON.stringify(errs)}` : 'dead letters: none');
console.log(`cdc echoes seen: ${cdc.length ? cdc.join(', ') : 'none'}`);
await nc.drain();
