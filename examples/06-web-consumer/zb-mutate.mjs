// Ingress probe — publishes one mutation with the §7.2 envelope and reports the
// **definitive reply** the bridge sends back (§7.4b).
//
//   node zb-mutate.mjs [uid]    (from web-consumer/, so the @nats-io packages resolve)
//
// Everything identity-shaped comes from the environment, so the same script can probe as
// either principal without editing it:
//
//   ZB_PRINCIPAL=bob ZB_PASSWORD=s3cret ZB_TENANT=globex node zb-mutate.mjs
//
//   ZB_PRINCIPAL  default alice     the NATS user AND the subject token — they must match
//   ZB_PASSWORD   default s3cret
//   ZB_TENANT     default acme      must be mapped to this principal, or RLS refuses
//   ZB_CLIENT_ID  default random    the LWW tiebreak; a constant makes every run one writer
//   ZB_WS         default ws://localhost:8080
//
// This probe used to exist because "no dead letter" was not success: a well-formed
// mutation that hit a SQL error was retried silently, and the only proof it landed was the
// CDC echo and the row itself. It was written while chasing exactly that — an accepted
// envelope that produced no row, because `bridge_writer` had no GRANT on the table.
//
// That gap is closed. Every write now gets one reply on
// `mutation_ack.<principal>.<msg_id>`, and it says which of five things happened. The CDC
// echo is still checked here, because the two answer different questions: the verdict says
// what the *database* did, the echo says whether the change reached *subscribers*.
//
// Companion to zb-probe.mjs, which covers the permissions side.

import { wsconnect, headers } from '@nats-io/nats-core';
import { encode } from '@msgpack/msgpack';

// ⚠️ Env-driven, and `client_id` defaults to a fresh value per run rather than a
// constant. It is the last-write-wins tiebreak (§7.3): two writers sharing one id cannot
// have a tie broken between them, so equal versions leave their replicas divergent with
// no error. A constant here made every probe run look like the same writer.
const PRINCIPAL = process.env.ZB_PRINCIPAL ?? 'alice';
const PASSWORD = process.env.ZB_PASSWORD ?? 's3cret';
const CLIENT_ID = process.env.ZB_CLIENT_ID ?? `c-${crypto.randomUUID().slice(0, 8)}`;

// ⚠️ Must match a row in `zebridge_user_tenants` for this principal, or RLS refuses the
// write — and `tenant_id` is NOT NULL, so omitting it fails for a reason unrelated to
// whatever is being probed.
const TENANT = process.env.ZB_TENANT ?? 'acme';
const table = 'test_types';
// ⚠️ A UUID, not an integer. `test_types` keys on `uid uuid`, and the client mints it —
// PROTOCOL.md §7.2: the key travels in the envelope, so the row's identity is decided
// before the write leaves. This probe previously sent `key: { id }` against the pre-uuid
// schema and reported "dead letters: none, cdc echoes: none" — indistinguishable from a
// bridge that was simply slow. The verdict says `MissingPrimaryKey` outright.
const uid = process.argv[2] ?? crypto.randomUUID();
const version = new Date().toISOString().replace('Z', '') + '000';  // the shape CDC echoes back
// Every NOT NULL column with no default has to be supplied or the row is refused for a
// reason unrelated to what is being probed.


// ⚠️ The msg_id becomes the last token of the verdict subject, and `version` carries
// fractional seconds — so an unsanitised id makes `mutation_ack.alice.<id>` four tokens
// instead of three, and a trailing or doubled dot is an *invalid subject* the publish
// rejects outright. Either way the reply is lost with only a bridge-side log. Same rule,
// and the same helper, as src/App.tsx.
const subjectSafe = (v) => v.replace(/[.*>\s]/g, '-');
const msgId = subjectSafe(`${CLIENT_ID}-${table}-${uid}-${version}`);

const nc = await wsconnect({
  servers: process.env.ZB_WS ?? 'ws://localhost:8080',
  user: PRINCIPAL,
  pass: PASSWORD,
});

let verdict = null;
(async () => {
  for await (const m of nc.subscribe(`mutation_ack.${PRINCIPAL}.${msgId}`)) {
    verdict = JSON.parse(new TextDecoder().decode(m.data));
  }
})();
// ⚠️ No `mutation_error.>` subscription, and that is a permission, not an omission: a
// client is not allowed to read the dead-letter channel. Its payloads carry the server's
// full message, whose DETAIL can quote rows belonging to other tenants, so it is
// operator-facing — `nats-server.conf.template` grants alice `cdc.>`, `init.>` and
// `mutation_ack.alice.>` and nothing more. Subscribing anyway kills the connection with a
// permissions violation, which is how this probe first failed after the grants were
// tightened.
//
// The verdict is the client-facing half of the same information, minus the parts a client
// may not see: `detail` carries the first line only.
const cdc = [];
(async () => { for await (const m of nc.subscribe('cdc.>')) cdc.push(m.subject); })();
await new Promise(r => setTimeout(r, 300));

const payload = {
  key: { uid },
  data: {
    uid,
    some_text: 'envelope probe',
    age: 42,
    price: 99.99,
    is_true: true,
    tenant_id: TENANT,
    updated_at: version,
    inserted_at: version,
  },
  version,
  client_id: CLIENT_ID,
};
const h = headers();
h.set('Nats-Msg-Id', msgId);
nc.publish(`mutation.${PRINCIPAL}.${table}.insert`, encode(payload), { headers: h });
await nc.flush();
console.log(`published uid=${uid} version=${version}`);
console.log(`awaiting mutation_ack.${PRINCIPAL}.${msgId}`);
await new Promise(r => setTimeout(r, 3000));

// What §7.1 tells a client to do with each. This is the whole point of the channel: the
// outbox pops on four of the five, and only `failed` is worth resending.
const ACTION = {
  accepted:    'pop — the write applied',
  stale:       'pop, do NOT revert — a newer version won, the winning row arrives via CDC',
  row_deleted: 'pop and SURFACE — the row was deleted elsewhere',
  rejected:    'pop — PostgreSQL will refuse these same bytes again',
  failed:      'keep and retry under the same Nats-Msg-Id',
};

if (verdict) {
  console.log(`verdict: ${JSON.stringify(verdict)}`);
  console.log(`  → ${ACTION[verdict.status] ?? 'unknown status — keep pending rather than guess'}`);
  if (verdict.reason === 'version_clamped') {
    console.log(`  ⚠️ the version was clamped to ${verdict.version} — this clock is ahead of the database's`);
  }
} else {
  // Now a real signal rather than the normal case: silence means the bridge never reached
  // a conclusion — ingress is not running, it died mid-write, or the reply was
  // undeliverable.
  console.log('verdict: NONE within 3s — ingress may be down, or the reply was undeliverable');
}
console.log(`cdc echoes seen: ${cdc.length ? cdc.join(', ') : 'none'}`);
await nc.drain();
