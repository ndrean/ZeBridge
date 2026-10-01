// The zb-client-ts side of revoke_purge.py. Prints JSON lines, then exits.
//
//   node --experimental-strip-types purge_client.mts <bridgeUrl> <invite|-> <dbPath> <natsUrl> [wait|once]
//
//   invite, wait  enroll, connect, then wait (≤ 30 s) to be revoked; report revoked/purged
//   invite, once  enroll, connect, close: a device that goes away with its identity stored
//   -             reopen on the stored identity (no invite); report the connect's outcome
import { ZeBridge } from '../../zb-client-ts/src/entry-node.ts';

const [, , bridgeUrl, inviteArg, dbPath, natsUrl, mode = 'wait'] = process.argv;
const invite = inviteArg === '-' ? undefined : inviteArg;
const zb = new ZeBridge({ bridgeUrl, invite, dbPath, natsUrl, tables: ['counter_public'], heartbeatMs: 0 });

try {
  await zb.connect();
} catch (e) {
  console.log(JSON.stringify({ error: (e as Error).message, revoked: zb.revoked, purged: zb.purged }));
  process.exit(0);
}
console.log(JSON.stringify({ connected: true }));
if (!invite || mode === 'once') {
  await zb.close();
  process.exit(0);
}

const t0 = Date.now();
while (!zb.revoked && Date.now() - t0 < 30_000) await new Promise((r) => setTimeout(r, 200));
// The purge runs after the ban is read: give it the moment it needs to delete the files.
await new Promise((r) => setTimeout(r, 1_500));
console.log(JSON.stringify({ revoked: zb.revoked, purged: zb.purged }));
process.exit(0);
