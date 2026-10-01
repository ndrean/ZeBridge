// The zb-client-ts side of jwt_renew.py. Prints one JSON line, then exits.
//
//   node --experimental-strip-types renew_client.mts <bridgeUrl> <invite|-> <dbPath> <natsUrl> <table> <seconds>
//
// Connects (enrolling with the invite, or on the stored identity with `-`), stays
// connected <seconds>, and reports the rows it holds and how many distinct JWTs its
// identity file carried meanwhile.
import { readFileSync } from 'node:fs';
import { ZeBridge } from '../../zb-client-ts/src/entry-node.ts';

const [, , bridgeUrl, inviteArg, dbPath, natsUrl, table, seconds] = process.argv;
const invite = inviteArg === '-' ? undefined : inviteArg;
const zb = new ZeBridge({ bridgeUrl, invite, dbPath, natsUrl, tables: [table], heartbeatMs: 0 });

const jwts = new Set<string>();
const note = () => {
  try { jwts.add(JSON.parse(readFileSync(`${dbPath}.identity`, 'utf8')).creds.split('\n')[1]); } catch { /* not written yet */ }
};

try {
  await zb.connect();
} catch (e) {
  console.log(JSON.stringify({ error: (e as Error).message }));
  process.exit(0);
}
note();
const t0 = Date.now();
while (Date.now() - t0 < Number(seconds) * 1000) {
  await new Promise((r) => setTimeout(r, 500));
  note();
}
const rows = (await zb.query(`SELECT count(*) AS n FROM ${table}`))[0]?.n ?? 0;
await zb.close();
console.log(JSON.stringify({ connected: true, rows: Number(rows), jwts: jwts.size }));
process.exit(0);
