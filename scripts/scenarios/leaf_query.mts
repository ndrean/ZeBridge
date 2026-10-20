// The real TS client transport, exercised by leaf_security.py on an isolated leaf.
import { ZeBridge } from '../../zb-client-ts/src/entry-node.ts';
const [, , url, creds, db] = process.argv;
const client = new ZeBridge({ natsUrl: url, credsPath: creds, dbPath: db,
  principal: 'lf-leaf1.alice', jsDomain: 'hub', seedDomain: 'leaf', tables: [], heartbeatMs: 0 });
try {
  await client.connect();
  const result = await client.request('query.acme.probe', {});
  console.log(JSON.stringify(result));
} finally {
  await client.close();
}
