import { parseArgs } from 'node:util';
import { Client } from 'pg';
import { compactDocument } from './compaction.ts';

const { values } = parseArgs({ options: {
  tenant: { type: 'string' }, document: { type: 'string' },
  'min-updates': { type: 'string', default: '1000' }, help: { type: 'boolean' },
} });
if (values.help) {
  console.log('YJS_MAINTENANCE_DATABASE_URL=… pnpm run compact --tenant acme --document <uuid> [--min-updates 1000]');
} else {
  const connectionString = process.env.YJS_MAINTENANCE_DATABASE_URL;
  if (!connectionString || !values.tenant || !values.document) {
    throw new Error('Set YJS_MAINTENANCE_DATABASE_URL to a table-owner connection and pass --tenant and --document');
  }
  const client = new Client({ connectionString, connectionTimeoutMillis: 5000 });
  try {
    await client.connect();
    console.log(JSON.stringify(await compactDocument(client, values.tenant, values.document, Number(values['min-updates']))));
  } finally { await client.end(); }
}
