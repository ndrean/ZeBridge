import { Libzb } from '../src';

/// Run in an Expo development build that includes ZbNative and ABI 8.
export async function checkOrderingModes(natsUrl: string, dbPath: string): Promise<void> {
  const client = await Libzb.connect({ natsUrl, dbPath, tables: ['wr_rows'], principal: 'tester', clientId: 'react-native' });
  try {
    await client.sync();
    const write = async (uid: string, options?: { after?: string; independent?: boolean }) => {
      const reply = await client.mutate('wr_rows', 'INSERT', { uid }, { uid }, options);
      if (!reply.msgId) throw new Error(reply.error ?? 'Missing msgId');
      return reply.msgId;
    };
    const parent = await write('parent');
    await write('unrelated');
    const child = await write('child', { after: parent });
    await write('independent', { independent: true });
    await write('tail');
    const { rows } = await client.query('SELECT ordered, predecessor FROM _zebridge_outbox ORDER BY created_at');
    if (rows.length !== 5 || rows[2][1] !== parent || rows[3][0] !== 0 ||
        rows[3][1] !== null || rows[4][1] !== child) {
      throw new Error('Unexpected outbox: ' + JSON.stringify(rows));
    }
  } finally {
    await client.close();
  }
}
