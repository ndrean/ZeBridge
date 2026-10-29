// A real zb-client-ts client for recreate_recovery.py check 12 (the fence, TypeScript side).
// One process per case: it connects, follows the table, and runs until `<db>.stop` appears.
//
// The fence's fault points are the harness's, not the library's: the library reads no
// environment for them. `faultHook` is the one seam, and this file maps the same variables
// libzb's fault build reads onto it, so check 4 and check 12 drive both clients alike:
//   ZB_TEST_FENCE_ABORT=after_chunk|before_commit   exit(70) at that boundary
//   ZB_TEST_FENCE_SLEEP_MS=<ms>                     hold before the fence's VERIFY
//   ZB_TEST_SHARED_UNREADABLE=1                     the shared route's lookup fails
//   ZB_TEST_FENCE_HOLD_FILE=<path>                  while the file exists, hold before the fence reads the position (the tails keep applying)
//
//   node --experimental-strip-types fence_client.mts <natsUrl> <creds> <db> <principal> <table[,table...]> [seedChunkRows]
import { existsSync } from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';
import { ZeBridge } from '../../zb-client-ts/src/entry-node.ts';

const [, , url, creds, db, principal, table, chunk] = process.argv;
const abortAt = process.env.ZB_TEST_FENCE_ABORT;
const holdMs = Number(process.env.ZB_TEST_FENCE_SLEEP_MS ?? 0);
const holdFile = process.env.ZB_TEST_FENCE_HOLD_FILE;
const sharedUnreadable = !!process.env.ZB_TEST_SHARED_UNREADABLE;
// leaf_fence.py: a client behind a seeding leaf (creds '-': the stack has no authentication).
const seedDomain = process.env.ZB_SEED_DOMAIN;

const say = (line: string) => console.error(line);

const client = new ZeBridge({
  natsUrl: url, ...(creds === '-' ? {} : { credsPath: creds }), dbPath: db, principal, jsDomain: 'hub', tables: table.split(','), heartbeatMs: 0,
  ...(seedDomain ? { seedDomain } : {}),
  ...(Number(chunk) > 0 ? { seedChunkRows: Number(chunk) } : {}),
  faultHook: async (point) => {
    if (point === 'before_decision' && holdFile && existsSync(holdFile)) {
      say('test: fence holding before reading the position');
      while (existsSync(holdFile)) await sleep(100);
    }
    if (point === 'after_chunk' && abortAt === 'after_chunk') process.exit(70);
    if (point === 'before_commit' && abortAt === 'before_commit') process.exit(70);
    if (point === 'after_commit' && abortAt === 'after_commit') process.exit(70);
    if (point === 'before_verify' && holdMs > 0) {
      say(`test: fence holding ${holdMs} ms before its verify`);
      await sleep(holdMs);
    }
    if (point === 'shared_lookup' && sharedUnreadable) throw new Error('test: the shared route is unreadable');
  },
});
// One line per log message (events are objects and stay out): the scenario greps these ("fence prepared — chain g3 cut at 412 …").
client.onLog((_topic, data) => { if (typeof data === 'string') say(data); });

await client.connect();
while (!existsSync(`${db}.stop`)) await sleep(200);
await client.close();
process.exit(0);
