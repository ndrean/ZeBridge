/// The client, wired for a phone — the same host layer as examples/08-map/native
/// (platform.ts, expo-storage.ts), which explains each piece. Only the table differs:
/// this app follows ONE big table and nothing else.
import './platform';
import { Platform } from 'react-native';
import { ZeBridge } from 'zb-client-ts';
import { expoStorage } from './expo-storage';
import { zstd, zstdStream } from './platform';
import { TRACE, traced } from './seed-trace';

/// The iOS simulator shares the Mac's network; the Android emulator reaches it as
/// 10.0.2.2. Resolved at run time so one bundle serves both (08-map, §10io). A real
/// device needs the Mac's LAN address in `EXPO_PUBLIC_NATS_URL` — and `ws://`: this
/// client speaks NATS over WebSocket (nats-server's websocket block, port 8080).
const HOST = Platform.OS === 'android' ? '10.0.2.2' : '127.0.0.1';
export const NATS_URL = process.env.EXPO_PUBLIC_NATS_URL ?? `ws://${HOST}:8080`;
/// bob is on globex, the tenant that holds the fixture: test_types, 3,055,002 rows.
export const PRINCIPAL = process.env.EXPO_PUBLIC_PRINCIPAL ?? 'bob';
export const TABLE = process.env.EXPO_PUBLIC_TABLE ?? 'test_types';

/// The creds are an app secret: `EXPO_PUBLIC_CREDS="$(cat …/bob.creds)"` at build time.
const CREDS = process.env.EXPO_PUBLIC_CREDS ?? '';

export function makeClient() {
  if (!CREDS) throw new Error("set EXPO_PUBLIC_CREDS to the principal's creds file contents");
  return new ZeBridge({
    natsUrl: NATS_URL,
    principal: PRINCIPAL,
    creds: CREDS,
    tables: [TABLE],
    // ONE database, kept across launches: a second launch finds the table seeded and
    // only tails. "wipe & seed again" is the button.
    durable: true,
    heartbeatMs: 0,
    storage: TRACE ? traced(expoStorage) : expoStorage,
    zstdDecompress: zstd,
    // §10ix: the base streams in as it arrives and is STAGED — expo-storage says
    // `spillsTemp`, so windows are appended to a TEMP table and the real table is
    // filled once, sorted by SQLite on disk: bounded RAM, ~3× the table on disk while
    // it runs.
    seedStreaming: true,
    zstdDecompressStream: zstdStream,
  });
}
