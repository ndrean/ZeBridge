/// The client for this app: what is about the APP — the NATS URL, who we are, which
/// table. Storage (expo-sqlite), zstd (libzb's native decoder when the app has the
/// ZbNative module, fzstd otherwise) and the crypto Hermes lacks come from zb-client-ts's
/// react-native entry, which Metro picks by itself.
import { Platform } from 'react-native';
import { ZeBridge } from 'zb-client-ts';
import { expoStorage } from 'zb-client-ts/react-native';
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
export const CREDS = process.env.EXPO_PUBLIC_CREDS ?? '';

export function makeClient() {
  if (!CREDS) throw new Error("set EXPO_PUBLIC_CREDS to the principal's creds file contents");
  return new ZeBridge({
    natsUrl: NATS_URL,
    principal: PRINCIPAL,
    creds: CREDS,
    tables: [TABLE],
    heartbeatMs: 0,
    // §10ix: the base streams in as it arrives, a window at a time — bounded memory.
    seedStreaming: true,
    // EXPO_PUBLIC_ZB_TRACE=1: the same storage, timed (src/seed-trace.ts).
    ...(TRACE ? { storage: traced(expoStorage) } : {}),
  });
}
