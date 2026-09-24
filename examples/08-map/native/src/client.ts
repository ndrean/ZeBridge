/// The client, wired for a phone. Everything platform-shaped is a parameter: the
/// storage, the decompressor, the connection. Nothing here is a fork of the library.
import './platform';
import { Platform } from 'react-native';
import { ZeBridge } from 'zb-client-ts';
import { expoStorage } from './expo-storage';
import { zstd, zstdStream } from './platform';

/// ⚠️ A phone is not the dev machine, and the two emulators disagree about how to reach
/// it. The iOS simulator shares the host's network stack, so `127.0.0.1` IS the Mac. The
/// Android emulator runs behind its own NAT and reserves `10.0.2.2` as the alias for the
/// host — `127.0.0.1` there is the emulator itself, which answers nothing.
///
/// §10io: resolved at RUN time rather than through the environment, so ONE bundle serves
/// both. `EXPO_PUBLIC_*` is inlined when Metro builds, so a variable would mean a
/// separate bundler per platform.
///
/// A real device on the same network needs the Mac's LAN address instead; set
/// `EXPO_PUBLIC_NATS_URL` for that case.
///
/// And it is `ws://`, not `nats://`: this client speaks NATS over WebSocket, so the
/// server needs its websocket block (scripts/native/nats-server-jwt.conf, port 8080).
const HOST = Platform.OS === 'android' ? '10.0.2.2' : '127.0.0.1';
export const NATS_URL = process.env.EXPO_PUBLIC_NATS_URL ?? `ws://${HOST}:8080`;
export const PRINCIPAL = process.env.EXPO_PUBLIC_PRINCIPAL ?? 'omar';
export const TENANT = process.env.EXPO_PUBLIC_TENANT ?? '_default';

/// The creds travel as an app secret, never as a repo path: an app bundle cannot read
/// the developer's home directory, which is the assumption both Flutter apps still make.
const CREDS = process.env.EXPO_PUBLIC_CREDS ?? '';

export function makeClient() {
  if (!CREDS) throw new Error('set EXPO_PUBLIC_CREDS to the principal\'s creds file contents');
  return new ZeBridge({
    natsUrl: NATS_URL,
    principal: PRINCIPAL,
    creds: CREDS,
    // §10ic: the markers are the ANSWER, never a stored table. `charge_points` stays
    // DECLARED so a mutation has its descriptor; only `routes` is followed.
    tables: ['routes'],
    ondemandTables: ['charge_points'],
    heartbeatMs: 0,
    storage: expoStorage,
    zstdDecompress: zstd,
    // §10ix: seed large tables as they arrive. The phone has a real filesystem, so
    // expo-storage says `spillsTemp` and a full is STAGED — fast and bounded in RAM,
    // at ~3× the table on disk while it runs.
    seedStreaming: true,
    zstdDecompressStream: zstdStream,
  });
}
