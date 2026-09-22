/// The client, wired for a phone. Everything platform-shaped is a parameter: the
/// storage, the decompressor, the connection. Nothing here is a fork of the library.
import './platform';
import { ZeBridge } from 'zb-client-ts';
import { expoStorage } from './expo-storage';
import { zstd } from './platform';

/// ⚠️ A phone is not the dev machine. `127.0.0.1` reaches the host from the iOS
/// simulator but NOT from a device or the Android emulator, which needs the host's LAN
/// address (the Android emulator's alias for the host is 10.0.2.2). And it is `ws://`,
/// not `nats://`: this client speaks NATS over WebSocket, so the server needs its
/// websocket block enabled.
export const NATS_URL = process.env.EXPO_PUBLIC_NATS_URL ?? 'ws://127.0.0.1:8080';
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
  });
}
