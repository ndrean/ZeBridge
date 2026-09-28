/// What differs between Node, the browser and React Native — and nothing else. The core
/// (libzb.ts) never imports platform code: an entry file registers one `Platform`, and
/// `import { ZeBridge } from 'zb-client-ts'` gets the right entry from the bundler's
/// conditions (package.json `exports`: react-native, browser, node). An app passes what
/// is about the APP — natsUrl, creds, tables, dbPath — never a storage adapter or a
/// decoder (NOTES §10ja).
import type { StorageFactory } from './storage.ts';
import type { TransportConnection } from './transport.ts';

export type PlatformName = 'node' | 'browser' | 'react-native';

export interface Platform {
  name: PlatformName;
  /// The replica at `dbPath` (a file on Node and React Native, an OPFS name in the
  /// browser). `engine` is the config's: 'pglite' only in the browser.
  storage(opts: { dbPath: string; engine?: 'sqlite' | 'pglite' }): StorageFactory | Promise<StorageFactory>;
  /// The NATS dial. Absent: the transport's own (WebSocket).
  connect?: (opts: any) => Promise<TransportConnection>;
  zstdDecompress(b: Uint8Array): Uint8Array | Promise<Uint8Array>;
  zstdDecompressStream(chunks: AsyncIterable<Uint8Array>): AsyncIterable<Uint8Array>;
  /// For answers this client serves (§10hq). Absent: answers go uncompressed.
  zstdCompress?(b: Uint8Array): Uint8Array | Promise<Uint8Array>;
  /// An incremental SHA-256 (a streamed chain object is never one buffer). Absent: the
  /// core's pure-JS one.
  sha256Stream?(): { update(c: Uint8Array): void; base64(): string };
  /// `credsPath`: only where there is a filesystem to read it from.
  readText?(path: string): string;
  /// §10jq: where the identity `/enroll` produced is kept between runs, by key (the
  /// `identityPath`): a mode-600 file on Node — the same JSON libzb writes, so both
  /// clients read one identity — localStorage in the browser, an expo-sqlite row on
  /// React Native. Absent: an enrollment still works, but is not remembered.
  identity?: { load(key: string): Promise<string | null>; save(key: string, text: string): Promise<void> };
  /// Whether this platform dials NATS over TCP (Node) or only over WebSocket (the
  /// browser, React Native): picks `nats_url` or `nats_ws_url` from the identity.
  natsOverWebSocket?: boolean;
  /// Which zstd decoder runs — `zb.platformInfo`, for a log line.
  zstdName(): string;
}

let current: Platform | null = null;

export function registerPlatform(p: Platform): void {
  current = p;
}

export function currentPlatform(): Platform {
  if (!current) {
    throw new Error("zb-client-ts: no platform loaded — import from 'zb-client-ts' (the bundler picks node, browser or react-native) or from 'zb-client-ts/<platform>'");
  }
  return current;
}
