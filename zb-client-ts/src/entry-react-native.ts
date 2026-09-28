/// zb-client-ts on React Native: the replica in expo-sqlite, NATS over WebSocket, and
/// what Hermes does not ship. zstd is libzb's native decoder when the app has the
/// ZeBridge native module (`ZbNative`), fzstd otherwise — measured on an iPhone 12, a
/// 3M-row seed took 309.8 s native against 393.8 s with fzstd (NOTES §10ja). The
/// bundler's `react-native` condition picks this file for `import … from 'zb-client-ts'`;
/// it is also 'zb-client-ts/react-native'. expo-sqlite and
/// react-native-get-random-values are the app's to install (optional peers).
import 'react-native-get-random-values'; // crypto.getRandomValues, which `uuid` needs
import { sha256 } from 'js-sha256';
import { decompress } from 'fzstd';
import * as SQLite from 'expo-sqlite';
import { registerPlatform } from './platform.ts';
import { expoStorage } from './expo-storage.ts';
import { fzstdStream } from './fzstd-stream.ts';

// `crypto.randomUUID` (the client id) and `crypto.subtle.digest` (the grammar hash and
// chain objects). Hermes has neither.
const g = globalThis as any;
if (!g.crypto) g.crypto = {};
if (!g.crypto.randomUUID) {
  g.crypto.randomUUID = () => {
    const b = g.crypto.getRandomValues(new Uint8Array(16));
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // RFC 4122 variant
    const h = [...b].map((x: number) => x.toString(16).padStart(2, '0')).join('');
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
  };
}
if (!g.crypto.subtle) {
  g.crypto.subtle = {
    /// ⚠️ §10ik: this must hash BYTES — the first version hashed a latin1 string's UTF-8
    /// encoding, every chain object failed its digest, and `connect()` never resolved.
    digest: async (alg: string, data: BufferSource) => {
      if (String(alg).toUpperCase() !== 'SHA-256') throw new Error(`unsupported digest ${alg}`);
      const bytes = data instanceof Uint8Array ? data : ArrayBuffer.isView(data)
        ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength) : new Uint8Array(data as ArrayBuffer);
      return sha256.arrayBuffer(bytes);
    },
  };
}

/// libzb's zstd through the native module (zb_zstd_new/push/free), looked up at first
/// use — Expo installs its modules on `globalThis.expo.modules` before the app runs.
/// Bytes cross JSI only as arguments (zero-copy): `zstdPush` inflates into a buffer kept
/// on the native side and returns its length, `zstdTake` fills a Uint8Array that long.
type ZbZstd = { zstdNew(): number; zstdPush(id: number, c: Uint8Array): number; zstdTake(id: number, d: Uint8Array): void; zstdFree(id: number): void };
let nativeZstd: ZbZstd | null | undefined;
const native = (): ZbZstd | null => {
  if (nativeZstd === undefined) {
    const m = g.expo?.modules?.ZbNative;
    nativeZstd = typeof m?.zstdNew === 'function' ? m : null;
  }
  return nativeZstd ?? null;
};

const nativeOnce = (z: ZbZstd, b: Uint8Array): Uint8Array => {
  const id = z.zstdNew();
  try {
    const n = z.zstdPush(id, b);
    const out = new Uint8Array(n);
    if (n) z.zstdTake(id, out);
    return out;
  } finally { z.zstdFree(id); }
};

const nativeStream = (z: ZbZstd, chunks: AsyncIterable<Uint8Array>): AsyncIterable<Uint8Array> =>
  (async function* () {
    const id = z.zstdNew();
    try {
      for await (const c of chunks) {
        const n = z.zstdPush(id, c);
        if (n) { const out = new Uint8Array(n); z.zstdTake(id, out); yield out; }
      }
    } finally { z.zstdFree(id); }
  })();

registerPlatform({
  name: 'react-native',
  storage: ({ engine }) => {
    if (engine === 'pglite') throw new Error("zb-client-ts: engine 'pglite' is for the browser and Node; React Native uses expo-sqlite");
    return expoStorage;
  },
  zstdDecompress: (b) => { const z = native(); return z ? nativeOnce(z, b) : decompress(b); },
  zstdDecompressStream: (chunks) => { const z = native(); return z ? nativeStream(z, chunks) : fzstdStream(chunks); },
  zstdName: () => (native() ? 'libzb (native)' : 'fzstd (JS)'),
  // §10jq: the identity in a small expo-sqlite database of its own (expo-sqlite is
  // already this platform's storage). The Keychain (expo-secure-store) would suit the
  // seed better; it is one more native module, not taken yet.
  identity: {
    load: async (key) => {
      const db = await identityDb();
      const rows = await db.getAllAsync(`SELECT text FROM identity WHERE key = '${key.replace(/'/g, "''")}'`) as { text: string }[];
      return rows[0]?.text ?? null;
    },
    save: async (key, text) => {
      const db = await identityDb();
      const q = (v: string) => `'${v.replace(/'/g, "''")}'`; // this expo-sqlite's execAsync takes no parameters
      await db.execAsync(`INSERT OR REPLACE INTO identity (key, text) VALUES (${q(key)}, ${q(text)})`);
    },
  },
  natsOverWebSocket: true,
});

let identityDbP: Promise<SQLite.SQLiteDatabase> | null = null;
function identityDb(): Promise<SQLite.SQLiteDatabase> {
  identityDbP ??= SQLite.openDatabaseAsync('zebridge-identity.db').then(async (db) => {
    await db.execAsync('CREATE TABLE IF NOT EXISTS identity (key TEXT PRIMARY KEY, text TEXT NOT NULL)');
    return db;
  });
  return identityDbP;
}

export * from './index.ts';
export { expoStorage } from './expo-storage.ts';
