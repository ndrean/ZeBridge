/// zb-client-ts in the browser: the replica in sqlite-wasm on OPFS (or PGlite with
/// `engine: 'pglite'`), NATS over WebSocket, zstd by fzstd (pure JS, plain frames —
/// every chain object since NOTES §10iy). The bundler's `browser` condition picks this
/// file for `import … from 'zb-client-ts'`; it is also 'zb-client-ts/browser'.
import { decompress } from 'fzstd';
import { fzstdStream } from './fzstd-stream.ts';
import { registerPlatform } from './platform.ts';
import { browserStorage } from './browser-storage.ts';

registerPlatform({
  name: 'browser',
  // PGlite is loaded only when asked for: it is a large WASM engine.
  storage: async ({ engine }) => engine === 'pglite' ? (await import('./pglite-storage.ts')).makePgliteStorage({ persist: true }) : browserStorage,
  zstdDecompress: (b) => decompress(b),
  zstdDecompressStream: fzstdStream,
  zstdName: () => 'fzstd (JS)',
  // §10jq: the identity per origin, like the OPFS replica beside it.
  identity: {
    load: async (key) => { try { return localStorage.getItem(`zebridge.identity:${key}`); } catch { return null; } },
    save: async (key, text) => { localStorage.setItem(`zebridge.identity:${key}`, text); },
  },
  natsOverWebSocket: true,
});

export * from './index.ts';
export { browserStorage } from './browser-storage.ts';
