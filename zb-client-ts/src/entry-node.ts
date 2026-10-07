/// zb-client-ts on Node: the replica in a better-sqlite3 file, NATS over TCP, zstd and
/// SHA-256 from node:zlib and node:crypto, `credsPath` read from disk. The bundler's
/// `node` condition picks this file for `import … from 'zb-client-ts'`; it is also
/// 'zb-client-ts/node'. better-sqlite3 and @nats-io/transport-node are the host's to
/// install (optional peers).
import { readFileSync } from 'node:fs';
import { readFile, rename, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { createZstdDecompress, zstdCompressSync, zstdDecompressSync } from 'node:zlib';
import { registerPlatform } from './platform.ts';
import { nodeConnect, nodeStorage } from './node.ts';

registerPlatform({
  name: 'node',
  // PGlite in a directory at `dbPath`, or SQLite in a file there.
  storage: async ({ engine }) => engine === 'pglite' ? (await import('./pglite-storage.ts')).makePgliteStorage({ persist: true }) : nodeStorage,
  connect: nodeConnect,
  zstdDecompress: (b) => new Uint8Array(zstdDecompressSync(b)),
  zstdCompress: (b) => new Uint8Array(zstdCompressSync(b)),
  /// Written into the Transform by hand rather than `pipe`d, so a source error (a
  /// digest mismatch after the last chunk) destroys the output and the reader sees it,
  /// instead of an end.
  zstdDecompressStream: (chunks) => {
    const out = createZstdDecompress();
    void (async () => {
      try {
        for await (const c of chunks) { if (!out.write(c)) await once(out, 'drain'); }
        out.end();
      } catch (e) { out.destroy(e as Error); }
    })();
    return out as AsyncIterable<Uint8Array>;
  },
  sha256Stream: () => {
    const h = createHash('sha256');
    return { update: (c) => { h.update(c); }, base64: () => h.digest('base64') };
  },
  readText: (path) => readFileSync(path, 'utf8'),
  identity: {
    load: async (key) => readFile(key, 'utf8').catch((e) => (e?.code === 'ENOENT' ? null : Promise.reject(e))),
    // A temporary name renamed into place, mode 600: never half an identity, never
    // readable by another user (the seed is in it).
    save: async (key, text) => {
      await writeFile(`${key}.tmp`, text, { mode: 0o600 });
      await rename(`${key}.tmp`, key);
    },
  },
  zstdName: () => 'node:zlib',
  coreWasm: () => readFile(new URL('../wasm/zb_core.wasm', import.meta.url)),
});

export * from './index.ts';
export { nodeConnect, nodeStorage } from './node.ts';
