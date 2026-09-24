/// What React Native does not ship that the client expects. Three small gaps, all
/// filled here rather than in the library, because the library already takes each one
/// as a parameter or reads it off `globalThis` (§10ih).
///
/// Imported for its side effects, FIRST, before anything touches the client.
import 'react-native-get-random-values'; // crypto.getRandomValues, which `uuid` needs
import * as Crypto from 'expo-crypto';
import { decompress as zstdDecompress, Decompress } from 'fzstd';

// `crypto.randomUUID` (the client id) and `crypto.subtle.digest` (the grammar hash).
// React Native has neither; both are one call to expo-crypto.
const g = globalThis as any;
if (!g.crypto) g.crypto = {};
if (!g.crypto.randomUUID) g.crypto.randomUUID = () => Crypto.randomUUID();
if (!g.crypto.subtle) {
  g.crypto.subtle = {
    /// ⚠️ §10ik: this MUST hash BYTES. The first version converted the buffer to a
    /// latin1 string and called `digestStringAsync`, which encodes its input as UTF-8 —
    /// so every byte above 0x7F became two, and the hash was of different data. It is
    /// used to verify chain objects, so the symptom was
    ///
    ///     routes: chain object routes-g42-full unreadable: digest mismatch
    ///
    /// repeated for ever: the seed retried, failed the check, waited for the producer,
    /// retried. `connect()` never resolved and nothing was thrown — the app simply sat
    /// on "connecting…". A wrong hash of binary data is silent by construction, which
    /// is why it cost so much to find.
    ///
    /// `Crypto.digest` takes a BufferSource and returns the real digest.
    digest: async (alg: string, data: BufferSource) => {
      if (String(alg).toUpperCase() !== 'SHA-256') throw new Error(`unsupported digest ${alg}`);
      return await Crypto.digest(Crypto.CryptoDigestAlgorithm.SHA256, data);
    },
  };
}

/// ⚠️ The one that would have been a wall. Chain objects may arrive as zstd frames, and
/// the library's LAST-RESORT decompressor is a WebAssembly module — which React Native's
/// engine cannot run. It never gets that far, because `zstdDecompress` is a config
/// option: supply one and the WASM import is never reached.
///
/// fzstd is pure JavaScript and handles plain frames. It does NOT do DICTIONARY frames.
/// Measured on this stack: the `routes` chain this app follows carries no dictionary and
/// its objects are a few hundred bytes. A table whose chain does use one (the charge
/// points do, `charge_points-g1-dict`) would need a native zstd module — and this app
/// never seeds that table, because it is on-demand.
export const zstd = (b: Uint8Array, dict?: Uint8Array) => {
  if (dict) throw new Error('zstd dictionary frames need a native decompressor; fzstd does plain frames only');
  return zstdDecompress(b);
};

/// §10ix: the same decoder, STREAMING — what lets a phone seed a large table without
/// holding it. fzstd's `Decompress` takes chunks as they arrive and hands back inflated
/// bytes block by block; the client decodes rows out of those and applies them in
/// windows, so the table is never whole in memory (measured in Node with this exact
/// pipeline, NOTES §10ix). Plain frames only, like `zstd` above: a FULL is one (the
/// dictionary is trained from it), and dictionary steps stay on the buffered path
/// because client.ts sets `zstdStreamDictionaries: false`.
export const zstdStream = (chunks: AsyncIterable<Uint8Array>, dict?: Uint8Array): AsyncIterable<Uint8Array> => {
  if (dict) throw new Error('fzstd streams plain frames only');
  return (async function* () {
    const out: Uint8Array[] = [];
    const d = new Decompress((chunk: Uint8Array) => { out.push(chunk); });
    for await (const c of chunks) { d.push(c); while (out.length) yield out.shift()!; }
    d.push(new Uint8Array(0), true);
    while (out.length) yield out.shift()!;
  })();
};
