/// What React Native does not ship that the client expects. Three small gaps, all
/// filled here rather than in the library, because the library already takes each one
/// as a parameter or reads it off `globalThis` (§10ih).
///
/// Imported for its side effects, FIRST, before anything touches the client.
import 'react-native-get-random-values'; // crypto.getRandomValues, which `uuid` needs
import * as Crypto from 'expo-crypto';
import { decompress as zstdDecompress, Decompress } from 'fzstd';
import { requireOptionalNativeModule } from 'expo';

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

/// ⚠️ The one that would have been a wall. Chain objects arrive as zstd frames, and the
/// library's LAST-RESORT decompressor is a WebAssembly module — which React Native's
/// engine cannot run. It never gets that far, because `zstdDecompress` is a config
/// option: supply one and the WASM import is never reached. fzstd is pure JavaScript
/// and does plain frames — which every chain object is, since the per-era dictionary
/// went on 2026-09-24 (NOTES §10iy; until then a delta was a dictionary frame and
/// needed a native decoder here).
const fzstdOnce = (b: Uint8Array) => zstdDecompress(b);

/// §10ix: the same decoder, STREAMING — what lets a phone seed a large table without
/// holding it. fzstd's `Decompress` takes chunks as they arrive and hands back inflated
/// bytes block by block; the client decodes rows out of those and applies them in
/// windows, so the table is never whole in memory (measured, NOTES §10ix).
const fzstdStream = (chunks: AsyncIterable<Uint8Array>): AsyncIterable<Uint8Array> => {
  return (async function* () {
    const out: Uint8Array[] = [];
    const d = new Decompress((chunk: Uint8Array) => { out.push(chunk); });
    for await (const c of chunks) { d.push(c); while (out.length) yield out.shift()!; }
    d.push(new Uint8Array(0), true);
    while (out.length) yield out.shift()!;
  })();
};

/// §10ja: libzb's zstd through the app's native module (modules/zb-native), when it is
/// in the build: fzstd in JS was ~40 % of the seed's JS time. Synchronous calls, one
/// chunk at a time; bytes cross as arguments only (ZbNativeModule.swift).
/// `EXPO_PUBLIC_ZB_ZSTD=fzstd` keeps the JavaScript decoder, to compare.
type ZbZstd = { zstdNew(): number; zstdPush(id: number, c: Uint8Array): number; zstdTake(id: number, d: Uint8Array): void; zstdFree(id: number): void };
const native = process.env.EXPO_PUBLIC_ZB_ZSTD === 'fzstd' ? null : requireOptionalNativeModule<ZbZstd>('ZbNative');
const hasNative = typeof native?.zstdNew === 'function';

const nativeOnce = (b: Uint8Array): Uint8Array => {
  const id = native!.zstdNew();
  try {
    const n = native!.zstdPush(id, b);
    const out = new Uint8Array(n);
    if (n) native!.zstdTake(id, out);
    return out;
  } finally { native!.zstdFree(id); }
};

const nativeStream = (chunks: AsyncIterable<Uint8Array>): AsyncIterable<Uint8Array> => {
  return (async function* () {
    const id = native!.zstdNew();
    try {
      for await (const c of chunks) {
        const n = native!.zstdPush(id, c);
        if (n) { const out = new Uint8Array(n); native!.zstdTake(id, out); yield out; }
      }
    } finally { native!.zstdFree(id); }
  })();
};

export const ZSTD_ENGINE = hasNative ? 'libzb (native)' : 'fzstd (JS)';
export const zstd = hasNative ? nativeOnce : fzstdOnce;
export const zstdStream = hasNative ? nativeStream : fzstdStream;
