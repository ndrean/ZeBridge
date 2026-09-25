/// fzstd's streaming decoder as a chunk stream: chunks in, inflated bytes out, block by
/// block, so a large chain object is never whole in memory (§10ix). Pure JavaScript and
/// plain frames only — every chain object since NOTES §10iy. The browser's decoder, and
/// React Native's when the app has no native module.
import { Decompress } from 'fzstd';

export function fzstdStream(chunks: AsyncIterable<Uint8Array>): AsyncIterable<Uint8Array> {
  return (async function* () {
    const out: Uint8Array[] = [];
    const d = new Decompress((chunk: Uint8Array) => { out.push(chunk); });
    for await (const c of chunks) { d.push(c); while (out.length) yield out.shift()!; }
    d.push(new Uint8Array(0), true);
    while (out.length) yield out.shift()!;
  })();
}
