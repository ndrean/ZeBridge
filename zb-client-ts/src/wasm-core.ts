/// libzb's core, compiled to WebAssembly (libzb/src/wasm_core.zig, `zig build wasm-core`):
/// the rules both clients share, written once, in Zig. zb-client-ts keeps what is about
/// its host — the NATS socket, the storage, one thread — and asks the core for decisions.
///
/// The module is loaded once per process, before the first connect (`loadCore`, which
/// `connect()` calls with the platform's bytes); every rule after that is synchronous.
/// The memory protocol, per call: room from the core's arena (`zb_alloc`), the arguments
/// written there as UTF-8 JSON, the call, its result read through a FRESH view of the
/// memory (a call may grow it, which detaches every older view), then `zb_reset`.
/// Integers that need no allocation cross as BigInt.

/// The module's bytes, or the fetch answering with them.
export type CoreSource = BufferSource | Response;

type Exports = {
  memory: WebAssembly.Memory;
  zb_alloc(len: number): number;
  zb_reset(): void;
  zb_caught_up_position(pos: bigint, firstSeq: bigint, lastSeq: bigint, numPending: bigint, numAckPending: bigint, deliveredCount: bigint, delivered: bigint): bigint;
  zb_scope_seeding(ptr: number, len: number): bigint;
};

let ex: Exports | null = null;
let loading: Promise<void> | null = null;

/// Instantiates the core once; later calls (another client, a reconnect) wait for the
/// same load.
export function loadCore(src: CoreSource | Promise<CoreSource>): Promise<void> {
  if (ex) return Promise.resolve();
  loading ??= (async () => {
    const s = await src;
    const bytes = s instanceof Response ? await s.arrayBuffer() : s;
    const { instance } = await WebAssembly.instantiate(bytes, {});
    ex = instance.exports as unknown as Exports;
  })().catch((e) => {
    loading = null;
    throw new Error(`zb-client-ts: the WASM core did not load: ${e?.message ?? e}`);
  });
  return loading;
}

function core(): Exports {
  if (!ex) throw new Error('zb-client-ts: the WASM core is not loaded — connect() loads it, or call loadCore()');
  return ex;
}

const enc = new TextEncoder();
const dec = new TextDecoder();

/// One JSON call: arguments in, the result parsed. 0 means the core refused them.
function call(fn: (ptr: number, len: number) => bigint, name: string, args: unknown): any {
  const c = core();
  const bytes = enc.encode(JSON.stringify(args));
  try {
    const ptr = c.zb_alloc(bytes.length);
    if (!ptr) throw new Error(`zb-client-ts: the WASM core could not allocate ${bytes.length} bytes for ${name}`);
    new Uint8Array(c.memory.buffer, ptr, bytes.length).set(bytes);
    const at = fn(ptr, bytes.length);
    if (at === 0n) throw new Error(`zb-client-ts: the WASM core refused ${name}'s arguments`);
    const off = Number(at >> 32n), len = Number(at & 0xffffffffn);
    return JSON.parse(dec.decode(new Uint8Array(c.memory.buffer, off, len)));
  } finally {
    c.zb_reset();
  }
}

/// A stream's span against the position a client stored (the gap rule, D2, §10n).
export type StreamGap = { firstSeq: number; stored: number; lastSeq?: number };

/// Seeding is SCOPED: a gap on one stream re-seeds only the tables routed to it (a
/// tenant table's open-tenant rows ride `sharedRoute`, CDC_PUBLIC), plus tables never
/// seeded. libzb core.scopeSeeding.
export function scopeSeeding(
  streams: Record<string, StreamGap>,
  tables: Record<string, { route: string; sharedRoute?: string; seeded: boolean }>,
): { gapped: string[]; tablesToSeed: string[] } {
  const c = core();
  return call(c.zb_scope_seeding, 'scopeSeeding', { streams, tables });
}

/// Where an idle, caught-up consumer's position may move: to the stream's end when
/// nothing is pending, in flight or delivered past `pos`; never over a pruned range
/// (`firstSeq` past `pos + 1`), never backwards. `firstSeq` absent: unknown, 0.
/// libzb core.caughtUpPosition.
export function caughtUpPosition(pos: number, lastSeq: number, c: { numPending: number; numAckPending: number; deliveredCount: number; delivered: number; firstSeq?: number }): number {
  const b = (n: number) => BigInt(Math.max(0, Math.trunc(n)));
  return Number(core().zb_caught_up_position(b(pos), b(c.firstSeq ?? 0), b(lastSeq), b(c.numPending), b(c.numAckPending), b(c.deliveredCount), b(c.delivered)));
}
