/// App.tsx — one table, one bar, one clock. The point of this example is the SEED of a
/// big table from the generation chain in a browser (NOTES §10ix): the client stages
/// the rows as they arrive into a TEMP table on OPFS and lets SQLite sort them once
/// into the real table — bounded in memory, a few minutes, and the page shows that it
/// is working. Nothing else — no mutation, no console. The numbers it prints at the
/// end (rows, distinct keys, a sum) are the same three the Node and libzb benchmarks
/// checked against PostgreSQL, so a run here is a measurement.

import { ZeBridge, type SeedProgress, type Phase, type ConnStatus } from '../../../../zb-client-ts';
import { Decompress } from 'fzstd';
import { createSignal, onMount, onCleanup, For, Show } from 'solid-js';

/// One client, one socket, one replica: a hot update must reload, not re-run this scope.
if (import.meta.hot) import.meta.hot.accept(() => location.reload());

/// Same-origin paths, proxied by the dev server to wherever the stack is (vite.config.ts).
const wsScheme = location.protocol === 'https:' ? 'wss' : 'ws';
const NATS_URL = `${wsScheme}://${location.host}/nats`;
const BRIDGE_URL = '/bridge';

/// `?principal=` and `?table=`. bob is on globex, the tenant that holds the fixture:
/// test_types at 3,055,002 rows (NOTES §10iw). Creds are served from /creds, a symlink
/// to scripts/native/creds — dev only.
const _qs = new URLSearchParams(location.search);
const PRINCIPAL = _qs.get('principal') ?? 'bob';
const TABLE = _qs.get('table') ?? 'test_types';
const CREDS = await fetch(`/creds/${PRINCIPAL}.creds`).then((r) => (r.ok ? r.text() : undefined)).catch(() => undefined);

/// fzstd inflates plain frames chunk by chunk — and every chain object is one.
const zstdStream = (chunks: AsyncIterable<Uint8Array>): AsyncIterable<Uint8Array> => {
  return (async function* () {
    const out: Uint8Array[] = [];
    const d = new Decompress((chunk: Uint8Array) => { out.push(chunk); });
    for await (const c of chunks) { d.push(c); while (out.length) yield out.shift()!; }
    d.push(new Uint8Array(0), true);
    while (out.length) yield out.shift()!;
  })();
};

/// ⚠️ `durable: true` — ONE database, `zebridge_<principal>.sqlite3` in OPFS, not a
/// fresh file per load. A 1 GB replica per reload is how 05-tables blew its quota. A
/// second load finds the table seeded and only tails; "wipe & reload" is how you
/// seed again.
const zb = new ZeBridge({
  natsUrl: NATS_URL,
  bridgeUrl: BRIDGE_URL,
  principal: PRINCIPAL,
  creds: CREDS,
  tables: [TABLE],
  durable: true,
  engine: 'sqlite',
  seedStreaming: true,
  zstdDecompressStream: zstdStream,
});

/// A console handle for probing the replica (`zb.query('PRAGMA journal_mode')`) —
/// the database is in OPFS, there is no file to open with the sqlite3 CLI.
declare global { interface Window { zb: ZeBridge } }
window.zb = zb;

const fmt = (n: number) => n.toLocaleString('en-US');
const secs = (ms: number) => `${(ms / 1000).toFixed(1)} s`;
type Line = { text: string; err: boolean };
type Facts = { count: number; distinct?: number; sum?: number; dbBytes?: number; heapPeak?: number; files?: string[] };

export default function App() {
  const [status, setStatus] = createSignal<ConnStatus>('disconnected');
  const [phase, setPhase] = createSignal<Record<Phase, boolean>>({ connected: false, migrated: false, snapshot: false, cdc: false });
  const [progress, setProgress] = createSignal<SeedProgress | null>(null);
  const [elapsed, setElapsed] = createSignal(0);
  const [seedMs, setSeedMs] = createSignal<number | null>(null);
  const [facts, setFacts] = createSignal<Facts | null>(null);
  const [lines, setLines] = createSignal<Line[]>([]);
  const [busy, setBusy] = createSignal(false);

  /// The clock: from `connect()` until the table is usable (phase `cdc`), ticked every
  /// 100 ms. The seed span alone — first window to `done` — is reported separately.
  let t0 = 0;
  let seedT0 = 0;
  let heapPeak = 0;
  let tick: ReturnType<typeof setInterval> | undefined;
  const stopClock = () => { if (tick) clearInterval(tick); tick = undefined; setElapsed(performance.now() - t0); };

  zb.onStatus(setStatus);
  zb.onPhase((p) => {
    setPhase((prev) => ({ ...prev, [p]: true }));
    if (p === 'cdc') { stopClock(); void measure(); }
  });
  zb.onSeedProgress((p) => {
    if (p.table !== TABLE) return;
    if (!seedT0) seedT0 = performance.now();
    setProgress(p);
    if (p.done) setSeedMs(performance.now() - seedT0);
  });
  zb.onLog((topic, data, level) => {
    if (topic !== 'SYS') return;
    const text = typeof data === 'string' ? data : JSON.stringify(data);
    setLines((prev) => [...prev.slice(-199), { text: `[${level}] ${text}`, err: level === 'ERROR' }]);
  });

  /// After the table is usable: the three facts the benchmarks compare with PostgreSQL
  /// (`SELECT count(*), count(DISTINCT uid), sum(age) FROM test_types` on the tenant —
  /// 3,055,002 / 3,055,002 / 138,916,285 on 2026-09-24), the OPFS bytes, and the JS
  /// heap peak sampled during the seed (Chrome only; wasm and OPFS are not in it —
  /// the browser's task manager has the process figure).
  async function measure() {
    const [row] = TABLE === 'test_types'
      ? await zb.query('SELECT count(*) AS count, count(DISTINCT uid) AS "distinct", sum(age) AS sum FROM test_types')
      : await zb.query(`SELECT count(*) AS count FROM ${TABLE}`);
    const est = await navigator.storage?.estimate?.().catch(() => undefined);
    // What the origin actually holds — the replica, and any temp file sqlite-wasm's
    // VFS left behind (16 random letters; the client sweeps them, §10ix).
    const files: string[] = [];
    try {
      const root = await navigator.storage.getDirectory();
      for await (const [name, h] of (root as any).entries()) {
        if (h.kind === 'file') files.push(`${name} ${((await h.getFile()).size / 1e6).toFixed(0)} MB`);
      }
    } catch { /* not listable: fine */ }
    setFacts({ count: Number(row.count), distinct: row.distinct != null ? Number(row.distinct) : undefined, sum: row.sum != null ? Number(row.sum) : undefined, dbBytes: est?.usage, heapPeak: heapPeak || undefined, files });
  }

  onMount(() => {
    t0 = performance.now();
    tick = setInterval(() => {
      setElapsed(performance.now() - t0);
      const used = (performance as any).memory?.usedJSHeapSize as number | undefined;
      if (used && used > heapPeak) heapPeak = used;
    }, 100);
    zb.connect().catch((e) => setLines((prev) => [...prev, { text: `connect failed: ${e}`, err: true }]));
  });
  onCleanup(() => { stopClock(); void zb.close(); });

  async function wipe() {
    setBusy(true);
    await zb.wipe();
    location.reload();
  }

  const pct = () => { const p = progress(); return p && p.total ? Math.round((100 * p.applied) / p.total) : 0; };

  return (
    <>
      <h1>ZeBridge — one large table <span class={`badge ${status()}`}>{status()}</span></h1>
      <p class="sub">
        <code>{TABLE}</code> as <code>{PRINCIPAL}</code>, seeded from the generation chain into OPFS-SQLite: staged as it arrives, sorted
        once by SQLite (bounded memory, a few minutes — hence the bar; the last stretch at 100% is the sort). Database <code>{zb.dbName}</code>, kept across reloads.
      </p>

      <ul class="phases">
        <For each={[['connected', 'NATS connected'], ['migrated', 'schema migrated'], ['snapshot', 'snapshot replayed'], ['cdc', 'CDC active']] as [Phase, string][]}>
          {([key, label]) => <li classList={{ done: phase()[key] }}>{label}</li>}
        </For>
      </ul>

      <div class="seed" classList={{ idle: !progress() && !phase().cdc }}>
        <div class="row">
          <progress max={progress()?.total ?? 1} value={progress()?.applied ?? 0} />
          <span class="clock">{secs(elapsed())}</span>
        </div>
        <div class="detail">
          <span>
            <Show when={progress()} fallback={phase().cdc ? 'no seed needed — the table was already in the replica' : 'waiting for the first window…'}>
              {(p) => <>{fmt(p().applied)} / {fmt(p().total)} rows ({pct()}%) · {p().kind} {p().step}{p().done ? ' · done' : ''}</>}
            </Show>
          </span>
          <span>
            <Show when={seedMs() !== null}>seed {secs(seedMs()!)} · {fmt(Math.round(progress()!.total / (seedMs()! / 1000)))} rows/s</Show>
          </span>
        </div>
      </div>

      <Show when={facts()}>
        {(f) => (
          <table class="facts">
            <tbody>
              <tr><td>rows</td><td>{fmt(f().count)}</td></tr>
              <Show when={f().distinct !== undefined}><tr><td>distinct uid</td><td>{fmt(f().distinct!)}</td></tr></Show>
              <Show when={f().sum !== undefined}><tr><td>sum(age)</td><td>{fmt(f().sum!)}</td></tr></Show>
              <tr><td>total, connect → usable</td><td>{secs(elapsed())}</td></tr>
              <Show when={f().dbBytes}><tr><td>OPFS usage</td><td>{(f().dbBytes! / 1e9).toFixed(2)} GB by <code>storage.estimate()</code> — Chrome's accounting, which does not shrink when files are removed until it recounts; the files row below is what is there</td></tr></Show>
              <Show when={f().heapPeak}><tr><td>JS heap peak</td><td>{(f().heapPeak! / 1e6).toFixed(0)} MB (heap only — see the task manager)</td></tr></Show>
              <Show when={f().files?.length}><tr><td>OPFS files</td><td>{f().files!.join(' · ')}</td></tr></Show>
            </tbody>
          </table>
        )}
      </Show>

      <div class="actions">
        <button onClick={wipe} disabled={busy()}>wipe &amp; reload</button>
      </div>

      <pre class="log"><For each={lines()}>{(l) => <div class={l.err ? 'err' : ''}>{l.text}</div>}</For></pre>
    </>
  );
}
