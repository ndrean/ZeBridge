/// Where a zb-client-ts seed spends its time on the phone, every 100k rows: inside
/// SQLite (native) versus JS, the Hermes GC, the heap, the WAL. On with
/// `EXPO_PUBLIC_ZB_TRACE=1`; the trace lands in the app's Documents/seed-trace.tsv,
/// which `xcrun devicectl device copy from … --domain-type appDataContainer` fetches.
import * as FileSystem from 'expo-file-system';
import type { Exec, StorageFactory } from 'zb-client-ts/storage';

export const TRACE = process.env.EXPO_PUBLIC_ZB_TRACE === '1';

const sq = { ms: 0, chars: 0 };

/// SQLite time = every statement, plus each transaction's own BEGIN/COMMIT (the WAL
/// writes happen at COMMIT): the transaction's wall minus the JS inside its callback.
export const traced = (factory: StorageFactory): StorageFactory => (name) => {
  const st = factory(name);
  const wrap = (e: Exec): Exec => async (q, ...params) => {
    const t = performance.now();
    try { return await e(q, ...params); } finally {
      sq.ms += performance.now() - t;
      for (const p of params) if (typeof p === 'string') sq.chars += p.length;
    }
  };
  return {
    ...st,
    exec: wrap(st.exec),
    transaction: async (fn) => {
      const t = performance.now();
      let inner = 0; // the callback's wall, SQLite statements included
      await st.transaction(async (tx) => {
        const u = performance.now();
        try { await fn(wrap(tx)); } finally { inner += performance.now() - u; }
      });
      sq.ms += performance.now() - t - inner;
    },
  };
};

type Stats = Record<string, number>;
const hermes = (): Stats => (globalThis as any).HermesInternal?.getInstrumentedStats?.() ?? {};

export function makeRecorder(dbUri: string) {
  const out = `${FileSystem.documentDirectory}seed-trace.tsv`;
  const lines = ['rows\twall_s\td_wall_ms\td_sqlite_ms\td_js_ms\tgc_time\tgc_cpu\tnum_gcs\theap_mb\talloc_mb\twal_mb\td_json_mchars'];
  let t0 = 0; let next = 100_000;
  let last = { wall: 0, sqlite: 0, chars: 0 };
  return {
    start() {
      t0 = performance.now(); next = 100_000; last = { wall: 0, sqlite: 0, chars: 0 };
      sq.ms = 0; sq.chars = 0;
      lines.length = 1;
      const h = hermes();
      lines.push(`# start: hermes stats keys: ${Object.keys(h).join(',') || 'none'}`);
    },
    async progress(applied: number, done: boolean) {
      if (applied < next && !done) return;
      while (next <= applied) next += 100_000;
      const wall = performance.now() - t0;
      const h = hermes();
      let walMb = -1;
      try { const w = await FileSystem.getInfoAsync(`${dbUri}-wal`); if (w.exists) walMb = w.size / 1e6; } catch { /* a nicety */ }
      const dWall = wall - last.wall; const dSql = sq.ms - last.sqlite;
      lines.push([
        applied, (wall / 1000).toFixed(1), dWall.toFixed(0), dSql.toFixed(0), (dWall - dSql).toFixed(0),
        h.js_gcTime ?? '', h.js_gcCPUTime ?? '', h.js_numGCs ?? '',
        ((h.js_heapSize ?? 0) / 1e6).toFixed(1), ((h.js_totalAllocatedBytes ?? 0) / 1e6).toFixed(0),
        walMb.toFixed(1), ((sq.chars - last.chars) / 1e6).toFixed(1),
      ].join('\t'));
      last = { wall, sqlite: sq.ms, chars: sq.chars };
      try { await FileSystem.writeAsStringAsync(out, lines.join('\n') + '\n'); } catch { /* next line retries */ }
    },
  };
}
