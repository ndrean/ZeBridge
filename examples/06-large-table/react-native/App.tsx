/// One table, one bar, one clock — on a phone. The SEED of a big table from the
/// generation chain through zb-client-ts (NOTES §10ix): staged as it arrives into a
/// TEMP table on the device's filesystem, sorted once by SQLite into the real table.
/// Nothing else — no mutation. The three facts at the end (rows, distinct keys, a sum)
/// are the ones the Node, libzb and browser seeds are checked with against PostgreSQL,
/// so a run here is a measurement. The toggle at the top runs the same seed through
/// libzb instead (src/libzb-seed.tsx, a native module): same phone, same table, the
/// engine is the only difference.
import { useEffect, useRef, useState } from 'react';
import { Pressable, ScrollView, StyleSheet, Text, View } from 'react-native';
import * as FileSystem from 'expo-file-system';
import { defaultDatabaseDirectory } from 'expo-sqlite';
import type { ConnStatus, Phase, SeedProgress, ZeBridge } from 'zb-client-ts';
import { makeClient, PRINCIPAL, TABLE } from './src/client';
import { LibzbSeed } from './src/libzb-seed';
import { makeRecorder, TRACE } from './src/seed-trace';

const fmt = (n: number) => n.toLocaleString('en-US');
const secs = (ms: number) => `${(ms / 1000).toFixed(1)} s`;
type Line = { text: string; err: boolean };
type Facts = { count: number; distinct?: number; sum?: number; dbBytes?: number };
const PHASES: [Phase, string][] = [['connected', 'NATS'], ['migrated', 'schema'], ['snapshot', 'snapshot'], ['cdc', 'CDC']];

type Engine = 'ts' | 'libzb';

export default function App() {
  const [engine, setEngine] = useState<Engine>(process.env.EXPO_PUBLIC_ZB_ENGINE === 'libzb' ? 'libzb' : 'ts');
  return (
    <View style={s.root}>
      <View style={s.engines}>
        {(['ts', 'libzb'] as const).map((e) => (
          <Pressable key={e} onPress={() => setEngine(e)} style={[s.engine, engine === e && s.engineOn]}>
            <Text style={[s.engineText, engine === e && s.engineTextOn]}>{e === 'ts' ? 'zb-client-ts' : 'libzb'}</Text>
          </Pressable>
        ))}
      </View>
      {engine === 'ts' ? <TsSeed /> : <LibzbSeed />}
    </View>
  );
}

function TsSeed() {
  const zbRef = useRef<ZeBridge | null>(null);
  const [status, setStatus] = useState<ConnStatus>('disconnected');
  const [phase, setPhase] = useState<Record<Phase, boolean>>({ connected: false, migrated: false, snapshot: false, cdc: false });
  const [progress, setProgress] = useState<SeedProgress | null>(null);
  const [elapsed, setElapsed] = useState(0);
  const [seedMs, setSeedMs] = useState<number | null>(null);
  const [facts, setFacts] = useState<Facts | null>(null);
  const [lines, setLines] = useState<Line[]>([]);
  const [busy, setBusy] = useState(false);
  const [gen, setGen] = useState(0); // bumps to start over after a wipe
  /// The clock is STATE, not a closure: `running` owns the interval through its own
  /// effect, so stopping it cannot depend on which callback instance holds the handle.
  /// (The first version kept the handle in the connect effect's closure and stopped it
  /// from the `cdc` phase handler — on Android it never stopped, 2026-09-24.)
  const [running, setRunning] = useState(false);
  const t0Ref = useRef(0);
  useEffect(() => {
    if (!running) return;
    const id = setInterval(() => setElapsed(Date.now() - t0Ref.current), 100);
    return () => { clearInterval(id); setElapsed(Date.now() - t0Ref.current); };
  }, [running]);

  useEffect(() => {
    let zb: ZeBridge;
    try { zb = makeClient(); } catch (e) { setLines([{ text: String(e), err: true }]); return; }
    zbRef.current = zb;
    setStatus('disconnected'); setPhase({ connected: false, migrated: false, snapshot: false, cdc: false });
    setProgress(null); setSeedMs(null); setFacts(null); setLines([]);

    /// The clock: from `connect()` until the table is usable (phase `cdc`), ticked every
    /// 100 ms. The seed span alone — first window to `done` — is reported separately.
    t0Ref.current = Date.now();
    setElapsed(0);
    setRunning(true);
    let seedT0 = 0;
    const dbDir = defaultDatabaseDirectory.startsWith('file://') ? defaultDatabaseDirectory : `file://${defaultDatabaseDirectory}`;
    const rec = TRACE ? makeRecorder(`${dbDir}/${zb.dbName}`) : null;
    const stopClock = () => setRunning(false);
    const log = (text: string, err = false) => setLines((prev) => [...prev.slice(-199), { text, err }]);

    /// After the table is usable: the three facts (`SELECT count(*), count(DISTINCT uid),
    /// sum(age) FROM test_types` on the tenant — 3,055,002 / 3,055,002 / 138,916,285 on
    /// 2026-09-24) and the size of the replica file.
    const measure = async () => {
      const [row] = TABLE === 'test_types'
        ? await zb.query('SELECT count(*) AS count, count(DISTINCT uid) AS "distinct", sum(age) AS sum FROM test_types')
        : await zb.query(`SELECT count(*) AS count FROM ${TABLE}`);
      let dbBytes: number | undefined;
      try {
        // Android's expo-file-system wants a file:// URI; iOS takes either.
        const dir = defaultDatabaseDirectory.startsWith('file://') ? defaultDatabaseDirectory : `file://${defaultDatabaseDirectory}`;
        const info = await FileSystem.getInfoAsync(`${dir}/${zb.dbName}`);
        if (info.exists) dbBytes = info.size;
      } catch { /* the size is a nicety */ }
      setFacts({ count: Number(row.count), distinct: row.distinct != null ? Number(row.distinct) : undefined, sum: row.sum != null ? Number(row.sum) : undefined, dbBytes });
    };

    const offs = [
      zb.onStatus(setStatus),
      zb.onPhase((p) => {
        setPhase((prev) => ({ ...prev, [p]: true }));
        if (p === 'cdc') { stopClock(); void measure().catch((e) => log(`measure failed: ${e}`, true)); }
      }),
      zb.onSeedProgress((p) => {
        if (p.table !== TABLE) return;
        if (!seedT0) { seedT0 = Date.now(); rec?.start(); }
        void rec?.progress(p.applied, p.done);
        setProgress(p);
        if (p.done) setSeedMs(Date.now() - seedT0);
      }),
      zb.onLog((topic, data, level) => {
        if (topic !== 'SYS') return;
        log(`[${level}] ${typeof data === 'string' ? data : JSON.stringify(data)}`, level === 'ERROR');
      }),
    ];
    zb.connect().catch((e) => log(`connect failed: ${e}`, true));
    return () => { stopClock(); for (const off of offs) off(); void zb.close(); };
  }, [gen]);

  const wipe = async () => {
    const zb = zbRef.current; if (!zb) return;
    setBusy(true);
    try { await zb.wipe(); } finally { setBusy(false); }
    setGen((g) => g + 1);
  };

  const pct = progress && progress.total ? progress.applied / progress.total : 0;
  const detail = progress
    ? `${fmt(progress.applied)} / ${fmt(progress.total)} rows (${Math.round(pct * 100)}%) · ${progress.kind} ${progress.step}${progress.done ? ' · done' : ''}`
    : phase.cdc ? 'no seed needed — the table was already in the replica' : 'waiting for the first window…';

  return (
    <View style={{ flex: 1 }}>
      <View style={s.head}>
        <Text style={s.title}>ZeBridge — one large table</Text>
        <Text style={[s.badge, s[status]]}>{status}</Text>
      </View>
      <Text style={s.sub}>
        {TABLE} as {PRINCIPAL}, seeded from the generation chain into SQLite: staged as it arrives, sorted once by SQLite
        (bounded memory — hence the bar; the last stretch at 100% is the sort). One database, kept across launches.
      </Text>

      <View style={s.phases}>
        {PHASES.map(([key, label]) => (
          <View key={key} style={[s.phase, phase[key] && s.phaseDone]}><Text style={[s.phaseText, phase[key] && s.phaseTextDone]}>{label}</Text></View>
        ))}
      </View>

      <View style={s.seed}>
        <View style={s.row}>
          <View style={s.track}><View style={[s.fill, { width: `${Math.round(pct * 100)}%` }]} /></View>
          <Text style={s.clock}>{secs(elapsed)}</Text>
        </View>
        <Text style={s.detail}>{detail}</Text>
        {seedMs !== null && progress && (
          <Text style={s.detail}>seed {secs(seedMs)} · {fmt(Math.round(progress.total / (seedMs / 1000)))} rows/s</Text>
        )}
      </View>

      {facts && (
        <View style={s.facts}>
          <Fact k="rows" v={fmt(facts.count)} />
          {facts.distinct !== undefined && <Fact k="distinct uid" v={fmt(facts.distinct)} />}
          {facts.sum !== undefined && <Fact k="sum(age)" v={fmt(facts.sum)} />}
          <Fact k="connect → usable" v={secs(elapsed)} />
          {facts.dbBytes !== undefined && <Fact k="replica" v={`${(facts.dbBytes / 1e9).toFixed(2)} GB`} />}
        </View>
      )}

      <Pressable style={[s.button, busy && s.buttonOff]} onPress={wipe} disabled={busy}>
        <Text style={s.buttonText}>wipe & seed again</Text>
      </Pressable>

      <ScrollView style={s.log}>
        {lines.map((l, i) => <Text key={i} style={[s.logLine, l.err && s.logErr]}>{l.text}</Text>)}
      </ScrollView>
    </View>
  );
}

const Fact = ({ k, v }: { k: string; v: string }) => (
  <View style={s.fact}><Text style={s.factK}>{k}</Text><Text style={s.factV}>{v}</Text></View>
);

const s = StyleSheet.create({
  root: { flex: 1, backgroundColor: '#121212', paddingTop: 56, paddingHorizontal: 14 },
  engines: { flexDirection: 'row', marginBottom: 10 },
  engine: { flex: 1, paddingVertical: 6, alignItems: 'center', backgroundColor: '#2b2b2b', borderWidth: 1, borderColor: '#3a3a3a' },
  engineOn: { backgroundColor: '#0d47a1' },
  engineText: { color: '#888', fontSize: 12 },
  engineTextOn: { color: '#e3f2fd', fontWeight: '700' },
  head: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  title: { color: '#e0e0e0', fontSize: 18, fontWeight: '700' },
  badge: { paddingHorizontal: 7, paddingVertical: 2, borderRadius: 4, fontSize: 12, fontWeight: '700', overflow: 'hidden' },
  connected: { backgroundColor: '#1b5e20', color: '#81c784' },
  disconnected: { backgroundColor: '#b71c1c', color: '#ef9a9a' },
  connecting: { backgroundColor: '#e65100', color: '#ffcc80' },
  sub: { color: '#999', fontSize: 12, marginTop: 4, marginBottom: 10 },
  phases: { flexDirection: 'row' },
  phase: { flex: 1, paddingVertical: 6, alignItems: 'center', backgroundColor: '#2b2b2b', borderWidth: 1, borderColor: '#3a3a3a' },
  phaseDone: { backgroundColor: '#1b5e20' },
  phaseText: { color: '#888', fontSize: 12 },
  phaseTextDone: { color: '#d7ffd9' },
  seed: { marginVertical: 14, padding: 12, backgroundColor: '#1c1c1c', borderWidth: 1, borderColor: '#333', borderRadius: 6 },
  row: { flexDirection: 'row', alignItems: 'center', gap: 10 },
  track: { flex: 1, height: 14, backgroundColor: '#2b2b2b', borderRadius: 7, overflow: 'hidden' },
  fill: { height: 14, backgroundColor: '#43a047' },
  clock: { color: '#e0e0e0', fontFamily: 'Menlo', fontSize: 22, minWidth: 80, textAlign: 'right' },
  detail: { color: '#aaa', fontFamily: 'Menlo', fontSize: 11, marginTop: 6 },
  facts: { marginBottom: 10 },
  fact: { flexDirection: 'row', paddingVertical: 2 },
  factK: { color: '#888', fontFamily: 'Menlo', fontSize: 12, width: 150 },
  factV: { color: '#ccc', fontFamily: 'Menlo', fontSize: 12 },
  button: { alignSelf: 'flex-start', backgroundColor: '#263238', borderWidth: 1, borderColor: '#455a64', borderRadius: 4, paddingHorizontal: 12, paddingVertical: 6, marginBottom: 10 },
  buttonOff: { opacity: 0.4 },
  buttonText: { color: '#eceff1', fontSize: 13 },
  log: { flex: 1, backgroundColor: '#0d0d0d', borderWidth: 1, borderColor: '#2a2a2a', padding: 8 },
  logLine: { color: '#9e9e9e', fontFamily: 'Menlo', fontSize: 10 },
  logErr: { color: '#ef9a9a' },
});
