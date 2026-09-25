/// The same seed through libzb — the C client, Zig inside — behind a native module
/// (modules/zb-native). What the Flutter app does over dart:ffi, from JavaScript:
/// `connect`, then one `sync` that streams the chain and applies it in Zig. JS only
/// keeps the clock and asks for the three facts. libzb reports nothing while `sync`
/// runs, so there is no bar.
import { useEffect, useRef, useState } from 'react';
import { Platform, Pressable, ScrollView, StyleSheet, Text, View } from 'react-native';
import * as FileSystem from 'expo-file-system';
import Zb from '../modules/zb-native';
import { PRINCIPAL, TABLE } from './client';
import { fileLog } from './app-log';

/// libzb speaks NATS over TCP (4222), not WebSocket: its own URL.
const HOST = Platform.OS === 'android' ? '10.0.2.2' : '127.0.0.1';
const NATS_URL = process.env.EXPO_PUBLIC_ZB_NATS_URL ?? `nats://${HOST}:4222`;
const CREDS = process.env.EXPO_PUBLIC_CREDS ?? '';
/// Its own file: the two engines never share a replica.
const DB_FILE = `zebridge_${PRINCIPAL}_libzb.sqlite3`;

const fmt = (n: number) => n.toLocaleString('en-US');
const secs = (ms: number) => `${(ms / 1000).toFixed(1)} s`;
type Facts = { count: number; distinct: number; sum: number; dbBytes?: number };

export function LibzbSeed() {
  const handleRef = useRef<string | null>(null);
  const [elapsed, setElapsed] = useState(0);
  const [running, setRunning] = useState(false);
  const [phase, setPhase] = useState('starting');
  const [facts, setFacts] = useState<Facts | null>(null);
  const [lines, setLines] = useState<{ text: string; err: boolean }[]>([]);
  const [busy, setBusy] = useState(false);
  const [gen, setGen] = useState(0);
  const t0Ref = useRef(0);

  useEffect(() => {
    if (!running) return;
    const id = setInterval(() => setElapsed(Date.now() - t0Ref.current), 100);
    return () => { clearInterval(id); setElapsed(Date.now() - t0Ref.current); };
  }, [running]);

  const dir = FileSystem.documentDirectory ?? '';
  const dbPath = dir.replace(/^file:\/\//, '') + DB_FILE;

  useEffect(() => {
    let closed = false;
    const log = (text: string, err = false) => { fileLog('libzb', text, err); setLines((prev) => [...prev.slice(-199), { text, err }]); };
    setFacts(null); setLines([]); setPhase('starting');
    log(`— start (${PRINCIPAL}, ${TABLE})`);
    (async () => {
      if (!CREDS) throw new Error("set EXPO_PUBLIC_CREDS to the principal's creds file contents");
      // libzb wants a creds FILE; the text is an app secret baked in at build time.
      const credsUri = `${dir}${PRINCIPAL}.creds`;
      await FileSystem.writeAsStringAsync(credsUri, CREDS);
      const fresh = !(await FileSystem.getInfoAsync(dir + DB_FILE)).exists;
      log(fresh ? 'fresh replica — the seed is the whole table' : 'replica present — no seed unless the chain moved');
      log(`connecting to ${NATS_URL} as ${PRINCIPAL}, following [${TABLE}]`);
      t0Ref.current = Date.now(); setElapsed(0); setRunning(true); setPhase('connect + seed');
      const h = await Zb.connect(JSON.stringify({
        natsUrl: NATS_URL, credsPath: credsUri.replace(/^file:\/\//, ''), dbPath,
        principal: PRINCIPAL, tables: [TABLE], seedStreaming: true,
      }));
      if (closed) { await Zb.close(h); return; }
      handleRef.current = h;
      // The first sync is schema, seed and positions: the clock stops at "usable".
      const report = JSON.parse(await Zb.sync(h));
      setRunning(false);
      const ms = Date.now() - t0Ref.current;
      const failed = (report.unseeded ?? []).filter((u: { table: string }) => u.table === TABLE);
      if (failed.length) { setPhase('failed'); log(`not seeded after ${secs(ms)}: ${failed[0].reason}`, true); return; }
      setPhase('usable');
      log(`usable after ${secs(ms)} (tenant ${report.tenant ?? '—'})`);
      const q = JSON.parse(await Zb.query(h, `SELECT count(*), count(DISTINCT uid), sum(age) FROM ${TABLE}`, '[]'));
      const [count, distinct, sum] = q.rows[0].map(Number);
      const info = await FileSystem.getInfoAsync(dir + DB_FILE);
      setFacts({ count, distinct, sum, dbBytes: info.exists ? info.size : undefined });
    })().catch((e) => { setRunning(false); setPhase('failed'); log(String(e?.message ?? e), true); });
    return () => {
      closed = true;
      const h = handleRef.current; handleRef.current = null;
      if (h) void Zb.close(h);
    };
  }, [gen]);

  const wipe = async () => {
    setBusy(true);
    try {
      const h = handleRef.current; handleRef.current = null;
      if (h) await Zb.close(h);
      for (const suffix of ['', '-wal', '-shm', '-journal']) {
        await FileSystem.deleteAsync(dir + DB_FILE + suffix, { idempotent: true });
      }
    } finally { setBusy(false); }
    setGen((g) => g + 1);
  };

  return (
    <View style={{ flex: 1 }}>
      <Text style={s.sub}>
        {TABLE} as {PRINCIPAL}, seeded by libzb (Zig) through a native module: streamed, applied in C. JS keeps the
        clock only. Its own database, kept across launches.
      </Text>
      <View style={s.seed}>
        <View style={s.row}>
          <Text style={[s.detail, { flex: 1 }]}>
            {phase === 'connect + seed' ? 'connecting + seeding…  (libzb reports nothing until it is done)' : phase}
          </Text>
          <Text style={s.clock}>{secs(elapsed)}</Text>
        </View>
      </View>
      {facts && (
        <View style={s.facts}>
          <Fact k="rows" v={fmt(facts.count)} />
          <Fact k="distinct uid" v={fmt(facts.distinct)} />
          <Fact k="sum(age)" v={fmt(facts.sum)} />
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
  sub: { color: '#999', fontSize: 12, marginTop: 4, marginBottom: 10 },
  seed: { marginVertical: 14, padding: 12, backgroundColor: '#1c1c1c', borderWidth: 1, borderColor: '#333', borderRadius: 6 },
  row: { flexDirection: 'row', alignItems: 'center', gap: 10 },
  clock: { color: '#e0e0e0', fontFamily: 'Menlo', fontSize: 22, minWidth: 80, textAlign: 'right' },
  detail: { color: '#aaa', fontFamily: 'Menlo', fontSize: 11 },
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
