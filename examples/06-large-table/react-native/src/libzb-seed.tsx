/// The seed through libzb — the C client, Zig inside — from the zb-react-native package.
/// What the Flutter app does over dart:ffi, from JavaScript: `connect`, then one `sync`
/// that streams the chain and applies it in Zig. JS only keeps the clock and asks for the
/// three facts. libzb reports nothing while `sync` runs, so there is no bar. Once usable it
/// keeps following: `poll` in a loop (§10jc), the way a host drives libzb — the harness
/// reads its consumer from nats-server.
import { useEffect, useRef, useState } from 'react';
import { Pressable, ScrollView, StyleSheet, Text, View } from 'react-native';
import * as FileSystem from 'expo-file-system';
import { Libzb, ZbNative, libzbAvailable } from '@zebridge/react-native';
import { fileLog } from './app-log';

// Expo inlines EXPO_PUBLIC_* at bundle time. The first run enrolls with the invite; libzb
// keeps the identity beside the replica, and later runs need neither.
const BRIDGE_URL = process.env.EXPO_PUBLIC_ZB_BRIDGE_URL ?? 'https://bridge.zebridge.eu';
/// Another NATS address than the one the bridge names, such as a leaf.
const NATS_URL = process.env.EXPO_PUBLIC_ZB_NATS_URL || undefined;
const INVITE = process.env.EXPO_PUBLIC_ZB_INVITE || undefined;
/// test_types: 3,055,002 rows on tenant globex, the firehose fixture.
const TABLE = process.env.EXPO_PUBLIC_ZB_TABLE ?? 'test_types';
/// One replica and identity per bridge, and per NATS server when one is named (a leaf).
const host = (u: string) => u.replace(/^[a-z]+:\/\//, '').split(/[:/]/)[0];
const DB_FILE = `largetable-${host(BRIDGE_URL)}${NATS_URL ? `-${host(NATS_URL)}` : ''}.sqlite3`;

// A missing number (an empty table's sum is NULL) is "—", never a throw: an uncaught
// error in a render is a native abort in a Release build (§10jc).
const fmt = (n: number | undefined | null) => (n == null || Number.isNaN(n) ? '—' : n.toLocaleString('en-US'));
const secs = (ms: number) => `${(ms / 1000).toFixed(1)} s`;
type Facts = { count: number; distinct?: number; sum: number; dbBytes?: number };

export function LibzbSeed() {
  const clientRef = useRef<Libzb | null>(null);
  const [elapsed, setElapsed] = useState(0);
  const [running, setRunning] = useState(false);
  const [phase, setPhase] = useState('starting');
  const [who, setWho] = useState('');
  const [facts, setFacts] = useState<Facts | null>(null);
  const [lines, setLines] = useState<{ text: string; err: boolean }[]>([]);
  const [busy, setBusy] = useState(false);
  const [gen, setGen] = useState(0);
  const [follow, setFollow] = useState<{ applied: number; polls: number; errors: number } | null>(null);
  const t0Ref = useRef(0);

  /// count and sum now (the table's DISTINCT only on the fixture: it takes minutes on 3M rows).
  const check = async () => {
    const zb = clientRef.current; if (!zb) return;
    const q = await zb.query(TABLE === 'test_types'
      ? `SELECT count(*), count(DISTINCT uid), sum(age) FROM ${TABLE}`
      : `SELECT count(*), NULL, sum(age) FROM ${TABLE}`);
    const [count, distinct, sum] = q.rows[0].map((v: unknown) => (v == null ? undefined : Number(v)));
    // The size from SQLite itself: expo-file-system's getInfoAsync MD5s the whole file
    // (it tests for the `md5` KEY, which is always passed), and a 1.8 GB replica read into
    // memory got the app killed by iOS at 2.1 GB — at every launch (§10jc).
    const bytes = await zb.query(`SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()`);
    setFacts({ count: count!, distinct, sum: sum!, dbBytes: Number(bytes.rows[0][0]) });
  };

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
    log(`— start (${TABLE}, ${host(BRIDGE_URL)})`);
    (async () => {
      if (!libzbAvailable) { setPhase('libzb is not built into this app — run zb-react-native/scripts/build-ios.sh or build-android.sh, then rebuild'); return; }
      // libzb's own lines (seeded, gap healed, seed anchor, one per fetch and batch with the
      // peak RSS) appended to Documents/libzb-stderr.log — the phone has no terminal.
      if (process.env.EXPO_PUBLIC_ZB_TRACE === '1') {
        ZbNative?.captureStderr(dir.replace(/^file:\/\//, '') + 'libzb-stderr.log', true);
      }
      // A directory listing, not getInfoAsync: that one MD5s the file (see `check`).
      const fresh = !(await FileSystem.readDirectoryAsync(dir)).includes(DB_FILE);
      log(fresh ? 'fresh replica — the seed is the whole table' : 'replica present — no seed unless the chain moved');
      log(`connecting through ${BRIDGE_URL}${NATS_URL ? ` via ${NATS_URL}` : ''}, following [${TABLE}]`);
      t0Ref.current = Date.now(); setElapsed(0); setRunning(true); setPhase('connect + seed');
      const zb = await Libzb.connect({
        bridgeUrl: BRIDGE_URL, invite: INVITE, natsUrl: NATS_URL, dbPath,
        tables: [TABLE], seedStreaming: true,
      });
      if (closed) { await zb.close(); return; }
      clientRef.current = zb;
      // The first sync is schema, seed and positions: the clock stops at "usable".
      const report = await zb.sync();
      setRunning(false);
      const ms = Date.now() - t0Ref.current;
      const failed = (report.unseeded ?? []).filter((u: { table: string }) => u.table === TABLE);
      if (failed.length) { setPhase('failed'); log(`not seeded after ${secs(ms)}: ${failed[0].reason}`, true); return; }
      setPhase('usable');
      setWho(report.principal ?? '');
      log(`usable after ${secs(ms)} (${report.principal ?? '—'} on ${report.tenant ?? '—'})`);
      await check();
      // §10jc: follow — one poll at a time, a second of wait each; a re-seed or an error
      // is logged, the loop goes on until the tab closes or wipes.
      let applied = 0, polls = 0, errors = 0;
      while (!closed && clientRef.current === zb) {
        const r = await zb.poll(1000);
        polls++;
        applied += r.applied ?? 0;
        if (r.error) { errors++; log(`poll: ${r.error}`, true); }
        if (r.seeded?.length) log(`re-seeded: ${r.seeded.join(', ')}`);
        if (polls % 2 === 0 || r.error) setFollow({ applied, polls, errors });
      }
    })().catch((e) => { setRunning(false); setPhase('failed'); log(String(e?.message ?? e), true); });
    return () => {
      closed = true;
      const zb = clientRef.current; clientRef.current = null;
      if (zb) void zb.close();
    };
  }, [gen]);

  const wipe = async () => {
    setBusy(true);
    try {
      const zb = clientRef.current; clientRef.current = null;
      // libzb runs one call at a time per module (a serial queue): a close waits for the
      // running query — the facts' count(DISTINCT uid) over 3M rows is ~2 minutes here.
      if (zb) { fileLog('libzb', 'wipe: closing after the running query'); setLines((prev) => [...prev, { text: 'wipe: closing after the running query…', err: false }]); await zb.close(); }
      for (const suffix of ['', '-wal', '-shm', '-journal']) {
        await FileSystem.deleteAsync(dir + DB_FILE + suffix, { idempotent: true });
      }
    } finally { setBusy(false); }
    setGen((g) => g + 1);
  };

  return (
    <View style={{ flex: 1 }}>
      <Text style={s.sub}>
        {TABLE}{who ? ` as ${who}` : ''}, seeded by libzb (Zig) through a native module: streamed, applied in C. JS keeps the
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
          {facts.distinct !== undefined && <Fact k="distinct uid" v={fmt(facts.distinct)} />}
          <Fact k="sum(age)" v={fmt(facts.sum)} />
          <Fact k="connect → usable" v={secs(elapsed)} />
          {facts.dbBytes !== undefined && <Fact k="replica" v={`${(facts.dbBytes / 1e9).toFixed(2)} GB`} />}
        </View>
      )}
      {follow && (
        <Text style={s.detail}>following: {fmt(follow.applied)} event(s) applied in {fmt(follow.polls)} poll(s){follow.errors ? `, ${follow.errors} error(s)` : ''}</Text>
      )}
      <Pressable style={[s.button, busy && s.buttonOff]} onPress={wipe} disabled={busy}>
        <Text style={s.buttonText}>wipe & seed again</Text>
      </Pressable>
      <Pressable style={[s.button, (busy || !facts) && s.buttonOff]} disabled={busy || !facts}
        onPress={() => void check().catch((e) => fileLog('libzb', `check failed: ${e}`, true))}>
        <Text style={s.buttonText}>check (count, sum)</Text>
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
