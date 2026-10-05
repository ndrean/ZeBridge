/// 10-airports on a phone through zb-client-ts (React Native): the same two things as the
/// web page, without a map.
///   * the AIRPORTS around a city are a question — `request('query._default.airports_near')`
///     answered by the DuckDB service on the hub; nothing is replicated for it;
///   * the FLIGHT is a row — `flights`, replicated into expo-sqlite and written with
///     `mutate`: two registers {v, t, w}, the departure and the arrival, merged with
///     `mergeRegisters` exactly as the web page and the Flutter app do.
/// Built for zebridge.eu: EXPO_PUBLIC_ZB_INVITE on the first run, EXPO_PUBLIC_ZB_NATS_URL
/// for a leaf node (wss://leaf.example.com:8443).
import { useEffect, useRef, useState } from 'react';
import { FlatList, Pressable, SafeAreaView, StyleSheet, Text, View } from 'react-native';
import { ZeBridge, NotEnrolled, mergeRegisters } from 'zb-client-ts';

// Expo inlines EXPO_PUBLIC_* at bundle time; `process` exists only for that.
declare const process: { env: Record<string, string | undefined> };

const BRIDGE_URL = process.env.EXPO_PUBLIC_ZB_BRIDGE_URL ?? 'https://bridge.zebridge.eu';
const NATS_URL = process.env.EXPO_PUBLIC_ZB_NATS_URL || undefined;
const INVITE = process.env.EXPO_PUBLIC_ZB_INVITE || undefined;
const RADIUS_KM = 100;
const LIMIT = 20;

/// The places to ask about: a map's centre, without the map.
const CITIES = [
  { name: 'Nantes', lat: 47.218, lng: -1.553 },
  { name: 'Paris', lat: 48.857, lng: 2.352 },
  { name: 'Frankfurt', lat: 50.11, lng: 8.682 },
  { name: 'London', lat: 51.507, lng: -0.128 },
  { name: 'New York', lat: 40.713, lng: -74.006 },
];

type Airport = { code: string; name: string; lat: number; lng: number; km: number };
type End = 'origin' | 'destination';
type Register = { v: Omit<Airport, 'km'>; t: string; w: string };
type Doc = Partial<Record<End, Register>>;
const LABEL: Record<End, string> = { origin: 'departure', destination: 'arrival' };

/// One replica and identity per bridge, and per NATS server when one is named (a leaf),
/// as the Flutter app: an identity enrolled before a JetStream domain existed has none.
const host = (u: string) => u.replace(/^[a-z]+:\/\//, '').split(/[:/]/)[0];
const DB_PATH = `airports-${host(BRIDGE_URL)}${NATS_URL ? `-${host(NATS_URL)}` : ''}.sqlite3`;

export default function App() {
  const zbRef = useRef<ZeBridge | null>(null);
  const [status, setStatus] = useState('connecting…');
  const [city, setCity] = useState(CITIES[0]);
  const [airports, setAirports] = useState<Airport[]>([]);
  const [count, setCount] = useState('');
  const [timing, setTiming] = useState('');
  const [doc, setDoc] = useState<Doc>({});
  const [notes, setNotes] = useState<string[]>([]);

  // The flight's state outside React's render cycle: the change callback reads it.
  const flight = useRef({ id: '', doc: {} as Doc, rowExists: false, mine: {} as Doc, rounds: 0 });
  const asked = useRef(0);
  const say = (s: string) => setNotes((n) => [s, ...n].slice(0, 6));

  async function ask(c = city) {
    const zb = zbRef.current;
    if (!zb) return;
    const mine = ++asked.current;
    const t0 = Date.now();
    try {
      const a = await zb.request('query._default.airports_near', { lat: c.lat, lng: c.lng, radius_km: RADIUS_KM, limit: LIMIT });
      if (mine !== asked.current) return;
      if (a.error) { setCount(`the service refused: ${a.error}`); return; }
      const col = (n: string) => a.columns.indexOf(n);
      setAirports(a.rows.map((r: any[]) => ({
        code: r[col('code')], name: r[col('name')], lat: Number(r[col('latitude')]), lng: Number(r[col('longitude')]), km: Number(r[col('distance_km')]),
      })));
      setCount(`${a.count}${a.complete ? '' : '+'} airport${a.count === 1 ? '' : 's'} within ${RADIUS_KM} km of ${c.name}`);
      setTiming(`${a.ms} ms in the service, ${Date.now() - t0} ms round trip`);
    } catch (e) {
      if (mine === asked.current) setCount(`no answer: ${(e as Error).message}`);
    }
  }

  async function readFlight() {
    const zb = zbRef.current!;
    const f = flight.current;
    const r = (await zb.query('SELECT doc FROM flights WHERE id = ?', f.id))[0];
    const next: Doc = r ? (typeof r.doc === 'string' ? JSON.parse(r.doc) : (r.doc ?? {})) : {};
    f.rowExists = !!r;
    for (const end of ['origin', 'destination'] as End[]) {
      const was = f.doc[end], now = next[end];
      if (!now || now.t === was?.t) continue;
      const m = f.mine[end];
      if (now.w !== zb.principal) {
        if (m && now.t > m.t) say(`${now.w}'s ${now.v.code} came after your ${m.v.code}: the ${LABEL[end]} is ${now.v.code}`);
        else say(`${now.w} set the ${LABEL[end]} to ${now.v.code}`);
      }
      if (m && now.t >= m.t) delete f.mine[end]; // the row holds mine, or something later
    }
    f.doc = next;
    setDoc({ ...mergeRegisters(next as any, f.mine as any) } as Doc);
  }

  async function writeFlight() {
    const zb = zbRef.current!;
    const f = flight.current;
    const merged = mergeRegisters(f.doc as any, f.mine as any);
    if (f.rowExists) await zb.mutate('flights', 'UPDATE', { id: f.id }, { doc: merged });
    else await zb.mutate('flights', 'INSERT', { id: f.id }, { tenant_id: zb.tenant, doc: merged });
  }

  function setEnd(end: End, ap: Airport) {
    const zb = zbRef.current;
    if (!zb) return;
    const f = flight.current;
    const { km: _km, ...v } = ap;
    f.mine[end] = { v, t: zb.stamp(), w: zb.principal! };
    f.rounds = 0;
    setDoc({ ...mergeRegisters(f.doc as any, f.mine as any) } as Doc);
    void writeFlight().catch((e) => say(`write: ${(e as Error).message}`));
  }

  useEffect(() => {
    const zb = new ZeBridge({ bridgeUrl: BRIDGE_URL, invite: INVITE, natsUrl: NATS_URL, dbPath: DB_PATH, tables: ['flights'] });
    zbRef.current = zb;
    // The row moved, by me or by someone else: redraw from it, then write the merge
    // again while the row does not hold what this phone wrote.
    zb.onChange('flights', () => {
      void (async () => {
        await readFlight();
        const f = flight.current;
        if (!Object.keys(f.mine).length || f.rounds >= 10) return;
        const merged = mergeRegisters(f.doc as any, f.mine as any);
        if (JSON.stringify(merged) !== JSON.stringify(f.doc)) {
          f.rounds += 1;
          await writeFlight();
        }
      })();
    });
    (async () => {
      try {
        await zb.connect();
      } catch (e) {
        setStatus(e instanceof NotEnrolled
          ? 'Not enrolled: build with EXPO_PUBLIC_ZB_INVITE=<code> for the first run.'
          : `Could not connect: ${(e as Error).message}`);
        return;
      }
      setStatus(`${zb.principal} · ${zb.tenant}${NATS_URL ? ` · via ${host(NATS_URL)}` : ''}`);
      flight.current.id = `flight-${zb.tenant}`;
      await readFlight();
      await ask(CITIES[0]);
    })();
    return () => { void zb.close(); };
  }, []);

  const pending = (end: End) => !!flight.current.mine[end];
  const end = (e: End) => {
    const r = doc[e];
    return r ? `${r.v.code}${pending(e) ? ' (sending…)' : ''}  ·  ${r.w}` : '—';
  };

  return (
    <SafeAreaView style={s.root}>
      <Text style={s.title}>Airports and a shared flight</Text>
      <Text style={s.status}>{status}</Text>

      <View style={s.flight}>
        <Text style={s.flightLine}><Text style={s.dep}>Departure </Text>{end('origin')}</Text>
        <Text style={s.flightLine}><Text style={s.arr}>Arrival   </Text>{end('destination')}</Text>
        {notes.map((n, i) => <Text key={i} style={s.note}>{n}</Text>)}
      </View>

      <View style={s.cities}>
        {CITIES.map((c) => (
          <Pressable key={c.name} onPress={() => { setCity(c); void ask(c); }} style={[s.city, c.name === city.name && s.cityOn]}>
            <Text style={[s.cityText, c.name === city.name && s.cityTextOn]}>{c.name}</Text>
          </Pressable>
        ))}
      </View>
      <Text style={s.count}>{count}</Text>
      <Text style={s.timing}>{timing}</Text>

      <FlatList
        data={airports}
        keyExtractor={(a) => a.code}
        renderItem={({ item }) => (
          <View style={s.row}>
            <Text style={s.rowText} numberOfLines={1}><Text style={s.code}>{item.code}</Text>  {item.name} · {item.km} km</Text>
            <Pressable onPress={() => setEnd('origin', item)} style={[s.btn, s.btnDep]}><Text style={s.btnText}>Dep</Text></Pressable>
            <Pressable onPress={() => setEnd('destination', item)} style={[s.btn, s.btnArr]}><Text style={s.btnText}>Arr</Text></Pressable>
          </View>
        )}
      />
    </SafeAreaView>
  );
}

const s = StyleSheet.create({
  root: { flex: 1, backgroundColor: '#fff', paddingHorizontal: 16, paddingTop: 40 },
  title: { fontSize: 20, fontWeight: '600' },
  status: { color: '#666', marginTop: 4 },
  flight: { marginTop: 14, padding: 10, borderRadius: 8, backgroundColor: '#f4f4f6' },
  flightLine: { fontSize: 16, marginVertical: 2 },
  dep: { color: '#1e8e3e', fontWeight: '600' },
  arr: { color: '#7b3fbf', fontWeight: '600' },
  note: { color: '#555', fontSize: 12, marginTop: 2 },
  cities: { flexDirection: 'row', flexWrap: 'wrap', marginTop: 14 },
  city: { paddingVertical: 6, paddingHorizontal: 10, borderRadius: 14, borderWidth: 1, borderColor: '#ccc', marginRight: 6, marginBottom: 6 },
  cityOn: { backgroundColor: '#222', borderColor: '#222' },
  cityText: { color: '#222' },
  cityTextOn: { color: '#fff' },
  count: { marginTop: 8, fontWeight: '500' },
  timing: { color: '#666', marginBottom: 6 },
  row: { flexDirection: 'row', alignItems: 'center', paddingVertical: 6, borderBottomWidth: StyleSheet.hairlineWidth, borderColor: '#ddd' },
  rowText: { flex: 1 },
  code: { fontWeight: '700' },
  btn: { paddingVertical: 4, paddingHorizontal: 8, borderRadius: 6, marginLeft: 6 },
  btnDep: { backgroundColor: '#1e8e3e' },
  btnArr: { backgroundColor: '#7b3fbf' },
  btnText: { color: '#fff', fontWeight: '600' },
});
