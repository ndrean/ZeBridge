/// ZeMap on a phone: the SAME service, the SAME queries and the SAME shared route row as
/// `../flutter` and `../web`, through zb-client-ts — nothing native of ours is compiled
/// (§10ih). The markers are the ANSWER and are never stored (§10ic); the route is one
/// `routes` row two clients edit at once, converging through `mergeRegisters` (§10ho).
import { useCallback, useEffect, useRef, useState } from 'react';
import { Pressable, StyleSheet, Text, View } from 'react-native';
import MapLibreGL, { Camera, CircleLayer, LineLayer, MapView, MarkerView, ShapeSource, SymbolLayer } from '@maplibre/maplibre-react-native';
import { mergeRegisters } from 'zb-client-ts';
import { makeClient, PRINCIPAL, TENANT } from './src/client';

MapLibreGL.setAccessToken(null);

/// OpenStreetMap raster, the same as `../web`: no account, no API key. MapLibre can read
/// the R2 vector archive the Flutter app uses; that is the upgrade when this app owns a
/// style sheet.
const STYLE = {
  version: 8,
  sources: { osm: { type: 'raster', tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'], tileSize: 256 } },
  layers: [{ id: 'osm', type: 'raster', source: 'osm' }],
} as const;

const NANTES: [number, number] = [-1.5536, 47.2184];
const ROUTE_ID = '11111111-1111-4111-8111-111111111111';
const FUELS = ['SP95', 'Gazole', 'E10', 'SP98', 'E85', 'GPLc'] as const;

type Row = Record<string, any>;
const asMaps = (ans: any): Row[] => {
  const cols: string[] = ans?.columns ?? [];
  return (ans?.rows ?? []).map((r: any[]) => Object.fromEntries(cols.map((c, i) => [c, r[i]])));
};
const stamp = () => new Date().toISOString().replace('Z', '000Z');

/// §10il: rows as ONE GeoJSON collection for the map to draw in a single layer.
///
/// ⚠️ `MarkerView` was the wrong tool and cost us markers. It mounts a React view per
/// point, and the library's own docs say so: "If you have static view consider using
/// PointAnnotation or SymbolLayer they'll offer much better performance". With dozens
/// on screen it silently drew only some of them — the answer said 77 and the map showed
/// a fraction, which is exactly what the browser's canvas circles never do.
const collection = (rows: Row[]): any => ({
  type: 'FeatureCollection',
  features: rows
    .filter((r) => Number.isFinite(Number(r.lng)) && Number.isFinite(Number(r.lat)))
    .map((r) => ({
      type: 'Feature',
      id: String(r.id),
      properties: {
        id: String(r.id),
        kw: Number(r.max_power_kw ?? 0),
        dead: r.status_type_id != null && r.status_type_id !== 50 ? 1 : 0,
        price: r.price != null ? Number(r.price).toFixed(3) : '',
      },
      geometry: { type: 'Point', coordinates: [Number(r.lng), Number(r.lat)] },
    })),
});

/// Metres between two points. The radius each ask uses is half the visible diagonal,
/// exactly as the other two clients compute it — so the same viewport asks for the same
/// area on all three.
const metres = (a: [number, number], b: [number, number]) => {
  const p = Math.PI / 180;
  const h = Math.sin(((b[1] - a[1]) * p) / 2) ** 2 +
    Math.cos(a[1] * p) * Math.cos(b[1] * p) * Math.sin(((b[0] - a[0]) * p) / 2) ** 2;
  return 2 * 6371000 * Math.asin(Math.sqrt(h));
};

export default function App() {
  const zb = useRef<ReturnType<typeof makeClient> | null>(null);
  const map = useRef<any>(null);
  /// §10ik: every ask carries a ticket. A pan fires many region events, their answers
  /// come back out of order, and an older one landing last would draw the area you just
  /// left — "markers appear briefly and some other pop in, nothing centred". Only the
  /// newest ticket may touch state.
  const seq = useRef(0);
  const panTimer = useRef<number | null>(null);
  const [connected, setConnected] = useState(false);
  /// ⚠️ And the first ask must WAIT for this. `getVisibleBounds()` on a map that has not
  /// laid out yet answers a degenerate box, so the radius clamped to its 150 m floor and
  /// the map opened empty even though the toggle said the chargers were on.
  const [mapReady, setMapReady] = useState(false);
  const [status, setStatus] = useState('connecting…');
  const [chargers, setChargers] = useState<Row[]>([]);
  const [stations, setStations] = useState<Row[]>([]);
  const [minKw, setMinKw] = useState<number | null>(0);      // null = chargers off
  const [fuel, setFuel] = useState<string | null>(null);     // null = fuel off
  const [picked, setPicked] = useState<Row | null>(null);
  const [routeMode, setRouteMode] = useState(false);
  const [routeDoc, setRouteDoc] = useState<Record<string, any>>({});
  /// §10if: which end a tap moves. There is NO alternating — whatever a tap moves
  /// becomes held, so the same end moves as many times in a row as you like.
  const [held, setHeld] = useState<'start' | 'end' | null>(null);
  const mine = useRef<Record<string, any>>({});
  const routeReadPending = useRef<number | null>(null);
  const writer = `phone-${PRINCIPAL}`;

  useEffect(() => {
    let live = true;
    (async () => {
      try {
        const c = makeClient();
        // The library's own progress, surfaced through its `onLog` hook — subscribed
        // BEFORE connect, because everything interesting happens inside it.
        c.onLog((topic, data, level) => console.log(`[zb ${level}] ${topic}: ${typeof data === 'string' ? data : JSON.stringify(data)}`));
        await c.connect();
        if (!live) return;
        zb.current = c;
        setConnected(true);
        setStatus('connected');
        // The row moved — mine or another editor's. Redraw from what the ROW holds.
        // COALESCED: a seed's backlog fired this 185 times and produced 183 identical
        // queries. One read per burst is the same answer for a fraction of the work.
        c.onChange('routes', () => {
          if (routeReadPending.current) return;
          routeReadPending.current = setTimeout(() => {
            routeReadPending.current = null;
            void readRoute();
          }, 120) as unknown as number;
        });
        await readRoute();
      } catch (e) {
        if (live) setStatus(`connect: ${e}`);
      }
    })();
    return () => { live = false; void zb.current?.close(); };
  }, []);

  const readRoute = useCallback(async () => {
    const c = zb.current;
    if (!c) return;
    try {
      const r: any[] = await c.query('SELECT doc FROM routes WHERE id = ?', ROUTE_ID);
      const doc = r[0]?.doc;
      setRouteDoc(typeof doc === 'string' ? JSON.parse(doc) : (doc ?? {}));
    } catch { /* the table is not here yet */ }
  }, []);

  /// The union of this phone's own registers merged into the document it last saw —
  /// which is what makes two editors converge without either winning outright.
  const writeRoute = useCallback(async () => {
    const c = zb.current;
    if (!c || !Object.keys(mine.current).length) return;
    const merged = mergeRegisters(routeDoc as any, mine.current as any);
    await c.mutate('routes', 'UPDATE', { id: ROUTE_ID }, { doc: merged });
    setRouteDoc(merged);
  }, [routeDoc]);

  const ask = useCallback(async () => {
    const c = zb.current;
    const m = map.current;
    if (!c || !m) return;
    // NOT a re-entrancy guard. Refusing to start while one is in flight drops the LAST
    // viewport — the one the finger stopped on — which is the only one that matters.
    const mine = ++seq.current;
    try {
      const centre: [number, number] = await m.getCenter();
      const b = await m.getVisibleBounds();          // [[east, north], [west, south]]
      const half = metres(b[0] as [number, number], b[1] as [number, number]) / 2;
      // ⚠️ §10il: a map that has not finished laying out answers a DEGENERATE box, and
      // the radius then clamps to its 150 m floor — an ask that finds nothing and a map
      // that opens empty while the toggle says the chargers are on. Waiting for
      // `onDidFinishLoadingMap` was not enough on its own, so the bounds are checked
      // too: anything under a kilometre across at these zooms is the map not ready.
      if (!Number.isFinite(half) || half < 500) {
        setTimeout(() => void ask(), 250);
        return;
      }
      const t0 = Date.now();
      if (minKw !== null) {
        const radius = Math.min(150000, Math.max(150, half));
        const ans: any = await c.request(`query.${TENANT}.chargers_near`, {
          lat: centre[1], lng: centre[0], radius_m: Math.round(radius),
          ...(minKw > 0 ? { min_kw: minKw } : {}), limit: 2000,
        }, 20000);
        if (mine !== seq.current) return;   // a newer ask overtook this one
        setChargers(asMaps(ans));
        const t = ans.zb_transport ?? {};
        if (mine === seq.current) setStatus(`${ans.count} charger(s) in ${(radius / 1000).toFixed(0)} km · ${t.via} ${t.bytes} B · wire ${t.wire_ms} ms · db ${ans.ms} ms · ${Date.now() - t0} ms`);
      } else if (mine === seq.current) setChargers([]);
      if (fuel !== null) {
        const radius = Math.min(20000, Math.max(3000, half));
        const ans: any = await c.request(`query.${TENANT}.fuel_near`, {
          lat: centre[1], lng: centre[0], radius_m: Math.round(radius), fuel, sort: 'distance', limit: 40,
        }, 20000);
        if (mine !== seq.current) return;
        setStations(asMaps(ans));
      } else if (mine === seq.current) setStations([]);
    } catch (e) {
      if (mine === seq.current) setStatus(`ask: ${e}`);
    }
  }, [minKw, fuel]);

  /// A pan is many region events; ask once, when it settles. 350 ms is the Flutter
  /// app's debounce, so all three clients ask at the same cadence.
  const scheduleAsk = useCallback(() => {
    if (panTimer.current) clearTimeout(panTimer.current);
    panTimer.current = setTimeout(() => {
      panTimer.current = null;
      void ask();
    }, 350) as unknown as number;
  }, [ask]);

  // The first ask needs BOTH halves: a connected client and a laid-out map.
  useEffect(() => { if (connected && mapReady) void ask(); }, [connected, mapReady, minKw, fuel]);

  /// §10if: the held end, or the nearest — never an alternating turn.
  const target = (at: [number, number]): 'start' | 'end' => {
    if (held) return held;
    const s = routeDoc.start?.v, e = routeDoc.end?.v;
    if (typeof s?.lat !== 'number') return 'start';
    if (typeof e?.lat !== 'number') return 'end';
    return metres(at, [s.lng, s.lat]) <= metres(at, [e.lng, e.lat]) ? 'start' : 'end';
  };

  const onMapPress = async (f: any) => {
    if (!routeMode) { setPicked(null); return; }
    const [lng, lat] = f?.geometry?.coordinates ?? [];
    if (typeof lat !== 'number') return;
    const which = target([lng, lat]);
    mine.current[which] = { v: { lat, lng }, t: stamp(), w: writer };
    setHeld(which);
    try { await writeRoute(); } catch (e) { setStatus(`route: ${e}`); }
  };

  const pins = (['start', 'end'] as const)
    .map((k) => ({ k, v: routeDoc[k]?.v }))
    .filter((p) => typeof p.v?.lat === 'number') as { k: 'start' | 'end'; v: any }[];

  return (
    <View style={styles.fill}>
      <MapView ref={map} style={styles.fill} mapStyle={STYLE as any} onPress={onMapPress}
        onDidFinishLoadingMap={() => setMapReady(true)}
        onRegionDidChange={() => { setMapReady(true); scheduleAsk(); }}>
        <Camera defaultSettings={{ centerCoordinate: NANTES, zoomLevel: 11 }} />

        {pins.length === 2 && (
          <ShapeSource
            id="route"
            shape={{ type: 'Feature', properties: {}, geometry: { type: 'LineString', coordinates: pins.map((p) => [p.v.lng, p.v.lat]) } } as any}
          >
            {/* Dashed on purpose: it is the straight line between the pins, not a road
                (§10ie — Valhalla was dropped, and a solid stroke would claim otherwise). */}
            <LineLayer id="route-line" style={{ lineColor: '#1f6feb', lineWidth: 4, lineDasharray: [2, 2] }} />
          </ShapeSource>
        )}

        {chargers.length > 0 && (
          <ShapeSource
            id="chargers"
            shape={collection(chargers)}
            onPress={(e: any) => {
              const id = e?.features?.[0]?.properties?.id;
              setPicked(chargers.find((r) => String(r.id) === id) ?? null);
            }}
          >
            {/* Colour by power, grey when the feed says it is out of service — the same
                rule the other two clients paint by. */}
            <CircleLayer
              id="chargers-dot"
              style={{
                circleRadius: 6,
                circleStrokeWidth: 1,
                circleStrokeColor: '#ffffff',
                circleColor: [
                  'case',
                  ['==', ['get', 'dead'], 1], '#999999',
                  ['>=', ['get', 'kw'], 43], '#1a7f37',
                  '#1f6feb',
                ] as any,
              }}
            />
          </ShapeSource>
        )}

        {stations.length > 0 && (
          <ShapeSource
            id="stations"
            shape={collection(stations)}
            onPress={(e: any) => {
              const id = e?.features?.[0]?.properties?.id;
              setPicked(stations.find((r) => String(r.id) === id) ?? null);
            }}
          >
            <CircleLayer id="stations-dot" style={{ circleRadius: 9, circleColor: '#b45309', circleStrokeWidth: 1, circleStrokeColor: '#ffffff' }} />
            <SymbolLayer
              id="stations-price"
              style={{ textField: ['get', 'price'] as any, textSize: 9, textColor: '#ffffff', textAllowOverlap: true, textOffset: [0, 1.4] }}
            />
          </ShapeSource>
        )}

        {routeMode && pins.map((p) => (
          <MarkerView key={`r-${p.k}`} coordinate={[p.v.lng, p.v.lat]}>
            <Pressable hitSlop={12} onPress={() => setHeld(held === p.k ? null : p.k)}>
              <View style={[styles.routePin, held === p.k && styles.routePinHeld]}>
                <Text style={styles.routePinText}>{p.k === 'start' ? 'A' : 'B'}</Text>
              </View>
            </Pressable>
          </MarkerView>
        ))}
      </MapView>

      <View style={styles.controls}>
        <Pressable style={[styles.btn, minKw !== null && styles.btnOn]}
          onPress={() => setMinKw(minKw === null ? 0 : minKw === 0 ? 43 : minKw === 43 ? 150 : null)}>
          <Text style={styles.btnText}>{minKw === null ? 'chargers off' : minKw === 0 ? 'all kW' : `≥${minKw} kW`}</Text>
        </Pressable>
        <Pressable style={[styles.btn, fuel !== null && styles.btnOn]}
          onPress={() => setFuel(fuel === null ? FUELS[0] : FUELS[(FUELS.indexOf(fuel as any) + 1) % (FUELS.length + 1)] ?? null)}>
          <Text style={styles.btnText}>{fuel ?? 'fuel off'}</Text>
        </Pressable>
        <Pressable style={[styles.btn, routeMode && styles.btnOn]}
          onPress={() => { setRouteMode(!routeMode); setHeld(null); setPicked(null); }}>
          <Text style={styles.btnText}>{routeMode ? (held ? `holding ${held}` : 'route on') : 'route off'}</Text>
        </Pressable>
      </View>

      {picked && (
        <View style={styles.card}>
          <Text style={styles.cardTitle}>{String(picked.title ?? picked.address ?? picked.city ?? 'point')}</Text>
          <Text style={styles.cardBody}>
            {picked.max_power_kw != null
              ? `${picked.max_power_kw} kW · ${picked.points ?? '?'} point(s)${picked.status_type_id !== 50 ? ' · out of service' : ''}`
              : `${picked.price} € · ${Math.round(Number(picked.m ?? 0))} m${picked.outage ? ` · ${picked.outage} outage` : ''}`}
          </Text>
        </View>
      )}

      <View style={styles.bar}><Text style={styles.barText}>{status}</Text></View>
    </View>
  );
}

const styles = StyleSheet.create({
  fill: { flex: 1 },
  pin: { width: 14, height: 14, borderRadius: 7, borderWidth: 1, borderColor: '#fff' },
  pinPicked: { width: 20, height: 20, borderRadius: 10, borderWidth: 3 },
  rapid: { backgroundColor: '#1a7f37' },
  slow: { backgroundColor: '#1f6feb' },
  routePin: { width: 26, height: 26, borderRadius: 13, backgroundColor: '#fff', borderWidth: 3, borderColor: '#1f6feb', alignItems: 'center', justifyContent: 'center' },
  routePinHeld: { width: 34, height: 34, borderRadius: 17, borderWidth: 5 },
  routePinText: { color: '#1f6feb', fontWeight: 'bold', fontSize: 12 },
  price: { backgroundColor: '#b45309', paddingHorizontal: 5, paddingVertical: 2, borderRadius: 4, borderWidth: 1, borderColor: '#fff' },
  priceText: { color: '#fff', fontSize: 10, fontWeight: 'bold' },
  controls: { position: 'absolute', top: 60, left: 8, flexDirection: 'row', gap: 6 },
  btn: { backgroundColor: 'rgba(0,0,0,0.6)', paddingHorizontal: 10, paddingVertical: 7, borderRadius: 6 },
  btnOn: { backgroundColor: '#1f6feb' },
  btnText: { color: '#fff', fontSize: 12, fontWeight: '600' },
  card: { position: 'absolute', left: 8, right: 8, bottom: 44, backgroundColor: 'rgba(255,255,255,0.96)', padding: 10, borderRadius: 8 },
  cardTitle: { fontWeight: 'bold', fontSize: 13 },
  cardBody: { fontSize: 12, color: '#333', marginTop: 2 },
  bar: { position: 'absolute', left: 0, right: 0, bottom: 0, backgroundColor: 'rgba(0,0,0,0.7)', padding: 7 },
  barText: { color: '#fff', fontSize: 11 },
});
