/// ZeMap on a phone: the SAME service, the SAME queries, the SAME shared route row as
/// the browser and the desktop app — through zb-client-ts, so nothing native is
/// compiled (§10ih). The map is MapLibre reading the R2 vector archive the other two
/// already use; the markers are the ANSWER and are never stored (§10ic).
import { useCallback, useEffect, useRef, useState } from 'react';
import { StyleSheet, Text, View } from 'react-native';
import MapLibreGL, { Camera, MapView, MarkerView, ShapeSource, LineLayer } from '@maplibre/maplibre-react-native';
import { makeClient, TENANT } from './src/client';

MapLibreGL.setAccessToken(null);

/// The style the other two clients draw: OpenStreetMap raster, which needs no account
/// and no key. The R2 vector archive goes here when the app has a style sheet of its
/// own — the browser made the same choice for the same reason.
const STYLE = {
  version: 8,
  sources: {
    osm: {
      type: 'raster',
      tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
      tileSize: 256,
      attribution: '© OpenStreetMap contributors',
    },
  },
  layers: [{ id: 'osm', type: 'raster', source: 'osm' }],
} as const;

const NANTES: [number, number] = [-1.5536, 47.2184];

type Row = Record<string, any>;
const asMaps = (ans: any): Row[] => {
  const cols: string[] = ans?.columns ?? [];
  return (ans?.rows ?? []).map((r: any[]) => Object.fromEntries(cols.map((c, i) => [c, r[i]])));
};

export default function Screen() {
  const zb = useRef<ReturnType<typeof makeClient> | null>(null);
  const [status, setStatus] = useState('connecting…');
  const [chargers, setChargers] = useState<Row[]>([]);
  const [stations, setStations] = useState<Row[]>([]);

  useEffect(() => {
    let live = true;
    (async () => {
      try {
        const c = makeClient();
        await c.connect();
        if (!live) return;
        zb.current = c;
        setStatus('connected — pan to ask');
        await ask(NANTES[1], NANTES[0]);
      } catch (e) {
        if (live) setStatus(`connect: ${e}`);
      }
    })();
    return () => { live = false; void zb.current?.close(); };
  }, []);

  const ask = useCallback(async (lat: number, lng: number, radius = 20000) => {
    const c = zb.current;
    if (!c) return;
    try {
      const t0 = Date.now();
      const ans: any = await c.request(`query.${TENANT}.chargers_near`, {
        lat, lng, radius_m: Math.round(radius), limit: 2000,
      }, 15000);
      // §10ic: held in memory, never ingested — what is drawn is what was just asked
      // for, not the union of everywhere this phone has been.
      setChargers(asMaps(ans));
      const t = ans.zb_transport ?? {};
      setStatus(`${ans.count} charger(s) · ${t.via} ${t.bytes} B · wire ${t.wire_ms} ms · db ${ans.ms} ms · ${Date.now() - t0} ms total`);
      const fuel: any = await c.request(`query.${TENANT}.fuel_near`, {
        lat, lng, radius_m: Math.round(Math.min(20000, radius)), fuel: 'SP95', sort: 'distance', limit: 40,
      }, 15000);
      setStations(asMaps(fuel));
    } catch (e) {
      setStatus(`ask: ${e}`);
    }
  }, []);

  return (
    <View style={styles.fill}>
      <MapView
        style={styles.fill}
        mapStyle={STYLE as any}
        onRegionDidChange={(f: any) => {
          const [lng, lat] = f?.geometry?.coordinates ?? NANTES;
          void ask(lat, lng);
        }}
      >
        <Camera defaultSettings={{ centerCoordinate: NANTES, zoomLevel: 11 }} />
        {chargers.map((r) => (
          <MarkerView key={String(r.id)} coordinate={[Number(r.lng), Number(r.lat)]}>
            <View style={[styles.pin, Number(r.max_power_kw ?? 0) >= 43 ? styles.rapid : styles.slow]} />
          </MarkerView>
        ))}
        {stations.map((r) => (
          <MarkerView key={`f-${r.id}`} coordinate={[Number(r.lng), Number(r.lat)]}>
            <View style={styles.price}><Text style={styles.priceText}>{Number(r.price).toFixed(3)}</Text></View>
          </MarkerView>
        ))}
      </MapView>
      <View style={styles.bar}><Text style={styles.barText}>{status}</Text></View>
    </View>
  );
}

const styles = StyleSheet.create({
  fill: { flex: 1 },
  pin: { width: 12, height: 12, borderRadius: 6, borderWidth: 1, borderColor: '#fff' },
  rapid: { backgroundColor: '#1a7f37' },
  slow: { backgroundColor: '#1f6feb' },
  price: { backgroundColor: '#b45309', paddingHorizontal: 4, paddingVertical: 2, borderRadius: 4, borderWidth: 1, borderColor: '#fff' },
  priceText: { color: '#fff', fontSize: 10, fontWeight: 'bold' },
  bar: { position: 'absolute', left: 0, right: 0, bottom: 0, backgroundColor: 'rgba(0,0,0,0.65)', padding: 8 },
  barText: { color: '#fff', fontSize: 11 },
});
