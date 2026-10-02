/// The airports around the map's centre, asked of the DuckDB service over NATS.
///
/// The page holds nothing: no table is followed. After every pan or zoom it asks
/// `query._default.airports_near` for the airports within RADIUS_KM of the centre, draws
/// them, and writes the count under the map. PostgreSQL never sees the question.
///
/// The first load enrolls with `?invite=<code>`; the identity is kept in this browser,
/// so later loads need no invite.
import L from 'leaflet';
import { ZeBridge } from 'zb-client-ts';

const SAN_MATEO: L.LatLngTuple = [37.563, -122.326];
/// A circle 200 km across, around the centre of the map.
const RADIUS_KM = 100;
const LIMIT = 500;

const count = document.getElementById('count')!;
const detail = document.getElementById('detail')!;

const zb = new ZeBridge({
  natsUrl: `${location.origin.replace(/^http/, 'ws')}/nats`,
  bridgeUrl: `${location.origin}/bridge`,
  invite: new URLSearchParams(location.search).get('invite') ?? undefined,
  // A fixed name: the identity is kept under `<dbPath>.identity`, found again on reload.
  dbPath: 'airports.sqlite3',
  tables: [],
});

const map = L.map('map');
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 18,
  attribution: '&copy; OpenStreetMap contributors',
  // The page is cross-origin isolated (for OPFS): tiles must be fetched with CORS,
  // or each one loads and the map stays grey.
  crossOrigin: '',
}).addTo(map);
map.fitBounds(L.latLng(SAN_MATEO).toBounds(RADIUS_KM * 2 * 1000));
// Leaflet measures its container once: a window resized later leaves the map grey
// until it is told.
new ResizeObserver(() => map.invalidateSize()).observe(document.getElementById('map')!);

const circle = L.circle(SAN_MATEO, { radius: RADIUS_KM * 1000, fill: false, weight: 1, dashArray: '4 4' }).addTo(map);
const markers = L.layerGroup().addTo(map);

let asked = 0;
async function ask(): Promise<void> {
  const centre = map.getCenter();
  circle.setLatLng(centre);
  const mine = ++asked;
  const t0 = performance.now();
  try {
    const a = await zb.request('query._default.airports_near', {
      lat: centre.lat, lng: centre.lng, radius_km: RADIUS_KM, limit: LIMIT,
    });
    if (mine !== asked) return; // a newer pan asked meanwhile: its answer wins
    if (a.error) {
      count.textContent = `the service refused: ${a.error}`;
      return;
    }
    const col = (name: string) => a.columns.indexOf(name);
    markers.clearLayers();
    for (const r of a.rows) {
      L.circleMarker([Number(r[col('latitude')]), Number(r[col('longitude')])], { radius: 5, weight: 1 })
        .bindTooltip(`${r[col('code')]} — ${r[col('name')]}, ${r[col('distance_km')]} km`)
        .addTo(markers);
    }
    count.textContent = `${a.count}${a.complete ? '' : '+'} airport${a.count === 1 ? '' : 's'} within ${RADIUS_KM} km of the centre`;
    detail.textContent = `${a.ms} ms in DuckDB, ${Math.round(performance.now() - t0)} ms round trip`;
  } catch (e) {
    if (mine === asked) count.textContent = `no answer: ${(e as Error).message}`;
  }
}

map.on('moveend', () => void ask());
await zb.connect();
await ask();
