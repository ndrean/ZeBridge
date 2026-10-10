/// The airports around the map's centre, and one flight a tenant edits together.
///
/// Two mechanisms side by side:
///
///   * the AIRPORTS are a question — `query._default.airports_near` to the DuckDB service,
///     asked after every pan. This page stores none of them.
///   * the FLIGHT is a row — `flights`, replicated into this browser's SQLite and written
///     with `mutate`. Its `doc` holds two registers {v, t, w}: the departure and the arrival,
///     each with its stamp and its writer. Everyone in the tenant sees the same flight;
///     moves to different ends both survive, and on the same end the later stamp wins on
///     every screen (COOPERATIVE_EDITING.md). Another tenant never sees it.
///
/// The first load enrolls with `?invite=<code>`; the identity is kept in this browser.
/// `?as=<name>` keeps a separate identity and replica, so two people can share one browser.
import L from 'leaflet';
import { ZeBridge, NotEnrolled, mergeRegisters } from '@zebridge/client';

const SAN_MATEO: L.LatLngTuple = [37.563, -122.326];
/// A circle 200 km across, around the centre of the map.
const RADIUS_KM = 100;
const LIMIT = 500;

const qs = new URLSearchParams(location.search);
const AS = qs.get('as');
const el = (id: string) => document.getElementById(id)!;
const count = el('count'), detail = el('detail'), flightLine = el('flight'), notice = el('notice');

/// Built for a deployment (`VITE_ZB_BRIDGE_URL=https://bridge.example.com pnpm build`): the
/// page enrolls there, and the answer names the NATS websocket. Without it, the dev
/// server's proxy carries both, on this page's own origin.
const DEPLOYED_BRIDGE = import.meta.env.VITE_ZB_BRIDGE_URL as string | undefined;
/// `VITE_ZB_NATS_URL` connects elsewhere than the websocket the bridge names, such as a
/// leaf node (`wss://leaf.example.com:8443`).
const NATS_URL = import.meta.env.VITE_ZB_NATS_URL as string | undefined;

const zb = new ZeBridge({
  natsUrl: NATS_URL ?? (DEPLOYED_BRIDGE ? undefined : `${location.origin.replace(/^http/, 'ws')}/nats`),
  bridgeUrl: DEPLOYED_BRIDGE ?? `${location.origin}/bridge`,
  invite: qs.get('invite') ?? undefined,
  // A fixed name: the identity is kept under `<dbPath>.identity`, found again on reload.
  dbPath: AS ? `airports-${AS}.sqlite3` : 'airports.sqlite3',
  tables: ['flights'],
});

// ── the map ────────────────────────────────────────────────────────────────────
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
new ResizeObserver(() => map.invalidateSize()).observe(el('map'));

const circle = L.circle(SAN_MATEO, { radius: RADIUS_KM * 1000, fill: false, color: '#1f4fd1', weight: 3, dashArray: '8 6' }).addTo(map);
const markers = L.layerGroup().addTo(map);
const flightLayer = L.layerGroup().addTo(map);

// ── the airports: a question ───────────────────────────────────────────────────
type Airport = { code: string; name: string; lat: number; lng: number };

let asked = 0;
/// Leaflet does not wrap longitudes: pan west across the Pacific and the centre reads
/// -200, not 160. The service is asked with the wrapped value, and whatever is drawn is
/// moved onto the copy of the world in view (360° to the left or right).
const onView = (lng: number) => lng + 360 * Math.round((map.getCenter().lng - lng) / 360);

async function ask(): Promise<void> {
  const centre = map.getCenter();
  circle.setLatLng(centre);
  const wrapped = centre.wrap();
  const mine = ++asked;
  const t0 = performance.now();
  try {
    const a = await zb.request('query._default.airports_near', {
      lat: wrapped.lat, lng: wrapped.lng, radius_km: RADIUS_KM, limit: LIMIT,
    });
    if (mine !== asked) return; // a newer pan asked meanwhile: its answer wins
    if (a.error) {
      count.textContent = `the service refused: ${a.error}`;
      return;
    }
    const col = (name: string) => a.columns.indexOf(name);
    markers.clearLayers();
    for (const r of a.rows) {
      const ap: Airport = { code: r[col('code')], name: r[col('name')], lat: Number(r[col('latitude')]), lng: Number(r[col('longitude')]) };
      L.circleMarker([ap.lat, onView(ap.lng)], { radius: 9, weight: 2, color: '#fff', fillColor: '#d1361f', fillOpacity: 0.9 })
        .bindTooltip(`${ap.code} — ${ap.name}, ${r[col('distance_km')]} km`)
        .bindPopup(() => choose(ap))
        .addTo(markers);
    }
    count.textContent = `${a.count}${a.complete ? '' : '+'} airport${a.count === 1 ? '' : 's'} within ${RADIUS_KM} km of the centre`;
    detail.textContent = `${a.ms} ms in the service, ${Math.round(performance.now() - t0)} ms round trip`;
  } catch (e) {
    if (mine === asked) count.textContent = `no answer: ${(e as Error).message}`;
  }
}

/// The popup on an airport: make it the departure or the arrival.
function choose(ap: Airport): HTMLElement {
  const box = document.createElement('div');
  box.innerHTML = `<b>${ap.code}</b> — ${ap.name}<br>`;
  for (const [end, label] of [['origin', 'Departure'], ['destination', 'Arrival']] as const) {
    const b = document.createElement('button');
    b.textContent = label;
    b.style.margin = '6px 6px 0 0';
    b.onclick = () => { map.closePopup(); setEnd(end, ap); };
    box.appendChild(b);
  }
  return box;
}

// ── the flight: a row, two registers ───────────────────────────────────────────
type End = 'origin' | 'destination';
type Register = { v: Airport; t: string; w: string };
const LABEL: Record<End, string> = { origin: 'departure', destination: 'arrival' };

let flightId = '';
let doc: Partial<Record<End, Register>> = {};     // what the row holds
let rowExists = false;
const mine: Partial<Record<End, Register>> = {};  // what this browser wrote and has not seen in the row
let rounds = 0;

async function readFlight(): Promise<void> {
  const r = (await zb.query('SELECT doc FROM flights WHERE id = ?', flightId))[0];
  const next: Partial<Record<End, Register>> = r ? (typeof r.doc === 'string' ? JSON.parse(r.doc) : (r.doc ?? {})) : {};
  rowExists = !!r;
  for (const end of ['origin', 'destination'] as End[]) {
    const was = doc[end], now = next[end];
    if (!now || now.t === was?.t) continue;
    const m = mine[end];
    if (now.w !== zb.principal) {
      if (m && now.t > m.t) say(`${now.w}'s ${now.v.code} came after your ${m.v.code}: the ${LABEL[end]} is ${now.v.code}`);
      else say(`${now.w} set the ${LABEL[end]} to ${now.v.code}`);
    }
    if (m && now.t >= m.t) delete mine[end]; // the row holds mine, or something later
  }
  doc = next;
  drawFlight();
}

async function writeFlight(): Promise<void> {
  const merged = mergeRegisters(doc as any, mine as any);
  if (rowExists) await zb.mutate('flights', 'UPDATE', { id: flightId }, { doc: merged });
  else await zb.mutate('flights', 'INSERT', { id: flightId }, { tenant_id: zb.tenant, doc: merged });
}

function setEnd(end: End, ap: Airport): void {
  mine[end] = { v: ap, t: zb.stamp(), w: zb.principal };
  rounds = 0;
  drawFlight();
  void writeFlight().catch((e) => say(`write: ${(e as Error).message}`));
}

/// The row moved, by me or by someone else: redraw from it, then reconcile — write the
/// merge again while the row does not hold what this browser wrote.
zb.onChange('flights', () => {
  void (async () => {
    await readFlight();
    if (!Object.keys(mine).length || rounds >= 10) return;
    const merged = mergeRegisters(doc as any, mine as any);
    if (JSON.stringify(merged) !== JSON.stringify(doc)) {
      rounds += 1;
      await writeFlight();
    }
  })();
});

function drawFlight(): void {
  flightLayer.clearLayers();
  const ends: Partial<Record<End, { ap: Airport; pending: boolean; reg: Register }>> = {};
  for (const end of ['origin', 'destination'] as End[]) {
    const m = mine[end], d = doc[end];
    const reg = m ?? d;
    if (reg) ends[end] = { ap: reg.v, pending: !!m, reg };
  }
  const o = ends.origin, d = ends.destination;
  // The whole flight moves by one shift (the departure's), so the line stays continuous;
  // the arrival is drawn at the line's end, which may be past ±180.
  const shift = o ? onView(o.ap.lng) - o.ap.lng : d ? onView(d.ap.lng) - d.ap.lng : 0;
  let arrivalLng = d ? d.ap.lng + shift : 0;
  if (o && d) {
    const line = greatCircle(o.ap, d.ap).map(([la, ln]) => [la, ln + shift] as L.LatLngTuple);
    arrivalLng = line[line.length - 1][1];
    L.polyline(line, { color: '#7a1fd1', weight: 3 }).addTo(flightLayer);
  }
  for (const [end, e] of Object.entries(ends) as [End, NonNullable<typeof o>][]) {
    const colour = end === 'origin' ? '#1a7f37' : '#7a1fd1';
    L.circleMarker([e.ap.lat, end === 'origin' ? e.ap.lng + shift : arrivalLng], {
      radius: 12, weight: 4, color: colour, fillColor: colour, fillOpacity: e.pending ? 0 : 1,
    }).bindTooltip(`${LABEL[end]}: ${e.ap.code}${e.pending ? ' (waiting for PostgreSQL)' : ` · ${e.reg.w}`}`).addTo(flightLayer);
  }
  const part = (end: End) => {
    const e = ends[end];
    if (!e) return `${LABEL[end]}: —`;
    return `${LABEL[end]}: ${e.ap.code}${e.pending ? ' (pending)' : ` by ${e.reg.w} at ${e.reg.t.slice(11, 19)}`}`;
  };
  const leg = o && d ? ` · ${Math.round(distanceKm(o.ap, d.ap)).toLocaleString()} km, heading ${Math.round(bearing(o.ap, d.ap))}°` : '';
  flightLine.textContent = `Flight ${zb.tenant}: ${part('origin')} → ${part('destination')}${leg}`;
}

let noticeTimer = 0;
function say(text: string): void {
  notice.textContent = text;
  clearTimeout(noticeTimer);
  noticeTimer = window.setTimeout(() => { notice.textContent = ''; }, 8000);
}

// ── great-circle geometry ──────────────────────────────────────────────────────
const rad = (d: number) => (d * Math.PI) / 180, deg = (r: number) => (r * 180) / Math.PI;

function centralAngle(a: Airport, b: Airport): number {
  const dφ = rad(b.lat - a.lat), dλ = rad(b.lng - a.lng);
  return 2 * Math.asin(Math.sqrt(Math.sin(dφ / 2) ** 2 + Math.cos(rad(a.lat)) * Math.cos(rad(b.lat)) * Math.sin(dλ / 2) ** 2));
}

function distanceKm(a: Airport, b: Airport): number {
  return 6371 * centralAngle(a, b);
}

function bearing(a: Airport, b: Airport): number {
  const φ1 = rad(a.lat), φ2 = rad(b.lat), dλ = rad(b.lng - a.lng);
  return (deg(Math.atan2(Math.sin(dλ) * Math.cos(φ2), Math.cos(φ1) * Math.sin(φ2) - Math.sin(φ1) * Math.cos(φ2) * Math.cos(dλ))) + 360) % 360;
}

/// The shortest path over the sphere, as points. Longitudes are kept continuous (no jump
/// from +180 to -180), so a flight across the Pacific draws as one line.
function greatCircle(a: Airport, b: Airport, n = 128): L.LatLngTuple[] {
  const d = centralAngle(a, b);
  if (d === 0) return [[a.lat, a.lng], [b.lat, b.lng]];
  const φ1 = rad(a.lat), λ1 = rad(a.lng), φ2 = rad(b.lat), λ2 = rad(b.lng);
  const pts: L.LatLngTuple[] = [];
  let prev = a.lng;
  for (let i = 0; i <= n; i++) {
    const f = i / n;
    const A = Math.sin((1 - f) * d) / Math.sin(d), B = Math.sin(f * d) / Math.sin(d);
    const x = A * Math.cos(φ1) * Math.cos(λ1) + B * Math.cos(φ2) * Math.cos(λ2);
    const y = A * Math.cos(φ1) * Math.sin(λ1) + B * Math.cos(φ2) * Math.sin(λ2);
    const z = A * Math.sin(φ1) + B * Math.sin(φ2);
    let lng = deg(Math.atan2(y, x));
    while (lng - prev > 180) lng -= 360;
    while (lng - prev < -180) lng += 360;
    prev = lng;
    pts.push([deg(Math.atan2(z, Math.sqrt(x * x + y * y))), lng]);
  }
  return pts;
}

// ── go ─────────────────────────────────────────────────────────────────────────
map.on('moveend', () => { drawFlight(); void ask(); });
try {
  await zb.connect();
} catch (e) {
  // The first visit needs the invite link; any other failure is said as it is.
  count.textContent = e instanceof NotEnrolled
    ? 'This browser is not enrolled yet: open the invite link you were given (…/?invite=<code>).'
    : `Could not connect: ${(e as Error).message}`;
  throw e;
}
flightId = `flight-${zb.tenant}`;
await readFlight();
await ask();
