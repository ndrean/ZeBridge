/// ZeMap in a browser: the fuel prices and the shared route, on the same client
/// library the phone uses (NOTES §10hj, §10ho).
///
/// Two features, two mechanisms, deliberately:
///
///   * the CHARGE POINTS are an ASK the browser KEEPS — `charge_points` is an on-demand
///     table here too (§10hn, §10hr): the schema arrives, the local table is created,
///     nothing seeds it, and `ingest` keeps what each ask answered. The switch chooses
///     the minimum power, off asks for nothing.
///   * the FUEL prices are an ASK — `request('query.<tenant>.fuel_near')` to the POI
///     service, answered from its DuckDB replica of all of France. Nothing is stored
///     here: no table, no stream, no position. The page holds an answer for as long as
///     it draws it.
///   * the shared ROUTE is a ROW — `routes.doc`, a jsonb map of registers, replicated
///     into this browser's OPFS SQLite like any table, written with `mutate`. Two
///     editors converge because each ships the union of its own registers merged into
///     the document it last saw; the row underneath stays plain last-write-wins.
///
/// The basemap is OpenStreetMap raster, not the R2 vector tiles the phone renders —
/// the vector stack in a browser needs a style sheet this demo does not need to own.
import L from 'leaflet';
import { ZeBridge, mergeRegisters } from 'zb-client-ts';

const qs = new URLSearchParams(location.search);
const PRINCIPAL = qs.get('principal') ?? 'alice';
const TENANT = qs.get('tenant') ?? '_default';       // the open tenant answers for everyone
const ROUTE_ID = '11111111-1111-4111-8111-111111111111';
const NANTES: [number, number] = [47.2184, -1.5536];

const el = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const statusEl = el<HTMLElement>('status');
const say = (s: string) => { statusEl.textContent = s; };

const creds = await fetch(`/creds/${PRINCIPAL}.creds`).then((r) => (r.ok ? r.text() : undefined));
if (!creds) say(`no creds for ${PRINCIPAL} — is public/creds pointing at scripts/native/creds?`);

const zb = new ZeBridge({
  natsUrl: `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/nats`,
  principal: PRINCIPAL,
  creds,
  // §10hn: said out loud. The route is a replicated row; the charge points are held on
  // demand; the fuel needs no table at all.
  tables: ['routes'],
  ondemandTables: ['charge_points'],
});
(window as any).zb = zb;

// ── the map ────────────────────────────────────────────────────────────────────
const map = L.map('map').setView(NANTES, 14);
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19,
  attribution: '© OpenStreetMap',
  // The page is cross-origin ISOLATED (OPFS needs it), and under COEP a cross-origin
  // image must either carry CORP or be fetched with CORS. Without this the basemap is
  // a grey void while every tile request succeeds — measured, and invisible in the
  // network panel's status column.
  crossOrigin: '',
}).addTo(map);
const fuelLayer = L.layerGroup().addTo(map);
const chargerLayer = L.layerGroup().addTo(map);
const routeLayer = L.layerGroup().addTo(map);

// ── charge points: an ask the browser KEEPS ───────────────────────────────────
const chargerSelect = el<HTMLSelectElement>('chargers');
let askingChargers = false;

async function askChargers() {
  if (chargerSelect.value === '' || askingChargers) { if (chargerSelect.value === '') chargerLayer.clearLayers(); return; }
  askingChargers = true;
  const minKw = Number(chargerSelect.value);
  const c = map.getCenter();
  const b = map.getBounds();
  const radius = Math.min(20000, Math.max(500, map.distance(b.getSouthWest(), b.getNorthEast()) / 2));
  const t0 = performance.now();
  try {
    const ans = await zb.request(`query.${TENANT}.chargers_near`, {
      lat: c.lat, lng: c.lng, radius_m: Math.round(radius), ...(minKw > 0 ? { min_kw: minKw } : {}), limit: 2000,
    }, 15000);
    // The scope — "what I hold in this box and the answer lacks is gone" — only when the
    // answer is complete; one cut by the limit says nothing about the rest.
    const dlat = radius / 111320, dlng = radius / (111320 * Math.max(0.1, Math.cos(c.lat * Math.PI / 180)));
    const scope = ans.complete === true
      ? { where: 'lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?', params: [c.lat - dlat, c.lat + dlat, c.lng - dlng, c.lng + dlng] }
      : null;
    const kept = await zb.ingest('charge_points', ans, minKw > 0 ? null : scope);
    const ms = Math.round(performance.now() - t0);
    await drawChargers();
    say(`${ans.count} charge point(s) within ${(radius / 1000).toFixed(1)} km — ${ans.ms} ms in the service, ${ms} ms round trip, ${kept} kept`);
  } catch (e) {
    say(`chargers: ${e} — is the map service running?`);
  } finally {
    askingChargers = false;
  }
}

/// Drawn from the LOCAL table, never from the answer: what is on screen is what this
/// browser holds, which is also what it would show with the network gone.
async function drawChargers() {
  chargerLayer.clearLayers();
  if (chargerSelect.value === '') return;
  const minKw = Number(chargerSelect.value);
  const b = map.getBounds();
  const rows = await zb.query(
    `SELECT id, lat, lng, title, max_power_kw, points, status_type_id, ocm_id FROM charge_points
      WHERE lat BETWEEN ? AND ? AND lng BETWEEN ? AND ? AND coalesce(max_power_kw, 0) >= ? LIMIT 3000`,
    b.getSouth(), b.getNorth(), b.getWest(), b.getEast(), minKw);
  for (const r of rows) {
    const kw = Number(r.max_power_kw ?? 0);
    const colour = r.ocm_id == null ? '#d9480f' : r.status_type_id !== 50 ? '#999' : kw >= 43 ? '#1a7f37' : '#1f6feb';
    L.circleMarker([r.lat, r.lng], { radius: 6, color: colour, fillColor: colour, fillOpacity: 0.85, weight: 1 })
      .bindTooltip(`${r.title} · ${r.max_power_kw ?? '?'} kW · ${r.points ?? '?'} pt${r.status_type_id !== 50 ? ' · out of service' : ''}`)
      .addTo(chargerLayer);
  }
}

chargerSelect.addEventListener('change', () => { void askChargers(); });

// ── fuel: an ask, answered from the service's replica ──────────────────────────
const fuelSelect = el<HTMLSelectElement>('fuel');
let askingFuel = false;

async function askFuel() {
  const fuel = fuelSelect.value;
  fuelLayer.clearLayers();
  if (!fuel || askingFuel) return;
  askingFuel = true;
  const c = map.getCenter();
  const b = map.getBounds();
  const radius = Math.min(20000, Math.max(3000, map.distance(b.getSouthWest(), b.getNorthEast()) / 2));
  const t0 = performance.now();
  try {
    const ans = await zb.request(`query.${TENANT}.fuel_near`, {
      lat: c.lat, lng: c.lng, radius_m: Math.round(radius), fuel, sort: 'distance', limit: 40,
    }, 8000);
    const cols: string[] = ans.columns ?? [];
    const rows: any[][] = ans.rows ?? [];
    const at = (r: any[], name: string) => r[cols.indexOf(name)];
    const prices = rows.map((r) => Number(at(r, 'price'))).filter((n) => Number.isFinite(n));
    const cheapest = prices.length ? Math.min(...prices) : NaN;
    for (const r of rows) {
      const lat = Number(at(r, 'lat')), lng = Number(at(r, 'lng'));
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
      const price = Number(at(r, 'price'));
      const out = at(r, 'outage');
      const cls = out ? 'price out' : price === cheapest ? 'price cheap' : 'price';
      L.marker([lat, lng], {
        icon: L.divIcon({ className: '', html: `<span class="${cls}">${price.toFixed(3)} €</span>`, iconSize: [58, 18] }),
      })
        .bindTooltip(`${at(r, 'address') ?? ''} ${at(r, 'city') ?? ''} · ${Math.round(Number(at(r, 'm')))} m${out ? ` · ${out} outage` : ''}`)
        .addTo(fuelLayer);
    }
    say(`${rows.length} station(s) selling ${fuel} within ${(radius / 1000).toFixed(0)} km — ${ans.ms} ms in the service, ${Math.round(performance.now() - t0)} ms round trip`);
  } catch (e) {
    say(`fuel: ${e} — is the POI service running?`);
  } finally {
    askingFuel = false;
  }
}
fuelSelect.addEventListener('change', () => void askFuel());
map.on('moveend', () => {
  if (fuelSelect.value) void askFuel();
  if (chargerSelect.value !== '') void askChargers();
});

// ── the shared route: a row, edited by two people ──────────────────────────────
const routeButton = el<HTMLButtonElement>('routeMode');
const routeInfo = el<HTMLElement>('routeInfo');
let routeMode = false;
let routeDoc: Record<string, any> = {};
const routeMine: Record<string, any> = {};   // this browser's own registers, shipped whole
let routeNext: 'start' | 'end' = 'start';
let routeRounds = 0;

/// The stamp every editor orders the same way: RFC 3339 UTC, six fractional digits.
const stamp = () => new Date().toISOString().replace('Z', '000Z');
const writer = `browser-${PRINCIPAL}`;

async function readRoute() {
  try {
    const r = (await zb.query(`SELECT doc, last_writer, updated_at FROM routes WHERE id = ?`, ROUTE_ID))[0];
    if (!r) return;
    routeDoc = typeof r.doc === 'string' ? JSON.parse(r.doc) : (r.doc ?? {});
    drawRoute(String(r.last_writer ?? ''));
  } catch { /* the table is not here yet */ }
}

function drawRoute(lastWriter = '') {
  routeLayer.clearLayers();
  const pins: L.LatLng[] = [];
  for (const key of ['start', 'end'] as const) {
    const v = routeDoc[key]?.v;
    if (typeof v?.lat !== 'number' || typeof v?.lng !== 'number') continue;
    const here = L.latLng(v.lat, v.lng);
    pins.push(here);
    L.circleMarker(here, { radius: 8, color: '#1f6feb', fillColor: key === 'start' ? '#fff' : '#1f6feb', fillOpacity: 1 })
      .bindTooltip(`${key} · by ${routeDoc[key].w} at ${String(routeDoc[key].t).slice(11, 23)}`, { permanent: false })
      .addTo(routeLayer);
  }
  if (pins.length === 2) L.polyline(pins, { color: '#1f6feb', weight: 4, dashArray: '6 6' }).addTo(routeLayer);
  const who = ['start', 'end'].filter((k) => routeDoc[k]).map((k) => `${k} by ${routeDoc[k].w}`).join(', ');
  routeInfo.textContent = routeMode ? `${who || 'tap the map'}${lastWriter ? ` · row by ${lastWriter}` : ''}` : '';
}

async function writeRoute() {
  const merged = mergeRegisters(routeDoc as any, routeMine as any);
  await zb.mutate('routes', 'UPDATE', { id: ROUTE_ID }, { doc: merged });
}

map.on('click', (e: L.LeafletMouseEvent) => {
  if (!routeMode) return;
  routeMine[routeNext] = { v: { lat: e.latlng.lat, lng: e.latlng.lng }, t: stamp(), w: writer };
  routeNext = routeNext === 'start' ? 'end' : 'start';
  // Draw nothing yet: the pins come from the ROW, so what is on screen is what the
  // row holds — including the other editor's moves.
  void writeRoute().catch((err) => say(`route: ${err}`));
});

routeButton.addEventListener('click', () => {
  routeMode = !routeMode;
  routeButton.textContent = `route: ${routeMode ? 'on — click to move start, then end' : 'off'}`;
  drawRoute();
});

/// The row moved, mine or the phone's: redraw from it, then reconcile — write the
/// union again while what is observed does not contain what this browser wrote.
zb.onChange('routes', () => {
  void (async () => {
    await readRoute();
    if (!Object.keys(routeMine).length) return;
    const merged = mergeRegisters(routeDoc as any, routeMine as any);
    if (JSON.stringify(merged) !== JSON.stringify(routeDoc) && routeRounds < 10) {
      routeRounds += 1;
      await writeRoute();
    }
  })();
});

// ── go ─────────────────────────────────────────────────────────────────────────
zb.onStatus?.((s: string) => { el<HTMLElement>('who').textContent = `${PRINCIPAL} · ${s}`; });
try {
  await zb.connect();
  el<HTMLElement>('who').textContent = `${PRINCIPAL} · ${zb.tenant ?? '—'}`;
  say('connected — the charge points are on; pick a fuel, or turn the route on and click twice');
  await readRoute();
  await askChargers();
} catch (e) {
  say(`connect: ${e}`);
}
