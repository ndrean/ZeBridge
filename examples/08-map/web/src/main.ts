/// ZeMap in a browser: the fuel prices and the shared route, on the same client
/// library the phone uses (NOTES §10hj, §10ho).
///
/// Two mechanisms, deliberately — and §10ic collapsed the first two into one:
///
///   * the CHARGE POINTS and the FUEL prices are both an ASK —
///     `request('query.<tenant>.<name>')` to the POI service, answered from its DuckDB
///     replica of all of France. Nothing is stored here: no table, no stream, no
///     position. The page holds an answer for as long as it draws it. The chargers used
///     to be kept with `ingest`, and that made the map draw the union of everywhere this
///     browser had been rather than what was in view. `charge_points` stays declared
///     on-demand so the descriptor is there, but nothing is written to it.
///   * the shared ROUTE is a ROW — `routes.doc`, a jsonb map of registers, replicated
///     into this browser's OPFS SQLite like any table, written with `mutate`. Two
///     editors converge because each ships the union of its own registers merged into
///     the document it last saw; the row underneath stays plain last-write-wins.
///
/// The basemap is OpenStreetMap raster, not the R2 vector tiles the phone renders —
/// the vector stack in a browser needs a style sheet this demo does not need to own.
import L from 'leaflet';
import { ZeBridge, mergeRegisters } from '@zebridge/client';

const qs = new URLSearchParams(location.search);
const PRINCIPAL = qs.get('principal') ?? 'alice';
const TENANT = qs.get('tenant') ?? '_default';       // the open tenant answers for everyone
const ROUTE_ID = '11111111-1111-4111-8111-111111111111';
const NANTES: [number, number] = [47.2184, -1.5536];

const el = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const statusEl = el<HTMLElement>('status');
const say = (s: string) => { statusEl.textContent = s; };

/// §10hu: how the answer travelled, the same way for every dataset. The library
/// splices `zb_transport` into every answer, so fuel and chargers are read on one
/// clock. `wire` is ask to reply and carries the responder's poll wait, which
/// dominates; `fetch` is the object read, 0 when the answer came inline.
const transport = (ans: any): string => {
  const t = ans?.zb_transport;
  if (!t) return `db ${ans?.ms} ms`;
  const kb = (t.bytes / 1024).toFixed(1);
  const via = t.via === 'object' ? `object ${kb} KB · fetch ${t.fetch_ms} ms` : `inline ${kb} KB`;
  return `${via} · wire ${t.wire_ms} ms · db ${ans.ms} ms`;
};

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
  // §10hu: 150 km, not 20 km — at 20 km every answer stayed inline and the
  // large-answer path was unreachable from the browser too. `limit` still bounds it.
  const radius = Math.min(150000, Math.max(500, map.distance(b.getSouthWest(), b.getNorthEast()) / 2));
  const t0 = performance.now();
  try {
    const ans = await zb.request(`query.${TENANT}.chargers_near`, {
      lat: c.lat, lng: c.lng, radius_m: Math.round(radius), ...(minKw > 0 ? { min_kw: minKw } : {}), limit: 2000,
    }, 15000);
    // §10ic: NOT ingested. The answer IS the layer. Persisting it made the map draw the
    // union of everywhere this browser had been rather than what was in view, so a
    // zoom-out showed clusters from earlier pans. The stations were never ingested and
    // never had the problem; the chargers now work the same way.
    chargerRows = asMaps(ans);
    const ms = Math.round(performance.now() - t0);
    drawChargers();
    say(`${ans.count} charge point(s) within ${(radius / 1000).toFixed(1)} km · ${transport(ans)} · ${ms} ms total`);
  } catch (e) {
    say(`chargers: ${e} — is the map service running?`);
  } finally {
    askingChargers = false;
  }
}

/// The last answer, unfiltered. Not a local table (§10ic).
let chargerRows: any[] = [];

/// An answer's `columns`/`rows` as objects, the shape the markers read.
const asMaps = (ans: any): any[] => {
  const cols: string[] = ans.columns ?? [];
  return (ans.rows ?? []).map((r: any[]) => Object.fromEntries(cols.map((c, i) => [c, r[i]])));
};

/// Drawn from the ANSWER, narrowed to what is on screen and above the chosen power.
/// No SQL and no round trip: a filter over the rows the last ask returned.
function drawChargers() {
  chargerLayer.clearLayers();
  if (chargerSelect.value === '') return;
  const minKw = Number(chargerSelect.value);
  const b = map.getBounds();
  const rows = chargerRows.filter((r) =>
    r.lat >= b.getSouth() && r.lat <= b.getNorth() &&
    r.lng >= b.getWest() && r.lng <= b.getEast() &&
    Number(r.max_power_kw ?? 0) >= minKw);
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
    say(`${rows.length} station(s) selling ${fuel} within ${(radius / 1000).toFixed(0)} km · ${transport(ans)} · ${Math.round(performance.now() - t0)} ms total`);
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
/// §10if: which end a click moves. Null means the old behaviour — alternate start, end,
/// start — which cannot move ONE end twice in a row. Click a pin to take hold of it.
let routeSelected: 'start' | 'end' | null = null;
let routeRounds = 0;

/// A register's stamp: the bridge's time as this device estimates it, never behind what
/// this replica has seen (RFC 3339 UTC, six fractional digits).
const stamp = () => zb.stamp();
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
    const held = routeSelected === key;
    const m = L.circleMarker(here, {
      radius: held ? 11 : 8,
      color: '#1f6feb',
      weight: held ? 4 : 3,
      fillColor: key === 'start' ? '#fff' : '#1f6feb',
      fillOpacity: 1,
    })
      .bindTooltip(`${key} · by ${routeDoc[key].w} at ${String(routeDoc[key].t).slice(11, 23)}`, { permanent: false })
      .addTo(routeLayer);
    // Clicking a pin SELECTS it and must not also place one: the map's own handler
    // would otherwise fire for the same click.
    m.on('click', (e: L.LeafletMouseEvent) => {
      L.DomEvent.stopPropagation(e);
      routeSelected = routeSelected === key ? null : key;
      drawRoute(lastWriter);
    });
    // And dragging it moves that end directly — a circleMarker is not draggable, so the
    // drag is done by hand on the map's pointer events while the pin is held.
    m.on('mousedown', (e: L.LeafletMouseEvent) => {
      L.DomEvent.stopPropagation(e);
      routeSelected = key;
      map.dragging.disable();
      const move = (ev: L.LeafletMouseEvent) => { m.setLatLng(ev.latlng); };
      const up = (ev: L.LeafletMouseEvent) => {
        map.off('mousemove', move); map.off('mouseup', up);
        map.dragging.enable();
        moveEnd(key, ev.latlng);
      };
      map.on('mousemove', move); map.on('mouseup', up);
    });
  }
  if (pins.length === 2) L.polyline(pins, { color: '#1f6feb', weight: 4, dashArray: '6 6' }).addTo(routeLayer);
  const who = ['start', 'end'].filter((k) => routeDoc[k]).map((k) => `${k} by ${routeDoc[k].w}`).join(', ');
  const holding = routeSelected ? `holding ${routeSelected} — click to move it, click the other pin to switch · ` : '';
  routeInfo.textContent = routeMode ? `${holding}${who || 'click the map'}${lastWriter ? ` · row by ${lastWriter}` : ''}` : '';
}

async function writeRoute() {
  const merged = mergeRegisters(routeDoc as any, routeMine as any);
  await zb.mutate('routes', 'UPDATE', { id: ROUTE_ID }, { doc: merged });
}

/// One end moved, by a click on the map or by a drag of the pin. Draw nothing here: the
/// pins come from the ROW, so what is on screen is what the row holds — including the
/// other editor's moves.
function moveEnd(key: 'start' | 'end', at: L.LatLng) {
  routeMine[key] = { v: { lat: at.lat, lng: at.lng }, t: stamp(), w: writer };
  void writeRoute().catch((err) => say(`route: ${err}`));
}

/// §10if: which end a click moves. There is NO alternating any more.
///
///   * nothing placed yet  -> the click places `start`
///   * only `start` placed -> it places `end`
///   * both placed         -> the HELD end, or the NEAREST one, which it then holds
///
/// The last rule is what makes "move this one, then move it again" work without a
/// separate select step. Alternating made moving `start` twice in a row impossible.
function routeTarget(at: L.LatLng): 'start' | 'end' {
  if (routeSelected) return routeSelected;
  const s = routeDoc.start?.v, e = routeDoc.end?.v;
  if (!s || typeof s.lat !== 'number') return 'start';
  if (!e || typeof e.lat !== 'number') return 'end';
  return map.distance(at, L.latLng(s.lat, s.lng)) <= map.distance(at, L.latLng(e.lat, e.lng))
    ? 'start' : 'end';
}

map.on('click', (e: L.LeafletMouseEvent) => {
  if (!routeMode) return;
  const which = routeTarget(e.latlng);
  routeSelected = which;   // whatever the click moved is now held
  moveEnd(which, e.latlng);
});

routeButton.addEventListener('click', () => {
  routeMode = !routeMode;
  if (!routeMode) routeSelected = null;
  routeButton.textContent = `route: ${routeMode ? 'on — click near an end to hold it, or drag it' : 'off'}`;
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
