/// The depot demo, first milestone: a road route between two charge points.
///
/// Two flows meet on this page:
///
///   * the CHARGE POINTS are a replicated table — `charge_points`, all of France, followed
///     into this browser's SQLite. The chargers in view are a local query on every pan:
///     no service is asked for them.
///   * the ROUTE is a question — `query._default.route` to the routing service, which
///     forwards it to Valhalla. Valhalla knows roads only: the page sends the two
///     chargers' coordinates, and draws the shape that comes back.
///
/// The first load enrolls with `?invite=<code>`; the identity is kept in this browser.
import L from 'leaflet';
import { ZeBridge, NotEnrolled } from 'zb-client-ts';

const NANTES: L.LatLngTuple = [47.2184, -1.5536];
/// Chargers drawn at once: past this the view is too wide to pick one anyway.
const MAX_DRAWN = 1500;

const qs = new URLSearchParams(location.search);
const el = (id: string) => document.getElementById(id)!;
const status = el('status'), result = el('result'), detail = el('detail');
const traceButton = el('trace') as HTMLButtonElement;

/// Built for a deployment (`VITE_ZB_BRIDGE_URL=https://bridge.example.com pnpm build`), the
/// page enrolls there and the answer names the NATS websocket. Without it, the dev
/// server's proxy carries both on this page's own origin.
const DEPLOYED_BRIDGE = import.meta.env.VITE_ZB_BRIDGE_URL as string | undefined;
const NATS_URL = import.meta.env.VITE_ZB_NATS_URL as string | undefined;

const zb = new ZeBridge({
  natsUrl: NATS_URL ?? (DEPLOYED_BRIDGE ? undefined : `${location.origin.replace(/^http/, 'ws')}/nats`),
  bridgeUrl: DEPLOYED_BRIDGE ?? `${location.origin}/bridge`,
  invite: qs.get('invite') ?? undefined,
  dbPath: 'depot.sqlite3',
  tables: ['charge_points'],
});
(window as any).zb = zb;

// ── the map ────────────────────────────────────────────────────────────────────
// Canvas, not one DOM element per marker: a wide view holds a thousand chargers.
const map = L.map('map', { preferCanvas: true }).setView(NANTES, 11);
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 18,
  attribution: '&copy; OpenStreetMap contributors',
  // The page is cross-origin isolated (for OPFS): tiles must be fetched with CORS.
  crossOrigin: '',
}).addTo(map);
new ResizeObserver(() => map.invalidateSize()).observe(el('map'));
const chargerLayer = L.layerGroup().addTo(map);
const routeLayer = L.layerGroup().addTo(map);

// ── From and To ────────────────────────────────────────────────────────────────
type Charger = { id: string; title: string; town: string | null; lat: number; lng: number; max_power_kw: number | null };
type End = 'from' | 'to';
const picked: Record<End, Charger | null> = { from: null, to: null };
/// Which input the next charger click fills.
let active: End | null = 'from';

function showPoints() {
  for (const end of ['from', 'to'] as const) {
    const box = el(end);
    const c = picked[end];
    box.textContent = c ? `${c.title}${c.town ? ` · ${c.town}` : ''}` : 'click here, then a charger';
    box.classList.toggle('empty', !c);
    box.classList.toggle('active', active === end);
  }
  traceButton.disabled = !(picked.from && picked.to);
}

for (const end of ['from', 'to'] as const) {
  const box = el(end);
  const choose = () => { active = active === end ? null : end; showPoints(); };
  box.addEventListener('click', choose);
  box.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); choose(); } });
}

function pick(c: Charger) {
  if (!active) return;
  picked[active] = c;
  // After From, the next click goes to To; after To, nothing is held.
  active = active === 'from' && !picked.to ? 'to' : null;
  routeLayer.clearLayers();
  result.textContent = '';
  detail.textContent = '';
  showPoints();
  drawChargers();
}

// ── the chargers in view: a local query ────────────────────────────────────────
async function drawChargers() {
  const b = map.getBounds();
  const rows = (await zb.query(
    `SELECT id, title, town, lat, lng, max_power_kw FROM charge_points
     WHERE deleted_at IS NULL AND lat BETWEEN ? AND ? AND lng BETWEEN ? AND ? LIMIT ?`,
    b.getSouth(), b.getNorth(), b.getWest(), b.getEast(), MAX_DRAWN + 1,
  )) as Charger[];
  chargerLayer.clearLayers();
  if (rows.length > MAX_DRAWN) {
    status.textContent = `more than ${MAX_DRAWN} chargers in view: zoom in to pick one`;
    return;
  }
  for (const c of rows) {
    const chosen = picked.from?.id === c.id || picked.to?.id === c.id;
    const kw = Number(c.max_power_kw ?? 0);
    const colour = chosen ? '#d1361f' : kw >= 43 ? '#1a7f37' : '#1f6feb';
    L.circleMarker([c.lat, c.lng], { radius: chosen ? 9 : 6, color: colour, fillColor: colour, fillOpacity: 0.85, weight: 1 })
      .bindTooltip(`${c.title}${c.town ? ` · ${c.town}` : ''} · ${c.max_power_kw ?? '?'} kW`)
      .on('click', () => pick(c))
      .addTo(chargerLayer);
  }
  status.textContent = `${rows.length} charger(s) in view`;
}
map.on('moveend', () => { void drawChargers(); });

// ── the route: a question to Valhalla ──────────────────────────────────────────
/// Valhalla's shapes are Google's encoded polyline at 6 decimals (not 5).
function decodePolyline6(s: string): L.LatLngTuple[] {
  const out: L.LatLngTuple[] = [];
  let i = 0, lat = 0, lng = 0;
  while (i < s.length) {
    for (const axis of [0, 1]) {
      let shift = 0, value = 0, byte: number;
      do { byte = s.charCodeAt(i++) - 63; value |= (byte & 0x1f) << shift; shift += 5; } while (byte >= 0x20);
      const delta = value & 1 ? ~(value >> 1) : value >> 1;
      if (axis === 0) lat += delta; else lng += delta;
    }
    out.push([lat / 1e6, lng / 1e6]);
  }
  return out;
}

async function trace() {
  const { from, to } = picked;
  if (!from || !to) return;
  traceButton.disabled = true;
  result.textContent = 'asking…';
  detail.textContent = '';
  const t0 = performance.now();
  try {
    const a = await zb.request('query._default.route', {
      locations: [{ lat: from.lat, lon: from.lng }, { lat: to.lat, lon: to.lng }],
      costing: 'truck',
      units: 'km',
    }, 15_000);
    const ms = Math.round(performance.now() - t0);
    if (a.error || !a.trip) {
      result.textContent = 'no route';
      detail.textContent = String(a.error ?? a.detail ?? JSON.stringify(a)).slice(0, 300);
      return;
    }
    const s = a.trip.summary;
    const line = a.trip.legs.flatMap((leg: any) => decodePolyline6(leg.shape));
    routeLayer.clearLayers();
    L.polyline(line, { color: '#d1361f', weight: 5, opacity: 0.85 }).addTo(routeLayer);
    map.fitBounds(L.latLngBounds(line), { padding: [40, 40] });
    result.textContent = `${s.length.toFixed(1)} km · ${Math.round(s.time / 60)} min by truck`;
    detail.textContent = `by ${a.by ?? '?'} · ${a.ms ?? '?'} ms in Valhalla · ${ms} ms round trip`;
  } catch (e) {
    result.textContent = 'no answer';
    detail.textContent = `${(e as Error).message} — is the routing service running?`;
  } finally {
    traceButton.disabled = !(picked.from && picked.to);
  }
}
traceButton.addEventListener('click', () => { void trace(); });

// ── go ─────────────────────────────────────────────────────────────────────────
showPoints();
try {
  status.textContent = 'connecting… (the first visit copies 16,000 chargers into this browser)';
  await zb.connect();
} catch (e) {
  status.textContent = e instanceof NotEnrolled
    ? 'This browser is not enrolled yet: open the invite link you were given (…/?invite=<code>).'
    : `Could not connect: ${(e as Error).message}`;
  throw e;
}
zb.onChange('charge_points', () => { void drawChargers(); });
await drawChargers();
