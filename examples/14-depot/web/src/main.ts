/// The depot demo: trucks, their plans, and a route from Valhalla.
///
/// Two flows meet on this page:
///
///   * DATA, replicated into this browser's SQLite: `charge_points` (all of France; the
///     chargers in view are a local query on every pan) and `trucks` (five trucks, each
///     with a depot and a plan).
///   * QUESTIONS, answered by the routing service: `query._default.route` forwards to
///     Valhalla, which knows roads only. The page sends coordinates, draws the line.
///
/// A truck's `plan` is a document holding one register {v, t, w} (COOPERATIVE_EDITING.md):
///
///   leg        the trip under way: {from, to, started_at}. "Trace route" writes it. A
///              change of destination after departure writes a NEW leg from the truck's
///              position at that moment (AB → C). Two screens sending the same truck
///              somewhere at once: the later stamp wins on every screen.
///
/// From and To are a DRAFT, in this browser only: picking a charger writes nothing and
/// moves no other screen. Only the button writes, the leg.
///
/// The route's line is never stored: every browser asks for its leg's route and walks the
/// truck along it by the maneuvers' durations, from `started_at`. Every screen shows the
/// truck at the same spot, and nothing is sent while it moves.
///
/// The first load enrolls with `?invite=<code>`; the identity is kept in this browser.
/// `?as=<name>` keeps a separate identity and replica (two editors in one browser).
import L from 'leaflet';
import { ZeBridge, NotEnrolled, mergeRegisters } from 'zb-client-ts';

const T0 = performance.now();
const NANTES: L.LatLngTuple = [47.2184, -1.5536];
/// Chargers drawn at once: past this the view is too wide to pick one anyway.
const MAX_DRAWN = 1500;

const qs = new URLSearchParams(location.search);
const el = (id: string) => document.getElementById(id)!;
const status = el('status'), result = el('result'), detail = el('detail'), eta = el('eta'), notice = el('notice');
const traceButton = el('trace') as HTMLButtonElement;
const truckSelect = el('truck') as HTMLSelectElement;

/// Built for a deployment (`VITE_ZB_BRIDGE_URL=https://bridge.example.com pnpm build`), the
/// page enrolls there and the answer names the NATS websocket. Without it, the dev
/// server's proxy carries both on this page's own origin.
const DEPLOYED_BRIDGE = import.meta.env.VITE_ZB_BRIDGE_URL as string | undefined;
const NATS_URL = import.meta.env.VITE_ZB_NATS_URL as string | undefined;

const zb = new ZeBridge({
  natsUrl: NATS_URL ?? (DEPLOYED_BRIDGE ? undefined : `${location.origin.replace(/^http/, 'ws')}/nats`),
  bridgeUrl: DEPLOYED_BRIDGE ?? `${location.origin}/bridge`,
  invite: qs.get('invite') ?? undefined,
  // `?as=<name>`: a separate identity and replica, so two people can share one browser.
  dbPath: qs.get('as') ? `depot-${qs.get('as')}.sqlite3` : 'depot.sqlite3',
  tables: ['charge_points', 'trucks'],
});
(window as any).zb = zb;

// ── the map ────────────────────────────────────────────────────────────────────
// Canvas, not one DOM element per marker: a wide view holds a thousand chargers.
const map = L.map('map', { preferCanvas: true }).setView(NANTES, 9);
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 18,
  attribution: '&copy; OpenStreetMap contributors',
  // The page is cross-origin isolated (for OPFS): tiles must be fetched with CORS.
  crossOrigin: '',
}).addTo(map);
new ResizeObserver(() => map.invalidateSize()).observe(el('map'));
const chargerLayer = L.layerGroup().addTo(map);
const routeLayer = L.layerGroup().addTo(map);
const truckLayer = L.layerGroup().addTo(map);

// ── types ──────────────────────────────────────────────────────────────────────
/// `heading`: a mid-route start's direction of travel, degrees from north (Valhalla's own field).
type Point = { lat: number; lng: number; label: string; charger?: string; heading?: number };
type Leg = { from: Point; to: Point; started_at: string };
type Register<T> = { v: T; t: string; w: string };
type Plan = { leg?: Register<Leg> };
type End = 'from' | 'to';
/// This browser's own From/To per truck: chosen, not yet sent.
const draft = new Map<string, { from?: Point; to?: Point }>();
type Truck = { id: string; name: string; depot: Point; plan: Plan };
type Charger = { id: string; title: string; town: string | null; lat: number; lng: number; max_power_kw: number | null };

const pointOf = (c: Charger): Point => ({ lat: c.lat, lng: c.lng, label: `${c.title}${c.town ? ` · ${c.town}` : ''}`, charger: c.id });

// ── trucks: rows, and what this browser wrote that the row does not show yet ─────
const trucks = new Map<string, Truck>();
const mine = new Map<string, Plan>();       // per truck: registers written here, not yet in the row
const rounds = new Map<string, number>();
let selected = '';
/// Which planned end the next charger click fills.
let active: End | null = null;

/// The plan as this browser sees it: its own pending registers over the row's.
const viewPlan = (id: string): Plan => ({ ...(trucks.get(id)?.plan ?? {}), ...(mine.get(id) ?? {}) });

async function readTrucks(): Promise<void> {
  const rows = await zb.query(
    `SELECT t.id, t.name, t.plan, c.id AS depot_id, c.title, c.town, c.lat, c.lng
     FROM trucks t JOIN charge_points c ON c.id = t.depot WHERE t.deleted_at IS NULL ORDER BY t.id`,
  );
  for (const r of rows) {
    const plan: Plan = r.plan ? (typeof r.plan === 'string' ? JSON.parse(r.plan) : r.plan) : {};
    const known = trucks.has(r.id);   // first sight: its trip is not news
    const before = trucks.get(r.id)?.plan ?? {};
    trucks.set(r.id, {
      id: r.id, name: r.name, plan,
      depot: { lat: r.lat, lng: r.lng, label: `${r.title}${r.town ? ` · ${r.town}` : ''}`, charger: r.depot_id },
    });
    // Say what someone else changed, and drop what the row now holds of mine.
    const m = mine.get(r.id);
    for (const k of ['leg'] as const) {
      const now = plan[k];
      if (!now || now.t === before[k]?.t) continue;
      if (known && now.w !== zb.principal) {
        notice.textContent = `${now.w} sent ${r.name} to ${now.v.to.label}`;
        // Someone else decided: a To picked here for this truck is stale now.
        const d = draft.get(r.id);
        if (d) delete d.to;
      }
      if (m?.[k] && now.t >= m[k]!.t) delete m[k];
    }
    if (m && !Object.keys(m).length) mine.delete(r.id);
  }
  if (truckSelect.options.length !== trucks.size) {
    truckSelect.innerHTML = '';
    for (const t of trucks.values()) truckSelect.add(new Option(t.name, t.id));
    if (!selected && trucks.size) selected = [...trucks.keys()][0];
    truckSelect.value = selected;
  }
}

async function writePlan(id: string): Promise<void> {
  const merged = mergeRegisters((trucks.get(id)?.plan ?? {}) as any, (mine.get(id) ?? {}) as any);
  await zb.mutate('trucks', 'UPDATE', { id }, { plan: merged });
}

function setRegister(id: string, key: 'leg', v: Leg): void {
  const m = mine.get(id) ?? {};
  (m as any)[key] = { v, t: zb.stamp(), w: zb.principal };
  mine.set(id, m);
  rounds.set(id, 0);
  void writePlan(id).catch((e) => { notice.textContent = `write: ${(e as Error).message}`; });
  void refresh();
}

/// The row moved, by me or by someone else: redraw from it, then reconcile — write the
/// merge again while the row does not hold what this browser wrote.
zb.onChange('trucks', () => {
  void (async () => {
    await readTrucks();
    for (const [id, m] of mine) {
      const n = rounds.get(id) ?? 0;
      if (!Object.keys(m).length || n >= 10) continue;
      const merged = mergeRegisters((trucks.get(id)?.plan ?? {}) as any, m as any);
      if (JSON.stringify(merged) !== JSON.stringify(trucks.get(id)?.plan ?? {})) {
        rounds.set(id, n + 1);
        await writePlan(id);
      }
    }
    await refresh();
  })();
});

// ── routes: asked per leg, never stored ────────────────────────────────────────
/// A leg's route: the line, the distance along it, and the maneuvers' timing — what the
/// truck is walked along.
type Route = {
  line: L.LatLngTuple[];
  cum: number[];                                  // metres from the start, per point
  maneuvers: { b: number; e: number; t0: number; time: number }[];
  seconds: number;
  km: number;
  by: string;
  ms: number;
};
const routes = new Map<string, Route | 'asking' | { error: string }>();
const legKey = (leg: Leg) => JSON.stringify([leg.from.lat, leg.from.lng, leg.from.heading ?? null, leg.to.lat, leg.to.lng]);

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

async function askRoute(from: Point, to: Point): Promise<Route> {
  const t0 = performance.now();
  const a = await zb.request('query._default.route', {
    // A start with a heading: Valhalla keeps to roads leaving within 45° of it — no U-turn
    // in the street where the truck changed its mind.
    locations: [
      { lat: from.lat, lon: from.lng, ...(from.heading !== undefined ? { heading: from.heading, heading_tolerance: 45 } : {}) },
      { lat: to.lat, lon: to.lng },
    ],
    costing: 'truck',
    units: 'km',
  }, 15_000);
  if (a.error || !a.trip) throw new Error(String(a.error ?? a.detail ?? 'no route').slice(0, 200));
  const leg = a.trip.legs[0];
  const line = decodePolyline6(leg.shape);
  const cum = [0];
  for (let i = 1; i < line.length; i++) cum.push(cum[i - 1] + map.distance(line[i - 1], line[i]));
  let t = 0;
  const maneuvers = (leg.maneuvers ?? []).map((m: any) => {
    const out = { b: m.begin_shape_index, e: m.end_shape_index, t0: t, time: m.time ?? 0 };
    t += out.time;
    return out;
  });
  return { line, cum, maneuvers, seconds: a.trip.summary.time, km: a.trip.summary.length, by: a.by ?? '?', ms: Math.round(performance.now() - t0) };
}

function routeFor(leg: Leg): Route | null {
  const key = legKey(leg);
  const r = routes.get(key);
  if (r && r !== 'asking' && !('error' in r)) return r;
  if (!r) {
    routes.set(key, 'asking');
    askRoute(leg.from, leg.to)
      .then((route) => { routes.set(key, route); void refresh(); })
      .catch((e) => { routes.set(key, { error: (e as Error).message }); void refresh(); });
  }
  return null;
}

/// Where the truck is, `elapsed` seconds into its leg: the maneuver it is in by time,
/// then that stretch of line by distance. Slower in town, faster on the motorway.
function positionAt(r: Route, elapsed: number): L.LatLngTuple {
  return locate(r, elapsed).at;
}

/// The position and the stretch of line it is on (`i` → `i + 1`).
function locate(r: Route, elapsed: number): { at: L.LatLngTuple; i: number } {
  const last = r.line.length - 1;
  if (elapsed <= 0) return { at: r.line[0], i: 0 };
  if (elapsed >= r.seconds) return { at: r.line[last], i: Math.max(0, last - 1) };
  const m = r.maneuvers.find((x) => elapsed < x.t0 + x.time) ?? r.maneuvers[r.maneuvers.length - 1];
  const f = m.time > 0 ? (elapsed - m.t0) / m.time : 1;
  const target = r.cum[m.b] + f * (r.cum[m.e] - r.cum[m.b]);
  let i = m.b;
  while (i < m.e && r.cum[i + 1] < target) i++;
  const span = r.cum[i + 1] - r.cum[i];
  const g = span > 0 ? (target - r.cum[i]) / span : 0;
  const [a, b] = [r.line[i], r.line[Math.min(i + 1, last)]];
  return { at: [a[0] + g * (b[0] - a[0]), a[1] + g * (b[1] - a[1])], i: Math.min(i, Math.max(0, last - 1)) };
}

/// The truck's direction of travel, in degrees clockwise from north: the bearing of the
/// stretch of line it is on. Sent with a new leg's start, so Valhalla prefers to carry on
/// forward instead of planning a U-turn in the street.
function headingAt(r: Route, elapsed: number): number | undefined {
  const { i } = locate(r, elapsed);
  const [a, b] = [r.line[i], r.line[i + 1]];
  if (!a || !b || (a[0] === b[0] && a[1] === b[1])) return undefined;
  const rad = Math.PI / 180;
  const y = Math.sin((b[1] - a[1]) * rad) * Math.cos(b[0] * rad);
  const x = Math.cos(a[0] * rad) * Math.sin(b[0] * rad) - Math.sin(a[0] * rad) * Math.cos(b[0] * rad) * Math.cos((b[1] - a[1]) * rad);
  return Math.round(((Math.atan2(y, x) / rad) + 360) % 360);
}

const elapsedOf = (leg: Leg) => (Date.now() - Date.parse(leg.started_at)) / 1000;

/// The truck's position now: on its leg's route, at its depot, or null while the
/// route is being asked.
function whereIs(t: Truck): L.LatLngTuple | null {
  const leg = viewPlan(t.id).leg?.v;
  if (!leg) return [t.depot.lat, t.depot.lng];
  const r = routeFor(leg);
  return r ? positionAt(r, elapsedOf(leg)) : null;
}

// ── drawing ────────────────────────────────────────────────────────────────────
const fmtMin = (s: number) => (s >= 3600 ? `${Math.floor(s / 3600)} h ${Math.round((s % 3600) / 60)} min` : `${Math.round(s / 60)} min`);
const truckMarkers = new Map<string, L.Marker>();

async function refresh(): Promise<void> {
  routeLayer.clearLayers();
  truckLayer.clearLayers();
  truckMarkers.clear();
  for (const t of trucks.values()) {
    const plan = viewPlan(t.id);
    const leg = plan.leg?.v;
    const isSel = t.id === selected;
    L.marker([t.depot.lat, t.depot.lng], { icon: L.divIcon({ className: 'depot', iconSize: [14, 14] }) })
      .bindTooltip(`${t.name}'s depot · ${t.depot.label}`).addTo(truckLayer);
    if (leg) {
      const r = routeFor(leg);
      if (r) L.polyline(r.line, { color: isSel ? '#d1361f' : '#888', weight: isSel ? 5 : 3, opacity: isSel ? 0.85 : 0.6 }).addTo(routeLayer);
    }
    const at = whereIs(t);
    if (at) {
      const mk = L.marker(at, { icon: L.divIcon({ className: 'truck', html: '🚚', iconSize: [22, 22] }), zIndexOffset: isSel ? 1000 : 0 })
        .bindPopup(() => truckPopup(t))
        .on('click', () => selectTruck(t.id))
        .addTo(truckLayer);
      truckMarkers.set(t.id, mk);
    }
  }
  showPanel();
}

function truckPopup(t: Truck): string {
  const leg = viewPlan(t.id).leg?.v;
  if (!leg) return `<b>${t.name}</b><br>at its depot<br>${t.depot.label}`;
  const r = routeFor(leg);
  if (!r) return `<b>${t.name}</b><br>to ${leg.to.label}<br>route…`;
  const left = r.seconds - elapsedOf(leg);
  return `<b>${t.name}</b><br>to ${leg.to.label}<br>${left > 0 ? `arrives in ${fmtMin(left)} (${new Date(Date.parse(leg.started_at) + r.seconds * 1000).toLocaleTimeString()})` : 'arrived'}`;
}

/// Every second: move the trucks and the ETA. Positions only; the layers stay.
setInterval(() => {
  for (const [id, mk] of truckMarkers) {
    const t = trucks.get(id);
    const at = t && whereIs(t);
    if (at) mk.setLatLng(at);
  }
  showEta();
}, 1000);

function showEta() {
  const t = trucks.get(selected);
  const leg = t && viewPlan(t.id).leg?.v;
  if (!t || !leg) { eta.textContent = ''; return; }
  const r = routeFor(leg);
  if (!r) { eta.textContent = ''; return; }
  const left = r.seconds - elapsedOf(leg);
  eta.textContent = left > 0
    ? `${t.name}: ${fmtMin(left)} to ${leg.to.label}`
    : `${t.name} arrived at ${leg.to.label}`;
}

// ── the panel ──────────────────────────────────────────────────────────────────
/// The leg is under way (not arrived): a new destination then starts from where the
/// truck is now.
function enRoute(id: string): boolean {
  const leg = viewPlan(id).leg?.v;
  const r = leg && routeFor(leg);
  return !!(leg && r && elapsedOf(leg) < r.seconds);
}

function showPanel() {
  const t = trucks.get(selected);
  const plan = t ? viewPlan(t.id) : {};
  const moving = t ? enRoute(t.id) : false;
  const arrived = t && plan.leg && !moving;
  for (const end of ['from', 'to'] as const) {
    const box = el(end);
    let text = 'click here, then a charger';
    let empty = true;
    if (end === 'from' && t && plan.leg) {
      text = moving ? `${t.name}'s position (en route)` : `${t.name}'s position (${plan.leg.v.to.label})`;
      empty = false;
    } else {
      const d = t ? draft.get(t.id) : undefined;
      // To: this browser's pick, or else where the truck is going now — the same on every
      // screen, including one whose own pick lost to another screen's.
      const p = end === 'from' ? (d?.from ?? t?.depot) : (d?.to ?? plan.leg?.v.to);
      if (p) {
        const note = end === 'from' && !d?.from ? ' (depot)'
          : end === 'to' && !d?.to ? (moving ? ' (on its way)' : ' (arrived)') : '';
        text = `${p.label}${note}`;
        empty = false;
      }
    }
    box.textContent = text;
    box.classList.toggle('empty', empty);
    box.classList.toggle('active', active === end);
  }
  const to = t ? draft.get(t.id)?.to : undefined;
  const sameTrip = plan.leg && to && to.lat === plan.leg.v.to.lat && to.lng === plan.leg.v.to.lng;
  traceButton.textContent = moving ? 'Change destination' : 'Trace route';
  traceButton.disabled = !t || !to || !!sameTrip && (moving || !!arrived);
  const leg = plan.leg?.v;
  const r = leg && routes.get(legKey(leg));
  if (!leg) { result.textContent = ''; detail.textContent = ''; }
  else if (!r || r === 'asking') { result.textContent = 'asking the route…'; detail.textContent = ''; }
  else if ('error' in r) { result.textContent = 'no route'; detail.textContent = r.error; }
  else {
    result.textContent = `${r.km.toFixed(1)} km · ${fmtMin(r.seconds)} by truck`;
    detail.textContent = `by ${r.by} · ${r.ms} ms round trip · leg by ${plan.leg!.w}`;
  }
  showEta();
}

function selectTruck(id: string) {
  selected = id;
  truckSelect.value = id;
  active = null;
  notice.textContent = '';
  void refresh();
}
truckSelect.addEventListener('change', () => selectTruck(truckSelect.value));

for (const end of ['from', 'to'] as const) {
  const box = el(end);
  const choose = () => {
    if (end === 'from' && viewPlan(selected).leg) return;   // once under way, the start is the truck
    active = active === end ? null : end;
    showPanel();
  };
  box.addEventListener('click', choose);
  box.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); choose(); } });
}

function pick(c: Charger) {
  if (!active || !selected) return;
  const d = draft.get(selected) ?? {};
  d[active] = pointOf(c);
  draft.set(selected, d);
  active = active === 'from' && !d.to ? 'to' : null;
  showPanel();
  void drawChargers();
}

/// Trace route: the first leg from the planned From (or the depot); once a leg exists,
/// a new one from where the truck is now (AB → C).
traceButton.addEventListener('click', () => {
  const t = trucks.get(selected);
  if (!t) return;
  const plan = viewPlan(t.id);
  const to = draft.get(t.id)?.to;
  if (!to) return;
  let from: Point;
  if (plan.leg) {
    const r = routeFor(plan.leg.v);
    if (!r) { notice.textContent = 'the current route is still being asked — try again in a second'; return; }
    const elapsed = elapsedOf(plan.leg.v);
    const at = positionAt(r, elapsed);
    // Arrived: no direction to keep. On the way: the one it is driving in, stored in the
    // leg so every screen asks Valhalla the same question.
    const heading = elapsed < r.seconds ? headingAt(r, elapsed) : undefined;
    from = { lat: at[0], lng: at[1], label: `${t.name}'s position`, ...(heading !== undefined ? { heading } : {}) };
  } else {
    from = draft.get(t.id)?.from ?? t.depot;
  }
  setRegister(t.id, 'leg', { from, to, started_at: new Date().toISOString() });
});

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
  const d = draft.get(selected);
  const chosen = new Set([d?.from?.charger, d?.to?.charger].filter(Boolean));
  for (const c of rows) {
    const isChosen = chosen.has(c.id);
    const kw = Number(c.max_power_kw ?? 0);
    const colour = isChosen ? '#d1361f' : kw >= 43 ? '#1a7f37' : '#1f6feb';
    L.circleMarker([c.lat, c.lng], { radius: isChosen ? 9 : 5, color: colour, fillColor: colour, fillOpacity: 0.85, weight: 1 })
      .bindTooltip(`${c.title}${c.town ? ` · ${c.town}` : ''} · ${c.max_power_kw ?? '?'} kW`)
      .on('click', () => pick(c))
      .addTo(chargerLayer);
  }
  status.textContent = `${rows.length} charger(s) in view${timing}`;
}
map.on('moveend', () => { void drawChargers(); });

// ── go ─────────────────────────────────────────────────────────────────────────
/// How long the first screen took, once: connecting (enrolment and the replica's
/// catch-up included), then the first local queries.
let timing = '';
const secs = (ms: number) => `${(ms / 1000).toFixed(1)} s`;
try {
  status.textContent = 'connecting… (a first visit copies 16,000 chargers into this browser)';
  await zb.connect();
} catch (e) {
  status.textContent = e instanceof NotEnrolled
    ? 'This browser is not enrolled yet: open the invite link you were given (…/?invite=<code>).'
    : `Could not connect: ${(e as Error).message}`;
  throw e;
}
const tConnected = performance.now();
zb.onChange('charge_points', () => { void drawChargers(); });
await readTrucks();
await drawChargers();
// T0 itself is the time from navigation to this script: the HTML, the script, the stylesheet.
timing = ` · page ${secs(T0)} · ready in ${secs(performance.now() - T0)} (connect ${secs(tConnected - T0)}, first draw ${secs(performance.now() - tConnected)})`;
status.textContent += timing;
await refresh();
