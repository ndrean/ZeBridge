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
/// A finger is not a mouse pointer: on a touch screen, a tap counts within 12 px of a
/// charger (the canvas renderer's tolerance), and a tap that still misses picks the nearest
/// one within 30 px (below).
const TOUCH = matchMedia('(pointer: coarse)').matches;
const chargerRenderer = L.canvas({ tolerance: TOUCH ? 12 : 3 });
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
/// `stops`: the places between from and to, in order (a delivery round).
type Leg = { from: Point; stops?: Point[]; to: Point; started_at: string };
type Register<T> = { v: T; t: string; w: string };
type Plan = { leg?: Register<Leg> };
/// What the next charger click fills: From, To, a stop by its index, or a new stop.
type Active = 'from' | 'to' | 'add' | number;
/// This browser's own plan per truck: chosen, not yet sent.
type Draft = { from?: Point; stops?: Point[]; to?: Point };
const draft = new Map<string, Draft>();
type Truck = { id: string; name: string; depot: Point; plan: Plan };
type Charger = { id: string; title: string; town: string | null; lat: number; lng: number; max_power_kw: number | null };

const pointOf = (c: Charger): Point => ({ lat: c.lat, lng: c.lng, label: `${c.title}${c.town ? ` · ${c.town}` : ''}`, charger: c.id });

// ── trucks: rows, and what this browser wrote that the row does not show yet ─────
const trucks = new Map<string, Truck>();
const mine = new Map<string, Plan>();       // per truck: registers written here, not yet in the row
const rounds = new Map<string, number>();
let selected = '';
let active: Active | null = null;

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
        const n = now.v.stops?.length ?? 0;
        notice.textContent = `${now.w} sent ${r.name} to ${now.v.to.label}${n ? ` via ${n} stop(s)` : ''}`;
        // Someone else decided: what this screen was preparing for the truck is stale now.
        draft.delete(r.id);
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
  /// Seconds from the start at which each stop is reached (Valhalla's legs, one per stretch).
  stopAt: number[];
  by: string;
  ms: number;
};
const routes = new Map<string, Route | 'asking' | { error: string }>();
const legKey = (leg: Leg) => JSON.stringify([leg.from.lat, leg.from.lng, leg.from.heading ?? null,
  (leg.stops ?? []).map((p) => [p.lat, p.lng]), leg.to.lat, leg.to.lng]);

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

/// One question for the whole trip: from, every stop, to. Valhalla answers one leg per
/// stretch; they are joined into one line (each leg starts where the last ended) with the
/// maneuvers' shape indices moved along, and the time each stop is reached kept.
async function askRoute(leg: Leg): Promise<Route> {
  const t0 = performance.now();
  const { from, to } = leg;
  const a = await zb.request('query._default.route', {
    // A start with a heading: Valhalla keeps to roads leaving within 45° of it — no U-turn
    // in the street where the truck changed its mind.
    locations: [
      { lat: from.lat, lon: from.lng, ...(from.heading !== undefined ? { heading: from.heading, heading_tolerance: 45 } : {}) },
      ...(leg.stops ?? []).map((p) => ({ lat: p.lat, lon: p.lng })),
      { lat: to.lat, lon: to.lng },
    ],
    costing: 'truck',
    units: 'km',
  }, 15_000);
  if (a.error || !a.trip) throw new Error(String(a.error ?? a.detail ?? 'no route').slice(0, 200));
  const line: L.LatLngTuple[] = [];
  const maneuvers: Route['maneuvers'] = [];
  const stopAt: number[] = [];
  let t = 0;
  a.trip.legs.forEach((lg: any, li: number) => {
    const pts = decodePolyline6(lg.shape);
    // Index 0 of this leg is the last point of the line so far (the stop itself).
    const base = li === 0 ? 0 : line.length - 1;
    line.push(...(li === 0 ? pts : pts.slice(1)));
    for (const m of lg.maneuvers ?? []) {
      maneuvers.push({ b: m.begin_shape_index + base, e: m.end_shape_index + base, t0: t, time: m.time ?? 0 });
      t += m.time ?? 0;
    }
    if (li < a.trip.legs.length - 1) stopAt.push(t);
  });
  const cum = [0];
  for (let i = 1; i < line.length; i++) cum.push(cum[i - 1] + map.distance(line[i - 1], line[i]));
  return { line, cum, maneuvers, seconds: a.trip.summary.time, km: a.trip.summary.length, stopAt,
           by: a.by ?? '?', ms: Math.round(performance.now() - t0) };
}

function routeFor(leg: Leg): Route | null {
  const key = legKey(leg);
  const r = routes.get(key);
  if (r && r !== 'asking' && !('error' in r)) return r;
  if (!r) {
    routes.set(key, 'asking');
    askRoute(leg)
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
/// One colour per truck, by its place in the list: its route, its stops, the ring around
/// it — the same on every screen. The selected truck's route is drawn thicker.
const PALETTE = ['#d1361f', '#1f6feb', '#1a7f37', '#8e44ad', '#d68910', '#00838f'];
const colourOf = (id: string) => PALETTE[Math.max(0, [...trucks.keys()].indexOf(id)) % PALETTE.length];

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
      if (r) L.polyline(r.line, { color: colourOf(t.id), weight: isSel ? 6 : 4, opacity: isSel ? 0.95 : 0.7 }).addTo(routeLayer);
      // The selected truck's stops still ahead, numbered as in the panel.
      if (isSel) stopsLeft(leg, r, elapsedOf(leg)).forEach((p, k) => {
        L.marker([p.lat, p.lng], { icon: L.divIcon({ className: 'stopmark', html: `<span style="background:${colourOf(t.id)}">${k + 1}</span>`, iconSize: [18, 18] }), zIndexOffset: 500 })
          .bindTooltip(`stop ${k + 1} · ${p.label}`).addTo(routeLayer);
      });
    }
    const at = whereIs(t);
    if (at) {
      const mk = L.marker(at, { icon: L.divIcon({ className: 'truck', html: `<span style="border-color:${colourOf(t.id)}">🚚</span>`, iconSize: [26, 26] }), zIndexOffset: isSel ? 1000 : 0 })
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
  return `<b>${t.name}</b><br>${tripText(t) ?? `to ${leg.to.label}<br>route…`}`;
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

/// Where the truck is in its trip: the next stop and when, then the end and when.
function tripText(t: Truck): string | null {
  const leg = viewPlan(t.id).leg?.v;
  if (!leg) return null;
  const r = routeFor(leg);
  if (!r) return null;
  const now = elapsedOf(leg);
  if (now >= r.seconds) return `arrived at ${leg.to.label}`;
  const end = `${fmtMin(r.seconds - now)} to ${leg.to.label} (${new Date(Date.parse(leg.started_at) + r.seconds * 1000).toLocaleTimeString()})`;
  const k = (leg.stops ?? []).findIndex((_, i) => (r.stopAt[i] ?? 0) > now);
  return k >= 0 ? `next stop ${leg.stops![k].label} in ${fmtMin(r.stopAt[k] - now)} · ${end}` : end;
}

function showEta() {
  const t = trucks.get(selected);
  const text = t && tripText(t);
  eta.textContent = text ? `${t!.name}: ${text}` : '';
}

// ── the panel ──────────────────────────────────────────────────────────────────
/// The leg is under way (not arrived): a new destination then starts from where the
/// truck is now.
function enRoute(id: string): boolean {
  const leg = viewPlan(id).leg?.v;
  const r = leg && routeFor(leg);
  return !!(leg && r && elapsedOf(leg) < r.seconds);
}

/// The stops not reached yet, `elapsed` seconds into the leg.
function stopsLeft(leg: Leg, r: Route | null, elapsed: number): Point[] {
  const stops = leg.stops ?? [];
  return r ? stops.filter((_, k) => (r.stopAt[k] ?? 0) > elapsed) : stops;
}

/// What is left of the truck's trip now: the stops ahead, and To.
function remaining(t: Truck): Draft {
  const leg = viewPlan(t.id).leg?.v;
  if (!leg) return {};
  return { stops: stopsLeft(leg, routeFor(leg), elapsedOf(leg)), to: leg.to };
}

/// What the panel shows: this browser's draft, or else the trip as it stands.
const viewDraft = (t: Truck): Draft => draft.get(t.id) ?? remaining(t);

/// Start editing: the trip as it stands becomes this browser's draft (AB → C → B).
function editDraft(t: Truck): Draft {
  let d = draft.get(t.id);
  if (!d) {
    const r = remaining(t);
    d = { ...r, stops: [...(r.stops ?? [])] };
    draft.set(t.id, d);
  }
  return d;
}

const placesOf = (x: Draft) => JSON.stringify([(x.stops ?? []).map((p) => [p.lat, p.lng]), x.to ? [x.to.lat, x.to.lng] : null]);

const stopsBox = el('stops');
/// The stops, in order, each with a ✕; then "+ Add a stop", always offered again.
function showStops(t: Truck | undefined, stops: Point[]) {
  stopsBox.innerHTML = '';
  if (!t) return;
  stops.forEach((p, k) => {
    // A div, not a <label>: a label's click would also press the ✕ inside it.
    const row = document.createElement('div');
    row.className = 'stoplabel';
    row.innerHTML = `<span style="font-size:12px;opacity:.8">Stop ${k + 1}</span>`;
    const line = document.createElement('div');
    line.className = 'stoprow';
    const box = document.createElement('div');
    box.className = `point${active === k ? ' active' : ''}`;
    box.tabIndex = 0;
    box.textContent = p.label;
    box.addEventListener('click', () => { editDraft(t); active = active === k ? null : k; showPanel(); });
    const x = document.createElement('button');
    x.textContent = '✕';
    x.title = 'remove this stop';
    x.addEventListener('click', () => {
      editDraft(t).stops!.splice(k, 1);
      active = null;
      showPanel();
      void drawChargers();
    });
    line.append(box, x);
    row.append(line);
    stopsBox.append(row);
  });
  const add = document.createElement('div');
  add.className = `point add${active === 'add' ? ' active' : ''}`;
  add.tabIndex = 0;
  add.textContent = active === 'add' ? 'now click a charger' : '+ Add a stop';
  add.addEventListener('click', () => { editDraft(t); active = active === 'add' ? null : 'add'; showPanel(); });
  stopsBox.append(add);
}

function showPanel() {
  const t = trucks.get(selected);
  const plan = t ? viewPlan(t.id) : {};
  const moving = t ? enRoute(t.id) : false;
  const d = t ? draft.get(t.id) : undefined;
  const v = t ? viewDraft(t) : {};

  const fromBox = el('from');
  let fromText = 'click here, then a charger', fromEmpty = true;
  if (t && plan.leg) {
    fromText = moving ? `${t.name}'s position (en route)` : `${t.name}'s position (${plan.leg.v.to.label})`;
    fromEmpty = false;
  } else if (t) {
    const p = d?.from ?? t.depot;
    fromText = `${p.label}${d?.from ? '' : ' (depot)'}`;
    fromEmpty = false;
  }
  fromBox.textContent = fromText;
  fromBox.classList.toggle('empty', fromEmpty);
  fromBox.classList.toggle('active', active === 'from');

  showStops(t, v.stops ?? []);

  // To: this browser's pick, or else where the truck is going now — the same on every
  // screen, including one whose own pick lost to another screen's.
  const toBox = el('to');
  toBox.textContent = v.to ? `${v.to.label}${!d && plan.leg ? (moving ? ' (on its way)' : ' (arrived)') : ''}` : 'click here, then a charger';
  toBox.classList.toggle('empty', !v.to);
  toBox.classList.toggle('active', active === 'to');

  // Something new to send: a draft with a To, different from the trip as it stands.
  const changed = !!t && !!d?.to && placesOf(d) !== placesOf(remaining(t));
  traceButton.textContent = moving ? 'Update route' : 'Trace route';
  traceButton.disabled = !changed;

  const leg = plan.leg?.v;
  const r = leg && routes.get(legKey(leg));
  if (!leg) { result.textContent = ''; detail.textContent = ''; }
  else if (!r || r === 'asking') { result.textContent = 'asking the route…'; detail.textContent = ''; }
  else if ('error' in r) { result.textContent = 'no route'; detail.textContent = r.error; }
  else {
    const n = leg.stops?.length ?? 0;
    result.textContent = `${r.km.toFixed(1)} km · ${fmtMin(r.seconds)} by truck${n ? ` · ${n} stop(s)` : ''}`;
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
    const t = trucks.get(selected);
    if (end === 'to' && t) editDraft(t);
    active = active === end ? null : end;
    showPanel();
  };
  box.addEventListener('click', choose);
  box.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); choose(); } });
}

/// The chargers on screen now: a tap between them picks the nearest.
let drawn: Charger[] = [];
map.on('click', (e: L.LeafletMouseEvent) => {
  if (closestArmed) { void closestTo(e.latlng.lat, e.latlng.lng, 'the place tapped'); return; }
  if (active === null || !drawn.length) return;
  const tap = map.latLngToContainerPoint(e.latlng);
  let best: Charger | null = null, bestPx = 30;
  for (const c of drawn) {
    const px = tap.distanceTo(map.latLngToContainerPoint([c.lat, c.lng]));
    if (px < bestPx) { best = c; bestPx = px; }
  }
  if (best) pick(best);
});

function pick(c: Charger) {
  if (closestArmed) { void closestTo(c.lat, c.lng, pointOf(c).label); return; }
  const t = trucks.get(selected);
  if (active === null || !t) return;
  const d = editDraft(t);
  const p = pointOf(c);
  if (active === 'add') { d.stops = [...(d.stops ?? []), p]; active = null; }
  else if (typeof active === 'number') { d.stops![active] = p; active = null; }
  else { d[active] = p; active = active === 'from' && !d.to ? 'to' : null; }
  showPanel();
  void drawChargers();
}

/// Trace route: the first leg from the planned From (or the depot), through the stops; once
/// a leg exists, a new one from where the truck is now, through the stops still ahead
/// and any added (AB → C → B).
traceButton.addEventListener('click', () => {
  const t = trucks.get(selected);
  if (!t) return;
  const plan = viewPlan(t.id);
  const d = draft.get(t.id);
  const to = d?.to;
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
    from = d?.from ?? t.depot;
  }
  setRegister(t.id, 'leg', { from, stops: d?.stops ?? [], to, started_at: new Date().toISOString() });
  draft.delete(t.id);   // sent: the panel shows the trip as it stands now
  active = null;
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
  drawn = rows.length > MAX_DRAWN ? [] : rows;   // nothing drawn: nothing to tap
  if (rows.length > MAX_DRAWN) {
    status.textContent = `more than ${MAX_DRAWN} chargers in view: zoom in to pick one`;
    return;
  }
  const st = trucks.get(selected);
  const v = st ? viewDraft(st) : {};
  const chosen = new Set([v.from?.charger, ...(v.stops ?? []).map((p) => p.charger), v.to?.charger].filter(Boolean));
  for (const c of rows) {
    const isChosen = chosen.has(c.id);
    const kw = Number(c.max_power_kw ?? 0);
    const colour = isChosen ? '#d1361f' : kw >= 43 ? '#1a7f37' : '#1f6feb';
    L.circleMarker([c.lat, c.lng], { renderer: chargerRenderer, bubblingMouseEvents: false, radius: isChosen ? 9 : TOUCH ? 7 : 5, color: colour, fillColor: colour, fillOpacity: 0.85, weight: 1 })
      .bindTooltip(`${c.title}${c.town ? ` · ${c.town}` : ''} · ${c.max_power_kw ?? '?'} kW`)
      .on('click', () => pick(c))
      .addTo(chargerLayer);
  }
  status.textContent = `${rows.length} charger(s) in view${timing}`;
}
map.on('moveend', () => { void drawChargers(); });

// ── the closest truck: the positions the page computes, asked of Valhalla ──────
/// Positions are never stored (every screen computes them from its leg and the clock), so
/// this is not a SQL question: the page sends where each truck is now to the routing
/// service's `matrix` — Valhalla, by truck, every truck to one place — and ranks them by
/// driving time. Closest by road, not as the crow flies: a river changes the answer.
let closestArmed = false;
const closestBtn = el('closestBtn') as HTMLButtonElement;
const closestBox = el('closest');
const targetLayer = L.layerGroup().addTo(map);
closestBtn.addEventListener('click', () => {
  closestArmed = !closestArmed;
  closestBtn.classList.toggle('armed', closestArmed);
  closestBtn.textContent = closestArmed ? 'Tap a place on the map…' : 'Closest truck…';
  if (closestArmed) { active = null; showPanel(); }
});

async function closestTo(lat: number, lng: number, label: string) {
  closestArmed = false;
  closestBtn.classList.remove('armed');
  closestBtn.textContent = 'Closest truck…';
  targetLayer.clearLayers();
  L.marker([lat, lng], { icon: L.divIcon({ className: 'target', html: '📍', iconSize: [22, 22] }), zIndexOffset: 2000 })
    .bindTooltip(label).addTo(targetLayer);
  const here = [...trucks.values()].map((t) => ({ t, at: whereIs(t) })).filter((x) => x.at) as { t: Truck; at: L.LatLngTuple }[];
  if (!here.length) { closestBox.textContent = 'no truck position yet'; return; }
  closestBox.textContent = `asking Valhalla for ${here.length} trucks…`;
  const t0 = performance.now();
  try {
    const a = await zb.request('query._default.matrix', {
      sources: here.map(({ at }) => ({ lat: at[0], lon: at[1] })),
      targets: [{ lat, lon: lng }],
      costing: 'truck',
      units: 'km',
    }, 15_000);
    const cells: any[] = (a.sources_to_targets ?? []).map((row: any[]) => row[0]);
    if (a.error || !cells.length) throw new Error(String(a.error ?? 'no answer'));
    const ranked = here.map((h, i) => ({ ...h, time: cells[i]?.time as number | null, km: cells[i]?.distance as number | null }))
      .sort((x, y) => (x.time ?? Infinity) - (y.time ?? Infinity));
    const ms = Math.round(performance.now() - t0);
    closestBox.innerHTML = `<b>To ${escapeHtml(label)}</b>, by road:<ol>${ranked.map((r) =>
      `<li><span style="color:${colourOf(r.t.id)}">●</span> ${escapeHtml(r.t.name)}: ${
        r.time == null ? 'no road' : `${fmtMin(r.time)} · ${r.km!.toFixed(1)} km`}</li>`).join('')
    }</ol><span style="font-size:12px;opacity:.75">one matrix question, ${ms} ms · by ${escapeHtml(a.by ?? '?')}</span>`;
    const best = ranked[0];
    if (best.time != null) {
      truckMarkers.get(best.t.id)?.bindPopup(`<b>${escapeHtml(best.t.name)}</b><br>closest: ${fmtMin(best.time)} by road`).openPopup();
    }
  } catch (e) {
    closestBox.textContent = `no answer: ${(e as Error).message}`;
  }
}

// ── what does my fleet do? SQL on the replica ──────────────────────────────────
/// The plan is JSON text in `trucks.plan`; the browser's SQLite opens it with its JSON
/// functions. `json_each` keeps an array's order, so the stops come out as driven.
const PRESETS: { label: string; sql: string }[] = [
  {
    label: 'Roadmaps: from, stops, to, since when',
    sql: `SELECT t.id                                   AS truck,
       t.plan ->> '$.leg.v.from.label'        AS "from",
       (SELECT group_concat(s.value ->> '$.label', ' → ')
          FROM json_each(t.plan, '$.leg.v.stops') AS s) AS stops,
       t.plan ->> '$.leg.v.to.label'          AS "to",
       t.plan ->> '$.leg.v.started_at'        AS started_at,
       t.plan ->> '$.leg.w'                   AS sent_by
FROM trucks t
ORDER BY t.id;`,
  },
  {
    label: 'Every stop, one row each',
    sql: `SELECT t.id AS truck, s.key + 1 AS stop, s.value ->> '$.label' AS charger
FROM trucks t, json_each(t.plan, '$.leg.v.stops') AS s
ORDER BY t.id, s.key;`,
  },
  {
    label: 'The trucks and their depots',
    sql: `SELECT t.id AS truck, t.name, c.title AS depot, c.town, c.max_power_kw AS kw
FROM trucks t JOIN charge_points c ON c.id = t.depot
ORDER BY t.id;`,
  },
  {
    label: 'Fast chargers per town, Pays de la Loire',
    sql: `SELECT town, count(*) AS chargers, max(max_power_kw) AS max_kw
FROM charge_points
WHERE deleted_at IS NULL AND max_power_kw >= 43
  AND lat BETWEEN 46.2 AND 48.6 AND lng BETWEEN -2.6 AND 0.9
GROUP BY town ORDER BY chargers DESC LIMIT 15;`,
  },
];
const presetSelect = el('preset') as HTMLSelectElement;
const sqlBox = el('sql') as HTMLTextAreaElement;
const sqlInfo = el('sqlinfo'), sqlErr = el('sqlerr'), sqlOut = el('sqlout');
const liveBox = el('live') as HTMLInputElement;
PRESETS.forEach((p, i) => presetSelect.add(new Option(p.label, String(i))));
sqlBox.value = PRESETS[0].sql;
presetSelect.addEventListener('change', () => { sqlBox.value = PRESETS[Number(presetSelect.value)].sql; void runSql(); });

const escapeHtml = (v: unknown) => String(v).replace(/[&<>"]/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[ch]!));
let sqlHasRun = false;
/// `zb.query` reads only: the library refuses anything that would write the replica.
async function runSql() {
  const q = sqlBox.value.trim();
  if (!q) return;
  sqlHasRun = true;
  const t0 = performance.now();
  try {
    const rows = await zb.query(q);
    const ms = Math.round(performance.now() - t0);
    sqlErr.textContent = '';
    const shown = rows.slice(0, 200);
    sqlInfo.textContent = `${rows.length} row(s) · ${ms} ms${rows.length > 200 ? ' · first 200' : ''}`;
    if (!shown.length) { sqlOut.innerHTML = ''; return; }
    const cols = Object.keys(shown[0]);
    sqlOut.innerHTML = `<table><thead><tr>${cols.map((c) => `<th>${escapeHtml(c)}</th>`).join('')}</tr></thead><tbody>${
      shown.map((r: any) => `<tr>${cols.map((c) => `<td>${r[c] === null ? '<i>NULL</i>' : escapeHtml(r[c])}</td>`).join('')}</tr>`).join('')
    }</tbody></table>`;
  } catch (e) {
    sqlErr.textContent = (e as Error).message;
    sqlInfo.textContent = '';
    sqlOut.innerHTML = '';
  }
}
el('run').addEventListener('click', () => { void runSql(); });
sqlBox.addEventListener('keydown', (e) => { if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); void runSql(); } });
(el('fleet') as HTMLDetailsElement).addEventListener('toggle', (e) => { if ((e.target as HTMLDetailsElement).open && !sqlHasRun) void runSql(); });
/// Live: a truck sent somewhere, on this screen or another, re-runs the query.
zb.onChange('trucks', () => { if (liveBox.checked && sqlHasRun) void runSql(); });

// ── go ─────────────────────────────────────────────────────────────────────────
/// How long the first screen took, once: connecting (enrolment and the replica's
/// catch-up included), then the first local queries.
let timing = '';
const secs = (ms: number) => `${(ms / 1000).toFixed(1)} s`;
/// When the library first reached each phase, from the script's start: identity, storage
/// and the NATS dial ('connected'), schemas ('migrated'), the seed ('snapshot'), the
/// streams followed ('cdc').
const phaseAt: Record<string, number> = {};
zb.onPhase((p) => { if (!(p in phaseAt)) phaseAt[p] = performance.now() - T0; });
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
const phases = (['connected', 'migrated', 'snapshot', 'cdc'] as const)
  .filter((p) => p in phaseAt).map((p) => `${p} ${secs(phaseAt[p])}`).join(', ');
timing = ` · page ${secs(T0)} · ready in ${secs(performance.now() - T0)} (connect ${secs(tConnected - T0)}: ${phases}; first draw ${secs(performance.now() - tConnected)})`;
status.textContent += timing;
await refresh();
