/// 09-event in a browser: one sensor's readings as curves, asked from the event service.
///
/// The page holds no copy of the table. `sensor_events` is declared ON DEMAND, so its
/// schema arrives and nothing is seeded; every half second the page asks the service
/// `query.<tenant>.moving_avg` for one sensor's series and redraws it. The service answers
/// from its DuckDB replica; PostgreSQL never sees the question. `zb.request` unpacks a
/// compressed answer, or one stored as an object, by itself.
///
///   ?principal=bob&tenant=globex   (default)      ?principal=alice&tenant=acme
///
/// A tenant's page can only ask about its own tenant: NATS refuses the publish otherwise.
import { ZeBridge } from 'zb-client-ts';

const qs = new URLSearchParams(location.search);
const PRINCIPAL = qs.get('principal') ?? 'bob';
const TENANT = qs.get('tenant') ?? 'globex';
const KINDS = ['temperature', 'humidity', 'pressure'] as const;
const UNITS = { temperature: '°C', humidity: '%', pressure: 'hPa' } as const;

const el = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const statusEl = el<HTMLElement>('status');
const canvas = el<HTMLCanvasElement>('chart');
const sensorIn = el<HTMLInputElement>('sensor');
const bucketIn = el<HTMLSelectElement>('bucket');
const windowIn = el<HTMLSelectElement>('window');
const spanIn = el<HTMLSelectElement>('span');

const creds = await fetch(`/creds/${PRINCIPAL}.creds`).then((r) => (r.ok ? r.text() : undefined));
if (!creds) statusEl.textContent = `no creds for ${PRINCIPAL}: is public/creds pointing at scripts/native/creds?`;

const zb = new ZeBridge({
  natsUrl: `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/nats`,
  principal: PRINCIPAL,
  creds,
  ondemandTables: ['sensor_events'], // the schema, no rows: the page only asks
});
await zb.connect();
el('who').textContent = `${PRINCIPAL} · ${TENANT}`;

type Row = [number, number, number, number, number]; // sensor_id, t_ms, bucket avg, n, moving avg
let series: Row[] = [];
let fresh: any = null;
let lastAsk = { ms: 0, db: 0 };

// ── asking ──────────────────────────────────────────────────────────────────
let asking = false;
async function askSeries() {
  if (asking) return;              // one question in flight at a time
  asking = true;
  const sensor = Math.max(0, Number(sensorIn.value) || 0);
  const t0 = performance.now();
  try {
    const ans = await zb.request(`query.${TENANT}.moving_avg`, {
      kind: KINDS[sensor % 3],        // sensors.py: sensor_id % 3 picks the kind
      sensor_id: sensor,
      series: true,
      // One window more than the span: the first point drawn already has a full window
      // behind it, instead of a moving average that starts from a single bucket.
      since_s: Number(spanIn.value) + Number(windowIn.value),
      window_s: Number(windowIn.value),
      bucket_ms: Number(bucketIn.value),
    });
    if (ans?.error) statusEl.textContent = `moving_avg: ${ans.error}`;
    else { series = ans.rows as Row[]; lastAsk = { ms: performance.now() - t0, db: ans.ms }; }
  } catch (e) {
    statusEl.textContent = `moving_avg: ${e}`;
  } finally {
    asking = false;
  }
  draw();
}

async function askFreshness() {
  try { fresh = await zb.request(`query.${TENANT}.freshness`, {}); } catch { fresh = null; }
  const f = fresh && !fresh.error ? fresh : null;
  statusEl.textContent = f
    ? `${TENANT}: ${f.rows.toLocaleString()} readings · newest ${f.age_ms} ms old · ${f.last_10s_per_s.toLocaleString()} readings/s` +
      ` · chart question ${lastAsk.ms.toFixed(0)} ms round trip (${lastAsk.db} ms in DuckDB)`
    : `freshness: ${fresh?.error ?? 'no answer — is event_service.py running?'}`;
}

// ── drawing ─────────────────────────────────────────────────────────────────
function draw() {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, h = canvas.clientHeight;
  if (canvas.width !== w * dpr || canvas.height !== h * dpr) { canvas.width = w * dpr; canvas.height = h * dpr; }
  const g = canvas.getContext('2d')!;
  g.setTransform(dpr, 0, 0, dpr, 0, 0);
  g.clearRect(0, 0, w, h);
  const css = getComputedStyle(document.documentElement);
  const color = (v: string) => css.getPropertyValue(v).trim();

  const left = 58, right = 12, top = 12, bottom = 26;
  const pw = w - left - right, ph = h - top - bottom;
  const span = Number(spanIn.value) * 1000;
  const now = Date.now();
  const x = (t: number) => left + ((t - (now - span)) / span) * pw;

  const shown = series.filter((r) => r[1] >= now - span);
  const values = shown.flatMap((r) => [r[2], r[4]]);
  const kind = KINDS[Math.max(0, Number(sensorIn.value) || 0) % 3];
  g.font = '11px system-ui, sans-serif';
  g.fillStyle = color('--muted');
  if (values.length === 0) {
    g.fillText('no readings in this span: are the sensors running?', left + 8, top + 20);
    return;
  }
  let lo = Math.min(...values), hi = Math.max(...values);
  const pad = (hi - lo) * 0.1 || 0.5;
  lo -= pad; hi += pad;
  const y = (v: number) => top + (1 - (v - lo) / (hi - lo)) * ph;

  // grid and axes: five horizontal lines with values, a tick every 5 s
  g.strokeStyle = color('--line'); g.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const v = lo + ((hi - lo) * i) / 4, yy = y(v);
    g.beginPath(); g.moveTo(left, yy); g.lineTo(w - right, yy); g.stroke();
    g.fillText(`${v.toFixed(2)} ${UNITS[kind]}`, 4, yy + 4);
  }
  for (let s = 0; s <= span / 1000; s += 5) {
    const xx = x(now - s * 1000);
    g.fillText(s === 0 ? 'now' : `-${s} s`, xx - 12, h - 8);
  }

  const line = (col: number, stroke: string, width: number) => {
    g.strokeStyle = stroke; g.lineWidth = width; g.lineJoin = 'round';
    g.beginPath();
    shown.forEach((r, i) => (i === 0 ? g.moveTo(x(r[1]), y(r[col])) : g.lineTo(x(r[1]), y(r[col]))));
    g.stroke();
  };
  line(2, color('--raw'), 1.5);   // the bucket averages: the wave, at 100 ms buckets
  line(4, color('--avg'), 2.5);   // the moving average: the wave cancelled, the drift left
}

window.addEventListener('resize', draw);
for (const input of [sensorIn, bucketIn, windowIn, spanIn]) input.addEventListener('change', askSeries);
setInterval(askSeries, 500);
setInterval(askFreshness, 1000);
askSeries();
askFreshness();
