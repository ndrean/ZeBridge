/// Example 15, first step: the current row of `site_survey`, and one write.
///
/// One row, two flavours of the same five fields (README):
///   * part A, five plain columns — a write sets one column;
///   * part B, one jsonb `doc` holding five registers {v, t, w} (COOPERATIVE_EDITING.md) —
///     a write merges one register into the current doc and writes the doc. A doc write
///     the row outran is lost WHOLE by the library (one column): the page then merges its
///     own registers into the winning doc and writes again — the merge per register.
///
/// The first load enrolls with `?invite=<code>`; the identity is kept in this browser.
/// `?as=<name>` keeps a separate identity and replica: several editors in one browser.
import { ZeBridge, NotEnrolled, mergeRegisters } from 'zb-client-ts';

const qs = new URLSearchParams(location.search);
const el = (id: string) => document.getElementById(id)!;
const rowBox = el('row'), status = el('status'), sent = el('sent'), who = el('who');
const fieldSelect = el('field') as HTMLSelectElement;
const registerSelect = el('register') as HTMLSelectElement;
const valueInput = el('value') as HTMLInputElement;
const netButton = el('net') as HTMLButtonElement;
const chip = el('chip');

/// Built for a deployment (`VITE_ZB_BRIDGE_URL=https://bridge.example.com pnpm build`), the
/// page enrolls there and the answer names the NATS websocket. Without it, the dev
/// server's proxy carries both on this page's own origin.
const DEPLOYED_BRIDGE = import.meta.env.VITE_ZB_BRIDGE_URL as string | undefined;
const NATS_URL = import.meta.env.VITE_ZB_NATS_URL as string | undefined;

const zb = new ZeBridge({
  natsUrl: NATS_URL ?? (DEPLOYED_BRIDGE ? undefined : `${location.origin.replace(/^http/, 'ws')}/nats`),
  bridgeUrl: DEPLOYED_BRIDGE ?? `${location.origin}/bridge`,
  invite: qs.get('invite') ?? undefined,
  dbPath: qs.get('as') ? `survey-${qs.get('as')}.sqlite3` : 'survey.sqlite3',
  tables: ['site_survey'],
});
(window as any).zb = zb;

/// The library's own log, whole, in the console…
const T0 = performance.now();
zb.onLog((topic, data, level) => console.info(`[zb ${((performance.now() - T0) / 1000).toFixed(1)} s] ${level} ${topic}`, data));

// ── activity: what happened to this editor's writes, on the page ───────────────
/// …and the part that tells the merge, on the page: each write sent, its verdict, and,
/// for a write the row outran, its rebase (columns disjoint: re-sent, it lands) or its
/// loss (a column the winner changed too: the winning row stands).
const activity = el('activity');
const MAX_ACTIVITY = 40;
function note(kind: string, text: string): void {
  const li = document.createElement('li');
  li.className = kind;
  li.innerHTML = `<time>${new Date().toTimeString().slice(0, 8)}</time>`;
  li.append(text);
  activity.prepend(li);
  while (activity.children.length > MAX_ACTIVITY) activity.lastElementChild!.remove();
}
const TABLE = 'site_survey';
const VERSION_COL = 'updated_at';
zb.onLog((topic, data, level) => {
  if (level === 'MUTATION OUT' && topic.includes(`.${TABLE}.`)) {
    const cols = Object.keys((data as any)?.data ?? {}).filter((c) => c !== VERSION_COL);
    note('sent', `sent: ${cols.join(', ') || '(no column)'} · version ${(data as any)?.version ?? '?'}`);
  } else if (level === 'VERDICT' && typeof data === 'object' && data) {
    const v = data as any;
    if (v.status === 'accepted') docWriteSettled(topic, true);
    note(v.status === 'accepted' ? 'accepted' : 'rejected', `${v.status}${v.reason ? ` (${v.reason})` : ''}`);
  } else if (typeof data === 'string' && topic.startsWith('mutation_ack')) {
    // stale, rejected, row_deleted: said in words by the library
    const kind = /newer version won/.test(data) ? 'stale' : /rejected|refused/.test(data) ? 'rejected' : 'stale';
    if (kind === 'stale') docWriteSettled(topic, false);
    note(kind, data.replace(/^[^:]*#[^:]*: /, ''));
  } else if (topic === TABLE && typeof data === 'string') {
    if (data.startsWith('rebased')) note('rebased', data);
    else if (/^edit LOST .*column\(s\) doc\b/.test(data)) note('stale', 'the whole doc lost to a newer one — the page re-merges its registers into it');
    else if (data.startsWith('edit LOST')) note('lost', data.replace(' — the winning row stands; surface this to the user', ' — the winning row stands'));
  }
});

const COLUMNS = ['access', 'hazard', 'contact', 'rating', 'notes'] as const;
type Register = { v: unknown; t: string; w: string };
type Row = Record<string, any> & { id: string; doc: Record<string, Register> };
let row: Row | null = null;

const escapeHtml = (v: unknown) => String(v).replace(/[&<>"]/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[ch]!));
const shown = (v: unknown) => (v === null || v === undefined || v === '' ? '—' : escapeHtml(v));

/// The last row written: the seeded one, or whichever moved last.
async function readRow(): Promise<void> {
  const rows = await zb.query(
    `SELECT id, access, hazard, contact, rating, notes, doc, updated_at, last_writer
     FROM site_survey WHERE deleted_at IS NULL ORDER BY updated_at DESC LIMIT 1`,
  );
  const r = rows[0];
  row = r ? { ...r, doc: typeof r.doc === 'string' ? JSON.parse(r.doc || '{}') : (r.doc ?? {}) } : null;
  show();
  await reconcile();
}

function show(): void {
  if (!row) { rowBox.textContent = 'no row in site_survey yet (seed it: README, the INSERT)'; return; }
  const a = COLUMNS.map((c) => `<b>${c}</b> ${shown(row![c])}`).join(' · ');
  const b = COLUMNS.map((c) => {
    const r = row!.doc[c];
    return `<span class="reg"><b>${c}</b> ${r ? `${shown(r.v)} <small>(${escapeHtml(r.w)}, ${escapeHtml(r.t.slice(11, 23))})</small>` : '—'}</span>`;
  }).join(' · ');
  rowBox.innerHTML =
    `<div>A · ${a}</div><div class="part">B · doc: ${b}</div>` +
    `<div class="meta">row ${escapeHtml(row.id.slice(0, 8))} · version ${escapeHtml(row.updated_at)} · last writer ${shown(row.last_writer)}</div>`;
}

/// `doc` writes one register: pick which.
fieldSelect.addEventListener('change', () => { registerSelect.hidden = fieldSelect.value !== 'doc'; });

el('edit').addEventListener('submit', (e) => {
  e.preventDefault();
  void write();
});

async function write(): Promise<void> {
  if (!row) { sent.textContent = 'no row to write to'; return; }
  const field = fieldSelect.value;
  const name = field === 'doc' ? registerSelect.value : field;
  const raw = valueInput.value.trim();
  // rating is an integer column; the register keeps the same type.
  const value: unknown = name === 'rating' ? Number.parseInt(raw, 10) : raw;
  if (name === 'rating' && !Number.isInteger(value)) { sent.textContent = 'rating is a whole number (1–5)'; return; }

  try {
    if (field === 'doc') {
      // One register, stamped by this client (the bridge's clock, never behind what it saw),
      // kept as MINE until the row holds it, and merged with the doc this replica holds:
      // the newer `t` wins per register.
      mine.set(name, { v: value, t: zb.stamp(), w: zb.principal });
      saveMine();
      const version = await writeDoc(row.doc);
      sent.textContent = `doc.${name} ← ${String(value)} · sent as version ${version}`;
    } else {
      const { version } = await zb.mutate('site_survey', 'UPDATE', { id: row.id }, { [name]: value });
      sent.textContent = `${name} ← ${String(value)} · sent as version ${version}`;
    }
    valueInput.value = '';
  } catch (err) {
    sent.textContent = `not sent: ${(err as Error).message}`;
  }
}

// ── part B: the merge per register ──────────────────────────────────────────────
/// MINE: the registers this editor wrote that the row has not confirmed. Kept in this
/// browser's storage too, so a reload while offline does not forget them.
const MINE_KEY = `zb-survey-mine-${qs.get('as') ?? ''}`;
const mine = new Map<string, Register>((() => {
  try { return Object.entries(JSON.parse(localStorage.getItem(MINE_KEY) ?? '{}')) as [string, Register][]; } catch { return []; }
})());
function saveMine(): void {
  try { localStorage.setItem(MINE_KEY, JSON.stringify(Object.fromEntries(mine))); } catch { /* private window */ }
}
/// The order mergeRegisters keeps: the later stamp, then the writer.
const newer = (a: Register, b: Register) => a.t > b.t || (a.t === b.t && a.w > b.w);
/// Each doc write in flight, by its version: which registers it carried.
const inFlight = new Map<string, Map<string, Register>>();
/// The version of a doc write the row outran: re-merge once the winning row is HERE —
/// merged into this replica's own optimistic copy, it would write the winner's other
/// registers back at their old values.
let outranBy: string | null = null;

async function writeDoc(base: Record<string, Register>): Promise<string> {
  const doc = mergeRegisters(base as any, Object.fromEntries(mine) as any);
  const carried = new Map([...mine].map(([k, r]) => [k, { ...r }]));
  const { version } = await zb.mutate(TABLE, 'UPDATE', { id: row!.id }, { doc });
  inFlight.set(version, carried);
  return version;
}

/// The version a verdict's subject names: its msg id ends with it, '.' written '-'
/// (…-2026-10-07T15:11:19-273000Z). Read from the subject, not from this page's memory,
/// so a write queued before a reload is still recognised.
function versionOf(topic: string): string | null {
  const m = /(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)-(\d+Z)$/.exec(topic);
  return m ? `${m[1]}.${m[2]}` : null;
}

function docWriteSettled(topic: string, accepted: boolean): void {
  if (!topic.includes(`-${TABLE}-`)) return;
  const version = versionOf(topic);
  if (version === null) return;
  const carried = inFlight.get(version);
  inFlight.delete(version);
  if (accepted) {
    // In the row now: no longer mine (unless written again since).
    if (carried) for (const [k, r] of carried) if (mine.get(k)?.t === r.t) mine.delete(k);
    saveMine();
  } else if (mine.size) {
    // Outrun: re-merge as soon as the winning row is here — it may already be (the
    // catch-up can bring it before this verdict), so look now as well as on the echo.
    outranBy = version;
    void readRow();
  }
}

/// On every row change: once the winner of an outrun write has arrived, each register
/// either lost to a newer stamp (said so, dropped) or is merged into the winner's doc and
/// written again.
let rounds = 0;
async function reconcile(): Promise<void> {
  if (!row || outranBy === null || row.updated_at === outranBy || !online) return;
  outranBy = null;
  for (const [k, r] of [...mine]) {
    const held = row.doc[k];
    if (held && newer(held, r)) {
      note('lost', `doc.${k}: ${held.w}'s newer value stands (${String(held.v)})`);
      mine.delete(k);
    }
  }
  saveMine();
  if (!mine.size) { rounds = 0; return; }
  if (++rounds > 5) { note('lost', `doc: still outrun after 5 re-merges — ${[...mine.keys()].join(', ')} kept here, not sent`); return; }
  note('rebased', `doc: ${[...mine.keys()].join(', ')} merged into the newer doc, written again`);
  await writeDoc(row.doc);
}

// ── offline, for real ──────────────────────────────────────────────────────────
/// DevTools' "Offline" does not cut a WebSocket already open, so the page hangs up
/// itself: `close()` drops the NATS connection; a write then applies here and waits in
/// the outbox (nothing is sent); `connect()` sends what waited, and the verdicts come back.
let online = false;
async function queued(): Promise<number> {
  try { return Number((await zb.query('SELECT count(*) AS n FROM _zebridge_outbox'))[0]?.n ?? 0); } catch { return 0; }
}
async function showNet(): Promise<void> {
  const n = await queued();
  netButton.textContent = online ? 'Go offline' : 'Go online';
  chip.textContent = online ? (n ? `online · ${n} write(s) in flight` : 'online') : `offline · ${n} queued write(s)`;
  chip.classList.toggle('off', !online);
  document.body.classList.toggle('offline', !online);
}
netButton.addEventListener('click', () => {
  void (async () => {
    netButton.disabled = true;
    try {
      if (online) { await zb.close(); online = false; note('net', 'went offline: writes wait in the outbox'); }
      else {
        await zb.connect(); online = true;
        note('net', `back online: ${await queued()} write(s) left in the outbox`);
        await readRow();
      }
    } catch (err) {
      status.textContent = `${online ? 'going offline' : 'reconnecting'} failed: ${(err as Error).message}`;
    }
    netButton.disabled = false;
    await showNet();
  })();
});
/// The chip follows the outbox: a write while offline, the flush once back.
setInterval(() => { void showNet(); }, 1000);

// ── go ─────────────────────────────────────────────────────────────────────────
/// A returning visit draws its replica at once; the connect then only catches up.
try { await readRow(); } catch { /* first visit: no table yet */ }
zb.onChange('site_survey', () => { readRow().catch(() => {}); });
try {
  status.textContent = 'connecting…';
  await zb.connect();
} catch (e) {
  status.textContent = e instanceof NotEnrolled
    ? 'This browser is not enrolled yet: open the invite link you were given (…/?invite=<code>).'
    : `Could not connect: ${(e as Error).message}`;
  throw e;
}
who.textContent = `editing as ${zb.principal}${qs.get('as') ? ` (?as=${qs.get('as')})` : ''}`;
status.textContent = `connected in ${((performance.now() - T0) / 1000).toFixed(1)} s`;
online = true;
netButton.disabled = false;
await readRow();
await showNet();
