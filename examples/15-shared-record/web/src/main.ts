/// Example 15: one row of `site_survey`, edited two ways (README):
///   * part A, five plain columns — form A writes one column; the library rebases a write
///     the row outran when the winner changed other columns, and reports LOST otherwise;
///   * part B, one jsonb `doc` of five registers {v, t, w} (COOPERATIVE_EDITING.md) — form B
///     writes one register merged into the doc this replica holds. PostgreSQL merges each
///     accepted doc write register by register (`register_cols`), so a late write cannot
///     roll anyone back; a doc write refused as stale comes back here, and the page merges
///     its registers into the newer row once more.
///
/// Only the survival kit: connect, query, mutate, onChange, stamp, mergeRegisters,
/// close/connect/pending, onVerdict. `onLog` goes to the console, for people.
///
/// The first load enrolls with `?invite=<code>`; the identity is kept in this browser.
/// `?as=<name>` keeps a separate identity and replica: several editors in one browser.
import { ZeBridge, NotEnrolled, mergeRegisters, type Register, type Verdict } from 'zb-client-ts';

const qs = new URLSearchParams(location.search);
const el = (id: string) => document.getElementById(id)!;
const rowBox = el('row'), status = el('status'), sent = el('sent'), who = el('who');
const netButton = el('net') as HTMLButtonElement;
const chip = el('chip');

/// Built for a deployment (`VITE_ZB_BRIDGE_URL=https://bridge.example.com pnpm build`), the
/// page enrolls there and the answer names the NATS websocket. Without it, the dev
/// server's proxy carries both on this page's own origin.
const DEPLOYED_BRIDGE = import.meta.env.VITE_ZB_BRIDGE_URL as string | undefined;
const NATS_URL = import.meta.env.VITE_ZB_NATS_URL as string | undefined;

const TABLE = 'site_survey';
const COLUMNS = ['access', 'hazard', 'contact', 'rating', 'notes'] as const;

const zb = new ZeBridge({
  natsUrl: NATS_URL ?? (DEPLOYED_BRIDGE ? undefined : `${location.origin.replace(/^http/, 'ws')}/nats`),
  bridgeUrl: DEPLOYED_BRIDGE ?? `${location.origin}/bridge`,
  invite: qs.get('invite') ?? undefined,
  dbPath: qs.get('as') ? `survey-${qs.get('as')}.sqlite3` : 'survey.sqlite3',
  tables: [TABLE],
});
(window as any).zb = zb;

/// The library's own log, whole, in the console.
const T0 = performance.now();
zb.onLog((topic, data, level) => console.info(`[zb ${((performance.now() - T0) / 1000).toFixed(1)} s] ${level} ${topic}`, data));

// ── activity: what became of this editor's writes ──────────────────────────────
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

// ── the row ─────────────────────────────────────────────────────────────────────
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
}

function show(): void {
  if (!row) { rowBox.textContent = 'no row in site_survey yet (seed it: README, the INSERT)'; return; }
  const a = COLUMNS.map((c) => `<b>${c}</b> ${shown(row![c])}`).join(' · ');
  const b = COLUMNS.map((c) => {
    const r = row!.doc[c];
    return `<span class="reg"><b>${c}</b> ${r ? `${shown(r.v)} <small>(${escapeHtml(r.w)}, ${escapeHtml(String(r.t).slice(11, 23))})</small>` : '—'}</span>`;
  }).join(' · ');
  rowBox.innerHTML =
    `<div>A · ${a}</div><div class="part">B · doc: ${b}</div>` +
    `<div class="meta">row ${escapeHtml(row.id.slice(0, 8))} · version ${escapeHtml(row.updated_at)} · last writer ${shown(row.last_writer)}</div>`;
}

/// rating is an integer column; the register keeps the same type.
function valueOf(name: string, raw: string): unknown {
  return name === 'rating' ? Number.parseInt(raw, 10) : raw;
}

// ── form A: one column ──────────────────────────────────────────────────────────
el('edit-a').addEventListener('submit', (e) => {
  e.preventDefault();
  void (async () => {
    const name = (el('col-a') as HTMLSelectElement).value;
    const input = el('value-a') as HTMLInputElement;
    const value = valueOf(name, input.value.trim());
    if (!row) { sent.textContent = 'no row to write to'; return; }
    if (name === 'rating' && !Number.isInteger(value)) { sent.textContent = 'rating is a whole number (1–5)'; return; }
    try {
      const { version } = await zb.mutate(TABLE, 'UPDATE', { id: row.id }, { [name]: value });
      sent.textContent = `column ${name} ← ${String(value)} · version ${version}`;
      note('sent', `column ${name} ← ${String(value)}`);
      input.value = '';
    } catch (err) {
      sent.textContent = `not sent: ${(err as Error).message}`;
    }
    await showNet();
  })();
});

// ── form B: one register in doc ─────────────────────────────────────────────────
/// MINE: the registers this editor wrote whose write has not settled yet. Kept in this
/// browser's storage, so a reload while offline does not forget them.
const MINE_KEY = `zb-survey-mine-${qs.get('as') ?? ''}`;
const mine = new Map<string, Register>((() => {
  try { return Object.entries(JSON.parse(localStorage.getItem(MINE_KEY) ?? '{}')) as [string, Register][]; } catch { return []; }
})());
function saveMine(): void {
  try { localStorage.setItem(MINE_KEY, JSON.stringify(Object.fromEntries(mine))); } catch { /* private window */ }
}
/// The order mergeRegisters keeps: the later stamp, then the writer.
const newer = (a: Register, b: Register) => String(a.t) > String(b.t) || (a.t === b.t && String(a.w) > String(b.w));
/// Which registers each doc write carried, by the version mutate() returned.
const carried = new Map<string, Map<string, Register>>();

async function writeDoc(): Promise<string> {
  const doc = mergeRegisters(row!.doc, Object.fromEntries(mine));
  const { version } = await zb.mutate(TABLE, 'UPDATE', { id: row!.id }, { doc });
  carried.set(version, new Map([...mine].map(([k, r]) => [k, { ...r }])));
  return version;
}

el('edit-b').addEventListener('submit', (e) => {
  e.preventDefault();
  void (async () => {
    const name = (el('reg-b') as HTMLSelectElement).value;
    const input = el('value-b') as HTMLInputElement;
    const value = valueOf(name, input.value.trim());
    if (!row) { sent.textContent = 'no row to write to'; return; }
    if (name === 'rating' && !Number.isInteger(value)) { sent.textContent = 'rating is a whole number (1–5)'; return; }
    try {
      // One register, stamped by this client (the bridge's clock, never behind what it saw).
      mine.set(name, { v: value, t: zb.stamp(), w: zb.principal });
      saveMine();
      const version = await writeDoc();
      sent.textContent = `doc.${name} ← ${String(value)} · version ${version}`;
      note('sent', `doc.${name} ← ${String(value)}`);
      input.value = '';
    } catch (err) {
      sent.textContent = `not sent: ${(err as Error).message}`;
    }
    await showNet();
  })();
});

/// A doc write refused as stale: the library reports it LOST once the newer row is here.
/// Each of mine either lost to a newer stamp (said so, dropped) or goes again, merged into
/// the newer doc. Bounded: a register still outrun after five rounds stays here, unsent.
let rounds = 0;
async function remerge(): Promise<void> {
  await readRow();
  if (!row) return;
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
  await writeDoc();
}

// ── what became of each write ───────────────────────────────────────────────────
zb.onVerdict((v: Verdict) => {
  if (v.table !== TABLE) return;
  const isDoc = v.columns.includes('doc');
  const regs = isDoc ? carried.get(v.version) : undefined;
  const what = isDoc ? `doc.${regs ? [...regs.keys()].join(', doc.') : '…'}` : `column ${v.columns.join(', ')}`;
  switch (v.outcome) {
    case 'applied':
    case 'rebased':
      note(v.outcome, `${what}: ${v.outcome === 'applied' ? 'applied' : 'rebased onto the newer row, sent again'}`);
      if (isDoc && regs) {
        if (v.outcome === 'rebased' && v.rebasedAs) carried.set(v.rebasedAs, regs);
        else for (const [k, r] of regs) if (mine.get(k)?.t === r.t) mine.delete(k);   // in the row now
        saveMine();
      }
      if (v.outcome === 'applied') rounds = 0;
      break;
    case 'lost':
      if (isDoc && mine.size) {
        note('stale', `${what}: the row moved first — merging into it`);
        void remerge();
      } else {
        note('lost', `${what}: a newer value of ${(v.lostColumns ?? v.columns).join(', ')} stands`);
      }
      break;
    default:   // deleted, rejected: the library put the local copy back
      note(v.outcome, `${what}: ${v.outcome}${v.reason ? ` (${v.reason})` : ''}`);
      if (regs) { for (const k of regs.keys()) mine.delete(k); saveMine(); }
  }
  carried.delete(v.version);
  void showNet();
});

// ── offline, for real ──────────────────────────────────────────────────────────
/// DevTools' "Offline" does not cut a WebSocket already open, so the page hangs up
/// itself: `close()` drops the NATS connection; a write then applies here and waits in
/// the outbox (nothing is sent); `connect()` sends what waited, and the verdicts come back.
let online = false;
async function showNet(): Promise<void> {
  const n = await zb.pending();
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
        note('net', `back online: ${await zb.pending()} write(s) left in the outbox`);
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
zb.onChange(TABLE, () => { readRow().catch(() => {}); });
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
