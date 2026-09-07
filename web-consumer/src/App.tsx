/// App.tsx — a teaser and a teacher. The subscription lives in `zb-client-ts`
/// (NOTES.md §10): this file is that class's FIRST consumer, which makes the browser
/// demo the regression test for the library. Everything here is UI.
///
/// The page is built as claims. Each block states, in one sentence, the one fact it
/// exists to prove; under its controls a "last events" line shows the library keeping
/// that claim — or breaking it — live, from the log the library already emits.
/// Nothing on this page teaches silently.

import { createSignal, onCleanup, For, Show } from 'solid-js';
import { ZeBridge, credsFileText, principalFromCreds } from 'zb-client-ts';
import { makePgliteStorage } from 'zb-client-ts/pglite';
import { init as zstdInit, decompress as zstdDecompress, createDCtx, decompressUsingDict } from '@bokuweb/zstd-wasm';
import { nkeys } from '@nats-io/nats-core';

/// ⚠️ This module owns ONE client, one socket, one replica. An edit to this file must
/// therefore reload the page, not hot-swap the module: a hot update re-runs the module
/// scope, which starts a SECOND client on a fresh database next to the first and
/// re-initialises the wasm decoder under it (measured 2026-09-07: two seeds at once
/// and "Failed to compress with code -20/-72" on objects whose digest had just been
/// verified). Accepting the update and reloading is how a module opts out.
if (import.meta.hot) import.meta.hot.accept(() => location.reload());

/// §10x: chain objects are zstd frames, deltas may name a dictionary. One wasm
/// decoder, initialized once per PAGE (kept on globalThis so nothing re-inits it);
/// a dict frame decodes through a decompression context holding the dictionary.
const g = globalThis as any;
async function zstdDecode(b: Uint8Array, dict?: Uint8Array): Promise<Uint8Array> {
  g.__zbZstdReady ??= zstdInit();
  await g.__zbZstdReady;
  if (!dict) return zstdDecompress(b);
  const dctx = createDCtx();
  try { return decompressUsingDict(dctx, b, dict); } finally { /* ctx freed by GC in this build */ }
}

/// ⚠️ No ports here, on purpose. Both of these are SAME-ORIGIN paths served by the
/// Vite dev server, which proxies them to wherever the stack actually is —
/// `ZB_BRIDGE_ORIGIN` and `ZB_NATS_WS_ORIGIN` in vite.config.ts, one file, two
/// values. Same-origin also settles COEP: the page sets `require-corp` for OPFS.
const wsScheme = location.protocol === 'https:' ? 'wss' : 'ws';
const NATS_URL = import.meta.env.VITE_NATS_URL ?? `${wsScheme}://${location.host}/nats`;
const BRIDGE_URL = import.meta.env.VITE_BRIDGE_URL ?? '/bridge';

/// The dev accounts. Auth on this page is "pick who you are": each name has a creds
/// file under /creds (a symlink to scripts/native/creds, minted by
/// scripts/native/jwt-bootstrap.sh), and the JWT carries the permissions. The tenant
/// is NOT in the file — it is the bridge's mapping (zebridge_user_tenants), which is
/// why `guest` exists: a valid principal with no mapping follows public tables only.
/// `alice` runs on PGlite (PostgreSQL in the browser) so the code shows how the
/// storage adapter is chosen; the others on OPFS SQLite.
///
/// ⚠️ Dev only. Creds served to a browser is a demo convenience, never a deployment.
const ACCOUNTS = [
  { principal: 'guest', tenant: 'no tenant', engine: 'sqlite' },
  { principal: 'alice', tenant: 'acme', engine: 'pglite' },
  { principal: 'bob', tenant: 'globex', engine: 'sqlite' },
  { principal: 'mary', tenant: 'globex', engine: 'sqlite' },
] as const;

/// `?principal=bob` beats the build-time env: one dev server serves several
/// principals side by side (one tab per account, one database per tab).
const _qs = new URLSearchParams(window.location.search);
const PRINCIPAL = _qs.get('principal') ?? (import.meta.env.VITE_PRINCIPAL as string | undefined) ?? 'alice';
const PASSWORD = _qs.get('password') ?? (import.meta.env.VITE_PASSWORD as string | undefined) ?? 's3cret';
const ACCOUNT = ACCOUNTS.find((a) => a.principal === PRINCIPAL);

/// Opt in to a stable per-principal OPFS file instead of a fresh one every load.
/// Off by default — the timestamped name is the project's clean-room dev convention.
const DURABLE = ['1', 'true'].includes(_qs.get('durable') ?? (import.meta.env.VITE_DURABLE as string | undefined) ?? '');

/// `?engine=pglite`: PostgreSQL-in-the-browser as the replica engine instead of
/// OPFS SQLite. Same core, same protocol; the adapter brings the dialect
/// (zb-client-ts/src/dialect.ts). Defaults from the account picked.
const ENGINE = (_qs.get('engine') ?? (import.meta.env.VITE_ENGINE as string | undefined) ?? ACCOUNT?.engine ?? 'sqlite') as 'sqlite' | 'pglite';

/// Enrollment (?invite=<code>): the pump-starter, live. The app generates its OWN
/// nkey pair, sends {code, user_pubkey} to the bridge's mint endpoint, and gets back
/// a JWT — the seed never crosses the wire. The principal comes back INSIDE the JWT.
/// Kept as a URL flow: the page's front door is the account picker.
const INVITE = _qs.get('invite');

/// Which credential the BROKER will accept — a property of the stack you are pointed
/// at, not a preference: 'creds' (operator/JWT, default) or 'password'.
const AUTH = _qs.get('auth') ?? (import.meta.env.VITE_AUTH as string | undefined) ?? 'creds';

async function enroll(code: string): Promise<string | undefined> {
  if (!code.trim()) return 'enter an invite code';
  const kp = nkeys.createUser();
  const seed = new TextDecoder().decode(kp.getSeed());
  const res = await fetch(`${BRIDGE_URL}/enroll?code=${code.trim()}&user_pubkey=${kp.getPublicKey()}`).catch(() => null);
  if (!res) return `bridge unreachable at ${BRIDGE_URL}`;
  if (!res.ok) return `refused (${res.status}) — invalid, used, or expired code`;
  const { jwt, grammar_hash } = await res.json();
  sessionStorage.setItem('zb_creds', credsFileText(jwt, seed));
  if (grammar_hash) sessionStorage.setItem('zb_grammar_hash', grammar_hash);
  location.href = location.pathname;
  return undefined;
}

const CREDS = await (async () => {
  if (AUTH === 'password') return undefined;
  const stashed = sessionStorage.getItem('zb_creds');
  if (stashed) return stashed;
  if (INVITE) {
    const err = await enroll(INVITE);
    if (err) console.error('enrollment failed:', err);
    return undefined;
  }
  return fetch(`/creds/${PRINCIPAL}.creds`)
    .then((r) => (r.ok ? r.text() : undefined))
    .catch(() => undefined);
})();

/// The creds are authoritative for identity — the header shows who the JWT says we
/// are, not what the URL guessed.
const EFFECTIVE_PRINCIPAL = (CREDS && principalFromCreds(CREDS)) || PRINCIPAL;

/// The wire grammar is compiled into the library (§10dq) — the same bytes the bridge
/// embeds. What the page asks the bridge for is only its HASH, to refuse loudly if
/// this build and that bridge speak different protocols. A bridge that does not
/// answer is not a mismatch: the page connects anyway, since NATS holds everything
/// a provisioned replica needs. An enrolled tab already holds the hash from /enroll.
const GRAMMAR_HASH: string | undefined = sessionStorage.getItem('zb_grammar_hash')
  ?? (await fetch(`${BRIDGE_URL}/grammar`, { signal: AbortSignal.timeout(3000) })
    .then((r) => (r.ok ? r.headers.get('x-grammar-hash') : null))
    .catch(() => null))
  ?? undefined;

/// THE instance. One replica, one socket, one outbox.
const zb = new ZeBridge({
  zstdDecompress: zstdDecode,
  natsUrl: NATS_URL,
  principal: PRINCIPAL,
  password: PASSWORD,
  creds: CREDS,
  grammarHash: GRAMMAR_HASH,
  durable: DURABLE,
  storage: ENGINE === 'pglite' ? makePgliteStorage({ persist: DURABLE }) : undefined,
});

// Console handle for inspecting the local replica directly — the database lives in
// OPFS under a per-session name, so there is no file to open with the sqlite3 CLI.
declare global {
  interface Window { zb: any }
}
if (typeof window !== 'undefined') {
  window.zb = {
    db: zb.dbName,
    uuid: () => zb.uuid(),
    q: (text: string, ...params: any[]) => zb.query(text, ...params),
    count: async (table: string) => (await zb.query(`SELECT COUNT(*) AS n FROM ${table}`))[0],
    state: () => zb.syncState(),
    outbox: () => zb.outboxAll(),
    watermark: () => zb.gcWatermark(),
    flushOutbox: () => zb.flushOutbox(),
    mutate: (table: string, op: 'INSERT' | 'UPDATE' | 'DELETE', key: any, values?: any, opts?: { version?: string }) =>
      zb.mutate(table, op, key, values, opts),
    newVersion: () => zb.newVersion(),
    connect: () => zb.connect(),
    close: () => zb.close(),
    reset: async () => { await zb.deleteDatabaseFile(); location.reload(); },
    orphans: async () => {
      const root = await navigator.storage.getDirectory();
      const names: string[] = [];
      for await (const name of (root as any).keys()) {
        if (typeof name === 'string' && name.startsWith('zebridge_')) names.push(name);
      }
      return names.sort();
    },
    purge: async () => {
      const root = await navigator.storage.getDirectory();
      const removed: string[] = [];
      for (const name of await window.zb.orphans()) {
        if (name === zb.dbName) continue;
        try { await root.removeEntry(name); removed.push(name); } catch { /* held by another tab */ }
      }
      return { removed: removed.length, kept: zb.dbName };
    },
  };
}

/// One well-known counter row per table, addressed by key: a fixed uid for the public
/// counter, one uid per tenant for the tenant counter (uid is the PK, so tenants
/// cannot share one).
const counterUid = (table: 'counter_public' | 'counter_tenant'): string => {
  if (table === 'counter_public') return '00000000-0000-4000-8000-00000000c0de';
  let h = 0x811c9dc5;
  for (const ch of zb.tenant || '_default') h = Math.imul(h ^ ch.charCodeAt(0), 0x01000193) >>> 0;
  return `00000000-0000-4000-8000-${h.toString(16).padStart(8, '0')}c0df`;
};

type Ev = { at: string; level: string; text: string };
type CounterRow = { value: number; version: string; writer: string } | null;
type UserRow = { uid: string; name: string };
type OrderRow = { uid: string; user_id: string; item: string; note: string | null; version: string; writer: string };

export default function App() {
  const [status, setStatus] = createSignal<'connected' | 'disconnected' | 'connecting'>('disconnected');
  const [health, setHealth] = createSignal<'up' | 'down' | 'unknown'>('unknown');
  const [phase, setPhase] = createSignal<Record<string, boolean>>({ connected: false, migrated: false, snapshot: false, cdc: false });
  const [suspended, setSuspended] = createSignal<Record<string, string>>({});
  const [tenant, setTenant] = createSignal<string>('—');
  const [outboxCount, setOutboxCount] = createSignal(0);
  const [heldCount, setHeldCount] = createSignal(0);
  const [tables, setTables] = createSignal<string[]>([]);
  const [counters, setCounters] = createSignal<Record<string, CounterRow>>({});
  const [users, setUsers] = createSignal<UserRow[]>([]);
  const [orders, setOrders] = createSignal<OrderRow[]>([]);
  const [events, setEvents] = createSignal<Record<string, Ev[]>>({});
  const has = (t: string) => tables().includes(t);

  // ── the last events, per block: the library's log, filtered by table ──────
  //
  // Verdicts name the write (`table#id`), echoes and holds name the table, the
  // rebase and the loss name the table. Anything mentioning a block's table lands
  // under that block; the newest three stay. High-volume CDC noise never shows.
  const BLOCKS: Record<string, string[]> = {
    counter_public: ['counter_public'],
    counter_tenant: ['counter_tenant'],
    shop: ['app_users', 'app_orders'],
  };
  const eventText = (topic: string, data: any, level: string): string => {
    if (level === 'VERDICT' && data && typeof data === 'object') {
      return `${data.write ?? topic}: ${data.status}${data.reason ? ` (${data.reason})` : ''}${data.detail ? ` — ${data.detail}` : ''}`;
    }
    if (level === 'MUTATION OUT' && data && typeof data === 'object') {
      return `sent, stored by JetStream (seq ${data._ack?.seq}${data._ack?.duplicate ? ', duplicate' : ''})`;
    }
    return typeof data === 'string' ? data : JSON.stringify(data);
  };
  const onLog = (topic: string, data: any, level: string) => {
    // A migration that landed is a change of the replica too: re-read the lists, so
    // a renamed column shows up the moment it lands, not on the next row event.
    if (level === 'SCHEMA') { void refresh(); return; }
    if (['INSERT', 'UPDATE', 'DELETE', 'snapshot', 'CDC'].includes(level)) return;
    const text = eventText(topic, data, level);
    const hay = `${topic} ${text}`;
    for (const [block, names] of Object.entries(BLOCKS)) {
      if (!names.some((n) => hay.includes(n))) continue;
      const ev: Ev = { at: new Date().toLocaleTimeString(), level, text };
      setEvents((prev) => ({ ...prev, [block]: [ev, ...(prev[block] ?? [])].slice(0, 3) }));
    }
    if (['ERROR', 'WARNING', 'WARN', 'VERDICT', 'INFO', 'OUTBOX', 'CONFIRMED', 'HOLD', 'SYS'].includes(level)) {
      console.log(`[${new Date().toLocaleTimeString()}] ${topic} ${level}:`, text);
    }
  };

  // ── reads: the replica IS the API ─────────────────────────────────────────
  const refresh = async () => {
    setTables(zb.tableNames().sort());
    setTenant(zb.tenant || '—');
    setHeldCount(zb.heldCount);
    try { setOutboxCount((await zb.outboxAll()).length); } catch { /* outbox not ready */ }

    const next: Record<string, CounterRow> = {};
    for (const t of ['counter_public', 'counter_tenant'] as const) {
      if (!has(t)) continue;
      try {
        const r = await zb.query(`SELECT value, updated_at, last_writer FROM ${t} WHERE uid = ?`, counterUid(t));
        next[t] = r[0] ? { value: r[0].value, version: String(r[0].updated_at ?? ''), writer: String(r[0].last_writer ?? '') } : null;
      } catch { /* not ready */ }
    }
    setCounters(next);

    if (has('app_users')) {
      try { setUsers(await zb.query(`SELECT uid, name FROM app_users WHERE deleted_at IS NULL ORDER BY name`)); } catch { /* not ready */ }
    }
    if (has('app_orders')) {
      try {
        const r = await zb.query(`SELECT uid, user_id, item, note, updated_at, last_writer FROM app_orders WHERE deleted_at IS NULL ORDER BY updated_at DESC`);
        setOrders(r.map((o: any) => ({ uid: o.uid, user_id: o.user_id, item: o.item, note: o.note, version: String(o.updated_at ?? ''), writer: String(o.last_writer ?? '') })));
      } catch { /* not ready */ }
    }
    if (sqlLive() && sqlHasRun) void runSql();
  };

  zb.onStatus((s) => setStatus(s));
  zb.onPhase((p) => setPhase((prev) => ({ ...prev, [p]: true })));
  zb.onSuspended((table, reason) => setSuspended((prev) => {
    const next = { ...prev };
    if (reason === null) delete next[table]; else next[table] = reason;
    return next;
  }));
  zb.onLog(onLog);
  zb.onAnyChange(() => void refresh());

  // ── writes: every one through mutate(), then a refresh for the outbox count ──
  const write = async (table: string, op: 'INSERT' | 'UPDATE' | 'DELETE', key: Record<string, unknown>, values?: Record<string, unknown>) => {
    await zb.mutate(table, op, key, values);
    void refresh();
  };

  /// INSERT on first click (no row yet), UPDATE after — only the column that changes.
  const bump = async (table: 'counter_public' | 'counter_tenant', delta: number) => {
    const uid = counterUid(table);
    const row = counters()[table];
    if (row) return write(table, 'UPDATE', { uid }, { value: row.value + delta });
    const version = zb.newVersion();
    const data: Record<string, unknown> = { uid, value: delta, inserted_at: version, updated_at: version };
    if (table === 'counter_tenant') data.tenant_id = zb.tenant;
    return write(table, 'INSERT', { uid }, data);
  };

  // the shop form: a user (datalist), the order's text (datalist of existing orders),
  // a note. Picking an existing order — from the datalist or the list below — binds
  // the form to that row (`pickedUid`); the text and the note are then editable, and
  // UPDATE sends only what changed. Typing a text nobody has is a new order.
  const [userName, setUserName] = createSignal('');
  const [item, setItem] = createSignal('');
  const [note, setNote] = createSignal('');
  const [pickedUid, setPickedUid] = createSignal('');
  const [formMsg, setFormMsg] = createSignal('');
  /// Every request ends with an empty form: a datalist filters by what the field
  /// holds, so a name left behind hides every other suggestion.
  const clearForm = () => { setUserName(''); setItem(''); setNote(''); setPickedUid(''); };
  const pickedUser = () => users().find((u) => u.name === userName().trim());
  const pickedOrder = () => orders().find((o) => o.uid === pickedUid());
  const pickOrder = (o: OrderRow) => { setPickedUid(o.uid); setItem(o.item); setNote(o.note ?? ''); setUserName(userOf(o)); };
  const userOf = (o: OrderRow) => users().find((u) => u.uid === o.user_id)?.name ?? o.user_id.slice(0, 8);

  /// CREATE: the user if the name is new, then the order for them.
  const createOrder = async (): Promise<void> => {
    setFormMsg('');
    if (!userName().trim() || !item().trim()) { setFormMsg('a user and an order are needed'); return; }
    let user = pickedUser();
    if (!user) {
      const uid = zb.uuid(); const version = zb.newVersion();
      await write('app_users', 'INSERT', { uid }, { uid, name: userName().trim(), tenant_id: zb.tenant, inserted_at: version, updated_at: version });
      user = { uid, name: userName().trim() };
    }
    const uid = zb.uuid(); const version = zb.newVersion();
    await write('app_orders', 'INSERT', { uid }, {
      uid, user_id: user.uid, item: item().trim(), note: note().trim() || null, tenant_id: zb.tenant, inserted_at: version, updated_at: version,
    });
    clearForm();
  };

  /// UPDATE: only the columns that changed travel — the rebase needs the sparse form.
  /// Two text columns, item and note, so two tabs can edit different columns of one row.
  const updateOrder = async (): Promise<void> => {
    setFormMsg('');
    const o = pickedOrder();
    if (!o) { setFormMsg('pick an existing order to update (from the list, or by its exact text)'); return; }
    const values: Record<string, unknown> = {};
    if (item().trim() && item().trim() !== o.item) values.item = item().trim();
    if (note().trim() !== (o.note ?? '')) values.note = note().trim() || null;
    if (!Object.keys(values).length) { setFormMsg('nothing changed'); return; }
    await write('app_orders', 'UPDATE', { uid: o.uid }, values);
    clearForm();
  };

  /// DELETE: the picked order; with no order picked, the picked user — refused by
  /// PostgreSQL while a live order still references them.
  const deleteOrder = async (): Promise<void> => {
    setFormMsg('');
    const o = pickedOrder();
    if (o) { await write('app_orders', 'DELETE', { uid: o.uid }); clearForm(); return; }
    const u = pickedUser();
    if (u) { await write('app_users', 'DELETE', { uid: u.uid }); clearForm(); return; }
    setFormMsg('pick an existing order, or a user, to delete');
  };

  // ── SQL console — arbitrary reads against THIS tab's own replica ───────────
  const [sqlText, setSqlText] = createSignal('SELECT name, updated_at, last_writer FROM app_users ORDER BY updated_at DESC LIMIT 8');
  const [sqlRows, setSqlRows] = createSignal<any[] | null>(null);
  const [sqlError, setSqlError] = createSignal<string | null>(null);
  const [sqlLive, setSqlLive] = createSignal(true);
  const [sqlMs, setSqlMs] = createSignal<number | null>(null);
  let sqlHasRun = false;
  const runSql = async () => {
    const q = sqlText().trim();
    if (!q) return;
    sqlHasRun = true;
    const t0 = performance.now();
    try {
      const rows = await zb.query(q);
      setSqlMs(Math.round(performance.now() - t0)); setSqlError(null);
      setSqlRows(Array.isArray(rows) ? rows.slice(0, 200) : []);
    } catch (e: any) {
      setSqlError(String(e?.message ?? e)); setSqlRows(null); setSqlMs(null);
    }
  };

  // ── the socket, as a button ───────────────────────────────────────────────
  //
  // close() hangs up; writes made meanwhile apply locally and wait in the outbox;
  // connect() catches up on CDC and flushes them. The outbox count in the header is
  // the number to watch.
  const toggleSocket = async () => {
    if (status() === 'connected') { await zb.close(); setStatus('disconnected'); void refresh(); }
    else if (status() === 'disconnected') void zb.connect().catch(() => { /* logged by the class */ });
  };
  const switchAccount = (principal: string) => {
    const a = ACCOUNTS.find((x) => x.principal === principal);
    // The picker means operator/JWT auth: each account IS a creds file. This beats
    // a dev server started with VITE_AUTH=password.
    location.search = `?principal=${principal}&engine=${a?.engine ?? 'sqlite'}&auth=creds`;
  };

  // ── boot ──────────────────────────────────────────────────────────────────
  void zb.connect().catch(() => { /* logged by the class; badge shows disconnected */ });
  void refresh();
  let healthWarned = false;
  const pollHealth = async () => {
    try {
      const r = await fetch('/bridge/health', { signal: AbortSignal.timeout(3000) });
      setHealth(r.ok ? 'up' : 'down'); healthWarned = false;
    } catch (err: any) {
      setHealth('down');
      if (!healthWarned) { healthWarned = true; onLog('SYS', `bridge /health unreachable: ${err?.message ?? err}`, 'WARNING'); }
    }
  };
  void pollHealth();
  const healthId = setInterval(pollHealth, 10_000);
  onCleanup(() => { clearInterval(healthId); void zb.close(); });

  const Events = (props: { block: string }) => (
    <ul class="events">
      <For each={events()[props.block] ?? []}>
        {(e) => <li class={`event level-${e.level.toLowerCase().replace(' ', '-')}`}><span class="at">{e.at}</span> {e.text}</li>}
      </For>
    </ul>
  );
  const shortVersion = (v: string) => v.replace(/^\d{4}-\d{2}-\d{2}T/, '').replace(/Z$/, '');

  return (
    <>
      <header>
        <div class="row">
          <h1>ZeBridge web consumer</h1>
          <div class="status-bar">
            <label class="account">
              <span>account</span>
              <select value={PRINCIPAL} onChange={(e) => switchAccount(e.currentTarget.value)}>
                <For each={ACCOUNTS}>{(a) => <option value={a.principal} selected={a.principal === PRINCIPAL}>{a.principal} | {a.tenant} · {a.engine}</option>}</For>
              </select>
            </label>
            <button class={`badge ${status()}`} onClick={() => void toggleSocket()} title="connect / disconnect the socket">
              NATS {status()}{status() === 'connected' ? ' — hang up' : status() === 'disconnected' ? ' — connect' : ''}
            </button>
            <span class={`badge ${health() === 'up' ? 'connected' : 'disconnected'}`}>bridge {health()}</span>
          </div>
        </div>
        {/* The startup state machine. Each cell greens once and stays green — the
            useful signal is how FAR it got. */}
        <ul class="phases">
          <li class="head">phases</li>
          <For each={[['connected', 'NATS connected'], ['migrated', 'schema migrated'], ['snapshot', 'snapshot replayed'], ['cdc', 'CDC active']]}>
            {([key, label]) => <li classList={{ done: phase()[key] }}>{label}</li>}
          </For>
        </ul>
        <p class="identity">
          principal <strong>{EFFECTIVE_PRINCIPAL}</strong> · tenant <strong>{tenant()}</strong> · client <strong>{zb.clientId}</strong>
          {' '}· engine <strong>{ENGINE}</strong> · outbox <strong>{outboxCount()}</strong> pending · held <strong>{heldCount()}</strong>
        </p>
      </header>

      <For each={Object.entries(suspended())}>
        {([table, reason]) => (
          <div class="suspended-banner">
            ⏸ <strong>{table}</strong> is suspended upstream ({reason}). Local rows are frozen and still valid,
            but no new events or snapshots will arrive, and writes are refused client-side, until the shape is fixed.
          </div>
        )}
      </For>

      <main>
        {/* ── counters ── */}
        <section>
          <h2>Two counters</h2>
          <div class="cards">
            <div class="card">
              <h3>counter_public</h3>
              <p class="claim">
                One row, one column, shared by every tenant. Two tabs pressing + at once: the later stamp wins
                and the other click is lost — this is last-writer-wins on a contested column, not a CRDT counter.
              </p>
              <Show when={has('counter_public')} fallback={<p class="muted">not replicated here</p>}>
                <div class="counter">
                  <button onClick={() => void bump('counter_public', -1)}>−</button>
                  <strong class="value">{counters().counter_public?.value ?? 0}</strong>
                  <button onClick={() => void bump('counter_public', +1)}>+</button>
                </div>
                <p class="meta">version {shortVersion(counters().counter_public?.version ?? '—')} · last writer {counters().counter_public?.writer || '—'}</p>
              </Show>
              <Events block="counter_public" />
            </div>
            <div class="card">
              <h3>counter_tenant</h3>
              <p class="claim">
                The same widget, but this row travels on your tenant's stream and is filtered by row-level security.
                Open a tab as another tenant: it never moves there. As <code>guest</code> it does not exist at all.
              </p>
              <Show when={has('counter_tenant')} fallback={<p class="muted">not replicated here — no tenant mapping for this principal</p>}>
                <div class="counter">
                  <button onClick={() => void bump('counter_tenant', -1)}>−</button>
                  <strong class="value">{counters().counter_tenant?.value ?? 0}</strong>
                  <button onClick={() => void bump('counter_tenant', +1)}>+</button>
                </div>
                <p class="meta">version {shortVersion(counters().counter_tenant?.version ?? '—')} · last writer {counters().counter_tenant?.writer || '—'}</p>
              </Show>
              <Events block="counter_tenant" />
            </div>
          </div>
        </section>

        {/* ── users ⟶ orders ── */}
        <section>
          <h2>Users ⟶ orders</h2>
          <p class="claim">
            <code>app_orders.user_id</code> references <code>app_users.uid</code>. Delete a user who still has orders and
            PostgreSQL refuses it, the verdict comes back <code>rejected</code>, the local copy is restored. Delete an order
            and it vanishes on every replica: a tombstone, later reaped. Edit the order's text in one tab and its note in another:
            both land, the later one rebased onto the earlier. Edit the same column in both: the later stamp wins.
          </p>
          <Show when={has('app_users') && has('app_orders')} fallback={<p class="muted">not replicated here — no tenant mapping for this principal</p>}>
            <div class="form">
              {/* The datalist ids change with the row count on purpose: Chrome does not
                  re-read a datalist whose options changed under a focused input until the
                  input is re-attached to it — a new `list` attribute does exactly that. */}
              <label>user
                <input list={`users-list-${users().length}`} value={userName()} onInput={(e) => setUserName(e.currentTarget.value)} placeholder="existing, or a new name" />
                <datalist id={`users-list-${users().length}`}><For each={users()}>{(u) => <option value={u.name} />}</For></datalist>
              </label>
              <label>order{pickedOrder() ? ' (editing an existing one)' : ''}
                <input list={`orders-list-${orders().length}`} value={item()} onInput={(e) => {
                  setItem(e.currentTarget.value);
                  // an exact match with an existing order's text picks that order
                  const o = orders().find((x) => x.item === e.currentTarget.value.trim());
                  if (o && o.uid !== pickedUid()) pickOrder(o);
                }} placeholder="what was ordered — pick an existing one, or type a new one" />
                <datalist id={`orders-list-${orders().length}`}><For each={orders()}>{(o) => <option value={o.item}>{userOf(o)} · {o.note ?? ''}</option>}</For></datalist>
              </label>
              <label>note <input value={note()} onInput={(e) => setNote(e.currentTarget.value)} placeholder="free text" /></label>
              <div class="buttons">
                <button class="create" onClick={() => void createOrder()}>CREATE</button>
                <button class="update" onClick={() => void updateOrder()}>UPDATE</button>
                <button class="delete" onClick={() => void deleteOrder()}>DELETE</button>
                <span class="form-msg">{formMsg()}</span>
              </div>
            </div>
            <p class="meta">
              users <strong>{users().length}</strong> · orders <strong>{orders().length}</strong>
              <Show when={pickedOrder()}>{(o) => <> · picked: {o().item} by {userOf(o())}, version {shortVersion(o().version)}, last writer {o().writer || '—'}</>}</Show>
            </p>
            {/* The rows themselves — what the replica holds, live, no popup needed. */}
            <div class="rows">
              <ul class="rowlist">
                <For each={users()}>{(u) => <li onClick={() => { clearForm(); setUserName(u.name); }}>{u.name}</li>}</For>
              </ul>
              <ul class="rowlist">
                <For each={orders()}>{(o) => (
                  <li classList={{ picked: o.uid === pickedUid() }} onClick={() => pickOrder(o)}>
                    <strong>{o.item}</strong> · {userOf(o)}{o.note ? ` · ${o.note}` : ''} <span class="muted">{o.writer || ''}</span>
                  </li>
                )}</For>
              </ul>
            </div>
          </Show>
          <Events block="shop" />
        </section>

        {/* ── SQL console ── */}
        <section>
          <h2>SQL console</h2>
          <p class="claim">
            Your local replica, this tab only. Reads are free: any SQL, joins, aggregates, offline. Writes are not SQL —
            they go through <code>mutate()</code>, and <code>query()</code> refuses anything that is not a read.
          </p>
          <div class="sql">
            <textarea rows={3} value={sqlText()} onInput={(e) => setSqlText(e.currentTarget.value)}
              onKeyDown={(e) => { if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); void runSql(); } }} />
            <div class="buttons">
              <button onClick={() => void runSql()}>Run (⌘⏎)</button>
              <label class="inline"><input type="checkbox" checked={sqlLive()} onInput={(e) => setSqlLive(e.currentTarget.checked)} /> live — re-run when CDC touches the replica</label>
              <Show when={sqlMs() != null}>
                <span class="muted">{sqlRows()?.length ?? 0} row(s) · {sqlMs()}ms{(sqlRows()?.length ?? 0) === 200 ? ' · capped at 200' : ''}</span>
              </Show>
            </div>
            <Show when={sqlError()}><div class="sql-error">{sqlError()}</div></Show>
            <Show when={sqlRows() && sqlRows()!.length > 0}>
              <div class="scroll">
                <table class="grid">
                  <thead><tr><For each={Object.keys(sqlRows()![0])}>{(c) => <th>{c}</th>}</For></tr></thead>
                  <tbody>
                    <For each={sqlRows()!}>{(r) => (
                      <tr><For each={Object.keys(sqlRows()![0])}>{(c) => <td>{r[c] === null ? 'NULL' : String(r[c])}</td>}</For></tr>
                    )}</For>
                  </tbody>
                </table>
              </div>
            </Show>
            <Show when={sqlRows() && sqlRows()!.length === 0 && !sqlError()}><div class="muted">0 rows</div></Show>
          </div>
        </section>
      </main>
    </>
  );
}
