/// App.tsx — the theater. The subscription lives in `libzb.ts` (NOTES.md §10):
/// this file is now the ZeBridge class's FIRST consumer, which makes the browser
/// demo the regression test for the extraction. Everything here is UI: signals,
/// badges, demo buttons, the SQL console — and the one platform concern the class
/// deliberately does not own, the /bridge/health poll through the Vite proxy.

import { createSignal, onCleanup, For } from 'solid-js';
import grammar from '../../grammar.json';
import { ZeBridge, credsFileText, principalFromCreds } from './libzb';
import { nkeys } from '@nats-io/nats-core';

const NATS_URL = 'ws://localhost:8080';

/// `?principal=bob` beats the build-time env: one dev server serves several
/// principals side by side (multi-browser demos, the generations staggered-seed
/// test). The name must exist as a NATS user with a matching `mutation.<p>.>` grant.
const _qs = new URLSearchParams(window.location.search);
const PRINCIPAL = _qs.get('principal') ?? (import.meta.env.VITE_PRINCIPAL as string | undefined) ?? 'alice';
const PASSWORD = _qs.get('password') ?? (import.meta.env.VITE_PASSWORD as string | undefined) ?? 's3cret';

/// Opt in to a stable per-principal OPFS file instead of a fresh one every load.
/// Off by default — the timestamped name is the project's clean-room dev convention.
const DURABLE = ['1', 'true'].includes((import.meta.env.VITE_DURABLE as string | undefined) ?? '');

/// THE instance. One replica, one socket, one outbox — module-level like the
/// SQLocal handle it wraps used to be.
/// Operator/JWT mode: the dev server exposes scripts/native/creds/ under
/// /creds (a public/ symlink), so each principal's tab fetches its own creds
/// file — the JWT carries the permissions (scoped signing key), and no server
/// conf names the principal. Missing file → fall back to user/password, so the
/// same App works against a pre-operator server.
/// Enrollment (?invite=<code>): the pump-starter, live. The app generates its
/// OWN nkey pair, sends {code, user_pubkey} to the bridge's mint endpoint, and
/// gets back a JWT — the seed never crosses the wire in either direction. The
/// code is a one-time bearer secret (a password with hygiene); the principal is
/// never sent — it comes back INSIDE the JWT, and libzb treats it as
/// authoritative.
const INVITE = _qs.get('invite');

/// The button's half of enrollment: generate a pair, send code + PUBLIC key,
/// stash the assembled creds in sessionStorage (demo-tier storage — per tab,
/// gone with it) and reload clean. Returns an error string instead of reloading
/// when the mint refuses.
async function enroll(code: string): Promise<string | undefined> {
  if (!code.trim()) return 'enter an invite code';
  const kp = nkeys.createUser();
  const seed = new TextDecoder().decode(kp.getSeed());
  // GET with params: a CORS "simple request" — no preflight, no body parsing.
  const res = await fetch(
    `http://localhost:9090/enroll?code=${code.trim()}&user_pubkey=${kp.getPublicKey()}`,
  ).catch(() => null);
  if (!res) return 'bridge unreachable on :9090';
  if (!res.ok) return `refused (${res.status}) — invalid, used, or expired code`;
  const { jwt } = await res.json();
  sessionStorage.setItem('zb_creds', credsFileText(jwt, seed));
  location.href = location.pathname; // clean reload; the stash wins below
  return undefined;
}

const CREDS = await (async () => {
  const stashed = sessionStorage.getItem('zb_creds');
  if (stashed) return stashed;
  if (INVITE) {
    const err = await enroll(INVITE); // URL flow reuses the button's path
    if (err) console.error('enrollment failed:', err);
    return undefined; // success never reaches here — enroll() reloads
  }
  return fetch(`/creds/${PRINCIPAL}.creds`)
    .then((r) => (r.ok ? r.text() : undefined))
    .catch(() => undefined);
})();

/// The creds are authoritative for identity — the header must show who the JWT
/// says we are, not what the URL guessed (measured: an enrolled pia labeled
/// "alice" because the display used the URL default).
const EFFECTIVE_PRINCIPAL = (CREDS && principalFromCreds(CREDS)) || PRINCIPAL;

const zb = new ZeBridge({
  natsUrl: NATS_URL,
  principal: PRINCIPAL,
  password: PASSWORD,
  creds: CREDS,
  grammar,
  durable: DURABLE,
});

// Console handle for inspecting the local replica directly — the database lives in
// OPFS under a per-session name, so there is no file to open with the sqlite3 CLI
// and no way to query it except through the worker. `zb.q(...)`, `zb.state()`,
// `zb.orphans()`, `zb.purge()`, `zb.reset()` — deliberate, not left to a breakpoint.
declare global {
  interface Window { zb: any }
}
if (typeof window !== 'undefined') {
  window.zb = {
    db: zb.dbName,
    uuid: () => zb.uuid(),
    q: (text: string, ...params: any[]) => zb.query(text, ...params),   // params PASS THROUGH — a one-arg shim silently binds NULL
    count: async (table: string) => (await zb.query(`SELECT COUNT(*) AS n FROM ${table}`))[0],
    state: () => zb.syncState(),
    ids: async (table: string, col = 'id') =>
      (await zb.query(`SELECT ${col} FROM ${table} ORDER BY ${col}`)).map((r: any) => r[col]),
    outbox: () => zb.outboxAll(),
    flushOutbox: () => zb.flushOutbox(),
    /** The blessed write path, exposed for console-driven tests (oversize probes,
     *  scripted demos). Same rules as the buttons — verdicts and reverts included. */
    mutate: (table: string, op: 'INSERT' | 'UPDATE' | 'DELETE', key: any, values?: any, opts?: { version?: string }) =>
      zb.mutate(table, op, key, values, opts),
    newVersion: () => zb.newVersion(),
    /** Delete this session's database file, then reload into a fresh one.
     *  ("Clear site data" does not clear OPFS. Nothing in devtools does.) */
    reset: async () => {
      await zb.deleteDatabaseFile();
      location.reload();
    },
    /** Every zebridge database left in OPFS — one per past page load. */
    orphans: async () => {
      const root = await navigator.storage.getDirectory();
      const names: string[] = [];
      for await (const name of (root as any).keys()) {
        if (typeof name === 'string' && name.startsWith('zebridge_')) names.push(name);
      }
      return names.sort();
    },
    /** Remove every zebridge database except the one this page is using. */
    purge: async () => {
      const root = await navigator.storage.getDirectory();
      const removed: string[] = [];
      for (const name of await window.zb.orphans()) {
        if (name === zb.dbName) continue;
        try {
          await root.removeEntry(name);
          removed.push(name);
        } catch { /* held by another tab, or already gone */ }
      }
      return { removed: removed.length, kept: zb.dbName };
    },
  };
}

export default function App() {
  const [status, setStatus] = createSignal<'connected' | 'disconnected' | 'connecting'>('disconnected');
  const [pendingCount, setPendingCount] = createSignal(0);
  const [suspended, setSuspended] = createSignal<Record<string, string>>({});
  const [phase, setPhase] = createSignal<Record<string, boolean>>({
    connected: false, migrated: false, snapshot: false, cdc: false,
  });
  const [health, setHealth] = createSignal<'up' | 'down' | 'unknown'>('unknown');
  const [tenant, setTenant] = createSignal<string>('—');
  const [inviteCode, setInviteCode] = createSignal('');
  const [enrollMsg, setEnrollMsg] = createSignal('');
  const [counts, setCounts] = createSignal<Record<string, number>>({});
  const [counterValues, setCounterValues] = createSignal<Record<string, number>>({});
  const [lastVerb, setLastVerb] = createSignal<Record<string, string>>({});
  const [logging, setLogging] = createSignal<Record<string, boolean>>({});
  const [syncedTableNames, setSyncedTableNames] = createSignal<string[]>([]);

  /// The same skip rule the old inline appendLog had: high-volume CDC noise stays out
  /// of the console; everything else prints with a timestamp.
  const appendLog = (topic: string, data: any, opType = '') => {
    if (['INSERT', 'UPDATE', 'DELETE', 'snapshot', 'CDC'].includes(opType)) return;
    const timestamp = new Date().toLocaleTimeString();
    const bodyStr = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
    console.log(`[${timestamp}] ${topic} ${opType}:`, bodyStr);
  };

  /// INS green, UP orange, DEL red — a soft delete arrives as an UPDATE with the
  /// tombstone set (§7.5), so it shows as DEL: the verb a user cares about is the
  /// intent, not the SQL. Held until the next event, deliberately not a flash.
  const markVerb = (table: string, ev: any) => {
    if (!ev?.operation) return;   // evless notifications (reverts, seeds) carry no verb
    const state = zb.tableState(table);
    const tombstoned = state?.tombstoneColumn && ev?.data?.[state.tombstoneColumn] != null;
    const verb = tombstoned ? 'DEL'
      : ev.operation === 'INSERT' ? 'INS'
      : ev.operation === 'DELETE' ? 'DEL'
      : 'UP';
    setLastVerb((prev) => ({ ...prev, [table]: verb }));
    if (logging()[table]) console.log(`[${table}] ${verb}`, ev.data);
  };

  /// Counts only — a row grid here is what used to make the UI thread the bottleneck.
  const recount = async () => {
    setSyncedTableNames(zb.tableNames().sort());
    setPendingCount(zb.heldCount);
    setTenant(zb.tenant || '—');

    const next: Record<string, number> = {};
    for (const table of zb.tableNames()) {
      try {
        const state = zb.tableState(table);
        // Soft-deleted rows are present locally and must not be counted (§7.5).
        const liveOnly = state?.tombstoneColumn ? ` WHERE "${state.tombstoneColumn}" IS NULL` : '';
        const r = await zb.query(`SELECT COUNT(*) as count FROM ${table}${liveOnly}`);
        next[table] = r[0]?.count ?? 0;
      } catch { /* table not ready yet */ }
    }
    setCounts(next);

    const nextCounters: Record<string, number> = {};
    for (const table of ['counter_public', 'counter_tenant']) {
      if (!zb.tableNames().includes(table)) continue;
      try {
        const r = await zb.query(`SELECT value FROM ${table} LIMIT 1`);
        nextCounters[table] = r[0]?.value ?? 0;
      } catch { /* table not ready yet */ }
    }
    setCounterValues(nextCounters);

    // The SQL console's live mode rides the same trigger: CDC applied → recount →
    // the last query re-runs against the fresh replica.
    if (sqlLive() && sqlHasRun) void runSql();
  };

  // ── wire the theater to the subscription ──────────────────────────────────
  zb.onStatus((s) => setStatus(s));
  zb.onPhase((p) => setPhase((prev) => ({ ...prev, [p]: true })));
  zb.onSuspended((table, reason) => setSuspended((prev) => {
    const next = { ...prev };
    if (reason === null) delete next[table];
    else next[table] = reason;
    return next;
  }));
  zb.onLog(appendLog);
  zb.onTableEvent((table, ev) => markVerb(table, ev));
  zb.onAnyChange(() => void recount());

  // ── demo actions: three verbs, all through mutate() ───────────────────────

  /// Each returns immediately — the count updates when the CDC echo is APPLIED,
  /// because that is when the row actually exists locally.
  const insertRandom = async () => {
    const id = zb.uuid();
    const version = zb.newVersion();
    await zb.mutate('test_types', 'INSERT', { uid: id }, {
      uid: id,
      some_text: `random ${Math.floor(Math.random() * 10_000)}`,
      age: Math.floor(Math.random() * 90),
      is_true: Math.random() > 0.5,
      tenant_id: zb.tenant || undefined,
      updated_at: version,
      inserted_at: version,
    }, { version });
  };

  /// The most recently touched LIVE row — a soft-deleted one is still here (§7.5),
  /// and updating it would be writing to something the user already deleted.
  const lastLiveUid = async (): Promise<string | null> => {
    try {
      const r = await zb.query(`SELECT uid FROM test_types WHERE deleted_at IS NULL ORDER BY updated_at DESC LIMIT 1`);
      return r[0]?.uid ?? null;
    } catch {
      return null;
    }
  };

  const updateLast = async () => {
    const uid = await lastLiveUid();
    if (!uid) return appendLog('SYS', 'nothing live to update', 'WARNING');
    const version = zb.newVersion();
    await zb.mutate('test_types', 'UPDATE', { uid }, {
      uid, some_text: `updated ${new Date().toLocaleTimeString()}`, updated_at: version,
    }, { version });
  };

  /// A delete from the edge is a SOFT delete (§7.5): it comes back as an UPDATE
  /// setting the tombstone — which is why markVerb reads the tombstone, not the op.
  const deleteLast = async () => {
    const uid = await lastLiveUid();
    if (!uid) return appendLog('SYS', 'nothing live to delete', 'WARNING');
    await zb.mutate('test_types', 'DELETE', { uid });
  };

  /// INSERT on first click (no row yet, needs inserted_at for the NOT NULL), UPDATE
  /// after — sending only what each verb actually needs.
  const bumpCounter = async (table: 'counter_public' | 'counter_tenant', delta: number) => {
    let row: { uid: string; value: number } | null = null;
    try {
      const r = await zb.query(`SELECT uid, value FROM ${table} LIMIT 1`);
      row = r[0] ? { uid: r[0].uid, value: r[0].value } : null;
    } catch { /* table not ready yet */ }

    const id = row?.uid ?? zb.uuid();
    const version = zb.newVersion();
    const data: Record<string, unknown> = { uid: id, value: (row?.value ?? 0) + delta, updated_at: version };
    if (table === 'counter_tenant') data.tenant_id = zb.tenant || undefined;
    if (!row) data.inserted_at = version;

    await zb.mutate(table, row ? 'UPDATE' : 'INSERT', { uid: id }, data, { version });
  };

  /// `users` is outbound-only (no bridge_writer grant): expect ~4s of bridge retries,
  /// then `{"status":"failed","reason":"MutationFailed","sqlstate":"42501",...}`.
  const publishToReadOnlyTable = async () => {
    const version = zb.newVersion();
    const id = Math.floor(Date.now() / 1000);
    await zb.mutate('users', 'INSERT', { id }, {
      id, name: 'should not land', updated_at: version, inserted_at: version,
    }, { version });
  };

  /// A payload with NO key — `MissingPrimaryKey` is permanent, dead-lettered on first
  /// delivery, verdict `rejected` immediately. Deliberately bypasses mutate() (which
  /// would refuse to build it): rawMutation is the escape hatch that exists for this.
  const publishMalformed = async () => {
    const version = zb.newVersion();
    const id = zb.uuid();
    await zb.rawMutation('test_types', 'INSERT', id, version, {
      data: { uid: id, some_text: 'malformed on purpose', updated_at: version, inserted_at: version },
      version,
      client_id: zb.clientId,
    });
  };

  // ── SQL console — the replica IS the API ──────────────────────────────────
  const [sqlText, setSqlText] = createSignal(
    'SELECT name, email, updated_at FROM users ORDER BY updated_at DESC LIMIT 8',
  );
  const [sqlRows, setSqlRows] = createSignal<any[] | null>(null);
  const [sqlError, setSqlError] = createSignal<string | null>(null);
  const [sqlLive, setSqlLive] = createSignal(true);
  const [sqlMs, setSqlMs] = createSignal<number | null>(null);
  let sqlHasRun = false;
  const sqlIsRead = () => /^\s*(select|with|explain|pragma|values)\b/i.test(sqlText());
  const runSql = async () => {
    const q = sqlText().trim();
    if (!q) return;
    sqlHasRun = true;
    const t0 = performance.now();
    try {
      const rows = await zb.query(q);
      setSqlMs(Math.round(performance.now() - t0));
      setSqlError(null);
      setSqlRows(Array.isArray(rows) ? rows.slice(0, 200) : []);
    } catch (e: any) {
      setSqlError(String(e?.message ?? e));
      setSqlRows(null);
      setSqlMs(null);
    }
  };

  // ── boot ──────────────────────────────────────────────────────────────────
  void zb.connect().catch(() => { /* logged by the class; badge shows disconnected */ });
  void recount();

  /// Fetched through the Vite proxy (`/bridge/*`) — under COEP a cross-origin
  /// response needs CORS + CORP headers the bridge does not (and should not) send.
  let healthWarned = false;
  const pollHealth = async () => {
    try {
      const r = await fetch('/bridge/health', { signal: AbortSignal.timeout(3000) });
      setHealth(r.ok ? 'up' : 'down');
      healthWarned = false;
    } catch (err: any) {
      setHealth('down');
      if (!healthWarned) {
        healthWarned = true;
        appendLog('SYS', `bridge /health unreachable: ${err?.message ?? err}`, 'WARNING');
      }
    }
  };
  void pollHealth();
  const healthId = setInterval(pollHealth, 10_000);

  onCleanup(() => {
    clearInterval(healthId);
    void zb.close();
  });

  return (
    <>
      <header>
        <h1>ZeBridge CDC Web Consumer</h1>
        <div class="status-bar">
          <span class={`badge ${status()}`}>{status().toUpperCase()}</span>
          <span id="server-url">{NATS_URL}</span>
          <span class={`badge ${health() === 'up' ? 'connected' : 'disconnected'}`}>
            bridge {health()}
          </span>
        </div>

        {/* The startup state machine. Each cell greens once and stays green — the
            useful signal is how FAR it got. */}
        <ul class="phases">
          <For each={[
            ['connected', 'NATS connected'],
            ['migrated', 'schema migrated'],
            ['snapshot', 'snapshot replayed'],
            ['cdc', 'CDC active'],
          ]}>
            {([key, label]) => (
              <li classList={{ done: phase()[key] }}>{label}</li>
            )}
          </For>
        </ul>
      </header>

      <For each={Object.entries(suspended())}>
        {([table, reason]) => (
          <div class="suspended-banner">
            ⏸ <strong>{table}</strong> is suspended upstream ({reason}). Local rows are frozen and
            still valid, but no new events or snapshots will arrive, and writes are refused
            client-side, until the shape is fixed.
          </div>
        )}
      </For>

      <main>
        <h3>Replica</h3>
        <p id="sync-state">
          principal <strong>{EFFECTIVE_PRINCIPAL}</strong> · client <strong>{zb.clientId}</strong>
          {' '}· tenant <strong>{tenant()}</strong>
          {' '}· held {pendingCount()}
        </p>
        <table class="tables-summary">
          <thead>
            <tr><th>table</th><th>rows</th><th>last CDC</th><th>log</th><th>actions</th></tr>
          </thead>
          <tbody>
            <For each={syncedTableNames()}>
              {(t) => (
                <tr>
                  <td><strong>{t}</strong></td>
                  <td class="count">{counts()[t] ?? 0}</td>
                  <td>
                    <span class={`verb ${lastVerb()[t] ? 'verb-' + lastVerb()[t] : ''}`}>
                      {lastVerb()[t] || ''}
                    </span>
                  </td>
                  <td>
                    <input
                      type="checkbox"
                      checked={!!logging()[t]}
                      onChange={(e) =>
                        setLogging((prev) => ({ ...prev, [t]: e.currentTarget.checked }))
                      }
                    />
                  </td>
                  <td>
                    {t === 'test_types' ? (
                      <span class="row-actions">
                        <button onClick={() => void insertRandom()}>INS</button>
                        <button onClick={() => void updateLast()}>UP</button>
                        <button onClick={() => void deleteLast()}>DEL</button>
                      </span>
                    ) : (
                      <em class="readonly">read-only</em>
                    )}
                  </td>
                </tr>
              )}
            </For>
          </tbody>
        </table>

        <div class="controls">
          <button onClick={() => void recount()} style="background: #37474f;">Recount</button>
        </div>

        {/* SQL console — arbitrary SQL against THIS tab's own replica. Worst case you
            wreck your copy; a reload rebuilds it from the generation chain. */}
        <div class="controls" style="flex-direction: column; align-items: stretch; gap: 6px;">
          <span>SQL console — your local replica, this tab only:</span>
          <textarea
            rows={3}
            style="width: 100%; font-family: monospace; font-size: 12px; background: #263238; color: #eceff1; border: 1px solid #455a64; border-radius: 4px; padding: 6px; box-sizing: border-box;"
            value={sqlText()}
            onInput={(e) => setSqlText(e.currentTarget.value)}
            onKeyDown={(e) => { if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); void runSql(); } }}
          />
          <div style="display: flex; gap: 8px; align-items: center; flex-wrap: wrap;">
            <button onClick={() => void runSql()} style="background: #37474f;">Run (⌘⏎)</button>
            <label style="display: flex; gap: 4px; align-items: center; font-size: 12px; cursor: pointer;">
              <input type="checkbox" checked={sqlLive()} onInput={(e) => setSqlLive(e.currentTarget.checked)} />
              live — re-run when CDC touches the replica
            </label>
            {sqlMs() != null && (
              <span style="font-size: 12px; opacity: 0.7;">
                {sqlRows()?.length ?? 0} row(s) · {sqlMs()}ms{(sqlRows()?.length ?? 0) === 200 ? ' · capped at 200' : ''}
              </span>
            )}
          </div>
          {!sqlIsRead() && (
            <div style="font-size: 12px; color: #ffb74d;">
              ⚠ not a read: this executes locally only — the feed owns these tables, so this
              edit lasts exactly until the row next changes upstream. Real writes go through
              the outbox, never SQL.
            </div>
          )}
          {sqlError() && (
            <div style="font-size: 12px; color: #ef5350; font-family: monospace;">{sqlError()}</div>
          )}
          {sqlRows() && sqlRows()!.length > 0 && (
            <div style="overflow-x: auto;">
              <table class="tables-summary" style="font-size: 12px;">
                <thead>
                  <tr><For each={Object.keys(sqlRows()![0])}>{(c) => <th>{c}</th>}</For></tr>
                </thead>
                <tbody>
                  <For each={sqlRows()!}>{(r) => (
                    <tr>
                      <For each={Object.keys(sqlRows()![0])}>{(c) => (
                        <td>{r[c] === null ? 'NULL' : String(r[c])}</td>
                      )}</For>
                    </tr>
                  )}</For>
                </tbody>
              </table>
            </div>
          )}
          {sqlRows() && sqlRows()!.length === 0 && !sqlError() && (
            <div style="font-size: 12px; opacity: 0.7;">0 rows</div>
          )}
        </div>

        {/* Enrollment: the pump-starter as a BUTTON. Paste the one-time code the
            operator handed you; the click generates this tab's own nkey pair,
            sends code + public key to the bridge's mint, and reloads connected
            as whoever the returned JWT says you are. */}
        <div class="controls">
          <span>enroll:</span>
          <input
            id="invite-input"
            value={inviteCode()}
            onInput={(e) => setInviteCode(e.currentTarget.value)}
            placeholder="one-time invite code"
            style="width: 22rem; font-family: monospace;"
          />
          <button
            onClick={() => void enroll(inviteCode()).then((err) => setEnrollMsg(err ?? ''))}
            style="background: #6a1b9a;"
          >Enroll → get JWT</button>
          <span style="color: #ef6c00;">{enrollMsg()}</span>
        </div>

        {/* One row per demo: the three test_types buttons are a set (accepted /
            refused-by-grant / refused-by-shape), the counters are the offline-first
            walkthrough fixture — one public, one tenant-scoped. */}
        <div class="controls">
          <span>test_types:</span>
          <button onClick={() => void insertRandom()} style="background: #2e7d32;">Push — accepted</button>
          <button onClick={() => void updateLast()} style="background: #1565c0;">Update last row</button>
          <button onClick={() => void deleteLast()} style="background: #ef6c00;">Delete last row</button>
        </div>

        <div class="controls">
          <span>counter (public):</span>
          <button onClick={() => void bumpCounter('counter_public', -1)} style="background: #ef6c00;">-</button>
          <strong>{counterValues()['counter_public'] ?? 0}</strong>
          <button onClick={() => void bumpCounter('counter_public', 1)} style="background: #2e7d32;">+</button>
        </div>

        <div class="controls">
          <span>counter (tenant):</span>
          <button onClick={() => void bumpCounter('counter_tenant', -1)} style="background: #ef6c00;">-</button>
          <strong>{counterValues()['counter_tenant'] ?? 0}</strong>
          <button onClick={() => void bumpCounter('counter_tenant', 1)} style="background: #2e7d32;">+</button>
        </div>

        <div class="controls">
          <button onClick={() => void publishToReadOnlyTable()} style="background: #6a1b9a;">Push to users — refused</button>
          <button onClick={() => void publishMalformed()} style="background: #b71c1c;">Push malformed — verdict now</button>
        </div>
      </main>
    </>
  );
}
