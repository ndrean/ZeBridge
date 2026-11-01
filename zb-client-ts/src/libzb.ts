/// libzb.ts — the ZeBridge client core, extracted from App.tsx (NOTES.md §10).
///
/// "Theater around a subscription": App.tsx keeps the theater; this file keeps the
/// subscription — schema watch, chain-first seeding with the snapshot fallback, CDC
/// apply, the LWW write path (outbox, optimistic apply, verdicts, echo-confirm), and
/// the change doorbell. App.tsx is this module's FIRST consumer, so the browser demo
/// is the regression test for the extraction; a headless Node consumer is the second.
///
/// This is also the TypeScript ancestor of the sans-I/O wasm eater: transport (the
/// "speaker": this file's NATS calls) and decision logic (the "eater": plans, guards,
/// verdict transitions) are kept separable on purpose, so the later split is a
/// refactor, not a rewrite.
///
/// The consumer contract (§10, the index card):
///   query      — arbitrary SELECTs against the local replica; the replica IS the API
///   mutate     — three verbs, a 1:1 constructor for the wire message; no SQL parsed
///   onChange   — the doorbell; the app re-queries
///
/// Browser-tier write guard: this API exports no write path except `mutate()`. The raw
/// `sql` handle stays public for the SQL console and for people deliberately off the
/// path (one sqlocal connection — OPFS sync handles are exclusive, so a second
/// read-only connection is not available here the way `libzebridge` native has one).

import { natsTransport } from './transport.ts';
import { Measure, MeasureAttempt, emptyMeasureCounts, type MeasureCounts } from './measure.ts';
import { endpointIdentity, identityEndpointUrl, orderedEndpoints } from './endpoints.ts';
import { currentPlatform, type Platform, type PlatformName } from './platform.ts';
import type { Transport, TransportConnection, JetStreamOpts, UserKeyPair } from './transport.ts';
import { decode, encode, decodeMulti } from '@msgpack/msgpack';
import type { Storage, StorageFactory, Exec as StorageExec } from './storage.ts';

import { sqliteDialect, type Dialect } from './dialect.ts';
import { decideFence, fenceScope as fenceScopeOf, frontierDrops, verifyRoutes, type FenceRoute, type Frontier, type Untouched } from './fence.ts';
import { loadCore, scopeSeeding, caughtUpPosition, streamResume, deltaEligible, cdcBacklogBytes, streamDeltaChoice, streamForward } from './wasm-core.ts';
import { v7 as uuidv7 } from 'uuid';
import { heartbeatPayload,
  seedGateDrops, tombstoned, planFromManifest, fullPredatesReplica as coreFullPredates,
  advancePosition, foreignKeyFailureKind, lsnToNumber, pgTsToWire, tableSet,
  planKeyChange, planUpsert, planUpdate, planExists, planDelete, pgEngineValues, chainUpsertSql, chainRowParams,
  type SqlStep,
  fkClausesFor, createTableSteps, rebuildSteps, diffColumns, keyShape, typeShape, retypedColumns, isReadOnlySql,
  mutationSubject, mutationMsgId, idKeyPart, mutationKeyId, mutationPayload, optimisticEvent, normalizeOp,
  normalizeVersion, maxVersion, hlcVersion,
  fkTextDiffers, viewSteps, indexSyncPlan, outboxWatermarkGate,
  isBytes, pgArrayValues, pgArrayLiteral, sortRowsByKey, chainBulkSql, chainChunkJson, parseChainHead, msgpackScan, msgpackDecodeScanned, chainStageSql, vecColsOf, pgVectorValues, vecLiteral, strictMissing,
  planCdcBulk, type CdcBulkTable, cdcValue,
} from './core.ts';
import type { VecCol } from './core.ts';
import type { PlanStep } from './core.ts';

/// §10jc: how long the server keeps a tail consumer with no pull outstanding —
/// libzb's `tail_inactive_ns`, the same number. Unset, nats-server's ephemeral default
/// of 5 s applies, and a phone applying one 20,000-event batch spends longer than that
/// between pulls: the server deleted the consumer, the deaf watchdog found it gone 25 s
/// later and recreated it — every ~50 s on an iPhone 12 at 10k events/s, a third of the
/// time spent not reading. Measured 2026-09-26.
const TAIL_INACTIVE_NS = 120 * 1_000_000_000;

/// §10jc test hook: ZB_TEST_DROP_DELIVERY=N (Node only) discards every Nth delivery as if
/// lost in transit. 0: off.
const TEST_DROP_EVERY = Number((globalThis as any).process?.env?.ZB_TEST_DROP_DELIVERY || 0); // no Node types in a browser build
let testDropCount = 0;
import GRAMMAR_JSON from './grammar.json' with { type: 'json' };

/// §10dq: the wire grammar, compiled in — a copy of `src/grammar.json` pinned
/// byte-for-byte by core.test.ts. Nothing fetches it, nothing is passed in; a
/// rename is a protocol fork whose cost is a rebuild on both sides.
export const GRAMMAR: any = GRAMMAR_JSON;

/// sha256 of the embedded grammar (lowercase hex): the value the bridge serves as
/// `X-Grammar-Hash` and in the /enroll payload's `grammar_hash` for the same bytes.
/// Computed on the canonical text the bridge embeds, so the JSON is re-serialized
/// the way the file is written: two-space indent, trailing newline.
export async function grammarHashHex(): Promise<string> {
  const text = JSON.stringify(GRAMMAR, null, 2) + '\n';
  const digest = await globalThis.crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

export interface ZeBridgeConfig {
  /// Replace the whole wire layer (NATS factories, headers, wire constants) —
  /// the transport seam (transport.ts). Default: the @nats-io libraries.
  transport?: Transport;
  /// Where NATS is. Optional since §10jq: an enrolled client takes it from its identity
  /// (`nats_url`, or `nats_ws_url` in the browser).
  natsUrl?: string;
  /// Optional since §10jq: the creds (or the identity) name the principal.
  principal?: string;
  password?: string;
  /// §10jq: a one-time invite code. With `bridgeUrl` and no stored identity, `connect()`
  /// generates this device's key pair, redeems the code at `<bridgeUrl>/enroll`, and
  /// keeps the result (`identityPath`) — every later run needs neither. libzb: same.
  invite?: string;
  /// §10jq: where the identity is kept: a file on Node, a key in the browser's
  /// localStorage. Default `<dbPath>.identity`, else
  /// `zebridge.identity`. libzb: same name, same default, same JSON.
  identityPath?: string;
  /// Override the platform's zstd (platform.ts). An app never needs to: Node inflates
  /// with node:zlib, the browser with fzstd.
  zstdDecompress?: (b: Uint8Array) => Uint8Array | Promise<Uint8Array>;
  /// §10ip: override how a query ANSWER is compressed when this client SERVES. A host
  /// without a compressor answers uncompressed, and every asking client reads both.
  zstdCompress?: (b: Uint8Array) => Uint8Array | Promise<Uint8Array>;
  /// Operator/JWT mode: the CONTENT of a .creds file (user JWT + nkey seed) — what
  /// /enroll returns. When set it wins over user/password — the JWT carries the
  /// permissions (scoped signing key), so no server conf names this principal at all.
  /// libzb takes the same option.
  creds?: string;
  /// The same credentials as a FILE, where there is a filesystem (Node). libzb: same.
  credsPath?: string;
  /// Where the replica lives: a file on Node, an OPFS database name in the browser. Default `zebridge_<principal>.sqlite3`, kept across runs (libzb: the
  /// same default). A fresh name per run — `zebridge_${Date.now()}.sqlite3` — is a
  /// clean room.
  dbPath?: string;
  /// Stable across restarts: it prefixes every mutation's msg_id. Default: a random one
  /// per instance. libzb: same.
  clientId?: string;
  /// Assert the platform. The bundler already picks the entry (package.json `exports`:
  /// browser, node); this only refuses a build that loaded another one.
  platform?: PlatformName;
  /// The grammar hash this client RECEIVED — from the /enroll payload beside the JWT,
  /// or the bridge's `X-Grammar-Hash` header. When set, a mismatch refuses to connect:
  /// this library is built for another protocol than the bridge it is pointed at.
  /// Unset skips the check (a bridge that could not be reached is not a mismatch).
  grammarHash?: string;
  /// The JetStream domain the deployment's grants name — from the /enroll payload's
  /// `js_domain` beside the JWT. Unset speaks to the server's own JetStream; a name
  /// addresses `$JS.<name>.API.`, which is how a client on a leaf node reaches the
  /// hub's. The wrong value is not detectable at connect: every API call simply gets
  /// no responder, so it fails at the first request.
  jsDomain?: string;
  /// Optional JetStream domain of the connected leaf's seed mirrors. Generation
  /// manifests and objects are read there first; a missing, stale or incomplete
  /// copy falls back to jsDomain. Schemas, membership, CDC and writes use jsDomain.
  seedDomain?: string;
  /// The bridge's HTTP url: where `invite` is redeemed (`/enroll`) and the JWT renewed
  /// (`/renew`), kept in the identity; and where the grammar hash is fetched when
  /// `grammarHash` is unset. libzb: same name.
  bridgeUrl?: string;
  /** @internal The compiled-in grammar (§10dq). Not a consumer input: any value passed here is replaced. */
  grammar?: any;
  /// PROTOCOL §11: the fleet heartbeat cadence in ms (default 30 000; 0 disables).
  heartbeatMs?: number;
  /// §10fb: follow only these tables. Default: every table the schemas bucket names
  /// (a job that wants one table of a tenant with a big one should say so — libzb
  /// takes the same list).
  /// §10hn: the tables to seed and tail — a list, or '*' for every published table.
  /// Absent, nothing is followed (and the log says so): a client declares what it
  /// holds, or says '*'. The same rule as libzb's `tables`; core.tableSet decides.
  tables?: string[] | '*';
  /// §10hn (libzb §10hj): tables held ON DEMAND — the schema arrives and the local
  /// table is created, nothing seeds it, no stream is tailed for it; rows come only
  /// through `ingest` answering this client's own `request`s. A table in both lists
  /// is on-demand.
  ondemandTables?: string[];
  /// §10fb: rows per transaction when a chain step seeds a table (default 50 000;
  /// 0 = one transaction for the step). Bounds memory and how long the lock is held.
  seedChunkRows?: number;
  /// TEST HARNESSES ONLY. Called at the fence's boundaries — `before_decision` (the chain is
  /// read and proven, the position not yet: a harness that waits here lets the tails keep
  /// applying, so the position runs ahead of the chain's cut), `after_chunk` (a window of the
  /// replacement committed), `before_verify`, `before_commit`, and `shared_lookup` (the
  /// shared route's stream lookup; a throw there makes the route unreadable). A harness
  /// crashes the process, sleeps while it prunes the stream, or throws. Never set in an app.
  faultHook?: (point: 'before_decision' | 'after_chunk' | 'before_verify' | 'before_commit' | 'after_commit' | 'shared_lookup') => void | Promise<void>;
  /// §10ix (libzb: same name): apply a chain step AS IT ARRIVES — chunks inflated and
  /// decoded into windows of `seedChunkRows`, never the whole document. Measured on a
  /// 3M-row base: 2.5 GB peak when the document was materialised whole (§10iw).
  /// Default false, like libzb.
  seedStreaming?: boolean;
  /// §10ix (libzb: same name): stream only a step whose stored object is at least this
  /// many bytes (default 8 MiB); below it the buffered path is cheaper.
  seedStreamingAboveBytes?: number;
  /// Override the platform's STREAMING inflate (chunks in, inflated bytes out). Every
  /// chain object is a plain frame (NOTES §10iy), so any plain-frames decoder will do.
  zstdDecompressStream?: (chunks: AsyncIterable<Uint8Array>) => AsyncIterable<Uint8Array>;
  /// §10hc: apply a CDC batch through `core.planCdcBulk` — one statement per run of
  /// eligible events, the per-event path for the rest (default true; false = every
  /// event through `applyEvent`, the A/B for a measurement).
  bulkCdc?: boolean;
  /// §10hd: how a `bulk` segment is executed on SQLite. `rows` (default) binds one
  /// prepared VALUES upsert per row — measured faster than the multi-row
  /// `INSERT … SELECT FROM json_each` (`json_each`), which SQLite materializes first.
  bulkStatement?: 'rows' | 'json_each';
  /// §10hd: events per CDC transaction (default 20 000; the 200 ms timer and the
  /// last-in-flight rule still bound latency). One commit per 430-event message
  /// applied 24k rows/s on the firehose replica; fifty messages per commit, 59k.
  cdcBatchEvents?: number;
  /// 'sqlite' (default) or 'pglite' (browser and Node). libzb: 'sqlite' or 'duckdb'.
  engine?: 'sqlite' | 'pglite';
  /// Override the two seams (NOTES §10) — for a test, or a storage of your own. The
  /// platform provides both: better-sqlite3 + TCP on Node, sqlite-wasm on OPFS (or
  /// PGlite) + WebSocket in the browser.
  storage?: StorageFactory;
  connect?: (opts: any) => Promise<TransportConnection>;
}

/// `{"$bin": "<base64>"}` → bytes; anything else as it is. The marker a JSON answer
/// uses for a byte column (PROTOCOL §2), decoded before the chain upsert.
function binMarker(v: any): any {
  if (v && typeof v === 'object' && !Array.isArray(v) && typeof v.$bin === 'string' && Object.keys(v).length === 1) {
    const bin = atob(v.$bin);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }
  return v;
}

export type TableState = {
  pkCols: string[];
  columns: string[];
  /// §10ey: the columns whose PostgreSQL type is an array — the wire carries them as
  /// JSON text, and a PostgreSQL engine binds them from the literal on apply.
  arrayCols?: string[];
  /// §10fc: the columns declared BLOB in the sqlite block — their bytes cross a seed
  /// chunk's JSON as {"$x": hex}, back through unhex (core.chainChunkJson).
  blobCols?: string[];
  /// §10fg: the pgvector/bit columns (`pg` block) — a PostgreSQL engine binds the
  /// text form of the wire BLOB; SQLite keeps the BLOB.
  vecCols?: VecCol[];
  tombstoneColumn: string | null;
  tenantColumn: string | null;
  /// The table's LWW version column (from the schema payload) — read to feed
  /// the HLC floor; the guard itself runs in SQL and in PG.
  versionColumn?: string | null;
  /// The catalogue's seed_epoch the descriptor carried (§10df).
  seedEpoch?: number;
  lsn: number;
  /// The seed gate's PRIMARY anchor (finding 7, NOTES §10i): the CDC stream's
  /// last_seq captured by the producer AT CHAIN BUILD TIME, and which stream it
  /// belongs to. Stream sequence is commit-ordered and monotonic — lsn is NOT
  /// (a transaction that begins early and commits late delivers late with a
  /// LOWER lsn), so gating on lsn silently dropped in-flight transactions.
  seedSeq?: number;
  seedStream?: string;
  /// §10lw: the chain's cut on CDC_PUBLIC for a tenant table (its open-tenant rows ride
  /// there): what the seed proved on the shared route. Absent: nothing proved.
  sharedSeedSeq?: number;
  /// FINDING 10: the lsn fallback gate must compare against a lsn that a SEED set —
  /// never `state.lsn`, which the boot schema-republish advances to the WAL head, so
  /// after any bridge restart every replayed data event carried an older lsn and was
  /// dropped as "already seeded" (measured: two rows consumed, accounted, and absent).
  seedLsn?: number;
  /// §10et: followed but not yet seeded — the chain was not there when this client
  /// connected (a table enabled between two producer ticks, or a chain that
  /// predates the stream, §10ei). Its CDC events are HELD, not applied and not
  /// dropped, until the chain lands; a background loop keeps asking for it.
  unseeded?: boolean;
  unseededHeld?: number;
};

/// 'snapshot' means "seeded" — the name predates the retirement of
/// snapshot-on-demand and is kept for UI compatibility.
export type Phase = 'connected' | 'migrated' | 'snapshot' | 'cdc';

/// What became of ONE write, once and for good (`onVerdict`). `version` is the one
/// `mutate()` returned. The library has already acted on it — dropped it from the outbox,
/// re-sent it, or put the local row back — and only then says so:
///   applied   PostgreSQL took it (`reason: 'version_clamped'` when this clock was ahead);
///   rebased   a newer row won, but on other columns: re-sent as `rebasedAs`, which gets
///             its own verdict;
///   lost      a newer row won on the same columns (`lostColumns`), or a DELETE lost to a
///             newer edit: the winning row stands, it arrives via CDC;
///   deleted   the row was deleted elsewhere: the local copy is reverted;
///   rejected  refused for good (`reason`, e.g. a policy): the local copy is reverted.
///             When PostgreSQL refused it, `sqlstate` and `detail` are its own code and
///             message (22023 a malformed register document, 42501 row-level security):
///             `reason` names the bridge's error, the same for both.
/// A write kept for retry (`failed`, `rate_limited`) has no verdict yet.
///   indeterminate  settled, but whether it committed is unknown (`reason`: MessageIdReused,
///             MessageIdExpired, ReceiptUnreadable). Never re-sent under a new ID: its table
///             reseeds from the hub (`reconciling()` counts it until then), then
///   reconciled  the local copy is the authoritative state, whichever way it went.
export type Verdict = {
  version: string;
  table: string;
  key: Record<string, unknown>;
  columns: string[];
  outcome: 'applied' | 'rebased' | 'lost' | 'deleted' | 'rejected' | 'indeterminate' | 'reconciled';
  rebasedAs?: string;
  lostColumns?: string[];
  reason?: string;
  sqlstate?: string;
  detail?: string;
};
/// §10ix: one table being seeded, a window at a time. `applied` counts the step's rows
/// handled so far, `total` the step's row count, `kind` the chain step's (full, delta).
/// `done` is true only on the event after the step is fully in the table: on the staged
/// path the rows are STAGED as they arrive and sorted into the table at the end, so
/// `applied === total` arrives ~20 s before `done` on a 3M-row full — a host that shows
/// a bar should read `done`, not the count, and say "sorting" in between.
export interface SeedProgress { table: string; step: string; kind: string; applied: number; total: number; done: boolean }
export type ConnStatus = 'connected' | 'disconnected' | 'connecting';

/// A table the joint fence replaces together with the fenced one: unseeded, with a chain the hub's streams still prove.
interface Joiner { table: string; man: any; pr: Proof }

/// What `proveManifest` established about one manifest.
type Proof = { gateSeq: number; sharedSeq: number | null; ownReplayable: boolean; sharedProven: boolean; ownLast: number | null };

/// The routes one chain replacement holds (see beginReplacement); `fence`: its PREPARE parked them.
interface Replacement {
  routes: string[];
  fence: boolean;
  /// The replacement is durably OWED (`_zebridge_fence`, anchor cleared, table unseeded): a fence
  /// from its PREPARE, a plain full from `oweReplacement`.
  owed: boolean;
}

/// One seed's claim on the stream queue (see applyGenerations).
interface SeedRun {
  /// Started inside a stream pass, which holds the queue.
  inPass: boolean;
  /// A pass joined this seed while it ran: that pass lent its turn.
  joinedByPass: boolean;
  /// Set while the seed waits in the queue; starts its work early under a joining pass's turn.
  lend?: () => void;
}

interface BucketEntry {
  key: string;
  operation: 'PUT' | 'DEL' | 'PURGE';
  value: Uint8Array;
  delta: number;
}

/// Snapshot-on-demand seeding is DELETED (2026-08-27, NOTES §10p) — generations
/// are the only seed path. Its three findings (a stale descriptor that DELETEd
/// 5,000 correct rows, the SNAP_RET throttle deadlock, head-of-line blocking)
/// live in NOTES §10g/§10h.
/// How long a client waits for the producer to publish a usable chain before
/// declaring the table unseedable. Covers a fresh table between two cadence ticks;
/// a table still chainless after this fails LOUDLY and seeds on the next connect.
const GENERATION_WAIT_MS = 90_000;
const GENERATION_SLOW_POLL_MS = 15_000; // after the first window: the producer's cadence is minutes
const GENERATION_POLL_MS = 10_000;
/// §10et: events held for a table waiting for its chain, at most, per table.
const UNSEEDED_HOLD_MAX = 50_000;
/// Derived from bridge-side max_deliver × retry sleep + batch window + slack; see the
/// verdict-timeout discussion in PROTOCOL §7 — a guess kept in step by hand until it
/// rides in the schema descriptor.
const WRITE_TIMEOUT_MS = 10_000;

/// Watch a KV bucket through a plain pull consumer instead of `kv.watch()` — the
/// ordered-push consumer `kv.watch()` uses leaks a consumer per reset under this
/// server's grant set (NOTES.md §1.14). Same semantics the callers rely on:
/// LastPerSubject replays current values first; `delta === 0` marks the replay's end.
async function watchBucket(
  js: any,
  bucket: string,
  filterKey: string = '>',
): Promise<{ pending: number; entries: AsyncIterable<BucketEntry>; stop: () => void }> {
  const stream = `KV_${bucket}`;
  const prefix = `$KV.${bucket}.`;
  const jsm = await js.jetstreamManager();
  const ci = await jsm.consumers.add(stream, {
    deliver_policy: 'last_per_subject', // NATS wire constant (transport.DELIVER_POLICY)
    filter_subject: `${prefix}${filterKey}`,
    ack_policy: 'none',
  });
  const consumer = await js.consumers.get(stream, ci.name);
  const iter = await consumer.consume();
  const entries = (async function* () {
    for await (const m of iter) {
      const op = m.headers?.get('KV-Operation') || 'PUT';
      yield {
        key: m.subject.substring(prefix.length),
        operation: (op === 'DEL' || op === 'PURGE' ? op : 'PUT') as BucketEntry['operation'],
        value: m.data,
        delta: m.info.pending,
      };
    }
  })();
  return { pending: ci.num_pending ?? 0, entries, stop: () => { try { iter.stop(); } catch { /* closed */ } } };
}

// pgTsToWire and lsnToNumber live in core.ts (§10s).
const td = new TextDecoder();
type Exec = (q: string, ...params: any[]) => Promise<any[]>;

/// Assemble a .creds file's text from a JWT and a seed — what the enrollment
/// flow holds after the mint responds (the seed never crossed the wire; the app
/// generated the pair itself).
// foreignKeyFailureKind lives in core.ts (§10s) — the three measured SQLite messages.

/// Generate the nkey pair a client enrols with — libzb's `zb_create_user()`, and the
/// same `{publicKey, seed}` shape. The SEED is the private half: it never leaves the
/// host, and the host stores it (a keychain, IndexedDB, a 600 file) beside the JWT
/// that `GET /enroll?code=…&user_pubkey=<publicKey>` mints for it. `credsFileText`
/// then joins the two into what `config.creds` takes.
///
/// Callable before any client exists — enrolment comes first — so it takes the
/// transport rather than reading one off an instance. The default is the NATS seam;
/// a port with its own crypto passes its own (§10s).
export function createUser(transport: Transport = natsTransport): UserKeyPair {
  return transport.createUser();
}

/// §10jq: what an enrollment leaves behind — the same JSON libzb writes, so a Node
/// client and a libzb client can share one identity file.
export interface EnrolledIdentity {
  version: 1;
  bridge_url: string;
  principal: string;
  creds: string;
  nats_url?: string;
  nats_ws_url?: string;
  nats_urls?: string[];
  nats_ws_urls?: string[];
  grammar_hash?: string;
  js_domain?: string;
  /// The bridge's clock minus this device's, in seconds, from the JWT's `iat` when it
  /// arrived. Renewal is timed and stamped in the bridge's time with it, so a device
  /// whose clock is off still renews on time and is not refused. libzb: the same field.
  clock_offset?: number;
}

const localNow = () => Math.floor(Date.now() / 1000);

/// Now, on the bridge's clock.
export function serverNow(id: EnrolledIdentity): number {
  return localNow() + (id.clock_offset ?? 0);
}

/// The bridge's clock minus the device's, from a JWT just received: its `iat` is the
/// bridge's time when it was minted.
export function offsetFrom(creds: string, local = localNow()): number {
  const t = jwtTimes(creds);
  return t ? t.iat - local : 0;
}

/// Redeem an invite: this device's own key pair (the seed never leaves it), `GET
/// <bridgeUrl>/enroll`, and the identity built from the answer. `connect()` calls it
/// with an `invite`; an app that manages identities itself can call it directly.
export async function enrollAt(bridgeUrl: string, code: string, transport: Transport = natsTransport): Promise<EnrolledIdentity> {
  const base = bridgeUrl.replace(/\/+$/, '');
  const kp = transport.createUser();
  let res: Response;
  try {
    res = await fetch(`${base}/enroll?code=${encodeURIComponent(code.trim())}&user_pubkey=${kp.publicKey}`);
  } catch (e) {
    throw new Error(`enroll: ${base} unreachable (${(e as Error).message})`);
  }
  if (res.status === 404) throw new Error(`enroll: ${base} has enrollment off (404) — the bridge needs ZB_SIGNING_SEED and ZB_ACCOUNT_PUB`);
  if (res.status === 401 || res.status === 403) throw new Error(`enroll: refused (${res.status}): the code is invalid, used or expired, or the principal was revoked`);
  if (!res.ok) throw new Error(`enroll: ${base} answered ${res.status}: ${(await res.text()).slice(0, 200)}`);
  const p = await res.json() as Record<string, string | undefined>;
  if (!p.jwt || !p.principal) throw new Error('enroll: the bridge\'s answer has no jwt or principal');
  return {
    version: 1, bridge_url: base, principal: p.principal, creds: credsFileText(p.jwt, kp.seed),
    clock_offset: offsetFrom(credsFileText(p.jwt, kp.seed)),
    ...endpointIdentity(p),
    ...(p.grammar_hash ? { grammar_hash: p.grammar_hash } : {}),
    ...(p.js_domain ? { js_domain: p.js_domain } : {}),
  };
}

/// §10jt: the JWT's issue and expiry (unix seconds), read from its payload — to decide
/// WHEN to renew; the server checks the signature.
export function jwtTimes(creds: string): { iat: number; exp: number } | null {
  const m = /-----BEGIN NATS USER JWT-----\s*([^\s]+)\s*------END NATS USER JWT------/.exec(creds);
  const part = m?.[1].split('.')[1];
  if (!part) return null;
  try {
    const p = JSON.parse(atob(part.replace(/-/g, '+').replace(/_/g, '/')));
    return typeof p.iat === 'number' && typeof p.exp === 'number' ? { iat: p.iat, exp: p.exp } : null;
  } catch { return null; }
}

/// Renew when less than a quarter of the JWT's life is left (or it is gone). `now` is the
/// bridge's time (`serverNow`), the clock the JWT's times come from. libzb: same line.
export function renewDue(creds: string, now: number): boolean {
  const t = jwtTimes(creds);
  return !!t && t.exp > t.iat && (t.exp - now) * 4 < t.exp - t.iat;
}

/// §10kn: `/renew` refused a revoked key AND asked for its local data to be deleted
/// (`bridge --revoke <principal> --purge`).
export class RevokedPurge extends Error {
  readonly purge = true;
  constructor() {
    super('renew: refused — the principal was revoked, and its local data must be deleted');
  }
}

/// No stored identity and no invite: this device was never enrolled here. Its own class,
/// so an app can tell "ask the user for the invite link" from a real failure.
export class NotEnrolled extends Error {
  readonly notEnrolled = true;
  constructor() {
    super('not enrolled: no identity is stored here, and no invite was given — open the invite link (an `invite` with `bridgeUrl`)');
  }
}

/// A stored identity this client can connect with: creds, and a NATS URL for its
/// platform (or one the app passes itself). Text that does not parse cannot be used.
export function identityUsable(text: string, natsUrl: string | undefined, overWebSocket: boolean): boolean {
  try {
    const id = JSON.parse(text) as Partial<EnrolledIdentity>;
    if (typeof id.creds !== 'string' || id.creds.length === 0) return false;
    return !!(natsUrl ? orderedEndpoints(natsUrl).length : identityEndpointUrl(id, overWebSocket));
  } catch {
    return false;
  }
}

/// §10jt: a new JWT for the SAME key, no invite — sign `zebridge-renew:<pub>:<ts>` with the
/// identity's seed, `GET <bridge>/renew`, and rebuild the identity (NATS URLs, grammar
/// hash and memberships come back current). libzb's `renew`, the same wire.
export async function renewAt(id: EnrolledIdentity, transport: Transport = natsTransport): Promise<EnrolledIdentity> {
  const seed = /-----BEGIN USER NKEY SEED-----\s*([^\s]+)\s*------END USER NKEY SEED------/.exec(id.creds)?.[1];
  if (!seed) throw new Error('renew: the identity\'s creds hold no seed');
  // Stamped in the bridge's time. If the device's clock moved since the offset was taken,
  // the bridge refuses the stamp and says its time: one more try with it.
  let offset = id.clock_offset ?? 0;
  let res!: Response;
  for (let attempt = 0; attempt < 2; attempt++) {
    const ts = localNow() + offset;
    const { publicKey, signature } = transport.nkeySign(seed, new TextEncoder().encode(`zebridge-renew:${publicKeyOf(seed, transport)}:${ts}`));
    const sig = btoa(String.fromCharCode(...signature)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    try {
      res = await fetch(`${id.bridge_url}/renew?user_pubkey=${publicKey}&ts=${ts}&sig=${sig}`);
    } catch (e) {
      throw new Error(`renew: ${id.bridge_url} unreachable (${(e as Error).message})`);
    }
    if (attempt > 0 || res.status !== 401) break;
    const body = await res.clone().json().catch(() => ({})) as { server_time?: number };
    if (typeof body.server_time !== 'number') break;
    offset = body.server_time - localNow();
  }
  if (res.status === 403) {
    // §10kn: `bridge --revoke --purge` — the answer says so, and the caller deletes the
    // device's replica and identity.
    const body = await res.json().catch(() => ({})) as { purge?: boolean };
    if (body.purge) throw new RevokedPurge();
    throw new Error('renew: refused — the key is revoked or no membership is left: a new invite is needed');
  }
  if (!res.ok) throw new Error(`renew: ${id.bridge_url} answered ${res.status}: ${(await res.text()).slice(0, 160)}`);
  const p = await res.json() as Record<string, string | undefined>;
  if (!p.jwt) throw new Error('renew: the bridge\'s answer has no jwt');
  return {
    version: 1, bridge_url: id.bridge_url, principal: p.principal ?? id.principal, creds: credsFileText(p.jwt, seed),
    clock_offset: offsetFrom(credsFileText(p.jwt, seed)),
    ...endpointIdentity(p),
    ...(p.grammar_hash ? { grammar_hash: p.grammar_hash } : {}),
    ...(p.js_domain ? { js_domain: p.js_domain } : {}),
  };
}

function publicKeyOf(seed: string, transport: Transport): string {
  return transport.nkeySign(seed, new Uint8Array(0)).publicKey;
}

export function credsFileText(jwt: string, seed: string): string {
  // The layout `nsc` writes and `bridge --init-nats` emits, byte for byte — libzb's
  // `zb_creds_file_text` produces the same. The warning block is not decoration: this
  // text is what a dev looks at when wondering whether the file is a secret.
  return `-----BEGIN NATS USER JWT-----\n${jwt}\n------END NATS USER JWT------\n` +
         `\n************************* IMPORTANT *************************\n` +
         `NKEY Seed printed below can be used to sign and prove identity.\n` +
         `NKEYs are sensitive and should be treated as secrets.\n\n` +
         `-----BEGIN USER NKEY SEED-----\n${seed}\n------END USER NKEY SEED------\n` +
         `\n*************************************************************\n`;
}

/// The JWT's own name claim — the creds are AUTHORITATIVE for the principal:
/// the payload is plain base64 (only the signature is cryptographic), and the
/// server expands permissions from THIS name, never from what the app believes.
export function principalFromCreds(creds: string): string | null {
  const m = creds.match(/BEGIN NATS USER JWT-+\s*\n([^\n]+)/);
  if (!m) return null;
  try {
    const payload = m[1].split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
    return JSON.parse(atob(payload)).name ?? null;
  } catch { return null; }
}

/// The version an outbox entry carries, dug out of the envelope it stored.
///
/// The payload is the mutation envelope as JSON (`{version, key, data}`), so the
/// version is a field of it rather than a column — the outbox table deliberately does
/// not duplicate it, and a copy would be one more thing to drift.
function outboxVersionOf(row: { payload?: string }): string | null {
  try {
    const v = JSON.parse(row.payload ?? '').version;
    return typeof v === 'string' && v.length ? v : null;
  } catch {
    return null;
  }
}


/// §10jc: chunks asked for per object pull. nats.js takes ONE bound per request, count or
/// bytes; a chunk is at most 128 KiB, so 16 is 2 MiB, libzb's bound (a byte bound sized
/// from the payload refused full chunks: their wire size counts the headers too). At 64
/// (8 MiB) the moto e20's JS thread, busy applying, left a WebSocket undrained for 10 s and
/// nats-server cut it ("WriteDeadline of 10s exceeded with 145 chunks of 8266295 bytes").
const OBJECT_PULL_CHUNKS = 16;

export class ZeBridge {
  /// The replica's name. With an invite and nothing given, known only once `connect()`
  /// has enrolled (the default is per principal).
  public dbName: string;
  public sql: StorageExec = async () => { throw new Error('Not connected'); };
  public transaction: Storage['transaction'] = async () => { throw new Error('Not connected'); };
  public deleteDatabaseFile: Storage['deleteDatabaseFile'] = async () => { throw new Error('Not connected'); };

  /// The JetStream options every transport factory receives: the domain, when the
  /// deployment has one (`config.jsDomain`), else nothing — the seam's default.
  private jsOpts(): JetStreamOpts | undefined {
    return this.config.jsDomain ? { domain: this.config.jsDomain } : undefined;
  }

  private nc: TransportConnection | null = null;
  private storage!: Storage;
  /// What the replica engine speaks (dialect.ts) — SQLite unless the adapter says otherwise.
  private dialect!: Dialect;

  private syncedTables = new Map<string, TableState>();
  /// §10hn: the on-demand tables (deduplicated); cdcFilters, the seed planner and the
  /// epoch re-seed all skip these.
  private ondemandSet = new Set<string>();
  /// ZB_MEASURE (measure.ts): the seeding report's lines, off unless asked for.
  private measure = new Measure((line) => this.appendLog('SYS', line, 'INFO'));
  /// §10kj: per CDC stream, `num_pending` of the last message applied (memory only).
  private lastPending: Record<string, number> = {};
  private warnedNoTables = false;
  /// §10go: how long to wait before re-opening a tail on a stream whose gap could not be
  /// healed (no chain past the hole yet). Doubles while it stays blocked, cleared once healed.
  private gapBackoffMs = 0;
  private failed = new Set<string>();
  private suspendedMap = new Map<string, string>();
  private globalSyncState: { lsn: number; seq: Record<string, number> } = { lsn: 0, seq: {} };
  /// §10jc: per stream, the generation of the ONE live tail. `subscribeStreams` runs on a
  /// reconnect, a status-loop restart, an RTT recovery and after a hole, and each run opened
  /// a new tail while the previous one kept going — measured on an iPhone: two tails on one
  /// stream after an RTT recovery, applying the same events and racing the position. A tail
  /// whose generation is no longer current stops at its next message, without flushing or
  /// acking what it holds; the current one re-reads that from the stored position.
  private tailGen = new Map<string, number>();
  /// Stop buffered work and wait for active flush/idle work before rewinding a route.
  private tailStops = new Map<string, () => Promise<void>>();
  /// The subject filters each live tail was opened with. A consumer's filters are fixed
  /// at creation, so a change in what a stream should carry needs a new tail.
  private tailFilters = new Map<string, string>();
  private streamsSubscribed = false;
  private streamWork: Promise<void> = Promise.resolve();
  /// The streams a fence has parked (fence.ts), each with the tables that hold the parking: the
  /// fence's own and every chain replacement that overlaps it. While a route has an owner no pass
  /// opens a tail on it: a tail would advance the cursor the fence moved back to the chain's cut,
  /// or apply a live event to rows another chain is about to replace. The parking ends with its
  /// LAST owner; an owner whose table is no longer followed is stale and ignored.
  private fenced = new Map<string, Set<string>>();
  /// The chain replacements in flight (a fence's, or a plain seed's) per route. A fence that
  /// commits hands the parking to the ones still running instead of releasing the route.
  private replacing = new Map<string, Set<string>>();
  /// The unfinished joint fences of a previous run were parked again (ensureJointRestored).
  private jointRestored = false;
  /// The applied frontiers (fence.ts), per table: from `_zebridge_frontier`, only those taken on
  /// the stream identity the stored position carries now. Written by a fence for the tables it
  /// leaves untouched, spent as the position passes them, gone when the table is replaced.
  private frontiers = new Map<string, Frontier[]>();
  /// Events the frontier gate dropped (the log says it when a frontier is spent).
  private frontierGated = 0;

  /// Consumer setup and reconciliation rewinds must not race each other's positions.
  private changeStreams(fn: () => Promise<void>): Promise<void> {
    const work = this.streamWork.then(fn);
    this.streamWork = work.catch(() => {});
    return work;
  }

  private async stopTail(stream: string): Promise<void> {
    this.tailGen.set(stream, (this.tailGen.get(stream) ?? 0) + 1);
    const stop = this.tailStops.get(stream);
    if (stop) {
      await stop();
      if (this.tailStops.get(stream) === stop) this.tailStops.delete(stream);
    }
  }

  /// `version` is the stamp the write carries: the CDC echo that confirms it is the
  /// row bearing THAT stamp, not any row on the same key (§10dt).
  private pendingWrites = new Map<string, { table: string; id: number | string; at: number; version?: string | null }>();
  /// §10fk: a `rate_limited` verdict names `retry_after_ms`; the outbox is not flushed before then.
  private holdUntil = 0;
  private nextOutboxRetry = 0;
  private outboxWork: Promise<void> | null = null;
  private outboxWake = false;
  private outboxReconnect = 0;
  private outboxRetryEpoch = 0;
  /// Writes settled by their CDC echo whose verdict has not come yet (`settleByEcho`).
  private echoedWrites?: Map<string, { version: string; table: string; key: any; columns: string[] }>;
  /// IDs whose outcome was already reported in this session (`firstReport`).
  private verdictReported?: Set<string>;
  /// IDs whose verdict is being handled right now (`handleVerdict`).
  private verdictInFlight?: Set<string>;
  /// Per key: the confirmed row beneath my pending overlays, kept after a decision while other
  /// edits on the key depend on it (`keepConfirmed`); forgotten when an authoritative row arrives.
  private confirmedBase?: Map<string, { row: Record<string, unknown>; at: number }>;
  private readonly outboxWindow = 128;
  /// §10do: UPDATEs judged `stale` whose columns may still be rebased onto the
  /// winning row. Held until that row is here (it may already be), then either
  /// resubmitted with a fresh stamp or dropped and surfaced.
  private rebase = new Map<string, { table: string; key: Record<string, unknown>; values: Record<string, unknown>; before: Record<string, unknown> | null; basis?: Record<string, unknown> | null; rowId?: string; createdAt?: number; version: string; sentVersion: string; at: number }>();
  private rebaseTimer: ReturnType<typeof setTimeout> | null = null;
  /// Streams the gap rule found RESTARTED (position beyond last_seq, §10bm's third
  /// shape) and no manifest has re-anchored on since. A manifest whose cutoff_seq is
  /// beyond such a stream's last_seq was cut on the previous numbering: it must not
  /// gate, or every event of the recreated stream is dropped as "in the chain"
  /// (measured in libzb, stream_wipe.py 2026-09-17: replica 1 row, PostgreSQL 13).
  private restarted = new Set<string>();
  /// The `created` timestamp of each stream this replica holds a position on: a
  /// different one under the same name is a recreated stream — a restart whatever the
  /// sequence numbers say (`stored > last_seq` misses it once the new stream is longer).
  private streamCreated = new Map<string, string>();
  /// The streams whose identity this replica recorded while they had never held a message (core.streamHasGap's
  /// `born_empty`): a position of 0 on one of them that still is empty is not a gap. Cleared by every restart.
  private streamBornEmpty = new Set<string>();

  private tenantValue = '';
  /// Per instance, like the replica (see App.tsx's CLIENT_ID history): the LWW
  /// tiebreaker and msg-id prefix. Must move INTO the replica when durable outboxes
  /// need identity to survive a reload (NOTES.md §1.9).
  private clientIdValue = `c-${crypto.randomUUID().slice(0, 8)}`;
  private readonly transport: Transport;
  private lastVersion = '';
  /// The HLC floor (§10q): the newest version this client has OBSERVED —
  /// CDC events' version column, chain cutoff_version. newVersion() stamps
  /// strictly above it, so a slow clock cannot lose to a row already seen.
  private hlcFloor = '';
  /// §10hc: what the bulk CDC path did so far — events bulked / through applyEvent, statements, fallbacks.
  bulkStats = { bulked: 0, single: 0, statements: 0, fallbacks: 0 };

  private outboxInitPromise: Promise<void>;
  private resolveOutboxInit!: () => void;
  private resyncing = false;
  /// Reconciliation of `indeterminate` writes (PROTOCOL §7.4c): routes rewound for an
  /// authoritative reseed, seeded from the hub only until healed.
  private hubOnly = new Set<string>();
  /// Tables whose chain is being applied by the delta path's choice: applyGenerationsFrom
  /// checks the manifest it reads against core.deltaEligible again before any row changes.
  private applyingByChoice = new Set<string>();
  private healedRoutes = new Set<string>();
  private reconcileLoaded = false;
  private reconcileBusy = false;
  private naturallyConnected = true;

  private tableListeners: Record<string, Set<(ev?: any) => void>> = {};
  private eventListeners = new Set<(table: string, ev: any) => void>();
  private anyChangeListeners = new Set<() => void>();
  private logHandlers = new Set<(topic: string, data: any, level: string) => void>();
  private verdictHandlers = new Set<(v: Verdict) => void>();
  private phaseHandlers = new Set<(p: Phase) => void>();
  private suspendedHandlers = new Set<(table: string, reason: string | null) => void>();
  private statusHandlers = new Set<(s: ConnStatus) => void>();

  private fkHeld: { id?: number; table: string; ev: any }[] = [];
  /// §10dm: the ban was seen (`mutation_ack.<principal>.revoked`) — closed, and staying
  /// closed. The rows stay; the wipe is the application's explicit `wipe()`.
  public revoked = false;
  /// §10kn: the revocation asked for a purge (`bridge --revoke --purge`), and this
  /// client deleted its replica and forgot its identity.
  public purged = false;
  private sweepId?: ReturnType<typeof setInterval>;
  private rttIntervalId?: ReturnType<typeof setInterval>;
  private hbIntervalId?: ReturnType<typeof setInterval>;
  private recountTimer?: ReturnType<typeof setTimeout>;

  private config: ZeBridgeConfig;
  private platform: Platform;

  constructor(config: ZeBridgeConfig) {
    this.config = config;
    config.grammar = GRAMMAR;
    this.platform = currentPlatform();
    // The core's rules (mergeRegisters, …) are WASM: start loading now, so a page that
    // edits before connect() has finished finds it ready; connect() awaits the same load.
    void loadCore(this.platform.coreWasm()).catch(() => { /* connect() retries and says why */ });
    if (config.platform && config.platform !== this.platform.name) {
      throw new Error(`zb-client-ts: platform '${config.platform}' asked, but this build loaded the '${this.platform.name}' entry — import from '@zebridge/client/${config.platform}'`);
    }
    if (!config.creds && config.credsPath) {
      if (!this.platform.readText) throw new Error(`zb-client-ts: credsPath needs a filesystem; on ${this.platform.name} pass the creds text as \`creds\``);
      config.creds = this.platform.readText(config.credsPath);
    }
    // The creds win over the passed principal — kills the mismatch class where
    // config says bob but the JWT says omar (every publish would just bounce).
    if (config.creds) {
      const fromJwt = principalFromCreds(config.creds);
      if (fromJwt && fromJwt !== config.principal) config.principal = fromJwt;
    }
    if (config.clientId) this.clientIdValue = config.clientId;
    // The same default as libzb: one replica per principal, kept across runs. Without a
    // principal yet (an invite, §10jq) the name is settled by connect().
    this.dbName = config.dbPath ?? (config.principal ? `zebridge_${config.principal}.sqlite3` : '');
    
    this.transport = config.transport ?? natsTransport;
    this.outboxInitPromise = new Promise((resolve) => { this.resolveOutboxInit = resolve; });
  }

  /// §10jq: who this client is, before anything opens. Explicit `creds` win; else the
  /// stored identity; else `bridgeUrl` + `invite` enroll and store one. What the identity
  /// knows fills every option the app left out — the same rule as libzb's.
  private identityResolved = false;
  /// §10jt: the stored identity this client runs on (null: explicit creds) and its key,
  /// for the renewal timer.
  private identityKey: string | null = null;
  private identityNow: EnrolledIdentity | null = null;
  private renewTimer: ReturnType<typeof setTimeout> | null = null;

  /// §10jt: every eighth of the JWT's life (at most a minute), renew when due; the new
  /// creds reach the NEXT handshake through the authenticator's function, so the
  /// connection is not rebuilt — the server ends it at the old JWT's expiry.
  private scheduleRenew(): void {
    if (!this.identityNow) return;
    const t = jwtTimes(this.identityNow.creds);
    const every = Math.min(60, Math.max(1, Math.floor(((t?.exp ?? 480) - (t?.iat ?? 0)) / 8)));
    this.renewTimer = setTimeout(async () => {
      if (await this.renewNow(false) === 'purged') return; // nothing left to renew
      this.scheduleRenew();
    }, every * 1000);
  }

  /// Renew when due in the bridge's time, or now (`force`): NATS refused the JWT, so the
  /// device's estimate of the bridge's time may be wrong, and a renewal corrects it.
  private async renewNow(force: boolean): Promise<'renewed' | 'skipped' | 'failed' | 'purged'> {
    const id = this.identityNow;
    if (!id || (!force && !renewDue(id.creds, serverNow(id)))) return 'skipped';
    try {
      const fresh = await renewAt(id, this.transport);
      this.identityNow = fresh;
      this.config.creds = fresh.creds;
      if (this.identityKey) await this.platform.identity?.save(this.identityKey, JSON.stringify(fresh));
      this.appendLog('SYS', `Renewed '${fresh.principal}' — the next reconnect presents the new JWT`, 'INFO');
      return 'renewed';
    } catch (e) {
      if (e instanceof RevokedPurge) {
        await this.purgeLocal();
        return 'purged';
      }
      this.appendLog('SYS', `${(e as Error).message} — retried shortly`, 'WARN');
      return 'failed';
    }
  }
  private async resolveIdentity(): Promise<void> {
    if (this.identityResolved) return;
    const c = this.config;
    if (!c.creds) {
      const key = c.identityPath ?? (c.dbPath ? `${c.dbPath}.identity` : 'zebridge.identity');
      const store = this.platform.identity;
      let text = store ? await store.load(key) : null;
      // A stored identity wins over an invite (a bookmarked invite link must not spend a
      // code on every visit) — when it can be used. One that cannot (unreadable, or from
      // a deployment that named no NATS URL) would only fail below: with an invite given,
      // enroll instead and replace it.
      if (text && c.invite && !identityUsable(text, c.natsUrl, this.platform.natsOverWebSocket === true)) {
        this.appendLog('SYS', 'the stored identity cannot be used (no creds, or no NATS URL): enrolling with the invite instead', 'WARN');
        text = null;
      }
      if (!text && c.invite) {
        if (!c.bridgeUrl) throw new Error('zb-client-ts: an invite needs `bridgeUrl`: the bridge that redeems it');
        const id = await enrollAt(c.bridgeUrl, c.invite, this.transport);
        text = JSON.stringify(id);
        if (store) await store.save(key, text);
        else this.appendLog('SYS', `enrolled as '${id.principal}', but ${this.platform.name} has no identity store: the next run needs creds`, 'WARN');
        this.appendLog('SYS', `Enrolled as '${id.principal}' at ${id.bridge_url}`, 'INFO');
      }
      if (text) {
        let id = JSON.parse(text) as EnrolledIdentity;
        // §10jt: close to (or past) expiry → renew now, with the key. Before expiry a
        // failure is a warning (the JWT still works); after it, it is the connect's error.
        if (renewDue(id.creds, serverNow(id))) {
          try {
            id = await renewAt(id, this.transport);
            if (store) await store.save(key, JSON.stringify(id));
            this.appendLog('SYS', `Renewed '${id.principal}' at ${id.bridge_url}`, 'INFO');
          } catch (e) {
            if (e instanceof RevokedPurge) {
              // §10kn: revoked with a purge — delete the replica and the identity, then
              // fail the connect: this device has nothing left to connect with.
              this.identityKey = key;
              await this.purgeLocal();
              throw e;
            }
            const t = jwtTimes(id.creds);
            if (!t || t.exp <= serverNow(id)) throw e;
            this.appendLog('SYS', `${(e as Error).message} — carrying on with the current JWT`, 'WARN');
          }
        }
        this.identityKey = key;
        this.identityNow = id;
        c.creds = id.creds;
        c.principal = principalFromCreds(id.creds) ?? id.principal;
        c.natsUrl ??= identityEndpointUrl(id, !!this.platform.natsOverWebSocket);
        c.grammarHash ??= id.grammar_hash ?? undefined;
        c.jsDomain ??= id.js_domain ?? undefined;
        c.bridgeUrl ??= id.bridge_url;
      }
    }
    if (!c.natsUrl && !c.creds && !this.identityNow) throw new NotEnrolled();
    if (!c.natsUrl) throw new Error('zb-client-ts: no `natsUrl`, and no identity that names one (the bridge sets ENROLL_NATS_URL / ENROLL_NATS_WS_URL)');
    if (!c.principal) throw new Error('zb-client-ts: no principal: pass `creds`, an `invite` with `bridgeUrl`, or `principal` + `password`');
    if (!this.dbName) (this as { dbName: string }).dbName = `zebridge_${c.principal}.sqlite3`;
    this.identityResolved = true;
  }

  private async initializeStorage() {
    if (this.storage) { this.storage.reacquire?.(); return; } // already initialized (close() gave the replica lock up)
    if (!this.dbName) throw new Error('zb-client-ts: the replica is named after the principal, which enrollment gives — call connect() first');
    
    const factory = this.config.storage ?? await this.platform.storage({ dbPath: this.dbName, engine: this.config.engine });

    this.storage = factory(this.dbName);
    this.dialect = this.storage.dialect ?? sqliteDialect;
    this.sql = this.storage.exec;
    this.transaction = (fn) => this.storage.transaction(fn);
    this.deleteDatabaseFile = () => this.storage.deleteDatabaseFile();
  }

  // ─── the index card ───────────────────────────────────────────────────────

  /// SQL against the local replica — READS ONLY (§10di). Writes go through
  /// mutate(); the replica's data is the feed's, and its bookkeeping (the outbox,
  /// the positions, the shape record) is the core's. On Node the statement runs on a
  /// second connection the engine opened read-only; where the adapter has one
  /// handle (OPFS, PGlite) the statement must read by shape (core.isReadOnlySql),
  /// and anything else is refused before it runs.
  public async query(sqlText: string, ...params: any[]): Promise<any[]> {
    await this.initializeStorage();
    // The shape rule runs on EVERY path: a read-only connection still lets a
    // connection-local pragma "succeed" and drops a second statement in silence.
    if (!isReadOnlySql(sqlText)) {
      throw new Error(`query() is read-only: this statement writes (or is not a single SELECT/PRAGMA read) — use mutate() for writes`);
    }
    if (this.storage.readOnly) return this.storage.readOnly(sqlText, ...params);
    return this.run(sqlText, ...params);
  }

  /// The blessed write path: a 1:1 constructor for the wire message
  /// `mutation.<principal>.<table>.<verb>` — no SQL is ever parsed. Returns the
  /// version stamped on the write so callers can mirror it into value columns.
  public async mutate(
    table: string,
    opIn: string,
    key: Record<string, unknown>,
    values?: Record<string, unknown>,
    opts?: { version?: string },
  ): Promise<{ version: string }> {
    const op = normalizeOp(opIn); // §10jm: any case in, capitals from here on
    await this.initializeStorage();
    const state = this.syncedTables.get(table);
    if (!state || !state.pkCols.length) throw new Error(`table ${table} is not synced or has no primary key`);
    const version = opts?.version ?? this.newVersion();
    const id = mutationKeyId(state.pkCols, key);
    const payload = mutationPayload(op, key, values, version, this.clientIdValue);
    await this.rawMutation(table, op, id, version, payload);
    return { version };
  }

  /// §10hp: BE a service. Answer `query.<tenant>.<name>` for these tenants, in one
  /// queue group, on this client's own connection — the same verb libzb has, in this
  /// library's idiom: the handlers are yours, the subscribing, the dispatch, the reply
  /// and the error envelope are the library's.
  ///
  /// A handler receives the asker's parsed payload and the tenant the question arrived
  /// on (from the subject, never the payload), and returns the answer, sent as JSON. Throwing is allowed: the asker gets `{"error": …}` rather than a
  /// timeout, which is the difference between a service that says no and one that
  /// looks dead. Returns how many subjects this client now answers.
  public async serve(opts: {
    tenants: string[];
    handlers: Record<string, (payload: any, tenant: string) => unknown | Promise<unknown>>;
    queue?: string;
  }): Promise<number> {
    if (!this.nc) throw new Error('not connected');
    const prefix = this.config.grammar?.subjects?.query_prefix ?? 'query';
    const queue = opts.queue ?? 'zb';
    let n = 0;
    for (const tenant of opts.tenants) {
      for (const [name, fn] of Object.entries(opts.handlers)) {
        const subject = `${prefix}.${tenant}.${name}`;
        const sub = this.nc.subscribe(subject, { queue });
        n += 1;
        void (async () => {
          for await (const m of sub as AsyncIterable<any>) {
            if (!m.reply) continue;   // nobody is waiting: nothing to answer
            let answer: unknown;
            try {
              const raw = new TextDecoder().decode(m.data);
              answer = await fn(raw ? JSON.parse(raw) : {}, tenant);
            } catch (e) {
              answer = { error: `${(e as Error)?.name ?? 'Error'}: ${(e as Error)?.message ?? e}` };
            }
            try {
              const body = new TextEncoder().encode(JSON.stringify(answer));
              m.respond(await this.answerBody(tenant, body));
            } catch (e) { this.appendLog('SYS', `${subject}: reply failed: ${e}`, 'ERROR'); }
          }
        })();
      }
    }
    this.appendLog('SYS', `serving ${Object.keys(opts.handlers).sort().join(', ')} for ${opts.tenants.join(', ')} in queue group '${queue}' (${n} subject(s))`, 'INFO');
    return n;
  }

  /// §10hq: an answer that fits goes as it is; one that does not becomes an OBJECT in
  /// the asking tenant's bucket, and what travels is a small envelope naming it. NATS
  /// caps a message near a megabyte, and a service that answers rows cannot promise to
  /// stay under it. The asking library resolves the envelope, so the host never sees
  /// the difference.
  private async answerBody(tenant: string, raw: Uint8Array): Promise<Uint8Array> {
    const results = this.config.grammar?.results ?? {};
    const inlineMax: number = results.inline_max_bytes ?? 262_144;
    // §10ip: compressed FIRST, so inline-or-object is decided on what travels. Measured
    // on this stack answers compress about 4x, which drops a 621 KB answer to 163 KB —
    // under the limit, so it goes inline and the whole object round trip disappears.
    const body = await this.compressAnswer(raw);
    if (body.length <= inlineMax) return body;
    const bucket = `${results.bucket_prefix ?? 'res-'}${tenant}`;
    const maxAgeNs = (results.max_age_seconds ?? 600) * 1_000_000_000;
    let os: any;
    try { os = await this.transport.objectStoreCreate(this.nc!, bucket, { max_age_ns: maxAgeNs }, this.jsOpts()); }
    catch { os = await this.transport.objectStore(this.nc!, bucket, this.jsOpts()); }
    const name = `ans-${this.clientIdValue}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    await os.putBlob({ name }, body);
    return new TextEncoder().encode(JSON.stringify({ zb_object: { bucket, name, bytes: body.length } }));
  }

  /// The answer as it should travel: a zstd frame when this host can make one, the
  /// bytes untouched otherwise. Never fails an answer over compression, and never
  /// returns something LARGER than it was given.
  ///
  /// A plain frame, like every chain object since 2026-09-24 — what every client reads.
  private async compressAnswer(b: Uint8Array): Promise<Uint8Array> {
    if (b.length < 512) return b;           // a frame header would be most of it
    try {
      let out: Uint8Array | null = null;
      const compress = this.config.zstdCompress ?? this.platform.zstdCompress;
      if (compress) out = await compress(b);
      return out && out.length < b.length ? out : b;
    } catch {
      return b;   // a host without a compressor answers uncompressed; every asker reads both
    }
  }

  /// §10hq: `{"zb_object":{bucket,name,bytes}}` → the object's bytes, parsed; anything
  /// else → null, so a service may legitimately answer a body with such a key.
  private async resolveAnswer(body: Uint8Array): Promise<{ value: any; bytes: number } | null> {
    let parsed: any;
    try { parsed = JSON.parse(new TextDecoder().decode(body)); } catch { return null; }
    const env = parsed?.zb_object;
    if (!env?.bucket || !env?.name) return null;
    const os = await this.transport.objectStore(this.nc!, env.bucket, this.jsOpts());
    const blob = await this.objectBlob(os, env.bucket, env.name);
    if (!blob) throw new Error(`answer object ${env.name} is gone from ${env.bucket} (expired?)`);
    // §10ip: `maybeZstd` hands back its input untouched when the bytes are not a frame.
    const inflated = await this.maybeZstd(blob);
    return { value: JSON.parse(new TextDecoder().decode(inflated)), bytes: blob.length };
  }

  /// §10hn (libzb §10hj): ask a service — request/reply on a subject this principal may
  /// publish to (`query.<tenant>.<name>`), the answer as parsed JSON. No stream, no
  /// position: what a service answers is its own contract.
  public async request(subject: string, payload: unknown, timeoutMs = 5_000): Promise<any> {
    if (!this.nc) throw new Error('not connected');
    const t0 = Date.now();
    const parts = subject.split('.');
    const isQuery = parts[0] === (this.config.grammar?.subjects?.query_prefix ?? 'query');
    if (isQuery && (parts.length < 3 || !parts[1] || /[* >\s]/.test(parts[1]))) throw new Error('malformed query subject');
    const reply = isQuery ? `_INBOX.${this.config.principal}.query.${parts[1]}.${crypto.randomUUID()}` : undefined;
    const m = await this.nc.request(subject, new TextEncoder().encode(JSON.stringify(payload ?? {})), {
      timeout: timeoutMs, ...(reply ? { noMux: true, reply } : {}),
    });
    const wire_ms = Date.now() - t0;
    // §10hq: an answer too large for one message arrives as an envelope naming an object.
    const t1 = Date.now();
    const large = await this.resolveAnswer(m.data);
    const fetch_ms = large ? Date.now() - t1 : 0;
    const value = large ? large.value : JSON.parse(new TextDecoder().decode(await this.maybeZstd(m.data)));
    // §10hu: say HOW the answer travelled, on the same clock either way. libzb splices
    // the identical block (`spliceTransport`), so a host reads one shape from either
    // library and can price the object fetch instead of guessing at it.
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      value.zb_transport = {
        via: large ? 'object' : 'inline',
        bytes: large ? large.bytes : m.data.length,
        wire_ms,
        fetch_ms,
      };
    }
    return value;
  }

  /// §10hn (libzb §10hj): keep an answer in a table this client holds — rows in the
  /// chain object's shape (`columns`, `rows`), applied through the chain's
  /// version-guarded upsert so a stale answer never overwrites a newer row. With a
  /// `scope`, the rows of the table inside the scope's WHERE that the answer did not
  /// carry are deleted: the area the answer is complete for. Returns the rows applied.
  public async ingest(
    table: string,
    answer: { columns: string[]; rows: any[][] },
    scope?: { where: string; params?: any[] } | null,
  ): Promise<number> {
    await this.initializeStorage();
    const state = this.syncedTables.get(table);
    if (!state) throw new Error(`table ${table} is not held`);
    const cols = answer.columns ?? [];
    const rows = answer.rows ?? [];
    for (const c of cols) if (!state.columns.includes(c)) throw new Error(`schema behind: ${table} has no column ${c}`);
    const vcol = state.versionColumn && cols.includes(state.versionColumn) ? state.versionColumn : null;
    const q = chainUpsertSql(table, cols, state.pkCols, vcol);
    const pkIdx = state.pkCols.length === 1 ? cols.indexOf(state.pkCols[0]) : -1;
    const arrIdx = this.dialect.name === 'postgres' ? (state.arrayCols ?? []).map((c) => cols.indexOf(c)).filter((i) => i >= 0) : [];
    const vecIdx = this.dialect.name === 'postgres' ? (state.vecCols ?? []).map((vc) => ({ i: cols.indexOf(vc.name), vc })).filter((x) => x.i >= 0) : [];
    let applied = 0;
    const seen: any[] = [];
    await this.transaction(async (txExec) => {
      for (const row of rows) {
        if (!Array.isArray(row)) continue;
        // A service answers in JSON, which has no bytes: a cell `{"$bin": "<base64>"}`
        // is a byte string (PROTOCOL §2; libzb's ingest reads the same marker).
        const params = chainRowParams(row.map(binMarker));
        for (const i of arrIdx) {
          const v = params[i];
          if (typeof v === 'string' && v.startsWith('[')) { try { params[i] = pgArrayLiteral(JSON.parse(v)); } catch { /* not JSON: as is */ } }
        }
        for (const { i, vc } of vecIdx) if (isBytes(params[i])) params[i] = vecLiteral(vc.kind, params[i], vc.bits);
        await txExec(q, ...params);
        if (pkIdx >= 0) seen.push(params[pkIdx]);
        applied++;
      }
      // The scope: what the answer did not carry inside it is gone.
      if (scope?.where && pkIdx >= 0) {
        await txExec(`CREATE TEMP TABLE IF NOT EXISTS _zbz_seen (k${this.dialect.name === 'postgres' ? ' text' : ''})`);
        await txExec(`DELETE FROM _zbz_seen`);
        for (const k of seen) await txExec(`INSERT INTO _zbz_seen (k) VALUES (?)`, k);
        await txExec(`DELETE FROM ${table} WHERE (${scope.where}) AND "${state.pkCols[0]}" NOT IN (SELECT k FROM _zbz_seen)`, ...(scope.params ?? []));
        await txExec(`DELETE FROM _zbz_seen`);
      }
    });
    this.triggerChange(table);
    return applied;
  }

  public onChange(table: string, cb: (ev?: any) => void): () => void {
    if (!this.tableListeners[table]) this.tableListeners[table] = new Set();
    this.tableListeners[table].add(cb);
    return () => this.tableListeners[table].delete(cb);
  }

  // ─── the rest of the public surface ──────────────────────────────────────

  /// Every applied event, with its table — the hook for verb badges and per-table
  /// console logging. High-volume: keep handlers cheap.
  public onTableEvent(cb: (table: string, ev: any) => void): () => void {
    this.eventListeners.add(cb);
    return () => this.eventListeners.delete(cb);
  }

  /// Debounced "something changed somewhere" (250ms) — the recount trigger.
  public onAnyChange(cb: () => void): () => void {
    this.anyChangeListeners.add(cb);
    return () => this.anyChangeListeners.delete(cb);
  }

  public onLog(cb: (topic: string, data: any, level: string) => void): () => void {
    this.logHandlers.add(cb);
    return () => this.logHandlers.delete(cb);
  }

  /// The final outcome of each of this client's writes (`Verdict`), matched to `mutate()`
  /// by `version`. `onLog` says the same things in words, for people; this is for code.
  public onVerdict(cb: (v: Verdict) => void): () => void {
    this.verdictHandlers.add(cb);
    return () => this.verdictHandlers.delete(cb);
  }

  /// Writes still in the outbox: applied here, not yet judged by PostgreSQL (offline, in
  /// flight, or kept for retry).
  public async pending(): Promise<number> {
    await this.outboxInitPromise;
    try { return Number((await this.run(`SELECT count(*) AS n FROM _zebridge_outbox`))[0]?.n ?? 0); } catch { return 0; }
  }

  private emitVerdict(v: Verdict) {
    for (const cb of this.verdictHandlers) { try { cb(v); } catch { /* a host's listener must not break the verdict path */ } }
  }

  /// What a verdict reports about its write, from the outbox row (read before the row goes).
  /// Null when the write is not this client's: verdicts travel on the PRINCIPAL's subject,
  /// so another device of the same principal hears them too, and reports them as its own
  /// only if its outbox holds the write (or this session sent it).
  private async outboxWrite(msgId: string, fallback: { table: string; version?: string | null } | undefined) {
    await this.outboxInitPromise;
    let row: any;
    try { row = (await this.run(`SELECT tbl, payload FROM _zebridge_outbox WHERE msg_id = ?`, msgId))[0]; } catch { /* none */ }
    if (!row) {
      // Settled by its echo before its verdict: the description kept at the echo, read once.
      const kept = this.echoedWrites?.get(msgId);
      if (kept) { this.echoedWrites!.delete(msgId); return kept; }
    }
    if (!row && !fallback) return null;
    let sent: any = {};
    try { sent = row ? JSON.parse(row.payload) : {}; } catch { /* unreadable */ }
    const table = String(row?.tbl ?? fallback?.table ?? '?');
    const vcol = this.syncedTables.get(table)?.versionColumn;
    const data = sent?.data && typeof sent.data === 'object' ? sent.data : {};
    return {
      version: String(sent?.version ?? fallback?.version ?? ''),
      table,
      key: sent?.key && typeof sent.key === 'object' ? sent.key : {},
      columns: Object.keys(data).filter((c) => c !== vcol),
    };
  }

  public onPhase(cb: (p: Phase) => void): () => void {
    this.phaseHandlers.add(cb);
    return () => this.phaseHandlers.delete(cb);
  }

  private seedProgressHandlers = new Set<(p: SeedProgress) => void>();
  /// §10ix: progress of a seed, window by window — what a host shows so a legitimate
  /// minute-long seed does not look like a hang (it did, twice, before this existed).
  /// Fires on every path: buffered, streamed per window, streamed and staged.
  public onSeedProgress(cb: (p: SeedProgress) => void): () => void {
    this.seedProgressHandlers.add(cb);
    return () => this.seedProgressHandlers.delete(cb);
  }
  private seedProgress(p: SeedProgress) {
    for (const cb of this.seedProgressHandlers) { try { cb(p); } catch { /* a host's listener must not break the seed */ } }
  }

  public onSuspended(cb: (table: string, reason: string | null) => void): () => void {
    this.suspendedHandlers.add(cb);
    return () => this.suspendedHandlers.delete(cb);
  }

  public onStatus(cb: (s: ConnStatus) => void): () => void {
    this.statusHandlers.add(cb);
    return () => this.statusHandlers.delete(cb);
  }

  public uuid(): string { return uuidv7(); }
  public get tenant(): string { return this.tenantValue; }
  public get clientId(): string { return this.clientIdValue; }
  /// Which platform entry this client runs on, and its zstd decoder — for a log line.
  public get platformInfo(): { name: PlatformName; zstd: string } {
    return { name: this.platform.name, zstd: this.platform.zstdName() };
  }
  /// Events held back waiting for a schema newer than they are.
  public get heldCount(): number { return this.fkHeld.length; }
  /// Events held waiting for a PARENT ROW — a foreign key whose target has not
  /// arrived yet, which happens when one PostgreSQL transaction is split across
  /// batches. Non-zero for long is a real signal: the parent never came.
  public get fkHeldCount(): number { return this.fkHeld.length; }
  /// The durable hold queue, oldest first — what is waiting and for how long. A row
  /// with a high `attempts` is a parent that is never coming, which is a real
  /// condition worth seeing rather than expiring away.
  public async inbox(): Promise<any[]> {
    return this.run(`SELECT id, tbl, lsn, reason, held_at, attempts FROM _zebridge_inbox ORDER BY id`);
  }
  public tableNames(): string[] { return [...this.syncedTables.keys()]; }
  public tableState(table: string): TableState | undefined { return this.syncedTables.get(table); }
  public suspendedTables(): Record<string, string> { return Object.fromEntries(this.suspendedMap); }

  /// Debug snapshot: stored positions and per-table lsn — `zb.state()` in the console.
  public syncState() {
    return {
      global: { ...this.globalSyncState },
      tables: Object.fromEntries([...this.syncedTables].map(([t, v]) => [t, { lsn: v.lsn, columns: v.columns.length }])),
      failed: [...this.failed],
    };
  }

  /// Render "now" as a version value — the exact shape CDC echoes back for a
  /// timestamptz column (ISO, six digits, trailing Z), strictly increasing within
  /// this instance so same-millisecond edits never tie against themselves.
  public newVersion(): string {
    // core.hlcVersion (§10q): the wall clock floored by the newest version
    // seen arriving — a slow clock lifts to just past the observed floor, and
    // arrival time never becomes the comparator (that would punish offline).
    // The wall clock is the BRIDGE's, as this device estimates it (§10kr: the
    // identity's clock_offset), so a device whose clock is off stamps near true time.
    this.lastVersion = hlcVersion(new Date(Date.now() + this.clockOffsetMs()).toISOString(), this.lastVersion, this.hlcFloor);
    return this.lastVersion;
  }

  /// A register stamp (COOPERATIVE_EDITING.md, `t` of {v, t, w}): the same clock as a
  /// row version — the bridge's time as estimated here, never behind what this client
  /// has seen. A phone whose clock runs fast no longer wins a register race by its error.
  public stamp(): string {
    return this.newVersion();
  }

  /// Who this client is: the `w` of a register it writes.
  public get principal(): string { return this.config.principal ?? ''; }

  private clockOffsetMs(): number {
    return (this.identityNow?.clock_offset ?? 0) * 1000;
  }

  // ─── lifecycle ────────────────────────────────────────────────────────────

  public async connect(): Promise<void> {
    // libzb's core (wasm-core.ts): once per process, before any rule is asked.
    await loadCore(this.platform.coreWasm());
    await this.resolveIdentity();
    await this.initializeStorage();
    if (!this.config.grammarHash && this.config.bridgeUrl) {
      try {
        const r = await fetch(`${this.config.bridgeUrl.replace(/\/$/, '')}/grammar`, { signal: AbortSignal.timeout(3000) });
        if (r.ok) this.config.grammarHash = r.headers.get('x-grammar-hash') || undefined;
      } catch { /* ignore fetch failure, will just skip grammar check if grammarHash is undefined */ }
    }
    await this.initSyncState();
    await this.refuseIfGrammarForked();

    if (this.nc) {
      try { await this.nc.close(); } catch { /* already closed */ }
      this.nc = null;
    }

    try {
      this.emitStatus('connecting');
      this.appendLog('SYS', `Connecting to NATS at ${this.config.natsUrl}...`);

      const dial = this.config.connect ?? this.platform.connect ?? this.transport.connect;
      const opts = {
        servers: orderedEndpoints(this.config.natsUrl!),
        noRandomize: true,
        ignoreClusterUpdates: true,
        ...(this.config.creds
          ? { authenticator: this.transport.credsAuthenticator(() => new TextEncoder().encode(this.config.creds!)) }
          : { user: this.config.principal, pass: this.config.password }),
        reconnect: true,
        maxReconnectAttempts: -1,
        // §10hm: this principal's own inbox space, so a `_INBOX.<principal>.>`
        // grant covers every reply, watcher and pull this client opens.
        inboxPrefix: `_INBOX.${this.config.principal}`,
      };
      try {
        this.nc = await dial(opts);
      } catch (e) {
        // NATS refused the JWT: renew it once whatever the clock says, and dial again
        // (the authenticator reads the renewed creds).
        if (!/authorization violation|authentication expired/i.test(String((e as Error)?.message ?? e))) throw e;
        const r = await this.renewNow(true);
        if (r === 'purged') throw new RevokedPurge();
        if (r !== 'renewed') throw e;
        this.nc = await dial(opts);
      }
      // This call's own connection. `close()` (or `wipe()`) may run while the awaits
      // below are in flight — the seed can take minutes — and it nulls `this.nc`;
      // everything after them must then stop, not read a connection that is gone
      // ("cannot read property 'closed' of null", 2026-09-25, a wipe during connect).
      const nc = this.nc;
      this.nextOutboxRetry = performance.now() + Math.floor(Math.random() * 5000);

      this.emitStatus('connected');
      this.reach('connected');
      this.appendLog('SYS', `Connected to NATS as '${this.config.principal}'`);
      if (this.renewTimer) clearTimeout(this.renewTimer);
      this.scheduleRenew();
      await this.loadHeldEvents();
      // The tenant FIRST (§10dr): the schema watch replays every descriptor at once,
      // and a tenant-scoped table that arrives before the tenant is known is skipped
      // as "no tenant" — and never created. Measured: counter_tenant missing for
      // alice on the web page, note_t for the same creds in Node, by arrival order.
      await this.resolveTenant();
      await this.watchSchemas();
      await this.watchVerdicts();
      // §10dm: a ban published while this client was away is retained — one direct get.
      await this.collectMissedVerdicts([{ msg_id: 'revoked' }]);
      if (this.revoked) throw new Error(`'${this.config.principal}' is revoked`);
      // PROTOCOL §11: beat from HERE, before the seed — a client stuck seeding (a chain
      // that never comes, a slow device) is exactly the one an operator must see, with
      // its zero positions. libzb beats from its first poll for the same reason.
      const hb = this.config.heartbeatMs ?? 30_000;
      if (hb > 0) {
        this.hbIntervalId = setInterval(() => void this.sendHeartbeat(), hb);
        void this.sendHeartbeat();
      }
      // Writes answered `indeterminate` before a restart: their routes rewind before any
      // tail opens, and the subscribe below is their heal.
      try { await this.reconcileRewind(); } catch (e) { this.appendLog('SYS', `reconcile: ${e}`, 'WARNING'); }
      await this.subscribeStreams();
      if (this.nc !== nc) {
        this.appendLog('SYS', 'closed while connecting — this connect() stops here', 'INFO');
        return;
      }
      try {
        if (await this.reconcileClear()) setTimeout(() => void this.reconcile(), 2_000);
      } catch (e) { this.appendLog('SYS', `reconcile: ${e}`, 'WARNING'); }
      this.reach('cdc');

      // Start the durable write window, including entries from a previous page.
      // Verdicts clock subsequent windows; reconnect jitter only delays the start.
      void this.recoverOutbox();

      // `reconnect: true` restores the connection without coming back through this
      // function — the status loop is what re-syncs after a blip. Safe to re-run
      // subscribeStreams: the gap check compares persisted positions, so a normal
      // reconnect just resumes CDC (upserts idempotent even if consumers double up).
      void (async () => {
        // The status loop is the RE-SYNC trigger, so it must outlive any single
        // status() iterator: one client's loop died at the first broker bounce
        // (the iterator threw, the catch swallowed, the loop exited) and that
        // client never re-synced again — consumers born deaf stayed deaf while
        // eleven siblings healed three times (§10cq). Same rule as the tails:
        // recreate until close.
        while (this.nc) {
          try {
            for await (const st of this.nc.status()) {
              const t = String((st as any).type);
              if (t === 'reconnect') {
                this.naturallyConnected = true;
                this.outboxReconnect++;
                this.emitStatus('connected');
                // Spread fleet replay behind the reconnect control plane, without
                // delaying subsequent slots freed by verdicts.
                this.nextOutboxRetry = Math.max(this.nextOutboxRetry, performance.now() + Math.floor(Math.random() * 5000));
                if (this.resyncing) continue;
                this.resyncing = true;
                this.appendLog('SYS', 'NATS reconnected — scheduling outbox recovery and re-syncing streams', 'INFO');
                void this.subscribeStreams().catch(() => {}).finally(() => { this.resyncing = false; });
              } else if (t === 'disconnect') {
                this.naturallyConnected = false;
                this.emitStatus('disconnected');
              } else if (t === 'error') {
                // The server's own verdict (§10dl): `-ERR 'Authentication Revoked'`,
                // an authorization violation — the one line that says WHY the
                // connection is going, before every later call says only "closed".
                this.appendLog('SYS', `NATS server error: ${String((st as any).data ?? '')}`, 'ERROR');
                // The session ended on the JWT: renew now, whatever the clock says, so
                // the automatic reconnect presents a fresh one.
                if (/authentication expired/i.test(String((st as any).data ?? ''))) void this.renewNow(true);
              }
            }
          } catch { /* iterator died — recreate below */ }
          if (!this.nc) break;
          await new Promise((r) => setTimeout(r, 1000));
          this.appendLog('SYS', 'status loop restarted — re-syncing in case a reconnect was missed', 'WARNING');
          if (!this.resyncing) {
            this.resyncing = true;
            void this.subscribeStreams().catch(() => {}).finally(() => { this.resyncing = false; });
          }
        }
      })();

      // The terminal reason, named (§10dl): closed() resolves with the error that
      // ended the connection — an auth verdict reads as such, not as a bare close.
      const closedP: Promise<unknown> | undefined = (nc as any).closed?.();
      void closedP?.then((err: any) => {
        if (err) this.appendLog('SYS', `NATS connection closed by the server: ${err?.message ?? err}`, 'ERROR');
      });

      this.sweepId = setInterval(() => this.sweepPendingWrites(), 1000);
      this.rttIntervalId = setInterval(() => void this.pollNatsRtt(), 10_000);
    } catch (err) {
      this.emitStatus('disconnected');
      this.appendLog('SYS', `Connection failed: ${err}`, 'ERROR');
      throw err;
    }
  }

  /// Close AND delete the replica files (§10dl). Never automatic: a revoked principal's
  /// device keeps its rows and simply stops receiving; the wipe is the application's
  /// explicit act, and this is the one verb for it (libzb: `zb_client_wipe`).
  /// §10dq: the check that makes a fork loud. A client with valid creds and another
  /// grammar is the "impossible" case of §10ci — it would subscribe to streams that do
  /// not exist — so it is refused here, before a socket opens with the wrong names.
  private async refuseIfGrammarForked(): Promise<void> {
    const want = this.config.grammarHash?.trim().toLowerCase();
    if (!want) return;
    const have = await grammarHashHex();
    if (have === want) return;
    this.appendLog('SYS', `grammar mismatch: this library embeds ${have}, the bridge serves ${want} — built for another protocol; rebuild the client`, 'ERROR');
    this.emitStatus('disconnected');
    throw new Error(`GrammarMismatch: library ${have} vs bridge ${want}`);
  }

  public async wipe(): Promise<void> {
    await this.close();
    await this.deleteDatabaseFile();
  }

  /// §10kn: the operator revoked this principal with `--purge`. Delete the replica and
  /// forget the identity, so the device holds neither the data nor a key. Only on that
  /// instruction: a plain revocation still leaves the rows, and wipe() to the app.
  private async purgeLocal(): Promise<void> {
    if (this.purged) return;
    this.purged = true;
    this.revoked = true;
    try { await this.initializeStorage(); } catch { /* nothing opened, nothing to keep */ }
    try {
      await this.wipe();
    } catch (e) {
      this.appendLog('SYS', `purge: the replica could not be deleted (${(e as Error).message})`, 'ERROR');
    }
    const key = this.identityKey ?? this.identityKeyFor();
    try { await this.platform.identity?.save(key, ''); } catch { /* no store: nothing kept */ }
    this.identityNow = null;
    this.appendLog('SYS', `'${this.config.principal}' REVOKED with a purge by the operator — the local replica and identity are deleted`, 'ERROR');
    this.emitStatus('disconnected');
  }

  /// Where the identity is kept: `identityPath`, else `<dbPath>.identity`, else the default.
  private identityKeyFor(): string {
    const c = this.config;
    return c.identityPath ?? (c.dbPath ? `${c.dbPath}.identity` : 'zebridge.identity');
  }

  public async close(): Promise<void> {
    if (this.renewTimer) { clearTimeout(this.renewTimer); this.renewTimer = null; }
    clearInterval(this.sweepId);
    clearInterval(this.rttIntervalId);
    clearInterval(this.hbIntervalId);
    clearTimeout(this.recountTimer);
    clearTimeout(this.rebaseTimer ?? undefined);
    // `this.nc` is cleared BEFORE the close is awaited: every loop that asks `this.nc`
    // whether to go on (the status loop, the tails, a connect() in flight) must see the
    // close at once. Cleared after, the status loop saw its iterator end with the
    // connection still set, took it for a lost connection and re-synced a client being
    // closed — racing wipe()'s database delete, which never finished (2026-09-25, the
    // RN app's "wipe & seed again" stopped dead).
    const nc = this.nc;
    this.nc = null;
    if (nc) await nc.close();
    await this.outboxWork?.catch(() => {});
    for (const route of this.tailStops.keys()) await this.stopTail(route);
    // Last: nothing of this client applies to the replica any more. connect() takes the lock again.
    this.storage?.release?.();
  }

  // ─── internals ────────────────────────────────────────────────────────────

  private run: Exec = (q, ...params) => this.sql(q, ...params);

  private appendLog(topic: string, data: any, level = 'INFO') {
    for (const cb of this.logHandlers) cb(topic, data, level);
  }

  private reach(p: Phase) {
    for (const cb of this.phaseHandlers) cb(p);
  }

  private emitStatus(s: ConnStatus) {
    for (const cb of this.statusHandlers) cb(s);
  }

  private scheduleRecount() {
    clearTimeout(this.recountTimer);
    this.recountTimer = setTimeout(() => {
      for (const cb of this.anyChangeListeners) cb();
    }, 250);
  }

  private triggerChange(table: string, ev?: any) {
    // Per-event hooks (verb badges, per-table logging) only fire WITH an event —
    // a revert or seed notifies "something changed" without one, and handlers must
    // not be handed undefined (measured: markVerb crashed on ev.operation).
    if (ev !== undefined) {
      for (const cb of this.eventListeners) cb(table, ev);
    }
    if (this.tableListeners[table]) {
      for (const cb of this.tableListeners[table]) cb(ev);
    }
    this.scheduleRecount();
  }

  private async initSyncState() {
    await this.run(`
      CREATE TABLE IF NOT EXISTS _zebridge_sync (
        id INTEGER PRIMARY KEY,
        global_last_lsn ${this.dialect.int64},
        global_last_seq ${this.dialect.int64}
      );
    `);
    // One row per stream: JetStream sequences are per stream, and a single global
    // number would corrupt both (resuming one stream from the other's position).
    await this.run(`
      CREATE TABLE IF NOT EXISTS _zebridge_stream_seq (
        stream TEXT PRIMARY KEY,
        last_seq ${this.dialect.int64} NOT NULL
      );
    `);
    // Generation watermarks (NOTES.md §1.13): the client tracks WATERMARKS, never gen
    // numbers — a gen is an object-naming detail; the cutoff is what deltas chain on.
    await this.run(`
      CREATE TABLE IF NOT EXISTS _zebridge_generations (
        tbl TEXT PRIMARY KEY,
        watermark TEXT NOT NULL,
        cutoff_lsn ${this.dialect.int64} NOT NULL,
        seed_epoch INTEGER NOT NULL DEFAULT 0
      );
    `);
    // The fence's owed record (fence.ts): a row between its PREPARE and its anchor's commit.
    await this.run(`CREATE TABLE IF NOT EXISTS _zebridge_fence (tbl TEXT PRIMARY KEY)`);
    // `joint`: the owed record belongs to a joint fence (several tables replaced together). With `_zebridge_joint_done`, the
    // proofs of the participants that already committed, it is the group, durably: a restart rebuilds the parking from it.
    try { await this.run(`ALTER TABLE _zebridge_fence ADD COLUMN joint INTEGER`); } catch { /* present */ }
    await this.run(`CREATE TABLE IF NOT EXISTS _zebridge_joint_done (tbl TEXT NOT NULL, stream TEXT NOT NULL, cut ${this.dialect.int64} NOT NULL, created TEXT NOT NULL, PRIMARY KEY (tbl, stream))`);
    // The applied frontiers (fence.ts): the stream identity they were taken on rides with them.
    await this.run(`CREATE TABLE IF NOT EXISTS _zebridge_frontier (tbl TEXT NOT NULL, stream TEXT NOT NULL, created TEXT NOT NULL, seq ${this.dialect.int64} NOT NULL, PRIMARY KEY (tbl, stream))`);
    // A replica from before §10df: the column is added; refused, harmlessly, on one that has it.
    try { await this.run(`ALTER TABLE _zebridge_generations ADD COLUMN seed_epoch INTEGER NOT NULL DEFAULT 0`); } catch { /* present */ }
    try { await this.run(`ALTER TABLE _zebridge_stream_seq ADD COLUMN created TEXT`); } catch { /* present */ }
    try { await this.run(`ALTER TABLE _zebridge_stream_seq ADD COLUMN born_empty INTEGER`); } catch { /* present */ }
    // §10jc: the seed gate persisted with its watermark (it died with the process: a
    // client killed after a re-seed re-applied, on relaunch, what its chain already
    // carried in newer versions).
    try { await this.run(`ALTER TABLE _zebridge_generations ADD COLUMN seed_seq ${this.dialect.int64}`); } catch { /* present */ }
    try { await this.run(`ALTER TABLE _zebridge_generations ADD COLUMN seed_stream TEXT`); } catch { /* present */ }
    try { await this.run(`ALTER TABLE _zebridge_generations ADD COLUMN shared_seed_seq ${this.dialect.int64}`); } catch { /* present */ }

    for (const r of await this.run(`SELECT stream, created, born_empty FROM _zebridge_stream_seq WHERE created IS NOT NULL`)) {
      this.streamCreated.set(String(r.stream), String(r.created));
      if (Number(r.born_empty) === 1) this.streamBornEmpty.add(String(r.stream));
    }
    // §10dg: the shape this replica BUILT each table with (core.keyShape/typeShape) —
    // the record a re-key or a re-type is detected against.
    await this.run(`CREATE TABLE IF NOT EXISTS _zebridge_shape (tbl TEXT PRIMARY KEY, key_shape TEXT NOT NULL, type_shape TEXT NOT NULL)`);
    // The dictionary cache of chains cut before 2026-09-24 (NOTES §10iy): gone with them.
    try { await this.run(`DROP TABLE IF EXISTS _zebridge_dicts`); } catch { /* a replica that never had it */ }
    await this.createOutboxTable();
    this.resolveOutboxInit();
    await this.run(this.dialect.insertIgnore('_zebridge_sync', ['id', 'global_last_lsn', 'global_last_seq'], ['id']), 1, 0, 0);
    const res = await this.run(`SELECT global_last_lsn FROM _zebridge_sync WHERE id = 1`);
    if (res.length > 0) this.globalSyncState.lsn = res[0].global_last_lsn ?? 0;
    for (const r of await this.run(`SELECT stream, last_seq FROM _zebridge_stream_seq`)) {
      this.globalSyncState.seq[(r as any).stream] = (r as any).last_seq ?? 0;
    }
    await this.loadFrontiers();
  }

  /// The applied frontiers back from the replica. A row taken on another stream identity —
  /// the stream was recreated since (R4) — is dead and deleted. FAILS CLOSED: a read that fails
  /// throws and leaves memory as it was; it never reads as "no frontier" (replay would then apply
  /// events a table already holds). The caller decides what stays parked until it succeeds.
  private async loadFrontiers() {
    const next = new Map<string, Frontier[]>();
    for (const r of (await this.run(
      `SELECT f.tbl, f.stream, f.seq FROM _zebridge_frontier f JOIN _zebridge_stream_seq s ON s.stream = f.stream AND s.created = f.created`,
    )) ?? []) {
      const seq = Number((r as any).seq);
      if (!(seq > 0)) continue;
      const t = String((r as any).tbl);
      next.set(t, [...(next.get(t) ?? []), { stream: String((r as any).stream), seq }]);
    }
    this.frontiers = next;
    // Dead rows are already left out by the join; deleting them is housekeeping.
    try {
      await this.run(`DELETE FROM _zebridge_frontier WHERE NOT EXISTS (SELECT 1 FROM _zebridge_stream_seq s WHERE s.stream = _zebridge_frontier.stream AND s.created = _zebridge_frontier.created)`);
    } catch { /* harmless */ }
  }

  /// The position of `stream` reached `seq`: every frontier on it at or below `seq` is spent
  /// (replay has passed what those tables already held). Called after the position was persisted,
  /// with the caller's `exec` where it is inside a transaction.
  private async spendFrontiers(stream: string, seq: number, exec: Exec = this.run) {
    if (!this.frontiers.size) return;
    let spent = false;
    for (const [t, fr] of [...this.frontiers]) {
      const keep = fr.filter((f) => !(f.stream === stream && f.seq <= seq));
      if (keep.length === fr.length) continue;
      spent = true;
      if (keep.length) this.frontiers.set(t, keep); else this.frontiers.delete(t);
    }
    if (!spent) return;
    try {
      await exec(`DELETE FROM _zebridge_frontier WHERE stream = ? AND seq <= ?`, stream, seq);
    } catch (e) {
      this.appendLog('SQLITE', `Failed to spend the applied frontiers on ${stream}: ${e}`, 'ERROR');
    }
    this.appendLog('SYS', `${stream}: replay passed the applied frontiers at ${seq} — ${this.frontierGated} event(s) skipped so far for the tables that already held them`, 'INFO');
  }

  /// Memory only: the frontiers on `stream` forgotten (the caller deleted the rows in its own
  /// transaction).
  private forgetFrontiers(stream: string) {
    for (const [t, fr] of [...this.frontiers]) {
      const keep = fr.filter((f) => f.stream !== stream);
      if (keep.length === fr.length) continue;
      if (keep.length) this.frontiers.set(t, keep); else this.frontiers.delete(t);
    }
  }

  /// A table's rows stop matching its frontier when a chain replaces them as of ITS cut, or the
  /// table goes: the events up to the frontier must replay for it too. The replica first, then
  /// memory, and a failure throws: a row left behind would come back at the next load as a
  /// frontier over rows it no longer describes.
  private async dropTableFrontiers(table: string) {
    await this.run(`DELETE FROM _zebridge_frontier WHERE tbl = ?`, table);
    this.frontiers.delete(table);
  }

  // ── outbox (PROTOCOL.md §7.1) ──────────────────────────────────────────────

  private async createOutboxTable() {
    await this.run(`
      CREATE TABLE IF NOT EXISTS _zebridge_inbox (
        id       ${this.dialect.autoincrementPk},
        tbl      TEXT    NOT NULL,
        lsn      ${this.dialect.int64} NOT NULL,
        ev       TEXT    NOT NULL,
        reason   TEXT    NOT NULL,
        held_at  ${this.dialect.int64} NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0
      )
    `);
    await this.run(`CREATE INDEX IF NOT EXISTS _zebridge_inbox_tbl ON _zebridge_inbox (tbl, lsn)`);
    await this.run(`
      CREATE TABLE IF NOT EXISTS _zebridge_outbox (
        msg_id     TEXT PRIMARY KEY,
        subject    TEXT NOT NULL,
        payload    TEXT NOT NULL,
        tbl        TEXT NOT NULL,
        row_id     TEXT NOT NULL,
        before     TEXT,
        created_at ${this.dialect.int64} NOT NULL,
        attempts   INTEGER NOT NULL DEFAULT 0,
        published  INTEGER NOT NULL DEFAULT 1,
        optimistic TEXT,
        basis      TEXT
      )
    `);
    // `published` is set before the first send: a write never published cannot have
    // committed, so the GC gate may revert it here; a published one goes to the bridge,
    // whose receipt knows (an outbox from before the column counts as published).
    // `optimistic`: the local apply, re-applied after a full seed replaced the rows.
    try { await this.run(`ALTER TABLE _zebridge_outbox ADD COLUMN published INTEGER NOT NULL DEFAULT 1`); } catch { /* present */ }
    try { await this.run(`ALTER TABLE _zebridge_outbox ADD COLUMN optimistic TEXT`); } catch { /* present */ }
    try { await this.run(`ALTER TABLE _zebridge_outbox ADD COLUMN retry_at INTEGER DEFAULT 0`); } catch { /* present */ }
    // `basis`: the row as the user saw it when making the edit, written once and never
    // rewritten. `before` is the rollback target, which a reseed refreshes; a rebase judges
    // the winner against `basis` (an outbox from before the column falls back to `before`).
    // Added: the existing entries' `before` is copied in at once, before any reseed of this
    // session can rewrite it; an entry with no `before` keeps an unknown basis, which a rebase
    // treats as a conflict (lost, never re-sent).
    try {
      await this.run(`ALTER TABLE _zebridge_outbox ADD COLUMN basis TEXT`);
      await this.run(`UPDATE _zebridge_outbox SET basis = before WHERE basis IS NULL`);
    } catch { /* present */ }
    await this.run(`CREATE INDEX IF NOT EXISTS _zbz_outbox_retry ON _zebridge_outbox (retry_at, attempts, created_at, msg_id)`);
    await this.run(`CREATE INDEX IF NOT EXISTS _zbz_outbox_recovery ON _zebridge_outbox (attempts, created_at, msg_id)`);
    await this.run(`
      CREATE TABLE IF NOT EXISTS _zebridge_reconcile (
        msg_id  TEXT PRIMARY KEY,
        tbl     TEXT NOT NULL,
        reason  TEXT NOT NULL,
        version TEXT NOT NULL DEFAULT '',
        data    TEXT,
        routes  TEXT NOT NULL DEFAULT '',
        targets TEXT NOT NULL DEFAULT '',
        since   ${this.dialect.int64} NOT NULL,
        state   TEXT NOT NULL DEFAULT 'pending'
      )
    `);
  }

  private async outboxPut(
    r: { msgId: string; subject: string; payload: unknown; table: string; id: string | number; before: unknown; optimistic: unknown },
    exec: Exec = this.run,
  ) {
    await this.outboxInitPromise;
    await exec(
      `INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, before, basis, created_at, attempts, published, optimistic)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?)
       ON CONFLICT(msg_id) DO UPDATE SET attempts = _zebridge_outbox.attempts + 1`,
      r.msgId, r.subject, JSON.stringify(r.payload), r.table, String(r.id),
      r.before == null ? null : JSON.stringify(r.before), r.before == null ? null : JSON.stringify(r.before), Date.now(), JSON.stringify(r.optimistic),
    );
  }

  private async outboxDrop(msgId: string) {
    await this.outboxInitPromise;
    await this.run(`DELETE FROM _zebridge_outbox WHERE msg_id = ?`, msgId);
    this.wakeOutbox();
  }

  /// The one row of `zebridge_gc_watermark`, read from THIS client's own replica.
  ///
  /// No extra subscription and no request path: the table arrives over CDC like any
  /// other, which is the whole point of publishing it (§10as). Returns null whenever
  /// the answer is not known — the table is not replicated here, has no row yet, or
  /// the read failed — and `outboxWatermarkGate` treats null as "refuse nothing".
  ///
  /// ⚠️ Deployments exist where this table is absent (it is only in a publication if
  /// the DBA put it there), so a missing table is an ordinary state, not an error.
  public async gcWatermark(): Promise<string | null> {
    try {
      const rows = await this.run(`SELECT watermark FROM zebridge_gc_watermark LIMIT 1`);
      const w = rows?.[0]?.watermark;
      return typeof w === 'string' && w.length ? w : null;
    } catch {
      return null;
    }
  }

  /// Public for the console (`zb.outbox()`): a queue you only inspect when something
  /// has already gone wrong should be inspectable.
  public async outboxAll(): Promise<any[]> {
    await this.outboxInitPromise;
    return this.run(`SELECT * FROM _zebridge_outbox ORDER BY created_at`);
  }

  /// Undo an optimistic apply when a verdict says the write cannot land ('rejected' →
  /// restore the before-image; 'row_deleted' → the row is confirmed gone server-side).
  /// Guarded: only if the row still shows exactly what our own write applied.
  // ─── §10do: rebase on stale ─────────────────────────────────────────────

  /// A `stale` verdict names an UPDATE that lost to a newer version. Its columns
  /// are not necessarily the columns the winner changed: when the two sets are
  /// disjoint the edit is still right and is resubmitted with a fresh stamp (the
  /// HLC floor puts it above the winner); when they overlap it is dropped and
  /// surfaced — LWW's word stands. Only an UPDATE qualifies.
  private async holdForRebase(msgId: string) {
    await this.outboxInitPromise;
    const rows = await this.run(`SELECT tbl, subject, payload, before, basis, row_id, created_at FROM _zebridge_outbox WHERE msg_id = ?`, msgId);
    const entry = rows[0];
    if (!entry || String(entry.subject).split('.').pop() !== 'update') return;
    const sent = JSON.parse(entry.payload);
    const values = sent?.data;
    const key = sent?.key;
    if (!values || typeof values !== 'object' || !key || typeof key !== 'object') return;
    this.rebase.set(msgId, {
      table: entry.tbl, key, values,
      before: entry.before ? JSON.parse(entry.before) : null,
      basis: entry.basis ? JSON.parse(entry.basis) : null,
      rowId: String(entry.row_id ?? ''), createdAt: Number(entry.created_at ?? 0),
      version: normalizeVersion(String(sent.version ?? '')),
      sentVersion: String(sent.version ?? ''),
      at: Date.now(),
    });
  }

  /// Runs outside any apply transaction: the resubmit is a full mutate().
  private scheduleRebase() {
    if (this.rebaseTimer || !this.rebase.size) return;
    this.rebaseTimer = setTimeout(() => { this.rebaseTimer = null; void this.drainRebase(); }, 50);
  }

  private async drainRebase() {
    for (const msgId of [...this.rebase.keys()]) {
      try {
        await this.tryRebase(msgId);
      } catch (err) {
        this.appendLog('SYS', `rebase of ${msgId} deferred: ${err}`, 'WARNING');
        this.scheduleRebase();
      }
    }
  }

  /// Decides once the winning row is here: its version is above the refused stamp.
  private async tryRebase(msgId: string) {
    const e = this.rebase.get(msgId);
    if (!e) return;
    const state = this.syncedTables.get(e.table);
    if (!state?.pkCols.length || !state.versionColumn) { this.rebase.delete(msgId); return; }
    const where = state.pkCols.map((c) => `"${c}" = ?`).join(' AND ');
    const cur = (await this.run(`SELECT * FROM ${e.table} WHERE ${where}`, ...state.pkCols.map((c) => e.key[c])))[0];
    const reported = (outcome: Verdict['outcome'], extra: Partial<Verdict> = {}) => this.emitVerdict({
      version: e.sentVersion, table: e.table, key: e.key,
      columns: Object.keys(e.values).filter((c) => c !== state.versionColumn), outcome, ...extra,
    });
    if (!cur) {
      this.rebase.delete(msgId);
      this.appendLog(e.table, `rebase of ${msgId} abandoned: the row is gone`, 'ERROR');
      reported('deleted');
      return;
    }
    // "The winner is here" — by the row's version now, OR by the version the row carried
    // BEFORE this write. The second case is a slow clock (§10do): the winner had already
    // arrived when the write was made, the optimistic apply then stamped the row with
    // the slow clock's older version, and no CDC echo will ever raise it again — the
    // hold waited for an arrival that had happened before it was set (measured:
    // rebase_stale.py §C, Node held the edit for ever; libzb's same-shape rebase in §A
    // works because its winner arrives after the write).
    const verNow = normalizeVersion(String(cur[state.versionColumn] ?? ''));
    const verBefore = e.before ? normalizeVersion(String(e.before[state.versionColumn] ?? '')) : '';
    // The row here may be ANY of my pending edits on this key, not only this one: after a
    // reseed, each pending edit is laid over the one before it. The confirmed row is beneath
    // all of them: the `before` of the oldest pending edit on the key.
    const mineVersions = new Set([e.version]);
    let base = e.before, baseAt = e.createdAt ?? Number.MAX_SAFE_INTEGER;
    for (const o of await this.run(`SELECT payload, before, created_at FROM _zebridge_outbox WHERE tbl = ? AND row_id = ?`, e.table, e.rowId ?? '')) {
      try { const v = normalizeVersion(String(JSON.parse(o.payload)?.version ?? '')); if (v) mineVersions.add(v); } catch { /* unreadable */ }
      if (Number(o.created_at) < baseAt) { baseAt = Number(o.created_at); base = o.before ? JSON.parse(o.before) : null; }
    }
    // An earlier decision on this key may have kept the confirmed row: once the oldest pending
    // edit settled and left the outbox, it is the only place the seeded winner still lives.
    const ckey = `${e.table}\u0000${e.rowId ?? ''}`;
    const kept = this.confirmedBase?.get(ckey);
    if (kept && kept.at < baseAt) { baseAt = kept.at; base = kept.row; }
    const overlaid = mineVersions.has(verNow) && base != null;
    const winner = overlaid ? base! : cur;
    const verWinner = normalizeVersion(String(winner[state.versionColumn] ?? ''));
    if (!(verWinner > e.version || verBefore > e.version)) return; // the winner is not here yet
    const mine = Object.keys(e.values).filter((c) => c !== state.versionColumn);
    // Judged against the AUTHORITATIVE winner, never against what is displayed: right after
    // a reseed re-applied this pending edit, the row here is my own overlay (it carries my
    // version) and the winner is the seeded row underneath it, which `before` holds. Read
    // from the overlay, my own value looked like the winner's change, matched my edit, and
    // a stale edit was re-sent as a fresh write (shared_record: carol's rating 2 overwrote
    // alice's 5). And the winner is compared with the row as I saw it when I made the edit
    // (`basis`), which a reseed does not rewrite.
    const basis = e.basis ?? null; // unknown: every column of the edit counts as lost
    const winnerChanged = Object.keys(winner).filter(
      (c) => c !== state.versionColumn && JSON.stringify(winner[c] ?? null) !== JSON.stringify(basis?.[c] ?? null),
    );
    // The winner changed one of MY columns, to something other than my value: a real loss.
    // (The winner writing my very value is no loss, and a re-send of it changes nothing.)
    const overlap = basis
      ? mine.filter((c) => winnerChanged.includes(c) && JSON.stringify(winner[c] ?? null) !== JSON.stringify(e.values[c] ?? null))
      : mine;
    this.rebase.delete(msgId);
    await this.keepConfirmed(e, ckey, base, baseAt);
    if (overlap.length) {
      this.appendLog(e.table, `edit LOST to a newer version on the same column(s) ${overlap.join(', ')} — the winning row stands; surface this to the user`, 'ERROR');
      reported('lost', { lostColumns: overlap });
      return;
    }
    const r = await this.mutate(e.table, 'UPDATE', e.key, e.values);
    reported('rebased', { rebasedAs: r.version });
    this.appendLog(e.table, `rebased ${mine.join(', ')} onto the newer row (the winner changed ${winnerChanged.filter((c) => c !== 'last_writer').join(', ') || 'nothing else'}) as ${r.version}`, 'INFO');
  }

  /// Decided: the confirmed row stays for the other edits on the key, if any remain.
  private async keepConfirmed(e: { table: string; rowId?: string }, ckey: string, base: Record<string, unknown> | null, at: number) {
    const left = Number((await this.run(`SELECT count(*) AS n FROM _zebridge_outbox WHERE tbl = ? AND row_id = ?`, e.table, e.rowId ?? ''))[0]?.n ?? 0) > 0
      || [...this.rebase.values()].some((o) => o.table === e.table && o.rowId === e.rowId);
    if (left && base) (this.confirmedBase ??= new Map()).set(ckey, { row: base, at });
    else this.confirmedBase?.delete(ckey);
  }

  private async revertOptimisticWrite(msgId: string, mode: 'restore' | 'delete') {
    await this.outboxInitPromise;
    const rows = await this.run(`SELECT tbl, payload, before FROM _zebridge_outbox WHERE msg_id = ?`, msgId);
    const entry = rows[0];
    if (!entry) return;

    const table: string = entry.tbl;
    const sent = JSON.parse(entry.payload);
    const before = entry.before ? JSON.parse(entry.before) : null;
    const state = this.syncedTables.get(table);
    const keyObj = sent.key as Record<string, unknown> | undefined;

    if (!state?.pkCols.length || !keyObj) {
      await this.run(`DELETE FROM _zebridge_outbox WHERE msg_id = ?`, msgId);
      return;
    }

    const where = state.pkCols.map((c) => `"${c}" = ?`).join(' AND ');
    const pkVals = state.pkCols.map((c) => keyObj[c]);

    await this.transaction(async (txExec) => {
      const current = await txExec(`SELECT * FROM ${table} WHERE ${where}`, ...pkVals);
      const currentRow = current[0] ?? null;

      // SQLite stores booleans as 0/1 and objects as JSON text — normalize the SENT
      // side to storage shape before comparing, or a row with any boolean column
      // never matches and the revert declines forever (the oversize test's ghost).
      const asStored = (v: unknown): string =>
        typeof v === 'boolean' ? (v ? '1' : '0')
        : isBytes(v) ? Array.from(v, (b) => b.toString(16).padStart(2, '0')).join('')
        : v !== null && typeof v === 'object' ? JSON.stringify(v)
        : String(v);
      const stillOurs = sent.data
        ? currentRow != null && Object.entries(sent.data as Record<string, unknown>)
            .every(([k, v]) => asStored(currentRow[k]) === asStored(v))
        : currentRow == null;

      if (stillOurs) {
        if (mode === 'delete') {
          if (currentRow) await txExec(`DELETE FROM ${table} WHERE ${where}`, ...pkVals);
        } else if (before == null) {
          if (currentRow) await txExec(`DELETE FROM ${table} WHERE ${where}`, ...pkVals);
        } else if (currentRow == null) {
          const cols = Object.keys(before);
          const placeholders = cols.map(() => '?').join(', ');
          await txExec(
            `INSERT INTO ${table} (${cols.map((c) => `"${c}"`).join(', ')}) VALUES (${placeholders})`,
            ...cols.map((c) => before[c]),
          );
        } else {
          const cols = Object.keys(before).filter((c) => !state.pkCols.includes(c));
          if (cols.length) {
            const setClause = cols.map((c) => `"${c}" = ?`).join(', ');
            await txExec(`UPDATE ${table} SET ${setClause} WHERE ${where}`, ...cols.map((c) => before[c]), ...pkVals);
          }
        }
      }

      await txExec(`DELETE FROM _zebridge_outbox WHERE msg_id = ?`, msgId);
    });
    this.triggerChange(table);
  }

  // ── schemas ────────────────────────────────────────────────────────────────

  private watchSchemas(): Promise<void> | undefined {
    if (!this.nc) return;
    try {
      return (async () => {
        const watch = await watchBucket(this.transport.jetstream(this.nc!, this.jsOpts()), this.config.grammar.kv.schemas);
        this.appendLog('SCHEMA', `Watching KV bucket "${this.config.grammar.kv.schemas}" for all tables...`, 'WATCH');

        return new Promise<void>((resolve) => {
          let initialized = false;
          if (watch.pending === 0) { initialized = true; this.reach('migrated'); resolve(); }

          void (async () => {
            for await (const entry of watch.entries) {
              // `delta === 0` marks the last entry of the replay, but it has NOT been
              // applied yet — resolve in the finally, once the migration below landed,
              // or subscribeStreams snapshots syncedTables while the last table is
              // still being created and never seeds it (measured live).
              const isLastOfInitialReplay = !initialized && (!entry || entry.delta === 0);
              try {
                if (!entry || !entry.key) continue;
                // §10fb/§10hn: a table outside the declared set is never created, seeded
                // or followed — its descriptor, tombstone and suspension are all skipped.
                // The set is core.tableSet's: `tables` (a list or '*') plus `ondemandTables`.
                {
                  const ts = tableSet(this.config.tables, this.config.ondemandTables, [entry.key]);
                  this.ondemandSet = new Set(ts.ondemand);
                  const held = ts.follow.includes(entry.key) || ts.ondemand.includes(entry.key);
                  if (!held) {
                    if (!this.config.tables && !this.config.ondemandTables?.length && !this.warnedNoTables) {
                      this.warnedNoTables = true;
                      this.appendLog('SCHEMA', `no tables declared — following nothing; pass tables: '*' to follow every published table, or a list`, 'WARNING');
                    }
                    continue;
                  }
                }

                if (entry.operation === 'DEL' || entry.operation === 'PURGE') {
                  await this.dropLocalTable(entry.key, 'KV key removed');
                  continue;
                }

                // Schema is always JSON, never msgpack — a fixed bridge-side rule.
                const val: any = JSON.parse(td.decode(entry.value));

                if (val?.dropped === true) {
                  await this.dropLocalTable(entry.key, `tombstone @ lsn ${val.lsn ?? '?'}`);
                  continue;
                }

                if (val?.suspended === true) {
                  // NOTES.md §1.6c: the bridge refuses the table upstream; local rows
                  // stay frozen and valid, writes are refused client-side.
                  const reason = String(val.reason ?? 'suspended');
                  this.suspendedMap.set(entry.key, reason);
                  for (const cb of this.suspendedHandlers) cb(entry.key, reason);
                  this.appendLog('SCHEMA', `${entry.key} suspended upstream (${reason})`, 'WARNING');
                  continue;
                }

                if (val?.sqlite?.columns) {
                  if (this.suspendedMap.delete(entry.key)) {
                    for (const cb of this.suspendedHandlers) cb(entry.key, null);
                  }
                  await this.applySchema(entry.key, val);
                }
              } finally {
                if (isLastOfInitialReplay) {
                  initialized = true;
                  resolve();
                }
              }
            }
          })();
        });
      })();
    } catch (err) {
      this.appendLog('SCHEMA', `Watch failed: ${err}`, 'ERROR');
      return undefined;
    }
  }

  private async dropLocalTable(table: string, reason: string) {
    try {
      await this.pruneInboxDropped(table);
      await this.run(`DROP VIEW IF EXISTS ${table}_view;`);
      await this.run(`DROP TABLE IF EXISTS ${table};`);
      this.syncedTables.delete(table);
      await this.dropTableFrontiers(table);
      await this.discardOutbox(table, 'dropped');
      this.appendLog('SCHEMA', `Dropped local table "${table}" (${reason})`, 'DROP');
      this.scheduleRecount();
    } catch (err) {
      this.appendLog('SCHEMA', `Drop of ${table} failed: ${err}`, 'ERROR');
    }
  }

  /// Queued writes for a dropped or re-keyed table can never apply — the server
  /// would only answer row_deleted (or worse, land on an unrelated table that later
  /// reuses the name). Discard them LOUDLY: a silent queue that drains into a void
  /// is exactly what the outbox exists to prevent.
  /// §10hn: an on-demand table is never seeded from a chain — the seed loop skips it —
  /// so a message promising a fresh full describes work that will never happen, and it
  /// reads as a fault. libzb says the same thing at the same three outcomes (§10hj).
  private reseedNote(table: string): string {
    return this.ondemandSet.has(table)
      ? 'on demand, so no chain seed; rows arrive as query answers'
      : 'watermark dropped, re-seeding from a fresh full';
  }

  private async discardOutbox(table: string, why: string) {
    try {
      const q = await this.run(`SELECT count(*) AS k FROM _zebridge_outbox WHERE tbl = ?`, table);
      const k = q?.[0]?.k ?? 0;
      if (k > 0) {
        await this.run(`DELETE FROM _zebridge_outbox WHERE tbl = ?`, table);
        this.appendLog('SCHEMA', `${k} queued write(s) for ${why} table "${table}" discarded — surface this to the user`, 'WARNING');
      }
    } catch { /* outbox not initialized yet — nothing queued */ }
  }

  private async applySchema(table: string, val: any) {
    // `pk_columns` is authoritative; `pk` is the legacy single-column form.
    const pkCols: string[] = Array.isArray(val.pk_columns) ? val.pk_columns : val.pk ? [val.pk] : [];
    // The column TYPES come from the block this engine speaks (dialect.ts): `sqlite`
    // for the SQLite adapters, `pg` for PGlite / a local Postgres — the descriptor has
    // carried both since §10c, and this is the first consumer of `pg`. Column NAMES,
    // pk, indexes and FKs are dialect-neutral at the root.
    const block = val[this.dialect.schemaBlock] ?? val.sqlite;
    const cols: { name: string; type: string; required?: boolean; default?: string }[] = block.columns;
    // §10ey: array columns, from the `pg` block whatever the engine (the sqlite block says TEXT).
    const arrayCols: string[] = ((val.pg?.columns ?? []) as { name: string; type?: string }[])
      .filter((c) => typeof c.type === 'string' && c.type.endsWith('[]')).map((c) => c.name);
    const blobCols: string[] = ((val.sqlite?.columns ?? []) as { name: string; type?: string }[])
      .filter((c) => typeof c.type === 'string' && c.type.toUpperCase() === 'BLOB').map((c) => c.name);
    const vecCols = vecColsOf((val.pg?.columns ?? []) as { name: string; type?: string }[]);
    // Dialect-neutral, at the root: CREATE [UNIQUE] INDEX is the same statement here
    // and in PGlite/local Postgres, so one list serves every consumer shape (§10c).
    const indexes: { name: string; unique?: boolean; columns: string[] }[] =
      Array.isArray(val.indexes) ? val.indexes : [];
    // Already filtered upstream to constraints SQLite can satisfy — parent key is
    // the parent's PK or has a ported UNIQUE index (NOTES §10d).
    const foreignKeys: { name: string; columns: string[]; references: string; parent_columns: string[] }[] =
      Array.isArray(val.foreign_keys) ? val.foreign_keys : [];
    const lsn: number = typeof val.lsn === 'number' ? val.lsn : 0;
    const tombstoneColumn: string | null = typeof val.tombstone_column === 'string' ? val.tombstone_column : null;
    const tenantColumn: string | null = typeof val.tenant_column === 'string' ? val.tenant_column : null;
    const seedEpoch: number = typeof val.seed_epoch === 'number' ? val.seed_epoch : 0;
    const versionColumn: string | null = typeof val.version_column === 'string' ? val.version_column : null;
    const names = cols.map((c) => c.name);

    // ── FINDING 9 (the destroyer behind §10j): "first sight" must be decided by
    // the DATABASE, not this in-memory map. `syncedTables` is empty in every fresh
    // process, so a durable replica's every reconnect took the "first sight" path —
    // DROP TABLE + CREATE — wiping the data while the durable bookkeeping
    // (watermarks, stream positions) survived and testified that everything was
    // fine. Measured in a clean room: every run logged `created (first sight)` for
    // every table; a reconnect with no gap left users=0/salaries=0 because nothing
    // re-seeded what the drop had just emptied; and one run kept its users only
    // because the salaries FOREIGN KEY blocked the DROP. This single defect is the
    // vanished-cx-users and the 3750-of-4500 of §10j.
    // §10dj: a host killed between a rebuild's DROP and its RENAME leaves the rows in
    // `<table>__migrating` and no `<table>`. Finish the rename rather than start
    // from nothing — the rows are right there.
    try {
      const tmpInfo = await this.dialect.tableInfo(this.run, `${table}__migrating`);
      const realInfo = await this.dialect.tableInfo(this.run, table);
      if (tmpInfo.length && !realInfo.length) {
        await this.run(`ALTER TABLE ${table}__migrating RENAME TO ${table};`);
        this.appendLog('SCHEMA', `${table}: a rebuild was interrupted before its rename — adopted ${table}__migrating`, 'MIGRATE');
      }
    } catch { /* no leftover */ }
    let existing = this.syncedTables.get(table);
    if (!existing) {
      try {
        const phys = await this.dialect.tableInfo(this.run, table);
        if (phys.length) {
          existing = {
            columns: phys.map((c) => c.name),
            pkCols: phys.filter((c) => c.pk > 0).sort((a, b) => a.pk - b.pk).map((c) => c.name),
            lsn: 0,
            tombstoneColumn: null,
            tenantColumn: null,
          };
        }
      } catch { /* introspection failed — behaves as before */ }
    }
    // §10dg: the shape this replica BUILT — its own record, not an introspection
    // (whose type text differs per engine). Key shape moved → re-key: the table is
    // rebuilt EMPTY (rows keyed the old way cannot be re-keyed in place; SQLite's
    // INTEGER PRIMARY KEY refuses a uuid) and re-seeded. A non-key type moved → an
    // ALTER COLUMN TYPE where the engine has one, a row-keeping rebuild where not.
    const keyNow = keyShape(pkCols, cols);
    const typeNow = typeShape(cols);
    let keyBefore: string | null = null;
    let typeBefore: string | null = null;
    try {
      const r = await this.run(`SELECT key_shape, type_shape FROM _zebridge_shape WHERE tbl = ?`, table);
      if (r?.length) { keyBefore = r[0].key_shape ?? null; typeBefore = r[0].type_shape ?? null; }
    } catch { /* no record yet */ }
    // With no record (a replica from before §10dg, or a record lost to a kill) the
    // PHYSICAL pk column names decide — names only, never the engine's type text.
    let physPk: string[] = [];
    if (existing && keyBefore === null) {
      try { physPk = (await this.dialect.tableInfo(this.run, table)).filter((c) => c.pk > 0).sort((a, b) => a.pk - b.pk).map((c) => c.name); } catch { /* unknown */ }
    }
    const rekey = !!existing && (keyBefore !== null ? keyBefore !== keyNow : physPk.length > 0 && physPk.join(',') !== pkCols.join(','));
    const retyped = rekey ? [] : retypedColumns(typeBefore, cols);
    // Set when the rows had to go (a re-key, or a rebuild that could not carry them):
    // the watermark goes with them and a fresh full brings them back.
    let emptied = rekey;
    const recordShape = () =>
      this.run(this.dialect.insertReplace('_zebridge_shape', ['tbl', 'key_shape', 'type_shape'], ['tbl']), table, keyNow, typeNow);

    // core.diffColumns (§10s 2b): rename-aware — a hinted rename is neither
    // added nor removed; an unhinted one degrades to add+remove (§1.2).
    const { renames, added, removed } = diffColumns(
      existing ? existing.columns : null, names, (val.renamed ?? {}) as Record<string, string>);

    // DDL text and constraints come from core (columnDdl/fkClausesFor, §10s 2b).
    // SQLite has no ALTER TABLE ADD CONSTRAINT, so an FK change forces a rebuild
    // below while an index change stays a cheap CREATE/DROP.
    const ddlOpts = { deferrable: this.dialect.deferrableForeignKeys, strict: this.dialect.name === 'sqlite' };
    const fkClauses = fkClausesFor(foreignKeys, ddlOpts);

    const rebuildPreservingData = async (why: string) => {
      // Schema surgery on an already-consistent copy: with foreign_keys ON, the
      // DROP of a referenced parent is refused outright (measured: users, blocked
      // by salaries' FK). Off for the surgery, back on after — the data is copied,
      // not changed.
      try { await this.dialect.setForeignKeys(this.run, false); } catch { /* engine without it */ }
      try {
        for (const st of rebuildSteps(table, cols, pkCols, foreignKeys, existing ? existing.columns : [], ddlOpts)) {
          await this.run(st.sql, ...st.params);
        }
        this.appendLog('SCHEMA', `${table}: rebuilt preserving common columns (${why}), lsn=${lsn}`, 'MIGRATE');
      } catch (carryErr) {
        // §10dg: a migration that cannot carry the rows degrades to a re-seed, never
        // to a table stuck in its old shape — measured on a re-key: the parent stood
        // empty (waiting for its full) while the child's copy hit its FOREIGN KEY.
        // Empty now; the watermark goes below, and the next full brings the rows back.
        await this.run(`DROP TABLE IF EXISTS ${table}__migrating;`);
        for (const st of createTableSteps(table, cols, pkCols, foreignKeys, ddlOpts)) await this.run(st.sql, ...st.params);
        await this.pruneInboxDropped(table);
        await this.discardOutbox(table, 'emptied');
        await this.run(`DELETE FROM _zebridge_generations WHERE tbl = ?`, table);
        emptied = true;
        this.appendLog('SCHEMA', `${table}: rows could not be carried through the rebuild (${carryErr}) — rebuilt EMPTY, ${this.reseedNote(table)}`, 'REKEY');
      } finally {
        try { await this.dialect.setForeignKeys(this.run, true); } catch { /* engine without it */ }
      }
    };

    try {
      if (!existing || rekey) {
        if (rekey) {
          // Everything that referred to the old key goes with it: held events, queued
          // writes, the view; the watermark below, once the empty table stands.
          await this.pruneInboxDropped(table);
          await this.discardOutbox(table, 're-keyed');
          await this.run(`DROP VIEW IF EXISTS ${table}_view;`);
          try { await this.dialect.setForeignKeys(this.run, false); } catch { /* engine without it */ }
        }
        for (const st of createTableSteps(table, cols, pkCols, foreignKeys, ddlOpts)) {
          await this.run(st.sql, ...st.params);
        }
        if (rekey) {
          try { await this.dialect.setForeignKeys(this.run, true); } catch { /* engine without it */ }
          await this.run(`DELETE FROM _zebridge_generations WHERE tbl = ?`, table);
          this.appendLog('SCHEMA', `${table}: key shape changed (${keyBefore} → ${keyNow}) — rebuilt EMPTY, ${this.reseedNote(table)}`, 'REKEY');
        } else {
          // A table that did not exist cannot be seeded, whatever a watermark left
          // behind by a kill says (§10dj): forget it, so the seed happens.
          await this.run(`DELETE FROM _zebridge_generations WHERE tbl = ?`, table);
          emptied = true;
          this.appendLog('SCHEMA', `${table}: created (first sight), lsn=${lsn}`, 'MIGRATE');
        }
      } else if (added.length === 0 && removed.length === 0 && renames.length === 0 && retyped.length === 0 &&
                 !(await this.foreignKeysDiffer(table, fkClauses)) && !(await this.strictMissing(table))) {
        await recordShape();
        this.syncedTables.set(table, { pkCols, columns: names, arrayCols, blobCols, vecCols, lsn, tombstoneColumn, tenantColumn, versionColumn, seedEpoch });
        await this.restoreSeedGate(table);
        this.reach('migrated');
        this.scheduleRecount();
        // ⚠️ NOT a no-op path for indexes. Adding an index in PostgreSQL changes no
        // column, so the republish that carries it lands EXACTLY here — returning
        // without syncing would make `CREATE INDEX` upstream a silent no-op forever.
        await this.syncIndexes(table, indexes);
        // §10df: a republish is usually identical in shape — the epoch is the message.
        await this.reseedIfEpochMoved(table, seedEpoch);
        return; // identical schema, e.g. a boot republish
      } else {
        // The view goes FIRST (§1.17): DROP COLUMN re-validates every schema object
        // referencing the table, and the stale view kills the ALTER.
        await this.run(`DROP VIEW IF EXISTS ${table}_view;`);
        await this.syncIndexes(table, indexes, 'drops');
        for (const [from, to] of renames) {
          await this.run(`ALTER TABLE ${table} RENAME COLUMN "${from}" TO "${to}";`);
        }
        try {
          for (const name of removed) {
            await this.run(`ALTER TABLE ${table} DROP COLUMN "${name}";`);
          }
          for (const name of added) {
            const c = cols.find((x) => x.name === name)!;
            // A constant default rides along (§10df): SQLite and PGlite both fill the
            // existing rows with it, so PostgreSQL's old rows and ours converge.
            const dflt = c.default != null && c.default !== '' ? ` DEFAULT ${c.default}` : '';
            await this.run(`ALTER TABLE ${table} ADD COLUMN "${name}" ${c.type}${dflt};`);
          }
          for (const name of retyped) {
            const c = cols.find((x) => x.name === name)!;
            // SQLite has no ALTER COLUMN TYPE: the throw lands in the rebuild below,
            // which copies the rows (affinity converts what converts).
            if (!this.dialect.alterColumnType) throw new Error(`column "${name}" re-typed to ${c.type}`);
            await this.dialect.alterColumnType(this.run, table, name, c.type);
            this.appendLog('SCHEMA', `${table}: column "${name}" re-typed to ${c.type} in place`, 'MIGRATE');
          }
          // §10fi: a table from before STRICT is rebuilt once, rows carried.
          if (await this.strictMissing(table)) throw new Error('table is not STRICT');
          if (await this.foreignKeysDiffer(table, fkClauses)) {
            if (this.dialect.alterForeignKeys) {
              // PostgreSQL: constraints are ALTERable, and a rebuild would be refused
              // for a referenced parent anyway (dialect.ts).
              await this.dialect.alterForeignKeys(this.run, table, foreignKeys);
              this.appendLog('SCHEMA', `${table}: foreign keys altered in place`, 'MIGRATE');
            } else {
              // ALTER cannot add or drop a constraint in SQLite; only a rebuild can.
              await rebuildPreservingData('foreign keys changed');
            }
          }
        } catch (alterErr) {
          await rebuildPreservingData(`ALTER refused: ${alterErr}`);
        }
      }

      for (const st of viewSteps(table, names)) await this.run(st.sql, ...st.params);

      // After every shape change, because a rebuild DROPs the table and takes its
      // indexes with it.
      await this.syncIndexes(table, indexes);
      await recordShape();

      this.syncedTables.set(table, { pkCols, columns: names, arrayCols, blobCols, vecCols, lsn, tombstoneColumn, tenantColumn, versionColumn, seedEpoch });
      await this.restoreSeedGate(table);
      // Both registration paths mark the phase — a strip that lies is worse than none.
      this.reach('migrated');
      this.scheduleRecount();

      await this.reseedIfEpochMoved(table, seedEpoch);
      // A re-key (or an emptied rebuild) dropped its own watermark above; the epoch
      // check has nothing to compare, so the seed is kicked here.
      if (emptied) this.kickReseed(table);
      await this.drainPending(table);
      this.ensureTails();
    } catch (err) {
      this.appendLog('SCHEMA', `Applying schema for ${table} failed: ${err}`, 'ERROR');
    }
  }

  /// Does the stored table's DDL disagree with the foreign keys we now want?
  ///
  /// Compared against the recorded CREATE TABLE text because SQLite keeps no
  /// queryable "expected constraints" — `foreign_key_list` reports what IS declared,
  /// and comparing that back to clauses is more fragile than comparing the clauses
  /// themselves. A mismatch forces `rebuildPreservingData`, since ALTER cannot add
  /// or drop a constraint.
  /// §10fi: on SQLite, is the stored table not STRICT (created before §10fi)?
  private async strictMissing(table: string): Promise<boolean> {
    if (this.dialect.name !== 'sqlite') return false;
    try {
      const ddl = await this.dialect.tableDdl(this.run, table);
      return ddl === null ? false : strictMissing(ddl);
    } catch {
      return false;
    }
  }

  private async foreignKeysDiffer(table: string, fkClauses: string): Promise<boolean> {
    try {
      const ddl = await this.dialect.tableDdl(this.run, table);
      if (ddl === null) return false; // engine keeps no DDL text: an FK change is not detectable here
      return fkTextDiffers(ddl, fkClauses); // the pure compare lives in core (§10s 2b)
    } catch {
      return false;
    }
  }

  /// Bring the replica's secondary indexes in line with the published list.
  ///
  /// Without these the replica answers every predicate with a sequential scan, which
  /// quietly undercuts the whole promise of querying it directly. The bridge sends
  /// only what translates (no partial, expression or non-btree indexes), so this is a
  /// straight create/drop against the names it names.
  ///
  /// Drops indexes we hold that are no longer published — an index removed upstream
  /// should not linger, costing writes for a query nobody makes. `sqlite_`-prefixed
  /// entries are SQLite's own (the implicit PK index) and are never touched.
  /// `only: 'drops'` runs the drop half alone — called BEFORE the ALTERs, because
  /// SQLite refuses DROP COLUMN on an indexed column (found by libzb's unit test on
  /// 2026-08-29: a plain column removal fell through to a rebuild). Same rule as the
  /// view (§1.17): every schema object on the column goes first.
  private async syncIndexes(table: string, indexes: { name: string; unique?: boolean; columns: string[] }[], only?: 'drops') {
    try {
      const have = await this.dialect.indexNames(this.run, table);
      const plan = indexSyncPlan(table, have, indexes);
      for (const d of plan.drops) {
        await this.run(d.sql, ...d.params);
        this.appendLog('SCHEMA', `${table}: dropped index (no longer published): ${d.sql}`, 'MIGRATE');
      }
      if (only === 'drops') return;
      for (const c of plan.creates) await this.run(c.sql, ...c.params);
      if (plan.creates.length) {
        this.appendLog('SCHEMA', `${table}: ${plan.creates.length} index(es) created`, 'MIGRATE');
      }
    } catch (err) {
      // An index is a performance object: failing to build one must never stop a
      // table from syncing. Loud, but not fatal.
      this.appendLog('SCHEMA', `${table}: index sync failed (queries will scan): ${err}`, 'ERROR');
    }
  }

  /// §10df: the descriptor's seed_epoch is above the one this replica seeded at —
  /// zebridge_reseed() ran upstream. Forget the watermark, so the table seeds a fresh
  /// full (now, if connected; else at the next connect's gap check).
  private async reseedIfEpochMoved(table: string, epoch: number) {
    if (this.ondemandSet.has(table)) return; // §10hn: unseeded by design
    try {
      const r = await this.run(`SELECT seed_epoch FROM _zebridge_generations WHERE tbl = ?`, table);
      if (!r?.length) return;
      const stored = Number(r[0].seed_epoch ?? 0);
      if (stored >= epoch) return;
      await this.run(`DELETE FROM _zebridge_generations WHERE tbl = ?`, table);
      this.appendLog('SYS', `${table}: seed epoch ${stored} → ${epoch} (zebridge_reseed) — ${this.reseedNote(table)}`, 'RESEED');
      this.kickReseed(table);
    } catch { /* no watermark yet */ }
  }

  /// §10df/§10dg: a re-seed asked for by a descriptor (an epoch move, a re-key, a
  /// rebuild that could not carry the rows) may precede the producer's full by up to
  /// one cadence — `applyGenerations` answers false ("wait") until it lands. One
  /// fire-and-forget call was measured to leave the table empty until the next
  /// connect; this keeps asking, one loop per table, on the connect path's clock.
  private reseedKicks = new Map<string, Promise<void>>();
  /// How many re-seeds hold FK enforcement off (a parent's full replay DELETEs rows
  /// its children still reference — measured: "FOREIGN KEY constraint failed" on
  /// the first kick). Off at the first, back on after the last, like the connect path.
  private fkHolds = 0;
  /// §10et: keep asking for a table's chain after the connect-time wait ran out,
  /// without a deadline; when it lands, seed, then replay what was held meanwhile.
  private lateSeeds = new Set<string>();
  private waitForChain(table: string) {
    const state = this.syncedTables.get(table);
    if (!state || this.lateSeeds.has(table)) return;
    state.unseeded = true;
    state.unseededHeld = 0;
    this.lateSeeds.add(table);
    void (async () => {
      const started = Date.now();
      let lastSaid = started;
      while (this.nc && this.syncedTables.get(table)?.unseeded) {
        await new Promise((r) => setTimeout(r, GENERATION_SLOW_POLL_MS));
        if (!this.nc) return;
        // An attempt that throws is one failed attempt, not the end of the wait: the table is still
        // unseeded, its events are held, and this loop is the only thing asking (a throw inside an
        // automatic retry asks for another loop, which `lateSeeds` turns into this one).
        let landed = false;
        try {
          landed = await this.applyGenerations(table);
        } catch (e) {
          this.appendLog('SYS', `${table}: late seed attempt failed: ${e} — asking again in ${GENERATION_SLOW_POLL_MS / 1000}s`, 'WARNING');
        }
        if (landed) {
          const st = this.syncedTables.get(table);
          if (st) { st.unseeded = false; }
          this.reach('snapshot');
          this.appendLog('SYS', `${table}: chain landed after ${Math.round((Date.now() - started) / 1000)}s — seeded; replaying ${st?.unseededHeld ?? 0} held event(s)`, 'INFO');
          await this.drainPending(table);
          this.scheduleRecount();
          return;
        }
        if (Date.now() - lastSaid >= 60_000) {
          lastSaid = Date.now();
          this.appendLog('SYS', `${table}: still no chain (${Math.round((Date.now() - started) / 1000)}s); ${this.syncedTables.get(table)?.unseededHeld ?? 0} event(s) held`, 'INFO');
        }
      }
    })().catch((e) => this.appendLog('SYS', `${table}: late seed failed: ${e}`, 'ERROR'))
      .finally(() => this.lateSeeds.delete(table));
  }

  private kickReseed(table: string) {
    if (!this.nc || this.resyncing || this.reseedKicks.has(table)) return;
    const loop = (async () => {
      if (this.fkHolds++ === 0) { try { await this.dialect.setForeignKeys(this.run, false); } catch { /* engine without it */ } }
      try {
        // §10du: no deadline. The producer cuts the full under the new epoch on ITS
        // cadence, which this client does not know — a 90 s budget against a 300 s
        // cadence lost by 25 s, measured on the re-key of app_orders, and "retried at
        // the next connect" meant a reload. Poll fast for the first window, then slowly
        // for as long as the connection lives; say so once a minute.
        const started = Date.now();
        let lastSaid = started;
        while (this.nc) {
          // A throwing attempt is one failed attempt: the epoch moved and the table is waiting for
          // its full, and this loop is the only thing asking.
          try {
            if (await this.applyGenerations(table)) return;
          } catch (e) {
            this.appendLog('SYS', `${table}: re-seed attempt failed: ${e} — asking again`, 'WARNING');
          }
          const waited = Date.now() - started;
          if (Date.now() - lastSaid >= 60_000) {
            lastSaid = Date.now();
            this.appendLog('SYS', `${table}: still waiting for the producer's full under the new epoch (${Math.round(waited / 1000)}s) — it comes on the producer's cadence`, 'INFO');
          }
          await new Promise((r) => setTimeout(r, waited < GENERATION_WAIT_MS ? GENERATION_POLL_MS : GENERATION_SLOW_POLL_MS));
        }
      } finally {
        if (--this.fkHolds === 0) { try { await this.dialect.setForeignKeys(this.run, true); } catch { /* engine without it */ } }
      }
    })().catch((e) => this.appendLog('SYS', `${table}: re-seed failed: ${e}`, 'ERROR'))
      .finally(() => this.reseedKicks.delete(table));
    this.reseedKicks.set(table, loop);
  }

  private async drainPending(table: string) {
    // The schema moved: whatever was held for this table (unknown columns, a missing
    // parent) gets its retry now, from the inbox.
    if (!this.fkHeld.some((h) => h.table === table)) return;
    await this.retryFkHeld(`schema:${table}`);
  }

  /// Tentative measurement outcomes, owned by one transaction attempt.
  private measurementAttempt(stream: string, entries: { table: string; ev: any }[]): MeasureAttempt | undefined {
    if (!this.measure.open(stream)) return;
    const counted = entries.filter(({ ev }) => this.measure.counts(ev.stream, ev.seq));
    const attempt = new MeasureAttempt(counted.map(({ table, ev }) => {
      const state = this.syncedTables.get(table);
      return { ev, gated: !!state && !state.unseeded && (seedGateDrops(ev, state) || frontierDrops(ev, this.frontiers.get(table))) };
    }));
    for (const { table, ev } of counted) {
      if (!this.syncedTables.has(table) || !ev.data || typeof ev.data !== 'object') attempt.failed(ev);
    }
    return attempt;
  }

  /// A batch failed as a unit — replay it event by event so one bad row cannot take
  /// the rest with it. Anything failing a FOREIGN KEY check is HELD, not dropped:
  /// its parent may simply be in a later batch (NOTES.md finding 5).
  private async applyBatchIsolated(streamName: string, toApply: { table: string; ev: any }[], batchErr: string) {
    let applied = 0, held = 0, failed = 0;
    const measured = emptyMeasureCounts();
    for (const { table, ev } of toApply) {
      let attempt = this.measurementAttempt(streamName, [{ table, ev }]);
      try {
        await this.transaction(async (txExec) => {
          await this.dialect.deferForeignKeys(txExec);
          await this.applyEvent(table, ev, txExec, false, attempt);
        });
        applied++;
        this.triggerChange(table, ev);
      } catch (e) {
        // The refused transaction's tentative holds/outcomes did not commit.
        attempt = this.measurementAttempt(streamName, [{ table, ev }]);
        const kind = foreignKeyFailureKind(e);
        if (kind === 'missing-parent') {
          await this.holdEvent(table, ev, String(e), attempt);
          held++;
        } else if (kind === 'mismatch') {
          // Waiting cannot fix this: the parent key is not the PK and carries no
          // UNIQUE index, so the constraint should never have been ported.
          failed++;
          attempt?.failed(ev);
          this.appendLog('CDC', `DROPPED ${ev?.operation} on ${table}: ${e} — the ported FK is unsatisfiable, its parent key needs a UNIQUE index (NOTES §10d)`, 'ERROR');
        } else {
          failed++;
          attempt?.failed(ev);
          this.appendLog('CDC', `DROPPED ${ev?.operation} on ${table} (lsn ${ev?.lsn}): ${e}`, 'ERROR');
        }
      }
      if (attempt) {
        const counts = attempt.snapshot();
        for (const key of Object.keys(counts) as (keyof MeasureCounts)[]) measured[key] += counts[key];
      }
    }
    this.appendLog(
      'SYS',
      `${streamName} batch of ${toApply.length} failed as a unit (${batchErr}) — isolated replay: ` +
        `${applied} applied, ${held} held for a missing parent, ${failed} dropped`,
      failed ? 'ERROR' : 'WARNING',
    );
    return measured;
  }

  private async deleteHeld(rows: { id?: number }[], exec: Exec = this.run) {
    const ids = rows.map((r) => r.id).filter((i): i is number => i != null);
    if (!ids.length) return;
    await exec(`DELETE FROM _zebridge_inbox WHERE id IN (${ids.map(() => '?').join(',')})`, ...ids);
  }

  /// Persist a held event and remember it. In memory ALONE it was lost on restart
  /// while already ACKed to JetStream — so it would never be redelivered either.
  /// Silent, permanent divergence, the same family as findings 4 and 5.
  ///
  /// `id` is AUTOINCREMENT: arrival order is apply order, and a restart must
  /// replay them in the order they were received, not by lsn (which is NOT
  /// monotonic in delivery order — NOTES §10f).
  private async holdEvent(table: string, ev: any, reason: string, attempt?: MeasureAttempt) {
    const lsn = typeof ev?.lsn === 'number' ? ev.lsn : 0;
    try {
      const r = await this.run(
        `INSERT INTO _zebridge_inbox (tbl, lsn, ev, reason, held_at) VALUES (?,?,?,?,?) RETURNING id`,
        table, lsn, JSON.stringify(ev), reason.slice(0, 300), Date.now(),
      );
      this.fkHeld.push({ id: r?.[0]?.id, table, ev });
    } catch (err) {
      // Never lose the event to a bookkeeping failure — hold it in memory at least.
      this.fkHeld.push({ table, ev });
      this.appendLog('CDC', `held event could not be persisted (${err}) — it survives only until restart`, 'ERROR');
    }
    attempt?.held(ev);
  }

  /// Reload holds after a restart, in arrival order.
  private async loadHeldEvents() {
    try {
      const rows = await this.run(`SELECT id, tbl, ev FROM _zebridge_inbox ORDER BY id`);
      if (!rows?.length) return;
      for (const r of rows) {
        try { this.fkHeld.push({ id: r.id, table: r.tbl, ev: JSON.parse(r.ev) }); } catch { /* unreadable row */ }
      }
      this.appendLog('SYS', `${this.fkHeld.length} held event(s) restored from the inbox — awaiting their parents`, 'INFO');
    } catch { /* table absent on an older replica */ }
  }

  /// PRUNING. Four points, and deliberately no fifth:
  ///
  ///   applied      — the row is done; delete it. The primary path.
  ///   re-seeded    — a seed is a new baseline at a watermark, so anything at or
  ///                  below it is ALREADY in the seeded data. Rows ABOVE it are
  ///                  NOT, and must survive: they were acked, so CDC will never
  ///                  redeliver them. Hence `lsn <= watermark`, never a blanket
  ///                  delete by table.
  ///   table dropped— the table is gone; its holds are meaningless.
  ///   ...and NOT by age. A parent that never arrives is a REAL condition (a
  ///   constraint whose parent row the client may not read), and expiring the row
  ///   would silently discard data — the exact failure class this whole table
  ///   exists to end. It stays, it is counted, and it is loud.
  private async pruneInboxKey(exec: Exec, table: string, pkCols: string[], data: any) {
    if (!pkCols.length || !data) return;
    const same = (d: any) => pkCols.every((c) => d?.[c] !== undefined && String(d[c]) === String(data[c]));
    const before = this.fkHeld.length;
    this.fkHeld = this.fkHeld.filter((h) => !(h.table === table && same(h.ev?.data)));
    try {
      const rows = await exec(`SELECT id, ev FROM _zebridge_inbox WHERE tbl = ?`, table);
      const ids = (rows ?? []).filter((r: any) => { try { return same(JSON.parse(r.ev)?.data); } catch { return false; } }).map((r: any) => r.id);
      if (ids.length) await exec(`DELETE FROM _zebridge_inbox WHERE id IN (${ids.map(() => '?').join(',')})`, ...ids);
      const n = Math.max(ids.length, before - this.fkHeld.length);
      if (n) this.appendLog('SYS', `${table}: ${n} held event(s) for a row deleted upstream discarded`, 'INFO');
    } catch { /* inbox not initialized yet */ }
  }

  private async pruneInboxSeeded(table: string, watermarkLsn: number) {
    try {
      await this.run(`DELETE FROM _zebridge_inbox WHERE tbl = ? AND lsn <= ?`, table, watermarkLsn);
      this.fkHeld = this.fkHeld.filter((h) => !(h.table === table && (h.ev?.lsn ?? 0) <= watermarkLsn));
    } catch { /* nothing held */ }
  }

  private async pruneInboxDropped(table: string) {
    try {
      const q = await this.run(`SELECT count(*) AS k FROM _zebridge_inbox WHERE tbl = ?`, table);
      const k = q?.[0]?.k ?? 0;
      if (k > 0) {
        this.appendLog('CDC', `${table}: discarding ${k} held event(s) — the table was dropped upstream`, 'WARNING');
        await this.run(`DELETE FROM _zebridge_inbox WHERE tbl = ?`, table);
      }
      this.fkHeld = this.fkHeld.filter((h) => h.table !== table);
    } catch { /* nothing held */ }
  }

  /// Retry events held for a missing parent. Called after every batch, because the
  /// parent arrives in a LATER batch when a big transaction was split — which makes
  /// a cross-batch split self-healing rather than a silent hole.
  private async retryFkHeld(streamName: string) {
    if (!this.fkHeld.length) return;
    // An event still ahead of its table's schema is not retried, and not dropped:
    // it waits for the descriptor (attempts + 1), the same as a missing parent.
    const behind = (h: { table: string; ev: any }) => {
      const st = this.syncedTables.get(h.table);
      if (!st) return true;
      const data = h.ev?.data ?? {};
      return Object.keys(data).some((k) => !k.startsWith('old.') && !st.columns.includes(k));
    };
    const waiting = this.fkHeld.filter(behind);
    const pending = this.fkHeld.filter((h) => !behind(h));
    this.fkHeld = waiting;
    for (const h of waiting) if (h.id != null) { try { await this.run(`UPDATE _zebridge_inbox SET attempts = attempts + 1 WHERE id = ?`, h.id); } catch { /* best effort */ } }
    if (!pending.length) return;
    let applied = 0;

    // ⚠️ BULK FIRST, and this is the whole difference between converging and not.
    // Retrying one-transaction-per-event after every batch is O(n * batches): with
    // 15,000 held across 51 batches that is ~765,000 transactions, which never
    // finishes and starves the very batches that carry the missing parents —
    // measured, the held set sat at 15,000 with 0 resolved while the parent table
    // stopped advancing entirely. One deferred transaction retries the whole set at
    // once, and once the parents have landed that is a single COMMIT.
    try {
      await this.transaction(async (txExec) => {
        await this.dialect.deferForeignKeys(txExec);
        for (const { table, ev } of pending) await this.applyEvent(table, ev, txExec);
        // Same transaction as the apply: an event is either applied AND forgotten,
        // or neither. A crash between the two would replay it forever or lose it.
        await this.deleteHeld(pending, txExec);
      });
      applied = pending.length;
      for (const { table, ev } of pending) this.triggerChange(table, ev);
    } catch {
      // Some are still orphaned. NOW it is worth isolating, because only the ones
      // that individually fail go back on the queue.
      for (const held of pending) {
        const { table, ev } = held;
        try {
          await this.transaction(async (txExec) => {
            await this.dialect.deferForeignKeys(txExec);
            await this.applyEvent(table, ev, txExec);
            await this.deleteHeld([held], txExec);
          });
          applied++;
          this.triggerChange(table, ev);
        } catch (e) {
          if (foreignKeyFailureKind(e) === 'missing-parent') {
            this.fkHeld.push(held);            // still waiting; try again next batch
            if (held.id != null) {
              try { await this.run(`UPDATE _zebridge_inbox SET attempts = attempts + 1 WHERE id = ?`, held.id); } catch { /* best effort */ }
            }
          } else {
            this.appendLog('CDC', `DROPPED held ${ev?.operation} on ${table}: ${e}`, 'ERROR');
            await this.deleteHeld([held]);
          }
        }
      }
    }
    if (applied) {
      this.appendLog('SYS', `${streamName}: ${applied} held event(s) applied once their parent arrived` +
        (this.fkHeld.length ? `, ${this.fkHeld.length} still waiting` : ''), 'INFO');
    }
  }

  // ── CDC apply ──────────────────────────────────────────────────────────────

  /// `ev.optimistic` marks a synthetic event from our own mutate(): lsn is a sentinel
  /// so the gate never blocks it, and neither the echo-pop nor resume bookkeeping run.
  private async applyEvent(table: string, ev: any, exec: Exec = this.run, seed = false, attempt?: MeasureAttempt) {
    const state = this.syncedTables.get(table);
    if (!state || !ev?.data) return;
    // §10et: a table still waiting for its chain holds its events — applying them to
    // an unseeded table diverges silently, dropping them loses them for good. The
    // hold is the FK inbox with its own reason; the late seed replays it, and the
    // seed gate then drops what the chain already carries. Bounded: past the cap the
    // events are dropped and said, and the table needs a reconnect once its chain
    // exists — a table this busy with no chain for this long is a misconfiguration.
    if (state.unseeded && !seed) {
      const held = (state.unseededHeld ?? 0) + 1;
      state.unseededHeld = held;
      if (held <= UNSEEDED_HOLD_MAX) { await this.holdEvent(table, ev, 'unseeded', attempt); return; }
      attempt?.failed(ev);
      if (held === UNSEEDED_HOLD_MAX + 1) this.appendLog('SYS', `${table}: ${UNSEEDED_HOLD_MAX} events held while waiting for a chain that never came — dropping further events; reconnect once the producer has built one`, 'ERROR');
      return;
    }

    // Strictly `<`, never `<=`: the first post-snapshot commit carries the watermark
    // LSN itself — skipping it loses exactly one row per snapshot (measured).
    // Re-applying is free (the upsert converges); skipping is permanent.
    //
    // ⚠️ `seed` bypasses the gate entirely. The schema descriptor is re-published at
    // every bridge restart with the CURRENT WAL LSN, so on a fresh replica
    // `state.lsn` starts far ahead of any stored snapshot's descriptor LSN — and
    // this line silently dropped EVERY snapshot-replayed row while the replay
    // counter kept counting ("3 rows applied", table empty — found live in the
    // enrollment demo; chain seeding survived only because it writes outside this
    // path). Seeding IS the baseline: it must land unconditionally, and the
    // caller re-anchors state.lsn to the snapshot's own watermark afterwards.
    // The decision is core.seedGateDrops — findings 7 and 10 as one pure rule
    // (seq-primary because commit-ordered; strict-< lsn fallback anchored only
    // by a seed), pinned executable in fixtures/core-fixtures.json.
    if (!seed && seedGateDrops(ev, state)) return;
    // The applied frontier (fence.ts): a fence rewound this route for ANOTHER table, and this
    // one already holds everything up to its frontier.
    if (!seed && frontierDrops(ev, this.frontiers.get(table))) { this.frontierGated++; return; }

    this.feedFloor(state, ev);

    // PROTOCOL §7.5, decided by core.tombstoned: an INSERT/UPDATE that carries the
    // tombstone set is the delete — the reap that follows is never forwarded, so this
    // is the replica's only chance to drop the row. Applies to our own optimistic
    // soft delete too, exactly as the server's echo of it would.
    // ⚠️ This was stored (`state.tombstoneColumn`) and never read: every replica held
    // its tombstones until a full re-seed (measured in libzb: 134 reaped rows kept).
    const op = tombstoned(state.tombstoneColumn, ev.data) ? 'DELETE' : ev.operation;

    // A LOCAL write's payload carries JS values (arrays, objects); a PostgreSQL engine
    // must bind arrays as array literals — core.pgEngineValues — or `text[]` refuses
    // the JSON form (measured: `malformed array literal` on every optimistic INS on
    // PGlite, the row appearing only with the echo). CDC events already carry
    // PostgreSQL's text forms and are left alone; SQLite stores either as text.
    if (ev.optimistic && this.dialect.name === 'postgres') ev = { ...ev, data: pgEngineValues(ev.data) };
    // §10ey: a CDC event carries an array as JSON text; a PostgreSQL engine binds the literal.
    if (!ev.optimistic && this.dialect.name === 'postgres' && state.arrayCols?.length) ev = { ...ev, data: pgArrayValues(ev.data, state.arrayCols) };
    // §10fg: a pgvector/bit column's bytes, local or CDC, bind as the text form on PostgreSQL.
    if (this.dialect.name === 'postgres' && state.vecCols?.length) ev = { ...ev, data: pgVectorValues(ev.data, state.vecCols) };

    if (op === 'INSERT' || op === 'UPDATE') {
      const keys = Object.keys(ev.data);
      const unknown = keys.filter((k) => !state.columns.includes(k) && !k.startsWith('old.'));
      if (unknown.length) {
        // Durable, in the inbox (CLIENTS.md, §10de): a host killed while a migration
        // is in flight must not lose the rows that arrived in the new shape.
        await this.holdEvent(table, ev, `unknown column(s) [${unknown.join(', ')}]`, attempt);
        this.appendLog('CDC', `Holding ${op} on ${table}: unknown column(s) [${unknown.join(', ')}] — awaiting schema newer than lsn ${state.lsn}`, 'HOLD');
        this.scheduleRecount();
        return;
      }

      // The statements come from core.ts (§10s 2a): the key-change delete
      // (a changed PK arrives as an UPDATE with old.* — measured: the row
      // lived under both keys) and the idempotent upsert, fixture-pinned.
      const kc = planKeyChange(table, state.pkCols, ev.data);
      if (kc) {
        try {
          await exec(kc.sql, ...kc.params);
          this.appendLog('CDC', `Key change on ${table}: ${JSON.stringify(kc.oldKey)} → ${JSON.stringify(kc.newKey)}`, 'REKEY');
        } catch (err) {
          this.appendLog('SQLITE', `Key-change cleanup on ${table} failed: ${err}`, 'ERROR');
        }
      }

      // An UPDATE for a row that is HERE is applied as an UPDATE — only the columns
      // sent — never as the upsert, whose INSERT arm fails NOT NULL on a partial
      // payload before the conflict is resolved (core.planUpdate's comment; measured
      // on every browser `UP` and in libzb's soak). A row that is not here (or an
      // INSERT) takes the upsert as before. The existence probe is core.planExists.
      let step: SqlStep | null = null;
      if (op === 'UPDATE' && !kc) {
        const ex = planExists(table, state.pkCols, ev.data);
        if (ex) {
          try {
            const hit = await exec(ex.sql, ...ex.params);
            if (Array.isArray(hit) && hit.length) step = planUpdate(table, state.pkCols, ev.data);
          } catch { /* probe failed — the upsert path answers */ }
        }
      }
      const up = step ?? planUpsert(table, state.pkCols, ev.data);
      try {
        await exec(up.sql, ...up.params);
      } catch (err) {
        this.appendLog('SQLITE', `${step ? 'UPDATE' : 'UPSERT'} on ${table} failed: ${err}`, 'ERROR');
      }
    } else if (op === 'DELETE') {
      // core.planDelete: null on a partial composite key — deleting on it would
      // match MORE rows than PostgreSQL did.
      const del = planDelete(table, state.pkCols, ev.data);
      if (del) {
        try {
          await exec(del.sql, ...del.params);
        } catch (err) {
          this.appendLog('SQLITE', `DELETE on ${table} failed: ${err}`, 'ERROR');
        }
      }
      // §10dg: whatever was HELD for this key (an INSERT waiting for its parent) must
      // not replay after the row's DELETE went by — it would resurrect the row.
      await this.pruneInboxKey(exec, table, state.pkCols, ev.data);
    }

    await this.afterApplied(table, state, ev, exec);
  }

  /// §10q: the HLC floor, fed from every arriving row's version column — observed
  /// remote versions, never our own optimistic stamps. Per event, on both apply paths.
  private feedFloor(state: TableState, ev: any) {
    if (!ev.optimistic && state.versionColumn) {
      const seen = ev.data?.[state.versionColumn];
      if (typeof seen === 'string') {
        this.hlcFloor = maxVersion(this.hlcFloor, normalizeVersion(pgTsToWire(seen)));
      }
    }
  }

  /// What follows a row's landing, whichever statement landed it: the echo pops the
  /// outbox entry, a held edit may have its winner, the global position advances.
  private async afterApplied(table: string, state: TableState, ev: any, exec: Exec) {
    // An authoritative row for a key: my overlays on it are gone, and the confirmed row kept beneath them.
    if (!ev.optimistic && this.confirmedBase?.size && state.pkCols.length) {
      this.confirmedBase.delete(`${table}\u0000${state.pkCols.map((c) => String(ev.data?.[c])).join('|')}`);
    }
    this.confirmEcho(table, state, ev);
    // §10do: a row arriving may be the winner a held edit waits for.
    if (!ev.optimistic && this.rebase.size) this.scheduleRebase();
    if (!ev.optimistic) await this.advanceGlobal(exec, ev.lsn ?? 0, ev.stream ?? '', ev.seq ?? 0);
  }

  /// The echo is the success signal: the CDC row that carries OUR stamp pops the
  /// outbox entry. §10dt: it must be our stamp, not merely our key — a queued
  /// offline write met another client's row on the same key arriving in the
  /// reconnect catch-up, was "confirmed" by it, and was dropped unsent (or, when the
  /// flush won the race, judged stale with no outbox row left to rebase from).
  private confirmEcho(table: string, state: TableState, ev: any) {
    if (ev.optimistic || !state.pkCols.length || !this.pendingWrites.size) return;
    const echoedKey = state.pkCols.map((c) => String(ev.data?.[c])).join('|');
    const echoedVersion = state.versionColumn ? normalizeVersion(String(ev.data?.[state.versionColumn] ?? '')) : null;
    for (const [msgId, w] of this.pendingWrites) {
      if (w.table !== table || String(w.id) !== echoedKey) continue;
      if (w.version && echoedVersion && normalizeVersion(w.version) !== echoedVersion) continue; // someone else's row on our key
      this.pendingWrites.delete(msgId);
      void this.settleByEcho(msgId, w);
      this.appendLog(table, `confirmed by CDC echo after ${Date.now() - w.at}ms`, 'CONFIRMED');
    }
  }

  /// The echo stops retransmission; it does not complete the write for the app. Its
  /// verdict may still be on its way: the write's description is kept until that verdict
  /// reads it, so onVerdict fires once whichever comes first. Dropping both here lost the
  /// callback when the echo won (shared_record: alice's accepted doc write, then a
  /// 60-second settle timeout).
  private async settleByEcho(msgId: string, w: { table: string; version?: string | null }) {
    const write = await this.outboxWrite(msgId, w);
    if (write && !this.verdictReported?.has(msgId) && !this.verdictInFlight?.has(msgId)) (this.echoedWrites ??= new Map()).set(msgId, write);
    await this.outboxDrop(msgId);
  }

  /// The ID's definitive outcome was handled to the end in this session; bounded.
  private markReported(msgId: string) {
    const done = (this.verdictReported ??= new Set());
    done.add(msgId);
    if (done.size > 10_000) done.delete(done.values().next().value!);
  }

  /// The global lsn and the stream's seq, persisted when either moves forward. Once
  /// per event on the per-event path; once per statement, with the maxima, on the
  /// bulk path — the same monotonic result.
  private async advanceGlobal(exec: Exec, lsn: number, stream: string, seq: number) {
    const streamSeq = stream ? (this.globalSyncState.seq[stream] ?? 0) : 0;
    if (!(lsn > this.globalSyncState.lsn || seq > streamSeq)) return;
    this.globalSyncState.lsn = Math.max(this.globalSyncState.lsn, lsn);
    if (stream) this.globalSyncState.seq[stream] = Math.max(streamSeq, seq);
    try {
      await exec(`UPDATE _zebridge_sync SET global_last_lsn = ? WHERE id = 1`, this.globalSyncState.lsn);
      if (stream) {
        await exec(
          `INSERT INTO _zebridge_stream_seq (stream, last_seq) VALUES (?, ?)
           ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq`,
          stream, this.globalSyncState.seq[stream],
        );
        await this.spendFrontiers(stream, this.globalSyncState.seq[stream], exec);
      }
    } catch (e) {
      this.appendLog('SQLITE', `Failed to update sync state: ${e}`, 'ERROR');
    }
  }

  /// §10hc: a CDC batch through the planner — `core.planCdcBulk` cuts it into
  /// segments; a `bulk` one is ONE json_each upsert for a run of eligible events, a
  /// `single` takes `applyEvent`, a `drop` is gated. A bulk statement that fails is
  /// replayed event by event through `applyEvent` (a hold or a bad row costs a retry,
  /// never a silent drop) and counted as a fallback. The per-event duties — HLC floor,
  /// echo, rebase — still run for every bulked event; the position advances once per
  /// statement with the maxima.
  private async applyBatchPlanned(toApply: { table: string; ev: any }[], exec: Exec, attempt?: MeasureAttempt) {
    const tables: Record<string, CdcBulkTable> = {};
    for (const { table } of toApply) {
      if (tables[table]) continue;
      const st = this.syncedTables.get(table);
      if (st) tables[table] = { columns: st.columns, pkCols: st.pkCols, tombstoneColumn: st.tombstoneColumn, unseeded: st.unseeded, anchor: st, blobCols: st.blobCols };
    }
    const segments = planCdcBulk(this.dialect.name, tables, toApply.map(({ table, ev }) => {
      // An event the table's applied frontier covers goes to the planner without its data: a
      // `drop`, like a gated one (the position still counts it).
      const covered = frontierDrops(ev ?? {}, this.frontiers.get(table));
      if (covered) this.frontierGated++;
      return { table, operation: ev?.operation, data: covered ? null : ev?.data, seq: ev?.seq, stream: ev?.stream, lsn: ev?.lsn, optimistic: ev?.optimistic };
    }));
    for (const seg of segments) {
      if (seg.kind === 'single') {
        this.bulkStats.single++;
        await this.applyEvent(toApply[seg.event].table, toApply[seg.event].ev, exec, false, attempt);
      } else if (seg.kind === 'bulk') {
        const state = this.syncedTables.get(seg.table)!;
        try {
          if (this.config.bulkStatement === 'json_each') {
            await exec(seg.sql, JSON.stringify(seg.rows));
          } else {
            // One prepared statement, one row per call: the segment already proved every
            // row whole-tuple and known-column, so no probe and no per-event decision.
            const sql = chainUpsertSql(seg.table, seg.cols, state.pkCols, null);
            for (const row of seg.rows) await exec(sql, ...row.map(cdcValue));
          }
        } catch (err) {
          this.bulkStats.fallbacks++;
          this.appendLog('CDC', `bulk upsert of ${seg.rows.length} row(s) on ${seg.table} failed: ${err} — applying them one at a time`, 'WARNING');
          for (const i of seg.events) await this.applyEvent(toApply[i].table, toApply[i].ev, exec, false, attempt);
          continue;
        }
        this.bulkStats.statements++;
        this.bulkStats.bulked += seg.events.length;
        let lsn = 0, seq = 0, stream = '';
        for (const i of seg.events) {
          const ev = toApply[i].ev;
          this.feedFloor(state, ev);
          this.confirmEcho(seg.table, state, ev);
          lsn = Math.max(lsn, ev.lsn ?? 0); seq = Math.max(seq, ev.seq ?? 0); stream = ev.stream ?? stream;
        }
        if (this.rebase.size) this.scheduleRebase();
        await this.advanceGlobal(exec, lsn, stream, seq);
      }
    }
  }

  // ── stream/tenant routing ──────────────────────────────────────────────────

  /// One stream per tenant plus the shared public one — the stream NAME is the read
  /// boundary (a filter_subject is reader-chosen, not a permission).
  private cdcStreams(): string[] {
    const cfg = this.config.grammar.cdc_streams;
    if (!cfg) return [this.config.grammar.streams.cdc];
    // $KV.tenants is the runtime truth, NOT grammar.json's tenant list: a tenant
    // born after the file was written (dyntenant) has real streams the client must
    // read. The one tenant with no stream of its own is the OPEN tenant — checked
    // explicitly (the old list-membership gate silently ignored dynamic tenants;
    // its original job was only to avoid the CDC__DEFAULT ghost stream).
    const open = this.config.grammar.open_tenant || '_default';
    if (!this.tenantValue || this.tenantValue === open) return [cfg.public];
    return [`${cfg.tenant_prefix}${this.tenantValue}`, cfg.public];
  }

  /// §10gm: the subjects this client wants on `streamName` — one per followed table
  /// (`cdc.<table>.>` public, `cdc.<tenant>.<table>.>` tenant-scoped). Efficiency, NOT a
  /// boundary: the stream stays the ACL (a filter is reader-chosen, so it can only ever
  /// narrow what this reader pulls). Without it a client that follows one table still
  /// downloads every table of the stream and drops the rest — the whole feed, on a phone.
  /// Empty (no schema yet) means no filter, the behaviour before.
  private cdcFilters(streamName: string): string[] {
    const cfg = this.config.grammar.cdc_streams;
    const prefix = this.config.grammar.subjects?.cdc_prefix ?? 'cdc';
    const open = this.config.grammar.open_tenant || '_default';
    const out = new Set<string>();
    for (const [table, st] of this.syncedTables) {
      if (this.ondemandSet.has(table)) continue; // §10hn: nothing is tailed for it
      if (!st.tenantColumn) {
        if (!cfg || streamName === cfg.public) out.add(`${prefix}.${table}.>`);
        continue;
      }
      const tenant = this.tenantValue;
      if (tenant && this.cdcStreamForTenant(tenant) === streamName) out.add(`${prefix}.${tenant}.${table}.>`);
      if (!cfg || streamName === cfg.public) out.add(`${prefix}.${open}.${table}.>`);
    }
    return [...out];
  }

  private cdcStreamForTenant(tenant: string): string {
    const cfg = this.config.grammar.cdc_streams;
    if (!cfg) return this.config.grammar.streams.cdc;
    const open = this.config.grammar.open_tenant || '_default';
    if (!tenant || tenant === open) return cfg.public;
    return `${cfg.tenant_prefix}${tenant}`;
  }

  /// The tenant token THIS table's descriptors/manifests live under: the client's own
  /// tenant when the table is tenant-scoped, the open tenant otherwise — so every
  /// principal converges on one shared entry for tenant-agnostic tables.
  ///
  /// ⚠️ Returns null when the table is tenant-scoped and this principal resolved NO
  /// tenant. That is a real state, not an error: `resolveTenant()` already logs
  /// "No tenant mapping for 'x' — public-only reads" when `$KV.tenants.<principal>`
  /// is absent. What used to happen next was that this returned `''` anyway, and the
  /// caller built the manifest key `'' + '.' + table` — so NATS refused
  /// `.counter_tenant` and the client reported `chain manifest unreadable: invalid
  /// key`, blaming the key syntax for a missing roster entry. Measured against the
  /// compose stack 2026-08-28, where `zebridge_user_tenants` is empty.
  private effectiveTenantFor(table: string): string | null {
    const state = this.syncedTables.get(table);
    if (state?.tenantColumn) return this.tenantValue || null;
    return this.config.grammar.open_tenant || this.tenantValue || null;
  }

  /// zstd frame magic: 28 B5 2F FD. Our msgpack docs always start with a map
  /// marker, so the sniff is unambiguous and needs no wire-format field.
  /// §10eh: a chain object is read as its chunk MESSAGES, with a pull consumer.
  ///
  /// The object store's own reader (`getBlob`, `get`) is an ordered PUSH consumer
  /// with flow control, and it stops after exactly 2 MiB — sixteen 128 KiB chunks —
  /// on @nats-io/obj 3.4.0 against nats-server 2.14: the server's flow-control
  /// request is never answered and the 107 remaining chunks stay pending for ever,
  /// at 0% CPU. Measured in isolation, in Node, three ways (getBlob, the stream
  /// reader, two concurrent reads): a 15 MiB full never arrived; an 11-chunk delta
  /// read in 26 ms. A client on a table whose full passes 2 MiB therefore never
  /// seeded. The chunks are plain messages on `$O.<bucket>.C.<nuid>` in stream
  /// `OBJ_<bucket>`, so they are fetched the way CDC is — a pull consumer, nothing
  /// to answer — 16 MB in 78 ms, and the object's own SHA-256 is checked before
  /// the blob is trusted.
  private async objectBlob(os: any, bucket: string, name: string, jsOpts: JetStreamOpts | undefined = this.jsOpts()): Promise<Uint8Array | null> {
    const info = await os.info(name);
    if (!info || info.deleted) return null;
    if (!info.chunks || !info.size) return new Uint8Array(0);
    const js = this.transport.jetstream(this.nc!, jsOpts);
    const c = await js.consumers.get(`OBJ_${bucket}`, { filter_subjects: [`$O.${bucket}.C.${info.nuid}`] });
    const parts: Uint8Array[] = [];
    let got = 0;
    // §10hn: a BOUNDED pull per request, never the whole object. One request for all
    // 730 chunks of a 95 MB object had the server write 95 MB at once; while this
    // process was applying rows through a synchronous driver the socket went
    // undrained, the server's per-connection pending passed its 64 MB limit and it
    // cut the connection as a slow consumer ("Slow Consumer Detected: MaxPending of
    // 67108864 Exceeded", twice, once per attempt — 24 then 18 chunks arrived). libzb
    // reads a few chunks per pull (§10fh) and never trips it; so does this now: at
    // most OBJECT_PULL_CHUNKS in flight per request, the loop asking again until the last chunk.
    while (parts.length < info.chunks) {
      const remaining = info.chunks - parts.length;
      const iter = await c.fetch({ max_messages: Math.min(remaining, OBJECT_PULL_CHUNKS), expires: 30_000 });
      let inThis = 0;
      for await (const m of iter) {
        parts.push(m.data); got += m.data.length; inThis++;
        if (parts.length >= info.chunks) break;
      }
      // Stopped, not abandoned: an open fetch keeps its subscription until `expires`,
      // and `close()` drains every subscription — eleven seeds left `close()` waiting
      // most of a minute each (measured: a fifteen-minute hang-up on the wall).
      try { (iter as any).stop(); } catch { /* already ended */ }
      if (inThis === 0) break; // the request expired empty: the chunks are not there
    }
    if (parts.length !== info.chunks || got !== info.size) {
      throw new Error(`object ${name}: ${parts.length}/${info.chunks} chunks, ${got}/${info.size} bytes`);
    }
    const blob = new Uint8Array(got);
    let o = 0;
    for (const p of parts) { blob.set(p, o); o += p.length; }
    if (info.digest) {
      const want = String(info.digest).replace(/^SHA-256=/, '').replace(/=+$/, '');
      const hash = new Uint8Array(await crypto.subtle.digest('SHA-256', blob));
      const have = btoa(String.fromCharCode(...hash)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
      if (want !== have) throw new Error(`object ${name}: digest mismatch (${have} vs ${want})`);
    }
    return blob;
  }

  /// §10ix: the streaming read of one plan step, or null for the buffered path —
  /// streaming off, or an object under `seedStreamingAboveBytes`.
  private async chainStepStream(os: any, bucket: string, step: PlanStep, table: string, jsOpts: JetStreamOpts | undefined):
    Promise<{ columns: string[]; nrows: number; size: number; batches: AsyncIterable<any[][]>; tail: () => Promise<Record<string, any>> } | null> {
    if (!this.config.seedStreaming) return null;
    let src: { info: any; chunks: AsyncIterable<Uint8Array> } | null;
    try { src = await this.objectChunkStream(os, bucket, step.name, jsOpts); }
    catch (e) { this.appendLog('SYS', `${table}: chain object ${step.name} unreadable: ${e}`, 'ERROR'); return null; }
    if (!src || src.info.size < (this.config.seedStreamingAboveBytes ?? 8 * 1024 * 1024)) return null;
    const doc = await this.chainDocStream(await this.zstdChunkStream(src.chunks));
    if (!doc) { this.appendLog('SYS', `${table}: chain object ${step.name} is not a chain document — buffered path`, 'WARN'); return null; }
    return { ...doc, size: src.info.size };
  }

  /// §10ix: the object's chunks as they arrive, never assembled — the same bounded pull
  /// as `objectBlob` (OBJECT_PULL_CHUNKS in flight), each chunk yielded and dropped. The digest is
  /// folded in as they pass and checked after the last one: a truncated or corrupt
  /// object still fails, only after the rows it did deliver, and before the watermark
  /// that would have made them count.
  private async objectChunkStream(os: any, bucket: string, name: string, jsOpts: JetStreamOpts | undefined): Promise<{ info: any; chunks: AsyncIterable<Uint8Array> } | null> {
    const info = await os.info(name);
    if (!info || info.deleted) return null;
    const self = this;
    async function* chunks(): AsyncGenerator<Uint8Array> {
      if (!info.chunks || !info.size) return;
      const hash = await self.streamingSha256();
      const js = self.transport.jetstream(self.nc!, jsOpts);
      const c = await js.consumers.get(`OBJ_${bucket}`, { filter_subjects: [`$O.${bucket}.C.${info.nuid}`] });
      let n = 0, got = 0;
      while (n < info.chunks) {
        const iter = await c.fetch({ max_messages: Math.min(info.chunks - n, OBJECT_PULL_CHUNKS), expires: 30_000 });
        let inThis = 0;
        for await (const m of iter) {
          hash.update(m.data); got += m.data.length; n++; inThis++;
          yield m.data;
          if (n >= info.chunks) break;
        }
        try { (iter as any).stop(); } catch { /* already ended */ }
        if (inThis === 0) break;
      }
      if (n !== info.chunks || got !== info.size) throw new Error(`object ${name}: ${n}/${info.chunks} chunks, ${got}/${info.size} bytes`);
      if (info.digest) {
        const want = String(info.digest).replace(/^SHA-256=/, '').replace(/=+$/, '');
        const have = hash.base64().replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
        if (want !== have) throw new Error(`object ${name}: digest mismatch (${have} vs ${want})`);
      }
    }
    return { info, chunks: chunks() };
  }

  /// §10ix: an incremental SHA-256 for the streaming read — the platform's (node:crypto on
  /// Node), else js-sha256 (pure, 11 KB): Web Crypto's `digest` takes one buffer, and a
  /// streamed object is never one buffer.
  private async streamingSha256(): Promise<{ update(c: Uint8Array): void; base64(): string }> {
    if (this.platform.sha256Stream) return this.platform.sha256Stream();
    const { sha256 } = await import('js-sha256');
    const h = sha256.create();
    return {
      update: (c) => h.update(c),
      base64: () => btoa(String.fromCharCode(...new Uint8Array(h.arrayBuffer()))),
    };
  }

  /// §10ix: inflate a chunk stream as it flows — the platform's decoder, or the config's
  /// override. The first chunk is sniffed for the frame magic (§10w): an object that is
  /// not zstd passes through untouched.
  private async zstdChunkStream(chunks: AsyncIterable<Uint8Array>): Promise<AsyncIterable<Uint8Array>> {
    const it = chunks[Symbol.asyncIterator]();
    const first = await it.next();
    if (first.done) return (async function* () {})();
    const head = first.value;
    const rest = (async function* () { yield head; for (;;) { const r = await it.next(); if (r.done) return; yield r.value; } })();
    if (!(head.length >= 4 && head[0] === 0x28 && head[1] === 0xb5 && head[2] === 0x2f && head[3] === 0xfd)) return rest;
    return (this.config.zstdDecompressStream ?? this.platform.zstdDecompressStream)(rest);
  }


  /// §10ix: a chain document decoded as it arrives. `core.parseChainHead` reads the
  /// head by hand; every value after it is one row, handed out in batches (all the
  /// complete rows of the bytes at hand); after `nrows` of them come the tail's
  /// alternating keys and values —
  /// gen, kind, cutoff, version_column, prev_cutoff. Null if the stream ends inside
  /// the head: not a chain document.
  private async chainDocStream(inflated: AsyncIterable<Uint8Array>):
    Promise<{ columns: string[]; nrows: number; batches: AsyncIterable<any[][]>; tail: () => Promise<Record<string, any>> } | null> {
    const it = inflated[Symbol.asyncIterator]();
    let buf = new Uint8Array(0);
    let head = parseChainHead(buf);
    while (!head) {
      const r = await it.next();
      if (r.done) return null;
      const nb = new Uint8Array(buf.length + r.value.length); nb.set(buf); nb.set(r.value, buf.length); buf = nb;
      head = parseChainHead(buf);
    }
    const nrows = head.nrows;
    // §10ja: rows in BATCHES — every complete row of the bytes at hand, found by
    // `msgpackScan` and decoded in one call; the partial one waits for the next chunk.
    // One await per chunk, not one per row through `decodeMultiStream`'s generator
    // and ours (on Hermes, Babel's async generators: most of the seed's JS time).
    let carry: Uint8Array = buf.subarray(head.offset);
    let got = 0;
    const BATCH = 4096;
    const batches = (async function* () {
      for (;;) {
        // At most BATCH rows decoded at a time: a large chunk (the browser's are) is
        // several batches, or its rows all sit decoded beside the window being built
        // (measured: Chrome's heap peak 220 → 333 MB uncapped).
        while (carry.length && got < nrows) {
          const { end, count } = msgpackScan(carry, Math.min(nrows - got, BATCH));
          if (!count) break;
          const rows = msgpackDecodeScanned(decode, carry, end, count) as any[][];
          got += count;
          carry = carry.subarray(end);
          yield rows;
        }
        if (got >= nrows) return;
        const r = await it.next();
        if (r.done) throw new Error(`chain document ended after ${got} of ${nrows} rows`);
        if (carry.length) {
          const nb = new Uint8Array(carry.length + r.value.length); nb.set(carry); nb.set(r.value, carry.length); carry = nb;
        } else carry = r.value;
      }
    })();
    // After the rows, the tail's alternating keys and values — small; read to the end of
    // the stream, which is also what checks the object's digest.
    const tail = async () => {
      const parts: Uint8Array[] = [carry];
      for (;;) { const r = await it.next(); if (r.done) break; parts.push(r.value); }
      const all = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
      let o = 0; for (const p of parts) { all.set(p, o); o += p.length; }
      const vals = [...decodeMulti(all)];
      const t: Record<string, any> = {};
      for (let i = 0; i + 1 < vals.length; i += 2) t[String(vals[i])] = vals[i + 1];
      return t;
    };
    return { columns: head.columns, nrows, batches, tail };
  }

  /// A zstd frame (sniffed by its magic, §10w) inflated by the platform or the override;
  /// anything else as it is.
  private async maybeZstd(b: Uint8Array): Promise<Uint8Array> {
    if (b.length < 4 || b[0] !== 0x28 || b[1] !== 0xb5 || b[2] !== 0x2f || b[3] !== 0xfd) return b;
    return await (this.config.zstdDecompress ?? this.platform.zstdDecompress)(b);
  }


  /// grammar.json's `subjects.mutation_ack_prefix` — the verdict channel's first token.
  /// The literal `mutation_ack` used to be hardcoded at three sites; the grammar is
  /// the single source (PROTOCOL §1) and a renamed prefix must not leave a client
  /// listening on a subject nobody publishes.
  private ackPrefix(): string {
    return this.config.grammar.subjects?.mutation_ack_prefix ?? 'mutation_ack';
  }

  private async resolveTenant() {
    if (!this.nc) return;
    try {
      // allow_direct must be explicit: the KV open never asks the server, and the
      // grant covers ONLY the per-key Direct Get path (measured — App.tsx history).
      const kv = await this.transport.kv(this.nc, this.config.grammar.kv.tenants, { allow_direct: true }, this.jsOpts());
      const entry = await kv.get(this.config.principal);
      // A purged mapping (§10ce: `bridge --revoke`) leaves a DEL marker with an empty
      // value — that is "no mapping", not a tenant named "".
      if (entry && entry.operation === 'PUT' && entry.value?.length) {
        let val: string;
        try { val = decode(entry.value) as string; } catch { val = td.decode(entry.value); }
        if (typeof val !== 'string') val = td.decode(entry.value);
        // §10fn: the value is the roster's SET, a JSON array (`["acme","globex"]`). This
        // client follows ONE tenant — the first — and
        // says so when there are more (membership across several is libzb's, parity
        // queued).
        const list = parseTenantList(val);
        this.tenantValue = list[0] ?? '';
        if (list.length > 1) {
          this.appendLog('SYS', `'${this.config.principal}' belongs to ${list.length} tenants (${list.join(', ')}) — this client follows '${list[0]}' only`, 'WARN');
        } else {
          this.appendLog('SYS', `Resolved tenant for '${this.config.principal}': ${this.tenantValue}`, 'INFO');
        }
      } else {
        this.tenantValue = '';
        this.appendLog('SYS', `No tenant mapping for '${this.config.principal}' — revoked, or never enrolled: tenant-scoped tables are not followed, public tables are`, 'WARN');
      }
    } catch (err) {
      this.appendLog('SYS', `Tenant resolution failed: ${err}`, 'ERROR');
    }
  }

  // ── seeding: generations first, snapshots as the fallback ─────────────────

  /// Seed or catch up one table from its delta-generation chain (NOTES.md §1.13).
  /// Returns false for every "not this way" outcome — the snapshot path is the
  /// fallback, not an error handler. Watermark-based walk, guarded upsert, one
  /// manifest re-read on a 404 mid-walk. Never throws.
  /// One seed per table at a time (§10eh). Two paths ask for a seed on a fresh
  /// replica: the schema watch, as it creates each table at first sight, and the
  /// stream subscribe, for every table without a watermark — and both asked at once,
  /// so every table was seeded TWICE concurrently (every `Seeded` line printed twice;
  /// two readers on the same 15 MiB object). A second request joins the one in flight.
  private seedInFlight = new Map<string, { promise: Promise<boolean>; run: SeedRun }>();
  /// The delta path on a (re)connect for one stream with no gap (core.streamDeltaChoice):
  /// whether every table depending on it may take its chain instead of the CDC to the
  /// chain's cut, decided before any object is fetched — the same on the hub or a seeding
  /// leaf. Returns the tables to seed by choice; any failure answers "replay the CDC".
  private async streamDeltas(stream: string, routes: Record<string, { route: string; sharedRoute?: string; seeded: boolean }>, gapped: string[], jsm: any): Promise<string[]> {
    const GEN = this.config.grammar.generations;
    if (!GEN || !this.nc) return [];
    try {
      const stored = this.globalSyncState.seq[stream] ?? 0;
      const info = await jsm.streams.info(stream);
      const created = String((info as { created?: unknown }).created ?? '');
      const whys: string[] = [];
      const deps: { table: string; man: any; watermark: string | null; src: JetStreamOpts | undefined }[] = [];
      let lowCut: number | null = null;
      for (const [table, r] of Object.entries(routes)) {
        const own = r.route === stream;
        const shared = !own && r.sharedRoute === stream;
        if (!own && !shared) continue;
        // Seeded this pass anyway (never seeded, or its other route gapped): the gap
        // path owns it; keep the stream on its CDC.
        if (!r.seeded || gapped.includes(r.route) || (r.sharedRoute && gapped.includes(r.sharedRoute))) { whys.push('seeding-owed'); continue; }
        const tenant = this.effectiveTenantFor(table);
        if (tenant === null) { whys.push('no-cut'); continue; }
        const read = async (opts: JetStreamOpts | undefined) => {
          const kv = await this.transport.kv(this.nc!, GEN.kv, { allow_direct: true }, opts);
          const entry = await kv.get(`${tenant}.${table}`);
          return entry && entry.operation === 'PUT' && entry.value?.length ? JSON.parse(td.decode(entry.value)) : null;
        };
        let src = !this.hubOnly.has(r.route) && this.config.seedDomain && this.config.seedDomain !== this.config.jsDomain
          ? { domain: this.config.seedDomain } : this.jsOpts();
        let man = await read(src).catch(() => null);
        if (!man && src !== this.jsOpts()) { src = this.jsOpts(); man = await read(src); }
        if (!man) { whys.push('no-cut'); continue; }
        const rows: any[] = (await this.run(`SELECT watermark FROM _zebridge_generations WHERE tbl = ?`, table)) ?? [];
        const watermark: string | null = rows[0]?.watermark ?? null;
        // The cut on THIS stream, on the incarnation it was taken on.
        const [kStream, kCreated, kCut] = own ? ['cdc_stream', 'cdc_stream_created', 'cutoff_seq'] : ['shared_cdc_stream', 'shared_cdc_stream_created', 'shared_cutoff_seq'];
        const same = man[kStream] === stream && (!man[kCreated] || !created || man[kCreated] === created);
        const cut = same && typeof man[kCut] === 'number' ? man[kCut] as number : null;
        whys.push(deltaEligible({ manifest: man, watermark, stored, cut }).why);
        if (cut !== null) lowCut = lowCut === null ? cut : Math.min(lowCut, cut);
        deps.push({ table, man, watermark, src });
      }
      // Every dependent must be eligible before any object is fetched.
      let choice = streamDeltaChoice(whys, 0, 1);
      let matched = -1, backlog = -1, deltaBytes = -1;
      if (choice.take) {
        matched = 0;
        for (const f of this.cdcFilters(stream)) {
          const fi = await jsm.streams.info(stream, { subjects_filter: f });
          for (const n of Object.values(fi.state.subjects ?? {})) matched += Number(n);
        }
        backlog = cdcBacklogBytes({ stored, cut: lowCut ?? stored, streamMessages: info.state.messages, streamBytes: info.state.bytes, matchedMessages: matched });
        deltaBytes = 0;
        for (const d of deps) {
          const os = await this.transport.objectStore(this.nc, d.man.bucket, d.src);
          for (const step of planFromManifest(d.man, d.watermark)) {
            const oi = await os.info(step.name);
            if (!oi || oi.deleted) { deltaBytes = -1; break; }
            deltaBytes += oi.size ?? 0;
          }
          if (deltaBytes < 0) break;
        }
        choice = streamDeltaChoice(whys, deltaBytes, backlog);
      }
      this.measure.emit({ m: 'delta_choice', stream, take: choice.take, why: choice.why, dependents: whys.length, stored, cut: lowCut ?? -1, matched_messages: matched, backlog_bytes: backlog, delta_bytes: deltaBytes, t0_ms: Date.now() });
      if (!choice.take) return [];
      this.appendLog('SYS', `${stream}: ${(lowCut ?? stored) - stored} message(s) behind — taking the chains' deltas (${Math.floor(deltaBytes / 1024)} KiB) over the CDC backlog (~${Math.floor(backlog / 1024)} KiB) for [${deps.map((d) => d.table).join(', ')}]`, 'INFO');
      return deps.map((d) => d.table);
    } catch (e) {
      this.appendLog('SYS', `${stream}: delta path not weighed (${e}) — replaying its CDC backlog`, 'INFO');
      return [];
    }
  }

  /// `inPass`: asked from inside a stream pass (subscribeStreamsOnce), which already holds
  /// the stream queue and reopens every tail itself afterwards.
  ///
  /// Who owns the stream queue is settled BEFORE a promise is shared. A seed started by a
  /// poll loop must queue for it (the fence's PREPARE moves cursors and parks routes); a
  /// pass that then joins that seed holds the queue and waits for it, so the seed would wait
  /// behind the very pass that waits for it. The joining pass therefore LENDS its turn: from
  /// the moment it joins, the seed runs its queued work under the pass's turn (the pass does
  /// nothing else for this table while it waits, which is what the queue protects).
  private applyGenerations(table: string, inPass = false): Promise<boolean> {
    const flight = this.seedInFlight.get(table);
    if (flight) {
      if (inPass && !flight.run.inPass) {
        flight.run.joinedByPass = true;
        flight.run.lend?.(); // already queued: start it now, under this pass's turn
      }
      return flight.promise;
    }
    const run: SeedRun = { inPass, joinedByPass: false };
    const promise = this.applyGenerationsOnce(table, run).finally(() => { this.seedInFlight.delete(table); });
    this.seedInFlight.set(table, { promise, run });
    return promise;
  }

  /// `fn` with the stream queue's turn: at once for a pass's seed (it holds the queue) or
  /// one a pass has joined (it lent its turn); otherwise queued, and started early if a pass
  /// joins this seed while it waits.
  private withStreamQueue(run: SeedRun, fn: () => Promise<void>): Promise<void> {
    if (run.inPass || run.joinedByPass) return fn();
    return new Promise<void>((resolve, reject) => {
      let started = false;
      const go = (): Promise<void> => {
        if (started) return Promise.resolve();
        started = true;
        run.lend = undefined;
        return fn().then(resolve, reject);
      };
      run.lend = () => { void go(); };
      void this.changeStreams(go);
    });
  }

  /// §10ja: is a jump in stream sequence past `pos` a hole? Only if the stream no longer
  /// holds the message right after it — a consumer filtered to this client's tables
  /// skips the other tables' messages on a shared tenant stream, and each write to one
  /// of them used to read as a prune and cost a re-seed. Info unreadable → a hole.
  private async prunedAfter(jsm: any, stream: string, pos: number): Promise<boolean> {
    try {
      const first = (await jsm.streams.info(stream))?.state?.first_seq;
      return typeof first !== 'number' || first > pos + 1;
    } catch {
      return true;
    }
  }

  private async applyGenerationsOnce(table: string, run: SeedRun): Promise<boolean> {
    // A route under reconciliation seeds from the hub only: a leaf's copy may be behind
    // the very commit the reconciliation is about.
    const tenant = this.effectiveTenantFor(table);
    const hubOnly = tenant !== null && this.hubOnly.has(this.cdcStreamForTenant(tenant));
    if (!hubOnly && this.config.seedDomain && this.config.seedDomain !== this.config.jsDomain) {
      try {
        if (await this.applyGenerationsFrom(table, { domain: this.config.seedDomain }, run)) return true;
      } catch (e) {
        this.appendLog('SYS', `${table}: leaf seed failed: ${e}`, 'WARNING');
      }
      this.appendLog('SYS', `${table}: leaf chain unavailable or behind — seeding from the hub`, 'INFO');
    }
    return this.applyGenerationsFrom(table, this.jsOpts(), run);
  }

  private async applyGenerationsFrom(table: string, seedOpts: JetStreamOpts | undefined, run: SeedRun): Promise<boolean> {
    const hold: Replacement = { routes: [], fence: false, owed: false };
    try {
      return await this.applyGenerationsFromHeld(table, seedOpts, run, hold);
    } catch (e) {
      // Rows may be half replaced, the anchor gone, the table unseeded and its events held: nothing
      // else will ask again (a poll loop ends on a throw, a pass only notes the failure), and the
      // held events are bounded. The retry loop runs for as long as the table is unseeded.
      if (hold.owed) this.waitForChain(table);
      throw e;
    } finally {
      this.endReplacement(hold, table, run);
    }
  }

  private async applyGenerationsFromHeld(table: string, seedOpts: JetStreamOpts | undefined, run: SeedRun, hold: Replacement): Promise<boolean> {
    const GEN = this.config.grammar.generations;
    if (!GEN || !this.nc) return false;
    const state = this.syncedTables.get(table);
    if (!state || !state.pkCols.length) return false;
    await this.ensureJointRestored();

    const tenantForTable = this.effectiveTenantFor(table);
    if (tenantForTable === null) {
      // Say what is actually wrong. A tenant-scoped table is unreadable by a
      // principal with no tenant — there is no chain to find, and no key that could
      // name one.
      this.appendLog(
        'SYS',
        `${table} is tenant-scoped and '${this.config.principal}' has no tenant ` +
          `(no ${this.config.grammar.kv.tenants}.${this.config.principal} entry) — skipping. ` +
          `Add the principal to zebridge_user_tenants.`,
        'WARN',
      );
      return false;
    }
    const key = `${tenantForTable}.${table}`;
    const readManifest = async (): Promise<any | null> => {
      try {
        const kv = await this.transport.kv(this.nc!, GEN.kv, { allow_direct: true }, seedOpts);
        const entry = await kv.get(key);
        // A swept chain (§10dg) leaves a DEL marker with an empty value: no manifest.
        if (!entry || entry.operation !== 'PUT' || !entry.value?.length) return null;
        return JSON.parse(td.decode(entry.value)); // manifest is JSON
      } catch (e) { this.appendLog('SYS', `${table}: chain manifest unreadable: ${e}`, 'ERROR'); return null; }
    };
    let manifest = await readManifest();
    if (!manifest?.full?.object) return false;
    // §10df: a manifest built BEFORE the re-seed was asked for cannot serve it — seeding
    // from it would record the new epoch over the old data. The descriptor's epoch and
    // the producer's full arrive on independent clocks; false here means "wait", and
    // the caller's loop polls again.
    if ((manifest.seed_epoch ?? 0) < (state.seedEpoch ?? 0)) {
      this.appendLog('SYS', `${table}: chain g${manifest.gen} predates the re-seed (epoch ${manifest.seed_epoch ?? 0} < ${state.seedEpoch}) — waiting for the producer's full`, 'INFO');
      return false;
    }

    // The proofs of ONE manifest, taken again for every manifest this apply reads (the one
    // re-read after a pruned object may be another chain, cut elsewhere).
    //
    // §10ei: a chain older than the STREAM cannot splice — the events between its
    // cutoff and the oldest message the stream still holds are gone, and a replica
    // seeded from it that read on would carry the hole for ever. The contract (§10eg)
    // keeps the age above two cadences so this never happens in a healthy deployment;
    // a size valve, a purge or a too-short age breaks it, and the honest move is to
    // wait for the producer's next generation (the seed loop polls this again).
    // What the seed gate may anchor on: the manifest's cutoff_seq — unless the stream
    // restarted under us and this manifest predates the restart (see `restarted`).
    // `ownReplayable` is what the fence needs: the stream read, on the incarnation the
    // chain was cut on.
    const prove = (man: any): Promise<Proof | null> => this.proveManifest(table, man);
    let proof = await prove(manifest);
    if (!proof) return false;

    let os: any;
    try { os = await this.transport.objectStore(this.nc, manifest.bucket, seedOpts); } catch (e) { this.appendLog('SYS', `${table}: chain bucket ${manifest.bucket} unreachable: ${e}`, 'ERROR'); return false; }
    // ZB_MEASURE: the buffered step's split — fetch, inflate, decode — and its bytes.
    let lastDoc = { bytes: 0, fetch_ms: 0, inflate_ms: 0, decode_ms: 0 };
    const chainT0 = Date.now();
    const fetchDoc = async (name: string): Promise<any | null> => {
      try {
        const t0 = Date.now();
        let blob = await this.objectBlob(os, manifest.bucket, name, seedOpts);
        if (!blob) return null;
        const t1 = Date.now();
        const bytes = blob.length;
        blob = await this.maybeZstd(blob); // §10w: sniffed by magic, mixed chains fine
        const t2 = Date.now();
        const doc = decode(blob) as any; // objects are msgpack
        lastDoc = { bytes, fetch_ms: t1 - t0, inflate_ms: t2 - t1, decode_ms: Date.now() - t2 };
        return doc;
      } catch (e) { this.appendLog('SYS', `${table}: chain object ${name} unreadable: ${e}`, 'ERROR'); return null; }
    };
    const chainTotals = { bytes: 0, fetch_ms: 0, inflate_ms: 0, decode_ms: 0, apply_ms: 0, streamed: 0 };

    let watermark: string | null = null;
    try {
      const r = await this.run(`SELECT watermark FROM _zebridge_generations WHERE tbl = ?`, table);
      watermark = r[0]?.watermark ?? null;
    } catch { /* fresh replica */ }

    // The delta path approved a manifest; this is the one read now (on the leaf, or the
    // hub's on fallback, or again after a pruned object). Checked again before any row
    // changes: a newer base published in between may carry an absence its deltas cannot.
    const stillEligible = async (man: any, wm: string | null): Promise<boolean> => {
      if (!this.applyingByChoice.has(table)) return true;
      const route = this.cdcStreamForTenant(tenantForTable);
      let created = '';
      try { created = String(((await (await this.transport.jetstreamManager(this.nc!, this.jsOpts())).streams.info(route)) as { created?: unknown }).created ?? ''); } catch { /* unknown: not compared */ }
      const same = man?.cdc_stream === route && (!man.cdc_stream_created || !created || man.cdc_stream_created === created);
      const cut = same && typeof man.cutoff_seq === 'number' ? man.cutoff_seq : null;
      const e = deltaEligible({ manifest: man, watermark: wm, stored: this.globalSyncState.seq[route] ?? 0, cut });
      if (!e.take) this.appendLog('SYS', `${table}: the chain read for the delta path is no longer eligible (${e.why}, g${man?.gen}) — nothing applied; the CDC replays`, 'INFO');
      return e.take;
    };
    if (!(await stillEligible(manifest, watermark))) return false;

    // The walk itself is core.planFromManifest; `watermark` is read at call
    // time (it resets to null on a mid-walk manifest re-read).
    const planFrom = (man: any) => planFromManifest(man, watermark);

    const applyPlan = async (plan: PlanStep[]): Promise<number | null> => {
      let applied = 0;
      // The chain's steps put rows back as of ITS cut: a delta's upsert re-inserts a row this
      // table has since deleted (no version rejects an insert onto nothing). Whatever frontier
      // the table held (a fence on another table left it) no longer describes its rows, so the
      // events up to it replay for this table too — dropped BEFORE the first step: a crash
      // between a step and the anchor must not leave rows from the cut under a frontier that
      // skips the events that correct them.
      if (plan.length && this.frontiers.has(table)) await this.dropTableFrontiers(table);
      for (const step of plan) {
        const stepT0 = Date.now();
        const applied0 = applied;
        // §10ix: a large step streams; anything else, or any host that cannot stream,
        // takes the buffered path. Both apply through the same `applyWindow` below.
        const stream = await this.chainStepStream(os, manifest.bucket, step, table, seedOpts);
        const doc = stream ? null : await fetchDoc(step.name);
        const applyT0 = Date.now();
        if (!stream && !doc) return null; // pruned under us — caller re-reads
        const cols: string[] = (stream ? stream.columns : doc.columns) ?? [];
        if (!cols.length || !cols.every((c) => state.columns.includes(c))) {
          this.appendLog('SYS', `Generation ${step.name} for ${table} references columns the local schema lacks — falling back to snapshot`, 'WARNING');
          return null;
        }
        // About to delete the baseline: the replacement becomes durably owed first (a fence's already is),
        // and what the position did during the download is settled with the tails stopped (below).
        if (step.kind === 'full' && !hold.owed && !(await settleBeforeFull(plan))) return null;
        // The streaming path meets the object's version column in the TAIL, after the
        // rows; the manifest's is the same column unless the schema moved under the cut,
        // which the tail check reports.
        const vcol: string = doc?.version_column ?? manifest.version_column;
        // core.chainUpsertSql: version-guarded when the object carries the
        // table's version column — LWW holds during seeding too.
        const q = chainUpsertSql(table, cols, state.pkCols,
          vcol && cols.includes(vcol) ? vcol : null);
        // §7.5 on the seed path: a chain is built from the table as it stands, so it
        // carries every tombstone not yet reaped. Chain rows are positional — resolve
        // the tombstone column to its index once, then decide per row via
        // core.tombstoned on a keyed view of the row.
        const tombIdx = state.tombstoneColumn ? cols.indexOf(state.tombstoneColumn) : -1;
        const pkIdx = state.pkCols.map((c) => cols.indexOf(c));
        // §10ey: chain cells carry arrays as JSON text too; the PostgreSQL engine wants the literal.
        const arrIdx = this.dialect.name === 'postgres' ? (state.arrayCols ?? []).map((c) => cols.indexOf(c)).filter((i) => i >= 0) : [];
        // §10fg: and pgvector/bit cells as their text form.
        const vecIdx = this.dialect.name === 'postgres' ? (state.vecCols ?? []).map((vc) => ({ i: cols.indexOf(vc.name), vc })).filter((x) => x.i >= 0) : [];
        // §10fb (libzb §10ez/§10fa on this side): rows in key order, applied in chunks
        // of one transaction each, and a page cache the seed's b-tree fits in while it
        // lasts. The first chunk of a full runs the DELETE; a kill between chunks is
        // safe by the watermark rule (written after the last chunk, so a restart
        // plans the full again and begins with the DELETE).
        // §10ix: the buffered path sorts the whole table once; the streaming path sorts
        // each window — libzb's trade (client.zig `seed_chunk_rows`): runs of ordered
        // keys instead of one, slower for the b-tree, bounded in memory.
        const keyIdx = pkIdx[0] ?? -1;
        const rows: any[][] = stream ? [] : step.sorted ? doc.rows : sortRowsByKey(doc.rows, keyIdx);
        const chunk = this.config.seedChunkRows ?? 50_000;
        const size = chunk > 0 ? chunk : Math.max(stream ? stream.nrows : rows.length, 1);
        const sqlite = this.dialect.name === 'sqlite';
        // §10fc: on SQLite, a chunk is ONE statement — the live rows as JSON text through
        // json_each. A BLOB column's bytes cross as {"$x": hex} and come back through
        // unhex (chainChunkJson): row by row cost a worker round trip per row in a
        // browser — ~20 s for 16k rows with a PostGIS column (2026-10-05).
        const blobIdx = (state.blobCols ?? []).map((c) => cols.indexOf(c)).filter((i) => i >= 0);
        const bulk = sqlite ? chainBulkSql(table, cols, state.pkCols, vcol && cols.includes(vcol) ? vcol : null, blobIdx) : null;
        // One window, one transaction — the body both paths share.
        const applyWindow = async (win: any[][], first: boolean) => {
            await this.transaction(async (txExec) => {
              // A full replaces the baseline wholesale; the DELETE shares the first
              // chunk's transaction so a crash mid-apply cannot leave an empty table.
              if (step.kind === 'full' && first) await txExec(`DELETE FROM ${table}`);
              const live: any[][] = [];
              for (const row of win) {
                if (tombIdx >= 0 && tombstoned(state.tombstoneColumn, { [cols[tombIdx]]: row[tombIdx] })) {
                  const keyed: Record<string, unknown> = {};
                  state.pkCols.forEach((c, i) => { if (pkIdx[i] >= 0) keyed[c] = row[pkIdx[i]]; });
                  const del = planDelete(table, state.pkCols, keyed);
                  if (del) await txExec(del.sql, ...del.params);
                  continue;
                }
                if (bulk) { live.push(row); continue; }
                const params = chainRowParams(row);
                for (const i of arrIdx) {
                  const v = params[i];
                  if (typeof v === 'string' && v.startsWith('[')) { try { params[i] = pgArrayLiteral(JSON.parse(v)); } catch { /* not JSON: as is */ } }
                }
                for (const { i, vc } of vecIdx) if (isBytes(params[i])) params[i] = vecLiteral(vc.kind, params[i], vc.bits);
                await txExec(q, ...params);
              }
              if (bulk && live.length) await txExec(bulk, chainChunkJson(live, blobIdx));
            });
            if (fenceRun) await this.fault('after_chunk');
        };
        if (sqlite) { try { await this.run('PRAGMA cache_size = -131072'); } catch { /* an adapter that refuses PRAGMA: the default cache */ } }
        try {
          // §10ja: a step the producer cut in key order goes straight in, a window at a
          // time — each window appends to the key index, so staging would only write every
          // row twice (measured: 26 s direct against 34 s staged, 3.66M rows).
          if (stream && sqlite && bulk && step.kind === 'full' && this.storage.spillsTemp === true && !step.sorted) {
            // §10ix: a streamed FULL on SQLite is STAGED, not sorted per window. Sorting
            // each window scattered its inserts across the whole b-tree (measured: 50k
            // windows, 141 s, sys 38.6 s — the scatter is kernel I/O — against 38.9 s
            // buffered; 1M windows, 41.7 s, but 2.4 GB). Here every window is a heap
            // append into a keyless TEMP table, and the real table is filled once,
            // `SELECT … ORDER BY pk`, by SQLite's external sorter — bounded memory,
            // sequential b-tree build, the order the buffered path had for free. The
            // table stays intact until that last transaction, so a reader never sees
            // it empty and a kill leaves the old rows with no watermark: safe by the
            // same rule. Tombstoned rows are simply not staged — a full replaces all.
            const colList = cols.map((c) => `"${c}"`).join(', ');
            const order = state.pkCols.map((c) => `"${c}"`).join(', ');
            const stageSql = chainStageSql('temp._zb_seed_stage', cols, blobIdx);
            let win: any[][] = []; let n = 0;
            const flush = async () => {
              if (win.length) { await this.run(stageSql, chainChunkJson(win, blobIdx)); win = []; }
              this.seedProgress({ table, step: step.name, kind: step.kind, applied: n, total: stream.nrows, done: false });
            };
            const tempBefore = await this.storage.tempFiles?.();
            try {
              await this.run('DROP TABLE IF EXISTS temp._zb_seed_stage');
              await this.run(`CREATE TEMP TABLE _zb_seed_stage (${colList})`);
              for await (const batch of stream.batches) {
                for (const row of batch) {
                  n++;
                  if (tombIdx >= 0 && tombstoned(state.tombstoneColumn, { [cols[tombIdx]]: row[tombIdx] })) continue;
                  win.push(row);
                  if (win.length >= size) await flush();
                }
              }
              await flush();
              const tail = await stream.tail();
              if (tail.version_column && tail.version_column !== vcol) {
                this.appendLog('SYS', `${table}: chain object ${step.name} names version column ${tail.version_column}, the manifest ${vcol} — the schema moved under the cut; applied with the manifest's`, 'WARN');
              }
              await this.transaction(async (txExec) => {
                await txExec(`DELETE FROM ${table}`);
                await txExec(`INSERT INTO ${table} (${colList}) SELECT ${colList} FROM temp._zb_seed_stage ORDER BY ${order}`);
              });
              if (fenceRun) await this.fault('after_chunk');
              this.seedProgress({ table, step: step.name, kind: step.kind, applied: n, total: stream.nrows, done: true });
            } catch (e) {
              this.appendLog('SYS', `${table}: chain object ${step.name} unreadable while streaming: ${e}`, 'ERROR');
              return null;
            } finally {
              try { await this.run('DROP TABLE IF EXISTS temp._zb_seed_stage'); } catch { /* the connection is going anyway */ }
              // §10ix: what the stage and the sort left on disk (sqlite-wasm over OPFS
              // orphans its temp files on close — 1.3 GB after this table in Chrome).
              if (tempBefore && this.storage.sweepTemp) {
                try {
                  const bytes = await this.storage.sweepTemp(tempBefore);
                  if (bytes) this.appendLog('SYS', `${table}: reclaimed ${(bytes / 1048576).toFixed(0)} MB of temp files left by the seed`, 'INFO');
                  else if (this.storage.tempFiles) {
                    // Nothing went: either the VFS deleted its own on close (fine) or the
                    // files are still open — say so, since the next open sweeps them.
                    const left = await this.storage.tempFiles();
                    const stayed = [...left].filter((n) => !tempBefore.has(n));
                    if (stayed.length) this.appendLog('SYS', `${table}: ${stayed.length} temp file(s) of the seed still open — swept at the next open`, 'WARN');
                  }
                } catch { /* best effort */ }
              }
            }
            applied += n;
          } else if (stream) {
            let win: any[][] = []; let first = true; let n = 0;
            try {
              for await (const batch of stream.batches) {
                for (const row of batch) {
                  win.push(row); n++;
                  if (win.length >= size) {
                    await applyWindow(step.sorted ? win : sortRowsByKey(win, keyIdx), first); first = false; win = [];
                    this.seedProgress({ table, step: step.name, kind: step.kind, applied: n, total: stream.nrows, done: false });
                  }
                }
              }
              // The last partial window — or, for an empty full, the DELETE alone.
              if (win.length || (first && step.kind === 'full')) { await applyWindow(step.sorted ? win : sortRowsByKey(win, keyIdx), first); first = false; }
              this.seedProgress({ table, step: step.name, kind: step.kind, applied: n, total: stream.nrows, done: true });
              const tail = await stream.tail();
              if (tail.version_column && tail.version_column !== vcol) {
                this.appendLog('SYS', `${table}: chain object ${step.name} names version column ${tail.version_column}, the manifest ${vcol} — the schema moved under the cut; applied with the manifest's`, 'WARN');
              }
            } catch (e) {
              // As fetchDoc: pruned under us, or corrupt (the digest is checked once the
              // last chunk passed). Windows already applied are safe by the watermark
              // rule — it is written after applyPlan returns, so a restart re-plans.
              this.appendLog('SYS', `${table}: chain object ${step.name} unreadable while streaming: ${e}`, 'ERROR');
              return null;
            }
            applied += n;
          } else {
            for (let from = 0; from < rows.length || (from === 0 && step.kind === 'full'); from += size) {
              const to = Math.min(from + size, rows.length);
              await applyWindow(rows.slice(from, to), from === 0);
              this.seedProgress({ table, step: step.name, kind: step.kind, applied: to, total: rows.length, done: to >= rows.length });
              if (to === from) break;
            }
            applied += rows.length;
          }
        } finally {
          if (sqlite) { try { await this.run('PRAGMA cache_size = -2000'); } catch { /* as above */ } }
        }
        if (this.measure.enabled) {
          // A streamed step overlaps fetch, inflate and apply: only its wall time is whole.
          const split = stream ? { bytes: stream.size, fetch_ms: null, inflate_ms: null, decode_ms: null, apply_ms: null }
            : { ...lastDoc, apply_ms: Date.now() - applyT0 };
          chainTotals.bytes += split.bytes;
          if (stream) chainTotals.streamed++;
          else for (const k of ['fetch_ms', 'inflate_ms', 'decode_ms', 'apply_ms'] as const) chainTotals[k] += split[k] ?? 0;
          this.measure.emit({ m: 'step', table, tenant: tenantForTable, object: step.name, kind: step.kind, rows: applied - applied0,
            t0_ms: stepT0, ms: Date.now() - stepT0, ...split, streamed: !!stream });
        }
      }
      return applied;
    };

    // ── D2, and the fence that replaces its refusal where that is safe (fence.ts) ─────
    // D2's destruction guard is core.fullPredatesReplica (NOTES §10n/§10s): a chain-full is
    // DELETE FROM + replay, so a chain cut BEHIND the replica's position would destroy rows
    // the resumed CDC never re-delivers. A follower stays ahead of every new cut, so
    // refusing left it unseeded for as long as writes went on. The fence takes the chain
    // when this table is the only one on the routes it rewinds, and moves the cursors back
    // to the chain's cuts: PREPARE, replace, VERIFY, COMMIT (libzb's client.zig, same order).
    const ownRoute = this.cdcStreamForTenant(tenantForTable);
    const publicRoute = this.config.grammar.cdc_streams?.public;
    const sharedRoute: string | null = publicRoute && ownRoute !== publicRoute ? publicRoute : null;
    // Assigned in `begin` (a closure), so typed through a cast: TS would narrow a plain null.
    let fenceRun = null as { fromPos: number; shared: boolean; plain?: boolean } | null;
    const fromLeaf = !!this.config.seedDomain && this.config.seedDomain !== this.config.jsDomain && seedOpts?.domain === this.config.seedDomain;

    // A full is DESTRUCTIVE: its first chunk deletes the baseline, and what a later chunk loses is
    // not in the CDC suffix (a stable row created before the cut). So a replacement that is not a
    // fence still leaves a durable incomplete state BEFORE its first full chunk — the anchor
    // cleared, the owed record written, in one transaction — and holds the table's events meanwhile.
    // A failure then reads as what it is (unseeded, owed): the retry is a fence's retry, with every
    // proof. Taken once the full's object is in hand, not at the decision: a chain pruned under us
    // changes nothing. Deltas are not destructive (version-guarded upserts, idempotent under a
    // retry) and write nothing.
    const oweReplacement = async (man: any) => {
      await this.transaction(async (txExec) => {
        await txExec(`DELETE FROM _zebridge_generations WHERE tbl = ?`, table);
        await txExec(`INSERT INTO _zebridge_fence (tbl) VALUES (?) ON CONFLICT(tbl) DO NOTHING`, table);
        await txExec(`DELETE FROM _zebridge_frontier WHERE tbl = ?`, table); // it is replaced: no frontier of its own
      });
      this.frontiers.delete(table);
      state.seedSeq = undefined; state.seedStream = undefined; state.sharedSeedSeq = undefined; state.seedLsn = undefined;
      state.unseeded = true; // its events are held, not applied, until the anchor commits
      hold.owed = true;
      fenceRun = fenceRun ?? { fromPos: this.globalSyncState.seq[man.cdc_stream] ?? 0, shared: false, plain: true };
    };

    // The decision, from fresh facts: positions, the owed record and the other tables' seeded
    // state change under any other fence (a PREPARE moves cursors and unseeds its table), so it
    // is taken again inside `prepareExclusive`, never trusted from before the queue.
    // The age of the chain and the CDC left to take after its cut (from the hub): reported with every
    // fence and chain, so a cost-based fallback from a stale leaf can be decided on evidence.
    const chainCost = (man: any, pr: Proof) => {
      const t = Date.parse(pgTsToWire(String(man.cutoff_version ?? '')));
      return {
        ageMs: Number.isFinite(t) ? Date.now() - t : -1,
        behind: pr.ownLast !== null && typeof man.cutoff_seq === 'number' ? pr.ownLast - man.cutoff_seq : -1,
      };
    };

    const decide = async (man: any, pr: Proof, plan: PlanStep[]) => {
      const ownPos = this.globalSyncState.seq[man.cdc_stream] ?? 0;
      const predates = coreFullPredates(man, plan, ownPos);
      const pending = await this.fenceOwed(table);
      const none = { refused: null as string | null, untouched: [] as Untouched[], joiners: [] as Joiner[] };
      const ownScope = predates ? await this.fenceScope(table, [man.cdc_stream ?? '']) : none;
      // Only a shared route that is past its cut is rewound, and only then does its scope matter (and its unseeded
      // tables get read for the joint fence).
      const sharedPos = sharedRoute ? (this.globalSyncState.seq[sharedRoute] ?? 0) : 0;
      const sharedScope = sharedRoute && pr.sharedSeq !== null && sharedPos > pr.sharedSeq ? await this.fenceScope(table, [sharedRoute]) : none;
      const decision = decideFence({
        predates, pending, hasFull: plan.some((x) => x.kind === 'full'),
        ownStream: man.cdc_stream ?? '', ownPos, ownCut: typeof man.cutoff_seq === 'number' ? man.cutoff_seq : 0,
        ownScopeRefusal: ownScope.refused,
        ownReplayable: pr.ownReplayable,
        sharedStream: sharedRoute, manifestSharedStream: man.shared_cdc_stream ?? '',
        sharedCut: pr.sharedSeq, sharedProven: pr.sharedProven,
        sharedPos,
        sharedScopeRefusal: sharedScope.refused,
      });
      return { ownPos, predates, ownScope, sharedScope, decision };
    };

    // PREPARE, shared by the fence decided at the start (`begin`) and by a plain seed whose position
    // moved past the cut while its full downloaded (`settleBeforeFull`). `late` decides again with the
    // tails stopped and, if the chain is still ahead of the position, owes the plain replacement there.
    type PrepOutcome = { kind: 'prepared'; ownPos: number; ownBehind: boolean; sharedMoved: boolean; untouched: number; joint: string[] } | { kind: 'plain' } | { kind: 'wait'; why: string };
    const prepare = async (man: any, pr: Proof, plan: PlanStep[], late: boolean): Promise<PrepOutcome> => {
      let outcome = { kind: 'wait', why: 'not decided' } as PrepOutcome;
      await this.withStreamQueue(run, () => this.prepareExclusive(async () => {
        // Late: the full's object is in hand and the first chunk deletes the baseline. The tails on
        // its routes stop FIRST, so the position read next is final — CDC may have passed the cut
        // while the object downloaded, and a plain full over that would delete the rows of events
        // the cursor already counts.
        if (late) for (const x of [man.cdc_stream, ...(sharedRoute ? [sharedRoute] : [])]) if (x) await this.stopTail(x);
        const now = await decide(man, pr, plan);
        const decision = now.decision;
        if (decision.kind === 'refuse') { outcome = { kind: 'wait', why: decision.why }; return; }
        if (decision.kind === 'none') {
          // A plain seed: it may still overlap a fence on its routes (one decided just before it, or
          // one that parks them later). Recorded here, in the exclusive section, so that fence's
          // commit cannot reopen a tail under this replacement's download.
          if (plan.length) this.beginReplacement(table, [man.cdc_stream, ...(sharedRoute ? [sharedRoute] : [])].filter((x): x is string => !!x), hold);
          if (late) await oweReplacement(man); // still ahead of the position: owed, here, with the tails stopped
          outcome = { kind: 'plain' };
          return;
        }
        const streams = [man.cdc_stream, ...(decision.sharedTo !== null && sharedRoute ? [sharedRoute] : [])];
        // The tables the fence leaves untouched, each with the route it rewinds. Their frontier is
        // taken on the stream identity the chain's cut was proven on (R4): the stored identity of
        // the position must be that one, or they cannot stay untouched.
        const untouched: Untouched[] = [...(now.predates ? now.ownScope.untouched : []), ...(decision.sharedTo !== null ? now.sharedScope.untouched : [])];
        // The joint fence: the unseeded tables of the routes this fence rewinds are replaced WITH it. Each is owed
        // from this PREPARE on, the routes stay parked until every one of them has committed, and the cursors go to
        // the smallest cut among them; each is anchored at its own cut when its turn comes.
        const joiners: Joiner[] = [];
        for (const j of [...(now.predates ? now.ownScope.joiners : []), ...(decision.sharedTo !== null ? now.sharedScope.joiners : [])]) {
          if (!joiners.some((x) => x.table === j.table)) joiners.push(j);
        }
        if (joiners.length) {
          // Every object of THIS table's plan too, before anything changes: a joint fence over a chain that
          // cannot be applied would park the routes for nothing.
          for (const step of plan) {
            let oi: any = null;
            try { oi = await os.info(step.name); } catch { /* unreadable: absent */ }
            if (!oi || oi.deleted) {
              outcome = { kind: 'wait', why: `the joint fence with ${joiners.map((x) => x.table).join(', ')} needs ${step.name}, which the object store does not hold: nothing prepared` };
              return;
            }
          }
        }
        const createdOf = (x: string) => String(x === man.cdc_stream ? (man.cdc_stream_created ?? '') : (man.shared_cdc_stream_created ?? ''));
        for (const u of untouched) {
          const want = createdOf(u.stream);
          if (!want || this.streamCreated.get(u.stream) !== want) {
            outcome = { kind: 'wait', why: `chain predates replica — refusing the full (D2): ${u.table} stays untouched on ${u.stream} only if its position and the chain's cut were taken on the same stream incarnation, and that is not settled` };
            return;
          }
        }
        // The parking is memory the transaction does not own: a PREPARE that fails before
        // it commits must leave it as it found it, or the routes stay parked with nothing
        // prepared (no fence owed, the anchor intact) and every later pass skips them.
        const parked = streams.map((x) => [x, this.fenced.has(x) ? new Set(this.fenced.get(x)) : undefined] as const);
        let to = (_x: string) => 0;
        let stuck: unknown = null;
        try {
          for (const x of streams) {
            if (!this.fenced.has(x)) this.fenced.set(x, new Set());
            this.fenced.get(x)!.add(table);
            for (const j of joiners) this.fenced.get(x)!.add(j.table);
          }
          for (const x of streams) await this.stopTail(x);
          // The tails are stopped: each route's position is final, and it certifies the
          // untouched tables (R2) — their frontier is that position, above the cursor it guards.
          // The cursors never move forward: the target is the cut or the position, whichever is lower.
          // A participant's cut lowers only a cursor this fence rewinds, whose other tables were looked at (the scope):
          // a participant that rides another route too rewinds it in its own turn, with its own scope.
          const scoped = new Set<string>([...(now.predates ? [man.cdc_stream as string] : []), ...(decision.sharedTo !== null && sharedRoute ? [sharedRoute] : [])]);
          const jointCut = (x: string) => !scoped.has(x) ? Infinity : Math.min(...joiners.map((j) => x === j.man.cdc_stream && typeof j.man.cutoff_seq === 'number' ? j.man.cutoff_seq
            : x === j.man.shared_cdc_stream && j.pr.sharedSeq !== null ? j.pr.sharedSeq : Infinity));
          to = (x: string) => Math.min(this.globalSyncState.seq[x] ?? 0, x === man.cdc_stream ? (typeof man.cutoff_seq === 'number' ? man.cutoff_seq : 0) : (decision.sharedTo as number), jointCut(x));
          const writes = untouched
            .map((u) => ({ ...u, created: createdOf(u.stream), seq: this.globalSyncState.seq[u.stream] ?? 0 }))
            .filter((w) => w.seq > to(w.stream));
          const targets = new Map(streams.map((x) => [x, to(x)] as const));
          await this.transaction(async (txExec) => {
            await txExec(`DELETE FROM _zebridge_generations WHERE tbl = ?`, table);
            if (joiners.length) await txExec(`INSERT INTO _zebridge_fence (tbl, joint) VALUES (?, 1) ON CONFLICT(tbl) DO UPDATE SET joint = 1`, table);
            else await txExec(`INSERT INTO _zebridge_fence (tbl) VALUES (?) ON CONFLICT(tbl) DO NOTHING`, table);
            // The fenced table is replaced: what it held before is no frontier of its own.
            await txExec(`DELETE FROM _zebridge_frontier WHERE tbl = ?`, table);
            // The participants: owed like this table (and flagged as the same group), no anchor, no frontier of their own.
            for (const j of joiners) {
              await txExec(`DELETE FROM _zebridge_generations WHERE tbl = ?`, j.table);
              await txExec(`INSERT INTO _zebridge_fence (tbl, joint) VALUES (?, 1) ON CONFLICT(tbl) DO UPDATE SET joint = 1`, j.table);
              await txExec(`DELETE FROM _zebridge_frontier WHERE tbl = ?`, j.table);
            }
            for (const x of streams) {
              await txExec(`INSERT INTO _zebridge_stream_seq (stream, last_seq) VALUES (?, ?) ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq`, x, targets.get(x));
            }
            // After the cursors. Monotone on the same stream identity (an earlier, interrupted
            // fence may have written a higher one), replaced on another.
            for (const w of writes) {
              await txExec(
                `INSERT INTO _zebridge_frontier (tbl, stream, created, seq) VALUES (?, ?, ?, ?)
                 ON CONFLICT(tbl, stream) DO UPDATE SET
                   seq = CASE WHEN _zebridge_frontier.created = excluded.created AND _zebridge_frontier.seq > excluded.seq THEN _zebridge_frontier.seq ELSE excluded.seq END,
                   created = excluded.created`,
                w.table, w.stream, w.created, w.seq);
            }
          });
          to = (x: string) => targets.get(x) as number;
          // The durable state is the truth: the frontiers back from it.
          try { await this.loadFrontiers(); } catch (e) { stuck = e; }
        } catch (e) {
          for (const [x, owners] of parked) { if (owners === undefined) this.fenced.delete(x); else this.fenced.set(x, owners); }
          // The tails were stopped for the PREPARE: reopen them from the cursors, which did
          // not move (a pass reopens its own in step 2).
          if (!run.inPass) void this.subscribeStreams().catch(() => {});
          throw e;
        }
        // Committed: the cursors, the unseeded table and the owed record are durable, so memory says the same.
        for (const x of streams) this.globalSyncState.seq[x] = to(x);
        state.seedSeq = undefined; state.seedStream = undefined; state.sharedSeedSeq = undefined; state.seedLsn = undefined;
        state.unseeded = true; // its events are held, not applied, until the anchor commits
        for (const j of joiners) {
          const js = this.syncedTables.get(j.table);
          if (js) { js.seedSeq = undefined; js.seedStream = undefined; js.sharedSeedSeq = undefined; js.seedLsn = undefined; js.unseeded = true; }
          this.frontiers.delete(j.table);
        }
        if (stuck) {
          // The frontiers could not be read back: the other tables' protection is not in memory.
          // The routes STAY parked (no tail applies a replayed event to a table that already holds
          // it); the fence is still owed, and its retry reads them again.
          this.appendLog('SYS', `${table}: fence prepared but the applied frontiers could not be read back (${stuck}) — ${streams.join(', ')} stay parked until they can; the fence is still owed`, 'ERROR');
          throw stuck;
        }
        hold.fence = true;
        hold.owed = true;
        this.beginReplacement(table, streams, hold);
        outcome = { kind: 'prepared', ownPos: now.ownPos, ownBehind: now.predates, sharedMoved: decision.sharedTo !== null, untouched: untouched.length, joint: joiners.map((j) => j.table) };
      }));
      return outcome;
    };

    /// The log line of a prepared fence, for both ways in (`begin`, `settleBeforeFull`). Greppable by the scenarios:
    /// "fence prepared — chain g<gen> cut at <cut> on <stream>". A fence that only the SHARED route needs leaves the
    /// own cursor where it is, and says so.
    const preparedLine = (man: any, pr: Proof, out: Extract<PrepOutcome, { kind: 'prepared' }>) => {
      const cost = chainCost(man, pr);
      const where = out.ownBehind
        ? `behind position ${out.ownPos}: anchor cleared, cursor at the cut${out.sharedMoved ? ' (the shared route too)' : ''}`
        : `(position ${out.ownPos}, not behind it): anchor cleared; only the shared route ${sharedRoute} is past its cut and goes back to it`;
      return `fence prepared — chain g${man.gen} cut at ${man.cutoff_seq} on ${man.cdc_stream} ${where}; replacing the rows${out.joint.length ? ` jointly with ${out.joint.join(', ')} (unseeded: owed too, the routes stay parked until all have committed)` : ''}${out.untouched ? ' — the other tables on the route keep their applied frontier' : ''}${fromLeaf ? ' [from the seeding leaf]' : ''} (chain ${Math.floor(cost.ageMs / 1000)} s old, ${cost.behind} CDC message(s) after its cut on the hub)`;
    };

    // Before the first chunk of a full that `begin` took as plain. The decision was taken before
    // the download, and the tails kept running meanwhile (a poll loop's seed does not stop them): the
    // position may have passed the cut since, and a plain full over rows that already carry those
    // events deletes them while the cursor still counts them — gone for ever. So the position is read
    // again with the tails stopped, under the queue: still ahead of it, the replacement is owed (as
    // before); passed, the full becomes a fence (PREPARE: cursors back to the cut) with the object
    // already in hand. `false` = wait, nothing changed.
    const settleBeforeFull = async (plan: PlanStep[]): Promise<boolean> => {
      const pr = proof as Proof; // proven before `begin` ran: a plan exists only with one
      const out = await prepare(manifest, pr, plan, true);
      if (out.kind === 'prepared') {
        fenceRun = { fromPos: out.ownPos, shared: out.sharedMoved };
        this.appendLog('SYS', `${table}: the position passed the cut while the full downloaded — ${preparedLine(manifest, pr, out)}`, 'WARNING');
        return true;
      }
      // The tails were stopped for the decision: reopen them, unless a fence parks the route (it keeps
      // them stopped until it commits, and a pass would find them parked anyway).
      if (!run.inPass && ![manifest.cdc_stream, sharedRoute].some((x) => x && this.routeParked(x))) void this.subscribeStreams().catch(() => {});
      if (out.kind === 'wait') { this.appendLog('SYS', `${table}: ${out.why}`, 'WARNING'); return false; }
      return true;
    };

    // One manifest's plan and fence decision; null = wait (refused, and said).
    const begin = async (man: any, pr: Proof): Promise<PlanStep[] | null> => {
      await this.fault('before_decision');
      const plan = planFrom(man);
      // A seeding leaf's manifest can arrive before its objects do (two mirrors). Every object of
      // the plan must be there BEFORE anything changes — a fence's PREPARE included — or the
      // hub's chain is the one used.
      if (fromLeaf) {
        for (const step of plan) {
          let oi: any = null;
          try { oi = await os.info(step.name); } catch { /* unreadable: absent */ }
          if (!oi || oi.deleted) {
            this.appendLog('SYS', `${table}: the seeding leaf's manifest names ${step.name}, which its cache does not hold yet — seeding from the hub`, 'INFO');
            return null;
          }
        }
      }
      const first = await decide(man, pr, plan);
      if (first.decision.kind === 'refuse') { this.appendLog('SYS', `${table}: ${first.decision.why}`, 'WARNING'); return null; }
      if (first.decision.kind === 'none') {
        // A plain seed is recorded on its routes all the same: a fence that parks them while it
        // downloads, and commits first, must hand the parking over (beginReplacement).
        if (plan.length) await this.prepareExclusive(async () => this.beginReplacement(table, [man.cdc_stream, ...(sharedRoute ? [sharedRoute] : [])].filter((x): x is string => !!x), hold));
        return plan;
      }
      // A fence is taken from a seeding leaf's chain too (the recovery model, §6): its cut and
      // identities have their hub meaning, and every proof (`prove`, `decide`, VERIFY) reads the
      // HUB's stream — the incarnation it was cut on, the event after the cut still retained. A
      // leaf chain whose suffix the hub no longer holds is refused by those proofs, as before.

      // PREPARE, before any row changes: the table durably unseeded and the routes' cursors
      // at the cuts, in one transaction, the fence recorded as owed. A failure or a crash
      // from here to the anchor's commit leaves "unseeded, cursor at the cut" — the next
      // attempt is a fence again, and the events after the cut replay (a rewind never
      // skips) — never replaced rows under the old cursor. The tails on these routes stop
      // first and no pass reopens them (`fenced`): an old gate above the cut would drop the
      // very events that must replay.
      //
      // Fences on one route are exclusive and decided AGAIN inside: two fences with cuts 100 and
      // 200, each decided against the position as it was before either prepared, would leave the
      // cursor at the later cut and the table seeded at 100 without events 101..200. Inside, the
      // position is the one the earlier fence left (the cursor never moves forward), the other
      // tables are seen as they are now (the earlier fence's table is unseeded: refused), and a
      // chain that no longer predates the position is no fence at all.
      const out = await prepare(man, pr, plan, false);
      if (out.kind === 'wait') { this.appendLog('SYS', `${table}: ${out.why}`, 'WARNING'); return null; }
      if (out.kind === 'plain') return plan;
      fenceRun = { fromPos: out.ownPos, shared: out.sharedMoved };
      this.appendLog('SYS', `${table}: ${preparedLine(man, pr, out)}`, 'WARNING');
      return plan;
    };

    let plan = await begin(manifest, proof);
    if (!plan) return false;
    let applied = await applyPlan(plan);
    if (applied === null) {
      // Pruned between manifest read and fetch: re-read ONCE, restart from ITS full —
      // overlap, never a gap; a second failure falls back to the snapshot path.
      manifest = await readManifest();
      if (!manifest?.full?.object) return false;
      watermark = null;
      if (!(await stillEligible(manifest, watermark))) return false;
      const again = await prove(manifest);
      if (!again) return false;
      proof = again;
      plan = await begin(manifest, proof);
      if (!plan) return false;
      applied = await applyPlan(plan);
      if (applied === null) return false;
    }

    // The fence's VERIFY, after the rows were replaced and before the anchor: the proof
    // taken before the download may be stale — the stream can prune or restart while the
    // rows are replaced. Fresh facts, for every route the table depends on: the same
    // incarnation, and the event after each cut still held. Otherwise no anchor: the
    // table stays unseeded with its cursor at the cut and the fence still owed, which the
    // gap rule then reads as the gap it is — recovery incomplete, a newer chain next.
    let verified: FenceRoute[] | null = null;
    if (fenceRun && !fenceRun.plain) {
      await this.fault('before_verify');
      const routes: FenceRoute[] = [{ stream: manifest.cdc_stream, cut: manifest.cutoff_seq, created: String(manifest.cdc_stream_created ?? '') }];
      if (sharedRoute) routes.push({ stream: sharedRoute, cut: proof.sharedSeq ?? -1, created: String(manifest.shared_cdc_stream_created ?? '') });
      const now = await this.routeFacts(routes.map((r) => r.stream));
      verified = routes;
      const lost = verifyRoutes(routes, (x) => now.get(x) ?? null);
      if (lost) {
        this.appendLog('SYS', `${table}: fence not committed — ${lost.stream} no longer holds the events after the cut ${lost.cut} on the incarnation the chain was cut on; the table stays unseeded with its cursor at the cut (recovery incomplete)`, 'WARNING');
        return false;
      }
      await this.fault('before_commit');
    }

    // The anchor. Under the fence it is COMMITTED first, with the owed record's removal in
    // the same transaction, and only then published in memory: published before, a failed
    // commit would leave a gate the durable state does not have.
    const seedLsn = lsnToNumber(manifest.cutoff_lsn);
    const gated = proof.gateSeq > 0 && !!manifest.cdc_stream;
    const setAnchor = () => {
      state.lsn = seedLsn;
      state.seedLsn = state.lsn; // the ONE place the legacy data gate may anchor to (finding 10)
      if (gated) {
        state.seedSeq = proof.gateSeq;
        state.seedStream = manifest.cdc_stream;
      }
      state.sharedSeedSeq = proof.sharedSeq ?? undefined;
      // The chain's cutoff_version is an observed version watermark: floor the HLC
      // with it so a freshly seeded slow-clock client stamps above its own seed.
      if (typeof manifest.cutoff_version === 'string') {
        this.hlcFloor = maxVersion(this.hlcFloor, normalizeVersion(pgTsToWire(manifest.cutoff_version)));
      }
    };
    const upsert = `INSERT INTO _zebridge_generations (tbl, watermark, cutoff_lsn, seed_epoch, seed_seq, seed_stream, shared_seed_seq) VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(tbl) DO UPDATE SET watermark = excluded.watermark, cutoff_lsn = excluded.cutoff_lsn, seed_epoch = excluded.seed_epoch,
         seed_seq = excluded.seed_seq, seed_stream = excluded.seed_stream, shared_seed_seq = excluded.shared_seed_seq`;
    if (fenceRun) {
      await this.pruneInboxSeeded(table, seedLsn);
      // The completion DECISION is one exclusive section, on the chain the PREPARE uses: which participants are still owed, the
      // proofs of the earlier ones, the commit's transaction and the parking all depend on each other, and two completions that
      // read the owed records before either wrote them both took themselves for an intermediate participant — nobody proved
      // anybody again and the routes reopened with the proofs left behind. It waits for no seed and for no stream pass.
      const stale: { lost: { stream: string; cut: number } | null } = { lost: null };
      await this.prepareExclusive(async () => {
        // The table's OWN proof is taken again here, first: its VERIFY ran before the queue, and what it waited behind (another
        // PREPARE, another completion, a download of theirs) is not short. The stream may have dropped the events after this cut,
        // on its own route or on the shared one, meanwhile. Not atomic with the commit (the gap rule stays necessary), but no
        // longer older than the queue.
        if (verified) {
          const facts = await this.routeFacts(verified.map((r) => r.stream));
          const lost = verifyRoutes(verified, (x) => facts.get(x) ?? null);
          if (lost) { stale.lost = { stream: lost.stream, cut: lost.cut }; return; }
        }
        // A participant of a joint fence: while others are still owed it leaves its proofs in the replica; the LAST one proves
        // every earlier participant again, on fresh facts, BEFORE its own commit, and the same transaction either ends the
        // group or owes a lapsed participant again. A crash anywhere in between leaves the owed records, which are the group.
        const joint = await this.jointFlag(table);
        const othersOwed = joint ? await this.jointOwedOthers(table) : 0;
        const lapsed = joint && othersOwed === 0 ? await this.jointLapsed(table) : [];
        await this.transaction(async (txExec) => {
          await txExec(upsert, table, manifest.cutoff_version, seedLsn, state.seedEpoch ?? 0,
            gated ? proof.gateSeq : null, gated ? manifest.cdc_stream : null, proof.sharedSeq);
          await txExec(`DELETE FROM _zebridge_fence WHERE tbl = ?`, table);
          await txExec(`DELETE FROM _zebridge_joint_done WHERE tbl = ?`, table);
          if (joint && othersOwed > 0) {
            for (const r of verified ?? []) await txExec(`INSERT INTO _zebridge_joint_done (tbl, stream, cut, created) VALUES (?, ?, ?, ?)`, table, r.stream, r.cut, r.created);
          } else if (joint && !lapsed.length) {
            await txExec(`DELETE FROM _zebridge_joint_done`);
          } else if (joint) {
            for (const l of lapsed) {
              await txExec(`DELETE FROM _zebridge_generations WHERE tbl = ?`, l.table);
              await txExec(`INSERT INTO _zebridge_fence (tbl, joint) VALUES (?, 1) ON CONFLICT(tbl) DO UPDATE SET joint = 1`, l.table);
              await txExec(`DELETE FROM _zebridge_frontier WHERE tbl = ?`, l.table);
              await txExec(`DELETE FROM _zebridge_joint_done WHERE tbl = ?`, l.table);
            }
          }
        });
        await this.fault('after_commit');
        setAnchor();
        state.unseeded = false;
        for (const l of lapsed) this.lapseJoint(l);
        if (joint && othersOwed === 0 && !lapsed.length) this.appendLog('SYS', 'joint fence complete — its routes are open again', 'INFO');
        // The routes' parking ends with the fence, or with the last participant of a joint fence (none lapsed): their tails
        // reopen from the cursors it set. A lapsed participant keeps it.
        this.releaseFence(table);
      });
      if (stale.lost) {
        this.appendLog('SYS', `${table}: fence not committed — ${stale.lost.stream} no longer holds the events after the cut ${stale.lost.cut} on the incarnation the chain was cut on; the table stays unseeded with its cursor at the cut (recovery incomplete)`, 'WARNING');
        return false;
      }
      if (!fenceRun.plain) this.appendLog('SYS', `${table}: fenced — chain g${manifest.gen} cut at ${manifest.cutoff_seq} on ${manifest.cdc_stream} replaced the rows; the position moved back from ${fenceRun.fromPos}, the events after the cut replay${fenceRun.shared ? ' (the shared route too)' : ''}`, 'WARNING');
      if (!run.inPass) void this.subscribeStreams().catch(() => {}); // the routes it held may be open now
      await this.drainPending(table); // the events held while the rows were replaced
    } else {
      setAnchor();
      await this.pruneInboxSeeded(table, state.lsn);
      await this.run(upsert, table, manifest.cutoff_version, state.lsn, state.seedEpoch ?? 0, state.seedSeq ?? null, state.seedStream ?? null, proof.sharedSeq);
    }
    // A full replaced the rows: this client's writes still queued lost their optimistic
    // copies with them, so they go back on top (PROTOCOL §7.4c).
    if (plan.some((stp: PlanStep) => stp.kind === 'full')) await this.reapplyPending(table);
    if (this.measure.enabled) {
      const fulls = plan.filter((stp: PlanStep) => stp.kind === 'full').length;
      // `seed`: from the seeding leaf's mirrors (seedDomain); `hub`: from the hub.
      const source = seedOpts?.domain && seedOpts.domain === this.config.seedDomain ? 'seed' : 'hub';
      this.measure.emit({ m: 'chain', table, tenant: tenantForTable, source, gen: manifest.gen, full_gen: manifest.full?.gen ?? 0, fulls, deltas: plan.length - fulls,
        rows: applied, t0_ms: chainT0, t1_ms: Date.now(), ...chainTotals, cutoff_version: manifest.cutoff_version ?? '',
        cdc_stream: manifest.cdc_stream ?? '', cutoff_seq: manifest.cutoff_seq ?? -1, shared_cutoff_seq: proof.sharedSeq ?? -1,
        cut_age_ms: chainCost(manifest, proof).ageMs, cdc_behind: chainCost(manifest, proof).behind });
    }
    this.triggerChange(table);
    this.appendLog('SYS', `Seeded ${table} from generation chain g${manifest.gen} (${applied} row(s), watermark ${manifest.cutoff_version} @ ${manifest.cutoff_lsn})`, 'INFO');
    return true;
  }

  /// The proofs of ONE manifest (the cuts the fence and the seed gate may rely on), taken from the hub's
  /// streams: what a chain replacement of `table` reads before anything changes, and what the joint fence
  /// reads for every table it would replace with it. null = wait (the stream no longer holds what the
  /// chain's cut needs).
  private async proveManifest(table: string, man: any): Promise<Proof | null> {
      let gateSeq = typeof man.cutoff_seq === 'number' && man.cutoff_seq > 0 ? man.cutoff_seq : 0;
      let ownReplayable = false;
      // The hub's newest event on the route: the CDC left to take after the chain's cut (a leaf's chain
      // is accepted however old while its suffix is replayable; age and backlog are reported).
      let ownLast: number | null = null;
      // §10ja: checked whenever the field is PRESENT — 0 included. A chain cut on an empty,
      // brand-new stream says 0, and on a stream that now starts past 1 it predates it: the
      // events between were pruned, and seeding it then tailing from the oldest message left
      // would leave them out for good.
      if (typeof man.cutoff_seq === 'number' && man.cutoff_seq >= 0 && man.cdc_stream) {
        try {
          const jsm = await this.transport.jetstreamManager(this.nc!, this.jsOpts());
          const info = await jsm.streams.info(man.cdc_stream);
          const st = info.state;
          const first = st.first_seq;
          ownLast = st.last_seq;
          const nowCreated = String((info as { created?: unknown }).created ?? '');
          if (man.cdc_stream_created && nowCreated) {
            if (man.cdc_stream_created !== nowCreated) {
              this.appendLog('SYS', `${table}: chain g${man.gen} was cut on a previous incarnation of ${man.cdc_stream} (created ${man.cdc_stream_created}, now ${nowCreated}) — seeding it, gating nothing until a newer generation`);
              gateSeq = 0;
            } else {
              this.restarted.delete(man.cdc_stream);
              ownReplayable = true;
            }
          } else if (this.restarted.has(man.cdc_stream)) {
            if (man.cutoff_seq > st.last_seq) {
              this.appendLog('SYS', `${table}: chain g${man.gen} was cut before ${man.cdc_stream} restarted (cutoff seq ${man.cutoff_seq} beyond last_seq ${st.last_seq}) — seeding it, gating nothing until a newer generation`);
              gateSeq = 0;
            } else {
              this.restarted.delete(man.cdc_stream);
            }
          }
          if (man.cutoff_seq + 1 < first) {
            this.appendLog('SYS', `${table}: chain g${man.gen} predates the stream (cutoff seq ${man.cutoff_seq} < first ${first} on ${man.cdc_stream}) — the events between are gone; waiting for the producer's next generation`, 'WARNING');
            return null;
          }
        } catch { /* stream info unavailable — the gap rule at the next connect covers it */ }
      }
      // §10lw: the same splice test on the SHARED route — the open-tenant rows of a tenant
      // table ride CDC_PUBLIC. A shared cut below its oldest message means rows the chain
      // does not carry are gone from the stream too: wait for the next generation. A cut on
      // a previous incarnation of the stream proves nothing there.
      let sharedSeq: number | null = null;
      let sharedProven = false;
      if (typeof man.shared_cutoff_seq === 'number' && man.shared_cutoff_seq >= 0 && man.shared_cdc_stream) {
        try {
          await this.fault('shared_lookup');
          const jsm = await this.transport.jetstreamManager(this.nc!, this.jsOpts());
          const info = await jsm.streams.info(man.shared_cdc_stream);
          const nowCreated = String((info as { created?: unknown }).created ?? '');
          if (!(man.shared_cdc_stream_created && nowCreated && man.shared_cdc_stream_created !== nowCreated)) {
            if (man.shared_cutoff_seq + 1 < info.state.first_seq) {
              this.appendLog('SYS', `${table}: chain g${man.gen} predates ${man.shared_cdc_stream} (shared cut ${man.shared_cutoff_seq} < first ${info.state.first_seq}) — its open-tenant rows between are gone; waiting for the producer's next generation`, 'WARNING');
              return null;
            }
            sharedSeq = man.shared_cutoff_seq;
            sharedProven = !!man.shared_cdc_stream_created && !!nowCreated;
          }
        } catch { /* stream info unavailable — the shared route stays unproven */ }
      }
      return { gateSeq, sharedSeq, ownReplayable, sharedProven, ownLast };
  }

  /// The fence's owed record (fence.ts): a PREPARE with no anchor commit after it.
  private async fenceOwed(table: string): Promise<boolean> {
    try { return ((await this.run(`SELECT 1 AS owed FROM _zebridge_fence WHERE tbl = ?`, table)) ?? []).length > 0; } catch { return false; }
  }

  /// fence.ts's scope rule for `streams`: the other followed tables riding them keep their rows
  /// and get an applied frontier, if their coverage is known (they are seeded). On-demand tables
  /// and tables this principal cannot route are not followed and ride nothing. A table that is NOT
  /// seeded cannot stay untouched: it takes part in the fence (the joint fence) if its own chain
  /// can be replaced with this one, and the fence is refused, as it always was, if it cannot.
  private async fenceScope(table: string, streams: readonly string[]): Promise<{ refused: string | null; untouched: Untouched[]; joiners: Joiner[] }> {
    const pub = this.config.grammar.cdc_streams?.public;
    const have = new Set(((await this.run(`SELECT tbl FROM _zebridge_generations`)) ?? []).map((r: any) => String(r.tbl)));
    const scope = fenceScopeOf(table, streams, this.syncedTables.keys(), (t) => {
      if (this.ondemandSet.has(t)) return null;
      const tn = this.effectiveTenantFor(t);
      if (tn === null) return null;
      const route = this.cdcStreamForTenant(tn);
      return pub && route !== pub ? { route, sharedRoute: pub } : { route };
    }, (t) => have.has(t) && !this.syncedTables.get(t)?.unseeded, 'collect');
    const joiners: Joiner[] = [];
    for (const u of scope.unseeded) {
      if (joiners.some((j) => j.table === u.table)) continue;
      const j = await this.jointCandidate(u.table);
      if (typeof j === 'string') {
        return { refused: `${u.table} is not seeded, so its coverage on ${u.stream} is unknown, and it cannot be replaced with this table: ${j}`, untouched: [], joiners: [] };
      }
      joiners.push(j);
    }
    return { refused: null, untouched: scope.untouched, joiners };
  }

  /// Can `other`, which is not seeded, be replaced together with a fenced table? Its chain is read from the
  /// HUB and proven as any chain is (same incarnation, the event after its cut still retained, on its own route
  /// and on the shared one), and every object of its plan is there now: a joint fence moves the cursors for it
  /// and parks the routes until it is done, so it is never promised on a chain that cannot be applied. A reason
  /// otherwise. A promise of a replacement is not coverage.
  private async jointCandidate(other: string): Promise<Joiner | string> {
    const GEN = this.config.grammar.generations;
    const st = this.syncedTables.get(other);
    const tenant = this.effectiveTenantFor(other);
    if (!GEN || !this.nc || !st || tenant === null) return 'it has no chain to read';
    let man: any = null;
    try {
      const kv = await this.transport.kv(this.nc, GEN.kv, { allow_direct: true }, this.jsOpts());
      const entry = await kv.get(`${tenant}.${other}`);
      if (entry && entry.operation === 'PUT' && entry.value?.length) man = JSON.parse(td.decode(entry.value));
    } catch { /* unreadable: no chain */ }
    if (!man?.full?.object) return 'the hub holds no chain for it';
    if ((man.seed_epoch ?? 0) < (st.seedEpoch ?? 0)) return `its chain g${man.gen} predates its re-seed (epoch ${man.seed_epoch ?? 0} < ${st.seedEpoch})`;
    const pr = await this.proveManifest(other, man);
    if (!pr || !pr.ownReplayable) return `its cut on ${man.cdc_stream} cannot be proven replayable`;
    if (typeof man.shared_cutoff_seq === 'number' && man.shared_cdc_stream && (pr.sharedSeq === null || !pr.sharedProven)) {
      return `its shared cut on ${man.shared_cdc_stream} cannot be proven replayable`;
    }
    try {
      const os = await this.transport.objectStore(this.nc, man.bucket, this.jsOpts());
      for (const step of planFromManifest(man, null)) {
        const info = await os.info(step.name);
        if (!info || info.deleted) return `its chain object ${step.name} is not in ${man.bucket}`;
      }
    } catch (e) { return `its chain objects are unreadable (${e})`; }
    return { table: other, man, pr };
  }

  /// No tail may open on this route now (a fence, or a chain replacement overlapping it, holds it).
  private routeParked(stream: string): boolean {
    return (this.fenced.get(stream)?.size ?? 0) > 0;
  }

  /// A chain replacement of `table` starts on `routes` (inside the exclusive section, so it is seen
  /// by every fence that decides or releases after it). On a route that is parked it JOINS the
  /// parking; on one that is not, it is only recorded, so a fence that parks it later and commits
  /// first hands the parking over instead of reopening the tail under this replacement.
  private beginReplacement(table: string, routes: readonly string[], hold: Replacement) {
    for (const x of routes) {
      if (!this.replacing.has(x)) this.replacing.set(x, new Set());
      this.replacing.get(x)!.add(table);
      if (!hold.routes.includes(x)) hold.routes.push(x);
      const owners = this.fenced.get(x);
      if (owners?.size) owners.add(table);
    }
  }

  /// Fresh facts of streams, from the hub: the incarnation and the oldest message each holds (null = unreadable).
  private async routeFacts(streams: readonly string[]): Promise<Map<string, { created: string; firstSeq: number } | null>> {
    const now = new Map<string, { created: string; firstSeq: number } | null>();
    try {
      const jsm = await this.transport.jetstreamManager(this.nc!, this.jsOpts());
      for (const x of new Set(streams)) {
        try {
          const info: any = await jsm.streams.info(x);
          now.set(x, { created: String(info.created ?? ''), firstSeq: info.state.first_seq });
        } catch { now.set(x, null); }
      }
    } catch { /* every route unread */ }
    return now;
  }

  /// The owed record of `table` says it belongs to a joint fence.
  private async jointFlag(table: string): Promise<boolean> {
    try { return Number(((await this.run(`SELECT joint FROM _zebridge_fence WHERE tbl = ?`, table)) ?? [])[0]?.joint) === 1; } catch { return false; }
  }

  /// The other participants of a joint fence still owed.
  private async jointOwedOthers(table: string): Promise<number> {
    return Number(((await this.run(`SELECT count(*) AS n FROM _zebridge_fence WHERE joint = 1 AND tbl <> ?`, table)) ?? [])[0]?.n ?? 0);
  }

  /// The participants that committed earlier (rows of `_zebridge_joint_done`, so they survive a restart), proven again on
  /// fresh facts: the same incarnation, and the event after each cut still retained. Those that fail.
  private async jointLapsed(except: string): Promise<{ table: string; stream: string; cut: number }[]> {
    const rows: any[] = (await this.run(`SELECT tbl, stream, cut, created FROM _zebridge_joint_done WHERE tbl <> ?`, except)) ?? [];
    if (!rows.length) return [];
    const facts = await this.routeFacts(rows.map((r) => String(r.stream)));
    const byTable = new Map<string, FenceRoute[]>();
    for (const r of rows) byTable.set(String(r.tbl), [...(byTable.get(String(r.tbl)) ?? []), { stream: String(r.stream), cut: Number(r.cut), created: String(r.created) }]);
    const out: { table: string; stream: string; cut: number }[] = [];
    for (const [t, routes] of byTable) {
      const lost = verifyRoutes(routes, (x) => facts.get(x) ?? null);
      if (lost) out.push({ table: t, stream: lost.stream, cut: lost.cut });
    }
    return out;
  }

  /// A participant was owed again by the last one's commit (its row is already durable): memory says the same, and the
  /// routes it rides stay parked for it.
  private lapseJoint(l: { table: string; stream: string; cut: number }) {
    const st = this.syncedTables.get(l.table);
    if (st) { st.seedSeq = undefined; st.seedStream = undefined; st.sharedSeedSeq = undefined; st.seedLsn = undefined; st.unseeded = true; }
    this.frontiers.delete(l.table);
    for (const x of this.jointRoutes(l.table)) {
      if (!this.fenced.has(x)) this.fenced.set(x, new Set());
      this.fenced.get(x)!.add(l.table);
    }
    this.appendLog('SYS', `${l.table}: joint fence not released — ${l.stream} no longer holds the events after its cut ${l.cut} on the incarnation the chain was cut on; it is owed again and the routes stay parked until it is replaced (recovery incomplete)`, 'WARNING');
    this.waitForChain(l.table);
  }

  /// The routes a table's chain replacement parks: its own, and the shared one.
  private jointRoutes(table: string): string[] {
    const tn = this.effectiveTenantFor(table);
    if (tn === null || this.ondemandSet.has(table)) return [];
    const own = this.cdcStreamForTenant(tn);
    const pub = this.config.grammar.cdc_streams?.public;
    return pub && own !== pub ? [own, pub] : [own];
  }

  /// Once per process, before any consumer opens or any chain is applied: the joint fences a previous run left unfinished
  /// park their routes again, from the durable owed records (`joint`). The group does not live in memory alone: without
  /// this a restart would open the tails under participants that are still to be replaced, and the proofs of the ones that
  /// committed are in `_zebridge_joint_done`, read when the last one commits.
  private async ensureJointRestored() {
    if (this.jointRestored) return;
    const rows: any[] = (await this.run(`SELECT tbl FROM _zebridge_fence WHERE joint = 1`)) ?? [];
    for (const r of rows) {
      const t = String(r.tbl);
      if (!this.syncedTables.has(t)) continue;
      const st = this.syncedTables.get(t);
      if (st) st.unseeded = true;
      for (const x of this.jointRoutes(t)) {
        if (!this.fenced.has(x)) this.fenced.set(x, new Set());
        this.fenced.get(x)!.add(t);
      }
    }
    this.jointRestored = true;
    if (rows.length) this.appendLog('SYS', `joint fence unfinished: ${rows.map((r) => r.tbl).join(', ')} still owed — their routes stay parked until all have been replaced`, 'WARNING');
  }

  /// A fence committed: its parking ends, unless a chain replacement is still running on the route
  /// — that one holds it now.
  private releaseFence(table: string) {
    for (const [x, owners] of [...this.fenced]) {
      if (!owners.delete(table)) continue;
      const rep = this.replacing.get(x);
      rep?.delete(table);
      for (const o of rep ?? []) owners.add(o);
      if (!owners.size) { this.fenced.delete(x); this.tailFilters.delete(x); }
    }
  }

  /// A chain replacement ended (committed, refused, failed): it leaves the routes it held. A fence
  /// that failed after its PREPARE keeps its own parking (the fence is still owed). When the last
  /// owner leaves a route its tails reopen.
  private endReplacement(hold: Replacement, table: string, run: SeedRun) {
    let reopen = false;
    for (const x of hold.routes) {
      this.replacing.get(x)?.delete(table);
      if (hold.fence) continue;
      const owners = this.fenced.get(x);
      if (owners?.delete(table) && !owners.size) { this.fenced.delete(x); this.tailFilters.delete(x); reopen = true; }
    }
    if (reopen && !run.inPass) void this.subscribeStreams().catch(() => {});
  }

  /// Fences are exclusive: the section that decides a fence again and prepares it runs alone.
  /// (Pass-owned fences of several tables run concurrently — the queue only orders passes — and
  /// background ones queue, but each must see the cursors the others left.) Nothing in it waits
  /// for the stream queue or for a seed, so it cannot deadlock with either.
  private prepareChain: Promise<void> = Promise.resolve();
  private prepareExclusive(fn: () => Promise<void>): Promise<void> {
    const work = (this.prepareChain ?? Promise.resolve()).then(fn);
    this.prepareChain = work.catch(() => {});
    return work;
  }

  /// The test harness's fault point (config.faultHook); nothing in an app.
  private async fault(point: 'before_decision' | 'after_chunk' | 'before_verify' | 'before_commit' | 'after_commit' | 'shared_lookup'): Promise<void> {
    await this.config.faultHook?.(point);
  }

  /// §10jc: the seed gate of the chain this replica last applied, back from the replica.
  private async restoreSeedGate(table: string) {
    const st = this.syncedTables.get(table);
    if (!st || typeof st.seedSeq === 'number' || typeof st.sharedSeedSeq === 'number') return;
    try {
      const [r] = (await this.run(`SELECT seed_seq, seed_stream, shared_seed_seq FROM _zebridge_generations WHERE tbl = ?`, table)) as any[];
      if (r && r.seed_seq != null && r.seed_stream) { st.seedSeq = Number(r.seed_seq); st.seedStream = String(r.seed_stream); }
      if (r && r.shared_seed_seq != null) st.sharedSeedSeq = Number(r.shared_seed_seq);
    } catch { /* a replica from before the columns: no gate to restore */ }
  }

  // ── the main orchestration: gap check → seed → CDC ────────────────────────

  private subscribeStreams(): Promise<void> {
    return this.changeStreams(() => this.subscribeStreamsOnce());
  }

  private async subscribeStreamsOnce() {
    if (!this.nc) return;
    try { await this.ensureJointRestored(); } catch (e) {
      // Fail closed: no consumer opens under a group that could not be read; the next reconnect or pass asks again.
      this.appendLog('SYS', `subscribeStreams: the unfinished joint fences could not be read (${e}) — no tail opens, the next pass retries`, 'ERROR');
      return;
    }
    // The tables a chain was applied to in this pass: each took the chain's state as of
    // its cut, which step 2 must replay from on every stream it rides.
    const seededThisPass = new Set<string>();
    // The tables seeded by choice (the delta path), not because of a gap: a failure just
    // replays the CDC backlog — nothing was owed.
    const byChoice = new Set<string>();
    // Throw-proof from the first await: this function runs as a floating
    // promise from the reconnect handler and the status-loop restart, and an
    // unhandled rejection here KILLED THE PROCESS when the connection happened
    // to be closed at that instant (§10cs: ClosedConnectionError out of
    // jetstreamManager, twenty clients gone). The next reconnect retries.
    let js: ReturnType<typeof this.transport.jetstream>;
    let jsm: Awaited<ReturnType<typeof this.transport.jetstreamManager>>;
    try {
      js = this.transport.jetstream(this.nc, this.jsOpts());
      jsm = await this.transport.jetstreamManager(this.nc, this.jsOpts());
    } catch (e) {
      this.appendLog('SYS', `subscribeStreams: connection unavailable (${e}) — the next reconnect retries`, 'WARNING');
      return;
    }

    // 1. Gap detection — asked of EVERY stream this client reads: a gap in any of
    // them means missing rows, and checking only one looks like an empty table.
    try {
      const streamGaps: Record<string, { firstSeq: number; stored: number; lastSeq: number; bornEmpty?: boolean }> = {};
      for (const streamName of this.cdcStreams()) {
        const info = await jsm.streams.info(streamName);
        streamGaps[streamName] = {
          firstSeq: info.state.first_seq,
          lastSeq: info.state.last_seq,   // a position beyond it = the stream restarted (lost slot, NOTES §10bm)
          stored: this.globalSyncState.seq[streamName] ?? 0,
        };
        const created = String((info as { created?: unknown }).created ?? '');
        const knownCreated = this.streamCreated.get(streamName);
        const recreated = !!knownCreated && created !== '' && knownCreated !== created;
        if (recreated) this.appendLog('SYNC', `${streamName}: stream recreated (created ${created}, was ${knownCreated}) — position ${streamGaps[streamName].stored} reset`);
        const restartedNow = recreated || streamGaps[streamName].stored > info.state.last_seq;
        // core.streamHasGap's exception: the identity was recorded on a stream that had never held a message, and it is
        // that stream still. Never after a restart, whatever the stream looks like now.
        streamGaps[streamName].bornEmpty = !restartedNow && !!knownCreated && this.streamBornEmpty.has(streamName);
        if (restartedNow) {
          // The feed restarted under us: the position is meaningless in the new
          // numbering. Reset it, or fullPredates reads the fresh chain's small
          // cutoff_seq as "older than where I am" and skips the full this gap needs.
          this.appendLog('SYNC', `${streamName}: stream restarted (position ${streamGaps[streamName].stored} beyond last_seq ${info.state.last_seq}) — position reset`, 'GAP');
          streamGaps[streamName].stored = 0;
          this.globalSyncState.seq[streamName] = 0;
          this.restarted.add(streamName);
          await this.transaction(async (txExec) => {
            await txExec(`UPDATE _zebridge_stream_seq SET last_seq = 0, born_empty = 0 WHERE stream = ?`, streamName);
            await txExec(`DELETE FROM _zebridge_frontier WHERE stream = ?`, streamName);
          });
          this.streamBornEmpty.delete(streamName);
          this.forgetFrontiers(streamName);
        }
        if (created !== '' && (!knownCreated || recreated)) {
          this.streamCreated.set(streamName, created);
          // Born empty: the FIRST sight of a stream that has never held a message, with no position of its own. A
          // restarted stream never is (its tables must be re-seeded before it counts as quiet).
          const bornEmpty = !knownCreated && !restartedNow && streamGaps[streamName].stored === 0
            && info.state.first_seq === 0 && info.state.last_seq === 0;
          if (bornEmpty) this.streamBornEmpty.add(streamName); else this.streamBornEmpty.delete(streamName);
          await this.run(`INSERT INTO _zebridge_stream_seq (stream, last_seq, created, born_empty) VALUES (?, 0, ?, ?) ON CONFLICT(stream) DO UPDATE SET created = excluded.created, born_empty = excluded.born_empty`, streamName, created, bornEmpty ? 1 : 0);
        }
      }

      // D2 (NOTES §10n): seeding is SCOPED. A gap on one stream re-seeds only
      // the tables ROUTED to that stream; every other table resumes from its
      // stored position untouched — a mobile client reconnecting with one stale
      // stream must not rebuild its whole replica. A table with no generations
      // watermark has never been seeded at all (enabled between two connects,
      // or a brand-new replica) and seeds regardless of its stream's health.
      let seededBefore = new Set<string>();
      try {
        const rows: any[] = (await this.run(`SELECT tbl FROM _zebridge_generations`)) ?? [];
        seededBefore = new Set(rows.map((r: any) => r.tbl));
      } catch { /* fresh replica */ }
      const tableRoutes: Record<string, { route: string; sharedRoute?: string; seeded: boolean }> = {};
      for (const table of this.syncedTables.keys()) {
        if (this.ondemandSet.has(table)) continue; // §10hn: never seeded from a chain
        // Same null as above: a tenant-scoped table this principal cannot route is
        // left out of the seeding decision entirely rather than routed to a stream
        // name built from an empty token.
        const t = this.effectiveTenantFor(table);
        if (t === null) continue;
        const route = this.cdcStreamForTenant(t);
        const publicStream = this.config.grammar.cdc_streams?.public;
        tableRoutes[table] = {
          route,
          // A tenant-scoped table's OPEN-TENANT rows — the shared ones every tenant may
          // read — ride CDC_PUBLIC while its own ride CDC_<tenant> (§10bq). Both streams
          // must be gap-free or half the table goes quietly stale.
          ...(publicStream && route !== publicStream ? { sharedRoute: publicStream } : {}),
          seeded: seededBefore.has(table),
        };
      }
      // The decision is core.scopeSeeding (D2, §10n) — gapped streams plus
      // never-seeded tables, everything else untouched.
      const scoped = scopeSeeding(streamGaps, tableRoutes);
      const gapDetail = scoped.gapped.map(
        (g) => `${g}: local ${streamGaps[g].stored}, stream first ${streamGaps[g].firstSeq}`);
      const tablesToSeed = new Set(scoped.tablesToSeed);
      // The delta path (core.streamDeltaChoice): per stream with no gap, decided before any
      // object is fetched — every table depending on it may take its chain instead of the
      // CDC to the chain's cut. This pass runs at connect and at every reconnect.
      for (const stream of new Set(Object.values(tableRoutes).map((r) => r.route))) {
        if (scoped.gapped.includes(stream)) continue;
        for (const table of await this.streamDeltas(stream, tableRoutes, scoped.gapped, jsm)) { tablesToSeed.add(table); byChoice.add(table); }
      }
      // ZB_TEST_DELTA_PAUSE=<subject> (delta_revalidate.py): after a selection, ask the test
      // and wait for its reply — it replaces the approved manifest in between, so the
      // recheck at application is exercised deterministically. Node only.
      const pause = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.ZB_TEST_DELTA_PAUSE;
      if (byChoice.size && pause) {
        this.appendLog('SYS', 'test: paused between the delta path\'s selection and its application', 'INFO');
        try { await this.nc!.request(pause, new Uint8Array(0), { timeout: 30_000 }); } catch (e) { this.appendLog('SYS', `test: pause request failed (${e})`, 'WARNING'); }
      }
      const gap = tablesToSeed.size > 0;

      if (!gap) {
        this.appendLog('SYS', 'No CDC gap — resuming from stored positions, no seeding needed', 'INFO');
        this.reach('snapshot');
      } else {
        const untouched = this.syncedTables.size - tablesToSeed.size;
        // An empty replica — nothing seeded, no position anywhere — is a first run, the
        // normal start after an install or a wipe: said as such, not as a gap.
        const firstRun = seededBefore.size === 0 && scoped.gapped.every((g) => streamGaps[g].stored === 0);
        this.appendLog('SYS',
          `${firstRun ? 'First run — ' : gapDetail.length ? `Gap detected! ${gapDetail.join('; ')}. ` : ''}` +
          `${byChoice.size ? `Taking deltas over the CDC backlog for [${[...byChoice].join(', ')}]. ` : ''}` +
          `Seeding ${tablesToSeed.size} table(s) [${[...tablesToSeed].join(', ')}]` +
          `${untouched > 0 ? `; ${untouched} table(s) resume untouched` : ''}`, firstRun ? 'INFO' : 'WARNING');

        const seedPromises: Promise<void>[] = [];

        // Off for the duration of the bulk load — see the re-arm below. A no-op
        // inside a transaction, which is why it is here and not in a seed step.
        try { await this.dialect.setForeignKeys(this.run, false); } catch { /* engine without it */ }

        // ⚠️ ONE TABLE PER PROMISE, and that is the point.
        //
        // This used to be a sequential `for` that awaited each table's whole
        // request/retry cycle inline — so a single table that could not be seeded
        // blocked every table AFTER it for 5 attempts x 60 s. Measured: `orders`
        // never seeded at all and looked broken, when in fact `test_types` sat ahead
        // of it in the loop, orphaned and throttled, and `orders` was never reached.
        // Five minutes of head-of-line blocking presenting as data loss.
        //
        // A table that cannot seed is ITS OWN failure (`this.failed`), never a
        // reason to starve the rest.
        // No tail may apply events while a chain rewrites rows: the chain's state is as of
        // its cut, and an event the tail applied past the cut would be overwritten, then
        // skipped when the tail resumes (the tails are reopened in step 2).
        for (const stream of [...this.tailStops.keys()]) await this.stopTail(stream);
        for (const table of tablesToSeed) seedPromises.push((async () => {
          // Generations are the ONLY seeding path (NOTES.md §1.13, §10h): the
          // producer builds once on a cadence, every client catches up on deltas.
          if (byChoice.has(table)) this.applyingByChoice.add(table);
          let applied: boolean;
          try { applied = await this.applyGenerations(table, true); } finally { this.applyingByChoice.delete(table); }
          if (applied) {
            seededThisPass.add(table);
            this.reach('snapshot');
            return;
          }
          if (byChoice.has(table)) {
            // Steps already applied are version-guarded; the position has not moved.
            this.appendLog('SYS', `${table}: the delta path did not complete — replaying the CDC backlog instead`, 'INFO');
            return;
          }

          {
            // No usable chain yet — the ordinary case is a table created between two
            // cadence ticks. Wait for the producer rather than demanding a bespoke
            // dump: polling the chain is idempotent and cannot rewind anything,
            // which is precisely what the retired path below could not promise.
            this.appendLog('SYS', `${table}: no generation chain yet — waiting up to ${GENERATION_WAIT_MS / 1000}s for the producer (builds every GENERATION_CADENCE_SECONDS)`, 'INFO');
            const deadline = Date.now() + GENERATION_WAIT_MS;
            while (Date.now() < deadline) {
              await new Promise((r) => setTimeout(r, GENERATION_POLL_MS));
              if (await this.applyGenerations(table, true)) {
                this.reach('snapshot');
                return;
              }
            }
            // §10et: past the first window, the wait goes to the background — this
            // used to give up for the life of the process ("NOT following CDC for
            // it"), which on a 300 s cadence meant every table enabled between two
            // ticks, and every chain that predates its stream (§10ei), was lost until
            // a reload. Unseeded is NOT synced: the table stays registered, its
            // events are held (applyEvent), and the loop below asks for the chain
            // for as long as the connection lives, saying so once a minute.
            this.appendLog('SYS', `${table}: no generation chain after ${GENERATION_WAIT_MS / 1000}s — its events are held and the chain is asked for every ${GENERATION_SLOW_POLL_MS / 1000}s; the table seeds the moment the producer builds one (GENERATIONS_ENABLED, its cadence, the gen-<tenant> object store)`, 'WARNING');
            this.waitForChain(table);
            return;
          }
        })().catch((e) => {
          // One table's failure is one table's failure.
          this.failed.add(table);
          this.appendLog('SYS', `Seeding ${table} failed: ${e}`, 'ERROR');
        }));

        await Promise.all(seedPromises);

        // ⚠️ Re-arm referential integrity, and CHECK what the bulk load produced.
        // Seeding runs every table CONCURRENTLY (the Promise.all above), so with
        // enforcement on, a child's rows hit the constraint before its parent's have
        // landed — measured: `orders` seeded from a chain holding 6,500 rows applied
        // NOTHING, silently, because `users` was still loading beside it. A seed is a
        // bulk load of an ALREADY-CONSISTENT snapshot: enforcing order during it is
        // both unnecessary and actively wrong. The standard SQLite bulk-load shape —
        // off during load, on after, then verify.
        try {
          await this.dialect.setForeignKeys(this.run, true);
          const bad = await this.dialect.foreignKeyViolations(this.run);
          if (bad > 0) {
            this.appendLog('SYS', `⚠️ ${bad} foreign key violation(s) survive seeding — the seeded set is not self-consistent`, 'ERROR');
          } else if (bad < 0) {
            this.appendLog('SYS', `foreign keys re-enabled; this engine (${this.dialect.name}) offers no cheap post-load check`, 'INFO');
          }
        } catch { /* engine without the pragma */ }
        if (this.failed.size > 0) {
          this.appendLog('SYS', `Seeding done, but ${this.failed.size} table(s) could not be seeded and are excluded: ${[...this.failed].join(', ')}`, 'WARNING');
        } else {
          this.appendLog('SYS', `All required tables seeded successfully!`, 'INFO');
        }
      }
    } catch (e) {
      this.appendLog('SYS', `Failed to resolve gap and seed tables: ${e}`, 'ERROR');
    }

    // 2. CDC consumers, ONLY AFTER seeding is resolved — one consumer per stream,
    // because a consumer belongs to exactly one stream and the stream is the ACL
    // boundary. The subject filter below is efficiency only (§10gm): it narrows what
    // THIS reader pulls to its own tables and can never widen it, so the boundary is
    // still the stream's name.
    try {
      for (const streamName of this.cdcStreams()) {
        // A fence is rewinding this route: its end reopens the tail from the cursor it set.
        const owners = this.fenced.get(streamName);
        if (owners) {
          for (const o of [...owners]) if (!this.syncedTables.has(o) || this.ondemandSet.has(o)) owners.delete(o); // gone: nothing is owed
          if (this.routeParked(streamName)) {
            this.appendLog('SYS', `${streamName}: parked by ${[...owners].join(', ')} — its tail opens when the last of them is through`, 'INFO');
            continue;
          }
          this.fenced.delete(streamName);
        }
        const setupStart = performance.now();
        await this.stopTail(streamName);
        // A stream that is fully consumed (or empty) delivers no message, so the
        // per-batch persist below never fires — record a floor now, or a quiet
        // stream reads as `local 0` forever and re-seeds every reconnect.
        //
        // ⚠️ The floor comes from what the SEED proved, never from the stream's
        // current tail. The chain's cutoff can be minutes older than "now", and
        // every row written in between must REPLAY through this consumer: under
        // a 12-client swarm writing 40 rows/s, consumer setup took 95 s and
        // recording the tail silently skipped ~340 rows per replica — permanent
        // holes that no reconnect heals (found by swarm.py, NOTES §10cp). The
        // per-event gate (core seedSeq/seedStream, finding 10's lsn fallback)
        // makes a floor that is too LOW merely cheap duplicates; a floor too
        // HIGH is data loss. Only a stream with no chain-seeded tables still
        // takes the tail — the quiet-stream case this block exists for.
        try {
          const seeded = [...this.syncedTables.values()].filter((st) => st.seedLsn != null);
          const seedFloors = seeded
            .filter((st) => st.seedStream === streamName && typeof st.seedSeq === 'number')
            .map((st) => st.seedSeq as number);
          // Seeded tables whose manifest carried no cutoff_seq: floor 0 — the
          // consumer replays from the stream's start and the lsn gate drops what
          // the seed covered. Cheap (retention is short), and never lossy.
          const floor = seedFloors.length
            ? Math.min(...seedFloors)
            : (seeded.length ? 0 : ((await jsm.streams.info(streamName))?.state?.last_seq ?? 0));
          const stored0 = this.globalSyncState.seq[streamName] ?? 0;
          // §10ei, §10go, §10lw: where the position goes is core.streamResume, from what
          // each chain PROVED on this stream — a table's own cut when this is its route,
          // its shared cut when this is CDC_PUBLIC and the table is tenant-scoped (its
          // open-tenant rows ride here). Coverage, never "seeded at some point": a cut
          // below the stream's oldest message certifies nothing about the messages it
          // dropped, and moving past them loses rows for good — measured on the first
          // heal, 1,081,522 of 1,800,000 rows at 60k events a second. Before §10lw only
          // the own cut counted, so a tenant table's client kept CDC_PUBLIC at 0 and read
          // a false gap at every connect.
          const pub = this.config.grammar.cdc_streams?.public;
          const cuts: (number | null)[] = [];
          for (const table of this.syncedTables.keys()) {
            if (this.ondemandSet.has(table)) continue;
            const t = this.effectiveTenantFor(table);
            if (t === null) continue;
            const route = this.cdcStreamForTenant(t);
            const st = this.syncedTables.get(table);
            const failed = this.failed.has(table);
            if (route === streamName) {
              cuts.push(!failed && st?.seedStream === streamName && typeof st.seedSeq === 'number' ? st.seedSeq : null);
            } else if (pub === streamName && route !== pub) {
              cuts.push(!failed && typeof st?.sharedSeedSeq === 'number' ? st.sharedSeedSeq : null);
            }
          }
          let firstSeq = 0;
          try { firstSeq = (await jsm.streams.info(streamName))?.state?.first_seq ?? 0; } catch { /* keep the position */ }
          // A stream no seeded table depends on takes the tail: the quiet-stream case.
          const decided = cuts.length && seeded.length ? streamResume(stored0, firstSeq, cuts) : { to: stored0 > 0 ? stored0 : floor, blocked: false };
          // A table reseeded in this pass now holds the chain's state as of its cut on this
          // stream. When the position is past that cut — the stream itself had no gap, a gap
          // on the table's OTHER route (the shared stream) reseeded it — the events between
          // the cut and the position were applied and then overwritten by the chain, deletes
          // included: they must replay. Measured before this rule: a client kept 4,000 rows
          // PostgreSQL had deleted (gen_follow, shared-stream gap at 2,000 ids/s).
          let rewindTo = decided.to;
          for (const table of seededThisPass) {
            const st = this.syncedTables.get(table);
            const t = this.effectiveTenantFor(table);
            if (!st || t === null) continue;
            const route = this.cdcStreamForTenant(t);
            const cut = route === streamName
              ? (st.seedStream === streamName && typeof st.seedSeq === 'number' ? st.seedSeq : null)
              : (pub === streamName && route !== pub && typeof st.sharedSeedSeq === 'number' ? st.sharedSeedSeq : null);
            if (cut === null || cut >= rewindTo) continue;
            if (firstSeq > 1 && cut < firstSeq - 1) {
              this.appendLog('SYS', `${streamName}: ${table}'s chain was cut at ${cut}, before the stream's oldest message (${firstSeq}) — the events after it cannot replay`, 'ERROR');
              continue;
            }
            rewindTo = cut;
          }
          if (rewindTo < decided.to) {
            this.appendLog('SYS', `${streamName}: rewound to ${rewindTo} (was ${decided.to}) — a table reseeded here replays the events after its chain's cut`, 'WARNING');
            this.globalSyncState.seq[streamName] = rewindTo;
            await this.run(
              `INSERT INTO _zebridge_stream_seq (stream, last_seq) VALUES (?, ?)
               ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq`,
              streamName, rewindTo,
            );
          }
          if (decided.blocked) {
            this.appendLog('SYS', `${streamName}: the gap stays open — no chain past the stream's oldest message (${firstSeq}) yet; waiting for the producer's next generation`, 'WARNING');
          }
          // A blocked gap retries on the next pass, not in a tight loop: without a pause
          // the tail re-opens, meets the same hole and re-seeds immediately (§10go).
          this.gapBackoffMs = decided.blocked ? Math.min(5_000, (this.gapBackoffMs || 250) * 2) : 0;
          // A route under reconciliation healed when its position is decided and every
          // table riding it has a chain applied again (an empty stream's chain has no cut).
          if (this.hubOnly.has(streamName) && !decided.blocked) {
            const have = new Set(((await this.run(`SELECT tbl FROM _zebridge_generations`)) ?? []).map((r: any) => String(r.tbl)));
            const riding = [...this.syncedTables.keys()].filter((t) => {
              if (this.ondemandSet.has(t)) return false;
              const tn = this.effectiveTenantFor(t);
              return tn !== null && this.cdcStreamForTenant(tn) === streamName;
            });
            if (riding.every((t) => have.has(t) && !this.failed.has(t))) this.healedRoutes.add(streamName);
          }
          // The delta path's other half (core.streamForward): with no gap, the stream moves to
          // the lowest cut of the chains applied THIS pass when every table depending on
          // it took one; the events up to there are in them. Otherwise it stays, and the
          // seed gate drops what the chains carried.
          if (decided.to === stored0 && rewindTo === stored0 && seededThisPass.size) {
            const passCuts: (number | null)[] = [];
            for (const table of this.syncedTables.keys()) {
              if (this.ondemandSet.has(table)) continue;
              const t = this.effectiveTenantFor(table);
              if (t === null) continue;
              const route = this.cdcStreamForTenant(t);
              const st = this.syncedTables.get(table);
              const took = seededThisPass.has(table) && !this.failed.has(table);
              if (route === streamName) {
                passCuts.push(took && st?.seedStream === streamName && typeof st.seedSeq === 'number' ? st.seedSeq : null);
              } else if (pub === streamName && route !== pub) {
                passCuts.push(took && typeof st?.sharedSeedSeq === 'number' ? st.sharedSeedSeq : null);
              }
            }
            const to = streamForward(stored0, passCuts);
            if (to > stored0) {
              this.appendLog('SYS', `${streamName}: forwarded to ${to} (was ${stored0}) — every table on it took its chain's deltas; the CDC between is in them`, 'INFO');
              this.globalSyncState.seq[streamName] = to;
              await this.run(
                `INSERT INTO _zebridge_stream_seq (stream, last_seq) VALUES (?, ?)
                 ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq`,
                streamName, to,
              );
              await this.spendFrontiers(streamName, to);
            }
          }
          if (decided.to > stored0 && rewindTo >= decided.to) {
            if (stored0 > 0) this.appendLog('SYS', `${streamName}: gap healed — resuming at ${decided.to} (was ${stored0}, stream holds from ${firstSeq})`, 'INFO');
            this.globalSyncState.seq[streamName] = decided.to;
            await this.run(
              `INSERT INTO _zebridge_stream_seq (stream, last_seq) VALUES (?, ?)
               ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq`,
              streamName, decided.to,
            );
            await this.spendFrontiers(streamName, decided.to);
          }
        } catch { /* stream info unavailable — the per-batch persist still covers it */ }
        // A reconciliation whose seed could not splice must not replay from zero.
        if (this.hubOnly.has(streamName) && !this.healedRoutes.has(streamName)) continue;
        const filters = this.cdcFilters(streamName);
        // No followed table rides this stream (on-demand only, or none yet): no tail. An
        // unfiltered consumer would hand this client every event of the stream, for
        // nothing — measured: 250 such clients × 32,000 events overran their event
        // loops into reconnect storms. libzb opens none either.
        if (!filters.length) { this.tailFilters.delete(streamName); continue; }
        const last = this.globalSyncState.seq[streamName] ?? 0;
        this.tailFilters.set(streamName, filters.join('|'));
        const ci = await jsm.consumers.add(streamName, {
          deliver_policy: last > 0 ? this.transport.deliverPolicy.byStartSequence : this.transport.deliverPolicy.all,
          opt_start_seq: last > 0 ? last + 1 : undefined,
          // One filter is understood by every server; several need nats-server >= 2.10.
          ...(filters.length === 1 ? { filter_subject: filters[0] } : {}),
          ...(filters.length > 1 ? { filter_subjects: filters } : {}),
          inactive_threshold: TAIL_INACTIVE_NS,
        });
        const consumer = await js.consumers.get(streamName, ci.name);
        const setupMs = Math.round(performance.now() - setupStart);
        // ZB_MEASURE: a tail opening at or past its target has nothing to catch up.
        this.measure.positions(this.globalSyncState.seq);
        this.appendLog('SYS', `CDC consumer on ${streamName} (from seq ${last || 'all'}, ${ci.num_pending ?? '?'} messages pending, consumer setup took ${setupMs}ms)`, 'INFO');

        const iter = await consumer.consume();
        const myGen = (this.tailGen.get(streamName) ?? 0) + 1;
        this.tailGen.set(streamName, myGen);
        const superseded = () => this.tailGen.get(streamName) !== myGen;
        void (async () => {
          // The tail must OUTLIVE its consumer. A consumer born into reconnect churn
          // can go DEAF — created "from seq N, 0 pending" and never delivering again
          // while the stream advances (measured §10cq: stored 3236, stream at 3353,
          // sibling replicas on other streams healthy). Whether the iterator hangs or
          // ends, this loop notices — an idle guard stops a deaf iterator when data
          // is provably waiting — and recreates the consumer from the stored
          // position. Duplicates are idempotent; silence is data loss.
          // ⚠️ A consumed iterator must NEVER be iterated again: @nats-io throws
          // InvalidOperationError ("iterator is already yielding") on the second
          // iterate() — even after the first ended — and inside this
          // fire-and-forget IIFE that throw is an unhandled rejection that kills
          // the whole PROCESS (measured: five clients per casualty, 25 of 100
          // clients dead in the §10cs soak). `curIter` is nulled the moment its
          // for-await returns; a null iterator means "recreate first".
          let curIter: typeof iter | null = iter;
          let curName: string = ci.name;
          let curPending: number | null = ci.num_pending ?? null;
          let attempt = 0;
          let flushWork: Promise<void> | null = null;
          let idleWork: Promise<void> | null = null;
          let idleGuard: ReturnType<typeof setInterval> | null = null;
          let discardBatch = () => {};
          this.tailStops.set(streamName, async () => {
            discardBatch();
            if (idleGuard) clearInterval(idleGuard);
            try { curIter?.stop(); } catch { /* already stopped */ }
            // Work already inside a storage transaction must settle BEFORE the rewind.
            await Promise.all([flushWork, idleWork]);
          });
          // §10ei: the last sequence this tail was handed — the gap rule, LIVE. The next
          // message is `lastSeen + 1` unless the stream pruned under the consumer (a tab
          // throttled in the background, a slow apply) and the server says nothing — or
          // (§10ja) the consumer is filtered to this client's tables and jumped over
          // other tables' messages, which is no hole: on a jump the stream is asked
          // whether it still holds what follows the position. libzb measured it first: twelve messages, two
          // deletes, a replica that disagreed until reopened.
          let lastSeen = last;
          let holeFound = false;
          while (this.nc && !superseded()) {
          if (!curIter) {
            attempt++;
            await new Promise((r) => setTimeout(r, 1000));
            if (superseded()) break;
            try {
              const resumeFrom = this.globalSyncState.seq[streamName] ?? 0;
              // The same filter as the first consumer (§10ja: the recreation used to drop
              // it, and the tail then read every table on the stream).
              const filters2 = this.cdcFilters(streamName);
              const ci2 = await jsm.consumers.add(streamName, {
                deliver_policy: resumeFrom > 0 ? this.transport.deliverPolicy.byStartSequence : this.transport.deliverPolicy.all,
                opt_start_seq: resumeFrom > 0 ? resumeFrom + 1 : undefined,
                ...(filters2.length === 1 ? { filter_subject: filters2[0] } : {}),
                ...(filters2.length > 1 ? { filter_subjects: filters2 } : {}),
                inactive_threshold: TAIL_INACTIVE_NS,
              });
              curName = ci2.name;
              const consumer2 = await js.consumers.get(streamName, ci2.name);
              curPending = ci2.num_pending ?? null;
              curIter = await consumer2.consume();
              if (superseded()) { curIter.stop(); break; }
              lastSeen = resumeFrom;
              this.appendLog('SYS', `${streamName}: tail recreated (attempt ${attempt}) from seq ${resumeFrom}, ${curPending ?? '?'} pending`, 'WARNING');
            } catch (e) {
              this.appendLog('SYS', `${streamName}: tail recreate failed (${e}) — retrying`, 'ERROR');
              await new Promise((r) => setTimeout(r, 4000));
              continue;
            }
          }
          let processedSinceStart = 0;
          let caughtUpLogged = curPending === 0;
          // §10jc: this consumer's delivery numbering (+1 per delivery, by the server). A
          // jump is a delivery lost in transit: what came before it is applied, nothing
          // after it is (neither applied nor acked), and the tail is recreated from the
          // position — the server re-sends from the lost message on, IN ORDER. Applying
          // past the gap and filling it later lost updates: the lost message is older than
          // what followed (an INSERT re-sent over its own later UPDATE, 5 batches).
          let cseq = 0;
          const progressEvery = 2000;

          // Batched into ONE transaction per flush: N autocommits each pay OPFS
          // commit/fsync, one transaction of N pays it once. Messages are acked only
          // after their batch's transaction committed.
          const BATCH_SIZE = this.config.cdcBatchEvents ?? 20_000;
          const BATCH_MS = 200;
          let batch: { table: string; ev: any }[] = [];
          let batchMsgs: any[] = [];
          let flushTimer: ReturnType<typeof setTimeout> | null = null;

          discardBatch = () => {
            if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
            batch = []; batchMsgs = [];
          };
          const applyBuffered = async () => {
            if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
            if (superseded()) { discardBatch(); return; }
            if (!batch.length) return;
            const toApply = batch;
            const toAck = batchMsgs;
            batch = [];
            batchMsgs = [];
            // Measurements are staged with the transaction, never published by a hold
            // that a failed COMMIT can roll back. Isolated replay supplies its own counts.
            let mc: MeasureCounts | undefined;
            const mt = this.measure.open(streamName);
            const attempt = this.measurementAttempt(streamName, toApply);
            try {
              await this.transaction(async (txExec) => {
                if (superseded()) return;
                // PROTOCOL.md §4's FK rule in executable form: enforcement waits for
                // this batch's COMMIT, so a child arriving before its parent inside
                // one batch cannot fail the apply.
                await this.dialect.deferForeignKeys(txExec);
                if (this.config.bulkCdc === false) {
                  for (const { table, ev } of toApply) await this.applyEvent(table, ev, txExec, false, attempt);
                } else {
                  await this.applyBatchPlanned(toApply, txExec, attempt);
                }
              });
              if (superseded()) return;
              mc = attempt?.snapshot();
              for (const { table, ev } of toApply) this.triggerChange(table, ev);
            } catch (err) {
              if (superseded()) return;
              // ⚠️ This used to log and fall through to the ack below, so ONE bad event
              // silently discarded the other 99 and the replica still reported itself
              // caught up. Measured 2026-08-26: six batches × 100 events dropped that
              // way during the Node-consumer work, from a single adapter bug.
              //
              // Isolate instead: replay the batch ONE EVENT AT A TIME, each in its own
              // transaction, so only the genuinely bad event is affected.
              mc = await this.applyBatchIsolated(streamName, toApply, String(err));
            }
            if (superseded()) return;
            // Held events are accounted for (retried after later batches), so acking
            // here is correct — what must never happen again is acking events that
            // were neither applied nor held.
            for (const m of toAck) m.ack();
            // §10kj: the server's count of what is behind the batch's last message —
            // the heartbeat reports it as this stream's backlog.
            const lastMsg = toAck[toAck.length - 1];
            if (lastMsg && typeof lastMsg.info?.pending === 'number') this.lastPending[streamName] = lastMsg.info.pending;

            // ── ADVANCE THE STREAM POSITION FOR EVERY DELIVERED MESSAGE ──
            // It used to advance only inside applyEvent, whose early returns (the
            // seed gate, the FK hold, the schema hold) skipped it — so a stream whose
            // delivered events were all gated or held NEVER persisted a position,
            // read as `local 0` on the next connect, and forced a FULL RE-SEED on
            // every reconnect (measured in a clean room: run 1 applied everything
            // and stream_seq still lacked CDC_PUBLIC, because its one event was
            // correctly gate-dropped; run 2 then gap-detected and re-seeded with no
            // new data). Delivery + accounting IS the position: an applied event is
            // in the tables, a gated one is provably in the seeded chain, a held one
            // is durably in the inbox — none of them needs redelivery.
            const maxSeq = advancePosition(0, toAck.map((msg) => msg.seq ?? 0));
            if (maxSeq > (this.globalSyncState.seq[streamName] ?? 0)) {
              this.globalSyncState.seq[streamName] = maxSeq;
              try {
                await this.run(
                  `INSERT INTO _zebridge_stream_seq (stream, last_seq) VALUES (?, ?)
                   ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq`,
                  streamName, maxSeq,
                );
                await this.spendFrontiers(streamName, maxSeq);
              } catch (e) {
                this.appendLog('SQLITE', `Failed to persist ${streamName} position: ${e}`, 'ERROR');
              }
            }
            if (mt && mc) {
              const inTarget = toAck.filter((m) => (m.seq ?? 0) <= mt.target);
              mc.messages = inTarget.length;
              // Payload bytes as delivered (msgpack, before decoding): CDC's transfer cost.
              mc.bytes = inTarget.reduce((n, m) => n + (m.data?.length ?? 0), 0);
              this.measure.add(streamName, mc);
              if (maxSeq >= mt.target) this.measure.reached(streamName, 'position', maxSeq);
              else if (lastMsg && lastMsg.info?.pending === 0) this.measure.reached(streamName, 'pending', maxSeq);
            }
            await this.retryFkHeld(streamName);
          };
          const flushBatch = () => {
            // Chain every caller, including timers that fire while another caller is
            // waiting. stopTail must await the entire queue, not just its first job.
            const work = (flushWork ?? Promise.resolve()).then(applyBuffered);
            flushWork = work;
            return work.finally(() => { if (flushWork === work) flushWork = null; });
          };

          let lastMsgAt = Date.now();
          const itRef = curIter;
          idleGuard = setInterval(() => {
            if (superseded() || idleWork || Date.now() - lastMsgAt < 25_000) return;
            idleWork = (async () => {
              try {
                // §10ja: ask the CONSUMER, not the stream. "The stream's last sequence is
                // past my position" is always true for a consumer filtered to this
                // client's tables (other tables advance the stream), and for a stream
                // whose messages all aged out — both read as deaf every 25 s, for ever.
                // The consumer's own `num_pending` counts the messages it should hand
                // over: some waiting and none delivered is deaf; none waiting is idle; a
                // consumer the server no longer knows is gone, recreated as before.
                const stored = this.globalSyncState.seq[streamName] ?? 0;
                // The stream's end FIRST, then the consumer: see caughtUpPosition.
                let lastSeq = 0;
                let firstSeq = 0;
                try {
                  const st = (await jsm.streams.info(streamName))?.state;
                  lastSeq = st?.last_seq ?? 0;
                  firstSeq = st?.first_seq ?? 0;
                } catch { /* keep 0: no move */ }
                let pending = -1;
                let ci: any = null;
                try { ci = await jsm.consumers.info(streamName, curName); pending = ci?.num_pending ?? 0; } catch { /* gone */ }
                if (superseded()) return;
                if (pending !== 0) {
                  this.appendLog('SYS', `${streamName}: consumer idle 25s with ${pending < 0 ? 'no consumer on the server' : `${pending} message(s) waiting`} (stored ${stored}) — deaf; recreating`, 'WARNING');
                  try { itRef.stop(); } catch { /* already ended */ }
                  return;
                }
                // §10jc: nothing in hand, yet deliveries unacknowledged — lost in transit;
                // the server re-sends them only after ack_wait. Recreate from the position.
                if ((ci?.num_ack_pending ?? 0) > 0 && batch.length === 0) {
                  this.appendLog('SYS', `${streamName}: consumer idle 25s with ${ci.num_ack_pending} delivery(ies) unacknowledged — lost in transit; recreating from ${stored}`, 'WARNING');
                  try { itRef.stop(); } catch { /* already ended */ }
                  return;
                }
                // §10jh: nothing pending, yet the stream no longer holds what follows the
                // position — dropped before it was delivered (a follower behind a slow
                // link past the stream's window). No delivery will ever show this hole:
                // take the gap as a live hole is taken, and let the resync heal it from
                // the chain.
                if (stored > 0 && firstSeq > stored + 1 && (ci?.num_ack_pending ?? 0) === 0) {
                  this.appendLog('SYS', `${streamName}: the stream dropped ${firstSeq - stored - 1} message(s) after position ${stored} before they were delivered — healing from the chain`, 'WARNING');
                  holeFound = true;
                  try { itRef.stop(); } catch { /* already ended */ }
                  return;
                }
                // §10ja: idle and caught up — the position is the stream's end, or a
                // consumer filtered to other tables' silence stays at 0 and the next
                // launch reads that as a gap.
                const to = caughtUpPosition(stored, lastSeq, {
                  firstSeq,
                  numPending: pending, numAckPending: ci?.num_ack_pending ?? 1,
                  deliveredCount: ci?.delivered?.consumer_seq ?? 1, delivered: ci?.delivered?.stream_seq ?? Number.MAX_SAFE_INTEGER,
                });
                if (to > (this.globalSyncState.seq[streamName] ?? 0)) {
                  this.globalSyncState.seq[streamName] = to;
                  lastSeen = Math.max(lastSeen, to);
                  await this.run(
                    `INSERT INTO _zebridge_stream_seq (stream, last_seq) VALUES (?, ?)
                     ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq`,
                    streamName, to,
                  );
                  await this.spendFrontiers(streamName, to);
                }
              } catch { /* stream info unavailable mid-outage — keep waiting */ }
            })().finally(() => { idleWork = null; });
          }, 10_000);
          try {
          for await (const msg of curIter) {
            if (superseded()) {
              this.appendLog('SYS', `${streamName}: tail ${curName} superseded by a newer tail — stopping without applying what it holds`, 'INFO');
              batch = []; batchMsgs = [];
              try { itRef.stop(); } catch { /* already ended */ }
              break;
            }
            lastMsgAt = Date.now();
            // §10jc test hook (as libzb's): every Nth delivery discarded on arrival — not
            // applied, not acked, its delivery sequence never seen: a loss in transit.
            if (TEST_DROP_EVERY > 0 && ++testDropCount % TEST_DROP_EVERY === 0) {
              this.appendLog('SYS', `test: delivery of seq ${msg.seq} discarded as lost in transit`, 'WARNING');
              continue;
            }
            // §10jc: a lost delivery is a jump in the consumer's own numbering: stop here —
            // this message and whatever follows stay unacked, the batch in hand is flushed
            // after the loop, and the tail is recreated from the position.
            const dseq = Number(msg.info?.deliverySequence ?? 0);
            if (dseq > cseq + 1) {
              this.appendLog('SYS', `${streamName}: delivery lost in transit on ${curName} (consumer seq ${dseq} after ${cseq}) — applying up to the gap, then recreating the tail from the position`, 'WARNING');
              try { itRef.stop(); } catch { /* already ended */ }
              break;
            }
            if (dseq > cseq) cseq = dseq;
            // §10jc: at or below the position is a redelivery — acked, never re-applied.
            const posNow = Math.max(this.globalSyncState.seq[streamName] ?? 0, advancePosition(0, batchMsgs.map((bm) => bm.seq ?? 0)));
            if (msg.seq <= posNow) {
              msg.ack();
              continue;
            }
            if (lastSeen > 0 && msg.seq > lastSeen + 1 && await this.prunedAfter(jsm, streamName, lastSeen)) {
              this.appendLog('SYS', `${streamName}: ${msg.seq - lastSeen - 1} message(s) pruned under the live consumer (position ${lastSeen}, delivered ${msg.seq}) — taking the gap: re-seeding the tables routed to it`, 'WARNING');
              holeFound = true;
              try { itRef.stop(); } catch { /* already ended */ }
              break;
            }
            if (superseded()) break;
            lastSeen = msg.seq;
            processedSinceStart++;
            if (processedSinceStart % progressEvery === 0) {
              this.appendLog('SYS', `${streamName} catch-up: ${processedSinceStart} messages processed so far, at seq ${msg.seq}; bulk ${this.bulkStats.bulked} events in ${this.bulkStats.statements} statements, ${this.bulkStats.single} per event, ${this.bulkStats.fallbacks} fallbacks`, 'INFO');
            }
            if (!caughtUpLogged && curPending != null && processedSinceStart >= curPending) {
              caughtUpLogged = true;
              const totalMs = Math.round(performance.now() - setupStart);
              this.appendLog('SYS', `${streamName} caught up (${processedSinceStart} messages, ${totalMs}ms total since consumer setup started) — now live; bulk ${this.bulkStats.bulked} events in ${this.bulkStats.statements} statements, ${this.bulkStats.single} per event, ${this.bulkStats.fallbacks} fallbacks`, 'INFO');
            }
            let decoded: any;
            try {
              decoded = decode(msg.data); // CDC events are always msgpack
            } catch (err) {
              // Caught, not thrown: an uncaught throw in this fire-and-forget IIFE
              // silently ends CDC for the whole stream.
              this.appendLog('SYS', `CDC event on ${streamName} failed to decode (seq ${msg.seq}): ${err} — skipping this message`, 'ERROR');
              msg.ack();
              continue;
            }
            const events = Array.isArray(decoded) ? decoded : [decoded];

            for (const ev of events) {
              ev.seq = msg.seq;
              ev.stream = streamName;
              // cdc.<tenant>.<table>.<op> has the table at [2]; cdc.<table>.<op> at [1].
              const parts = msg.subject.split('.');
              const table = ev?.table || (parts.length >= 4 ? parts[2] : parts[1]);
              this.appendLog(msg.subject, ev, ev?.operation || 'CDC');
              if (table) batch.push({ table, ev });
            }
            batchMsgs.push(msg);

            // ⚠️ The timer is for a BURST, not for a lone event. JetStream tells us per
            // message how many are still pending for this consumer; when this one was
            // the last in flight, waiting BATCH_MS gains nothing and costs exactly
            // BATCH_MS — measured in the browser 2026-08-29: a write's CDC echo landed
            // at 235 ± 3 ms while the verdict took 26 ms and a Zig client saw the same
            // echo in 3 ms. Flush now when nothing follows; batch when something does.
            const lastInFlight = typeof msg.info?.pending === 'number' && msg.info.pending === 0;
            if (batch.length >= BATCH_SIZE || lastInFlight) {
              await flushBatch();
            } else if (!flushTimer) {
              flushTimer = setTimeout(() => { void flushBatch(); }, BATCH_MS);
            }
          }
          } catch (e) {
            // Contained, whatever it is: a tail must never take the process down.
            this.appendLog('SYS', `${streamName}: tail iterator errored (${e}) — will recreate`, 'ERROR');
          } finally { clearInterval(idleGuard); }
          if (superseded()) { discardBatch(); break; }
          await flushBatch();
          curIter = null; // consumed — never iterate it again (see above)
          if (!this.nc) break;
          if (holeFound) {
            // The position stays below the hole, so the gap rule sees it: the resync
            // seeds what rides this stream and opens a fresh tail from there. This
            // loop ends — two tails on one stream would race the position.
            if (!this.resyncing) {
              this.resyncing = true;
              const wait = this.gapBackoffMs;
              void (async () => {
                if (wait > 0) await new Promise((r) => setTimeout(r, wait));
                await this.subscribeStreams();
              })().catch(() => {}).finally(() => { this.resyncing = false; });
            }
            break;
          }
          this.appendLog('SYS', `${streamName}: tail ended — recreating from the stored position`, 'WARNING');
          }
        })();
      }
    } catch (e) {
      this.appendLog('SYS', `Failed to start CDC consumer: ${e}`, 'ERROR');
    }
    this.streamsSubscribed = true;
  }

  /// A table followed after connect (a schema arriving later): a stream whose tail
  /// lacks it, or has no tail at all, is subscribed again with the right filters.
  private ensureTails() {
    if (!this.nc || !this.streamsSubscribed || this.resyncing) return;
    const stale = this.cdcStreams().some((stream) => {
      const want = this.cdcFilters(stream).join('|');
      return want !== (this.tailFilters.get(stream) ?? '');
    });
    if (!stale) return;
    this.resyncing = true;
    void this.subscribeStreams().catch(() => {}).finally(() => { this.resyncing = false; });
  }

  // ── the write path (PROTOCOL.md §7) ───────────────────────────────────────

  /// The verdicts this client MISSED — published while it was offline, or before this
  /// process existed (PROTOCOL §7.4b). The outbox knows every msg_id it awaits and a
  /// verdict is one retained message on `mutation_ack.<principal>.<msg_id>`, so one
  /// per-key direct get per pending entry answers "was this judged?" and settles it
  /// through the same handler the live subscription uses. A 404 means not judged (or
  /// judged longer ago than MUTATIONS keeps) — the entry stays and is replayed.
  ///
  /// ⚠️ This is what makes "verdicts are stored so an offline client can collect them"
  /// (PROTOCOL §2) TRUE. Until 2026-08-29 no client read a stored verdict: convergence
  /// came from the REPLAY earning a fresh one, which costs a second PostgreSQL write
  /// attempt per entry and, past the duplicate window, a `stale` for a write that had
  /// in fact been accepted. The grant is per key —
  /// `$JS.API.DIRECT.GET.VERDICTS.mutation_ack.<principal>.>` — scoped like `$KV.tenants`.
  private async collectMissedVerdicts(rows: any[], manager?: Promise<any>): Promise<Set<string>> {
    const settled = new Set<string>();
    if (!this.nc || !rows.length) return settled;
    const stream = this.config.grammar.streams?.verdicts ?? 'VERDICTS';
    let jsm: any;
    try { jsm = await (manager ?? this.transport.jetstreamManager(this.nc, this.jsOpts())); } catch { return settled; }
    // ⚠️ The DIRECT form only. `jsm.streams.getMessage` is the legacy
    // `$JS.API.STREAM.MSG.GET.<stream>` request, which the client JWT does not grant
    // (and must not: it is not scopable per key). Without `jsm.direct` there is
    // nothing to collect with, and the replay path answers as before.
    if (!jsm.direct?.getMessage) {
      this.appendLog('OUTBOX', 'this NATS client has no direct-get API — stored verdicts cannot be collected, replaying instead', 'WARNING');
      return settled;
    }
    let firstFailure: string | null = null;
    for (const r of rows) {
      const subject = `${this.ackPrefix()}.${this.config.principal}.${r.msg_id}`;
      try {
        const m = await jsm.direct.getMessage(stream, { last_by_subj: subject });
        if (!m) continue;
        const data: Uint8Array = m.data instanceof Uint8Array ? m.data : new TextEncoder().encode(String(m.data ?? ''));
        // Retained failures do not settle a write, and their relative retry hint
        // is historical. Only a live failure may start a new hold interval.
        if (JSON.parse(new TextDecoder().decode(data)).status === 'failed') continue;
        if (await this.handleVerdict(m.subject ?? subject, data)) {
          settled.add(r.msg_id);
          this.wakeOutbox();
        }
      } catch (e: any) {
        // A 404 is the ordinary answer (not judged yet, or aged out); anything else —
        // a permission violation, a timeout — is worth one line, or a grant mistake
        // reads as "verdicts never get collected" with no symptom.
        const text = String(e?.message ?? e);
        if (!/404|no message found|not found/i.test(text) && firstFailure === null) firstFailure = text;
      }
    }
    if (firstFailure) this.appendLog('OUTBOX', `collecting stored verdicts failed (${firstFailure}) — replaying instead`, 'WARNING');
    if (settled.size) this.appendLog('OUTBOX', `collected ${settled.size} verdict(s) published while this client was away — settled without replay`, 'INFO');
    return settled;
  }

  private async watchVerdicts() {
    if (!this.nc) return;
    const sub = this.nc.subscribe(`${this.ackPrefix()}.${this.config.principal}.>`);
    void (async () => {
      for await (const m of sub) {
        if (await this.handleVerdict(m.subject, m.data)) this.wakeOutbox();
      }
    })();
  }

  /// One verdict, live or collected: true when it was definitive (the outbox entry is
  /// settled one way or another), false for `failed` or an unknown status (kept).
  /// A write's outcome is reported once. A verdict for an ID being handled, or already
  /// handled to the end, does nothing; the ID is marked done only when the handling returned,
  /// so a failure on the way (an outbox delete, a revert) lets a later delivery retry it.
  private async handleVerdict(subject: string, data: Uint8Array): Promise<boolean> {
    const prefix = `${this.ackPrefix()}.${this.config.principal}.`;
    const msgId = subject.startsWith(prefix) ? subject.slice(prefix.length) : subject;
    let status = '';
    try { status = String(JSON.parse(new TextDecoder().decode(data))?.status ?? ''); } catch { /* the handler reports it */ }
    const tracked = msgId !== 'revoked' && status !== 'failed';
    if (tracked && (this.verdictReported?.has(msgId) || this.verdictInFlight?.has(msgId))) return true;
    if (tracked) (this.verdictInFlight ??= new Set()).add(msgId);
    try {
      const ok = await this.handleVerdictOnce(subject, data);
      if (tracked) this.markReported(msgId);
      return ok;
    } finally {
      if (tracked) this.verdictInFlight!.delete(msgId);
    }
  }

  private async handleVerdictOnce(subject: string, data: Uint8Array): Promise<boolean> {
    const m = { subject, data };
    let ok: boolean;
    {
      {
        const prefix = `${this.ackPrefix()}.${this.config.principal}.`;
        const msgId = m.subject.startsWith(prefix) ? m.subject.slice(prefix.length) : m.subject;
        const verdict = JSON.parse(new TextDecoder().decode(m.data));
        if (msgId === 'revoked') {
          // §10dm: the ban — hang up now, stay hung up. Cooperative: this library obeys;
          // the account JWT's revocation is the enforcement.
          this.revoked = true;
          if (verdict.purge) {
            // §10kn: `--revoke --purge` — the data goes with the access.
            void this.purgeLocal();
            return true;
          }
          this.appendLog('SYS', `'${this.config.principal}' REVOKED by the operator (${verdict.reason ?? ''}) — hanging up now. The local rows stay; wipe() is the application's explicit act.`, 'ERROR');
          this.emitStatus('disconnected');
          void this.close();
          return true;
        }
        const pending = this.pendingWrites.get(msgId);
        const where = pending ? `${pending.table}#${pending.id}` : '(not from this session)';

        // `indeterminate`, or a status this client does not know: settled for
        // retransmission, outcome unknown (PROTOCOL §7.4c). Never reverted, never
        // re-sent under a new ID: the table reseeds from the hub, then `reconciled`.
        if (!['accepted', 'stale', 'rejected', 'row_deleted', 'failed'].includes(verdict.status)) {
          const reason = verdict.status === 'indeterminate' ? String(verdict.reason ?? '') : `UnknownStatus:${verdict.status}`;
          const write = await this.outboxWrite(msgId, pending);
          this.pendingWrites.delete(msgId);
          if (await this.settleIndeterminate(msgId, reason, write)) {
            this.appendLog(m.subject, `${where}: indeterminate (${reason}) — whether it committed is unknown; reconciling from the hub`, 'WARNING');
            if (write) this.emitVerdict({ ...write, outcome: 'indeterminate', reason });
            void this.reconcile();
          }
          return true;
        }

        // 'failed' is the ONE status that is not definitive (§7.1: keep and retry).
        const definitive = verdict.status !== 'failed';
        ok = definitive;
        // What onVerdict reports, read while the outbox row is still there.
        const write = definitive ? await this.outboxWrite(msgId, pending) : null;
        if (definitive) this.pendingWrites.delete(msgId);
        // §10do: a stale UPDATE is read BEFORE its outbox row goes — it may be rebased.
        // The verb lives on the outbox row (the verdict subject has none): read it now.
        let staleVerb = '';
        if (verdict.status === 'stale') {
          try { staleVerb = String((await this.run(`SELECT subject FROM _zebridge_outbox WHERE msg_id = ?`, msgId))[0]?.subject ?? '').split('.').pop() ?? ''; } catch { /* no row */ }
          await this.holdForRebase(msgId);
        }
        if (definitive && verdict.status !== 'rejected' && verdict.status !== 'row_deleted') {
          await this.outboxDrop(msgId);
        }

        switch (verdict.status) {
          case 'accepted':
            // Nothing applied here: state arrives over CDC, never from a verdict.
            if (verdict.reason === 'version_clamped') {
              this.appendLog(m.subject, `${where}: accepted, but the version was clamped to ${verdict.version} — this client's clock is ahead of the database`, 'WARNING');
            } else {
              this.appendLog(m.subject, { ...verdict, write: where }, 'VERDICT');
            }
            if (write) this.emitVerdict({ ...write, outcome: 'applied', ...(verdict.reason ? { reason: String(verdict.reason) } : {}) });
            break;
          case 'stale':
            // Pop and do NOT hand-revert: the winning row arrives via CDC. An UPDATE
            // is held for a rebase (§10do); anything else is dropped.
            if (this.rebase.has(msgId)) {
              this.appendLog(m.subject, `${where}: a newer version won — held for a rebase onto the winning row`, 'INFO');
              this.scheduleRebase();
            } else if (staleVerb === 'delete') {
              // §10dw: a delete that lost. Not rebased on purpose — a delete touches every
              // column, and re-issuing it would erase an edit its author saw accepted.
              this.appendLog(m.subject, `${where}: your DELETE lost to a newer edit of the same row — the row is back with that edit (surface this: delete again if it is still meant)`, 'WARNING');
              if (write) this.emitVerdict({ ...write, outcome: 'lost', lostColumns: write.columns });
            } else {
              this.appendLog(m.subject, `${where}: a newer version won — dropping this edit, the winning row arrives via CDC`, 'INFO');
              if (write) this.emitVerdict({ ...write, outcome: 'lost', lostColumns: write.columns });
            }
            break;
          case 'row_deleted':
            // Nothing is coming via CDC to correct this one — revert by hand (§1.6d).
            await this.revertOptimisticWrite(msgId, 'delete');
            this.appendLog(m.subject, `${where}: the row was deleted elsewhere, so this edit cannot be applied — reverting the local copy. Surface this to the user rather than dropping it silently.`, 'ERROR');
            if (write) this.emitVerdict({ ...write, outcome: 'deleted' });
            break;
          case 'rejected':
            await this.revertOptimisticWrite(msgId, 'restore');
            this.appendLog(m.subject, `${where}: refused permanently (${verdict.reason}${verdict.sqlstate ? ` / SQLSTATE ${verdict.sqlstate}` : ''}) — ${verdict.detail || 'no detail'} — reverting the local copy`, 'ERROR');
            if (write) this.emitVerdict({
              ...write, outcome: 'rejected',
              ...(verdict.reason ? { reason: String(verdict.reason) } : {}),
              ...(verdict.sqlstate ? { sqlstate: String(verdict.sqlstate) } : {}),
              ...(verdict.detail ? { detail: String(verdict.detail) } : {}),
            });
            break;
          case 'failed':
            if (verdict.reason === 'rate_limited') {
              // §10fk: kept, and the outbox holds for what the bridge asked.
              const wait = Number((verdict as { retry_after_ms?: number }).retry_after_ms ?? 1000);
              this.holdUntil = Math.max(this.holdUntil, Date.now() + wait);
              this.appendLog(m.subject, `${where}: rate limited by the bridge — kept, outbox held for ${wait} ms`, 'WARNING');
            } else {
              this.appendLog(m.subject, `${where}: failed after the bridge's delivery limit (${verdict.reason}) — kept for retry`, 'WARNING');
            }
            break;
          default:
            // Unreachable: every other status settles as indeterminate above.
            break;
        }
      }
    }
    return ok;
  }

  // ── reconciliation of `indeterminate` writes (PROTOCOL §7.4c) ─────────────

  /// In ONE local transaction the outbox entry goes and the marker comes, so a crash
  /// leaves either the queued write (re-sent, answered again) or the marker (reconciled
  /// at the next connect). False when the write is not this client's.
  private async settleIndeterminate(msgId: string, reason: string, write: Record<string, unknown> | null): Promise<boolean> {
    await this.outboxInitPromise;
    const row = (await this.run(`SELECT tbl FROM _zebridge_outbox WHERE msg_id = ?`, msgId))[0];
    if (!row) return false;
    await this.transaction(async (txExec) => {
      await txExec(`DELETE FROM _zebridge_outbox WHERE msg_id = ?`, msgId);
      await txExec(
        `INSERT INTO _zebridge_reconcile (msg_id, tbl, reason, version, data, since, state) VALUES (?, ?, ?, ?, ?, ?, 'pending')
         ON CONFLICT(msg_id) DO NOTHING`,
        msgId, String(row.tbl), reason, write?.version ?? '', write ? JSON.stringify(write) : null, Date.now(),
      );
    });
    return true;
  }

  /// Writes answered `indeterminate` whose reconciliation has not finished.
  public async reconciling(): Promise<number> {
    await this.outboxInitPromise;
    try { return Number((await this.run(`SELECT count(*) AS n FROM _zebridge_reconcile`))[0]?.n ?? 0); } catch { return 0; }
  }

  /// Rewind what the markers need: the table's watermark goes (the reseed must be a FULL,
  /// which replaces the optimistic row) and its route's position returns to 0, so the gap
  /// path seeds from the HUB's chain and resumes the tail at the chain's cut — every
  /// commit after the cut arrives too. Rewinding the route, rather than waiting for a
  /// chain newer than the replica's position, cannot starve under continuing load.
  private reconcileRewind(): Promise<void> {
    return this.changeStreams(() => this.reconcileRewindOnce());
  }

  private async reconcileRewindOnce() {
    await this.outboxInitPromise;
    const rows: any[] = await this.run(`SELECT msg_id, tbl, state, routes FROM _zebridge_reconcile ORDER BY since`);
    if (!this.reconcileLoaded) {
      // After a restart: a rewound route still at 0 waits for its heal; one past 0 healed
      // before the marker could be cleared.
      this.reconcileLoaded = true;
      for (const r of rows) {
        if (r.state !== 'rewound') continue;
        for (const route of String(r.routes ?? '').split(',')) {
          if (route && (this.globalSyncState.seq[route] ?? 0) === 0) this.hubOnly.add(route);
        }
      }
    }
    // Older clients may have persisted an empty route for an on-demand/unfollowed
    // table. It stays unresolved and can acquire a recovery route when followed.
    const pending = rows.filter((r) => r.state === 'pending' || !r.routes);
    if (!pending.length) return;
    const rewind = new Set<string>();
    const marks = pending.flatMap((r) => {
      const tenant = this.syncedTables.has(r.tbl) && !this.ondemandSet.has(r.tbl) ? this.effectiveTenantFor(r.tbl) : null;
      const route = tenant === null ? '' : this.cdcStreamForTenant(tenant);
      if (!route) return [];
      if (!this.hubOnly.has(route) || (this.globalSyncState.seq[route] ?? 0) > 0 || this.tailStops.has(route)) rewind.add(route);
      return [{ msgId: String(r.msg_id), tbl: String(r.tbl), route }];
    });
    if (!marks.length) return;
    // Fence every old callback and await work already in storage before writing zero.
    for (const route of rewind) await this.stopTail(route);
    for (const m of marks) this.healedRoutes.delete(m.route);
    await this.transaction(async (txExec) => {
      for (const route of rewind) {
        await txExec(`INSERT INTO _zebridge_stream_seq (stream, last_seq) VALUES (?, 0) ON CONFLICT(stream) DO UPDATE SET last_seq = 0, born_empty = 0`, route);
        await txExec(`DELETE FROM _zebridge_frontier WHERE stream = ?`, route); // replayed from 0: nothing taken on this numbering counts
      }
      for (const m of marks) {
        await txExec(`DELETE FROM _zebridge_generations WHERE tbl = ?`, m.tbl);
        await txExec(`UPDATE _zebridge_reconcile SET state = 'rewound', routes = ? WHERE msg_id = ?`, m.route, m.msgId);
      }
    });
    for (const route of rewind) {
      this.globalSyncState.seq[route] = 0;
      this.streamBornEmpty.delete(route); // a reset position is no longer "born empty" (core.streamHasGap)
      this.forgetFrontiers(route);
      this.hubOnly.add(route);
      this.appendLog('SYS', `${route}: rewound for reconciliation — reseeding from the hub`, 'WARNING');
    }
  }

  /// A rewound marker whose route healed waits for the tail to reach the stream's end as
  /// it is now — a commit after the chain's cut arrives by the tail, not the seed; a
  /// healed one clears once it did. True while a recoverable marker needs another pass;
  /// parked markers remain durable but wake only on connect/configuration changes.
  private async reconcileClear(): Promise<boolean> {
    for (const route of this.healedRoutes) this.hubOnly.delete(route);
    this.healedRoutes.clear();
    const live = new Set(this.cdcStreams());
    for (const route of [...this.hubOnly]) if (!live.has(route)) this.hubOnly.delete(route);
    const rows: any[] = await this.run(`SELECT msg_id, tbl, state, routes, targets, data FROM _zebridge_reconcile ORDER BY since`);
    let left = 0;
    for (const r of rows) {
      const routes = String(r.routes ?? '').split(',').filter(Boolean);
      if (!this.syncedTables.has(r.tbl) || this.ondemandSet.has(r.tbl)) {
        // A configuration change can remove the recovery path even after a rewind.
        // Require a fresh rewind if this table is followed again later.
        if (r.state !== 'pending' || routes.length) {
          await this.run(`UPDATE _zebridge_reconcile SET state = 'pending', routes = '', targets = '' WHERE msg_id = ?`, r.msg_id);
        }
        continue;
      }
      if (!routes.length) continue; // no recovery route: no polling timer
      if (r.state === 'rewound' || (r.state === 'healed' && !r.targets)) {
        left++;
        if (routes.some((route) => this.hubOnly.has(route)) || !this.nc) continue;
        const targets: string[] = [];
        try {
          const jsm = await this.transport.jetstreamManager(this.nc, this.jsOpts());
          for (const route of routes) {
            const last = (await jsm.streams.info(route))?.state?.last_seq;
            if (!Number.isSafeInteger(last) || last < 0) throw new Error(`${route}: missing reconciliation tail target`);
            targets.push(`${route}:${last}`);
          }
        } catch (e) {
          this.appendLog('SYS', `reconcile ${r.msg_id}: tail targets unavailable (${e}) — marker retained`, 'WARNING');
          continue; // keep retrying this marker, while independent markers progress
        }
        await this.run(`UPDATE _zebridge_reconcile SET state = 'healed', targets = ? WHERE msg_id = ?`, targets.join(','), r.msg_id);
        continue;
      }
      if (r.state !== 'healed') { left++; continue; }
      const behind = String(r.targets ?? '').split(',').filter(Boolean).some((pair) => {
        const at = pair.lastIndexOf(':');
        return (this.globalSyncState.seq[pair.slice(0, at)] ?? 0) < Number(pair.slice(at + 1));
      });
      if (behind) { left++; continue; }
      await this.run(`DELETE FROM _zebridge_reconcile WHERE msg_id = ?`, r.msg_id);
      this.appendLog('SYS', `${r.msg_id} reconciled — the local copy is the authoritative state`, 'INFO');
      let write: any = null;
      try { write = r.data ? JSON.parse(r.data) : null; } catch { /* unreadable */ }
      if (write) this.emitVerdict({ ...write, outcome: 'reconciled' });
    }
    return left > 0;
  }

  /// Rewind, heal through `subscribeStreams` (the gap path), clear. Retried every few
  /// seconds while recoverable markers remain, and at every connect. Parked markers
  /// stay visible through reconciling() without keeping a polling timer alive.
  private async reconcile(): Promise<void> {
    if (this.reconcileBusy || !this.nc) return;
    this.reconcileBusy = true;
    let again = false;
    try {
      await this.reconcileRewind();
      if (this.hubOnly.size) {
        if (this.resyncing) {
          again = true;
          return;
        }
        this.resyncing = true;
        try { await this.subscribeStreams(); } finally { this.resyncing = false; }
      }
      again = await this.reconcileClear();
    } catch (e) {
      this.appendLog('SYS', `reconcile: ${e} — retried shortly`, 'WARNING');
      again = true;
    } finally {
      this.reconcileBusy = false;
      if (again) setTimeout(() => void this.reconcile(), 2_000);
    }
  }

  /// A full seed replaced the table's rows with the chain's, which knows nothing of this
  /// client's writes still in the outbox: their optimistic copies are gone. Re-apply them,
  /// oldest first, each over a fresh before-image (the seeded row, or the previous pending
  /// write's). A write queued before the `optimistic` column cannot be (logged).
  private async reapplyPending(table: string) {
    await this.outboxInitPromise;
    const state = this.syncedTables.get(table);
    const rows: any[] = await this.run(`SELECT msg_id, payload, optimistic FROM _zebridge_outbox WHERE tbl = ? ORDER BY created_at`, table);
    let applied = 0, skipped = 0;
    for (const r of rows) {
      if (!r.optimistic || !state?.pkCols.length) { skipped++; continue; }
      let sent: any, ev: any;
      try { sent = JSON.parse(r.payload); ev = JSON.parse(r.optimistic); } catch { skipped++; continue; }
      const keyObj = sent?.key as Record<string, unknown> | undefined;
      if (!keyObj) { skipped++; continue; }
      await this.transaction(async (txExec) => {
        const where = state.pkCols.map((c) => `"${c}" = ?`).join(' AND ');
        const before = (await txExec(`SELECT * FROM ${table} WHERE ${where}`, ...state.pkCols.map((c) => keyObj[c])))[0] ?? null;
        await txExec(`UPDATE _zebridge_outbox SET before = ? WHERE msg_id = ?`, before == null ? null : JSON.stringify(before), r.msg_id);
        await this.applyEvent(table, ev, txExec);
      });
      applied++;
    }
    if (applied) this.appendLog('SYS', `${table}: ${applied} pending write(s) re-applied over the seed`, 'INFO');
    if (skipped) this.appendLog('SYS', `${table}: ${skipped} pending write(s) queued before this version cannot be re-applied over the seed — their verdicts or echoes restore them`, 'WARNING');
  }

  private sweepPendingWrites() {
    const now = Date.now();
    // A reconnect can precede admission's own reconnect, or arrive while stream
    // recovery is already running. Retrying is independent of that recovery and
    // bounded even when the first publish after reconnect has no responder.
    const retryNow = performance.now();
    if (this.nc && this.naturallyConnected && retryNow >= this.nextOutboxRetry) {
      void this.recoverOutbox().catch((e) => this.appendLog('OUTBOX', `recovery failed, pending writes retained: ${e}`, 'WARNING'));
    }
    for (const [msgId, w] of this.pendingWrites) {
      if (now - w.at < WRITE_TIMEOUT_MS) continue;
      this.pendingWrites.delete(msgId);
      // No echo and no verdict: report as unconfirmed, not denied — the two are
      // indistinguishable from here and only one is the client's fault.
      this.appendLog(msgId, `no echo and no verdict within ${WRITE_TIMEOUT_MS}ms — unconfirmed (${w.table})`, 'ERROR');
    }
  }

  /// The low-level write: publish one mutation and record it as pending. Public as an
  /// escape hatch for demos that deliberately send broken payloads (no key, wrong
  /// grants) — mutate() is the blessed path and builds the payload correctly.
  public async rawMutation(table: string, op: string, id: string | number, version: string, payload: Record<string, unknown>) {
    // No socket is not a refusal: the write applies locally and waits in the outbox,
    // and connect() flushes it (§10dp). Only a table without a shape cannot be written.
    if (!this.syncedTables.has(table)) return;

    // A suspended table has no CDC path: an optimistic write here would never get a
    // confirming or correcting echo — permanently. Refused client-side.
    const suspendReason = this.suspendedMap.get(table);
    if (suspendReason) {
      this.appendLog(table, `write refused: table is suspended upstream (${suspendReason}) — no CDC echo would ever confirm it`, 'ERROR');
      return;
    }

    // Subject and idempotency id come from core (§10s 2c): the version stays
    // IN the id — a second edit to the same row is a different write; a retry
    // of the same edit is not.
    const subject = mutationSubject(this.config.principal!, table, op, this.config.grammar.subjects?.mutations_prefix);
    // createdMs: the bridge's clock as this device estimates it, taken once here; the
    // outbox keeps the resulting ID, so a retry never regenerates it.
    // The key parts in declared primary-key order, each kept apart in the ID.
    const pkCols = this.syncedTables.get(table)?.pkCols ?? [];
    const keyObj = (payload as { key?: Record<string, unknown> }).key;
    const keyParts = pkCols.length && keyObj ? pkCols.map((c) => idKeyPart(keyObj[c])) : [String(id)];
    const msgId = mutationMsgId(Math.floor(Date.now() + this.clockOffsetMs()), this.clientIdValue, table, op, keyParts, version);

    // Outbox insert and optimistic apply in ONE transaction (§7.1), persisted BEFORE
    // the publish: a duplicate is collapsed by dedup, a loss is unrecoverable.
    let applied: Record<string, unknown> | null = null;
    try {
      await this.transaction(async (txExec) => {
        const state = this.syncedTables.get(table);
        let before: any = null;
        if (state?.pkCols.length) {
          const keyObj = (payload as any).key as Record<string, unknown> | undefined;
          if (keyObj) {
            const where = state.pkCols.map((c) => `"${c}" = ?`).join(' AND ');
            const pkVals = state.pkCols.map((c) => keyObj[c]);
            const existing = await txExec(`SELECT * FROM ${table} WHERE ${where}`, ...pkVals);
            before = existing[0] ?? null;
          }
        }
        // §10dx: the optimistic row carries the write's own stamp in the version column
        // — the ingress sets it from `version` server-side, and locally a NOT NULL
        // version column without it refused every optimistic INSERT.
        applied = optimisticEvent(table, op, payload);
        if (op !== 'DELETE' && state?.versionColumn && applied.data && typeof applied.data === 'object' && !(state.versionColumn in (applied.data as any))) {
          (applied.data as any)[state.versionColumn] = version;
        }
        await this.outboxPut({ msgId, subject, payload, table, id, before, optimistic: applied }, txExec);
        await this.applyEvent(table, applied, txExec);
      });
      // Fire the change event AFTER the local replica is successfully updated — with
      // the event that WAS applied, stamp included, so a handler reading
      // `ev.data.<version column>` sees the write's own version, not undefined.
      if (applied) this.triggerChange(table, applied);
    } catch (err) {
      this.pendingWrites.delete(msgId);
      this.appendLog(subject, `optimistic apply failed, write not sent: ${err}`, 'ERROR');
      return;
    }

    // ⚠️ The GC watermark gates the FIRST send too, not only replays.
    //
    // Publishing straight from here left a hole: the gate lives in `flushOutbox`, so a
    // write's first send skipped it entirely and only a later replay was ever checked.
    // Found in the Zig port's first live test — the gate printed its refusal while the
    // row was already in PostgreSQL — and the same shape was here.
    //
    // Normally invisible, because a fresh write is stamped `now` and the watermark is
    // in the past. It bites exactly where it matters: a lagging clock, or a client
    // whose queued write is being sent for the first time long after it was made.
    const wm = await this.gcWatermark();
    if (outboxWatermarkGate([{ msgId, version: (payload as any)?.version ?? null }], wm).refuse.length) {
      // Never published, so it never committed: the revert is exact.
      const write = await this.outboxWrite(msgId, { table, version });
      this.pendingWrites.delete(msgId);
      await this.revertOptimisticWrite(msgId, 'restore');
      await this.outboxDrop(msgId);
      if (write) this.emitVerdict({ ...write, outcome: 'rejected', reason: 'PredatesGcWatermark' });
      this.appendLog(
        subject,
        `${table}[${id}] predates the GC watermark (${wm}) and CANNOT be sent: its tombstone ` +
          `has been reaped, so sending it would resurrect a deleted row (PROTOCOL §MUST 6). ` +
          `The local copy has been reverted — this edit is lost.`,
        'ERROR',
      );
      return;
    }

    // Closed on purpose (close(), a battery saver, an offline toggle): the row is
    // queued and optimistic, and goes out with the outbox flush on the next connect().
    if (!this.nc) {
      this.appendLog(subject, `${table}[${id}] queued — no connection; sent on the next connect()`, 'OUTBOX');
      return;
    }

    // A fresh edit shares the durable window with recovery. It remains queued
    // when every slot is occupied; the next verdict advances it automatically.
    await this.flushOutbox();
  }

  /// Fill the in-flight window. Remaining entries advance automatically as verdicts
  /// arrive; the Promise never promises that every pending write was judged.
  public async flushOutbox() {
    return this.flushOutboxPass();
  }

  private wakeOutbox() {
    if (!this.nc) return;
    this.outboxWake = true;
    void this.recoverOutbox().catch((e) => this.appendLog('OUTBOX', `recovery failed, pending writes retained: ${e}`, 'WARNING'));
  }

  private async recoverOutbox() {
    // This deadline is reconnect jitter only, never steady-state drain pacing.
    if (performance.now() < this.nextOutboxRetry) return;
    return this.flushOutboxPass();
  }

  private async flushOutboxPass() {
    if (this.outboxWork) { this.outboxWake = true; return this.outboxWork; }
    this.outboxWake = false;
    const work = this.flushOutboxOnce();
    this.outboxWork = work;
    try { await work; }
    finally {
      if (this.outboxWork === work) this.outboxWork = null;
      // Cover a verdict delivered between the loop's final check and cleanup.
      if (this.outboxWake && this.nc) queueMicrotask(() => this.wakeOutbox());
    }
  }

  private async flushOutboxOnce() {
    const nc = this.nc;
    if (!nc || Date.now() < this.holdUntil) return;
    let rows: any[] = [];
    try {
      await this.outboxInitPromise;
      // Serialize reservations with CDC/optimistic transactions. A transient
      // apply rollback must never undo a reservation whose send escaped to NATS.
      const epoch = this.outboxReconnect;
      await this.transaction(async (exec) => {
        if (epoch !== this.outboxRetryEpoch) await exec(`UPDATE _zebridge_outbox SET retry_at = 1 WHERE retry_at > 0`);
        const active = Number((await exec(`SELECT count(*) AS n FROM _zebridge_outbox WHERE retry_at > 0`))[0].n);
        const slots = Math.max(0, this.outboxWindow - active);
        if (slots) await exec(`UPDATE _zebridge_outbox SET retry_at = 1 WHERE msg_id IN
          (SELECT msg_id FROM _zebridge_outbox WHERE retry_at = 0 ORDER BY attempts, created_at, msg_id LIMIT ?)`, slots);
        const latest = Date.now() + 61_000;
        await exec(`UPDATE _zebridge_outbox SET retry_at = ? WHERE retry_at > ?`, latest, latest);
        rows = await exec(`SELECT * FROM _zebridge_outbox WHERE retry_at > 0 AND retry_at <= ?
          ORDER BY retry_at, attempts, created_at, msg_id LIMIT ?`, Date.now(), this.outboxWindow);
      });
      this.outboxRetryEpoch = epoch;
    } catch (err) {
      this.appendLog('OUTBOX', `could not be read: ${err}`, 'ERROR');
      return;
    }
    if (!rows.length || this.nc !== nc) return;

    // ── the GC watermark gate (PROTOCOL.md §MUST 6) ─────────────────────────
    //
    // Runs BEFORE the first publish, not per-entry inside the loop: one read of the
    // watermark for the whole flush, and an entry that must not be sent is never
    // sent even if a later one fails.
    const watermark = await this.gcWatermark();
    // Only a write never published is judged here. A published one may have committed:
    // the bridge answers it from its receipt, or refuses it itself (PredatesGcWatermark)
    // when no attempt did.
    const gate = outboxWatermarkGate(
      rows.map((r: any) => ({ msgId: r.msg_id, version: Number(r.published ?? 1) === 0 ? outboxVersionOf(r) : null })),
      watermark,
    );
    if (gate.refuse.length) {
      const refused = new Set(gate.refuse);
      for (const r of rows.filter((x: any) => refused.has(x.msg_id))) {
        // Same handling as a `rejected` verdict, and for the same reason: this write
        // will never be sent, so the optimistic copy is a divergence. Restore the
        // before-image and say so — the user's edit is being dropped, and PROTOCOL
        // §SHOULD 2's rule (surface it, never silently discard) is exactly this case.
        const write = await this.outboxWrite(r.msg_id, undefined);
        await this.revertOptimisticWrite(r.msg_id, 'restore');
        await this.outboxDrop(r.msg_id);
        if (write) this.emitVerdict({ ...write, outcome: 'rejected', reason: 'PredatesGcWatermark' });
        this.appendLog(
          'OUTBOX',
          `${r.tbl}[${r.row_id}] ${r.msg_id} was queued before the GC watermark ` +
            `(${watermark}) and CANNOT be sent: the tombstone that would have overruled ` +
            `it has been reaped, so sending it would resurrect a deleted row ` +
            `(PROTOCOL §MUST 6). The local copy has been reverted — this edit is lost.`,
          'ERROR',
        );
      }
      rows = rows.filter((x: any) => !refused.has(x.msg_id));
      if (!rows.length) return;
    }

    let verdictManager: Promise<any> | undefined;
    for (const r of rows) {
      if (this.nc !== nc || Date.now() < this.holdUntil) return;
      try {
        const delay = Math.min(60_000, 5000 * 2 ** Math.min(4, Math.max(0, Number(r.attempts)))) + Math.floor(Math.random() * 1000);
        let present = false;
        await this.transaction(async (exec) => {
          present = (await exec(`UPDATE _zebridge_outbox SET retry_at = ?, attempts = CASE WHEN attempts < 2147483647 THEN attempts + 1 ELSE attempts END
            WHERE msg_id = ? RETURNING msg_id`, Date.now() + delay, r.msg_id)).length > 0;
        });
        if (!present) continue; // a concurrent live verdict/echo already settled it
        if (Number(r.published ?? 1) !== 0) {
          verdictManager ??= this.transport.jetstreamManager(nc, this.jsOpts());
          if ((await this.collectMissedVerdicts([r], verdictManager)).has(r.msg_id)) continue;
        }
        if (this.nc !== nc || Date.now() < this.holdUntil) return;
        const h = this.transport.headers();
        h.set('Nats-Msg-Id', r.msg_id);
        this.pendingWrites.set(r.msg_id, { table: r.tbl, id: r.row_id, at: Date.now(), version: outboxVersionOf(r) });
        // Persist before publishing, independently of any CDC transaction.
        await this.transaction(async (exec) => { await exec(`UPDATE _zebridge_outbox SET published = 1 WHERE msg_id = ?`, r.msg_id); });
        const ack = await this.transport.jetstream(nc, this.jsOpts()).publish(r.subject, encode(JSON.parse(r.payload)), { headers: h });
        this.appendLog('OUTBOX', `replayed ${r.msg_id} (seq ${ack.seq}${ack.duplicate ? ', duplicate — already landed' : ''})`, 'INFO');
      } catch (err) {
        this.appendLog('OUTBOX', `replay of ${r.msg_id} failed, kept with its retry deadline: ${err}`, 'ERROR');
        break;
      }
    }
  }

  /// A real PING against a possibly-lying transport: a frozen server can leave the
  /// WebSocket believing it is open with no 'disconnect' ever fired. Acts only on
  /// transitions — the recovery transition is the one nc.status() might never report.
  /// PROTOCOL §11: the fleet heartbeat — this client's applied position per CDC stream,
  /// to `$KV.live.<tenant>.<principal>`, behind `$JS.<domain>.API.` with a JetStream
  /// domain (last value per key, TTL on the bucket, so a
  /// client that stops beating drops off the bridge's fleet metrics by itself). The
  /// payload is core.heartbeatPayload, fixture-pinned with libzb. A report, never a
  /// request: a core publish, no PubAck awaited (as libzb); a lost beat is replaced by
  /// the next, and the bucket's TTL says stale.
  private async sendHeartbeat() {
    if (!this.nc) return;
    const tenant = this.tenantValue || this.config.grammar?.open_tenant || '_default';
    const bucket = this.config.grammar?.kv?.live ?? 'live';
    // With a JetStream domain, through the domain's API (as libzb): the hub maps
    // `$JS.<domain>.API.$KV.>` onto its buckets and never announces a bare `$KV.>` to a leaf.
    const pre = this.config.jsDomain ? `$JS.${this.config.jsDomain}.API.` : '';
    const subject = `${pre}$KV.${bucket}.${tenant}.${this.config.principal}`;
    try {
      const payload = heartbeatPayload(this.config.principal!, tenant, Date.now(), this.globalSyncState.seq, this.lastPending);
      this.nc.publish(subject, new TextEncoder().encode(payload));
    } catch (err) {
      this.appendLog('SYS', `heartbeat not sent (${subject}): ${err}`, 'WARNING');
    }
  }

  private async pollNatsRtt() {
    if (!this.nc) return;
    try {
      await Promise.race([
        this.nc.rtt(),
        new Promise<never>((_, reject) => setTimeout(() => reject(new Error('rtt timeout')), 3000)),
      ]);
      if (!this.naturallyConnected) {
        this.naturallyConnected = true;
        this.emitStatus('connected');
        this.appendLog('SYS', 'NATS rtt check recovered — re-syncing (nc.status() never reported this)', 'INFO');
        if (!this.resyncing) {
          this.resyncing = true;
          void this.recoverOutbox();
          void this.subscribeStreams().catch(() => {}).finally(() => { this.resyncing = false; });
        }
      }
    } catch (err) {
      if (this.naturallyConnected) {
        this.naturallyConnected = false;
        this.emitStatus('disconnected');
        this.appendLog('SYS', `NATS rtt check failed — connection is not actually answering: ${err}`, 'WARNING');
      }
    }
  }
}


/** §10fn: `$KV.tenants.<principal>` — a JSON array of tenants. Anything else is malformed. */
export function parseTenantList(value: string): string[] {
  const v = (value ?? '').trim();
  if (!v) return [];
  const arr = JSON.parse(v);
  if (!Array.isArray(arr)) throw new Error(`tenant list malformed: ${v.slice(0, 80)}`);
  return arr.filter((t): t is string => typeof t === 'string' && t.length > 0);
}
