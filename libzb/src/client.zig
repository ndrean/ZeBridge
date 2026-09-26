//! The orchestration loop — the third piece after core and the two shells.
//!
//! Wires core.zig (decisions) to storage.zig (SQLite) and transport.zig
//! (nats.zig) into a working read-path client: schemas from KV, seeding from
//! generation chains (with the §10n destruction guard), the per-stream gap
//! rule, the finding-7/10 seed gate, FK hold/retry, and durable positions.
//! This is INCREMENT 1: read path + catch-up-to-tail semantics. The write
//! path (outbox, verdicts, HLC stamping via core) and live tailing are next.
//!
//! Composition, not invention: every rule here is a call into core.zig, the
//! same functions the 91 fixtures pin.

const std = @import("std");
const core = @import("core.zig");
pub const storage = @import("storage.zig");
const transport = @import("transport.zig");
const msgpack = @import("msgpack");
const C = @import("c");

const Value = std.json.Value;

/// §10dq: the wire grammar, compiled in — the same `src/grammar.json` the bridge
/// embeds (§10ci). A rename is a protocol fork whose cost is a rebuild, on both
/// sides; nothing reads a file, nothing fetches, and a client opens with the bridge
/// down (NATS holds everything a provisioned client needs).
pub const grammar_json: []const u8 = @embedFile("grammar");

/// sha256 of the embedded grammar, lowercase hex — what the bridge's `X-Grammar-Hash`
/// header and the /enroll payload's `grammar_hash` carry for the same bytes.
pub fn grammarHashHex(buf: *[64]u8) []const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(grammar_json, &digest, .{});
    return std.fmt.bufPrint(buf, "{x}", .{&digest}) catch unreachable;
}

pub const Options = struct {
    url: []const u8,
    creds_path: []const u8,
    /// The credentials as text — the option both clients call `creds`. Empty: none.
    creds: []const u8 = "",
    /// The grammar hash the host RECEIVED — from the /enroll payload beside the JWT, or
    /// GET /grammar's header. When set, a mismatch refuses to open: this library is
    /// built for another protocol than the bridge it is pointed at. Unset skips the
    /// check (a bridge that could not be reached is not a mismatch).
    grammar_hash: ?[]const u8 = null,
    /// The JetStream domain the deployment's grants name — from the /enroll payload's
    /// `js_domain` beside the JWT. Null speaks to the server's own JetStream; a name
    /// addresses `$JS.<name>.API.`, which is how a client on a leaf node reaches the
    /// hub's. The wrong value is not a mismatch the library can detect: every API call
    /// simply gets no responder, so it fails at the first request, not at connect.
    js_domain: ?[]const u8 = null,
    db_path: [*:0]const u8,
    /// §10fd: a PostgreSQL replica instead of the SQLite file — a libpq URL. The
    /// replica is then a database any PostgreSQL tool reads (the micro-VM case).
    db_url: ?[*:0]const u8 = null,
    /// §10fl: the storage engine. `sqlite` (the default) and `duckdb` take
    /// `db_path`; `postgres` is implied by `db_url`. DuckDB is the analytical
    /// replica of the micro-VM worker, built in with `-Dduckdb=true`.
    engine: storage.Engine = .sqlite,
    principal: []const u8,
    /// Parents FIRST is still the recommended order — but since §10cp the seed
    /// runs with foreign_keys OFF (chains are per-table snapshots cut at different
    /// moments, so no order can fully protect a child chain), re-enables them
    /// after, and reports surviving violations loudly.
    tables: []const []const u8,
    /// §10hn: follow EVERY published table — the schemas bucket's keys, re-read on each
    /// sync, new keys joining live. `tables` is then ignored except as the on-demand
    /// list's complement. Convenient and dangerous in equal measure: a DBA enabling a
    /// table changes what this replica downloads, with no deploy on this side.
    follow_all: bool = false,
    /// §10hj: tables held ON DEMAND — the schema arrives like any table's and the local
    /// table is created from it, but nothing seeds it and no stream is tailed for it.
    /// Rows come only as answers to the host's own `request`s, applied through `ingest`
    /// (the chain's version-guarded upsert), and a `mutate` on it goes the normal way.
    /// The phone's "what is around me" table: it holds what it asked for.
    ondemand: []const []const u8 = &.{},
    /// This replica's identity, and it must be STABLE across restarts: it is the
    /// tiebreak value the bridge stores and the prefix of every msg_id, so a client
    /// that changes it loses idempotency on anything still unconfirmed.
    client_id: []const u8 = "zig-client",
    /// Fleet heartbeat cadence (NOTES §10dc); 0 disables. The bridge's bucket TTL
    /// defaults to three of these.
    heartbeat_ms: u64 = 30_000,
    /// §10fa: rows per transaction when a chain step is applied; 0 = one transaction
    /// for the whole step. Bounds what a seed holds in memory at once (a chunk's
    /// bindings, not the document) and how long the replica's lock is held.
    seed_chunk_rows: usize = 50_000,
    /// §10fh: the streaming seed, for a host that cannot hold a chain object
    /// inflated (a phone). Off, a step is fetched and inflated whole, its rows
    /// indexed and sorted by key, then applied in chunks: the fastest apply, at the
    /// cost of the inflated document in memory (1.16 GB for 3 M rows). On, the
    /// object is read from the store chunk by chunk, inflated through a window, and
    /// rows are cut from the window into chunks of `seed_chunk_rows`, each sorted by
    /// key on its own and applied: what a step holds is one object-store chunk, the
    /// window, one chunk of rows and its index. The sort is per chunk, so the b-tree
    /// sees `seed_chunk_rows` runs of ordered keys instead of one — slower, bounded.
    seed_streaming: bool = false,
    /// §10fh: with `seed_streaming` on, a step whose COMPRESSED object is smaller than
    /// this takes the whole-object path anyway — its inflated size is small, and the
    /// whole-object apply is twice as fast. The pull reader knows the size from the
    /// object's meta before reading a chunk. 8 MiB compressed is roughly 90 MB
    /// inflated (test_types: 98 MiB → 1.1 GB); a delta is a few hundred bytes.
    seed_streaming_above: usize = 8 * 1024 * 1024,
};

const SeedAnchor = struct { stream: []const u8, seq: u64, lsn: i64 };

/// §10jc: what this client applies from one stream stays IN STREAM ORDER. A pull
/// consumer can lose a delivery in transit (a reset link, a phone killed mid-fetch) and
/// hand it over again only after `ack_wait`, out of order; the position (highest seq
/// applied) had moved past it, the consumer was gone by then (a drain's end, a kill), the
/// new one started at position + 1, and the message was never read — measured on an
/// iPhone at 40k events/s: 25,914 rows missing; and a redelivered older row image applied
/// after a newer one lost 76,132 updates in another run.
///
/// Every delivery's CONSUMER sequence (+1 per delivery, by the server) is checked: a jump
/// is a lost delivery. What precedes it is applied; nothing after it is — not applied, not
/// acked — and the consumer is recreated from the position, so the server re-sends from
/// the lost message on, in order. Applying past a gap and filling it later is NOT
/// equivalent: the lost message is older than what followed it (an INSERT lost, its
/// UPDATE a second later applied, the INSERT re-sent on top: the row went back to the
/// insert's image — measured in zb-client-ts, 5 batches). Anything at or below the
/// position is a redelivery: acked, never re-applied.
const Flow = struct {
    /// The consumer the sequence belongs to; another name resets `cseq`.
    consumer: []const u8 = "",
    /// The highest consumer sequence seen from `consumer` (0: none yet).
    cseq: u64 = 0,
    /// A delivery was lost: this consumer is recreated from the position.
    gap: bool = false,
};

const BulkStats = struct {
    bulked: usize = 0,
    statements: usize = 0,
    singles: usize = 0,
    fn add(self: *BulkStats, b: usize, s: usize, one: usize) void {
        self.bulked += b;
        self.statements += s;
        self.singles += one;
        if ((self.statements % 50) == 0 and s > 0) std.debug.print("bulk cdc: {d} events in {d} statements, {d} per event\n", .{ self.bulked, self.statements, self.singles });
    }
};

const TableState = struct {
    pk: []const []const u8,
    cols: []const []const u8,
    version_col: ?[]const u8,
    tenant_col: ?[]const u8,
    /// PROTOCOL §7.5: a soft delete arrives as an update setting this column, and the
    /// physical reap is never forwarded — so a row with it set is REMOVED here, on
    /// seed, on CDC and on the local optimistic apply alike. See `tombstoned`.
    tombstone_col: ?[]const u8,
    /// §10fd: the columns whose PostgreSQL type is an array (the descriptor's `pg`
    /// block). The wire carries them as JSON text (§10ey); a PostgreSQL replica binds
    /// the array literal instead, on every apply path.
    array_cols: []const []const u8 = &.{},
    /// §10fe: the PostGIS columns (`pg` block, type geometry/geography): bytes into
    /// them are bare EWKB hex in COPY text, where a bytea takes `\\x` hex.
    geom_cols: []const []const u8 = &.{},
    /// §10fg: the pgvector and bit(n) columns (`pg` block): a PostgreSQL replica
    /// binds pgvector's text form of the wire BLOB (core.vecLiteral); SQLite keeps
    /// the BLOB.
    vec_cols: []const VecCol = &.{},
    /// §10fn: the CDC streams this table's events ride — one `CDC_<tenant>` per
    /// tenant the client belongs to for a tenant-scoped table, `CDC_PUBLIC` alone
    /// for a public one. Empty for a tenant-scoped table while the client has no
    /// tenant (its shared rows still ride `shared_route`).
    routes: []const []const u8,
    /// For a tenant-scoped table: the stream its OPEN-TENANT rows ride (CDC_PUBLIC).
    /// Those are the shared rows every tenant may read — `zb_reader_all` admits
    /// `tenant_col = <open tenant>`, and the chain carries them because the producer
    /// reads under that policy — so the table depends on TWO streams, and a gap on
    /// either leaves half of it stale (NOTES §10bq). Null for a public table, whose
    /// only route already IS that stream.
    shared_route: ?[]const u8 = null,
    /// The seed gate's anchors (findings 7 + 10), one per stream a chain of this
    /// table was cut on: events at or below `seq` on that stream are inside the chain
    /// already applied. §10fn: a tenant-scoped table has one chain per tenant, each
    /// on its own stream, so the gate is per stream.
    anchors: std.ArrayListUnmanaged(SeedAnchor) = .empty,
    /// The catalogue's seed_epoch the descriptor carried (§10df).
    seed_epoch: i64 = 0,
};

var trace_enabled: bool = false;
/// §10jc test hook: every Nth delivery is discarded on arrival — not applied, not acked,
/// its consumer sequence never seen — exactly what a delivery lost in transit looks like
/// from here. The server re-sends it after ack_wait, out of order. 0: off.
var test_drop_every: u64 = 0;
var test_drop_count: u64 = 0;
fn tr(comptime fmt: []const u8, args: anytype) void {
    if (trace_enabled) std.debug.print("trace: " ++ fmt ++ "\n", args);
}

pub const SyncClient = struct {
    a: std.mem.Allocator,
    arena: std.heap.ArenaAllocator, // client-lifetime allocations (states, grammar)
    t: *transport.Transport,
    st: storage.Storage,
    /// The app's connection: the same file, opened read-only (NOTES §10). `query` runs
    /// here and nowhere else, so the replica cannot be written around `mutate`.
    ro: storage.Storage,
    opts: Options,
    /// `syncOnce` runs the one-time steps once; the per-pass steps every call.
    schemas_synced: bool = false,

    // ── grammar.json, and ONLY grammar.json (PROTOCOL §1) ─────────────────────
    // No defaults: every name below is REQUIRED by `loadGrammar`, which fails naming
    // the missing key. A hardcoded fallback would let a renamed stream or bucket
    // leave this client publishing into, or watching, a subject nobody else uses —
    // with no error on either side. `undefined` until `loadGrammar` has run; `init`
    // does not return without it.
    cdc_prefix: []const u8 = undefined, // cdc_streams.tenant_prefix — the CDC_<tenant> STREAM prefix
    /// subjects.cdc_prefix — the SUBJECT token (`cdc`), for the consumer's filters.
    subject_cdc_prefix: []const u8 = undefined,
    /// subjects.query_prefix — `query`, the family a responder answers on (§10hp).
    subject_query_prefix: []const u8 = "query",
    /// §10hq: answers too large for one message. The bucket is `<prefix><tenant>`, an
    /// answer above `inline_max` becomes an object there, and it lives `max_age`.
    results_bucket_prefix: []const u8 = "res-",
    results_inline_max: usize = 262_144,
    results_max_age_ns: u64 = 600 * std.time.ns_per_s,
    cdc_public: []const u8 = undefined, // cdc_streams.public
    kv_schemas: []const u8 = undefined, // kv.schemas
    kv_tenants: []const u8 = undefined, // kv.tenants
    kv_live: []const u8 = "live", // kv.live — optional in the grammar (a bridge older than §10dc has no key)
    last_heartbeat_ms: i64 = 0,
    kv_generations: []const u8 = undefined, // generations.kv
    gen_bucket_prefix: []const u8 = undefined, // generations.bucket_prefix
    open_tenant: []const u8 = undefined, // open_tenant
    /// §10dl: no `$KV.tenants.<principal>` key — the principal was never mapped, or was
    /// revoked. Tenant-scoped tables are then unreadable (no stream to grant) and are
    /// SKIPPED, audibly, the TS client's rule; public tables still follow.
    tenant_missing: bool = false,
    /// `open_tenant` as a one-element list (what `tenantsFor` hands out for a public table).
    open_tenants: []const []const u8 = &.{},
    /// §10dm: the ban was seen (`mutation_ack.<principal>.revoked`): the connection is
    /// closed and every later call answers `error.Revoked`. The rows stay; the wipe is
    /// the application's explicit act (`zb_client_wipe`).
    revoked: bool = false,
    hung_up: bool = false,
    stream_mutations: []const u8 = undefined, // streams.mutations
    subject_mutations_prefix: []const u8 = undefined, // subjects.mutations_prefix
    subject_mutation_ack_prefix: []const u8 = undefined, // subjects.mutation_ack_prefix

    /// The FIRST tenant (sorted), or the open tenant when the principal has none:
    /// the sync report's `tenant`, and the shape every single-tenant host knows.
    tenant: []const u8 = "",
    /// §10fn: every tenant this client follows — the roster's set from
    /// `$KV.tenants.<principal>` (a JSON array; a bare string is one tenant), then
    /// `join`/`leave` at runtime. Tenant-scoped tables seed one chain per entry and
    /// read one `CDC_<tenant>` stream per entry. Client arena.
    tenants: []const []const u8 = &.{},
    /// §10el: when each pending write was last sent by THIS process (unix ms). A
    /// `flush` used to run the reconnect pass — one direct get for the verdict, then
    /// a replay — over every pending entry, including a write sent a millisecond
    /// earlier: the direct get beat the verdict subscription and the log said the
    /// client had been "away", and every entry cost a request per flush. An entry
    /// sent within the grace window is left to the subscription. Empty at start, so a
    /// fresh process (and a real outage, longer than the grace) still runs the full
    /// pass, which is what PROTOCOL §7.4b asks. Keys owned here, freed on settle.
    sent_at: std.StringHashMapUnmanaged(i64) = .empty,
    /// §10fk: a `rate_limited` verdict names `retry_after_ms`; the outbox is not
    /// flushed before then. The write stays queued, exactly as after `failed`.
    hold_until_ms: i64 = 0,
    /// Streams the gap rule found RESTARTED (position beyond last_seq, NOTES §10bm's
    /// third shape) and no manifest has re-anchored on since. While a stream is here, a
    /// manifest whose `cutoff_seq` is beyond the stream's last_seq was cut on the previous
    /// numbering and must not gate: its seq means nothing on this stream. Measured
    /// (stream_wipe.py, 2026-09-17): the position was reset but the seed gate kept the
    /// old anchor (163), and every event of the recreated stream (seq 3–14) was dropped
    /// as "in the chain" — replica 1 row, PostgreSQL 13.
    restarted: std.StringHashMapUnmanaged(void) = .empty,
    states: std.StringArrayHashMapUnmanaged(TableState) = .empty,
    /// §10hp: this client SERVES — the queue subscriptions it drains in `poll`, and the
    /// requests it has handed the host but not yet answered. A responder is a client
    /// that answers questions about its own replica; nothing else changes.
    serve_subs: []*@import("nats").Subscription = &.{},
    serve_queue: []const u8 = "",
    pending: std.ArrayListUnmanaged(Pending) = .empty,
    next_request_id: u64 = 1,

    /// §10hn: the tables this replica holds — `opts.tables` (the declared union) or,
    /// with `follow_all`, the schemas bucket's keys as last listed; arena-owned, grows
    /// only when a new key appears.
    followed: []const []const u8 = &.{},
    /// §10do: UPDATEs judged `stale` whose columns may still be rebased onto the
    /// winning row, keyed by msg_id; strings live in the client arena (rare, small).
    rebase: std.StringArrayHashMapUnmanaged(Rebase) = .empty,
    rebase_due: bool = false,
    /// Cumulative verdict counts by status, for the host (the flush report carries
    /// them): a single-writer run with anything but `accepted` here has a finding.
    verdict_counts: VerdictCounts = .{},
    /// FK-held events (§10h) outlive the `drainStream` call that decoded them — they
    /// wait for `drainCdc`'s retry pass — but not the pass itself. So they get their
    /// own arena, reset after every pass: a third lifetime, between "this call" and
    /// "this client", and the only one that needs a deep copy (see `cloneValue`).
    /// The HLC's two inputs (§7.2). `seen_floor` is the newest version this replica
    /// has OBSERVED (from CDC), `last_version` its own last stamp. A version is
    /// strictly after both, so a lagging clock cannot stamp under a row it has seen —
    /// and arrival time never becomes the comparator, which would punish offline edits.
    ///
    /// ⚠️ `last_version` is a slice into `last_version_buf`, not an arena string: it is
    /// rewritten on every `mutate`, and a per-write dupe into the client arena would
    /// grow that arena for the life of the client (the pattern §1 of the review found
    /// in every loop entry point — the arena is a LIFETIME, not a convenience).
    seen_floor: []const u8 = "",
    seen_floor_buf: [64]u8 = undefined,
    /// §10hg: what the planned batch path did — events bulked, statements, events through applyEvent.
    bulk_stats: BulkStats = .{},
    /// Live schema (CLIENTS.md divergence 2, ported from the TS `watchSchemas`): a KV
    /// watch on the schemas bucket, drained at the top of every poll, so a host that
    /// only polls still follows a migration. Opened lazily on the first poll.
    schema_kv: ?@import("nats").KV = null,
    /// §10df: a descriptor arrived with a higher seed_epoch — seed on the next poll.
    reseed_pending: bool = false,
    /// Events the seed gate dropped, for the trace.
    gated: usize = 0,
    /// §10iz: what kept each table unseeded — the error name of its last failed seed,
    /// by table; an entry leaves when the table seeds. `zb_client_sync` and
    /// `zb_client_poll` report it as `unseeded`, so a host never calls a replica
    /// "usable" over a stderr line it cannot see (the iPhone said "usable in 15.8 s"
    /// with 0 rows). Keys in the client arena, one per table at most.
    unseeded: std.StringArrayHashMapUnmanaged([]const u8) = .empty,
    schema_watch: ?@import("nats").KVWatcher = null,
    last_version: []const u8 = "",
    last_version_buf: [64]u8 = undefined,
    /// Held open across mutate/flush: a CORE subscription only delivers what arrives
    /// while it exists, so subscribing after publishing misses the verdict every time
    /// (measured — the demo settled 0 of 1 until this was split out of drainVerdicts).
    verdicts: ?*@import("nats").Subscription = null,
    /// Live tailing (§10bh): one persistent pull consumer per CDC stream, created by
    /// the first `poll` and kept across calls. `stream` points into `states`' routes
    /// (client-lifetime memory).
    tails: std.ArrayListUnmanaged(Tail) = .empty,
    /// §10fq: streams set aside because they cannot be read right now — deleted under
    /// a live tail, denied after an account update, not created yet. A dark stream is
    /// left out of the shared fetch and the seed pass, so the others keep being
    /// served; it is retried after `retry_at_ms` with a doubling backoff, and the poll
    /// report names its tenant (`unreadable`). Keys in the client arena.
    dark: std.StringArrayHashMapUnmanaged(Dark) = .empty,
    /// The ONE inbox every tail answers into (nats.zig `PullInbox`): `poll` is a single
    /// wait over all streams, ended by the first message from any of them.
    tail_inbox: ?*@import("nats").PullInbox = null,
    /// §10jc: per stream, the contiguity of what was applied (`Flow`). Keys in the arena.
    flows: std.StringArrayHashMapUnmanaged(Flow) = .empty,

    const Tail = struct {
        stream: []const u8,
        sub: *@import("nats").PullSubscription,
        /// When the idle watch last asked the server about this tail (§10ja).
        watch_ms: i64 = 0,
        /// When this consumer last handed a message over — or was opened.
        delivered_ms: i64 = 0,
    };
    /// An idle tail asks the server at most this often: is it caught up (the position
    /// moves to the stream's end), gone (reaped: reopen), or DEAF — messages pending on
    /// the server and nothing delivered for this long (reopen, and say so). The same
    /// guard zb-client-ts has had since §10cq; libzb fetched from a deaf consumer for
    /// 416 polls under the firehose (§10ja).
    const tail_watch_ms: i64 = 25_000;
    const Dark = struct { retry_at_ms: i64, backoff_ms: i64 };
    const dark_backoff_first_ms: i64 = 5_000;
    const dark_backoff_max_ms: i64 = 60_000;
    /// The server reaps a tail consumer idle this long (`inactive_threshold`). A host
    /// that stops polling for longer gets a fresh consumer at the stored position —
    /// the reaped one answers the next fetch with `NoResponders` (nats.zig patch).
    const tail_inactive_ns: u64 = 120 * std.time.ns_per_s;
    /// §10jc: the byte cap on every pull request (tail and drain), see `tailInbox`. Two
    /// requests may be outstanding (`pull_depth`), so up to twice this is in flight, and
    /// nats-server must write it to the client within its write deadline (10 s): at 8 MB a
    /// moto e20 on Wi-Fi (under ~1.6 MB/s read) was cut as a slow consumer six times in a
    /// run ("WriteDeadline of 10s exceeded with 16 MB"). 2 MB keeps a slow link under the
    /// deadline down to ~0.4 MB/s, and costs a fast one nothing: at a 75 ms round trip a
    /// few MB/s need well under 1 MB in flight.
    const pull_max_bytes: u64 = 2 * 1024 * 1024;

    /// Acquire in order, and register the matching release BEFORE the next acquire.
    ///
    /// ⚠️ This used to leak on every failure path, and the shape is worth naming
    /// because it reads as correct: `Storage.open` sat INSIDE the struct literal while
    /// `errdefer a.destroy(self)` came AFTER it, so a database that would not open
    /// leaked the struct — and a failure in `loadGrammar` or `connect` destroyed the
    /// struct while leaving the SQLite handle open and the arena unfreed. A leaked DB
    /// handle is precisely what bites a host that retries `open` after a failure.
    ///
    /// The rule that prevents all three: one acquire per statement, its `errdefer` on
    /// the next line, in the same order `deinit` releases them in reverse.
    pub fn init(a: std.mem.Allocator, opts: Options) !*SyncClient {
        const self = try a.create(SyncClient);
        errdefer a.destroy(self);
        // ZB_TAIL_TRACE=1: the tail path narrated on stderr — what each fetch returned
        // per stream, what applying did to the position, how many events the seed gate
        // dropped, what the server said in the idle check (§10ja, the deaf tail hunt).
        trace_enabled = std.c.getenv("ZB_TAIL_TRACE") != null;
        test_drop_every = if (std.c.getenv("ZB_TEST_DROP_DELIVERY")) |v| (std.fmt.parseInt(u64, std.mem.span(v), 10) catch 0) else 0;

        self.* = .{
            .a = a,
            .arena = std.heap.ArenaAllocator.init(a),
            .t = undefined,
            .st = undefined,
            .ro = undefined,
            .opts = opts,
            .followed = opts.tables,
        };
        errdefer self.arena.deinit();

        self.st = if (opts.db_url) |url|
            try storage.Storage.openPostgres(url, false)
        else if (opts.engine == .duckdb)
            try storage.Storage.openDuckdb(opts.db_path)
        else
            try storage.Storage.open(opts.db_path);
        errdefer self.st.close();
        // After the read-write open: that is what creates the file. DuckDB allows one
        // open of a file per process, so the card's handle is a second connection on
        // the same database (the read-only rule is the SQL guard, core.isReadOnlySql).
        self.ro = if (opts.db_url) |url|
            try storage.Storage.openPostgres(url, true)
        else if (opts.engine == .duckdb)
            try storage.Storage.openDuckdbShared(&self.st)
        else
            try storage.Storage.openReadOnly(opts.db_path);
        errdefer self.ro.close();

        // Before the transport: the grammar is compiled in (§10dq), and a hash
        // mismatch must refuse before a socket is opened with the wrong names.
        try self.loadGrammar();

        // An empty credsPath is "no creds" (an anonymous server), not a file named "": the
        // C ABI defaults the field to "" and nats.zig opened it — FileNotFound at open.
        // §10hm: this principal's own inbox space. A deployment granting
        // `_INBOX.<principal>.>` isolates replies with it; under the wider
        // `_INBOX.>` grant it costs nothing and changes nothing.
        const inbox_prefix = if (opts.principal.len > 0)
            try std.fmt.allocPrint(self.aa(), "_INBOX.{s}", .{opts.principal})
        else
            "_INBOX";
        self.t = try transport.Transport.connect(a, .{ .url = opts.url, .creds_path = if (opts.creds_path.len > 0) opts.creds_path else null, .creds = if (opts.creds.len > 0) opts.creds else null, .inbox_prefix = inbox_prefix, .js_domain = opts.js_domain });
        errdefer self.t.deinit();

        return self;
    }

    /// Release EVERY handle, in the exact reverse of `init`'s acquisition order.
    ///
    /// ⚠️ The subscription must go before the transport: nats.zig panics if a
    /// connection is destroyed with a live subscription, and it is right to — a
    /// subscription is owned by its creator, so cascading the destroy would leave the
    /// caller holding a pointer into freed state, which is a use-after-free later and
    /// somewhere unrelated. The panic reports the mistake where it is made.
    ///
    /// That panic found a real hole here: `verdicts` is the only handle this client
    /// holds ACROSS calls, and it was added as a field without being added to this
    /// function. `releasables()` below exists so the next such field cannot be
    /// forgotten the same way — it is the one list, and both this and the test read it.
    pub fn deinit(self: *SyncClient) void {
        self.releaseHandles();
        var sit = self.sent_at.iterator();
        while (sit.next()) |e| self.a.free(e.key_ptr.*);
        self.sent_at.deinit(self.a);
        self.flows.deinit(self.a);
        self.ro.close();
        self.st.close();
        self.arena.deinit();
        self.a.destroy(self);
    }

    /// Everything acquired after the transport, released before it — in ONE place, so
    /// that adding a handle has an obvious home and cannot be forgotten the way
    /// `verdicts` was. (Only `deinit` calls it today; a reconnect that had to release
    /// and re-acquire this set would call the same function.)
    fn releaseHandles(self: *SyncClient) void {
        if (self.schema_watch) |*w| {
            w.deinit();
            self.schema_watch = null;
        }
        if (self.schema_kv) |*kv| {
            kv.deinit();
            self.schema_kv = null;
        }
        if (self.verdicts) |sub| {
            sub.deinit();
            self.verdicts = null;
        }
        // §10hp: the serve subscriptions, and the inboxes of questions never answered.
        // nats.zig panics on a connection destroyed with a live subscription, which is
        // how this was found the first time it was written.
        for (self.serve_subs) |sub| sub.deinit();
        self.serve_subs = &.{};
        for (self.pending.items) |p| {
            self.a.free(p.reply_subject);
            self.a.free(p.tenant);
        }
        self.pending.deinit(self.a);
        self.dropTails();
        self.t.deinit();
    }

    fn dropTails(self: *SyncClient) void {
        for (self.tails.items) |t| t.sub.deinit();
        self.tails.clearRetainingCapacity();
        // After the subscriptions that borrow it.
        if (self.tail_inbox) |ib| {
            ib.deinit();
            self.tail_inbox = null;
        }
    }

    /// §10iz: one entry per table; the reason is an error name (static), the key is
    /// duplicated once into the client arena and reused after that.
    fn rememberUnseeded(self: *SyncClient, table: []const u8, reason: []const u8) void {
        const gop = self.unseeded.getOrPut(self.aa(), table) catch return;
        if (!gop.found_existing) {
            gop.key_ptr.* = self.aa().dupe(u8, table) catch {
                _ = self.unseeded.swapRemove(table);
                return;
            };
        }
        gop.value_ptr.* = reason;
    }

    fn aa(self: *SyncClient) std.mem.Allocator {
        return self.arena.allocator();
    }

    fn loadGrammar(self: *SyncClient) !void {
        // Client-lifetime arena, deliberately: the grammar strings below are read for
        // the life of the client, and this runs once.
        const a = self.aa();
        if (self.opts.grammar_hash) |want| {
            var buf: [64]u8 = undefined;
            const have = grammarHashHex(&buf);
            if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, want, " \t\r\n"), have)) {
                std.debug.print("grammar mismatch: this library embeds {s}, the bridge serves {s} — built for another protocol; rebuild the client\n", .{ have, want });
                return error.GrammarMismatch;
            }
        }
        const g = try std.json.parseFromSlice(Value, a, grammar_json, .{});
        const root = g.value;
        self.cdc_prefix = try grammarString(root, &.{ "cdc_streams", "tenant_prefix" });
        self.cdc_public = try grammarString(root, &.{ "cdc_streams", "public" });
        self.subject_cdc_prefix = grammarString(root, &.{ "subjects", "cdc_prefix" }) catch "cdc";
        self.subject_query_prefix = grammarString(root, &.{ "subjects", "query_prefix" }) catch "query";
        self.results_bucket_prefix = grammarString(root, &.{ "results", "bucket_prefix" }) catch "res-";
        if (root.object.get("results")) |rv| if (rv == .object) {
            if (rv.object.get("inline_max_bytes")) |v| if (v == .integer and v.integer > 0) {
                self.results_inline_max = @intCast(v.integer);
            };
            if (rv.object.get("max_age_seconds")) |v| if (v == .integer and v.integer >= 0) {
                self.results_max_age_ns = @as(u64, @intCast(v.integer)) * std.time.ns_per_s;
            };
        };
        self.kv_schemas = try grammarString(root, &.{ "kv", "schemas" });
        self.kv_tenants = try grammarString(root, &.{ "kv", "tenants" });
        self.kv_live = grammarString(root, &.{ "kv", "live" }) catch "live";
        self.kv_generations = try grammarString(root, &.{ "generations", "kv" });
        self.gen_bucket_prefix = try grammarString(root, &.{ "generations", "bucket_prefix" });
        self.open_tenant = try grammarString(root, &.{"open_tenant"});
        self.open_tenants = try self.aa().dupe([]const u8, &.{self.open_tenant});
        self.stream_mutations = try grammarString(root, &.{ "streams", "mutations" });
        self.subject_mutations_prefix = try grammarString(root, &.{ "subjects", "mutations_prefix" });
        self.subject_mutation_ack_prefix = try grammarString(root, &.{ "subjects", "mutation_ack_prefix" });
    }

    // ─── step 0: tenant (PROTOCOL §6 Step 0 — resolved, never guessed) ──────

    pub fn resolveTenant(self: *SyncClient) !void {
        const bytes = (try self.t.kvGet(self.aa(), self.kv_tenants, self.opts.principal)) orelse {
            self.tenants = &.{};
            self.tenant = self.open_tenant;
            self.tenant_missing = true;
            std.debug.print("tenant: {s} has no mapping ($KV.tenants.{s}) — revoked, or never enrolled: tenant-scoped tables are skipped, public tables follow\n", .{ self.opts.principal, self.opts.principal });
            return;
        };
        self.tenants = try parseTenantList(self.aa(), decodeMaybeMsgpackString(self.aa(), bytes) catch bytes);
        self.tenant_missing = self.tenants.len == 0;
        self.tenant = if (self.tenants.len > 0) self.tenants[0] else self.open_tenant;
        if (self.tenants.len == 1) {
            std.debug.print("tenant: {s} -> {s}\n", .{ self.opts.principal, self.tenant });
        } else {
            std.debug.print("tenant: {s} -> {d} membership(s)", .{ self.opts.principal, self.tenants.len });
            for (self.tenants) |t| std.debug.print(" {s}", .{t});
            std.debug.print("\n", .{});
        }
    }

    /// §10fn: the roster set as the bridge writes it — `["acme","globex"]` — or a
    /// bare tenant string (one tenant, the pre-§10fn value). Sorted, deduplicated.
    fn parseTenantList(a: std.mem.Allocator, text: []const u8) ![]const []const u8 {
        var list: std.ArrayListUnmanaged([]const u8) = .empty;
        const trimmed = std.mem.trim(u8, text, " \t\r\n");
        if (trimmed.len > 0 and trimmed[0] == '[') {
            const v = std.json.parseFromSliceLeaky(Value, a, trimmed, .{}) catch return error.TenantListMalformed;
            if (v != .array) return error.TenantListMalformed;
            for (v.array.items) |it| if (it == .string and it.string.len > 0) try appendUnique(a, &list, it.string);
        } else if (trimmed.len > 0) {
            try appendUnique(a, &list, trimmed);
        }
        std.mem.sort([]const u8, list.items, {}, lessStr);
        return list.items;
    }

    fn appendUnique(a: std.mem.Allocator, list: *std.ArrayListUnmanaged([]const u8), t: []const u8) !void {
        for (list.items) |x| if (std.mem.eql(u8, x, t)) return;
        try list.append(a, try a.dupe(u8, t));
    }

    fn lessStr(_: void, x: []const u8, y: []const u8) bool {
        return std.mem.lessThan(u8, x, y);
    }

    /// The tenants a table's chains are keyed on: every membership for a
    /// tenant-scoped table, the open tenant alone for a public one.
    fn tenantsFor(self: *SyncClient, st: TableState) []const []const u8 {
        return if (st.tenant_col != null) self.tenants else self.open_tenants[0..1];
    }

    /// The stream a tenant's rows of a table ride: `CDC_<tenant>`, or CDC_PUBLIC for
    /// the open tenant (its rows ride the public stream; there is no `CDC_<open>`).
    fn routeFor(self: *SyncClient, a: std.mem.Allocator, tenant: []const u8) ![]const u8 {
        if (std.mem.eql(u8, tenant, self.open_tenant)) return self.cdc_public;
        return try std.fmt.allocPrint(a, "{s}{s}", .{ self.cdc_prefix, tenant });
    }

    /// §10fn: the routes of a table under the current membership. Client arena.
    /// §10hj: an on-demand table — schema yes, seed and tail no.
    fn isOnDemand(self: *SyncClient, table: []const u8) bool {
        for (self.opts.ondemand) |t| if (std.mem.eql(u8, t, table)) return true;
        return false;
    }

    fn routesFor(self: *SyncClient, tenant_col: ?[]const u8) ![]const []const u8 {
        const ca = self.aa();
        if (tenant_col == null) return try ca.dupe([]const u8, &.{self.cdc_public});
        var routes: std.ArrayListUnmanaged([]const u8) = .empty;
        for (self.tenants) |t| try routes.append(ca, try self.routeFor(ca, t));
        return routes.items;
    }

    /// The table is tenant-scoped and the client follows several tenants: a full
    /// chain of one tenant may only clear THAT tenant's rows, and its rows go in as
    /// upserts (the open rows it carries are already there from a sibling's chain).
    fn multiTenant(self: *SyncClient, st: TableState) bool {
        return st.tenant_col != null and self.tenants.len > 1;
    }

    // ─── step 1: schemas → local DDL, existence from the DATABASE (finding 9) ─

    /// The outcome of one table's migration — what `syncSchemas` logs and what the
    /// unit test asserts.
    pub const Migration = enum { created, unchanged, altered, rebuilt, rekeyed, emptied };

    /// Bring one replica table in line with its published descriptor, deciding from
    /// the DATABASE (finding 9: `PRAGMA table_info`, `sqlite_master`), never from
    /// memory. The port of the TS shell's `applySchema` decision path, with the
    /// planner already in `core` (fixture-pinned): first sight → create; column
    /// diff (rename-aware) → ALTER; an FK change, or an ALTER SQLite refuses →
    /// rebuild copying the common columns; the `_view` dropped before and recreated
    /// after; indexes synced on EVERY path, because an index added upstream changes
    /// no column and lands on the "unchanged" path (the TS shell's ⚠️).
    ///
    /// Touches only `st`, so it is testable against a scratch SQLite with no NATS.
    /// (§10bi — the "increment 2" this file promised in §10v.)
    pub fn migrateTable(st: *storage.Storage, a: std.mem.Allocator, table: []const u8, val: Value) !Migration {
        // Schema surgery ahead (possibly): cached statements referencing this
        // table would fail forever after a rebuild — clear first, cheap certainty.
        st.clearStmtCache();
        // ⚠️ Never `.?` into a descriptor. A suspension (`{"table":…,"suspended":…}`,
        // §5) has no columns, and a host behind the C ABI cannot catch a Zig panic —
        // measured 2026-08-29: a refused table's 97-byte suspension reached this
        // function and took a Python process down. Unusable → an error the caller
        // logs and skips; the replica keeps what it has.
        const cols_v = (try descriptorColumns(a, val, st.engine)) orelse return error.SchemaUnusable;
        const pk = try jsonStrList(a, val.object.get("pk_columns"));
        var names: std.ArrayList([]const u8) = .empty;
        for (cols_v.array.items) |c| try names.append(a, c.object.get("name").?.string);
        const empty_arr = Value{ .array = std.json.Array.init(a) };
        // §10hh: a DuckDB replica carries NO foreign keys. DuckDB can neither defer a check
        // to COMMIT (PROTOCOL §4's rule for a batch) nor switch it off for a seed, and a
        // seed's chains are cut per table at different times — measured: a prices chain
        // still holding rows of a station the newer stations chain had tombstoned, the
        // whole seed refused, the tail then landing a day's rows on an empty table. The
        // rows converge to PostgreSQL's, which holds the constraint for everyone.
        const fks = if (st.engine == .duckdb) empty_arr else (val.object.get("foreign_keys") orelse empty_arr);
        const idx = val.object.get("indexes") orelse empty_arr;
        const renamed = val.object.get("renamed") orelse Value{ .object = .empty };

        // §10dj: a host killed between a rebuild's DROP and its RENAME leaves the rows
        // in `<table>__migrating` and no `<table>`: finish the rename, the rows are there.
        {
            const tmp = try std.fmt.allocPrint(a, "{s}__migrating", .{table});
            const tmp_info = try st.tableColumns(a, tmp);
            const real_info = try st.tableColumns(a, table);
            if (tmp_info.len > 0 and real_info.len == 0) {
                try execSql(st, a, try std.fmt.allocPrint(a, "ALTER TABLE \"{s}\" RENAME TO \"{s}\";", .{ tmp, table }));
                std.debug.print("{s}: a rebuild was interrupted before its rename — adopted {s}\n", .{ table, tmp });
            }
        }
        // FINDING 9: existence — and the existing columns — are the DATABASE's to answer.
        const info = try st.tableColumns(a, table);
        const existing: ?[]const []const u8 = if (info.len > 0) info else null;

        // §10dg: the shape this replica BUILT — its own record, not an introspection
        // (whose type text differs per engine). Key shape moved → re-key: the table
        // is rebuilt EMPTY (rows keyed the old way cannot be re-keyed in place, and
        // SQLite's INTEGER PRIMARY KEY refuses a uuid) and the caller re-seeds it.
        // A non-key type moved → a rebuild that keeps the rows (below).
        const key_now = try core.keyShape(a, pk, cols_v.array);
        const type_now = try core.typeShape(a, cols_v.array);
        try ensureShape(st);
        const shape_rows = try st.query(a, "SELECT key_shape, type_shape FROM _zbz_shape WHERE tbl = ?", &.{.{ .text = table }});
        const key_before: ?[]const u8 = if (shape_rows.len > 0 and shape_rows[0][0] == .text) shape_rows[0][0].text else null;
        const type_before: ?[]const u8 = if (shape_rows.len > 0 and shape_rows[0][1] == .text) shape_rows[0][1].text else null;
        // With no record (a replica from before the record, or one lost to a kill) the
        // PHYSICAL pk column names decide — names only, never the engine's type text.
        const rekey = blk: {
            if (existing == null) break :blk false;
            if (key_before) |kb| break :blk !std.mem.eql(u8, kb, key_now);
            const phys = try st.tablePkColumns(a, table);
            if (phys.len == 0 or phys.len != pk.len) break :blk phys.len > 0;
            for (phys, 0..) |name, k| if (!std.mem.eql(u8, name, pk[k])) break :blk true;
            break :blk false;
        };
        const retyped = (try core.retypedColumns(a, if (rekey) null else type_before, cols_v.array)).array.items;

        var outcome: Migration = .unchanged;
        if (existing == null or rekey) {
            if (rekey) {
                // ⚠️ `key_before` is null on the very path this branch exists for: a
                // replica with no recorded shape — one from before the record, or one a
                // kill interrupted — where the PHYSICAL pk columns decided the re-key.
                // Unwrapping it aborted the whole client process (measured: rebuild_kill).
                std.debug.print("{s}: key shape changed ({s} -> {s}) — rebuilding EMPTY\n", .{ table, key_before orelse "no recorded shape; the physical key decided", key_now });
                try execSql(st, a, try std.fmt.allocPrint(a, "DROP VIEW IF EXISTS {s}_view;", .{table}));
                try execSql(st, a, "PRAGMA foreign_keys = OFF;");
            }
            const steps = try core.createTableSteps(a, table, cols_v.array, pk, fks.array, st.engine == .sqlite);
            for (steps.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
            if (rekey) {
                try execSql(st, a, "PRAGMA foreign_keys = ON;");
                const vsteps = try core.viewSteps(a, table, names.items);
                for (vsteps.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
            }
            outcome = if (rekey) .rekeyed else .created;
        } else {
            const diff = try core.diffColumns(a, existing, names.items, renamed);
            const renames = diff.object.get("renames").?.array.items;
            const added = diff.object.get("added").?.array.items;
            const removed = diff.object.get("removed").?.array.items;
            const fk_clauses = try core.fkClausesFor(a, fks.array);
            // §10fd: PostgreSQL keeps no CREATE TABLE text; an FK change is not detected there.
            const ddl: []const u8 = (try st.tableDdl(a, table)) orelse "";
            const fk_differs = if (st.engine == .postgres) false else try core.fkTextDiffers(a, ddl, fk_clauses);
            // §10fi: a table from before STRICT is rebuilt once, rows carried.
            const strict_missing = st.engine == .sqlite and core.strictMissing(ddl);
            if (strict_missing) std.debug.print("{s}: not a STRICT table — rebuilding, rows kept\n", .{table});
            const shape_changed = renames.len > 0 or added.len > 0 or removed.len > 0 or retyped.len > 0;

            if (shape_changed or fk_differs or strict_missing) {
                // The view goes FIRST (§1.17): DROP COLUMN re-validates every schema
                // object referencing the table, and a stale view kills the ALTER.
                try execSql(st, a, try std.fmt.allocPrint(a, "DROP VIEW IF EXISTS {s}_view;", .{table}));
                // And so do the indexes no longer published — SQLite refuses DROP
                // COLUMN on an indexed column, which sent a plain remove down the
                // rebuild path in the unit test before this line existed. Drops here,
                // creates after the shape settled (the plan below).
                const pre = try indexPlan(st, a, table, idx.array);
                for (pre.object.get("drops").?.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
                for (renames) |pair| {
                    try execSql(st, a, try std.fmt.allocPrint(a, "ALTER TABLE {s} RENAME COLUMN \"{s}\" TO \"{s}\";", .{ table, pair.array.items[0].string, pair.array.items[1].string }));
                }
                outcome = .altered;
                const altered = blk: {
                    for (removed) |n| execSql(st, a, try std.fmt.allocPrint(a, "ALTER TABLE {s} DROP COLUMN \"{s}\";", .{ table, n.string })) catch break :blk false;
                    for (added) |n| {
                        const ty = columnType(cols_v.array, n.string) orelse "TEXT";
                        // A constant default rides along (§10df): SQLite fills the existing
                        // rows with it, so PostgreSQL's old rows and ours converge.
                        const dflt: []const u8 = if (columnDefault(cols_v.array, n.string)) |d| try std.fmt.allocPrint(a, " DEFAULT {s}", .{d}) else "";
                        execSql(st, a, try std.fmt.allocPrint(a, "ALTER TABLE {s} ADD COLUMN \"{s}\" {s}{s};", .{ table, n.string, ty, dflt })) catch break :blk false;
                    }
                    // SQLite has no ALTER TABLE ADD/DROP CONSTRAINT: an FK change is a rebuild.
                    if (fk_differs or strict_missing) break :blk false;
                    // Nor ALTER COLUMN TYPE (§10dg): a re-typed column is a rebuild that
                    // copies the rows — affinity converts what converts.
                    if (retyped.len > 0) {
                        std.debug.print("{s}: {d} column(s) re-typed — rebuilding, rows kept\n", .{ table, retyped.len });
                        break :blk false;
                    }
                    break :blk true;
                };
                if (!altered) {
                    // Schema surgery on a consistent copy: with foreign_keys ON the DROP
                    // of a referenced parent is refused (measured: users, blocked by
                    // salaries' FK). Off for the surgery, on after — data is copied.
                    try execSql(st, a, "PRAGMA foreign_keys = OFF;");
                    const steps = try core.rebuildSteps(a, table, cols_v.array, pk, fks.array, existing.?, st.engine == .sqlite);
                    const carried = blk: {
                        for (steps.array.items) |stp| {
                            const sql = stp.object.get("sql").?.string;
                            execSql(st, a, sql) catch {
                                std.debug.print("{s}: rows could not be carried through the rebuild ({s}) at: {s}\n", .{ table, st.errMsg(), sql });
                                break :blk false;
                            };
                        }
                        break :blk true;
                    };
                    if (!carried) {
                        // §10dg: a migration that cannot carry the rows degrades to a
                        // re-seed, never to a table stuck in its old shape — measured on
                        // a re-key: the parent stood empty (waiting for its full) while
                        // the child's copy hit its FOREIGN KEY. Empty now; the caller
                        // drops the watermark and the next full brings the rows back.
                        try execSql(st, a, try std.fmt.allocPrint(a, "DROP TABLE IF EXISTS \"{s}__migrating\";", .{table}));
                        const fresh = try core.createTableSteps(a, table, cols_v.array, pk, fks.array, st.engine == .sqlite);
                        for (fresh.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
                    }
                    try execSql(st, a, "PRAGMA foreign_keys = ON;");
                    outcome = if (carried) .rebuilt else .emptied;
                }
                const vsteps = try core.viewSteps(a, table, names.items);
                for (vsteps.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
            }
        }

        // Indexes on every path — after a rebuild, because the DROP took them along.
        const plan = try indexPlan(st, a, table, idx.array);
        for (plan.object.get("drops").?.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
        for (plan.object.get("creates").?.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
        // The shape record, on every path (§10dg) — including the first sight, so the
        // NEXT descriptor has something to be compared with.
        _ = try st.query(a, "INSERT INTO _zbz_shape (tbl, key_shape, type_shape) VALUES (?, ?, ?) ON CONFLICT(tbl) DO UPDATE SET key_shape = excluded.key_shape, type_shape = excluded.type_shape", &.{ .{ .text = table }, .{ .text = key_now }, .{ .text = type_now } });
        return outcome;
    }

    /// `core.indexSyncPlan` over what the database actually holds (finding 9 again).
    fn indexPlan(st: *storage.Storage, a: std.mem.Allocator, table: []const u8, want: std.json.Array) !Value {
        const have = try st.indexNames(a, table);
        return core.indexSyncPlan(a, table, have, want);
    }

    /// The `sqlite.columns` array of a descriptor, or null for anything that is not a
    /// usable descriptor (a suspension, a non-object, a missing key).
    /// The descriptor's column block for this engine: `sqlite` (its types are SQLite's)
    /// or `pg` (PostgreSQL's own `format_type` spellings — usable in CREATE TABLE as
    /// they are, PostGIS and arrays included).
    fn descriptorColumns(a: std.mem.Allocator, val: Value, engine: storage.Engine) !?Value {
        if (val != .object) return null;
        const sq = val.object.get(if (engine == .sqlite) "sqlite" else "pg") orelse return null;
        if (sq != .object) return null;
        const cols = sq.object.get("columns") orelse return null;
        if (cols != .array) return null;
        if (engine != .duckdb) return cols;
        // §10fl: DuckDB reads PostgreSQL's type names for most types; the few it
        // does not are mapped here on a copy of the column list.
        var out = std.json.Array.init(a);
        for (cols.array.items) |c| {
            if (c != .object) {
                try out.append(c);
                continue;
            }
            var copy = try c.object.clone(a);
            if (c.object.get("type")) |t| if (t == .string) try copy.put(a, "type", .{ .string = try duckdbType(a, t.string) });
            try out.append(.{ .object = copy });
        }
        return .{ .array = out };
    }

    /// §10fl: a PostgreSQL type (format_type's spelling) as DuckDB's. Most names
    /// are DuckDB's too (integer, bigint, boolean, text, uuid, timestamp with time
    /// zone, double precision, text[]); the rest: bytes and PostGIS as BLOB, pgvector
    /// as a fixed FLOAT array (sparsevec as the wire's BLOB), json as JSON, a
    /// numeric with a modifier as the same DECIMAL, one without as DECIMAL(38,10)
    /// (DuckDB has no unbounded decimal), bit as BIT.
    pub fn duckdbType(a: std.mem.Allocator, pg: []const u8) ![]const u8 {
        const bare = if (std.mem.indexOfScalar(u8, pg, '(')) |i| pg[0..i] else pg;
        const mod: ?[]const u8 = if (std.mem.indexOfScalar(u8, pg, '(')) |i| pg[i + 1 .. (std.mem.lastIndexOfScalar(u8, pg, ')') orelse pg.len)] else null;
        if (std.mem.eql(u8, bare, "bytea") or std.mem.eql(u8, bare, "geometry") or std.mem.eql(u8, bare, "geography") or std.mem.eql(u8, bare, "sparsevec")) return "BLOB";
        if (std.mem.eql(u8, bare, "vector") or std.mem.eql(u8, bare, "halfvec")) return if (mod) |m| try std.fmt.allocPrint(a, "FLOAT[{s}]", .{m}) else "FLOAT[]";
        if (std.mem.eql(u8, bare, "json") or std.mem.eql(u8, bare, "jsonb")) return "JSON";
        if (std.mem.eql(u8, bare, "numeric") or std.mem.eql(u8, bare, "decimal")) return if (mod) |m| try std.fmt.allocPrint(a, "DECIMAL({s})", .{m}) else "DECIMAL(38,10)";
        if (std.mem.eql(u8, bare, "bit") or std.mem.eql(u8, bare, "bit varying")) return "BIT";
        if (std.mem.eql(u8, bare, "tsvector") or std.mem.eql(u8, bare, "xml")) return "TEXT";
        return pg;
    }

    /// §10fe: the PostGIS columns named by the descriptor's `pg` block.
    fn geomColsOf(a: std.mem.Allocator, val: Value) ![]const []const u8 {
        var out: std.ArrayListUnmanaged([]const u8) = .empty;
        const pg = if (val == .object) (val.object.get("pg") orelse Value.null) else Value.null;
        if (pg != .object) return out.items;
        const cols = pg.object.get("columns") orelse return out.items;
        if (cols != .array) return out.items;
        for (cols.array.items) |c| {
            if (c != .object) continue;
            const n = c.object.get("name") orelse continue;
            const t = c.object.get("type") orelse continue;
            if (n == .string and t == .string and (std.mem.startsWith(u8, t.string, "geometry") or std.mem.startsWith(u8, t.string, "geography"))) try out.append(a, n.string);
        }
        return out.items;
    }

    /// §10fg: the pgvector and bit(n) columns named by the descriptor's `pg` block,
    /// with a bit(n)'s declared length. `bit varying` is not one: it travels as text.
    fn vecColsOf(a: std.mem.Allocator, val: Value) ![]const VecCol {
        var out: std.ArrayListUnmanaged(VecCol) = .empty;
        const pg = if (val == .object) (val.object.get("pg") orelse Value.null) else Value.null;
        if (pg != .object) return out.items;
        const cols = pg.object.get("columns") orelse return out.items;
        if (cols != .array) return out.items;
        for (cols.array.items) |c| {
            if (c != .object) continue;
            const n = c.object.get("name") orelse continue;
            const t = c.object.get("type") orelse continue;
            if (n != .string or t != .string) continue;
            const ty = t.string;
            const bare = if (std.mem.indexOfScalar(u8, ty, '(')) |i| ty[0..i] else ty;
            const kind: core.VecKind = if (std.mem.eql(u8, bare, "vector")) .vector else if (std.mem.eql(u8, bare, "halfvec")) .halfvec else if (std.mem.eql(u8, bare, "sparsevec")) .sparsevec else if (std.mem.eql(u8, bare, "bit")) .bit else continue;
            var bits: u32 = 0;
            if (kind == .bit) if (std.mem.indexOfScalar(u8, ty, '(')) |i| {
                const close = std.mem.indexOfScalar(u8, ty, ')') orelse ty.len;
                bits = std.fmt.parseInt(u32, ty[i + 1 .. close], 10) catch 0;
            };
            try out.append(a, .{ .name = n.string, .kind = kind, .bits = bits });
        }
        return out.items;
    }

    fn dupeVecCols(a: std.mem.Allocator, src: []const VecCol) ![]const VecCol {
        const out = try a.alloc(VecCol, src.len);
        for (src, 0..) |v, i| out[i] = .{ .name = try a.dupe(u8, v.name), .kind = v.kind, .bits = v.bits };
        return out;
    }

    /// §10fd: the array columns named by the descriptor's `pg` block (`type` ending in `[]`).
    fn arrayColsOf(a: std.mem.Allocator, val: Value) ![]const []const u8 {
        var out: std.ArrayListUnmanaged([]const u8) = .empty;
        const pg = if (val == .object) (val.object.get("pg") orelse Value.null) else Value.null;
        if (pg != .object) return out.items;
        const cols = pg.object.get("columns") orelse return out.items;
        if (cols != .array) return out.items;
        for (cols.array.items) |c| {
            if (c != .object) continue;
            const n = c.object.get("name") orelse continue;
            const t = c.object.get("type") orelse continue;
            if (n == .string and t == .string and std.mem.endsWith(u8, t.string, "[]")) try out.append(a, n.string);
        }
        return out.items;
    }

    /// §10fd: on a PostgreSQL replica, the array columns of a row object become the
    /// literal — in place, on the copy the caller holds. §10fg: and the pgvector
    /// columns' bytes (the `$bin` marker) become pgvector's text form.
    fn pgArrayFixup(self: *SyncClient, a: std.mem.Allocator, st: TableState, data: *Value) !void {
        if ((self.st.engine != .postgres and self.st.engine != .duckdb) or data.* != .object) return;
        if (self.st.engine == .postgres) for (st.array_cols) |col| {
            const v = data.object.get(col) orelse continue;
            const lit: []const u8 = switch (v) {
                .string => |txt| try jsonArrayToLiteral(a, txt),
                .array => |arr| try core.pgArrayLiteral(a, arr),
                else => continue,
            };
            try data.object.put(a, col, .{ .string = lit });
        };
        for (st.vec_cols) |vc| {
            if (self.st.engine == .duckdb and vc.kind == .sparsevec) continue;
            const v = data.object.get(vc.name) orelse continue;
            const bytes = try binOf(a, v) orelse continue;
            try data.object.put(a, vc.name, .{ .string = try core.vecLiteral(a, vc.kind, bytes, vc.bits) });
        }
    }

    fn columnDefault(cols: std.json.Array, name: []const u8) ?[]const u8 {
        for (cols.items) |c| {
            if (c != .object) continue;
            const n = c.object.get("name") orelse continue;
            if (n != .string or !std.mem.eql(u8, n.string, name)) continue;
            const d = c.object.get("default") orelse return null;
            return if (d == .string and d.string.len > 0) d.string else null;
        }
        return null;
    }

    fn columnType(cols: std.json.Array, name: []const u8) ?[]const u8 {
        for (cols.items) |c| {
            if (std.mem.eql(u8, c.object.get("name").?.string, name)) {
                return if (c.object.get("type")) |t| (if (t == .string) t.string else null) else null;
            }
        }
        return null;
    }

    fn execSql(st: *storage.Storage, a: std.mem.Allocator, sql: []const u8) !void {
        // §10fd: a rebuild drops a table other tables may reference; PostgreSQL wants
        // that said (SQLite has the FK pragma off for the duration instead).
        if ((st.engine == .postgres or st.engine == .duckdb) and std.mem.startsWith(u8, sql, "DROP TABLE IF EXISTS ") and std.mem.indexOf(u8, sql, "CASCADE") == null) {
            const body = std.mem.trimEnd(u8, sql, "; ");
            _ = try st.query(a, try std.fmt.allocPrint(a, "{s} CASCADE;", .{body}), &.{});
            return;
        }
        _ = try st.query(a, sql, &.{});
    }

    /// Every table's descriptor from KV through `migrateTable`, then the in-memory
    /// state (pk, columns, route…) refreshed only when it changed — this runs on
    /// every `syncOnce`, and the client-lifetime arena must not grow on a no-op.
    pub fn syncSchemas(self: *SyncClient) !void {
        var sa = std.heap.ArenaAllocator.init(self.a);
        defer sa.deinit();
        const a = sa.allocator();
        if (self.opts.follow_all) try self.refreshFollowed(a);
        for (self.followed) |table| {
            const bytes = (try self.t.kvGet(a, self.kv_schemas, table)) orelse {
                std.debug.print("schema missing for {s}\n", .{table});
                continue;
            };
            const val = (try std.json.parseFromSlice(Value, a, bytes, .{})).value;
            try self.applyDescriptor(a, table, val);
        }
        try execDdl(&self.st, "CREATE TABLE IF NOT EXISTS _zbz_stream_seq (stream TEXT PRIMARY KEY, last_seq INTEGER NOT NULL)");
        // The stream's identity beside its position (§10bm's third shape, made exact): a
        // position is only meaningful on the stream it was taken on, and `stored >
        // last_seq` can only notice a restart while the new stream is still shorter
        // than the old position. A replica from before this column has NULL and learns
        // the identity on its next sync.
        execDdl(&self.st, "ALTER TABLE _zbz_stream_seq ADD COLUMN created TEXT") catch {};
        try ensureInbox(&self.st);
        try self.ensureGenerations(a);
        try ensureShape(&self.st);
        // A schema that just moved may be what a held event was waiting for.
        self.retryHeld(null, null);
    }

    /// §10fn: the watermark table, keyed by (table, tenant) — one chain per tenant of
    /// a tenant-scoped table, the open tenant for a public one. A replica from before
    /// (keyed by table alone) is carried over under the tenant it followed then, so
    /// the upgrade costs no re-seed.
    fn ensureGenerations(self: *SyncClient, a: std.mem.Allocator) !void {
        // §10hg: DuckDB RAISES on pragma_table_info of a table that does not exist, where
        // SQLite answers no rows — on a fresh DuckDB replica this `try` ended the whole
        // bookkeeping setup before the CREATE, and every seed then failed on the missing
        // table ("Catalog Error: Table with name _zbz_generations does not exist").
        const cols = self.st.query(a, "SELECT name FROM pragma_table_info('_zbz_generations')", &.{}) catch &.{};
        var has_tenant = false;
        for (cols) |r| if (r.len > 0 and r[0] == .text and std.mem.eql(u8, r[0].text, "tenant")) {
            has_tenant = true;
        };
        if (cols.len > 0 and !has_tenant) {
            execDdl(&self.st, "ALTER TABLE _zbz_generations ADD COLUMN seed_epoch INTEGER NOT NULL DEFAULT 0") catch {}; // a replica from before §10df
            try execDdl(&self.st, "ALTER TABLE _zbz_generations RENAME TO _zbz_generations_v1");
        }
        try execDdl(&self.st, "CREATE TABLE IF NOT EXISTS _zbz_generations (tbl TEXT NOT NULL, tenant TEXT NOT NULL DEFAULT '', watermark TEXT, cutoff_lsn INTEGER, seed_epoch INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (tbl, tenant))");
        // §10jc: the seed gate's anchor, persisted with the watermark it belongs to. Kept
        // in memory only, it died with the process: a client killed after a re-seed and
        // before its tail passed the chain's cutoff re-applied, on relaunch, events the
        // chain already carried in newer versions. (Existing replicas gain the columns.)
        execDdl(&self.st, "ALTER TABLE _zbz_generations ADD COLUMN anchor_stream TEXT") catch {};
        execDdl(&self.st, "ALTER TABLE _zbz_generations ADD COLUMN anchor_seq INTEGER") catch {};
        if (cols.len > 0 and !has_tenant) {
            const old = try self.st.query(a, "SELECT tbl, watermark, cutoff_lsn, seed_epoch FROM _zbz_generations_v1", &.{});
            for (old) |r| {
                if (r.len < 4 or r[0] != .text) continue;
                const st = self.states.get(r[0].text) orelse continue;
                const tenant = self.tenantsFor(st);
                if (tenant.len != 1) continue; // several: seeded again, per tenant
                _ = try self.st.query(a, "INSERT OR REPLACE INTO _zbz_generations (tbl, tenant, watermark, cutoff_lsn, seed_epoch) VALUES (?, ?, ?, ?, ?)", &.{ r[0], .{ .text = tenant[0] }, r[1], r[2], r[3] });
            }
            try execDdl(&self.st, "DROP TABLE _zbz_generations_v1");
            std.debug.print("_zbz_generations: keyed by (table, tenant) now — {d} watermark(s) carried over\n", .{old.len});
        }
    }

    /// One descriptor onto one table: migrate the physical table and refresh the
    /// TableState — or, on a tombstone, drop the local table (the TS `dropLocalTable`).
    /// Shared by `syncSchemas` (every table, on sync) and `drainSchemaWatch` (whatever
    /// moved, on poll).
    fn applyDescriptor(self: *SyncClient, a: std.mem.Allocator, table: []const u8, val: Value) !void {
        const outcome = migrateTable(&self.st, a, table, val) catch |err| switch (err) {
            error.SchemaUnusable => {
                // Suspended (no primary key, or unrouted — §5) or malformed: the
                // table stays as it is locally, and stays out of `states` if it was
                // never usable, so no CDC event is applied against a missing table.
                const why = if (val == .object) (val.object.get("suspended") orelse Value{ .null = {} }) else Value{ .null = {} };
                std.debug.print("{s}: schema unusable ({s}) — skipped\n", .{ table, if (why == .string) why.string else "no columns" });
                // Dropped upstream: whatever was held for it will never find a parent.
                const dropped = if (val == .object) (val.object.get("dropped") orelse Value{ .null = {} }) else Value{ .null = {} };
                if (dropped == .bool and dropped.bool) try self.dropLocalTable(a, table);
                return;
            },
            else => return err,
        };
        // One line per event, not two: the branch below says everything the short
        // form says and then what follows from it, so the short form is for the
        // outcomes that branch does not cover (an ALTER that kept the rows).
        const watermark_dropped = outcome == .rekeyed or outcome == .emptied or outcome == .created;
        if (outcome != .unchanged and !watermark_dropped) std.debug.print("{s}: {s}\n", .{ table, @tagName(outcome) });
        if (watermark_dropped) {
            // §10dg: the rows are gone (with the old key, or because the rebuild could
            // not carry them); so is everything that referred to them — the watermark
            // (the next gap check seeds a fresh full) and the events held for the
            // table (keyed the old way, they can never apply). `.created` too (§10dj):
            // a table that did not exist cannot be seeded, whatever a watermark left
            // behind by a kill says.
            // `catch`: on the very first sync the bookkeeping tables are created AFTER
            // this loop, and a table with no bookkeeping has no watermark to drop.
            _ = self.st.query(a, "DELETE FROM _zbz_generations WHERE tbl = ?", &.{.{ .text = table }}) catch {};
            pruneInboxDropped(&self.st, a, table) catch {};
            if (outcome != .created) discardOutbox(&self.st, a, table, @tagName(outcome)) catch {};
            // §10hj: an ON-DEMAND table is never seeded from a chain — both loops in
            // `gapAndSeed` skip it — so promising a fresh full here describes work that
            // will never happen, and it was read as a fault. Say what is true instead,
            // and leave `reseed_pending` alone: a pass raised for a table that cannot
            // seed is a wasted pass, and every table that DOES need one raises it.
            if (self.isOnDemand(table)) {
                std.debug.print("{s}: {s} — on demand, so no chain seed; rows arrive as query answers\n", .{ table, @tagName(outcome) });
            } else {
                self.reseed_pending = true;
                std.debug.print("{s}: {s} — watermark dropped, re-seeding from a fresh full\n", .{ table, @tagName(outcome) });
            }
        }

        const pk = try jsonStrList(a, val.object.get("pk_columns"));
        const cols_v = (try descriptorColumns(a, val, self.st.engine)).?; // checked by migrateTable above
        var names: std.ArrayList([]const u8) = .empty;
        for (cols_v.array.items) |c| try names.append(a, c.object.get("name").?.string);
        const tenant_col: ?[]const u8 = if (val.object.get("tenant_column")) |v| (if (v == .string) v.string else null) else null;
        const version_col: ?[]const u8 = if (val.object.get("version_column")) |v| (if (v == .string) v.string else null) else null;
        const tombstone_col: ?[]const u8 = if (val.object.get("tombstone_column")) |v| (if (v == .string) v.string else null) else null;
        const seed_epoch: i64 = if (val.object.get("seed_epoch")) |v| (if (v == .integer) v.integer else 0) else 0;

        if (self.states.getPtr(table)) |st| {
            // §10df first: a republished descriptor is usually UNCHANGED in shape — the
            // epoch is the whole message, and it must not be lost to the shortcut below.
            st.seed_epoch = seed_epoch;
            try self.reseedIfEpochMoved(a, table, seed_epoch);
            if (outcome == .unchanged and sameStrings(st.cols, names.items) and sameStrings(st.pk, pk)) return;
        }
        if (tenant_col != null and self.tenant_missing) {
            std.debug.print("{s}: tenant-scoped and '{s}' has no tenant — not followed (the local rows stay as they are)\n", .{ table, self.opts.principal });
            return;
        }
        // Changed, or first time: into the client-lifetime arena.
        const ca = self.aa();
        // The OPEN tenant's rows ride the public stream (`cdc.<open>.>` is one of its
        // subjects); a principal mapped to it must not look for a `CDC_<open>` stream.
        const routes: []const []const u8 = if (self.isOnDemand(table)) &.{} else try self.routesFor(tenant_col);
        const shared_route: ?[]const u8 = if (tenant_col != null) self.cdc_public else null;
        const fresh: TableState = .{
            .pk = try dupeStrings(ca, pk),
            .cols = try dupeStrings(ca, names.items),
            .version_col = if (version_col) |v| try ca.dupe(u8, v) else null,
            .tenant_col = if (tenant_col) |v| try ca.dupe(u8, v) else null,
            .tombstone_col = if (tombstone_col) |v| try ca.dupe(u8, v) else null,
            .array_cols = try dupeStrings(ca, try arrayColsOf(a, val)),
            .geom_cols = try dupeStrings(ca, try geomColsOf(a, val)),
            .vec_cols = try dupeVecCols(ca, try vecColsOf(a, val)),
            .routes = routes,
            .shared_route = shared_route,
            .seed_epoch = seed_epoch,
        };
        if (self.states.getPtr(table)) |st| {
            // In place: the seed gate (`anchors`) belongs to the replica's history,
            // not to the descriptor, and survives a migration.
            st.pk = fresh.pk;
            st.cols = fresh.cols;
            st.version_col = fresh.version_col;
            st.tenant_col = fresh.tenant_col;
            st.tombstone_col = fresh.tombstone_col;
            st.array_cols = fresh.array_cols;
            st.geom_cols = fresh.geom_cols;
            st.vec_cols = fresh.vec_cols;
            st.routes = fresh.routes;
            st.shared_route = fresh.shared_route;
            st.seed_epoch = fresh.seed_epoch;
        } else {
            try self.states.put(ca, try ca.dupe(u8, table), fresh);
            // §10jc: the anchors of the chains this replica already applied, back from the
            // replica — a relaunch after a re-seed keeps its seed gate.
            try self.loadAnchors(a, table);
            // Born under the live watch (enabled after this client connected): it has
            // no watermark and nothing asked for its seed — the next poll does.
            self.reseed_pending = true;
        }
    }

    fn loadAnchors(self: *SyncClient, a: std.mem.Allocator, table: []const u8) !void {
        const st = self.states.getPtr(table) orelse return;
        const rows = self.st.query(a, "SELECT anchor_stream, anchor_seq, cutoff_lsn FROM _zbz_generations WHERE tbl = ? AND anchor_seq IS NOT NULL AND anchor_seq > 0", &.{.{ .text = table }}) catch return;
        for (rows) |r| {
            if (r.len < 3 or r[0] != .text or r[1] != .integer) continue;
            const lsn: i64 = if (r[2] == .integer) r[2].integer else 0;
            var replaced = false;
            for (st.anchors.items) |*an| if (std.mem.eql(u8, an.stream, r[0].text)) {
                if (@as(i64, @intCast(an.seq)) < r[1].integer) an.* = .{ .stream = an.stream, .seq = @intCast(r[1].integer), .lsn = lsn };
                replaced = true;
            };
            if (!replaced) try st.anchors.append(self.aa(), .{ .stream = try self.aa().dupe(u8, r[0].text), .seq = @intCast(r[1].integer), .lsn = lsn });
        }
    }

    /// §10df: the descriptor's seed_epoch is above the one this replica seeded at —
    /// zebridge_reseed() ran upstream. Forget the watermark; `gapAndSeed` (next sync,
    /// or the end of this poll's schema drain) seeds a fresh full.
    fn reseedIfEpochMoved(self: *SyncClient, a: std.mem.Allocator, table: []const u8, epoch: i64) !void {
        const rows = self.st.query(a, "SELECT seed_epoch FROM _zbz_generations WHERE tbl = ? ORDER BY seed_epoch LIMIT 1", &.{.{ .text = table }}) catch return;
        if (rows.len == 0) {
            // Never seeded (no chain at connect, or one the replica could not use):
            // nothing to drop, but the next poll must ask again — measured: a table
            // following CDC unseeded stayed that way through an epoch move.
            self.reseed_pending = true;
            return;
        }
        const stored: i64 = if (rows[0][0] == .integer) rows[0][0].integer else 0;
        if (stored >= epoch) {
            if (stored > epoch) std.debug.print("{s}: descriptor carries seed epoch {d}, the replica was seeded under {d} — nothing to do\n", .{ table, epoch, stored });
            return;
        }
        _ = try self.st.query(a, "DELETE FROM _zbz_generations WHERE tbl = ?", &.{.{ .text = table }});
        std.debug.print("{s}: seed epoch {d} -> {d} (zebridge_reseed) — watermark dropped, re-seeding from a fresh full\n", .{ table, stored, epoch });
        self.reseed_pending = true;
    }

    /// The table is gone upstream: drop it here, forget its state, discard what was
    /// held for it (the TS `dropLocalTable`). Stale rows must not stay readable as
    /// if they were live.
    fn dropLocalTable(self: *SyncClient, a: std.mem.Allocator, table: []const u8) !void {
        pruneInboxDropped(&self.st, a, table) catch {};
        discardOutbox(&self.st, a, table, "dropped") catch {};
        try execSql(&self.st, a, try std.fmt.allocPrint(a, "DROP VIEW IF EXISTS \"{s}_view\";", .{table}));
        try execSql(&self.st, a, try std.fmt.allocPrint(a, "DROP TABLE IF EXISTS \"{s}\";", .{table}));
        _ = self.states.orderedRemove(table);
        std.debug.print("{s}: dropped locally — the table was dropped upstream\n", .{table});
    }

    /// §10hn: `tables: "*"` — the followed set is the schemas bucket's key list, plus
    /// the on-demand names. Only NEW keys are added, into the client-lifetime arena, so a
    /// sync with nothing new allocates nothing that outlives it.
    fn refreshFollowed(self: *SyncClient, a: std.mem.Allocator) !void {
        const keys = self.t.kvKeys(a, self.kv_schemas) catch |err| {
            std.debug.print("schemas: keys not listed ({s}) — following what is known\n", .{@errorName(err)});
            return;
        };
        for (keys) |k| try self.follow(k);
    }

    fn follows(self: *SyncClient, table: []const u8) bool {
        for (self.followed) |t| if (std.mem.eql(u8, t, table)) return true;
        return false;
    }

    /// Add one table to the followed set (no-op when present), arena-owned.
    fn follow(self: *SyncClient, table: []const u8) !void {
        if (table.len == 0 or self.follows(table)) return;
        const ca = self.aa();
        const grown = try ca.alloc([]const u8, self.followed.len + 1);
        @memcpy(grown[0..self.followed.len], self.followed);
        grown[self.followed.len] = try ca.dupe(u8, table);
        self.followed = grown;
    }

    /// Drain the schemas watch: every descriptor that changed since the last poll,
    /// applied through the same path `sync()` uses. Non-blocking (1 ms). Opens the
    /// watch on first use — after the grammar named the bucket.
    fn drainSchemaWatch(self: *SyncClient, report_a: std.mem.Allocator, changed_map: *std.StringArrayHashMapUnmanaged(void), seeded_map: *std.StringArrayHashMapUnmanaged(void)) !void {
        if (self.schema_watch == null) {
            self.schema_kv = try self.t.js.kvBucket(self.kv_schemas);
            self.schema_watch = try self.schema_kv.?.watchAll(.{ .updates_only = true });
        }
        var sa = std.heap.ArenaAllocator.init(self.a);
        defer sa.deinit();
        const a = sa.allocator();
        var moved: usize = 0;
        const t: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } };
        while (true) {
            var entry = (self.schema_watch.?.next(t) catch |err| switch (err) {
                error.Timeout => break,
                else => return err,
            }) orelse break;
            defer entry.deinit();
            // §10hn: with `follow_all` a key never seen before joins the set right here.
            if (self.opts.follow_all and !entry.isDeleted()) try self.follow(entry.key);
            if (!self.follows(entry.key) or entry.isDeleted()) continue;
            const val = std.json.parseFromSliceLeaky(Value, a, entry.value, .{}) catch continue;
            const key = try a.dupe(u8, entry.key);
            self.applyDescriptor(a, key, val) catch |err| {
                std.debug.print("{s}: live schema not applied: {s}\n", .{ key, @errorName(err) });
                continue;
            };
            moved += 1;
        }
        if (moved > 0) self.retryHeld(report_a, changed_map);
        if (self.reseed_pending) {
            self.reseed_pending = false;
            self.gapAndSeed(report_a, seeded_map) catch |err| std.debug.print("re-seed after epoch move: {s} — {s}\n", .{ @errorName(err), self.st.errMsg() });
        }
    }

    /// DDL steps from core carry no params (unlike the DML plans `stepExec` runs).
    fn execStep(self: *SyncClient, stp: Value) !void {
        var qa = std.heap.ArenaAllocator.init(self.a);
        defer qa.deinit();
        _ = try self.st.query(qa.allocator(), stp.object.get("sql").?.string, &.{});
    }

    // ─── positions ──────────────────────────────────────────────────────────

    /// ⚠️ A read failure is an ERROR, not 0. Answering 0 to "where was I" tells the
    /// gap rule the replica has never been here, which is a full re-seed — the most
    /// expensive thing this client can do, triggered by a locked database file.
    fn storedSeq(self: *SyncClient, stream: []const u8) !u64 {
        var qa = std.heap.ArenaAllocator.init(self.a);
        defer qa.deinit();
        const rows = try self.st.query(qa.allocator(), "SELECT last_seq FROM _zbz_stream_seq WHERE stream = ?", &.{.{ .text = stream }});
        if (rows.len == 0) return 0;
        return @intCast(rows[0][0].integer);
    }

    /// The `created` timestamp of the stream this position was taken on, or null for a
    /// stream never seen (or a replica from before the column).
    fn storedCreated(self: *SyncClient, a: std.mem.Allocator, stream: []const u8) !?[]const u8 {
        const rows = try self.st.query(a, "SELECT created FROM _zbz_stream_seq WHERE stream = ?", &.{.{ .text = stream }});
        if (rows.len == 0 or rows[0][0] != .text) return null;
        return rows[0][0].text;
    }

    fn persistCreated(self: *SyncClient, stream: []const u8, created: []const u8) !void {
        var qa = std.heap.ArenaAllocator.init(self.a);
        defer qa.deinit();
        _ = try self.st.query(qa.allocator(), "INSERT INTO _zbz_stream_seq (stream, last_seq, created) VALUES (?, 0, ?) ON CONFLICT(stream) DO UPDATE SET created = excluded.created", &.{ .{ .text = stream }, .{ .text = created } });
    }

    fn persistSeq(self: *SyncClient, stream: []const u8, seq: u64) !void {
        var qa = std.heap.ArenaAllocator.init(self.a);
        defer qa.deinit();
        _ = try self.st.query(qa.allocator(), "INSERT INTO _zbz_stream_seq (stream, last_seq) VALUES (?, ?) ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq", &.{ .{ .text = stream }, .{ .integer = @intCast(seq) } });
    }

    /// §10jc: the stream's `Flow`.
    fn flowFor(self: *SyncClient, stream: []const u8) !*Flow {
        if (self.flows.getPtr(stream)) |f| return f;
        try self.flows.put(self.a, try self.aa().dupe(u8, stream), .{});
        return self.flows.getPtr(stream).?;
    }

    /// §10jc: before any apply — redeliveries acked and dropped; at a lost delivery the
    /// batch stops (the rest neither applied nor acked). `consumer` is the one just fetched
    /// from: a message carrying another name is a straggler of a consumer dropped after a
    /// gap (the shared inbox still held it) — left unacked too; the new consumer re-sends
    /// it in order.
    fn admit(self: *SyncClient, a: std.mem.Allocator, stream: []const u8, consumer: []const u8, msgs: []const *@import("nats").JetStreamMessage) ![]const *@import("nats").JetStreamMessage {
        const f = try self.flowFor(stream);
        if (!std.mem.eql(u8, f.consumer, consumer)) {
            f.consumer = try self.aa().dupe(u8, consumer);
            f.cseq = 0; // a new consumer numbers its deliveries from 1
            f.gap = false;
        }
        const pos = try self.storedSeq(stream);
        var keep: std.ArrayListUnmanaged(*@import("nats").JetStreamMessage) = .empty;
        var dups: usize = 0;
        var stragglers: usize = 0;
        for (msgs) |m| {
            if (f.gap) break;
            if (test_drop_every > 0) {
                test_drop_count += 1;
                if (test_drop_count % test_drop_every == 0) {
                    std.debug.print("test: delivery of seq {d} (consumer seq {d}) discarded as lost in transit\n", .{ m.metadata.sequence.stream, m.metadata.sequence.consumer });
                    continue;
                }
            }
            if (!std.mem.eql(u8, m.metadata.consumer, consumer)) {
                stragglers += 1;
                continue;
            }
            const c = m.metadata.sequence.consumer;
            if (c > f.cseq + 1) {
                f.gap = true;
                std.debug.print("{s}: delivery lost in transit on {s} (consumer seq {d} after {d}) — applying up to the gap, then recreating the consumer from the position\n", .{ stream, consumer, c, f.cseq });
                break;
            }
            if (c > f.cseq) f.cseq = c;
            if (m.metadata.sequence.stream <= pos) {
                m.ack() catch {};
                dups += 1;
                continue;
            }
            try keep.append(a, m);
        }
        if (dups > 0 or stragglers > 0) tr("{s}: {d} redelivery(ies) acked, not applied; {d} straggler(s) of a dropped consumer left (position {d})", .{ stream, dups, stragglers, pos });
        return keep.items;
    }

    /// Inside an apply's transaction: the position follows the batch (in order, §10jc).
    fn positionAfter(self: *SyncClient, stream: []const u8, last: u64, max_seq: u64, messages: []const *@import("nats").JetStreamMessage) !void {
        _ = messages;
        if (max_seq > last) try self.persistSeq(stream, max_seq);
    }

    /// §10jc: a gap closes by recreating the consumer from the position: forget the old
    /// consumer's numbering. The caller opens the new one.
    fn resetFlow(self: *SyncClient, stream: []const u8) void {
        if (self.flows.getPtr(stream)) |f| {
            f.gap = false;
            f.cseq = 0;
            f.consumer = "";
        }
    }

    // ─── step 2: the gap rule (per stream) + scoped seeding (§10n) ──────────

    pub fn gapAndSeed(self: *SyncClient, report_a: ?std.mem.Allocator, seeded_map: ?*std.StringArrayHashMapUnmanaged(void)) !void {
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const a = ca.allocator();
        // Gapped stream → its `first_seq`: the resume point once the gap is healed.
        var gapped: std.StringArrayHashMapUnmanaged(i64) = .empty;
        // Every stream any table depends on — a tenant-scoped table names two (§10bq),
        // and a client with no public table would otherwise never inspect CDC_PUBLIC at
        // all, so its shared rows could fall off the back unnoticed.
        var streams: std.StringArrayHashMapUnmanaged(void) = .empty;
        var sit = self.states.iterator();
        while (sit.next()) |e| {
            for (e.value_ptr.routes) |r| try streams.put(a, r, {});
            if (e.value_ptr.shared_route) |sr| try streams.put(a, sr, {});
        }
        var it = streams.iterator();
        while (it.next()) |e| {
            const stream = e.key_ptr.*;
            if (gapped.contains(stream)) continue;
            if (self.isDark(stream)) continue;
            // §10fq: an unreadable stream costs a request timeout on every pass; set
            // it aside instead, so an unseeded tenant behind it does not slow each poll.
            var info = self.t.js.getStreamInfo(stream) catch |err| {
                self.markDark(stream, err);
                continue;
            };
            defer info.deinit();
            const first: i64 = @intCast(info.value.state.first_seq);
            const last: i64 = @intCast(info.value.state.last_seq);
            const stored: i64 = @intCast(try self.storedSeq(stream));
            // The stream's identity: a different `created` under the same name is a
            // stream recreated (a wipe, a lost slot) — a restart whatever the numbers say.
            // Measured (stream_wipe.py, 2026-09-17): position 1, the recreated stream
            // already at last_seq 2, `stored > last_seq` never fired, and the client read
            // on from 2 — one row of the new numbering skipped for ever.
            const created: []const u8 = info.value.created;
            const known_created = try self.storedCreated(a, stream);
            const recreated = if (known_created) |kc| (created.len > 0 and !std.mem.eql(u8, kc, created)) else false;
            if (core.streamHasGap(first, stored, last) or recreated) {
                try gapped.put(a, stream, first);
                // The feed restarted under us (position beyond last_seq): the position is
                // meaningless in the new numbering. Reset it, or `fullPredates` reads the
                // fresh chain's small cutoff_seq as "older than where I am" and skips the
                // very full this gap needs (measured: slot_loss.py, 2026-08-29).
                if (recreated or (last >= 0 and stored > last)) {
                    if (recreated) {
                        std.debug.print("{s}: stream recreated (created {s}, was {s}) — position {d} reset\n", .{ stream, created, known_created.?, stored });
                    } else {
                        std.debug.print("{s}: stream restarted (position {d} beyond last_seq {d}) — position reset\n", .{ stream, stored, last });
                    }
                    try self.persistSeq(stream, 0);
                    if (!self.restarted.contains(stream)) try self.restarted.put(self.aa(), try self.aa().dupe(u8, stream), {});
                    // The tail's consumer died with the old stream; `tailFor` would keep
                    // handing back the cached, dead one and every fetch would answer
                    // nothing. Drop it — the next poll opens one from the reset position.
                    self.dropTail(stream);
                }
            }
            if (created.len > 0 and (known_created == null or recreated)) try self.persistCreated(stream, created);
        }
        // Seed with foreign_keys OFF — the standard bulk-load shape, and the same
        // one the TS client uses (dialect.deferForeignKeys + foreignKeyViolations).
        // Order alone cannot save this: chains are per-table snapshots built at
        // DIFFERENT cutoffs, so a child's chain can reference a parent born after
        // the parent's chain was cut — a fresh client seeding an orders chain died
        // at row 1 on the FK, rolling back the whole step (§10cp finding 2). A seed
        // is a bulk load of almost-consistent snapshots; the tail reconciles the
        // difference, and the post-load check makes any residue LOUD instead of
        // fatal. The pragma sits OUTSIDE the per-step transactions because SQLite
        // silently ignores it inside one.
        try execSql(&self.st, a, "PRAGMA foreign_keys = OFF;");
        defer execSql(&self.st, a, "PRAGMA foreign_keys = ON;") catch {};
        // Still the CONFIGURED order (parents first — cheapest path to zero
        // residue), scoped to gapped routes plus never-seeded tables (§10n).
        // §10fq: which gapped streams this pass may heal. A tenant's own stream heals
        // only when that (table, tenant) pair seeded; the shared stream a tenant-scoped
        // table's open rows ride heals when ANY of the table's pairs holds a chain past
        // the gap — every chain carries the open rows. The heal used to wait for the
        // whole pass (`reseed_pending`), so one tenant with no chain yet kept the shared
        // stream "gapped" for ever, and every sibling re-seeded on every poll.
        var blocked: std.StringArrayHashMapUnmanaged(void) = .empty;
        for (self.followed) |table| {
            if (self.isOnDemand(table)) continue; // §10hj: never seeded from a chain
            const st = self.states.get(table) orelse continue;
            const shared_gapped = if (st.shared_route) |sr| gapped.contains(sr) else false;
            var covered = false;
            // §10fn: one chain per tenant of the table, each judged on its own stream.
            for (self.tenantsFor(st)) |tenant| {
                const route = try self.routeFor(a, tenant);
                if (self.dark.contains(route)) continue; // back when readable (clearDark re-asks)
                // ⚠️ `try`, not "treat a failed read as never seeded": that would answer a
                // locked database with a full re-seed (the same trap as `storedSeq`).
                const seeded = (try self.st.query(a, "SELECT tbl FROM _zbz_generations WHERE tbl = ? AND tenant = ?", &.{ .{ .text = table }, .{ .text = tenant } })).len > 0;
                if (!(gapped.contains(route) or shared_gapped or !seeded)) {
                    covered = true;
                    continue;
                }
                // One table's failure is one table's failure (the TS rule): the rest
                // still seed, and the next poll's gap check retries this one. A chain
                // that is not there yet, or cannot splice, leaves `reseed_pending` set.
                const pending_before = self.reseed_pending;
                self.reseed_pending = false;
                const ok = if (self.applyChain(table, tenant)) |_| !self.reseed_pending else |err| blk: {
                    std.debug.print("{s}: seeding failed: {s} — retried at the next poll\n", .{ table, @errorName(err) });
                    self.rememberUnseeded(table, @errorName(err));
                    break :blk false;
                };
                self.reseed_pending = pending_before or self.reseed_pending or !ok;
                const has_chain = (try self.st.query(a, "SELECT tbl FROM _zbz_generations WHERE tbl = ? AND tenant = ?", &.{ .{ .text = table }, .{ .text = tenant } })).len > 0;
                if (ok and has_chain) {
                    _ = self.unseeded.swapRemove(table);
                    covered = true;
                    if (report_a) |ra_| if (seeded_map) |sm| {
                        if (!sm.contains(table)) sm.put(ra_, ra_.dupe(u8, table) catch table, {}) catch {};
                    };
                } else {
                    try blocked.put(a, route, {});
                }
            }
            if (!covered) if (st.shared_route) |sr| try blocked.put(a, sr, {});
        }
        // §10dg: whatever kept a table unseeded — no chain yet, a chain that predates
        // the replica or the re-seed, a shape the replica lacks, a failed step — the
        // next poll asks again. Unseeded is not synced (the TS rule), and a poll loop
        // that never re-asks would follow CDC on an empty table forever.
        outer: for (self.followed) |table| {
            if (self.isOnDemand(table)) continue; // §10hj: unseeded by design, not a gap
            const st = self.states.get(table) orelse continue;
            for (self.tenantsFor(st)) |tenant| {
                if (self.dark.contains(try self.routeFor(a, tenant))) continue;
                const seeded_now = (self.st.query(a, "SELECT tbl FROM _zbz_generations WHERE tbl = ? AND tenant = ?", &.{ .{ .text = table }, .{ .text = tenant } }) catch continue).len > 0;
                if (!seeded_now) {
                    self.reseed_pending = true;
                    break :outer;
                }
            }
        }
        // §10ei: a gap healed by this pass resumes at the stream's OLDEST message.
        // Every table routed to the gapped stream was just seeded to a cutoff at or
        // past `first_seq - 1` (the predates-the-stream guard), so nothing between
        // is owed and the seed gate drops what the chain carried. Left below
        // `first_seq`, the tail would ask for a sequence the stream no longer holds,
        // the server would continue from its oldest, and the live gap rule would read
        // that as a fresh hole — a second seed at every poll after every gap.
        {
            var git = gapped.iterator();
            while (git.next()) |ge| {
                if (blocked.contains(ge.key_ptr.*)) continue;
                const first = ge.value_ptr.*;
                const stored: i64 = @intCast(try self.storedSeq(ge.key_ptr.*));
                if (first > 1 and stored < first - 1) {
                    // A first run (stored 0) resumes there too, silently: it is not a healed gap.
                    if (stored > 0) std.debug.print("{s}: gap healed — resuming at the stream's oldest message ({d}; was {d})\n", .{ ge.key_ptr.*, first - 1, stored });
                    try self.persistSeq(ge.key_ptr.*, @intCast(first - 1));
                }
            }
        }
        const orphans = try self.st.query(a, "PRAGMA foreign_key_check;", &.{});
        if (orphans.len > 0) {
            std.debug.print(
                "⚠️ {d} foreign key violation(s) survive seeding — cross-chain skew; the tail reconciles or holds them\n",
                .{orphans.len},
            );
        }
    }

    /// One chain step's rows, applied inside `Storage.transaction` — the DELETE of a
    /// full shares the transaction so a crash mid-apply cannot leave an empty table
    /// (§10n). A struct because `transaction` takes `(ctx, fn)`, and the fn needs
    /// everything the step decoded.
    /// §10fa: one chain step applied from the msgpack document itself — no value
    /// tree. `offsets[order[i]]` is where row i starts in `bytes`; a chunk decodes
    /// its rows one at a time into a scratch arena, binds straight from the payload,
    /// and commits. The first chunk of a full runs the DELETE FROM.
    const ChainStep = struct {
        client: *SyncClient,
        a: std.mem.Allocator,
        table: []const u8,
        sql: []const u8,
        bytes: []const u8,
        offsets: []const usize,
        order: []const usize,
        from: usize,
        to: usize,
        is_full: bool,
        first_chunk: bool,
        cols: []const []const u8,
        pk: []const []const u8,
        tomb_idx: ?usize,
        /// §10fd: the array columns' indices, for a PostgreSQL replica's literal.
        array_idx: []const usize,
        /// §10fe: the PostGIS columns' indices (bare EWKB hex in COPY text).
        geom_idx: []const usize,
        /// §10fg: the pgvector/bit columns' indices and shapes (text form on PostgreSQL).
        vec_idx: []const VecIdx,
        /// §10fe: on PostgreSQL, a chunk goes in through COPY FROM STDIN — straight into
        /// the emptied table for a full, through a temporary table and the upsert for
        /// a delta, so the version guard holds. Tombstones keep their per-row DELETE.
        copy: bool,
        /// §10fe: the delta's upsert from the COPY's temporary table (core.pgUpsertFromCopySql).
        copy_upsert_sql: []const u8,
        /// §10fe: the step's COPY text buffer, reused across its chunks.
        copy_buf: *std.ArrayListUnmanaged(u8),
        /// §10fn: what a full clears before its first chunk — the whole table, or,
        /// for a tenant-scoped table under several memberships, only this tenant's
        /// rows (`wipe_arg`), the rest going in as upserts (`wipe_whole` false).
        wipe_sql: []const u8,
        wipe_arg: ?[]const u8,
        wipe_whole: bool,

        fn applyCopy(cs: ChainStep, st: *storage.Storage, row_arena: *std.heap.ArenaAllocator) !void {
            return applyCopyStep(cs, st, row_arena);
        }

        fn apply(cs: ChainStep, st: *storage.Storage) !void {
            if (cs.is_full and cs.first_chunk) {
                if (cs.wipe_arg) |arg| {
                    _ = try st.query(cs.a, cs.wipe_sql, &.{.{ .text = arg }});
                } else {
                    _ = try st.query(cs.a, cs.wipe_sql, &.{});
                }
            }
            var row_arena = std.heap.ArenaAllocator.init(cs.client.a);
            defer row_arena.deinit();
            if (cs.copy) return cs.applyCopy(st, &row_arena);
            for (cs.order[cs.from..cs.to]) |ri| {
                _ = row_arena.reset(.retain_capacity);
                const ra = row_arena.allocator();
                var cur = MpCursor{ .bytes = cs.bytes, .off = cs.offsets[ri] };
                const row = try cur.readValue(ra);
                if (row != .arr) return error.ChainObjectMalformed;
                const cells = row.arr;
                // §7.5: a tombstoned row in a chain is a row this replica must NOT hold.
                // A chain is built from the table as it stands, so it carries every
                // tombstone not yet reaped; the reap itself never reaches a client.
                if (cs.tomb_idx) |ti| if (ti < cells.len and cells[ti] != .nil) {
                    var keyed: std.json.ObjectMap = .empty;
                    for (cs.pk) |pc| {
                        for (cs.cols, 0..) |c, i| if (std.mem.eql(u8, c, pc) and i < cells.len) {
                            try keyed.put(ra, pc, try mpToJson(ra, cells[i]));
                        };
                    }
                    if (try core.planDelete(ra, cs.table, cs.pk, .{ .object = keyed })) |stp| {
                        _ = try cs.client.stepExec(ra, stp);
                    }
                    continue;
                };
                const params = try ra.alloc(storage.Value, cells.len);
                for (cells, 0..) |cell, i| params[i] = try payloadToStorage(ra, cell);
                for (cs.array_idx) |i| if (i < params.len and params[i] == .text) {
                    params[i] = .{ .text = try jsonArrayToLiteral(ra, params[i].text) };
                };
                for (cs.vec_idx) |vi| if (vi.i < params.len and params[vi.i] == .blob) {
                    params[vi.i] = .{ .text = try core.vecLiteral(ra, vi.kind, params[vi.i].blob, vi.bits) };
                };
                _ = st.query(ra, cs.sql, params) catch |err| {
                    // Read the SQLite text HERE, before the rollback clears it: it is
                    // what tells an FK refusal from a bad column apart.
                    std.debug.print("{s}: row {d} of {d} refused: {any} — sqlite: {s}\n", .{
                        cs.table, ri + 1, cs.offsets.len, err, st.errMsg(),
                    });
                    return err;
                };
            }
        }
    };

    /// §10fe: the chunk as COPY text — one line per live row, tab-separated, `\\N` for
    /// null, backslash, tab, newline and return escaped — into the table itself for
    /// a full, into a temporary table then the upsert for a delta.
    /// §10fl: DuckDB's bulk path — the appender, through a temp table of the chain's
    /// column order, then one INSERT … SELECT (a full) or the upsert (a delta). The
    /// appender wants rows in the target's column order, and `_zbz_copy` is created
    /// in the chain's, so no mapping; it casts text into lists, arrays, timestamps
    /// and decimals itself (measured). Tombstones keep their per-row DELETE.
    fn applyAppendStep(cs: ChainStep, st: *storage.Storage, row_arena: *std.heap.ArenaAllocator) !void {
        var chunk_arena = std.heap.ArenaAllocator.init(cs.client.a);
        defer chunk_arena.deinit();
        const ca = chunk_arena.allocator();
        var rows: std.ArrayListUnmanaged([]const storage.Value) = .empty;
        for (cs.order[cs.from..cs.to]) |ri| {
            _ = row_arena.reset(.retain_capacity);
            const ra = row_arena.allocator();
            var cur = MpCursor{ .bytes = cs.bytes, .off = cs.offsets[ri] };
            const row = try cur.readValue(ra);
            if (row != .arr) return error.ChainObjectMalformed;
            const cells = row.arr;
            if (cs.tomb_idx) |ti| if (ti < cells.len and cells[ti] != .nil) {
                var keyed: std.json.ObjectMap = .empty;
                for (cs.pk) |pc| {
                    for (cs.cols, 0..) |c, i| if (std.mem.eql(u8, c, pc) and i < cells.len) {
                        try keyed.put(ra, pc, try mpToJson(ra, cells[i]));
                    };
                }
                if (try core.planDelete(ra, cs.table, cs.pk, .{ .object = keyed })) |stp| {
                    _ = try cs.client.stepExec(ra, stp);
                }
                continue;
            };
            // ⚠️ Copied into the chunk arena: a text or blob value from the payload is a
            // slice into the row arena, which the next row resets (measured: a chain
            // row's JSON array reached the appender truncated).
            const params = try ca.alloc(storage.Value, cs.cols.len);
            for (params, 0..) |*p, i| {
                const v: storage.Value = if (i < cells.len) try payloadToStorage(ra, cells[i]) else .null;
                p.* = switch (v) {
                    .text => |t| .{ .text = try ca.dupe(u8, t) },
                    .blob => |b| .{ .blob = try ca.dupe(u8, b) },
                    else => v,
                };
            }
            for (cs.vec_idx) |vi| if (vi.i < params.len and params[vi.i] == .blob) {
                params[vi.i] = .{ .text = try core.vecLiteral(ca, vi.kind, params[vi.i].blob, vi.bits) };
            };
            try rows.append(ca, params);
        }
        if (rows.items.len == 0) return;
        const col_list = try core.quotedJoin(cs.a, cs.cols);
        try st.execSimple(try std.fmt.allocPrint(ca, "CREATE OR REPLACE TEMP TABLE _zbz_copy AS SELECT {s} FROM {s} LIMIT 0", .{ col_list, cs.table }));
        try st.dkAppend("_zbz_copy", rows.items);
        if (cs.is_full and cs.wipe_whole) {
            try st.execSimple(try std.fmt.allocPrint(ca, "INSERT INTO {s} ({s}) SELECT {s} FROM _zbz_copy", .{ cs.table, col_list, col_list }));
        } else {
            try st.execSimple(cs.copy_upsert_sql);
        }
        try st.execSimple("DROP TABLE _zbz_copy");
    }

    fn applyCopyStep(cs: ChainStep, st: *storage.Storage, row_arena: *std.heap.ArenaAllocator) !void {
        if (st.engine == .duckdb) return applyAppendStep(cs, st, row_arena);
        // One text buffer for the whole step, reused chunk after chunk: a buffer built
        // fresh per chunk by doubling left every freed size behind in the allocator's
        // cache — measured 590 MB over the document at 50,000 rows a chunk, 43 MB at
        // 10,000 — until the step ended. Retained capacity, one block.
        const buf = cs.copy_buf;
        buf.clearRetainingCapacity();
        var n_live: usize = 0;
        for (cs.order[cs.from..cs.to]) |ri| {
            _ = row_arena.reset(.retain_capacity);
            const ra = row_arena.allocator();
            var cur = MpCursor{ .bytes = cs.bytes, .off = cs.offsets[ri] };
            const row = try cur.readValue(ra);
            if (row != .arr) return error.ChainObjectMalformed;
            const cells = row.arr;
            if (cs.tomb_idx) |ti| if (ti < cells.len and cells[ti] != .nil) {
                var keyed: std.json.ObjectMap = .empty;
                for (cs.pk) |pc| {
                    for (cs.cols, 0..) |c, i| if (std.mem.eql(u8, c, pc) and i < cells.len) {
                        try keyed.put(ra, pc, try mpToJson(ra, cells[i]));
                    };
                }
                if (try core.planDelete(ra, cs.table, cs.pk, .{ .object = keyed })) |stp| {
                    _ = try cs.client.stepExec(ra, stp);
                }
                continue;
            };
            for (cells, 0..) |cell, i| {
                if (i > 0) try buf.append(cs.client.a, '\t');
                const is_array = for (cs.array_idx) |ai| {
                    if (ai == i) break true;
                } else false;
                const is_geom = for (cs.geom_idx) |gi| {
                    if (gi == i) break true;
                } else false;
                const vec: ?VecIdx = for (cs.vec_idx) |vi| {
                    if (vi.i == i) break vi;
                } else null;
                try writeCopyCell(buf, cs.client.a, ra, cell, is_array, is_geom, vec);
            }
            try buf.append(cs.client.a, '\n');
            n_live += 1;
        }
        if (n_live == 0) return;
        const col_list = try core.quotedJoin(cs.a, cs.cols);
        if (cs.is_full and cs.wipe_whole) {
            const copy_sql = try std.fmt.allocPrintSentinel(cs.a, "COPY {s} ({s}) FROM STDIN", .{ cs.table, col_list }, 0);
            try st.pgCopy(copy_sql, buf.items);
            return;
        }
        // A delta: through a temporary table, then the upsert's own conflict clause.
        try st.execSimple(try std.fmt.allocPrint(cs.a, "CREATE TEMP TABLE _zbz_copy (LIKE {s}) ON COMMIT DROP", .{cs.table}));
        const copy_sql = try std.fmt.allocPrintSentinel(cs.a, "COPY _zbz_copy ({s}) FROM STDIN", .{col_list}, 0);
        try st.pgCopy(copy_sql, buf.items);
        try st.execSimple(cs.copy_upsert_sql);
    }

    /// One cell in COPY text. Bytes are bytea's `\\x` hex, or bare EWKB hex into a
    /// PostGIS column; a JSON array text becomes the literal for an array column.
    fn writeCopyCell(out: *std.ArrayListUnmanaged(u8), a: std.mem.Allocator, ra: std.mem.Allocator, p: msgpack.Payload, is_array: bool, is_geom: bool, vec: ?VecIdx) !void {
        switch (p) {
            .nil => try out.appendSlice(a, "\\N"),
            .bool => |b| try out.append(a, if (b) 't' else 'f'),
            .int => |i| try out.print(a, "{d}", .{i}),
            .uint => |u| try out.print(a, "{d}", .{u}),
            .float => |f| try out.print(a, "{d}", .{f}),
            .str => |v| {
                var txt: []const u8 = try core.pgTsToWire(ra, v.value());
                if (is_array) txt = try jsonArrayToLiteral(ra, txt);
                try copyEscape(out, a, txt);
            },
            .bin => |b| {
                // §10fg: a pgvector/bit cell goes in as its text form.
                if (vec) |vi| {
                    try copyEscape(out, a, try core.vecLiteral(ra, vi.kind, b.value(), vi.bits));
                    return;
                }
                if (!is_geom) try out.appendSlice(a, "\\\\x");
                for (b.value()) |byte| try out.print(a, "{x:0>2}", .{byte});
            },
            else => try copyEscape(out, a, try core.valueToString(ra, try mpToJson(ra, p))),
        }
    }

    fn copyEscape(out: *std.ArrayListUnmanaged(u8), a: std.mem.Allocator, s: []const u8) !void {
        for (s) |ch| switch (ch) {
            '\\' => try out.appendSlice(a, "\\\\"),
            '\t' => try out.appendSlice(a, "\\t"),
            '\n' => try out.appendSlice(a, "\\n"),
            '\r' => try out.appendSlice(a, "\\r"),
            else => try out.append(a, ch),
        };
    }

    /// A chain cell, straight from its msgpack payload to a bind: the shapes
    /// `chainCellToStorage` gives a JSON value, without the JSON value.
    fn payloadToStorage(a: std.mem.Allocator, p: msgpack.Payload) !storage.Value {
        return switch (p) {
            .nil => .null,
            .bool => |b| .{ .boolean = b },
            .int => |i| .{ .integer = i },
            .uint => |u| if (u <= std.math.maxInt(i64)) storage.Value{ .integer = @intCast(u) } else storage.Value{ .real = @floatFromInt(u) },
            .float => |f| .{ .real = f },
            .str => |v| .{ .text = try core.pgTsToWire(a, v.value()) },
            .bin => |b| .{ .blob = b.value() },
            else => .{ .text = try core.valueToString(a, try mpToJson(a, p)) },
        };
    }

    /// The walk over a step's `rows` (§10fa): every row's offset, and the first
    /// primary-key cell as text (a str's bytes sliced from the document, an integer
    /// rendered), for the sort. Each row is decoded once into a scratch arena.
    const RowIndex = struct { offsets: []usize, keys: []const []const u8 };

    fn indexRows(self: *SyncClient, a: std.mem.Allocator, cur: *MpCursor, nrows: usize, cols: []const []const u8, pk: []const []const u8) !RowIndex {
        const offsets = try a.alloc(usize, nrows);
        const keys = try a.alloc([]const u8, nrows);
        const ki: ?usize = if (pk.len > 0) indexOf(cols, pk[0]) else null;
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        for (0..nrows) |i| {
            offsets[i] = cur.off;
            keys[i] = "";
            _ = scratch.reset(.retain_capacity);
            const sa = scratch.allocator();
            // The key cell without decoding the row: array header, then the cells
            // before it skipped, then its bytes; the lib decodes what the peek cannot.
            var peek = cur.*;
            const ncells = peek.readArrayLen() catch null;
            if (ki) |k| if (ncells != null and k < ncells.?) {
                var ok = true;
                for (0..k) |_| peek.skipValue(sa) catch {
                    ok = false;
                    break;
                };
                if (ok) {
                    if (peek.peekStr()) |sl| {
                        keys[i] = sl;
                    } else {
                        const v = peek.readValue(sa) catch msgpack.Payload{ .nil = {} };
                        if (v == .int) keys[i] = try std.fmt.allocPrint(a, "{d:0>20}", .{v.int}) else if (v == .uint) keys[i] = try std.fmt.allocPrint(a, "{d:0>20}", .{v.uint});
                    }
                }
            };
            try cur.skipValue(sa);
        }
        return .{ .offsets = offsets, .keys = keys };
    }

    /// Row indices ordered by key (the sort of §10ez), on the index above.
    fn sortedByKeys(a: std.mem.Allocator, keys: []const []const u8) ![]usize {
        const order = try a.alloc(usize, keys.len);
        for (order, 0..) |*o, i| o.* = i;
        const Ctx = struct {
            keys: []const []const u8,
            fn lessThan(self: @This(), x: usize, y: usize) bool {
                return std.mem.order(u8, self.keys[x], self.keys[y]) == .lt;
            }
        };
        std.sort.pdq(usize, order, Ctx{ .keys = keys }, Ctx.lessThan);
        return order;
    }

    fn applyChain(self: *SyncClient, table: []const u8, tenant: []const u8) !void {
        // Per-call: a seed can be megabytes, and it is dead the moment it is applied.
        // Only `seed_stream` leaves this arena, duped into the client arena explicitly
        // below.
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const a = ca.allocator();
        const key = try std.fmt.allocPrint(a, "{s}.{s}", .{ tenant, table });
        const man_bytes = (try self.t.kvGet(a, self.kv_generations, key)) orelse {
            std.debug.print("{s}: no chain yet ({s})\n", .{ table, tenant });
            return;
        };
        const man = (try std.json.parseFromSlice(Value, a, man_bytes, .{})).value;

        // §10df: a manifest built BEFORE the re-seed was asked for cannot serve it —
        // seeding from it would record the new epoch over the old data. The descriptor
        // (the epoch we hold) and the producer's full arrive on independent clocks;
        // wait for the manifest to catch up, retried on the next poll or sync.
        const man_epoch: i64 = if (man.object.get("seed_epoch")) |v| (if (v == .integer) v.integer else 0) else 0;
        const want_epoch: i64 = if (self.states.get(table)) |s0| s0.seed_epoch else 0;
        if (man_epoch < want_epoch) {
            std.debug.print("{s}: chain g{d} predates the re-seed (epoch {d} < {d}) — waiting for the producer's full\n", .{ table, if (man.object.get("gen")) |v| v.integer else 0, man_epoch, want_epoch });
            self.reseed_pending = true;
            return;
        }

        const wm_rows = try self.st.query(a, "SELECT watermark FROM _zbz_generations WHERE tbl = ? AND tenant = ?", &.{ .{ .text = table }, .{ .text = tenant } });
        const watermark: ?[]const u8 = if (wm_rows.len > 0 and wm_rows[0][0] == .text) wm_rows[0][0].text else null;

        const plan = try core.planFromManifest(a, man, watermark);
        // D2's destruction guard: a full from a chain older than this replica's
        // position would destroy rows CDC will never re-deliver.
        const cdc_stream = if (man.object.get("cdc_stream")) |v| (if (v == .string) v.string else "") else "";
        const pos: i64 = if (cdc_stream.len > 0) @intCast(try self.storedSeq(cdc_stream)) else 0;
        if (core.fullPredatesReplica(man, plan.array, pos)) {
            std.debug.print("{s}: chain predates replica — refusing the full (D2)\n", .{table});
            return;
        }
        // §10ei: a chain older than the STREAM cannot splice — the events between its
        // cutoff and the oldest message the stream still holds are gone, and a client
        // that seeded from it and read on would carry the hole for ever. The contract
        // (§10eg) keeps the age above two cadences so this never happens in a healthy
        // deployment; a size valve, a purge or a too-short age breaks it, and then the
        // honest move is to wait for the producer's next generation, polled each turn.
        const cutoff_seq: i64 = if (man.object.get("cutoff_seq")) |v| (if (v == .integer) v.integer else 0) else 0;
        // The incarnation of the stream this manifest was cut on (its `created`); absent
        // in manifests from before the field.
        const man_created = if (man.object.get("cdc_stream_created")) |v| (if (v == .string) v.string else "") else "";
        // What the seed gate may anchor on: the manifest's cutoff_seq — but only on the
        // stream incarnation it was cut on. A manifest naming another incarnation was
        // cut before a recreate: its seq means nothing here and gates nothing. Without
        // the field, the `restarted` heuristic (cutoff beyond last_seq) stands in.
        var gate_seq: i64 = cutoff_seq;
        // §10ja (core.ts's shell): checked whenever the field is PRESENT, 0 included — a
        // chain cut on an empty stream predates a stream that now starts past 1.
        const has_cutoff_seq = man.object.get("cutoff_seq") != null;
        if (cdc_stream.len > 0 and has_cutoff_seq) {
            if (self.t.js.getStreamInfo(cdc_stream)) |info_c| {
                var info = info_c;
                defer info.deinit();
                const first: i64 = @intCast(info.value.state.first_seq);
                const last: i64 = @intCast(info.value.state.last_seq);
                if (man_created.len > 0 and info.value.created.len > 0) {
                    if (!std.mem.eql(u8, man_created, info.value.created)) {
                        std.debug.print("{s}: chain g{d} was cut on a previous incarnation of {s} (created {s}, now {s}) — seeding it, gating nothing until a newer generation\n", .{ table, if (man.object.get("gen")) |v| v.integer else 0, cdc_stream, man_created, info.value.created });
                        gate_seq = 0;
                    } else {
                        _ = self.restarted.remove(cdc_stream);
                    }
                } else if (self.restarted.contains(cdc_stream)) {
                    if (cutoff_seq > last) {
                        std.debug.print("{s}: chain g{d} was cut before {s} restarted (cutoff seq {d} beyond last_seq {d}) — seeding it, gating nothing until a newer generation\n", .{ table, if (man.object.get("gen")) |v| v.integer else 0, cdc_stream, cutoff_seq, last });
                        gate_seq = 0;
                    } else {
                        _ = self.restarted.remove(cdc_stream);
                    }
                }
                if (cutoff_seq + 1 < first) {
                    std.debug.print("{s}: chain g{d} predates the stream (cutoff seq {d} < first {d} on {s}) — the events between are gone; waiting for the producer's next generation\n", .{ table, if (man.object.get("gen")) |v| v.integer else 0, cutoff_seq, first, cdc_stream });
                    self.reseed_pending = true;
                    return;
                }
            } else |_| {}
        }

        const st = self.states.getPtr(table).?;
        const bucket = try std.fmt.allocPrint(a, "{s}{s}", .{ self.gen_bucket_prefix, tenant });
        var applied: usize = 0;
        var streamed: usize = 0;
        // Phase timers (§10ez): where a seed's time goes — fetch, inflate, decode, apply.
        var ph = [_]i64{ 0, 0, 0, 0 };
        // §10ez: a seed inserts millions of random keys into a b-tree; SQLite's default
        // page cache is 2 MB, so nearly every insert touched a cold page. 128 MB for the
        // seed, the default back after — the replica's steady state is small.
        self.st.execSimple("PRAGMA cache_size = -131072") catch {};
        defer self.st.execSimple("PRAGMA cache_size = -2000") catch {};
        for (plan.array.items) |step| {
            // Per-step: `raw`, `blob` and `doc` are three copies of the same seed at
            // their largest; freeing them per step bounds the peak at one step's worth.
            var sa = std.heap.ArenaAllocator.init(self.a);
            defer sa.deinit();
            const step_a = sa.allocator();
            var t_ph = msNow();
            const is_full = std.mem.eql(u8, step.object.get("kind").?.string, "full");
            const raw: []const u8 = if (self.opts.seed_streaming) blk: {
                // The object's size is in its meta, read before any chunk: a small
                // step takes the whole-object path through the same pull reader.
                var pull = try transport.ObjectPull.open(self.t, step_a, bucket, step.object.get("name").?.string);
                if (pull.size >= self.opts.seed_streaming_above) {
                    defer pull.deinit();
                    applied += try self.applyStepStreaming(step_a, table, tenant, st, man, step, &pull, is_full, &ph);
                    streamed += 1;
                    continue;
                }
                defer pull.deinit();
                break :blk try pull.readAll(step_a);
            } else try self.t.objectGetBytes(step_a, bucket, step.object.get("name").?.string);
            ph[0] += msNow() - t_ph;
            t_ph = msNow();
            const blob = try maybeZstd(step_a, raw); // §10w: magic-sniffed, mixed chains fine
            ph[1] += msNow() - t_ph;
            t_ph = msNow();
            // §10fa: the document is walked, not decoded. Its keys in the producer's
            // order — columns, rows, then the small ones; `rows` is indexed (offset and
            // key per row) in one pass, everything else read as a value. A document
            // that is not the shape the producer writes (a corrupt or foreign object
            // under the name) is an ERROR the host sees — never a union access that
            // kills the host process.
            var cur = MpCursor{ .bytes = blob };
            const nkeys = cur.readMapLen() catch return error.ChainObjectMalformed;
            var cols: []const []const u8 = &.{};
            var index: ?RowIndex = null;
            var vcol_doc: ?[]const u8 = null;
            for (0..nkeys) |_| {
                const mkey = cur.readStr() catch return error.ChainObjectMalformed;
                if (std.mem.eql(u8, mkey, "columns")) {
                    const v = cur.readValue(step_a) catch return error.ChainObjectMalformed;
                    cols = try jsonStrList(step_a, try mpToJson(step_a, v));
                    for (cols) |col| if (!contains(st.cols, col)) {
                        std.debug.print("{s}: chain object {s} names column {s}, which the replica lacks — predates the schema, waiting for the producer's full\n", .{ table, step.object.get("name").?.string, col });
                        return;
                    };
                } else if (std.mem.eql(u8, mkey, "rows")) {
                    if (cols.len == 0) return error.ChainObjectMalformed;
                    const nrows = cur.readArrayLen() catch return error.ChainObjectMalformed;
                    index = try self.indexRows(step_a, &cur, nrows, cols, st.pk);
                } else {
                    const v = cur.readValue(step_a) catch return error.ChainObjectMalformed;
                    if (std.mem.eql(u8, mkey, "version_column") and v == .str) vcol_doc = v.str.value();
                }
            }
            const idx = index orelse return error.ChainObjectMalformed;
            const vcol_v: ?[]const u8 = vcol_doc orelse (if (man.object.get("version_column")) |v| (if (v == .string) v.string else null) else null);
            const vcol: ?[]const u8 = if (vcol_v != null and contains(cols, vcol_v.?)) vcol_v else null;
            const order = try sortedByKeys(step_a, idx.keys);
            const shape = try self.stepShape(step_a, table, tenant, st.*, cols, vcol, is_full);
            var copy_buf: std.ArrayListUnmanaged(u8) = .empty;
            defer copy_buf.deinit(self.a);
            ph[2] += msNow() - t_ph;
            t_ph = msNow();
            const chunk: usize = if (self.opts.seed_chunk_rows == 0) @max(idx.offsets.len, 1) else self.opts.seed_chunk_rows;
            var from: usize = 0;
            while (from < idx.offsets.len or (from == 0 and is_full)) {
                const to = @min(from + chunk, idx.offsets.len);
                const cs = shape.step(self, step_a, blob, idx.offsets, order, from, to, from == 0, &copy_buf);
                self.st.transaction(cs, ChainStep.apply) catch |err| {
                    std.debug.print("{s}: chain step {s} ({s}, rows {d}..{d} of {d}) rolled back: {any} — {s}\n", .{
                        table, step.object.get("name").?.string, if (is_full) "full" else "delta", from, to, idx.offsets.len, err, self.st.errMsg(),
                    });
                    return err;
                };
                if (to == from) break;
                from = to;
            }
            applied += idx.offsets.len;
            ph[3] += msNow() - t_ph;
        }

        // Anchors (findings 7 + 10): the ONE place the gate may anchor to — per
        // stream (§10fn): this chain's cutoff on the stream it was cut on.
        const seed_lsn: i64 = if (man.object.get("cutoff_lsn")) |v| (if (v == .string) core.lsnToNumber(v.string) else 0) else 0;
        if (cdc_stream.len > 0) {
            var anchor: SeedAnchor = .{ .stream = try self.aa().dupe(u8, cdc_stream), .seq = 0, .lsn = seed_lsn };
            if (gate_seq > 0) anchor.seq = @intCast(gate_seq);
            tr("{s}: seed anchor on {s}: seq {d} (chain g{d} cutoff_seq {d}, watermark {s}, position {d})", .{ table, cdc_stream, anchor.seq, if (man.object.get("gen")) |v| v.integer else 0, cutoff_seq, if (man.object.get("cutoff_version")) |v| (if (v == .string) v.string else "?") else "?", pos });
            var replaced = false;
            for (st.anchors.items) |*an| if (std.mem.eql(u8, an.stream, cdc_stream)) {
                an.* = anchor;
                replaced = true;
            };
            if (!replaced) try st.anchors.append(self.aa(), anchor);
        }
        const cv = if (man.object.get("cutoff_version")) |v| (if (v == .string) v.string else "") else "";
        // The chain's cutoff is a version this replica has now seen (every seeded row is
        // at or below it): feed the HLC floor, as the TS client does.
        if (cv.len > 0) {
            const norm = try core.normalizeVersion(a, try core.pgTsToWire(a, cv));
            const floor = core.maxVersion(self.seen_floor, norm);
            if (floor.ptr != self.seen_floor.ptr and floor.len <= self.seen_floor_buf.len) {
                @memcpy(self.seen_floor_buf[0..floor.len], floor);
                self.seen_floor = self.seen_floor_buf[0..floor.len];
            }
        }
        const anchor_seq: i64 = if (cdc_stream.len > 0 and gate_seq > 0) gate_seq else 0;
        _ = try self.st.query(a, "INSERT INTO _zbz_generations (tbl, tenant, watermark, cutoff_lsn, seed_epoch, anchor_stream, anchor_seq) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(tbl, tenant) DO UPDATE SET watermark = excluded.watermark, cutoff_lsn = excluded.cutoff_lsn, seed_epoch = excluded.seed_epoch, anchor_stream = excluded.anchor_stream, anchor_seq = excluded.anchor_seq", &.{ .{ .text = table }, .{ .text = tenant }, .{ .text = cv }, .{ .integer = seed_lsn }, .{ .integer = st.seed_epoch }, .{ .text = cdc_stream }, .{ .integer = anchor_seq } });
        // A held event at or below the seed's LSN is inside the chain just applied:
        // superseded, not waiting (the TS client's pruneInboxSeeded).
        try pruneInboxSeeded(&self.st, a, table, seed_lsn);
        if (streamed > 0) {
            std.debug.print("{s}: seeded {d} row(s) from chain g{d} — fetch {d} ms, inflate {d} ms, decode {d} ms, apply {d} ms ({d} of {d} step(s) streamed: fetch+inflate as read, decode = the stage's sort)\n", .{ table, applied, if (man.object.get("gen")) |v| v.integer else 0, ph[0], ph[1], ph[2], ph[3], streamed, plan.array.items.len });
        } else {
            std.debug.print("{s}: seeded {d} row(s) from chain g{d} ({s}) — fetch {d} ms, inflate {d} ms, decode {d} ms, apply {d} ms\n", .{ table, applied, if (man.object.get("gen")) |v| v.integer else 0, tenant, ph[0], ph[1], ph[2], ph[3] });
        }
    }

    /// §10fh: one chain step, streamed. The object is read from the store a chunk at a
    /// time and inflated through a window (StreamInflate); the document's keys are
    /// parsed from the window as they complete — a parse that runs out of bytes before
    /// the source is exhausted just means "read more", after it means malformed. Rows
    /// are copied out of the window into a chunk buffer of `seed_chunk_rows`, indexed,
    /// sorted by key and applied through the same ChainStep as the whole-object path;
    /// the chunk buffer is reused, so a step never holds more than one chunk of rows.
    /// Returns the rows applied.
    fn applyStepStreaming(self: *SyncClient, step_a: std.mem.Allocator, table: []const u8, tenant: []const u8, st: *TableState, man: Value, step: Value, pull: *transport.ObjectPull, is_full: bool, ph: *[4]i64) !usize {
        const name = step.object.get("name").?.string;
        const res = pull;
        var zs = try StreamInflate.init(self.a, res);
        defer zs.deinit();
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();

        var t_ph = msNow();
        const nkeys = try zs.parse(&scratch, MpCursor.readMapLen);
        var cols: []const []const u8 = &.{};
        var nrows: ?usize = null;
        var keys_after: usize = 0;
        for (0..nkeys) |k| {
            const mkey = try step_a.dupe(u8, try zs.parse(&scratch, MpCursor.readStr));
            if (std.mem.eql(u8, mkey, "columns")) {
                // ⚠️ Duped: the value lives in the scratch arena, reset at the next
                // parse — the list sliced into it and read row bytes as column names
                // (the key column was never found; the stage sorted by ordinal, and
                // the apply ran in arrival order: 94 s for what takes 9 s in order).
                const v = try zs.parseValue(&scratch);
                cols = try dupeStrings(step_a, try jsonStrList(scratch.allocator(), try mpToJson(scratch.allocator(), v)));
                for (cols) |col| if (!contains(st.cols, col)) {
                    std.debug.print("{s}: chain object {s} names column {s}, which the replica lacks — predates the schema, waiting for the producer's full\n", .{ table, name, col });
                    return error.ChainObjectMalformed;
                };
            } else if (std.mem.eql(u8, mkey, "rows")) {
                if (cols.len == 0) return error.ChainObjectMalformed;
                nrows = try zs.parse(&scratch, MpCursor.readArrayLen);
                keys_after = nkeys - k - 1;
                break;
            } else {
                _ = try zs.parseValue(&scratch);
            }
        }
        const total = nrows orelse return error.ChainObjectMalformed;
        // The version column is needed before the rows, and the document names it
        // after them: the manifest's is the same column, as of the chain's last step.
        const vcol_v: ?[]const u8 = if (man.object.get("version_column")) |v| (if (v == .string) v.string else null) else null;
        const vcol: ?[]const u8 = if (vcol_v != null and contains(cols, vcol_v.?)) vcol_v else null;
        const shape = try self.stepShape(step_a, table, tenant, st.*, cols, vcol, is_full);
        var copy_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer copy_buf.deinit(self.a);
        const chunk_rows: usize = if (self.opts.seed_chunk_rows == 0) @max(total, 1) else self.opts.seed_chunk_rows;

        // On SQLite the rows are STAGED: an unindexed temp table takes them in arrival
        // order (batches of `stage_batch` in transactions of `chunk_rows`), one index
        // build sorts them on disk, and keyed pages come back in GLOBAL key order for
        // the apply. Sorting per chunk instead (the first cut) put 61 runs of random
        // keys into a 3 M-entry b-tree and took 127 s where the whole-object seed takes
        // 11 s; the staged sort keeps the order the b-tree wants, at the cost of the
        // rows written twice. On PostgreSQL the chunks go in as they come — COPY into
        // a heap needs no order.
        // §10ja: a step the producer cut in key order is applied as it arrives — each chunk
        // appends to the key index, and staging would write every row twice for an order
        // the rows already have.
        const step_sorted = if (step.object.get("sorted")) |v| v == .bool and v.bool else false;
        const staged = self.st.engine == .sqlite and !step_sorted;
        const stage_batch: usize = 64;
        if (staged) {
            self.st.execSimple("DROP TABLE IF EXISTS temp._zbz_stage") catch {};
            // The stage lives in the temp database: a file, with a cache of its own
            // kept small — the sorter and the stage's pages are what a phone must not
            // hold, the replica's cache (128 MB for the seed) is what the ordered
            // apply wants.
            self.st.execSimple("PRAGMA temp_store = FILE") catch {};
            self.st.execSimple("PRAGMA temp.cache_size = -32768") catch {};
            try self.st.execSimple("CREATE TEMP TABLE _zbz_stage (k BLOB, row BLOB)");
        }
        defer if (staged) self.st.execSimple("DROP TABLE IF EXISTS temp._zbz_stage") catch {};
        const stage_sql = try stageInsertSql(step_a, stage_batch);

        var chunk_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer chunk_buf.deinit(self.a);
        var chunk_arena = std.heap.ArenaAllocator.init(self.a);
        defer chunk_arena.deinit();
        var batch_keys: std.ArrayListUnmanaged([]const u8) = .empty;
        var batch_offs: std.ArrayListUnmanaged(usize) = .empty;
        var in_chunk: usize = 0;
        var chunk_no: usize = 0;
        var applied: usize = 0;
        var i: usize = 0;
        if (staged and total > 0) try self.st.execSimple("BEGIN");
        // §10gm: an error while staging (the object stream timing out, a full pruned while
        // it was read) left this transaction open, and every later BEGIN on the connection
        // failed — each seed retry and each CDC batch refused with StepFailed until the
        // client was closed. A ROLLBACK outside a transaction is an error, ignored.
        errdefer if (staged) self.st.execSimple("ROLLBACK") catch {};
        while (i < total or (total == 0 and is_full and chunk_no == 0 and !staged)) {
            if (i < total) {
                const len = try zs.parseRowLen(&scratch);
                const row = zs.avail()[0..len];
                if (staged) {
                    // the key now, while the row is in the window; the row itself is
                    // copied into the batch buffer — the window moves under it
                    const ba = chunk_arena.allocator();
                    var kc = MpCursor{ .bytes = row };
                    const ki = try self.indexRows(ba, &kc, 1, cols, st.pk);
                    // the key made unique by the row's ordinal, so a page is
                    // `k > last` on the index — no row-value trick, no re-sort per page
                    const kb = try ba.alloc(u8, ki.keys[0].len + 8);
                    @memcpy(kb[0..ki.keys[0].len], ki.keys[0]);
                    std.mem.writeInt(u64, kb[ki.keys[0].len..][0..8], @intCast(i), .big);
                    try batch_keys.append(ba, kb);
                    try batch_offs.append(ba, chunk_buf.items.len);
                }
                try chunk_buf.appendSlice(self.a, row);
                zs.consumed += len;
                in_chunk += 1;
                i += 1;
                if (staged and (batch_keys.items.len == stage_batch or i == total)) {
                    const n = batch_keys.items.len;
                    const ba = chunk_arena.allocator();
                    const params = try ba.alloc(storage.Value, n * 2);
                    for (0..n) |bi| {
                        const end = if (bi + 1 < n) batch_offs.items[bi + 1] else chunk_buf.items.len;
                        params[bi * 2] = .{ .blob = batch_keys.items[bi] };
                        params[bi * 2 + 1] = .{ .blob = chunk_buf.items[batch_offs.items[bi]..end] };
                    }
                    _ = try self.st.query(ba, if (n == stage_batch) stage_sql else try stageInsertSql(ba, n), params);
                    chunk_buf.clearRetainingCapacity();
                    batch_keys = .empty;
                    batch_offs = .empty;
                    _ = chunk_arena.reset(.retain_capacity);
                }
            }
            if (in_chunk == chunk_rows or i == total) {
                if (staged) {
                    try self.st.execSimple("COMMIT");
                    if (i < total) try self.st.execSimple("BEGIN");
                    in_chunk = 0;
                    chunk_no += 1;
                    continue;
                }
                ph[1] += msNow() - t_ph;
                t_ph = msNow();
                _ = chunk_arena.reset(.retain_capacity);
                const ca = chunk_arena.allocator();
                var cur = MpCursor{ .bytes = chunk_buf.items };
                const idx = try self.indexRows(ca, &cur, in_chunk, cols, st.pk);
                const order = try sortedByKeys(ca, idx.keys);
                ph[2] += msNow() - t_ph;
                t_ph = msNow();
                const cs = shape.step(self, ca, chunk_buf.items, idx.offsets, order, 0, in_chunk, chunk_no == 0, &copy_buf);
                self.st.transaction(cs, ChainStep.apply) catch |err| {
                    std.debug.print("{s}: chain step {s} ({s}, streaming chunk {d}, {d} row(s), {d} of {d} read) rolled back: {any}\n", .{
                        table, name, if (is_full) "full" else "delta", chunk_no, in_chunk, i, total, err,
                    });
                    return err;
                };
                ph[3] += msNow() - t_ph;
                t_ph = msNow();
                applied += in_chunk;
                chunk_buf.clearRetainingCapacity();
                in_chunk = 0;
                chunk_no += 1;
                if (total == 0) break;
            }
        }
        // The keys after `rows` (gen, kind, cutoff, version_column, prev_cutoff):
        // read to the end of the document, so a truncated object is an error here.
        for (0..keys_after) |_| {
            _ = try zs.parse(&scratch, MpCursor.readStr);
            _ = try zs.parseValue(&scratch);
        }
        ph[1] += msNow() - t_ph;
        t_ph = msNow();
        try res.verify();
        if (!staged) return applied;
        const trace = std.c.getenv("ZB_SEED_TRACE") != null;
        if (trace) std.debug.print("  trace: staged {d} rows in {d} ms, maxrss {d} MB\n", .{ total, msNow() - t_ph + ph[1], maxRssMb() });

        // ── the staged apply: one sort, then pages in key order ──────────────────
        // The index CARRIES the row: building it is the external sort of the rows
        // themselves (SQLite's sorter, temp files, the temp cache's memory), and a
        // page is then a sequential read of the index. An index on the key alone
        // sorted the keys and fetched each row from the stage by rowid — a random
        // read per row, 3 M of them, 60 s.
        // SQLite's sorter budgets itself on the MAIN database's cache_size, per worker
        // thread: 128 MB × 3 measured 390 MB. One thread and a 32 MB cache for the
        // build; the seed's cache back for the apply, which is what wants it.
        self.st.execSimple("PRAGMA threads = 0") catch {};
        self.st.execSimple("PRAGMA cache_size = -32768") catch {};
        try self.st.execSimple("CREATE INDEX _zbz_stage_k ON _zbz_stage (k, row)");
        self.st.execSimple("PRAGMA cache_size = -131072") catch {};
        ph[2] += msNow() - t_ph;
        if (trace) std.debug.print("  trace: index built in {d} ms, maxrss {d} MB\n", .{ msNow() - t_ph, maxRssMb() });
        t_ph = msNow();
        var sel_ms: i64 = 0;
        var app_ms: i64 = 0;
        var inversions: usize = 0;
        var prev_k: std.ArrayListUnmanaged(u8) = .empty;
        defer prev_k.deinit(self.a);
        var last_k: std.ArrayListUnmanaged(u8) = .empty;
        defer last_k.deinit(self.a);
        var page_no: usize = 0;
        while (true) {
            _ = chunk_arena.reset(.retain_capacity);
            const ca = chunk_arena.allocator();
            const t_sel = msNow();
            const rows = try self.st.query(ca, "SELECT k, row FROM _zbz_stage WHERE k > ? ORDER BY k LIMIT ?", &.{ .{ .blob = last_k.items }, .{ .integer = @intCast(chunk_rows) } });
            sel_ms += msNow() - t_sel;
            if (rows.len == 0 and !(page_no == 0 and is_full)) break;
            chunk_buf.clearRetainingCapacity();
            const offsets = try ca.alloc(usize, rows.len);
            const order = try ca.alloc(usize, rows.len);
            for (rows, 0..) |r, ri| {
                offsets[ri] = chunk_buf.items.len;
                order[ri] = ri;
                if (trace) {
                    const kk: []const u8 = switch (r[0]) {
                        .blob => |bb| bb,
                        .text => |tt| tt,
                        else => "",
                    };
                    if (prev_k.items.len > 0 and std.mem.order(u8, prev_k.items, kk) != .lt) inversions += 1;
                    prev_k.clearRetainingCapacity();
                    try prev_k.appendSlice(self.a, kk);
                }
                try chunk_buf.appendSlice(self.a, switch (r[1]) {
                    .blob => |bb| bb,
                    .text => |tt| tt,
                    else => return error.ChainObjectMalformed,
                });
            }
            if (rows.len > 0) {
                last_k.clearRetainingCapacity();
                try last_k.appendSlice(self.a, switch (rows[rows.len - 1][0]) {
                    .blob => |bb| bb,
                    .text => |tt| tt,
                    else => "",
                });
            }
            const cs = shape.step(self, ca, chunk_buf.items, offsets, order, 0, rows.len, page_no == 0, &copy_buf);
            const t_app = msNow();
            self.st.transaction(cs, ChainStep.apply) catch |err| {
                std.debug.print("{s}: chain step {s} ({s}, staged page {d}, {d} row(s)) rolled back: {any}\n", .{
                    table, name, if (is_full) "full" else "delta", page_no, rows.len, err,
                });
                return err;
            };
            app_ms += msNow() - t_app;
            if (trace and (page_no < 3 or page_no % 20 == 0)) std.debug.print("  trace: page {d}: {d} rows, apply {d} ms, maxrss {d} MB, inversions so far {d}\n", .{ page_no, rows.len, msNow() - t_app, maxRssMb(), inversions });
            applied += rows.len;
            page_no += 1;
            if (rows.len < chunk_rows) break;
        }
        ph[3] += msNow() - t_ph;
        if (trace) std.debug.print("  trace: {d} page(s): select {d} ms, apply {d} ms, maxrss {d} MB\n", .{ page_no, sel_ms, app_ms, maxRssMb() });
        return applied;
    }

    /// `INSERT INTO _zbz_stage (k, row) VALUES (?, ?), …` for `n` rows.
    fn stageInsertSql(a: std.mem.Allocator, n: usize) ![]const u8 {
        var out: std.ArrayListUnmanaged(u8) = .empty;
        try out.appendSlice(a, "INSERT INTO _zbz_stage (k, row) VALUES ");
        for (0..n) |k| try out.appendSlice(a, if (k == 0) "(?, ?)" else ", (?, ?)");
        return out.toOwnedSlice(a);
    }

    /// What every chunk of a step shares — the statement, the column indices a
    /// PostgreSQL replica renders specially, the COPY upsert — built once per step.
    const StepShape = struct {
        table: []const u8,
        sql: []const u8,
        is_full: bool,
        cols: []const []const u8,
        pk: []const []const u8,
        tomb_idx: ?usize,
        array_idx: []const usize,
        geom_idx: []const usize,
        vec_idx: []const VecIdx,
        copy: bool,
        copy_upsert_sql: []const u8,
        wipe_sql: []const u8,
        wipe_arg: ?[]const u8,
        wipe_whole: bool,

        fn step(self: StepShape, client: *SyncClient, a: std.mem.Allocator, bytes: []const u8, offsets: []const usize, order: []const usize, from: usize, to: usize, first_chunk: bool, copy_buf: *std.ArrayListUnmanaged(u8)) ChainStep {
            return .{
                .client = client,
                .a = a,
                .table = self.table,
                .sql = self.sql,
                .bytes = bytes,
                .offsets = offsets,
                .order = order,
                .from = from,
                .to = to,
                .is_full = self.is_full,
                .first_chunk = first_chunk,
                .cols = self.cols,
                .pk = self.pk,
                .tomb_idx = self.tomb_idx,
                .array_idx = self.array_idx,
                .geom_idx = self.geom_idx,
                .vec_idx = self.vec_idx,
                .copy = self.copy,
                .copy_upsert_sql = self.copy_upsert_sql,
                .copy_buf = copy_buf,
                .wipe_sql = self.wipe_sql,
                .wipe_arg = self.wipe_arg,
                .wipe_whole = self.wipe_whole,
            };
        }
    };

    fn stepShape(self: *SyncClient, a: std.mem.Allocator, table: []const u8, tenant: []const u8, st: TableState, cols: []const []const u8, vcol: ?[]const u8, is_full: bool) !StepShape {
        const whole = !self.multiTenant(st);
        var array_idx_list: std.ArrayListUnmanaged(usize) = .empty;
        if (self.st.engine == .postgres) for (st.array_cols) |ac| {
            if (indexOf(cols, ac)) |i| try array_idx_list.append(a, i);
        };
        var geom_idx_list: std.ArrayListUnmanaged(usize) = .empty;
        if (self.st.engine == .postgres) for (st.geom_cols) |gc| {
            if (indexOf(cols, gc)) |i| try geom_idx_list.append(a, i);
        };
        var vec_idx_list: std.ArrayListUnmanaged(VecIdx) = .empty;
        // §10fl: DuckDB takes a vector as its list text (`[1,2,3]` into FLOAT[n], '101'
        // into BIT) and keeps a sparsevec as the wire's BLOB; arrays stay JSON text,
        // which DuckDB casts to a list itself.
        if (self.st.engine == .postgres or self.st.engine == .duckdb) for (st.vec_cols) |vc| {
            if (self.st.engine == .duckdb and vc.kind == .sparsevec) continue;
            if (indexOf(cols, vc.name)) |i| try vec_idx_list.append(a, .{ .i = i, .kind = vc.kind, .bits = vc.bits });
        };
        const bulk = self.st.engine == .postgres or self.st.engine == .duckdb;
        return .{
            .table = table,
            .sql = if (is_full and whole) try core.chainInsertSql(a, table, cols) else try core.chainUpsertSql(a, table, cols, st.pk, vcol),
            .wipe_sql = if (whole) try std.fmt.allocPrint(a, "DELETE FROM {s}", .{table}) else try std.fmt.allocPrint(a, "DELETE FROM {s} WHERE \"{s}\" = ?", .{ table, st.tenant_col.? }),
            .wipe_arg = if (whole) null else tenant,
            .wipe_whole = whole,
            .is_full = is_full,
            .cols = cols,
            .pk = st.pk,
            .tomb_idx = if (st.tombstone_col) |tc| indexOf(cols, tc) else null,
            .array_idx = array_idx_list.items,
            .geom_idx = geom_idx_list.items,
            .vec_idx = vec_idx_list.items,
            .copy = bulk,
            .copy_upsert_sql = if (bulk) try core.pgUpsertFromCopySql(a, table, cols, st.pk, vcol) else "",
        };
    }

    // ─── step 3: CDC catch-up (gate → apply → hold → positions) ─────────────

    pub fn drainCdc(self: *SyncClient) !void {
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const a = ca.allocator();
        var streams: std.StringArrayHashMapUnmanaged(void) = .empty;
        // Public first: parents (users) ride CDC_PUBLIC — fewer FK holds.
        var it = self.states.iterator();
        while (it.next()) |e| {
            for (e.value_ptr.routes) |r| if (std.mem.eql(u8, r, self.cdc_public)) try streams.put(a, r, {});
            if (e.value_ptr.shared_route) |sr| try streams.put(a, sr, {});
        }
        it = self.states.iterator();
        while (it.next()) |e| for (e.value_ptr.routes) |r| try streams.put(a, r, {});

        var sit = streams.iterator();
        while (sit.next()) |se| try self.drainStream(se.key_ptr.*);
        self.retryHeld(null, null);
    }

    /// The streams this client reads, public first: parents (users) ride
    /// CDC_PUBLIC — fewer FK holds. Allocated from `a`.
    fn cdcStreams(self: *SyncClient, a: std.mem.Allocator) ![]const []const u8 {
        var streams: std.StringArrayHashMapUnmanaged(void) = .empty;
        var it = self.states.iterator();
        while (it.next()) |e| {
            for (e.value_ptr.routes) |r| if (std.mem.eql(u8, r, self.cdc_public)) try streams.put(a, r, {});
            if (e.value_ptr.shared_route) |sr| try streams.put(a, sr, {});
        }
        it = self.states.iterator();
        while (it.next()) |e| for (e.value_ptr.routes) |r| try streams.put(a, r, {});
        return try a.dupe([]const u8, streams.keys());
    }

    /// FK hold/retry (§10h): one bulk retry pass after every stream had its turn.
    /// The inbox is the truth (the TS client's `_zebridge_inbox`, ported — CLIENTS.md):
    /// every held event lives in `_zbz_inbox` from the batch that held it until the
    /// retry that lands it. Still missing its parent → attempts + 1, next pass. Any
    /// other failure → dropped, loudly: an event that can never apply must not sit
    /// forever pretending it will.
    /// Passes until a pass resolves nothing (§10dg): a child held behind a parent that
    /// is itself held behind a grandparent needs the second pass — measured, the
    /// single pass left the child in the inbox until an unrelated event arrived.
    fn retryHeld(self: *SyncClient, report_a: ?std.mem.Allocator, changed_map: ?*std.StringArrayHashMapUnmanaged(void)) void {
        var passes: usize = 0;
        var total_len: usize = 0;
        var total_resolved: usize = 0;
        var total_dropped: usize = 0;
        // Limit passes to break cycles if two rows wait for each other.
        while (passes < 16) : (passes += 1) {
            const r = self.retryHeldPass(report_a, changed_map) catch return;
            if (passes == 0) total_len = r.len;
            total_resolved += r.resolved;
            total_dropped += r.dropped;
            if (r.resolved == 0 or r.len == r.resolved + r.dropped) break;
        }
        if (total_resolved > 0 or total_dropped > 0) {
            std.debug.print("fk held: {d}, applied on retry: {d}, dropped: {d}, still waiting: {d} ({d} pass(es))\n", .{ total_len, total_resolved, total_dropped, total_len -| (total_resolved + total_dropped), passes + 1 });
        }
    }

    const RetryPass = struct { len: usize, resolved: usize, dropped: usize };

    fn retryHeldPass(self: *SyncClient, report_a: ?std.mem.Allocator, changed_map: ?*std.StringArrayHashMapUnmanaged(void)) !RetryPass {
        var ra = std.heap.ArenaAllocator.init(self.a);
        defer ra.deinit();
        const a = ra.allocator();
        const rows = try self.st.query(a, "SELECT id, tbl, ev FROM _zbz_inbox ORDER BY id", &.{});
        if (rows.len == 0) return .{ .len = 0, .resolved = 0, .dropped = 0 };
        var resolved: usize = 0;
        var dropped: usize = 0;
        for (rows) |r| {
            const id = r[0];
            const table = r[1].text;
            const ev = std.json.parseFromSliceLeaky(Value, a, r[2].text, .{}) catch {
                _ = self.st.query(a, "DELETE FROM _zbz_inbox WHERE id = ?", &.{id}) catch {};
                dropped += 1;
                continue;
            };
            if (self.applyEvent(table, ev, "", 0)) |_| {
                _ = self.st.query(a, "DELETE FROM _zbz_inbox WHERE id = ?", &.{id}) catch {};
                resolved += 1;
                // Duped: `table` is a row of this pass's arena (§10ee).
                if (report_a) |ra_| if (changed_map) |cm| {
                    if (!cm.contains(table)) cm.put(ra_, ra_.dupe(u8, table) catch table, {}) catch {};
                };
            } else |err| switch (err) {
                error.FkHeld, error.SchemaBehind => {
                    _ = self.st.query(a, "UPDATE _zbz_inbox SET attempts = attempts + 1 WHERE id = ?", &.{id}) catch {};
                },
                else => {
                    std.debug.print("DROPPED held event on {s}: {s}\n", .{ table, @errorName(err) });
                    _ = self.st.query(a, "DELETE FROM _zbz_inbox WHERE id = ?", &.{id}) catch {};
                    dropped += 1;
                },
            }
        }
        return .{ .len = rows.len, .resolved = resolved, .dropped = dropped };
    }

    /// §10gm: the subjects this client wants on `stream` — one per followed table:
    /// `cdc.<table>.>` for a public table, `cdc.<tenant>.<table>.>` for a tenant-scoped
    /// one (its open-tenant rows ride the public stream under the open tenant's name).
    /// Without them the consumer downloads every table of the stream and drops what the
    /// client does not follow: a phone that follows one table pays for the whole feed.
    /// Nothing else rides a CDC stream (PROTOCOL §4), so the filters lose nothing.
    fn cdcFilters(self: *SyncClient, a: std.mem.Allocator, stream: []const u8) ![]const []const u8 {
        var subs: std.ArrayListUnmanaged([]const u8) = .empty;
        var it = self.states.iterator();
        while (it.next()) |e| {
            const table = e.key_ptr.*;
            const st = e.value_ptr.*;
            if (st.tenant_col == null) {
                for (st.routes) |r| if (std.mem.eql(u8, r, stream)) {
                    try subs.append(a, try std.fmt.allocPrint(a, "{s}.{s}.>", .{ self.subject_cdc_prefix, table }));
                };
                continue;
            }
            for (self.tenantsFor(st)) |tenant| {
                const route = try self.routeFor(a, tenant);
                if (!std.mem.eql(u8, route, stream)) continue;
                try subs.append(a, try std.fmt.allocPrint(a, "{s}.{s}.{s}.>", .{ self.subject_cdc_prefix, tenant, table }));
            }
            if (st.shared_route) |sr| if (std.mem.eql(u8, sr, stream)) {
                const open = try std.fmt.allocPrint(a, "{s}.{s}.{s}.>", .{ self.subject_cdc_prefix, self.open_tenant, table });
                for (subs.items) |have| {
                    if (std.mem.eql(u8, have, open)) break;
                } else try subs.append(a, open);
            };
        }
        return subs.items;
    }

    /// A pull consumer on `stream` positioned just past the stored sequence, named
    /// uniquely per process, reaped by the server after `inactive_ns` idle.
    fn openConsumer(self: *SyncClient, stream: []const u8, inactive_ns: u64, shared: ?*@import("nats").PullInbox) !*@import("nats").PullSubscription {
        const last = try self.storedSeq(stream);
        var name_buf: [48]u8 = undefined;
        // Unique-enough per process: pid + a monotonic counter (std.crypto.random
        // wants an Io in 0.16; libc is already linked).
        const Ctr = struct {
            var n = std.atomic.Value(u32).init(0);
        };
        const cname = try std.fmt.bufPrint(&name_buf, "zbz{d}x{d}", .{ std.c.getpid(), Ctr.n.fetchAdd(1, .monotonic) });

        var cfg = transportConsumerConfig();
        cfg.name = cname;
        cfg.durable_name = cname;
        // §10gm: only this client's tables. `filter_subject` for one (every server
        // understands it), `filter_subjects` for several (nats-server >= 2.10).
        var fa = std.heap.ArenaAllocator.init(self.a);
        defer fa.deinit();
        const filters = try self.cdcFilters(fa.allocator(), stream);
        if (filters.len == 1) cfg.filter_subject = filters[0] else if (filters.len > 1) cfg.filter_subjects = filters;
        // ⚠️ Reaped by the SERVER, not deleted by us. The consumer is named (nats.zig's
        // `pullSubscribe` requires it, and sets it durable), and the client JWT has no
        // `$JS.API.CONSUMER.DELETE` grant — by design — so deleting it from here was
        // refused and cost a 5 s timeout per stream (measured 2026-08-29). An inactive
        // threshold makes the server do it: nats-server >= 2.9 honours it on durable
        // consumers as well. 30 s is well past any fetch gap in a drain and short
        // enough that a client draining on a timer never accumulates consumers.
        // (Field added by the local nats.zig patch — nats.zig/NOTES.md.)
        cfg.inactive_threshold = inactive_ns;
        if (last > 0) {
            cfg.deliver_policy = .by_start_sequence;
            cfg.opt_start_seq = last + 1;
        } else {
            cfg.deliver_policy = .all;
        }
        std.debug.print("{s}: tail consumer {s} from seq {d}\n", .{ stream, cname, cfg.opt_start_seq orelse 0 });
        const sub = try self.t.js.pullSubscribe(null, cname, .{ .stream = stream, .config = cfg, .inbox = shared });
        sub.max_bytes = pull_max_bytes; // §10jc: the drain's own requests too (see `tailInbox`)
        // §10jc: two requests outstanding — the next 8 MB downloads while this client
        // applies the last batch (a phone's catch-up alternated the two: 91.8 s for a
        // 1.3M-row first seed where the link alone would take about half).
        sub.pull_depth = 2;
        return sub;
    }

    /// §10ja: is a jump in stream sequence past `pos` a hole? Only if the stream no longer
    /// holds the message right after it. A consumer filtered to this client's tables
    /// (§10gm) is handed the next message that MATCHES, so on a tenant stream shared by
    /// several tables every write to a table it does not follow is a jump — read as a
    /// prune until 2026-09-25, and answered with a re-seed: a client following
    /// test_types_v7 re-fetched 3M rows three times because test_types_v4 got a burst.
    /// Stream info unreadable → a hole: the conservative answer, as before.
    /// The drain's end: the position a caught-up consumer may move to (§10ja).
    fn caughtUpTo(self: *SyncClient, stream: []const u8, consumer: []const u8, pos: u64) u64 {
        return switch (self.tailHealth(stream, consumer, pos, 0)) {
            .fine => |to| to,
            else => pos,
        };
    }

    /// §10ja: where the first HOLE opens in a batch — the index of the first message
    /// whose predecessor (the position, for the first one) is not the message right
    /// before it, AND the stream no longer holds what followed that predecessor. One
    /// stream-info request per batch with a jump; a filtered consumer's jumps over other
    /// tables' messages (§10gm) still hold in the stream and are not holes. A stream that
    /// prunes while the server delivers a batch of 100 opens a hole INSIDE the batch as
    /// easily as before it: checking the first message alone, libzb applied 100 messages
    /// spanning 259 sequences and moved its position past the 159 it never saw — 41 of
    /// 120 batches lost with a healthy-looking tail (the firehose at 100k events/s).
    /// zb-client-ts checks every message; so does this now.
    fn firstHole(self: *SyncClient, stream: []const u8, position: u64, msgs: []const *@import("nats").JetStreamMessage) ?usize {
        var prev = position;
        var first_seq: ?u64 = null;
        for (msgs, 0..) |m, i| {
            const sq = m.metadata.sequence.stream;
            if (prev > 0 and sq > prev + 1) {
                if (first_seq == null) first_seq = self.streamFirst(stream) orelse std.math.maxInt(u64); // unreadable: a hole, as prunedAfter
                if (first_seq.? > prev + 1) return i;
            }
            prev = sq;
        }
        return null;
    }

    fn streamFirst(self: *SyncClient, stream: []const u8) ?u64 {
        var info = self.t.js.getStreamInfo(stream) catch return null;
        defer info.deinit();
        return info.value.state.first_seq;
    }

    /// §10jc: deliveries the server counts as outstanding on `consumer`. Everything this
    /// client received is acked after its COMMIT, so once it has nothing in hand this is
    /// what was lost in transit. Unreadable: 0 (the drain ends as before).
    fn unackedOn(self: *SyncClient, stream: []const u8, consumer: []const u8) u64 {
        var ci = self.t.js.getConsumerInfo(stream, consumer) catch return 0;
        defer ci.deinit();
        return ci.value.num_ack_pending;
    }

    fn prunedAfter(self: *SyncClient, stream: []const u8, pos: u64) bool {
        var info = self.t.js.getStreamInfo(stream) catch return true;
        defer info.deinit();
        return info.value.state.first_seq > pos + 1;
    }

    /// §10gm: the drain, with the gap rule the live tail already had. A stream that
    /// prunes faster than this client applies leaves the drain reading the oldest message
    /// the server still holds, and the server says nothing — measured at 100k events a
    /// second: 3.8M of 17M rows, no error, no re-seed. A gap here is the same answer as in
    /// the tail (§10ei): re-seed the tables routed to this stream from their chains, which
    /// moves the position to their cutoff, and drain again. Bounded: a client slower than
    /// the stream's retention would loop for ever, so after three tries the drain leaves
    /// the rest to the live tail, which takes the gap on its own terms and says so.
    fn drainStream(self: *SyncClient, stream: []const u8) !void {
        var attempt: u8 = 0;
        while (true) {
            if (!try self.drainStreamOnce(stream)) return;
            attempt += 1;
            if (attempt >= 3) {
                std.debug.print("{s}: pruned under the drain three times — this client applies slower than the stream prunes; the live tail continues from the chain's cutoff\n", .{stream});
                return;
            }
            std.debug.print("{s}: re-seeding the tables routed to it, then draining again ({d}/3)\n", .{ stream, attempt });
            try self.gapAndSeed(null, null);
        }
    }

    /// One pass of the drain. True when the stream pruned under it (the caller re-seeds).
    fn drainStreamOnce(self: *SyncClient, stream: []const u8) !bool {
        const last = try self.storedSeq(stream);
        var sub = try self.openConsumer(stream, 30 * std.time.ns_per_s, null);
        defer sub.deinit(); // the server reaps the consumer itself — see openConsumer

        // §10jc: recreations of the consumer after a lost delivery (bounded: a link losing
        // deliveries on every batch would otherwise drain for ever).
        var gap_reopens: u32 = 0;
        // ONE reopen if the consumer dies under us mid-drain (a 409 under 12
        // concurrent seeding clients, §10cq): the position is the client's, so a
        // fresh consumer resumes exactly where the last batch left off. A second
        // death is a real fault and propagates.
        var reopened = false;
        while (true) {
            // ⚠️ Only a TIMEOUT means "caught up". A closed connection or a slow
            // consumer used to break here too — and then persist the position, which
            // is how a network blip becomes a recorded claim to have read the tail.
            //
            // ⚠️ This arm was DEAD CODE until 2026-08-29 (§10bh): the unpatched
            // nats.zig `fetch` never returned `Timeout` — it handed back an empty batch
            // with the error parked in `batch.err`, and the length check below was the
            // only thing ending the drain. Against the patched `fetch`
            // (`nats.zig-fetch-early-return-503.patch`) an empty batch with a terminal
            // status IS a returned error, so this arm is the exit and the length check
            // is a belt for a partial-batch `fetch` that returns nothing (it cannot).
            var batch = sub.fetch(100, .{ .duration = .{ .raw = .fromMilliseconds(900), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => {
                    // The expiry: caught up to the tail — unless the server still counts
                    // deliveries this client never received (§10jc: all it received is
                    // acked). Those are lost in transit: recreate from the position.
                    if (gap_reopens < 50 and self.unackedOn(stream, sub.consumer_name) > 0) {
                        std.debug.print("{s}: drain idle with deliveries unacknowledged on {s} — lost in transit; recreating the consumer from position {d}\n", .{ stream, sub.consumer_name, try self.storedSeq(stream) });
                        gap_reopens += 1;
                        self.resetFlow(stream);
                        const fresh = try self.openConsumer(stream, 30 * std.time.ns_per_s, null);
                        sub.deinit();
                        sub = fresh;
                        continue;
                    }
                    break;
                },
                error.ConsumerSequenceMismatch, error.NoResponders => {
                    if (reopened) return err;
                    reopened = true;
                    const fresh = try self.openConsumer(stream, 30 * std.time.ns_per_s, null);
                    sub.deinit();
                    sub = fresh;
                    continue;
                },
                else => return err,
            };
            defer batch.deinit();
            if (batch.messages.len == 0) break;
            // The gap rule (§10ei), here too — on a jump, and only if the stream no longer
            // holds what follows the position (§10ja: a filtered consumer jumps over the
            // other tables' messages as a matter of course).
            // Every consecutive pair, not the first message alone (firstHole, §10ja): what
            // precedes the hole is applied, the position follows it, and the caller re-seeds.
            var ba = std.heap.ArenaAllocator.init(self.a);
            defer ba.deinit();
            // §10jc: duplicates out, a lost delivery flagged — before the hole scan, which
            // must not read a redelivered old message as the batch's predecessor.
            const keep = try self.admit(ba.allocator(), stream, sub.consumer_name, batch.messages);
            const pos = try self.storedSeq(stream);
            if (self.firstHole(stream, pos, keep)) |j| {
                var ms = pos;
                if (j > 0) _ = try self.applyBatch(null, stream, keep[0..j], pos, &ms, null);
                const at = try self.storedSeq(stream);
                const first_here = keep[j].metadata.sequence.stream;
                std.debug.print("{s}: {d} message(s) pruned under the drain (position {d}, delivered {d})\n", .{ stream, first_here - at - 1, at, first_here });
                return true;
            }
            var ms = pos;
            if (keep.len > 0) _ = try self.applyBatch(null, stream, keep, pos, &ms, null);
            if ((try self.flowFor(stream)).gap and gap_reopens < 50) {
                std.debug.print("{s}: recreating the drain's consumer from position {d} after a lost delivery\n", .{ stream, try self.storedSeq(stream) });
                gap_reopens += 1;
                self.resetFlow(stream);
                const fresh = try self.openConsumer(stream, 30 * std.time.ns_per_s, null);
                sub.deinit();
                sub = fresh;
            }
        }
        const at = try self.storedSeq(stream);
        const to = self.caughtUpTo(stream, sub.consumer_name, at);
        if (to > at) try self.persistSeq(stream, to);
        std.debug.print("{s}: drained to seq {d}\n", .{ stream, @max(to, at) });
        _ = last;
        return false;
    }

    /// One fetched batch through the gate → apply → hold → position path, shared by
    /// the bounded drain and the live tail. Returns the number of events offered to
    /// `applyEvent` (applied, gated or held — D1: all three ARE the position).
    fn applyBatch(self: *SyncClient, report_a: ?std.mem.Allocator, stream: []const u8, messages: []const *@import("nats").JetStreamMessage, last: u64, max_seq: *u64, changed_map: ?*std.StringArrayHashMapUnmanaged(void)) !usize {
        // Per-batch: every decoded event dies with the batch, except the FK-held
        // ones, which are held DURABLY in `_zbz_inbox` below (§10de finding 1).
        var ba = std.heap.ArenaAllocator.init(self.a);
        defer ba.deinit();
        const t_batch = msNow();
        if (trace_enabled) _ = self.st.cacheStats(); // reset: the counters below are this batch's

        // ONE transaction for the whole batch — the same lesson the TS client has
        // in writing: N autocommits each pay a full SQLite commit/fsync, one
        // transaction of N pays it once. Row-by-row, a swarm client applied ~7
        // events/s against a 19/s feed and fell 76 s behind by the audit (§10cq);
        // the position write rides the same transaction, so data and position
        // cannot disagree across a crash. Acks stay OUTSIDE, after COMMIT: an
        // acked-but-rolled-back batch would be lost, an unacked-but-committed one
        // merely redelivers into idempotent upserts.
        const Ctx = struct {
            client: *SyncClient,
            a: std.mem.Allocator,
            stream: []const u8,
            messages: []const *@import("nats").JetStreamMessage,
            last: u64,
            max_seq: *u64,
            offered: *usize,
            report_a: ?std.mem.Allocator,
            changed_map: ?*std.StringArrayHashMapUnmanaged(void),
            /// §10dg: FOREIGN KEY checks deferred to COMMIT for the whole batch (the TS
            /// client's `defer_foreign_keys`): a family inserted child-first, or a
            /// cascade's deletes arriving parent-first, lands as one unit with no hold at
            /// all. A COMMIT refused (a parent missing across BATCHES) rolls back and the
            /// caller replays with immediate checks, holding what cannot land.
            deferred: bool,
            fn apply(cx: @This(), st_: *storage.Storage) !void {
                if (st_.engine == .duckdb) return cx.client.applyBatchPlanned(cx, st_);
                if (cx.deferred) try st_.execSimple("PRAGMA defer_foreign_keys = ON;");
                for (cx.messages) |m| {
                    const seq = m.metadata.sequence.stream;
                    const doc = decodeMsgpack(cx.a, m.msg.data) catch continue;
                    const events: []const Value = if (doc == .array) doc.array.items else &.{doc};
                    for (events) |ev| {
                        if (ev != .object) continue;
                        const table = if (ev.object.get("table")) |v| (if (v == .string) v.string else continue) else continue;
                        if (cx.client.states.get(table) == null) continue;
                        cx.offered.* += 1;
                        var applied_here = true;
                        cx.client.applyEvent(table, ev, m.metadata.stream, seq) catch |err| switch (err) {
                            // An OOM here is a `try`, not a `catch {}`: a dropped hold is an
                            // event that is acked, positioned past, and never applied.
                            // Held DURABLY, in this batch's transaction (CLIENTS.md, §10de
                            // finding 1): the position below is persisted past this event,
                            // so the inbox is the only thing that remembers it.
                            error.FkHeld => {
                                applied_here = false;
                                try holdEvent(st_, cx.a, table, ev, "missing-parent");
                            },
                            error.SchemaBehind => {
                                applied_here = false;
                                try holdEvent(st_, cx.a, table, ev, "unknown-column");
                            },
                            // Anything else is an event acked, positioned past and never
                            // applied — it must at least be SAID. The SQLite text names the cause.
                            else => |e| {
                                applied_here = false;
                                // §10hf: DuckDB aborts the whole transaction on a failed
                                // statement — every later statement would fail too, and the
                                // batch would be acked with nothing applied. Hand it to the
                                // isolated replay instead.
                                if (st_.engine == .duckdb) {
                                    std.debug.print("{s}: event at seq {d} aborted the batch: {s} — duckdb: {s}\n", .{ table, seq, @errorName(e), st_.errMsg() });
                                    return e;
                                }
                                std.debug.print("{s}: event at seq {d} not applied: {s} — sqlite: {s}\n", .{ table, seq, @errorName(e), st_.errMsg() });
                            },
                        };
                        // The host's `changed_tables` (§10ee): only what was APPLIED — a
                        // held event changed nothing yet — and the name DUPED into the
                        // report's allocator: `table` lives in this batch's arena, which
                        // dies before the host reads the report.
                        if (applied_here) if (cx.report_a) |ra_| if (cx.changed_map) |cm| {
                            if (!cm.contains(table)) cm.put(ra_, ra_.dupe(u8, table) catch table, {}) catch {};
                        };
                    }
                    if (seq > cx.max_seq.*) cx.max_seq.* = seq;
                }
                try cx.client.positionAfter(cx.stream, cx.last, cx.max_seq.*, cx.messages);
            }
        };
        var offered: usize = 0;
        var ctx = Ctx{
            .client = self,
            .a = ba.allocator(),
            .stream = stream,
            .messages = messages,
            .last = last,
            .max_seq = max_seq,
            .offered = &offered,
            .report_a = report_a,
            .changed_map = changed_map,
            .deferred = true,
        };
        self.st.transaction(ctx, Ctx.apply) catch |err| {
            std.debug.print("{s}: batch of {d} message(s) refused as a unit ({s}: {s}) — replaying event by event, holding what cannot land\n", .{ stream, messages.len, @errorName(err), self.st.commitErr() });
            offered = 0;
            max_seq.* = last;
            ctx.deferred = false;
            self.st.transaction(ctx, Ctx.apply) catch |err2| {
                // §10hf: refused even with immediate checks. On DuckDB one bad row aborts
                // the transaction and takes every other row with it; the third pass is one
                // transaction per EVENT, so only the bad event is lost — and said.
                std.debug.print("{s}: batch refused again ({s}: {s}) — one transaction per event\n", .{ stream, @errorName(err2), self.st.commitErr() });
                offered = 0;
                max_seq.* = last;
                try self.applyBatchIsolated(report_a, stream, messages, max_seq, changed_map, &offered);
            };
        };
        for (messages) |m| m.ack() catch {};
        if (trace_enabled) {
            const cs = self.st.cacheStats();
            tr("{s}: batch of {d} message(s) [{d}..{d}], {d} event(s) applied in {d} ms — page cache: {d} hit, {d} miss, {d} written; peak RSS {d} MB", .{ stream, messages.len, if (messages.len > 0) messages[0].metadata.sequence.stream else 0, if (messages.len > 0) messages[messages.len - 1].metadata.sequence.stream else 0, offered, msNow() - t_batch, cs.hit, cs.miss, cs.write, maxRssMb() });
        }
        return offered;
    }

    /// §10hf: the batch one event per transaction — the TS client's `applyBatchIsolated`.
    /// A held event is held durably as in the batch path; a refused one is printed and
    /// skipped, never poisoning its neighbours. The position is persisted last.
    fn applyBatchIsolated(self: *SyncClient, report_a: ?std.mem.Allocator, stream: []const u8, messages: []const *@import("nats").JetStreamMessage, max_seq: *u64, changed_map: ?*std.StringArrayHashMapUnmanaged(void), offered: *usize) !void {
        const pos_before = max_seq.*;
        var ba = std.heap.ArenaAllocator.init(self.a);
        defer ba.deinit();
        const a = ba.allocator();
        const Outcome = enum { applied, held, refused };
        const One = struct {
            client: *SyncClient,
            a: std.mem.Allocator,
            table: []const u8,
            ev: Value,
            stream: []const u8,
            seq: u64,
            outcome: *Outcome,
            fn apply(cx: @This(), _: *storage.Storage) !void {
                // A hold is written OUTSIDE this transaction: on DuckDB the refused
                // statement has already aborted it, and an INSERT into the inbox here
                // would fail too — measured as 50 parent tombstones "refused alone" while
                // the engine kept reporting the FOREIGN KEY text (§10hh).
                try cx.client.applyEvent(cx.table, cx.ev, cx.stream, cx.seq);
            }
        };
        const Hold = struct {
            a: std.mem.Allocator,
            table: []const u8,
            ev: Value,
            reason: []const u8,
            fn apply(cx: @This(), st_: *storage.Storage) !void {
                try holdEvent(st_, cx.a, cx.table, cx.ev, cx.reason);
            }
        };
        var applied: usize = 0;
        var held: usize = 0;
        var refused: usize = 0;
        for (messages) |m| {
            const seq = m.metadata.sequence.stream;
            const doc = decodeMsgpack(a, m.msg.data) catch continue;
            const events: []const Value = if (doc == .array) doc.array.items else &.{doc};
            for (events) |ev| {
                if (ev != .object) continue;
                const table = if (ev.object.get("table")) |v| (if (v == .string) v.string else continue) else continue;
                if (self.states.get(table) == null) continue;
                offered.* += 1;
                var outcome: Outcome = .applied;
                const one = One{ .client = self, .a = a, .table = table, .ev = ev, .stream = m.metadata.stream, .seq = seq, .outcome = &outcome };
                self.st.transaction(one, One.apply) catch |err| {
                    const reason: ?[]const u8 = switch (err) {
                        error.FkHeld => "missing-parent",
                        error.SchemaBehind => "unknown-column",
                        else => null,
                    };
                    if (reason) |why| {
                        // Rolled back above; held durably in its own transaction, retried
                        // after the batch like every held event (§10dg).
                        self.st.transaction(Hold{ .a = a, .table = table, .ev = ev, .reason = why }, Hold.apply) catch |e2| {
                            refused += 1;
                            std.debug.print("{s}: event at seq {d} could not be held ({s}: {s}) — dropped\n", .{ table, seq, @errorName(e2), self.st.errMsg() });
                            continue;
                        };
                        outcome = .held;
                    } else {
                        refused += 1;
                        std.debug.print("{s}: event at seq {d} refused alone ({s}: {s}) — dropped\n", .{ table, seq, @errorName(err), self.st.errMsg() });
                        continue;
                    }
                };
                switch (outcome) {
                    .applied => {
                        applied += 1;
                        if (report_a) |ra_| if (changed_map) |cm| {
                            if (!cm.contains(table)) cm.put(ra_, ra_.dupe(u8, table) catch table, {}) catch {};
                        };
                    },
                    .held => held += 1,
                    .refused => refused += 1,
                }
            }
            if (seq > max_seq.*) max_seq.* = seq;
        }
        const Pos = struct {
            client: *SyncClient,
            stream: []const u8,
            last: u64,
            seq: u64,
            messages: []const *@import("nats").JetStreamMessage,
            fn apply(cx: @This(), _: *storage.Storage) !void {
                try cx.client.positionAfter(cx.stream, cx.last, cx.seq, cx.messages);
            }
        };
        try self.st.transaction(Pos{ .client = self, .stream = stream, .last = pos_before, .seq = max_seq.*, .messages = messages }, Pos.apply);
        std.debug.print("{s}: isolated replay of {d} message(s): {d} applied, {d} held, {d} refused\n", .{ stream, messages.len, applied, held, refused });
    }

    // ─── live tailing (§10bh): the host-driven poll ─────────────────────────

    /// The tail for `stream`, opened on first use.
    fn tailInbox(self: *SyncClient) !*@import("nats").PullInbox {
        if (self.tail_inbox) |ib| return ib;
        self.tail_inbox = try self.t.js.pullInbox();
        // §10jb: the inbox holds what the server sent and this thread has not fetched —
        // at most about two batches since nats.zig patch 20 requests only the deficit. The
        // library's 64 MB byte limit was one message from a drop at 75k events/s (~45 MB
        // held, 256 KB messages); past it a message is DROPPED, which reads as a gap and
        // costs a re-seed. A valve four times wider: reached only when something is wrong,
        // never in the steady state.
        self.tail_inbox.?.inbox_subscription.setPendingLimits(500_000, 256 * 1024 * 1024);
        // §10jc: and never ask the server for more than 8 MB per request — overlapping
        // requests over a slow link otherwise pass its 64 MB per-connection queue and the
        // server closes the connection (an iPhone on Wi-Fi: 66 disconnects, every
        // delivery in flight lost each time).
        self.tail_inbox.?.max_bytes = pull_max_bytes;
        return self.tail_inbox.?;
    }

    /// Set aside and not yet due for a retry.
    fn isDark(self: *SyncClient, stream: []const u8) bool {
        const d = self.dark.get(stream) orelse return false;
        return nowMillis() < d.retry_at_ms;
    }

    /// The stream could not be read: set it aside, say so once, back off. Its tail, if
    /// any, is closed — a consumer that cannot be re-opened would otherwise be handed
    /// to every fetch again.
    fn markDark(self: *SyncClient, stream: []const u8, err: anyerror) void {
        const now = nowMillis();
        if (self.dark.getPtr(stream)) |d| {
            d.backoff_ms = @min(d.backoff_ms * 2, dark_backoff_max_ms);
            d.retry_at_ms = now + d.backoff_ms;
        } else {
            const key = self.aa().dupe(u8, stream) catch return;
            self.dark.put(self.aa(), key, .{ .retry_at_ms = now + dark_backoff_first_ms, .backoff_ms = dark_backoff_first_ms }) catch return;
            std.debug.print("⚠️ {s}: unreadable ({s}) — set aside, the other streams go on; retried with backoff\n", .{ stream, @errorName(err) });
        }
        self.dropTail(stream);
    }

    /// Readable again: back into the fetch, and whatever it left unseeded is asked for.
    fn clearDark(self: *SyncClient, stream: []const u8) void {
        if (self.dark.fetchOrderedRemove(stream) != null) {
            std.debug.print("✅ {s}: readable again — back in the tail\n", .{stream});
            self.reseed_pending = true;
        }
    }

    fn dropTail(self: *SyncClient, stream: []const u8) void {
        var k: usize = 0;
        while (k < self.tails.items.len) {
            if (std.mem.eql(u8, self.tails.items[k].stream, stream)) {
                std.debug.print("{s}: tail dropped\n", .{stream});
                self.tails.items[k].sub.deinit();
                _ = self.tails.orderedRemove(k);
            } else k += 1;
        }
    }

    /// The tenants whose streams are set aside, for the host (a stream's tenant is its
    /// name past the prefix; CDC_PUBLIC's is the open tenant).
    fn unreadableTenants(self: *SyncClient, a: std.mem.Allocator) ![]const []const u8 {
        var out: std.ArrayListUnmanaged([]const u8) = .empty;
        for (self.dark.keys()) |stream| {
            const t = if (std.mem.eql(u8, stream, self.cdc_public)) self.open_tenant else if (std.mem.startsWith(u8, stream, self.cdc_prefix)) stream[self.cdc_prefix.len..] else stream;
            try out.append(a, try a.dupe(u8, t));
        }
        return out.items;
    }

    fn tailFor(self: *SyncClient, stream: []const u8) !*Tail {
        for (self.tails.items) |*t| {
            if (std.mem.eql(u8, t.stream, stream)) return t;
        }
        const sub = try self.openConsumer(stream, tail_inactive_ns, try self.tailInbox());
        errdefer sub.deinit();
        try self.tails.append(self.aa(), .{ .stream = stream, .sub = sub, .delivered_ms = nowMillis() });
        return &self.tails.items[self.tails.items.len - 1];
    }

    /// The consumer behind a tail is gone (reaped, or deleted): open a fresh one at
    /// the stored position. Positions are the client's, not the consumer's (D1), so
    /// nothing is lost — the new consumer starts where the replica actually is.
    fn reopenTail(self: *SyncClient, t: *Tail) !void {
        // OPEN-THEN-SWAP. Opening can fail — a JS API request mid-outage times out —
        // and deinit-first left `t.sub` DANGLING on exactly that failure: every later
        // poll handed the freed pointer to PullInbox.fetch, which refused it
        // (NotOnThisInbox) for the rest of the process — the silent tail wedge of
        // §10cp/§10cq, measured as one Timeout then 154 refusals while verdicts kept
        // flowing on the same connection. The old consumer needs no explicit delete:
        // the server reaps it via inactive_threshold.
        const fresh = try self.openConsumer(t.stream, tail_inactive_ns, try self.tailInbox());
        t.sub.deinit();
        t.sub = fresh;
        t.delivered_ms = nowMillis();
        t.watch_ms = 0;
    }

    /// What the server says about an idle tail's consumer (§10ja).
    const TailHealth = union(enum) {
        /// Caught up, or nothing to say: the position it may move to (unchanged: `pos`).
        fine: u64,
        /// Messages pending on the server for this consumer, and it delivered nothing
        /// for a watch interval: deaf.
        deaf: u64,
        /// The server no longer knows the consumer (reaped, or the stream went).
        gone,
    };

    fn tailHealth(self: *SyncClient, stream: []const u8, consumer: []const u8, pos: u64, idle_ms: i64) TailHealth {
        var si = self.t.js.getStreamInfo(stream) catch |e| {
            tr("{s}: idle check — stream info failed ({s})", .{ stream, @errorName(e) });
            return .{ .fine = pos };
        };
        defer si.deinit();
        const last = si.value.state.last_seq;
        var ci = self.t.js.getConsumerInfo(stream, consumer) catch |e| {
            tr("{s}: idle check — consumer {s} info failed ({s}): gone", .{ stream, consumer, @errorName(e) });
            return .gone;
        };
        defer ci.deinit();
        const c = ci.value;
        tr("{s}: idle {d} s, position {d}; server: first {d} last {d}; consumer {s}: pending {d}, ack_pending {d}, delivered {d} (stream seq {d})", .{ stream, @divTrunc(idle_ms, 1000), pos, si.value.state.first_seq, last, consumer, c.num_pending, c.num_ack_pending, c.delivered.consumer_seq, c.delivered.stream_seq });
        if (c.num_pending > 0 and idle_ms >= tail_watch_ms) return .{ .deaf = c.num_pending };
        // §10jc: nothing in hand, yet deliveries unacknowledged — lost in transit, and the
        // server re-sends them only after ack_wait, out of order. Reopen from the position.
        if (c.num_ack_pending > 0 and idle_ms >= tail_watch_ms) return .{ .deaf = c.num_ack_pending };
        if (last <= pos) return .{ .fine = pos };
        return .{ .fine = core.caughtUpPosition(pos, last, c.num_pending, c.num_ack_pending, c.delivered.consumer_seq, c.delivered.stream_seq) };
    }

    /// A request taken off a queue subscription and handed to the host: the id the host
    /// answers with, and the inbox to answer on. Both live in the client's allocator
    /// until `reply` (or `deinit`) frees them — a poll's arena is gone by then.
    const Pending = struct {
        id: u64,
        reply_subject: []const u8,
        /// §10hq: which tenant asked — an answer too large for one message goes to
        /// THAT tenant's bucket, which is the only one its asker may read.
        tenant: []const u8,
    };

    /// One question for the host, as `poll` reports it.
    pub const Request = struct {
        id: u64,
        tenant: []const u8,
        name: []const u8,
        payload: []const u8,
    };

    pub const PollReport = struct {
        applied: usize,
        settled: usize,
        changed_tables: []const []const u8,
        seeded: []const []const u8,
        /// §10fq: tenants whose streams are set aside (unreadable now, retried with
        /// backoff). A host that followed them by choice may `leave` them.
        unreadable: []const []const u8 = &.{},
        /// §10hp: questions that arrived on this client's `serve` subscriptions. Each
        /// must be answered with `reply(id, …)`; an unanswered one leaves its asker
        /// waiting out its own timeout.
        requests: []const Request = &.{},
    };

    /// One turn of the host's loop: wait up to `wait_ms` for CDC on the persistent
    /// tails, apply what arrived, retry the FK-held, then sweep verdicts without
    /// waiting. Blocks the calling thread only — the host owns the thread, which is
    /// the C-ABI-honest shape (§10bh). The wait is shared across the streams in turn;
    /// once anything arrives the remaining streams get a quick look, not the full wait.
    /// The server's own auth verdict, if the connection was terminally closed for
    /// one (§10cj). Consulted by the C ABI so a poll that fails on a dead socket is
    /// named "AuthExpired"/"AuthRevoked" — actionable — not a bare transport error.
    pub fn authError(self: *SyncClient) ?anyerror {
        return self.t.conn.lastAuthError();
    }

    /// §10hp: answer `query.<tenant>.<name>` for these tenants, in ONE queue group.
    /// Idempotent per call in the sense that it ADDS: calling it twice subscribes
    /// twice. Returns how many subjects this client now answers.
    ///
    /// A responder is a client that answers questions about its own replica. It needs
    /// no second connection and no thread of its own: the questions arrive on this
    /// client's socket and are handed to the host by `poll`, like everything else.
    pub fn serve(self: *SyncClient, tenants: []const []const u8, names: []const []const u8, queue: []const u8) !usize {
        const ca = self.aa();
        var subs: std.ArrayListUnmanaged(*@import("nats").Subscription) = .empty;
        try subs.appendSlice(ca, self.serve_subs);
        for (tenants) |tenant| {
            for (names) |name| {
                const subject = try std.fmt.allocPrint(ca, "{s}.{s}.{s}", .{ self.subject_query_prefix, tenant, name });
                try subs.append(ca, try self.t.queueSubscribeSync(subject, queue));
            }
        }
        self.serve_queue = try ca.dupe(u8, queue);
        self.serve_subs = try subs.toOwnedSlice(ca);
        return self.serve_subs.len;
    }

    /// Take what is waiting on the serve subscriptions, without blocking. Each becomes
    /// a `Request` for the host and a `Pending` here, holding the inbox to answer on.
    fn drainServe(self: *SyncClient, report_a: std.mem.Allocator, out: *std.ArrayListUnmanaged(Request)) !void {
        const now: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(0), .clock = .awake } };
        for (self.serve_subs) |sub| {
            while (true) {
                const msg = sub.nextMsgTimeout(now) catch break;
                defer msg.deinit();
                // No reply subject: nobody is waiting, so there is nothing to answer.
                const reply_subject = msg.reply orelse continue;
                const dot = std.mem.lastIndexOfScalar(u8, msg.subject, '.') orelse continue;
                const head = msg.subject[0..dot];
                const tenant_dot = std.mem.lastIndexOfScalar(u8, head, '.') orelse continue;
                const id = self.next_request_id;
                self.next_request_id += 1;
                try self.pending.append(self.a, .{
                    .id = id,
                    .reply_subject = try self.a.dupe(u8, reply_subject),
                    .tenant = try self.a.dupe(u8, head[tenant_dot + 1 ..]),
                });
                try out.append(report_a, .{
                    .id = id,
                    .tenant = try report_a.dupe(u8, head[tenant_dot + 1 ..]),
                    .name = try report_a.dupe(u8, msg.subject[dot + 1 ..]),
                    .payload = try report_a.dupe(u8, msg.data),
                });
            }
        }
    }

    /// §10hp: the answer to one request, on the asker's inbox. A core publish: no
    /// stream, no ack, no position. An id answered twice, or never seen, is an error.
    pub fn reply(self: *SyncClient, id: u64, answer: []const u8) !void {
        for (self.pending.items, 0..) |p, i| {
            if (p.id != id) continue;
            defer {
                self.a.free(p.reply_subject);
                self.a.free(p.tenant);
                _ = self.pending.orderedRemove(i);
            }
            // §10hq: an answer that fits goes as it is. One that does not becomes an
            // OBJECT in the asking tenant's bucket, and the reply is a small envelope
            // naming it — which the asking library resolves, so the host never sees the
            // difference. NATS caps a message near a megabyte; a service that answers
            // rows cannot promise to stay under it.
            var sa = std.heap.ArenaAllocator.init(self.a);
            defer sa.deinit();
            const a = sa.allocator();
            // §10ip: compressed FIRST, so the inline-or-object decision is made on what
            // actually travels. An answer is JSON or a zstd frame and nothing else, and
            // JSON never begins 0x28 — so the asking side needs no flag, just the magic
            // bytes it already checks for chain objects.
            const body = compressAnswer(a, answer) catch answer;
            if (body.len <= self.results_inline_max) {
                try self.t.publishCore(p.reply_subject, body);
                return;
            }
            const bucket = try std.fmt.allocPrint(a, "{s}{s}", .{ self.results_bucket_prefix, p.tenant });
            // Unique-enough per process: pid, a monotonic counter and the request id
            // (std.crypto.random wants an Io in 0.16; libc is already linked). The
            // object lives minutes and is read once.
            const Ctr = struct {
                var n = std.atomic.Value(u32).init(0);
            };
            const name = try std.fmt.allocPrint(a, "ans-{d}-{d}-{d}", .{ std.c.getpid(), Ctr.n.fetchAdd(1, .monotonic), id });
            try self.t.objectPutBytes(bucket, name, body, self.results_max_age_ns);
            const envelope = try std.fmt.allocPrint(a, "{{\"zb_object\":{{\"bucket\":\"{s}\",\"name\":\"{s}\",\"bytes\":{d}}}}}", .{ bucket, name, body.len });
            try self.t.publishCore(p.reply_subject, envelope);
            return;
        }
        return error.UnknownRequest;
    }

    /// The host's loop body. With `serve` subscriptions the questions are drained
    /// BEFORE the CDC wait and again after it, so `wait_ms` bounds how long a question
    /// can sit unseen: a responder polls with a short wait.
    pub fn poll(self: *SyncClient, report_a: std.mem.Allocator, wait_ms: u64) !PollReport {
        var requests: std.ArrayListUnmanaged(Request) = .empty;
        if (self.serve_subs.len > 0) try self.drainServe(report_a, &requests);
        var r = try self.pollInner(report_a, wait_ms);
        if (self.serve_subs.len > 0) {
            try self.drainServe(report_a, &requests);
            r.requests = requests.items;
        }
        return r;
    }

    fn pollInner(self: *SyncClient, report_a: std.mem.Allocator, wait_ms: u64) !PollReport {
        try self.refuseIfRevoked();

        var changed_map: std.StringArrayHashMapUnmanaged(void) = .empty;
        var seeded_map: std.StringArrayHashMapUnmanaged(void) = .empty;

        // Schema first: a row in a new shape must find its table already moved.
        self.drainSchemaWatch(report_a, &changed_map, &seeded_map) catch |err| std.debug.print("schema watch: {s}\n", .{@errorName(err)});
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const streams = try self.cdcStreams(ca.allocator());
        if (streams.len == 0) return .{ .applied = 0, .settled = 0, .changed_tables = changed_map.keys(), .seeded = seeded_map.keys() };

        // ONE wait over every stream (nats.zig `PullInbox`, §10bh): one pull per
        // tail into a shared inbox, and the first message from any of them ends the
        // wait. This replaced a 250 ms round-robin whose cost was not the average
        // slice but a FIXED one: the write always lands on CDC_<tenant>, every poll
        // began with CDC_PUBLIC's full idle slice, and the row waited 265 ± 1 ms on
        // every one of 20 runs. Idle now costs one pull per stream per `wait_ms`.
        const a = ca.allocator();
        // §10fq: one unreadable stream must not stall the others. A tail that cannot be
        // opened sets its stream aside instead of failing the poll; the fetch runs over
        // what is readable, in the same order as `live`.
        var live: std.ArrayListUnmanaged([]const u8) = .empty;
        var subs_list: std.ArrayListUnmanaged(*@import("nats").PullSubscription) = .empty;
        for (streams) |stream| {
            if (self.isDark(stream)) continue;
            const tl = self.tailFor(stream) catch |err| {
                self.markDark(stream, err);
                continue;
            };
            self.clearDark(stream);
            try live.append(a, stream);
            try subs_list.append(a, tl.sub);
        }
        const unreadable = try self.unreadableTenants(report_a);
        if (subs_list.items.len == 0) {
            // Everything is dark: the wait still costs `wait_ms` (spent on verdicts), so
            // a host's loop does not spin against the backoff.
            const settled0 = try self.drainVerdictsWith(wait_ms, 1);
            return .{ .applied = 0, .settled = settled0, .changed_tables = changed_map.keys(), .seeded = seeded_map.keys(), .unreadable = unreadable };
        }
        const subs = subs_list.items;
        const t: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(@intCast(@max(1, wait_ms))), .clock = .awake } };
        var applied: usize = 0;
        // §10jb: what the shared inbox already holds before this pull is requested — a
        // queue that climbs across polls is a pull requested on top of one still arriving.
        const q_msgs = (try self.tailInbox()).inbox_subscription.pending_msgs.load(.acquire);
        const q_bytes = (try self.tailInbox()).inbox_subscription.pending_bytes.load(.acquire);
        var mb = (try self.tailInbox()).fetch(subs, 100, t) catch |err| switch (err) {
            // EVERY tail's consumer is gone (a long pause past inactive_threshold):
            // re-open them all at the stored positions; the next poll reads. One that
            // cannot be re-opened is set aside rather than failing the rest.
            error.NoResponders => {
                tr("fetch → NoResponders: every tail's consumer is gone — reopening all", .{});
                for (live.items) |stream| {
                    const tl = self.tailFor(stream) catch |e| {
                        self.markDark(stream, e);
                        continue;
                    };
                    self.reopenTail(tl) catch |e| self.markDark(stream, e);
                }
                return .{ .applied = 0, .settled = try self.drainVerdictsWith(0, 1), .changed_tables = changed_map.keys(), .seeded = seeded_map.keys(), .unreadable = try self.unreadableTenants(report_a) };
            },
            else => return err,
        };
        defer mb.deinit();
        // A consumer that went away while the others answered: re-open it alone.
        // By NAME, not by index — `gone` is ordered like `live`. A re-open that fails
        // (the stream was deleted, the grant withdrawn) sets that stream aside.
        for (mb.gone, 0..) |g, i| {
            if (!g) continue;
            std.debug.print("{s}: tail consumer gone — reopening\n", .{live.items[i]});
            const tl = self.tailFor(live.items[i]) catch |e| {
                self.markDark(live.items[i], e);
                continue;
            };
            self.reopenTail(tl) catch |e| self.markDark(live.items[i], e);
        }
        // Messages come interleaved across streams; the applier positions per stream,
        // so group them (order within a stream is preserved).
        for (streams) |stream| {
            var mine: std.ArrayListUnmanaged(*@import("nats").JetStreamMessage) = .empty;
            for (mb.messages) |m| {
                if (std.mem.eql(u8, m.metadata.stream, stream)) try mine.append(a, m);
            }
            if (trace_enabled) {
                const lo: u64 = if (mine.items.len > 0) mine.items[0].metadata.sequence.stream else 0;
                const hi: u64 = if (mine.items.len > 0) mine.items[mine.items.len - 1].metadata.sequence.stream else 0;
                var bytes: usize = 0;
                for (mine.items) |m| bytes += m.msg.data.len;
                tr("{s}: fetched {d} msg(s) [{d}..{d}] of {d} in the batch ({d} KiB), position {d}; inbox held {d} msg(s) / {d} KiB before the pull", .{ stream, mine.items.len, lo, hi, mb.messages.len, bytes / 1024, try self.storedSeq(stream), q_msgs, q_bytes / 1024 });
            }
            if (mine.items.len == 0) {
                // Idle this poll: now and then, ask the server about the tail (§10ja) —
                // caught up (the position moves to the stream's end), gone, or deaf.
                if (self.isDark(stream)) continue;
                const tl = self.tailFor(stream) catch continue;
                const now = nowMillis();
                if (now - tl.watch_ms < tail_watch_ms) continue;
                tl.watch_ms = now;
                const pos = try self.storedSeq(stream);
                switch (self.tailHealth(stream, tl.sub.consumer_name, pos, now - tl.delivered_ms)) {
                    .fine => |to| if (to > pos) try self.persistSeq(stream, to),
                    .deaf => |pending| {
                        std.debug.print("{s}: tail deaf — {d} message(s) pending on the server for {s}, nothing delivered for {d} s (position {d}); reopening\n", .{ stream, pending, tl.sub.consumer_name, @divTrunc(now - tl.delivered_ms, 1000), pos });
                        self.reopenTail(tl) catch |e| self.markDark(stream, e);
                    },
                    .gone => {
                        std.debug.print("{s}: tail consumer {s} gone from the server (position {d}) — reopening\n", .{ stream, tl.sub.consumer_name, pos });
                        self.reopenTail(tl) catch |e| self.markDark(stream, e);
                    },
                }
                continue;
            }
            if (self.tailFor(stream)) |tl| tl.delivered_ms = nowMillis() else |_| {}
            var last = try self.storedSeq(stream);
            // §10jc: duplicates out and a lost delivery flagged, before anything else.
            const cons_name: []const u8 = if (self.tailFor(stream)) |t2| t2.sub.consumer_name else |_| "";
            const admitted = try self.admit(a, stream, cons_name, mine.items);
            // §10ei: the gap rule, LIVE. The next message this tail is handed is `last + 1`
            // unless the stream pruned under the consumer while the host did not poll —
            // or (§10ja) the consumer is filtered to this client's tables and skipped
            // other tables' messages, which is no hole at all: `firstHole` asks the
            // stream which of the two a jump is, for EVERY consecutive pair of the batch
            // (a stream pruning under a delivery opens holes inside it too). The server
            // continues from the oldest it holds and says nothing; the numbering does.
            // Measured: a client idle 267 s against a 263 s window resumed past twelve
            // messages, two deletes among them, and disagreed with PostgreSQL until it
            // was reopened. A hole is §7's gap taken now: what precedes it is applied,
            // then re-seed what rides this stream, then the rest — the seed gate drops
            // what the chain already carries. A chain that predates the stream leaves the
            // hole OPEN: the position stays below it, the rest is not acked, and the next
            // poll asks again.
            var pending: []const *@import("nats").JetStreamMessage = admitted;
            while (pending.len > 0) {
                const cut = self.firstHole(stream, last, pending);
                if (cut) |j| if (j > 0) {
                    // Contiguous up to the hole: applied first, the position follows.
                    var ms: u64 = last;
                    const gb = self.gated;
                    const off = try self.applyBatch(report_a, stream, pending[0..j], last, &ms, &changed_map);
                    applied += off;
                    tr("{s}: offered {d} event(s) before a hole, gated {d}, position {d} → {d}", .{ stream, off, self.gated - gb, last, ms });
                    last = ms;
                    pending = pending[j..];
                    continue;
                };
                if (cut != null) {
                    const first_here = pending[0].metadata.sequence.stream;
                    std.debug.print("{s}: {d} message(s) pruned under the live consumer (position {d}, delivered {d}) — taking the gap: re-seeding the tables routed to it\n", .{ stream, first_here - last - 1, last, first_here });
                    self.reseed_pending = false;
                    self.gapAndSeed(report_a, &seeded_map) catch |err| std.debug.print("{s}: re-seed after the hole failed: {s}\n", .{ stream, @errorName(err) });
                    if (self.reseed_pending) {
                        std.debug.print("{s}: the hole stays open — waiting for the producer's next generation before moving past it\n", .{stream});
                        break;
                    }
                    // §10ja: the batch in hand was fetched BEFORE this re-seed, and the chain
                    // just applied is newer than all of it — the heal moved the position to
                    // the stream's oldest message, which under a firehose is far past this
                    // batch (a 108 s re-seed at 100k: position 10175 → 18136, batch 11098..
                    // 11197). Applied, its old events land over the newer chain and its max
                    // seq overwrites the healed position — backwards (11197) — so the next
                    // message reads as a hole: re-seed, again, for ever. What the chain covers
                    // is acked, not applied; zb-client-ts drops the batch in hand the same way.
                    const healed = try self.storedSeq(stream);
                    if (healed > last) {
                        var beyond: std.ArrayListUnmanaged(*@import("nats").JetStreamMessage) = .empty;
                        for (pending) |m| {
                            if (m.metadata.sequence.stream > healed) try beyond.append(a, m) else m.ack() catch {};
                        }
                        pending = beyond.items;
                        last = healed;
                        continue; // the rest, scanned again
                    }
                }
                var max_seq = last;
                const gated_before = self.gated;
                const offered = try self.applyBatch(report_a, stream, pending, last, &max_seq, &changed_map);
                applied += offered;
                tr("{s}: offered {d} event(s), gated {d}, position {d} → {d}", .{ stream, offered, self.gated - gated_before, last, max_seq });
                break;
            }
            // §10jc: a delivery lost — this tail is closed; the next poll reopens it from the
            // position and the server re-sends from the lost message on, in order.
            if ((try self.flowFor(stream)).gap) {
                std.debug.print("{s}: reopening the tail from position {d} after a lost delivery\n", .{ stream, try self.storedSeq(stream) });
                self.dropTail(stream);
                self.resetFlow(stream);
            }
        }
        if (applied > 0) self.retryHeld(report_a, &changed_map);
        self.drainRebase();
        const settled = try self.drainVerdictsWith(0, 1);
        self.heartbeatIfDue() catch |err| std.debug.print("heartbeat: {s}\n", .{@errorName(err)});
        return .{ .applied = applied, .settled = settled, .changed_tables = changed_map.keys(), .seeded = seeded_map.keys(), .unreadable = try self.unreadableTenants(report_a) };
    }

    fn applyEvent(self: *SyncClient, table: []const u8, ev: Value, stream: []const u8, seq: u64) !void {
        const st = self.states.get(table).?;
        // The seed gate (findings 7 + 10) — seq primary on the event's own stream,
        // seed-anchored lsn fallback (the lowest anchor: the strictest floor that is
        // still inside every chain applied).
        if (seq != 0) {
            for (st.anchors.items) |an| if (an.seq > 0 and std.mem.eql(u8, an.stream, stream)) {
                if (seq <= an.seq) {
                    self.gated += 1;
                    return;
                }
            };
        } else if (st.anchors.items.len > 0) {
            var floor: i64 = std.math.maxInt(i64);
            for (st.anchors.items) |an| floor = @min(floor, an.lsn);
            const lsn: i64 = if (ev.object.get("lsn")) |v| (if (v == .integer) v.integer else 0) else 0;
            if (lsn != 0 and lsn < floor) {
                self.gated += 1;
                return;
            }
        }

        const op = if (ev.object.get("operation")) |v| (if (v == .string) v.string else "") else "";
        var data = ev.object.get("data") orelse return;
        if (data != .object) return;

        // A column this table does not have yet: the row is newer than the schema
        // (a migration whose descriptor has not reached us). Held — durably, in the
        // inbox — never dropped, never upserted into the wrong shape (the TS client's
        // unknown-column hold, CLIENTS.md divergence 2).
        if (!std.mem.eql(u8, op, "DELETE")) {
            var kit = data.object.iterator();
            while (kit.next()) |e| {
                const k = e.key_ptr.*;
                if (std.mem.startsWith(u8, k, "old.")) continue;
                const known = for (st.cols) |c| {
                    if (std.mem.eql(u8, c, k)) break true;
                } else false;
                if (!known) return error.SchemaBehind;
            }
        }

        var ta = std.heap.ArenaAllocator.init(self.a);
        defer ta.deinit();
        const taa = ta.allocator();
        try self.pgArrayFixup(taa, st, &data);

        // ⚠️ The HLC floor (§7.2, NOTES §10q), fed from every arriving row's version
        // column — OBSERVED remote versions, never our own stamps. This field was
        // declared, read by `hlcVersion` and never written (found by the 2026-08-29
        // NOTES reread): a lagging clock could stamp under a row this replica had
        // already seen, the exact case the floor exists to prevent. A fixed buffer, not
        // an arena dupe, for the same reason as `last_version`.
        try self.feedFloor(taa, st, data);

        // A hard DELETE (tables without a tombstone column) and a soft delete (an
        // update that SETS the tombstone, §7.5) end the same way here: the row goes.
        if (std.mem.eql(u8, op, "DELETE") or tombstoned(st, data)) {
            if (try core.planDelete(taa, table, st.pk, data)) |stp| {
                // A parent's DELETE ahead of its children's (a cascade split across
                // batches) is HELD like a child ahead of its parent, and lands on retry.
                _ = self.stepExec(taa, stp) catch |err| {
                    if (err == storage.Error.StepFailed and self.fkRefused()) return error.FkHeld;
                    return err;
                };
            }
            // §10dg: whatever was held for this key (an INSERT waiting for its parent)
            // must not replay after the row's DELETE went by — it would resurrect it.
            pruneInboxKey(&self.st, taa, table, st.pk, data) catch {};
            return;
        }
        if (try core.planKeyChange(taa, table, st.pk, data)) |kc| _ = try self.stepExec(taa, kc);
        const up = try self.updateOrUpsert(taa, table, st, op, data);
        _ = self.stepExec(taa, up) catch |err| {
            if (err == storage.Error.StepFailed and self.fkRefused()) return error.FkHeld;
            return err;
        };
        // §10do: a row arriving may be the winner a held edit waits for.
        if (self.rebase.count() > 0) self.rebase_due = true;
    }

    /// §10hh: did the last statement fail on a FOREIGN KEY? Each engine says it in its own
    /// words — SQLite "FOREIGN KEY constraint failed", DuckDB "Violates foreign key
    /// constraint because key … is still referenced", PostgreSQL "violates foreign key
    /// constraint". Matching SQLite's capitals alone let DuckDB DROP a parent's tombstone
    /// that arrived ahead of its children's (measured: 50 closed stations kept, §10hh);
    /// recognised, it is HELD and lands on the retry, as §10dg designed.
    fn fkRefused(self: *SyncClient) bool {
        return std.ascii.indexOfIgnoreCase(self.st.errMsg(), "foreign key") != null;
    }

    /// §10q: the HLC floor, fed from every arriving row's version column — observed
    /// remote versions, never our own stamps. Per event, on both apply paths.
    fn feedFloor(self: *SyncClient, a: std.mem.Allocator, st: TableState, data: Value) !void {
        if (data != .object) return;
        if (st.version_col) |vc| if (data.object.get(vc)) |sv| if (sv == .string) {
            const wire = try core.pgTsToWire(a, sv.string);
            const norm = try core.normalizeVersion(a, wire);
            const floor = core.maxVersion(self.seen_floor, norm);
            if (floor.ptr != self.seen_floor.ptr and floor.len <= self.seen_floor_buf.len) {
                @memcpy(self.seen_floor_buf[0..floor.len], floor);
                self.seen_floor = self.seen_floor_buf[0..floor.len];
            }
        };
    }

    /// §10hg: a CDC batch on DuckDB, planned. Row-at-a-time upserts exhaust DuckDB's memory
    /// (an in-transaction UPDATE keeps an undo copy per column vector: ~39k of them hit the
    /// limit, §10hf), so a `bulk` segment of `core.planCdcBulk` goes the way the chain does
    /// on this engine — appended into a temp table, then ONE set-based upsert, no version
    /// guard (the stream's order is the truth; a key repeated in the segment keeps its LAST
    /// row, since DuckDB refuses to update one row twice in a statement). `single`s take
    /// `applyEvent`; a refused one aborts the batch for the isolated replay.
    fn applyBatchPlanned(self: *SyncClient, cx: anytype, st_: *storage.Storage) !void {
        const a = cx.a;
        const Ev = struct { table: []const u8, ev: Value, seq: u64, stream: []const u8 };
        var evs: std.ArrayListUnmanaged(Ev) = .empty;
        var tables_v: std.json.ObjectMap = .empty;
        var events_v = std.json.Array.init(a);
        for (cx.messages) |m| {
            const seq = m.metadata.sequence.stream;
            const doc = decodeMsgpack(a, m.msg.data) catch continue;
            const items: []const Value = if (doc == .array) doc.array.items else &.{doc};
            for (items) |ev| {
                if (ev != .object) continue;
                const table = if (ev.object.get("table")) |v| (if (v == .string) v.string else continue) else continue;
                const st = self.states.get(table) orelse continue;
                if (tables_v.get(table) == null) {
                    var tv: std.json.ObjectMap = .empty;
                    var cols = std.json.Array.init(a);
                    for (st.cols) |cn| try cols.append(.{ .string = cn });
                    var pk = std.json.Array.init(a);
                    for (st.pk) |cn| try pk.append(.{ .string = cn });
                    try tv.put(a, "columns", .{ .array = cols });
                    try tv.put(a, "pkCols", .{ .array = pk });
                    try tv.put(a, "tombstoneColumn", if (st.tombstone_col) |tc| Value{ .string = tc } else .null);
                    // The gate's anchor on this batch's stream (the planner takes one; libzb
                    // keeps one per stream): the highest seq a chain of this table was cut at.
                    var gate: u64 = 0;
                    for (st.anchors.items) |an| if (std.mem.eql(u8, an.stream, m.metadata.stream) and an.seq > gate) {
                        gate = an.seq;
                    };
                    if (gate > 0) {
                        var av: std.json.ObjectMap = .empty;
                        try av.put(a, "seedSeq", .{ .integer = @intCast(gate) });
                        try av.put(a, "seedStream", .{ .string = m.metadata.stream });
                        try tv.put(a, "anchor", .{ .object = av });
                    }
                    // Bytes (PostGIS) stay row by row, as on SQLite.
                    if (st.geom_cols.len > 0) {
                        var bc = std.json.Array.init(a);
                        for (st.geom_cols) |cn| try bc.append(.{ .string = cn });
                        try tv.put(a, "blobCols", .{ .array = bc });
                    }
                    try tables_v.put(a, table, .{ .object = tv });
                }
                // The engine's value shaping (vectors as text) before the rows are cut.
                var ev_m = ev;
                if (ev_m.object.getPtr("data")) |dp| try self.pgArrayFixup(a, st, dp);
                var pv: std.json.ObjectMap = .empty;
                try pv.put(a, "table", .{ .string = table });
                try pv.put(a, "operation", ev_m.object.get("operation") orelse .null);
                try pv.put(a, "data", ev_m.object.get("data") orelse .null);
                try pv.put(a, "seq", .{ .integer = @intCast(seq) });
                try pv.put(a, "stream", .{ .string = m.metadata.stream });
                try pv.put(a, "lsn", ev_m.object.get("lsn") orelse .null);
                try events_v.append(.{ .object = pv });
                try evs.append(a, .{ .table = table, .ev = ev_m, .seq = seq, .stream = m.metadata.stream });
            }
            if (seq > cx.max_seq.*) cx.max_seq.* = seq;
        }
        const segments = try core.planCdcBulk(a, "duckdb", .{ .object = tables_v }, .{ .array = events_v });
        var bulked: usize = 0;
        var statements: usize = 0;
        var singles: usize = 0;
        for (segments.array.items) |seg| {
            const kind = seg.object.get("kind").?.string;
            if (std.mem.eql(u8, kind, "drop")) {
                cx.offered.* += 1;
                continue;
            }
            if (std.mem.eql(u8, kind, "single")) {
                const i: usize = @intCast(seg.object.get("event").?.integer);
                const e = evs.items[i];
                cx.offered.* += 1;
                singles += 1;
                var applied_here = true;
                self.applyEvent(e.table, e.ev, e.stream, e.seq) catch |err| switch (err) {
                    error.FkHeld => {
                        applied_here = false;
                        try holdEvent(st_, a, e.table, e.ev, "missing-parent");
                    },
                    error.SchemaBehind => {
                        applied_here = false;
                        try holdEvent(st_, a, e.table, e.ev, "unknown-column");
                    },
                    else => |err2| {
                        std.debug.print("{s}: event at seq {d} aborted the batch: {s} — duckdb: {s}\n", .{ e.table, e.seq, @errorName(err2), st_.errMsg() });
                        return err2;
                    },
                };
                if (applied_here) if (cx.report_a) |ra_| if (cx.changed_map) |cm| {
                    if (!cm.contains(e.table)) cm.put(ra_, ra_.dupe(u8, e.table) catch e.table, {}) catch {};
                };
                continue;
            }
            // bulk
            const table = seg.object.get("table").?.string;
            const st = self.states.get(table).?;
            const cols = try core.strArrPub(a, seg.object.get("cols").?.array);
            const rows_v = seg.object.get("rows").?.array.items;
            const ev_idx = seg.object.get("events").?.array.items;
            // Keys de-duplicated: the last occurrence's row stays, in the first's slot.
            var pk_idx = try a.alloc(usize, st.pk.len);
            for (st.pk, 0..) |pc, k| {
                pk_idx[k] = for (cols, 0..) |cn, ci| {
                    if (std.mem.eql(u8, cn, pc)) break ci;
                } else return error.ChainObjectMalformed;
            }
            var slot: std.StringArrayHashMapUnmanaged(usize) = .empty;
            var rows: std.ArrayListUnmanaged([]const storage.Value) = .empty;
            for (rows_v) |rv| {
                const cells = rv.array.items;
                var key: std.ArrayListUnmanaged(u8) = .empty;
                for (pk_idx) |ci| {
                    try key.appendSlice(a, try core.valueToString(a, cells[ci]));
                    try key.append(a, 0x1f);
                }
                const params = try a.alloc(storage.Value, cols.len);
                for (params, 0..) |*p, ci| p.* = if (ci < cells.len) try jsonToStorage(a, cells[ci]) else .null;
                if (slot.get(key.items)) |at| {
                    rows.items[at] = params;
                } else {
                    try slot.put(a, key.items, rows.items.len);
                    try rows.append(a, params);
                }
            }
            const col_list = try core.quotedJoin(a, cols);
            try st_.execSimple(try std.fmt.allocPrint(a, "CREATE OR REPLACE TEMP TABLE _zbz_copy AS SELECT {s} FROM {s} LIMIT 0", .{ col_list, table }));
            try st_.dkAppend("_zbz_copy", rows.items);
            try st_.execSimple(try core.pgUpsertFromCopySql(a, table, cols, st.pk, null));
            try st_.execSimple("DROP TABLE _zbz_copy");
            statements += 1;
            bulked += ev_idx.len;
            for (ev_idx) |iv| {
                const e = evs.items[@intCast(iv.integer)];
                cx.offered.* += 1;
                if (e.ev.object.get("data")) |dv| try self.feedFloor(a, st, dv);
            }
            if (cx.report_a) |ra_| if (cx.changed_map) |cm| {
                if (!cm.contains(table)) cm.put(ra_, ra_.dupe(u8, table) catch table, {}) catch {};
            };
            if (self.rebase.count() > 0) self.rebase_due = true;
        }
        if (bulked > 0 or singles > 0) self.bulk_stats.add(bulked, statements, singles);
        try self.positionAfter(cx.stream, cx.last, cx.max_seq.*, cx.messages);
    }

    /// An UPDATE for a row that is HERE is applied as an UPDATE (core.planUpdate —
    /// only the columns sent), never as the upsert whose INSERT arm fails NOT NULL on
    /// a partial payload before the conflict is resolved (§7's asymmetry; measured in
    /// the soak as a bare StepFailed on a four-field UPDATE). A row that is not here,
    /// or an INSERT, takes the upsert. The probe is core.planExists.
    fn updateOrUpsert(self: *SyncClient, a: std.mem.Allocator, table: []const u8, st: TableState, op: []const u8, data: Value) !Value {
        if (std.mem.eql(u8, op, "UPDATE")) {
            if (try core.planExists(a, table, st.pk, data)) |ex| {
                const hit = self.stepExec(a, ex) catch &.{};
                if (hit.len > 0) {
                    if (try core.planUpdate(a, table, st.pk, data)) |upd| return upd;
                }
            }
        }
        return core.planUpsert(a, table, st.pk, data);
    }

    // ── §10hj: ask a service, keep its answer ───────────────────────────────

    /// One request/reply on the card's own connection: `subject` is a query subject the
    /// principal may publish to (`query.<tenant>.<name>`), `payload` the question, the
    /// reply's bytes come back allocated from `a` — JSON by convention, but bytes here.
    pub fn request(self: *SyncClient, a: std.mem.Allocator, subject: []const u8, payload: []const u8, timeout_ms: u64) ![]u8 {
        const t: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(@intCast(@max(1, timeout_ms))), .clock = .awake } };
        const t0 = msNow();
        const msg = try self.t.conn.request(subject, payload, t);
        defer msg.deinit();
        const wire_ms = msNow() - t0;
        // §10hq: an answer too large for one message arrives as a small envelope naming
        // an object; read it and hand back the answer itself. The host asked a question
        // and gets an answer, whichever way it travelled.
        // §10hu: …and is TOLD which way, on the same clock either way. Without this a
        // host times the two paths differently and reads the difference as the cost of
        // the data rather than the cost of the fetch.
        if (try self.objectEnvelope(a, msg.data)) |fetched| {
            const fetch_ms = msNow() - t0 - wire_ms;
            // §10ip: `maybeZstd` returns its input untouched when the bytes are not a
            // frame, so an uncompressed answer costs one magic-byte check.
            const body = try maybeZstd(a, fetched);
            return try spliceTransport(a, body, "object", fetched.len, wire_ms, fetch_ms);
        }
        const body = try maybeZstd(a, msg.data);
        return try spliceTransport(a, body, "inline", msg.data.len, wire_ms, 0);
    }

    /// How the answer travelled, spliced into the answer itself as `zb_transport`:
    /// `via` ("inline" or "object"), `bytes` (the answer's own size, not the reply's),
    /// `wire_ms` (ask → reply) and `fetch_ms` (0 inline, the object read otherwise).
    /// ⚠️ TEXTUAL, not parse-then-reprint: re-serialising a megabyte to add four
    /// numbers would cost more than the fetch it reports and spoil the measurement.
    /// A body that is not a JSON object travels unchanged — a service may answer with
    /// anything, and this must never corrupt it.
    fn spliceTransport(a: std.mem.Allocator, body: []const u8, via: []const u8, bytes: usize, wire_ms: i64, fetch_ms: i64) ![]u8 {
        if (body.len == 0 or body[0] != '{') return try a.dupe(u8, body);
        const head = try std.fmt.allocPrint(a, "{{\"zb_transport\":{{\"via\":\"{s}\",\"bytes\":{d},\"wire_ms\":{d},\"fetch_ms\":{d}}}", .{ via, bytes, wire_ms, fetch_ms });
        // `{}` takes no comma; `{"a":1}` does.
        var i: usize = 1;
        while (i < body.len and std.ascii.isWhitespace(body[i])) i += 1;
        const empty = i >= body.len or body[i] == '}';
        const out = try a.alloc(u8, head.len + @as(usize, if (empty) 0 else 1) + (body.len - 1));
        @memcpy(out[0..head.len], head);
        var n = head.len;
        if (!empty) {
            out[n] = ',';
            n += 1;
        }
        @memcpy(out[n..][0 .. body.len - 1], body[1..]);
        return out;
    }

    /// `{"zb_object":{"bucket","name","bytes"}}` → the object's bytes; null when the
    /// reply is an ordinary answer. Anything malformed is treated as an ordinary
    /// answer: a service is free to send a body that happens to have one of these keys.
    fn objectEnvelope(self: *SyncClient, a: std.mem.Allocator, body: []const u8) !?[]u8 {
        if (body.len == 0 or body[0] != '{') return null;
        if (std.mem.indexOf(u8, body, "\"zb_object\"") == null) return null;
        var sa = std.heap.ArenaAllocator.init(self.a);
        defer sa.deinit();
        const parsed = std.json.parseFromSlice(Value, sa.allocator(), body, .{}) catch return null;
        if (parsed.value != .object) return null;
        const env = parsed.value.object.get("zb_object") orelse return null;
        if (env != .object) return null;
        const bucket = env.object.get("bucket") orelse return null;
        const name = env.object.get("name") orelse return null;
        if (bucket != .string or name != .string) return null;
        return try self.t.objectGetBytes(a, bucket.string, name.string);
    }

    /// A service's answer into a table, `{"columns":[…],"rows":[[…],…]}` — the chain
    /// object's own shape — through the chain's version-guarded upsert (a fresher row
    /// wins, an unconfirmed local edit is not overwritten). `scope` names the area the
    /// answer is authoritative for: rows of the table inside it that the answer did not
    /// carry are gone upstream and are deleted here (`{"where": "lat BETWEEN ? AND ? …",
    /// "params": […]}`); null keeps every local row. Returns rows applied.
    pub fn ingest(self: *SyncClient, a: std.mem.Allocator, table: []const u8, answer: Value, scope: ?Value) !usize {
        const st = self.states.get(table) orelse return error.UnknownTable;
        if (answer != .object) return error.Malformed;
        const cols = try core.strArrPub(a, (answer.object.get("columns") orelse return error.Malformed).array);
        const rows = (answer.object.get("rows") orelse return error.Malformed).array.items;
        for (cols) |cn| {
            const known = for (st.cols) |c| {
                if (std.mem.eql(u8, c, cn)) break true;
            } else false;
            if (!known) return error.SchemaBehind;
        }
        const vcol: ?[]const u8 = if (st.version_col) |vc| (for (cols) |cn| {
            if (std.mem.eql(u8, cn, vc)) break vc;
        } else null) else null;
        const Ctx = struct {
            client: *SyncClient,
            a: std.mem.Allocator,
            table: []const u8,
            st: TableState,
            cols: []const []const u8,
            rows: []const Value,
            vcol: ?[]const u8,
            scope: ?Value,
            applied: *usize,
            fn apply(cx: @This(), st_: *storage.Storage) !void {
                // The engine's text is gone once the wrapper rolls back: say it here.
                errdefer std.debug.print("ingest {s}: refused — {s}\n", .{ cx.table, st_.errMsg() });
                const sql = try core.chainUpsertSql(cx.a, cx.table, cx.cols, cx.st.pk, cx.vcol);
                var keys_seen: std.ArrayListUnmanaged(storage.Value) = .empty;
                const pk_idx: ?usize = if (cx.st.pk.len == 1) (for (cx.cols, 0..) |cn, i| {
                    if (std.mem.eql(u8, cn, cx.st.pk[0])) break i;
                } else null) else null;
                for (cx.rows) |rv| {
                    if (rv != .array) continue;
                    const cells = rv.array.items;
                    const params = try cx.a.alloc(storage.Value, cx.cols.len);
                    for (params, 0..) |*p, i| p.* = if (i < cells.len) try chainCellToStorage(cx.a, cells[i]) else .null;
                    _ = try st_.query(cx.a, sql, params);
                    if (pk_idx) |pi| if (pi < params.len) try keys_seen.append(cx.a, params[pi]);
                    cx.applied.* += 1;
                }
                // The scope: what the answer did not carry inside it is gone.
                if (cx.scope) |sc| if (sc == .object) if (sc.object.get("where")) |w| if (w == .string) if (pk_idx != null) {
                    try st_.execSimple("CREATE TEMP TABLE IF NOT EXISTS _zbz_seen (k)");
                    try st_.execSimple("DELETE FROM _zbz_seen");
                    for (keys_seen.items) |k| _ = try st_.query(cx.a, "INSERT INTO _zbz_seen (k) VALUES (?)", &.{k});
                    const pv = sc.object.get("params") orelse Value.null;
                    const n: usize = if (pv == .array) pv.array.items.len else 0;
                    const params = try cx.a.alloc(storage.Value, n);
                    if (pv == .array) for (pv.array.items, 0..) |v, i| {
                        params[i] = try jsonToStorage(cx.a, v);
                    };
                    const del = try std.fmt.allocPrint(cx.a, "DELETE FROM {s} WHERE ({s}) AND \"{s}\" NOT IN (SELECT k FROM _zbz_seen)", .{ cx.table, w.string, cx.st.pk[0] });
                    _ = try st_.query(cx.a, del, params);
                    try st_.execSimple("DELETE FROM _zbz_seen");
                };
            }
        };
        var applied: usize = 0;
        try self.st.transaction(Ctx{ .client = self, .a = a, .table = table, .st = st, .cols = cols, .rows = rows, .vcol = vcol, .scope = scope, .applied = &applied }, Ctx.apply);
        return applied;
    }

    // ── the write path (PROTOCOL.md §7.1) ───────────────────────────────────

    /// The outbox: what makes this a queue rather than a log. An entry leaves only on
    /// a definitive verdict — never on a send failure, which is why a flush is safe to
    /// repeat and why the original msg_id must survive a restart.
    pub fn ensureOutbox(self: *SyncClient) !void {
        try execDdl(&self.st,
            \\CREATE TABLE IF NOT EXISTS _zebridge_outbox (
            \\  msg_id     TEXT PRIMARY KEY,
            \\  subject    TEXT NOT NULL,
            \\  payload    TEXT NOT NULL,
            \\  tbl        TEXT NOT NULL,
            \\  row_id     TEXT NOT NULL,
            \\  before     TEXT,
            \\  created_at INTEGER NOT NULL,
            \\  attempts   INTEGER NOT NULL DEFAULT 0
            \\)
        );
    }

    /// The GC watermark as THIS replica sees it — the one row of
    /// `zebridge_gc_watermark`, which arrives over CDC like any other table. null
    /// whenever the answer is not known (not replicated here, no row yet, read
    /// failed), and `core.outboxWatermarkGate` treats null as "refuse nothing".
    pub fn gcWatermark(self: *SyncClient, a: std.mem.Allocator) ?[]const u8 {
        const rows = self.st.query(a, "SELECT watermark FROM zebridge_gc_watermark LIMIT 1", &.{}) catch return null;
        if (rows.len == 0 or rows[0].len == 0) return null;
        const v = rows[0][0];
        return if (v == .text and v.text.len > 0) v.text else null;
    }

    /// One edit: stamp it, apply it locally, queue it, send it.
    ///
    /// The order is the contract. The optimistic apply and the outbox row are written
    /// BEFORE the publish, so a crash between them leaves a queued write that the next
    /// flush repeats — never a write that was sent but not remembered.
    ///
    /// `result_a` owns the returned msg_id and nothing else: everything this call
    /// builds — envelope, before-image, JSON, msgpack — is per-call and freed on return.
    pub fn mutate(self: *SyncClient, result_a: std.mem.Allocator, table: []const u8, op: []const u8, key: Value, values: ?Value) ![]const u8 {
        return self.mutateAt(result_a, table, op, key, values, null);
    }

    /// `mutate` with the caller's own stamp (the TypeScript client's `opts.version`):
    /// a host that keeps its own clock, or a test modelling a slow one. The HLC only
    /// advances: a stamp above it is followed, one below it is sent and forgotten.
    pub fn mutateAt(self: *SyncClient, result_a: std.mem.Allocator, table: []const u8, op: []const u8, key: Value, values: ?Value, stamp: ?[]const u8) ![]const u8 {
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const a = ca.allocator();
        const st = self.states.get(table) orelse return error.UnknownTable;
        try self.ensureOutbox();

        const version = stamp orelse try core.hlcVersion(a, try nowWireIso(a), self.last_version, self.seen_floor);
        if (version.len > self.last_version_buf.len) return error.VersionTooLong;
        // The clock only moves forward: a caller's stamp from the past is sent as
        // given but never becomes what the next unstamped write follows.
        if (stamp == null or std.mem.order(u8, version, self.last_version) == .gt) {
            @memcpy(self.last_version_buf[0..version.len], version);
            self.last_version = self.last_version_buf[0..version.len];
        }

        var args: std.json.ObjectMap = .empty;
        try args.put(a, "principal", .{ .string = self.opts.principal });
        try args.put(a, "clientId", .{ .string = self.opts.client_id });
        try args.put(a, "table", .{ .string = table });
        try args.put(a, "op", .{ .string = op });
        try args.put(a, "version", .{ .string = version });
        try args.put(a, "key", key);
        if (values) |v| try args.put(a, "values", v);
        var pk_arr = std.json.Array.init(a);
        for (st.pk) |c| try pk_arr.append(.{ .string = c });
        try args.put(a, "pkCols", .{ .array = pk_arr });
        try args.put(a, "mutationsPrefix", .{ .string = self.subject_mutations_prefix });

        const env = try core.buildMutation(a, .{ .object = args });
        const subject = env.object.get("subject").?.string;
        const msg_id = env.object.get("msgId").?.string;
        const row_id = env.object.get("id").?.string;
        const payload = env.object.get("payload").?;

        // The before-image, for the revert a refusal or a rejection needs. Captured
        // BEFORE the optimistic apply overwrites it, obviously.
        const before = try self.beforeImage(a, table, st, key);

        // NON-FATAL, matching the TS client (its failed optimistic upsert is a log
        // line and the write still queues): the payload is valid ON THE WIRE — the
        // bridge builds the UPDATE from key + data — so a local echo that cannot
        // apply (an UPDATE whose values do not repeat the pk fails the upsert arm's
        // NOT NULL) must not abort the queueing. Aborting here LOST every such
        // update: 24 per client per swarm smoke, audible but wrong (§10cp). The
        // CDC echo applies the row properly moments later.
        // §10dx: the optimistic row carries the write's own stamp in the version column
        // (the ingress sets it from `version`); without it a NOT NULL version column
        // refused every optimistic INSERT locally.
        var opt = env.object.get("optimistic").?;
        if (st.version_col) |vc| {
            if (opt == .object and !std.mem.eql(u8, op, "DELETE")) {
                if (opt.object.get("data")) |d| {
                    if (d == .object and d.object.get(vc) == null) {
                        var stamped = d;
                        try stamped.object.put(a, vc, .{ .string = version });
                        try opt.object.put(a, "data", stamped);
                    }
                }
            }
        }
        self.applyOptimistic(table, st, opt) catch {};

        const payload_json = try core.valueToString(a, payload);
        const before_json: storage.Value = if (before) |b| .{ .text = try core.valueToString(a, b) } else .null;
        _ = try self.st.query(a,
            \\INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, before, created_at, attempts)
            \\VALUES (?,?,?,?,?,?,?,0)
            \\ON CONFLICT(msg_id) DO UPDATE SET attempts = _zebridge_outbox.attempts + 1
        , &.{
            .{ .text = msg_id },         .{ .text = subject }, .{ .text = payload_json },
            .{ .text = table },          .{ .text = row_id },  before_json,
            .{ .integer = nowMillis() },
        });

        // ⚠️ Sent through flushOutbox, NOT published directly from here.
        //
        // Publishing here was a hole the first live test walked straight into: the
        // watermark gate lives in the flush, so a write's FIRST send skipped it and
        // only replays were checked. Measured — a write the gate then refused was
        // already in PostgreSQL, printed as "flushed: 0" next to a row that existed.
        //
        // One path for every send closes it. The cost is that a flush republishes any
        // stragglers too, which is what an outbox is for, and the msg_id makes it
        // idempotent.
        _ = self.flushOutbox() catch |err| {
            // Kept, not lost: the entry is durable and the next flush retries.
            std.debug.print("mutate: send failed ({any}) — queued as {s}\n", .{ err, msg_id });
        };
        return try result_a.dupe(u8, msg_id);
    }

    /// Republish everything still queued — and refuse what the GC watermark has
    /// outlived (§10at). Returns the number actually sent.
    ///
    /// A publish failure is logged and the entry stays queued; the pass continues so
    /// one bad send does not starve the rest. If NOTHING could be sent the last error
    /// is returned — "flushed: 0" next to a broker that is down should say why.
    pub fn flushOutbox(self: *SyncClient) !usize {
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const a = ca.allocator();
        try self.ensureOutbox();
        const rows = try self.st.query(a, "SELECT msg_id, subject, payload, tbl, row_id, before FROM _zebridge_outbox ORDER BY created_at", &.{});
        if (rows.len == 0) return 0;
        if (nowMillis() < self.hold_until_ms) return 0; // §10fk: rate limited — not yet

        // ⚠️ Gated ONCE, before the first publish — one read of the watermark for the
        // whole pass, and an entry that must not be sent is not sent even if an
        // earlier publish throws.
        const watermark = self.gcWatermark(a);
        var entries = std.json.Array.init(a);
        for (rows) |r| {
            var e: std.json.ObjectMap = .empty;
            try e.put(a, "msgId", .{ .string = if (r[0] == .text) r[0].text else "" });
            const env = if (r[2] == .text) parseStoredJson(a, r[2].text) catch Value.null else Value.null;
            const ver = if (env == .object) env.object.get("version") else null;
            try e.put(a, "version", if (ver) |v| v else .null);
            try entries.append(.{ .object = e });
        }
        const gate = try core.outboxWatermarkGate(a, entries, watermark);

        var refused: std.StringArrayHashMapUnmanaged(void) = .empty;
        for (gate.object.get("refuse").?.array.items) |v| try refused.put(a, v.string, {});
        // §10el: the grace — the bridge answers in milliseconds and its ack wait is ten
        // seconds; five leaves the subscription its turn and still replays a straggler
        // well inside one poll of a slow host.
        const grace_ms: i64 = 5_000;
        const now_ms = nowMillis();

        var sent: usize = 0;
        var failed: usize = 0;
        var last_err: ?anyerror = null;
        var collected: usize = 0;
        for (rows) |r| {
            const msg_id = if (r[0] == .text) r[0].text else continue;
            if (self.sent_at.get(msg_id)) |t| if (now_ms - t < grace_ms) continue;
            // The verdicts this client MISSED (PROTOCOL §7.4b): one per-key direct get per
            // pending entry, settled through the same handler `drainVerdicts` uses. Until
            // 2026-08-29 nothing read a stored verdict — the replay earned a fresh one,
            // at the cost of a second PostgreSQL attempt and, past the duplicate window,
            // a `stale` for a write that had been accepted.
            if (!refused.contains(msg_id)) {
                const ack_subject = try std.fmt.allocPrint(a, "{s}.{s}.{s}", .{ self.subject_mutation_ack_prefix, self.opts.principal, msg_id });
                if (self.t.lastBySubject(self.stream_mutations, ack_subject) catch null) |vm| {
                    defer vm.deinit();
                    if (try self.settleVerdict(a, msg_id, vm.data)) {
                        collected += 1;
                        continue;
                    }
                }
            }
            if (refused.contains(msg_id)) {
                // Exactly a `rejected` verdict's handling, because that is what it is:
                // this write will never be sent, so the optimistic copy is a
                // divergence. Restore and SAY SO — the user's edit is being dropped.
                try self.revertOptimistic(a, r);
                _ = try self.st.query(a, "DELETE FROM _zebridge_outbox WHERE msg_id = ?", &.{.{ .text = msg_id }});
                std.debug.print(
                    "outbox: {s}[{s}] {s} predates the GC watermark ({s}) and CANNOT be sent — " ++
                        "its tombstone was reaped, so sending it would resurrect a deleted row " ++
                        "(PROTOCOL MUST 6). Local copy reverted; this edit is lost.\n",
                    .{ if (r[3] == .text) r[3].text else "?", if (r[4] == .text) r[4].text else "?", msg_id, watermark orelse "?" },
                );
                continue;
            }
            const env = if (r[2] == .text) try parseStoredJson(a, r[2].text) else Value.null;
            self.publishEnvelope(a, r[1].text, env, msg_id) catch |err| {
                failed += 1;
                last_err = err;
                std.debug.print("outbox: {s} not sent ({any}) — still queued\n", .{ msg_id, err });
                continue;
            };
            sent += 1;
            if (self.sent_at.getPtr(msg_id)) |slot| {
                slot.* = now_ms;
            } else if (self.a.dupe(u8, msg_id)) |key| {
                self.sent_at.put(self.a, key, now_ms) catch self.a.free(key);
            } else |_| {}
        }
        if (collected > 0) std.debug.print("outbox: collected {d} verdict(s) published while this client was away — settled without replay\n", .{collected});
        if (sent == 0 and failed > 0) return last_err.?;
        return sent;
    }

    /// One verdict, live or collected: true when definitive (the outbox entry is
    /// settled one way or another), false for `failed` (kept for retry).
    fn settleVerdict(self: *SyncClient, a: std.mem.Allocator, mid: []const u8, data: []const u8) !bool {
        // §10el: settled or not, this entry's send time has served; a `failed` verdict
        // keeps the entry queued and the next flush past the grace replays it.
        if (self.sent_at.fetchRemove(mid)) |kv| self.a.free(kv.key);
        const v = parseStoredJson(a, data) catch return false;
        const status = if (v == .object) (if (v.object.get("status")) |x| (if (x == .string) x.string else "") else "") else "";
        const reason0 = if (v == .object) (if (v.object.get("reason")) |x| (if (x == .string) x.string else "") else "") else "";
        // §10fk: rate limited — keep the write, hold the outbox for what the verdict says.
        if (std.mem.eql(u8, status, "failed") and std.mem.eql(u8, reason0, "rate_limited")) {
            const wait: i64 = if (v.object.get("retry_after_ms")) |x| (if (x == .integer) x.integer else 1000) else 1000;
            self.hold_until_ms = @max(self.hold_until_ms, nowMillis() + wait);
            self.verdict_counts.rate_limited += 1;
            return false;
        }
        self.verdict_counts.count(status);
        if (!std.mem.eql(u8, status, "accepted")) {
            // Say it: a refusal that only the counters knew about was found by a run
            // whose every INSERT was rejected in silence (§10dx).
            const reason = reason0;
            const detail = if (v == .object) (if (v.object.get("detail")) |x| (if (x == .string) x.string else "") else "") else "";
            std.debug.print("libzb: verdict {s} for {s}{s}{s}{s}{s}\n", .{ status, mid, if (reason.len > 0) " — " else "", reason, if (detail.len > 0) ": " else "", detail });
        }
        if (std.mem.eql(u8, status, "failed")) return false;
        if (std.mem.eql(u8, status, "rejected") or std.mem.eql(u8, status, "row_deleted")) {
            const rows = try self.st.query(a, "SELECT msg_id, subject, payload, tbl, row_id, before FROM _zebridge_outbox WHERE msg_id = ?", &.{.{ .text = mid }});
            // §10dy: `rejected` restores the before-image (nothing changed server-side);
            // `row_deleted` REMOVES the local row — the server's word is "deleted", and
            // restoring a before-image resurrected, on the emitter's own replica, a row
            // it had itself deleted a tick later (the TS client always deleted here).
            if (rows.len == 1) {
                if (std.mem.eql(u8, status, "row_deleted")) try self.removeOptimistic(a, rows[0]) else try self.revertOptimistic(a, rows[0]);
            }
        }
        // `stale` does not revert: the authoritative row is already on its way over
        // CDC, and restoring a before-image could undo a newer value. §10do: the edit
        // is kept aside instead — resubmitted if its columns are disjoint from the
        // winner's, dropped and surfaced otherwise (see `tryRebase`).
        if (std.mem.eql(u8, status, "stale")) {
            self.holdForRebase(a, mid) catch |err| std.debug.print("libzb: rebase hold for {s} failed: {any}\n", .{ mid, err });
        }
        _ = try self.st.query(a, "DELETE FROM _zebridge_outbox WHERE msg_id = ?", &.{.{ .text = mid }});
        return true;
    }

    // ─── §10do: rebase on stale ───────────────────────────────────────────────

    /// A `stale` verdict names an UPDATE that lost to a newer version. Its columns
    /// are not necessarily the columns the winner changed: when the two sets are
    /// disjoint the edit is still right and is resubmitted with a fresh stamp (the
    /// HLC floor puts it above the winner); when they overlap it is dropped and
    /// surfaced — LWW's word stands. Only an UPDATE qualifies; read BEFORE the
    /// outbox row goes.
    fn holdForRebase(self: *SyncClient, a: std.mem.Allocator, mid: []const u8) !void {
        const rows = try self.st.query(a, "SELECT subject, payload, tbl, before FROM _zebridge_outbox WHERE msg_id = ?", &.{.{ .text = mid }});
        if (rows.len != 1) return;
        const r = rows[0];
        if (r[0] != .text or r[1] != .text or r[2] != .text) return;
        const dot = std.mem.lastIndexOfScalar(u8, r[0].text, '.') orelse return;
        if (!std.mem.eql(u8, r[0].text[dot + 1 ..], "update")) return;
        const env = try parseStoredJson(a, r[1].text);
        if (env != .object) return;
        const key = env.object.get("key") orelse return;
        const data = env.object.get("data") orelse return;
        const ver = env.object.get("version") orelse return;
        if (key != .object or data != .object or ver != .string) return;
        const la = self.aa();
        try self.rebase.put(la, try la.dupe(u8, mid), .{
            .table = try la.dupe(u8, r[2].text),
            .key = try la.dupe(u8, try core.valueToString(a, key)),
            .values = try la.dupe(u8, try core.valueToString(a, data)),
            .before = if (r[3] == .text) try la.dupe(u8, r[3].text) else null,
            .version = try la.dupe(u8, try core.normalizeVersion(a, ver.string)),
        });
        self.rebase_due = true;
    }

    /// Runs at the end of poll() and flush(), outside any apply transaction: the
    /// resubmit is a full mutate() (outbox + optimistic apply), sent by the host's
    /// next flush like any other write.
    fn drainRebase(self: *SyncClient) void {
        if (!self.rebase_due or self.rebase.count() == 0) return;
        self.rebase_due = false;
        var ta = std.heap.ArenaAllocator.init(self.a);
        defer ta.deinit();
        var i: usize = 0;
        while (i < self.rebase.count()) {
            const mid = self.rebase.keys()[i];
            const done = self.tryRebase(ta.allocator(), mid) catch |err| blk: {
                std.debug.print("libzb: rebase of {s} deferred: {any}\n", .{ mid, err });
                self.rebase_due = true;
                break :blk false;
            };
            if (done) _ = self.rebase.swapRemoveAt(i) else i += 1;
        }
    }

    /// True once the entry is settled (rebased or dropped); false while the winning
    /// row is not here yet — it is, when its version is above the refused stamp.
    fn tryRebase(self: *SyncClient, a: std.mem.Allocator, mid: []const u8) !bool {
        const e = self.rebase.get(mid) orelse return true;
        const st = self.states.get(e.table) orelse return true;
        const vcol = st.version_col orelse return true;
        const key = try parseStoredJson(a, e.key);
        const cur = (try self.beforeImage(a, e.table, st, key)) orelse {
            std.debug.print("libzb: rebase of {s} abandoned: the row is gone\n", .{mid});
            return true;
        };
        if (cur != .object) return true;
        const cur_ver = if (cur.object.get(vcol)) |v| (if (v == .string) try core.normalizeVersion(a, v.string) else "") else "";
        if (std.mem.order(u8, cur_ver, e.version) != .gt) return false;
        const values = try parseStoredJson(a, e.values);
        const before: ?Value = if (e.before) |b| try parseStoredJson(a, b) else null;
        var mine: std.ArrayList(u8) = .empty;
        var overlap: std.ArrayList(u8) = .empty;
        for (values.object.keys()) |c| {
            if (std.mem.eql(u8, c, vcol)) continue;
            if (mine.items.len > 0) try mine.appendSlice(a, ", ");
            try mine.appendSlice(a, c);
            // The winner changed c when the row here holds neither the pre-write value
            // nor mine: the CDC row overwrote my optimistic copy with something else.
            const winner_changed = if (before) |b|
                (b != .object or (!sameJson(a, cur.object.get(c), b.object.get(c)) and !sameJson(a, cur.object.get(c), values.object.get(c))))
            else
                true;
            if (winner_changed) {
                if (overlap.items.len > 0) try overlap.appendSlice(a, ", ");
                try overlap.appendSlice(a, c);
            }
        }
        if (overlap.items.len > 0) {
            std.debug.print("libzb: {s} edit LOST to a newer version on the same column(s) {s} — the winning row stands; surface this to the user\n", .{ e.table, overlap.items });
            return true;
        }
        const ver = try self.mutate(a, e.table, "UPDATE", key, values);
        std.debug.print("libzb: rebased {s} of {s} onto the newer row as {s}\n", .{ mine.items, e.table, ver });
        return true;
    }

    /// Pop entries whose fate is settled. `applied` is confirmation; `rejected` and
    /// `row_deleted` are refusals that also undo the optimistic copy; `failed` is
    /// retryable and deliberately left queued.
    /// Open the verdict channel. Call BEFORE the first write — see the field comment.
    ///
    /// ⚠️ Core, not JetStream, which bounds what this can promise: verdicts are also
    /// STORED in the MUTATIONS stream precisely so a client that was offline when its
    /// write was judged can collect the verdict on reconnect (§7.1). A core
    /// subscription cannot do that — it only sees what arrives while it is open. A
    /// JetStream consumer filtered to this subject is the durable form, and the
    /// follow-up; this is correct for a client that stays connected.
    pub fn subscribeVerdicts(self: *SyncClient) !void {
        if (self.verdicts != null) return;
        const subject = try std.fmt.allocPrint(self.aa(), "{s}.{s}.>", .{ self.subject_mutation_ack_prefix, self.opts.principal });
        self.verdicts = try self.t.subscribeSync(subject);
    }

    /// `wait_ms` is a BUDGET for the first verdict, not a per-message timeout. A
    /// verdict costs the bridge a PostgreSQL round trip, so a single 250 ms wait gave
    /// up before the answer existed — measured: 0 of 1 settled while the row was
    /// already in PostgreSQL. Once messages start arriving the loop drains them all
    /// and stops at the first gap.
    ///
    /// The GAP follows the budget, capped at 250 ms (§10ef). It was a fixed 250 ms,
    /// so `flush(0)` — the loop's "send what is queued, take what is there" turn —
    /// blocked a quarter second after the last verdict, every turn: a 5 ms sting
    /// ran at four ticks a second, and the Flutter worker's turn cost 350 ms against
    /// the 100 ms the README promised. A short budget now means a short gap; what a
    /// 1 ms gap leaves behind, the next poll's sweep takes.
    pub fn drainVerdicts(self: *SyncClient, wait_ms: u64) !usize {
        return self.drainVerdictsWith(wait_ms, @min(250, @max(1, wait_ms)));
    }

    /// `per_msg_ms` is the wait for EACH next message; `poll` passes 1 so a sweep with
    /// nothing pending costs a millisecond, not the 250 ms a settle wait affords.
    fn drainVerdictsWith(self: *SyncClient, wait_ms: u64, per_msg_ms: u64) !usize {
        try self.subscribeVerdicts();
        const sub = self.verdicts.?;
        var settled: usize = 0;
        const started = nowMillis();
        while (true) {
            if (settled == 0 and @as(u64, @intCast(nowMillis() - started)) > wait_ms) break;
            // The bridge's own idiom (nats_publisher.zig): a bounded wait, so a
            // drain with nothing pending returns instead of blocking forever.
            // Only a timeout ends the drain quietly; a closed connection is an error.
            const msg = sub.nextMsgTimeout(
                .{ .duration = .{ .raw = .fromMilliseconds(@intCast(per_msg_ms)), .clock = .awake } },
            ) catch |err| switch (err) {
                error.Timeout => break,
                else => return err,
            };
            // ⚠️ The message is OURS: `nextMsgTimeout` hands over a `*Message` and
            // nats.zig frees nothing on our behalf. Without this, every verdict leaked
            // its arena (or its pool slot) — invisible to `std.testing.allocator`
            // because no unit test drains a verdict, and to the soak because nothing
            // measured RSS across it.
            defer msg.deinit();
            // Per-verdict arena: the parsed JSON and the outbox row are dead once the
            // entry is popped.
            var va = std.heap.ArenaAllocator.init(self.a);
            defer va.deinit();
            const a = va.allocator();
            // ⚠️ The msg id is in the SUBJECT, not the body — `mutation_ack.<principal>.<msg_id>`
            // — for the same reason a mutation carries no `table` field: NATS authorizes
            // subjects and not payloads, so identity belongs where the broker can see it.
            // Reading it from the body found nothing and silently settled zero verdicts
            // while the row was already in PostgreSQL (measured).
            const mid = blk: {
                var last: []const u8 = "";
                var it = std.mem.splitScalar(u8, msg.subject, '.');
                while (it.next()) |part| last = part;
                break :blk last;
            };
            if (mid.len == 0) continue;
            if (std.mem.eql(u8, mid, "revoked")) return self.hangUpRevoked();

            // ⚠️ JSON, not msgpack. CDC and mutations are msgpack; a verdict is JSON —
            // it is read by humans and by `nats sub` as often as by a client. The wire
            // names are the bridge's: accepted / stale / row_deleted / rejected settle
            // the entry (the last two revert the optimistic copy); `failed` stays queued.
            if (try self.settleVerdict(a, mid, msg.data)) settled += 1;
        }
        return settled;
    }

    /// §10dm: the ban, acted on — the socket dropped now, the flag set for every later
    /// call. Cooperative: this library obeys; the JWT revocation is the enforcement.
    fn hangUpRevoked(self: *SyncClient) error{Revoked} {
        self.revoked = true;
        std.debug.print("{s}: REVOKED by the operator (mutation_ack.{s}.revoked) — hanging up now; every later call answers Revoked. The local rows stay; the wipe is the application's (zb_client_wipe).\n", .{ self.opts.principal, self.opts.principal });
        // The socket is dropped at the NEXT entry point, not here: this runs inside the
        // verdict drain, over a subscription the close would free under the loop's feet
        // (measured: SIGSEGV in the host right after the ban).
        return error.Revoked;
    }

    /// Every entry point: once revoked, drop the socket (once) and answer Revoked.
    fn refuseIfRevoked(self: *SyncClient) !void {
        if (!self.revoked) return;
        if (!self.hung_up) {
            self.hung_up = true;
            self.t.conn.close();
        }
        return error.Revoked;
    }

    /// At connect: a ban published while this client was away is retained on the
    /// MUTATIONS stream — one direct get, before anything is read.
    fn probeRevoked(self: *SyncClient) !void {
        var buf: [256]u8 = undefined;
        const subject = try std.fmt.bufPrint(&buf, "{s}.{s}.revoked", .{ self.subject_mutation_ack_prefix, self.opts.principal });
        if (self.t.lastBySubject(self.stream_mutations, subject) catch null) |m| {
            m.deinit();
            return self.hangUpRevoked();
        }
    }

    /// The row as it stands, as a JSON object — or null when there is none.
    fn beforeImage(self: *SyncClient, a: std.mem.Allocator, table: []const u8, st: TableState, key: Value) !?Value {
        var where: std.ArrayList(u8) = .empty;
        var params = try a.alloc(storage.Value, st.pk.len);
        for (st.pk, 0..) |c, i| {
            if (i > 0) try where.appendSlice(a, " AND ");
            // ⚠️ No `.writer(a)` on an unmanaged ArrayList in 0.16 — allocPrint then
            // append, which is what the rest of this file does.
            try where.appendSlice(a, try std.fmt.allocPrint(a, "\"{s}\" = ?", .{c}));
            const kv = if (key == .object) key.object.get(c) orelse Value.null else Value.null;
            params[i] = try jsonToStorage(a, kv);
        }
        const sql = try std.fmt.allocPrint(a, "SELECT * FROM \"{s}\" WHERE {s}", .{ table, where.items });
        const rows = self.st.query(a, sql, params) catch return null;
        if (rows.len == 0) return null;
        var obj: std.json.ObjectMap = .empty;
        for (st.cols, 0..) |c, i| {
            if (i >= rows[0].len) break;
            try obj.put(a, c, try storageToJson(a, rows[0][i]));
        }
        return .{ .object = obj };
    }

    /// Apply this client's own edit locally, before the server has seen it.
    ///
    /// ⚠️ A partial payload fails here, and the message must say so. The local apply is
    /// an UPSERT, so SQLite evaluates the INSERT arm first — a payload missing a
    /// `NOT NULL` column violates it before the conflict is ever resolved, and the
    /// error is a bare `StepFailed` unless the SQLite text is carried out with it.
    /// This is PROTOCOL §7's asymmetry exactly: a partial payload succeeds on the
    /// update path and fails on the insert path, which is why the wire carries FULL
    /// rows.
    fn applyOptimistic(self: *SyncClient, table: []const u8, st: TableState, ev: Value) !void {
        var ta = std.heap.ArenaAllocator.init(self.a);
        defer ta.deinit();
        const taa = ta.allocator();
        const op = if (ev.object.get("operation")) |v| (if (v == .string) v.string else "") else "";
        var data = ev.object.get("data") orelse return;
        try self.pgArrayFixup(taa, st, &data);
        // The same §7.5 rule as the CDC path: our own edit that sets the tombstone
        // removes the row locally, exactly as the server's echo of it would.
        if (std.mem.eql(u8, op, "DELETE") or tombstoned(st, data)) {
            if (try core.planDelete(taa, table, st.pk, data)) |stp| _ = try self.stepExec(taa, stp);
            return;
        }
        const up = try self.updateOrUpsert(taa, table, st, op, data);
        _ = self.stepExec(taa, up) catch |err| {
            std.debug.print(
                "optimistic apply of {s} failed: {any} — sqlite: {s}\n",
                .{ table, err, self.st.errMsg() },
            );
            return err;
        };
    }

    /// Undo an optimistic apply from the stored before-image: restore it if there was
    /// a row, delete ours if there was not.
    /// The `row_deleted` revert: the row is gone upstream, so it goes here too.
    fn removeOptimistic(self: *SyncClient, a: std.mem.Allocator, r: storage.Row) !void {
        const table = if (r[3] == .text) r[3].text else return;
        const st = self.states.get(table) orelse return;
        const env = if (r[2] == .text) try parseStoredJson(a, r[2].text) else return;
        const key = if (env == .object) env.object.get("key") orelse return else return;
        if (try core.planDelete(a, table, st.pk, key)) |stp| _ = self.stepExec(a, stp) catch {};
    }

    fn revertOptimistic(self: *SyncClient, a: std.mem.Allocator, r: storage.Row) !void {
        const table = if (r[3] == .text) r[3].text else return;
        const st = self.states.get(table) orelse return;
        const env = if (r[2] == .text) try parseStoredJson(a, r[2].text) else return;
        const key = if (env == .object) env.object.get("key") orelse return else return;

        if (r[5] == .text) {
            const before = try parseStoredJson(a, r[5].text);
            const up = try core.planUpsert(a, table, st.pk, before);
            _ = self.stepExec(a, up) catch {};
        } else if (try core.planDelete(a, table, st.pk, key)) |stp| {
            _ = self.stepExec(a, stp) catch {};
        }
    }

    /// Fleet observability (NOTES §10dc): once per `heartbeat_ms`, publish this client's
    /// applied position per CDC stream to `$KV.<live>.<tenant>.<principal>` — last value
    /// per key, TTL on the bucket, so a client that stops beating simply goes stale. The
    /// bridge reads the bucket on its own cadence and turns head − applied into lag.
    /// Cooperative: a failed beat is printed and retried on the next turn, never fatal.
    fn heartbeatIfDue(self: *SyncClient) !void {
        if (self.opts.heartbeat_ms == 0 or self.tenant.len == 0) return;
        const now = nowMillis();
        if (self.last_heartbeat_ms != 0 and now - self.last_heartbeat_ms < @as(i64, @intCast(self.opts.heartbeat_ms))) return;
        var ha = std.heap.ArenaAllocator.init(self.a);
        defer ha.deinit();
        const a = ha.allocator();
        const streams = try self.cdcStreams(a);
        const seqs = try a.alloc(u64, streams.len);
        for (streams, 0..) |s, i| seqs[i] = try self.storedSeq(s);
        // §10fn: one beat per membership — the grant is `$KV.live.<tenant>.<principal>`
        // per tag, and each tenant's fleet view lists this client under its own key.
        const beats: []const []const u8 = if (self.tenants.len > 0) self.tenants else self.open_tenants;
        for (beats) |tenant| {
            const payload = try core.heartbeatPayload(a, self.opts.principal, tenant, now, streams, seqs);
            const subject = try std.fmt.allocPrint(a, "$KV.{s}.{s}.{s}", .{ self.kv_live, tenant, self.opts.principal });
            try self.t.publish(subject, payload, null);
        }
        self.last_heartbeat_ms = now;
    }

    fn publishEnvelope(self: *SyncClient, a: std.mem.Allocator, subject: []const u8, payload: Value, msg_id: []const u8) !void {
        const bytes = try encodeMsgpack(a, payload);
        try self.t.publish(subject, bytes, msg_id);
    }

    fn stepExec(self: *SyncClient, a: std.mem.Allocator, stp: Value) ![]storage.Row {
        const params_v = stp.object.get("params").?.array;
        const params = try a.alloc(storage.Value, params_v.items.len);
        for (params_v.items, 0..) |v, i| params[i] = try jsonToStorage(a, v);
        return self.st.query(a, stp.object.get("sql").?.string, params);
    }

    // ── the index card (NOTES §10): query / mutate / sync / flush ────────────────

    /// The app's read: arbitrary SQL against the read-only connection, JSON params in,
    /// `{"columns":[…],"rows":[[…],…]}` out — allocated from `a`. A write through here
    /// fails in SQLite ("attempt to write a readonly database"), which is the point.
    pub fn query(self: *SyncClient, a: std.mem.Allocator, sql: []const u8, params_json: Value) !Value {
        const n: usize = if (params_json == .array) params_json.array.items.len else 0;
        const params = try a.alloc(storage.Value, n);
        if (params_json == .array) for (params_json.array.items, 0..) |v, i| {
            params[i] = try jsonToStorage(a, v);
        };
        const res = try self.ro.queryNamed(a, sql, params);
        var cols = std.json.Array.init(a);
        for (res.columns) |cn| try cols.append(.{ .string = cn });
        var rows = std.json.Array.init(a);
        for (res.rows) |r| {
            var row = std.json.Array.init(a);
            for (r) |cell| try row.append(try storageToJson(a, cell));
            try rows.append(.{ .array = row });
        }
        var out: std.json.ObjectMap = .empty;
        try out.put(a, "columns", .{ .array = cols });
        try out.put(a, "rows", .{ .array = rows });
        return .{ .object = out };
    }

    pub const SyncReport = struct { tenant: []const u8, tenants: []const []const u8, first: bool };

    /// One pass of the read side: the one-time steps (tenant, schemas, outbox table,
    /// the verdict channel) the first time, then seed-if-gapped and drain-to-tail every
    /// time. Idempotent by construction — a host calls it at boot and on a timer.
    pub fn syncOnce(self: *SyncClient) !SyncReport {
        try self.refuseIfRevoked();
        const first = !self.schemas_synced;
        if (first) {
            try self.resolveTenant();
            try self.probeRevoked();
            try self.ensureOutbox();
            try self.subscribeVerdicts();
            self.schemas_synced = true;
        }
        // Every pass, not only the first: `migrateTable` decides from the database and
        // is a no-op on an identical descriptor, so a schema change published while
        // this client runs lands on its next sync (§10v's alter/rebuild path).
        try self.syncSchemas();
        try self.gapAndSeed(null, null);
        try self.drainCdc();
        // A host that syncs before it ever polls is a client too (PROTOCOL §9).
        self.heartbeatIfDue() catch |err| std.debug.print("heartbeat: {s}\n", .{@errorName(err)});
        return .{ .tenant = self.tenant, .tenants = self.tenants, .first = first };
    }

    /// §10fn: follow one more tenant at runtime — subscribe to its stream and seed
    /// its chain for every tenant-scoped table, into the same local tables (every row
    /// carries its tenant). Whether the broker lets it happen is the JWT's decision:
    /// a `region` principal's wildcard grant covers any cell, a `client` principal
    /// needs the tenant among its tags — a join outside the grant fails at the
    /// consumer, loudly, and is retried at the next poll like any unseeded table.
    pub fn join(self: *SyncClient, tenant: []const u8) !void {
        if (tenant.len == 0 or std.mem.indexOfAny(u8, tenant, ".*> ") != null) return error.TenantMalformed;
        for (self.tenants) |t| if (std.mem.eql(u8, t, tenant)) return;
        // Ask the broker FIRST (§10fo): a tenant outside the JWT's tags, or one whose
        // stream does not exist yet, answers here and the membership is never taken.
        // Taken anyway, the denied tail wedged every later poll — the permitted
        // tenants' included (measured: a join of an unenrolled cell, then Timeout on
        // every poll).
        {
            var qa = std.heap.ArenaAllocator.init(self.a);
            defer qa.deinit();
            const route = try self.routeFor(qa.allocator(), tenant);
            var info = self.t.js.getStreamInfo(route) catch |err| {
                std.debug.print("tenant: join {s} refused — {s} is not readable with these credentials ({s}): not among the JWT's tenants, or its stream does not exist yet\n", .{ tenant, route, @errorName(err) });
                return error.JoinRefused;
            };
            info.deinit();
        }
        var list: std.ArrayListUnmanaged([]const u8) = .empty;
        try list.appendSlice(self.aa(), self.tenants);
        try list.append(self.aa(), try self.aa().dupe(u8, tenant));
        std.mem.sort([]const u8, list.items, {}, lessStr);
        self.tenants = list.items;
        self.tenant_missing = false;
        self.tenant = self.tenants[0];
        try self.refreshRoutes();
        std.debug.print("tenant: joined {s} ({d} membership(s))\n", .{ tenant, self.tenants.len });
        // Tables skipped for want of a tenant get their descriptor applied now; then
        // the unseeded (table, tenant) pairs seed. The tail opens at the next poll.
        try self.syncSchemas();
        self.reseed_pending = false;
        try self.gapAndSeed(null, null);
    }

    /// §10fn: stop following a tenant — its rows leave the local tables, its
    /// watermarks and positions are forgotten, its tail is closed. A rejoin seeds it
    /// afresh. The client's own writes to it that are still in the outbox stay there:
    /// the verdict, not the membership, settles a write.
    pub fn leave(self: *SyncClient, tenant: []const u8) !void {
        var found = false;
        var list: std.ArrayListUnmanaged([]const u8) = .empty;
        for (self.tenants) |t| {
            if (std.mem.eql(u8, t, tenant)) {
                found = true;
            } else try list.append(self.aa(), t);
        }
        if (!found) return;
        self.tenants = list.items;
        self.tenant_missing = self.tenants.len == 0;
        self.tenant = if (self.tenants.len > 0) self.tenants[0] else self.open_tenant;
        var qa = std.heap.ArenaAllocator.init(self.a);
        defer qa.deinit();
        const a = qa.allocator();
        const route = try self.routeFor(a, tenant);
        var it = self.states.iterator();
        while (it.next()) |e| {
            const st = e.value_ptr;
            const tc = st.tenant_col orelse continue;
            _ = try self.st.query(a, try std.fmt.allocPrint(a, "DELETE FROM {s} WHERE \"{s}\" = ?", .{ e.key_ptr.*, tc }), &.{.{ .text = tenant }});
            _ = try self.st.query(a, "DELETE FROM _zbz_generations WHERE tbl = ? AND tenant = ?", &.{ .{ .text = e.key_ptr.* }, .{ .text = tenant } });
            var k: usize = 0;
            while (k < st.anchors.items.len) {
                if (std.mem.eql(u8, st.anchors.items[k].stream, route)) {
                    _ = st.anchors.orderedRemove(k);
                } else k += 1;
            }
        }
        try self.refreshRoutes();
        // The stream is nobody's now: close its tail and forget its position.
        var k: usize = 0;
        while (k < self.tails.items.len) {
            if (std.mem.eql(u8, self.tails.items[k].stream, route)) {
                self.tails.items[k].sub.deinit();
                _ = self.tails.orderedRemove(k);
            } else k += 1;
        }
        _ = try self.st.query(a, "DELETE FROM _zbz_stream_seq WHERE stream = ?", &.{.{ .text = route }});
        _ = self.dark.fetchOrderedRemove(route);
        std.debug.print("tenant: left {s} ({d} membership(s) left)\n", .{ tenant, self.tenants.len });
    }

    /// The membership moved: every tenant-scoped table's routes follow it.
    fn refreshRoutes(self: *SyncClient) !void {
        var it = self.states.iterator();
        while (it.next()) |e| {
            const st = e.value_ptr;
            if (st.tenant_col == null) continue;
            st.routes = if (self.isOnDemand(e.key_ptr.*)) &.{} else try self.routesFor(st.tenant_col);
        }
    }

    pub const FlushReport = struct { sent: usize, settled: usize };

    pub const VerdictCounts = struct {
        accepted: usize = 0,
        stale: usize = 0,
        rejected: usize = 0,
        row_deleted: usize = 0,
        failed: usize = 0,
        /// §10fk: `failed` with reason `rate_limited` — counted here, not as failed.
        rate_limited: usize = 0,
        other: usize = 0,
        fn count(self: *VerdictCounts, status: []const u8) void {
            if (std.mem.eql(u8, status, "accepted")) self.accepted += 1 else if (std.mem.eql(u8, status, "stale")) self.stale += 1 else if (std.mem.eql(u8, status, "rejected")) self.rejected += 1 else if (std.mem.eql(u8, status, "row_deleted")) self.row_deleted += 1 else if (std.mem.eql(u8, status, "failed")) self.failed += 1 else self.other += 1;
        }
    };

    /// The write side's pump: collect and replay the outbox (§7.1), then wait up to
    /// `wait_ms` for the first verdict and drain what follows.
    pub fn flush(self: *SyncClient, wait_ms: u64) !FlushReport {
        try self.refuseIfRevoked();
        const sent = try self.flushOutbox();
        const settled = try self.drainVerdicts(wait_ms);
        self.drainRebase();
        return .{ .sent = sent, .settled = settled };
    }

    pub fn count(self: *SyncClient, table: []const u8) i64 {
        var qa = std.heap.ArenaAllocator.init(self.a);
        defer qa.deinit();
        const sql = std.fmt.allocPrint(qa.allocator(), "SELECT count(*) FROM {s}", .{table}) catch return -1;
        const rows = self.st.query(qa.allocator(), sql, &.{}) catch return -1;
        return rows[0][0].integer;
    }
};

/// One required string from grammar.json, by path. The error names the key, because
/// "which name did the file forget" is the whole diagnostic.
fn grammarString(root: Value, path: []const []const u8) ![]const u8 {
    var cur = root;
    for (path) |key| {
        if (cur != .object) return grammarMissing(path);
        cur = cur.object.get(key) orelse return grammarMissing(path);
    }
    if (cur != .string or cur.string.len == 0) return grammarMissing(path);
    return cur.string;
}

fn grammarMissing(path: []const []const u8) error{GrammarKeyMissing} {
    // Not under test: the grammar test provokes this on purpose, and the build
    // runner reports any stderr from a test step as a failed command.
    if (!@import("builtin").is_test) {
        std.debug.print("grammar.json: required key missing or not a string: ", .{});
        for (path, 0..) |k, i| std.debug.print("{s}{s}", .{ if (i > 0) "." else "", k });
        std.debug.print("\n", .{});
    }
    return error.GrammarKeyMissing;
}

/// `_zbz_inbox` — held child-before-parent events, durable (CLIENTS.md, §10de finding 1).
/// Same shape as the TS client's `_zebridge_inbox`; standalone on `Storage` so a test
/// needs no NATS and no client.
/// §10dg: the shape this replica BUILT each table with (core.keyShape/typeShape) —
/// the record a re-key or a re-type is detected against.
pub fn ensureShape(st: *storage.Storage) !void {
    try execDdl(st, "CREATE TABLE IF NOT EXISTS _zbz_shape (tbl TEXT PRIMARY KEY, key_shape TEXT NOT NULL, type_shape TEXT NOT NULL)");
}

pub fn ensureInbox(st: *storage.Storage) !void {
    try execDdl(st, "CREATE TABLE IF NOT EXISTS _zbz_inbox (id INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT NOT NULL, lsn INTEGER NOT NULL, ev TEXT NOT NULL, reason TEXT NOT NULL, held_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0)");
    try execDdl(st, "CREATE INDEX IF NOT EXISTS _zbz_inbox_tbl ON _zbz_inbox (tbl, lsn)");
}

/// §10fd: the bookkeeping tables' DDL is written for SQLite; on PostgreSQL the
/// autoincrement key is a BIGSERIAL and every INTEGER a BIGINT (an LSN as a number
/// does not fit int4).
pub fn execDdl(st: *storage.Storage, sql: []const u8) !void {
    if (st.engine == .sqlite) return st.execSimple(sql);
    const a = std.heap.c_allocator;
    if (st.engine == .duckdb) {
        // §10fl: no serial — a sequence, created first when the inbox needs one.
        if (std.mem.indexOf(u8, sql, "INTEGER PRIMARY KEY AUTOINCREMENT") != null) try st.execSimple("CREATE SEQUENCE IF NOT EXISTS _zbz_inbox_seq");
        const d1 = try std.mem.replaceOwned(u8, a, sql, "INTEGER PRIMARY KEY AUTOINCREMENT", "BIGINT PRIMARY KEY DEFAULT nextval('_zbz_inbox_seq')");
        defer a.free(d1);
        const d2 = try std.mem.replaceOwned(u8, a, d1, " INTEGER", " BIGINT");
        defer a.free(d2);
        return st.execSimple(d2);
    }
    const s1 = try std.mem.replaceOwned(u8, a, sql, "INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY");
    defer a.free(s1);
    const s2 = try std.mem.replaceOwned(u8, a, s1, " INTEGER", " BIGINT");
    defer a.free(s2);
    return st.execSimple(s2);
}

test "duckdbType: PostgreSQL's spellings as DuckDB's (§10fl)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqualStrings("BLOB", try SyncClient.duckdbType(a, "bytea"));
    try std.testing.expectEqualStrings("BLOB", try SyncClient.duckdbType(a, "geometry(Point,4326)"));
    try std.testing.expectEqualStrings("FLOAT[3]", try SyncClient.duckdbType(a, "vector(3)"));
    try std.testing.expectEqualStrings("JSON", try SyncClient.duckdbType(a, "jsonb"));
    try std.testing.expectEqualStrings("DECIMAL(12,4)", try SyncClient.duckdbType(a, "numeric(12,4)"));
    try std.testing.expectEqualStrings("DECIMAL(38,10)", try SyncClient.duckdbType(a, "numeric"));
    try std.testing.expectEqualStrings("BIT", try SyncClient.duckdbType(a, "bit(8)"));
    try std.testing.expectEqualStrings("text[]", try SyncClient.duckdbType(a, "text[]"));
    try std.testing.expectEqualStrings("timestamp with time zone", try SyncClient.duckdbType(a, "timestamp with time zone"));
}

/// §10fd: a JSON array text (the wire's array form, §10ey) as PostgreSQL's array
/// literal; anything that is not one is returned as it came.
fn jsonArrayToLiteral(a: std.mem.Allocator, text: []const u8) ![]const u8 {
    if (text.len == 0 or text[0] != '[') return text;
    const parsed = std.json.parseFromSliceLeaky(Value, a, text, .{}) catch return text;
    if (parsed != .array) return text;
    return core.pgArrayLiteral(a, parsed.array);
}

/// Drop every held event of `table` whose key equals the deleted row's (§10dg).
pub fn pruneInboxKey(st: *storage.Storage, a: std.mem.Allocator, table: []const u8, pk: []const []const u8, data: Value) !void {
    if (pk.len == 0 or data != .object) return;
    const rows = try st.query(a, "SELECT id, ev FROM _zbz_inbox WHERE tbl = ?", &.{.{ .text = table }});
    for (rows) |r| {
        const ev = std.json.parseFromSliceLeaky(Value, a, r[1].text, .{}) catch continue;
        const d = if (ev == .object) (ev.object.get("data") orelse continue) else continue;
        if (d != .object) continue;
        var same = true;
        for (pk) |col| {
            const x = data.object.get(col) orelse {
                same = false;
                break;
            };
            const y = d.object.get(col) orelse {
                same = false;
                break;
            };
            if (!valueEql(x, y)) {
                same = false;
                break;
            }
        }
        if (same) {
            _ = try st.query(a, "DELETE FROM _zbz_inbox WHERE id = ?", &.{r[0]});
            std.debug.print("{s}: a held event for a row deleted upstream was discarded\n", .{table});
        }
    }
}

fn valueEql(x: Value, y: Value) bool {
    return switch (x) {
        .string => |s| y == .string and std.mem.eql(u8, s, y.string),
        .integer => |i| y == .integer and y.integer == i,
        .bool => |b| y == .bool and y.bool == b,
        else => false,
    };
}

pub fn holdEvent(st: *storage.Storage, a: std.mem.Allocator, table: []const u8, ev: Value, reason: []const u8) !void {
    const lsn: i64 = if (ev == .object) (if (ev.object.get("lsn")) |v| (if (v == .integer) v.integer else 0) else 0) else 0;
    const json = try core.valueToString(a, ev);
    _ = try st.query(a, "INSERT INTO _zbz_inbox (tbl, lsn, ev, reason, held_at) VALUES (?, ?, ?, ?, ?)", &.{
        .{ .text = table }, .{ .integer = lsn }, .{ .text = json }, .{ .text = reason }, .{ .integer = nowMillis() },
    });
}

pub fn pruneInboxSeeded(st: *storage.Storage, a: std.mem.Allocator, table: []const u8, watermark_lsn: i64) !void {
    _ = try st.query(a, "DELETE FROM _zbz_inbox WHERE tbl = ? AND lsn <= ?", &.{ .{ .text = table }, .{ .integer = watermark_lsn } });
}

pub fn pruneInboxDropped(st: *storage.Storage, a: std.mem.Allocator, table: []const u8) !void {
    const q = try st.query(a, "SELECT count(*) FROM _zbz_inbox WHERE tbl = ?", &.{.{ .text = table }});
    const k: i64 = if (q.len > 0 and q[0][0] == .integer) q[0][0].integer else 0;
    if (k > 0) std.debug.print("{s}: discarding {d} held event(s) — the table was dropped upstream\n", .{ table, k });
    _ = try st.query(a, "DELETE FROM _zbz_inbox WHERE tbl = ?", &.{.{ .text = table }});
}

/// Queued writes for a dropped or re-keyed table can never apply: the server would
/// answer `row_deleted` at best, and at worst land on an unrelated table that later
/// reuses the name. Discard them LOUDLY — a silent queue draining into a void is
/// exactly what an outbox exists to prevent. The TS client's `discardOutbox`; this
/// is the libzb half, which was missing (§10hs).
pub fn discardOutbox(st: *storage.Storage, a: std.mem.Allocator, table: []const u8, why: []const u8) !void {
    const q = try st.query(a, "SELECT count(*) FROM _zebridge_outbox WHERE tbl = ?", &.{.{ .text = table }});
    const k: i64 = if (q.len > 0 and q[0][0] == .integer) q[0][0].integer else 0;
    if (k == 0) return;
    _ = try st.query(a, "DELETE FROM _zebridge_outbox WHERE tbl = ?", &.{.{ .text = table }});
    std.debug.print("{s}: {d} queued write(s) discarded — the table was {s} — surface this to the user\n", .{ table, k, why });
}

test "inbox: a held event survives closing the database, and a seed past it prunes it" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const aa_ = arena.allocator();
    const path = "/tmp/zb-inbox-test.sqlite3";
    for ([_][]const u8{ path, path ++ "-wal", path ++ "-shm" }) |f| std.Io.Dir.cwd().deleteFile(std.testing.io, f) catch {};
    const ev = (try std.json.parseFromSlice(Value, aa_,
        \\{"table":"child","operation":"INSERT","lsn":10,"data":{"uid":"c1","parent_id":"p1"}}
    , .{})).value;
    {
        var st = try storage.Storage.open(path);
        defer st.close();
        try ensureInbox(&st);
        try holdEvent(&st, aa_, "child", ev, "missing-parent");
    }
    // The host died here. A fresh open still holds the row.
    var st = try storage.Storage.open(path);
    defer st.close();
    try ensureInbox(&st);
    var rows = try st.query(aa_, "SELECT tbl, lsn, reason FROM _zbz_inbox", &.{});
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("child", rows[0][0].text);
    try std.testing.expectEqual(@as(i64, 10), rows[0][1].integer);
    // A chain seeded short of it leaves it; one seeded past it supersedes it.
    try pruneInboxSeeded(&st, aa_, "child", 9);
    rows = try st.query(aa_, "SELECT id FROM _zbz_inbox", &.{});
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try pruneInboxSeeded(&st, aa_, "child", 10);
    rows = try st.query(aa_, "SELECT id FROM _zbz_inbox", &.{});
    try std.testing.expectEqual(@as(usize, 0), rows.len);
    // Dropped upstream: discarded.
    try holdEvent(&st, aa_, "child", ev, "missing-parent");
    try pruneInboxDropped(&st, aa_, "child");
    rows = try st.query(aa_, "SELECT id FROM _zbz_inbox", &.{});
    try std.testing.expectEqual(@as(usize, 0), rows.len);
}

test "spliceTransport: the answer keeps its shape, whatever it is" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // An ordinary answer: the marker goes first, the body follows intact.
    const got = try SyncClient.spliceTransport(a, "{\"count\":3}", "inline", 11, 7, 0);
    try std.testing.expectEqualStrings("{\"zb_transport\":{\"via\":\"inline\",\"bytes\":11,\"wire_ms\":7,\"fetch_ms\":0},\"count\":3}", got);
    // It must still parse, and both halves must be readable.
    const v = (try std.json.parseFromSlice(Value, a, got, .{})).value;
    try std.testing.expectEqual(@as(i64, 3), v.object.get("count").?.integer);
    try std.testing.expectEqualStrings("inline", v.object.get("zb_transport").?.object.get("via").?.string);
    // An empty object takes no comma.
    const e = try SyncClient.spliceTransport(a, "{}", "object", 900, 1, 24);
    try std.testing.expectEqualStrings("{\"zb_transport\":{\"via\":\"object\",\"bytes\":900,\"wire_ms\":1,\"fetch_ms\":24}}", e);
    _ = try std.json.parseFromSlice(Value, a, e, .{});
    // `{ }` — whitespace before the brace is still empty.
    const w = try SyncClient.spliceTransport(a, "{  }", "inline", 4, 0, 0);
    _ = try std.json.parseFromSlice(Value, a, w, .{});
    // Not an object: a service may answer with anything, and it travels UNCHANGED.
    try std.testing.expectEqualStrings("[1,2]", try SyncClient.spliceTransport(a, "[1,2]", "inline", 5, 0, 0));
    try std.testing.expectEqualStrings("\"hi\"", try SyncClient.spliceTransport(a, "\"hi\"", "inline", 4, 0, 0));
    try std.testing.expectEqualStrings("", try SyncClient.spliceTransport(a, "", "inline", 0, 0, 0));
}

test "outbox: a dropped or re-keyed table takes its queued writes with it" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const aa_ = arena.allocator();
    const path = "/tmp/zb-outbox-drop-test.sqlite3";
    for ([_][]const u8{ path, path ++ "-wal", path ++ "-shm" }) |f| std.Io.Dir.cwd().deleteFile(std.testing.io, f) catch {};
    var st = try storage.Storage.open(path);
    defer st.close();
    try execDdl(&st,
        \\CREATE TABLE IF NOT EXISTS _zebridge_outbox (
        \\  msg_id TEXT PRIMARY KEY, subject TEXT NOT NULL, payload TEXT NOT NULL,
        \\  tbl TEXT NOT NULL, row_id TEXT NOT NULL, before TEXT,
        \\  created_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0
        \\)
    );
    const ins = "INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, created_at) VALUES (?, 'sub', '{}', ?, 'r1', 1)";
    _ = try st.query(aa_, ins, &.{ .{ .text = "m1" }, .{ .text = "gone" } });
    _ = try st.query(aa_, ins, &.{ .{ .text = "m2" }, .{ .text = "gone" } });
    _ = try st.query(aa_, ins, &.{ .{ .text = "m3" }, .{ .text = "stays" } });
    // The dropped table's writes go; another table's are untouched — a discard is
    // scoped to one table, never a queue-wide flush.
    try discardOutbox(&st, aa_, "gone", "dropped");
    const rows = try st.query(aa_, "SELECT msg_id, tbl FROM _zebridge_outbox", &.{});
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("stays", rows[0][1].text);
    // Idempotent: a second drop of the same table is silent, not an error.
    try discardOutbox(&st, aa_, "gone", "dropped");
    // A client that never wrote has no outbox at all; the caller's `catch {}` covers
    // it, and the function itself must not pretend the table exists.
    try execDdl(&st, "DROP TABLE _zebridge_outbox");
    try std.testing.expectError(error.StepFailed, discardOutbox(&st, aa_, "gone", "dropped"));
}

test "grammar: a missing key fails naming its path, never a silent default" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ok = try std.json.parseFromSlice(Value, a,
        \\{"kv":{"schemas":"schemas"}}
    , .{});
    try std.testing.expectEqualStrings("schemas", try grammarString(ok.value, &.{ "kv", "schemas" }));
    try std.testing.expectError(error.GrammarKeyMissing, grammarString(ok.value, &.{ "kv", "tenants" }));
    try std.testing.expectError(error.GrammarKeyMissing, grammarString(ok.value, &.{"open_tenant"}));
    const empty = try std.json.parseFromSlice(Value, a,
        \\{"open_tenant":""}
    , .{});
    try std.testing.expectError(error.GrammarKeyMissing, grammarString(empty.value, &.{"open_tenant"}));
}

// ─── conversions ────────────────────────────────────────────────────────────

fn transportConsumerConfig() @import("nats").ConsumerConfig {
    return .{ .ack_policy = .explicit, .max_ack_pending = 512 };
}

fn contains(list: []const []const u8, s: []const u8) bool {
    for (list) |x| if (std.mem.eql(u8, x, s)) return true;
    return false;
}

fn indexOf(list: []const []const u8, s: []const u8) ?usize {
    for (list, 0..) |x, i| if (std.mem.eql(u8, x, s)) return i;
    return null;
}

/// PROTOCOL §7.5 — the decision is core.tombstoned, fixture-pinned in both cores.
fn tombstoned(st: TableState, data: Value) bool {
    return core.tombstoned(st.tombstone_col, data);
}

fn sameStrings(x: []const []const u8, y: []const []const u8) bool {
    if (x.len != y.len) return false;
    for (x, y) |p, q| if (!std.mem.eql(u8, p, q)) return false;
    return true;
}

fn dupeStrings(a: std.mem.Allocator, src: []const []const u8) ![]const []const u8 {
    const out = try a.alloc([]const u8, src.len);
    for (src, 0..) |sv, i| out[i] = try a.dupe(u8, sv);
    return out;
}

fn jsonStrList(a: std.mem.Allocator, v: ?Value) ![]const []const u8 {
    const val = v orelse return &.{};
    if (val != .array) return &.{};
    var out = try a.alloc([]const u8, val.array.items.len);
    for (val.array.items, 0..) |x, i| out[i] = if (x == .string) x.string else "";
    return out;
}

/// Chain objects may be zstd frames (§10w) — sniffed by the standard 4-byte magic.
/// §10ip: one-shot zstd for the QUERY channel — a plain frame, like every chain object
/// (the per-era dictionary went on 2026-09-24, NOTES §10iy).
///
/// Returns the INPUT unchanged when compression does not pay, so a tiny answer is never
/// made bigger. The caller decides inline-or-object on what comes back, which is the
/// point: measured on this stack, answers compress about 4x, so a 621 KB answer becomes
/// 163 KB and travels INLINE instead of becoming an object — the whole write-object,
/// reply-envelope, fetch-object round trip disappears.
fn compressAnswer(a: std.mem.Allocator, b: []const u8) ![]const u8 {
    // Below a few hundred bytes a frame header is most of the result.
    if (b.len < 512) return b;
    const bound = C.ZSTD_compressBound(b.len);
    const buf = try a.alloc(u8, bound);
    const n = C.ZSTD_compress(buf.ptr, buf.len, b.ptr, b.len, 3);
    if (C.ZSTD_isError(n) != 0) return b; // never fail an answer over compression
    if (n >= b.len) return b;             // incompressible: send it as it is
    return buf[0..n];
}

fn maybeZstd(a: std.mem.Allocator, b: []const u8) ![]const u8 {
    if (b.len < 4 or b[0] != 0x28 or b[1] != 0xb5 or b[2] != 0x2f or b[3] != 0xfd) return b;
    // §10ez: a frame that states its content size is inflated in one call — a 102 MB
    // full took 4.2 s through std.compress.zstd and a fraction of that here.
    // ZSTD_CONTENTSIZE_UNKNOWN/ERROR are (0ULL-1)/(0ULL-2): translate-c overflows on
    // the macros, so they are spelled out.
    const unknown: c_ulonglong = std.math.maxInt(c_ulonglong);
    const size = C.ZSTD_getFrameContentSize(b.ptr, b.len);
    if (size != unknown and size != unknown - 1) {
        const out = try a.alloc(u8, @intCast(size));
        const n = C.ZSTD_decompress(out.ptr, out.len, b.ptr, b.len);
        if (C.ZSTD_isError(n) != 0) return error.ZstdDecompressFailed;
        return out[0..n];
    }
    // §10gi: a frame without its size — the producer writes a full as a stream, and
    // the size is not known when the header goes out. libzstd's streaming decoder, the
    // output grown as it fills.
    const dctx = C.ZSTD_createDCtx() orelse return error.ZstdDecompressFailed;
    defer _ = C.ZSTD_freeDCtx(dctx);
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(a);
    try out.ensureTotalCapacity(a, @max(b.len * 4, 64 * 1024));
    var in: C.ZSTD_inBuffer = .{ .src = b.ptr, .size = b.len, .pos = 0 };
    while (true) {
        if (out.unusedCapacitySlice().len == 0) try out.ensureUnusedCapacity(a, out.capacity);
        const spare = out.unusedCapacitySlice();
        var ob: C.ZSTD_outBuffer = .{ .dst = spare.ptr, .size = spare.len, .pos = 0 };
        const r = C.ZSTD_decompressStream(dctx, &ob, &in);
        if (C.ZSTD_isError(r) != 0) return error.ZstdDecompressFailed;
        out.items.len += ob.pos;
        if (r == 0 and in.pos == in.size) break; // the frame is complete and flushed
        if (in.pos == in.size and ob.pos < spare.len) return error.ZstdDecompressFailed; // truncated
    }
    return try out.toOwnedSlice(a);
}

/// Stored JSON (an outbox payload or before-image) → the core's Value.
///
/// `.value` is arena-owned by the caller's allocator here, which is the client arena —
/// the same lifetime rule the rest of this file uses for parsed grammar and schemas.
/// §10do: a stale UPDATE kept aside for a rebase. `before` is the pre-write image
/// the outbox stored; null when the row was not here at write time (then every
/// column counts as changed by the winner, and the edit is dropped).
const Rebase = struct { table: []const u8, key: []const u8, values: []const u8, before: ?[]const u8, version: []const u8 };

fn sameJson(a: std.mem.Allocator, x: ?Value, y: ?Value) bool {
    const xs = core.valueToString(a, x orelse .null) catch return false;
    const ys = core.valueToString(a, y orelse .null) catch return false;
    return std.mem.eql(u8, xs, ys);
}

fn parseStoredJson(a: std.mem.Allocator, text: []const u8) !Value {
    const parsed = try std.json.parseFromSlice(Value, a, text, .{});
    return parsed.value;
}

/// std.json.Value → msgpack bytes: the inverse of decodeMsgpack, for the write path.
///
/// The wire is msgpack in both directions (§7 — a `table` field in the body would be
/// worth nothing, so the envelope carries only key/version/data and the SUBJECT
/// carries identity, table and operation).
/// ⚠️ The error set is spelled out: Zig cannot INFER one for a recursive function,
/// and `mapPut`/`setArrElement` add NotMap/NotArray to OutOfMemory.
fn jsonToMsgpack(a: std.mem.Allocator, v: Value) error{ OutOfMemory, NotMap, NotArray }!msgpack.Payload {
    return switch (v) {
        .null => .{ .nil = {} },
        .bool => |b| .{ .bool = b },
        .integer => |i| .{ .int = i },
        .float => |f| .{ .float = f },
        .number_string => |x| try msgpack.Payload.strToPayload(x, a),
        .string => |x| try msgpack.Payload.strToPayload(x, a),
        .array => |arr| blk: {
            var pl = try msgpack.Payload.arrPayload(arr.items.len, a);
            for (arr.items, 0..) |item, i| try pl.setArrElement(i, try jsonToMsgpack(a, item));
            break :blk pl;
        },
        .object => |obj| blk: {
            // §10ex: the bytes marker is msgpack `bin` on the wire, never a map.
            if (try binOf(a, v)) |b| break :blk try msgpack.Payload.binToPayload(b, a);
            var pl = msgpack.Payload.mapPayload(a);
            var it = obj.iterator();
            while (it.next()) |e| try pl.mapPut(e.key_ptr.*, try jsonToMsgpack(a, e.value_ptr.*));
            break :blk pl;
        },
    };
}

/// ⚠️ A growing writer, not a fixed 1 MiB buffer. The fixed buffer was allocated per
/// publish from whatever arena the caller passed — and the caller passed the
/// client-lifetime arena, so every write pinned a megabyte until `deinit`. Virtual
/// and untouched, so RSS never showed it; only VSZ would have.
fn encodeMsgpack(a: std.mem.Allocator, v: Value) ![]const u8 {
    const payload = try jsonToMsgpack(a, v);
    var reader = std.Io.Reader.fixed(&[_]u8{});
    var out: std.Io.Writer.Allocating = .init(a);
    defer out.deinit();
    var packer = msgpack.PackerIO.init(&reader, &out.writer);
    try packer.write(payload);
    return try out.toOwnedSlice();
}

/// Deep copy of a Value into `a` — for the one case where a Value must outlive the
/// arena it was decoded into (an FK-held event, before the inbox made holds durable).
/// ⚠️ The error set is explicit because the function is recursive (as `jsonToMsgpack`).
fn cloneValue(a: std.mem.Allocator, v: Value) error{OutOfMemory}!Value {
    return switch (v) {
        .null, .bool, .integer, .float => v,
        .number_string => |s| .{ .number_string = try a.dupe(u8, s) },
        .string => |s| .{ .string = try a.dupe(u8, s) },
        .array => |arr| blk: {
            var out = try std.json.Array.initCapacity(a, arr.items.len);
            for (arr.items) |item| out.appendAssumeCapacity(try cloneValue(a, item));
            break :blk .{ .array = out };
        },
        .object => |obj| blk: {
            var out: std.json.ObjectMap = .empty;
            try out.ensureTotalCapacity(a, obj.count());
            var it = obj.iterator();
            while (it.next()) |e| {
                out.putAssumeCapacity(try a.dupe(u8, e.key_ptr.*), try cloneValue(a, e.value_ptr.*));
            }
            break :blk .{ .object = out };
        },
    };
}

/// A stored cell → the JSON the core speaks (for the before-image).
fn storageToJson(a: std.mem.Allocator, v: storage.Value) error{OutOfMemory}!Value {
    return switch (v) {
        .null => .null,
        .integer => |i| .{ .integer = i },
        .real => |f| .{ .float = f },
        .text => |t| .{ .string = t },
        .blob => |b| try binMarker(a, b),
        .boolean => |b| .{ .bool = b },
    };
}

/// ⚠️ Zig 0.16's `std.time` has NO timestamp functions — no `nanoTimestamp`, no
/// `milliTimestamp`. libc is linked here (SQLite needs it), so `clock_gettime` is the
/// one that exists, and REALTIME is required rather than MONOTONIC: this is a wall
/// clock stamp that another machine will compare against, not an interval.
/// Milliseconds of wall clock, for the seed's phase timers.
/// The process's peak resident set, in MB (getrusage; bytes on macOS, KB on Linux).
fn maxRssMb() u64 {
    var ru: std.c.rusage = undefined;
    if (std.c.getrusage(0, &ru) != 0) return 0;
    const raw: u64 = @intCast(ru.maxrss);
    // Darwin (macOS and iOS alike) reports bytes; Linux and Android kilobytes.
    return if (@import("builtin").os.tag.isDarwin()) raw / (1 << 20) else raw / 1024;
}

fn msNow() i64 {
    const ts = nowRealtime();
    return @as(i64, @intCast(ts.sec)) * 1000 + @divFloor(@as(i64, @intCast(ts.nsec)), 1_000_000);
}

fn nowRealtime() std.c.timespec {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return ts;
}

fn nowMillis() i64 {
    const ts = nowRealtime();
    return @as(i64, ts.sec) * 1000 + @divTrunc(@as(i64, ts.nsec), 1_000_000);
}

/// Wall clock in the §7.2 wire format: UTC, six fractional digits, trailing Z.
fn nowWireIso(a: std.mem.Allocator) ![]const u8 {
    const ts = nowRealtime();
    const secs: u64 = @intCast(ts.sec);
    const micros: u64 = @intCast(@divTrunc(@as(i64, ts.nsec), 1000));
    const es = std.time.epoch.EpochSeconds{ .secs = secs };
    const yd = es.getEpochDay().calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();
    return std.fmt.allocPrint(a, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}Z", .{
        yd.year,              md.month.numeric(),      @as(u32, md.day_index) + 1,
        ds.getHoursIntoDay(), ds.getMinutesIntoHour(), ds.getSecondsIntoMinute(),
        micros,
    });
}

/// ⚠️ A decoder with the container caps LIFTED. zig_msgpack's `PackerIO` refuses any
/// array or map over 1,000,000 entries (`ParseLimits`) — a defence against hostile
/// input. A chain full is one `rows` array holding the whole table, and `users` is
/// two million rows: the stock decoder answered `ArrayTooLarge` and seeding died on
/// the first object (measured 2026-08-29 — the soak never seeds, so nothing noticed).
/// Everything decoded here arrives on subjects the broker authorised, from the
/// bridge; the byte limits and the depth cap stay at their defaults.
const WideReaderCtx = struct {
    reader: *std.Io.Reader,
    fn read(self: WideReaderCtx, buf: []u8) std.Io.Reader.Error!usize {
        try self.reader.readSliceAll(buf);
        return buf.len;
    }
};
const WideWriterCtx = struct {
    writer: *std.Io.Writer,
    fn write(self: WideWriterCtx, bytes: []const u8) std.Io.Writer.Error!usize {
        try self.writer.writeAll(bytes);
        return bytes.len;
    }
};
const WidePack = msgpack.PackWithLimits(
    WideWriterCtx,
    WideReaderCtx,
    std.Io.Writer.Error,
    std.Io.Reader.Error,
    WideWriterCtx.write,
    WideReaderCtx.read,
    .{ .max_array_length = 1 << 31, .max_map_size = 1 << 31 },
);

/// §10fa: a cursor over msgpack bytes. The headers a chain document uses — maps,
/// arrays, strings — are read by hand so the walk can index rows without building
/// them; a whole value is read through the library at the cursor's offset.
const MpCursor = struct {
    bytes: []const u8,
    off: usize = 0,

    fn byte(self: *MpCursor) !u8 {
        if (self.off >= self.bytes.len) return error.ChainObjectMalformed;
        const b = self.bytes[self.off];
        self.off += 1;
        return b;
    }

    fn be(self: *MpCursor, comptime T: type) !T {
        const n = @sizeOf(T);
        if (self.off + n > self.bytes.len) return error.ChainObjectMalformed;
        const v = std.mem.readInt(T, self.bytes[self.off..][0..n], .big);
        self.off += n;
        return v;
    }

    fn readMapLen(self: *MpCursor) !usize {
        const b = try self.byte();
        if (b >= 0x80 and b <= 0x8f) return b & 0x0f;
        if (b == 0xde) return try self.be(u16);
        if (b == 0xdf) return try self.be(u32);
        return error.ChainObjectMalformed;
    }

    fn readArrayLen(self: *MpCursor) !usize {
        const b = try self.byte();
        if (b >= 0x90 and b <= 0x9f) return b & 0x0f;
        if (b == 0xdc) return try self.be(u16);
        if (b == 0xdd) return try self.be(u32);
        return error.ChainObjectMalformed;
    }

    /// A string's bytes, sliced from the document — or null when the next value is
    /// not a string (the cursor does not move then).
    fn peekStr(self: *MpCursor) ?[]const u8 {
        const save = self.off;
        const b = self.byte() catch return null;
        const n: usize = blk: {
            if (b >= 0xa0 and b <= 0xbf) break :blk b & 0x1f;
            if (b == 0xd9) break :blk self.be(u8) catch return null;
            if (b == 0xda) break :blk self.be(u16) catch return null;
            if (b == 0xdb) break :blk self.be(u32) catch return null;
            self.off = save;
            return null;
        };
        if (self.off + n > self.bytes.len) {
            self.off = save;
            return null;
        }
        const sl = self.bytes[self.off..][0..n];
        self.off += n;
        return sl;
    }

    fn readStr(self: *MpCursor) ![]const u8 {
        return self.peekStr() orelse error.ChainObjectMalformed;
    }

    /// One whole value through the library, from the cursor's offset; the cursor
    /// moves past it.
    fn readValue(self: *MpCursor, a: std.mem.Allocator) !msgpack.Payload {
        if (self.off > self.bytes.len) return error.ChainObjectMalformed;
        var reader = std.Io.Reader.fixed(self.bytes[self.off..]);
        var dummy: [0]u8 = .{};
        var writer = std.Io.Writer.fixed(&dummy);
        var packer = WidePack.init(.{ .writer = &writer }, .{ .reader = &reader });
        const v = try packer.read(a);
        self.off += reader.seek;
        return v;
    }

    /// Past one value, whatever it is (decoded into `a`, which should be a scratch).
    fn skipValue(self: *MpCursor, a: std.mem.Allocator) !void {
        _ = try self.readValue(a);
    }

    /// §10fh: skip one value and say how long it was — the streaming seed's row cut.
    fn skipValueLen(self: *MpCursor, a: std.mem.Allocator) !usize {
        const start = self.off;
        _ = try self.readValue(a);
        return self.off - start;
    }
};

/// §10fh: a chain object inflated as it is read from the object store (ObjectPull,
/// a batch of chunks at a time). Holds 128 KB of input, the zstd context, and a window of output
/// that grows to fit the largest value parsed from it. Plain objects (no zstd
/// magic — the producer writes zstd, but §10w keeps mixed chains legal) go through
/// the same window. `parse` runs a cursor operation on the unread part of the
/// window: when the operation runs out of bytes and the source has more, the window
/// is filled and the operation retried; when the source is exhausted, the failure
/// is the document's.
const StreamInflate = struct {
    res: *transport.ObjectPull,
    a: std.mem.Allocator,
    dctx: ?*C.ZSTD_DCtx = null,
    in_buf: []u8,
    in: C.ZSTD_inBuffer,
    sniffed: bool = false,
    plain: bool = false,
    window: std.ArrayListUnmanaged(u8) = .empty,
    consumed: usize = 0,
    src_eof: bool = false,
    eof: bool = false,

    const in_size: usize = 128 * 1024;
    const window_step: usize = 4 * 1024 * 1024;

    fn init(a: std.mem.Allocator, res: *transport.ObjectPull) !StreamInflate {
        const in_buf = try a.alloc(u8, in_size);
        return .{ .res = res, .a = a, .in_buf = in_buf, .in = .{ .src = in_buf.ptr, .size = 0, .pos = 0 } };
    }

    fn deinit(self: *StreamInflate) void {
        if (self.dctx) |d| _ = C.ZSTD_freeDCtx(d);
        self.window.deinit(self.a);
        self.a.free(self.in_buf);
    }

    fn avail(self: *StreamInflate) []const u8 {
        return self.window.items[self.consumed..];
    }

    /// More input from the store into `in_buf`; false when the source is exhausted.
    fn readInput(self: *StreamInflate) !bool {
        if (self.src_eof) return false;
        var n: usize = 0;
        while (n < self.in_buf.len) {
            const got = try self.res.read(self.in_buf[n..]);
            if (got == 0) {
                self.src_eof = true;
                break;
            }
            n += got;
        }
        self.in = .{ .src = self.in_buf.ptr, .size = n, .pos = 0 };
        return n > 0;
    }

    /// The window gains bytes, or `eof` is set. The consumed prefix is dropped first,
    /// so the window holds only what is still unparsed plus what this call adds.
    fn fill(self: *StreamInflate) !void {
        if (self.eof) return;
        if (self.consumed > 0) {
            const rest = self.window.items.len - self.consumed;
            std.mem.copyForwards(u8, self.window.items[0..rest], self.window.items[self.consumed..]);
            self.window.items.len = rest;
            self.consumed = 0;
        }
        if (!self.sniffed) {
            _ = try self.readInput();
            self.sniffed = true;
            const b = self.in_buf[0..self.in.size];
            self.plain = !(b.len >= 4 and b[0] == 0x28 and b[1] == 0xb5 and b[2] == 0x2f and b[3] == 0xfd);
            if (!self.plain) {
                const d = C.ZSTD_createDCtx() orelse return error.ZstdDecompressFailed;
                self.dctx = d;
            }
        }
        const before = self.window.items.len;
        while (self.window.items.len == before and !self.eof) {
            if (self.plain) {
                if (self.in.pos < self.in.size) {
                    try self.window.appendSlice(self.a, self.in_buf[self.in.pos..self.in.size]);
                    self.in.pos = self.in.size;
                } else if (!(try self.readInput())) {
                    self.eof = true;
                }
                continue;
            }
            if (self.in.pos >= self.in.size and !(try self.readInput())) {
                // the frame did not end and the source is dry: whatever is parsed
                // from here on fails as malformed, which it is (a truncated object).
                self.eof = true;
                break;
            }
            try self.window.ensureUnusedCapacity(self.a, window_step);
            var out: C.ZSTD_outBuffer = .{ .dst = self.window.items.ptr + self.window.items.len, .size = self.window.capacity - self.window.items.len, .pos = 0 };
            const r = C.ZSTD_decompressStream(self.dctx.?, &out, &self.in);
            if (C.ZSTD_isError(r) != 0) return error.ZstdDecompressFailed;
            self.window.items.len += out.pos;
            if (r == 0) self.eof = true; // the frame is complete
        }
    }

    /// One header operation (`readMapLen`, `readStr`, `readArrayLen`) over the unread
    /// window, retried with more bytes until it succeeds or the source is exhausted;
    /// on success the bytes are consumed.
    fn parse(self: *StreamInflate, scratch: *std.heap.ArenaAllocator, comptime op: anytype) !ReturnOf(op) {
        while (true) {
            _ = scratch.reset(.retain_capacity);
            var cur = MpCursor{ .bytes = self.avail() };
            if (op(&cur)) |v| {
                self.consumed += cur.off;
                return v;
            } else |err| {
                if (self.eof) return err;
                try self.fill();
            }
        }
    }

    /// The length of the next value (a row), which is NOT consumed: the caller copies
    /// `avail()[0..len]` out of the window first, then consumes.
    fn parseRowLen(self: *StreamInflate, scratch: *std.heap.ArenaAllocator) !usize {
        while (true) {
            _ = scratch.reset(.retain_capacity);
            var cur = MpCursor{ .bytes = self.avail() };
            if (cur.skipValueLen(scratch.allocator())) |len| {
                return len;
            } else |err| {
                if (self.eof) return err;
                try self.fill();
            }
        }
    }

    /// A whole value, read through the library, consumed.
    fn parseValue(self: *StreamInflate, scratch: *std.heap.ArenaAllocator) !msgpack.Payload {
        while (true) {
            _ = scratch.reset(.retain_capacity);
            var cur = MpCursor{ .bytes = self.avail() };
            if (cur.readValue(scratch.allocator())) |v| {
                self.consumed += cur.off;
                return v;
            } else |err| {
                if (self.eof) return err;
                try self.fill();
            }
        }
    }

    fn ReturnOf(comptime op: anytype) type {
        const ret = @typeInfo(@TypeOf(op)).@"fn".return_type.?;
        return @typeInfo(ret).error_union.payload;
    }
};

test "MpCursor: headers by hand, values through the library, offsets exact (§10fa)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // {"columns":["uid","n"],"rows":[["b",2],["a",1]],"kind":"full"}
    const doc = [_]u8{ 0x83, 0xa7, 'c', 'o', 'l', 'u', 'm', 'n', 's', 0x92, 0xa3, 'u', 'i', 'd', 0xa1, 'n', 0xa4, 'r', 'o', 'w', 's', 0x92, 0x92, 0xa1, 'b', 0x02, 0x92, 0xa1, 'a', 0x01, 0xa4, 'k', 'i', 'n', 'd', 0xa4, 'f', 'u', 'l', 'l' };
    var cur = MpCursor{ .bytes = &doc };
    try std.testing.expectEqual(@as(usize, 3), try cur.readMapLen());
    try std.testing.expectEqualStrings("columns", try cur.readStr());
    const cols = try cur.readValue(a);
    try std.testing.expectEqual(@as(usize, 2), try cols.getArrLen());
    try std.testing.expectEqualStrings("rows", try cur.readStr());
    try std.testing.expectEqual(@as(usize, 2), try cur.readArrayLen());
    const off0 = cur.off;
    var peek = cur;
    try std.testing.expectEqual(@as(usize, 2), try peek.readArrayLen());
    try std.testing.expectEqualStrings("b", peek.peekStr().?);
    try cur.skipValue(a);
    const off1 = cur.off;
    try std.testing.expect(off1 == off0 + 4);
    try cur.skipValue(a);
    try std.testing.expectEqualStrings("kind", try cur.readStr());
    const kind = try cur.readValue(a);
    try std.testing.expectEqualStrings("full", kind.str.value());
    try std.testing.expectEqual(doc.len, cur.off);
}

/// msgpack bytes → std.json.Value (the shape core.zig speaks).
fn decodeMsgpack(a: std.mem.Allocator, bytes: []const u8) !Value {
    var reader = std.Io.Reader.fixed(bytes);
    var dummy: [0]u8 = .{};
    var writer = std.Io.Writer.fixed(&dummy);
    var packer = WidePack.init(.{ .writer = &writer }, .{ .reader = &reader });
    const payload = try packer.read(a);
    return mpToJson(a, payload);
}

fn decodeMaybeMsgpackString(a: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    const v = decodeMsgpack(a, bytes) catch return bytes;
    return if (v == .string) v.string else bytes;
}

/// §10ex: bytes inside the JSON the core speaks. `std.json.Value` has no binary, and
/// a JSON text with raw bytes in a string is not valid JSON (the outbox stores
/// payloads as JSON text) — so bytes travel as `{"$bin": "<base64>"}` everywhere
/// JSON is the shape: the CDC value on its way to a bind, a chain cell, the
/// before-image, a query result handed to the host, a host's write. The host sees
/// and sends the same marker.
pub const BIN_KEY = "$bin";

fn binMarker(a: std.mem.Allocator, bytes: []const u8) error{OutOfMemory}!Value {
    const enc = std.base64.standard.Encoder;
    const buf = try a.alloc(u8, enc.calcSize(bytes.len));
    _ = enc.encode(buf, bytes);
    var obj: std.json.ObjectMap = .empty;
    try obj.put(a, BIN_KEY, .{ .string = buf });
    return .{ .object = obj };
}

/// The bytes of a `{"$bin": …}` marker, or null when `v` is anything else.
/// §10fg: a pgvector/bit column as the descriptor names it, with a bit(n)'s length.
pub const VecCol = struct { name: []const u8, kind: core.VecKind, bits: u32 };
/// The same, by column index in a chain step.
pub const VecIdx = struct { i: usize, kind: core.VecKind, bits: u32 };

fn binOf(a: std.mem.Allocator, v: Value) error{OutOfMemory}!?[]u8 {
    if (v != .object or v.object.count() != 1) return null;
    const s = v.object.get(BIN_KEY) orelse return null;
    if (s != .string) return null;
    const dec = std.base64.standard.Decoder;
    const n = dec.calcSizeForSlice(s.string) catch return null;
    const out = try a.alloc(u8, n);
    dec.decode(out, s.string) catch return null;
    return out;
}

fn mpToJson(a: std.mem.Allocator, p: msgpack.Payload) error{OutOfMemory}!Value {
    return switch (p) {
        .nil => .null,
        .bool => |b| .{ .bool = b },
        .int => |i| .{ .integer = i },
        .uint => |u| if (u <= std.math.maxInt(i64)) Value{ .integer = @intCast(u) } else Value{ .float = @floatFromInt(u) },
        .float => |f| .{ .float = f },
        .str => |s| .{ .string = s.value() },
        .bin => |b| try binMarker(a, b.value()),
        .arr => |items| blk: {
            var arr = std.json.Array.init(a);
            for (items) |item| try arr.append(try mpToJson(a, item));
            break :blk .{ .array = arr };
        },
        .map => |m| blk: {
            var obj: std.json.ObjectMap = .empty;
            var it = m.map.iterator();
            while (it.next()) |e| {
                const k = e.key_ptr.*;
                if (k != .str) continue;
                try obj.put(a, k.str.value(), try mpToJson(a, e.value_ptr.*));
            }
            break :blk .{ .object = obj };
        },
        else => .null,
    };
}

/// A core-built SQL param (json.Value) → a storage bind value.
fn jsonToStorage(a: std.mem.Allocator, v: Value) !storage.Value {
    return switch (v) {
        .null => .null,
        .bool => |b| .{ .boolean = b },
        .integer => |i| .{ .integer = i },
        .float => |f| .{ .real = f },
        .string => |s| .{ .text = s },
        else => if (try binOf(a, v)) |b| .{ .blob = b } else .{ .text = try core.valueToString(a, v) },
    };
}

/// A chain-row cell (json.Value from msgpack) → a storage bind value,
/// mirroring core.chainRowParams: structured → JSON text, strings → wire ts.
fn chainCellToStorage(a: std.mem.Allocator, v: Value) !storage.Value {
    return switch (v) {
        .string => |s| .{ .text = try core.pgTsToWire(a, s) },
        .object, .array => if (try binOf(a, v)) |b| .{ .blob = b } else .{ .text = try core.valueToString(a, v) },
        else => jsonToStorage(a, v),
    };
}

test "bytes round the JSON world as a base64 marker and bind as a blob (§10ex)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const raw = [_]u8{ 0, 0xff, 0xfe, 'A' };
    // the wire's bin → the core's JSON → a BLOB bind
    const pl = try msgpack.Payload.binToPayload(&raw, a);
    const j = try mpToJson(a, pl);
    try std.testing.expect(j == .object);
    try std.testing.expectEqualStrings("AP/+QQ==", j.object.get(BIN_KEY).?.string);
    const cell = try chainCellToStorage(a, j);
    try std.testing.expect(cell == .blob);
    try std.testing.expectEqualSlices(u8, &raw, cell.blob);
    const bound = try jsonToStorage(a, j);
    try std.testing.expect(bound == .blob);
    // a stored blob → the host's JSON → the outbox's msgpack bin
    const back = try storageToJson(a, .{ .blob = &raw });
    const mp = try jsonToMsgpack(a, back);
    try std.testing.expect(mp == .bin);
    try std.testing.expectEqualSlices(u8, &raw, mp.bin.value());
    // a plain object is still JSON text
    var plain: std.json.ObjectMap = .empty;
    try plain.put(a, "k", .{ .integer = 1 });
    const t = try jsonToStorage(a, .{ .object = plain });
    try std.testing.expect(t == .text);
}

// ─── ownership tests ────────────────────────────────────────────────────────
//
// `std.testing.allocator` fails a test that leaks, which is what makes these
// meaningful: they assert that a FAILED init frees everything it acquired. Before the
// errdefer chain was made exact, the first of these leaked the struct and the second
// leaked an open SQLite handle — neither showed up as a test failure anywhere, because
// nothing was calling init on a path that fails.

test "cloneValue: a held event survives the arena it was decoded into" {
    var keep = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer keep.deinit();
    var copy: Value = undefined;
    {
        var batch = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer batch.deinit();
        const src = try std.json.parseFromSlice(Value, batch.allocator(),
            \\{"table":"salaries","data":{"id":7,"tags":["a","b"],"amt":1.5,"gone":null}}
        , .{});
        copy = try cloneValue(keep.allocator(), src.value);
    } // the batch arena is gone; every byte the copy points at must be in `keep`
    try std.testing.expectEqualStrings("salaries", copy.object.get("table").?.string);
    const data = copy.object.get("data").?.object;
    try std.testing.expectEqual(@as(i64, 7), data.get("id").?.integer);
    try std.testing.expectEqualStrings("b", data.get("tags").?.array.items[1].string);
    try std.testing.expect(data.get("gone").? == .null);
}

test "msgpack encode/decode roundtrip allocates only what it writes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const src = try std.json.parseFromSlice(Value, a,
        \\{"key":{"uid":"u1"},"version":"2026-01-01T00:00:00.000000Z","n":[1,2,3]}
    , .{});
    const bytes = try encodeMsgpack(a, src.value);
    // The old fixed buffer was 1 MiB per call; the envelope is well under 100 bytes.
    try std.testing.expect(bytes.len < 128);
    const back = try decodeMsgpack(a, bytes);
    try std.testing.expectEqualStrings("u1", back.object.get("key").?.object.get("uid").?.string);
    try std.testing.expectEqual(@as(i64, 3), back.object.get("n").?.array.items[2].integer);
}

test "init that cannot open the database leaks nothing" {
    const bad = "/nonexistent-directory-for-zb-tests/replica.sqlite3";
    const r = SyncClient.init(std.testing.allocator, .{
        .url = "nats://127.0.0.1:1",
        .creds_path = "/nonexistent.creds",
        .db_path = bad,
        .principal = "t",
        .tables = &.{},
    });
    try std.testing.expectError(storage.Error.OpenFailed, r);
}

test "init with another protocol's grammar hash refuses, and leaks nothing — including the open database" {
    // The database DOES open here, so this is the case that used to leave a live
    // SQLite handle behind: errdefer destroyed the struct and closed nothing.
    const r = SyncClient.init(std.testing.allocator, .{
        .url = "nats://127.0.0.1:1",
        .creds_path = "/nonexistent.creds",
        .grammar_hash = "0000000000000000000000000000000000000000000000000000000000000000",
        .db_path = "zbz-test-ownership.sqlite3",
        .principal = "t",
        .tables = &.{},
    });
    try std.testing.expectError(error.GrammarMismatch, r);
    _ = std.c.unlink("zbz-test-ownership.sqlite3");
}

test "the embedded grammar parses and its hash is the bridge's form" {
    const g = try std.json.parseFromSlice(Value, std.testing.allocator, grammar_json, .{});
    defer g.deinit();
    try std.testing.expect(g.value == .object);
    try std.testing.expect(g.value.object.get("streams") != null);
    var buf: [64]u8 = undefined;
    const h = grammarHashHex(&buf);
    try std.testing.expectEqual(@as(usize, 64), h.len);
    for (h) |c| try std.testing.expect(std.ascii.isHex(c));
}

test "migrateTable: create, ALTER add/remove, rename hint, FK change rebuilds — the row survives each" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const aa_ = arena.allocator();
    const path = "/tmp/zb-migrate-test.sqlite3";
    for ([_][]const u8{ path, path ++ "-wal", path ++ "-shm" }) |f| std.Io.Dir.cwd().deleteFile(std.testing.io, f) catch {};
    var st = try storage.Storage.open(path);
    defer st.close();

    const desc = struct {
        fn make(al: std.mem.Allocator, json: []const u8) !Value {
            return (try std.json.parseFromSlice(Value, al, json, .{})).value;
        }
    };
    // 1. first sight: created (with an index)
    const d1 = try desc.make(aa_,
        \\{"pk_columns":["uid"],"sqlite":{"columns":[{"name":"uid","type":"TEXT"},{"name":"a","type":"INTEGER"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[],"indexes":[{"name":"t_a","columns":["a"]}]}
    );
    try std.testing.expectEqual(SyncClient.Migration.created, try SyncClient.migrateTable(&st, aa_, "t", d1));
    _ = try st.query(aa_, "INSERT INTO t (uid, a, updated_at) VALUES ('u1', 7, 'v1')", &.{});
    try std.testing.expectEqual(SyncClient.Migration.unchanged, try SyncClient.migrateTable(&st, aa_, "t", d1));

    // 2. add b, remove a — ALTER; the index on a goes with it; the view exists
    const d2 = try desc.make(aa_,
        \\{"pk_columns":["uid"],"sqlite":{"columns":[{"name":"uid","type":"TEXT"},{"name":"b","type":"TEXT"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[],"indexes":[]}
    );
    try std.testing.expectEqual(SyncClient.Migration.altered, try SyncClient.migrateTable(&st, aa_, "t", d2));
    var rows = try st.query(aa_, "SELECT uid, b FROM t", &.{});
    try std.testing.expectEqual(1, rows.len);
    try std.testing.expectEqualStrings("u1", rows[0][0].text);
    try std.testing.expectEqual(0, (try st.query(aa_, "SELECT name FROM sqlite_master WHERE type='index' AND name='t_a'", &.{})).len);
    try std.testing.expectEqual(1, (try st.query(aa_, "SELECT name FROM sqlite_master WHERE type='view' AND name='t_view'", &.{})).len);

    // 3. a rename hint: b → c keeps the value (§1.2: without the hint it would be lost)
    _ = try st.query(aa_, "UPDATE t SET b = 'kept'", &.{});
    const d3 = try desc.make(aa_,
        \\{"pk_columns":["uid"],"sqlite":{"columns":[{"name":"uid","type":"TEXT"},{"name":"c","type":"TEXT"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[],"indexes":[],"renamed":{"c":"b"}}
    );
    try std.testing.expectEqual(SyncClient.Migration.altered, try SyncClient.migrateTable(&st, aa_, "t", d3));
    rows = try st.query(aa_, "SELECT c FROM t", &.{});
    try std.testing.expectEqualStrings("kept", rows[0][0].text);

    // 4. an FK appears: SQLite cannot ALTER a constraint in — rebuild, row copied
    _ = try st.query(aa_, "CREATE TABLE p (uid TEXT PRIMARY KEY)", &.{});
    _ = try st.query(aa_, "INSERT INTO p VALUES ('u1')", &.{});
    const d4 = try desc.make(aa_,
        \\{"pk_columns":["uid"],"sqlite":{"columns":[{"name":"uid","type":"TEXT"},{"name":"c","type":"TEXT"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[{"columns":["uid"],"references":"p","parent_columns":["uid"]}],"indexes":[{"name":"t_c","columns":["c"]}]}
    );
    try std.testing.expectEqual(SyncClient.Migration.rebuilt, try SyncClient.migrateTable(&st, aa_, "t", d4));
    rows = try st.query(aa_, "SELECT c FROM t", &.{});
    try std.testing.expectEqual(1, rows.len);
    try std.testing.expectEqualStrings("kept", rows[0][0].text);
    try std.testing.expect(std.mem.indexOf(u8, (try st.query(aa_, "SELECT sql FROM sqlite_master WHERE name='t'", &.{}))[0][0].text, "FOREIGN KEY") != null);
    try std.testing.expectEqual(1, (try st.query(aa_, "SELECT name FROM sqlite_master WHERE type='index' AND name='t_c'", &.{})).len);
    try std.testing.expectEqual(SyncClient.Migration.unchanged, try SyncClient.migrateTable(&st, aa_, "t", d4));
    // and the FK is live again after the surgery
    try std.testing.expectError(error.StepFailed, st.query(aa_, "INSERT INTO t (uid, c, updated_at) VALUES ('orphan', 'x', 'v')", &.{}));

    // 5. §10dg re-type: c TEXT → REAL, same names — invisible to diffColumns, caught by
    // the shape record; SQLite has no ALTER COLUMN TYPE, so a rebuild that keeps the row.
    const d5 = try desc.make(aa_,
        \\{"pk_columns":["uid"],"sqlite":{"columns":[{"name":"uid","type":"TEXT"},{"name":"c","type":"REAL"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[{"columns":["uid"],"references":"p","parent_columns":["uid"]}],"indexes":[{"name":"t_c","columns":["c"]}]}
    );
    try std.testing.expectEqual(SyncClient.Migration.rebuilt, try SyncClient.migrateTable(&st, aa_, "t", d5));
    rows = try st.query(aa_, "SELECT c FROM t", &.{});
    try std.testing.expectEqual(1, rows.len);
    try std.testing.expect(std.mem.indexOf(u8, (try st.query(aa_, "SELECT sql FROM sqlite_master WHERE name='t'", &.{}))[0][0].text, "\"c\" REAL") != null);
    try std.testing.expectEqual(SyncClient.Migration.unchanged, try SyncClient.migrateTable(&st, aa_, "t", d5));

    // 6. §10dg re-key: the pk uid TEXT → INTEGER (the bigserial→uuid shape, mirrored).
    // Same names again — but a key cannot move in place: rebuilt EMPTY, the row is gone,
    // and the shape record now names the new key.
    const d6 = try desc.make(aa_,
        \\{"pk_columns":["uid"],"sqlite":{"columns":[{"name":"uid","type":"INTEGER"},{"name":"c","type":"REAL"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[],"indexes":[]}
    );
    try std.testing.expectEqual(SyncClient.Migration.rekeyed, try SyncClient.migrateTable(&st, aa_, "t", d6));
    try std.testing.expectEqual(0, (try st.query(aa_, "SELECT uid FROM t", &.{})).len);
    try std.testing.expect(std.mem.indexOf(u8, (try st.query(aa_, "SELECT sql FROM sqlite_master WHERE name='t'", &.{}))[0][0].text, "\"uid\" INTEGER NOT NULL PRIMARY KEY") != null);
    try std.testing.expectEqualStrings("[[\"uid\",\"INTEGER\"]]", (try st.query(aa_, "SELECT key_shape FROM _zbz_shape WHERE tbl='t'", &.{}))[0][0].text);
    try std.testing.expectEqual(1, (try st.query(aa_, "SELECT name FROM sqlite_master WHERE type='view' AND name='t_view'", &.{})).len);
    try std.testing.expectEqual(SyncClient.Migration.unchanged, try SyncClient.migrateTable(&st, aa_, "t", d6));
    // 7. a composite key (uid, c) — the pk column SET moved: re-key again
    const d7 = try desc.make(aa_,
        \\{"pk_columns":["uid","c"],"sqlite":{"columns":[{"name":"uid","type":"INTEGER"},{"name":"c","type":"REAL"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[],"indexes":[]}
    );
    try std.testing.expectEqual(SyncClient.Migration.rekeyed, try SyncClient.migrateTable(&st, aa_, "t", d7));
    try std.testing.expect(std.mem.indexOf(u8, (try st.query(aa_, "SELECT sql FROM sqlite_master WHERE name='t'", &.{}))[0][0].text, "PRIMARY KEY (\"uid\", \"c\")") != null);
}

test "migrateTable: a suspension descriptor is an error, never a panic" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const path = "/tmp/zb-migrate-susp.sqlite3";
    for ([_][]const u8{ path, path ++ "-wal", path ++ "-shm" }) |f| std.Io.Dir.cwd().deleteFile(std.testing.io, f) catch {};
    var st = try storage.Storage.open(path);
    defer st.close();
    const susp = (try std.json.parseFromSlice(Value, arena.allocator(), "{\"table\":\"t\",\"suspended\":\"no_cdc_subject\",\"lsn\":1}", .{})).value;
    try std.testing.expectError(error.SchemaUnusable, SyncClient.migrateTable(&st, arena.allocator(), "t", susp));
    const junk = (try std.json.parseFromSlice(Value, arena.allocator(), "[1,2]", .{})).value;
    try std.testing.expectError(error.SchemaUnusable, SyncClient.migrateTable(&st, arena.allocator(), "t", junk));
}

test "maybeZstd: a frame without its content size inflates (§10gi)" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const aa = arena.allocator();
    const doc = try aa.alloc(u8, 3 * 1024 * 1024 + 17);
    for (doc, 0..) |*b, i| b.* = "chain rows repeat, a little"[i % 27] ^ @as(u8, @truncate(i / 4096));
    {
        const cctx = C.ZSTD_createCCtx().?;
        defer _ = C.ZSTD_freeCCtx(cctx);
        // Streamed in small pieces, like the producer's full: no size in the header.
        var z: std.ArrayListUnmanaged(u8) = .empty;
        // (A first call with ZSTD_e_end and all the input would state the size.)
        var chunk: [4096]u8 = undefined;
        var fed: usize = 0;
        while (true) {
            const piece = @min(doc.len - fed, 64 * 1024);
            var in: C.ZSTD_inBuffer = .{ .src = doc[fed..].ptr, .size = piece, .pos = 0 };
            const end = fed + piece == doc.len;
            while (true) {
                var ob: C.ZSTD_outBuffer = .{ .dst = &chunk, .size = chunk.len, .pos = 0 };
                const r = C.ZSTD_compressStream2(cctx, &ob, &in, if (end) C.ZSTD_e_end else C.ZSTD_e_continue);
                try std.testing.expect(C.ZSTD_isError(r) == 0);
                try z.appendSlice(aa, chunk[0..ob.pos]);
                if (end and r == 0) break;
                if (!end and in.pos == in.size and ob.pos < chunk.len) break;
            }
            fed += piece;
            if (end) break;
        }
        const unknown: c_ulonglong = std.math.maxInt(c_ulonglong);
        try std.testing.expectEqual(unknown, C.ZSTD_getFrameContentSize(z.items.ptr, z.items.len));
        try std.testing.expectEqualSlices(u8, doc, try maybeZstd(aa, z.items));
        // Truncated: an error, never a short document.
        try std.testing.expectError(error.ZstdDecompressFailed, maybeZstd(aa, z.items[0 .. z.items.len - 9]));
    }
}
