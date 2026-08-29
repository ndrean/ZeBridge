//! Centralized configuration for CDC Bridge
//!
//! This module consolidates all configuration constants used throughout the application.
//! Instead of hardcoded values scattered across files, all tunables are defined here.

const std = @import("std");
const Topo = @import("topology.zig");

pub const log = std.log.scoped(.config);

/// PostgreSQL connection and replication configuration
pub const Postgres = struct {
    // No default_slot_name / default_publication_name on purpose: a default here
    // is a guess about which tables to replicate and whose WAL position to keep.
    // `parseArgs` requires --slot/--pub or their env vars and refuses without them.

    /// TCP keepalives on every libpq connection.
    ///
    /// PostgreSQL's `tcp_keepalives_*` default to 0, meaning "use the OS default" —
    /// ~2 hours idle on Linux. That is fine for the replication connection, which has
    /// its own application-level heartbeat, but the **snapshot and mutation connections
    /// sit idle for long stretches**: if a NAT or a paused VM silently forgets the flow,
    /// the next query *hangs* for hours instead of failing. libpq accepts these in the
    /// conninfo, so a dead peer is detected in ~60s (30 + 3×10) rather than ~2h.
    pub const tcp_keepalives_idle_s = 30;
    pub const tcp_keepalives_interval_s = 10;
    pub const tcp_keepalives_count = 3;
};

/// NATS JetStream configuration
pub const Nats = struct {
    pub const default_host = "127.0.0.1";
    pub const default_port = 4222;

    /// Idle pause of the mutation pull loop when a fetch returns no messages.
    /// Applies ONLY to the empty case — under backlog the loop must drain at
    /// fetch speed (the sleep once ran unconditionally and capped ingress at
    /// ~10 msg/s; see mutation_listener.zig).
    pub const mutation_pull_idle_ms = 100;

    /// Derived, never written out again. Nothing in the live path builds a URL any more
    /// — `Endpoint` below is the address, and a URL is only ever an *input* — but the
    /// literal used to be spelled here, again in `nats_publisher.PublisherConfig`, and a
    /// third time in `bridge.zig`, so changing the port here moved none of them.
    /// (`nats_publisher.old.zig` is the last reader.)
    pub const default_url = "nats://" ++ default_host ++ ":" ++ std.fmt.comptimePrint("{d}", .{default_port});

    /// Where NATS is, resolved **once** for the whole process.
    ///
    /// This exists because three components used to answer that question differently:
    /// the publisher parsed `NATS_URL`; the snapshot listener borrowed the publisher's
    /// parsed result; and the mutation listener read `NATS_HOST` raw and passed **no
    /// port at all**, so it silently used 4222 whatever `NATS_URL` said. Setting
    /// `NATS_HOST=nats-server` (a name that resolves only inside compose) alongside a
    /// working `NATS_URL` therefore produced a bridge where CDC and snapshots ran fine
    /// and ingress alone failed with `HostResolutionFailed` — one process, two
    /// destinations, and nothing in the logs saying so.
    ///
    /// Every field borrows from the URL string or the environment block, both of which
    /// outlive the process (see `parseArgs`), so there is nothing to free.
    pub const Endpoint = struct {
        host: []const u8 = default_host,
        port: u16 = default_port,
        /// Parsed out of `nats://user:pass@host:port`. Dropping these leaves the client
        /// waiting forever against a server that requires authorization.
        user: ?[]const u8 = null,
        pass: ?[]const u8 = null,
        seed: ?[]const u8 = null,
        /// Path to a .creds file (operator/JWT mode). Takes precedence in the
        /// client over the bare seed; the file is re-read on every reconnect,
        /// so rotation needs no restart.
        creds: ?[]const u8 = null,

        pub const ParseError = error{ MissingScheme, BadPort };

        /// Parse `nats://[user:pass@]host[:port]`.
        pub fn parseUrl(url: []const u8) ParseError!Endpoint {
            const scheme = "nats://";
            if (!std.mem.startsWith(u8, url, scheme)) return error.MissingScheme;
            var rest = url[scheme.len..];

            // A trailing path is not part of the address. `nats://host:4222/` is a URL a
            // person will reasonably type, and parsing "4222/" as a port fails.
            if (std.mem.indexOfScalar(u8, rest, '/')) |slash| rest = rest[0..slash];

            var out = Endpoint{};

            // Rightmost '@': a password may legally contain one.
            if (std.mem.lastIndexOfScalar(u8, rest, '@')) |at| {
                const creds = rest[0..at];
                rest = rest[at + 1 ..];
                if (std.mem.indexOfScalar(u8, creds, ':')) |colon| {
                    out.user = creds[0..colon];
                    out.pass = creds[colon + 1 ..];
                } else if (creds.len > 0) {
                    out.user = creds;
                }
            }

            if (std.mem.lastIndexOfScalar(u8, rest, ':')) |colon| {
                out.host = rest[0..colon];
                out.port = std.fmt.parseInt(u16, rest[colon + 1 ..], 10) catch return error.BadPort;
            } else {
                out.host = rest;
            }

            if (out.host.len == 0) out.host = default_host;
            return out;
        }

        /// One address input, one rule. `NATS_HOST` was the second input and is gone:
        /// see `args.zig`, which now warns when it is set.
        pub fn resolve(rc: *const RuntimeConfig) ParseError!Endpoint {
            var out = if (rc.nats_url) |url| try parseUrl(url) else Endpoint{};

            // Kept out of the URL deliberately: a seed is a private key and belongs in
            // its own variable, not in a string that gets logged by accident.
            out.seed = rc.nats_seed;
            out.creds = rc.nats_creds;
            return out;
        }
    };

    /// Maximum reconnection attempts (-1 = infinite)
    pub const max_reconnect_attempts = -1;

    /// Wait time between reconnection attempts (milliseconds).
    /// Was 1000 here while nats_publisher used a hardcoded 2000; 2000 is the value
    /// that was actually in effect and the one the README documents, so that wins.
    pub const reconnect_wait_ms = 2000;

    /// ── boot stream reconciliation (bridge.zig reconcileCdcStreams) ─────────
    /// Parameters for the CDC/INIT streams the bridge creates itself. max_bytes
    /// is deliberately modest: JetStream treats it as a RESERVATION against the
    /// server's storage budget (the INIT_TANGO lesson — a 10G-per-tenant boot
    /// refuses to create anything on a small server).
    pub const reconciled_cdc_max_age_days: u64 = 8;
    pub const reconciled_stream_max_bytes: i64 = 1 << 30;
    pub const reconciled_stream_max_msgs: i64 = 10_000_000;

    /// Ceiling for the publisher's exponential reconnect backoff (milliseconds)
    pub const max_backoff_ms = 30_000;
    pub const storage_type = .file;

    /// Async publish flush timeout (milliseconds)
    /// Must be >= reconnect_wait_ms * max attempts
    pub const flush_timeout_ms = 10_000; // 10 seconds
    pub const nats_flush_interval_seconds = 5; // 5 seconds
    pub const status_update_interval_ms = 100; // 100 milliseconds
    pub const status_update_byte_threshold: u64 = 1024 * 1024; // 1MB

    /// JetStream's own mapping of a KV bucket into the subject space: `$KV.<bucket>.<key>`.
    /// Not in grammar.json, and not a name anyone is free to choose — it is the server's.
    pub const kv_subject_prefix = "$KV.";

    /// How long the async publish window waits for the last outstanding PubAck before
    /// failing the whole flush into the retry path (NOTES.md §1.13). Matches the
    /// JetStream client's own request-timeout order of magnitude; a healthy colocated
    /// ack is ~1–2 ms, so hitting this means the server is gone, not slow.
    pub const publish_ack_timeout_ms: i64 = 10_000;

    // Stream names, subject prefixes, subject patterns and KV bucket names all used to
    // live here as `pub const`s fed by build.zig's @embedFile of grammar.json. They are
    // now fields on `RuntimeConfig.topology`, read from that file at startup — see
    // src/topology.zig for why the compile-time version had to go.
    //
    // What stays here is what grammar.json does not describe: retry budgets, token
    // positions, and the redelivery limits below.

    /// Redelivery budget for a mutation. Bounded so that a message the bridge
    /// misclassifies as retryable still stops eventually, instead of pinning the worker
    /// at one attempt per second for the life of the process (observed: 24 redeliveries
    /// in 15 s before this existed).
    ///
    /// Raised from 5 once `sqlstateIsPermanent` began ending the hopeless cases early.
    /// At 5 the budget was wrong in both directions at once: a privilege error burned all
    /// five attempts before the client heard anything, while five attempts a second apart
    /// could not outlast a single `Retry.pg_reconnect_delay_seconds` (5 s), so a genuine
    /// Postgres restart dead-lettered writes that would have succeeded.
    ///
    /// Now the budget is spent only on failures that might actually recover, so it can
    /// cover a few reconnects. ⚠️ The cost is paid by *unrecognised* permanent errors —
    /// they now take ~15 s to be reported rather than ~4 s. That is the right trade only
    /// while the classifier stays conservative: widen `sqlstateIsPermanent` before
    /// raising this further.
    pub const mutation_max_deliver: i32 = 15;

    /// Token positions in the mutation subject, after splitting on '.'. The *shape* of
    /// `mutation.<principal>.<table>.<operation>` is fixed here while the prefix itself
    /// comes from grammar.json: a deployment may rename the prefix, but moving the
    /// principal to another position would change what the broker's authorization
    /// wildcard covers, which is a protocol change and not a configuration one.
    pub const mutation_token_principal = 1;
    pub const mutation_token_table = 2;
    pub const mutation_token_operation = 3;
    pub const mutation_token_count = 4;

    /// Room a published message needs beyond the row's own bytes: the subject, the
    /// `Nats-Msg-Id` and content headers, one MessagePack map key per column, and the
    /// batch array framing when several events travel together. An event buffer sized
    /// right up to `max_payload` therefore still produces messages the server rejects,
    /// which is why the startup check leaves this much clearance.
    pub const payload_envelope_margin_bytes: u64 = 16 * 1024;

    pub const publisher_max_wait = 10_000; // 10 seconds

    /// JetStream stream default configuration
    pub const stream_max_msgs = 1_000_000; // Maximum messages per stream
    pub const stream_max_bytes = 1024 * 1024 * 1024; // 1GB maximum stream size
    pub const stream_max_age_ns = 60 * 1_000_000_000; // 1 minute retention in nanoseconds
};

/// HTTP metrics server configuration
pub const Http = struct {
    /// Default HTTP port for metrics endpoint
    pub const default_port = 9090;

    /// How long a connection may take to send its request line and headers.
    ///
    /// Sized for the expected traffic and nothing more: a Prometheus scrape every ~10s
    /// and the occasional `curl /status`. Both send a complete request immediately, so
    /// three seconds is already generous — this is a deadline for *silence*, not for a
    /// slow client.
    ///
    /// ⚠️ Without it a client that connects and never sends parks a task forever. That is
    /// no longer an outage (each connection runs on its own thread), but it is still an
    /// unbounded leak, and it is free to open sockets.
    pub const receive_timeout_ns: u64 = 3 * std.time.ns_per_s;

    /// Maximum connections being served at once.
    ///
    /// One scraper plus a human is two. Sixteen leaves room for a second scraper, a
    /// dashboard and a handful of stragglers still winding down, while bounding what an
    /// abusive client can allocate. Past this, connections are accepted and closed
    /// immediately rather than queued — a scraper retries in 10s, and refusing fast beats
    /// a queue nobody drains.
    pub const max_connections: u32 = 16;

    /// Concurrent /enroll requests (each dials a PG writer connection). Must FIT
    /// the writer role's CONNECTION LIMIT alongside the mutation listener and the
    /// sweeper — preflight's budget introspection warns when it does not.
    pub const max_concurrent_enrolls: u32 = 4;

    /// The writer role's slots that are NOT enrollment's to spend: the mutation
    /// listener's persistent connection plus the sweeper's reserved one (an
    /// external cron job — transient, but its slot must exist when it fires).
    pub const pg_writer_reserved_connections: u32 = 2;

    /// Invite-code length bounds accepted by /enroll. Codes are operator-minted
    /// hex (32 chars today); the range refuses garbage before any PG work.
    pub const enroll_code_min_len: usize = 16;
    pub const enroll_code_max_len: usize = 128;

    /// Lifetime of a minted user JWT. Expiry IS the revocation story: a deleted
    /// mapping stops mattering at most this long after the deletion.
    pub const enroll_jwt_ttl_seconds: i64 = 60 * 60 * 24;

    /// connect_timeout for the enroll handler's PG dial: a hung Postgres must
    /// cost an enroll permit for seconds, not forever — the permits are the
    /// flood bound, so nothing may park in one.
    pub const enroll_pg_connect_timeout_seconds: u32 = 3;

    /// HTTP server bind address, overridable with `BRIDGE_BIND`.
    ///
    /// ⚠️ Loopback by default. Every endpoint is unauthenticated and they disclose table
    /// names, replication lag and throughput — so reaching them should require being on
    /// the host, or going through a reverse proxy an operator put there on purpose.
    /// This constant said "0.0.0.0" and was read by nobody: the server hardcoded
    /// INADDR_ANY, so the declared default and the actual bind disagreed silently.
    pub const default_bind = "127.0.0.1";

};

/// Batch publishing configuration
pub const Batch = struct {
    /// Maximum number of events to batch before flushing
    pub const max_events = 5000;

    /// Maximum time to wait before flushing an incomplete batch
    pub const max_age_ms = 500;

    pub const max_payload_bytes = 256 * 1024; // 256KB

    /// Columns one CDC event can carry — now a **per-instance runtime value**, not a
    /// compile constant. `CDCEvent.columns` is a slice into a separately-allocated
    /// "columns slab" (mirroring `data_buffer`/`data_slab`), sized once at boot for
    /// `RuntimeConfig.max_columns`, so the cost is `RING_BUFFER_COUNT × max_columns ×
    /// 8 bytes` — paid for what the actual monitored tables need, not a fixed guess.
    ///
    /// `max_columns` itself is resolved at boot (see `bridge.zig`'s `resolveMaxColumns`):
    /// `MAX_COLUMNS` if set, otherwise auto-detected as the widest monitored table's
    /// live column count, rounded up by `column_headroom_rounding` for migration
    /// headroom. It is a **budget, not a format limit** — PostgreSQL allows 1600
    /// columns; a table past this one is refused with `TooManyColumns` — loudly, with
    /// its events dropped and its clients told a reason — rather than truncated.
    ///
    /// These four constants only bound that resolution:
    /// `MAX_COLUMNS`'s floor. A table narrower than this is implausible enough that
    /// going lower is almost certainly a units mistake, not a real deployment —
    /// mirrors `Buffers.min_event_data_buffer_log2`'s reasoning.
    pub const min_columns: u16 = 8;

    /// The fallback when auto-detection cannot run (no PG connection at resolution
    /// time) and `MAX_COLUMNS` is unset. 128 because a table that wide is already
    /// unusual, and the widest table in this repo's own fixtures is 14.
    pub const default_max_columns: u16 = 128;

    /// Hard outer bound — PostgreSQL's own column ceiling (`MaxHeapAttributeNumber`).
    /// Nothing, auto-detected or `MAX_COLUMNS`-overridden, can ask for more than this;
    /// a typo (`MAX_COLUMNS=99999`) clamps here rather than sizing a slab for it.
    pub const absolute_max_columns: u16 = 1600;

    /// Auto-detection rounds the widest monitored table's column count UP to the next
    /// multiple of this, so a routine `ALTER TABLE ADD COLUMN` has headroom to land
    /// without immediately hitting `TooManyColumns` and forcing a bridge reboot to
    /// re-detect. Reasonable slack without meaningfully inflating the slab.
    pub const column_headroom_rounding: u16 = 8;
    // NOTE: the ring buffer size lives in Buffers.default_ring_buffer_count, which is
    // what RuntimeConfig actually reads. A `Batch.ring_buffer_size` used to be
    // declared here with the sizing rationale attached, but nothing referenced it —
    // the reasoning has moved next to the live constant.
};

/// WAL monitoring configuration
pub const WalMonitor = struct {
    /// Default check interval (seconds)
    pub const default_check_interval_seconds = 30;

};

pub const Bridge = struct {
    pub const keepalive_interval_seconds = 30;
};

/// Edge-write (ingress) configuration.
pub const Sync = struct {
    /// The column compared for last-write-wins, when a table does not name its own in
    /// SYNC_RULES. `updated_at` because that is what Ecto's `timestamps()` and Rails'
    /// `t.timestamps` produce; Django and TypeORM differ, which is exactly why it is a
    /// default rather than a rule.
    pub const default_version_column = "updated_at";

    /// The session setting the bridge stamps with the authenticated principal before every
    /// mutation, and that row-level policies read back.
    ///
    /// ⚠️ **This name exists in two places and they must match**: here, and in the policies
    /// `zebridge_scope_publication_to_one_tenant()` creates in `init.{core,write}.template.sql`. A mismatch is
    /// silent in the worst way — `current_setting(..., true)` returns NULL for an unknown
    /// setting, every policy predicate evaluates to NULL, and **every write is refused**
    /// with `new row violates row-level security policy`. Nothing names the real cause.
    pub const principal_setting = "zb.principal";

    /// The session setting the generation producer stamps with the tenant before
    /// every content query, and that `zebridge_scope_reads_by_tenant()`'s
    /// `zb_reader_all` policy reads back — PROTOCOL.md "The Connection Flow" Step 0,
    /// NOTES.md §1.13.
    ///
    /// ⚠️ Same failure shape as `principal_setting` above if this drifts from the name
    /// `zebridge_scope_reads_by_tenant()` uses in `init.core.template.sql`: a mismatch
    /// is silent, `current_setting(..., true)` reads as empty either way (not NULL —
    /// see that function's own comment on the `coalesce(..., '') = ''` trap), and the
    /// policy falls through to its wholesale branch instead of filtering — a chain
    /// build that silently carries every tenant's rows instead of refusing outright.
    pub const tenant_setting = "zb.tenant";

    /// Column-name prefixes that can never be a version column, whatever the operator
    /// configures. Both are set once at insert and never touched again, so as a version
    /// they either reject every update (`stored < incoming` is false forever) or, if the
    /// bridge wrote to them, destroy the column's meaning for the application.
    pub const creation_column_prefixes = [_][]const u8{ "created", "inserted" };

    /// `atttypmod` for a timestamp is its fractional-second precision; -1 means the
    /// default, which is 6. Below this, ties are common — and a tie is *rejected* by
    /// `<`, so a legitimate edit is dropped in silence.
    pub const min_timestamp_precision = 6;

    /// How far ahead of the database's clock a client's version may be before the bridge
    /// caps it — PROTOCOL.md §7.2.
    ///
    /// A timestamp version from a skewed client is not merely wrong, it is **sticky**:
    /// stored a year ahead, the row rejects every subsequent write (`stored < incoming`
    /// stays false) until wall-clock time catches up, and nothing reports it. Clamping
    /// keeps the write — the client's data is not the problem, its clock is — while
    /// bounding how long the row can be frozen to this window.
    ///
    /// ⚠️ Not zero. Benign skew of a few seconds is normal between a browser and a
    /// server, and clamping every write to `now()` would quietly relabel writes that
    /// arrived in a legitimate order, turning a clock question into an ordering one.
    ///
    /// ⚠️ Compared against the **database's** `now()`, never the bridge's clock: it is
    /// the same clock every other writer's `updated_at` comes from, and it costs nothing
    /// because the comparison happens inside the statement.
    ///
    /// Applies only to timestamp version columns. An integer version has no future.
    pub const version_future_tolerance = "5 seconds";
};

/// Logging and metrics configuration
pub const Metrics = struct {

    pub const metric_log_interval_seconds = 15;
};

/// Event classification and semantic routing configuration
/// Enables intelligent routing of CDC events based on business logic state transitions
///
/// Instead of hardcoded column names, transition rules are configured per-table at runtime.
/// The bridge doesn't make assumptions about which columns are semantically important -
/// that's domain knowledge that belongs in the application configuration.
///
/// Example configuration via environment variable:
///   TRANSITION_RULES=users:status,kyc_level;orders:state,payment_status
///
/// This creates table-specific rules:
///   - "users" table watches: status, kyc_level
///   - "orders" table watches: state, payment_status
///   - Other tables: no transition detection (zero overhead)
pub const EventClassification = struct {
    /// Per-table transition column rules
    /// Key: table name (e.g., "users", "orders")
    /// Value: list of column names to watch for transitions
    pub const TransitionRules = std.StringHashMap([]const []const u8);
};

/// Reconnection and retry configuration
/// Retry and backoff defaults.
///
/// These are the compile-time defaults; the operationally interesting ones are
/// overridable at runtime via RuntimeConfig (see args.zig for the env names).
/// Values here were previously duplicated as literals across batch_publisher
/// and event_processor — identical by intention but free to
/// drift, which is exactly the failure this section exists to prevent.
pub const Retry = struct {
    /// PostgreSQL reconnection delay (seconds)
    pub const pg_reconnect_delay_seconds = 5;

    /// Publish retry budget for the CDC batch publisher. Exhausting it is fatal: the bridge stops rather than ACK an LSN
    /// whose data never reached NATS.
    pub const publish_max_retries = 5;

    /// First backoff after a failed publish; doubles each attempt up to the cap.
    pub const publish_backoff_ms = 100;
    pub const publish_max_backoff_ms = 5_000;

    /// While backing off, wake this often to notice a shutdown request rather than
    /// sleeping through the whole interval.
    pub const shutdown_poll_ms = 50;

    /// Delay between NATS reconnection attempts in the listener threads.
    pub const nats_reconnect_delay_ms = 2_000;

    /// How many times a listener thread may fail to establish its **first** NATS
    /// connection before the bridge gives up and stops.
    ///
    /// Only the first one is bounded. A drop after the listener has worked once is a
    /// real outage and must be retried forever — that is what the outer loop is for.
    /// But a listener that has *never* connected is describing a configuration fault,
    /// not an outage: the wrong host, a rejected nkey, or a stream that was never
    /// created. Left unbounded, the bridge logged one line every 2s and otherwise
    /// looked healthy — `/health` green, CDC flowing — while the listener thread was
    /// spinning on a connection it would never get.
    ///
    /// 5 × `nats_reconnect_delay_ms` ≈ 10s, enough to ride out a NATS container still
    /// coming up beside the bridge, short enough to be an obvious startup failure.
    pub const listener_boot_connect_attempts: u32 = 5;

    /// How many spins on a full ring buffer before checking whether the flush
    /// thread has died. Internal tuning, not worth exposing.
    pub const spins_before_fatal_check = 1_000;

};

/// Buffer sizes
pub const Buffers = struct {
    /// Subject buffer size (for formatting NATS subjects)
    pub const subject_buffer_size = 128;

    /// Message ID buffer size
    pub const msg_id_buffer_size = 128;

    /// Event data buffer size (per-event packed column storage), as log2 bytes.
    /// Configurable via BASE_BUF: BASE_BUF=16 → 64KB, 14 → 16KB.
    ///
    /// A row larger than this **suspends its table** (`reason: "row_too_large"`) and the
    /// bridge keeps running — it used to `@panic`, which crash-looped under a supervisor
    /// because the offending row precedes any later ACK and is re-read on restart.
    /// See README "Sizing BASE_BUF and RING_BUFFER_COUNT" for the memory formula.
    pub const default_event_data_buffer_log2: u6 = 12; // 2^12 = 4KB

    /// `BASE_BUF`'s floor. Nothing structural sits at 2^10 — it is just small enough
    /// that going lower is almost certainly a units mistake, not a deployment that
    /// genuinely wants a 512-byte event buffer.
    pub const min_event_data_buffer_log2: u6 = 10; // 2^10 = 1KB

    /// `BASE_BUF`'s **default** ceiling — the value every one of the three places that
    /// used to restate "1 MiB" independently (a bare `20` in args.zig's clamp, a bare
    /// `1024*1024` in batch_publisher.zig's startup validation, and prose in a doc
    /// comment nothing actually referenced) now reads instead. Raising the ceiling used
    /// to mean remembering all three; forgetting one left two different ceilings
    /// silently enforced against the same setting.
    ///
    /// 1 MiB is not structural — `CDCEvent.ColumnView`'s offsets are `u32`, room for far
    /// more — it is NATS's own out-of-the-box `max_payload`. A deployment that has
    /// raised its server's `max_payload` can raise this too, **per instance**, via
    /// `BASE_BUF_MAX` (see args.zig) — without a rebuild, the same way `BASE_BUF` itself
    /// is already tuned per instance, because one host can run several bridges against
    /// tables of very different shapes on the same WAL (see `RuntimeConfig
    /// .event_buffer_ceiling_log2`, the resolved per-instance value; this constant is
    /// only the compiled-in default when `BASE_BUF_MAX` is unset).
    ///
    /// ⚠️ This ceiling does not, by itself, prove a row can be published — a buffer
    /// sized right up to it can still exceed what *this* server actually advertises.
    /// That is checked separately, at connect time, against the live value
    /// (`nats_publisher.serverMaxPayload`, enforced in bridge.zig as
    /// `EventBufferExceedsMaxPayload`). This constant only bounds how far
    /// `BASE_BUF`/`BASE_BUF_MAX` can be pushed before that live check gets to run —
    /// think of it as "the largest value worth trying," not "the largest value that
    /// will work."
    pub const default_max_event_data_buffer_log2: u6 = 20; // 2^20 = 1MiB

    /// Hard outer bound on `BASE_BUF_MAX` itself, so a typo (`BASE_BUF_MAX=200`) cannot
    /// ask for a multi-exabyte per-slot buffer, silently multiplied by RING_BUFFER_COUNT
    /// on top. 24 = 16 MiB is already far past any NATS max_payload this project has
    /// been run against.
    pub const absolute_max_event_data_buffer_log2: u6 = 24;

    /// Ring buffer event count (number of pre-allocated event slots)
    /// Default: 32768 events
    /// Configurable via environment variable RING_BUFFER_COUNT
    /// Total memory = event_count × event_buffer_size
    /// Example: 65536 slots × 16KB (BASE_BUF=14) = 1GB slab
    ///
    /// Sizing rationale — the buffer is what absorbs a NATS outage before the
    /// producer has to backpressure and let WAL accumulate:
    ///   65536 slots ≈ 1092ms of headroom at 60K events/s
    ///
    /// That headroom used to exceed one NATS reconnect interval (then 1000ms).
    /// Nats.reconnect_wait_ms is now 2000ms — the value that was actually in
    /// effect and that the README documents — so the buffer covers roughly half
    /// a reconnect interval, not a whole one.
    ///
    /// This is safe, not broken: a full ring makes acquireAndFillSlot backpressure
    /// the WAL reader, so events are delayed, never dropped. But the old "one
    /// reconnect fits entirely in RAM" property is gone. To restore it, raise
    /// RING_BUFFER_COUNT to 131072 (≈2184ms, ~4MB slab) rather than shortening
    /// the reconnect wait.
    pub const default_ring_buffer_count: usize = 32768;

    /// `RING_BUFFER_COUNT`'s clamp range. Named here rather than left as bare literals
    /// in args.zig so the `--help` usage string can be generated from the same numbers
    /// it describes, instead of restating them as prose that can drift.
    pub const min_ring_buffer_count: usize = 1024;
    pub const max_ring_buffer_count: usize = 1024 * 1024;
};

/// Memory-layout bounds used for compile-time pipeline safety checks.
///
/// The core invariant: ring_buffer_capacity > max_rows_per_transaction.
/// If a single transaction could claim every ring buffer slot before its
/// .commit arrives, acquireAndFillSlot would spin forever because the
/// background flush thread cannot reclaim from an empty pending_events queue.
/// A strict gap ensures at least one slot always remains free for the
/// background thread, breaking any potential deadlock.
///
/// These are the DEFAULT values. RING_BUFFER_COUNT env-var overrides the
/// runtime ring buffer; the bridge derives max_tx_rows = runtime_size - 1
/// to maintain the invariant regardless of env-var value.
pub const MemoryBounds = struct {
    pub const ring_buffer_capacity: usize = Buffers.default_ring_buffer_count;
    pub const max_rows_per_transaction: usize = ring_buffer_capacity - 1;
};

/// Runtime configuration combining compile-time defaults with CLI arguments and environment variables
/// This struct should be passed to modules instead of having them import config.zig directly
/// Delta-generation producer (NOTES.md §1.13). Enabled by GENERATION_RULES.
pub const Generations = struct {
    pub const default_cadence_seconds: u64 = 600;
    pub const min_cadence_seconds: u64 = 5;
    pub const max_cadence_seconds: u64 = 86_400;
    /// Generations kept per (tenant, table) — the delta chain depth k. Coupled to the
    /// sweeper by the correctness inequality: sweeper retention ≥ k × cadence.
    pub const default_chain_depth: u32 = 6;
    // The KV bucket and per-tenant object-bucket prefix live in grammar.json
    // (`"generations": {kv, bucket_prefix}`) — one file, three readers, same as every
    // other wire name. Only pacing stays here.
};

pub const RuntimeConfig = struct {
    // HTTP
    http_port: u16,

    /// `DATABASE_READER_URL` — the read/replication connection, credentials and all.
    ///
    /// Required, with **no fallback to PG_HOST/PG_USER/PG_PASSWORD**. Those name the
    /// superuser `bridge-init` uses to create roles; they live in the same environment,
    /// and while the bridge accepted them a missing or misspelled DATABASE_READER_URL meant
    /// connecting as `postgres` and looking perfectly healthy. `sslmode` belongs in the
    /// URL's query string, where it stays a stated decision rather than whatever libpq's
    /// `prefer` happens to negotiate.
    db_url: []const u8,
    /// `DATABASE_WRITER_URL` — the ingress connection, under its own role.
    ///
    /// Null means "no writer configured": the mutation listener does not start, rather
    /// than quietly falling back to the read role. Falling back would mean the ingress
    /// path silently runs with replication rights — the exact privilege the split
    /// exists to avoid. That role also has no table privileges until a DBA opens one
    /// (`zebridge_grant_edge_writes`).
    pg_writer_url: ?[]const u8,

    // PostgreSQL replication
    slot_name: []const u8,
    publication_name: []const u8,
    generation_cadence_seconds: u64 = Generations.default_cadence_seconds,
    generation_chain_depth: u32 = Generations.default_chain_depth,
    /// Master switch: derive-and-produce for every published table. GENERATION_RULES
    /// alone also enables the producer, as a RESTRICTION (probes, dev subsets).
    generations_enabled: bool = false,

    /// Every wire name, read from grammar.json at startup. Carried on RuntimeConfig
    /// because that is already threaded to each component that publishes or subscribes,
    /// which is what keeps one file the single source for the bridge, `nats-init` and
    /// the clients alike.
    topology: Topo.Topology,

    // NATS
    /// `nats://[user:pass@]host[:port]` — the only address input. Optional here only
    /// because `defaults()` predates parsing; `args.zig` always fills it in.
    nats_url: ?[]const u8,
    nats_seed: ?[]const u8, // Optional NKey Seed
    nats_creds: ?[]const u8, // Optional .creds path (operator/JWT mode; wins over the seed)

    // Batch settings
    batch_max_events: usize,
    batch_max_wait_ms: i64,
    batch_max_payload_bytes: usize,
    batch_ring_buffer_size: usize,

    /// Refuse to start when any published table has no primary key (STRICT_TABLES).
    strict_tables: bool,

    // Publish retry budget (see Retry section for the rationale)
    publish_max_retries: u32,
    publish_backoff_ms: u64,
    publish_max_backoff_ms: u64,

    // Buffer settings
    event_data_buffer_log2: u6,
    /// The ceiling `event_data_buffer_log2` (`BASE_BUF`) may not exceed for *this*
    /// instance — `Buffers.default_max_event_data_buffer_log2` unless `BASE_BUF_MAX`
    /// raised it. Threaded through so batch_publisher.zig's startup validation checks
    /// against the same number args.zig's clamp already enforced, rather than a second,
    /// independently-hardcoded ceiling that could disagree with it.
    event_data_buffer_max_log2: u6,

    /// `MAX_COLUMNS`, as parsed by args.zig — `null` means "not set, auto-detect at
    /// boot from the publication's actual tables" (see `bridge.zig`'s
    /// `resolveMaxColumns`, which needs a PG connection args.zig doesn't have).
    /// Non-null is an explicit operator override, already clamped to
    /// `[Batch.min_columns, Batch.absolute_max_columns]`.
    ///
    /// This field is never mutated after `parseArgs` — the value `BatchPublisher.init`
    /// actually allocates for is `main`'s locally-resolved `resolved_max_columns`,
    /// threaded through as its own parameter rather than written back here, so a
    /// reader never has to wonder whether this field means "what was configured" or
    /// "what got decided."
    max_columns_override: ?u16,

    /// Create default runtime configuration from compile-time constants
    /// Note: PostgreSQL connection fields are set to defaults that should be overridden from environment
    pub fn defaults() RuntimeConfig {
        return .{
            .http_port = Http.default_port,
            // Not a usable connection on purpose: `parseArgs` requires DATABASE_READER_URL and
            // fails without it, so nothing should ever reach a default here.
            .db_url = "",
            .pg_writer_url = null,
            // Unusable on purpose, like `db_url` above: `parseArgs` overwrites both
            // and refuses to return when either name is unset, so nothing reaches these.
            .slot_name = "",
            .publication_name = "",
            // Always replaced in `main` immediately after parseArgs. Safe as a default
            // only because a test asserts `for_tests` equals the repository's own
            // grammar.json, so the two cannot drift.
            .topology = Topo.Topology.for_tests,
            .nats_url = Nats.default_url,
            .nats_seed = null,
            .nats_creds = null,
            .batch_max_events = Batch.max_events,
            .batch_max_wait_ms = Batch.max_age_ms,
            .batch_max_payload_bytes = Batch.max_payload_bytes,
            .batch_ring_buffer_size = Buffers.default_ring_buffer_count,
            .strict_tables = false,
            .publish_max_retries = Retry.publish_max_retries,
            .publish_backoff_ms = Retry.publish_backoff_ms,
            .publish_max_backoff_ms = Retry.publish_max_backoff_ms,
            .event_data_buffer_log2 = Buffers.default_event_data_buffer_log2,
            .event_data_buffer_max_log2 = Buffers.default_max_event_data_buffer_log2,
            .max_columns_override = null,
        };
    }

    // No deinit: every string here is either a compile-time default or a slice
    // borrowed from argv/environ, all of which outlive the process. See parseArgs.
};

/// Resolve the runtime log level from `LOG_LEVEL`.
///
/// Takes the environ from `std.process.Init` rather than calling `std.c.getenv`, so it
/// reads the same block as every other setting (see args.zig) instead of a second,
/// libc-dependent path.
///
/// Deliberately silent: this runs *before* the caller has assigned the level it
/// returns, so anything it logged at debug would be filtered by the level still in
/// effect — which is exactly why the old "Level---------->" line never appeared. The
/// caller logs the outcome after applying it. An unrecognised value is worth a warning
/// though: `warn` passes the default filter, and silently running at info when you
/// asked for debug is the confusing case.
pub fn getDefaultLogLevel(init: *const std.process.Init) std.log.Level {
    const raw = init.minimal.environ.getPosix("LOG_LEVEL") orelse return .info;

    // Both spellings of the two ambiguous levels are accepted on purpose. Zig names the
    // enum tags `.warn` and `.err`, but `std.log` *prints* "warning" and "error" — so
    // the obvious thing to type is whatever you last saw in the log, and being strict
    // here would reject it. This is the one place the two vocabularies meet.
    if (std.mem.eql(u8, raw, "debug")) return .debug;
    if (std.mem.eql(u8, raw, "info")) return .info;
    if (std.mem.eql(u8, raw, "warn") or std.mem.eql(u8, raw, "warning")) return .warn;
    if (std.mem.eql(u8, raw, "err") or std.mem.eql(u8, raw, "error")) return .err;

    log.warn("LOG_LEVEL='{s}' is not one of debug|info|warn(ing)|err(or) — using info", .{raw});
    return .info;
}

// ─── Nats.Endpoint ──────────────────────────────────────────────────────────────
//
// One resolution rule for the whole process. These are regression tests for a real
// split-brain: NATS_HOST=nats-server beside a working NATS_URL made ingress dial a
// name that resolves only inside compose while CDC and snapshots ran fine.

test "Endpoint.parseUrl: host and port" {
    const ep = try Nats.Endpoint.parseUrl("nats://10.0.0.4:5222");
    try std.testing.expectEqualStrings("10.0.0.4", ep.host);
    try std.testing.expectEqual(@as(u16, 5222), ep.port);
    try std.testing.expect(ep.user == null);
}

test "Endpoint.parseUrl: no port falls back to the default" {
    const ep = try Nats.Endpoint.parseUrl("nats://nats-server");
    try std.testing.expectEqualStrings("nats-server", ep.host);
    try std.testing.expectEqual(@as(u16, Nats.default_port), ep.port);
}

test "Endpoint.parseUrl: credentials are kept" {
    // Dropping these leaves the client waiting forever against a server that requires
    // authorization, with no error surfaced anywhere.
    const ep = try Nats.Endpoint.parseUrl("nats://alice:s3cret@host:4222");
    try std.testing.expectEqualStrings("alice", ep.user.?);
    try std.testing.expectEqualStrings("s3cret", ep.pass.?);
    try std.testing.expectEqualStrings("host", ep.host);
    try std.testing.expectEqual(@as(u16, 4222), ep.port);
}

test "Endpoint.parseUrl: a password may contain '@'" {
    // Split on the rightmost '@', or the host becomes a fragment of the password.
    const ep = try Nats.Endpoint.parseUrl("nats://bob:p@ss@10.1.2.3:4222");
    try std.testing.expectEqualStrings("bob", ep.user.?);
    try std.testing.expectEqualStrings("p@ss", ep.pass.?);
    try std.testing.expectEqualStrings("10.1.2.3", ep.host);
}

test "Endpoint.parseUrl: a trailing path is not part of the address" {
    const ep = try Nats.Endpoint.parseUrl("nats://host:4222/");
    try std.testing.expectEqualStrings("host", ep.host);
    try std.testing.expectEqual(@as(u16, 4222), ep.port);
}

test "Endpoint.parseUrl: rejects what it cannot resolve" {
    try std.testing.expectError(error.MissingScheme, Nats.Endpoint.parseUrl("127.0.0.1:4222"));
    // `tls://` is refused rather than silently downgraded to plaintext: TLS was never
    // made to work with the vendored client, so accepting the scheme would promise
    // encryption the connection does not have. See COPY_BINARY_PLAN "encryption in
    // transit".
    try std.testing.expectError(error.MissingScheme, Nats.Endpoint.parseUrl("tls://host:4222"));
    try std.testing.expectError(error.BadPort, Nats.Endpoint.parseUrl("nats://host:not-a-port"));
}

test "Endpoint.resolve: the URL is the address" {
    var rc = RuntimeConfig.defaults();
    rc.nats_url = "nats://10.9.8.7:5222";

    const ep = try Nats.Endpoint.resolve(&rc);
    try std.testing.expectEqualStrings("10.9.8.7", ep.host);
    try std.testing.expectEqual(@as(u16, 5222), ep.port);
}

test "Endpoint.resolve: no URL falls back to the compiled default, not to a second variable" {
    var rc = RuntimeConfig.defaults();
    rc.nats_url = null;

    const ep = try Nats.Endpoint.resolve(&rc);
    try std.testing.expectEqualStrings(Nats.default_host, ep.host);
    try std.testing.expectEqual(@as(u16, Nats.default_port), ep.port);
}

test "Endpoint.resolve: the seed rides along, never through the URL" {
    var rc = RuntimeConfig.defaults();
    rc.nats_seed = "SUAxxxx";
    const ep = try Nats.Endpoint.resolve(&rc);
    try std.testing.expectEqualStrings("SUAxxxx", ep.seed.?);
}
