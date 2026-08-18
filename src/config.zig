//! Centralized configuration for CDC Bridge
//!
//! This module consolidates all configuration constants used throughout the application.
//! Instead of hardcoded values scattered across files, all tunables are defined here.

const std = @import("std");
const encoder = @import("encoder.zig");
const Topo = @import("topology.zig");

pub const log = std.log.scoped(.config);

/// PostgreSQL connection and replication configuration
pub const Postgres = struct {
    pub const default_slot_name = "cdc_slot";
    pub const default_publication_name = "cdc_pub";
    pub const connection_timeout_ms = 5000; // milliseconds

    /// Replication connection receive timeout (milliseconds)
    /// Set to 0 for blocking mode
    pub const replication_receive_timeout_ms = 0;

    /// WAL sender timeout (PostgreSQL server-side, configured in docker-compose.yml)
    /// This is just for documentation - actual value set in PostgreSQL config
    pub const wal_sender_timeout_seconds = 300; // 5 minutes

    /// Maximum WAL retention size (10GB)
    pub const max_wal_retention_gb = 10;

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
            return out;
        }
    };

    /// Maximum reconnection attempts (-1 = infinite)
    pub const max_reconnect_attempts = -1;

    /// Wait time between reconnection attempts (milliseconds).
    /// Was 1000 here while nats_publisher used a hardcoded 2000; 2000 is the value
    /// that was actually in effect and the one the README documents, so that wins.
    pub const reconnect_wait_ms = 2000;

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
    /// Not in topology.json, and not a name anyone is free to choose — it is the server's.
    pub const kv_subject_prefix = "$KV.";

    // Stream names, subject prefixes, subject patterns and KV bucket names all used to
    // live here as `pub const`s fed by build.zig's @embedFile of topology.json. They are
    // now fields on `RuntimeConfig.topology`, read from that file at startup — see
    // src/topology.zig for why the compile-time version had to go.
    //
    // What stays here is what topology.json does not describe: retry budgets, token
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
    /// comes from topology.json: a deployment may rename the prefix, but moving the
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

    /// HTTP server bind address, overridable with `BRIDGE_BIND`.
    ///
    /// ⚠️ Loopback by default. Every endpoint is unauthenticated and they disclose table
    /// names, replication lag and throughput — so reaching them should require being on
    /// the host, or going through a reverse proxy an operator put there on purpose.
    /// This constant said "0.0.0.0" and was read by nobody: the server hardcoded
    /// INADDR_ANY, so the declared default and the actual bind disagreed silently.
    pub const default_bind = "127.0.0.1";

    /// Metrics endpoint path
    pub const metrics_path = "/metrics";

    /// Health check endpoint path
    pub const health_path = "/health";
};

/// Batch publishing configuration
pub const Batch = struct {
    /// Maximum number of events to batch before flushing
    pub const max_events = 5000;

    /// Maximum time to wait before flushing an incomplete batch
    pub const max_age_ms = 500;

    pub const max_payload_bytes = 256 * 1024; // 256KB
    // NOTE: the ring buffer size lives in Buffers.default_ring_buffer_count, which is
    // what RuntimeConfig actually reads. A `Batch.ring_buffer_size` used to be
    // declared here with the sizing rationale attached, but nothing referenced it —
    // the reasoning has moved next to the live constant.
};

/// WAL monitoring configuration
pub const WalMonitor = struct {
    /// Default check interval (seconds)
    pub const default_check_interval_seconds = 30;

    /// Warning threshold for WAL lag (bytes)
    pub const warning_threshold_bytes = 512 * 1024 * 1024; // 512MB

    /// Critical threshold for WAL lag (bytes)
    pub const critical_threshold_bytes = 1024 * 1024 * 1024; // 1GB
};

pub const Bridge = struct {
    /// Maximum size of a single CDC message (bytes)
    pub const max_cdc_message_size_bytes = 900 * 1024; // 900KB
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
    /// `zebridge_publish_tenant_table()` creates in `init.sql.template`. A mismatch is
    /// silent in the worst way — `current_setting(..., true)` returns NULL for an unknown
    /// setting, every policy predicate evaluates to NULL, and **every write is refused**
    /// with `new row violates row-level security policy`. Nothing names the real cause.
    pub const principal_setting = "zb.principal";

    /// Column-name prefixes that can never be a version column, whatever the operator
    /// configures. Both are set once at insert and never touched again, so as a version
    /// they either reject every update (`stored < incoming` is false forever) or, if the
    /// bridge wrote to them, destroy the column's meaning for the application.
    pub const creation_column_prefixes = [_][]const u8{ "created", "inserted" };

    /// `atttypmod` for a timestamp is its fractional-second precision; -1 means the
    /// default, which is 6. Below this, ties are common — and a tie is *rejected* by
    /// `<`, so a legitimate edit is dropped in silence.
    pub const min_timestamp_precision = 6;
};

/// Snapshot generation configuration
pub const Snapshot = struct {
    // Snapshot identifiers

    /// **Ceiling** on rows per snapshot chunk, not the row count.
    ///
    /// The real limit is bytes: the chunk query asks Postgres for the longest prefix whose
    /// cumulative row size fits one NATS message, and this only stops a table of tiny rows
    /// from asking for a million of them at once. A chunk of 10 000 rows is what a narrow
    /// table gets; a table of 256 KiB blobs gets three.
    pub const chunk_size = 10_000;

    /// How full a chunk aims to be: 4/5 of the message budget.
    ///
    /// The row count is derived from the *average* row size, and a run of above-average
    /// rows still has to fit. The running sum in the chunk query enforces the budget
    /// exactly — this only decides how often the encoder has to re-encode a prefix, and
    /// aiming at 100% would guarantee it.
    pub const chunk_fill_num = 4;
    pub const chunk_fill_den = 5;

    /// Ceiling on the encode buffer, which is otherwise sized to the server's
    /// `max_payload`. A NATS server advertising 8 MiB should not turn into an 8 MiB
    /// resident buffer. One buffer exists at a time — snapshots run sequentially on a
    /// single thread — so this is a ceiling on the bridge, not per request.
    pub const encode_buffer_max_bytes = 2 * 1024 * 1024;

    // Snapshot freshness used to be a bridge-local cache (`SnapshotCache`), which
    // could not coordinate across bridge instances and died on restart. It is now the
    // REQUESTS stream's own policy — max-msgs-per-subject=1, discard=new, max-age —
    // so the window is enforced by the broker for every client, and SNAP_RET_SECONDS
    // configures nats-init rather than the bridge.

    /// ⚠️ **Declared, never read.** Snapshots are not concurrent: `listenForSnapshotRequests`
    /// calls `generateIncrementalSnapshot` inline, so requests are served strictly one at a
    /// time in arrival order, and a large table blocks the queue behind it. Left here
    /// because the number states an intent — a worker pool — that was never built. Delete
    /// it or implement it, but do not read it as describing current behaviour.
    pub const max_concurrent_snapshots = 3;

    /// How long the broker waits for the snapshot worker's ack before redelivering.
    /// Generous because generation is synchronous and a large table takes minutes; the
    /// default 30s would redeliver a request whose COPY is still running.
    pub const request_ack_wait_ns: u64 = 10 * 60 * std.time.ns_per_s;

    /// A snapshot request that keeps failing must stop being redelivered.
    pub const request_max_deliver: i32 = 3;

    /// Snapshot polling interval (milliseconds)
    /// How often to check for new snapshot requests via LISTEN/NOTIFY
    pub const poll_interval_ms = 100;

    // Snapshot subject patterns moved to `RuntimeConfig.topology` (src/topology.zig).

    /// Message ID pattern for data chunks: "snap-<table>-<snapshot_id>-<chunk>"
    pub const data_msg_id_pattern = "snap-{s}-{s}-{d}";
};

/// Logging and metrics configuration
pub const Metrics = struct {
    /// Metric log interval (seconds)
    /// How often to emit structured metric logs for observability
    pub const log_interval_seconds = 60;

    /// Enable debug logging
    pub const debug_enabled = false;
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
/// Values here were previously duplicated as literals across batch_publisher,
/// snapshot_listener and event_processor — identical by intention but free to
/// drift, which is exactly the failure this section exists to prevent.
pub const Retry = struct {
    /// PostgreSQL reconnection delay (seconds)
    pub const pg_reconnect_delay_seconds = 5;

    /// Publish retry budget, shared by the CDC batch publisher and the snapshot
    /// publisher. Exhausting it is fatal: the bridge stops rather than ACK an LSN
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
    /// not an outage: the wrong host, a rejected nkey, or a `REQUESTS` stream that
    /// `nats-init` never created. Left unbounded, the bridge logged one line every 2s
    /// and otherwise looked healthy — `/health` green, CDC flowing — while no client
    /// could ever bootstrap, because the thread that answers snapshot requests was
    /// spinning on a connection it would never get.
    ///
    /// 5 × `nats_reconnect_delay_ms` ≈ 10s, enough to ride out a NATS container still
    /// coming up beside the bridge, short enough to be an obvious startup failure.
    pub const listener_boot_connect_attempts: u32 = 5;

    /// How many spins on a full ring buffer before checking whether the flush
    /// thread has died. Internal tuning, not worth exposing.
    pub const spins_before_fatal_check = 1_000;

    /// Hard ceiling on a stalled flush thread before declaring the NATS client
    /// hung and forcing shutdown (nanoseconds).
    pub const flush_stall_timeout_ns = 30 * std.time.ns_per_s;
};

/// Threading configuration
pub const Threading = struct {
    /// Number of WAL monitor threads
    pub const wal_monitor_threads = 1;

    /// Number of snapshot generator threads
    pub const snapshot_generator_threads = 1;

    /// Number of HTTP server threads
    pub const http_server_threads = 1;

    /// Main loop sleep interval when idle (milliseconds)
    pub const main_loop_sleep_ms = 1;
};

/// Buffer sizes
pub const Buffers = struct {
    /// Subject buffer size (for formatting NATS subjects)
    pub const subject_buffer_size = 128;

    /// Message ID buffer size
    pub const msg_id_buffer_size = 128;

    /// Connection string buffer size
    pub const conninfo_buffer_size = 512;

    /// URL buffer size
    pub const url_buffer_size = 256;

    /// Event data buffer size (per-event packed column storage), as log2 bytes.
    /// Configurable via BASE_BUF (range 10-20): BASE_BUF=16 → 64KB, 14 → 16KB.
    ///
    /// A row larger than this **suspends its table** (`reason: "row_too_large"`) and the
    /// bridge keeps running — it used to `@panic`, which crash-looped under a supervisor
    /// because the offending row precedes any later ACK and is re-read on restart.
    /// See README "Sizing BASE_BUF and RING_BUFFER_COUNT" for the memory formula.
    pub const default_event_data_buffer_log2: u6 = 14; // 2^14 = 16KB

    /// Ring buffer event count (number of pre-allocated event slots)
    /// Default: 65536 events
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
    pub const default_ring_buffer_count: usize = 65536;
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
pub const RuntimeConfig = struct {
    // HTTP
    http_port: u16,

    /// `DATABASE_URL` — the read/replication connection, credentials and all.
    ///
    /// Required, with **no fallback to PG_HOST/PG_USER/PG_PASSWORD**. Those name the
    /// superuser `bridge-init` uses to create roles; they live in the same environment,
    /// and while the bridge accepted them a missing or misspelled DATABASE_URL meant
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

    /// Every wire name, read from topology.json at startup. Carried on RuntimeConfig
    /// because that is already threaded to each component that publishes or subscribes,
    /// which is what keeps one file the single source for the bridge, `nats-init` and
    /// the clients alike.
    topology: Topo.Topology,

    // NATS
    /// `nats://[user:pass@]host[:port]` — the only address input. Optional here only
    /// because `defaults()` predates parsing; `args.zig` always fills it in.
    nats_url: ?[]const u8,
    nats_seed: ?[]const u8, // Optional NKey Seed

    // Batch settings
    batch_max_events: usize,
    batch_max_wait_ms: i64,
    batch_max_payload_bytes: usize,
    batch_ring_buffer_size: usize,

    // Snapshot settings
    snapshot_chunk_size: usize,
    /// Refuse to start when any published table has no primary key (STRICT_TABLES).
    strict_tables: bool,

    // Publish retry budget (see Retry section for the rationale)
    publish_max_retries: u32,
    publish_backoff_ms: u64,
    publish_max_backoff_ms: u64,

    // Buffer settings
    event_data_buffer_log2: u6,

    // Encoding format
    encoding_format: encoder.Format,

    /// Create default runtime configuration from compile-time constants
    /// Note: PostgreSQL connection fields are set to defaults that should be overridden from environment
    pub fn defaults() RuntimeConfig {
        return .{
            .http_port = Http.default_port,
            // Not a usable connection on purpose: `parseArgs` requires DATABASE_URL and
            // fails without it, so nothing should ever reach a default here.
            .db_url = "",
            .pg_writer_url = null,
            .slot_name = Postgres.default_slot_name,
            .publication_name = Postgres.default_publication_name,
            // Always replaced in `main` immediately after parseArgs. Safe as a default
            // only because a test asserts `for_tests` equals the repository's own
            // topology.json, so the two cannot drift.
            .topology = Topo.Topology.for_tests,
            .nats_url = Nats.default_url,
            .nats_seed = null,
            .batch_max_events = Batch.max_events,
            .batch_max_wait_ms = Batch.max_age_ms,
            .batch_max_payload_bytes = Batch.max_payload_bytes,
            .batch_ring_buffer_size = Buffers.default_ring_buffer_count,
            .snapshot_chunk_size = Snapshot.chunk_size,
            .strict_tables = false,
            .publish_max_retries = Retry.publish_max_retries,
            .publish_backoff_ms = Retry.publish_backoff_ms,
            .publish_max_backoff_ms = Retry.publish_max_backoff_ms,
            .event_data_buffer_log2 = Buffers.default_event_data_buffer_log2,
            .encoding_format = .msgpack,
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
