//! Centralized configuration for CDC Bridge
//!
//! This module consolidates all configuration constants used throughout the application.
//! Instead of hardcoded values scattered across files, all tunables are defined here.

const std = @import("std");
const encoder = @import("encoder.zig");
const topology = @import("topology");

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
    pub const default_url = "nats://127.0.0.1:4222";

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

    /// JetStream stream names. Every one of these is a name `nats-init` creates from
    /// the same topology.json, so the two ends cannot disagree.
    pub const stream_cdc = topology.stream_cdc;
    pub const stream_init = topology.stream_init;
    pub const stream_mutations = topology.stream_mutations;
    pub const stream_requests = topology.stream_requests;

    /// Subject prefixes
    pub const subject_cdc_prefix = topology.subject_cdc_prefix;
    pub const subject_init_prefix = topology.subject_init_prefix;
    pub const subject_mutations_prefix = topology.subject_mutations_prefix;

    /// Every mutation subject the ingress consumer filters on: "mutation.>"
    pub const mutations_subject_wildcard = subject_mutations_prefix ++ ".>";

    /// `mutation.<principal>.<table>.<operation>` — the ingress subject grammar.
    ///
    /// The principal is a **subject token, not a payload field**, because NATS
    /// authorizes subjects and not payloads: a client granted
    /// `publish: ["mutation.alice.>"]` physically cannot write as anyone else, and the
    /// bridge reading the principal off the subject is reading a claim the broker has
    /// already vouched for. Principal first so a per-user grant is one wildcard rule.
    ///
    /// ⚠️ The principal must be a legal NATS token — no `.`, space, `*` or `>` — so an
    /// email address cannot be one. Use an opaque id.
    pub const mutation_subject_pattern = topology.mutation_pattern;

    /// Where a mutation goes when it can never succeed: `mutation_error.<table>`.
    ///
    /// ⚠️ Deliberately **outside** the `mutation.>` space. A dead-letter subject under
    /// the same prefix would be matched by the ingress consumer's own filter, so the
    /// bridge would consume its own failures and republish them forever — a poison pill
    /// with a feedback loop.
    pub const mutation_error_prefix = topology.mutation_error_prefix;
    pub const mutation_error_pattern = topology.mutation_error_pattern;

    /// Redelivery budget for a mutation. Bounded so that a message the bridge
    /// misclassifies as retryable still stops eventually, instead of pinning the worker
    /// at one attempt per second for the life of the process (observed: 24 redeliveries
    /// in 15 s before this existed).
    pub const mutation_max_deliver: i32 = 5;

    /// Token positions in the mutation subject, after splitting on '.'.
    pub const mutation_token_principal = 1;
    pub const mutation_token_table = 2;
    pub const mutation_token_operation = 3;
    pub const mutation_token_count = 4;

    /// Subject a client publishes a schema request to, and the bridge subscribes to.
    pub const schema_request_subject = topology.schema_request;

    /// Where the bridge publishes a table's schema: "init.schema.<table>". Under the
    /// init prefix so the INIT stream captures it — a subject outside every stream is
    /// published, never acked, and fails after the full retry budget.
    pub const schema_subject_pattern = topology.schema_request ++ ".{s}";

    /// JetStream exposes a KV bucket as the subject space $KV.<bucket>.<key>, so a
    /// bucket is written by publishing to that subject. Single-sourced from topology:
    /// these used to be hardcoded "$KV.schemas.{s}" literals in four places, which
    /// meant renaming the bucket in topology.json changed the server and the config
    /// constants but NOT what the bridge actually published.
    pub const kv_subject_prefix = "$KV.";
    pub const kv_schemas_subject_pattern = kv_subject_prefix ++ topology.kv_schemas ++ ".{s}";

    /// Snapshot descriptor bucket, same reasoning as above. This one was a live
    /// instance of the bug rather than a hypothetical: `snapshot_listener` published to
    /// a hardcoded "$KV.snapshots.{s}" while `nats-init` never created the bucket at
    /// all (it read only `.kv.schemas` from topology.json), so every snapshot ended in
    /// six failed publish retries against a bucket that did not exist.
    pub const snapshot_kv_bucket = topology.kv_snapshots;
    pub const kv_snapshots_subject_pattern = kv_subject_prefix ++ topology.kv_snapshots ++ ".{s}";

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

    /// HTTP server bind address
    pub const bind_address = "0.0.0.0";

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

    /// Rows per snapshot chunk
    pub const chunk_size = 10_000;

    // Snapshot freshness used to be a bridge-local cache (`SnapshotCache`), which
    // could not coordinate across bridge instances and died on restart. It is now the
    // REQUESTS stream's own policy — max-msgs-per-subject=1, discard=new, max-age —
    // so the window is enforced by the broker for every client, and SNAP_RET_SECONDS
    // configures nats-init rather than the bridge.

    /// Maximum concurrent snapshot requests
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

    /// NATS subject prefix for snapshot requests: "snapshot.request.<table>"
    pub const request_subject_prefix = topology.snapshot_request ++ ".";

    /// NATS subject wildcard for subscribing to all snapshot requests
    pub const request_subject_wildcard = topology.snapshot_request ++ ".>";

    /// NATS subject pattern for data chunks: "init.snap.{table}.{snapshot_id}.{chunk}"
    pub const data_subject_pattern = topology.snapshot_data_pattern;

    /// NATS subject pattern for snapshot start notification: "init.snap.start.{table}"
    pub const start_subject_pattern = topology.snapshot_start_pattern;

    /// NATS subject pattern for snapshot errors: "init.snap.error.{table}"
    pub const error_subject_pattern = topology.snapshot_error_pattern;

    /// NATS subject pattern for metadata: "init.snap.meta.{table}"
    pub const meta_subject_pattern = topology.snapshot_meta_pattern;

    /// Column order for a snapshot's chunks.
    /// Was hardcoded as "snapshot.schema.{table}.{id}" — a subject NO stream captures,
    /// so JetStream never acked it, the publish timed out through all its retries, and
    /// the whole snapshot failed. It must live under init.> for the INIT stream to
    /// store it.
    pub const schema_subject_pattern = topology.snapshot_schema_pattern;

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

    // PostgreSQL connection
    pg_host: []const u8,
    pg_port: u16,
    pg_user: []const u8,
    pg_password: []const u8,
    pg_database: []const u8,
    /// libpq sslmode. Set explicitly rather than letting libpq apply its default of
    /// `prefer`, which negotiates TLS when offered, falls back to plaintext when not,
    /// and validates the server in neither case — so you cannot tell what you got.
    /// Use `disable` when Postgres and the bridge share a host, `verify-full` when the
    /// connection crosses one. `require` encrypts but does not authenticate the server.
    pg_sslmode: []const u8,
    db_url: ?[]const u8, // Optional unified connection URI

    /// Credentials for the **ingress** connection, kept separate from the read path on
    /// purpose. `pg_user` keeps SELECT + REPLICATION and stays physically unable to
    /// write, whatever the write path grows into; this role has no table privileges
    /// until a DBA opens one (`zebridge_grant_edge_writes`).
    ///
    /// Null means "no writer configured": the mutation listener does not start, rather
    /// than quietly falling back to the read role. Falling back would mean the ingress
    /// path silently runs with replication rights — the exact privilege the split
    /// exists to avoid.
    pg_writer_user: ?[]const u8,
    pg_writer_password: ?[]const u8,
    /// Unified ingress URI, the symmetric counterpart of `db_url`. Preferred when set:
    /// `connInfo` passes a URL through natively, so it avoids rebuilding the keyword
    /// form from six fields.
    pg_writer_url: ?[]const u8,

    // PostgreSQL replication
    slot_name: []const u8,
    publication_name: []const u8,

    // NATS
    nats_url: ?[]const u8, // Optional unified NATS URI
    nats_host: []const u8,
    nats_seed: ?[]const u8, // Optional NKey Seed

    // Batch settings
    batch_max_events: usize,
    batch_max_wait_ms: i64,
    batch_max_payload_bytes: usize,
    batch_ring_buffer_size: usize,

    // Snapshot settings
    snapshot_chunk_size: usize,
    // [Deprecated] Handled by JetStream max_age
    // snapshot_retention_seconds: i64,
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
            .pg_host = "127.0.0.1",
            .pg_port = 5432,
            .pg_user = "postgres",
            .pg_password = "postgres",
            .pg_database = "postgres",
            .pg_sslmode = "disable",
            .db_url = null,
            .pg_writer_user = null,
            .pg_writer_password = null,
            .pg_writer_url = null,
            .slot_name = Postgres.default_slot_name,
            .publication_name = Postgres.default_publication_name,
            .nats_url = null,
            .nats_host = "127.0.0.1",
            .nats_seed = null,
            .batch_max_events = Batch.max_events,
            .batch_max_wait_ms = Batch.max_age_ms,
            .batch_max_payload_bytes = Batch.max_payload_bytes,
            .batch_ring_buffer_size = Buffers.default_ring_buffer_count,
            .snapshot_chunk_size = Snapshot.chunk_size,
            // .snapshot_retention_seconds = Snapshot.retention_seconds,
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
