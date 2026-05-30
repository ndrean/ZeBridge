//! Centralized configuration for CDC Bridge
//!
//! This module consolidates all configuration constants used throughout the application.
//! Instead of hardcoded values scattered across files, all tunables are defined here.

const std = @import("std");
const encoder = @import("encoder.zig");

/// Compression recipe for zstd snapshots
/// Defined here to avoid circular imports (config.zig ← → zstd.zig)
/// The actual compression parameters (level, strategy) are interpreted by zstd.zig
pub const CompressionRecipe = enum {
    fast,            // level 1, fast strategy
    balanced,        // level 3, dfast strategy
    binary,          // level 6, lazy2 strategy (default for snapshots)
    text,            // level 9, btopt strategy
    structured_data, // level 9, btultra strategy (MessagePack/JSON)
    maximum,         // level 22, btultra2 strategy
};

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
};

/// NATS JetStream configuration
pub const Nats = struct {
    pub const default_port = 4222;
    pub const default_url = "nats://127.0.0.1:4222";

    /// Maximum reconnection attempts (-1 = infinite)
    pub const max_reconnect_attempts = -1;

    /// Wait time between reconnection attempts (milliseconds)
    pub const reconnect_wait_ms = 1000; // 1s
    pub const storage_type = .file;

    /// Async publish flush timeout (milliseconds)
    /// Must be >= reconnect_wait_ms * max attempts
    pub const flush_timeout_ms = 10_000; // 10 seconds
    pub const nats_flush_interval_seconds = 5; // 5 seconds
    pub const status_update_interval_seconds = 1; // 1 second
    pub const status_update_byte_threshold: u64 = 1024 * 1024; // 1MB

    /// JetStream stream names
    pub const stream_cdc = "CDC";
    pub const stream_schema = "SCHEMA";
    pub const stream_init = "INIT";

    /// Default streams to verify on startup
    pub const default_streams = &[_][]const u8{ stream_cdc, stream_init };

    /// Subject prefixes
    pub const subject_cdc_prefix = "cdc";
    pub const subject_schema_prefix = "schema";
    pub const subject_init_prefix = "init";

    /// CDC subject patterns: "cdc.<table>.<operation>"
    pub const cdc_subject_pattern = "cdc.{s}.{s}";

    /// CDC wildcard subject for subscribing to all CDC events
    pub const cdc_subject_wildcard = "cdc.>";

    /// Schema KV bucket name
    pub const schema_kv_bucket = "schemas";

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
    /// Maximum events per batch
    pub const max_events = 500;

    /// Maximum batch age before force flush (milliseconds)
    pub const max_age_ms = 100;

    /// Size of the ring buffer (must be power of 2)
    /// Sized for NATS/PostgreSQL reconnection resilience:
    /// - 65536 slots = ~1092ms buffer at 60K events/s
    /// - Absorbs jitter and provides meaningful buffer during reconnection
    /// - NATS reconnect_wait_ms = 1000ms → covers retry interval
    /// - PostgreSQL reconnect_delay = 5000ms
    /// - Memory cost: ~2MB (negligible for production resilience)
    pub const ring_buffer_size = 65536;
    pub const max_payload_bytes = 256 * 1024; // 256KB
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

/// Snapshot generation configuration
pub const Snapshot = struct {
    /// PostgreSQL NOTIFY channel for snapshot requests (deprecated - using NATS now)
    pub const notify_channel = "snapshot_request";

    /// Rows per snapshot chunk
    pub const chunk_size = 10_000;

    /// Snapshot ID prefix
    pub const id_prefix = "snap";

    /// Maximum concurrent snapshot requests
    pub const max_concurrent_snapshots = 3;

    /// Snapshot polling interval (milliseconds)
    /// How often to check for new snapshot requests via LISTEN/NOTIFY
    pub const poll_interval_ms = 100;

    /// NATS subject prefix for snapshot requests: "snapshot.request.<table>"
    pub const request_subject_prefix = "snapshot.request.";

    /// NATS subject wildcard for subscribing to all snapshot requests
    pub const request_subject_wildcard = "snapshot.request.>";

    /// NATS subject pattern for data chunks: "init.snap.<table>.<snapshot_id>.<chunk>"
    pub const data_subject_pattern = "init.snap.{s}.{s}.{d}";

    /// NATS subject pattern for snapshot start notification: "init.snap.start.<table>"
    pub const start_subject_pattern = "init.snap.start.{s}";

    /// NATS subject pattern for snapshot errors: "init.snap.error.<table>"
    pub const error_subject_pattern = "init.snap.error.{s}";

    /// NATS subject pattern for metadata: "init.snap.meta.<table>"
    pub const meta_subject_pattern = "init.snap.meta.{s}";

    /// NATS KV bucket name for schemas
    pub const kv_bucket_schemas = "schemas";

    /// NATS KV bucket name for zstd dictionaries
    pub const kv_bucket_dictionaries = "dictionaries";

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
pub const Retry = struct {
    /// PostgreSQL reconnection delay (seconds)
    pub const pg_reconnect_delay_seconds = 5;

    /// NATS reconnection is handled by NATS client library
    /// See Nats.reconnect_wait_ms and Nats.max_reconnect_attempts
    /// Exponential backoff base (for future use)
    pub const backoff_base_ms = 1000;

    /// Maximum backoff time (for future use)
    pub const max_backoff_ms = 60_000; // 1 minute
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

    /// Event data buffer size (per-event packed column storage)
    /// Default: 15 → 2^15 = 32KB per event
    /// Configurable via environment variable BASE_BUF (log2 of desired size)
    /// If a row exceeds this size, the bridge will panic to prevent data loss
    /// Example: BASE_BUF=16 → 64KB, BASE_BUF=14 → 16KB
    pub const default_event_data_buffer_log2: u6 = 15; // 2^15 = 32KB

    /// Ring buffer event count (number of pre-allocated event slots)
    /// Default: 65536 events
    /// Configurable via environment variable RING_BUFFER_COUNT
    /// Total memory = event_count × event_buffer_size
    /// Example: 65536 slots × 32KB = 2GB slab
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

    // PostgreSQL replication
    slot_name: []const u8,
    publication_name: []const u8,

    // NATS
    nats_url: []const u8,
    nats_host: []const u8,
    nats_user: ?[]const u8,
    nats_pass: ?[]const u8,

    // Batch settings
    batch_max_events: usize,
    batch_max_wait_ms: i64,
    batch_max_payload_bytes: usize,
    batch_ring_buffer_size: usize,

    // Snapshot settings
    snapshot_chunk_size: usize,

    // Compression settings
    enable_compression: bool,
    recipe: CompressionRecipe,

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
            .slot_name = Postgres.default_slot_name,
            .publication_name = Postgres.default_publication_name,
            .nats_url = Nats.default_url,
            .nats_host = "127.0.0.1",
            .nats_user = null,
            .nats_pass = null,
            .batch_max_events = Batch.max_events,
            .batch_max_wait_ms = Batch.max_age_ms,
            .batch_max_payload_bytes = Batch.max_payload_bytes,
            .batch_ring_buffer_size = Buffers.default_ring_buffer_count,
            .snapshot_chunk_size = Snapshot.chunk_size,
            .enable_compression = false, // disabled by default
            .recipe = .binary, // optimal balance for snapshots (94% compression, 6ms/MB)
            .event_data_buffer_log2 = Buffers.default_event_data_buffer_log2,
            .encoding_format = .msgpack,
        };
    }

    /// Free owned strings in RuntimeConfig
    /// Only frees PostgreSQL connection strings that were allocated from environment variables
    /// Does not free slot_name/publication_name as they reference CLI args or compile-time constants
    pub fn deinit(self: *RuntimeConfig, allocator: std.mem.Allocator) void {
        // Only free if not pointing to compile-time constants
        const default_config = RuntimeConfig.defaults();

        if (self.pg_host.ptr != default_config.pg_host.ptr) {
            allocator.free(self.pg_host);
        }
        if (self.pg_user.ptr != default_config.pg_user.ptr) {
            allocator.free(self.pg_user);
        }
        if (self.pg_password.ptr != default_config.pg_password.ptr) {
            allocator.free(self.pg_password);
        }
        if (self.pg_database.ptr != default_config.pg_database.ptr) {
            allocator.free(self.pg_database);
        }
    }
};

/// Get default log level based on environment
pub fn getDefaultLogLevel() std.log.Level {
    if (std.posix.getenv("LOG_LEVEL")) |level_str| {
        if (std.mem.eql(u8, level_str, "debug")) return .debug;
        if (std.mem.eql(u8, level_str, "info")) return .info;
        if (std.mem.eql(u8, level_str, "warn")) return .warn;
        if (std.mem.eql(u8, level_str, "err")) return .err;
    }

    return .info;
}
