const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;

/// Lock-free metrics shared between bridge and HTTP server using atomics
pub const Metrics = struct {
    start_time: i64, // Unix timestamp in seconds (immutable after init)

    // Message counters (atomics for lock-free increment)
    wal_messages_received: std.atomic.Value(u64),
    cdc_events_published: std.atomic.Value(u64),

    // LSN tracking (atomic for lock-free updates)
    last_ack_lsn: std.atomic.Value(u64),

    // Connection state (atomics)
    is_connected: std.atomic.Value(bool),
    reconnect_count: std.atomic.Value(u32), // PostgreSQL reconnections
    last_reconnect_time: std.atomic.Value(i64),

    // NATS connection state (atomics)
    nats_reconnect_count: std.atomic.Value(u32), // NATS reconnections
    last_nats_reconnect_time: std.atomic.Value(i64),

    // WAL lag metrics (atomics)
    slot_active: std.atomic.Value(bool),
    /// WAL PostgreSQL retains for the slot (from restart_lsn). A disk-pressure number.
    wal_lag_bytes: std.atomic.Value(u64),
    /// WAL the bridge has not confirmed yet (from confirmed_flush_lsn). THE backlog
    /// number: restart_lsn only moves at checkpoints, so `wal_lag_bytes` plateaus at a
    /// few MB on a perfectly healthy bridge and cannot answer "am I keeping up?".
    wal_confirmed_lag_bytes: std.atomic.Value(u64),
    last_wal_check_time: std.atomic.Value(i64),

    // Queue metrics (atomic)
    // Stored as integer percentage (0-100) to avoid f64 atomic operations
    queue_usage_percent: std.atomic.Value(u32),

    // JetStream publish→ack timing (§1.13's measurement prerequisite): the wall time
    // between issuing a publish and its PubAck returning, summed, plus the call count —
    // mean latency is the quotient, rate windows come from scraping deltas. Counters
    // rather than a gauge so no sample is lost between scrapes.
    nats_publish_ack_ns: std.atomic.Value(u64),
    nats_publishes: std.atomic.Value(u64),

    // SCHEMA/KV publishes (DDL schemas, suspensions, drop tombstones). Deliberately a
    // separate counter from `cdc_events_published` (§1.7): that one's arithmetic is
    // trusted to equal ROW events delivered (the README burst method and speed.py both
    // poll it for exact row counts), so folding rare schema traffic into it would skew
    // the measurements it exists for — the undercount was the lesser evil until this
    // counter existed.
    schema_events_published: std.atomic.Value(u64),

    /// Initialize metrics struct with zeroed atomic counters
    pub fn init() Metrics {
        return .{
            .start_time = @as(i64, @intCast(c.time(null))),
            .wal_messages_received = std.atomic.Value(u64).init(0),
            .cdc_events_published = std.atomic.Value(u64).init(0),
            .last_ack_lsn = std.atomic.Value(u64).init(0),
            .is_connected = std.atomic.Value(bool).init(false),
            .reconnect_count = std.atomic.Value(u32).init(0),
            .last_reconnect_time = std.atomic.Value(i64).init(0),
            .nats_reconnect_count = std.atomic.Value(u32).init(0),
            .last_nats_reconnect_time = std.atomic.Value(i64).init(0),
            .slot_active = std.atomic.Value(bool).init(false),
            .wal_lag_bytes = std.atomic.Value(u64).init(0),
            .wal_confirmed_lag_bytes = std.atomic.Value(u64).init(0),
            .last_wal_check_time = std.atomic.Value(i64).init(0),
            .queue_usage_percent = std.atomic.Value(u32).init(0),
            .nats_publish_ack_ns = std.atomic.Value(u64).init(0),
            .nats_publishes = std.atomic.Value(u64).init(0),
            .schema_events_published = std.atomic.Value(u64).init(0),
        };
    }

    /// Lock-free increment of WAL message counter
    pub fn incrementWalMessages(self: *Metrics) void {
        _ = self.wal_messages_received.fetchAdd(1, .monotonic);
    }

    /// Lock-free increment of CDC events counter
    pub fn incrementCdcEvents(self: *Metrics) void {
        _ = self.cdc_events_published.fetchAdd(1, .monotonic);
    }

    /// Lock-free update of LSN position
    pub fn updateLsn(self: *Metrics, lsn: u64) void {
        self.last_ack_lsn.store(lsn, .monotonic);
    }

    /// Lock-free connection state update
    pub fn setConnected(self: *Metrics, connected: bool) void {
        self.is_connected.store(connected, .monotonic);
    }

    /// Lock-free reconnection tracking (PostgreSQL)
    pub fn recordReconnect(self: *Metrics) void {
        _ = self.reconnect_count.fetchAdd(1, .monotonic);
        self.last_reconnect_time.store(@as(i64, @intCast(c.time(null))), .monotonic);
        self.is_connected.store(true, .monotonic);
    }

    /// Lock-free NATS reconnection tracking
    pub fn recordNatsReconnect(self: *Metrics) void {
        _ = self.nats_reconnect_count.fetchAdd(1, .monotonic);
        self.last_nats_reconnect_time.store(@as(i64, @intCast(c.time(null))), .monotonic);
    }

    /// Lock-free WAL lag update
    pub fn updateWalLag(self: *Metrics, slot_active: bool, lag_bytes: u64, confirmed_lag_bytes: u64) void {
        self.slot_active.store(slot_active, .monotonic);
        self.wal_lag_bytes.store(lag_bytes, .monotonic);
        self.wal_confirmed_lag_bytes.store(confirmed_lag_bytes, .monotonic);
        self.last_wal_check_time.store(@as(i64, @intCast(c.time(null))), .monotonic);
    }

    /// Lock-free queue usage update
    /// Takes percentage as f64 (0.0 to 1.0) and stores as integer (0-100)
    pub fn updateQueueUsage(self: *Metrics, usage_ratio: f64) void {
        const percent = @as(u32, @intFromFloat(usage_ratio * 100.0));
        self.queue_usage_percent.store(percent, .monotonic);
    }

    /// Lock-free increment of the SCHEMA/KV events counter
    pub fn incrementSchemaEvents(self: *Metrics) void {
        _ = self.schema_events_published.fetchAdd(1, .monotonic);
    }

    /// Lock-free record of one JetStream publish→ack round trip
    pub fn recordPublishAck(self: *Metrics, elapsed_ns: u64) void {
        _ = self.nats_publish_ack_ns.fetchAdd(elapsed_ns, .monotonic);
        _ = self.nats_publishes.fetchAdd(1, .monotonic);
    }

    /// Get current uptime in seconds (lock-free read)
    pub fn getUptimeSeconds(self: *Metrics) i64 {
        return @as(i64, @intCast(c.time(null))) - self.start_time;
    }

    /// Thread-safe snapshot of metrics for display
    pub const Snapshot = struct {
        uptime_seconds: i64,
        wal_messages_received: u64,
        cdc_events_published: u64,
        last_ack_lsn: u64,
        /// `last_ack_lsn` as `0/<hex>`, for humans. It is the position THIS BRIDGE has
        /// confirmed — never `pg_current_wal_lsn()`, the server's WAL head. It was
        /// called `current_lsn_str` (and exposed on /status as "current_lsn"), which
        /// read exactly like the server's head; that conflation is how a WAL-head LSN
        /// ended up passed to START_REPLICATION and silently skipped every change made
        /// while the bridge was down (NOTES.md finding 4).
        last_ack_lsn_str: []const u8,
        is_connected: bool,
        reconnect_count: u32, // PostgreSQL reconnections
        last_reconnect_time: i64,
        nats_reconnect_count: u32, // NATS reconnections
        last_nats_reconnect_time: i64,
        slot_active: bool,
        wal_lag_bytes: u64,
        wal_confirmed_lag_bytes: u64,
        last_wal_check_time: i64,
        queue_usage_percent: u32, // 0-100
        nats_publish_ack_ns: u64,
        nats_publishes: u64,
        schema_events_published: u64,
    };

    /// Get a lock-free snapshot of all metrics
    /// Note: snapshot may not be perfectly consistent (values read at slightly different times)
    /// but this is acceptable for monitoring/observability use case
    pub fn snapshot(self: *Metrics, allocator: std.mem.Allocator) !Snapshot {
        // Read all atomics with monotonic ordering
        const lsn = self.last_ack_lsn.load(.monotonic);

        // Format LSN as hex string on-demand
        var lsn_buf: [32]u8 = undefined;
        const lsn_str = try std.fmt.bufPrint(&lsn_buf, "0/{x}", .{lsn});
        const lsn_str_owned = try allocator.dupe(u8, lsn_str);

        return .{
            .uptime_seconds = @as(i64, @intCast(c.time(null))) - self.start_time,
            .wal_messages_received = self.wal_messages_received.load(.monotonic),
            .cdc_events_published = self.cdc_events_published.load(.monotonic),
            .last_ack_lsn = lsn,
            .last_ack_lsn_str = lsn_str_owned,
            .is_connected = self.is_connected.load(.monotonic),
            .reconnect_count = self.reconnect_count.load(.monotonic),
            .last_reconnect_time = self.last_reconnect_time.load(.monotonic),
            .nats_reconnect_count = self.nats_reconnect_count.load(.monotonic),
            .last_nats_reconnect_time = self.last_nats_reconnect_time.load(.monotonic),
            .slot_active = self.slot_active.load(.monotonic),
            .wal_lag_bytes = self.wal_lag_bytes.load(.monotonic),
            .wal_confirmed_lag_bytes = self.wal_confirmed_lag_bytes.load(.monotonic),
            .last_wal_check_time = self.last_wal_check_time.load(.monotonic),
            .queue_usage_percent = self.queue_usage_percent.load(.monotonic),
            .nats_publish_ack_ns = self.nats_publish_ack_ns.load(.monotonic),
            .nats_publishes = self.nats_publishes.load(.monotonic),
            .schema_events_published = self.schema_events_published.load(.monotonic),
        };
    }
};
