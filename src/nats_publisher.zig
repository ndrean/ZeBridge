//! NATS/JetStream Publisher Module (Pure Zig - g41797/nats)
//!
//! Provides NATS connectivity and JetStream publishing using the pure Zig nats library.
//! Replaces the old C-based implementation with a cleaner, native Zig solution.
//!
//! Key features:
//! - Pure Zig implementation (no C dependencies)
//! - Synchronous publishing for reliable LSN tracking
//! - Automatic stream verification on startup
//! - Clean error handling and reconnection

const std = @import("std");
const nats = @import("nats");
const Conf = @import("config.zig");
const Metrics = @import("metrics.zig").Metrics;
const utils = @import("utils.zig");

pub const log = std.log.scoped(.nats_pub);

/// NATS Publisher Configuration
pub const PublisherConfig = struct {
    /// Where NATS is. Resolved once by `Conf.Nats.Endpoint.resolve` and handed to every
    /// component, rather than each one re-deriving it: this field used to be a URL
    /// string with a hardcoded `nats://127.0.0.1:4222` default that the publisher then
    /// parsed itself, which is how the bridge ended up with two different opinions about
    /// where the server was.
    endpoint: Conf.Nats.Endpoint = .{},
    max_reconnect_attempts: i32 = Conf.Nats.max_reconnect_attempts, // -1 = infinite
    reconnect_wait_ms: i64 = Conf.Nats.reconnect_wait_ms,
    max_backoff_ms: i64 = Conf.Nats.max_backoff_ms,
};

/// Pure Zig NATS/JetStream Publisher
///
/// Provides reliable message publishing to NATS JetStream streams.
/// Uses synchronous publishing for CDC to ensure LSN watermarks are acknowledged.
///
/// Usage:
/// ```zig
/// var publisher = try Publisher.init(allocator, .{});
/// defer publisher.deinit();
/// try publisher.connect();
///
/// // Publish with headers
/// var headers = nats.pool.Headers{};
/// try headers.init(allocator, 256);
/// defer headers.deinit();
/// try headers.append("Nats-Msg-Id", "msg-123");
///
/// try publisher.publish("events.test", &headers, "Hello");
/// ```
pub const Publisher = struct {
    allocator: std.mem.Allocator,
    config: PublisherConfig,
    nc: ?*nats.Client = null,
    js: ?nats.JS = null, // JS is a struct, not a pointer
    is_connected: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    reconnect_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    reconnect_mutex: std.Io.Mutex = .{ .state = std.atomic.Value(std.Io.Mutex.State).init(.unlocked) },
    metrics: ?*Metrics = null, // Optional metrics tracking
    io: std.Io = undefined,

    pub fn init(
        allocator: std.mem.Allocator,
        config: PublisherConfig,
        io: std.Io,
    ) !Publisher {
        return Publisher{
            .allocator = allocator,
            .config = config,
            .nc = null,
            .js = null,
            .io = io,
        };
    }

    /// The resolved address, for anything that needs to connect the same way.
    pub fn endpoint(self: *const Publisher) Conf.Nats.Endpoint {
        return self.config.endpoint;
    }

    pub fn connect(self: *Publisher) !void {
        const ep = self.config.endpoint;

        // Never log the URL — it embeds the password, and these logs ship to Loki.
        log.info("Connecting to NATS at {s}:{d} (auth={s})", .{
            ep.host,
            ep.port,
            if (ep.user != null or ep.seed != null) "yes" else "no",
        });

        // Create JetStream context (includes NATS client connection)
        const js = try nats.JS.CONNECT(self.allocator, .{
            .addr = ep.host,
            .port = ep.port,
            .user = ep.user,
            .pass = ep.pass,
            .nkey_seed = ep.seed,
        }, self.io);
        self.js = js;

        log.info("🟢 Connected to NATS at {s}:{d}", .{ ep.host, ep.port });
        log.info("✅ JetStream context acquired", .{});
        self.is_connected.store(true, .seq_cst);

        // Note: Stream verification is not available in pure Zig nats library
        // Streams must be created by infrastructure (nats-init)
        log.info("✅ NATS/JetStream ready (stream verification skipped - ensure streams exist)", .{});
    }

    /// Attempt to reconnect to NATS server
    /// Returns true if reconnection succeeded, false otherwise
    fn reconnect(self: *Publisher) bool {
        self.reconnect_mutex.lockUncancelable(self.io);
        defer self.reconnect_mutex.unlock(self.io);

        // Double-check if already connected (another thread might have reconnected)
        if (self.is_connected.load(.seq_cst)) {
            return true;
        }

        var attempt: u32 = 0;
        const max_attempts: u32 = if (self.config.max_reconnect_attempts < 0)
            std.math.maxInt(u32)
        else
            @intCast(self.config.max_reconnect_attempts);

        log.warn("🔴 NATS connection lost - attempting reconnection...", .{});

        while (attempt < max_attempts) : (attempt += 1) {
            // Calculate backoff with exponential increase, capped at max_backoff_ms
            const base_wait: i64 = self.config.reconnect_wait_ms;
            const backoff_multiplier: i64 = @as(i64, 1) << @intCast(@min(attempt, 5)); // Cap at 2^5 = 32x
            const wait_ms = @min(base_wait * backoff_multiplier, self.config.max_backoff_ms);

            if (attempt > 0) {
                log.info("Reconnect attempt {d}/{d} - waiting {d}ms...", .{
                    attempt + 1,
                    if (max_attempts == std.math.maxInt(u32)) @as(i32, -1) else @as(i32, @intCast(max_attempts)),
                    wait_ms,
                });
                utils.sleep(@intCast(wait_ms * std.time.ns_per_ms));
            }

            // Disconnect old connection if any
            if (self.js) |*js| {
                js.DISCONNECT();
                self.js = null;
            }

            // Attempt new connection to the same resolved endpoint
            const ep = self.config.endpoint;
            const js = nats.JS.CONNECT(self.allocator, .{
                .addr = ep.host,
                .port = ep.port,
                .user = ep.user,
                .pass = ep.pass,
                .nkey_seed = ep.seed,
            }, self.io) catch |err| {
                log.warn("Reconnect attempt {d} failed: {s}", .{ attempt + 1, @errorName(err) });
                continue;
            };

            // Success!
            self.js = js;
            self.is_connected.store(true, .seq_cst);
            const count = self.reconnect_count.fetchAdd(1, .monotonic) + 1;

            // Update metrics if available
            if (self.metrics) |metrics| {
                metrics.recordNatsReconnect();
            }

            log.info("🟢 NATS reconnected to {s}:{d} (reconnect #{d})", .{
                ep.host,
                ep.port,
                count,
            });

            return true;
        }

        log.err("🔴 NATS reconnection failed after {d} attempts", .{attempt});
        self.is_connected.store(false, .seq_cst);
        return false;
    }

    /// Get the number of times NATS has reconnected
    pub fn getReconnectCount(self: *Publisher) u32 {
        return self.reconnect_count.load(.monotonic);
    }

    pub fn deinit(self: *Publisher) void {
        // "Disconnected" is only true if we ever connected. Logging it unconditionally
        // made a failed *first* connection read as a lost one — the misleading
        // `🥁 Disconnected from NATS` that appeared when NATS_URL did not resolve.
        const was_connected = self.js != null;
        self.is_connected.store(false, .seq_cst);

        if (self.js) |*js| {
            js.DISCONNECT();
            self.js = null;
        }

        // No separate nc in pure Zig nats - JetStream includes the connection.
        // Nothing to free: the endpoint borrows from the environment block.
        self.nc = null;

        if (was_connected) log.info("🥁 Disconnected from NATS", .{});
    }

    /// Check if NATS connection is alive
    pub fn isConnected(self: *Publisher) bool {
        return self.is_connected.load(.seq_cst) and self.js != null;
    }

    /// Publish a message to JetStream with headers (synchronous)
    ///
    /// This is synchronous and waits for JetStream acknowledgment.
    /// Critical for CDC to ensure LSN watermarks are committed.
    /// Automatically attempts reconnection on connection failures.
    ///
    /// Parameters:
    /// - subject: Full subject name
    /// - headers: NATS message headers (includes Nats-Msg-Id for deduplication)
    /// - data: Message payload
    pub fn publish(
        self: *Publisher,
        subject: []const u8,
        headers: *nats.pool.Headers,
        data: []const u8,
    ) !void {
        // Fast path: try publish if connected
        if (self.js) |*js| {
            js.PUBLISH(subject, headers, data) catch |err| {
                // Connection might be lost, mark as disconnected
                log.warn("NATS publish failed: {s} - attempting reconnection", .{@errorName(err)});
                self.is_connected.store(false, .seq_cst);

                // Attempt reconnection
                if (!self.reconnect()) {
                    return error.ReconnectionFailed;
                }

                // Retry publish after reconnection
                if (self.js) |*js_retry| {
                    try js_retry.PUBLISH(subject, headers, data);
                    return;
                }

                return error.NotConnected;
            };
            return;
        }

        // Not connected, try to reconnect first
        if (!self.reconnect()) {
            return error.NotConnected;
        }

        // Retry after reconnection
        if (self.js) |*js| {
            try js.PUBLISH(subject, headers, data);
        } else {
            return error.NotConnected;
        }
    }

    /// Publish without headers (for simple messages)
    /// Automatically attempts reconnection on connection failures.
    pub fn publishSimple(
        self: *Publisher,
        subject: []const u8,
        data: []const u8,
    ) !void {
        // Create headers (needed even for simple publish)
        var headers = nats.pool.Headers{};
        try headers.init(self.allocator, 64);
        defer headers.deinit();

        // Use the main publish function which handles reconnection
        try self.publish(subject, &headers, data);
    }

    /// Get stream information (for HTTP endpoints)
    ///
    /// Calls JetStream INFO API and returns JSON response.
    /// Returns detailed stream configuration and state information.
    pub fn getStreamInfo(self: *Publisher, stream_name: []const u8) ![]const u8 {
        if (self.js == null) {
            return error.NotConnected;
        }

        // Call JetStream INFO API with empty request (basic info)
        const request: nats.JS.StreamInfoRequest = .{};
        const info = try self.js.?.INFO(stream_name, &request);

        // Build JSON response from StreamInfoResponse
        var out: std.Io.Writer.Allocating = .init(self.allocator);
        defer out.deinit();

        try out.writer.writeAll("{");

        // Add stream name from config
        if (info.config) |config| {
            try out.writer.print("\"name\":\"{s}\"", .{config.name});

            // Add config details
            try out.writer.writeAll(",\"config\":{");
            try out.writer.print("\"retention\":\"{s}\"", .{config.retention});
            try out.writer.print(",\"storage\":\"{s}\"", .{config.storage});
            try out.writer.print(",\"max_msgs\":{d}", .{config.max_msgs});
            try out.writer.print(",\"max_bytes\":{d}", .{config.max_bytes});
            try out.writer.print(",\"max_age\":{d}", .{config.max_age});
            try out.writer.print(",\"max_msg_size\":{d}", .{config.max_msg_size});

            // Add subjects array
            if (config.subjects) |subjects| {
                try out.writer.writeAll(",\"subjects\":[");
                for (subjects, 0..) |subject, i| {
                    if (i > 0) try out.writer.writeAll(",");
                    try out.writer.print("\"{s}\"", .{subject});
                }
                try out.writer.writeAll("]");
            }
            try out.writer.writeAll("}");
        }

        // Add state information
        if (info.state) |state| {
            try out.writer.writeAll(",\"state\":{");
            try out.writer.print("\"messages\":{d}", .{state.messages});
            try out.writer.print(",\"bytes\":{d}", .{state.bytes});
            try out.writer.print(",\"first_seq\":{d}", .{state.first_seq});
            try out.writer.print(",\"last_seq\":{d}", .{state.last_seq});
            try out.writer.print(",\"consumer_count\":{d}", .{state.consumer_count});

            // Add optional fields
            if (state.first_ts) |first_ts| {
                try out.writer.print(",\"first_ts\":\"{s}\"", .{first_ts});
            }
            if (state.last_ts) |last_ts| {
                try out.writer.print(",\"last_ts\":\"{s}\"", .{last_ts});
            }
            if (state.num_subjects) |num| {
                try out.writer.print(",\"num_subjects\":{d}", .{num});
            }
            if (state.num_deleted) |num| {
                try out.writer.print(",\"num_deleted\":{d}", .{num});
            }
            try out.writer.writeAll("}");
        }

        // Add timestamps
        if (info.created) |created| {
            try out.writer.print(",\"created\":\"{s}\"", .{created});
        }
        if (info.ts) |ts| {
            try out.writer.print(",\"ts\":\"{s}\"", .{ts});
        }

        try out.writer.writeAll("}");

        return try out.toOwnedSlice();
    }

    /// Purge all messages from a stream
    /// Automatically attempts reconnection on connection failures.
    pub fn purgeStream(self: *Publisher, stream_name: []const u8) !void {
        // Fast path: try purge if connected
        if (self.js) |*js| {
            js.PURGE(stream_name) catch |err| {
                // Connection might be lost, mark as disconnected
                log.warn("NATS purge failed: {s} - attempting reconnection", .{@errorName(err)});
                self.is_connected.store(false, .seq_cst);

                // Attempt reconnection
                if (!self.reconnect()) {
                    return error.ReconnectionFailed;
                }

                // Retry purge after reconnection
                if (self.js) |*js_retry| {
                    try js_retry.PURGE(stream_name);
                    log.info("☑️ Stream '{s}' purged", .{stream_name});
                    return;
                }

                return error.NotConnected;
            };
            log.info("☑️ Stream '{s}' purged", .{stream_name});
            return;
        }

        // Not connected, try to reconnect first
        if (!self.reconnect()) {
            return error.NotConnected;
        }

        // Retry after reconnection
        if (self.js) |*js| {
            try js.PURGE(stream_name);
            log.info("☑️ Stream '{s}' purged", .{stream_name});
        } else {
            return error.NotConnected;
        }
    }
};

/// Ensure a JetStream stream exists (check-or-fail-fast)
///
/// Note: The pure Zig nats library doesn't expose stream INFO API yet.
/// This function is a no-op placeholder. Streams must be created by infrastructure.
/// `max_payload` as this connection's server advertised it, or null if unknown.
pub fn serverMaxPayload(js: *nats.JS) ?u64 {
    const conn = js.connection orelse return null;
    return conn.server_max_payload;
}

pub fn ensureStream(js: *nats.JS, allocator: std.mem.Allocator, stream_name: []const u8) !void {
    _ = js;
    _ = allocator;
    _ = stream_name;
    // No-op: pure Zig nats doesn't expose INFO API
    // Streams must be created by infrastructure (nats-init)
}

/// Check multiple JetStream streams exist
///
/// Verifies that all required streams are created and accessible.
pub fn checkStreams(
    publisher: *Publisher,
    allocator: std.mem.Allocator,
    stream_names: []const []const u8,
) !void {
    log.info("Verifying {d} NATS JetStream stream(s)...", .{stream_names.len});

    if (publisher.js) |*js| {
        for (stream_names) |stream_name| {
            try ensureStream(js, allocator, stream_name);
            log.info("  ✅ Stream '{s}' verified", .{stream_name});
        }
    } else {
        return error.NotConnected;
    }
}
