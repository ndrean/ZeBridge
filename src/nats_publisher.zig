//! NATS/JetStream publishing, on lalinsky/nats.zig.
//!
//! Migrated off the vendored g41797 client, which carried its own `@cImport` of
//! `sys/socket.h` + `netdb.h` — reintroducing the C type-identity problem the project had
//! already eliminated for libpq (see src/c_includes.h), and working around it with a cast
//! its own comment apologised for. Two of its functions had also never compiled: Zig only
//! analyses what is called, and `getHeaders()` returned a value where a pointer was
//! declared.
//!
//! What the library brings that is load-bearing here:
//!
//! - `std.Io` transport, so no C sockets
//! - reconnection with a pending buffer that replays publishes, tested upstream
//! - `PubAck` carrying `seq` and `duplicate`
//! - headers readable on *received* messages, which the verdict path depends on

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
    /// Heap-allocated: `nats.Connection` registers internal pointers to itself, so it
    /// cannot be copied after `init` — and `Publisher` is returned by value from `init`.
    conn: ?*nats.Connection = null,
    js: ?nats.JetStream = null,
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
            .conn = null,
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

        const conn = try self.allocator.create(nats.Connection);
        errdefer self.allocator.destroy(conn);

        conn.* = nats.Connection.init(self.allocator, self.io, .{
            .user = ep.user,
            .password = ep.pass,
            .nkey_seed = ep.seed,
            // The library reconnects on its own and buffers publishes while it does. The
            // hand-rolled reconnect below stays as the outer guard for the case it gives
            // up on, but it is no longer the first line of defence.
            .reconnect = .{
                .allow_reconnect = true,
                .max_reconnect = if (self.config.max_reconnect_attempts < 0)
                    std.math.maxInt(u32)
                else
                    @intCast(self.config.max_reconnect_attempts),
            },
        });
        errdefer conn.deinit();

        const url = try std.fmt.allocPrint(self.allocator, "nats://{s}:{d}", .{ ep.host, ep.port });
        defer self.allocator.free(url);
        try conn.connect(url);

        self.conn = conn;
        self.js = nats.JetStream.init(conn, .{});

        log.info("🟢 Connected to NATS at {s}:{d}", .{ ep.host, ep.port });
        log.info("✅ JetStream context acquired", .{});
        self.is_connected.store(true, .seq_cst);
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

            // Tear the old one down first. `Connection.deinit` stops the reader, flusher
            // and manager fibers; leaking it would leave them running against a socket
            // nobody reads.
            self.teardown();

            // The library reconnects internally, so reaching here means it gave up — a
            // fresh connection is the only thing left to try.
            self.connect() catch |err| {
                log.warn("Reconnect attempt {d} failed: {s}", .{ attempt + 1, @errorName(err) });
                continue;
            };
            self.is_connected.store(true, .seq_cst);
            const count = self.reconnect_count.fetchAdd(1, .monotonic) + 1;

            // Update metrics if available
            if (self.metrics) |metrics| {
                metrics.recordNatsReconnect();
            }

            log.info("🟢 NATS reconnected to {s}:{d} (reconnect #{d})", .{
                self.config.endpoint.host,
                self.config.endpoint.port,
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
        const was_connected = self.conn != null;
        self.is_connected.store(false, .seq_cst);
        self.teardown();
        if (was_connected) log.info("🥁 Disconnected from NATS", .{});
    }

    /// Close the connection and free it. Safe to call when there is none.
    ///
    /// Separate from `deinit` because `reconnect` needs exactly this and nothing else —
    /// no logging, no "was_connected" bookkeeping.
    fn teardown(self: *Publisher) void {
        self.js = null; // borrows the connection; drop it first
        if (self.conn) |conn| {
            conn.deinit();
            self.allocator.destroy(conn);
            self.conn = null;
        }
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
    /// Publish to JetStream with a `Nats-Msg-Id`, and wait for the PubAck.
    ///
    /// The msg_id is passed as a value rather than a prepared header block: every caller
    /// wanted exactly one header, and each was allocating a 256-byte pool structure to
    /// carry it. `PublishOptions.msg_id` does the same thing with no allocation.
    ///
    /// ⚠️ Synchronous by design. The PubAck is what proves the message is *stored*, and
    /// the CDC path advances its LSN watermark on that proof — a fire-and-forget publish
    /// would let the bridge report progress past data the stream never accepted.
    ///
    /// `ack.duplicate` is not an error: it means JetStream recognised this msg_id inside
    /// the stream's dedup window and collapsed a retry into the original write, which is
    /// exactly what an idempotent republish should do.
    pub fn publish(
        self: *Publisher,
        subject: []const u8,
        msg_id: ?[]const u8,
        data: []const u8,
    ) !void {
        if (self.js) |*js| {
            var res = js.publish(subject, data, .{ .msg_id = msg_id }) catch |err| {
                log.warn("NATS publish failed: {s} - attempting reconnection", .{@errorName(err)});
                self.is_connected.store(false, .seq_cst);

                if (!self.reconnect()) return error.ReconnectionFailed;

                if (self.js) |*js_retry| {
                    var retry = try js_retry.publish(subject, data, .{ .msg_id = msg_id });
                    retry.deinit();
                    return;
                }
                return error.NotConnected;
            };
            res.deinit();
            return;
        }

        if (!self.reconnect()) return error.NotConnected;

        if (self.js) |*js| {
            var res = try js.publish(subject, data, .{ .msg_id = msg_id });
            res.deinit();
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
        try self.publish(subject, null, data);
    }

    /// Get stream information (for HTTP endpoints)
    ///
    /// Calls JetStream INFO API and returns JSON response.
    /// Returns detailed stream configuration and state information.
    /// Does a stream exist? Used by the boot preflight to fail loudly when a declared
    /// tenant has no `CDC_<TENANT>` stream, rather than FATAL'ing mid-publish under load.
    ///
    /// ⚠️ Replaces a `getStreamInfo` that returned a hand-built JSON blob and referenced
    /// `nats.JS.StreamInfoRequest`, a type that does not exist — it was dead code Zig never
    /// analysed because nothing called it. The library's own `js.getStreamInfo` (used in
    /// snapshot_listener.zig and below) is the real API; this wraps it for a bool.
    pub fn streamExists(self: *Publisher, stream_name: []const u8) bool {
        if (self.js) |*js| {
            var res = js.getStreamInfo(stream_name) catch return false;
            res.deinit();
            return true;
        }
        return false;
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
/// `max_payload` as this connection's server advertised it, or null if unknown.
///
/// Load-bearing rather than informational: `BASE_BUF` must stay under it, and the snapshot
/// chunker sizes every COPY against it. A message over the limit is not rejected with an
/// error — the server closes the connection, and every retry re-sends the same bytes.
pub fn serverMaxPayload(js: *nats.JetStream) ?u64 {
    const mp = js.nc.server_info.max_payload;
    return if (mp > 0) @intCast(mp) else null;
}

/// Verify a stream exists, by asking JetStream.
///
/// ⚠️ Was a no-op — the vendored client exposed no INFO API, so "verified" meant nothing
/// and a missing stream surfaced later as publishes that silently went nowhere. It now
/// actually checks, which is the difference between the startup log being a claim and
/// being a fact.
pub fn ensureStream(js: *nats.JetStream, allocator: std.mem.Allocator, stream_name: []const u8) !void {
    _ = allocator;
    var res = js.getStreamInfo(stream_name) catch |err| {
        log.err("🔴 Stream '{s}' is not reachable: {s}", .{ stream_name, @errorName(err) });
        return error.StreamNotFound;
    };
    defer res.deinit();
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
