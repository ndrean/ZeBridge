const std = @import("std");
const metrics_mod = @import("metrics.zig");
const nats_publisher = @import("nats_publisher.zig");
const refused_tables = @import("refused_tables.zig");
const utils = @import("utils.zig");
const Config = @import("config.zig");

// std.net was removed in Zig 0.16 "Juicy Main". Use C POSIX sockets directly.
// No @cImport here any more.
//
// This file used to reach for `sys/socket.h` + `poll.h` and hand-roll socket/bind/accept/
// read. It bought exactly one thing — a *cancellable* accept, via a 100ms `poll` that let
// the loop check `should_stop` — and paid for it with everything else: `INADDR_ANY` as the
// accidental default (a zeroed `sockaddr_in` leaves `s_addr == 0`), a `sin_addr` byte
// order to get wrong by hand, and request parsing over a fixed 2048-byte buffer.
//
// `std.Io` gives the same cancellation for free: `AcceptError.SocketNotListening` is
// documented as the shutdown mechanism — closing the listener wakes a blocked `accept`.

pub const log = std.log.scoped(.http_server);

// `parseIp4` and its two tests lived here. `std.Io.net.IpAddress.parse` does it, and does
// not offer the byte-order mistake the hand-rolled version made: `readInt(u32, .big)`
// returns the numeric value, which lays down reversed on a little-endian host, so
// `127.0.0.1` bound as `1.0.0.127` and the server failed with `BindFailed`. The unit tests
// missed it because they asserted against the same wrong expression.

pub const Server = struct {
    allocator: std.mem.Allocator,
    port: u16,
    bind: []const u8 = "127.0.0.1",
    io: std.Io,
    /// The live listener, so `stop()` can close it and wake a blocked `accept`. Valid only
    /// while `run` is on the stack.
    server: ?*std.Io.net.Server = null,
    /// Connections currently being served. Bounded by `Config.Http.max_connections`.
    open_conns: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    should_stop: *std.atomic.Value(bool),
    metrics: ?*metrics_mod.Metrics,
    nats_publisher: ?*nats_publisher.Publisher,
    /// Set after construction, like nats_publisher. Only its atomic summaries are read
    /// here — the map belongs to the replication thread (see refused_tables.zig).
    refused: ?*const refused_tables.Registry = null,
    thread: ?std.Thread = null,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        /// Address to bind. Defaults to loopback (see `Config.Http.default_bind`):
        /// telemetry discloses table names, lag and throughput, and every endpoint is
        /// unauthenticated, so reaching it should require being on the host — or a reverse
        /// proxy the operator put there deliberately.
        bind: []const u8,
        port: u16,
        should_stop: *std.atomic.Value(bool),
        metrics: ?*metrics_mod.Metrics,
        nats_pub: ?*nats_publisher.Publisher,
    ) !Server {
        return Server{
            .allocator = allocator,
            .io = io,
            .bind = bind,
            .port = port,
            .should_stop = should_stop,
            .metrics = metrics,
            .nats_publisher = nats_pub,
            .thread = null,
        };
    }

    /// Start the HTTP server thread
    pub fn start(self: *Server) !void {
        if (self.thread != null) return error.AlreadyStarted;
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    /// Wake a blocked `accept`, so `join` can return.
    ///
    /// ⚠️ Setting `should_stop` is not enough on its own. The loop checks it between
    /// accepts, and `accept` blocks indefinitely — the old C implementation only got away
    /// with a flag because it wrapped every accept in a 100ms `poll`. Closing the listener
    /// is the documented replacement: std says `SocketNotListening` is returned when
    /// "shutdown was called (possibly while this call was blocking). This allows shutdown
    /// to be used as a concurrent cancellation mechanism."
    ///
    /// Safe to call when `run` is not on the stack, and safe to call twice.
    pub fn stop(self: *Server) void {
        self.should_stop.store(true, .seq_cst);
        if (self.server) |srv| {
            self.server = null;
            srv.deinit(self.io);
        }
    }

    /// Join the HTTP server thread (waits for completion).
    ///
    /// Calls `stop` first: a `join` that does not wake the accept loop is a hang, and the
    /// one place that hurts most is `defer http_srv.join()` on the shutdown path.
    pub fn join(self: *Server) void {
        self.stop();
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    /// Deinit - cleanup resources (call after join)
    pub fn deinit(self: *Server) void {
        _ = self;
    }

    /// Run the HTTP server (in a separate thread)
    pub fn run(self: *Server) !void {
        const addr = std.Io.net.IpAddress.parse(self.bind, self.port) catch {
            log.err("🔴 BRIDGE_BIND '{s}' is not an IP address (e.g. 127.0.0.1 or 0.0.0.0)", .{self.bind});
            return error.InvalidBindAddress;
        };

        var server = addr.listen(self.io, .{ .reuse_address = true }) catch |err| {
            log.err("🔴 Could not listen on {s}:{d}: {s}", .{ self.bind, self.port, @errorName(err) });
            return error.BindFailed;
        };
        self.server = &server;
        // `stop()` may have closed and cleared it already; only clean up if it did not.
        defer if (self.server != null) {
            self.server = null;
            server.deinit(self.io);
        };

        log.info("✅ HTTP server listening on http://{s}:{d}", .{ self.bind, self.port });
        if (std.mem.eql(u8, self.bind, "0.0.0.0")) {
            log.warn("⚠️  Telemetry is bound to every interface and every endpoint is unauthenticated — table names, lag and throughput are readable by anyone who can reach this port.", .{});
        }
        log.info("ℹ️ Available endpoints:", .{});
        log.info("  GET  /health         - Health check", .{});
        log.info("  GET  /status         - Bridge status (JSON)", .{});
        log.info("  GET  /metrics        - Prometheus metrics", .{});

        // No poll timer. `accept` blocks until a client arrives *or* the listener is
        // closed — std documents `SocketNotListening` as exactly that: "shutdown was
        // called (possibly while this call was blocking). This allows shutdown to be used
        // as a concurrent cancellation mechanism." `stop()` closes it.
        // Tasks serving connections. Awaited on the way out, so a response in flight is
        // not cut off by shutdown.
        var conns: std.Io.Group = .init;
        defer conns.cancel(self.io);

        while (!self.should_stop.load(.seq_cst)) {
            const stream = server.accept(self.io) catch |err| switch (err) {
                error.SocketNotListening, error.Canceled => break,
                // Transient: a peer that hung up between the SYN and the accept, or a
                // momentary fd shortage. Losing the loop over either would take telemetry
                // down for the life of the process.
                error.ConnectionAborted,
                error.WouldBlock,
                error.ProcessFdQuotaExceeded,
                error.SystemFdQuotaExceeded,
                error.SystemResources,
                => continue,
                else => {
                    log.err("🔴 accept failed: {s}", .{@errorName(err)});
                    break;
                },
            };

            // The cap. Checked before spawning, so a flood costs one accept and one close
            // rather than a thread — and the counter is decremented by `serviceStream`
            // itself, so a task that dies still releases its slot.
            //
            // Closing immediately, not queueing: a Prometheus scraper retries on its own
            // schedule, and a queue nobody drains is just a slower outage.
            if (self.open_conns.load(.monotonic) >= Config.Http.max_connections) {
                log.warn("⚠️  {d} connections already open — refusing another", .{Config.Http.max_connections});
                stream.close(self.io);
                continue;
            }
            _ = self.open_conns.fetchAdd(1, .monotonic);

            // ⚠️ `concurrent`, not `async`, and the difference is the whole point.
            //
            // `Io.async` is *allowed to run the task inline*: std says of the thread pool
            // limit, "after this limit, calls to `Io.async` when all threads are busy run
            // the task immediately". Measured — with `async` a single client that opened
            // a socket and sent **zero bytes** still wedged the server: three consecutive
            // `GET /metrics` returned `000`, exactly as with the old inline C loop.
            // `Io.concurrent` guarantees a separate thread, so a stalled connection can
            // never occupy the accept loop.
            //
            // A rate limiter would not have helped either version: one connection, zero
            // requests, nothing to count.
            //
            // Into a `Group` so shutdown can wait for in-flight responses instead of
            // abandoning them. `ConcurrentDeadlock` is the one refusal worth handling: it
            // means the runtime cannot promise a thread, and serving inline then is better
            // than dropping the request — a stall is possible but a lost response is
            // certain.
            conns.concurrent(self.io, serviceStream, .{ self, stream }) catch {
                // The runtime cannot promise a thread. Serving inline risks a stall;
                // dropping guarantees a lost response, so this takes the risk.
                serviceStream(self, stream);
            };
        }

        log.info("👋 HTTP server stopped", .{});
    }

    /// Serve one connection, then close it.
    ///
    /// ⚠️ Still missing a receive deadline. A client that connects and never sends now
    /// parks one task instead of the accept loop — a leak rather than an outage, which is
    /// the right direction but not a resolution. A bounded read timeout (and a cap on
    /// concurrent connections) is the remaining hardening; both belong here, not in the
    /// accept loop.
    fn serviceStream(self: *Server, stream: std.Io.net.Stream) void {
        defer stream.close(self.io);
        defer _ = self.open_conns.fetchSub(1, .monotonic);

        // A watchdog, because the reader has no per-read deadline: it sleeps, then shuts
        // down the receive side if the request has not been parsed yet. `shutdown` is what
        // unblocks a read that is waiting on a client saying nothing — the same mechanism
        // std documents for cancelling `accept`.
        //
        // `concurrent` for the same reason as above: an `async` watchdog is allowed to run
        // inline, which would mean sleeping *before* serving the request — turning every
        // scrape into a 3-second wait.
        var done = std.atomic.Value(bool).init(false);
        var watchdog: std.Io.Group = .init;
        defer watchdog.cancel(self.io);
        watchdog.concurrent(self.io, receiveWatchdog, .{ self, stream, &done }) catch {};

        // Sized for what actually crosses this socket, because these are per-connection
        // and now multiplied by `max_connections`.
        //
        //   read  — a request head. `GET /metrics HTTP/1.1` plus Prometheus' headers is
        //           ~200 bytes; 1 KiB leaves room for a browser's, and a head that does
        //           not fit is refused rather than served, which is the right answer for
        //           an endpoint with no long URLs.
        //   write — the largest response. `/metrics` measured 2,296 bytes and grows with
        //           the table count, so 8 KiB is headroom rather than a limit. ⚠️ Unlike
        //           the old hand-built response buffer this is not a truncation point:
        //           `std.http` streams a body larger than the buffer.
        var read_buffer: [1024]u8 = undefined;
        var write_buffer: [8192]u8 = undefined;
        var reader = stream.reader(self.io, &read_buffer);
        var writer = stream.writer(self.io, &write_buffer);

        var http = std.http.Server.init(&reader.interface, &writer.interface);

        // One request per connection: every response says `Connection: close`, and
        // telemetry scrapes are not hot enough for keep-alive to matter.
        var req = http.receiveHead() catch |err| {
            done.store(true, .release);
            // Includes a client that connected and sent nothing, or sent garbage. Debug,
            // not error: it is neither the operator's problem nor unusual on an open port.
            log.debug("no usable request: {s}", .{@errorName(err)});
            return;
        };

        done.store(true, .release);

        self.dispatch(&req) catch |err| {
            log.debug("failed to respond: {s}", .{@errorName(err)});
        };
    }

    /// Shut down a connection that has not sent a parseable request in time.
    ///
    /// Runs alongside `serviceStream`; `done` is set the moment the head is parsed, so a
    /// normal request never sees this fire.
    fn receiveWatchdog(self: *Server, stream: std.Io.net.Stream, done: *std.atomic.Value(bool)) void {
        std.Io.Timeout.sleep(
            .{ .duration = .{ .raw = .fromNanoseconds(Config.Http.receive_timeout_ns), .clock = .awake } },
            self.io,
        ) catch return;
        if (done.load(.acquire)) return;
        log.debug("no request within {d}ms — closing", .{Config.Http.receive_timeout_ns / std.time.ns_per_ms});
        stream.shutdown(self.io, .recv) catch {};
    }

    /// Route one parsed request.
    ///
    /// Parsing is `std.http`'s job now — this used to split the raw bytes on '\n' and
    /// take the first two space-separated tokens, over a fixed 2048-byte buffer.
    fn dispatch(self: *Server, req: *std.http.Server.Request) !void {
        const target = req.head.target;
        const method = req.head.method;
        log.debug("{s} {s}", .{ @tagName(method), target });

        if (method == .GET and std.mem.eql(u8, target, "/health")) {
            try self.handleHealth(req);
        } else if (method == .GET and std.mem.eql(u8, target, "/status")) {
            try self.handleStatus(req);
        } else if (method == .GET and std.mem.eql(u8, target, "/metrics")) {
            try self.handleMetrics(req);
        } else {
            try req.respond("Not Found\n", .{ .status = .not_found });
        }
    }

    /// `std.http` writes the status line, headers and framing; this only picks the type.
    ///
    /// Replaces a hand-built response string over a 4096-byte buffer that silently
    /// truncated anything larger — `/metrics` was already close to it.
    fn respond(req: *std.http.Server.Request, status: std.http.Status, content_type: []const u8, body: []const u8) !void {
        try req.respond(body, .{
            .status = status,
            .extra_headers = &.{.{ .name = "content-type", .value = content_type }},
        });
    }

    // -------------------------------------------------------------------------
    // Handlers
    // -------------------------------------------------------------------------

    fn handleHealth(self: *Server, req: *std.http.Server.Request) !void {
        _ = self;
        try respond(req, .ok, "application/json", "{\"status\":\"ok\"}\n");
    }

    fn handleStatus(self: *Server, req: *std.http.Server.Request) !void {
        const status_cpu_ns = utils.cpuTimeNanos();
        if (self.metrics) |m| {
            const snap = try m.snapshot(self.allocator);
            defer self.allocator.free(snap.current_lsn_str);

            var buf: [4096]u8 = undefined;
            const body = try std.fmt.bufPrint(&buf,
                \\{{
                \\  "status": "{s}",
                \\  "uptime_seconds": {d},
                \\  "wal_messages_received": {d},
                \\  "cdc_events_published": {d},
                \\  "current_lsn": "{s}",
                \\  "is_connected": {s},
                \\  "pg_reconnect_count": {d},
                \\  "nats_reconnect_count": {d},
                \\  "slot_active": {s},
                \\  "wal_lag_bytes": {d},
                \\  "wal_lag_mb": {d},
                \\  "wal_confirmed_lag_bytes": {d},
                \\  "cpu_seconds": {d}.{d:0>3},
                \\  "max_rss_mb": {d},
                \\  "queue_usage_percent": {d},
                \\  "refused_tables": {d},
                \\  "refused_events_dropped": {d}
                \\}}
            , .{
                if (snap.is_connected) "connected" else "disconnected",
                snap.uptime_seconds,
                snap.wal_messages_received,
                snap.cdc_events_published,
                snap.current_lsn_str,
                if (snap.is_connected) "true" else "false",
                snap.reconnect_count,
                snap.nats_reconnect_count,
                if (snap.slot_active) "true" else "false",
                snap.wal_lag_bytes,
                snap.wal_lag_bytes / (1024 * 1024),
                snap.wal_confirmed_lag_bytes,
                status_cpu_ns / std.time.ns_per_s,
                (status_cpu_ns % std.time.ns_per_s) / std.time.ns_per_ms,
                utils.maxRssBytes() / (1024 * 1024),
                snap.queue_usage_percent,
                if (self.refused) |r| r.refused_count.load(.acquire) else 0,
                if (self.refused) |r| r.dropped_total.load(.acquire) else 0,
            });

            try respond(req, .ok, "application/json", body);
        } else {
            try respond(req, .ok, "application/json", "{\"status\":\"no_metrics\"}\n");
        }
    }

    /// ⚠️ Keep every value `{d}` on an unsigned integer. Never `{d:.2}` on a float.
    ///
    /// Prometheus drops the **entire scrape** on one unparseable line — the failure shows
    /// up as dashboards going quiet, not as an error anywhere. Integer formatting cannot
    /// produce `nan` or `inf`, so the document is well-formed by construction rather than
    /// by care.
    ///
    /// `bridge_cpu_seconds_total` looks like an exception and is not: it is written as
    /// `{d}.{d:0>3}` from two integer divisions of a nanosecond counter, so both halves
    /// are always digits. A float would reintroduce the risk for cosmetic gain.
    ///
    /// (Truncation was the other way to break the document — the pre-`std.http` version
    /// built responses in a 4096-byte buffer and cut mid-line above that, with /metrics
    /// already past 2 KB. The body streams now.)
    fn handleMetrics(self: *Server, req: *std.http.Server.Request) !void {
        // Read once: two getrusage calls would report two different instants.
        const cpu_ns = utils.cpuTimeNanos();
        if (self.metrics) |m| {
            const snap = try m.snapshot(self.allocator);
            defer self.allocator.free(snap.current_lsn_str);

            var buf: [8192]u8 = undefined;
            const body = try std.fmt.bufPrint(&buf,
                \\# HELP bridge_uptime_seconds Time since bridge started
                \\# TYPE bridge_uptime_seconds gauge
                \\bridge_uptime_seconds {d}
                \\
                \\# HELP bridge_wal_messages_received_total Total WAL messages received from PostgreSQL
                \\# TYPE bridge_wal_messages_received_total counter
                \\bridge_wal_messages_received_total {d}
                \\
                \\# HELP bridge_cdc_events_published_total Total CDC events published to NATS
                \\# TYPE bridge_cdc_events_published_total counter
                \\bridge_cdc_events_published_total {d}
                \\
                \\# HELP bridge_schema_events_published_total SCHEMA/KV events published to NATS (DDL schemas, suspensions, drop tombstones) - kept out of the CDC counter so that one stays equal to row events
                \\# TYPE bridge_schema_events_published_total counter
                \\bridge_schema_events_published_total {d}
                \\
                \\# HELP bridge_last_ack_lsn Last acknowledged LSN position
                \\# TYPE bridge_last_ack_lsn gauge
                \\bridge_last_ack_lsn {d}
                \\
                \\# HELP bridge_connected Connection status (1=connected, 0=disconnected)
                \\# TYPE bridge_connected gauge
                \\bridge_connected {d}
                \\
                \\# HELP bridge_pg_reconnects_total Total number of PostgreSQL reconnections
                \\# TYPE bridge_pg_reconnects_total counter
                \\bridge_pg_reconnects_total {d}
                \\
                \\# HELP bridge_nats_reconnects_total Total number of NATS reconnections
                \\# TYPE bridge_nats_reconnects_total counter
                \\bridge_nats_reconnects_total {d}
                \\
                \\# HELP bridge_slot_active Replication slot active status (1=active, 0=inactive)
                \\# TYPE bridge_slot_active gauge
                \\bridge_slot_active {d}
                \\
                \\# HELP bridge_wal_lag_bytes Bytes of WAL PostgreSQL retains for the slot (from restart_lsn, moves at checkpoints)
                \\# TYPE bridge_wal_lag_bytes gauge
                \\bridge_wal_lag_bytes {d}
                \\
                \\# HELP bridge_wal_confirmed_lag_bytes Bytes of WAL the bridge has not yet confirmed (from confirmed_flush_lsn) - the real backlog
                \\# TYPE bridge_wal_confirmed_lag_bytes gauge
                \\bridge_wal_confirmed_lag_bytes {d}
                \\
                \\# HELP bridge_queue_usage_percent Event queue usage percentage (0-100)
                \\# TYPE bridge_queue_usage_percent gauge
                \\bridge_queue_usage_percent {d}
                \\
                \\# HELP bridge_nats_publish_ack_seconds_total Summed wall time between issuing a JetStream publish and its PubAck (divide by bridge_nats_publishes_total for the mean)
                \\# TYPE bridge_nats_publish_ack_seconds_total counter
                \\bridge_nats_publish_ack_seconds_total {d}.{d:0>6}
                \\
                \\# HELP bridge_nats_publishes_total JetStream publishes timed by the flush thread
                \\# TYPE bridge_nats_publishes_total counter
                \\bridge_nats_publishes_total {d}
                \\
                \\# HELP bridge_cpu_seconds_total CPU time consumed by the bridge process (user + system)
                \\# TYPE bridge_cpu_seconds_total counter
                \\bridge_cpu_seconds_total {d}.{d:0>3}
                \\
                \\# HELP bridge_max_rss_bytes Peak resident set size of the bridge process
                \\# TYPE bridge_max_rss_bytes gauge
                \\bridge_max_rss_bytes {d}
                \\
            , .{
                snap.uptime_seconds,
                snap.wal_messages_received,
                snap.cdc_events_published,
                snap.schema_events_published,
                snap.last_ack_lsn,
                if (snap.is_connected) @as(u8, 1) else @as(u8, 0),
                snap.reconnect_count,
                snap.nats_reconnect_count,
                if (snap.slot_active) @as(u8, 1) else @as(u8, 0),
                snap.wal_lag_bytes,
                snap.wal_confirmed_lag_bytes,
                snap.queue_usage_percent,
                snap.nats_publish_ack_ns / std.time.ns_per_s,
                (snap.nats_publish_ack_ns % std.time.ns_per_s) / std.time.ns_per_us,
                snap.nats_publishes,
                cpu_ns / std.time.ns_per_s,
                (cpu_ns % std.time.ns_per_s) / std.time.ns_per_ms,
                utils.maxRssBytes(),
            });

            // Appended rather than folded into the format string: the registry owns its
            // own exposition text so the metric names stay next to the thing counting.
            var w = std.Io.Writer.fixed(buf[body.len..]);
            if (self.refused) |r| try r.writePrometheus(&w);

            try respond(req, .ok, "text/plain", buf[0 .. body.len + w.buffered().len]);
        } else {
            try respond(req, .ok, "text/plain", "# No metrics available\n");
        }
    }

    // `POST /shutdown` was removed.
    //
    // It stopped CDC in one unauthenticated request, from any interface, and it duplicated
    // a capability the process already has: SIGTERM reaches the same graceful path
    // (`bridge.zig` registers the handlers). The difference is who can use it — SIGTERM
    // needs ownership of the process, an HTTP route needs only a socket.
    //
    // ⚠️ Rate limiting would not have helped: the first request wins, and stopping the
    // bridge once is the whole attack. Removing the route is the fix; `kill -TERM <pid>`
    // or `docker stop` is the replacement.

    fn handleNotFound(self: *Server, req: *std.http.Server.Request) !void {
        _ = self;
        try respond(req, .not_found, "text/plain", "Not Found\n");
    }

    // -------------------------------------------------------------------------
    // NATS stream management endpoints  (?stream=NAME query param)
    // -------------------------------------------------------------------------

    fn parseQueryParam(path: []const u8, param_name: []const u8) ?[]const u8 {
        const query_start = std.mem.indexOf(u8, path, "?") orelse return null;
        var it = std.mem.splitScalar(u8, path[query_start + 1 ..], '&');
        while (it.next()) |param| {
            if (std.mem.indexOf(u8, param, "=")) |eq_pos| {
                if (std.mem.eql(u8, param[0..eq_pos], param_name)) {
                    return param[eq_pos + 1 ..];
                }
            }
        }
        return null;
    }

    // `GET /streams/info?stream=` was removed.
    //
    // It made a **NATS round trip per HTTP request** — an unauthenticated caller turning
    // a cheap request into broker work — and disclosed stream names and configuration.
    // It was also redundant: NATS publishes the same facts on its own monitoring port
    // (`/jsz`, `/varz` on 8222), which is what the nats exporter and Prometheus already
    // scrape. The bridge was proxying a service that answers for itself.
    //
    // This is the endpoint a rate limiter would genuinely have helped, which is the
    // argument for deleting it rather than limiting it: the cheapest request is the one
    // that is never served.

    // `POST /streams/purge` was removed along with its handler.
    //
    // It deleted stream data on an unauthenticated request. Its route had already gone
    // with `/streams/info`, leaving it reachable by nobody — but still wired to
    // `purgeStream`, one `else if` away from being live again. Dead code that deletes
    // data is worth removing rather than leaving for someone to rediscover.
};
