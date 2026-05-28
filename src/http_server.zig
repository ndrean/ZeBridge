const std = @import("std");
const posix = std.posix;
const metrics_mod = @import("metrics.zig");
const nats_publisher = @import("nats_publisher.zig");

pub const log = std.log.scoped(.http_server);

/// Simple HTTP server for health checks and basic control
pub const Server = struct {
    allocator: std.mem.Allocator,
    port: u16,
    should_stop: *std.atomic.Value(bool),
    metrics: ?*metrics_mod.Metrics,
    nats_publisher: ?*nats_publisher.Publisher,
    thread: ?std.Thread = null,

    pub fn init(
        allocator: std.mem.Allocator,
        port: u16,
        should_stop: *std.atomic.Value(bool),
        metrics: ?*metrics_mod.Metrics,
        nats_pub: ?*nats_publisher.Publisher,
    ) !Server {
        return Server{
            .allocator = allocator,
            .port = port,
            .should_stop = should_stop,
            .metrics = metrics,
            .nats_publisher = nats_pub,
            .thread = null,
        };
    }

    /// Start the HTTP server thread
    pub fn start(self: *Server) !void {
        if (self.thread != null) {
            return error.AlreadyStarted;
        }
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    /// Join the HTTP server thread (waits for completion)
    pub fn join(self: *Server) void {
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
        const sock_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
        defer posix.close(sock_fd);

        const one: i32 = 1;
        try posix.setsockopt(sock_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&one));

        var addr = std.mem.zeroes(posix.sockaddr.in);
        addr.family = posix.AF.INET;
        addr.port = std.mem.nativeToBig(u16, self.port);
        // addr.addr = 0 is INADDR_ANY (already zeroed)

        try posix.bind(sock_fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));
        try posix.listen(sock_fd, 128);

        log.info("✅ HTTP server listening on http://0.0.0.0:{d}", .{self.port});
        log.info("ℹ️ Available endpoints:", .{});
        log.info("  GET  /health         - Health check", .{});
        log.info("  GET  /status         - Bridge status (JSON)", .{});
        log.info("  GET  /metrics        - Prometheus metrics", .{});
        log.info("  POST /shutdown       - Graceful shutdown", .{});
        log.info("  GET  /streams/info?stream=NAME   - NATS stream info", .{});

        while (!self.should_stop.load(.seq_cst)) {
            // Poll with timeout to allow checking shutdown flag
            var poll_fds = [_]posix.pollfd{
                .{
                    .fd = sock_fd,
                    .events = posix.POLL.IN,
                    .revents = 0,
                },
            };

            // Poll with 100ms timeout
            const ready = posix.poll(&poll_fds, 100) catch |err| {
                log.err("⚠️ Poll error: {}", .{err});
                std.Thread.sleep(100 * std.time.ns_per_ms);
                continue;
            };

            if (ready == 0) {
                // Timeout - check shutdown flag again
                continue;
            }

            const client_fd = posix.accept(sock_fd, null, null) catch |err| {
                log.err("🔴 Failed to accept connection: {}", .{err});
                continue;
            };
            defer posix.close(client_fd);

            self.handleRequest(client_fd) catch |err| {
                log.warn("⚠️ Error handling request: {}", .{err});
            };
        }

        log.info("👋 HTTP server stopped", .{});
    }

    fn handleRequest(self: *Server, fd: posix.fd_t) !void {
        var buffer: [2048]u8 = undefined;

        const bytes_read = try posix.read(fd, &buffer);
        if (bytes_read == 0) return;

        const request = buffer[0..bytes_read];

        // Parse method and path (simple HTTP parser)
        var lines = std.mem.splitScalar(u8, request, '\n');
        const first_line = lines.next() orelse return;

        var parts = std.mem.splitScalar(u8, first_line, ' ');
        const method = std.mem.trim(u8, parts.next() orelse return, &std.ascii.whitespace);
        const path = std.mem.trim(u8, parts.next() orelse return, &std.ascii.whitespace);

        log.debug("{s} {s}", .{ method, path });

        // Route
        if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/health")) {
            try self.handleHealth(fd);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/status")) {
            try self.handleStatus(fd);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/metrics")) {
            try self.handleMetrics(fd);
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/shutdown")) {
            try self.handleShutdown(fd);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/streams/info")) {
            try self.handleStreamInfo(fd, path);
        } else {
            try self.handleNotFound(fd);
        }
    }

    fn sendResponse(
        fd: posix.fd_t,
        status: []const u8,
        content_type: []const u8,
        body: []const u8,
    ) !void {
        var response_buffer: [4096]u8 = undefined;
        const response = try std.fmt.bufPrint(&response_buffer,
            \\HTTP/1.1 {s}
            \\Content-Type: {s}
            \\Content-Length: {d}
            \\Connection: close
            \\
            \\{s}
        , .{ status, content_type, body.len, body });

        var pos: usize = 0;
        while (pos < response.len) {
            const n = try posix.write(fd, response[pos..]);
            if (n == 0) return;
            pos += n;
        }
    }

    // handlers ----------------------------------------------------
    fn handleHealth(self: *Server, fd: posix.fd_t) !void {
        _ = self;
        try sendResponse(
            fd,
            "200 OK",
            "application/json",
            "{\"status\":\"ok\"}\n",
        );
    }

    fn handleStatus(self: *Server, fd: posix.fd_t) !void {
        if (self.metrics) |m| {
            // Get metrics snapshot
            const snap = try m.snapshot(self.allocator);
            defer self.allocator.free(snap.current_lsn_str);

            // Format JSON with real metrics
            var buffer: [4096]u8 = undefined;
            const status_json = try std.fmt.bufPrint(&buffer,
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
                \\  "queue_usage_percent": {d}
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
                snap.wal_lag_bytes / (1024 * 1024), // Convert to MB
                snap.queue_usage_percent,
            });

            try sendResponse(
                fd,
                "200 OK",
                "application/json",
                status_json,
            );
        } else {
            // Fallback if no metrics available
            try sendResponse(
                fd,
                "200 OK",
                "application/json",
                "{\"status\":\"no_metrics\"}\n",
            );
        }
    }

    fn handleMetrics(self: *Server, fd: posix.fd_t) !void {
        if (self.metrics) |m| {
            // Get metrics snapshot
            const snap = try m.snapshot(self.allocator);
            defer self.allocator.free(snap.current_lsn_str);

            // Format Prometheus text format
            var buffer: [4096]u8 = undefined;
            const prom_metrics = try std.fmt.bufPrint(&buffer,
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
                \\# HELP bridge_wal_lag_bytes Bytes of WAL retained for replication slot
                \\# TYPE bridge_wal_lag_bytes gauge
                \\bridge_wal_lag_bytes {d}
                \\
                \\# HELP bridge_queue_usage_percent Event queue usage percentage (0-100)
                \\# TYPE bridge_queue_usage_percent gauge
                \\bridge_queue_usage_percent {d}
                \\
            , .{
                snap.uptime_seconds,
                snap.wal_messages_received,
                snap.cdc_events_published,
                snap.last_ack_lsn,
                if (snap.is_connected) @as(u8, 1) else @as(u8, 0),
                snap.reconnect_count,
                snap.nats_reconnect_count,
                if (snap.slot_active) @as(u8, 1) else @as(u8, 0),
                snap.wal_lag_bytes,
                snap.queue_usage_percent,
            });

            try sendResponse(
                fd,
                "200 OK",
                "text/plain",
                prom_metrics,
            );
        } else {
            // Empty metrics if not available
            try sendResponse(
                fd,
                "200 OK",
                "text/plain;",
                "# ⚠️ No metrics available\n",
            );
        }
    }

    fn handleShutdown(self: *Server, fd: posix.fd_t) !void {
        log.info("👋 Shutdown requested via HTTP", .{});

        // Set shutdown flag
        self.should_stop.store(true, .seq_cst);

        try sendResponse(
            fd,
            "200 OK",
            "text/plain",
            "Shutdown initiated\n",
        );
    }

    fn handleNotFound(self: *Server, fd: posix.fd_t) !void {
        _ = self;
        try sendResponse(
            fd,
            "404 Not Found",
            "text/plain",
            "Not Found\n",
        );
    }

    // NATS Stream Management Endpoints. "path?key=value&..."

    fn parseQueryParam(path: []const u8, param_name: []const u8) ?[]const u8 {
        const query_start = std.mem.indexOf(u8, path, "?") orelse return null;

        var it = std.mem.splitScalar(u8, path[query_start + 1 ..], '&');
        while (it.next()) |param| {
            if (std.mem.indexOf(u8, param, "=")) |eq_pos| {
                const key = param[0..eq_pos];
                const value = param[eq_pos + 1 ..];
                if (std.mem.eql(u8, key, param_name)) {
                    return value;
                }
            }
        }
        return null;
    }

    fn handleStreamInfo(self: *Server, fd: posix.fd_t, path: []const u8) !void {
        const stream_name = parseQueryParam(path, "stream") orelse {
            try sendResponse(
                fd,
                "400 Bad Request",
                "text/plain",
                "⚠️ Missing 'stream' parameter\n",
            );
            return;
        };

        if (self.nats_publisher) |publisher| {
            // Get stream info from NATS
            const info = publisher.getStreamInfo(stream_name) catch |err| {
                var err_buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&err_buf, "Failed to get stream info: {}\n", .{err});
                try sendResponse(fd, "500 Internal Server Error", "text/plain", err_msg);
                return;
            };
            defer self.allocator.free(info);

            try sendResponse(fd, "200 OK", "application/json", info);
        } else {
            try sendResponse(fd, "503 Service Unavailable", "text/plain", "⚠️ NATS publisher not available\n");
        }
    }

    fn handleStreamPurge(self: *Server, fd: posix.fd_t, path: []const u8) !void {
        const stream_name = parseQueryParam(path, "stream") orelse {
            try sendResponse(
                fd,
                "400 Bad Request",
                "text/plain",
                "⚠️ Missing 'stream' parameter\n",
            );
            return;
        };

        if (self.nats_publisher) |publisher| {
            publisher.purgeStream(stream_name) catch |err| {
                var err_buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&err_buf, "⚠️ Failed to purge stream: {}\n", .{err});
                try sendResponse(fd, "500 Internal Server Error", "text/plain", err_msg);
                return;
            };

            var response_buf: [256]u8 = undefined;
            const response = try std.fmt.bufPrint(&response_buf, "Stream '{s}' purged\n", .{stream_name});
            try sendResponse(fd, "200 OK", "text/plain", response);
        } else {
            try sendResponse(fd, "503 Service Unavailable", "text/plain", "⚠️ NATS publisher not available\n");
        }
    }
};
