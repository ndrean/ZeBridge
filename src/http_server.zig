const std = @import("std");
const metrics_mod = @import("metrics.zig");
const nats_publisher = @import("nats_publisher.zig");
const utils = @import("utils.zig");

// std.net was removed in Zig 0.16 "Juicy Main". Use C POSIX sockets directly.
const sock_c = @cImport({
    @cInclude("sys/socket.h");
    @cInclude("netinet/in.h");
    @cInclude("unistd.h");
    @cInclude("poll.h");
});

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
        if (self.thread != null) return error.AlreadyStarted;
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
        const sock_raw = sock_c.socket(sock_c.AF_INET, sock_c.SOCK_STREAM, 0);
        if (sock_raw < 0) return error.SocketFailed;
        const sock_fd: c_int = sock_raw;
        defer _ = sock_c.close(sock_fd);

        const optval: c_int = 1;
        _ = sock_c.setsockopt(sock_fd, sock_c.SOL_SOCKET, sock_c.SO_REUSEADDR,
            @ptrCast(&optval), @as(c_uint, @sizeOf(c_int)));

        var addr = std.mem.zeroes(sock_c.struct_sockaddr_in);
        addr.sin_family = @intCast(sock_c.AF_INET);
        addr.sin_port = std.mem.nativeToBig(u16, self.port);
        // sin_addr.s_addr == 0 is INADDR_ANY (zeroed by zeroes())

        if (sock_c.bind(sock_fd, @ptrCast(&addr), @as(c_uint, @sizeOf(sock_c.struct_sockaddr_in))) < 0)
            return error.BindFailed;
        if (sock_c.listen(sock_fd, 128) < 0)
            return error.ListenFailed;

        log.info("✅ HTTP server listening on http://0.0.0.0:{d}", .{self.port});
        log.info("ℹ️ Available endpoints:", .{});
        log.info("  GET  /health         - Health check", .{});
        log.info("  GET  /status         - Bridge status (JSON)", .{});
        log.info("  GET  /metrics        - Prometheus metrics", .{});
        log.info("  POST /shutdown       - Graceful shutdown", .{});
        log.info("  GET  /streams/info?stream=NAME   - NATS stream info", .{});

        while (!self.should_stop.load(.seq_cst)) {
            // Poll with 100 ms timeout so we can re-check should_stop without blocking.
            var poll_fds = [_]sock_c.struct_pollfd{
                .{
                    .fd = sock_fd,
                    .events = @as(c_short, @intCast(sock_c.POLLIN)),
                    .revents = 0,
                },
            };

            const ready = sock_c.poll(&poll_fds[0], 1, 100);
            if (ready < 0) {
                log.err("⚠️ Poll error", .{});
                utils.sleep(100 * std.time.ns_per_ms);
                continue;
            }
            if (ready == 0) continue;

            const client_raw = sock_c.accept(sock_fd, null, null);
            if (client_raw < 0) {
                log.err("🔴 Failed to accept connection", .{});
                continue;
            }
            const client_fd: c_int = client_raw;
            defer _ = sock_c.close(client_fd);

            self.handleRequest(client_fd) catch |err| {
                log.warn("⚠️ Error handling request: {}", .{err});
            };
        }

        log.info("👋 HTTP server stopped", .{});
    }

    fn handleRequest(self: *Server, fd: c_int) !void {
        var buffer: [2048]u8 = undefined;
        const bytes_read_n = sock_c.read(fd, &buffer, buffer.len);
        if (bytes_read_n <= 0) return;
        const bytes_read: usize = @intCast(bytes_read_n);

        const request = buffer[0..bytes_read];

        var lines = std.mem.splitScalar(u8, request, '\n');
        const first_line = lines.next() orelse return;
        var parts = std.mem.splitScalar(u8, first_line, ' ');
        const method = std.mem.trim(u8, parts.next() orelse return, &std.ascii.whitespace);
        const path = std.mem.trim(u8, parts.next() orelse return, &std.ascii.whitespace);

        log.debug("{s} {s}", .{ method, path });

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

    // HTTP/1.1 requires CRLF (\r\n) line endings in the response header.
    fn sendResponse(
        fd: c_int,
        status: []const u8,
        content_type: []const u8,
        body: []const u8,
    ) !void {
        var buf: [4096]u8 = undefined;
        const response = try std.fmt.bufPrint(&buf,
            "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
            .{ status, content_type, body.len, body },
        );

        var pos: usize = 0;
        while (pos < response.len) {
            const n = sock_c.write(fd, response[pos..].ptr, response[pos..].len);
            if (n <= 0) return;
            pos += @intCast(n);
        }
    }

    // -------------------------------------------------------------------------
    // Handlers
    // -------------------------------------------------------------------------

    fn handleHealth(self: *Server, fd: c_int) !void {
        _ = self;
        try sendResponse(fd, "200 OK", "application/json", "{\"status\":\"ok\"}\n");
    }

    fn handleStatus(self: *Server, fd: c_int) !void {
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
                snap.wal_lag_bytes / (1024 * 1024),
                snap.queue_usage_percent,
            });

            try sendResponse(fd, "200 OK", "application/json", body);
        } else {
            try sendResponse(fd, "200 OK", "application/json", "{\"status\":\"no_metrics\"}\n");
        }
    }

    fn handleMetrics(self: *Server, fd: c_int) !void {
        if (self.metrics) |m| {
            const snap = try m.snapshot(self.allocator);
            defer self.allocator.free(snap.current_lsn_str);

            var buf: [4096]u8 = undefined;
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

            try sendResponse(fd, "200 OK", "text/plain", body);
        } else {
            try sendResponse(fd, "200 OK", "text/plain", "# No metrics available\n");
        }
    }

    fn handleShutdown(self: *Server, fd: c_int) !void {
        log.info("👋 Shutdown requested via HTTP", .{});
        self.should_stop.store(true, .seq_cst);
        try sendResponse(fd, "200 OK", "text/plain", "Shutdown initiated\n");
    }

    fn handleNotFound(self: *Server, fd: c_int) !void {
        _ = self;
        try sendResponse(fd, "404 Not Found", "text/plain", "Not Found\n");
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

    fn handleStreamInfo(self: *Server, fd: c_int, path: []const u8) !void {
        const stream_name = parseQueryParam(path, "stream") orelse {
            try sendResponse(fd, "400 Bad Request", "text/plain", "Missing 'stream' parameter\n");
            return;
        };

        if (self.nats_publisher) |publisher| {
            const info = publisher.getStreamInfo(stream_name) catch |err| {
                var err_buf: [256]u8 = undefined;
                const msg = try std.fmt.bufPrint(&err_buf, "Failed to get stream info: {}\n", .{err});
                try sendResponse(fd, "500 Internal Server Error", "text/plain", msg);
                return;
            };
            defer self.allocator.free(info);
            try sendResponse(fd, "200 OK", "application/json", info);
        } else {
            try sendResponse(fd, "503 Service Unavailable", "text/plain", "NATS publisher not available\n");
        }
    }

    fn handleStreamPurge(self: *Server, fd: c_int, path: []const u8) !void {
        const stream_name = parseQueryParam(path, "stream") orelse {
            try sendResponse(fd, "400 Bad Request", "text/plain", "Missing 'stream' parameter\n");
            return;
        };

        if (self.nats_publisher) |publisher| {
            publisher.purgeStream(stream_name) catch |err| {
                var err_buf: [256]u8 = undefined;
                const msg = try std.fmt.bufPrint(&err_buf, "Failed to purge stream: {}\n", .{err});
                try sendResponse(fd, "500 Internal Server Error", "text/plain", msg);
                return;
            };
            var resp_buf: [256]u8 = undefined;
            const resp = try std.fmt.bufPrint(&resp_buf, "Stream '{s}' purged\n", .{stream_name});
            try sendResponse(fd, "200 OK", "text/plain", resp);
        } else {
            try sendResponse(fd, "503 Service Unavailable", "text/plain", "NATS publisher not available\n");
        }
    }
};
