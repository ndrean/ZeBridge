//! WAL monitoring background thread functionality.
//!
//! It monitoring WAL lag using `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`and includes getting current WAL LSN
const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const utils = @import("utils.zig");
const pg_conn = @import("pg_conn.zig");
const metrics_mod = @import("metrics.zig");
const Conf = @import("config.zig");

pub const log = std.log.scoped(.wal_monitor);

/// Configuration for WAL monitoring
pub const WalConfig = struct {
    pg_config: *const pg_conn.PgConf,
    slot_name: []const u8, // via CLI arg
    check_interval_seconds: u32 = Conf.WalMonitor.default_check_interval_seconds, // 30s
};

/// WAL monitor with thread management
pub const WalMonitor = struct {
    metrics: *metrics_mod.Metrics,
    config: WalConfig,
    should_stop: *std.atomic.Value(bool),
    allocator: std.mem.Allocator,
    thread: ?std.Thread = null,

    /// Initialize WAL monitor (does not start the thread)
    pub fn init(
        allocator: std.mem.Allocator,
        metrics: *metrics_mod.Metrics,
        config: WalConfig,
        should_stop: *std.atomic.Value(bool),
    ) WalMonitor {
        return .{
            .metrics = metrics,
            .config = config,
            .should_stop = should_stop,
            .allocator = allocator,
            .thread = null,
        };
    }

    /// Start the WAL monitoring loop thread that queries WAL lag periodically.
    pub fn start(self: *WalMonitor) !void {
        if (self.thread != null) {
            return error.AlreadyStarted;
        }
        self.thread = try std.Thread.spawn(.{}, monitorLoop, .{self});
    }

    /// Join the WAL monitoring thread (waits for completion)
    pub fn join(self: *WalMonitor) void {
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    /// Deinit - cleanup resources (call after join)
    pub fn deinit(self: *WalMonitor) void {
        // No resources to clean up currently
        _ = self;
    }

    /// checkWALLag monitoring loop (internal)
    fn monitorLoop(self: *WalMonitor) !void {
        log.info(
            "ℹ️ WAL lag monitor started (checking every {d}s)\n",
            .{self.config.check_interval_seconds},
        );

        while (!self.should_stop.load(.seq_cst)) {
            // Check WAL lag
            checkWalLag(
                self.metrics,
                self.config,
                self.allocator,
            ) catch |err| {
                log.warn(
                    "⚠️ Failed to check WAL lag: {}",
                    .{err},
                );
            };

            // Sleep for check interval
            var remaining_seconds = self.config.check_interval_seconds;
            while (remaining_seconds > 0 and !self.should_stop.load(.seq_cst)) {
                utils.sleep(1 * std.time.ns_per_s);
                remaining_seconds -= 1;
            }
        }

        log.info("🥁 WAL lag monitor stopped\n", .{});
    }
};

/// Get current WAL LSN position with the query `SELECT pg_current_wal_lsn()::text`
///
/// Caller is responsible for freeing the returned LSN string
pub fn getCurrentLSN(allocator: std.mem.Allocator, pg_conf: *const pg_conn.PgConf) ![]const u8 {
    const conninfo = try pg_conf.connInfo(allocator, false);
    defer allocator.free(conninfo);

    const conn = c.PQconnectdb(conninfo.ptr) orelse {
        log.err("🔴 Connection failed: PQconnectdb returned null", .{});
        return error.ConnectionFailed;
    };

    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        const err_msg = c.PQerrorMessage(conn);
        log.err("🔴 Connection failed: {s}", .{err_msg});
        c.PQfinish(conn);
        return error.ConnectionFailed;
    }

    defer c.PQfinish(conn);

    const query = "SELECT pg_current_wal_lsn()::text";

    const result = c.PQexec(conn, query.ptr) orelse {
        log.err("🔴 Query execution failed: PQexec returned null", .{});
        return error.QueryFailed;
    };

    if (c.PQresultStatus(result) != c.PGRES_TUPLES_OK and c.PQresultStatus(result) != c.PGRES_COMMAND_OK) {
        const err_msg = c.PQerrorMessage(conn);
        log.err("🔴 Query failed: {s}", .{err_msg});
        c.PQclear(result);
        return error.QueryFailed;
    }
    defer c.PQclear(result);

    if (c.PQntuples(result) == 0) {
        return error.NoLSNReturned;
    }

    const lsn_cstr = c.PQgetvalue(result, 0, 0);
    // potential error if lsn_cstr is null, but PQgetvalue should not return null if there is at least one tuple, the check above
    const lsn = std.mem.span(lsn_cstr);
    return try allocator.dupe(u8, lsn);
}

/// Check WAL lag for the given replication slot and update metrics. Runs the query `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`.
fn checkWalLag(
    metrics: *metrics_mod.Metrics,
    config: WalConfig,
    allocator: std.mem.Allocator,
) !void {
    // Build connection string
    const conninfo = try config.pg_config.connInfo(allocator, false);
    defer allocator.free(conninfo);

    const conn = c.PQconnectdb(conninfo.ptr);

    defer c.PQfinish(conn);

    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        log.warn("⚠️ WAL monitor connection failed: {s}", .{c.PQerrorMessage(conn)});
        return error.ConnectionFailed;
    }

    // Query replication slot status
    const query_str = try std.fmt.allocPrint(
        allocator,
        \\SELECT
        \\  active,
        \\  COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0) as lag_bytes
        \\FROM pg_replication_slots
        \\WHERE slot_name = '{s}'
    ,
        .{config.slot_name},
    );
    defer allocator.free(query_str);
    const query = try allocator.dupeZ(u8, query_str);
    defer allocator.free(query);

    const result = c.PQexec(conn, query.ptr);
    defer c.PQclear(result);

    if (c.PQresultStatus(result) != c.PGRES_TUPLES_OK) {
        log.warn("🔴 WAL lag query failed: {s}", .{c.PQerrorMessage(conn)});
        return error.QueryFailed;
    }

    const nrows = c.PQntuples(result);
    if (nrows == 0) {
        log.warn(
            "🔴 Replication slot '{s}' not found",
            .{config.slot_name},
        );
        metrics.updateWalLag(
            false,
            0,
        );
        return;
    }

    // Parse result
    const active_str = c.PQgetvalue(result, 0, 0);
    const lag_bytes_str = c.PQgetvalue(result, 0, 1);

    const slot_active = std.mem.eql(
        u8,
        std.mem.span(active_str),
        "t",
    );
    const lag_bytes = try std.fmt.parseInt(
        u64,
        std.mem.span(lag_bytes_str),
        10,
    );

    // Update metrics
    metrics.updateWalLag(
        slot_active,
        lag_bytes,
    );

    // Log warnings for concerning states
    const one_gb: u64 = 1024 * 1024 * 1024;
    if (!slot_active) {
        log.warn("⚠️  Replication slot '{s}' is INACTIVE (lag: {d} MB)", .{
            config.slot_name,
            lag_bytes / (1024 * 1024),
        });
    } else if (lag_bytes > one_gb) {
        log.warn("🔴 CRITICAL: WAL lag exceeds 1GB! ({d} MB) - disk may fill up!", .{
            lag_bytes / (1024 * 1024),
        });
    } else if (lag_bytes > (512 * 1024 * 1024)) { // 512MB warning threshold
        log.warn("⚠️  WAL lag exceeds 512MB ({d} MB)", .{
            lag_bytes / (1024 * 1024),
        });
    } else {
        log.debug("ℹ️ WAL lag: {d} MB (slot active: {s})", .{
            lag_bytes / (1024 * 1024),
            if (slot_active) "yes" else "no",
        });
    }
}
