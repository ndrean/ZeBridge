//! PostgreSQL configuration and connection management.
//!
//! Methods for building connection strings and managing PostgreSQL connections.
const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const Config = @import("config.zig");
const utils = @import("utils.zig");

pub const log = std.log.scoped(.pg_conn);

/// PostgreSQL connection configuration
/// Use RuntimeConfig as the source of truth for connection parameters
pub const PgConf = struct {
    host: []const u8,
    port: u16,
    user: []const u8,
    password: []const u8,
    database: []const u8,
    /// Enable replication mode (adds replication=database to connection string)
    replication: bool = false,

    /// Create PgConf from RuntimeConfig
    /// Does not allocate - just references strings from RuntimeConfig
    pub fn from_runtime_config(runtime_config: *const Config.RuntimeConfig) PgConf {
        return .{
            .host = runtime_config.pg_host,
            .port = runtime_config.pg_port,
            .user = runtime_config.pg_user,
            .password = runtime_config.pg_password,
            .database = runtime_config.pg_database,
            .replication = false,
        };
    }

    /// Build a PostgreSQL connection string
    ///
    /// Caller is responsible for freeing the returned string
    pub fn connInfo(self: *const PgConf, allocator: std.mem.Allocator, replication: bool) ![:0]const u8 {
        // null-terminated [:0]u8 slice suitable for C APIs (e.g. PQconnectdb)
        return if (replication)
            try utils.allocPrintZ(
                allocator,
                "host={s} port={d} user={s} password={s} dbname={s} replication=database",
                .{ self.host, self.port, self.user, self.password, self.database },
            )
        else
            try utils.allocPrintZ(
                allocator,
                "host={s} port={d} user={s} password={s} dbname={s}",
                .{ self.host, self.port, self.user, self.password, self.database },
            );
    }
};

/// Connect to PostgreSQL with the given configuration
///
/// Returns a PGconn pointer that must be closed with PQfinish()
/// Caller is responsible for calling c.PQfinish(conn) when done
pub fn connect(allocator: std.mem.Allocator, pg_conf: PgConf) !*c.PGconn {
    // Build connection string
    const conninfo = try pg_conf.connInfo(
        allocator,
        pg_conf.replication,
    );
    defer allocator.free(conninfo);

    if (conninfo.len == 0) return error.InvalidConfig;

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

    return conn;
}
