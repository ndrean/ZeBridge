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
    db_url: ?[]const u8,
    /// libpq sslmode — always sent explicitly so the transport is a stated decision
    /// rather than whatever `prefer` happens to negotiate. See RuntimeConfig.pg_sslmode.
    sslmode: []const u8 = "disable",
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
            .sslmode = runtime_config.pg_sslmode,
            .db_url = runtime_config.db_url,
            .replication = false,
        };
    }

    /// Build a PostgreSQL connection string
    ///
    /// Caller is responsible for freeing the returned string
    pub fn connInfo(self: *const PgConf, allocator: std.mem.Allocator, replication: bool) ![:0]const u8 {
        if (self.db_url) |url| {
            if (replication) {
                if (std.mem.indexOf(u8, url, "?") != null) {
                    return try utils.allocPrintZ(allocator, "{s}&replication=database", .{url});
                } else {
                    return try utils.allocPrintZ(allocator, "{s}?replication=database", .{url});
                }
            } else {
                return try utils.allocPrintZ(allocator, "{s}", .{url});
            }
        }

        // null-terminated [:0]u8 slice suitable for C APIs (e.g. PQconnectdb)
        return if (replication)
            try utils.allocPrintZ(
                allocator,
                "host={s} port={d} user={s} password={s} dbname={s} sslmode={s} replication=database",
                .{ self.host, self.port, self.user, self.password, self.database, self.sslmode },
            )
        else
            try utils.allocPrintZ(
                allocator,
                "host={s} port={d} user={s} password={s} dbname={s} sslmode={s}",
                .{ self.host, self.port, self.user, self.password, self.database, self.sslmode },
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
