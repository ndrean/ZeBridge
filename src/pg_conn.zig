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

    /// The ingress connection description: the same host/database, a different role.
    ///
    /// Returns null when no writer is configured, which the caller must treat as "do not
    /// start the mutation listener" — never as "use the read role instead".
    ///
    /// Two ways to configure it, mirroring the read path:
    ///
    /// - `DATABASE_WRITER_URL` — passed through natively by `connInfo`, so nothing is
    ///   rebuilt from parts;
    /// - `POSTGRES_WRITER_USER` + `_PASSWORD` — host and database inherited from the
    ///   read config, role swapped.
    ///
    /// ⚠️ The read path's `db_url` is **never** inherited. `DATABASE_URL` embeds its own
    /// credentials, so carrying it here would silently reconnect as the read role and
    /// undo the split — with logs that look entirely healthy.
    pub fn writer_from_runtime_config(runtime_config: *const Config.RuntimeConfig) ?PgConf {
        var conf = from_runtime_config(runtime_config);
        conf.db_url = null;
        conf.replication = false;

        if (runtime_config.pg_writer_url) |url| {
            conf.db_url = url;
            // Cosmetic only — the URL carries the real credentials — but it keeps logs
            // and error messages from naming the read role on a writer connection.
            conf.user = runtime_config.pg_writer_user orelse "(from DATABASE_WRITER_URL)";
            return conf;
        }

        conf.user = runtime_config.pg_writer_user orelse return null;
        conf.password = runtime_config.pg_writer_password orelse return null;
        return conf;
    }

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

    /// TCP keepalives, in libpq's two spellings. See Config.Postgres for why.
    const keepalives_kw = std.fmt.comptimePrint(
        " keepalives=1 keepalives_idle={d} keepalives_interval={d} keepalives_count={d}",
        .{ Config.Postgres.tcp_keepalives_idle_s, Config.Postgres.tcp_keepalives_interval_s, Config.Postgres.tcp_keepalives_count },
    );
    const keepalives_uri = std.fmt.comptimePrint(
        "keepalives=1&keepalives_idle={d}&keepalives_interval={d}&keepalives_count={d}",
        .{ Config.Postgres.tcp_keepalives_idle_s, Config.Postgres.tcp_keepalives_interval_s, Config.Postgres.tcp_keepalives_count },
    );

    /// Build a PostgreSQL connection string
    ///
    /// Caller is responsible for freeing the returned string
    pub fn connInfo(self: *const PgConf, allocator: std.mem.Allocator, replication: bool) ![:0]const u8 {
        if (self.db_url) |url| {
            // A URL that already mentions keepalives was set deliberately; do not
            // second-guess it, and do not append a duplicate parameter.
            const ka: []const u8 = if (std.mem.indexOf(u8, url, "keepalives") != null) "" else keepalives_uri;
            const sep: []const u8 = if (ka.len == 0)
                ""
            else if (std.mem.indexOfScalar(u8, url, '?') != null) "&" else "?";

            if (replication) {
                // Whatever came before, there is now a query string to extend.
                const rep_sep: []const u8 = if (ka.len > 0 or std.mem.indexOfScalar(u8, url, '?') != null) "&" else "?";
                return try utils.allocPrintZ(allocator, "{s}{s}{s}{s}replication=database", .{ url, sep, ka, rep_sep });
            }
            return try utils.allocPrintZ(allocator, "{s}{s}{s}", .{ url, sep, ka });
        }

        // null-terminated [:0]u8 slice suitable for C APIs (e.g. PQconnectdb)
        return if (replication)
            try utils.allocPrintZ(
                allocator,
                "host={s} port={d} user={s} password={s} dbname={s} sslmode={s} replication=database" ++ keepalives_kw,
                .{ self.host, self.port, self.user, self.password, self.database, self.sslmode },
            )
        else
            try utils.allocPrintZ(
                allocator,
                "host={s} port={d} user={s} password={s} dbname={s} sslmode={s}" ++ keepalives_kw,
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

// ---------------------------------------------------------------------------
// Tests — connInfo is pure string building, and every branch of it is a place a
// connection can silently fail to carry the settings it was supposed to.
// ---------------------------------------------------------------------------

const testing = std.testing;

fn testConf(db_url: ?[]const u8) PgConf {
    return .{
        .host = "h", .port = 5432, .user = "u", .password = "p",
        .database = "d", .sslmode = "disable", .db_url = db_url,
    };
}

test "connInfo - keyword form carries keepalives, and replication when asked" {
    const alloc = testing.allocator;
    const conf = testConf(null);

    const plain = try conf.connInfo(alloc, false);
    defer alloc.free(plain);
    try testing.expect(std.mem.indexOf(u8, plain, "keepalives=1") != null);
    try testing.expect(std.mem.indexOf(u8, plain, "replication=database") == null);

    const rep = try conf.connInfo(alloc, true);
    defer alloc.free(rep);
    try testing.expect(std.mem.indexOf(u8, rep, "keepalives_idle=30") != null);
    try testing.expect(std.mem.indexOf(u8, rep, "replication=database") != null);
}

test "connInfo - a URL with no query gets '?', one with a query gets '&'" {
    const alloc = testing.allocator;

    const bare = try testConf("postgres://u:p@h/d").connInfo(alloc, false);
    defer alloc.free(bare);
    try testing.expectEqualStrings(
        "postgres://u:p@h/d?keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=3",
        bare,
    );

    const with_query = try testConf("postgres://u:p@h/d?sslmode=require").connInfo(alloc, false);
    defer alloc.free(with_query);
    try testing.expect(std.mem.indexOf(u8, with_query, "?sslmode=require&keepalives=1") != null);
}

test "connInfo - replication is appended after the keepalives, still one query string" {
    const alloc = testing.allocator;
    const rep = try testConf("postgres://u:p@h/d").connInfo(alloc, true);
    defer alloc.free(rep);
    try testing.expectEqualStrings(
        "postgres://u:p@h/d?keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=3&replication=database",
        rep,
    );
    // Exactly one '?' — a second would make libpq reject the URI.
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, rep, "?"));
}

test "connInfo - a URL that already sets keepalives is left alone" {
    // Deliberate operator choice; appending a duplicate would silently override it.
    const alloc = testing.allocator;
    const url = "postgres://u:p@h/d?keepalives_idle=5";

    const plain = try testConf(url).connInfo(alloc, false);
    defer alloc.free(plain);
    try testing.expectEqualStrings(url, plain);

    const rep = try testConf(url).connInfo(alloc, true);
    defer alloc.free(rep);
    try testing.expectEqualStrings(url ++ "&replication=database", rep);
}
