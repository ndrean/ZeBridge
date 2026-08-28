//! PostgreSQL configuration and connection management.
//!
//! Methods for building connection strings and managing PostgreSQL connections.
const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const Config = @import("config.zig");
const utils = @import("utils.zig");

pub const log = std.log.scoped(.pg_conn);

/// How the bridge connects to PostgreSQL: a URL, and nothing else.
///
/// It used to carry `host`/`port`/`user`/`password`/`database`/`sslmode` as well, filled
/// from `PG_HOST`/`PG_USER`/`PG_PASSWORD`/… — which are the **superuser** credentials
/// `bridge-init` interpolates into `init.sql` to create roles. They sat in the same
/// environment as everything else, so a bridge launched with `DATABASE_READER_URL` unset, or
/// misspelled, fell back to connecting as `postgres` and looked entirely healthy doing
/// it. Convenient, and exactly the wrong thing to be convenient about: the read role is
/// deliberately unable to write, and that guarantee is worth nothing if the process can
/// silently connect as someone else.
///
/// So there is no fallback. `DATABASE_READER_URL` is required (see `args.zig`), the ingress path
/// needs its own `DATABASE_WRITER_URL`, and neither can be assembled out of parts.
pub const PgConf = struct {
    /// `postgres://user:pass@host:port/db[?sslmode=…]`. Credentials included — this is
    /// the whole connection, not a template. `sslmode` rides in the query string, so it
    /// stays an explicit decision rather than whatever libpq's `prefer` negotiates.
    url: []const u8,
    /// Enable replication mode (adds replication=database to connection string)
    replication: bool = false,
    /// Which role this is, for logs only. Never used to connect — the URL carries the
    /// real credentials — but it keeps a writer connection from being announced under
    /// the reader's name.
    role: []const u8 = "(from DATABASE_READER_URL)",

    /// The ingress connection, or null when none is configured.
    ///
    /// Null must be read as "do not start the mutation listener", never as "use the read
    /// role instead": that role holds REPLICATION, which is precisely what must not be
    /// executing client-driven writes.
    ///
    /// ⚠️ The read path's URL is **never** inherited. It embeds its own credentials, so
    /// falling back to it would silently reconnect as the read role and undo the split,
    /// with logs that look healthy.
    pub fn writer_from_runtime_config(runtime_config: *const Config.RuntimeConfig) ?PgConf {
        const url = runtime_config.pg_writer_url orelse return null;
        return .{ .url = url, .replication = false, .role = "(from DATABASE_WRITER_URL)" };
    }

    /// The read/replication connection.
    /// Does not allocate — just references strings from RuntimeConfig.
    pub fn from_runtime_config(runtime_config: *const Config.RuntimeConfig) PgConf {
        return .{
            .url = runtime_config.db_url,
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
        const url = self.url;

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

fn testConf(url: []const u8) PgConf {
    return .{ .url = url };
}

test "writer_from_runtime_config: no writer URL means no ingress, never the read role" {
    var rc = Config.RuntimeConfig.defaults();
    rc.db_url = "postgres://reader:p@h/d";
    rc.pg_writer_url = null;

    // The read URL must not leak in as a fallback: it carries REPLICATION rights.
    try testing.expect(PgConf.writer_from_runtime_config(&rc) == null);

    rc.pg_writer_url = "postgres://writer:w@h/d";
    const writer = PgConf.writer_from_runtime_config(&rc).?;
    try testing.expectEqualStrings("postgres://writer:w@h/d", writer.url);
    try testing.expect(!writer.replication);
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
