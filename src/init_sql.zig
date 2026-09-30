//! `bridge --init-sql` (§10ke): the init SQL, rendered, on stdout.
//!
//!     set -a; . /etc/zebridge/.env.bridge; set +a
//!     bridge --init-sql | psql "$ADMIN_DATABASE_URL" -v ON_ERROR_STOP=1
//!
//! It replaces `zb-derive-env.py` + `envsubst` + a checkout of the repository: the two
//! templates and the grammar are inside the binary, so the SQL always matches the bridge
//! that will run on it. The values come from where they already live:
//!   role names and passwords  ← DATABASE_READER_URL / DATABASE_WRITER_URL
//!   TARGET_DB                 ← the reader URL's database (TARGET_DB overrides)
//!   OPEN_TENANT               ← the embedded grammar
//! No DATABASE_WRITER_URL: the read-only profile (init.core only), as the bridge itself
//! runs without ingress. BRIDGE_CDC_PUBLICATION set: `zebridge_create_publication` is
//! appended, so one pipe does the whole of step 3.
//!
//! Stricter than envsubst on purpose: a role or database name must be a plain
//! identifier (it is written bare into CREATE USER / GRANT), a password has its quotes
//! doubled (it is written inside '…'), and a `${…}` left over is an error, not an
//! empty string — `CREATE USER  WITH PASSWORD ''` was envsubst's failure mode.
const std = @import("std");
const topology_mod = @import("topology.zig");

const core_template = @embedFile("init_core_sql");
const write_template = @embedFile("init_write_sql");

const out = std.debug.print;

pub fn run(io: std.Io, init: *const std.process.Init) u8 {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const env = init.minimal.environ;

    {
        var it = init.minimal.args.iterate();
        _ = it.next();
        while (it.next()) |arg| {
            if (std.mem.eql(u8, arg, "--init-sql")) continue;
            out("🔴 unknown argument '{s}'\n  bridge --init-sql   (reads DATABASE_READER_URL, DATABASE_WRITER_URL, BRIDGE_CDC_PUBLICATION)\n", .{arg});
            return 1;
        }
    }

    const reader_url = env.getPosix("DATABASE_READER_URL") orelse {
        out("🔴 DATABASE_READER_URL is not set — load .env.bridge first: set -a; . .env.bridge; set +a\n", .{});
        return 1;
    };
    const reader = parseUrl(a, reader_url) catch |err| return urlError("DATABASE_READER_URL", err);
    const writer_url = env.getPosix("DATABASE_WRITER_URL");
    const writer: ?Parsed = if (writer_url) |u| (parseUrl(a, u) catch |err| return urlError("DATABASE_WRITER_URL", err)) else null;
    const target_db = env.getPosix("TARGET_DB") orelse reader.db;
    if (!isIdent(target_db)) {
        out("🔴 the database name '{s}' must be a plain identifier (letters, digits, '_')\n", .{target_db});
        return 1;
    }

    const owned = topology_mod.loadEmbedded(a) catch {
        out("🔴 the embedded grammar did not load\n", .{});
        return 1;
    };
    const open_tenant = owned.topology.open_tenant;

    var vars = [_]Var{
        .{ .name = "POSTGRES_READER_USER", .value = reader.user },
        .{ .name = "POSTGRES_READER_PASSWORD", .value = quoteDoubled(a, reader.password) catch return 1 },
        .{ .name = "POSTGRES_WRITER_USER", .value = if (writer) |w| w.user else "" },
        .{ .name = "POSTGRES_WRITER_PASSWORD", .value = if (writer) |w| (quoteDoubled(a, w.password) catch return 1) else "" },
        .{ .name = "TARGET_DB", .value = target_db },
        .{ .name = "OPEN_TENANT", .value = open_tenant },
    };

    var sql: std.ArrayList(u8) = .empty;
    render(a, &sql, core_template, &vars) catch |err| return renderError("init.core", err);
    if (writer != null) {
        render(a, &sql, write_template, &vars) catch |err| return renderError("init.write", err);
    }
    if (env.getPosix("BRIDGE_CDC_PUBLICATION")) |pub_name| {
        if (!isIdent(pub_name)) {
            out("🔴 BRIDGE_CDC_PUBLICATION '{s}' must be a plain identifier\n", .{pub_name});
            return 1;
        }
        sql.print(a, "\nSELECT * FROM public.zebridge_create_publication('{s}');\n", .{pub_name}) catch return 1;
    }

    std.Io.File.stdout().writeStreamingAll(io, sql.items) catch return 1;
    out("✅ init SQL, {s}: database '{s}', reader '{s}'", .{ if (writer != null) "read/write" else "read-only (no DATABASE_WRITER_URL)", target_db, reader.user });
    if (writer) |w| out(", writer '{s}'", .{w.user});
    if (env.getPosix("BRIDGE_CDC_PUBLICATION")) |p| out(", publication '{s}'\n", .{p}) else out(", no publication (BRIDGE_CDC_PUBLICATION unset)\n", .{});
    return 0;
}

const Var = struct { name: []const u8, value: []const u8 };

const Parsed = struct { user: []const u8, password: []const u8, db: []const u8 };

/// postgres://user:pass@host:port/db?opts → its parts, percent-decoded — the same
/// split zb-derive-env.py made: the LAST '@' ends the credentials, the FIRST ':' in
/// them ends the user, as libpq reads it.
fn parseUrl(a: std.mem.Allocator, url: []const u8) !Parsed {
    const scheme_end = std.mem.indexOf(u8, url, "://") orelse return error.NotAUrl;
    const rest = url[scheme_end + 3 ..];
    const at = std.mem.lastIndexOfScalar(u8, rest, '@') orelse return error.NoCredentials;
    const creds = rest[0..at];
    const colon = std.mem.indexOfScalar(u8, creds, ':');
    const user = try percentDecode(a, if (colon) |c| creds[0..c] else creds);
    const password = try percentDecode(a, if (colon) |c| creds[c + 1 ..] else "");
    const hostpart = rest[at + 1 ..];
    const slash = std.mem.indexOfScalar(u8, hostpart, '/') orelse return error.NoDatabase;
    var db = hostpart[slash + 1 ..];
    if (std.mem.indexOfScalar(u8, db, '?')) |q| db = db[0..q];
    if (db.len == 0) return error.NoDatabase;
    if (!isIdent(user)) return error.BadRoleName;
    return .{ .user = user, .password = password, .db = try percentDecode(a, db) };
}

fn percentDecode(a: std.mem.Allocator, s: []const u8) ![]const u8 {
    var buf: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        if (s[i] == '%' and i + 2 < s.len) {
            const byte = std.fmt.parseInt(u8, s[i + 1 .. i + 3], 16) catch return error.BadPercentEncoding;
            try buf.append(a, byte);
            i += 2;
        } else try buf.append(a, s[i]);
    }
    return buf.items;
}

/// A name written bare into SQL: a role, a database, a publication.
fn isIdent(s: []const u8) bool {
    if (s.len == 0 or s.len > 63) return false;
    if (!(std.ascii.isAlphabetic(s[0]) or s[0] == '_')) return false;
    for (s) |ch| if (!(std.ascii.isAlphanumeric(ch) or ch == '_')) return false;
    return true;
}

/// A value written inside '…': every ' doubled, as SQL spells it.
fn quoteDoubled(a: std.mem.Allocator, s: []const u8) ![]const u8 {
    return std.mem.replaceOwned(u8, a, s, "'", "''");
}

/// `${NAME}` → its value, for the six names the templates use. Anything else in that
/// shape is an error: the templates have exactly these, and a new one must be added here.
fn render(a: std.mem.Allocator, sql: *std.ArrayList(u8), template: []const u8, vars: []const Var) !void {
    var i: usize = 0;
    while (std.mem.indexOfPos(u8, template, i, "${")) |start| {
        try sql.appendSlice(a, template[i..start]);
        const close = std.mem.indexOfScalarPos(u8, template, start, '}') orelse return error.UnclosedVariable;
        const name = template[start + 2 .. close];
        const v = for (vars) |v| {
            if (std.mem.eql(u8, v.name, name)) break v;
        } else {
            out("🔴 the template names ${{{s}}}, which --init-sql does not know\n", .{name});
            return error.UnknownVariable;
        };
        if (v.value.len == 0) {
            // A comment may name a variable this profile leaves empty (init.core says
            // "Nothing here may reference ${POSTGRES_WRITER_USER}"): keep it as written.
            // In SQL an empty value is an error, never a silent blank.
            if (inLineComment(template, start)) {
                try sql.appendSlice(a, template[start .. close + 1]);
                i = close + 1;
                continue;
            }
            out("🔴 ${{{s}}} has no value\n", .{name});
            return error.EmptyVariable;
        }
        try sql.appendSlice(a, v.value);
        i = close + 1;
    }
    try sql.appendSlice(a, template[i..]);
}

/// Whether `pos` sits after a `--` on its own line (outside a string, near enough for
/// these templates).
fn inLineComment(text: []const u8, pos: usize) bool {
    const line_start = if (std.mem.lastIndexOfScalar(u8, text[0..pos], '\n')) |n| n + 1 else 0;
    return std.mem.indexOf(u8, text[line_start..pos], "--") != null;
}

fn urlError(which: []const u8, err: anyerror) u8 {
    out("🔴 {s}: {s} — expected postgres://role:password@host:port/database\n", .{ which, switch (err) {
        error.NotAUrl => "not a URL",
        error.NoCredentials => "no role and password",
        error.NoDatabase => "no database name",
        error.BadRoleName => "the role name must be a plain identifier (letters, digits, '_')",
        error.BadPercentEncoding => "a bad %-escape",
        else => @errorName(err),
    } });
    return 1;
}

fn renderError(which: []const u8, err: anyerror) u8 {
    out("🔴 {s} template: {s}\n", .{ which, @errorName(err) });
    return 1;
}

test "parseUrl splits and decodes like libpq" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const p = try parseUrl(arena.allocator(), "postgres://bridge_reader:p%40ss:w@db.example.com:5432/app?sslmode=verify-full");
    try std.testing.expectEqualStrings("bridge_reader", p.user);
    try std.testing.expectEqualStrings("p@ss:w", p.password);
    try std.testing.expectEqualStrings("app", p.db);
    try std.testing.expectError(error.BadRoleName, parseUrl(arena.allocator(), "postgres://bad-name:x@h/app"));
    try std.testing.expectError(error.NoDatabase, parseUrl(arena.allocator(), "postgres://r:x@h:5432/"));
}

test "render substitutes, quotes passwords, and refuses a leftover" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const vars = [_]Var{
        .{ .name = "POSTGRES_READER_USER", .value = "r" },
        .{ .name = "POSTGRES_READER_PASSWORD", .value = try quoteDoubled(aa, "it's") },
    };
    var sql: std.ArrayList(u8) = .empty;
    try render(aa, &sql, "CREATE USER ${POSTGRES_READER_USER} WITH PASSWORD '${POSTGRES_READER_PASSWORD}';", &vars);
    try std.testing.expectEqualStrings("CREATE USER r WITH PASSWORD 'it''s';", sql.items);
    var bad: std.ArrayList(u8) = .empty;
    try std.testing.expectError(error.UnknownVariable, render(aa, &bad, "x ${NOPE} y", &vars));
    // An empty value: kept in a comment, refused in SQL.
    const empty = [_]Var{.{ .name = "POSTGRES_WRITER_USER", .value = "" }};
    var c: std.ArrayList(u8) = .empty;
    try render(aa, &c, "-- never ${POSTGRES_WRITER_USER}\nSELECT 1;", &empty);
    try std.testing.expectEqualStrings("-- never ${POSTGRES_WRITER_USER}\nSELECT 1;", c.items);
    var d: std.ArrayList(u8) = .empty;
    try std.testing.expectError(error.EmptyVariable, render(aa, &d, "GRANT x TO ${POSTGRES_WRITER_USER};", &empty));
}

test "both embedded templates name only the six known variables" {
    const known = [_][]const u8{ "POSTGRES_READER_USER", "POSTGRES_READER_PASSWORD", "POSTGRES_WRITER_USER", "POSTGRES_WRITER_PASSWORD", "TARGET_DB", "OPEN_TENANT" };
    for ([_][]const u8{ core_template, write_template }) |t| {
        var i: usize = 0;
        while (std.mem.indexOfPos(u8, t, i, "${")) |start| {
            const close = std.mem.indexOfScalarPos(u8, t, start, '}').?;
            const name = t[start + 2 .. close];
            var found = false;
            for (known) |k| if (std.mem.eql(u8, k, name)) {
                found = true;
            };
            try std.testing.expect(found);
            i = close + 1;
        }
    }
}
