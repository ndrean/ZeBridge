//! `bridge --invite --principal <p> --tenant <t> [--expires <interval>]`: an enrollment
//! invite, printed alone on stdout so a caller can capture it (`code=$(bridge --invite …)`).
//!
//! It calls `public.zebridge_invite()` (init.write.template.sql) over INVITER_DATABASE_URL,
//! passed for the invocation like --revoke's ADMIN_DATABASE_URL, never stored in
//! .env.bridge. The URL's login needs only `zebridge_inviter`: an app's backend, after its
//! own sign-in, runs this with the principal and tenant it chose, and holds no other right
//! on PostgreSQL. The rules (a revoked principal, the lifetime) live in the function, so
//! this command and a backend calling the function directly get the same answers.
const std = @import("std");
const c = @import("c_imports.zig").c;

const out = std.debug.print;

const usage =
    "🔴 usage: INVITER_DATABASE_URL=postgres://… bridge --invite --principal <name> --tenant <tenant> [--expires '1 hour']\n" ++
    "   --expires is a PostgreSQL interval, more than 0 and at most 30 days (default '1 day')\n";

pub fn run(init: *const std.process.Init) u8 {
    var principal: ?[]const u8 = null;
    var tenant: ?[]const u8 = null;
    var expires: []const u8 = "1 day";
    {
        var it = init.minimal.args.iterate();
        _ = it.next();
        while (it.next()) |arg| {
            if (std.mem.eql(u8, arg, "--invite")) {
                continue;
            } else if (std.mem.eql(u8, arg, "--principal")) {
                principal = it.next();
            } else if (std.mem.eql(u8, arg, "--tenant")) {
                tenant = it.next();
            } else if (std.mem.eql(u8, arg, "--expires")) {
                expires = it.next() orelse "";
            } else {
                out("🔴 unknown argument: {s}\n{s}", .{ arg, usage });
                return 1;
            }
        }
    }
    const p = principal orelse {
        out("{s}", .{usage});
        return 1;
    };
    const t = tenant orelse {
        out("{s}", .{usage});
        return 1;
    };

    const url = init.minimal.environ.getPosix("INVITER_DATABASE_URL") orelse {
        out("🔴 INVITER_DATABASE_URL is not set: pass it for this command only, with a login\n" ++
            "   granted zebridge_inviter (GRANT zebridge_inviter TO <login>):\n" ++
            "     INVITER_DATABASE_URL=postgres://… bridge --invite --principal {s} --tenant {s}\n", .{ p, t });
        return 1;
    };

    var url_buf: [1024]u8 = undefined;
    const url_z = std.fmt.bufPrintSentinel(&url_buf, "{s}", .{url}, 0) catch {
        out("🔴 INVITER_DATABASE_URL too long\n", .{});
        return 1;
    };
    const conn = c.PQconnectdb(url_z.ptr);
    defer if (conn != null) c.PQfinish(conn);
    if (conn == null or c.PQstatus(conn) != c.CONNECTION_OK) {
        if (conn != null) {
            out("🔴 could not connect with INVITER_DATABASE_URL: {s}\n", .{c.PQerrorMessage(conn)});
        } else {
            out("🔴 could not connect with INVITER_DATABASE_URL\n", .{});
        }
        return 1;
    }

    var p_buf: [300]u8 = undefined;
    var t_buf: [300]u8 = undefined;
    var e_buf: [100]u8 = undefined;
    const p_z = std.fmt.bufPrintSentinel(&p_buf, "{s}", .{p}, 0) catch {
        out("🔴 principal too long\n", .{});
        return 1;
    };
    const t_z = std.fmt.bufPrintSentinel(&t_buf, "{s}", .{t}, 0) catch {
        out("🔴 tenant too long\n", .{});
        return 1;
    };
    const e_z = std.fmt.bufPrintSentinel(&e_buf, "{s}", .{expires}, 0) catch {
        out("🔴 --expires too long\n", .{});
        return 1;
    };
    const params = [_]?[*:0]const u8{ p_z.ptr, t_z.ptr, e_z.ptr };
    const res = c.PQexecParams(conn, "SELECT public.zebridge_invite($1, $2, $3::interval)", 3, null, &params[0], null, null, 0);
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK or c.PQntuples(res) != 1) {
        out("🔴 no invite: {s}\n", .{explain(c.PQresultErrorField(res, c.PG_DIAG_SQLSTATE), c.PQresultErrorField(res, c.PG_DIAG_MESSAGE_PRIMARY))});
        return 1;
    }

    var line_buf: [128]u8 = undefined;
    const line = std.fmt.bufPrint(&line_buf, "{s}\n", .{std.mem.span(c.PQgetvalue(res, 0, 0))}) catch return 1;
    std.Io.File.stdout().writeStreamingAll(init.io, line) catch return 1;
    return 0;
}

/// PostgreSQL's words, with the two answers a caller can act on said plainly first.
fn explain(state: [*c]const u8, msg: [*c]const u8) []const u8 {
    const s: []const u8 = if (state != null) std.mem.span(state) else "";
    const m: []const u8 = if (msg != null) std.mem.span(msg) else "unknown error";
    if (std.mem.eql(u8, s, "42883")) return "zebridge_invite() does not exist: apply the init SQL (bridge --init-sql)";
    if (std.mem.eql(u8, s, "42501") and std.mem.indexOf(u8, m, "revoked") == null)
        return "this login may not create invites: GRANT zebridge_inviter TO <login>";
    if (std.mem.eql(u8, s, "23514")) return "the principal is letters, digits, '_' and '-'; the tenant has no '.', '*', '>' or space";
    return m;
}
