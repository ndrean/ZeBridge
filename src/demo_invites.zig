//! `bridge --demo-invites --redirect URL [--tenant demo] [--expires '1 hour'] [--listen 127.0.0.1:27435]`
//!
//! A public demo's front door, as its own small process: `GET /demo` creates a one-time
//! invite for a new principal (`demo-<random>`) in the demo tenant and redirects to
//! `URL?invite=<code>`, which the demo page redeems at `/enroll` like any invite. The
//! visitor's browser then keeps its identity and renews it on its own.
//!
//! It holds one PostgreSQL connection, as a login granted `zebridge_inviter`
//! (INVITER_DATABASE_URL): it can create invites and nothing else. It listens on loopback;
//! a proxy in front (HAProxy) is the public side and limits the rate per visitor.
//! `GET /health` answers 200 for the proxy's check.
//!
//! Separate from the bridge on purpose: its own credentials, its own unit, and a fault
//! here never reaches replication. About 20 MB of memory.
const std = @import("std");
const http_respond = @import("http_respond.zig");
const c = @import("c_imports.zig").c;
const admin_invite = @import("admin_invite.zig");

const out = std.debug.print;

const usage =
    "🔴 usage: INVITER_DATABASE_URL=postgres://… bridge --demo-invites --redirect https://demo.example.com/\n" ++
    "          [--tenant demo] [--expires '1 hour'] [--listen 127.0.0.1:27435] [--prefix demo]\n";

/// Connections served at once; more are closed at accept. A demo behind a rate limit
/// never needs many.
const max_connections: u32 = 16;
/// A client that connects and sends no request in time is cut off.
const receive_timeout_ns: u64 = 3 * std.time.ns_per_s;

const Demo = struct {
    io: std.Io,
    conn: *c.PGconn,
    /// One connection, shared by concurrent requests: libpq connections are not
    /// thread-safe, so every query holds this.
    db_lock: std.Io.Mutex = .{ .state = std.atomic.Value(std.Io.Mutex.State).init(.unlocked) },
    open_conns: std.atomic.Value(u32) = .init(0),
    tenant: []const u8,
    expires: []const u8,
    redirect: []const u8,
    prefix: []const u8,
};

pub fn run(init: *const std.process.Init) u8 {
    var redirect: ?[]const u8 = null;
    var tenant: []const u8 = "demo";
    var expires: []const u8 = "1 hour";
    var listen: []const u8 = "127.0.0.1:27435";
    var prefix: []const u8 = "demo";
    {
        var it = init.minimal.args.iterate();
        _ = it.next();
        while (it.next()) |arg| {
            if (std.mem.eql(u8, arg, "--demo-invites")) continue;
            const value = it.next() orelse {
                out("🔴 {s} needs a value\n{s}", .{ arg, usage });
                return 1;
            };
            if (std.mem.eql(u8, arg, "--redirect")) {
                redirect = value;
            } else if (std.mem.eql(u8, arg, "--tenant")) {
                tenant = value;
            } else if (std.mem.eql(u8, arg, "--expires")) {
                expires = value;
            } else if (std.mem.eql(u8, arg, "--listen")) {
                listen = value;
            } else if (std.mem.eql(u8, arg, "--prefix")) {
                prefix = value;
            } else {
                out("🔴 unknown argument: {s}\n{s}", .{ arg, usage });
                return 1;
            }
        }
    }
    const target = redirect orelse {
        out("{s}", .{usage});
        return 1;
    };
    if (!std.mem.startsWith(u8, target, "https://") and !std.mem.startsWith(u8, target, "http://")) {
        out("🔴 --redirect is a URL (https://…): {s}\n", .{target});
        return 1;
    }
    const colon = std.mem.lastIndexOfScalar(u8, listen, ':') orelse {
        out("🔴 --listen is ADDRESS:PORT, e.g. 127.0.0.1:27435\n", .{});
        return 1;
    };
    const port = std.fmt.parseInt(u16, listen[colon + 1 ..], 10) catch {
        out("🔴 --listen port is not a number: {s}\n", .{listen});
        return 1;
    };

    const url = init.minimal.environ.getPosix("INVITER_DATABASE_URL") orelse {
        out("🔴 INVITER_DATABASE_URL is not set: a login granted zebridge_inviter (GRANT zebridge_inviter TO <login>)\n", .{});
        return 1;
    };
    var url_buf: [1024]u8 = undefined;
    const url_z = std.fmt.bufPrintSentinel(&url_buf, "{s}", .{url}, 0) catch {
        out("🔴 INVITER_DATABASE_URL too long\n", .{});
        return 1;
    };
    const conn = c.PQconnectdb(url_z.ptr) orelse {
        out("🔴 could not connect with INVITER_DATABASE_URL\n", .{});
        return 1;
    };
    defer c.PQfinish(conn);
    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        out("🔴 could not connect with INVITER_DATABASE_URL: {s}\n", .{c.PQerrorMessage(conn)});
        return 1;
    }

    var demo: Demo = .{
        .io = init.io,
        .conn = conn,
        .tenant = tenant,
        .expires = expires,
        .redirect = target,
        .prefix = prefix,
    };

    const addr = std.Io.net.IpAddress.parse(listen[0..colon], port) catch {
        out("🔴 --listen address is not an IP address: {s}\n", .{listen[0..colon]});
        return 1;
    };
    var server = addr.listen(init.io, .{ .reuse_address = true }) catch |err| {
        out("🔴 could not listen on {s}: {s}\n", .{ listen, @errorName(err) });
        return 1;
    };
    defer server.deinit(init.io);
    out("🎟️ demo invites on http://{s}/demo → {s}?invite=… (tenant '{s}', {s})\n", .{ listen, target, tenant, expires });

    var conns: std.Io.Group = .init;
    defer conns.cancel(init.io);
    while (true) {
        const stream = server.accept(init.io) catch |err| switch (err) {
            error.SocketNotListening, error.Canceled => break,
            error.ConnectionAborted, error.WouldBlock, error.ProcessFdQuotaExceeded, error.SystemFdQuotaExceeded, error.SystemResources => continue,
            else => {
                out("🔴 accept failed: {s}\n", .{@errorName(err)});
                return 1;
            },
        };
        if (demo.open_conns.load(.monotonic) >= max_connections) {
            stream.close(init.io);
            continue;
        }
        _ = demo.open_conns.fetchAdd(1, .monotonic);
        // `concurrent`, as in http_server.zig: a client that sends nothing must not hold
        // the accept loop.
        conns.concurrent(init.io, serve, .{ &demo, stream }) catch serve(&demo, stream);
    }
    return 0;
}

fn serve(d: *Demo, stream: std.Io.net.Stream) void {
    defer stream.close(d.io);
    defer _ = d.open_conns.fetchSub(1, .monotonic);

    var done = std.atomic.Value(bool).init(false);
    var watchdog: std.Io.Group = .init;
    defer watchdog.cancel(d.io);
    watchdog.concurrent(d.io, cutSilent, .{ d, stream, &done }) catch {};

    var read_buffer: [2048]u8 = undefined;
    var write_buffer: [1024]u8 = undefined;
    var reader = stream.reader(d.io, &read_buffer);
    var writer = stream.writer(d.io, &write_buffer);
    var http = std.http.Server.init(&reader.interface, &writer.interface);
    var req = http.receiveHead() catch {
        done.store(true, .release);
        return;
    };
    done.store(true, .release);
    handle(d, &req) catch {};
}

fn cutSilent(d: *Demo, stream: std.Io.net.Stream, done: *std.atomic.Value(bool)) void {
    std.Io.Timeout.sleep(.{
        .duration = .{
            .raw = .fromNanoseconds(receive_timeout_ns),
            .clock = .awake,
        },
    }, d.io) catch return;
    if (!done.load(.acquire)) stream.shutdown(d.io, .recv) catch {};
}

const no_store: std.http.Header = .{ .name = "cache-control", .value = "no-store" };

fn handle(d: *Demo, req: *std.http.Server.Request) !void {
    const target = req.head.target;
    if (req.head.method != .GET) return http_respond.respond(req, "Method Not Allowed\n", .{ .status = .method_not_allowed });
    if (std.mem.eql(u8, target, "/health")) return http_respond.respond(req, "ok\n", .{});
    const path = if (std.mem.indexOfScalar(u8, target, '?')) |q| target[0..q] else target;
    if (!std.mem.eql(u8, path, "/demo")) return http_respond.respond(req, "Not Found\n", .{ .status = .not_found });

    // A principal of its own per visitor: letters, digits and '-', as the invites table wants.
    var rnd: [6]u8 = undefined;
    d.io.random(&rnd);
    var p_buf: [80]u8 = undefined;
    const principal = std.fmt.bufPrint(&p_buf, "{s}-{x}", .{ d.prefix, rnd }) catch return http_respond.respond(req, "Internal Server Error\n", .{ .status = .internal_server_error });

    var code_buf: [256]u8 = undefined;
    const result = blk: {
        d.db_lock.lockUncancelable(d.io);
        defer d.db_lock.unlock(d.io);
        // A connection the server dropped (a restart, an idle timeout) is reopened once.
        if (c.PQstatus(d.conn) != c.CONNECTION_OK) c.PQreset(d.conn);
        break :blk admin_invite.create(d.conn, principal, d.tenant, d.expires, &code_buf);
    };
    switch (result) {
        .refused => |why| {
            out("⚠️ no demo invite: {s}\n", .{why});
            return http_respond.respond(req, "The demo cannot hand out an invite right now. Please try again in a minute.\n", .{
                .status = .service_unavailable,
                .extra_headers = &.{no_store},
            });
        },
        .code => |code| {
            var loc_buf: [1024]u8 = undefined;
            const sep: []const u8 = if (std.mem.indexOfScalar(u8, d.redirect, '?') != null) "&" else "?";
            const location = std.fmt.bufPrint(&loc_buf, "{s}{s}invite={s}", .{ d.redirect, sep, code }) catch return http_respond.respond(req, "Internal Server Error\n", .{ .status = .internal_server_error });
            out("🎟️ demo invite for {s}\n", .{principal});
            return http_respond.respond(req, "", .{
                .status = .found,
                .extra_headers = &.{ .{ .name = "location", .value = location }, no_store },
            });
        },
    }
}
