//! `zb sync` — the replica as a command (§10fl): connect, seed the tables from the
//! chain, drain the change feed to the tail, and either exit (`--once`, the micro-VM
//! worker: boot, have the data, run the job, be gone) or keep following.
//!
//!   zb sync --url nats://host:4222 --creds bob.creds --principal bob \
//!           --tables test_types,orders --db /data/replica.duckdb --engine duckdb --once
//!
//! Options: --engine sqlite (default) | duckdb; --db-url postgres://… instead of --db
//! (the PostgreSQL engine); --stream (the streaming seed, §10fh); --max-wait-s N
//! (how long --once waits for a chain the producer has not cut yet, default 600);
//! --poll-ms N (the follower's turn, default 500). The summary on exit is one JSON
//! line: the tenant, the tables and their row counts, the seconds it took.
//!
//! No Python, no nats CLI: every step is libzb's own, the same the C card takes.

const std = @import("std");
const client = @import("client.zig");
const Value = std.json.Value;

const Args = struct {
    url: []const u8 = "nats://127.0.0.1:4222",
    creds: []const u8 = "",
    principal: []const u8 = "",
    tables: []const u8 = "",
    db: [:0]const u8 = "", // default zebridge_<principal>.sqlite3, as both clients
    db_url: ?[:0]const u8 = null,
    engine: client.storage.Engine = .sqlite,
    once: bool = false,
    stream: bool = false,
    max_wait_s: u64 = 600,
    poll_ms: u64 = 500,
    client_id: []const u8 = "zb-cli",
    /// The JetStream domain, when the deployment reaches JetStream across a leaf link.
    js_domain: ?[]const u8 = null,
};

fn usage() void {
    std.debug.print(
        \\usage: zb sync --creds <file> --principal <name> --tables <a,b> [--db <path>] [--engine sqlite|duckdb]
        \\               [--db-url postgres://…] [--url nats://…] [--js-domain NAME] [--once] [--stream] [--max-wait-s N] [--poll-ms N]
        \\
    , .{});
}

pub fn main(init: std.process.Init) !u8 {
    const a = std.heap.c_allocator;
    // Zig 0.16: argv comes through std.process.Init; the strings outlive the process.
    var argv_list: std.ArrayListUnmanaged([]const u8) = .empty;
    var ait = init.minimal.args.iterate();
    while (ait.next()) |arg| try argv_list.append(a, arg);
    const argv = argv_list.items;
    if (argv.len < 2 or !std.mem.eql(u8, argv[1], "sync")) {
        usage();
        return 2;
    }
    var args = Args{};
    var i: usize = 2;
    while (i < argv.len) : (i += 1) {
        const arg = argv[i];
        const next = if (i + 1 < argv.len) argv[i + 1] else null;
        if (std.mem.eql(u8, arg, "--once")) {
            args.once = true;
        } else if (std.mem.eql(u8, arg, "--stream")) {
            args.stream = true;
        } else if (next == null) {
            usage();
            return 2;
        } else {
            i += 1;
            const v = next.?;
            if (std.mem.eql(u8, arg, "--url")) args.url = v else if (std.mem.eql(u8, arg, "--creds")) args.creds = v else if (std.mem.eql(u8, arg, "--principal")) args.principal = v else if (std.mem.eql(u8, arg, "--tables")) args.tables = v else if (std.mem.eql(u8, arg, "--db")) args.db = try a.dupeZ(u8, v) else if (std.mem.eql(u8, arg, "--db-url")) args.db_url = try a.dupeZ(u8, v) else if (std.mem.eql(u8, arg, "--client-id")) args.client_id = v else if (std.mem.eql(u8, arg, "--js-domain")) args.js_domain = v else if (std.mem.eql(u8, arg, "--engine")) {
                args.engine = if (std.mem.eql(u8, v, "duckdb")) .duckdb else if (std.mem.eql(u8, v, "sqlite")) .sqlite else {
                    std.debug.print("--engine: sqlite or duckdb (postgres is --db-url)\n", .{});
                    return 2;
                };
            } else if (std.mem.eql(u8, arg, "--max-wait-s")) args.max_wait_s = try std.fmt.parseInt(u64, v, 10) else if (std.mem.eql(u8, arg, "--poll-ms")) args.poll_ms = try std.fmt.parseInt(u64, v, 10) else {
                usage();
                return 2;
            }
        }
    }
    if (args.creds.len == 0 or args.principal.len == 0 or args.tables.len == 0) {
        usage();
        return 2;
    }
    if (args.db.len == 0) args.db = try std.fmt.allocPrintSentinel(a, "zebridge_{s}.sqlite3", .{args.principal}, 0);
    var tables: std.ArrayListUnmanaged([]const u8) = .empty;
    var it = std.mem.splitScalar(u8, args.tables, ',');
    while (it.next()) |t| if (t.len > 0) try tables.append(a, t);

    const t0 = nowMs();
    const c = try client.SyncClient.init(a, .{
        .url = args.url,
        .creds_path = args.creds,
        .db_path = args.db.ptr,
        .db_url = if (args.db_url) |u| u.ptr else null,
        .engine = args.engine,
        .principal = args.principal,
        .tables = tables.items,
        .client_id = args.client_id,
        .js_domain = args.js_domain,
        .heartbeat_ms = 0,
        .seed_streaming = args.stream,
    });
    defer c.deinit();

    const rep = try c.syncOnce();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    // A chain the producer has not cut yet (a table enabled a moment ago, or one
    // whose chain predates the stream) is retried on every turn until it lands.
    const deadline = nowMs() + @as(i64, @intCast(args.max_wait_s * 1000));
    while (true) {
        _ = arena.reset(.retain_capacity);
        const pr = try c.poll(arena.allocator(), args.poll_ms);
        if (!args.once) {
            if (pr.changed_tables.len > 0 or pr.seeded.len > 0) {
                std.debug.print("{s}: applied {d}, seeded {d} table(s), changed {d}\n", .{ rep.tenant, pr.applied, pr.seeded.len, pr.changed_tables.len });
            }
            continue;
        }
        if (!c.reseed_pending and try allSeeded(c, arena.allocator(), tables.items)) break;
        if (nowMs() > deadline) {
            std.debug.print("zb: gave up after {d} s — a table has no usable chain yet (the producer cuts one per cadence)\n", .{args.max_wait_s});
            return 1;
        }
    }
    // The summary: one JSON line a script reads.
    var out: std.ArrayListUnmanaged(u8) = .empty;
    const took: u64 = @intCast(nowMs() - t0);
    try out.print(a, "{{\"tenant\":\"{s}\",\"engine\":\"{s}\",\"seconds\":{d}.{d},\"tables\":{{", .{ rep.tenant, if (args.db_url != null) "postgres" else @tagName(args.engine), took / 1000, (took % 1000) / 100 });
    for (tables.items, 0..) |t, k| {
        if (k > 0) try out.append(a, ',');
        try out.print(a, "\"{s}\":{d}", .{ t, c.count(t) });
    }
    try out.appendSlice(a, "}}\n");
    try std.Io.File.stdout().writeStreamingAll(init.io, out.items);
    return 0;
}

/// Wall-clock milliseconds (libc is linked; std.time has no clock in 0.16).
fn nowMs() i64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return @as(i64, @intCast(ts.sec)) * 1000 + @divTrunc(@as(i64, @intCast(ts.nsec)), 1_000_000);
}

/// Every table has its watermark: the seed landed and the tail was drained.
fn allSeeded(c: *client.SyncClient, a: std.mem.Allocator, tables: []const []const u8) !bool {
    const res = try c.query(a, "SELECT tbl FROM _zbz_generations WHERE watermark IS NOT NULL", .{ .array = std.json.Array.init(a) });
    const rows = if (res == .object) (res.object.get("rows") orelse return false) else return false;
    if (rows != .array) return false;
    for (tables) |t| {
        var found = false;
        for (rows.array.items) |r| {
            if (r == .array and r.array.items.len > 0 and r.array.items[0] == .string and std.mem.eql(u8, r.array.items[0].string, t)) found = true;
        }
        if (!found) return false;
    }
    return true;
}
