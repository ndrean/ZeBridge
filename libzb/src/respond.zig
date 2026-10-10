//! `zb-respond config.json` — a responder with no Python: libzb serves the configured
//! questions (`query.<tenant>.<name>`, one queue group) and forwards each to a local HTTP
//! service, its answer going back to the asker. A routing engine (Valhalla), any service
//! that answers JSON over HTTP, becomes a ZeBridge service without code of its own.
//!
//!   {
//!     "url": "tls://nats.example.com:4222",      // the NATS server (a leaf near its askers)
//!     "creds": "/creds",                          // a responder's (bridge --mint-responder)
//!     "jsDomain": "hub",                          // behind a leaf node
//!     "name": "routing-nantes",                   // said in every answer as `by`
//!     "queue": "routing",                         // instances in one group share the questions
//!     "tenants": ["_default"],
//!     "questions": {
//!       "route":  { "http": "http://127.0.0.1:8002/route" },
//!       "matrix": { "http": "http://127.0.0.1:8002/sources_to_targets" },
//!       "tour":   { "http": "http://127.0.0.1:8002/optimized_route" }
//!     }
//!   }
//!
//! The question's payload is POSTed as it is; a JSON object answer gains `by` (the
//! instance) and `ms` (the time in the service). An HTTP error comes back as
//! `{"error", "status", "detail"}`, never as silence: the asker would wait out its timeout.
//! It needs no replica: with no table to follow, libzb's poll waits on the questions.

const std = @import("std");
const zblog = @import("zblog.zig");

pub const std_options: std.Options = .{ .log_level = .debug, .logFn = zblog.logFn };
const log = std.log.scoped(.libzb);
const client = @import("client.zig");
const enroll = @import("enroll.zig");
const Value = std.json.Value;

const Question = struct { name: []const u8, http: []const u8 };

/// Set by SIGINT/SIGTERM: the loop ends, every defer runs, and in Debug the allocator
/// reports what was never freed.
var stop = std.atomic.Value(bool).init(false);

fn onSignal(_: std.posix.SIG) callconv(.c) void {
    stop.store(true, .release);
}

pub fn main(init: std.process.Init) !u8 {
    // Debug: the leak-checking allocator, its report printed when main returns.
    var debug_alloc: std.heap.DebugAllocator(.{}) = .init;
    defer if (@import("builtin").mode == .Debug) {
        _ = debug_alloc.deinit();
    };
    const a = if (@import("builtin").mode == .Debug) debug_alloc.allocator() else std.heap.c_allocator;

    var argv: std.ArrayList([]const u8) = .empty;
    defer argv.deinit(a);
    var ait = init.minimal.args.iterate();
    while (ait.next()) |arg| try argv.append(a, arg);
    if (argv.items.len == 2 and std.mem.eql(u8, argv.items[1], "--version")) {
        std.Io.File.stdout().writeStreamingAll(init.io, "zb-respond " ++ @import("build_options").version ++ "\n") catch {};
        return 0;
    }
    if (argv.items.len != 2) {
        std.debug.print("usage: zb-respond config.json\n", .{});
        return 2;
    }

    // ── the configuration ──────────────────────────────────────────────────────
    const text = std.Io.Dir.cwd().readFileAlloc(init.io, argv.items[1], a, .limited(1 << 20)) catch |err| {
        log.err("🔴 cannot read {s}: {s}", .{ argv.items[1], @errorName(err) });
        return 1;
    };
    defer a.free(text);
    const parsed = std.json.parseFromSlice(Value, a, text, .{}) catch {
        log.err("🔴 {s} is not JSON", .{argv.items[1]});
        return 1;
    };
    defer parsed.deinit();
    const cfg = parsed.value.object;
    const str = struct {
        fn get(o: std.json.ObjectMap, k: []const u8, d: []const u8) []const u8 {
            const v = o.get(k) orelse return d;
            return if (v == .string) v.string else d;
        }
    }.get;
    const url = str(cfg, "url", "nats://127.0.0.1:4222");
    const creds = str(cfg, "creds", "");
    const name = str(cfg, "name", "zb-respond");
    const queue = str(cfg, "queue", "zb-respond");
    const js_domain = str(cfg, "jsDomain", "");
    const db = try a.dupeSentinel(u8, str(cfg, "db", "/tmp/zb-respond.sqlite3"), 0);
    defer a.free(db);
    if (creds.len == 0) {
        log.err("🔴 the config names no `creds` (a responder's: bridge --mint-responder)", .{});
        return 1;
    }
    const principal = enroll.principalFromCredsFile(a, creds) orelse {
        log.err("🔴 {s}: no principal in the creds", .{creds});
        return 1;
    };
    defer a.free(principal);

    var tenants: std.ArrayList([]const u8) = .empty;
    defer tenants.deinit(a);
    if (cfg.get("tenants")) |tv| if (tv == .array) for (tv.array.items) |t| if (t == .string) try tenants.append(a, t.string);
    if (tenants.items.len == 0) try tenants.append(a, "_default");

    var questions: std.ArrayList(Question) = .empty;
    defer questions.deinit(a);
    var names: std.ArrayList([]const u8) = .empty;
    defer names.deinit(a);
    if (cfg.get("questions")) |qv| if (qv == .object) {
        var it = qv.object.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.* != .object) continue;
            const target = str(e.value_ptr.object, "http", "");
            if (target.len == 0) {
                log.err("🔴 question {s}: only `http` targets are built", .{e.key_ptr.*});
                return 1;
            }
            try questions.append(a, .{ .name = e.key_ptr.*, .http = target });
            try names.append(a, e.key_ptr.*);
        }
    };
    if (questions.items.len == 0) {
        log.err("🔴 the config names no `questions`", .{});
        return 1;
    }

    // ── the client: no tables, a responder's identity ──────────────────────────
    const c = try client.SyncClient.init(a, .{
        .url = url,
        .creds_path = creds,
        .db_path = db.ptr,
        .principal = principal,
        .tables = &.{},
        .client_id = name,
        .js_domain = if (js_domain.len > 0) js_domain else null,
        .heartbeat_ms = 0,
    });
    defer c.deinit();
    _ = try c.syncOnce();
    const n = try c.serve(tenants.items, names.items, queue);
    log.info("{s}: answering {d} subject(s) in queue group {s}, as {s}", .{ name, n, queue, principal });

    var threaded: std.Io.Threaded = .init(a, .{});
    defer threaded.deinit();
    var http: std.http.Client = .{ .allocator = a, .io = threaded.io() };
    defer http.deinit();

    const on_signal: std.posix.Sigaction = .{ .handler = .{ .handler = onSignal }, .mask = std.mem.zeroes(std.posix.sigset_t), .flags = 0 };
    std.posix.sigaction(std.posix.SIG.INT, &on_signal, null);
    std.posix.sigaction(std.posix.SIG.TERM, &on_signal, null);

    // ── the loop: wait for questions, forward, answer ──────────────────────────
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    while (!stop.load(.acquire)) {
        _ = arena.reset(.retain_capacity);
        const aa = arena.allocator();
        const report = c.poll(aa, 1000) catch |err| {
            log.warn("poll: {s}", .{@errorName(err)});
            continue;
        };
        for (report.requests) |req| {
            const t0 = nowMs();
            const target = for (questions.items) |q| {
                if (std.mem.eql(u8, q.name, req.name)) break q.http;
            } else "";
            const answer = forward(aa, &http, target, req.payload, name, t0) catch |err|
                try std.fmt.allocPrint(aa, "{{\"error\":\"{s}\",\"by\":\"{s}\"}}", .{ @errorName(err), name });
            c.reply(req.id, answer) catch |err| log.err("reply {d}: {s}", .{ req.id, @errorName(err) });
            log.info("{s}: {d} bytes in {d} ms", .{ req.name, answer.len, nowMs() - t0 });
        }
    }
    log.info("{s}: stopped", .{name});
    return 0;
}

/// POST the payload to `target`; the answer as JSON text, `by` and `ms` added to an object.
fn forward(a: std.mem.Allocator, http: *std.http.Client, target: []const u8, payload: []const u8, name: []const u8, t0: i64) ![]const u8 {
    var body: std.Io.Writer.Allocating = .init(a);
    const res = try http.fetch(.{
        .location = .{ .url = target },
        .method = .POST,
        .payload = payload,
        .headers = .{ .content_type = .{ .override = "application/json" } },
        .response_writer = &body.writer,
    });
    const text = body.written();
    if (res.status != .ok) {
        var o: std.json.ObjectMap = .empty;
        try o.put(a, "error", .{ .string = "the service refused the question" });
        try o.put(a, "status", .{ .integer = @intFromEnum(res.status) });
        try o.put(a, "detail", .{ .string = text[0..@min(text.len, 500)] });
        try o.put(a, "by", .{ .string = name });
        return std.json.Stringify.valueAlloc(a, Value{ .object = o }, .{});
    }
    const parsed = std.json.parseFromSlice(Value, a, text, .{}) catch return text;
    if (parsed.value != .object) return text;
    var o = parsed.value.object;
    try o.put(a, "by", .{ .string = name });
    try o.put(a, "ms", .{ .integer = nowMs() - t0 });
    return std.json.Stringify.valueAlloc(a, Value{ .object = o }, .{});
}

fn nowMs() i64 {
    return @divTrunc(enroll.steadyMillis(), 1);
}
