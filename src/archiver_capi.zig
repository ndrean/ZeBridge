//! libzbarchive: the native readings archiver behind a C ABI (include/zbarchive.h), for a
//! host in any language that calls C — Go (cgo), Rust (`extern "C"`), Python (ctypes), Swift.
//! A library of its own, not part of libzbcore: a mobile client never carries the archiver.
//!
//! One handle is one archiver for one tenant. `zba_run` blocks until the archive stops (a
//! `zba_stop`, `exit_when_idle_s`, or an outcome with its own code) and returns the exit code
//! the `bridge_archiver` command and the Python package return. `zba_stop` is safe from any
//! thread and from a signal handler (one atomic store); `zba_run` returns within a quarter of a
//! second, or after the chunk being written.

const std = @import("std");
const archiver = @import("archiver.zig");

pub const std_options: std.Options = .{ .log_level = .warn };

/// Bumped when a function changes its meaning; a host checks it before the first call.
const abi_version: c_int = 1;

/// Debug and ReleaseSafe builds check every handle's allocations (Zig's SafeAllocator, the one
/// `main` gets in those modes): `zba_close` reports a leak on stderr and counts it in
/// `zba_leaks()`, so a host's test sees a Zig-side leak without valgrind. ReleaseFast uses libc.
const checked = switch (@import("builtin").mode) {
    .Debug, .ReleaseSafe => true,
    .ReleaseFast, .ReleaseSmall => false,
};
const Safe = std.heap.SafeAllocator;
var leaks = std.atomic.Value(u32).init(0);

const Handle = struct {
    safe: if (checked) Safe else void,
    arena: std.heap.ArenaAllocator,
    threaded: std.Io.Threaded,
    cfg: archiver.Config,
    stop: std.atomic.Value(bool) = .init(false),
    log: archiver.Log,
};

const c_gpa = std.heap.c_allocator;

fn allocatorOf(h: *Handle) std.mem.Allocator {
    return if (checked) h.safe.allocator() else c_gpa;
}

threadlocal var last_error_buf: [512]u8 = undefined;
threadlocal var last_error: ?[*:0]const u8 = null;

fn setError(comptime fmt: []const u8, args: anytype) void {
    const s = std.mem.printSentinel(&last_error_buf, fmt, args, 0) catch blk: {
        last_error_buf[last_error_buf.len - 1] = 0;
        break :blk last_error_buf[0 .. last_error_buf.len - 1 :0];
    };
    last_error = s.ptr;
}

/// The JSON a host passes to `zba_open`: the Config fields by name, every one optional but
/// `tenant`. An unknown key is refused, so a misspelled option is an error, not a default.
const Json = struct {
    tenant: []const u8,
    flush_s: f64 = 60,
    batch: u64 = 100_000,
    outage_s: f64 = 3600,
    exit_when_idle_s: f64 = 0,
    accept_loss: bool = false,
    nats_url: ?[]const u8 = null,
    creds: ?[]const u8 = null,
    tls_ca: ?[]const u8 = null,
    s3_endpoint: ?[]const u8 = null,
    s3_bucket: ?[]const u8 = null,
    s3_region: ?[]const u8 = null,
    access_key_id: ?[]const u8 = null,
    secret_access_key: ?[]const u8 = null,
    work_dir: ?[]const u8 = null,
};

export fn zba_abi_version() c_int {
    return abi_version;
}

/// Why the last `zba_open` on this thread returned NULL. Valid until the next call on this thread.
export fn zba_last_error() ?[*:0]const u8 {
    return last_error;
}

/// A handle, or NULL with `zba_last_error()` set. `log` may be NULL (lines go to stdout);
/// it is called on the thread that runs `zba_run`, with a line valid for the call only.
export fn zba_open(config_json: ?[*:0]const u8, log: ?*const fn (?*anyopaque, u8, [*:0]const u8) callconv(.c) void, ctx: ?*anyopaque) ?*Handle {
    last_error = null;
    const text = std.mem.span(config_json orelse {
        setError("zba_open: no configuration", .{});
        return null;
    });
    const h = c_gpa.create(Handle) catch {
        setError("zba_open: out of memory", .{});
        return null;
    };
    h.* = .{ .safe = if (checked) .init(std.heap.page_allocator, .{}) else {}, .arena = undefined, .threaded = undefined, .cfg = .{}, .log = .{ .ctx = ctx, .func = log } };
    const gpa = allocatorOf(h);
    h.arena = .init(gpa);
    const arena = h.arena.allocator();
    const j = std.json.parseFromSliceLeaky(Json, arena, text, .{ .ignore_unknown_fields = false, .allocate = .alloc_always }) catch |err| {
        setError("zba_open: the configuration is not valid JSON for the archiver ({s}); keys: tenant, flush_s, batch, outage_s, exit_when_idle_s, accept_loss, nats_url, creds, tls_ca, s3_endpoint, s3_bucket, s3_region, access_key_id, secret_access_key, work_dir", .{@errorName(err)});
        destroy(h);
        return null;
    };
    if (j.tenant.len == 0 or j.batch == 0 or !(j.flush_s >= 0) or !(j.outage_s >= 0)) {
        setError("zba_open: tenant must be set, batch above 0, flush_s and outage_s 0 or more", .{});
        destroy(h);
        return null;
    }
    h.cfg = .{
        .tenant = j.tenant, .flush_s = j.flush_s, .batch = j.batch, .outage_s = j.outage_s,
        .exit_when_idle_s = j.exit_when_idle_s, .accept_loss = j.accept_loss,
        .nats_url = j.nats_url, .creds = j.creds, .tls_ca = j.tls_ca,
        .s3_endpoint = j.s3_endpoint, .s3_bucket = j.s3_bucket, .s3_region = j.s3_region,
        .access_key_id = j.access_key_id, .secret_access_key = j.secret_access_key, .work_dir = j.work_dir,
    };
    h.threaded = .init(gpa, .{});
    archiver.resolve(arena, h.threaded.io(), &h.cfg) catch {
        setError("zba_open: out of memory", .{});
        h.threaded.deinit();
        destroy(h);
        return null;
    };
    return h;
}

/// Blocks until the archive stops; returns its exit code (0 stopped, 1 failed, 2 retention too
/// short, 3 another archiver moved the manifest, 4 readings lost, 5 an upload of the wrong size).
/// A handle runs once at a time; after it returns, `zba_run` may be called again.
export fn zba_run(h: ?*Handle) c_int {
    const handle = h orelse return @intFromEnum(archiver.ExitCode.failed);
    handle.stop.store(false, .release);
    var a: archiver.Archiver = .{ .gpa = allocatorOf(handle), .io = handle.threaded.io(), .cfg = handle.cfg, .stop = &handle.stop, .log = handle.log };
    return @intFromEnum(a.run());
}

/// Asks a running `zba_run` to return. Async-signal-safe: one atomic store.
export fn zba_stop(h: ?*Handle) void {
    if (h) |handle| handle.stop.store(true, .release);
}

/// Frees the handle. Not while `zba_run` is running on it: stop it, wait for it to return.
export fn zba_close(h: ?*Handle) void {
    const handle = h orelse return;
    handle.threaded.deinit();
    destroy(handle);
}

fn destroy(h: *Handle) void {
    h.arena.deinit();
    if (checked and h.safe.deinit() > 0) {
        _ = leaks.fetchAdd(1, .monotonic);
        std.debug.print("🔴 libzbarchive: this handle leaked memory (the allocations are listed above)\n", .{});
    }
    c_gpa.destroy(h);
}

/// Handles closed with a leak since the library was loaded: always 0 in ReleaseFast/Small
/// (nothing is checked), so a host's test asserts 0 on a Debug build.
export fn zba_leaks() u32 {
    return leaks.load(.monotonic);
}

test "a configuration is refused by name" {
    try std.testing.expect(zba_open("{\"tenant\":\"acme\",\"flush\":1}", null, null) == null);
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(zba_last_error().?), "keys: tenant") != null);
    try std.testing.expect(zba_open("{\"tenant\":\"\"}", null, null) == null);
    try std.testing.expect(zba_open(null, null, null) == null);
    const h = zba_open("{\"tenant\":\"acme\",\"s3_endpoint\":\"http://127.0.0.1:1\",\"access_key_id\":\"k\",\"secret_access_key\":\"s\"}", null, null) orelse return error.Refused;
    defer zba_close(h);
    try std.testing.expectEqualStrings("acme", h.cfg.tenant);
    try std.testing.expectEqualStrings("http://127.0.0.1:1", h.cfg.s3_endpoint.?);
    try std.testing.expectEqual(@as(c_int, 1), zba_abi_version());
    try std.testing.expectEqual(@as(u32, 0), zba_leaks());
}
