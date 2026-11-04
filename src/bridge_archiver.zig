//! bridge_archiver: the native readings archiver as a command (archiver.zig). The options,
//! environment and exit codes of the Python package's `zb-archiver` (zb-archiver/README.md),
//! so either can run in the other's place:
//!
//!   NATS_URL=… NATS_CREDS=archiver-acme.creds S3_ENDPOINT=… S3_BUCKET=… AWS_ACCESS_KEY_ID=… \
//!     AWS_SECRET_ACCESS_KEY=… bridge_archiver --tenant acme --flush-s 60
//!
//! Needs libduckdb at run time (ZB_DUCKDB_LIB, else the loader's search). SIGINT and SIGTERM
//! stop it after the chunk being written.

const std = @import("std");
const archiver = @import("archiver.zig");

/// A debug-level library log would put message bodies (sensor data) on stderr.
pub const std_options: std.Options = .{ .log_level = .warn };

const usage =
    \\usage: bridge_archiver --tenant T [--flush-s S] [--batch N] [--outage-s S] [--exit-when-idle S] [--accept-loss]
    \\  --tenant T          the tenant to archive (READINGS_<T> → s3://<bucket>/<T>/readings/…)
    \\  --flush-s S         flush at least this often while readings arrive (default 60)
    \\  --batch N           flush when this many readings are waiting; messages per file (default 100000)
    \\  --outage-s S        the S3 outage the stream must outlast (default 3600)
    \\  --exit-when-idle S  stop after this long with nothing to flush (tests; default never)
    \\  --accept-loss       start even if the stream lost readings the archive does not hold
    \\Environment: NATS_URL, NATS_CREDS, NATS_TLS_CA, S3_ENDPOINT, S3_BUCKET, S3_REGION,
    \\AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, ZB_DUCKDB_LIB.
    \\Exit codes: 0 stopped, 1 failed, 2 retention too short, 3 another archiver moved the
    \\manifest, 4 readings lost, 5 an upload of the wrong size, 9 killed on purpose (tests).
    \\
;

var stop: std.atomic.Value(bool) = .init(false);

fn onSignal(_: std.posix.SIG) callconv(.c) void {
    stop.store(true, .release);
}

fn parseFloat(s: []const u8) ?f64 {
    const v = std.fmt.parseFloat(f64, s) catch return null;
    return if (std.math.isFinite(v) and v >= 0) v else null;
}

fn parseArgs(init: std.process.Init, cfg: *archiver.Config) bool {
    var it = init.minimal.args.iterate();
    _ = it.next();
    while (it.next()) |arg| {
        if (std.mem.eql(u8, arg, "--accept-loss")) {
            cfg.accept_loss = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--crash-after-upload")) {
            cfg.crash_after_upload = true;
            continue;
        }
        const val = it.next() orelse return false;
        // The last --tenant wins, as with argparse.
        if (std.mem.eql(u8, arg, "--tenant")) cfg.tenant = val else if (std.mem.eql(u8, arg, "--flush-s")) cfg.flush_s = parseFloat(val) orelse return false else if (std.mem.eql(u8, arg, "--outage-s")) cfg.outage_s = parseFloat(val) orelse return false else if (std.mem.eql(u8, arg, "--exit-when-idle")) cfg.exit_when_idle_s = parseFloat(val) orelse return false else if (std.mem.eql(u8, arg, "--batch")) cfg.batch = std.fmt.parseInt(u64, val, 10) catch return false else return false;
    }
    return cfg.tenant.len > 0 and cfg.batch > 0;
}

pub fn main(init: std.process.Init) u8 {
    var cfg: archiver.Config = .{};
    if (!parseArgs(init, &cfg)) {
        std.debug.print("{s}", .{usage});
        return 1;
    }
    var arena_state = std.heap.ArenaAllocator.init(init.gpa);
    defer arena_state.deinit();
    archiver.resolve(arena_state.allocator(), init.io, &cfg) catch {
        std.debug.print("🔴 bridge_archiver: out of memory\n", .{});
        return 1;
    };
    const on_signal: std.posix.Sigaction = .{ .handler = .{ .handler = onSignal }, .mask = std.mem.zeroes(std.posix.sigset_t), .flags = 0 };
    std.posix.sigaction(std.posix.SIG.INT, &on_signal, null);
    std.posix.sigaction(std.posix.SIG.TERM, &on_signal, null);
    var a: archiver.Archiver = .{ .gpa = init.gpa, .io = init.io, .cfg = cfg, .stop = &stop, .log = .{} };
    return @intFromEnum(a.run());
}
