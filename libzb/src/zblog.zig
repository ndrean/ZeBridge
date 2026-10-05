//! libzb's log filter: every level is compiled in, `ZB_LOG_LEVEL` (debug|info|warn|err,
//! default info) picks what prints. It covers nats.zig's `debug(nats)` lines too, which
//! no longer reach a Debug build's stderr unless asked for. Each root (capi.zig, zb.zig,
//! respond.zig) installs it with
//!
//!   pub const std_options: std.Options = .{ .log_level = .debug, .logFn = zblog.logFn };
const std = @import("std");

/// Read once, on the first line logged: 0xff until then.
var level = std.atomic.Value(u8).init(0xff);

fn current() std.log.Level {
    const v = level.load(.acquire);
    if (v != 0xff) return @enumFromInt(v);
    const parsed = parse(if (std.c.getenv("ZB_LOG_LEVEL")) |p| std.mem.span(p) else null);
    level.store(@intFromEnum(parsed), .release);
    return parsed;
}

fn parse(raw: ?[]const u8) std.log.Level {
    const v = raw orelse return .info;
    if (std.mem.eql(u8, v, "debug")) return .debug;
    if (std.mem.eql(u8, v, "warn") or std.mem.eql(u8, v, "warning")) return .warn;
    if (std.mem.eql(u8, v, "err") or std.mem.eql(u8, v, "error")) return .err;
    return .info;
}

pub fn logFn(
    comptime message_level: std.log.Level,
    comptime scope: @TypeOf(.EnumLiteral),
    comptime format: []const u8,
    args: anytype,
) void {
    if (@intFromEnum(message_level) > @intFromEnum(current())) return;
    std.log.defaultLog(message_level, scope, format, args);
}

test "ZB_LOG_LEVEL spellings" {
    try std.testing.expectEqual(std.log.Level.info, parse(null));
    try std.testing.expectEqual(std.log.Level.warn, parse("warning"));
    try std.testing.expectEqual(std.log.Level.err, parse("error"));
    try std.testing.expectEqual(std.log.Level.info, parse("loud"));
}
