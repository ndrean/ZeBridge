//! Various utility functions
const std = @import("std");
const builtin = @import("builtin");

/// Cross-platform sleep (std.Thread.sleep removed in Zig 0.16 Juicy Main).
pub fn sleep(nanoseconds: u64) void {
    var ts = std.c.timespec{
        .sec = @as(isize, @intCast(nanoseconds / std.time.ns_per_s)),
        .nsec = @as(isize, @intCast(nanoseconds % std.time.ns_per_s)),
    };
    _ = std.c.nanosleep(&ts, null);
}

/// Monotonic millisecond timestamp
/// Monotonic nanoseconds. Same clock as getMilliTimestamp, finer resolution — for
/// measuring intervals inside the WAL loop, never for wall-clock time.
pub fn nanoTimestamp() u64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

/// CPU time this process has consumed, in nanoseconds (user + system).
///
/// From `getrusage(RUSAGE_SELF)` — POSIX, so no `/proc` parsing and no platform split.
/// Exposed because "how busy is the bridge?" was otherwise only answerable with `htop`,
/// where a single process is hard to isolate from the noise; as a counter it turns into
/// cores-used with `rate(bridge_cpu_seconds_total[1m])` in Prometheus.
pub fn cpuTimeNanos() u64 {
    var usage: std.c.rusage = undefined;
    if (std.c.getrusage(std.c.rusage.SELF, &usage) != 0) return 0;

    const user_ns = @as(u64, @intCast(usage.utime.sec)) * std.time.ns_per_s +
        @as(u64, @intCast(usage.utime.usec)) * std.time.ns_per_us;
    const sys_ns = @as(u64, @intCast(usage.stime.sec)) * std.time.ns_per_s +
        @as(u64, @intCast(usage.stime.usec)) * std.time.ns_per_us;
    return user_ns + sys_ns;
}

/// Peak resident set size in bytes. `ru_maxrss` is **bytes on Darwin and kilobytes on
/// Linux** — one of the few genuinely divergent POSIX fields, so it is normalised here
/// rather than at each call site.
pub fn maxRssBytes() u64 {
    var usage: std.c.rusage = undefined;
    if (std.c.getrusage(std.c.rusage.SELF, &usage) != 0) return 0;
    const raw: u64 = @intCast(@max(usage.maxrss, 0));
    return if (builtin.os.tag == .linux) raw * 1024 else raw;
}

pub fn getMilliTimestamp() i64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * 1000 + @divTrunc(@as(i64, ts.nsec), 1_000_000);
}

/// Convert days since Unix epoch (1970-01-01) to civil calendar date (year, month, day)
///
/// This uses the proleptic Gregorian calendar algorithm from Howard Hinnant's date library,
/// which is what PostgreSQL uses internally for efficient date arithmetic.
///
/// Algorithm: http://howardhinnant.github.io/date_algorithms.html#civil_from_days
pub inline fn civilFromDays(z: i64) struct { year: i32, month: u8, day: u8 } {
    // Shift epoch from 1970-01-01 to 0000-03-01 (March 1, year 0)
    // This makes leap day the last day of the year for simpler arithmetic
    const z2 = z + 719468;

    // An "era" is a 400-year cycle (146097 days) in the Gregorian calendar
    const era = @divFloor(z2, 146097);
    const doe = @as(u32, @intCast(z2 - era * 146097)); // Day of era [0, 146096]

    // Year of era [0, 399]
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);

    // Compute year: cast era to i32 then multiply
    const y = @as(i32, @intCast(yoe)) + @as(i32, @intCast(era)) * 400;

    // Day of year [0, 365]
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));

    // Month pointer: [0=Mar, 1=Apr, ..., 9=Dec, 10=Jan, 11=Feb]
    const mp = @divFloor(5 * doy + 2, 153);

    // Day of month [1, 31]
    const d = @as(u8, @intCast(doy - @divFloor(153 * mp + 2, 5) + 1));

    // Convert month pointer to civil month [1=Jan, ..., 12=Dec]
    // mp: [0=Mar, 1=Apr, ..., 9=Dec, 10=Jan, 11=Feb]
    const m: u8 = if (mp < 10) @intCast(mp + 3) else @intCast(mp - 9);

    // Adjust year for Jan/Feb (they belong to the next year in our shifted calendar)
    const year = y + @as(i32, if (m <= 2) 1 else 0);

    return .{
        .year = year,
        .month = m,
        .day = d,
    };
}

/// std.fmt.allocPrintZ was removed in Zig 0.16; this is a drop-in replacement
/// that allocates a null-terminated C string from a format string.
pub fn allocPrintZ(allocator: std.mem.Allocator, comptime fmt_str: []const u8, args: anytype) ![:0]u8 {
    const s = try std.fmt.allocPrint(allocator, fmt_str, args);
    defer allocator.free(s);
    const buf = try allocator.alloc(u8, s.len + 1);
    @memcpy(buf[0..s.len], s);
    buf[s.len] = 0;
    return buf[0..s.len :0];
}

pub fn byteToHex(out: []u8, byte: u8) void {
    out[0] = toHexDigit(@as(u8, byte >> 4));
    out[1] = toHexDigit(@as(u8, byte & 0x0F));
}

fn toHexDigit(d: u8) u8 {
    return if (d < 10) '0' + d else 'a' + (d - 10);
}
