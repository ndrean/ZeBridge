//! Various utility functions
const std = @import("std");

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
