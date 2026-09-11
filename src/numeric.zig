const std = @import("std");

pub const NumericDecodeError = error{
    Truncated,
    InvalidNumeric,
    OutOfMemory,
};

const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;

pub const NumericResult = struct {
    slice: []const u8,
    allocated: []u8, // what to actually free

    pub fn deinit(self: NumericResult, allocator: std.mem.Allocator) void {
        allocator.free(self.allocated);
    }
};

/// Parse PostgreSQL NUMERIC binary format into a decimal string.
pub fn parseNumeric(allocator: std.mem.Allocator, buf: []const u8) NumericDecodeError!NumericResult {
    if (buf.len < 8) return NumericDecodeError.Truncated;

    const ndigits = std.mem.readInt(u16, buf[0..2], .big);
    const weight = std.mem.readInt(i16, buf[2..4], .big);
    const sign = std.mem.readInt(u16, buf[4..6], .big);
    // The display scale: PostgreSQL prints exactly this many digits after the point,
    // whatever the base-10000 groups carry — `237.5` stored with scale 8 prints as
    // `237.50000000`, `1.5` stored as the groups [1, 5000] with scale 1 prints as
    // `1.5`. Ignored until 2026-09-11 (§10ep): every numeric reached the clients with
    // the groups' padding instead, `237.5000`, on CDC and on the chain alike.
    const dscale: usize = std.mem.readInt(u16, buf[6..8], .big);

    // Handle special values
    switch (sign) {
        NUMERIC_NAN => {
            const mem = allocator.alloc(u8, 3) catch return NumericDecodeError.OutOfMemory;
            @memcpy(mem, "NaN");
            return .{ .slice = mem, .allocated = mem };
        },
        NUMERIC_PINF => {
            const mem = allocator.alloc(u8, 8) catch return NumericDecodeError.OutOfMemory;
            @memcpy(mem, "Infinity");
            return .{ .slice = mem, .allocated = mem };
        },
        NUMERIC_NINF => {
            const mem = allocator.alloc(u8, 9) catch return NumericDecodeError.OutOfMemory;
            @memcpy(mem, "-Infinity");
            return .{ .slice = mem, .allocated = mem };
        },
        NUMERIC_POS, NUMERIC_NEG => {},
        else => return NumericDecodeError.InvalidNumeric,
    }

    const n: usize = @intCast(ndigits);
    if (n == 0) {
        const mem = allocator.alloc(u8, 1) catch return NumericDecodeError.OutOfMemory;
        mem[0] = '0';
        return .{ .slice = mem, .allocated = mem };
    }

    if (8 + n * 2 > buf.len) return NumericDecodeError.Truncated;

    const leading_zero_groups: usize = if (weight < -1) @intCast(-weight - 1) else 0;
    const int_groups: usize = if (weight >= 0) @intCast(weight + 1) else 0;
    const max_size = 1 + 2 + leading_zero_groups * 4 + int_groups * 4 + 1 + n * 4 + dscale + 1;

    var out = allocator.alloc(u8, max_size) catch return NumericDecodeError.OutOfMemory;
    var pos: usize = 0;

    if (sign == NUMERIC_NEG) {
        out[pos] = '-';
        pos += 1;
    }

    const digit_buf = buf[8..];

    if (weight < 0) {
        out[pos] = '0';
        out[pos + 1] = '.';
        pos += 2;

        for (0..leading_zero_groups) |_| {
            @memcpy(out[pos..][0..4], "0000");
            pos += 4;
        }

        for (0..n) |i| {
            const d = std.mem.readInt(u16, digit_buf[i * 2 ..][0..2], .big);
            pos += writeDigitPadded(out[pos..][0..4], d);
        }
    } else {
        for (0..int_groups) |i| {
            const d: u16 = if (i < n) std.mem.readInt(u16, digit_buf[i * 2 ..][0..2], .big) else 0;

            if (i == 0) {
                pos += writeDigit(out[pos..], d);
            } else {
                pos += writeDigitPadded(out[pos..][0..4], d);
            }
        }

        if (n > int_groups) {
            out[pos] = '.';
            pos += 1;

            for (int_groups..n) |i| {
                const d = std.mem.readInt(u16, digit_buf[i * 2 ..][0..2], .big);
                pos += writeDigitPadded(out[pos..][0..4], d);
            }
        }
    }

    pos = fitScale(out, pos, dscale);
    return .{ .slice = out[0..pos], .allocated = out };
}

/// Fit the fraction to exactly `dscale` digits: pad with zeros, or trim — the digits
/// trimmed are zeros by construction, since PostgreSQL rounds a numeric to its scale
/// on input and the groups only pad to a multiple of four. Scale 0 drops the point.
fn fitScale(out: []u8, len: usize, dscale: usize) usize {
    const dot = std.mem.indexOfScalar(u8, out[0..len], '.');
    if (dscale == 0) return if (dot) |d| d else len;
    if (dot) |d| {
        const frac = len - d - 1;
        if (frac == dscale) return len;
        if (frac > dscale) return d + 1 + dscale;
        @memset(out[len .. len + (dscale - frac)], '0');
        return len + (dscale - frac);
    }
    out[len] = '.';
    @memset(out[len + 1 .. len + 1 + dscale], '0');
    return len + 1 + dscale;
}

fn writeDigit(out: []u8, d: u16) usize {
    if (d == 0) {
        out[0] = '0';
        return 1;
    }

    var val = d;
    var len: usize = 0;
    var tmp = d;
    while (tmp > 0) : (tmp /= 10) len += 1;

    var i = len;
    while (val > 0) : (val /= 10) {
        i -= 1;
        out[i] = '0' + @as(u8, @intCast(val % 10));
    }
    return len;
}

fn writeDigitPadded(out: *[4]u8, d: u16) usize {
    out[0] = '0' + @as(u8, @intCast(d / 1000));
    out[1] = '0' + @as(u8, @intCast((d / 100) % 10));
    out[2] = '0' + @as(u8, @intCast((d / 10) % 10));
    out[3] = '0' + @as(u8, @intCast(d % 10));
    return 4;
}

fn makeNumericBuf(comptime ndigits: u16, weight: i16, sign: u16, digits: []const u16) [8 + ndigits * 2]u8 {
    return makeNumericBufScaled(ndigits, weight, sign, digits, 0);
}

fn makeNumericBufScaled(comptime ndigits: u16, weight: i16, sign: u16, digits: []const u16, dscale: u16) [8 + ndigits * 2]u8 {
    var buf: [8 + ndigits * 2]u8 = undefined;
    std.mem.writeInt(u16, buf[0..2], ndigits, .big);
    std.mem.writeInt(i16, buf[2..4], weight, .big);
    std.mem.writeInt(u16, buf[4..6], sign, .big);
    std.mem.writeInt(u16, buf[6..8], dscale, .big);
    for (digits, 0..) |d, i| {
        std.mem.writeInt(u16, buf[8 + i * 2 ..][0..2], d, .big);
    }
    return buf;
}

test "parseNumeric" {
    const alloc = std.testing.allocator;

    // Zero
    {
        const result = try parseNumeric(alloc, &[_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 });
        defer result.deinit(alloc);
        try std.testing.expectEqualStrings("0", result.slice);
    }

    // 1234567.91011000
    {
        const buf = makeNumericBufScaled(4, 1, 0, &[_]u16{ 123, 4567, 9101, 1000 }, 8);
        const result = try parseNumeric(alloc, &buf);
        defer result.deinit(alloc);
        try std.testing.expectEqualStrings("1234567.91011000", result.slice);
    }

    // 12345
    {
        const buf = makeNumericBuf(2, 1, 0, &[_]u16{ 1, 2345 });
        const result = try parseNumeric(alloc, &buf);
        defer result.deinit(alloc);
        try std.testing.expectEqualStrings("12345", result.slice);
    }

    // -0.0042
    {
        const buf = makeNumericBufScaled(1, -1, 0x4000, &[_]u16{42}, 4);
        const result = try parseNumeric(alloc, &buf);
        defer result.deinit(alloc);
        try std.testing.expectEqualStrings("-0.0042", result.slice);
    }
    // NaN
    {
        const result = try parseNumeric(alloc, &[_]u8{ 0, 0, 0, 0, 0xC0, 0, 0, 0 });
        defer result.deinit(alloc);
        try std.testing.expectEqualStrings("NaN", result.slice);
    }

    // §10ep: the display scale, both directions — padded to the scale the groups
    // fall short of, trimmed to the scale the groups exceed.
    {
        const buf = makeNumericBufScaled(2, 0, 0, &[_]u16{ 237, 5000 }, 8);
        const result = try parseNumeric(alloc, &buf);
        defer alloc.free(result.allocated);
        try std.testing.expectEqualStrings("237.50000000", result.slice);
    }
    {
        const buf = makeNumericBufScaled(2, 0, 0, &[_]u16{ 1, 5000 }, 1);
        const result = try parseNumeric(alloc, &buf);
        defer alloc.free(result.allocated);
        try std.testing.expectEqualStrings("1.5", result.slice);
    }
    {
        const buf = makeNumericBufScaled(1, 1, 0, &[_]u16{1}, 0);
        const result = try parseNumeric(alloc, &buf);
        defer alloc.free(result.allocated);
        try std.testing.expectEqualStrings("10000", result.slice);
    }
    {
        const buf = makeNumericBufScaled(1, 0, 0, &[_]u16{7}, 2);
        const result = try parseNumeric(alloc, &buf);
        defer alloc.free(result.allocated);
        try std.testing.expectEqualStrings("7.00", result.slice);
    }
}
