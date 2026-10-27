//! One stream message of a readings stream as a row — the contract between the sensors and the
//! archive (examples/16-archive/README.md, "The payload").
//!
//!   subject  sensors.<tenant>.<sensor>.<kind>      exactly four tokens
//!   payload  a JSON number                         the value; the sensor time is the time the stream took it
//!            {"value": <number>, "ts_ms": <integer | float | null>}
//!                                                  BOTH keys present; null takes the stream's time;
//!                                                  a time before 1970 or after the year 9999 is no time
//!
//! Anything else is not a reading: it is skipped (the stream position still moves past it), never
//! a crash and never a guess. In particular a quoted number, a boolean, a non-finite number
//! (`1e999`, `NaN`), an object without `ts_ms` or with a repeated key, a byte-order mark and
//! UTF-16 are skipped. `examples/16-archive/common.py`
//! `reading()` applies the same rules, so the archive (this decoder, run by
//! `bridge_readings_fetch`) and the responder's hot tail (Python) count the same readings; the
//! scenario `readings_fetch.py` feeds both the same awkward payloads and compares.
const std = @import("std");

/// 9999-12-31T23:59:59.999Z: the last day Python's `datetime` can name. A sensor time beyond it
/// (a clock set wrong, a seconds-for-milliseconds slip the other way) used to stop the archiver
/// with a ValueError while it named the day of the first reading in a file.
pub const max_sensor_ms: i64 = 253_402_300_799_999;

pub const Row = struct {
    tenant: []const u8,
    sensor: []const u8,
    kind: []const u8,
    value: f64,
    /// The sensor's own time, or the stream's when the payload carries none.
    sensor_ms: i64,
};

/// `null` when the message is not a reading. `arena` holds the parsed JSON and is the caller's to
/// reset (one reset per batch keeps the cost per reading at a few allocations).
pub fn decode(arena: std.mem.Allocator, subject: []const u8, data: []const u8, received_ms: i64) ?Row {
    var it = std.mem.splitScalar(u8, subject, '.');
    _ = it.next() orelse return null;
    const tenant = it.next() orelse return null;
    const sensor = it.next() orelse return null;
    const kind = it.next() orelse return null;
    if (it.next() != null) return null;

    const body = std.json.parseFromSliceLeaky(std.json.Value, arena, data, .{}) catch return null;
    var ts: ?i64 = null;
    const value: f64 = switch (body) {
        .object => |o| blk: {
            const v = o.get("value") orelse return null;
            const t = o.get("ts_ms") orelse return null;
            ts = switch (t) {
                .null => null,
                .integer => |i| i,
                .float => |f| floatToMs(f) orelse return null,
                .number_string => return null, // beyond i64: not a time
                else => return null,
            };
            break :blk number(v) orelse return null;
        },
        else => number(body) orelse return null,
    };
    const sensor_ms = ts orelse received_ms;
    // A time the archive cannot name a file after (its day is part of the key) is not a time.
    if (sensor_ms < 0 or sensor_ms > max_sensor_ms) return null;
    return .{ .tenant = tenant, .sensor = sensor, .kind = kind, .value = value, .sensor_ms = sensor_ms };
}

fn number(v: std.json.Value) ?f64 {
    const f: f64 = switch (v) {
        .integer => |i| @floatFromInt(i),
        .float => |x| x,
        .number_string => |s| std.fmt.parseFloat(f64, s) catch return null,
        else => return null,
    };
    return if (std.math.isFinite(f)) f else null;
}

/// Python's `int(float)` truncates toward zero; a value outside i64 (or not finite) is not a time.
fn floatToMs(f: f64) ?i64 {
    if (!std.math.isFinite(f)) return null;
    if (f >= 9.2e18 or f <= -9.2e18) return null;
    return @intFromFloat(f);
}

const testing = std.testing;

fn expectRow(subject: []const u8, data: []const u8, want_value: f64, want_ms: i64) !void {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const r = decode(arena.allocator(), subject, data, 1234) orelse return error.NotAReading;
    try testing.expectEqual(want_value, r.value);
    try testing.expectEqual(want_ms, r.sensor_ms);
}

fn expectSkipped(subject: []const u8, data: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    try testing.expect(decode(arena.allocator(), subject, data, 1234) == null);
}

test "a bare number takes the stream's time" {
    try expectRow("sensors.acme.s1.temp", "21.5", 21.5, 1234);
    try expectRow("sensors.acme.s1.temp", "7", 7.0, 1234);
    try expectRow("sensors.acme.s1.temp", " -3e2 ", -300.0, 1234);
}

test "an object carries its own time; null takes the stream's" {
    try expectRow("sensors.acme.s1.temp", "{\"value\": 1.5, \"ts_ms\": 1700000000000}", 1.5, 1700000000000);
    try expectRow("sensors.acme.s1.temp", "{\"ts_ms\": 99, \"value\": 2, \"extra\": {\"value\": 5}}", 2.0, 99);
    try expectRow("sensors.acme.s1.temp", "{\"value\": 1, \"ts_ms\": null}", 1.0, 1234);
    // int(float) truncates toward zero, as in Python
    try expectRow("sensors.acme.s1.temp", "{\"value\": 1, \"ts_ms\": 1700000000000.9}", 1.0, 1700000000000);
    try expectRow("sensors.acme.s1.temp", "{\"value\": 1, \"ts_ms\": 0.9}", 1.0, 0);
}

test "what is not a reading is skipped" {
    const s = "sensors.acme.s1.temp";
    try expectSkipped(s, "");
    try expectSkipped(s, "not json");
    try expectSkipped(s, "\"21.5\""); // a quoted number
    try expectSkipped(s, "true");
    try expectSkipped(s, "null");
    try expectSkipped(s, "[1]");
    try expectSkipped(s, "NaN");
    try expectSkipped(s, "Infinity");
    try expectSkipped(s, "1e999"); // overflows to infinity
    try expectSkipped(s, "{\"value\": 1}"); // no ts_ms
    try expectSkipped(s, "{\"ts_ms\": 1}"); // no value
    try expectSkipped(s, "{\"value\": \"1\", \"ts_ms\": 1}");
    try expectSkipped(s, "{\"value\": true, \"ts_ms\": 1}");
    try expectSkipped(s, "{\"value\": 1, \"ts_ms\": \"1\"}");
    try expectSkipped(s, "{\"value\": 1, \"ts_ms\": 1e30}");
    try expectSkipped(s, "{\"value\": 1, \"ts_ms\": -1}"); // before 1970
    try expectSkipped(s, "{\"value\": 1, \"ts_ms\": 253402300800000}"); // after the year 9999
    try expectSkipped(s, "{\"value\": 1e999, \"ts_ms\": 1}");
}

test "the last millisecond of 9999 is a time" {
    try expectRow("sensors.acme.s1.temp", "{\"value\": 1, \"ts_ms\": 253402300799999}", 1.0, max_sensor_ms);
}

test "the subject has exactly four tokens" {
    try expectSkipped("sensors.acme.s1", "1");
    try expectSkipped("sensors.acme.s1.temp.extra", "1");
    try expectSkipped("sensors", "1");
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const r = decode(arena.allocator(), "sensors.acme.s-1.temp_c", "1", 5).?;
    try testing.expectEqualStrings("acme", r.tenant);
    try testing.expectEqualStrings("s-1", r.sensor);
    try testing.expectEqualStrings("temp_c", r.kind);
}

test "a big integer is a number, a huge one is not a time" {
    try expectRow("sensors.acme.s1.temp", "12345678901234567890", 12345678901234567890.0, 1234);
    try expectSkipped("sensors.acme.s1.temp", "{\"value\": 1, \"ts_ms\": 12345678901234567890}");
}
