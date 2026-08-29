//! The sans-I/O core, in Zig — the port of zb-client-ts/src/core.ts.
//!
//! The spec is zb-client-ts/fixtures/core-fixtures.json: this module is correct
//! exactly when the Python runner (libzb/python/runner.py) passes every case
//! against the built library. Functions mirror core.ts one to one; JSON in,
//! JSON out (capi.zig owns the dispatch), because the fixtures are JSON and a
//! C ABI wants one string-shaped calling convention anyway.

const std = @import("std");
const Value = std.json.Value;

// ─── JSON output writer (compact, JS-JSON.stringify-compatible) ─────────────

pub fn writeJsonString(a: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    try out.append(a, '"');
    for (s) |c| switch (c) {
        '"' => try out.appendSlice(a, "\\\""),
        '\\' => try out.appendSlice(a, "\\\\"),
        '\n' => try out.appendSlice(a, "\\n"),
        '\r' => try out.appendSlice(a, "\\r"),
        '\t' => try out.appendSlice(a, "\\t"),
        else => if (c < 0x20) {
            var buf: [6]u8 = undefined;
            const hex = std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{c}) catch unreachable;
            try out.appendSlice(a, hex);
        } else try out.append(a, c),
    };
    try out.append(a, '"');
}

/// Re-serialize a parsed JSON value compactly, preserving object key order —
/// the shape JS `JSON.stringify` produces, which the fixtures compare against.
pub fn writeValue(a: std.mem.Allocator, out: *std.ArrayList(u8), v: Value) !void {
    switch (v) {
        .null => try out.appendSlice(a, "null"),
        .bool => |x| try out.appendSlice(a, if (x) "true" else "false"),
        .integer => |x| {
            var buf: [24]u8 = undefined;
            try out.appendSlice(a, std.fmt.bufPrint(&buf, "{d}", .{x}) catch unreachable);
        },
        .float => |x| {
            var buf: [32]u8 = undefined;
            try out.appendSlice(a, std.fmt.bufPrint(&buf, "{d}", .{x}) catch unreachable);
        },
        .number_string => |x| try out.appendSlice(a, x),
        .string => |x| try writeJsonString(a, out, x),
        .array => |arr| {
            try out.append(a, '[');
            for (arr.items, 0..) |item, i| {
                if (i > 0) try out.append(a, ',');
                try writeValue(a, out, item);
            }
            try out.append(a, ']');
        },
        .object => |obj| {
            try out.append(a, '{');
            var first = true;
            var it = obj.iterator();
            while (it.next()) |e| {
                if (!first) try out.append(a, ',');
                first = false;
                try writeJsonString(a, out, e.key_ptr.*);
                try out.append(a, ':');
                try writeValue(a, out, e.value_ptr.*);
            }
            try out.append(a, '}');
        },
    }
}

pub fn valueToString(a: std.mem.Allocator, v: Value) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    try writeValue(a, &out, v);
    return out.items;
}

// ─── small helpers ──────────────────────────────────────────────────────────

fn getStr(v: Value, key: []const u8) ?[]const u8 {
    if (v != .object) return null;
    const f = v.object.get(key) orelse return null;
    return if (f == .string) f.string else null;
}

fn getInt(v: Value, key: []const u8) ?i64 {
    if (v != .object) return null;
    const f = v.object.get(key) orelse return null;
    return if (f == .integer) f.integer else null;
}

fn getArr(v: Value, key: []const u8) ?std.json.Array {
    if (v != .object) return null;
    const f = v.object.get(key) orelse return null;
    return if (f == .array) f.array else null;
}

fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0 or haystack.len < needle.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        var ok = true;
        for (needle, 0..) |c, j| {
            if (std.ascii.toLower(haystack[i + j]) != std.ascii.toLower(c)) {
                ok = false;
                break;
            }
        }
        if (ok) return true;
    }
    return false;
}

/// JS `String(v)` for the scalar shapes fixtures carry (key ids).
fn scalarToString(a: std.mem.Allocator, v: Value) ![]const u8 {
    return switch (v) {
        .string => |s| s,
        .integer => |x| try std.fmt.allocPrint(a, "{d}", .{x}),
        .float => |x| try std.fmt.allocPrint(a, "{d}", .{x}),
        .bool => |x| if (x) "true" else "false",
        .null => "null",
        else => try valueToString(a, v),
    };
}

fn scalarEql(x: Value, y: Value) bool {
    return switch (x) {
        .string => |s| y == .string and std.mem.eql(u8, s, y.string),
        .integer => |i| switch (y) {
            .integer => |j| i == j,
            .float => |f| @as(f64, @floatFromInt(i)) == f,
            else => false,
        },
        .float => |f| switch (y) {
            .float => |g| f == g,
            .integer => |j| f == @as(f64, @floatFromInt(j)),
            else => false,
        },
        .bool => |bv| y == .bool and bv == y.bool,
        .null => y == .null,
        else => false,
    };
}

// ─── versions (nextVersion / normalizeVersion / hlcVersion) ─────────────────

/// core.ts nextVersion: wall-clock ISO widened to micros, bumped past the last
/// stamp when the clock is tied or behind.
pub fn nextVersion(a: std.mem.Allocator, now_iso: []const u8, last: []const u8) ![]const u8 {
    // candidate = nowIso with trailing 'Z' stripped + "000Z"
    const base = if (now_iso.len > 0 and now_iso[now_iso.len - 1] == 'Z')
        now_iso[0 .. now_iso.len - 1]
    else
        now_iso;
    var candidate = try std.fmt.allocPrint(a, "{s}000Z", .{base});
    if (std.mem.order(u8, candidate, last) != .gt) {
        // micros = (int(last[-7..-1]) + 1) % 1_000_000 over last's prefix
        if (last.len >= 7) {
            const digits = last[last.len - 7 .. last.len - 1];
            const parsed = std.fmt.parseInt(u32, digits, 10) catch 0;
            const micros = (parsed + 1) % 1_000_000;
            candidate = try std.fmt.allocPrint(a, "{s}{d:0>6}Z", .{ last[0 .. last.len - 7], micros });
        }
    }
    return candidate;
}

/// core.ts normalizeVersion: canonical six fractional digits, or unchanged.
pub fn normalizeVersion(a: std.mem.Allocator, v: []const u8) ![]const u8 {
    // ^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$
    if (v.len < 20 or v[v.len - 1] != 'Z') return v;
    const head = v[0..19];
    const shape = "0000-00-00T00:00:00";
    for (head, shape) |c, s| {
        if (s == '0') {
            if (!std.ascii.isDigit(c)) return v;
        } else if (c != s) return v;
    }
    var frac: []const u8 = "";
    if (v.len > 20) {
        if (v[19] != '.') return v;
        frac = v[20 .. v.len - 1];
        if (frac.len == 0 or frac.len > 9) return v;
        for (frac) |c| if (!std.ascii.isDigit(c)) return v;
    } else if (v.len == 20) {
        // "....SSZ" with no fraction
    } else return v;
    var padded: [6]u8 = @splat('0');
    const n = @min(frac.len, 6);
    @memcpy(padded[0..n], frac[0..n]);
    return try std.fmt.allocPrint(a, "{s}.{s}Z", .{ head, padded });
}

pub fn maxVersion(x: []const u8, y: []const u8) []const u8 {
    return if (std.mem.order(u8, y, x) == .gt) y else x;
}

pub fn hlcVersion(a: std.mem.Allocator, now_iso: []const u8, last: []const u8, floor: []const u8) ![]const u8 {
    return nextVersion(a, now_iso, maxVersion(last, floor));
}

// ─── wire helpers ───────────────────────────────────────────────────────────

/// core.ts pgTsToWire: PG text timestamptz -> the CDC wire shape.
/// ^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?\+00(:00)?$
pub fn pgTsToWire(a: std.mem.Allocator, v: []const u8) ![]const u8 {
    if (v.len < 22) return v;
    const head = v[0..19];
    const shape = "0000-00-00 00:00:00";
    for (head, shape) |c, s| {
        if (s == '0') {
            if (!std.ascii.isDigit(c)) return v;
        } else if (c != s) return v;
    }
    var i: usize = 19;
    if (i < v.len and v[i] == '.') {
        i += 1;
        const start = i;
        while (i < v.len and std.ascii.isDigit(v[i])) i += 1;
        if (i == start) return v;
    }
    const rest = v[i..];
    if (!(std.mem.eql(u8, rest, "+00") or std.mem.eql(u8, rest, "+00:00"))) return v;
    return try std.fmt.allocPrint(a, "{s}T{s}Z", .{ v[0..10], v[11..i] });
}

/// core.ts lsnToNumber: "2/97B6ED8" -> hi * 2^32 + lo.
pub fn lsnToNumber(lsn: []const u8) i64 {
    const slash = std.mem.indexOfScalar(u8, lsn, '/') orelse return 0;
    const hi = std.fmt.parseInt(i64, lsn[0..slash], 16) catch 0;
    const lo = std.fmt.parseInt(i64, lsn[slash + 1 ..], 16) catch 0;
    return hi * 0x1_0000_0000 + lo;
}

/// core.ts subjectSafeToken: [.*>\s] -> '-'.
pub fn subjectSafeToken(a: std.mem.Allocator, v: []const u8) ![]const u8 {
    const out = try a.dupe(u8, v);
    for (out) |*c| switch (c.*) {
        '.', '*', '>', ' ', '\t', '\n', '\r', 0x0b, 0x0c => c.* = '-',
        else => {},
    };
    return out;
}

// ─── classifiers and gates ──────────────────────────────────────────────────

/// core.ts foreignKeyFailureKind over the error MESSAGE.
pub fn foreignKeyFailureKind(message: []const u8) ?[]const u8 {
    if (containsIgnoreCase(message, "foreign key mismatch")) return "mismatch";
    if (containsIgnoreCase(message, "FOREIGN KEY constraint failed")) return "missing-parent";
    if (containsIgnoreCase(message, "no such table: ")) return "missing-parent";
    return null;
}

/// core.ts tombstoned (PROTOCOL §7.5): the row is soft-deleted when its tombstone
/// column is present and not null; a table without one is never tombstoned.
pub fn tombstoned(tombstone_col: ?[]const u8, data: Value) bool {
    const tc = tombstone_col orelse return false;
    if (data != .object) return false;
    const v = data.object.get(tc) orelse return false;
    return v != .null;
}

/// core.ts seedGateDrops (findings 7 + 10).
pub fn seedGateDrops(ev: Value, anchor: Value) bool {
    const seed_seq = getInt(anchor, "seedSeq");
    const seed_stream = getStr(anchor, "seedStream");
    if (seed_seq != null and seed_stream != null) {
        const ev_stream = getStr(ev, "stream") orelse return false;
        const ev_seq = getInt(ev, "seq") orelse return false;
        return std.mem.eql(u8, ev_stream, seed_stream.?) and ev_seq <= seed_seq.?;
    }
    const seed_lsn = getInt(anchor, "seedLsn") orelse return false;
    const ev_lsn = getInt(ev, "lsn") orelse return false;
    return ev_lsn < seed_lsn;
}

/// core.ts advancePosition (D1).
pub fn advancePosition(stored: i64, batch: std.json.Array) i64 {
    var m = stored;
    for (batch.items) |s| {
        const seq: i64 = if (s == .integer) s.integer else 0;
        if (seq > m) m = seq;
    }
    return m;
}

// ─── the apply SQL builders ─────────────────────────────────────────────────

/// core.ts cdcValue: structured values become compact JSON text.
fn cdcValue(a: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .object, .array => .{ .string = try valueToString(a, v) },
        else => v,
    };
}

fn quotedJoin(a: std.mem.Allocator, names: []const []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (names, 0..) |n, i| {
        if (i > 0) try out.appendSlice(a, ", ");
        try out.append(a, '"');
        try out.appendSlice(a, n);
        try out.append(a, '"');
    }
    return out.items;
}

fn strArr(a: std.mem.Allocator, arr: std.json.Array) ![]const []const u8 {
    var out = try a.alloc([]const u8, arr.items.len);
    for (arr.items, 0..) |v, i| out[i] = if (v == .string) v.string else "";
    return out;
}

fn containsStr(list: []const []const u8, s: []const u8) bool {
    for (list) |x| if (std.mem.eql(u8, x, s)) return true;
    return false;
}

/// core.ts planKeyChange: {sql, params, oldKey, newKey} | null.
pub fn planKeyChange(a: std.mem.Allocator, table: []const u8, pk: []const []const u8, data: Value) !?Value {
    if (pk.len == 0 or data != .object) return null;
    var old_key = std.json.Array.init(a);
    var new_key = std.json.Array.init(a);
    var complete = true;
    var changed = false;
    for (pk) |c| {
        const old_field = try std.fmt.allocPrint(a, "old.{s}", .{c});
        const ov = data.object.get(old_field) orelse .null;
        const nv = data.object.get(c) orelse .null;
        if (ov == .null) complete = false;
        if (!scalarEql(ov, nv)) changed = true;
        try old_key.append(ov);
        try new_key.append(nv);
    }
    if (!complete or !changed) return null;
    var where: std.ArrayList(u8) = .empty;
    for (pk, 0..) |c, i| {
        if (i > 0) try where.appendSlice(a, " AND ");
        try where.appendSlice(a, try std.fmt.allocPrint(a, "\"{s}\" = ?", .{c}));
    }
    var obj: std.json.ObjectMap = .empty;
    try obj.put(a, "sql", .{ .string = try std.fmt.allocPrint(a, "DELETE FROM {s} WHERE {s}", .{ table, where.items }) });
    try obj.put(a, "params", .{ .array = old_key });
    try obj.put(a, "oldKey", .{ .array = old_key });
    try obj.put(a, "newKey", .{ .array = new_key });
    return .{ .object = obj };
}

/// core.ts planUpsert: {sql, params}.
pub fn planUpsert(a: std.mem.Allocator, table: []const u8, pk: []const []const u8, data: Value) !Value {
    var names: std.ArrayList([]const u8) = .empty;
    var params = std.json.Array.init(a);
    if (data == .object) {
        var it = data.object.iterator();
        while (it.next()) |e| {
            if (std.mem.startsWith(u8, e.key_ptr.*, "old.")) continue;
            try names.append(a, e.key_ptr.*);
            try params.append(try cdcValue(a, e.value_ptr.*));
        }
    }
    var updates: std.ArrayList(u8) = .empty;
    var first = true;
    for (names.items) |n| {
        if (containsStr(pk, n)) continue;
        if (!first) try updates.appendSlice(a, ", ");
        first = false;
        try updates.appendSlice(a, try std.fmt.allocPrint(a, "\"{s}\" = excluded.\"{s}\"", .{ n, n }));
    }
    var ph: std.ArrayList(u8) = .empty;
    for (names.items, 0..) |_, i| {
        if (i > 0) try ph.appendSlice(a, ", ");
        try ph.append(a, '?');
    }
    var sql: std.ArrayList(u8) = .empty;
    try sql.appendSlice(a, try std.fmt.allocPrint(a, "INSERT INTO {s} ({s}) VALUES ({s})", .{
        table, try quotedJoin(a, names.items), ph.items,
    }));
    if (pk.len > 0) {
        const conflict = try quotedJoin(a, pk);
        if (updates.items.len > 0) {
            try sql.appendSlice(a, try std.fmt.allocPrint(a, " ON CONFLICT({s}) DO UPDATE SET {s}", .{ conflict, updates.items }));
        } else {
            try sql.appendSlice(a, try std.fmt.allocPrint(a, " ON CONFLICT({s}) DO NOTHING", .{conflict}));
        }
    }
    var obj: std.json.ObjectMap = .empty;
    try obj.put(a, "sql", .{ .string = sql.items });
    try obj.put(a, "params", .{ .array = params });
    return .{ .object = obj };
}

/// core.ts pgArrayLiteral: a JSON array as PostgreSQL's array-literal text —
/// `{a,b}`, nested `{{1,2},{3}}`, elements quoted when they need it, null → NULL.
/// The form the CDC wire already carries for arrays; a replica on a PostgreSQL
/// engine binds its own optimistic write in it. Pinned in fixtures/pgArrayLiteral.
pub fn pgArrayLiteral(a: std.mem.Allocator, arr: std.json.Array) error{OutOfMemory}![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    try out.append(a, '{');
    for (arr.items, 0..) |item, i| {
        if (i > 0) try out.append(a, ',');
        switch (item) {
            .null => try out.appendSlice(a, "NULL"),
            .array => |inner| try out.appendSlice(a, try pgArrayLiteral(a, inner)),
            .object => try pgArrayQuote(a, &out, try valueToString(a, item)),
            .bool => |b| try out.appendSlice(a, if (b) "t" else "f"),
            .integer => |n| try out.appendSlice(a, try std.fmt.allocPrint(a, "{d}", .{n})),
            .float => |f| try out.appendSlice(a, try std.fmt.allocPrint(a, "{d}", .{f})),
            .number_string => |s| try out.appendSlice(a, s),
            .string => |s| try pgArrayQuote(a, &out, s),
        }
    }
    try out.append(a, '}');
    return out.items;
}

fn pgArrayQuote(a: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8) error{OutOfMemory}!void {
    var needs = s.len == 0 or std.ascii.eqlIgnoreCase(s, "NULL");
    for (s) |ch| {
        if (ch == ',' or ch == '{' or ch == '}' or ch == '"' or ch == '\\' or std.ascii.isWhitespace(ch)) needs = true;
    }
    if (!needs) return out.appendSlice(a, s);
    try out.append(a, '"');
    for (s) |ch| {
        if (ch == '"' or ch == '\\') try out.append(a, '\\');
        try out.append(a, ch);
    }
    try out.append(a, '"');
}

/// core.ts planUpdate: the UPDATE-shaped plan for a row that exists — only the
/// payload's columns are set, so a partial payload is fine. Null without a full
/// key or without a non-key column. See core.ts for why it sits beside planUpsert.
pub fn planUpdate(a: std.mem.Allocator, table: []const u8, pk: []const []const u8, data: Value) !?Value {
    if (pk.len == 0 or data != .object) return null;
    var key = std.json.Array.init(a);
    for (pk) |c| {
        const v = data.object.get(c) orelse return null;
        if (v == .null) return null;
        try key.append(v);
    }
    var sets: std.ArrayList(u8) = .empty;
    var params = std.json.Array.init(a);
    var it = data.object.iterator();
    while (it.next()) |e| {
        const k = e.key_ptr.*;
        if (std.mem.startsWith(u8, k, "old.") or containsStr(pk, k)) continue;
        if (params.items.len > 0) try sets.appendSlice(a, ", ");
        try sets.appendSlice(a, try std.fmt.allocPrint(a, "\"{s}\" = ?", .{k}));
        try params.append(try cdcValue(a, e.value_ptr.*));
    }
    if (params.items.len == 0) return null;
    for (key.items) |v| try params.append(v);
    var where: std.ArrayList(u8) = .empty;
    for (pk, 0..) |c, i| {
        if (i > 0) try where.appendSlice(a, " AND ");
        try where.appendSlice(a, try std.fmt.allocPrint(a, "\"{s}\" = ?", .{c}));
    }
    var obj: std.json.ObjectMap = .empty;
    try obj.put(a, "sql", .{ .string = try std.fmt.allocPrint(a, "UPDATE {s} SET {s} WHERE {s}", .{ table, sets.items, where.items }) });
    try obj.put(a, "params", .{ .array = params });
    return .{ .object = obj };
}

/// core.ts planExists: `SELECT 1 … LIMIT 1` by the full key, or null.
pub fn planExists(a: std.mem.Allocator, table: []const u8, pk: []const []const u8, data: Value) !?Value {
    if (pk.len == 0 or data != .object) return null;
    var params = std.json.Array.init(a);
    var where: std.ArrayList(u8) = .empty;
    for (pk, 0..) |c, i| {
        const v = data.object.get(c) orelse return null;
        if (v == .null) return null;
        try params.append(v);
        if (i > 0) try where.appendSlice(a, " AND ");
        try where.appendSlice(a, try std.fmt.allocPrint(a, "\"{s}\" = ?", .{c}));
    }
    var obj: std.json.ObjectMap = .empty;
    try obj.put(a, "sql", .{ .string = try std.fmt.allocPrint(a, "SELECT 1 FROM {s} WHERE {s} LIMIT 1", .{ table, where.items }) });
    try obj.put(a, "params", .{ .array = params });
    return .{ .object = obj };
}

/// core.ts planDelete: {sql, params} | null.
pub fn planDelete(a: std.mem.Allocator, table: []const u8, pk: []const []const u8, data: Value) !?Value {
    if (pk.len == 0 or data != .object) return null;
    var params = std.json.Array.init(a);
    for (pk) |c| {
        const v = data.object.get(c) orelse return null;
        if (v == .null) return null;
        try params.append(v);
    }
    var where: std.ArrayList(u8) = .empty;
    for (pk, 0..) |c, i| {
        if (i > 0) try where.appendSlice(a, " AND ");
        try where.appendSlice(a, try std.fmt.allocPrint(a, "\"{s}\" = ?", .{c}));
    }
    var obj: std.json.ObjectMap = .empty;
    try obj.put(a, "sql", .{ .string = try std.fmt.allocPrint(a, "DELETE FROM {s} WHERE {s}", .{ table, where.items }) });
    try obj.put(a, "params", .{ .array = params });
    return .{ .object = obj };
}

/// core.ts chainUpsertSql.
pub fn chainUpsertSql(a: std.mem.Allocator, table: []const u8, cols: []const []const u8, pk: []const []const u8, version_col: ?[]const u8) ![]const u8 {
    var ph: std.ArrayList(u8) = .empty;
    for (cols, 0..) |_, i| {
        if (i > 0) try ph.appendSlice(a, ", ");
        try ph.append(a, '?');
    }
    var sets: std.ArrayList(u8) = .empty;
    var first = true;
    for (cols) |c| {
        if (containsStr(pk, c)) continue;
        if (!first) try sets.appendSlice(a, ", ");
        first = false;
        try sets.appendSlice(a, try std.fmt.allocPrint(a, "\"{s}\" = excluded.\"{s}\"", .{ c, c }));
    }
    var sql: std.ArrayList(u8) = .empty;
    try sql.appendSlice(a, try std.fmt.allocPrint(a, "INSERT INTO {s} ({s}) VALUES ({s})", .{
        table, try quotedJoin(a, cols), ph.items,
    }));
    const conflict = try quotedJoin(a, pk);
    if (sets.items.len > 0) {
        try sql.appendSlice(a, try std.fmt.allocPrint(a, " ON CONFLICT({s}) DO UPDATE SET {s}", .{ conflict, sets.items }));
        if (version_col) |vc| {
            try sql.appendSlice(a, try std.fmt.allocPrint(a, " WHERE excluded.\"{s}\" > {s}.\"{s}\"", .{ vc, table, vc }));
        }
    } else {
        try sql.appendSlice(a, try std.fmt.allocPrint(a, " ON CONFLICT({s}) DO NOTHING", .{conflict}));
    }
    return sql.items;
}

/// core.ts chainRowParams: objects -> compact JSON, strings -> pgTsToWire.
pub fn chainRowParams(a: std.mem.Allocator, row: std.json.Array) !Value {
    var out = std.json.Array.init(a);
    for (row.items) |v| {
        switch (v) {
            .object, .array => try out.append(.{ .string = try valueToString(a, v) }),
            .string => |s| try out.append(.{ .string = try pgTsToWire(a, s) }),
            else => try out.append(v),
        }
    }
    return .{ .array = out };
}

// ─── the mutate() envelope ──────────────────────────────────────────────────

/// core.ts buildMutation: the whole envelope in one call.
pub fn buildMutation(a: std.mem.Allocator, args: Value) !Value {
    const principal = getStr(args, "principal") orelse "";
    const client_id = getStr(args, "clientId") orelse "";
    const table = getStr(args, "table") orelse "";
    const op = getStr(args, "op") orelse "";
    const version = getStr(args, "version") orelse "";
    const key = if (args == .object) args.object.get("key") orelse .null else Value.null;
    const values = if (args == .object) args.object.get("values") else null;
    const pk = try strArr(a, getArr(args, "pkCols") orelse std.json.Array.init(a));

    // id: pk values joined with '|', JS String() semantics
    var id: std.ArrayList(u8) = .empty;
    for (pk, 0..) |c, i| {
        if (i > 0) try id.append(a, '|');
        const v = if (key == .object) key.object.get(c) orelse .null else Value.null;
        try id.appendSlice(a, try scalarToString(a, v));
    }

    // op lowercased for the subject
    const op_lower = try a.dupe(u8, op);
    for (op_lower) |*c| c.* = std.ascii.toLower(c.*);

    const is_delete = std.mem.eql(u8, op, "DELETE");

    var payload: std.json.ObjectMap = .empty;
    try payload.put(a, "key", key);
    try payload.put(a, "version", .{ .string = version });
    try payload.put(a, "client_id", .{ .string = client_id });
    if (!is_delete) {
        const data: Value = if (values) |v| (if (v == .null) Value{ .object = .empty } else v) else .{ .object = .empty };
        try payload.put(a, "data", data);
    }

    var optimistic: std.json.ObjectMap = .empty;
    try optimistic.put(a, "table", .{ .string = table });
    try optimistic.put(a, "operation", .{ .string = op });
    try optimistic.put(a, "data", if (is_delete) key else payload.get("data").?);
    try optimistic.put(a, "lsn", .{ .integer = 9007199254740991 });
    try optimistic.put(a, "optimistic", .{ .bool = true });

    var out: std.json.ObjectMap = .empty;
    // The subject prefix is grammar.json's `subjects.mutations_prefix`; the shell passes
    // it as `mutationsPrefix`. Absent (the fixtures) → the protocol default.
    const prefix = getStr(args, "mutationsPrefix") orelse "mutation";
    try out.put(a, "subject", .{ .string = try std.fmt.allocPrint(a, "{s}.{s}.{s}.{s}", .{ prefix, principal, table, op_lower }) });
    try out.put(a, "msgId", .{ .string = try subjectSafeToken(a, try std.fmt.allocPrint(a, "{s}-{s}-{s}-{s}", .{ client_id, table, id.items, version })) });
    try out.put(a, "id", .{ .string = id.items });
    try out.put(a, "payload", .{ .object = payload });
    try out.put(a, "optimistic", .{ .object = optimistic });
    return .{ .object = out };
}

// ─── chain planning (§10n) ──────────────────────────────────────────────────

/// core.ts planFromManifest.
pub fn planFromManifest(a: std.mem.Allocator, man: Value, watermark: ?[]const u8) !Value {
    var plan = std.json.Array.init(a);
    const deltas: std.json.Array = getArr(man, "deltas") orelse std.json.Array.init(a);

    var applicable = std.json.Array.init(a);
    for (deltas.items) |d| {
        const cutoff = getStr(d, "cutoff") orelse "";
        if (watermark == null or std.mem.order(u8, cutoff, watermark.?) == .gt) {
            try applicable.append(d);
        }
    }
    const reaches = watermark != null and
        (applicable.items.len == 0 or
        std.mem.order(u8, getStr(applicable.items[0], "prev_cutoff") orelse "", watermark.?) != .gt);

    if (reaches) {
        for (applicable.items) |d| try plan.append(try deltaStep(a, d));
        return .{ .array = plan };
    }
    const full = if (man == .object) man.object.get("full") else null;
    if (full == null or full.? == .null) return .{ .array = plan };
    var fstep: std.json.ObjectMap = .empty;
    try fstep.put(a, "name", .{ .string = getStr(full.?, "object") orelse "" });
    try fstep.put(a, "kind", .{ .string = "full" });
    try plan.append(.{ .object = fstep });
    const full_gen = getInt(full.?, "gen") orelse 0;
    for (deltas.items) |d| {
        if ((getInt(d, "gen") orelse 0) > full_gen) try plan.append(try deltaStep(a, d));
    }
    return .{ .array = plan };
}

/// A delta step carries its dictionary name when the manifest names one (§10x).
fn deltaStep(a: std.mem.Allocator, d: Value) !Value {
    var step: std.json.ObjectMap = .empty;
    try step.put(a, "name", .{ .string = getStr(d, "object") orelse "" });
    try step.put(a, "kind", .{ .string = "delta" });
    if (getStr(d, "dict")) |dn| try step.put(a, "dict", .{ .string = dn });
    return .{ .object = step };
}

/// core.ts fullPredatesReplica (D2's destruction guard).
pub fn fullPredatesReplica(man: Value, plan: std.json.Array, stored_seq: i64) bool {
    var has_full = false;
    for (plan.items) |step| {
        if (std.mem.eql(u8, getStr(step, "kind") orelse "", "full")) has_full = true;
    }
    if (!has_full) return false;
    const cutoff = getInt(man, "cutoff_seq") orelse return false;
    if (cutoff <= 0) return false;
    if (getStr(man, "cdc_stream") == null) return false;
    return cutoff < stored_seq;
}

// ─── the gap rule and seeding scope (D2) ────────────────────────────────────

pub fn streamHasGap(first_seq: i64, stored: i64) bool {
    return stored == 0 or (first_seq > 0 and stored < first_seq - 1);
}

/// core.ts scopeSeeding: {gapped, tablesToSeed}.
pub fn scopeSeeding(a: std.mem.Allocator, streams: Value, tables: Value) !Value {
    var gapped = std.json.Array.init(a);
    if (streams == .object) {
        var it = streams.object.iterator();
        while (it.next()) |e| {
            const first_seq = getInt(e.value_ptr.*, "firstSeq") orelse 0;
            const stored = getInt(e.value_ptr.*, "stored") orelse 0;
            if (streamHasGap(first_seq, stored)) try gapped.append(.{ .string = e.key_ptr.* });
        }
    }
    var to_seed = std.json.Array.init(a);
    if (tables == .object) {
        var it = tables.object.iterator();
        while (it.next()) |e| {
            const route = getStr(e.value_ptr.*, "route") orelse "";
            const seeded = if (e.value_ptr.* == .object)
                (if (e.value_ptr.object.get("seeded")) |s| (s == .bool and s.bool) else false)
            else
                false;
            var route_gapped = false;
            for (gapped.items) |g| {
                if (std.mem.eql(u8, g.string, route)) route_gapped = true;
            }
            if (route_gapped or !seeded) try to_seed.append(.{ .string = e.key_ptr.* });
        }
    }
    var out: std.json.ObjectMap = .empty;
    try out.put(a, "gapped", .{ .array = gapped });
    try out.put(a, "tablesToSeed", .{ .array = to_seed });
    return .{ .object = out };
}

/// core.ts outboxWatermarkGate — PROTOCOL.md §MUST 6.
///
/// A queued mutation older than the GC watermark cannot be sent: the tombstone that
/// would have overruled it has been reaped, so the write lands as a resurrection of a
/// row somebody deleted. It is the one failure LWW cannot catch, because every version
/// the bridge would compare has already been discarded.
///
/// Conservative at both unknowns, and the TS core is the spec here (fixtures
/// `outboxWatermark`): a null/empty watermark refuses NOTHING — the table is only
/// published if the DBA put it there, so "not known" is an ordinary deployment — and an
/// entry with no version is never refused, because the server's own version guard still
/// fronts it. The comparison is `<=`: the watermark is the OLDEST STANDING tombstone, so
/// a mutation stamped exactly at it may already have lost its overruling tombstone.
///
/// String comparison AFTER normalizeVersion, exactly as planFromManifest does with
/// cutoffs — PG trims trailing fractional zeros, so `.5Z` and `.50001Z` order wrongly
/// until both are padded to six digits.
pub fn outboxWatermarkGate(a: std.mem.Allocator, entries: std.json.Array, watermark: ?[]const u8) !Value {
    var send = std.json.Array.init(a);
    var refuse = std.json.Array.init(a);

    const mark: ?[]const u8 = if (watermark) |w|
        (if (w.len == 0) null else try normalizeVersion(a, w))
    else
        null;

    for (entries.items) |e| {
        const msg_id = getStr(e, "msgId") orelse "";
        const ver: ?[]const u8 = getStr(e, "version");
        var refused = false;
        if (mark) |m| {
            if (ver) |v| {
                if (v.len != 0) {
                    const nv = try normalizeVersion(a, v);
                    refused = std.mem.order(u8, nv, m) != .gt;
                }
            }
        }
        if (refused) try refuse.append(.{ .string = msg_id }) else try send.append(.{ .string = msg_id });
    }

    var out: std.json.ObjectMap = .empty;
    try out.put(a, "send", .{ .array = send });
    try out.put(a, "refuse", .{ .array = refuse });
    return .{ .object = out };
}

// ─── the schema migration planner (§10s 2b) ─────────────────────────────────

fn colName(c: Value) []const u8 {
    return getStr(c, "name") orelse "";
}

/// core.ts columnDdl.
pub fn columnDdl(a: std.mem.Allocator, col: Value, pk: []const []const u8) ![]const u8 {
    const name = colName(col);
    const typ = getStr(col, "type") orelse "";
    const required = if (col == .object)
        (if (col.object.get("required")) |r| (r == .bool and r.bool) else false)
    else
        false;
    const is_pk = containsStr(pk, name);
    const inline_pk = pk.len == 1;
    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(a, try std.fmt.allocPrint(a, "\"{s}\" {s}", .{ name, typ }));
    if (is_pk or required) try out.appendSlice(a, " NOT NULL");
    if (inline_pk and std.mem.eql(u8, name, pk[0])) try out.appendSlice(a, " PRIMARY KEY");
    return out.items;
}

/// core.ts fkClausesFor: malformed entries dropped, not guessed.
pub fn fkClausesFor(a: std.mem.Allocator, fks: std.json.Array) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (fks.items) |f| {
        const references = getStr(f, "references") orelse continue;
        const cols = getArr(f, "columns") orelse continue;
        const parents = getArr(f, "parent_columns") orelse continue;
        if (cols.items.len == 0 or parents.items.len != cols.items.len) continue;
        try out.appendSlice(a, ", FOREIGN KEY (");
        try out.appendSlice(a, try quotedJoin(a, try strArrPub(a, cols)));
        try out.appendSlice(a, try std.fmt.allocPrint(a, ") REFERENCES {s} (", .{references}));
        try out.appendSlice(a, try quotedJoin(a, try strArrPub(a, parents)));
        try out.append(a, ')');
    }
    return out.items;
}

pub fn strArrPub(a: std.mem.Allocator, arr: std.json.Array) ![]const []const u8 {
    return strArr(a, arr);
}

fn tableBody(a: std.mem.Allocator, cols: std.json.Array, pk: []const []const u8, fks: std.json.Array) ![]const u8 {
    var body: std.ArrayList(u8) = .empty;
    for (cols.items, 0..) |c, i| {
        if (i > 0) try body.appendSlice(a, ", ");
        try body.appendSlice(a, try columnDdl(a, c, pk));
    }
    if (pk.len > 1) {
        try body.appendSlice(a, try std.fmt.allocPrint(a, ", PRIMARY KEY ({s})", .{try quotedJoin(a, pk)}));
    }
    try body.appendSlice(a, try fkClausesFor(a, fks));
    return body.items;
}

fn sqlStep(a: std.mem.Allocator, sql: []const u8) !Value {
    var obj: std.json.ObjectMap = .empty;
    try obj.put(a, "sql", .{ .string = sql });
    try obj.put(a, "params", .{ .array = std.json.Array.init(a) });
    return .{ .object = obj };
}

/// core.ts createTableSteps.
pub fn createTableSteps(a: std.mem.Allocator, table: []const u8, cols: std.json.Array, pk: []const []const u8, fks: std.json.Array) !Value {
    var steps = std.json.Array.init(a);
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "DROP TABLE IF EXISTS {s};", .{table})));
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "CREATE TABLE {s} ({s});", .{ table, try tableBody(a, cols, pk, fks) })));
    return .{ .array = steps };
}

/// core.ts rebuildSteps (finding 9's legitimate drop/recreate).
pub fn rebuildSteps(a: std.mem.Allocator, table: []const u8, cols: std.json.Array, pk: []const []const u8, fks: std.json.Array, existing: []const []const u8) !Value {
    const tmp = try std.fmt.allocPrint(a, "{s}__migrating", .{table});
    var steps = std.json.Array.init(a);
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "DROP TABLE IF EXISTS {s};", .{tmp})));
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "CREATE TABLE {s} ({s});", .{ tmp, try tableBody(a, cols, pk, fks) })));
    var common: std.ArrayList([]const u8) = .empty;
    for (cols.items) |c| {
        const n = colName(c);
        if (containsStr(existing, n)) try common.append(a, n);
    }
    if (common.items.len > 0) {
        const list = try quotedJoin(a, common.items);
        try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "INSERT INTO {s} ({s}) SELECT {s} FROM {s};", .{ tmp, list, list, table })));
    }
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "DROP TABLE IF EXISTS {s};", .{table})));
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "ALTER TABLE {s} RENAME TO {s};", .{ tmp, table })));
    return .{ .array = steps };
}

/// core.ts diffColumns (rename-aware; §1.2 as behaviour).
pub fn diffColumns(a: std.mem.Allocator, existing: ?[]const []const u8, wanted: []const []const u8, renamed: Value) !Value {
    var renames = std.json.Array.init(a);
    var added = std.json.Array.init(a);
    var removed = std.json.Array.init(a);
    if (existing) |ex| {
        // renames: entries {to: from} where from exists and to does not
        var from_list: std.ArrayList([]const u8) = .empty;
        var to_list: std.ArrayList([]const u8) = .empty;
        if (renamed == .object) {
            var it = renamed.object.iterator();
            while (it.next()) |e| {
                const to = e.key_ptr.*;
                const from = if (e.value_ptr.* == .string) e.value_ptr.string else continue;
                if (containsStr(ex, from) and !containsStr(ex, to)) {
                    var pair = std.json.Array.init(a);
                    try pair.append(.{ .string = from });
                    try pair.append(.{ .string = to });
                    try renames.append(.{ .array = pair });
                    try from_list.append(a, from);
                    try to_list.append(a, to);
                }
            }
        }
        var effective: std.ArrayList([]const u8) = .empty;
        for (ex) |n| {
            var mapped: []const u8 = n;
            for (from_list.items, 0..) |f, i| {
                if (std.mem.eql(u8, f, n)) mapped = to_list.items[i];
            }
            try effective.append(a, mapped);
        }
        for (wanted) |n| if (!containsStr(effective.items, n)) try added.append(.{ .string = n });
        for (effective.items) |n| if (!containsStr(wanted, n)) try removed.append(.{ .string = n });
    }
    var out: std.json.ObjectMap = .empty;
    try out.put(a, "renames", .{ .array = renames });
    try out.put(a, "added", .{ .array = added });
    try out.put(a, "removed", .{ .array = removed });
    return .{ .object = out };
}

fn collapseWs(a: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    var in_ws = false;
    for (s) |c| {
        if (std.ascii.isWhitespace(c)) {
            in_ws = true;
        } else {
            if (in_ws and out.items.len > 0) try out.append(a, ' ');
            in_ws = false;
            try out.append(a, c);
        }
    }
    return out.items;
}

/// core.ts fkTextDiffers.
pub fn fkTextDiffers(a: std.mem.Allocator, ddl: []const u8, fk_clauses: []const u8) !bool {
    if (ddl.len == 0) return false;
    const has_any = containsIgnoreCase(ddl, "FOREIGN KEY");
    if (fk_clauses.len == 0) return has_any;
    // strip leading ",\s*" then collapse whitespace
    var want = fk_clauses;
    if (want.len > 0 and want[0] == ',') {
        want = want[1..];
        while (want.len > 0 and std.ascii.isWhitespace(want[0])) want = want[1..];
    }
    const want_n = try collapseWs(a, want);
    const ddl_n = try collapseWs(a, ddl);
    return std.mem.indexOf(u8, ddl_n, want_n) == null;
}

/// core.ts viewSteps: the plumbing-column exclusion.
pub const view_excluded = [_][]const u8{ "uid", "inserted_at", "updated_at", "metadata" };

pub fn viewSteps(a: std.mem.Allocator, table: []const u8, names: []const []const u8) !Value {
    var kept: std.ArrayList([]const u8) = .empty;
    for (names) |n| if (!containsStr(&view_excluded, n)) try kept.append(a, n);
    var steps = std.json.Array.init(a);
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "DROP VIEW IF EXISTS {s}_view;", .{table})));
    if (kept.items.len > 0) {
        try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "CREATE VIEW {s}_view AS SELECT {s} FROM {s};", .{ table, try quotedJoin(a, kept.items), table })));
    }
    return .{ .array = steps };
}

/// core.ts indexSyncPlan.
pub fn indexSyncPlan(a: std.mem.Allocator, table: []const u8, have: []const []const u8, want: std.json.Array) !Value {
    var drops = std.json.Array.init(a);
    var creates = std.json.Array.init(a);
    for (have) |n| {
        var still = false;
        for (want.items) |ix| {
            if (std.mem.eql(u8, getStr(ix, "name") orelse "", n)) still = true;
        }
        if (!still) try drops.append(try sqlStep(a, try std.fmt.allocPrint(a, "DROP INDEX IF EXISTS \"{s}\";", .{n})));
    }
    for (want.items) |ix| {
        const name = getStr(ix, "name") orelse "";
        const cols = getArr(ix, "columns") orelse continue;
        if (name.len == 0 or cols.items.len == 0) continue;
        if (containsStr(have, name)) continue;
        const unique = if (ix == .object)
            (if (ix.object.get("unique")) |u| (u == .bool and u.bool) else false)
        else
            false;
        try creates.append(try sqlStep(a, try std.fmt.allocPrint(a, "CREATE {s}INDEX IF NOT EXISTS \"{s}\" ON {s} ({s});", .{
            @as([]const u8, if (unique) "UNIQUE " else ""), name, table, try quotedJoin(a, try strArr(a, cols)),
        })));
    }
    var out: std.json.ObjectMap = .empty;
    try out.put(a, "drops", .{ .array = drops });
    try out.put(a, "creates", .{ .array = creates });
    return .{ .object = out };
}
