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

/// §10ja: a plan step carries `"sorted": true` when the manifest entry does — the
/// object's rows are in primary-key order and apply without staging or sorting.
fn putSorted(a: std.mem.Allocator, step: *std.json.ObjectMap, entry: Value) !void {
    if (entry != .object) return;
    if (entry.object.get("sorted")) |v| if (v == .bool and v.bool) try step.put(a, "sorted", .{ .bool = true });
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
    // §10ja (core.ts): an event with its stream sequence is gated by the sequence anchor
    // alone — never by lsn, whose in-flight transactions commit after the snapshot.
    if (getInt(ev, "seq")) |ev_seq| if (getStr(ev, "stream")) |ev_stream| {
        const seed_seq = getInt(anchor, "seedSeq") orelse return false;
        const seed_stream = getStr(anchor, "seedStream") orelse return false;
        return std.mem.eql(u8, ev_stream, seed_stream) and ev_seq <= seed_seq;
    };
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

/// core.ts caughtUpPosition (§10ja): the position of a caught-up consumer — the
/// stream's `last_seq`, read before the consumer's info, when nothing is pending,
/// nothing is unacked and nothing handed over is past `pos` (`delivered_count` 0: a
/// fresh consumer's `delivered` is its start - 1, no message). Otherwise `pos`.
/// §10jh: and never over a PRUNED range. `first_seq` past `pos + 1` means the stream
/// dropped messages after the position before this client saw them; a filtered consumer
/// then has nothing pending and looks caught up, and jumping to `last_seq` erased the
/// hole (a follower behind a slow link: 330,000 rows missing, the tail idle and "fine").
/// Stay, and let the gap rule heal it from the chain. `first_seq` 0: unknown, the old rule.
pub fn caughtUpPosition(pos: u64, first_seq: u64, last_seq: u64, num_pending: u64, num_ack_pending: u64, delivered_count: u64, delivered: u64) u64 {
    if (last_seq <= pos) return pos;
    if (first_seq > pos + 1) return pos;
    if (num_pending != 0 or num_ack_pending != 0 or (delivered_count > 0 and delivered > pos)) return pos;
    return last_seq;
}

// ─── the apply SQL builders ─────────────────────────────────────────────────

/// §10ex: the one object that is NOT JSON text — bytes, as `{"$bin": "<base64>"}`
/// (client.zig binMarker). It passes through every structured→text conversion here
/// and binds as a BLOB in the shell (client.zig jsonToStorage). core.ts isBytes.
pub fn isBinMarker(v: Value) bool {
    if (v != .object or v.object.count() != 1) return false;
    const s = v.object.get("$bin") orelse return false;
    return s == .string;
}

/// core.ts cdcValue: structured values become compact JSON text; bytes stay bytes.
fn cdcValue(a: std.mem.Allocator, v: Value) !Value {
    if (isBinMarker(v)) return v;
    return switch (v) {
        .object, .array => .{ .string = try valueToString(a, v) },
        else => v,
    };
}

pub fn quotedJoin(a: std.mem.Allocator, names: []const []const u8) ![]const u8 {
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

/// core.ts heartbeatPayload (PROTOCOL §9): the fleet heartbeat, byte-identical across
/// cores — stream keys sorted bytewise, fixed key order, integers verbatim. Pinned in
/// fixtures/heartbeat. `names`/`seqs` are parallel; sorted here, not by the caller.
pub fn heartbeatPayload(a: std.mem.Allocator, principal: []const u8, tenant: []const u8, ts: i64, names: []const []const u8, seqs: []const u64) ![]const u8 {
    const idx = try a.alloc(usize, names.len);
    for (idx, 0..) |*x, i| x.* = i;
    std.mem.sort(usize, idx, names, struct {
        fn lt(ctx: []const []const u8, x: usize, y: usize) bool {
            return std.mem.lessThan(u8, ctx[x], ctx[y]);
        }
    }.lt);
    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(a, "{\"principal\":");
    try writeJsonString(a, &out, principal);
    try out.appendSlice(a, ",\"tenant\":");
    try writeJsonString(a, &out, tenant);
    try out.appendSlice(a, try std.fmt.allocPrint(a, ",\"ts\":{d},\"streams\":{{", .{ts}));
    for (idx, 0..) |i, n| {
        if (n > 0) try out.append(a, ',');
        try writeJsonString(a, &out, names[i]);
        try out.appendSlice(a, try std.fmt.allocPrint(a, ":{d}", .{seqs[i]}));
    }
    try out.appendSlice(a, "}}");
    return out.items;
}

/// core.ts pgArrayLiteral: a JSON array as PostgreSQL's array-literal text —
/// `{a,b}`, nested `{{1,2},{3}}`, elements quoted when they need it, null → NULL.
/// The form the CDC wire already carries for arrays; a replica on a PostgreSQL
/// engine binds its own optimistic write in it. Pinned in fixtures/pgArrayLiteral.
/// §10fg: the pgvector wire shapes — the bridge's normalised BLOBs: `vector` as
/// little-endian float32s, `halfvec` as little-endian float16s, `sparsevec` as u32 dim,
/// u32 nnz, nnz u32 indices (0-based), nnz float32s, `bit` as packed bits MSB first.
/// A SQLite replica stores them as they are; a PostgreSQL replica binds pgvector's
/// text form, which this renders. `bits` is a bit(n) column's declared length (0 when
/// unknown) — the wire pads to a byte and bit(3) refuses eight.
pub const VecKind = enum { vector, halfvec, sparsevec, bit };

pub fn vecLiteral(a: std.mem.Allocator, kind: VecKind, bytes: []const u8, bits: u32) error{ OutOfMemory, InvalidVector }![]const u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(a);
    switch (kind) {
        .vector, .halfvec => {
            const size: usize = if (kind == .vector) 4 else 2;
            if (bytes.len % size != 0) return error.InvalidVector;
            try out.append(a, '[');
            var i: usize = 0;
            while (i < bytes.len) : (i += size) {
                if (i > 0) try out.append(a, ',');
                if (kind == .vector) {
                    try out.print(a, "{d}", .{@as(f32, @bitCast(std.mem.readInt(u32, bytes[i..][0..4], .little)))});
                } else {
                    try out.print(a, "{d}", .{@as(f16, @bitCast(std.mem.readInt(u16, bytes[i..][0..2], .little)))});
                }
            }
            try out.append(a, ']');
        },
        .sparsevec => {
            if (bytes.len < 8) return error.InvalidVector;
            const dim = std.mem.readInt(u32, bytes[0..4], .little);
            const nnz: usize = std.mem.readInt(u32, bytes[4..8], .little);
            if (bytes.len != 8 + nnz * 8) return error.InvalidVector;
            try out.append(a, '{');
            for (0..nnz) |k| {
                if (k > 0) try out.append(a, ',');
                const idx = std.mem.readInt(u32, bytes[8 + k * 4 ..][0..4], .little);
                const val: f32 = @bitCast(std.mem.readInt(u32, bytes[8 + nnz * 4 + k * 4 ..][0..4], .little));
                try out.print(a, "{d}:{d}", .{ idx + 1, val });
            }
            try out.print(a, "}}/{d}", .{dim});
        },
        .bit => {
            const all = bytes.len * 8;
            const n: usize = if (bits > 0 and bits < all) bits else all;
            for (0..n) |i| try out.append(a, if ((bytes[i / 8] >> @intCast(7 - (i % 8))) & 1 == 1) '1' else '0');
        },
    }
    return out.toOwnedSlice(a);
}

test "vecLiteral: the wire shapes back to pgvector's text (§10fg)" {
    const a = std.testing.allocator;
    const v = try vecLiteral(a, .vector, &[_]u8{ 0, 0, 0x80, 0x3f, 0, 0, 0x20, 0xc0 }, 0);
    defer a.free(v);
    try std.testing.expectEqualStrings("[1,-2.5]", v);
    const h = try vecLiteral(a, .halfvec, &[_]u8{ 0x00, 0x3e, 0x00, 0xbc }, 0);
    defer a.free(h);
    try std.testing.expectEqualStrings("[1.5,-1]", h);
    const s = try vecLiteral(a, .sparsevec, &[_]u8{ 5, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0x3f }, 0);
    defer a.free(s);
    try std.testing.expectEqualStrings("{3:0.5}/5", s);
    const b = try vecLiteral(a, .bit, &[_]u8{0xa0}, 3);
    defer a.free(b);
    try std.testing.expectEqualStrings("101", b);
    try std.testing.expectError(error.InvalidVector, vecLiteral(a, .vector, &[_]u8{ 1, 2, 3 }, 0));
}

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
/// §10fe: the delta upsert from the COPY's temporary table — `INSERT INTO t (cols)
/// SELECT cols FROM _zbz_copy WHERE true ON CONFLICT …`, the same clauses as
/// chainUpsertSql; `WHERE true` disambiguates INSERT … SELECT … ON CONFLICT.
pub fn pgUpsertFromCopySql(a: std.mem.Allocator, table: []const u8, cols: []const []const u8, pk: []const []const u8, version_col: ?[]const u8) ![]const u8 {
    var sets: std.ArrayList(u8) = .empty;
    var first = true;
    for (cols) |c| {
        if (containsStr(pk, c)) continue;
        if (!first) try sets.appendSlice(a, ", ");
        first = false;
        try sets.appendSlice(a, try std.fmt.allocPrint(a, "\"{s}\" = excluded.\"{s}\"", .{ c, c }));
    }
    const conflict = try quotedJoin(a, pk);
    var tail: std.ArrayList(u8) = .empty;
    if (sets.items.len > 0) {
        try tail.appendSlice(a, try std.fmt.allocPrint(a, " ON CONFLICT({s}) DO UPDATE SET {s}", .{ conflict, sets.items }));
        if (version_col) |vc| try tail.appendSlice(a, try std.fmt.allocPrint(a, " WHERE excluded.\"{s}\" > {s}.\"{s}\"", .{ vc, table, vc }));
    } else {
        try tail.appendSlice(a, try std.fmt.allocPrint(a, " ON CONFLICT({s}) DO NOTHING", .{conflict}));
    }
    return std.fmt.allocPrint(a, "INSERT INTO {s} ({s}) SELECT {s} FROM _zbz_copy WHERE true{s}", .{ table, try quotedJoin(a, cols), try quotedJoin(a, cols), tail.items });
}

/// §10ez: the INSERT a full's rows take — the table was emptied first, so no conflict
/// clause and no version guard: every row is new.
pub fn chainInsertSql(a: std.mem.Allocator, table: []const u8, cols: []const []const u8) ![]const u8 {
    var ph: std.ArrayList(u8) = .empty;
    for (cols, 0..) |_, i| {
        if (i > 0) try ph.appendSlice(a, ", ");
        try ph.append(a, '?');
    }
    return std.fmt.allocPrint(a, "INSERT INTO {s} ({s}) VALUES ({s})", .{ table, try quotedJoin(a, cols), ph.items });
}

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

/// core.ts cdcBulkSql: the chain's `json_each` upsert with NO version guard (§10gp —
/// two updates of one row in one PostgreSQL transaction carry the same version; the
/// stream's order is the truth, and SQLite applies the SELECT's rows in order, so the
/// last occurrence of a key wins). Pinned in fixtures/cdcBulk.
pub fn cdcBulkSql(a: std.mem.Allocator, table: []const u8, cols: []const []const u8, pk: []const []const u8) ![]const u8 {
    var picks: std.ArrayList(u8) = .empty;
    for (cols, 0..) |_, i| {
        if (i > 0) try picks.appendSlice(a, ", ");
        try picks.appendSlice(a, try std.fmt.allocPrint(a, "json_extract(value, '$[{d}]')", .{i}));
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
    try sql.appendSlice(a, try std.fmt.allocPrint(a, "INSERT INTO {s} ({s}) SELECT {s} FROM json_each(?) WHERE true", .{
        table, try quotedJoin(a, cols), picks.items,
    }));
    const conflict = try quotedJoin(a, pk);
    if (sets.items.len > 0) {
        try sql.appendSlice(a, try std.fmt.allocPrint(a, " ON CONFLICT({s}) DO UPDATE SET {s}", .{ conflict, sets.items }));
    } else {
        try sql.appendSlice(a, try std.fmt.allocPrint(a, " ON CONFLICT({s}) DO NOTHING", .{conflict}));
    }
    return sql.items;
}

fn sameSet(x: []const []const u8, y: []const []const u8) bool {
    if (x.len != y.len) return false;
    for (x) |s| if (!containsStr(y, s)) return false;
    return true;
}

fn boolField(v: Value, key: []const u8) bool {
    if (v != .object) return false;
    const f = v.object.get(key) orelse return false;
    return f == .bool and f.bool;
}

fn segment(a: std.mem.Allocator, kind: []const u8, event: usize, why: []const u8) !Value {
    var obj: std.json.ObjectMap = .empty;
    try obj.put(a, "kind", .{ .string = kind });
    try obj.put(a, "event", .{ .integer = @intCast(event) });
    try obj.put(a, "why", .{ .string = why });
    return .{ .object = obj };
}

/// core.ts planCdcBulk: which events of one CDC batch share ONE `cdcBulkSql`
/// statement and which take the per-event path (`single`, named) or are dropped
/// (`drop`: gated, or no data). Per table, a run of eligible events with one column
/// set is a `bulk` segment; a per-event event of the SAME table closes its run first
/// (the order within a table is the stream's), other tables' events do not (one
/// transaction, FK checks deferred). `tables[t].columns` is the replica table's own
/// column list. Pinned in fixtures/cdcBulk; the rules are the TS core's, in order.
pub fn planCdcBulk(a: std.mem.Allocator, engine: []const u8, tables: Value, events: Value) !Value {
    const Group = struct {
        table: []const u8,
        cols: []const []const u8,
        sql: []const u8,
        rows: std.json.Array,
        events: std.json.Array,
    };
    var out = std.json.Array.init(a);
    var open: std.StringArrayHashMapUnmanaged(Group) = .empty;
    const Closer = struct {
        fn close(a_: std.mem.Allocator, out_: *std.json.Array, open_: *std.StringArrayHashMapUnmanaged(Group), table: []const u8) !void {
            const g = open_.get(table) orelse return;
            _ = open_.orderedRemove(table);
            var cols = std.json.Array.init(a_);
            for (g.cols) |c| try cols.append(.{ .string = c });
            var obj: std.json.ObjectMap = .empty;
            try obj.put(a_, "kind", .{ .string = "bulk" });
            try obj.put(a_, "table", .{ .string = g.table });
            try obj.put(a_, "cols", .{ .array = cols });
            try obj.put(a_, "sql", .{ .string = g.sql });
            try obj.put(a_, "rows", .{ .array = g.rows });
            try obj.put(a_, "events", .{ .array = g.events });
            try out_.append(.{ .object = obj });
        }
    };
    const evs: []const Value = if (events == .array) events.array.items else &.{};
    for (evs, 0..) |ev, i| {
        if (ev != .object) continue;
        const table = if (ev.object.get("table")) |v| (if (v == .string) v.string else "") else "";
        const t: Value = if (tables == .object) (tables.object.get(table) orelse .null) else .null;
        if (t != .object) {
            try out.append(try segment(a, "single", i, "not-followed"));
            continue;
        }
        const data = ev.object.get("data") orelse .null;
        if (data != .object) {
            try out.append(try segment(a, "drop", i, "no-data"));
            continue;
        }
        // Every `single` closes this table's run first: the order within a table is the stream's.
        const why: ?[]const u8 = blk: {
            if (boolField(ev, "optimistic")) break :blk "optimistic";
            if (boolField(t, "unseeded")) break :blk "unseeded";
            if (t.object.get("anchor")) |anchor| if (anchor == .object and seedGateDrops(ev, anchor)) {
                try out.append(try segment(a, "drop", i, "gate"));
                continue;
            };
            if (!std.mem.eql(u8, engine, "sqlite") and !std.mem.eql(u8, engine, "duckdb")) break :blk "engine";
            const sqlite = std.mem.eql(u8, engine, "sqlite");
            if (sqlite) if (t.object.get("blobCols")) |bc| if (bc == .array and bc.array.items.len > 0) break :blk "blob-table";
            const op = if (ev.object.get("operation")) |v| (if (v == .string) v.string else "") else "";
            if (std.mem.eql(u8, op, "DELETE")) break :blk "delete";
            const tomb: ?[]const u8 = if (t.object.get("tombstoneColumn")) |v| (if (v == .string) v.string else null) else null;
            if (tombstoned(tomb, data)) break :blk "tombstone";
            if (!std.mem.eql(u8, op, "INSERT") and !std.mem.eql(u8, op, "UPDATE")) break :blk "operation";
            const cols_v: Value = t.object.get("columns") orelse .null;
            const pk_v: Value = t.object.get("pkCols") orelse .null;
            const columns: []const []const u8 = if (cols_v == .array) try strArr(a, cols_v.array) else &.{};
            const pk: []const []const u8 = if (pk_v == .array) try strArr(a, pk_v.array) else &.{};
            var keys: std.ArrayList([]const u8) = .empty;
            var it = data.object.iterator();
            while (it.next()) |e| {
                if (std.mem.startsWith(u8, e.key_ptr.*, "old.")) continue;
                try keys.append(a, e.key_ptr.*);
            }
            for (keys.items) |k| if (!containsStr(columns, k)) break :blk "unknown-column";
            if ((try planKeyChange(a, table, pk, data)) != null) break :blk "key-change";
            if (pk.len == 0) break :blk "keyless";
            for (pk) |c| {
                const v = data.object.get(c) orelse .null;
                if (v == .null) break :blk "incomplete-key";
            }
            if (sqlite) for (keys.items) |k| if (isBinMarker(data.object.get(k).?)) break :blk "bytes";
            if (std.mem.eql(u8, op, "UPDATE") and !sameSet(keys.items, columns)) break :blk "partial-update";
            // Eligible: join this table's open run, or open one for this column set.
            if (open.getPtr(table)) |g| if (!sameSet(g.cols, keys.items)) try Closer.close(a, &out, &open, table);
            if (open.getPtr(table) == null) {
                try open.put(a, table, .{
                    .table = table,
                    .cols = keys.items,
                    .sql = try cdcBulkSql(a, table, keys.items, pk),
                    .rows = std.json.Array.init(a),
                    .events = std.json.Array.init(a),
                });
            }
            const g = open.getPtr(table).?;
            var row = std.json.Array.init(a);
            for (g.cols) |c| try row.append(data.object.get(c) orelse .null);
            try g.rows.append(.{ .array = row });
            try g.events.append(.{ .integer = @intCast(i) });
            break :blk null;
        };
        if (why) |w| {
            try Closer.close(a, &out, &open, table);
            try out.append(try segment(a, "single", i, w));
        }
    }
    while (open.count() > 0) try Closer.close(a, &out, &open, open.keys()[0]);
    return .{ .array = out };
}

/// core.ts chainRowParams: objects -> compact JSON, strings -> pgTsToWire.
pub fn chainRowParams(a: std.mem.Allocator, row: std.json.Array) !Value {
    var out = std.json.Array.init(a);
    for (row.items) |v| {
        if (isBinMarker(v)) {
            try out.append(v);
            continue;
        }
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
        // §10dv: a key column in the values must equal the key — an UPDATE cannot move
        // a row (the ingress refuses `KeyChange`), so it is refused here, before an
        // outbox row exists. Rename = delete + create.
        if (key == .object and data == .object) {
            var kit = key.object.iterator();
            while (kit.next()) |e| {
                const sent = data.object.get(e.key_ptr.*) orelse continue;
                const ks = try valueToString(a, e.value_ptr.*);
                const vs = try valueToString(a, sent);
                if (!std.mem.eql(u8, std.mem.trim(u8, ks, "\""), std.mem.trim(u8, vs, "\""))) return error.KeyChange;
            }
        }
        try payload.put(a, "data", data);
    }

    // §10ds: the local apply addresses the row by its KEY, so the optimistic data is
    // the key merged with the (sparse) wire data — the wire payload stays sparse.
    var opt_data: Value = key;
    if (!is_delete) {
        var merged: std.json.ObjectMap = .empty;
        if (key == .object) {
            var it = key.object.iterator();
            while (it.next()) |e| try merged.put(a, e.key_ptr.*, e.value_ptr.*);
        }
        const d = payload.get("data").?;
        if (d == .object) {
            var it = d.object.iterator();
            while (it.next()) |e| try merged.put(a, e.key_ptr.*, e.value_ptr.*);
        }
        opt_data = .{ .object = merged };
    }
    var optimistic: std.json.ObjectMap = .empty;
    try optimistic.put(a, "table", .{ .string = table });
    try optimistic.put(a, "operation", .{ .string = op });
    try optimistic.put(a, "data", opt_data);
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

/// core.ts planFromManifest. The incremental chain (§10gs): a base (the whole table, the
/// manifest's `base` or, under the older name, `full`), checkpoints (rows whose version
/// moved inside each one's window, tombstones included) and deltas. A checkpoint applies
/// exactly as a delta does; only the base wipes and reloads, so it keeps the step kind
/// `full` the clients already act on.
pub fn planFromManifest(a: std.mem.Allocator, man: Value, watermark: ?[]const u8) !Value {
    var plan = std.json.Array.init(a);
    const deltas: std.json.Array = getArr(man, "deltas") orelse std.json.Array.init(a);
    const checkpoints: std.json.Array = getArr(man, "checkpoints") orelse std.json.Array.init(a);
    const base_v = if (man == .object) (man.object.get("base") orelse man.object.get("full")) else null;
    const base: ?Value = if (base_v != null and base_v.? == .object) base_v.? else null;

    var applicable = std.json.Array.init(a);
    for (deltas.items) |d| {
        const cutoff = getStr(d, "cutoff") orelse "";
        if (watermark == null or std.mem.order(u8, cutoff, watermark.?) == .gt) {
            try applicable.append(d);
        }
    }

    // ⚠️ FIRST, before any arithmetic on cutoffs: a replica older than the gc watermark
    // cannot be caught up incrementally. A row deleted while it was away may have been
    // REAPED since, and no checkpoint or delta carries an absence — only the base. (The
    // sweeper cannot reap a tombstone newer than the watermark: that is what makes the
    // other branches sound. PROTOCOL §7.5.)
    const gc = getStr(man, "gc_watermark");
    const gc_too_old = watermark != null and gc != null and std.mem.order(u8, watermark.?, gc.?) == .lt;

    // "Reaches" = the chain continues from where this replica stands: the first
    // applicable delta starts at or before the watermark — or nothing is newer AND the
    // chain's base itself is not newer than the watermark. A chain REBUILT after the
    // watermark (a fresh g1 full, no deltas: what a feed restart produces, NOTES §10bm)
    // is unreachable, not "already applied"; the walk starts from its base.
    const base_cutoff: ?[]const u8 = if (base) |b| getStr(b, "cutoff") else null;
    const reaches = !gc_too_old and watermark != null and
        (if (applicable.items.len > 0)
            std.mem.order(u8, getStr(applicable.items[0], "prev_cutoff") orelse "", watermark.?) != .gt
        else
            (base_cutoff == null or std.mem.order(u8, base_cutoff.?, watermark.?) != .gt));

    if (reaches) {
        for (applicable.items) |d| try plan.append(try deltaStep(a, d));
        return .{ .array = plan };
    }

    // The deltas do not reach it, but the checkpoints may: every checkpoint whose window
    // ends after the watermark, then the deltas. Overlap between the two is harmless —
    // version-guarded upserts, tombstones deleting by key — so the rule is "everything
    // newer than the watermark", not "exactly the gap".
    if (!gc_too_old and watermark != null and checkpoints.items.len > 0) {
        const oldest_lower = getStr(checkpoints.items[0], "lower") orelse "";
        if (std.mem.order(u8, oldest_lower, watermark.?) != .gt) {
            for (checkpoints.items) |ck| {
                if (std.mem.order(u8, getStr(ck, "cutoff") orelse "", watermark.?) == .gt) {
                    try plan.append(try checkpointStep(a, ck));
                }
            }
            for (applicable.items) |d| try plan.append(try deltaStep(a, d));
            return .{ .array = plan };
        }
    }

    // The base, then everything it does not already carry.
    const b = base orelse return .{ .array = plan };
    var fstep: std.json.ObjectMap = .empty;
    try fstep.put(a, "name", .{ .string = getStr(b, "object") orelse "" });
    try fstep.put(a, "kind", .{ .string = "full" });
    try putSorted(a, &fstep, b);
    try plan.append(.{ .object = fstep });
    const base_gen = getInt(b, "gen") orelse 0;
    for (checkpoints.items) |ck| {
        if ((getInt(ck, "gen") orelse 0) > base_gen) try plan.append(try checkpointStep(a, ck));
    }
    for (deltas.items) |d| {
        if ((getInt(d, "gen") orelse 0) > base_gen) try plan.append(try deltaStep(a, d));
    }
    return .{ .array = plan };
}

/// A checkpoint step: applied like a delta, named so a client can say which it took.
fn checkpointStep(a: std.mem.Allocator, ck: Value) !Value {
    var step: std.json.ObjectMap = .empty;
    try step.put(a, "name", .{ .string = getStr(ck, "object") orelse "" });
    try step.put(a, "kind", .{ .string = "checkpoint" });
    try putSorted(a, &step, ck);
    return .{ .object = step };
}

fn deltaStep(a: std.mem.Allocator, d: Value) !Value {
    var step: std.json.ObjectMap = .empty;
    try step.put(a, "name", .{ .string = getStr(d, "object") orelse "" });
    try step.put(a, "kind", .{ .string = "delta" });
    try putSorted(a, &step, d);
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

/// Three shapes of "the stream no longer continues from where I stopped": never here
/// (`stored == 0`); the tail retained away (`stored < first_seq - 1`); and the stream
/// RESTARTED under me — a position beyond its last sequence. The third is a lost
/// replication slot seen from a client (NOTES §10bm): WAL the bridge never saw leaves
/// no hole in the numbering, so the bridge recreates the CDC streams on a new slot and
/// this is the only trace a client can read. `last_seq < 0` = unknown (not checked).
pub fn streamHasGap(first_seq: i64, stored: i64, last_seq: i64) bool {
    if (stored == 0) return true;
    if (first_seq > 0 and stored < first_seq - 1) return true;
    if (last_seq >= 0 and stored > last_seq) return true;
    return false;
}

/// core.ts scopeSeeding: {gapped, tablesToSeed}.
pub fn scopeSeeding(a: std.mem.Allocator, streams: Value, tables: Value) !Value {
    var gapped = std.json.Array.init(a);
    if (streams == .object) {
        var it = streams.object.iterator();
        while (it.next()) |e| {
            const first_seq = getInt(e.value_ptr.*, "firstSeq") orelse 0;
            const last_seq = getInt(e.value_ptr.*, "lastSeq") orelse -1;
            const stored = getInt(e.value_ptr.*, "stored") orelse 0;
            if (streamHasGap(first_seq, stored, last_seq)) try gapped.append(.{ .string = e.key_ptr.* });
        }
    }
    var to_seed = std.json.Array.init(a);
    if (tables == .object) {
        var it = tables.object.iterator();
        while (it.next()) |e| {
            const route = getStr(e.value_ptr.*, "route") orelse "";
            // A tenant-scoped table's OPEN-TENANT rows ride CDC_PUBLIC while its own ride
            // CDC_<tenant>: both must be gap-free, or a gap on one silently leaves half
            // the table stale (NOTES §10bq).
            const shared = getStr(e.value_ptr.*, "sharedRoute") orelse "";
            const seeded = if (e.value_ptr.* == .object)
                (if (e.value_ptr.object.get("seeded")) |s| (s == .bool and s.bool) else false)
            else
                false;
            var route_gapped = false;
            for (gapped.items) |g| {
                if (std.mem.eql(u8, g.string, route)) route_gapped = true;
                if (shared.len > 0 and std.mem.eql(u8, g.string, shared)) route_gapped = true;
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
    if (getStr(col, "default")) |d| if (d.len > 0) try out.appendSlice(a, try std.fmt.allocPrint(a, " DEFAULT {s}", .{d}));
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
/// `strict` (§10fi): SQLite's STRICT tables — a bind that is not of the column's
/// declared type is refused, never stored as whatever arrived. SQLite only.
pub fn createTableSteps(a: std.mem.Allocator, table: []const u8, cols: std.json.Array, pk: []const []const u8, fks: std.json.Array, strict: bool) !Value {
    var steps = std.json.Array.init(a);
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "DROP TABLE IF EXISTS {s};", .{table})));
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "CREATE TABLE {s} ({s}){s};", .{ table, try tableBody(a, cols, pk, fks), if (strict) " STRICT" else "" })));
    return .{ .array = steps };
}

/// §10fi: a SQLite table created before STRICT existed is rebuilt once (rows kept).
/// Empty ddl → false: no table yet, the create path owns it.
pub fn strictMissing(ddl: []const u8) bool {
    if (ddl.len == 0) return false;
    var t = std.mem.trim(u8, ddl, " \t\r\n;");
    t = std.mem.trimEnd(u8, t, " \t\r\n");
    if (t.len < 6) return true;
    return !std.ascii.eqlIgnoreCase(t[t.len - 6 ..], "STRICT");
}

test "strictMissing: only a trailing STRICT counts (§10fi)" {
    try std.testing.expect(!strictMissing(""));
    try std.testing.expect(strictMissing("CREATE TABLE t (\"a\" TEXT)"));
    try std.testing.expect(!strictMissing("CREATE TABLE t (\"a\" TEXT) STRICT"));
    try std.testing.expect(!strictMissing("CREATE TABLE t (\"a\" TEXT) strict;\n"));
}

/// core.ts rebuildSteps (finding 9's legitimate drop/recreate).
pub fn rebuildSteps(a: std.mem.Allocator, table: []const u8, cols: std.json.Array, pk: []const []const u8, fks: std.json.Array, existing: []const []const u8, strict: bool) !Value {
    const tmp = try std.fmt.allocPrint(a, "{s}__migrating", .{table});
    var steps = std.json.Array.init(a);
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "DROP TABLE IF EXISTS {s};", .{tmp})));
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "CREATE TABLE {s} ({s}){s};", .{ tmp, try tableBody(a, cols, pk, fks), if (strict) " STRICT" else "" })));
    var common: std.ArrayList([]const u8) = .empty;
    var select: std.ArrayList([]const u8) = .empty;
    for (cols.items) |c| {
        const n = colName(c);
        if (!containsStr(existing, n)) continue;
        try common.append(a, n);
        // A STRICT target refuses a value of another type where affinity used to
        // convert it (a re-typed column: TEXT '1.5' into REAL): the copy casts.
        const ty: []const u8 = if (c == .object) (if (c.object.get("type")) |t| (if (t == .string) t.string else "TEXT") else "TEXT") else "TEXT";
        try select.append(a, if (strict) try std.fmt.allocPrint(a, "CAST(\"{s}\" AS {s})", .{ n, ty }) else try std.fmt.allocPrint(a, "\"{s}\"", .{n}));
    }
    if (common.items.len > 0) {
        const list = try quotedJoin(a, common.items);
        const sel = try std.mem.join(a, ", ", select.items);
        try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "INSERT INTO {s} ({s}) SELECT {s} FROM {s};", .{ tmp, list, sel, table })));
    }
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "DROP TABLE IF EXISTS {s};", .{table})));
    try steps.append(try sqlStep(a, try std.fmt.allocPrint(a, "ALTER TABLE {s} RENAME TO {s};", .{ tmp, table })));
    return .{ .array = steps };
}

/// core.ts isReadOnlySql (§10di). libzb enforces read-only `query` with a READONLY
/// SQLite connection; this is the same rule as a pure function, so the fixtures pin
/// one meaning of "reads" for both clients.
/// §10hn: which tables a client holds, and how — the ONE rule both clients follow
/// (core.ts `tableSet`, fixtures `tableSet`). `tables` is a list, or every published
/// table when `all` is set (`keys` is the schemas bucket's key list, consulted only
/// then); `ondemand` is held for its schema alone — no seed, no tail. Absent both:
/// nothing. A name in both is on-demand. Deduplicated; a declared name the bucket does
/// not know yet is kept.
pub const TableSet = struct { follow: []const []const u8, ondemand: []const []const u8 };

pub fn tableSet(a: std.mem.Allocator, all: bool, tables: []const []const u8, ondemand: []const []const u8, keys: []const []const u8) !TableSet {
    var od: std.ArrayListUnmanaged([]const u8) = .empty;
    for (ondemand) |t| {
        if (t.len == 0 or contains(od.items, t)) continue;
        try od.append(a, t);
    }
    var follow: std.ArrayListUnmanaged([]const u8) = .empty;
    const src = if (all) keys else tables;
    for (src) |t| {
        if (t.len == 0 or contains(od.items, t) or contains(follow.items, t)) continue;
        try follow.append(a, t);
    }
    return .{ .follow = try follow.toOwnedSlice(a), .ondemand = try od.toOwnedSlice(a) };
}

fn contains(list: []const []const u8, t: []const u8) bool {
    for (list) |x| if (std.mem.eql(u8, x, t)) return true;
    return false;
}

fn writeStrArr(a: std.mem.Allocator, out: *std.ArrayList(u8), items: []const []const u8) !void {
    try out.append(a, '[');
    for (items, 0..) |s, i| {
        if (i > 0) try out.append(a, ',');
        try writeJsonString(a, out, s);
    }
    try out.append(a, ']');
}

pub fn writeTableSet(a: std.mem.Allocator, out: *std.ArrayList(u8), ts: TableSet) !void {
    try out.appendSlice(a, "{\"follow\":");
    try writeStrArr(a, out, ts.follow);
    try out.appendSlice(a, ",\"ondemand\":");
    try writeStrArr(a, out, ts.ondemand);
    try out.append(a, '}');
}

/// §10ho: a map of LWW registers, merged — core.ts `mergeRegisters`, fixtures
/// `mergeRegisters`. A register is `{v, t, w}`; per key the higher (t, w) wins, string
/// order on both; a value without `t` is the oldest. Both documents are JSON objects;
/// the result is a fresh object in `a`, keys in `a`'s order then `b`'s new ones.
pub fn mergeRegisters(a: std.mem.Allocator, doc_a: Value, doc_b: Value) !Value {
    var out: std.json.ObjectMap = .empty;
    if (doc_a == .object) {
        var it = doc_a.object.iterator();
        while (it.next()) |e| try out.put(a, e.key_ptr.*, e.value_ptr.*);
    }
    if (doc_b == .object) {
        var it = doc_b.object.iterator();
        while (it.next()) |e| {
            const cur = out.get(e.key_ptr.*);
            if (cur == null or registerWins(e.value_ptr.*, cur.?)) try out.put(a, e.key_ptr.*, e.value_ptr.*);
        }
    }
    return .{ .object = out };
}

fn registerField(reg: Value, key: []const u8) []const u8 {
    if (reg != .object) return "";
    const f = reg.object.get(key) orelse return "";
    return if (f == .string) f.string else "";
}

/// `b` beats `a` when its (t, w) is strictly higher.
fn registerWins(b: Value, a: Value) bool {
    const tb = registerField(b, "t");
    const ta = registerField(a, "t");
    return switch (std.mem.order(u8, tb, ta)) {
        .gt => true,
        .lt => false,
        .eq => std.mem.order(u8, registerField(b, "w"), registerField(a, "w")) == .gt,
    };
}

pub fn isReadOnlySql(sql: []const u8) bool {
    // §10hy: the normalised form is never LONGER than the input (comments shrink to a
    // space, a string literal to its two quotes, everything else is 1:1), so the input
    // length bounds it. This used to be a fixed 4 KiB buffer with
    // `if (sql.len > buf.len) return false;` on top, which refused a perfectly
    // read-only query for being long and blamed it for writing. core.ts has no such
    // limit, so the two clients disagreed about the same SQL — found by a corridor
    // query whose anchor list pushed it past 4 KiB.
    var stack: [4096]u8 = undefined;
    const heap: ?[]u8 = if (sql.len > stack.len) std.heap.page_allocator.alloc(u8, sql.len) catch return false else null;
    defer if (heap) |h| std.heap.page_allocator.free(h);
    const buf: []u8 = heap orelse stack[0..];
    // blank block comments, line comments and string literals in one pass
    var n: usize = 0;
    var i: usize = 0;
    while (i < sql.len) {
        if (i + 1 < sql.len and sql[i] == '/' and sql[i + 1] == '*') {
            i += 2;
            while (i + 1 < sql.len and !(sql[i] == '*' and sql[i + 1] == '/')) i += 1;
            i = @min(i + 2, sql.len);
            buf[n] = ' ';
            n += 1;
        } else if (i + 1 < sql.len and sql[i] == '-' and sql[i + 1] == '-') {
            while (i < sql.len and sql[i] != '\n') i += 1;
            buf[n] = ' ';
            n += 1;
        } else if (sql[i] == '\'' or sql[i] == '"') {
            const q = sql[i];
            i += 1;
            while (i < sql.len) : (i += 1) {
                if (sql[i] == q) {
                    if (i + 1 < sql.len and sql[i + 1] == q) {
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            i = @min(i + 1, sql.len);
            buf[n] = q;
            buf[n + 1] = q;
            n += 2;
        } else {
            buf[n] = std.ascii.toLower(sql[i]);
            n += 1;
            i += 1;
        }
    }
    var s = std.mem.trim(u8, buf[0..n], " \t\r\n");
    if (s.len == 0) return false;
    s = std.mem.trimEnd(u8, s, "; \t\r\n");
    if (std.mem.indexOfScalar(u8, s, ';') != null) return false;
    var end: usize = 0;
    while (end < s.len and (std.ascii.isAlphabetic(s[end]) or s[end] == '_')) end += 1;
    const first = s[0..end];
    const heads = [_][]const u8{ "select", "with", "explain", "values", "pragma" };
    var ok = false;
    for (heads) |h| ok = ok or std.mem.eql(u8, first, h);
    if (!ok) return false;
    const writes = [_][]const u8{ "insert", "update", "delete", "replace", "drop", "alter", "create", "attach", "detach", "vacuum", "reindex", "truncate" };
    for (writes) |w| if (containsWord(s, w)) return false;
    if (std.mem.eql(u8, first, "pragma") and std.mem.indexOfScalar(u8, s, '=') != null) return false;
    return true;
}

fn containsWord(s: []const u8, w: []const u8) bool {
    var from: usize = 0;
    while (std.mem.indexOfPos(u8, s, from, w)) |at| {
        const before_ok = at == 0 or !(std.ascii.isAlphanumeric(s[at - 1]) or s[at - 1] == '_');
        const after = at + w.len;
        const after_ok = after >= s.len or !(std.ascii.isAlphanumeric(s[after]) or s[after] == '_');
        if (before_ok and after_ok) return true;
        from = at + 1;
    }
    return false;
}

/// core.ts keyShape (§10dg): the pk columns in pk order with their dialect type,
/// as canonical JSON `[["id","INTEGER"]]`. A pk column the descriptor does not
/// list is skipped, not guessed.
pub fn keyShape(a: std.mem.Allocator, pk: []const []const u8, cols: std.json.Array) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    try out.append(a, '[');
    var n: usize = 0;
    for (pk) |name| {
        const ty = columnTypeOf(cols, name) orelse continue;
        if (n > 0) try out.append(a, ',');
        try writePair(a, &out, name, ty);
        n += 1;
    }
    try out.append(a, ']');
    return out.items;
}

/// core.ts typeShape: every column with its type, sorted by name.
pub fn typeShape(a: std.mem.Allocator, cols: std.json.Array) ![]const u8 {
    const idx = try a.alloc(usize, cols.items.len);
    for (idx, 0..) |*x, k| x.* = k;
    const Ctx = struct {
        cols: std.json.Array,
        fn lessThan(ctx: @This(), x: usize, y: usize) bool {
            return std.mem.lessThan(u8, colName(ctx.cols.items[x]), colName(ctx.cols.items[y]));
        }
    };
    std.mem.sort(usize, idx, Ctx{ .cols = cols }, Ctx.lessThan);
    var out: std.ArrayList(u8) = .empty;
    try out.append(a, '[');
    for (idx, 0..) |k, i| {
        if (i > 0) try out.append(a, ',');
        try writePair(a, &out, colName(cols.items[k]), getStr(cols.items[k], "type") orelse "");
    }
    try out.append(a, ']');
    return out.items;
}

/// core.ts retypedColumns: columns present in BOTH the stored type shape and the
/// descriptor whose type text differs, in descriptor order. An absent or
/// unreadable record reads as "nothing known".
pub fn retypedColumns(a: std.mem.Allocator, stored: ?[]const u8, cols: std.json.Array) !Value {
    var out = std.json.Array.init(a);
    const text = stored orelse return .{ .array = out };
    const parsed = std.json.parseFromSliceLeaky(Value, a, text, .{}) catch return .{ .array = out };
    if (parsed != .array) return .{ .array = out };
    for (cols.items) |c| {
        const name = colName(c);
        const ty = getStr(c, "type") orelse "";
        for (parsed.array.items) |e| {
            if (e != .array or e.array.items.len != 2 or e.array.items[0] != .string or e.array.items[1] != .string) continue;
            if (!std.mem.eql(u8, e.array.items[0].string, name)) continue;
            if (!std.mem.eql(u8, e.array.items[1].string, ty)) try out.append(.{ .string = name });
            break;
        }
    }
    return .{ .array = out };
}

fn columnTypeOf(cols: std.json.Array, name: []const u8) ?[]const u8 {
    for (cols.items) |c| if (std.mem.eql(u8, colName(c), name)) return getStr(c, "type") orelse "";
    return null;
}

fn writePair(a: std.mem.Allocator, out: *std.ArrayList(u8), name: []const u8, ty: []const u8) !void {
    try out.append(a, '[');
    try writeJsonString(a, out, name);
    try out.append(a, ',');
    try writeJsonString(a, out, ty);
    try out.append(a, ']');
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
