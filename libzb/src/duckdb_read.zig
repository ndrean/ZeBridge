//! §10fl: reading a DuckDB result through the chunk and vector API — the supported one.
//!
//! The `duckdb_value_*` family and `duckdb_column_data` are marked "scheduled for
//! removal" in the header and are not safe on every result (measured on 1.5.5: an
//! empty string for a TIMESTAMP_TZ, zeros for a UUID, then a crash in
//! `duckdb_value_is_null`). A result is a list of data chunks, a chunk a vector per
//! column, a vector a validity mask and typed data; every type the wire produces is
//! read from that layout, and a list, a fixed array or a struct is rendered as JSON
//! text — the shape the wire carries arrays in (§10ey), so a replica reads a DuckDB
//! list the way it reads a SQLite one.

const std = @import("std");
const dk = @import("duckdb");
const storage = @import("storage.zig");
const engines = @import("engines.zig");
inline fn D() *const engines.DuckDB {
    return engines.duckdb.get();
}
const Value = storage.Value;
const Row = storage.Row;

pub fn readResult(a: std.mem.Allocator, res: *dk.duckdb_result, want_names: bool) storage.Error!storage.Storage.Named {
    const ncol: usize = @intCast(D().duckdb_column_count(res));
    const cols = try a.alloc([]const u8, if (want_names) ncol else 0);
    if (want_names) for (cols, 0..) |*nm, i| {
        nm.* = try a.dupe(u8, std.mem.span(D().duckdb_column_name(res, @intCast(i))));
    };
    var rows: std.ArrayList(Row) = .empty;
    const nchunks: usize = @intCast(D().duckdb_result_chunk_count(res.*));
    for (0..nchunks) |ci| {
        var chunk = D().duckdb_result_get_chunk(res.*, @intCast(ci));
        if (chunk == null) continue;
        defer D().duckdb_destroy_data_chunk(&chunk);
        const size: usize = @intCast(D().duckdb_data_chunk_get_size(chunk));
        const first = rows.items.len;
        try rows.ensureUnusedCapacity(a, size);
        for (0..size) |_| rows.appendAssumeCapacity(try a.alloc(Value, ncol));
        for (0..ncol) |col| {
            const vec = D().duckdb_data_chunk_get_vector(chunk, @intCast(col));
            var lt = D().duckdb_vector_get_column_type(vec);
            defer D().duckdb_destroy_logical_type(&lt);
            for (0..size) |r| rows.items[first + r][col] = try readCell(a, vec, lt, r);
        }
    }
    return .{ .columns = cols, .rows = rows.items };
}

fn readCell(a: std.mem.Allocator, vec: dk.duckdb_vector, lt: dk.duckdb_logical_type, row: usize) storage.Error!Value {
    const validity = D().duckdb_vector_get_validity(vec);
    if (validity != null and !D().duckdb_validity_row_is_valid(validity, @intCast(row))) return .null;
    const tid = D().duckdb_get_type_id(lt);
    // An ARRAY or STRUCT vector has no data of its own (it lives in the children):
    // only the scalar and LIST branches read `data`, and a missing buffer there is
    // an empty cell rather than a read past nothing.
    const data = D().duckdb_vector_get_data(vec) orelse switch (tid) {
        dk.DUCKDB_TYPE_ARRAY, dk.DUCKDB_TYPE_STRUCT => @as(*anyopaque, @ptrFromInt(8)),
        else => return .null,
    };
    switch (tid) {
        dk.DUCKDB_TYPE_BOOLEAN => return .{ .boolean = @as([*]const bool, @ptrCast(data))[row] },
        dk.DUCKDB_TYPE_TINYINT => return .{ .integer = @as([*]const i8, @ptrCast(data))[row] },
        dk.DUCKDB_TYPE_SMALLINT => return .{ .integer = @as([*]const i16, @ptrCast(@alignCast(data)))[row] },
        dk.DUCKDB_TYPE_INTEGER => return .{ .integer = @as([*]const i32, @ptrCast(@alignCast(data)))[row] },
        dk.DUCKDB_TYPE_BIGINT => return .{ .integer = @as([*]const i64, @ptrCast(@alignCast(data)))[row] },
        dk.DUCKDB_TYPE_UTINYINT => return .{ .integer = @as([*]const u8, @ptrCast(data))[row] },
        dk.DUCKDB_TYPE_USMALLINT => return .{ .integer = @as([*]const u16, @ptrCast(@alignCast(data)))[row] },
        dk.DUCKDB_TYPE_UINTEGER => return .{ .integer = @as([*]const u32, @ptrCast(@alignCast(data)))[row] },
        dk.DUCKDB_TYPE_UBIGINT => {
            const v = @as([*]const u64, @ptrCast(@alignCast(data)))[row];
            return if (v <= std.math.maxInt(i64)) .{ .integer = @intCast(v) } else .{ .text = try std.fmt.allocPrint(a, "{d}", .{v}) };
        },
        dk.DUCKDB_TYPE_FLOAT => return .{ .real = @as([*]const f32, @ptrCast(@alignCast(data)))[row] },
        dk.DUCKDB_TYPE_DOUBLE => return .{ .real = @as([*]const f64, @ptrCast(@alignCast(data)))[row] },
        dk.DUCKDB_TYPE_VARCHAR => return .{ .text = try a.dupe(u8, stringAt(data, row)) },
        dk.DUCKDB_TYPE_BLOB => return .{ .blob = try a.dupe(u8, stringAt(data, row)) },
        dk.DUCKDB_TYPE_TIMESTAMP, dk.DUCKDB_TYPE_TIMESTAMP_TZ => return .{ .text = try timestampText(a, @as([*]const i64, @ptrCast(@alignCast(data)))[row]) },
        dk.DUCKDB_TYPE_TIMESTAMP_S => return .{ .text = try timestampText(a, @as([*]const i64, @ptrCast(@alignCast(data)))[row] * 1_000_000) },
        dk.DUCKDB_TYPE_TIMESTAMP_MS => return .{ .text = try timestampText(a, @as([*]const i64, @ptrCast(@alignCast(data)))[row] * 1_000) },
        dk.DUCKDB_TYPE_TIMESTAMP_NS => return .{ .text = try timestampText(a, @divTrunc(@as([*]const i64, @ptrCast(@alignCast(data)))[row], 1_000)) },
        dk.DUCKDB_TYPE_DATE => {
            const d = D().duckdb_from_date(.{ .days = @as([*]const i32, @ptrCast(@alignCast(data)))[row] });
            return .{ .text = try std.fmt.allocPrint(a, "{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u32, @intCast(d.year)), @as(u8, @intCast(d.month)), @as(u8, @intCast(d.day)) }) };
        },
        dk.DUCKDB_TYPE_TIME => {
            const us = @as([*]const i64, @ptrCast(@alignCast(data)))[row];
            return .{ .text = try std.fmt.allocPrint(a, "{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{ @as(u32, @intCast(@divTrunc(us, 3_600_000_000))), @as(u32, @intCast(@mod(@divTrunc(us, 60_000_000), 60))), @as(u32, @intCast(@mod(@divTrunc(us, 1_000_000), 60))), @as(u32, @intCast(@mod(us, 1_000_000))) }) };
        },
        dk.DUCKDB_TYPE_UUID => {
            const h = @as([*]const dk.duckdb_hugeint, @ptrCast(@alignCast(data)))[row];
            const upper: u64 = @as(u64, @bitCast(h.upper)) ^ (@as(u64, 1) << 63);
            return .{ .text = try std.fmt.allocPrint(a, "{x:0>8}-{x:0>4}-{x:0>4}-{x:0>4}-{x:0>12}", .{
                @as(u32, @truncate(upper >> 32)), @as(u16, @truncate(upper >> 16)), @as(u16, @truncate(upper)),
                @as(u16, @truncate(h.lower >> 48)), @as(u48, @truncate(h.lower)),
            }) };
        },
        dk.DUCKDB_TYPE_HUGEINT => {
            const h = @as([*]const dk.duckdb_hugeint, @ptrCast(@alignCast(data)))[row];
            return .{ .text = try std.fmt.allocPrint(a, "{d}", .{hugeToI128(h)}) };
        },
        dk.DUCKDB_TYPE_UHUGEINT => {
            const h = @as([*]const dk.duckdb_hugeint, @ptrCast(@alignCast(data)))[row];
            const v: u128 = (@as(u128, @as(u64, @bitCast(h.upper))) << 64) | h.lower;
            return .{ .text = try std.fmt.allocPrint(a, "{d}", .{v}) };
        },
        dk.DUCKDB_TYPE_DECIMAL => {
            const scale: u8 = D().duckdb_decimal_scale(lt);
            const raw: i128 = switch (D().duckdb_decimal_internal_type(lt)) {
                dk.DUCKDB_TYPE_SMALLINT => @as([*]const i16, @ptrCast(@alignCast(data)))[row],
                dk.DUCKDB_TYPE_INTEGER => @as([*]const i32, @ptrCast(@alignCast(data)))[row],
                dk.DUCKDB_TYPE_BIGINT => @as([*]const i64, @ptrCast(@alignCast(data)))[row],
                else => hugeToI128(@as([*]const dk.duckdb_hugeint, @ptrCast(@alignCast(data)))[row]),
            };
            return .{ .text = try decimalText(a, raw, scale) };
        },
        dk.DUCKDB_TYPE_ENUM => {
            const idx: usize = switch (D().duckdb_enum_internal_type(lt)) {
                dk.DUCKDB_TYPE_UTINYINT => @as([*]const u8, @ptrCast(data))[row],
                dk.DUCKDB_TYPE_USMALLINT => @as([*]const u16, @ptrCast(@alignCast(data)))[row],
                else => @as([*]const u32, @ptrCast(@alignCast(data)))[row],
            };
            const s = D().duckdb_enum_dictionary_value(lt, @intCast(idx));
            defer D().duckdb_free(s);
            return .{ .text = try a.dupe(u8, if (s != null) std.mem.span(s) else "") };
        },
        dk.DUCKDB_TYPE_INTERVAL => {
            const iv = @as([*]const dk.duckdb_interval, @ptrCast(@alignCast(data)))[row];
            return .{ .text = try std.fmt.allocPrint(a, "{d} mons {d} days {d} us", .{ iv.months, iv.days, iv.micros }) };
        },
        // BIT: a string whose first byte is the count of padding bits, then the bits
        // MSB first — rendered as '0101' text, the wire's varbit form.
        dk.DUCKDB_TYPE_BIT => {
            const s = stringAt(data, row);
            if (s.len == 0) return .{ .text = "" };
            const pad: usize = s[0];
            const nbits = (s.len - 1) * 8 -| pad;
            const out = try a.alloc(u8, nbits);
            for (0..nbits) |i| {
                const bi = i + pad;
                out[i] = if ((s[1 + bi / 8] >> @intCast(7 - (bi % 8))) & 1 == 1) '1' else '0';
            }
            return .{ .text = out };
        },
        dk.DUCKDB_TYPE_LIST => {
            const entries = @as([*]const dk.duckdb_list_entry, @ptrCast(@alignCast(data)))[row];
            const child = D().duckdb_list_vector_get_child(vec);
            var clt = D().duckdb_vector_get_column_type(child);
            defer D().duckdb_destroy_logical_type(&clt);
            var out: std.ArrayList(u8) = .empty;
            try out.append(a, '[');
            for (0..@intCast(entries.length)) |k| {
                if (k > 0) try out.append(a, ',');
                try jsonValue(a, &out, try readCell(a, child, clt, @intCast(entries.offset + k)));
            }
            try out.append(a, ']');
            return .{ .text = out.items };
        },
        dk.DUCKDB_TYPE_ARRAY => {
            const n: usize = @intCast(D().duckdb_array_type_array_size(lt));
            const child = D().duckdb_array_vector_get_child(vec);
            var clt = D().duckdb_vector_get_column_type(child);
            defer D().duckdb_destroy_logical_type(&clt);
            var out: std.ArrayList(u8) = .empty;
            try out.append(a, '[');
            for (0..n) |k| {
                if (k > 0) try out.append(a, ',');
                try jsonValue(a, &out, try readCell(a, child, clt, row * n + k));
            }
            try out.append(a, ']');
            return .{ .text = out.items };
        },
        dk.DUCKDB_TYPE_STRUCT => {
            const n: usize = @intCast(D().duckdb_struct_type_child_count(lt));
            var out: std.ArrayList(u8) = .empty;
            try out.append(a, '{');
            for (0..n) |k| {
                if (k > 0) try out.append(a, ',');
                const nm = D().duckdb_struct_type_child_name(lt, @intCast(k));
                defer D().duckdb_free(nm);
                try jsonString(a, &out, if (nm != null) std.mem.span(nm) else "");
                try out.append(a, ':');
                const child = D().duckdb_struct_vector_get_child(vec, @intCast(k));
                var clt = D().duckdb_vector_get_column_type(child);
                defer D().duckdb_destroy_logical_type(&clt);
                try jsonValue(a, &out, try readCell(a, child, clt, row));
            }
            try out.append(a, '}');
            return .{ .text = out.items };
        },
        else => return .{ .text = "" },
    }
}

/// A `duckdb_string_t` at `row`: inlined up to 12 bytes, a pointer beyond.
fn stringAt(data: *anyopaque, row: usize) []const u8 {
    const arr: [*]dk.duckdb_string_t = @ptrCast(@alignCast(data));
    const s = &arr[row];
    const len: usize = D().duckdb_string_t_length(s.*);
    const p = D().duckdb_string_t_data(s);
    return if (p != null) p[0..len] else "";
}

fn hugeToI128(h: dk.duckdb_hugeint) i128 {
    return (@as(i128, h.upper) << 64) | @as(i128, h.lower);
}

/// Micros since the epoch (UTC) in PostgreSQL's own text shape, `+00` included.
fn timestampText(a: std.mem.Allocator, micros: i64) ![]const u8 {
    const t = D().duckdb_from_timestamp(.{ .micros = micros });
    return std.fmt.allocPrint(a, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}+00", .{
        @as(u32, @intCast(t.date.year)), @as(u8, @intCast(t.date.month)), @as(u8, @intCast(t.date.day)),
        @as(u8, @intCast(t.time.hour)), @as(u8, @intCast(t.time.min)), @as(u8, @intCast(t.time.sec)), @as(u32, @intCast(t.time.micros)),
    });
}

/// An unscaled decimal as digits with the point placed — the numeric's own text.
fn decimalText(a: std.mem.Allocator, raw: i128, scale: u8) ![]const u8 {
    const neg = raw < 0;
    const mag: u128 = if (neg) @intCast(-raw) else @intCast(raw);
    const digits = try std.fmt.allocPrint(a, "{d}", .{mag});
    defer a.free(digits);
    var out: std.ArrayList(u8) = .empty;
    if (neg) try out.append(a, '-');
    if (scale == 0) {
        try out.appendSlice(a, digits);
    } else if (digits.len <= scale) {
        try out.appendSlice(a, "0.");
        for (0..scale - digits.len) |_| try out.append(a, '0');
        try out.appendSlice(a, digits);
    } else {
        try out.appendSlice(a, digits[0 .. digits.len - scale]);
        try out.append(a, '.');
        try out.appendSlice(a, digits[digits.len - scale ..]);
    }
    return out.toOwnedSlice(a);
}

fn jsonValue(a: std.mem.Allocator, out: *std.ArrayList(u8), v: Value) !void {
    switch (v) {
        .null => try out.appendSlice(a, "null"),
        .boolean => |b| try out.appendSlice(a, if (b) "true" else "false"),
        .integer => |i| try out.print(a, "{d}", .{i}),
        .real => |f| try out.print(a, "{d}", .{f}),
        .text => |s| try jsonString(a, out, s),
        // bytes inside a list: the wire's own form for a bytea element, `\x` hex
        .blob => |b| {
            try out.appendSlice(a, "\"\\\\x");
            for (b) |byte| try out.print(a, "{x:0>2}", .{byte});
            try out.append(a, '"');
        },
    }
}

fn jsonString(a: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    try out.append(a, '"');
    for (s) |ch| switch (ch) {
        '"' => try out.appendSlice(a, "\\\""),
        '\\' => try out.appendSlice(a, "\\\\"),
        '\n' => try out.appendSlice(a, "\\n"),
        '\r' => try out.appendSlice(a, "\\r"),
        '\t' => try out.appendSlice(a, "\\t"),
        else => if (ch < 0x20) try out.print(a, "\\u{x:0>4}", .{ch}) else try out.append(a, ch),
    };
    try out.append(a, '"');
}

test "decimalText places the point from the scale" {
    const a = std.testing.allocator;
    const cases = [_]struct { raw: i128, scale: u8, want: []const u8 }{
        .{ .raw = 1250, .scale = 2, .want = "12.50" },
        .{ .raw = -5, .scale = 2, .want = "-0.05" },
        .{ .raw = 7, .scale = 0, .want = "7" },
        .{ .raw = 123456789012345678901234567890, .scale = 10, .want = "12345678901234567890.1234567890" },
    };
    for (cases) |c| {
        const got = try decimalText(a, c.raw, c.scale);
        defer a.free(got);
        try std.testing.expectEqualStrings(c.want, got);
    }
}
