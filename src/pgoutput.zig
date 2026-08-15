//! Parser for PostgreSQL logical replication protocol
const std = @import("std");
const type_registry = @import("type_registry.zig");
const array_mod = @import("array.zig");
const ArrayResult = array_mod.ArrayResult;
const ArrayElement = array_mod.ArrayElement;
const numeric_mod = @import("numeric.zig");
const utils = @import("utils.zig");
const PgOid = @import("pg_constants.zig").PgOid;

pub const log = std.log.scoped(.pgoutput);

/// pgoutput protocol parser
///
/// Parses PostgreSQL logical replication binary messages
///
/// Spec: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
pub const PgOutputMessage = union(enum) {
    begin: BeginMessage,
    commit: CommitMessage,
    relation: RelationMessage,
    insert: InsertMessage,
    update: UpdateMessage,
    delete: DeleteMessage,
    origin: OriginMessage,
    type: TypeMessage,
    truncate: TruncateMessage,
};

/// pgoutput message BEGIN structures
pub const BeginMessage = struct {
    final_lsn: u64,
    timestamp: i64,
    xid: u32,
};

/// pgoutput message COMMIT structures
pub const CommitMessage = struct {
    flags: u8,
    commit_lsn: u64,
    end_lsn: u64,
    timestamp: i64,
};

/// Decoded column value representation
pub const DecodedValue = union(enum) {
    null,
    /// pgoutput's `'u'`: a TOASTed value this UPDATE did not modify, so Postgres sent
    /// no bytes for it. **Not a NULL** — the row still holds the old value. Conflating
    /// the two (as this code did) tells a client to overwrite a large text/jsonb column
    /// with NULL every time an unrelated column changes. Encoded by *omitting* the
    /// column from the payload map, which is the same thing Postgres is saying.
    unchanged,
    boolean: bool,
    int32: i32,
    int64: i64,
    float64: f64,
    text: []const u8, // TEXT, VARCHAR, CHAR, UUID, DATE, TIMESTAMPTZ
    numeric: []const u8, // NUMERIC - keep as decimal string
    jsonb: []const u8, // JSONB - as JSON string
    array: []const u8, // ARRAY - as JSON string
    bytea: []const u8, // BYTEA - raw bytes or hex/base64 encoded
};

/// Type tag for packed buffer storage (separate from data)
/// Used in CDCEvent.ColumnView to identify value type without storing the full union
pub const ValueTag = enum(u8) {
    null = 0,
    boolean = 1,
    int32 = 2,
    int64 = 3,
    float64 = 4,
    text = 5,
    numeric = 6,
    jsonb = 7,
    array = 8,
    bytea = 9,
    unchanged = 10,
};

/// Decoded column (name + value pair)
/// Used instead of HashMap for better performance - we only iterate, never lookup by key
pub const Column = struct {
    name: []const u8,
    value: DecodedValue,
};

/// Decodes a scalar of raw bytes based on the PostgreSQL type OID (BINARY format).
///
/// Decodes binary-encoded column data from PostgreSQL when using
/// START_REPLICATION with binary 'true'.
pub fn decodeBinColumnData(
    allocator: std.mem.Allocator,
    type_id: u32,
    raw_bytes: []const u8,
) !DecodedValue {
    const oid: PgOid = @enumFromInt(type_id);

    return switch (oid) {
        // --- Fixed-Width Numeric Types (Binary format) ---
        .BOOL => {
            if (raw_bytes.len != 1) return error.InvalidDataLength;
            return .{ .boolean = raw_bytes[0] != 0 };
        },

        .INT2 => {
            if (raw_bytes.len != 2) return error.InvalidDataLength;
            const val = std.mem.readInt(i16, raw_bytes[0..2], .big);
            return .{ .int32 = @intCast(val) };
        },

        .INT4 => {
            if (raw_bytes.len != 4) return error.InvalidDataLength;
            const val = std.mem.readInt(i32, raw_bytes[0..4], .big);
            return .{ .int32 = val };
        },

        .INT8 => {
            if (raw_bytes.len != 8) return error.InvalidDataLength;
            const val = std.mem.readInt(i64, raw_bytes[0..8], .big);
            return .{ .int64 = val };
        },

        .FLOAT4 => {
            if (raw_bytes.len != 4) return error.InvalidDataLength;
            const bits = std.mem.readInt(u32, raw_bytes[0..4], .big);
            const val: f32 = @bitCast(bits);
            return .{ .float64 = @floatCast(val) };
        },

        .FLOAT8 => {
            if (raw_bytes.len != 8) return error.InvalidDataLength;
            const bits = std.mem.readInt(u64, raw_bytes[0..8], .big);
            const val: f64 = @bitCast(bits);
            return .{ .float64 = val };
        },

        // --- Date/Time Types (Binary format) ---

        .DATE => {
            if (raw_bytes.len != 4) return error.InvalidDataLength;
            const pg_days = std.mem.readInt(i32, raw_bytes[0..4], .big);
            // PostgreSQL epoch: 2000-01-01
            // Convert to Unix epoch (1970-01-01)

            const PG_EPOCH_DAYS: i64 = 10957;
            const unix_days = @as(i64, pg_days) + PG_EPOCH_DAYS;
            // Use efficient civil calendar algorithm
            const date = utils.civilFromDays(unix_days);

            return .{
                // Format as YYYY-MM-DD (always 10 chars)
                .text = try std.fmt.allocPrint(
                    allocator,
                    "{d:0>4}-{d:0>2}-{d:0>2}",
                    .{ @as(u32, @intCast(date.year)), date.month, date.day },
                ),
            };
        },

        // ISO 8601. ⚠️ The suffix differs by type and that is not cosmetic:
        //
        //   TIMESTAMPTZ  is stored as UTC microseconds since 2000-01-01, so `Z` states
        //                a fact PostgreSQL actually recorded.
        //   TIMESTAMP    is a naive wall-clock reading with no zone at all. Appending
        //                `Z` *asserts* it is UTC, which the database never said. It is
        //                accidentally true when the writer happens to store UTC (Ecto's
        //                `:utc_datetime_usec` does, and it produces a naive column), and
        //                silently wrong for any writer that stores local time.
        //
        // Both types were formatted identically until 2026-08-16, so every naive column
        // shipped with a `Z` it had not earned. Clients must not localise a value with
        // no suffix — see PROTOCOL.md §4.
        .TIMESTAMP, .TIMESTAMPTZ => {
            if (raw_bytes.len != 8) return error.InvalidDataLength;
            const microseconds = std.mem.readInt(i64, raw_bytes[0..8], .big);

            const PG_EPOCH_SECONDS: i64 = 946684800;
            const total_seconds = PG_EPOCH_SECONDS + @divFloor(microseconds, 1_000_000);
            const remaining_micros: u32 = @intCast(@abs(@mod(microseconds, 1_000_000)));

            const unix_days = @divFloor(total_seconds, 86400);
            const date = utils.civilFromDays(unix_days);

            const day_seconds = @mod(total_seconds, 86400);
            const hour = @divFloor(day_seconds, 3600);
            const minute = @divFloor(@mod(day_seconds, 3600), 60);
            const second = @mod(day_seconds, 60);

            // Fixed width, so the buffer is a stack array and the only allocation is the
            // final dupe: "YYYY-MM-DDTHH:MM:SS.ffffff" is 26 bytes, +1 for the optional
            // `Z`. allocPrint here was the most repeated allocation in the decoder —
            // most tables carry two timestamp columns per row.
            var buf: [27]u8 = undefined;
            const rendered = try std.fmt.bufPrint(
                &buf,
                "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}{s}",
                .{
                    @as(u32, @intCast(date.year)),
                    date.month,
                    date.day,
                    @as(u8, @intCast(hour)),
                    @as(u8, @intCast(minute)),
                    @as(u8, @intCast(second)),
                    remaining_micros,
                    if (type_id == @intFromEnum(PgOid.TIMESTAMPTZ)) "Z" else "",
                },
            );
            return .{ .text = try allocator.dupe(u8, rendered) };
        },

        // --- UUID (Binary format - 16 bytes) ---
        .UUID => {
            if (raw_bytes.len != 16) return error.InvalidDataLength;
            return .{ .text = try formatUUID(allocator, raw_bytes) };
        },

        // --- Variable-Length Text Types (Binary format is still text) ---
        .TEXT,
        .VARCHAR,
        .BPCHAR,
        .JSON,
        => {
            // raw_bytes is from readBytes() which uses arena_allocator
            // We need to dupe with main_allocator so it survives arena cleanup
            const owned = try allocator.dupe(u8, raw_bytes);
            return .{ .text = owned };
        },
        .BYTEA => {
            // raw_bytes is from readBytes() which uses arena_allocator
            // We need to dupe with main_allocator so it survives arena cleanup
            const owned = try allocator.dupe(u8, raw_bytes);
            return .{ .bytea = owned };
        },

        // --- NUMERIC (Binary format - use numeric.zig) ---
        .NUMERIC => {
            const result = try numeric_mod.parseNumeric(allocator, raw_bytes);
            // parseNumeric returns NumericResult { .slice, .allocated }
            // where .slice is a sub-slice of .allocated pointing to the actual data
            //
            // We dupe the slice and free the original buffer
            const owned = try allocator.dupe(u8, result.slice);
            allocator.free(result.allocated);
            return .{ .numeric = owned };
        },

        // // --- JSON (text format even in binary mode) ---
        // .JSON => {
        //     // PostgreSQL sends JSON as plain text even when binary format is requested
        //     const owned = try allocator.dupe(u8, raw_bytes);
        //     return .{ .jsonb = owned }; // Use jsonb field for compatibility
        // },

        // --- JSONB (binary format is version byte + JSON text) ---
        .JSONB => {
            // PostgreSQL JSONB binary format can be sent in different ways:
            // 1. Standard: version byte (0x01) + JSON text
            // 2. Text output plugin quirk: Sometimes wrapped in quotes: "{"key":"value"}"
            if (raw_bytes.len < 1) return error.InvalidDataLength;

            const first_byte = raw_bytes[0];

            // Check if it starts with a quote (text format wrapper)
            if (first_byte == '"' and raw_bytes.len >= 2) {
                // Strip surrounding quotes: "{"key":"value"}" -> {"key":"value"}
                // Find the closing quote (should be at the end)
                if (raw_bytes[raw_bytes.len - 1] == '"') {
                    // Extract the JSON between quotes
                    const json_text = raw_bytes[1 .. raw_bytes.len - 1];
                    const owned = try allocator.dupe(u8, json_text);
                    return .{ .jsonb = owned };
                } else {
                    log.warn("JSONB starts with quote but doesn't end with quote", .{});
                    // Fall through to version byte handling
                }
            }

            // Standard binary format: version byte + JSON
            if (first_byte != 1) {
                log.warn("Unexpected JSONB first byte: 0x{x:0>2} (expected 0x01 or '\"')", .{first_byte});
                // Try to use it anyway, might be valid JSON
                const owned = try allocator.dupe(u8, raw_bytes);
                return .{ .jsonb = owned };
            }

            // Skip version byte, return JSON text
            const owned = try allocator.dupe(u8, raw_bytes[1..]);
            return .{ .jsonb = owned };
        },

        // --- Array Types (Binary format - use array.zig) ---
        .ARRAY_INT2, .ARRAY_INT4, .ARRAY_INT8, .ARRAY_TEXT, .ARRAY_VARCHAR, .ARRAY_FLOAT4, .ARRAY_FLOAT8, .ARRAY_BOOL, .ARRAY_JSONB, .ARRAY_NUMERIC, .ARRAY_UUID, .ARRAY_TIMESTAMPTZ => {
            const parsed = try array_mod.parseArray(allocator, raw_bytes);
            defer parsed.deinit(allocator);

            // Convert ArrayResult to PostgreSQL text format: {1,2,3}
            const text_repr = try arrayToText(allocator, parsed);
            return .{ .array = text_repr };
        },

        // --- Handle ENUMs and unknown types gracefully ---
        else => {
            // ENUMs and other user-defined types are sent as text in binary format
            // Treat them as TEXT and log a warning for visibility
            log.warn("Unknown type OID {d}, treating as text", .{type_id});
            const owned = try allocator.dupe(u8, raw_bytes);
            return .{ .text = owned };
        },
    };
}

/// Convert ArrayResult to PostgreSQL array style text format '{{1,2}, {3,4}}'
pub fn arrayToText(allocator: std.mem.Allocator, arr: ArrayResult) ![]u8 {
    var res: std.ArrayList(u8) = .empty;
    _ = try writeArrayRecursive(&res, allocator, arr, 0, 0);
    return res.toOwnedSlice(allocator);
}

fn writeArrayRecursive(
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    arr: ArrayResult,
    dim: usize,
    index: usize,
) !usize {
    const dims = arr.dimensions;
    const ndim = arr.ndim;

    const count: usize = @intCast(dims[dim]);

    var flat_index = index;

    try out.append(allocator, '{');

    var i: usize = 0;
    while (i < count) : (i += 1) {
        if (i > 0) try out.append(allocator, ',');

        if (dim + 1 == ndim) {
            // leaf element
            const elem = arr.elements[flat_index];
            switch (elem) {
                .null_value => try out.appendSlice(allocator, "NULL"),
                .data => blk: {
                    const pg_oid: PgOid = @enumFromInt(arr.element_oid);

                    // BYTEA: \xHEXSTRING format
                    if (pg_oid == PgOid.BYTEA) {
                        const hex = try elementToText(allocator, pg_oid, elem.data);
                        defer allocator.free(hex);
                        try out.appendSlice(allocator, "\\x");
                        try out.appendSlice(allocator, hex);
                        break :blk;
                    }

                    // UUID: formatted string (no quotes needed in array)
                    if (pg_oid == PgOid.UUID) {
                        const uuid_str = try elementToText(allocator, pg_oid, elem.data);
                        defer allocator.free(uuid_str);
                        try out.appendSlice(allocator, uuid_str);
                        break :blk;
                    }

                    // TEXT/VARCHAR: requires quotes and escaping
                    if (pg_oid == PgOid.TEXT or pg_oid == PgOid.VARCHAR) {
                        const txt = try elementToText(allocator, pg_oid, elem.data);
                        defer allocator.free(txt);

                        try out.append(allocator, '"');
                        for (txt) |c| {
                            if (c == '"' or c == '\\')
                                try out.append(allocator, '\\');
                            try out.append(allocator, c);
                        }
                        try out.append(allocator, '"');
                        break :blk;
                    }

                    // All other types (INT, FLOAT, BOOL, etc.): just append as-is
                    const s = try elementToText(allocator, pg_oid, elem.data);
                    defer allocator.free(s);
                    try out.appendSlice(allocator, s);
                },
            }
            flat_index += 1;
        } else {
            // recurse into next dimension
            flat_index = try writeArrayRecursive(out, allocator, arr, dim + 1, flat_index);
        }
    }

    try out.append(allocator, '}');

    return flat_index;
}

// Array element decoding
/// ⚠️ Every fixed-width branch checks its length first. `data[0..4]` on a shorter slice
/// is a bounds check in Debug and an **out-of-bounds read in ReleaseFast**, which is the
/// mode this runs in — so a truncated or unexpected array element would read past the
/// buffer rather than fail.
fn elementToText(allocator: std.mem.Allocator, pg_oid: PgOid, data: []const u8) ![]u8 {
    switch (pg_oid) {
        .INT2 => {
            if (data.len != 2) return error.InvalidDataLength;
            const v = std.mem.readInt(i16, data[0..2], .big);
            return std.fmt.allocPrint(allocator, "{}", .{v});
        },
        .INT4 => {
            if (data.len != 4) return error.InvalidDataLength;
            const v = std.mem.readInt(i32, data[0..4], .big);
            return std.fmt.allocPrint(allocator, "{}", .{v});
        },
        .INT8 => {
            if (data.len != 8) return error.InvalidDataLength;
            const v = std.mem.readInt(i64, data[0..8], .big);
            return std.fmt.allocPrint(allocator, "{}", .{v});
        },
        .FLOAT4 => {
            if (data.len != 4) return error.InvalidDataLength;
            const bits = std.mem.readInt(u32, data[0..4], .big);
            const v: f32 = @bitCast(bits);
            return std.fmt.allocPrint(allocator, "{}", .{v});
        },
        .FLOAT8 => {
            if (data.len != 8) return error.InvalidDataLength;
            const bits = std.mem.readInt(u64, data[0..8], .big);
            const v: f64 = @bitCast(bits);
            return std.fmt.allocPrint(allocator, "{}", .{v});
        },
        .TEXT, .VARCHAR => return allocator.dupe(u8, data),
        .BYTEA => {
            // Encode as lowercase hex format
            return formatBYTEA(allocator, data);
        },
        .UUID => {
            if (data.len != 16) return error.InvalidDataLength;
            return try formatUUID(allocator, data);
        },
        else => return allocator.dupe(u8, data),
    }
}

fn formatBYTEA(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    var hex_buf = try allocator.alloc(u8, data.len * 2);
    var pos: usize = 0;
    for (data) |b| {
        const hi: u8 = @intCast((b >> 4) & 0xF);
        const lo: u8 = @intCast(b & 0xF);
        hex_buf[pos] = if (hi < 10) '0' + hi else 'a' + hi - 10;
        pos += 1;
        hex_buf[pos] = if (lo < 10) '0' + lo else 'a' + lo - 10;
        pos += 1;
    }
    return hex_buf;
}

// 4-2-2-2-6 hex digit format with dashes, 128bits = 16x8 bits
fn formatUUID(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    // UUID is 16 bytes
    var res = try allocator.alloc(u8, 36);
    var pos: usize = 0;

    for (data, 0..) |b, i| {
        if (i == 4 or i == 6 or i == 8 or i == 10) {
            res[pos] = '-';
            pos += 1;
        }
        // Write byte as two hex digits
        utils.byteToHex(res[pos..][0..2], b);
        pos += 2;
    }

    return res;
}

/// One column as it arrived on the wire, keeping pgoutput's per-column format byte.
///
/// The parser used to collapse this to `?[]u8`: `'t'` and `'b'` took identical branches
/// and `'n'`/`'u'` both became `null`. Both losses were silent corruption downstream —
/// text bytes decoded as if binary, and unchanged TOAST values delivered as NULL — so
/// the distinction is now in the type rather than in a comment.
pub const ColumnValue = union(enum) {
    /// `'n'` — SQL NULL.
    null,
    /// `'u'` — TOASTed and unmodified by this UPDATE; no bytes were sent.
    unchanged,
    /// `'t'` — text format. Postgres falls back to this per column even under
    /// `binary 'true'` when the type has no `typsend`, so it is not a rare path.
    text: []u8,
    /// `'b'` — binary format, to be decoded against the column's type OID.
    binary: []u8,
};

pub const TupleData = struct {
    cols: []ColumnValue,

    pub fn deinit(self: *TupleData, allocator: std.mem.Allocator) void {
        for (self.cols) |col| {
            switch (col) {
                .text, .binary => |data| allocator.free(data),
                .null, .unchanged => {},
            }
        }
        allocator.free(self.cols);
    }
};

/// Decode one CDC tuple.
///
/// `type_guard` decides what to do with a type the decoder's OID switch does not cover.
/// Passing null keeps the old "treat unknown as text" behaviour and is only for tests —
/// the live path must supply a registry, because that fallback is silent corruption for
/// anything that is not an enum (see type_registry.zig).
pub fn decodeTuple(
    allocator: std.mem.Allocator,
    tuple: TupleData,
    columns: []const RelationMessage.ColumnInfo,
    type_guard: ?*type_registry.Registry,
) !std.ArrayList(Column) {
    if (tuple.cols.len != columns.len) return error.ColumnMismatch;

    var decoded_columns: std.ArrayList(Column) = .empty;
    errdefer decoded_columns.deinit(allocator);

    // Pre-allocate capacity for all columns
    try decoded_columns.ensureTotalCapacity(allocator, columns.len);

    for (columns, tuple.cols) |col_info, col_data| {
        // Column name points to RelationMessage which has stable lifetime
        // No need to duplicate - it lives for the entire relation cache
        const value: DecodedValue = switch (col_data) {
            .null => .null,
            .unchanged => .unchanged,
            // Already text, whatever the type is. Postgres sends `'t'` when the type
            // has no binary send function, and its text output is exactly what a
            // consumer wants — so this needs no OID knowledge and cannot be corrupted
            // by lacking it.
            .text => |raw_bytes| .{ .text = try allocator.dupe(u8, raw_bytes) },
            .binary => |raw_bytes| blk: {
                if (type_guard) |guard| switch (guard.verdict(col_info.type_id)) {
                    .decode => {},
                    // Postgres sends an enum's label, so the bytes already are the text.
                    .text_passthrough => break :blk DecodedValue{ .text = try allocator.dupe(u8, raw_bytes) },
                    .refuse => {
                        log.err(
                            "🔴 column '{s}' has type OID {d}, which the decoder does not implement and is not an enum — refusing rather than shipping its raw binary as a string",
                            .{ col_info.name, col_info.type_id },
                        );
                        return error.UnsupportedColumnType;
                    },
                };

                break :blk decodeBinColumnData(allocator, col_info.type_id, raw_bytes) catch |err| {
                    log.err("Failed to decode column '{s}' (type_id={d}, bytes={d}): {}", .{ col_info.name, col_info.type_id, raw_bytes.len, err });
                    return err;
                };
            },
        };

        decoded_columns.appendAssumeCapacity(Column{ .name = col_info.name, .value = value });
    }
    return decoded_columns;
}

pub const RelationMessage = struct {
    relation_id: u32,
    namespace: []const u8,
    name: []const u8,
    replica_identity: u8,
    columns: []ColumnInfo,

    pub const ColumnInfo = struct {
        flags: u8,
        name: []const u8,
        type_id: u32,
        type_modifier: i32,
    };

    pub fn deinit(self: *RelationMessage, allocator: std.mem.Allocator) void {
        allocator.free(self.namespace);
        allocator.free(self.name);
        for (self.columns) |col| {
            allocator.free(col.name);
        }
        allocator.free(self.columns);
    }

    pub fn clone(self: *const RelationMessage, allocator: std.mem.Allocator) !*RelationMessage {
        var out = try allocator.create(RelationMessage);
        errdefer allocator.destroy(out);

        out.* = self.*;

        // Deep copy namespace
        out.namespace = try allocator.dupe(u8, self.namespace);
        errdefer allocator.free(out.namespace);

        // Deep copy name
        out.name = try allocator.dupe(u8, self.name);
        errdefer allocator.free(out.name);

        // Deep copy columns array
        out.columns = try allocator.alloc(ColumnInfo, self.columns.len);
        errdefer allocator.free(out.columns);

        for (self.columns, 0..) |col, i| {
            const cloned_col_name = try allocator.dupe(u8, col.name);
            errdefer {
                // Free previously cloned column names on error
                for (out.columns[0..i]) |c| {
                    allocator.free(c.name);
                }
            }
            out.columns[i] = ColumnInfo{
                .flags = col.flags,
                .name = cloned_col_name,
                .type_id = col.type_id,
                .type_modifier = col.type_modifier,
            };
        }

        return out;
    }
};

pub const InsertMessage = struct {
    relation_id: u32,
    tuple_data: TupleData,

    pub fn deinit(self: *InsertMessage, allocator: std.mem.Allocator) void {
        self.tuple_data.deinit(allocator);
    }
};

pub const UpdateMessage = struct {
    relation_id: u32,
    old_tuple: ?TupleData,
    new_tuple: TupleData,

    pub fn deinit(self: *UpdateMessage, allocator: std.mem.Allocator) void {
        if (self.old_tuple) |*old| old.deinit(allocator);

        self.new_tuple.deinit(allocator);
    }
};

pub const DeleteMessage = struct {
    relation_id: u32,
    old_tuple: TupleData,

    pub fn deinit(self: *DeleteMessage, allocator: std.mem.Allocator) void {
        self.old_tuple.deinit(allocator);
    }
};

pub const OriginMessage = struct {
    commit_lsn: u64,
    name: []const u8,
};

pub const TypeMessage = struct {
    type_id: u32,
    namespace: []const u8,
    name: []const u8,
};

pub const TruncateMessage = struct {
    options: u8,
    relation_ids: []u32,
};

/// Parser state for decoding pgoutput messages
///
/// Handles reading from a byte slice and parsing messages sequentially
pub const Parser = struct {
    allocator: std.mem.Allocator,
    data: []const u8,
    pos: usize,

    pub fn init(allocator: std.mem.Allocator, data: []const u8) Parser {
        return .{
            .allocator = allocator,
            .data = data,
            .pos = 0,
        };
    }

    /// Parse the next pgoutput message from the data based on message type
    ///
    /// Source: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-MESSAGE-TYPE
    pub fn parse(self: *Parser) !PgOutputMessage {
        if (self.pos >= self.data.len) {
            return error.EndOfData;
        }

        const msg_type = self.data[self.pos];
        self.pos += 1;

        return switch (msg_type) {
            'B' => .{ .begin = try self.parseBegin() },
            'C' => .{ .commit = try self.parseCommit() },
            'R' => .{ .relation = try self.parseRelation() },
            'I' => .{ .insert = try self.parseInsert() },
            'U' => .{ .update = try self.parseUpdate() },
            'D' => .{ .delete = try self.parseDelete() },
            'O' => .{ .origin = try self.parseOrigin() },
            'Y' => .{ .type = try self.parseType() },
            'T' => .{ .truncate = try self.parseTruncate() },
            else => {
                log.warn("Unknown pgoutput message type: {c} (0x{x})", .{ msg_type, msg_type });
                return error.UnknownMessageType;
            },
        };
    }

    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-BEGIN
    fn parseBegin(self: *Parser) !BeginMessage {
        const final_lsn = try self.readU64();
        const timestamp = try self.readI64();
        const xid = try self.readU32();

        return BeginMessage{
            .final_lsn = final_lsn,
            .timestamp = timestamp,
            .xid = xid,
        };
    }

    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-COMMIT
    fn parseCommit(self: *Parser) !CommitMessage {
        const flags = try self.readU8();
        const commit_lsn = try self.readU64();
        const end_lsn = try self.readU64();
        const timestamp = try self.readI64();

        return CommitMessage{
            .flags = flags,
            .commit_lsn = commit_lsn,
            .end_lsn = end_lsn,
            .timestamp = timestamp,
        };
    }

    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-RELATION
    fn parseRelation(self: *Parser) !RelationMessage {
        const relation_id = try self.readU32();
        const namespace = try self.readString();
        const name = try self.readString();
        const replica_identity = try self.readU8();
        const num_columns = try self.readU16();

        var columns_ptr = try self.allocator.alloc(
            RelationMessage.ColumnInfo,
            num_columns,
        );
        errdefer self.allocator.free(columns_ptr);

        var i: usize = 0;
        while (i < num_columns) : (i += 1) {
            const flags = try self.readU8();
            const col_name = try self.readString();
            const type_id = try self.readU32();
            const type_modifier = try self.readI32();

            columns_ptr[i] = .{
                .flags = flags,
                .name = col_name,
                .type_id = type_id,
                .type_modifier = type_modifier,
            };
        }

        return RelationMessage{
            .relation_id = relation_id,
            .namespace = namespace,
            .name = name,
            .replica_identity = replica_identity,
            .columns = columns_ptr[0..num_columns],
        };
    }

    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-INSERT
    fn parseInsert(self: *Parser) !InsertMessage {
        const relation_id = try self.readU32();
        const tuple_type = try self.readU8();

        if (tuple_type != 'N') {
            log.warn("Expected 'N' (new tuple), got: {c}", .{tuple_type});
            return error.InvalidTupleType;
        }

        const tuple_data = try self.parseTupleData();

        return InsertMessage{
            .relation_id = relation_id,
            .tuple_data = tuple_data,
        };
    }

    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-UPDATE
    fn parseUpdate(self: *Parser) !UpdateMessage {
        const relation_id = try self.readU32();
        const key_or_old = try self.readU8();

        var old_tuple: ?TupleData = null;

        if (key_or_old == 'K' or key_or_old == 'O') {
            // 'K' = old key tuple, 'O' = old full tuple
            old_tuple = try self.parseTupleData();

            // After old tuple, next byte MUST be 'N' for new tuple
            const new_marker = try self.readU8();
            if (new_marker != 'N') {
                log.warn("Expected 'N' (new tuple) after old tuple, got: {c} (0x{x})", .{ new_marker, new_marker });
                return error.InvalidTupleType;
            }
        } else if (key_or_old != 'N') {
            // If not 'K', 'O', or 'N', this is invalid
            log.warn("Invalid UPDATE tuple marker: {c} (0x{x}), expected 'K', 'O', or 'N'", .{ key_or_old, key_or_old });
            return error.InvalidTupleType;
        }
        // else: key_or_old == 'N', no old tuple, proceed directly to new tuple

        const new_tuple = try self.parseTupleData();

        return UpdateMessage{
            .relation_id = relation_id,
            .old_tuple = old_tuple,
            .new_tuple = new_tuple,
        };
    }

    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-DELETE
    fn parseDelete(self: *Parser) !DeleteMessage {
        const relation_id = try self.readU32();
        const key_or_old = try self.readU8();

        if (key_or_old != 'K' and key_or_old != 'O') {
            log.warn("Expected 'K' or 'O', got: {c}", .{key_or_old});
            return error.InvalidTupleType;
        }

        const old_tuple = try self.parseTupleData();

        return DeleteMessage{
            .relation_id = relation_id,
            .old_tuple = old_tuple,
        };
    }

    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-ORIGIN
    fn parseOrigin(self: *Parser) !OriginMessage {
        const commit_lsn = try self.readU64();
        const name = try self.readString();

        return OriginMessage{
            .commit_lsn = commit_lsn,
            .name = name,
        };
    }

    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-TYPE
    fn parseType(self: *Parser) !TypeMessage {
        const type_id = try self.readU32();
        const namespace = try self.readString();
        const name = try self.readString();

        return TypeMessage{
            .type_id = type_id,
            .namespace = namespace,
            .name = name,
        };
    }

    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-TRUNCATE
    fn parseTruncate(self: *Parser) !TruncateMessage {
        const options = try self.readU8();
        const num_relations = try self.readU32();

        var relation_ids = try self.allocator.alloc(u32, num_relations);
        errdefer self.allocator.free(relation_ids);

        var i: usize = 0;
        while (i < num_relations) : (i += 1) {
            relation_ids[i] = try self.readU32();
        }

        return TruncateMessage{
            .options = options,
            .relation_ids = relation_ids,
        };
    }

    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-TUPLEDATA
    fn parseTupleData(self: *Parser) !TupleData {
        const num_columns = try self.readU16();
        const ncols: usize = @intCast(num_columns);
        const cols_ptr = try self.allocator.alloc(ColumnValue, ncols);

        // Initialise before the errdefer can observe it: an early error would otherwise
        // walk undefined memory deciding what to free.
        @memset(cols_ptr, .null);

        // `errdefer`, not `defer` + a "should I clean up?" flag. The flag version had to
        // be flipped by hand on the success path, which is one edit away from freeing
        // the tuple it just returned.
        errdefer {
            for (cols_ptr) |col| {
                switch (col) {
                    .text, .binary => |data| self.allocator.free(data),
                    .null, .unchanged => {},
                }
            }
            self.allocator.free(cols_ptr);
        }

        var i: usize = 0;
        while (i < ncols) : (i += 1) {
            const kind_byte = try self.readU8();
            cols_ptr[i] = switch (kind_byte) {
                'n' => .null,
                // Distinct from NULL: the value exists, Postgres just did not resend it.
                'u' => .unchanged,
                't' => .{ .text = try self.readBytesOwned(try self.readU32()) },
                'b' => .{ .binary = try self.readBytesOwned(try self.readU32()) },
                else => return error.UnknownColumnType,
            };
        }
        return TupleData{ .cols = cols_ptr };
    }

    /// Read a single byte
    fn readU8(self: *Parser) !u8 {
        if (self.pos >= self.data.len) return error.UnexpectedEndOfData;
        const val = self.data[self.pos];
        self.pos += 1;
        return val;
    }

    /// Read a 16-bit unsigned integer in big-endian order
    fn readU16(self: *Parser) !u16 {
        if (self.pos + 2 > self.data.len) return error.UnexpectedEndOfData;
        const array_ptr: *const [2]u8 = self.data[self.pos..][0..2];
        const val = std.mem.readInt(u16, array_ptr, .big);
        self.pos += 2;
        return val;
    }

    /// Read a 32-bit unsigned integer in big-endian order
    fn readU32(self: *Parser) !u32 {
        if (self.pos + 4 > self.data.len) return error.UnexpectedEndOfData;
        const array_ptr = self.data[self.pos..][0..4];
        const val = std.mem.readInt(u32, array_ptr, .big);
        self.pos += 4;
        return val;
    }

    /// Read a 32-bit signed integer in big-endian order
    fn readI32(self: *Parser) !i32 {
        if (self.pos + 4 > self.data.len) return error.UnexpectedEndOfData;
        const array_ptr = self.data[self.pos..][0..4];
        const val = std.mem.readInt(i32, array_ptr, .big);
        self.pos += 4;
        return val;
    }

    /// Read a 64-bit unsigned integer in big-endian order
    fn readU64(self: *Parser) !u64 {
        if (self.pos + 8 > self.data.len) return error.UnexpectedEndOfData;
        const array_ptr = self.data[self.pos..][0..8];
        const val = std.mem.readInt(u64, array_ptr, .big);
        self.pos += 8;
        return val;
    }

    /// Read a 64-bit signed integer in big-endian order
    fn readI64(self: *Parser) !i64 {
        if (self.pos + 8 > self.data.len) return error.UnexpectedEndOfData;
        const array_ptr = self.data[self.pos..][0..8];
        const val = std.mem.readInt(i64, array_ptr, .big);
        self.pos += 8;
        return val;
    }

    /// Read a null-terminated string
    ///
    /// Caller is responsible for freeing the returned string
    fn readString(self: *Parser) ![]const u8 {
        const start = self.pos;
        while (self.pos < self.data.len and self.data[self.pos] != 0) {
            self.pos += 1;
        }

        if (self.pos >= self.data.len) return error.UnexpectedEndOfData;

        const str = self.data[start..self.pos];
        self.pos += 1; // Skip null terminator

        return try self.allocator.dupe(u8, str);
    }

    /// Read `len` bytes and return an owned byte mutable slice
    ///
    /// Caller is responsible for freeing the returned byte slice
    fn readBytesOwned(self: *Parser, len: u32) ![]u8 {
        const len_usize: usize = @intCast(len);
        const end = self.pos + len_usize;
        if (end > self.data.len) return error.UnexpectedEndOfData;

        const bytes = self.data[self.pos..end];
        self.pos = end;

        const dst = try self.allocator.alloc(u8, len_usize);
        @memcpy(dst, bytes);
        return dst;
    }
};

// === Tests ===

const parseArray = array_mod.parseArray;

inline fn writeIntBE(comptime T: type, buf: []u8, value: T, endian: std.builtin.Endian) void {
    const ptr: *[@divExact(@typeInfo(T).int.bits, 8)]u8 = @ptrCast(buf.ptr);
    std.mem.writeInt(T, ptr, value, endian);
}

test "1D int4 → {1,2,3,4}" {
    const alloc = std.testing.allocator;

    const buf = [_]u8{
        0, 0, 0, 1, // ndim
        0, 0, 0, 0, // flags
        0, 0, 0, 23, // element_oid = INT4
        0, 0, 0, 4, // dim1_len
        0, 0, 0, 1, // dim1_lower
        0, 0, 0, 4,
        0, 0, 0, 1,
        0, 0, 0, 4,
        0, 0, 0, 2,
        0, 0, 0, 4,
        0, 0, 0, 3,
        0, 0, 0, 4,
        0, 0, 0, 4,
    };

    var arr = try parseArray(alloc, &buf);
    defer arr.deinit(alloc);

    const txt = try arrayToText(alloc, arr);
    defer alloc.free(txt);

    try std.testing.expectEqualStrings("{1,2,3,4}", txt);
}

test "2D int4 → {{1,2},{3,4}}" {
    const alloc = std.testing.allocator;

    const buf = [_]u8{
        0, 0, 0, 2, // ndim = 2
        0, 0, 0, 0,
        0, 0, 0, 23, // INT4
        0, 0, 0, 2, 0, 0, 0, 1, // dim1_len, lower
        0, 0, 0, 2, 0, 0, 0, 1, // dim2_len, lower

        // (1,1)
        0, 0, 0, 4, 0, 0, 0, 1,
        // (1,2)
        0, 0, 0, 4, 0, 0, 0, 2,
        // (2,1)
        0, 0, 0, 4, 0, 0, 0, 3,
        // (2,2)
        0, 0, 0, 4, 0, 0, 0, 4,
    };

    var arr = try parseArray(alloc, &buf);
    defer arr.deinit(alloc);

    const txt = try arrayToText(alloc, arr);
    defer alloc.free(txt);

    try std.testing.expectEqualStrings("{{1,2},{3,4}}", txt);
}

test "3D int4 → {{{1,2},{3,4}},{{5,6},{7,8}}}" {
    const alloc = std.testing.allocator;

    // 3D: dims = [2,2,2], flattened [1..8]
    const buf = blk: {
        var b: [136]u8 = undefined;
        var pos: usize = 0;

        // ndim = 3
        writeIntBE(i32, b[pos .. pos + 4], 3, .big);
        pos += 4;
        // flags
        writeIntBE(i32, b[pos .. pos + 4], 0, .big);
        pos += 4;
        // oid INT4 = 23
        writeIntBE(u32, b[pos .. pos + 4], 23, .big);
        pos += 4;

        // dim1 len=2, lower=1
        writeIntBE(i32, b[pos .. pos + 4], 2, .big);
        pos += 4;
        writeIntBE(i32, b[pos .. pos + 4], 1, .big);
        pos += 4;

        // dim2 len=2, lower=1
        writeIntBE(i32, b[pos .. pos + 4], 2, .big);
        pos += 4;
        writeIntBE(i32, b[pos .. pos + 4], 1, .big);
        pos += 4;

        // dim3 len=2, lower=1
        writeIntBE(i32, b[pos .. pos + 4], 2, .big);
        pos += 4;
        writeIntBE(i32, b[pos .. pos + 4], 1, .big);
        pos += 4;

        var x: i32 = 1;
        for (0..8) |_| {
            writeIntBE(i32, b[pos .. pos + 4], 4, .big);
            pos += 4;
            writeIntBE(i32, b[pos .. pos + 4], x, .big);
            pos += 4;
            x += 1;
        }

        break :blk b;
    };

    var arr = try parseArray(alloc, &buf);
    defer arr.deinit(alloc);

    const txt = try arrayToText(alloc, arr);
    defer alloc.free(txt);

    try std.testing.expectEqualStrings(
        "{{{1,2},{3,4}},{{5,6},{7,8}}}",
        txt,
    );
}

test "NULL elements → {1,NULL,3}" {
    const alloc = std.testing.allocator;

    const buf = [_]u8{
        0,   0,   0,   1, // ndim = 1
        0,   0,   0,   0, // flags
        0,   0,   0,   23, // INT4
        0,   0,   0,   3, // dim_len = 3 elements
        0,   0,   0,   1, // dim_lower = 1
        // Element 1: length=4, value=1
        0,   0,   0,   4,
        0,   0,   0,   1,
        // Element 2: length=-1 (NULL)
        255, 255, 255, 255,
        // Element 3: length=4, value=3
        0,   0,   0,   4,
        0,   0,   0,   3,
    };

    var arr = try parseArray(alloc, &buf);
    defer arr.deinit(alloc);

    const txt = try arrayToText(alloc, arr);
    defer alloc.free(txt);

    try std.testing.expectEqualStrings("{1,NULL,3}", txt);
}

test "TEXT array escaping → {\"a\",\"b c\",\"d\\\"e\"}" {
    const alloc = std.testing.allocator;

    const buf = [_]u8{
        0, 0, 0, 1,
        0, 0, 0, 0,
        0,   0,   0,   25, // TEXT
        0,   0,   0,   3,
        0,   0,   0,   1,
        // "a"
        0,   0,   0,   1,
        'a',
        // "b c"
        0,   0,   0,
        3,   'b', ' ', 'c',
        // "d" e → d\"e
        0,   0,   0,   3,
        'd', '"', 'e',
    };

    var arr = try parseArray(alloc, &buf);
    defer arr.deinit(alloc);

    const txt = try arrayToText(alloc, arr);
    defer alloc.free(txt);

    try std.testing.expectEqualStrings(
        "{\"a\",\"b c\",\"d\\\"e\"}",
        txt,
    );
}

test "BYTEA array → {\\xdeadbeef,\\x01020304}" {
    const alloc = std.testing.allocator;

    const buf = [_]u8{
        0, 0, 0, 1, // ndim
        0, 0, 0, 0,
        0,    0,    0,    17, // BYTEA
        0,    0,    0,    2,
        0,    0,    0,    1,

        // element 1: 4 bytes
        0,    0,    0,    4,
        0xDE, 0xAD, 0xBE, 0xEF,

        // element 2: 4 bytes
        0,    0,    0,    4,
        0x01, 0x02, 0x03, 0x04,
    };

    var arr = try parseArray(alloc, &buf);
    defer arr.deinit(alloc);

    const txt = try arrayToText(alloc, arr);
    defer alloc.free(txt);

    try std.testing.expectEqualStrings(
        "{\\xdeadbeef,\\x01020304}",
        txt,
    );
}

test "FLOAT8 array → {1.5,2.25,3.75}" {
    const alloc = std.testing.allocator;

    var buf: [4 + 4 + 4 + 8 + 8 + 8 + 8 + 8 + 8]u8 = undefined;
    var pos: usize = 0;

    // ndim = 1
    writeIntBE(i32, buf[pos .. pos + 4], 1, .big);
    pos += 4;
    writeIntBE(i32, buf[pos .. pos + 4], 0, .big);
    pos += 4;
    writeIntBE(u32, buf[pos .. pos + 4], @intFromEnum(PgOid.FLOAT8), .big);
    pos += 4;

    writeIntBE(i32, buf[pos .. pos + 4], 3, .big);
    pos += 4;
    writeIntBE(i32, buf[pos .. pos + 4], 1, .big);
    pos += 4;

    const floats = [_]f64{ 1.5, 2.25, 3.75 };

    for (floats) |f| {
        writeIntBE(i32, buf[pos .. pos + 4], 8, .big);
        pos += 4;
        const bits: u64 = @bitCast(f);
        writeIntBE(u64, buf[pos .. pos + 8], bits, .big);
        pos += 8;
    }

    var arr = try parseArray(alloc, &buf);
    defer arr.deinit(alloc);

    const txt = try arrayToText(alloc, arr);
    defer alloc.free(txt);

    try std.testing.expectEqualStrings("{1.5,2.25,3.75}", txt);
}

test "UUID array → {uuid1,uuid2}" {
    const alloc = std.testing.allocator;

    const uuid1 = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF };
    const uuid2 = [_]u8{ 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0, 0xC0, 0xD0, 0xE0, 0xF0, 0xFF };

    var buf: [4 + 4 + 4 + 8 + 4 + 16 + 4 + 16]u8 = undefined;
    var pos: usize = 0;

    writeIntBE(i32, buf[pos .. pos + 4], 1, .big);
    pos += 4;
    writeIntBE(i32, buf[pos .. pos + 4], 0, .big);
    pos += 4;
    writeIntBE(u32, buf[pos .. pos + 4], @intFromEnum(PgOid.UUID), .big);
    pos += 4;
    writeIntBE(i32, buf[pos .. pos + 4], 2, .big);
    pos += 4;
    writeIntBE(i32, buf[pos .. pos + 4], 1, .big);
    pos += 4;

    writeIntBE(i32, buf[pos .. pos + 4], 16, .big);
    pos += 4;
    @memcpy(buf[pos .. pos + 16], &uuid1);
    pos += 16;

    writeIntBE(i32, buf[pos .. pos + 4], 16, .big);
    pos += 4;
    @memcpy(buf[pos .. pos + 16], &uuid2);
    pos += 16;

    var arr = try parseArray(alloc, buf[0..pos]);
    defer arr.deinit(alloc);

    const out = try arrayToText(alloc, arr);
    defer alloc.free(out);

    // PostgreSQL format
    try std.testing.expectEqualStrings(
        "{00010203-0405-0607-0809-aabbccddeeff,10203040-5060-7080-90a0-b0c0d0e0f0ff}",
        out,
    );
}

// ---------------------------------------------------------------------------
// TupleData framing — the per-column format byte.
//
// These pin the two distinctions the parser used to drop on the floor: `'t'` vs `'b'`
// (which decides whether the bytes go through the OID decoder at all) and `'u'` vs
// `'n'` (which decides whether a client keeps or erases a TOASTed column).
// ---------------------------------------------------------------------------

/// Build a TupleData wire fragment: column count, then one entry per column.
fn buildTuple(buf: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, cols: []const ColumnValue) !void {
    var count: [2]u8 = undefined;
    std.mem.writeInt(u16, &count, @intCast(cols.len), .big);
    try buf.appendSlice(alloc, &count);

    for (cols) |col| {
        switch (col) {
            .null => try buf.append(alloc, 'n'),
            .unchanged => try buf.append(alloc, 'u'),
            .text, .binary => |data| {
                try buf.append(alloc, if (col == .text) 't' else 'b');
                var len: [4]u8 = undefined;
                std.mem.writeInt(u32, &len, @intCast(data.len), .big);
                try buf.appendSlice(alloc, &len);
                try buf.appendSlice(alloc, data);
            },
        }
    }
}

test "tuple - each format byte keeps its own meaning" {
    const alloc = std.testing.allocator;

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(alloc);
    try buildTuple(&buf, alloc, &.{
        .null,
        .unchanged,
        .{ .text = @constCast("42") },
        .{ .binary = @constCast(&[_]u8{ 0, 0, 0, 42 }) },
    });

    var parser = Parser.init(alloc, buf.items);
    var tuple = try parser.parseTupleData();
    defer tuple.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 4), tuple.cols.len);
    try std.testing.expect(tuple.cols[0] == .null);
    try std.testing.expect(tuple.cols[1] == .unchanged);
    try std.testing.expectEqualStrings("42", tuple.cols[2].text);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 0, 0, 42 }, tuple.cols[3].binary);
}

test "tuple - an unchanged TOAST value is not a NULL" {
    // The whole point: before this, both arrived as `null` and a client applying the
    // UPDATE would erase a column PostgreSQL never touched.
    const alloc = std.testing.allocator;

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(alloc);
    try buildTuple(&buf, alloc, &.{ .null, .unchanged });

    var parser = Parser.init(alloc, buf.items);
    var tuple = try parser.parseTupleData();
    defer tuple.deinit(alloc);

    try std.testing.expect(tuple.cols[0] == .null);
    try std.testing.expect(tuple.cols[1] == .unchanged);
}

test "decodeTuple - text columns bypass the OID decoder" {
    // A `'t'` column is already Postgres's own text output, so it is correct without
    // any knowledge of the type. The OID here is deliberately one the decoder does not
    // know: under the old code these bytes went through decodeBinColumnData anyway.
    const alloc = std.testing.allocator;

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(alloc);
    try buildTuple(&buf, alloc, &.{ .{ .text = @constCast("a=>1") }, .unchanged, .null });

    var parser = Parser.init(alloc, buf.items);
    var tuple = try parser.parseTupleData();
    defer tuple.deinit(alloc);

    const columns = [_]RelationMessage.ColumnInfo{
        .{ .flags = 0, .name = "attrs", .type_id = 16416, .type_modifier = -1 },
        .{ .flags = 0, .name = "body", .type_id = 25, .type_modifier = -1 },
        .{ .flags = 0, .name = "note", .type_id = 25, .type_modifier = -1 },
    };

    var decoded = try decodeTuple(alloc, tuple, &columns, null);
    defer {
        for (decoded.items) |col| switch (col.value) {
            .text, .numeric, .jsonb, .array, .bytea => |v| alloc.free(v),
            else => {},
        };
        decoded.deinit(alloc);
    }

    try std.testing.expectEqualStrings("a=>1", decoded.items[0].value.text);
    try std.testing.expect(decoded.items[1].value == .unchanged);
    try std.testing.expect(decoded.items[2].value == .null);
}

test "TIMESTAMPTZ keeps its Z, TIMESTAMP does not" {
    const alloc = std.testing.allocator;

    // 2025-10-26T10:00:00.000000 as microseconds since the 2000 epoch.
    var buf: [8]u8 = undefined;
    const micros: i64 = (1761472800 - 946684800) * 1_000_000;
    std.mem.writeInt(i64, &buf, micros, .big);

    const tz = try decodeBinColumnData(alloc, @intFromEnum(PgOid.TIMESTAMPTZ), &buf);
    defer alloc.free(tz.text);
    const naive = try decodeBinColumnData(alloc, @intFromEnum(PgOid.TIMESTAMP), &buf);
    defer alloc.free(naive.text);

    try std.testing.expectEqualStrings("2025-10-26T10:00:00.000000Z", tz.text);
    // No suffix: the column carries no zone, so claiming UTC would be inventing one.
    try std.testing.expectEqualStrings("2025-10-26T10:00:00.000000", naive.text);
}

test "elementToText refuses a truncated fixed-width element" {
    // In ReleaseFast an unchecked data[0..4] reads past the buffer instead of failing.
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidDataLength, elementToText(alloc, .INT4, &[_]u8{ 0, 1 }));
    try std.testing.expectError(error.InvalidDataLength, elementToText(alloc, .INT8, &[_]u8{ 0, 1, 2, 3 }));
    try std.testing.expectError(error.InvalidDataLength, elementToText(alloc, .UUID, &[_]u8{ 0, 1, 2 }));
}
