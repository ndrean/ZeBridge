//! `COPY ... TO STDOUT WITH (FORMAT binary)` framing.
//!
//! Replaces the CSV snapshot path. Two reasons, one of them a correctness fix:
//!
//! 1. **CSV parsing here was wrong.** `pg_copy_csv.parseCsvLineStreaming` splits rows
//!    on `,` with `splitScalar`, which knows nothing about quoting — a `TEXT` value
//!    containing a comma is silently split into two fields, shifting every column
//!    after it. Binary framing is length-prefixed, so a value's bytes cannot be
//!    confused with a delimiter.
//! 2. Postgres no longer renders every value to text for us to parse back, and
//!    `pgoutput.decodeBinColumnData` — already used by the CDC path, which runs
//!    `START_REPLICATION ... binary 'true'` — accepts exactly the per-value encodings
//!    binary COPY emits. This is framing plus a type lookup, not a new decoder.
//!
//! ## The thing binary COPY does not tell you
//!
//! CSV's `HEADER true` made the stream self-describing: names travelled with the data,
//! so parser and payload could not disagree. The binary header is 19 fixed bytes — no
//! names, no types, no column count. **Layout must arrive out-of-band**, from a
//! `pg_attribute` lookup taken on the same connection inside the snapshot's
//! `REPEATABLE READ` transaction, so no `ALTER TABLE` can slip between the lookup and
//! the COPY.
//!
//! The per-tuple field count is then the only integrity check available. When it
//! disagrees with the catalog it is a hard error, never a warning: decoding onward
//! would assign values to the wrong columns and produce plausible garbage.
//!
//! Framing functions return typed errors and do not log. The caller knows the table
//! and the chunk, so its message is the useful one; logging here would duplicate it
//! without that context.

const std = @import("std");
const pgoutput = @import("pgoutput.zig");
const pg_constants = @import("pg_constants.zig");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const streaming_encoder = @import("streaming_encoder.zig");

pub const log = std.log.scoped(.pg_copy_binary);

/// `PGCOPY\n\377\r\n\0` — the fixed 11-byte signature every binary COPY starts with.
pub const signature = "PGCOPY\n\xff\r\n\x00";

/// Signature (11) + flags (4) + header extension length (4).
pub const header_len = signature.len + 4 + 4;

/// A column's name and type, from `pg_attribute` in `SELECT *` order.
pub const ColumnMeta = struct {
    name: []const u8,
    oid: u32,
    /// Declared scale for `numeric(p,s)`, used to restore trailing zeros that binary
    /// NUMERIC does not carry. Null for every other type, and for an unconstrained
    /// `numeric` — which has no declared scale, so its natural scale is already right.
    numeric_scale: ?u16 = null,
    /// `pg_type.typtype`: 'b' base, 'e' enum, 'c' composite, 'd' domain, 'r' range.
    /// The discriminator that separates "unknown but safely text" from "unknown and
    /// structured" — see `decodeTuple`.
    typtype: u8 = 'b',
    /// 1-based position in the primary key, or 0 if this column is not part of it.
    /// Carrying the whole key rather than a single column name is what lets the same
    /// lookup serve composite keys, which is what pagination pages on.
    pk_ord: u16 = 0,
    /// `format_type(atttypid, atttypmod)` — the declared type as Postgres spells it,
    /// e.g. `numeric(20,8)`, `character varying(64)`, `public.mood`. Used to cast
    /// keyset cursor literals back to the column's own type; see `keysetPredicate`.
    type_text: []const u8 = "",
};

/// `pg_type.oid` for `numeric`. A fixed catalog constant, unlike extension types.
pub const numeric_oid: u32 = 1700;

/// `pg_type.oid` for `bytea`. Its decoded form is raw bytes — the only rendered value
/// that cannot be pasted into SQL text as-is.
pub const bytea_oid: u32 = 17;

// ---------------------------------------------------------------------------
// Keyset pagination
//
// Chunking pages on the primary key rather than `OFFSET`, so each boundary costs an
// index seek instead of re-reading everything before it. The key may have any number
// of columns, so the predicate is a **row-value comparison** — `("a","b") > (…)` —
// which Postgres compares lexicographically, exactly matching the order that
// `ORDER BY "a","b"` produces.
//
// Paging on the first column alone would not merely be incomplete, it would be wrong:
// that column is not unique on its own, so a chunk boundary landing inside a run of
// equal first-column values would skip the rest of that run.
// ---------------------------------------------------------------------------

/// Write one cursor value as a SQL literal cast to the column's declared type:
/// `'42'::integer`, `'2025-01-01T00:00:00Z'::timestamp with time zone`.
///
/// The cast is not decoration. Inside a row constructor an unquoted literal is
/// `unknown`, and leaving type resolution to the planner is the kind of thing that
/// works for `int` and surprises on a domain or an enum. Naming the type makes the
/// comparison the same one `ORDER BY` uses, whatever the column is.
///
/// Quoting doubles `'`. Backslashes are left alone, which is correct only under
/// `standard_conforming_strings = on` — the caller asserts that once per snapshot from
/// `PQparameterStatus`, rather than trusting the server's default.
fn appendSqlLiteral(
    out: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    col: ColumnMeta,
    value: []const u8,
) !void {
    try out.append(allocator, '\'');
    if (col.oid == bytea_oid) {
        // Raw bytes: NULs and quotes would truncate or break the statement, so the
        // hex input form is the only safe spelling.
        try out.appendSlice(allocator, "\\x");
        const hex = "0123456789abcdef";
        for (value) |byte| {
            try out.append(allocator, hex[byte >> 4]);
            try out.append(allocator, hex[byte & 0x0f]);
        }
    } else {
        for (value) |ch| {
            if (ch == '\'') try out.append(allocator, '\'');
            try out.append(allocator, ch);
        }
    }
    try out.append(allocator, '\'');

    // An empty `type_text` means the catalog lookup did not fill it in; casting to
    // nothing at all is better than emitting `::`.
    if (col.type_text.len > 0) {
        try out.appendSlice(allocator, "::");
        try out.appendSlice(allocator, col.type_text);
    }
}

/// Build the keyset `WHERE` for the chunk after `cursor`: `("a","b") > ('1'::int4, …)`.
///
/// `pk_idx` holds indices into `columns` in primary-key order, and `cursor` the
/// matching values from the last row of the previous chunk. A single-column key
/// degenerates to `("a") > ('1'::int4)`, which Postgres reads as a plain scalar
/// comparison — so there is one code path, not two.
pub fn keysetPredicate(
    allocator: std.mem.Allocator,
    columns: []const ColumnMeta,
    pk_idx: []const usize,
    cursor: []const []const u8,
) ![]u8 {
    std.debug.assert(pk_idx.len == cursor.len);
    std.debug.assert(pk_idx.len > 0);

    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);

    try out.append(allocator, '(');
    for (pk_idx, 0..) |idx, i| {
        if (i > 0) try out.appendSlice(allocator, ", ");
        try out.append(allocator, '"');
        try out.appendSlice(allocator, columns[idx].name);
        try out.append(allocator, '"');
    }
    try out.appendSlice(allocator, ") > (");
    for (pk_idx, cursor, 0..) |idx, value, i| {
        if (i > 0) try out.appendSlice(allocator, ", ");
        try appendSqlLiteral(&out, allocator, columns[idx], value);
    }
    try out.append(allocator, ')');

    return out.toOwnedSlice(allocator);
}

/// Build `ORDER BY "a", "b"` over the primary key, in key order.
///
/// Must stay in lockstep with `keysetPredicate`: the row comparison is only a correct
/// cursor if the rows arrive in the order it compares against.
pub fn orderByClause(
    allocator: std.mem.Allocator,
    columns: []const ColumnMeta,
    pk_idx: []const usize,
) ![]u8 {
    std.debug.assert(pk_idx.len > 0);

    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);

    for (pk_idx, 0..) |idx, i| {
        if (i > 0) try out.appendSlice(allocator, ", ");
        try out.append(allocator, '"');
        try out.appendSlice(allocator, columns[idx].name);
        try out.append(allocator, '"');
    }

    return out.toOwnedSlice(allocator);
}

pub const Error = error{
    /// The stream did not begin with the binary COPY signature. Usually means the
    /// query said `FORMAT csv`, or an error response was read as data.
    BadSignature,
    /// The flags word sets bit 16 (OIDs included per tuple). Postgres does not emit
    /// this for `COPY TO`, and the tuple layout would differ if it did.
    OidsNotSupported,
    /// Data ended mid-frame: truncated stream.
    UnexpectedEnd,
    /// A tuple's field count disagrees with the catalog column count.
    FieldCountMismatch,
    /// A field length that is neither -1 (NULL) nor a plausible size.
    BadFieldLength,
};

/// Reads big-endian integers out of a byte slice, tracking position.
///
/// Binary COPY is big-endian throughout; there is no negotiation and no variant, so
/// this is deliberately not generic over endianness.
pub const Reader = struct {
    data: []const u8,
    pos: usize = 0,

    pub fn init(data: []const u8) Reader {
        return .{ .data = data };
    }

    pub fn remaining(self: Reader) usize {
        return self.data.len - self.pos;
    }

    pub fn take(self: *Reader, n: usize) ![]const u8 {
        if (self.remaining() < n) return Error.UnexpectedEnd;
        const out = self.data[self.pos .. self.pos + n];
        self.pos += n;
        return out;
    }

    pub fn readI16(self: *Reader) !i16 {
        const b = try self.take(2);
        return std.mem.readInt(i16, b[0..2], .big);
    }

    pub fn readI32(self: *Reader) !i32 {
        const b = try self.take(4);
        return std.mem.readInt(i32, b[0..4], .big);
    }
};

/// Consume the 19-byte header, validating it. Returns bytes consumed.
pub fn readHeader(r: *Reader) !void {
    const sig = r.take(signature.len) catch return Error.BadSignature;
    if (!std.mem.eql(u8, sig, signature)) return Error.BadSignature;

    const flags = try r.readI32();
    // Bit 16 means each tuple carries OIDs. Postgres never sets it for COPY TO, and
    // the tuple layout would differ, so refuse rather than misparse.
    if (flags & (1 << 16) != 0) return Error.OidsNotSupported;

    // Header extension: a length followed by that many bytes we must skip. Always 0
    // today, but the format reserves it, so honour it rather than assuming.
    const ext_len = try r.readI32();
    if (ext_len < 0) return Error.BadFieldLength;
    _ = try r.take(@intCast(ext_len));
}

/// The result of trying to read one tuple.
pub const TupleResult = union(enum) {
    /// A complete tuple: one entry per column, `null` for SQL NULL.
    tuple: []const ?[]const u8,
    /// The `-1` field count that terminates the stream.
    trailer,
};

/// Read one tuple's raw field bytes. Slices point into the reader's buffer — they are
/// valid as long as that buffer is, and are not copied.
///
/// `expected_columns` comes from the catalog, and a mismatch is fatal by design.
pub fn readTuple(
    r: *Reader,
    allocator: std.mem.Allocator,
    expected_columns: usize,
) !TupleResult {
    const field_count = try r.readI16();
    if (field_count == -1) return .trailer;
    if (field_count < 0) return Error.BadFieldLength;

    const n: usize = @intCast(field_count);
    // Values would land in the wrong columns. The caller logs it — it knows the table
    // and chunk, which is what makes the message useful.
    if (n != expected_columns) return Error.FieldCountMismatch;

    const fields = try allocator.alloc(?[]const u8, n);
    // A truncated or malformed frame aborts mid-loop; without this the partially
    // filled slice leaks. Caught by the truncated-field test.
    errdefer allocator.free(fields);
    for (fields) |*f| {
        const len = try r.readI32();
        if (len == -1) {
            // NULL. Distinct from a zero-length value, which has len 0 and no bytes —
            // conflating them would turn an empty string into a NULL.
            f.* = null;
        } else if (len < 0) {
            return Error.BadFieldLength;
        } else {
            f.* = try r.take(@intCast(len));
        }
    }
    return .{ .tuple = fields };
}

/// Render a decoded value as the text the snapshot payload carries.
///
/// The streaming encoder writes `?[]const u8`, so binary values are decoded and then
/// rendered. That is not a round-trip to where we started: the decode is
/// length-delimited and type-aware, so a comma or newline inside a value can no longer
/// shift columns.
///
/// Verified against a live snapshot of `test_types`, the representations that actually
/// come out (the `DecodedValue.array` doc comment says "as JSON string", which is not
/// what the wire shows):
///
/// - arrays keep the **Postgres literal** form — `{"a","b,c"}`, `{{1,2},{3,4}}` — the
///   same text CSV produced, so array columns are *not* a protocol change;
/// - JSONB is JSON (`{"k": "v"}`), timestamps are ISO-8601, UUIDs are canonical text.
///
/// ⚠️ One genuine divergence: NUMERIC trailing zeros. Binary NUMERIC stores base-10000
/// digit groups, so a `numeric(20,8)` holding `0.1` decodes as `0.1000` where text
/// COPY emitted `0.10000000`. The value is numerically identical and no precision is
/// lost — but a consumer storing NUMERIC as TEXT (which the type mapping requires, to
/// avoid float64) will see a different *string*, so equality on the text form can
/// differ across the CSV→binary switch.
pub fn renderValue(
    allocator: std.mem.Allocator,
    value: pgoutput.DecodedValue,
    numeric_scale: ?u16,
) !?[]const u8 {
    return switch (value) {
        .null => null,
        // Only logical replication can produce this: `COPY` reads the current row, so
        // every value is present. Reaching here means a decoder was fed a CDC tuple.
        .unchanged => error.UnchangedValueInSnapshot,
        // "t"/"f" matches what Postgres itself writes for a boolean in text output, so
        // consumers that already parse the CSV snapshots keep working.
        .boolean => |b| if (b) "t" else "f",
        .int32 => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
        .int64 => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
        .float64 => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
        .numeric => |sv| try padNumeric(allocator, sv, numeric_scale),
        // Already text in the form the payload wants.
        .text, .jsonb, .array, .bytea => |sv| sv,
    };
}

/// Extract the declared scale from `format_type` output, e.g. `numeric(20,8)` → 8.
///
/// Returns null when there is nothing to pad to:
/// - `numeric` — unconstrained, so the value's natural scale is already correct;
/// - `numeric(10)` — precision only, which means scale 0;
/// - `numeric(10,-2)` — negative scale (PG 15+) puts significant digits *left* of the
///   point, so there are no trailing decimals to restore.
///
/// Parsing Postgres's own rendering keeps the `atttypmod` bit layout — which changed in
/// PG 15 — on the server side, where it belongs.
pub fn scaleFromFormatType(type_text: []const u8) ?u16 {
    const open = std.mem.indexOfScalar(u8, type_text, '(') orelse return null;
    const close = std.mem.indexOfScalarPos(u8, type_text, open, ')') orelse return null;
    const args = type_text[open + 1 .. close];

    const comma = std.mem.indexOfScalar(u8, args, ',') orelse return null;
    const scale_text = std.mem.trim(u8, args[comma + 1 ..], " ");
    const parsed = std.fmt.parseInt(i32, scale_text, 10) catch return null;
    if (parsed <= 0) return null;
    return @intCast(parsed);
}


/// Restore the trailing zeros a `numeric(p,s)` column declares.
///
/// Binary NUMERIC stores base-10000 digit groups and carries only the digits that
/// exist, so `numeric(20,8)` holding `0.1` arrives as `0.1000` where text COPY emitted
/// `0.10000000`. That matters because the type mapping stores NUMERIC as **TEXT** in
/// SQLite — `REAL` is float64 and would lose digits on a money column — and in TEXT,
/// `'0.1000' = '0.10000000'` is false. Two spellings of one number would compare
/// unequal on the replica.
///
/// Padding to the declared scale makes binary byte-identical to text for NUMERIC, so
/// the switch is invisible to consumers.
pub fn padNumeric(
    allocator: std.mem.Allocator,
    value: []const u8,
    scale: ?u16,
) ![]const u8 {
    const want = scale orelse return value;
    if (want == 0) return value;

    // NaN and ±Infinity are valid NUMERIC values (Infinity since PG 14) and have no
    // decimal places to pad. Detected by the absence of a digit rather than by name,
    // so it holds whatever spelling Postgres uses.
    for (value) |ch| {
        if (std.ascii.isDigit(ch)) break;
    } else return value;

    const dot = std.mem.indexOfScalar(u8, value, '.');
    const have: usize = if (dot) |d| value.len - d - 1 else 0;
    if (have >= want) return value; // already at or beyond the declared scale

    const pad = want - have;
    // Exact-sized alloc rather than an ArrayList: `items` is a prefix of a larger
    // capacity, and freeing that slice trips the allocator's size check.
    const extra: usize = if (dot == null) 1 else 0;
    const out = try allocator.alloc(u8, value.len + extra + pad);
    @memcpy(out[0..value.len], value);
    if (dot == null) out[value.len] = '.';
    @memset(out[value.len + extra ..], '0');
    return out;
}

/// Decode one tuple's raw fields into rendered text values, using the catalog OIDs.
///
/// Allocates from `arena`; there is no per-row cleanup because the arena is the only
/// owner. This is why rows carry no `deinit`, unlike `CsvRow`.
pub fn decodeTuple(
    arena: std.mem.Allocator,
    columns: []const ColumnMeta,
    fields: []const ?[]const u8,
) ![]const ?[]const u8 {
    std.debug.assert(fields.len == columns.len);
    const out = try arena.alloc(?[]const u8, fields.len);

    for (fields, columns, out) |raw, col, *slot| {
        if (raw == null) {
            slot.* = null;
            continue;
        }
        // `decodeBinColumnData` falls through to "treat as text" for any OID it does
        // not know. That is *correct* for an enum — Postgres sends enums as their label
        // in binary — and silent corruption for anything else. Verified against a live
        // hstore column, whose binary form (int32 count, then length-prefixed pairs)
        // arrived as a string of embedded lengths and NUL bytes with only a log.warn.
        //
        // User-defined and extension types get per-database OIDs, so they can never be
        // constants in the decoder's switch; `typtype` is what tells the two cases
        // apart at runtime.
        if (!pg_constants.isKnownOid(col.oid) and col.typtype != 'e') {
            log.err(
                "🔴 column '{s}' has unsupported type OID {d} (typtype '{c}') — refusing to snapshot rather than shipping its raw binary as text. Only enums are safe to pass through undecoded.",
                .{ col.name, col.oid, col.typtype },
            );
            return error.UnsupportedColumnType;
        }

        const decoded = pgoutput.decodeBinColumnData(arena, col.oid, raw.?) catch |err| {
            log.err(
                "🔴 cannot decode column '{s}' (OID {d}): {} — snapshot aborted rather than emitting unverified bytes",
                .{ col.name, col.oid, err },
            );
            return err;
        };
        slot.* = try renderValue(arena, decoded, col.numeric_scale);
    }
    return out;
}


/// Drives one `COPY ... FORMAT binary` and encodes its rows.
///
/// Unlike the CSV path this buffers the whole chunk before encoding, which is what
/// makes it simpler rather than heavier: the row count is known before the first byte
/// is written, so the MessagePack array header goes down correctly the first time. The
/// CSV path could not know the count in advance, so it reserved five bytes, patched
/// them afterwards, and memmoved the entire payload when the real header was shorter.
///
/// Memory is bounded by the chunk's `LIMIT`, and the buffer comes from the caller's
/// per-chunk arena, so it is released on the next reset.
pub const Streamer = struct {
    allocator: std.mem.Allocator,
    conn: ?*c.PGconn,
    columns: []const ColumnMeta,
    buffer: std.ArrayListUnmanaged(u8) = .empty,
    /// Rendered primary-key values of the last row, in key order — the next chunk's
    /// keyset cursor. Points into the arena, so the caller must copy it before
    /// resetting.
    last_pk: ?[]const []const u8 = null,

    pub fn init(
        allocator: std.mem.Allocator,
        conn: ?*c.PGconn,
        columns: []const ColumnMeta,
    ) Streamer {
        return .{ .allocator = allocator, .conn = conn, .columns = columns };
    }

    pub fn deinit(self: *Streamer) void {
        self.buffer.deinit(self.allocator);
    }

    /// The keyset cursor for the next chunk: one value per primary-key column, in key
    /// order. Only valid until the caller's arena is reset.
    pub fn getLastPkValues(self: *const Streamer) ?[]const []const u8 {
        return self.last_pk;
    }

    /// Run the COPY, decode every tuple, and encode the chunk. Returns the row count.
    ///
    /// `pk_idx` holds indices into `columns` in primary-key order; the values at those
    /// indices in the final row become the next chunk's cursor.
    pub fn streamToEncoder(
        self: *Streamer,
        query: [:0]const u8,
        encoder: *streaming_encoder.StreamingEncoder,
        pk_idx: []const usize,
    ) !usize {
        const result = c.PQexec(self.conn, query.ptr);
        defer c.PQclear(result);

        if (c.PQresultStatus(result) != c.PGRES_COPY_OUT) {
            log.err("binary COPY failed: {s}", .{c.PQerrorMessage(self.conn)});
            return error.CopyFailed;
        }

        // Binary COPY hands back arbitrary buffers, not one per row as text COPY does,
        // so a tuple can straddle two calls. Accumulating first sidesteps partial-frame
        // bookkeeping entirely.
        self.buffer.clearRetainingCapacity();
        while (true) {
            var buf_ptr: [*c]u8 = undefined;
            const len = c.PQgetCopyData(self.conn, &buf_ptr, 0);
            if (len == -1) break;
            if (len == -2) {
                log.err("PQgetCopyData failed: {s}", .{c.PQerrorMessage(self.conn)});
                return error.CopyDataFailed;
            }
            if (len > 0) {
                defer c.PQfreemem(buf_ptr);
                try self.buffer.appendSlice(self.allocator, buf_ptr[0..@intCast(len)]);
            }
        }

        // An empty result still carries the 19-byte header and a trailer; a genuinely
        // empty body means the COPY produced nothing at all.
        if (self.buffer.items.len == 0) return 0;

        var r = Reader.init(self.buffer.items);
        readHeader(&r) catch |err| {
            log.err("binary COPY header rejected: {} — query was: {s}", .{ err, query });
            return err;
        };

        var rows: std.ArrayListUnmanaged([]const ?[]const u8) = .empty;
        while (true) {
            const res = readTuple(&r, self.allocator, self.columns.len) catch |err| {
                if (err == Error.FieldCountMismatch) {
                    log.err(
                        "🔴 binary COPY tuple disagrees with the catalog ({d} columns expected) — aborting chunk rather than misassigning values",
                        .{self.columns.len},
                    );
                }
                return err;
            };
            switch (res) {
                .trailer => break,
                .tuple => |t| try rows.append(self.allocator, t),
            }
        }

        // Header first, count already known.
        try encoder.writeArrayHeader(rows.items.len);

        // One cursor buffer for the whole chunk, overwritten per row: only the last
        // row's values are ever read, and the arena would otherwise hold a discarded
        // copy per row.
        const cursor = if (pk_idx.len > 0)
            try self.allocator.alloc([]const u8, pk_idx.len)
        else
            null;

        for (rows.items) |raw_fields| {
            const values = try decodeTuple(self.allocator, self.columns, raw_fields);
            try encoder.beginRow(values.len);
            for (values) |v| try encoder.writeField(v);

            if (cursor) |slots| {
                for (pk_idx, slots) |idx, *slot| {
                    std.debug.assert(idx < values.len);
                    // A primary key column is NOT NULL by definition, so a null here
                    // means the layout is not the one this COPY emitted — the same
                    // class of fault as a field-count mismatch, and not something to
                    // page past.
                    slot.* = values[idx] orelse {
                        log.err(
                            "🔴 primary key column '{s}' decoded as NULL — the catalog layout does not match this COPY, aborting rather than paginating on it",
                            .{self.columns[idx].name},
                        );
                        return error.NullPrimaryKeyValue;
                    };
                }
                self.last_pk = slots;
            }
        }

        return rows.items.len;
    }
};

// ---------------------------------------------------------------------------
// Tests — synthetic buffers, no database required. The framing is the part that
// must be exactly right, and it is the part a live test would exercise least
// precisely (a real table rarely produces a truncated frame or a bad field count).
// ---------------------------------------------------------------------------

const testing = std.testing;

/// Build a valid binary COPY header for tests.
fn buildHeader(buf: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator) !void {
    try buf.appendSlice(alloc, signature);
    try buf.appendSlice(alloc, &[_]u8{ 0, 0, 0, 0 }); // flags
    try buf.appendSlice(alloc, &[_]u8{ 0, 0, 0, 0 }); // header extension length
}

fn appendI16(buf: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, v: i16) !void {
    var b: [2]u8 = undefined;
    std.mem.writeInt(i16, &b, v, .big);
    try buf.appendSlice(alloc, &b);
}

fn appendI32(buf: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, v: i32) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, v, .big);
    try buf.appendSlice(alloc, &b);
}

test "header - a valid header is consumed exactly" {
    const alloc = testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(alloc);
    try buildHeader(&buf, alloc);

    var r = Reader.init(buf.items);
    try readHeader(&r);
    try testing.expectEqual(header_len, r.pos);
    try testing.expectEqual(@as(usize, 0), r.remaining());
}

test "header - a CSV stream is rejected, not misparsed" {
    // The failure this guards: pointing the binary parser at a FORMAT csv query. The
    // bytes are readable, so without a signature check it would produce nonsense.
    var r = Reader.init("id,name\n1,alice\n");
    try testing.expectError(Error.BadSignature, readHeader(&r));
}

test "header - honours a non-zero extension length" {
    const alloc = testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(alloc);
    try buf.appendSlice(alloc, signature);
    try appendI32(&buf, alloc, 0); // flags
    try appendI32(&buf, alloc, 3); // extension length
    try buf.appendSlice(alloc, "abc"); // extension payload
    try appendI16(&buf, alloc, -1); // trailer

    var r = Reader.init(buf.items);
    try readHeader(&r);
    // Skipping the extension is what leaves the reader aligned on the trailer.
    const t = try readTuple(&r, alloc, 0);
    try testing.expect(t == .trailer);
}

test "header - refuses the OIDs-included flag" {
    const alloc = testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(alloc);
    try buf.appendSlice(alloc, signature);
    try appendI32(&buf, alloc, 1 << 16); // OIDs included
    try appendI32(&buf, alloc, 0);

    var r = Reader.init(buf.items);
    try testing.expectError(Error.OidsNotSupported, readHeader(&r));
}

test "tuple - NULL and empty string stay distinct" {
    // The bug this prevents: treating length 0 as NULL, which silently turns every
    // empty string in the table into a NULL on the replica.
    const alloc = testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(alloc);
    try buildHeader(&buf, alloc);
    try appendI16(&buf, alloc, 2);
    try appendI32(&buf, alloc, -1); // NULL
    try appendI32(&buf, alloc, 0); // empty, zero bytes follow
    try appendI16(&buf, alloc, -1);

    var r = Reader.init(buf.items);
    try readHeader(&r);
    const res = try readTuple(&r, alloc, 2);
    defer alloc.free(res.tuple);

    try testing.expect(res.tuple[0] == null);
    try testing.expect(res.tuple[1] != null);
    try testing.expectEqualStrings("", res.tuple[1].?);
}

test "tuple - a value containing a comma survives intact" {
    // Exactly what the CSV path got wrong: splitScalar on ',' would break this value
    // into two fields and shift every column after it.
    const alloc = testing.allocator;
    const value = "Doe, John";
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(alloc);
    try buildHeader(&buf, alloc);
    try appendI16(&buf, alloc, 1);
    try appendI32(&buf, alloc, @intCast(value.len));
    try buf.appendSlice(alloc, value);
    try appendI16(&buf, alloc, -1);

    var r = Reader.init(buf.items);
    try readHeader(&r);
    const res = try readTuple(&r, alloc, 1);
    defer alloc.free(res.tuple);

    try testing.expectEqualStrings(value, res.tuple[0].?);
}

test "tuple - a newline inside a value survives intact" {
    const alloc = testing.allocator;
    const value = "line1\nline2";
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(alloc);
    try buildHeader(&buf, alloc);
    try appendI16(&buf, alloc, 1);
    try appendI32(&buf, alloc, @intCast(value.len));
    try buf.appendSlice(alloc, value);

    var r = Reader.init(buf.items);
    try readHeader(&r);
    const res = try readTuple(&r, alloc, 1);
    defer alloc.free(res.tuple);

    try testing.expectEqualStrings(value, res.tuple[0].?);
}

test "tuple - a field count disagreeing with the catalog is fatal" {
    const alloc = testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(alloc);
    try buildHeader(&buf, alloc);
    try appendI16(&buf, alloc, 3); // stream says 3
    try appendI32(&buf, alloc, 0);
    try appendI32(&buf, alloc, 0);
    try appendI32(&buf, alloc, 0);

    var r = Reader.init(buf.items);
    try readHeader(&r);
    // Catalog says 2. Continuing would assign field 0 to column 0, field 1 to column
    // 1, and drop field 2 — plausible-looking, entirely wrong.
    try testing.expectError(Error.FieldCountMismatch, readTuple(&r, alloc, 2));
}

test "tuple - a truncated field is an error, not a short read" {
    const alloc = testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(alloc);
    try buildHeader(&buf, alloc);
    try appendI16(&buf, alloc, 1);
    try appendI32(&buf, alloc, 10); // claims 10 bytes
    try buf.appendSlice(alloc, "abc"); // provides 3

    var r = Reader.init(buf.items);
    try readHeader(&r);
    try testing.expectError(Error.UnexpectedEnd, readTuple(&r, alloc, 1));
}

test "tuple - reads several rows then the trailer" {
    const alloc = testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(alloc);
    try buildHeader(&buf, alloc);
    for ([_][]const u8{ "a", "bb", "ccc" }) |v| {
        try appendI16(&buf, alloc, 1);
        try appendI32(&buf, alloc, @intCast(v.len));
        try buf.appendSlice(alloc, v);
    }
    try appendI16(&buf, alloc, -1);

    var r = Reader.init(buf.items);
    try readHeader(&r);

    var seen: usize = 0;
    while (true) {
        const res = try readTuple(&r, alloc, 1);
        switch (res) {
            .trailer => break,
            .tuple => |t| {
                defer alloc.free(t);
                seen += 1;
            },
        }
    }
    try testing.expectEqual(@as(usize, 3), seen);
}

test "renderValue - NULL renders as null, not the string \"null\"" {
    const alloc = testing.allocator;
    try testing.expect((try renderValue(alloc, .null, null)) == null);
}

test "renderValue - booleans use Postgres text form" {
    const alloc = testing.allocator;
    try testing.expectEqualStrings("t", (try renderValue(alloc, .{ .boolean = true }, null)).?);
    try testing.expectEqualStrings("f", (try renderValue(alloc, .{ .boolean = false }, null)).?);
}

test "renderValue - integers render as decimal" {
    const alloc = testing.allocator;
    const v = (try renderValue(alloc, .{ .int64 = -42 }, null)).?;
    defer alloc.free(v);
    try testing.expectEqualStrings("-42", v);
}

test "renderValue - numeric keeps its exact decimal string" {
    // NUMERIC must never become a float on the way through: that is the same precision
    // loss the type mapping avoids by sending NUMERIC to TEXT in SQLite.
    const alloc = testing.allocator;
    const v = (try renderValue(alloc, .{ .numeric = "12345.678901234567890" }, null)).?;
    try testing.expectEqualStrings("12345.678901234567890", v);
}

test "padNumeric - restores trailing zeros to the declared scale" {
    // The observed divergence: numeric(20,8) holding 0.1 arrives as "0.1000" from
    // binary COPY where text COPY emitted "0.10000000".
    const alloc = testing.allocator;
    const v = try padNumeric(alloc, "0.1000", 8);
    defer alloc.free(v);
    try testing.expectEqualStrings("0.10000000", v);
}

test "padNumeric - adds the decimal point when there is none" {
    const alloc = testing.allocator;
    const v = try padNumeric(alloc, "42", 2);
    defer alloc.free(v);
    try testing.expectEqualStrings("42.00", v);
}

test "padNumeric - never truncates a value longer than the declared scale" {
    // Padding restores zeros; it must not remove digits. Truncating would be silent
    // data loss on exactly the money columns this exists to protect.
    const v = try padNumeric(testing.allocator, "1.23456789", 2);
    try testing.expectEqualStrings("1.23456789", v);
}

test "padNumeric - unconstrained numeric is left alone" {
    // atttypmod -1: no declared scale, so the natural scale is already correct and
    // padding would invent precision the column never promised.
    const v = try padNumeric(testing.allocator, "0.1", null);
    try testing.expectEqualStrings("0.1", v);
}

test "padNumeric - scale 0 adds no decimal point" {
    const v = try padNumeric(testing.allocator, "42", 0);
    try testing.expectEqualStrings("42", v);
}

test "padNumeric - NaN and Infinity pass through unpadded" {
    // Both are valid NUMERIC values (Infinity since PG 14). "NaN.00000000" would be
    // neither a number nor NaN.
    for ([_][]const u8{ "NaN", "Infinity", "-Infinity" }) |special| {
        const v = try padNumeric(testing.allocator, special, 8);
        try testing.expectEqualStrings(special, v);
    }
}

test "padNumeric - negative values keep their sign" {
    const alloc = testing.allocator;
    const v = try padNumeric(alloc, "-3.5", 4);
    defer alloc.free(v);
    try testing.expectEqualStrings("-3.5000", v);
}

test "renderValue - a scaled numeric matches what text COPY would emit" {
    const alloc = testing.allocator;
    const v = (try renderValue(alloc, .{ .numeric = "0.1000" }, 8)).?;
    defer alloc.free(v);
    try testing.expectEqualStrings("0.10000000", v);
}

test "scaleFromFormatType - reads the declared scale" {
    try testing.expectEqual(@as(?u16, 8), scaleFromFormatType("numeric(20,8)"));
    try testing.expectEqual(@as(?u16, 2), scaleFromFormatType("numeric(10, 2)"));
}

test "scaleFromFormatType - nothing to pad" {
    // Unconstrained numeric, precision-only, and PG 15+ negative scale all mean the
    // value arrives with the right number of decimals already.
    try testing.expectEqual(@as(?u16, null), scaleFromFormatType("numeric"));
    try testing.expectEqual(@as(?u16, null), scaleFromFormatType("numeric(10)"));
    try testing.expectEqual(@as(?u16, null), scaleFromFormatType("numeric(10,-2)"));
    try testing.expectEqual(@as(?u16, null), scaleFromFormatType("numeric(10,0)"));
}

test "keysetPredicate - a single-column key is a plain scalar comparison" {
    const alloc = testing.allocator;
    const columns = [_]ColumnMeta{
        .{ .name = "id", .oid = 23, .pk_ord = 1, .type_text = "integer" },
        .{ .name = "email", .oid = 25, .type_text = "text" },
    };
    const sql = try keysetPredicate(alloc, &columns, &.{0}, &.{"41"});
    defer alloc.free(sql);
    try testing.expectEqualStrings("(\"id\") > ('41'::integer)", sql);
}

test "keysetPredicate - a composite key compares the whole row, in key order" {
    const alloc = testing.allocator;
    // Declaration order is (name, tenant); key order is (tenant, name). Paging must
    // follow the key, so the caller's pk_idx — not the column order — decides.
    const columns = [_]ColumnMeta{
        .{ .name = "name", .oid = 25, .pk_ord = 2, .type_text = "text" },
        .{ .name = "tenant", .oid = 23, .pk_ord = 1, .type_text = "integer" },
    };
    const sql = try keysetPredicate(alloc, &columns, &.{ 1, 0 }, &.{ "7", "carol" });
    defer alloc.free(sql);
    try testing.expectEqualStrings(
        "(\"tenant\", \"name\") > ('7'::integer, 'carol'::text)",
        sql,
    );
}

test "keysetPredicate - a quote in a key value cannot end the literal" {
    // O'Brien as a text key: the cursor comes straight from table data, so this is
    // ordinary input, not an attack. Getting it wrong is a syntax error at best and a
    // truncated predicate at worst.
    const alloc = testing.allocator;
    const columns = [_]ColumnMeta{
        .{ .name = "name", .oid = 25, .pk_ord = 1, .type_text = "text" },
    };
    const sql = try keysetPredicate(alloc, &columns, &.{0}, &.{"O'Brien"});
    defer alloc.free(sql);
    try testing.expectEqualStrings("(\"name\") > ('O''Brien'::text)", sql);
}

test "keysetPredicate - a bytea key goes out as hex, not raw bytes" {
    // Raw bytes would carry a NUL that truncates the statement at the libpq boundary.
    const alloc = testing.allocator;
    const columns = [_]ColumnMeta{
        .{ .name = "digest", .oid = bytea_oid, .pk_ord = 1, .type_text = "bytea" },
    };
    const sql = try keysetPredicate(alloc, &columns, &.{0}, &.{&[_]u8{ 0x00, 0xff, 0x10 }});
    defer alloc.free(sql);
    try testing.expectEqualStrings("(\"digest\") > ('\\x00ff10'::bytea)", sql);
}

test "orderByClause - follows key order and stays in lockstep with the predicate" {
    const alloc = testing.allocator;
    const columns = [_]ColumnMeta{
        .{ .name = "name", .oid = 25, .pk_ord = 2, .type_text = "text" },
        .{ .name = "tenant", .oid = 23, .pk_ord = 1, .type_text = "integer" },
    };
    const sql = try orderByClause(alloc, &columns, &.{ 1, 0 });
    defer alloc.free(sql);
    try testing.expectEqualStrings("\"tenant\", \"name\"", sql);
}

test "isKnownOid - built-in types are known, dynamic ones are not" {
    // The OIDs observed live: hstore 16416 and the `mood` enum 16544 are assigned per
    // database, so no switch can ever contain them.
    try testing.expect(pg_constants.isKnownOid(23)); // int4
    try testing.expect(pg_constants.isKnownOid(numeric_oid));
    try testing.expect(!pg_constants.isKnownOid(16416));
    try testing.expect(!pg_constants.isKnownOid(16544));
}
