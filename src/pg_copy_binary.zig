//! `COPY ... TO STDOUT WITH (FORMAT binary)` framing.
//!
//! Replaces the CSV snapshot path. Two reasons, one of them a correctness fix:
//!
//! 1. **The CSV parser it replaced was wrong.** It split rows on `,` with
//!    `splitScalar`, which knows nothing about quoting — a `TEXT` value containing a
//!    comma was silently split into two fields, shifting every column after it. Binary
//!    framing is length-prefixed, so a value's bytes cannot be confused with a
//!    delimiter. (`src/pg_copy_csv.zig`, deleted 2026-08-15 once a consumer had
//!    rebuilt a table from a binary snapshot.)
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

/// `"a", "b", "c"` — every column, quoted, in catalog order.
///
/// The chunk query cannot use `SELECT *`: its subquery adds a running-total column, and
/// binary COPY carries no names, so the decoder would match that extra column
/// positionally against the catalog and shift every value after it.
///
/// Caller owns the returned string.
pub fn columnList(allocator: std.mem.Allocator, columns: []const ColumnMeta) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);

    for (columns, 0..) |col, i| {
        if (i > 0) try out.appendSlice(allocator, ", ");
        try out.append(allocator, '"');
        try out.appendSlice(allocator, col.name);
        try out.append(allocator, '"');
    }

    return out.toOwnedSlice(allocator);
}

/// Bytes a fixed-width column contributes, plus MessagePack's per-field framing.
///
/// Deliberately generous. The sum only has to be an *upper-ish* bound good enough to keep
/// a chunk under budget; the encoder's own overflow check is what guarantees the message
/// actually fits, so erring high costs a slightly emptier chunk and erring low costs a
/// re-encode. Neither is a correctness problem.
const fixed_column_bytes: usize = 24;

/// SQL that evaluates to roughly the wire size of one row.
///
/// Used two ways, and both need the same expression or they disagree about what a row
/// costs: `max()` before the snapshot starts, to reject a row no NATS message can carry,
/// and a running `sum()` per chunk, to stop transferring once the budget is reached.
///
/// `octet_length` on `text`/`bytea` is the reason this is affordable: PostgreSQL reads the
/// length out of the TOAST pointer without fetching the out-of-line data. Measured on a
/// 200-row table of 256 KiB blobs — `max(octet_length(blob))` touched **2 buffers**,
/// against **430** for an expression that must actually read the value. A `::text` cast
/// (jsonb, arrays) gets no such break and does materialise the value.
///
/// Caller owns the returned string.
pub fn sizeExpression(allocator: std.mem.Allocator, columns: []const ColumnMeta) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);

    var fixed: usize = 0;
    for (columns) |col| {
        const direct = switch (col.oid) {
            @intFromEnum(pg_constants.PgOid.TEXT),
            @intFromEnum(pg_constants.PgOid.VARCHAR),
            @intFromEnum(pg_constants.PgOid.BPCHAR),
            bytea_oid,
            => true,
            else => false,
        };
        const castable = !direct and isVariableWidth(col);

        if (!direct and !castable) {
            fixed += fixed_column_bytes;
            continue;
        }

        if (out.items.len > 0) try out.appendSlice(allocator, " + ");
        try out.appendSlice(allocator, "coalesce(octet_length(\"");
        try out.appendSlice(allocator, col.name);
        try out.appendSlice(allocator, if (direct) "\"),0)" else "\"::text),0)");
    }

    if (out.items.len > 0) try out.appendSlice(allocator, " + ");
    var num_buf: [24]u8 = undefined;
    try out.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{fixed + fixed_column_bytes}));

    return out.toOwnedSlice(allocator);
}

/// Whether a column can hold an arbitrarily large value, and therefore has to be measured
/// rather than assumed. Arrays (any `oid` with an element type) and the document types
/// qualify; everything else is bounded by a small constant.
fn isVariableWidth(col: ColumnMeta) bool {
    return switch (col.oid) {
        @intFromEnum(pg_constants.PgOid.JSON),
        @intFromEnum(pg_constants.PgOid.JSONB),
        => true,
        // Arrays and anything else declared with a trailing `[]`. Matched on the rendered
        // type rather than an OID list because a domain or a user-defined array type has
        // an OID this file cannot know, and `format_type` renders all of them the same way.
        else => std.mem.endsWith(u8, col.type_text, "[]"),
    };
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
    /// The COPY stopped early because the *server* failed — reported by `PQgetResult`
    /// after the data ends, not by `PQgetCopyData`, which returns a perfectly ordinary
    /// -1. Distinct from `UnexpectedEnd` (a malformed frame) because the bytes received
    /// are well-formed; there are simply fewer of them than the query asked for, which
    /// is otherwise indistinguishable from reaching the end of the table.
    CopyIncomplete,
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
/// owner, which is why rows carry no `deinit` of their own.
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
    /// What one chunk actually produced.
    ///
    /// Two counts, not one, because they stop being the same number the moment a chunk is
    /// capped by size: `fetched` is what the COPY returned and answers "is this the end of
    /// the table", while `encoded` is what fits in one message and answers "where does the
    /// cursor go next". Collapsing them is what made `num_rows < chunk_size` mean "last
    /// chunk", which a byte-capped chunk would have claimed falsely — truncating the
    /// snapshot in silence.
    pub const ChunkResult = struct {
        fetched: usize,
        encoded: usize,
        /// Bytes the encoded prefix occupies — the number the shrink loop used to compute
        /// and discard. Reported so the caller can log it and size what comes next.
        encoded_bytes: usize,
        /// The LIMIT this chunk's COPY actually ran with.
        ///
        /// Carried on the result rather than supplied at judgement time, because the limit
        /// varies per chunk now: comparing `fetched` against a constant `chunk_size` would
        /// call a deliberately small chunk "final" and end the snapshot mid-table.
        limit: usize,

        /// True when this chunk *might* be the last: the COPY returned fewer rows than
        /// asked for, and every one of them fitted in the message.
        ///
        /// ⚠️ **"Might" is the whole point — this is not sufficient on its own.** Since the
        /// chunk query caps itself by a running byte sum, a short result has two possible
        /// causes: the table ended, or the sum trimmed the rows. They are indistinguishable
        /// from here, and guessing wrong ends the snapshot mid-table. Measured: a chunk
        /// with `limit=7506 fetched=4670 encoded=4670` was trimmed by the sum, looked
        /// final, and truncated a 25 000-row table at 8 389.
        ///
        /// The caller must confirm with a lookahead past the cursor. This exists to answer
        /// the cheap half — a full chunk, or one the encoder trimmed, needs no query at all.
        pub fn mayBeFinal(self: ChunkResult) bool {
            return self.fetched < self.limit and self.encoded == self.fetched;
        }
    };

    /// One chunk's worth of instructions.
    ///
    /// A struct rather than positional parameters because `limit` and `max_bytes` are both
    /// `usize` and adjacent: swapping them at a call site would compile, and would make
    /// every chunk report itself final.
    pub const ChunkRequest = struct {
        query: [:0]const u8,
        pk_idx: []const usize,
        /// The LIMIT already baked into `query`. Passed rather than parsed back out of it.
        limit: usize,
        /// Encoded byte budget. Must equal the encoder's buffer length, so that "the
        /// encoder ran out of room" and "this prefix exceeds a NATS message" are the same
        /// event with one handling path instead of two that can disagree.
        max_bytes: usize,
        /// Hint for reserving the raw COPY buffer in one allocation instead of a doubling
        /// series — on an arena every growth strands the previous copy.
        expected_raw_bytes: usize = 0,
    };

    /// Stream one chunk into `encoder`, capped by both row count and **bytes**.
    ///
    /// `max_bytes` is the real limit: NATS rejects a message over the server's
    /// `max_payload`, and the row cap alone cannot know how wide a row is. Measured on a
    /// 12-column table, 10 000 rows encoded to ~1.11 MB against a 1 MiB limit — the
    /// server closed the connection, every retry re-sent the identical payload and closed
    /// it again, and the snapshot was lost with no error published.
    ///
    /// When the encoding overflows, the rows already fetched are re-encoded as a prefix.
    /// No re-query: the COPY result is in hand, and the array header carries a row count
    /// that has to be written before the rows, so a chunk cannot be truncated in place.
    pub fn streamToEncoder(
        self: *Streamer,
        req: ChunkRequest,
        encoder: *streaming_encoder.StreamingEncoder,
    ) !ChunkResult {
        std.debug.assert(req.max_bytes == encoder.buffer.len);
        if (req.expected_raw_bytes > 0) {
            try self.buffer.ensureTotalCapacity(self.allocator, req.expected_raw_bytes);
        }

        const result = c.PQexec(self.conn, req.query.ptr);
        defer c.PQclear(result);

        if (c.PQresultStatus(result) != c.PGRES_COPY_OUT) {
            log.err("binary COPY failed: {s}", .{c.PQerrorMessage(self.conn)});
            return error.CopyFailed;
        }

        // Binary COPY hands back arbitrary buffers, not one per row as text COPY does,
        // so a tuple can straddle two calls. Accumulating first sidesteps partial-frame
        // bookkeeping entirely.
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

        // ⚠️ `-1` means "no more data", NOT "the COPY succeeded".
        //
        // A server-side failure *during* COPY OUT — statement timeout, a cancelled
        // backend, an unreadable TOAST chunk — also stops the data, and libpq reports it
        // only from the PQgetResult that follows. Without this check the caller receives
        // a short, well-formed buffer that is indistinguishable from a small final chunk:
        // `fetched < limit and encoded == fetched` → `isFinal()` → the snapshot ends
        // mid-table and is published as complete. Nothing else notices, because the next
        // chunk's PQexec silently clears the pending result.
        const tail = c.PQgetResult(self.conn);
        defer c.PQclear(tail);
        if (c.PQresultStatus(tail) != c.PGRES_COMMAND_OK) {
            log.err("🔴 binary COPY aborted mid-stream: {s}", .{c.PQerrorMessage(self.conn)});
            return error.CopyIncomplete;
        }

        // An empty result still carries the 19-byte header and a trailer; a genuinely
        // empty body means the COPY produced nothing at all.
        if (self.buffer.items.len == 0) {
            return .{ .fetched = 0, .encoded = 0, .encoded_bytes = 0, .limit = req.limit };
        }

        var r = Reader.init(self.buffer.items);
        readHeader(&r) catch |err| {
            log.err("binary COPY header rejected: {} — query was: {s}", .{ err, req.query });
            return err;
        };

        // Decode every row **once**, before any encoding. This used to live inside the
        // shrink loop, so each retry allocated a fresh decoded copy of every row into the
        // chunk arena with nothing freed — the retries, not the data, were the memory
        // amplifier. Now a retry re-runs encoder writes over values already in hand.
        var decoded: std.ArrayListUnmanaged([]const ?[]const u8) = .empty;
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
                .tuple => |t| try decoded.append(self.allocator, try decodeTuple(self.allocator, self.columns, t)),
            }
        }

        // One cursor buffer for the whole chunk, overwritten per row: only the last row's
        // values are ever read, and the arena would otherwise hold a discarded copy per row.
        const cursor = if (req.pk_idx.len > 0)
            try self.allocator.alloc([]const u8, req.pk_idx.len)
        else
            null;

        // Encode a prefix, shrinking until it fits. The first attempt is every row, so a
        // chunk already within budget — the overwhelming majority — pays one pass.
        var take = decoded.items.len;
        while (true) {
            encoder.reset();
            try encoder.writeArrayHeader(take);

            var done: usize = 0;
            var overflowed = false;
            for (decoded.items[0..take]) |values| {
                // Checkpoint, so a row that does not fit leaves nothing half-written
                // behind. `NoSpaceLeft` is a *shrink signal*, not a failure: the buffer is
                // the budget. Before this it escaped as StreamEncodingFailed and killed
                // the snapshot the first time a chunk exceeded the buffer — which is every
                // table averaging more than ~210 bytes a row.
                const mark = encoder.getPos();
                encodeRow(encoder, values, req.pk_idx, cursor, self.columns) catch |err| switch (err) {
                    error.NoSpaceLeft => {
                        encoder.setPos(mark);
                        overflowed = true;
                        break;
                    },
                    else => return err,
                };
                done += 1;
            }

            const written = encoder.getWritten().len;
            if (!overflowed and written <= req.max_bytes) {
                // Only now: a discarded attempt must never leave the cursor pointing past
                // the rows that actually shipped.
                if (cursor) |slots| self.last_pk = slots;
                return .{
                    .fetched = decoded.items.len,
                    .encoded = take,
                    .encoded_bytes = written,
                    .limit = req.limit,
                };
            }

            if (done == 0) {
                // A single row over the budget cannot be split, and dividing by `done`
                // below would be a division by zero.
                log.err(
                    "🔴 one row does not fit the {d}-byte message budget — a snapshot cannot split a row",
                    .{req.max_bytes},
                );
                return error.RowTooLargeForMessage;
            }

            take = shrinkTake(written, done, req.max_bytes, take);
            log.debug("chunk of {d} rows overflowed ({d} bytes, budget {d}); retrying with {d}", .{
                decoded.items.len, written, req.max_bytes, take,
            });
        }
    }
};

/// Encode one decoded row, and record its primary key into `cursor`.
///
/// Split out so the encode loop can checkpoint around it: every write it makes is between
/// one `getPos` and the next, so rolling back to the mark undoes the whole row.
fn encodeRow(
    encoder: *streaming_encoder.StreamingEncoder,
    values: []const ?[]const u8,
    pk_idx: []const usize,
    cursor: ?[][]const u8,
    columns: []const ColumnMeta,
) !void {
    try encoder.beginRow(values.len);
    for (values) |v| try encoder.writeField(v);

    if (cursor) |slots| {
        for (pk_idx, slots) |idx, *slot| {
            std.debug.assert(idx < values.len);
            // A primary key column is NOT NULL by definition, so a null here means the
            // layout is not the one this COPY emitted — the same class of fault as a field
            // count mismatch, and not something to page past.
            slot.* = values[idx] orelse {
                log.err(
                    "🔴 primary key column '{s}' decoded as NULL — the catalog layout does not match this COPY, aborting rather than paginating on it",
                    .{columns[idx].name},
                );
                return error.NullPrimaryKeyValue;
            };
        }
    }
}

/// How many rows to retry with after a prefix overflowed.
///
/// Bounded twice: never more than `rows_done` (the rows that demonstrably fit) and never
/// more than `current - 1` (so the loop strictly decreases and cannot stall). The 9/10 is
/// headroom — `written / rows_done` is a mean, and the rows that did not fit were, by
/// definition, the wider ones.
fn shrinkTake(written: usize, rows_done: usize, budget: usize, current: usize) usize {
    const per_row = @max(written / rows_done, 1);
    const fit = (budget / per_row) * 9 / 10;
    return @max(@min(@min(fit, rows_done), current - 1), 1);
}

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

// ─── ChunkResult ────────────────────────────────────────────────────────────────
//
// The distinction these encode is the whole reason the type has two counts: "the table
// ended" and "the message filled up" both produce fewer rows than asked for, and only the
// first may end a snapshot. Judged against the limit *that chunk* ran with, because the
// limit now varies per chunk.

test "ChunkResult.mayBeFinal - a chunk that filled its own limit cannot be the last" {
    // No query needed: the table clearly has more.
    const r = Streamer.ChunkResult{ .fetched = 3, .encoded = 3, .encoded_bytes = 900_000, .limit = 3 };
    try testing.expect(!r.mayBeFinal());
}

test "ChunkResult.mayBeFinal - judged against its own limit, not the row ceiling" {
    // A 256 KiB-row table paginates three rows at a time. Compared against
    // `Snapshot.chunk_size` (10 000) this reads as `3 < 10_000 and 3 == 3` → "maybe
    // final", and a caller that trusted it would stop after three rows of a 200-row table.
    const r = Streamer.ChunkResult{ .fetched = 3, .encoded = 3, .encoded_bytes = 786_432, .limit = 3 };
    try testing.expect(!r.mayBeFinal());
    try testing.expect(r.fetched < 10_000 and r.encoded == r.fetched); // the old, wrong test
}

test "ChunkResult.mayBeFinal - a chunk the ENCODER trimmed is never the last" {
    // encoded < fetched: rows were left behind, so there is certainly more to send. This
    // one needs no lookahead either.
    const r = Streamer.ChunkResult{ .fetched = 800, .encoded = 640, .encoded_bytes = 1_000_000, .limit = 800 };
    try testing.expect(!r.mayBeFinal());
    const tail = Streamer.ChunkResult{ .fetched = 500, .encoded = 300, .encoded_bytes = 1_000_000, .limit = 800 };
    try testing.expect(!tail.mayBeFinal());
}

test "ChunkResult.mayBeFinal - a short, fully-encoded chunk is only a MAYBE" {
    // ⚠️ The regression this name exists to prevent. Observed live:
    // `limit=7506 fetched=4670 encoded=4670` — trimmed by the running byte sum, not by
    // the end of the table. Treated as final it truncated 25 000 rows at 8 389.
    // The caller must confirm with a lookahead past the cursor.
    const r = Streamer.ChunkResult{ .fetched = 4670, .encoded = 4670, .encoded_bytes = 518_373, .limit = 7506 };
    try testing.expect(r.mayBeFinal());
}

// ─── shrinkTake ─────────────────────────────────────────────────────────────────

test "shrinkTake - retries with no more rows than actually fit" {
    // 500 rows encoded to 2 MB against a 1 MB budget: the retry must not exceed 500, and
    // should land near half of it.
    const next = shrinkTake(2_000_000, 500, 1_000_000, 800);
    try testing.expect(next <= 500);
    try testing.expect(next >= 1);
}

test "shrinkTake - strictly decreases, so the loop cannot stall" {
    // Even when the arithmetic suggests keeping every row — a fat first row followed by
    // thin ones — the result is bounded by current - 1.
    try testing.expectEqual(@as(usize, 9), shrinkTake(100, 10, 1_000_000, 10));
    try testing.expectEqual(@as(usize, 1), shrinkTake(100, 2, 1_000_000, 2));
}

test "shrinkTake - never returns zero" {
    // The caller checks `done == 0` before dividing, so this only guards the arithmetic:
    // one enormous row still yields a retry count of 1, not 0.
    try testing.expectEqual(@as(usize, 1), shrinkTake(50_000_000, 1, 1024, 1));
    try testing.expectEqual(@as(usize, 1), shrinkTake(50_000_000, 2, 1024, 2));
}

// ─── sizeExpression / columnList ────────────────────────────────────────────────

fn metaFor(name: []const u8, oid: u32, type_text: []const u8) ColumnMeta {
    return .{ .name = name, .oid = oid, .type_text = type_text };
}

test "sizeExpression - text and bytea are measured without a cast" {
    // The cast is what costs: `octet_length(text)` reads the TOAST pointer's length,
    // `octet_length(x::text)` materialises the value.
    const alloc = testing.allocator;
    const cols = [_]ColumnMeta{
        metaFor("blob", @intFromEnum(pg_constants.PgOid.TEXT), "text"),
        metaFor("raw", bytea_oid, "bytea"),
    };
    const expr = try sizeExpression(alloc, &cols);
    defer alloc.free(expr);

    try testing.expect(std.mem.indexOf(u8, expr, "octet_length(\"blob\")") != null);
    try testing.expect(std.mem.indexOf(u8, expr, "octet_length(\"raw\")") != null);
    try testing.expect(std.mem.indexOf(u8, expr, "::text") == null);
}

test "sizeExpression - jsonb and arrays need the cast" {
    const alloc = testing.allocator;
    const cols = [_]ColumnMeta{
        metaFor("doc", @intFromEnum(pg_constants.PgOid.JSONB), "jsonb"),
        metaFor("tags", 1009, "character varying(255)[]"),
    };
    const expr = try sizeExpression(alloc, &cols);
    defer alloc.free(expr);

    try testing.expect(std.mem.indexOf(u8, expr, "octet_length(\"doc\"::text)") != null);
    try testing.expect(std.mem.indexOf(u8, expr, "octet_length(\"tags\"::text)") != null);
}

test "sizeExpression - fixed-width columns become a constant, not a per-column call" {
    // Measuring an int8 would cost a function call per row for a value that cannot vary.
    const alloc = testing.allocator;
    const cols = [_]ColumnMeta{
        metaFor("id", 20, "bigint"),
        metaFor("at", 1114, "timestamp without time zone"),
    };
    const expr = try sizeExpression(alloc, &cols);
    defer alloc.free(expr);

    try testing.expect(std.mem.indexOf(u8, expr, "octet_length") == null);
    // Two fixed columns plus the per-row envelope.
    try testing.expectEqualStrings("72", expr);
}

test "sizeExpression - a table of only fixed columns still yields valid SQL" {
    const alloc = testing.allocator;
    const cols = [_]ColumnMeta{metaFor("id", 20, "bigint")};
    const expr = try sizeExpression(alloc, &cols);
    defer alloc.free(expr);
    // Must be a bare number, never an empty string or a dangling `+`.
    _ = try std.fmt.parseInt(usize, expr, 10);
}

test "columnList - quotes every column in catalog order" {
    const alloc = testing.allocator;
    const cols = [_]ColumnMeta{
        metaFor("id", 20, "bigint"),
        metaFor("order", 25, "text"), // a reserved word: the quoting is load-bearing
    };
    const list = try columnList(alloc, &cols);
    defer alloc.free(list);
    try testing.expectEqualStrings("\"id\", \"order\"", list);
}
