//! The readings archiver's decisions, with no I/O: what every archiver shell — the native
//! one (`archiver.zig`, `libzbarchive`, `bridge_archiver`), the Python package (zb-archiver)
//! and a WebAssembly host (`zig build archive-core-wasm`) — must agree on.
//!
//!   - the retention inequality: a reading must outlive two flushes and an S3 outage;
//!   - where an archive starts on a stream, and when a pruned prefix means lost readings;
//!   - whether a chunk the reader returned may move the manifest (a hole never does);
//!   - the manifest: `{"covers_up_to", "files": [{"key", "rows", "bytes", "first_seq",
//!     "last_seq"}], "schema"}`, other keys kept as they were read;
//!   - the file key `<tenant>/readings/<yyyy-mm-dd>/<first_seq>-<last_seq>.parquet`.
//!
//! The contract is the archive's (zb-archiver/src/zb_archiver/layout.py names the same
//! things); a change here is a change there.

const std = @import("std");

pub const table = "readings";
pub const kv_bucket = "time_manifests";
pub const schema: i64 = 1;

/// What one read of the stream returned (the fetcher's JSON line, bridge_readings_fetch.zig).
pub const Summary = struct {
    messages: u64 = 0,
    rows: u64 = 0,
    first_seq: u64 = 0,
    first_row_seq: u64 = 0,
    last_seq: u64 = 0,
    first_ms: i64 = 0,
};

// ─── retention ──────────────────────────────────────────────────────────────────

/// The stream's `max_age` (ns; 0 = unbounded) is too short when it is under 2 × flush + outage.
/// Returns the seconds needed when it is, null when it is enough.
pub fn retentionShort(max_age_ns: u64, flush_s: f64, outage_s: f64) ?f64 {
    const need = 2 * flush_s + outage_s;
    if (max_age_ns == 0) return null;
    const have = @as(f64, @floatFromInt(max_age_ns)) / std.time.ns_per_s;
    return if (have < need) need else null;
}

// ─── the start ──────────────────────────────────────────────────────────────────

pub const Start = union(enum) {
    /// Follow the stream from this sequence.
    from: u64,
    /// Readings `covers + 1 .. first - 1` left the stream before any file held them.
    lost: struct { covers: u64, first: u64 },
};

/// `archived`: the manifest existed. A tenant with no archive yet has nothing to lose, and a
/// stream with no message has pruned nothing the archive needs.
pub fn startAt(archived: bool, covers: u64, stream_first: u64, stream_messages: u64, accept_loss: bool) Start {
    if (archived and stream_messages > 0 and stream_first > covers + 1 and !accept_loss)
        return .{ .lost = .{ .covers = covers, .first = stream_first } };
    return .{ .from = @max(covers + 1, stream_first) };
}

// ─── a chunk ────────────────────────────────────────────────────────────────────

pub const Chunk = enum {
    /// Nothing more on the stream: the cycle ends.
    empty,
    /// The chunk starts after `pos + 1`: the stream pruned readings before they were read.
    pruned,
    /// The chunk has fewer messages than its range: a hole inside it.
    gap,
    /// Readings to write as one file, then the manifest.
    rows,
    /// Only skipped messages: the manifest's coverage moves past them, no file.
    skipped_only,
};

/// `pos` is the last stream sequence the archive covers. A hole is never certified as covered.
pub fn checkChunk(s: Summary, pos: u64, accept_loss: bool) Chunk {
    if (s.messages == 0) return .empty;
    if (s.first_seq != pos + 1 and !accept_loss) return .pruned;
    if (s.last_seq < s.first_seq or s.messages != s.last_seq - s.first_seq + 1) return .gap;
    return if (s.rows > 0) .rows else .skipped_only;
}

/// A cycle reads chunks until one comes back short of the batch.
pub fn lastOfCycle(s: Summary, batch: u64) bool {
    return s.messages < batch;
}

/// A cycle starts when a batch is waiting, or readings have waited `flush_s`.
pub fn shouldFlush(pending: u64, waited_s: f64, batch: u64, flush_s: f64) bool {
    return pending > 0 and (pending >= batch or waited_s >= flush_s);
}

// ─── file names ─────────────────────────────────────────────────────────────────

pub const Date = struct { year: i64, month: u8, day: u8 };

/// The UTC date of a time in milliseconds since 1970 (`sensor_ms` is 0 ..= 9999-12-31).
/// Howard Hinnant's `civil_from_days`, the proleptic Gregorian calendar Python's datetime uses.
pub fn dateOf(ms: i64) Date {
    const z = @divFloor(ms, std.time.ms_per_day) + 719468;
    const era = @divFloor(z, 146097);
    const doe = z - era * 146097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d = doy - @divFloor(153 * mp + 2, 5) + 1;
    const m = if (mp < 10) mp + 3 else mp - 9;
    return .{ .year = yoe + era * 400 + @as(i64, if (m <= 2) 1 else 0), .month = @intCast(m), .day = @intCast(d) };
}

/// `<tenant>/readings/<yyyy-mm-dd>/<first_seq:012>-<last_seq:012>.parquet`, the day being the
/// sensor date of the chunk's first row.
pub fn fileKey(buf: []u8, tenant: []const u8, first_ms: i64, first_seq: u64, last_seq: u64) ![]const u8 {
    const d = dateOf(first_ms);
    return std.fmt.bufPrint(buf, "{s}/" ++ table ++ "/{d:0>4}-{d:0>2}-{d:0>2}/{d:0>12}-{d:0>12}.parquet", .{
        tenant, @as(u64, @intCast(d.year)), d.month, d.day, first_seq, last_seq,
    });
}

pub fn manifestKey(buf: []u8, tenant: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}." ++ table, .{tenant});
}

// ─── the manifest ───────────────────────────────────────────────────────────────

/// A manifest as read, kept as a JSON value so that keys this version does not know survive
/// a rewrite. Owns nothing: everything lives in the arena given to `parse`/`empty`.
pub const Manifest = struct {
    root: std.json.Value,

    pub fn empty(arena: std.mem.Allocator) !Manifest {
        var obj: std.json.ObjectMap = .empty;
        try obj.put(arena, "covers_up_to", .{ .integer = 0 });
        try obj.put(arena, "files", .{ .array = .init(arena) });
        try obj.put(arena, "schema", .{ .integer = schema });
        return .{ .root = .{ .object = obj } };
    }

    /// error.BadManifest for a value that is not an object with an integer `covers_up_to` ≥ 0
    /// and a `files` array: the archiver stops rather than overwrite what it cannot read.
    pub fn parse(arena: std.mem.Allocator, bytes: []const u8) !Manifest {
        const root = std.json.parseFromSliceLeaky(std.json.Value, arena, bytes, .{}) catch return error.BadManifest;
        if (root != .object) return error.BadManifest;
        const c = root.object.get("covers_up_to") orelse return error.BadManifest;
        if (c != .integer or c.integer < 0) return error.BadManifest;
        const f = root.object.get("files") orelse return error.BadManifest;
        if (f != .array) return error.BadManifest;
        return .{ .root = root };
    }

    pub fn coversUpTo(m: Manifest) u64 {
        return @intCast(m.root.object.get("covers_up_to").?.integer);
    }

    pub fn fileCount(m: Manifest) usize {
        return m.root.object.get("files").?.array.items.len;
    }

    pub fn setCoversUpTo(m: *Manifest, seq: u64) void {
        m.root.object.getPtr("covers_up_to").?.* = .{ .integer = @intCast(seq) };
    }

    /// One file, and the coverage moved to its last sequence. `key` is copied into the arena.
    pub fn addFile(m: *Manifest, arena: std.mem.Allocator, key: []const u8, rows: u64, bytes: u64, first_seq: u64, last_seq: u64) !void {
        var obj: std.json.ObjectMap = .empty;
        try obj.put(arena, "key", .{ .string = try arena.dupe(u8, key) });
        try obj.put(arena, "rows", .{ .integer = @intCast(rows) });
        try obj.put(arena, "bytes", .{ .integer = @intCast(bytes) });
        try obj.put(arena, "first_seq", .{ .integer = @intCast(first_seq) });
        try obj.put(arena, "last_seq", .{ .integer = @intCast(last_seq) });
        try m.root.object.getPtr("files").?.array.append(.{ .object = obj });
        m.setCoversUpTo(last_seq);
    }

    pub fn serialize(m: Manifest, gpa: std.mem.Allocator) ![]u8 {
        return std.json.Stringify.valueAlloc(gpa, m.root, .{});
    }
};

// ─── tests ──────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "retention: two flushes and an outage, a stream with no age bound is enough" {
    try testing.expectEqual(@as(?f64, 3720), retentionShort(3719 * std.time.ns_per_s, 60, 3600));
    try testing.expectEqual(@as(?f64, null), retentionShort(3720 * std.time.ns_per_s, 60, 3600));
    try testing.expectEqual(@as(?f64, null), retentionShort(0, 60, 3600));
}

test "start: a pruned prefix is lost only for an archive that exists" {
    try testing.expectEqual(Start{ .lost = .{ .covers = 4, .first = 6 } }, startAt(true, 4, 6, 10, false));
    try testing.expectEqual(Start{ .from = 6 }, startAt(true, 4, 6, 10, true));
    try testing.expectEqual(Start{ .from = 6 }, startAt(false, 0, 6, 10, false));
    try testing.expectEqual(Start{ .from = 5 }, startAt(true, 4, 5, 10, false));
    try testing.expectEqual(Start{ .from = 9 }, startAt(true, 4, 9, 0, false)); // an empty stream: nothing lost, follow from its next
    try testing.expectEqual(Start{ .from = 5 }, startAt(true, 4, 2, 10, false)); // never re-read what is covered
}

test "a chunk moves the manifest only when it is whole" {
    try testing.expectEqual(Chunk.empty, checkChunk(.{}, 0, false));
    try testing.expectEqual(Chunk.rows, checkChunk(.{ .messages = 2, .rows = 1, .first_seq = 1, .last_seq = 2 }, 0, false));
    try testing.expectEqual(Chunk.skipped_only, checkChunk(.{ .messages = 1, .rows = 0, .first_seq = 1, .last_seq = 1 }, 0, false));
    try testing.expectEqual(Chunk.pruned, checkChunk(.{ .messages = 1, .rows = 1, .first_seq = 5, .last_seq = 5 }, 0, false));
    try testing.expectEqual(Chunk.rows, checkChunk(.{ .messages = 1, .rows = 1, .first_seq = 5, .last_seq = 5 }, 0, true));
    // A hole inside the chunk is refused even with --accept-loss: it is not a prefix.
    try testing.expectEqual(Chunk.gap, checkChunk(.{ .messages = 2, .rows = 2, .first_seq = 1, .last_seq = 3 }, 0, true));
    try testing.expectEqual(Chunk.gap, checkChunk(.{ .messages = 2, .rows = 2, .first_seq = 3, .last_seq = 1 }, 2, false));
}

test "flush on a full batch or after the wait" {
    try testing.expect(!shouldFlush(0, 1000, 10, 60));
    try testing.expect(shouldFlush(10, 0, 10, 60));
    try testing.expect(shouldFlush(1, 60, 10, 60));
    try testing.expect(!shouldFlush(1, 59.9, 10, 60));
    try testing.expect(lastOfCycle(.{ .messages = 9 }, 10));
    try testing.expect(!lastOfCycle(.{ .messages = 10 }, 10));
}

test "dates: Python's datetime, from 1970 to the year 9999" {
    try testing.expectEqual(Date{ .year = 1970, .month = 1, .day = 1 }, dateOf(0));
    try testing.expectEqual(Date{ .year = 2023, .month = 11, .day = 14 }, dateOf(1_700_000_000_000));
    try testing.expectEqual(Date{ .year = 2000, .month = 2, .day = 29 }, dateOf(951_782_400_000));
    try testing.expectEqual(Date{ .year = 9999, .month = 12, .day = 31 }, dateOf(253_402_300_799_999));
    try testing.expectEqual(Date{ .year = 1972, .month = 12, .day = 31 }, dateOf(94_694_399_999));
}

test "the file key is the Python archiver's" {
    var buf: [256]u8 = undefined;
    try testing.expectEqualStrings("acme/readings/2023-11-14/000000000001-000000000003.parquet", try fileKey(&buf, "acme", 1_700_000_000_000, 1, 3));
    try testing.expectEqualStrings("acme.readings", try manifestKey(&buf, "acme"));
}

test "a manifest keeps the keys it does not know" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var m = try Manifest.parse(arena, "{\"covers_up_to\": 4, \"files\": [], \"schema\": 1, \"note\": {\"by\": \"ops\"}}");
    try testing.expectEqual(@as(u64, 4), m.coversUpTo());
    try m.addFile(arena, "acme/readings/x.parquet", 2, 100, 5, 9);
    m.setCoversUpTo(11);
    const out = try m.serialize(testing.allocator);
    defer testing.allocator.free(out);
    try testing.expectEqualStrings("{\"covers_up_to\":11,\"files\":[{\"key\":\"acme/readings/x.parquet\",\"rows\":2,\"bytes\":100,\"first_seq\":5,\"last_seq\":9}],\"schema\":1,\"note\":{\"by\":\"ops\"}}", out);
}

test "a manifest the archiver cannot read is not overwritten" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    for ([_][]const u8{ "[]", "{}", "{\"covers_up_to\": -1, \"files\": []}", "{\"covers_up_to\": 1}", "{\"covers_up_to\": \"1\", \"files\": []}", "not json" }) |bad|
        try testing.expectError(error.BadManifest, Manifest.parse(arena, bad));
    const e = try Manifest.empty(arena);
    const out = try e.serialize(testing.allocator);
    defer testing.allocator.free(out);
    try testing.expectEqualStrings("{\"covers_up_to\":0,\"files\":[],\"schema\":1}", out);
}
