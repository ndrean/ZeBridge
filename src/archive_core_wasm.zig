//! The readings archiver's decisions for a WebAssembly host (`zig build archive-core-wasm`,
//! zig-out/wasm/zb_archive_core.wasm): the rules of archive_core.zig and the payload decoder of
//! readings_decode.zig, so a JavaScript shell (Node, Bun, Deno, a Worker) archives and decodes
//! exactly as the native archiver does. The I/O — NATS, S3, Parquet — stays the host's.
//!
//! The memory protocol is libzb's (libzb/src/wasm_core.zig): `zba_alloc(len)` for arguments,
//! a call returns its JSON result as (offset << 32 | length), 0 for a failure, and
//! `zba_reset()` frees everything the calls allocated. Plain numbers cross as numbers.
const std = @import("std");
const core = @import("archive_core.zig");
const decode_mod = @import("readings_decode.zig");
const Value = std.json.Value;

var arena = std.heap.ArenaAllocator.init(std.heap.wasm_allocator);

export fn zba_alloc(len: usize) ?[*]u8 {
    const buf = arena.allocator().alloc(u8, len) catch return null;
    return buf.ptr;
}

export fn zba_reset() void {
    _ = arena.reset(.retain_capacity);
}

fn located(bytes: []const u8) u64 {
    return (@as(u64, @intFromPtr(bytes.ptr)) << 32) | @as(u64, bytes.len);
}

/// Seconds the stream must keep readings, or 0 when its `max_age` (ns, 0 = unbounded) is enough.
export fn zba_retention_short(max_age_ns: u64, flush_s: f64, outage_s: f64) f64 {
    return core.retentionShort(max_age_ns, flush_s, outage_s) orelse 0;
}

/// The first sequence to follow, or 0 when readings were lost (then `stream_first` names
/// where the stream starts, and `covers + 1 .. stream_first - 1` are gone).
export fn zba_start_at(archived: bool, covers: u64, stream_first: u64, stream_messages: u64, accept_loss: bool) u64 {
    return switch (core.startAt(archived, covers, stream_first, stream_messages, accept_loss)) {
        .from => |s| s,
        .lost => 0,
    };
}

/// archive_core.Chunk as its ordinal: 0 empty, 1 pruned, 2 gap, 3 rows, 4 skipped_only.
export fn zba_check_chunk(messages: u64, rows: u64, first_seq: u64, last_seq: u64, pos: u64, accept_loss: bool) u32 {
    return @intFromEnum(core.checkChunk(.{ .messages = messages, .rows = rows, .first_seq = first_seq, .last_seq = last_seq }, pos, accept_loss));
}

/// One stream message (`subject`, `data` as UTF-8 at their offsets) as
/// `{"tenant","sensor_id","kind","value","sensor_ms"}`, or 0 when it is not a reading.
export fn zba_decode(subject: [*]const u8, subject_len: usize, data: [*]const u8, data_len: usize, received_ms: i64) u64 {
    const a = arena.allocator();
    const row = decode_mod.decode(a, subject[0..subject_len], data[0..data_len], received_ms) orelse return 0;
    const text = std.json.Stringify.valueAlloc(a, .{
        .tenant = row.tenant,
        .sensor_id = row.sensor,
        .kind = row.kind,
        .value = row.value,
        .sensor_ms = row.sensor_ms,
    }, .{}) catch return 0;
    return located(text);
}

/// The object key of a chunk's file.
export fn zba_file_key(tenant: [*]const u8, tenant_len: usize, first_ms: i64, first_seq: u64, last_seq: u64) u64 {
    const buf = arena.allocator().alloc(u8, tenant_len + 64) catch return 0;
    const key = core.fileKey(buf, tenant[0..tenant_len], first_ms, first_seq, last_seq) catch return 0;
    return located(key);
}

/// A manifest (`ptr`, JSON) with one file added: `{"manifest", "key", "rows", "bytes",
/// "first_seq", "last_seq"}` → the manifest to write. Keys the archiver does not know are kept.
export fn zba_manifest_add(ptr: [*]const u8, len: usize) u64 {
    const a = arena.allocator();
    const args = std.json.parseFromSliceLeaky(Value, a, ptr[0..len], .{}) catch return 0;
    if (args != .object) return 0;
    const o = args.object;
    const m_text = std.json.Stringify.valueAlloc(a, o.get("manifest") orelse return 0, .{}) catch return 0;
    var m = core.Manifest.parse(a, m_text) catch return 0;
    const int = struct {
        fn get(obj: std.json.ObjectMap, k: []const u8) ?u64 {
            const v = obj.get(k) orelse return null;
            return if (v == .integer and v.integer >= 0) @intCast(v.integer) else null;
        }
    }.get;
    const key = o.get("key") orelse return 0;
    if (key != .string) return 0;
    m.addFile(a, key.string, int(o, "rows") orelse return 0, int(o, "bytes") orelse return 0, int(o, "first_seq") orelse return 0, int(o, "last_seq") orelse return 0) catch return 0;
    return located(m.serialize(a) catch return 0);
}
