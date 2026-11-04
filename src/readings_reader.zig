//! One read of a tenant's readings stream into a CSV file: the reader behind both
//! `bridge_readings_fetch` (a process per chunk, for the Python archiver) and the native
//! archiver (archiver.zig, in-process). One function, so the two cannot read differently.
//!
//! CSV, one line per reading, no header — strings quoted, numbers not:
//!   "tenant","sensor","kind",value,sensor_ms,received_ms,stream_seq
//!
//! The consumer is a throwaway, unacknowledged one that the server reaps: the position is the
//! caller's (the archive's manifest).

const std = @import("std");
const nats = @import("nats");
const decode_mod = @import("readings_decode.zig");
const core = @import("archive_core.zig");

/// Fixed names, bridge-side only (topology.zig, "sensor readings"); a test there pins them.
pub const stream_prefix = "READINGS_";
pub const subject_prefix = "sensors";

pub const Options = struct {
    tenant: []const u8,
    /// The first stream sequence to read (the archive's covers_up_to + 1).
    from: u64,
    /// Stop after this many messages.
    max: u64 = 100_000,
    /// Stop when nothing arrives for this long.
    idle_ms: u64 = 500,
    /// Messages per pull request.
    batch: usize = 5000,
};

pub const Error = error{
    /// The stream `READINGS_<tenant>` does not exist.
    StreamNotFound,
    /// A hole after the first message: the CSV is incomplete and must not move the manifest.
    SequenceGap,
};

/// A CSV string field: quoted, a quote doubled. A subject token may hold a comma or a quote.
fn appendQuoted(buf: *std.ArrayList(u8), gpa: std.mem.Allocator, s: []const u8) !void {
    try buf.append(gpa, '"');
    for (s) |ch| {
        if (ch == '"') try buf.append(gpa, '"');
        try buf.append(gpa, ch);
    }
    try buf.append(gpa, '"');
}

/// Reads up to `o.max` messages from `o.from` into `out_path` (created, truncated) and says
/// what it read. An empty range is a Summary with `messages == 0`, not an error. A
/// `first_seq` above `o.from` means the stream pruned readings before they were read: the
/// caller decides (archive_core.checkChunk), this function reports.
/// On error.SequenceGap, `gap` holds (the last sequence read, the one that came after it).
pub fn readChunk(gpa: std.mem.Allocator, io: std.Io, js: nats.JetStream, o: Options, out_path: []const u8, gap: *[2]u64) !core.Summary {
    const stream = try std.fmt.allocPrint(gpa, "{s}{s}", .{ stream_prefix, o.tenant });
    defer gpa.free(stream);
    const filter = try std.fmt.allocPrint(gpa, "{s}.{s}.>", .{ subject_prefix, o.tenant });
    defer gpa.free(filter);

    var name_buf: [32]u8 = undefined;
    var rnd: [6]u8 = undefined;
    io.random(&rnd);
    const cname = try std.fmt.bufPrint(&name_buf, "rf{x}", .{rnd});

    const sub = js.pullSubscribe(null, cname, .{
        .stream = stream,
        .config = .{
            .name = cname,
            .durable_name = cname,
            .ack_policy = .none,
            .filter_subject = filter,
            .deliver_policy = .by_start_sequence,
            .opt_start_seq = o.from,
            // The server reaps it: the archiver's grants do not include CONSUMER.DELETE.
            .inactive_threshold = 30 * std.time.ns_per_s,
        },
    }) catch |err| {
        if (err == error.StreamNotFound) return error.StreamNotFound;
        return err;
    };
    defer sub.deinit();
    sub.pull_depth = 2;
    sub.max_bytes = 8 * 1024 * 1024;
    // A fetch returns when its batch is full or this long after the first message.
    sub.idle_after_first = .{ .duration = .{ .raw = .fromMilliseconds(@intCast(o.idle_ms)), .clock = .awake } };

    const file = try std.Io.Dir.cwd().createFile(io, out_path, .{});
    defer file.close(io);
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(gpa);
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();

    const idle: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(@intCast(o.idle_ms)), .clock = .awake } };
    var s: core.Summary = .{};
    var first_ms: ?i64 = null;
    while (s.messages < o.max) {
        const want: usize = @intCast(@min(@as(u64, o.batch), o.max - s.messages));
        var batch = sub.fetch(want, idle) catch |err| switch (err) {
            error.Timeout => break,
            else => return err,
        };
        defer batch.deinit();
        if (batch.messages.len == 0) break;
        _ = arena_state.reset(.retain_capacity);
        const arena = arena_state.allocator();
        for (batch.messages) |m| {
            const seq = m.metadata.sequence.stream;
            // The first sequence reports a prefix already pruned before this read. A hole
            // AFTER it is different: never certify the missing messages as covered, even
            // when retention advances while a multi-batch fetch is running.
            if (s.messages != 0 and (s.last_seq == std.math.maxInt(u64) or seq != s.last_seq + 1)) {
                gap.* = .{ s.last_seq, seq };
                return error.SequenceGap;
            }
            s.messages += 1;
            if (s.first_seq == 0) s.first_seq = seq;
            s.last_seq = seq;
            const received_ms = m.metadata.timestamp.toMilliseconds();
            const row = decode_mod.decode(arena, m.msg.subject, m.msg.data, received_ms) orelse continue;
            s.rows += 1;
            if (first_ms == null) {
                first_ms = row.sensor_ms;
                s.first_row_seq = seq;
            }
            try appendQuoted(&buf, gpa, row.tenant);
            try buf.append(gpa, ',');
            try appendQuoted(&buf, gpa, row.sensor);
            try buf.append(gpa, ',');
            try appendQuoted(&buf, gpa, row.kind);
            try buf.print(gpa, ",{d},{d},{d},{d}\n", .{ row.value, row.sensor_ms, received_ms, seq });
            if (buf.items.len >= 1 << 20) {
                try file.writeStreamingAll(io, buf.items);
                buf.clearRetainingCapacity();
            }
        }
    }
    if (buf.items.len > 0) try file.writeStreamingAll(io, buf.items);
    s.first_ms = first_ms orelse 0;
    return s;
}

test "the names are the topology's" {
    const topology = @import("topology.zig");
    var owned = try topology.loadEmbedded(std.testing.allocator);
    defer owned.deinit();
    try std.testing.expectEqualStrings(owned.topology.readings_stream_prefix, stream_prefix);
    try std.testing.expectEqualStrings(owned.topology.subject_readings_prefix, subject_prefix);
}
