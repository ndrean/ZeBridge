//! §10er: which CDC streams are bursting, known from the one place that knows it
//! first — the publisher, at the moment it publishes.
//!
//! The edge watch (§10eq) protects a pair's cut by re-cutting before the stream
//! prunes past it. Its first version polled every stream every five seconds, which
//! is a `STREAM.INFO` per stream per five seconds — a cost that scales with tenants,
//! for a question that only matters on the streams that are moving. The publisher
//! pushes every byte of every stream, so it can say "this stream is hot" a second
//! into a burst, with no request to NATS at all; the producer then asks that stream
//! alone. The full scan stays as a slow safety net.
//!
//! Hot = more than `bytes_per_s` bytes or `msgs_per_s` messages in one second on one
//! stream, thresholds set from the stream caps: what would fill a tenth of the cap
//! within a minute. A mark lasts `hot_ttl_ms` past the last hot second, so the watch
//! keeps reading the stream while the burst drains.
//!
//! Two threads touch this: the publisher's flush thread writes, the producer's
//! thread reads. A spin lock, held for a map lookup — the same lock the registries
//! use — and every string owned here.
const std = @import("std");
const utils = @import("utils.zig");
const topology_mod = @import("topology.zig");

pub const log = std.log.scoped(.hot);

pub const HotStreams = struct {
    allocator: std.mem.Allocator,
    topo: *const topology_mod.Topology,
    bytes_per_s: u64,
    msgs_per_s: u64,
    hot_ttl_ms: i64 = 10_000,
    mutex: utils.SpinLock = .{},
    /// stream → the current second's accounting and the hot mark.
    streams: std.StringArrayHashMapUnmanaged(Entry) = .empty,

    const Entry = struct {
        second: i64 = 0,
        bytes: u64 = 0,
        msgs: u64 = 0,
        hot_until_ms: i64 = 0,
    };

    pub fn init(allocator: std.mem.Allocator, topo: *const topology_mod.Topology, max_bytes: i64, max_msgs: i64) HotStreams {
        return .{
            .allocator = allocator,
            .topo = topo,
            // A tenth of the cap in a minute; never under 1 MiB/s or 1,000 msg/s, so a
            // small cap does not make every trickle "hot".
            .bytes_per_s = @max(@as(u64, @intCast(@max(max_bytes, 0))) / 600, 1 << 20),
            .msgs_per_s = @max(@as(u64, @intCast(@max(max_msgs, 0))) / 600, 1_000),
        };
    }

    pub fn deinit(self: *HotStreams) void {
        var it = self.streams.iterator();
        while (it.next()) |e| self.allocator.free(e.key_ptr.*);
        self.streams.deinit(self.allocator);
    }

    /// The stream a CDC subject lands on: `cdc.<tenant>.<table>.<op>` rides
    /// `CDC_<tenant>` when the second token is a tenant that is not the open one,
    /// anything else rides `CDC_PUBLIC`. Written into `buf`.
    fn streamOf(self: *const HotStreams, subject: []const u8, buf: []u8) ?[]const u8 {
        if (!std.mem.startsWith(u8, subject, self.topo.subject_cdc_prefix)) return null;
        var it = std.mem.splitScalar(u8, subject, '.');
        _ = it.next(); // the prefix
        const second = it.next() orelse return null;
        for (self.topo.tenants) |t| {
            if (std.mem.eql(u8, t, second) and !std.mem.eql(u8, t, self.topo.open_tenant)) {
                return std.fmt.bufPrint(buf, "{s}{s}", .{ self.topo.cdc_stream_prefix, t }) catch null;
            }
        }
        return self.topo.cdc_stream_public;
    }

    /// One publish on `subject` of `bytes` bytes — the publisher's flush thread.
    pub fn account(self: *HotStreams, subject: []const u8, bytes: usize) void {
        var name_buf: [256]u8 = undefined;
        const stream = self.streamOf(subject, &name_buf) orelse return;
        const now_ms = utils.unixMillis();
        const second = @divFloor(now_ms, 1000);
        self.mutex.lock();
        defer self.mutex.unlock();
        const gop = self.streams.getOrPut(self.allocator, stream) catch return;
        if (!gop.found_existing) {
            gop.key_ptr.* = self.allocator.dupe(u8, stream) catch {
                _ = self.streams.swapRemove(stream);
                return;
            };
            gop.value_ptr.* = .{};
        }
        const e = gop.value_ptr;
        if (e.second != second) {
            e.second = second;
            e.bytes = 0;
            e.msgs = 0;
        }
        e.bytes += bytes;
        e.msgs += 1;
        if ((e.bytes >= self.bytes_per_s or e.msgs >= self.msgs_per_s) and e.hot_until_ms < now_ms) {
            log.warn("🔥 {s}: {d} KB and {d} message(s) in one second — hot; the producer watches its edge", .{ stream, e.bytes / 1024, e.msgs });
        }
        if (e.bytes >= self.bytes_per_s or e.msgs >= self.msgs_per_s) e.hot_until_ms = now_ms + self.hot_ttl_ms;
    }

    /// The streams hot right now, names duped into `a` — the producer's thread.
    pub fn hotNow(self: *HotStreams, a: std.mem.Allocator) ![]const []const u8 {
        const now_ms = utils.unixMillis();
        var out: std.ArrayListUnmanaged([]const u8) = .empty;
        self.mutex.lock();
        defer self.mutex.unlock();
        var it = self.streams.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.hot_until_ms > now_ms) try out.append(a, try a.dupe(u8, e.key_ptr.*));
        }
        return out.items;
    }
};
