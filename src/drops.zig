//! Events the bridge could not publish and dropped, counted by table and reason.
//!
//! A dropped row is gone from every replica until the table is re-seeded, and the bridge goes on:
//! a log line is not enough to know that a table is stale. This registry is what `/metrics`
//! (`bridge_dropped_events_total`, `bridge_table_degraded`), `/status` (`degraded_tables`) and
//! `bridge --diagnose` read. It is per process: a restart starts at zero, which is why a table's
//! health is "dropped something since boot", not "is consistent" — `zebridge_reseed` is what makes
//! it consistent again (CLAUDE.md, "Troubleshooting": recovery).
//!
//! Distinct from `refused_tables.zig`, which holds TABLE-level suspensions (no primary key, a row
//! wider than the buffer) with a lift; these are ROW-level drops of a table that otherwise works.
//!
//! One writer (the replication thread, through the event processor), any number of readers (the
//! HTTP thread): entries are filled before `len` is published, counts are atomics.
const std = @import("std");

pub const max_entries = 256;

/// Why a row was dropped. The tag name is the label value in `/metrics`.
pub const Reason = enum {
    /// The tenant column holds a value that cannot be a subject token: empty, longer than
    /// `limits.max_tenant_bytes`, or with `.`, `*`, `>`, whitespace or a control character.
    tenant_unusable,
    /// The tenant column is NULL or absent from the event (replica identity not covering it).
    tenant_missing,
    /// The subject did not fit the slot's buffer. The sizing makes this unreachable for supported
    /// identifiers (`limits.zig`); it stays as the backstop for an overlong subject prefix.
    subject_too_long,

    pub fn fixHint(self: Reason) []const u8 {
        return switch (self) {
            .tenant_unusable => "correct the tenant value in PostgreSQL, then zebridge_reseed(table)",
            .tenant_missing => "make the replica identity cover the tenant column, then zebridge_reseed(table)",
            .subject_too_long => "shorten the subject prefix in grammar.json, then zebridge_reseed(table)",
        };
    }
};

const Entry = struct {
    name_buf: [64]u8 = undefined,
    name_len: u8 = 0,
    reason: Reason = .tenant_unusable,
    count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Unix seconds of the latest drop.
    last: std.atomic.Value(i64) = std.atomic.Value(i64).init(0),

    fn name(self: *const Entry) []const u8 {
        return self.name_buf[0..self.name_len];
    }
};

pub const Registry = struct {
    entries: [max_entries]Entry = @splat(.{}),
    len: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    /// Drops of (table, reason) pairs beyond `max_entries`: counted, not named.
    overflow: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Count one dropped row. Returns how many this (table, reason) has lost since boot, so the
    /// caller can say so in its log line. Replication thread only.
    pub fn record(self: *Registry, table: []const u8, reason: Reason, now_unix: i64) u64 {
        const n = self.len.load(.acquire);
        for (self.entries[0..n]) |*e| {
            if (e.reason == reason and std.mem.eql(u8, e.name(), table)) {
                e.last.store(now_unix, .release);
                return e.count.fetchAdd(1, .acq_rel) + 1;
            }
        }
        if (n >= max_entries or table.len > 64) {
            return self.overflow.fetchAdd(1, .acq_rel) + 1;
        }
        const e = &self.entries[n];
        @memcpy(e.name_buf[0..table.len], table);
        e.name_len = @intCast(table.len);
        e.reason = reason;
        e.count.store(1, .release);
        e.last.store(now_unix, .release);
        self.len.store(n + 1, .release);
        return 1;
    }

    /// Whether the nth drop of a (table, reason) deserves a log line: the first few, then every
    /// thousandth. A tenant column filled with long values would otherwise log once per row.
    pub fn shouldLog(count: u64) bool {
        return count <= 3 or count % 1000 == 0;
    }

    pub fn total(self: *const Registry) u64 {
        var t: u64 = self.overflow.load(.acquire);
        for (self.entries[0..self.len.load(.acquire)]) |*e| t += e.count.load(.acquire);
        return t;
    }

    /// How many distinct tables have dropped a row.
    pub fn degradedTables(self: *const Registry) u64 {
        var n: u64 = 0;
        const len = self.len.load(.acquire);
        for (0..len) |i| {
            if (firstOf(self, i)) n += 1;
        }
        return n;
    }

    pub fn tableCount(self: *const Registry, table: []const u8) u64 {
        var t: u64 = 0;
        for (self.entries[0..self.len.load(.acquire)]) |*e| {
            if (std.mem.eql(u8, e.name(), table)) t += e.count.load(.acquire);
        }
        return t;
    }

    /// Prometheus text. Reads atomics and immutable names only: the HTTP thread may call it while
    /// the replication thread writes.
    pub fn writePrometheus(self: *const Registry, w: *std.Io.Writer) !void {
        try w.print("# HELP bridge_dropped_events_total Rows dropped before publication, by table and reason since this process started. Each is missing from every replica until the table is re-seeded (zebridge_reseed)\n", .{});
        try w.print("# TYPE bridge_dropped_events_total counter\n", .{});
        const n = self.len.load(.acquire);
        for (self.entries[0..n]) |*e| {
            try w.print("bridge_dropped_events_total{{table=\"{s}\",reason=\"{s}\"}} {d}\n", .{ e.name(), @tagName(e.reason), e.count.load(.acquire) });
        }
        const over = self.overflow.load(.acquire);
        if (over > 0) try w.print("bridge_dropped_events_total{{table=\"_other\",reason=\"overflow\"}} {d}\n", .{over});
        if (n > 0) {
            try w.print("# HELP bridge_table_degraded 1 for a table that has dropped rows since this process started (its replicas are missing them)\n", .{});
            try w.print("# TYPE bridge_table_degraded gauge\n", .{});
            for (self.entries[0..n], 0..) |*e, i| {
                // once per table: the first entry that names it
                if (firstOf(self, i)) try w.print("bridge_table_degraded{{table=\"{s}\"}} 1\n", .{e.name()});
            }
        }
    }

    fn firstOf(self: *const Registry, i: usize) bool {
        const name = self.entries[i].name();
        for (self.entries[0..i]) |*e| if (std.mem.eql(u8, e.name(), name)) return false;
        return true;
    }

    /// `[{"table":…,"reason":…,"dropped":N,"last":unix,"fix":…}]`, for /status and --diagnose.
    pub fn writeJson(self: *const Registry, w: *std.Io.Writer) !void {
        try w.print("[", .{});
        const n = self.len.load(.acquire);
        for (self.entries[0..n], 0..) |*e, i| {
            if (i > 0) try w.print(",", .{});
            try w.print("{{\"table\":\"{s}\",\"reason\":\"{s}\",\"dropped\":{d},\"last\":{d},\"fix\":\"{s}\"}}", .{
                e.name(), @tagName(e.reason), e.count.load(.acquire), e.last.load(.acquire), e.reason.fixHint(),
            });
        }
        try w.print("]", .{});
    }
};

const testing = std.testing;

test "a drop is counted by table and reason" {
    var r = Registry{};
    try testing.expectEqual(@as(u64, 1), r.record("orders", .tenant_unusable, 10));
    try testing.expectEqual(@as(u64, 2), r.record("orders", .tenant_unusable, 11));
    try testing.expectEqual(@as(u64, 1), r.record("orders", .tenant_missing, 12));
    try testing.expectEqual(@as(u64, 1), r.record("items", .tenant_unusable, 13));
    try testing.expectEqual(@as(u64, 4), r.total());
    try testing.expectEqual(@as(u64, 3), r.tableCount("orders"));
}

test "logging: the first few, then every thousandth" {
    try testing.expect(Registry.shouldLog(1) and Registry.shouldLog(3));
    try testing.expect(!Registry.shouldLog(4) and !Registry.shouldLog(999));
    try testing.expect(Registry.shouldLog(1000) and Registry.shouldLog(2000));
}

test "the exposition names the table once as degraded and every reason as a counter" {
    var r = Registry{};
    _ = r.record("orders", .tenant_unusable, 1);
    _ = r.record("orders", .subject_too_long, 2);
    var buf: [2048]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try r.writePrometheus(&w);
    const text = w.buffered();
    try testing.expect(std.mem.find(u8, text, "bridge_dropped_events_total{table=\"orders\",reason=\"tenant_unusable\"} 1") != null);
    try testing.expect(std.mem.find(u8, text, "bridge_dropped_events_total{table=\"orders\",reason=\"subject_too_long\"} 1") != null);
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, text, "bridge_table_degraded{table=\"orders\"} 1"));
}

test "past the registry's capacity the drops are still counted" {
    var r = Registry{};
    var name: [16]u8 = undefined;
    var i: usize = 0;
    while (i < max_entries + 5) : (i += 1) {
        const t = std.fmt.bufPrint(&name, "t{d}", .{i}) catch unreachable;
        _ = r.record(t, .tenant_unusable, 1);
    }
    try testing.expectEqual(@as(u64, max_entries + 5), r.total());
    try testing.expectEqual(@as(u64, 5), r.overflow.load(.acquire));
}
