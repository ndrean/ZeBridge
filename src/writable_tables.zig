//! Per-table edge-writability, computed once by `preflight.zig` and read back by
//! `event_processor.zig` when it builds a table's write contract (NOTES.md §1.11).
//!
//! Preflight already concludes the right answer — a usable version column
//! (`SYNC_RULES`), the writer's actual `INSERT` grant, and whether the primary key is
//! database-allocated (which refuses edge writes outright, independent of the grant) —
//! but until this registry existed the verdict was print-only: a boot-time log line and
//! nothing else. This is where it is kept instead, so `appendWriteContract` can publish
//! it rather than a client discovering "outbound-only" only after a `42501` rejection.
//!
//! `true`/`false` is the whole-bridge fact — there is one `bridge_writer` role, so
//! "writable" is the same answer for every principal today. That stays correct even if
//! per-principal write grants arrive later (§7.1): the field does not change type, it
//! just gets computed more precisely, and a client that already trusts it needs no
//! change to benefit.
//!
//! ## Why an append-only array rather than a hash map
//!
//! Same reasoning as `refused_tables.zig`: no `std.Io.Mutex` (0.16 wants an `Io` threaded
//! through callers that have none), so entries are appended and never removed, and a
//! reader may hold a name slice with no risk of it being freed under it. Writers reserve
//! a slot with an atomic `fetchAdd` and publish `len` in reservation order.
//!
//! Two writers: preflight's boot pass (`run`/`reportVersionColumns`), which completes
//! before any other thread starts, and `event_processor.reportEdgeWritability` on a
//! `CREATE TABLE` DDL event thereafter — never concurrent with each other, but the read
//! side (`appendWriteContract`, building a schema payload) can run on a different thread
//! than either, so the same atomic discipline applies regardless.

const std = @import("std");

const log = std.log.scoped(.writable_tables);

/// Far beyond any realistic number of tables in one publication. Fixed rather than
/// growable so a concurrent reader is never walking a reallocated array.
pub const max_tables = 256;

const Entry = struct {
    /// Owned. Freed only in `deinit`, so readers may hold it while writers work.
    name: []const u8,
    /// Written with release ordering, read with acquire — the payload of the entry.
    writable: std.atomic.Value(bool) = .init(false),
};

pub const Registry = struct {
    allocator: std.mem.Allocator,
    entries: [max_tables]Entry = undefined,
    /// Published with release ordering once an entry is fully initialised. Only grows.
    len: std.atomic.Value(usize) = .init(0),
    /// Slots handed out, separate from `len` for the same reason `refused_tables.zig`
    /// splits them: two writers reserving concurrently must not claim the same index.
    reserved: std.atomic.Value(usize) = .init(0),
    /// Set once if a table could not be recorded for want of capacity. A table silently
    /// missing from this registry reads to `appendWriteContract` as "unknown", which
    /// omits the field rather than publishing a wrong guess — safe, but worth knowing
    /// about, so this is loud once rather than silent.
    overflowed: std.atomic.Value(bool) = .init(false),

    pub fn init(allocator: std.mem.Allocator) Registry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Registry) void {
        for (self.entries[0..self.len.load(.acquire)]) |e| self.allocator.free(e.name);
        self.* = undefined;
    }

    fn find(self: *const Registry, table: []const u8) ?*Entry {
        const n = self.len.load(.acquire);
        for (self.entries[0..n]) |*e| {
            if (std.mem.eql(u8, e.name, table)) return @constCast(e);
        }
        return null;
    }

    /// Record whether `table` accepts edge writes right now. Idempotent: a table's
    /// verdict can change across a restart (a grant added or dropped, a PK swapped for
    /// one that is database-allocated), and a re-check simply overwrites the flag on the
    /// existing entry rather than requiring a fresh slot.
    pub fn set(self: *Registry, table: []const u8, writable: bool) !void {
        if (self.find(table)) |e| {
            e.writable.store(writable, .release);
            return;
        }

        const owned = try self.allocator.dupe(u8, table);
        errdefer self.allocator.free(owned);

        const n = self.reserved.fetchAdd(1, .acq_rel);
        if (n >= max_tables) {
            if (!self.overflowed.swap(true, .acq_rel)) {
                log.err(
                    "🔴 Writable-table registry is full ({d} tables) — '{s}' cannot be recorded and its schema payload will omit \"writable\".",
                    .{ max_tables, table },
                );
            }
            return error.WritableRegistryFull;
        }

        self.entries[n] = .{ .name = owned, .writable = .init(writable) };

        while (self.len.cmpxchgWeak(n, n + 1, .release, .acquire) != null) {
            std.atomic.spinLoopHint();
        }
    }

    /// `null` means "not yet reported" — a table preflight has not checked, or one that
    /// registered late and lost the race with a reader. `appendWriteContract` omits the
    /// field for `null` rather than guessing either way.
    pub fn get(self: *const Registry, table: []const u8) ?bool {
        const e = self.find(table) orelse return null;
        return e.writable.load(.acquire);
    }
};

test "unrecorded table reads as unknown" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try std.testing.expectEqual(@as(?bool, null), r.get("users"));
}

test "set then get round-trips" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try r.set("orders", true);
    try r.set("users", false);

    try std.testing.expectEqual(@as(?bool, true), r.get("orders"));
    try std.testing.expectEqual(@as(?bool, false), r.get("users"));
}

test "re-checking a table overwrites the flag, not the slot" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try r.set("orders", false);
    try r.set("orders", true);

    try std.testing.expectEqual(@as(?bool, true), r.get("orders"));
    try std.testing.expectEqual(@as(usize, 1), r.len.load(.acquire));
}

test "overflow is reported rather than silently ignored" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    var buf: [32]u8 = undefined;
    for (0..max_tables) |i| {
        try r.set(try std.fmt.bufPrint(&buf, "t{d}", .{i}), true);
    }

    r.overflowed.store(true, .release);
    try std.testing.expectError(error.WritableRegistryFull, r.set("one_too_many", true));
    try std.testing.expectEqual(@as(?bool, null), r.get("one_too_many"));
}

test "names are owned, so callers may free their own" {
    var name_buf: [16]u8 = undefined;
    const scratch = try std.fmt.bufPrint(&name_buf, "orders", .{});

    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try r.set(scratch, true);
    @memset(&name_buf, 0);

    try std.testing.expectEqual(@as(?bool, true), r.get("orders"));
}

test "concurrent writers each get their own slot" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    const Worker = struct {
        fn run(reg: *Registry, base: usize) void {
            var buf: [32]u8 = undefined;
            for (0..8) |i| {
                const name = std.fmt.bufPrint(&buf, "t{d}", .{base + i}) catch return;
                reg.set(name, true) catch return;
            }
        }
    };

    var a = try std.Thread.spawn(.{}, Worker.run, .{ &r, 0 });
    var b = try std.Thread.spawn(.{}, Worker.run, .{ &r, 100 });
    a.join();
    b.join();

    try std.testing.expectEqual(@as(usize, 16), r.len.load(.acquire));
    for (0..8) |i| {
        var buf: [32]u8 = undefined;
        try std.testing.expectEqual(@as(?bool, true), r.get(try std.fmt.bufPrint(&buf, "t{d}", .{i})));
        try std.testing.expectEqual(@as(?bool, true), r.get(try std.fmt.bufPrint(&buf, "t{d}", .{100 + i})));
    }
}
