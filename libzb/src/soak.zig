//! zb-soak: the tombstone/GC soak, driven through the Zig client's own `mutate()`.
//!
//! Every INTERVAL seconds: INSERT a fresh row, then DELETE it — both as real
//! mutations over NATS, not psql. With the write guards installed, the DELETE writes
//! `deleted_at` rather than removing the row, so each cycle leaves a tombstone for
//! `bridge_sweeper` to reap. Every uid touched is appended to a ledger file, which is
//! what makes the run CHECKABLE afterwards rather than merely survivable.
//!
//! The property under test is one sentence: **no delete is ever lost.** At the end,
//! every uid in the ledger must be either tombstoned (`deleted_at` set) or reaped
//! (gone). A uid that is present and ALIVE means a delete was dropped, or a row was
//! resurrected — which is exactly the failure the GC watermark exists to prevent.
//!
//!   cd libzb && zig build
//!   # run the sweeper with a threshold SHORTER than the soak, or nothing is reaped:
//!   GC_THRESHOLD_MS=120000 ../zig-out/bin/bridge_sweeper &
//!   ZB_SOAK_SECONDS=3600 ZB_SOAK_INTERVAL=30 ./zig-out/bin/zb-soak
//!   # then, against PostgreSQL:
//!   ../scripts/soak-check.sh soak-uids.txt
//!
//! ⚠️ The sweeper's GC_THRESHOLD_MS defaults to one hour. Left at the default for a
//! one-hour soak, NOTHING is reaped and the run proves only that tombstones are
//! written — half the test.

const std = @import("std");
const client = @import("client.zig");

fn env(name: [*:0]const u8, dflt: u64) u64 {
    const v = std.c.getenv(name) orelse return dflt;
    return std.fmt.parseInt(u64, std.mem.span(v), 10) catch dflt;
}

/// A uuid-shaped id that is unique per (process, cycle) without needing an Io —
/// `std.crypto.random` wants one in 0.16, and this only has to not collide.
fn soakUid(a: std.mem.Allocator, seed: u64, n: u64) ![]const u8 {
    return std.fmt.allocPrint(a, "{x:0>8}-{x:0>4}-4{x:0>3}-8{x:0>3}-{x:0>12}", .{
        @as(u32, @truncate(seed >> 16)), @as(u16, @truncate(seed)),
        @as(u12, @truncate(n >> 12)),    @as(u12, @truncate(n)),
        @as(u48, @truncate(seed ^ (n << 8))),
    });
}

fn nowIso(a: std.mem.Allocator) ![]const u8 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    const es = std.time.epoch.EpochSeconds{ .secs = @intCast(ts.sec) };
    const yd = es.getEpochDay().calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();
    return std.fmt.allocPrint(a, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}Z", .{
        yd.year, md.month.numeric(), @as(u32, md.day_index) + 1,
        ds.getHoursIntoDay(), ds.getMinutesIntoHour(), ds.getSecondsIntoMinute(),
        @as(u64, @intCast(@divTrunc(@as(i64, ts.nsec), 1000))),
    });
}

pub fn main() !void {
    const a = std.heap.c_allocator;
    const seconds = env("ZB_SOAK_SECONDS", 3600);
    const interval = env("ZB_SOAK_INTERVAL", 30);
    const table = if (std.c.getenv("ZB_SOAK_TABLE")) |t| std.mem.span(t) else "test_types";

    // One replica for the whole run: the outbox has to survive between cycles, which
    // is half of what is being soaked.
    const c = try client.SyncClient.init(a, .{
        .url = "nats://127.0.0.1:4222",
        .creds_path = "../scripts/native/creds/omar.creds",
        .grammar_path = "../grammar.json",
        .db_path = "zbz-soak.sqlite3",
        .principal = "omar",
        .tables = &.{ "users", "salaries", "test_types" },
        .client_id = "zig-soak",
    });
    defer c.deinit();

    try c.resolveTenant();
    try c.syncSchemas();
    try c.ensureOutbox();
    try c.subscribeVerdicts(); // before the first write, or its verdict is gone

    // ⚠️ std.fs.cwd() is gone in 0.16 and std.Io.Dir.cwd() wants an Io. libc is
    // linked, so the posix layer is the one that exists — same call shape the bridge
    // uses to read cgroup files.
    const ledger = try std.posix.openat(std.posix.AT.FDCWD, "soak-uids.txt",
        .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, 0o644);
    defer _ = std.posix.system.close(ledger);

    // pid + start second: unique enough that two soaks never mint the same uid,
    // without needing an Io for std.crypto.random (0.16).
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    const seed: u64 = (@as(u64, @intCast(std.c.getpid())) << 32) ^ @as(u64, @intCast(ts.sec));

    const deadline = @as(i64, ts.sec) + @as(i64, @intCast(seconds));
    var cycle: u64 = 0;
    var inserted: u64 = 0;
    var deleted: u64 = 0;
    var settled: usize = 0;

    std.debug.print("zb-soak: {d}s at {d}s intervals on '{s}' — ledger: soak-uids.txt\n",
        .{ seconds, interval, table });

    while (true) {
        _ = std.c.clock_gettime(.REALTIME, &ts);
        if (@as(i64, ts.sec) >= deadline) break;
        cycle += 1;

        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const aa = arena.allocator();

        const uid = try soakUid(aa, seed, cycle);
        const now = try nowIso(aa);

        var key: std.json.ObjectMap = .empty;
        try key.put(aa, "uid", .{ .string = uid });

        var vals: std.json.ObjectMap = .empty;
        try vals.put(aa, "uid", .{ .string = uid });
        try vals.put(aa, "tenant_id", .{ .string = "kilo" });
        try vals.put(aa, "some_text", .{ .string = "soak" });
        try vals.put(aa, "inserted_at", .{ .string = now });
        try vals.put(aa, "updated_at", .{ .string = now });

        _ = c.mutate(table, "INSERT", .{ .object = key }, .{ .object = vals }) catch |err| {
            std.debug.print("cycle {d}: INSERT failed: {any}\n", .{ cycle, err });
            continue;
        };
        inserted += 1;

        // The DELETE is a separate mutation, as a client would send it — the tombstone
        // is written by the guard in PostgreSQL, not by us.
        _ = c.mutate(table, "DELETE", .{ .object = key }, null) catch |err| {
            std.debug.print("cycle {d}: DELETE failed: {any}\n", .{ cycle, err });
            continue;
        };
        deleted += 1;

        const line = try std.fmt.allocPrint(aa, "{s}\n", .{uid});
        _ = std.c.write(ledger, line.ptr, line.len);

        settled += c.drainVerdicts(3000) catch 0;
        if (cycle % 10 == 0 or cycle == 1) {
            std.debug.print("  cycle {d}: inserted={d} deleted={d} verdicts={d} watermark={s}\n",
                .{ cycle, inserted, deleted, settled, c.gcWatermark(aa) orelse "(none)" });
        }

        // ⚠️ No std.c.sleep and no std.time.sleep in 0.16 — nanosleep is the one
        // that exists through libc.
        var nap: std.c.timespec = .{ .sec = @intCast(interval), .nsec = 0 };
        _ = std.c.nanosleep(&nap, null);
    }

    // A final pass: verdicts for the last cycle, and whatever the outbox still holds.
    settled += c.drainVerdicts(5000) catch 0;
    var fa = std.heap.ArenaAllocator.init(a);
    defer fa.deinit();
    std.debug.print("zb-soak done: cycles={d} inserted={d} deleted={d} verdicts={d} watermark={s}\n",
        .{ cycle, inserted, deleted, settled, c.gcWatermark(fa.allocator()) orelse "(none)" });
}
