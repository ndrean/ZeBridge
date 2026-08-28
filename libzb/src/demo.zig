//! zb-demo: the orchestration loop against the live stack — consumer #4.
//!
//!   cd libzb && zig build && ./zig-out/bin/zb-demo
//!
//! Connects as omar (client creds), resolves the tenant, builds users +
//! salaries from their schemas, seeds from the generation chains (parents
//! first, FK ON), drains CDC to the tail through the seed gate, and prints
//! counts to compare against Postgres.
//!
//! `ZB_WRITE=1` also exercises the WRITE path: one mutation on `salaries`,
//! stamped, applied optimistically, queued in the outbox, published — then a
//! flush (which gates on the GC watermark) and a verdict drain.

const std = @import("std");
const client = @import("client.zig");

/// The wire format for a timestamp the bridge will compare (§7.2).
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

    // Fresh replica every run (the browser dev convention): the point of the
    // demo is the full seed + catch-up path.
    //
    // ZB_KEEP_DB=1 keeps it, which is what the WRITE path needs to be observed at
    // all: an outbox entry lives until a verdict settles it, so wiping between runs
    // means never seeing one settled — or a queued write gated by the watermark.
    if (std.c.getenv("ZB_KEEP_DB") == null) {
        _ = std.c.unlink("zbz-demo.sqlite3");
        _ = std.c.unlink("zbz-demo.sqlite3-wal");
        _ = std.c.unlink("zbz-demo.sqlite3-shm");
    }

    const c = try client.SyncClient.init(a, .{
        .url = "nats://127.0.0.1:4222",
        .creds_path = "../scripts/native/creds/omar.creds",
        .grammar_path = "../grammar.json",
        .db_path = "zbz-demo.sqlite3",
        .principal = "omar",
        .tables = &.{ "users", "salaries", "test_types" }, // parents first
    });
    defer c.deinit();

    try c.resolveTenant();
    try c.syncSchemas();

    // ZB_SKIP_SEED: exercise the WRITE path alone. The write path needs schemas
    // (for pk/columns) but not a seeded replica, and separating them means a chain
    // that will not decode — stale objects in a long-lived dev store, say — does not
    // block testing writes.
    if (std.c.getenv("ZB_SKIP_SEED") == null) {
        try c.gapAndSeed();
        try c.drainCdc();
        std.debug.print("=> users={d} salaries={d}\n", .{ c.count("users"), c.count("salaries") });
    }

    // ⚠️ Referenced unconditionally, not behind `if`: Zig analyses lazily, so a
    // `pub fn` nobody calls is never type-checked. The write path is compiled
    // because THIS is here — the same lesson as the Linux-only branches that a
    // macOS build switched out at comptime and never saw (NOTES §10ai).
    // std.posix.getenv is gone in 0.16; libc is linked, so std.c.getenv is the one that exists.
    const write = std.c.getenv("ZB_WRITE") != null;
    try c.ensureOutbox();
    // Before the first publish, or the verdict for it is gone before we listen.
    try c.subscribeVerdicts();
    std.debug.print("   gc watermark: {s}\n", .{c.gcWatermark(a) orelse "(none)"});

    if (write) {
        // A whole row: an upsert that turns into an INSERT must satisfy every NOT NULL
        // column, so a partial payload succeeds on the update path and fails on the
        // insert path — the asymmetry PROTOCOL §7 forbids.
        // `test_types`, not `salaries`: salaries has a FOREIGN KEY to users, and the
        // optimistic apply enforces it LOCALLY — with ZB_SKIP_SEED the parent is not
        // in this replica, so the insert fails before anything reaches the wire. That
        // is the applier being right, not a bug.
        const uid = if (std.c.getenv("ZB_WRITE_UID")) |u| std.mem.span(u) else "00000000-0000-0000-0000-0000000000aa";
        const now = try nowIso(a);

        var key: std.json.ObjectMap = .empty;
        try key.put(a, "uid", .{ .string = uid });

        var vals: std.json.ObjectMap = .empty;
        try vals.put(a, "uid", .{ .string = uid });
        try vals.put(a, "tenant_id", .{ .string = "kilo" });
        try vals.put(a, "some_text", .{ .string = "written by the zig client" });
        try vals.put(a, "age", .{ .integer = 42 });
        try vals.put(a, "inserted_at", .{ .string = now });
        try vals.put(a, "updated_at", .{ .string = now });

        const msg_id = try c.mutate("test_types", "INSERT", .{ .object = key }, .{ .object = vals });
        std.debug.print("   mutate queued: {s}\n", .{msg_id});
    }

    std.debug.print("   flushed: {d}, verdicts settled: {d}\n",
        .{ try c.flushOutbox(), try c.drainVerdicts(4000) });
}
