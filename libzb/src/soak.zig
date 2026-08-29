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
    const verdict_ms = env("ZB_SOAK_VERDICT_MS", 800);

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
    var updated: u64 = 0;
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

        // A WIDE row on purpose, and the distinction is worth being exact about.
        // `test_types` exists to carry the awkward types and IS mutated from the
        // browser — but only its scalars: web-consumer sends
        // `uid, some_text, age, is_true, tenant_id, updated_at, inserted_at`. The
        // exotic columns were exercised INBOUND only (PG -> bridge -> client, which
        // decode_integrity.py covers). Nothing had ever sent `text[]`, `int[][]`,
        // `jsonb`, `numeric` or `double precision` OUTBOUND, which is why the first
        // wide row here was rejected by an ingress nobody had pushed on.
        //
        // ⚠️ Arrays and jsonb go as their POSTGRESQL TEXT form, not as msgpack arrays
        // or maps. The ingress accepts scalars only — `payloadToString` handles
        // nil/bool/int/uint/float/str and answers `UnsupportedPayloadType` for
        // anything else — because a mutation is applied as a parameterised statement
        // and PostgreSQL parses these literals itself. Sending real arrays got every
        // INSERT rejected while the soak still counted a verdict for each, which is
        // exactly the failure a one-cycle smoke test is for.
        // NATIVE values now — a real array and a real map, no hand-built literals.
        // The ingress renders each one the way its column's input function reads it
        // (mutation_listener.zig ColKind), so the client sends what it has.
        var tags = std.json.Array.init(aa);
        try tags.append(.{ .string = "soak" });
        try tags.append(.{ .string = try std.fmt.allocPrint(aa, "cycle-{d},with{{braces}}", .{cycle}) });

        var inner = std.json.Array.init(aa);
        try inner.append(.{ .integer = @intCast(cycle) });
        try inner.append(.{ .integer = @intCast(cycle * 2) });
        var matrix = std.json.Array.init(aa);
        try matrix.append(.{ .array = inner });

        var meta: std.json.ObjectMap = .empty;
        try meta.put(aa, "cycle", .{ .integer = @intCast(cycle) });
        try meta.put(aa, "source", .{ .string = "zb-soak" });

        var vals: std.json.ObjectMap = .empty;
        try vals.put(aa, "uid", .{ .string = uid });
        try vals.put(aa, "tenant_id", .{ .string = "kilo" });
        try vals.put(aa, "some_text", .{ .string = "soak" });
        try vals.put(aa, "age", .{ .integer = @intCast(cycle % 120) });
        try vals.put(aa, "temperature", .{ .float = 36.6 });
        try vals.put(aa, "price", .{ .string = "1234.56789012" }); // numeric(20,8) as text
        try vals.put(aa, "is_true", .{ .bool = cycle % 2 == 0 });
        try vals.put(aa, "tags", .{ .array = tags });
        try vals.put(aa, "matrix", .{ .array = matrix });
        try vals.put(aa, "metadata", .{ .object = meta });
        try vals.put(aa, "inserted_at", .{ .string = now });
        try vals.put(aa, "updated_at", .{ .string = now });

        _ = c.mutate(aa, table, "INSERT", .{ .object = key }, .{ .object = vals }) catch |err| {
            std.debug.print("cycle {d}: INSERT failed: {any}\n", .{ cycle, err });
            continue;
        };
        inserted += 1;

        // UPDATE between the two: a different plan from INSERT (a partial payload on
        // an existing row) and a second version stamp, so LWW has something to order.
        // The version must move or the bridge is right to call the write stale.
        // ⚠️ A FULL row, not just the changed fields. The local apply is an upsert, so
        // SQLite evaluates the INSERT arm and a payload missing a NOT NULL column
        // fails before the conflict is resolved — PROTOCOL §7's asymmetry, which is
        // why full rows are the rule. Measured: a four-field UPDATE failed with a bare
        // `StepFailed` and never reached the wire at all.
        const later = try nowIso(aa);
        var upd: std.json.ObjectMap = .empty;
        var vit = vals.iterator();
        while (vit.next()) |e| try upd.put(aa, e.key_ptr.*, e.value_ptr.*);
        try upd.put(aa, "some_text", .{ .string = "soak-updated" });
        try upd.put(aa, "age", .{ .integer = @intCast((cycle % 120) + 1) });
        try upd.put(aa, "updated_at", .{ .string = later });
        _ = c.mutate(aa, table, "UPDATE", .{ .object = key }, .{ .object = upd }) catch |err| {
            std.debug.print("cycle {d}: UPDATE failed: {any}\n", .{ cycle, err });
        };
        updated += 1;

        // The DELETE is a separate mutation, as a client would send it — the tombstone
        // is written by the guard in PostgreSQL, not by us.
        _ = c.mutate(aa, table, "DELETE", .{ .object = key }, null) catch |err| {
            std.debug.print("cycle {d}: DELETE failed: {any}\n", .{ cycle, err });
            continue;
        };
        deleted += 1;

        const line = try std.fmt.allocPrint(aa, "{s}\n", .{uid});
        _ = std.c.write(ledger, line.ptr, line.len);

        // ⚠️ The budget only applies when NOTHING has arrived yet, but at a 1s cycle
        // even that dominates the loop — a 3s wait per cycle makes the interval a lie.
        // Locally a verdict lands in ~100ms; ZB_SOAK_VERDICT_MS raises it if a slower
        // deployment needs to.
        settled += c.drainVerdicts(verdict_ms) catch 0;
        if (cycle % 10 == 0 or cycle == 1) {
            std.debug.print("  cycle {d}: ins={d} upd={d} del={d} verdicts={d} watermark={s}\n",
                .{ cycle, inserted, updated, deleted, settled, c.gcWatermark(aa) orelse "(none)" });
        }

        // ⚠️ No std.c.sleep and no std.time.sleep in 0.16 — nanosleep is the one
        // that exists through libc.
        var nap: std.c.timespec = .{ .sec = @intCast(interval), .nsec = 0 };
        _ = std.c.nanosleep(&nap, null);
    }

    // A final pass: verdicts for the last cycle, and whatever the outbox still holds.
    settled += c.drainVerdicts(5000) catch 0; // a generous last pass
    var fa = std.heap.ArenaAllocator.init(a);
    defer fa.deinit();
    std.debug.print("zb-soak done: cycles={d} ins={d} upd={d} del={d} verdicts={d} watermark={s}\n",
        .{ cycle, inserted, updated, deleted, settled, c.gcWatermark(fa.allocator()) orelse "(none)" });
}
