//! zb-demo: the orchestration loop against the live stack — consumer #4.
//!
//!   cd libzb && zig build && ./zig-out/bin/zb-demo
//!
//! Connects as omar (client creds), resolves the tenant, builds users +
//! salaries from their schemas, seeds from the generation chains (parents
//! first, FK ON), drains CDC to the tail through the seed gate, and prints
//! counts to compare against Postgres.

const std = @import("std");
const client = @import("client.zig");

pub fn main() !void {
    const a = std.heap.c_allocator;

    // Fresh replica every run (the browser dev convention): the point of the
    // demo is the full seed + catch-up path.
    _ = std.c.unlink("zbz-demo.sqlite3");
    _ = std.c.unlink("zbz-demo.sqlite3-wal");
    _ = std.c.unlink("zbz-demo.sqlite3-shm");

    const c = try client.SyncClient.init(a, .{
        .url = "nats://127.0.0.1:4222",
        .creds_path = "../scripts/native/creds/omar.creds",
        .grammar_path = "../grammar.json",
        .db_path = "zbz-demo.sqlite3",
        .principal = "omar",
        .tables = &.{ "users", "salaries" }, // parents first
    });
    defer c.deinit();

    try c.resolveTenant();
    try c.syncSchemas();
    try c.gapAndSeed();
    try c.drainCdc();

    std.debug.print("=> users={d} salaries={d}\n", .{ c.count("users"), c.count("salaries") });
}
