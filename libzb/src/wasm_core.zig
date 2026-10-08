//! libzb's core for the browser (§10lx): the pure rules compiled to wasm32, no I/O, no
//! threads. zb-client-ts calls them and keeps the browser's own idioms around them — the
//! NATS WebSocket, SQLite on OPFS, a single thread — so a rule is written once, here.
//!
//! The memory protocol, per call:
//!   1. JS asks for room: `ptr = zb_alloc(len)`, and writes its arguments (UTF-8 JSON) at
//!      `ptr` through a view of `memory.buffer`;
//!   2. JS calls the rule with (ptr, len); it returns where its JSON result is, packed
//!      in one u64 — the offset in the high 32 bits, the length in the low 32 (0 = failed);
//!   3. JS reads the result through a FRESH view (a call may have grown the memory, which
//!      detaches every earlier view), then calls `zb_reset()`.
//! Everything a call allocates comes from one arena over `std.heap.wasm_allocator`, so
//! `zb_reset` frees it at once and keeps the pages: nothing to free piecemeal, and the
//! memory stops growing once the largest call has been seen.
//!
//! Integers that need no allocation cross as plain u64 (JavaScript sees BigInt).
const std = @import("std");
const core = @import("core.zig");
const Value = std.json.Value;

var arena = std.heap.ArenaAllocator.init(std.heap.wasm_allocator);

export fn zb_alloc(len: usize) ?[*]u8 {
    const buf = arena.allocator().alloc(u8, len) catch return null;
    return buf.ptr;
}

export fn zb_reset() void {
    _ = arena.reset(.retain_capacity);
}

fn located(bytes: []const u8) u64 {
    return (@as(u64, @intFromPtr(bytes.ptr)) << 32) | @as(u64, bytes.len);
}

fn parseArgs(ptr: [*]const u8, len: usize) ?Value {
    return std.json.parseFromSliceLeaky(Value, arena.allocator(), ptr[0..len], .{}) catch null;
}

fn result(v: Value) u64 {
    const text = std.json.Stringify.valueAlloc(arena.allocator(), v, .{}) catch return 0;
    return located(text);
}

export fn zb_caught_up_position(pos: u64, first_seq: u64, last_seq: u64, num_pending: u64, num_ack_pending: u64, delivered_count: u64, delivered: u64) u64 {
    return core.caughtUpPosition(pos, first_seq, last_seq, num_pending, num_ack_pending, delivered_count, delivered);
}

/// core.mergeRegisters (COOPERATIVE_EDITING.md): `{"a": doc, "b": doc}` → the merged doc.
export fn zb_merge_registers(ptr: [*]const u8, len: usize) u64 {
    const args = parseArgs(ptr, len) orelse return 0;
    if (args != .object) return 0;
    const out = core.mergeRegisters(arena.allocator(), args.object.get("a") orelse .null, args.object.get("b") orelse .null) catch return 0;
    return result(out);
}

/// core.streamResume (§10lw): `{"stored", "firstSeq", "cuts": [n|null]}` → `{"to", "blocked"}`.
export fn zb_stream_resume(ptr: [*]const u8, len: usize) u64 {
    const args = parseArgs(ptr, len) orelse return 0;
    const out = core.streamResumeJson(arena.allocator(), args) catch return 0;
    return result(out);
}

/// core.scopeSeeding: `{"streams": {...}, "tables": {...}}` → `{"gapped": [...], "tablesToSeed": [...]}`.
export fn zb_scope_seeding(ptr: [*]const u8, len: usize) u64 {
    const args = parseArgs(ptr, len) orelse return 0;
    if (args != .object) return 0;
    const streams = args.object.get("streams") orelse return 0;
    const tables = args.object.get("tables") orelse return 0;
    const out = core.scopeSeeding(arena.allocator(), streams, tables) catch return 0;
    return result(out);
}
