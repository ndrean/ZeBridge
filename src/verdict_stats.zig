//! Write verdicts, counted as the mutation listener publishes them (NOTES §10ji). Every
//! verdict goes through `publishVerdictExtra`, whichever lane sent it, so one counter per
//! status covers all of them. Process-wide atomics: the listener lanes increment, the HTTP
//! thread reads; no lock, no pointer threaded through the listener's constructor.
const std = @import("std");

pub const Status = enum { accepted, stale, row_deleted, rejected, failed, other };

var counts = [_]std.atomic.Value(u64){std.atomic.Value(u64).init(0)} ** @typeInfo(Status).@"enum".fields.len;
/// `failed` because the ingress rate limit refused it (MUTATION_RATE_PER_PRINCIPAL) —
/// counted inside `failed` too, and here on its own so a limit is told from a fault.
var rate_limited = std.atomic.Value(u64).init(0);

pub fn record(status: []const u8, reason: []const u8) void {
    const s = std.meta.stringToEnum(Status, status) orelse .other;
    _ = counts[@intFromEnum(s)].fetchAdd(1, .monotonic);
    if (std.mem.eql(u8, reason, "rate_limited")) _ = rate_limited.fetchAdd(1, .monotonic);
}

pub fn writePrometheus(w: *std.Io.Writer) !void {
    try w.print("# HELP bridge_mutation_verdicts_total Write verdicts published to clients, by status (accepted, stale, row_deleted, rejected, failed)\n# TYPE bridge_mutation_verdicts_total counter\n", .{});
    inline for (@typeInfo(Status).@"enum".fields) |f| {
        try w.print("bridge_mutation_verdicts_total{{status=\"" ++ f.name ++ "\"}} {d}\n", .{counts[f.value].load(.monotonic)});
    }
    try w.print("# HELP bridge_mutation_rate_limited_total Writes refused by the ingress rate limit (also counted as failed)\n# TYPE bridge_mutation_rate_limited_total counter\nbridge_mutation_rate_limited_total {d}\n", .{rate_limited.load(.monotonic)});
}
