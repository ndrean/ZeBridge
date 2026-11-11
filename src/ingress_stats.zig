//! What the ingress dispatcher and its lanes are doing, as process-wide atomics the HTTP
//! thread reads for `/metrics` (the pattern of `verdict_stats.zig`). These are the numbers
//! that tell a routing problem from a slow database: how much is waiting in the
//! dispatcher's pending buffers, how deep each lane's queue got, how often an ack deadline
//! had to be renewed, and how many writes parked (`Dispatcher` in mutation_listener.zig).
const std = @import("std");
const config = @import("config.zig");

const lanes_max = config.Nats.max_ingress_lanes;

pub var fetched = std.atomic.Value(u64).init(0);
pub var parked = std.atomic.Value(u64).init(0);
pub var renewals = std.atomic.Value(u64).init(0);
pub var budget_waits = std.atomic.Value(u64).init(0);
pub var fetches = std.atomic.Value(u64).init(0);
pub var fetches_held = std.atomic.Value(u64).init(0);
pub var dropped_at_stop = std.atomic.Value(u64).init(0);
pub var pending = std.atomic.Value(u64).init(0);
pub var pending_high = std.atomic.Value(u64).init(0);
pub var routed_ordered = std.atomic.Value(u64).init(0);
pub var routed_spread = std.atomic.Value(u64).init(0);
pub var classify_ns = std.atomic.Value(u64).init(0);
/// Verdicts of released parked writes left to their park credit, by where the release ran:
/// the sweeper's replay, or a lane's ordinary batch (a redelivered parked write).
pub var credit_verdicts_sweep = std.atomic.Value(u64).init(0);
pub var credit_verdicts_lane = std.atomic.Value(u64).init(0);
var queued: [lanes_max]std.atomic.Value(u64) = @splat(.init(0));
var queued_high: [lanes_max]std.atomic.Value(u64) = @splat(.init(0));
var lane_writes: [lanes_max]std.atomic.Value(u64) = @splat(.init(0));
var lane_batches: [lanes_max]std.atomic.Value(u64) = @splat(.init(0));
var lane_wait_ms: [lanes_max]std.atomic.Value(u64) = @splat(.init(0));
var lane_busy_ms: [lanes_max]std.atomic.Value(u64) = @splat(.init(0));

/// One pull's routing: kept on a client's lane, spread, and the wall time (not CPU) the
/// dispatcher spent deciding, envelope parse included.
pub fn noteRouted(ordered: usize, spread: usize, ns: u64) void {
    _ = routed_ordered.fetchAdd(ordered, .monotonic);
    _ = routed_spread.fetchAdd(spread, .monotonic);
    _ = classify_ns.fetchAdd(ns, .monotonic);
}

/// A lane took `n` writes as one batch, which had waited `waited_ms` in all in its queue.
pub fn noteBatch(lane: usize, n: usize, waited_ms: u64) void {
    if (lane >= lanes_max) return;
    _ = lane_writes[lane].fetchAdd(n, .monotonic);
    _ = lane_batches[lane].fetchAdd(1, .monotonic);
    _ = lane_wait_ms[lane].fetchAdd(waited_ms, .monotonic);
}

/// A lane spent `ms` applying one batch.
pub fn noteBusy(lane: usize, ms: u64) void {
    if (lane >= lanes_max) return;
    _ = lane_busy_ms[lane].fetchAdd(ms, .monotonic);
}

pub fn notePending(n: usize) void {
    pending.store(n, .monotonic);
    _ = pending_high.fetchMax(n, .monotonic);
}

pub fn noteQueued(lane: usize, depth: usize) void {
    if (lane >= lanes_max) return;
    queued[lane].store(depth, .monotonic);
    _ = queued_high[lane].fetchMax(depth, .monotonic);
}

pub fn writePrometheus(w: *std.Io.Writer) !void {
    try w.print(
        \\# HELP bridge_ingress_fetched_total Writes the dispatcher pulled from the mutation stream
        \\# TYPE bridge_ingress_fetched_total counter
        \\bridge_ingress_fetched_total {d}
        \\# HELP bridge_ingress_parked_total Ordered writes parked because their predecessor had no receipt yet
        \\# TYPE bridge_ingress_parked_total counter
        \\bridge_ingress_parked_total {d}
        \\# HELP bridge_ingress_ack_renewals_total Ack deadlines extended for writes waiting in a queue or executing
        \\# TYPE bridge_ingress_ack_renewals_total counter
        \\bridge_ingress_ack_renewals_total {d}
        \\# HELP bridge_ingress_budget_waits_total Times the dispatcher stopped pulling because its pending buffers were full
        \\# TYPE bridge_ingress_budget_waits_total counter
        \\bridge_ingress_budget_waits_total {d}
        \\# HELP bridge_ingress_fetches_total Fetches the dispatcher made (each waits at most its timeout)
        \\# TYPE bridge_ingress_fetches_total counter
        \\bridge_ingress_fetches_total {d}
        \\# HELP bridge_ingress_fetches_held_total Of those, fetches started while fetched writes waited for a lane (each bounded by the held-work timeout)
        \\# TYPE bridge_ingress_fetches_held_total counter
        \\bridge_ingress_fetches_held_total {d}
        \\# HELP bridge_ingress_dropped_at_stop_total Fetched writes left unacked at a clean stop, redelivered at the next boot
        \\# TYPE bridge_ingress_dropped_at_stop_total counter
        \\bridge_ingress_dropped_at_stop_total {d}
        \\# HELP bridge_ingress_pending Writes fetched and routed but not yet in a lane's queue
        \\# TYPE bridge_ingress_pending gauge
        \\bridge_ingress_pending {d}
        \\# HELP bridge_ingress_pending_high_water The most writes ever pending at once since boot
        \\# TYPE bridge_ingress_pending_high_water gauge
        \\bridge_ingress_pending_high_water {d}
        \\
    , .{ fetched.load(.monotonic), parked.load(.monotonic), renewals.load(.monotonic), budget_waits.load(.monotonic), fetches.load(.monotonic), fetches_held.load(.monotonic), dropped_at_stop.load(.monotonic), pending.load(.monotonic), pending_high.load(.monotonic) });
    try w.print("# HELP bridge_ingress_queued Writes waiting in a lane's queue\n# TYPE bridge_ingress_queued gauge\n", .{});
    for (&queued, 0..) |*q, i| try w.print("bridge_ingress_queued{{lane=\"{d}\"}} {d}\n", .{ i, q.load(.monotonic) });
    try w.print("# HELP bridge_ingress_queued_high_water The deepest a lane's queue got since boot\n# TYPE bridge_ingress_queued_high_water gauge\n", .{});
    for (&queued_high, 0..) |*q, i| try w.print("bridge_ingress_queued_high_water{{lane=\"{d}\"}} {d}\n", .{ i, q.load(.monotonic) });
    try w.print(
        \\# HELP bridge_ingress_routed_ordered_total Writes kept on their client's lane (ordered, or a malformed ordering envelope)
        \\# TYPE bridge_ingress_routed_ordered_total counter
        \\bridge_ingress_routed_ordered_total {d}
        \\# HELP bridge_ingress_routed_spread_total Unordered writes handed to the lane with the most room
        \\# TYPE bridge_ingress_routed_spread_total counter
        \\bridge_ingress_routed_spread_total {d}
        \\# HELP bridge_ingress_classify_seconds_total Dispatcher wall time spent routing (envelope parse included), measured on the monotonic clock: not CPU time
        \\# TYPE bridge_ingress_classify_seconds_total counter
        \\bridge_ingress_classify_seconds_total {d:.6}
        \\# HELP bridge_ingress_credit_verdicts_total Verdicts of released parked writes sent by their park credit instead of the lane, by where the release ran
        \\# TYPE bridge_ingress_credit_verdicts_total counter
        \\bridge_ingress_credit_verdicts_total{{path="sweep"}} {d}
        \\bridge_ingress_credit_verdicts_total{{path="lane"}} {d}
        \\
    , .{ routed_ordered.load(.monotonic), routed_spread.load(.monotonic), @as(f64, @floatFromInt(classify_ns.load(.monotonic))) / std.time.ns_per_s, credit_verdicts_sweep.load(.monotonic), credit_verdicts_lane.load(.monotonic) });
    const per_lane = [_]struct { name: []const u8, help: []const u8, v: *[lanes_max]std.atomic.Value(u64) }{
        .{ .name = "lane_writes_total", .help = "Writes a lane took from its queue", .v = &lane_writes },
        .{ .name = "lane_batches_total", .help = "Batches a lane took from its queue", .v = &lane_batches },
        .{ .name = "lane_queue_wait_ms_total", .help = "Milliseconds the writes a lane took had waited in its queue, summed", .v = &lane_wait_ms },
        .{ .name = "lane_busy_ms_total", .help = "Milliseconds a lane spent applying batches", .v = &lane_busy_ms },
    };
    for (per_lane) |m| {
        try w.print("# HELP bridge_ingress_{s} {s}\n# TYPE bridge_ingress_{s} counter\n", .{ m.name, m.help, m.name });
        for (m.v, 0..) |*x, i| try w.print("bridge_ingress_{s}{{lane=\"{d}\"}} {d}\n", .{ m.name, i, x.load(.monotonic) });
    }
}

test "high-water marks only rise" {
    noteQueued(2, 5);
    noteQueued(2, 1);
    notePending(7);
    notePending(0);
    try std.testing.expectEqual(@as(u64, 1), queued[2].load(.monotonic));
    try std.testing.expectEqual(@as(u64, 5), queued_high[2].load(.monotonic));
    try std.testing.expectEqual(@as(u64, 7), pending_high.load(.monotonic));
    noteBatch(3, 4, 10);
    noteBusy(3, 7);
    var buf: [16384]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writePrometheus(&w);
    try std.testing.expect(std.mem.indexOf(u8, w.buffered(), "bridge_ingress_queued_high_water{lane=\"2\"} 5\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, w.buffered(), "bridge_ingress_lane_writes_total{lane=\"3\"} 4\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, w.buffered(), "bridge_ingress_lane_busy_ms_total{lane=\"3\"} 7\n") != null);
}
