//! The mutation durable's `max_ack_pending`, set explicitly (NOTES "Ordered execution per client",
//! Affinity). The server default of 1,000 capped ingress below the lanes' queues; 2,048 is the
//! value the dispatcher and the receipt gates were validated at. A durable created before this
//! carries 1,000, and is raised in place before the first fetch: never deleted and created again,
//! which would lose its delivery state (what was delivered, what is still unacked).
const std = @import("std");
const nats = @import("nats");
const config = @import("config.zig");

const log = std.log.scoped(.mutation_listener);

pub const Action = enum { create, keep, update, mismatch };

/// What to do with a durable whose `max_ack_pending` is `current` (null: no durable yet).
/// Any value other than the old default or ours is an operator's choice: not overwritten.
pub fn decide(current: ?i64) Action {
    const v = current orelse return .create;
    if (v == config.Nats.mutation_max_ack_pending) return .keep;
    if (v == config.Nats.mutation_max_ack_pending_old_default) return .update;
    return .mismatch;
}

const timeout: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(5000), .clock = .awake } };

/// Before the first fetch. Absent: nothing to do (the subscribe creates it with the limit). At the
/// old default: the server's own config with only `max_ack_pending` changed, sent as an update
/// (refused if the durable has gone, so it never creates one), then read back. Any error stops the
/// boot: a durable left at 1,000 would run a configuration that was never validated.
pub fn ensure(js: nats.JetStream, a: std.mem.Allocator, stream: []const u8, durable: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ar = arena.allocator();
    const cfg = try readConfig(js, ar, stream, durable) orelse return;
    const current: ?i64 = switch (cfg.object.get("max_ack_pending") orelse .null) {
        .integer => |n| n,
        else => {
            log.err("🔴 Mutation dispatcher: durable '{s}' reports no max_ack_pending — refusing to guess; inspect it with `nats consumer info {s} {s}`", .{ durable, stream, durable });
            return error.AckLimitUnreadable;
        },
    };
    switch (decide(current)) {
        .create => unreachable,
        .keep => return,
        .mismatch => {
            log.err("🔴 Mutation dispatcher: durable '{s}' has max_ack_pending {d}, not the old default {d} nor {d} — left as it is; set it to {d} (`nats consumer edit {s} {s} --max-pending {d}`) to start", .{ durable, current.?, config.Nats.mutation_max_ack_pending_old_default, config.Nats.mutation_max_ack_pending, config.Nats.mutation_max_ack_pending, stream, durable, config.Nats.mutation_max_ack_pending });
            return error.AckLimitMismatch;
        },
        .update => {},
    }
    log.info("Mutation dispatcher: durable '{s}' has max_ack_pending {d} (the old default) — raising it to {d} in place; its delivery state is kept", .{ durable, current.?, config.Nats.mutation_max_ack_pending });
    var edited = cfg;
    try edited.object.put(ar, "max_ack_pending", .{ .integer = config.Nats.mutation_max_ack_pending });
    var body: std.Io.Writer.Allocating = .init(ar);
    try std.json.Stringify.value(.{ .stream_name = stream, .config = edited, .action = "update" }, .{}, &body.writer);
    const filter: ?[]const u8 = switch (cfg.object.get("filter_subject") orelse .null) {
        .string => |s| s,
        else => null,
    };
    const subject = if (filter) |f|
        try js.apiSubject(ar, "CONSUMER.CREATE.{s}.{s}.{s}", .{ stream, durable, f })
    else
        try js.apiSubject(ar, "CONSUMER.CREATE.{s}.{s}", .{ stream, durable });
    const reply = js.nc.request(subject, body.written(), timeout) catch |err| {
        // A publish the account may not make is answered by nobody: a Timeout here.
        log.err("🔴 Mutation dispatcher: could not raise max_ack_pending of durable '{s}' ({s}) — is $JS.API.CONSUMER.CREATE.{s}.> granted to the bridge? Startup stops; the durable is unchanged", .{ durable, @errorName(err), stream });
        return error.AckLimitUpdateRefused;
    };
    defer reply.deinit();
    if (apiError(ar, reply.data)) |e| {
        log.err("🔴 Mutation dispatcher: the server refused to raise max_ack_pending of durable '{s}': {s} (code {d}) — startup stops; the durable is unchanged", .{ durable, e.description, e.err_code });
        return error.AckLimitUpdateRefused;
    }
    const after = try readConfig(js, ar, stream, durable) orelse return error.AckLimitNotApplied;
    const now: ?i64 = switch (after.object.get("max_ack_pending") orelse .null) {
        .integer => |n| n,
        else => null,
    };
    if (now != config.Nats.mutation_max_ack_pending) {
        log.err("🔴 Mutation dispatcher: durable '{s}' still reports max_ack_pending {?d} after the update — startup stops", .{ durable, now });
        return error.AckLimitNotApplied;
    }
    log.info("Mutation dispatcher: durable '{s}' now has max_ack_pending {d}", .{ durable, config.Nats.mutation_max_ack_pending });
}

const ApiError = struct { err_code: i64, description: []const u8 };

fn apiError(a: std.mem.Allocator, data: []const u8) ?ApiError {
    const v = std.json.parseFromSliceLeaky(std.json.Value, a, data, .{}) catch return .{ .err_code = 0, .description = "an unreadable answer" };
    const e = (if (v == .object) v.object.get("error") else null) orelse return null;
    if (e != .object) return .{ .err_code = 0, .description = "an unreadable error" };
    const code: i64 = switch (e.object.get("err_code") orelse .null) {
        .integer => |n| n,
        else => 0,
    };
    const desc: []const u8 = switch (e.object.get("description") orelse .null) {
        .string => |s| s,
        else => "no description",
    };
    return .{ .err_code = code, .description = desc };
}

/// The durable's config exactly as the server reports it (every field, known to nats.zig or
/// not), or null when it does not exist.
fn readConfig(js: nats.JetStream, a: std.mem.Allocator, stream: []const u8, durable: []const u8) !?std.json.Value {
    const subject = try js.apiSubject(a, "CONSUMER.INFO.{s}.{s}", .{ stream, durable });
    const reply = try js.nc.request(subject, "", timeout);
    defer reply.deinit();
    if (apiError(a, reply.data)) |e| {
        if (e.err_code == 10014) return null; // consumer not found
        log.err("🔴 Mutation dispatcher: cannot read durable '{s}': {s} (code {d})", .{ durable, e.description, e.err_code });
        return error.ConsumerInfoUnreadable;
    }
    const v = try std.json.parseFromSliceLeaky(std.json.Value, a, try a.dupe(u8, reply.data), .{});
    const cfg = (if (v == .object) v.object.get("config") else null) orelse return error.ConsumerInfoUnreadable;
    if (cfg != .object) return error.ConsumerInfoUnreadable;
    return cfg;
}

test "decide: the old default is raised in place, ours kept, an operator's value left alone" {
    try std.testing.expectEqual(Action.create, decide(null));
    try std.testing.expectEqual(Action.update, decide(1000));
    try std.testing.expectEqual(Action.keep, decide(2048));
    try std.testing.expectEqual(Action.mismatch, decide(4096));
    try std.testing.expectEqual(Action.mismatch, decide(-1)); // unlimited
    try std.testing.expectEqual(Action.mismatch, decide(999));
}
