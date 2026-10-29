//! Hub-side admission, preserving the public mutation subject and JetStream PubAck.
//! Only this service publishes the private, one-subject-per-principal queue. The
//! storage server replies directly to the client's original inbox: no synthetic ack.
const std = @import("std");
const builtin = @import("builtin");
const nats = @import("nats");
const Conf = @import("config.zig");
const topology = @import("topology.zig");
const leaf_scope = @import("leaf_scope.zig");
const publisher_mod = @import("nats_publisher.zig");

const log = std.log.scoped(.mutation_admission);
pub const original_subject_header = "ZB-Original-Subject";
pub const original_id_header = "ZB-Client-Msg-Id";
pub const leaf_tenants_header = "ZB-Leaf-Tenants";
var stopping: std.atomic.Value(bool) = .init(false);
const SignalHandler = @typeInfo(@TypeOf(@as(std.posix.Sigaction, undefined).handler.handler)).optional.child;
const Signal = @typeInfo(@typeInfo(SignalHandler).pointer.child).@"fn".param_types[0].?;

fn stop(_: Signal) callconv(.c) void {
    stopping.store(true, .release);
}

pub fn enabled(init: *const std.process.Init) bool {
    const value = init.minimal.environ.getPosix("MUTATION_ADMISSION") orelse return false;
    return std.mem.eql(u8, value, "1");
}

pub fn privatePrefix(a: std.mem.Allocator, prefix: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "$ZB.{s}", .{prefix});
}

pub const Limits = struct {
    stream_bytes: i64 = 1 << 30,
    principal_bytes: i64 = 64 << 20,
    message_bytes: i32 = 64 << 10,
    principal_messages: i64 = 5000,

    pub fn count(self: Limits) !i64 {
        // Include a conservative per-record allowance for subject and store metadata.
        if (self.message_bytes < 4096 or self.message_bytes > 1 << 20 or
            self.principal_messages < 1 or self.principal_bytes < self.message_bytes + 512 or
            self.stream_bytes < 1 or self.principal_bytes > @divFloor(self.stream_bytes, 2)) return error.InvalidAdmissionLimits;
        return @min(self.principal_messages, @divFloor(self.principal_bytes, @as(i64, self.message_bytes) + 512));
    }

    pub fn fromEnv(init: *const std.process.Init) !Limits {
        var limits: Limits = .{};
        inline for (.{
            .{ "stream_bytes", "MUTATION_STREAM_MAX_BYTES" },
            .{ "principal_bytes", "MUTATION_BACKLOG_BYTES_PER_PRINCIPAL" },
            .{ "message_bytes", "MUTATION_MAX_MESSAGE_BYTES" },
            .{ "principal_messages", "MUTATION_BACKLOG_PER_PRINCIPAL" },
        }) |setting| {
            if (init.minimal.environ.getPosix(setting[1])) |value| {
                @field(limits, setting[0]) = try std.fmt.parseInt(@TypeOf(@field(limits, setting[0])), value, 10);
            }
        }
        _ = try limits.count();
        return limits;
    }
};

pub const Route = struct { principal: []const u8, table: []const u8, operation: []const u8 };

pub fn route(subject: []const u8, prefix: []const u8, leaf_prefix: []const u8) !Route {
    if (subject.len > 512) return error.MalformedSubject;
    const first_dot = std.mem.findScalar(u8, subject, '.') orelse return error.MalformedSubject;
    if (!std.mem.eql(u8, subject[0..first_dot], prefix)) return error.MalformedSubject;
    const last_dot = std.mem.findScalarLast(u8, subject, '.') orelse return error.MalformedSubject;
    if (last_dot <= first_dot) return error.MalformedSubject;
    const table_dot = std.mem.findScalarLast(u8, subject[0..last_dot], '.') orelse return error.MalformedSubject;
    if (table_dot <= first_dot) return error.MalformedSubject;
    const principal = subject[first_dot + 1 .. table_dot];
    const table = subject[table_dot + 1 .. last_dot];
    const op = subject[last_dot + 1 ..];
    if (!leaf_scope.principalIn(leaf_prefix, principal) or table.len == 0 or table.len > 128) return error.MalformedSubject;
    if (!std.mem.eql(u8, op, "insert") and !std.mem.eql(u8, op, "update") and !std.mem.eql(u8, op, "delete")) return error.UnknownOperation;
    return .{ .principal = principal, .table = table, .operation = op };
}

/// Internal headers are trusted only on the private subject, and must agree with it.
pub fn originalSubject(msg: *nats.Message, prefix: []const u8, leaf_prefix: []const u8) ![]const u8 {
    var expected: [256]u8 = undefined;
    const start = try std.fmt.bufPrint(&expected, "$ZB.{s}.", .{prefix});
    if (!std.mem.startsWith(u8, msg.subject, start)) return msg.subject;
    const original = msg.headerGet(original_subject_header) orelse return error.MalformedSubject;
    const parsed = try route(original, prefix, leaf_prefix);
    if (!std.mem.eql(u8, msg.subject[start.len..], parsed.principal)) return error.MalformedSubject;
    return original;
}

pub fn ensureStream(js: nats.JetStream, a: std.mem.Allocator, topo: *const topology.Topology, limits: Limits) !void {
    const prefix = try privatePrefix(a, topo.subject_mutations_prefix);
    defer a.free(prefix);
    const subject = try std.fmt.allocPrint(a, "{s}.>", .{prefix});
    defer a.free(subject);
    if (js.getStreamInfo(topo.stream_mutations)) |info_value| {
        var info = info_value;
        defer info.deinit();
        // Never race a public JetStream responder with the admission service. A
        // legacy queue must be drained and migrated during a reviewed cutover.
        var canonical = false;
        for (info.value.config.subjects) |item| {
            if (std.mem.eql(u8, item, subject)) canonical = true;
            if (std.mem.startsWith(u8, item, topo.subject_mutations_prefix)) return error.LegacyMutationStreamNeedsMigration;
        }
        if (!canonical or info.value.config.max_age != 0 or info.value.config.no_ack or info.value.config.subjects.len != 1 or info.value.config.max_msgs_per_subject > try limits.count() or
            info.value.config.max_msgs_per_subject < 1 or info.value.config.max_msg_size > limits.message_bytes or
            info.value.config.max_msg_size < 1 or info.value.config.max_bytes > limits.stream_bytes or
            info.value.config.max_bytes < 1 or info.value.config.discard != .new or
            info.value.config.discard_new_per_subject != true or info.value.config.retention != .workqueue)
            return error.UnsafeMutationStreamLimits;
        try ensureErrors(js, a, topo);
        return;
    } else |err| {
        if (err != error.StreamNotFound) return err;
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var created = try js.addStream(try desiredStream(arena.allocator(), topo, limits));
    created.deinit();
    try ensureErrors(js, a, topo);
}

fn desiredStream(a: std.mem.Allocator, topo: *const topology.Topology, limits: Limits) !nats.StreamConfig {
    const subjects = try a.alloc([]const u8, 1);
    subjects[0] = try std.fmt.allocPrint(a, "$ZB.{s}.>", .{topo.subject_mutations_prefix});
    return .{
        .name = topo.stream_mutations,
        .subjects = subjects,
        .retention = .workqueue,
        .max_age = 0, // Accepted writes remain until processed; capacity is bounded separately.
        .max_bytes = limits.stream_bytes,
        .max_msg_size = limits.message_bytes,
        .max_msgs_per_subject = try limits.count(),
        .discard = .new,
        .discard_new_per_subject = true,
    };
}

fn ensureErrors(js: nats.JetStream, a: std.mem.Allocator, topo: *const topology.Topology) !void {
    const name = try std.fmt.allocPrint(a, "{s}_ERRORS", .{topo.stream_mutations});
    defer a.free(name);
    if (js.getStreamInfo(name)) |info_value| {
        var info = info_value;
        defer info.deinit();
        if (info.value.config.max_bytes <= 0 or info.value.config.max_bytes > 64 << 20 or
            info.value.config.discard != .old) return error.UnsafeMutationErrorStreamLimits;
        return;
    } else |err| if (err != error.StreamNotFound) return err;
    const subject = try std.fmt.allocPrint(a, "{s}.>", .{topo.mutation_error_prefix});
    defer a.free(subject);
    var created = try js.addStream(.{
        .name = name,
        .subjects = &.{subject},
        .max_bytes = 64 << 20,
        .max_age = 2 * 3600 * std.time.ns_per_s,
        .discard = .old,
    });
    created.deinit();
}

/// A client's original message ID: one subject token, since it ends the verdict subject
/// `mutation_ack.<principal>.<id>`, of 1 to 512 bytes. An ID starting with `zb1-` must be
/// exactly `zb1-t<epoch-ms>-<rest>`: never treated as legacy, because its receipt month
/// comes from that time. Any other ID is a legacy one, accepted from deployed clients.
pub fn validMessageId(id: []const u8) bool {
    if (id.len == 0 or id.len > 512) return false;
    for (id) |ch| switch (ch) {
        0...32, 127, '.', '*', '>' => return false,
        else => {},
    };
    return !std.mem.startsWith(u8, id, "zb1-") or createdMs(id) != null;
}

/// The creation time of a `zb1-t<epoch-ms>-<rest>` ID (1 to 15 digits, a non-empty rest).
pub fn createdMs(id: []const u8) ?i64 {
    const head = "zb1-t";
    if (!std.mem.startsWith(u8, id, head)) return null;
    const dash = std.mem.findScalarPos(u8, id, head.len, '-') orelse return null;
    const digits = id[head.len..dash];
    if (digits.len == 0 or digits.len > 15 or dash + 1 >= id.len) return null;
    for (digits) |d| if (!std.ascii.isDigit(d)) return null;
    return std.fmt.parseInt(i64, digits, 10) catch null;
}

pub fn replyBelongsTo(reply: []const u8, principal: []const u8) bool {
    const prefix = "_INBOX.";
    if (!std.mem.startsWith(u8, reply, prefix)) return false;
    const rest = reply[prefix.len..];
    return rest.len > principal.len + 1 and std.mem.startsWith(u8, rest, principal) and rest[principal.len] == '.';
}

fn relay(conn: *nats.Connection, incoming: *nats.Message, prefix: []const u8, leaf_prefix: []const u8, limits: Limits) !void {
    const parsed = try route(incoming.subject, prefix, leaf_prefix);
    if (incoming.data.len > @as(usize, @intCast(limits.message_bytes - 2048))) return error.MessageTooLarge;
    if (incoming.reply) |reply| {
        if (!replyBelongsTo(reply, parsed.principal)) return error.InvalidReplyInbox;
    }
    // Every admitted write carries a valid original ID: its receipt is keyed by it, and
    // a write that cannot have one is refused here, while the publisher still waits for
    // the PubAck. No ID is ever made up on its behalf.
    const id = incoming.headerGet("Nats-Msg-Id") orelse return error.MissingMessageId;
    if (!validMessageId(id)) return error.InvalidMessageId;
    const outgoing = try conn.newMsg();
    defer outgoing.deinit();
    const subject = try std.fmt.allocPrint(outgoing.arena.allocator(), "$ZB.{s}.{s}", .{ prefix, parsed.principal });
    try outgoing.setSubject(subject, false);
    try outgoing.setPayload(incoming.data, false);
    // Rebuild headers: a public caller cannot inject an original route or leaf scope.
    try outgoing.headerSet(original_subject_header, incoming.subject);
    const scoped_id = try std.fmt.allocPrint(outgoing.arena.allocator(), "{s}:{s}", .{ parsed.principal, id });
    try outgoing.headerSet("Nats-Msg-Id", scoped_id);
    try outgoing.headerSet(original_id_header, id);
    if (incoming.reply) |reply| try conn.publishRequestMsg(outgoing, reply) else try conn.publishMsg(outgoing);
}

fn runImpl(init: *const std.process.Init, a: std.mem.Allocator) !void {
    var ep = try Conf.Nats.Endpoint.parseUrl(optionalEnv(init, "NATS_URL") orelse Conf.Nats.default_url);
    ep.creds = optionalEnv(init, "NATS_CREDS");
    ep.tls_ca = optionalEnv(init, "NATS_TLS_CA");
    ep.tls_server_name = optionalEnv(init, "NATS_TLS_SERVER_NAME");
    ep.js_domain = optionalEnv(init, "NATS_JS_DOMAIN");
    var owned = try topology.loadEmbedded(a);
    defer owned.deinit();
    const limits = try Limits.fromEnv(init);
    var prepare_projections = false;
    var check_only = false;
    var args = init.minimal.args.iterate();
    while (args.next()) |arg| if (std.mem.eql(u8, arg, "--print-config")) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const config = try desiredStream(arena.allocator(), &owned.topology, limits);
        const json = try std.json.Stringify.valueAlloc(arena.allocator(), config, .{ .emit_null_optional_fields = false });
        try std.Io.File.stdout().writeStreamingAll(init.io, json);
        return;
    } else if (std.mem.eql(u8, arg, "--prepare-projections")) {
        prepare_projections = true;
    } else if (std.mem.eql(u8, arg, "--check")) {
        check_only = true;
    };
    var publisher = try publisher_mod.Publisher.init(a, .{ .endpoint = ep }, init.io);
    defer publisher.deinit();
    try publisher.connect();
    if (prepare_projections) {
        var tenants: std.ArrayList([]const u8) = .empty;
        defer tenants.deinit(a);
        var values = std.mem.tokenizeAny(u8, optionalEnv(init, "ZB_LEAF_TENANTS") orelse return error.MissingLeafTenants, " \t\r\n");
        while (values.next()) |t| {
            if (!leaf_scope.token(t) or tenants.items.len >= 64) return error.InvalidLeafTenants;
            try tenants.append(a, t);
        }
        if (tenants.items.len == 0) return error.MissingLeafTenants;
        var topo = owned.topology;
        topo.tenants = tenants.items;
        try leaf_scope.ensureProjections(a, publisher.conn.?, &topo, ep.js_domain orelse return error.MissingHubDomain);
        return;
    }
    try ensureStream(publisher.js.?, a, &owned.topology, limits);
    if (check_only) return;
    const conn = publisher.conn.?;
    var subscriptions: std.ArrayList(*nats.Subscription) = .empty;
    defer {
        for (subscriptions.items) |sub| sub.deinit();
        subscriptions.deinit(a);
    }
    const root_subject = try std.fmt.allocPrint(a, "{s}.*.*.*", .{owned.topology.subject_mutations_prefix});
    defer a.free(root_subject);
    try subscriptions.append(a, try conn.queueSubscribeSync(root_subject, "zebridge_mutation_admission"));
    var leaves = std.mem.tokenizeAny(u8, optionalEnv(init, "ZB_LEAF_NAMES") orelse "", " \t\r\n");
    var names: std.StringHashMap(void) = .init(a);
    defer names.deinit();
    while (leaves.next()) |leaf| {
        if (!leaf_scope.leafId(owned.topology.leaf_prefix, leaf) or names.count() >= 64) return error.InvalidLeafNames;
        if ((try names.getOrPut(leaf)).found_existing) continue;
        const scoped_subject = try std.fmt.allocPrint(a, "{s}.{s}.*.*.*", .{ owned.topology.subject_mutations_prefix, leaf });
        defer a.free(scoped_subject);
        try subscriptions.append(a, try conn.queueSubscribeSync(scoped_subject, "zebridge_mutation_admission"));
    }
    for (subscriptions.items) |sub| sub.setPendingLimits(1024, 8 << 20);
    try conn.flush();
    const action = std.posix.Sigaction{ .handler = .{ .handler = stop }, .mask = std.mem.zeroes(std.posix.sigset_t), .flags = 0 };
    std.posix.sigaction(std.posix.SIG.TERM, &action, null);
    std.posix.sigaction(std.posix.SIG.INT, &action, null);
    log.info("mutation admission ready: {d} queued messages per principal, {d} byte budget", .{ try limits.count(), limits.principal_bytes });
    var refusals: Refusals = .{};
    var last_warning: i64 = 0;
    var idle_ms: u64 = 0;
    while (!stopping.load(.acquire)) {
        var activity = false;
        for (subscriptions.items) |subscription| {
            const incoming = subscription.nextMsgTimeout(.{ .duration = .{ .raw = .fromMilliseconds(0), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout, error.SlowConsumer => continue,
                else => return err,
            };
            activity = true;
            defer incoming.deinit();
            relay(conn, incoming, owned.topology.subject_mutations_prefix, owned.topology.leaf_prefix, limits) catch |err| {
                refusals.note(err);
                if (err == error.InvalidReplyInbox) if (incoming.reply) |reply| if (route(incoming.subject, owned.topology.subject_mutations_prefix, owned.topology.leaf_prefix)) |parsed| {
                    refusals.noteInbox(parsed.principal, reply);
                } else |_| {};
                if (incoming.reply) |reply| {
                    // Never emit an error to an arbitrary caller-chosen subject.
                    const parsed = route(incoming.subject, owned.topology.subject_mutations_prefix, owned.topology.leaf_prefix) catch continue;
                    if (replyBelongsTo(reply, parsed.principal)) {
                        // The reason is an error name: an identifier, safe inside the JSON.
                        var buf: [192]u8 = undefined;
                        const body = std.fmt.bufPrint(&buf, "{{\"error\":{{\"code\":400,\"err_code\":10003,\"description\":\"mutation admission refused: {s}\"}}}}", .{@errorName(err)}) catch
                            "{\"error\":{\"code\":400,\"err_code\":10003,\"description\":\"mutation admission refused\"}}";
                        conn.publish(reply, body) catch {};
                    }
                }
            };
        }
        // At most one line a second, and the tail of a burst is not left unsaid until the next refusal.
        if (refusals.pending()) {
            const now = std.Io.Clock.real.now(init.io).toSeconds();
            if (now != last_warning) {
                var line_buf: [768]u8 = undefined;
                log.warn("{s}", .{refusals.line(&line_buf)});
                last_warning = now;
            }
        }
        // Polled, not blocked on: one wait would starve the other subscriptions (the
        // public subject and one per leaf). Idle, the pause grows to idle_sleep_max_ms:
        // a fixed 1 ms pause woke the loop 1,000 times a second, 1.4% of a core all day
        // on a hub with no writes (2026-10-24). The first write after a quiet spell waits
        // at most that long; while writes keep coming, the loop never sleeps.
        idle_ms = nextIdleSleep(idle_ms, activity);
        if (idle_ms > 0) try std.Io.sleep(init.io, .fromMilliseconds(@intCast(idle_ms)), .awake);
    }
}

/// Refusals by reason, for the operator. Bounded by construction: a fixed array indexed by a closed set of reasons, plus the
/// last offender in two fixed buffers. Nothing here is keyed by anything a caller chose, so a flood of refusals costs the
/// same memory as one.
const Refusals = struct {
    const Reason = enum { invalid_reply_inbox, missing_message_id, invalid_message_id, message_too_large, bad_route, other };
    const names = [_][]const u8{ "InvalidReplyInbox", "MissingMessageId", "InvalidMessageId", "MessageTooLarge", "BadRoute", "Other" };
    since_start: [names.len]u64 = @splat(0),
    window: [names.len]u64 = @splat(0),
    /// The last InvalidReplyInbox: who wrote, and the inbox PREFIX it asked to be answered on (the first two tokens, not the
    /// per-request token after them). A client's default prefix is `_INBOX.<random>`; a random identifier, not a secret.
    principal: [64]u8 = undefined,
    principal_len: usize = 0,
    inbox: [48]u8 = undefined,
    inbox_len: usize = 0,

    fn reasonOf(err: anyerror) Reason {
        return switch (err) {
            error.InvalidReplyInbox => .invalid_reply_inbox,
            error.MissingMessageId => .missing_message_id,
            error.InvalidMessageId => .invalid_message_id,
            error.MessageTooLarge => .message_too_large,
            error.MalformedSubject, error.UnknownOperation => .bad_route,
            else => .other,
        };
    }

    fn note(self: *Refusals, err: anyerror) void {
        const i = @intFromEnum(reasonOf(err));
        self.since_start[i] +|= 1;
        self.window[i] +|= 1;
    }

    /// Remembers who sent a reply inbox the rule refuses.
    fn noteInbox(self: *Refusals, principal: []const u8, reply: []const u8) void {
        self.principal_len = @min(principal.len, self.principal.len);
        @memcpy(self.principal[0..self.principal_len], principal[0..self.principal_len]);
        // `_INBOX.<prefix token>`: up to the second dot, so the per-request token is not printed.
        var end = reply.len;
        if (std.mem.indexOfScalar(u8, reply, '.')) |d| if (std.mem.indexOfScalarPos(u8, reply, d + 1, '.')) |d2| {
            end = d2;
        };
        self.inbox_len = @min(end, self.inbox.len);
        @memcpy(self.inbox[0..self.inbox_len], reply[0..self.inbox_len]);
    }

    fn pending(self: *const Refusals) bool {
        for (self.window) |n| if (n > 0) return true;
        return false;
    }

    /// The log line for the refusals of the last window, which is then cleared. `buf` is the caller's.
    fn line(self: *Refusals, buf: []u8) []const u8 {
        var w = std.Io.Writer.fixed(buf);
        var total: u64 = 0;
        for (self.window) |n| total +|= n;
        w.print("mutation admission refused {d} request(s):", .{total}) catch return buf[0..0];
        for (self.window, 0..) |n, i| if (n > 0) w.print(" {s} {d} (since start {d})", .{ names[i], n, self.since_start[i] }) catch break;
        if (self.window[@intFromEnum(Reason.invalid_reply_inbox)] > 0 and self.principal_len > 0) {
            w.print("; latest InvalidReplyInbox: '{s}' asked to be answered on '{s}…', and a request on mutation.{s}.… must name an inbox that starts with '_INBOX.{s}.' — set the client's inbox prefix to '_INBOX.{s}'", .{
                self.principal[0..self.principal_len], self.inbox[0..self.inbox_len], self.principal[0..self.principal_len], self.principal[0..self.principal_len], self.principal[0..self.principal_len],
            }) catch {};
        }
        self.window = @splat(0);
        return w.buffered();
    }
};

/// The admission loop's pause after a pass, in milliseconds: none after a pass that
/// relayed something, then 1, 2, 4 … up to `idle_sleep_max_ms` while passes stay empty.
const idle_sleep_max_ms: u64 = 20;

fn nextIdleSleep(prev_ms: u64, activity: bool) u64 {
    if (activity) return 0;
    if (prev_ms == 0) return 1;
    return @min(prev_ms * 2, idle_sleep_max_ms);
}

test "admission idles: no pause while busy, a doubling pause up to the ceiling when quiet" {
    try std.testing.expectEqual(@as(u64, 0), nextIdleSleep(0, true));
    try std.testing.expectEqual(@as(u64, 0), nextIdleSleep(20, true));
    var ms: u64 = 0;
    const want = [_]u64{ 1, 2, 4, 8, 16, 20, 20 };
    for (want) |w| {
        ms = nextIdleSleep(ms, false);
        try std.testing.expectEqual(w, ms);
    }
}

fn optionalEnv(init: *const std.process.Init, key: []const u8) ?[]const u8 {
    const value = init.minimal.environ.getPosix(key) orelse return null;
    return if (value.len == 0) null else value;
}

pub fn run(init: *const std.process.Init) u8 {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer if (builtin.mode == .debug) {
        _ = gpa.deinit();
    };
    const a = if (builtin.mode == .debug) gpa.allocator() else std.heap.c_allocator;
    runImpl(init, a) catch |err| {
        log.err("mutation admission stopped: {s}", .{@errorName(err)});
        return 1;
    };
    return 0;
}

test "admission caps all of a principal's routes within a byte budget" {
    const limits: Limits = .{};
    try std.testing.expect((try limits.count()) * (@as(i64, limits.message_bytes) + 512) <= limits.principal_bytes);
    try std.testing.expectError(error.InvalidAdmissionLimits, (Limits{ .principal_messages = -1 }).count());
    try std.testing.expectError(error.InvalidAdmissionLimits, (Limits{ .principal_bytes = 1 << 30 }).count());
    try std.testing.expectError(error.InvalidAdmissionLimits, (Limits{ .principal_bytes = std.math.maxInt(i64) }).count());
    try std.testing.expectEqualStrings("alice", (try route("mutation.alice.table_a.insert", "mutation", "lf-")).principal);
    try std.testing.expectEqualStrings("alice", (try route("mutation.alice.table_b.delete", "mutation", "lf-")).principal);
    // `alice.t` is no principal (a two-token one starts with the leaf prefix): one token too many is malformed.
    try std.testing.expectError(error.MalformedSubject, route("mutation.alice.t.insert.extra", "mutation", "lf-"));
    try std.testing.expectError(error.UnknownOperation, route("mutation.alice.t.truncate", "mutation", "lf-"));
    try std.testing.expectEqualStrings("lf-leaf1.alice", (try route("mutation.lf-leaf1.alice.t.insert", "mutation", "lf-")).principal);
    try std.testing.expectError(error.MalformedSubject, route("mutation.lf-leaf1.alice.extra.t.insert", "mutation", "lf-"));
    // A plain principal in the leaf namespace, and a two-token one outside it: both refused.
    try std.testing.expectError(error.MalformedSubject, route("mutation.lf-leaf1.t.insert", "mutation", "lf-"));
    try std.testing.expectError(error.MalformedSubject, route("mutation.canada.alice.t.insert", "mutation", "lf-"));
    try std.testing.expect(replyBelongsTo("_INBOX.alice.123", "alice"));
    try std.testing.expect(!replyBelongsTo("_INBOX.alice-other.123", "alice"));
    try std.testing.expect(!replyBelongsTo("_INBOX.bob.123", "alice"));
}

test "private mutation headers must agree with the stored principal" {
    var msg = nats.Message.init(std.testing.allocator);
    defer msg.deinit();
    try msg.setSubject("$ZB.mutation.alice", false);
    try msg.headerSet(original_subject_header, "mutation.alice.orders.update");
    try std.testing.expectEqualStrings("mutation.alice.orders.update", try originalSubject(&msg, "mutation", "lf-"));
    try msg.headerSet(original_subject_header, "mutation.bob.orders.delete");
    try std.testing.expectError(error.MalformedSubject, originalSubject(&msg, "mutation", "lf-"));
    // A public caller's forged header never replaces its broker-authorized subject.
    try msg.setSubject("mutation.alice.orders.update", false);
    try std.testing.expectEqualStrings(msg.subject, try originalSubject(&msg, "mutation", "lf-"));
}

test "message IDs: zb1 IDs are strict, legacy IDs pass, nothing else does" {
    try std.testing.expect(validMessageId("zb1-t1787824800123-c-40acde74-test_types-u-1-2026-08-27T10:00:00-123000Z"));
    try std.testing.expectEqual(@as(?i64, 1787824800123), createdMs("zb1-t1787824800123-x"));
    try std.testing.expect(validMessageId("c-40acde74-test_types-u-1-2026-08-27T10:00:00-123000Z")); // legacy
    // zb1- but not the strict shape: refused, never read as a legacy ID.
    for ([_][]const u8{ "zb1-", "zb1-t-x", "zb1-t12", "zb1-t12-", "zb1-tx1-x", "zb1-t1234567890123456-x", "zb1-1787824800123-x" }) |id|
        try std.testing.expect(!validMessageId(id));
    // Not one subject token, empty, or too long.
    for ([_][]const u8{ "", "a.b", "a*", "a>", "a b", "a\tb", "a\x01b", "a\x7fb" }) |id|
        try std.testing.expect(!validMessageId(id));
    try std.testing.expect(validMessageId(&@as([512]u8, @splat('x'))) and !validMessageId(&@as([513]u8, @splat('x'))));
    // Bytes, not characters, and ASCII only: Unicode spaces are ordinary UTF-8 (the same
    // policy as both builders and PostgreSQL's receipt check).
    try std.testing.expect(validMessageId("a\u{2003}b") and validMessageId("a\u{85}b") and validMessageId("a\u{a0}b"));
    const em_spaces = comptime blk: {
        var b: [513]u8 = undefined;
        for (0..171) |i| @memcpy(b[i * 3 ..][0..3], "\u{2003}");
        break :blk b;
    };
    try std.testing.expect(!validMessageId(&em_spaces)); // 513 bytes, 171 characters
}

test "refusals are counted by reason in fixed memory, and the inbox rule names what to set" {
    var r: Refusals = .{};
    r.note(error.InvalidReplyInbox);
    r.note(error.InvalidReplyInbox);
    r.note(error.MissingMessageId);
    r.note(error.MalformedSubject);
    r.note(error.OutOfMemory); // anything else is "Other", not a new bucket
    r.noteInbox("alice", "_INBOX.tatajov82pvkmapghgmsnh.5");
    try std.testing.expect(r.pending());
    var buf: [768]u8 = undefined;
    const first = r.line(&buf);
    try std.testing.expect(std.mem.indexOf(u8, first, "refused 5 request(s)") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "InvalidReplyInbox 2 (since start 2)") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "MissingMessageId 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "BadRoute 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "Other 1") != null);
    // What the operator needs: who, the inbox prefix it used (its first two tokens, not the per-request one), and what to set.
    try std.testing.expect(std.mem.indexOf(u8, first, "'alice' asked to be answered on '_INBOX.tatajov82pvkmapghgmsnh…'") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "set the client's inbox prefix to '_INBOX.alice'") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, ".5") == null);
    // The window is cleared, the totals are not.
    try std.testing.expect(!r.pending());
    r.note(error.InvalidReplyInbox);
    const second = r.line(&buf);
    try std.testing.expect(std.mem.indexOf(u8, second, "InvalidReplyInbox 1 (since start 3)") != null);
    // A principal or an inbox longer than the buffers is cut, not overrun.
    var long_principal: [200]u8 = undefined;
    @memset(&long_principal, 'p');
    var long_inbox: [200]u8 = undefined;
    @memset(&long_inbox, 'x');
    r.noteInbox(&long_principal, &long_inbox);
    try std.testing.expectEqual(@as(usize, 64), r.principal_len);
    try std.testing.expectEqual(@as(usize, 48), r.inbox_len);
}
