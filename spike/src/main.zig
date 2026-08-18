//! Does `nats.zig` cover what ZeBridge actually depends on?
//!
//! Five capabilities carry the work done on the bridge so far, and two of them are exactly
//! where the vendored client failed *silently*:
//!
//!   1. headers on a RECEIVED message — the verdict feature reads `Nats-Msg-Id` off a
//!      delivered mutation. In `src/nats_vendor`, `getHeaders()` returned a value where a
//!      pointer was declared (so it had never been compiled — Zig only analyses what is
//!      called), and `Headers.hiter()` silently yields nothing because `read_HMSG` strips
//!      the trailing CRLF that `std.http.HeaderIterator` needs to emit the last header.
//!   2. delivery count + stream sequence — the bridge publishes a verdict on its *last*
//!      attempt, which means knowing which attempt it is on. Hand-parsed today from the
//!      `$JS.ACK.…` subject, whose token layout shifts by two when a JetStream domain is
//!      configured.
//!   3. PubAck.duplicate — what lets an idempotent outbox pop without writing twice.
//!   4. purge by subject filter — orphaned snapshot chunks after an aborted snapshot.
//!   5. durable pull consumer with max_deliver / filter_subject / explicit ack.
//!
//! Run against the running stack. Exits non-zero if any check fails, so it is a test and
//! not a demo.

const std = @import("std");
const nats = @import("nats");

const log = std.log.scoped(.spike);
var failures: usize = 0;

fn check(ok: bool, comptime what: []const u8, args: anytype) void {
    if (ok) {
        std.debug.print("  \x1b[32m✓\x1b[0m " ++ what ++ "\n", args);
    } else {
        std.debug.print("  \x1b[31m✗\x1b[0m " ++ what ++ "\n", args);
        failures += 1;
    }
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;

    // The bridge's own credential. user/password would exercise the client path instead;
    // this spike is about the bridge's side.
    var conn = nats.Connection.init(gpa, init.io, .{
        .nkey_seed = "SUAPSL67RKOUDZFREHHDWUXDXLYZKEHMWEXMIUC35Z4Z2LXWP55SWVJS4Q",
    });
    defer conn.deinit();
    try conn.connect("nats://127.0.0.1:4222");
    std.debug.print("\nconnected\n\n", .{});

    var js = nats.JetStream.init(&conn, .{});

    // A stream of our own, so nothing here can disturb CDC/INIT/MUTATIONS.
    const stream = "ZB_SPIKE";
    const subject = "zb.spike.one";
    _ = js.deleteStream(stream) catch {};
    {
        var res = try js.addStream(.{
            .name = stream,
            .subjects = &.{"zb.spike.>"},
            .storage = .file,
            .retention = .limits,
        });
        defer res.deinit();
        _ = res.value;
        std.debug.print("stream {s} created\n\n", .{stream});
    }
    defer {
        _ = js.deleteStream(stream) catch {};
    }

    // ── 1 + 3. publish with Nats-Msg-Id, and read the PubAck ────────────────────
    const msg_id = "spike-msg-id-42";
    var first_seq: u64 = 0;
    {
        var res = try js.publish(subject, "payload-one", .{ .msg_id = msg_id });
        defer res.deinit();
        const ack = res.value;
        first_seq = ack.seq;
        check(std.mem.eql(u8, ack.stream, stream), "PubAck names the stream ({s}, seq={d})", .{ ack.stream, ack.seq });
        check(!ack.duplicate, "PubAck.duplicate is false for a first publish", .{});
    }
    {
        // Same Nats-Msg-Id inside the dedup window: the ack must say so, which is what an
        // outbox needs to pop safely after a lost acknowledgement.
        var res = try js.publish(subject, "payload-one", .{ .msg_id = msg_id });
        defer res.deinit();
        const ack = res.value;
        check(ack.duplicate, "PubAck.duplicate is TRUE for a repeat of the same msg_id", .{});
        check(ack.seq == first_seq, "and it reports the original sequence ({d})", .{ack.seq});
    }

    // ── 5. durable pull consumer with the config the bridge uses ────────────────
    // Deliberately WITHOUT `.stream`, so the library looks it up from the subject — the
    // path that used to hit a use-after-free (see nats.zig/NOTES.md #1). The bridge would
    // normally pass `.stream` since topology.json names it, which is exactly why the bug
    // was easy to miss; this spike takes the other path on purpose.
    const sub = try js.pullSubscribe(subject, "zb_spike_worker", .{
        .config = .{
            .ack_policy = .explicit,
            .max_deliver = 3,
            .filter_subject = subject,
        },
    });
    defer sub.deinit();
    check(true, "durable pull consumer created (explicit ack, max_deliver=3)", .{});

    // ── 1 + 2. fetch, then read the header and the metadata off the message ─────
    {
        var batch = try sub.fetch(1, .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } });
        defer batch.deinit();
        check(batch.messages.len == 1, "fetched {d} message", .{batch.messages.len});

        if (batch.messages.len == 1) {
            const m = batch.messages[0];

            const got = m.msg.headerGet("Nats-Msg-Id");
            check(got != null, "Nats-Msg-Id is readable on a RECEIVED message", .{});
            if (got) |v| check(std.mem.eql(u8, v, msg_id), "and its value round-tripped: '{s}'", .{v});

            check(m.metadata.num_delivered == 1, "metadata.num_delivered = {d} on first delivery", .{m.metadata.num_delivered});
            check(m.metadata.sequence.stream == first_seq, "metadata stream seq {d} matches the PubAck", .{m.metadata.sequence.stream});

            // NAK, so the next fetch must report delivery 2 — the bridge decides whether it
            // is on its last attempt from exactly this number.
            try m.nak();
        }
    }
    {
        var batch = try sub.fetch(1, .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } });
        defer batch.deinit();
        if (batch.messages.len == 1) {
            const m = batch.messages[0];
            check(m.metadata.num_delivered == 2, "after a NAK, num_delivered = {d} — redelivery is observable", .{m.metadata.num_delivered});
            try m.ack();
        } else {
            check(false, "expected a redelivery after NAK, got {d} messages", .{batch.messages.len});
        }
    }

    // ── 4. purge by subject filter ──────────────────────────────────────────────
    {
        var r1 = try js.publish("zb.spike.keep", "keep me", .{});
        r1.deinit();
        var r2 = try js.publish("zb.spike.drop", "drop me", .{});
        r2.deinit();
        var r3 = try js.publish("zb.spike.drop", "drop me too", .{});
        r3.deinit();

        var res = try js.purgeStream(stream, .{ .filter = "zb.spike.drop" });
        defer res.deinit();
        const purged = res.value;
        check(purged.purged == 2, "purge by subject filter removed {d} message(s), leaving the rest", .{purged.purged});
    }

    std.debug.print("\n", .{});
    if (failures == 0) {
        std.debug.print("\x1b[32mall five capabilities present\x1b[0m\n", .{});
    } else {
        std.debug.print("\x1b[31m{d} check(s) failed\x1b[0m\n", .{failures});
        std.process.exit(1);
    }
}
