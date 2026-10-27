//! Reads a range of one tenant's readings stream and writes it as CSV, for the archiver
//! (examples/16-archive/archiver.py), which turns the file into Parquet with DuckDB.
//!
//!   bridge_readings_fetch --tenant acme --from 120001 --max 100000 --out /tmp/x.csv [--inbox-prefix _INBOX.archiver-acme]
//!
//! Prints one JSON line on stdout and exits 0:
//!
//!   {"messages":N,"rows":R,"skipped":K,"first_seq":F,"first_row_seq":G,"last_seq":L,"first_ms":M}
//!
//! `messages` is what the stream delivered, `rows` what was a reading (readings_decode.zig),
//! `skipped` the rest. `first_seq`/`last_seq` are the stream sequences of the first and last
//! MESSAGE, so the caller's position moves past a skipped one; `first_ms` is the sensor time of
//! the first row, `first_row_seq` the sequence of that row (the name of the archive file). The stream is contiguous (it holds only this tenant's readings), so a
//! `first_seq` above `--from` means the stream pruned readings before they were read: the caller
//! decides, this tool reports.
//!
//! It stops at `--max` messages, or when the stream has nothing more for `--idle-ms`. An empty
//! range is `{"messages":0,...}`, not an error. The position is the caller's (the archive's
//! manifest): the consumer is a throwaway, unacknowledged one that the server reaps.
//!
//! CSV, one line per reading, no header — strings quoted, numbers not:
//!   "tenant","sensor","kind",value,sensor_ms,received_ms,stream_seq
//!
//! Connects like the bridge: NATS_URL (nats:// or tls://), NATS_CREDS, NATS_TLS_CA, NATS_TLS_CERT,
//! NATS_TLS_KEY, NATS_TLS_SERVER_NAME, NATS_JS_DOMAIN.
//!
//! Exit codes: 0 done, 1 usage or a failure (said on stderr), 2 the stream does not exist.
const std = @import("std");
const nats = @import("nats");
const config = @import("config.zig");
const nats_endpoint = @import("nats_endpoint.zig");
const decode_mod = @import("readings_decode.zig");

/// A debug-level library log would put message bodies (sensor data) on stderr, one line per message.
pub const std_options: std.Options = .{ .log_level = .warn };

/// Fixed names, bridge-side only (topology.zig, "sensor readings"); a test there pins them.
pub const stream_prefix = "READINGS_";
pub const subject_prefix = "sensors";

const usage =
    \\usage: bridge_readings_fetch --tenant T --from SEQ --out PATH [--max N] [--idle-ms MS] [--batch N] [--inbox-prefix P]
    \\  --tenant T     the tenant whose stream READINGS_<T> is read
    \\  --from SEQ     the first stream sequence to read (the archive's covers_up_to + 1)
    \\  --out PATH     the CSV file to write (created, truncated)
    \\  --max N        stop after N messages (default 100000)
    \\  --idle-ms MS   stop when nothing arrives for this long (default 500)
    \\  --batch N      messages per pull request (default 5000)
    \\  --inbox-prefix P  the reply inbox, `_INBOX.<name>` for a credential that may subscribe to
    \\                 nothing else (default `_INBOX`; also NATS_INBOX_PREFIX)
    \\
;

const Args = struct {
    tenant: []const u8,
    from: u64,
    out: []const u8,
    max: u64 = 100_000,
    idle_ms: u64 = 500,
    batch: usize = 5000,
    inbox_prefix: ?[]const u8 = null,
};

fn parseArgs(init: std.process.Init) ?Args {
    var it = init.minimal.args.iterate();
    _ = it.next();
    var tenant: ?[]const u8 = null;
    var from: ?u64 = null;
    var out: ?[]const u8 = null;
    var a = Args{ .tenant = "", .from = 0, .out = "" };
    while (it.next()) |arg| {
        const val = it.next() orelse return null;
        if (std.mem.eql(u8, arg, "--tenant")) tenant = val else if (std.mem.eql(u8, arg, "--from")) from = std.fmt.parseInt(u64, val, 10) catch return null else if (std.mem.eql(u8, arg, "--out")) out = val else if (std.mem.eql(u8, arg, "--max")) a.max = std.fmt.parseInt(u64, val, 10) catch return null else if (std.mem.eql(u8, arg, "--idle-ms")) a.idle_ms = std.fmt.parseInt(u64, val, 10) catch return null else if (std.mem.eql(u8, arg, "--batch")) a.batch = std.fmt.parseInt(usize, val, 10) catch return null else if (std.mem.eql(u8, arg, "--inbox-prefix")) a.inbox_prefix = val else return null;
    }
    a.tenant = tenant orelse return null;
    a.from = from orelse return null;
    a.out = out orelse return null;
    if (a.from == 0 or a.max == 0 or a.batch == 0) return null;
    return a;
}

/// A CSV string field: quoted, a quote doubled. A subject token may hold a comma or a quote.
fn appendQuoted(buf: *std.ArrayList(u8), gpa: std.mem.Allocator, s: []const u8) !void {
    try buf.append(gpa, '"');
    for (s) |ch| {
        if (ch == '"') try buf.append(gpa, '"');
        try buf.append(gpa, ch);
    }
    try buf.append(gpa, '"');
}

fn env(init: std.process.Init, name: []const u8) ?[]const u8 {
    const v = init.minimal.environ.getPosix(name) orelse return null;
    return if (v.len == 0) null else v;
}

pub fn main(init: std.process.Init) u8 {
    return run(init) catch |err| {
        std.debug.print("🔴 bridge_readings_fetch: {s}\n", .{@errorName(err)});
        return 1;
    };
}

fn run(init: std.process.Init) !u8 {
    const gpa = init.gpa;
    const io = init.io;
    const a = parseArgs(init) orelse {
        std.debug.print("{s}", .{usage});
        return 1;
    };

    var ep = config.Nats.Endpoint.parseUrl(env(init, "NATS_URL") orelse "nats://127.0.0.1:4222") catch {
        std.debug.print("🔴 NATS_URL is not usable: expected nats://[user:pass@]host[:port] or tls://…\n", .{});
        return 1;
    };
    ep.creds = env(init, "NATS_CREDS");
    ep.tls_ca = env(init, "NATS_TLS_CA");
    ep.tls_cert = env(init, "NATS_TLS_CERT");
    ep.tls_key = env(init, "NATS_TLS_KEY");
    ep.tls_server_name = env(init, "NATS_TLS_SERVER_NAME");
    ep.js_domain = env(init, "NATS_JS_DOMAIN");

    var conn = nats.Connection.init(gpa, io, .{
        .user = ep.user,
        .password = ep.pass,
        .nkey_seed = ep.seed,
        .user_creds = ep.creds,
        .tls = nats_endpoint.tlsOptions(ep),
        .inbox_prefix = a.inbox_prefix orelse env(init, "NATS_INBOX_PREFIX") orelse "_INBOX",
    });
    defer conn.deinit();
    const url = try ep.dialUrl(gpa);
    defer gpa.free(url);
    try conn.connect(url);
    const js = nats.JetStream.init(&conn, .{ .domain = ep.js_domain });

    const stream = try std.fmt.allocPrint(gpa, "{s}{s}", .{ stream_prefix, a.tenant });
    defer gpa.free(stream);
    const filter = try std.fmt.allocPrint(gpa, "{s}.{s}.>", .{ subject_prefix, a.tenant });
    defer gpa.free(filter);

    var name_buf: [32]u8 = undefined;
    var rnd: [6]u8 = undefined;
    io.random(&rnd);
    const cname = try std.fmt.bufPrint(&name_buf, "rf{x}", .{rnd});

    const sub = js.pullSubscribe(null, cname, .{ .stream = stream, .config = .{
        .name = cname,
        .durable_name = cname,
        .ack_policy = .none,
        .filter_subject = filter,
        .deliver_policy = .by_start_sequence,
        .opt_start_seq = a.from,
        // The server reaps it: the archiver's grants do not include CONSUMER.DELETE.
        .inactive_threshold = 30 * std.time.ns_per_s,
    } }) catch |err| {
        if (err == error.StreamNotFound) {
            std.debug.print("🔴 the stream {s} does not exist\n", .{stream});
            return 2;
        }
        return err;
    };
    defer sub.deinit();
    sub.pull_depth = 2;
    sub.max_bytes = 8 * 1024 * 1024;
    // A fetch returns when its batch is full or this long after the first message.
    sub.idle_after_first = .{ .duration = .{ .raw = .fromMilliseconds(@intCast(a.idle_ms)), .clock = .awake } };

    const file = try std.Io.Dir.cwd().createFile(io, a.out, .{});
    defer file.close(io);
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(gpa);
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();

    const idle: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(@intCast(a.idle_ms)), .clock = .awake } };
    var messages: u64 = 0;
    var rows: u64 = 0;
    var first_seq: u64 = 0;
    var last_seq: u64 = 0;
    var first_ms: ?i64 = null;
    var first_row_seq: u64 = 0;
    while (messages < a.max) {
        const want: usize = @intCast(@min(@as(u64, a.batch), a.max - messages));
        var batch = sub.fetch(want, idle) catch |err| switch (err) {
            error.Timeout => break,
            else => return err,
        };
        defer batch.deinit();
        if (batch.messages.len == 0) break;
        _ = arena_state.reset(.retain_capacity);
        const arena = arena_state.allocator();
        for (batch.messages) |m| {
            messages += 1;
            const seq = m.metadata.sequence.stream;
            if (first_seq == 0) first_seq = seq;
            last_seq = seq;
            const received_ms = m.metadata.timestamp.toMilliseconds();
            const row = decode_mod.decode(arena, m.msg.subject, m.msg.data, received_ms) orelse continue;
            rows += 1;
            if (first_ms == null) {
                first_ms = row.sensor_ms;
                first_row_seq = seq;
            }
            try appendQuoted(&buf, gpa, row.tenant);
            try buf.append(gpa, ',');
            try appendQuoted(&buf, gpa, row.sensor);
            try buf.append(gpa, ',');
            try appendQuoted(&buf, gpa, row.kind);
            try buf.print(gpa, ",{d},{d},{d},{d}\n", .{ row.value, row.sensor_ms, received_ms, seq });
            if (buf.items.len >= 1 << 20) {
                try file.writeStreamingAll(io, buf.items);
                buf.clearRetainingCapacity();
            }
        }
    }
    if (buf.items.len > 0) try file.writeStreamingAll(io, buf.items);

    var line: [256]u8 = undefined;
    const out = try std.fmt.bufPrint(&line, "{{\"messages\":{d},\"rows\":{d},\"skipped\":{d},\"first_seq\":{d},\"first_row_seq\":{d},\"last_seq\":{d},\"first_ms\":{d}}}\n", .{
        messages, rows, messages - rows, first_seq, first_row_seq, last_seq, first_ms orelse 0,
    });
    try std.Io.File.stdout().writeStreamingAll(io, out);
    return 0;
}

test "the names are the topology's" {
    const topology = @import("topology.zig");
    var owned = try topology.loadEmbedded(std.testing.allocator);
    defer owned.deinit();
    try std.testing.expectEqualStrings(owned.topology.readings_stream_prefix, stream_prefix);
    try std.testing.expectEqualStrings(owned.topology.subject_readings_prefix, subject_prefix);
}
