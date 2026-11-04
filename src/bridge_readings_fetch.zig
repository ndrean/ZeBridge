//! Reads a range of one tenant's readings stream and writes it as CSV, for the archiver
//! (zb-archiver, the `zb_archiver` Python package), which turns the file into Parquet with DuckDB.
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

/// A debug-level library log would put message bodies (sensor data) on stderr, one line per message.
pub const std_options: std.Options = .{ .log_level = .warn };

const reader = @import("readings_reader.zig");
const stream_prefix = reader.stream_prefix;

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

    var gap: [2]u64 = undefined;
    const sum = reader.readChunk(gpa, io, js, .{ .tenant = a.tenant, .from = a.from, .max = a.max, .idle_ms = a.idle_ms, .batch = a.batch }, a.out, &gap) catch |err| switch (err) {
        error.StreamNotFound => {
            std.debug.print("🔴 the stream {s}{s} does not exist\n", .{ stream_prefix, a.tenant });
            return 2;
        },
        error.SequenceGap => {
            std.debug.print("🔴 readings sequence gap: after {d}, received {d}; the CSV is incomplete and must not advance the archive manifest\n", .{ gap[0], gap[1] });
            return err;
        },
        else => return err,
    };

    var line: [256]u8 = undefined;
    const out = try std.fmt.bufPrint(&line, "{{\"messages\":{d},\"rows\":{d},\"skipped\":{d},\"first_seq\":{d},\"first_row_seq\":{d},\"last_seq\":{d},\"first_ms\":{d}}}\n", .{
        sum.messages, sum.rows, sum.messages - sum.rows, sum.first_seq, sum.first_row_seq, sum.last_seq, sum.first_ms,
    });
    try std.Io.File.stdout().writeStreamingAll(io, out);
    return 0;
}

test {
    _ = reader;
}
