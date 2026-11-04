//! The native readings archiver: one tenant's readings, from the stream into Parquet files in S3.
//!
//!     stream READINGS_<tenant>  →  chunk (readings_reader.zig)  →  Parquet (DuckDB)  →  S3  →  manifest (NATS KV)
//!
//! The same archive as the Python package (zb-archiver/), the same rules (archive_core.zig),
//! the same log lines; one process, no Python, no SDK. Its position is the manifest's
//! `covers_up_to`: it keeps no consumer of its own. Each flush:
//!
//!   1. drops resent readings (the same sensor, kind and sensor time: the latest one stays);
//!   2. writes one Parquet file with DuckDB and uploads it;
//!   3. checks the object is there, with the right size;
//!   4. only then writes the manifest, on the revision it read: a second archiver fails here.
//!
//! Killed between 2 and 4, the file is an orphan nobody reads, and the next archiver covers
//! the same readings again. It refuses to start when the stream no longer holds
//! `covers_up_to + 1`, or keeps readings for less than two flushes plus the S3 outage.
//!
//! Used by `bridge_archiver` (the command) and `libzbarchive` (the C ABI, archiver_capi.zig).

const std = @import("std");
const nats = @import("nats");
const config = @import("config.zig");
const nats_endpoint = @import("nats_endpoint.zig");
const core = @import("archive_core.zig");
const reader = @import("readings_reader.zig");
const S3 = @import("archive_s3.zig");
const Duck = @import("archive_duckdb.zig");

/// The process exit codes of the Python archiver, the same numbers (zb-archiver/README.md).
pub const ExitCode = enum(u8) {
    stopped = 0,
    failed = 1,
    retention_short = 2,
    manifest_moved = 3,
    readings_lost = 4,
    upload_mismatch = 5,
    crashed = 9,
};

pub const Level = enum(u8) { info = 0, warn = 1, err = 2 };

/// Where the archiver's lines go: stdout by default (the command), a callback for a host.
pub const Log = struct {
    ctx: ?*anyopaque = null,
    func: ?*const fn (ctx: ?*anyopaque, level: u8, line: [*:0]const u8) callconv(.c) void = null,
};

/// Every field the archiver reads. Unset strings come from the environment (`resolve`), the
/// Python package's names: NATS_URL, NATS_CREDS, NATS_TLS_CA, S3_ENDPOINT, S3_BUCKET,
/// S3_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_KEYS (a local endpoint only).
pub const Config = struct {
    tenant: []const u8 = "",
    flush_s: f64 = 60,
    batch: u64 = 100_000,
    outage_s: f64 = 3600,
    /// Stop after this long with nothing to flush (0: never). Tests.
    exit_when_idle_s: f64 = 0,
    accept_loss: bool = false,
    nats_url: ?[]const u8 = null,
    creds: ?[]const u8 = null,
    tls_ca: ?[]const u8 = null,
    s3_endpoint: ?[]const u8 = null,
    s3_bucket: ?[]const u8 = null,
    s3_region: ?[]const u8 = null,
    access_key_id: ?[]const u8 = null,
    secret_access_key: ?[]const u8 = null,
    /// Where the chunk's CSV and Parquet are written (default: $TMPDIR, else /tmp).
    work_dir: ?[]const u8 = null,
    /// Tests: exit 9 between the upload and the manifest.
    crash_after_upload: bool = false,
};

fn getenv(name: [:0]const u8) ?[]const u8 {
    const v = std.mem.span(std.c.getenv(name) orelse return null);
    return if (v.len == 0) null else v;
}

/// Fills what `cfg` leaves unset from the environment. Strings point into the environment or
/// `arena`, which must outlive the run.
pub fn resolve(arena: std.mem.Allocator, io: std.Io, cfg: *Config) !void {
    if (cfg.nats_url == null) cfg.nats_url = getenv("NATS_URL");
    if (cfg.creds == null) cfg.creds = getenv("NATS_CREDS");
    if (cfg.tls_ca == null) cfg.tls_ca = getenv("NATS_TLS_CA");
    if (cfg.s3_endpoint == null) cfg.s3_endpoint = getenv("S3_ENDPOINT") orelse "http://127.0.0.1:8333";
    if (cfg.s3_bucket == null) cfg.s3_bucket = getenv("S3_BUCKET") orelse "zb-events";
    if (cfg.s3_region == null) cfg.s3_region = getenv("S3_REGION") orelse "auto";
    if (cfg.access_key_id == null) cfg.access_key_id = getenv("AWS_ACCESS_KEY_ID");
    if (cfg.secret_access_key == null) cfg.secret_access_key = getenv("AWS_SECRET_ACCESS_KEY");
    if (cfg.work_dir == null) cfg.work_dir = getenv("TMPDIR") orelse "/tmp";
    // The dev keys file is for a local S3 only (zb_archiver.env.s3): a misspelled variable once
    // sent them to R2, which answered "400 Bad Request" with no word of why.
    if (cfg.access_key_id == null and isLocal(cfg.s3_endpoint.?)) {
        const home = getenv("HOME") orelse "";
        const path = getenv("S3_KEYS") orelse try std.fmt.allocPrint(arena, "{s}/.config/zebridge/s3.json", .{home});
        const text = std.Io.Dir.cwd().readFileAlloc(io, path, arena, .limited(1 << 20)) catch return;
        const v = std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{}) catch return;
        if (v != .object) return;
        var c = v.object;
        if (c.get("identities")) |ids| {
            if (ids != .array or ids.array.items.len == 0 or ids.array.items[0] != .object) return;
            const creds = ids.array.items[0].object.get("credentials") orelse return;
            if (creds != .array or creds.array.items.len == 0 or creds.array.items[0] != .object) return;
            c = creds.array.items[0].object;
        }
        const id = c.get("accessKey") orelse c.get("access_key_id") orelse return;
        const secret = c.get("secretKey") orelse c.get("secret_access_key") orelse return;
        if (id != .string or secret != .string) return;
        cfg.access_key_id = id.string;
        cfg.secret_access_key = secret.string;
    }
}

fn isLocal(endpoint: []const u8) bool {
    for ([_][]const u8{ "://127.0.0.1", "://localhost", "://[::1]" }) |h| if (std.mem.indexOf(u8, endpoint, h) != null) return true;
    return false;
}

/// The user name in a creds file's JWT: its own inbox is `_INBOX.<name>.>`.
pub fn principalOf(arena: std.mem.Allocator, creds_text: []const u8) ?[]const u8 {
    const begin = "-----BEGIN NATS USER JWT-----";
    const at = std.mem.indexOf(u8, creds_text, begin) orelse return null;
    const rest = creds_text[at + begin.len ..];
    const end = std.mem.indexOf(u8, rest, "------END") orelse return null;
    const jwt = std.mem.trim(u8, rest[0..end], " \t\r\n");
    var parts = std.mem.splitScalar(u8, jwt, '.');
    _ = parts.next() orelse return null;
    const body = parts.next() orelse return null;
    const dec = std.base64.url_safe_no_pad.Decoder;
    const trimmed = std.mem.trimEnd(u8, body, "=");
    const n = dec.calcSizeForSlice(trimmed) catch return null;
    const out = arena.alloc(u8, n) catch return null;
    dec.decode(out, trimmed) catch return null;
    const v = std.json.parseFromSliceLeaky(std.json.Value, arena, out, .{}) catch return null;
    if (v != .object) return null;
    const name = v.object.get("name") orelse return null;
    return if (name == .string) name.string else null;
}

pub const Archiver = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    cfg: Config,
    stop: *std.atomic.Value(bool),
    log: Log,

    fn say(a: *Archiver, level: Level, comptime fmt: []const u8, args: anytype) void {
        var buf: [1024]u8 = undefined;
        const line = std.mem.printSentinel(&buf, fmt, args, 0) catch blk: {
            buf[buf.len - 1] = 0;
            break :blk buf[0 .. buf.len - 1 :0];
        };
        if (a.log.func) |f| return f(a.log.ctx, @intFromEnum(level), line.ptr);
        var out: [1100]u8 = undefined;
        const text = std.fmt.bufPrint(&out, "{s}\n", .{line}) catch return;
        std.Io.File.stdout().writeStreamingAll(a.io, text) catch {};
    }

    fn stopped(a: *Archiver) bool {
        return a.stop.load(.acquire);
    }

    /// Sleeps up to `ms`, in steps that a stop ends.
    fn nap(a: *Archiver, ms: u64) void {
        var left = ms;
        while (left > 0 and !a.stopped()) {
            const step = @min(left, 250);
            a.io.sleep(.fromMilliseconds(@intCast(step)), .awake) catch return;
            left -= step;
        }
    }

    fn monotonicS(a: *Archiver) f64 {
        const ns = std.Io.Clock.awake.now(a.io).toNanoseconds();
        return @as(f64, @floatFromInt(ns)) / std.time.ns_per_s;
    }

    pub fn run(a: *Archiver) ExitCode {
        return a.runInner() catch |err| {
            a.say(.err, "🔴 the archiver stopped: {s}", .{@errorName(err)});
            return .failed;
        };
    }

    fn runInner(a: *Archiver) !ExitCode {
        const gpa = a.gpa;
        const io = a.io;
        const cfg = a.cfg;
        if (cfg.tenant.len == 0) {
            a.say(.err, "🔴 no tenant: the archiver archives one tenant (--tenant)", .{});
            return .failed;
        }
        const endpoint = cfg.s3_endpoint.?;
        if (cfg.access_key_id == null or cfg.secret_access_key == null) {
            a.say(.err, "🔴 AWS_ACCESS_KEY_ID is not set for {s}: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY " ++
                "(the S3 key pair; with R2, the API token's access key)", .{endpoint});
            return .failed;
        }

        var arena_state = std.heap.ArenaAllocator.init(gpa);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        // DuckDB first: a missing library is said before anything connects.
        var duck: Duck.Db = .{ .api = undefined, .lib = undefined };
        duck.open() catch {
            a.say(.err, "🔴 {s}", .{duck.why});
            return .failed;
        };
        defer duck.close();

        // NATS, like bridge_readings_fetch: NATS_URL, creds, a private CA.
        const url_text = cfg.nats_url orelse "nats://127.0.0.1:4222";
        var ep = config.Nats.Endpoint.parseUrl(url_text) catch {
            a.say(.err, "🔴 NATS_URL is not usable: expected nats://[user:pass@]host[:port] or tls://…", .{});
            return .failed;
        };
        ep.creds = cfg.creds;
        ep.tls_ca = cfg.tls_ca;
        ep.tls_cert = getenv("NATS_TLS_CERT");
        ep.tls_key = getenv("NATS_TLS_KEY");
        ep.tls_server_name = getenv("NATS_TLS_SERVER_NAME");
        ep.js_domain = getenv("NATS_JS_DOMAIN");
        var inbox: []const u8 = "_INBOX";
        if (cfg.creds) |path| {
            const text = std.Io.Dir.cwd().readFileAlloc(io, path, arena, .limited(64 << 10)) catch {
                a.say(.err, "🔴 no creds file at {s}: mount the archiver's (bridge --mint-archiver)", .{path});
                return .failed;
            };
            // An archiver's credential may subscribe to its own inbox only.
            if (principalOf(arena, text)) |name| inbox = try std.fmt.allocPrint(arena, "_INBOX.{s}", .{name});
        }
        var conn = nats.Connection.init(gpa, io, .{
            .user = ep.user,
            .password = ep.pass,
            .nkey_seed = ep.seed,
            .user_creds = ep.creds,
            .tls = nats_endpoint.tlsOptions(ep),
            .inbox_prefix = inbox,
        });
        defer conn.deinit();
        const dial = try ep.dialUrl(gpa);
        defer gpa.free(dial);
        try conn.connect(dial);
        const js = nats.JetStream.init(&conn, .{ .domain = ep.js_domain });

        const stream = try std.fmt.allocPrint(arena, "{s}{s}", .{ reader.stream_prefix, cfg.tenant });

        // The stream may not exist yet: the bridge makes it when the tenant has its first row
        // in zebridge_user_tenants. Waiting, rather than exiting, starts the archive the moment
        // it does, with one line said instead of a restart loop.
        var said = false;
        var info = while (true) {
            if (a.stopped()) return .stopped;
            break js.getStreamInfo(stream) catch |err| switch (err) {
                error.StreamNotFound => {
                    if (!said) {
                        a.say(.info, "⏳ waiting for the stream {s}: the bridge makes it when the tenant has a row in zebridge_user_tenants " ++
                            "(INSERT INTO zebridge_user_tenants (principal, tenant_id) VALUES ('<principal>', '{s}')). Checking every 10 s.", .{ stream, cfg.tenant });
                        said = true;
                    }
                    a.nap(10_000);
                    continue;
                },
                else => return err,
            };
        };

        // The manifest AFTER the wait: its revision is what the first write must name.
        var mkey_buf: [256]u8 = undefined;
        const mkey = try core.manifestKey(&mkey_buf, cfg.tenant);
        var read = try readManifest(arena, js, mkey);
        const archived = read.manifest != null; // a tenant with no archive yet has nothing to lose
        var manifest = read.manifest orelse try core.Manifest.empty(arena);
        const covers = manifest.coversUpTo();

        const max_age = info.value.config.max_age;
        const first = info.value.state.first_seq;
        const messages = info.value.state.messages;
        info.deinit();
        if (core.retentionShort(max_age, cfg.flush_s, cfg.outage_s)) |need| {
            a.say(.err, "🔴 {s} keeps readings {d:.0} s; this archiver needs {d:.0} s (2 × flush {d:.0} s + outage {d:.0} s): raise the stream's max_age", .{
                stream, @as(f64, @floatFromInt(max_age)) / std.time.ns_per_s, need, cfg.flush_s, cfg.outage_s,
            });
            return .retention_short;
        }
        const start = switch (core.startAt(archived, covers, first, messages, cfg.accept_loss)) {
            .lost => |l| {
                a.say(.err, "🔴 the stream starts at {d}, the archive covers up to {d}: readings {d}..{d} are gone and no file holds them (--accept-loss to go on)", .{ l.first, l.covers, l.covers + 1, l.first - 1 });
                return .readings_lost;
            },
            .from => |s| s,
        };
        a.say(.info, "archiver {s}: archive covers up to {d} ({d} file(s)), following sensors.{s}.> from {d} (native, DuckDB {s})", .{ cfg.tenant, covers, manifest.fileCount(), cfg.tenant, start, duck.version() });

        var s3: S3.Client = .init(gpa, io, .{
            .endpoint = endpoint,
            .bucket = cfg.s3_bucket.?,
            .region = cfg.s3_region.?,
            .access_key_id = cfg.access_key_id.?,
            .secret_access_key = cfg.secret_access_key.?,
        });
        defer s3.deinit();

        // A private work directory: two archivers on one host never share a chunk file.
        var rnd: [6]u8 = undefined;
        io.random(&rnd);
        const work = try std.fmt.allocPrint(arena, "{s}/zb-archiver-{s}-{x}", .{ std.mem.trimEnd(u8, cfg.work_dir.?, "/"), cfg.tenant, rnd });
        try std.Io.Dir.cwd().createDirPath(io, work);
        defer std.Io.Dir.cwd().deleteTree(io, work) catch {};
        const csv_path = try std.fmt.allocPrint(arena, "{s}/chunk.csv", .{work});
        const parquet_path = try std.fmt.allocPrint(arena, "{s}/chunk.parquet", .{work});

        var pos = start - 1; // the last stream sequence this archiver has read
        var waiting_since: ?f64 = null;
        var idle_since = a.monotonicS();
        while (!a.stopped()) {
            var si = try js.getStreamInfo(stream);
            const last_seq = si.value.state.last_seq;
            si.deinit();
            const pending: u64 = if (last_seq > pos) last_seq - pos else 0;
            const now = a.monotonicS();
            if (pending > 0 and waiting_since == null) waiting_since = now;
            if (pending == 0) {
                waiting_since = null;
                if (cfg.exit_when_idle_s > 0 and now - idle_since >= cfg.exit_when_idle_s) break;
            } else if (core.shouldFlush(pending, now - waiting_since.?, cfg.batch, cfg.flush_s)) {
                idle_since = now;
                while (!a.stopped()) {
                    var gap: [2]u64 = undefined;
                    const sum = reader.readChunk(gpa, io, js, .{ .tenant = cfg.tenant, .from = pos + 1, .max = cfg.batch }, csv_path, &gap) catch |err| {
                        switch (err) {
                            error.SequenceGap => a.say(.err, "🔴 readings sequence gap: after {d}, received {d}; the archive manifest stays unchanged", .{ gap[0], gap[1] }),
                            else => a.say(.err, "🔴 reading the stream failed ({s})", .{@errorName(err)}),
                        }
                        return .failed;
                    };
                    switch (core.checkChunk(sum, pos, cfg.accept_loss)) {
                        .empty => break,
                        .pruned => {
                            a.say(.err, "🔴 the stream now starts at {d}, the archive covers up to {d}: readings {d}..{d} are gone and no file holds them (--accept-loss to go on)", .{ sum.first_seq, pos, pos + 1, sum.first_seq - 1 });
                            return .readings_lost;
                        },
                        .gap => {
                            a.say(.err, "🔴 the reader returned a gap inside {d}..{d}; the archive manifest stays unchanged", .{ sum.first_seq, sum.last_seq });
                            return .readings_lost;
                        },
                        .skipped_only => manifest.setCoversUpTo(sum.last_seq),
                        .rows => {
                            const code = try a.flush(arena, &duck, &s3, &manifest, csv_path, parquet_path, sum);
                            if (code != .stopped) return code;
                        },
                    }
                    const code = try a.commit(js, mkey, &manifest, &read.rev);
                    if (code != .stopped) return code;
                    pos = sum.last_seq;
                    if (core.lastOfCycle(sum, cfg.batch)) break;
                }
                waiting_since = null;
                continue;
            }
            a.nap(250);
        }
        return .stopped;
    }

    fn flush(a: *Archiver, arena: std.mem.Allocator, duck: *Duck.Db, s3: *S3.Client, manifest: *core.Manifest, csv_path: []const u8, parquet_path: []const u8, sum: core.Summary) !ExitCode {
        const nrows = duck.csvToParquet(a.gpa, csv_path, parquet_path) catch |err| {
            a.say(.err, "🔴 DuckDB could not write the Parquet file: {s}", .{if (err == error.DuckDBFailed) duck.why else @errorName(err)});
            return .failed;
        };
        const body = try std.Io.Dir.cwd().readFileAlloc(a.io, parquet_path, a.gpa, .unlimited);
        defer a.gpa.free(body);
        var key_buf: [512]u8 = undefined;
        const key = try core.fileKey(&key_buf, a.cfg.tenant, sum.first_ms, sum.first_row_seq, sum.last_seq);
        s3.put(key, body) catch |err| {
            a.say(.err, "🔴 {s}: the upload failed ({s}{s}{d}); the manifest stays", .{ key, @errorName(err), if (err == error.S3Refused) ", HTTP " else "", if (err == error.S3Refused) s3.last_status else 0 });
            return .failed;
        };
        const there = s3.headSize(key) catch |err| {
            a.say(.err, "🔴 {s}: the size check failed ({s}); the manifest stays", .{ key, @errorName(err) });
            return .failed;
        };
        if (there == null or there.? != body.len) {
            a.say(.err, "🔴 {s}: {d} bytes in S3, {d} written; the manifest stays", .{ key, there orelse 0, body.len });
            return .upload_mismatch;
        }
        a.say(.info, "flushed {d} reading(s) to {s} ({d} bytes)", .{ nrows, key, body.len });
        if (a.cfg.crash_after_upload) std.process.exit(@intFromEnum(ExitCode.crashed));
        try manifest.addFile(arena, key, nrows, body.len, sum.first_row_seq, sum.last_seq);
        return .stopped;
    }

    /// Files first, manifest second, on the revision read: one writer per tenant.
    fn commit(a: *Archiver, js: nats.JetStream, mkey: []const u8, manifest: *core.Manifest, rev: *?u64) !ExitCode {
        const text = try manifest.serialize(a.gpa);
        defer a.gpa.free(text);
        var subj_buf: [320]u8 = undefined;
        const subject = try std.fmt.bufPrint(&subj_buf, "$KV." ++ core.kv_bucket ++ ".{s}", .{mkey});
        var ack = js.publish(subject, text, .{ .expected_last_subject_seq = rev.* orelse 0 }) catch |err| switch (err) {
            error.StreamWrongLastSequence => {
                a.say(.err, "🔴 the manifest moved under this archiver (another one runs?): {s}", .{@errorName(err)});
                return .manifest_moved;
            },
            else => return err,
        };
        defer ack.deinit();
        rev.* = ack.value.seq;
        a.say(.info, "manifest: covers up to {d}, {d} file(s)", .{ manifest.coversUpTo(), manifest.fileCount() });
        return .stopped;
    }
};

const Read = struct { manifest: ?core.Manifest, rev: ?u64 };

/// (manifest, revision): (null, null) when the key never existed, (null, rev) when it was
/// deleted or purged — the next write must name that revision. Read by DIRECT GET of the exact
/// key: binding the bucket asks for its stream info, which the archiver's grants refuse.
fn readManifest(arena: std.mem.Allocator, js: nats.JetStream, mkey: []const u8) !Read {
    var subj_buf: [320]u8 = undefined;
    const subject = try std.fmt.bufPrint(&subj_buf, "$KV." ++ core.kv_bucket ++ ".{s}", .{mkey});
    const msg = js.getMsg("KV_" ++ core.kv_bucket, .{ .last_by_subj = subject, .direct = true }) catch |err| switch (err) {
        error.MessageNotFound => return .{ .manifest = null, .rev = null },
        else => return err,
    };
    defer msg.deinit();
    const rev = msg.seq;
    if (msg.headerGet("KV-Operation")) |op| {
        if (std.mem.eql(u8, op, "DEL") or std.mem.eql(u8, op, "PURGE")) return .{ .manifest = null, .rev = rev };
    }
    if (msg.data.len == 0) return .{ .manifest = null, .rev = rev };
    return .{ .manifest = try core.Manifest.parse(arena, try arena.dupe(u8, msg.data)), .rev = rev };
}

test "the principal of a creds file" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    // {"name":"archiver-acme"} as the JWT's body.
    const creds = "-----BEGIN NATS USER JWT-----\neyJhbGciOiJlZDI1NTE5In0.eyJuYW1lIjoiYXJjaGl2ZXItYWNtZSJ9.sig\n------END NATS USER JWT------\n";
    try std.testing.expectEqualStrings("archiver-acme", principalOf(arena_state.allocator(), creds).?);
    try std.testing.expect(principalOf(arena_state.allocator(), "no jwt here") == null);
}

test "a local endpoint" {
    try std.testing.expect(isLocal("http://127.0.0.1:8333"));
    try std.testing.expect(isLocal("http://localhost:9000"));
    try std.testing.expect(!isLocal("https://acct.r2.cloudflarestorage.com"));
}

test {
    _ = core;
    _ = S3;
    _ = Duck;
    _ = reader;
}
