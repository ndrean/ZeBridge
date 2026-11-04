//! The S3 calls the archiver makes, and nothing else: PUT one object, HEAD it for its size.
//! Signed with AWS Signature Version 4 over std.http, so the archiver carries no SDK and
//! DuckDB needs no `httpfs` extension (an extension DuckDB would otherwise download at run
//! time). Any S3 API: Cloudflare R2 (region `auto`), AWS, SeaweedFS, MinIO. Path-style
//! addressing (`<endpoint>/<bucket>/<key>`), which every one of them accepts.
//!
//! The payload is signed (its SHA-256 in `x-amz-content-sha256`), not `UNSIGNED-PAYLOAD`: a
//! file corrupted on its way is refused by the server instead of stored.

const std = @import("std");
const Sha256 = std.crypto.hash.sha2.Sha256;
const Hmac = std.crypto.auth.hmac.sha2.HmacSha256;

pub const Config = struct {
    /// `https://<account>.r2.cloudflarestorage.com`, `http://127.0.0.1:8333`, …
    endpoint: []const u8,
    bucket: []const u8,
    region: []const u8 = "auto",
    access_key_id: []const u8,
    secret_access_key: []const u8,
};

/// `x-amz-date` (`20130524T000000Z`) and the scope's date (`20130524`) of a time in seconds.
pub const Stamp = struct {
    amz_date: [16]u8,
    date: [8]u8,

    pub fn of(unix_s: i64) Stamp {
        const es = std.time.epoch.EpochSeconds{ .secs = @intCast(unix_s) };
        const yd = es.getEpochDay().calculateYearDay();
        const md = yd.calculateMonthDay();
        const ds = es.getDaySeconds();
        var s: Stamp = undefined;
        _ = std.fmt.bufPrint(&s.amz_date, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
            yd.year, md.month.numeric(), md.day_index + 1, ds.getHoursIntoDay(), ds.getMinutesIntoHour(), ds.getSecondsIntoMinute(),
        }) catch unreachable;
        @memcpy(&s.date, s.amz_date[0..8]);
        return s;
    }
};

/// SigV4's URI encoding: everything but `A-Z a-z 0-9 - . _ ~` as `%XX` (upper case), and `/`
/// kept in a path. A tenant is any subject token, so a key may hold `%`, `+` or UTF-8.
pub fn uriEncode(w: *std.Io.Writer, s: []const u8, keep_slash: bool) !void {
    for (s) |c| {
        const plain = std.ascii.isAlphanumeric(c) or c == '-' or c == '.' or c == '_' or c == '~' or (keep_slash and c == '/');
        if (plain) try w.writeByte(c) else try w.print("%{X:0>2}", .{c});
    }
}

fn hex(out: *[64]u8, digest: [32]u8) []const u8 {
    return std.fmt.bufPrint(out, "{x}", .{digest}) catch unreachable;
}

pub fn sha256Hex(out: *[64]u8, data: []const u8) []const u8 {
    var d: [32]u8 = undefined;
    Sha256.hash(data, &d, .{});
    return hex(out, d);
}

fn hmac(key: []const u8, msg: []const u8) [32]u8 {
    var out: [32]u8 = undefined;
    Hmac.create(&out, msg, key);
    return out;
}

pub const Header = struct { name: []const u8, value: []const u8 };

/// The `Authorization` header of one request. `headers` are the signed ones, names lower
/// case and sorted, `host` and `x-amz-*` among them; `path` is already URI-encoded.
pub fn authorization(buf: []u8, cfg: Config, stamp: Stamp, method: []const u8, path: []const u8, query: []const u8, headers: []const Header, payload_hash: []const u8) ![]const u8 {
    var creq_buf: [4096]u8 = undefined;
    var cw: std.Io.Writer = .fixed(&creq_buf);
    try cw.print("{s}\n{s}\n{s}\n", .{ method, path, query });
    for (headers) |h| try cw.print("{s}:{s}\n", .{ h.name, std.mem.trim(u8, h.value, " ") });
    try cw.writeByte('\n');
    var signed_buf: [256]u8 = undefined;
    var sw: std.Io.Writer = .fixed(&signed_buf);
    for (headers, 0..) |h, i| {
        if (i > 0) try sw.writeByte(';');
        try sw.writeAll(h.name);
    }
    const signed = sw.buffered();
    try cw.print("{s}\n{s}", .{ signed, payload_hash });

    var scope_buf: [128]u8 = undefined;
    const scope = try std.fmt.bufPrint(&scope_buf, "{s}/{s}/s3/aws4_request", .{ &stamp.date, cfg.region });
    var h1: [64]u8 = undefined;
    var sts_buf: [256]u8 = undefined;
    const sts = try std.fmt.bufPrint(&sts_buf, "AWS4-HMAC-SHA256\n{s}\n{s}\n{s}", .{ &stamp.amz_date, scope, sha256Hex(&h1, cw.buffered()) });

    var k_secret_buf: [160]u8 = undefined;
    const k_secret = try std.fmt.bufPrint(&k_secret_buf, "AWS4{s}", .{cfg.secret_access_key});
    const k_date = hmac(k_secret, &stamp.date);
    const k_region = hmac(&k_date, cfg.region);
    const k_service = hmac(&k_region, "s3");
    const k_signing = hmac(&k_service, "aws4_request");
    var sig: [64]u8 = undefined;
    _ = hex(&sig, hmac(&k_signing, sts));
    return std.fmt.bufPrint(buf, "AWS4-HMAC-SHA256 Credential={s}/{s}, SignedHeaders={s}, Signature={s}", .{ cfg.access_key_id, scope, signed, &sig });
}

pub const Error = error{ S3Refused, BadEndpoint } || std.mem.Allocator.Error;

pub const Client = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    cfg: Config,
    http: std.http.Client,
    /// The status of the last refused call, for the log line.
    last_status: u16 = 0,

    pub fn init(gpa: std.mem.Allocator, io: std.Io, cfg: Config) Client {
        return .{ .gpa = gpa, .io = io, .cfg = cfg, .http = .{ .allocator = gpa, .io = io } };
    }

    pub fn deinit(c: *Client) void {
        c.http.deinit();
    }

    const Target = struct {
        url: []u8,
        path: []u8,
        host: []u8,
        fn deinit(t: Target, gpa: std.mem.Allocator) void {
            gpa.free(t.url);
            gpa.free(t.path);
            gpa.free(t.host);
        }
    };

    fn target(c: *Client, key: []const u8) !Target {
        const endpoint = std.mem.trimEnd(u8, c.cfg.endpoint, "/");
        const uri = std.Uri.parse(endpoint) catch return error.BadEndpoint;
        const host_name = switch (uri.host orelse return error.BadEndpoint) {
            .raw => |r| r,
            .percent_encoded => |p| p,
        };
        const host = if (uri.port) |p| try std.fmt.allocPrint(c.gpa, "{s}:{d}", .{ host_name, p }) else try c.gpa.dupe(u8, host_name);
        errdefer c.gpa.free(host);
        var pw: std.Io.Writer.Allocating = .init(c.gpa);
        errdefer pw.deinit();
        // An endpoint with a path prefix (a gateway) keeps it, encoded once as given.
        const base_path = switch (uri.path) {
            .raw => |r| r,
            .percent_encoded => |p| p,
        };
        try pw.writer.writeAll(std.mem.trimEnd(u8, base_path, "/"));
        try pw.writer.writeByte('/');
        try uriEncode(&pw.writer, c.cfg.bucket, false);
        try pw.writer.writeByte('/');
        try uriEncode(&pw.writer, key, true);
        const path = try pw.toOwnedSlice();
        errdefer c.gpa.free(path);
        const url = try std.fmt.allocPrint(c.gpa, "{s}://{s}{s}", .{ uri.scheme, host, path });
        return .{ .url = url, .path = path, .host = host };
    }

    fn signedHeaders(c: *Client, auth_buf: []u8, method: []const u8, t: Target, payload_hash: []const u8, stamp: *const Stamp) ![]const u8 {
        const hs = [_]Header{
            .{ .name = "host", .value = t.host },
            .{ .name = "x-amz-content-sha256", .value = payload_hash },
            .{ .name = "x-amz-date", .value = &stamp.amz_date },
        };
        return authorization(auth_buf, c.cfg, stamp.*, method, t.path, "", &hs, payload_hash);
    }

    fn now(c: *Client) Stamp {
        const ts = std.Io.Clock.real.now(c.io);
        return Stamp.of(@intCast(@divFloor(ts.toNanoseconds(), std.time.ns_per_s)));
    }

    /// PUT `body` at `key`. error.S3Refused (status in `last_status`) for a non-2xx answer.
    pub fn put(c: *Client, key: []const u8, body: []const u8) !void {
        const t = try c.target(key);
        defer t.deinit(c.gpa);
        var ph_buf: [64]u8 = undefined;
        const ph = sha256Hex(&ph_buf, body);
        const stamp = c.now();
        var auth_buf: [512]u8 = undefined;
        const auth = try c.signedHeaders(&auth_buf, "PUT", t, ph, &stamp);
        const extra = [_]std.http.Header{
            .{ .name = "x-amz-content-sha256", .value = ph },
            .{ .name = "x-amz-date", .value = &stamp.amz_date },
        };
        var req = try c.http.request(.PUT, try std.Uri.parse(t.url), .{
            .headers = .{ .host = .{ .override = t.host }, .authorization = .{ .override = auth }, .content_type = .{ .override = "application/vnd.apache.parquet" }, .accept_encoding = .omit, .user_agent = .{ .override = "zebridge-archiver" } },
            .extra_headers = &extra,
            .keep_alive = false,
        });
        defer req.deinit();
        req.transfer_encoding = .{ .content_length = body.len };
        try req.sendBodyComplete(@constCast(body));
        var redirect: [1024]u8 = undefined;
        const resp = try req.receiveHead(&redirect);
        const code = @intFromEnum(resp.head.status);
        if (code < 200 or code > 299) {
            c.last_status = code;
            return error.S3Refused;
        }
    }

    /// The object's size, null when it is not there (404).
    pub fn headSize(c: *Client, key: []const u8) !?u64 {
        const t = try c.target(key);
        defer t.deinit(c.gpa);
        var ph_buf: [64]u8 = undefined;
        const ph = sha256Hex(&ph_buf, "");
        const stamp = c.now();
        var auth_buf: [512]u8 = undefined;
        const auth = try c.signedHeaders(&auth_buf, "HEAD", t, ph, &stamp);
        const extra = [_]std.http.Header{
            .{ .name = "x-amz-content-sha256", .value = ph },
            .{ .name = "x-amz-date", .value = &stamp.amz_date },
        };
        var req = try c.http.request(.HEAD, try std.Uri.parse(t.url), .{
            .headers = .{ .host = .{ .override = t.host }, .authorization = .{ .override = auth }, .accept_encoding = .omit, .user_agent = .{ .override = "zebridge-archiver" } },
            .extra_headers = &extra,
            .keep_alive = false,
        });
        defer req.deinit();
        try req.sendBodiless();
        var redirect: [1024]u8 = undefined;
        const resp = try req.receiveHead(&redirect);
        const code = @intFromEnum(resp.head.status);
        if (code == 404) return null;
        if (code < 200 or code > 299) {
            c.last_status = code;
            return error.S3Refused;
        }
        return resp.head.content_length orelse 0;
    }
};

// ─── tests ──────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "the signature of AWS's own example (GET /test.txt, Range)" {
    // docs.aws.amazon.com, "Signature Calculations for the Authorization Header", example GET Object.
    const cfg: Config = .{ .endpoint = "https://examplebucket.s3.amazonaws.com", .bucket = "examplebucket", .region = "us-east-1", .access_key_id = "AKIAIOSFODNN7EXAMPLE", .secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" };
    const empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const stamp = Stamp.of(1369353600); // 2013-05-24T00:00:00Z
    try testing.expectEqualStrings("20130524T000000Z", &stamp.amz_date);
    var buf: [512]u8 = undefined;
    const got = try authorization(&buf, cfg, stamp, "GET", "/test.txt", "", &.{
        .{ .name = "host", .value = "examplebucket.s3.amazonaws.com" },
        .{ .name = "range", .value = "bytes=0-9" },
        .{ .name = "x-amz-content-sha256", .value = empty },
        .{ .name = "x-amz-date", .value = "20130524T000000Z" },
    }, empty);
    try testing.expectEqualStrings("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", got);
}

test "a key is encoded the way S3 signs it" {
    var buf: [128]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try uriEncode(&w, "a b/c+d%é~_.-", true);
    try testing.expectEqualStrings("a%20b/c%2Bd%25%C3%A9~_.-", w.buffered());
    w = .fixed(&buf);
    try uriEncode(&w, "x/y", false);
    try testing.expectEqualStrings("x%2Fy", w.buffered());
}

test "the date of a stamp" {
    const s = Stamp.of(1_700_000_000);
    try testing.expectEqualStrings("20231114T221320Z", &s.amz_date);
    try testing.expectEqualStrings("20231114", &s.date);
}
