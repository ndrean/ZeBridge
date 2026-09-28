//! Enrollment inside the library (NOTES §10jq): `zb_client_connect` with a bridge URL
//! and an invite code does what every app used to write itself — generate the nkey
//! pair, `GET /enroll?code=…&user_pubkey=…`, build the `.creds` text from the JWT and
//! the seed — then keeps the result as an IDENTITY file, so every later connect needs
//! neither the invite nor any of the facts the bridge handed out (the NATS URL, the
//! grammar hash, the JetStream domain, the principal).
//!
//! The identity file is JSON, mode 0600, written to a temporary name and renamed into
//! place: a crash leaves the old identity or the new one, never half of one. The seed is
//! generated here and never leaves the device; it is in the file because the creds are
//! (the same as a `.creds` file, which is what apps kept before).
//!
//! HTTPS uses std.http.Client, whose trust store is the system's on macOS, Linux and
//! Android — NOT on iOS, where Zig cannot read the keychain: an https:// enrollment
//! there fails with a message saying so (§10jq), and the host enrolls itself.

const std = @import("std");
const builtin = @import("builtin");
const nats = @import("nats");
const core = @import("core.zig");
const Value = std.json.Value;

/// Why the last enrollment or identity step failed on this thread, for zb_last_error.
pub threadlocal var last_failure: ?[]const u8 = null;
threadlocal var failure_buf: [512]u8 = undefined;

fn fail(comptime fmt: []const u8, args: anytype) error{EnrollFailed} {
    last_failure = std.fmt.bufPrint(&failure_buf, fmt, args) catch failure_buf[0..];
    return error.EnrollFailed;
}

pub const Identity = struct {
    bridge_url: []u8,
    principal: []u8,
    creds: []u8,
    nats_url: ?[]u8 = null,
    grammar_hash: ?[]u8 = null,
    js_domain: ?[]u8 = null,

    pub fn deinit(self: *Identity, a: std.mem.Allocator) void {
        std.crypto.secureZero(u8, self.creds); // the seed is in there
        a.free(self.bridge_url);
        a.free(self.principal);
        a.free(self.creds);
        if (self.nats_url) |v| a.free(v);
        if (self.grammar_hash) |v| a.free(v);
        if (self.js_domain) |v| a.free(v);
    }
};

/// The `.creds` text: the JWT and the seed in the layout every NATS client reads.
pub fn credsText(a: std.mem.Allocator, jwt: []const u8, seed: []const u8) ![]u8 {
    return std.fmt.allocPrint(a,
        \\-----BEGIN NATS USER JWT-----
        \\{s}
        \\------END NATS USER JWT------
        \\
        \\************************* IMPORTANT *************************
        \\NKEY Seed printed below can be used to sign and prove identity.
        \\NKEYs are sensitive and should be treated as secrets.
        \\
        \\-----BEGIN USER NKEY SEED-----
        \\{s}
        \\------END USER NKEY SEED------
        \\
        \\*************************************************************
        \\
    , .{ jwt, seed });
}

pub const UserKeys = struct {
    seed_buf: [nats.nkeys.seed_text_len]u8 = undefined,
    pub_buf: [nats.nkeys.public_key_text_len]u8 = undefined,
    seed: []const u8 = &.{},
    public: []const u8 = &.{},

    pub fn wipe(self: *UserKeys) void {
        std.crypto.secureZero(u8, &self.seed_buf);
    }
};

/// A fresh user nkey pair — what `zb_create_user` returns, shared with enrollment.
/// The public text is derived back FROM the seed text, so a seed that cannot be read
/// back never escapes.
pub fn newUserKeys(out: *UserKeys) !void {
    var threaded: std.Io.Threaded = .init(std.heap.c_allocator, .{});
    defer threaded.deinit();
    const kp = std.crypto.sign.Ed25519.KeyPair.generate(threaded.io());
    out.seed = nats.nkeys.encodeSeed(.user, &kp.secret_key.seed(), &out.seed_buf);
    var skp = try nats.nkeys.SeedKeyPair.fromSeed(out.seed);
    defer skp.wipe();
    out.public = skp.publicKeyText(&out.pub_buf);
}

/// Redeem `code` at `bridge_url`: a new identity, not yet saved.
pub fn enroll(a: std.mem.Allocator, bridge_url: []const u8, code: []const u8) !Identity {
    for (code) |ch| if (!(std.ascii.isAlphanumeric(ch) or ch == '-' or ch == '_' or ch == '.' or ch == '~'))
        return fail("enroll: the invite code has a character outside [A-Za-z0-9-_.~]", .{});
    const base = std.mem.trimEnd(u8, bridge_url, "/");
    if (builtin.os.tag == .ios and std.ascii.startsWithIgnoreCase(base, "https://"))
        return fail("enroll: https on iOS needs the system trust store, which libzb cannot read — enroll in the app (URLSession) and pass `creds`", .{});

    var keys: UserKeys = .{};
    defer keys.wipe();
    newUserKeys(&keys) catch return fail("enroll: could not generate the key pair", .{});

    const url = try std.fmt.allocPrint(a, "{s}/enroll?code={s}&user_pubkey={s}", .{ base, code, keys.public });
    defer a.free(url);

    var threaded: std.Io.Threaded = .init(a, .{});
    defer threaded.deinit();
    var client: std.http.Client = .{ .allocator = a, .io = threaded.io() };
    defer client.deinit();
    var body: std.Io.Writer.Allocating = .init(a);
    defer body.deinit();
    const res = client.fetch(.{ .location = .{ .url = url }, .response_writer = &body.writer }) catch |err|
        return fail("enroll: {s} unreachable ({s})", .{ base, @errorName(err) });
    const text = body.written();
    if (res.status != .ok) {
        const why = std.mem.trim(u8, text, " \r\n");
        return switch (res.status) {
            .not_found => fail("enroll: {s} has enrollment off (404) — the bridge needs ZB_SIGNING_SEED and ZB_ACCOUNT_PUB", .{base}),
            .forbidden, .unauthorized => fail("enroll: refused ({d}): the code is invalid, used or expired, or the principal was revoked", .{@intFromEnum(res.status)}),
            else => fail("enroll: {s} answered {d}: {s}", .{ base, @intFromEnum(res.status), why[0..@min(why.len, 200)] }),
        };
    }

    const parsed = std.json.parseFromSlice(Value, a, text, .{}) catch return fail("enroll: the bridge's answer is not JSON", .{});
    defer parsed.deinit();
    const o = parsed.value;
    const jwt = str(o, "jwt") orelse return fail("enroll: the bridge's answer has no jwt", .{});
    const principal = str(o, "principal") orelse return fail("enroll: the bridge's answer has no principal", .{});

    var id: Identity = .{
        .bridge_url = try a.dupe(u8, base),
        .principal = try a.dupe(u8, principal),
        .creds = try credsText(a, jwt, keys.seed),
    };
    errdefer id.deinit(a);
    if (str(o, "nats_url")) |v| id.nats_url = try a.dupe(u8, v);
    if (str(o, "grammar_hash")) |v| id.grammar_hash = try a.dupe(u8, v);
    if (str(o, "js_domain")) |v| id.js_domain = try a.dupe(u8, v);
    return id;
}

/// §10jt: the JWT's issue and expiry times (unix seconds), read from its payload —
/// no signature check, the server does that; this only decides WHEN to renew.
pub const JwtTimes = struct { iat: i64, exp: i64 };

pub fn jwtTimes(creds: []const u8) ?JwtTimes {
    const jwt = section(creds, "-----BEGIN NATS USER JWT-----", "------END NATS USER JWT------") orelse return null;
    var parts = std.mem.splitScalar(u8, jwt, '.');
    _ = parts.next() orelse return null;
    const payload = parts.next() orelse return null;
    var buf: [4096]u8 = undefined;
    const dec = std.base64.url_safe_no_pad.Decoder;
    const n = dec.calcSizeForSlice(payload) catch return null;
    if (n > buf.len) return null;
    dec.decode(buf[0..n], payload) catch return null;
    var fba = std.heap.FixedBufferAllocator.init(buf[n..]);
    const parsed = std.json.parseFromSliceLeaky(Value, fba.allocator(), buf[0..n], .{}) catch return null;
    if (parsed != .object) return null;
    const iat = parsed.object.get("iat") orelse return null;
    const exp = parsed.object.get("exp") orelse return null;
    if (iat != .integer or exp != .integer) return null;
    return .{ .iat = iat.integer, .exp = exp.integer };
}

/// Renew when less than a quarter of the JWT's life is left (or it is gone).
pub fn renewDue(creds: []const u8, now: i64) bool {
    const t = jwtTimes(creds) orelse return false;
    const life = t.exp - t.iat;
    return life > 0 and (t.exp - now) * 4 < life;
}

fn section(text: []const u8, begin: []const u8, end: []const u8) ?[]const u8 {
    const b = std.mem.indexOf(u8, text, begin) orelse return null;
    const from = b + begin.len;
    const e = std.mem.indexOfPos(u8, text, from, end) orelse return null;
    return std.mem.trim(u8, text[from..e], " \r\n\t");
}

/// §10jt: a new JWT for the SAME key, no invite: sign `zebridge-renew:<pub>:<ts>` with
/// the identity's seed, `GET <bridge>/renew`, and rebuild the identity from the answer
/// (the NATS URLs, grammar hash and memberships come back current).
pub fn renew(a: std.mem.Allocator, id: Identity) !Identity {
    if (id.bridge_url.len == 0) return fail("renew: the identity names no bridge", .{});
    if (builtin.os.tag == .ios and std.ascii.startsWithIgnoreCase(id.bridge_url, "https://"))
        return fail("renew: https on iOS needs the system trust store, which libzb cannot read", .{});
    const seed = section(id.creds, "-----BEGIN USER NKEY SEED-----", "------END USER NKEY SEED------") orelse
        return fail("renew: the identity's creds hold no seed", .{});
    var skp = nats.nkeys.SeedKeyPair.fromSeed(seed) catch return fail("renew: the identity's seed does not decode", .{});
    defer skp.wipe();
    var pub_buf: [nats.nkeys.public_key_text_len]u8 = undefined;
    const public = skp.publicKeyText(&pub_buf);
    var ts_c: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts_c);
    const ts: i64 = @intCast(ts_c.sec);
    var msg_buf: [128]u8 = undefined;
    const msg = std.fmt.bufPrint(&msg_buf, "zebridge-renew:{s}:{d}", .{ public, ts }) catch unreachable;
    const sig = skp.sign(msg) catch return fail("renew: signing failed", .{});
    var sig_buf: [88]u8 = undefined;
    const sig_s = std.base64.url_safe_no_pad.Encoder.encode(&sig_buf, &sig);

    const url = try std.fmt.allocPrint(a, "{s}/renew?user_pubkey={s}&ts={d}&sig={s}", .{ id.bridge_url, public, ts, sig_s });
    defer a.free(url);
    var threaded: std.Io.Threaded = .init(a, .{});
    defer threaded.deinit();
    var client: std.http.Client = .{ .allocator = a, .io = threaded.io() };
    defer client.deinit();
    var body: std.Io.Writer.Allocating = .init(a);
    defer body.deinit();
    const res = client.fetch(.{ .location = .{ .url = url }, .response_writer = &body.writer }) catch |err|
        return fail("renew: {s} unreachable ({s})", .{ id.bridge_url, @errorName(err) });
    const text = body.written();
    if (res.status != .ok) {
        const why = std.mem.trim(u8, text, " \r\n");
        return switch (res.status) {
            .forbidden => fail("renew: refused — the key is revoked or no membership is left: a new invite is needed", .{}),
            .unauthorized => fail("renew: refused ({s})", .{why[0..@min(why.len, 160)]}),
            else => fail("renew: {s} answered {d}: {s}", .{ id.bridge_url, @intFromEnum(res.status), why[0..@min(why.len, 160)] }),
        };
    }
    const parsed = std.json.parseFromSlice(Value, a, text, .{}) catch return fail("renew: the bridge's answer is not JSON", .{});
    defer parsed.deinit();
    const o = parsed.value;
    const jwt = str(o, "jwt") orelse return fail("renew: the bridge's answer has no jwt", .{});
    var out: Identity = .{
        .bridge_url = try a.dupe(u8, id.bridge_url),
        .principal = try a.dupe(u8, str(o, "principal") orelse id.principal),
        .creds = try credsText(a, jwt, seed),
    };
    errdefer out.deinit(a);
    if (str(o, "nats_url")) |v| out.nats_url = try a.dupe(u8, v);
    if (str(o, "grammar_hash")) |v| out.grammar_hash = try a.dupe(u8, v);
    if (str(o, "js_domain")) |v| out.js_domain = try a.dupe(u8, v);
    return out;
}

fn str(o: Value, k: []const u8) ?[]const u8 {
    if (o != .object) return null;
    const v = o.object.get(k) orelse return null;
    return if (v == .string and v.string.len > 0) v.string else null;
}

/// The identity at `path`, or null when there is none yet.
pub fn load(a: std.mem.Allocator, path: []const u8) !?Identity {
    const pz = try a.dupeZ(u8, path);
    defer a.free(pz);
    const fd = std.posix.openat(std.posix.AT.FDCWD, pz, .{ .ACCMODE = .RDONLY }, 0) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return fail("identity: cannot open {s} ({s})", .{ path, @errorName(err) }),
    };
    defer _ = std.posix.system.close(fd);
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer {
        std.crypto.secureZero(u8, buf.items);
        buf.deinit(a);
    }
    var chunk: [4096]u8 = undefined;
    while (true) {
        const n = std.posix.read(fd, &chunk) catch return fail("identity: cannot read {s}", .{path});
        if (n == 0) break;
        try buf.appendSlice(a, chunk[0..n]);
        if (buf.items.len > 1 << 20) return fail("identity: {s} is not an identity file (over 1 MiB)", .{path});
    }
    const parsed = std.json.parseFromSlice(Value, a, buf.items, .{}) catch return fail("identity: {s} is not JSON", .{path});
    defer parsed.deinit();
    const o = parsed.value;
    const creds = str(o, "creds") orelse return fail("identity: {s} has no creds", .{path});
    var id: Identity = .{
        .bridge_url = try a.dupe(u8, str(o, "bridge_url") orelse ""),
        .principal = try a.dupe(u8, str(o, "principal") orelse ""),
        .creds = try a.dupe(u8, creds),
    };
    errdefer id.deinit(a);
    if (str(o, "nats_url")) |v| id.nats_url = try a.dupe(u8, v);
    if (str(o, "grammar_hash")) |v| id.grammar_hash = try a.dupe(u8, v);
    if (str(o, "js_domain")) |v| id.js_domain = try a.dupe(u8, v);
    return id;
}

/// Write `id` to `path`: mode 0600, a temporary name renamed into place.
pub fn save(a: std.mem.Allocator, path: []const u8, id: Identity) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const aa = arena.allocator();
    var obj: std.json.ObjectMap = .empty;
    try obj.put(aa, "version", .{ .integer = 1 });
    try obj.put(aa, "bridge_url", .{ .string = id.bridge_url });
    try obj.put(aa, "principal", .{ .string = id.principal });
    try obj.put(aa, "creds", .{ .string = id.creds });
    if (id.nats_url) |v| try obj.put(aa, "nats_url", .{ .string = v });
    if (id.grammar_hash) |v| try obj.put(aa, "grammar_hash", .{ .string = v });
    if (id.js_domain) |v| try obj.put(aa, "js_domain", .{ .string = v });
    const text = try core.valueToString(aa, .{ .object = obj });
    defer std.crypto.secureZero(u8, @constCast(text));

    const tmp = try std.fmt.allocPrintSentinel(aa, "{s}.tmp", .{path}, 0);
    const final = try aa.dupeZ(u8, path);
    const fd = std.posix.openat(std.posix.AT.FDCWD, tmp, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, 0o600) catch |err|
        return fail("identity: cannot write {s} ({s})", .{ path, @errorName(err) });
    var off: usize = 0;
    while (off < text.len) {
        const n = std.c.write(fd, text[off..].ptr, text.len - off);
        if (n <= 0) {
            _ = std.posix.system.close(fd);
            _ = std.c.unlink(tmp);
            return fail("identity: short write to {s}", .{path});
        }
        off += @intCast(n);
    }
    _ = std.c.fsync(fd);
    _ = std.posix.system.close(fd);
    if (std.c.rename(tmp, final) != 0) {
        _ = std.c.unlink(tmp);
        return fail("identity: cannot rename into {s}", .{path});
    }
}

test "an identity survives save and load, and the file is private" {
    const a = std.testing.allocator;
    const path = "zbz-test-identity.json";
    defer _ = std.c.unlink(path);
    var id: Identity = .{
        .bridge_url = try a.dupe(u8, "https://zb.example.com"),
        .principal = try a.dupe(u8, "alice"),
        .creds = try credsText(a, "eyJ0eXAi.jwt.sig", "SUAEXAMPLESEED"),
        .nats_url = try a.dupe(u8, "tls://zb.example.com:4222"),
        .grammar_hash = try a.dupe(u8, "848d9fc8"),
    };
    defer id.deinit(a);
    try save(a, path, id);
    var back = (try load(a, path)).?;
    defer back.deinit(a);
    try std.testing.expectEqualStrings(id.creds, back.creds);
    try std.testing.expectEqualStrings("alice", back.principal);
    try std.testing.expectEqualStrings("tls://zb.example.com:4222", back.nats_url.?);
    try std.testing.expect(back.js_domain == null);
    const st = try std.Io.Dir.cwd().statFile(std.testing.io, path, .{});
    try std.testing.expectEqual(@as(u32, 0o600), @as(u32, @intCast(@intFromEnum(st.permissions))) & 0o777);
    try std.testing.expect((try load(a, "zbz-no-such-identity.json")) == null);
}

test "a code outside the safe alphabet is refused before any request" {
    try std.testing.expectError(error.EnrollFailed, enroll(std.testing.allocator, "http://127.0.0.1:1", "bad code&x=1"));
    try std.testing.expect(std.mem.indexOf(u8, last_failure.?, "invite code") != null);
}

test "renewDue: a quarter of the life left is the line" {
    // header.payload.sig with payload {"iat":1000,"exp":2000}
    const payload = "eyJpYXQiOjEwMDAsImV4cCI6MjAwMH0";
    const creds = "-----BEGIN NATS USER JWT-----\nxx." ++ payload ++ ".yy\n------END NATS USER JWT------\n";
    try std.testing.expectEqual(@as(i64, 2000), jwtTimes(creds).?.exp);
    try std.testing.expect(!renewDue(creds, 1500));
    try std.testing.expect(!renewDue(creds, 1750));
    try std.testing.expect(renewDue(creds, 1751));
    try std.testing.expect(renewDue(creds, 2500));
}
