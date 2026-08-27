//! The transport shell: nats.zig behind the same surface transport.ts declares.
//!
//! Maps one to one: connect (creds file wins, like the TS credsAuthenticator),
//! JetStream publish with a Nats-Msg-Id, KV get, object-store get — the calls
//! the orchestration makes through `Transport` in TS. nkeys (enrollment key
//! generation, nonce signing) is nats.zig's own nkeys.zig; nothing to add.

const std = @import("std");
const nats = @import("nats");

pub const ConnectOptions = struct {
    url: []const u8,
    /// Path to a .creds file (operator/JWT mode) — wins over user/password.
    creds_path: ?[]const u8 = null,
    user: ?[]const u8 = null,
    password: ?[]const u8 = null,
};

pub const Transport = struct {
    allocator: std.mem.Allocator,
    threaded: std.Io.Threaded,
    conn: *nats.Connection,
    js: nats.JetStream,

    pub fn connect(allocator: std.mem.Allocator, opts: ConnectOptions) !*Transport {
        const self = try allocator.create(Transport);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        self.threaded = .init(allocator, .{});
        errdefer self.threaded.deinit();
        const io = self.threaded.io();

        self.conn = try allocator.create(nats.Connection);
        errdefer allocator.destroy(self.conn);
        self.conn.* = nats.Connection.init(allocator, io, .{
            .user_creds = opts.creds_path,
            .user = opts.user,
            .password = opts.password,
            .reconnect = .{ .allow_reconnect = true },
        });
        errdefer self.conn.deinit();
        try self.conn.connect(opts.url);
        self.js = self.conn.jetstream(.{});
        return self;
    }

    pub fn deinit(self: *Transport) void {
        self.conn.deinit();
        self.allocator.destroy(self.conn);
        self.threaded.deinit();
        self.allocator.destroy(self);
    }

    /// KV get: the current value for `bucket`/`key`, copied into `a`-owned
    /// memory, or null when the key is absent (or tombstoned — the KV layer
    /// reads DEL/PURGE markers, which a raw direct get would misread as values).
    ///
    /// ⚠️ Depends on the local nats.zig fix (nats.zig/NOTES.md): direct gets
    /// must use the PER-KEY subject form, because the JWT grants deliberately
    /// exclude the bucket-level API — per-key scoping is what makes
    /// tenant-scoped KV grants possible.
    pub fn kvGet(self: *Transport, a: std.mem.Allocator, bucket: []const u8, key: []const u8) !?[]u8 {
        var kv = try self.js.kvBucket(bucket);
        defer kv.deinit();
        var entry = kv.get(key) catch return null;
        defer entry.deinit();
        return try a.dupe(u8, entry.value);
    }

    /// Object store get: the whole object, `a`-owned. The store chunks at
    /// 128 KiB — no NATS max_payload limit applies to a seed (§10n).
    pub fn objectGetBytes(self: *Transport, a: std.mem.Allocator, bucket: []const u8, name: []const u8) ![]u8 {
        var os = try nats.ObjectStore.init(self.allocator, self.js, bucket, 128 * 1024);
        defer os.deinit();
        var res = try os.getBytes(name);
        defer res.deinit();
        return try a.dupe(u8, res.value);
    }

    /// JetStream publish; `msg_id` is the idempotency key (the envelope's).
    pub fn publish(self: *Transport, subject: []const u8, data: []const u8, msg_id: ?[]const u8) !void {
        var res = try self.js.publish(subject, data, .{ .msg_id = msg_id });
        res.deinit();
    }

    /// Core subscription (the verdict channel `mutation_ack.<principal>.>`).
    pub fn subscribeSync(self: *Transport, subject: []const u8) !*nats.Subscription {
        return self.conn.subscribeSync(subject);
    }
};

// ─── live test (gated: ZB_LIVE=1 with the native stack up) ──────────────────

test "live: creds connect, schema KV, chain manifest, chain object" {
    const a = std.testing.allocator;
    const live_c = std.c.getenv("ZB_LIVE") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(live_c), "1")) return error.SkipZigTest;

    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const aa = arena.allocator();

    const t = try Transport.connect(a, .{
        .url = "nats://127.0.0.1:4222",
        .creds_path = "../scripts/native/creds/omar.creds",
    });
    defer t.deinit();

    // Schema payloads are JSON (§3): must parse and carry sqlite columns.
    const schema = (try t.kvGet(aa, "schemas", "users")) orelse return error.TestUnexpectedResult;
    const parsed = try std.json.parseFromSlice(std.json.Value, aa, schema, .{});
    try std.testing.expect(parsed.value.object.get("sqlite") != null);

    // Chain manifest (JSON) names the full object; the object must download.
    const man_bytes = (try t.kvGet(aa, "generations", "_default.users")) orelse return error.TestUnexpectedResult;
    const man = try std.json.parseFromSlice(std.json.Value, aa, man_bytes, .{});
    const full = man.value.object.get("full").?.object.get("object").?.string;
    const blob = try t.objectGetBytes(aa, "gen-_default", full);
    try std.testing.expect(blob.len > 0);
    // msgpack map marker (fixmap/map16/map32) OR a zstd frame (§10w magic).
    const b0 = blob[0];
    const is_map = (b0 >= 0x80 and b0 <= 0x8f) or b0 == 0xde or b0 == 0xdf;
    const is_zstd = blob.len >= 4 and b0 == 0x28 and blob[1] == 0xb5 and blob[2] == 0x2f and blob[3] == 0xfd;
    try std.testing.expect(is_map or is_zstd);
}
