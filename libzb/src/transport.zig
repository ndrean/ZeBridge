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

/// §10fh: a chain object read through a PULL consumer, a few chunks at a time — the
/// streaming seed's source. The object store's own reader is a push subscription:
/// the server sends every chunk at once and the client's pending queue (64 MB)
/// drops the rest as SlowConsumer the moment the reader pauses to apply a chunk of
/// rows (measured: the seed stopped after its first 50,000 rows). A pull consumer
/// asks for `batch` chunks when it wants them, the way CDC is read (§10bh) and the
/// way the TypeScript client reads objects (@nats-io/obj #430). Memory held: one
/// batch of chunk messages (8 × 128 KB). The digest is checked at the end, as the
/// store's reader does.
pub const ObjectPull = struct {
    a: std.mem.Allocator,
    sub: *nats.PullSubscription,
    chunks: u32,
    size: u64,
    digest: []const u8,
    batch: ?nats.MessageBatch = null,
    bi: usize = 0,
    pos: usize = 0,
    chunk_index: u32 = 0,
    hasher: std.crypto.hash.sha2.Sha256 = std.crypto.hash.sha2.Sha256.init(.{}),
    eof: bool = false,

    const batch_size: usize = 8;
    const Ctr = struct {
        var n = std.atomic.Value(u32).init(0);
    };

    pub fn open(t: *Transport, a: std.mem.Allocator, bucket: []const u8, name: []const u8) !ObjectPull {
        const stream = try std.fmt.allocPrint(a, "OBJ_{s}", .{bucket});
        const enc = try a.alloc(u8, std.base64.url_safe.Encoder.calcSize(name.len));
        _ = std.base64.url_safe.Encoder.encode(enc, name);
        const meta_subject = try std.fmt.allocPrint(a, "$O.{s}.M.{s}", .{ bucket, enc });
        const msg = t.js.getMsg(stream, .{ .last_by_subj = meta_subject, .direct = true }) catch |err| switch (err) {
            error.MessageNotFound => return error.ObjectNotFound,
            else => return err,
        };
        defer msg.deinit();
        const Info = struct { nuid: []const u8, size: u64 = 0, chunks: u32 = 0, digest: []const u8 = "", deleted: bool = false };
        const info = (try std.json.parseFromSliceLeaky(Info, a, msg.data, .{ .ignore_unknown_fields = true, .allocate = .alloc_always }));
        if (info.deleted) return error.ObjectNotFound;
        const chunk_subject = try std.fmt.allocPrint(a, "$O.{s}.C.{s}", .{ bucket, info.nuid });
        // Named like the CDC consumers (pid + counter), reaped by the server after
        // 30 s idle: the client JWT has no CONSUMER.DELETE grant (see openConsumer).
        const cname = try std.fmt.allocPrint(a, "zbo{d}x{d}", .{ std.c.getpid(), Ctr.n.fetchAdd(1, .monotonic) });
        var cfg: nats.ConsumerConfig = .{ .ack_policy = .none, .deliver_policy = .all };
        cfg.name = cname;
        cfg.durable_name = cname;
        cfg.inactive_threshold = 30 * std.time.ns_per_s;
        const sub = try t.js.pullSubscribe(chunk_subject, cname, .{ .stream = stream, .config = cfg });
        return .{ .a = a, .sub = sub, .chunks = info.chunks, .size = info.size, .digest = info.digest };
    }

    pub fn deinit(self: *ObjectPull) void {
        if (self.batch) |*b| b.deinit();
        self.sub.deinit();
    }

    /// The next bytes of the object into `dest`; 0 at the end.
    pub fn read(self: *ObjectPull, dest: []u8) !usize {
        while (!self.eof) {
            if (self.batch) |*b| {
                if (self.bi < b.messages.len) {
                    const data = b.messages[self.bi].msg.data;
                    const n = @min(dest.len, data.len - self.pos);
                    @memcpy(dest[0..n], data[self.pos..][0..n]);
                    self.pos += n;
                    if (self.pos >= data.len) {
                        self.hasher.update(data);
                        self.bi += 1;
                        self.pos = 0;
                        self.chunk_index += 1;
                        if (self.chunk_index >= self.chunks) self.eof = true;
                    }
                    return n;
                }
                b.deinit();
                self.batch = null;
                self.bi = 0;
            }
            if (self.chunk_index >= self.chunks) {
                self.eof = true;
                break;
            }
            const want: usize = @min(batch_size, self.chunks - self.chunk_index);
            self.batch = try self.sub.fetch(want, .{ .duration = .{ .raw = .fromMilliseconds(10_000), .clock = .awake } });
            if (self.batch.?.messages.len == 0) return error.ObjectTruncated;
        }
        return 0;
    }

    /// The object's digest against what was read — a chunk lost or replaced is an error.
    pub fn verify(self: *ObjectPull) !void {
        if (self.chunk_index < self.chunks) return error.ObjectTruncated;
        if (self.digest.len == 0) return;
        var hash: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        self.hasher.final(&hash);
        var text: [8 + std.base64.url_safe.Encoder.calcSize(32)]u8 = undefined;
        @memcpy(text[0..8], "SHA-256=");
        _ = std.base64.url_safe.Encoder.encode(text[8..], &hash);
        if (!std.mem.eql(u8, &text, self.digest)) return error.ObjectDigestMismatch;
    }
};

pub const Transport = struct {
    allocator: std.mem.Allocator,
    threaded: std.Io.Threaded,
    conn: *nats.Connection,
    js: nats.JetStream,
    /// The object store handle for the bucket last read (a chain is one bucket, many
    /// objects). `ObjectStore.init` is four small string dupes, so this is a tidiness
    /// cache, not a hot-path one; what matters is that it is released in `deinit`.
    os: ?nats.ObjectStore = null,
    os_bucket: []u8 = &.{},

    pub fn connect(allocator: std.mem.Allocator, opts: ConnectOptions) !*Transport {
        const self = try allocator.create(Transport);
        errdefer allocator.destroy(self);
        // ⚠️ A struct literal, so the defaulted fields (`os`, `os_bucket`) are set.
        // Field-by-field assignment leaves every unassigned field undefined.
        self.* = .{
            .allocator = allocator,
            .threaded = .init(allocator, .{}),
            .conn = undefined,
            .js = undefined,
        };
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
        self.dropObjectStore();
        self.conn.deinit();
        self.allocator.destroy(self.conn);
        self.threaded.deinit();
        self.allocator.destroy(self);
    }

    fn dropObjectStore(self: *Transport) void {
        if (self.os) |*o| o.deinit();
        self.os = null;
        self.allocator.free(self.os_bucket);
        self.os_bucket = &.{};
    }

    fn objectStore(self: *Transport, bucket: []const u8) !*nats.ObjectStore {
        if (self.os != null and std.mem.eql(u8, self.os_bucket, bucket)) return &self.os.?;
        self.dropObjectStore();
        const name = try self.allocator.dupe(u8, bucket);
        errdefer self.allocator.free(name);
        self.os = try nats.ObjectStore.init(self.allocator, self.js, bucket, 128 * 1024);
        self.os_bucket = name;
        return &self.os.?;
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
        const os = try self.objectStore(bucket);
        var res = try os.getBytes(name);
        defer res.deinit();
        return try a.dupe(u8, res.value);
    }

    /// JetStream publish; `msg_id` is the idempotency key (the envelope's).
    pub fn publish(self: *Transport, subject: []const u8, data: []const u8, msg_id: ?[]const u8) !void {
        var res = try self.js.publish(subject, data, .{ .msg_id = msg_id });
        res.deinit();
    }

    /// The last retained message on one subject, by direct get — or null when the
    /// stream holds none. The per-key form is what the client JWT grants
    /// (`DIRECT.GET.<stream>.<subject>`), as for `$KV.tenants`.
    pub fn lastBySubject(self: *Transport, stream: []const u8, subject: []const u8) !?*nats.Message {
        return self.js.getMsg(stream, .{ .last_by_subj = subject, .direct = true }) catch |err| switch (err) {
            error.MessageNotFound => null,
            else => err,
        };
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

