//! The transport shell: nats.zig behind the same surface transport.ts declares.
//!
//! Maps one to one: connect (creds file wins, like the TS credsAuthenticator),
//! JetStream publish with a Nats-Msg-Id, KV get, object-store get — the calls
//! the orchestration makes through `Transport` in TS. nkeys (enrollment key
//! generation, nonce signing) is nats.zig's own nkeys.zig; nothing to add.

const std = @import("std");
const log = std.log.scoped(.libzb);
const android_io = @import("android_io.zig"); // Android: names through getaddrinfo (§10ln)
const nats = @import("nats");

/// A list has one security mode. Never let a secure primary fall back to plaintext.
fn endpointTls(urls: []const u8) !bool {
    var secure: ?bool = null;
    var parts = std.mem.splitScalar(u8, urls, ',');
    while (parts.next()) |part| {
        const url = std.mem.trim(u8, part, " \t");
        if (url.len == 0) return error.InvalidUrl;
        const tls = if (std.mem.startsWith(u8, url, "tls://")) true else if (std.mem.startsWith(u8, url, "nats://") or std.mem.find(u8, url, "://") == null) false else return error.InvalidUrl;
        for (url) |ch| if (ch <= 0x20 or ch == 0x7f) return error.InvalidUrl;
        if (secure) |previous| if (previous != tls) return error.MixedEndpointSecurity;
        secure = tls;
    }
    return secure orelse error.InvalidUrl;
}

pub const ConnectOptions = struct {
    url: []const u8,
    /// Path to a .creds file (operator/JWT mode) — wins over user/password.
    creds_path: ?[]const u8 = null,
    /// The same credentials as TEXT, for a host that holds them in memory (nats.zig
    /// patch 19). Used when `creds_path` is not set; must outlive the connection.
    creds: ?[]const u8 = null,
    user: ?[]const u8 = null,
    password: ?[]const u8 = null,
    /// §10hm: the prefix of every inbox this connection generates — reply subjects,
    /// KV watchers, object reads, pull consumers. `_INBOX.<principal>` keeps them
    /// inside a `_INBOX.<principal>.>` grant; the library's default `_INBOX` is one
    /// space every principal listens in.
    inbox_prefix: []const u8 = "_INBOX",
    /// The JetStream domain to address — `$JS.<domain>.API.` instead of `$JS.API.` —
    /// when JetStream is reached across a leaf link. The /enroll payload's `js_domain`
    /// says whether the deployment has one; null is the server's own JetStream.
    js_domain: ?[]const u8 = null,
    /// A seeding leaf's JetStream domain: manifests and chain objects are read from its
    /// mirrors first (`seed_js`), everything else from `js_domain`. Null: no seeding leaf.
    seed_domain: ?[]const u8 = null,
    /// A PEM bundle of trusted root certificates for a `tls://` URL, in place of the
    /// system's: Zig reads no trust store on iOS, so a phone host ships its own.
    ca_file: ?[]const u8 = null,
};

test "endpoint lists keep one security mode and reject missing entries" {
    try std.testing.expect(try endpointTls("  tls://leaf.example:4222, tls://hub.example:4222"));
    try std.testing.expect(!try endpointTls("nats://leaf.example:4222,hub.example:4222"));
    try std.testing.expectError(error.MixedEndpointSecurity, endpointTls("tls://leaf.example,nats://hub.example"));
    try std.testing.expectError(error.MixedEndpointSecurity, endpointTls("leaf.example,tls://hub.example"));
    try std.testing.expectError(error.InvalidUrl, endpointTls("nats://leaf.example,,nats://hub.example"));
    try std.testing.expectError(error.InvalidUrl, endpointTls("nats://leaf.example,\nnats://hub.example"));
}

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
    /// The JetStream the object is read from: the hub's, or a seeding leaf's (`seed_js`).
    js: nats.JetStream,
    sub: *nats.PullSubscription,
    /// To reopen the reader where it stopped (see `read`): the transport, the object's
    /// stream and chunk subject, and the stream sequence of the next chunk (0: from the start).
    t: *Transport,
    stream: []const u8,
    chunk_subject: []const u8,
    next_seq: u64 = 0,
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
    const fetch_timeout: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(10_000), .clock = .awake } };
    const Ctr = struct {
        var n = std.atomic.Value(u32).init(0);
    };

    pub fn open(t: *Transport, a: std.mem.Allocator, bucket: []const u8, name: []const u8) !ObjectPull {
        return openIn(t, t.js, a, bucket, name);
    }

    /// The object's size from its meta message alone, no chunk consumer: what the delta
    /// path (core.preferDeltas) weighs against the CDC backlog. Absent or deleted:
    /// error.ObjectNotFound.
    pub fn sizeIn(js: nats.JetStream, a: std.mem.Allocator, bucket: []const u8, name: []const u8) !u64 {
        const stream = try std.fmt.allocPrint(a, "OBJ_{s}", .{bucket});
        const enc = try a.alloc(u8, std.base64.url_safe.Encoder.calcSize(name.len));
        _ = std.base64.url_safe.Encoder.encode(enc, name);
        const meta_subject = try std.fmt.allocPrint(a, "$O.{s}.M.{s}", .{ bucket, enc });
        const msg = js.getMsg(stream, .{ .last_by_subj = meta_subject, .direct = true }) catch |err| switch (err) {
            error.MessageNotFound => return error.ObjectNotFound,
            else => return err,
        };
        defer msg.deinit();
        const Info = struct { size: u64 = 0, deleted: bool = false };
        const info = try std.json.parseFromSliceLeaky(Info, a, msg.data, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        if (info.deleted) return error.ObjectNotFound;
        return info.size;
    }

    pub fn openIn(t: *Transport, js: nats.JetStream, a: std.mem.Allocator, bucket: []const u8, name: []const u8) !ObjectPull {
        const stream = try std.fmt.allocPrint(a, "OBJ_{s}", .{bucket});
        const enc = try a.alloc(u8, std.base64.url_safe.Encoder.calcSize(name.len));
        _ = std.base64.url_safe.Encoder.encode(enc, name);
        const meta_subject = try std.fmt.allocPrint(a, "$O.{s}.M.{s}", .{ bucket, enc });
        const msg = js.getMsg(stream, .{ .last_by_subj = meta_subject, .direct = true }) catch |err| switch (err) {
            error.MessageNotFound => return error.ObjectNotFound,
            else => return err,
        };
        defer msg.deinit();
        const Info = struct { nuid: []const u8, size: u64 = 0, chunks: u32 = 0, digest: []const u8 = "", deleted: bool = false };
        const info = (try std.json.parseFromSliceLeaky(Info, a, msg.data, .{ .ignore_unknown_fields = true, .allocate = .alloc_always }));
        if (info.deleted) return error.ObjectNotFound;
        const chunk_subject = try std.fmt.allocPrint(a, "$O.{s}.C.{s}", .{ bucket, info.nuid });
        const sub = try openChunks(js, a, stream, chunk_subject, 0);
        return .{ .a = a, .js = js, .sub = sub, .t = t, .stream = stream, .chunk_subject = chunk_subject, .chunks = info.chunks, .size = info.size, .digest = info.digest };
    }

    /// The chunk reader's pull consumer, from `start_seq` (0: the first chunk). Named like
    /// the CDC consumers (pid + counter), reaped by the server when idle: the client JWT has
    /// no CONSUMER.DELETE grant (see openConsumer). 5 min, not 30 s: a streamed seed reads a
    /// window, then sorts and applies it before reading on, and on a slow phone (a moto e20,
    /// eMMC) that took longer than 30 s — the consumer was reaped and the next read failed
    /// with NoResponders, the seed with it. `read` also reopens here if it happens anyway.
    fn openChunks(js: nats.JetStream, a: std.mem.Allocator, stream: []const u8, chunk_subject: []const u8, start_seq: u64) !*nats.PullSubscription {
        const cname = try std.fmt.allocPrint(a, "zbo{d}x{d}", .{ std.c.getpid(), Ctr.n.fetchAdd(1, .monotonic) });
        var cfg: nats.ConsumerConfig = .{ .ack_policy = .none, .deliver_policy = .all };
        if (start_seq > 0) {
            cfg.deliver_policy = .by_start_sequence;
            cfg.opt_start_seq = start_seq;
        }
        cfg.name = cname;
        cfg.durable_name = cname;
        cfg.inactive_threshold = 5 * 60 * std.time.ns_per_s;
        const sub = try js.pullSubscribe(chunk_subject, cname, .{ .stream = stream, .config = cfg });
        // §10iy: wait for the WHOLE batch. nats.zig's fetch returns 1 ms after its first
        // message (right for a CDC tail); over Wi-Fi the 8 chunks of a batch do not land
        // within a millisecond, so `read` got one, asked for eight more, and the server
        // ended up with dozens of requests in flight for one reader — 42 MB pending, cut
        // as a slow consumer, on an iPhone that never got past a third of the object.
        sub.idle_after_first = fetch_timeout;
        return sub;
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
                        self.next_seq = b.messages[self.bi].metadata.sequence.stream + 1;
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
            // The consumer can be gone (reaped while the caller applied a window, or a
            // lost connection): reopen it at the next chunk and read on — a few times in
            // a row at most, then the failure is real.
            var reopened: u8 = 0;
            self.batch = while (true) {
                break self.sub.fetch(want, fetch_timeout) catch |err| switch (err) {
                    error.NoResponders, error.ConsumerSequenceMismatch => {
                        if (reopened >= 5) return err;
                        reopened += 1;
                        log.warn("object reader: consumer gone at chunk {d} of {d} ({s}) — reopening from stream seq {d}", .{ self.chunk_index, self.chunks, @errorName(err), self.next_seq });
                        const fresh = try openChunks(self.js, self.a, self.stream, self.chunk_subject, self.next_seq);
                        self.sub.deinit();
                        self.sub = fresh;
                        continue;
                    },
                    else => return err,
                };
            };
            if (self.batch.?.messages.len == 0) return error.ObjectTruncated;
        }
        return 0;
    }

    /// The whole object, read through the same pull consumer and verified — for a
    /// step below the streaming threshold, where the whole-object apply is faster and
    /// the memory is small anyway.
    pub fn readAll(self: *ObjectPull, a: std.mem.Allocator) ![]u8 {
        const out = try a.alloc(u8, @intCast(self.size));
        var n: usize = 0;
        while (n < out.len) {
            const got = try self.read(out[n..]);
            if (got == 0) break;
            n += got;
        }
        if (n != out.len) return error.ObjectTruncated;
        try self.verify();
        return out;
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
    /// A seeding leaf's JetStream, on the same connection (ConnectOptions.seed_domain).
    seed_js: ?nats.JetStream = null,
    /// The object store handle for the bucket last read (a chain is one bucket, many
    /// objects). `ObjectStore.init` is four small string dupes, so this is a tidiness
    /// cache, not a hot-path one; what matters is that it is released in `deinit`.
    os: ?nats.ObjectStore = null,
    os_bucket: []u8 = &.{},

    pub fn connect(allocator: std.mem.Allocator, opts: ConnectOptions) !*Transport {
        const uses_tls = try endpointTls(opts.url);
        const self = try allocator.create(Transport);
        errdefer allocator.destroy(self);
        // ⚠️ A struct literal, so the defaulted fields (`os`, `os_bucket`) are set.
        // Field-by-field assignment leaves every unassigned field undefined.
        self.* = .{
            .allocator = allocator,
            .threaded = android_io.threaded(allocator),
            .conn = undefined,
            .js = undefined,
        };
        errdefer self.threaded.deinit();
        const io = android_io.io(&self.threaded);

        self.conn = try allocator.create(nats.Connection);
        errdefer allocator.destroy(self.conn);

        // The FIRST connection is retried, briefly, on a transient failure. nats.zig
        // reconnects only a connection that has been up once, and a server busy with a
        // crowd of clients connecting at once refuses some of them for a moment (storm
        // test, 500 clients: one ConnectionFailed, never retried). A refused identity or
        // a bad TLS setup is not retried: it would fail the same way. A failed
        // nats.Connection cannot connect again (ConnectionClosed), so each attempt is a
        // fresh one. The waits double from 250 ms, each with up to half of it again at
        // random, so a crowd does not come back all at once: 8 to 12 s in all.
        var attempt: u5 = 0;
        while (true) : (attempt += 1) {
            self.conn.* = nats.Connection.init(allocator, io, .{
                .user_creds = opts.creds_path,
                .user_creds_content = opts.creds,
                .user = opts.user,
                .password = opts.password,
                .inbox_prefix = opts.inbox_prefix,
                // Never give up on the server: nats.zig's default stops after 60 tries (about
                // two minutes), and a client whose NATS was down longer stayed dead after it
                // came back. The host decides when to stop, by closing.
                .reconnect = .{ .allow_reconnect = true, .max_reconnect = std.math.maxInt(u32) },
                // Only on a tls:// URL: TLS options on a nats:// URL would force TLS on a
                // server that does not speak it.
                .tls = if (opts.ca_file != null and uses_tls) .{ .ca_file = opts.ca_file } else null,
            });
            if (self.conn.connect(opts.url)) |_| break else |err| {
                self.conn.deinit();
                if (attempt >= first_connect_attempts or !transientConnectError(err)) return err;
                log.info("NATS refused the first connection ({s}): retry {d} of {d}", .{ @errorName(err), attempt + 1, first_connect_attempts });
                backoff(io, attempt);
            }
        }
        errdefer self.conn.deinit();
        self.js = self.conn.jetstream(.{ .domain = opts.js_domain });
        if (opts.seed_domain) |d| self.seed_js = self.conn.jetstream(.{ .domain = d });
        return self;
    }

    /// Retries of the first connection after the first attempt (see `connect`).
    pub const first_connect_attempts: u5 = 5;

    /// A failure that may pass by itself: the server busy, the network or the name lookup
    /// failing for a moment. Everything else (an identity refused, TLS, a bad URL) is final.
    pub fn transientConnectError(err: anyerror) bool {
        const transient = [_][]const u8{
            "ConnectionFailed",      "ConnectionRefused",  "ConnectionTimedOut", "Timeout",
            "NoServerAvailable",     "NetworkUnreachable", "HostUnreachable",    "NameServerFailure",
            "ConnectionResetByPeer", "BrokenPipe",
        };
        for (transient) |name| if (std.mem.eql(u8, @errorName(err), name)) return true;
        return false;
    }

    /// 250 ms × 2^attempt, plus up to half of that at random.
    pub fn backoff(io: std.Io, attempt: u5) void {
        const base_ms: u64 = @as(u64, 250) << @min(attempt, 6);
        var r: [2]u8 = undefined;
        io.random(&r);
        const jitter_ms = (@as(u64, std.mem.readInt(u16, &r, .little)) * (base_ms / 2)) / 65535;
        std.Io.Timeout.sleep(.{ .duration = .{ .raw = .fromNanoseconds((base_ms + jitter_ms) * std.time.ns_per_ms), .clock = .awake } }, io) catch {};
    }

    pub fn deinit(self: *Transport) void {
        self.dropObjectStore();
        self.conn.deinit();
        self.allocator.destroy(self.conn);
        self.threaded.deinit();
        self.allocator.destroy(self);
    }

    fn sameDomain(x: ?[]const u8, y: ?[]const u8) bool {
        if (x == null or y == null) return x == null and y == null;
        return std.mem.eql(u8, x.?, y.?);
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
    /// One key, by direct get. §10jr: BOUND, not opened — `kvBucket` asks STREAM.INFO first,
    /// and that grant lists every key of the bucket through its `subjects_filter`, so the
    /// client templates no longer carry it for the key-scoped buckets (tenants,
    /// generations). A missing bucket now reads as a missing key.
    pub fn kvGet(self: *Transport, a: std.mem.Allocator, bucket: []const u8, key: []const u8) !?[]u8 {
        return kvGetIn(self.js, a, bucket, key);
    }

    /// `kvGet` in a given JetStream: the hub's, or a seeding leaf's (`seed_js`).
    pub fn kvGetIn(js: nats.JetStream, a: std.mem.Allocator, bucket: []const u8, key: []const u8) !?[]u8 {
        var kv = try js.kvBind(bucket);
        defer kv.deinit();
        // Only a key that is not there is "no value". An unreachable hub (NoResponders, a
        // timeout: a leaf cut off from it) is an error the caller sees: read as "absent",
        // it marked a tenant-scoped client as having no tenant for the whole session.
        var entry = kv.get(key) catch |err| return if (err == error.KeyNotFound) null else err;
        defer entry.deinit();
        return try a.dupe(u8, entry.value);
    }

    /// §10hn: every key of a KV bucket, `a`-owned — the schemas bucket, for a client
    /// that follows every published table (`tables: "*"`). nats.zig lists through a
    /// headers-only watch; the grant every client holds covers it.
    pub fn kvKeys(self: *Transport, a: std.mem.Allocator, bucket: []const u8) ![]const []const u8 {
        var kv = try self.js.kvBucket(bucket);
        defer kv.deinit();
        var res = try kv.keys();
        defer res.deinit();
        const out = try a.alloc([]const u8, res.value.len);
        for (res.value, 0..) |k, i| out[i] = try a.dupe(u8, k);
        return out;
    }

    /// Object store get: the whole object, `a`-owned. The store chunks at
    /// 128 KiB — no NATS max_payload limit applies to a seed (§10n).
    pub fn objectGetBytes(self: *Transport, a: std.mem.Allocator, bucket: []const u8, name: []const u8) ![]u8 {
        return self.objectGetBytesIn(self.js, a, bucket, name);
    }

    /// `objectGetBytes` in a given JetStream. The hub's store handle is cached; a seeding
    /// leaf's is made for the read (four string dupes).
    pub fn objectGetBytesIn(self: *Transport, js: nats.JetStream, a: std.mem.Allocator, bucket: []const u8, name: []const u8) ![]u8 {
        if (!sameDomain(js.opts.domain, self.js.opts.domain)) {
            var store = try nats.ObjectStore.init(self.allocator, js, bucket, 128 * 1024);
            defer store.deinit();
            var res = try store.getBytes(name);
            defer res.deinit();
            return try a.dupe(u8, res.value);
        }
        const os = try self.objectStore(bucket);
        var res = try os.getBytes(name);
        defer res.deinit();
        return try a.dupe(u8, res.value);
    }

    /// §10hq: write one object into a bucket that may not exist yet. The bucket is
    /// created with `max_age_ns` so the answers in it expire on their own: nobody
    /// sweeps a result store. Creating a bucket that exists is not an error here —
    /// the create is attempted once and its refusal ignored, which is cheaper than a
    /// STREAM.INFO on every large answer.
    pub fn objectPutBytes(self: *Transport, bucket: []const u8, name: []const u8, data: []const u8, max_age_ns: u64) !void {
        var mgr = self.js.objectStoreManager();
        if (mgr.createStore(.{ .store_name = bucket, .max_age_ns = max_age_ns, .description = "ZeBridge answers (§10hq)" })) |store| {
            var s = store;
            s.deinit();
        } else |_| {}
        const os = try self.objectStore(bucket);
        var res = try os.putBytes(name, data);
        res.deinit();
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

    pub const WindowWrite = struct { subject: []const u8, data: []const u8, msg_id: []const u8 };

    /// A window of JetStream publishes, every write sent before any PubAck is awaited:
    /// `publish` waited for each ack in turn, so one client wrote at most one write per
    /// round trip to its server (26/s at 23 ms). Each write keeps its `Nats-Msg-Id` and has
    /// its own reply subject under `_INBOX.<principal>.` (the only replies a client is
    /// granted, §10hm); `results[i]` is null for a stored write (a duplicate included) and
    /// an error otherwise — no responders, a JetStream refusal, or no ack before the
    /// timeout. A write that fails stays in the outbox for its retry, as before.
    ///
    /// `await_acks == false` publishes with no reply subject and returns at once (`mutate`): the
    /// outbox never depended on the ack (the retry deadline is set before the send, the verdict
    /// settles the entry), and JetStream still deduplicates by the header. A write that was
    /// not stored is sent again at its deadline, like a lost publish.
    pub fn publishWindow(self: *Transport, a: std.mem.Allocator, principal: []const u8, writes: []const WindowWrite, results: []?anyerror, timeout_ms: i64, await_acks: bool) !void {
        if (!await_acks) {
            @memset(results, null);
            for (writes, 0..) |w, i| {
                const msg = try self.conn.newMsg();
                defer msg.deinit();
                try msg.setSubject(w.subject, false);
                try msg.setPayload(w.data, false);
                try msg.headerSet("Nats-Msg-Id", w.msg_id);
                self.conn.publishMsg(msg) catch |err| {
                    for (results[i..]) |*r| r.* = err;
                    return;
                };
            }
            return;
        }
        @memset(results, error.Timeout);
        const fresh = try self.conn.newInbox(a);
        defer a.free(fresh);
        const nonce = fresh[(std.mem.findScalarLast(u8, fresh, '.') orelse return error.MalformedInbox) + 1 ..];
        const base = try std.fmt.allocPrint(a, "_INBOX.{s}.pa.{s}", .{ principal, nonce });
        defer a.free(base);
        const filter = try std.fmt.allocPrint(a, "{s}.*", .{base});
        defer a.free(filter);
        const sub = try self.conn.subscribeSync(filter);
        defer sub.deinit();
        sub.setPendingLimits(@intCast(writes.len + 1), 8 << 20);
        var sent: usize = 0;
        for (writes, 0..) |w, i| {
            const msg = try self.conn.newMsg();
            defer msg.deinit();
            try msg.setSubject(w.subject, false);
            try msg.setPayload(w.data, false);
            try msg.headerSet("Nats-Msg-Id", w.msg_id);
            const reply = try std.fmt.allocPrint(a, "{s}.{d}", .{ base, i });
            defer a.free(reply);
            self.conn.publishRequestMsg(msg, reply) catch |err| {
                for (results[i..]) |*r| r.* = err;
                break;
            };
            sent += 1;
        }
        const received = try a.alloc(bool, sent);
        var acks: WindowAcks = .{ .results = results[0..sent], .received = received, .deadline_us = monoUs() + timeout_ms * 1000 };
        acks.reset();
        while (!acks.done()) {
            // One deadline for the whole window: each receive waits only for what is left of
            // it, so a trickle of replies cannot hold the polling thread for minutes.
            const left_ms = acks.leftMs(monoUs()) orelse break;
            const m = sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromMilliseconds(left_ms), .clock = .awake } }) catch break;
            defer m.deinit();
            const i = std.fmt.parseInt(usize, m.subject[(std.mem.findScalarLast(u8, m.subject, '.') orelse continue) + 1 ..], 10) catch continue;
            _ = acks.take(i, ackOutcome(m.isNoResponders(), m.data));
        }
    }

    /// What one PubAck says: stored (a duplicate included), no responders, or refused.
    fn ackOutcome(no_responders: bool, data: []const u8) ?anyerror {
        if (no_responders) return error.NoStreamResponse;
        if (std.mem.find(u8, data, "\"error\"") != null or std.mem.find(u8, data, "\"seq\"") == null) return error.JetStreamRefused;
        return null;
    }

    /// The window's replies: each index is received at most once, whatever its outcome, so a
    /// repeated reply (an error included) cannot stand in for another write's missing one.
    pub const WindowAcks = struct {
        results: []?anyerror,
        received: []bool,
        deadline_us: i64,
        count: usize = 0,

        fn reset(self: *WindowAcks) void {
            @memset(self.results, error.Timeout);
            @memset(self.received, false);
        }

        /// True when this reply was the index's first.
        fn take(self: *WindowAcks, i: usize, outcome: ?anyerror) bool {
            if (i >= self.received.len or self.received[i]) return false;
            self.received[i] = true;
            self.count += 1;
            self.results[i] = outcome;
            return true;
        }

        fn done(self: *const WindowAcks) bool {
            return self.count == self.received.len;
        }

        fn leftMs(self: *const WindowAcks, now_us: i64) ?i64 {
            const left = self.deadline_us - now_us;
            return if (left <= 0) null else @max(1, @divTrunc(left, 1000));
        }
    };

    /// Query replies name the tenant in the inbox, allowing a restricted leaf to
    /// answer that tenant's requests without permission to publish all inboxes.
    pub fn requestQuery(self: *Transport, a: std.mem.Allocator, principal: []const u8, prefix: []const u8, subject: []const u8, payload: []const u8, timeout: std.Io.Timeout) !*nats.Message {
        var parts = std.mem.splitScalar(u8, subject, '.');
        if (!std.mem.eql(u8, parts.next() orelse "", prefix)) return self.conn.request(subject, payload, timeout);
        const tenant = parts.next() orelse return error.MalformedQuerySubject;
        if (tenant.len == 0 or std.mem.findAny(u8, tenant, "*> \t\r\n") != null or parts.next() == null) return error.MalformedQuerySubject;
        const fresh = try self.conn.newInbox(a);
        defer a.free(fresh);
        const nonce = fresh[(std.mem.findScalarLast(u8, fresh, '.') orelse return error.MalformedInbox) + 1 ..];
        const reply = try std.fmt.allocPrint(a, "_INBOX.{s}.query.{s}.{s}", .{ principal, tenant, nonce });
        defer a.free(reply);
        const sub = try self.conn.subscribeSync(reply);
        defer sub.deinit();
        sub.setPendingLimits(1, 2 << 20);
        try self.conn.publishRequest(subject, reply, payload);
        const msg = try sub.nextMsgTimeout(timeout);
        if (msg.isNoResponders()) {
            msg.deinit();
            return error.NoResponders;
        }
        return msg;
    }

    /// Core subscription (the verdict channel `mutation_ack.<principal>.>`).
    /// §10hp: a QUEUE subscription the caller drains itself. The async form would
    /// deliver on the reader's thread, which a host-driven client does not own; this
    /// one is emptied by `poll`, like everything else libzb does.
    pub fn queueSubscribeSync(self: *Transport, subject: []const u8, queue: []const u8) !*nats.Subscription {
        return self.conn.queueSubscribeSync(subject, queue);
    }

    /// A CORE publish, not JetStream: nothing is awaited. For a reply (it goes to the
    /// asker's inbox, where no stream is listening and no ack is coming) and for the
    /// fleet heartbeat (a report: the stream stores it, nobody waits for the ack).
    pub fn publishCore(self: *Transport, subject: []const u8, data: []const u8) !void {
        try self.conn.publish(subject, data);
    }

    pub fn subscribeSync(self: *Transport, subject: []const u8) !*nats.Subscription {
        return self.conn.subscribeSync(subject);
    }
};

fn monoUs() i64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * 1_000_000 + @divTrunc(@as(i64, ts.nsec), 1000);
}

test "WindowAcks: each write counted once; a duplicate error cannot complete the window; missing replies stay Timeout" {
    var results: [4]?anyerror = undefined;
    var received: [4]bool = undefined;
    var w: Transport.WindowAcks = .{ .results = &results, .received = &received, .deadline_us = 1_000_000 };
    w.reset();
    try std.testing.expect(w.take(0, error.JetStreamRefused));
    try std.testing.expect(!w.take(0, error.JetStreamRefused)); // the same error again
    try std.testing.expect(!w.take(0, null)); // and a late success for it: still the first reply's outcome
    try std.testing.expect(w.take(1, null));
    try std.testing.expect(!w.take(1, null));
    try std.testing.expect(!w.take(9, null)); // an index outside the window
    try std.testing.expect(!w.done()); // 2 and 3 never answered
    try std.testing.expectEqual(@as(?anyerror, error.JetStreamRefused), results[0]);
    try std.testing.expectEqual(@as(?anyerror, null), results[1]);
    try std.testing.expectEqual(@as(?anyerror, error.Timeout), results[2]);
    try std.testing.expectEqual(@as(?anyerror, error.Timeout), results[3]);
    try std.testing.expect(w.take(3, error.NoStreamResponse));
    try std.testing.expect(w.take(2, null));
    try std.testing.expect(w.done());
}

test "WindowAcks: one deadline for the window, not one per reply" {
    var results: [1]?anyerror = undefined;
    var received: [1]bool = undefined;
    const w: Transport.WindowAcks = .{ .results = &results, .received = &received, .deadline_us = 5_000_000 };
    try std.testing.expectEqual(@as(?i64, 5_000), w.leftMs(0));
    try std.testing.expectEqual(@as(?i64, 1_000), w.leftMs(4_000_000));
    try std.testing.expectEqual(@as(?i64, 1), w.leftMs(4_999_999)); // under a millisecond left: wait 1 ms, not 0 (no wait)
    try std.testing.expectEqual(@as(?i64, null), w.leftMs(5_000_000));
}

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
