//! Fleet observability (NOTES §10dc). The bridge sees its slot and its own lag; it
//! never saw its CLIENTS. Now each client beats into a TTL'd KV bucket:
//!
//!     key    <tenant>.<principal>
//!     value  {"principal":"omar","tenant":"acme","ts":<unix ms>,
//!             "streams":{"CDC_acme":1234,"CDC_PUBLIC":56}}      (applied seq per stream)
//!
//! and this thread reads the WHOLE bucket on its own slow cadence, asks JetStream for
//! each stream's head, and renders `head − applied` per client per stream: the metric
//! an operator wants ("who is falling behind"), next to plain liveness. Cooperative by
//! design — a dead client just stops beating and the TTL drops it, which is precisely
//! what is being detected. Bounded — last value per key, one entry per live client.
//! The bridge never writes the bucket; it creates it if missing (TTL from
//! FLEET_TTL_SECONDS) and reads it.
const std = @import("std");
const nats = @import("nats");
const config = @import("config.zig");
const topology_mod = @import("topology.zig");
const utils = @import("utils.zig");

pub const log = std.log.scoped(.fleet);

pub const StreamLag = struct { stream: []const u8, applied: u64, head: u64, lag: u64 };

/// What a CDC stream HOLDS, read on the same pass (§10eg): now minus its oldest
/// message is the window a client can fall behind and still resume; the chain's
/// newest cutoff is at most one cadence old, so the floor is two cadences. `short`
/// is that floor breached on a stream that has PRUNED (`first_seq > 1`) — a young
/// stream's small window is age, not loss.
pub const StreamWindow = struct {
    stream: []const u8,
    messages: u64,
    bytes: u64,
    first_seq: u64,
    /// -1 when the stream is empty.
    window_seconds: i64,
    short: bool,
};

pub const Client = struct {
    tenant: []const u8,
    principal: []const u8,
    /// Seconds since the client's own `ts`; -1 when the beat carried none.
    age_seconds: i64,
    streams: []const StreamLag,
};

/// Snapshot-swapped under a mutex, same shape as wal_monitor.SlotRegistry: the monitor
/// thread builds a fresh arena and swaps it in whole; /metrics renders the current one.
pub const Registry = struct {
    allocator: std.mem.Allocator,
    mutex: utils.SpinLock = .{},
    arena: ?*std.heap.ArenaAllocator = null,
    clients: []const Client = &.{},
    streams: []const StreamWindow = &.{},
    polled_at_unix: i64 = 0,

    pub fn init(allocator: std.mem.Allocator) Registry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Registry) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.arena) |ar| {
            ar.deinit();
            self.allocator.destroy(ar);
            self.arena = null;
            self.clients = &.{};
            self.streams = &.{};
        }
    }

    fn swap(self: *Registry, arena: *std.heap.ArenaAllocator, clients: []const Client, streams: []const StreamWindow, now_unix: i64) void {
        self.mutex.lock();
        const old = self.arena;
        self.arena = arena;
        self.clients = clients;
        self.streams = streams;
        self.polled_at_unix = now_unix;
        self.mutex.unlock();
        if (old) |o| {
            o.deinit();
            self.allocator.destroy(o);
        }
    }

    pub fn writePrometheus(self: *Registry, w: *std.Io.Writer) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try w.print("# HELP bridge_fleet_poll_timestamp_seconds Unix time the clients' heartbeat bucket was last read (0 = never)\n", .{});
        try w.print("# TYPE bridge_fleet_poll_timestamp_seconds gauge\n", .{});
        try w.print("bridge_fleet_poll_timestamp_seconds {d}\n", .{self.polled_at_unix});

        // Per-tenant live count, always emitted (a tenant with zero live clients is a
        // fact worth graphing, but a tenant we have never heard of is not — only the
        // tenants present in the bucket appear).
        var counts: std.StringArrayHashMapUnmanaged(usize) = .empty;
        defer counts.deinit(self.allocator);
        for (self.clients) |cl| {
            const gop = try counts.getOrPut(self.allocator, cl.tenant);
            if (!gop.found_existing) gop.value_ptr.* = 0;
            gop.value_ptr.* += 1;
        }
        try w.print("# HELP bridge_fleet_clients_live Clients whose heartbeat is inside the live bucket's TTL, per tenant\n", .{});
        try w.print("# TYPE bridge_fleet_clients_live gauge\n", .{});
        try w.print("bridge_fleet_clients_live_total {d}\n", .{self.clients.len});
        for (counts.keys(), counts.values()) |tenant, n| {
            try w.print("bridge_fleet_clients_live{{tenant=\"{s}\"}} {d}\n", .{ tenant, n });
        }
        if (self.streams.len > 0) {
            try w.print("# HELP bridge_cdc_window_seconds Seconds of events the CDC stream holds (now minus its oldest message); -1 when empty. A client further behind re-seeds from the chain, whose newest cutoff is at most one cadence old\n", .{});
            try w.print("# TYPE bridge_cdc_window_seconds gauge\n", .{});
            for (self.streams) |sw| try w.print("bridge_cdc_window_seconds{{stream=\"{s}\"}} {d}\n", .{ sw.stream, sw.window_seconds });
            try w.print("# HELP bridge_cdc_window_short 1 when the stream has pruned and holds less than 2 × cadence: a size or message valve ends the window before the age, or CDC_MAX_AGE_SECONDS is too short\n", .{});
            try w.print("# TYPE bridge_cdc_window_short gauge\n", .{});
            for (self.streams) |sw| try w.print("bridge_cdc_window_short{{stream=\"{s}\"}} {d}\n", .{ sw.stream, @as(u8, if (sw.short) 1 else 0) });
            try w.print("# HELP bridge_cdc_stream_bytes Bytes the CDC stream holds\n", .{});
            try w.print("# TYPE bridge_cdc_stream_bytes gauge\n", .{});
            for (self.streams) |sw| try w.print("bridge_cdc_stream_bytes{{stream=\"{s}\"}} {d}\n", .{ sw.stream, sw.bytes });
            try w.print("# HELP bridge_cdc_stream_messages Messages the CDC stream holds\n", .{});
            try w.print("# TYPE bridge_cdc_stream_messages gauge\n", .{});
            for (self.streams) |sw| try w.print("bridge_cdc_stream_messages{{stream=\"{s}\"}} {d}\n", .{ sw.stream, sw.messages });
        }
        if (self.clients.len == 0) return;

        try w.print("# HELP bridge_fleet_client_last_seen_seconds Seconds since the client's last heartbeat, as of the last poll\n", .{});
        try w.print("# TYPE bridge_fleet_client_last_seen_seconds gauge\n", .{});
        for (self.clients) |cl| {
            try w.print("bridge_fleet_client_last_seen_seconds{{tenant=\"{s}\",principal=\"{s}\"}} {d}\n", .{ cl.tenant, cl.principal, cl.age_seconds });
        }
        try w.print("# HELP bridge_fleet_client_lag_events Stream head minus the client's applied sequence, in stream MESSAGES (a published batch counts one) — how far behind the client is, per CDC stream\n", .{});
        try w.print("# TYPE bridge_fleet_client_lag_events gauge\n", .{});
        for (self.clients) |cl| {
            for (cl.streams) |sl| {
                try w.print("bridge_fleet_client_lag_events{{tenant=\"{s}\",principal=\"{s}\",stream=\"{s}\"}} {d}\n", .{ cl.tenant, cl.principal, sl.stream, sl.lag });
            }
        }
    }
};

pub const FleetMonitor = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    endpoint: config.Nats.Endpoint,
    topo: *const topology_mod.Topology,
    should_stop: *std.atomic.Value(bool),
    poll_seconds: u64,
    ttl_seconds: u64,
    /// Two generation cadences: the least a CDC stream must hold (§10eg).
    min_window_seconds: u64,
    registry: Registry,
    thread: ?std.Thread = null,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        endpoint: config.Nats.Endpoint,
        topo: *const topology_mod.Topology,
        should_stop: *std.atomic.Value(bool),
        poll_seconds: u64,
        ttl_seconds: u64,
        min_window_seconds: u64,
    ) FleetMonitor {
        return .{
            .allocator = allocator,
            .io = io,
            .endpoint = endpoint,
            .topo = topo,
            .should_stop = should_stop,
            .poll_seconds = poll_seconds,
            .ttl_seconds = ttl_seconds,
            .min_window_seconds = min_window_seconds,
            .registry = Registry.init(allocator),
        };
    }

    pub fn start(self: *FleetMonitor) !void {
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    pub fn join(self: *FleetMonitor) void {
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    pub fn deinit(self: *FleetMonitor) void {
        self.registry.deinit();
    }

    fn run(self: *FleetMonitor) void {
        log.info("👁️  fleet monitor started: reading $KV.{s} every {d}s (bucket TTL {d}s)", .{ self.topo.kv_live, self.poll_seconds, self.ttl_seconds });
        while (!self.should_stop.load(.seq_cst)) {
            self.tick() catch |err| {
                log.warn("⚠️ fleet poll failed: {s} — the previous snapshot stands", .{@errorName(err)});
            };
            var remaining = self.poll_seconds;
            while (remaining > 0 and !self.should_stop.load(.seq_cst)) : (remaining -= 1) {
                utils.sleep(1 * std.time.ns_per_s);
            }
        }
        log.info("👁️  fleet monitor stopped", .{});
    }

    /// One pass: a fresh connection (the cadence is minutes; keeping a socket open
    /// between passes would only be one more thing to reconnect), the whole bucket,
    /// one STREAM.INFO per distinct stream named by any client, then swap.
    fn tick(self: *FleetMonitor) !void {
        const arena = try self.allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        errdefer {
            arena.deinit();
            self.allocator.destroy(arena);
        }
        const a = arena.allocator();

        var conn = nats.Connection.init(self.allocator, self.io, .{
            .user = self.endpoint.user,
            .password = self.endpoint.pass,
            .nkey_seed = self.endpoint.seed,
            .user_creds = self.endpoint.creds,
        });
        defer conn.deinit();
        const url = try std.fmt.allocPrint(a, "nats://{s}:{d}", .{ self.endpoint.host, self.endpoint.port });
        try conn.connect(url);
        const js = conn.jetstream(.{});

        var kv = try self.openLive(js);
        defer kv.deinit();

        var keys = kv.keys() catch |err| switch (err) {
            error.KeyNotFound => null, // an empty bucket: nobody is beating
            else => return err,
        };
        defer if (keys) |*k| k.deinit();

        var heads: std.StringArrayHashMapUnmanaged(u64) = .empty;
        var clients: std.ArrayList(Client) = .empty;
        const now_ms = utils.unixMillis();

        // The streams first: one STREAM.INFO each gives the window AND the head the
        // lag rows below need, so a client's stream costs nothing more.
        var windows: std.ArrayList(StreamWindow) = .empty;
        try self.windowOf(js, &heads, &windows, a, self.topo.cdc_stream_public, now_ms);
        for (self.topo.tenants) |tenant| {
            if (std.mem.eql(u8, tenant, self.topo.open_tenant)) continue;
            const name = try std.fmt.allocPrint(a, "{s}{s}", .{ self.topo.cdc_stream_prefix, tenant });
            try self.windowOf(js, &heads, &windows, a, name, now_ms);
        }

        if (keys) |k| for (k.value) |key| {
            var entry = kv.get(key) catch continue;
            defer entry.deinit();
            const parsed = std.json.parseFromSliceLeaky(std.json.Value, a, entry.value, .{}) catch continue;
            if (parsed != .object) continue;
            const o = parsed.object;
            // The key is the identity: `<tenant>.<principal>` — both subject tokens, so
            // the first dot is the boundary. The payload repeats them for readers of the
            // bucket itself; the key is what the JWT grant scoped.
            const dot = std.mem.indexOfScalar(u8, key, '.') orelse continue;
            const tenant = try a.dupe(u8, key[0..dot]);
            const principal = try a.dupe(u8, key[dot + 1 ..]);
            const ts: i64 = if (o.get("ts")) |v| (if (v == .integer) v.integer else 0) else 0;
            const age: i64 = if (ts > 0) @divFloor(now_ms - ts, 1000) else -1;
            var lags: std.ArrayList(StreamLag) = .empty;
            if (o.get("streams")) |sv| if (sv == .object) {
                var it = sv.object.iterator();
                while (it.next()) |e| {
                    const applied: u64 = if (e.value_ptr.* == .integer and e.value_ptr.integer >= 0) @intCast(e.value_ptr.integer) else 0;
                    const head = try self.headOf(js, &heads, a, e.key_ptr.*);
                    try lags.append(a, .{ .stream = try a.dupe(u8, e.key_ptr.*), .applied = applied, .head = head, .lag = head -| applied });
                }
            };
            try clients.append(a, .{ .tenant = tenant, .principal = principal, .age_seconds = age, .streams = lags.items });
        };

        self.registry.swap(arena, clients.items, windows.items, @divFloor(now_ms, 1000));
        log.debug("fleet poll: {d} live client(s), {d} stream window(s)", .{ clients.items.len, windows.items.len });
    }

    /// One STREAM.INFO: the head for the lag rows and the window the stream holds.
    /// A stream that does not exist yet (a tenant enrolled before its first row) is
    /// skipped, not failed. Short windows are SAID every poll while they last: a
    /// breached floor is not routine.
    fn windowOf(self: *FleetMonitor, js: nats.JetStream, heads: *std.StringArrayHashMapUnmanaged(u64), windows: *std.ArrayList(StreamWindow), a: std.mem.Allocator, stream: []const u8, now_ms: i64) !void {
        var info = js.getStreamInfo(stream) catch return;
        defer info.deinit();
        const st = info.value.state;
        const name = try a.dupe(u8, stream);
        try heads.put(a, name, st.last_seq);
        const first_unix: ?i64 = if (st.messages > 0) unixSecondsOf(st.first_ts) else null;
        const window: i64 = if (first_unix) |f| @max(0, @divFloor(now_ms, 1000) - f) else -1;
        const short = st.messages > 0 and st.first_seq > 1 and window >= 0 and window < @as(i64, @intCast(self.min_window_seconds));
        if (short) log.warn("⚠️ {s} holds {d}s of events, under the 2 × cadence floor of {d}s: a client that falls off this stream can find a chain that predates it — CDC_MAX_BYTES or CDC_MAX_MSGS is ending the window before the age, or CDC_MAX_AGE_SECONDS is too short", .{ stream, window, self.min_window_seconds });
        try windows.append(a, .{ .stream = name, .messages = st.messages, .bytes = st.bytes, .first_seq = st.first_seq, .window_seconds = window, .short = short });
    }

    fn headOf(self: *FleetMonitor, js: nats.JetStream, heads: *std.StringArrayHashMapUnmanaged(u64), a: std.mem.Allocator, stream: []const u8) !u64 {
        _ = self;
        if (heads.get(stream)) |h| return h;
        var info = js.getStreamInfo(stream) catch {
            // A stream the client names that does not exist here (a tenant stream
            // gone, a typo): lag is unknowable, report 0 rather than fail the pass.
            try heads.put(a, try a.dupe(u8, stream), 0);
            return 0;
        };
        defer info.deinit();
        const h: u64 = info.value.state.last_seq;
        try heads.put(a, try a.dupe(u8, stream), h);
        return h;
    }

    /// Bind to the bucket; create it with the configured TTL only when it is missing.
    /// A bucket that already exists keeps ITS TTL — the setting is applied at creation.
    /// Open FIRST: creating over an existing bucket is refused by the server (10058,
    /// "already in use with a different configuration") and the library logs that
    /// refusal at error level, so create-then-open printed a misleading boot error
    /// on every start (§10dr).
    fn openLive(self: *FleetMonitor, js: nats.JetStream) !nats.KV {
        const km = js.kvManager();
        return km.openBucket(self.topo.kv_live) catch |err| switch (err) {
            error.BucketNotFound => km.createBucket(.{
                .bucket = self.topo.kv_live,
                .history = 1,
                .ttl = .fromNanoseconds(@intCast(self.ttl_seconds * std.time.ns_per_s)),
            }),
            else => err,
        };
    }
};

/// "2026-09-10T08:03:12.608794123Z" → Unix seconds: JetStream's own timestamp shape
/// (`first_ts`, `last_ts`). The fraction is ignored — a window is measured in
/// seconds. Days from the civil date by Howard Hinnant's algorithm.
fn unixSecondsOf(ts: []const u8) ?i64 {
    if (ts.len < 19) return null;
    const y = std.fmt.parseInt(i64, ts[0..4], 10) catch return null;
    const m = std.fmt.parseInt(i64, ts[5..7], 10) catch return null;
    const d = std.fmt.parseInt(i64, ts[8..10], 10) catch return null;
    const hh = std.fmt.parseInt(i64, ts[11..13], 10) catch return null;
    const mm = std.fmt.parseInt(i64, ts[14..16], 10) catch return null;
    const ss = std.fmt.parseInt(i64, ts[17..19], 10) catch return null;
    if (m < 1 or m > 12 or d < 1 or d > 31) return null;
    const yy = if (m <= 2) y - 1 else y;
    const era = @divFloor(yy, 400);
    const yoe = yy - era * 400;
    const mp = @mod(m + 9, 12);
    const doy = @divFloor(153 * mp + 2, 5) + d - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    const days = era * 146097 + doe - 719468;
    return days * 86400 + hh * 3600 + mm * 60 + ss;
}

test "unixSecondsOf reads JetStream timestamps" {
    try std.testing.expectEqual(@as(?i64, 0), unixSecondsOf("1970-01-01T00:00:00Z"));
    try std.testing.expectEqual(@as(?i64, 951868800), unixSecondsOf("2000-03-01T00:00:00Z"));
    try std.testing.expectEqual(@as(?i64, 1789027392), unixSecondsOf("2026-09-10T08:03:12.608794123Z"));
    try std.testing.expectEqual(@as(?i64, null), unixSecondsOf("0001-01-01"));
}
