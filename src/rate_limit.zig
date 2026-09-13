//! §10fk: the mutation ingress rate limit — a token bucket per principal and per tenant.
//!
//! One phone in a loop, or one hostile credential, can flood `mutation.>` and put every
//! other writer's verdict behind its backlog: the ingress lanes pull in order, and a
//! write is not refused by being slow. The bucket is the answer at the door: a
//! principal (and its tenant) has `burst` tokens and earns `rate_per_s` more each
//! second. A write with no token is NAK'd with a delay — JetStream redelivers it
//! then, once — and the bucket goes into DEBT for it: the k-th refused write waits
//! k steps, so a flood is serialised at the rate by the server's own scheduling,
//! each message redelivered once, at the time its token exists. Not acknowledged
//! with a "retry later" verdict, because the client's re-publish of the same
//! `Nats-Msg-Id` inside the stream's duplicate window (120 s) is dropped silently:
//! a client-side retry cannot work; a server-side delay can. Only past the delivery
//! limit is a write answered `failed`/`rate_limited` with `retry_after_ms`.
//!
//! Shared by every ingress lane (one instance, a spinlock — `std.Thread.Mutex` does not
//! exist in Zig 0.16, and every operation here is a few arithmetic steps), so the limit
//! is per principal across lanes, not per lane. `rate_per_s == 0` is off.

const std = @import("std");

const SpinLock = struct {
    v: std.atomic.Value(bool) = .init(false),
    fn lock(self: *SpinLock) void {
        while (self.v.swap(true, .acquire)) std.atomic.spinLoopHint();
    }
    fn unlock(self: *SpinLock) void {
        self.v.store(false, .release);
    }
};

pub const Limiter = struct {
    allocator: std.mem.Allocator,
    rate_per_s: f64,
    burst: f64,
    lock: SpinLock = .{},
    buckets: std.StringHashMapUnmanaged(Bucket) = .empty,
    /// Writes refused so far, for the log and a future metric.
    limited_total: std.atomic.Value(u64) = .init(0),

    const Bucket = struct { tokens: f64, at_ms: i64 };

    /// `burst` 0 means "the rate": one second's worth.
    pub fn init(allocator: std.mem.Allocator, rate_per_s: u32, burst: u32) Limiter {
        return .{
            .allocator = allocator,
            .rate_per_s = @floatFromInt(rate_per_s),
            .burst = @floatFromInt(if (burst == 0) rate_per_s else burst),
        };
    }

    pub fn deinit(self: *Limiter) void {
        var it = self.buckets.iterator();
        while (it.next()) |e| self.allocator.free(e.key_ptr.*);
        self.buckets.deinit(self.allocator);
    }

    pub fn enabled(self: *const Limiter) bool {
        return self.rate_per_s > 0;
    }

    /// One token from `key`'s bucket at `now_ms`. Null when taken; otherwise the
    /// milliseconds until this write's token exists — the bucket owes it one now
    /// (debt), so later refusals in the same flood queue behind it. The refused
    /// write is charged NOW: its redelivery must not take a second token.
    pub fn take(self: *Limiter, key: []const u8, now_ms: i64) ?u64 {
        if (!self.enabled()) return null;
        self.lock.lock();
        defer self.lock.unlock();
        return self.takeLocked(key, now_ms);
    }

    /// One token from each of two buckets — the principal's, then the tenant's —
    /// atomically: a write the tenant's bucket refuses gives the principal's token
    /// back, so a refused write costs nothing and the wait reported is the longer.
    pub fn takeTwo(self: *Limiter, first: []const u8, second: []const u8, now_ms: i64) ?u64 {
        if (!self.enabled()) return null;
        self.lock.lock();
        defer self.lock.unlock();
        if (self.takeLocked(first, now_ms)) |wait| return wait;
        if (self.takeLocked(second, now_ms)) |wait| {
            if (self.buckets.getPtr(first)) |b| b.tokens = @min(self.burst, b.tokens + 1);
            return wait;
        }
        return null;
    }

    fn takeLocked(self: *Limiter, key: []const u8, now_ms: i64) ?u64 {
        const gop = self.buckets.getOrPut(self.allocator, key) catch return null; // out of memory: let it through
        if (!gop.found_existing) {
            gop.key_ptr.* = self.allocator.dupe(u8, key) catch {
                _ = self.buckets.remove(key);
                return null;
            };
            gop.value_ptr.* = .{ .tokens = self.burst, .at_ms = now_ms };
        }
        const b = gop.value_ptr;
        const elapsed_s: f64 = @as(f64, @floatFromInt(@max(now_ms - b.at_ms, 0))) / 1000.0;
        b.tokens = @min(self.burst, b.tokens + elapsed_s * self.rate_per_s);
        b.at_ms = now_ms;
        if (b.tokens >= 1.0) {
            b.tokens -= 1.0;
            return null;
        }
        _ = self.limited_total.fetchAdd(1, .monotonic);
        // the debt: this write's token is spoken for; the wait is when it exists
        b.tokens -= 1.0;
        const wait_s = (1.0 - b.tokens) / self.rate_per_s;
        return @intFromFloat(@ceil(wait_s * 1000.0));
    }
};

// ─── tests ──────────────────────────────────────────────────────────────────

test "off when the rate is zero" {
    var l = Limiter.init(std.testing.allocator, 0, 0);
    defer l.deinit();
    for (0..1000) |_| try std.testing.expect(l.take("bob", 0) == null);
}

test "a burst, then the rate — and the wait names the next token" {
    var l = Limiter.init(std.testing.allocator, 10, 3);
    defer l.deinit();
    try std.testing.expect(l.take("bob", 0) == null);
    try std.testing.expect(l.take("bob", 0) == null);
    try std.testing.expect(l.take("bob", 0) == null);
    // the fourth in the same millisecond: no token; its own arrives in 200 ms at
    // 10/s (the bucket owes it one: debt), the fifth's in 300 ms — the queue
    try std.testing.expectEqual(@as(?u64, 200), l.take("bob", 0));
    try std.testing.expectEqual(@as(?u64, 300), l.take("bob", 0));
    try std.testing.expectEqual(@as(u64, 2), l.limited_total.load(.monotonic));
    // the debts are paid from the refill: at 300 ms the two are covered and one
    // token is over; a new write takes it, the next waits
    try std.testing.expect(l.take("bob", 300) == null);
    try std.testing.expect(l.take("bob", 300) != null);
    // a long pause refills to the burst, never above it
    try std.testing.expect(l.take("bob", 100_000) == null);
    try std.testing.expect(l.take("bob", 100_000) == null);
    try std.testing.expect(l.take("bob", 100_000) == null);
    try std.testing.expect(l.take("bob", 100_000) != null);
}

test "principals do not share a bucket; a tenant's refusal refunds the principal" {
    var l = Limiter.init(std.testing.allocator, 10, 2);
    defer l.deinit();
    try std.testing.expect(l.take("bob", 0) == null);
    try std.testing.expect(l.take("bob", 0) == null);
    try std.testing.expect(l.take("bob", 0) != null);
    try std.testing.expect(l.take("alice", 0) == null); // her own bucket, untouched
    // the tenant bucket empties first: mary's two writes drain it, then nina's is
    // refused by the tenant and her own token is given back
    try std.testing.expect(l.takeTwo("mary", "tenant:acme", 0) == null);
    try std.testing.expect(l.takeTwo("mary", "tenant:acme", 0) == null);
    try std.testing.expect(l.takeTwo("nina", "tenant:acme", 0) != null);
    try std.testing.expectEqual(@as(f64, 2), l.buckets.get("nina").?.tokens);
    try std.testing.expectEqual(@as(f64, -1), l.buckets.get("tenant:acme").?.tokens);
}
