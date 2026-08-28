//! The handle table: how a C host owns a Zig object without being able to corrupt it.
//!
//! A host cannot be trusted to call `close` exactly once. Double close, close from two
//! threads, close then use — these are the ordinary mistakes of every FFI binding, and
//! in Zig each of them is undefined behaviour the moment a raw pointer crosses the
//! boundary. An embedder CANNOT catch a Zig panic, so at this boundary a lifecycle
//! mistake is not a stack trace, it is the host process dying (NOTES §10ax).
//!
//! So the host never receives a pointer. It receives a `u64` **handle**:
//!
//!     bits 0..31   slot index
//!     bits 32..63  generation — bumped on every close
//!
//! Every access re-validates both halves against the table. A stale handle names a
//! slot whose generation has moved on, so it resolves to `null` and the call returns
//! an error — deterministically, with no memory touched. That is the whole trick: the
//! generation makes "this handle is from a previous life" a CHECKABLE fact rather than
//! a use-after-free.
//!
//! ⚠️ Slots are never reused for a different pointer without a generation bump, and
//! the generation is 32 bits: a slot would have to be opened and closed four billion
//! times before a stale handle could alias a live one.

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

pub fn Table(comptime T: type, comptime capacity: usize) type {
    return struct {
        const Self = @This();

        const Slot = struct {
            ptr: ?*T = null,
            generation: u32 = 1, // starts at 1 so a zeroed handle is never valid
        };

        slots: [capacity]Slot = @splat(.{}),
        /// ⚠️ Not `std.Thread.Mutex` — it does not exist in Zig 0.16, and `std.Io.Mutex`
        /// would force an `Io` on every caller of a C ABI whose whole point is that a
        /// host passes nothing but integers and strings. `storage.zig` has the same
        /// spinlock for the same reason. Every operation here is O(1) and uncontended
        /// in practice, so a spin is cheaper than the plumbing it avoids.
        lock: SpinLock = .{},

        pub fn handleOf(index: u32, generation: u32) u64 {
            return (@as(u64, generation) << 32) | @as(u64, index);
        }

        fn split(h: u64) struct { index: u32, generation: u32 } {
            return .{ .index = @truncate(h), .generation = @truncate(h >> 32) };
        }

        /// Take ownership of `ptr`, returning the handle the host will hold.
        /// 0 means the table is full — a real answer, not a panic.
        pub fn insert(self: *Self, ptr: *T) u64 {
            self.lock.lock();
            defer self.lock.unlock();
            for (&self.slots, 0..) |*slot, i| {
                if (slot.ptr == null) {
                    slot.ptr = ptr;
                    return handleOf(@intCast(i), slot.generation);
                }
            }
            return 0;
        }

        /// The pointer this handle names, or null if it names nothing — out of range,
        /// already closed, or from a previous life of the same slot.
        pub fn get(self: *Self, h: u64) ?*T {
            const p = split(h);
            if (p.index >= capacity) return null;
            self.lock.lock();
            defer self.lock.unlock();
            const slot = &self.slots[p.index];
            if (slot.generation != p.generation) return null;
            return slot.ptr;
        }

        /// Detach the pointer so the caller can destroy it. Returns null when the
        /// handle names nothing — which is exactly what a DOUBLE CLOSE looks like, and
        /// why a double close is a no-op here instead of a second free.
        ///
        /// The generation is bumped under the same lock that clears the slot, so two
        /// threads closing the same handle cannot both come away with the pointer.
        pub fn remove(self: *Self, h: u64) ?*T {
            const p = split(h);
            if (p.index >= capacity) return null;
            self.lock.lock();
            defer self.lock.unlock();
            const slot = &self.slots[p.index];
            if (slot.generation != p.generation) return null;
            const ptr = slot.ptr orelse return null;
            slot.ptr = null;
            slot.generation +%= 1;
            if (slot.generation == 0) slot.generation = 1; // never hand back a zero handle
            return ptr;
        }

        pub fn liveCount(self: *Self) usize {
            self.lock.lock();
            defer self.lock.unlock();
            var n: usize = 0;
            for (self.slots) |slot| {
                if (slot.ptr != null) n += 1;
            }
            return n;
        }
    };
}

// ─── tests ──────────────────────────────────────────────────────────────────

const Dummy = struct { x: u32 = 7 };

test "a handle resolves, and a closed one resolves to nothing" {
    var t: Table(Dummy, 4) = .{};
    var d = Dummy{};
    const h = t.insert(&d);
    try std.testing.expect(h != 0);
    try std.testing.expect(t.get(h) == &d);
    try std.testing.expect(t.remove(h) == &d);
    try std.testing.expect(t.get(h) == null);
}

test "double close is a no-op, not a second free" {
    var t: Table(Dummy, 4) = .{};
    var d = Dummy{};
    const h = t.insert(&d);
    try std.testing.expect(t.remove(h) != null);
    // The whole point: the second remove hands back NOTHING, so the caller cannot
    // free the same pointer twice however many times the host calls close.
    try std.testing.expect(t.remove(h) == null);
    try std.testing.expect(t.remove(h) == null);
}

test "a stale handle cannot reach the slot's new occupant" {
    var t: Table(Dummy, 4) = .{};
    var first = Dummy{ .x = 1 };
    var second = Dummy{ .x = 2 };
    const old = t.insert(&first);
    _ = t.remove(old);
    const new = t.insert(&second); // same slot, next generation
    try std.testing.expect(new != old);
    try std.testing.expect(t.get(new) == &second);
    // Without the generation this would return &second — the classic FFI bug where a
    // host's stale handle silently addresses somebody else's object.
    try std.testing.expect(t.get(old) == null);
}

test "a fabricated handle resolves to nothing" {
    var t: Table(Dummy, 4) = .{};
    var d = Dummy{};
    _ = t.insert(&d);
    try std.testing.expect(t.get(0) == null); // zeroed: generation 0 is never valid
    try std.testing.expect(t.get(999999) == null); // out of range
    try std.testing.expect(t.get(Table(Dummy, 4).handleOf(0, 12345)) == null); // wrong generation
}

test "a full table answers 0 rather than panicking" {
    var t: Table(Dummy, 2) = .{};
    var a = Dummy{};
    var b = Dummy{};
    var c = Dummy{};
    try std.testing.expect(t.insert(&a) != 0);
    try std.testing.expect(t.insert(&b) != 0);
    try std.testing.expect(t.insert(&c) == 0);
    try std.testing.expectEqual(@as(usize, 2), t.liveCount());
}
