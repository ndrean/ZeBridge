//! The set of tables the bridge refuses to replicate, and how many of their events it
//! has dropped.
//!
//! A table is refused when it has no primary key. That verdict is reached in two
//! places — `preflight.zig` at boot, for tables already in the publication, and
//! `event_processor.zig` at runtime, when a DDL event announces a new one — and both
//! must reach the same conclusion, or a table refused at boot would still stream rows.
//! This registry is that shared conclusion.
//!
//! Refusal is not permanent. Adding a primary key emits a DDL event, the runtime path
//! re-evaluates, and `clear` lifts the refusal without a restart — which is the whole
//! point of not stopping the bridge in the first place.
//!
//! ## Why an append-only array rather than a hash map
//!
//! Three threads ask about refusals: the replication thread (writes, and drops events),
//! the snapshot listener (must reject a snapshot request for a refused table), and the
//! HTTP thread (scrapes). A hash map would need a lock, and 0.16's `std.Io.Mutex` wants
//! an `Io` threaded into every caller — including `event_processor`, which has none.
//!
//! So entries are appended and never removed, and their names are freed only at
//! `deinit`. A reader can therefore hold a name slice with no risk that a writer frees
//! it. Writers reserve their slot with an atomic `fetchAdd` and publish `len` in
//! reservation order, so two of them — the replication thread and the snapshot listener —
//! cannot claim the same index. `clear` flips an atomic `active` flag rather than removing the entry, which also
//! preserves the drop count across a refuse → fix → refuse cycle. `len` is published
//! with release ordering *after* an entry is fully built, so a reader either does not
//! see the entry at all or sees it complete.
//!
//! The cost is a linear scan, but only when something is refused — the common case
//! exits on one atomic load — and the scan covers a handful of entries, next to tuple
//! decoding and encoding on the same event. Capacity is fixed because growing would
//! realloc the backing array under a reader mid-scan.

const std = @import("std");
const utils = @import("utils.zig");

const log = std.log.scoped(.refused);

/// Far beyond any realistic number of keyless tables in one publication. Fixed rather
/// than growable so a concurrent reader is never walking a reallocated array.
pub const max_refused = 64;

/// Why a table is refused.
///
/// The tag name *is* the wire string published in the suspension payload
/// (`reason: "no_primary_key"`), so the operator-facing log, the client-facing KV value
/// and this enum cannot drift into three different spellings.
pub const Reason = enum {
    /// No primary key: rows cannot be identified, so DELETE is ambiguous.
    no_primary_key,
    /// A column whose type the CDC decoder cannot handle, and which is not an enum.
    /// Passing its bytes through as text is silent corruption, so the table stops.
    unsupported_column_type,
    /// A row larger than the per-event buffer (`BASE_BUF`). Unlike the other two this
    /// is a sizing verdict rather than a schema one, and it lifts on restart rather
    /// than on a migration.
    row_too_large,
    /// `TENANT_RULES` names a column the table does not have. Routing every row to a
    /// subject derived from a column that is not there would send them all to the same
    /// place — which is the opposite of scoping.
    no_tenant_column,
    /// The tenant column exists but sits outside the replica identity, so a DELETE
    /// carries the key and nothing else. Inserts and updates would route correctly and
    /// deletes could not route at all: rows would stay in every replica that held them,
    /// with no later event able to remove them.
    tenant_not_in_replica_identity,
    /// Neither tenant-scoped nor marked public in `zebridge_catalogue` — its CDC subject
    /// matches no stream's filter at all. Found live: publishing to it doesn't fail
    /// fast, it blocks for the full publish timeout (nothing ever acks a message no
    /// stream accepted), then retries the identical dead end, and enough of that
    /// exhausts the bridge's retry budget and self-terminates. Refusing at the source
    /// — before the first publish is ever attempted — is what turns that into a named,
    /// immediate refusal instead of a bridge-wide outage.
    no_cdc_subject,

    /// The string clients receive in a suspension payload.
    pub fn wireName(self: Reason) []const u8 {
        return @tagName(self);
    }

    /// What the operator has to change to lift the refusal.
    /// Whether a refusal for this reason must be RE-TESTED on every event instead of
    /// being held until the next boot or DDL event.
    ///
    /// `row_too_large` is the only DATA-dependent verdict here: nothing is wrong with
    /// the table, one ROW was wider than the event buffer — and that row may be deleted
    /// or narrowed a minute later. Held until boot (as it was), the table stayed frozen
    /// for its clients long after the cause was gone; `legacybait.py`'s 20 KB bait left
    /// `test_types` suspended with the bait already deleted, and only a restart lifted it
    /// (NOTES §10bm/§10bp).
    ///
    /// The others are STRUCTURAL and no event can change the answer: a table with no
    /// primary key has no keyable row, and one with no CDC subject has nowhere to
    /// publish. Re-testing those would burn a decode per event, forever, to learn
    /// nothing.
    pub fn probesEveryEvent(self: Reason) bool {
        return self == .row_too_large;
    }

    pub fn fixHint(self: Reason) []const u8 {
        return switch (self) {
            .no_primary_key => "add a primary key to replicate this table",
            .unsupported_column_type => "change the column to a type the bridge can decode (or drop it from the table)",
            .row_too_large => "restart with a larger BASE_BUF, or move the oversized column out of the replicated table",
            .no_tenant_column => "add the column the catalogue/TENANT_RULES names, or correct the rule",
            .tenant_not_in_replica_identity => "CREATE UNIQUE INDEX <t>_zb_ri ON <t> (<tenant>, <pk>); ALTER TABLE <t> REPLICA IDENTITY USING INDEX <t>_zb_ri",
            .no_cdc_subject => "declare the table in zebridge_catalogue (zebridge_enable with public_reason or tenant_col) — the bridge reloads on the catalogue row and lifts this itself, no restart",
        };
    }
};

const Entry = struct {
    /// Owned. Freed only in `deinit`, so readers may hold it while writers work.
    name: []const u8,
    /// Events dropped for this table. Replication thread only.
    dropped: u64 = 0,
    /// Whether the refusal currently stands. Read from any thread.
    active: std.atomic.Value(bool) = .init(false),
    /// Why. Written only by the replication thread (or preflight, before it starts).
    reason: Reason = .no_primary_key,
    /// When this refusal was (re)raised, for the probe cooldown. Replication thread only.
    suspended_at_ms: i64 = 0,
};

/// How long a probed table stays suspended before a fitting row may lift it.
///
/// ⚠️ Hysteresis, not politeness. Lifting on the FIRST fit is correct for the case this
/// exists for — one oversized row, deleted — but a table whose rows straddle the buffer
/// (a text column with the occasional large document) would otherwise lift and
/// re-suspend per event, and each flap costs a schema publish plus a suspension publish:
/// two KV writes per row. Thirty seconds bounds that to two writes a minute while still
/// recovering a genuinely-fixed table long before the restart it used to need.
const lift_cooldown_ms: i64 = 30_000;

/// Monotonic, in milliseconds. A cooldown measured on the wall clock would jump with
/// NTP; `utils.nanoTimestamp` is CLOCK_MONOTONIC.
fn nowMs() i64 {
    return @intCast(utils.nanoTimestamp() / std.time.ns_per_ms);
}

pub const Registry = struct {
    allocator: std.mem.Allocator,
    entries: [max_refused]Entry = undefined,
    /// Published with release ordering once an entry is fully initialised. Only grows.
    /// A reader walking `entries[0..len]` therefore never sees a half-built entry.
    len: std.atomic.Value(usize) = .init(0),
    /// Slots handed out. Separate from `len` so two writers cannot claim the same index:
    /// a writer reserves with `fetchAdd`, builds its entry, then advances `len` only once
    /// every earlier reservation has published. Before this, `refuse` did
    /// `n = len.load()` … `len.store(n + 1)`, which is correct for exactly one writer —
    /// the replication thread — and the snapshot listener is now a second one.
    reserved: std.atomic.Value(usize) = .init(0),
    /// How many entries are currently active. The cheap "is anything refused?" test.
    refused_count: std.atomic.Value(usize) = .init(0),
    dropped_total: std.atomic.Value(u64) = .init(0),
    /// Set once if a refusal could not be recorded for want of capacity. A table we
    /// failed to record would silently keep streaming, so this must be loud.
    overflowed: std.atomic.Value(bool) = .init(false),

    pub fn init(allocator: std.mem.Allocator) Registry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Registry) void {
        for (self.entries[0..self.len.load(.acquire)]) |e| self.allocator.free(e.name);
        self.* = undefined;
    }

    /// Locate an entry by name, active or not. Safe from any thread.
    fn find(self: *const Registry, table: []const u8) ?*Entry {
        const n = self.len.load(.acquire);
        for (self.entries[0..n]) |*e| {
            if (std.mem.eql(u8, e.name, table)) return @constCast(e);
        }
        return null;
    }

    /// Mark a table refused. Idempotent: re-refusing keeps the running drop count, so a
    /// re-announced DDL event does not reset the number the operator is watching.
    pub fn refuse(self: *Registry, table: []const u8, reason: Reason) !void {
        if (self.find(table)) |e| {
            // A table can fail a second check after passing the first (gain a key, then
            // gain an hstore column), so the reason is refreshed even when already active.
            e.reason = reason;
            if (!e.active.load(.acquire)) {
                // Stamped only on the transition into refusal, not on every re-refusal:
                // the cooldown measures how long the table has been suspended, and
                // re-stamping would let a stream of oversized rows hold the lift off
                // forever (the flap this bounds is the opposite problem).
                e.suspended_at_ms = nowMs();
                e.active.store(true, .release);
                _ = self.refused_count.fetchAdd(1, .acq_rel);
            }
            return;
        }

        // Duped *before* the slot is reserved, deliberately. Nothing between the
        // reservation and the publish below may fail: a writer that claimed slot n and
        // then returned early would leave `len` unable to reach n + 1, and every later
        // writer would spin on it forever. Allocating first means the only path that can
        // fail does so while holding nothing.
        const owned = try self.allocator.dupe(u8, table);
        errdefer self.allocator.free(owned);

        const n = self.reserved.fetchAdd(1, .acq_rel);
        if (n >= max_refused) {
            // Not silent: an unrecorded refusal means the table keeps publishing rows
            // that no client can apply, which is the exact failure this guards against.
            // Safe to return without publishing: every writer at or past the ceiling
            // returns here too, so nobody is left waiting on this index.
            if (!self.overflowed.swap(true, .acq_rel)) {
                log.err(
                    "🔴 Refusal registry is full ({d} tables) — '{s}' cannot be tracked and its events will NOT be dropped. Fix the keyless tables.",
                    .{ max_refused, table },
                );
            }
            return error.RefusalRegistryFull;
        }

        self.entries[n] = .{ .name = owned, .dropped = 0, .active = .init(true), .reason = reason, .suspended_at_ms = nowMs() };

        // Publish last, and in reservation order: `len` may only advance to n + 1 once
        // every slot below n has published, or a reader walking `entries[0..len]` would
        // step over one that is still `undefined`. Refusals are rare and the window is a
        // few instructions, so a spin costs less than any alternative would.
        while (self.len.cmpxchgWeak(n, n + 1, .release, .acquire) != null) {
            std.atomic.spinLoopHint();
        }
        _ = self.refused_count.fetchAdd(1, .acq_rel);
    }

    /// Lift a refusal — the table's shape was fixed. The entry stays (its name may
    /// be held by a reader, and its drop count is worth keeping); only the flag drops.
    pub fn clear(self: *Registry, table: []const u8) void {
        const e = self.find(table) orelse return;
        if (e.active.swap(false, .acq_rel)) {
            _ = self.refused_count.fetchSub(1, .acq_rel);
            // "was", not "resolved": this is also the path a DROP TABLE takes, where
            // nothing was fixed — the table simply stopped existing.
            log.info("✅ '{s}' is no longer refused (was: {s})", .{ e.name, e.reason.wireName() });
        }
    }

    /// Why this table is refused, or null if it is not.
    ///
    /// ⚠️ Exists because reporting the wrong reason is worse than reporting none. The
    /// snapshot path used to announce every refusal as "no primary key", so a table
    /// suspended for `row_too_large` sent its operator to add a key it already had —
    /// measured on a 256 KiB-per-row fixture whose snapshot was refused with a message
    /// naming the one thing that was not wrong with it.
    pub fn reasonFor(self: *const Registry, table: []const u8) ?Reason {
        const e = self.find(table) orelse return null;
        return if (e.active.load(.acquire)) e.reason else null;
    }

    /// What the replication loop must do with an event for this table.
    pub const Verdict = enum {
        /// Not refused — pack it.
        pass,
        /// Refused structurally: skip it, counted.
        drop,
        /// Refused for a data-dependent reason: TRY to pack it. Fitting lifts the
        /// refusal (`liftIfProbing`); not fitting drops it, counted at that point
        /// (`countDrop`) rather than here, because only the pack knows which it was.
        probe,
    };

    /// The hot-path question, in one call: same single atomic load as `shouldDrop` when
    /// nothing is refused, which is every event on a healthy bridge.
    pub fn verdictFor(self: *Registry, table: []const u8) Verdict {
        if (self.refused_count.load(.acquire) == 0) return .pass;

        const e = self.find(table) orelse return .pass;
        if (!e.active.load(.acquire)) return .pass;

        // ⚠️ The cooldown gates the PROBE, not the lift. Inside it the table behaves
        // exactly as a suspended table always did — events dropped and counted — because
        // an event that packed while the table was still refused would publish CDC for a
        // table whose clients are holding a suspension for it, which is the one state
        // this must never produce. Outside it, every event is a probe.
        if (e.reason.probesEveryEvent() and nowMs() - e.suspended_at_ms >= lift_cooldown_ms) {
            return .probe;
        }

        self.recordDrop(e);
        return .drop;
    }

    /// Count one dropped event for a table already known to be refused. Used by the
    /// probe path, where the drop is decided after the attempt, not before it.
    pub fn countDrop(self: *Registry, table: []const u8) void {
        const e = self.find(table) orelse return;
        if (!e.active.load(.acquire)) return;
        self.recordDrop(e);
    }

    fn recordDrop(self: *Registry, e: *Entry) void {
        e.dropped +|= 1;
        // Saturating, not wrapping: a counter that rolls over to zero would read as
        // "nothing was dropped", which is the one thing this must never say.
        self.dropped_total.store(self.dropped_total.raw +| 1, .release);
    }

    /// A probed event packed successfully, so the row that suspended this table is gone
    /// or has narrowed: lift the refusal.
    ///
    /// ⚠️ Call only after `verdictFor` answered `.probe` for this table — the cooldown
    /// lives there, and this does not re-check it. Returns true when it actually cleared one, so
    /// the caller knows to republish the descriptor its clients are still holding a
    /// suspension for. Same cheap fast path as `verdictFor`.
    pub fn liftIfProbing(self: *Registry, table: []const u8) bool {
        if (self.refused_count.load(.acquire) == 0) return false;
        const e = self.find(table) orelse return false;
        if (!e.active.load(.acquire)) return false;
        if (!e.reason.probesEveryEvent()) return false;
        // No cooldown check here: `verdictFor` refuses to probe inside it, so reaching
        // this point means an event was let through and fitted.
        self.clear(table);
        return true;
    }

    /// True if this table is refused; also counts the event as dropped.
    ///
    /// Combining the test and the count is deliberate: a caller that asks the question
    /// is about to drop the event, and two separate calls would eventually drift apart.
    /// Replication thread only — it mutates `dropped`.
    pub fn shouldDrop(self: *Registry, table: []const u8) bool {
        // Fast path: nothing refused, so an accepted table pays one atomic load per
        // event and never a scan.
        if (self.refused_count.load(.acquire) == 0) return false;

        const e = self.find(table) orelse return false;
        if (!e.active.load(.acquire)) return false;

        e.dropped +|= 1;
        // Saturating, not wrapping: a counter that rolls over to zero would read as
        // "nothing was dropped", which is the one thing this must never say.
        self.dropped_total.store(self.dropped_total.raw +| 1, .release);
        return true;
    }

    /// Test membership without counting a drop. Safe from any thread — this is what the
    /// snapshot listener calls to reject a request for a refused table.
    ///
    /// Separate from `shouldDrop` because a rejected snapshot request and a withheld
    /// boot schema are not dropped rows, and must not inflate the operator's number.
    pub fn isRefused(self: *const Registry, table: []const u8) bool {
        if (self.refused_count.load(.acquire) == 0) return false;
        const e = self.find(table) orelse return false;
        return e.active.load(.acquire);
    }

    pub fn count(self: *const Registry) usize {
        return self.refused_count.load(.acquire);
    }

    /// Log every refused table and its drop count. Called from the periodic METRICS
    /// line: a message printed once at boot scrolls away, and the operator who needs to
    /// see it is usually looking at a dashboard hours later, wondering where the rows
    /// went. Silence after the first minute is what makes this failure mode invisible,
    /// so we keep saying it.
    pub fn logStatus(self: *const Registry) void {
        for (self.entries[0..self.len.load(.acquire)]) |*e| {
            if (!e.active.load(.acquire)) continue;
            log.warn(
                "🔴 REFUSED '{s}' ({s}): {d} event(s) dropped — {s}",
                .{ e.name, e.reason.wireName(), e.dropped, e.reason.fixHint() },
            );
        }
    }

    /// Render as Prometheus exposition text. Reads only the atomics and the immortal
    /// names, so the HTTP thread may call it while the replication thread writes.
    pub fn writePrometheus(self: *const Registry, w: *std.Io.Writer) !void {
        try w.print("# HELP bridge_refused_tables Tables refused (no primary key, undecodable column type, or a row too large for the event buffer)\n", .{});
        try w.print("# TYPE bridge_refused_tables gauge\n", .{});
        try w.print("bridge_refused_tables {d}\n", .{self.refused_count.load(.acquire)});

        try w.print("# HELP bridge_refused_events_dropped_total Events dropped for refused tables\n", .{});
        try w.print("# TYPE bridge_refused_events_dropped_total counter\n", .{});
        try w.print("bridge_refused_events_dropped_total {d}\n", .{self.dropped_total.load(.acquire)});
    }
};

test "unrefused tables are never dropped" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try std.testing.expect(!r.shouldDrop("users"));
    try r.refuse("t_nopk", .no_primary_key);
    try std.testing.expect(!r.shouldDrop("users"));
    try std.testing.expectEqual(@as(usize, 1), r.count());
}

test "refused tables drop and count" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try r.refuse("t_nopk", .no_primary_key);
    try std.testing.expect(r.shouldDrop("t_nopk"));
    try std.testing.expect(r.shouldDrop("t_nopk"));
    try std.testing.expect(r.shouldDrop("t_nopk"));

    var buf: [512]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try r.writePrometheus(&w);
    try std.testing.expect(std.mem.indexOf(u8, w.buffered(), "bridge_refused_tables 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, w.buffered(), "bridge_refused_events_dropped_total 3") != null);
}

test "re-refusing keeps the running count" {
    // A DDL event for an already-refused table must not reset the number the operator
    // has been watching climb.
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try r.refuse("t_nopk", .no_primary_key);
    _ = r.shouldDrop("t_nopk");
    _ = r.shouldDrop("t_nopk");
    try r.refuse("t_nopk", .no_primary_key);

    try std.testing.expectEqual(@as(usize, 1), r.count());
    try std.testing.expect(r.shouldDrop("t_nopk"));

    var buf: [512]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try r.writePrometheus(&w);
    try std.testing.expect(std.mem.indexOf(u8, w.buffered(), "bridge_refused_events_dropped_total 3") != null);
}

test "clearing a refusal resumes replication" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try r.refuse("t_nopk", .no_primary_key);
    try std.testing.expect(r.shouldDrop("t_nopk"));

    r.clear("t_nopk");
    try std.testing.expectEqual(@as(usize, 0), r.count());
    try std.testing.expect(!r.shouldDrop("t_nopk"));
    try std.testing.expect(!r.isRefused("t_nopk"));
}

test "a re-refused table reuses its entry and keeps its history" {
    // A table can lose its PK, regain it, and lose it again across migrations. Each
    // cycle must reuse the one entry rather than consuming a new slot.
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try r.refuse("t_nopk", .no_primary_key);
    _ = r.shouldDrop("t_nopk");
    r.clear("t_nopk");
    try r.refuse("t_nopk", .no_primary_key);
    _ = r.shouldDrop("t_nopk");

    try std.testing.expectEqual(@as(usize, 1), r.count());
    try std.testing.expectEqual(@as(usize, 1), r.len.load(.acquire));
    try std.testing.expectEqual(@as(u64, 2), r.dropped_total.load(.acquire));
}

test "clearing an unrefused table is a no-op" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    r.clear("never_refused");
    try std.testing.expectEqual(@as(usize, 0), r.count());
}

test "isRefused does not inflate the drop count" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try r.refuse("t_nopk", .no_primary_key);
    try std.testing.expect(r.isRefused("t_nopk"));
    try std.testing.expect(r.isRefused("t_nopk"));
    try std.testing.expect(!r.isRefused("users"));
    try std.testing.expectEqual(@as(u64, 0), r.dropped_total.load(.acquire));
}

test "names are owned, so callers may free their own" {
    // The DDL path passes a name pointing into an arena that is reset per message.
    var name_buf: [16]u8 = undefined;
    const scratch = try std.fmt.bufPrint(&name_buf, "t_nopk", .{});

    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try r.refuse(scratch, .no_primary_key);
    @memset(&name_buf, 0); // simulate the arena being reset under us

    try std.testing.expect(r.shouldDrop("t_nopk"));
}

test "overflow is reported rather than silently ignored" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    var buf: [32]u8 = undefined;
    for (0..max_refused) |i| {
        try r.refuse(try std.fmt.bufPrint(&buf, "t{d}", .{i}), .no_primary_key);
    }
    try std.testing.expectEqual(@as(usize, max_refused), r.count());

    // Pre-latch the "already reported" flag: the behaviour under test is the error
    // return, and letting the real log.err fire would have the test runner count this
    // deliberate case as a failed build.
    r.overflowed.store(true, .release);
    try std.testing.expectError(error.RefusalRegistryFull, r.refuse("one_too_many", .no_primary_key));
    try std.testing.expectEqual(@as(usize, max_refused), r.count());
}

test "refuse: concurrent writers each get their own slot" {
    // The replication thread and the snapshot listener both refuse tables now. Under the
    // old `len.load()` … `len.store(n + 1)` these would collide: two writers read the same
    // index, both wrote it, one entry was lost and its name leaked.
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    const Worker = struct {
        fn run(reg: *Registry, base: usize) void {
            var buf: [32]u8 = undefined;
            for (0..8) |i| {
                const name = std.fmt.bufPrint(&buf, "t{d}", .{base + i}) catch return;
                reg.refuse(name, .no_primary_key) catch return;
            }
        }
    };

    var a = try std.Thread.spawn(.{}, Worker.run, .{ &r, 0 });
    var b = try std.Thread.spawn(.{}, Worker.run, .{ &r, 100 });
    a.join();
    b.join();

    try std.testing.expectEqual(@as(usize, 16), r.len.load(.acquire));
    try std.testing.expectEqual(@as(usize, 16), r.refused_count.load(.acquire));

    // Every name must be present exactly once, and none may be the empty slot a lost
    // publish would leave behind.
    for (0..8) |i| {
        var buf: [32]u8 = undefined;
        try std.testing.expect(r.isRefused(try std.fmt.bufPrint(&buf, "t{d}", .{i})));
        try std.testing.expect(r.isRefused(try std.fmt.bufPrint(&buf, "t{d}", .{100 + i})));
    }
}

test "verdict: a structural refusal drops every event, a size refusal is probed once it may lift" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try std.testing.expectEqual(Registry.Verdict.pass, r.verdictFor("users"));

    // Structural: no event can change the answer, so it is dropped and counted forever.
    try r.refuse("t_nopk", .no_primary_key);
    try std.testing.expectEqual(Registry.Verdict.drop, r.verdictFor("t_nopk"));
    try std.testing.expectEqual(Registry.Verdict.drop, r.verdictFor("t_nopk"));
    try std.testing.expectEqual(@as(u64, 2), r.dropped_total.load(.acquire));

    // Data-dependent: inside the cooldown it behaves identically — nothing may flow for
    // a table whose clients hold a suspension.
    try r.refuse("t_big", .row_too_large);
    try std.testing.expectEqual(Registry.Verdict.drop, r.verdictFor("t_big"));
    try std.testing.expect(r.isRefused("t_big"));

    // Past the cooldown the next event is a probe (the clock is monotonic, so the test
    // moves the stamp back rather than sleeping).
    r.find("t_big").?.suspended_at_ms -= lift_cooldown_ms;
    try std.testing.expectEqual(Registry.Verdict.probe, r.verdictFor("t_big"));
    // A probe is NOT a drop: whether it counts is decided by whether it packed.
    try std.testing.expectEqual(@as(u64, 3), r.dropped_total.load(.acquire));
}

test "liftIfProbing lifts only a size suspension, and only once" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try r.refuse("t_nopk", .no_primary_key);
    r.find("t_nopk").?.suspended_at_ms -= lift_cooldown_ms;
    // Structural refusals are never lifted by data — that needs a migration (a DDL event).
    try std.testing.expect(!r.liftIfProbing("t_nopk"));
    try std.testing.expect(r.isRefused("t_nopk"));

    try r.refuse("t_big", .row_too_large);
    r.find("t_big").?.suspended_at_ms -= lift_cooldown_ms;
    try std.testing.expect(r.liftIfProbing("t_big"));
    try std.testing.expect(!r.isRefused("t_big"));
    try std.testing.expectEqual(Registry.Verdict.pass, r.verdictFor("t_big"));
    // Idempotent: a second fitting row in the same transaction must not republish.
    try std.testing.expect(!r.liftIfProbing("t_big"));
}

test "countDrop records a probe that did not fit, and the history survives a lift" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();

    try r.refuse("t_big", .row_too_large);
    r.countDrop("t_big");
    r.countDrop("t_big");
    try std.testing.expectEqual(@as(u64, 2), r.dropped_total.load(.acquire));

    // An unrefused table cannot accrue drops — the probe path only calls this when the
    // pack failed for a table it already knew was suspended.
    r.countDrop("never_refused");
    try std.testing.expectEqual(@as(u64, 2), r.dropped_total.load(.acquire));

    r.find("t_big").?.suspended_at_ms -= lift_cooldown_ms;
    try std.testing.expect(r.liftIfProbing("t_big"));
    // The count is the operator's running total, so a lift must not reset it (same rule
    // as re-refusing).
    try std.testing.expectEqual(@as(u64, 2), r.dropped_total.load(.acquire));
    try std.testing.expectEqual(@as(u64, 2), r.find("t_big").?.dropped);
}
