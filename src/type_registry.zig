//! OID → `pg_type.typtype`, for deciding whether a CDC value can be decoded at all.
//!
//! ## Why this exists
//!
//! `pgoutput.decodeBinColumnData` switches on a fixed list of built-in OIDs. Anything
//! else used to fall through to "treat the bytes as text", which is **correct for an
//! enum** — Postgres sends enum values as their label — and **silent corruption for
//! everything else**. Observed live: an `hstore` column shipped its binary wire form,
//! `\0\0\0\1\0\0\0\1k\0\0\0\1v`, to clients as a string, behind one `log.warn`.
//!
//! User-defined and extension types get per-database OIDs, so no compile-time switch
//! can ever contain them. `typtype` is what separates the two cases at runtime, and it
//! is free at the point the DDL event is built — inside Postgres, with the catalog
//! right there — so the bridge never queries for it on the replication hot path.
//!
//! ## Why it never needs invalidating
//!
//! A type's OID lives exactly as long as the type does. Drop and recreate it and you
//! get a *new* OID, arriving on a *new* DDL event. A stale entry therefore describes
//! something that can no longer appear on the wire, so entries are only ever added.

const std = @import("std");
const pg_constants = @import("pg_constants.zig");

pub const log = std.log.scoped(.type_registry);

/// What the decoder may do with a column of this type.
pub const Verdict = enum {
    /// The OID is in the decoder's switch: decode it.
    decode,
    /// Not in the switch, but `typtype = 'e'`. Postgres sends enum values as their
    /// label, so passing the bytes through as text is exactly right.
    text_passthrough,
    /// Not in the switch and not an enum — or not known at all. Refuse: emitting the
    /// raw wire form as a string is the corruption this whole module exists to prevent.
    refuse,
};

/// ## Threading
///
/// Deliberately unsynchronised, because every access is on one thread: `publishBootSchemas`
/// populates it on the main thread *before* replication starts, and after that both the
/// writer (`packDdlToSlot`) and the reader (`packMutationToSlot` → `decodeTuple`) are the
/// WAL thread. The snapshot path does not consult it at all — it has the catalog open
/// inside its own transaction and reads `typtype` from there directly.
///
/// If a second thread ever needs to read this, it cannot simply take a lock: 0.16's
/// `std.Io.Mutex` wants an `Io` threaded into every caller, and `event_processor` has
/// none — the same constraint that shaped `refused_tables`. Copy that module's
/// append-only-with-atomics approach instead.
pub const Registry = struct {
    map: std.AutoHashMap(u32, u8),

    pub fn init(allocator: std.mem.Allocator) Registry {
        return .{ .map = std.AutoHashMap(u32, u8).init(allocator) };
    }

    pub fn deinit(self: *Registry) void {
        self.map.deinit();
    }

    /// Record one column's type facts. Idempotent, and additive by design (see above).
    pub fn record(self: *Registry, oid: u32, typtype: u8) !void {
        // Built-in OIDs the decoder already handles carry no information worth storing;
        // keeping them out bounds the map to types that are actually exotic.
        if (pg_constants.isKnownOid(oid)) return;

        const gop = try self.map.getOrPut(oid);
        if (!gop.found_existing) {
            gop.value_ptr.* = typtype;
            log.debug("recorded OID {d} as typtype '{c}'", .{ oid, typtype });
        }
    }

    /// Decide what may be done with a value of this type.
    ///
    /// Fails closed: an OID the decoder does not know and this registry has never seen
    /// is `.refuse`, not "probably text". The open version of this decision is what
    /// shipped hstore's bytes as a string.
    pub fn verdict(self: *Registry, oid: u32) Verdict {
        if (pg_constants.isKnownOid(oid)) return .decode;

        const typtype = self.map.get(oid) orelse return .refuse;
        return if (typtype == 'e') .text_passthrough else .refuse;
    }

    pub fn count(self: *Registry) usize {
        return self.map.count();
    }
};

const testing = std.testing;

test "verdict - a built-in OID is decodable without ever being recorded" {
    var reg = Registry.init(testing.allocator);
    defer reg.deinit();

    try testing.expectEqual(Verdict.decode, reg.verdict(23)); // int4
    try testing.expectEqual(Verdict.decode, reg.verdict(1700)); // numeric
    try testing.expectEqual(@as(usize, 0), reg.count());
}

test "verdict - an unknown OID with no entry refuses rather than guessing" {
    var reg = Registry.init(testing.allocator);
    defer reg.deinit();

    // hstore's OID is assigned per database, so it can never be a compile-time constant.
    try testing.expectEqual(Verdict.refuse, reg.verdict(16416));
}

test "verdict - an enum passes through as text, a base type does not" {
    var reg = Registry.init(testing.allocator);
    defer reg.deinit();

    try reg.record(16544, 'e'); // a `mood` enum
    try reg.record(16416, 'b'); // hstore: base type, binary send format

    try testing.expectEqual(Verdict.text_passthrough, reg.verdict(16544));
    try testing.expectEqual(Verdict.refuse, reg.verdict(16416));
}

test "record - built-in OIDs are not stored, exotic ones are" {
    var reg = Registry.init(testing.allocator);
    defer reg.deinit();

    try reg.record(23, 'b'); // int4 — the decoder's switch already covers it
    try reg.record(16416, 'b');

    try testing.expectEqual(@as(usize, 1), reg.count());
}

test "record - first writer wins, so a repeated DDL event changes nothing" {
    var reg = Registry.init(testing.allocator);
    defer reg.deinit();

    try reg.record(16544, 'e');
    try reg.record(16544, 'e');

    try testing.expectEqual(@as(usize, 1), reg.count());
    try testing.expectEqual(Verdict.text_passthrough, reg.verdict(16544));
}
