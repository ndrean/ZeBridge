const std = @import("std");

const log = std.log.scoped(.catalog_epoch);

/// How many DDL changes the replication stream has carried.
///
/// The bridge caches catalog facts — column names, key columns — because re-reading them
/// per mutation would double a round trip that is already synchronous. Anything cached
/// from PostgreSQL can be invalidated by PostgreSQL, and until this existed the only
/// signal was **a statement failing**: `42703 undefined_column` told the write path its
/// column list was stale. That covers a *dropped* column and nothing else. A column that
/// was **added** is rejected against the cached list before any SQL is built, so
/// PostgreSQL never sees the statement, never answers, and the cache never learns —
/// leaving `ALTER TABLE … ADD COLUMN` unwritable until the process was restarted.
///
/// ⚠️ "Restart to re-cache" is the smell this removes. A cache with no invalidation path
/// is process state that the database can silently contradict.
///
/// The signal is taken from the WAL, not from NATS (the rule is PROTOCOL.md §0): the DDL event trigger writes a row,
/// that row travels the replication stream like any other, and the bridge sees it before
/// it publishes anything. **Postgres is the source of truth for the catalogue; NATS is
/// the source of truth for consumers.** Reading the published KV schema back to decide
/// what the bridge itself should do would invert that, make a failed publish into a
/// correctness bug, and could not bootstrap against an empty bucket.
///
/// **Deliberately one counter for the whole database, not one per table.** Per-table
/// would need a map shared across threads, and so a lock on the mutation path, to save
/// re-reading a handful of catalogs on an event that happens when someone runs a
/// migration. A global bump costs one wasted catalog read per cached table, once.
///
/// ⚠️ **This narrows the stale window; it does not close it.** The DDL row reaches the
/// bridge some time after the transaction that wrote it commits, so a mutation arriving
/// inside that gap still sees the old cache. The re-read-and-retry in
/// `mutation_listener.handleMutation` remains the backstop, and a bridge that starts
/// mid-migration depends on it entirely.
pub const CatalogEpoch = struct {
    value: std.atomic.Value(u64) = .init(0),

    /// The replication thread observed DDL. Called for every `zebridge_ddl_events` row,
    /// including the ones that publish nothing (a schema that de-duplicated, a table that
    /// is refused): the question here is "did the catalogue move", not "did a client need
    /// telling".
    pub fn bump(self: *CatalogEpoch) void {
        const prev = self.value.fetchAdd(1, .release);
        log.debug("catalog epoch {d} → {d}", .{ prev, prev + 1 });
    }

    /// Read by the write path, once per mutation.
    pub fn current(self: *const CatalogEpoch) u64 {
        return self.value.load(.acquire);
    }
};

test "a fresh epoch starts at zero and advances once per bump" {
    var e: CatalogEpoch = .{};
    try std.testing.expectEqual(@as(u64, 0), e.current());
    e.bump();
    e.bump();
    try std.testing.expectEqual(@as(u64, 2), e.current());
}

test "a cache stamped with an older epoch is detectably stale" {
    // The whole contract, in the shape the write path uses it: stamp on read, compare
    // before reuse. Equality means reusable; anything else means re-read.
    var e: CatalogEpoch = .{};
    const stamped_at = e.current();
    try std.testing.expect(stamped_at == e.current());
    e.bump();
    try std.testing.expect(stamped_at != e.current());
}
