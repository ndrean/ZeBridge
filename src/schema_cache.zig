//! Schema version tracking and change detection
//!
//! This module manages schema versioning using PostgreSQL relation_id as a natural
//! version number. When a table's DDL changes, PostgreSQL assigns a new relation_id,
//! which we detect and use to trigger schema republishing.

const std = @import("std");

pub const log = std.log.scoped(.schema_cache);

/// Schema version tracker - uses relation_id as natural version number
///
/// PostgreSQL assigns a new relation_id whenever a table's structure changes
/// (ALTER TABLE, DROP/CREATE, etc.). We use this as a reliable schema version
/// indicator without needing to parse column definitions.
///
/// Thread safety: Not thread-safe. Caller must synchronize access if used
/// from multiple threads (in our case, only the main thread uses this).
pub const SchemaCache = struct {
    // Map: table_name -> relation_id
    allocator: std.mem.Allocator,
    cache: std.StringHashMap(u32),

    /// Initialize an empty schema cache
    pub fn init(allocator: std.mem.Allocator) SchemaCache {
        return .{
            .allocator = allocator,
            .cache = std.StringHashMap(u32).init(allocator),
        };
    }

    /// Free all resources
    pub fn deinit(self: *SchemaCache) void {
        // Free all keys (table names) we own
        var it = self.cache.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.cache.deinit();
    }

    /// Check if schema has changed for a table
    ///
    /// Returns true if:
    /// - Table is new (not in cache)
    /// - Table's relation_id has changed (schema modification detected)
    ///
    /// Side effect: Updates cache with new relation_id if changed
    ///
    /// Args:
    ///   table_name: Name of the table to check
    ///   relation_id: Current relation_id from PostgreSQL
    ///
    /// Returns:
    ///   true if schema changed or is new, false if unchanged
    pub fn hasChanged(self: *SchemaCache, table_name: []const u8, relation_id: u32) !bool {
        // NOTE: do not reach for fetchPut here. As of Zig 0.16 it updates only the
        // value and leaves the map's existing key pointer in place, so freeing the
        // returned KV.key dangles the live key and leaks the replacement.
        const gop = try self.cache.getOrPut(table_name);

        if (gop.found_existing) {
            // Key already owned by the map — only the version needs updating,
            // so neither branch here allocates.
            if (gop.value_ptr.* == relation_id) return false;
            gop.value_ptr.* = relation_id;
            return true;
        }

        // New table: getOrPut stored the caller's slice as the key, which we do not
        // own. Replace it with a copy before returning, and unwind on failure.
        const owned_name = self.allocator.dupe(u8, table_name) catch |err| {
            _ = self.cache.remove(table_name);
            return err;
        };
        gop.key_ptr.* = owned_name;
        gop.value_ptr.* = relation_id;

        return true;
    }

    /// Get the cached relation_id for a table, if it exists
    pub fn get(self: *const SchemaCache, table_name: []const u8) ?u32 {
        return self.cache.get(table_name);
    }

    /// Get the number of tables tracked in the cache
    pub fn count(self: *const SchemaCache) usize {
        return self.cache.count();
    }

    /// Clear all cached schemas
    pub fn clear(self: *SchemaCache) void {
        var it = self.cache.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.cache.clearRetainingCapacity();
    }
};

// Tests
test "SchemaCache - basic operations" {
    const allocator = std.testing.allocator;
    var cache = SchemaCache.init(allocator);
    defer cache.deinit();

    // New table should report as changed
    try std.testing.expect(try cache.hasChanged("users", 12345) == true);
    try std.testing.expect(cache.count() == 1);
    try std.testing.expect(cache.get("users").? == 12345);

    // Same relation_id should NOT report as changed
    try std.testing.expect(try cache.hasChanged("users", 12345) == false);
    try std.testing.expect(cache.count() == 1);

    // Different relation_id should report as changed
    try std.testing.expect(try cache.hasChanged("users", 67890) == true);
    try std.testing.expect(cache.get("users").? == 67890);
    try std.testing.expect(cache.count() == 1);
}

test "SchemaCache - multiple tables" {
    const allocator = std.testing.allocator;
    var cache = SchemaCache.init(allocator);
    defer cache.deinit();

    try std.testing.expect(try cache.hasChanged("users", 100) == true);
    try std.testing.expect(try cache.hasChanged("orders", 200) == true);
    try std.testing.expect(try cache.hasChanged("products", 300) == true);
    try std.testing.expect(cache.count() == 3);

    // No changes
    try std.testing.expect(try cache.hasChanged("users", 100) == false);
    try std.testing.expect(try cache.hasChanged("orders", 200) == false);
    try std.testing.expect(try cache.hasChanged("products", 300) == false);

    // One table changes
    try std.testing.expect(try cache.hasChanged("orders", 999) == true);
    try std.testing.expect(cache.get("orders").? == 999);
    try std.testing.expect(cache.count() == 3);
}

test "SchemaCache - clear" {
    const allocator = std.testing.allocator;
    var cache = SchemaCache.init(allocator);
    defer cache.deinit();

    try std.testing.expect(try cache.hasChanged("users", 100) == true);
    try std.testing.expect(try cache.hasChanged("orders", 200) == true);
    try std.testing.expect(cache.count() == 2);

    cache.clear();
    try std.testing.expect(cache.count() == 0);

    // After clear, tables should be "new" again
    try std.testing.expect(try cache.hasChanged("users", 100) == true);
    try std.testing.expect(cache.count() == 1);
}
