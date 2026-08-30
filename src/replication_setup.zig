//! Replication setup and initialization
//!
//! This module handles PostgreSQL replication setup:
//! - Creating replication slots
//! - Verifying publications exist
//! - Extracting monitored table lists
//!
//! Use `init()` to perform one-time setup and get the list of monitored tables.

const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const pg_conn = @import("pg_conn.zig");
const utils = @import("utils.zig");

pub const log = std.log.scoped(.replication_setup);

/// Replication context - holds the list of monitored tables from the publication
///
/// This is the simplified result of replication setup initialization.
/// Use `init()` to create and `deinit()` to clean up.
pub const ReplicationContext = struct {
    allocator: std.mem.Allocator,
    tables: [][]const u8, // Owned strings in "schema.table" format
    /// True when THIS boot created the slot. A new slot is a new feed: nothing between
    /// the old slot's last confirmed LSN and now was ever published, and the CDC
    /// streams' numbering shows no hole — so the bridge restarts the streams and the
    /// chains, which is the only trace a client can read (NOTES §10bm).
    slot_created: bool = false,

    pub fn deinit(self: *ReplicationContext) void {
        for (self.tables) |table| {
            self.allocator.free(table);
        }
        self.allocator.free(self.tables);
    }
};

/// Publication information returned by checkPublication (internal)
const PublicationInfo = struct {
    allocator: std.mem.Allocator,
    pubname: []const u8,
    puballtables: bool,
    tables: [][]const u8, // Owned strings: schema.table format

    pub fn deinit(self: *PublicationInfo) void {
        for (self.tables) |table| {
            self.allocator.free(table);
        }
        self.allocator.free(self.tables);
        self.allocator.free(self.pubname);
    }
};

/// Struct to handle replication setup tasks and includes get current WAL LSN.
///
/// Includes creating|droping replication slots and publications.
pub const ReplicationSetup = struct {
    allocator: std.mem.Allocator,
    pg_config: pg_conn.PgConf,

    /// Create a replication slot if it doesn't exist (hybrid: try create, fallback to verify)
    ///
    /// This function attempts to create a replication slot. If it already exists, it verifies
    /// and continues. If creation fails due to permissions, it checks if the slot exists
    /// (perhaps created by an admin) and uses it.
    ///
    /// Privilege requirements:
    /// - To CREATE: Requires REPLICATION privilege
    /// - To VERIFY: Requires ability to query pg_replication_slots
    /// Refuse an INVALIDATED slot at boot. PostgreSQL drops the WAL a slot retains once
    /// it outgrows max_slot_wal_keep_size and marks the slot `wal_status = lost`; the
    /// row stays, so "exists" alone would let START_REPLICATION fail and the reconnect
    /// loop retry a dead slot every two seconds forever (found by
    /// scripts/scenarios/slot_loss.py, 2026-08-29). Say what happened and how to recover.
    fn refuseLostSlot(self: *const ReplicationSetup, conn: *c.PGconn, slot_name: []const u8) !void {
        const q = try utils.allocPrintZ(self.allocator,
            "SELECT wal_status, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) " ++
                "FROM pg_replication_slots WHERE slot_name = '{s}'", .{slot_name});
        defer self.allocator.free(q);
        const res = runQuery(conn, q) catch return;
        defer c.PQclear(res);
        if (c.PQntuples(res) == 0) return;
        const status = std.mem.span(c.PQgetvalue(res, 0, 0));
        if (std.mem.eql(u8, status, "lost")) {
            log.err("🔴 FATAL: replication slot '{s}' is INVALIDATED (wal_status = lost): PostgreSQL discarded the WAL it retained for this bridge (max_slot_wal_keep_size). Every change since is gone from the feed; no restart can recover it.", .{slot_name});
            log.err("   Recovery: SELECT pg_drop_replication_slot('{s}'); then start the bridge ONCE with ZB_FEED_RESTART=1 — it creates a new slot, restarts the CDC streams and the chains, and every client re-seeds from a fresh full (NOTES §10bm).", .{slot_name});
            return error.SlotInvalidated;
        }
        if (std.mem.eql(u8, status, "unreserved")) {
            log.warn("⚠️ replication slot '{s}' is 'unreserved' ({s} of WAL behind): PostgreSQL may invalidate it at the next checkpoint — catch up now, or raise max_slot_wal_keep_size", .{ slot_name, std.mem.span(c.PQgetvalue(res, 0, 1)) });
        }
    }

    /// Returns true when this call CREATED the slot — a new feed, see ReplicationContext.
    pub fn createSlot(
        self: *const ReplicationSetup,
        slot_name: []const u8,
    ) !bool {
        const conn = try self.connect();
        defer c.PQfinish(conn);

        // Check if slot exists
        const check_query = try utils.allocPrintZ(
            self.allocator,
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{s}'",
            .{slot_name},
        );
        defer self.allocator.free(check_query);

        const check_result = try runQuery(
            conn,
            check_query,
        );
        defer c.PQclear(check_result);
        log.debug("Checking for replication slot '{s}'...", .{slot_name});

        const exists = c.PQntuples(check_result) > 0;

        if (exists) {
            try self.refuseLostSlot(conn, slot_name);
            log.info("✅ Replication slot '{s}' already exists", .{slot_name});
            return false;
        }

        // Try to create the slot
        const create_query = try utils.allocPrintZ(
            self.allocator,
            "SELECT pg_create_logical_replication_slot('{s}', 'pgoutput')",
            .{slot_name},
        );
        defer self.allocator.free(create_query);

        const create_result = runQuery(conn, create_query) catch |err| {
            // Creation failed - check if slot now exists (race condition or permission issue)
            const recheck_result = runQuery(conn, check_query) catch {
                log.err("🔴 Failed to create replication slot '{s}' and cannot verify existence", .{slot_name});
                log.err("   → Ensure an admin has created the slot or grant REPLICATION privilege", .{});
                return err;
            };
            defer c.PQclear(recheck_result);

            if (c.PQntuples(recheck_result) > 0) {
                log.info("✅ Replication slot '{s}' exists (created externally)", .{slot_name});
                return false;
            }

            log.err("🔴 Failed to create replication slot '{s}'", .{slot_name});
            log.err("   → Ask your database administrator to run:", .{});
            log.err("      SELECT pg_create_logical_replication_slot('{s}', 'pgoutput');", .{slot_name});
            return err;
        };
        defer c.PQclear(create_result);

        log.info("✅ Replication slot '{s}' created", .{slot_name});
        return true;
    }

    /// Check that a publication exists and return its table list
    ///
    /// In production, the database administrator should pre-create the publication
    /// using SUPERUSER privileges. This function verifies it exists and queries
    /// which tables it contains.
    ///
    /// The bridge user only needs REPLICATION privilege to use an existing publication.
    ///
    /// Production deployment:
    ///   CREATE PUBLICATION cdc_pub FOR ALL TABLES;
    ///   -- or --
    ///   CREATE PUBLICATION cdc_pub FOR TABLE users, orders;
    ///
    /// Privilege requirements:
    /// - To VERIFY: Requires ability to query pg_publication and pg_publication_tables
    /// - To USE: Requires REPLICATION privilege
    ///
    /// Returns: PublicationInfo with table list (caller must call .deinit())
    pub fn checkPublication(
        self: *const ReplicationSetup,
        pub_name: []const u8,
    ) !PublicationInfo {
        const conn = try self.connect();
        defer c.PQfinish(conn);

        // Query publication and its tables
        const check_query = try utils.allocPrintZ(
            self.allocator,
            \\SELECT
            \\  p.pubname,
            \\  p.puballtables::text,
            \\  COALESCE(
            \\    string_agg(pt.schemaname || '.' || pt.tablename, ','),
            \\    ''
            \\  ) as tables
            \\FROM pg_publication p
            \\LEFT JOIN pg_publication_tables pt ON pt.pubname = p.pubname
            \\WHERE p.pubname = '{s}'
            \\GROUP BY p.pubname, p.puballtables
        ,
            .{pub_name},
        );
        defer self.allocator.free(check_query);

        const check_result = try runQuery(conn, check_query);
        defer c.PQclear(check_result);

        const row_count = c.PQntuples(check_result);
        if (row_count == 0) {
            // Publication doesn't exist - admin must create it
            log.err("🔴 Publication '{s}' not found", .{pub_name});
            log.err("   → Ask your database administrator to run:", .{});
            log.err("      CREATE PUBLICATION {s} FOR ALL TABLES;", .{pub_name});
            return error.PublicationNotFound;
        }

        // Parse result
        const pubname_cstr = c.PQgetvalue(check_result, 0, 0);
        const puballtables_cstr = c.PQgetvalue(check_result, 0, 1);
        const tables_cstr = c.PQgetvalue(check_result, 0, 2);

        const pubname = try self.allocator.dupe(u8, std.mem.span(pubname_cstr));
        const puballtables = std.mem.eql(u8, std.mem.span(puballtables_cstr), "t");

        // Parse comma-separated table list
        var table_list: std.ArrayList([]const u8) = .empty;
        errdefer {
            for (table_list.items) |table| {
                self.allocator.free(table);
            }
            table_list.deinit(self.allocator);
        }

        const tables_str = std.mem.span(tables_cstr);
        if (tables_str.len > 0) {
            var iter = std.mem.splitScalar(u8, tables_str, ',');
            while (iter.next()) |table| {
                const trimmed = std.mem.trim(u8, table, &std.ascii.whitespace);
                if (trimmed.len > 0) {
                    const owned = try self.allocator.dupe(u8, trimmed);
                    try table_list.append(self.allocator, owned);
                }
            }
        }

        const tables = try table_list.toOwnedSlice(self.allocator);

        // Log success
        if (puballtables) {
            log.info("✅  Publication '{s}' verified (FOR ALL TABLES, {d} tables detected)", .{ pub_name, tables.len });
        } else if (tables.len > 0) {
            log.info("✅  Publication '{s}' verified ({d} tables: {s})", .{ pub_name, tables.len, tables_str });
        } else {
            log.warn("⚠️  Publication '{s}' exists but has no tables", .{pub_name});
        }

        return PublicationInfo{
            .pubname = pubname,
            .puballtables = puballtables,
            .tables = tables,
            .allocator = self.allocator,
        };
    }

    /// Drop a replication slot
    pub fn dropSlot(self: *const ReplicationSetup, slot_name: []const u8) !void {
        log.info("Dropping replication slot '{s}'...", .{slot_name});

        const query = try utils.allocPrintZ(
            self.allocator,
            "SELECT pg_drop_replication_slot('{s}')",
            .{slot_name},
        );
        defer self.allocator.free(query);

        const conn = try self.connect();
        defer c.PQfinish(conn);

        const result = try runQuery(conn, query);
        defer c.PQclear(result);

        log.info("✓ Replication slot '{s}' dropped", .{slot_name});
    }

    fn runQuery(conn: *c.PGconn, query: []const u8) !*c.PGresult {
        const result = c.PQexec(conn, query.ptr) orelse {
            log.err("Query execution failed: PQexec returned null", .{});
            return error.QueryFailed;
        };

        if (c.PQresultStatus(result) != c.PGRES_TUPLES_OK and c.PQresultStatus(result) != c.PGRES_COMMAND_OK) {
            const err_msg = c.PQerrorMessage(conn);
            log.err("Query failed: {s}", .{err_msg});
            c.PQclear(result);
            return error.QueryFailed;
        }

        return result;
    }

    pub fn connect(self: *const ReplicationSetup) !*c.PGconn {
        // Build connection string
        const conninfo = try self.pg_config.connInfo(
            self.allocator,
            self.pg_config.replication,
        );
        defer self.allocator.free(conninfo);

        if (conninfo.len == 0) {
            log.err("Invalid configuration: connection string is empty", .{});
            return error.InvalidConfig;
        }

        const conn = c.PQconnectdb(conninfo.ptr) orelse {
            log.err("🔴 Connection failed: PQconnectdb returned null", .{});
            return error.ConnectionFailed;
        };

        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            const err_msg = c.PQerrorMessage(conn);
            log.err("🔴 Connection failed: {s}", .{err_msg});
            c.PQfinish(conn);
            return error.ConnectionFailed;
        }

        return conn;
    }
};

/// Initialize replication setup - one-time setup function
///
/// This function combines slot creation and publication verification into a single call.
/// It creates the replication slot (if needed) and verifies the publication exists,
/// returning only the list of monitored tables.
///
/// Args:
///   allocator: Memory allocator
///   pg_config: PostgreSQL connection configuration
///   slot_name: Replication slot name (null-terminated)
///   publication_name: Publication name (null-terminated)
///
/// Returns:
///   ReplicationContext with monitored tables list
///   Caller must call .deinit() to free resources
pub fn init(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    slot_name: [:0]const u8,
    publication_name: [:0]const u8,
) !ReplicationContext {
    const setup = ReplicationSetup{
        .allocator = allocator,
        .pg_config = pg_config.*,
    };

    // Create replication slot (if it doesn't exist)
    log.info("Setting up replication slot '{s}'...", .{slot_name});
    const slot_created = try setup.createSlot(slot_name);

    // Verify publication and get table list
    log.info("Checking publication '{s}'...", .{publication_name});
    var pub_info = try setup.checkPublication(publication_name);
    defer pub_info.deinit();

    // Transfer ownership of tables to ReplicationContext
    // We take the tables slice and let pub_info.deinit() handle the rest
    const tables = pub_info.tables;
    pub_info.tables = &[_][]const u8{}; // Empty slice so deinit() doesn't free tables

    return ReplicationContext{
        .slot_created = slot_created,
        .allocator = allocator,
        .tables = tables,
    };
}
