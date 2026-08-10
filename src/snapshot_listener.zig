//! Snapshot request listener and generator
//!
//! Runs in a dedicated thread to:
//! 1. Subscribe to NATS 'snapshot.request.>' subject
//! 2. Generate incremental snapshots in chunks using COPY
//! 3. Publish snapshot chunks to NATS INIT stream

const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const pg_conn = @import("pg_conn.zig");
const publication_mod = @import("publication.zig");
const config = @import("config.zig");
const msgpack = @import("msgpack");
const pg_copy_csv = @import("pg_copy_csv.zig");
const encoder_mod = @import("encoder.zig");
const streaming_encoder = @import("streaming_encoder.zig");
const RuntimeConfig = @import("config.zig").RuntimeConfig;
const nats = @import("nats");
const utils = @import("utils.zig");

pub const log = std.log.scoped(.snapshot_listener);

/// Publish to NATS JetStream with retry logic and exponential backoff
/// Matches the retry strategy used in batch_publisher.zig for consistency
/// Checks should_stop flag during retries to allow graceful shutdown
fn publishWithRetry(
    js: *nats.JS,
    subject: []const u8,
    headers: ?*nats.pool.Headers,
    payload: []const u8,
    should_stop: *std.atomic.Value(bool),
) !void {
    var retry_count: u32 = 0;
    const max_retries = 5;
    var backoff_ms: u64 = 100; // Start with 100ms

    while (!should_stop.load(.acquire)) {
        const result = js.PUBLISH(subject, headers, payload);

        if (result) |_| {
            // SUCCESS
            return;
        } else |err| {
            // FAILURE
            log.err("❌ NATS publish failed for {s} (attempt {d}/{d}): {}", .{
                subject,
                retry_count + 1,
                max_retries + 1,
                err,
            });

            if (retry_count >= max_retries) {
                // Exhausted retries
                log.err("🔴 FATAL: Exhausted retries for snapshot publish to {s}", .{subject});
                return err;
            }

            // Wait before retrying (exponential backoff), but check should_stop periodically
            log.warn("⏳ Retrying in {d}ms...", .{backoff_ms});

            // Sleep in small increments to allow faster shutdown response
            const sleep_increment_ms = 50; // Check every 50ms
            var elapsed_ms: u64 = 0;
            while (elapsed_ms < backoff_ms) {
                if (should_stop.load(.acquire)) {
                    log.info("🛑 Shutdown requested during retry backoff - aborting", .{});
                    return error.ShutdownRequested;
                }
                utils.sleep(sleep_increment_ms * std.time.ns_per_ms);
                elapsed_ms += sleep_increment_ms;
            }

            // Double the wait for next time, capped at 5 seconds
            backoff_ms = @min(backoff_ms * 2, 5000);
            retry_count += 1;
        }
    }

    // If we exit the loop due to should_stop, return error
    log.info("🛑 Shutdown requested - aborting publish", .{});
    return error.ShutdownRequested;
}

/// Primary key metadata for a table (used for chunked snapshots)
const PkMetadata = struct {
    name: []const u8,
    is_numeric: bool,

    pub fn deinit(self: PkMetadata, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
    }
};

/// Query PostgreSQL system catalogs to discover the primary key of a table
/// This is critical for chunked snapshots - we need to know which column to use for WHERE > last_value
/// Supports single-column primary keys (composite keys not yet supported)
fn getTablePrimaryKey(
    allocator: std.mem.Allocator,
    conn: *c.PGconn,
    table_name: []const u8,
) !PkMetadata {
    // Parse schema.table or default to public
    var schema: []const u8 = "public";
    var table: []const u8 = table_name;

    if (std.mem.indexOf(u8, table_name, ".")) |dot_idx| {
        schema = table_name[0..dot_idx];
        table = table_name[dot_idx + 1 ..];
    }

    // Query system catalogs for primary key column
    const query = try utils.allocPrintZ(
        allocator,
        \\SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        \\FROM pg_index i
        \\JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        \\WHERE i.indrelid = '"{s}"."{s}"'::regclass
        \\  AND i.indisprimary
        \\  AND array_length(i.indkey, 1) = 1;
    ,
        .{ schema, table },
    );
    defer allocator.free(query);

    const res = c.PQexec(conn, query.ptr);
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        log.err("Failed to query primary key for table '{s}': {s}", .{
            table_name,
            c.PQerrorMessage(conn),
        });
        return error.QueryFailed;
    }

    const ntuples = c.PQntuples(res);
    if (ntuples == 0) {
        log.err("❌ Table '{s}' has no single-column primary key. Chunked snapshots require a PK.", .{table_name});
        return error.NoPrimaryKey;
    }

    if (ntuples > 1) {
        log.err("❌ Table '{s}' has composite primary key. Only single-column PKs are currently supported.", .{table_name});
        return error.CompositePrimaryKey;
    }

    const pk_name = std.mem.span(c.PQgetvalue(res, 0, 0));
    const pk_type = std.mem.span(c.PQgetvalue(res, 0, 1));

    // Determine if quotes are needed in WHERE clause (numeric types don't need quotes)
    const is_numeric = std.mem.indexOf(u8, pk_type, "int") != null or
        std.mem.indexOf(u8, pk_type, "serial") != null or
        std.mem.indexOf(u8, pk_type, "bigserial") != null;

    log.info("📋 Discovered primary key for '{s}': column='{s}', type='{s}', numeric={}", .{
        table_name,
        pk_name,
        pk_type,
        is_numeric,
    });

    return .{
        .name = try allocator.dupe(u8, pk_name),
        .is_numeric = is_numeric,
    };
}

/// Snapshot request context passed to NATS callback
const SnapshotContext = struct {
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    js: *nats.JS, // JetStream connection for publishing
    monitored_tables: []const []const u8,
    format: encoder_mod.Format,
    chunk_size: usize,
    // js_ctx: ?*anyopaque, // JetStream context for KV access (optional)
};

/// Snapshot listener with thread management
pub const SnapshotListener = struct {
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    should_stop: *std.atomic.Value(bool),
    monitored_tables: []const []const u8,
    thread: ?std.Thread = null,
    format: encoder_mod.Format,
    chunk_size: usize,
    io: std.Io,
    nats_host: []const u8,
    nats_user: ?[]const u8,
    nats_pass: ?[]const u8,

    /// Initialize snapshot listener (does not start the thread)
    pub fn init(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        should_stop: *std.atomic.Value(bool),
        monitored_tables: []const []const u8,
        format: encoder_mod.Format,
        runtime_config: *const config.RuntimeConfig,
        io: std.Io,
    ) SnapshotListener {
        return .{
            .allocator = allocator,
            .pg_config = pg_config,
            .should_stop = should_stop,
            .monitored_tables = monitored_tables,
            .thread = null,
            .format = format,
            .chunk_size = runtime_config.snapshot_chunk_size,
            .io = io,
            .nats_host = runtime_config.nats_host,
            .nats_user = runtime_config.nats_user,
            .nats_pass = runtime_config.nats_pass,
        };
    }

    /// Start the snapshot listener thread
    pub fn start(self: *SnapshotListener) !void {
        if (self.thread != null) {
            return error.AlreadyStarted;
        }
        self.thread = try std.Thread.spawn(.{}, listenLoop, .{self});
    }

    /// Join the snapshot listener thread (waits for completion)
    pub fn join(self: *SnapshotListener) void {
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    /// Deinit - cleanup resources (call after join)
    pub fn deinit(self: *SnapshotListener) void {
        _ = self;
    }

    /// Background listening loop (internal)
    fn listenLoop(self: *SnapshotListener) !void {
        // Spawn schema request handler thread (nats.zig)
        log.info("📋 Spawning schema request listener (nats.zig)...", .{});
        const schema_thread = try std.Thread.spawn(.{}, listenForSchemaRequestsZig, .{
            self.allocator,
            self.pg_config,
            self.should_stop,
            self.monitored_tables,
            self.format,
            self.io,
            self.nats_host,
            self.nats_user,
            self.nats_pass,
        });

        // Spawn snapshot request handler thread (pure g41797/nats - no nats.c dependency!)
        log.info("📸 Spawning snapshot request listener (pure g41797/nats)...", .{});
        const snapshot_thread = try std.Thread.spawn(.{}, listenForSnapshotRequestsZig, .{
            self.allocator,
            self.pg_config,
            self.should_stop,
            self.monitored_tables,
            self.format,
            self.chunk_size,
            self.io,
            self.nats_host,
            self.nats_user,
            self.nats_pass,
        });

        // Keep main thread alive - just sleep until stop signal
        while (!self.should_stop.load(.seq_cst)) {
            utils.sleep(100 * std.time.ns_per_ms);
        }

        // Wait for threads to finish
        schema_thread.join();
        snapshot_thread.join();
    }

    // Listens for schema requests on "init.schema" subject and responds with
    // fresh schema data queried from PostgreSQL information_schema.

    /// Column information from information_schema
    const SchemaColumnInfo = struct {
        name: []const u8,
        position: u32,
        data_type: []const u8,
        is_nullable: bool,
        column_default: ?[]const u8,
    };

    /// Schema request handler - listens on "init.schema" and responds with table schemas
    /// Note: Thread functions cannot return errors - all errors must be caught internally
    fn listenForSchemaRequestsZig(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        should_stop: *std.atomic.Value(bool),
        monitored_tables: []const []const u8,
        format: encoder_mod.Format,
        io: std.Io,
        nats_host: []const u8,
        nats_user: ?[]const u8,
        nats_pass: ?[]const u8,
    ) void {
        const reconnect_delay_ms = 2000; // 2 seconds between reconnect attempts

        // Outer reconnection loop
        while (!should_stop.load(.acquire)) {
            log.info("📋 Schema listener: Connecting to NATS with g41797/nats...", .{});

            // Create Core NATS connection
            var core = nats.Core{};
            const connect_opts = nats.protocol.ConnectOpts{ .addr = nats_host, .user = nats_user, .pass = nats_pass };
            core.CONNECT(allocator, connect_opts, io) catch |err| {
                log.err("📋 Schema listener: Failed to connect: {} - retrying in {d}ms", .{ err, reconnect_delay_ms });
                utils.sleep(reconnect_delay_ms * std.time.ns_per_ms);
                continue;
            };
            defer core.DISCONNECT();

            log.info("📋 Schema listener: Connected! Subscribing to 'init.schema'...", .{});

            // Subscribe to init.schema
            const sid = "schema-listener-1";
            core.SUB("init.schema", null, sid) catch |err| {
                log.err("📋 Schema listener: Failed to subscribe: {} - reconnecting", .{err});
                utils.sleep(reconnect_delay_ms * std.time.ns_per_ms);
                continue;
            };

            log.info("📋 Schema listener: ✅ Subscribed to 'init.schema'! Waiting for schema requests...", .{});

            // Listen for schema requests
            while (!should_stop.load(.acquire)) {
                if (core.connection) |conn| {
                    const msg = conn.waitMessageNMT(nats.protocol.SECNS * 5, null) catch |err| {
                        if (err == error.Timeout) {
                            continue; // Normal timeout, keep polling
                        }
                        log.err("📋 Schema listener: Error receiving: {} - reconnecting", .{err});
                        utils.sleep(reconnect_delay_ms * std.time.ns_per_ms);
                        break; // Break inner loop to trigger reconnection
                    };
                    defer conn.reuse(msg);

                    const payload = msg.letter.getPayload() orelse "(empty)";
                    log.info("📋 Schema request received: {s}", .{payload});

                    // Query PostgreSQL for schemas and publish response
                    handleSchemaRequest(
                        allocator,
                        &core,
                        pg_config,
                        monitored_tables,
                        format,
                    ) catch |err| {
                        log.err("📋 Failed to handle schema request: {}", .{err});
                    };
                } else {
                    log.err("📋 Schema listener: Connection lost - reconnecting", .{});
                    break; // Break inner loop to trigger reconnection
                }
            }
        }

        log.info("📋 Schema listener stopping...", .{});
    }

    /// Handle a single schema request by querying PostgreSQL and publishing response
    fn handleSchemaRequest(
        allocator: std.mem.Allocator,
        core: *nats.Core,
        pg_config: *const pg_conn.PgConf,
        monitored_tables: []const []const u8,
        format: encoder_mod.Format,
    ) !void {
        // Create PostgreSQL connection
        const conninfo = try pg_config.connInfo(allocator, false);
        defer allocator.free(conninfo);

        const conn = c.PQconnectdb(conninfo.ptr);
        if (conn == null) return error.ConnectionFailed;
        defer c.PQfinish(conn);

        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            return error.ConnectionFailed;
        }

        // Build IN clause for monitored tables
        var in_clause: std.ArrayList(u8) = .empty;
        defer in_clause.deinit(allocator);

        try in_clause.appendSlice(allocator, "(");
        for (monitored_tables, 0..) |table, i| {
            if (i > 0) try in_clause.appendSlice(allocator, ", ");

            // Extract table name if it's "schema.table" format
            const table_name = blk: {
                if (std.mem.indexOf(u8, table, ".")) |idx| {
                    break :blk table[idx + 1 ..];
                }
                break :blk table;
            };

            try in_clause.appendSlice(allocator, "'");
            try in_clause.appendSlice(allocator, table_name);
            try in_clause.appendSlice(allocator, "'");
        }
        try in_clause.appendSlice(allocator, ")");

        // Query information_schema
        const query = try utils.allocPrintZ(
            allocator,
            \\SELECT
            \\    t.table_schema,
            \\    t.table_name,
            \\    c.column_name,
            \\    c.ordinal_position,
            \\    c.data_type,
            \\    c.is_nullable,
            \\    c.column_default
            \\FROM information_schema.tables t
            \\JOIN information_schema.columns c
            \\    ON t.table_schema = c.table_schema
            \\    AND t.table_name = c.table_name
            \\WHERE t.table_schema = 'public'
            \\    AND t.table_type = 'BASE TABLE'
            \\    AND t.table_name IN {s}
            \\ORDER BY t.table_name, c.ordinal_position
        ,
            .{in_clause.items},
        );
        defer allocator.free(query);

        const result = c.PQexec(conn, query.ptr);
        defer c.PQclear(result);

        if (c.PQresultStatus(result) != c.PGRES_TUPLES_OK) {
            return error.QueryFailed;
        }

        const num_rows: usize = @intCast(c.PQntuples(result));
        if (num_rows == 0) {
            log.warn("📋 No schemas found for monitored tables", .{});
            return;
        }

        // Group columns by table
        var table_schemas = std.StringHashMap(std.ArrayList(SchemaColumnInfo)).init(allocator);
        defer {
            var it = table_schemas.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                for (entry.value_ptr.items) |col| {
                    allocator.free(col.name);
                    allocator.free(col.data_type);
                    if (col.column_default) |d| allocator.free(d);
                }
                entry.value_ptr.deinit(allocator);
            }
            table_schemas.deinit();
        }

        // Parse rows and group by table
        for (0..num_rows) |ui| {
            const i: c_int = @intCast(ui);
            const table_schema = std.mem.span(c.PQgetvalue(result, i, 0));
            const table_name = std.mem.span(c.PQgetvalue(result, i, 1));
            const column_name = std.mem.span(c.PQgetvalue(result, i, 2));
            const position_str = std.mem.span(c.PQgetvalue(result, i, 3));
            const data_type = std.mem.span(c.PQgetvalue(result, i, 4));
            const is_nullable_str = std.mem.span(c.PQgetvalue(result, i, 5));
            const column_default_ptr = c.PQgetvalue(result, i, 6);

            const position = try std.fmt.parseInt(u32, position_str, 10);
            const is_nullable = std.mem.eql(u8, is_nullable_str, "YES");
            const column_default = if (c.PQgetisnull(result, i, 6) == 1)
                null
            else
                std.mem.span(column_default_ptr);

            // Build full table name: schema.table
            const full_table_name = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ table_schema, table_name });

            const entry = try table_schemas.getOrPut(full_table_name);
            if (!entry.found_existing) {
                entry.value_ptr.* = std.ArrayList(SchemaColumnInfo).empty;
            } else {
                allocator.free(full_table_name);
            }

            try entry.value_ptr.append(allocator, .{
                .name = try allocator.dupe(u8, column_name),
                .position = position,
                .data_type = try allocator.dupe(u8, data_type),
                .is_nullable = is_nullable,
                .column_default = if (column_default) |d| try allocator.dupe(u8, d) else null,
            });
        }

        // Publish each table's schema as response
        var it = table_schemas.iterator();
        while (it.next()) |entry| {
            const table_name = entry.key_ptr.*;
            const columns = entry.value_ptr.items;

            try publishSchemaResponse(allocator, core, table_name, columns, format);
        }

        log.info("📋 Published schemas for {d} tables", .{table_schemas.count()});
    }

    /// Publish schema response to NATS using nats.zig
    fn publishSchemaResponse(
        allocator: std.mem.Allocator,
        core: *nats.Core,
        table_name: []const u8,
        columns: []const SchemaColumnInfo,
        format: encoder_mod.Format,
    ) !void {
        // Extract just table name (remove schema prefix)
        const table_only = blk: {
            if (std.mem.indexOf(u8, table_name, ".")) |idx| {
                break :blk table_name[idx + 1 ..];
            }
            break :blk table_name;
        };

        // Build payload
        var encoder = encoder_mod.Encoder.init(allocator, format);
        defer encoder.deinit();

        var schema_map = encoder.createMap();
        defer schema_map.free(allocator);

        try schema_map.put(encoder.allocator, "table", try encoder.createString(table_only));
        try schema_map.put(encoder.allocator, "schema", try encoder.createString(table_name));
        try schema_map.put(encoder.allocator, "timestamp", encoder.createInt(@as(i64, c.time(null))));

        var columns_array = try encoder.createArray(columns.len);
        for (columns, 0..) |col, i| {
            var col_map = encoder.createMap();
            try col_map.put(encoder.allocator, "name", try encoder.createString(col.name));
            try col_map.put(encoder.allocator, "position", encoder.createInt(@intCast(col.position)));
            try col_map.put(encoder.allocator, "data_type", try encoder.createString(col.data_type));
            try col_map.put(encoder.allocator, "is_nullable", encoder.createBool(col.is_nullable));

            if (col.column_default) |default_val| {
                try col_map.put(encoder.allocator, "column_default", try encoder.createString(default_val));
            } else {
                try col_map.put(encoder.allocator, "column_default", encoder.createNull());
            }

            try columns_array.setIndex(i, col_map);
        }
        try schema_map.put(encoder.allocator, "columns", columns_array);

        const encoded = try encoder.encode(schema_map);
        defer allocator.free(encoded);

        // Publish to schema.{table_name} subject
        const subject = try std.fmt.allocPrint(allocator, "schema.{s}", .{table_only});
        defer allocator.free(subject);

        core.PUB(subject, null, encoded) catch |err| {
            log.err("📋 Failed to publish schema for {s}: {}", .{ table_only, err });
            return err;
        };

        log.info("📋 ✅ Published schema → schema.{s} ({d} columns, {d} bytes)", .{
            table_only,
            columns.len,
            encoded.len,
        });
    }

    /// Snapshot request handler - listens on "snapshot.request.>" and generates snapshots
    /// Note: Thread functions cannot return errors - all errors must be caught internally
    fn listenForSnapshotRequestsZig(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        should_stop: *std.atomic.Value(bool),
        monitored_tables: []const []const u8,
        format: encoder_mod.Format,
        chunk_size: usize,
        io: std.Io,
        nats_host: []const u8,
        nats_user: ?[]const u8,
        nats_pass: ?[]const u8,
    ) void {
        const reconnect_delay_ms = 2000; // 2 seconds between reconnect attempts

        // Outer reconnection loop
        while (!should_stop.load(.acquire)) {
            log.info("📸 Snapshot listener (nats.zig): Connecting to NATS...", .{});

            // Create Core NATS connection
            var core = nats.Core{};
            const connect_opts = nats.protocol.ConnectOpts{ .addr = nats_host, .user = nats_user, .pass = nats_pass };
            core.CONNECT(allocator, connect_opts, io) catch |err| {
                log.err("📸 Snapshot listener: Failed to connect: {} - retrying in {d}ms", .{ err, reconnect_delay_ms });
                utils.sleep(reconnect_delay_ms * std.time.ns_per_ms);
                continue;
            };
            defer core.DISCONNECT();

            log.info("📸 Snapshot listener: Connected! Subscribing to 'init.snapshot.>'...", .{});

            // Subscribe to init.snapshot.> wildcard
            const sid = "snapshot-listener-1";
            core.SUB("init.snapshot.>", null, sid) catch |err| {
                log.err("📸 Snapshot listener: Failed to subscribe: {} - reconnecting", .{err});
                utils.sleep(reconnect_delay_ms * std.time.ns_per_ms);
                continue;
            };

            log.info("📸 Snapshot listener: ✅ Subscribed! Waiting for snapshot requests...", .{});

            // Listen for snapshot requests
            while (!should_stop.load(.acquire)) {
                if (core.connection) |conn| {
                    const msg = conn.waitMessageNMT(nats.protocol.SECNS * 5, null) catch |err| {
                        if (err == error.Timeout) {
                            continue; // Normal timeout, keep polling
                        }
                        log.err("📸 Snapshot listener: Error receiving: {} - reconnecting", .{err});
                        utils.sleep(reconnect_delay_ms * std.time.ns_per_ms);
                        break; // Break inner loop to trigger reconnection
                    };
                    defer conn.reuse(msg);

                    // Extract table name from subject: init.snapshot.<table>
                    const subject = msg.letter.subject.body() orelse {
                        log.err("📸 No subject in message", .{});
                        continue;
                    };

                    const table_name = blk: {
                        const prefix = "init.snapshot.";
                        if (std.mem.startsWith(u8, subject, prefix)) {
                            break :blk subject[prefix.len..];
                        }
                        log.err("📸 Invalid snapshot request subject: {s}", .{subject});
                        continue;
                    };

                    log.info("📸 Snapshot request received for table: {s}", .{table_name});

                    // Validate table is monitored
                    const is_monitored = publication_mod.isTableMonitored(table_name, monitored_tables);
                    if (!is_monitored) {
                        log.warn("📸 Table '{s}' not in monitored tables", .{table_name});
                        // TODO: Implement publishSnapshotErrorZig if needed for error reporting
                        // For now, just log the error - consumers won't get notification
                        continue;
                    }

                    // Generate snapshot ID
                    const snapshot_id = generateSnapshotId(allocator) catch |err| {
                        log.err("Failed to generate snapshot ID: {}", .{err});
                        continue;
                    };
                    defer allocator.free(snapshot_id);

                    log.info("📸 Generating snapshot for '{s}' (id={s})", .{ table_name, snapshot_id });

                    // Generate snapshot using pure g41797/nats JetStream
                    generateIncrementalSnapshotZig(
                        allocator,
                        pg_config,
                        table_name,
                        snapshot_id,
                        format,
                        chunk_size,
                        should_stop,
                        io,
                        nats_host,
                        nats_user,
                        nats_pass,
                    ) catch |err| {
                        log.err("📸 Snapshot generation failed for '{s}': {}", .{ table_name, err });
                        // TODO: Implement publishSnapshotErrorZig if needed for error reporting
                        // For now, just log the error - consumers won't get notification
                        continue;
                    };

                    log.info("📸 ✅ Snapshot completed for '{s}'", .{table_name});
                } else {
                    log.err("📸 Snapshot listener: Connection lost - reconnecting", .{});
                    break; // Break inner loop to trigger reconnection
                }
            }
        }

        log.info("📸 Snapshot listener stopping...", .{});
    }

    /// Generate incremental snapshot
    fn generateIncrementalSnapshotZig(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        table_name: []const u8,
        snapshot_id: []const u8,
        format: encoder_mod.Format,
        chunk_size: usize,
        should_stop: *std.atomic.Value(bool),
        io: std.Io,
        nats_host: []const u8,
        nats_user: ?[]const u8,
        nats_pass: ?[]const u8,
    ) !void {
        log.info("📸 Generating snapshot for '{s}' (id={s}, chunk_size={d})", .{
            table_name,
            snapshot_id,
            chunk_size,
        });

        // Create JetStream connection for publishing snapshot data
        const connect_opts = nats.protocol.ConnectOpts{ .addr = nats_host, .user = nats_user, .pass = nats_pass };
        var js = nats.JS.CONNECT(allocator, connect_opts, io) catch |err| {
            log.err("📸 Failed to connect to JetStream: {}", .{err});
            return error.JetStreamConnectionFailed;
        };
        defer js.DISCONNECT();

        log.info("📸 Connected to JetStream for snapshot publishing", .{});

        // Create PostgreSQL connection
        const conninfo = try pg_config.connInfo(allocator, false);
        defer allocator.free(conninfo);

        const conn = c.PQconnectdb(conninfo.ptr);
        if (conn == null) return error.ConnectionFailed;
        defer c.PQfinish(conn);

        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            return error.ConnectionFailed;
        }

        // Begin REPEATABLE READ transaction for snapshot consistency
        const begin_result = c.PQexec(conn, "BEGIN ISOLATION LEVEL REPEATABLE READ");
        defer c.PQclear(begin_result);

        if (c.PQresultStatus(begin_result) != c.PGRES_COMMAND_OK) {
            log.err("BEGIN failed: {s}", .{c.PQerrorMessage(conn)});
            return error.TransactionFailed;
        }

        // Get snapshot LSN
        const lsn_query = "SELECT pg_current_wal_lsn()::text";
        const lsn_result = c.PQexec(conn, lsn_query.ptr);
        defer c.PQclear(lsn_result);

        if (c.PQresultStatus(lsn_result) != c.PGRES_TUPLES_OK) {
            return error.QueryFailed;
        }

        const lsn_str: []const u8 = std.mem.span(c.PQgetvalue(lsn_result, 0, 0));
        log.info("📸 Snapshot started at LSN: {s}", .{lsn_str});

        // Publish snapshot start notification
        publishSnapshotStartZig(
            allocator,
            &js,
            table_name,
            snapshot_id,
            lsn_str,
            format,
            should_stop,
        ) catch |err| {
            log.warn("Failed to publish snapshot start: {}", .{err});
        };

        // Discover primary key for this table
        const pk = try getTablePrimaryKey(allocator, conn.?, table_name);
        defer pk.deinit(allocator);

        // Create arena for snapshot processing
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        // Process chunks using streaming encoder
        var batch: u32 = 0;
        var total_rows: u64 = 0;
        var last_val: []u8 = try allocator.dupe(u8, if (pk.is_numeric) "0" else "");
        defer allocator.free(last_val);

        // Pre-allocate encoding buffer (2MB for chunk data)
        const encode_buffer = try allocator.alloc(u8, 2 * 1024 * 1024);
        defer allocator.free(encode_buffer);

        while (true) {
            _ = arena.reset(.retain_capacity);
            const chunk_alloc = arena.allocator();

            // Build COPY query with dynamic PK column
            // Quote value if string type (UUID, varchar, etc.)
            const where_clause = if (pk.is_numeric)
                try std.fmt.allocPrint(chunk_alloc, "\"{s}\" > {s}", .{ pk.name, last_val })
            else if (last_val.len == 0)
                try std.fmt.allocPrint(chunk_alloc, "1=1", .{})
            else
                try std.fmt.allocPrint(chunk_alloc, "\"{s}\" > '{s}'", .{ pk.name, last_val });

            const copy_query = try utils.allocPrintZ(
                chunk_alloc,
                "COPY (SELECT * FROM \"{s}\" WHERE {s} ORDER BY \"{s}\" LIMIT {d}) TO STDOUT WITH (FORMAT csv, HEADER true)",
                .{ table_name, where_clause, pk.name, chunk_size },
            );

            // Initialize streaming encoder with fixed buffer
            var encoder = streaming_encoder.StreamingEncoder.init(encode_buffer);

            // Stream: PostgreSQL COPY → Parser → Encoder (zero intermediate allocations)
            var parser = pg_copy_csv.CopyCsvParser.init(chunk_alloc, @ptrCast(conn));
            defer parser.deinit();

            const num_rows = parser.streamToEncoder(copy_query, &encoder, pk.name) catch |err| {
                log.err("Stream encoding failed: {}", .{err});
                _ = c.PQexec(conn, "ROLLBACK");
                return error.StreamEncodingFailed;
            };

            if (num_rows == 0) break;

            total_rows += num_rows;

            // On first chunk, publish schema so consumer knows column order
            if (batch == 0) {
                const col_names = parser.columnNames() orelse return error.NoHeader;
                try publishSchemaZig(
                    chunk_alloc,
                    &js,
                    table_name,
                    snapshot_id,
                    col_names,
                    format,
                    should_stop,
                );
            }

            // Get the encoded MessagePack data (no allocation - slice of encode_buffer)
            const encoded = encoder.getWritten();

            const payload = encoded;

            // Publish chunk to JetStream with Nats-Msg-Id header for deduplication
            // Use chunk_alloc (arena) for all per-chunk allocations
            const subject = try std.fmt.allocPrint(
                chunk_alloc,
                config.Snapshot.data_subject_pattern,
                .{ table_name, snapshot_id, batch },
            );

            const msg_id_buf = try std.fmt.allocPrint(
                chunk_alloc,
                config.Snapshot.data_msg_id_pattern,
                .{ table_name, snapshot_id, batch },
            );

            // Create headers with metadata for versioning and deduplication
            var headers = nats.pool.Headers{};
            try headers.init(chunk_alloc, 512);
            defer headers.deinit();

            // Deduplication
            try headers.append("Nats-Msg-Id", msg_id_buf);

            // Content metadata
            try headers.append("Content-Type", "application/msgpack");

            // Snapshot versioning
            try headers.append("X-Format", "array"); // Tells consumer: expect [[v1,v2,...]]
            try headers.append("X-Snapshot-Version", "1.0");

            // Schema reference (will be published separately)
            const schema_ref = try std.fmt.allocPrint(chunk_alloc, "{s}.{s}", .{ table_name, snapshot_id });
            try headers.append("X-Schema-Ref", schema_ref);

            // Progress tracking for consumers
            const chunk_num_str = try std.fmt.allocPrint(chunk_alloc, "{d}", .{batch});
            try headers.append("X-Snapshot-Chunk-Num", chunk_num_str);

            const rows_in_chunk_str = try std.fmt.allocPrint(chunk_alloc, "{d}", .{num_rows});
            try headers.append("X-Snapshot-Rows-In-Chunk", rows_in_chunk_str);

            const total_rows_str = try std.fmt.allocPrint(chunk_alloc, "{d}", .{total_rows});
            try headers.append("X-Snapshot-Total-Rows-So-Far", total_rows_str);

            // Flag final chunk when we receive fewer rows than chunk_size
            if (num_rows < chunk_size) {
                try headers.append("X-Snapshot-Final-Chunk", "true");
            }

            // Publish to JetStream with headers and retry logic
            try publishWithRetry(&js, subject, &headers, payload, should_stop);

            log.info("📦 Published chunk {d} ({d} rows, {d} bytes) → {s} (msg_id={s})", .{
                batch,
                num_rows,
                payload.len,
                subject,
                msg_id_buf,
            });

            batch += 1;

            // Advance keyset cursor using the actual last PK value captured during streaming.
            // This is correct for gaps, deletions, and non-sequential PKs (e.g. UUIDs).
            const pk_val = parser.getLastPkValue() orelse {
                log.err("⚠️  No PK value captured from chunk — aborting snapshot", .{});
                _ = c.PQexec(conn, "ROLLBACK");
                return error.PkValueMissing;
            };
            allocator.free(last_val);
            last_val = try allocator.dupe(u8, pk_val);

            // Break if we got fewer rows than requested (end of table)
            if (num_rows < chunk_size) break;
        }

        // Commit transaction
        const commit_result = c.PQexec(conn, "COMMIT");
        defer c.PQclear(commit_result);

        if (c.PQresultStatus(commit_result) != c.PGRES_COMMAND_OK) {
            log.err("COMMIT failed: {s}", .{c.PQerrorMessage(conn)});
            return error.TransactionFailed;
        }

        log.info("✅ Transaction committed", .{});

        // Publish metadata
        try publishSnapshotMetadataZig(
            allocator,
            &js,
            table_name,
            snapshot_id,
            lsn_str,
            batch,
            total_rows,
            format,
            should_stop,
        );

        log.info("✅ Snapshot complete: {s} ({d} batches, {d} rows)", .{
            snapshot_id,
            batch,
            total_rows,
        });
    }

    /// Publish snapshot start notification using g41797/nats JetStream
    fn publishSnapshotStartZig(
        allocator: std.mem.Allocator,
        js: *nats.JS,
        table_name: []const u8,
        snapshot_id: []const u8,
        lsn: []const u8,
        format: encoder_mod.Format,
        should_stop: *std.atomic.Value(bool),
    ) !void {
        var encoder = encoder_mod.Encoder.init(allocator, format);
        defer encoder.deinit();

        var start_map = encoder.createMap();
        defer start_map.free(allocator);

        // Parse PostgreSQL LSN string to u64 integer
        const lsn_int = try parsePgLsn(lsn);

        try start_map.put(encoder.allocator, "snapshot_id", try encoder.createString(snapshot_id));
        try start_map.put(encoder.allocator, "table", try encoder.createString(table_name));
        try start_map.put(encoder.allocator, "lsn", encoder.createInt(@intCast(lsn_int)));
        try start_map.put(encoder.allocator, "timestamp", encoder.createInt(@as(i64, c.time(null))));
        try start_map.put(encoder.allocator, "status", try encoder.createString("starting"));
        try start_map.put(encoder.allocator, "format", try encoder.createString(@tagName(format)));

        const encoded = try encoder.encode(start_map);
        defer allocator.free(encoded);

        const subject = try std.fmt.allocPrint(
            allocator,
            config.Snapshot.start_subject_pattern,
            .{table_name},
        );
        defer allocator.free(subject);

        // Publish to JetStream with retry logic (no msg_id needed for start notification)
        try publishWithRetry(js, subject, null, encoded, should_stop);

        log.info("🚀 Published snapshot start → {s} (LSN watermark: {s})", .{ subject, lsn });
    }

    /// Publish snapshot metadata using g41797/nats JetStream
    fn publishSnapshotMetadataZig(
        allocator: std.mem.Allocator,
        js: *nats.JS,
        table_name: []const u8,
        snapshot_id: []const u8,
        lsn: []const u8,
        batch_count: u32,
        row_count: u64,
        format: encoder_mod.Format,
        should_stop: *std.atomic.Value(bool),
    ) !void {
        var encoder = encoder_mod.Encoder.init(allocator, format);
        defer encoder.deinit();

        var meta_map = encoder.createMap();
        defer meta_map.free(allocator);

        // Parse PostgreSQL LSN string to u64 integer
        const lsn_int = try parsePgLsn(lsn);

        try meta_map.put(encoder.allocator, "snapshot_id", try encoder.createString(snapshot_id));
        try meta_map.put(encoder.allocator, "lsn", encoder.createInt(@intCast(lsn_int)));
        try meta_map.put(encoder.allocator, "timestamp", encoder.createInt(@as(i64, c.time(null))));
        try meta_map.put(encoder.allocator, "batch_count", encoder.createInt(@intCast(batch_count)));
        try meta_map.put(encoder.allocator, "row_count", encoder.createInt(@intCast(row_count)));
        try meta_map.put(encoder.allocator, "table", try encoder.createString(table_name));

        const encoded = try encoder.encode(meta_map);
        defer allocator.free(encoded);

        const subject = try std.fmt.allocPrint(
            allocator,
            config.Snapshot.meta_subject_pattern,
            .{table_name},
        );
        defer allocator.free(subject);

        // Create headers for metadata message
        var headers = nats.pool.Headers{};
        try headers.init(allocator, 256);
        defer headers.deinit();

        try headers.append("Content-Type", "application/msgpack");
        try headers.append("X-Snapshot-Version", "1.0");
        try headers.append("X-Message-Type", "snapshot-complete");

        // Publish to JetStream with retry logic
        try publishWithRetry(js, subject, &headers, encoded, should_stop);

        log.info("📋 Published snapshot metadata → {s}", .{subject});
    }

    // ========================================================================
    // PoC: g41797/nats Core NATS subscriber (Testing Zig 0.15+ compatibility)
    // ========================================================================
    // This PoC demonstrates using the g41797/nats pure Zig library for Core NATS
    // pub/sub instead of nats.c. Successfully vendored with:
    // - mailbox.zig vendored into src/nats/src/mailbox.zig
    // - zul dependency replaced with std.crypto for UUID generation
    // ========================================================================

    // / PoC: Core NATS subscriber (no JetStream, just pub/sub)
    // / Note: Thread functions cannot return errors - all errors must be caught internally
    // fn testCoreNatsSubscriber(
    //     allocator: std.mem.Allocator,
    //     should_stop: *std.atomic.Value(bool),
    // ) void {
    //     log.info("🧪 PoC: Connecting to Core NATS with g41797/nats...", .{});

    //     // Create Core NATS connection
    //     var core = nats.Core{};
    //     const connect_opts = nats.protocol.ConnectOpts{}; // Use defaults
    //     core.CONNECT(allocator, connect_opts) catch |err| {
    //         log.err("🧪 PoC: Failed to connect: {}", .{err});
    //         return; // Cannot propagate error from thread function
    //     };
    //     defer core.DISCONNECT();

    //     log.info("🧪 PoC: Connected! Subscribing to 'init.schema' with Core NATS...", .{});

    //     // Subscribe to init.schema
    //     const sid = "1"; // Subscription ID
    //     core.SUB("init.schema", null, sid) catch |err| {
    //         log.err("🧪 PoC: Failed to subscribe: {}", .{err});
    //         return; // Cannot propagate error from thread function
    //     };

    //     log.info("🧪 PoC: Subscribed! Waiting for messages from Elixir on 'init.schema'...", .{});

    //     // Listen for messages (using internal connection)
    //     while (!should_stop.load(.acquire)) {
    //         if (core.connection) |conn| {
    //             const msg = conn.waitMessageNMT(nats.protocol.SECNS * 5, null) catch |err| {
    //                 if (err == error.Timeout) {
    //                     log.debug("🧪 PoC: No message (5s timeout)", .{});
    //                     continue;
    //                 }
    //                 log.err("🧪 PoC: Error receiving: {}", .{err});
    //                 utils.sleep(1 * std.time.ns_per_s);
    //                 continue;
    //             };
    //             defer conn.reuse(msg);

    //             const payload = msg.letter.getPayload() orelse "(empty)";
    //             const subject = msg.letter.subject.body() orelse "(no subject)";
    //             log.info("🧪 PoC: ✅ Received from Elixir on '{s}': {s}", .{
    //                 subject,
    //                 payload,
    //             });
    //         } else {
    //             log.err("🧪 PoC: Connection lost", .{});
    //             break;
    //         }
    //     }

    //     log.info("🧪 PoC: Subscriber stopping...", .{});
    // }
};

/// Publish snapshot error to NATS for consumer feedback
fn publishSnapshotError(
    allocator: std.mem.Allocator,
    js: *nats.JS,
    table_name: []const u8,
    error_type: []const u8,
    available_tables: []const []const u8,
    snapshot_id: ?[]const u8,
    error_message: ?[]const u8,
    format: encoder_mod.Format,
    should_stop: *std.atomic.Value(bool),
) !void {
    const subject = try std.fmt.allocPrint(
        allocator,
        config.Snapshot.error_subject_pattern,
        .{table_name},
    );
    defer allocator.free(subject);

    var encoder = encoder_mod.Encoder.init(allocator, format);
    defer encoder.deinit();

    var map = encoder.createMap();
    defer map.free(allocator);

    try map.put(encoder.allocator, "error_type", try encoder.createString(error_type));
    try map.put(encoder.allocator, "table", try encoder.createString(table_name));
    try map.put(encoder.allocator, "timestamp", encoder.createInt(@as(i64, c.time(null))));
    try map.put(encoder.allocator, "status", try encoder.createString("failed"));

    // Optional fields
    if (snapshot_id) |sid| {
        try map.put(encoder.allocator, "snapshot_id", try encoder.createString(sid));
    }
    if (error_message) |msg| {
        try map.put(encoder.allocator, "error_message", try encoder.createString(msg));
    }

    // Create array for available_tables
    var tables_array = try encoder.createArray(available_tables.len);
    for (available_tables, 0..) |table, i| {
        try tables_array.setIndex(i, try encoder.createString(table));
    }
    try map.put(encoder.allocator, "available_tables", tables_array);

    const payload = try encoder.encode(map);
    defer allocator.free(payload);

    // Publish to JetStream with retry logic (Core NATS pub goes through JetStream connection)
    try publishWithRetry(js, subject, null, payload, should_stop);

    log.err("❌ Published snapshot error → {s}: {s}", .{ subject, error_type });
}

/// NATS message callback for snapshot requests
fn onSnapshotRequest(
    _: ?*c.natsConnection,
    sub: ?*c.natsSubscription,
    msg: ?*c.natsMsg,
    closure: ?*anyopaque,
) callconv(.c) void {
    _ = sub;

    const ctx: *SnapshotContext = @ptrCast(@alignCast(closure));

    defer c.natsMsg_Destroy(msg);

    // Extract table name from subject: snapshot.request.<table>
    const subject_ptr = c.natsMsg_GetSubject(msg);
    const subject = std.mem.span(subject_ptr);

    const table_name = blk: {
        if (std.mem.startsWith(u8, subject, config.Snapshot.request_subject_prefix)) {
            break :blk subject[config.Snapshot.request_subject_prefix.len..];
        }
        log.err("⚠️ Invalid snapshot request subject: {s}", .{subject});
        return;
    };

    log.info("📩 Snapshot request via NATS: table='{s}'", .{table_name});

    // Validate table is in monitored tables list
    const is_monitored = publication_mod.isTableMonitored(table_name, ctx.monitored_tables);

    if (!is_monitored) {
        log.warn("⚠️ Snapshot requested for non-monitored table '{s}' (not in publication)", .{table_name});

        // Publish error to NATS so consumer gets feedback
        publishSnapshotError(
            ctx.allocator,
            ctx.js,
            table_name,
            "table_not_in_publication",
            ctx.monitored_tables,
            null, // no snapshot_id yet
            null, // no error_message
            ctx.format,
        ) catch |err| {
            log.err("Failed to publish snapshot error: {}", .{err});
        };

        return;
    }

    // Get request metadata from message payload (MessagePack: requested_by, etc.)
    const data_ptr = c.natsMsg_GetData(msg);
    const data_len: usize = @intCast(c.natsMsg_GetDataLength(msg));

    const requested_by = if (data_len > 0) blk: {
        const payload = data_ptr[0..data_len];
        // Try to parse MessagePack for requested_by field
        // For now, just use "nats-consumer"
        _ = payload;
        break :blk "nats-consumer";
    } else "unknown";

    log.info("🔄 Processing snapshot request for table '{s}' (requested_by: {s})", .{
        table_name,
        requested_by,
    });

    // Generate snapshot ID
    const snapshot_id = generateSnapshotId(ctx.allocator) catch |err| {
        log.err("Failed to generate snapshot ID: {}", .{err});
        return;
    };
    defer ctx.allocator.free(snapshot_id);

    // Generate snapshot
    generateIncrementalSnapshot(
        ctx.allocator,
        ctx.pg_config,
        ctx.js,
        null, // No PostgreSQL connection needed (we create our own)
        table_name,
        snapshot_id,
        ctx.format,
        ctx.chunk_size,
        ctx.js_ctx,
    ) catch |err| {
        const error_message = std.fmt.allocPrint(
            ctx.allocator,
            "Snapshot generation failed: {}",
            .{err},
        ) catch "Unknown error";
        defer if (!std.mem.eql(u8, error_message, "Unknown error")) {
            ctx.allocator.free(error_message);
        };

        log.err("Snapshot generation failed for table '{s}': {}", .{ table_name, err });

        // Publish error notification to NATS for consumer feedback
        publishSnapshotError(
            ctx.allocator,
            ctx.js,
            table_name,
            @errorName(err),
            ctx.monitored_tables,
            snapshot_id,
            error_message,
            ctx.format,
        ) catch |pub_err| {
            log.err("Failed to publish snapshot error notification: {}", .{pub_err});
        };

        return;
    };

    log.info("✅ Snapshot request for '{s}' completed successfully", .{table_name});
}

/// DEPRECATED: Old nats.c-based snapshot listener (kept for reference)
/// Use listenForSnapshotRequestsZig instead (pure g41797/nats)
fn listenForSnapshotRequestsOld(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    nc: ?*c.natsConnection,
    should_stop: *std.atomic.Value(bool),
    monitored_tables: []const []const u8,
    format: encoder_mod.Format,
    chunk_size: usize,
    js_ctx: ?*anyopaque,
) !void {
    log.info("🔔 Starting NATS snapshot listener thread", .{});

    // Create JetStream connection
    const connect_opts = nats.protocol.ConnectOpts{};
    // listenForSnapshotRequestsOld is deprecated; io is not propagated here
    var js = nats.JS.CONNECT(allocator, connect_opts, undefined) catch |err| {
        log.err("Failed to connect to JetStream: {}", .{err});
        return error.JetStreamConnectionFailed;
    };
    defer js.DISCONNECT();

    // Create context for NATS callback
    var ctx = SnapshotContext{
        .allocator = allocator,
        .pg_config = pg_config,
        .js = &js,
        .monitored_tables = monitored_tables,
        .format = format,
        .chunk_size = chunk_size,
        .js_ctx = js_ctx,
    };

    // Subscribe to snapshot.request.> (wildcard for all tables)
    var sub: ?*c.natsSubscription = null;
    const status = c.natsConnection_Subscribe(
        &sub,
        nc,
        config.Snapshot.request_subject_wildcard,
        onSnapshotRequest,
        &ctx,
    );

    if (status != c.NATS_OK) {
        log.err("Failed to subscribe to {s}: {s}", .{
            config.Snapshot.request_subject_wildcard,
            std.mem.span(c.natsStatus_GetText(status)),
        });
        return error.SubscribeFailed;
    }
    defer c.natsSubscription_Destroy(sub);

    // Flush to ensure server processed the subscription
    // This sends PING and waits for PONG to verify subscription was registered
    const flush_status = c.natsConnection_Flush(nc);
    if (flush_status != c.NATS_OK) {
        log.err("⚠️ Failed to flush after subscription: {s}", .{
            std.mem.span(c.natsStatus_GetText(flush_status)),
        });
        return error.FlushFailed;
    }

    // Check if server had any errors processing the subscription
    var last_err_text: [*c]const u8 = null;
    const last_err = c.natsConnection_GetLastError(nc, &last_err_text);
    if (last_err != c.NATS_OK) {
        const err_msg = if (last_err_text != null) std.mem.span(last_err_text) else "unknown";
        log.err("Server error after subscription: {s}", .{err_msg});
        return error.SubscriptionError;
    }

    log.info("🔔 Subscribed to NATS subject 'snapshot.request.>' for snapshot requests", .{});

    // Keep thread alive until stop signal
    while (!should_stop.load(.seq_cst)) {
        utils.sleep(100 * std.time.ns_per_ms);
    }

    log.info("🥁 Snapshot listener thread stopped", .{});

    // REMOVED: nats_ReleaseThreadMemory() no longer needed with pure Zig NATS
    // Pure Zig NATS doesn't have thread-local storage that needs cleanup
}

/// Generate incremental snapshot in chunks and publish to NATS
fn generateIncrementalSnapshot(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    js: *nats.JS,
    _: ?*c.PGconn, // Original connection (not used, we create a new one for snapshot query)
    table_name: []const u8,
    snapshot_id: []const u8,
    format: encoder_mod.Format,
    chunk_size: usize,
    js_ctx: ?*anyopaque, // JetStream context for KV access (optional)
    should_stop: *std.atomic.Value(bool),
) !void {
    log.info("🔄 Generating incremental snapshot for table '{s}' (snapshot_id={s})", .{
        table_name,
        snapshot_id,
    });

    _ = js_ctx;

    // Create a separate connection for snapshot query
    const conninfo = try pg_config.connInfo(allocator, false);
    defer allocator.free(conninfo);

    const conn = c.PQconnectdb(conninfo.ptr);
    if (conn == null) return error.ConnectionFailed;
    defer c.PQfinish(conn);

    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        return error.ConnectionFailed;
    }

    // Begin transaction with REPEATABLE READ isolation for snapshot consistency
    // This ensures all COPY queries see the same database state
    const begin_result = c.PQexec(conn, "BEGIN ISOLATION LEVEL REPEATABLE READ");
    defer c.PQclear(begin_result);

    if (c.PQresultStatus(begin_result) != c.PGRES_COMMAND_OK) {
        log.err("BEGIN REPEATABLE READ failed: {s}", .{c.PQerrorMessage(conn)});
        return error.TransactionFailed;
    }

    // Get snapshot LSN AFTER beginning transaction
    // This LSN represents the consistent point in WAL for this snapshot
    const lsn_query = "SELECT pg_current_wal_lsn()::text";
    const lsn_result = c.PQexec(conn, lsn_query.ptr);
    defer c.PQclear(lsn_result);

    if (c.PQresultStatus(lsn_result) != c.PGRES_TUPLES_OK) {
        return error.QueryFailed;
    }

    const lsn_str: []const u8 = std.mem.span(c.PQgetvalue(lsn_result, 0, 0));

    log.info("📸 Snapshot transaction started at LSN: {s}", .{lsn_str});

    // Publish snapshot start notification with LSN watermark
    // Consumers use this to filter CDC events with LSN < snapshot_lsn
    publishSnapshotStart(
        allocator,
        js,
        table_name,
        snapshot_id,
        lsn_str,
        format,
        should_stop,
    ) catch |err| {
        log.warn("Failed to publish snapshot start notification: {}", .{err});
    };

    // Create arena allocator for snapshot processing (reused across all chunks)
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // Discover primary key for this table
    const pk = try getTablePrimaryKey(allocator, conn.?, table_name);
    defer pk.deinit(allocator);

    // Use COPY CSV format to fetch rows in chunks
    // Use WHERE pk > last_val instead of OFFSET for better performance on large tables
    var batch: u32 = 0;
    var total_rows: u64 = 0;
    var last_val: []const u8 = try allocator.dupe(u8, if (pk.is_numeric) "0" else "");
    defer allocator.free(last_val);

    while (true) {
        // Reset arena for this chunk (retains capacity for efficiency)
        _ = arena.reset(.retain_capacity);
        const chunk_alloc = arena.allocator();

        // Build COPY CSV query with dynamic PK column
        // Quote value if string type (UUID, varchar, etc.)
        const where_clause = if (pk.is_numeric)
            try std.fmt.allocPrint(chunk_alloc, "\"{s}\" > {s}", .{ pk.name, last_val })
        else
            try std.fmt.allocPrint(chunk_alloc, "\"{s}\" > '{s}'", .{ pk.name, last_val });

        const copy_query = try utils.allocPrintZ(
            chunk_alloc,
            "COPY (SELECT * FROM \"{s}\" WHERE {s} ORDER BY \"{s}\" LIMIT {d}) TO STDOUT WITH (FORMAT csv, HEADER true)",
            .{ table_name, where_clause, pk.name, chunk_size },
        );

        // Parse CSV COPY data using arena allocator
        var parser = pg_copy_csv.CopyCsvParser.init(
            chunk_alloc,
            @ptrCast(conn),
        );
        defer parser.deinit();

        parser.executeCopy(copy_query) catch |err| {
            log.err("COPY CSV command failed: {}", .{err});
            _ = c.PQexec(conn, "ROLLBACK");
            return error.CopyFailed;
        };

        // Collect rows into array using arena allocator
        // chunk_alloc is the arena: every row and field was allocated from it, and the
        // arena.reset(.retain_capacity) at the top of this loop reclaims all of it at once.
        // Calling row.deinit() here would walk every field of every row (up to chunk_size
        // rows) issuing frees the arena discards — pure work for no reclaimed bytes.
        // CsvRow.deinit() stays meaningful for non-arena owners; see pg_copy_csv tests.
        var rows_list: std.ArrayList(pg_copy_csv.CsvRow) = .empty;

        var row_iterator = parser.rows();
        while (try row_iterator.next()) |row| {
            try rows_list.append(chunk_alloc, row);
        }

        const num_rows = rows_list.items.len;
        if (num_rows == 0) break;

        total_rows += num_rows;

        // Get column names from parser header
        const col_names = parser.columnNames() orelse return error.NoHeader;

        // Encode chunk as MessagePack with metadata wrapper using arena allocator
        const encoded = try encodeCsvRows(
            chunk_alloc,
            rows_list.items,
            col_names,
            table_name,
            snapshot_id,
            lsn_str,
            batch,
            format,
        );

        const payload = encoded;

        // Publish chunk to JetStream with Nats-Msg-Id header for deduplication
        const subject = try std.fmt.allocPrint(
            allocator,
            config.Snapshot.data_subject_pattern,
            .{ table_name, snapshot_id, batch },
        );
        defer allocator.free(subject);

        // Message ID for deduplication
        const msg_id_buf = try std.fmt.allocPrint(
            allocator,
            config.Snapshot.data_msg_id_pattern,
            .{ table_name, snapshot_id, batch },
        );
        defer allocator.free(msg_id_buf);

        // Create headers with metadata for versioning and deduplication
        var headers = nats.pool.Headers{};
        try headers.init(allocator, 256);
        defer headers.deinit();

        // Deduplication
        try headers.append("Nats-Msg-Id", msg_id_buf);

        // Content metadata
        try headers.append("Content-Type", "application/msgpack");

        // Publish to JetStream with headers and retry logic
        try publishWithRetry(js, subject, &headers, payload, should_stop);

        log.info("📦 Published chunk {d} ({d} rows, {d} bytes) → {s} (msg_id={s})", .{
            batch,
            num_rows,
            payload.len,
            subject,
            msg_id_buf,
        });

        batch += 1;

        // Update last_val from the last row's PK column
        // Find PK column index in the CSV header
        if (num_rows > 0) {
            const header_cols = parser.columnNames() orelse return error.NoHeader;
            var pk_idx: ?usize = null;
            for (header_cols, 0..) |name, i| {
                if (std.mem.eql(u8, name, pk.name)) {
                    pk_idx = i;
                    break;
                }
            }

            if (pk_idx) |idx| {
                const last_row = rows_list.items[num_rows - 1];
                if (idx < last_row.fields.len) {
                    if (last_row.fields[idx].value) |pk_val| {
                        // Update high-water mark (dupe into stable allocator)
                        allocator.free(last_val);
                        last_val = try allocator.dupe(u8, pk_val);
                    }
                }
            }
        }

        // If we got fewer rows than chunk_size, we're done
        if (num_rows < chunk_size) {
            break;
        }
    }

    // Commit transaction to release snapshot isolation
    const commit_result = c.PQexec(conn, "COMMIT");
    defer c.PQclear(commit_result);

    if (c.PQresultStatus(commit_result) != c.PGRES_COMMAND_OK) {
        log.err("COMMIT failed: {s}", .{c.PQerrorMessage(conn)});
        return error.TransactionFailed;
    }

    log.info("✅ Snapshot transaction committed", .{});

    // Publish metadata: init.users.meta
    try publishSnapshotMetadata(
        allocator,
        js,
        table_name,
        snapshot_id,
        lsn_str,
        batch,
        total_rows,
        format,
        should_stop,
    );

    log.info("✅ Snapshot complete: {s} ({d} batches, {d} rows)", .{
        snapshot_id,
        batch,
        total_rows,
    });
}

/// Parse PostgreSQL LSN format (e.g., "0/17FBE78") to u64
/// PostgreSQL LSN format: "segment/offset" where both are hex numbers
fn parsePgLsn(lsn_str: []const u8) !u64 {
    // Find the '/' separator
    const slash_pos = std.mem.indexOfScalar(u8, lsn_str, '/') orelse return error.InvalidLsnFormat;

    const segment_str = lsn_str[0..slash_pos];
    const offset_str = lsn_str[slash_pos + 1 ..];

    // Parse both parts as hex
    const segment = try std.fmt.parseInt(u32, segment_str, 16);
    const offset = try std.fmt.parseInt(u32, offset_str, 16);

    // Combine: segment is upper 32 bits, offset is lower 32 bits
    return (@as(u64, segment) << 32) | @as(u64, offset);
}

/// Encode CSV rows to MessagePack with metadata wrapper
/// Wraps snapshot data with table name, operation type, LSN, and chunk info
fn encodeCsvRows(
    allocator: std.mem.Allocator,
    rows: []const pg_copy_csv.CsvRow,
    col_names: [][]const u8,
    table_name: []const u8,
    snapshot_id: []const u8,
    lsn: []const u8,
    chunk: u32,
    format: encoder_mod.Format,
) ![]const u8 {
    // Use unified encoder (always MessagePack for snapshots)
    var encoder = encoder_mod.Encoder.init(
        allocator,
        format, // changed to use passed format
    );
    defer encoder.deinit();

    // Build data array (array of row maps)
    var data_array = try encoder.createArray(rows.len);

    for (rows, 0..) |row, row_idx| {
        var row_map = encoder.createMap();

        for (row.fields, 0..) |csv_field, col_idx| {
            if (col_idx >= col_names.len) continue;

            const col_name = col_names[col_idx];

            if (csv_field.isNull()) {
                try row_map.put(encoder.allocator, col_name, encoder.createNull());
            } else if (csv_field.value) |text_val| {
                // CSV values are already text, just encode them
                try row_map.put(encoder.allocator, col_name, try encoder.createString(text_val));
            }
        }

        try data_array.setIndex(row_idx, row_map);
    }

    // Build metadata wrapper map
    var wrapper_map = encoder.createMap();
    defer wrapper_map.free(allocator);

    // Parse PostgreSQL LSN string to u64 integer (same format as CDC events)
    const lsn_int = try parsePgLsn(lsn);

    try wrapper_map.put(encoder.allocator, "table", try encoder.createString(table_name));
    try wrapper_map.put(encoder.allocator, "operation", try encoder.createString("snapshot"));
    try wrapper_map.put(encoder.allocator, "snapshot_id", try encoder.createString(snapshot_id));
    try wrapper_map.put(encoder.allocator, "chunk", encoder.createInt(@intCast(chunk)));
    try wrapper_map.put(encoder.allocator, "lsn", encoder.createInt(@intCast(lsn_int)));
    try wrapper_map.put(encoder.allocator, "data", data_array);

    return try encoder.encode(wrapper_map);
}

/// Publish snapshot metadata to NATS
fn publishSnapshotMetadata(
    allocator: std.mem.Allocator,
    js: *nats.JS,
    table_name: []const u8,
    snapshot_id: []const u8,
    lsn: []const u8,
    batch_count: u32,
    row_count: u64,
    format: encoder_mod.Format,
    should_stop: *std.atomic.Value(bool),
) !void {
    var encoder = encoder_mod.Encoder.init(allocator, format);
    defer encoder.deinit();

    var meta_map = encoder.createMap();
    defer meta_map.free(allocator);

    // Parse PostgreSQL LSN string to u64 integer
    const lsn_int = try parsePgLsn(lsn);

    try meta_map.put(encoder.allocator, "snapshot_id", try encoder.createString(snapshot_id));
    try meta_map.put(encoder.allocator, "lsn", encoder.createInt(@intCast(lsn_int)));
    try meta_map.put(encoder.allocator, "timestamp", encoder.createInt(@as(i64, c.time(null))));
    try meta_map.put(encoder.allocator, "batch_count", encoder.createInt(@intCast(batch_count)));
    try meta_map.put(encoder.allocator, "row_count", encoder.createInt(@intCast(row_count)));
    try meta_map.put(encoder.allocator, "table", try encoder.createString(table_name));

    const encoded = try encoder.encode(meta_map);
    defer allocator.free(encoded);

    const subject = try std.fmt.allocPrint(
        allocator,
        config.Snapshot.meta_subject_pattern,
        .{table_name},
    );
    defer allocator.free(subject);

    // Publish to JetStream with retry logic
    try publishWithRetry(js, subject, null, encoded, should_stop);

    log.info("📋 Published snapshot metadata → {s}", .{subject});
}

/// Publish snapshot start notification to NATS
/// This notifies consumers that a snapshot is starting with the LSN watermark
/// Consumers should filter CDC events with LSN < snapshot_lsn to avoid duplicates
fn publishSnapshotStart(
    allocator: std.mem.Allocator,
    js: *nats.JS,
    table_name: []const u8,
    snapshot_id: []const u8,
    lsn: []const u8,
    format: encoder_mod.Format,
    should_stop: *std.atomic.Value(bool),
) !void {
    var encoder = encoder_mod.Encoder.init(allocator, format);
    defer encoder.deinit();

    var start_map = encoder.createMap();
    defer start_map.free(allocator);

    // Parse PostgreSQL LSN string to u64 integer
    const lsn_int = try parsePgLsn(lsn);

    try start_map.put(encoder.allocator, "snapshot_id", try encoder.createString(snapshot_id));
    try start_map.put(encoder.allocator, "table", try encoder.createString(table_name));
    try start_map.put(encoder.allocator, "lsn", encoder.createInt(@intCast(lsn_int)));
    try start_map.put(encoder.allocator, "timestamp", encoder.createInt(@as(i64, c.time(null))));
    try start_map.put(encoder.allocator, "status", try encoder.createString("starting"));
    try start_map.put(encoder.allocator, "format", try encoder.createString(@tagName(format)));

    const encoded = try encoder.encode(start_map);
    defer allocator.free(encoded);

    const subject = try std.fmt.allocPrint(
        allocator,
        config.Snapshot.start_subject_pattern,
        .{table_name},
    );
    defer allocator.free(subject);

    // Create headers for start message
    var headers = nats.pool.Headers{};
    try headers.init(allocator, 256);
    defer headers.deinit();

    try headers.append("Content-Type", "application/msgpack");
    try headers.append("X-Snapshot-Version", "1.0");
    try headers.append("X-Message-Type", "snapshot-start");

    // Publish to JetStream with retry logic
    try publishWithRetry(js, subject, &headers, encoded, should_stop);

    log.info("🚀 Published snapshot start → {s} (LSN watermark: {s})", .{ subject, lsn });
}

/// Publish schema (column names) to NATS so consumer knows the array field order
/// Subject: snapshot.schema.{table_name}.{snapshot_id}
fn publishSchemaZig(
    allocator: std.mem.Allocator,
    js: *nats.JS,
    table_name: []const u8,
    snapshot_id: []const u8,
    column_names: [][]const u8,
    format: encoder_mod.Format,
    should_stop: *std.atomic.Value(bool),
) !void {
    var encoder = encoder_mod.Encoder.init(allocator, format);
    defer encoder.deinit();

    var schema_map = encoder.createMap();
    defer schema_map.free(allocator);

    // Build schema array
    var schema_array = try encoder.createArray(column_names.len);
    for (column_names, 0..) |col_name, idx| {
        try schema_array.setIndex(idx, try encoder.createString(col_name));
    }

    try schema_map.put(encoder.allocator, "table", try encoder.createString(table_name));
    try schema_map.put(encoder.allocator, "snapshot_id", try encoder.createString(snapshot_id));
    try schema_map.put(encoder.allocator, "schema", schema_array);
    try schema_map.put(encoder.allocator, "timestamp", encoder.createInt(@as(i64, c.time(null))));

    const encoded = try encoder.encode(schema_map);
    defer allocator.free(encoded);

    const subject = try std.fmt.allocPrint(
        allocator,
        "snapshot.schema.{s}.{s}",
        .{ table_name, snapshot_id },
    );
    defer allocator.free(subject);

    // Create headers for schema message
    var headers = nats.pool.Headers{};
    try headers.init(allocator, 256);
    defer headers.deinit();

    try headers.append("Content-Type", "application/msgpack");
    try headers.append("X-Schema-Version", "1.0");
    try headers.append("X-Column-Count", try std.fmt.allocPrint(allocator, "{d}", .{column_names.len}));

    // Publish to JetStream with retry logic
    try publishWithRetry(js, subject, &headers, encoded, should_stop);

    log.info("📋 Published schema → {s} ({d} columns)", .{ subject, column_names.len });
}

/// Generate snapshot ID based on current timestamp with random entropy
/// Format: snap-{timestamp}-{random_u16}
/// Prevents collisions when multiple snapshots are requested in the same second
fn generateSnapshotId(allocator: std.mem.Allocator) ![]const u8 {
    var ts: std.os.linux.timespec = undefined;
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    const micro_seed = @as(u64, @intCast(ts.sec)) * 1_000_000 + @as(u64, @intCast(ts.nsec)) / 1_000;
    var prng = std.Random.DefaultPrng.init(micro_seed);
    const random_suffix = prng.random().int(u16);

    return try std.fmt.allocPrint(
        allocator,
        "snap-{d}-{x:0>4}",
        .{ @as(i64, c.time(null)), random_suffix },
    );
}
