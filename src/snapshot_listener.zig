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
const refused_tables = @import("refused_tables.zig");
const pg_copy_binary = @import("pg_copy_binary.zig");
const publication_mod = @import("publication.zig");
const config = @import("config.zig");
const msgpack = @import("msgpack");
const encoder_mod = @import("encoder.zig");
const streaming_encoder = @import("streaming_encoder.zig");
const RuntimeConfig = @import("config.zig").RuntimeConfig;
const nats = @import("nats");
const topology_mod = @import("topology.zig");
const utils = @import("utils.zig");

pub const log = std.log.scoped(.snapshot_listener);



/// What a completed snapshot reports back, so the cache can answer later requests
/// without re-running the COPY. `lsn_text` is owned by the caller's allocator: inside
/// the generator it points into a PGresult that is cleared on return.
const SnapshotResult = struct {
    lsn_text: []const u8,
    batch_count: u32,
    row_count: u64,
};

/// Publish a snapshot error.
///
/// Uses the ingress consumer's own connection — one connection consumes requests and
/// answers them. `init.snap.error.<table>` is a
/// subject the INIT stream captures, so a core publish is stored exactly like a
/// JetStream one — the only thing given up is the publish ack, which is acceptable for
/// an error notification.
///
/// This exists because the alternative was `continue`: a client that published a
/// request and got neither data nor error waits forever. Silence is the worst possible
/// answer to a request.
fn publishSnapshotError(
    allocator: std.mem.Allocator,
    consumer: *nats.Consumer,
    table_name: []const u8,
    error_type: []const u8,
    error_message: []const u8,
    available_tables: []const []const u8,
    format: encoder_mod.Format,
    topo: *const topology_mod.Topology,
) !void {
    const subject = try topology_mod.render(
        allocator,
        topo.snapshot_error_pattern,
        &.{.{ .name = "table", .value = table_name }},
        null,
    );
    defer allocator.free(subject);

    var encoder = encoder_mod.Encoder.init(allocator, format);
    defer encoder.deinit();

    var map = encoder.createMap();
    defer map.free(allocator);

    try map.put(encoder.allocator, "table", try encoder.createString(table_name));
    try map.put(encoder.allocator, "status", try encoder.createString("failed"));
    try map.put(encoder.allocator, "error_type", try encoder.createString(error_type));
    try map.put(encoder.allocator, "error_message", try encoder.createString(error_message));
    try map.put(encoder.allocator, "timestamp", encoder.createInt(@as(i64, c.time(null))));

    // Tell the client what it *could* have asked for — the usual cause is a typo or a
    // table outside the publication.
    var tables_array = try encoder.createArray(available_tables.len);
    for (available_tables, 0..) |t, i| {
        try tables_array.setIndex(i, try encoder.createString(t));
    }
    try map.put(encoder.allocator, "available_tables", tables_array);

    const payload = try encoder.encode(map);
    defer allocator.free(payload);

    try consumer.PUBLISH(subject, null, payload);
}

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
    const max_retries = config.Retry.publish_max_retries;
    var backoff_ms: u64 = config.Retry.publish_backoff_ms;

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
            const sleep_increment_ms = config.Retry.shutdown_poll_ms;
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
            backoff_ms = @min(backoff_ms * 2, config.Retry.publish_max_backoff_ms);
            retry_count += 1;
        }
    }

    // If we exit the loop due to should_stop, return error
    log.info("🛑 Shutdown requested - aborting publish", .{});
    return error.ShutdownRequested;
}

/// Column names and type OIDs in `SELECT *` order, for decoding binary COPY.
///
/// Binary COPY's header carries no names, types, or column count, so the layout has to
/// arrive out-of-band. It must come from the **same connection inside the snapshot's
/// REPEATABLE READ transaction**: a lookup taken elsewhere could observe a different
/// schema than the COPY emits, and a wrong OID makes `decodeBinColumnData` produce
/// plausible garbage rather than an error.
///
/// Two sources were rejected. `information_schema` stores type *names* (`"timestamp
/// with time zone"`), which would need a hand-maintained name→OID table. And
/// `pgoutput.RelationMessage` carries real OIDs but only after the first change to the
/// table since the bridge connected — a snapshot can be requested before that.
///
/// Caller owns the returned slice and each name.
fn getTableColumns(
    allocator: std.mem.Allocator,
    conn: *c.PGconn,
    table_name: []const u8,
) ![]pg_copy_binary.ColumnMeta {
    var schema: []const u8 = "public";
    var table: []const u8 = table_name;
    if (std.mem.indexOf(u8, table_name, ".")) |dot_idx| {
        schema = table_name[0..dot_idx];
        table = table_name[dot_idx + 1 ..];
    }

    // `format_type` renders the declared type — `numeric(20,8)` — so Postgres owns the
    // `atttypmod` decoding, whose bit layout changed in PG 15 to allow negative scales.
    // Still one query and one round trip: a plain catalog function on the row we are
    // already reading, not an information_schema view (which joins several catalogs and
    // applies privilege filtering to answer the same question).
    //
    // One query, not two: the primary key comes back with the layout, from the same
    // rows, so the key and the column order cannot describe different states of the
    // table. A separate PK lookup used to exist and could disagree with this one.
    const query = try utils.allocPrintZ(
        allocator,
        \\SELECT a.attname,
        \\       a.atttypid,
        \\       format_type(a.atttypid, a.atttypmod),
        \\       COALESCE(k.ord, 0),
        \\       t.typtype
        \\FROM pg_attribute a
        \\JOIN pg_type t ON t.oid = a.atttypid
        \\LEFT JOIN pg_index i
        \\  ON i.indrelid = a.attrelid AND i.indisprimary
        \\LEFT JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
        \\  ON k.attnum = a.attnum
        \\WHERE a.attrelid = '"{s}"."{s}"'::regclass
        \\  AND a.attnum > 0 AND NOT a.attisdropped
        \\ORDER BY a.attnum;
    ,
        .{ schema, table },
    );
    defer allocator.free(query);

    const res = c.PQexec(conn, query.ptr);
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        log.err("📸 column lookup failed for '{s}': {s}", .{ table_name, c.PQerrorMessage(conn) });
        return error.ColumnLookupFailed;
    }

    const n: usize = @intCast(c.PQntuples(res));
    if (n == 0) return error.NoColumns;

    const cols = try allocator.alloc(pg_copy_binary.ColumnMeta, n);
    // Free the strings already duped if a later one fails, so an error frees everything.
    var filled: usize = 0;
    errdefer {
        for (cols[0..filled]) |col| {
            allocator.free(col.name);
            allocator.free(col.type_text);
        }
        allocator.free(cols);
    }

    for (cols, 0..) |*col, i| {
        const idx: c_int = @intCast(i);
        const name = std.mem.span(c.PQgetvalue(res, idx, 0));
        const oid_text = std.mem.span(c.PQgetvalue(res, idx, 1));
        const oid = std.fmt.parseInt(u32, oid_text, 10) catch return error.BadTypeOid;

        // Only NUMERIC gets padded — appending a decimal point to every integer in the
        // snapshot would be a far louder bug than the one being fixed. The type text
        // itself is kept for every column: keyset cursor literals are cast to it, and
        // which columns are the key is not known here.
        const type_text = std.mem.span(c.PQgetvalue(res, idx, 2));
        const scale: ?u16 = if (oid == pg_copy_binary.numeric_oid)
            pg_copy_binary.scaleFromFormatType(type_text)
        else
            null;

        const pk_ord_text = std.mem.span(c.PQgetvalue(res, idx, 3));
        const pk_ord = std.fmt.parseInt(u16, pk_ord_text, 10) catch 0;

        const typtype_text = std.mem.span(c.PQgetvalue(res, idx, 4));

        const name_owned = try allocator.dupe(u8, name);
        errdefer allocator.free(name_owned);

        col.* = .{
            .name = name_owned,
            .oid = oid,
            .numeric_scale = scale,
            .pk_ord = pk_ord,
            .typtype = if (typtype_text.len > 0) typtype_text[0] else 'b',
            .type_text = try allocator.dupe(u8, type_text),
        };
        filled += 1;
    }
    return cols;
}

/// Free what `getTableColumns` returned.
fn freeTableColumns(allocator: std.mem.Allocator, cols: []pg_copy_binary.ColumnMeta) void {
    for (cols) |col| {
        allocator.free(col.name);
        allocator.free(col.type_text);
    }
    allocator.free(cols);
}

/// Indices into a column list, in primary-key order — what pagination pages on.
///
/// Resolved from `pk_ord` rather than a second catalog query, so the key and the layout
/// can never disagree: they are the same rows, read inside the same transaction.
const PrimaryKey = struct {
    /// Indices into the caller's `columns`, `[0]` being the first key column.
    idx: []usize,

    fn deinit(self: PrimaryKey, allocator: std.mem.Allocator) void {
        allocator.free(self.idx);
    }
};

/// Collect the primary-key columns in key order.
///
/// Returns `error.NoPrimaryKey` when there is none — snapshots have nothing to paginate
/// on — and `error.MalformedPrimaryKey` if the ordinals are not exactly `1..n`, which
/// would leave a hole in the cursor.
fn resolvePrimaryKey(
    allocator: std.mem.Allocator,
    columns: []const pg_copy_binary.ColumnMeta,
) !PrimaryKey {
    var count: usize = 0;
    for (columns) |col| {
        if (col.pk_ord > 0) count += 1;
    }
    if (count == 0) return error.NoPrimaryKey;

    const idx = try allocator.alloc(usize, count);
    errdefer allocator.free(idx);

    // Sentinel rather than a parallel "seen" array: every slot must be written exactly
    // once, and an unwritten slot is what a duplicate or out-of-range ordinal leaves.
    @memset(idx, std.math.maxInt(usize));
    for (columns, 0..) |col, i| {
        if (col.pk_ord == 0) continue;
        if (col.pk_ord > count) return error.MalformedPrimaryKey;
        const slot = &idx[col.pk_ord - 1];
        if (slot.* != std.math.maxInt(usize)) return error.MalformedPrimaryKey;
        slot.* = i;
    }
    for (idx) |i| {
        if (i == std.math.maxInt(usize)) return error.MalformedPrimaryKey;
    }

    return .{ .idx = idx };
}

/// The keyset cursor carried between chunks: the previous chunk's last row, one value
/// per primary-key column.
///
/// Owned outside the chunk arena on purpose — the arena is reset before the next
/// chunk's query is built, which is exactly when the cursor is read.
const Cursor = struct {
    allocator: std.mem.Allocator,
    values: [][]const u8 = &.{},

    /// Take a copy of `values`, replacing whatever was held. The old copy is freed only
    /// once the new one is complete, so a failure part-way leaves the cursor intact.
    fn set(self: *Cursor, values: []const []const u8) !void {
        const copy = try self.allocator.alloc([]const u8, values.len);
        var filled: usize = 0;
        errdefer {
            for (copy[0..filled]) |v| self.allocator.free(v);
            self.allocator.free(copy);
        }
        for (values, copy) |src, *dst| {
            dst.* = try self.allocator.dupe(u8, src);
            filled += 1;
        }

        self.deinit();
        self.values = copy;
    }

    fn deinit(self: *Cursor) void {
        for (self.values) |v| self.allocator.free(v);
        self.allocator.free(self.values);
        self.values = &.{};
    }
};


/// Bounds a listener thread's **first** NATS connection, and only that one.
///
/// These threads cannot return an error — `std.Thread.spawn` on a `void` function
/// swallows one — so before this existed, a listener that could not reach NATS logged an
/// error every 2s and looped forever. Nothing else noticed: the CDC publisher has its own
/// connection and had already succeeded, so `/health` stayed green and events kept
/// flowing while no client could bootstrap. The bridge was *silently half-working*, which
/// is the state worth refusing.
///
/// The asymmetry is the point. A drop after the listener has worked once is an outage —
/// retry it forever. A first connection that never lands is a configuration fault, and
/// the only useful response is to stop and say so loudly.
///
/// Stopping means setting the shared `should_stop`, the same flag Ctrl+C sets, so the
/// bridge unwinds through its normal shutdown (flush thread drained, session summary
/// printed) rather than dying inside a worker thread.
const BootConnect = struct {
    /// Log prefix identifying which listener this is.
    who: []const u8,
    /// Named in the fatal message, because "the stream nats-init should have created" is
    /// the most common cause and its name is now a per-deployment value.
    requests_stream: []const u8,
    should_stop: *std.atomic.Value(bool),
    /// Raised alongside `should_stop` so `main` can exit non-zero. A supervisor reads
    /// the exit code, not the log: stopping with 0 tells `restart: on-failure` the job
    /// finished, and a misconfigured bridge then stays down and quiet.
    fatal: *std.atomic.Value(bool),
    attempts: u32 = 0,
    connected_once: bool = false,

    /// What a failure means, once counted.
    const Verdict = enum {
        /// Already worked once: this is an outage, retry without limit and without
        /// touching the budget.
        outage,
        /// Still inside the boot budget.
        retry,
        /// Budget exhausted; `should_stop` has been set.
        give_up,
    };

    /// Advance the state for one failure. Split from `failed` so the rule — and the
    /// stop flag it sets — can be tested without a broker and without asserting on log
    /// output, the same split `preflight.classifyVersionColumn` uses.
    fn record(self: *BootConnect) Verdict {
        if (self.connected_once) return .outage;

        self.attempts += 1;
        if (self.attempts < config.Retry.listener_boot_connect_attempts) return .retry;

        // Fatal first: `main` reads it once it observes should_stop, so the two stores
        // must not be visible in the other order.
        self.fatal.store(true, .release);
        self.should_stop.store(true, .release);
        return .give_up;
    }

    /// Record a failure and report it. Returns true when the caller must give up and
    /// return from the thread.
    fn failed(self: *BootConnect, err: anyerror, host: []const u8, port: u16) bool {
        switch (self.record()) {
            .outage => return false,
            .retry => {
                log.warn("{s}: NATS unavailable at {s}:{d} ({s}) — attempt {d}/{d}", .{
                    self.who,
                    host,
                    port,
                    @errorName(err),
                    self.attempts,
                    config.Retry.listener_boot_connect_attempts,
                });
                return false;
            },
            .give_up => {
                log.err(
                    "🔴 FATAL: {s} never reached NATS at {s}:{d} after {d} attempts (last: {s}). This is a configuration fault, not an outage — check NATS_URL/NATS_HOST, the nkey seed, and that nats-init created the '{s}' stream. Stopping the bridge rather than running without a snapshot path.",
                    .{
                        self.who,
                        host,
                        port,
                        self.attempts,
                        @errorName(err),
                        self.requests_stream,
                    },
                );
                return true;
            },
        }
    }

    /// The connection is up; every later failure is an outage, retried without limit.
    fn succeeded(self: *BootConnect) void {
        self.connected_once = true;
        self.attempts = 0;
    }
};

/// Snapshot request context passed to NATS callback
const SnapshotContext = struct {
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    js: *nats.JS, // JetStream connection for publishing
    monitored_tables: []const []const u8,
    refused: *const refused_tables.Registry,
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
    refused: *const refused_tables.Registry,
    thread: ?std.Thread = null,
    format: encoder_mod.Format,
    chunk_size: usize,
    io: std.Io,
    /// Where NATS is, resolved once in `bridge.zig`. Not re-derived here: see
    /// `Config.Nats.Endpoint`.
    endpoint: config.Nats.Endpoint,
    /// Set when a listener gives up on its first connection; see `BootConnect`.
    boot_fatal: *std.atomic.Value(bool),
    /// Wire names, read from topology.json at startup. See src/topology.zig.
    topology: *const topology_mod.Topology,

    /// Initialize snapshot listener (does not start the thread)
    pub fn init(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        should_stop: *std.atomic.Value(bool),
        monitored_tables: []const []const u8,
        refused: *const refused_tables.Registry,
        format: encoder_mod.Format,
        runtime_config: *const config.RuntimeConfig,
        io: std.Io,
        endpoint: config.Nats.Endpoint,
        boot_fatal: *std.atomic.Value(bool),
    ) SnapshotListener {
        return .{
            .allocator = allocator,
            .pg_config = pg_config,
            .should_stop = should_stop,
            .monitored_tables = monitored_tables,
            .refused = refused,
            .thread = null,
            .format = format,
            .io = io,
            .endpoint = endpoint,
            .boot_fatal = boot_fatal,
            .topology = &runtime_config.topology,
            .chunk_size = runtime_config.snapshot_chunk_size,
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
        // Two independent subscribers, each with its own NATS connection: a schema
        // request must not queue behind a snapshot's COPY, which can run for minutes.
        log.info("📋 Spawning schema request listener...", .{});
        const schema_thread = try std.Thread.spawn(.{}, listenForSchemaRequests, .{
            self.allocator,
            self.pg_config,
            self.should_stop,
            self.monitored_tables,
            self.refused,
            self.format,
            self.io,
            self.endpoint,
            self.boot_fatal,
            self.topology,
        });

        log.info("📸 Spawning snapshot request listener...", .{});
        const snapshot_thread = try std.Thread.spawn(.{}, listenForSnapshotRequests, .{
            self.allocator,
            self.pg_config,
            self.should_stop,
            self.monitored_tables,
            self.refused,
            self.format,
            self.chunk_size,
            self.io,
            self.endpoint,
            self.boot_fatal,
            self.topology,
        });

        // Keep main thread alive - just sleep until stop signal
        while (!self.should_stop.load(.seq_cst)) {
            utils.sleep(100 * std.time.ns_per_ms);
        }

        // Wait for threads to finish
        schema_thread.join();
        snapshot_thread.join();
    }

    // Listens for schema requests on topo.schema_request and responds
    // with fresh schema data queried from PostgreSQL information_schema.

    /// Column information from information_schema
    const SchemaColumnInfo = struct {
        name: []const u8,
        position: u32,
        data_type: []const u8,
        is_nullable: bool,
        column_default: ?[]const u8,
    };

    /// Schema request handler — subscribes to the topology's schema request subject and
    /// responds with table schemas
    /// Note: Thread functions cannot return errors - all errors must be caught internally
    fn listenForSchemaRequests(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        should_stop: *std.atomic.Value(bool),
        monitored_tables: []const []const u8,
        refused: *const refused_tables.Registry,
        format: encoder_mod.Format,
        io: std.Io,
        endpoint: config.Nats.Endpoint,
        boot_fatal: *std.atomic.Value(bool),
        topo: *const topology_mod.Topology,
    ) void {
        const reconnect_delay_ms = config.Retry.nats_reconnect_delay_ms;

        // See `Retry.listener_boot_connect_attempts`: the first connection is bounded
        // because failing it means misconfiguration, every later one is not.
        var boot = BootConnect{ .who = "📋 Schema listener", .should_stop = should_stop, .fatal = boot_fatal, .requests_stream = topo.stream_requests };

        // Outer reconnection loop
        while (!should_stop.load(.acquire)) {
            log.info("📋 Schema listener: Connecting to NATS...", .{});

            // Create Core NATS connection
            var core = nats.Core{};
            const connect_opts = nats.protocol.ConnectOpts{
                .addr = endpoint.host,
                .port = endpoint.port,
                .user = endpoint.user,
                .pass = endpoint.pass,
                .nkey_seed = endpoint.seed,
            };
            core.CONNECT(allocator, connect_opts, io) catch |err| {
                if (boot.failed(err, endpoint.host, endpoint.port)) return;
                utils.sleep(reconnect_delay_ms * std.time.ns_per_ms);
                continue;
            };
            defer core.DISCONNECT();

            log.info("📋 Schema listener: Connected! Subscribing to 'init.schema'...", .{});

            // Subscribe to init.schema
            const sid = "schema-listener-1";
            core.SUB(topo.schema_request, null, sid) catch |err| {
                // Counted against the boot budget too: a subscription that never
                // succeeds leaves the listener as useless as one that never connects,
                // and the connect above will keep succeeding, so nothing else would
                // ever stop the loop.
                if (boot.failed(err, endpoint.host, endpoint.port)) return;
                utils.sleep(reconnect_delay_ms * std.time.ns_per_ms);
                continue;
            };
            boot.succeeded();

            log.info("📋 Schema listener: ✅ Subscribed to '{s}'! Waiting for schema requests...", .{topo.schema_request});

            // Listen for schema requests
            while (!should_stop.load(.acquire)) {
                if (core.connection) |conn| {
                    const msg = conn.waitMessageNMT(nats.protocol.SECNS / 2, null) catch |err| {
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
                        refused,
                        format,
                        topo,
                    ) catch |err| {
                        log.err("📋 Failed to handle schema request: {}", .{err});
                    };
                } else {
                    log.err("📋 Schema listener: Connection lost - reconnecting", .{});
                    break; // Break inner loop to trigger reconnection
                }
            }
        }

        log.info("📋 Schema listener stopped", .{});
    }

    /// Handle a single schema request by querying PostgreSQL and publishing response
    fn handleSchemaRequest(
        allocator: std.mem.Allocator,
        core: *nats.Core,
        pg_config: *const pg_conn.PgConf,
        monitored_tables: []const []const u8,
        refused: *const refused_tables.Registry,
        format: encoder_mod.Format,
        topo: *const topology_mod.Topology,
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
        var listed: usize = 0;
        for (monitored_tables) |table| {
            // Extract table name if it's "schema.table" format
            const table_name = blk: {
                if (std.mem.indexOf(u8, table, ".")) |idx| {
                    break :blk table[idx + 1 ..];
                }
                break :blk table;
            };

            // Withhold refused tables here too. This is the third schema publisher
            // (boot pass, DDL path, and this on-demand responder); a guard on two of
            // three still leaks a live schema for a table that will never send rows.
            if (refused.isRefused(table_name)) {
                log.warn("📋 Withholding schema for refused table '{s}' (no primary key)", .{table_name});
                continue;
            }

            // Counted separately from the loop index: skipping a table must not leave a
            // dangling comma in the IN clause.
            if (listed > 0) try in_clause.appendSlice(allocator, ", ");
            listed += 1;

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
            \\ORDER BY t.table_name, c.ordinal_position;
        ,
            .{in_clause.items},
        );
        // !! TODO handle empty tables. `if (length(in_clause.items)==0) {}
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

            try publishSchemaResponse(allocator, core, table_name, columns, format, topo);
        }

        log.info("📋 Published schemas for {d} tables", .{table_schemas.count()});
    }

    /// Publish schema response to NATS
    fn publishSchemaResponse(
        allocator: std.mem.Allocator,
        core: *nats.Core,
        table_name: []const u8,
        columns: []const SchemaColumnInfo,
        format: encoder_mod.Format,
        topo: *const topology_mod.Topology,
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

        // Publish to $KV.schemas.{table_name} subject to populate the KV bucket!
        const subject = try topology_mod.render(
            allocator,
            topo.kv_schemas_subject_pattern,
            &.{.{ .name = "table", .value = table_only }},
            null,
        );
        defer allocator.free(subject);

        core.PUB(subject, null, encoded) catch |err| {
            log.err("📋 Failed to publish schema for {s}: {}", .{ table_only, err });
            return err;
        };

        // Print the subject actually published to. It read "schema.{table}" before —
        // a subject this code has not used since schemas moved into the KV bucket.
        log.info("📋 ✅ Published schema → {s} ({d} columns, {d} bytes)", .{
            subject,
            columns.len,
            encoded.len,
        });
    }

    /// Snapshot request handler - listens on "snapshot.request.>" and generates snapshots
    /// Note: Thread functions cannot return errors - all errors must be caught internally
    fn listenForSnapshotRequests(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        should_stop: *std.atomic.Value(bool),
        monitored_tables: []const []const u8,
        refused: *const refused_tables.Registry,
        format: encoder_mod.Format,
        chunk_size: usize,
        io: std.Io,
        endpoint: config.Nats.Endpoint,
        boot_fatal: *std.atomic.Value(bool),
        topo: *const topology_mod.Topology,
    ) void {
        const reconnect_delay_ms = config.Retry.nats_reconnect_delay_ms;
        var boot = BootConnect{ .who = "📸 Snapshot listener", .should_stop = should_stop, .fatal = boot_fatal, .requests_stream = topo.stream_requests };

        // Outer reconnection loop
        while (!should_stop.load(.acquire)) {
            log.info("📸 Snapshot listener: Connecting to NATS...", .{});

            // A JetStream consumer on REQUESTS, deliberately NOT a core subscription.
            //
            // The REQUESTS stream is configured `--max-msgs-per-subject=1 --discard=new
            // --max-age=SNAP_RET`, which is the stampede protection: one snapshot request
            // per table per window, enforced by the broker for every client. That only
            // works if the bridge reads the *stream*. A core subscriber is a parallel
            // listener — the stream's limits govern what it stores, never what a core
            // subscription receives — so with `core.SUB` a hundred reconnecting clients
            // still produced a hundred requests and a hundred COPYs, and the policy
            // protected nothing.
            //
            // It also replaces trust with enforcement: previously the only thing standing
            // between the primary and a stampede was each client politely checking the KV
            // descriptor first.
            var cscnf = nats.protocol.ConsumerConfig{
                .durable_name = "bridge_snapshot_worker",
                .ack_policy = "explicit",
                .deliver_policy = "all",
                .filter_subject = topo.request_subject_wildcard,
                // A snapshot runs synchronously in this loop and can take minutes on a
                // large table. The default 30s ack_wait would redeliver the request while
                // the first COPY is still running — duplicate snapshots for one request.
                .ack_wait = config.Snapshot.request_ack_wait_ns,
                // A request that keeps failing must not be retried forever.
                .max_deliver = config.Snapshot.request_max_deliver,
            };

            const connect_opts = nats.protocol.ConnectOpts{
                .addr = endpoint.host,
                .port = endpoint.port,
                .user = endpoint.user,
                .pass = endpoint.pass,
                .nkey_seed = endpoint.seed,
            };
            var consumer = nats.Consumer.START(
                allocator,
                connect_opts,
                topo.stream_requests,
                &cscnf,
                io,
            ) catch |err| {
                // Bounded on the first pass only. START covers both the connection and
                // the consumer, so the common permanent failure here is not a bad host
                // but a `REQUESTS` stream `nats-init` never created — which no amount
                // of retrying fixes, and which used to be invisible behind one log line
                // every 2s while the bridge otherwise looked healthy.
                if (boot.failed(err, endpoint.host, endpoint.port)) return;
                log.err("📸 Snapshot listener: Failed to start consumer on '{s}': {} - retrying in {d}ms", .{ topo.stream_requests, err, reconnect_delay_ms });
                utils.sleep(reconnect_delay_ms * std.time.ns_per_ms);
                continue;
            };
            defer nats.Consumer.STOP(&consumer, false);
            boot.succeeded();

            log.info("📸 Snapshot listener: ✅ Consuming '{s}' from stream '{s}'", .{
                topo.request_subject_wildcard,
                topo.stream_requests,
            });

            // Listen for snapshot requests
            while (!should_stop.load(.acquire)) {
                if (consumer.CONSUME(nats.protocol.SECNS / 2) catch null) |msg| {
                    // Not every message a pull consumer receives is a delivery. The
                    // server also sends status messages on the pull inbox — `408 Request
                    // Timeout`, `409`, idle heartbeats — whose subject is the inbox
                    // itself (`<uuid>.<seq>`) and which carry **no reply-to**.
                    //
                    // They must be recycled, never acked: `Consumer.ack` publishes to
                    // `msg.letter.ReplyTo().?`, so acking one panics the thread on a null
                    // unwrap — observed as
                    // `📸 Invalid snapshot request subject: 04b01ecd-….2196` followed
                    // immediately by `attempt to use null value`, killing the snapshot
                    // path for the life of the process.
                    //
                    // Reply-to is the right discriminator here, not an empty payload:
                    // a snapshot request legitimately has no payload — the table name is
                    // in the subject — so the emptiness test the mutation listener uses
                    // would discard every real request.
                    if (msg.letter.ReplyTo() == null) {
                        log.debug("📸 JetStream status message (no reply-to), recycling", .{});
                        consumer.REUSE(msg);
                        continue;
                    }

                    // ACKed as soon as it is understood, not after the COPY finishes.
                    // Holding the ack across a multi-minute snapshot only buys
                    // redelivery-on-crash, and pays for it with duplicate snapshots
                    // whenever generation outlives ack_wait. The stream's own
                    // max-msgs-per-subject window is what stops a re-request anyway.
                    defer consumer.ACK(msg, true) catch consumer.REUSE(msg);

                    // Extract table name from subject: init.snapshot.<table>
                    const subject = msg.letter.subject.body() orelse {
                        log.err("📸 No subject in message", .{});
                        continue;
                    };

                    const table_name = blk: {
                        const prefix = topo.request_subject_prefix;
                        if (std.mem.startsWith(u8, subject, prefix)) {
                            break :blk subject[prefix.len..];
                        }
                        log.err("📸 Invalid snapshot request subject: {s}", .{subject});
                        continue;
                    };

                    log.info("📸 Snapshot request received for table: {s}", .{table_name});

                    // A refused table is in the publication but has no schema in KV, so
                    // seeding from it would populate a client table built from a shape
                    // Postgres no longer has. Checked before monitoring, because
                    // "refused" is the more specific and more actionable answer.
                    if (refused.isRefused(table_name)) {
                        log.warn("📸 Table '{s}' is refused (no primary key) — publishing error", .{table_name});
                        publishSnapshotError(
                            allocator,
                            &consumer,
                            table_name,
                            "table_refused",
                            "Table has no primary key: replication is suspended, so no snapshot can be served",
                            monitored_tables,
                            format,
                            topo,
                        ) catch |err| {
                            log.err("📸 Failed to publish snapshot error for '{s}': {}", .{ table_name, err });
                        };
                        continue;
                    }

                    // Validate table is monitored
                    const is_monitored = publication_mod.isTableMonitored(table_name, monitored_tables);
                    if (!is_monitored) {
                        log.warn("📸 Table '{s}' not in monitored tables — publishing error", .{table_name});
                        publishSnapshotError(
                            allocator,
                            &consumer,
                            table_name,
                            "table_not_monitored",
                            "Table is not in the publication this bridge replicates",
                            monitored_tables,
                            format,
                            topo,
                        ) catch |err| {
                            log.err("📸 Failed to publish snapshot error for '{s}': {}", .{ table_name, err });
                        };
                        continue;
                    }

                    // Generate snapshot ID
                    const snapshot_id = generateSnapshotId(allocator) catch |err| {
                        log.err("Failed to generate snapshot ID: {}", .{err});
                        continue;
                    };
                    defer allocator.free(snapshot_id);

                    log.info("📸 Generating snapshot for '{s}' (id={s})", .{ table_name, snapshot_id });

                    const result = generateIncrementalSnapshot(
                        allocator,
                        pg_config,
                        table_name,
                        snapshot_id,
                        format,
                        chunk_size,
                        should_stop,
                        io,
                        endpoint,
                        topo,
                    ) catch |err| {
                        log.err("📸 Snapshot generation failed for '{s}': {}", .{ table_name, err });
                        publishSnapshotError(
                            allocator,
                            &consumer,
                            table_name,
                            "generation_failed",
                            @errorName(err),
                            monitored_tables,
                            format,
                            topo,
                        ) catch |perr| {
                            log.err("📸 Failed to publish generation error for '{s}': {}", .{ table_name, perr });
                        };
                        continue;
                    };
                    defer allocator.free(result.lsn_text);

                    log.info("📸 ✅ Snapshot completed for '{s}'", .{table_name});
                }
            }
        }

        log.info("📸 Snapshot listener stopped", .{});
    }

    /// Generate incremental snapshot
    fn generateIncrementalSnapshot(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        table_name: []const u8,
        snapshot_id: []const u8,
        format: encoder_mod.Format,
        chunk_size: usize,
        should_stop: *std.atomic.Value(bool),
        io: std.Io,
        endpoint: config.Nats.Endpoint,
        topo: *const topology_mod.Topology,
    ) !SnapshotResult {
        log.info("📸 Generating snapshot for '{s}' (id={s}, chunk_size={d})", .{
            table_name,
            snapshot_id,
            chunk_size,
        });

        // Create JetStream connection for publishing snapshot data
        const connect_opts = nats.protocol.ConnectOpts{
                .addr = endpoint.host,
                .port = endpoint.port,
                .user = endpoint.user,
                .pass = endpoint.pass,
                .nkey_seed = endpoint.seed,
            };
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
        publishSnapshotStart(
            allocator,
            &js,
            table_name,
            snapshot_id,
            lsn_str,
            format,
            should_stop,
            topo,
        ) catch |err| {
            log.warn("Failed to publish snapshot start: {}", .{err});
        };

        // Column layout *and* primary key in one round trip. Taken here — after BEGIN
        // ISOLATION LEVEL REPEATABLE READ, on this connection — so no ALTER TABLE can
        // land between the lookup and any chunk's COPY. One lookup serves every chunk,
        // since the schema cannot move inside the transaction, which is also why it
        // must never be cached across snapshots: a stale layout decodes values into the
        // wrong columns silently.
        const columns = try getTableColumns(allocator, conn.?, table_name);
        defer freeTableColumns(allocator, columns);

        // The keyset cursor columns, resolved from the same rows rather than a second
        // query. Binary COPY has no header to resolve against, so this is the only
        // source of truth for both layout and key.
        const pk = resolvePrimaryKey(allocator, columns) catch |err| {
            switch (err) {
                error.NoPrimaryKey => log.err(
                    "📸 '{s}' has no primary key — nothing to paginate a snapshot on",
                    .{table_name},
                ),
                error.MalformedPrimaryKey => log.err(
                    "📸 '{s}': the catalog's primary key ordinals are not 1..n — refusing to paginate on a partial key",
                    .{table_name},
                ),
                else => {},
            }
            _ = c.PQexec(conn, "ROLLBACK");
            return err;
        };
        defer pk.deinit(allocator);

        // Cursor literals are quoted by doubling `'` and leaving backslashes alone,
        // which is only correct while a backslash is an ordinary character. libpq
        // already knows the answer — the server reports this GUC at startup — so this
        // costs no round trip, and a wrong cursor would skip or repeat rows silently.
        const scs = c.PQparameterStatus(conn, "standard_conforming_strings");
        if (scs == null or !std.mem.eql(u8, std.mem.span(scs), "on")) {
            log.err(
                "📸 refusing to snapshot '{s}': standard_conforming_strings is off, so a backslash in a key value would be read as an escape and the pagination cursor could silently skip rows",
                .{table_name},
            );
            _ = c.PQexec(conn, "ROLLBACK");
            return error.UnsafeStringLiterals;
        }

        if (pk.idx.len > 1) {
            log.info("📸 '{s}': paginating on a {d}-column primary key", .{ table_name, pk.idx.len });
        }

        // Fixed for the whole snapshot: the key cannot move under REPEATABLE READ, and
        // every chunk must be ordered identically for the cursor to mean anything.
        const order_by = try pg_copy_binary.orderByClause(allocator, columns, pk.idx);
        defer allocator.free(order_by);

        // Create arena for snapshot processing
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        // Process chunks using streaming encoder
        var batch: u32 = 0;
        var total_rows: u64 = 0;
        var cursor = Cursor{ .allocator = allocator };
        defer cursor.deinit();

        // Pre-allocate encoding buffer (2MB for chunk data)
        const encode_buffer = try allocator.alloc(u8, 2 * 1024 * 1024);
        defer allocator.free(encode_buffer);

        while (true) {
            _ = arena.reset(.retain_capacity);
            const chunk_alloc = arena.allocator();

            // The first chunk has no cursor, so it takes the whole table rather than a
            // sentinel value: `"id" > 0` would silently drop a row with a zero or
            // negative key, and there is no sentinel at all for a text or uuid key.
            const where_clause: []const u8 = if (cursor.values.len == 0)
                "1=1"
            else
                try pg_copy_binary.keysetPredicate(chunk_alloc, columns, pk.idx, cursor.values);

            const copy_query = try utils.allocPrintZ(
                chunk_alloc,
                "COPY (SELECT * FROM \"{s}\" WHERE {s} ORDER BY {s} LIMIT {d}) TO STDOUT WITH (FORMAT binary)",
                .{ table_name, where_clause, order_by, chunk_size },
            );

            // Initialize streaming encoder with fixed buffer
            var encoder = streaming_encoder.StreamingEncoder.init(encode_buffer);

            // Stream: PostgreSQL binary COPY → framing → decoder → encoder.
            // `columns` comes from the catalog inside this same REPEATABLE READ
            // transaction, because the binary header carries no layout of its own.
            var parser = pg_copy_binary.Streamer.init(chunk_alloc, @ptrCast(conn), columns);
            defer parser.deinit();

            const num_rows = parser.streamToEncoder(copy_query, &encoder, pk.idx) catch |err| {
                log.err("📸 binary COPY chunk failed for '{s}': {}", .{ table_name, err });
                _ = c.PQexec(conn, "ROLLBACK");
                return error.StreamEncodingFailed;
            };

            // On first chunk (even if empty), publish schema so consumer knows column order
            if (batch == 0) {
                const col_names = try chunk_alloc.alloc([]const u8, columns.len);
                for (columns, col_names) |col, *name| name.* = col.name;
                try publishSchema(
                    chunk_alloc,
                    &js,
                    table_name,
                    snapshot_id,
                    col_names,
                    format,
                    should_stop,
                    topo,
                );
            }

            if (num_rows == 0) break;

            total_rows += num_rows;

            // Get the encoded MessagePack data (no allocation - slice of encode_buffer)
            const encoded = encoder.getWritten();

            const payload = encoded;

            // Publish chunk to JetStream with Nats-Msg-Id header for deduplication
            // Use chunk_alloc (arena) for all per-chunk allocations
            const chunk_text = try std.fmt.allocPrint(chunk_alloc, "{d}", .{batch});
            const subject = try topology_mod.render(chunk_alloc, topo.snapshot_data_pattern, &.{
                .{ .name = "table", .value = table_name },
                .{ .name = "snapshot_id", .value = snapshot_id },
                .{ .name = "chunk", .value = chunk_text },
            }, null);

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

            // Advance the keyset cursor to the actual last row of this chunk, captured
            // during streaming. Correct for gaps, deletions, and non-sequential keys
            // (uuid, text) alike, because it never assumes what the next key would be.
            const pk_values = parser.getLastPkValues() orelse {
                log.err("⚠️  No PK values captured from chunk — aborting snapshot", .{});
                _ = c.PQexec(conn, "ROLLBACK");
                return error.PkValueMissing;
            };
            // Copied out of the chunk arena, which the next iteration resets before it
            // builds the query that reads this.
            try cursor.set(pk_values);

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
        try publishSnapshotMetadata(
            allocator,
            &js,
            table_name,
            snapshot_id,
            lsn_str,
            batch,
            total_rows,
            format,
            should_stop,
            topo,
        );

        log.info("✅ Snapshot complete: {s} ({d} batches, {d} rows)", .{
            snapshot_id,
            batch,
            total_rows,
        });

        // lsn_str points into a PGresult cleared by a defer above — dupe before returning.
        return SnapshotResult{
            .lsn_text = try allocator.dupe(u8, lsn_str),
            .batch_count = batch,
            .row_count = total_rows,
        };
    }

    /// Publish snapshot start notification
    fn publishSnapshotStart(
        allocator: std.mem.Allocator,
        js: *nats.JS,
        table_name: []const u8,
        snapshot_id: []const u8,
        lsn: []const u8,
        format: encoder_mod.Format,
        should_stop: *std.atomic.Value(bool),
        topo: *const topology_mod.Topology,
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

        const subject = try topology_mod.render(
            allocator,
            topo.snapshot_start_pattern,
            &.{.{ .name = "table", .value = table_name }},
            null,
        );
        defer allocator.free(subject);

        // Publish to JetStream with retry logic (no msg_id needed for start notification)
        try publishWithRetry(js, subject, null, encoded, should_stop);

        log.info("🚀 Published snapshot start → {s} (LSN watermark: {s})", .{ subject, lsn });
    }

    /// Publish snapshot metadata
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
        topo: *const topology_mod.Topology,
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

        const subject = try topology_mod.render(
            allocator,
            topo.snapshot_meta_pattern,
            &.{.{ .name = "table", .value = table_name }},
            null,
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

        // Also publish to the KV bucket so clients can check for a cached snapshot
        // before requesting a fresh one. Subject built from topology, not a literal:
        // renaming the bucket there must move the bridge and `nats-init` together.
        const kv_subject = try topology_mod.render(
            allocator,
            topo.kv_snapshots_subject_pattern,
            &.{.{ .name = "table", .value = table_name }},
            null,
        );
        defer allocator.free(kv_subject);
        try publishWithRetry(js, kv_subject, null, encoded, should_stop);

        log.info("📋 Published snapshot metadata → {s} (and KV {s})", .{ subject, topo.kv_snapshots });
    }

};









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







/// Publish schema (column names) to NATS so consumer knows the array field order
/// Subject: config.Snapshot.schema_subject_pattern (under init.> so INIT stores it)
fn publishSchema(
    allocator: std.mem.Allocator,
    js: *nats.JS,
    table_name: []const u8,
    snapshot_id: []const u8,
    column_names: [][]const u8,
    format: encoder_mod.Format,
    should_stop: *std.atomic.Value(bool),
    topo: *const topology_mod.Topology,
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

    const subject = try topology_mod.render(allocator, topo.snapshot_schema_pattern, &.{
        .{ .name = "table", .value = table_name },
        .{ .name = "snapshot_id", .value = snapshot_id },
    }, null);
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
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const micro_seed = @as(u64, @intCast(ts.sec)) * 1_000_000 + @as(u64, @intCast(ts.nsec)) / 1_000;
    var prng = std.Random.DefaultPrng.init(micro_seed);
    const random_suffix = prng.random().int(u16);

    return try std.fmt.allocPrint(
        allocator,
        "snap-{d}-{x:0>4}",
        .{ @as(i64, c.time(null)), random_suffix },
    );
}

// ─── BootConnect ────────────────────────────────────────────────────────────────
//
// The behaviour under test is an asymmetry that is easy to get backwards: bounded
// before the first success, unbounded after it. `record` is exercised rather than
// `failed` so the fatal path can be asserted without the test runner treating its own
// FATAL log line as a failure.

test "BootConnect: gives up after the budget and stops the bridge" {
    var stop = std.atomic.Value(bool).init(false);
    var fatal = std.atomic.Value(bool).init(false);
    var boot = BootConnect{ .who = "test", .should_stop = &stop, .fatal = &fatal, .requests_stream = "REQUESTS" };

    for (1..config.Retry.listener_boot_connect_attempts) |_| {
        try std.testing.expectEqual(BootConnect.Verdict.retry, boot.record());
        try std.testing.expect(!stop.load(.acquire));
    }

    try std.testing.expectEqual(BootConnect.Verdict.give_up, boot.record());
    try std.testing.expect(stop.load(.acquire));
    // Both, and in that order: the exit code is what a supervisor acts on.
    try std.testing.expect(fatal.load(.acquire));
}

test "BootConnect: a listener that connected once retries forever" {
    var stop = std.atomic.Value(bool).init(false);
    var fatal = std.atomic.Value(bool).init(false);
    var boot = BootConnect{ .who = "test", .should_stop = &stop, .fatal = &fatal, .requests_stream = "REQUESTS" };

    boot.succeeded();

    // Ten times the boot budget: an outage after a working connection must never be
    // the thing that stops the bridge.
    for (0..config.Retry.listener_boot_connect_attempts * 10) |_| {
        try std.testing.expectEqual(BootConnect.Verdict.outage, boot.record());
    }
    try std.testing.expect(!stop.load(.acquire));
    try std.testing.expect(!fatal.load(.acquire));
}

test "BootConnect: a success part-way through the budget clears it" {
    var stop = std.atomic.Value(bool).init(false);
    var fatal = std.atomic.Value(bool).init(false);
    var boot = BootConnect{ .who = "test", .should_stop = &stop, .fatal = &fatal, .requests_stream = "REQUESTS" };

    // Two failures, then NATS comes up — the earlier attempts must not carry over, or a
    // slow-starting broker would arm a trap that fires on the first later outage.
    _ = boot.record();
    _ = boot.record();
    boot.succeeded();

    try std.testing.expectEqual(@as(u32, 0), boot.attempts);
    try std.testing.expectEqual(BootConnect.Verdict.outage, boot.record());
    try std.testing.expect(!stop.load(.acquire));
}
