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
const nats_publisher = @import("nats_publisher.zig");
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
    conn_nats: *nats.Connection,
    tenant: []const u8,
    table_name: []const u8,
    error_type: []const u8,
    error_message: []const u8,
    available_tables: []const []const u8,
    topo: *const topology_mod.Topology,
    /// The snapshot that failed, when one had been started. Null for a request rejected
    /// before generation began (unknown table, refused table). Present so an operator can
    /// tie the failure to the chunks it wrote — without it, an aborted snapshot and its
    /// orphans could only be correlated by timestamp.
    snapshot_id: ?[]const u8,
) !void {
    const subject = try topology_mod.render(
        allocator,
        topo.snapshot_error_pattern,
        &.{ .{ .name = "tenant", .value = tenant }, .{ .name = "table", .value = table_name } },
        null,
    );
    defer allocator.free(subject);

    var encoder = encoder_mod.Encoder.init(allocator, .msgpack);
    defer encoder.deinit();

    var map = encoder.createMap();
    defer map.free(allocator);

    try map.put(encoder.allocator, "table", try encoder.createString(table_name));
    try map.put(encoder.allocator, "status", try encoder.createString("failed"));
    try map.put(encoder.allocator, "error_type", try encoder.createString(error_type));
    try map.put(encoder.allocator, "error_message", try encoder.createString(error_message));
    try map.put(encoder.allocator, "timestamp", encoder.createInt(@as(i64, c.time(null))));
    if (snapshot_id) |sid| {
        try map.put(encoder.allocator, "snapshot_id", try encoder.createString(sid));
    }

    // Tell the client what it *could* have asked for — the usual cause is a typo or a
    // table outside the publication.
    var tables_array = try encoder.createArray(available_tables.len);
    for (available_tables, 0..) |t, i| {
        try tables_array.setIndex(i, try encoder.createString(t));
    }
    try map.put(encoder.allocator, "available_tables", tables_array);

    const payload = try encoder.encode(map);
    defer allocator.free(payload);

    try conn_nats.publish(subject, payload);
}

/// How long the REQUESTS stream keeps an unread request, in seconds. 0 = unknown.
///
/// Read from the server rather than from config, because the value belongs to `nats-init`
/// (`SNAP_RET_SECONDS` in .env.admin) and the bridge is never told it. Asking is the only
/// way to know what window the requests it is serving actually have.
fn requestWindowSeconds(js: *nats.JetStream, stream: []const u8) u64 {
    var res = js.getStreamInfo(stream) catch |err| {
        log.debug("could not read '{s}' max_age ({}); queue-expiry warning disabled", .{ stream, err });
        return 0;
    };
    defer res.deinit();
    const max_age = res.value.config.max_age;
    if (max_age <= 0) return 0; // 0 means "never expires"
    return @intCast(@divTrunc(max_age, std.time.ns_per_s));
}

/// Is this table in the publication *now*?
///
/// `monitored_tables` is read once at boot, and a publication changes without restarting
/// the bridge: `ALTER PUBLICATION … ADD TABLE` after startup left the snapshot listener
/// refusing a table the DDL path had *already published a schema for* — the client built a
/// local table it could never seed, and the only remedy was a restart nothing told the
/// operator to perform.
///
/// So the boot list is treated as a cache, not as truth: when it says no, ask Postgres
/// before refusing. One query, on a path that only runs when a request would otherwise be
/// rejected.
fn isTableInPublication(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publication: []const u8,
    table: []const u8,
) bool {
    const conninfo = pg_config.connInfo(allocator, false) catch return false;
    defer allocator.free(conninfo);

    const conn = c.PQconnectdb(conninfo.ptr);
    defer c.PQfinish(conn);
    if (c.PQstatus(conn) != c.CONNECTION_OK) return false;

    const query =
        \\SELECT 1 FROM pg_publication_tables
        \\WHERE pubname = $1 AND schemaname = 'public' AND tablename = $2
    ;
    const pub_z = allocator.dupeZ(u8, publication) catch return false;
    defer allocator.free(pub_z);
    const tbl_z = allocator.dupeZ(u8, table) catch return false;
    defer allocator.free(tbl_z);

    const values = [_][*c]const u8{ pub_z.ptr, tbl_z.ptr };
    const res = c.PQexecParams(conn, query, 2, null, &values, null, null, 0);
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) return false;
    return c.PQntuples(res) > 0;
}

/// Is there any row after the cursor?
///
/// The end-of-table test. A short chunk is ambiguous — the running byte sum trims chunks
/// routinely, and a trimmed chunk is indistinguishable from the last one by row count
/// alone — so the only honest answer comes from asking. One indexed lookup against the
/// same keyset predicate the next chunk would use, run only when a chunk *looks* final.
fn hasRowsBeyond(
    allocator: std.mem.Allocator,
    conn: *c.PGconn,
    table_name: []const u8,
    columns: []const pg_copy_binary.ColumnMeta,
    pk_idx: []const usize,
    cursor_values: []const []const u8,
) !bool {
    if (cursor_values.len == 0) return true;

    const predicate = try pg_copy_binary.keysetPredicate(allocator, columns, pk_idx, cursor_values);
    defer allocator.free(predicate);

    const query = try utils.allocPrintZ(
        allocator,
        "SELECT 1 FROM \"{s}\" WHERE {s} LIMIT 1",
        .{ table_name, predicate },
    );
    defer allocator.free(query);

    const res = c.PQexec(conn, query.ptr);
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        // Cannot prove the table ended, so assume it has not: another chunk that turns out
        // empty costs one round trip, while a wrong "final" costs the rest of the table.
        log.warn("📸 end-of-table check failed for '{s}': {s}", .{ table_name, c.PQerrorMessage(conn) });
        return true;
    }
    return c.PQntuples(res) > 0;
}

/// Rows that should fit one message, given what a row costs.
///
/// Aims at 4/5 of the budget rather than all of it: the input is a *mean*, and a run of
/// above-average rows still has to fit. The running sum in the chunk query enforces the
/// budget exactly — this only decides how often the encoder has to re-encode a prefix.
fn rowsPerChunk(bytes_per_row: usize, budget: usize, ceiling: usize) usize {
    const per_row = @max(bytes_per_row, 1);
    const target = budget / config.Snapshot.chunk_fill_den * config.Snapshot.chunk_fill_num;
    return std.math.clamp(target / per_row, 1, ceiling);
}

/// Publish the same suspension the CDC path publishes, from the snapshot thread.
///
/// `event_processor.publishSuspension` cannot be reused: it goes through the ring buffer
/// via `acquireAndFillSlot`, which only the replication thread owns. The payload shape is
/// copied deliberately — a client cannot be asked to understand two spellings of "this
/// table is suspended", and the reason string comes from the same `RefusedTables.Reason`
/// enum, so the wire word cannot drift between the two producers.
fn publishSuspension(
    allocator: std.mem.Allocator,
    js: *nats.JetStream,
    topo: *const topology_mod.Topology,
    table_name: []const u8,
    reason: []const u8,
    lsn: u64,
    should_stop: *std.atomic.Value(bool),
) !void {
    // `"writable":false` always — see `event_processor.publishSuspension`'s comment.
    // Must match that payload's spelling exactly, same reasoning as this function's own
    // doc comment on why the shape is copied rather than shared.
    const payload = try std.fmt.allocPrint(
        allocator,
        "{{\"table\":\"{s}\",\"suspended\":true,\"reason\":\"{s}\",\"lsn\":{d},\"writable\":false}}",
        .{ table_name, reason, lsn },
    );
    defer allocator.free(payload);

    const subject = try topology_mod.render(
        allocator,
        topo.kv_schemas_subject_pattern,
        &.{.{ .name = "table", .value = table_name }},
        null,
    );
    defer allocator.free(subject);

    try publishWithRetry(js, subject, null, payload, should_stop);
    log.warn("🚫 Published suspension for '{s}' (reason: {s}) → {s}", .{ table_name, reason, subject });
}

/// What one scan says about a table's row widths.
const RowWidths = struct {
    /// The widest row. A row at or above the message budget makes the table
    /// unsnapshottable — and unreplicable by CDC too, which is why it suspends rather
    /// than merely failing this request.
    max_bytes: usize,
    /// The mean, for sizing chunks. Only a hint: the running sum is what enforces the
    /// budget.
    avg_bytes: usize,
};

/// Measure the table's rows before transferring any of them.
///
/// Runs inside the snapshot's REPEATABLE READ transaction, so what it measures is exactly
/// what the COPYs will later read — no row can grow in between.
///
/// Cheap for the types that matter: `octet_length` on `text`/`bytea` reads the length from
/// the TOAST pointer without fetching the value. Measured on 200 rows of 256 KiB blobs,
/// this touched 2 shared buffers against 430 for an expression that must read the data.
fn measureWidestRow(
    allocator: std.mem.Allocator,
    conn: *c.PGconn,
    table_name: []const u8,
    size_expr: []const u8,
) !RowWidths {
    const query = try utils.allocPrintZ(
        allocator,
        "SELECT coalesce(max({s}),0)::bigint, coalesce(avg({s}),0)::bigint FROM \"{s}\"",
        .{ size_expr, size_expr, table_name },
    );
    defer allocator.free(query);

    const res = c.PQexec(conn, query.ptr);
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        log.err("📸 row-width scan failed for '{s}': {s}", .{ table_name, c.PQerrorMessage(conn) });
        return error.RowWidthScanFailed;
    }
    if (c.PQntuples(res) == 0) return .{ .max_bytes = 0, .avg_bytes = 0 };

    const max_text = std.mem.span(c.PQgetvalue(res, 0, 0));
    const avg_text = std.mem.span(c.PQgetvalue(res, 0, 1));
    return .{
        .max_bytes = std.fmt.parseInt(usize, max_text, 10) catch 0,
        .avg_bytes = std.fmt.parseInt(usize, avg_text, 10) catch 0,
    };
}

/// Delete an aborted snapshot's chunks from the INIT stream.
///
/// A snapshot that dies after publishing chunk 0 leaves those chunks behind under a
/// `snapshot_id` no descriptor will ever reference — the descriptor is written only after
/// COMMIT, so no client can find them. They are invisible, not harmless: `INIT` is
/// 8 GiB with **discard = old**, so a wide table failing repeatedly evicts *other*
/// tables' snapshot chunks and schema messages. A table that is broken takes down tables
/// that are not.
///
/// Best effort. If the purge fails the data still ages out at the stream's max-age, and
/// the descriptor gate still protects every client — this reclaims space, it does not
/// protect correctness, and it must never turn a failed snapshot into a failed thread.
fn purgeOrphanChunks(
    allocator: std.mem.Allocator,
    js: *nats.JetStream,
    topo: *const topology_mod.Topology,
    tenant: []const u8,
    table_name: []const u8,
    snapshot_id: []const u8,
) void {
    // `>` in the chunk position turns the publish pattern into a subject filter covering
    // every chunk of exactly this snapshot. Built from the same topology pattern the
    // chunks were published with, so a renamed subject space cannot leave the purge
    // pointing at the old one.
    const chunk_filter = topology_mod.render(allocator, topo.snapshot_data_pattern, &.{
        .{ .name = "tenant", .value = tenant },
        .{ .name = "table", .value = table_name },
        .{ .name = "snapshot_id", .value = snapshot_id },
        .{ .name = "chunk", .value = ">" },
    }, null) catch return;
    defer allocator.free(chunk_filter);

    const schema_subject = topology_mod.render(allocator, topo.snapshot_schema_pattern, &.{
        .{ .name = "tenant", .value = tenant },
        .{ .name = "table", .value = table_name },
        .{ .name = "snapshot_id", .value = snapshot_id },
    }, null) catch return;
    defer allocator.free(schema_subject);

    // INIT is split per tenant (same reason as CDC — a JetStream purge is stream-scoped,
    // not subject-checked). Which stream actually holds this tenant's chunks isn't known
    // here without threading OPEN_TENANT into the bridge, so this tries both candidates
    // and tolerates whichever one doesn't apply — cheap, since this is best-effort orphan
    // cleanup on an already-failed snapshot, not a path anything correctness-sensitive
    // depends on. `js.purgeStream` on a stream with no matching messages (or a tenant
    // stream that doesn't exist at all) just fails softly into the existing warn-and-skip
    // branch below.
    var upper_buf: [64]u8 = undefined;
    const upper_len = @min(tenant.len, upper_buf.len);
    const tenant_upper = std.ascii.upperString(upper_buf[0..upper_len], tenant[0..upper_len]);
    const tenant_stream = std.fmt.allocPrint(allocator, "{s}{s}", .{ topo.init_stream_prefix, tenant_upper }) catch return;
    defer allocator.free(tenant_stream);

    for ([_][]const u8{ tenant_stream, topo.init_stream_public }) |stream| {
        for ([_][]const u8{ chunk_filter, schema_subject }) |filter| {
            // Native here; the vendored client had no purge-by-filter, so this used to call
            // a `PURGE_FILTER` added to it by hand.
            var res = js.purgeStream(stream, .{ .filter = filter }) catch |err| {
                log.warn("🧹 could not purge orphaned '{s}' on {s} ({}); it will age out with the stream", .{ filter, stream, err });
                continue;
            };
            res.deinit();
            log.info("🧹 purged orphaned snapshot data: {s} on {s}", .{ filter, stream });
        }
    }
}

/// Publish to NATS JetStream with retry logic and exponential backoff
/// Matches the retry strategy used in batch_publisher.zig for consistency
/// Checks should_stop flag during retries to allow graceful shutdown
fn publishWithRetry(
    js: *nats.JetStream,
    subject: []const u8,
    msg_id: ?[]const u8,
    payload: []const u8,
    should_stop: *std.atomic.Value(bool),
) !void {
    var retry_count: u32 = 0;
    const max_retries = config.Retry.publish_max_retries;
    var backoff_ms: u64 = config.Retry.publish_backoff_ms;

    while (!should_stop.load(.acquire)) {
        const result = js.publish(subject, payload, .{ .msg_id = msg_id });

        if (result) |ok| {
            var ack = ok;
            ack.deinit();
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
/// The publication's row filter for this table, or null when it publishes every row.
///
/// pgoutput evaluates `prqual` on every change, so a filtered publication ships only the
/// matching rows — while a snapshot is a plain `SELECT` and ships all of them. Left
/// unhandled, the two paths describe different tables: a client seeds with rows the change
/// feed will never mention again, and they sit in the replica forever because no delete
/// ever arrives for them.
///
/// ⚠️ The expression is taken from `pg_get_expr`, i.e. PostgreSQL's own rendering of a
/// parsed node tree — not from user text. It cannot carry an injection: the DBA already
/// had to run `ALTER PUBLICATION` to put it there, and what comes back is the planner's
/// deparse of what was stored.
///
/// Returns null on any failure, which keeps a snapshot possible on a database whose
/// catalog this cannot read — at the cost of the filter, which is the safer direction to
/// fail only because the alternative is no snapshot at all. It is logged loudly.
fn publicationRowFilter(
    allocator: std.mem.Allocator,
    conn: *c.PGconn,
    table_name: []const u8,
    publication: []const u8,
) ?[]const u8 {
    var schema: []const u8 = "public";
    var table: []const u8 = table_name;
    if (std.mem.indexOfScalar(u8, table_name, '.')) |dot| {
        schema = table_name[0..dot];
        table = table_name[dot + 1 ..];
    }

    const query = utils.allocPrintZ(
        allocator,
        \\SELECT pg_get_expr(pr.prqual, pr.prrelid)
        \\FROM pg_publication_rel pr
        \\JOIN pg_publication p ON p.oid = pr.prpubid
        \\WHERE p.pubname = '{s}' AND pr.prrelid = '"{s}"."{s}"'::regclass;
    ,
        .{ publication, schema, table },
    ) catch return null;
    defer allocator.free(query);

    const res = c.PQexec(conn, query.ptr);
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK or c.PQntuples(res) == 0) return null;
    if (c.PQgetisnull(res, 0, 0) == 1) return null; // published whole — the common case

    const expr = std.mem.span(c.PQgetvalue(res, 0, 0));
    if (expr.len == 0) return null;
    return allocator.dupe(u8, expr) catch null;
}

fn getTableColumns(
    allocator: std.mem.Allocator,
    conn: *c.PGconn,
    table_name: []const u8,
    /// The publication whose column list bounds the snapshot. A table published whole
    /// has `prattrs IS NULL` and every column is taken, which is the common case.
    publication: []const u8,
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
        \\LEFT JOIN pg_publication_rel pr
        \\  ON pr.prrelid = a.attrelid
        \\ AND pr.prpubid = (SELECT oid FROM pg_publication WHERE pubname = '{s}')
        \\WHERE a.attrelid = '"{s}"."{s}"'::regclass
        \\  AND a.attnum > 0 AND NOT a.attisdropped
        \\  AND (pr.prattrs IS NULL OR a.attnum = ANY(pr.prattrs))
        \\ORDER BY a.attnum;
    ,
        .{ publication, schema, table },
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
    js: *nats.JetStream, // JetStream connection for publishing
    monitored_tables: []const []const u8,
    refused: *refused_tables.Registry,
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
    /// The publication this bridge streams. Kept so the snapshot thread can re-check
    /// membership against Postgres when its boot-time list says a table is unknown.
    publication_name: []const u8,
    refused: *refused_tables.Registry,
    thread: ?std.Thread = null,
    chunk_size: usize,
    io: std.Io,
    /// Where NATS is, resolved once in `bridge.zig`. Not re-derived here: see
    /// `Config.Nats.Endpoint`.
    endpoint: config.Nats.Endpoint,
    /// Set when a listener gives up on its first connection; see `BootConnect`.
    boot_fatal: *std.atomic.Value(bool),
    /// Wire names, read from grammar.json at startup. See src/topology.zig.
    topology: *const topology_mod.Topology,

    /// Initialize snapshot listener (does not start the thread)
    pub fn init(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        should_stop: *std.atomic.Value(bool),
        monitored_tables: []const []const u8,
        publication_name: []const u8,
        refused: *refused_tables.Registry,
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
            .publication_name = publication_name,
            .refused = refused,
            .thread = null,
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
        // The schema-request listener (`init.schema.<table>`) that used to run alongside
        // this was removed: it was a pull mechanism from an earlier design that predates
        // the current push model (`event_processor.publishBootSchemas` at boot,
        // `packDdlToSlot` on every DDL change, both writing straight to `$KV.schemas` —
        // see PROTOCOL.md). Its own stated rationale, "answers on its own connection so a
        // request never queues behind a slow COPY," only matters if a client asks and
        // waits; the real client only ever `kv.watch()`s `$KV.schemas`, which delivers the
        // current value immediately and every update after, with nothing to queue behind.
        // Zero scenario-script coverage and zero use in App.tsx confirmed it was never
        // exercised outside manual testing.
        log.info("📸 Spawning snapshot request listener...", .{});
        const snapshot_thread = try std.Thread.spawn(.{}, listenForSnapshotRequests, .{
            self.allocator,
            self.pg_config,
            self.should_stop,
            self.monitored_tables,
            self.publication_name,
            self.refused,
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

        // Wait for the snapshot listener to finish.
        snapshot_thread.join();
    }

    /// Snapshot request handler - listens on "snapshot.request.>" and generates snapshots
    /// Note: Thread functions cannot return errors - all errors must be caught internally
    fn listenForSnapshotRequests(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        should_stop: *std.atomic.Value(bool),
        monitored_tables: []const []const u8,
        publication_name: []const u8,
        refused: *refused_tables.Registry,
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
            var snap_conn = nats.Connection.init(allocator, io, .{
                .user = endpoint.user,
                .password = endpoint.pass,
                .nkey_seed = endpoint.seed,
            });
            defer snap_conn.deinit();

            const snap_url = std.fmt.allocPrint(allocator, "nats://{s}:{d}", .{ endpoint.host, endpoint.port }) catch continue;
            defer allocator.free(snap_url);
            snap_conn.connect(snap_url) catch |err| {
                if (boot.failed(err, endpoint.host, endpoint.port)) return;
                utils.sleep(reconnect_delay_ms * std.time.ns_per_ms);
                continue;
            };

            var snap_js = nats.JetStream.init(&snap_conn, .{});

            const consumer = snap_js.pullSubscribe(
                topo.request_subject_wildcard,
                "bridge_snapshot_worker",
                .{
                    .stream = topo.stream_requests,
                    .config = .{
                        .ack_policy = .explicit,
                        .deliver_policy = .all,
                        .filter_subject = topo.request_subject_wildcard,
                        // A snapshot runs synchronously in this loop and can take minutes
                        // on a large table. The default 30s ack_wait would redeliver the
                        // request while the first COPY is still running — duplicate
                        // snapshots for one request.
                        .ack_wait = config.Snapshot.request_ack_wait_ns,
                        // A request that keeps failing must not be retried forever.
                        .max_deliver = config.Snapshot.request_max_deliver,
                    },
                },
            ) catch |err| {
                // Bounded on the first pass only. The common permanent failure here is
                // not a bad host but a `REQUESTS` stream `nats-init` never created —
                // which no amount of retrying fixes, and which used to be invisible
                // behind one log line every 2s while the bridge otherwise looked healthy.
                if (boot.failed(err, endpoint.host, endpoint.port)) return;
                log.err("📸 Snapshot listener: Failed to start consumer on '{s}': {} - retrying in {d}ms", .{ topo.stream_requests, err, reconnect_delay_ms });
                utils.sleep(reconnect_delay_ms * std.time.ns_per_ms);
                continue;
            };
            defer consumer.deinit();
                        boot.succeeded();

            log.info("📸 Snapshot listener: ✅ Consuming '{s}' from stream '{s}'", .{
                topo.request_subject_wildcard,
                topo.stream_requests,
            });


            // Listen for snapshot requests
            while (!should_stop.load(.acquire)) {
                var batch = consumer.fetch(1, .{ .duration = .{ .raw = .fromMilliseconds(500), .clock = .awake } }) catch {
                    continue;
                };
                defer batch.deinit();

                for (batch.messages) |msg| {
                    // ⚠️ Status frames (408, 409, idle heartbeats) no longer reach here —
                    // the library recognises them. With the vendored client they arrived
                    // as ordinary messages with no reply-to, and acking one panicked on a
                    // null unwrap, killing the snapshot path for the life of the process.
                    // The guard for that lived here and is now the library's job.

                    // ACKed **now**, before the COPY, not at scope exit.
                    //
                    // This was `defer ACK`, whose comment claimed an immediate ack while
                    // `defer` in fact held it for the whole snapshot — so a table taking
                    // longer than `ack_wait` was redelivered and snapshotted twice, which
                    // is precisely what the comment said it was avoiding. Holding the ack
                    // buys only redelivery-on-crash, and pays for it with duplicate work
                    // on exactly the tables least able to afford it. The stream's own
                    // max-msgs-per-subject window is what stops a re-request meanwhile.
                    msg.ack() catch {};

                    // Extract tenant + table from subject: snapshot.request.<tenant>.<table>
                    // — tenant-keyed, not principal-keyed (NOTES.md §1.12 part 2: a dump is
                    // shared by every principal in a tenant). The tenant is a NATS subject
                    // token the requester's own permissions already bounded them to; the
                    // bridge relays it into the query, it does not validate which tenants
                    // exist (PROTOCOL.md "The Connection Flow", Step 0).
                    const subject = msg.msg.subject;

                    const rest = blk: {
                        const prefix = topo.request_subject_prefix;
                        if (std.mem.startsWith(u8, subject, prefix)) {
                            break :blk subject[prefix.len..];
                        }
                        log.err("📸 Invalid snapshot request subject: {s}", .{subject});
                        continue;
                    };
                    const dot = std.mem.indexOfScalar(u8, rest, '.') orelse {
                        log.err("📸 Invalid snapshot request subject (no tenant segment): {s}", .{subject});
                        continue;
                    };
                    const tenant = rest[0..dot];
                    const table_name = rest[dot + 1 ..];

                    log.info("📸 Snapshot request received for tenant '{s}', table '{s}'", .{ tenant, table_name });

                    // A refused table is in the publication but has no schema in KV, so
                    // seeding from it would populate a client table built from a shape
                    // Postgres no longer has. Checked before monitoring, because
                    // "refused" is the more specific and more actionable answer.
                    if (refused.reasonFor(table_name)) |reason| {
                        // ⚠️ The *actual* reason, not a guess. This used to say "no primary
                        // key" for every refusal, which sent operators to fix the one thing
                        // that was not wrong: a table suspended for `row_too_large` has a
                        // perfectly good key.
                        log.warn(
                            "📸 Table '{s}' is refused ({s}) — publishing error. Fix: {s}",
                            .{ table_name, reason.wireName(), reason.fixHint() },
                        );
                        // Stack, not allocator: the longest reason and hint together are
                        // well under this, and a fallible allocation on an error path is
                        // how an error report becomes a second error.
                        var detail_buf: [512]u8 = undefined;
                        const detail = std.fmt.bufPrint(
                            &detail_buf,
                            "Table is refused ({s}): replication is suspended, so no snapshot can be served. {s}",
                            .{ reason.wireName(), reason.fixHint() },
                        ) catch "Table is refused: replication is suspended, so no snapshot can be served";
                        publishSnapshotError(
                            allocator,
                            &snap_conn,
                            tenant,
                            table_name,
                            "table_refused",
                            detail,
                            monitored_tables,
                            topo,
                            null,
                        ) catch |err| {
                            log.err("📸 Failed to publish snapshot error for '{s}': {}", .{ table_name, err });
                        };
                        continue;
                    }

                    // Validate table is monitored. The boot list first — one string
                    // comparison — and Postgres only when that says no, because a
                    // publication can grow while the bridge runs.
                    const is_monitored = publication_mod.isTableMonitored(table_name, monitored_tables) or
                        isTableInPublication(allocator, pg_config, publication_name, table_name);
                    if (!is_monitored) {
                        log.warn("📸 Table '{s}' not in monitored tables — publishing error", .{table_name});
                        publishSnapshotError(
                            allocator,
                            &snap_conn,
                            tenant,
                            table_name,
                            "table_not_monitored",
                            "Table is not in the publication this bridge replicates",
                            monitored_tables,
                            topo,
                            null,
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
                        tenant,
                        table_name,
                        snapshot_id,
                        chunk_size,
                        should_stop,
                        io,
                        endpoint,
                        topo,
                        refused,
                        publication_name,
                    ) catch |err| {
                        log.err("📸 Snapshot generation failed for '{s}': {}", .{ table_name, err });
                        publishSnapshotError(
                            allocator,
                            &snap_conn,
                            tenant,
                            table_name,
                            "generation_failed",
                            @errorName(err),
                            monitored_tables,
                            topo,
                            snapshot_id,
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
        tenant: []const u8,
        table_name: []const u8,
        snapshot_id: []const u8,
        chunk_size: usize,
        should_stop: *std.atomic.Value(bool),
        io: std.Io,
        endpoint: config.Nats.Endpoint,
        topo: *const topology_mod.Topology,
        /// Shared with preflight and the CDC path. A row too wide to publish is refused
        /// here on the same terms, so the two paths cannot disagree about one table.
        refused: *refused_tables.Registry,
        /// The publication, so the snapshot can be bounded by the same column list and
        /// row filter that bound CDC. Without it the two paths describe different tables:
        /// pgoutput honours `prattrs`/`prqual`, a plain `SELECT` does not.
        publication_name: []const u8,
    ) !SnapshotResult {
        const started_at = @as(i64, c.time(null));
        log.info("📸 Generating snapshot for '{s}' (id={s}, row ceiling {d})", .{
            table_name,
            snapshot_id,
            chunk_size,
        });

        // A connection of its own for publishing chunks: a snapshot can run for minutes,
        // and sharing the request consumer's connection would tie chunk publishing to the
        // lifetime of whatever that loop is doing.
        const pub_conn = try allocator.create(nats.Connection);
        defer allocator.destroy(pub_conn);
        pub_conn.* = nats.Connection.init(allocator, io, .{
            .user = endpoint.user,
            .password = endpoint.pass,
            .nkey_seed = endpoint.seed,
        });
        defer pub_conn.deinit();

        const pub_url = try std.fmt.allocPrint(allocator, "nats://{s}:{d}", .{ endpoint.host, endpoint.port });
        defer allocator.free(pub_url);
        pub_conn.connect(pub_url) catch |err| {
            log.err("📸 Failed to connect to JetStream: {}", .{err});
            return error.JetStreamConnectionFailed;
        };

        var js = nats.JetStream.init(pub_conn, .{});

        log.info("📸 Connected to JetStream for snapshot publishing", .{});

        // Chunks already on the wire when this function returns an error are orphans: the
        // KV descriptor is written only after COMMIT, so no client will ever learn the id
        // they were published under. Registered after `js.DISCONNECT` so it runs *before*
        // it — errdefers unwind in reverse — and guarded on the counter so a snapshot that
        // failed before publishing anything does not issue a pointless purge.
        var chunks_published: u32 = 0;
        errdefer if (chunks_published > 0) purgeOrphanChunks(
            allocator,
            &js,
            topo,
            tenant,
            table_name,
            snapshot_id,
        );

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

        // `zb.tenant`, from the authenticated request subject — never resolved through
        // zebridge_user_tenants, which is the write path's (NOTES.md §1.12 part 1: two
        // derivations of one fact can disagree; a token relayed from the subject that
        // already gated this request cannot disagree with itself). Parameterized, not
        // string-built: `tenant` is client-controlled input, however NATS-authenticated.
        // Must share this transaction, or the setting is gone before the SELECT/COPY
        // below ever runs — the same rule mutation_listener.zig's own `zb.principal`
        // documents, for the same reason (`set_config(..., is_local => true)` resets at
        // COMMIT). `zebridge_scope_reads_by_tenant()`'s `zb_reader_all` policy is what
        // reads this back; a table nobody scoped that way just sees it and ignores it.
        {
            const tenant_z = try allocator.dupeZ(u8, tenant);
            defer allocator.free(tenant_z);
            const set_sql = "SELECT set_config('" ++ config.Sync.tenant_setting ++ "', $1, true)";
            const set_params = [_]?[*:0]const u8{tenant_z.ptr};
            const set_result = c.PQexecParams(conn, set_sql, 1, null, &set_params[0], null, null, 0);
            defer c.PQclear(set_result);
            if (c.PQresultStatus(set_result) != c.PGRES_TUPLES_OK) {
                log.err("set_config('{s}', ...) failed: {s}", .{ config.Sync.tenant_setting, c.PQerrorMessage(conn) });
                return error.TransactionFailed;
            }
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
            tenant,
            table_name,
            snapshot_id,
            lsn_str,
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
        const columns = try getTableColumns(allocator, conn.?, table_name, publication_name);
        defer freeTableColumns(allocator, columns);

        // Inside the same transaction as the layout, for the same reason: a publication
        // altered mid-snapshot must not change which rows the later chunks carry.
        const row_filter = publicationRowFilter(allocator, conn.?, table_name, publication_name);
        defer if (row_filter) |f| allocator.free(f);
        if (row_filter) |f| {
            log.info("📸 '{s}': publication row filter applies — {s}", .{ table_name, f });
        }

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
        // Where a snapshot's time actually goes. Split because the two halves have
        // completely different remedies: `copy_encode` is Postgres and this process, while
        // `publish` is one synchronous ack per chunk against a file-backed stream — and if
        // that dominates, the fix is to pipeline publishes, which touches neither.
        var copy_encode_ns: u64 = 0;
        var publish_ns: u64 = 0;
        var cursor = Cursor{ .allocator = allocator };
        defer cursor.deinit();

        // What one chunk may encode to. The server states its limit in INFO on every
        // connection, so this is a fact rather than an assumption; the margin covers the
        // subject, the headers and the msg-id that travel with the payload.
        //
        // Computed *before* the buffer is allocated, so the buffer can be exactly this
        // size — then "the encoder ran out of room" and "this prefix exceeds a NATS
        // message" are one event with one handling path, instead of two that can disagree.
        const max_chunk_bytes = blk: {
            const advertised = nats_publisher.serverMaxPayload(&js) orelse {
                // Same guess CDC falls back on when it cannot ask the server either
                // (Buffers.default_max_event_data_buffer_log2) — NATS's own
                // out-of-the-box max_payload, not a value chosen for snapshots
                // specifically.
                const assumed: usize = @as(usize, 1) << @intCast(config.Buffers.default_max_event_data_buffer_log2);
                log.warn("📸 NATS did not advertise max_payload; capping chunks at {d} KB", .{assumed / 1024});
                break :blk assumed - config.Nats.payload_envelope_margin_bytes;
            };
            const budget = if (advertised > config.Nats.payload_envelope_margin_bytes)
                advertised - config.Nats.payload_envelope_margin_bytes
            else
                advertised / 2;
            break :blk @min(@as(usize, @intCast(budget)), config.Snapshot.encode_buffer_max_bytes);
        };
        const encode_buffer = try allocator.alloc(u8, max_chunk_bytes);
        defer allocator.free(encode_buffer);

        // ─── the widest row, before a single byte is transferred ──────────────────────
        //
        // One scan answers both questions the chunk loop needs. `max` says whether any row
        // is unpublishable — a row larger than a NATS message can never be sent, by this
        // path or by CDC, so the table is refused here rather than discovered halfway
        // through a COPY that already pulled it into memory. `avg` sizes the chunks.
        //
        // It also buys the loop its central guarantee: with `max < budget`, the running
        // sum below can never return zero rows, so the cursor always advances and the
        // "first row alone exceeds the budget" deadlock cannot occur.
        const size_expr = try pg_copy_binary.sizeExpression(allocator, columns);
        defer allocator.free(size_expr);

        const column_list = try pg_copy_binary.columnList(allocator, columns);
        defer allocator.free(column_list);

        const t_scan = utils.nanoTimestamp();
        const widest = try measureWidestRow(allocator, conn.?, table_name, size_expr);
        const scan_ms: u64 = @intCast(@divTrunc(utils.nanoTimestamp() - t_scan, std.time.ns_per_ms));
        if (widest.max_bytes >= max_chunk_bytes) {
            log.err(
                "🔴 SUSPENDING '{s}': its widest row is {d} bytes, over the {d}-byte message budget. A snapshot cannot split a row, and CDC cannot carry it either. Fix: move the oversized column out of the replicated table (store a reference, not the blob).",
                .{ table_name, widest.max_bytes, max_chunk_bytes },
            );
            _ = c.PQexec(conn, "ROLLBACK");

            // Same verdict CDC reaches when the row is written, reached here before a byte
            // moves. Registering it stops this bridge serving snapshots for the table and
            // drops its CDC events; the suspension tells clients to drop their copy.
            refused.refuse(table_name, refused_tables.Reason.row_too_large) catch |err| {
                log.err("🔴 Could not record refusal for '{s}': {}", .{ table_name, err });
            };
            publishSuspension(
                allocator,
                &js,
                topo,
                table_name,
                refused_tables.Reason.row_too_large.wireName(),
                parsePgLsn(lsn_str) catch 0,
                should_stop,
            ) catch |err| {
                log.err("🔴 Could not publish suspension for '{s}': {}", .{ table_name, err });
            };
            return error.RowTooLargeForMessage;
        }

        // 4/5 of the budget: `avg` is a mean, and a run of above-average rows still has to
        // fit. The running sum enforces the budget exactly; this only decides how full a
        // typical chunk gets, and aiming at 100% would just cause re-encodes.
        var rows_per_chunk = rowsPerChunk(widest.avg_bytes, max_chunk_bytes, chunk_size);
        log.debug("📸 chunk budget {d} bytes; widest row {d}, average {d} → {d} rows/chunk (ceiling {d})", .{
            max_chunk_bytes, widest.max_bytes, widest.avg_bytes, rows_per_chunk, chunk_size,
        });

        while (true) {
            _ = arena.reset(.retain_capacity);
            const chunk_alloc = arena.allocator();

            // The first chunk has no cursor, so it takes the whole table rather than a
            // sentinel value: `"id" > 0` would silently drop a row with a zero or
            // negative key, and there is no sentinel at all for a text or uuid key.
            const keyset: []const u8 = if (cursor.values.len == 0)
                "1=1"
            else
                try pg_copy_binary.keysetPredicate(chunk_alloc, columns, pk.idx, cursor.values);

            // The publication's row filter, ANDed in so the snapshot ships exactly the
            // rows CDC would. Resolved once per snapshot, above the loop, because it
            // cannot change inside the REPEATABLE READ transaction.
            const where_clause: []const u8 = if (row_filter) |f|
                try std.fmt.allocPrint(chunk_alloc, "({s}) AND ({s})", .{ keyset, f })
            else
                keyset;

            // Two nested bounds. The inner LIMIT caps how many rows Postgres *measures*;
            // the outer running sum caps how many it *sends*, so the bridge receives at
            // most one message's worth however wide the rows turn out to be.
            //
            // The column list is spelled out rather than `SELECT *`: the subquery carries
            // `zb_running`, and `SELECT *` would hand that extra column to a binary COPY
            // decoder that matches columns positionally against the catalog.
            const copy_query = try utils.allocPrintZ(
                chunk_alloc,
                "COPY (SELECT {s} FROM (SELECT {s}, sum({s}) OVER (ORDER BY {s}) AS zb_running" ++
                    " FROM \"{s}\" WHERE {s} ORDER BY {s} LIMIT {d}) s WHERE s.zb_running <= {d})" ++
                    " TO STDOUT WITH (FORMAT binary)",
                .{ column_list, column_list, size_expr, order_by, table_name, where_clause, order_by, rows_per_chunk, max_chunk_bytes },
            );

            // Initialize streaming encoder with fixed buffer
            var encoder = streaming_encoder.StreamingEncoder.init(encode_buffer);

            // Stream: PostgreSQL binary COPY → framing → decoder → encoder.
            // `columns` comes from the catalog inside this same REPEATABLE READ
            // transaction, because the binary header carries no layout of its own.
            var parser = pg_copy_binary.Streamer.init(chunk_alloc, @ptrCast(conn), columns);
            defer parser.deinit();

            const t_copy = utils.nanoTimestamp();
            const chunk = parser.streamToEncoder(.{
                .query = copy_query,
                .pk_idx = pk.idx,
                .limit = rows_per_chunk,
                .max_bytes = max_chunk_bytes,
                .expected_raw_bytes = max_chunk_bytes,
            }, &encoder) catch |err| {
                log.err("📸 binary COPY chunk failed for '{s}': {}", .{ table_name, err });
                _ = c.PQexec(conn, "ROLLBACK");
                return err;
            };
            copy_encode_ns += @intCast(utils.nanoTimestamp() - t_copy);
            const num_rows = chunk.encoded;

            log.debug("📸 chunk {d}: limit={d} fetched={d} encoded={d} bytes={d}", .{
                batch, chunk.limit, chunk.fetched, chunk.encoded, chunk.encoded_bytes,
            });

            // Correct the estimate with what the rows actually encoded to. The SQL size
            // expression is deliberately conservative — it sums `octet_length` plus a flat
            // per-column allowance, while MessagePack packs a small int into one byte — so
            // on a narrow table it can overstate by 2x and halve the chunk size for no
            // reason. One measurement fixes that from chunk 1 onward.
            //
            // Only ever *raises* the row count: the pre-scan's `max` is what guarantees
            // safety, and the running sum still trims any chunk that overshoots, so a
            // larger limit cannot produce an oversized message.
            if (chunk.encoded > 0) {
                const measured = @max(chunk.encoded_bytes / chunk.encoded, 1);
                rows_per_chunk = @max(rows_per_chunk, rowsPerChunk(measured, max_chunk_bytes, chunk_size));
            }

            // Advance the keyset cursor to the actual last row of this chunk, captured
            // during streaming. Correct for gaps, deletions, and non-sequential keys
            // (uuid, text) alike, because it never assumes what the next key would be.
            //
            // Done here rather than after publishing because the end-of-table check below
            // needs it: it asks whether any row exists past this cursor.
            if (num_rows > 0) {
                const pk_values = parser.getLastPkValues() orelse {
                    log.err("⚠️  No PK values captured from chunk — aborting snapshot", .{});
                    _ = c.PQexec(conn, "ROLLBACK");
                    return error.PkValueMissing;
                };
                // Copied out of the chunk arena, which the next iteration resets before it
                // builds the query that reads this.
                try cursor.set(pk_values);
            }

            // A short chunk is ambiguous — the running byte sum trims chunks routinely —
            // so `mayBeFinal` only decides whether the question is worth asking. The
            // answer costs one indexed lookup, and only on chunks that look like the last.
            //
            // `num_rows == 0` short-circuits it, which is also what makes the loop
            // terminate unconditionally: an empty chunk always ends the snapshot, so no
            // disagreement between the size estimate and the COPY can spin it forever.
            const is_final = num_rows == 0 or (chunk.mayBeFinal() and
                !(try hasRowsBeyond(chunk_alloc, conn.?, table_name, columns, pk.idx, cursor.values)));

            // On first chunk (even if empty), publish schema so consumer knows column order
            if (batch == 0) {
                const col_names = try chunk_alloc.alloc([]const u8, columns.len);
                for (columns, col_names) |col, *name| name.* = col.name;
                try publishSchema(
                    chunk_alloc,
                    &js,
                    tenant,
                    table_name,
                    snapshot_id,
                    col_names,
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
                .{ .name = "tenant", .value = tenant },
                .{ .name = "table", .value = table_name },
                .{ .name = "snapshot_id", .value = snapshot_id },
                .{ .name = "chunk", .value = chunk_text },
            }, null);

            const msg_id_buf = try std.fmt.allocPrint(
                chunk_alloc,
                config.Snapshot.data_msg_id_pattern,
                .{ table_name, snapshot_id, batch },
            );

            // Only the msg_id survives the migration.
            //
            // The vendored path also wrote Content-Type, X-Format, X-Snapshot-Version,
            // X-Schema-Ref, X-Snapshot-Chunk-Num, X-Snapshot-Rows-In-Chunk,
            // X-Snapshot-Total-Rows-So-Far and X-Snapshot-Final-Chunk — none of which any
            // consumer in the tree reads, and none of which PROTOCOL.md documents. The
            // chunk subject already carries the table, snapshot id and chunk number, and
            // the descriptor in KV carries the totals. Porting them would have asserted a
            // contract that never existed.
            // Publish to JetStream with headers and retry logic
            const t_pub = utils.nanoTimestamp();
            try publishWithRetry(&js, subject, msg_id_buf, payload, should_stop);
            publish_ns += @intCast(utils.nanoTimestamp() - t_pub);

            chunks_published += 1;
            log.info("📦 Published chunk {d} ({d} rows, {d} bytes) → {s} (msg_id={s})", .{
                batch,
                num_rows,
                payload.len,
                subject,
                msg_id_buf,
            });

            batch += 1;

            if (is_final) break;
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
            tenant,
            table_name,
            snapshot_id,
            lsn_str,
            batch,
            total_rows,
            should_stop,
            topo,
        );

        const elapsed: u64 = @intCast(@max(@as(i64, c.time(null)) - started_at, 0));
        log.info("✅ Snapshot complete: {s} ({d} batches, {d} rows, {d}s) — prescan {d}ms, copy+encode {d}ms, publish {d}ms", .{
            snapshot_id,
            batch,
            total_rows,
            elapsed,
            scan_ms,
            copy_encode_ns / std.time.ns_per_ms,
            publish_ns / std.time.ns_per_ms,
        });

        // Snapshots are served one at a time, and a request waiting its turn ages against
        // the REQUESTS stream's own max_age. So once a snapshot takes a large fraction of
        // that window, a request queued behind it is dropped by the broker before the
        // worker ever polls for it: the client gets no chunks, no error, and nothing to
        // retry against until the window clears.
        //
        // Warned rather than fixed: the window belongs to nats-init (SNAP_RET_SECONDS),
        // and the real remedies are a larger window or concurrent workers.
        const window_seconds = requestWindowSeconds(&js, topo.stream_requests);
        log.debug("📸 request window for '{s}': {d}s (0 = unknown)", .{ topo.stream_requests, window_seconds });
        if (window_seconds > 0 and elapsed * 2 > window_seconds) {
            log.warn(
                "⏳ '{s}' took {d}s against a {d}s request window. Snapshots run one at a time, so a request queued behind one this long expires unread. Raise SNAP_RET_SECONDS above the worst-case snapshot time, multiplied by how many tables may request at once.",
                .{ table_name, elapsed, window_seconds },
            );
        }

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
        js: *nats.JetStream,
        tenant: []const u8,
        table_name: []const u8,
        snapshot_id: []const u8,
        lsn: []const u8,
        should_stop: *std.atomic.Value(bool),
        topo: *const topology_mod.Topology,
    ) !void {
        var encoder = encoder_mod.Encoder.init(allocator, .msgpack);
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

        const encoded = try encoder.encode(start_map);
        defer allocator.free(encoded);

        const subject = try topology_mod.render(
            allocator,
            topo.snapshot_start_pattern,
            &.{ .{ .name = "tenant", .value = tenant }, .{ .name = "table", .value = table_name } },
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
        js: *nats.JetStream,
        tenant: []const u8,
        table_name: []const u8,
        snapshot_id: []const u8,
        lsn: []const u8,
        batch_count: u32,
        row_count: u64,
        should_stop: *std.atomic.Value(bool),
        topo: *const topology_mod.Topology,
    ) !void {
        var encoder = encoder_mod.Encoder.init(allocator, .msgpack);
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
            &.{ .{ .name = "tenant", .value = tenant }, .{ .name = "table", .value = table_name } },
            null,
        );
        defer allocator.free(subject);

        // No headers: `Content-Type`/`X-Snapshot-Version`/`X-Message-Type` were written
        // here and read by nothing — the payload is self-describing MessagePack, and no
        // consumer in the tree looks at them. Dropped with the migration rather than
        // ported, since porting them would have implied someone depended on them.

        // Publish to JetStream with retry logic
        try publishWithRetry(js, subject, null, encoded, should_stop);

        // Also publish to the KV bucket so clients can check for a cached snapshot
        // before requesting a fresh one. Subject built from topology, not a literal:
        // renaming the bucket there must move the bridge and `nats-init` together.
        // Tenant-keyed like the request/data subjects (NOTES.md §1.12 part 2): keyed by
        // table alone, a second tenant's descriptor would overwrite the first's.
        const kv_subject = try topology_mod.render(
            allocator,
            topo.kv_snapshots_subject_pattern,
            &.{ .{ .name = "tenant", .value = tenant }, .{ .name = "table", .value = table_name } },
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
    js: *nats.JetStream,
    tenant: []const u8,
    table_name: []const u8,
    snapshot_id: []const u8,
    column_names: [][]const u8,
    should_stop: *std.atomic.Value(bool),
    topo: *const topology_mod.Topology,
) !void {
    var encoder = encoder_mod.Encoder.init(allocator, .msgpack);
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
        .{ .name = "tenant", .value = tenant },
        .{ .name = "table", .value = table_name },
        .{ .name = "snapshot_id", .value = snapshot_id },
    }, null);
    defer allocator.free(subject);

    // No headers — see the note in the metadata publisher above.

    // Publish to JetStream with retry logic
    try publishWithRetry(js, subject, null, encoded, should_stop);

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
