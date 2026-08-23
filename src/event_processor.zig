//! CDC Event Processor
//!
//! Processes CDC events from PostgreSQL pgoutput format and enqueues them
//! for publishing to NATS. Runs in the main thread.

const std = @import("std");
const pgoutput = @import("pgoutput.zig");
const SPSCQueue = @import("spsc_queue.zig").SPSCQueue;
const msgpack = @import("msgpack");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const pg_conn = @import("pg_conn.zig");
const utils = @import("utils.zig");
const Config = @import("config.zig");
const RefusedTables = @import("refused_tables.zig");
const WritableTables = @import("writable_tables.zig");
const TypeRegistry = @import("type_registry.zig");
const Topology = @import("topology.zig");
const Preflight = @import("preflight.zig");
const Metrics = @import("metrics.zig").Metrics;
const batch_publisher = @import("batch_publisher.zig");
const schema_mapper = @import("schema_mapper.zig");

fn nanoNow() u64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s +
        @as(u64, @intCast(ts.nsec));
}

pub const log = std.log.scoped(.event_processor);

/// Tables whose schema is infrastructure, not client data.
///
/// Mirrors zebridge_is_internal_table() in init.{core,write}.template.sql. The DDL triggers already
/// skip these, but the boot pass iterates the publication's table list — which contains
/// zebridge_ddl_events, since its INSERTs are how schema events travel. Without this the
/// two paths disagree and clients build a local replica of our own bookkeeping.
fn isInternalTable(name: []const u8) bool {
    return std.mem.eql(u8, name, "zebridge_ddl_events") or
        std.mem.eql(u8, name, "schema_migrations");
}

/// Parse a PostgreSQL LSN in its text form ("1A/2B3C4D5E") into a u64, matching the
/// integer LSN the WAL path reports. Returns 0 when unparseable — 0 reads as "valid
/// from the beginning of the stream", which is the permissive choice: a client will
/// apply CDC rather than stall waiting for a schema it will never get.
fn parsePgLsnText(text: []const u8) u64 {
    const slash = std.mem.indexOfScalar(u8, text, '/') orelse return 0;
    const hi = std.fmt.parseInt(u64, text[0..slash], 16) catch return 0;
    const lo = std.fmt.parseInt(u64, text[slash + 1 ..], 16) catch return 0;
    return (hi << 32) | lo;
}

/// Format a DecodedValue for human-readable logging
/// Returns a string representation allocated in the provided arena
fn formatValueForLog(arena: std.mem.Allocator, value: pgoutput.DecodedValue) ![]const u8 {
    return switch (value) {
        .null => "NULL",
        .unchanged => "<unchanged TOAST>",
        .boolean => |v| if (v) "true" else "false",
        .int32 => |v| try std.fmt.allocPrint(arena, "{d}", .{v}),
        .int64 => |v| try std.fmt.allocPrint(arena, "{d}", .{v}),
        .float64 => |v| try std.fmt.allocPrint(arena, "{d}", .{v}),
        .text => |v| if (v.len > 50)
            try std.fmt.allocPrint(arena, "\"{s}...\" ({d} chars)", .{ v[0..47], v.len })
        else
            try std.fmt.allocPrint(arena, "\"{s}\"", .{v}),
        .numeric => |v| v,
        .jsonb => |v| if (v.len > 50)
            try std.fmt.allocPrint(arena, "{s}... ({d} chars)", .{ v[0..47], v.len })
        else
            v,
        .array => |v| if (v.len > 50)
            try std.fmt.allocPrint(arena, "{s}... ({d} chars)", .{ v[0..47], v.len })
        else
            v,
        .bytea => |v| try std.fmt.allocPrint(arena, "<bytea {d} bytes>", .{v.len}),
    };
}

/// Compare two DecodedValue instances for equality
/// Used for transition detection to determine if a column's value changed
fn valuesEqual(a: pgoutput.DecodedValue, b: pgoutput.DecodedValue) bool {
    // First check if types match
    if (@as(std.meta.Tag(pgoutput.DecodedValue), a) != @as(std.meta.Tag(pgoutput.DecodedValue), b)) {
        return false;
    }

    // Compare based on type
    return switch (a) {
        .null => true, // Both are null
        // Both sides unchanged means this UPDATE did not touch the column — which is
        // precisely "equal" for transition detection.
        .unchanged => true,
        .boolean => |av| av == b.boolean,
        .int32 => |av| av == b.int32,
        .int64 => |av| av == b.int64,
        .float64 => |av| av == b.float64,
        .text => |av| std.mem.eql(u8, av, b.text),
        .numeric => |av| std.mem.eql(u8, av, b.numeric),
        .jsonb => |av| std.mem.eql(u8, av, b.jsonb),
        .array => |av| std.mem.eql(u8, av, b.array),
        .bytea => |av| std.mem.eql(u8, av, b.bytea),
    };
}

/// Event processor that runs in the main thread
/// Decodes pgoutput tuples, creates CDC events, and enqueues them to the SPSC queue
pub const EventProcessor = struct {
    allocator: std.mem.Allocator,
    batch_publisher: *batch_publisher.BatchPublisher,
    metrics: ?*Metrics,
    transition_rules: *const Config.EventClassification.TransitionRules,
    pg_config: *const pg_conn.PgConf,
    refused: *RefusedTables.Registry,
    /// OID → typtype, populated from DDL events and at boot. The CDC decoder consults
    /// it for any type its switch does not cover; see type_registry.zig.
    types: *TypeRegistry.Registry,
    /// Wire names, read from topology.json at startup. See src/topology.zig.
    topology: *const Topology.Topology,
    /// Per-table version-column overrides and the global default, for the edge-writability
    /// report a table gets when it appears after boot. Same two values preflight uses.
    sync_rules: *const Config.EventClassification.TransitionRules,
    default_version_column: []const u8,
    /// `TENANT_RULES`: which column carries the tenant, per table. Empty means reads are
    /// unscoped — every subscriber of `cdc.>` receives every row, which is the default and
    /// is reported as such at boot.
    tenant_rules: *const Config.EventClassification.TransitionRules,
    /// `2^BASE_BUF` — the widest row CDC can carry, and therefore the widest a client may
    /// write. Published in the write contract because it is a **deployment** setting, not
    /// a protocol constant: a client that hardcoded it would be wrong the moment an
    /// operator raised it.
    event_buf_bytes: usize,
    /// Per-table edge-writability (NOTES.md §1.11), computed by preflight at boot and
    /// kept current here by `reportEdgeWritability` on a `CREATE TABLE` DDL event.
    /// `appendWriteContract` publishes it; nothing else reads it.
    writable: *WritableTables.Registry,
    /// Same role `writable`'s verdicts were checked against, so a table appearing after
    /// boot is graded by the same grant `preflight.run` used.
    writer_role: ?[]const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        batch_pub: *batch_publisher.BatchPublisher,
        metrics: ?*Metrics,
        transition_rules: *const Config.EventClassification.TransitionRules,
        pg_config: *const pg_conn.PgConf,
        refused: *RefusedTables.Registry,
        types: *TypeRegistry.Registry,
        topology: *const Topology.Topology,
        sync_rules: *const Config.EventClassification.TransitionRules,
        default_version_column: []const u8,
        tenant_rules: *const Config.EventClassification.TransitionRules,
        /// `2^BASE_BUF`. Published so a client can size a write before sending it.
        event_buf_bytes: usize,
        writable: *WritableTables.Registry,
        writer_role: ?[]const u8,
    ) EventProcessor {
        return .{
            .allocator = allocator,
            .batch_publisher = batch_pub,
            .metrics = metrics,
            .transition_rules = transition_rules,
            .pg_config = pg_config,
            .refused = refused,
            .types = types,
            .topology = topology,
            .sync_rules = sync_rules,
            .default_version_column = default_version_column,
            .tenant_rules = tenant_rules,
            .event_buf_bytes = event_buf_bytes,
            .writable = writable,
            .writer_role = writer_role,
        };
    }

    /// Run preflight's version-column report for one table, on its own connection.
    ///
    /// Short-lived by design: this fires on `CREATE TABLE`, which is rare, and holding an
    /// idle connection for it would be a worse trade than opening one. Every failure is
    /// swallowed with a line — the report is advice, and advice must not break CDC.
    fn reportEdgeWritability(self: *EventProcessor, table: []const u8) void {
        const conninfo = self.pg_config.connInfo(self.allocator, false) catch return;
        defer self.allocator.free(conninfo);

        const conn = c.PQconnectdb(conninfo.ptr);
        defer c.PQfinish(conn);
        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            log.debug("edge-writability report for '{s}' skipped: no connection", .{table});
            return;
        }

        Preflight.reportTable(
            self.allocator,
            conn.?,
            table,
            self.sync_rules,
            self.default_version_column,
            self.writer_role,
            self.writable,
        ) catch |err| {
            log.debug("edge-writability report for '{s}' skipped: {}", .{ table, err });
        };
    }

    /// Record one `schema_def` column's `oid`/`typtype` in the type registry.
    ///
    /// Silently ignores a column that carries neither — a database initialised before
    /// these fields existed simply leaves the registry empty for its types, and the
    /// decoder then refuses anything exotic rather than guessing. Failing to record is
    /// never fatal: the registry only ever widens what can be decoded.
    fn recordColumnType(self: *EventProcessor, col_val: std.json.Value) void {
        if (col_val != .object) return;
        const oid_v = col_val.object.get("oid") orelse return;
        const typtype_v = col_val.object.get("typtype") orelse return;
        if (oid_v != .integer or typtype_v != .string or typtype_v.string.len == 0) return;
        if (oid_v.integer <= 0) return;

        self.types.record(@intCast(oid_v.integer), typtype_v.string[0]) catch |err| {
            log.warn("could not record type OID {d}: {}", .{ oid_v.integer, err });
        };
    }

    /// A column arrived whose type the decoder cannot handle: suspend the table.
    ///
    /// The alternative was letting `error.TupleDecodeFailed` propagate out of main,
    /// which stops replication for *every* table over one exotic column — the blast
    /// radius `refused_tables` exists to avoid. This publishes the suspension in place
    /// of the mutation, so it keeps the stream's ordering and the client learns at the
    /// exact LSN where its data stops being complete.
    ///
    /// Only the first such event pays this cost: afterwards `refused.shouldDrop` filters
    /// the table before it ever reaches the decoder.
    fn suspendForUnsupportedType(
        self: *EventProcessor,
        arena: std.mem.Allocator,
        rel: pgoutput.RelationMessage,
        wal_end: u64,
    ) !u32 {
        log.err(
            "🔴 SUSPENDING '{s}': it has a column whose type the CDC decoder cannot handle (see the preceding line for which). Its events are dropped and its schema withheld until the column is changed to a supported type. Snapshots refuse it for the same reason.",
            .{rel.name},
        );

        self.refused.refuse(rel.name, .unsupported_column_type) catch |err| {
            log.err("🔴 Could not record refusal for '{s}': {}", .{ rel.name, err });
        };

        const msg_id = try std.fmt.allocPrint(arena, "schema-suspend-type-{s}-{d}", .{ rel.name, wal_end });
        return self.publishSuspension(
            arena,
            rel.name,
            RefusedTables.Reason.unsupported_column_type.wireName(),
            msg_id,
            rel.relation_id,
            wal_end,
        );
    }

    /// A row did not fit in the pre-allocated event buffer: suspend the table.
    ///
    /// Sized by `BASE_BUF` (log2 bytes per event), so this is a configuration verdict,
    /// not a schema one — but it is data-dependent and therefore not knowable when the
    /// bridge is deployed. The row that overflows may arrive years later, when someone
    /// pastes a large JSON document into a text column.
    ///
    /// The log is deliberately loud and arithmetic: the operator needs the value to set,
    /// not an adjective. It is also the line to alert on in Loki/Prometheus —
    /// `bridge_refused_tables` rises at the same moment.
    fn suspendForRowTooLarge(
        self: *EventProcessor,
        arena: std.mem.Allocator,
        rel: pgoutput.RelationMessage,
        wal_end: u64,
    ) !u32 {
        const buffer_bytes = self.batch_publisher.events[0].data_buffer.len;

        log.err(
            "🔴 SUSPENDING '{s}': a row does not fit in the {d} KB per-event buffer (BASE_BUF={d}).",
            .{ rel.name, buffer_bytes / 1024, std.math.log2_int(usize, buffer_bytes) },
        );
        log.err(
            "    Fix: restart with a larger BASE_BUF (each +1 doubles it, max 20 = 1 MB). The data slab is 2^BASE_BUF x RING_BUFFER_COUNT, so raise BASE_BUF and lower RING_BUFFER_COUNT together to hold memory steady.",
            .{},
        );
        log.err(
            "    If the row exceeds 1 MB it cannot be replicated at any setting — NATS's default max_payload is 1 MB. Move the oversized column out of the replicated table (store a reference, not the blob).",
            .{},
        );
        log.err(
            "    Every other table keeps replicating. This one resumes automatically once the bridge restarts with a buffer that fits, and its clients re-seed from a snapshot.",
            .{},
        );

        self.refused.refuse(rel.name, .row_too_large) catch |err| {
            log.err("🔴 Could not record refusal for '{s}': {}", .{ rel.name, err });
        };

        const msg_id = try std.fmt.allocPrint(arena, "schema-suspend-size-{s}-{d}", .{ rel.name, wal_end });
        return self.publishSuspension(
            arena,
            rel.name,
            RefusedTables.Reason.row_too_large.wireName(),
            msg_id,
            rel.relation_id,
            wal_end,
        );
    }

    /// Decode a WAL mutation, pack it into a ring-buffer slot, and return the slot index.
    /// The slot is NOT yet visible to the background publisher — call releaseSlotToQueue
    /// on .commit, or discardSlot on rollback / connection loss.
    ///
    /// arena_allocator: Temporary arena used only during tuple decoding.
    ///                  Safe to reset after this call returns.
    pub fn packMutationToSlot(
        self: *EventProcessor,
        arena_allocator: std.mem.Allocator,
        rel: pgoutput.RelationMessage,
        tuple_data: pgoutput.TupleData,
        old_tuple_data: ?pgoutput.TupleData,
        operation: []const u8,
        wal_end: u64,
    ) !u32 {
        // We will collect ALL columns (Old + New) in this list
        var all_columns: std.ArrayList(pgoutput.Column) = .empty;
        // No defer deinit needed because we use the arena_allocator which is reset per message

        // Track transition detection results
        var is_transition = false;
        var transition_column_name: ?[]const u8 = null;
        var old_value_str: ?[]const u8 = null;
        var new_value_str: ?[]const u8 = null;

        // 1. Handle "Old" Tuple (REPLICA IDENTITY FULL Updates) + Transition Detection
        if (old_tuple_data) |old_data| {
            // Decode the raw WAL bytes for the old tuple
            const old_decoded = pgoutput.decodeTuple(arena_allocator, old_data, rel.columns, self.types) catch |err| {
                if (err == error.UnsupportedColumnType) return self.suspendForUnsupportedType(arena_allocator, rel, wal_end);
                log.warn("⚠️ Failed to decode old tuple: {}", .{err});
                return error.TupleDecodeFailed;
            };

            // 2. Decode "Main" Tuple (The standard New values)
            const main_decoded = pgoutput.decodeTuple(arena_allocator, tuple_data, rel.columns, self.types) catch |err| {
                if (err == error.UnsupportedColumnType) return self.suspendForUnsupportedType(arena_allocator, rel, wal_end);
                log.warn("⚠️ Failed to decode tuple: {}", .{err});
                return error.TupleDecodeFailed;
            };

            // Check if this table has transition rules configured
            // If not, cols_to_watch will be null and we skip transition detection entirely (zero overhead)
            const cols_to_watch = self.transition_rules.get(rel.name);

            // Single pass: Add old.* columns AND detect transitions (if table has rules)
            for (old_decoded.items, 0..) |col, i| {
                // A. Always add old.* column to output
                const prefixed_name = try std.fmt.allocPrint(arena_allocator, "old.{s}", .{col.name});
                try all_columns.append(arena_allocator, .{
                    .name = prefixed_name,
                    .value = col.value,
                });

                // B. Transition detection (ONLY if table has rules AND this is an UPDATE)
                if (cols_to_watch) |watch_list| {
                    if (operation[0] == 'U' and !is_transition and i < main_decoded.items.len) {
                        // Check if THIS column is in the watch list for THIS table
                        const is_monitored = for (watch_list) |w_col| {
                            if (std.mem.eql(u8, col.name, w_col)) break true;
                        } else false;

                        if (is_monitored) {
                            const new_val = main_decoded.items[i].value;
                            if (!valuesEqual(col.value, new_val)) {
                                is_transition = true;
                                transition_column_name = col.name;

                                // Format old and new values for logging
                                old_value_str = try formatValueForLog(arena_allocator, col.value);
                                new_value_str = try formatValueForLog(arena_allocator, new_val);

                                log.debug("🔄 Transition detected: {s}.{s} changed from {s} to {s}", .{
                                    rel.name,
                                    col.name,
                                    old_value_str.?,
                                    new_value_str.?,
                                });
                            }
                        }
                    }
                }
            }

            // Add all new columns to output
            try all_columns.appendSlice(arena_allocator, main_decoded.items);
        } else {
            // No old tuple data - just process new/main tuple (INSERT or DELETE)
            const main_decoded = pgoutput.decodeTuple(arena_allocator, tuple_data, rel.columns, self.types) catch |err| {
                if (err == error.UnsupportedColumnType) return self.suspendForUnsupportedType(arena_allocator, rel, wal_end);
                log.warn("⚠️ Failed to decode tuple: {}", .{err});
                return error.TupleDecodeFailed;
            };
            try all_columns.appendSlice(arena_allocator, main_decoded.items);
        }

        // Extract ID value for logging (scan for "id" column in the combined list)
        var id_buf: [64]u8 = undefined;
        var tenant_buf: [64]u8 = undefined;
        const id_str = blk: {
            for (all_columns.items) |column| {
                // Quick rejection: check length first (avoid memcmp for wrong-length names)
                if (column.name.len == 2 and column.name[0] == 'i' and column.name[1] == 'd') {
                    break :blk switch (column.value) {
                        .int32 => |v| std.fmt.bufPrint(&id_buf, "{d}", .{v}) catch "?",
                        .int64 => |v| std.fmt.bufPrint(&id_buf, "{d}", .{v}) catch "?",
                        .text => |v| if (v.len <= id_buf.len) v else "?",
                        else => "?",
                    };
                }
            }
            break :blk null;
        };

        // Convert operation to lowercase for NATS subject
        const operation_lower = switch (operation[0]) {
            'I' => "insert", // INSERT
            'U' => "update", // UPDATE
            'D' => "delete", // DELETE
            else => unreachable, // Only these 3 operations exist in CDC
        };

        // Determine subject suffix based on transition detection
        // Only applies to UPDATE operations on tables with configured transition rules:
        //   - If transition detected: .transition
        //   - Otherwise: .data
        // For other operations or tables without rules: no suffix (backward compatible)
        const has_rules = self.transition_rules.contains(rel.name);
        const suffix = if (has_rules and operation[0] == 'U')
            if (is_transition) "transition" else "data"
        else
            null;

        // ── Tenant token ────────────────────────────────────────────────────────────
        //
        // Appended last so a subscriber can be granted `cdc.*.*.<tenant>` and receive
        // exactly its own rows — the same subject-authorization mechanism that already
        // scopes writes (§7.1), applied to reads.
        //
        // The value comes from **the row itself**, never from configuration: config says
        // *which column*, the row says *which tenant*. That is what makes a mis-routing
        // bug require misreading a value that is right there in the decoded tuple, rather
        // than a mismatched file.
        //
        // ⚠️ A table listed in TENANT_RULES whose tenant value is missing here is dropped,
        // not published unscoped. Preflight already refuses tables whose tenant is outside
        // the replica identity — the case that would make DELETEs arrive bare — so
        // reaching this branch means something changed underneath a running bridge. The
        // safe failure is silence: publishing to `cdc.<table>.<op>` with no tenant token
        // would deliver the row to a subject no tenant grant covers, but an operator
        // subscribed to `cdc.>` would receive it.
        const tenant: ?[]const u8 = blk: {
            const rule = self.tenant_rules.get(rel.name) orelse break :blk null;
            if (rule.len == 0) break :blk null;
            for (all_columns.items) |column| {
                if (!std.mem.eql(u8, column.name, rule[0])) continue;
                const raw: ?[]const u8 = switch (column.value) {
                    .text => |v| v,
                    .int32 => |v| std.fmt.bufPrint(&tenant_buf, "{d}", .{v}) catch null,
                    .int64 => |v| std.fmt.bufPrint(&tenant_buf, "{d}", .{v}) catch null,
                    else => null,
                };
                // ⚠️ The tenant is **row data**, so it can contain anything the schema
                // allows. A dot would split it into two tokens and the row would vanish
                // from its own tenant's feed (measured: `cdc.t.insert.evil.acme` is not
                // received by `cdc.*.*.acme`) while still existing in PostgreSQL — a
                // divergence with no event able to correct it. A `*` or `>` publishes as a
                // literal and matches broadly on the subscribe side.
                //
                // Quarantined rather than sanitised: rewriting the value would route the
                // row to a tenant that does not exist, which is worse than not routing it.
                break :blk if (raw) |v| (if (utils.isSubjectToken(v)) v else null) else null;
            }
            break :blk null;
        };

        // A configured table whose event carries no tenant value is **quarantined, not
        // dropped and not published bare**.
        //
        // Preflight refuses tables whose tenant sits outside the replica identity — the
        // case that makes DELETEs arrive without it — so reaching here means something
        // changed under a running bridge. Dropping would lose data silently; publishing to
        // `cdc.<table>.<op>` would put it on a three-token subject that no tenant grant
        // covers but a wildcard operator subscription does.
        //
        // `unrouted` keeps the four-token shape, so `cdc.*.*.<tenant>` cannot match it and
        // no tenant-scoped client receives it, while an operator can subscribe to
        // `cdc.*.*.unrouted` on purpose and see exactly what failed to route.
        const routed: ?[]const u8 = if (tenant) |t| t else if (self.tenant_rules.contains(rel.name)) blk: {
            const named: []const u8 = if (self.tenant_rules.get(rel.name)) |r|
                (if (r.len > 0) r[0] else "?")
            else
                "?";
            log.err(
                "🔴 Quarantining {s} on '{s}' to '.unrouted': TENANT_RULES names '{s}' but this event carries no value for it. Check that the replica identity still covers it.",
                .{ operation_lower, rel.name, named },
            );
            break :blk "unrouted";
        } else null;

        // Create NATS subject
        var subject_buf: [Config.Buffers.subject_buffer_size]u8 = undefined;
        // ⚠️ The tenant goes **immediately after the prefix**, not at the end:
        //
        //     cdc.<tenant>.<table>.<operation>[.<suffix>]
        //
        // so one grant — `cdc.acme.>` — covers every table, every operation, and every
        // suffix this subject may ever grow. With the tenant last it did not: batches
        // publish to `<subject>.batch` (batch_publisher.zig), so a single event landed on
        // `cdc.orders.insert.acme` (four tokens, matched by `cdc.*.*.acme`) while a
        // batched one landed on `cdc.orders.insert.acme.batch` (five, matched by nothing
        // the tenant holds). A tenant's rows would arrive under light load and silently
        // stop under heavy load — the worst possible shape for a bug, because the trigger
        // is *volume*.
        //
        // This is the same reason `mutation.<principal>.<table>.<operation>` puts the
        // principal first (§7.1): an identity token is only a useful grant if everything
        // that can vary sits after it.
        const subject = if (routed) |t| (if (suffix) |s|
            try std.fmt.bufPrintZ(
                &subject_buf,
                "{s}.{s}.{s}.{s}.{s}",
                .{ self.topology.subject_cdc_prefix, t, rel.name, operation_lower, s },
            )
        else
            try std.fmt.bufPrintZ(
                &subject_buf,
                "{s}.{s}.{s}.{s}",
                .{ self.topology.subject_cdc_prefix, t, rel.name, operation_lower },
            )) else if (suffix) |s|
            try std.fmt.bufPrintZ(
                &subject_buf,
                "{s}.{s}.{s}.{s}",
                .{ self.topology.subject_cdc_prefix, rel.name, operation_lower, s },
            )
        else
            try std.fmt.bufPrintZ(
                &subject_buf,
                "{s}.{s}.{s}",
                .{ self.topology.subject_cdc_prefix, rel.name, operation_lower },
            );

        // Generate message ID from WAL LSN for idempotent delivery
        var msg_id_buf: [Config.Buffers.msg_id_buffer_size]u8 = undefined;
        const msg_id = try std.fmt.bufPrint(
            &msg_id_buf,
            "{x}-{s}-{s}",
            .{ wal_end, rel.name, operation_lower },
        );

        // Pack decoded columns into the ring-buffer slab (zero heap allocation).
        // Data is deep-copied from the transient arena into the long-lived slot.
        const slot_idx = self.acquireAndFillSlot(
            subject,
            rel.name,
            operation,
            msg_id,
            rel.relation_id,
            all_columns,
            wal_end,
        ) catch |err| {
            // The suspension takes this event's place in the stream, so ordering holds
            // and the client learns at the exact LSN where its copy stops being current.
            if (err == error.RowTooLarge) return self.suspendForRowTooLarge(arena_allocator, rel, wal_end);
            return err;
        };

        // NOT counted here any more: this point is "packed into a ring-buffer slot",
        // which is a different event from "NATS accepted it". The counter moved to the
        // batch publisher's post-ack path — see the note there.

        if (is_transition and transition_column_name != null) {
            if (id_str) |id| {
                log.debug("{s} {s}.{s} id={s} [{s}: {s} → {s}] → {s}", .{
                    operation,
                    rel.namespace,
                    rel.name,
                    id,
                    transition_column_name.?,
                    old_value_str.?,
                    new_value_str.?,
                    subject,
                });
            } else {
                log.debug("{s} {s}.{s} [{s}: {s} → {s}] → {s}", .{
                    operation,
                    rel.namespace,
                    rel.name,
                    transition_column_name.?,
                    old_value_str.?,
                    new_value_str.?,
                    subject,
                });
            }
        } else {
            if (id_str) |id| {
                log.debug("{s} {s}.{s} id={s} → {s} [slot={d}]", .{ operation, rel.namespace, rel.name, id, subject, slot_idx });
            } else {
                log.debug("{s} {s}.{s} → {s} [slot={d}]", .{ operation, rel.namespace, rel.name, subject, slot_idx });
            }
        }

        return slot_idx;
    }

    /// Push a packed slot onto the pending_events queue, making it visible to the
    /// background publisher. Call this only after the matching .commit is confirmed.
    pub fn releaseSlotToQueue(self: *EventProcessor, slot_idx: u32) !void {
        var retry_count: usize = 0;
        const max_retries_before_fatal_check = Config.Retry.spins_before_fatal_check;
        const timer_start: u64 = nanoNow();
        const watchdog_timeout_ns = std.time.ns_per_s * 30;

        // ⚠️ Checked BEFORE the push, not after every push — signalling on every single
        // event turned the flush thread's idle wait from a latency problem into a
        // throughput one. It went from ~2-3k wake calls for a 2M-event burst to ~15.5k,
        // because it woke on nearly every individual push, drained whatever few events
        // had landed by then, and went straight back to waiting — thrashing instead of
        // letting the natural accumulation a busy producer provides do its job. Only the
        // transition from empty to non-empty needs to interrupt a genuinely idle flush
        // thread; once it's awake, its own inner drain loop picks up everything a fast
        // producer piles on without needing to be told again.
        const queue_was_empty = self.batch_publisher.pending_events.isEmpty();

        while (true) {
            self.batch_publisher.pending_events.push(slot_idx) catch |err| {
                if (err == error.QueueFull) {
                    retry_count += 1;

                    if (retry_count % max_retries_before_fatal_check == 0) {
                        if (self.batch_publisher.hasFatalError()) {
                            log.err("🔴 Flush thread fatal error during commit release", .{});
                            self.batch_publisher.events[slot_idx].reset();
                            self.batch_publisher.free_slots.push(slot_idx) catch {};
                            return error.PublisherFatalError;
                        }
                    }

                    if (nanoNow() - timer_start > watchdog_timeout_ns) {
                        log.err("🔴 FATAL: Flush thread blocked >30s during commit release", .{});
                        self.batch_publisher.events[slot_idx].reset();
                        self.batch_publisher.free_slots.push(slot_idx) catch {};
                        self.batch_publisher.fatal_error.store(true, .seq_cst);
                        return error.FlushThreadStalled;
                    }

                    if (retry_count == 1 or retry_count % 100 == 0) {
                        log.warn("⚠️ Pending queue full during commit release (retry #{d})", .{retry_count});
                    }
                    std.Thread.yield() catch {};
                    continue;
                }

                self.batch_publisher.events[slot_idx].reset();
                self.batch_publisher.free_slots.push(slot_idx) catch {};
                return err;
            };
            break;
        }

        // Only the empty→non-empty transition wakes the flush thread — see this
        // function's opening comment for why signalling on every push was worse, not
        // better.
        if (queue_was_empty) {
            self.batch_publisher.signalNewWork();
        }

        log.debug("Slot {d} released to publisher", .{slot_idx});
    }

    /// Return a packed slot to the free pool without publishing.
    /// Call on connection loss or when discarding an incomplete transaction.
    pub fn discardSlot(self: *EventProcessor, slot_idx: u32) void {
        self.batch_publisher.events[slot_idx].reset();
        self.batch_publisher.free_slots.push(slot_idx) catch {};
    }

    /// Acquire a free slot and pack event data into the ring-buffer slab.
    /// Returns the slot index. Does NOT push to pending_events.
    fn acquireAndFillSlot(
        self: *EventProcessor,
        subject: []const u8,
        table: []const u8,
        operation: []const u8,
        msg_id: []const u8,
        relation_id: u32,
        decoded_values: std.ArrayList(pgoutput.Column),
        lsn: u64,
    ) !u32 {
        log.debug("📥 Packing event into slot: {s} {s}", .{ operation, table });

        // Get a free slot from the ring buffer with backpressure retry + watchdog
        // Exit if flush thread has fatal error (NATS dead) to prevent infinite spinning
        var retry_count: usize = 0;
        const max_retries_before_fatal_check = Config.Retry.spins_before_fatal_check;
        const timer_start: u64 = nanoNow();
        const watchdog_timeout_ns = std.time.ns_per_s * 30; // 30 second hard timeout

        const slot_idx = while (true) {
            if (self.batch_publisher.free_slots.pop()) |idx| {
                break idx;
            }

            retry_count += 1;

            // 1. Check if flush thread has encountered a fatal error (e.g., NATS permanently down)
            if (retry_count % max_retries_before_fatal_check == 0) {
                if (self.batch_publisher.hasFatalError()) {
                    log.err("🔴 Flush thread has fatal error - aborting event processing", .{});
                    return error.PublisherFatalError;
                }
            }

            // 2. Watchdog: Hard timeout if flush thread is completely stuck (NATS client hung)
            {
                if (nanoNow() - timer_start > watchdog_timeout_ns) {
                    log.err("🔴 FATAL: Flush thread blocked for >30s without setting fatal_error", .{});
                    log.err("    This indicates NATS client library is hung. Forcing shutdown.", .{});
                    self.batch_publisher.fatal_error.store(true, .seq_cst);
                    return error.FlushThreadStalled;
                }
            }

            // Log warning on first retry, then periodically
            if (retry_count == 1 or retry_count % 100 == 0) {
                log.warn(
                    "⚠️ Ring buffer full! Applying backpressure (retry #{d}). Capacity: {d}",
                    .{ retry_count, self.batch_publisher.events.len },
                );
            }

            // Yield CPU to flush thread
            std.Thread.yield() catch {};
        };

        // Success - got a free slot!
        if (retry_count > 0) {
            log.info("Ring buffer slot available after {d} retries, resuming", .{retry_count});
        }

        // Get mutable reference to the pre-allocated event slot
        const event = &self.batch_publisher.events[slot_idx];

        // Reset event to clear any previous data
        event.reset();

        // Copy strings into inline buffers (zero heap allocation!)
        try event.setSubject(subject);
        try event.setTable(table);
        try event.setOperation(operation);
        try event.setMsgId(msg_id);

        // Set remaining fields
        event.relation_id = relation_id;
        event.lsn = lsn;

        // Pack columns into data_buffer (zero heap allocation!)
        for (decoded_values.items) |column| {
            event.addColumn(column.name, column.value) catch |err| {
                if (err == error.BufferOverflow) {
                    // This used to `@panic`, on the reasoning that stopping was the only
                    // way to avoid ACKing an LSN whose row never reached NATS. The
                    // invariant was right; the mechanism was not:
                    //
                    //   * the row sits *before* any later ACK, so a restart re-reads it
                    //     and panics again — under any supervisor that is a crash loop,
                    //     with every other table stopped too;
                    //   * "raise BASE_BUF" has a ceiling (2^20 = 1 MB, and NATS's
                    //     default max_payload is 1 MB), so for a genuinely large row it
                    //     is not a fix at all, just an infinite retry for the operator.
                    //
                    // Suspending the table instead is the same answer this bridge
                    // already gives for a keyless table or an undecodable column type:
                    // the client is told its data for THIS table is frozen at THIS LSN
                    // and everything else keeps replicating. ACKing past the row is then
                    // safe precisely because recovery is a snapshot re-seed, not WAL
                    // replay — nothing is silently lost, it is explicitly withheld.
                    log.err("🔴 Row too large for the event buffer on table '{s}' — column '{s}' did not fit", .{ table, column.name });
                    event.reset();
                    self.batch_publisher.free_slots.push(slot_idx) catch {};
                    return error.RowTooLarge;
                }
                // Other errors - return slot and propagate
                log.err("Failed to pack column '{s}': {}", .{ column.name, err });
                event.reset();
                self.batch_publisher.free_slots.push(slot_idx) catch {};
                return err;
            };
        }

        log.debug("Event packed into slot {d} ({d} columns)", .{ slot_idx, event.column_count });
        return @intCast(slot_idx);
    }

    /// Process a DDL event, query PostgreSQL for the new schema, perform SQLite transformation,
    /// and pack it into the ring buffer directly to the KV schemas subject.

    /// Does this table declare a tombstone column?
    ///
    /// Read from `SYNC_RULES`, the same source `mutation_listener` uses to decide whether a
    /// client's delete becomes an UPDATE — so the two cannot disagree about whether a
    /// table soft-deletes.
    ///
    /// Used to suppress the sweeper's reaps on the CDC path: a table that soft-deletes
    /// only ever produces a physical DELETE from the sweeper, and clients removed that row
    /// when the tombstone update arrived.
    ///
    /// ⚠️ Deliberately *not* "does the table have a column called deleted_at". A column
    /// nobody configured is an ordinary nullable timestamp, deletes on that table are
    /// physical and real, and suppressing them would strand rows in every replica.
    pub fn hasTombstone(self: *const EventProcessor, table: []const u8) bool {
        const cols = self.sync_rules.get(table) orelse return false;
        return cols.len > 1 and cols[1].len > 0;
    }

    /// Append `"version_column"` and `"tombstone_column"` to a schema descriptor.
    ///
    /// The bridge resolves both from `SYNC_RULES` (falling back to the global default
    /// version column), and until now kept the answer to itself — a descriptor carried
    /// `pk_columns` but never said *which* column a client must send as `version`, nor
    /// whether deletes on this table are soft. Both are required to write to the table
    /// (PROTOCOL.md §7.4), so leaving them out meant the write contract could not be
    /// derived from the wire at all: it lived in the bridge operator's env file.
    ///
    /// `version_column` is **null when the named column does not exist on the table**,
    /// which is the honest way to say "outbound-only" — the mutation path would reject
    /// every write with `NoVersionColumn`, and a client is better off knowing before it
    /// queues an edit than after.
    fn appendWriteContract(
        self: *EventProcessor,
        arena: std.mem.Allocator,
        json_str: *std.ArrayListUnmanaged(u8),
        table: []const u8,
        column_names: []const []const u8,
    ) !void {
        var version_name: []const u8 = self.default_version_column;
        var tombstone_name: ?[]const u8 = null;
        if (self.sync_rules.get(table)) |cols| {
            if (cols.len > 0) version_name = cols[0];
            if (cols.len > 1) tombstone_name = cols[1];
        }

        const has = struct {
            fn f(names: []const []const u8, want: []const u8) bool {
                for (names) |n| if (std.mem.eql(u8, n, want)) return true;
                return false;
            }
        }.f;

        // How long a client should wait before calling a write unconfirmed.
        //
        // Derived from the bridge's own retry budget, because the client cannot see it:
        // `mutation_max_deliver` attempts, one second apart, plus the CDC batch window,
        // plus slack for the round trip. Published so a client stops guessing — raising
        // `max_deliver` used to silently make every client's timeout wrong, and the
        // symptom was writes reported "unconfirmed" seconds before their echo arrived.
        //
        // ⚠️ A floor, not a promise: a genuinely transient failure that recovers on the
        // last attempt lands right at this number. A client should treat expiry as "not
        // yet confirmed", never as "refused" — the verdict channel is what says refused.
        const timeout_ms: u64 = @as(u64, @intCast(Config.Nats.mutation_max_deliver)) * 1000 +
            @as(u64, @intCast(Config.Batch.max_age_ms)) + 2000;
        try json_str.appendSlice(arena, try std.fmt.allocPrint(
            arena,
            ",\"mutation_timeout_ms\":{d}",
            .{timeout_ms},
        ));

        // ⚠️ The widest row this deployment can carry, published so a client can check
        // **before** sending rather than discovering by rejection. A mutation above this
        // is refused (`RowTooLargeToReplicate`) — the write would store a row CDC cannot
        // pack, which suspends the table for every client.
        //
        // Published rather than documented because it is a deployment setting: `BASE_BUF`
        // differs between installations and an operator may raise it. A client that
        // hardcoded the number would be wrong the day that happened, and a consumer author
        // has no way to read the bridge's environment.
        //
        // ⚠️ A ceiling on the *encoded row*, which is larger than the payload a client
        // sends — the CDC event carries every column plus its name. Treat it as an upper
        // bound to stay under, not a budget to fill.
        try json_str.appendSlice(arena, try std.fmt.allocPrint(
            arena,
            ",\"max_row_bytes\":{d}",
            .{self.event_buf_bytes},
        ));

        if (has(column_names, version_name)) {
            try json_str.appendSlice(arena, try std.fmt.allocPrint(
                arena,
                ",\"version_column\":\"{s}\"",
                .{version_name},
            ));
        } else {
            try json_str.appendSlice(arena, ",\"version_column\":null");
        }

        if (tombstone_name) |t| {
            if (has(column_names, t)) {
                try json_str.appendSlice(arena, try std.fmt.allocPrint(
                    arena,
                    ",\"tombstone_column\":\"{s}\"",
                    .{t},
                ));
            } else {
                try json_str.appendSlice(arena, ",\"tombstone_column\":null");
            }
        } else {
            try json_str.appendSlice(arena, ",\"tombstone_column\":null");
        }

        // `TENANT_RULES`: which column scopes this table's rows by tenant, or `null` for
        // a table every principal reads and writes the same content of (system tables,
        // genuinely public business tables alike). Published so a client can tell the two
        // apart per table instead of assuming its own tenant everywhere — before this, a
        // client's snapshot key for a tenant-agnostic table like `users` used its own
        // tenant unconditionally, so five principals ended up caching five redundant
        // snapshots of identical content instead of sharing one.
        if (self.tenant_rules.get(table)) |rule| {
            if (rule.len > 0 and has(column_names, rule[0])) {
                try json_str.appendSlice(arena, try std.fmt.allocPrint(
                    arena,
                    ",\"tenant_column\":\"{s}\"",
                    .{rule[0]},
                ));
            } else {
                try json_str.appendSlice(arena, ",\"tenant_column\":null");
            }
        } else {
            try json_str.appendSlice(arena, ",\"tenant_column\":null");
        }

        // NOTES.md §1.11: whether this table accepts edge writes at all, from the same
        // verdict preflight already computes (a usable version column, the writer's
        // actual grant, and not a database-allocated key — `logVerdict`/`reportTable`).
        // `null` means "not yet reported" (a table that raced this publish before its
        // own DDL event landed), not "not writable" — a client should not treat the two
        // the same, so the field is omitted rather than defaulted to `false`.
        if (self.writable.get(table)) |w| {
            try json_str.appendSlice(arena, try std.fmt.allocPrint(
                arena,
                ",\"writable\":{s}",
                .{if (w) "true" else "false"},
            ));
        } else {
            try json_str.appendSlice(arena, ",\"writable\":null");
        }
    }

    pub fn packDdlToSlot(
        self: *EventProcessor,
        arena: std.mem.Allocator,
        rel: pgoutput.RelationMessage,
        tuple_data: pgoutput.TupleData,
        wal_end: u64,
    ) !?u32 {
        // Decode zebridge_ddl_events tuple
        const decoded = try pgoutput.decodeTuple(arena, tuple_data, rel.columns, self.types);

        var target_table: ?[]const u8 = null;
        var command_tag: ?[]const u8 = null;
        var schema_def: ?[]const u8 = null;
        for (decoded.items) |col| {
            if (std.mem.eql(u8, col.name, "table_name")) {
                if (col.value == .text) target_table = col.value.text;
            } else if (std.mem.eql(u8, col.name, "command_tag")) {
                if (col.value == .text) command_tag = col.value.text;
            } else if (std.mem.eql(u8, col.name, "schema_def")) {
                // JSONB decodes to .jsonb; tolerate .text in case the column type
                // is ever changed to json/text.
                schema_def = switch (col.value) {
                    .jsonb => |j| j,
                    .text => |t| t,
                    else => null,
                };
            }
        }

        if (target_table == null) return null;

        // Trim optional quotes or public schema prefix
        var clean_table = target_table.?;
        if (std.mem.startsWith(u8, clean_table, "public.")) {
            clean_table = clean_table[7..];
        }

        log.info("🔍 Processing DDL event for table '{s}' ({s})", .{
            clean_table,
            command_tag orelse "UNKNOWN",
        });

        // DROP TABLE carries no schema — the object is gone. Publish a tombstone
        // rather than deleting the KV key: a client that reconnects after the drop
        // must still learn the table went away so it can drop its local replica.
        // A vanished key is indistinguishable from "never seen".
        if (command_tag) |tag| {
            if (std.mem.eql(u8, tag, "DROP TABLE")) {
                const tombstone = try std.fmt.allocPrint(
                    arena,
                    "{{\"table\":\"{s}\",\"dropped\":true,\"lsn\":{d}}}",
                    .{ clean_table, wal_end },
                );
                const kv_subject = try Topology.render(arena, self.topology.kv_schemas_subject_pattern, &.{.{ .name = "table", .value = clean_table }}, null);
                const msg_id = try std.fmt.allocPrint(arena, "schema-drop-{s}-{d}", .{ clean_table, wal_end });

                var drop_cols: std.ArrayList(pgoutput.Column) = .empty;
                try drop_cols.append(arena, .{ .name = "schema", .value = .{ .text = tombstone } });

                const slot_idx = try self.acquireAndFillSlot(
                    kv_subject,
                    clean_table,
                    "SCHEMA",
                    msg_id,
                    rel.relation_id,
                    drop_cols,
                    wal_end,
                );

                // A dropped table cannot be refused: there is nothing left to refuse.
                // Without this the registry keeps announcing a table that no longer
                // exists — `logStatus` repeats it every metrics interval forever, and
                // `shouldDrop` would silently drop events for a *new* table that later
                // reuses the name. Every other refusal lifts through a DDL event
                // carrying a fixed schema; a DROP carries none, so it must lift here.
                self.refused.clear(clean_table);

                log.info("🗑️  DROP TABLE tombstone published for '{s}'", .{clean_table});
                return slot_idx;
            }
        }

        // The schema travels in the WAL row itself, captured by the event trigger
        // inside the DDL transaction. Deliberately NOT re-queried here: querying at
        // processing time reads the catalog as of "now", not as of this LSN, which
        // mis-orders bursts of DDL and reports today's schema when replaying old WAL
        // after a restart.
        const raw = schema_def orelse {
            log.warn("⚠️ DDL event for '{s}' has no schema_def — is the event trigger current? Skipping.", .{clean_table});
            return null;
        };

        const parsed = std.json.parseFromSlice(std.json.Value, arena, raw, .{}) catch |err| {
            log.err("Failed to parse schema_def for '{s}': {}", .{ clean_table, err });
            return null;
        };
        defer parsed.deinit();

        if (parsed.value != .object) {
            log.warn("⚠️ schema_def for '{s}' is not an object — skipping", .{clean_table});
            return null;
        }

        const cols_val = parsed.value.object.get("columns") orelse {
            log.warn("⚠️ schema_def for '{s}' has no columns — skipping", .{clean_table});
            return null;
        };
        if (cols_val != .array or cols_val.array.items.len == 0) {
            log.warn("⚠️ schema_def for '{s}' has an empty column list — skipping", .{clean_table});
            return null;
        }
        const columns = cols_val.array.items;

        // Re-emit as `{ pg: {...}, sqlite: {...}, pk }`. The Postgres shape is copied
        // through as captured; the SQLite dialect is derived here so the mapping stays
        // in one testable place (schema_mapper.zig) rather than in plpgsql.
        var json_str: std.ArrayList(u8) = .empty;

        // Name the table in the payload as well as the KV key. The key stays
        // authoritative, but every other payload in the protocol is self-describing
        // (CDC events, snapshot chunks, and this schema's own tombstone all carry
        // "table") — a schema value that does not name itself is the odd one out, and
        // is useless once it travels without its key (logs, caches, forwarding).
        try json_str.appendSlice(arena, try std.fmt.allocPrint(
            arena,
            "{{\"table\":\"{s}\",\"pg\":{{\"columns\":[",
            .{clean_table},
        ));
        // Column names, kept so the write contract can say whether the configured
        // version/tombstone columns actually exist on this table.
        var column_names: std.ArrayListUnmanaged([]const u8) = .empty;
        for (columns, 0..) |col_val, i| {
            const name = if (col_val == .object) col_val.object.get("name") else null;
            const ty = if (col_val == .object) col_val.object.get("type") else null;
            if (name == null or ty == null or name.? != .string or ty.? != .string) continue;

            // Consume the bridge-facing facts here and do NOT forward them: the KV
            // value is the client contract, and a client has no use for an OID.
            self.recordColumnType(col_val);

            // `required` = NOT NULL with no DEFAULT: the columns a write must carry.
            // Forwarded, not derived, so the descriptor and PostgreSQL cannot disagree.
            // Absent on a database whose trigger predates the field, which reads as
            // "not required" — the same answer the descriptor gave before it existed.
            const required = if (col_val == .object) col_val.object.get("required") else null;
            const is_required = required != null and required.? == .bool and required.?.bool;

            if (i > 0) try json_str.appendSlice(arena, ",");
            try column_names.append(arena, name.?.string);
            try json_str.appendSlice(arena, try std.fmt.allocPrint(
                arena,
                "{{\"name\":\"{s}\",\"type\":\"{s}\",\"required\":{s}}}",
                .{ name.?.string, ty.?.string, if (is_required) "true" else "false" },
            ));
        }

        try json_str.appendSlice(arena, "]},\"sqlite\":{\"columns\":[");
        for (columns, 0..) |col_val, i| {
            const name = if (col_val == .object) col_val.object.get("name") else null;
            const ty = if (col_val == .object) col_val.object.get("type") else null;
            if (name == null or ty == null or name.? != .string or ty.? != .string) continue;

            if (i > 0) try json_str.appendSlice(arena, ",");
            try json_str.appendSlice(arena, try std.fmt.allocPrint(
                arena,
                "{{\"name\":\"{s}\",\"type\":\"{s}\"}}",
                .{ name.?.string, schema_mapper.pgToSqliteType(ty.?.string) },
            ));
        }
        try json_str.appendSlice(arena, "] ");

        // Validate here, not only at boot. preflight.zig checks the publication at
        // startup, but a table created *while the bridge runs* is never seen by it —
        // and that is the common case, since migrations run after the bridge is up.
        // The DDL event already carries the schema, so the same defect is detectable
        // at exactly the moment it is introduced.
        const identity: []const u8 = if (parsed.value.object.get("replica_identity")) |ri|
            (if (ri == .string) ri.string else "unknown")
        else
            "unknown";

        // `pk` is the full key, in key order: null/absent means genuinely no primary
        // key; one element is fully supported; several is a composite key.
        const pk_cols: []const std.json.Value = blk: {
            const v = parsed.value.object.get("pk") orelse break :blk &.{};
            if (v != .array) break :blk &.{};
            break :blk v.array.items;
        };

        // Refuse tables with no primary key. Not a warning: without a key, DELETE can
        // only be expressed as a full-row match, which removes *every* duplicate where
        // PostgreSQL removed one — the replica diverges destructively and silently.
        // A composite key has none of that ambiguity and is fully supported, snapshots
        // included — pagination compares the whole key as a row value.
        if (pk_cols.len == 0) {
            log.err(
                "🔴 REFUSING '{s}': no primary key (REPLICA IDENTITY {s}). Rows cannot be identified, so DELETE would match on every column and could remove more rows than PostgreSQL did. No schema will be published and its CDC events will be dropped. Fix with a migration adding a primary key.",
                .{ clean_table, identity },
            );
            self.refused.refuse(clean_table, .no_primary_key) catch |err| {
                // The registry is full: we cannot track the drop, so say so and keep
                // going rather than losing the DDL event entirely.
                log.err("🔴 Could not record refusal for '{s}': {}", .{ clean_table, err });
            };

            const msg_id = try std.fmt.allocPrint(arena, "schema-suspend-{s}-{d}", .{ clean_table, wal_end });
            return try self.publishSuspension(
                arena,
                clean_table,
                RefusedTables.Reason.no_primary_key.wireName(),
                msg_id,
                rel.relation_id,
                wal_end,
            );
        }

        // Refuse a table whose CDC subject has nowhere to go — neither tenant-scoped
        // nor in `topology.json`'s `public_tables`. Checked *before* attempting to
        // publish anything for it: found live, a publish to an unrouted subject does
        // not fail fast, it blocks for the full publish timeout, retries the identical
        // dead end, and enough of that exhausts the bridge's retry budget and
        // self-terminates. Refusing here turns a bridge-wide outage into a named,
        // immediate, per-table refusal instead.
        //
        // ⚠️ `zebridge_user_tenants` is exempt, not `isInternalTable` (it still needs
        // every other check — no-PK, version columns, writability). Its row changes
        // never touch `cdc.<table>.<op>` at all: they are transformed into
        // `$KV.tenants.<principal>` by a dedicated path elsewhere in this file, which
        // has nothing to do with the CDC stream subject filter this check is about —
        // deliberately, so the tenant roster is never broadcast (NOTES.md §…, the
        // roster-disclosure leak `$KV.tenants` exists to avoid).
        if (!std.mem.eql(u8, clean_table, "zebridge_user_tenants") and
            !self.topology.isCdcRoutable(clean_table, self.tenant_rules.contains(clean_table)))
        {
            log.err(
                "🔴 REFUSING '{s}': not tenant-scoped and not in topology.json's public_tables — its CDC subject matches no stream. Publishing would block on every write. Add it to public_tables and CDC_PUBLIC's subjects, or set TENANT_RULES for it.",
                .{clean_table},
            );
            self.refused.refuse(clean_table, .no_cdc_subject) catch |err| {
                log.err("🔴 Could not record refusal for '{s}': {}", .{ clean_table, err });
            };

            const msg_id = try std.fmt.allocPrint(arena, "schema-suspend-{s}-{d}", .{ clean_table, wal_end });
            return try self.publishSuspension(
                arena,
                clean_table,
                RefusedTables.Reason.no_cdc_subject.wireName(),
                msg_id,
                rel.relation_id,
                wal_end,
            );
        }

        // Reaching here means the table has a key. If it was refused before, this DDL
        // event is the migration that fixed it — resume without a restart.
        self.refused.clear(clean_table);

        if (self.transition_rules.contains(clean_table) and !std.mem.eql(u8, identity, "full")) {
            log.warn(
                "⚠️  '{s}' has TRANSITION_RULES but REPLICA IDENTITY {s}: no old tuple is sent for non-key updates, so transitions can NEVER fire",
                .{ clean_table, identity },
            );
        }

        // `pk` stays a single string for the common case (existing clients read it
        // directly), and is null for a composite key — a client cannot identify rows
        // from one column of a two-column key, so claiming one would be worse than
        // admitting none. `pk_columns` always carries the whole key, so a client that
        // understands composites has what it needs.
        if (pk_cols.len == 1 and pk_cols[0] == .string) {
            try json_str.appendSlice(arena, try std.fmt.allocPrint(
                arena,
                ",\"pk\":\"{s}\"",
                .{pk_cols[0].string},
            ));
        } else {
            try json_str.appendSlice(arena, ",\"pk\":null");
        }

        try json_str.appendSlice(arena, ",\"pk_columns\":[");
        for (pk_cols, 0..) |col, i| {
            if (col != .string) continue;
            if (i > 0) try json_str.appendSlice(arena, ",");
            try json_str.appendSlice(arena, try std.fmt.allocPrint(arena, "\"{s}\"", .{col.string}));
        }
        try json_str.appendSlice(arena, "]");

        // Close the sqlite object, then attach the LSN at the root. Clients need the
        // ordering key: KV and CDC are independent subscriptions, so without it a
        // consumer cannot tell whether a CDC event precedes or follows this schema
        // and the bridge's ordering guarantee stops at the wire.
        try json_str.appendSlice(arena, "}");
        try json_str.appendSlice(arena, try std.fmt.allocPrint(
            arena,
            ",\"replica_identity\":\"{s}\"",
            .{identity},
        ));
        try self.appendWriteContract(arena, &json_str, clean_table, column_names.items);
        try json_str.appendSlice(arena, try std.fmt.allocPrint(arena, ",\"lsn\":{d}}}", .{wal_end}));

        // Create subject: $KV.schemas.{table}
        const kv_subject = try Topology.render(arena, self.topology.kv_schemas_subject_pattern, &.{.{ .name = "table", .value = clean_table }}, null);
        const msg_id = try std.fmt.allocPrint(arena, "schema-{s}-{d}", .{clean_table, wal_end});

        var dummy_cols: std.ArrayList(pgoutput.Column) = .empty;
        try dummy_cols.append(arena, .{ .name = "schema", .value = .{ .text = json_str.items } });

        // Acquire slot and pack!
        const slot_idx = try self.acquireAndFillSlot(
            kv_subject,
            clean_table,
            "SCHEMA",
            msg_id,
            rel.relation_id,
            dummy_cols,
            wal_end,
        );

        log.info("✅ DDL schema mapped and published to KV for '{s}'", .{clean_table});

        // The edge-writability report, for a table that appeared after boot. Preflight
        // runs once and only sees what the publication held then, so without this the
        // ✍️/⚠️ lines — the sole warning that the version column is naive, coarse or
        // nullable — were shown only to whoever restarted the bridge afterwards.
        // Best-effort: a failed report must never cost the DDL event.
        self.reportEdgeWritability(clean_table);
        return slot_idx;
    }


    /// Publish a suspension notice for a refused table.
    ///
    /// Shaped like the DROP TABLE tombstone, not like a schema: it carries no columns,
    /// because a client must not build a table from it. But it is deliberately a
    /// distinct state from `dropped` — the table still exists upstream and will come
    /// back the moment someone adds a primary key, so a client must not destroy local
    /// data over it the way it does for a tombstone.
    ///
    /// Withholding the schema silently was not enough: a client that already holds rows
    /// has no way to learn its table went stale, and a fresh client would read the last
    /// good schema, build the table, and wait forever for rows that are being dropped.
    fn publishSuspension(
        self: *EventProcessor,
        arena: std.mem.Allocator,
        clean_table: []const u8,
        reason: []const u8,
        msg_id: []const u8,
        relation_id: u32,
        lsn: u64,
    ) !u32 {
        // ⚠️ `"writable":false`, always — not read from the registry. A suspended
        // table can never be safely written to regardless of what preflight concluded
        // about its grant: CDC drops every event for it at the source, so an optimistic
        // write would never be confirmed or corrected (§1.6c/§1.6d). The client-side
        // `suspended` gate already blocks this independently; this field exists so a
        // client checking `writable` alone — without also checking `suspended` — does
        // not read a merely-absent field as "unknown, might be writable".
        const payload = try std.fmt.allocPrint(
            arena,
            "{{\"table\":\"{s}\",\"suspended\":true,\"reason\":\"{s}\",\"lsn\":{d},\"writable\":false}}",
            .{ clean_table, reason, lsn },
        );
        const kv_subject = try Topology.render(arena, self.topology.kv_schemas_subject_pattern, &.{.{ .name = "table", .value = clean_table }}, null);

        var cols: std.ArrayList(pgoutput.Column) = .empty;
        try cols.append(arena, .{ .name = "schema", .value = .{ .text = payload } });

        return self.acquireAndFillSlot(kv_subject, clean_table, "SCHEMA", msg_id, relation_id, cols, lsn);
    }

    /// `zebridge_user_tenants` (principal, tenant_id) → `$KV.tenants.<principal>`, a bare
    /// tenant string, never a JSON payload. Mirrors `packDdlToSlot`, with two
    /// differences: the value is a plain string a client reads directly (no JSON
    /// wrapping — `$KV.tenants` is not a schema-shaped bucket), and there is no
    /// tombstone case, because a DELETE from this table is not handled here at all
    /// (see the caller in bridge.zig) — a revoked mapping leaves a stale KV entry
    /// rather than a fresh one, a known, deliberately deferred gap.
    ///
    /// Called only for INSERT and UPDATE (the caller passes the NEW row's tuple data
    /// either way) — both mean "this principal's tenant is now this value", which is
    /// exactly what the KV bucket should hold, so one function serves both operations.
    ///
    /// ⚠️ Not validated for NATS subject legality the way `zebridge_guard_tenant()`
    /// validates an application table's tenant column on write. That trigger is
    /// attached per tenant-scoped *application* table, not to `zebridge_user_tenants`
    /// itself — a malformed `tenant_id` here (a dot, a space) would currently reach
    /// this KV bucket unchecked. Not a routing hazard the way it would be for CDC (this
    /// bucket is a lookup, not a subject a row is published on), but still worth
    /// guarding if this table ever gets a DBA-facing insert path less careful than a
    /// manual `INSERT`.
    pub fn packTenantToKvSlot(
        self: *EventProcessor,
        arena: std.mem.Allocator,
        rel: pgoutput.RelationMessage,
        tuple_data: pgoutput.TupleData,
        wal_end: u64,
    ) !?u32 {
        const decoded = try pgoutput.decodeTuple(arena, tuple_data, rel.columns, self.types);

        var principal: ?[]const u8 = null;
        var tenant_id: ?[]const u8 = null;
        for (decoded.items) |col| {
            if (std.mem.eql(u8, col.name, "principal")) {
                if (col.value == .text) principal = col.value.text;
            } else if (std.mem.eql(u8, col.name, "tenant_id")) {
                if (col.value == .text) tenant_id = col.value.text;
            }
        }

        if (principal == null or tenant_id == null) return null;

        const kv_subject = try Topology.render(
            arena,
            self.topology.kv_tenants_subject_pattern,
            &.{.{ .name = "principal", .value = principal.? }},
            null,
        );
        const msg_id = try std.fmt.allocPrint(arena, "tenant-{s}-{d}", .{ principal.?, wal_end });

        var cols: std.ArrayList(pgoutput.Column) = .empty;
        try cols.append(arena, .{ .name = "tenant_id", .value = .{ .text = tenant_id.? } });

        const slot_idx = try self.acquireAndFillSlot(
            kv_subject,
            principal.?,
            "TENANT",
            msg_id,
            rel.relation_id,
            cols,
            wal_end,
        );

        log.info("✅ tenant mapping published to KV: '{s}' → '{s}'", .{ principal.?, tenant_id.? });
        return slot_idx;
    }

    /// One-shot boot backfill of `$KV.tenants.*` from every row already in
    /// `zebridge_user_tenants` — replication only carries *changes*, so a mapping
    /// inserted before this boot needs its own pass, mirroring `publishBootSchemas`.
    ///
    /// Guarded on the table existing: a readonly-profile deployment (`init.core` only,
    /// no `init.write`) never creates `zebridge_user_tenants`, and that is a valid
    /// shape, not a misconfiguration — skip rather than fail the whole boot on
    /// something this deployment does not need.
    pub fn publishBootTenants(self: *EventProcessor, allocator: std.mem.Allocator) !void {
        var arena_state = std.heap.ArenaAllocator.init(allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var standard_pg_config = self.pg_config.*;
        standard_pg_config.replication = false;

        const conn = pg_conn.connect(arena, standard_pg_config) catch |err| {
            log.err("Failed to connect to Postgres for boot tenant fetch: {}", .{err});
            return error.PgConnectionFailed;
        };
        defer c.PQfinish(conn);

        const exists_res = c.PQexec(conn, "SELECT to_regclass('public.zebridge_user_tenants') IS NOT NULL");
        defer c.PQclear(exists_res);
        if (c.PQresultStatus(exists_res) != c.PGRES_TUPLES_OK or c.PQntuples(exists_res) == 0) {
            log.warn("Could not check for zebridge_user_tenants; skipping boot tenant backfill", .{});
            return;
        }
        if (!std.mem.eql(u8, std.mem.span(c.PQgetvalue(exists_res, 0, 0)), "t")) {
            log.info("zebridge_user_tenants not present — skipping boot tenant backfill (readonly profile, or write half not installed)", .{});
            return;
        }

        const result = c.PQexec(conn, "SELECT principal, tenant_id FROM public.zebridge_user_tenants ORDER BY principal");
        defer c.PQclear(result);
        if (c.PQresultStatus(result) != c.PGRES_TUPLES_OK) {
            log.warn("⚠️ Failed to query zebridge_user_tenants for boot backfill: {s}", .{c.PQerrorMessage(conn)});
            return;
        }

        const num_rows: i32 = c.PQntuples(result);
        if (num_rows == 0) {
            log.info("zebridge_user_tenants is empty — nothing to backfill", .{});
            return;
        }

        var r: i32 = 0;
        while (r < num_rows) : (r += 1) {
            const principal = std.mem.span(c.PQgetvalue(result, r, 0));
            const tenant_id = std.mem.span(c.PQgetvalue(result, r, 1));

            const kv_subject = try Topology.render(
                arena,
                self.topology.kv_tenants_subject_pattern,
                &.{.{ .name = "principal", .value = principal }},
                null,
            );
            const msg_id = try std.fmt.allocPrint(arena, "tenant-boot-{s}", .{principal});

            var cols: std.ArrayList(pgoutput.Column) = .empty;
            try cols.append(arena, .{ .name = "tenant_id", .value = .{ .text = tenant_id } });

            const slot_idx = try self.acquireAndFillSlot(kv_subject, principal, "TENANT", msg_id, 0, cols, 0);
            try self.releaseSlotToQueue(slot_idx);
        }

        log.info("✅ Boot tenant backfill published to KV for {d} principal(s)", .{num_rows});
    }

    /// Publish schemas for all monitored tables on boot.
    pub fn publishBootSchemas(
        self: *EventProcessor,
        allocator: std.mem.Allocator,
        monitored_tables: []const []const u8,
    ) !void {
        if (monitored_tables.len == 0) return;
        
        var arena_state = std.heap.ArenaAllocator.init(allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        
        log.info("📋 Extracting and publishing schemas for {d} monitored tables on boot...", .{monitored_tables.len});
        
        var standard_pg_config = self.pg_config.*;
        standard_pg_config.replication = false;
        
        const conn = pg_conn.connect(arena, standard_pg_config) catch |err| {
            log.err("Failed to connect to Postgres for boot schema fetch: {}", .{err});
            return error.PgConnectionFailed;
        };
        defer c.PQfinish(conn);

        // One WAL position for the whole boot pass, read before any schema is sent.
        // Taking it once (rather than per table) keeps every boot schema on a single
        // consistent watermark, and taking it *before* publishing means the stamp can
        // only be older than reality — never claim to describe WAL we have not seen.
        const boot_lsn = blk: {
            const lsn_res = c.PQexec(conn, "SELECT pg_current_wal_lsn()::text");
            defer c.PQclear(lsn_res);
            if (c.PQresultStatus(lsn_res) != c.PGRES_TUPLES_OK or c.PQntuples(lsn_res) == 0) {
                log.warn("Could not read current WAL LSN for boot schemas; stamping 0", .{});
                break :blk 0;
            }
            break :blk parsePgLsnText(std.mem.span(c.PQgetvalue(lsn_res, 0, 0)));
        };

        for (monitored_tables) |target_table| {
            var clean_table = target_table;
            if (std.mem.startsWith(u8, clean_table, "public.")) {
                clean_table = clean_table[7..];
            }

            if (isInternalTable(clean_table)) {
                log.debug("Skipping boot schema for internal table '{s}'", .{clean_table});
                continue;
            }

            // preflight refused this table before any thread started, and a refusal
            // means its schema is withheld — publishing it here would hand clients a
            // table they can never receive rows for, and would contradict the refusal
            // message. This pass builds its own schema JSON and so needs its own guard.
            if (self.refused.isRefused(clean_table)) {
                log.warn("⚠️  Withholding boot schema for refused table '{s}' — publishing suspension", .{clean_table});
                // No column or PK queries: we are not describing a shape, only saying
                // the table is not replicating.
                const msg_id = try std.fmt.allocPrint(arena, "schema-suspend-boot-{s}", .{clean_table});
                const slot_idx = try self.publishSuspension(
                    arena,
                    clean_table,
                    RefusedTables.Reason.no_primary_key.wireName(),
                    msg_id,
                    0,
                    boot_lsn,
                );
                try self.releaseSlotToQueue(slot_idx);
                continue;
            }
            
            const query = try utils.allocPrintZ(
                arena,
                \\SELECT a.attname,
                \\       format_type(a.atttypid, a.atttypmod) AS data_type,
                \\       a.atttypid,
                \\       t.typtype,
                \\       (a.attnotnull AND NOT a.atthasdef) AS required
                \\FROM pg_attribute a
                \\JOIN pg_type t ON t.oid = a.atttypid
                \\WHERE a.attrelid = '"{s}"."{s}"'::regclass
                \\  AND a.attnum > 0
                \\  AND NOT a.attisdropped
                \\ORDER BY a.attnum;
            ,
                .{ "public", clean_table },
            );
            
            const result = c.PQexec(conn, query.ptr);
            defer c.PQclear(result);
            
            if (c.PQresultStatus(result) != c.PGRES_TUPLES_OK) {
                log.warn("⚠️ Failed to query schema for {s}: {s}", .{clean_table, c.PQerrorMessage(conn)});
                continue;
            }
            
            const num_rows: i32 = c.PQntuples(result);
            if (num_rows == 0) {
                log.warn("⚠️ No schema found for table {s}", .{clean_table});
                continue;
            }
            
            var json_str: std.ArrayList(u8) = .empty;
            try json_str.appendSlice(arena, try std.fmt.allocPrint(
                arena,
                "{{\"table\":\"{s}\",\"pg\":{{\"columns\":[",
                .{clean_table},
            ));
            
            var column_names: std.ArrayListUnmanaged([]const u8) = .empty;
            var r: i32 = 0;
            while (r < num_rows) : (r += 1) {
                if (r > 0) try json_str.appendSlice(arena, ",");
                const col_name = std.mem.span(c.PQgetvalue(result, r, 0));
                const data_type = std.mem.span(c.PQgetvalue(result, r, 1));

                // Tables that have had no DDL since startup never produce a DDL event,
                // so this pass is the only chance to learn their exotic types. Without
                // it the decoder would refuse a perfectly good enum column until the
                // next ALTER.
                const oid_text = std.mem.span(c.PQgetvalue(result, r, 2));
                const typtype_text = std.mem.span(c.PQgetvalue(result, r, 3));
                if (std.fmt.parseInt(u32, oid_text, 10)) |oid| {
                    if (typtype_text.len > 0) {
                        self.types.record(oid, typtype_text[0]) catch |err| {
                            log.warn("could not record type OID {d}: {}", .{ oid, err });
                        };
                    }
                } else |_| {}

                try column_names.append(arena, col_name);
                // `attnotnull AND NOT atthasdef` — the catalog form of "NOT NULL with no
                // DEFAULT", matching what the DDL trigger computes from information_schema.
                const required_txt = std.mem.span(c.PQgetvalue(result, r, 4));
                const is_required = std.mem.eql(u8, required_txt, "t");
                const col_json = try std.fmt.allocPrint(
                    arena,
                    "{{\"name\":\"{s}\",\"type\":\"{s}\",\"required\":{s}}}",
                    .{ col_name, data_type, if (is_required) "true" else "false" },
                );
                try json_str.appendSlice(arena, col_json);
            }
            
            try json_str.appendSlice(arena, "]},\"sqlite\":{\"columns\":[");
            
            r = 0;
            while (r < num_rows) : (r += 1) {
                if (r > 0) try json_str.appendSlice(arena, ",");
                const col_name = std.mem.span(c.PQgetvalue(result, r, 0));
                const data_type = std.mem.span(c.PQgetvalue(result, r, 1));
                
                const col_json = try std.fmt.allocPrint(arena, "{{\"name\":\"{s}\",\"type\":\"{s}\"}}", .{col_name, schema_mapper.pgToSqliteType(data_type)});
                try json_str.appendSlice(arena, col_json);
            }
            try json_str.appendSlice(arena, "] ");
            
            const pk_query = try utils.allocPrintZ(
                arena,
                \\SELECT a.attname
                \\FROM pg_index i
                \\JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                \\WHERE i.indrelid = '"{s}"."{s}"'::regclass
                \\  AND i.indisprimary
                \\  AND array_length(i.indkey, 1) = 1;
            ,
                .{ "public", clean_table },
            );
            const pk_result = c.PQexec(conn, pk_query.ptr);
            defer c.PQclear(pk_result);
            
            // Emit pk unconditionally — null when there is no single-column primary
            // key. See the DDL path for why omission is not an acceptable encoding.
            if (c.PQresultStatus(pk_result) == c.PGRES_TUPLES_OK and c.PQntuples(pk_result) > 0) {
                const pk_name = std.mem.span(c.PQgetvalue(pk_result, 0, 0));
                const pk_json = try std.fmt.allocPrint(arena, ",\"pk\":\"{s}\"", .{pk_name});
                try json_str.appendSlice(arena, pk_json);
            } else {
                try json_str.appendSlice(arena, ",\"pk\":null");
            }
            // Replica identity, so a boot schema carries the same fields as a
            // DDL-driven one. A client must not have to know which path produced its
            // schema in order to know what UPDATE/DELETE will contain.
            const ri_query = try utils.allocPrintZ(
                arena,
                \\SELECT CASE relreplident
                \\         WHEN 'd' THEN 'default' WHEN 'f' THEN 'full'
                \\         WHEN 'n' THEN 'nothing' WHEN 'i' THEN 'index'
                \\       END
                \\FROM pg_class WHERE oid = '"{s}"."{s}"'::regclass;
            ,
                .{ "public", clean_table },
            );
            const ri_result = c.PQexec(conn, ri_query.ptr);
            defer c.PQclear(ri_result);
            const identity: []const u8 = if (c.PQresultStatus(ri_result) == c.PGRES_TUPLES_OK and
                c.PQntuples(ri_result) > 0)
                std.mem.span(c.PQgetvalue(ri_result, 0, 0))
            else
                "unknown";

            // Boot schemas describe tables that emitted no DDL event, so there is no
            // wal_end to inherit. Stamp the current WAL position: the schema is valid
            // from here, which lets a client compare it against CDC lsns the same way
            // it would a DDL-driven schema.
            try json_str.appendSlice(arena, "}");
            try json_str.appendSlice(arena, try std.fmt.allocPrint(
                arena,
                ",\"replica_identity\":\"{s}\"",
                .{identity},
            ));
            try self.appendWriteContract(arena, &json_str, clean_table, column_names.items);
            try json_str.appendSlice(arena, try std.fmt.allocPrint(arena, ",\"lsn\":{d}}}", .{boot_lsn}));

            const kv_subject = try Topology.render(arena, self.topology.kv_schemas_subject_pattern, &.{.{ .name = "table", .value = clean_table }}, null);
            const msg_id = try std.fmt.allocPrint(arena, "schema-boot-{s}", .{clean_table});
            
            var dummy_cols: std.ArrayList(pgoutput.Column) = .empty;
            try dummy_cols.append(arena, .{ .name = "schema", .value = .{ .text = json_str.items } });
            
            const slot_idx = try self.acquireAndFillSlot(
                kv_subject,
                clean_table,
                "SCHEMA",
                msg_id,
                0, // Dummy relation_id
                dummy_cols,
                0, // Dummy LSN
            );
            
            try self.releaseSlotToQueue(slot_idx);
            log.info("✅ Boot schema published to KV for '{s}'", .{clean_table});
        }
    }
};
