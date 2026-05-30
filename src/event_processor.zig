//! CDC Event Processor
//!
//! Processes CDC events from PostgreSQL pgoutput format and enqueues them
//! for publishing to NATS. Runs in the main thread.

const std = @import("std");
const pgoutput = @import("pgoutput.zig");
const SPSCQueue = @import("spsc_queue.zig").SPSCQueue;
const Config = @import("config.zig");
const Metrics = @import("metrics.zig").Metrics;
const batch_publisher = @import("batch_publisher.zig");

fn nanoNow() u64 {
    var ts: std.os.linux.timespec = undefined;
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s +
        @as(u64, @intCast(ts.nsec));
}

pub const log = std.log.scoped(.event_processor);

/// Format a DecodedValue for human-readable logging
/// Returns a string representation allocated in the provided arena
fn formatValueForLog(arena: std.mem.Allocator, value: pgoutput.DecodedValue) ![]const u8 {
    return switch (value) {
        .null => "NULL",
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
    batch_publisher: *batch_publisher.BatchPublisher, // Reference to batch publisher for ring buffer access
    metrics: ?*Metrics, // Optional metrics reference
    transition_rules: *const Config.EventClassification.TransitionRules, // Per-table transition column rules

    pub fn init(
        allocator: std.mem.Allocator,
        batch_pub: *batch_publisher.BatchPublisher,
        metrics: ?*Metrics,
        transition_rules: *const Config.EventClassification.TransitionRules,
    ) EventProcessor {
        return .{
            .allocator = allocator,
            .batch_publisher = batch_pub,
            .metrics = metrics,
            .transition_rules = transition_rules,
        };
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
            const old_decoded = pgoutput.decodeTuple(arena_allocator, old_data, rel.columns) catch |err| {
                log.warn("⚠️ Failed to decode old tuple: {}", .{err});
                return error.TupleDecodeFailed;
            };

            // 2. Decode "Main" Tuple (The standard New values)
            const main_decoded = pgoutput.decodeTuple(arena_allocator, tuple_data, rel.columns) catch |err| {
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
            const main_decoded = pgoutput.decodeTuple(arena_allocator, tuple_data, rel.columns) catch |err| {
                log.warn("⚠️ Failed to decode tuple: {}", .{err});
                return error.TupleDecodeFailed;
            };
            try all_columns.appendSlice(arena_allocator, main_decoded.items);
        }

        // Extract ID value for logging (scan for "id" column in the combined list)
        var id_buf: [64]u8 = undefined;
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

        // Create NATS subject
        var subject_buf: [Config.Buffers.subject_buffer_size]u8 = undefined;
        const subject = if (suffix) |s|
            try std.fmt.bufPrintZ(
                &subject_buf,
                "{s}.{s}.{s}.{s}",
                .{ Config.Nats.subject_cdc_prefix, rel.name, operation_lower, s },
            )
        else
            try std.fmt.bufPrintZ(
                &subject_buf,
                "{s}.{s}.{s}",
                .{ Config.Nats.subject_cdc_prefix, rel.name, operation_lower },
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
        const slot_idx = try self.acquireAndFillSlot(
            subject,
            rel.name,
            operation,
            msg_id,
            rel.relation_id,
            all_columns,
            wal_end,
        );

        if (self.metrics) |m| {
            m.incrementCdcEvents();
        }

        if (is_transition and transition_column_name != null) {
            if (id_str) |id| {
                log.info("{s} {s}.{s} id={s} [{s}: {s} → {s}] → {s}", .{
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
                log.info("{s} {s}.{s} [{s}: {s} → {s}] → {s}", .{
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
                log.info("{s} {s}.{s} id={s} → {s} [slot={d}]", .{ operation, rel.namespace, rel.name, id, subject, slot_idx });
            } else {
                log.info("{s} {s}.{s} → {s} [slot={d}]", .{ operation, rel.namespace, rel.name, subject, slot_idx });
            }
        }

        return slot_idx;
    }

    /// Push a packed slot onto the pending_events queue, making it visible to the
    /// background publisher. Call this only after the matching .commit is confirmed.
    pub fn releaseSlotToQueue(self: *EventProcessor, slot_idx: u32) !void {
        var retry_count: usize = 0;
        const max_retries_before_fatal_check = 1000;
        const timer_start: u64 = nanoNow();
        const watchdog_timeout_ns = std.time.ns_per_s * 30;

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
        const max_retries_before_fatal_check = 1000; // Check fatal error every 1000 retries
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
                    // TERMINAL FAILURE: Do not return the slot, do not yield.
                    // We must stop to prevent ACKing this LSN to PostgreSQL.
                    // If we ACK'd, PostgreSQL would discard this WAL data and we'd lose the row permanently.
                    log.err("🔴🔴🔴 FATAL: CDC Event too large for pre-allocated buffer 🔴🔴🔴", .{});
                    log.err("This is a configuration error - the bridge is shutting down to prevent data loss.", .{});
                    log.err("The current row will be replayed when the bridge restarts with a larger buffer.", .{});
                    @panic("CDC Event too large for buffer. Increase BASE_BUF environment variable and restart.");
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
};
