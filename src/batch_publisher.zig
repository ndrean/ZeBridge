//! Batch Publisher with background flush thread
//!
//! Manages a dedicated thread that dequeues CDC events from an SPSC queue,
//! batches them, encodes to MessagePack/JSON, and publishes to NATS.

const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const nats = @import("nats");
const nats_publisher = @import("nats_publisher.zig");
const msgpack = @import("msgpack");
const pgoutput = @import("pgoutput.zig");
const SPSCQueue = @import("spsc_queue.zig").SPSCQueue;
const Config = @import("config.zig");
const encoder_mod = @import("encoder.zig");
const Metrics = @import("metrics.zig").Metrics;

pub const log = std.log.scoped(.batch_publisher);

/// Monotonic millisecond timestamp (replacement for removed getMilliTimestamp())
fn getMilliTimestamp() i64 {
    var ts: c.struct_timespec = undefined;
    _ = c.clock_gettime(c.CLOCK_MONOTONIC, &ts);
    return @as(i64, ts.tv_sec) * 1000 + @divTrunc(@as(i64, ts.tv_nsec), 1_000_000);
}

/// Initialize memory slab for event ring buffer with optional memory locking
/// Returns a contiguous block of memory sized for `slot_count` events of `slot_size` bytes each
///
/// Memory locking (mlock) prevents the kernel from swapping this buffer to disk,
/// eliminating swap-related latency spikes in production. This is critical for:
/// - Zero-stutter CDC replication (no pause when memory is swapped back in)
/// - Predictable latency under memory pressure
/// - Real-time performance guarantees
///
/// Note: mlock may fail if RLIMIT_MEMLOCK is too low. Check with `ulimit -l`.
/// On systemd systems, you may need to set LimitMEMLOCK=infinity in the service file.
pub fn initSlab(allocator: std.mem.Allocator, slot_count: usize, slot_size: usize) ![]u8 {
    const total_bytes = slot_count * slot_size;

    // Allocate page-aligned memory for mlock compatibility
    const page_size = std.heap.page_size_min;
    const slab = try allocator.alignedAlloc(u8, page_size, total_bytes);
    errdefer allocator.free(slab);

    // Attempt to lock memory to prevent swapping
    // std.posix.mlock is available on POSIX systems (Linux, macOS, BSD)
    if (@hasDecl(std.posix, "mlock")) {
        std.posix.mlock(slab.ptr, slab.len) catch |err| {
            // mlock failure is not fatal - bridge will still work, just with potential swap latency
            log.warn("⚠️  Could not lock memory to RAM ({any}). Bridge will work but may experience swap-related latency.", .{err});
            log.warn("    To enable memory locking, increase RLIMIT_MEMLOCK: ulimit -l unlimited", .{});
            log.warn("    For systemd services, add LimitMEMLOCK=infinity to the service file.", .{});
        };
        log.info("🔒 Successfully pinned {d} MB to RAM (Zero-Stutter mode)", .{total_bytes / (1024 * 1024)});
    } else {
        log.info("📦 Allocated {d} MB slab (memory locking not available on this platform)", .{total_bytes / (1024 * 1024)});
    }

    return slab;
}

/// Convert pgoutput.Column value to encoder.Value
fn columnValueToEncoderValue(
    encoder: *encoder_mod.Encoder,
    column_value: pgoutput.DecodedValue,
) !encoder_mod.Value {
    return switch (column_value) {
        .int32 => |v| encoder.createInt(@intCast(v)),
        .int64 => |v| encoder.createInt(v),
        .float64 => |v| encoder.createFloat(v),
        .boolean => |v| encoder.createBool(v),
        .text, .bytea, .array, .numeric => |v| try encoder.createString(v),
        .jsonb => |v| blk: {
            // For JSONB columns in MessagePack mode, parse and convert to native types
            // For JSON mode, just treat as a string (simpler and avoids ownership issues)
            break :blk switch (encoder.format) {
                .msgpack => blk2: {
                    // Parse JSON and convert to MessagePack native types
                    const parsed = std.json.parseFromSlice(
                        std.json.Value,
                        encoder.allocator,
                        v,
                        .{},
                    ) catch {
                        // If parsing fails, fall back to string
                        break :blk2 try encoder.createString(v);
                    };
                    defer parsed.deinit();

                    const mp = try jsonValueToMsgpack(parsed.value, encoder.allocator);
                    break :blk2 encoder_mod.Value{ .msgpack = mp };
                },
                .json => try encoder.createString(v), // Keep JSONB as JSON string
            };
        },
        .null => encoder.createNull(),
    };
}

/// Convert std.json.Value to msgpack.Payload recursively (for JSONB columns in MessagePack mode)
fn jsonValueToMsgpack(value: std.json.Value, allocator: std.mem.Allocator) !msgpack.Payload {
    return switch (value) {
        .null => msgpack.Payload{ .nil = {} },
        .bool => |b| msgpack.Payload{ .bool = b },
        .integer => |i| msgpack.Payload{ .int = i },
        .float => |f| msgpack.Payload{ .float = f },
        .number_string => |s| blk: {
            // Try to parse as number, fallback to string
            if (std.fmt.parseInt(i64, s, 10)) |int_val| {
                break :blk msgpack.Payload{ .int = int_val };
            } else |_| {
                if (std.fmt.parseFloat(f64, s)) |float_val| {
                    break :blk msgpack.Payload{ .float = float_val };
                } else |_| {
                    break :blk try msgpack.Payload.strToPayload(s, allocator);
                }
            }
        },
        .string => |s| try msgpack.Payload.strToPayload(s, allocator),
        .array => |arr| blk: {
            var msgpack_arr = try allocator.alloc(msgpack.Payload, arr.items.len);
            for (arr.items, 0..) |item, i| {
                msgpack_arr[i] = try jsonValueToMsgpack(item, allocator);
            }
            break :blk msgpack.Payload{ .arr = msgpack_arr };
        },
        .object => |obj| blk: {
            var map_payload = msgpack.Payload.mapPayload(allocator);
            var it = obj.iterator();
            while (it.next()) |entry| {
                const val = try jsonValueToMsgpack(entry.value_ptr.*, allocator);
                try map_payload.mapPut(entry.key_ptr.*, val);
            }
            break :blk map_payload;
        },
    };
}

/// Configuration for batch publishing
pub const BatchConfig = struct {
    /// Maximum number of events per batch
    max_events: usize = Config.Batch.max_events,
    /// Maximum time to wait before flushing (milliseconds)
    max_wait_ms: i64 = Config.Batch.max_age_ms,
    /// Maximum payload size in bytes
    max_payload_bytes: usize = 128 * 1024, // 128KB
};

/// Maximum packed buffer size for column data (1MB = 2^20)
/// Actual size used is determined by runtime config (event_data_buffer_log2)
/// This is the compile-time maximum - runtime config must not exceed this
const MAX_EVENT_DATA_BUFFER_SIZE: usize = 1024 * 1024; // 1MB

/// A single CDC event to be batched
/// Zero-allocation design: all data stored in packed inline buffers
pub const CDCEvent = struct {
    // Fixed-size buffers for metadata (inline storage, no heap allocation)
    subject_buf: [128]u8 = undefined,
    subject_len: u8 = 0,

    table_buf: [64]u8 = undefined,
    table_len: u8 = 0,

    operation_buf: [8]u8 = undefined, // INSERT, UPDATE, DELETE
    operation_len: u8 = 0,

    msg_id_buf: [64]u8 = undefined,
    msg_id_len: u8 = 0,

    relation_id: u32 = 0, // PostgreSQL relation OID (for schema version tracking)
    lsn: u64 = 0, // WAL LSN for this event

    // Packed data area - column names and values packed contiguously
    // Points into separately-allocated data slab (sized at runtime)
    // This avoids wasting memory when user configures smaller buffer sizes
    data_buffer: []u8 = &[_]u8{}, // Slice into data slab
    data_len: usize = 0,
    // Note: data_buffer.len is the limit (no separate field needed)

    // Fixed array of column descriptors (indices into data_buffer)
    // Maximum 64 columns per event
    // columns: [64]ColumnView = undefined,
    columns: [512]ColumnView = undefined, // UPDATE to 512 columns
    column_count: u8 = 0,

    /// Column descriptor - "fat pointer" into packed data_buffer
    pub const ColumnView = struct {
        name_len: u8,
        name_offset: u16,
        value_tag: pgoutput.ValueTag,
        value_len: u32,
        value_offset: u32,
    };

    /// Set subject string (copies into inline buffer)
    pub fn setSubject(self: *CDCEvent, subject: []const u8) !void {
        if (subject.len > self.subject_buf.len) {
            return error.SubjectTooLong;
        }
        @memcpy(self.subject_buf[0..subject.len], subject);
        self.subject_len = @intCast(subject.len);
    }

    /// Get subject string as slice
    pub inline fn getSubject(self: *const CDCEvent) []const u8 {
        return self.subject_buf[0..self.subject_len];
    }

    /// Set table name (copies into inline buffer)
    pub fn setTable(self: *CDCEvent, table: []const u8) !void {
        if (table.len > self.table_buf.len) {
            return error.TableNameTooLong;
        }
        @memcpy(self.table_buf[0..table.len], table);
        self.table_len = @intCast(table.len);
    }

    /// Get table name as slice
    pub inline fn getTable(self: *const CDCEvent) []const u8 {
        return self.table_buf[0..self.table_len];
    }

    /// Set operation (copies into inline buffer)
    pub fn setOperation(self: *CDCEvent, operation: []const u8) !void {
        if (operation.len > self.operation_buf.len) {
            return error.OperationTooLong;
        }
        @memcpy(self.operation_buf[0..operation.len], operation);
        self.operation_len = @intCast(operation.len);
    }

    /// Get operation as slice
    pub inline fn getOperation(self: *const CDCEvent) []const u8 {
        return self.operation_buf[0..self.operation_len];
    }

    /// Set message ID (copies into inline buffer)
    pub fn setMsgId(self: *CDCEvent, msg_id: []const u8) !void {
        if (msg_id.len > self.msg_id_buf.len) {
            return error.MsgIdTooLong;
        }
        @memcpy(self.msg_id_buf[0..msg_id.len], msg_id);
        self.msg_id_len = @intCast(msg_id.len);
    }

    /// Get message ID as slice
    pub inline fn getMsgId(self: *const CDCEvent) []const u8 {
        return self.msg_id_buf[0..self.msg_id_len];
    }

    /// Add column to packed buffer (zero-allocation)
    /// Packs column name and value contiguously into data_buffer
    pub fn addColumn(self: *CDCEvent, name: []const u8, value: pgoutput.DecodedValue) !void {
        if (self.column_count >= 512) { // CHANGED FORM 64->512
            return error.TooManyColumns;
        }

        // Pack column name
        const name_off = @as(u16, @intCast(self.data_len));
        const required_size = self.data_len + name.len;
        if (required_size > self.data_buffer.len) {
            log.err("🔴 FATAL: Row size exceeds configured buffer capacity!", .{});
            log.err("    Column: '{s}'", .{name});
            log.err("    Required: {d} bytes", .{required_size});
            log.err("    Available: {d} bytes (BASE_BUF={d})", .{ self.data_buffer.len, std.math.log2_int(usize, self.data_buffer.len) });
            log.err("    Table: {s}", .{self.getTable()});
            log.err("    Solution: Increase BASE_BUF environment variable (e.g., BASE_BUF=16 for 64KB)", .{});
            return error.BufferOverflow;
        }
        @memcpy(self.data_buffer[self.data_len..][0..name.len], name);
        self.data_len += name.len;

        // Pack value based on type and determine value tag
        const val_off = @as(u32, @intCast(self.data_len));
        var val_len: u32 = 0;
        const value_tag: pgoutput.ValueTag = switch (value) {
            .null => .null,
            .boolean => .boolean,
            .int32 => .int32,
            .int64 => .int64,
            .float64 => .float64,
            .text => .text,
            .numeric => .numeric,
            .jsonb => .jsonb,
            .array => .array,
            .bytea => .bytea,
        };

        switch (value) {
            .null => {
                // No data to pack
                val_len = 0;
            },
            .boolean => |v| {
                val_len = 1;
                if (self.data_len + val_len > self.data_buffer.len) {
                    log.err("🔴 FATAL: Row size exceeds configured buffer capacity (boolean value)!", .{});
                    log.err("    Required: {d} bytes, Available: {d} bytes", .{ self.data_len + val_len, self.data_buffer.len });
                    return error.BufferOverflow;
                }
                self.data_buffer[self.data_len] = if (v) 1 else 0;
            },
            .int32 => |v| {
                val_len = 4;
                if (self.data_len + val_len > self.data_buffer.len) {
                    log.err("🔴 FATAL: Row size exceeds configured buffer capacity (int32 value)!", .{});
                    log.err("    Required: {d} bytes, Available: {d} bytes", .{ self.data_len + val_len, self.data_buffer.len });
                    return error.BufferOverflow;
                }
                std.mem.writeInt(i32, self.data_buffer[self.data_len..][0..4], v, .little);
            },
            .int64 => |v| {
                val_len = 8;
                if (self.data_len + val_len > self.data_buffer.len) {
                    log.err("🔴 FATAL: Row size exceeds configured buffer capacity (int64 value)!", .{});
                    log.err("    Required: {d} bytes, Available: {d} bytes", .{ self.data_len + val_len, self.data_buffer.len });
                    return error.BufferOverflow;
                }
                std.mem.writeInt(i64, self.data_buffer[self.data_len..][0..8], v, .little);
            },
            .float64 => |v| {
                val_len = 8;
                if (self.data_len + val_len > self.data_buffer.len) {
                    log.err("🔴 FATAL: Row size exceeds configured buffer capacity (float64 value)!", .{});
                    log.err("    Required: {d} bytes, Available: {d} bytes", .{ self.data_len + val_len, self.data_buffer.len });
                    return error.BufferOverflow;
                }
                const bytes = std.mem.asBytes(&v);
                @memcpy(self.data_buffer[self.data_len..][0..8], bytes);
            },
            .text, .numeric, .jsonb, .array, .bytea => |v| {
                val_len = @intCast(v.len);
                if (self.data_len + val_len > self.data_buffer.len) {
                    log.err("🔴 FATAL: Row size exceeds configured buffer capacity (string/blob value)!", .{});
                    log.err("    Value size: {d} bytes, Total required: {d} bytes, Available: {d} bytes", .{ val_len, self.data_len + val_len, self.data_buffer.len });
                    return error.BufferOverflow;
                }
                @memcpy(self.data_buffer[self.data_len..][0..v.len], v);
            },
        }
        self.data_len += val_len;

        // Store column descriptor
        self.columns[self.column_count] = .{
            .name_len = @intCast(name.len),
            .name_offset = name_off,
            .value_tag = value_tag,
            .value_len = val_len,
            .value_offset = val_off,
        };
        self.column_count += 1;
    }

    /// Reset event for reuse (zero-allocation design)
    /// Called when returning slot to free queue
    pub fn reset(self: *CDCEvent) void {
        self.subject_len = 0;
        self.table_len = 0;
        self.operation_len = 0;
        self.msg_id_len = 0;
        self.relation_id = 0;
        self.lsn = 0;
        self.data_len = 0;
        self.column_count = 0;
    }
};

/// Batch publisher that runs a background flush thread
/// Dequeues events from SPSC queue, batches, encodes, and publishes to NATS
pub const BatchPublisher = struct {
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    config: BatchConfig,
    format: encoder_mod.Format,
    metrics: ?*Metrics, // Optional metrics reference

    // Pre-allocated ring buffer of CDCEvent structs (zero malloc/free!)
    // Allocated once at startup, reused for entire lifetime
    events: []CDCEvent,

    // Data slab - backing storage for all event data_buffers
    // Allocated separately to avoid wasting memory on MAX_EVENT_DATA_BUFFER_SIZE
    data_slab: []u8,

    // Lock-free queue of INDICES into events array (SPSC)
    // Producer: Main thread (via EventProcessor) - pushes event slot indices
    // Consumer: Flush thread - pops indices to read events
    pending_events: SPSCQueue(usize),

    // Free slots queue - tracks available event slots
    // Initially filled with all indices (0..capacity-1)
    // Producer pops free slot, writes event, pushes to pending
    // Consumer pops pending, reads event, pushes back to free
    free_slots: SPSCQueue(usize),

    // Atomic state shared between threads
    last_confirmed_lsn: std.atomic.Value(u64), // Last LSN confirmed by NATS
    fatal_error: std.atomic.Value(bool), // Set when NATS reconnection fails
    flush_complete: std.atomic.Value(bool), // Set when flush thread finishes final flush

    // Flush thread
    flush_thread: ?std.Thread,
    should_stop: std.atomic.Value(bool),

    pub fn init(
        allocator: std.mem.Allocator,
        publisher: *nats_publisher.Publisher,
        config: BatchConfig,
        format: encoder_mod.Format,
        metrics: ?*Metrics,
        runtime_config: *const Config.RuntimeConfig,
    ) !*BatchPublisher {
        // 1. The actual number of events the user wants
        const event_count = runtime_config.batch_ring_buffer_size;

        // 2a. Allocate metadata structs (tiny - just pointers and counters)
        const events = try allocator.alloc(CDCEvent, event_count);
        errdefer allocator.free(events);

        // 2b. Calculate runtime buffer size from config
        // bit shifting equivalent to td.math.pow(2, N)
        const buffer_limit = @as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2);

        // Validate buffer limit doesn't exceed compile-time maximum
        if (buffer_limit > MAX_EVENT_DATA_BUFFER_SIZE) {
            log.err("🔴 FATAL: Configured buffer size ({d} bytes) exceeds compile-time maximum ({d} bytes)", .{
                buffer_limit,
                MAX_EVENT_DATA_BUFFER_SIZE,
            });
            log.err("    BASE_BUF={d} is too large. Maximum allowed: {d}", .{
                runtime_config.event_data_buffer_log2,
                std.math.log2_int(usize, MAX_EVENT_DATA_BUFFER_SIZE),
            });
            return error.BufferSizeTooLarge;
        }

        // 2c. Allocate DATA SLAB - this is the heavy allocation we want to mlock
        // Allocate exactly the size the user configured (no waste!)
        const data_slab_size = event_count * buffer_limit;

        // Allocate page-aligned for mlock compatibility
        // Use regular alloc - alignment will be sufficient for mlock on most systems
        const data_slab = try allocator.alloc(u8, data_slab_size);
        errdefer allocator.free(data_slab);

        // 2d. Lock the DATA SLAB to RAM (this prevents swapping)
        if (@hasDecl(std.posix, "mlock")) {
            std.posix.mlock(data_slab.ptr, data_slab.len) catch |err| {
                log.warn("⚠️  Could not lock data slab to RAM ({any})", .{err});
                log.warn("    Bridge will work but may experience swap-related latency", .{});
            };
        }

        // 2e. Link: Point each event's data_buffer into its slice of the slab
        for (events, 0..) |*event, i| {
            const offset = i * buffer_limit;
            event.* = CDCEvent{
                .data_buffer = data_slab[offset .. offset + buffer_limit],
            };
        }

        // Calculate actual memory usage (metadata + data)
        const metadata_bytes = @sizeOf(CDCEvent) * event_count;
        const data_bytes = data_slab_size;
        const total_bytes = metadata_bytes + data_bytes;
        const total_mb = total_bytes / (1024 * 1024);

        log.info("📦 Ring buffer allocation (two-stage):", .{});
        log.info("   • Event count: {d}", .{event_count});
        log.info("   • Buffer per event: {d} bytes (BASE_BUF={d})", .{ buffer_limit, runtime_config.event_data_buffer_log2 });
        log.info("   • Metadata: {d} KB ({d} bytes/event)", .{ metadata_bytes / 1024, @sizeOf(CDCEvent) });
        log.info("   • Data slab: {d} MB (mlock={s})", .{ data_bytes / (1024 * 1024), if (@hasDecl(std.posix, "mlock")) "yes" else "no" });
        log.info("   • Total: {d} MB", .{total_mb});

        // 3. Queue Capacity: SPSC math requires:
        //    a) Power of 2
        //    b) N+1 slots to hold N items (one slot reserved for full/empty distinction)
        const min_queue_size = event_count + 1;
        const queue_cap = std.math.ceilPowerOfTwo(usize, min_queue_size) catch return error.CapacityTooLarge;

        var pending_events = try SPSCQueue(usize).init(allocator, queue_cap);
        errdefer pending_events.deinit();

        var free_slots = try SPSCQueue(usize).init(allocator, queue_cap);
        errdefer free_slots.deinit();

        // 4. Populate: Only push the indices of the storage we actually have
        for (0..event_count) |i| {
            try free_slots.push(i);
        }

        log.info("✨ Pre-allocated ring buffer: {d} events × {d} bytes = {d} KB (queue capacity: {d})", .{
            event_count,
            @sizeOf(CDCEvent),
            (event_count * @sizeOf(CDCEvent)) / 1024,
            queue_cap,
        });

        // Allocate BatchPublisher on heap for stable memory address
        // This is critical because flush_thread holds references to pending_events/free_slots
        const self = try allocator.create(BatchPublisher);
        errdefer allocator.destroy(self);

        self.* = BatchPublisher{
            .allocator = allocator,
            .publisher = publisher,
            .config = config,
            .format = format,
            .metrics = metrics,
            .events = events,
            .data_slab = data_slab,
            .pending_events = pending_events,
            .free_slots = free_slots,
            .last_confirmed_lsn = std.atomic.Value(u64).init(0),
            .fatal_error = std.atomic.Value(bool).init(false),
            .flush_complete = std.atomic.Value(bool).init(false),
            .flush_thread = null,
            .should_stop = std.atomic.Value(bool).init(false),
        };

        return self;
    }

    /// Start the background flush thread. Must be called after init() and after
    /// the BatchPublisher is in its final memory location (not on stack).
    pub fn start(self: *BatchPublisher) !void {
        // Spawn flush thread - self must be at stable address
        self.flush_thread = try std.Thread.spawn(.{}, flushLoop, .{self});
    }

    /// Join the flush thread (waits for completion)
    pub fn join(self: *BatchPublisher) void {
        // Signal flush thread to stop
        self.should_stop.store(true, .seq_cst);

        // Wait for flush thread to finish
        if (self.flush_thread) |thread| {
            thread.join();
            self.flush_thread = null;
        }
    }

    /// Deinit - cleanup resources
    /// Safe to call even if join() wasn't called - will join first if needed
    pub fn deinit(self: *BatchPublisher) void {
        // 1. Ensure background thread is stopped before freeing memory
        //    This prevents shutdown race where thread accesses freed memory
        //    Safe to call multiple times - join() is idempotent
        self.join();

        const allocator = self.allocator;

        // 2. Unlock the data slab from RAM (symmetric with mlock in init)
        if (@hasDecl(std.posix, "munlock")) {
            std.posix.munlock(self.data_slab.ptr, self.data_slab.len) catch |err| {
                log.warn("⚠️  Failed to unlock data slab: {any}", .{err});
            };
        }

        // 3. Now safe to free memory - no other thread is accessing it
        allocator.free(self.data_slab); // Free data slab first
        allocator.free(self.events); // Then metadata structs
        self.pending_events.deinit();
        self.free_slots.deinit();

        log.info("🛑 Batch publisher stopped cleanly", .{});

        // 4. Free the heap-allocated BatchPublisher struct itself
        allocator.destroy(self);
    }

    /// Add an event index to the batch (called from flush thread)
    /// Works with slot indices (not pointers!)
    fn addToBatch(
        batch: *std.ArrayList(usize),
        slot_idx: usize,
        allocator: std.mem.Allocator,
    ) !void {
        try batch.append(allocator, slot_idx);
    }

    /// Get the last LSN that was successfully confirmed by NATS
    /// Called only when ACK thresholds are met (bytes/time/keepalive)
    pub fn getLastConfirmedLsn(self: *BatchPublisher) u64 {
        return self.last_confirmed_lsn.load(.seq_cst);
    }

    /// Get current queue usage (0.0 = empty, 1.0 = full)
    pub fn getQueueUsage(self: *BatchPublisher) f64 {
        const current_len = self.pending_events.len();
        const capacity = self.events.len;
        return @as(f64, @floatFromInt(current_len)) / @as(f64, @floatFromInt(capacity));
    }

    /// Check if pending queue is empty (safe point for shutdown)
    pub fn queuesEmpty(self: *BatchPublisher) bool {
        return self.pending_events.isEmpty();
    }

    /// Check if a fatal error occurred (e.g., NATS reconnection timeout)
    pub fn hasFatalError(self: *BatchPublisher) bool {
        return self.fatal_error.load(.seq_cst);
    }

    /// Check if the flush thread has completed all pending work
    pub fn isFlushComplete(self: *BatchPublisher) bool {
        return self.flush_complete.load(.seq_cst);
    }

    /// Get the current queue length (number of pending events)
    pub fn len(self: *BatchPublisher) usize {
        return self.pending_events.len();
    }

    /// Background thread that continuously drains events from lock-free queue and flushes to NATS
    fn flushLoop(self: *BatchPublisher) void {
        log.info("ℹ️ Lock-free flush thread started", .{});

        var batches_processed: usize = 0;
        var last_flush_time = getMilliTimestamp();

        // Persistent batch that accumulates slot indices across iterations
        var batch: std.ArrayList(usize) = .empty;
        var current_payload_size: usize = 0; // Track approximate payload size
        defer {
            // Return slot indices to free queue on thread exit
            for (batch.items) |slot_idx| {
                self.events[slot_idx].reset();
                self.free_slots.push(slot_idx) catch |err| {
                    log.err("Failed to return slot on thread exit: {}", .{err});
                };
            }
            batch.deinit(self.allocator);
        }

        while (!self.should_stop.load(.seq_cst)) {
            // Drain events from the queue until we hit a threshold
            while (batch.items.len < self.config.max_events and
                current_payload_size < self.config.max_payload_bytes)
            {
                const slot_idx = self.pending_events.pop() orelse break;

                const event = &self.events[slot_idx];
                // Approximate payload size (table + operation + subject strings)
                const event_size = event.table_len + event.operation_len + event.subject_len;

                addToBatch(&batch, slot_idx, self.allocator) catch |err| {
                    log.err("⚠️ Failed to append to batch: {}", .{err});
                    // Return slot to free queue on error
                    event.reset();
                    self.free_slots.push(slot_idx) catch {};
                    break;
                };

                current_payload_size += event_size;
            }
            const now = getMilliTimestamp();
            const time_elapsed = now - last_flush_time;

            // Flush if we have events AND (batch is full OR payload too large OR timeout reached)
            const should_flush = batch.items.len > 0 and
                (batch.items.len >= self.config.max_events or
                    current_payload_size >= self.config.max_payload_bytes or
                    time_elapsed >= self.config.max_wait_ms);

            if (should_flush) {
                batches_processed += 1;
                log.info("Flush thread processing batch #{d} with {d} events", .{ batches_processed, batch.items.len });

                // Log the msg_ids of events being flushed
                if (batch.items.len > 0) {
                    const first_event = &self.events[batch.items[0]];
                    const last_event = &self.events[batch.items[batch.items.len - 1]];
                    log.debug("Batch #{d} contains msg_ids: {s} ... {s}", .{
                        batches_processed,
                        first_event.getMsgId(),
                        last_event.getMsgId(),
                    });
                }

                // Flush the current batch
                self.flushBatch(&batch) catch |err| {
                    log.err("⚠️ Failed to flush batch: {}", .{err});
                };

                // Clear the batch (capacity is retained for reuse)
                current_payload_size = 0;

                last_flush_time = now;

                // Update queue usage metrics after flushing batch
                if (self.metrics) |m| {
                    const queue_usage = self.getQueueUsage();
                    m.updateQueueUsage(queue_usage);
                }
            } else if (batch.items.len == 0) {
                // No events available
                if (self.should_stop.load(.seq_cst)) {
                    break;
                }
                // Sleep briefly to avoid busy-waiting
                std.Thread.sleep(1 * std.time.ns_per_ms);
            } else {
                // Have events but timeout not reached
                if (self.should_stop.load(.seq_cst)) {
                    log.info("Shutdown detected with {d} pending events - flushing now", .{batch.items.len});
                    self.flushBatch(&batch) catch |err| {
                        log.err("⚠️ Failed to flush pending batch on shutdown: {}", .{err});
                    };
                    current_payload_size = 0;
                    break;
                }
                // Keep them and sleep briefly
                std.Thread.sleep(1 * std.time.ns_per_ms);
            }
        }

        // Drain any remaining events on shutdown
        log.info("Flush thread shutting down, draining remaining events...", .{});

        // Drain remaining slot indices into the existing batch
        while (self.pending_events.pop()) |slot_idx| {
            addToBatch(&batch, slot_idx, self.allocator) catch |err| {
                log.err("⚠️ Failed to append final event: {}", .{err});
                self.events[slot_idx].reset();
                self.free_slots.push(slot_idx) catch {};
                break;
            };
        }

        if (batch.items.len > 0) {
            log.info("Flushing final batch with {d} events", .{batch.items.len});
            self.flushBatch(&batch) catch |err| {
                log.err("⚠️ Failed to flush final batch: {}", .{err});
            };
        }

        // Signal that flush thread has completed all work
        self.flush_complete.store(true, .seq_cst);
        log.info("✅ Flush thread completed - all events published", .{});
    }

    /// Update confirmed LSN and return slots to free queue after successful publish
    fn updateConfirmedLsn(self: *BatchPublisher, batch_items: []usize) void {
        // Calculate maximum LSN in this batch
        var max_lsn: u64 = 0;
        for (batch_items) |slot_idx| {
            const event = &self.events[slot_idx];
            if (event.lsn > max_lsn) {
                max_lsn = event.lsn;
            }
        }

        // Update last confirmed LSN after successful flush
        if (max_lsn > 0) {
            self.last_confirmed_lsn.store(max_lsn, .seq_cst);
            log.debug("Updated last confirmed LSN to {x}", .{max_lsn});
        }

        // Return slot indices to free queue for reuse
        for (batch_items) |slot_idx| {
            self.events[slot_idx].reset();
            self.free_slots.push(slot_idx) catch |err| {
                log.err("Failed to push slot to free queue: {}", .{err});
            };
        }
    }

    /// Perform the actual encoding and publishing to NATS
    /// Separated from flushBatch to enable clean retry logic
    fn doPublish(self: *BatchPublisher, indices: []usize) !void {
        if (indices.len == 0) return;

        const flush_alloc = self.allocator;
        const event_count = indices.len;

        log.debug("📦 Encoding {d} events for publish", .{event_count});

        // For single event, encode and publish directly
        if (event_count == 1) {
            const slot_idx = indices[0];
            const event = &self.events[slot_idx];

            var encoder = encoder_mod.Encoder.init(flush_alloc, self.format);
            defer encoder.deinit();

            var event_map = encoder.createMap();
            defer event_map.free(flush_alloc);

            try event_map.put("subject", try encoder.createString(event.getSubject()));
            try event_map.put("table", try encoder.createString(event.getTable()));
            try event_map.put("operation", try encoder.createString(event.getOperation()));
            try event_map.put("msg_id", try encoder.createString(event.getMsgId()));
            try event_map.put("relation_id", encoder.createInt(@intCast(event.relation_id)));
            try event_map.put("lsn", encoder.createInt(@intCast(event.lsn)));

            // Add column data from packed buffer
            if (event.column_count > 0) {
                log.debug("Single event has {d} columns", .{event.column_count});
                var data_map = encoder.createMap();

                // Iterate over column descriptors and decode from packed buffer
                for (event.columns[0..event.column_count]) |col_view| {
                    // Extract column name from packed buffer
                    const col_name = event.data_buffer[col_view.name_offset..][0..col_view.name_len];

                    // Decode value based on type tag
                    const value_enc = try decodePackedValue(&encoder, event, col_view);
                    try data_map.put(col_name, value_enc);
                }

                try event_map.put("data", data_map);
            }

            const encoded = try encoder.encode(event_map);
            defer self.allocator.free(encoded);

            // Create headers with message ID for deduplication
            var headers = nats.pool.Headers{};
            try headers.init(flush_alloc, 256);
            defer headers.deinit();
            try headers.append("Nats-Msg-Id", event.getMsgId());

            try self.publisher.publish(event.getSubject(), &headers, encoded);

            log.debug("Published single event: {s}", .{event.getSubject()});
        } else {
            // Batch publishing
            var encoder = encoder_mod.Encoder.init(flush_alloc, self.format);
            defer encoder.deinit();

            var batch_array = try encoder.createArray(event_count);
            defer batch_array.free(flush_alloc);

            const encode_start = getMilliTimestamp();

            for (indices, 0..) |slot_idx, i| {
                const event = &self.events[slot_idx];

                var event_map = encoder.createMap();

                try event_map.put("subject", try encoder.createString(event.getSubject()));
                try event_map.put("table", try encoder.createString(event.getTable()));
                try event_map.put("operation", try encoder.createString(event.getOperation()));
                try event_map.put("msg_id", try encoder.createString(event.getMsgId()));
                try event_map.put("relation_id", encoder.createInt(@intCast(event.relation_id)));
                try event_map.put("lsn", encoder.createInt(@intCast(event.lsn)));

                // Add column data from packed buffer
                if (event.column_count > 0) {
                    var data_map = encoder.createMap();

                    for (event.columns[0..event.column_count]) |col_view| {
                        const col_name = event.data_buffer[col_view.name_offset..][0..col_view.name_len];
                        const value_enc = try decodePackedValue(&encoder, event, col_view);
                        try data_map.put(col_name, value_enc);
                    }

                    try event_map.put("data", data_map);
                }

                try batch_array.setIndex(i, event_map);
            }

            const encoded = try encoder.encode(batch_array);
            defer self.allocator.free(encoded);

            const encode_elapsed = getMilliTimestamp() - encode_start;

            // Publish the batch with a composite message ID
            const publish_start = getMilliTimestamp();
            const first_event = &self.events[indices[0]];
            const last_event = &self.events[indices[event_count - 1]];
            const batch_msg_id = try std.fmt.allocPrint(
                flush_alloc,
                "batch-{s}-to-{s}",
                .{ first_event.getMsgId(), last_event.getMsgId() },
            );
            defer self.allocator.free(batch_msg_id);

            const batch_subject = try std.fmt.allocPrint(
                flush_alloc,
                "{s}.batch",
                .{first_event.getSubject()},
            );
            defer self.allocator.free(batch_subject);

            var headers = nats.pool.Headers{};
            try headers.init(flush_alloc, 256);
            defer headers.deinit();
            try headers.append("Nats-Msg-Id", batch_msg_id);

            try self.publisher.publish(batch_subject, &headers, encoded);
            const publish_elapsed = getMilliTimestamp() - publish_start;

            log.info("📤 Published batch: {d} events, {d} bytes to {s} (encode: {d}ms, publish: {d}ms)", .{
                event_count,
                encoded.len,
                batch_subject,
                encode_elapsed,
                publish_elapsed,
            });
        }
    }

    /// Flush a batch to NATS with retry logic and exponential backoff
    /// Takes a pointer to the batch ArrayList of slot indices
    /// Only returns slots to free queue after successful publish
    fn flushBatch(self: *BatchPublisher, batch: *std.ArrayList(usize)) !void {
        if (batch.items.len == 0) return;

        var retry_count: u32 = 0;
        const max_retries = 5;
        var backoff_ms: u64 = 100; // Start with 100ms

        const flush_start = getMilliTimestamp();

        log.info("📦 Starting flush of {d} events", .{batch.items.len});

        while (true) {
            // Attempt to encode and publish
            const result = self.doPublish(batch.items);

            if (result) |_| {
                // SUCCESS - Update LSN and return slots to free queue
                self.updateConfirmedLsn(batch.items);
                batch.clearRetainingCapacity();

                // Log flush timing if it took longer than expected
                const flush_elapsed = getMilliTimestamp() - flush_start;
                if (flush_elapsed > 100) {
                    log.warn("⏱️  Slow flush: {d}ms for {d} events", .{
                        flush_elapsed,
                        batch.items.len,
                    });
                }
                return;
            } else |err| {
                // FAILURE - Log and retry with backoff
                log.err("❌ NATS publish failed (attempt {d}/{d}): {}", .{
                    retry_count + 1,
                    max_retries + 1,
                    err,
                });

                if (retry_count >= max_retries) {
                    // Exhausted retries - set fatal flag to stop PostgreSQL replication
                    log.err("🔴 FATAL: Exhausted retries for batch publish - stopping bridge to prevent WAL overflow", .{});
                    self.fatal_error.store(true, .seq_cst);
                    return err;
                }

                // Wait before retrying (exponential backoff)
                log.warn("⏳ Retrying in {d}ms...", .{backoff_ms});
                std.Thread.sleep(backoff_ms * std.time.ns_per_ms);

                // Double the wait for next time, capped at 5 seconds
                backoff_ms = @min(backoff_ms * 2, 5000);
                retry_count += 1;
            }
        }
    }

    /// Decode a value from the packed buffer and convert to encoder value
    fn decodePackedValue(
        encoder: *encoder_mod.Encoder,
        event: *const CDCEvent,
        col_view: CDCEvent.ColumnView,
    ) !encoder_mod.Value {
        return switch (col_view.value_tag) {
            .null => encoder.createNull(),
            .boolean => blk: {
                const val = event.data_buffer[col_view.value_offset] != 0;
                break :blk encoder.createBool(val);
            },
            .int32 => blk: {
                const bytes = event.data_buffer[col_view.value_offset..][0..4];
                const val = std.mem.readInt(i32, bytes, .little);
                break :blk encoder.createInt(@intCast(val));
            },
            .int64 => blk: {
                const bytes = event.data_buffer[col_view.value_offset..][0..8];
                const val = std.mem.readInt(i64, bytes, .little);
                break :blk encoder.createInt(val);
            },
            .float64 => blk: {
                const bytes = event.data_buffer[col_view.value_offset..][0..8];
                const val: f64 = @bitCast(bytes.*);
                break :blk encoder.createFloat(val);
            },
            .text, .numeric, .array, .bytea, .jsonb => blk: {
                const str = event.data_buffer[col_view.value_offset..][0..col_view.value_len];
                break :blk try encoder.createString(str);
            },
            // .jsonb => blk: {
            //     const json_str = event.data_buffer[col_view.value_offset..][0..col_view.value_len];
            //     // For JSONB columns in MessagePack mode, parse and convert to native types
            //     break :blk switch (encoder.format) {
            //         .msgpack => blk2: {
            //             const parsed = std.json.parseFromSlice(
            //                 std.json.Value,
            //                 encoder.allocator,
            //                 json_str,
            //                 .{},
            //             ) catch {
            //                 break :blk2 try encoder.createString(json_str);
            //             };
            //             defer parsed.deinit();

            //             const mp = try jsonValueToMsgpack(parsed.value, encoder.allocator);
            //             break :blk2 encoder_mod.Value{ .msgpack = mp };
            //         },
            //         .json => try encoder.createString(json_str),
            //     };
            // },
        };
    }
};
