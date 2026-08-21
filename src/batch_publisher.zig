//! Batch Publisher with background flush thread
//!
//! Manages a dedicated thread that dequeues CDC events from an SPSC queue,
//! batches them, encodes to MessagePack/JSON, and publishes to NATS.

const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const utils = @import("utils.zig");
const nats = @import("nats");
const nats_publisher = @import("nats_publisher.zig");
const msgpack = @import("msgpack");
const pgoutput = @import("pgoutput.zig");
const SPSCQueue = @import("spsc_queue.zig").SPSCQueue;
const Config = @import("config.zig");
const encoder_mod = @import("encoder.zig");
const Metrics = @import("metrics.zig").Metrics;

pub const log = std.log.scoped(.batch_publisher);



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

/// Configuration for batch publishing
pub const BatchConfig = struct {
    /// Maximum number of events per batch
    max_events: usize = Config.Batch.max_events,
    /// Maximum time to wait before flushing (milliseconds)
    max_wait_ms: i64 = Config.Batch.max_age_ms,
    /// Maximum payload size in bytes
    max_payload_bytes: usize = 128 * 1024, // 128KB
    /// Publish retry budget. Exhausting it is fatal — the bridge stops rather than
    /// ACK an LSN whose data never reached NATS.
    max_retries: u32 = Config.Retry.publish_max_retries,
    backoff_ms: u64 = Config.Retry.publish_backoff_ms,
    max_backoff_ms: u64 = Config.Retry.publish_max_backoff_ms,
};

/// A single CDC event to be batched
/// Zero-allocation design: all data stored in packed inline buffers
/// What one event will occupy in a published batch.
///
/// ⚠️ Extracted and tested because the version that lived inline in `flushLoop` was
/// **wrong for eighteen months of row shapes and nothing noticed**: it summed
/// `table_len + operation_len + subject_len` — the metadata — and omitted `data_len`, the
/// row itself. On a table with an 11 KB jsonb column that is ~29 bytes against ~11 000,
/// so the batch's byte budget was never reached and batches ran to `max_events` instead:
/// 5 000 events in one 55 MB message against a 1 MiB server limit. Every publish was
/// refused, every retry re-sent the same bytes, and the events counter still climbed.
///
/// The lesson is the same one the snapshot path learned: **a size cap must measure the
/// thing it caps**, and arithmetic buried in a loop is arithmetic nobody can test.
pub fn wireSize(event: *const CDCEvent) usize {
    return event.data_len + event.table_len + event.operation_len +
        event.subject_len + per_event_envelope_bytes;
}

/// Per-event overhead beyond the packed column data: the MessagePack map framing, the
/// column-name keys, and the batch array entry. Deliberately generous — over-estimating
/// costs a slightly emptier batch, under-estimating costs a message the server refuses.
pub const per_event_envelope_bytes: usize = 256;

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
    columns: [max_columns]ColumnView = undefined,
    /// ⚠️ `u16`, not `u8`. The array holds 512 and the guard in `addColumn` tests against
    /// 512, but a `u8` counter panics on increment at 255 — so the guard could never fire
    /// and a wide table crashed before reaching the error it was written to return.
    column_count: u16 = 0,

    /// Columns one event can carry — see `config.Batch.max_columns`, which explains why
    /// this is the dominant term in the bridge's startup memory.
    pub const max_columns = Config.Batch.max_columns;

    /// Column descriptor - "fat pointer" into packed data_buffer
    ///
    /// `packed`, not the plain `struct` this used to be — and deliberately in *this*
    /// field order and these *exact* widths (u8 + u24 + u8-backed-enum + u24 = 64 bits).
    /// Every field lands on a byte boundary (bit offsets 0, 8, 32, 40, all multiples of
    /// 8), so despite being packed there is nothing to shift-and-mask across: reading a
    /// field compiles to a load plus one register shift, writing to a load, one
    /// hardware bitfield-insert (`bfi` on ARM64), and a store — verified by
    /// disassembly, not assumed. Reordering these fields or changing a width such that a
    /// field no longer starts on a byte boundary loses that property silently — the
    /// code keeps compiling, it just gets slower.
    ///
    /// 8 bytes against the previous 12 — a third off `RING_BUFFER_COUNT × max_columns`,
    /// the dominant term in the bridge's memory (`Config.Batch.max_columns`'s doc
    /// comment).
    pub const ColumnView = packed struct {
        name_len: u8,
        /// ⚠️ `u24`, not `u16`, not `u32`. This indexes into `data_buffer`, whose size
        /// is `2^BASE_BUF`. It used to be `u16`, which meant any row whose columns
        /// pushed the write offset past 65535 bytes **panicked in Debug and was
        /// undefined behaviour in ReleaseFast**: the cast would wrap and every later
        /// column would be read from the wrong offset, so the event decoded as garbage
        /// with nothing reporting an error. Found by snapshotting a table of 256 KiB
        /// rows at BASE_BUF=19, a configuration the sizing guards accept as valid.
        ///
        /// `u24`'s range (0..16,777,215) is not a smaller version of that same risk: it
        /// is exactly `Buffers.absolute_max_event_data_buffer_log2` (24, i.e. 16 MiB) —
        /// the hard ceiling `BASE_BUF`/`BASE_BUF_MAX` can never exceed. A name can never
        /// start at the very last byte of the buffer with nothing before it *and* fill
        /// it (a column name is never zero-length), so `name_offset` never needs to
        /// reach `data_buffer.len` itself, only up to `data_buffer.len - 1` — which
        /// tops out at exactly `u24`'s max at the absolute ceiling. `addColumn`'s
        /// `@intCast` to this type is therefore expected to always succeed, not a place
        /// that is trusted to silently truncate: if the bounds check above it is ever
        /// wrong, this panics loudly (Debug/ReleaseSafe) instead of wrapping quietly,
        /// which is the whole lesson `u16` taught here the first time.
        name_offset: u24,
        value_tag: pgoutput.ValueTag,
        /// Same width, same reasoning as `name_offset` — and the same invariant that
        /// keeps it in range: `addColumn` always writes a column's name before its
        /// value, and a name is never zero-length, so a value can never occupy the
        /// buffer's very last byte *and* everything before it — `value_len` therefore
        /// never needs to reach `data_buffer.len`, only `data_buffer.len - 1` at most.
        value_len: u24,

        /// Where this column's value starts.
        ///
        /// ⚠️ Derived, not stored — same reasoning as before packing existed: `addColumn`
        /// writes the name and then the value immediately after it, so this is always
        /// `name_offset + name_len`.
        ///
        /// ⚠️ Returns `u32`, wider than either operand. `name_offset` alone can reach
        /// `data_buffer.len - 1`; adding `name_len` for a value that starts at the very
        /// end of a full buffer can therefore reach `data_buffer.len` exactly — one past
        /// `u24`'s own max at the absolute ceiling. Harmless (that value is never
        /// dereferenced — it only occurs when `value_len` is 0, i.e. NULL/unchanged) but
        /// it must not be narrowed back to `u24` to "match" the fields it is built from.
        pub inline fn valueOffset(self: ColumnView) u32 {
            return @as(u32, self.name_offset) + self.name_len;
        }
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
        if (self.column_count >= max_columns) {
            return error.TooManyColumns;
        }

        // ⚠️ Bounds first, cast second. The offset used to be computed *before* this
        // check, so an oversized row tripped the narrowing cast on the way to the error
        // that would have described it.
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
        // Safe to narrow: the check above already proved data_len + name.len <=
        // data_buffer.len <= 2^24 (Buffers.absolute_max_event_data_buffer_log2), and
        // data_len itself is strictly less than data_buffer.len whenever name.len >= 1
        // (always true — a column name is never empty) — so this never exceeds u24's max.
        const name_off: u24 = @intCast(self.data_len);
        @memcpy(self.data_buffer[self.data_len..][0..name.len], name);
        self.data_len += name.len;

        // Pack value based on type and determine value tag.
        //
        // ⚠️ The value goes immediately after the name, which is exactly what makes
        // `ColumnView.valueOffset()` derivable. Any change that inserts padding or
        // reorders these two writes breaks that identity silently — the decode side would
        // read from the wrong place with no error.
        var val_len: u32 = 0;
        const value_tag: pgoutput.ValueTag = switch (value) {
            .null => .null,
            .unchanged => .unchanged,
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
            // Neither carries bytes: NULL has none, and an unchanged TOAST value was
            // never sent. They stay distinct through the tag, not the payload.
            .null, .unchanged => {
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
                // ⚠️ Bounds first, cast second — v.len comes from a decoded Postgres
                // value (a jsonb/text column can be far larger than this event's
                // buffer) and is checked here as a `usize` before it is ever narrowed
                // to fit `value_len: u24`. Casting first, as this used to, would panic
                // in Debug/ReleaseSafe or wrap in ReleaseFast for any value over 16 MiB
                // — the same failure shape `name_offset`'s `u16` bug had, on a
                // different field.
                if (self.data_len + v.len > self.data_buffer.len) {
                    log.err("🔴 FATAL: Row size exceeds configured buffer capacity (string/blob value)!", .{});
                    log.err("    Value size: {d} bytes, Total required: {d} bytes, Available: {d} bytes", .{ v.len, self.data_len + v.len, self.data_buffer.len });
                    return error.BufferOverflow;
                }
                // Safe now: v.len <= data_buffer.len - data_len <= data_buffer.len - 1
                // (data_len >= 1 here — the name was already written), which tops out
                // at u24's own max at the absolute ceiling.
                val_len = @intCast(v.len);
                @memcpy(self.data_buffer[self.data_len..][0..v.len], v);
            },
        }
        self.data_len += val_len;

        // Store column descriptor. val_len stays a `u32` local above so every branch's
        // arithmetic reads naturally; this is the one place it narrows to the field's
        // actual `u24` — safe because every branch above already proved its own value
        // fits (the fixed-width branches trivially, the variable-length one by the
        // bounds check just above it).
        self.columns[self.column_count] = .{
            .name_len = @intCast(name.len),
            .name_offset = name_off,
            .value_tag = value_tag,
            .value_len = @intCast(val_len),
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
    // Allocated separately, sized to the resolved per-instance buffer size rather than
    // the compiled-in ceiling, so a small BASE_BUF does not reserve the ceiling's worth
    // of memory per slot.
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
    force_flush: std.atomic.Value(bool), // Set by WAL thread on COMMIT

    // Flush thread
    flush_thread: ?std.Thread,
    should_stop: std.atomic.Value(bool),

    // Backing allocator for one flush's worth of encoding work: the msgpack/JSON value
    // trees built in publishSubjectGroup and the byte buffers doPublish/publishSubjectGroup
    // hand to the NATS client. Reset (not deinit'd) after every doPublish call, so its
    // pages settle at the flush thread's steady-state peak instead of being torn down and
    // rebuilt on `self.allocator` (c_allocator/DebugAllocator) once per column per event —
    // the same "packed into the ring buffer with zero heap allocation, then unpacked back
    // into a fresh malloc per string" pattern this arena exists to remove. Owned only by
    // the flush thread, so it needs no locking.
    encode_arena: std.heap.ArenaAllocator,

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

        // Defensive: args.zig already clamps event_data_buffer_log2 to at most
        // event_data_buffer_max_log2 before this ever runs, so this should be
        // unreachable in the live path — but BatchPublisher.init has no other caller to
        // enforce that, and checking against the *resolved* per-instance ceiling here
        // (rather than a second, independently-hardcoded one) is what makes it
        // impossible for this check and args.zig's clamp to disagree about what the
        // limit for this instance actually is.
        const max_event_data_buffer_size: usize = @as(usize, 1) << @intCast(runtime_config.event_data_buffer_max_log2);
        if (buffer_limit > max_event_data_buffer_size) {
            log.err("🔴 FATAL: Configured buffer size ({d} bytes) exceeds this instance's ceiling ({d} bytes)", .{
                buffer_limit,
                max_event_data_buffer_size,
            });
            log.err("    BASE_BUF={d} is too large. Maximum allowed: {d} (raise with BASE_BUF_MAX)", .{
                runtime_config.event_data_buffer_log2,
                runtime_config.event_data_buffer_max_log2,
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
            .force_flush = std.atomic.Value(bool).init(false),
            .encode_arena = std.heap.ArenaAllocator.init(allocator),
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

        // The flush thread is joined above, so nothing can still be allocating from it.
        self.encode_arena.deinit();

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

    /// Trigger an immediate flush of the current batch
    pub fn forceFlush(self: *BatchPublisher) void {
        self.force_flush.store(true, .seq_cst);
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
        var last_flush_time = utils.getMilliTimestamp();

        // Persistent batch that accumulates slot indices across iterations
        var batch: std.ArrayList(usize) = .empty;
        var current_payload_size: usize = 0; // Bytes this batch will occupy on the wire
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
                const event_size = wireSize(event);
                addToBatch(&batch, slot_idx, self.allocator) catch |err| {
                    log.err("⚠️ Failed to append to batch: {}", .{err});
                    // Return slot to free queue on error
                    event.reset();
                    self.free_slots.push(slot_idx) catch {};
                    break;
                };

                current_payload_size += event_size;
            }
            const now = utils.getMilliTimestamp();
            const time_elapsed = now - last_flush_time;
            const force = self.force_flush.swap(false, .seq_cst);

            // Flush if we have events AND (batch is full OR payload too large OR timeout reached OR forced)
            const reason_max_events = batch.items.len >= self.config.max_events;
            const reason_max_payload = current_payload_size >= self.config.max_payload_bytes;
            const reason_timeout = time_elapsed >= self.config.max_wait_ms;

            const should_flush = batch.items.len > 0 and (force or reason_max_events or reason_max_payload or reason_timeout);

            if (should_flush) {
                batches_processed += 1;
                if (batch.items.len < self.config.max_events) {
                    log.debug("Flush triggered early! events={d}/{d}, payload={d}/{d}, time={d}/{d}ms. Reasons: forced={}, max_events={}, max_payload={}, timeout={}", .{
                        batch.items.len, self.config.max_events,
                        current_payload_size, self.config.max_payload_bytes,
                        time_elapsed, self.config.max_wait_ms,
                        force, reason_max_events, reason_max_payload, reason_timeout
                    });
                } else {
                    log.debug("Flush thread processing batch #{d} with {d} events", .{ batches_processed, batch.items.len });
                }


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
                utils.sleep(1 * std.time.ns_per_ms);
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
                utils.sleep(1 * std.time.ns_per_ms);
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
                // free_slots.push() cannot fail under correct operation: total slot indices
                // are a conserved quantity equal to event_count, and queue capacity >= event_count+1.
                // If we land here it's an invariant violation / programming bug. Without fatal_error
                // the slot leaks permanently, the ring buffer starves, and the pipeline silently deadlocks.
                log.err("🔴 Invariant violated: free_slots full on slot reclaim {d}: {} — shutting down", .{ slot_idx, err });
                self.fatal_error.store(true, .seq_cst);
            };
        }
    }

    fn doPublish(self: *BatchPublisher, indices: []usize) !void {
        if (indices.len == 0) return;

        // Every allocation this flush needs — the msgpack/JSON value trees and the
        // encoded byte buffers handed to the NATS client — comes from this arena instead
        // of `self.allocator` directly, and none of it needs to survive past this
        // function returning (the publish calls below are synchronous). Reset rather
        // than a fresh init/deinit per flush: capacity settles at this flush thread's
        // steady-state peak instead of being torn down and grown again every batch.
        defer _ = self.encode_arena.reset(.retain_capacity);
        const flush_alloc = self.encode_arena.allocator();
        const event_count = indices.len;

        log.debug("📦 Encoding {d} events for publish", .{event_count});

        var cdc_indices: std.ArrayListUnmanaged(usize) = .empty;
        defer cdc_indices.deinit(flush_alloc);

        for (indices) |slot_idx| {
            const event = &self.events[slot_idx];
            if (std.mem.startsWith(u8, event.getSubject(), Config.Nats.kv_subject_prefix)) {
                // If there are pending CDC events, flush them first to preserve order
                if (cdc_indices.items.len > 0) {
                    try self.publishCDCSubBatch(cdc_indices.items);
                    cdc_indices.clearRetainingCapacity();
                }
                
                // Publish KV event directly as raw JSON
                if (event.column_count > 0) {
                    const col_view = event.columns[0];
                    const raw_json = event.data_buffer[col_view.valueOffset()..][0..col_view.value_len];
                    
                    const msg_id = event.getMsgId();
                    
                    try self.publisher.publish(event.getSubject(), msg_id, raw_json);
                    log.info("📤 Published KV schema: {d} bytes to {s}", .{raw_json.len, event.getSubject()});
                }
            } else {
                try cdc_indices.append(flush_alloc, slot_idx);
            }
        }

        // Flush any remaining CDC events
        if (cdc_indices.items.len > 0) {
            try self.publishCDCSubBatch(cdc_indices.items);
        }
    }

    /// Publish a run of CDC events, grouped by subject.
    ///
    /// A flush batch is drained from the ring buffer indiscriminately, so one run can
    /// mix operations AND tables. This used to publish the whole run under the first
    /// event's subject, which misrouted everything else in it: a DELETE batched behind
    /// an INSERT landed on `cdc.<table>.insert.batch`, and a run spanning two tables
    /// published one table's rows under the other's subject. Payloads still carried
    /// the right per-event `subject`/`table`/`operation`, so nothing was lost — but
    /// subject filtering, which is the documented consumer pattern, returned wrong
    /// data and leaked rows across tables.
    ///
    /// Groups are emitted in first-appearance order, so a causal sequence on the same
    /// row (INSERT then DELETE in one transaction) still reaches the stream in the
    /// right order for a consumer reading `cdc.>`.
    fn publishCDCSubBatch(self: *BatchPublisher, indices: []usize) !void {
        if (indices.len == 0) return;
        if (indices.len == 1) return self.publishSubjectGroup(indices);

        // Same arena as doPublish, which called this and reset it fresh — grouping
        // bookkeeping and the encode work below share one lifetime.
        const flush_alloc = self.encode_arena.allocator();

        // Subjects in first-appearance order; the map holds each subject's indices.
        var order: std.ArrayListUnmanaged([]const u8) = .empty;
        defer order.deinit(flush_alloc);

        var groups = std.StringHashMap(std.ArrayListUnmanaged(usize)).init(flush_alloc);
        defer {
            var it = groups.valueIterator();
            while (it.next()) |list| list.deinit(flush_alloc);
            groups.deinit();
        }

        for (indices) |slot_idx| {
            // Slices into the slot's inline subject_buf; slots are not reset until
            // after publishing, so these stay valid for the life of this call.
            const subject = self.events[slot_idx].getSubject();
            const gop = try groups.getOrPut(subject);
            if (!gop.found_existing) {
                gop.value_ptr.* = .empty;
                try order.append(flush_alloc, subject);
            }
            try gop.value_ptr.append(flush_alloc, slot_idx);
        }

        // Homogeneous run: avoid the copy and publish it directly.
        if (order.items.len == 1) return self.publishSubjectGroup(indices);

        log.debug("📦 Flush run spans {d} subjects — publishing one message each", .{order.items.len});

        for (order.items) |subject| {
            const group = groups.get(subject).?;
            try self.publishSubjectGroup(group.items);
        }
    }

    /// Publish events that all share one subject. Callers must guarantee that;
    /// publishCDCSubBatch is what establishes it.
    ///
    /// Every allocation here — the msgpack/JSON value tree, the encoded bytes, the
    /// batch subject/msg-id — comes from `doPublish`'s arena and needs no individual
    /// `.free()`: the caller resets the whole arena once this flush's publish calls
    /// return, which is the same "arena.deinit() handles everything" pattern the WAL
    /// parse loop uses. Freeing any of it here would be freeing arena memory through
    /// `self.allocator` (c_allocator/DebugAllocator), which does not own it.
    fn publishSubjectGroup(self: *BatchPublisher, indices: []usize) !void {
        const flush_alloc = self.encode_arena.allocator();
        const event_count = indices.len;

        // For single event, encode and publish directly
        if (event_count == 1) {
            const slot_idx = indices[0];
            const event = &self.events[slot_idx];

            var encoder = encoder_mod.Encoder.init(flush_alloc, self.format);

            var event_map = encoder.createMap();

            try event_map.put(encoder.allocator, "subject", try encoder.createString(event.getSubject()));
            try event_map.put(encoder.allocator, "table", try encoder.createString(event.getTable()));
            try event_map.put(encoder.allocator, "operation", try encoder.createString(event.getOperation()));
            try event_map.put(encoder.allocator, "msg_id", try encoder.createString(event.getMsgId()));
            try event_map.put(encoder.allocator, "relation_id", encoder.createInt(@intCast(event.relation_id)));
            try event_map.put(encoder.allocator, "lsn", encoder.createInt(@intCast(event.lsn)));

            // Add column data from packed buffer
            if (event.column_count > 0) {
                log.debug("Single event has {d} columns", .{event.column_count});
                var data_map = encoder.createMap();

                // Iterate over column descriptors and decode from packed buffer
                for (event.columns[0..event.column_count]) |col_view| {
                    // Extract column name from packed buffer
                    const col_name = event.data_buffer[col_view.name_offset..][0..col_view.name_len];

                    // Decode value based on type tag. An absent key means "unchanged",
                    // which is what Postgres said.
                    const value_enc = try decodePackedValue(&encoder, event, col_view) orelse continue;
                    try data_map.put(encoder.allocator, col_name, value_enc);
                }

                try event_map.put(encoder.allocator, "data", data_map);
            }

            const encoded = try encoder.encode(event_map);

            // Create headers with message ID for deduplication
                    const msg_id = event.getMsgId();

            try self.publisher.publish(event.getSubject(), msg_id, encoded);

            // ⚠️ Counted here too, not only in the batch path below.
            //
            // It was missing, and the omission was invisible at load and glaring at idle:
            // a burst goes through the batch path and increments, while a demo doing one
            // insert at a time goes through *this* path and never did — so
            // `bridge_cdc_events_published_total` read **0** while `cdc.users.insert`
            // was demonstrably on the wire. The metric said the bridge was dead when it
            // was working, which is the worst direction for a counter to be wrong in.
            if (self.metrics) |m| m.incrementCdcEvents();

            log.debug("Published single event: {s}", .{event.getSubject()});
        } else {
            // Batch publishing
            var encoder = encoder_mod.Encoder.init(flush_alloc, self.format);

            var batch_array = try encoder.createArray(event_count);

            const encode_start = utils.getMilliTimestamp();

            for (indices, 0..) |slot_idx, i| {
                const event = &self.events[slot_idx];

                var event_map = encoder.createMap();

                try event_map.put(encoder.allocator, "subject", try encoder.createString(event.getSubject()));
                try event_map.put(encoder.allocator, "table", try encoder.createString(event.getTable()));
                try event_map.put(encoder.allocator, "operation", try encoder.createString(event.getOperation()));
                try event_map.put(encoder.allocator, "msg_id", try encoder.createString(event.getMsgId()));
                try event_map.put(encoder.allocator, "relation_id", encoder.createInt(@intCast(event.relation_id)));
                try event_map.put(encoder.allocator, "lsn", encoder.createInt(@intCast(event.lsn)));

                // Add column data from packed buffer
                if (event.column_count > 0) {
                    var data_map = encoder.createMap();

                    for (event.columns[0..event.column_count]) |col_view| {
                        const col_name = event.data_buffer[col_view.name_offset..][0..col_view.name_len];
                        // Absent key = "unchanged", which is what Postgres said.
                        const value_enc = try decodePackedValue(&encoder, event, col_view) orelse continue;
                        try data_map.put(encoder.allocator, col_name, value_enc);
                    }

                    try event_map.put(encoder.allocator, "data", data_map);
                }

                try batch_array.setIndex(i, event_map);
            }

            const encoded = try encoder.encode(batch_array);

            const encode_elapsed = utils.getMilliTimestamp() - encode_start;

            // Publish the batch with a composite message ID
            const publish_start = utils.getMilliTimestamp();
            const first_event = &self.events[indices[0]];
            const last_event = &self.events[indices[event_count - 1]];
            const batch_msg_id = try std.fmt.allocPrint(
                flush_alloc,
                "batch-{s}-to-{s}",
                .{ first_event.getMsgId(), last_event.getMsgId() },
            );

            const batch_subject = try std.fmt.allocPrint(
                flush_alloc,
                "{s}.batch",
                .{first_event.getSubject()},
            );

            const msg_id = batch_msg_id;

            try self.publisher.publish(batch_subject, msg_id, encoded);
            const publish_elapsed = utils.getMilliTimestamp() - publish_start;

            // Counted **here**, on a publish the server acknowledged — not when the event
            // was packed into a ring-buffer slot, which is where this used to live.
            //
            // That placement made `bridge_cdc_events_published_total` count events the
            // bridge had *produced*, under a name promising events it had *delivered*.
            // When oversized batches were being refused by the server, the metric read
            // 50 000 published against a stream holding zero — the dashboard's "NATS
            // Events Out" line was green throughout total data loss. A counter that
            // cannot distinguish those two states cannot be alerted on.
            if (self.metrics) |m| {
                for (0..event_count) |_| m.incrementCdcEvents();
            }

            log.debug("📤 Published batch: {d} events, {d} bytes to {s} (encode: {d}ms, publish: {d}ms)", .{
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
        const max_retries = self.config.max_retries;
        var backoff_ms: u64 = self.config.backoff_ms;

        const flush_start = utils.getMilliTimestamp();

        log.debug("📦 Starting flush of {d} events", .{batch.items.len});

        while (true) {
            // Attempt to encode and publish
            const result = self.doPublish(batch.items);

            if (result) |_| {
                // Read the count BEFORE clearing. This log used to sit after
                // `clearRetainingCapacity()` and report `batch.items.len`, so every slow
                // flush claimed "0 events" no matter how many it had just published —
                // which reads like a stalled pipeline and is nothing of the sort.
                const event_count = batch.items.len;

                // SUCCESS - Update LSN and return slots to free queue
                self.updateConfirmedLsn(batch.items);
                batch.clearRetainingCapacity();

                // Log flush timing if it took longer than expected
                const flush_elapsed = utils.getMilliTimestamp() - flush_start;
                if (flush_elapsed > 100) {
                    log.warn("⏱️  Slow flush: {d}ms for {d} events", .{
                        flush_elapsed,
                        event_count,
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
                utils.sleep(backoff_ms * std.time.ns_per_ms);

                // Double the wait for next time, capped at 5 seconds
                backoff_ms = @min(backoff_ms * 2, self.config.max_backoff_ms);
                retry_count += 1;
            }
        }
    }

    /// Decode a value from the packed buffer and convert to encoder value
    /// Decode one packed column into an encoder value.
    ///
    /// Returns `null` for an unchanged TOAST column, meaning **omit the key**. Making
    /// omission the return value rather than a filter at each call site is deliberate:
    /// there are two call sites, and one of them forgetting would silently reintroduce
    /// "unchanged means NULL" — the bug this whole path exists to remove.
    fn decodePackedValue(
        encoder: *encoder_mod.Encoder,
        event: *const CDCEvent,
        col_view: CDCEvent.ColumnView,
    ) !?encoder_mod.Value {
        return switch (col_view.value_tag) {
            .unchanged => null,
            .null => encoder.createNull(),
            .boolean => blk: {
                const val = event.data_buffer[col_view.valueOffset()] != 0;
                break :blk encoder.createBool(val);
            },
            .int32 => blk: {
                const bytes = event.data_buffer[col_view.valueOffset()..][0..4];
                const val = std.mem.readInt(i32, bytes, .little);
                break :blk encoder.createInt(@intCast(val));
            },
            .int64 => blk: {
                const bytes = event.data_buffer[col_view.valueOffset()..][0..8];
                const val = std.mem.readInt(i64, bytes, .little);
                break :blk encoder.createInt(val);
            },
            .float64 => blk: {
                const bytes = event.data_buffer[col_view.valueOffset()..][0..8];
                const val: f64 = @bitCast(bytes.*);
                break :blk encoder.createFloat(val);
            },
            .text, .numeric, .array, .bytea, .jsonb => blk: {
                const str = event.data_buffer[col_view.valueOffset()..][0..col_view.value_len];
                break :blk try encoder.createString(str);
            },
        };
    }
};

// ─── wireSize ───────────────────────────────────────────────────────────────────
//
// The regression these pin: a batch's byte budget is only a budget if the per-event
// figure it accumulates is dominated by the row, not by the table name.

const testing = std.testing;

fn sizedEvent(data_len: usize) CDCEvent {
    var e = CDCEvent{};
    e.data_len = data_len;
    e.table_len = 5; // "mixed"
    e.operation_len = 6; // "INSERT"
    e.subject_len = 18; // "cdc.mixed.insert"
    return e;
}

test "wireSize - the row dominates, not the metadata" {
    // The bug: 11 KB of jsonb reported as ~29 bytes, so `max_payload_bytes` was
    // unreachable and every batch ran to max_events instead.
    const wide = sizedEvent(11_000);
    try testing.expect(wireSize(&wide) > 11_000);

    const metadata_only = 5 + 6 + 18;
    try testing.expect(wireSize(&wide) > metadata_only * 100);
}

test "wireSize - a batch of wide rows reaches the byte budget before the event count" {
    // 256 KB budget, 5 000 event ceiling. With 11 KB rows the budget must bind first —
    // roughly 22 events — or a batch becomes a 55 MB message the server refuses.
    const budget: usize = 256 * 1024;
    const max_events: usize = 5000;

    var total: usize = 0;
    var count: usize = 0;
    const e = sizedEvent(11_000);
    while (total < budget and count < max_events) : (count += 1) total += wireSize(&e);

    try testing.expect(count < 30);
    try testing.expect(total <= budget + wireSize(&e)); // overshoots by at most one event
}

test "wireSize - narrow rows still fill a batch by count, not bytes" {
    // The other direction: 100-byte rows should reach 5 000 events long before 256 KB…
    // except the envelope allowance dominates there, which is the correct conservative
    // behaviour — an emptier batch costs a round trip, an oversized one costs everything.
    const e = sizedEvent(100);
    try testing.expect(wireSize(&e) >= per_event_envelope_bytes);
    try testing.expect(wireSize(&e) < 1024);
}

test "wireSize - an empty event is still charged its envelope" {
    const e = sizedEvent(0);
    try testing.expect(wireSize(&e) >= per_event_envelope_bytes);
}

test "addColumn: a column past 64 KB into the buffer keeps its offset" {
    // ⚠️ The regression this exists for: `ColumnView.name_offset` was `u16`, so the first
    // column written past byte 65535 of the event buffer panicked in Debug and silently
    // wrapped in ReleaseFast — every later column then read from the wrong offset and the
    // event decoded as garbage with no error anywhere.
    //
    // Only reachable at BASE_BUF >= 17, which the sizing guards accept as valid: it took a
    // table of 256 KiB rows to reach it, and it killed the bridge outright.
    const allocator = std.testing.allocator;
    const buf_len = 256 * 1024;
    const data = try allocator.alloc(u8, buf_len);
    defer allocator.free(data);

    var ev = CDCEvent{ .data_buffer = data };
    ev.reset();

    // One fat value to push the write offset well past a u16, then a normal column after
    // it — the one whose offset used to wrap.
    const big = try allocator.alloc(u8, 100 * 1024);
    defer allocator.free(big);
    @memset(big, 'x');
    try ev.addColumn("blob", .{ .text = big });
    try std.testing.expect(ev.data_len > 65535);

    try ev.addColumn("after", .{ .int64 = 42 });

    const last = ev.columns[ev.column_count - 1];
    try std.testing.expect(last.name_offset > 65535);
    try std.testing.expectEqualStrings(
        "after",
        data[last.name_offset..][0..last.name_len],
    );
}

test "addColumn: the column ceiling is reported, not crashed into" {
    // `column_count` was a `u8` while the guard tested against 512, so the counter
    // panicked on increment at 255 and `TooManyColumns` was unreachable.
    const allocator = std.testing.allocator;
    const data = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(data);

    var ev = CDCEvent{ .data_buffer = data };
    ev.reset();

    var i: usize = 0;
    while (i < CDCEvent.max_columns) : (i += 1) {
        try ev.addColumn("c", .{ .int64 = 1 });
    }
    try std.testing.expectEqual(@as(u16, CDCEvent.max_columns), ev.column_count);
    try std.testing.expectError(error.TooManyColumns, ev.addColumn("one_too_many", .{ .int64 = 1 }));
}



