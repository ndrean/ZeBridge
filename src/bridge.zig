//! Bridge application that streams PostgreSQL CDC events to NATS JetStream using pgoutput format
const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Config = @import("config.zig");
const wal_stream = @import("wal_stream.zig");
const pgoutput = @import("pgoutput.zig");
const nats_publisher = @import("nats_publisher.zig");
const batch_publisher = @import("batch_publisher.zig");
const event_processor = @import("event_processor.zig");
const replication_setup = @import("replication_setup.zig");
const msgpack = @import("msgpack");
const http_server = @import("http_server.zig");
const metrics_mod = @import("metrics.zig");
const wal_monitor = @import("wal_monitor.zig");
const pg_conn = @import("pg_conn.zig");
const args = @import("args.zig");
const schema_publisher = @import("schema_publisher.zig");
const schema_cache_mod = @import("schema_cache.zig");
const publication_mod = @import("publication.zig");
const snapshot_listener = @import("snapshot_listener.zig");
const mutation_listener = @import("mutation_listener.zig");
const encoder_mod = @import("encoder.zig");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const utils = @import("utils.zig");
const preflight = @import("preflight.zig");
const refused_tables = @import("refused_tables.zig");
const type_registry = @import("type_registry.zig");
const topology_mod = @import("topology.zig");

// Force test discovery for imported modules.
// Zig only collects tests from files the root actually references, so every module
// carrying `test` blocks must be listed here or its tests silently never run.
comptime {
    _ = @import("array.zig");
    _ = @import("batch_publisher.zig");
    _ = @import("config.zig");
    _ = @import("encoder.zig");
    _ = @import("numeric.zig");
    _ = @import("mutation_listener.zig");
    _ = @import("pg_conn.zig");
    _ = @import("pgoutput.zig");
    _ = @import("publication.zig");
    _ = @import("preflight.zig");
    _ = @import("refused_tables.zig");
    _ = @import("type_registry.zig");
    _ = @import("pg_copy_binary.zig");
    _ = @import("snapshot_listener.zig");
    _ = @import("schema_cache.zig");
    _ = @import("schema_mapper.zig");
    _ = @import("spsc_queue.zig");
    _ = @import("streaming_encoder.zig");
    _ = @import("topology.zig");
}

pub const log = std.log.scoped(.bridge);

var runtime_log_level: std.log.Level = .info;

pub const std_options = std.Options{
    .log_level = .debug, // Compile all logs, filter at runtime
    .logFn = customLogFn,
};

pub fn customLogFn(
    comptime message_level: std.log.Level,
    comptime scope: @TypeOf(.EnumLiteral),
    comptime format: []const u8,
    fmt_args: anytype,
) void {
    if (@intFromEnum(message_level) > @intFromEnum(runtime_log_level)) return;
    std.log.defaultLog(message_level, scope, format, fmt_args);
}

// Double-check the pipeline invariant before spinning up threads.
// ring_buffer_capacity > max_rows_per_transaction must hold; if it doesn't,
// a single large transaction can exhaust every free slot before .commit lands,
// causing acquireAndFillSlot to spin forever (background thread starved).
comptime {
    if (Config.MemoryBounds.ring_buffer_capacity <= Config.MemoryBounds.max_rows_per_transaction) {
        @compileError("Deadlock risk: ring_buffer_capacity must be strictly greater than max_rows_per_transaction.");
    }
}

// Global flag for graceful shutdown (shared with HTTP server)
var should_stop = std.atomic.Value(bool).init(false);

/// Raised when a worker thread stops the bridge because of a configuration fault, as
/// opposed to Ctrl+C or /shutdown. Only the exit code distinguishes the two for whatever
/// supervises this process, and "misconfigured" must not look like "finished".
var boot_fatal = std.atomic.Value(bool).init(false);

// Derive signal handler parameter type from posix.Sigaction.
// On macOS Zig 0.16 the handler takes an anonymous enum(u32), not c_int.
const sig_num_t = t: {
    for (@typeInfo(posix.Sigaction).@"struct".fields) |f| {
        if (std.mem.eql(u8, f.name, "handler")) {
            for (@typeInfo(f.type).@"union".fields) |uf| {
                if (std.mem.eql(u8, uf.name, "handler")) {
                    const opt_ptr = @typeInfo(uf.type).optional.child;
                    const params = @typeInfo(@typeInfo(opt_ptr).pointer.child).@"fn".params;
                    break :t params[0].type.?;
                }
            }
        }
    }
    break :t c_int;
};

fn handleShutdown(_: sig_num_t) callconv(.c) void {
    should_stop.store(true, .seq_cst);
}

/// Initialize and verify PostgreSQL replication setup
/// Returns the monitored tables from the publication
fn initReplication(
    allocator: std.mem.Allocator,
    pg_config: *pg_conn.PgConf,
    slot_name: [:0]const u8,
    pub_name: [:0]const u8,
) !replication_setup.ReplicationContext {
    log.info("Initializing PostgreSQL replication...", .{});
    return try replication_setup.init(
        allocator,
        pg_config,
        slot_name,
        pub_name,
    );
}

/// Initialize and connect NATS publisher with metrics tracking
fn initNatsPublisher(
    allocator: std.mem.Allocator,
    metrics: *metrics_mod.Metrics,
    endpoint: Config.Nats.Endpoint,
    io: std.Io,
) !nats_publisher.Publisher {
    log.debug("Connecting to NATS JetStream...", .{});
    var publisher = try nats_publisher.Publisher.init(
        allocator,
        .{ .endpoint = endpoint },
        io,
    );
    publisher.metrics = metrics;
    errdefer publisher.deinit();

    publisher.metrics = metrics;
    publisher.connect() catch |err| {
        log.err(
            "🔴 Cannot reach NATS at {s}:{d} ({s}) — check NATS_URL. The bridge needs NATS at startup; it does not run degraded.",
            .{ endpoint.host, endpoint.port, @errorName(err) },
        );
        return err;
    };
    return publisher;
}

/// Initialize async batch publisher (does NOT start the thread - caller must call start())
/// Returns heap-allocated BatchPublisher to ensure stable memory address for flush thread
fn initBatchPublisher(
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    format: encoder_mod.Format,
    metrics: *metrics_mod.Metrics,
    runtime_config: *const Config.RuntimeConfig,
) !*batch_publisher.BatchPublisher {
    const batch_config = batch_publisher.BatchConfig{
        .max_events = runtime_config.batch_max_events,
        .max_wait_ms = runtime_config.batch_max_wait_ms,
        .max_payload_bytes = runtime_config.batch_max_payload_bytes,
        .max_retries = runtime_config.publish_max_retries,
        .backoff_ms = runtime_config.publish_backoff_ms,
        .max_backoff_ms = runtime_config.publish_max_backoff_ms,
    };

    return try batch_publisher.BatchPublisher.init(
        allocator,
        publisher,
        batch_config,
        format,
        metrics,
        runtime_config,
    );
}

/// Initialize HTTP server for health checks and metrics (does NOT start the thread)
/// The thread MUST be started after the server is at its final memory location
fn initHttpServer(
    allocator: std.mem.Allocator,
    port: u16,
    should_stop_flag: *std.atomic.Value(bool),
    metrics: *metrics_mod.Metrics,
    publisher: ?*nats_publisher.Publisher,
) !http_server.Server {
    return try http_server.Server.init(
        allocator,
        port,
        should_stop_flag,
        metrics,
        publisher,
    );
}

/// Connect to PostgreSQL replication stream and start streaming from current LSN
fn initReplicationStream(
    allocator: std.mem.Allocator,
    pg_config: *pg_conn.PgConf,
    slot_name: [:0]const u8,
    pub_name: [:0]const u8,
) !wal_stream.ReplicationStream {
    log.debug("Connecting to WAL replication stream...", .{});

    const current_lsn = try wal_monitor.getCurrentLSN(allocator, pg_config);
    defer allocator.free(current_lsn);

    log.debug("▶️ Current LSN: {s}\n", .{current_lsn});

    var pg_stream = wal_stream.ReplicationStream.init(
        allocator,
        .{
            .pg_config = pg_config,
            .slot_name = slot_name,
            .publication_name = pub_name,
        },
    );
    errdefer pg_stream.deinit();

    try pg_stream.connect();
    try pg_stream.startStreaming(current_lsn);

    log.info("✅ WAL replication stream started from LSN {s}\n", .{current_lsn});

    return pg_stream;
}

pub fn main(init: std.process.Init) !void {
    // Assign first, then report: customLogFn filters against runtime_log_level, so a
    // line logged inside getDefaultLogLevel is judged by the level it is about to
    // replace and disappears at exactly the setting that asked to see it.
    runtime_log_level = Config.getDefaultLogLevel(&init);
    log.debug("LOG_LEVEL → runtime log level: {s}", .{@tagName(runtime_log_level)});
    const io = init.io;
    const IS_DEBUG = builtin.mode == .Debug;

    var gpa: std.heap.DebugAllocator(.{}) = .init;
    const allocator = if (IS_DEBUG) gpa.allocator() else std.heap.c_allocator;

    defer if (IS_DEBUG) {
        _ = gpa.detectLeaks();
    };

    // CDC event allocator
    // Debug: DebugAllocator is thread-safe internally in Zig 0.16
    // Release: Use c_allocator directly (arena without reset was growing infinitely)
    const event_alloc = if (IS_DEBUG)
        gpa.allocator()
    else
        std.heap.c_allocator;

    // Snapshot allocator (used by snapshot_listener for chunk processing)
    // Debug: Use GPA for leak detection
    // Release: Use c_allocator for better performance
    const snap_base_alloc = if (IS_DEBUG) allocator else std.heap.c_allocator;

    // Parse command-line arguments and build runtime config.
    // --help prints usage and exits 0; anything unparseable exits non-zero.
    const parsed = args.Args.parseArgs(&init) catch |err| switch (err) {
        error.HelpRequested => return,
        else => return err,
    };
    const parsed_args = parsed.args;
    var runtime_config = parsed.runtime_config;

    // Every wire name, read from disk before anything connects. A missing key stops the
    // bridge here, with the key named — the same guarantee the compile-time version gave,
    // moved to where an operator error belongs. TOPOLOGY_PATH overrides the location for
    // a deployment that does not run from the directory holding the file.
    const topology_path = parsed_args.topology_path;
    var topology_owned = try topology_mod.load(allocator, io, topology_path);
    defer topology_owned.deinit();
    runtime_config.topology = topology_owned.topology;
    log.info("Topology: \x1b[1m {s} \x1b[0m (read at startup)", .{topology_path});

    // Create null-terminated versions for C APIs (kept alive for entire program)
    const slot_name_z = try allocator.dupeZ(u8, parsed_args.slot_name);
    defer allocator.free(slot_name_z);
    const pub_name_z = try allocator.dupeZ(u8, parsed_args.publication_name);
    defer allocator.free(pub_name_z);

    log.info("▶️ Starting CDC Bridge with parameters:\n", .{});
    log.info("Publication name: \x1b[1m {s} \x1b[0m", .{parsed_args.publication_name});
    log.info("Slot name: \x1b[1m {s} \x1b[0m", .{parsed_args.slot_name});
    log.info("HTTP port: \x1b[1m {d} \x1b[0m", .{parsed_args.http_port});
    log.info("Encoding format: \x1b[1m {s} \x1b[0m", .{@tagName(parsed_args.encoding_format)});
    log.info("Streams: \x1b[1m {s}, {s}, {s}, {s} \x1b[0m (from {s})", .{
        runtime_config.topology.stream_cdc,
        runtime_config.topology.stream_init,
        runtime_config.topology.stream_requests,
        runtime_config.topology.stream_mutations,
        topology_path,
    });

    // Register signal handlers for graceful shutdown
    const empty_mask = std.mem.zeroes(posix.sigset_t);
    const sigaction = posix.Sigaction{
        .handler = .{ .handler = handleShutdown },
        .mask = empty_mask,
        .flags = 0,
    };
    posix.sigaction(posix.SIG.INT, &sigaction, null); // Ctrl+C
    posix.sigaction(posix.SIG.TERM, &sigaction, null); // kill command
    log.info("👋 Press \x1b[1m Ctrl+C \x1b[0m to stop gracefully\n", .{});

    // === Initialize metrics
    var metrics = metrics_mod.Metrics.init();

    // === Start thread: HTTP server (at final memory location)
    var http_srv = try initHttpServer(
        allocator,
        parsed_args.http_port,
        &should_stop,
        &metrics,
        null,
    );
    // Start HTTP server thread AFTER http_srv is at its final memory location
    try http_srv.start();
    defer http_srv.join();
    defer http_srv.deinit();

    // Registered *after* the joins above so it runs *before* them when a later `try`
    // fails: Zig unwinds defers in reverse registration order, and those threads only
    // leave their loops when should_stop is set. Without this, a startup failure — a
    // NATS_URL that does not resolve, say — exits into `join()` and hangs there until
    // someone presses Ctrl-C, which then prints the original error as if the user had
    // caused it. Idempotent, so one per started thread is fine.
    errdefer should_stop.store(true, .seq_cst);

    // PostgreSQL connection configuration from RuntimeConfig
    var pg_config = pg_conn.PgConf.from_runtime_config(&runtime_config);

    // Initialize replication: create slot + verify publication
    var replication_ctx = try initReplication(
        allocator,
        &pg_config,
        slot_name_z,
        pub_name_z,
    );
    defer replication_ctx.deinit();

    // === Parse transition rules from environment variable
    // Format: "table1:col1,col2;table2:col3,col4"
    // Example: "users:status,kyc_level;orders:state,payment_status"
    var transition_rules = try args.Args.parseTransitionRules(allocator, &init);
    defer args.Args.deinitTransitionRules(&transition_rules, allocator);

    // Which column carries the version, per table. Same grammar as TRANSITION_RULES.
    var sync_rules = try args.Args.parseSyncRules(allocator, &init);
    defer args.Args.deinitTransitionRules(&sync_rules, allocator);
    const default_version_column = init.minimal.environ.getPosix("SYNC_VERSION_COLUMN") orelse
        Config.Sync.default_version_column;

    // Shared between preflight (boot) and the DDL path (runtime): both decide a table
    // has no primary key, and the mutation path must honour either verdict.
    var refused = refused_tables.Registry.init(allocator);
    defer refused.deinit();

    // OID → typtype for types the CDC decoder's switch does not cover. Populated from
    // DDL events and from the boot schema pass, and never invalidated: an OID outlives
    // nothing, so a stale entry describes a type that can no longer reach the wire.
    var type_registry_inst = type_registry.Registry.init(allocator);
    defer type_registry_inst.deinit();

    // Validate the publication before any thread exists. Preflight only needs Postgres,
    // and under STRICT_TABLES it returns an error — bailing out after the worker threads
    // start would unwind into `defer join()` on threads nobody signalled, hanging the
    // process instead of exiting it.
    _ = preflight.run(
        allocator,
        &pg_config,
        parsed_args.publication_name,
        &transition_rules,
        &sync_rules,
        default_version_column,
        runtime_config.strict_tables,
        &refused,
    ) catch |err| switch (err) {
        // STRICT_TABLES asked us to stop; that verdict must reach main, not be
        // swallowed as "the check could not run". Signal first: unwinding runs
        // `defer http_srv.join()`, and that thread only leaves its accept loop when
        // should_stop is set — returning without it exits into a hang, not an exit.
        error.RefusedTables => {
            should_stop.store(true, .seq_cst);
            return err;
        },
        else => blk: {
            log.warn("⚠️  Preflight check failed to run: {}", .{err});
            break :blk preflight.Summary{};
        },
    };

    // === Start thread: WAL lag monitor
    const wal_monitor_config = wal_monitor.WalConfig{
        .pg_config = &pg_config,
        .slot_name = parsed_args.slot_name,
        .check_interval_seconds = 30,
    };
    var wal_mon = wal_monitor.WalMonitor.init(
        allocator,
        &metrics,
        wal_monitor_config,
        &should_stop,
    );
    try wal_mon.start();
    defer wal_mon.join();
    defer wal_mon.deinit();
    errdefer should_stop.store(true, .seq_cst);

    // === Connect to NATS JetStream
    //
    // Resolved once, here, and passed to the publisher, the snapshot listener and the
    // mutation listener alike. They used to derive it independently — see
    // `Config.Nats.Endpoint` for the split-brain that produced.
    const nats_endpoint = Config.Nats.Endpoint.resolve(&runtime_config) catch |err| {
        log.err("🔴 NATS_URL is not a usable address ({s}) — expected nats://[user:pass@]host[:port]", .{@errorName(err)});
        return err;
    };
    log.info("NATS endpoint: {s}:{d} (from {s})", .{
        nats_endpoint.host,
        nats_endpoint.port,
        if (runtime_config.nats_url != null) "NATS_URL" else "NATS_HOST",
    });

    var publisher = try initNatsPublisher(allocator, &metrics, nats_endpoint, io);
    defer publisher.deinit();

    // What the server will accept, logged once. The check that acts on it runs below,
    // just before the slab is allocated — this is the observation, that is the decision.
    if (publisher.js) |*js| {
        if (nats_publisher.serverMaxPayload(js)) |max_payload| {
            log.info("NATS max_payload: {d} KB (server-advertised); per-event buffer: {d} KB (BASE_BUF={d})", .{
                max_payload / 1024,
                (@as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2)) / 1024,
                runtime_config.event_data_buffer_log2,
            });
        } else {
            log.debug("NATS server did not advertise max_payload; cannot check it against BASE_BUF", .{});
        }
    }

    // Make publisher available to HTTP server for stream management
    http_srv.nats_publisher = &publisher;
    http_srv.refused = &refused;

    // Initialize schema cache for tracking relation_id changes
    var schema_cache = schema_cache_mod.SchemaCache.init(allocator);
    defer schema_cache.deinit();
    log.debug("Schema cache initialized\n", .{});

    // Publish initial schemas to INIT stream (only for monitored tables)
    // Store monitored tables for validation (used by schema changes and snapshot requests)
    const monitored_tables = replication_ctx.tables;

    // === Start thread: snapshot listener
    log.info("Starting snapshot listener thread...", .{});
    var snap_listener = snapshot_listener.SnapshotListener.init(
        snap_base_alloc,
        &pg_config,
        &should_stop,
        monitored_tables,
        &refused,
        parsed_args.encoding_format,
        &runtime_config,
        io,
        nats_endpoint,
        &boot_fatal,
    );
    try snap_listener.start();
    defer snap_listener.join();
    defer snap_listener.deinit();
    errdefer should_stop.store(true, .seq_cst);
    log.info("✅ Snapshot listener thread started\n", .{});

    // === Start thread: mutation listener (only if an ingress role is configured)
    //
    // The write path runs under its own role, so enabling it can never widen the read
    // path's privileges. No writer configured means no listener — not a fallback to
    // `bridge_reader`, which has REPLICATION and is exactly what must not be doing
    // client-driven writes.
    var writer_config = pg_conn.PgConf.writer_from_runtime_config(&runtime_config);
    var mut_listener: ?*mutation_listener.MutationListener = null;
    if (writer_config) |*wc| {
        log.info("Starting mutation listener thread (role: {s})...", .{wc.role});
        mut_listener = try mutation_listener.MutationListener.init(
            allocator,
            wc,
            nats_endpoint,
            &runtime_config.topology,
            &sync_rules,
            default_version_column,
            io,
            &should_stop,
        );
        try mut_listener.?.start();
        log.info("✅ Mutation listener thread started\n", .{});
    } else {
        log.info("ℹ️  Ingress disabled: no POSTGRES_WRITER_USER/_PASSWORD configured", .{});
    }
    defer if (mut_listener) |m| m.deinit();
    defer if (mut_listener) |m| m.join();
    errdefer should_stop.store(true, .seq_cst);

    // ─── two startup checks on the ring buffer, before a byte of it is allocated ─────
    //
    // Both exist because their failure modes are silent and late: an OOM kill under load,
    // and a row that packs successfully and is then refused by the server forever.
    {
        const event_buf_bytes = @as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2);
        const slab_bytes = event_buf_bytes * runtime_config.batch_ring_buffer_size;

        // #1 — will the slab fit in the memory this process actually has?
        const limit = utils.memoryLimitBytes();
        if (limit == 0) {
            log.debug("Cannot determine the memory limit; skipping the slab sizing check", .{});
        } else if (slab_bytes * 2 > limit) {
            // Half the limit is the line: the slab is not the only thing resident — libpq
            // buffers, the NATS client, the snapshot encode buffer and the arenas all sit
            // beside it — and a slab past half leaves no room for the work it exists to do.
            log.err(
                "🔴 The event slab would be {d} MB (BASE_BUF={d} → {d} KB × RING_BUFFER_COUNT={d}), against a {d} MB memory limit. It is pre-allocated at startup, so this is an OOM kill under load rather than a slow degradation. Halve RING_BUFFER_COUNT for each step you raise BASE_BUF; see README 'Sizing BASE_BUF and RING_BUFFER_COUNT'.",
                .{ slab_bytes / 1024 / 1024, runtime_config.event_data_buffer_log2, event_buf_bytes / 1024, runtime_config.batch_ring_buffer_size, limit / 1024 / 1024 },
            );
            return error.SlabExceedsMemory;
        } else {
            log.info("Event slab: {d} MB of a {d} MB limit ({d}%)", .{
                slab_bytes / 1024 / 1024,
                limit / 1024 / 1024,
                slab_bytes * 100 / limit,
            });
        }

        // #2 — can a row that fills the buffer ever be published?
        //
        // Was a warning. It is an error now because the failure it describes is the same
        // shape as the two size-cap bugs found on 2026-08-16: the row packs, the publish is
        // refused, and nothing in the data path says why. A buffer larger than the server
        // will accept is not a tuning choice, it is a configuration that cannot work.
        if (publisher.js) |*js| {
            if (nats_publisher.serverMaxPayload(js)) |max_payload| {
                if (event_buf_bytes + Config.Nats.payload_envelope_margin_bytes > max_payload) {
                    log.err(
                        "🔴 BASE_BUF={d} allows a {d} KB row, but this NATS server accepts at most {d} KB per message and the envelope (subject, headers, column keys, batch framing) needs roughly {d} KB more. A row in that range packs successfully and is then REJECTED at publish time. Lower BASE_BUF to {d} or raise max_payload in nats-server.conf.",
                        .{
                            runtime_config.event_data_buffer_log2,
                            event_buf_bytes / 1024,
                            max_payload / 1024,
                            Config.Nats.payload_envelope_margin_bytes / 1024,
                            std.math.log2_int(u64, max_payload - Config.Nats.payload_envelope_margin_bytes),
                        },
                    );
                    return error.EventBufferExceedsMaxPayload;
                }
            }
        }
    }

    // === Start thread: CDC async publisher (heap-allocated for stable address)
    // Use c_allocator (thread-safe) for cross-thread allocations:
    // Main thread allocates CDC event data, flush thread deallocates it
    // Returns *BatchPublisher to ensure memory stability for flush thread
    const batch_pub = try initBatchPublisher(
        event_alloc,
        &publisher,
        parsed_args.encoding_format,
        &metrics,
        &runtime_config,
    );
    // Start flush thread - batch_pub is now at stable heap address
    try batch_pub.start();
    defer batch_pub.join();
    errdefer should_stop.store(true, .seq_cst);
    defer batch_pub.deinit(); // Will free the heap-allocated BatchPublisher itself

    // === Initialize EventProcessor (in this main thread, the CDC processing)
    // EventProcessor enqueues to the SPSC queue that BatchPublisher consumes
    var event_proc = event_processor.EventProcessor.init(
        event_alloc,
        batch_pub,
        &metrics,
        &transition_rules,
        &pg_config,
        &refused,
        &type_registry_inst,
        &runtime_config.topology,
        &sync_rules,
        default_version_column,
    );

    // Publish boot schemas to NATS KV for all monitored tables
    try event_proc.publishBootSchemas(allocator, monitored_tables);

    const batch_config = batch_publisher.BatchConfig{
        .max_events = Config.Batch.max_events,
        .max_wait_ms = Config.Batch.max_age_ms,
        .max_payload_bytes = Config.Batch.max_payload_bytes,
    };
    log.info("✅ Async batch publishing enabled (max {d} events or {d}ms or {d}KB)\n", .{
        batch_config.max_events,
        batch_config.max_wait_ms,
        batch_config.max_payload_bytes / 1024,
    });

    // Connect to replication stream
    var pg_stream = try initReplicationStream(
        allocator,
        &pg_config,
        slot_name_z,
        pub_name_z,
    );
    defer pg_stream.deinit();

    // Mark as connected in metrics
    metrics.setConnected(true);

    log.info("ℹ️ CDC subject pattern: \x1b[1m {s}.<table>.<operation> \x1b[0m", .{runtime_config.topology.subject_cdc_prefix});

    // <--- Metrics setup
    const present = @as(i64, @intCast(c.time(null)));
    var msg_count: u32 = 0;
    var cdc_events: u32 = 0;
    var last_lsn: u64 = 0;
    var last_ack_lsn: u64 = 0; // Track last acknowledged LSN for keepalives
    var last_keepalive_time = present; // Track last keepalive sent

    // Status update batching to reduce PostgreSQL round trips
    var bytes_since_ack: u64 = 0; // Track bytes processed since last ack
    // Read configuration values once
    const status_update_interval_ms: i64 = Config.Nats.status_update_interval_ms;
    const keepalive_interval_seconds: i64 = 10; // Send keepalives every 10s if idle
    var last_status_update_time = utils.getMilliTimestamp();
    const status_update_byte_threshold: u64 = Config.Nats.status_update_byte_threshold; // Or after 1MB of data (better than message count)

    // Periodic structured metric logging for Grafana Alloy/Loki
    var last_metric_log_time = present;
    const metric_log_interval_seconds: i64 = Config.Metrics.metric_log_interval_seconds; // Log metrics every 15 seconds

    // Main loop iteration counter for periodic background tasks (e.g. time-based ACKs)
    var loop_iterations: u32 = 0;

    // ---- WAL loop profile, reset every metrics interval -----------------------
    // The queue-usage gauge tells you whether the *flush* side is backed up; nothing
    // told you where the *reading* side spends its time. These four do, and they are
    // what separates "CPU-bound decoding" from "sleeping between messages".
    var prof_iters: u64 = 0; // outer loop iterations
    var prof_idle: u64 = 0; // iterations where PQgetCopyData had nothing
    var prof_recv_ns: u64 = 0; // time inside receiveMessage (libpq + parse)
    var prof_proc_ns: u64 = 0; // time turning a message into ring-buffer slots
    // Process CPU at the last report, so the log can print a percentage rather than a
    // total. Covers every thread, not just this loop — `cpu=250%` means 2.5 cores busy.
    var prof_cpu_ns: u64 = utils.cpuTimeNanos();
    var prof_wall_ms: i64 = utils.getMilliTimestamp();
    // --->

    // Per-transaction slot scratchpad: holds ring-buffer indices for the current
    // in-flight transaction. Nothing is released to the background publisher until
    // the matching .commit arrives.
    //
    // Safety invariant: tx_slots_buf.len < ring_buffer_capacity.
    // If a single transaction claimed every ring buffer slot before .commit arrived,
    // acquireAndFillSlot would spin forever because the background thread cannot free
    // anything from an empty pending_events queue. By capping the tracker one below
    // the ring buffer size the overflow guard always fires with at least one slot
    // still free, keeping the background thread able to make progress.
    const ring_buffer_capacity = runtime_config.batch_ring_buffer_size;
    const max_tx_rows = ring_buffer_capacity - 1;
    const tx_slots_buf: []u32 = try allocator.alloc(u32, max_tx_rows);
    defer allocator.free(tx_slots_buf);
    var tx_slots_count: usize = 0;
    std.debug.assert(ring_buffer_capacity > tx_slots_buf.len); // invariant: never exhaust the pool
    log.info("Ring buffer capacity: {d} slots, transaction row limit: {d}", .{ ring_buffer_capacity, max_tx_rows });

    // Track relation metadata (table info)
    var relation_map = std.AutoHashMap(u32, pgoutput.RelationMessage).init(allocator);
    defer {
        var it = relation_map.valueIterator();
        while (it.next()) |rel| {
            var r = rel.*;
            r.deinit(allocator);
        }
        relation_map.deinit();
    }

    // Create arena allocator for messages parsing
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // Run until graceful shutdown signal received
    while (!should_stop.load(.seq_cst)) {
        loop_iterations +%= 1;
        const now_s = @as(i64, @intCast(c.time(null)));
        const now_ms = utils.getMilliTimestamp();

        // Flush pending status updates if time threshold reached
        if (now_ms - last_status_update_time >= status_update_interval_ms) {
            const confirmed_lsn = batch_pub.getLastConfirmedLsn();
            if (confirmed_lsn > last_ack_lsn) {
                try pg_stream.sendStatusUpdate(confirmed_lsn);
                log.debug("✓ ACKed to PostgreSQL: LSN {x} (time-based)", .{confirmed_lsn});
                last_ack_lsn = confirmed_lsn;
                bytes_since_ack = 0;
                last_keepalive_time = now_s;
            }
            last_status_update_time = now_ms;
        }

        // Unconditional heartbeat. PostgreSQL terminates a walsender that stays silent
        // for `wal_sender_timeout`, and it does NOT keep asking: during a burst it stops
        // sending keepalives of its own and streams xlogdata continuously, so a receiver
        // that only answers `reply_requested` can go quiet for minutes and be killed
        // mid-stream.
        //
        // Sent even when `last_ack_lsn` is still 0 — an LSN of 0 means "I have confirmed
        // nothing yet", which is true and harmless, and PostgreSQL prunes nothing on the
        // strength of it. The previous `if (last_ack_lsn > 0)` guard meant a bridge that
        // had not yet confirmed anything — NATS down at boot, or a long first
        // transaction — sent no heartbeat at all, which is exactly when it most needs to
        // say it is alive.
        if (now_s - last_keepalive_time >= keepalive_interval_seconds) {
            try pg_stream.sendStatusUpdate(last_ack_lsn);
            last_keepalive_time = now_s;
            log.debug("Sent keepalive (LSN: {x})", .{last_ack_lsn});
        }

        // Periodic structured metric logging for Alloy/Loki
        if (now_s - last_metric_log_time >= metric_log_interval_seconds) {
            const snap = try metrics.snapshot(allocator);
            defer allocator.free(snap.current_lsn_str);

            // Structured log format parseable by Grafana Alloy
            // A refusal logged once at boot has scrolled away by the time anyone
            // wonders where the rows went. Repeat it with the running drop count.
            refused.logStatus();

            // Per-interval loop profile. `idle` counts iterations that found nothing and
            // therefore slept 1ms: if it dominates `iters`, the reader is waiting, not
            // working, and throughput is capped by the sleep rather than by decoding.
            const cpu_now = utils.cpuTimeNanos();
            const wall_now = utils.getMilliTimestamp();
            const wall_delta_ms: u64 = @intCast(@max(wall_now - prof_wall_ms, 1));
            const cpu_pct = (cpu_now -| prof_cpu_ns) / std.time.ns_per_ms * 100 / wall_delta_ms;

            log.info("LOOP iters={d} idle={d} recv_ms={d} proc_ms={d} cpu={d}%", .{
                prof_iters,
                prof_idle,
                prof_recv_ns / std.time.ns_per_ms,
                prof_proc_ns / std.time.ns_per_ms,
                cpu_pct,
            });
            prof_iters = 0;
            prof_idle = 0;
            prof_recv_ns = 0;
            prof_proc_ns = 0;
            prof_cpu_ns = cpu_now;
            prof_wall_ms = wall_now;

            log.info("METRICS uptime={d} wal_messages={d} cdc_events={d} lsn={s} connected={d} pg_reconnects={d} nats_reconnects={d} lag_bytes={d} slot_active={d}", .{
                snap.uptime_seconds,
                snap.wal_messages_received,
                snap.cdc_events_published,
                snap.current_lsn_str,
                if (snap.is_connected) @as(u8, 1) else @as(u8, 0),
                snap.reconnect_count,
                snap.nats_reconnect_count,
                snap.wal_lag_bytes,
                if (snap.slot_active) @as(u8, 1) else @as(u8, 0),
            });

            last_metric_log_time = now_s;
        }

        // Check for fatal NATS errors (e.g., reconnection timeout exceeded)
        if (batch_pub.hasFatalError()) {
            log.err("🔴 FATAL ERROR: NATS reconnection failed - shutting down bridge to prevent WAL overflow", .{});
            break;
        }

        // Zero-allocation ring buffer: no need to reclaim events!
        // Slots are automatically returned to free_slots queue by flush thread

        prof_iters += 1;
        const t_recv0 = utils.nanoTimestamp();
        if (pg_stream.receiveMessage()) |maybe_msg| {
            prof_recv_ns += utils.nanoTimestamp() - t_recv0;
            if (maybe_msg) |wal_msg_val| {
                const t_proc0 = utils.nanoTimestamp();
                defer prof_proc_ns += utils.nanoTimestamp() - t_proc0;

                var wal_msg = wal_msg_val;
                defer wal_msg.deinit(allocator);

                msg_count += 1;
                metrics.incrementWalMessages();

                // Handle keepalive messages - reply immediately if requested
                if (wal_msg.type == .keepalive) {
                    if (wal_msg.reply_requested) {
                        // PostgreSQL is requesting a reply - send status update immediately
                        const reply_lsn = if (last_ack_lsn > 0) last_ack_lsn else wal_msg.wal_end;
                        try pg_stream.sendStatusUpdate(reply_lsn);
                        last_keepalive_time = @as(i64, @intCast(c.time(null)));
                        log.debug("Replied to keepalive request (LSN: {x})", .{reply_lsn});
                    }
                    continue; // Don't process keepalives further
                }

                // Parse and publish pgoutput messages
                if (wal_msg.type == .xlogdata and wal_msg.payload.len > 0) {
                    // Reset arena for this message (retains capacity for efficiency)
                    // reset allocations from previous message while keeping the memory buffer
                    _ = arena.reset(.retain_capacity);
                    const arena_allocator = arena.allocator();

                    // Parse messages with arena allocator
                    // Relations use main allocator since they persist in the map
                    var parser = pgoutput.Parser.init(arena_allocator, wal_msg.payload);
                    if (parser.parse()) |pg_msg| {
                        // arena.deinit() handles everything

                        switch (pg_msg) {
                            .relation => |rel| {
                                // "what the columns are"
                                // Relations persist in the map, so clone with main allocator
                                // (arena will be destroyed at end of scope)
                                const cloned_rel_ptr = try rel.clone(allocator);
                                // NOTE: Don't defer destroy - relation is stored in map and freed on:
                                // 1. Replacement by new relation (old_rel.deinit below)
                                // 2. Program exit (relation_map cleanup at line 385-391)

                                // Check if schema changed for a monitored table
                                const schema_changed = try schema_cache.hasChanged(rel.name, rel.relation_id);

                                if (schema_changed) {
                                    log.info("🔔 Relation mapping updated for table '{s}' (relation_id={d})", .{ rel.name, rel.relation_id });
                                    // We no longer publish schemas synchronously here.
                                    // Schemas are updated via DDL events intersecting the WAL stream
                                    // and pushed through the SPSC ring buffer for strict ordering.
                                }

                                // Update the current relation map HashMap
                                const result = try relation_map.fetchPut(
                                    cloned_rel_ptr.relation_id,
                                    cloned_rel_ptr.*,
                                );
                                // If relation already exists, free the old one
                                if (result) |old_entry| {
                                    var old_rel = old_entry.value;
                                    old_rel.deinit(allocator);
                                }
                                // Free ONLY the pointer wrapper, NOT the nested data
                                // The map now owns the nested data (namespace, name, columns)
                                allocator.destroy(cloned_rel_ptr);

                                log.debug("RELATION: {s}.{s} (id={d}, {d} columns)", .{ rel.namespace, rel.name, rel.relation_id, rel.columns.len });
                            },
                            .begin => |b| {
                                // Defensive: discard any stale slots from a broken prior tx.
                                for (tx_slots_buf[0..tx_slots_count]) |s| event_proc.discardSlot(s);
                                tx_slots_count = 0;
                                log.debug("BEGIN: xid={d} lsn={x}", .{ b.xid, b.final_lsn });
                            },
                            .insert => |ins| {
                                if (relation_map.get(ins.relation_id)) |rel| {
                                    if (tx_slots_count >= tx_slots_buf.len) {
                                        log.err("Transaction overflow: exceeds {d} row limit — discarding entire in-flight transaction", .{tx_slots_buf.len});
                                        for (tx_slots_buf[0..tx_slots_count]) |s| event_proc.discardSlot(s);
                                        tx_slots_count = 0;
                                        return error.TransactionOverflow;
                                    }
                                    if (std.mem.eql(u8, rel.name, "zebridge_ddl_events")) {
                                        if (try event_proc.packDdlToSlot(arena_allocator, rel, ins.tuple_data, wal_msg.wal_end)) |slot_idx| {
                                            tx_slots_buf[tx_slots_count] = slot_idx;
                                            tx_slots_count += 1;
                                            cdc_events += 1;
                                        }
                                    } else if (!event_proc.refused.shouldDrop(rel.name)) {
                                        // Refused: no schema was published for this table, so a
                                        // client receiving the row would have nowhere to put it.
                                        const slot_idx = try event_proc.packMutationToSlot(
                                            arena_allocator,
                                            rel,
                                            ins.tuple_data,
                                            null,
                                            "INSERT",
                                            wal_msg.wal_end,
                                        );
                                        tx_slots_buf[tx_slots_count] = slot_idx;
                                        tx_slots_count += 1;
                                        cdc_events += 1;
                                    }
                                }
                            },
                            .update => |upd| blk_upd: {
                                if (relation_map.get(upd.relation_id)) |rel| {
                                    // Housekeeping on the DDL tracker is not client data: only its
                                    // INSERTs carry schema events (handled in the .insert prong).
                                    // Pruning old rows produces DELETEs that must never surface as
                                    // cdc.zebridge_ddl_events.* to consumers.
                                    if (std.mem.eql(u8, rel.name, "zebridge_ddl_events")) break :blk_upd;
                                    if (event_proc.refused.shouldDrop(rel.name)) break :blk_upd;
                                    if (tx_slots_count >= tx_slots_buf.len) {
                                        log.err("Transaction overflow: exceeds {d} row limit — discarding entire in-flight transaction", .{tx_slots_buf.len});
                                        for (tx_slots_buf[0..tx_slots_count]) |s| event_proc.discardSlot(s);
                                        tx_slots_count = 0;
                                        return error.TransactionOverflow;
                                    }
                                    const slot_idx = try event_proc.packMutationToSlot(
                                        arena_allocator,
                                        rel,
                                        upd.new_tuple,
                                        upd.old_tuple,
                                        "UPDATE",
                                        wal_msg.wal_end,
                                    );
                                    tx_slots_buf[tx_slots_count] = slot_idx;
                                    tx_slots_count += 1;
                                    cdc_events += 1;
                                }
                            },
                            .delete => |del| blk_del: {
                                if (relation_map.get(del.relation_id)) |rel| {
                                    // Housekeeping on the DDL tracker is not client data: only its
                                    // INSERTs carry schema events (handled in the .insert prong).
                                    // Pruning old rows produces DELETEs that must never surface as
                                    // cdc.zebridge_ddl_events.* to consumers.
                                    if (std.mem.eql(u8, rel.name, "zebridge_ddl_events")) break :blk_del;
                                    if (event_proc.refused.shouldDrop(rel.name)) break :blk_del;
                                    if (tx_slots_count >= tx_slots_buf.len) {
                                        log.err("Transaction overflow: exceeds {d} row limit — discarding entire in-flight transaction", .{tx_slots_buf.len});
                                        for (tx_slots_buf[0..tx_slots_count]) |s| event_proc.discardSlot(s);
                                        tx_slots_count = 0;
                                        return error.TransactionOverflow;
                                    }
                                    const slot_idx = try event_proc.packMutationToSlot(
                                        arena_allocator,
                                        rel,
                                        del.old_tuple,
                                        null,
                                        "DELETE",
                                        wal_msg.wal_end,
                                    );
                                    tx_slots_buf[tx_slots_count] = slot_idx;
                                    tx_slots_count += 1;
                                    cdc_events += 1;
                                }
                            },
                            .commit => |commit| {
                                // Transaction confirmed — release all buffered slots to the publisher.
                                // On error, recycle remaining unreleased slots rather than leaking them.
                                var released: usize = 0;
                                while (released < tx_slots_count) : (released += 1) {
                                    event_proc.releaseSlotToQueue(tx_slots_buf[released]) catch |err| {
                                        log.err("Failed to release slot to publisher: {} — discarding {d} remaining slots", .{ err, tx_slots_count - released });
                                        for (tx_slots_buf[released..tx_slots_count]) |s| event_proc.discardSlot(s);
                                        break;
                                    };
                                }

                                const events_released_in_tx = tx_slots_count;
                                tx_slots_count = 0;

                                if (commit.commit_lsn != last_lsn) {
                                    const lsn_diff = commit.commit_lsn - last_lsn;
                                    log.debug("COMMIT: lsn={x} (delta: +{d}, {d} events released)", .{ commit.commit_lsn, lsn_diff, events_released_in_tx });
                                    last_lsn = commit.commit_lsn;
                                } else {
                                    log.debug("COMMIT: lsn={x}", .{commit.commit_lsn});
                                }

                                // Tell publisher to flush immediately rather than waiting for max_age_ms
                                batch_pub.forceFlush();
                            },
                            else => {},
                        }
                    } else |err| {
                        log.warn("Failed to parse pgoutput message: {}", .{err});
                    }
                }

                // Track the latest WAL position we've received and update metrics
                if (wal_msg.wal_end > 0) {
                    bytes_since_ack += wal_msg.payload.len;
                    metrics.updateLsn(wal_msg.wal_end);
                }

                // Send buffered status update if we hit byte threshold
                if (bytes_since_ack >= status_update_byte_threshold) {
                    // Only read atomic LSN when we're about to ACK
                    const confirmed_lsn = batch_pub.getLastConfirmedLsn();
                    if (confirmed_lsn > last_ack_lsn) {
                        try pg_stream.sendStatusUpdate(confirmed_lsn);
                        log.debug("✓ ACKed to PostgreSQL: LSN {x} (NATS confirmed, {d} bytes)", .{ confirmed_lsn, bytes_since_ack });
                        last_ack_lsn = confirmed_lsn;
                        bytes_since_ack = 0;
                        last_status_update_time = now_ms;
                        last_keepalive_time = now_s; // Reset keepalive timer
                    }
                }
            } else {
                // No message available - idle path
                // Sleep 1 ms first to avoid busy-waiting and increase throughput
                prof_idle += 1;
                utils.sleep(1 * std.time.ns_per_ms);
            }
        } else |err| {
            if (err == error.StreamEnded) {
                log.info("Stream ended gracefully", .{});
                break;
            }

            // Handle connection errors by reconnecting
            log.warn("Connection lost: {}", .{err});
            metrics.setConnected(false);

            // Discard any slots buffered for the in-flight transaction.
            // The stream broke before .commit arrived — those rows must not reach NATS.
            if (tx_slots_count > 0) {
                log.warn("Discarding {d} unpublished slots from incomplete transaction", .{tx_slots_count});
                for (tx_slots_buf[0..tx_slots_count]) |s| event_proc.discardSlot(s);
                tx_slots_count = 0;
            }

            log.info("Attempting to reconnect in 2 seconds...", .{});

            // Zero-allocation ring buffer: no reclaim needed

            utils.sleep(2000 * std.time.ns_per_ms); // 2 seconds

            // Get latest LSN and reconnect
            const reconnect_lsn = wal_monitor.getCurrentLSN(allocator, &pg_config) catch |lsn_err| {
                log.err("Failed to get LSN for reconnect: {}", .{lsn_err});
                utils.sleep(Config.Retry.pg_reconnect_delay_seconds * std.time.ns_per_s);
                continue;
            };
            defer allocator.free(reconnect_lsn);

            // Clean up old connection before reconnecting
            pg_stream.deinit();

            // Reconnect to replication stream
            pg_stream.connect() catch |conn_err| {
                log.err("Failed to reconnect: {}", .{conn_err});
                utils.sleep(Config.Retry.pg_reconnect_delay_seconds * std.time.ns_per_s);
                continue;
            };

            pg_stream.startStreaming(reconnect_lsn) catch |stream_err| {
                log.err("Failed to restart streaming: {}", .{stream_err});
                utils.sleep(Config.Retry.pg_reconnect_delay_seconds * std.time.ns_per_s);
                continue;
            };

            log.info("✓ Reconnected to WAL stream at LSN {s}", .{reconnect_lsn});
            metrics.recordReconnect();
            metrics.setConnected(true);
        }
    }

    // Graceful shutdown: signal flush thread to stop and wait for completion
    log.info("\n🛑 Shutdown initiated - signaling flush thread to stop...", .{});

    // CRITICAL: Signal should_stop BEFORE waiting for completion
    batch_pub.should_stop.store(true, .seq_cst);

    const initial_queue_len = batch_pub.pending_events.len();
    if (initial_queue_len > 0) {
        log.info("📤 Queue has {d} events waiting to be published...", .{initial_queue_len});
    }

    const shutdown_timeout_seconds = 30;
    const start_time = @as(i64, @intCast(c.time(null)));
    var last_log_time: i64 = 0;

    // Wait for flush thread to complete (both queue empty AND final flush done)
    while (!batch_pub.isFlushComplete()) {
        const elapsed = @as(i64, @intCast(c.time(null))) - start_time;
        if (elapsed > shutdown_timeout_seconds) {
            const remaining = batch_pub.pending_events.len();
            log.warn("⚠️ Shutdown timeout reached - {d} events may not have been published", .{remaining});
            break;
        }

        // Log progress every second
        if (elapsed - last_log_time >= 1) {
            const remaining = batch_pub.pending_events.len();
            if (remaining > 0 or !batch_pub.isFlushComplete()) {
                log.info("📊 Draining: {d} events in queue, flush thread working...", .{remaining});
            }
            last_log_time = elapsed;
        }

        utils.sleep(100 * std.time.ns_per_ms); // 100ms
    }

    if (batch_pub.isFlushComplete()) {
        log.info("✅ Flush thread completed - all events published to NATS", .{});
    }

    // Send final ACK to PostgreSQL with last confirmed LSN
    const final_lsn = batch_pub.getLastConfirmedLsn();
    if (final_lsn > last_ack_lsn) {
        pg_stream.sendStatusUpdate(final_lsn) catch |err| {
            log.warn("Failed to send final ACK: {}", .{err});
        };
        log.info("📨 Final ACK sent to PostgreSQL: LSN {x}", .{final_lsn});
    }

    log.info("\n=== Bridge Session Summary ------------------------------", .{});
    log.info("Total WAL messages received: {d}", .{msg_count});
    log.info("CDC events published to NATS: {d}", .{cdc_events});
    if (boot_fatal.load(.acquire)) {
        // Not "gracefully": a listener gave up on its first NATS connection. The FATAL
        // line above says which and why; this is what makes the shell and the
        // supervisor agree with it.
        log.err("🔴 Bridge stopped because of a startup fault — see the FATAL line above", .{});
        return error.StartupFault;
    }

    log.info("Bridge stopped gracefully\n", .{});
}
