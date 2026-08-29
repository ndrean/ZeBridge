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
const publication_mod = @import("publication.zig");
const catalogue = @import("catalogue.zig");
const generation_producer = @import("generation_producer.zig");
const mutation_listener = @import("mutation_listener.zig");
const catalog_epoch_mod = @import("catalog_epoch.zig");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const utils = @import("utils.zig");
const preflight = @import("preflight.zig");
const refused_tables = @import("refused_tables.zig");
const writable_tables = @import("writable_tables.zig");
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
    _ = @import("catalog_epoch.zig");
    _ = @import("type_registry.zig");
    _ = @import("schema_mapper.zig");
    _ = @import("spsc_queue.zig");
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
/// opposed to Ctrl+C. Only the exit code distinguishes the two for whatever
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

/// Resolve this instance's per-event column-descriptor ceiling.
///
/// `MAX_COLUMNS` (already clamped by args.zig) wins outright — set it and
/// auto-detection never runs. Otherwise: one catalog query finds the widest table
/// actually in the publication, and the result is rounded up by
/// `Config.Batch.column_headroom_rounding` so a routine `ALTER TABLE ADD COLUMN`
/// doesn't immediately need a reboot to avoid `TooManyColumns`. Anything past
/// `Config.Batch.absolute_max_columns` (PostgreSQL's own ceiling) is refused by the
/// query's own JOINs long before it could matter.
///
/// Must run after the publication exists (this queries it directly, not
/// `monitored_tables`, so it needs no "schema.table" string parsing) and before
/// `initBatchPublisher`, which allocates the columns slab this value sizes.
fn resolveMaxColumns(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publication_name: []const u8,
    override: ?u16,
) u16 {
    if (override) |v| {
        log.info("MAX_COLUMNS={d} (explicit override)", .{v});
        return v;
    }

    var standard_config = pg_config.*;
    standard_config.replication = false;

    const conn = pg_conn.connect(allocator, standard_config) catch |err| {
        log.warn("⚠️  MAX_COLUMNS auto-detection skipped: could not connect to PostgreSQL ({any}); falling back to {d}", .{
            err,
            Config.Batch.default_max_columns,
        });
        return Config.Batch.default_max_columns;
    };
    defer c.PQfinish(conn);

    // One query, not one per table: join the publication's tables straight through
    // to pg_attribute and group by relation, so the widest table's live column count
    // comes back as a single row regardless of how many tables are published.
    const query = utils.allocPrintZ(
        allocator,
        \\SELECT COALESCE(MAX(col_count), 0) FROM (
        \\  SELECT count(*) AS col_count
        \\  FROM pg_publication_tables pt
        \\  JOIN pg_namespace ns ON ns.nspname = pt.schemaname
        \\  JOIN pg_class cl ON cl.relname = pt.tablename AND cl.relnamespace = ns.oid
        \\  JOIN pg_attribute a ON a.attrelid = cl.oid AND a.attnum > 0 AND NOT a.attisdropped
        \\  WHERE pt.pubname = '{s}'
        \\  GROUP BY pt.tablename
        \\) widest;
    ,
        .{publication_name},
    ) catch {
        log.warn("⚠️  MAX_COLUMNS auto-detection skipped: out of memory building the query; falling back to {d}", .{Config.Batch.default_max_columns});
        return Config.Batch.default_max_columns;
    };
    defer allocator.free(query);

    const res = c.PQexec(conn, query.ptr);
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK or c.PQntuples(res) == 0) {
        log.warn("⚠️  MAX_COLUMNS auto-detection skipped: catalog query failed ({s}); falling back to {d}", .{
            c.PQerrorMessage(conn),
            Config.Batch.default_max_columns,
        });
        return Config.Batch.default_max_columns;
    }

    const raw = std.mem.span(c.PQgetvalue(res, 0, 0));
    const widest = std.fmt.parseInt(u32, raw, 10) catch 0;

    if (widest == 0) {
        log.warn("⚠️  MAX_COLUMNS auto-detection found no published tables; falling back to {d}", .{Config.Batch.default_max_columns});
        return Config.Batch.default_max_columns;
    }

    const chunks = std.math.divCeil(u32, widest, Config.Batch.column_headroom_rounding) catch widest;
    const headroomed: u32 = chunks * Config.Batch.column_headroom_rounding;
    const clamped: u16 = @intCast(std.math.clamp(
        headroomed,
        @as(u32, Config.Batch.min_columns),
        @as(u32, Config.Batch.absolute_max_columns),
    ));

    log.info("MAX_COLUMNS={d} (auto-detected: widest monitored table has {d} columns, rounded up to a multiple of {d})", .{
        clamped,
        widest,
        Config.Batch.column_headroom_rounding,
    });
    return clamped;
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
    metrics: *metrics_mod.Metrics,
    runtime_config: *const Config.RuntimeConfig,
    max_columns: u16,
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
        metrics,
        runtime_config,
        max_columns,
    );
}

/// Initialize HTTP server for health checks and metrics (does NOT start the thread)
/// The thread MUST be started after the server is at its final memory location
fn initHttpServer(
    allocator: std.mem.Allocator,
    io: std.Io,
    bind: []const u8,
    port: u16,
    should_stop_flag: *std.atomic.Value(bool),
    metrics: *metrics_mod.Metrics,
    publisher: ?*nats_publisher.Publisher,
) !http_server.Server {
    return try http_server.Server.init(
        allocator,
        io,
        bind,
        port,
        should_stop_flag,
        metrics,
        publisher,
    );
}

/// Hand a transaction's buffered slots to the background publisher.
///
/// Called at `.commit` for the whole transaction, and mid-transaction whenever the
/// scratchpad fills (NOTES.md finding 5) — which is what lets a transaction of any
/// size stream through at ring-buffer pace instead of killing the process.
fn releaseTxSlots(
    event_proc: anytype,
    tx_slots_buf: []u32,
    tx_slots_count: *usize,
) !void {
    var released: usize = 0;
    while (released < tx_slots_count.*) : (released += 1) {
        event_proc.releaseSlotToQueue(tx_slots_buf[released]) catch |err| {
            log.err("Failed to release slot to publisher: {} — discarding {d} remaining slots", .{ err, tx_slots_count.* - released });
            for (tx_slots_buf[released..tx_slots_count.*]) |s| event_proc.discardSlot(s);
            tx_slots_count.* = 0;
            return err;
        };
    }
    tx_slots_count.* = 0;
}

/// Register THIS instance's row-width budget in `zebridge_limits`.
///
/// The PostgreSQL-side width guard refuses rows the change feed could not carry.
/// Its budget used to be a single global row a human had to keep in step with
/// `BASE_BUF` — SECURITY.md called it "the one coupling to maintain by hand", and
/// it was duly forgotten the first time BASE_BUF changed: buffer 4 KB, table still
/// 16384, so Postgres would accept rows that suspend the table on the first CDC
/// touch. The budget is per INSTANCE (BASE_BUF is a per-process setting), so the
/// instance is what should write it, every boot, from the value it actually uses.
///
/// Runs over the READER connection through a SECURITY DEFINER function, which is
/// what lets a read-only deployment do this at all: the reader keeps SELECT +
/// REPLICATION and gains no write privilege on any table — only EXECUTE on one
/// function that writes rows describing this bridge.
///
/// Not fatal. A bridge that cannot register still replicates correctly; what it
/// loses is the guard's accuracy, so this warns loudly rather than refusing to boot.
fn registerRowWidthBudget(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    slot_name: []const u8,
    pub_name: []const u8,
    max_row_bytes: usize,
) ?usize {
    const conninfo = pg_config.connInfo(allocator, false) catch |err| {
        log.warn("⚠️  row-width budget not registered (conninfo: {}) — the guard keeps its previous value", .{err});
        return null;
    };
    defer allocator.free(conninfo);

    const conn = c.PQconnectdb(conninfo.ptr);
    defer if (conn != null) c.PQfinish(conn);
    if (conn == null or c.PQstatus(conn) != c.CONNECTION_OK) {
        log.warn("⚠️  row-width budget not registered (no connection) — the guard keeps its previous value", .{});
        return null;
    }

    // Bound as parameters, never interpolated: a slot or publication name is
    // operator input, and this is the one place a bridge writes SQL of its own.
    var slot_buf: [256]u8 = undefined;
    var pub_buf: [256]u8 = undefined;
    var bytes_buf: [32]u8 = undefined;
    const slot_z = std.fmt.bufPrintZ(&slot_buf, "{s}", .{slot_name}) catch return null;
    const pub_z = std.fmt.bufPrintZ(&pub_buf, "{s}", .{pub_name}) catch return null;
    const bytes_z = std.fmt.bufPrintZ(&bytes_buf, "{d}", .{max_row_bytes}) catch return null;
    const params = [_]?[*:0]const u8{ slot_z.ptr, pub_z.ptr, bytes_z.ptr };

    // Second column: the EFFECTIVE budget — MIN over the instances carrying anything
    // in this publication. The ingress check below uses it so the bridge stops
    // accepting writes PostgreSQL will then refuse (measured: a 3352-byte edge write
    // passed a 4096 bridge and was rejected by a 2048 trigger — correct, but a wasted
    // round trip and a confusing log).
    const res = c.PQexecParams(
        conn,
        "SELECT public.zebridge_register_limits($1, $2::name, $3::integer), " ++
            "COALESCE((SELECT MIN(max_row_bytes) FROM public.zebridge_limits), $3::integer)",
        3,
        null,
        &params[0],
        null,
        null,
        0,
    );
    defer if (res != null) c.PQclear(res);
    if (res == null or c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        log.warn(
            "⚠️  row-width budget not registered: {s} — the guard keeps its previous value, so PostgreSQL may accept rows this bridge cannot carry",
            .{c.PQerrorMessage(conn)},
        );
        return null;
    }

    const n = std.mem.span(c.PQgetvalue(res, 0, 0));
    const effective = std.fmt.parseInt(usize, std.mem.span(c.PQgetvalue(res, 0, 1)), 10) catch max_row_bytes;
    if (effective < max_row_bytes) {
        log.info(
            "📏 row-width budget registered: {d} bytes (slot '{s}', publication '{s}'), {s} guard(s) re-baked — ingress capped at {d}, the NARROWEST instance carrying these tables",
            .{ max_row_bytes, slot_name, pub_name, n, effective },
        );
    } else {
        log.info(
            "📏 row-width budget registered: {d} bytes (slot '{s}', publication '{s}'), {s} guard(s) re-baked",
            .{ max_row_bytes, slot_name, pub_name, n },
        );
    }
    return effective;
}

/// Connect to PostgreSQL replication stream and resume from the SLOT's position.
///
/// ⚠️ It resumes from the slot's `confirmed_flush_lsn` — the last position this
/// bridge actually acked — because `startStreaming()` takes no position and cannot
/// be told otherwise. That is the entire purpose of a persistent replication slot,
/// and the only way a restart cannot lose data.
///
/// This used to pass `pg_current_wal_lsn()` — the CURRENT WAL head — which silently
/// skipped every change committed while the bridge was down. Measured: stop the
/// bridge, `DELETE FROM memo ...`, restart → the delete never reached CDC, so every
/// replica kept the row forever while Postgres no longer had it. Silent divergence,
/// no error anywhere. Resuming from the slot instead means a restart may REPUBLISH
/// events already published (at-least-once), which is safe by construction: msg ids
/// are LSN-derived, so JetStream dedups inside its window and the client's applier
/// is idempotent (LWW upsert + LSN gate). Duplicates are recoverable; a missing
/// delete is not.
fn initReplicationStream(
    allocator: std.mem.Allocator,
    pg_config: *pg_conn.PgConf,
    slot_name: [:0]const u8,
    pub_name: [:0]const u8,
) !wal_stream.ReplicationStream {
    log.debug("Connecting to WAL replication stream...", .{});

    // No LSN is read here on purpose. The WAL head is not this function's business,
    // and a `current_lsn` in scope next to the START_REPLICATION call is exactly how
    // it got passed as the start position for so long. Lag belongs to wal_monitor,
    // which reports it against the slot (`confirmed_flush_lsn`) — the number that
    // actually means something — every check interval.
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
    try pg_stream.startStreaming();

    log.info(
        "✅ WAL replication resumed from slot '{s}' — anything it still holds replays now\n",
        .{slot_name},
    );

    return pg_stream;
}

pub fn main(init: std.process.Init) !void {
    // Assign first, then report: customLogFn filters against runtime_log_level, so a
    // line logged inside getDefaultLogLevel is judged by the level it is about to
    // replace and disappears at exactly the setting that asked to see it.
    runtime_log_level = Config.getDefaultLogLevel(&init);
    log.debug("LOG_LEVEL → runtime log level: {s}", .{@tagName(runtime_log_level)});
    // Loud on purpose, and warn-level so it survives every filter: debug logs PER
    // EVENT from the hot path (a writev per line), measured at **4x CPU per event**
    // under burst (8.5s → 35s for 2M) — a benchmark run at debug measures the logger,
    // not the bridge. This has now been rediscovered twice via a LOG_LEVEL=debug
    // inherited from a dev shell (`.env.bridge` used to carry it); the third time
    // should cost one glance at the boot log. NOTES.md §1.13.
    if (runtime_log_level == .debug) {
        log.warn("⚠️  LOG_LEVEL=debug: per-event hot-path logging costs ~4x CPU under load — throughput numbers taken at this level are invalid", .{});
    }
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
    log.info("Wire format: \x1b[1m msgpack (CDC), JSON (schema) \x1b[0m — fixed, not configurable", .{});
    log.info("Streams: \x1b[1m {s}, {s} \x1b[0m (from {s})", .{
        runtime_config.topology.stream_cdc,
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
        io,
        parsed_args.http_bind,
        parsed_args.http_port,
        &should_stop,
        &metrics,
        null,
    );
    // Enrollment/mint (NOTES: the JWT mint flow): armed only when the operator
    // handed over the scoped client signing seed — the bridge is a signer, not a
    // key store: no file I/O, the seed arrives like NATS_BRIDGE_NKEY_SEED always did.
    if (init.minimal.environ.getPosix("ZB_SIGNING_SEED")) |seed| {
        if (init.minimal.environ.getPosix("ZB_ACCOUNT_PUB")) |acct| {
            if (runtime_config.pg_writer_url) |wurl| {
                // connect_timeout: a hung PG must cost an enroll permit for
                // seconds, not forever — the permits are the flood bound.
                const sep: []const u8 = if (std.mem.indexOfScalar(u8, wurl, '?') != null) "&" else "?";
                http_srv.enroll = .{
                    .writer_conninfo = try std.fmt.allocPrintSentinel(allocator, "{s}{s}connect_timeout={d}", .{ wurl, sep, Config.Http.enroll_pg_connect_timeout_seconds }, 0),
                    .signing_seed = seed,
                    .account_pub = acct,
                };
                log.info("🎟️ enrollment endpoint armed: GET /enroll (signer: scoped client key)", .{});
            } else {
                log.warn("🎟️ ZB_SIGNING_SEED set but no DATABASE_WRITER_URL — enrollment needs the writer; endpoint stays off", .{});
            }
        }
    }

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

    // The slot exists and the publication is verified, so the registrar's GC step
    // cannot delete the rows it is about to write. Reader connection, SECURITY
    // DEFINER function — works in the read-only profile too.
    const own_event_buf = @as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2);
    const effective_row_budget = registerRowWidthBudget(
        allocator,
        &pg_config,
        slot_name_z,
        pub_name_z,
        own_event_buf,
    ) orelse own_event_buf;

    // === Parse transition rules from environment variable
    // Format: "table1:col1,col2;table2:col3,col4"
    // Example: "users:status,kyc_level;orders:state,payment_status"
    var transition_rules = try args.Args.parseTransitionRules(allocator, &init);
    defer args.Args.deinitTransitionRules(&transition_rules, allocator);

    // Which column carries the version, per table. Same grammar as TRANSITION_RULES.
    var sync_rules = try args.Args.parseSyncRules(allocator, &init);
    defer args.Args.deinitTransitionRules(&sync_rules, allocator);

    // Which column carries the tenant, per table. Empty by default: reads are unscoped
    // unless an operator says otherwise, and preflight reports that plainly rather than
    // letting it be assumed.
    var tenant_rules = try args.Args.parseTenantRules(allocator, &init);
    defer args.Args.deinitTransitionRules(&tenant_rules, allocator);

    // THE CATALOGUE (NOTES.md, static-residue endgame): `zebridge_catalogue` is the
    // authoritative per-table rule source, written by zebridge_enable atomically
    // with the guards. Env rules above become per-table OVERRIDES; the catalogue
    // fills everything else, and a pre-catalogue database loads zero rows.
    var cat = catalogue.loadRules(allocator, &pg_config, &tenant_rules, &sync_rules);
    // Freed on the way out of main — topology.public_tables/tenants alias these
    // slices, and LIFO defers put this AFTER every thread join that reads them.
    defer cat.deinit(allocator);
    if (cat.available) {
        // The catalogue is authoritative for the public set, and the tenant list is
        // DATA (zebridge_user_tenants) — the grammar file no longer carries either
        // (both keys still parse, for a pre-catalogue database only).
        runtime_config.topology.public_tables = cat.publics;
        if (cat.tenants.len > 0) runtime_config.topology.tenants = cat.tenants;
    }
    const default_version_column = init.minimal.environ.getPosix("SYNC_VERSION_COLUMN") orelse
        Config.Sync.default_version_column;

    // Shared between preflight (boot) and the DDL path (runtime): both decide a table
    // has no primary key, and the mutation path must honour either verdict.
    var refused = refused_tables.Registry.init(allocator);
    defer refused.deinit();

    // Shared the same way, for the opposite fact: whether a table accepts edge writes
    // at all (NOTES.md §1.11). Preflight computes it; `EventProcessor.appendWriteContract`
    // reads it back when building each table's schema payload.
    var writable = writable_tables.Registry.init(allocator);
    defer writable.deinit();

    // OID → typtype for types the CDC decoder's switch does not cover. Populated from
    // DDL events and from the boot schema pass, and never invalidated: an OID outlives
    // nothing, so a stale entry describes a type that can no longer reach the wire.
    var type_registry_inst = type_registry.Registry.init(allocator);
    defer type_registry_inst.deinit();

    // The writer's grants are the authoritative statement of which tables are meant to
    // accept edge writes. Null when ingress is unconfigured, which makes every grant
    // check a correct false. Shared with `EventProcessor` below, so a table that
    // appears after boot is checked against the same role preflight used.
    const writer_role = if (runtime_config.pg_writer_url) |url| preflight.roleFromUrl(url) else null;

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
        writer_role,
        &tenant_rules,
        @as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2),
        &writable,
        &runtime_config.topology,
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
    // Resolved once, here, and passed to the publisher and the
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
    //
    if (publisher.js) |*js| {
        if (nats_publisher.serverMaxPayload(js)) |max_payload| {
            log.info(
                "NATS max_payload: {d} KB (server-advertised) → CDC per-event buffer: {d} KB (BASE_BUF={d}, ceiling {d})",
                .{
                    max_payload / 1024,
                    (@as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2)) / 1024,
                    runtime_config.event_data_buffer_log2,
                    runtime_config.event_data_buffer_max_log2,
                },
            );
        } else {
            log.debug("NATS server did not advertise max_payload; cannot check it against BASE_BUF", .{});
        }
    }

    // === Boot reconciliation: the streams the catalogue implies must exist ===
    //
    // Tenants come from the DATA (zebridge_user_tenants) and the public set from the
    // catalogue, so the bridge no longer asks an init container to pre-build what it
    // can ensure itself: a missing CDC_<TENANT> stream is CREATED here
    // (modest 1G caps — JetStream reservations count against the server's storage
    // budget, the INIT_TANGO lesson; an operator can raise them), and CDC_PUBLIC's
    // subject filter is reconciled to exactly the catalogue's public tables plus the
    // open tenant. That closes the declared half of the routing hole at boot; a
    // tenant born AFTER boot keeps the dyntenant contract — whoever onboards it
    // provisions its streams before the first row.
    reconcileCdcStreams(allocator, &publisher, &runtime_config.topology) catch |err| {
        log.err("🔴 FATAL: stream reconciliation failed ({s}) — refusing to start rather than FATAL under load.", .{@errorName(err)});
        should_stop.store(true, .seq_cst);
        return err;
    };

    // Make publisher available to HTTP server for stream management
    http_srv.nats_publisher = &publisher;
    http_srv.refused = &refused;

    // Initialize schema cache for tracking relation_id changes
    log.debug("Schema cache initialized\n", .{});

    // Monitored tables (boot schema publish + schema-change validation).
    const monitored_tables = replication_ctx.tables;

    // Resolve this instance's column-descriptor ceiling now — after the publication
    // exists (auto-detection queries it) and before the ring buffer's sizing check and
    // `initBatchPublisher`, both of which need the final number, not a compile-time
    // guess. See `resolveMaxColumns`'s doc comment for MAX_COLUMNS vs auto-detect.
    const resolved_max_columns = resolveMaxColumns(
        allocator,
        &pg_config,
        parsed_args.publication_name,
        runtime_config.max_columns_override,
    );

    // === Start thread: generation producer (only when GENERATION_RULES is set)
    var generation_rules = try args.Args.parseGenerationRules(allocator, &init);
    defer args.Args.deinitTransitionRules(&generation_rules, allocator);
    var gen_producer: ?*generation_producer.GenerationProducer = null;
    defer if (gen_producer) |gp| {
        gp.join();
        allocator.destroy(gp);
    };
    if (runtime_config.generations_enabled or generation_rules.count() > 0) {
        // The correctness inequality (NOTES.md §1.13): a client up to `depth` gens
        // behind catches up on deltas, so tombstones must outlive depth × cadence or
        // a hard delete is reaped before the delta that would have shipped it — and
        // the row silently survives on that client. The sweeper is a separate
        // process; when its GC_THRESHOLD_MS is visible here, check it, otherwise
        // state the number the operator must hold it above.
        const promise_s: u64 = runtime_config.generation_cadence_seconds * runtime_config.generation_chain_depth;
        if (init.minimal.environ.getPosix("GC_THRESHOLD_MS")) |thr_str| {
            const thr_ms = std.fmt.parseInt(u64, thr_str, 10) catch 0;
            if (thr_ms / 1000 < promise_s) {
                log.warn("⚠️ GC_THRESHOLD_MS ~{d}s is BELOW chain depth × cadence = {d}s: a tombstone can be reaped before the delta that ships it, and a hard-deleted row silently survives on a catching-up client (NOTES.md §1.13)", .{ thr_ms / 1000, promise_s });
            } else {
                log.info("🧬 retention: sweeper window {d}s ≥ depth × cadence {d}s ✓", .{ thr_ms / 1000, promise_s });
            }
        } else {
            log.info("🧬 delta catch-up window: depth × cadence = {d}s. The sweeper's GC_THRESHOLD_MS must stay above it (not visible in this env — checked when it is)", .{promise_s});
        }
        const gp = try allocator.create(generation_producer.GenerationProducer);
        gp.* = generation_producer.GenerationProducer.init(
            allocator,
            &pg_config,
            &should_stop,
            io,
            nats_endpoint,
            &generation_rules,
            &sync_rules,
            &tenant_rules,
            &runtime_config.topology,
            parsed_args.publication_name,
            runtime_config.generation_cadence_seconds,
            runtime_config.generation_chain_depth,
            @as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2),
        );
        try gp.start();
        gen_producer = gp;
        log.info("🧬 Generation producer thread started", .{});
    } else {
        log.info("Generation producer disabled (set GENERATIONS_ENABLED=1 to derive from the publication, or GENERATION_RULES to restrict a probe)", .{});
    }

    // === Start thread: mutation listener (only if an ingress role is configured)
    //
    // The write path runs under its own role, so enabling it can never widen the read
    // path's privileges. No writer configured means no listener — not a fallback to
    // `bridge_reader`, which has REPLICATION and is exactly what must not be doing
    // client-driven writes.
    // Shared between the replication thread (which bumps it on DDL) and the write path
    // (which stamps its catalog cache with it). See catalog_epoch.zig for why the signal
    // comes from the WAL rather than from the schemas KV.
    var catalog_epoch: catalog_epoch_mod.CatalogEpoch = .{};

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
            &catalog_epoch,
            // min(my buffer, the narrowest instance carrying these tables). The first
            // is physical — I cannot encode more than my own buffer. The second is the
            // system constraint PostgreSQL's width guard enforces anyway, so refusing
            // at ingress turns a wasted round trip (publish → apply → 23514 verdict)
            // into an immediate, cheaper rejection with the same outcome.
            @min(own_event_buf, effective_row_budget),
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
        const event_buf_bytes = own_event_buf;
        const count = runtime_config.batch_ring_buffer_size;
        const data_bytes = event_buf_bytes * count;

        // ⚠️ The ring is **three** allocations, and this check used to count only one of
        // them. Each event also carries a fixed `CDCEvent` descriptor (small, now that
        // `columns` is a slice rather than an inline array) PLUS its own slice of a
        // separate columns slab (`resolved_max_columns × 8 bytes`) — and neither term
        // shrinks with BASE_BUF.
        //
        // So the smaller the event buffer, the more metadata+columns dominate, and the
        // further this check drifted from the truth in exactly the configuration that
        // makes sense for many small events. Measured (pre-slab, `[512]ColumnView`
        // inline): BASE_BUF=11 with RING_BUFFER_COUNT=262144 was 512 MiB of data and
        // 2.08 GiB of metadata — the old check saw the 512 MiB, waved it through against
        // a 4 GB machine, and the process then wanted 2.6 GB.
        const meta_bytes = @sizeOf(batch_publisher.CDCEvent) * count;
        const columns_bytes = @as(usize, resolved_max_columns) * @sizeOf(batch_publisher.CDCEvent.ColumnView) * count;
        const slab_bytes = data_bytes + meta_bytes + columns_bytes;

        // #1 — will the slab fit in the memory this process actually has?
        const limit = utils.memoryLimitBytes();
        if (limit == 0) {
            log.debug("Cannot determine the memory limit; skipping the slab sizing check", .{});
        } else if (slab_bytes * 2 > limit) {
            // Half the limit is the line: the slab is not the only thing resident — libpq
            // buffers, the NATS client and the arenas all sit
            // beside it — and a slab past half leaves no room for the work it exists to do.
            log.err(
                "🔴 The ring would be {d} MB — {d} MB of data (BASE_BUF={d} → {d} KB × RING_BUFFER_COUNT={d}) plus {d} MB of per-event metadata ({d} B each) plus {d} MB of column descriptors (MAX_COLUMNS={d} × {d} B × RING_BUFFER_COUNT) — against a {d} MB memory limit. It is pre-allocated at startup, so this is an OOM kill under load rather than a slow degradation. Halve RING_BUFFER_COUNT for each step you raise BASE_BUF; see README 'Sizing BASE_BUF and RING_BUFFER_COUNT'.",
                .{
                    slab_bytes / 1024 / 1024,
                    data_bytes / 1024 / 1024,
                    runtime_config.event_data_buffer_log2,
                    event_buf_bytes / 1024,
                    count,
                    meta_bytes / 1024 / 1024,
                    @sizeOf(batch_publisher.CDCEvent),
                    columns_bytes / 1024 / 1024,
                    resolved_max_columns,
                    @sizeOf(batch_publisher.CDCEvent.ColumnView),
                    limit / 1024 / 1024,
                },
            );
            return error.SlabExceedsMemory;
        } else {
            // All three terms, always: an operator reading "512 MB" while the process
            // takes 2.6 GB has no way to discover the difference short of watching it die.
            log.info("Event ring: {d} MB of a {d} MB limit ({d}%) — {d} MB data + {d} MB metadata + {d} MB columns", .{
                slab_bytes / 1024 / 1024,
                limit / 1024 / 1024,
                slab_bytes * 100 / limit,
                data_bytes / 1024 / 1024,
                meta_bytes / 1024 / 1024,
                columns_bytes / 1024 / 1024,
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
        &metrics,
        &runtime_config,
        resolved_max_columns,
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
        &tenant_rules,
        @as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2),
        &writable,
        writer_role,
    );

    // The DROP-prune needs the shared publisher's JetStream context (assigned
    // here, not in init, because the publisher is built earlier in boot but the
    // processor's ctor predates it in the file's dependency order).
    event_proc.publisher = &publisher;

    // Publish boot schemas to NATS KV for all monitored tables
    try event_proc.publishBootSchemas(allocator, monitored_tables);

    // Backfill $KV.tenants.* from zebridge_user_tenants — replication only carries
    // changes from here on; this catches mappings that existed before this boot.
    try event_proc.publishBootTenants(allocator);

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
    // Upper bound on an idle wait; the poll returns early the moment WAL arrives.
    const idle_poll_timeout_ms: i32 = 100;
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
    // in-flight transaction, normally released to the publisher at .commit so a
    // consumer sees a whole transaction at once.
    //
    // ⚠️ When it FILLS, the batch is released early instead (`drainTxSlots` below).
    // It used to log, discard the transaction and `return error.TransactionOverflow`,
    // which killed the process — and since the transaction was still unacked in the
    // slot, every restart replayed it and died again. Measured 2026-08-26: a single
    // 2M-row transaction (an ordinary `DO` block, a bulk import, a `COPY`) made the
    // bridge PERMANENTLY unstartable, recoverable only by an operator running
    // pg_replication_slot_advance — which silently discards those changes and
    // diverges every replica. NOTES.md finding 5.
    //
    // Releasing early is safe because `proto_version '1'` does not stream: PostgreSQL
    // sends a transaction's changes only AFTER it commits, so everything here is
    // already committed and there is nothing to roll back. The LSN is still acked
    // only at .commit, so a crash mid-transaction replays the whole thing — and the
    // pipeline is idempotent by construction (LSN-derived msg ids → JetStream dedup,
    // LWW upsert + LSN gate in the applier). A duplicate is recoverable; a crash
    // loop is not.
    //
    // This also un-breaks the back-pressure that was always there:
    // `acquireAndFillSlot` blocks for a free slot with a watchdog, but during an
    // all-or-nothing hold the publisher's queue stayed EMPTY, so no slot could ever
    // come back and the wait would have been infinite. That starvation — created by
    // the hold itself — is what the old row cap existed to pre-empt. Handing the
    // publisher work removes the starvation, so the cap has nothing left to guard.
    //
    // Safety invariant: tx_slots_buf.len < ring_buffer_capacity.
    // If a single transaction claimed every ring buffer slot before .commit arrived,
    // acquireAndFillSlot would spin forever because the background thread cannot free
    // anything from an empty pending_events queue. By capping the tracker one below
    // the ring buffer size the overflow guard always fires with at least one slot
    // still free, keeping the background thread able to make progress.
    const ring_buffer_capacity = runtime_config.batch_ring_buffer_size;
    // Not a limit any more — the point at which a long transaction is handed to the
    // publisher in a batch. One below capacity so a release always happens while at
    // least one slot is still free.
    const tx_flush_quantum = ring_buffer_capacity - 1;
    const tx_slots_buf: []u32 = try allocator.alloc(u32, tx_flush_quantum);
    defer allocator.free(tx_slots_buf);
    var tx_slots_count: usize = 0;
    log.info("Ring buffer capacity: {d} slots, transaction flush quantum: {d} rows (no transaction size limit)", .{ ring_buffer_capacity, tx_flush_quantum });

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
            defer allocator.free(snap.last_ack_lsn_str);

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
                snap.last_ack_lsn_str,
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

                                // ⚠️ Nothing is compared here, deliberately. A `SchemaCache`
                                // used to sit at this point, keyed on `relation_id` — which
                                // is the table's OID and does NOT change on ALTER TABLE, so
                                // it could only ever fire on a drop-and-recreate, never on
                                // the migration people actually run. It published nothing,
                                // and it was a standing invitation to put "republish the
                                // schema" inside a branch that never runs. Removed.
                                //
                                // Schema changes reach clients through DDL events captured
                                // by the event trigger, which travel this same WAL stream as
                                // ordinary rows and keep their order relative to the data.
                                // This RELATION message has one job: describe the tuples
                                // that follow, which are decoded positionally against it.

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
                                        // Long transaction: hand this batch over so the publisher can drain
                                        // and slots return to the pool. The LSN is still acked at .commit.
                                        try releaseTxSlots(&event_proc, tx_slots_buf, &tx_slots_count);
                                    }
                                    if (std.mem.eql(u8, rel.name, "zebridge_ddl_events")) {
                                        // Before packing, and unconditionally: the write
                                        // path's catalog cache is stale whether or not
                                        // this row produces something to publish. A
                                        // de-duplicated schema and a refused table both
                                        // still mean the catalogue moved.
                                        catalog_epoch.bump();
                                        if (try event_proc.packDdlToSlot(arena_allocator, rel, ins.tuple_data, wal_msg.wal_end)) |slot_idx| {
                                            tx_slots_buf[tx_slots_count] = slot_idx;
                                            tx_slots_count += 1;
                                            cdc_events += 1;
                                        }
                                    } else if (std.mem.eql(u8, rel.name, "zebridge_user_tenants")) {
                                        // Never a cdc.zebridge_user_tenants.* event — see
                                        // the matching exemption in
                                        // zebridge_publication_guard() and NOTES.md §1.12
                                        // part 3. Diverted into $KV.tenants.<principal>
                                        // only, same shape as the DDL branch above.
                                        if (try event_proc.packTenantToKvSlot(arena_allocator, rel, ins.tuple_data, wal_msg.wal_end)) |slot_idx| {
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
                                    if (std.mem.eql(u8, rel.name, "zebridge_user_tenants")) {
                                        // A reassignment (`UPDATE ... SET tenant_id = ...`)
                                        // arrives here, not as .insert — the NEW row is
                                        // what the KV bucket should hold, same helper as
                                        // the insert branch above.
                                        if (tx_slots_count >= tx_slots_buf.len) {
                                            // Long transaction: hand this batch over so the publisher can drain
                                            // and slots return to the pool. The LSN is still acked at .commit.
                                            try releaseTxSlots(&event_proc, tx_slots_buf, &tx_slots_count);
                                        }
                                        if (try event_proc.packTenantToKvSlot(arena_allocator, rel, upd.new_tuple, wal_msg.wal_end)) |slot_idx| {
                                            tx_slots_buf[tx_slots_count] = slot_idx;
                                            tx_slots_count += 1;
                                            cdc_events += 1;
                                        }
                                        break :blk_upd;
                                    }
                                    if (event_proc.refused.shouldDrop(rel.name)) break :blk_upd;
                                    if (tx_slots_count >= tx_slots_buf.len) {
                                        // Long transaction: hand this batch over so the publisher can drain
                                        // and slots return to the pool. The LSN is still acked at .commit.
                                        try releaseTxSlots(&event_proc, tx_slots_buf, &tx_slots_count);
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
                                    // A revoked mapping (DELETE FROM zebridge_user_tenants):
                                    // purge the principal's $KV.tenants key, so the next
                                    // resolveTenant() reads "no mapping" → the open tenant,
                                    // the same state a principal with no row has. No client
                                    // change needed: both already treat an absent key that
                                    // way. Never a cdc.zebridge_user_tenants.* event either.
                                    // The DELETE carries the key columns (REPLICA IDENTITY
                                    // DEFAULT, PK = principal) — enough to name the key.
                                    if (std.mem.eql(u8, rel.name, "zebridge_user_tenants")) {
                                        if (pgoutput.decodeTuple(arena_allocator, del.old_tuple, rel.columns, event_proc.types)) |cols| {
                                            for (cols.items) |col| {
                                                if (std.mem.eql(u8, col.name, "principal") and col.value == .text) {
                                                    event_proc.purgeTenantKey(col.value.text);
                                                }
                                            }
                                        } else |err| log.warn("revoked tenant mapping: could not decode the key: {s}", .{@errorName(err)});
                                        break :blk_del;
                                    }
                                    if (event_proc.refused.shouldDrop(rel.name)) break :blk_del;

                                    // ── The sweeper's reaps are not client data ──────────
                                    //
                                    // A table with a tombstone column never deletes from the
                                    // edge: `applyDelete` turns a client's delete into an
                                    // UPDATE that sets the tombstone, and clients act on that.
                                    // The only DELETEs such a table ever produces are the
                                    // sweeper reaping rows whose tombstones have aged out —
                                    // rows every client already removed when the soft delete
                                    // arrived.
                                    //
                                    // Forwarding them costs a message per reaped row per
                                    // client, for a row they know is gone, and a sweep of a
                                    // million tombstones is a million such messages.
                                    //
                                    // ⚠️ Suppressed **here**, not in PostgreSQL, because
                                    // PostgreSQL cannot express it. Measured: `publish` is a
                                    // publication-level option, so it cannot be set per table;
                                    // publications *union*, so a second one with
                                    // `publish='insert,update'` still emits the delete when
                                    // both are named on the slot; and a row filter
                                    // (`WHERE deleted_at IS NULL`) makes the table unwritable,
                                    // because the filter column is not in the replica identity.
                                    //
                                    // ⚠️ Only for tables that HAVE a tombstone. A table
                                    // without one deletes physically, those deletes are the
                                    // real thing, and dropping them would leave the row in
                                    // every replica forever with no later event to remove it.
                                    if (event_proc.hasTombstone(rel.name)) {
                                        log.debug("🧹 suppressed reap DELETE on '{s}': soft-deleted rows already reached clients as an update", .{rel.name});
                                        break :blk_del;
                                    }
                                    if (tx_slots_count >= tx_slots_buf.len) {
                                        // Long transaction: hand this batch over so the publisher can drain
                                        // and slots return to the pool. The LSN is still acked at .commit.
                                        try releaseTxSlots(&event_proc, tx_slots_buf, &tx_slots_count);
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
                // No message available - idle path.
                //
                // ⚠️ Block on the socket, do not sleep on a timer. `PQgetCopyData` returns
                // 0 as soon as libpq's buffer is drained, so a 1 ms sleep here woke this
                // loop ~1000×/s to find nothing: measured `iters=11617 idle=11616` per 15 s
                // window and ~4% CPU on a completely idle database.
                //
                // `poll()` wakes the instant PostgreSQL writes, so latency is unchanged or
                // better; the timeout only bounds how long we wait before re-checking the
                // keepalive and status-update deadlines below.
                prof_idle += 1;
                const wal_fd = pg_stream.socketFd();
                if (wal_fd >= 0) {
                    var pfd = [_]std.posix.pollfd{.{
                        .fd = wal_fd,
                        .events = std.posix.POLL.IN,
                        .revents = 0,
                    }};
                    _ = std.posix.poll(&pfd, idle_poll_timeout_ms) catch {
                        utils.sleep(1 * std.time.ns_per_ms);
                    };
                } else {
                    utils.sleep(1 * std.time.ns_per_ms);
                }
            }
        } else |err| {
            // A stream end is only GRACEFUL if we requested the shutdown. Otherwise
            // the server severed it — a killed walsender, a dropped link libpq read
            // as a clean COPY end — and a bridge that BREAKS here keeps running with
            // CDC silently dead (found by chaos.py's pg_terminate_backend phase:
            // the process stayed green, the mutation listener reconnected, and
            // replication was gone with pg_reconnects still 0). A severed stream is
            // reconnectable exactly like any other connection loss.
            if (err == error.StreamEnded and should_stop.load(.seq_cst)) {
                log.info("Stream ended (shutdown requested)", .{});
                break;
            }

            // Handle connection errors by reconnecting.
            if (err == error.StreamEnded) {
                log.warn("⚠️ Replication stream ended unexpectedly (server severed it) — reconnecting", .{});
            } else {
                log.warn("Connection lost: {}", .{err});
            }
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

            // Clean up old connection before reconnecting
            pg_stream.deinit();

            // Reconnect to replication stream
            pg_stream.connect() catch |conn_err| {
                log.err("Failed to reconnect: {}", .{conn_err});
                utils.sleep(Config.Retry.pg_reconnect_delay_seconds * std.time.ns_per_s);
                continue;
            };

            // Resumes from the slot, like the boot path — a reconnect happens
            // exactly when the walsender was severed (Finding 1's territory), the
            // moment changes are most likely to be in flight.
            pg_stream.startStreaming() catch |stream_err| {
                log.err("Failed to restart streaming: {}", .{stream_err});
                utils.sleep(Config.Retry.pg_reconnect_delay_seconds * std.time.ns_per_s);
                continue;
            };

            log.info("✓ Reconnected to WAL stream, resumed from the slot's confirmed position", .{});
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

/// Are two subject filters the same set? Order-insensitive, exact strings.
fn streamSubjectsEqual(have: []const []const u8, wanted: []const []const u8) bool {
    if (have.len != wanted.len) return false;
    outer: for (wanted) |w| {
        for (have) |h| if (std.mem.eql(u8, h, w)) continue :outer;
        return false;
    }
    return true;
}

/// Boot-time stream reconciliation (see the call site above). Creation parameters
/// mirror what nats-init used to apply — file storage, limits retention, s2, 8d CDC /
/// 7d INIT — except max_bytes, deliberately 1G: JetStream treats every stream's
/// max_bytes as a reservation against the server's storage budget, and a boot that
/// reserves 10G per tenant refuses to create anything on a small server.
fn reconcileCdcStreams(
    allocator: std.mem.Allocator,
    publisher: anytype,
    topo: *const topology_mod.Topology,
) !void {
    const js = if (publisher.js) |*j| j else return error.NotConnected;
    const day_ns: u64 = 24 * 60 * 60 * std.time.ns_per_s;
    const cap_bytes: i64 = Config.Nats.reconciled_stream_max_bytes;

    // ── per-tenant streams: ensure, create when missing ─────────────────────────
    for (topo.tenants) |tenant| {
        var name_buf: [256]u8 = undefined;
        inline for (.{
            .{ topo.cdc_stream_prefix, "{s}.{s}.>", Config.Nats.reconciled_cdc_max_age_days },
        }) |shape| {
            const prefix = shape[0];
            // CDC_<tenant> — the tenant AS-IS, never upper-cased:
            // the JWT signing key's role template renders CDC_{{tag(tenant)}}
            // literally and nsc lowercases tags, so the stream name must match
            // the tag byte-for-byte.
            @memcpy(name_buf[0..prefix.len], prefix);
            @memcpy(name_buf[prefix.len..][0..tenant.len], tenant);
            const stream_name = name_buf[0 .. prefix.len + tenant.len];
            if (publisher.streamExists(stream_name)) {
                log.info("✅ tenant '{s}' → stream {s}", .{ tenant, stream_name });
            } else {
                const prefix_val = topo.subject_cdc_prefix;
                const subj = try std.fmt.allocPrint(allocator, shape[1], .{ prefix_val, tenant });
                defer allocator.free(subj);
                var res = try js.addStream(.{
                    .name = stream_name,
                    .subjects = &.{subj},
                    .max_age = shape[2] * day_ns,
                    .max_msgs = Config.Nats.reconciled_stream_max_msgs,
                    .max_bytes = cap_bytes,
                    .compression = .s2,
                });
                res.deinit();
                log.info("🆕 tenant '{s}' → created stream {s} ({s})", .{ tenant, stream_name, subj });
            }
        }
    }

    // ── CDC_PUBLIC: subjects ARE the catalogue's public set + the open tenant ───
    var wanted: std.ArrayList([]const u8) = .empty;
    defer {
        for (wanted.items) |w| allocator.free(w);
        wanted.deinit(allocator);
    }
    for (topo.public_tables) |tbl|
        try wanted.append(allocator, try std.fmt.allocPrint(allocator, "{s}.{s}.>", .{ topo.subject_cdc_prefix, tbl }));
    try wanted.append(allocator, try std.fmt.allocPrint(allocator, "{s}.{s}.>", .{ topo.subject_cdc_prefix, topo.open_tenant }));

    if (js.getStreamInfo(topo.cdc_stream_public)) |info_const| {
        var info = info_const;
        defer info.deinit();
        if (streamSubjectsEqual(info.value.config.subjects, wanted.items)) {
            log.info("✅ {s} subjects already match the catalogue ({d} subject(s))", .{ topo.cdc_stream_public, wanted.items.len });
        } else {
            // The FULL stored config back, with only the subjects replaced —
            // updateStream serializes everything, so a partial config would silently
            // reset retention and limits to defaults.
            var cfg = info.value.config;
            cfg.subjects = wanted.items;
            var res = try js.updateStream(cfg);
            res.deinit();
            log.info("🛠️ {s} subjects reconciled to the catalogue ({d} subject(s))", .{ topo.cdc_stream_public, wanted.items.len });
        }
    } else |_| {
        var res = try js.addStream(.{
            .name = topo.cdc_stream_public,
            .subjects = wanted.items,
            .max_age = Config.Nats.reconciled_cdc_max_age_days * day_ns,
            .max_msgs = Config.Nats.reconciled_stream_max_msgs,
            .max_bytes = cap_bytes,
            .compression = .s2,
        });
        res.deinit();
        log.info("🆕 created stream {s} with {d} subject(s)", .{ topo.cdc_stream_public, wanted.items.len });
    }
}
