//! Command-line arguments parsing for CDC Bridge application
const std = @import("std");
const config = @import("config.zig");
const encoder = @import("encoder.zig");
const log = std.log.scoped(.args);

const usage =
    \\Usage: bridge [OPTIONS]
    \\
    \\Streams PostgreSQL logical replication (pgoutput) to NATS JetStream.
    \\
    \\Options:
    \\  --slot <NAME>   Replication slot name (created if absent)
    \\  --pub <NAME>    PostgreSQL PUBLICATION to stream
    \\  --port <PORT>   HTTP telemetry port
    \\  --json          Encode as JSON (default: MessagePack)
    \\  --strict-tables Refuse to start if any published table lacks a primary key
    \\                  (default: skip the table, keep replicating the rest)
    \\  --help, -h      Show this message
    \\
    \\Environment:
    \\  PG_HOST, PG_PORT, PG_DB          PostgreSQL connection
    \\  POSTGRES_BRIDGE_USER/_PASSWORD   PostgreSQL credentials
    \\  PG_SSLMODE                       libpq sslmode (default: disable)
    \\  DATABASE_URL                     Unified PostgreSQL URI (overrides PG_* above)
    \\  NATS_HOST, NATS_URL              NATS server host / unified URI
    \\  NATS_NKEY_SEED                   NATS nkey seed
    \\  BASE_BUF                         log2 of per-event data buffer (10-20)
    \\  RING_BUFFER_COUNT                Ring buffer slots (1024-1048576)
    \\  PUBLISH_MAX_RETRIES              Publish retries before fatal (default: 5)
    \\  PUBLISH_BACKOFF_MS               First publish backoff (default: 100)
    \\  PUBLISH_MAX_BACKOFF_MS           Publish backoff ceiling (default: 5000)
    \\  SNAP_RET_SECONDS                 Snapshot freshness window (default: 600)
    \\  TRANSITION_RULES                 e.g. users:status,kyc_level;orders:state
    \\
    \\Defaults for all of the above live in src/config.zig.
    \\
;

/// Signals that `--help` was handled and the process should exit cleanly.
pub const HelpRequested = error{HelpRequested};

/// Read an unsigned integer from the environment, falling back to `default` when
/// unset, unparseable, or outside [min, max]. Never fails: a bad tunable should
/// warn and use the documented default, not stop the bridge from starting.
fn envUint(
    comptime T: type,
    init: *const std.process.Init,
    comptime name: []const u8,
    default: T,
    min: T,
    max: T,
) T {
    const raw = init.minimal.environ.getPosix(name) orelse {
        log.info(name ++ " not set, using default: {d}", .{default});
        return default;
    };
    const parsed = std.fmt.parseInt(T, raw, 10) catch |err| {
        log.warn("Invalid " ++ name ++ " value '{s}' ({any}), using default: {d}", .{ raw, err, default });
        return default;
    };
    if (parsed < min or parsed > max) {
        log.warn(name ++ "={d} out of range ({d}-{d}), using default: {d}", .{ parsed, min, max, default });
        return default;
    }
    log.info(name ++ "={d}", .{parsed});
    return parsed;
}

/// Command-line arguments structure
pub const Args = struct {
    http_port: u16,
    slot_name: []const u8,
    publication_name: []const u8,
    encoding_format: encoder.Format,

    /// Parse command-line arguments and create RuntimeConfig.
    ///
    /// Zig 0.16 "Juicy Main": args and environ are received via std.process.Init
    /// passed from main(). init.args.iterate() replaces the removed argsWithAllocator/argsAlloc.
    /// init.minimal.environ.getPosix() replaces the removed std.process.getEnvVarOwned().
    /// Allocates nothing: every string in the returned config is either a compile-time
    /// default or a slice borrowed from argv/environ, both of which outlive the process.
    pub fn parseArgs(init: *const std.process.Init) !struct { args: Args, runtime_config: config.RuntimeConfig } {
        var args_iter = init.minimal.args.iterate();
        _ = args_iter.next(); // skip argv[0] (program name)

        var http_port: u16 = 6543; // default
        var slot_name: []const u8 = config.Postgres.default_slot_name; // default
        var publication_name: []const u8 = config.Postgres.default_publication_name; // default
        var encoding_format: encoder.Format = .msgpack; // default
        // Off by default: one keyless table should not stop every other table from
        // replicating. See preflight.zig for the full argument.
        var strict_tables: bool = false;

        while (args_iter.next()) |arg| {
            if (std.mem.eql(u8, arg, "--port")) {
                if (args_iter.next()) |value| {
                    http_port = std.fmt.parseInt(u16, value, 10) catch {
                        log.err("--port requires a valid port number (1-65535)", .{});
                        return error.InvalidArguments;
                    };
                }
            } else if (std.mem.eql(u8, arg, "--slot")) {
                if (args_iter.next()) |value| slot_name = value;
            } else if (std.mem.eql(u8, arg, "--pub")) {
                if (args_iter.next()) |value| publication_name = value;
            } else if (std.mem.eql(u8, arg, "--json")) {
                encoding_format = .json;
            } else if (std.mem.eql(u8, arg, "--strict-tables")) {
                strict_tables = true;
            } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
                std.debug.print("{s}", .{usage});
                return error.HelpRequested;
            } else {
                // Reject rather than ignore: silently skipping unknown flags is how a
                // stale --zstd survived in deployment configs after the flag was removed.
                log.err("unknown argument: {s}", .{arg});
                std.debug.print("{s}", .{usage});
                return error.InvalidArguments;
            }
        }

        const cli_args = Args{
            .http_port = http_port,
            .slot_name = slot_name,
            .publication_name = publication_name,
            .encoding_format = encoding_format,
        };

        // Build runtime configuration by merging CLI args with compile-time defaults
        var runtime_config = config.RuntimeConfig.defaults();
        runtime_config.http_port = http_port;
        runtime_config.slot_name = slot_name;
        runtime_config.publication_name = publication_name;
        runtime_config.encoding_format = encoding_format;
        runtime_config.strict_tables = strict_tables;

        // Read PostgreSQL configuration from environment variables via Juicy Main environ.
        // getPosix() borrows from the environ block, which lives as long as the process,
        // so these are assigned directly — no dupe, nothing to free. (0.15's
        // getEnvVarOwned returned owned memory; 0.16 changed that.) Valid because
        // nothing here calls setenv/putenv, which could reallocate the block.
        runtime_config.db_url = init.minimal.environ.getPosix("DATABASE_URL");
        runtime_config.pg_host = init.minimal.environ.getPosix("PG_HOST") orelse blk: {
            log.info("PG_HOST not set, using default: {s}", .{runtime_config.pg_host});
            break :blk runtime_config.pg_host;
        };

        if (init.minimal.environ.getPosix("PG_PORT")) |port_str| {
            runtime_config.pg_port = std.fmt.parseInt(u16, port_str, 10) catch |err| blk: {
                log.warn("Invalid PG_PORT value '{s}' ({any}), using default: {d}", .{ port_str, err, runtime_config.pg_port });
                break :blk runtime_config.pg_port;
            };
        } else {
            log.info("PG_PORT not set, using default: {d}", .{runtime_config.pg_port});
        }

        // Priority: POSTGRES_BRIDGE_USER > PG_USER > default
        runtime_config.pg_user = init.minimal.environ.getPosix("POSTGRES_BRIDGE_USER") orelse init.minimal.environ.getPosix("PG_USER") orelse blk: {
            log.info("POSTGRES_BRIDGE_USER and PG_USER not set, using default: {s}", .{runtime_config.pg_user});
            break :blk runtime_config.pg_user;
        };

        // Priority: POSTGRES_BRIDGE_PASSWORD > PG_PASSWORD > default
        runtime_config.pg_password = init.minimal.environ.getPosix("POSTGRES_BRIDGE_PASSWORD") orelse init.minimal.environ.getPosix("PG_PASSWORD") orelse blk: {
            log.info("POSTGRES_BRIDGE_PASSWORD and PG_PASSWORD not set, using default", .{});
            break :blk runtime_config.pg_password;
        };

        runtime_config.pg_database = init.minimal.environ.getPosix("PG_DB") orelse blk: {
            log.info("PG_DB not set, using default: {s}", .{runtime_config.pg_database});
            break :blk runtime_config.pg_database;
        };

        runtime_config.pg_sslmode = init.minimal.environ.getPosix("PG_SSLMODE") orelse blk: {
            log.info("PG_SSLMODE not set, using default: {s}", .{runtime_config.pg_sslmode});
            break :blk runtime_config.pg_sslmode;
        };

        // Parse BASE_BUF environment variable (log2 of buffer size)
        if (init.minimal.environ.getPosix("BASE_BUF")) |buf_log2_str| {
            if (std.fmt.parseInt(u6, buf_log2_str, 10)) |buf_log2| {
                if (buf_log2 >= 10 and buf_log2 <= 20) {
                    const buf_size = @as(usize, 1) << @intCast(buf_log2);
                    runtime_config.event_data_buffer_log2 = buf_log2;
                    log.info("BASE_BUF={d} → event buffer size: {d} bytes ({d}KB)", .{ buf_log2, buf_size, buf_size / 1024 });
                } else {
                    log.warn("BASE_BUF={d} out of range (10-20), using default: {d} ({}KB)", .{
                        buf_log2,
                        runtime_config.event_data_buffer_log2,
                        (@as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2)) / 1024,
                    });
                }
            } else |err| {
                log.warn("Invalid BASE_BUF value '{s}' ({any}), using default: {d} ({}KB)", .{
                    buf_log2_str,
                    err,
                    runtime_config.event_data_buffer_log2,
                    (@as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2)) / 1024,
                });
            }
        } else {
            const buf_size = @as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2);
            log.info("BASE_BUF not set, using default: {d} → {d} bytes ({d}KB)", .{
                runtime_config.event_data_buffer_log2,
                buf_size,
                buf_size / 1024,
            });
        }

        // Parse RING_BUFFER_COUNT environment variable
        if (init.minimal.environ.getPosix("RING_BUFFER_COUNT")) |count_str| {
            if (std.fmt.parseInt(usize, count_str, 10)) |count| {
                if (count >= 1024 and count <= 1024 * 1024) {
                    runtime_config.batch_ring_buffer_size = count;
                    log.info("RING_BUFFER_COUNT={d} events", .{count});
                } else {
                    log.warn("RING_BUFFER_COUNT={d} out of range (1024-1048576), using default: {d}", .{
                        count,
                        runtime_config.batch_ring_buffer_size,
                    });
                }
            } else |err| {
                log.warn("Invalid RING_BUFFER_COUNT value '{s}' ({any}), using default: {d}", .{
                    count_str,
                    err,
                    runtime_config.batch_ring_buffer_size,
                });
            }
        } else {
            log.info("RING_BUFFER_COUNT not set, using default: {d} events", .{runtime_config.batch_ring_buffer_size});
        }

        // Publish retry budget. Defaults live in config.zig (Config.Retry); these
        // env vars exist so the budget can be tuned per deployment without a rebuild.
        // Exhausting the budget is fatal by design, so raising it trades faster
        // failure detection for more tolerance of a flaky NATS link.
        runtime_config.publish_max_retries = envUint(
            u32,
            init,
            "PUBLISH_MAX_RETRIES",
            runtime_config.publish_max_retries,
            0,
            100,
        );
        runtime_config.publish_backoff_ms = envUint(
            u64,
            init,
            "PUBLISH_BACKOFF_MS",
            runtime_config.publish_backoff_ms,
            1,
            60_000,
        );
        runtime_config.publish_max_backoff_ms = envUint(
            u64,
            init,
            "PUBLISH_MAX_BACKOFF_MS",
            runtime_config.publish_max_backoff_ms,
            runtime_config.publish_backoff_ms,
            300_000,
        );

        runtime_config.snapshot_retention_seconds = @intCast(envUint(
            u32,
            init,
            "SNAP_RET_SECONDS",
            @intCast(runtime_config.snapshot_retention_seconds),
            1,
            86_400,
        ));

        // NATS connection — slices into environ block, no allocation needed
        runtime_config.nats_url = init.minimal.environ.getPosix("NATS_URL");
        runtime_config.nats_host = init.minimal.environ.getPosix("NATS_HOST") orelse blk: {
            log.info("NATS_HOST not set, using default: {s}", .{runtime_config.nats_host});
            break :blk runtime_config.nats_host;
        };
        runtime_config.nats_seed = init.minimal.environ.getPosix("NATS_NKEY_SEED");

        return .{
            .args = cli_args,
            .runtime_config = runtime_config,
        };
    }

    /// Parse TRANSITION_RULES environment variable into a HashMap
    ///
    /// Format: "table1:col1,col2;table2:col3,col4"
    /// Example: "users:status,kyc_level;orders:state,payment_status"
    pub fn parseTransitionRules(allocator: std.mem.Allocator, init: *const std.process.Init) !config.EventClassification.TransitionRules {
        var rules = config.EventClassification.TransitionRules.init(allocator);
        errdefer rules.deinit();

        const rules_str = init.minimal.environ.getPosix("TRANSITION_RULES") orelse return rules;

        if (rules_str.len == 0) {
            log.info("TRANSITION_RULES is empty, no transition detection configured", .{});
            return rules;
        }

        log.info("Parsing TRANSITION_RULES: {s}", .{rules_str});

        // Split by semicolon to get table rules: "users:status,kyc_level;orders:state"
        var table_iter = std.mem.splitScalar(u8, rules_str, ';');
        while (table_iter.next()) |table_rule| {
            const trimmed = std.mem.trim(u8, table_rule, " \t\n\r");
            if (trimmed.len == 0) continue;

            var colon_iter = std.mem.splitScalar(u8, trimmed, ':');
            const table_name = colon_iter.next() orelse {
                log.warn("Invalid TRANSITION_RULES entry (missing ':'): {s}", .{trimmed});
                continue;
            };
            const columns_str = colon_iter.next() orelse {
                log.warn("Invalid TRANSITION_RULES entry (no columns after ':'): {s}", .{trimmed});
                continue;
            };

            var col_list: std.ArrayList([]const u8) = .empty;
            errdefer col_list.deinit(allocator);

            var col_iter = std.mem.splitScalar(u8, columns_str, ',');
            while (col_iter.next()) |col| {
                const col_trimmed = std.mem.trim(u8, col, " \t\n\r");
                if (col_trimmed.len > 0) {
                    const col_owned = try allocator.dupe(u8, col_trimmed);
                    try col_list.append(allocator, col_owned);
                }
            }

            if (col_list.items.len == 0) {
                col_list.deinit(allocator);
                log.warn("No valid columns for table '{s}', skipping", .{table_name});
                continue;
            }

            const table_name_owned = try allocator.dupe(u8, std.mem.trim(u8, table_name, " \t\n\r"));
            const columns_slice = try col_list.toOwnedSlice(allocator);

            try rules.put(table_name_owned, columns_slice);
            log.info("  → Table '{s}': watching {d} columns: {s}", .{
                table_name_owned,
                columns_slice.len,
                columns_str,
            });
        }

        if (rules.count() == 0) {
            log.info("No valid transition rules parsed, transition detection disabled", .{});
        } else {
            log.info("Transition detection configured for {d} table(s)", .{rules.count()});
        }

        return rules;
    }

    /// Free all memory allocated for transition rules HashMap
    pub fn deinitTransitionRules(rules: *config.EventClassification.TransitionRules, allocator: std.mem.Allocator) void {
        var iter = rules.iterator();
        while (iter.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            for (entry.value_ptr.*) |col_name| {
                allocator.free(col_name);
            }
            allocator.free(entry.value_ptr.*);
        }
        rules.deinit();
    }
};
