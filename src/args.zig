//! Command-line arguments parsing for CDC Bridge application
const std = @import("std");
const config = @import("config.zig");
const encoder = @import("encoder.zig");
const topology_mod = @import("topology.zig");
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
    \\  --top <PATH>    topology.json to read (default: ./topology.json). Carries every
    \\                  stream, subject and KV name; missing file or key is fatal.
    \\  --json          Encode as JSON (default: MessagePack)
    \\  --strict-tables Refuse to start if any published table lacks a primary key
    \\                  (default: skip the table, keep replicating the rest)
    \\  --help, -h      Show this message
    \\
    \\Environment:
    \\  DATABASE_URL          REQUIRED. Read path, credentials included:
    \\                        postgres://<role>:<pass>@<host>:<port>/<db>[?sslmode=…]
    \\  DATABASE_WRITER_URL   Ingress path, same form. Unset disables the mutation
    \\                        listener entirely — it never falls back to the read role.
    \\  BRIDGE_PORT           HTTP telemetry port when --port is absent
    \\  NATS_URL              nats://[user:pass@]host[:port] (default: localhost)
    \\  NATS_NKEY_SEED        NATS nkey seed
    \\  BASE_BUF              log2 of per-event data buffer (10-20)
    \\  RING_BUFFER_COUNT     Ring buffer slots (1024-1048576)
    \\  PUBLISH_MAX_RETRIES   Publish retries before fatal (default: 5)
    \\  PUBLISH_BACKOFF_MS    First publish backoff (default: 100)
    \\  PUBLISH_MAX_BACKOFF_MS  Publish backoff ceiling (default: 5000)
    \\  TRANSITION_RULES      e.g. users:status,kyc_level;orders:state
    \\  SYNC_VERSION_COLUMN   default version column (default: updated_at)
    \\  SYNC_RULES            per-table override, same grammar:
    \\                        users:updated_at,deleted_at;orders:modified_at
    \\  TENANT_RULES          which column carries the tenant, per table:
    \\                        orders:tenant_id;invoices:tenant_id
    \\                        A listed table gets its tenant appended to every CDC
    \\                        subject, so reads can be scoped per principal.
    \\  LOG_LEVEL             debug|info|warn|err (default: info).
    \\                        Log lines print "warning"/"error"; both
    \\                        spellings are accepted here.
    \\
    \\Admin credentials (PG_HOST/PG_USER/PG_PASSWORD, POSTGRES_BRIDGE_*,
    \\POSTGRES_WRITER_*) are NOT read by the bridge — they belong to init.sql and the
    \\bridge-init container. Keep them in .env.admin. Defaults live in src/config.zig.
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
    /// Where topology.json is. See `--top`.
    topology_path: []const u8,
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

        // Sentinel, not a value: the env fallback must only apply when --port was
        // absent, and 0 is not a usable port so it cannot be confused with one. The
        // literal default used to be 6543 here while Config.Http.default_port said
        // 9090 — two answers, and the one that won was the one nobody had written down.
        var http_port: ?u16 = null;
        var slot_name: []const u8 = config.Postgres.default_slot_name; // default
        var publication_name: []const u8 = config.Postgres.default_publication_name; // default
        var encoding_format: encoder.Format = .msgpack; // default
        // Off by default: one keyless table should not stop every other table from
        // replicating. See preflight.zig for the full argument.
        var strict_tables: bool = false;
        // Null means "not given", so TOPOLOGY_PATH can still speak before the default.
        var topology_path: ?[]const u8 = null;

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
            } else if (std.mem.eql(u8, arg, "--top")) {
                if (args_iter.next()) |value| topology_path = value;
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

        // `--port` beats `BRIDGE_PORT` beats the compiled default. BRIDGE_PORT was
        // declared in .env.bridge and read by nobody: setting it and omitting --port
        // left telemetry on a port no curl was pointed at, with nothing in the log to
        // say so — hence the resolved value and its source are printed below.
        const resolved_port: u16 = http_port orelse envUint(
            u16,
            init,
            "BRIDGE_PORT",
            config.Http.default_port,
            1,
            65_535,
        );

        log.info("HTTP telemetry port {d} (from {s})", .{
            resolved_port,
            if (http_port != null)
                "--port"
            else if (init.minimal.environ.getPosix("BRIDGE_PORT") != null)
                "BRIDGE_PORT"
            else
                "default",
        });

        // `--top` beats TOPOLOGY_PATH beats ./topology.json — the same precedence as
        // `--port` over BRIDGE_PORT. Only the *path* is resolved here; reading it is
        // `main`'s job, because a failure there must stop the process with a message,
        // not be folded into argument parsing.
        const resolved_topology = topology_path orelse
            init.minimal.environ.getPosix("TOPOLOGY_PATH") orelse
            topology_mod.default_path;

        const cli_args = Args{
            .topology_path = resolved_topology,
            .http_port = resolved_port,
            .slot_name = slot_name,
            .publication_name = publication_name,
            .encoding_format = encoding_format,
        };

        // Build runtime configuration by merging CLI args with compile-time defaults
        var runtime_config = config.RuntimeConfig.defaults();
        runtime_config.http_port = resolved_port;
        runtime_config.slot_name = slot_name;
        runtime_config.publication_name = publication_name;
        runtime_config.encoding_format = encoding_format;
        runtime_config.strict_tables = strict_tables;

        // Read PostgreSQL configuration from environment variables via Juicy Main environ.
        // getPosix() borrows from the environ block, which lives as long as the process,
        // so these are assigned directly — no dupe, nothing to free. (0.15's
        // getEnvVarOwned returned owned memory; 0.16 changed that.) Valid because
        // nothing here calls setenv/putenv, which could reallocate the block.
        // The bridge connects with URLs and nothing else.
        //
        // PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DB used to be a fallback here. They are
        // the **superuser** credentials `bridge-init` interpolates into init.sql to
        // create the bridge's roles, and they sit in the same environment — so a missing
        // or misspelled DATABASE_URL silently connected as `postgres` instead of the
        // read role, and the log looked fine. Convenient, and precisely the wrong thing
        // to be convenient about: the read role is deliberately unable to write, and
        // that guarantee is worth nothing if the process can connect as someone else.
        //
        // Failing here rather than defaulting is the whole point. `sslmode` goes in the
        // URL's query string.
        runtime_config.db_url = init.minimal.environ.getPosix("DATABASE_URL") orelse {
            log.err(
                "🔴 DATABASE_URL is required: postgres://<role>:<password>@<host>:<port>/<db>. " ++
                    "There is no PG_HOST/PG_USER fallback — those are the admin credentials init.sql " ++
                    "runs under, and the bridge must not be able to connect with them.",
                .{},
            );
            return error.MissingDatabaseUrl;
        };
        // `debug`, not `warn`: on a correctly split environment these variables are simply
        // absent, so warning would cry wolf at every start. It stays in the code — and
        // matchable — because when someone *does* hit the old layout, this line is the
        // difference between "why is it connecting as postgres" and a two-second answer.
        if (init.minimal.environ.getPosix("PG_HOST") != null or
            init.minimal.environ.getPosix("PG_USER") != null)
        {
            log.debug(
                "PG_HOST/PG_USER/PG_PASSWORD/PG_DB are set but ignored — the bridge uses DATABASE_URL. " ++
                    "Those belong in .env.admin, which only compose and bridge-init read.",
                .{},
            );
        }

        // Ingress. Same rule, and no fallback to the read URL: it carries REPLICATION.
        runtime_config.pg_writer_url = init.minimal.environ.getPosix("DATABASE_WRITER_URL");
        if (runtime_config.pg_writer_url == null) {
            log.info("No DATABASE_WRITER_URL — ingress (mutation) path disabled", .{});
            if (init.minimal.environ.getPosix("POSTGRES_WRITER_USER") != null) {
                log.warn("POSTGRES_WRITER_USER is set but ignored — set DATABASE_WRITER_URL to enable ingress", .{});
            }
        }

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

        // NATS connection — slices into environ block, no allocation needed.
        //
        // `NATS_URL` is the only address input. `NATS_HOST` used to be a second one, and
        // a second one is worse than none: it was read by the mutation listener alone,
        // which then dialled `NATS_HOST:4222` while everything else went where the URL
        // said. Warned rather than silently ignored, because a variable that is set,
        // wrong, and inert is the hardest kind to debug.
        runtime_config.nats_url = init.minimal.environ.getPosix("NATS_URL") orelse blk: {
            log.info("NATS_URL not set, using default: {s}", .{config.Nats.default_url});
            break :blk config.Nats.default_url;
        };
        // Same reasoning as the PG_* line above: debug, not warn.
        if (init.minimal.environ.getPosix("NATS_HOST")) |host| {
            log.debug("NATS_HOST='{s}' is ignored — set NATS_URL (currently {s})", .{ host, runtime_config.nats_url.? });
        }
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
        return parseTableRules(allocator, init, "TRANSITION_RULES");
    }

    /// `SYNC_RULES=users:updated_at,deleted_at;orders:modified_at`
    ///
    /// Same grammar as TRANSITION_RULES on purpose — operators learn one format, and the
    /// parser is literally the same code. First column is the version, optional second
    /// is the tombstone.
    pub fn parseSyncRules(allocator: std.mem.Allocator, init: *const std.process.Init) !config.EventClassification.TransitionRules {
        return parseTableRules(allocator, init, "SYNC_RULES");
    }

    /// `TENANT_RULES=orders:tenant_id;invoices:tenant_id`
    ///
    /// Which column carries the tenant, per table. A table listed here has its tenant
    /// appended to every CDC subject (`cdc.<table>.<op>.<tenant>`), so NATS subject
    /// permissions can scope reads the way they already scope writes.
    ///
    /// Deliberately a **separate variable** from SYNC_RULES rather than a third positional
    /// field. SYNC_RULES is `table:version[,tombstone]` with an optional second column, so
    /// a third could not be told apart from a tombstone by position — and confusing a
    /// tombstone column with a tenant column would route every row of the table to a
    /// subject named after a timestamp.
    ///
    /// The same list-of-columns type as the other two, so the parser is shared; only the
    /// first column is read.
    pub fn parseTenantRules(allocator: std.mem.Allocator, init: *const std.process.Init) !config.EventClassification.TransitionRules {
        return parseTableRules(allocator, init, "TENANT_RULES");
    }

    fn parseTableRules(
        allocator: std.mem.Allocator,
        init: *const std.process.Init,
        comptime env_name: []const u8,
    ) !config.EventClassification.TransitionRules {
        var rules = config.EventClassification.TransitionRules.init(allocator);
        errdefer rules.deinit();

        const rules_str = init.minimal.environ.getPosix(env_name) orelse return rules;

        if (rules_str.len == 0) {
            log.info(env_name ++ " is empty", .{});
            return rules;
        }

        log.info("Parsing " ++ env_name ++ ": {s}", .{rules_str});

        // Split by semicolon to get table rules: "users:status,kyc_level;orders:state"
        var table_iter = std.mem.splitScalar(u8, rules_str, ';');
        while (table_iter.next()) |table_rule| {
            const trimmed = std.mem.trim(u8, table_rule, " \t\n\r");
            if (trimmed.len == 0) continue;

            var colon_iter = std.mem.splitScalar(u8, trimmed, ':');
            const table_name = colon_iter.next() orelse {
                log.warn("Invalid " ++ env_name ++ " entry (missing ':'): {s}", .{trimmed});
                continue;
            };
            const columns_str = colon_iter.next() orelse {
                log.warn("Invalid " ++ env_name ++ " entry (no columns after ':'): {s}", .{trimmed});
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
            log.info("No valid " ++ env_name ++ " parsed", .{});
        } else {
            log.info(env_name ++ ": {d} table(s) configured", .{rules.count()});
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
