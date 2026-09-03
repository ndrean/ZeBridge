//! Command-line arguments parsing for CDC Bridge application
const std = @import("std");
const config = @import("config.zig");
const topology_mod = @import("topology.zig");
const log = std.log.scoped(.args);

const usage =
    \\Usage: bridge [OPTIONS]
    \\
    \\Streams PostgreSQL logical replication (pgoutput) to NATS JetStream.
    \\
    \\Options:
    \\  --slot <NAME>   Replication slot (created if absent). REQUIRED here or as
    \\                  BRIDGE_CDC_SLOT — there is no default.
    \\  --pub <NAME>    PostgreSQL PUBLICATION to stream. REQUIRED here or as
    \\                  BRIDGE_CDC_PUBLICATION — there is no default.
    \\  --port <PORT>   HTTP telemetry port
    \\  --top <PATH>    grammar.json to read (default: ./grammar.json). Carries every
    \\                  stream, subject and KV name; missing file or key is fatal.
    \\  --strict-tables Refuse to start if any published table lacks a primary key
    \\  --diagnose           Pre-run doctor: report everything boot would decide, write nothing, exit
    \\                  (default: skip the table, keep replicating the rest)
    \\  --gen-nkey      Mint one nkey pair and exit, without starting anything.
    \\                  Prints NATS_BRIDGE_NKEY_PUB / NATS_BRIDGE_NKEY_SEED on
    \\                  stdout (the warning goes to stderr, so it pipes:
    \\                  `bridge --gen-nkey >> .env.bridge`). The PUB line goes in
    \\                  nats-server.conf, the SEED is the bridge's credential —
    \\                  it is shown once and never stored by this command.
    \\  --help, -h      Show this message
    \\
    \\Environment:
    \\  DATABASE_READER_URL          REQUIRED. Read path, credentials included:
    \\                        postgres://<role>:<pass>@<host>:<port>/<db>[?sslmode=…]
    \\  BRIDGE_CDC_PUBLICATION  REQUIRED unless --pub is given (which overrides it).
    \\  BRIDGE_CDC_SLOT       REQUIRED unless --slot is given (which overrides it).
    \\  DATABASE_WRITER_URL   Ingress path, same form. Unset disables the mutation
    \\                        listener entirely — it never falls back to the read role.
    \\  BRIDGE_PORT           HTTP telemetry port when --port is absent
    \\  BRIDGE_BIND           telemetry bind address (default 127.0.0.1). Every
    \\                        endpoint is unauthenticated, so 0.0.0.0 exposes table
    \\                        names, lag and throughput to anything that can reach it.
    \\  NATS_URL              nats://[user:pass@]host[:port] (default: localhost)
    \\  NATS_BRIDGE_NKEY_SEED        NATS nkey seed
    \\  NATS_CREDS            path to a .creds file (operator/JWT mode; wins over the seed)
    \\  BASE_BUF              log2 of per-event data buffer (10-20 by default)
    \\  BASE_BUF_MAX          Raise BASE_BUF's ceiling past 20 (to at most 24), for a
    \\                        deployment that has also raised NATS's own max_payload.
    \\                        Per instance, like BASE_BUF itself — no rebuild needed.
    \\  RING_BUFFER_COUNT     Ring buffer slots (1024-1048576)
    \\  MAX_COLUMNS           Columns one CDC event can carry (8-1600). Unset (default):
    \\                        auto-detected at boot from the widest monitored table,
    \\                        rounded up for migration headroom. Set only to override.
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
    \\  GENERATIONS_ENABLED   1/true: the generation producer derives its table set
    \\                        from the publication (minus internals, no-PK,
    \\                        non-routable, opted-out via zebridge_enable) and its
    \\                        tenants from the data (NOTES.md §1.13)
    \\  GENERATION_RULES      a RESTRICTION intersected with the derived set —
    \\                        probes/dev only; also enables the producer by itself:
    \\                        users:_default;test_types:acme,globex
    \\  GENERATION_CADENCE_SECONDS  tick interval (default: 600). A CORRECTNESS
    \\                        parameter: depth × cadence must stay under the
    \\                        sweeper's tombstone retention.
    \\  GENERATION_CHAIN_DEPTH  generations kept per pair (default: 6)
    \\  LOG_LEVEL             debug|info|warn|err (default: info).
    \\                        Log lines print "warning"/"error"; both
    \\                        spellings are accepted here.
    \\
    \\Admin credentials (PG_HOST/PG_USER/PG_PASSWORD, POSTGRES_READER_*,
    \\POSTGRES_WRITER_*) are NOT read by the bridge — they belong to init.sql and the
    \\bridge-init container. Keep them in .env.admin. Defaults live in src/config.zig.
    \\
;

/// The one spelling of each early flag, so the scan below and any future parse cannot
/// drift apart.
pub const help_flags = [_][]const u8{ "--help", "-h" };
pub const gen_nkey_flag = "--gen-nkey";

/// Flags that REPLACE the program instead of configuring it: they read no environment,
/// open nothing, and their whole output IS the answer.
pub const EarlyExit = enum { help, gen_nkey };

/// Answered from argv by `main` BEFORE the log level is resolved and before anything
/// else prints — boot noise on stderr is noise in a command meant to be piped
/// (`bridge --gen-nkey >> .env.bridge`, `bridge --help | less`). Reads argv, nothing else.
///
/// ⚠️ A flat scan, so `bridge --slot --help` answers help rather than "--slot requires a
/// value". Every CLI that honours `--help` anywhere on the line has that edge; the
/// alternative is resolving configuration first, which is exactly what this avoids.
pub fn earlyExit(init: *const std.process.Init) ?EarlyExit {
    var it = init.minimal.args.iterate();
    _ = it.next(); // argv[0]
    while (it.next()) |arg| {
        for (help_flags) |flag| {
            if (std.mem.eql(u8, arg, flag)) return .help;
        }
        if (std.mem.eql(u8, arg, gen_nkey_flag)) return .gen_nkey;
    }
    return null;
}

/// The usage text on STDOUT: help that was ASKED FOR is output, and output belongs in
/// the pipe (`bridge --help | less`, `bridge --help > usage.txt`). A usage printed
/// because an argument was WRONG stays on stderr below, where a diagnostic belongs —
/// that difference is the whole reason these are two call sites and not one.
pub fn printUsage(io: std.Io) !void {
    try std.Io.File.stdout().writeStreamingAll(io, usage);
}

/// Read an unsigned integer from the environment. Never fails: a bad tunable should warn
/// and carry on, not stop the bridge from starting.
///
/// ⚠️ **Unset or unparseable falls back to the default; out-of-range CLAMPS.** They are
/// different signals and used to be treated alike. `banana` carries no intent, so the
/// default is the only sensible answer — but `RING_BUFFER_COUNT=64` carries a *direction*:
/// "less memory". Substituting the default gave 32768, a thousand times **more** than was
/// asked for, which at BASE_BUF=19 is a 32 GB ring that the sizing check then refuses
/// outright. The leniency produced both a fatal error and an unrecognisable one — the
/// operator sees a number they never typed.
///
/// Clamping keeps the never-fail property and moves toward the intent instead of away
/// from it.
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
        const clamped = @min(@max(parsed, min), max);
        log.warn(name ++ "={d} out of range ({d}-{d}), clamped to {d}", .{ parsed, min, max, clamped });
        return clamped;
    }
    log.info(name ++ "={d}", .{parsed});
    return parsed;
}

/// Command-line arguments structure
pub const Args = struct {
    /// `--diagnose` / `--run-diagnose`: the pre-run doctor. Everything the boot would
    /// DECIDE, said before anything is DONE — no slot, no budget registration, no guard
    /// re-bake, no NATS connection, no stream creation. For the operator standing in
    /// front of a database that has never run the bridge: is init applied, is the
    /// publication whole, which tables would be refused and why, does the stored data
    /// fit this BASE_BUF, and what is the smallest BASE_BUF that would fit it.
    diagnose: bool = false,
    /// Where grammar.json is. See `--top`.
    topology_path: []const u8,
    http_port: u16,
    http_bind: []const u8,
    slot_name: []const u8,
    publication_name: []const u8,

    /// Parse command-line arguments and create RuntimeConfig.
    ///
    /// Zig 0.16 "Juicy Main": args and environ are received via std.process.Init
    /// passed from main(). init.args.iterate() replaces the removed argsWithAllocator/argsAlloc.
    /// init.minimal.environ.getPosix() replaces the removed std.process.getEnvVarOwned().
    /// Allocates nothing: every string in the returned config is either a compile-time
    /// default or a slice borrowed from argv/environ, both of which outlive the process.
    /// The value that must follow a flag, or a refusal naming the flag.
    fn requireValue(it: anytype, flag: []const u8) ![]const u8 {
        return it.next() orelse {
            log.err("🔴 {s} requires a value (it was given as the last argument, with nothing after it)", .{flag});
            return error.InvalidArguments;
        };
    }

    pub fn parseArgs(init: *const std.process.Init) !struct { args: Args, runtime_config: config.RuntimeConfig } {
        var args_iter = init.minimal.args.iterate();
        _ = args_iter.next(); // skip argv[0] (program name)

        // Sentinel, not a value: the env fallback must only apply when --port was
        // absent, and 0 is not a usable port so it cannot be confused with one. The
        // literal default used to be 6543 here while Config.Http.default_port said
        // 27434 — two answers, and the one that won was the one nobody had written down.
        var http_port: ?u16 = null;
        // Precedence: CLI flag > env > NOTHING. There is no compiled default for
        // either name, and adding one back would be a mistake:
        //
        // These two names decide WHICH ROWS this bridge replicates and WHERE its
        // WAL position is kept. A default cannot be right — it can only be a name
        // that happens to exist somewhere. `cdc_pub` and `cdc_slot` were those
        // names, and they turned every way of forgetting to say which publication
        // you meant into a bridge that started, logged nothing unusual, and
        // replicated a different table set. Two tenants on one cluster share the
        // default; the second bridge to boot then fights the first for its slot.
        //
        // Unset is not ambiguous, so it is not guessed at: the bridge stops and
        // says which of the two channels to use. Both channels are real — the env
        // var is what lets the bridge boot with no flags at all from .env.bridge
        // (systemd EnvironmentFile, compose env_file), and the flag overrides it
        // for a one-off run against another publication on the same host.
        var slot_name: ?[]const u8 = init.minimal.environ.getPosix("BRIDGE_CDC_SLOT");
        var publication_name: ?[]const u8 = init.minimal.environ.getPosix("BRIDGE_CDC_PUBLICATION");
        // Off by default: one keyless table should not stop every other table from
        // replicating. See preflight.zig for the full argument.
        var strict_tables: bool = false;
        var diagnose = false;
        // Null means "not given", so TOPOLOGY_PATH can still speak before the default.
        var topology_path: ?[]const u8 = null;

        // ⚠️ A flag whose value is MISSING must fail, not fall back.
        // `--pub` at the end of the command line used to leave the compiled
        // default in place — measured: `bridge --slot my_slot --pub` started on
        // `cdc_pub`. It happened to die here because that publication does not
        // exist; on a deployment where it does, the bridge would have replicated
        // the WRONG table set, quietly. `--pub ""` was always caught (the
        // existence check refuses ''), which is exactly what made the missing
        // value the dangerous one: it looks like a typo and behaves like a
        // default.

        while (args_iter.next()) |arg| {
            if (std.mem.eql(u8, arg, "--port")) {
                if (args_iter.next()) |value| {
                    http_port = std.fmt.parseInt(u16, value, 10) catch {
                        log.err("--port requires a valid port number (1-65535)", .{});
                        return error.InvalidArguments;
                    };
                }
            } else if (std.mem.eql(u8, arg, "--slot")) {
                slot_name = try requireValue(&args_iter, "--slot");
            } else if (std.mem.eql(u8, arg, "--pub")) {
                publication_name = try requireValue(&args_iter, "--pub");
            } else if (std.mem.eql(u8, arg, "--top")) {
                topology_path = try requireValue(&args_iter, "--top");
            } else if (std.mem.eql(u8, arg, "--strict-tables")) {
                strict_tables = true;
            } else if (std.mem.eql(u8, arg, "--diagnose") or std.mem.eql(u8, arg, "--run-diagnose")) {
                diagnose = true;
            } else {
                // `--help`/`-h` and `--gen-nkey` never reach here: `earlyExit` answered
                // them from argv before main resolved anything (see above). They are
                // absent from this loop deliberately — one place decides, not two.
                //
                // Reject rather than ignore: silently skipping unknown flags is how a
                // stale --zstd survived in deployment configs after the flag was removed.
                log.err("unknown argument: {s}", .{arg});
                std.debug.print("{s}", .{usage});
                return error.InvalidArguments;
            }
        }

        // ⚠️ No default, so an unset name stops the boot here rather than
        // silently choosing one. Reported together: a fresh deployment is
        // usually missing both, and one error per run is one edit per run.
        const resolved_slot = slot_name orelse {
            log.err("🔴 no replication slot named: pass --slot <name>, or set BRIDGE_CDC_SLOT (there is no default — the slot holds this bridge's WAL position)", .{});
            return error.InvalidArguments;
        };
        const resolved_publication = publication_name orelse {
            log.err("🔴 no publication named: pass --pub <name>, or set BRIDGE_CDC_PUBLICATION (there is no default — the publication decides which tables this bridge replicates)", .{});
            return error.InvalidArguments;
        };

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

        // Borrowed from the environment block, which outlives the process.
        const resolved_bind: []const u8 = init.minimal.environ.getPosix("BRIDGE_BIND") orelse
            config.Http.default_bind;

        log.info("HTTP telemetry bind {s} (from {s})", .{
            resolved_bind,
            if (init.minimal.environ.getPosix("BRIDGE_BIND") != null) "BRIDGE_BIND" else "default",
        });

        log.info("HTTP telemetry port {d} (from {s})", .{
            resolved_port,
            if (http_port != null)
                "--port"
            else if (init.minimal.environ.getPosix("BRIDGE_PORT") != null)
                "BRIDGE_PORT"
            else
                "default",
        });

        // `--top` beats TOPOLOGY_PATH beats ./grammar.json — the same precedence as
        // `--port` over BRIDGE_PORT. Only the *path* is resolved here; reading it is
        // `main`'s job, because a failure there must stop the process with a message,
        // not be folded into argument parsing.
        const resolved_topology = topology_path orelse
            init.minimal.environ.getPosix("TOPOLOGY_PATH") orelse
            topology_mod.default_path;

        const cli_args = Args{
            .topology_path = resolved_topology,
            .http_port = resolved_port,
            .http_bind = resolved_bind,
            .slot_name = resolved_slot,
            .publication_name = resolved_publication,
            .diagnose = diagnose,
        };

        // Build runtime configuration by merging CLI args with compile-time defaults
        var runtime_config = config.RuntimeConfig.defaults();
        runtime_config.http_port = resolved_port;
        runtime_config.slot_name = resolved_slot;
        runtime_config.publication_name = resolved_publication;
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
        // or misspelled DATABASE_READER_URL silently connected as `postgres` instead of the
        // read role, and the log looked fine. Convenient, and precisely the wrong thing
        // to be convenient about: the read role is deliberately unable to write, and
        // that guarantee is worth nothing if the process can connect as someone else.
        //
        // Failing here rather than defaulting is the whole point. `sslmode` goes in the
        // URL's query string.
        // ⚠️ Named DATABASE_URL until 2026-08-28, and renamed for the same reason the
        // PG_* fallback was deleted: DATABASE_URL is the Heroku-style "my application's
        // database", so it invites exactly the value that must never be pasted here — a
        // full-privilege URL works and silently dissolves the reader/writer split. No
        // compatibility path: an env still carrying the old name fails HERE, by name.
        runtime_config.db_url = init.minimal.environ.getPosix("DATABASE_READER_URL") orelse {
            log.err(
                "🔴 DATABASE_READER_URL is required: postgres://<role>:<password>@<host>:<port>/<db>. " ++
                    "(It was called DATABASE_URL before 2026-08-28 — rename it.) " ++
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
                "PG_HOST/PG_USER/PG_PASSWORD/PG_DB are set but ignored — the bridge uses DATABASE_READER_URL. " ++
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

        // `BASE_BUF_MAX` first: it sets the ceiling `BASE_BUF` itself is about to be
        // clamped against, so it has to be resolved before BASE_BUF is parsed. Unset
        // means "the compiled-in default", which is NATS's own out-of-the-box
        // max_payload (1 MiB) — see Buffers.default_max_event_data_buffer_log2 for why.
        // Raising it is for a deployment that has *also* raised its NATS server's
        // max_payload and wants BASE_BUF allowed to follow — per instance, without a
        // rebuild, the same reason BASE_BUF itself is an env var rather than a compile
        // constant: one host can run several bridges against tables of very different
        // shapes on the same WAL.
        runtime_config.event_data_buffer_max_log2 = envUint(
            u6,
            init,
            "BASE_BUF_MAX",
            config.Buffers.default_max_event_data_buffer_log2,
            config.Buffers.min_event_data_buffer_log2,
            config.Buffers.absolute_max_event_data_buffer_log2,
        );

        // BASE_BUF: log2 of the per-event data buffer, clamped to
        // [min_event_data_buffer_log2, event_data_buffer_max_log2] — the second bound is
        // the value just resolved above, not a second, independently-hardcoded ceiling.
        runtime_config.event_data_buffer_log2 = envUint(
            u6,
            init,
            "BASE_BUF",
            config.Buffers.default_event_data_buffer_log2,
            config.Buffers.min_event_data_buffer_log2,
            runtime_config.event_data_buffer_max_log2,
        );
        {
            const buf_size = @as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2);
            log.info("Event buffer: {d} bytes ({d}KB) — BASE_BUF={d}, ceiling BASE_BUF_MAX={d}", .{
                buf_size,
                buf_size / 1024,
                runtime_config.event_data_buffer_log2,
                runtime_config.event_data_buffer_max_log2,
            });
        }

        // RING_BUFFER_COUNT: same clamp-not-default policy as BASE_BUF (see `envUint`'s
        // doc comment — this is the setting that motivated it, now actually wired to it
        // instead of restating the same bounds and the same clamp-vs-default logic by hand).
        runtime_config.batch_ring_buffer_size = envUint(
            usize,
            init,
            "RING_BUFFER_COUNT",
            config.Buffers.default_ring_buffer_count,
            config.Buffers.min_ring_buffer_count,
            config.Buffers.max_ring_buffer_count,
        );

        // MAX_COLUMNS: explicit override for the per-instance column-descriptor slab
        // width. Deliberately NOT `envUint` — that helper's "unset → default" policy is
        // wrong here, because unset means "auto-detect at boot from the publication's
        // actual tables" (bridge.zig's `resolveMaxColumns`, which needs a PG connection
        // this function doesn't have), not "fall back to Batch.default_max_columns".
        // Substituting the default here would silently skip auto-detection for every
        // deployment that never touched this variable, which is the common case.
        runtime_config.max_columns_override = if (init.minimal.environ.getPosix("MAX_COLUMNS")) |raw| blk: {
            const parsed = std.fmt.parseInt(u16, raw, 10) catch |err| {
                log.warn("Invalid MAX_COLUMNS value '{s}' ({any}), ignoring — will auto-detect at boot", .{ raw, err });
                break :blk null;
            };
            const clamped = std.math.clamp(parsed, config.Batch.min_columns, config.Batch.absolute_max_columns);
            if (clamped != parsed) {
                log.warn("MAX_COLUMNS={d} out of range ({d}-{d}), clamped to {d}", .{
                    parsed,
                    config.Batch.min_columns,
                    config.Batch.absolute_max_columns,
                    clamped,
                });
            } else {
                log.info("MAX_COLUMNS={d} (explicit override — auto-detection skipped)", .{clamped});
            }
            break :blk clamped;
        } else null;

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
        runtime_config.nats_seed = init.minimal.environ.getPosix("NATS_BRIDGE_NKEY_SEED");
        runtime_config.nats_creds = init.minimal.environ.getPosix("NATS_CREDS");

        // Generation producer pacing. ⚠️ The cadence is a CORRECTNESS parameter, not a
        // freshness knob: chain depth × cadence must stay under the sweeper's tombstone
        // retention or deltas silently lose deletions (NOTES.md §1.13).
        runtime_config.generations_enabled = if (init.minimal.environ.getPosix("GENERATIONS_ENABLED")) |v|
            (std.mem.eql(u8, v, "1") or std.ascii.eqlIgnoreCase(v, "true"))
        else
            false;
        runtime_config.generation_cadence_seconds = envUint(
            u64,
            init,
            "GENERATION_CADENCE_SECONDS",
            config.Generations.default_cadence_seconds,
            config.Generations.min_cadence_seconds,
            config.Generations.max_cadence_seconds,
        );
        runtime_config.generation_chain_depth = envUint(
            u32,
            init,
            "GENERATION_CHAIN_DEPTH",
            config.Generations.default_chain_depth,
            1,
            64,
        );

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

    /// `GENERATION_RULES=users:_default;test_types:acme,globex`
    ///
    /// A RESTRICTION, not a source (NOTES.md §1.13). The producer derives its
    /// membership from the publication (minus internals, no-PK, non-routable,
    /// opted-out) and its tenants from the data via `zebridge_tenants_of` — this
    /// variable, when set, only intersects that derived set: probes and dev runs
    /// scope themselves to one pair without a second list existing to disagree.
    /// Setting it also enables the producer on its own (implied intent);
    /// production uses `GENERATIONS_ENABLED=1` with this unset.
    pub fn parseGenerationRules(allocator: std.mem.Allocator, init: *const std.process.Init) !config.EventClassification.TransitionRules {
        return parseTableRules(allocator, init, "GENERATION_RULES");
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

            // ⚠️ Empty fields are KEPT as empty strings: the grammar is positional
            // (`version[,tombstone[,tiebreak]]`), so `updated_at,,last_writer` means
            // "no tombstone, tiebreak last_writer". Skipping empties silently slid the
            // tiebreak into the tombstone slot — found live: counter_public treated
            // `last_writer` as its tombstone and never stamped a tiebreak. Every
            // consumer treats an empty entry as "not configured".
            var col_iter = std.mem.splitScalar(u8, columns_str, ',');
            var any_nonempty = false;
            while (col_iter.next()) |col| {
                const col_trimmed = std.mem.trim(u8, col, " \t\n\r");
                if (col_trimmed.len > 0) any_nonempty = true;
                const col_owned = try allocator.dupe(u8, col_trimmed);
                try col_list.append(allocator, col_owned);
            }

            if (!any_nonempty) {
                for (col_list.items) |c| allocator.free(c);
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
