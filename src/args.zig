//! Command-line arguments parsing for CDC Bridge application
const std = @import("std");
const config = @import("config.zig");
const encoder = @import("encoder.zig");
const log = std.log.scoped(.args);

/// Command-line arguments structure
pub const Args = struct {
    http_port: u16,
    slot_name: []const u8,
    publication_name: []const u8,
    encoding_format: encoder.Format,
    enable_compression: bool,

    /// Parse command-line arguments and create RuntimeConfig
    /// Returns both the CLI args and the merged runtime configuration
    pub fn parseArgs(allocator: std.mem.Allocator) !struct { args: Args, runtime_config: config.RuntimeConfig } {
        // argsWithAllocator was removed in Zig 0.16; argsAlloc is the replacement.
        // The returned slice is intentionally not freed — CLI arg strings live for the
        // program's lifetime (same semantics as POSIX argv pointers).
        const argv = try std.process.argsAlloc(allocator);

        var http_port: u16 = 6543; // default
        var slot_name: []const u8 = config.Postgres.default_slot_name; // default
        var publication_name: []const u8 = config.Postgres.default_publication_name; // default
        var encoding_format: encoder.Format = .msgpack; // default
        var enable_compression: bool = false; // default: disabled

        var i: usize = 1; // argv[0] is the program name
        while (i < argv.len) : (i += 1) {
            const arg = argv[i];
            if (std.mem.eql(u8, arg, "--port")) {
                i += 1;
                if (i < argv.len) {
                    http_port = std.fmt.parseInt(u16, argv[i], 10) catch {
                        log.err("--port requires a valid port number (1-65535)", .{});
                        return error.InvalidArguments;
                    };
                }
            } else if (std.mem.eql(u8, arg, "--slot")) {
                i += 1;
                if (i < argv.len) slot_name = argv[i];
            } else if (std.mem.eql(u8, arg, "--pub")) {
                i += 1;
                if (i < argv.len) publication_name = argv[i];
            } else if (std.mem.eql(u8, arg, "--json")) {
                encoding_format = .json;
            } else if (std.mem.eql(u8, arg, "--zstd")) {
                enable_compression = true;
            }
        }

        const cli_args = Args{
            .http_port = http_port,
            .slot_name = slot_name,
            .publication_name = publication_name,
            .encoding_format = encoding_format,
            .enable_compression = enable_compression,
        };

        // Build runtime configuration by merging CLI args with compile-time defaults
        var runtime_config = config.RuntimeConfig.defaults();
        runtime_config.http_port = http_port;
        runtime_config.slot_name = slot_name;
        runtime_config.publication_name = publication_name;
        runtime_config.enable_compression = enable_compression;

        // Read PostgreSQL configuration from environment variables
        // Priority: POSTGRES_BRIDGE_* > PG_* > defaults
        runtime_config.pg_host = std.process.getEnvVarOwned(allocator, "PG_HOST") catch |err| blk: {
            log.info("PG_HOST not set ({any}), using default: {s}", .{ err, runtime_config.pg_host });
            break :blk runtime_config.pg_host;
        };

        if (std.process.getEnvVarOwned(allocator, "PG_PORT")) |port_str| {
            defer allocator.free(port_str);
            runtime_config.pg_port = std.fmt.parseInt(u16, port_str, 10) catch |err| blk: {
                log.warn("Invalid PG_PORT value '{s}' ({any}), using default: {d}", .{ port_str, err, runtime_config.pg_port });
                break :blk runtime_config.pg_port;
            };
        } else |err| {
            log.info("PG_PORT not set ({any}), using default: {d}", .{ err, runtime_config.pg_port });
        }

        // Try POSTGRES_BRIDGE_USER first, fallback to PG_USER
        runtime_config.pg_user = std.process.getEnvVarOwned(allocator, "POSTGRES_BRIDGE_USER") catch blk: {
            const generic_user = std.process.getEnvVarOwned(allocator, "PG_USER") catch |err| blk2: {
                log.info("POSTGRES_BRIDGE_USER and PG_USER not set ({any}), using default: {s}", .{ err, runtime_config.pg_user });
                break :blk2 runtime_config.pg_user;
            };
            break :blk generic_user;
        };

        // Try POSTGRES_BRIDGE_PASSWORD first, fallback to PG_PASSWORD
        runtime_config.pg_password = std.process.getEnvVarOwned(allocator, "POSTGRES_BRIDGE_PASSWORD") catch blk: {
            const generic_password = std.process.getEnvVarOwned(allocator, "PG_PASSWORD") catch |err| blk2: {
                log.info("POSTGRES_BRIDGE_PASSWORD and PG_PASSWORD not set ({any}), using default", .{err});
                break :blk2 runtime_config.pg_password;
            };
            break :blk generic_password;
        };

        runtime_config.pg_database = std.process.getEnvVarOwned(allocator, "PG_DB") catch |err| blk: {
            log.info("PG_DB not set ({any}), using default: {s}", .{ err, runtime_config.pg_database });
            break :blk runtime_config.pg_database;
        };

        // Parse BASE_BUF environment variable (log2 of buffer size)
        if (std.process.getEnvVarOwned(allocator, "BASE_BUF")) |buf_log2_str| {
            defer allocator.free(buf_log2_str);
            if (std.fmt.parseInt(u6, buf_log2_str, 10)) |buf_log2| {
                if (buf_log2 >= 10 and buf_log2 <= 20) { // 1KB to 1MB range
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
        } else |err| {
            const buf_size = @as(usize, 1) << @intCast(runtime_config.event_data_buffer_log2);
            log.info("BASE_BUF not set ({any}), using default: {d} → {d} bytes ({d}KB)", .{
                err,
                runtime_config.event_data_buffer_log2,
                buf_size,
                buf_size / 1024,
            });
        }

        // Parse RING_BUFFER_COUNT environment variable
        if (std.process.getEnvVarOwned(allocator, "RING_BUFFER_COUNT")) |count_str| {
            defer allocator.free(count_str);
            if (std.fmt.parseInt(usize, count_str, 10)) |count| {
                if (count >= 1024 and count <= 1024 * 1024) { // 1K to 1M events
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
        } else |err| {
            log.info("RING_BUFFER_COUNT not set ({any}), using default: {d} events", .{
                err,
                runtime_config.batch_ring_buffer_size,
            });
        }

        return .{
            .args = cli_args,
            .runtime_config = runtime_config,
        };
    }

    /// Parse TRANSITION_RULES environment variable into a HashMap
    ///
    /// Format: "table1:col1,col2;table2:col3,col4"
    /// Example: "users:status,kyc_level;orders:state,payment_status"
    ///
    /// Returns:
    /// - HashMap mapping table names to arrays of column names
    /// - Empty HashMap if TRANSITION_RULES is not set or invalid
    ///
    /// Caller must call deinit() on the returned HashMap to free memory
    pub fn parseTransitionRules(allocator: std.mem.Allocator) !config.EventClassification.TransitionRules {
        var rules = config.EventClassification.TransitionRules.init(allocator);
        errdefer rules.deinit();

        const rules_str = std.process.getEnvVarOwned(allocator, "TRANSITION_RULES") catch |err| {
            if (err != error.EnvironmentVariableNotFound) {
                log.warn("Failed to read TRANSITION_RULES: {}", .{err});
            }
            return rules; // Return empty map
        };
        defer allocator.free(rules_str);

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

            // Split by colon to get table name and columns: "users:status,kyc_level"
            var colon_iter = std.mem.splitScalar(u8, trimmed, ':');
            const table_name = colon_iter.next() orelse {
                log.warn("Invalid TRANSITION_RULES entry (missing ':'): {s}", .{trimmed});
                continue;
            };
            const columns_str = colon_iter.next() orelse {
                log.warn("Invalid TRANSITION_RULES entry (no columns after ':'): {s}", .{trimmed});
                continue;
            };

            // Split columns by comma: "status,kyc_level"
            var col_list: std.ArrayList([]const u8) = .empty;
            errdefer col_list.deinit(allocator);

            var col_iter = std.mem.splitScalar(u8, columns_str, ',');
            while (col_iter.next()) |col| {
                const col_trimmed = std.mem.trim(u8, col, " \t\n\r");
                if (col_trimmed.len > 0) {
                    // Duplicate the string so it outlives the rules_str buffer
                    const col_owned = try allocator.dupe(u8, col_trimmed);
                    try col_list.append(allocator, col_owned);
                }
            }

            if (col_list.items.len == 0) {
                col_list.deinit(allocator);
                log.warn("No valid columns for table '{s}', skipping", .{table_name});
                continue;
            }

            // Duplicate table name and transfer ownership to HashMap
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
            // Free table name
            allocator.free(entry.key_ptr.*);
            // Free column names array
            for (entry.value_ptr.*) |col_name| {
                allocator.free(col_name);
            }
            allocator.free(entry.value_ptr.*);
        }
        rules.deinit();
    }
};
