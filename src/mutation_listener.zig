const std = @import("std");
const nats = @import("nats");
const log = std.log.scoped(.mutation_listener);
const topology = @import("topology");
const config = @import("config.zig");
const args = @import("args.zig");
const utils = @import("utils.zig");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;

pub const MutationListener = struct {
    allocator: std.mem.Allocator,
    thread: ?std.Thread = null,
    pg_host: []const u8,
    pg_port: u16,
    pg_db: []const u8,
    pg_user: []const u8,
    pg_pass: []const u8,
    nats_host: []const u8,
    nats_seed: ?[]const u8,
    io: std.Io,
    should_stop: *std.atomic.Value(bool),

    pub fn init(allocator: std.mem.Allocator, runtime_config: config.RuntimeConfig, cli_args: args.Args, io: std.Io, should_stop: *std.atomic.Value(bool)) !*MutationListener {
        _ = cli_args;
        const self = try allocator.create(MutationListener);

        self.* = .{
            .allocator = allocator,
            .pg_host = runtime_config.pg_host,
            .pg_port = runtime_config.pg_port,
            .pg_db = runtime_config.pg_database,
            .pg_user = runtime_config.pg_user,
            .pg_pass = runtime_config.pg_password,
            .nats_host = runtime_config.nats_host,
            .nats_seed = runtime_config.nats_seed,
            .io = io,
            .should_stop = should_stop,
        };

        return self;
    }

    pub fn deinit(self: *MutationListener) void {
        self.allocator.destroy(self);
    }

    pub fn start(self: *MutationListener) !void {
        self.thread = try std.Thread.spawn(.{}, listenLoop, .{self});
    }

    pub fn join(self: *MutationListener) void {
        if (self.thread) |th| {
            th.join();
        }
    }

    fn listenLoop(self: *MutationListener) void {
        log.info("Mutation listener thread started. Connecting to NATS and PostgreSQL...", .{});

        // 1. Connect to PostgreSQL (Dedicated connection for mutations)
        const conninfo = std.fmt.allocPrint(
            self.allocator,
            "host={s} port={d} dbname={s} user={s} password={s}",
            .{ self.pg_host, self.pg_port, self.pg_db, self.pg_user, self.pg_pass },
        ) catch return;
        defer self.allocator.free(conninfo);

        const pg_conn = c.PQconnectdb(conninfo.ptr);
        if (c.PQstatus(pg_conn) != c.CONNECTION_OK) {
            log.err("Mutation listener: Failed to connect to PostgreSQL: {s}", .{c.PQerrorMessage(pg_conn)});
            c.PQfinish(pg_conn);
            return;
        }
        defer c.PQfinish(pg_conn);
        log.info("Mutation listener: Connected to PostgreSQL.", .{});

        // 2. Connect to NATS JetStream Pull Consumer
        var cscnf = nats.protocol.ConsumerConfig{
            .durable_name = "bridge_mutations_worker",
            .ack_policy = "explicit",
            .deliver_policy = "all",
            .filter_subject = "mutation.>", // From topology.json prefix
        };

        const connect_opts = nats.protocol.ConnectOpts{
            .addr = self.nats_host,
            .nkey_seed = self.nats_seed,
        };

        var consumer = nats.Consumer.START(
            self.allocator,
            connect_opts,
            "MUTATIONS", // Stream name
            &cscnf,
            self.io,
        ) catch |err| {
            log.err("Mutation listener: Failed to start NATS consumer: {}", .{err});
            return;
        };
        defer nats.Consumer.STOP(&consumer, false);

        log.info("Mutation listener: ✅ Ready! Pulling mutations from JetStream...", .{});

        // 3. Pull Loop
        while (!self.should_stop.load(.seq_cst)) {
            // Ensure PostgreSQL connection is alive
            if (c.PQstatus(pg_conn) == c.CONNECTION_BAD) {
                log.warn("Mutation listener: PostgreSQL connection lost. Attempting to reconnect...", .{});
                c.PQreset(pg_conn);
                if (c.PQstatus(pg_conn) != c.CONNECTION_OK) {
                    log.err("Mutation listener: Reconnection failed: {s}", .{c.PQerrorMessage(pg_conn)});
                    utils.sleep(2 * std.time.ns_per_s);
                    continue; // Skip pulling until we reconnect
                }
                log.info("Mutation listener: Reconnected to PostgreSQL.", .{});
            }

            // Pull next message with a 500ms timeout for faster graceful shutdown
            const timeout_ns = nats.protocol.SECNS / 2;
            if (consumer.CONSUME(timeout_ns) catch null) |msg| {
                // JetStream sends 408 Request Timeout when no messages are available.
                // It has no payload. We MUST reuse the message envelope.
                const payload = msg.letter.getPayload() orelse {
                    consumer.REUSE(msg);
                    continue;
                };
                
                self.handleMutation(payload, pg_conn) catch |err| {
                    log.err("Failed to handle mutation: {}", .{err});
                    // We NAck to let it retry immediately or according to backoff
                    consumer.NACK(msg, true) catch {
                        consumer.REUSE(msg);
                    };
                    utils.sleep(1 * std.time.ns_per_s);
                    continue;
                };

                consumer.ACK(msg, true) catch {
                    consumer.REUSE(msg);
                };
            } else {
                // Timeout, just loop again
                utils.sleep(100 * std.time.ns_per_ms);
            }
        }
    }

    fn handleMutation(self: *MutationListener, payload_bytes: []const u8, pg_conn: ?*c.PGconn) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        var reader = std.Io.Reader.fixed(payload_bytes);
        var dummy_out: [0]u8 = .{};
        var writer = std.Io.Writer.fixed(&dummy_out);
        var packer = @import("msgpack").PackerIO.init(&reader, &writer);
        
        const payload = try packer.read(alloc);
        // Arena will clean up everything, including payload

        if (payload != .map) return error.InvalidPayloadFormat;
        const map = payload.map;

        const table_val = map.getByString("table") orelse return error.MissingTable;
        if (table_val != .str) return error.InvalidTableFormat;
        const table_name = table_val.str.value();

        const op_val = map.getByString("operation") orelse return error.MissingOperation;
        if (op_val != .str) return error.InvalidOperationFormat;
        const op_name = op_val.str.value();

        const hlc_val = map.getByString("hlc") orelse return error.MissingHLC;
        if (hlc_val != .str) return error.InvalidHLCFormat;
        const hlc_str = hlc_val.str.value();

        log.info("Applying mutation: {s} on table {s} at {s}", .{op_name, table_name, hlc_str});
        
        const pk_val = map.getByString("primary_key") orelse return error.MissingPrimaryKey;
        if (pk_val != .map) return error.InvalidPrimaryKeyFormat;
        const pk_map = pk_val.map;

        var columns: std.ArrayList([]const u8) = .empty;
        var param_vals: std.ArrayList(?[*:0]const u8) = .empty;

        const is_delete = std.mem.eql(u8, op_name, "DELETE");

        // Iterate over fields to update
        if (is_delete) {
            // For DELETE, we only set PKs + _deleted + _hlc
            var pk_it = pk_map.map.iterator();
            while (pk_it.next()) |entry| {
                if (entry.key_ptr.* != .str) continue;
                try columns.append(alloc, entry.key_ptr.str.value());
                try param_vals.append(alloc, try self.payloadToString(alloc, entry.value_ptr.*));
            }
        } else {
            // For INSERT/UPDATE, we set all data fields
            const data_val = map.getByString("data") orelse return error.MissingData;
            if (data_val != .map) return error.InvalidDataFormat;
            const data_map = data_val.map;

            var data_it = data_map.map.iterator();
            while (data_it.next()) |entry| {
                if (entry.key_ptr.* != .str) continue;
                try columns.append(alloc, entry.key_ptr.str.value());
                try param_vals.append(alloc, try self.payloadToString(alloc, entry.value_ptr.*));
            }
        }

        // Add _hlc
        try columns.append(alloc, "_hlc");
        const hlc_str_fmt = try std.fmt.allocPrint(alloc, "{s}", .{hlc_str});
        const hlc_z = try alloc.dupeZ(u8, hlc_str_fmt);
        try param_vals.append(alloc, hlc_z.ptr);

        // Add _deleted
        try columns.append(alloc, "_deleted");
        if (is_delete) {
            try param_vals.append(alloc, "true");
        } else {
            try param_vals.append(alloc, "false");
        }

        // Generate SQL
        // INSERT INTO table (col1, col2, _hlc, _deleted) VALUES ($1, $2, $3, $4)
        // ON CONFLICT (pk1, pk2) DO UPDATE
        // SET col1 = EXCLUDED.col1, ...
        // WHERE table._hlc IS NULL OR table._hlc < EXCLUDED._hlc
        
        var sql_cols: std.ArrayList(u8) = .empty;
        var sql_vals: std.ArrayList(u8) = .empty;
        var sql_sets: std.ArrayList(u8) = .empty;
        var sql_pks: std.ArrayList(u8) = .empty;

        for (columns.items, 0..) |col, i| {
            if (i > 0) {
                try sql_cols.appendSlice(alloc, ", ");
                try sql_vals.appendSlice(alloc, ", ");
                try sql_sets.appendSlice(alloc, ", ");
            }
            try sql_cols.appendSlice(alloc, try std.fmt.allocPrint(alloc, "\"{s}\"", .{col}));
            try sql_vals.appendSlice(alloc, try std.fmt.allocPrint(alloc, "${d}", .{i + 1}));
            try sql_sets.appendSlice(alloc, try std.fmt.allocPrint(alloc, "\"{s}\" = EXCLUDED.\"{s}\"", .{col, col}));
        }

        var pk_count: usize = 0;
        var pk_it = pk_map.map.iterator();
        while (pk_it.next()) |entry| {
            if (entry.key_ptr.* != .str) continue;
            if (pk_count > 0) try sql_pks.appendSlice(alloc, ", ");
            try sql_pks.appendSlice(alloc, try std.fmt.allocPrint(alloc, "\"{s}\"", .{entry.key_ptr.str.value()}));
            pk_count += 1;
        }

        const sql_fmt = try std.fmt.allocPrint(
            alloc,
            "INSERT INTO \"{s}\" ({s}) VALUES ({s}) ON CONFLICT ({s}) DO UPDATE SET {s} WHERE \"{s}\"._hlc IS NULL OR \"{s}\"._hlc < EXCLUDED._hlc;",
            .{ table_name, sql_cols.items, sql_vals.items, sql_pks.items, sql_sets.items, table_name, table_name }
        );
        const sql = try alloc.dupeZ(u8, sql_fmt);

        log.info("Executing UPSERT: {s}", .{sql});

        const res = c.PQexecParams(
            pg_conn,
            sql.ptr,
            @intCast(param_vals.items.len),
            null, // paramTypes
            param_vals.items.ptr,
            null, // paramLengths
            null, // paramFormats
            0     // resultFormat (text)
        );
        defer c.PQclear(res);

        if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
            log.err("Mutation failed: {s}", .{c.PQerrorMessage(pg_conn)});
            return error.MutationFailed;
        }

        log.info("Mutation applied successfully.", .{});
    }

    fn payloadToString(self: *MutationListener, alloc: std.mem.Allocator, payload: @import("msgpack").Payload) !?[*:0]const u8 {
        _ = self;
        switch (payload) {
            .nil => return null,
            .bool => |b| return if (b) "true" else "false",
            .int => |i| {
                const s = try alloc.dupeZ(u8, try std.fmt.allocPrint(alloc, "{d}", .{i}));
                return s.ptr;
            },
            .uint => |u| {
                const s = try alloc.dupeZ(u8, try std.fmt.allocPrint(alloc, "{d}", .{u}));
                return s.ptr;
            },
            .float => |f| {
                const s = try alloc.dupeZ(u8, try std.fmt.allocPrint(alloc, "{d}", .{f}));
                return s.ptr;
            },
            .str => |str| {
                const s = try alloc.dupeZ(u8, str.value());
                return s.ptr;
            },
            else => return error.UnsupportedPayloadType,
        }
    }
};
