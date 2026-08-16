const std = @import("std");
const nats = @import("nats");
const log = std.log.scoped(.mutation_listener);
const config = @import("config.zig");
const pg_conn = @import("pg_conn.zig");
const utils = @import("utils.zig");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;

pub const MutationListener = struct {
    allocator: std.mem.Allocator,
    thread: ?std.Thread = null,
    /// The one connection description every component shares. Held by pointer, like the
    /// snapshot listener does: this used to copy `pg_host`/`pg_port`/… out of
    /// RuntimeConfig and rebuild a conninfo string by hand, which silently ignored
    /// `DATABASE_URL` (kept in `PgConf.db_url`) and `sslmode`. With a URL set, every
    /// other component connected where it said and this thread dialled the PG_* defaults.
    pg_config: *const pg_conn.PgConf,
    nats_host: []const u8,
    nats_seed: ?[]const u8,
    io: std.Io,
    should_stop: *std.atomic.Value(bool),

    pub fn init(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        runtime_config: *const config.RuntimeConfig,
        io: std.Io,
        should_stop: *std.atomic.Value(bool),
    ) !*MutationListener {
        const self = try allocator.create(MutationListener);

        self.* = .{
            .allocator = allocator,
            .pg_config = pg_config,
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

        // 1. Connect to PostgreSQL (dedicated connection for mutations — the WAL
        //    stream's connection is in replication mode and cannot run ordinary SQL).
        //    `connInfo` is the shared builder, so DATABASE_URL and sslmode apply here
        //    exactly as they do everywhere else.
        const conninfo = self.pg_config.connInfo(self.allocator, false) catch |err| {
            log.err("Mutation listener: cannot build connection string: {}", .{err});
            return;
        };
        defer self.allocator.free(conninfo);

        const conn = c.PQconnectdb(conninfo.ptr);
        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            log.err("Mutation listener: Failed to connect to PostgreSQL: {s}", .{c.PQerrorMessage(conn)});
            c.PQfinish(conn);
            return;
        }
        defer c.PQfinish(conn);
        // Asked of libpq rather than read back from the config fields: with
        // DATABASE_URL set those fields hold the unused PG_* defaults, so printing them
        // would report a host this connection never dialled.
        // Role included: the whole point of the ingress connection is that it is NOT
        // the replication role, and a log line that cannot show which one connected
        // cannot show that the split is working.
        log.info("Mutation listener: connected to PostgreSQL {s}:{s} as '{s}'.", .{
            c.PQhost(conn), c.PQport(conn), c.PQuser(conn),
        });

        // 2. Connect to NATS JetStream Pull Consumer
        var cscnf = nats.protocol.ConsumerConfig{
            .durable_name = "bridge_mutations_worker",
            .ack_policy = "explicit",
            .deliver_policy = "all",
            // The server-side half of the poison-pill guard: even a failure the bridge
            // wrongly classifies as retryable stops after this many attempts.
            .max_deliver = config.Nats.mutation_max_deliver,
            // Not "mutation.>" spelled out: the comment used to say "from topology.json
            // prefix", which is the shape of the drift this centralisation exists to stop.
            .filter_subject = config.Nats.mutations_subject_wildcard,
        };

        const connect_opts = nats.protocol.ConnectOpts{
            .addr = self.nats_host,
            .nkey_seed = self.nats_seed,
        };

        var consumer = nats.Consumer.START(
            self.allocator,
            connect_opts,
            config.Nats.stream_mutations,
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
            if (c.PQstatus(conn) == c.CONNECTION_BAD) {
                log.warn("Mutation listener: PostgreSQL connection lost. Attempting to reconnect...", .{});
                c.PQreset(conn);
                if (c.PQstatus(conn) != c.CONNECTION_OK) {
                    log.err("Mutation listener: Reconnection failed: {s}", .{c.PQerrorMessage(conn)});
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
                
                self.handleMutation(payload, conn) catch |err| {
                    // Retrying a malformed payload cannot help: the bytes will not
                    // improve. Before this split, one bad message NAK'd forever at one
                    // attempt per second and the queue never advanced past it.
                    if (isPermanent(err)) {
                        log.err("🔴 Unprocessable mutation ({}): dead-lettering, not retrying", .{err});
                        self.deadLetter(msg, payload, err);
                        // ACK, not NACK: the message is handled — badly, but finally.
                        consumer.ACK(msg, true) catch consumer.REUSE(msg);
                        continue;
                    }

                    log.err("Failed to handle mutation, will retry: {}", .{err});
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

        // Every other thread announces its exit — a shutdown where one thread goes
        // quiet without saying so is indistinguishable from one that is stuck.
        log.info("🛑 Mutation listener stopped", .{});
    }


    /// Errors a retry can never fix.
    ///
    /// The distinction is the whole poison-pill guard: a transient failure (PostgreSQL
    /// restarting, a lock timeout) deserves redelivery, while a payload that does not
    /// decode will fail identically forever. `max_deliver` bounds the mistake in either
    /// direction, but classifying correctly is what keeps a healthy queue moving.
    ///
    /// `MutationFailed` — any SQL error — is deliberately treated as *transient*: it
    /// covers both a lost connection and a constraint violation, and until the reply
    /// channel exists there is no way to tell a client which one it hit. `max_deliver`
    /// catches the permanent ones after five attempts.
    fn isPermanent(err: anyerror) bool {
        return switch (err) {
            error.InvalidPayloadFormat,
            error.MissingTable,
            error.InvalidTableFormat,
            error.MissingOperation,
            error.InvalidOperationFormat,
            error.MissingHLC,
            error.InvalidHLCFormat,
            error.MissingPrimaryKey,
            error.InvalidPrimaryKeyFormat,
            error.MissingData,
            error.InvalidDataFormat,
            error.UnsupportedPayloadType,
            => true,
            else => false,
        };
    }

    /// Publish the rejected mutation to `mutation_error.<table>` so the failure is
    /// visible to whoever sent it, rather than vanishing on ACK.
    ///
    /// Best effort by design: if this publish fails there is nothing useful left to do,
    /// and refusing to ACK would restore the infinite loop it exists to prevent.
    fn deadLetter(
        self: *MutationListener,
        msg: *nats.Conn.AllocatedMSG,
        payload: []const u8,
        err: anyerror,
    ) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        // The table is parsed from the subject rather than the payload: the payload is
        // exactly what failed to parse, so it cannot be trusted to name itself.
        const subject = msg.letter.subject.body() orelse "";
        var it = std.mem.splitScalar(u8, subject, '.');
        var token: usize = 0;
        var table: []const u8 = "unknown";
        while (it.next()) |tok| : (token += 1) {
            if (token == config.Nats.mutation_token_table) {
                table = tok;
                break;
            }
        }

        // Named field, like the snapshot subject patterns: topology writes
        // "{[table]s}", so the argument is a struct, not a tuple.
        const err_subject = std.fmt.allocPrint(alloc, config.Nats.mutation_error_pattern, .{ .table = table }) catch return;
        const body = std.fmt.allocPrint(
            alloc,
            "{{\"error\":\"{s}\",\"subject\":\"{s}\",\"bytes\":{d}}}",
            .{ @errorName(err), subject, payload.len },
        ) catch return;

        log.warn("↩️  dead-letter → {s}: {s}", .{ err_subject, body });
        // TODO: publish once the listener holds a publish-capable NATS handle; the
        // consumer connection is pull-only today.
    }

    fn handleMutation(self: *MutationListener, payload_bytes: []const u8, conn: ?*c.PGconn) !void {
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
            conn,
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
            log.err("Mutation failed: {s}", .{c.PQerrorMessage(conn)});
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
