const std = @import("std");
const nats = @import("nats");
const Topology = @import("topology.zig");
const log = std.log.scoped(.mutation_listener);
const config = @import("config.zig");
const pg_conn = @import("pg_conn.zig");
const utils = @import("utils.zig");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;

/// What the catalog says about one edge-writable table.
///
/// **Every identifier the bridge puts into SQL comes from here, never from the client.**
/// A mutation payload supplies values; the table name arrives as a subject token the
/// broker vouched for; and the column and key names are read from `pg_attribute` /
/// `pg_index`. Before this, `INSERT INTO "{s}"` interpolated a client-supplied string,
/// so a table named `x" ; DROP TABLE …` closed the quote.
const TableMeta = struct {
    /// Primary key columns in key order — the `ON CONFLICT` target.
    pk_cols: [][]const u8,
    /// Every column, so a payload naming something else can be rejected before SQL.
    columns: [][]const u8,
    /// The column compared for last-write-wins.
    version_col: []const u8,
    /// The tombstone column, when the table has one. Null means a delete is physical.
    tombstone_col: ?[]const u8,

    fn deinit(self: *TableMeta, allocator: std.mem.Allocator) void {
        for (self.pk_cols) |col| allocator.free(col);
        allocator.free(self.pk_cols);
        for (self.columns) |col| allocator.free(col);
        allocator.free(self.columns);
        allocator.free(self.version_col);
        if (self.tombstone_col) |t| allocator.free(t);
    }

    fn hasColumn(self: *const TableMeta, name: []const u8) bool {
        for (self.columns) |col| {
            if (std.mem.eql(u8, col, name)) return true;
        }
        return false;
    }

    fn isPk(self: *const TableMeta, name: []const u8) bool {
        for (self.pk_cols) |col| {
            if (std.mem.eql(u8, col, name)) return true;
        }
        return false;
    }
};

/// Append a quoted SQL identifier, doubling any embedded `"`.
///
/// Every identifier reaching SQL passes through here. The names themselves come from the
/// catalog, so the escaping is belt-and-braces rather than the primary defence — but it
/// is what makes that claim checkable in one place.
fn appendIdent(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, ident: []const u8) !void {
    try out.append(alloc, '"');
    for (ident) |ch| {
        if (ch == '"') try out.append(alloc, '"');
        try out.append(alloc, ch);
    }
    try out.append(alloc, '"');
}

/// One mutation, as parsed from its subject and payload.
const Mutation = struct {
    principal: []const u8,
    table: []const u8,
    operation: Operation,

    const Operation = enum { insert, update, delete };
};

pub const MutationListener = struct {
    allocator: std.mem.Allocator,
    thread: ?std.Thread = null,
    /// The one connection description every component shares. Held by pointer, like the
    /// snapshot listener does: this used to copy `pg_host`/`pg_port`/… out of
    /// RuntimeConfig and rebuild a conninfo string by hand, which silently ignored
    /// `DATABASE_URL` (kept in `PgConf.db_url`) and `sslmode`. With a URL set, every
    /// other component connected where it said and this thread dialled the PG_* defaults.
    pg_config: *const pg_conn.PgConf,
    /// Wire names, read from topology.json at startup. See src/topology.zig.
    endpoint_topology: *const Topology.Topology,
    /// Where NATS is, resolved once in `bridge.zig`. This used to be `nats_host` read
    /// straight off RuntimeConfig **with no port at all**, so `NATS_URL` was ignored on
    /// this path alone and ingress dialled `NATS_HOST:4222` while everything else went
    /// where the URL said. See `Config.Nats.Endpoint`.
    endpoint: config.Nats.Endpoint,
    io: std.Io,
    should_stop: *std.atomic.Value(bool),
    /// Per-table version/tombstone column overrides, `SYNC_RULES`.
    sync_rules: *const config.EventClassification.TransitionRules,
    default_version_column: []const u8,
    /// table -> catalog facts, resolved on first use. See `TableMeta`.
    meta_cache: std.StringHashMap(TableMeta),

    pub fn init(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        endpoint: config.Nats.Endpoint,
        topology: *const Topology.Topology,
        sync_rules: *const config.EventClassification.TransitionRules,
        default_version_column: []const u8,
        io: std.Io,
        should_stop: *std.atomic.Value(bool),
    ) !*MutationListener {
        const self = try allocator.create(MutationListener);

        self.* = .{
            .allocator = allocator,
            .pg_config = pg_config,
            .endpoint = endpoint,
            .endpoint_topology = topology,
            .io = io,
            .should_stop = should_stop,
            .sync_rules = sync_rules,
            .default_version_column = default_version_column,
            .meta_cache = std.StringHashMap(TableMeta).init(allocator),
        };

        return self;
    }

    pub fn deinit(self: *MutationListener) void {
        var it = self.meta_cache.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            e.value_ptr.deinit(self.allocator);
        }
        self.meta_cache.deinit();
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
            .filter_subject = self.endpoint_topology.mutations_subject_wildcard,
        };

        const connect_opts = nats.protocol.ConnectOpts{
            .addr = self.endpoint.host,
            .port = self.endpoint.port,
            .user = self.endpoint.user,
            .pass = self.endpoint.pass,
            .nkey_seed = self.endpoint.seed,
        };

        var consumer = nats.Consumer.START(
            self.allocator,
            connect_opts,
            self.endpoint_topology.stream_mutations,
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
                // JetStream status messages (408 Request Timeout, 409, idle heartbeats)
                // arrive on the pull inbox with no reply-to. Recycle, never ack: the
                // vendored `Consumer.ack` unwraps `ReplyTo()` and panics on null. The
                // payload check below used to be the only guard, which held only because
                // status messages happen to be empty too — reply-to is the actual
                // distinction between a delivery and a control frame.
                if (msg.letter.ReplyTo() == null) {
                    consumer.REUSE(msg);
                    continue;
                }

                const payload = msg.letter.getPayload() orelse {
                    consumer.REUSE(msg);
                    continue;
                };

                // Subject first: it carries the identity, the table and the verb, and
                // all three are refused before a byte of the payload is decoded.
                const subject = msg.letter.subject.body() orelse "";
                const mutation = parseSubject(subject, self.endpoint_topology.subject_mutations_prefix) catch |err| {
                    log.err("🔴 Malformed mutation subject '{s}' ({}): dead-lettering", .{ subject, err });
                    self.deadLetter(&consumer, msg, payload, err);
                    consumer.ACK(msg, true) catch consumer.REUSE(msg);
                    continue;
                };

                if (isForbiddenTable(mutation.table)) {
                    log.err(
                        "🔴 '{s}' refused writes to '{s}': that table is the bridge's own, and a forged row in it would publish a fabricated schema to every client",
                        .{ mutation.principal, mutation.table },
                    );
                    self.deadLetter(&consumer, msg, payload, error.ForbiddenTable);
                    consumer.ACK(msg, true) catch consumer.REUSE(msg);
                    continue;
                }

                self.handleMutation(mutation, payload, conn) catch |err| {
                    // Retrying a malformed payload cannot help: the bytes will not
                    // improve. Before this split, one bad message NAK'd forever at one
                    // attempt per second and the queue never advanced past it.
                    if (isPermanent(err)) {
                        log.err("🔴 Unprocessable mutation ({}): dead-lettering, not retrying", .{err});
                        self.deadLetter(&consumer, msg, payload, err);
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
            // Subject-shaped and schema-shaped refusals: the same bytes will be refused
            // identically every time.
            error.MalformedSubject,
            error.UnknownOperation,
            error.ForbiddenTable,
            error.UnknownColumn,
            error.MissingVersion,
            error.NoVersionColumn,
            error.NoTombstoneColumn,
            error.NoPrimaryKey,
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
        consumer: *nats.Consumer,
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
        const err_subject = Topology.render(alloc, self.endpoint_topology.mutation_error_pattern, &.{.{ .name = "table", .value = table }}, null) catch return;
        const body = std.fmt.allocPrint(
            alloc,
            "{{\"error\":\"{s}\",\"subject\":\"{s}\",\"bytes\":{d}}}",
            .{ @errorName(err), subject, payload.len },
        ) catch return;

        // Published on the consumer's own connection — the same trick the snapshot
        // listener uses for `init.snap.error.<table>`. An earlier note here claimed the
        // pull consumer could not publish; it can (`nats.Consumer.PUBLISH`), and
        // `mutation_error.>` is one of the MUTATIONS stream's subjects, so the message
        // is stored rather than dropped for want of a stream.
        //
        // ⚠️ The subject sits deliberately *outside* `mutation.>`, which is what the
        // ingress consumer filters on. Republishing a failure under that prefix would
        // feed it straight back to this loop — a poison pill with a feedback loop.
        consumer.PUBLISH(err_subject, null, body) catch |perr| {
            log.err("↩️  dead-letter publish to {s} failed ({}): {s}", .{ err_subject, perr, body });
            return;
        };
        log.warn("↩️  dead-letter → {s}: {s}", .{ err_subject, body });
    }

    /// Parse `mutation.<principal>.<table>.<operation>`.
    ///
    /// The subject is the **only** trusted source of these three: NATS authorizes
    /// subjects, not payloads, so a client granted `publish: ["mutation.alice.>"]`
    /// physically cannot write as anyone else — while a `table` field in the body is
    /// simply whatever the client typed. Reading them here is not the bridge trusting
    /// the client; it is reading a claim the broker already checked.
    /// `mutations_prefix` is passed rather than read from a constant: the prefix is a
    /// topology name now, and a parser that hardcoded "mutation" would accept subjects a
    /// renamed deployment never issues — and reject the ones it does.
    fn parseSubject(subject: []const u8, mutations_prefix: []const u8) !Mutation {
        var tokens: [config.Nats.mutation_token_count][]const u8 = undefined;
        var n: usize = 0;
        var it = std.mem.splitScalar(u8, subject, '.');
        while (it.next()) |tok| {
            // Counted before indexing: a longer subject must be rejected, not truncated
            // to something that looks valid.
            if (n >= tokens.len) return error.MalformedSubject;
            tokens[n] = tok;
            n += 1;
        }
        if (n != config.Nats.mutation_token_count) return error.MalformedSubject;
        if (!std.mem.eql(u8, tokens[0], mutations_prefix)) return error.MalformedSubject;

        const principal = tokens[config.Nats.mutation_token_principal];
        const table = tokens[config.Nats.mutation_token_table];
        const op_text = tokens[config.Nats.mutation_token_operation];
        if (principal.len == 0 or table.len == 0) return error.MalformedSubject;

        const operation: Mutation.Operation =
            if (std.mem.eql(u8, op_text, "insert")) .insert else if (std.mem.eql(u8, op_text, "update")) .update else if (std.mem.eql(u8, op_text, "delete")) .delete else return error.UnknownOperation;

        return .{ .principal = principal, .table = table, .operation = operation };
    }

    /// Tables the ingress path must never write, whatever any grant says.
    ///
    /// `zebridge_ddl_events` is the escalation path: a forged row there makes the bridge
    /// publish a fabricated schema, suspension or tombstone to **every** client. The SQL
    /// helper refuses to grant on it, and this refuses again — a privilege check and a
    /// name check fail in different ways, and this one costs nothing.
    fn isForbiddenTable(table: []const u8) bool {
        return std.mem.eql(u8, table, "zebridge_ddl_events") or
            std.mem.eql(u8, table, "schema_migrations");
    }

    /// Read a table's identifiers from the catalog, once, and remember them.
    ///
    /// Cached because a mutation is already one synchronous round trip and this would
    /// double it. The cache is dropped for a table whenever a statement fails with
    /// `42703 undefined_column` or `42P01 undefined_table`, so a DDL change heals on the
    /// next attempt instead of requiring a restart.
    fn tableMeta(self: *MutationListener, conn: ?*c.PGconn, table: []const u8) !*const TableMeta {
        if (self.meta_cache.getPtr(table)) |cached| return cached;

        const alloc = self.allocator;

        // `$1::regclass` resolves through search_path and errors on a table that does not
        // exist — so an unknown name is rejected by PostgreSQL's own parser rather than
        // by string matching here.
        const query =
            \\SELECT a.attname,
            \\       COALESCE(k.ord, 0) AS pk_ord
            \\FROM pg_attribute a
            \\LEFT JOIN pg_index i ON i.indrelid = a.attrelid AND i.indisprimary
            \\LEFT JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
            \\  ON k.attnum = a.attnum
            \\WHERE a.attrelid = $1::regclass
            \\  AND a.attnum > 0 AND NOT a.attisdropped
            \\ORDER BY a.attnum
        ;

        const table_z = try alloc.dupeZ(u8, table);
        defer alloc.free(table_z);
        const params = [_]?[*:0]const u8{table_z.ptr};

        const res = c.PQexecParams(conn, query, 1, null, &params[0], null, null, 0);
        defer c.PQclear(res);

        if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
            log.err("Mutation listener: cannot resolve '{s}': {s}", .{ table, c.PQerrorMessage(conn) });
            return error.UnknownTable;
        }
        const rows: usize = @intCast(c.PQntuples(res));
        if (rows == 0) return error.UnknownTable;

        var columns: std.ArrayList([]const u8) = .empty;
        errdefer {
            for (columns.items) |col| alloc.free(col);
            columns.deinit(alloc);
        }
        var pk: std.ArrayList([]const u8) = .empty;
        errdefer {
            for (pk.items) |col| alloc.free(col);
            pk.deinit(alloc);
        }

        // Key order, not column order: `ON CONFLICT (a,b)` and `(b,a)` name the same
        // index, but the cursor and every error message read better in key order.
        var ordered: [64]?[]const u8 = @splat(null);
        var r: usize = 0;
        while (r < rows) : (r += 1) {
            const name = std.mem.span(c.PQgetvalue(res, @intCast(r), 0));
            const ord = std.fmt.parseInt(usize, std.mem.span(c.PQgetvalue(res, @intCast(r), 1)), 10) catch 0;
            const owned = try alloc.dupe(u8, name);
            try columns.append(alloc, owned);
            if (ord > 0 and ord <= ordered.len) ordered[ord - 1] = owned;
        }
        for (ordered) |maybe| {
            const col = maybe orelse continue;
            try pk.append(alloc, try alloc.dupe(u8, col));
        }
        if (pk.items.len == 0) return error.NoPrimaryKey;

        // SYNC_RULES wins over the global default; the tombstone is its optional second
        // column. Absent means deletes are physical for this table.
        var version_name: []const u8 = self.default_version_column;
        var tombstone_name: ?[]const u8 = null;
        if (self.sync_rules.get(table)) |cols| {
            if (cols.len > 0) version_name = cols[0];
            if (cols.len > 1) tombstone_name = cols[1];
        }

        var meta = TableMeta{
            .pk_cols = try pk.toOwnedSlice(alloc),
            .columns = try columns.toOwnedSlice(alloc),
            .version_col = try alloc.dupe(u8, version_name),
            .tombstone_col = if (tombstone_name) |t| try alloc.dupe(u8, t) else null,
        };
        errdefer meta.deinit(alloc);

        if (!meta.hasColumn(meta.version_col)) {
            log.err(
                "🔴 '{s}' has no version column '{s}': it is outbound-only. Preflight lists the candidates and the SYNC_RULES line to set.",
                .{ table, meta.version_col },
            );
            return error.NoVersionColumn;
        }
        if (meta.tombstone_col) |t| {
            if (!meta.hasColumn(t)) return error.NoTombstoneColumn;
        }

        const key_owned = try alloc.dupe(u8, table);
        errdefer alloc.free(key_owned);
        try self.meta_cache.put(key_owned, meta);
        return self.meta_cache.getPtr(table).?;
    }

    /// Forget one table's catalog facts, so the next mutation re-reads them.
    fn invalidate(self: *MutationListener, table: []const u8) void {
        if (self.meta_cache.fetchRemove(table)) |kv| {
            var meta = kv.value;
            self.allocator.free(kv.key);
            meta.deinit(self.allocator);
            log.info("Mutation listener: catalog cache dropped for '{s}'", .{table});
        }
    }

    /// Apply one mutation.
    ///
    /// Shape of the payload (PROTOCOL.md §7) — note what is **absent**: no table, no
    /// operation, no identity. Those are subject tokens.
    ///
    /// ```json
    /// { "key": {"id": 42}, "data": {…full row…}, "version": "…", "client_id": "c-8f3a" }
    /// ```
    fn handleMutation(
        self: *MutationListener,
        mutation: Mutation,
        payload_bytes: []const u8,
        conn: ?*c.PGconn,
    ) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const meta = try self.tableMeta(conn, mutation.table);

        var reader = std.Io.Reader.fixed(payload_bytes);
        var dummy_out: [0]u8 = .{};
        var writer = std.Io.Writer.fixed(&dummy_out);
        var packer = @import("msgpack").PackerIO.init(&reader, &writer);

        const payload = try packer.read(alloc);
        if (payload != .map) return error.InvalidPayloadFormat;
        const map = payload.map;

        const version_val = map.getByString("version") orelse return error.MissingVersion;
        const version_text = try self.payloadToString(alloc, version_val) orelse return error.MissingVersion;

        const key_val = map.getByString("key") orelse return error.MissingPrimaryKey;
        if (key_val != .map) return error.InvalidPrimaryKeyFormat;

        // Every key column must be present. A partial key would let `ON CONFLICT` match
        // a different row than the client meant, or a DELETE remove more rows than it
        // asked for.
        var key_values: std.ArrayList(?[*:0]const u8) = .empty;
        for (meta.pk_cols) |col| {
            const v = key_val.map.getByString(col) orelse return error.MissingPrimaryKey;
            const as_text = try self.payloadToString(alloc, v) orelse return error.MissingPrimaryKey;
            try key_values.append(alloc, as_text);
        }

        switch (mutation.operation) {
            .delete => try self.applyDelete(alloc, conn, meta, mutation, key_values.items, version_text),
            .insert, .update => try self.applyUpsert(alloc, conn, meta, mutation, map, version_text),
        }
    }

    /// INSERT … ON CONFLICT (<catalog pk>) DO UPDATE … WHERE stored.version < incoming.
    ///
    /// Column names come from `meta.columns`; the payload only decides which of them
    /// carry values. A payload naming a column the table does not have is rejected here
    /// rather than becoming SQL.
    fn applyUpsert(
        self: *MutationListener,
        alloc: std.mem.Allocator,
        conn: ?*c.PGconn,
        meta: *const TableMeta,
        mutation: Mutation,
        map: anytype,
        version_text: [*:0]const u8,
    ) !void {
        const data_val = map.getByString("data") orelse return error.MissingData;
        if (data_val != .map) return error.InvalidDataFormat;

        var cols: std.ArrayList([]const u8) = .empty;
        var vals: std.ArrayList(?[*:0]const u8) = .empty;

        var it = data_val.map.map.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.* != .str) continue;
            const name = entry.key_ptr.str.value();
            // The catalog is the allowlist. An unknown name never reaches SQL.
            if (!meta.hasColumn(name)) {
                log.err("🔴 '{s}' has no column '{s}' — rejecting mutation from '{s}'", .{ mutation.table, name, mutation.principal });
                return error.UnknownColumn;
            }
            if (std.mem.eql(u8, name, meta.version_col)) continue; // set from `version`
            try cols.append(alloc, name);
            try vals.append(alloc, try self.payloadToString(alloc, entry.value_ptr.*));
        }

        // The version is the bridge's to write, from the message's `version` field —
        // never from `data`, so a client cannot claim one value and stamp another.
        try cols.append(alloc, meta.version_col);
        try vals.append(alloc, version_text);

        var sql: std.ArrayListUnmanaged(u8) = .empty;
        try sql.appendSlice(alloc, "INSERT INTO ");
        try appendIdent(&sql, alloc, mutation.table);
        try sql.appendSlice(alloc, " (");
        for (cols.items, 0..) |col, i| {
            if (i > 0) try sql.appendSlice(alloc, ", ");
            try appendIdent(&sql, alloc, col);
        }
        try sql.appendSlice(alloc, ") VALUES (");
        for (cols.items, 0..) |_, i| {
            if (i > 0) try sql.appendSlice(alloc, ", ");
            try sql.appendSlice(alloc, try std.fmt.allocPrint(alloc, "${d}", .{i + 1}));
        }
        try sql.appendSlice(alloc, ") ON CONFLICT (");
        for (meta.pk_cols, 0..) |col, i| {
            if (i > 0) try sql.appendSlice(alloc, ", ");
            try appendIdent(&sql, alloc, col);
        }
        try sql.appendSlice(alloc, ") DO UPDATE SET ");
        var set_count: usize = 0;
        for (cols.items) |col| {
            if (meta.isPk(col)) continue; // never reassign the key it matched on
            if (set_count > 0) try sql.appendSlice(alloc, ", ");
            try appendIdent(&sql, alloc, col);
            try sql.appendSlice(alloc, " = EXCLUDED.");
            try appendIdent(&sql, alloc, col);
            set_count += 1;
        }
        // `IS NULL OR` because a NULL stored version makes the comparison NULL, which
        // would reject every write to a row that has never been stamped.
        try sql.appendSlice(alloc, " WHERE ");
        try appendIdent(&sql, alloc, mutation.table);
        try sql.appendSlice(alloc, ".");
        try appendIdent(&sql, alloc, meta.version_col);
        try sql.appendSlice(alloc, " IS NULL OR ");
        try appendIdent(&sql, alloc, mutation.table);
        try sql.appendSlice(alloc, ".");
        try appendIdent(&sql, alloc, meta.version_col);
        try sql.appendSlice(alloc, " < EXCLUDED.");
        try appendIdent(&sql, alloc, meta.version_col);

        try self.exec(alloc, conn, mutation, sql.items, vals.items);
    }

    /// A delete from the edge is an **UPDATE**, not a DELETE and not an upsert.
    ///
    /// Not a DELETE: the `BEFORE DELETE` trigger stamps `now()`, which is *arrival* time
    /// — so a client that deleted at 09:00 and reconnected at 17:00 would beat a 10:00
    /// edit that should have won. The bridge stamps the client's own version instead.
    ///
    /// Not an upsert: if the row does not exist there is nothing to tombstone, and
    /// inserting one would create a phantom row of key columns and nulls.
    ///
    /// With no tombstone column the delete is physical, which is the documented weaker
    /// guarantee: an offline client's queued edit can then resurrect the row.
    fn applyDelete(
        self: *MutationListener,
        alloc: std.mem.Allocator,
        conn: ?*c.PGconn,
        meta: *const TableMeta,
        mutation: Mutation,
        key_values: []const ?[*:0]const u8,
        version_text: [*:0]const u8,
    ) !void {
        var sql: std.ArrayListUnmanaged(u8) = .empty;
        var params: std.ArrayList(?[*:0]const u8) = .empty;

        if (meta.tombstone_col) |tombstone| {
            try sql.appendSlice(alloc, "UPDATE ");
            try appendIdent(&sql, alloc, mutation.table);
            try sql.appendSlice(alloc, " SET ");
            try appendIdent(&sql, alloc, tombstone);
            try sql.appendSlice(alloc, " = $1, ");
            try appendIdent(&sql, alloc, meta.version_col);
            try sql.appendSlice(alloc, " = $1");
            try params.append(alloc, version_text);
        } else {
            try sql.appendSlice(alloc, "DELETE FROM ");
            try appendIdent(&sql, alloc, mutation.table);
            try params.append(alloc, version_text);
        }

        try sql.appendSlice(alloc, " WHERE ");
        for (meta.pk_cols, key_values, 0..) |col, val, i| {
            if (i > 0) try sql.appendSlice(alloc, " AND ");
            try appendIdent(&sql, alloc, col);
            try sql.appendSlice(alloc, try std.fmt.allocPrint(alloc, " = ${d}", .{params.items.len + 1}));
            try params.append(alloc, val);
        }
        // Same last-write-wins guard as the upsert: a stale delete must not win.
        try sql.appendSlice(alloc, " AND (");
        try appendIdent(&sql, alloc, meta.version_col);
        try sql.appendSlice(alloc, " IS NULL OR ");
        try appendIdent(&sql, alloc, meta.version_col);
        try sql.appendSlice(alloc, " < $1)");

        try self.exec(alloc, conn, mutation, sql.items, params.items);
    }

    /// Run the statement, and heal the catalog cache when the schema has moved.
    fn exec(
        self: *MutationListener,
        alloc: std.mem.Allocator,
        conn: ?*c.PGconn,
        mutation: Mutation,
        sql: []const u8,
        params: []const ?[*:0]const u8,
    ) !void {
        const sql_z = try alloc.dupeZ(u8, sql);
        log.debug("mutation [{s}] {s}", .{ mutation.principal, sql_z });

        const res = c.PQexecParams(
            conn,
            sql_z.ptr,
            @intCast(params.len),
            null,
            if (params.len > 0) &params[0] else null,
            null,
            null,
            0,
        );
        defer c.PQclear(res);

        if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
            const state = c.PQresultErrorField(res, c.PG_DIAG_SQLSTATE);
            const sqlstate: []const u8 = if (state != null) std.mem.span(state) else "";
            // 42703 undefined_column / 42P01 undefined_table: the cached layout is stale,
            // so drop it and let the retry re-read the catalog rather than failing until
            // the next restart.
            if (std.mem.eql(u8, sqlstate, "42703") or std.mem.eql(u8, sqlstate, "42P01")) {
                self.invalidate(mutation.table);
            }
            log.err("Mutation failed [{s}] on '{s}': {s}", .{ mutation.principal, mutation.table, c.PQerrorMessage(conn) });
            return error.MutationFailed;
        }

        // 0 rows means the guard rejected it: the stored version is newer, so this write
        // lost. Not an error — it is last-write-wins working — but invisible to the
        // client until the reply channel exists.
        const affected = std.mem.span(c.PQcmdTuples(res));
        if (std.mem.eql(u8, affected, "0")) {
            log.info("↩️  stale mutation [{s}] on '{s}': a newer version is stored", .{ mutation.principal, mutation.table });
        } else {
            log.info("✅ mutation applied [{s}] on '{s}' ({s} row)", .{ mutation.principal, mutation.table, affected });
        }
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

const testing = std.testing;

test "subject - the grammar is the authorization surface" {
    const m = try MutationListener.parseSubject("mutation.a3f9c1.users.insert", "mutation");
    try testing.expectEqualStrings("a3f9c1", m.principal);
    try testing.expectEqualStrings("users", m.table);
    try testing.expect(m.operation == .insert);
}

test "subject - wrong token count is refused, never truncated" {
    // Truncating a longer subject to the first four tokens would let
    // `mutation.alice.users.insert.extra` be read as a valid write.
    try testing.expectError(error.MalformedSubject, MutationListener.parseSubject("mutation.alice.users", "mutation"));
    try testing.expectError(error.MalformedSubject, MutationListener.parseSubject("mutation.alice.users.insert.extra", "mutation"));
    try testing.expectError(error.MalformedSubject, MutationListener.parseSubject("cmd.alice.users.insert", "mutation"));
    try testing.expectError(error.MalformedSubject, MutationListener.parseSubject("mutation..users.insert", "mutation"));
    try testing.expectError(error.UnknownOperation, MutationListener.parseSubject("mutation.alice.users.truncate", "mutation"));
}

test "the bridge's own tables are never writable from the edge" {
    // A forged row here publishes a fabricated schema to every client.
    try testing.expect(MutationListener.isForbiddenTable("zebridge_ddl_events"));
    try testing.expect(MutationListener.isForbiddenTable("schema_migrations"));
    try testing.expect(!MutationListener.isForbiddenTable("users"));
}

test "identifiers are quoted, and an embedded quote cannot escape" {
    const alloc = testing.allocator;
    var out: std.ArrayListUnmanaged(u8) = .empty;
    defer out.deinit(alloc);

    try appendIdent(&out, alloc, "users");
    try testing.expectEqualStrings("\"users\"", out.items);

    out.clearRetainingCapacity();
    try appendIdent(&out, alloc, "x\" ; DROP TABLE users; --");
    try testing.expectEqualStrings("\"x\"\" ; DROP TABLE users; --\"", out.items);
}
