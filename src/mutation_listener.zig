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
    /// Why the last `exec` failed, kept so the verdict can say more than "it failed".
    ///
    /// Fixed buffers rather than allocations: this is written on an error path that must
    /// not itself be able to fail, and read a few microseconds later on the same thread.
    /// `PQerrorMessage` points into the connection and is overwritten by the next call,
    /// so it has to be copied *now* or not at all.
    last_sqlstate_buf: [8]u8 = undefined,
    last_sqlstate_len: usize = 0,
    last_error_buf: [220]u8 = undefined,
    last_error_len: usize = 0,

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
                    // `warn`, not `err` — and not `info` either. Client-caused like the
                    // refusals below, so it is not the operator's to fix. But it is the
                    // one client fault where **nobody is told**: the principal lives in
                    // the subject, and the subject is what failed to parse, so there is
                    // no address to send a verdict to. Worth keeping visible for exactly
                    // that reason.
                    log.warn("⚠️  Malformed mutation subject '{s}' ({}): dead-lettering, and no verdict is addressable", .{ subject, err });
                    self.deadLetter(&consumer, msg, payload, err);
                    consumer.ACK(msg, true) catch consumer.REUSE(msg);
                    continue;
                };

                if (isForbiddenTable(mutation.table)) {
                    // Policy, not a fault: a client reaching for this table is refused by
                    // design. Worth tracing — it is the one refusal that would matter if
                    // it ever *succeeded* — but it asks nothing of the operator.
                    log.info(
                        "⛔ '{s}' refused writes to '{s}': the bridge's own table, where a forged row would publish a fabricated schema to every client",
                        .{ mutation.principal, mutation.table },
                    );
                    self.deadLetter(&consumer, msg, payload, error.ForbiddenTable);
                    consumer.ACK(msg, true) catch consumer.REUSE(msg);
                    continue;
                }

                // Each attempt starts with no remembered reason, so a verdict can only
                // ever quote a failure from *this* message.
                self.clearFailure();
                self.handleMutation(mutation, payload, conn) catch |err| {
                    // Retrying a malformed payload cannot help: the bytes will not
                    // improve. Before this split, one bad message NAK'd forever at one
                    // attempt per second and the queue never advanced past it.
                    if (isPermanent(err)) {
                        if (isOperatorFault(err)) {
                            log.err(
                                "🔴 '{s}' cannot accept writes ({}): the schema and SYNC_RULES disagree. Preflight reports this at boot.",
                                .{ mutation.table, err },
                            );
                        } else {
                            // Traced, not raised: the client is the only one who can fix a
                            // payload, and the verdict below is how they are told.
                            log.info("⛔ Mutation refused [{s}] on '{s}': {} (not retrying)", .{ mutation.principal, mutation.table, err });
                        }
                        self.deadLetter(&consumer, msg, payload, err);
                        self.publishVerdict(&consumer, msg, mutation.principal, "rejected", @errorName(err));
                        // ACK, not NACK: the message is handled — badly, but finally.
                        consumer.ACK(msg, true) catch consumer.REUSE(msg);
                        continue;
                    }

                    // Every SQL error lands here, because the bridge cannot tell a
                    // dropped connection from `permission denied` — so both get the full
                    // retry budget. That is the right default and it is also why a client
                    // used to learn nothing: after the last attempt the server stops
                    // redelivering and **nobody is told anything at all**.
                    //
                    // The delivery count is in the message's own ack subject, so the
                    // bridge can tell when it is out of attempts. On that last one, stop
                    // pretending it is transient: say why, and ACK. A genuinely transient
                    // failure still gets all five tries first.
                    const meta = parseAckMeta(msg.letter.ReplyTo() orelse "");
                    const out_of_attempts = if (meta) |m| m.delivered >= config.Nats.mutation_max_deliver else false;
                    // A SQLSTATE the server returned and we recognise as permanent ends
                    // the retries now: waiting cannot change a privilege or a constraint.
                    const hopeless = sqlstateIsPermanent(self.lastSqlstate());
                    const final = out_of_attempts or hopeless;

                    if (final) {
                        if (hopeless) {
                            // `info`: PostgreSQL evaluated the statement and refused it,
                            // which is the boundary doing its job. Nothing here is the
                            // operator's to fix — if the *grant* were the mistake,
                            // preflight would already have said so at boot, because
                            // SYNC_RULES naming an ungrantable table is checked there.
                            log.info(
                                "⛔ Mutation refused [{s}] on '{s}': SQLSTATE {s} (not retrying)",
                                .{ mutation.principal, mutation.table, self.lastSqlstate() },
                            );
                        } else {
                            log.err(
                                "🔴 Mutation failed on the last of {d} attempts [{s}] on '{s}': {} ({s})",
                                .{ config.Nats.mutation_max_deliver, mutation.principal, mutation.table, err, self.lastSqlstate() },
                            );
                        }
                        self.deadLetter(&consumer, msg, payload, err);
                        self.publishVerdict(&consumer, msg, mutation.principal, if (hopeless) "rejected" else "failed", @errorName(err));
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
        //
        // ⚠️ But the *subject* may be the thing that failed, so its token positions cannot
        // be trusted either. Reading position 2 blindly turned
        // `mutation.test_types.insert` — a client still on the 3-token grammar — into a
        // dead-letter on `mutation_error.insert`: a table that does not exist, on a
        // subject nobody is listening to. The client never learns why its write vanished.
        //
        // So the token count is checked first, and an unparseable subject is routed to
        // `unknown` — honest, and at least a subject an operator can subscribe to.
        const subject = msg.letter.subject.body() orelse "";
        var table: []const u8 = "unknown";
        var count: usize = 0;
        var counter = std.mem.splitScalar(u8, subject, '.');
        while (counter.next()) |_| count += 1;
        if (count == config.Nats.mutation_token_count) {
            var it = std.mem.splitScalar(u8, subject, '.');
            var token: usize = 0;
            while (it.next()) |tok| : (token += 1) {
                if (token == config.Nats.mutation_token_table) {
                    table = tok;
                    break;
                }
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
        // `debug`, not `warn`. The failure itself is already reported at error level by
        // the caller, and the verdict that goes to the client is logged at info — so this
        // line was the third of three for one event. It is also the least informative of
        // them: `mutation_error.<table>` carries no SQLSTATE and no msg_id, and since
        // clients are no longer granted that subject its only audience is an operator
        // watching the per-table feed, who can see the message on the stream itself.
        //
        // ⚠️ A *failed* dead-letter publish stays at error level above: that one means an
        // operator's failure feed has a hole in it, which nothing else would report.
        log.debug("↩️  dead-letter → {s}: {s}", .{ err_subject, body });
    }

    /// Publish a per-write verdict to `mutation_ack.<principal>.<msg_id>`.
    ///
    /// This is the piece that closes the loop, and it exists because nothing else could:
    ///
    ///  - a **PubAck** is emitted by the server at publish time and means "stored", not
    ///    "applied" — the bridge may not have been running;
    ///  - a **`-NAK`** is a fixed token addressed to the server, with no payload and no
    ///    route back to a publisher who is not subscribed to anything;
    ///  - the **`$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES`** advisory does carry the
    ///    stream sequence, but it is ephemeral, lives in a system subject space no client
    ///    should be granted, and says only "five attempts", never why.
    ///
    /// So the verdict is an ordinary message on an ordinary subject: durable in MUTATIONS
    /// for as long as the stream keeps it, which is what lets a client that was offline
    /// collect the verdicts it missed — the one thing none of the three above can do.
    ///
    /// ⚠️ Keyed by the client's `Nats-Msg-Id`, and **silently skipped without one**. A
    /// verdict nobody can match to a queued write is noise, and inventing a key would be
    /// worse: it would look addressed while reaching no one.
    fn publishVerdict(
        self: *MutationListener,
        consumer: *nats.Consumer,
        msg: *nats.Conn.AllocatedMSG,
        principal: []const u8,
        status: []const u8,
        reason: []const u8,
    ) void {
        const msg_id = msgIdOf(msg) orelse {
            log.debug("no Nats-Msg-Id on this mutation; no verdict addressable", .{});
            return;
        };

        // ⚠️ Client-supplied, and it becomes the last token of a subject. A value carrying
        // a dot silently adds tokens; `*` and `>` publish as literals but match broadly
        // when someone subscribes with them. The principal comes first in the pattern, so
        // a crafted id cannot climb above `alice` — but it can reshape what follows, and a
        // verdict on a subject nobody expects is a verdict nobody receives.
        //
        // No verdict rather than a sanitised one: the client matches on the id it chose,
        // so a rewritten id would arrive unmatchable. Its own id, its own problem.
        if (!utils.isSubjectToken(msg_id)) {
            log.warn(
                "⚠️  Nats-Msg-Id is not a valid subject token ('{s}'): no verdict can be addressed for this write",
                .{msg_id},
            );
            return;
        }

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const subject = Topology.render(alloc, self.endpoint_topology.mutation_ack_pattern, &.{
            .{ .name = "principal", .value = principal },
            .{ .name = "msg_id", .value = msg_id },
        }, null) catch return;

        // `seq` is included even though the subject already identifies the write: it is
        // the number the client got back in its PubAck, so it lets a client correlate
        // without having stored the msg_id it chose — and it is what an operator greps
        // for when reconciling against `nats stream view MUTATIONS`.
        const seq: u64 = if (msg.letter.ReplyTo()) |r| blk: {
            const meta = parseAckMeta(r) orelse break :blk 0;
            break :blk meta.stream_seq;
        } else 0;

        const body = std.fmt.allocPrint(
            alloc,
            "{{\"status\":\"{s}\",\"reason\":\"{s}\",\"sqlstate\":\"{s}\",\"detail\":\"{s}\",\"seq\":{d}}}",
            .{ status, reason, self.lastSqlstate(), self.lastError(), seq },
        ) catch return;

        // ⚠️ Outside `mutation.>`, like the dead-letter subject and for the same reason:
        // the ingress consumer filters on that prefix, so a verdict published under it
        // would be consumed by this loop as if it were a write — a feedback loop that
        // ends in a poison pill.
        consumer.PUBLISH(subject, null, body) catch |perr| {
            log.err("verdict publish to {s} failed ({}): {s}", .{ subject, perr, body });
            return;
        };
        log.info("📮 verdict → {s}: {s}", .{ subject, body });
    }

    /// Copy the reason out of libpq before the next call overwrites it.
    ///
    /// Truncation is deliberate and silent: a verdict is a signal, not a log. The full
    /// text is already on the operator's side via `log.err`, and a client only needs
    /// enough to tell "you may not do that" from "try again".
    fn rememberFailure(self: *MutationListener, sqlstate: []const u8, message: []const u8) void {
        const s_len = @min(sqlstate.len, self.last_sqlstate_buf.len);
        @memcpy(self.last_sqlstate_buf[0..s_len], sqlstate[0..s_len]);
        self.last_sqlstate_len = s_len;

        // ⚠️ **Only the first line.** libpq returns `ERROR: …\nDETAIL: …\nHINT: …`, and the
        // DETAIL can quote values from a row the client never wrote: a unique violation
        // reports `Key (email)=(alice@x.com) already exists`, which tells the writer that
        // such a row exists and what is in its key. The verdict goes to the client, so
        // that would turn a rejected write into a probe for other people's data.
        //
        // The first line is the actionable part anyway — it names the constraint and the
        // column ("null value in column \"inserted_at\" … violates not-null constraint"),
        // which is what a client author needs and what the schema descriptor cannot tell
        // them. The full text stays in the operator's log via `log.err`.
        const first_line = blk: {
            const nl = std.mem.indexOfScalar(u8, message, '\n') orelse break :blk message;
            break :blk message[0..nl];
        };

        // Quotes would break the JSON body this ends up in, and libpq's messages are full
        // of them.
        var out: usize = 0;
        for (first_line) |ch| {
            if (out == self.last_error_buf.len) break;
            self.last_error_buf[out] = switch (ch) {
                '\n', '\r', '\t' => ' ',
                '"', '\\' => '\'',
                else => ch,
            };
            out += 1;
        }
        self.last_error_len = out;
    }

    /// Does this failure ask something of the **bridge's operator**?
    ///
    /// Log level is a message to whoever runs the bridge, so the question is not "how bad
    /// is this" but "can the person reading it act on it". Most mutation failures cannot
    /// be acted on by them at all:
    ///
    ///   `permission denied` on a table nobody granted   the boundary holding. It is what
    ///                                                   the operator configured, and a
    ///                                                   client testing it is ordinary
    ///                                                   traffic, not a fault.
    ///   a malformed payload, a missing key              a client bug. The verdict already
    ///                                                   went to the client, who is the
    ///                                                   only one who can fix it.
    ///   a constraint violation                          same: the row is wrong.
    ///
    /// These are the chain of custody working, and logging them at error level trains an
    /// operator to ignore the level — which is what makes a real one invisible later.
    ///
    /// What *is* theirs:
    ///
    ///   `NoVersionColumn` / `NoTombstoneColumn`         `SYNC_RULES` names a column the
    ///                                                   schema does not have. Preflight
    ///                                                   says so at boot; if it reaches
    ///                                                   here, that warning was missed.
    ///   retries exhausted on a transient SQLSTATE       PostgreSQL is unreachable or
    ///                                                   unhealthy. Nobody else can fix it.
    ///
    /// ⚠️ The asymmetry is deliberate: a client-caused failure is *reported to the client*
    /// and merely traced for the operator, while an operator-caused one has no other
    /// channel — no verdict reaches anyone who can act on it.
    fn isOperatorFault(err: anyerror) bool {
        return switch (err) {
            // The schema and SYNC_RULES disagree. Preflight reports this at boot; seeing
            // it at runtime means a table appeared later, or the warning went unread.
            error.NoVersionColumn,
            error.NoTombstoneColumn,
            error.NoPrimaryKey,
            => true,
            else => false,
        };
    }

    /// Is this SQLSTATE one that a retry can never fix?
    ///
    /// The bridge used to treat every SQL error as transient, on the stated grounds that
    /// `permission denied` and a dropped connection are indistinguishable. They are not —
    /// PostgreSQL says which is which, and libpq hands it over in a field this file
    /// already reads to invalidate the catalog cache:
    ///
    /// ```txt
    /// ERROR:  42501: permission denied for table users     ← class 42, will never succeed
    /// FATAL:  57P01: terminating connection …              ← class 57, reconnect and retry
    ///         server closed the connection unexpectedly    ← then no SQLSTATE at all
    /// ```
    ///
    /// Classifying costs one comparison and buys both directions of the budget:
    /// a privilege error is reported in milliseconds instead of burning five attempts,
    /// and the attempts it stops consuming can be spent on failures that might actually
    /// recover (`Retry.pg_reconnect_delay_seconds` alone is 5s — the old five-attempt
    /// budget could not outlast one reconnect).
    ///
    /// ⚠️ **Unknown means transient.** An empty SQLSTATE is libpq's own error, raised
    /// without the server ever answering, which is definitionally worth retrying. And an
    /// unrecognised class keeps the old behaviour: the cost of retrying a permanent error
    /// is a few seconds and a dead letter, while the cost of *not* retrying a transient
    /// one is a lost write. Only classes we are sure about are named here.
    fn sqlstateIsPermanent(sqlstate: []const u8) bool {
        if (sqlstate.len < 2) return false; // libpq-generated: the server never answered
        const class = sqlstate[0..2];

        // 42 access rule violation — insufficient_privilege, undefined_table/column.
        // 23 integrity constraint — not_null, foreign_key, unique, check.
        // 22 data exception — invalid text representation, numeric out of range.
        // 3F invalid_schema_name.  0A feature_not_supported.
        //
        // None of these change because time passed. A GRANT or a migration might fix
        // them, but that is an operator acting, not a retry succeeding — and the client
        // is better told now than in four seconds.
        const permanent = [_][]const u8{ "42", "23", "22", "3F", "0A" };
        for (permanent) |cls| {
            if (std.mem.eql(u8, class, cls)) return true;
        }
        return false;
    }

    /// Forget the previous message's failure.
    ///
    /// ⚠️ Called before **every** attempt, because the buffers are sticky by design — they
    /// hold the last SQL error so a verdict can quote it. Without clearing, a failure that
    /// never reaches SQL at all reports the previous *message's* reason: a real run
    /// produced `{"reason":"MissingPrimaryKey","sqlstate":"42501","detail":"permission
    /// denied for table users"}` — a payload-shape rejection on `test_types` wearing a
    /// privilege error from a different table sixteen seconds earlier.
    ///
    /// That is worse than no detail. An operator reading it chases the wrong table, and
    /// the verdict is confidently wrong rather than merely thin. Empty fields are the
    /// honest answer for an error that never touched PostgreSQL.
    fn clearFailure(self: *MutationListener) void {
        self.last_sqlstate_len = 0;
        self.last_error_len = 0;
    }

    fn lastSqlstate(self: *const MutationListener) []const u8 {
        return self.last_sqlstate_buf[0..self.last_sqlstate_len];
    }

    fn lastError(self: *const MutationListener) []const u8 {
        return self.last_error_buf[0..self.last_error_len];
    }

    /// What JetStream tells the consumer about *this delivery*, read out of the
    /// message's own reply subject.
    ///
    /// ```txt
    /// $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
    /// $JS.ACK.<domain>.<acct>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
    /// ```
    ///
    /// ⚠️ **Counted from the end, never from the start.** The two forms differ by two
    /// leading tokens, so a fixed index reads `<consumer>` as the delivery count on one
    /// deployment and works on another — and the bug only appears once someone enables a
    /// JetStream domain, which is exactly when nobody is looking at this function. The
    /// trailing five are stable in both.
    ///
    /// Returns null rather than guessing when the shape is unrecognised: acting on a
    /// wrong delivery count means either giving up on the first attempt or never giving
    /// up at all.
    const AckMeta = struct {
        /// 1 on first delivery. Equal to `max_deliver` on the last attempt the server
        /// will ever make — after which the message is dropped and nobody is told.
        delivered: u64,
        /// The stream sequence — the same number the client received in its PubAck, and
        /// therefore the one identifier both ends already hold.
        stream_seq: u64,
    };

    fn parseAckMeta(reply_to: []const u8) ?AckMeta {
        if (!std.mem.startsWith(u8, reply_to, "$JS.ACK.")) return null;

        var count: usize = 0;
        var counter = std.mem.splitScalar(u8, reply_to, '.');
        while (counter.next()) |_| count += 1;
        // 9 tokens without a domain, 11 with one. Anything else is a shape this was not
        // written against, and a wrong parse is worse than no parse.
        if (count != 9 and count != 11) return null;

        var delivered: ?u64 = null;
        var stream_seq: ?u64 = null;
        var it = std.mem.splitScalar(u8, reply_to, '.');
        var i: usize = 0;
        while (it.next()) |tok| : (i += 1) {
            if (i == count - 5) delivered = std.fmt.parseInt(u64, tok, 10) catch return null;
            if (i == count - 4) stream_seq = std.fmt.parseInt(u64, tok, 10) catch return null;
        }
        return .{ .delivered = delivered orelse return null, .stream_seq = stream_seq orelse return null };
    }

    /// The client's own `Nats-Msg-Id`, which is the only correlation token it chose
    /// itself — and therefore the only one it can match against a queued write without
    /// having stored a server-assigned number.
    ///
    /// JetStream consumes this header for deduplication; nothing surfaced it to the
    /// bridge before, so a verdict had no way to name the write it was about.
    fn msgIdOf(msg: *nats.Conn.AllocatedMSG) ?[]const u8 {
        const hdrs = msg.letter.getHeaders() orelse return null;
        const raw = hdrs.buffer.body() orelse return null;
        return headerValue(raw, "Nats-Msg-Id");
    }

    /// Find one header in a raw NATS header block.
    ///
    /// ```txt
    /// NATS/1.0␍␊Nats-Msg-Id: abc␍␊Other: x
    /// ```
    ///
    /// ⚠️ Hand-rolled rather than `Headers.hiter()`, which yields **nothing** here. That
    /// iterator is `std.http.HeaderIterator`, which needs every header line to end in
    /// ␍␊ — but `Conn.read_HMSG` strips the block's trailing ␍␊␍␊ before storing it, so
    /// the last (and in our case only) header has no terminator and is never emitted.
    /// The failure is silent: a message that plainly carries `Nats-Msg-Id` reads as
    /// having no headers at all, so a verdict is simply never published and nothing
    /// says why.
    ///
    /// Tolerates a present or absent trailing terminator, any spacing after the colon,
    /// and matches the name case-insensitively as the protocol requires.
    fn headerValue(raw: []const u8, name: []const u8) ?[]const u8 {
        var lines = std.mem.splitSequence(u8, raw, "\r\n");
        _ = lines.next(); // the "NATS/1.0" version line is not a header
        while (lines.next()) |line| {
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
            const key = std.mem.trim(u8, line[0..colon], " \t");
            if (!std.ascii.eqlIgnoreCase(key, name)) continue;
            return std.mem.trim(u8, line[colon + 1 ..], " \t");
        }
        return null;
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
                // The catalog is the allowlist and the client sent something outside it —
                // a client fault, reported to them by the verdict, so traced here rather
                // than raised.
                log.info("⛔ '{s}' has no column '{s}' — refusing mutation from '{s}'", .{ mutation.table, name, mutation.principal });
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
            const msg_ptr = c.PQerrorMessage(conn);
            const msg_text: []const u8 = if (msg_ptr != null) std.mem.span(msg_ptr) else "";
            self.rememberFailure(sqlstate, msg_text);

            // Level by fault, like the caller: a SQLSTATE the server returned and we know
            // is permanent describes the client's row or the operator's policy, and the
            // client is told directly by the verdict — so it is traced, not raised. A
            // transient one means PostgreSQL is unhealthy or unreachable, which nobody
            // but the operator can act on, and which has no other channel.
            //
            // ⚠️ Kept at full detail either way. This is the only place the *whole*
            // message survives: the verdict carries the first line alone, because its
            // DETAIL can quote rows the client never wrote.
            if (sqlstateIsPermanent(sqlstate)) {
                log.info("⛔ Mutation refused [{s}] on '{s}': {s}", .{ mutation.principal, mutation.table, c.PQerrorMessage(conn) });
            } else {
                log.err("🔴 Mutation failed [{s}] on '{s}': {s}", .{ mutation.principal, mutation.table, c.PQerrorMessage(conn) });
            }
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

test "parseAckMeta: the plain form" {
    const m = MutationListener.parseAckMeta(
        "$JS.ACK.MUTATIONS.bridge_mutations_worker.1.16.31.1787001231697045182.0",
    ).?;
    try std.testing.expectEqual(@as(u64, 1), m.delivered);
    try std.testing.expectEqual(@as(u64, 16), m.stream_seq);
}

test "parseAckMeta: the domain form shifts every index by two" {
    // The whole reason the parse counts from the end. Reading fixed positions here
    // would take "acct" as the delivery count and fail to parse — or worse, succeed
    // against a numeric account hash.
    const m = MutationListener.parseAckMeta(
        "$JS.ACK.hub.ACCTHASH.MUTATIONS.bridge_mutations_worker.5.17.42.1787001231697045182.0",
    ).?;
    try std.testing.expectEqual(@as(u64, 5), m.delivered);
    try std.testing.expectEqual(@as(u64, 17), m.stream_seq);
}

test "parseAckMeta: the last attempt is recognisable" {
    // What the whole feature turns on: delivered == max_deliver means the server will not
    // try again, so this is the only moment a verdict can still be published.
    //
    // Built from the constant rather than hardcoding it — this test failed the moment the
    // budget moved from 5 to 15, which is the correct behaviour for an assertion about
    // "the last attempt" but a poor reason to edit a subject literal.
    var buf: [128]u8 = undefined;
    const subject = try std.fmt.bufPrint(
        &buf,
        "$JS.ACK.MUTATIONS.w.{d}.99.100.1787001231697045182.0",
        .{config.Nats.mutation_max_deliver},
    );
    const m = MutationListener.parseAckMeta(subject).?;
    try std.testing.expect(m.delivered == config.Nats.mutation_max_deliver);

    // And one attempt short of it must not be mistaken for the last.
    const earlier = try std.fmt.bufPrint(
        &buf,
        "$JS.ACK.MUTATIONS.w.{d}.99.100.1787001231697045182.0",
        .{config.Nats.mutation_max_deliver - 1},
    );
    try std.testing.expect(MutationListener.parseAckMeta(earlier).?.delivered < config.Nats.mutation_max_deliver);
}

test "parseAckMeta: refuses shapes it was not written against" {
    // Null, never a guess: a wrong delivery count means either giving up on the first
    // attempt or never giving up at all.
    try std.testing.expect(MutationListener.parseAckMeta("") == null);
    try std.testing.expect(MutationListener.parseAckMeta("$JS.ACK.TOO.FEW.TOKENS") == null);
    try std.testing.expect(MutationListener.parseAckMeta("cdc.users.insert") == null);
    try std.testing.expect(MutationListener.parseAckMeta(
        "$JS.ACK.MUTATIONS.w.notanumber.16.31.1787001231697045182.0",
    ) == null);
    // A core-NATS reply inbox, which is what arrives if the consumer is misconfigured.
    try std.testing.expect(MutationListener.parseAckMeta("_INBOX.abc.def") == null);
}

test "headerValue: the shape NATS actually delivers (no trailing CRLF)" {
    // `Conn.read_HMSG` strips the block's trailing ␍␊␍␊, so the last header ends the
    // buffer. This is the exact case `std.http.HeaderIterator` drops on the floor.
    const raw = "NATS/1.0\r\nNats-Msg-Id: c-1-users-42";
    try std.testing.expectEqualStrings(
        "c-1-users-42",
        MutationListener.headerValue(raw, "Nats-Msg-Id").?,
    );
}

test "headerValue: with a trailing CRLF, and among several headers" {
    const raw = "NATS/1.0\r\nNats-Expected-Stream: MUTATIONS\r\nNats-Msg-Id: abc\r\n";
    try std.testing.expectEqualStrings("abc", MutationListener.headerValue(raw, "Nats-Msg-Id").?);
    try std.testing.expectEqualStrings(
        "MUTATIONS",
        MutationListener.headerValue(raw, "Nats-Expected-Stream").?,
    );
}

test "headerValue: name match is case-insensitive, value keeps its inner colons" {
    // The msg_id embeds an ISO timestamp, so colons in the value are the normal case —
    // splitting on the first colon is what makes that safe.
    const raw = "NATS/1.0\r\nNATS-MSG-ID: c-1-t-2026-08-18T04:25:19.801000";
    try std.testing.expectEqualStrings(
        "c-1-t-2026-08-18T04:25:19.801000",
        MutationListener.headerValue(raw, "Nats-Msg-Id").?,
    );
}

test "headerValue: absent, malformed, and empty blocks yield null" {
    try std.testing.expect(MutationListener.headerValue("NATS/1.0\r\nOther: x", "Nats-Msg-Id") == null);
    try std.testing.expect(MutationListener.headerValue("NATS/1.0", "Nats-Msg-Id") == null);
    try std.testing.expect(MutationListener.headerValue("", "Nats-Msg-Id") == null);
    // A line with no colon must be skipped, not mistaken for a header.
    try std.testing.expect(MutationListener.headerValue("NATS/1.0\r\ngarbage", "Nats-Msg-Id") == null);
}

test "clearFailure: a verdict cannot inherit the previous message's reason" {
    // The exact sequence that produced the wrong verdict in a live run: a SQL failure on
    // one table, then a payload-shape rejection on another that never reaches SQL.
    var l: MutationListener = undefined;
    l.last_sqlstate_len = 0;
    l.last_error_len = 0;

    l.rememberFailure("42501", "ERROR:  permission denied for table users");
    try std.testing.expectEqualStrings("42501", l.lastSqlstate());

    l.clearFailure();
    try std.testing.expectEqualStrings("", l.lastSqlstate());
    try std.testing.expectEqualStrings("", l.lastError());
}

test "rememberFailure: newlines and quotes cannot break the JSON body" {
    // libpq routinely returns multi-line messages (`ERROR: ...\nDETAIL: ...`), and the
    // verdict is hand-built JSON.
    var l: MutationListener = undefined;
    l.last_sqlstate_len = 0;
    l.last_error_len = 0;

    l.rememberFailure("23502", "ERROR:  null value\nDETAIL:  Failing row contains \"x\"");
    const got = l.lastError();
    try std.testing.expect(std.mem.indexOfScalar(u8, got, '\n') == null);
    try std.testing.expect(std.mem.indexOfScalar(u8, got, '"') == null);
    try std.testing.expect(std.mem.indexOf(u8, got, "null value") != null);
}

test "rememberFailure: an over-long message truncates instead of overflowing" {
    var l: MutationListener = undefined;
    l.last_sqlstate_len = 0;
    l.last_error_len = 0;

    const long = "E" ** 4096;
    l.rememberFailure("XX000", long);
    try std.testing.expect(l.lastError().len <= l.last_error_buf.len);
}

test "sqlstateIsPermanent: the case that motivated it" {
    // 42501 insufficient_privilege — a missing GRANT. Retrying cannot produce one.
    try std.testing.expect(MutationListener.sqlstateIsPermanent("42501"));
}

test "sqlstateIsPermanent: transient classes keep their retries" {
    // 57P01 is what a terminated backend reports; 08006 a lost connection; 40001 a
    // serialization failure and 40P01 a deadlock — the two cases where retrying is not
    // merely allowed but is the documented remedy. 53300 is too_many_connections.
    for ([_][]const u8{ "57P01", "08006", "08000", "40001", "40P01", "53300", "55P03" }) |st| {
        try std.testing.expect(!MutationListener.sqlstateIsPermanent(st));
    }
}

test "sqlstateIsPermanent: an absent SQLSTATE is transient, never permanent" {
    // libpq raises its own error when the server never answered, and leaves this empty.
    // Reading that as permanent would dead-letter every write made during an outage.
    try std.testing.expect(!MutationListener.sqlstateIsPermanent(""));
    try std.testing.expect(!MutationListener.sqlstateIsPermanent("4"));
}

test "sqlstateIsPermanent: constraint and data errors are permanent" {
    // 23502 not_null_violation is the `inserted_at` case; 23503 foreign_key; 23505 unique;
    // 22P02 invalid_text_representation (a malformed uuid). None improve with time.
    for ([_][]const u8{ "23502", "23503", "23505", "22P02", "42703", "42P01", "0A000" }) |st| {
        try std.testing.expect(MutationListener.sqlstateIsPermanent(st));
    }
}

test "sqlstateIsPermanent: an unknown class is treated as transient" {
    // Conservative on purpose: retrying a permanent error costs seconds and a dead
    // letter, while refusing to retry a transient one loses the write.
    try std.testing.expect(!MutationListener.sqlstateIsPermanent("XX000"));
    try std.testing.expect(!MutationListener.sqlstateIsPermanent("99999"));
}

test "rememberFailure: DETAIL never reaches the verdict" {
    // A unique violation's DETAIL names a value from a row the client did not write.
    // The verdict is client-facing, so it must not carry it.
    var l: MutationListener = undefined;
    l.last_sqlstate_len = 0;
    l.last_error_len = 0;

    l.rememberFailure(
        "23505",
        "ERROR:  duplicate key value violates unique constraint \"users_email_key\"\nDETAIL:  Key (email)=(alice@example.com) already exists.",
    );
    const got = l.lastError();
    try std.testing.expect(std.mem.indexOf(u8, got, "alice@example.com") == null);
    try std.testing.expect(std.mem.indexOf(u8, got, "DETAIL") == null);
    // The actionable half survives: which constraint was violated.
    try std.testing.expect(std.mem.indexOf(u8, got, "users_email_key") != null);
}

test "rememberFailure: the column name survives for a NOT NULL violation" {
    // The one thing the schema descriptor cannot tell a client — which columns are
    // required — is exactly what this line supplies.
    var l: MutationListener = undefined;
    l.last_sqlstate_len = 0;
    l.last_error_len = 0;

    l.rememberFailure(
        "23502",
        "ERROR:  null value in column \"inserted_at\" of relation \"test_types\" violates not-null constraint\nDETAIL:  Failing row contains (uuid, null, secret).",
    );
    const got = l.lastError();
    try std.testing.expect(std.mem.indexOf(u8, got, "inserted_at") != null);
    try std.testing.expect(std.mem.indexOf(u8, got, "secret") == null);
}
