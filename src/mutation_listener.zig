const std = @import("std");
const nats = @import("nats");
const Topology = @import("topology.zig");
const CatalogEpoch = @import("catalog_epoch.zig").CatalogEpoch;
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
/// What became of a write that PostgreSQL accepted without error — PROTOCOL.md §7.1.
///
/// ⚠️ All three are *successes* at the SQL level. A client cannot tell them apart from the
/// row count alone, and the difference decides what it does with the edit still sitting in
/// its outbox: drop it quietly, or tell the user their row is gone.
const WriteOutcome = enum {
    /// Rows changed. The client pops and forgets.
    applied,
    /// Zero rows, and the row is still there, undeleted — the last-write-wins guard
    /// refused because a newer version is stored. The client pops **without reverting**:
    /// the winning row arrives over CDC.
    stale,
    /// Zero rows because the row is not there to write: physically gone, or carrying a
    /// tombstone. The one case LWW cannot decide for the user, so the client surfaces it.
    row_deleted,

    fn wireName(self: WriteOutcome) []const u8 {
        return switch (self) {
            .applied => "accepted",
            .stale => "stale",
            .row_deleted => "row_deleted",
        };
    }
};

/// Which clamp expression a version column needs, if any — PROTOCOL.md §7.2.
///
/// Read from `pg_attribute.atttypid` rather than guessed from the column name: the same
/// `updated_at` is `timestamptz` in one schema and naive `timestamp` in the next, and
/// Ecto's `timestamps(type: :utc_datetime_usec)` produces the naive one despite the name.
const VersionType = enum {
    /// `timestamptz` (oid 1184) — an absolute instant, comparable to `now()` directly.
    timestamptz,
    /// `timestamp` (oid 1114) — no zone. Compared against `now() AT TIME ZONE 'UTC'`,
    /// because the protocol requires clients to send UTC for this column (§7.2) and
    /// comparing against local `now()` would clamp by the server's offset.
    timestamp_naive,
    /// Integer, or anything else. Not clamped: "future" is meaningless for a counter, and
    /// silently capping one would corrupt a perfectly sound version scheme.
    other,

    fn fromOid(oid: u32) VersionType {
        return switch (oid) {
            1184 => .timestamptz,
            1114 => .timestamp_naive,
            else => .other,
        };
    }

    /// How to render the stored value so it matches what CDC puts on the wire.
    ///
    /// ⚠️ Not the column itself. `RETURNING "updated_at"` hands back libpq's text output —
    /// `2026-08-19 05:54:11.363299+00`, space-separated — which is precisely the shape
    /// PROTOCOL.md §7.2 warns clients never to match, because the bridge does not emit it
    /// anywhere else. A client that stored a verdict's version verbatim would hold a
    /// string that disagrees with the CDC rendering of the very same column, and its own
    /// comparisons would quietly stop lining up with the feed.
    ///
    /// So the verdict is rendered here, in the same ISO 8601 shape §7.2 documents:
    /// `T` separator, six fractional digits, and a zone suffix that follows the column
    /// type — `Z` for `timestamptz`, nothing for naive `timestamp`.
    fn wireFormat(self: VersionType) ?struct { prefix: []const u8, suffix: []const u8 } {
        return switch (self) {
            .timestamptz => .{
                .prefix = "to_char(",
                .suffix = " AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US') || 'Z'",
            },
            .timestamp_naive => .{
                .prefix = "to_char(",
                .suffix = ", 'YYYY-MM-DD\"T\"HH24:MI:SS.US')",
            },
            // An integer version has no notation question: it is already its own text.
            .other => null,
        };
    }

    /// The cast a raw parameter needs to be comparable with the stored column.
    fn castSuffix(self: VersionType) ?[]const u8 {
        return switch (self) {
            .timestamptz => "::timestamptz",
            .timestamp_naive => "::timestamp",
            .other => null,
        };
    }

    /// The SQL that caps a client's value at the database's clock plus the tolerance,
    /// as the two halves that wrap the parameter placeholder. Null when the type has no
    /// meaningful future.
    ///
    /// Split rather than formatted because the placeholder index is only known at
    /// runtime, and a format string must be comptime-known.
    fn clampExpr(self: VersionType) ?struct { prefix: []const u8, suffix: []const u8 } {
        return switch (self) {
            .timestamptz => .{
                .prefix = "LEAST(",
                .suffix = "::timestamptz, now() + interval '" ++
                    config.Sync.version_future_tolerance ++ "')",
            },
            .timestamp_naive => .{
                .prefix = "LEAST(",
                .suffix = "::timestamp, (now() AT TIME ZONE 'UTC') + interval '" ++
                    config.Sync.version_future_tolerance ++ "')",
            },
            .other => null,
        };
    }
};

/// What a column's TEXT form has to look like, which is not the same question as what
/// the value is.
///
/// The bridge binds every parameter as text and lets PostgreSQL parse it with the
/// column's own input function — so a msgpack array destined for `text[]` must arrive
/// as `{a,b}` and the SAME array destined for `jsonb` must arrive as `["a","b"]`.
/// Those literals are not interchangeable, so the target type decides, and the value
/// alone cannot.
const ColKind = enum {
    scalar,
    /// json / jsonb: any msgpack value serialises, objects included.
    json,
    /// A PostgreSQL array type: `{…}` literal, quoted per element.
    array,
};

/// A msgpack payload as JSON, for a json/jsonb column.
fn writeJsonPayload(
    alloc: std.mem.Allocator,
    out: *std.ArrayList(u8),
    p: @import("msgpack").Payload,
) error{OutOfMemory}!void {
    switch (p) {
        .nil => try out.appendSlice(alloc, "null"),
        .bool => |b| try out.appendSlice(alloc, if (b) "true" else "false"),
        .int => |i| try out.print(alloc, "{d}", .{i}),
        .uint => |u| try out.print(alloc, "{d}", .{u}),
        .float => |f| try out.print(alloc, "{d}", .{f}),
        .str => |v| try writeJsonString(alloc, out, v.value()),
        .bin => |v| try writeJsonString(alloc, out, v.value()),
        .arr => |items| {
            try out.append(alloc, '[');
            for (items, 0..) |item, i| {
                if (i > 0) try out.append(alloc, ',');
                try writeJsonPayload(alloc, out, item);
            }
            try out.append(alloc, ']');
        },
        .map => |m| {
            try out.append(alloc, '{');
            var it = m.map.iterator();
            var first = true;
            while (it.next()) |e| {
                if (e.key_ptr.* != .str) continue; // a non-string key has no JSON form
                if (!first) try out.append(alloc, ',');
                first = false;
                try writeJsonString(alloc, out, e.key_ptr.str.value());
                try out.append(alloc, ':');
                try writeJsonPayload(alloc, out, e.value_ptr.*);
            }
            try out.append(alloc, '}');
        },
        else => try out.appendSlice(alloc, "null"),
    }
}

fn writeJsonString(alloc: std.mem.Allocator, out: *std.ArrayList(u8), v: []const u8) error{OutOfMemory}!void {
    try out.append(alloc, '"');
    for (v) |ch| switch (ch) {
        '"' => try out.appendSlice(alloc, "\\\""),
        '\\' => try out.appendSlice(alloc, "\\\\"),
        '\n' => try out.appendSlice(alloc, "\\n"),
        '\r' => try out.appendSlice(alloc, "\\r"),
        '\t' => try out.appendSlice(alloc, "\\t"),
        else => if (ch < 0x20) try out.print(alloc, "\\u{x:0>4}", .{ch}) else try out.append(alloc, ch),
    };
    try out.append(alloc, '"');
}

/// A msgpack array as a PostgreSQL array literal: `{a,b}`, nesting as `{{1,2}}`.
///
/// ⚠️ Every element is DOUBLE-QUOTED unless it is NULL. That is not laziness — an
/// unquoted element ends at a comma or a brace, so a string containing `,` `{` `}` `"`
/// `\` or leading whitespace would silently split into several elements or corrupt the
/// literal. Quoting always is both correct and shorter than deciding when to.
fn writeArrayLiteral(
    alloc: std.mem.Allocator,
    out: *std.ArrayList(u8),
    p: @import("msgpack").Payload,
) error{OutOfMemory}!void {
    switch (p) {
        .arr => |items| {
            try out.append(alloc, '{');
            for (items, 0..) |item, i| {
                if (i > 0) try out.append(alloc, ',');
                try writeArrayLiteral(alloc, out, item);
            }
            try out.append(alloc, '}');
        },
        .nil => try out.appendSlice(alloc, "NULL"), // unquoted: quoted is the STRING "NULL"
        .bool => |b| try out.appendSlice(alloc, if (b) "\"t\"" else "\"f\""),
        .int => |i| try out.print(alloc, "\"{d}\"", .{i}),
        .uint => |u| try out.print(alloc, "\"{d}\"", .{u}),
        .float => |f| try out.print(alloc, "\"{d}\"", .{f}),
        .str => |v| try writeArrayElement(alloc, out, v.value()),
        .bin => |v| try writeArrayElement(alloc, out, v.value()),
        else => try out.appendSlice(alloc, "NULL"),
    }
}

fn writeArrayElement(alloc: std.mem.Allocator, out: *std.ArrayList(u8), v: []const u8) error{OutOfMemory}!void {
    try out.append(alloc, '"');
    for (v) |ch| switch (ch) {
        '"' => try out.appendSlice(alloc, "\\\""),
        '\\' => try out.appendSlice(alloc, "\\\\"),
        else => try out.append(alloc, ch),
    };
    try out.append(alloc, '"');
}

const TableMeta = struct {
    /// Primary key columns in key order — the `ON CONFLICT` target.
    pk_cols: [][]const u8,
    /// Every column, so a payload naming something else can be rejected before SQL.
    columns: [][]const u8,
    /// Parallel to `columns`: how each one's text form must be shaped. Read from the
    /// catalog with everything else, so it is re-read on a DDL epoch change like the
    /// rest of this struct.
    col_kinds: []ColKind,
    /// The column compared for last-write-wins.
    version_col: []const u8,
    /// Optional third `SYNC_RULES` column: where the winning writer's id is stored, so
    /// equal versions can be broken instead of rejected. Null means no tiebreak — a tie
    /// is refused, which is the safe default and the behaviour before this existed.
    client_col: ?[]const u8 = null,
    /// The tombstone column, when the table has one. Null means a delete is physical.
    tombstone_col: ?[]const u8,
    /// What the version column actually is, read from the catalog. Decides whether a
    /// future-dated version can be clamped — an integer version has no future.
    version_type: VersionType = .other,
    /// The catalog epoch these facts were read at. Not equal to the current epoch means
    /// DDL has crossed the replication stream since, so they are re-read rather than
    /// trusted. See `catalog_epoch.zig`.
    epoch: u64 = 0,

    fn deinit(self: *TableMeta, allocator: std.mem.Allocator) void {
        for (self.pk_cols) |col| allocator.free(col);
        allocator.free(self.pk_cols);
        for (self.columns) |col| allocator.free(col);
        allocator.free(self.columns);
        allocator.free(self.col_kinds);
        allocator.free(self.version_col);
        if (self.client_col) |cc| allocator.free(cc);
        if (self.tombstone_col) |t| allocator.free(t);
    }

    /// The kind of `name`, or `.scalar` when the column is unknown — an unknown column
    /// is rejected elsewhere, and guessing `.scalar` here keeps that the single place
    /// that decides it.
    fn kindOf(self: *const TableMeta, name: []const u8) ColKind {
        for (self.columns, 0..) |col, i| {
            if (std.mem.eql(u8, col, name)) return self.col_kinds[i];
        }
        return .scalar;
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
    /// `DATABASE_READER_URL` (kept in `PgConf.db_url`) and `sslmode`. With a URL set, every
    /// other component connected where it said and this thread dialled the PG_* defaults.
    pg_config: *const pg_conn.PgConf,
    /// Wire names, read from grammar.json at startup. See src/topology.zig.
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
    /// Bumped by the replication thread on every DDL event, so a cached `TableMeta` can
    /// be recognised as stale without first failing a statement.
    catalog_epoch: *const CatalogEpoch,
    /// The CDC per-event buffer, `2^BASE_BUF`. A mutation larger than this describes a row
    /// the change feed could never carry back out — see `handleMutation`.
    event_buf_bytes: usize,
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
    /// The version PostgreSQL actually stored, read back via `RETURNING`. Differs from
    /// what the client sent exactly when the clamp fired, which is the case PROTOCOL.md
    /// §7.2 promises to report. Sticky like the buffers above, and cleared with them.
    last_version_buf: [64]u8 = undefined,
    last_version_len: usize = 0,
    /// True when PostgreSQL stored something other than what the client sent, which for a
    /// successful write means the future-version clamp fired.
    last_clamped: bool = false,
    /// What actually became of the last successfully-executed write — the three outcomes
    /// PROTOCOL.md §7.1 tells clients to key their outbox on.
    last_outcome: WriteOutcome = .applied,

    pub fn init(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        endpoint: config.Nats.Endpoint,
        topology: *const Topology.Topology,
        sync_rules: *const config.EventClassification.TransitionRules,
        default_version_column: []const u8,
        io: std.Io,
        should_stop: *std.atomic.Value(bool),
        catalog_epoch: *const CatalogEpoch,
        event_buf_bytes: usize,
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
            .catalog_epoch = catalog_epoch,
            .event_buf_bytes = event_buf_bytes,
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
        //    `connInfo` is the shared builder, so DATABASE_READER_URL and sslmode apply here
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
        // DATABASE_READER_URL set those fields hold the unused PG_* defaults, so printing them
        // would report a host this connection never dialled.
        // Role included: the whole point of the ingress connection is that it is NOT
        // the replication role, and a log line that cannot show which one connected
        // cannot show that the split is working.
        log.info("Mutation listener: connected to PostgreSQL {s}:{s} as '{s}'.", .{
            c.PQhost(conn), c.PQport(conn), c.PQuser(conn),
        });

        // 2. Connect to NATS, and bind a durable pull consumer.
        //
        // Pull, not push: the bridge fetches when it is ready, so a burst cannot outrun
        // the PostgreSQL apply loop, and the position lives in the durable so a restart
        // resumes where it stopped.
        const conn_nats = self.allocator.create(nats.Connection) catch |err| {
            log.err("Mutation listener: out of memory: {}", .{err});
            return;
        };
        defer self.allocator.destroy(conn_nats);

        conn_nats.* = nats.Connection.init(self.allocator, self.io, .{
            .user = self.endpoint.user,
            .password = self.endpoint.pass,
            .nkey_seed = self.endpoint.seed,
            .user_creds = self.endpoint.creds,
        });
        defer conn_nats.deinit();

        const url = std.fmt.allocPrint(self.allocator, "nats://{s}:{d}", .{ self.endpoint.host, self.endpoint.port }) catch return;
        defer self.allocator.free(url);
        conn_nats.connect(url) catch |err| {
            log.err("Mutation listener: Failed to connect to NATS: {}", .{err});
            return;
        };

        var js = nats.JetStream.init(conn_nats, .{});

        // `.stream` given explicitly rather than derived from the subject: grammar.json
        // names it, and the lookup path allocates a name the subscription then owns (see
        // nats.zig/NOTES.md #1).
        const consumer = js.pullSubscribe(
            self.endpoint_topology.mutations_subject_wildcard,
            "bridge_mutations_worker",
            .{
                .stream = self.endpoint_topology.stream_mutations,
                .config = .{
                    .ack_policy = .explicit,
                    .deliver_policy = .all,
                    // The server-side half of the poison-pill guard: even a failure the
                    // bridge wrongly classifies as retryable stops after this many
                    // attempts.
                    .max_deliver = config.Nats.mutation_max_deliver,
                    .filter_subject = self.endpoint_topology.mutations_subject_wildcard,
                },
            },
        ) catch |err| {
            log.err("Mutation listener: Failed to start NATS consumer: {}", .{err});
            return;
        };
        defer consumer.deinit();

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

            // One message at a time, with a short timeout so shutdown is responsive.
            //
            // ⚠️ Status frames (408, 409, idle heartbeats) are handled by the library now
            // — it recognises them and does not surface them as messages. The vendored
            // client did not, and acking one panicked because `Consumer.ack` unwrapped a
            // null ReplyTo; the guard for that lived here.
            var batch = consumer.fetch(1, .{ .duration = .{ .raw = .fromMilliseconds(500), .clock = .awake } }) catch {
                continue;
            };
            defer batch.deinit();

            for (batch.messages) |msg| {
                const payload = msg.msg.data;

                // Subject first: it carries the identity, the table and the verb, and
                // all three are refused before a byte of the payload is decoded.
                const subject = msg.msg.subject;
                const mutation = parseSubject(subject, self.endpoint_topology.subject_mutations_prefix) catch |err| {
                    // `warn`, not `err` — and not `info` either. Client-caused like the
                    // refusals below, so it is not the operator's to fix. But it is the
                    // one client fault where **nobody is told**: the principal lives in
                    // the subject, and the subject is what failed to parse, so there is
                    // no address to send a verdict to. Worth keeping visible for exactly
                    // that reason.
                    log.warn("⚠️  Malformed mutation subject '{s}' ({}): dead-lettering, and no verdict is addressable", .{ subject, err });
                    self.deadLetter(conn_nats, msg, payload, err);
                    msg.ack() catch {};
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
                    self.deadLetter(conn_nats, msg, payload, error.ForbiddenTable);
                    msg.ack() catch {};
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
                            // One line for every operator fault: the specific reason was
                            // already logged where it was detected (`tableMeta`), and
                            // `DbAllocatedKey` is not a SYNC_RULES disagreement — it is
                            // the table's key shape — so this stays generic.
                            log.err(
                                "🔴 '{s}' cannot accept writes ({}): only a migration or a SYNC_RULES change can fix this. Preflight reports it at boot; see the line above for the reason.",
                                .{ mutation.table, err },
                            );
                        } else {
                            // Traced, not raised: the client is the only one who can fix a
                            // payload, and the verdict below is how they are told.
                            log.info("⛔ Mutation refused [{s}] on '{s}': {} (not retrying)", .{ mutation.principal, mutation.table, err });
                        }
                        self.deadLetter(conn_nats, msg, payload, err);
                        self.publishVerdict(conn_nats, msg, mutation.principal, "rejected", @errorName(err));
                        // ACK, not NACK: the message is handled — badly, but finally.
                        msg.ack() catch {};
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
                    // The library parses this off the ack subject for us, including the
                    // JetStream-domain form where every token shifts by two — the case
                    // the bridge's own parser had to special-case.
                    const out_of_attempts = msg.metadata.num_delivered >= config.Nats.mutation_max_deliver;
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
                        self.deadLetter(conn_nats, msg, payload, err);
                        self.publishVerdict(conn_nats, msg, mutation.principal, if (hopeless) "rejected" else "failed", @errorName(err));
                        msg.ack() catch {};
                        continue;
                    }

                    log.err("Failed to handle mutation, will retry: {}", .{err});
                    msg.nak() catch {};
                    utils.sleep(1 * std.time.ns_per_s);
                    continue;
                };

                // ⚠️ A verdict on **every** write that PostgreSQL accepted, not only the
                // ones that changed something. PROTOCOL.md §7.1 makes a client's outbox
                // rule "pop only on a definitive reply", and the three definitive replies
                // are all successes at the SQL level — `accepted`, `stale`, `row_deleted`.
                // Publishing only on failure, which is what this did before, left a
                // correct client unable to ever pop a successful write: it would retry
                // forever, idempotently and invisibly.
                //
                // This is the one place the bridge speaks per-message rather than
                // per-table, so it doubles ingress message count by construction. That is
                // the cost of the outbox protocol, not an accident of this
                // implementation — the alternative is a client that cannot distinguish
                // "applied" from "lost in transit".
                if (self.last_clamped) {
                    log.info(
                        "🕒 clamped a future version [{s}] on '{s}' → {s}",
                        .{ mutation.principal, mutation.table, self.lastVersion() },
                    );
                }
                self.publishVerdict(
                    conn_nats,
                    msg,
                    mutation.principal,
                    self.last_outcome.wireName(),
                    if (self.last_clamped) "version_clamped" else "",
                );

                msg.ack() catch {};
            }

            // Sleep ONLY when the fetch came back empty. This was a `for … else`,
            // meant as "sleep on timeout" — but Zig runs the else on every normal
            // completion, so the listener slept 100 ms after EVERY message and the
            // whole ingress lane was capped at ~10 writes/s regardless of backlog
            // (found by race.py: 24 writers × 5 writes outlived a 6 s window).
            if (batch.messages.len == 0) {
                utils.sleep(config.Nats.mutation_pull_idle_ms * std.time.ns_per_ms);
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
            error.RowTooLargeToReplicate,
            error.MissingVersion,
            error.NoVersionColumn,
            error.NoTombstoneColumn,
            error.NoPrimaryKey,
            // The client did nothing wrong: the table's key shape makes edge writes
            // unsafe, and only a migration can change that.
            error.DbAllocatedKey,
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
        conn_nats: *nats.Connection,
        msg: *nats.JetStreamMessage,
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
        const subject = msg.msg.subject;
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
        conn_nats.publish(err_subject, body) catch |perr| {
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
        conn_nats: *nats.Connection,
        msg: *nats.JetStreamMessage,
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
        const seq: u64 = msg.metadata.sequence.stream;

        const body = std.fmt.allocPrint(
            alloc,
            "{{\"status\":\"{s}\",\"reason\":\"{s}\",\"sqlstate\":\"{s}\",\"detail\":\"{s}\",\"seq\":{d},\"version\":\"{s}\"}}",
            .{ status, reason, self.lastSqlstate(), self.lastError(), seq, self.lastVersion() },
        ) catch return;

        // ⚠️ Outside `mutation.>`, like the dead-letter subject and for the same reason:
        // the ingress consumer filters on that prefix, so a verdict published under it
        // would be consumed by this loop as if it were a write — a feedback loop that
        // ends in a poison pill.
        conn_nats.publish(subject, body) catch |perr| {
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
            // The client did nothing wrong: the table's key shape makes edge writes
            // unsafe, and only a migration can change that.
            error.DbAllocatedKey,
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
        // ⚠️ The stored version is sticky for the same reason and must be cleared for the
        // same reason: without this a write whose guard rejected it (no RETURNING row)
        // would report the *previous* message's version as its own, which is the bug that
        // once made a verdict quote another table's SQLSTATE.
        self.last_version_len = 0;
        self.last_clamped = false;
        // Sticky like the rest: without this a write whose statement never ran
        // would report the previous message's outcome as its own.
        self.last_outcome = .applied;
    }

    fn lastSqlstate(self: *const MutationListener) []const u8 {
        return self.last_sqlstate_buf[0..self.last_sqlstate_len];
    }

    /// The version PostgreSQL stored, or empty when nothing was stored.
    fn lastVersion(self: *const MutationListener) []const u8 {
        return self.last_version_buf[0..self.last_version_len];
    }

    fn lastError(self: *const MutationListener) []const u8 {
        return self.last_error_buf[0..self.last_error_len];
    }

    // `parseAckMeta` and `AckMeta` lived here: they read the delivery count and stream
    // sequence out of the `$JS.ACK.…` reply subject, counting tokens from the end because
    // the layout shifts by two when a JetStream domain is configured. nats.zig parses
    // both into `msg.metadata` (and handles the domain form, with tests), so the hand
    // parser and its four tests are gone.

    /// The client's own `Nats-Msg-Id`, which is the only correlation token it chose
    /// itself — and therefore the only one it can match against a queued write without
    /// having stored a server-assigned number.
    ///
    /// JetStream consumes this header for deduplication; nothing surfaced it to the
    /// bridge before, so a verdict had no way to name the write it was about.
    fn msgIdOf(msg: *nats.JetStreamMessage) ?[]const u8 {
        return msg.msg.headerGet("Nats-Msg-Id");
    }

    // `headerValue` lived here: a hand-rolled parse of the raw header block, written
    // because the vendored client's `Headers.hiter()` silently yielded nothing (it is
    // `std.http.HeaderIterator`, which needs a trailing CRLF that `read_HMSG` had already
    // stripped). `msg.headerGet(name)` replaces it.

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
    ///
    /// ⚠️ `zebridge_gc_watermark` was NOT on this list, and that was exploitable. Proven
    /// 2026-08-28 against the compose stack: `alice`, an ordinary principal whose only
    /// publish grant is `mutation.alice.>`, sent one envelope to
    /// `mutation.alice.zebridge_gc_watermark.update` and PostgreSQL changed —
    ///
    ///     watermark     2026-08-28  ->  2020-01-01
    ///     threshold_ms  0           ->  999999999
    ///     ✅ mutation applied [alice] on 'zebridge_gc_watermark' (1 row)
    ///
    /// Nothing else stood in the way: the table is in `zebridge_catalogue` (so the
    /// listener resolves its identifiers), and `bridge_writer` holds UPDATE on it
    /// BECAUSE THE SWEEPER NEEDS IT — that grant cannot be revoked, which is exactly why
    /// the name check has to exist. The watermark bounds the offline window for every
    /// client, so moving it is a global act by one principal.
    ///
    /// `zebridge_user_tenants` is the principal→tenant roster: a forged row is
    /// cross-tenant privilege escalation.
    ///
    /// `zebridge_invites` is the enrollment table, and `bridge_writer` holds
    /// SELECT+UPDATE on it because redeeming an invite stamps `used_at`. UPDATE covers
    /// every column, so a client who knows any code — their own, already spent — could
    /// clear `used_at` to replay it, push `expires_at` out, or rewrite
    /// `principal`/`tenant_id`/`role` and enrol as somebody else in another tenant.
    ///
    /// ⚠️ Both of those were reachable only BY ACCIDENT: neither is in
    /// `zebridge_catalogue`, so the listener fails to resolve their identifiers. That
    /// is not a control — it is a property of a config table that a future migration
    /// could change without anyone connecting it to this. `zebridge_gc_watermark` IS in
    /// the catalogue, which is the whole difference between the three, and it is the
    /// one that was actually exploited (§10aq).
    fn isForbiddenTable(table: []const u8) bool {
        return std.mem.eql(u8, table, "zebridge_ddl_events") or
            std.mem.eql(u8, table, "zebridge_gc_watermark") or
            std.mem.eql(u8, table, "zebridge_user_tenants") or
            std.mem.eql(u8, table, "zebridge_invites") or
            std.mem.eql(u8, table, "schema_migrations");
    }

    /// Read a table's identifiers from the catalog, once, and remember them.
    ///
    /// Cached because a mutation is already one synchronous round trip and this would
    /// double it. Two signals drop it, and it takes both to cover a DDL change:
    ///
    ///   * a statement failing with `42703 undefined_column` / `42P01 undefined_table` —
    ///     the *removal* case, where the cached list still names something gone;
    ///   * a payload refused as `UnknownColumn` / `MissingPrimaryKey` (`handleMutation`) —
    ///     the *addition* case, which never reaches PostgreSQL and so produces no
    ///     SQLSTATE to react to.
    ///
    /// Neither is a timer: nothing here polls the catalog.
    fn tableMeta(self: *MutationListener, conn: ?*c.PGconn, table: []const u8) !*const TableMeta {
        const epoch = self.catalog_epoch.current();
        if (self.meta_cache.getPtr(table)) |cached| {
            if (cached.epoch == epoch) return cached;
            // DDL crossed the stream since these were read. Cheaper to re-read one
            // catalog than to build a statement from a shape that has moved.
            self.invalidate(table);
        }

        const alloc = self.allocator;

        // `$1::regclass` resolves through search_path and errors on a table that does not
        // exist — so an unknown name is rejected by PostgreSQL's own parser rather than
        // by string matching here.
        const query =
            \\SELECT a.attname,
            \\       COALESCE(k.ord, 0) AS pk_ord,
            \\       a.atttypid::int AS type_oid,
            \\       -- `typcategory = 'A'` is PostgreSQL's own answer to "is this an
            \\       -- array type", which beats a hardcoded OID list that would miss
            \\       -- every domain and every user-defined array.
            \\       t.typcategory::text AS type_cat,
            \\       t.typname::text AS type_name,
            \\       (k.ord IS NOT NULL
            \\        AND pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) IS NOT NULL)
            \\         AS key_from_sequence
            \\FROM pg_attribute a
            \\JOIN pg_type t ON t.oid = a.atttypid
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
        // Collected in the same pass so the version column's type is known without a
        // second query; matched by name below, once SYNC_RULES has said which it is.
        var type_oids: std.ArrayList(u32) = .empty;
        defer type_oids.deinit(alloc);
        var kinds: std.ArrayList(ColKind) = .empty;
        errdefer kinds.deinit(alloc);
        var key_from_sequence = false;
        var r: usize = 0;
        while (r < rows) : (r += 1) {
            // ⚠️ Column indices, in the query's own order: 0 attname, 1 pk_ord,
            // 2 type_oid, 3 type_cat, 4 type_name, 5 key_from_sequence. Adding the two
            // type columns SHIFTED key_from_sequence from 3 to 5 — read positionally,
            // so a new column in the middle silently changes what every later index
            // means.
            const name = std.mem.span(c.PQgetvalue(res, @intCast(r), 0));
            const ord = std.fmt.parseInt(usize, std.mem.span(c.PQgetvalue(res, @intCast(r), 1)), 10) catch 0;
            const oid = std.fmt.parseInt(u32, std.mem.span(c.PQgetvalue(res, @intCast(r), 2)), 10) catch 0;
            const cat = std.mem.span(c.PQgetvalue(res, @intCast(r), 3));
            const tname = std.mem.span(c.PQgetvalue(res, @intCast(r), 4));
            if (std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, @intCast(r), 5)), "t")) key_from_sequence = true;

            const kind: ColKind = if (std.mem.eql(u8, tname, "json") or std.mem.eql(u8, tname, "jsonb"))
                .json
            else if (std.mem.eql(u8, cat, "A"))
                .array
            else
                .scalar;

            const owned = try alloc.dupe(u8, name);
            try columns.append(alloc, owned);
            try type_oids.append(alloc, oid);
            try kinds.append(alloc, kind);
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
        var client_name: ?[]const u8 = null;
        if (self.sync_rules.get(table)) |cols| {
            // Positional, and an empty entry means "not configured" — `updated_at,,x`
            // is version + tiebreak with NO tombstone (see parseTableRules).
            if (cols.len > 0 and cols[0].len > 0) version_name = cols[0];
            if (cols.len > 1 and cols[1].len > 0) tombstone_name = cols[1];
            // `table:updated_at,deleted_at,last_writer` — the third column is where the
            // winner's client_id is kept. Opt-in per table because it costs a column, and
            // a table that never sees concurrent equal versions does not need one.
            if (cols.len > 2 and cols[2].len > 0) client_name = cols[2];
        }

        var version_type: VersionType = .other;
        for (columns.items, 0..) |col, i| {
            if (std.mem.eql(u8, col, version_name)) {
                version_type = VersionType.fromOid(type_oids.items[i]);
                break;
            }
        }

        var meta = TableMeta{
            .pk_cols = try pk.toOwnedSlice(alloc),
            .columns = try columns.toOwnedSlice(alloc),
            .version_col = try alloc.dupe(u8, version_name),
            .col_kinds = try kinds.toOwnedSlice(alloc),
            .version_type = version_type,
            .client_col = if (client_name) |cn| try alloc.dupe(u8, cn) else null,
            .tombstone_col = if (tombstone_name) |t| try alloc.dupe(u8, t) else null,
        };
        errdefer meta.deinit(alloc);

        // ⚠️ **Refused, not warned.** A client mints its own key (§7.2), and an explicit
        // value does not advance `nextval` — so every edge write to a sequence-backed key
        // plants a value the application's own next INSERT will collide with, and two
        // offline clients minting from the same small integers collide by construction.
        //
        // What makes it worse than the other refusals is that nothing fails: the upsert's
        // `ON CONFLICT DO UPDATE` overwrites whichever row was there first, and
        // last-write-wins picks between two rows that were never related. Silent
        // cross-row data loss, surfacing in the application months later.
        //
        // ⚠️ Scoped to the WRITE path on purpose. The table replicates outbound perfectly
        // well and its readers are not at risk, so refusing it in the shared registry
        // would drop CDC and snapshots for a hazard that only exists on ingress.
        //
        // The escape is a migration, not a flag: use a uuid key, or drop the DEFAULT and
        // assign each client a disjoint range (§7.2) — that turns the sequence off, which
        // is what this detects.
        if (key_from_sequence) {
            log.err(
                "🔴 '{s}': primary key is database-allocated (serial/identity), so edge writes are refused. A client-supplied key does not advance the sequence: writes would SUCCEED and then collide with the application's own inserts, overwriting unrelated rows through ON CONFLICT. Use a uuid key, or drop the column DEFAULT and assign clients disjoint ranges.",
                .{table},
            );
            return error.DbAllocatedKey;
        }

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
        meta.epoch = epoch;
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

        // ── the ingress counterpart of the egress size guard ────────────────────
        //
        // ⚠️ A write the change feed cannot carry back out must not be applied. CDC packs
        // each row into a fixed `2^BASE_BUF` buffer; a row that overflows it suspends the
        // **whole table** for **every** client (`row_too_large`). So without this check a
        // single authorised writer could send one legal 50 KB row — under `max_payload`,
        // accepted by PostgreSQL, answered `accepted` — and quarantine a table everybody
        // else was reading. Measured before this existed.
        //
        // That is the wrong shape of failure: a bad write should cost its sender a
        // verdict, not cost every reader a table. Suspension is for a table whose *shape*
        // is wrong — a migration — which no client can cause.
        //
        // ⚠️ The size is **measured, never declared**. `payload_bytes` is what NATS
        // delivered; a `size` field in the envelope would be a number the sender chooses,
        // and the sender is exactly who this is guarding against. Same rule as `client_id`
        // (stamped from the envelope, not read from `data`) and the principal (a subject
        // token the broker vouched for).
        //
        // ⚠️ Deliberately a lower bound, not an estimate. The CDC event carries every
        // column plus its name, so it is always **larger** than this payload — meaning a
        // payload that already exceeds the buffer certainly will not fit, and no
        // legitimate write is refused. A row that is oversized for some *other* reason
        // (columns this mutation never touched) is not caught here and still suspends the
        // table, which is correct: that row was not created by this write.
        if (payload_bytes.len >= self.event_buf_bytes) {
            log.info(
                "⛔ Mutation refused [{s}] on '{s}': {d} bytes exceeds the {d}-byte CDC event buffer (BASE_BUF). Applying it would suspend the table for every reader.",
                .{ mutation.principal, mutation.table, payload_bytes.len, self.event_buf_bytes },
            );
            return error.RowTooLargeToReplicate;
        }

        // Was this served from cache? It decides whether a schema-shaped refusal below is
        // worth a second look: a meta just read from the catalog cannot be stale, so a
        // cold-cache payload that is simply wrong pays nothing extra.
        const meta_was_cached = if (self.meta_cache.getPtr(mutation.table)) |m|
            m.epoch == self.catalog_epoch.current()
        else
            false;
        const meta = try self.tableMeta(conn, mutation.table);

        var reader = std.Io.Reader.fixed(payload_bytes);
        var dummy_out: [0]u8 = .{};
        var writer = std.Io.Writer.fixed(&dummy_out);
        var packer = @import("msgpack").PackerIO.init(&reader, &writer);

        // A decode failure is PERMANENT by nature: the same bytes decode the same
        // way forever, so a truncated or garbage payload must dead-letter on first
        // delivery, never enter the transient-retry budget. Before this, a
        // truncated msgpack raised `error.EndOfStream`, which fell through to
        // "will retry" and was redelivered mutation_max_deliver times — a handful
        // of malformed messages stalled the single-consumer queue long enough to
        // block legitimate writes behind them (a poison-pill DoS on the mutation
        // lane, found by adversarial.py). Only OutOfMemory stays transient: it is
        // genuine memory pressure that a retry can outlast. Every other decode
        // error collapses to InvalidPayloadFormat, which isPermanent() already
        // dead-letters — future-proof against any error the msgpack lib adds.
        const payload = packer.read(alloc) catch |err| {
            if (err == error.OutOfMemory) return error.OutOfMemory;
            return error.InvalidPayloadFormat;
        };
        if (payload != .map) return error.InvalidPayloadFormat;
        const map = payload.map;

        const version_val = map.getByString("version") orelse return error.MissingVersion;
        const version_text = try self.payloadToString(alloc, version_val) orelse return error.MissingVersion;

        const key_val = map.getByString("key") orelse return error.MissingPrimaryKey;
        if (key_val != .map) return error.InvalidPrimaryKeyFormat;

        self.applyWithMeta(alloc, conn, meta, mutation, map, key_val.map, version_text) catch |err| {
            // ⚠️ These two are raised by comparing the payload against the *cached*
            // catalog, before any SQL runs — so PostgreSQL never sees the statement and
            // never answers `42703 undefined_column`, which is the only signal that
            // drops the cache. Without this second look the cache has no way to learn
            // about a column or a key that appeared after it was filled, and the refusal
            // is permanent: `ALTER TABLE … ADD COLUMN` publishes a schema saying the
            // column exists, every client migrates to it, and every write naming it is
            // refused until the bridge is restarted. Measured — see
            // scripts/scenarios/invalidate.py §1.
            //
            // The reverse case heals on its own: a *dropped* column is still in the
            // cached list, so the statement is built, PostgreSQL answers 42703, and the
            // existing invalidation fires.
            if (!meta_was_cached) return err;
            if (err != error.UnknownColumn and err != error.MissingPrimaryKey) return err;

            self.invalidate(mutation.table);
            const fresh = try self.tableMeta(conn, mutation.table);
            log.info(
                "Mutation listener: '{s}' re-read from the catalog after {s}; retrying once",
                .{ mutation.table, @errorName(err) },
            );
            // Once. If the fresh catalog refuses it too, the payload is wrong rather than
            // the cache, and the verdict says so.
            return self.applyWithMeta(alloc, conn, fresh, mutation, map, key_val.map, version_text);
        };
    }

    /// The part of a mutation that depends on the table's shape, so it can be run twice
    /// against two different `meta` values without re-parsing the payload.
    fn applyWithMeta(
        self: *MutationListener,
        alloc: std.mem.Allocator,
        conn: ?*c.PGconn,
        meta: *const TableMeta,
        mutation: Mutation,
        map: anytype,
        key_map: anytype,
        version_text: [*:0]const u8,
    ) !void {
        // Every key column must be present. A partial key would let `ON CONFLICT` match
        // a different row than the client meant, or a DELETE remove more rows than it
        // asked for.
        var key_values: std.ArrayList(?[*:0]const u8) = .empty;
        for (meta.pk_cols) |col| {
            const v = key_map.getByString(col) orelse return error.MissingPrimaryKey;
            const as_text = try self.payloadToString(alloc, v) orelse return error.MissingPrimaryKey;
            try key_values.append(alloc, as_text);
        }

        switch (mutation.operation) {
            .delete => try self.applyDelete(alloc, conn, meta, mutation, key_values.items, version_text),
            .update => try self.applyUpdate(alloc, conn, meta, mutation, map, version_text, key_values.items),
            .insert => try self.applyUpsert(alloc, conn, meta, mutation, map, version_text, key_values.items),
        }
    }

    /// "Is the row there, and is it deleted?" — asked of the same transaction, so its
    /// answer describes the state the write left behind.
    ///
    /// ⚠️ This exists because **`stale` and `row_deleted` are indistinguishable from the
    /// row count**: both are zero. Without it the bridge can only say "nothing changed",
    /// and a client cannot tell "someone else edited this, drop your copy" from "the row
    /// you are editing no longer exists, tell the user" — which PROTOCOL.md §7.1 makes it
    /// responsible for surfacing.
    ///
    /// Costs no round trip: it rides the same pipeline as the write, so the whole exchange
    /// is still one network turn. It does cost a primary-key lookup on every mutation,
    /// including the ones that succeeded, which is the price of being able to answer
    /// without a second exchange in the cases that did not.
    fn buildClassify(alloc: std.mem.Allocator, meta: *const TableMeta, table: []const u8) ![]const u8 {
        var out: std.ArrayListUnmanaged(u8) = .empty;
        try out.appendSlice(alloc, "SELECT ");
        if (meta.tombstone_col) |tombstone| {
            try appendIdent(&out, alloc, tombstone);
            try out.appendSlice(alloc, " IS NOT NULL");
        } else {
            // No tombstone column, so the only kind of "deleted" this table has is the
            // row being absent — which the empty result already says.
            try out.appendSlice(alloc, "false");
        }
        try out.appendSlice(alloc, " AS zb_deleted FROM ");
        try appendIdent(&out, alloc, table);
        try out.appendSlice(alloc, " WHERE ");
        for (meta.pk_cols, 0..) |col, i| {
            if (i > 0) try out.appendSlice(alloc, " AND ");
            try appendIdent(&out, alloc, col);
            try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, " = ${d}", .{i + 1}));
        }
        return out.items;
    }

    /// `RETURNING "version"` — plus, when the column can be clamped, a boolean saying
    /// whether it *was*.
    ///
    /// ⚠️ The comparison is made by PostgreSQL against the raw parameter, not in Zig
    /// against the returned text. The two are the same instant in different notations
    /// (`2026-08-19T05:12:33.123456Z` from the client, `2026-08-19 05:12:33.123456+00`
    /// back from libpq), so a string comparison would report every single write as
    /// clamped.
    fn buildReturning(
        alloc: std.mem.Allocator,
        meta: *const TableMeta,
        version_param: usize,
    ) ![]const u8 {
        var out: std.ArrayListUnmanaged(u8) = .empty;
        try out.appendSlice(alloc, " RETURNING ");
        if (meta.version_type.wireFormat()) |wire| {
            try out.appendSlice(alloc, wire.prefix);
            try appendIdent(&out, alloc, meta.version_col);
            try out.appendSlice(alloc, wire.suffix);
        } else {
            try appendIdent(&out, alloc, meta.version_col);
        }
        if (meta.version_type.castSuffix()) |cast| {
            try out.appendSlice(alloc, ", (");
            try appendIdent(&out, alloc, meta.version_col);
            try out.appendSlice(alloc, try std.fmt.allocPrint(
                alloc,
                " IS DISTINCT FROM ${d}{s}) AS zb_clamped",
                .{ version_param, cast },
            ));
        }
        return out.items;
    }

    /// `$n`, or the clamped form when this is a timestamp version column.
    ///
    /// PROTOCOL.md §7.2 tells clients "do not send a future timestamp… the bridge clamps
    /// and tells you what it used", and until this existed the bridge did neither. The
    /// consequence of not clamping is not a bad value, it is a **frozen row**: a version
    /// stored a year ahead makes `stored < incoming` false for every later write, from
    /// every client, with no error anywhere — the row simply stops accepting edits until
    /// wall-clock time passes it.
    ///
    /// ⚠️ Clamped in SQL rather than in Zig, deliberately. The comparison must be against
    /// the **database's** clock, because that is the clock every other writer's version
    /// comes from; a bridge-side check would compare against a third clock and could
    /// itself be skewed. It also costs no round trip, being part of the statement.
    ///
    /// ⚠️ The value is still a parameter. Only the *expression around* it is generated,
    /// so nothing client-supplied is ever concatenated into SQL.
    fn renderValuePlaceholder(
        alloc: std.mem.Allocator,
        meta: *const TableMeta,
        col: []const u8,
        n: usize,
    ) ![]const u8 {
        if (!std.mem.eql(u8, col, meta.version_col)) {
            return try std.fmt.allocPrint(alloc, "${d}", .{n});
        }
        return renderVersionExpr(alloc, meta, n);
    }

    /// `$n` wrapped in this table's clamp, or bare when the version type has no future.
    fn renderVersionExpr(alloc: std.mem.Allocator, meta: *const TableMeta, n: usize) ![]const u8 {
        const placeholder = try std.fmt.allocPrint(alloc, "${d}", .{n});
        const clamp = meta.version_type.clampExpr() orelse return placeholder;
        return try std.fmt.allocPrint(alloc, "{s}{s}{s}", .{ clamp.prefix, placeholder, clamp.suffix });
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
        key_values: []const ?[*:0]const u8,
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
            // Set from the envelope's `client_id`. Letting `data` carry it would let a
            // client claim any identity for tiebreaking purposes.
            if (meta.client_col) |cc| {
                if (std.mem.eql(u8, name, cc)) continue;
            }
            try cols.append(alloc, name);
            // Typed: the column decides the text form (ColKind). Only the DATA binds
            // go through this — the version and the primary key are scalars by
            // definition, and routing them here would only add a lookup.
            try vals.append(alloc, try self.payloadToStringTyped(alloc, entry.value_ptr.*, meta.kindOf(name)));
        }

        // The version is the bridge's to write, from the message's `version` field —
        // never from `data`, so a client cannot claim one value and stamp another.
        try cols.append(alloc, meta.version_col);
        try vals.append(alloc, version_text);

        // Same for the tiebreak column: stamped from the envelope's `client_id`, never
        // from `data`. Written on every upsert, because a row whose stored id is stale
        // would break ties against the wrong writer — and a NULL one loses every tie
        // (coalesced to '' in the guard), which would silently favour whoever wrote last
        // rather than whoever the rule says.
        //
        // ⚠️ Only when the table declares the column. `data` may not name it either: it is
        // in `meta.columns`, so a client *could* set it directly and choose which id it
        // appears to be — the same forgery the version column is protected from. Filtered
        // out of the data loop above for that reason.
        if (meta.client_col) |client_col| {
            const cid = if (map.getByString("client_id")) |v|
                try self.payloadToString(alloc, v)
            else
                null;
            try cols.append(alloc, client_col);
            try vals.append(alloc, cid);
        }

        var sql: std.ArrayListUnmanaged(u8) = .empty;
        try sql.appendSlice(alloc, "INSERT INTO ");
        try appendIdent(&sql, alloc, mutation.table);
        try sql.appendSlice(alloc, " (");
        for (cols.items, 0..) |col, i| {
            if (i > 0) try sql.appendSlice(alloc, ", ");
            try appendIdent(&sql, alloc, col);
        }
        try sql.appendSlice(alloc, ") VALUES (");
        for (cols.items, 0..) |col, i| {
            if (i > 0) try sql.appendSlice(alloc, ", ");
            try sql.appendSlice(alloc, try renderValuePlaceholder(alloc, meta, col, i + 1));
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

        // ── The tie ────────────────────────────────────────────────────────────────
        //
        // `<` alone **rejects** equal versions, which is not a resolution: two replicas
        // holding different rows both refuse the other's write and stay divergent forever,
        // with no error anywhere. PROTOCOL.md §7.3 says the comparison is
        // `(version, client_id)` for exactly this reason.
        //
        // A tie is not exotic. It is the *normal* case for an integer version column
        // (`stored + 1` from two clients that read the same value), and it happens with
        // timestamps whenever two writes land in the same microsecond.
        //
        // The rule is arbitrary but must be **total and identical everywhere**: the higher
        // client_id wins. Any deterministic order works; what matters is that two replicas
        // given the same pair of writes reach the same row, which a rejection does not
        // guarantee and a coin flip actively breaks.
        //
        // ⚠️ Only when the table declares a column to store the winner's id (the third
        // `SYNC_RULES` field). Without one there is nothing to compare against — the
        // stored id has to be *on the row* — so a tie is refused exactly as before, which
        // is the safe default rather than a silent coin flip.
        if (meta.client_col) |client_col| {
            try sql.appendSlice(alloc, " OR (");
            try appendIdent(&sql, alloc, mutation.table);
            try sql.appendSlice(alloc, ".");
            try appendIdent(&sql, alloc, meta.version_col);
            try sql.appendSlice(alloc, " = EXCLUDED.");
            try appendIdent(&sql, alloc, meta.version_col);
            try sql.appendSlice(alloc, " AND coalesce(");
            try appendIdent(&sql, alloc, mutation.table);
            try sql.appendSlice(alloc, ".");
            try appendIdent(&sql, alloc, client_col);
            try sql.appendSlice(alloc, ", '') < coalesce(EXCLUDED.");
            try appendIdent(&sql, alloc, client_col);
            try sql.appendSlice(alloc, ", ''))");
        }

        var version_param: usize = 0;
        for (cols.items, 0..) |col, i| {
            if (std.mem.eql(u8, col, meta.version_col)) {
                version_param = i + 1;
                break;
            }
        }
        try self.exec(
            alloc,
            conn,
            mutation,
            sql.items,
            vals.items,
            try buildReturning(alloc, meta, version_param),
            try buildClassify(alloc, meta, mutation.table),
            key_values,
        );
    }

    /// UPDATE <table> SET col1 = $1, … WHERE <pk> = $key AND (version guard [OR tie]).
    ///
    /// Not an upsert: `applyUpsert` builds `INSERT (cols from data) … ON CONFLICT DO
    /// UPDATE`, and PostgreSQL must be able to satisfy the INSERT branch even though only
    /// the UPDATE branch can ever fire for a row the client already knows exists — every
    /// NOT NULL column without a database-level default has to be present in `data` or the
    /// statement is rejected before ON CONFLICT is even considered. Measured: a genuine
    /// partial update — `{some_text, updated_at}` against a table whose `inserted_at` is
    /// NOT NULL with no DB default — failed `23502` even though the row already existed
    /// and `inserted_at` was never going to change. A real UPDATE has no INSERT branch to
    /// satisfy, so it only ever needs the columns actually changing.
    ///
    /// Same last-write-wins guard and tie-break as `applyUpsert`, just written against a
    /// bound parameter instead of `EXCLUDED` — there is no pseudo-table in a plain UPDATE.
    fn applyUpdate(
        self: *MutationListener,
        alloc: std.mem.Allocator,
        conn: ?*c.PGconn,
        meta: *const TableMeta,
        mutation: Mutation,
        map: anytype,
        version_text: [*:0]const u8,
        key_values: []const ?[*:0]const u8,
    ) !void {
        const data_val = map.getByString("data") orelse return error.MissingData;
        if (data_val != .map) return error.InvalidDataFormat;

        var cols: std.ArrayList([]const u8) = .empty;
        var vals: std.ArrayList(?[*:0]const u8) = .empty;

        var it = data_val.map.map.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.* != .str) continue;
            const name = entry.key_ptr.str.value();
            if (!meta.hasColumn(name)) {
                log.info("⛔ '{s}' has no column '{s}' — refusing mutation from '{s}'", .{ mutation.table, name, mutation.principal });
                return error.UnknownColumn;
            }
            if (meta.isPk(name)) continue; // the key it matched on — never reassigned
            if (std.mem.eql(u8, name, meta.version_col)) continue; // set from `version`
            if (meta.client_col) |cc| {
                if (std.mem.eql(u8, name, cc)) continue;
            }
            try cols.append(alloc, name);
            // Typed: the column decides the text form (ColKind). Only the DATA binds
            // go through this — the version and the primary key are scalars by
            // definition, and routing them here would only add a lookup.
            try vals.append(alloc, try self.payloadToStringTyped(alloc, entry.value_ptr.*, meta.kindOf(name)));
        }

        try cols.append(alloc, meta.version_col);
        try vals.append(alloc, version_text);

        if (meta.client_col) |client_col| {
            const cid = if (map.getByString("client_id")) |v|
                try self.payloadToString(alloc, v)
            else
                null;
            try cols.append(alloc, client_col);
            try vals.append(alloc, cid);
        }

        var version_param: usize = 0;
        var client_param: usize = 0;
        for (cols.items, 0..) |col, i| {
            if (std.mem.eql(u8, col, meta.version_col)) version_param = i + 1;
            if (meta.client_col) |cc| {
                if (std.mem.eql(u8, col, cc)) client_param = i + 1;
            }
        }
        const version_expr = try renderVersionExpr(alloc, meta, version_param);

        var sql: std.ArrayListUnmanaged(u8) = .empty;
        try sql.appendSlice(alloc, "UPDATE ");
        try appendIdent(&sql, alloc, mutation.table);
        try sql.appendSlice(alloc, " SET ");
        for (cols.items, 0..) |col, i| {
            if (i > 0) try sql.appendSlice(alloc, ", ");
            try appendIdent(&sql, alloc, col);
            try sql.appendSlice(alloc, " = ");
            try sql.appendSlice(alloc, try renderValuePlaceholder(alloc, meta, col, i + 1));
        }
        try sql.appendSlice(alloc, " WHERE ");
        for (meta.pk_cols, key_values, 0..) |col, val, i| {
            if (i > 0) try sql.appendSlice(alloc, " AND ");
            try appendIdent(&sql, alloc, col);
            try sql.appendSlice(alloc, try std.fmt.allocPrint(alloc, " = ${d}", .{cols.items.len + i + 1}));
            try vals.append(alloc, val);
        }
        // Same "IS NULL OR stale" guard as the upsert and the delete: a stale update must
        // not win, and a never-stamped row must not refuse every write forever.
        try sql.appendSlice(alloc, " AND (");
        try appendIdent(&sql, alloc, meta.version_col);
        try sql.appendSlice(alloc, " IS NULL OR ");
        try appendIdent(&sql, alloc, meta.version_col);
        try sql.appendSlice(alloc, " < ");
        try sql.appendSlice(alloc, version_expr);
        if (meta.client_col) |client_col| {
            try sql.appendSlice(alloc, " OR (");
            try appendIdent(&sql, alloc, meta.version_col);
            try sql.appendSlice(alloc, " = ");
            try sql.appendSlice(alloc, version_expr);
            try sql.appendSlice(alloc, " AND coalesce(");
            try appendIdent(&sql, alloc, client_col);
            try sql.appendSlice(alloc, ", '') < coalesce(");
            try sql.appendSlice(alloc, try std.fmt.allocPrint(alloc, "${d}", .{client_param}));
            try sql.appendSlice(alloc, ", ''))");
        }
        try sql.appendSlice(alloc, ")");

        try self.exec(
            alloc,
            conn,
            mutation,
            sql.items,
            vals.items,
            try buildReturning(alloc, meta, version_param),
            try buildClassify(alloc, meta, mutation.table),
            key_values,
        );
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

        // `$1` capped at the database's clock, exactly as the upsert caps it — see
        // `renderValuePlaceholder`. Used for the guard too, so the value a delete is
        // *compared* by is the value it would *store*; clamping only one of the two
        // would let a skewed client win a comparison it then does not record.
        //
        // ⚠️ The tombstone gets the same clamped expression. A future `deleted_at` is not
        // cosmetic: the sweeper reaps on `deleted_at < now() - threshold`, so a row
        // tombstoned a year ahead is never reaped and the soft delete never completes.
        const version_expr = try renderVersionExpr(alloc, meta, 1);

        if (meta.tombstone_col) |tombstone| {
            try sql.appendSlice(alloc, "UPDATE ");
            try appendIdent(&sql, alloc, mutation.table);
            try sql.appendSlice(alloc, " SET ");
            try appendIdent(&sql, alloc, tombstone);
            try sql.appendSlice(alloc, " = ");
            try sql.appendSlice(alloc, version_expr);
            try sql.appendSlice(alloc, ", ");
            try appendIdent(&sql, alloc, meta.version_col);
            try sql.appendSlice(alloc, " = ");
            try sql.appendSlice(alloc, version_expr);
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
        try sql.appendSlice(alloc, " < ");
        try sql.appendSlice(alloc, version_expr);
        try sql.appendSlice(alloc, ")");

        try self.exec(
            alloc,
            conn,
            mutation,
            sql.items,
            params.items,
            try buildReturning(alloc, meta, 1),
            try buildClassify(alloc, meta, mutation.table),
            key_values,
        );
    }

    /// Run the statement, and heal the catalog cache when the schema has moved.
    fn exec(
        self: *MutationListener,
        alloc: std.mem.Allocator,
        conn: ?*c.PGconn,
        mutation: Mutation,
        sql: []const u8,
        params: []const ?[*:0]const u8,
        /// Already-built `RETURNING …`, because only the caller knows which parameter
        /// index carries the version.
        returning: []const u8,
        /// Third statement in the pipeline: what state the row is in afterwards. See
        /// `buildClassify` — it is what separates `stale` from `row_deleted`.
        classify_sql: []const u8,
        classify_params: []const ?[*:0]const u8,
    ) !void {
        // ⚠️ `RETURNING` the version is not decoration: PROTOCOL.md §7.2 promises the
        // bridge "tells you what it used", and after a clamp the value the client sent is
        // not the value stored. Reading it back from the statement is the only account
        // that cannot drift — a bridge-side calculation would be guessing at what
        // PostgreSQL's `now()` was at the moment it evaluated.
        //
        // Returns no row when the last-write-wins guard rejects the write, which is the
        // same signal as the zero row count already handled below.
        var full: std.ArrayListUnmanaged(u8) = .empty;
        try full.appendSlice(alloc, sql);
        try full.appendSlice(alloc, returning);
        const sql_z = try alloc.dupeZ(u8, full.items);
        log.debug("mutation [{s}] {s}", .{ mutation.principal, sql_z });

        // ── The principal, handed to PostgreSQL so its policies can use it ──────────
        //
        // Row-level security is what bounds *which rows* a principal may write, and a
        // policy can only reason about an identity the session carries. Without this the
        // whole write-scoping design is inert — and worse than inert: a policy reading
        // `current_setting('zb.principal', true)` gets NULL, the predicate evaluates to
        // NULL, and **every** write is refused with `new row violates row-level security
        // policy`. Enabling RLS would look like it broke the bridge.
        //
        // ⚠️ `SET LOCAL` is transaction-scoped, and outside a transaction PostgreSQL warns
        // and does nothing — so the statement must be wrapped. One mutation, one
        // transaction: a batch sharing a transaction would share the setting, and the
        // second principal's rows would be written under the first one's identity.
        //
        // `set_config(..., is_local => true)` rather than `SET LOCAL zb.principal = '…'`
        // because it takes the value as a **parameter**. The principal comes from the
        // subject and is vouched for by NATS, but building SQL by concatenation is how a
        // trusted value becomes an injection the day the trust assumption changes.
        // ── One network exchange, not four ──────────────────────────────────────
        //
        // ⚠️ These two statements MUST share a transaction. `set_config(…, is_local =>
        // true)` lasts until the transaction ends, and the RLS policies on the target
        // table read `zb.principal` back; sent as two transactions the setting would be
        // gone by the time the row is checked and **every** RLS-protected write would be
        // refused.
        //
        // The transaction is the pipeline's own: statements queued between two sync
        // points form one **implicit** transaction. That is stronger than the explicit
        // BEGIN/COMMIT this replaces — on error an implicit transaction rolls back what
        // ran and skips what was queued, whereas an explicit one leaves the session in a
        // failed-transaction state that needs a ROLLBACK before the connection is usable
        // again.
        //
        // Why at all: BEGIN, set_config, upsert, COMMIT were four separate round trips,
        // and on a remote PostgreSQL **round trips are the cost** — 4×RTT per row no
        // matter how fast the server is. Measured colocated, the catalog read looked like
        // 9% of a mutation; counted in round trips it is 25%, and that ratio is what
        // survives the move to a real network. Pipelining sends everything before reading
        // anything, so the same work costs one RTT.
        //
        // ⚠️ Client-side only: libpq ≥ 14 to **build**, and works against any server
        // speaking the v3 extended query protocol. It does not raise the PostgreSQL
        // version floor.
        if (c.PQenterPipelineMode(conn) != 1) {
            const msg_ptr = c.PQerrorMessage(conn);
            self.rememberFailure("", if (msg_ptr != null) std.mem.span(msg_ptr) else "");
            log.err("🔴 Could not enter pipeline mode for [{s}]: {s}", .{ mutation.principal, c.PQerrorMessage(conn) });
            return error.MutationFailed;
        }
        // Leaving the connection in pipeline mode would break every later statement on
        // it, so this unwinds on every path out — including the error returns below.
        //
        // ⚠️ Checked, not discarded. It fails when results are still unconsumed, and a
        // silent failure here is how the write path wedges: the connection stays in
        // pipeline mode and every subsequent mutation misaligns against it. Saying so is
        // the difference between a diagnosable stall and ingress simply going quiet.
        defer {
            if (c.PQexitPipelineMode(conn) != 1) {
                log.err(
                    "🔴 Could not leave pipeline mode ({s}) — the connection is unusable for further mutations and will be reset",
                    .{c.PQerrorMessage(conn)},
                );
                // Force the next iteration to notice: a reset is cheaper than a listener
                // that appears alive and accepts nothing.
                c.PQreset(conn);
            }
        }

        const principal_z = try alloc.dupeZ(u8, mutation.principal);
        const set_params = [_]?[*:0]const u8{principal_z.ptr};
        const set_sql = "SELECT set_config('" ++ config.Sync.principal_setting ++ "', $1, true)";

        if (c.PQsendQueryParams(conn, set_sql, 1, null, &set_params[0], null, null, 0) != 1 or
            c.PQsendQueryParams(
                conn,
                sql_z.ptr,
                @intCast(params.len),
                null,
                if (params.len > 0) &params[0] else null,
                null,
                null,
                0,
            ) != 1 or
            c.PQsendQueryParams(
                conn,
                (alloc.dupeZ(u8, classify_sql) catch return error.MutationFailed).ptr,
                @intCast(classify_params.len),
                null,
                if (classify_params.len > 0) &classify_params[0] else null,
                null,
                null,
                0,
            ) != 1 or
            c.PQpipelineSync(conn) != 1)
        {
            const msg_ptr = c.PQerrorMessage(conn);
            self.rememberFailure("", if (msg_ptr != null) std.mem.span(msg_ptr) else "");
            log.err("🔴 Could not dispatch the mutation for [{s}]: {s}", .{ mutation.principal, c.PQerrorMessage(conn) });
            return error.MutationFailed;
        }

        // Results arrive in the order the statements were queued, each terminated by a
        // null, with `PGRES_PIPELINE_SYNC` closing the whole exchange. Everything up to
        // that sync must be consumed or the connection cannot be reused — so this drains
        // even when the first statement already failed.
        var stmt_idx: usize = 0;
        var failed = false;
        var affected_buf: [24]u8 = undefined;
        var affected_len: usize = 0;
        var row_exists = false;
        var row_tombstoned = false;

        while (true) {
            const r = c.PQgetResult(conn);
            if (r == null) {
                stmt_idx += 1;
                // Defensive: a connection that dies mid-drain returns null forever, and
                // the loop must not become the way the listener thread stops responding.
                // Three statements plus the sync means an honest run never reaches 5.
                if (stmt_idx > 5) {
                    failed = true;
                    self.rememberFailure("", "connection closed while reading pipeline results");
                    log.err("🔴 Pipeline drained without a sync point for [{s}] — connection lost?", .{mutation.principal});
                    break;
                }
                continue;
            }
            defer c.PQclear(r);

            const st = c.PQresultStatus(r);
            if (st == c.PGRES_PIPELINE_SYNC) {
                // ⚠️ The sync is not the last thing to read. libpq emits a trailing null
                // after it, and `PQexitPipelineMode` **fails while any result is
                // unconsumed** — leaving the connection in pipeline mode. The next
                // mutation then enters a pipeline that is already open, its results
                // misalign with the statements that produced them, and the listener
                // eventually blocks in `PQgetResult` waiting for a result the server has
                // already sent.
                //
                // Measured: PostgreSQL showed the session `idle` in `Client/ClientRead`
                // with the classify statement as its last query — the server finished and
                // was waiting, while the bridge waited for it. Ingress stopped, mutations
                // piled up unacked, and nothing logged an error.
                while (true) {
                    const trailing = c.PQgetResult(conn);
                    if (trailing == null) break;
                    c.PQclear(trailing);
                }
                break;
            }

            // Queued behind a statement that failed, so it never ran. The error was
            // already recorded from the statement that actually failed.
            if (st == c.PGRES_PIPELINE_ABORTED) {
                failed = true;
                continue;
            }

            if (st != c.PGRES_COMMAND_OK and st != c.PGRES_TUPLES_OK) {
                failed = true;
                const state = c.PQresultErrorField(r, c.PG_DIAG_SQLSTATE);
                const sqlstate: []const u8 = if (state != null) std.mem.span(state) else "";
                // 42703 undefined_column / 42P01 undefined_table: the cached layout is
                // stale, so drop it and let the retry re-read the catalog rather than
                // failing until the next restart.
                if (std.mem.eql(u8, sqlstate, "42703") or std.mem.eql(u8, sqlstate, "42P01")) {
                    self.invalidate(mutation.table);
                }
                // ⚠️ `PQresultErrorMessage`, not `PQerrorMessage`: in a pipeline the
                // connection-level message belongs to whichever statement failed last,
                // and the per-result message is the only one guaranteed to describe THIS
                // statement.
                const msg_ptr = c.PQresultErrorMessage(r);
                const msg_text: []const u8 = if (msg_ptr != null) std.mem.span(msg_ptr) else "";
                self.rememberFailure(sqlstate, msg_text);

                // Level by fault, like the caller: a SQLSTATE the server returned and we
                // know is permanent describes the client's row or the operator's policy,
                // and the client is told directly by the verdict — so it is traced, not
                // raised. A transient one means PostgreSQL is unhealthy or unreachable,
                // which nobody but the operator can act on, and which has no other
                // channel.
                //
                // ⚠️ Kept at full detail either way. This is the only place the *whole*
                // message survives: the verdict carries the first line alone, because its
                // DETAIL can quote rows the client never wrote.
                if (sqlstateIsPermanent(sqlstate)) {
                    log.info("⛔ Mutation refused [{s}] on '{s}': {s}", .{ mutation.principal, mutation.table, msg_text });
                } else {
                    log.err("🔴 Mutation failed [{s}] on '{s}': {s}", .{ mutation.principal, mutation.table, msg_text });
                }
                continue;
            }

            // The third statement answers "is the row there, and is it deleted".
            if (stmt_idx == 2) {
                row_exists = c.PQntuples(r) > 0;
                if (row_exists and c.PQgetisnull(r, 0, 0) == 0) {
                    row_tombstoned = std.mem.eql(u8, std.mem.span(c.PQgetvalue(r, 0, 0)), "t");
                }
                continue;
            }

            // The upsert is the second statement, and its tag carries the row count.
            // Copied out now because the result is cleared at the end of this iteration.
            if (stmt_idx == 1) {
                const tuples = std.mem.span(c.PQcmdTuples(r));
                affected_len = @min(tuples.len, affected_buf.len);
                @memcpy(affected_buf[0..affected_len], tuples[0..affected_len]);

                // The version PostgreSQL settled on, from `RETURNING`. Absent when the
                // guard rejected the write, in which case nothing was stored and there is
                // nothing to report.
                if (c.PQntuples(r) > 0 and c.PQgetisnull(r, 0, 0) == 0) {
                    const stored = std.mem.span(c.PQgetvalue(r, 0, 0));
                    self.last_version_len = @min(stored.len, self.last_version_buf.len);
                    @memcpy(self.last_version_buf[0..self.last_version_len], stored[0..self.last_version_len]);

                    // Second column, present only for clampable types: PostgreSQL's own
                    // verdict on whether it changed the client's value.
                    if (c.PQnfields(r) > 1 and c.PQgetisnull(r, 0, 1) == 0) {
                        self.last_clamped = std.mem.eql(u8, std.mem.span(c.PQgetvalue(r, 0, 1)), "t");
                    }
                }
            }
        }

        if (failed) return error.MutationFailed;

        // 0 rows means the guard rejected it: the stored version is newer, so this write
        // lost. Not an error — it is last-write-wins working — but invisible to the
        // client until the reply channel exists.
        // Zero rows is not a failure — it is last-write-wins working — but *which* of the
        // two zero-row outcomes it is decides what the client does with the edit still in
        // its outbox, and only the row's state can say. PROTOCOL.md §7.1.
        const affected = affected_buf[0..affected_len];
        if (!std.mem.eql(u8, affected, "0")) {
            self.last_outcome = .applied;
            log.info("✅ mutation applied [{s}] on '{s}' ({s} row)", .{ mutation.principal, mutation.table, affected });
        } else if (!row_exists or row_tombstoned) {
            self.last_outcome = .row_deleted;
            log.info(
                "🪦 mutation on a deleted row [{s}] on '{s}': {s}",
                .{ mutation.principal, mutation.table, if (row_exists) "tombstoned" else "row is gone" },
            );
        } else {
            self.last_outcome = .stale;
            log.info("↩️  stale mutation [{s}] on '{s}': a newer version is stored", .{ mutation.principal, mutation.table });
        }
    }

    /// A value's text form for a column of `kind` — the typed entry point.
    ///
    /// Scalars are unchanged from before. What is new is that a msgpack ARRAY or MAP is
    /// no longer refused outright: it is rendered the way the target column's input
    /// function will read it.
    ///
    ///   json / jsonb   any value, as JSON            -> {"k":1}   ["a","b"]
    ///   array          a msgpack array, as a literal -> {a,b}     {{1,2}}
    ///   scalar         unchanged; an array or map is still UnsupportedPayloadType,
    ///                  because no scalar column can parse either
    ///
    /// ⚠️ The two literals are NOT interchangeable — `{a,b}` is not valid JSON and
    /// `["a","b"]` is not a valid array literal — which is exactly why this takes the
    /// column kind and cannot be decided from the value alone.
    fn payloadToStringTyped(
        self: *MutationListener,
        alloc: std.mem.Allocator,
        payload: @import("msgpack").Payload,
        kind: ColKind,
    ) !?[*:0]const u8 {
        return switch (kind) {
            .scalar => self.payloadToString(alloc, payload),
            .json => switch (payload) {
                .nil => null,
                else => blk: {
                    var out: std.ArrayList(u8) = .empty;
                    defer out.deinit(alloc);
                    try writeJsonPayload(alloc, &out, payload);
                    break :blk (try out.toOwnedSliceSentinel(alloc, 0)).ptr;
                },
            },
            .array => switch (payload) {
                .nil => null,
                .arr => blk: {
                    var out: std.ArrayList(u8) = .empty;
                    defer out.deinit(alloc);
                    try writeArrayLiteral(alloc, &out, payload);
                    break :blk (try out.toOwnedSliceSentinel(alloc, 0)).ptr;
                },
                // A scalar into an array column: let PostgreSQL judge it rather than
                // inventing a one-element array the client did not ask for.
                else => self.payloadToString(alloc, payload),
            },
        };
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

// ─── array / json literal tests ─────────────────────────────────────────────
//
// The escaping is where this kind of code goes wrong, so the cases that matter are the
// ugly ones: an element containing the delimiters, a quote, a backslash, a NULL.

const mp = @import("msgpack");

fn arrLit(a: std.mem.Allocator, p: mp.Payload) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    try writeArrayLiteral(a, &out, p);
    return out.toOwnedSlice(a);
}

fn jsonLit(a: std.mem.Allocator, p: mp.Payload) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    try writeJsonPayload(a, &out, p);
    return out.toOwnedSlice(a);
}

test "array literal: scalars are quoted, NULL is not" {
    const a = std.testing.allocator;
    var arr = try mp.Payload.arrPayload(3, a);
    defer arr.free(a);
    try arr.setArrElement(0, try mp.Payload.strToPayload("soak", a));
    try arr.setArrElement(1, .{ .int = 42 });
    try arr.setArrElement(2, .{ .nil = {} });
    const got = try arrLit(a, arr);
    defer a.free(got);
    // NULL bare: quoted, it would be the four-character STRING "NULL".
    try std.testing.expectEqualStrings("{\"soak\",\"42\",NULL}", got);
}

test "array literal: an element containing the delimiters survives" {
    const a = std.testing.allocator;
    var arr = try mp.Payload.arrPayload(1, a);
    defer arr.free(a);
    // Unquoted, this would split into three elements and leave an unbalanced brace.
    try arr.setArrElement(0, try mp.Payload.strToPayload("a,b{c}d", a));
    const got = try arrLit(a, arr);
    defer a.free(got);
    try std.testing.expectEqualStrings("{\"a,b{c}d\"}", got);
}

test "array literal: quotes and backslashes are escaped" {
    const a = std.testing.allocator;
    var arr = try mp.Payload.arrPayload(1, a);
    defer arr.free(a);
    try arr.setArrElement(0, try mp.Payload.strToPayload("he said \"hi\" \\ bye", a));
    const got = try arrLit(a, arr);
    defer a.free(got);
    try std.testing.expectEqualStrings("{\"he said \\\"hi\\\" \\\\ bye\"}", got);
}

test "array literal: nesting" {
    const a = std.testing.allocator;
    var inner = try mp.Payload.arrPayload(2, a);
    try inner.setArrElement(0, .{ .int = 1 });
    try inner.setArrElement(1, .{ .int = 2 });
    var outer = try mp.Payload.arrPayload(1, a);
    defer outer.free(a);
    try outer.setArrElement(0, inner);
    const got = try arrLit(a, outer);
    defer a.free(got);
    try std.testing.expectEqualStrings("{{\"1\",\"2\"}}", got);
}

test "json: an object, with the string escapes jsonb needs" {
    const a = std.testing.allocator;
    var m = mp.Payload.mapPayload(a);
    defer m.free(a);
    try m.mapPut("cycle", .{ .int = 355 });
    try m.mapPut("quote", try mp.Payload.strToPayload("say \"hi\"", a));
    const got = try jsonLit(a, m);
    defer a.free(got);
    // Key order follows the map's iteration order, so assert on the pieces.
    try std.testing.expect(std.mem.indexOf(u8, got, "\"cycle\":355") != null);
    try std.testing.expect(std.mem.indexOf(u8, got, "\"quote\":\"say \\\"hi\\\"\"") != null);
    try std.testing.expect(got[0] == '{' and got[got.len - 1] == '}');
}

test "json: an array is a JSON array, NOT a PG array literal" {
    const a = std.testing.allocator;
    var arr = try mp.Payload.arrPayload(2, a);
    defer arr.free(a);
    try arr.setArrElement(0, try mp.Payload.strToPayload("a", a));
    try arr.setArrElement(1, .{ .int = 2 });
    const got = try jsonLit(a, arr);
    defer a.free(got);
    // The whole reason ColKind exists: the same value, two incompatible spellings.
    try std.testing.expectEqualStrings("[\"a\",2]", got);
    const as_array = try arrLit(a, arr);
    defer a.free(as_array);
    try std.testing.expectEqualStrings("{\"a\",\"2\"}", as_array);
}

