//! PostgreSQL as the bridge sees it, for /metrics (NOTES §10ji): the data the bridge OWNS
//! (its chain, catalogue, tenants, invites) and the server's HEALTH as it bears on the
//! bridge (connections per role against their limits, the oldest open transaction, the
//! size and dead rows of every published table).
//!
//! One pass = one connection as the READER role, seven independent queries; one that fails
//! (a grant missing, a table not yet created) leaves ITS section out and the rest stand.
//! Snapshot-swapped under a lock like the slot inventory: the monitor thread builds a fresh
//! arena, the HTTP thread renders whatever snapshot is current.
const std = @import("std");
const c = @import("c_imports.zig").c;
const utils = @import("utils.zig");
const pg_conn = @import("pg_conn.zig");

pub const log = std.log.scoped(.pg_health);

pub const Snapshot = struct {
    pub const Chain = struct { tenant: []const u8, table: []const u8, gen: i64, built_unix: i64, live: i64, rows: i64 };
    pub const Role = struct { role: []const u8, connections: i64, limit: i64 };
    pub const TenantCount = struct { tenant: []const u8, principals: i64 };
    pub const Table = struct { table: []const u8, bytes: i64, live: i64, dead: i64 };

    chain: ?[]const Chain = null,
    catalogue: ?struct { tables: i64, tenant_scoped: i64, public: i64, with_generations: i64 } = null,
    tenants: ?struct { principals: i64, tenants: i64, per_tenant: []const TenantCount } = null,
    invites: ?struct { pending: i64, used: i64, expired: i64 } = null,
    roles: ?[]const Role = null,
    cluster: ?struct { client_connections: i64, max_connections: i64 } = null,
    /// Seconds since the oldest open transaction of another session began; 0 when none.
    oldest_xact_age_s: ?i64 = null,
    tables: ?[]const Table = null,
};

pub const HealthRegistry = struct {
    allocator: std.mem.Allocator,
    mutex: utils.SpinLock = .{},
    arena: ?*std.heap.ArenaAllocator = null,
    snap: Snapshot = .{},
    polled_at_unix: i64 = 0,

    pub fn init(allocator: std.mem.Allocator) HealthRegistry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *HealthRegistry) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.arena) |ar| {
            ar.deinit();
            self.allocator.destroy(ar);
            self.arena = null;
            self.snap = .{};
        }
    }

    fn swap(self: *HealthRegistry, arena: *std.heap.ArenaAllocator, snap: Snapshot, now_unix: i64) void {
        self.mutex.lock();
        const old = self.arena;
        self.arena = arena;
        self.snap = snap;
        self.polled_at_unix = now_unix;
        self.mutex.unlock();
        if (old) |o| {
            o.deinit();
            self.allocator.destroy(o);
        }
    }

    /// ⚠️ Integers only, as everywhere in /metrics: one unparseable line drops the scrape.
    /// Label values are identifiers the catalogue already validated (table, tenant, role
    /// names); a quote or backslash in one is escaped anyway.
    pub fn writePrometheus(self: *HealthRegistry, w: *std.Io.Writer) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const s = self.snap;
        try w.print("# HELP bridge_pg_health_timestamp_seconds Unix time of the last PostgreSQL health pass (0 = none yet)\n# TYPE bridge_pg_health_timestamp_seconds gauge\nbridge_pg_health_timestamp_seconds {d}\n", .{self.polled_at_unix});

        if (s.chain) |rows| {
            try w.print("# HELP bridge_chain_generation Latest live generation number of a table's chain, per tenant\n# TYPE bridge_chain_generation gauge\n", .{});
            for (rows) |r| try w.print("bridge_chain_generation{{tenant=\"{f}\",table=\"{f}\"}} {d}\n", .{ esc(r.tenant), esc(r.table), r.gen });
            try w.print("# HELP bridge_chain_last_cut_timestamp_seconds When the latest live generation was built (compare with time() for its age)\n# TYPE bridge_chain_last_cut_timestamp_seconds gauge\n", .{});
            for (rows) |r| try w.print("bridge_chain_last_cut_timestamp_seconds{{tenant=\"{f}\",table=\"{f}\"}} {d}\n", .{ esc(r.tenant), esc(r.table), r.built_unix });
            try w.print("# HELP bridge_chain_live_generations Generations not retired — what a returning client can walk\n# TYPE bridge_chain_live_generations gauge\n", .{});
            for (rows) |r| try w.print("bridge_chain_live_generations{{tenant=\"{f}\",table=\"{f}\"}} {d}\n", .{ esc(r.tenant), esc(r.table), r.live });
            try w.print("# HELP bridge_chain_rows Row count recorded by the latest live generation\n# TYPE bridge_chain_rows gauge\n", .{});
            for (rows) |r| try w.print("bridge_chain_rows{{tenant=\"{f}\",table=\"{f}\"}} {d}\n", .{ esc(r.tenant), esc(r.table), r.rows });
        }
        if (s.catalogue) |k| {
            try w.print("# HELP bridge_catalogue_tables Tables in zebridge_catalogue, by kind\n# TYPE bridge_catalogue_tables gauge\n", .{});
            try w.print("bridge_catalogue_tables{{kind=\"all\"}} {d}\nbridge_catalogue_tables{{kind=\"tenant_scoped\"}} {d}\nbridge_catalogue_tables{{kind=\"public\"}} {d}\nbridge_catalogue_tables{{kind=\"with_generations\"}} {d}\n", .{ k.tables, k.tenant_scoped, k.public, k.with_generations });
        }
        if (s.tenants) |t| {
            try w.print("# HELP bridge_principals Principals with at least one tenant mapping (zebridge_user_tenants)\n# TYPE bridge_principals gauge\nbridge_principals {d}\n", .{t.principals});
            try w.print("# HELP bridge_tenants Tenants with at least one principal\n# TYPE bridge_tenants gauge\nbridge_tenants {d}\n", .{t.tenants});
            try w.print("# HELP bridge_tenant_principals Principals mapped to each tenant\n# TYPE bridge_tenant_principals gauge\n", .{});
            for (t.per_tenant) |r| try w.print("bridge_tenant_principals{{tenant=\"{f}\"}} {d}\n", .{ esc(r.tenant), r.principals });
        }
        if (s.invites) |i| {
            try w.print("# HELP bridge_invites Enrollment invites, by state\n# TYPE bridge_invites gauge\n", .{});
            try w.print("bridge_invites{{state=\"pending\"}} {d}\nbridge_invites{{state=\"used\"}} {d}\nbridge_invites{{state=\"expired\"}} {d}\n", .{ i.pending, i.used, i.expired });
        }
        if (s.roles) |rows| {
            try w.print("# HELP bridge_pg_role_connections Open PostgreSQL connections of each bridge role\n# TYPE bridge_pg_role_connections gauge\n", .{});
            for (rows) |r| try w.print("bridge_pg_role_connections{{role=\"{f}\"}} {d}\n", .{ esc(r.role), r.connections });
            try w.print("# HELP bridge_pg_role_connection_limit Each bridge role's CONNECTION LIMIT (-1 = unlimited)\n# TYPE bridge_pg_role_connection_limit gauge\n", .{});
            for (rows) |r| try w.print("bridge_pg_role_connection_limit{{role=\"{f}\"}} {d}\n", .{ esc(r.role), r.limit });
        }
        if (s.cluster) |k| {
            try w.print("# HELP bridge_pg_client_connections Client sessions on the whole PostgreSQL server (close estimate: other roles' session types are hidden from the reader)\n# TYPE bridge_pg_client_connections gauge\nbridge_pg_client_connections {d}\n", .{k.client_connections});
            try w.print("# HELP bridge_pg_max_connections The server's max_connections\n# TYPE bridge_pg_max_connections gauge\nbridge_pg_max_connections {d}\n", .{k.max_connections});
        }
        if (s.oldest_xact_age_s) |age| {
            try w.print("# HELP bridge_pg_oldest_xact_age_seconds Age of the oldest open transaction of another session (0 = none) — a long one holds the slot and vacuum back\n# TYPE bridge_pg_oldest_xact_age_seconds gauge\nbridge_pg_oldest_xact_age_seconds {d}\n", .{age});
        }
        if (s.tables) |rows| {
            try w.print("# HELP bridge_pg_table_bytes Total size of each published table, indexes and TOAST included\n# TYPE bridge_pg_table_bytes gauge\n", .{});
            for (rows) |r| try w.print("bridge_pg_table_bytes{{table=\"{f}\"}} {d}\n", .{ esc(r.table), r.bytes });
            try w.print("# HELP bridge_pg_table_live_rows Live rows of each published table (the planner's estimate)\n# TYPE bridge_pg_table_live_rows gauge\n", .{});
            for (rows) |r| try w.print("bridge_pg_table_live_rows{{table=\"{f}\"}} {d}\n", .{ esc(r.table), r.live });
            try w.print("# HELP bridge_pg_table_dead_rows Dead rows awaiting vacuum in each published table\n# TYPE bridge_pg_table_dead_rows gauge\n", .{});
            for (rows) |r| try w.print("bridge_pg_table_dead_rows{{table=\"{f}\"}} {d}\n", .{ esc(r.table), r.dead });
        }
    }
};

/// A Prometheus label value, escaped as the text format requires.
const Esc = struct {
    s: []const u8,
    pub fn format(self: Esc, w: *std.Io.Writer) std.Io.Writer.Error!void {
        for (self.s) |ch| switch (ch) {
            '\\' => try w.writeAll("\\\\"),
            '"' => try w.writeAll("\\\""),
            '\n' => try w.writeAll("\\n"),
            else => try w.writeByte(ch),
        };
    }
};
fn esc(s: []const u8) Esc {
    return .{ .s = s };
}

/// One pass: every section it can read, swapped in whole.
pub fn poll(registry: *HealthRegistry, pg_config: *const pg_conn.PgConf, allocator: std.mem.Allocator, writer_role: ?[]const u8, publication: []const u8) !void {
    const conninfo = try pg_config.connInfo(allocator, false);
    defer allocator.free(conninfo);
    const conn = c.PQconnectdb(conninfo.ptr) orelse return error.ConnectionFailed;
    defer c.PQfinish(conn);
    if (c.PQstatus(conn) != c.CONNECTION_OK) return error.ConnectionFailed;

    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer {
        arena.deinit();
        allocator.destroy(arena);
    }
    const a = arena.allocator();
    var snap: Snapshot = .{};

    // The chain: per tenant and table, the latest live generation and what it recorded.
    if (query(conn, a,
        \\SELECT tenant, tbl, max(gen)::text, coalesce(extract(epoch FROM max(built_at))::bigint, 0)::text,
        \\       count(*)::text, coalesce((array_agg(row_count ORDER BY gen DESC))[1], 0)::text
        \\FROM public.zebridge_generations WHERE retired_at IS NULL GROUP BY tenant, tbl ORDER BY tenant, tbl
    , &.{})) |rows| {
        const out = try a.alloc(Snapshot.Chain, rows.len);
        for (rows, 0..) |r, i| out[i] = .{ .tenant = r[0], .table = r[1], .gen = num(r[2]), .built_unix = num(r[3]), .live = num(r[4]), .rows = num(r[5]) };
        snap.chain = out;
    } else |_| {}

    if (query(conn, a,
        \\SELECT count(*)::text, count(tenant_col)::text, count(public_reason)::text, count(*) FILTER (WHERE generations)::text
        \\FROM public.zebridge_catalogue
    , &.{})) |rows| {
        if (rows.len == 1) snap.catalogue = .{ .tables = num(rows[0][0]), .tenant_scoped = num(rows[0][1]), .public = num(rows[0][2]), .with_generations = num(rows[0][3]) };
    } else |_| {}

    if (query(conn, a, "SELECT tenant_id, count(DISTINCT principal)::text FROM public.zebridge_user_tenants GROUP BY tenant_id ORDER BY tenant_id", &.{})) |rows| {
        if (query(conn, a, "SELECT count(DISTINCT principal)::text, count(DISTINCT tenant_id)::text FROM public.zebridge_user_tenants", &.{})) |tot| {
            const per = try a.alloc(Snapshot.TenantCount, rows.len);
            for (rows, 0..) |r, i| per[i] = .{ .tenant = r[0], .principals = num(r[1]) };
            if (tot.len == 1) snap.tenants = .{ .principals = num(tot[0][0]), .tenants = num(tot[0][1]), .per_tenant = per };
        } else |_| {}
    } else |_| {}

    if (query(conn, a,
        \\SELECT count(*) FILTER (WHERE used_at IS NULL AND expires_at > now())::text,
        \\       count(*) FILTER (WHERE used_at IS NOT NULL)::text,
        \\       count(*) FILTER (WHERE used_at IS NULL AND expires_at <= now())::text
        \\FROM public.zebridge_invites
    , &.{})) |rows| {
        if (rows.len == 1) snap.invites = .{ .pending = num(rows[0][0]), .used = num(rows[0][1]), .expired = num(rows[0][2]) };
    } else |_| {}

    // The bridge's two roles: this connection's (the reader) and the writer, if ingress is on.
    const wr = writer_role orelse "";
    if (query(conn, a,
        \\SELECT r.rolname, (SELECT count(*) FROM pg_stat_activity s WHERE s.usename = r.rolname)::text, r.rolconnlimit::text
        \\FROM pg_roles r WHERE r.rolname = current_user OR r.rolname = $1 ORDER BY r.rolname
    , &.{wr})) |rows| {
        const out = try a.alloc(Snapshot.Role, rows.len);
        for (rows, 0..) |r, i| out[i] = .{ .role = r[0], .connections = num(r[1]), .limit = num(r[2]) };
        snap.roles = out;
    } else |_| {}

    // Other roles' sessions show their user but hide their backend_type from the reader
    // (no pg_read_all_stats, on purpose): count sessions with a user whose type is either
    // `client backend` or hidden. Close, not exact — a hidden walsender of another role
    // counts as a client.
    if (query(conn, a, "SELECT count(*) FILTER (WHERE usename IS NOT NULL AND coalesce(backend_type, 'client backend') = 'client backend')::text, current_setting('max_connections') FROM pg_stat_activity", &.{})) |rows| {
        if (rows.len == 1) snap.cluster = .{ .client_connections = num(rows[0][0]), .max_connections = num(rows[0][1]) };
    } else |_| {}

    // zebridge_oldest_open_xact(): SECURITY DEFINER, so the reader sees every session's
    // transaction start without pg_read_all_stats — and nothing else of them.
    if (query(conn, a, "SELECT coalesce(extract(epoch FROM now() - public.zebridge_oldest_open_xact())::bigint, 0)::text", &.{})) |rows| {
        if (rows.len == 1) snap.oldest_xact_age_s = num(rows[0][0]);
    } else |_| {}

    if (query(conn, a,
        \\SELECT c.relname, pg_total_relation_size(c.oid)::text, coalesce(s.n_live_tup, 0)::text, coalesce(s.n_dead_tup, 0)::text
        \\FROM pg_publication_tables pt
        \\JOIN pg_namespace n ON n.nspname = pt.schemaname
        \\JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
        \\LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        \\WHERE pt.pubname = $1 ORDER BY c.relname
    , &.{publication})) |rows| {
        const out = try a.alloc(Snapshot.Table, rows.len);
        for (rows, 0..) |r, i| out[i] = .{ .table = r[0], .bytes = num(r[1]), .live = num(r[2]), .dead = num(r[3]) };
        snap.tables = out;
    } else |_| {}

    registry.swap(arena, snap, @divFloor(utils.unixMillis(), 1000));
}

fn num(s: []const u8) i64 {
    return std.fmt.parseInt(i64, s, 10) catch 0;
}

/// Rows of text columns, copied into `a`. A failed query logs at debug and is an error the
/// caller turns into "this section is missing".
fn query(conn: *c.PGconn, a: std.mem.Allocator, sql: [:0]const u8, params: []const []const u8) ![]const []const []const u8 {
    var pz: [4]?[*:0]const u8 = .{ null, null, null, null };
    for (params, 0..) |p, i| pz[i] = (try a.dupeSentinel(u8, p, 0)).ptr;
    const res = c.PQexecParams(conn, sql.ptr, @intCast(params.len), null, if (params.len > 0) &pz[0] else null, null, null, 0);
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        log.debug("pg health query skipped: {s}", .{c.PQerrorMessage(conn)});
        return error.QueryFailed;
    }
    const n: usize = @intCast(c.PQntuples(res));
    const m: usize = @intCast(c.PQnfields(res));
    const rows = try a.alloc([]const []const u8, n);
    for (0..n) |i| {
        const row = try a.alloc([]const u8, m);
        for (0..m) |j| row[j] = try a.dupe(u8, std.mem.span(c.PQgetvalue(res, @intCast(i), @intCast(j))));
        rows[i] = row;
    }
    return rows;
}
