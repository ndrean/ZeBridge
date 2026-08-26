//! The catalogue loader — `zebridge_catalogue` into the rule maps at boot.
//!
//! One row per replicated table, written by `zebridge_enable` atomically with the
//! guards (the static-residue endgame): `tenant_col NULL` = public, NOT NULL =
//! tenant-scoped; version/tombstone/tiebreak are the LWW columns. Loading it here
//! ends the T3 transcription era — SYNC_RULES/TENANT_RULES env become OVERRIDES
//! (an env entry for a table wins; the catalogue fills every table the env does
//! not name). A database predating the catalogue loads zero rows and the bridge
//! behaves exactly as before — graceful by construction, not by flag.
//!
//! Beyond the rule maps, the same read yields the two boot-scope lists the grammar
//! file used to carry:
//!   publics  — every catalogue row with `tenant_col IS NULL`: the set of tables
//!              `CDC_PUBLIC`'s subject filter must cover (the bridge reconciles the
//!              stream itself at boot; nats-init no longer builds this list).
//!   tenants  — `SELECT DISTINCT tenant_id FROM zebridge_user_tenants`: the tenants
//!              whose `CDC_<T>`/`INIT_<T>` streams the boot ensures exist. Tenants
//!              are DATA; a tenant born after boot gets its streams from the backend
//!              that onboards it (dyntenant contract) — this list only covers what is
//!              already known when the bridge starts.

const std = @import("std");
const config = @import("config.zig");
const pg_conn = @import("pg_conn.zig");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;

const log = std.log.scoped(.catalogue);

/// What one boot-time read of the catalogue produced. The slices are allocated from
/// the caller's allocator; the caller owns them for the life of the process (the
/// bridge reads the catalogue exactly once, at boot) and frees them via `deinit` on
/// the way out — an early exit (a refused NATS auth, say) must leave a clean
/// allocator audit, not a "lives forever" euphemism. Entries merged into the rule
/// MAPS are owned by the maps and freed by their own deinits, not here.
pub const Load = struct {
    /// Whether the catalogue was reachable at all. False = pre-catalogue database or
    /// connect failure; the caller keeps whatever the grammar file / env provided.
    available: bool = false,
    /// Rows in the catalogue.
    rows: usize = 0,
    /// Tables that gained rules here (env entries override, so an env-named table
    /// counts only if the catalogue added something the env did not).
    merged: usize = 0,
    /// Tables with `tenant_col IS NULL` — what CDC_PUBLIC must route.
    publics: []const []const u8 = &.{},
    /// DISTINCT tenants from `zebridge_user_tenants` — whose streams boot ensures.
    tenants: []const []const u8 = &.{},

    pub fn deinit(self: *Load, allocator: std.mem.Allocator) void {
        for (self.publics) |name| allocator.free(name);
        if (self.publics.len > 0) allocator.free(self.publics);
        for (self.tenants) |name| allocator.free(name);
        if (self.tenants.len > 0) allocator.free(self.tenants);
        self.* = .{};
    }
};

/// Merge catalogue rows into the already-parsed env maps (env wins per table) and
/// collect the boot-scope lists. Never fails: every degraded shape logs and returns
/// what it could get.
pub fn loadRules(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    tenant_rules: *config.EventClassification.TransitionRules,
    sync_rules: *config.EventClassification.TransitionRules,
) Load {
    var out: Load = .{};

    const conninfo = pg_config.connInfo(allocator, false) catch return out;
    defer allocator.free(conninfo);
    const conn = c.PQconnectdb(conninfo.ptr) orelse return out;
    defer c.PQfinish(conn);
    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        log.warn("🗂️ catalogue: PG connect failed at boot ({s}) — env rules only", .{c.PQerrorMessage(conn)});
        return out;
    }

    const res = c.PQexec(conn,
        "SELECT tbl, COALESCE(tenant_col::text, ''), version_col::text, " ++
            "COALESCE(tombstone_col::text, ''), COALESCE(tiebreak_col::text, '') " ++
            "FROM public.zebridge_catalogue ORDER BY tbl");
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        // Missing table = a pre-catalogue database. Not an error; say so once.
        log.info("🗂️ no zebridge_catalogue (pre-catalogue database?) — env rules only", .{});
        return out;
    }
    out.available = true;

    var publics: std.ArrayList([]const u8) = .empty;

    const n: usize = @intCast(c.PQntuples(res));
    out.rows = n;
    for (0..n) |i| {
        const tbl = std.mem.span(c.PQgetvalue(res, @intCast(i), 0));
        const tenant_col = std.mem.span(c.PQgetvalue(res, @intCast(i), 1));
        const version_col = std.mem.span(c.PQgetvalue(res, @intCast(i), 2));
        const tombstone_col = std.mem.span(c.PQgetvalue(res, @intCast(i), 3));
        const tiebreak_col = std.mem.span(c.PQgetvalue(res, @intCast(i), 4));
        var grew = false;

        if (tenant_col.len == 0) {
            // Public: what CDC_PUBLIC's subject filter must cover. A stale row (its
            // table since dropped) costs one dead subject in the filter — harmless.
            const name = allocator.dupe(u8, tbl) catch continue;
            publics.append(allocator, name) catch continue;
        }

        if (tenant_col.len > 0 and tenant_rules.get(tbl) == null) {
            const key = allocator.dupe(u8, tbl) catch continue;
            var cols = allocator.alloc([]const u8, 1) catch {
                allocator.free(key);
                continue;
            };
            cols[0] = allocator.dupe(u8, tenant_col) catch {
                allocator.free(cols);
                allocator.free(key);
                continue;
            };
            tenant_rules.put(key, cols) catch {
                allocator.free(cols[0]);
                allocator.free(cols);
                allocator.free(key);
                continue;
            };
            grew = true;
        }
        if (sync_rules.get(tbl) == null) {
            // POSITIONAL grammar mirrored from parseTableRules: version, tombstone,
            // tiebreak — and position IS the meaning, so a tiebreak with no tombstone
            // keeps an empty placeholder in slot 1. Sliding it forward made the
            // tiebreak the tombstone (found live on counter_public: last_writer as
            // tombstone, ties never stamped). Consumers treat "" as "not configured".
            var list: std.ArrayList([]const u8) = .empty;
            list.append(allocator, allocator.dupe(u8, version_col) catch continue) catch continue;
            if (tombstone_col.len > 0 or tiebreak_col.len > 0)
                list.append(allocator, allocator.dupe(u8, tombstone_col) catch continue) catch continue;
            if (tiebreak_col.len > 0)
                list.append(allocator, allocator.dupe(u8, tiebreak_col) catch continue) catch continue;
            const key = allocator.dupe(u8, tbl) catch continue;
            sync_rules.put(key, list.toOwnedSlice(allocator) catch continue) catch continue;
            grew = true;
        }
        if (grew) out.merged += 1;
    }
    out.publics = publics.toOwnedSlice(allocator) catch &.{};

    // Tenants are data, not config: whoever is mapped today is whose streams boot
    // must ensure. RLS does not hide this from the bridge role (the boot KV backfill
    // reads the same table on the same kind of connection).
    var tenants: std.ArrayList([]const u8) = .empty;
    const tres = c.PQexec(conn,
        "SELECT DISTINCT tenant_id::text FROM public.zebridge_user_tenants ORDER BY 1");
    defer c.PQclear(tres);
    if (c.PQresultStatus(tres) == c.PGRES_TUPLES_OK) {
        const tn: usize = @intCast(c.PQntuples(tres));
        for (0..tn) |i| {
            const t = std.mem.span(c.PQgetvalue(tres, @intCast(i), 0));
            if (t.len == 0) continue;
            const name = allocator.dupe(u8, t) catch continue;
            tenants.append(allocator, name) catch continue;
        }
    } else {
        log.warn("🗂️ catalogue: could not read zebridge_user_tenants for the tenant list ({s})", .{c.PQerrorMessage(conn)});
    }
    out.tenants = tenants.toOwnedSlice(allocator) catch &.{};

    log.info("🗂️ catalogue: {d} row(s), {d} table(s) gained rules (env entries override), {d} public, {d} tenant(s)", .{
        out.rows, out.merged, out.publics.len, out.tenants.len,
    });
    return out;
}
