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

/// The tables the ENV named (SYNC_RULES / TENANT_RULES) — snapshotted before the
/// first catalogue read. An env entry overrides the catalogue for its table, at boot
/// and on every reload; the catalogue owns every other table's entry outright.
pub const KeySet = std.StringHashMap(void);
pub const Env = struct {
    tenant_keys: *const KeySet,
    sync_keys: *const KeySet,
};

/// What one read of the catalogue produced. The slices are allocated from the
/// caller's allocator; the caller owns them until the next read replaces them (the
/// bridge re-reads on every `zebridge_catalogue` row the WAL carries — §10bj) and frees
/// them via `deinit` — an early exit (a refused NATS auth, say) must leave a clean
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
    /// Tables whose rules were added, replaced or removed by THIS read — what a live
    /// reload must (re)publish a schema for or lift a refusal on. Empty at boot.
    changed: []const []const u8 = &.{},

    pub fn deinit(self: *Load, allocator: std.mem.Allocator) void {
        for (self.publics) |name| allocator.free(name);
        if (self.publics.len > 0) allocator.free(self.publics);
        for (self.tenants) |name| allocator.free(name);
        if (self.tenants.len > 0) allocator.free(self.tenants);
        for (self.changed) |name| allocator.free(name);
        if (self.changed.len > 0) allocator.free(self.changed);
        self.* = .{};
    }
};

fn freeCols(allocator: std.mem.Allocator, cols: []const []const u8) void {
    for (cols) |col| allocator.free(col);
    allocator.free(cols);
}

/// Put `cols` under `tbl` unless the env owns that table. Returns true when the map
/// changed (added, or replaced with a different value). A replaced value is freed
/// here; the key is the map's and stays.
fn upsertRule(
    allocator: std.mem.Allocator,
    map: *config.EventClassification.TransitionRules,
    env_keys: *const KeySet,
    tbl: []const u8,
    cols: []const []const u8,
) bool {
    if (env_keys.contains(tbl)) {
        freeCols(allocator, cols);
        return false;
    }
    if (map.getPtr(tbl)) |vp| {
        var same = vp.*.len == cols.len;
        if (same) for (vp.*, cols) |x, y| {
            if (!std.mem.eql(u8, x, y)) same = false;
        };
        if (same) {
            freeCols(allocator, cols);
            return false;
        }
        freeCols(allocator, vp.*);
        vp.* = cols;
        return true;
    }
    const key = allocator.dupe(u8, tbl) catch {
        freeCols(allocator, cols);
        return false;
    };
    map.put(key, cols) catch {
        allocator.free(key);
        freeCols(allocator, cols);
        return false;
    };
    return true;
}

/// Drop every catalogue-owned entry whose table the catalogue no longer lists.
fn pruneRules(
    allocator: std.mem.Allocator,
    map: *config.EventClassification.TransitionRules,
    env_keys: *const KeySet,
    seen: *const KeySet,
    changed: *std.ArrayList([]const u8),
) void {
    var gone: std.ArrayList([]const u8) = .empty;
    defer gone.deinit(allocator);
    var it = map.iterator();
    while (it.next()) |e| {
        if (env_keys.contains(e.key_ptr.*) or seen.contains(e.key_ptr.*)) continue;
        gone.append(allocator, e.key_ptr.*) catch continue;
    }
    for (gone.items) |k| {
        const kv = map.fetchRemove(k) orelse continue;
        // Once per table, not once per map it was pruned from: a removed table
        // appeared twice here (tenant + sync) and had its suspension published twice.
        var listed = false;
        for (changed.items) |cname| {
            if (std.mem.eql(u8, cname, kv.key)) listed = true;
        }
        if (!listed) {
            changed.append(allocator, allocator.dupe(u8, kv.key) catch {
                allocator.free(kv.key);
                freeCols(allocator, kv.value);
                continue;
            }) catch {};
        }
        allocator.free(kv.key);
        freeCols(allocator, kv.value);
    }
}

/// Read the catalogue into the rule maps (env wins per table; the catalogue ADDS,
/// REPLACES and REMOVES everything else) and collect the scope lists. Never fails:
/// every degraded shape logs and returns what it could get. Called at boot and again
/// on every `zebridge_catalogue` row the WAL delivers (§10bj), from the same thread
/// that reads the maps for CDC — the mutation listener never reads them (it has the
/// env map and the catalogue itself).
pub fn loadRules(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    tenant_rules: *config.EventClassification.TransitionRules,
    sync_rules: *config.EventClassification.TransitionRules,
    env: Env,
) Load {
    var out: Load = .{};
    var changed: std.ArrayList([]const u8) = .empty;
    var seen: KeySet = .init(allocator);
    defer seen.deinit();

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

        seen.put(tbl, {}) catch {};
        if (tenant_col.len > 0) {
            var cols = allocator.alloc([]const u8, 1) catch continue;
            cols[0] = allocator.dupe(u8, tenant_col) catch {
                allocator.free(cols);
                continue;
            };
            if (upsertRule(allocator, tenant_rules, env.tenant_keys, tbl, cols)) grew = true;
        } else if (!env.tenant_keys.contains(tbl)) {
            // Public now: a tenant rule the catalogue used to carry goes.
            if (tenant_rules.fetchRemove(tbl)) |kv| {
                allocator.free(kv.key);
                freeCols(allocator, kv.value);
                grew = true;
            }
        }
        {
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
            const cols = list.toOwnedSlice(allocator) catch continue;
            if (upsertRule(allocator, sync_rules, env.sync_keys, tbl, cols)) grew = true;
        }
        if (grew) {
            out.merged += 1;
            changed.append(allocator, allocator.dupe(u8, tbl) catch continue) catch {};
        }
    }
    out.publics = publics.toOwnedSlice(allocator) catch &.{};
    // Tables gone from the catalogue: their catalogue-owned rules go with them.
    pruneRules(allocator, tenant_rules, env.tenant_keys, &seen, &changed);
    pruneRules(allocator, sync_rules, env.sync_keys, &seen, &changed);
    out.changed = changed.toOwnedSlice(allocator) catch &.{};

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

    log.info("🗂️ catalogue: {d} row(s), {d} table(s) gained or changed rules (env entries override), {d} public, {d} tenant(s), {d} changed", .{
        out.rows, out.merged, out.publics.len, out.tenants.len, out.changed.len,
    });
    return out;
}
