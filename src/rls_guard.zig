//! Row-level-security visibility guard for snapshot builds.
//!
//! A chain object (full, delta, checkpoint) is a plain `SELECT` run by the reader role
//! with `zb.tenant` set, so row-level security decides which rows the object holds. When
//! a policy hides rows the object is still well-formed: a full that is silently short
//! reads exactly like a full for a table that is genuinely small or empty, and every
//! client that seeds from it wipes its replica down to what the reader could see. CDC is
//! RLS-blind (logical decoding has no query), so the live feed keeps carrying the rows
//! the snapshot dropped and the two disagree until someone notices.
//!
//! This module answers one question before a build publishes: *can the reader's policy
//! configuration for this table hide a row the bridge means to ship?* Three answers:
//!
//!   safe          the effective configuration is one the bridge understands and none
//!                 of its rows can be hidden by it;
//!   hidden        the configuration provably hides rows (no policy applies, or an
//!                 applicable one is `USING (false)`);
//!   unverifiable  something applies that this module does not recognise. It is reported
//!                 as such and never certified safe: a restrictive policy is combined
//!                 with AND and can hide any subset of rows.
//!
//! ## What is, and is not, a guarantee
//!
//! The *classification* is the guarantee. It reads the catalog (`pg_policy`, `pg_class`,
//! the reader's role flags) and accepts only configurations it can prove complete:
//!
//!   * row security off, the reader a superuser / BYPASSRLS, or the reader the table's
//!     owner without FORCE ROW LEVEL SECURITY;
//!   * otherwise: every applicable restrictive policy is a no-op (`USING (true)`), and at
//!     least one applicable permissive policy is `USING (true)` or the bridge's own
//!     tenant predicate for this table's tenant column and the open tenant.
//!
//! "Applicable" follows PostgreSQL: the policy covers SELECT (`r` or `*`) and names
//! PUBLIC or a role the reader is a member of. Permissive policies are OR-ed, restrictive
//! ones AND-ed, so extra permissive policies can only add rows and are not a completeness
//! risk; extra restrictive ones can remove any. A policy without a USING clause adds
//! nothing and is skipped, as PostgreSQL does.
//!
//! The row-count comparison (`countMismatch`) is a second, weaker detector and says so:
//! equal counts prove nothing, because a restrictive policy hides the same rows whether
//! `zb.tenant` is set or cleared. It catches policies that interact with the setting.
//!
//! The policy configuration can change after startup, so callers check inside every
//! build's snapshot, after taking the table's lock, and compare `Check.fingerprint` again
//! with a fresh statement before publishing (`recheck`). On PostgreSQL 16 `ALTER POLICY`
//! takes ACCESS EXCLUSIVE on the table, so it waits for a build that holds the table and
//! cannot change what that build reads; what remains is the instant between the snapshot's
//! first statement (the catalog is read as of it) and the table lock, and any server that
//! takes a weaker lock. The recheck closes both: a fresh read that no longer matches the
//! verdict's fingerprint discards what the build produced.

const std = @import("std");
const c = @import("c_imports.zig").c;
const config = @import("config.zig");
const utils = @import("utils.zig");

const log = std.log.scoped(.rls_guard);

pub const Verdict = enum {
    safe,
    hidden,
    unverifiable,

    pub fn wireName(self: Verdict) []const u8 {
        return @tagName(self);
    }
};

/// One policy that applies to the reader for SELECT.
pub const Policy = struct {
    name: []const u8,
    permissive: bool,
    /// `pg_get_expr(polqual)`.
    qual: []const u8,
};

/// What the catalog says about one table and the connected reader.
pub const Facts = struct {
    exists: bool = true,
    rls_enabled: bool = false,
    force_rls: bool = false,
    /// The reader is a superuser or has BYPASSRLS.
    role_bypass: bool = false,
    /// The reader holds the table's owner privileges (PostgreSQL's own owner test).
    is_owner: bool = false,
    /// Applicable to the reader for SELECT, with a USING clause, by name.
    policies: []const Policy = &.{},
};

/// The reason a recheck gives when the configuration moved but is still one the guard accepts.
pub const changed_reason = "the read policies changed while this snapshot was being built, so what it read cannot be vouched for; nothing it produced is published";

pub const Check = struct {
    verdict: Verdict,
    /// Why, for the operator: names the policy that decided it. Owned by the caller's allocator.
    reason: []const u8,
    /// Hash of every fact the verdict rests on. Equal before and after a build means the
    /// configuration did not move under it.
    fingerprint: u64,
};

// ── normalisation ─────────────────────────────────────────────────────────────────────

fn startsWithIgnoreCase(hay: []const u8, needle: []const u8) bool {
    return hay.len >= needle.len and std.ascii.eqlIgnoreCase(hay[0..needle.len], needle);
}

fn isIdentChar(ch: u8) bool {
    return std.ascii.isAlphanumeric(ch) or ch == '_';
}

/// A policy expression reduced to a form that survives PostgreSQL's pretty-printing:
/// keywords and unquoted names lowercased, no whitespace or parentheses, no `::text`
/// casts, no `AS current_setting` aliases. String literals and quoted identifiers are
/// kept EXACTLY, quotes included (a policy naming open tenant `_DEFAULT` is not one naming
/// `_default`, and a column named "true" is not the keyword). Two expressions that
/// normalise equally are the same expression for this module's fixed vocabulary; anything
/// else is "not recognised", which is the safe direction.
pub fn normalize(alloc: std.mem.Allocator, expr: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    while (i < expr.len) {
        const ch = expr[i];
        switch (ch) {
            '\'' => {
                // A literal, verbatim, `''` being an escaped quote inside it.
                try out.append(alloc, '\'');
                i += 1;
                while (i < expr.len) : (i += 1) {
                    try out.append(alloc, expr[i]);
                    if (expr[i] == '\'') {
                        if (i + 1 < expr.len and expr[i + 1] == '\'') {
                            i += 1;
                            try out.append(alloc, '\'');
                        } else {
                            i += 1;
                            break;
                        }
                    }
                }
            },
            '"' => {
                // A quoted identifier, verbatim WITH its quotes, `""` being one quote: it
                // is a name, never the keyword it spells ("true" is a column, true is not).
                try out.append(alloc, '"');
                i += 1;
                while (i < expr.len) : (i += 1) {
                    try out.append(alloc, expr[i]);
                    if (expr[i] == '"') {
                        if (i + 1 < expr.len and expr[i + 1] == '"') {
                            i += 1;
                            try out.append(alloc, '"');
                        } else {
                            i += 1;
                            break;
                        }
                    }
                }
            },
            ' ', '\t', '\r', '\n' => {
                const alias = " as current_setting";
                if (startsWithIgnoreCase(expr[i..], alias) and (i + alias.len >= expr.len or !isIdentChar(expr[i + alias.len])))
                    i += alias.len
                else
                    i += 1;
            },
            '(', ')' => i += 1,
            ':' => {
                if (startsWithIgnoreCase(expr[i..], "::text")) i += "::text".len else {
                    try out.append(alloc, ':');
                    i += 1;
                }
            },
            else => {
                try out.append(alloc, std.ascii.toLower(ch));
                i += 1;
            },
        }
    }
    return out.items;
}

fn quoteLiteral(alloc: std.mem.Allocator, s: []const u8) ![]u8 {
    return std.mem.replaceOwned(u8, alloc, s, "'", "''");
}

/// An identifier as PostgreSQL prints it: bare when it is plain lowercase, quoted
/// otherwise. Reserved words are not recognised, so a column called `user` is quoted by
/// PostgreSQL and not here: its policy then reads as "not recognised", the safe direction.
fn quoteIdent(alloc: std.mem.Allocator, name: []const u8) ![]u8 {
    var plain = name.len > 0 and (std.ascii.isLower(name[0]) or name[0] == '_');
    for (name) |ch| plain = plain and (std.ascii.isLower(ch) or std.ascii.isDigit(ch) or ch == '_');
    if (plain) return alloc.dupe(u8, name);
    const doubled = try std.mem.replaceOwned(u8, alloc, name, "\"", "\"\"");
    return std.fmt.allocPrint(alloc, "\"{s}\"", .{doubled});
}

/// The text of the bridge's own read policy (`zebridge_scope_reads_by_tenant`) for a
/// table, in the unnormalised form it was written in.
fn bridgePredicate(alloc: std.mem.Allocator, tenant_col: []const u8, open_tenant: []const u8) ![]u8 {
    const lit = try quoteLiteral(alloc, open_tenant);
    const col = try quoteIdent(alloc, tenant_col);
    return std.fmt.allocPrint(
        alloc,
        "coalesce((select current_setting('{s}', true)), '') = '' or {s}::text = (select current_setting('{s}', true)) or {s}::text = '{s}'",
        .{ config.Sync.tenant_setting, col, config.Sync.tenant_setting, col, lit },
    );
}

// ── classification ───────────────────────────────────────────────────────────────────

fn finding(alloc: std.mem.Allocator, v: Verdict, comptime fmt: []const u8, args: anytype) !struct { Verdict, []const u8 } {
    return .{ v, try std.fmt.allocPrint(alloc, fmt, args) };
}

/// The pure decision. `tenant_col` is '' for an untenanted table.
pub fn classify(alloc: std.mem.Allocator, facts: Facts, tenant_col: []const u8, open_tenant: []const u8) !struct { Verdict, []const u8 } {
    if (!facts.exists) return finding(alloc, .unverifiable, "the table is not visible to the reader's catalog lookup", .{});
    if (!facts.rls_enabled) return finding(alloc, .safe, "row security is off for the table", .{});
    if (facts.role_bypass) return finding(alloc, .safe, "the reader bypasses row security", .{});
    if (facts.is_owner and !facts.force_rls) return finding(alloc, .safe, "the reader owns the table and row security is not forced", .{});

    const expected: ?[]const u8 = if (tenant_col.len > 0)
        try normalize(alloc, try bridgePredicate(alloc, tenant_col, open_tenant))
    else
        null;

    // Restrictive policies are AND-ed: each one is a possible filter.
    var unknown_restrictive: ?Policy = null;
    for (facts.policies) |p| {
        if (p.permissive) continue;
        const n = try normalize(alloc, p.qual);
        if (std.mem.eql(u8, n, "true")) continue;
        if (std.mem.eql(u8, n, "false"))
            return finding(alloc, .hidden, "restrictive policy '{s}' is USING (false): every row is hidden", .{p.name});
        if (unknown_restrictive == null) unknown_restrictive = p;
    }

    // Permissive policies are OR-ed: one understood pass-through is enough.
    var permissive_seen: usize = 0;
    var all_false = true;
    var passes = false;
    var unknown_permissive: ?Policy = null;
    for (facts.policies) |p| {
        if (!p.permissive) continue;
        permissive_seen += 1;
        const n = try normalize(alloc, p.qual);
        if (std.mem.eql(u8, n, "true") or (expected != null and std.mem.eql(u8, n, expected.?))) {
            passes = true;
            all_false = false;
        } else if (std.mem.eql(u8, n, "false")) {
            // contributes nothing
        } else {
            all_false = false;
            if (unknown_permissive == null) unknown_permissive = p;
        }
    }
    if (permissive_seen == 0)
        return finding(alloc, .hidden, "row security is on and no policy lets the reader SELECT: PostgreSQL then shows it no row", .{});
    if (!passes and all_false)
        return finding(alloc, .hidden, "every permissive policy that applies to the reader is USING (false): every row is hidden", .{});

    if (unknown_restrictive) |p|
        return finding(alloc, .unverifiable, "restrictive policy '{s}' applies to the reader and is not one this bridge recognises (USING {s}); it can hide any subset of rows", .{ p.name, p.qual });
    if (!passes) {
        const p = unknown_permissive.?;
        return finding(alloc, .unverifiable, "no permissive policy for the reader is USING (true) or the bridge's tenant predicate; '{s}' (USING {s}) is not recognised", .{ p.name, p.qual });
    }
    return finding(alloc, .safe, "the reader's read policy is the bridge's own and nothing restricts it", .{});
}

/// Hash of everything a verdict rests on, so "the configuration moved during the build"
/// is a single comparison.
pub fn fingerprintOf(facts: Facts) u64 {
    var h = std.hash.Wyhash.init(0);
    const head = [_]u8{
        @intFromBool(facts.exists),      @intFromBool(facts.rls_enabled), @intFromBool(facts.force_rls),
        @intFromBool(facts.role_bypass), @intFromBool(facts.is_owner),
    };
    h.update(&head);
    for (facts.policies) |p| {
        h.update(p.name);
        h.update(&[_]u8{ 0, @intFromBool(p.permissive), 0 });
        h.update(p.qual);
        h.update(&[_]u8{0xff});
    }
    return h.final();
}

// ── catalog fetch ────────────────────────────────────────────────────────────────────

fn run(pgc: *c.PGconn, sql: [:0]const u8, params: []const ?[*:0]const u8) !*c.PGresult {
    const res = c.PQexecParams(pgc, sql.ptr, @intCast(params.len), null, if (params.len > 0) params.ptr else null, null, null, 0) orelse return error.QueryFailed;
    const st = c.PQresultStatus(res);
    if (st != c.PGRES_TUPLES_OK and st != c.PGRES_COMMAND_OK) {
        log.err("catalog query failed: {s}", .{c.PQerrorMessage(pgc)});
        c.PQclear(res);
        return error.QueryFailed;
    }
    return res;
}

fn cell(res: *c.PGresult, row: c_int, col: c_int) []const u8 {
    return std.mem.span(c.PQgetvalue(res, row, col));
}

/// Reads the facts for `public.<table>` as the connected role. Plain catalog joins, no
/// regclass cast: the reader's name resolution is not needed to read the catalog.
pub fn fetch(alloc: std.mem.Allocator, pgc: *c.PGconn, table: []const u8) !Facts {
    const table_z = try alloc.dupeSentinel(u8, table, 0);
    const params = [_]?[*:0]const u8{table_z.ptr};
    const head = try run(pgc, "SELECT c.oid::text, c.relrowsecurity, c.relforcerowsecurity, " ++
        "COALESCE((SELECT r.rolsuper OR r.rolbypassrls FROM pg_roles r WHERE r.rolname = current_user), false), " ++
        "pg_has_role(current_user, c.relowner, 'USAGE') " ++
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " ++
        "WHERE n.nspname = 'public' AND c.relname = $1", &params);
    defer c.PQclear(head);
    if (c.PQntuples(head) == 0) return .{ .exists = false };
    const oid_z = try alloc.dupeSentinel(u8, cell(head, 0, 0), 0);
    var facts: Facts = .{
        .rls_enabled = cell(head, 0, 1)[0] == 't',
        .force_rls = cell(head, 0, 2)[0] == 't',
        .role_bypass = cell(head, 0, 3)[0] == 't',
        .is_owner = cell(head, 0, 4)[0] == 't',
    };
    if (!facts.rls_enabled) return facts;

    // polroles = {0} is PUBLIC. A policy without USING adds nothing (PostgreSQL skips it),
    // so it is left out here too.
    const pol_params = [_]?[*:0]const u8{oid_z.ptr};
    const pol = try run(pgc, "SELECT p.polname, p.polpermissive, pg_get_expr(p.polqual, p.polrelid) " ++
        "FROM pg_policy p " ++
        "WHERE p.polrelid = $1::oid AND p.polcmd IN ('r', '*') AND p.polqual IS NOT NULL " ++
        "  AND (p.polroles = '{0}'::oid[] " ++
        "       OR EXISTS (SELECT 1 FROM unnest(p.polroles) AS r(oid) WHERE r.oid <> 0 AND pg_has_role(current_user, r.oid, 'MEMBER'))) " ++
        "ORDER BY p.polname", &pol_params);
    defer c.PQclear(pol);
    const n: usize = @intCast(c.PQntuples(pol));
    const list = try alloc.alloc(Policy, n);
    for (0..n) |i| {
        const r: c_int = @intCast(i);
        list[i] = .{
            .name = try alloc.dupe(u8, cell(pol, r, 0)),
            .permissive = cell(pol, r, 1)[0] == 't',
            .qual = try alloc.dupe(u8, cell(pol, r, 2)),
        };
    }
    facts.policies = list;
    return facts;
}

/// What a build does with the fresh `check` it takes after reading its content: null when
/// the configuration is the one it was verified under, else the verdict to block with.
pub fn recheck(fresh: Check, verified_fingerprint: u64) ?struct { Verdict, []const u8 } {
    if (fresh.verdict != .safe) return .{ fresh.verdict, fresh.reason };
    if (fresh.fingerprint != verified_fingerprint) return .{ .unverifiable, changed_reason };
    return null;
}

/// Fetch, classify, fingerprint: the one call a build makes.
pub fn check(alloc: std.mem.Allocator, pgc: *c.PGconn, table: []const u8, tenant_col: []const u8, open_tenant: []const u8) !Check {
    const facts = try fetch(alloc, pgc, table);
    const v, const why = try classify(alloc, facts, tenant_col, open_tenant);
    return .{ .verdict = v, .reason = why, .fingerprint = fingerprintOf(facts) };
}

// ── the count detector ───────────────────────────────────────────────────────────────

pub const Counts = struct { scoped: i64, cleared: i64 };

/// Counts the pair's rows twice inside the open snapshot, with identical explicit
/// tenant / open-tenant and tombstone filters: once as the build reads them (`zb.tenant`
/// set to the tenant) and once with the setting cleared, which turns the bridge's own
/// policy into a pass-through. Fewer rows with the setting on means something besides the
/// explicit filter is narrowing the read.
///
/// ⚠️ Not a completeness proof. A restrictive policy that ignores `zb.tenant` hides the
/// same rows in both counts. `check` is the guarantee; this catches the policies that
/// react to the setting. Leaves `zb.tenant` as it found it. Only for a tenant-scoped pair.
pub fn countMismatch(alloc: std.mem.Allocator, pgc: *c.PGconn, table: []const u8, tenant_col: []const u8, live_col: []const u8, tenant: []const u8, open_tenant: []const u8) !Counts {
    const live = if (live_col.len > 0) try std.fmt.allocPrint(alloc, " AND \"{s}\" IS NULL", .{live_col}) else "";
    const sql = try utils.allocPrintZ(alloc, "SELECT count(*) FROM \"{s}\" WHERE \"{s}\"::text IN ($1, $2){s}", .{ table, tenant_col, live });
    const tenant_z = try alloc.dupeSentinel(u8, tenant, 0);
    const open_z = try alloc.dupeSentinel(u8, open_tenant, 0);
    const params = [_]?[*:0]const u8{ tenant_z.ptr, open_z.ptr };
    const set_sql = "SELECT set_config('" ++ config.Sync.tenant_setting ++ "', $1, true)";

    const a = try run(pgc, sql, &params);
    const scoped = std.fmt.parseInt(i64, cell(a, 0, 0), 10) catch 0;
    c.PQclear(a);

    const clear_p = [_]?[*:0]const u8{""};
    c.PQclear(try run(pgc, set_sql, &clear_p));
    const b_res = run(pgc, sql, &params);
    // Restore the scope whether or not the second count worked.
    const restore_p = [_]?[*:0]const u8{tenant_z.ptr};
    c.PQclear(try run(pgc, set_sql, &restore_p));
    const b = try b_res;
    defer c.PQclear(b);
    return .{ .scoped = scoped, .cleared = std.fmt.parseInt(i64, cell(b, 0, 0), 10) catch 0 };
}

// ── what a blocked pair looks like to the operator ─────────────────────────────────────

pub const Change = enum { new, changed, same };

const Entry = struct {
    tenant: []u8,
    table: []u8,
    verdict: Verdict,
    reason: []u8,
    since_ms: i64,
    active: bool,
};

/// The pairs whose publication is currently blocked, shared between the producer's
/// workers (which set and clear) and the HTTP thread (which renders). Entries outlive a
/// recovery with `active = false`, so a scrape sees the 1 → 0 edge rather than a series
/// that vanishes (the refused-tables registry does the same).
pub const Registry = struct {
    allocator: std.mem.Allocator,
    lock: utils.SpinLock = .{},
    entries: std.array_hash_map.String(Entry) = .empty,
    blocks_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    active_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    pub fn init(allocator: std.mem.Allocator) Registry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Registry) void {
        for (self.entries.keys(), self.entries.values()) |k, e| {
            self.allocator.free(k);
            self.allocator.free(e.tenant);
            self.allocator.free(e.table);
            self.allocator.free(e.reason);
        }
        self.entries.deinit(self.allocator);
    }

    fn keyOf(buf: []u8, tenant: []const u8, table: []const u8) ?[]const u8 {
        return std.fmt.bufPrint(buf, "{s}\x1f{s}", .{ tenant, table }) catch null;
    }

    /// Records (or refreshes) a block. `.new` and `.changed` are worth a loud log line;
    /// `.same` is the cadence repeating itself.
    pub fn block(self: *Registry, tenant: []const u8, table: []const u8, verdict: Verdict, reason: []const u8) !Change {
        var buf: [512]u8 = undefined;
        const key = keyOf(&buf, tenant, table) orelse return error.NameTooLong;
        self.lock.lock();
        defer self.lock.unlock();
        if (self.entries.getPtr(key)) |e| {
            if (e.active and e.verdict == verdict and std.mem.eql(u8, e.reason, reason)) return .same;
            const was_active = e.active;
            const owned = try self.allocator.dupe(u8, reason);
            self.allocator.free(e.reason);
            e.reason = owned;
            e.verdict = verdict;
            e.active = true;
            if (!was_active) {
                e.since_ms = utils.unixMillis();
                _ = self.active_count.fetchAdd(1, .acq_rel);
                _ = self.blocks_total.fetchAdd(1, .acq_rel);
                return .new;
            }
            return .changed;
        }
        const k = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(k);
        const e: Entry = .{
            .tenant = try self.allocator.dupe(u8, tenant),
            .table = try self.allocator.dupe(u8, table),
            .verdict = verdict,
            .reason = try self.allocator.dupe(u8, reason),
            .since_ms = utils.unixMillis(),
            .active = true,
        };
        try self.entries.put(self.allocator, k, e);
        _ = self.active_count.fetchAdd(1, .acq_rel);
        _ = self.blocks_total.fetchAdd(1, .acq_rel);
        return .new;
    }

    /// The pair published (or was cleared to publish). True when it had been blocked.
    pub fn clear(self: *Registry, tenant: []const u8, table: []const u8) bool {
        if (self.active_count.load(.acquire) == 0) return false;
        var buf: [512]u8 = undefined;
        const key = keyOf(&buf, tenant, table) orelse return false;
        self.lock.lock();
        defer self.lock.unlock();
        const e = self.entries.getPtr(key) orelse return false;
        if (!e.active) return false;
        e.active = false;
        _ = self.active_count.fetchSub(1, .acq_rel);
        return true;
    }

    pub fn count(self: *const Registry) usize {
        return self.active_count.load(.acquire);
    }

    pub fn isBlocked(self: *Registry, tenant: []const u8, table: []const u8) bool {
        var buf: [512]u8 = undefined;
        const key = keyOf(&buf, tenant, table) orelse return false;
        self.lock.lock();
        defer self.lock.unlock();
        const e = self.entries.getPtr(key) orelse return false;
        return e.active;
    }

    fn label(w: *std.Io.Writer, s: []const u8) !void {
        for (s) |ch| switch (ch) {
            '\\' => try w.writeAll("\\\\"),
            '"' => try w.writeAll("\\\""),
            '\n' => try w.writeAll("\\n"),
            else => try w.writeByte(ch),
        };
    }

    pub fn writePrometheus(self: *Registry, w: *std.Io.Writer) !void {
        try w.print("# HELP bridge_snapshot_rls_blocked_pairs (table, tenant) pairs whose chain is not being published because the reader's row-level-security configuration may hide rows\n", .{});
        try w.print("# TYPE bridge_snapshot_rls_blocked_pairs gauge\n", .{});
        try w.print("bridge_snapshot_rls_blocked_pairs {d}\n", .{self.active_count.load(.acquire)});
        try w.print("# HELP bridge_snapshot_rls_blocks_total Times a pair went from publishing to blocked by the row-level-security guard\n", .{});
        try w.print("# TYPE bridge_snapshot_rls_blocks_total counter\n", .{});
        try w.print("bridge_snapshot_rls_blocks_total {d}\n", .{self.blocks_total.load(.acquire)});

        self.lock.lock();
        defer self.lock.unlock();
        if (self.entries.count() == 0) return;
        try w.print("# HELP bridge_snapshot_rls_blocked Per-pair state (1 blocked, 0 recovered this process); verdict is hidden or unverifiable\n", .{});
        try w.print("# TYPE bridge_snapshot_rls_blocked gauge\n", .{});
        for (self.entries.values()) |e| {
            try w.writeAll("bridge_snapshot_rls_blocked{table=\"");
            try label(w, e.table);
            try w.writeAll("\",tenant=\"");
            try label(w, e.tenant);
            try w.print("\",verdict=\"{s}\"}} {d}\n", .{ e.verdict.wireName(), @as(u8, if (e.active) 1 else 0) });
        }
    }
};

// ── tests ────────────────────────────────────────────────────────────────────────────

const bridge_qual_text = "((COALESCE(( SELECT current_setting('zb.tenant'::text, true) AS current_setting), ''::text) = ''::text) OR (tenant_id = ( SELECT current_setting('zb.tenant'::text, true) AS current_setting)) OR (tenant_id = '_default'::text))";
const bridge_qual_varchar = "((COALESCE(( SELECT current_setting('zb.tenant'::text, true) AS current_setting), ''::text) = ''::text) OR ((tenant_id)::text = ( SELECT current_setting('zb.tenant'::text, true) AS current_setting)) OR ((tenant_id)::text = '_default'::text))";

fn verdictOf(facts: Facts, tenant_col: []const u8) !Verdict {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const v, _ = try classify(arena.allocator(), facts, tenant_col, "_default");
    return v;
}

fn reasonOf(alloc: std.mem.Allocator, facts: Facts, tenant_col: []const u8) ![]const u8 {
    _, const why = try classify(alloc, facts, tenant_col, "_default");
    return why;
}

test "no row security, a bypassing or owning reader are safe" {
    try std.testing.expectEqual(Verdict.safe, try verdictOf(.{ .rls_enabled = false }, "tenant_id"));
    try std.testing.expectEqual(Verdict.safe, try verdictOf(.{ .rls_enabled = true, .role_bypass = true }, "tenant_id"));
    try std.testing.expectEqual(Verdict.safe, try verdictOf(.{ .rls_enabled = true, .is_owner = true }, "tenant_id"));
    // FORCE ROW LEVEL SECURITY takes the owner's exemption away: with no policy, hidden.
    try std.testing.expectEqual(Verdict.hidden, try verdictOf(.{ .rls_enabled = true, .is_owner = true, .force_rls = true }, "tenant_id"));
}

test "the bridge's own tenant policy is safe, for text and varchar columns" {
    try std.testing.expectEqual(Verdict.safe, try verdictOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "zb_reader_all", .permissive = true, .qual = bridge_qual_text }} }, "tenant_id"));
    try std.testing.expectEqual(Verdict.safe, try verdictOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "zb_reader_all", .permissive = true, .qual = bridge_qual_varchar }} }, "tenant_id"));
    try std.testing.expectEqual(Verdict.safe, try verdictOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "zb_reader_all", .permissive = true, .qual = "true" }} }, ""));
}

test "a missing policy hides every row" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const f: Facts = .{ .rls_enabled = true };
    try std.testing.expectEqual(Verdict.hidden, try verdictOf(f, "tenant_id"));
    try std.testing.expect(std.mem.find(u8, try reasonOf(arena.allocator(), f, "tenant_id"), "no policy lets the reader SELECT") != null);
    // A policy for another command, or another role, never reaches this list: fetch filters it.
}

test "USING (false) is applicable and hides every row" {
    try std.testing.expectEqual(Verdict.hidden, try verdictOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "deny", .permissive = true, .qual = "false" }} }, "tenant_id"));
    // Beside a working policy a restrictive false still hides everything (AND).
    try std.testing.expectEqual(Verdict.hidden, try verdictOf(.{ .rls_enabled = true, .policies = &.{
        .{ .name = "zb_reader_all", .permissive = true, .qual = bridge_qual_text },
        .{ .name = "lockdown", .permissive = false, .qual = "false" },
    } }, "tenant_id"));
}

test "a restrictive policy is never certified safe unless it is a no-op" {
    const base: Policy = .{ .name = "zb_reader_all", .permissive = true, .qual = bridge_qual_text };
    try std.testing.expectEqual(Verdict.unverifiable, try verdictOf(.{ .rls_enabled = true, .policies = &.{ base, .{ .name = "only_blue", .permissive = false, .qual = "(colour = 'blue'::text)" } } }, "tenant_id"));
    try std.testing.expectEqual(Verdict.safe, try verdictOf(.{ .rls_enabled = true, .policies = &.{ base, .{ .name = "noop", .permissive = false, .qual = "true" } } }, "tenant_id"));
}

test "permissive policies the bridge does not recognise are unverifiable, not safe" {
    // Another tenant column, another open tenant, or a hand-written filter.
    try std.testing.expectEqual(Verdict.unverifiable, try verdictOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "mine", .permissive = true, .qual = "(owner_id = 7)" }} }, "tenant_id"));
    var other_open = bridge_qual_text.*;
    const patched = try std.mem.replaceOwned(u8, std.testing.allocator, &other_open, "_default", "elsewhere");
    defer std.testing.allocator.free(patched);
    try std.testing.expectEqual(Verdict.unverifiable, try verdictOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "zb_reader_all", .permissive = true, .qual = patched }} }, "tenant_id"));
    // Extra permissive policies only add rows: a recognised one beside them is enough.
    try std.testing.expectEqual(Verdict.safe, try verdictOf(.{ .rls_enabled = true, .policies = &.{
        .{ .name = "extra", .permissive = true, .qual = "(owner_id = 7)" },
        .{ .name = "zb_reader_all", .permissive = true, .qual = bridge_qual_text },
    } }, "tenant_id"));
}

test "the tenant predicate is only recognised for the table's own tenant column" {
    try std.testing.expectEqual(Verdict.unverifiable, try verdictOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "zb_reader_all", .permissive = true, .qual = bridge_qual_text }} }, "org_id"));
    try std.testing.expectEqual(Verdict.unverifiable, try verdictOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "zb_reader_all", .permissive = true, .qual = bridge_qual_text }} }, ""));
}

test "normalize keeps literals and quoted identifiers exact, drops the printing noise" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqualStrings("tenant_id='_default'", try normalize(a, "((tenant_id)::text = '_default'::text)"));
    // A literal that differs only in case is a different tenant.
    try std.testing.expect(!std.mem.eql(u8, try normalize(a, "tenant_id = '_default'"), try normalize(a, "tenant_id = '_DEFAULT'")));
    // Keywords and unquoted names are case-insensitive, as in PostgreSQL.
    try std.testing.expectEqualStrings(try normalize(a, "COALESCE(x, '') = ''"), try normalize(a, "coalesce(X,'')=''"));
    // A quoted identifier keeps its case, a doubled quote inside a literal survives.
    try std.testing.expectEqualStrings("\"Tenant Id\"='o''x'", try normalize(a, "(\"Tenant Id\" = 'o''x'::text)"));
    // The alias goes, an identifier that merely starts like it does not.
    try std.testing.expectEqualStrings("current_setting'zb.tenant',true", try normalize(a, "current_setting('zb.tenant'::text, true) AS current_setting"));
}

test "a quoted identifier is never the keyword it spells" {
    // A boolean column named "true": PostgreSQL prints USING ("true") for it. Read as the
    // keyword it would be a no-op; it is a filter, so it must not be certified.
    const base: Policy = .{ .name = "zb_reader_all", .permissive = true, .qual = bridge_qual_text };
    try std.testing.expectEqual(Verdict.unverifiable, try verdictOf(.{ .rls_enabled = true, .policies = &.{ base, .{ .name = "col_true", .permissive = false, .qual = "\"true\"" } } }, "tenant_id"));
    try std.testing.expectEqual(Verdict.unverifiable, try verdictOf(.{ .rls_enabled = true, .policies = &.{ base, .{ .name = "col_true", .permissive = false, .qual = "(\"true\")" } } }, "tenant_id"));
    // A permissive "true"/"false" column is a filter too, never a pass-through or a deny.
    try std.testing.expectEqual(Verdict.unverifiable, try verdictOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "col_true", .permissive = true, .qual = "\"true\"" }} }, "tenant_id"));
    try std.testing.expectEqual(Verdict.unverifiable, try verdictOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "col_false", .permissive = true, .qual = "\"false\"" }} }, "tenant_id"));
    // The keywords themselves are still recognised.
    try std.testing.expectEqual(Verdict.safe, try verdictOf(.{ .rls_enabled = true, .policies = &.{ base, .{ .name = "noop", .permissive = false, .qual = "TRUE" } } }, "tenant_id"));
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expect(!std.mem.eql(u8, try normalize(arena.allocator(), "\"true\""), try normalize(arena.allocator(), "true")));
}

test "a tenant column that needs quoting is recognised, a different open tenant is not" {
    const qual = "((COALESCE(( SELECT current_setting('zb.tenant'::text, true) AS current_setting), ''::text) = ''::text) OR ((\"Tenant Id\")::text = ( SELECT current_setting('zb.tenant'::text, true) AS current_setting)) OR ((\"Tenant Id\")::text = '_default'::text))";
    try std.testing.expectEqual(Verdict.safe, try verdictOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "zb_reader_all", .permissive = true, .qual = qual }} }, "Tenant Id"));
    const upper = try std.mem.replaceOwned(u8, std.testing.allocator, qual, "'_default'", "'_DEFAULT'");
    defer std.testing.allocator.free(upper);
    try std.testing.expectEqual(Verdict.unverifiable, try verdictOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "zb_reader_all", .permissive = true, .qual = upper }} }, "Tenant Id"));
}

test "the fingerprint moves with every fact a verdict rests on" {
    const p1: Policy = .{ .name = "a", .permissive = true, .qual = "true" };
    const base: Facts = .{ .rls_enabled = true, .policies = &.{p1} };
    const same: Facts = .{ .rls_enabled = true, .policies = &.{p1} };
    try std.testing.expectEqual(fingerprintOf(base), fingerprintOf(same));
    try std.testing.expect(fingerprintOf(base) != fingerprintOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "a", .permissive = true, .qual = "false" }} }));
    try std.testing.expect(fingerprintOf(base) != fingerprintOf(.{ .rls_enabled = true, .policies = &.{.{ .name = "a", .permissive = false, .qual = "true" }} }));
    try std.testing.expect(fingerprintOf(base) != fingerprintOf(.{ .rls_enabled = true, .force_rls = true, .policies = &.{p1} }));
    try std.testing.expect(fingerprintOf(base) != fingerprintOf(.{ .rls_enabled = true }));
}

/// Live checks against a real database, run only when `ZB_RLS_LIVE_URL` names one (an ADMIN
/// connection to a database with the init SQL applied) and skipped otherwise. They `SET ROLE`
/// to the reader (`ZB_RLS_LIVE_ROLE`, default bridge_reader) so row security applies, and
/// reach what the classifier normally pre-empts: the count detector on a policy it would
/// itself have refused.
fn liveConn() !*c.PGconn {
    const url = std.c.getenv("ZB_RLS_LIVE_URL") orelse return error.SkipZigTest;
    const conn = c.PQconnectdb(url);
    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        c.PQfinish(conn);
        return error.SkipZigTest;
    }
    return conn.?;
}

fn exec(conn: *c.PGconn, sql: [:0]const u8) !void {
    const res = c.PQexec(conn, sql.ptr) orelse return error.QueryFailed;
    defer c.PQclear(res);
    const st = c.PQresultStatus(res);
    if (st != c.PGRES_COMMAND_OK and st != c.PGRES_TUPLES_OK) {
        std.debug.print("live test statement failed: {s}\n  {s}\n", .{ sql, c.PQerrorMessage(conn) });
        return error.QueryFailed;
    }
}

test "live: the bridge's own policy is safe and counts agree; a policy reacting to zb.tenant is caught by the count detector" {
    const conn = try liveConn();
    defer c.PQfinish(conn);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const role = if (std.c.getenv("ZB_RLS_LIVE_ROLE")) |r| std.mem.span(r) else "bridge_reader";
    const set_role = try std.fmt.allocPrintSentinel(a, "SET ROLE {s}", .{role}, 0);

    try exec(conn, "DROP TABLE IF EXISTS public.zb_rls_unit CASCADE");
    try exec(conn, "CREATE TABLE public.zb_rls_unit (id int PRIMARY KEY, tenant_id text NOT NULL, deleted_at timestamptz)");
    try exec(conn, "INSERT INTO public.zb_rls_unit VALUES (1,'acme',NULL),(2,'acme',NULL),(3,'acme',now()),(4,'globex',NULL),(5,'_default',NULL)");
    defer exec(conn, "DROP TABLE IF EXISTS public.zb_rls_unit CASCADE") catch {};
    try exec(conn, "SELECT public.zebridge_scope_reads_by_tenant('public.zb_rls_unit', 'tenant_id')");
    try exec(conn, "GRANT SELECT ON public.zb_rls_unit TO PUBLIC");

    try exec(conn, "BEGIN");
    try exec(conn, set_role);
    try exec(conn, "SELECT set_config('zb.tenant', 'acme', true)");
    const healthy = try check(a, conn, "zb_rls_unit", "tenant_id", "_default");
    try std.testing.expectEqual(Verdict.safe, healthy.verdict);
    const n = try countMismatch(a, conn, "zb_rls_unit", "tenant_id", "deleted_at", "acme", "_default");
    // acme's two live rows and the open tenant's one, with the setting on and cleared.
    try std.testing.expectEqual(@as(i64, 3), n.scoped);
    try std.testing.expectEqual(@as(i64, 3), n.cleared);
    try exec(conn, "ROLLBACK");

    // A restrictive policy that reads the setting: invisible to a comparison of counts only
    // if it hid the same rows both ways; this one hides EVERYTHING once the setting is cleared.
    try exec(conn, "CREATE POLICY zb_rls_unit_react ON public.zb_rls_unit AS RESTRICTIVE FOR SELECT USING (coalesce(current_setting('zb.tenant', true), '') <> '')");
    try exec(conn, "BEGIN");
    try exec(conn, set_role);
    try exec(conn, "SELECT set_config('zb.tenant', 'acme', true)");
    const reacting = try check(a, conn, "zb_rls_unit", "tenant_id", "_default");
    try std.testing.expectEqual(Verdict.unverifiable, reacting.verdict);
    const m = try countMismatch(a, conn, "zb_rls_unit", "tenant_id", "deleted_at", "acme", "_default");
    try std.testing.expectEqual(@as(i64, 3), m.scoped);
    try std.testing.expectEqual(@as(i64, 0), m.cleared);
    // The detector leaves the scope as it found it.
    const res = c.PQexec(conn, "SELECT current_setting('zb.tenant', true)");
    defer c.PQclear(res);
    try std.testing.expectEqualStrings("acme", std.mem.span(c.PQgetvalue(res, 0, 0)));
    try exec(conn, "ROLLBACK");
}

test "recheck: unchanged passes, unsafe or moved blocks" {
    const same: Check = .{ .verdict = .safe, .reason = "ok", .fingerprint = 7 };
    try std.testing.expect(recheck(same, 7) == null);
    const moved = recheck(same, 8).?;
    try std.testing.expectEqual(Verdict.unverifiable, moved[0]);
    try std.testing.expectEqualStrings(changed_reason, moved[1]);
    // A fresh verdict that is no longer safe wins over the fingerprint comparison, with its own reason.
    const hidden = recheck(.{ .verdict = .hidden, .reason = "no policy", .fingerprint = 7 }, 7).?;
    try std.testing.expectEqual(Verdict.hidden, hidden[0]);
    try std.testing.expectEqualStrings("no policy", hidden[1]);
}

test "registry: block, same, changed, clear, and the scrape shows the 1 to 0 edge" {
    var r = Registry.init(std.testing.allocator);
    defer r.deinit();
    try std.testing.expectEqual(Change.new, try r.block("acme", "t", .hidden, "why"));
    try std.testing.expectEqual(Change.same, try r.block("acme", "t", .hidden, "why"));
    try std.testing.expectEqual(Change.changed, try r.block("acme", "t", .unverifiable, "other"));
    try std.testing.expectEqual(@as(usize, 1), r.count());
    try std.testing.expect(r.isBlocked("acme", "t"));
    try std.testing.expect(!r.isBlocked("globex", "t"));

    var buf: [2048]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try r.writePrometheus(&w);
    try std.testing.expect(std.mem.find(u8, w.buffered(), "bridge_snapshot_rls_blocked_pairs 1") != null);
    try std.testing.expect(std.mem.find(u8, w.buffered(), "bridge_snapshot_rls_blocked{table=\"t\",tenant=\"acme\",verdict=\"unverifiable\"} 1") != null);

    try std.testing.expect(r.clear("acme", "t"));
    try std.testing.expect(!r.clear("acme", "t"));
    var buf2: [2048]u8 = undefined;
    var w2 = std.Io.Writer.fixed(&buf2);
    try r.writePrometheus(&w2);
    try std.testing.expect(std.mem.find(u8, w2.buffered(), "bridge_snapshot_rls_blocked_pairs 0") != null);
    try std.testing.expect(std.mem.find(u8, w2.buffered(), "verdict=\"unverifiable\"} 0") != null);
    // Blocked again after a recovery counts as a new block.
    try std.testing.expectEqual(Change.new, try r.block("acme", "t", .hidden, "again"));
    try std.testing.expectEqual(@as(u64, 2), r.blocks_total.load(.acquire));
}
