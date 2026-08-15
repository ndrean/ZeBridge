//! Startup validation of the publication's tables.
//!
//! ZeBridge imposes requirements on the tables it replicates, but until this module
//! existed it enforced none of them — and every violation failed *silently or late*:
//!
//!   - no primary key + REPLICA IDENTITY DEFAULT
//!         PostgreSQL itself rejects UPDATE/DELETE on the table ("cannot update table
//!         ... because it does not have a replica identity and publishes updates").
//!         Writes fail upstream; the bridge never even sees them.
//!
//!   - no primary key (any identity)
//!         Snapshots are impossible: chunking uses keyset pagination over the primary
//!         key. Fails late, at snapshot-request time, per table. (A composite key is
//!         fine — pagination compares the whole key as a row value.)
//!
//!   - TRANSITION_RULES on a table that is not REPLICA IDENTITY FULL
//!         pgoutput only sends an old tuple for FULL (always) or DEFAULT when the key
//!         changed. Updating a non-key column on a DEFAULT table sends no old tuple,
//!         so transition detection can never fire and every update routes to `.data`.
//!         Nothing errors — the feature is simply inert forever.
//!
//! This reports; it does not refuse to start. A bridge that streams most tables
//! correctly is more useful than one that will not boot, and the operator may well
//! have made these choices deliberately (REPLICA IDENTITY FULL multiplies WAL volume,
//! so DEFAULT is a legitimate trade on wide tables).

const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const pg_conn = @import("pg_conn.zig");
const utils = @import("utils.zig");
const Config = @import("config.zig");
const RefusedTables = @import("refused_tables.zig");

pub const log = std.log.scoped(.preflight);

/// PostgreSQL relreplident values (pg_class.relreplident).
pub const ReplicaIdentity = enum(u8) {
    default = 'd',
    nothing = 'n',
    full = 'f',
    index = 'i',

    pub fn describe(self: ReplicaIdentity) []const u8 {
        return switch (self) {
            .default => "DEFAULT",
            .nothing => "NOTHING",
            .full => "FULL",
            .index => "USING INDEX",
        };
    }
};

/// What preflight found wrong with one table. A table can hit several at once, so
/// these are flags rather than a single verdict.
pub const Findings = struct {
    /// No primary key at all: the table is REFUSED. Rows cannot be identified, so a
    /// DELETE could only be a full-row match — removing every duplicate where
    /// PostgreSQL removed one — and with at-least-once delivery a redelivered INSERT
    /// has nothing to upsert on, so it duplicates. Neither is fixable downstream,
    /// which is why this is a refusal rather than a warning.
    refused_no_pk: bool = false,
    /// PostgreSQL will reject UPDATE/DELETE on this table outright.
    writes_rejected: bool = false,
    /// TRANSITION_RULES names this table but it cannot produce transitions.
    transitions_inert: bool = false,

    pub fn any(self: Findings) bool {
        return self.refused_no_pk or self.writes_rejected or self.transitions_inert;
    }
};

/// Classify a table from its catalog facts.
///
/// Pure so it can be tested without a database — the SQL that feeds it is trivial,
/// the interpretation is where the subtlety lives.
pub fn classify(pk_columns: u32, identity: ReplicaIdentity, has_transition_rules: bool) Findings {
    var f = Findings{};

    // Only "no key at all" is a problem. A composite key identifies rows exactly, and
    // snapshot pagination compares it as a row value, so it needs no finding.
    f.refused_no_pk = pk_columns == 0;

    // Without a PK, DEFAULT leaves the table with no replica identity at all, and
    // PostgreSQL refuses to publish updates for it. FULL rescues this (the whole row
    // becomes the identity), which is why the check is the conjunction, not just
    // "no PK".
    f.writes_rejected = pk_columns == 0 and (identity == .default or identity == .nothing);

    // Transition detection compares an old tuple against the new one. Only FULL
    // guarantees an old tuple on every UPDATE.
    f.transitions_inert = has_transition_rules and identity != .full;

    return f;
}

/// Tables that are ours or the migration tool's — mirrors zebridge_is_internal_table()
/// in init.sql.template and isInternalTable() in event_processor.zig.
fn isInternalTable(name: []const u8) bool {
    return std.mem.eql(u8, name, "zebridge_ddl_events") or
        std.mem.eql(u8, name, "schema_migrations");
}

pub const Summary = struct {
    checked: usize = 0,
    with_findings: usize = 0,
    refused: usize = 0,
};

/// Inspect every table in the publication and log anything that will not work.
///
/// Opens its own non-replication connection: the caller's is either the replication
/// stream (which cannot run ordinary queries) or not yet established at this point.
pub fn run(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publication_name: []const u8,
    transition_rules: *const Config.EventClassification.TransitionRules,
    strict: bool,
    refused: *RefusedTables.Registry,
) !Summary {
    var standard_config = pg_config.*;
    standard_config.replication = false;

    const conn = pg_conn.connect(allocator, standard_config) catch |err| {
        log.warn("⚠️  Preflight skipped: could not connect to PostgreSQL ({})", .{err});
        return Summary{};
    };
    defer c.PQfinish(conn);

    // array_length(indkey,1) counts the PK's columns; 0 rows means no PK at all.
    const query = try utils.allocPrintZ(
        allocator,
        \\SELECT pt.tablename,
        \\       cl.relreplident,
        \\       COALESCE((
        \\           SELECT array_length(i.indkey, 1)
        \\           FROM pg_index i
        \\           WHERE i.indrelid = cl.oid AND i.indisprimary
        \\           LIMIT 1
        \\       ), 0) AS pk_columns
        \\FROM pg_publication_tables pt
        \\JOIN pg_namespace ns ON ns.nspname = pt.schemaname
        \\JOIN pg_class cl ON cl.relname = pt.tablename AND cl.relnamespace = ns.oid
        \\WHERE pt.pubname = '{s}'
        \\ORDER BY pt.tablename;
    ,
        .{publication_name},
    );
    defer allocator.free(query);

    const res = c.PQexec(conn, query.ptr);
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        log.warn("⚠️  Preflight skipped: catalog query failed: {s}", .{c.PQerrorMessage(conn)});
        return Summary{};
    }

    var summary = Summary{};
    const rows: usize = @intCast(c.PQntuples(res));

    var r: c_int = 0;
    while (r < @as(c_int, @intCast(rows))) : (r += 1) {
        const table = std.mem.span(c.PQgetvalue(res, r, 0));
        if (isInternalTable(table)) continue;

        const ident_raw = std.mem.span(c.PQgetvalue(res, r, 1));
        // std.meta.intToEnum was removed in Zig 0.16; switch explicitly. Anything
        // unrecognised is treated as DEFAULT, the conservative reading.
        const identity: ReplicaIdentity = if (ident_raw.len == 0) .default else switch (ident_raw[0]) {
            'f' => .full,
            'n' => .nothing,
            'i' => .index,
            else => .default,
        };

        const pk_columns = std.fmt.parseInt(u32, std.mem.span(c.PQgetvalue(res, r, 2)), 10) catch 0;
        const has_rules = transition_rules.contains(table);

        summary.checked += 1;
        const f = classify(pk_columns, identity, has_rules);
        if (!f.any()) {
            log.debug("✓ {s}: pk_columns={d}, identity={s}", .{ table, pk_columns, identity.describe() });
            continue;
        }
        summary.with_findings += 1;

        if (f.writes_rejected) {
            log.err(
                "🔴 '{s}' has no primary key and REPLICA IDENTITY {s}: PostgreSQL will REJECT every UPDATE and DELETE on it. Fix with: ALTER TABLE {s} REPLICA IDENTITY FULL; (or add a primary key)",
                .{ table, identity.describe(), table },
            );
        }
        if (f.refused_no_pk) {
            summary.refused += 1;
            // Record it before the first WAL byte arrives, so the mutation path drops
            // this table's events from the very first batch rather than after its
            // first DDL event — which may never come.
            try refused.refuse(table);
            log.err(
                "🔴 REFUSING '{s}': no primary key. Rows cannot be identified, so DELETE would match every column (removing more rows than PostgreSQL did) and a redelivered INSERT would duplicate. Its schema is withheld and its CDC events are dropped. Fix with a migration adding a primary key.",
                .{table},
            );
        }
        if (f.transitions_inert) {
            log.warn(
                "⚠️  '{s}' has TRANSITION_RULES but REPLICA IDENTITY {s}: no old tuple is sent for non-key updates, so transitions can NEVER fire and every update routes to '.data'. Fix with: ALTER TABLE {s} REPLICA IDENTITY FULL;",
                .{ table, identity.describe(), table },
            );
        }
    }

    if (summary.with_findings == 0) {
        log.info("✅ Preflight: {d} table(s) checked, no issues", .{summary.checked});
    } else {
        log.warn("⚠️  Preflight: {d}/{d} table(s) have issues, {d} refused (see above)", .{
            summary.with_findings,
            summary.checked,
            summary.refused,
        });
    }

    // Refusing to start is deliberately NOT the default: one PK-less table would stop
    // replication for every other table, and a table created while the bridge runs
    // would turn a schema mistake into an outage. Skipping keeps the same correctness
    // property (no schema published, so no client builds it) without that blast
    // radius. --strict-tables exists for CI and staging gates, where failing the deploy
    // is the point.
    if (strict and summary.refused > 0) {
        log.err("🔴 --strict-tables was given and {d} table(s) were refused — refusing to start", .{summary.refused});
        return error.RefusedTables;
    }

    return summary;
}

// ---------------------------------------------------------------------------
// Tests — the SQL is trivial; the interpretation is what needs pinning down.
// ---------------------------------------------------------------------------

test "classify - single-column PK with DEFAULT is the happy path" {
    const f = classify(1, .default, false);
    try std.testing.expect(!f.any());
}

test "classify - no PK is refused outright" {
    const f = classify(0, .default, false);
    try std.testing.expect(f.refused_no_pk);
    try std.testing.expect(f.writes_rejected);
}

test "classify - FULL rescues the write but not the identity" {
    // Verified on PostgreSQL 18: no PK + DEFAULT errors on UPDATE, no PK + FULL
    // succeeds. Enough for PostgreSQL, not enough for a replica — DELETE is still a
    // full-row match and a redelivered INSERT still duplicates.
    const f = classify(0, .full, false);
    try std.testing.expect(!f.writes_rejected);
    try std.testing.expect(f.refused_no_pk);
}

test "classify - REPLICA IDENTITY NOTHING is as bad as DEFAULT without a PK" {
    const f = classify(0, .nothing, false);
    try std.testing.expect(f.writes_rejected);
}

test "classify - a composite PK is as ordinary as a single-column one" {
    // It identifies rows exactly, and snapshot pagination compares the whole key as a
    // row value, so there is nothing left to report about it.
    const f = classify(2, .default, false);
    try std.testing.expect(!f.any());
}

test "classify - transition rules need FULL" {
    try std.testing.expect(classify(1, .default, true).transitions_inert);
    try std.testing.expect(classify(1, .index, true).transitions_inert);
    try std.testing.expect(!classify(1, .full, true).transitions_inert);
    // No rules configured means nothing to be inert about.
    try std.testing.expect(!classify(1, .default, false).transitions_inert);
}

test "classify - findings combine" {
    const f = classify(0, .default, true);
    try std.testing.expect(f.refused_no_pk);
    try std.testing.expect(f.writes_rejected);
    try std.testing.expect(f.transitions_inert);
}

test "classify - a single-column PK is never refused, whatever the identity" {
    for ([_]ReplicaIdentity{ .default, .full, .index, .nothing }) |ri| {
        try std.testing.expect(!classify(1, ri, false).refused_no_pk);
    }
}

test "isInternalTable" {
    try std.testing.expect(isInternalTable("zebridge_ddl_events"));
    try std.testing.expect(isInternalTable("schema_migrations"));
    try std.testing.expect(!isInternalTable("users"));
}
