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
//!         Snapshots are impossible: chunking uses keyset pagination over a single
//!         PK column. Fails late, at snapshot-request time, per table.
//!
//!   - composite primary key
//!         Same: getTablePrimaryKey requires array_length(indkey,1) = 1.
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
    /// PostgreSQL will reject UPDATE/DELETE on this table outright.
    writes_rejected: bool = false,
    /// Snapshots cannot chunk this table (needs exactly one PK column).
    snapshots_unavailable: bool = false,
    /// TRANSITION_RULES names this table but it cannot produce transitions.
    transitions_inert: bool = false,

    pub fn any(self: Findings) bool {
        return self.writes_rejected or self.snapshots_unavailable or self.transitions_inert;
    }
};

/// Classify a table from its catalog facts.
///
/// Pure so it can be tested without a database — the SQL that feeds it is trivial,
/// the interpretation is where the subtlety lives.
pub fn classify(pk_columns: u32, identity: ReplicaIdentity, has_transition_rules: bool) Findings {
    var f = Findings{};

    // Snapshots use keyset pagination over one PK column; zero or several is fatal
    // to snapshotting regardless of replica identity.
    f.snapshots_unavailable = pk_columns != 1;

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
        if (f.snapshots_unavailable) {
            if (pk_columns == 0) {
                log.warn(
                    "⚠️  '{s}' has no primary key: CDC may stream, but snapshots are unavailable (chunking needs one PK column)",
                    .{table},
                );
            } else {
                log.warn(
                    "⚠️  '{s}' has a {d}-column primary key: snapshots are unavailable (chunking needs exactly one)",
                    .{ table, pk_columns },
                );
            }
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
        log.warn("⚠️  Preflight: {d}/{d} table(s) have issues (see above)", .{ summary.with_findings, summary.checked });
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

test "classify - no PK with DEFAULT means PostgreSQL rejects writes" {
    const f = classify(0, .default, false);
    try std.testing.expect(f.writes_rejected);
    try std.testing.expect(f.snapshots_unavailable);
}

test "classify - REPLICA IDENTITY FULL rescues writes for a PK-less table" {
    // Verified against PostgreSQL 18: no PK + DEFAULT errors on UPDATE, no PK + FULL
    // succeeds. FULL makes the whole row the identity.
    const f = classify(0, .full, false);
    try std.testing.expect(!f.writes_rejected);
    // ...but it does not rescue snapshots, which still need one PK column.
    try std.testing.expect(f.snapshots_unavailable);
}

test "classify - REPLICA IDENTITY NOTHING is as bad as DEFAULT without a PK" {
    const f = classify(0, .nothing, false);
    try std.testing.expect(f.writes_rejected);
}

test "classify - composite PK blocks snapshots but not writes" {
    const f = classify(2, .default, false);
    try std.testing.expect(!f.writes_rejected);
    try std.testing.expect(f.snapshots_unavailable);
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
    try std.testing.expect(f.writes_rejected);
    try std.testing.expect(f.snapshots_unavailable);
    try std.testing.expect(f.transitions_inert);
}

test "isInternalTable" {
    try std.testing.expect(isInternalTable("zebridge_ddl_events"));
    try std.testing.expect(isInternalTable("schema_migrations"));
    try std.testing.expect(!isInternalTable("users"));
}
