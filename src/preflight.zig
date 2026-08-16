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


// ---------------------------------------------------------------------------
// Version column — what makes a table writable from the edge
// ---------------------------------------------------------------------------

/// What a candidate version column is fit for.
///
/// Pure, like `classify`, so the judgement can be tested without a database — the SQL
/// that feeds it is trivial and the interpretation is where the subtlety lives.
pub const VersionVerdict = union(enum) {
    /// Usable. Flags are advisory: the table is edge-writable either way, but each is
    /// a way for last-write-wins to be quietly wrong.
    usable: struct {
        /// `timestamp` rather than `timestamptz`: a naive wall-clock reading. Ordering
        /// breaks the moment one writer stores local time.
        naive: bool,
        /// Second precision means frequent ties, and `<` rejects a tie — so a real edit
        /// is dropped with no error anywhere.
        coarse: bool,
        /// A NULL stored version makes `stored < incoming` evaluate to NULL, so the
        /// upsert's WHERE rejects the write. The bridge keeps an `IS NULL OR` guard,
        /// but a NOT NULL column removes the question.
        nullable: bool,
    },
    /// No column by that name. The table replicates outbound; it just cannot be edited.
    absent,
    /// Named like a creation stamp. Refused however it was configured.
    creation_column,
    /// Present but not orderable in a way LWW can use.
    wrong_type,
};

fn isCreationColumn(name: []const u8) bool {
    for (Config.Sync.creation_column_prefixes) |prefix| {
        if (std.mem.startsWith(u8, name, prefix)) return true;
    }
    return false;
}

/// `typname` is PostgreSQL's internal name (`timestamp`, `timestamptz`, `int8`, …);
/// `typmod` is `atttypmod`, which for a timestamp carries the fractional precision.
pub fn classifyVersionColumn(
    name: []const u8,
    typname: ?[]const u8,
    typmod: i32,
    notnull: bool,
) VersionVerdict {
    const ty = typname orelse return .absent;
    if (isCreationColumn(name)) return .creation_column;

    const is_naive = std.mem.eql(u8, ty, "timestamp");
    const is_aware = std.mem.eql(u8, ty, "timestamptz");
    const is_int = std.mem.eql(u8, ty, "int8") or std.mem.eql(u8, ty, "int4");

    if (!is_naive and !is_aware and !is_int) return .wrong_type;

    // -1 is "no modifier", which for a timestamp means the default of 6.
    const coarse = (is_naive or is_aware) and typmod >= 0 and typmod < Config.Sync.min_timestamp_precision;

    return .{ .usable = .{ .naive = is_naive, .coarse = coarse, .nullable = !notnull } };
}


/// Report, per published table, whether it can be written from the edge.
///
/// **Checks the named column; never searches for one.** Silently picking `created_at`
/// because `updated_at` was missing would mean the comparison never advances and every
/// client write loses — corruption that presents as "sync is flaky". But it does list
/// the table's orderable columns as *candidates*, so an operator is told exactly what to
/// configure instead of hunting through `\d`.
///
/// Reports only. A table without a version column still replicates outbound.
pub fn reportVersionColumns(
    allocator: std.mem.Allocator,
    conn: *c.PGconn,
    publication_name: []const u8,
    sync_rules: *const Config.EventClassification.TransitionRules,
    default_version_column: []const u8,
) !void {
    // Every orderable column of every published table, in one round trip. Ordering by
    // table then attnum makes the grouping a single pass.
    const query = try utils.allocPrintZ(
        allocator,
        \\SELECT pt.tablename, a.attname, t.typname, a.atttypmod, a.attnotnull
        \\FROM pg_publication_tables pt
        \\JOIN pg_namespace ns ON ns.nspname = pt.schemaname
        \\JOIN pg_class cl ON cl.relname = pt.tablename AND cl.relnamespace = ns.oid
        \\JOIN pg_attribute a ON a.attrelid = cl.oid AND a.attnum > 0 AND NOT a.attisdropped
        \\JOIN pg_type t ON t.oid = a.atttypid
        \\WHERE pt.pubname = '{s}'
        \\  AND t.typname IN ('timestamp', 'timestamptz', 'int8', 'int4')
        \\ORDER BY pt.tablename, a.attnum;
    ,
        .{publication_name},
    );
    defer allocator.free(query);

    const res = c.PQexec(conn, query.ptr);
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        log.warn("⚠️  Version-column report skipped: {s}", .{c.PQerrorMessage(conn)});
        return;
    }

    const rows: usize = @intCast(c.PQntuples(res));
    var writable: usize = 0;
    var outbound_only: usize = 0;

    var r: usize = 0;
    while (r < rows) {
        const table = std.mem.span(c.PQgetvalue(res, @intCast(r), 0));
        if (isInternalTable(table)) {
            while (r < rows and std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, @intCast(r), 0)), table)) r += 1;
            continue;
        }

        // The configured name for this table, or the global default.
        const wanted: []const u8 = if (sync_rules.get(table)) |cols|
            (if (cols.len > 0) cols[0] else default_version_column)
        else
            default_version_column;

        // One pass over this table's rows: find the wanted column, remember the rest.
        var verdict: VersionVerdict = .absent;
        var candidates: std.ArrayList([]const u8) = .empty;
        defer candidates.deinit(allocator);

        const table_start = r;
        while (r < rows and std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, @intCast(r), 0)), table)) : (r += 1) {
            const col = std.mem.span(c.PQgetvalue(res, @intCast(r), 1));
            const typname = std.mem.span(c.PQgetvalue(res, @intCast(r), 2));
            const typmod = std.fmt.parseInt(i32, std.mem.span(c.PQgetvalue(res, @intCast(r), 3)), 10) catch -1;
            const notnull = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, @intCast(r), 4)), "t");

            if (std.mem.eql(u8, col, wanted)) {
                verdict = classifyVersionColumn(col, typname, typmod, notnull);
            } else if (!isCreationColumn(col)) {
                try candidates.append(allocator, col);
            }
        }
        _ = table_start;

        switch (verdict) {
            .usable => |u| {
                writable += 1;
                log.info("✍️  '{s}': edge-writable on '{s}'", .{ table, wanted });
                if (u.naive) log.warn(
                    "⚠️  '{s}.{s}' is `timestamp` (no time zone): last-write-wins ordering breaks if any writer stores local time rather than UTC",
                    .{ table, wanted },
                );
                if (u.coarse) log.warn(
                    "⚠️  '{s}.{s}' has second precision: concurrent edits in the same second tie, and a tie is REJECTED — the later write is dropped with no error",
                    .{ table, wanted },
                );
                if (u.nullable) log.warn(
                    "⚠️  '{s}.{s}' is nullable: a NULL stored version makes the comparison NULL, which rejects the write",
                    .{ table, wanted },
                );
            },
            .creation_column => {
                outbound_only += 1;
                log.err(
                    "🔴 '{s}': '{s}' is a creation stamp — set once at insert, never updated. As a version it rejects every update forever. Choose a column that changes on write.",
                    .{ table, wanted },
                );
            },
            .wrong_type => {
                outbound_only += 1;
                log.warn("⚠️  '{s}': '{s}' is not an orderable timestamp/integer — outbound-only", .{ table, wanted });
            },
            .absent => {
                outbound_only += 1;
                if (candidates.items.len == 0) {
                    log.info("ℹ️  '{s}': outbound-only (no '{s}', and no orderable timestamp column to use)", .{ table, wanted });
                } else {
                    log.info("ℹ️  '{s}': outbound-only — no column '{s}'.", .{ table, wanted });
                    for (candidates.items) |cand| {
                        log.info("      candidate: {s}", .{cand});
                    }
                    log.info("      make it edge-writable with: SYNC_RULES={s}:<column>", .{table});
                }
            },
        }
    }

    log.info("✅ Edge writes: {d} table(s) writable, {d} outbound-only", .{ writable, outbound_only });
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
    sync_rules: *const Config.EventClassification.TransitionRules,
    default_version_column: []const u8,
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
            try refused.refuse(table, .no_primary_key);
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

    // Reported after the table shapes, because "can this table be edited from the edge"
    // only matters for tables that replicate at all.
    reportVersionColumns(allocator, conn, publication_name, sync_rules, default_version_column) catch |err| {
        log.warn("⚠️  Version-column report failed: {}", .{err});
    };

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

test "version column - a timestamptz with default precision is unreservedly usable" {
    const v = classifyVersionColumn("updated_at", "timestamptz", -1, true);
    try std.testing.expect(v == .usable);
    try std.testing.expect(!v.usable.naive);
    try std.testing.expect(!v.usable.coarse);
    try std.testing.expect(!v.usable.nullable);
}

test "version column - naive and coarse are warnings, not refusals" {
    // Ecto's timestamps() produces `timestamp`, so refusing naive would exclude the
    // most common Elixir schema there is.
    const naive = classifyVersionColumn("updated_at", "timestamp", -1, true);
    try std.testing.expect(naive == .usable);
    try std.testing.expect(naive.usable.naive);

    // timestamp(0): whole seconds. Ties are then common, and a tie is rejected by `<`.
    const coarse = classifyVersionColumn("updated_at", "timestamp", 0, true);
    try std.testing.expect(coarse.usable.coarse);
}

test "version column - a creation stamp is refused however it was configured" {
    // Set once at insert: as a version it rejects every update forever.
    try std.testing.expect(classifyVersionColumn("created_at", "timestamptz", -1, true) == .creation_column);
    try std.testing.expect(classifyVersionColumn("inserted_at", "timestamp", -1, true) == .creation_column);
}

test "version column - absent and wrong-type leave the table outbound-only" {
    try std.testing.expect(classifyVersionColumn("updated_at", null, 0, false) == .absent);
    try std.testing.expect(classifyVersionColumn("updated_at", "text", -1, true) == .wrong_type);
    // An integer version (microseconds in a bigint) is legitimate.
    try std.testing.expect(classifyVersionColumn("version", "int8", -1, true) == .usable);
}
