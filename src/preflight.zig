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
const WritableTables = @import("writable_tables.zig");
const Topology = @import("topology.zig");

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


/// Report, per tenant-scoped table, whether its tenant column can actually reach the
/// subject — and refuse the table if it cannot.
///
/// A tenant column routes CDC only if it is present on **every** event, and the one that
/// bites is DELETE: under `REPLICA IDENTITY DEFAULT` a delete carries the key columns and
/// nothing else. So a tenant outside the replica identity produces inserts and updates
/// that route correctly and deletes that cannot route at all — the worst shape of bug,
/// because it looks like it works.
///
/// ⚠️ PostgreSQL enforces the same rule from the other side: a *publication* row filter on
/// a column outside the replica identity makes the table unwritable (`cannot update table
/// … not part of the replica identity`). This check exists because model B uses no
/// publication filter, so nothing in the database would raise — the bridge has to.
///
/// Refusal, not a warning: a table whose deletes cannot be addressed would leave rows in
/// every replica that ever held them, and no later event could remove them.
pub fn reportTenantColumns(
    allocator: std.mem.Allocator,
    conn: *c.PGconn,
    publication_name: []const u8,
    tenant_rules: *const Config.EventClassification.TransitionRules,
    refused: *RefusedTables.Registry,
) !void {
    if (tenant_rules.count() == 0) return;

    var it = tenant_rules.iterator();
    var scoped: usize = 0;
    while (it.next()) |entry| {
        const table = entry.key_ptr.*;
        const cols = entry.value_ptr.*;
        if (cols.len == 0) continue;
        const tenant_col = cols[0];

        // One query answers both halves: does the column exist, and is it inside whatever
        // this table uses as its replica identity (the PK for 'd', the marked index for
        // 'i', every column for 'f').
        const query = try utils.allocPrintZ(
            allocator,
            \\SELECT
            \\  EXISTS (SELECT 1 FROM pg_attribute
            \\          WHERE attrelid = '"public"."{s}"'::regclass AND attname = '{s}'
            \\            AND attnum > 0 AND NOT attisdropped),
            \\  (SELECT relreplident FROM pg_class WHERE oid = '"public"."{s}"'::regclass),
            \\  COALESCE((SELECT bool_or(a.attname = '{s}')
            \\            FROM pg_class cl
            \\            JOIN pg_index i ON i.indrelid = cl.oid
            \\            JOIN pg_attribute a ON a.attrelid = cl.oid AND a.attnum = ANY(i.indkey)
            \\            WHERE cl.oid = '"public"."{s}"'::regclass
            \\              AND ((cl.relreplident = 'd' AND i.indisprimary)
            \\                OR (cl.relreplident = 'i' AND i.indisreplident))), false);
        ,
            .{ table, tenant_col, table, tenant_col, table },
        );
        defer allocator.free(query);

        const res = c.PQexec(conn, query.ptr);
        defer c.PQclear(res);
        if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK or c.PQntuples(res) == 0) {
            log.warn("⚠️  Tenant check skipped for '{s}': {s}", .{ table, c.PQerrorMessage(conn) });
            continue;
        }

        const exists = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, 0, 0)), "t");
        const ri = std.mem.span(c.PQgetvalue(res, 0, 1));
        const in_ri = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, 0, 2)), "t") or
            (ri.len > 0 and ri[0] == 'f');

        if (!exists) {
            log.err("🔴 REFUSING '{s}': TENANT_RULES names column '{s}', which the table does not have.", .{ table, tenant_col });
            refused.refuse(table, .no_tenant_column) catch {};
            continue;
        }
        if (!in_ri) {
            log.err(
                "🔴 REFUSING '{s}': tenant column '{s}' is outside the replica identity, so a DELETE carries the key and nothing else — inserts and updates would route to the right subject and deletes could not route at all, leaving rows in every replica that held them. Fix: CREATE UNIQUE INDEX {s}_zb_ri ON {s} ({s}, <pk>); ALTER TABLE {s} REPLICA IDENTITY USING INDEX {s}_zb_ri;",
                .{ table, tenant_col, table, table, tenant_col, table, table },
            );
            refused.refuse(table, .tenant_not_in_replica_identity) catch {};
            continue;
        }

        log.info("🏷️  '{s}': reads scoped by '{s}' → cdc.<tenant>.{s}.<op> (grant: cdc.<tenant>.>)", .{ table, tenant_col, table });
        scoped += 1;
    }

    log.info("✅ Tenant scoping: {d} table(s) route reads by tenant", .{scoped});
    _ = publication_name;
}

/// The role name out of a PostgreSQL connection URL.
///
/// The writer's *grants* are the only authoritative statement of which tables are meant
/// to accept edge writes — `SYNC_RULES` says how to write a table, never whether it may
/// be written — so the report needs the role those grants were made to. It is already in
/// `DATABASE_WRITER_URL`; nothing else has to be configured.
///
/// Pure and separately tested: a URL with no userinfo, or one whose password contains an
/// `@` or a `:`, must not silently yield a wrong role name and turn every grant check
/// into a false negative.
pub fn roleFromUrl(url: []const u8) ?[]const u8 {
    const scheme_end = std.mem.indexOf(u8, url, "://") orelse return null;
    var rest = url[scheme_end + 3 ..];

    // Authority ends at the first '/', and the password may legally contain '@', so the
    // split is on the *last* '@' before that.
    if (std.mem.indexOfScalar(u8, rest, '/')) |slash| rest = rest[0..slash];
    const at = std.mem.lastIndexOfScalar(u8, rest, '@') orelse return null;
    const userinfo = rest[0..at];
    if (userinfo.len == 0) return null;

    const colon = std.mem.indexOfScalar(u8, userinfo, ':') orelse return userinfo;
    return if (colon == 0) null else userinfo[0..colon];
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
    writer_role: ?[]const u8,
    /// Records the combined verdict (§1.11) so `appendWriteContract` can publish it
    /// instead of a client discovering "outbound-only" only after a rejected write.
    writable: *WritableTables.Registry,
) !void {
    // Every orderable column of every published table, in one round trip. Ordering by
    // table then attnum makes the grouping a single pass.
    const query = try utils.allocPrintZ(
        allocator,
        \\SELECT pt.tablename, a.attname, t.typname, a.atttypmod, a.attnotnull,
        \\       CASE WHEN '{s}' <> '' AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{s}')
        \\            THEN has_table_privilege('{s}', cl.oid, 'INSERT') ELSE false END AS granted,
        \\       coalesce((
        \\         SELECT bool_or(pg_get_expr(ad.adbin, ad.adrelid) LIKE 'nextval%'
        \\                        OR pk.attidentity <> '')
        \\         FROM pg_index i
        \\         JOIN pg_attribute pk ON pk.attrelid = i.indrelid AND pk.attnum = ANY(i.indkey)
        \\         LEFT JOIN pg_attrdef ad ON ad.adrelid = pk.attrelid AND ad.adnum = pk.attnum
        \\         WHERE i.indrelid = cl.oid AND i.indisprimary
        \\       ), false) AS key_db_allocated
        \\FROM pg_publication_tables pt
        \\JOIN pg_namespace ns ON ns.nspname = pt.schemaname
        \\JOIN pg_class cl ON cl.relname = pt.tablename AND cl.relnamespace = ns.oid
        \\JOIN pg_attribute a ON a.attrelid = cl.oid AND a.attnum > 0 AND NOT a.attisdropped
        \\JOIN pg_type t ON t.oid = a.atttypid
        \\WHERE pt.pubname = '{s}'
        \\  AND t.typname IN ('timestamp', 'timestamptz', 'int8', 'int4')
        \\ORDER BY pt.tablename, a.attnum;
    ,
        .{ writer_role orelse "", writer_role orelse "", writer_role orelse "", publication_name },
    );
    defer allocator.free(query);

    const res = c.PQexec(conn, query.ptr);
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        log.warn("⚠️  Version-column report skipped: {s}", .{c.PQerrorMessage(conn)});
        return;
    }

    const rows: usize = @intCast(c.PQntuples(res));
    var writable_count: usize = 0;
    var outbound_only: usize = 0;
    var mismatched: usize = 0;

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
        var granted = false;
        var key_db_allocated = false;
        var candidates: std.ArrayList([]const u8) = .empty;
        defer candidates.deinit(allocator);

        const table_start = r;
        while (r < rows and std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, @intCast(r), 0)), table)) : (r += 1) {
            const col = std.mem.span(c.PQgetvalue(res, @intCast(r), 1));
            const typname = std.mem.span(c.PQgetvalue(res, @intCast(r), 2));
            const typmod = std.fmt.parseInt(i32, std.mem.span(c.PQgetvalue(res, @intCast(r), 3)), 10) catch -1;
            const notnull = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, @intCast(r), 4)), "t");

            // Same for every row of the table; read once per row rather than tracked.
            granted = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, @intCast(r), 5)), "t");
            key_db_allocated = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, @intCast(r), 6)), "t");

            if (std.mem.eql(u8, col, wanted)) {
                verdict = classifyVersionColumn(col, typname, typmod, notnull);
            } else if (!isCreationColumn(col)) {
                try candidates.append(allocator, col);
            }
        }
        _ = table_start;

        const usable = logVerdict(table, wanted, verdict, candidates.items, granted);
        if (usable) writable_count += 1 else outbound_only += 1;
        if (reportWriteIntent(table, verdict, granted, key_db_allocated, sync_rules.get(table) != null)) mismatched += 1;

        // `logVerdict` alone would call a table with a database-allocated key
        // "writable" — it has grant and version column, but `mutation_listener.zig`
        // refuses every write to it anyway (`DbAllocatedKey`). The published fact has
        // to be what a client can actually rely on, not just what the schema shape
        // permits.
        writable.set(table, usable and !key_db_allocated) catch |err| {
            log.warn("⚠️  Could not record writability for '{s}': {}", .{ table, err });
        };
    }

    log.info("✅ Edge writes: {d} table(s) writable, {d} outbound-only", .{ writable_count, outbound_only });
    if (mismatched > 0) {
        // ⚠️ Deliberately does NOT say "each one fails at runtime". That was true when the
        // only finding here was a missing version column, and it is false for a
        // database-allocated key — those writes succeed. Telling an operator to expect a
        // runtime failure that never comes is worse than saying nothing.
        log.warn(
            "⚠️  {d} table(s) where the write grant and the schema disagree — see above. Every mutation for them is refused at the write path; reads are unaffected.",
            .{mismatched},
        );
    }
}

/// Cross-check **intent** against **capability**, and report where they disagree.
///
/// `SYNC_RULES` teaches the bridge *how* to write a table; nothing in it says a table
/// *may* be written, so nothing validates it against the schema. The missing declaration
/// was already there and unread: `GRANT INSERT ... TO bridge_writer` is explicit,
/// per-table, made deliberately by a DBA, and — unlike any config file — cannot drift
/// from what is enforced, because it *is* what PostgreSQL enforces.
///
/// Every case below fails at runtime today, and two of them fail *silently*: the bridge
/// classifies a SQL error as transient (it cannot tell `permission denied` from a dropped
/// connection), so the write is retried to `max_deliver` and the client sees no error, no
/// row, and no CDC echo. Saying it at boot costs one catalog query.
///
/// Returns true when something was reported.
fn reportWriteIntent(
    table: []const u8,
    verdict: VersionVerdict,
    granted: bool,
    key_db_allocated: bool,
    named_in_sync_rules: bool,
) bool {
    if (granted) {
        if (verdict == .absent or verdict == .creation_column or verdict == .wrong_type) {
            log.err(
                "🔴 '{s}': granted INSERT to the writer, but it has no usable version column. Every mutation will fail NoVersionColumn. Grant is intent; the schema cannot honour it.",
                .{table},
            );
            return true;
        }
        if (key_db_allocated) {
            // ⚠️ Worded to say that these writes SUCCEED. That is the whole danger, and it
            // is the opposite of every other finding here: nothing fails, nothing is
            // logged later, and the damage surfaces in the application months on as a
            // duplicate key it cannot explain — or, through the bridge, as one row
            // silently overwriting another that was never related to it.
            log.err(
                "🔴 '{s}': granted INSERT to the writer, but its primary key is database-allocated (serial/identity), so the write path REFUSES every mutation for it (DbAllocatedKey). Allowing them would corrupt quietly — a client-supplied key does not advance the sequence, so each edge write plants a value the application's own INSERT later collides with, and through the bridge that collision does not even error: ON CONFLICT DO UPDATE overwrites whichever row was there first. The table still replicates outbound. Use a uuid key, or drop the column DEFAULT and assign clients disjoint ranges.",
                .{table},
            );
            return true;
        }
        return false;
    }

    // Not granted. Silence is right for an ordinary read-only table — most of a schema —
    // but configuring one for writes it can never perform is worth saying out loud.
    if (named_in_sync_rules) {
        log.warn(
            "⚠️  '{s}': SYNC_RULES configures it for edge writes, but the writer has no INSERT privilege. Mutations will be retried to max_deliver and dead-lettered, with nothing explaining why. Either GRANT it or drop the rule.",
            .{table},
        );
        return true;
    }
    return false;
}

/// Log one table's verdict. Returns true when the table is edge-writable.
///
/// Split out of `reportVersionColumns` so the identical lines appear for a table that
/// arrives *after* boot via a DDL event. Before this, a table created while the bridge
/// was running got its schema published and its no-PK refusal made, but never the ✍️/⚠️
/// report — so the one warning that `updated_at` is naive, or coarse, or nullable, was
/// shown only to whoever happened to restart. These fire once per table, not per event.
fn logVerdict(
    table: []const u8,
    wanted: []const u8,
    verdict: VersionVerdict,
    candidates: []const []const u8,
    /// Whether the writer actually holds INSERT. A usable version column makes a table
    /// *capable* of edge writes; only the grant makes it **open** to them.
    ///
    /// ⚠️ Reported ✍️ on the column alone until this argument existed, so `users` — no
    /// grant, and refusing every write with `permission denied` — was announced as
    /// edge-writable at every boot. Two reports in the same output disagreeing about the
    /// same table is worse than either being wrong alone.
    granted: bool,
) bool {
    switch (verdict) {
        .usable => |u| {
            if (!granted) {
                log.info(
                    "📖 '{s}': outbound-only — '{s}' would serve as a version column, but the writer has no INSERT privilege. Open it with SELECT zebridge_grant_edge_writes('public.{s}').",
                    .{ table, wanted, table },
                );
                return false;
            }
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
            return true;
        },
        .creation_column => {
            log.err(
                "🔴 '{s}': '{s}' is a creation stamp — set once at insert, never updated. As a version it rejects every update forever. Choose a column that changes on write.",
                .{ table, wanted },
            );
            return false;
        },
        .wrong_type => {
            log.warn("⚠️  '{s}': '{s}' is not an orderable timestamp/integer — outbound-only", .{ table, wanted });
            return false;
        },
        .absent => {
            if (candidates.len == 0) {
                log.info("ℹ️  '{s}': outbound-only (no '{s}', and no orderable timestamp column to use)", .{ table, wanted });
            } else {
                log.info("ℹ️  '{s}': outbound-only — no column '{s}'.", .{ table, wanted });
                for (candidates) |cand| {
                    log.info("      candidate: {s}", .{cand});
                }
                log.info("      make it edge-writable with: SYNC_RULES={s}:<column>", .{table});
            }
            return false;
        },
    }
}

/// The same report, for one table, on demand.
///
/// Called from the DDL path when a table appears while the bridge is running. One extra
/// catalog query per `CREATE TABLE` — an event rare enough that the cost is irrelevant
/// and the silence was not.
pub fn reportTable(
    allocator: std.mem.Allocator,
    conn: *c.PGconn,
    table: []const u8,
    sync_rules: *const Config.EventClassification.TransitionRules,
    default_version_column: []const u8,
    /// Same role `run()` checks grants against. Null when ingress is unconfigured, in
    /// which case the query's own `pg_roles` guard makes every grant check a correct
    /// `false` — mirrors `reportVersionColumns`.
    writer_role: ?[]const u8,
    /// Records the combined verdict (§1.11) for `appendWriteContract` to publish.
    writable: *WritableTables.Registry,
) !void {
    if (isInternalTable(table)) return;

    // The role name is trusted config, safe to interpolate (same as
    // `reportVersionColumns`); the table name is not — it arrives from a DDL event, so
    // it stays a bound `$1` rather than being interpolated into SQL.
    const query = try utils.allocPrintZ(
        allocator,
        \\SELECT a.attname, t.typname, a.atttypmod, a.attnotnull,
        \\       CASE WHEN '{s}' <> '' AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{s}')
        \\            THEN has_table_privilege('{s}', cl.oid, 'INSERT') ELSE false END AS granted,
        \\       coalesce((
        \\         SELECT bool_or(pg_get_expr(ad.adbin, ad.adrelid) LIKE 'nextval%'
        \\                        OR pk.attidentity <> '')
        \\         FROM pg_index i
        \\         JOIN pg_attribute pk ON pk.attrelid = i.indrelid AND pk.attnum = ANY(i.indkey)
        \\         LEFT JOIN pg_attrdef ad ON ad.adrelid = pk.attrelid AND ad.adnum = pk.attnum
        \\         WHERE i.indrelid = cl.oid AND i.indisprimary
        \\       ), false) AS key_db_allocated
        \\FROM pg_class cl
        \\JOIN pg_namespace ns ON ns.oid = cl.relnamespace AND ns.nspname = 'public'
        \\JOIN pg_attribute a ON a.attrelid = cl.oid AND a.attnum > 0 AND NOT a.attisdropped
        \\JOIN pg_type t ON t.oid = a.atttypid
        \\WHERE cl.relname = $1
        \\  AND t.typname IN ('timestamp', 'timestamptz', 'int8', 'int4')
        \\ORDER BY a.attnum;
    ,
        .{ writer_role orelse "", writer_role orelse "", writer_role orelse "" },
    );
    defer allocator.free(query);

    const table_z = try allocator.dupeZ(u8, table);
    defer allocator.free(table_z);
    const values = [_][*c]const u8{table_z.ptr};

    const res = c.PQexecParams(conn, query.ptr, 1, null, &values, null, null, 0);
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        log.warn("⚠️  Version-column report skipped for '{s}': {s}", .{ table, c.PQerrorMessage(conn) });
        return;
    }

    const wanted: []const u8 = if (sync_rules.get(table)) |cols|
        (if (cols.len > 0) cols[0] else default_version_column)
    else
        default_version_column;

    var verdict: VersionVerdict = .absent;
    var granted = false;
    var key_db_allocated = false;
    var candidates: std.ArrayList([]const u8) = .empty;
    defer candidates.deinit(allocator);

    const rows: usize = @intCast(c.PQntuples(res));
    for (0..rows) |i| {
        const col = std.mem.span(c.PQgetvalue(res, @intCast(i), 0));
        const typname = std.mem.span(c.PQgetvalue(res, @intCast(i), 1));
        const typmod = std.fmt.parseInt(i32, std.mem.span(c.PQgetvalue(res, @intCast(i), 2)), 10) catch -1;
        const notnull = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, @intCast(i), 3)), "t");
        granted = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, @intCast(i), 4)), "t");
        key_db_allocated = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, @intCast(i), 5)), "t");

        if (std.mem.eql(u8, col, wanted)) {
            verdict = classifyVersionColumn(col, typname, typmod, notnull);
        } else if (!isCreationColumn(col)) {
            try candidates.append(allocator, col);
        }
    }

    // ⚠️ A table with no orderable column at all never enters the loop above, so
    // `granted`/`key_db_allocated` stay at their `false` default even if the grant
    // exists — the same blind spot `reportVersionColumns` has (its per-row read of the
    // same two flags never runs either, for the same reason). Harmless here: such a
    // table is `.absent` regardless, and `logVerdict` already returns false for that
    // case, so the registry ends up correctly `false` either way.
    const usable = logVerdict(table, wanted, verdict, candidates.items, granted);
    writable.set(table, usable and !key_db_allocated) catch |err| {
        log.warn("⚠️  Could not record writability for '{s}': {}", .{ table, err });
    };
}

/// Tables that are ours or the migration tool's — mirrors zebridge_is_internal_table()
/// in init.{core,write}.template.sql and isInternalTable() in event_processor.zig.
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
/// Does the data already in these tables still fit the buffer we are about to run with?
///
/// ⚠️ **`BASE_BUF` is a one-way door.** Raising it is free; lowering it retroactively
/// invalidates rows that were accepted under the old size, and nothing notices until
/// somebody *touches* one of them — at which point CDC cannot pack the row and the whole
/// table is suspended for every client. A configuration change today, an outage weeks
/// later, with nothing connecting the two.
///
/// The same query also catches the cases the ingress size guard cannot see, because they
/// were never mutations at all:
///
///   * rows written **directly to PostgreSQL** by the application, a cron job or a backfill;
///   * rows the database **expanded itself** — a large column `DEFAULT`, a `BEFORE` trigger,
///     a generated column — so that a 258-byte mutation stores a 50 KB row;
///   * data that predates the table's addition to the publication.
///
/// None of those can be prevented. All of them can be *named at boot* instead of
/// discovered at runtime, which is the whole difference between a migration you plan and
/// an incident you page for.
///
/// ⚠️ Cheap despite appearances: `octet_length` on `text`/`bytea` reads the length out of
/// the TOAST pointer without fetching the value — measured at 2 shared buffers and 0.07 ms
/// for a 50 MB table. Reported, never refused: the rows already exist, and refusing to
/// start would turn a warning into the outage it is trying to prevent.
fn checkStoredRowsFit(
    allocator: std.mem.Allocator,
    conn: ?*c.PGconn,
    publication_name: []const u8,
    event_buf_bytes: usize,
) void {
    // ⚠️ Asked per table rather than in one statement: reaching a table's own columns
    // needs its name in the query text, and the published list is normally a handful.
    const list_q = utils.allocPrintZ(
        allocator,
        "SELECT tablename FROM pg_publication_tables WHERE pubname = '{s}' AND schemaname = 'public'",
        .{publication_name},
    ) catch return;
    defer allocator.free(list_q);

    const list = c.PQexec(conn, list_q.ptr);
    defer c.PQclear(list);
    if (c.PQresultStatus(list) != c.PGRES_TUPLES_OK) return;

    var flagged: usize = 0;
    var r: c_int = 0;
    while (r < c.PQntuples(list)) : (r += 1) {
        const table = std.mem.span(c.PQgetvalue(list, r, 0));
        // ⚠️ `zebridge_widest_row`, not `pg_column_size`. The latter reports the *stored,
        // compressed* size — a 40 KB run of one character compresses to almost nothing, so
        // a check built on it called the table safe while CDC could not carry its widest
        // row. Measured: pg_column_size saw a fitting table; octet_length saw 40068 bytes.
        //
        // If the function is missing (init.sql not re-applied) the query errors and the
        // table is skipped, which is the right failure: a boot check that cannot run must
        // not stop a bridge from starting.
        const q = utils.allocPrintZ(
            allocator,
            "SELECT public.zebridge_widest_row('public.\"{s}\"'::regclass)",
            .{table},
        ) catch continue;
        defer allocator.free(q);

        const res = c.PQexec(conn, q.ptr);
        defer c.PQclear(res);
        if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK or c.PQntuples(res) == 0) continue;

        const widest = std.fmt.parseInt(usize, std.mem.span(c.PQgetvalue(res, 0, 0)), 10) catch continue;
        if (widest >= event_buf_bytes) {
            flagged += 1;
            log.err(
                "🔴 '{s}': a row already stored is {d} bytes, at or over the {d}-byte CDC event buffer. Touching that row suspends the table for every client. Raise BASE_BUF, or move the oversized column out of the replicated table — do NOT lower BASE_BUF below what is already stored.",
                .{ table, widest, event_buf_bytes },
            );
        }

        // ⚠️ And the hazard that has not happened yet. A table with an oversized column
        // DEFAULT and no rows measures as safe above, then the FIRST insert — a small
        // mutation that passed every ingress guard — stores an unsendable row. The default
        // is the fault; the row is only where it surfaces.
        const dq = utils.allocPrintZ(
            allocator,
            "SELECT col, bytes FROM public.zebridge_oversized_defaults('public.\"{s}\"'::regclass, {d})",
            .{ table, event_buf_bytes },
        ) catch continue;
        defer allocator.free(dq);

        const dres = c.PQexec(conn, dq.ptr);
        defer c.PQclear(dres);
        if (c.PQresultStatus(dres) != c.PGRES_TUPLES_OK) continue;

        var d: c_int = 0;
        while (d < c.PQntuples(dres)) : (d += 1) {
            flagged += 1;
            log.err(
                "🔴 '{s}.{s}': its column DEFAULT alone produces {s} bytes, at or over the {d}-byte CDC event buffer. No row need exist yet — the first INSERT that omits this column stores a row the change feed cannot carry, and the table is suspended for every client. Change the default, or move the column out of the replicated table.",
                .{ table, c.PQgetvalue(dres, d, 0), c.PQgetvalue(dres, d, 1), event_buf_bytes },
            );
        }
    }

    if (flagged == 0) {
        log.info("📏 Stored rows and column defaults fit the {d}-byte event buffer", .{event_buf_bytes});
    }
}

/// Is the DDL pipeline actually wired up? — see PROTOCOL.md §0
///
/// ⚠️ **Single point of failure for every schema change.** `zebridge_ddl_events` is an
/// ordinary published table: the event trigger writes a row, and that row reaches the
/// bridge only because the publication carries it. Take it out of the publication and
/// **nothing fails** — the trigger still fires, the row is still written, CDC keeps
/// flowing for every other table, and the bridge simply never hears about a migration
/// again. Clients keep the schema they last received, and the write path keeps the
/// catalog facts it last read.
///
/// This is not hypothetical: `ALTER PUBLICATION … SET TABLE x` **replaces** the table
/// list rather than adding to it, which silently unpublished this table once already.
///
/// Warned, not refused: a bridge that will not start is worse than one that says its
/// schema pipeline is deaf. `strict` is deliberately not consulted — an operator who
/// asked for lenient checks did not ask to lose migrations silently.
fn checkDdlPipeline(allocator: std.mem.Allocator, conn: ?*c.PGconn, publication_name: []const u8) void {
    const query = utils.allocPrintZ(
        allocator,
        \\SELECT
        \\  EXISTS (SELECT 1 FROM pg_publication_tables
        \\          WHERE pubname = '{s}' AND schemaname = 'public'
        \\            AND tablename = 'zebridge_ddl_events'),
        \\  EXISTS (SELECT 1 FROM pg_event_trigger
        \\          WHERE evtname = 'zebridge_ddl_trigger' AND evtenabled <> 'D');
    ,
        .{publication_name},
    ) catch return;
    defer allocator.free(query);

    const res = c.PQexec(conn, query.ptr);
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK or c.PQntuples(res) == 0) return;

    const published = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, 0, 0)), "t");
    const trigger_on = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, 0, 1)), "t");

    if (!published) {
        log.err(
            "🔴 'zebridge_ddl_events' is NOT in publication '{s}'. Schema changes will never reach the bridge: clients keep the schema they last received and the write path keeps the catalog facts it last read, both silently and both until a restart. Fix: ALTER PUBLICATION {s} ADD TABLE public.zebridge_ddl_events;",
            .{ publication_name, publication_name },
        );
    }
    if (!trigger_on) {
        log.err(
            "🔴 Event trigger 'zebridge_ddl_trigger' is missing or disabled, so no DDL is captured at all. Re-apply init.sql, or: ALTER EVENT TRIGGER zebridge_ddl_trigger ENABLE;",
            .{},
        );
    }
    if (published and trigger_on) {
        log.info("🔧 DDL pipeline: trigger enabled and 'zebridge_ddl_events' published", .{});
    }
}

pub fn run(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publication_name: []const u8,
    transition_rules: *const Config.EventClassification.TransitionRules,
    sync_rules: *const Config.EventClassification.TransitionRules,
    default_version_column: []const u8,
    strict: bool,
    refused: *RefusedTables.Registry,
    /// Role the write grants were made to, from `DATABASE_WRITER_URL`. Null when ingress
    /// is not configured, in which case no table is expected to be writable.
    writer_role: ?[]const u8,
    /// `TENANT_RULES`: which column carries the tenant, per table. Empty when reads are
    /// unscoped, which is the default and is reported as such.
    tenant_rules: *const Config.EventClassification.TransitionRules,
    /// The CDC per-event buffer, `2^BASE_BUF`. Compared against the widest row already
    /// stored — see `checkStoredRowsFit`.
    event_buf_bytes: usize,
    /// Records the per-table edge-writability verdict (§1.11) for `appendWriteContract`
    /// to publish.
    writable: *WritableTables.Registry,
    /// the catalogue's public set — what `isCdcRoutable` checks an untenanted table against.
    topology: *const Topology.Topology,
) !Summary {
    var standard_config = pg_config.*;
    standard_config.replication = false;

    const conn = pg_conn.connect(allocator, standard_config) catch |err| {
        log.warn("⚠️  Preflight skipped: could not connect to PostgreSQL ({})", .{err});
        return Summary{};
    };
    defer c.PQfinish(conn);

    checkDdlPipeline(allocator, conn, publication_name);
    checkStoredRowsFit(allocator, conn, publication_name, event_buf_bytes);

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

    // A second pass, over every published table regardless of PK/identity findings —
    // CDC-routability is orthogonal to those. Checked at boot so a table the catalogue
    // does not mark public (and not tenant-scoped) is refused before the first WAL byte
    // arrives, the same reasoning `no_primary_key` above already uses: publishing to an
    // unrouted subject blocks for the full publish timeout rather than failing fast,
    // and enough of that exhausts the bridge's retry budget and self-terminates.
    r = 0;
    while (r < @as(c_int, @intCast(rows))) : (r += 1) {
        const table = std.mem.span(c.PQgetvalue(res, r, 0));
        if (isInternalTable(table)) continue;
        // `zebridge_user_tenants` is exempt for the same reason `event_processor.zig`'s
        // runtime check exempts it: its rows are transformed into
        // `$KV.tenants.<principal>` by a dedicated path, never `cdc.<table>.<op>`.
        if (std.mem.eql(u8, table, "zebridge_user_tenants")) continue;
        if (topology.isCdcRoutable(table, tenant_rules.contains(table))) continue;

        summary.with_findings += 1;
        summary.refused += 1;
        refused.refuse(table, .no_cdc_subject) catch |err| {
            log.err("🔴 Could not record refusal for '{s}': {}", .{ table, err });
            continue;
        };
        log.err(
            "🔴 REFUSING '{s}': not tenant-scoped and no catalogue row marks it public — its CDC subject matches no stream. Publishing would block on every write. Declare it (zebridge_enable with public_reason or tenant_col) and restart the bridge.",
            .{table},
        );
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
    reportVersionColumns(allocator, conn, publication_name, sync_rules, default_version_column, writer_role, writable) catch |err| {
        log.warn("⚠️  Version-column report failed: {}", .{err});
    };

    // Last, because a table that cannot replicate at all does not need its read scoping
    // checked — and because this one *refuses* tables, so it must run after the shape
    // checks that might already have refused them for a better reason.
    reportTenantColumns(allocator, conn, publication_name, tenant_rules, refused) catch |err| {
        log.warn("⚠️  Tenant-column report failed: {}", .{err});
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

test "roleFromUrl: the ordinary case" {
    try std.testing.expectEqualStrings(
        "bridge_writer",
        preflightRole("postgres://bridge_writer:pw@127.0.0.1:55432/postgres").?,
    );
}

test "roleFromUrl: a password containing '@' does not move the split" {
    // The authority is split on the LAST '@'. Splitting on the first would yield the
    // role "bridge_writer:pw" and every has_table_privilege check would come back false
    // — reporting every writable table as unwritable, which reads like a real finding.
    try std.testing.expectEqualStrings(
        "bridge_writer",
        preflightRole("postgres://bridge_writer:p@ss@host/db").?,
    );
}

test "roleFromUrl: a password containing ':' does not move the split" {
    try std.testing.expectEqualStrings(
        "bridge_writer",
        preflightRole("postgres://bridge_writer:a:b:c@host/db").?,
    );
}

test "roleFromUrl: no userinfo yields null rather than a host name" {
    // Returning "host" here would check privileges for a role that does not exist, and
    // the query's pg_roles guard would then report every table as ungranted.
    try std.testing.expect(preflightRole("postgres://host:5432/db") == null);
    try std.testing.expect(preflightRole("postgres://@host/db") == null);
    try std.testing.expect(preflightRole("postgres://:pw@host/db") == null);
}

test "roleFromUrl: a role with no password" {
    try std.testing.expectEqualStrings("writer", preflightRole("postgres://writer@host/db").?);
}

test "roleFromUrl: an '@' in the path is not authority" {
    try std.testing.expectEqualStrings("w", preflightRole("postgres://w:p@host/db@name").?);
}

test "roleFromUrl: not a URL" {
    try std.testing.expect(preflightRole("bridge_writer") == null);
}

fn preflightRole(url: []const u8) ?[]const u8 {
    return roleFromUrl(url);
}
