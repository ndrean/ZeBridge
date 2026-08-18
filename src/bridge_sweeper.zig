const std = @import("std");
const c = @import("c_imports.zig").c;
const utils = @import("utils.zig");

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.c_allocator;

    std.debug.print("ZeBridge GC Sidecar Starting...\n", .{});

    // Read Env Vars
    //
    // A URL, like the bridge — and the *writer* one, because this sidecar DELETEs. It
    // used to assemble `host=… user=… password=…` from PG_HOST/PG_USER/PG_PASSWORD,
    // which are the superuser credentials init.sql runs under: a tombstone sweeper
    // running as `postgres` can delete anything in the database, and nothing in its
    // output would say so.
    const env = init.minimal.environ;
    const db_url = env.getPosix("DATABASE_WRITER_URL") orelse {
        std.debug.print(
            "FATAL: DATABASE_WRITER_URL is required (postgres://bridge_writer:...@host:port/db). " ++
                "There is no PG_HOST/PG_USER fallback: this process deletes rows, and must not be " ++
                "able to do it as the admin.\n",
            .{},
        );
        return;
    };

    // The sweep set comes from SYNC_RULES, not from a list of its own.
    //
    // ⚠️ This used to read `GC_TABLES` and delete `WHERE _deleted = true AND _hlc < $1` —
    // columns from an older HLC-based design that **no table has**. So the sweeper either
    // errored per table or matched nothing, and the GC watermark PROTOCOL.md §7.5 promises
    // (the maximum offline window a client may have) was not enforced at all.
    //
    // Deriving the set from SYNC_RULES makes that impossible to repeat: the bridge and the
    // sweeper read the same variable, so they cannot disagree about which column is the
    // tombstone. A table with no tombstone is not swept — its deletes are physical and
    // there is nothing to reap.
    const sync_rules_str = env.getPosix("SYNC_RULES") orelse {
        std.debug.print(
            "FATAL: SYNC_RULES is required (e.g. orders:updated_at,deleted_at). It names the " ++
                "tombstone column per table; without it there is nothing to sweep. GC_TABLES is " ++
                "no longer read — it could name a table whose tombstone column this process " ++
                "would then have to guess.\n",
            .{},
        );
        return;
    };

    // ── The threshold, and why it is guarded ────────────────────────────────────
    //
    // This number is a **promise to clients**: it is the maximum offline window the
    // deployment supports (PROTOCOL.md §7.5). A client offline longer than this can
    // resurrect a deleted row, because the tombstone that would have overruled its queued
    // edit is gone. Shortening it does not fail — it silently narrows that guarantee.
    //
    // The realistic accident is units, not malice. `GC_THRESHOLD_MS=3600` looks like an
    // hour and means **3.6 seconds**: every soft delete older than a few seconds is reaped
    // on the next pass, and nothing in the output says the window was wrong. `0` means
    // "reap everything ever soft-deleted, now".
    //
    // ⚠️ There is no undo. A reaped tombstone is a deleted row: the delete already
    // happened, and what is lost is the *evidence* that overrules a late writer.
    const threshold_ms_str = env.getPosix("GC_THRESHOLD_MS") orelse "3600000"; // 1 hour
    const threshold_ms = std.fmt.parseInt(u64, threshold_ms_str, 10) catch {
        std.debug.print("FATAL: GC_THRESHOLD_MS is not a number: '{s}'\n", .{threshold_ms_str});
        return;
    };

    const dry_run = env.getPosix("GC_DRY_RUN") != null;
    if (dry_run) std.debug.print("GC: DRY RUN — counting only, nothing will be deleted\n", .{});

    const min_threshold_ms: u64 = 60_000; // one minute
    if (threshold_ms < min_threshold_ms) {
        std.debug.print(
            "FATAL: GC_THRESHOLD_MS={d} is below the {d}ms floor.\n" ++
                "  This is the maximum offline window clients may have — below a minute it is\n" ++
                "  almost certainly a units mistake (3600 is 3.6 SECONDS, not an hour), and the\n" ++
                "  cost is silent: tombstones vanish and a late client resurrects deleted rows.\n" ++
                "  Set GC_ALLOW_SHORT_THRESHOLD=1 if a sub-minute window is genuinely intended.\n",
            .{ threshold_ms, min_threshold_ms },
        );
        if (env.getPosix("GC_ALLOW_SHORT_THRESHOLD") == null) return;
        std.debug.print("GC: proceeding anyway — GC_ALLOW_SHORT_THRESHOLD is set\n", .{});
    }

    // Refuses to start rather than reaping under a threshold nobody chose.
    if (env.getPosix("GC_THRESHOLD_MS") == null) {
        std.debug.print(
            "GC: GC_THRESHOLD_MS not set, using the {d}ms default. This is the maximum\n" ++
                "    offline window clients may have — set it explicitly in production.\n",
            .{threshold_ms},
        );
    }

    const interval_ms_str = env.getPosix("GC_INTERVAL_MS") orelse "60000";
    const interval_ms = try std.fmt.parseInt(u64, interval_ms_str, 10);

    // `table:version[,tombstone];table:version[,tombstone]` — the same grammar the bridge
    // parses. Only entries carrying a tombstone are swept.
    const Sweep = struct { table: []const u8, tombstone: []const u8 };
    var sweeps = std.ArrayList(Sweep).empty;
    defer sweeps.deinit(allocator);

    var rule_it = std.mem.splitScalar(u8, sync_rules_str, ';');
    while (rule_it.next()) |rule| {
        const trimmed = std.mem.trim(u8, rule, " ");
        if (trimmed.len == 0) continue;
        const colon = std.mem.indexOfScalar(u8, trimmed, ':') orelse continue;
        const table = std.mem.trim(u8, trimmed[0..colon], " ");
        const cols = trimmed[colon + 1 ..];
        // First column is the version, the optional second is the tombstone.
        const comma = std.mem.indexOfScalar(u8, cols, ',') orelse continue;
        const tombstone = std.mem.trim(u8, cols[comma + 1 ..], " ");
        if (table.len == 0 or tombstone.len == 0) continue;
        try sweeps.append(allocator, .{ .table = table, .tombstone = tombstone });
    }

    if (sweeps.items.len == 0) {
        std.debug.print(
            "No table in SYNC_RULES declares a tombstone column, so there is nothing to " ++
                "sweep. Deletes on those tables are physical, and an offline client's queued " ++
                "edit can resurrect a row (PROTOCOL.md §7.5).\n",
            .{},
        );
        return;
    }
    for (sweeps.items) |sw| {
        std.debug.print("GC: sweeping {s} on tombstone column '{s}'\n", .{ sw.table, sw.tombstone });
    }

    // Connect to PostgreSQL
    const conninfo = try utils.allocPrintZ(allocator, "{s}", .{db_url});
    defer allocator.free(conninfo);

    const pg_conn = c.PQconnectdb(conninfo.ptr);
    if (c.PQstatus(pg_conn) != c.CONNECTION_OK) {
        std.debug.print("FATAL: Failed to connect to PostgreSQL: {s}\n", .{c.PQerrorMessage(pg_conn)});
        c.PQfinish(pg_conn);
        return;
    }
    defer c.PQfinish(pg_conn);

    // Pinned, so a naive tombstone column is read as UTC rather than as whatever the
    // server's default zone happens to be. Every writer is required to store UTC (§7.3);
    // this makes the sweeper agree with them instead of with the machine.
    {
        const tz = c.PQexec(pg_conn, "SET TIME ZONE 'UTC'");
        defer c.PQclear(tz);
    }

    // ── The sweeper's identity ──────────────────────────────────────────────────
    //
    // Row-level security applies to this connection too, and a background process acting
    // for nobody sees **nothing**: `current_setting('zb.principal')` is unset, the tenant
    // predicate is NULL, and the sweep silently reaps zero rows while reporting success.
    // Measured before this existed: 0 of 4 rows.
    //
    // So it declares a principal, like every other writer. `SWEEPER_PRINCIPAL` must appear
    // in `zebridge_user_tenants` against each tenant it may reap:
    //
    //     INSERT INTO zebridge_user_tenants (principal, tenant_id)
    //     VALUES ('zb_sweeper', 'acme'), ('zb_sweeper', 'globex');
    //
    // ⚠️ **Not a policy that exempts principal-less sessions.** That was the first fix and
    // it was wrong: `USING (current_setting('zb.principal', true) IS NULL)` opens the
    // whole table to anything that reaches this connection *without* setting a principal —
    // so forgetting to set one became the unsafe default, which is backwards, and the
    // sweeper's reach became invisible to `SELECT * FROM zebridge_user_tenants`. A named
    // principal fails closed on omission (0 rows) and is auditable in the same table as
    // every other writer.
    //
    // It also needs no new policy at all: the existing tenant policy already admits it.
    // And it is still bounded — `zb_sweeper` writing into a tenant outside its mapping is
    // refused exactly as alice is.
    const principal = env.getPosix("SWEEPER_PRINCIPAL") orelse "zb_sweeper";
    {
        // Session-level, not `SET LOCAL`: this connection does one thing, and the setting
        // must survive every sweep. `set_config(..., false)` is the session form.
        const principal_z = utils.allocPrintZ(allocator, "{s}", .{principal}) catch return;
        defer allocator.free(principal_z);
        const params = [_]?[*:0]const u8{principal_z.ptr};
        const res = c.PQexecParams(pg_conn, "SELECT set_config('zb.principal', $1, false)", 1, null, &params[0], null, null, 0);
        defer c.PQclear(res);
        if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
            std.debug.print("FATAL: could not set the sweeper principal: {s}\n", .{c.PQerrorMessage(pg_conn)});
            return;
        }
    }
    std.debug.print("GC: acting as principal '{s}'\n", .{principal});

    // ⚠️ A tenant nobody mapped to the sweeper is invisible to it, and the symptom is
    // silence: tombstones accumulate, the GC watermark quietly stops holding for those
    // rows, and nothing fails. That is the price of a named principal over an open policy.
    //
    // This process **cannot report that gap itself** — tried, and it is impossible by
    // construction: RLS hides the unmapped rows from the warning query exactly as it hides
    // them from the DELETE. Measured: the same query returned "0 unmapped tenants" as the
    // sweeper and "1" as the admin. A blind spot cannot survey itself.
    //
    // So the audit lives where the reach is granted, not where it is used —
    // `zebridge_audit_publications()`'s neighbour in init.sql.template, run by a DBA:
    //
    //     SELECT DISTINCT t.tenant_id FROM <table> t
    //     WHERE NOT EXISTS (SELECT 1 FROM zebridge_user_tenants m
    //                       WHERE m.principal = 'zb_sweeper' AND m.tenant_id = t.tenant_id);
    //
    // Run loop
    while (true) {
        if (c.PQstatus(pg_conn) == c.CONNECTION_BAD) {
            std.debug.print("WARN: Connection lost. Reconnecting...\n", .{});
            c.PQreset(pg_conn);
            if (c.PQstatus(pg_conn) != c.CONNECTION_OK) {
                std.debug.print("ERROR: Reconnect failed: {s}\n", .{c.PQerrorMessage(pg_conn)});
                utils.sleep(5 * std.time.ns_per_s);
                continue;
            }
            std.debug.print("Reconnected.\n", .{});
        }

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const aa = arena.allocator();

        for (sweeps.items) |sw| {
            // The cutoff is computed by PostgreSQL, not here: `now()` on the server is the
            // only clock both sides agree on, and a sweeper whose host clock has drifted
            // would otherwise reap tombstones early — which is exactly the window a client
            // needs to still be overruled rather than resurrecting a row.
            //
            // `::timestamptz` because the tombstone may be a naive `timestamp` (Ecto's
            // `timestamps()` produces one). The session is pinned to UTC below, so the cast
            // reads a naive value as UTC — which is what every writer is required to store
            // (PROTOCOL.md §7.3).
            //
            // Identifiers are quoted rather than interpolated bare: they come from
            // SYNC_RULES, which is operator input.
            // `GC_DRY_RUN=1` counts instead of deleting. There is no undo on this path, so
            // the only safe way to change a threshold on a live database is to see what the
            // new one would take first.
            const sql = if (dry_run)
                try utils.allocPrintZ(
                    aa,
                    "SELECT count(*) FROM \"{s}\" WHERE \"{s}\" IS NOT NULL" ++
                        " AND \"{s}\"::timestamptz < now() - make_interval(secs => $1::double precision);",
                    .{ sw.table, sw.tombstone, sw.tombstone },
                )
            else
                try utils.allocPrintZ(
                    aa,
                    "DELETE FROM \"{s}\" WHERE \"{s}\" IS NOT NULL" ++
                        " AND \"{s}\"::timestamptz < now() - make_interval(secs => $1::double precision);",
                    .{ sw.table, sw.tombstone, sw.tombstone },
                );

            const secs = try utils.allocPrintZ(aa, "{d}", .{threshold_ms / 1000});
            const param_vals = [_]?[*:0]const u8{secs.ptr};

            const res = c.PQexecParams(pg_conn, sql.ptr, 1, null, &param_vals[0], null, null, 0);
            defer c.PQclear(res);

            if (dry_run) {
                if (c.PQresultStatus(res) == c.PGRES_TUPLES_OK and c.PQntuples(res) > 0) {
                    std.debug.print(
                        "GC: [dry run] WOULD reap {s} tombstone(s) from {s} older than {d}ms\n",
                        .{ c.PQgetvalue(res, 0, 0), sw.table, threshold_ms },
                    );
                }
                continue;
            }

            if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
                std.debug.print("ERROR: GC failed for table {s}: {s}\n", .{ sw.table, c.PQerrorMessage(pg_conn) });
            } else {
                const rows_deleted = c.PQcmdTuples(res);
                if (rows_deleted[0] != 0 and rows_deleted[0] != '0') {
                    std.debug.print(
                        "GC: reaped {s} tombstone(s) from {s} older than {d}ms\n",
                        .{ rows_deleted, sw.table, threshold_ms },
                    );
                }
            }
        }

        utils.sleep(interval_ms * std.time.ns_per_ms);
    }
}
