//! The tombstone GC sidecar. Sweeps every table whose `zebridge_catalogue` row
//! declares a `tombstone_col` (plus any `SYNC_RULES` override), reaping tombstones
//! older than `GC_THRESHOLD_MS`, then publishes the watermark.
//!
//! `SWEEP_ONLY_TABLES` (comma list) narrows one run to the named tables — a SCOPED
//! run for tests (`scripts/scenarios/sweeper.py` sets it to its own fixture so a test
//! pass never reaps a real table's tombstones). Unset in production: a sweeper that
//! silently skips tables lets their tombstones pile up and the GC watermark lie.
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
    // Optional OVERRIDE, no longer required: the sweep set's source of truth is
    // `zebridge_catalogue.tombstone_col`, read below on the same connection that
    // sweeps — written by `zebridge_enable` atomically with the tombstone trigger
    // itself, so the sweeper and the guard cannot disagree about which column it
    // is. An env entry still wins for its table (same contract as the bridge).
    const sync_rules_str = env.getPosix("SYNC_RULES");

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

    if (sync_rules_str) |srs| {
    var rule_it = std.mem.splitScalar(u8, srs, ';');
    while (rule_it.next()) |rule| {
        const trimmed = std.mem.trim(u8, rule, " ");
        if (trimmed.len == 0) continue;
        const colon = std.mem.indexOfScalar(u8, trimmed, ':') orelse continue;
        const table = std.mem.trim(u8, trimmed[0..colon], " ");
        const cols = trimmed[colon + 1 ..];
        // First column is the version, the optional second is the tombstone.
        // ⚠️ The SECOND field, bounded on both sides. This took everything after the first
        // comma, which was correct while `SYNC_RULES` had at most two columns and broke
        // silently the day a third (the tiebreak column) was added: it parsed
        // `updated_at,deleted_at,last_writer` into a tombstone called
        // "deleted_at,last_writer" and every sweep failed with
        // `column "deleted_at,last_writer" does not exist` — visible only in the sidecar's
        // own output, while the bridge looked healthy.
        var field_it = std.mem.splitScalar(u8, cols, ',');
        _ = field_it.next(); // the version column
        const tombstone = std.mem.trim(u8, field_it.next() orelse continue, " ");
        if (table.len == 0 or tombstone.len == 0) continue;
        try sweeps.append(allocator, .{ .table = table, .tombstone = tombstone });
    }
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


    // ── The sweep set, from the catalogue ───────────────────────────────────────
    //
    // `zebridge_catalogue.tombstone_col` is written in the same transaction as the
    // soft-delete trigger it names, so this read cannot disagree with the guard. A
    // table the env named above keeps its env columns (override); everything else
    // comes from here. A pre-catalogue database returns an error result and the env
    // set stands alone — graceful, like the bridge's own loader.
    {
        const cres = c.PQexec(pg_conn, "SELECT tbl, tombstone_col::text FROM public.zebridge_catalogue" ++
            " WHERE tombstone_col IS NOT NULL ORDER BY tbl");
        defer c.PQclear(cres);
        if (c.PQresultStatus(cres) == c.PGRES_TUPLES_OK) {
            const n: usize = @intCast(c.PQntuples(cres));
            var added: usize = 0;
            rows: for (0..n) |i| {
                const tbl = std.mem.span(c.PQgetvalue(cres, @intCast(i), 0));
                const tomb = std.mem.span(c.PQgetvalue(cres, @intCast(i), 1));
                if (tbl.len == 0 or tomb.len == 0) continue;
                for (sweeps.items) |sw| {
                    if (std.mem.eql(u8, sw.table, tbl)) continue :rows; // env override wins
                }
                try sweeps.append(allocator, .{
                    .table = try allocator.dupe(u8, tbl),
                    .tombstone = try allocator.dupe(u8, tomb),
                });
                added += 1;
            }
            std.debug.print("GC: catalogue supplied {d} sweep table(s)\n", .{added});
        } else {
            std.debug.print("GC: no zebridge_catalogue (pre-catalogue database?) — env SYNC_RULES only\n", .{});
        }
    }

    // ── SWEEP_ONLY_TABLES: a scoped run ─────────────────────────────────────────
    //
    // Applied AFTER the set is built from both sources, so it is a pure filter — it
    // cannot add a table the catalogue or SYNC_RULES did not declare, and a name that
    // matches nothing sweeps nothing (reported below as "nothing to sweep").
    if (env.getPosix("SWEEP_ONLY_TABLES")) |only| {
        var kept = std.ArrayList(Sweep).empty;
        for (sweeps.items) |sw| {
            var it = std.mem.splitScalar(u8, only, ',');
            while (it.next()) |raw| {
                if (std.mem.eql(u8, std.mem.trim(u8, raw, " "), sw.table)) {
                    try kept.append(allocator, sw);
                    break;
                }
            }
        }
        std.debug.print("GC: SWEEP_ONLY_TABLES='{s}' — scoped run, {d} of {d} table(s) kept\n", .{ only, kept.items.len, sweeps.items.len });
        sweeps.deinit(allocator);
        sweeps = kept;
    }

    if (sweeps.items.len == 0) {
        std.debug.print(
            "Neither the catalogue nor SYNC_RULES declares a tombstone column, so there " ++
                "is nothing to sweep. Deletes on those tables are physical, and an offline " ++
                "client's queued edit can resurrect a row (PROTOCOL.md \u{00a7}7.5).\n",
            .{},
        );
        return;
    }
    for (sweeps.items) |sw| {
        std.debug.print("GC: sweeping {s} on tombstone column '{s}'\n", .{ sw.table, sw.tombstone });
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

    const setup_connection = struct {
        fn do(a: std.mem.Allocator, conn: *c.PGconn, p: []const u8, swps: []const Sweep, drun: bool) !void {
            const tz = c.PQexec(conn, "SET TIME ZONE 'UTC'");
            defer c.PQclear(tz);
            if (c.PQresultStatus(tz) != c.PGRES_COMMAND_OK) return error.InitFailed;
            
            const p_z = try utils.allocPrintZ(a, "{s}", .{p});
            defer a.free(p_z);
            const params = [_]?[*:0]const u8{p_z.ptr};
            const p_res = c.PQexecParams(conn, "SELECT set_config('zb.principal', $1, false)", 1, null, &params[0], null, null, 0);
            defer c.PQclear(p_res);
            if (c.PQresultStatus(p_res) != c.PGRES_TUPLES_OK) return error.InitFailed;

            for (swps, 0..) |sw, i| {
                const stmt_name = try utils.allocPrintZ(a, "gc_sweep_{d}", .{i});
                defer a.free(stmt_name);
                const sql = if (drun)
                    try utils.allocPrintZ(a, "SELECT count(*) FROM \"{s}\" WHERE \"{s}\" IS NOT NULL AND \"{s}\"::timestamptz < now() - make_interval(secs => $1::double precision);", .{ sw.table, sw.tombstone, sw.tombstone })
                else
                    try utils.allocPrintZ(a, "DELETE FROM \"{s}\" WHERE \"{s}\" IS NOT NULL AND \"{s}\"::timestamptz < now() - make_interval(secs => $1::double precision);", .{ sw.table, sw.tombstone, sw.tombstone });
                defer a.free(sql);
                const res = c.PQprepare(conn, stmt_name.ptr, sql.ptr, 1, null);
                defer c.PQclear(res);
                if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) return error.PrepareFailed;
            }
            const wm_res = c.PQprepare(conn, "gc_watermark_update", "UPDATE public.zebridge_gc_watermark SET watermark = now() - make_interval(secs => $1::double precision), threshold_ms = $2::bigint, reaped = $3::bigint, swept_at = now(), updated_at = now() WHERE id = 1", 3, null);
            defer c.PQclear(wm_res);
            if (c.PQresultStatus(wm_res) != c.PGRES_COMMAND_OK) return error.PrepareFailed;
        }
    }.do;
    setup_connection(allocator, pg_conn.?, principal, sweeps.items, dry_run) catch |err| {
        std.debug.print("FATAL: failed to initialize connection: {any}\n", .{err});
        return;
    };
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
    // The GRANT is automatic now: a trigger on `zebridge_user_tenants` maps this
    // principal to every tenant the moment any principal is mapped to it
    // (`zebridge_sweeper_autogrant_t`, init.write.template.sql), so a tenant entering
    // through the normal door — a DBA INSERT or `/enroll` — is never unswept. What the
    // trigger cannot see is a tenant that exists only as DATA (rows an admin inserted
    // under a tenant_id nobody is mapped to): no mapping insert, no trigger. For those
    // the audit remains, where the reach is granted, run by a DBA:
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
            setup_connection(allocator, pg_conn.?, principal, sweeps.items, dry_run) catch |err| {
                std.debug.print("ERROR: failed to initialize reconnected session: {any}\n", .{err});
                utils.sleep(5 * std.time.ns_per_s);
                continue;
            };
            std.debug.print("Reconnected and initialized.\n", .{});
        }

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const aa = arena.allocator();

        var reaped_this_pass: u64 = 0;

        for (sweeps.items, 0..) |sw, i| {
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
            // the catalogue / SYNC_RULES, which is operator input.
            // `GC_DRY_RUN=1` counts instead of deleting. There is no undo on this path, so
            // the only safe way to change a threshold on a live database is to see what the
            // new one would take first.
            const stmt_name = try utils.allocPrintZ(aa, "gc_sweep_{d}", .{i});
            const secs = try utils.allocPrintZ(aa, "{d}", .{threshold_ms / 1000});
            const param_vals = [_]?[*:0]const u8{secs.ptr};
            const res = c.PQexecPrepared(pg_conn, stmt_name.ptr, 1, &param_vals[0], null, null, 0);
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
                reaped_this_pass += std.fmt.parseInt(u64, std.mem.span(rows_deleted), 10) catch 0;
                if (rows_deleted[0] != 0 and rows_deleted[0] != '0') {
                    std.debug.print(
                        "GC: reaped {s} tombstone(s) from {s} older than {d}ms\n",
                        .{ rows_deleted, sw.table, threshold_ms },
                    );
                }
            }
        }

        // ── Publish the watermark ───────────────────────────────────────────────
        //
        // The point of the whole sweep, from a client's perspective. `GC_THRESHOLD_MS` is
        // the maximum offline window this deployment supports, and until this row existed
        // a client had no way to locate that line — it could only assume the sweeper was
        // keeping up. A client whose oldest queued write predates `watermark` must
        // re-seed instead of flushing, or it resurrects rows deleted while it was away.
        //
        // ⚠️ Written **after** the deletes, never before. The guarantee is "nothing older
        // than this survives", so publishing first would advertise a line the sweep had
        // not yet reached — and a client trusting it would discard writes that were still
        // safe.
        //
        // `now()` is the server's, not this process's: the sweep's cutoff was computed
        // server-side for the same reason (a drifted host clock must not move the line).
        //
        // No NATS here. The row is replicated by CDC like any other, so every client that
        // is already consuming changes receives it with no new subscription — and the
        // sweeper stays a PostgreSQL client with no broker identity to compromise.
        //
        // A failure is logged and the loop continues: an unpublished watermark leaves
        // clients on the previous, *older* value, which is conservative. Stopping the
        // sweep over it would let tombstones accumulate instead, which is not.
        {
            const wm_secs = try utils.allocPrintZ(aa, "{d}", .{threshold_ms / 1000});
            const wm_ms = try utils.allocPrintZ(aa, "{d}", .{threshold_ms});
            const wm_reaped = try utils.allocPrintZ(aa, "{d}", .{reaped_this_pass});
            const wm_params = [_]?[*:0]const u8{ wm_secs.ptr, wm_ms.ptr, wm_reaped.ptr };

            const wm_res = c.PQexecPrepared(pg_conn, "gc_watermark_update", 3, &wm_params[0], null, null, 0);
            defer c.PQclear(wm_res);

            if (c.PQresultStatus(wm_res) != c.PGRES_COMMAND_OK) {
                std.debug.print(
                    "GC: WARNING could not publish the watermark: {s}\n",
                    .{c.PQerrorMessage(pg_conn)},
                );
            } else if (std.mem.eql(u8, std.mem.span(c.PQcmdTuples(wm_res)), "0")) {
                std.debug.print(
                    "GC: WARNING the watermark row is missing (zebridge_gc_watermark id=1)." ++
                        " Clients cannot tell how far back tombstones survive. Re-run init.sql.\n",
                    .{},
                );
            }
        }

        utils.sleep(interval_ms * std.time.ns_per_ms);
    }
}
