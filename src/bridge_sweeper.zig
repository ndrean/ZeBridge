//! The tombstone GC sidecar. Sweeps every table whose `zebridge_catalogue` row
//! declares a `tombstone_col`, reaping tombstones
//! older than `GC_THRESHOLD_MS`, then publishes the watermark.
//!
//! `SWEEP_ONLY_TABLES` (comma list) narrows one run to the named tables — a SCOPED
//! run for tests (`scripts/scenarios/sweeper.py` sets it to its own fixture so a test
//! pass never reaps a real table's tombstones). Unset in production: a sweeper that
//! silently skips tables lets their tombstones pile up and the GC watermark lie.
const std = @import("std");
const c = @import("c_imports.zig").c;
const utils = @import("utils.zig");

const log = std.log.scoped(.sweeper);

/// LOG_LEVEL filters at run time, as in the bridge: every level is compiled in.
var runtime_log_level: std.log.Level = .info;

pub const std_options = std.Options{
    .log_level = .debug,
    .logFn = logFn,
};

fn logFn(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.EnumLiteral),
    comptime format: []const u8,
    args: anytype,
) void {
    if (@intFromEnum(level) > @intFromEnum(runtime_log_level)) return;
    std.log.defaultLog(level, scope, format, args);
}

/// Set by SIGINT/SIGTERM: the loop ends at its next wake, every defer runs, and in Debug
/// the allocator reports what was never freed.
var stop = std.atomic.Value(bool).init(false);

fn onSignal(_: std.posix.SIG) callconv(.c) void {
    stop.store(true, .release);
}

/// The interval, slept in one-second steps so a signal ends the wait.
fn sleepUnlessStopped(ms: u64) void {
    var left = ms;
    while (left > 0 and !stop.load(.acquire)) {
        const step: u64 = @min(left, 1000);
        utils.sleep(step * std.time.ns_per_ms);
        left -= step;
    }
}

fn levelFrom(raw: ?[]const u8) std.log.Level {
    const v = raw orelse return .info;
    if (std.mem.eql(u8, v, "debug")) return .debug;
    if (std.mem.eql(u8, v, "warn") or std.mem.eql(u8, v, "warning")) return .warn;
    if (std.mem.eql(u8, v, "err") or std.mem.eql(u8, v, "error")) return .err;
    return .info;
}

const Sweep = struct { table: []const u8, tombstone: []const u8 };

fn freeSweeps(a: std.mem.Allocator, list: *std.ArrayList(Sweep)) void {
    for (list.items) |sw| {
        a.free(sw.table);
        a.free(sw.tombstone);
    }
    list.deinit(a);
}

/// The sweep set as the catalogue declares it now, and how many tables it declared before
/// `SWEEP_ONLY_TABLES` narrowed it. Null when the catalogue cannot be read.
///
/// Children BEFORE parents (§10dn): a reap is a physical DELETE, and a parent whose
/// tombstoned children are still rows is refused by NO ACTION — parent-first order costs
/// one failed pass per level of the family. Ordered by the depth of the foreign-key chain
/// above each table, deepest first. A catalogue row outlives its table (§10eu: late_t,
/// dropped, still enabled): a sweep prepared against it fails, and the first version died
/// there and reaped nothing anywhere. Rows whose table is gone are left out.
///
/// `SWEEP_ONLY_TABLES` is a pure filter, applied after: it cannot add a table the
/// catalogue did not declare, and a name that matches nothing sweeps nothing.
fn readSweeps(a: std.mem.Allocator, conn: *c.PGconn, only: ?[]const u8) !?struct { list: std.ArrayList(Sweep), declared: usize } {
    const cres = c.PQexec(conn, "SELECT cat.tbl, cat.tombstone_col::text FROM public.zebridge_catalogue cat" ++
        " WHERE cat.tombstone_col IS NOT NULL" ++
        " AND to_regclass(format('%I.%I', 'public', cat.tbl)) IS NOT NULL" ++
        " ORDER BY (WITH RECURSIVE up(oid, d) AS (" ++
        "   SELECT to_regclass(format('%I.%I', 'public', cat.tbl))::oid, 0" ++
        "   UNION ALL SELECT con.confrelid, up.d + 1 FROM pg_constraint con JOIN up ON con.conrelid = up.oid" ++
        "   WHERE con.contype = 'f' AND con.confrelid <> up.oid AND up.d < 32)" ++
        "   SELECT max(d) FROM up) DESC, cat.tbl");
    defer c.PQclear(cres);
    if (c.PQresultStatus(cres) != c.PGRES_TUPLES_OK) return null;

    var list = std.ArrayList(Sweep).empty;
    errdefer freeSweeps(a, &list);
    var declared: usize = 0;
    const n: usize = @intCast(c.PQntuples(cres));
    for (0..n) |i| {
        const tbl = std.mem.span(c.PQgetvalue(cres, @intCast(i), 0));
        const tomb = std.mem.span(c.PQgetvalue(cres, @intCast(i), 1));
        if (tbl.len == 0 or tomb.len == 0) continue;
        declared += 1;
        if (only) |o| {
            var it = std.mem.splitScalar(u8, o, ',');
            const wanted = while (it.next()) |raw| {
                if (std.mem.eql(u8, std.mem.trim(u8, raw, " "), tbl)) break true;
            } else false;
            if (!wanted) continue;
        }
        const t = try a.dupe(u8, tbl);
        errdefer a.free(t);
        try list.append(a, .{ .table = t, .tombstone = try a.dupe(u8, tomb) });
    }
    return .{ .list = list, .declared = declared };
}

fn sameSweeps(x: []const Sweep, y: []const Sweep) bool {
    if (x.len != y.len) return false;
    for (x, y) |p, q| {
        if (!std.mem.eql(u8, p.table, q.table) or !std.mem.eql(u8, p.tombstone, q.tombstone)) return false;
    }
    return true;
}

fn hasSweep(list: []const Sweep, sw: Sweep) bool {
    for (list) |o| {
        if (std.mem.eql(u8, o.table, sw.table) and std.mem.eql(u8, o.tombstone, sw.tombstone)) return true;
    }
    return false;
}

test "sameSweeps and hasSweep: a table, its column, and the order" {
    const a = [_]Sweep{ .{ .table = "orders", .tombstone = "deleted_at" }, .{ .table = "users", .tombstone = "deleted_at" } };
    const b = [_]Sweep{ .{ .table = "orders", .tombstone = "deleted_at" }, .{ .table = "users", .tombstone = "deleted_at" } };
    const other_col = [_]Sweep{ .{ .table = "orders", .tombstone = "gone_at" }, .{ .table = "users", .tombstone = "deleted_at" } };
    const swapped = [_]Sweep{ b[1], b[0] };
    try std.testing.expect(sameSweeps(&a, &b));
    try std.testing.expect(!sameSweeps(&a, &other_col));
    try std.testing.expect(!sameSweeps(&a, &swapped)); // the order is the FK order: a change
    try std.testing.expect(!sameSweeps(&a, a[0..1]));
    try std.testing.expect(hasSweep(&a, b[1]));
    try std.testing.expect(!hasSweep(&a, other_col[0]));
}

const usage =
    \\Usage: bridge_sweeper [--once]
    \\
    \\The tombstone GC sidecar: reaps tombstones older than GC_THRESHOLD_MS from every
    \\table the catalogue declares a tombstone column for, then publishes the watermark.
    \\
    \\  --once     one pass, then exit (a controlled run: check, sweep, check)
    \\  --help     this text
    \\
    \\Environment: DATABASE_WRITER_URL (required), GC_THRESHOLD_MS (default 604800000, 7 days),
    \\  GC_INTERVAL_MS (default 60000), GC_BATCH_ROWS (default 1000), GC_DRY_RUN,
    \\  GC_ALLOW_SHORT_THRESHOLD, SWEEP_ONLY_TABLES.
    \\
;

pub fn main(init: std.process.Init) !void {
    // Debug: the leak-checking allocator, its report printed when main returns.
    var debug_alloc: std.heap.DebugAllocator(.{}) = .init;
    defer if (@import("builtin").mode == .Debug) {
        _ = debug_alloc.deinit();
    };
    const allocator = if (@import("builtin").mode == .Debug) debug_alloc.allocator() else std.heap.c_allocator;
    runtime_log_level = levelFrom(init.minimal.environ.getPosix("LOG_LEVEL"));

    // Arguments first, before anything touches the database: this process deletes
    // rows, so an argument it does not know is a refusal, never a pass with defaults
    // (measured 2026-09-07: `bridge_sweeper --help` ran a real sweep).
    var once = false;
    {
        var it = init.minimal.args.iterate();
        _ = it.next(); // argv[0]
        while (it.next()) |arg| {
            if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
                try std.Io.File.stdout().writeStreamingAll(init.io, usage);
                return;
            } else if (std.mem.eql(u8, arg, "--once")) {
                once = true;
            } else {
                log.err("unknown argument '{s}'\n\n{s}", .{ arg, usage });
                std.process.exit(2);
            }
        }
    }

    log.info("ZeBridge GC Sidecar Starting...{s}", .{if (once) " (one pass)" else ""});

    // Read Env Vars
    //
    // A URL, like the bridge — and the *writer* one, because this sidecar DELETEs. It
    // used to assemble `host=… user=… password=…` from PG_HOST/PG_USER/PG_PASSWORD,
    // which are the superuser credentials init.sql runs under: a tombstone sweeper
    // running as `postgres` can delete anything in the database, and nothing in its
    // output would say so.
    const env = init.minimal.environ;
    const db_url = env.getPosix("DATABASE_WRITER_URL") orelse {
        log.err(
            "DATABASE_WRITER_URL is required (postgres://bridge_writer:...@host:port/db). " ++
                "There is no PG_HOST/PG_USER fallback: this process deletes rows, and must not be " ++
                "able to do it as the admin.",
            .{},
        );
        return;
    };

    // The sweep set comes from `zebridge_catalogue.tombstone_col` alone (§10jz), read
    // below on the same connection that sweeps. `zebridge_enable` writes it in the same
    // transaction as the tombstone trigger, so the sweeper and the guard cannot
    // disagree about which column it is. A table with no tombstone is not swept — its
    // deletes are physical and there is nothing to reap.
    //
    // ⚠️ This once read `GC_TABLES` and deleted `WHERE _deleted = true AND _hlc < $1` —
    // columns from an older design that **no table has** — so the GC watermark
    // PROTOCOL.md §7.5 promises was not enforced at all. One source, shared with the
    // guard, makes that impossible to repeat.

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
    // 7 days: a phone left off over a weekend or a short trip still catches up through the
    // chain and keeps its queued edits. An hour sent every client offline longer than that
    // to a full reload, and refused its pending edits ("predates the GC watermark").
    const threshold_ms_str = env.getPosix("GC_THRESHOLD_MS") orelse "604800000"; // 7 days
    const threshold_ms = std.fmt.parseInt(u64, threshold_ms_str, 10) catch {
        log.err("GC_THRESHOLD_MS is not a number: '{s}'", .{threshold_ms_str});
        return;
    };

    const dry_run = env.getPosix("GC_DRY_RUN") != null;
    // §10dn: a reap is a physical DELETE, and one statement over a million expired
    // tombstones is one transaction, one lock, one WAL spike — the cascade storm this
    // design keeps off the clients, arriving on the server instead. Bounded batches by
    // `ctid`, looped until a batch comes back short. 1000 rows is a size PostgreSQL
    // absorbs without notice; the operator sets GC_BATCH_ROWS to taste.
    const batch_rows_str = env.getPosix("GC_BATCH_ROWS") orelse "1000";
    const batch_rows = std.fmt.parseInt(u32, batch_rows_str, 10) catch {
        log.err("GC_BATCH_ROWS is not a number: '{s}'", .{batch_rows_str});
        return;
    };
    if (batch_rows == 0) {
        log.err("GC_BATCH_ROWS must be at least 1", .{});
        return;
    }
    if (dry_run) log.info("DRY RUN — counting only, nothing will be deleted", .{});

    const min_threshold_ms: u64 = 60_000; // one minute
    if (threshold_ms < min_threshold_ms) {
        log.err(
            "GC_THRESHOLD_MS={d} is below the {d}ms floor.\n" ++
                "  This is the maximum offline window clients may have — below a minute it is\n" ++
                "  almost certainly a units mistake (3600 is 3.6 SECONDS, not an hour), and the\n" ++
                "  cost is silent: tombstones vanish and a late client resurrects deleted rows.\n" ++
                "  Set GC_ALLOW_SHORT_THRESHOLD=1 if a sub-minute window is genuinely intended.",
            .{ threshold_ms, min_threshold_ms },
        );
        if (env.getPosix("GC_ALLOW_SHORT_THRESHOLD") == null) return;
        log.info("proceeding anyway — GC_ALLOW_SHORT_THRESHOLD is set", .{});
    }

    // Said at start, set or not: the threshold is the deployment's offline window.
    if (env.getPosix("GC_THRESHOLD_MS") == null) {
        log.info(
            "GC_THRESHOLD_MS not set: the default, {d}ms (7 days), is the longest a client may stay\n" ++
                "    offline and still catch up with its pending edits — set it to change that window.",
            .{threshold_ms},
        );
    }

    const interval_ms_str = env.getPosix("GC_INTERVAL_MS") orelse "60000";
    const interval_ms = try std.fmt.parseInt(u64, interval_ms_str, 10);

    var sweeps = std.ArrayList(Sweep).empty;
    defer freeSweeps(allocator, &sweeps);

    // Connect to PostgreSQL
    const conninfo = try utils.allocPrintZ(allocator, "{s}", .{db_url});
    defer allocator.free(conninfo);

    const pg_conn = c.PQconnectdb(conninfo.ptr);
    if (c.PQstatus(pg_conn) != c.CONNECTION_OK) {
        log.err("Failed to connect to PostgreSQL: {s}", .{c.PQerrorMessage(pg_conn)});
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
    // soft-delete trigger it names, so this read cannot disagree with the guard. It is read
    // again at the start of every pass: a table enabled while the sweeper runs is swept
    // from the next pass on, with no restart — the sweeper is the process nobody remembers.
    //
    // `SWEEP_ONLY_TABLES` narrows a run to the named tables — a scoped run for tests.
    const only = env.getPosix("SWEEP_ONLY_TABLES");
    if (try readSweeps(allocator, pg_conn.?, only)) |first| {
        sweeps = first.list;
        log.info("catalogue supplied {d} sweep table(s)", .{first.declared});
        if (only) |o| log.info("SWEEP_ONLY_TABLES='{s}' — scoped run, {d} of {d} table(s) kept", .{ o, sweeps.items.len, first.declared });
    } else {
        log.info("no zebridge_catalogue — apply init.core.template.sql", .{});
    }

    if (sweeps.items.len == 0) {
        log.warn(
            "The catalogue declares no tombstone column, so there " ++
                "is nothing to sweep. Deletes on those tables are physical, and an offline " ++
                "client's queued edit can resurrect a row (PROTOCOL.md \u{00a7}7.5).",
            .{},
        );
        // One pass asked for, nothing to do. Running, the sweeper waits for the first table.
        if (once) return;
    }
    for (sweeps.items) |sw| {
        log.info("sweeping {s} on tombstone column '{s}'", .{ sw.table, sw.tombstone });
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
                    try utils.allocPrintZ(a, "DELETE FROM \"{s}\" WHERE ctid IN (SELECT ctid FROM \"{s}\" WHERE \"{s}\" IS NOT NULL AND \"{s}\"::timestamptz < now() - make_interval(secs => $1::double precision) LIMIT $2::integer);", .{ sw.table, sw.table, sw.tombstone, sw.tombstone });
                defer a.free(sql);
                const res = c.PQprepare(conn, stmt_name.ptr, sql.ptr, if (drun) 1 else 2, null);
                defer c.PQclear(res);
                if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
                    log.err("cannot prepare the sweep of {s}: {s}", .{ sw.table, c.PQerrorMessage(conn) });
                    return error.PrepareFailed;
                }
            }
            const wm_res = c.PQprepare(conn, "gc_watermark_update", "UPDATE public.zebridge_gc_watermark SET watermark = now() - make_interval(secs => $1::double precision), threshold_ms = $2::bigint, reaped = $3::bigint, swept_at = now(), updated_at = now() WHERE id = 1", 3, null);
            defer c.PQclear(wm_res);
            if (c.PQresultStatus(wm_res) != c.PGRES_COMMAND_OK) return error.PrepareFailed;
        }
    }.do;
    setup_connection(allocator, pg_conn.?, principal, sweeps.items, dry_run) catch |err| {
        log.err("failed to initialize connection: {any}", .{err});
        return;
    };
    log.info("acting as principal '{s}'", .{principal});

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
    const on_signal: std.posix.Sigaction = .{ .handler = .{ .handler = onSignal }, .mask = std.mem.zeroes(std.posix.sigset_t), .flags = 0 };
    std.posix.sigaction(std.posix.SIG.INT, &on_signal, null);
    std.posix.sigaction(std.posix.SIG.TERM, &on_signal, null);

    // Run loop
    var first_pass = true;
    while (!stop.load(.acquire)) {
        if (c.PQstatus(pg_conn) == c.CONNECTION_BAD) {
            log.warn("Connection lost. Reconnecting...", .{});
            c.PQreset(pg_conn);
            if (c.PQstatus(pg_conn) != c.CONNECTION_OK) {
                log.err("Reconnect failed: {s}", .{c.PQerrorMessage(pg_conn)});
                sleepUnlessStopped(5000);
                continue;
            }
            setup_connection(allocator, pg_conn.?, principal, sweeps.items, dry_run) catch |err| {
                log.err("failed to initialize reconnected session: {any}", .{err});
                sleepUnlessStopped(5000);
                continue;
            };
            log.info("Reconnected and initialized.", .{});
        }

        // The catalogue as it is now: a table enabled, dropped or given another tombstone
        // column since the last pass. On a change, the statements are prepared again.
        if (!first_pass) {
            if (try readSweeps(allocator, pg_conn.?, only)) |now| {
                var fresh = now.list;
                if (sameSweeps(sweeps.items, fresh.items)) {
                    freeSweeps(allocator, &fresh);
                } else {
                    for (fresh.items) |sw| if (!hasSweep(sweeps.items, sw))
                        log.info("sweeping {s} on tombstone column '{s}'", .{ sw.table, sw.tombstone });
                    for (sweeps.items) |sw| if (!hasSweep(fresh.items, sw))
                        log.info("no longer sweeping {s}: the catalogue no longer declares its tombstone column '{s}'", .{ sw.table, sw.tombstone });
                    freeSweeps(allocator, &sweeps);
                    sweeps = fresh;
                    const dealloc = c.PQexec(pg_conn, "DEALLOCATE ALL");
                    c.PQclear(dealloc);
                    setup_connection(allocator, pg_conn.?, principal, sweeps.items, dry_run) catch |err| {
                        log.err("cannot prepare the new sweep set: {any} — retrying next pass", .{err});
                        // Forget it, so the next pass sees a change and prepares again.
                        freeSweeps(allocator, &sweeps);
                        sweeps = .empty;
                        sleepUnlessStopped(interval_ms);
                        continue;
                    };
                }
            } else {
                log.warn("cannot read the catalogue: {s} — keeping the last sweep set", .{c.PQerrorMessage(pg_conn)});
            }
        }
        first_pass = false;
        if (sweeps.items.len == 0) {
            sleepUnlessStopped(interval_ms);
            continue;
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
            // the catalogue, which is operator input.
            // `GC_DRY_RUN=1` counts instead of deleting. There is no undo on this path, so
            // the only safe way to change a threshold on a live database is to see what the
            // new one would take first.
            const stmt_name = try utils.allocPrintZ(aa, "gc_sweep_{d}", .{i});
            const secs = try utils.allocPrintZ(aa, "{d}", .{threshold_ms / 1000});
            const batch_z = try utils.allocPrintZ(aa, "{d}", .{batch_rows});
            const param_vals = [_]?[*:0]const u8{ secs.ptr, batch_z.ptr };
            var res = c.PQexecPrepared(pg_conn, stmt_name.ptr, if (dry_run) 1 else 2, &param_vals[0], null, null, 0);
            defer c.PQclear(res);

            if (dry_run) {
                if (c.PQresultStatus(res) == c.PGRES_TUPLES_OK and c.PQntuples(res) > 0) {
                    log.info(
                        "[dry run] WOULD reap {s} tombstone(s) from {s} older than {d}ms",
                        .{ c.PQgetvalue(res, 0, 0), sw.table, threshold_ms },
                    );
                }
                continue;
            }

            if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
                log.err("GC failed for table {s}: {s}", .{ sw.table, c.PQerrorMessage(pg_conn) });
            } else {
                // Batches until one comes back short. Each is its own statement and its
                // own transaction, so a family of a million tombstones is a thousand
                // small deletes, not one lock held for the duration.
                var reaped_table: u64 = std.fmt.parseInt(u64, std.mem.span(c.PQcmdTuples(res)), 10) catch 0;
                var batches: u32 = 1;
                while (reaped_table == @as(u64, batch_rows) * batches and batches < 100_000) {
                    c.PQclear(res);
                    res = c.PQexecPrepared(pg_conn, stmt_name.ptr, 2, &param_vals[0], null, null, 0);
                    if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
                        log.err("GC failed for table {s} after {d} batch(es): {s}", .{ sw.table, batches, c.PQerrorMessage(pg_conn) });
                        break;
                    }
                    reaped_table += std.fmt.parseInt(u64, std.mem.span(c.PQcmdTuples(res)), 10) catch 0;
                    batches += 1;
                }
                reaped_this_pass += reaped_table;
                if (reaped_table > 0) {
                    log.info(
                        "reaped {d} tombstone(s) from {s} older than {d}ms in {d} batch(es) of up to {d}",
                        .{ reaped_table, sw.table, threshold_ms, batches, batch_rows },
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
                log.warn(
                    "could not publish the watermark: {s}",
                    .{c.PQerrorMessage(pg_conn)},
                );
            } else if (std.mem.eql(u8, std.mem.span(c.PQcmdTuples(wm_res)), "0")) {
                log.warn(
                    "the watermark row is missing (zebridge_gc_watermark id=1)." ++
                        " Clients cannot tell how far back tombstones survive. Re-run init.sql.",
                    .{},
                );
            }
        }

        if (once) {
            log.info("one pass done, exiting (--once)", .{});
            return;
        }
        sleepUnlessStopped(interval_ms);
    }
    log.info("stopped", .{});
}
