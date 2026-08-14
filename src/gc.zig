const std = @import("std");
const c = @import("c_imports.zig").c;
const utils = @import("utils.zig");

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.c_allocator;

    std.debug.print("ZeBridge GC Sidecar Starting...\n", .{});

    // Read Env Vars
    const env = init.minimal.environ;
    const pg_host = env.getPosix("PG_HOST") orelse {
        std.debug.print("FATAL: Missing PG_HOST\n", .{});
        return;
    };
    const pg_port_str = env.getPosix("PG_PORT") orelse {
        std.debug.print("FATAL: Missing PG_PORT\n", .{});
        return;
    };
    const pg_db = env.getPosix("PG_DB") orelse {
        std.debug.print("FATAL: Missing PG_DB\n", .{});
        return;
    };
    const pg_user = env.getPosix("PG_USER") orelse {
        std.debug.print("FATAL: Missing PG_USER\n", .{});
        return;
    };
    const pg_pass = env.getPosix("PG_PASSWORD") orelse {
        std.debug.print("FATAL: Missing PG_PASSWORD\n", .{});
        return;
    };

    const tables_str = env.getPosix("GC_TABLES") orelse {
        std.debug.print("FATAL: Missing GC_TABLES\n", .{});
        return;
    };

    const interval_ms_str = env.getPosix("GC_INTERVAL_MS") orelse "60000";
    const interval_ms = try std.fmt.parseInt(u64, interval_ms_str, 10);

    const threshold_ms_str = env.getPosix("GC_THRESHOLD_MS") orelse "3600000"; // 1 hour
    const threshold_ms = try std.fmt.parseInt(u64, threshold_ms_str, 10);

    // Parse tables
    var tables = std.ArrayList([]const u8).empty;
    defer tables.deinit(allocator);
    var it = std.mem.splitScalar(u8, tables_str, ',');
    while (it.next()) |table| {
        try tables.append(allocator, std.mem.trim(u8, table, " "));
    }

    // Connect to PostgreSQL
    const conninfo = try std.fmt.allocPrint(
        allocator,
        "host={s} port={s} dbname={s} user={s} password={s}",
        .{ pg_host, pg_port_str, pg_db, pg_user, pg_pass },
    );
    defer allocator.free(conninfo);

    const pg_conn = c.PQconnectdb(conninfo.ptr);
    if (c.PQstatus(pg_conn) != c.CONNECTION_OK) {
        std.debug.print("FATAL: Failed to connect to PostgreSQL: {s}\n", .{c.PQerrorMessage(pg_conn)});
        c.PQfinish(pg_conn);
        return;
    }
    defer c.PQfinish(pg_conn);
    std.debug.print("Connected to PostgreSQL successfully.\n", .{});

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

        const now_ms = utils.getMilliTimestamp();
        const cutoff_ms = now_ms - @as(i64, @intCast(threshold_ms));
        
        // HLC format is roughly `<timestamp>-<seq>`. String comparison works well enough if 0-padded.
        // We'll generate the string cutoff: `1723456789000-0000`
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const aa = arena.allocator();

        const hlc_cutoff = try utils.allocPrintZ(aa, "{d}-0000", .{cutoff_ms});

        for (tables.items) |table| {
            const sql = try utils.allocPrintZ(
                aa,
                "DELETE FROM \"{s}\" WHERE _deleted = true AND _hlc < $1;",
                .{table}
            );

            const param_vals = [_]?[*:0]const u8{hlc_cutoff.ptr};

            const res = c.PQexecParams(
                pg_conn,
                sql.ptr,
                1,
                null,
                &param_vals[0],
                null,
                null,
                0
            );
            defer c.PQclear(res);

            if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
                std.debug.print("ERROR: GC failed for table {s}: {s}\n", .{table, c.PQerrorMessage(pg_conn)});
            } else {
                const rows_deleted = c.PQcmdTuples(res);
                if (rows_deleted[0] != 0 and rows_deleted[0] != '0') {
                    std.debug.print("GC: Deleted {s} tombstone(s) from {s} (older than {s})\n", .{rows_deleted, table, hlc_cutoff});
                }
            }
        }

        utils.sleep(interval_ms * std.time.ns_per_ms);
    }
}
