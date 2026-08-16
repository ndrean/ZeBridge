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

    const tables_str = env.getPosix("GC_TABLES") orelse {
        std.debug.print("FATAL: Missing GC_TABLES\n", .{});
        return;
    };

    const threshold_ms_str = env.getPosix("GC_THRESHOLD_MS") orelse "3600000"; // 1 hour
    const threshold_ms = try std.fmt.parseInt(u64, threshold_ms_str, 10);

    const interval_ms_str = env.getPosix("GC_INTERVAL_MS") orelse "60000";
    const interval_ms = try std.fmt.parseInt(u64, interval_ms_str, 10);

    // Parse tables
    var tables = std.ArrayList([]const u8).empty;
    defer tables.deinit(allocator);
    var it = std.mem.splitScalar(u8, tables_str, ',');
    while (it.next()) |table| {
        try tables.append(allocator, std.mem.trim(u8, table, " "));
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
