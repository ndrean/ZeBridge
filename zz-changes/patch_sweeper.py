import re

with open("src/bridge_sweeper.zig", "r") as f:
    content = f.read()

setup_conn = """
    const setup_connection = struct {
        fn do(a: std.mem.Allocator, conn: *c.PGconn, p: []const u8, swps: []const Sweep, drun: bool) !void {
            const tz = c.PQexec(conn, "SET TIME ZONE 'UTC'");
            defer c.PQclear(tz);
            if (c.PQresultStatus(tz) != c.PGRES_COMMAND_OK) return error.InitFailed;
            
            const p_z = try std.fmt.allocPrintZ(a, "{s}", .{p});
            defer a.free(p_z);
            const params = [_]?[*:0]const u8{p_z.ptr};
            const p_res = c.PQexecParams(conn, "SELECT set_config('zb.principal', $1, false)", 1, null, &params[0], null, null, 0);
            defer c.PQclear(p_res);
            if (c.PQresultStatus(p_res) != c.PGRES_TUPLES_OK) return error.InitFailed;

            for (swps, 0..) |sw, i| {
                const stmt_name = try std.fmt.allocPrintZ(a, "gc_sweep_{d}", .{i});
                defer a.free(stmt_name);
                const sql = if (drun)
                    try std.fmt.allocPrintZ(a, "SELECT count(*) FROM \\\"{s}\\\" WHERE \\\"{s}\\\" IS NOT NULL AND \\\"{s}\\\"::timestamptz < now() - make_interval(secs => $1::double precision);", .{ sw.table, sw.tombstone, sw.tombstone })
                else
                    try std.fmt.allocPrintZ(a, "DELETE FROM \\\"{s}\\\" WHERE \\\"{s}\\\" IS NOT NULL AND \\\"{s}\\\"::timestamptz < now() - make_interval(secs => $1::double precision);", .{ sw.table, sw.tombstone, sw.tombstone });
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
"""

# Find where to insert setup_connection (after principal is defined)
content = content.replace('    const principal = env.getPosix("SWEEPER_PRINCIPAL") orelse "zb_sweeper";\n', '    const principal = env.getPosix("SWEEPER_PRINCIPAL") orelse "zb_sweeper";\n' + setup_conn)

# Replace the initial setup
content = re.sub(
    r'    \{\n        const tz = c.PQexec\(pg_conn, "SET TIME ZONE \'UTC\'"\);\n        defer c.PQclear\(tz\);\n    \}',
    '',
    content,
    flags=re.MULTILINE
)

content = re.sub(
    r'    \{\n        // Session-level, not `SET LOCAL`:.*?\n        const principal_z = utils.allocPrintZ\(allocator, "\{s\}", \.\{principal\}\) catch return;\n        defer allocator.free\(principal_z\);\n        const params = \[_\]\?\[\*:0\]const u8\{principal_z.ptr\};\n        const res = c.PQexecParams\(pg_conn, "SELECT set_config\(\'zb.principal\', \$1, false\)", 1, null, &params\[0\], null, null, 0\);\n        defer c.PQclear\(res\);\n        if \(c.PQresultStatus\(res\) != c.PGRES_TUPLES_OK\) \{\n            std.debug.print\("FATAL: could not set the sweeper principal: \{s\}\\n", \.\{c.PQerrorMessage\(pg_conn\)\}\);\n            return;\n        \}\n    \}',
    '    setup_connection(allocator, pg_conn, principal, sweeps.items, dry_run) catch |err| {\n        std.debug.print("FATAL: failed to initialize connection: {any}\\n", .{err});\n        return;\n    };',
    content,
    flags=re.DOTALL
)

# Replace the reconnect setup
content = re.sub(
    r'            std\.debug\.print\("Reconnected\.\\n", \.\{\}\);\n        \}',
    '            setup_connection(allocator, pg_conn, principal, sweeps.items, dry_run) catch |err| {\n                std.debug.print("ERROR: failed to initialize reconnected session: {any}\\n", .{err});\n                utils.sleep(5 * std.time.ns_per_s);\n                continue;\n            };\n            std.debug.print("Reconnected and initialized.\\n", .{});\n        }',
    content,
    flags=re.MULTILINE
)

# Replace PQexecParams calls inside the loop
# 1. DELETE
content = re.sub(
    r'            const sql = if \(dry_run\).*?            const res = c\.PQexecParams\(pg_conn, sql\.ptr, 1, null, &param_vals\[0\], null, null, 0\);',
    r'            const stmt_name = try std.fmt.allocPrintZ(aa, "gc_sweep_{d}", .{i});\n            const res = c.PQexecPrepared(pg_conn, stmt_name.ptr, 1, &param_vals[0], null, null, 0);',
    content,
    flags=re.DOTALL
)

# 2. UPDATE watermark
content = re.sub(
    r'            const wm_res = c\.PQexecParams\(\n                pg_conn,\n                "UPDATE public\.zebridge_gc_watermark SET".*?WHERE id = 1",\n                3,\n                null,\n                &wm_params\[0\],\n                null,\n                null,\n                0,\n            \);',
    r'            const wm_res = c.PQexecPrepared(pg_conn, "gc_watermark_update", 3, &wm_params[0], null, null, 0);',
    content,
    flags=re.DOTALL
)

# wait, we need i in `for (sweeps.items) |sw| {`
content = content.replace('for (sweeps.items) |sw| {', 'for (sweeps.items, 0..) |sw, i| {')
# but wait, there is `for (sweeps.items) |sw| {` around line 216 for logging, we shouldn't change that.
# we only want to change the one in the while (true) loop, which is followed by `// The cutoff is computed`.
content = content.replace('        for (sweeps.items) |sw| {\n            // The cutoff is computed', '        for (sweeps.items, 0..) |sw, i| {\n            // The cutoff is computed')

with open("src/bridge_sweeper.zig", "w") as f:
    f.write(content)
