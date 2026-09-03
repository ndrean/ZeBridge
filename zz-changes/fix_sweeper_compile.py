with open("src/bridge_sweeper.zig", "r") as f:
    text = f.read()

# Fix unused i on line 190
text = text.replace('        for (sweeps.items, 0..) |sw, i| {\n            var it = std.mem.splitScalar(u8, only, \',\');', '        for (sweeps.items) |sw| {\n            var it = std.mem.splitScalar(u8, only, \',\');')

# Fix unused i on line 213 (actually around line 216)
text = text.replace('    for (sweeps.items, 0..) |sw, i| {\n        std.debug.print("GC: sweeping {s} on tombstone column \'{s}\'\\n", .{ sw.table, sw.tombstone });', '    for (sweeps.items) |sw| {\n        std.debug.print("GC: sweeping {s} on tombstone column \'{s}\'\\n", .{ sw.table, sw.tombstone });')

# Fix deleted param_vals around 336
text = text.replace('            const stmt_name = try std.fmt.allocPrintZ(aa, "gc_sweep_{d}", .{i});\n            const res = c.PQexecPrepared(pg_conn, stmt_name.ptr, 1, &param_vals[0], null, null, 0);', '            const stmt_name = try std.fmt.allocPrintZ(aa, "gc_sweep_{d}", .{i});\n            const secs = try utils.allocPrintZ(aa, "{d}", .{threshold_ms / 1000});\n            const param_vals = [_]?[*:0]const u8{secs.ptr};\n            const res = c.PQexecPrepared(pg_conn, stmt_name.ptr, 1, &param_vals[0], null, null, 0);')

with open("src/bridge_sweeper.zig", "w") as f:
    f.write(text)
