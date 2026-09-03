with open("src/bridge_sweeper.zig", "r") as f:
    text = f.read()

text = text.replace('for (sweeps.items, 0..) |sw, i| {\n                    if (std.mem.eql(u8, sw.table, tbl)) continue :rows;', 'for (sweeps.items) |sw| {\n                    if (std.mem.eql(u8, sw.table, tbl)) continue :rows;')

with open("src/bridge_sweeper.zig", "w") as f:
    f.write(text)
