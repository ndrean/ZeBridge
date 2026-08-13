const std = @import("std");

const type_map = std.StaticStringMap([]const u8).initComptime(.{
    .{ "integer", "INTEGER" },
    .{ "bigint", "INTEGER" },
    .{ "boolean", "INTEGER" },
    .{ "real", "REAL" },
});

pub fn main() !void {
    std.debug.print("integer: {s}\n", .{type_map.get("integer") orelse "TEXT"});
    std.debug.print("varchar: {s}\n", .{type_map.get("varchar") orelse "TEXT"});
}
