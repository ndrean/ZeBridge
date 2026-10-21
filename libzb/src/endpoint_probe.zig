//! Scenario driver for the real native transport's connection and server pool.
//! No public C ABI surface: `zig build endpoint-probe`, then send s (snapshot) or q.
const std = @import("std");
const Transport = @import("transport.zig").Transport;

fn snapshot(t: *Transport) !void {
    const a = std.heap.c_allocator;
    const nc = t.conn;
    try nc.mutex.lock(nc.io);
    const text = blk: {
        defer nc.mutex.unlock(nc.io);
        var urls: std.ArrayList([]const u8) = .empty;
        defer urls.deinit(a);
        var implicit: usize = 0;
        for (nc.server_pool.servers.values()) |server| {
            try urls.append(a, server.key);
            if (server.is_implicit) implicit += 1;
        }
        break :blk try std.json.Stringify.valueAlloc(a, .{
            .status = @tagName(nc.status),
            .server = if (nc.current_server) |s| s.key else "",
            .pool = urls.items,
            .implicit = implicit,
            .max_payload = nc.server_info.max_payload,
            .post_connect_info = nc.server_info.nonce == null,
        }, .{});
    };
    defer a.free(text);
    var written: usize = 0;
    while (written < text.len) {
        const n = std.c.write(1, text[written..].ptr, text.len - written);
        if (n <= 0) return error.WriteFailed;
        written += @intCast(n);
    }
    if (std.c.write(1, "\n", 1) != 1) return error.WriteFailed;
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(std.heap.c_allocator);
    defer std.heap.c_allocator.free(args);
    if (args.len < 4) return error.MissingArguments;
    const prefix = try std.fmt.allocPrint(std.heap.c_allocator, "_INBOX.{s}", .{args[3]});
    defer std.heap.c_allocator.free(prefix);
    const t = try Transport.connect(std.heap.c_allocator, .{
        .url = args[1],
        .creds_path = args[2],
        .inbox_prefix = prefix,
        .ca_file = if (args.len > 4) args[4] else null,
    });
    defer t.deinit();
    try snapshot(t);
    var byte: [1]u8 = undefined;
    while (try std.posix.read(0, &byte) != 0) {
        if (byte[0] == 'q') break;
        if (byte[0] == 's') try snapshot(t);
    }
}
