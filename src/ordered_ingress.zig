//! Ordered-envelope validation and the short PostgreSQL parking operations.
//! Application transactions never call park or settle. Original bytes are retained.
const std = @import("std");
const c = @import("c_imports.zig").c;
const receipts = @import("receipts.zig");
const mp = @import("msgpack");
pub const Envelope = struct { ordered: bool = false, predecessor: ?[:0]const u8 = null, refusal: ?anyerror = null };

pub fn parse(a: std.mem.Allocator, bytes: []const u8, id: []const u8) !Envelope {
    var r = std.Io.Reader.fixed(bytes);
    var buf: [0]u8 = .{};
    var w = std.Io.Writer.fixed(&buf);
    var packer = mp.PackerIO.init(&r, &w);
    const value = packer.read(a) catch |err| {
        if (err == error.OutOfMemory) return err;
        return .{}; // the existing payload validator records its permanent rejection
    };
    if (value != .map) return .{};
    const version = value.map.getByString("ordering") orelse {
        if (value.map.getByString("predecessor") != null) return .{ .refusal = error.InvalidOrderedEnvelope };
        return .{};
    };
    const v: i64 = switch (version) {
        .int => |x| x,
        .uint => |x| if (x <= std.math.maxInt(i64)) @intCast(x) else -1,
        else => -1,
    };
    if (v != 1) return .{ .refusal = error.UnsupportedOrderingVersion };
    const client = value.map.getByString("client_id") orelse return .{ .refusal = error.InvalidOrderedEnvelope };
    if (client != .str) return .{ .refusal = error.InvalidOrderedEnvelope };
    var encoded: std.ArrayList(u8) = .empty;
    for (client.str.value()) |ch| {
        if (std.ascii.isAlphanumeric(ch) or ch == '_' or ch == ':') try encoded.append(a, ch) else try encoded.print(a, "~{X:0>2}", .{ch});
    }
    var tokens = std.mem.splitScalar(u8, id, '-');
    const prefix = tokens.next() orelse "";
    _ = tokens.next(); // timestamp; SQL validates its range
    _ = tokens.next(); // operation
    const id_client = tokens.next() orelse "";
    if (!std.mem.eql(u8, prefix, "zb1") or encoded.items.len == 0 or !std.mem.eql(u8, id_client, encoded.items)) return .{ .refusal = error.InvalidOrderedEnvelope };
    const pred = value.map.getByString("predecessor") orelse return .{ .refusal = error.InvalidOrderedEnvelope };
    return switch (pred) {
        .nil => .{ .ordered = true },
        .str => |s| .{ .ordered = true, .predecessor = try a.dupeSentinel(u8, s.value(), 0) },
        else => .{ .refusal = error.InvalidOrderedEnvelope },
    };
}

pub fn park(a: std.mem.Allocator, conn: ?*c.PGconn, principal: [:0]const u8, id: [:0]const u8, route: []const u8, payload: []const u8, pred: ?[:0]const u8, sequence: u64) ![]const u8 {
    const h = try std.fmt.allocPrintSentinel(a, "{{\"seq\":{d}}}", .{sequence}, 0);
    const values = [_]?[*]const u8{ principal.ptr, id.ptr, route.ptr, payload.ptr, if (pred) |p| p.ptr else null, h.ptr };
    const lengths = [_]c_int{ 0, 0, @intCast(route.len), @intCast(payload.len), 0, 0 };
    const formats = [_]c_int{ 0, 0, 1, 1, 0, 0 };
    const r = c.PQexecParams(conn, "SELECT zebridge_private.park_mutation($1,$2,$3::bytea,$4::bytea,$5,$6::jsonb)", 6, null, @ptrCast(&values), &lengths, &formats, 0);
    defer c.PQclear(r);
    if (c.PQresultStatus(r) != c.PGRES_TUPLES_OK or c.PQntuples(r) != 1) return error.ReceiptInfrastructure;
    return a.dupe(u8, std.mem.span(c.PQgetvalue(r, 0, 0)));
}

pub fn settle(conn: ?*c.PGconn) !void {
    if (c.PQtransactionStatus(conn) != c.PQTRANS_IDLE) return error.ReceiptInfrastructure;
    const r = c.PQexec(conn, "SELECT zebridge_private.settle_park_credits(256)");
    defer c.PQclear(r);
    if (c.PQresultStatus(r) != c.PGRES_TUPLES_OK) return error.ReceiptInfrastructure;
}

test "ordered envelope root" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const got = try parse(arena.allocator(), "\x83\xa8\x6f\x72\x64\x65\x72\x69\x6e\x67\x01\xa9\x63\x6c\x69\x65\x6e\x74\x5f\x69\x64\xaa\x6f\x6e\x65\x2d\x63\x6c\x69\x65\x6e\x74\xab\x70\x72\x65\x64\x65\x63\x65\x73\x73\x6f\x72\xc0", "zb1-t1-INSERT-one~2Dclient-tab-k1-s-v");
    try std.testing.expect(got.ordered and got.predecessor == null and got.refusal == null);
}
test "ordered envelope dependency" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const got = try parse(arena.allocator(), "\x83\xa8\x6f\x72\x64\x65\x72\x69\x6e\x67\x01\xa9\x63\x6c\x69\x65\x6e\x74\x5f\x69\x64\xaa\x6f\x6e\x65\x2d\x63\x6c\x69\x65\x6e\x74\xab\x70\x72\x65\x64\x65\x63\x65\x73\x73\x6f\x72\xd9\x25\x7a\x62\x31\x2d\x74\x31\x2d\x49\x4e\x53\x45\x52\x54\x2d\x6f\x6e\x65\x7e\x32\x44\x63\x6c\x69\x65\x6e\x74\x2d\x74\x61\x62\x2d\x6b\x31\x2d\x70\x2d\x76", "zb1-t1-INSERT-one~2Dclient-tab-k1-s-v");
    try std.testing.expectEqualStrings("zb1-t1-INSERT-one~2Dclient-tab-k1-p-v", got.predecessor.?);
}
test "ordered envelope wrong client" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const got = try parse(arena.allocator(), "\x83\xa8\x6f\x72\x64\x65\x72\x69\x6e\x67\x01\xa9\x63\x6c\x69\x65\x6e\x74\x5f\x69\x64\xa5\x6f\x74\x68\x65\x72\xab\x70\x72\x65\x64\x65\x63\x65\x73\x73\x6f\x72\xc0", "zb1-t1-INSERT-one~2Dclient-tab-k1-s-v");
    try std.testing.expectEqual(error.InvalidOrderedEnvelope, got.refusal.?);
}
test "ordered envelope unsupported" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const got = try parse(arena.allocator(), "\x83\xa8\x6f\x72\x64\x65\x72\x69\x6e\x67\x02\xa9\x63\x6c\x69\x65\x6e\x74\x5f\x69\x64\xaa\x6f\x6e\x65\x2d\x63\x6c\x69\x65\x6e\x74\xab\x70\x72\x65\x64\x65\x63\x65\x73\x73\x6f\x72\xc0", "zb1-t1-INSERT-one~2Dclient-tab-k1-s-v");
    try std.testing.expectEqual(error.UnsupportedOrderingVersion, got.refusal.?);
}
test "ordered envelope missing predecessor" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const got = try parse(arena.allocator(), "\x82\xa8\x6f\x72\x64\x65\x72\x69\x6e\x67\x01\xa9\x63\x6c\x69\x65\x6e\x74\x5f\x69\x64\xaa\x6f\x6e\x65\x2d\x63\x6c\x69\x65\x6e\x74", "zb1-t1-INSERT-one~2Dclient-tab-k1-s-v");
    try std.testing.expectEqual(error.InvalidOrderedEnvelope, got.refusal.?);
}
test "ordered envelope legacy" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const got = try parse(arena.allocator(), "\x81\xa9\x63\x6c\x69\x65\x6e\x74\x5f\x69\x64\xaa\x6f\x6e\x65\x2d\x63\x6c\x69\x65\x6e\x74", "zb1-t1-INSERT-one~2Dclient-tab-k1-s-v");
    try std.testing.expect(!got.ordered and got.refusal == null);
}

/// Whether the dispatcher must keep a write on its client's lane: any ordered envelope,
/// the first of a sequence included (it has no predecessor), and any write whose ordering
/// fields are malformed, so the lane's validation refuses it rather than it being spread
/// as unordered. `parse` is the one rule; an allocation failure keeps affinity.
pub fn wantsAffinity(a: std.mem.Allocator, bytes: []const u8, id: []const u8) bool {
    const e = parse(a, bytes, id) catch return true;
    return e.ordered or e.refusal != null;
}
