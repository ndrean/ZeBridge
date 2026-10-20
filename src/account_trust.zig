//! Public account claims for leaf trust refresh. Root seeds never enter this path.
const std = @import("std");

pub fn accountJwt(conf: []const u8, account: []const u8) ![]const u8 {
    if (account.len != 56 or account[0] != 'A') return error.InvalidAccount;
    var needle: [57]u8 = undefined;
    @memcpy(needle[0..56], account);
    needle[56] = ':';
    const at = std.mem.find(u8, conf, &needle) orelse return error.AccountNotFound;
    const start = at + needle.len;
    const rest = std.mem.trimStart(u8, conf[start..], " \t\"");
    var end: usize = 0;
    while (end < rest.len and (std.ascii.isAlphanumeric(rest[end]) or rest[end] == '.' or rest[end] == '-' or rest[end] == '_')) end += 1;
    const jwt = rest[0..end];
    if (!std.mem.startsWith(u8, jwt, "ey") or std.mem.count(u8, jwt, ".") != 2) return error.InvalidAccountJwt;
    return jwt;
}

/// Distinct revisions need increasing iat: a receiver can then refuse signed but
/// older claims, including a replay that reinstates an old signing key.
pub fn freshIssuedAt(a: std.mem.Allocator, io: std.Io, claims: []const u8) !i64 {
    const parsed = try std.json.parseFromSlice(std.json.Value, a, claims, .{});
    defer parsed.deinit();
    const value = parsed.value.object.get("iat") orelse return error.MissingIssuedAt;
    if (value != .integer) return error.InvalidIssuedAt;
    var now = std.Io.Clock.real.now(io).toSeconds();
    if (value.integer > now) return error.AccountClockAhead;
    if (value.integer == now) {
        try std.Io.sleep(io, .fromSeconds(1), .awake);
        now = std.Io.Clock.real.now(io).toSeconds();
    }
    if (now <= value.integer) return error.AccountClockNotAdvanced;
    return now;
}

pub fn refreshIssuedAt(a: std.mem.Allocator, io: std.Io, claims: []const u8) ![]u8 {
    const now = try freshIssuedAt(a, io, claims);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, claims, .{});
    defer parsed.deinit();
    parsed.value.object.getPtr("iat").?.* = .{ .integer = now };
    return std.json.Stringify.valueAlloc(a, parsed.value, .{});
}

test "public account extraction selects one account and never another conf field" {
    const key = "A" ** 56;
    const jwt = "eyHeader.eyClaims.Signature";
    const conf = "operator: eyOperator.public.signature\nresolver_preload {\n" ++ key ++ ": \"" ++ jwt ++ "\"\n}\npassword: secret\n";
    try std.testing.expectEqualStrings(jwt, try accountJwt(conf, key));
    try std.testing.expectError(error.InvalidAccount, accountJwt(conf, "wrong"));
    try std.testing.expectError(error.AccountNotFound, accountJwt(conf, "AB" ** 28));
    try std.testing.expectError(error.InvalidAccountJwt, accountJwt(key ++ ": secret\n", key));
}

test "a future account revision fails without overflowing or waiting" {
    try std.testing.expectError(error.AccountClockAhead, freshIssuedAt(std.testing.allocator, std.testing.io, "{\"iat\":9223372036854775807}"));
    try std.testing.expectError(error.InvalidIssuedAt, freshIssuedAt(std.testing.allocator, std.testing.io, "{\"iat\":true}"));
}
