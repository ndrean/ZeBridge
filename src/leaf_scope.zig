//! Restricted leaves use a reserved, two-token principal: <leaf>.<device>.
//! The leaf id is an explicitly registered namespace, never a hub principal.
const std = @import("std");

pub fn token(value: []const u8) bool {
    if (value.len == 0 or value.len > 128) return false;
    for (value) |ch| if (!(std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '-')) return false;
    return true;
}

/// A restricted leaf's id: the grammar's `leaf_prefix`, then at least one more character.
pub fn leafId(prefix: []const u8, value: []const u8) bool {
    return value.len > prefix.len and value.len <= 64 and std.mem.startsWith(u8, value, prefix) and token(value);
}

/// A principal: one name that does NOT start with the leaf prefix, or `<leaf id>.<device>`.
/// The prefix keeps the two apart for good, wherever a principal is born (an invite, an
/// offline mint): a plain `lf-canada` would sit above `lf-canada.alice` in every
/// `<prefix>.<principal>.>` grant (inbox, verdicts, writes).
pub fn principalIn(prefix: []const u8, value: []const u8) bool {
    if (value.len > 128) return false;
    var parts = std.mem.splitScalar(u8, value, '.');
    const first = parts.next().?;
    const device = parts.next() orelse return token(first) and !std.mem.startsWith(u8, first, prefix);
    if (parts.next() != null) return false;
    return leafId(prefix, first) and token(device);
}

test "leaf ids carry the prefix; no other principal may" {
    for ([_][]const u8{ "alice", "lf-leaf1.alice", "lf-canada.phone-2", "leaf1" }) |p|
        try std.testing.expect(principalIn("lf-", p));
    for ([_][]const u8{ "", ".alice", "lf-leaf.", "lf-leaf.alice.other", "lf-leaf.*", "leaf name.alice", "lf-canada", "lf-", "canada.alice", "lf-.alice" }) |p|
        try std.testing.expect(!principalIn("lf-", p));
    try std.testing.expect(leafId("lf-", "lf-canada_1"));
    for ([_][]const u8{ "lf-", "canada", "lf-ca.na", "" }) |p| try std.testing.expect(!leafId("lf-", p));
}

const nats = @import("nats");
const topology = @import("topology.zig");

/// One filtered, read-only hub projection per tenant. A leaf never receives a
/// consumer-create grant on the shared generations bucket: body filters are not ACLs.
/// Sources avoid inheriting the origin's direct-get queue (internal mirrors do).
pub fn ensureProjections(a: std.mem.Allocator, conn: *nats.Connection, topo: *const topology.Topology, domain: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const aa = arena.allocator();
    var tenants: std.StringHashMap(void) = .init(aa);
    try tenants.put(topo.open_tenant, {});
    for (topo.tenants) |tenant| try tenants.put(tenant, {});
    var iterator = tenants.keyIterator();
    while (iterator.next()) |tenant| {
        if (!token(tenant.*)) return error.InvalidProjectionTenant;
        const name = try std.fmt.allocPrint(aa, "KV_{s}_leaf_{s}", .{ topo.kv_generations, tenant.* });
        const filter = try std.fmt.allocPrint(aa, "$KV.{s}.{s}.>", .{ topo.kv_generations, tenant.* });
        const api = try std.fmt.allocPrint(aa, "$JS.{s}.API", .{domain});
        const info_subject = try std.fmt.allocPrint(aa, "{s}.STREAM.INFO.{s}", .{ api, name });
        const info = try conn.request(info_subject, "{}", .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        defer info.deinit();
        const Info = struct {
            config: ?struct {
                subjects: ?[]const []const u8 = null,
                sources: ?[]const struct { name: []const u8, filter_subject: ?[]const u8 = null, subject_transforms: ?[]const std.json.Value = null } = null,
                mirror: ?std.json.Value = null,
                mirror_direct: bool = false,
                max_bytes: i64 = -1,
            } = null,
            @"error": ?struct { err_code: u32 } = null,
        };
        const parsed = try std.json.parseFromSliceLeaky(Info, aa, info.data, .{ .ignore_unknown_fields = true });
        if (parsed.config) |config| {
            const sources = config.sources orelse return error.UnsafeManifestProjection;
            if (sources.len != 1 or config.mirror != null) return error.UnsafeManifestProjection;
            const source = sources[0];
            if (!std.mem.eql(u8, source.name, try std.fmt.allocPrint(aa, "KV_{s}", .{topo.kv_generations})) or
                !std.mem.eql(u8, source.filter_subject orelse "", filter) or config.mirror_direct or
                config.max_bytes < 1 or config.max_bytes > 64 << 20 or
                (config.subjects != null and config.subjects.?.len > 0) or
                (source.subject_transforms != null and source.subject_transforms.?.len > 0)) return error.UnsafeManifestProjection;
            continue;
        }
        if (parsed.@"error" == null or parsed.@"error".?.err_code != 10059) return error.ProjectionLookupFailed;
        const config = .{
            .name = name,
            .storage = "file",
            .allow_direct = true,
            .mirror_direct = false,
            .max_bytes = @as(i64, 64 << 20),
            .max_msgs_per_subject = @as(i64, 1),
            .discard = "old",
            .allow_rollup_hdrs = true,
            .sources = .{.{
                .name = try std.fmt.allocPrint(aa, "KV_{s}", .{topo.kv_generations}),
                .filter_subject = filter,
            }},
        };
        const body = try std.json.Stringify.valueAlloc(aa, config, .{});
        const create_subject = try std.fmt.allocPrint(aa, "{s}.STREAM.CREATE.{s}", .{ api, name });
        const response = try conn.request(create_subject, body, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        defer response.deinit();
        const result = try std.json.parseFromSliceLeaky(Info, aa, response.data, .{ .ignore_unknown_fields = true });
        if (result.@"error" != null or result.config == null) return error.ProjectionCreationFailed;
    }
}
