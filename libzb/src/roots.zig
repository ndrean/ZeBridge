//! The trusted roots libzb carries on iOS and Android, where Zig reads no trust store:
//! Mozilla's CA bundle as curl publishes it (roots/cacert.pem, refreshed by
//! scripts/refresh-roots.sh). When the host names no `caFile`, the bundle is written once
//! beside the replica and every https:// and tls:// connection checks against it — so no
//! app or binding ships certificates of its own. Desktop builds embed none: they read the
//! system's store.
const std = @import("std");
const builtin = @import("builtin");

pub const needed = builtin.os.tag == .ios or builtin.abi.isAndroid();
pub const bundle: []const u8 = if (needed) @embedFile("roots/cacert.pem") else "";

/// The bundle as a file beside `replica_path` (`zb-roots.pem`), rewritten when missing or
/// different (a newer libzb carries newer roots). Null on desktop, or when it cannot be
/// written: then TLS fails with its own error, which names the certificate.
pub fn fileBeside(a: std.mem.Allocator, replica_path: []const u8) ?[]u8 {
    if (comptime !needed) return null;
    const dir = std.fs.path.dirname(replica_path) orelse ".";
    const path = std.fmt.allocPrint(a, "{s}/zb-roots.pem", .{dir}) catch return null;
    if (holds(a, path)) return path;
    write(a, path) catch {
        a.free(path);
        return null;
    };
    return path;
}

/// Whether `path` already holds exactly the bundle.
fn holds(a: std.mem.Allocator, path: []const u8) bool {
    const pz = a.dupeZ(u8, path) catch return false;
    defer a.free(pz);
    const fd = std.posix.openat(std.posix.AT.FDCWD, pz, .{ .ACCMODE = .RDONLY }, 0) catch return false;
    defer _ = std.posix.system.close(fd);
    const buf = a.alloc(u8, bundle.len + 1) catch return false;
    defer a.free(buf);
    var n: usize = 0;
    while (n < buf.len) {
        const r = std.c.read(fd, buf[n..].ptr, buf.len - n);
        if (r <= 0) break;
        n += @intCast(r);
    }
    return n == bundle.len and std.mem.eql(u8, buf[0..n], bundle);
}

/// Written whole, then renamed into place: a reader never sees half a bundle.
fn write(a: std.mem.Allocator, path: []const u8) !void {
    const tmp = try std.fmt.allocPrintSentinel(a, "{s}.tmp", .{path}, 0);
    defer a.free(tmp);
    const final = try a.dupeZ(u8, path);
    defer a.free(final);
    const fd = try std.posix.openat(std.posix.AT.FDCWD, tmp, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, 0o644);
    var off: usize = 0;
    while (off < bundle.len) {
        const r = std.c.write(fd, bundle[off..].ptr, bundle.len - off);
        if (r <= 0) {
            _ = std.posix.system.close(fd);
            _ = std.c.unlink(tmp);
            return error.ShortWrite;
        }
        off += @intCast(r);
    }
    _ = std.posix.system.close(fd);
    if (std.c.rename(tmp, final) != 0) {
        _ = std.c.unlink(tmp);
        return error.RenameFailed;
    }
}
