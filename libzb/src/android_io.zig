//! Android's DNS (NOTES §10ln). Zig treats Android as Linux and resolves host names
//! itself, reading /etc/resolv.conf for the nameservers. Android has no such file: Zig
//! then asks 127.0.0.1:53, where nothing answers, and every host name fails — enrollment
//! ("enroll: https://… unreachable") and the NATS dial alike. Android resolves through
//! libc (bionic, which asks netd). So on Android, libzb's `Io` keeps Zig's Threaded
//! implementation for everything but `netLookup`, which calls getaddrinfo. Elsewhere
//! `io(t)` is `t.io()`, unchanged.
const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const net = Io.net;

pub const is_android = builtin.abi.isAndroid();

/// `t.io()`, with host names resolved by the platform on Android.
pub fn io(t: *Io.Threaded) Io {
    const base = t.io();
    if (comptime !is_android) return base;
    return .{ .userdata = base.userdata, .vtable = androidVtable(base.vtable) };
}

// One copy of Threaded's table with `netLookup` replaced, built on first use. Every
// Threaded hands out the same table, so one copy serves them all.
var android_vtable: Io.VTable = undefined;
var android_state = std.atomic.Value(u8).init(0); // 0 none, 1 building, 2 ready

fn androidVtable(base: *const Io.VTable) *const Io.VTable {
    if (android_state.load(.acquire) == 2) return &android_vtable;
    if (android_state.cmpxchgStrong(0, 1, .acquire, .monotonic) == null) {
        android_vtable = base.*;
        android_vtable.netLookup = netLookup;
        android_state.store(2, .release);
    } else while (android_state.load(.acquire) != 2) std.atomic.spinLoopHint();
    return &android_vtable;
}

/// `HostName.lookup`'s contract: addresses, then one canonical name, and the queue closed
/// on return. At most 15 addresses, so a queue of 16 never blocks.
fn netLookup(
    userdata: ?*anyopaque,
    host_name: net.HostName,
    resolved: *Io.Queue(net.HostName.LookupResult),
    options: net.HostName.LookupOptions,
) net.HostName.LookupError!void {
    const c = std.c;
    const t: *Io.Threaded = @ptrCast(@alignCast(userdata));
    const tio = t.io();
    defer resolved.close(tio);

    var name_z: [net.HostName.max_len + 1]u8 = undefined;
    @memcpy(name_z[0..host_name.bytes.len], host_name.bytes);
    name_z[host_name.bytes.len] = 0;

    var hints = std.mem.zeroes(c.addrinfo);
    hints.family = if (options.family) |f| switch (f) {
        .ip4 => c.AF.INET,
        .ip6 => c.AF.INET6,
    } else c.AF.UNSPEC;
    hints.socktype = c.SOCK.STREAM;
    hints.flags = .{ .ADDRCONFIG = true }; // no AAAA asked on a network with no IPv6
    var res: ?*c.addrinfo = null;
    if (@intFromEnum(c.getaddrinfo(@ptrCast(&name_z), null, &hints, &res)) != 0) return error.UnknownHostName;
    defer if (res) |r| c.freeaddrinfo(r);

    var n: usize = 0;
    var it = res;
    while (it) |ai| : (it = ai.next) {
        if (n == 15) break;
        const sa = ai.addr orelse continue;
        const addr: net.IpAddress = switch (sa.family) {
            c.AF.INET => blk: {
                const in: *const c.sockaddr.in = @ptrCast(@alignCast(sa));
                break :blk .{ .ip4 = .{ .bytes = @bitCast(in.addr), .port = options.port } };
            },
            c.AF.INET6 => blk: {
                const in6: *const c.sockaddr.in6 = @ptrCast(@alignCast(sa));
                break :blk .{ .ip6 = .{ .bytes = in6.addr, .port = options.port } };
            },
            else => continue,
        };
        resolved.putOne(tio, .{ .address = addr }) catch |err| switch (err) {
            error.Closed => unreachable, // closed only by this function
            error.Canceled => |e| return e,
        };
        n += 1;
    }
    if (n == 0) return error.NoAddressReturned;

    if (options.canonical_name_buffer) |buf| {
        @memcpy(buf[0..host_name.bytes.len], host_name.bytes);
        resolved.putOne(tio, .{ .canonical_name = .{ .bytes = buf[0..host_name.bytes.len] } }) catch |err| switch (err) {
            error.Closed => unreachable,
            error.Canceled => |e| return e,
        };
    }
}
