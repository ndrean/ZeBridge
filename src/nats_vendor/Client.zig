// Copyright (c) 2025 g41797
// SPDX-License-Identifier: MIT

const SEC_TIMEOUT_MS = 1_000;
const MIN_TIMEOUT_MS = SEC_TIMEOUT_MS * 60;
const INFINITE_TIMEOUT_MS = -1;

/// Low-level TCP client for NATS protocol communication.
/// Handles socket operations with non-blocking I/O.
pub const Client = @This();

/// The underlying TCP stream.
stream: Stream = undefined,
/// Indicates whether the client is connected.
connected: bool = false,
/// Atomic bool for signaling attention/interrupts.
attention: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

/// Connects to a NATS server at the specified address and port.
pub fn connect(allocator: Allocator, co: protocol.ConnectOpts) !Client {
    _ = allocator;
    var host: []const u8 = protocol.DefaultAddr;

    if (co.addr != null) {
        host = co.addr.?;
    }

    var prt: u16 = protocol.DefaultPort;

    if (co.port != null) {
        prt = co.port.?;
    }
    var client: Client = .{};

    client.stream = try tcpConnect(host, prt);
    client.connected = true;
    errdefer client.close();
    try setSockNONBLOCK(client.stream.handle);

    return client;
}

/// Closes the TCP connection.
pub fn close(cl: *Client) void {
    if (!cl.connected) {
        return;
    }

    cl.stream.close();
    cl.connected = false;
    return;
}

/// Writes all bytes to the socket, blocking until complete.
pub fn writeAll(cl: *Client, bytes: []const u8) !void {
    if (!cl.connected) {
        return error.NotConnected;
    }

    var _poll: pollfd = .{
        .fd = cl.stream.handle,
        .events = POLL.OUT | POLL.HUP | POLL.NVAL,
        .revents = 0,
    };

    var wpoll: [*c]pollfd = &_poll;

    var index: usize = 0;
    while (index < bytes.len) {
        wpoll[0].revents = 0;

        const pollstatus = system.poll(wpoll, 1, INFINITE_TIMEOUT_MS);
        if (pollstatus == 0) {
            continue;
        }
        if (pollstatus < 0) {
            return error.WriteAllPollError;
        }

        if (wpoll[0].revents & POLL.OUT == POLL.OUT) {
            index += cl.stream.write(bytes[index..]) catch |err| switch (err) {
                error.WouldBlock => continue,
                else => return err,
            };
            continue;
        }

        if (wpoll[0].revents & POLL.HUP == POLL.HUP) {
            return error.WriteAllHUPError;
        }

        if (wpoll[0].revents & POLL.NVAL == POLL.NVAL) {
            return error.WriteAllNVALError;
        }
    }

    return;
}

/// Writes multiple buffers to the socket using vectored I/O.
pub fn writevAll(cl: *Client, iovecs: []posix.iovec_const) !void {
    if (!cl.connected) {
        return error.NotConnected;
    }

    if (iovecs.len == 0) {
        return;
    }

    var _poll: pollfd = .{
        .fd = cl.stream.handle,
        .events = POLL.OUT | POLL.HUP | POLL.NVAL,
        .revents = 0,
    };

    var wpoll: [*c]pollfd = &_poll;

    var i: usize = 0;
    while (true) {
        wpoll[0].revents = 0;

        const pollstatus = system.poll(wpoll, 1, INFINITE_TIMEOUT_MS);
        if (pollstatus == 0) {
            continue;
        }
        if (pollstatus < 0) {
            return error.WriteAllPollError;
        }

        if (wpoll[0].revents & POLL.HUP == POLL.HUP) {
            return error.WriteAllHUPError;
        }

        if (wpoll[0].revents & POLL.NVAL == POLL.NVAL) {
            return error.WriteAllNVALError;
        }

        var amt = cl.stream.writev(iovecs[i..]) catch |err| switch (err) {
            error.WouldBlock => continue,
            else => return err,
        };
        while (amt >= iovecs[i].len) {
            amt -= iovecs[i].len;
            i += 1;
            if (i >= iovecs.len) return;
        }
        iovecs[i].base += amt;
        iovecs[i].len -= amt;
    }
}

/// Reads a single byte from the socket with a 1-second timeout.
pub fn readByte(cl: *Client) !u8 {
    if (!cl.connected) {
        return error.NotConnected;
    }

    var _poll: pollfd = .{
        .fd = cl.stream.handle,
        .events = POLL.IN,
        .revents = 0,
    };

    const rpoll: [*c]pollfd = &_poll;

    var byte: [1]u8 = undefined;

    while (true) {
        const pollstatus = system.poll(rpoll, 1, SEC_TIMEOUT_MS);

        switch (pollstatus) {
            1 => {
                byte[0] = 0;
                const rlen = try cl.stream.read(&byte);
                if (rlen == 0) {
                    return error.WouldBlock;
                }
                return byte[0];
            },
            0 => return error.WouldBlock,
            else => return error.ReadBytePollError,
        }
    }
}

/// Reads data into the buffer, blocking until the buffer is full or stream ends.
pub fn readAll(cl: *Client, buffer: []u8) !usize {
    if (!cl.connected) {
        return error.NotConnected;
    }

    var _poll: pollfd = .{
        .fd = cl.stream.handle,
        .events = POLL.IN,
        .revents = 0,
    };

    var rpoll: [*c]pollfd = &_poll;

    var index: usize = 0;

    while (index < buffer.len) {
        if (cl.wasRaised()) {
            return error.WasCancelled;
        }

        rpoll[0].revents = 0;

        const pollstatus = system.poll(rpoll, 1, INFINITE_TIMEOUT_MS);

        if (pollstatus == 0) { //strange for INFINITE_TIMEOUT_MS
            continue;
        }

        if (pollstatus == 1) {
            const amt = cl.stream.read(buffer[index..]) catch |err| {
                if (err == error.WouldBlock) {
                    continue;
                }
                return err;
            };
            if (amt == 0) { // end of the stream
                break;
            }
            index += amt;
            continue;
        }

        return error.ReadAllPollError;
    }

    return index;
}

/// Signals an attention event to interrupt blocking operations.
pub fn raiseAttention(cl: *Client) void {
    cl.attention.store(true, .release);
}

/// Checks if an attention event was raised.
pub fn wasRaised(cl: *Client) bool {
    return cl.attention.swap(false, .acq_rel);
}

/// Sets a socket to non-blocking mode.
pub fn setSockNONBLOCK(sock: posix.socket_t) !void {
    if (builtin.os.tag == .windows) {
        var mode: c_ulong = 1;
        if (windows.ws2_32.ioctlsocket(sock, windows.ws2_32.FIONBIO, &mode) == windows.ws2_32.SOCKET_ERROR) {
            switch (windows.ws2_32.WSAGetLastError()) {
                .WSANOTINITIALISED => unreachable,
                .WSAENETDOWN => return error.NetworkSubsystemFailed,
                .WSAENOTSOCK => return error.FileDescriptorNotASocket,
                // !!! handle more errors !!!
                else => |err| return windows.unexpectedWSAError(err),
            }
        }
    } else {
        var fl_flags = posix.fcntl(sock, posix.F.GETFL, 0) catch |err| switch (err) {
            error.FileBusy => unreachable,
            error.Locked => unreachable,
            error.PermissionDenied => unreachable,
            error.DeadLock => unreachable,
            error.LockedRegionLimitExceeded => unreachable,
            else => |e| return e,
        };
        fl_flags |= 1 << @bitOffsetOf(system.O, "NONBLOCK");
        _ = posix.fcntl(sock, posix.F.SETFL, fl_flags) catch |err| switch (err) {
            error.FileBusy => unreachable,
            error.Locked => unreachable,
            error.PermissionDenied => unreachable,
            error.DeadLock => unreachable,
            error.LockedRegionLimitExceeded => unreachable,
            else => |e| return e,
        };
    }
}

const std = @import("std");
const builtin = @import("builtin");
const protocol = @import("protocol.zig");
const posix = std.posix;
const Socket = posix.socket_t;
const Allocator = std.mem.Allocator;
const windows = std.os.windows;
const system = posix.system;
const pollfd = posix.pollfd;
const POLL = system.POLL;

const c_net = @cImport({
    @cInclude("sys/socket.h");
    @cInclude("netdb.h");
    @cInclude("netinet/in.h");
});

/// Thin wrapper around a TCP socket fd, providing read/write/writev/close.
pub const Stream = struct {
    handle: posix.socket_t,

    pub fn write(s: Stream, bytes: []const u8) !usize {
        return posix.write(s.handle, bytes);
    }

    pub fn read(s: Stream, buf: []u8) !usize {
        return posix.read(s.handle, buf);
    }

    pub fn writev(s: Stream, iovecs: []posix.iovec_const) !usize {
        return posix.writev(s.handle, iovecs);
    }

    pub fn close(s: Stream) void {
        posix.close(s.handle);
    }
};

/// Opens a TCP connection to host:port using getaddrinfo for name resolution.
fn tcpConnect(host: []const u8, port: u16) !Stream {
    var host_buf: [256]u8 = undefined;
    if (host.len >= host_buf.len) return error.HostNameTooLong;
    @memcpy(host_buf[0..host.len], host);
    host_buf[host.len] = 0;

    var port_buf: [8]u8 = undefined;
    const port_slice = std.fmt.bufPrint(&port_buf, "{d}", .{port}) catch unreachable;
    port_buf[port_slice.len] = 0;

    var hints = std.mem.zeroes(c_net.struct_addrinfo);
    hints.ai_family = c_net.AF_UNSPEC;
    hints.ai_socktype = c_net.SOCK_STREAM;

    var res: ?*c_net.struct_addrinfo = null;
    if (c_net.getaddrinfo(&host_buf, &port_buf, &hints, &res) != 0)
        return error.HostResolutionFailed;
    defer c_net.freeaddrinfo(res);

    var ai = res;
    while (ai) |info| : (ai = info.ai_next) {
        const addr = info.ai_addr orelse continue;
        const sock = posix.socket(
            @intCast(info.ai_family),
            @intCast(info.ai_socktype),
            @intCast(info.ai_protocol),
        ) catch continue;
        posix.connect(sock, @ptrCast(addr), @intCast(info.ai_addrlen)) catch {
            posix.close(sock);
            continue;
        };
        return .{ .handle = sock };
    }
    return error.ConnectionFailed;
}
