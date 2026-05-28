// Copyright (c) 2025 g41797
// SPDX-License-Identifier: MIT

/// String formatting utility with automatic buffer expansion.
/// Wraps an Appendable buffer for formatted output operations.
pub const Formatter = @This();

/// The underlying buffer for formatted output.
formatbuf: Appendable = .{},
/// Current position in the buffer.
pos: usize = 0,

/// Creates a new formatter with the given allocator and initial buffer size.
pub fn init(allocator: Allocator, len: usize) !Formatter {
    var frmtr: Formatter = .{};

    try frmtr.formatbuf.init(allocator, len, 256);

    return frmtr;
}

/// Resets the formatter for reuse, clearing all buffered data.
pub fn reset(frmtr: *Formatter) void {
    frmtr.pos = 0;
    frmtr.formatbuf.reset();
}

/// Releases all allocated memory.
pub fn deinit(frmtr: *Formatter) void {
    frmtr.formatbuf.deinit();
}

/// Formats a string using the given format and arguments.
/// Automatically expands the buffer if needed.
/// Returns the formatted string or null on empty output.
pub fn sprintf(frmtr: *Formatter, comptime fmt: []const u8, args: anytype) !?[]const u8 {
    while (true) {
        if (frmtr.tryformat(fmt, args)) |_| {
            return frmtr.formatbuf.body();
        } else |ferr| switch (ferr) {
            error.NoSpaceLeft => {
                _ = try frmtr.formatbuf.alloc(frmtr.formatbuf.buffer.?.len + 256);
                continue;
            },
            else => {
                return ferr;
            },
        }
    }
}

fn tryformat(frmtr: *Formatter, comptime fmt: []const u8, args: anytype) !void {
    frmtr.pos = 0;
    const buffer = frmtr.formatbuf.buffer.?;
    const result = std.fmt.bufPrint(buffer, fmt, args) catch |err| switch (err) {
        error.NoSpaceLeft => return error.NoSpaceLeft,
    };
    frmtr.pos = result.len;
    try frmtr.formatbuf.change(frmtr.pos);
}

/// Converts a value to its JSON string representation.
/// Automatically expands the buffer if needed.
pub fn stringify(frmtr: *Formatter, value: anytype, options: StringifyOptions) !?[]const u8 {
    while (true) {
        if (frmtr.trystringify(value, options)) |_| {
            return frmtr.formatbuf.body();
        } else |ferr| switch (ferr) {
            error.NoSpaceLeft => {
                _ = try frmtr.formatbuf.alloc(frmtr.formatbuf.buffer.?.len + 256);
                continue;
            },
            else => {
                return ferr;
            },
        }
    }
}

const BufWriter = struct {
    buf: []u8,
    pos: usize = 0,
    const Error = error{NoSpaceLeft};
    pub fn writeByte(self: *BufWriter, byte: u8) Error!void {
        if (self.pos >= self.buf.len) return error.NoSpaceLeft;
        self.buf[self.pos] = byte;
        self.pos += 1;
    }
    pub fn writeAll(self: *BufWriter, bytes: []const u8) Error!void {
        if (self.pos + bytes.len > self.buf.len) return error.NoSpaceLeft;
        @memcpy(self.buf[self.pos..][0..bytes.len], bytes);
        self.pos += bytes.len;
    }
    pub fn writeBytesNTimes(self: *BufWriter, bytes: []const u8, n: usize) Error!void {
        for (0..n) |_| try self.writeAll(bytes);
    }
    pub fn print(self: *BufWriter, comptime fmt: []const u8, args: anytype) Error!void {
        const result = std.fmt.bufPrint(self.buf[self.pos..], fmt, args) catch return error.NoSpaceLeft;
        self.pos += result.len;
    }
};

fn trystringify(frmtr: *Formatter, value: anytype, options: StringifyOptions) !void {
    frmtr.pos = 0;
    const buffer = frmtr.formatbuf.buffer.?;
    var bw = BufWriter{ .buf = buffer };
    json.stringify(value, options, &bw) catch |err| switch (err) {
        error.NoSpaceLeft => return error.NoSpaceLeft,
        else => return err,
    };
    frmtr.pos = bw.pos;
    try frmtr.formatbuf.change(frmtr.pos);
}

const std = @import("std");
const json = std.json;
const StringifyOptions = json.Stringify.Options;

const Appendable = @import("Appendable.zig");

const Allocator = std.mem.Allocator;
