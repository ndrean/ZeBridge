//! Streaming MessagePack encoder that writes directly to a pre-allocated buffer.
//! Uses only direct buffer writes — no std.Io dependency — so it compiles on
//! any stable Zig version including 0.16.

const std = @import("std");

pub const StreamingEncoder = struct {
    buffer: []u8,
    pos: usize,

    pub fn init(buffer: []u8) StreamingEncoder {
        return .{ .buffer = buffer, .pos = 0 };
    }

    pub fn reset(self: *StreamingEncoder) void {
        self.pos = 0;
    }

    pub fn getWritten(self: *const StreamingEncoder) []const u8 {
        return self.buffer[0..self.pos];
    }

    pub fn getPos(self: *const StreamingEncoder) usize {
        return self.pos;
    }

    pub fn setPos(self: *StreamingEncoder, new_pos: usize) void {
        self.pos = new_pos;
    }

    // ---- low-level write helpers ----

    pub fn writeByte(self: *StreamingEncoder, byte: u8) !void {
        if (self.pos >= self.buffer.len) return error.NoSpaceLeft;
        self.buffer[self.pos] = byte;
        self.pos += 1;
    }

    fn writeAll(self: *StreamingEncoder, bytes: []const u8) !void {
        if (self.pos + bytes.len > self.buffer.len) return error.NoSpaceLeft;
        @memcpy(self.buffer[self.pos..][0..bytes.len], bytes);
        self.pos += bytes.len;
    }

    fn writeIntBig(self: *StreamingEncoder, comptime T: type, value: T) !void {
        const size = @sizeOf(T);
        if (self.pos + size > self.buffer.len) return error.NoSpaceLeft;
        std.mem.writeInt(T, self.buffer[self.pos..][0..size], value, .big);
        self.pos += size;
    }

    // ---- MessagePack header writers ----

    pub fn writeArrayHeader(self: *StreamingEncoder, len: usize) !void {
        if (len < 16) {
            try self.writeByte(@as(u8, @intCast(0x90 | len)));
        } else if (len < 65536) {
            try self.writeByte(0xdc);
            try self.writeIntBig(u16, @intCast(len));
        } else {
            try self.writeByte(0xdd);
            try self.writeIntBig(u32, @intCast(len));
        }
    }

    pub fn writeMapHeader(self: *StreamingEncoder, len: usize) !void {
        if (len < 16) {
            try self.writeByte(@as(u8, @intCast(0x80 | len)));
        } else if (len < 65536) {
            try self.writeByte(0xde);
            try self.writeIntBig(u16, @intCast(len));
        } else {
            try self.writeByte(0xdf);
            try self.writeIntBig(u32, @intCast(len));
        }
    }

    pub fn writeString(self: *StreamingEncoder, v: []const u8) !void {
        if (v.len < 32) {
            try self.writeByte(@as(u8, @intCast(0xa0 | v.len)));
        } else if (v.len < 256) {
            try self.writeByte(0xd9);
            try self.writeByte(@intCast(v.len));
        } else if (v.len < 65536) {
            try self.writeByte(0xda);
            try self.writeIntBig(u16, @intCast(v.len));
        } else {
            try self.writeByte(0xdb);
            try self.writeIntBig(u32, @intCast(v.len));
        }
        try self.writeAll(v);
    }

    pub fn writeInt(self: *StreamingEncoder, value: i64) !void {
        if (value >= 0 and value < 128) {
            try self.writeByte(@intCast(value));
        } else if (value < 0 and value >= -32) {
            try self.writeByte(@as(u8, @bitCast(@as(i8, @intCast(value)))));
        } else if (value >= 0 and value < 256) {
            try self.writeByte(0xcc);
            try self.writeByte(@intCast(value));
        } else if (value >= 0 and value < 65536) {
            try self.writeByte(0xcd);
            try self.writeIntBig(u16, @intCast(value));
        } else if (value >= 0) {
            try self.writeByte(0xcf);
            try self.writeIntBig(u64, @intCast(value));
        } else if (value >= -128) {
            try self.writeByte(0xd0);
            try self.writeByte(@bitCast(@as(i8, @intCast(value))));
        } else if (value >= -32768) {
            try self.writeByte(0xd1);
            try self.writeIntBig(i16, @intCast(value));
        } else {
            try self.writeByte(0xd3);
            try self.writeIntBig(i64, value);
        }
    }

    pub fn writeNull(self: *StreamingEncoder) !void {
        try self.writeByte(0xc0);
    }

    pub fn writeBool(self: *StreamingEncoder, value: bool) !void {
        try self.writeByte(if (value) 0xc3 else 0xc2);
    }

    // ---- high-level snapshot encoding helpers ----

    pub fn beginBatch(self: *StreamingEncoder, estimated_rows: usize) !void {
        try self.writeArrayHeader(estimated_rows);
    }

    pub fn beginRow(self: *StreamingEncoder, column_count: usize) !void {
        try self.writeArrayHeader(column_count);
    }

    pub fn writeField(self: *StreamingEncoder, value: ?[]const u8) !void {
        if (value) |v| {
            try self.writeString(v);
        } else {
            try self.writeNull();
        }
    }
};

// ---- tests (same assertions as before) ----

test "StreamingEncoder - write array header" {
    var buffer: [256]u8 = undefined;
    var encoder = StreamingEncoder.init(&buffer);
    try encoder.writeArrayHeader(5);
    const written = encoder.getWritten();
    try std.testing.expectEqual(@as(u8, 0x95), written[0]);
}

test "StreamingEncoder - write string" {
    var buffer: [256]u8 = undefined;
    var encoder = StreamingEncoder.init(&buffer);
    try encoder.writeString("hello");
    const written = encoder.getWritten();
    try std.testing.expectEqual(@as(u8, 0xa5), written[0]);
    try std.testing.expectEqualStrings("hello", written[1..6]);
}

test "StreamingEncoder - write null" {
    var buffer: [256]u8 = undefined;
    var encoder = StreamingEncoder.init(&buffer);
    try encoder.writeNull();
    const written = encoder.getWritten();
    try std.testing.expectEqual(@as(u8, 0xc0), written[0]);
}

test "StreamingEncoder - write field" {
    var buffer: [256]u8 = undefined;
    var encoder = StreamingEncoder.init(&buffer);
    try encoder.writeField("test");
    try encoder.writeField(null);
    const written = encoder.getWritten();
    try std.testing.expectEqual(@as(u8, 0xa4), written[0]);
    try std.testing.expectEqualStrings("test", written[1..5]);
    try std.testing.expectEqual(@as(u8, 0xc0), written[5]);
}

test "StreamingEncoder - snapshot row encoding" {
    var buffer: [256]u8 = undefined;
    var encoder = StreamingEncoder.init(&buffer);
    try encoder.beginBatch(1);
    try encoder.beginRow(3);
    try encoder.writeField("1");
    try encoder.writeField("Alice");
    try encoder.writeField("30");
    const written = encoder.getWritten();
    try std.testing.expectEqual(@as(u8, 0x91), written[0]);
    try std.testing.expectEqual(@as(u8, 0x93), written[1]);
}
