//! Unified encoder abstraction for MessagePack and JSON
//!
//! This module provides a common interface for encoding data structures
//! as either MessagePack or JSON, allowing runtime selection via CLI flag.
//!
//! Usage:
//!   var encoder = Encoder.init(allocator, .msgpack);
//!   defer encoder.deinit();
//!
//!   var map = try encoder.createMap();
//!   try map.put(allocator, "key", try encoder.createString("value"));
//!   const bytes = try encoder.encode(map);

const std = @import("std");
const msgpack = @import("msgpack");

pub const log = std.log.scoped(.encoder);

/// Encoding format selection
pub const Format = enum {
    msgpack,
    json,
};

/// Unified encoder supporting both MessagePack and JSON
pub const Encoder = struct {
    allocator: std.mem.Allocator,
    format: Format,

    pub fn init(allocator: std.mem.Allocator, format: Format) Encoder {
        log.info("Format used: {}", .{format});
        return .{
            .allocator = allocator,
            .format = format,
        };
    }

    pub fn deinit(self: *Encoder) void {
        _ = self;
        // No cleanup needed currently
    }

    /// Create a map/object value
    pub fn createMap(self: *Encoder) Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = msgpack.Payload.mapPayload(self.allocator) },
            .json => .{ .json = std.json.Value{ .object = std.json.ObjectMap.empty } },
        };
    }

    /// Create an array value
    pub fn createArray(self: *Encoder, len: usize) !Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = try msgpack.Payload.arrPayload(len, self.allocator) },
            .json => blk: {
                var arr = std.json.Array.init(self.allocator);
                try arr.ensureTotalCapacity(len);
                break :blk .{ .json = std.json.Value{ .array = arr } };
            },
        };
    }

    /// Create a string value
    pub fn createString(self: *Encoder, s: []const u8) !Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = try msgpack.Payload.strToPayload(s, self.allocator) },
            .json => .{ .json = std.json.Value{ .string = try self.allocator.dupe(u8, s) } },
        };
    }

    /// Create an integer value
    pub fn createInt(self: *Encoder, i: i64) Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = msgpack.Payload{ .int = i } },
            .json => .{ .json = std.json.Value{ .integer = i } },
        };
    }

    /// Create a float value
    pub fn createFloat(self: *Encoder, f: f64) Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = msgpack.Payload{ .float = f } },
            .json => .{ .json = std.json.Value{ .float = f } },
        };
    }

    /// Create a boolean value
    pub fn createBool(self: *Encoder, b: bool) Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = msgpack.Payload{ .bool = b } },
            .json => .{ .json = std.json.Value{ .bool = b } },
        };
    }

    /// Create a null value
    pub fn createNull(self: *Encoder) Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = msgpack.Payload{ .nil = {} } },
            .json => .{ .json = std.json.Value.null },
        };
    }

    /// Encode a value to bytes
    pub fn encode(self: *Encoder, value: Value) ![]const u8 {
        return switch (self.format) {
            .msgpack => try self.encodeMsgpack(value),
            .json => try self.encodeJson(value),
        };
    }

    /// Encode as MessagePack
    ///
    /// Caller owns the returned byte slice and must free it.
    fn encodeMsgpack(self: *Encoder, value: Value) ![]const u8 {
        const msgpack_value = switch (value) {
            .msgpack => |mp| mp,
            .json => return error.FormatMismatch,
        };

        // Use Writer.Allocating for dynamic buffer.
        // deinit covers the error path; toOwnedSlice moves the buffer out on success.
        var out: std.Io.Writer.Allocating = .init(self.allocator);
        defer out.deinit();

        // PackerIO takes a reader by signature, but only `read` ever touches it —
        // `write` does not. Hand it an empty reader rather than uninitialized stack.
        var empty: [0]u8 = .{};
        var reader = std.Io.Reader.fixed(&empty);

        var packer = msgpack.PackerIO.init(&reader, &out.writer);
        try packer.write(msgpack_value);

        return try out.toOwnedSlice();
    }

    /// Encode as JSON
    fn encodeJson(self: *Encoder, value: Value) ![]const u8 {
        const json_value = switch (value) {
            .json => |js| js,
            .msgpack => return error.FormatMismatch,
        };

        // deinit covers the error path; toOwnedSlice moves the buffer out on success.
        var out: std.Io.Writer.Allocating = .init(self.allocator);
        defer out.deinit();

        try std.json.Stringify.value(json_value, .{ .whitespace = .indent_2 }, &out.writer);
        return try out.toOwnedSlice();
    }
};

/// Tagged union for either MessagePack or JSON values
pub const Value = union(Format) {
    msgpack: msgpack.Payload,
    json: std.json.Value,

    /// Put a key-value pair into a map
    pub fn put(self: *Value, allocator: std.mem.Allocator, key: []const u8, value: Value) !void {
        switch (self.*) {
            .msgpack => |*mp| {
                const val_mp = switch (value) {
                    .msgpack => |v| v,
                    .json => return error.FormatMismatch,
                };
                try mp.mapPut(key, val_mp);
            },
            .json => |*js| {
                // JSON Value should be an object
                if (js.* != .object) return error.NotAnObject;

                const val_js = switch (value) {
                    .json => |v| v,
                    .msgpack => return error.FormatMismatch,
                };

                try js.object.put(allocator, key, val_js);
            },
        }
    }

    /// Set an array element at index
    pub fn setIndex(self: *Value, index: usize, value: Value) !void {
        switch (self.*) {
            .msgpack => |*mp| {
                const val_mp = switch (value) {
                    .msgpack => |v| v,
                    .json => return error.FormatMismatch,
                };
                mp.arr[index] = val_mp;
            },
            .json => |*js| {
                // JSON Value should be an array
                if (js.* != .array) return error.NotAnArray;

                const val_js = switch (value) {
                    .json => |v| v,
                    .msgpack => return error.FormatMismatch,
                };

                // For JSON arrays, we need to append items in order
                // Use appendAssumeCapacity since we already reserved capacity
                js.array.appendAssumeCapacity(val_js);
            },
        }
    }

    /// Free the value's resources
    pub fn free(self: *Value, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .msgpack => |*mp| mp.free(allocator),
            .json => |*js| jsonFree(js, allocator),
        }
    }

    /// Recursively free a JSON value and all nested values
    fn jsonFree(js: *std.json.Value, allocator: std.mem.Allocator) void {
        switch (js.*) {
            .object => |*obj| {
                // Recursively free all nested values in the object
                var it = obj.iterator();
                while (it.next()) |entry| {
                    jsonFree(entry.value_ptr, allocator);
                }
                obj.deinit(allocator);
            },
            .array => |*arr| {
                // Recursively free all elements
                for (arr.items) |*item| {
                    jsonFree(item, allocator);
                }
                arr.deinit();
            },
            .string => |s| allocator.free(s),
            else => {},
        }
    }
};

// Tests
test "encoder: create and encode simple msgpack map" {
    const allocator = std.testing.allocator;

    var encoder = Encoder.init(allocator, .msgpack);
    defer encoder.deinit();

    var map = encoder.createMap();
    defer map.free(allocator);

    try map.put(allocator, "name", try encoder.createString("Alice"));
    try map.put(allocator, "age", encoder.createInt(30));
    try map.put(allocator, "active", encoder.createBool(true));

    const encoded = try encoder.encode(map);
    defer allocator.free(encoded);

    try std.testing.expect(encoded.len > 0);
}

test "encoder: create and encode simple json map" {
    const allocator = std.testing.allocator;

    var encoder = Encoder.init(allocator, .json);
    defer encoder.deinit();

    var map = encoder.createMap();
    defer map.free(allocator);

    try map.put(allocator, "name", try encoder.createString("Alice"));
    try map.put(allocator, "age", encoder.createInt(30));
    try map.put(allocator, "active", encoder.createBool(true));

    const encoded = try encoder.encode(map);
    defer allocator.free(encoded);

    try std.testing.expect(encoded.len > 0);
    // JSON should contain the keys
    try std.testing.expect(std.mem.indexOf(u8, encoded, "name") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "Alice") != null);
}

test "encoder: create and encode array msgpack" {
    const allocator = std.testing.allocator;

    var encoder = Encoder.init(allocator, .msgpack);
    defer encoder.deinit();

    var array = try encoder.createArray(3);
    defer array.free(allocator);

    try array.setIndex(0, try encoder.createString("one"));
    try array.setIndex(1, try encoder.createString("two"));
    try array.setIndex(2, try encoder.createString("three"));

    const encoded = try encoder.encode(array);
    defer allocator.free(encoded);

    try std.testing.expect(encoded.len > 0);
}

test "encoder: create and encode array json" {
    const allocator = std.testing.allocator;

    var encoder = Encoder.init(allocator, .json);
    defer encoder.deinit();

    var array = try encoder.createArray(3);
    defer array.free(allocator);

    try array.setIndex(0, try encoder.createString("one"));
    try array.setIndex(1, try encoder.createString("two"));
    try array.setIndex(2, try encoder.createString("three"));

    const encoded = try encoder.encode(array);
    defer allocator.free(encoded);

    try std.testing.expect(encoded.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "one") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "two") != null);
}

test "encoder: nested structures msgpack" {
    const allocator = std.testing.allocator;

    var encoder = Encoder.init(allocator, .msgpack);
    defer encoder.deinit();

    var map = encoder.createMap();
    defer map.free(allocator);

    try map.put(allocator, "name", try encoder.createString("Bob"));

    var nested = encoder.createMap();
    try nested.put(allocator, "city", try encoder.createString("NYC"));
    try nested.put(allocator, "zip", encoder.createInt(10001));
    try map.put(allocator, "address", nested);

    const encoded = try encoder.encode(map);
    defer allocator.free(encoded);

    try std.testing.expect(encoded.len > 0);
}

test "encoder: nested structures json" {
    const allocator = std.testing.allocator;

    var encoder = Encoder.init(allocator, .json);
    defer encoder.deinit();

    var map = encoder.createMap();
    defer map.free(allocator);

    try map.put(allocator, "name", try encoder.createString("Bob"));

    var nested = encoder.createMap();
    try nested.put(allocator, "city", try encoder.createString("NYC"));
    try nested.put(allocator, "zip", encoder.createInt(10001));
    try map.put(allocator, "address", nested);

    const encoded = try encoder.encode(map);
    defer allocator.free(encoded);

    try std.testing.expect(encoded.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "Bob") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "NYC") != null);
}

test "encoder: all value types msgpack" {
    const allocator = std.testing.allocator;

    var encoder = Encoder.init(allocator, .msgpack);
    defer encoder.deinit();

    var map = encoder.createMap();
    defer map.free(allocator);

    try map.put(allocator, "string", try encoder.createString("test"));
    try map.put(allocator, "int", encoder.createInt(42));
    try map.put(allocator, "float", encoder.createFloat(3.14));
    try map.put(allocator, "bool_true", encoder.createBool(true));
    try map.put(allocator, "bool_false", encoder.createBool(false));
    try map.put(allocator, "null", encoder.createNull());

    const encoded = try encoder.encode(map);
    defer allocator.free(encoded);

    try std.testing.expect(encoded.len > 0);
}

test "encoder: all value types json" {
    const allocator = std.testing.allocator;

    var encoder = Encoder.init(allocator, .json);
    defer encoder.deinit();

    var map = encoder.createMap();
    defer map.free(allocator);

    try map.put(allocator, "string", try encoder.createString("test"));
    try map.put(allocator, "int", encoder.createInt(42));
    try map.put(allocator, "float", encoder.createFloat(3.14));
    try map.put(allocator, "bool_true", encoder.createBool(true));
    try map.put(allocator, "bool_false", encoder.createBool(false));
    try map.put(allocator, "null", encoder.createNull());

    const encoded = try encoder.encode(map);
    defer allocator.free(encoded);

    try std.testing.expect(encoded.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "test") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "null") != null);
}

test "encoder: format selection from runtime value" {
    const allocator = std.testing.allocator;

    // Simulate CLI flag parsing: default is msgpack
    const default_format: Format = .msgpack;
    var encoder1 = Encoder.init(allocator, default_format);
    defer encoder1.deinit();

    var map1 = encoder1.createMap();
    defer map1.free(allocator);
    try map1.put(allocator, "test", try encoder1.createString("msgpack"));
    const encoded1 = try encoder1.encode(map1);
    defer allocator.free(encoded1);

    // MessagePack is binary, should NOT contain readable "test" in the encoded output
    // (it will have the key, but in binary format)
    try std.testing.expect(encoded1.len > 0);

    // Simulate CLI flag parsing: --json flag
    const json_format: Format = .json;
    var encoder2 = Encoder.init(allocator, json_format);
    defer encoder2.deinit();

    var map2 = encoder2.createMap();
    defer map2.free(allocator);
    try map2.put(allocator, "test", try encoder2.createString("json"));
    const encoded2 = try encoder2.encode(map2);
    defer allocator.free(encoded2);

    // JSON is text, should contain readable "test" and "json"
    try std.testing.expect(std.mem.indexOf(u8, encoded2, "test") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded2, "json") != null);
}
