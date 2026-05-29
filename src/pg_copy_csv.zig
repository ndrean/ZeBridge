//! PostgreSQL COPY CSV format parser
//!
//! We use a CSV format for the snapshot data transferred via COPY command.
//!
//! Format: COPY (...) TO STDOUT WITH (FORMAT csv, HEADER true)
//! - First line: column names (comma-separated)
//! - Following lines: data rows (comma-separated, quoted if needed)
//! - NULL values: empty string or explicit \N
//! - Escaping: double quotes for quotes, standard CSV rules

const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const streaming_encoder = @import("streaming_encoder.zig");
const StreamingEncoder = streaming_encoder.StreamingEncoder;

pub const log = std.log.scoped(.pg_copy_csv);

/// Encode MessagePack array header into a buffer
/// Returns the number of bytes written (1, 3, or 5)
fn encodeArrayHeader(buf: *[5]u8, len: usize) usize {
    if (len < 16) {
        // fixarray (1 byte)
        buf[0] = @as(u8, @intCast(0x90 | len));
        return 1;
    } else if (len < 65536) {
        // array 16 (3 bytes)
        buf[0] = 0xdc;
        buf[1] = @as(u8, @intCast((len >> 8) & 0xff));
        buf[2] = @as(u8, @intCast(len & 0xff));
        return 3;
    } else {
        // array 32 (5 bytes)
        buf[0] = 0xdd;
        buf[1] = @as(u8, @intCast((len >> 24) & 0xff));
        buf[2] = @as(u8, @intCast((len >> 16) & 0xff));
        buf[3] = @as(u8, @intCast((len >> 8) & 0xff));
        buf[4] = @as(u8, @intCast(len & 0xff));
        return 5;
    }
}

/// PQgetCopyData provides a mutable pointer, we can treat it as a slice we own until PQfreemem is called
/// Unescape CSV quoted value in-place (replace "" with ")
/// Returns a slice of the original buffer with the new length.
fn unescapeCsvInPlace(input: []u8) []u8 {
    var write_idx: usize = 0;
    var read_idx: usize = 0;

    while (read_idx < input.len) {
        if (input[read_idx] == '"' and read_idx + 1 < input.len and input[read_idx + 1] == '"') {
            // Found "", write a single " and skip two characters
            input[write_idx] = '"';
            read_idx += 2;
        } else {
            // Normal character, copy it forward
            input[write_idx] = input[read_idx];
            read_idx += 1;
        }
        write_idx += 1;
    }

    return input[0..write_idx];
}

/// A row from CSV data
pub const CsvRow = struct {
    allocator: std.mem.Allocator,
    fields: []CsvField,

    pub fn deinit(self: *CsvRow) void {
        for (self.fields) |csv_field| {
            if (csv_field.value) |v| {
                self.allocator.free(v);
            }
        }
        self.allocator.free(self.fields);
    }

    pub fn getField(self: CsvRow, index: usize) ?CsvField {
        if (index < self.fields.len) {
            return self.fields[index];
        }
        return null;
    }

    pub fn fieldCount(self: CsvRow) usize {
        return self.fields.len;
    }
};

/// Parser for COPY CSV format
pub const CopyCsvParser = struct {
    allocator: std.mem.Allocator,
    conn: ?*c.PGconn,
    header: ?[][]const u8 = null,
    buffer: std.ArrayList(u8),
    // PK tracking for correct keyset pagination in the streaming path.
    // Set via streamToEncoder's pk_col_name parameter; updated per row.
    pk_col_idx: ?usize = null,
    last_pk_buf: [256]u8 = undefined,
    last_pk_len: usize = 0,

    pub fn init(allocator: std.mem.Allocator, conn: ?*c.PGconn) CopyCsvParser {
        return .{
            .allocator = allocator,
            .conn = conn,
            .buffer = .empty,
        };
    }

    pub fn deinit(self: *CopyCsvParser) void {
        if (self.header) |h| {
            for (h) |col| self.allocator.free(col);
            self.allocator.free(h);
        }
        self.buffer.deinit(self.allocator);
    }

    /// Execute PG_COPY command and read all data and parse header
    pub fn executeCopy(self: *CopyCsvParser, query: [:0]const u8) !void {
        // Execute COPY command
        const result = c.PQexec(self.conn, query.ptr);
        defer c.PQclear(result);

        if (c.PQresultStatus(result) != c.PGRES_COPY_OUT) {
            const err_msg = c.PQerrorMessage(self.conn);
            log.err("COPY command failed: {s}", .{err_msg});
            return error.CopyFailed;
        }

        // Read all COPY data into buffer
        self.buffer.clearRetainingCapacity();

        while (true) {
            var buf_ptr: [*c]u8 = undefined;
            const len = c.PQgetCopyData(self.conn, &buf_ptr, 0);

            if (len == -1) {
                // End of COPY data
                break;
            } else if (len == -2) {
                // Error
                const err_msg = c.PQerrorMessage(self.conn);
                log.err("PQgetCopyData failed: {s}", .{err_msg});
                return error.CopyDataFailed;
            } else if (len > 0) {
                const chunk = buf_ptr[0..@intCast(len)];
                try self.buffer.appendSlice(self.allocator, chunk);
                c.PQfreemem(buf_ptr);
            }
        }

        log.debug("COPY CSV received {d} bytes", .{self.buffer.items.len});

        // Parse header (first line)
        try self.parseHeader();
    }

    /// Parse CSV header line
    ///
    /// Sets self.header to array of column names
    ///
    /// Caller owns the memory for column names
    fn parseHeader(self: *CopyCsvParser) !void {
        const data = self.buffer.items;

        // Find first newline
        const newline_pos = std.mem.indexOfScalar(u8, data, '\n') orelse {
            if (data.len == 0) return;
            return error.NoHeaderFound;
        };

        const header_line = data[0..newline_pos];

        // Parse CSV header
        var cols = std.ArrayList([]const u8).empty;
        defer cols.deinit(self.allocator);

        var it = std.mem.splitScalar(u8, header_line, ',');
        while (it.next()) |col_name| {
            const trimmed = std.mem.trim(u8, col_name, " \r");
            try cols.append(self.allocator, try self.allocator.dupe(u8, trimmed));
        }

        self.header = try cols.toOwnedSlice(self.allocator);

        log.debug("COPY CSV header: {d} columns", .{self.header.?.len});
    }

    /// Row iterator for CSV data
    pub const RowIterator = struct {
        parser: *CopyCsvParser,
        line_start: usize,

        pub fn next(self: *RowIterator) !?CsvRow {
            const data = self.parser.buffer.items;

            // Skip header on first call
            if (self.line_start == 0) {
                const first_newline = std.mem.indexOfScalar(u8, data, '\n') orelse return null;
                self.line_start = first_newline + 1;
            }

            // Check if we've reached the end
            if (self.line_start >= data.len) {
                return null;
            }

            // Find next newline
            const line_end = if (std.mem.indexOfScalarPos(u8, data, self.line_start, '\n')) |pos|
                pos
            else
                data.len;

            const line = data[self.line_start..line_end];
            self.line_start = line_end + 1;

            // Skip empty lines
            if (line.len == 0) {
                return try self.next();
            }

            // Parse CSV line
            return try self.parser.parseCsvLine(line);
        }
    };

    /// Parse a single CSV line into fields (optimized with in-place unescaping)
    fn parseCsvLine(self: *CopyCsvParser, line: []const u8) !CsvRow {
        var fields = std.ArrayList(CsvField).empty;
        errdefer {
            for (fields.items) |field| {
                if (field.value) |v| self.allocator.free(v);
            }
            fields.deinit(self.allocator);
        }

        var it = std.mem.splitScalar(u8, line, ',');

        while (it.next()) |raw_field| {
            const trimmed = std.mem.trim(u8, raw_field, " \r");

            // Handle NULL
            if (trimmed.len == 0 or std.mem.eql(u8, trimmed, "\\N")) {
                try fields.append(self.allocator, .{ .value = null });
                continue;
            }

            // Handle quoted values with in-place unescaping
            if (trimmed.len >= 2 and trimmed[0] == '"' and trimmed[trimmed.len - 1] == '"') {
                // Unquote and unescape in-place
                const quoted = trimmed[1 .. trimmed.len - 1];
                // We need to copy first because the buffer is const from the iterator
                const mutable_copy = try self.allocator.dupe(u8, quoted);
                const unescaped = unescapeCsvInPlace(mutable_copy);
                // unescaped is a slice of mutable_copy, so we need to resize it
                const final_value = try self.allocator.realloc(mutable_copy, unescaped.len);
                try fields.append(self.allocator, .{ .value = final_value });
            } else {
                // Plain value
                try fields.append(self.allocator, .{ .value = try self.allocator.dupe(u8, trimmed) });
            }
        }

        return .{
            .fields = try fields.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }
    /// [OLD] Parse a single CSV line into fields
    // fn parseCsvLine(self: *CopyCsvParser, line: []const u8) !CsvRow {
    //     var fields = std.ArrayList(CsvField).empty;
    //     errdefer {
    //         for (fields.items) |field| {
    //             if (field.value) |v| self.allocator.free(v);
    //         }
    //         fields.deinit(self.allocator);
    //     }

    //     var col_idx: usize = 0;
    //     var it = std.mem.splitScalar(u8, line, ',');

    //     while (it.next()) |raw_field| : (col_idx += 1) {
    //         const trimmed = std.mem.trim(u8, raw_field, " \r");

    //         // Handle NULL
    //         if (trimmed.len == 0 or std.mem.eql(u8, trimmed, "\\N")) {
    //             try fields.append(self.allocator, .{ .value = null });
    //             continue;
    //         }

    //         // Handle quoted values
    //         if (trimmed.len >= 2 and trimmed[0] == '"' and trimmed[trimmed.len - 1] == '"') {
    //             // Unquote and unescape
    //             const quoted = trimmed[1 .. trimmed.len - 1];
    //             const unescaped = try self.unescapeCsv(quoted);
    //             try fields.append(self.allocator, .{ .value = unescaped });
    //         } else {
    //             // Plain value
    //             try fields.append(self.allocator, .{ .value = try self.allocator.dupe(u8, trimmed) });
    //         }
    //     }

    //     return .{
    //         .fields = try fields.toOwnedSlice(self.allocator),
    //         .allocator = self.allocator,
    //     };
    // }

    /// [OLD] Unescape CSV quoted value (replace "" with ")
    // fn unescapeCsv(self: *CopyCsvParser, input: []const u8) ![]const u8 {
    //     var result: std.ArrayList(u8) = .empty;
    //     defer result.deinit(self.allocator);

    //     var i: usize = 0;
    //     while (i < input.len) {
    //         if (input[i] == '"' and i + 1 < input.len and input[i + 1] == '"') {
    //             // Double quote -> single quote
    //             try result.append(self.allocator, '"');
    //             i += 2;
    //         } else {
    //             try result.append(self.allocator, input[i]);
    //             i += 1;
    //         }
    //     }

    //     return try result.toOwnedSlice(self.allocator);
    // }

    /// Get row iterator
    pub fn rows(self: *CopyCsvParser) RowIterator {
        return .{
            .parser = self,
            .line_start = 0,
        };
    }

    /// Get column names
    pub fn columnNames(self: *CopyCsvParser) ?[][]const u8 {
        return self.header;
    }

    /// Return the last primary-key value seen during streaming, or null if none.
    /// Only valid after streamToEncoder returns with num_rows > 0.
    pub fn getLastPkValue(self: *const CopyCsvParser) ?[]const u8 {
        if (self.last_pk_len == 0) return null;
        return self.last_pk_buf[0..self.last_pk_len];
    }

    // ========================================================================
    // Streaming API (zero-copy, direct-to-buffer encoding)
    // ========================================================================

    /// Parse header from a single line and store it
    fn parseHeaderFromLine(self: *CopyCsvParser, line: []const u8) !void {
        var cols = std.ArrayList([]const u8).empty;
        errdefer {
            for (cols.items) |col| self.allocator.free(col);
            cols.deinit(self.allocator);
        }

        var it = std.mem.splitScalar(u8, line, ',');
        while (it.next()) |col_name| {
            const trimmed = std.mem.trim(u8, col_name, " \r");
            // Dupe the header because it needs to live for the entire snapshot
            try cols.append(self.allocator, try self.allocator.dupe(u8, trimmed));
        }

        self.header = try cols.toOwnedSlice(self.allocator);
        log.debug("Streaming Parser initialized with {d} columns", .{self.header.?.len});
    }

    /// Parse a CSV line and stream fields directly to the encoder.
    /// When pk_col_idx is set, saves the value at that column index into
    /// last_pk_buf so callers can advance the keyset cursor after the chunk.
    fn parseCsvLineStreaming(
        self: *CopyCsvParser,
        line: []u8,
        encoder: *StreamingEncoder,
    ) !void {
        const expected_cols = if (self.header) |h| h.len else 0;
        try encoder.beginRow(expected_cols);

        var it = std.mem.splitScalar(u8, line, ',');
        var col_idx: usize = 0;

        while (it.next()) |raw_field| {
            const trimmed = std.mem.trim(u8, raw_field, " \r");

            const field_value: ?[]const u8 = if (trimmed.len == 0 or std.mem.eql(u8, trimmed, "\\N"))
                null
            else if (trimmed.len >= 2 and trimmed[0] == '"' and trimmed[trimmed.len - 1] == '"')
                unescapeCsvInPlace(@constCast(trimmed[1 .. trimmed.len - 1]))
            else
                trimmed;

            // Capture PK value for keyset pagination
            if (self.pk_col_idx) |pk_idx| {
                if (col_idx == pk_idx) {
                    if (field_value) |pk_val| {
                        if (pk_val.len > self.last_pk_buf.len) {
                            log.err("PK value length {d} exceeds buffer capacity {d} — aborting to prevent cursor corruption", .{ pk_val.len, self.last_pk_buf.len });
                            return error.PkValueTooLong;
                        }
                        @memcpy(self.last_pk_buf[0..pk_val.len], pk_val);
                        self.last_pk_len = pk_val.len;
                    }
                }
            }

            try encoder.writeField(field_value);
            col_idx += 1;
        }
    }

    /// Stream PostgreSQL COPY data directly to MessagePack encoder.
    ///
    /// pk_col_name: when non-null, the parser tracks the last value of that
    /// column so callers can advance the keyset cursor via getLastPkValue().
    pub fn streamToEncoder(
        self: *CopyCsvParser,
        query: [:0]const u8,
        encoder: *StreamingEncoder,
        pk_col_name: ?[]const u8,
    ) !usize {
        // 1. Start PostgreSQL COPY
        const result = c.PQexec(self.conn, query.ptr);
        defer c.PQclear(result);

        if (c.PQresultStatus(result) != c.PGRES_COPY_OUT) {
            const err_msg = c.PQerrorMessage(self.conn);
            log.err("COPY command failed: {s}", .{err_msg});
            return error.CopyFailed;
        }

        // 2. We'll count rows as we stream them
        var row_count: usize = 0;

        // Reserve 5 bytes for the array header (max msgpack array32 header).
        // We patch it in after we know the row count.
        const array_header_start = encoder.getPos();
        const max_header_size: usize = 5;
        for (0..max_header_size) |_| {
            try encoder.writeByte(0);
        }

        // 3. Stream loop: Fetch -> Parse -> Encode
        while (true) {
            var buf_ptr: [*c]u8 = undefined;
            const len = c.PQgetCopyData(self.conn, &buf_ptr, 0);

            if (len == -1) break; // End of COPY data
            if (len == -2) {
                const err_msg = c.PQerrorMessage(self.conn);
                log.err("PQgetCopyData failed: {s}", .{err_msg});
                return error.CopyDataFailed;
            }

            defer c.PQfreemem(buf_ptr);
            const line = std.mem.trim(u8, buf_ptr[0..@intCast(len)], " \n\r");
            if (line.len == 0) continue;

            // First line is the CSV header
            if (self.header == null) {
                try self.parseHeaderFromLine(line);
                // Resolve PK column index now that we know the column names
                if (pk_col_name) |pk_name| {
                    if (self.header) |h| {
                        for (h, 0..) |col, i| {
                            if (std.mem.eql(u8, col, pk_name)) {
                                self.pk_col_idx = i;
                                self.last_pk_len = 0;
                                break;
                            }
                        }
                        if (self.pk_col_idx == null) {
                            log.warn("PK column '{s}' not found in COPY header — pagination will not advance", .{pk_name});
                        }
                    }
                }
                continue;
            }

            // Subsequent lines are data rows
            const mutable_line = @constCast(line);
            try self.parseCsvLineStreaming(mutable_line, encoder);
            row_count += 1;
        }

        // 4. Now go back and write the correct array header with actual row count
        const total_written = encoder.getPos();
        const rows_data_start = array_header_start + max_header_size;

        // Build the correct array header
        var header_buf: [5]u8 = undefined;
        const actual_header_len = encodeArrayHeader(&header_buf, row_count);

        // Debug: Log what we're encoding
        log.debug("Array header encoding: row_count={d}, header_len={d}, header_bytes={any}", .{
            row_count,
            actual_header_len,
            header_buf[0..actual_header_len],
        });

        // Shift data forward if header is smaller than reserved space
        const shift_amount = max_header_size - actual_header_len;
        if (shift_amount > 0) {
            const data_len = total_written - rows_data_start;
            const dst_start = array_header_start + actual_header_len;

            log.debug("Shifting data: total_written={d}, shift_amount={d}, data_len={d}, dst_start={d}", .{
                total_written,
                shift_amount,
                data_len,
                dst_start,
            });

            // Move data forward using std.mem.copyForwards (safe for overlapping regions)
            std.mem.copyForwards(
                u8,
                encoder.buffer[dst_start..dst_start + data_len],
                encoder.buffer[rows_data_start..total_written]
            );

            // Write the header at the beginning
            @memcpy(encoder.buffer[array_header_start..array_header_start + actual_header_len], header_buf[0..actual_header_len]);

            // Update encoder position (we removed shift_amount bytes)
            encoder.setPos(total_written - shift_amount);

            log.debug("Final position: pos={d}, first 10 bytes={any}", .{
                encoder.pos,
                encoder.buffer[0..@min(10, encoder.pos)],
            });
        } else {
            // Header fits exactly (5 bytes), just write it
            @memcpy(encoder.buffer[array_header_start..array_header_start + actual_header_len], header_buf[0..actual_header_len]);

            log.debug("No shift needed, wrote header directly at position {d}", .{array_header_start});
        }

        log.debug("COPY streamed {d} rows directly to encoder", .{row_count});
        return row_count;
    }
};

/// A field in a CSV row
pub const CsvField = struct {
    value: ?[]const u8,

    pub fn isNull(self: CsvField) bool {
        return self.value == null;
    }
};

// ========================================
// Tests
// ========================================

test "CopyCsvParser - parse simple CSV line" {
    const allocator = std.testing.allocator;

    var parser = CopyCsvParser.init(allocator, null);
    defer parser.deinit();

    const line = "1,Alice,30";
    const row = try parser.parseCsvLine(line);
    defer {
        var mut_row = row;
        mut_row.deinit();
    }

    try std.testing.expectEqual(@as(usize, 3), row.fieldCount());
    try std.testing.expectEqualStrings("1", row.fields[0].value.?);
    try std.testing.expectEqualStrings("Alice", row.fields[1].value.?);
    try std.testing.expectEqualStrings("30", row.fields[2].value.?);
}

test "CopyCsvParser - parse CSV with NULL values" {
    const allocator = std.testing.allocator;

    var parser = CopyCsvParser.init(allocator, null);
    defer parser.deinit();

    // Test both empty field and \N for NULL
    const line = "1,,\\N";
    const row = try parser.parseCsvLine(line);
    defer {
        var mut_row = row;
        mut_row.deinit();
    }

    try std.testing.expectEqual(@as(usize, 3), row.fieldCount());
    try std.testing.expectEqualStrings("1", row.fields[0].value.?);
    try std.testing.expect(row.fields[1].isNull());
    try std.testing.expect(row.fields[2].isNull());
}

test "CopyCsvParser - parse CSV with quoted values" {
    const allocator = std.testing.allocator;

    var parser = CopyCsvParser.init(allocator, null);
    defer parser.deinit();

    const line =
        \\1,"Alice and Bob","Hello"
    ;
    const row = try parser.parseCsvLine(line);
    defer {
        var mut_row = row;
        mut_row.deinit();
    }

    try std.testing.expectEqual(@as(usize, 3), row.fieldCount());
    try std.testing.expectEqualStrings("1", row.fields[0].value.?);
    try std.testing.expectEqualStrings("Alice and Bob", row.fields[1].value.?);
    try std.testing.expectEqualStrings("Hello", row.fields[2].value.?);
}

test "CopyCsvParser - parse CSV with escaped quotes" {
    const allocator = std.testing.allocator;

    var parser = CopyCsvParser.init(allocator, null);
    defer parser.deinit();

    // CSV format uses "" to escape quotes inside quoted strings
    const line = "1,\"He said \"\"hello\"\"\",test";
    const row = try parser.parseCsvLine(line);
    defer {
        var mut_row = row;
        mut_row.deinit();
    }

    try std.testing.expectEqual(@as(usize, 3), row.fieldCount());
    try std.testing.expectEqualStrings("1", row.fields[0].value.?);
    try std.testing.expectEqualStrings("He said \"hello\"", row.fields[1].value.?);
    try std.testing.expectEqualStrings("test", row.fields[2].value.?);
}

test "CopyCsvParser - parse CSV with whitespace" {
    const allocator = std.testing.allocator;

    var parser = CopyCsvParser.init(allocator, null);
    defer parser.deinit();

    // CSV parser should trim whitespace around unquoted values
    const line = " 1 , Alice , 30 ";
    const row = try parser.parseCsvLine(line);
    defer {
        var mut_row = row;
        mut_row.deinit();
    }

    try std.testing.expectEqual(@as(usize, 3), row.fieldCount());
    try std.testing.expectEqualStrings("1", row.fields[0].value.?);
    try std.testing.expectEqualStrings("Alice", row.fields[1].value.?);
    try std.testing.expectEqualStrings("30", row.fields[2].value.?);
}

test "CopyCsvParser - parse header from buffer" {
    const allocator = std.testing.allocator;

    var parser = CopyCsvParser.init(allocator, null);
    defer parser.deinit();

    // Simulate COPY CSV data with header
    const csv_data = "id,name,age\n1,Alice,30\n2,Bob,25\n";
    try parser.buffer.appendSlice(allocator, csv_data);

    try parser.parseHeader();

    const header = parser.columnNames().?;
    try std.testing.expectEqual(@as(usize, 3), header.len);
    try std.testing.expectEqualStrings("id", header[0]);
    try std.testing.expectEqualStrings("name", header[1]);
    try std.testing.expectEqualStrings("age", header[2]);
}

test "CopyCsvParser - row iterator" {
    const allocator = std.testing.allocator;

    var parser = CopyCsvParser.init(allocator, null);
    defer parser.deinit();

    // Simulate COPY CSV data
    const csv_data = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Carol,35\n";
    try parser.buffer.appendSlice(allocator, csv_data);

    try parser.parseHeader();

    // Iterate through rows
    var row_count: usize = 0;
    var iterator = parser.rows();

    while (try iterator.next()) |row| {
        defer {
            var mut_row = row;
            mut_row.deinit();
        }
        row_count += 1;

        try std.testing.expectEqual(@as(usize, 3), row.fieldCount());

        // Check first row
        if (row_count == 1) {
            try std.testing.expectEqualStrings("1", row.fields[0].value.?);
            try std.testing.expectEqualStrings("Alice", row.fields[1].value.?);
            try std.testing.expectEqualStrings("30", row.fields[2].value.?);
        }

        // Check second row
        if (row_count == 2) {
            try std.testing.expectEqualStrings("2", row.fields[0].value.?);
            try std.testing.expectEqualStrings("Bob", row.fields[1].value.?);
            try std.testing.expectEqualStrings("25", row.fields[2].value.?);
        }

        // Check third row
        if (row_count == 3) {
            try std.testing.expectEqualStrings("3", row.fields[0].value.?);
            try std.testing.expectEqualStrings("Carol", row.fields[1].value.?);
            try std.testing.expectEqualStrings("35", row.fields[2].value.?);
        }
    }

    try std.testing.expectEqual(@as(usize, 3), row_count);
}

test "CopyCsvParser - empty result set" {
    const allocator = std.testing.allocator;

    var parser = CopyCsvParser.init(allocator, null);
    defer parser.deinit();

    // Only header, no data rows
    const csv_data = "id,name,age\n";
    try parser.buffer.appendSlice(allocator, csv_data);

    try parser.parseHeader();

    var iterator = parser.rows();
    const first_row = try iterator.next();
    try std.testing.expect(first_row == null);
}

test "CopyCsvParser - row with mixed NULL and values" {
    const allocator = std.testing.allocator;

    var parser = CopyCsvParser.init(allocator, null);
    defer parser.deinit();

    const line = "1,Alice,,35,\\N,\"quoted value\"";
    const row = try parser.parseCsvLine(line);
    defer {
        var mut_row = row;
        mut_row.deinit();
    }

    try std.testing.expectEqual(@as(usize, 6), row.fieldCount());
    try std.testing.expectEqualStrings("1", row.fields[0].value.?);
    try std.testing.expectEqualStrings("Alice", row.fields[1].value.?);
    try std.testing.expect(row.fields[2].isNull());
    try std.testing.expectEqualStrings("35", row.fields[3].value.?);
    try std.testing.expect(row.fields[4].isNull());
    try std.testing.expectEqualStrings("quoted value", row.fields[5].value.?);
}

test "CopyCsvParser - in-place unescape CSV double quotes" {
    // Test unescaping "" -> " in-place
    var input = [_]u8{ 'T', 'h', 'i', 's', ' ', 'i', 's', ' ', 'a', ' ', '"', '"', 'q', 'u', 'o', 't', 'e', 'd', '"', '"', ' ', 'w', 'o', 'r', 'd' };
    const result = unescapeCsvInPlace(&input);

    try std.testing.expectEqualStrings("This is a \"quoted\" word", result);
}

test "CopyCsvParser - in-place unescape multiple consecutive quotes" {
    // Test """" -> ""
    var input = [_]u8{ '"', '"', '"', '"' };
    const result = unescapeCsvInPlace(&input);

    try std.testing.expectEqualStrings("\"\"", result);
}

test "CopyCsvParser - complex CSV with all features" {
    const allocator = std.testing.allocator;

    var parser = CopyCsvParser.init(allocator, null);
    defer parser.deinit();

    // Complex CSV: quoted values, NULLs, escaped quotes
    const csv_data =
        \\id,name,email,bio,age
        \\1,Alice,alice@example.com,"A software engineer who said ""hello world!""",30
        \\2,"Bob Smith",,\N,25
        \\3,Carol,carol@test.com,"Lives in Portland OR",35
        \\
    ;

    try parser.buffer.appendSlice(allocator, csv_data);
    try parser.parseHeader();

    const header = parser.columnNames().?;
    try std.testing.expectEqual(@as(usize, 5), header.len);

    var row_count: usize = 0;
    var iterator = parser.rows();

    // Row 1: Alice with complex bio
    if (try iterator.next()) |row| {
        defer {
            var mut_row = row;
            mut_row.deinit();
        }
        row_count += 1;

        try std.testing.expectEqualStrings("1", row.fields[0].value.?);
        try std.testing.expectEqualStrings("Alice", row.fields[1].value.?);
        try std.testing.expectEqualStrings("alice@example.com", row.fields[2].value.?);
        try std.testing.expectEqualStrings("A software engineer who said \"hello world!\"", row.fields[3].value.?);
        try std.testing.expectEqualStrings("30", row.fields[4].value.?);
    }

    // Row 2: Bob with NULLs
    if (try iterator.next()) |row| {
        defer {
            var mut_row = row;
            mut_row.deinit();
        }
        row_count += 1;

        try std.testing.expectEqualStrings("2", row.fields[0].value.?);
        try std.testing.expectEqualStrings("Bob Smith", row.fields[1].value.?);
        try std.testing.expect(row.fields[2].isNull());
        try std.testing.expect(row.fields[3].isNull());
        try std.testing.expectEqualStrings("25", row.fields[4].value.?);
    }

    // Row 3: Carol with quoted bio
    if (try iterator.next()) |row| {
        defer {
            var mut_row = row;
            mut_row.deinit();
        }
        row_count += 1;

        try std.testing.expectEqualStrings("3", row.fields[0].value.?);
        try std.testing.expectEqualStrings("Carol", row.fields[1].value.?);
        try std.testing.expectEqualStrings("carol@test.com", row.fields[2].value.?);
        try std.testing.expectEqualStrings("Lives in Portland OR", row.fields[3].value.?);
        try std.testing.expectEqualStrings("35", row.fields[4].value.?);
    }

    try std.testing.expectEqual(@as(usize, 3), row_count);
}

test "CopyCsvParser - getField by index" {
    const allocator = std.testing.allocator;

    var parser = CopyCsvParser.init(allocator, null);
    defer parser.deinit();

    const line = "1,Alice,30";
    const row = try parser.parseCsvLine(line);
    defer {
        var mut_row = row;
        mut_row.deinit();
    }

    // Valid indices
    const field0 = row.getField(0).?;
    try std.testing.expectEqualStrings("1", field0.value.?);

    const field1 = row.getField(1).?;
    try std.testing.expectEqualStrings("Alice", field1.value.?);

    const field2 = row.getField(2).?;
    try std.testing.expectEqualStrings("30", field2.value.?);

    // Out of bounds
    const field_oob = row.getField(10);
    try std.testing.expect(field_oob == null);
}
