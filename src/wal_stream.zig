//! WAL streaming module for PostgreSQL logical replication
//!
//! Handles connecting to PostgreSQL in replication mode, starting replication from a slot,
//! receiving WAL messages ('k' and ''), and sending status updates.
const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const pg_conn = @import("pg_conn.zig");
const Conf = @import("config.zig");
const utils = @import("utils.zig");

pub const log = std.log.scoped(.wal_stream);

pub const StreamConfig = struct {
    pg_config: *const pg_conn.PgConf,
    slot_name: [:0]const u8 = Conf.Postgres.default_slot_name, // "cdc_slot"
    publication_name: [:0]const u8 = Conf.Postgres.default_publication_name, // "cdc_pub"
};

pub const ReplicationStream = struct {
    allocator: std.mem.Allocator,
    config: StreamConfig,
    conn: ?*c.PGconn = null,

    pub fn init(allocator: std.mem.Allocator, config: StreamConfig) ReplicationStream {
        return .{
            .allocator = allocator,
            .config = config,
            .conn = null,
        };
    }

    pub fn connect(self: *ReplicationStream) !void {
        // Build connection string with replication=database parameter
        const conninfo = try self.config.pg_config.connInfo(self.allocator, true);
        defer self.allocator.free(conninfo);

        log.info("Connecting to PostgreSQL for replication...", .{});
        self.conn = c.PQconnectdb(conninfo.ptr);

        if (c.PQstatus(self.conn) != c.CONNECTION_OK) {
            const err_msg = c.PQerrorMessage(self.conn);
            log.err("🔴 Connection failed: {s}", .{err_msg});
            c.PQfinish(self.conn);
            self.conn = null;
            return error.ConnectionFailed;
        }

        log.info("✅ Connected to PostgreSQL (replication mode)", .{});
    }

    pub fn deinit(self: *ReplicationStream) void {
        if (self.conn) |conn| {
            c.PQfinish(conn);
            self.conn = null;
        }
        log.info("🥁 Disconnected from PostgreSQL", .{});
    }

    /// Start streaming from the replication slot
    pub fn startStreaming(self: *ReplicationStream, start_lsn: ?[]const u8) !void {
        if (self.conn == null) {
            return error.NotConnected;
        }

        // Determine starting LSN (default to 0/0 if not provided)
        const lsn = start_lsn orelse "0/0";

        // Build START_REPLICATION command
        const query = try utils.allocPrintZ(
            self.allocator,
            "START_REPLICATION SLOT {s} LOGICAL {s} (proto_version '1', publication_names '{s}', binary 'true')",
            .{ self.config.slot_name, lsn, self.config.publication_name },
        );
        defer self.allocator.free(query);

        log.info("Starting replication...", .{});

        const result = c.PQexec(self.conn, query.ptr);
        defer c.PQclear(result);

        const status = c.PQresultStatus(result);
        if (status != c.PGRES_COPY_BOTH) {
            const err_msg = c.PQerrorMessage(self.conn);
            log.err("🔴 START_REPLICATION failed: {s}", .{err_msg});
            log.err("🔴 Expected COPY_BOTH mode, got status: {d}", .{status});
            return error.StartReplicationFailed;
        }

        log.info("✅ Replication started successfully", .{});
    }

    /// Receive one WAL message from the stream
    pub fn receiveMessage(self: *ReplicationStream) !?WalMessage {
        if (self.conn == null) {
            return error.NotConnected;
        }

        // First, consume any input waiting on the socket
        // This reads data from the network into libpq's internal buffer
        if (c.PQconsumeInput(self.conn) == 0) {
            const err_msg = c.PQerrorMessage(self.conn);
            log.err("🔴 PQconsumeInput error: {s}", .{err_msg});
            return error.ConsumeInputFailed;
        }

        var buffer: [*c]u8 = undefined;

        // Try to receive data (non-blocking mode with async = 1) and returns the length
        const result = c.PQgetCopyData(self.conn, &buffer, 1);

        if (result > 0) {
            // Got data with length 'result'
            defer c.PQfreemem(buffer);
            const len: usize = @intCast(result);
            log.debug("len---------:{d}", .{len});
            const data = buffer[0..len];

            // Parse the message
            return try self.parseWalData(data);
        } else if (result == 0) {
            // No data available yet
            return null;
        } else if (result == -1) {
            // End of copy stream
            log.info("⚠️ Replication stream ended", .{});
            return error.StreamEnded;
        } else {
            // Error (-2)
            const err_msg = c.PQerrorMessage(self.conn);
            log.err("🔴 PQgetCopyData error: {s}", .{err_msg});
            return error.CopyDataFailed;
        }
    }

    fn parseWalData(self: *ReplicationStream, data: []const u8) !WalMessage {
        if (data.len == 0) {
            return error.EmptyMessage;
        }

        const msg_type = data[0];

        switch (msg_type) {
            'w' => {
                // XLogData message
                if (data.len < 25) {
                    return error.InvalidMessageLength;
                }

                // Parse header:
                // byte 0: 'w' (message type)
                // bytes 1-8: WAL start LSN (int64)
                // bytes 9-16: WAL end LSN (int64)
                // bytes 17-24: Server timestamp (int64)
                // bytes 25+: Actual pgoutput data

                const wal_start = std.mem.readInt(u64, data[1..9], .big);
                const wal_end = std.mem.readInt(u64, data[9..17], .big);
                const timestamp = std.mem.readInt(i64, data[17..25], .big);

                const payload = data[25..];

                return WalMessage{
                    .type = .xlogdata,
                    .wal_start = wal_start,
                    .wal_end = wal_end,
                    .timestamp = timestamp,
                    .payload = try self.allocator.dupe(u8, payload),
                    .reply_requested = false,
                };
            },
            'k' => {
                // Primary keepalive message
                // Format:
                // byte 0: 'k' (message type)
                // bytes 1-8: Server's current WAL end position (int64)
                // byte 9: Reply requested flag (1 = reply required, 0 = optional)
                // bytes 10-17: Server timestamp (int64)
                if (data.len < 18) {
                    return error.InvalidMessageLength;
                }

                const wal_end = std.mem.readInt(u64, data[1..9], .big);
                const reply_requested = data[9] != 0; // Non-zero means reply is required
                const timestamp = std.mem.readInt(i64, data[10..18], .big);

                log.debug("Received keepalive from primary: wal_end={x} reply_requested={}", .{ wal_end, reply_requested });

                return WalMessage{
                    .type = .keepalive,
                    .wal_start = 0,
                    .wal_end = wal_end,
                    .timestamp = timestamp,
                    .payload = &.{},
                    .reply_requested = reply_requested,
                };
            },
            else => {
                log.warn("Unknown WAL message type: {c} (0x{x})", .{ msg_type, msg_type });
                return error.UnknownMessageType;
            },
        }
    }

    /// Send a status update to PostgreSQL acknowledging receipt up to a certain LSN
    pub fn sendStatusUpdate(self: *ReplicationStream, lsn: u64) !void {
        if (self.conn == null) {
            return error.NotConnected;
        }

        // Build standby status update message
        var buffer: [34]u8 = undefined;
        buffer[0] = 'r'; // Message type: receiver status update

        // Write LSN (received/flushed/applied - all set to same value)
        std.mem.writeInt(u64, buffer[1..9], lsn, .big); // Last WAL byte + 1 received
        std.mem.writeInt(u64, buffer[9..17], lsn, .big); // Last WAL byte + 1 flushed to disk
        std.mem.writeInt(u64, buffer[17..25], lsn, .big); // Last WAL byte + 1 applied

        // Timestamp (microseconds since 2000-01-01)
        const now = std.time.microTimestamp();
        const pg_epoch_offset: i64 = 946684800000000; // 2000-01-01 in microseconds since Unix epoch
        const pg_timestamp = now - pg_epoch_offset;
        std.mem.writeInt(i64, buffer[25..33], pg_timestamp, .big);

        // Reply requested flag (0 = no immediate reply needed)
        buffer[33] = 0;

        const result = c.PQputCopyData(self.conn, @ptrCast(&buffer), buffer.len);
        if (result != 1) {
            const err_msg = c.PQerrorMessage(self.conn);
            log.err("⚠️ Failed to send status update: {s}", .{err_msg});
            return error.StatusUpdateFailed;
        }

        // Flush the buffer to ensure the message is sent immediately
        const flush_result = c.PQflush(self.conn);
        if (flush_result == -1) {
            const err_msg = c.PQerrorMessage(self.conn);
            log.err("⚠️ Failed to flush status update: {s}", .{err_msg});
            return error.FlushFailed;
        }

        log.debug("Sent status update: LSN={x}", .{lsn});
    }
};

pub const WalMessage = struct {
    type: MessageType,
    wal_start: u64,
    wal_end: u64,
    timestamp: i64,
    payload: []const u8,
    reply_requested: bool, // For keepalive messages: does PostgreSQL want a reply?

    pub const MessageType = enum {
        xlogdata,
        keepalive,
    };

    pub fn deinit(self: *WalMessage, allocator: std.mem.Allocator) void {
        if (self.payload.len > 0) {
            allocator.free(self.payload);
        }
    }
};
