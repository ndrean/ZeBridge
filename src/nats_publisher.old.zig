// //! NATS/JetStream Publisher Module for Schemas and table snapshots
// //!
// //! The user should use `publish` to send messages to NATS JetStream streams, followed by `flushAsync` to get the ACKs from the NATS server.
// //!
// //! This is crucial to ensure that the Schema and Snapshot messages are reliably delivered to NATS JetStream.
// const std = @import("std");
// const c_imports = @import("c_imports.zig");
// const c = c_imports.c;
// const Conf = @import("config.zig");
// const Metrics = @import("metrics.zig").Metrics;

// pub const log = std.log.scoped(.nats_pub);

// /// NATS Publisher Configuration
// pub const PublisherConfig = struct {
//     url: [:0]const u8 = Conf.Nats.default_url,
//     max_reconnect_attempts: i32 = Conf.Nats.max_reconnect_attempts, // -1 = infinite
//     reconnect_wait_ms: i64 = Conf.Nats.reconnect_wait_ms, // 1s between attempts
// };

// /// JetStream Stream Configuration
// pub const StreamConfig = struct {
//     name: [:0]const u8,
//     subjects: []const []const u8,
//     retention: RetentionPolicy = .limits,
//     max_msgs: i64 = Conf.Nats.stream_max_msgs,
//     max_bytes: i64 = Conf.Nats.stream_max_bytes,
//     max_age_ns: i64 = Conf.Nats.stream_max_age_ns,
//     storage: StorageType = Conf.Nats.storage_type, // persisted on file system
//     replicas: i32 = 1,

//     pub const RetentionPolicy = enum {
//         limits,
//         interest,
//         workqueue,

//         fn toC(self: RetentionPolicy) c.jsRetentionPolicy {
//             return switch (self) {
//                 .limits => c.js_LimitsPolicy,
//                 .interest => c.js_InterestPolicy,
//                 .workqueue => c.js_WorkQueuePolicy,
//             };
//         }
//     };

//     pub const StorageType = enum {
//         file,
//         memory,

//         fn toC(self: StorageType) c.jsStorageType {
//             return switch (self) {
//                 .file => c.js_FileStorage,
//                 .memory => c.js_MemoryStorage,
//             };
//         }
//     };
// };

// // NATS C callbacks must be extern "C" functions (not closures)
// // We use a global pointer to track reconnection state
// var reconnect_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);

// fn disconnectedCallback(_: ?*c.natsConnection, user_data: ?*anyopaque) callconv(.c) void {
//     _ = user_data;
//     log.warn("🔴 NATS connection lost - attempting reconnection...", .{});
// }

// fn reconnectedCallback(nc: ?*c.natsConnection, user_data: ?*anyopaque) callconv(.c) void {
//     _ = reconnect_count.fetchAdd(1, .monotonic);
//     const count = reconnect_count.load(.monotonic);

//     // Update metrics if provided
//     if (user_data) |data| {
//         const metrics_mod = @import("metrics.zig");
//         const metrics_ptr: *metrics_mod.Metrics = @ptrCast(@alignCast(data));
//         metrics_ptr.recordNatsReconnect();
//     }

//     // Get current URL
//     if (nc) |conn| {
//         var url_buf: [256]u8 = undefined;
//         const status = c.natsConnection_GetConnectedUrl(conn, &url_buf, url_buf.len);
//         if (status == c.NATS_OK) {
//             const url_str = std.mem.sliceTo(&url_buf, 0);
//             log.info("🟢 NATS reconnected to {s} (reconnect #{d})", .{ url_str, count });
//         } else {
//             log.info("🟢 NATS reconnected (reconnect #{d})", .{count});
//         }
//     } else {
//         log.info("🟢 NATS reconnected (reconnect #{d})", .{count});
//     }
// }

// fn closedCallback(_: ?*c.natsConnection, user_data: ?*anyopaque) callconv(.c) void {
//     _ = user_data;
//     log.err("🔴 NATS connection closed permanently", .{});
// }

// /// Core NATS/JetStream Publisher
// ///
// /// Provides low-level NATS connectivity and JetStream publishing.
// /// Independent of any domain-specific logic (e.g., CDC).
// ///
// /// Features automatic reconnection on connection loss.
// ///
// /// Usage:
// /// ```zig
// /// var publisher = try Publisher.init(allocator, .{ .url = "nats://localhost:4222" });
// /// defer publisher.deinit();
// /// try publisher.connect();
// ///
// /// // Create a stream
// /// const stream_config = StreamConfig{
// ///     .name = "MY_STREAM",
// ///     .subjects = &.{"events.>"},
// /// };
// /// try createStream(publisher.js.?, allocator, stream_config);
// ///
// /// // Publish messages (automatically reconnects on failure)
// /// try publisher.publish("events.test", "Hello", null);
// /// ```
// pub const Publisher = struct {
//     allocator: std.mem.Allocator,
//     config: PublisherConfig,
//     nc: ?*c.natsConnection = null,
//     js: ?*c.jsCtx = null,
//     nats_host: [:0]const u8 = "",
//     is_connected: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
//     metrics: ?*Metrics = null, // Optional metrics tracking

//     pub fn init(
//         allocator: std.mem.Allocator,
//         config: PublisherConfig,
//     ) !Publisher {
//         const nats_uri = std.process.getEnvVarOwned(allocator, "NATS_HOST") catch |err| blk: {
//             log.info("NATS_HOST Env Var not set ({t}), using default 127.0.0.1", .{err});
//             break :blk try allocator.dupe(u8, "127.0.0.1");
//             // need a uniform allocation since we allocate when reading env var
//         };
//         defer allocator.free(nats_uri);

//         // Read optional NATS authentication credentials
//         const nats_user = std.process.getEnvVarOwned(allocator, "NATS_BRIDGE_USER") catch null;
//         defer if (nats_user) |u| allocator.free(u);

//         const nats_password = std.process.getEnvVarOwned(allocator, "NATS_BRIDGE_PASSWORD") catch null;
//         defer if (nats_password) |p| allocator.free(p);

//         // Build NATS URL with optional authentication
//         const url: [:0]const u8 = if (nats_user != null and nats_password != null)
//             try std.fmt.allocPrintSentinel(
//                 allocator,
//                 "nats://{s}:{s}@{s}:4222",
//                 .{ nats_user.?, nats_password.?, nats_uri },
//                 0,
//             )
//         else
//             try std.fmt.allocPrintSentinel(
//                 allocator,
//                 "nats://{s}:4222",
//                 .{nats_uri},
//                 0,
//             );

//         if (nats_user != null and nats_password != null) {
//             log.info("NATS authentication enabled (user:password) for user: {s}", .{nats_user.?});
//         } else {
//             log.info("NATS authentication disabled (no credentials provided)", .{});
//         }

//         return Publisher{
//             .allocator = allocator,
//             .config = config,
//             .nc = null,
//             .js = null,
//             .nats_host = url,
//         };
//     }

//     pub fn connect(self: *Publisher) !void {
//         var opts: ?*c.natsOptions = null;
//         var status: c.natsStatus = undefined;

//         // Create options
//         status = c.natsOptions_Create(&opts);
//         if (status != c.NATS_OK) {
//             log.err(
//                 "🔴 Failed to create NATS options: {s}",
//                 .{c.natsStatus_GetText(status)},
//             );
//             return error.NatsOptionsCreateFailed;
//         }
//         defer c.natsOptions_Destroy(opts);

//         // Set server URL
//         status = c.natsOptions_SetURL(opts, self.nats_host.ptr);
//         if (status != c.NATS_OK) {
//             log.err(
//                 "🔴 Failed to set NATS URL: {s}",
//                 .{c.natsStatus_GetText(status)},
//             );
//             return error.NatsSetURLFailed;
//         }

//         // Enable automatic reconnection
//         status = c.natsOptions_SetMaxReconnect(
//             opts,
//             self.config.max_reconnect_attempts,
//         );
//         if (status != c.NATS_OK) {
//             log.err(
//                 "🔴 Failed to set max reconnect attempts: {s}",
//                 .{c.natsStatus_GetText(status)},
//             );
//             return error.NatsOptionsSetFailed;
//         }

//         status = c.natsOptions_SetReconnectWait(
//             opts,
//             self.config.reconnect_wait_ms,
//         );
//         if (status != c.NATS_OK) {
//             log.err(
//                 "🔴 Failed to set reconnect wait time: {s}",
//                 .{c.natsStatus_GetText(status)},
//             );
//             return error.NatsOptionsSetFailed;
//         }

//         // Set connection event callbacks (pass metrics pointer via user_data)
//         const metrics_ptr = if (self.metrics) |m| @as(?*anyopaque, @ptrCast(m)) else null;

//         status = c.natsOptions_SetDisconnectedCB(
//             opts,
//             disconnectedCallback,
//             metrics_ptr,
//         );
//         if (status != c.NATS_OK) {
//             log.err(
//                 "🔴 Failed to set disconnected callback: {s}",
//                 .{c.natsStatus_GetText(status)},
//             );
//             return error.NatsOptionsSetFailed;
//         }

//         status = c.natsOptions_SetReconnectedCB(
//             opts,
//             reconnectedCallback,
//             metrics_ptr,
//         );
//         if (status != c.NATS_OK) {
//             log.err(
//                 "🔴 Failed to set reconnected callback: {s}",
//                 .{c.natsStatus_GetText(status)},
//             );
//             return error.NatsOptionsSetFailed;
//         }

//         status = c.natsOptions_SetClosedCB(
//             opts,
//             closedCallback,
//             null,
//         );
//         if (status != c.NATS_OK) {
//             log.err(
//                 "🔴 Failed to set closed callback: {s}",
//                 .{c.natsStatus_GetText(status)},
//             );
//             return error.NatsOptionsSetFailed;
//         }

//         // Allow reconnection to same server
//         status = c.natsOptions_SetAllowReconnect(opts, true);
//         if (status != c.NATS_OK) {
//             log.err(
//                 "🔴 Failed to enable reconnection: {s}",
//                 .{c.natsStatus_GetText(status)},
//             );
//             return error.NatsOptionsSetFailed;
//         }

//         log.info("Connecting to NATS at {s} (auto-reconnect enabled: max={d}, wait={d}ms)...", .{
//             self.nats_host,
//             self.config.max_reconnect_attempts,
//             self.config.reconnect_wait_ms,
//         });

//         // Connect to NATS
//         status = c.natsConnection_Connect(&self.nc, opts);
//         if (status != c.NATS_OK) {
//             log.err(
//                 "🔴 Failed to connect to NATS: {s}",
//                 .{c.natsStatus_GetText(status)},
//             );
//             return error.NatsConnectionFailed;
//         }

//         log.info("🟢 Connected to NATS at {s}", .{self.nats_host});
//         self.is_connected.store(true, .seq_cst);

//         // Get JetStream context
//         status = c.natsConnection_JetStream(
//             &self.js,
//             self.nc,
//             null,
//         );
//         if (status != c.NATS_OK) {
//             log.err(
//                 "🔴 Failed to get JetStream context: {s}",
//                 .{c.natsStatus_GetText(status)},
//             );
//             self.deinit();
//             return error.JetStreamContextFailed;
//         }

//         log.info("✅ JetStream context acquired", .{});

//         // Verify required streams exist
//         const required_streams = Conf.Nats.default_streams;
//         log.info("Verifying {d} NATS JetStream stream(s)...", .{required_streams.len});
//         for (required_streams) |stream_name| {
//             try ensureStream(self.js.?, self.allocator, stream_name);
//             log.info("  ✅ Stream '{s}' verified", .{stream_name});
//         }
//     }

//     pub fn deinit(self: *Publisher) void {
//         self.is_connected.store(false, .seq_cst);

//         if (self.js) |js| {
//             c.jsCtx_Destroy(js);
//             self.js = null;
//         }
//         if (self.nc) |nc| {
//             c.natsConnection_Destroy(nc);
//             self.nc = null;
//         }

//         if (self.nats_host.len > 0) {
//             self.allocator.free(self.nats_host);
//         }
//         self.nats_host = "";

//         log.info("🥁 Disconnected from NATS", .{});
//     }

//     /// Check if NATS connection is alive (not disconnected/closed)
//     pub fn isConnected(self: *Publisher) bool {
//         if (self.nc) |nc| {
//             const status = c.natsConnection_Status(nc);
//             return status == c.NATS_CONN_STATUS_CONNECTED or status == c.NATS_CONN_STATUS_RECONNECTING;
//         }
//         return false;
//     }

//     /// Get the number of times NATS has reconnected
//     pub fn getReconnectCount() u32 {
//         return reconnect_count.load(.monotonic);
//     }

//     /// Publish a message to JetStream asynchronously with optional message ID for deduplication
//     ///
//     /// This uses js_PublishAsync which is non-blocking and returns immediately.
//     /// Call flushAsync() periodically to complete pending publishes.
//     ///
//     /// Parameters:
//     /// - subject: Full subject name (without prefix)
//     /// - data: Message payload
//     /// - msg_id: Optional unique message ID for idempotent delivery
//     pub fn publish(
//         self: *Publisher,
//         subject: []const u8,
//         data: []const u8,
//         msg_id: ?[]const u8,
//     ) !void {
//         if (self.js == null) {
//             return error.NotConnected;
//         }

//         // Convert subject to null-terminated string for C API
//         const subject_cstr = try self.allocator.dupeZ(u8, subject);
//         defer self.allocator.free(subject_cstr);

//         // Prepare publish options with message ID if provided
//         var opts: ?*c.jsPubOptions = null;
//         var opts_storage: c.jsPubOptions = undefined;
//         var msg_id_cstr: ?[:0]const u8 = null;

//         if (msg_id) |id| {
//             // Initialize options
//             const init_status = c.jsPubOptions_Init(&opts_storage);
//             if (init_status != c.NATS_OK) {
//                 log.err(
//                     "🔴 Failed to initialize jsPubOptions: {d}",
//                     .{init_status},
//                 );
//                 return error.InitFailed;
//             }

//             // Convert message ID to null-terminated string
//             msg_id_cstr = try self.allocator.dupeZ(u8, id);
//             opts_storage.MsgId = msg_id_cstr.?.ptr;
//             opts = &opts_storage;
//         }
//         defer if (msg_id_cstr) |cstr| self.allocator.free(cstr);

//         // Use async publish - returns immediately without waiting for ack
//         const status = c.js_PublishAsync(
//             self.js,
//             subject_cstr.ptr,
//             @ptrCast(data.ptr),
//             @intCast(data.len),
//             opts,
//         );

//         if (status != c.NATS_OK) {
//             log.err(
//                 "🔴 Failed to publish async: {s}",
//                 .{c.natsStatus_GetText(status)},
//             );
//             return error.PublishFailed;
//         }
//     }

//     /// Flush pending async publishes and wait for acknowledgments
//     ///
//     /// Call this periodically to complete pending async publishes.
//     /// This blocks until all pending publishes are acknowledged or timeout.
//     ///
//     /// Timeout is set to 10 seconds to allow for NATS reconnection attempts.
//     /// With reconnect_wait_ms=2000, NATS can attempt reconnection up to 5 times.
//     pub fn flushAsync(self: *Publisher) !void {
//         if (self.js == null) {
//             return error.NotConnected;
//         }

//         log.debug("Flushing async publishes...", .{});

//         // Set a timeout of 10 seconds to allow for reconnection
//         // NATS C library will retry reconnection during this time
//         var opts: c.jsPubOptions = undefined;
//         const init_status = c.jsPubOptions_Init(&opts);
//         if (init_status != c.NATS_OK) {
//             log.err("🔴 Failed to initialize jsPubOptions for flush", .{});
//             return error.InitFailed;
//         }
//         opts.MaxWait = Conf.Nats.publisher_max_wait; // 10 seconds timeout (allows ~5 reconnection attempts)

//         // blocking until completion acknowledgment from NATS server
//         const status = c.js_PublishAsyncComplete(self.js, &opts);
//         if (status != c.NATS_OK) {
//             const status_text = c.natsStatus_GetText(status);

//             // Check if it's a connection issue vs other error
//             if (self.nc) |nc| {
//                 const conn_status = c.natsConnection_Status(nc);
//                 if (conn_status == c.NATS_CONN_STATUS_RECONNECTING) {
//                     log.warn(
//                         "🔴 Flush timeout while NATS reconnecting: {s}",
//                         .{status_text},
//                     );
//                 } else if (conn_status == c.NATS_CONN_STATUS_DISCONNECTED) {
//                     log.err("🔴 Flush failed - NATS disconnected: {s}", .{status_text});
//                 } else if (conn_status == c.NATS_CONN_STATUS_CLOSED) {
//                     log.err(
//                         "🔴 Flush failed - NATS connection closed: {s}",
//                         .{status_text},
//                     );
//                 } else {
//                     log.err(
//                         "🔴 Async publish completion failed: {s}",
//                         .{status_text},
//                     );
//                 }
//             } else {
//                 log.err(
//                     "🔴 Async publish completion failed: {s}",
//                     .{status_text},
//                 );
//             }

//             return error.FlushFailed;
//         }
//         log.debug("Async publishes flushed successfully", .{});
//     }

//     /// Get stream information as JSON from the JetStream server
//     ///
//     /// Used in the http_server "/streams/info" endpoint.
//     ///
//     /// Caller is responsible for freeing the returned memory.
//     pub fn getStreamInfo(self: *Publisher, stream_name: []const u8) ![]const u8 {
//         if (self.js == null) {
//             return error.NotConnected;
//         }

//         // Convert stream name to null-terminated string for the C API
//         const stream_name_z = try self.allocator.dupeZ(u8, stream_name);
//         defer self.allocator.free(stream_name_z);

//         var stream_info: ?*c.jsStreamInfo = null;
//         var js_err: c.jsErrCode = 0;

//         const status = c.js_GetStreamInfo(
//             &stream_info,
//             self.js,
//             stream_name_z.ptr,
//             null,
//             &js_err,
//         );

//         if (status != c.NATS_OK) {
//             return error.StreamNotFound;
//         }
//         defer c.jsStreamInfo_Destroy(stream_info);

//         // Format stream info as JSON
//         const info = stream_info.?.*;
//         const config = info.Config.*;

//         var buffer: [2048]u8 = undefined;
//         const json = try std.fmt.bufPrint(&buffer,
//             \\{{"name":"{s}","messages":{d},"bytes":{d},"first_seq":{d},"last_seq":{d},"consumer_count":{d}}}
//         , .{
//             std.mem.span(config.Name), // no allocation, a ref to the C string
//             info.State.Msgs,
//             info.State.Bytes,
//             info.State.FirstSeq,
//             info.State.LastSeq,
//             info.State.Consumers,
//         });

//         return try self.allocator.dupe(u8, json); // new allocation
//     }

//     /// Helper: Purge all messages from a stream
//     pub fn purgeStream(self: *Publisher, stream_name: []const u8) !void {
//         if (self.js == null) {
//             return error.NotConnected;
//         }

//         // Null terminate stream name for C API
//         const stream_name_z = try self.allocator.dupeZ(u8, stream_name);
//         defer self.allocator.free(stream_name_z);

//         var js_err: c.jsErrCode = 0;
//         const status = c.js_PurgeStream(self.js, stream_name_z.ptr, null, &js_err);

//         if (status != c.NATS_OK) {
//             log.err("🔴 Failed to purge stream: {s}", .{c.natsStatus_GetText(status)});
//             return error.PurgeStreamFailed;
//         }

//         log.info("☑️ Stream '{s}' purged", .{stream_name});
//     }
// };

// /// Ensure a JetStream stream exists (check-or-fail-fast)
// ///
// /// This validates that the stream exists and is accessible.
// /// The infrastructure (e.g., nats-init container) creates streams.
// pub fn ensureStream(js: *c.jsCtx, allocator: std.mem.Allocator, stream_name: []const u8) !void {
//     log.debug("Checking JetStream stream '{s}' exists...", .{stream_name});

//     // Convert slice into Null terminated stream name for C API
//     const stream_name_z = try allocator.dupeZ(u8, stream_name);
//     defer allocator.free(stream_name_z);

//     var js_err: c.jsErrCode = 0;
//     var stream_info: ?*c.jsStreamInfo = null;

//     // Try to get existing stream info
//     const status = c.js_GetStreamInfo(
//         &stream_info,
//         js,
//         stream_name_z.ptr,
//         null,
//         &js_err,
//     );

//     if (status == c.NATS_OK) {
//         log.debug("Stream '{s}' found and accessible", .{stream_name});
//         c.jsStreamInfo_Destroy(stream_info);
//         return;
//     }

//     // Stream doesn't exist or not accessible
//     const status_text = c.natsStatus_GetText(status);
//     log.err(
//         "🔴 Stream '{s}' not found or inaccessible: {s}. Ensure the stream is created by infrastructure (e.g., nats-init container)",
//         .{ stream_name, status_text },
//     );

//     return error.StreamNotFound;
// }

// /// Check multiple JetStream streams exist
// ///
// /// Verifies that all required streams are created and accessible.
// /// Streams should be created by infrastructure (nats-init), this function only verifies they exist.
// ///
// /// Args:
// ///   publisher: NATS publisher instance with active JetStream context
// ///   allocator: Memory allocator
// ///   stream_names: Slice of stream names to verify
// pub fn checkStreams(
//     publisher: *Publisher,
//     allocator: std.mem.Allocator,
//     stream_names: []const []const u8,
// ) !void {
//     log.info("Verifying {d} NATS JetStream stream(s)...", .{stream_names.len});

//     for (stream_names) |stream_name| {
//         try ensureStream(publisher.js.?, allocator, stream_name);
//         log.info("  ✅ Stream '{s}' verified", .{stream_name});
//     }
// }
