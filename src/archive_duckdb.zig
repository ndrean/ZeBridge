//! DuckDB for the archiver: one in-memory database that turns a chunk's CSV into one Parquet
//! file, the latest arrival kept per (sensor, kind, sensor time). Opened at run time like
//! libzb's engines (libzb/src/engines.zig, DISTRIBUTION.md): `ZB_DUCKDB_LIB` first, then the
//! loader's own search, then the usual install places. A missing library is a message, not a
//! binary that fails to load; the archiver needs no extension (S3 is archive_s3.zig).

const std = @import("std");
const builtin = @import("builtin");
const dk = @import("duckdb");

/// The functions the archiver calls, typed by hand from the header's types rather than with
/// `@TypeOf(@field(dk, name))`: that form makes Zig's own x86_64 backend (the one
/// `-fincremental` Debug builds use) reference the extern symbols, and the link fails without
/// libduckdb — which is opened at run time, never linked.
const DuckDB = struct {
    duckdb_open: *const fn ([*c]const u8, [*c]dk.duckdb_database) callconv(.c) dk.duckdb_state,
    duckdb_close: *const fn ([*c]dk.duckdb_database) callconv(.c) void,
    duckdb_connect: *const fn (dk.duckdb_database, [*c]dk.duckdb_connection) callconv(.c) dk.duckdb_state,
    duckdb_disconnect: *const fn ([*c]dk.duckdb_connection) callconv(.c) void,
    duckdb_query: *const fn (dk.duckdb_connection, [*c]const u8, [*c]dk.duckdb_result) callconv(.c) dk.duckdb_state,
    duckdb_result_error: *const fn ([*c]dk.duckdb_result) callconv(.c) [*c]const u8,
    duckdb_destroy_result: *const fn ([*c]dk.duckdb_result) callconv(.c) void,
    duckdb_fetch_chunk: *const fn (dk.duckdb_result) callconv(.c) dk.duckdb_data_chunk,
    duckdb_data_chunk_get_size: *const fn (dk.duckdb_data_chunk) callconv(.c) dk.idx_t,
    duckdb_data_chunk_get_vector: *const fn (dk.duckdb_data_chunk, dk.idx_t) callconv(.c) dk.duckdb_vector,
    duckdb_vector_get_data: *const fn (dk.duckdb_vector) callconv(.c) ?*anyopaque,
    duckdb_destroy_data_chunk: *const fn ([*c]dk.duckdb_data_chunk) callconv(.c) void,
    duckdb_library_version: *const fn () callconv(.c) [*c]const u8,
};
const names = std.meta.fieldNames(DuckDB);

const paths: []const [:0]const u8 = if (builtin.os.tag == .macos) &.{
    "libduckdb.dylib", "/opt/homebrew/lib/libduckdb.dylib", "/usr/local/lib/libduckdb.dylib",
} else &.{ "libduckdb.so", "/usr/local/lib/libduckdb.so", "/usr/lib/libduckdb.so" };

pub const Error = error{ DuckDBMissing, DuckDBFailed };

pub const Db = struct {
    api: DuckDB,
    lib: std.DynLib,
    db: dk.duckdb_database = null,
    con: dk.duckdb_connection = null,
    /// Why the last call failed: the library's own words, for the log line.
    why_buf: [512]u8 = undefined,
    why: []const u8 = "",

    fn tryPath(path: []const u8, api: *DuckDB, missing: *?[]const u8) ?std.DynLib {
        var lib = std.DynLib.open(path) catch return null;
        inline for (names) |n| {
            @field(api, n) = lib.lookup(@FieldType(DuckDB, n), n) orelse {
                missing.* = n;
                lib.close();
                return null;
            };
        }
        return lib;
    }

    /// error.DuckDBMissing with `why` set when no library opens with every function.
    pub fn open(d: *Db) Error!void {
        var missing: ?[]const u8 = null;
        const env_path: ?[]const u8 = if (std.c.getenv("ZB_DUCKDB_LIB")) |p| std.mem.span(p) else null;
        if (env_path) |p| {
            d.lib = tryPath(p, &d.api, &missing) orelse return d.fail(error.DuckDBMissing, "ZB_DUCKDB_LIB={s} does not open{s}{s}", .{ p, if (missing != null) " or lacks " else "", missing orelse "" });
        } else {
            const found = for (paths) |p| {
                if (tryPath(p, &d.api, &missing)) |lib| break lib;
            } else null;
            d.lib = found orelse return d.fail(error.DuckDBMissing, "the archiver needs libduckdb (looked for {s}{s}{s}): install it, or set ZB_DUCKDB_LIB to its path", .{ paths[0], if (missing != null) "; one found lacks " else "", missing orelse "" });
        }
        if (d.api.duckdb_open(null, &d.db) != dk.DuckDBSuccess) return d.fail(error.DuckDBFailed, "duckdb_open failed", .{});
        if (d.api.duckdb_connect(d.db, &d.con) != dk.DuckDBSuccess) return d.fail(error.DuckDBFailed, "duckdb_connect failed", .{});
    }

    pub fn close(d: *Db) void {
        if (d.con != null) d.api.duckdb_disconnect(&d.con);
        if (d.db != null) d.api.duckdb_close(&d.db);
        d.lib.close();
    }

    pub fn version(d: *Db) []const u8 {
        return std.mem.span(d.api.duckdb_library_version());
    }

    fn fail(d: *Db, err: Error, comptime fmt: []const u8, args: anytype) Error {
        d.why = std.fmt.bufPrint(&d.why_buf, fmt, args) catch d.why_buf[0..];
        return err;
    }

    /// Runs one statement and returns the first column of its first row as an integer (the
    /// row count a COPY reports), 0 when it returns no row.
    fn queryCount(d: *Db, sql: [:0]const u8) Error!u64 {
        var res: dk.duckdb_result = undefined;
        defer d.api.duckdb_destroy_result(&res);
        if (d.api.duckdb_query(d.con, sql.ptr, &res) != dk.DuckDBSuccess) {
            const msg: []const u8 = if (d.api.duckdb_result_error(&res) != null) std.mem.span(d.api.duckdb_result_error(&res)) else "unknown error";
            return d.fail(error.DuckDBFailed, "{s}", .{msg});
        }
        const chunk = d.api.duckdb_fetch_chunk(res);
        if (chunk == null) return 0;
        var ch = chunk;
        defer d.api.duckdb_destroy_data_chunk(&ch);
        if (d.api.duckdb_data_chunk_get_size(chunk) == 0) return 0;
        const vec = d.api.duckdb_data_chunk_get_vector(chunk, 0);
        const data: [*]const i64 = @ptrCast(@alignCast(d.api.duckdb_vector_get_data(vec)));
        return @intCast(data[0]);
    }

    /// The chunk's CSV (readings_reader.zig) as Parquet at `parquet_path`: resent readings
    /// (same sensor, kind and sensor time) once, the latest arrival kept, ordered by sensor time,
    /// zstd. Returns the rows written. The SQL is the Python archiver's, so their files agree.
    pub fn csvToParquet(d: *Db, gpa: std.mem.Allocator, csv_path: []const u8, parquet_path: []const u8) !u64 {
        var w: std.Io.Writer.Allocating = .init(gpa);
        defer w.deinit();
        // `make_timestamptz(µs)` is the instant itself. `make_timestamp(µs)::TIMESTAMPTZ` casts a
        // zoneless TIMESTAMP in the session's TimeZone, so a host with TZ=Europe/Paris stored every
        // reading an hour early. The Python writer's expression is the same (layout.utc_ts).
        try w.writer.writeAll("COPY (SELECT tenant, sensor_id, kind, value, make_timestamptz(sensor_ms * 1000) AS sensor_ts, " ++
            "make_timestamptz(received_ms * 1000) AS received_at, stream_seq FROM read_csv(");
        try quoted(&w.writer, csv_path);
        try w.writer.writeAll(", header=false, allow_quoted_nulls=false, columns={tenant:'VARCHAR', sensor_id:'VARCHAR', kind:'VARCHAR', " ++
            "value:'DOUBLE', sensor_ms:'BIGINT', received_ms:'BIGINT', stream_seq:'BIGINT'}) " ++
            "QUALIFY row_number() OVER (PARTITION BY sensor_id, kind, sensor_ms ORDER BY stream_seq DESC) = 1 " ++
            "ORDER BY sensor_ts) TO ");
        try quoted(&w.writer, parquet_path);
        try w.writer.writeAll(" (FORMAT parquet, COMPRESSION zstd)");
        try w.writer.writeByte(0);
        const sql = w.written();
        return d.queryCount(sql[0 .. sql.len - 1 :0]);
    }
};

/// A SQL string literal: single quotes, a quote doubled.
fn quoted(w: *std.Io.Writer, s: []const u8) !void {
    try w.writeByte('\'');
    for (s) |c| {
        if (c == '\'') try w.writeByte('\'');
        try w.writeByte(c);
    }
    try w.writeByte('\'');
}

test "a path is one SQL literal" {
    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try quoted(&w, "/tmp/it's.csv");
    try std.testing.expectEqualStrings("'/tmp/it''s.csv'", w.buffered());
}
