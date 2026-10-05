//! The optional storage engines, opened at run time (DISTRIBUTION.md).
//!
//! libzb compiles the DuckDB and PostgreSQL engines into every build, from the headers
//! in `include-engines/`, but links neither library. The first client that asks for
//! `engine: "duckdb"` (or a `dbUrl`) opens libduckdb (or libpq) and looks up every
//! function libzb calls. So a SQLite-only client loads on a machine that has neither
//! library, and a missing one is a clear message at connect, not a library that fails
//! to load at all.
//!
//! Where it looks: `ZB_DUCKDB_LIB` / `ZB_LIBPQ_LIB` (a path) first, then the loader's
//! own search by bare name, then the usual install places. The first file that opens
//! and has every function wins; the library stays open for the life of the process.

const std = @import("std");
const log = std.log.scoped(.libzb);
const builtin = @import("builtin");
const c = @import("c");
const dk = @import("duckdb");
const SpinLock = @import("storage.zig").SpinLock;

const duckdb_fns = [_][:0]const u8{
    "duckdb_append_blob",            "duckdb_append_bool",              "duckdb_append_double",
    "duckdb_append_int64",           "duckdb_append_null",              "duckdb_append_varchar_length",
    "duckdb_appender_close",         "duckdb_appender_create",          "duckdb_appender_destroy",
    "duckdb_appender_end_row",       "duckdb_appender_error",           "duckdb_array_type_array_size",
    "duckdb_array_vector_get_child", "duckdb_bind_blob",                "duckdb_bind_boolean",
    "duckdb_bind_double",            "duckdb_bind_int64",               "duckdb_bind_null",
    "duckdb_bind_varchar_length",    "duckdb_close",                    "duckdb_column_count",
    "duckdb_column_name",            "duckdb_connect",                  "duckdb_data_chunk_get_size",
    "duckdb_data_chunk_get_vector",  "duckdb_decimal_internal_type",    "duckdb_decimal_scale",
    "duckdb_destroy_data_chunk",     "duckdb_destroy_logical_type",     "duckdb_destroy_prepare",
    "duckdb_destroy_result",         "duckdb_disconnect",               "duckdb_enum_dictionary_value",
    "duckdb_enum_internal_type",     "duckdb_execute_prepared",         "duckdb_free",
    "duckdb_from_date",              "duckdb_from_timestamp",           "duckdb_get_type_id",
    "duckdb_list_vector_get_child",  "duckdb_open",                     "duckdb_prepare",
    "duckdb_prepare_error",          "duckdb_query",                    "duckdb_result_chunk_count",
    "duckdb_result_error",           "duckdb_result_get_chunk",         "duckdb_string_t_data",
    "duckdb_string_t_length",        "duckdb_struct_type_child_count",  "duckdb_struct_type_child_name",
    "duckdb_struct_vector_get_child", "duckdb_validity_row_is_valid",   "duckdb_vector_get_column_type",
    "duckdb_vector_get_data",        "duckdb_vector_get_validity",      "duckdb_library_version",
};

const libpq_fns = [_][:0]const u8{
    "PQclear",       "PQconnectdb",   "PQerrorMessage", "PQexec",           "PQexecPrepared",
    "PQfinish",      "PQfname",       "PQfreemem",      "PQftype",          "PQgetisnull",
    "PQgetResult",   "PQgetvalue",    "PQnfields",      "PQntuples",        "PQprepare",
    "PQputCopyData", "PQputCopyEnd",  "PQresultErrorMessage", "PQresultStatus", "PQsetNoticeProcessor",
    "PQstatus",      "PQunescapeBytea", "PQlibVersion",
};

/// A struct with one function-pointer field per name, typed from the translated header:
/// `api.duckdb_open(...)` reads like the direct call it replaces.
fn Api(comptime C: type, comptime names: []const [:0]const u8) type {
    var types: [names.len]type = undefined;
    for (names, 0..) |n, i| types[i] = *const @TypeOf(@field(C, n));
    return @Struct(.auto, null, names, &types, &@splat(.{}));
}

pub const DuckDB = Api(dk, &duckdb_fns);
pub const Libpq = Api(c, &libpq_fns);

const macos = builtin.os.tag == .macos;
const duckdb_paths: []const [:0]const u8 = if (macos) &.{
    "libduckdb.dylib", "/opt/homebrew/lib/libduckdb.dylib", "/usr/local/lib/libduckdb.dylib",
} else &.{ "libduckdb.so", "/usr/local/lib/libduckdb.so", "/usr/lib/libduckdb.so" };
const libpq_paths: []const [:0]const u8 = if (macos) &.{
    "libpq.5.dylib",                              "/opt/homebrew/opt/libpq/lib/libpq.5.dylib",
    "/opt/homebrew/lib/postgresql@18/libpq.5.dylib", "/usr/local/opt/libpq/lib/libpq.5.dylib",
    "/opt/homebrew/lib/libpq.5.dylib",
} else &.{ "libpq.so.5", "libpq.so", "/usr/local/lib/libpq.so.5", "/usr/lib/x86_64-linux-gnu/libpq.so.5" };

/// `std.DynLib` covers the platforms libzb ships on except Windows; there the engines
/// report themselves unavailable instead of failing the build.
const can_load = switch (builtin.os.tag) {
    .linux, .macos, .ios, .freebsd, .netbsd, .openbsd => true,
    else => false,
};

/// Why the last `load` on this thread failed, for `zb_last_error`: a phone has no
/// stderr anyone reads. Cleared by the caller before an open, set by a failed load.
pub threadlocal var last_failure: ?[]const u8 = null;

fn Engine(comptime T: type, comptime label: []const u8, comptime env: [:0]const u8, comptime paths: []const [:0]const u8, comptime names: []const [:0]const u8) type {
    return struct {
        var api: T = undefined;
        var state = std.atomic.Value(u8).init(0); // 0 not tried, 1 loaded, 2 failed
        var lock: SpinLock = .{};
        var reason_buf: [384]u8 = undefined;
        var reason: []const u8 = "";

        fn fail(comptime fmt: []const u8, args: anytype) bool {
            reason = std.fmt.bufPrint(&reason_buf, fmt, args) catch reason_buf[0..];
            log.info("{s}", .{reason});
            return false;
        }

        /// The loaded functions. Only after `load` succeeded: a Storage of this engine
        /// cannot exist otherwise, so every call site behind it may use this directly.
        pub inline fn get() *const T {
            return &api;
        }

        /// Opens the library once per process. Returns false, having printed why, when
        /// it is not installed, or is too old to have a function libzb calls.
        pub fn load() bool {
            if (state.load(.acquire) == 0) {
                lock.lock();
                defer lock.unlock();
                if (state.load(.acquire) == 0) state.store(if (tryLoad()) 1 else 2, .release);
            }
            if (state.load(.acquire) == 1) return true;
            last_failure = reason;
            return false;
        }

        fn tryLoad() bool {
            if (!can_load) return fail("engine '{s}': opening a library at run time is not supported on this platform", .{label});
            var missing: ?[:0]const u8 = null;
            if (std.c.getenv(env)) |p| {
                const path = std.mem.span(p);
                if (tryPath(path, &missing)) return true;
                if (missing) |m| return fail("engine '{s}': {s} lacks {s}: not this engine's library, or too old for this libzb", .{ label, path, m });
                return fail("engine '{s}': {s}={s} does not open", .{ label, env, path });
            }
            var old_path: ?[]const u8 = null;
            var old_fn: []const u8 = "";
            for (paths) |p| {
                missing = null;
                if (tryPath(p, &missing)) return true;
                if (missing) |m| if (old_path == null) {
                    old_path = p;
                    old_fn = m;
                };
            }
            if (old_path) |p| return fail("engine '{s}': {s} lacks {s}: not this engine's library, or too old for this libzb", .{ label, p, old_fn });
            return fail("engine '{s}' asked for, but its library is not installed (looked for {s}); install it, or set {s} to its path", .{ label, paths[0], env });
        }

        fn tryPath(path: []const u8, missing: *?[:0]const u8) bool {
            var lib = std.DynLib.open(path) catch return false;
            inline for (names) |n| {
                const f = lib.lookup(@FieldType(T, n), n) orelse {
                    missing.* = n;
                    lib.close();
                    return false;
                };
                @field(api, n) = f;
            }
            return true; // stays open: the functions live in it
        }
    };
}

pub const duckdb = Engine(DuckDB, "duckdb", "ZB_DUCKDB_LIB", duckdb_paths, &duckdb_fns);
pub const libpq = Engine(Libpq, "postgres", "ZB_LIBPQ_LIB", libpq_paths, &libpq_fns);

test "every name resolves to a function in the translated headers" {
    // Compile-time proof: `Api` takes `@TypeOf(@field(C, n))` for each name, so a
    // typo or a function the header lacks fails the build here, not at a customer.
    try std.testing.expect(@typeInfo(DuckDB).@"struct".fields.len == duckdb_fns.len);
    try std.testing.expect(@typeInfo(Libpq).@"struct".fields.len == libpq_fns.len);
}
