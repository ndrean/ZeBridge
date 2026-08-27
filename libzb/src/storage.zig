//! The storage shell: SQLite behind the same contract storage.ts declares.
//!
//! The contract, held here exactly as in the TS adapters:
//!   * transactions SERIALIZE (one mutex; the orchestration runs several lanes);
//!   * `foreign_keys` is ON — semantics, not a driver default to trust;
//!   * value BINDING is semantics: bool binds as 0/1, an absent value as NULL
//!     (finding 11 — the browser driver coerced silently, better-sqlite3
//!     refused loudly; here the coercion is the typed bind itself).

const std = @import("std");
const c = @import("c");

pub const Error = error{
    OpenFailed,
    PrepareFailed,
    BindFailed,
    StepFailed,
    ExecFailed,
    OutOfMemory,
};

/// One bound parameter / one result cell.
pub const Value = union(enum) {
    null,
    integer: i64,
    real: f64,
    text: []const u8,
    blob: []const u8,
    boolean: bool, // binds as 0/1 — the contract, spelled as a type

    pub fn eqlText(self: Value, s: []const u8) bool {
        return self == .text and std.mem.eql(u8, self.text, s);
    }
};

pub const Row = []Value;

/// Minimal serializer for the transaction contract. The orchestration layer
/// will bring std.Io and can swap this for std.Io.Mutex; a storage shell must
/// not force an Io on every caller just to lock.
const SpinLock = struct {
    v: std.atomic.Value(bool) = .init(false),
    fn lock(self: *SpinLock) void {
        while (self.v.swap(true, .acquire)) std.atomic.spinLoopHint();
    }
    fn unlock(self: *SpinLock) void {
        self.v.store(false, .release);
    }
};

pub const Storage = struct {
    db: *c.sqlite3,
    mutex: SpinLock = .{},

    /// Open (or create) a database. `":memory:"` works for tests.
    /// Applies the contract pragmas: foreign_keys ON (semantics) and WAL
    /// journaling (an adapter performance choice, like better-sqlite3's).
    pub fn open(path: [*:0]const u8) Error!Storage {
        var db: ?*c.sqlite3 = null;
        if (c.sqlite3_open(path, &db) != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return Error.OpenFailed;
        }
        var self = Storage{ .db = db.? };
        self.execSimple("PRAGMA journal_mode = WAL;") catch {};
        try self.execSimple("PRAGMA foreign_keys = ON;");
        return self;
    }

    pub fn close(self: *Storage) void {
        _ = c.sqlite3_close(self.db);
    }

    pub fn errMsg(self: *Storage) []const u8 {
        return std.mem.span(c.sqlite3_errmsg(self.db));
    }

    /// Statement without parameters or results.
    pub fn execSimple(self: *Storage, sql: []const u8) Error!void {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        _ = self.query(arena.allocator(), sql, &.{}) catch |e| return e;
    }

    /// Prepare, bind, step. Rows (and their text/blob contents) are allocated
    /// from `a` — hand an arena and drop it wholesale.
    pub fn query(self: *Storage, a: std.mem.Allocator, sql: []const u8, params: []const Value) Error![]Row {
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK) {
            return Error.PrepareFailed;
        }
        defer _ = c.sqlite3_finalize(stmt);
        const s = stmt orelse return if (params.len == 0) &.{} else Error.PrepareFailed;

        for (params, 1..) |p, i| {
            const idx: c_int = @intCast(i);
            const rc = switch (p) {
                .null => c.sqlite3_bind_null(s, idx),
                .integer => |v| c.sqlite3_bind_int64(s, idx, v),
                .real => |v| c.sqlite3_bind_double(s, idx, v),
                .boolean => |v| c.sqlite3_bind_int64(s, idx, if (v) 1 else 0),
                .text => |v| c.sqlite3_bind_text(s, idx, v.ptr, @intCast(v.len), null),
                .blob => |v| c.sqlite3_bind_blob(s, idx, v.ptr, @intCast(v.len), null),
            };
            if (rc != c.SQLITE_OK) return Error.BindFailed;
        }

        var rows: std.ArrayList(Row) = .empty;
        while (true) {
            const rc = c.sqlite3_step(s);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return Error.StepFailed;
            const ncol: usize = @intCast(c.sqlite3_column_count(s));
            const row = try a.alloc(Value, ncol);
            for (row, 0..) |*cell, col| {
                const ci: c_int = @intCast(col);
                cell.* = switch (c.sqlite3_column_type(s, ci)) {
                    c.SQLITE_INTEGER => .{ .integer = c.sqlite3_column_int64(s, ci) },
                    c.SQLITE_FLOAT => .{ .real = c.sqlite3_column_double(s, ci) },
                    c.SQLITE_TEXT => blk: {
                        const p = c.sqlite3_column_text(s, ci);
                        const n: usize = @intCast(c.sqlite3_column_bytes(s, ci));
                        break :blk .{ .text = try a.dupe(u8, @as([*]const u8, @ptrCast(p))[0..n]) };
                    },
                    c.SQLITE_BLOB => blk: {
                        const p = c.sqlite3_column_blob(s, ci);
                        const n: usize = @intCast(c.sqlite3_column_bytes(s, ci));
                        break :blk if (n == 0) Value{ .blob = &.{} } else Value{ .blob = try a.dupe(u8, @as([*]const u8, @ptrCast(p))[0..n]) };
                    },
                    else => .null,
                };
            }
            try rows.append(a, row);
        }
        return rows.items;
    }

    /// The serialized transaction: the contract's MUST. `func` gets the storage
    /// back; a returned error rolls back, otherwise commit.
    pub fn transaction(self: *Storage, ctx: anytype, func: anytype) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.execSimple("BEGIN IMMEDIATE;");
        if (func(ctx, self)) |_| {
            try self.execSimple("COMMIT;");
        } else |err| {
            self.execSimple("ROLLBACK;") catch {};
            return err;
        }
    }
};

// ─── tests (offline; :memory:) ──────────────────────────────────────────────

test "binding contract: bool binds as 0/1, roundtrips as integer" {
    var st = try Storage.open(":memory:");
    defer st.close();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try st.execSimple("CREATE TABLE t (id INTEGER PRIMARY KEY, flag INTEGER, note TEXT)");
    _ = try st.query(a, "INSERT INTO t (id, flag, note) VALUES (?, ?, ?)", &.{
        .{ .integer = 1 }, .{ .boolean = true }, .{ .text = "hi" },
    });
    const rows = try st.query(a, "SELECT flag, note FROM t WHERE id = ?", &.{.{ .integer = 1 }});
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
    try std.testing.expect(rows[0][1].eqlText("hi"));
}

test "foreign_keys is ON: an orphan child is refused" {
    var st = try Storage.open(":memory:");
    defer st.close();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try st.execSimple("CREATE TABLE p (id INTEGER PRIMARY KEY)");
    try st.execSimple("CREATE TABLE ch (id INTEGER PRIMARY KEY, p_id INTEGER, FOREIGN KEY (p_id) REFERENCES p (id))");
    const r = st.query(a, "INSERT INTO ch (id, p_id) VALUES (1, 999)", &.{});
    try std.testing.expectError(Error.StepFailed, r);
}

test "transaction rolls back on error and commits otherwise" {
    var st = try Storage.open(":memory:");
    defer st.close();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try st.execSimple("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    const Ops = struct {
        fn ok(_: void, s: *Storage) Error!void {
            try s.execSimple("INSERT INTO t (id) VALUES (1)");
        }
        fn boom(_: void, s: *Storage) Error!void {
            try s.execSimple("INSERT INTO t (id) VALUES (2)");
            return Error.ExecFailed;
        }
    };
    try st.transaction({}, Ops.ok);
    try std.testing.expectError(Error.ExecFailed, st.transaction({}, Ops.boom));
    const rows = try st.query(a, "SELECT count(*) FROM t", &.{});
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
}
