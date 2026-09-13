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

fn quietNotice(_: ?*anyopaque, _: [*c]const u8) callconv(.c) void {}

/// §10fd: the engine behind a replica. SQLite is a file; PostgreSQL is a server the
/// host points at with a URL (the micro-VM case: a replica any PostgreSQL tool
/// reads). One `Storage`, the client speaks one SQL to it, and the differences
/// live here: placeholders, PRAGMAs, the catalogue questions, typed results.
/// §10fl: `duckdb` is the third engine — an analytical replica in a `.duckdb` file
/// (the micro-VM worker's), built in with `-Dduckdb=true`. Every DuckDB path is
/// behind `build_options.duckdb`, so the default build never references libduckdb.
pub const Engine = enum { sqlite, postgres, duckdb };
const build_options = @import("build_options");
const dk = @import("duckdb");
const duckdb_read = @import("duckdb_read.zig");

pub const Storage = struct {
    engine: Engine = .sqlite,
    commit_err_buf: [256]u8 = undefined,
    commit_err_len: usize = 0,
    db: *c.sqlite3 = undefined,
    mutex: SpinLock = .{},
    // ── PostgreSQL ──
    pg: ?*c.PGconn = null,
    /// translated SQL → the server-side prepared statement's name.
    pg_stmts: std.StringHashMapUnmanaged([:0]const u8) = .empty,
    pg_next: usize = 0,
    pg_err_buf: [512]u8 = undefined,
    pg_err_len: usize = 0,
    // ── DuckDB (§10fl) ── opaque here so the struct compiles without the header.
    dk_db: ?*anyopaque = null,
    dk_con: ?*anyopaque = null,
    /// The card's second connection shares the primary's database handle and must
    /// not close it: DuckDB allows one open of a file per process, many connections.
    dk_owns_db: bool = true,
    dk_stmts: std.StringHashMapUnmanaged(*anyopaque) = .empty,
    /// Prepared-statement cache (§10cu). The CDC wire carries FULL rows (§7's
    /// asymmetry), so the apply path compiles the SAME SQL for every event of a
    /// table — millions of prepares of identical text. Prepare once, rebind per
    /// row: keyed by exact SQL text, capped, cleared on schema surgery (see
    /// clearStmtCache) and finalized at close. A cached statement is RESET and
    /// its bindings cleared after every use — an un-reset statement holds table
    /// locks and pins the read snapshot.
    stmt_cache: std.StringHashMapUnmanaged(*c.sqlite3_stmt) = .empty,

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
        // The handle is acquired; a failing pragma below must not leak it.
        errdefer _ = c.sqlite3_close(self.db);
        self.execSimple("PRAGMA journal_mode = WAL;") catch {};
        try self.execSimple("PRAGMA foreign_keys = ON;");
        return self;
    }

    /// A second connection to the same file, opened READ-ONLY — the app's handle
    /// (NOTES §10: "libzb keeps its read-write connection private and hands the app a
    /// second connection opened SQLITE_OPEN_READONLY"). A stray UPDATE through it is
    /// an SQLite error at the call site, not a violated convention. WAL: readers never
    /// block the applier. Must be opened AFTER the read-write one created the file.
    pub fn openReadOnly(path: [*:0]const u8) Error!Storage {
        var db: ?*c.sqlite3 = null;
        if (c.sqlite3_open_v2(path, &db, c.SQLITE_OPEN_READONLY, null) != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return Error.OpenFailed;
        }
        return Storage{ .db = db.? };
    }

    /// A PostgreSQL replica: `url` is a libpq connection string. Read-only opens the
    /// second connection the C card's `query` uses — enforced by the server, as the
    /// SQLite one is by the open flag.
    pub fn openPostgres(url: [*:0]const u8, read_only: bool) Error!Storage {
        const conn = c.PQconnectdb(url) orelse return Error.OpenFailed;
        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            c.PQfinish(conn);
            return Error.OpenFailed;
        }
        var self = Storage{ .engine = .postgres, .pg = conn };
        errdefer c.PQfinish(conn);
        // Server notices ("relation already exists, skipping" on every IF NOT EXISTS)
        // would land on the host's stderr; the client says what matters itself.
        _ = c.PQsetNoticeProcessor(conn, quietNotice, null);
        try self.execSimple("SET timezone TO 'UTC'");
        if (read_only) try self.execSimple("SET default_transaction_read_only = on");
        return self;
    }

    /// §10fl: a DuckDB replica — `path` is the database file (`:memory:` for a
    /// test). One connection; the card's read-only handle comes from
    /// `openDuckdbShared` on the same database.
    pub fn openDuckdb(path: [*:0]const u8) Error!Storage {
        if (!build_options.duckdb) return Error.OpenFailed;
        var db: dk.duckdb_database = null;
        if (dk.duckdb_open(path, &db) != dk.DuckDBSuccess) return Error.OpenFailed;
        var con: dk.duckdb_connection = null;
        if (dk.duckdb_connect(db, &con) != dk.DuckDBSuccess) {
            dk.duckdb_close(&db);
            return Error.OpenFailed;
        }
        var self = Storage{ .engine = .duckdb, .dk_db = @ptrCast(db), .dk_con = @ptrCast(con) };
        // A TIMESTAMPTZ prints in the session's zone once ICU is loaded (Homebrew's
        // build bundles it): UTC, as the PostgreSQL engine sets, so the text shape is
        // PostgreSQL's own and the wire's `Z` reading holds. Without ICU there is no
        // zone to set and the value is UTC already — the failure is ignored.
        self.execSimple("SET TimeZone = 'UTC'") catch {};
        return self;
    }

    /// A second connection on `primary`'s database (the card's query handle).
    pub fn openDuckdbShared(primary: *Storage) Error!Storage {
        if (!build_options.duckdb) return Error.OpenFailed;
        const db: dk.duckdb_database = @ptrCast(@alignCast(primary.dk_db orelse return Error.OpenFailed));
        var con: dk.duckdb_connection = null;
        if (dk.duckdb_connect(db, &con) != dk.DuckDBSuccess) return Error.OpenFailed;
        return Storage{ .engine = .duckdb, .dk_db = @ptrCast(db), .dk_con = @ptrCast(con), .dk_owns_db = false };
    }

    pub fn close(self: *Storage) void {
        self.clearStmtCache();
        switch (self.engine) {
            .sqlite => _ = c.sqlite3_close(self.db),
            .postgres => if (self.pg) |conn| c.PQfinish(conn),
            .duckdb => if (build_options.duckdb) {
                if (self.dk_con) |cp| {
                    var con: dk.duckdb_connection = @ptrCast(@alignCast(cp));
                    dk.duckdb_disconnect(&con);
                }
                if (self.dk_owns_db) if (self.dk_db) |dp| {
                    var db: dk.duckdb_database = @ptrCast(@alignCast(dp));
                    dk.duckdb_close(&db);
                };
            },
        }
    }

    /// Finalize every cached statement and drop the keys. Called at close, and
    /// before any schema surgery: prepare_v2 statements survive most schema
    /// changes (SQLite recompiles internally), but a DROPped or rebuilt table's
    /// statement fails forever after — clearing is cheap certainty. On PostgreSQL
    /// the server-side statements are deallocated the same way.
    pub fn clearStmtCache(self: *Storage) void {
        if (self.engine == .postgres) {
            var pit = self.pg_stmts.iterator();
            while (pit.next()) |e| {
                std.heap.c_allocator.free(e.key_ptr.*);
                std.heap.c_allocator.free(e.value_ptr.*);
            }
            self.pg_stmts.deinit(std.heap.c_allocator);
            self.pg_stmts = .empty;
            if (self.pg) |conn| {
                const r = c.PQexec(conn, "DEALLOCATE ALL");
                c.PQclear(r);
            }
            return;
        }
        if (self.engine == .duckdb) {
            if (build_options.duckdb) {
                var dit = self.dk_stmts.iterator();
                while (dit.next()) |e| {
                    var st: dk.duckdb_prepared_statement = @ptrCast(@alignCast(e.value_ptr.*));
                    dk.duckdb_destroy_prepare(&st);
                    std.heap.c_allocator.free(e.key_ptr.*);
                }
                self.dk_stmts.deinit(std.heap.c_allocator);
                self.dk_stmts = .empty;
            }
            return;
        }
        var it = self.stmt_cache.iterator();
        while (it.next()) |e| {
            _ = c.sqlite3_finalize(e.value_ptr.*);
            std.heap.c_allocator.free(e.key_ptr.*);
        }
        self.stmt_cache.deinit(std.heap.c_allocator);
        self.stmt_cache = .empty;
    }

    /// The rows AND the column names, for a caller that renders results (the C ABI's
    /// `zb_client_query`). `query` stays positional for the shell's own statements.
    pub const Named = struct { columns: []const []const u8, rows: []Row };

    pub fn queryNamed(self: *Storage, a: std.mem.Allocator, sql: []const u8, params: []const Value) Error!Named {
        if (self.engine == .postgres) return self.pgExec(a, sql, params, true);
        if (self.engine == .duckdb) return self.dkExec(a, sql, params, true);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK) return Error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        const s = stmt orelse return Error.PrepareFailed;
        const ncol: usize = @intCast(c.sqlite3_column_count(s));
        const cols = try a.alloc([]const u8, ncol);
        for (cols, 0..) |*name, i| name.* = try a.dupe(u8, std.mem.span(c.sqlite3_column_name(s, @intCast(i))));
        // Re-run through `query` for the rows: one bind/step implementation, not two.
        const rows = try self.query(a, sql, params);
        return .{ .columns = cols, .rows = rows };
    }

    pub fn errMsg(self: *Storage) []const u8 {
        if (self.engine == .postgres or self.engine == .duckdb) return self.pg_err_buf[0..self.pg_err_len];
        return std.mem.span(c.sqlite3_errmsg(self.db));
    }

    /// Why the last `transaction` COMMIT was refused (a deferred FOREIGN KEY check, §10dg).
    pub fn commitErr(self: *Storage) []const u8 {
        return self.commit_err_buf[0..self.commit_err_len];
    }

    /// Statement without parameters, whose results (if any) are discarded.
    ///
    /// A stack buffer, not an arena over `page_allocator`: that was an mmap/munmap
    /// pair per `BEGIN`, `COMMIT` and `PRAGMA`. Most such statements return no rows;
    /// `PRAGMA journal_mode` returns one short text cell, which this covers. A
    /// statement that returns more than fits is a misuse of execSimple — use `query`.
    pub fn execSimple(self: *Storage, sql: []const u8) Error!void {
        // A stack buffer first (the SQLite path allocates little); the PostgreSQL path
        // rewrites the statement and may not fit — an arena then.
        var buf: [256]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buf);
        _ = self.query(fba.allocator(), sql, &.{}) catch |err| {
            if (err != Error.OutOfMemory) return err;
            var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
            defer arena.deinit();
            _ = try self.query(arena.allocator(), sql, &.{});
        };
    }

    /// Prepare, bind, step. Rows (and their text/blob contents) are allocated
    /// from `a` — hand an arena and drop it wholesale.
    pub fn query(self: *Storage, a: std.mem.Allocator, sql: []const u8, params: []const Value) Error![]Row {
        if (self.engine == .postgres) return (try self.pgExec(a, sql, params, false)).rows;
        if (self.engine == .duckdb) return (try self.dkExec(a, sql, params, false)).rows;
        var transient: ?*c.sqlite3_stmt = null;
        const s: *c.sqlite3_stmt = if (self.stmt_cache.get(sql)) |hit| hit else blk: {
            var stmt: ?*c.sqlite3_stmt = null;
            if (c.sqlite3_prepare_v2(self.db, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK) {
                return Error.PrepareFailed;
            }
            // SQLITE_OK with a null statement means the SQL was empty or only a comment.
            // That is never what a caller meant, so it fails the same way for every
            // caller — it used to succeed silently when there were no params.
            const fresh = stmt orelse return Error.PrepareFailed;
            // Cache by exact text, capped; anything past the cap (or an OOM on the
            // key) runs transient exactly as before.
            if (self.stmt_cache.count() < 256) cache: {
                const key = std.heap.c_allocator.dupe(u8, sql) catch {
                    transient = fresh;
                    break :cache;
                };
                self.stmt_cache.put(std.heap.c_allocator, key, fresh) catch {
                    std.heap.c_allocator.free(key);
                    transient = fresh;
                };
            } else transient = fresh;
            break :blk fresh;
        };
        defer if (transient) |t| {
            _ = c.sqlite3_finalize(t);
        } else {
            _ = c.sqlite3_reset(s);
            _ = c.sqlite3_clear_bindings(s);
        };

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
    // ── PostgreSQL ────────────────────────────────────────────────────────────

    /// The client's SQL as PostgreSQL reads it: `?` becomes `$n` outside quotes,
    /// `BEGIN IMMEDIATE` becomes `BEGIN`. NUL-terminated for libpq.
    fn pgSql(a: std.mem.Allocator, sql: []const u8) Error![:0]u8 {
        const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
        if (std.ascii.eqlIgnoreCase(trimmed, "BEGIN IMMEDIATE")) return a.dupeZ(u8, "BEGIN");
        var out: std.ArrayListUnmanaged(u8) = .empty;
        var n: usize = 0;
        var in_single = false;
        var in_double = false;
        for (sql) |ch| {
            if (ch == '\'' and !in_double) in_single = !in_single;
            if (ch == '"' and !in_single) in_double = !in_double;
            if (ch == '?' and !in_single and !in_double) {
                n += 1;
                try out.print(a, "${d}", .{n});
            } else try out.append(a, ch);
        }
        return out.toOwnedSliceSentinel(a, 0);
    }

    fn pgRecordError(self: *Storage, msg: []const u8) void {
        const n = @min(msg.len, self.pg_err_buf.len);
        @memcpy(self.pg_err_buf[0..n], msg[0..n]);
        self.pg_err_len = n;
    }

    /// One statement through a cached server-side prepared statement, the params as
    /// text (bytes as a binary param), the rows typed by their column's OID: bool,
    /// the integers, the floats, bytea; everything else — numeric included, by the
    /// contract — as text. A PRAGMA is SQLite's business and answers nothing here.
    fn pgExec(self: *Storage, a: std.mem.Allocator, sql: []const u8, params: []const Value, want_names: bool) Error!Named {
        const conn = self.pg orelse return Error.ExecFailed;
        const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
        if (std.ascii.startsWithIgnoreCase(trimmed, "PRAGMA")) return .{ .columns = &.{}, .rows = try a.alloc(Row, 0) };
        const zsql = try pgSql(a, sql);
        const vals = try a.alloc([*c]const u8, params.len);
        const lens = try a.alloc(c_int, params.len);
        const fmts = try a.alloc(c_int, params.len);
        for (params, 0..) |p, i| {
            lens[i] = 0;
            fmts[i] = 0;
            switch (p) {
                .null => vals[i] = null,
                .integer => |v| vals[i] = (try std.fmt.allocPrintSentinel(a, "{d}", .{v}, 0)).ptr,
                .real => |v| vals[i] = (try std.fmt.allocPrintSentinel(a, "{d}", .{v}, 0)).ptr,
                .boolean => |v| vals[i] = if (v) "t" else "f",
                .text => |v| vals[i] = (try a.dupeZ(u8, v)).ptr,
                .blob => |v| {
                    vals[i] = v.ptr;
                    lens[i] = @intCast(v.len);
                    fmts[i] = 1;
                },
            }
        }
        var attempt: u8 = 0;
        while (true) : (attempt += 1) {
            const name: [:0]const u8 = self.pg_stmts.get(zsql) orelse blk: {
                const nm = try std.fmt.allocPrintSentinel(std.heap.c_allocator, "zb_{d}", .{self.pg_next}, 0);
                self.pg_next += 1;
                const pr = c.PQprepare(conn, nm.ptr, zsql.ptr, @intCast(params.len), null);
                const pst = c.PQresultStatus(pr);
                if (pst != c.PGRES_COMMAND_OK) {
                    self.pgRecordError(std.mem.span(c.PQresultErrorMessage(pr)));
                    c.PQclear(pr);
                    std.heap.c_allocator.free(nm);
                    return Error.PrepareFailed;
                }
                c.PQclear(pr);
                const key = std.heap.c_allocator.dupe(u8, zsql) catch {
                    std.heap.c_allocator.free(nm);
                    return Error.OutOfMemory;
                };
                self.pg_stmts.put(std.heap.c_allocator, key, nm) catch {
                    std.heap.c_allocator.free(key);
                    std.heap.c_allocator.free(nm);
                    return Error.OutOfMemory;
                };
                break :blk nm;
            };
            const res = c.PQexecPrepared(conn, name.ptr, @intCast(params.len), vals.ptr, lens.ptr, fmts.ptr, 0);
            defer c.PQclear(res);
            const st = c.PQresultStatus(res);
            if (st != c.PGRES_TUPLES_OK and st != c.PGRES_COMMAND_OK) {
                const msg = std.mem.span(c.PQresultErrorMessage(res));
                self.pgRecordError(msg);
                // A plan cached before a DDL on its table: forget it and go once more.
                if (attempt == 0 and std.mem.indexOf(u8, msg, "cached plan") != null) {
                    if (self.pg_stmts.fetchRemove(zsql)) |kv| {
                        const dl = std.fmt.allocPrintSentinel(a, "DEALLOCATE {s}", .{kv.value}, 0) catch "";
                        if (dl.len > 0) {
                            const dr = c.PQexec(conn, dl.ptr);
                            c.PQclear(dr);
                        }
                        std.heap.c_allocator.free(kv.key);
                        std.heap.c_allocator.free(kv.value);
                    }
                    continue;
                }
                return Error.StepFailed;
            }
            const ntup: usize = @intCast(c.PQntuples(res));
            const ncol: usize = @intCast(c.PQnfields(res));
            const cols = try a.alloc([]const u8, if (want_names) ncol else 0);
            if (want_names) for (cols, 0..) |*nm, i| {
                nm.* = try a.dupe(u8, std.mem.span(c.PQfname(res, @intCast(i))));
            };
            const rows = try a.alloc(Row, ntup);
            for (rows, 0..) |*row, r| {
                row.* = try a.alloc(Value, ncol);
                for (row.*, 0..) |*cell, col| {
                    const ri: c_int = @intCast(r);
                    const ci: c_int = @intCast(col);
                    if (c.PQgetisnull(res, ri, ci) == 1) {
                        cell.* = .null;
                        continue;
                    }
                    const txt = std.mem.span(c.PQgetvalue(res, ri, ci));
                    cell.* = switch (c.PQftype(res, ci)) {
                        16 => .{ .boolean = txt.len > 0 and txt[0] == 't' },
                        20, 21, 23 => .{ .integer = std.fmt.parseInt(i64, txt, 10) catch 0 },
                        700, 701 => .{ .real = std.fmt.parseFloat(f64, txt) catch 0 },
                        17 => blk: {
                            var n: usize = 0;
                            const raw = c.PQunescapeBytea(c.PQgetvalue(res, ri, ci), &n);
                            defer c.PQfreemem(raw);
                            break :blk .{ .blob = try a.dupe(u8, @as([*]const u8, @ptrCast(raw))[0..n]) };
                        },
                        else => .{ .text = try a.dupe(u8, txt) },
                    };
                }
            }
            return .{ .columns = cols, .rows = rows };
        }
    }

    // ─── DuckDB (§10fl) ───────────────────────────────────────────────────────

    fn dkRecordError(self: *Storage, msg: []const u8) void {
        const n = @min(msg.len, self.pg_err_buf.len - 1);
        @memcpy(self.pg_err_buf[0..n], msg[0..n]);
        self.pg_err_buf[n] = 0;
        self.pg_err_len = n;
    }

    /// DuckDB's SQL is close enough to the shell's that only two things move: SQLite's
    /// PRAGMAs answer nothing, and `BEGIN IMMEDIATE` is `BEGIN TRANSACTION`. `?` is a
    /// DuckDB placeholder as it is; a prepared statement is cached by text.
    fn dkExec(self: *Storage, a: std.mem.Allocator, sql: []const u8, params: []const Value, want_names: bool) Error!Named {
        if (!build_options.duckdb) return Error.ExecFailed;
        const con: dk.duckdb_connection = @ptrCast(@alignCast(self.dk_con orelse return Error.ExecFailed));
        const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
        if (std.ascii.startsWithIgnoreCase(trimmed, "PRAGMA")) return .{ .columns = &.{}, .rows = try a.alloc(Row, 0) };
        const zsql: [:0]const u8 = if (std.ascii.eqlIgnoreCase(trimmed, "BEGIN IMMEDIATE")) "BEGIN TRANSACTION" else try a.dupeZ(u8, trimmed);
        // A statement without parameters runs as a plain query: DDL in particular —
        // a prepared `CREATE OR REPLACE … AS SELECT` executed but left the old table
        // in place (measured: the appender then saw the old column count), and a
        // one-off DDL text has no business in the statement cache anyway.
        if (params.len == 0) {
            var qres: dk.duckdb_result = undefined;
            if (dk.duckdb_query(con, zsql.ptr, &qres) != dk.DuckDBSuccess) {
                const msg = dk.duckdb_result_error(&qres);
                self.dkRecordError(if (msg != null) std.mem.span(msg) else "query failed");
                dk.duckdb_destroy_result(&qres);
                return Error.StepFailed;
            }
            defer dk.duckdb_destroy_result(&qres);
            return duckdb_read.readResult(a, &qres, want_names);
        }
        const stmt: dk.duckdb_prepared_statement = blk: {
            if (self.dk_stmts.get(zsql)) |hit| break :blk @ptrCast(@alignCast(hit));
            var st: dk.duckdb_prepared_statement = null;
            if (dk.duckdb_prepare(con, zsql.ptr, &st) != dk.DuckDBSuccess) {
                const msg = dk.duckdb_prepare_error(st);
                self.dkRecordError(if (msg != null) std.mem.span(msg) else "prepare failed");
                dk.duckdb_destroy_prepare(&st);
                return Error.PrepareFailed;
            }
            if (self.dk_stmts.count() < 256) cache: {
                const key = std.heap.c_allocator.dupe(u8, zsql) catch break :cache;
                self.dk_stmts.put(std.heap.c_allocator, key, @ptrCast(st.?)) catch {
                    std.heap.c_allocator.free(key);
                    break :cache;
                };
            }
            break :blk st;
        };
        for (params, 0..) |p, i| {
            const idx: dk.idx_t = @intCast(i + 1);
            const ok = switch (p) {
                .null => dk.duckdb_bind_null(stmt, idx),
                .integer => |v| dk.duckdb_bind_int64(stmt, idx, v),
                .real => |v| dk.duckdb_bind_double(stmt, idx, v),
                .boolean => |v| dk.duckdb_bind_boolean(stmt, idx, v),
                .text => |v| dk.duckdb_bind_varchar_length(stmt, idx, v.ptr, v.len),
                .blob => |v| dk.duckdb_bind_blob(stmt, idx, v.ptr, v.len),
            };
            if (ok != dk.DuckDBSuccess) {
                self.dkRecordError("bind failed");
                return Error.BindFailed;
            }
        }
        var res: dk.duckdb_result = undefined;
        if (dk.duckdb_execute_prepared(stmt, &res) != dk.DuckDBSuccess) {
            const msg = dk.duckdb_result_error(&res);
            self.dkRecordError(if (msg != null) std.mem.span(msg) else "execute failed");
            dk.duckdb_destroy_result(&res);
            return Error.StepFailed;
        }
        defer dk.duckdb_destroy_result(&res);
        // The supported reader: chunks and vectors (duckdb_read.zig) — the legacy
        // `duckdb_value_*` family is marked for removal and was not safe here.
        return duckdb_read.readResult(a, &res, want_names);
    }

    /// §10fl: DuckDB's appender — the bulk path a seed takes. Rows are appended in
    /// TABLE column order (the caller maps the chain's columns to it, NULL for one
    /// the chain lacks); every value type the shell has binds directly.
    pub fn dkAppend(self: *Storage, table: [:0]const u8, rows: []const []const Value) Error!void {
        if (!build_options.duckdb) return Error.ExecFailed;
        const con: dk.duckdb_connection = @ptrCast(@alignCast(self.dk_con orelse return Error.ExecFailed));
        var app: dk.duckdb_appender = null;
        if (dk.duckdb_appender_create(con, null, table.ptr, &app) != dk.DuckDBSuccess) {
            const msg = dk.duckdb_appender_error(app);
            self.dkRecordError(if (msg != null) std.mem.span(msg) else "appender failed");
            _ = dk.duckdb_appender_destroy(&app);
            return Error.ExecFailed;
        }
        defer _ = dk.duckdb_appender_destroy(&app);
        for (rows) |row| {
            for (row) |v| {
                const ok = switch (v) {
                    .null => dk.duckdb_append_null(app),
                    .integer => |x| dk.duckdb_append_int64(app, x),
                    .real => |x| dk.duckdb_append_double(app, x),
                    .boolean => |x| dk.duckdb_append_bool(app, x),
                    .text => |x| dk.duckdb_append_varchar_length(app, x.ptr, x.len),
                    .blob => |x| dk.duckdb_append_blob(app, x.ptr, x.len),
                };
                if (ok != dk.DuckDBSuccess) {
                    const msg = dk.duckdb_appender_error(app);
                    self.dkRecordError(if (msg != null) std.mem.span(msg) else "append failed");
                    return Error.StepFailed;
                }
            }
            if (dk.duckdb_appender_end_row(app) != dk.DuckDBSuccess) {
                const msg = dk.duckdb_appender_error(app);
                self.dkRecordError(if (msg != null) std.mem.span(msg) else "append failed");
                return Error.StepFailed;
            }
        }
        if (dk.duckdb_appender_close(app) != dk.DuckDBSuccess) {
            const msg = dk.duckdb_appender_error(app);
            self.dkRecordError(if (msg != null) std.mem.span(msg) else "appender close failed");
            return Error.StepFailed;
        }
    }

    /// §10fe: COPY FROM STDIN — the statement, then the rows as one text buffer.
    pub fn pgCopy(self: *Storage, copy_sql: [:0]const u8, data: []const u8) Error!void {
        const conn = self.pg orelse return Error.ExecFailed;
        const start = c.PQexec(conn, copy_sql.ptr);
        const st0 = c.PQresultStatus(start);
        if (st0 != c.PGRES_COPY_IN) {
            self.pgRecordError(std.mem.span(c.PQresultErrorMessage(start)));
            c.PQclear(start);
            return Error.ExecFailed;
        }
        c.PQclear(start);
        if (c.PQputCopyData(conn, data.ptr, @intCast(data.len)) != 1) {
            self.pgRecordError(std.mem.span(c.PQerrorMessage(conn)));
            _ = c.PQputCopyEnd(conn, "aborted");
            return Error.ExecFailed;
        }
        if (c.PQputCopyEnd(conn, null) != 1) {
            self.pgRecordError(std.mem.span(c.PQerrorMessage(conn)));
            return Error.ExecFailed;
        }
        const done = c.PQgetResult(conn);
        defer c.PQclear(done);
        if (c.PQresultStatus(done) != c.PGRES_COMMAND_OK) {
            self.pgRecordError(std.mem.span(c.PQresultErrorMessage(done)));
            // Drain whatever else the server answers, or the connection stays busy.
            while (c.PQgetResult(conn)) |r| c.PQclear(r);
            return Error.ExecFailed;
        }
        while (c.PQgetResult(conn)) |r| c.PQclear(r);
    }

    // ── the catalogue questions, answered per engine ──────────────────────────

    /// The table's columns in order, or empty when it does not exist.
    pub fn tableColumns(self: *Storage, a: std.mem.Allocator, table: []const u8) Error![]const []const u8 {
        const rows = switch (self.engine) {
            .sqlite => try self.query(a, "SELECT name FROM pragma_table_info(?)", &.{.{ .text = table }}),
            .postgres => try self.query(a, "SELECT column_name::text FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ? ORDER BY ordinal_position", &.{.{ .text = table }}),
            .duckdb => try self.query(a, "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position", &.{.{ .text = table }}),
        };
        const out = try a.alloc([]const u8, rows.len);
        for (rows, 0..) |row, i| out[i] = row[0].text;
        return out;
    }

    /// The physical primary key's columns, in key order.
    pub fn tablePkColumns(self: *Storage, a: std.mem.Allocator, table: []const u8) Error![]const []const u8 {
        const rows = switch (self.engine) {
            .sqlite => try self.query(a, "SELECT name FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk", &.{.{ .text = table }}),
            .postgres => try self.query(a, "SELECT a.attname::text FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " ++
                "WHERE i.indrelid = to_regclass(?) AND i.indisprimary ORDER BY array_position(i.indkey, a.attnum)", &.{.{ .text = table }}),
            .duckdb => try self.query(a, "SELECT unnest(constraint_column_names) FROM duckdb_constraints() WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'", &.{.{ .text = table }}),
        };
        const out = try a.alloc([]const u8, rows.len);
        for (rows, 0..) |row, i| out[i] = row[0].text;
        return out;
    }

    /// The stored CREATE TABLE text (SQLite), where the FK clauses are compared; null
    /// on PostgreSQL, which keeps no such text — an FK change is not detected there.
    pub fn tableDdl(self: *Storage, a: std.mem.Allocator, table: []const u8) Error!?[]const u8 {
        switch (self.engine) {
            .sqlite => {
                const rows = try self.query(a, "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?", &.{.{ .text = table }});
                return if (rows.len > 0 and rows[0][0] == .text) rows[0][0].text else null;
            },
            .postgres, .duckdb => return null,
        }
    }

    /// The table's index names, the primary key's and the engine's own left out.
    pub fn indexNames(self: *Storage, a: std.mem.Allocator, table: []const u8) Error![]const []const u8 {
        const rows = switch (self.engine) {
            .sqlite => try self.query(a, "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name = ? AND name NOT LIKE 'sqlite_%'", &.{.{ .text = table }}),
            .postgres => try self.query(a, "SELECT indexname::text FROM pg_indexes WHERE schemaname = current_schema() AND tablename = ? AND indexname NOT LIKE '%_pkey'", &.{.{ .text = table }}),
            .duckdb => try self.query(a, "SELECT index_name FROM duckdb_indexes() WHERE table_name = ? AND NOT is_primary", &.{.{ .text = table }}),
        };
        const out = try a.alloc([]const u8, rows.len);
        for (rows, 0..) |row, i| out[i] = row[0].text;
        return out;
    }

    pub fn transaction(self: *Storage, ctx: anytype, func: anytype) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.execSimple("BEGIN IMMEDIATE;");
        if (func(ctx, self)) |_| {
            // A COMMIT refused by a deferred FOREIGN KEY check (§10dg) leaves the
            // transaction OPEN in SQLite; without the rollback every later statement
            // would land inside it.
            self.execSimple("COMMIT;") catch |err| {
                // The refusal's text is gone once ROLLBACK ran ("not an error"): keep it.
                const msg = self.errMsg();
                const n = @min(msg.len, self.commit_err_buf.len - 1);
                @memcpy(self.commit_err_buf[0..n], msg[0..n]);
                self.commit_err_buf[n] = 0;
                self.commit_err_len = n;
                self.execSimple("ROLLBACK;") catch {};
                return err;
            };
        } else |err| {
            self.execSimple("ROLLBACK;") catch {};
            return err;
        }
    }
};

// ─── tests (offline; :memory:) ──────────────────────────────────────────────

test "DuckDB engine: typed cells, bytes, lists from JSON text, the appender, BEGIN IMMEDIATE, a PRAGMA answers nothing (§10fl)" {
    if (!build_options.duckdb) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var st = try Storage.openDuckdb(":memory:");
    defer st.close();
    try st.execSimple("PRAGMA journal_mode = WAL;");
    st.execSimple("CREATE TABLE t (id BIGINT PRIMARY KEY, flag BOOLEAN, note TEXT, bytes BLOB, amount DECIMAL(8,2), ratio DOUBLE, ts TIMESTAMPTZ, tags TEXT[], emb FLOAT[3])") catch |err| {
        std.debug.print("duckdb: {s}\n", .{st.errMsg()});
        return err;
    };
    try st.execSimple("BEGIN IMMEDIATE;");
    _ = try st.query(a, "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", &.{ .{ .integer = 1 }, .{ .boolean = true }, .{ .text = "hi ? there" }, .{ .blob = &.{ 0, 255, 1 } }, .{ .text = "12.50" }, .{ .real = 0.25 }, .{ .text = "2026-09-12T16:51:10.307895Z" }, .{ .text = "[\"a\",\"b c\"]" }, .{ .text = "[1.0,2.0,3.0]" } });
    try st.execSimple("COMMIT;");
    const rows = try st.query(a, "SELECT id, flag, note, bytes, amount, ratio, ts, tags[2], emb[3], list_count(tags) FROM t WHERE id = ?", &.{.{ .integer = 1 }});
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
    try std.testing.expect(rows[0][1].boolean);
    try std.testing.expectEqualStrings("hi ? there", rows[0][2].text);
    try std.testing.expectEqualSlices(u8, &.{ 0, 255, 1 }, rows[0][3].blob);
    try std.testing.expectEqualStrings("12.50", rows[0][4].text);
    try std.testing.expectEqual(@as(f64, 0.25), rows[0][5].real);
    try std.testing.expectEqualStrings("b c", rows[0][7].text);
    try std.testing.expectEqual(@as(f64, 3.0), rows[0][8].real);
    try std.testing.expectEqual(@as(i64, 2), rows[0][9].integer);
    // the timestamptz comes back in PostgreSQL's own text shape (UTC session)
    try std.testing.expect(std.mem.startsWith(u8, rows[0][6].text, "2026-09-12 16:51:10.307895"));
    // the appender, then the upsert-from-copy SQL the PostgreSQL path uses
    try st.execSimple("CREATE TEMP TABLE _zbz_copy AS SELECT * FROM t LIMIT 0");
    const r2 = [_]Value{ .{ .integer = 1 }, .{ .boolean = false }, .{ .text = "newer" }, .null, .{ .text = "1.00" }, .{ .real = 1 }, .{ .text = "2026-09-13 00:00:00+00" }, .null, .null };
    const r3 = [_]Value{ .{ .integer = 2 }, .{ .boolean = false }, .{ .text = "two" }, .null, .null, .null, .{ .text = "2026-09-13 00:00:00+00" }, .null, .null };
    try st.dkAppend("_zbz_copy", &.{ &r2, &r3 });
    _ = try st.query(a, "INSERT INTO t (\"id\", \"flag\", \"note\", \"bytes\", \"amount\", \"ratio\", \"ts\", \"tags\", \"emb\") SELECT \"id\", \"flag\", \"note\", \"bytes\", \"amount\", \"ratio\", \"ts\", \"tags\", \"emb\" FROM _zbz_copy WHERE true ON CONFLICT(\"id\") DO UPDATE SET \"note\" = excluded.\"note\", \"ts\" = excluded.\"ts\" WHERE excluded.\"ts\" > t.\"ts\"", &.{});
    const after = try st.query(a, "SELECT id, note FROM t ORDER BY id", &.{});
    try std.testing.expectEqual(@as(usize, 2), after.len);
    try std.testing.expectEqualStrings("newer", after[0][1].text);
    try std.testing.expectEqualStrings("two", after[1][1].text);
    // does the appender cast text into a list, a fixed array, a timestamptz, a decimal, a uuid?
    try st.execSimple("CREATE TABLE u (id BIGINT, tags TEXT[], emb FLOAT[3], ts TIMESTAMPTZ, amount DECIMAL(8,2), uid UUID)");
    const r4 = [_]Value{ .{ .integer = 7 }, .{ .text = "[\"x\",\"y z\"]" }, .{ .text = "[1.5,2.5,3.5]" }, .{ .text = "2026-09-12T16:51:10.307895Z" }, .{ .text = "12.50" }, .{ .text = "0f2a3c4e-1b2d-4e5f-8a9b-0c1d2e3f4a5b" } };
    st.dkAppend("u", &.{&r4}) catch |err| {
        std.debug.print("duckdb appender casts: {s}\n", .{st.errMsg()});
        return err;
    };
    const u = try st.query(a, "SELECT tags[2], emb[2], ts, amount, uid, tags, emb FROM u", &.{});
    try std.testing.expectEqualStrings("[\"x\",\"y z\"]", u[0][5].text);
    try std.testing.expectEqualStrings("[1.5,2.5,3.5]", u[0][6].text);
    try std.testing.expectEqualStrings("y z", u[0][0].text);
    try std.testing.expectEqual(@as(f64, 2.5), u[0][1].real);
    try std.testing.expectEqualStrings("2026-09-12 16:51:10.307895+00", u[0][2].text);
    try std.testing.expectEqualStrings("12.50", u[0][3].text);
    try std.testing.expectEqualStrings("0f2a3c4e-1b2d-4e5f-8a9b-0c1d2e3f4a5b", u[0][4].text);
    // many rows through the appender, past its internal chunk of 2048
    try st.execSimple("CREATE TEMP TABLE _zbz_copy2 AS SELECT id, note, tags FROM t LIMIT 0");
    const many = try a.alloc([]const Value, 5000);
    for (many, 0..) |*row, i| {
        const r = try a.alloc(Value, 3);
        r[0] = .{ .integer = @intCast(100 + i) };
        r[1] = .{ .text = "n" };
        r[2] = .{ .text = "[\"a\"]" };
        row.* = r;
    }
    st.dkAppend("_zbz_copy2", many) catch |err| {
        std.debug.print("duckdb appender 5000 rows: {s}\n", .{st.errMsg()});
        return err;
    };
    try std.testing.expectEqual(@as(i64, 5000), (try st.query(a, "SELECT count(*) FROM _zbz_copy2", &.{}))[0][0].integer);
    // test_types' own shape, through the staging sequence the seed uses
    try st.execSimple("CREATE TABLE tt (uid UUID PRIMARY KEY, age INTEGER, temperature REAL, price DECIMAL(38,10), is_true BOOLEAN, some_text TEXT, tags TEXT[], matrix INTEGER[][], metadata JSON, deleted_at TIMESTAMPTZ, tenant_id TEXT, last_writer TEXT, inserted_at TIMESTAMPTZ, updated_at TIMESTAMPTZ)");
    try st.execSimple("CREATE OR REPLACE TEMP TABLE _zbz_copy AS SELECT \"uid\", \"age\", \"temperature\", \"price\", \"is_true\", \"some_text\", \"tags\", \"matrix\", \"metadata\", \"deleted_at\", \"tenant_id\", \"last_writer\", \"inserted_at\", \"updated_at\" FROM tt LIMIT 0");
    const tr = [_]Value{ .{ .text = "0f2a3c4e-1b2d-4e5f-8a9b-0c1d2e3f4a5c" }, .{ .integer = 30 }, .{ .real = 21.5 }, .{ .text = "12.50" }, .{ .boolean = true }, .{ .text = "hello" }, .{ .text = "[\"a\",\"b\"]" }, .{ .text = "[[1,2],[3,4]]" }, .{ .text = "{\"k\":1}" }, .null, .{ .text = "globex" }, .null, .{ .text = "2026-09-12T16:51:10.307895Z" }, .{ .text = "2026-09-12T16:51:10.307895Z" } };
    st.dkAppend("_zbz_copy", &.{ &tr, &tr }) catch |err| {
        std.debug.print("duckdb appender test_types shape: {s}\n", .{st.errMsg()});
        return err;
    };
    try st.execSimple("INSERT INTO tt SELECT * FROM _zbz_copy WHERE age = 30 LIMIT 1");
    try std.testing.expectEqual(@as(i64, 1), (try st.query(a, "SELECT count(*) FROM tt", &.{}))[0][0].integer);
    // the catalogue helpers
    const pk = try st.tablePkColumns(a, "t");
    try std.testing.expectEqual(@as(usize, 1), pk.len);
    try std.testing.expectEqualStrings("id", pk[0]);
    try std.testing.expectEqual(@as(usize, 9), (try st.tableColumns(a, "t")).len);
}

test "pgSql: placeholders become $n outside quotes, BEGIN IMMEDIATE becomes BEGIN (§10fd)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqualStrings("SELECT $1, '?', \"?\", $2", try Storage.pgSql(a, "SELECT ?, '?', \"?\", ?"));
    try std.testing.expectEqualStrings("BEGIN", try Storage.pgSql(a, "BEGIN IMMEDIATE;"));
}

test "PostgreSQL engine: typed cells, bytes, placeholders, a PRAGMA answers nothing (§10fd; ZB_PG_TEST_URL)" {
    const url_c = std.c.getenv("ZB_PG_TEST_URL") orelse return error.SkipZigTest;
    const url = std.mem.span(url_c);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var st = try Storage.openPostgres(try a.dupeZ(u8, url), false);
    defer st.close();
    try st.execSimple("DROP TABLE IF EXISTS _zbz_engine_test");
    try st.execSimple("CREATE TABLE _zbz_engine_test (id bigint PRIMARY KEY, flag boolean, note text, bytes bytea, amount numeric(8,2), ratio double precision)");
    _ = try st.query(a, "INSERT INTO _zbz_engine_test VALUES (?, ?, ?, ?, ?, ?)", &.{ .{ .integer = 1 }, .{ .boolean = true }, .{ .text = "hi ? there" }, .{ .blob = &.{ 0, 255, 1 } }, .{ .text = "12.50" }, .{ .real = 0.25 } });
    const rows = try st.query(a, "SELECT id, flag, note, bytes, amount, ratio FROM _zbz_engine_test WHERE id = ?", &.{.{ .integer = 1 }});
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
    try std.testing.expect(rows[0][1].boolean);
    try std.testing.expect(rows[0][2].eqlText("hi ? there"));
    try std.testing.expectEqualSlices(u8, &.{ 0, 255, 1 }, rows[0][3].blob);
    try std.testing.expect(rows[0][4].eqlText("12.50"));
    try std.testing.expectEqual(@as(f64, 0.25), rows[0][5].real);
    try std.testing.expectEqual(@as(usize, 0), (try st.query(a, "PRAGMA foreign_keys = OFF", &.{})).len);
    const cols = try st.tableColumns(a, "_zbz_engine_test");
    try std.testing.expectEqual(@as(usize, 6), cols.len);
    const pk = try st.tablePkColumns(a, "_zbz_engine_test");
    try std.testing.expectEqualStrings("id", pk[0]);
    try st.execSimple("DROP TABLE _zbz_engine_test");
}

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
