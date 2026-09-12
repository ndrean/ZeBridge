//! The C ABI: one JSON dispatch entrypoint.
//!
//!   char*    zb_call(const char* fn, const char* args_json);  // caller frees via zb_free
//!                                                            // NULL: a NULL argument, or malloc failed
//!   void     zb_free(char* p);
//!   int      zb_abi_version(void);
//!
//!   uint64_t zb_client_open(const char* opts_json);   // 0 on failure
//!   int      zb_client_close(uint64_t handle);        // 0 ok, 1 unknown handle
//!   int      zb_client_wipe(uint64_t handle);         // close AND delete the replica files — explicit, never automatic (§10dl)
//!   int      zb_client_live(void);                    // open clients, for tests
//!
//! The index card (NOTES §10), one JSON string in and one out, freed via zb_free.
//! Every call answers `{"error":"<Name>"}` on failure and never a NULL except for a
//! NULL argument or a failed malloc. A handle is NOT thread-safe: one thread drives
//! one client; the table only makes the WRONG thread's mistakes non-fatal.
//!   char* zb_client_sync(uint64_t h);                              // {"tenant":…,"first":bool}
//!   char* zb_client_query(uint64_t h, const char* sql, const char* params_json);
//!                                                                  // {"columns":[…],"rows":[[…],…]} — read-only connection
//!   char* zb_client_mutate(uint64_t h, const char* table, const char* op,
//!                          const char* key_json, const char* values_json);   // {"msgId":…}
//!   char* zb_client_flush(uint64_t h, uint64_t wait_ms);           // {"sent":n,"settled":n}
//!   char* zb_client_poll(uint64_t h, uint64_t wait_ms);            // {"applied":n,"settled":n} — live tail, blocks ≤ wait_ms
//! `opts_json`: url, credsPath, dbPath, principal, tables (array, parents first),
//! clientId (stable across restarts — it is the msg_id prefix), grammarHash
//! (optional: the hash the host received from /enroll or GET /grammar; a mismatch
//! refuses to open). The grammar itself is compiled in — `zb_grammar_hash()` says
//! which (§10dq).
//!
//! `fn` names a fixture section of core-fixtures.json; `args_json` is that
//! case's input fields verbatim; the return value is the expected output as
//! JSON. One string-shaped convention keeps every host binding (Python ctypes,
//! Dart ffi, .NET) to three declarations, and makes the conformance runner a
//! table, not a bridge. Unknown fn -> {"error":"unknown fn"} so a runner can
//! SKIP loudly instead of crashing.

const std = @import("std");
const core = @import("core.zig");
const client = @import("client.zig");
const handles = @import("handles.zig");
const Value = std.json.Value;

/// ⚠️ The host never receives a pointer — only a generation-tagged u64 (handles.zig).
///
/// An embedder cannot catch a Zig panic, so at this boundary a double close or a
/// use-after-close is not a stack trace, it is the host process dying. Routing every
/// client through this table turns all three ordinary FFI mistakes — close twice,
/// close from two threads, use after close — into a lookup that returns nothing.
///
/// 64 is a deployment ceiling, not a design one: a host that wants more clients than
/// that is doing something this API has not been asked for yet, and gets a clean 0.
var clients: handles.Table(ClientBox, 64) = .{};

export fn zb_abi_version() c_int {
    return 1;
}

export fn zb_free(p: ?[*:0]u8) void {
    if (p) |ptr| std.c.free(ptr);
}

fn dupeZ(s: []const u8) ?[*:0]u8 {
    const mem = std.c.malloc(s.len + 1) orelse return null;
    const out: [*]u8 = @ptrCast(mem);
    @memcpy(out[0..s.len], s);
    out[s.len] = 0;
    return @ptrCast(out);
}

fn strArrField(a: std.mem.Allocator, args: Value, key: []const u8) ![]const []const u8 {
    const f = if (args == .object) args.object.get(key) else null;
    if (f == null or f.? != .array) return &.{};
    var out = try a.alloc([]const u8, f.?.array.items.len);
    for (f.?.array.items, 0..) |v, i| out[i] = if (v == .string) v.string else "";
    return out;
}

fn dispatch(a: std.mem.Allocator, name: []const u8, args: Value) ![]const u8 {
    const eq = std.mem.eql;
    var out: std.ArrayList(u8) = .empty;

    if (eq(u8, name, "tombstoned")) {
        const tc_v = args.object.get("tombstoneColumn") orelse .null;
        const tc: ?[]const u8 = if (tc_v == .string) tc_v.string else null;
        return if (core.tombstoned(tc, args.object.get("data") orelse .null)) "true" else "false";
    }
    if (eq(u8, name, "seedGate")) {
        const ev = args.object.get("ev") orelse .null;
        const anchor = args.object.get("anchor") orelse .null;
        return if (core.seedGateDrops(ev, anchor)) "true" else "false";
    }
    if (eq(u8, name, "position")) {
        const stored = if (args.object.get("stored")) |v| v.integer else 0;
        const batch = args.object.get("batch").?.array;
        return try std.fmt.allocPrint(a, "{d}", .{core.advancePosition(stored, batch)});
    }
    if (eq(u8, name, "fkKind")) {
        const msg = args.object.get("message").?.string;
        if (core.foreignKeyFailureKind(msg)) |k| {
            try core.writeJsonString(a, &out, k);
            return out.items;
        }
        return "null";
    }
    if (eq(u8, name, "pgTsToWire")) {
        try core.writeJsonString(a, &out, try core.pgTsToWire(a, args.object.get("in").?.string));
        return out.items;
    }
    if (eq(u8, name, "lsnToNumber")) {
        return try std.fmt.allocPrint(a, "{d}", .{core.lsnToNumber(args.object.get("in").?.string)});
    }
    if (eq(u8, name, "normalizeVersion")) {
        try core.writeJsonString(a, &out, try core.normalizeVersion(a, args.object.get("in").?.string));
        return out.items;
    }
    if (eq(u8, name, "nextVersion")) {
        try core.writeJsonString(a, &out, try core.nextVersion(a, args.object.get("now").?.string, args.object.get("last").?.string));
        return out.items;
    }
    if (eq(u8, name, "hlcVersion")) {
        try core.writeJsonString(a, &out, try core.hlcVersion(a, args.object.get("now").?.string, args.object.get("last").?.string, args.object.get("floor").?.string));
        return out.items;
    }
    if (eq(u8, name, "subjectSafe")) {
        try core.writeJsonString(a, &out, try core.subjectSafeToken(a, args.object.get("in").?.string));
        return out.items;
    }
    if (eq(u8, name, "envelope")) {
        return try core.valueToString(a, try core.buildMutation(a, args));
    }
    if (eq(u8, name, "keyChange")) {
        const table = args.object.get("table").?.string;
        const pk = try strArrField(a, args, "pkCols");
        const data = args.object.get("data") orelse .null;
        if (try core.planKeyChange(a, table, pk, data)) |v| return try core.valueToString(a, v);
        return "null";
    }
    if (eq(u8, name, "upsert")) {
        const table = args.object.get("table").?.string;
        const pk = try strArrField(a, args, "pkCols");
        const data = args.object.get("data") orelse .null;
        return try core.valueToString(a, try core.planUpsert(a, table, pk, data));
    }
    if (eq(u8, name, "pgArrayLiteral")) {
        try core.writeJsonString(a, &out, try core.pgArrayLiteral(a, args.object.get("in").?.array));
        return out.items;
    }
    if (eq(u8, name, "heartbeat")) {
        const seqs_v = args.object.get("seqs") orelse .null;
        const n: usize = if (seqs_v == .object) seqs_v.object.count() else 0;
        const names = try a.alloc([]const u8, n);
        const seqs = try a.alloc(u64, n);
        if (seqs_v == .object) {
            var it = seqs_v.object.iterator();
            var i: usize = 0;
            while (it.next()) |e| : (i += 1) {
                names[i] = e.key_ptr.*;
                seqs[i] = if (e.value_ptr.* == .integer and e.value_ptr.integer >= 0) @intCast(e.value_ptr.integer) else 0;
            }
        }
        const ts: i64 = if (args.object.get("ts")) |v| (if (v == .integer) v.integer else 0) else 0;
        try core.writeJsonString(a, &out, try core.heartbeatPayload(a, args.object.get("principal").?.string, args.object.get("tenant").?.string, ts, names, seqs));
        return out.items;
    }
    if (eq(u8, name, "update")) {
        const table = args.object.get("table").?.string;
        const pk = try strArrField(a, args, "pkCols");
        const data = args.object.get("data") orelse .null;
        if (try core.planUpdate(a, table, pk, data)) |v| return try core.valueToString(a, v);
        return "null";
    }
    if (eq(u8, name, "exists")) {
        const table = args.object.get("table").?.string;
        const pk = try strArrField(a, args, "pkCols");
        const data = args.object.get("data") orelse .null;
        if (try core.planExists(a, table, pk, data)) |v| return try core.valueToString(a, v);
        return "null";
    }
    if (eq(u8, name, "delete")) {
        const table = args.object.get("table").?.string;
        const pk = try strArrField(a, args, "pkCols");
        const data = args.object.get("data") orelse .null;
        if (try core.planDelete(a, table, pk, data)) |v| return try core.valueToString(a, v);
        return "null";
    }
    if (eq(u8, name, "chainUpsert")) {
        const table = args.object.get("table").?.string;
        const cols = try strArrField(a, args, "cols");
        const pk = try strArrField(a, args, "pkCols");
        const vc = args.object.get("versionCol") orelse .null;
        const version_col: ?[]const u8 = if (vc == .string) vc.string else null;
        try core.writeJsonString(a, &out, try core.chainUpsertSql(a, table, cols, pk, version_col));
        return out.items;
    }
    if (eq(u8, name, "chainRowParams")) {
        const row = args.object.get("row").?.array;
        return try core.valueToString(a, try core.chainRowParams(a, row));
    }
    if (eq(u8, name, "chainPlan")) {
        const man = args.object.get("manifest") orelse .null;
        const wm_v = args.object.get("watermark") orelse .null;
        const wm: ?[]const u8 = if (wm_v == .string) wm_v.string else null;
        return try core.valueToString(a, try core.planFromManifest(a, man, wm));
    }
    if (eq(u8, name, "outboxWatermark")) {
        const entries = args.object.get("entries").?.array;
        const wm_v = args.object.get("watermark") orelse .null;
        const wm: ?[]const u8 = if (wm_v == .string) wm_v.string else null;
        return try core.valueToString(a, try core.outboxWatermarkGate(a, entries, wm));
    }
    if (eq(u8, name, "fullPredates")) {
        const man = args.object.get("manifest") orelse .null;
        const plan = args.object.get("plan").?.array;
        const stored = args.object.get("storedSeq").?.integer;
        return if (core.fullPredatesReplica(man, plan, stored)) "true" else "false";
    }
    if (eq(u8, name, "scope")) {
        const streams = args.object.get("streams") orelse .null;
        const tables = args.object.get("tables") orelse .null;
        return try core.valueToString(a, try core.scopeSeeding(a, streams, tables));
    }
    if (eq(u8, name, "columnDdl")) {
        const col = args.object.get("col") orelse .null;
        const pk = try strArrField(a, args, "pkCols");
        try core.writeJsonString(a, &out, try core.columnDdl(a, col, pk));
        return out.items;
    }
    if (eq(u8, name, "fkClauses")) {
        const fks = args.object.get("fks").?.array;
        try core.writeJsonString(a, &out, try core.fkClausesFor(a, fks));
        return out.items;
    }
    if (eq(u8, name, "createTable")) {
        return try core.valueToString(a, try core.createTableSteps(a, args.object.get("table").?.string, args.object.get("cols").?.array, try strArrField(a, args, "pkCols"), args.object.get("fks").?.array));
    }
    if (eq(u8, name, "rebuildSteps")) {
        return try core.valueToString(a, try core.rebuildSteps(a, args.object.get("table").?.string, args.object.get("cols").?.array, try strArrField(a, args, "pkCols"), args.object.get("fks").?.array, try strArrField(a, args, "existing")));
    }
    if (eq(u8, name, "readOnlySql")) {
        return if (core.isReadOnlySql(args.object.get("sql").?.string)) "true" else "false";
    }
    if (eq(u8, name, "shape")) {
        const cols = args.object.get("cols").?.array;
        const pk = try strArrField(a, args, "pkCols");
        try out.appendSlice(a, "{\"key\":");
        try core.writeJsonString(a, &out, try core.keyShape(a, pk, cols));
        try out.appendSlice(a, ",\"types\":");
        try core.writeJsonString(a, &out, try core.typeShape(a, cols));
        try out.appendSlice(a, "}");
        return out.items;
    }
    if (eq(u8, name, "retyped")) {
        const st_v = args.object.get("stored") orelse .null;
        const stored: ?[]const u8 = if (st_v == .string) st_v.string else null;
        return try core.valueToString(a, try core.retypedColumns(a, stored, args.object.get("cols").?.array));
    }
    if (eq(u8, name, "diffColumns")) {
        const ex_v = args.object.get("existing") orelse .null;
        const existing: ?[]const []const u8 = if (ex_v == .array) try core.strArrPub(a, ex_v.array) else null;
        const wanted = try strArrField(a, args, "wanted");
        const renamed = args.object.get("renamed") orelse .null;
        return try core.valueToString(a, try core.diffColumns(a, existing, wanted, renamed));
    }
    if (eq(u8, name, "fkDiffer")) {
        return if (try core.fkTextDiffers(a, args.object.get("ddl").?.string, args.object.get("want").?.string)) "true" else "false";
    }
    if (eq(u8, name, "viewSteps")) {
        return try core.valueToString(a, try core.viewSteps(a, args.object.get("table").?.string, try strArrField(a, args, "names")));
    }
    if (eq(u8, name, "indexPlan")) {
        return try core.valueToString(a, try core.indexSyncPlan(a, args.object.get("table").?.string, try strArrField(a, args, "have"), args.object.get("want").?.array));
    }
    return "{\"error\":\"unknown fn\"}";
}

export fn zb_call(fn_name: ?[*:0]const u8, args_json: ?[*:0]const u8) ?[*:0]u8 {
    const name = std.mem.span(fn_name orelse return null);
    const args_text = std.mem.span(args_json orelse return null);

    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const parsed = std.json.parseFromSlice(Value, a, args_text, .{}) catch
        return dupeZ("{\"error\":\"bad args json\"}");
    // The error NAME goes back to the host: a runner debugging a fixture gets
    // `NotMap` or `OutOfMemory`, not "dispatch failed". Error names are identifiers,
    // so they need no JSON escaping.
    const result = dispatch(a, name, parsed.value) catch |err| {
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "{{\"error\":\"dispatch failed: {s}\"}}", .{@errorName(err)}) catch
            return dupeZ("{\"error\":\"dispatch failed\"}");
        return dupeZ(msg);
    };
    return dupeZ(result);
}

/// A client plus the strings its `Options` point into.
///
/// ⚠️ `Options` holds SLICES, and the client reads `principal` (and the rest) for its
/// whole life — so these cannot die with the JSON they were parsed from, and cannot be
/// freed on success. They must be freed on FAILURE and at close, which is what this
/// box is for: one owner, one destructor, both paths.
///
/// This existed as five bare `dupeZ` calls first, and `leaks` counted the result:
/// 11,000 leaks for 422 KB across 2,200 failed opens — exactly five per open, because
/// `init` failing returned 0 without freeing any of them. `std.testing.allocator` sees
/// none of this; the strings come from `c_allocator`, whose leaks live in the malloc
/// zone where only `leaks` looks.
const ClientBox = struct {
    c: *client.SyncClient,
    url: [:0]u8,
    creds: [:0]u8,
    grammar_hash: ?[]u8,
    db: [:0]u8,
    principal: [:0]u8,
    client_id: [:0]u8,
    tables: []const []const u8,

    fn destroy(self: *ClientBox, a: std.mem.Allocator) void {
        self.c.deinit();
        a.free(self.url);
        a.free(self.creds);
        if (self.grammar_hash) |g| a.free(g);
        a.free(self.db);
        a.free(self.principal);
        a.free(self.client_id);
        for (self.tables) |t| a.free(t);
        a.free(self.tables);
        a.destroy(self);
    }
};

/// Open a client. `opts_json` mirrors `client.Options`; 0 means it did not open, and
/// the reason is logged rather than returned — a richer error channel is worth adding
/// the day a host needs to distinguish "bad credentials" from "no broker".
export fn zb_client_open(opts_json: ?[*:0]const u8) u64 {
    const text = std.mem.span(opts_json orelse return 0);
    const box = openBox(std.heap.c_allocator, text) catch return 0;
    const h = clients.insert(box);
    if (h == 0) box.destroy(std.heap.c_allocator); // table full: do not leak it
    return h;
}

/// Everything fallible, in a function that can actually RETURN an error.
///
/// ⚠️ This split is the whole point, and getting it wrong cost the first fix.
/// `errdefer` fires when its function returns an error — so in an `export fn`
/// returning `u64`, which cannot return one, **every `errdefer` is dead code**. The
/// five `dupeZ` calls sat there with `errdefer a.free(...)` beneath them looking
/// correct, and leaked on every failed open regardless: `leaks` counted 11,000 of them
/// across 2,200 opens and pointed at those exact lines. Zig does not warn, because an
/// unreachable `errdefer` is not an error — it is just never scheduled.
///
/// So: nothing fallible above the boundary, and the boundary only maps error -> 0.
fn openBox(a: std.mem.Allocator, text: []const u8) !*ClientBox {
    const parsed = try std.json.parseFromSlice(Value, a, text, .{});
    defer parsed.deinit();
    const o = parsed.value;
    if (o != .object) return error.BadOptions;

    const str = struct {
        fn get(v: Value, k: []const u8, dflt: []const u8) []const u8 {
            const f = v.object.get(k) orelse return dflt;
            return if (f == .string) f.string else dflt;
        }
    };

    // One acquire per statement, its errdefer on the next line — and here they FIRE,
    // because this function returns an error union.
    const url = try a.dupeZ(u8, str.get(o, "url", "nats://127.0.0.1:4222"));
    errdefer a.free(url);
    const creds = try a.dupeZ(u8, str.get(o, "credsPath", ""));
    errdefer a.free(creds);
    // "grammarHash": what the host received beside its creds (§10dq). Empty = unchecked.
    const gh_raw = str.get(o, "grammarHash", "");
    const grammar_hash: ?[]u8 = if (gh_raw.len > 0) try a.dupe(u8, gh_raw) else null;
    errdefer if (grammar_hash) |g| a.free(g);
    const db = try a.dupeZ(u8, str.get(o, "dbPath", "zb.sqlite3"));
    errdefer a.free(db);
    // dbUrl (§10fd): a PostgreSQL replica instead of the SQLite file.
    const db_url_raw = str.get(o, "dbUrl", "");
    const db_url: ?[:0]u8 = if (db_url_raw.len > 0) try a.dupeZ(u8, db_url_raw) else null;
    errdefer if (db_url) |u| a.free(u);
    const principal = try a.dupeZ(u8, str.get(o, "principal", ""));
    errdefer a.free(principal);
    const client_id = try a.dupeZ(u8, str.get(o, "clientId", "zig-client"));
    errdefer a.free(client_id);
    // heartbeatMs: the fleet beat (§10dc); 0 disables. Default matches client.zig.
    const heartbeat_ms: u64 = blk: {
        const f = o.object.get("heartbeatMs") orelse break :blk 30_000;
        break :blk if (f == .integer and f.integer >= 0) @intCast(f.integer) else 30_000;
    };
    // seedChunkRows (§10fa): rows per transaction when a chain step is applied; 0 = one.
    const seed_chunk_rows: usize = blk: {
        const f = o.object.get("seedChunkRows") orelse break :blk 50_000;
        break :blk if (f == .integer and f.integer >= 0) @intCast(f.integer) else 50_000;
    };
    // The tables, parents first, each its own allocation so the box can free them.
    const tv = o.object.get("tables");
    const ntab: usize = if (tv != null and tv.? == .array) tv.?.array.items.len else 0;
    const tables = try a.alloc([]const u8, ntab);
    var filled: usize = 0;
    errdefer {
        for (tables[0..filled]) |t| a.free(t);
        a.free(tables);
    }
    if (ntab > 0) for (tv.?.array.items) |v| {
        tables[filled] = try a.dupe(u8, if (v == .string) v.string else "");
        filled += 1;
    };

    const box = try a.create(ClientBox);
    errdefer a.destroy(box);

    box.* = .{
        .c = try client.SyncClient.init(a, .{
            .url = url,
            .creds_path = creds,
            .grammar_hash = grammar_hash,
            .heartbeat_ms = heartbeat_ms,
            .seed_chunk_rows = seed_chunk_rows,
            .db_path = db,
            .db_url = if (db_url) |u| u.ptr else null,
            .principal = principal,
            .tables = tables,
            .client_id = client_id,
        }),
        .url = url,
        .creds = creds,
        .grammar_hash = grammar_hash,
        .db = db,
        .principal = principal,
        .client_id = client_id,
        .tables = tables,
    };
    return box;
}

/// Close a client. Idempotent BY CONSTRUCTION: `remove` hands back the pointer at most
/// once, so a second close finds nothing and returns 1 instead of freeing twice.
export fn zb_client_close(handle: u64) c_int {
    const box = clients.remove(handle) orelse return 1;
    box.destroy(std.heap.c_allocator);
    return 0;
}

/// Close the client AND delete its replica files (§10dl). The library never wipes on
/// its own — a revoked principal's device keeps its rows and simply stops receiving;
/// the wipe is the application's explicit act, and this is the one verb for it.
export fn zb_client_wipe(handle: u64) c_int {
    const box = clients.remove(handle) orelse return 1;
    var path_buf: [1024]u8 = undefined;
    const db = std.fmt.bufPrintZ(&path_buf, "{s}", .{box.db}) catch return 1;
    box.destroy(std.heap.c_allocator);
    for ([_][]const u8{ "", "-wal", "-shm" }) |suffix| {
        var buf: [1040]u8 = undefined;
        const p = std.fmt.bufPrintZ(&buf, "{s}{s}", .{ db, suffix }) catch continue;
        _ = std.c.unlink(p.ptr);
    }
    return 0;
}

/// Open clients. Exists so a test can assert the table empties — a leak of a whole
/// client is otherwise invisible from outside.
/// The sha256 (lowercase hex) of the grammar this library was built with — compare
/// it with the bridge's `X-Grammar-Hash` or the /enroll payload's `grammar_hash`;
/// or pass that value as `grammarHash` at open and let the library refuse. Free
/// with zb_free.
export fn zb_grammar_hash() ?[*:0]u8 {
    var buf: [64]u8 = undefined;
    return dupeZ(client.grammarHashHex(&buf));
}

/// The grammar bytes themselves, for a host that needs a wire name (a NATS conf, a
/// diagnostic). Free with zb_free.
export fn zb_grammar_json() ?[*:0]u8 {
    return dupeZ(client.grammar_json);
}

export fn zb_client_live() c_int {
    return @intCast(clients.liveCount());
}

// ── the index card ────────────────────────────────────────────────────────────

fn errJson(name: []const u8) ?[*:0]u8 {
    var buf: [160]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "{{\"error\":\"{s}\"}}", .{name}) catch return dupeZ("{\"error\":\"error\"}");
    return dupeZ(msg);
}

/// `{"error":"<Name>","detail":"<SQLite's own words>"}` — for a storage error the
/// name alone says nothing a caller can act on ("PrepareFailed"), the message does
/// ("no such table: test_types"). Escaped by the JSON writer, never by hand.
fn errJsonDetail(name: []const u8, detail: []const u8) ?[*:0]u8 {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var out: std.json.ObjectMap = .empty;
    out.put(a, "error", .{ .string = name }) catch return errJson(name);
    out.put(a, "detail", .{ .string = detail }) catch return errJson(name);
    const s = core.valueToString(a, .{ .object = out }) catch return errJson(name);
    return dupeZ(s);
}

fn lookup(handle: u64) ?*ClientBox {
    return clients.get(handle);
}

/// Everything fallible in one error-returning function per verb (the openBox lesson:
/// an `errdefer` in an `export fn` is dead code), rendered to JSON by the export.
fn syncJson(a: std.mem.Allocator, b: *ClientBox) ![]const u8 {
    const r = try b.c.syncOnce();
    var out: std.json.ObjectMap = .empty;
    try out.put(a, "tenant", .{ .string = r.tenant });
    try out.put(a, "first", .{ .bool = r.first });
    return try core.valueToString(a, .{ .object = out });
}

export fn zb_client_sync(handle: u64) ?[*:0]u8 {
    const b = lookup(handle) orelse return errJson("UnknownHandle");
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const out = syncJson(arena.allocator(), b) catch |err| return errJson(@errorName(err));
    return dupeZ(out);
}

fn queryJson(a: std.mem.Allocator, b: *ClientBox, sql: []const u8, params_text: []const u8) ![]const u8 {
    // §10di: the read-only connection is the enforcement; the shape rule on top
    // refuses what a read-only connection would merely ignore — a connection-local
    // pragma, a second statement after a `;` — so the caller hears it.
    if (!core.isReadOnlySql(sql)) return error.ReadOnlyQuery;
    const params = if (params_text.len == 0) Value{ .array = std.json.Array.init(a) } else (try std.json.parseFromSlice(Value, a, params_text, .{})).value;
    return try core.valueToString(a, try b.c.query(a, sql, params));
}

export fn zb_client_query(handle: u64, sql: ?[*:0]const u8, params_json: ?[*:0]const u8) ?[*:0]u8 {
    const b = lookup(handle) orelse return errJson("UnknownHandle");
    const q = std.mem.span(sql orelse return null);
    const p = if (params_json) |pj| std.mem.span(pj) else "";
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const out = queryJson(arena.allocator(), b, q, p) catch |err| return switch (err) {
        // The replica refused the statement: SQLite's message names the table, the
        // column or the syntax; the error name alone does not. From the READ-ONLY
        // connection the read ran on — the writer's says "not an error".
        error.PrepareFailed, error.BindFailed, error.StepFailed => errJsonDetail(@errorName(err), b.c.ro.errMsg()),
        else => errJson(@errorName(err)),
    };
    return dupeZ(out);
}

fn mutateJson(a: std.mem.Allocator, b: *ClientBox, table: []const u8, op: []const u8, key_text: []const u8, values_text: []const u8, stamp: ?[]const u8) ![]const u8 {
    const key = (try std.json.parseFromSlice(Value, a, key_text, .{})).value;
    const values: ?Value = if (values_text.len == 0) null else (try std.json.parseFromSlice(Value, a, values_text, .{})).value;
    const msg_id = try b.c.mutateAt(a, table, op, key, values, stamp);
    var out: std.json.ObjectMap = .empty;
    try out.put(a, "msgId", .{ .string = msg_id });
    return try core.valueToString(a, .{ .object = out });
}

export fn zb_client_mutate(handle: u64, table: ?[*:0]const u8, op: ?[*:0]const u8, key_json: ?[*:0]const u8, values_json: ?[*:0]const u8) ?[*:0]u8 {
    const b = lookup(handle) orelse return errJson("UnknownHandle");
    const t = std.mem.span(table orelse return null);
    const o = std.mem.span(op orelse return null);
    const k = std.mem.span(key_json orelse return null);
    const v = if (values_json) |vj| std.mem.span(vj) else "";
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const out = mutateJson(arena.allocator(), b, t, o, k, v, null) catch |err| return errJson(@errorName(err));
    return dupeZ(out);
}

/// `zb_client_mutate` with the caller's own version stamp (RFC 3339 UTC, the wire
/// form): a host that keeps its own clock, or a test modelling a slow one.
export fn zb_client_mutate_at(handle: u64, table: ?[*:0]const u8, op: ?[*:0]const u8, key_json: ?[*:0]const u8, values_json: ?[*:0]const u8, version: ?[*:0]const u8) ?[*:0]u8 {
    const b = lookup(handle) orelse return errJson("UnknownHandle");
    const t = std.mem.span(table orelse return null);
    const o = std.mem.span(op orelse return null);
    const k = std.mem.span(key_json orelse return null);
    const v = if (values_json) |vj| std.mem.span(vj) else "";
    const stamp: ?[]const u8 = if (version) |vz| std.mem.span(vz) else null;
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const out = mutateJson(arena.allocator(), b, t, o, k, v, stamp) catch |err| return errJson(@errorName(err));
    return dupeZ(out);
}

fn flushJson(a: std.mem.Allocator, b: *ClientBox, wait_ms: u64) ![]const u8 {
    const r = try b.c.flush(wait_ms);
    var out: std.json.ObjectMap = .empty;
    try out.put(a, "sent", .{ .integer = @intCast(r.sent) });
    try out.put(a, "settled", .{ .integer = @intCast(r.settled) });
    // Cumulative verdict counts by status: the host's view of refusals without
    // parsing stderr (§10dx).
    var vc: std.json.ObjectMap = .empty;
    const c = b.c.verdict_counts;
    inline for (.{ "accepted", "stale", "rejected", "row_deleted", "failed", "other" }) |name| {
        try vc.put(a, name, .{ .integer = @intCast(@field(c, name)) });
    }
    try out.put(a, "verdicts", .{ .object = vc });
    return try core.valueToString(a, .{ .object = out });
}

fn pollJson(a: std.mem.Allocator, b: *ClientBox, wait_ms: u64) ![]const u8 {
    const r = try b.c.poll(a, wait_ms);
    var out: std.json.ObjectMap = .empty;
    try out.put(a, "applied", .{ .integer = @intCast(r.applied) });
    try out.put(a, "settled", .{ .integer = @intCast(r.settled) });

    var changed = std.json.Array.init(a);
    for (r.changed_tables) |t| try changed.append(.{ .string = t });
    try out.put(a, "changed_tables", .{ .array = changed });

    var seeded = std.json.Array.init(a);
    for (r.seeded) |t| try seeded.append(.{ .string = t });
    try out.put(a, "seeded", .{ .array = seeded });

    return try core.valueToString(a, .{ .object = out });
}

/// The host's loop body: `while (running) poll(h, 1000)`. Returns as soon as a CDC
/// batch was applied, or after `wait_ms` with nothing — never earlier on idle, so the
/// host's loop does not spin. Requires a prior `zb_client_sync` (schemas, positions).
export fn zb_client_poll(handle: u64, wait_ms: u64) ?[*:0]u8 {
    const b = lookup(handle) orelse return errJson("UnknownHandle");
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const out = pollJson(arena.allocator(), b, wait_ms) catch |err| {
        // The single door every poll error passes — so the auth verdict is named
        // HERE, once, whichever `try` inside poll surfaced the dead socket (§10cj:
        // a mid-session JWT expiry may appear as ConnectionClosed, a read error or a
        // timeout, but nats.zig recorded the server's "-ERR Authentication Expired").
        if (b.c.authError()) |auth_err| return errJson(@errorName(auth_err));
        return errJson(@errorName(err));
    };
    return dupeZ(out);
}

export fn zb_client_flush(handle: u64, wait_ms: u64) ?[*:0]u8 {
    const b = lookup(handle) orelse return errJson("UnknownHandle");
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const out = flushJson(arena.allocator(), b, wait_ms) catch |err| return errJson(@errorName(err));
    return dupeZ(out);
}

test "smoke: seed gate through the dispatch layer" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const parsed = try std.json.parseFromSlice(Value, a,
        \\{"ev":{"seq":862,"stream":"CDC_kilo"},"anchor":{"seedSeq":862,"seedStream":"CDC_kilo"}}
    , .{});
    const r = try dispatch(a, "seedGate", parsed.value);
    try std.testing.expectEqualStrings("true", r);
}

test {
    _ = @import("storage.zig");
    _ = @import("transport.zig");
    // ⚠️ client.zig belongs here too. Zig analyses lazily, so leaving it out meant
    // `zig build test` never TYPE-CHECKED the client at all — the whole write path
    // compiled clean while containing three errors, and they only surfaced when a
    // binary called it (NOTES §10av). A module absent from the test graph is a module
    // nobody is compiling.
    _ = @import("client.zig");
    _ = @import("handles.zig");
}
