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
//!   char* zb_client_sync(uint64_t h);                              // {"tenant":…,"tenants":[…],"first":bool}
//!   char* zb_client_query(uint64_t h, const char* sql, const char* params_json);
//!                                                                  // {"columns":[…],"rows":[[…],…]} — read-only connection
//!   char* zb_client_mutate(uint64_t h, const char* table, const char* op,
//!       // key_json addresses the row; an INSERT's values_json carries the WHOLE row, key included (the bridge builds it from data)
//!                          const char* key_json, const char* values_json);   // {"msgId":…}
//!   char* zb_client_flush(uint64_t h, uint64_t wait_ms);           // {"sent":n,"settled":n}
//!   char* zb_client_request(uint64_t h, const char* subject, const char* payload_json, uint64_t timeout_ms);
//!                                                                    // §10hj: ask a service on `query.<tenant>.<name>`; its reply verbatim
//!   char* zb_client_ingest(uint64_t h, const char* table, const char* answer_json, const char* scope_json);
//!                                                                    // §10hj: keep an answer ({"columns","rows"}) in an on-demand table → {"applied":n}
//!   char* zb_client_poll(uint64_t h, uint64_t wait_ms);            // {"applied":n,"settled":n,…,"requests":[…]?,"unreadable":[…]?} — live tail, blocks ≤ wait_ms
//!   char* zb_client_serve(uint64_t h, const char* opts_json);      // §10hp: answer query.<tenant>.<name> in a queue group → {"serving":n}
//!   char* zb_client_reply(uint64_t h, uint64_t id, const char* answer_json);  // §10hp: answer one request from poll
//!   char* zb_client_join(uint64_t h, const char* tenant);          // {"tenants":[…]} — follow one more tenant (§10fn)
//!   char* zb_client_leave(uint64_t h, const char* tenant);         // {"tenants":[…]} — drop one: its rows, watermarks, tail
//! `opts_json`: natsUrl, credsPath, dbPath, principal, tables (array, parents first — or
//! the string "*": every published table, §10hn; absent: nothing is followed),
//! ondemandTables (§10hj: schema yes, seed and tail no — filled by `zb_client_ingest`;
//! a name in both lists is on-demand),
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

fn boolField(args: Value, key: []const u8) bool {
    const v = args.object.get(key) orelse return false;
    return v == .bool and v.bool;
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
    if (eq(u8, name, "cdcBulk")) {
        const engine = args.object.get("engine").?.string;
        const tables = args.object.get("tables") orelse .null;
        const events = args.object.get("events") orelse .null;
        return try core.valueToString(a, try core.planCdcBulk(a, engine, tables, events));
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
        return try core.valueToString(a, try core.createTableSteps(a, args.object.get("table").?.string, args.object.get("cols").?.array, try strArrField(a, args, "pkCols"), args.object.get("fks").?.array, boolField(args, "strict")));
    }
    if (eq(u8, name, "rebuildSteps")) {
        return try core.valueToString(a, try core.rebuildSteps(a, args.object.get("table").?.string, args.object.get("cols").?.array, try strArrField(a, args, "pkCols"), args.object.get("fks").?.array, try strArrField(a, args, "existing"), boolField(args, "strict")));
    }
    if (eq(u8, name, "mergeRegisters")) {
        return try core.valueToString(a, try core.mergeRegisters(a, args.object.get("a") orelse .null, args.object.get("b") orelse .null));
    }
    if (eq(u8, name, "tableSet")) {
        const tv = args.object.get("tables") orelse .null;
        const all = tv == .string and eq(u8, tv.string, "*");
        const tables: []const []const u8 = if (tv == .array) try core.strArrPub(a, tv.array) else &.{};
        const ov = args.object.get("ondemand") orelse .null;
        const ondemand: []const []const u8 = if (ov == .array) try core.strArrPub(a, ov.array) else &.{};
        const keys = try strArrField(a, args, "keys");
        try core.writeTableSet(a, &out, try core.tableSet(a, all, tables, ondemand, keys));
        return out.items;
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
    ondemand: []const []const u8,
    all_tables: []const []const u8,

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
        for (self.ondemand) |t| a.free(t);
        a.free(self.ondemand);
        a.free(self.all_tables);
        a.destroy(self);
    }
};

/// Open a client. `opts_json` mirrors `client.Options`; 0 means it did not open, and
/// the reason is logged rather than returned — a richer error channel is worth adding
/// the day a host needs to distinguish "bad credentials" from "no broker".
export fn zb_client_open(opts_json: ?[*:0]const u8) u64 {
    const text = std.mem.span(opts_json orelse return 0);
    // §10hw: the doc above promised the reason was logged; it was swallowed. A bare 0
    // with no line is the worst failure this ABI can produce, because the host has
    // nothing at all to search for.
    const box = openBox(std.heap.c_allocator, text) catch |err| {
        std.debug.print("zb_client_open failed: {s}\n", .{@errorName(err)});
        return 0;
    };
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
    // §10hn: `natsUrl`, the one name both clients share.
    const url = try a.dupeZ(u8, str.get(o, "natsUrl", "nats://127.0.0.1:4222"));
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
    // engine (§10fl): "sqlite" (default) or "duckdb", both on dbPath; dbUrl implies postgres.
    const engine_s = str.get(o, "engine", "sqlite");
    const engine: client.storage.Engine = if (std.mem.eql(u8, engine_s, "duckdb")) .duckdb else .sqlite;
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
    // seedStreaming (§10fh): the chain object inflated as it is read, rows applied in
    // chunks sorted on their own — bounded memory for a phone; off by default.
    const seed_streaming: bool = blk: {
        const f = o.object.get("seedStreaming") orelse break :blk false;
        break :blk f == .bool and f.bool;
    };
    // seedStreamingAboveBytes (§10fh): with seedStreaming, a step whose compressed
    // object is smaller takes the whole-object path anyway. Default 8 MiB.
    const seed_streaming_above: usize = blk: {
        const f = o.object.get("seedStreamingAboveBytes") orelse break :blk 8 * 1024 * 1024;
        break :blk if (f == .integer and f.integer >= 0) @intCast(f.integer) else 8 * 1024 * 1024;
    };
    // §10hn: which tables this client holds — core.tableSet, the rule both clients
    // share. `tables` is a list (parents first) or "*", every published table;
    // `ondemandTables` are held for their schema only (§10hj); a name in both is
    // on-demand; names are deduplicated. Each survivor is its own allocation so the
    // box can free it; the intermediates live in a scratch arena.
    var sa = std.heap.ArenaAllocator.init(a);
    defer sa.deinit();
    const ta = sa.allocator();
    const tv = o.object.get("tables") orelse Value.null;
    const follow_all = tv == .string and std.mem.eql(u8, tv.string, "*");
    const odv = o.object.get("ondemandTables") orelse Value.null;
    const set = try core.tableSet(ta, false, if (tv == .array) try core.strArrPub(ta, tv.array) else &.{}, if (odv == .array) try core.strArrPub(ta, odv.array) else &.{}, &.{});
    const tables = try a.alloc([]const u8, set.follow.len);
    var filled: usize = 0;
    errdefer {
        for (tables[0..filled]) |t| a.free(t);
        a.free(tables);
    }
    for (set.follow) |t| {
        tables[filled] = try a.dupe(u8, t);
        filled += 1;
    }
    const ondemand = try a.alloc([]const u8, set.ondemand.len);
    var od_filled: usize = 0;
    errdefer {
        for (ondemand[0..od_filled]) |t| a.free(t);
        a.free(ondemand);
    }
    for (set.ondemand) |t| {
        ondemand[od_filled] = try a.dupe(u8, t);
        od_filled += 1;
    }
    // The union drives the schema walk; `ondemand` alone decides seed and tail.
    const all_tables = try a.alloc([]const u8, tables.len + ondemand.len);
    errdefer a.free(all_tables);
    @memcpy(all_tables[0..tables.len], tables);
    @memcpy(all_tables[tables.len..], ondemand);

    const box = try a.create(ClientBox);
    errdefer a.destroy(box);

    box.* = .{
        .c = try client.SyncClient.init(a, .{
            .url = url,
            .creds_path = creds,
            .grammar_hash = grammar_hash,
            .heartbeat_ms = heartbeat_ms,
            .seed_chunk_rows = seed_chunk_rows,
            .seed_streaming = seed_streaming,
            .seed_streaming_above = seed_streaming_above,
            .db_path = db,
            .db_url = if (db_url) |u| u.ptr else null,
            .engine = engine,
            .principal = principal,
            .tables = all_tables,
            .follow_all = follow_all,
            .ondemand = ondemand,
            .client_id = client_id,
        }),
        .url = url,
        .creds = creds,
        .grammar_hash = grammar_hash,
        .db = db,
        .principal = principal,
        .client_id = client_id,
        .tables = tables,
        .ondemand = ondemand,
        .all_tables = all_tables,
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
    try out.put(a, "tenants", try tenantsJson(a, r.tenants));
    try out.put(a, "first", .{ .bool = r.first });
    return try core.valueToString(a, .{ .object = out });
}

fn tenantsJson(a: std.mem.Allocator, tenants: []const []const u8) !std.json.Value {
    var arr = std.json.Array.init(a);
    for (tenants) |t| try arr.append(.{ .string = t });
    return .{ .array = arr };
}

/// §10fn: `{"tenants":[…]}` after a join or a leave — the memberships followed now.
fn membershipJson(a: std.mem.Allocator, b: *ClientBox, tenant: []const u8, joining: bool) ![]const u8 {
    if (joining) try b.c.join(tenant) else try b.c.leave(tenant);
    var out: std.json.ObjectMap = .empty;
    try out.put(a, "tenants", try tenantsJson(a, b.c.tenants));
    return try core.valueToString(a, .{ .object = out });
}

/// Follow one more tenant: subscribe to its stream, seed its chains into the same
/// local tables. The JWT decides whether the broker allows it. Returns
/// `{"tenants":[…]}` or `{"error":…}`.
export fn zb_client_join(handle: u64, tenant: ?[*:0]const u8) ?[*:0]u8 {
    const b = lookup(handle) orelse return errJson("UnknownHandle");
    const t = std.mem.span(tenant orelse return errJson("NullArgument"));
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const out = membershipJson(arena.allocator(), b, t, true) catch |err| return errJson(@errorName(err));
    return dupeZ(out);
}

/// Stop following a tenant: its rows leave the local tables, its watermarks and
/// positions are forgotten, its tail is closed. Returns `{"tenants":[…]}`.
export fn zb_client_leave(handle: u64, tenant: ?[*:0]const u8) ?[*:0]u8 {
    const b = lookup(handle) orelse return errJson("UnknownHandle");
    const t = std.mem.span(tenant orelse return errJson("NullArgument"));
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const out = membershipJson(arena.allocator(), b, t, false) catch |err| return errJson(@errorName(err));
    return dupeZ(out);
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
    inline for (.{ "accepted", "stale", "rejected", "row_deleted", "failed", "rate_limited", "other" }) |name| {
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

    // §10hp: the questions this client was asked, when it serves any. Each must be
    // answered with `zb_client_reply(h, id, answer_json)`.
    if (r.requests.len > 0) {
        var reqs = std.json.Array.init(a);
        for (r.requests) |q| {
            var one: std.json.ObjectMap = .empty;
            try one.put(a, "id", .{ .integer = @intCast(q.id) });
            try one.put(a, "tenant", .{ .string = q.tenant });
            try one.put(a, "name", .{ .string = q.name });
            // The payload is the asker's JSON, parsed here so the host gets an object
            // rather than a string it must parse again; unparseable payloads arrive as
            // null and the host answers an error, which is the asker's problem.
            const parsed = std.json.parseFromSlice(Value, a, if (q.payload.len > 0) q.payload else "null", .{}) catch null;
            try one.put(a, "payload", if (parsed) |p| p.value else .null);
            try reqs.append(.{ .object = one });
        }
        try out.put(a, "requests", .{ .array = reqs });
    }

    // §10fq: tenants whose streams are set aside — present only when there are some.
    if (r.unreadable.len > 0) {
        var unreadable = std.json.Array.init(a);
        for (r.unreadable) |t| try unreadable.append(.{ .string = t });
        try out.put(a, "unreadable", .{ .array = unreadable });
    }

    return try core.valueToString(a, .{ .object = out });
}

/// §10hp: answer `query.<tenant>.<name>` on this client's own connection, in a queue
/// group. `opts_json`: {"tenants": ["kilo","_default"], "queries": ["fuel_near", …],
/// "queue": "pois"}. Returns {"serving": n}, the number of subjects answered.
///
/// The questions arrive in `zb_client_poll`'s report as `requests`; each is answered
/// with `zb_client_reply`. No second connection, no thread, no lock: a responder is a
/// client that answers questions about its own replica.
export fn zb_client_serve(handle: u64, opts_json: ?[*:0]const u8) ?[*:0]u8 {
    const b = lookup(handle) orelse return errJson("UnknownHandle");
    const text = std.mem.span(opts_json orelse return errJson("NoOptions"));
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const parsed = std.json.parseFromSlice(Value, a, text, .{}) catch return errJson("BadJson");
    const o = parsed.value;
    if (o != .object) return errJson("NotAnObject");
    const tenants = strArrField(a, o, "tenants") catch return errJson("OutOfMemory");
    const names = strArrField(a, o, "queries") catch return errJson("OutOfMemory");
    if (tenants.len == 0 or names.len == 0) return errJson("NothingToServe");
    const qv = o.object.get("queue") orelse Value.null;
    const queue = if (qv == .string and qv.string.len > 0) qv.string else "zb";
    const n = b.c.serve(tenants, names, queue) catch |err| return errJson(@errorName(err));
    const out = std.fmt.allocPrint(a, "{{\"serving\":{d}}}", .{n}) catch return errJson("OutOfMemory");
    return dupeZ(out);
}

/// §10hp: the answer to one question from `poll`'s `requests`, on the asker's inbox.
/// `answer_json` is sent as it stands. Returns {"replied":<id>}.
export fn zb_client_reply(handle: u64, id: u64, answer_json: ?[*:0]const u8) ?[*:0]u8 {
    const b = lookup(handle) orelse return errJson("UnknownHandle");
    const answer = std.mem.span(answer_json orelse return errJson("NoAnswer"));
    b.c.reply(id, answer) catch |err| return errJson(@errorName(err));
    var buf: [64]u8 = undefined;
    const out = std.fmt.bufPrint(&buf, "{{\"replied\":{d}}}", .{id}) catch return errJson("OutOfMemory");
    return dupeZ(out);
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

/// §10hj: one request/reply on the card's connection. `subject` a query subject the
/// principal may publish to (`query.<tenant>.<name>`), `payload_json` the question;
/// the service's reply comes back verbatim (JSON by convention), or {"error":…}.
export fn zb_client_request(handle: u64, subject: ?[*:0]const u8, payload_json: ?[*:0]const u8, timeout_ms: u64) ?[*:0]u8 {
    const b = lookup(handle) orelse return errJson("UnknownHandle");
    const subj = std.mem.span(subject orelse return null);
    const payload = if (payload_json) |p| std.mem.span(p) else "";
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const out = b.c.request(arena.allocator(), subj, payload, timeout_ms) catch |err| return errJson(@errorName(err));
    return dupeZ(out);
}

/// §10hj: a service's answer into an on-demand table — `answer_json` is
/// `{"columns":[…],"rows":[[…],…]}`, `scope_json` optional `{"where": "<sql over the
/// table's columns, ? params>", "params": […]}`: the area the answer is authoritative
/// for, whose local rows the answer did not carry are deleted. {"applied": n}.
export fn zb_client_ingest(handle: u64, table: ?[*:0]const u8, answer_json: ?[*:0]const u8, scope_json: ?[*:0]const u8) ?[*:0]u8 {
    const b = lookup(handle) orelse return errJson("UnknownHandle");
    const tbl = std.mem.span(table orelse return null);
    const ans = std.mem.span(answer_json orelse return null);
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const answer = (std.json.parseFromSlice(Value, a, ans, .{}) catch return errJson("MalformedAnswer")).value;
    const scope: ?Value = if (scope_json) |sj| blk: {
        const s = std.mem.span(sj);
        if (s.len == 0) break :blk null;
        break :blk (std.json.parseFromSlice(Value, a, s, .{}) catch return errJson("MalformedScope")).value;
    } else null;
    const n = b.c.ingest(a, tbl, answer, scope) catch |err| return switch (err) {
        error.PrepareFailed, error.BindFailed, error.StepFailed => errJsonDetail(@errorName(err), b.c.st.errMsg()),
        else => errJson(@errorName(err)),
    };
    const out = std.fmt.allocPrint(a, "{{\"applied\":{d}}}", .{n}) catch return errJson("OutOfMemory");
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
