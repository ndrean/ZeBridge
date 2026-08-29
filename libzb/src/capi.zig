//! The C ABI: one JSON dispatch entrypoint.
//!
//!   char*    zb_call(const char* fn, const char* args_json);  // caller frees via zb_free
//!                                                            // NULL: a NULL argument, or malloc failed
//!   void     zb_free(char* p);
//!   int      zb_abi_version(void);
//!
//!   uint64_t zb_client_open(const char* opts_json);   // 0 on failure
//!   int      zb_client_close(uint64_t handle);        // 0 ok, 1 unknown handle
//!   int      zb_client_live(void);                    // open clients, for tests
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
    grammar: [:0]u8,
    db: [:0]u8,
    principal: [:0]u8,

    fn destroy(self: *ClientBox, a: std.mem.Allocator) void {
        self.c.deinit();
        a.free(self.url);
        a.free(self.creds);
        a.free(self.grammar);
        a.free(self.db);
        a.free(self.principal);
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
    const grammar = try a.dupeZ(u8, str.get(o, "grammarPath", "grammar.json"));
    errdefer a.free(grammar);
    const db = try a.dupeZ(u8, str.get(o, "dbPath", "zb.sqlite3"));
    errdefer a.free(db);
    const principal = try a.dupeZ(u8, str.get(o, "principal", ""));
    errdefer a.free(principal);

    const box = try a.create(ClientBox);
    errdefer a.destroy(box);

    box.* = .{
        .c = try client.SyncClient.init(a, .{
            .url = url,
            .creds_path = creds,
            .grammar_path = grammar,
            .db_path = db,
            .principal = principal,
            .tables = &.{},
        }),
        .url = url,
        .creds = creds,
        .grammar = grammar,
        .db = db,
        .principal = principal,
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

/// Open clients. Exists so a test can assert the table empties — a leak of a whole
/// client is otherwise invisible from outside.
export fn zb_client_live() c_int {
    return @intCast(clients.liveCount());
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
