//! Mutation receipts on the writer connection (init.write.template.sql, zebridge_private).
//!
//! One durable outcome per (principal, original message ID). The listener locks a write's
//! receipt key (a transaction-scoped advisory lock), looks the receipt up, runs the write
//! and inserts its complete receipt in the same transaction, so a retry with a retained
//! receipt is answered from it instead of running again. An expired ID without a receipt
//! is refused as indeterminate; it never starts a new execution.
//!
//! The completion guard is enabled by default (-Dreceipt-guard=true). One guard per
//! transaction records the undecided (principal, ID, fingerprint) set; close_guard verifies
//! the completed receipts match it, and a deferred check refuses a COMMIT with an open guard.
//! With the guard disabled, a premature COMMIT can persist writes without their receipts.
//!
//! A receipt failure (a missing partition, a refused claim or finish) is never the
//! client's: the transaction is rolled back and the write retried. It is never answered
//! through the SQLSTATE machinery that turns class 23 into a permanent rejection.
const std = @import("std");
const c = @import("c_imports.zig").c;

const log = std.log.scoped(.receipts);

pub const Status = enum { claimed, duplicate, repeat, conflict, expired, future, invalid };

pub const Claim = struct {
    status: Status,
    /// A duplicate's recorded outcome, as stored (JSON).
    outcome: ?[]const u8 = null,
    /// The write's fingerprint as PostgreSQL's bytea text (`\x…`), for its receipt.
    fingerprint: ?[:0]const u8 = null,
};

/// Whether the completion guard is on. The database decides (receipt_policy.
/// completion_guard); this build only knows whether to close it, and bootCheck refuses a
/// database that disagrees.
pub const guard = @import("build_options").receipt_guard;

/// A guard error (SQLSTATE ZBG01) is the bridge's, never the client's.
pub fn isGuardError(sqlstate: []const u8) bool {
    return std.mem.eql(u8, sqlstate, "ZBG01");
}

/// What a receipt stores and a duplicate gets back: the verdict's meaning without its
/// delivery fields. `seq` belongs to the message being answered and is rebuilt for it.
pub const Outcome = struct {
    status: []const u8,
    reason: []const u8 = "",
    sqlstate: []const u8 = "",
    detail: []const u8 = "",
    version: []const u8 = "",
};

pub fn outcomeJson(a: std.mem.Allocator, o: Outcome) ![:0]u8 {
    const text = try std.json.Stringify.valueAlloc(a, o, .{});
    return a.dupeZ(u8, text);
}

pub fn parseOutcome(a: std.mem.Allocator, text: []const u8) !Outcome {
    return std.json.parseFromSliceLeaky(Outcome, a, text, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
}

const lock_sql = "SELECT zebridge_private.lock_receipts(ARRAY[$1::text], ARRAY[$2::text])";
const claim_sql = "SELECT status, outcome::text, fingerprint FROM zebridge_private.lookup_receipts($1, ARRAY[$2::text], ARRAY[$3::bytea], ARRAY[$4::bytea])";
const finish_sql = "SELECT zebridge_private.record_receipts($1, ARRAY[$2::text], ARRAY[$3::bytea], ARRAY[$4::jsonb])";
pub const close_sql = "SELECT zebridge_private.close_guard()";

/// `$first`, `$first+1`, … as `ARRAY[$i::cast, …]`: every element its own parameter, so
/// bytes travel as bytes (binary) and nothing hand-encodes an array literal.
fn placeholders(a: std.mem.Allocator, sql: *std.ArrayList(u8), first: usize, n: usize, cast: []const u8) !void {
    try sql.appendSlice(a, "ARRAY[");
    for (0..n) |i| {
        if (i > 0) try sql.append(a, ',');
        try sql.print(a, "${d}::{s}", .{ first + i, cast });
    }
    try sql.append(a, ']');
}

/// Every receipt key of a batch, all principals, in one call: lock_receipts takes the locks
/// in one global order (sorted keys), so no two transactions wait on each other's receipt
/// locks in a cycle. Row locks and triggers can still deadlock: that recovery stays.
pub fn sendLocks(a: std.mem.Allocator, conn: ?*c.PGconn, principals: []const [:0]const u8, ids: []const [:0]const u8) !bool {
    const n = ids.len;
    var sql: std.ArrayList(u8) = .empty;
    try sql.appendSlice(a, "SELECT zebridge_private.lock_receipts(");
    try placeholders(a, &sql, 1, n, "text");
    try sql.appendSlice(a, ", ");
    try placeholders(a, &sql, 1 + n, n, "text");
    try sql.append(a, ')');
    const sql_z = try a.dupeZ(u8, sql.items);
    const values = try a.alloc(?[*:0]const u8, 2 * n);
    for (0..n) |i| {
        values[i] = principals[i].ptr;
        values[n + i] = ids[i].ptr;
    }
    return c.PQsendQueryParams(conn, sql_z.ptr, @intCast(2 * n), null, values.ptr, null, null, 0) == 1;
}

/// One principal's lookups in one call, pipelined; the rows come back in `ids` order. The
/// routes and payloads go as binary parameters: the exact bytes received, which is what
/// the fingerprint is taken over. (Per call, a function costs about 60 µs over its rows:
/// one call per principal, not per write.)
pub fn sendClaims(a: std.mem.Allocator, conn: ?*c.PGconn, principal: [:0]const u8, ids: []const [:0]const u8, routes: []const []const u8, payloads: []const []const u8) !bool {
    const n = ids.len;
    var sql: std.ArrayList(u8) = .empty;
    try sql.appendSlice(a, "SELECT status, outcome::text, fingerprint FROM zebridge_private.lookup_receipts($1, ");
    try placeholders(a, &sql, 2, n, "text");
    try sql.appendSlice(a, ", ");
    try placeholders(a, &sql, 2 + n, n, "bytea");
    try sql.appendSlice(a, ", ");
    try placeholders(a, &sql, 2 + 2 * n, n, "bytea");
    try sql.appendSlice(a, ") ORDER BY ord");
    const sql_z = try a.dupeZ(u8, sql.items);
    const values = try a.alloc(?[*]const u8, 1 + 3 * n);
    const lengths = try a.alloc(c_int, 1 + 3 * n);
    const formats = try a.alloc(c_int, 1 + 3 * n);
    values[0], lengths[0], formats[0] = .{ principal.ptr, 0, 0 };
    for (0..n) |i| {
        values[1 + i], lengths[1 + i], formats[1 + i] = .{ ids[i].ptr, 0, 0 };
        values[1 + n + i], lengths[1 + n + i], formats[1 + n + i] = .{ routes[i].ptr, @intCast(routes[i].len), 1 };
        values[1 + 2 * n + i], lengths[1 + 2 * n + i], formats[1 + 2 * n + i] = .{ payloads[i].ptr, @intCast(payloads[i].len), 1 };
    }
    return c.PQsendQueryParams(conn, sql_z.ptr, @intCast(1 + 3 * n), null, values.ptr, lengths.ptr, formats.ptr, 0) == 1;
}

/// One principal's complete receipts in one call, pipelined (one INSERT).
pub fn sendFinishes(a: std.mem.Allocator, conn: ?*c.PGconn, principal: [:0]const u8, ids: []const [:0]const u8, fps: []const [:0]const u8, outcomes: []const [:0]const u8) !bool {
    const n = ids.len;
    var sql: std.ArrayList(u8) = .empty;
    try sql.appendSlice(a, "SELECT zebridge_private.record_receipts($1, ");
    try placeholders(a, &sql, 2, n, "text");
    try sql.appendSlice(a, ", ");
    try placeholders(a, &sql, 2 + n, n, "bytea");
    try sql.appendSlice(a, ", ");
    try placeholders(a, &sql, 2 + 2 * n, n, "jsonb");
    try sql.append(a, ')');
    const sql_z = try a.dupeZ(u8, sql.items);
    const values = try a.alloc(?[*:0]const u8, 1 + 3 * n);
    values[0] = principal.ptr;
    for (0..n) |i| {
        values[1 + i] = ids[i].ptr;
        values[1 + n + i] = fps[i].ptr;
        values[1 + 2 * n + i] = outcomes[i].ptr;
    }
    return c.PQsendQueryParams(conn, sql_z.ptr, @intCast(1 + 3 * n), null, values.ptr, null, null, 0) == 1;
}

/// A grouped claim's rows, one per ID in send order.
pub fn readClaims(a: std.mem.Allocator, r: ?*c.PGresult, out: []Claim) !void {
    if (c.PQntuples(r) != @as(c_int, @intCast(out.len))) return error.ReceiptInfrastructure;
    for (out, 0..) |*claim, i| {
        const row: c_int = @intCast(i);
        const status = std.meta.stringToEnum(Status, std.mem.span(c.PQgetvalue(r, row, 0))) orelse return error.ReceiptInfrastructure;
        const outcome: ?[]const u8 = if (c.PQgetisnull(r, row, 1) == 0) try a.dupe(u8, std.mem.span(c.PQgetvalue(r, row, 1))) else null;
        const fp: ?[:0]const u8 = if (c.PQgetisnull(r, row, 2) == 0) try a.dupeZ(u8, std.mem.span(c.PQgetvalue(r, row, 2))) else null;
        claim.* = .{ .status = status, .outcome = outcome, .fingerprint = fp };
    }
}

/// A lookup's row: (status, outcome, fingerprint).
pub fn readClaim(a: std.mem.Allocator, r: ?*c.PGresult) !Claim {
    var one: [1]Claim = undefined;
    try readClaims(a, r, &one);
    return one[0];
}

/// One pipeline sync's results, in send order: `n` statements, then the sync marker.
/// `failed_at` is the first statement that did not succeed (n when all did). The caller
/// clears the results it got with `clearAll`.
pub fn collect(a: std.mem.Allocator, conn: ?*c.PGconn, n: usize, failed_at: *usize) ![]?*c.PGresult {
    const out = try a.alloc(?*c.PGresult, n);
    @memset(out, null);
    failed_at.* = n;
    var at: usize = 0;
    var nulls: usize = 0;
    while (true) {
        const r = c.PQgetResult(conn);
        if (r == null) {
            nulls += 1;
            if (nulls > n + 3) {
                clearAll(out);
                return error.PipelineLost; // the connection died mid-drain
            }
            continue;
        }
        const st = c.PQresultStatus(r);
        if (st == c.PGRES_PIPELINE_SYNC) {
            c.PQclear(r);
            // libpq emits a trailing null after the sync; leaving any result unread makes
            // PQexitPipelineMode fail and wedges the connection.
            while (c.PQgetResult(conn)) |trailing| c.PQclear(trailing);
            break;
        }
        const ok = st == c.PGRES_COMMAND_OK or st == c.PGRES_TUPLES_OK;
        if (!ok and failed_at.* == n) {
            failed_at.* = at;
            if (st != c.PGRES_PIPELINE_ABORTED) {
                const m = c.PQresultErrorMessage(r);
                log.warn("receipt round: statement {d} of {d} failed: {s}", .{ at + 1, n, if (m != null) std.mem.span(m) else "" });
            }
        }
        if (at < n) out[at] = r else c.PQclear(r);
        at += 1;
    }
    return out;
}

pub fn clearAll(results: []?*c.PGresult) void {
    for (results) |*r| if (r.*) |x| {
        c.PQclear(x);
        r.* = null;
    };
}

/// A statement outside pipeline mode; its SQLSTATE and message are returned on failure.
pub const Failure = struct { sqlstate: [5]u8 = .{ 0, 0, 0, 0, 0 }, message: [256]u8 = undefined, message_len: usize = 0 };

pub fn exec(conn: ?*c.PGconn, sql: [:0]const u8, failure: ?*Failure) !void {
    const r = c.PQexec(conn, sql.ptr);
    defer c.PQclear(r);
    const st = c.PQresultStatus(r);
    if (st == c.PGRES_COMMAND_OK or st == c.PGRES_TUPLES_OK) return;
    if (failure) |f| note(f, r);
    return error.StatementFailed;
}

fn note(f: *Failure, r: ?*c.PGresult) void {
    const state = c.PQresultErrorField(r, c.PG_DIAG_SQLSTATE);
    if (state != null) {
        const s = std.mem.span(state);
        @memcpy(f.sqlstate[0..@min(5, s.len)], s[0..@min(5, s.len)]);
    }
    const m = c.PQresultErrorMessage(r);
    const text: []const u8 = if (m != null) std.mem.span(m) else "";
    const line = text[0 .. std.mem.findScalar(u8, text, '\n') orelse text.len];
    f.message_len = @min(line.len, f.message.len);
    @memcpy(f.message[0..f.message_len], line[0..f.message_len]);
}

/// Leave pipeline mode after a round; a connection that cannot is reset (which ends its
/// transaction): an error, never a quiet continuation.
fn leave(conn: ?*c.PGconn) !void {
    if (c.PQpipelineStatus(conn) == c.PQ_PIPELINE_OFF) return;
    while (c.PQgetResult(conn)) |r| c.PQclear(r);
    if (c.PQexitPipelineMode(conn) != 1) {
        c.PQreset(conn);
        return error.TransactionLost;
    }
}

/// On an error mid-round: a pipeline still open is ended by a reset.
fn resetIfPipelined(conn: ?*c.PGconn) void {
    if (c.PQpipelineStatus(conn) != c.PQ_PIPELINE_OFF) c.PQreset(conn);
}

/// The single path's first exchange: BEGIN, the lock, the lookup (which opens the guard
/// when enabled) and SAVEPOINT zb_write in one pipeline round. The lock and the guard come
/// before the savepoint, so rolling back to it keeps both. An unclaimed write rolls it all
/// back. On success the transaction is open, out of pipeline mode. Any failure is the
/// receipts' (the caller rolls back and retries).
pub fn beginClaim(a: std.mem.Allocator, conn: ?*c.PGconn, principal: [:0]const u8, id: [:0]const u8, route: []const u8, payload: []const u8) !Claim {
    if (c.PQenterPipelineMode(conn) != 1) return error.ReceiptInfrastructure;
    errdefer resetIfPipelined(conn);
    const values = [_]?[*]const u8{ principal.ptr, id.ptr, route.ptr, payload.ptr };
    const lengths = [_]c_int{ 0, 0, @intCast(route.len), @intCast(payload.len) };
    const formats = [_]c_int{ 0, 0, 1, 1 };
    const lock_values = [_]?[*:0]const u8{ principal.ptr, id.ptr };
    const sent = c.PQsendQueryParams(conn, "BEGIN", 0, null, null, null, null, 0) == 1 and
        c.PQsendQueryParams(conn, lock_sql, 2, null, &lock_values, null, null, 0) == 1 and
        c.PQsendQueryParams(conn, claim_sql, 4, null, @ptrCast(&values), &lengths, &formats, 0) == 1 and
        c.PQsendQueryParams(conn, "SAVEPOINT zb_write", 0, null, null, null, null, 0) == 1 and
        c.PQpipelineSync(conn) == 1;
    if (!sent) return error.ReceiptInfrastructure;
    var failed_at: usize = 0;
    const rs = try collect(a, conn, 4, &failed_at);
    defer clearAll(rs);
    try leave(conn);
    if (failed_at != 4) return error.ReceiptInfrastructure;
    return readClaim(a, rs[2]);
}

/// The single path's last exchange: the receipt (and close_guard when enabled), the
/// application's deferred constraints checked now (SET CONSTRAINTS ALL IMMEDIATE) and
/// COMMIT, in one pipeline round. `skip_receipt` is the commit_before_receipts test fault:
/// COMMIT with no receipt and no close; the guard refuses it at SET CONSTRAINTS (ZBG01).
///   error.ReceiptInfrastructure  the receipt or the guard was refused
///   error.DeferredRefused        a deferred constraint refused the write; `failure` holds
///                                why, and the transaction is aborted but open (the caller
///                                rolls back to the savepoint and records the rejection)
///   error.CommitFailed           refused or lost at COMMIT: ambiguous, retry
pub fn finishCommit(a: std.mem.Allocator, conn: ?*c.PGconn, principal: [:0]const u8, id: [:0]const u8, fp: [:0]const u8, outcome: [:0]const u8, skip_receipt: bool, failure: *Failure) !void {
    if (c.PQenterPipelineMode(conn) != 1) return error.ReceiptInfrastructure;
    errdefer resetIfPipelined(conn);
    const values = [_]?[*:0]const u8{ principal.ptr, id.ptr, fp.ptr, outcome.ptr };
    const receipt_stmts: usize = if (skip_receipt) 0 else if (guard) 2 else 1;
    var sent = true;
    if (!skip_receipt) {
        sent = c.PQsendQueryParams(conn, finish_sql, 4, null, &values, null, null, 0) == 1;
        if (guard) sent = sent and c.PQsendQueryParams(conn, close_sql, 0, null, null, null, null, 0) == 1;
    }
    sent = sent and c.PQsendQueryParams(conn, "SET CONSTRAINTS ALL IMMEDIATE", 0, null, null, null, null, 0) == 1 and
        c.PQsendQueryParams(conn, "COMMIT", 0, null, null, null, null, 0) == 1 and
        c.PQpipelineSync(conn) == 1;
    if (!sent) return error.CommitFailed;
    const n = receipt_stmts + 2;
    var failed_at: usize = 0;
    const rs = collect(a, conn, n, &failed_at) catch return error.CommitFailed; // lost mid-round: ambiguous
    defer clearAll(rs);
    try leave(conn);
    if (failed_at < receipt_stmts) return error.ReceiptInfrastructure;
    if (failed_at == receipt_stmts) {
        note(failure, rs[receipt_stmts]);
        if (isGuardError(std.mem.sliceTo(&failure.sqlstate, 0))) return error.ReceiptInfrastructure;
        return error.DeferredRefused;
    }
    if (failed_at == n - 1 or !committed(rs[n - 1])) return error.CommitFailed;
}

/// One receipt outside pipeline mode (and close_guard when enabled).
pub fn finishOne(conn: ?*c.PGconn, principal: [:0]const u8, id: [:0]const u8, fp: [:0]const u8, outcome: [:0]const u8) !void {
    const values = [_]?[*:0]const u8{ principal.ptr, id.ptr, fp.ptr, outcome.ptr };
    const r = c.PQexecParams(conn, finish_sql, 4, null, &values, null, null, 0);
    defer c.PQclear(r);
    if (c.PQresultStatus(r) != c.PGRES_TUPLES_OK) {
        const m = c.PQresultErrorMessage(r);
        log.err("🔴 receipt record failed: {s}", .{if (m != null) std.mem.span(m) else ""});
        return error.ReceiptInfrastructure;
    }
    if (guard) exec(conn, close_sql, null) catch {
        log.err("🔴 receipt guard refused to close", .{});
        return error.ReceiptInfrastructure;
    };
}

/// COMMIT, and the proof it committed: PostgreSQL answers a COMMIT of an aborted
/// transaction with the tag ROLLBACK and no error, so the tag is what is checked.
pub fn commit(conn: ?*c.PGconn, failure: ?*Failure) !void {
    const r = c.PQexec(conn, "COMMIT");
    defer c.PQclear(r);
    if (c.PQresultStatus(r) != c.PGRES_COMMAND_OK) {
        if (failure) |f| note(f, r);
        return error.CommitFailed;
    }
    if (!std.mem.eql(u8, std.mem.span(c.PQcmdStatus(r)), "COMMIT")) return error.CommitFailed;
}

/// The commit tag of a pipelined COMMIT's result.
pub fn committed(r: ?*c.PGresult) bool {
    return c.PQresultStatus(r) == c.PGRES_COMMAND_OK and std.mem.eql(u8, std.mem.span(c.PQcmdStatus(r)), "COMMIT");
}

/// Best effort: a failed ROLLBACK means the connection is gone, and the caller's next
/// health check resets it.
pub fn rollback(conn: ?*c.PGconn) void {
    if (c.PQtransactionStatus(conn) == c.PQTRANS_IDLE) return;
    const r = c.PQexec(conn, "ROLLBACK");
    c.PQclear(r);
}

/// The month partitions every claim needs: run at startup and daily, outside any
/// transaction. A failure is the operator's: claims fail until it succeeds.
pub fn maintain(conn: ?*c.PGconn) !void {
    const r = c.PQexec(conn, "SELECT string_agg(p, ',') FROM zebridge_private.maintain_receipts() p");
    defer c.PQclear(r);
    if (c.PQresultStatus(r) != c.PGRES_TUPLES_OK) {
        const m = c.PQresultErrorMessage(r);
        log.err("🔴 receipt maintenance failed — writes will be retried until it succeeds: {s}", .{if (m != null) std.mem.span(m) else ""});
        return error.ReceiptInfrastructure;
    }
    if (c.PQgetisnull(r, 0, 0) == 0) log.info("🧾 receipt partitions created: {s}", .{std.mem.span(c.PQgetvalue(r, 0, 0))});
}

/// At boot, on the writer: the receipt schema is installed and this role may use it,
/// then one maintenance run, so the first write finds its month's partition. Without it
/// every write would wait and retry; refusing to start says why at once.
pub fn bootCheck(conn: ?*c.PGconn) !void {
    const sql =
        \\SELECT has_schema_privilege(current_user, 'zebridge_private', 'USAGE')
        \\   AND has_function_privilege(current_user, 'zebridge_private.lock_receipts(text[], text[])', 'EXECUTE')
        \\   AND has_function_privilege(current_user, 'zebridge_private.lookup_receipts(text, text[], bytea[], bytea[])', 'EXECUTE')
        \\   AND has_function_privilege(current_user, 'zebridge_private.record_receipts(text, text[], bytea[], jsonb[])', 'EXECUTE')
        \\   AND has_function_privilege(current_user, 'zebridge_private.close_guard()', 'EXECUTE')
        \\   AND has_function_privilege(current_user, 'zebridge_private.maintain_receipts()', 'EXECUTE')
    ;
    const r = c.PQexec(conn, sql);
    defer c.PQclear(r);
    if (c.PQresultStatus(r) != c.PGRES_TUPLES_OK) {
        const m = c.PQresultErrorMessage(r);
        log.err("🔴 FATAL: the mutation receipts are not installed ({s}) — apply the current `bridge --init-sql` to this database before starting this bridge", .{if (m != null) std.mem.trimEnd(u8, std.mem.span(m), "\n") else "?"});
        return error.ReceiptsNotInstalled;
    }
    if (!std.mem.eql(u8, std.mem.span(c.PQgetvalue(r, 0, 0)), "t")) {
        log.err("🔴 FATAL: the writer role may not use the mutation receipts (zebridge_private) — apply the current `bridge --init-sql`, which grants them, before starting this bridge", .{});
        return error.ReceiptsNotInstalled;
    }
    const g = c.PQexec(conn, "SELECT zebridge_private.completion_guard_on()");
    defer c.PQclear(g);
    if (c.PQresultStatus(g) != c.PGRES_TUPLES_OK or c.PQntuples(g) != 1) {
        log.err("🔴 FATAL: the receipt policy has no completion_guard — apply the current `bridge --init-sql`", .{});
        return error.ReceiptsNotInstalled;
    }
    const on = std.mem.eql(u8, std.mem.span(c.PQgetvalue(g, 0, 0)), "t");
    if (on != guard) {
        log.err("🔴 FATAL: receipt_policy.completion_guard is {} but this bridge requires {}", .{ on, guard });
        if (guard and !on) log.err("After draining and stopping old workers, enable the completion guard as administrator: UPDATE zebridge_private.receipt_policy SET completion_guard = true WHERE singleton;", .{});
        return error.ReceiptsNotInstalled;
    }
    if (!guard) log.warn("Receipt completion guard disabled: a premature COMMIT can persist writes without receipts", .{});
    log.info("🧾 receipts: advisory locks, completion guard {s}", .{if (guard) "enabled" else "disabled"});
    try maintain(conn);
}

test "outcome JSON round-trips and escapes the detail" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try outcomeJson(a, .{ .status = "rejected", .reason = "MutationFailed", .sqlstate = "23505", .detail = "duplicate key \"x\"\nline 2", .version = "" });
    const back = try parseOutcome(a, json);
    try std.testing.expectEqualStrings("rejected", back.status);
    try std.testing.expectEqualStrings("duplicate key \"x\"\nline 2", back.detail);
    try std.testing.expectEqualStrings("23505", back.sqlstate);
    const sparse = try parseOutcome(a, "{\"status\":\"accepted\",\"seq\":7}");
    try std.testing.expectEqualStrings("accepted", sparse.status);
    try std.testing.expectEqualStrings("", sparse.reason);
}
