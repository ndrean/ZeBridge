//! `bridge --view-slots`, `bridge --view-slot <slot>`, `bridge --drop-slot <slot>`:
//! the replication slots without hunting for the right psql (§10en).
//!
//! The slot is the one piece of state a bridge leaves in PostgreSQL, and the one an
//! operator meets in anger: an abandoned instance keeps its slot, the slot keeps the
//! WAL, and the disk fills while every dashboard says the bridge that matters is
//! healthy. The fleet's inventory already renders every slot on /metrics; this is the
//! same question from a shell that has the bridge binary and nothing else.
//!
//! Privilege posture, the revoke verb's: reading uses the bridge's own
//! DATABASE_READER_URL (the reader role sees `pg_replication_slots`), so a view is
//! ambient; dropping needs ADMIN_DATABASE_URL passed FOR THE INVOCATION and never
//! stored — the capability that frees retained WAL is also the one that kills a
//! running bridge, and a machine holding the bridge's env must not have it at hand.
//! An active slot is refused: stop that bridge first.
const std = @import("std");
const c = @import("c_imports.zig").c;

const out = std.debug.print;

pub const Mode = enum { view_all, view_one, drop };

pub fn run(init: *const std.process.Init, mode: Mode) u8 {
    // ── the slot name, from argv (view_one and drop) ────────────────────────────
    var slot: ?[]const u8 = null;
    {
        var it = init.minimal.args.iterate();
        _ = it.next();
        while (it.next()) |arg| {
            if (std.mem.eql(u8, arg, "--view-slot") or std.mem.eql(u8, arg, "--drop-slot")) slot = it.next();
        }
    }
    if (mode != .view_all and (slot == null or slot.?.len == 0)) {
        out("usage: bridge {s} <slot>\n", .{if (mode == .drop) "--drop-slot" else "--view-slot"});
        return 1;
    }
    if (slot) |s| if (s.len > 63) {
        out("🔴 '{s}' is not a slot name (63 characters at most)\n", .{s});
        return 1;
    };

    // ── the connection ──────────────────────────────────────────────────────────
    const url = switch (mode) {
        .drop => init.minimal.environ.getPosix("ADMIN_DATABASE_URL") orelse {
            out("🔴 ADMIN_DATABASE_URL is not set. Dropping a slot frees the WAL it retains — and kills the bridge that reads it — so the capability is passed for the invocation, never stored:\n" ++
                "   ADMIN_DATABASE_URL=postgres://<admin>:<password>@host:port/db bridge --drop-slot <slot>\n", .{});
            return 1;
        },
        else => init.minimal.environ.getPosix("DATABASE_READER_URL") orelse init.minimal.environ.getPosix("ADMIN_DATABASE_URL") orelse {
            out("🔴 neither DATABASE_READER_URL nor ADMIN_DATABASE_URL is set — the bridge's own env is enough to read the slots:\n   set -a && . ./.env.bridge && set +a\n", .{});
            return 1;
        },
    };
    var url_buf: [1024]u8 = undefined;
    const url_z = std.fmt.bufPrintZ(&url_buf, "{s}", .{url}) catch {
        out("🔴 the database URL is too long ({d} bytes)\n", .{url.len});
        return 1;
    };
    const conn = c.PQconnectdb(url_z.ptr) orelse {
        out("🔴 could not connect: PQconnectdb returned null\n", .{});
        return 1;
    };
    defer c.PQfinish(conn);
    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        out("🔴 could not connect: {s}", .{c.PQerrorMessage(conn)});
        return 1;
    }

    // ── the inventory: every slot, or the one named ─────────────────────────────
    // Retained WAL is measured from the current head; on a standby the head is the
    // last received LSN, so the query is tried both ways rather than assuming a primary.
    var sql_buf: [1024]u8 = undefined;
    const res = queryInventory(conn, &sql_buf, "pg_current_wal_lsn()", slot) orelse
        queryInventory(conn, &sql_buf, "pg_last_wal_receive_lsn()", slot) orelse {
        out("🔴 could not read pg_replication_slots: {s}", .{c.PQerrorMessage(conn)});
        return 1;
    };
    defer c.PQclear(res);
    const n: usize = @intCast(c.PQntuples(res));

    if (mode != .drop) {
        if (n == 0) {
            if (slot) |s| out("no replication slot named '{s}'\n", .{s}) else out("no replication slot on this server\n", .{});
            return if (slot != null) 1 else 0;
        }
        out("{s:<28} {s:<8} {s:<10} {s:<7} {s:<8} {s:<14} {s:<14} {s:<10} {s}\n", .{ "slot", "type", "plugin", "active", "pid", "restart_lsn", "confirmed", "wal", "retained" });
        for (0..n) |i| {
            out("{s:<28} {s:<8} {s:<10} {s:<7} {s:<8} {s:<14} {s:<14} {s:<10} {s}\n", .{
                cell(res, i, 0), cell(res, i, 1), cell(res, i, 2), cell(res, i, 3), cell(res, i, 4),
                cell(res, i, 5), cell(res, i, 6), cell(res, i, 7), cell(res, i, 8),
            });
        }
        out("\nretained = WAL the server keeps for the slot; it grows for a slot nobody reads. An inactive slot with a growing figure is an abandoned bridge: `bridge --drop-slot <slot>`.\n", .{});
        return 0;
    }

    // ── the drop ────────────────────────────────────────────────────────────────
    const name = slot.?;
    if (n == 0) {
        out("no replication slot named '{s}' — nothing to drop\n", .{name});
        return 1;
    }
    if (std.mem.eql(u8, cell(res, 0, 3), "true")) {
        out("🔴 '{s}' is ACTIVE (pid {s}): a bridge reads it. Stop that bridge first — a slot dropped under it is a fatal restart, and the WAL is freed the same either way.\n", .{ name, cell(res, 0, 4) });
        return 1;
    }
    var name_buf: [64]u8 = undefined;
    const name_z = std.fmt.bufPrintZ(&name_buf, "{s}", .{name}) catch return 1;
    const params = [_]?[*:0]const u8{name_z.ptr};
    const dropped = c.PQexecParams(conn, "SELECT pg_drop_replication_slot($1)", 1, null, &params[0], null, null, 0);
    defer c.PQclear(dropped);
    if (c.PQresultStatus(dropped) != c.PGRES_TUPLES_OK) {
        out("🔴 pg_drop_replication_slot('{s}') refused: {s}", .{ name, c.PQerrorMessage(conn) });
        return 1;
    }
    out("✅ dropped replication slot '{s}' (it retained {s} of WAL). A bridge started with --slot {s} creates a fresh one and, with ZB_FEED_RESTART on, restarts its feed — every client re-seeds from a fresh full.\n", .{ name, cell(res, 0, 8), name });
    return 0;
}

const cols_fmt = "slot_name, slot_type, COALESCE(plugin, ''), active::text, COALESCE(active_pid::text, ''), " ++
    "COALESCE(restart_lsn::text, ''), COALESCE(confirmed_flush_lsn::text, ''), COALESCE(wal_status, ''), " ++
    "COALESCE(pg_size_pretty(pg_wal_lsn_diff({s}, restart_lsn)), '')";

fn queryInventory(conn: *c.PGconn, buf: []u8, head: []const u8, slot: ?[]const u8) ?*c.PGresult {
    // The head expression fills the one `{s}` of `cols_fmt`; the WHERE takes the name as a parameter.
    var cols_buf: [512]u8 = undefined;
    const cols_s = std.fmt.bufPrint(&cols_buf, cols_fmt, .{head}) catch return null;
    const sql = std.fmt.bufPrintZ(buf, "SELECT {s} FROM pg_replication_slots{s} ORDER BY slot_name", .{ cols_s, if (slot != null) " WHERE slot_name = $1" else "" }) catch return null;
    var name_buf: [64]u8 = undefined;
    const res = if (slot) |s| blk: {
        const name_z = std.fmt.bufPrintZ(&name_buf, "{s}", .{s}) catch return null;
        const params = [_]?[*:0]const u8{name_z.ptr};
        break :blk c.PQexecParams(conn, sql.ptr, 1, null, &params[0], null, null, 0);
    } else c.PQexec(conn, sql.ptr);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        c.PQclear(res);
        return null;
    }
    return res;
}

fn cell(res: *c.PGresult, row: usize, col: usize) []const u8 {
    return std.mem.span(c.PQgetvalue(res, @intCast(row), @intCast(col)));
}
