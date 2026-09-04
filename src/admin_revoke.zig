//! `bridge --revoke <principal>`: revocation without hunting for the right psql (§10ck).
//!
//! Review's case: the database may be remote (RDS behind a VPC, IAM auth, the AWS
//! psql dance) and revocation happens at the worst possible moment — it should be one
//! command on a box that already has the bridge binary. The privilege posture is
//! deliberate: ADMIN_DATABASE_URL is passed FOR THE INVOCATION, never stored in
//! .env.bridge — the capability is non-ambient, a machine holding the bridge's env
//! still cannot revoke anyone.
//!
//! Two deletes, because raw-psql revocations forget the second one:
//!   - the mapping (zebridge_user_tenants) — closes writes NOW and resolution at the
//!     next connect ($KV.tenants purge rides the WAL through the running bridge);
//!   - the UNUSED invites — an unredeemed invite is a re-enrollment ticket in the
//!     drawer: one GET and the principal is back with a fresh JWT.
//!
//! No NATS access, same doctrine as the sweeper: the CLI is a pure PostgreSQL client;
//! the running bridge performs the KV purge when the delete rides the publication.
const std = @import("std");
const c = @import("c_imports.zig").c;
const nats = @import("nats");
const jwt_mint = @import("jwt_mint.zig");

const out = std.debug.print;

pub fn run(init: *const std.process.Init) u8 {
    // ── the principal, from argv ────────────────────────────────────────────────
    var principal: ?[]const u8 = null;
    var conf_path: ?[]const u8 = null;
    {
        var it = init.minimal.args.iterate();
        _ = it.next();
        while (it.next()) |arg| {
            if (std.mem.eql(u8, arg, "--revoke")) {
                principal = it.next();
            } else if (std.mem.eql(u8, arg, "--conf")) {
                conf_path = it.next();
            }
        }
    }
    const p = principal orelse {
        out("🔴 usage: ADMIN_DATABASE_URL=postgres://… bridge --revoke <principal>\n", .{});
        return 1;
    };
    if (p.len == 0 or p.len > 256) {
        out("🔴 principal must be 1–256 characters\n", .{});
        return 1;
    }

    // ── the admin connection, for this invocation only ──────────────────────────
    const url = init.minimal.environ.getPosix("ADMIN_DATABASE_URL") orelse {
        out("🔴 ADMIN_DATABASE_URL is not set. Deliberate: revocation is an admin act and\n" ++
            "   the capability must not live in the bridge's standing env — pass the URL\n" ++
            "   for this invocation only:\n" ++
            "     ADMIN_DATABASE_URL=postgres://… bridge --revoke {s}\n", .{p});
        return 1;
    };
    var url_buf: [1024]u8 = undefined;
    const url_z = std.fmt.bufPrintZ(&url_buf, "{s}", .{url}) catch {
        out("🔴 ADMIN_DATABASE_URL too long\n", .{});
        return 1;
    };
    const conn = c.PQconnectdb(url_z.ptr);
    defer if (conn != null) c.PQfinish(conn);
    if (conn == null or c.PQstatus(conn) != c.CONNECTION_OK) {
        if (conn != null) {
            out("🔴 could not connect with ADMIN_DATABASE_URL: {s}\n", .{c.PQerrorMessage(conn)});
        } else {
            out("🔴 could not connect with ADMIN_DATABASE_URL: PQconnectdb returned null\n", .{});
        }
        return 1;
    }

    var p_buf: [300]u8 = undefined;
    const p_z = std.fmt.bufPrintZ(&p_buf, "{s}", .{p}) catch return 1;
    const params = [_]?[*:0]const u8{p_z.ptr};

    // ── 1. the mapping ──────────────────────────────────────────────────────────
    const r1 = c.PQexecParams(conn, "DELETE FROM public.zebridge_user_tenants WHERE principal = $1", 1, null, &params[0], null, null, 0);
    defer c.PQclear(r1);
    if (c.PQresultStatus(r1) != c.PGRES_COMMAND_OK) {
        out("🔴 mapping delete failed: {s}\n", .{c.PQerrorMessage(conn)});
        return 1;
    }
    const mappings = std.fmt.parseInt(usize, std.mem.span(c.PQcmdTuples(r1)), 10) catch 0;

    // ── 2. the way back in ──────────────────────────────────────────────────────
    const r2 = c.PQexecParams(conn, "DELETE FROM public.zebridge_invites WHERE principal = $1 AND used_at IS NULL", 1, null, &params[0], null, null, 0);
    defer c.PQclear(r2);
    if (c.PQresultStatus(r2) != c.PGRES_COMMAND_OK) {
        out("🔴 invite void failed: {s}\n", .{c.PQerrorMessage(conn)});
        return 1;
    }
    const invites = std.fmt.parseInt(usize, std.mem.span(c.PQcmdTuples(r2)), 10) catch 0;

    // ── 3. stamp the enrolled keys — the intent, recorded (§10cm) ───────────────
    // Even a partial revocation records WHICH keys are dead; the full map in the
    // account JWT is re-derived from these rows, so later revocations compose.
    const r3 = c.PQexecParams(conn, "UPDATE public.zebridge_principal_keys SET revoked_at = now() WHERE principal = $1 AND revoked_at IS NULL", 1, null, &params[0], null, null, 0);
    defer c.PQclear(r3);
    const keys_stamped: usize = if (c.PQresultStatus(r3) == c.PGRES_COMMAND_OK)
        std.fmt.parseInt(usize, std.mem.span(c.PQcmdTuples(r3)), 10) catch 0
    else
        0;

    const op_seed = init.minimal.environ.getPosix("OPERATOR_SEED");
    const full_mode = op_seed != null and conf_path != null;
    if (mappings == 0 and invites == 0 and keys_stamped == 0 and !full_mode) {
        // With full-mode credentials we DO continue: a second, escalating invocation
        // ("--revoke p" then "--revoke p --conf … + seed") finds the DB side already
        // clean — the account JWT amendment is exactly what it still has to do.
        out("ⓘ  '{s}': no mapping and no unused invite — nothing to revoke (unknown principal, or already revoked)\n", .{p});
        return 1;
    }

    // ── the three clocks, narrated where the operator is looking ────────────────
    out(
        \\✅ revoked '{s}': {d} mapping(s) removed, {d} unused invite(s) voided, {d} key(s) marked
        \\   writes      refused as of NOW — the guard and RLS read the table live, per mutation
        \\   resolution  gone at the NEXT CONNECT — the $KV.tenants purge rides this delete's WAL
        \\               through the running bridge (no bridge running: it lands when one next runs)
        \\   reads       continue until the JWT EXPIRES — grants are baked into the token and NATS
        \\               authorizes from the signature alone. A FULL revocation cuts them NOW:
        \\               it amends the account JWT's revocations map (see below)
        \\
    , .{ p, mappings, invites, keys_stamped });

    // ── 4. FULL revocation, when the credentials allow it (§10cm) ───────────────
    // Partial is never a choice, only a fallback: with OPERATOR_SEED and --conf the
    // command escalates automatically — the account JWT's `revocations` map is
    // rebuilt from PG (every stamped key) and re-signed, and the server, once
    // reloaded, kicks the live session with "Authentication Revoked".
    if (!full_mode) {
        out(
            \\ⓘ  reads continue until the JWT expires. To CUT them now (full revocation):
            \\     OPERATOR_SEED=SO… ADMIN_DATABASE_URL=… \\
            \\       bridge --revoke {s} --conf /path/to/nats-server.conf
            \\   (amends the account JWT's revocations map and tells you how to reload)
            \\
        , .{p});
        return 0;
    }
    return fullRevoke(conn, init, op_seed.?, conf_path.?);
}

fn fullRevoke(conn: ?*c.PGconn, init: *const std.process.Init, op_seed: []const u8, conf_path: []const u8) u8 {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const account_pub = init.minimal.environ.getPosix("ZB_ACCOUNT_PUB") orelse {
        out("🔴 full revocation needs ZB_ACCOUNT_PUB (the account whose JWT carries the revocations)\n", .{});
        return 1;
    };

    // ── the complete map, from PG — the source of truth ─────────────────────────
    const rres = c.PQexec(conn, "SELECT user_pubkey, floor(extract(epoch FROM revoked_at))::bigint::text FROM public.zebridge_principal_keys WHERE revoked_at IS NOT NULL ORDER BY user_pubkey");
    defer c.PQclear(rres);
    if (c.PQresultStatus(rres) != c.PGRES_TUPLES_OK) {
        out("🔴 could not read the revocation rows: {s}\n", .{c.PQerrorMessage(conn)});
        return 1;
    }
    var map: std.ArrayList(u8) = .empty;
    var i: c_int = 0;
    while (i < c.PQntuples(rres)) : (i += 1) {
        if (i > 0) map.append(a, ',') catch return 1;
        map.print(a, "\"{s}\":{s}", .{ std.mem.span(c.PQgetvalue(rres, i, 0)), std.mem.span(c.PQgetvalue(rres, i, 1)) }) catch return 1;
    }

    // ── the conf: find the account JWT, amend its claims, re-sign, splice ───────
    const conf = std.Io.Dir.cwd().readFileAlloc(init.io, conf_path, a, .limited(1 << 20)) catch {
        out("🔴 cannot read {s}\n", .{conf_path});
        return 1;
    };
    var needle_buf: [128]u8 = undefined;
    const needle = std.fmt.bufPrint(&needle_buf, "{s}: ey", .{account_pub}) catch return 1;
    const at = std.mem.indexOf(u8, conf, needle) orelse {
        out("🔴 account {s} not found in {s}'s resolver_preload\n", .{ account_pub, conf_path });
        return 1;
    };
    const jwt_start = at + needle.len - 2;
    var jwt_end = jwt_start;
    while (jwt_end < conf.len and (std.ascii.isAlphanumeric(conf[jwt_end]) or conf[jwt_end] == '.' or conf[jwt_end] == '_' or conf[jwt_end] == '-')) jwt_end += 1;
    const old_jwt = conf[jwt_start..jwt_end];

    // decode the claims segment
    var it = std.mem.splitScalar(u8, old_jwt, '.');
    _ = it.next();
    const claims_b64 = it.next() orelse return 1;
    const dec = std.base64.url_safe_no_pad.Decoder;
    const claims_len = dec.calcSizeForSlice(claims_b64) catch return 1;
    const claims = a.alloc(u8, claims_len) catch return 1;
    dec.decode(claims, claims_b64) catch return 1;

    // ── byte-precise surgery: only the revocations map changes ──────────────────
    const rev_json = std.fmt.allocPrint(a, "\"revocations\":{{{s}}},", .{map.items}) catch return 1;
    var amended: []u8 = undefined;
    if (std.mem.indexOf(u8, claims, "\"revocations\":{")) |rstart| {
        const rbody = rstart + "\"revocations\":{".len;
        const rclose = std.mem.indexOfScalarPos(u8, claims, rbody, '}') orelse return 1;
        var after = rclose + 1;
        if (after < claims.len and claims[after] == ',') after += 1;
        amended = std.mem.concat(a, u8, &.{ claims[0..rstart], rev_json, claims[after..] }) catch return 1;
    } else {
        const t = std.mem.indexOf(u8, claims, "\"type\":\"account\"") orelse {
            out("🔴 {s}'s JWT does not look like an account JWT\n", .{account_pub});
            return 1;
        };
        amended = std.mem.concat(a, u8, &.{ claims[0..t], rev_json, claims[t..] }) catch return 1;
    }
    // fresh jti: swap the existing value for the signer's token
    const jstart = (std.mem.indexOf(u8, amended, "\"jti\":\"") orelse return 1) + "\"jti\":\"".len;
    const jend = std.mem.indexOfScalarPos(u8, amended, jstart, '"') orelse return 1;
    const pre = std.mem.concat(a, u8, &.{ amended[0..jstart], "__JTI__", amended[jend..] }) catch return 1;

    var op_kp = nats.nkeys.SeedKeyPair.fromSeed(op_seed) catch {
        out("🔴 OPERATOR_SEED is not a valid seed\n", .{});
        return 1;
    };
    defer op_kp.wipe();
    const new_jwt = jwt_mint.signClaims(a, &op_kp, pre, "__JTI__") catch return 1;

    const new_conf = std.mem.concat(a, u8, &.{ conf[0..jwt_start], new_jwt, conf[jwt_end..] }) catch return 1;
    var f = std.Io.Dir.cwd().createFile(init.io, conf_path, .{}) catch return 1;
    defer f.close(init.io);
    f.writeStreamingAll(init.io, new_conf) catch return 1;

    out(
        \\✅ FULL revocation written: {d} revoked key(s) in the account JWT ({s})
        \\   {s} amended in place. Now reload the server and the live session is kicked
        \\   with "Authentication Revoked":
        \\     kill -HUP $(pgrep -x nats-server)
        \\
    , .{ @as(usize, @intCast(c.PQntuples(rres))), account_pub, conf_path });
    return 0;
}
