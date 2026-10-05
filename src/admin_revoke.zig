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
//!
//! `bridge --revoke --key U… --conf nats-server.conf` (OPERATOR_SEED, ZB_ACCOUNT_PUB) revokes
//! one user key, with no database: the identities minted offline (`--mint-responder`,
//! `--mint-leaf`) were never enrolled, so PostgreSQL knows nothing of them — and a creds file
//! known to have leaked. The account JWT's revocations only ever grow: a full revocation
//! merges the map it finds with PostgreSQL's rows, it never rebuilds it from them alone.
const std = @import("std");
const c = @import("c_imports.zig").c;
const nats = @import("nats");
const jwt_mint = @import("jwt_mint.zig");

const out = std.debug.print;

pub fn run(init: *const std.process.Init) u8 {
    // ── the principal, from argv ────────────────────────────────────────────────
    var principal: ?[]const u8 = null;
    var key: ?[]const u8 = null;
    var conf_path: ?[]const u8 = null;
    var purge = false;
    {
        var it = init.minimal.args.iterate();
        _ = it.next();
        while (it.next()) |arg| {
            if (std.mem.eql(u8, arg, "--revoke")) {
                continue;
            } else if (std.mem.eql(u8, arg, "--key")) {
                key = it.next();
            } else if (std.mem.eql(u8, arg, "--conf")) {
                conf_path = it.next();
            } else if (std.mem.eql(u8, arg, "--purge")) {
                purge = true;
            } else if (principal == null and !std.mem.startsWith(u8, arg, "--")) {
                principal = arg;
            }
        }
    }
    if (key) |k| return revokeKey(init, k, conf_path);
    const p = principal orelse {
        out("🔴 usage: ADMIN_DATABASE_URL=postgres://… bridge --revoke <principal>\n" ++
            "        OPERATOR_SEED=SO… ZB_ACCOUNT_PUB=A… bridge --revoke --key U… --conf nats-server.conf\n", .{});
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
    const url_z = std.fmt.bufPrintSentinel(&url_buf, "{s}", .{url}, 0) catch {
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
    const p_z = std.fmt.bufPrintSentinel(&p_buf, "{s}", .{p}, 0) catch return 1;
    const params = [_]?[*:0]const u8{p_z.ptr};

    // ── 0. the purge request (§10kn), BEFORE the mapping delete ─────────────────
    // Each statement here commits on its own, and the running bridge publishes the ban
    // as soon as the mapping delete reaches it: the purge must already be recorded then,
    // or the ban goes out without it. A plain --revoke clears an earlier request, so the
    // latest revocation decides.
    const r0 = c.PQexecParams(conn, if (purge)
        "INSERT INTO public.zebridge_purges (principal) VALUES ($1) ON CONFLICT (principal) DO UPDATE SET requested_at = now()"
    else
        "DELETE FROM public.zebridge_purges WHERE principal = $1", 1, null, &params[0], null, null, 0);
    defer c.PQclear(r0);
    if (c.PQresultStatus(r0) != c.PGRES_COMMAND_OK) {
        out("🔴 purge request failed: {s}(is the init SQL up to date? zebridge_purges)\n", .{c.PQerrorMessage(conn)});
        return 1;
    }

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
    if (purge) out(
        \\   local data  devices are asked to DELETE their replica and identity: a connected
        \\               one at once (the ban carries it), a returning one when it renews its
        \\               JWT. Best-effort: a device that never reconnects keeps its data
        \\
    , .{});

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
    return amendRevocations(conn, init, op_seed.?, conf_path.?, null);
}

/// `--revoke --key U…`: one user key into the account JWT's revocations, no database.
fn revokeKey(init: *const std.process.Init, key: []const u8, conf_path: ?[]const u8) u8 {
    // A user key decodes (prefix U, checksum): the signature itself does not matter here.
    _ = nats.nkeys.verify(.user, key, "", [_]u8{0} ** 64) catch {
        out("🔴 {s} is not a user public key (U…, 56 characters: the `user key` a mint printed)\n", .{key});
        return 1;
    };
    const op_seed = init.minimal.environ.getPosix("OPERATOR_SEED");
    if (op_seed == null or conf_path == null) {
        out("🔴 revoking a key amends the account JWT: it needs OPERATOR_SEED, ZB_ACCOUNT_PUB and --conf\n" ++
            "     OPERATOR_SEED=SO… ZB_ACCOUNT_PUB=A… bridge --revoke --key {s} --conf /path/to/nats-server.conf\n", .{key});
        return 1;
    }
    return amendRevocations(null, init, op_seed.?, conf_path.?, key);
}

/// The account JWT's revocations: the map it carries, PostgreSQL's revoked keys (when `conn`)
/// and `extra` (one key, revoked now), merged — a key keeps its latest time — then re-signed
/// and spliced into the conf. Revocations only grow: nothing already in the JWT is dropped.
fn amendRevocations(conn: ?*c.PGconn, init: *const std.process.Init, op_seed: []const u8, conf_path: []const u8, extra: ?[]const u8) u8 {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const account_pub = init.minimal.environ.getPosix("ZB_ACCOUNT_PUB") orelse {
        out("🔴 full revocation needs ZB_ACCOUNT_PUB (the account whose JWT carries the revocations)\n", .{});
        return 1;
    };

    // ── the conf: find the account JWT and decode its claims ────────────────────
    const conf = std.Io.Dir.cwd().readFileAlloc(init.io, conf_path, a, .limited(1 << 20)) catch {
        out("🔴 cannot read {s}\n", .{conf_path});
        return 1;
    };
    var needle_buf: [128]u8 = undefined;
    const needle = std.fmt.bufPrint(&needle_buf, "{s}: ey", .{account_pub}) catch return 1;
    const at = std.mem.find(u8, conf, needle) orelse {
        out("🔴 account {s} not found in {s}'s resolver_preload\n", .{ account_pub, conf_path });
        return 1;
    };
    const jwt_start = at + needle.len - 2;
    var jwt_end = jwt_start;
    while (jwt_end < conf.len and (std.ascii.isAlphanumeric(conf[jwt_end]) or conf[jwt_end] == '.' or conf[jwt_end] == '_' or conf[jwt_end] == '-')) jwt_end += 1;
    const old_jwt = conf[jwt_start..jwt_end];

    var it = std.mem.splitScalar(u8, old_jwt, '.');
    _ = it.next();
    const claims_b64 = it.next() orelse return 1;
    const dec = std.base64.url_safe_no_pad.Decoder;
    const claims_len = dec.calcSizeForSlice(claims_b64) catch return 1;
    const claims = a.alloc(u8, claims_len) catch return 1;
    dec.decode(claims, claims_b64) catch return 1;

    // ── the complete map: the JWT's own, PostgreSQL's rows, the extra key ───────
    var revoked: std.array_hash_map.String(i64) = .empty;
    const parsed = std.json.parseFromSlice(std.json.Value, a, claims, .{}) catch {
        out("🔴 {s}'s JWT claims do not parse\n", .{account_pub});
        return 1;
    };
    if (parsed.value.object.get("nats")) |n| if (n == .object) if (n.object.get("revocations")) |r| if (r == .object) {
        var ri = r.object.iterator();
        while (ri.next()) |e| if (e.value_ptr.* == .integer) keep(a, &revoked, e.key_ptr.*, e.value_ptr.integer) catch return 1;
    };
    const from_jwt = revoked.count();
    if (conn) |pg| {
        const rres = c.PQexec(pg, "SELECT user_pubkey, floor(extract(epoch FROM revoked_at))::bigint::text FROM public.zebridge_principal_keys WHERE revoked_at IS NOT NULL ORDER BY user_pubkey");
        defer c.PQclear(rres);
        if (c.PQresultStatus(rres) != c.PGRES_TUPLES_OK) {
            out("🔴 could not read the revocation rows: {s}\n", .{c.PQerrorMessage(pg)});
            return 1;
        }
        var i: c_int = 0;
        while (i < c.PQntuples(rres)) : (i += 1) {
            const t = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(rres, i, 1)), 10) catch continue;
            const k = a.dupe(u8, std.mem.span(c.PQgetvalue(rres, i, 0))) catch return 1;
            keep(a, &revoked, k, t) catch return 1;
        }
    }
    if (extra) |k| keep(a, &revoked, k, std.Io.Clock.real.now(init.io).toSeconds()) catch return 1;
    const map = revocationsJson(a, &revoked) catch return 1;

    // ── byte-precise surgery: only the revocations map changes ──────────────────
    const rev_json = std.fmt.allocPrint(a, "\"revocations\":{{{s}}},", .{map}) catch return 1;
    var amended: []u8 = undefined;
    if (std.mem.find(u8, claims, "\"revocations\":{")) |rstart| {
        const rbody = rstart + "\"revocations\":{".len;
        const rclose = std.mem.findScalarPos(u8, claims, rbody, '}') orelse return 1;
        var after = rclose + 1;
        if (after < claims.len and claims[after] == ',') after += 1;
        amended = std.mem.concat(a, u8, &.{ claims[0..rstart], rev_json, claims[after..] }) catch return 1;
    } else {
        const t = std.mem.find(u8, claims, "\"type\":\"account\"") orelse {
            out("🔴 {s}'s JWT does not look like an account JWT\n", .{account_pub});
            return 1;
        };
        amended = std.mem.concat(a, u8, &.{ claims[0..t], rev_json, claims[t..] }) catch return 1;
    }
    // fresh jti: swap the existing value for the signer's token
    const jstart = (std.mem.find(u8, amended, "\"jti\":\"") orelse return 1) + "\"jti\":\"".len;
    const jend = std.mem.findScalarPos(u8, amended, jstart, '"') orelse return 1;
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

    if (extra) |k| out("✅ key {s} revoked in the account JWT ({s})\n", .{ k, account_pub });
    out(
        \\✅ FULL revocation written: {d} revoked key(s) in the account JWT ({d} it already held)
        \\   {s} amended in place. Now reload the server and the live session is kicked
        \\   with "Authentication Revoked":
        \\     nats-server --signal reload
        \\   A leaf node checks the devices against its own copy: refresh its trust.conf too
        \\   (deploy/ansible/leaf.yml does it on every run).
        \\
    , .{ revoked.count(), from_jwt, conf_path });
    return 0;
}

/// A revoked key keeps its latest time: a later revocation covers every JWT issued before it.
fn keep(a: std.mem.Allocator, m: *std.array_hash_map.String(i64), k: []const u8, t: i64) !void {
    const gop = try m.getOrPut(a, k);
    if (!gop.found_existing or gop.value_ptr.* < t) gop.value_ptr.* = t;
}

/// The map as JSON members, keys sorted: the same revocations always sign the same claims.
fn revocationsJson(a: std.mem.Allocator, m: *std.array_hash_map.String(i64)) ![]u8 {
    const keys = try a.dupe([]const u8, m.keys());
    std.mem.sort([]const u8, keys, {}, struct {
        fn lt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lt);
    var out_buf: std.ArrayList(u8) = .empty;
    for (keys, 0..) |k, i| {
        if (i > 0) try out_buf.append(a, ',');
        try out_buf.print(a, "\"{s}\":{d}", .{ k, m.get(k).? });
    }
    return out_buf.toOwnedSlice(a);
}

test "revocations merge: a key keeps its latest time, the order is stable" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var m: std.array_hash_map.String(i64) = .empty;
    try keep(a, &m, "UB", 100); // from the JWT
    try keep(a, &m, "UA", 50);
    try keep(a, &m, "UB", 90); // an older row from PostgreSQL does not move it back
    try keep(a, &m, "UA", 70); // a later one does
    try std.testing.expectEqualStrings("\"UA\":70,\"UB\":100", try revocationsJson(a, &m));
}
