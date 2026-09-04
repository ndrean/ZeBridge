//! `bridge --init-nats`: the whole NATS identity stack, generated — no nsc (§10ch).
//!
//! The friction it removes: to run ZeBridge under operator mode you need an operator,
//! an account carrying two SCOPED signing keys (the client template with
//! `{{tag(tenant)}}` / `{{name()}}` substitutions, and the service key), a bridge user
//! under the service scope, creds files, and a server conf with the resolver preload.
//! That was `nsc add operator` / `nsc add account` / a 90-line bootstrap script — a
//! wall for anyone who just wants to try the bridge.
//!
//! Two modes:
//!   --mode dev       10 seconds flat: an OPEN server conf (JetStream on, no auth at
//!                    all) plus a matching .env.bridge. No JWT in sight. Enrollment
//!                    stays off (no ZB_SIGNING_SEED) — the bridge already treats that
//!                    as "endpoint dark". Loudly marked dev-only.
//!   --mode operator  the full stack, self-contained: every key generated here, the
//!                    operator and account JWTs minted by `jwt_mint.signClaims`, the
//!                    client template's subject list derived FROM THE TOPOLOGY —
//!                    grammar renames propagate instead of drifting from a shell
//!                    script — and .env.bridge ready for enrollment (`ZB_SIGNING_SEED`
//!                    is the scoped client key, exactly what /enroll mints with).
//!
//! ⚠️ Never overwrites: an existing target file aborts the run (--force to override).
//! Seeds are credentials; clobbering them silently would orphan a running system.
const std = @import("std");
const nats = @import("nats");
const jwt_mint = @import("jwt_mint.zig");
const topology_mod = @import("topology.zig");
const c = @import("c_imports.zig").c;

const out = std.debug.print;

const Mode = enum { dev, operator };

const KeyPair = struct {
    seed_buf: [nats.nkeys.seed_text_len]u8 = undefined,
    pub_buf: [nats.nkeys.public_key_text_len]u8 = undefined,
    seed_len: usize = 0,
    pub_len: usize = 0,

    fn seed(self: *const KeyPair) []const u8 {
        return self.seed_buf[0..self.seed_len];
    }
    fn public(self: *const KeyPair) []const u8 {
        return self.pub_buf[0..self.pub_len];
    }
};

/// Generate one nkey pair of the given type — the `--gen-nkey` recipe, typed.
fn genKey(io: std.Io, key_type: nats.nkeys.KeyType) !KeyPair {
    var kp = KeyPair{};
    const ed = std.crypto.sign.Ed25519.KeyPair.generate(io);
    const s = nats.nkeys.encodeSeed(key_type, &ed.secret_key.seed(), &kp.seed_buf);
    kp.seed_len = s.len;
    // Round-tripped, like --gen-nkey: derive the public text from the seed TEXT about
    // to be written, so an unreadable seed never leaves this function.
    var skp = try nats.nkeys.SeedKeyPair.fromSeed(kp.seed());
    defer skp.wipe();
    const p = skp.publicKeyText(&kp.pub_buf);
    kp.pub_len = p.len;
    return kp;
}

fn writeFile(io: std.Io, path: []const u8, bytes: []const u8, force: bool) !void {
    if (!force) {
        if (std.Io.Dir.cwd().openFile(io, path, .{})) |f| {
            var fv = f;
            fv.close(io);
            out("🔴 refusing to overwrite {s} (pass --force to allow) — seeds are credentials\n", .{path});
            return error.WouldOverwrite;
        } else |_| {}
    }
    var f = try std.Io.Dir.cwd().createFile(io, path, .{});
    defer f.close(io);
    try f.writeStreamingAll(io, bytes);
}

/// The scoped CLIENT template's allow lists, from the topology — the single grant
/// block every principal inherits (jwt-bootstrap.sh's list, ported; see PROTOCOL §7.4b
/// for why mutation verdicts are DIRECT.GET and never a consumer).
fn clientAllows(a: std.mem.Allocator, topo: *const topology_mod.Topology) !struct { pub_json: []u8, sub_json: []u8 } {
    const cdc_pre = topo.cdc_stream_prefix; // "CDC_"
    const cdc_pub = topo.cdc_stream_public; // "CDC_PUBLIC"
    const kv_schemas = topo.kv_schemas;
    const kv_gens = topo.kv_generations;
    const kv_tenants = topo.kv_tenants;
    const obj_pre = topo.generation_bucket_prefix; // "gen-"
    const open = topo.open_tenant; // "_default"
    const subj_cdc = topo.subject_cdc_prefix; // "cdc"
    const subj_mut = topo.subject_mutations_prefix; // "mutation"
    const subj_ack = topo.mutation_ack_prefix; // "mutation_ack"

    var pubs: std.ArrayList([]u8) = .empty;
    var subs: std.ArrayList([]u8) = .empty;
    const P = struct {
        fn add(list: *std.ArrayList([]u8), alloc: std.mem.Allocator, comptime fmt: []const u8, args: anytype) !void {
            try list.append(alloc, try std.fmt.allocPrint(alloc, fmt, args));
        }
    };

    try P.add(&pubs, a, "{s}.{{{{name()}}}}.>", .{subj_mut});
    try P.add(&pubs, a, "$JS.API.INFO", .{});
    inline for (.{ "CONSUMER.CREATE", "CONSUMER.INFO", "CONSUMER.MSG.NEXT" }) |op| {
        // tenant stream, public stream, the two KV backers — CREATE also bare (no filter)
        if (std.mem.eql(u8, op, "CONSUMER.CREATE")) {
            try P.add(&pubs, a, "$JS.API.{s}.{s}{{{{tag(tenant)}}}}", .{ op, cdc_pre });
            try P.add(&pubs, a, "$JS.API.{s}.{s}", .{ op, cdc_pub });
            try P.add(&pubs, a, "$JS.API.{s}.KV_{s}", .{ op, kv_schemas });
            try P.add(&pubs, a, "$JS.API.{s}.KV_{s}", .{ op, kv_gens });
            try P.add(&pubs, a, "$JS.API.{s}.OBJ_{s}{{{{tag(tenant)}}}}", .{ op, obj_pre });
            try P.add(&pubs, a, "$JS.API.{s}.OBJ_{s}{s}", .{ op, obj_pre, open });
        }
        try P.add(&pubs, a, "$JS.API.{s}.{s}{{{{tag(tenant)}}}}.>", .{ op, cdc_pre });
        try P.add(&pubs, a, "$JS.API.{s}.{s}.>", .{ op, cdc_pub });
        try P.add(&pubs, a, "$JS.API.{s}.KV_{s}.>", .{ op, kv_schemas });
        try P.add(&pubs, a, "$JS.API.{s}.KV_{s}.>", .{ op, kv_gens });
        try P.add(&pubs, a, "$JS.API.{s}.OBJ_{s}{{{{tag(tenant)}}}}.>", .{ op, obj_pre });
        try P.add(&pubs, a, "$JS.API.{s}.OBJ_{s}{s}.>", .{ op, obj_pre, open });
    }
    try P.add(&pubs, a, "$JS.API.STREAM.INFO.{s}{{{{tag(tenant)}}}}", .{cdc_pre});
    try P.add(&pubs, a, "$JS.API.STREAM.INFO.{s}", .{cdc_pub});
    try P.add(&pubs, a, "$JS.API.STREAM.INFO.KV_{s}", .{kv_schemas});
    try P.add(&pubs, a, "$JS.API.STREAM.INFO.KV_{s}", .{kv_gens});
    try P.add(&pubs, a, "$JS.API.STREAM.INFO.KV_{s}", .{kv_tenants});
    try P.add(&pubs, a, "$JS.API.STREAM.INFO.OBJ_{s}{{{{tag(tenant)}}}}", .{obj_pre});
    try P.add(&pubs, a, "$JS.API.STREAM.INFO.OBJ_{s}{s}", .{ obj_pre, open });
    try P.add(&pubs, a, "$JS.API.STREAM.MSG.GET.KV_{s}", .{kv_schemas});
    try P.add(&pubs, a, "$JS.API.STREAM.MSG.GET.OBJ_{s}{{{{tag(tenant)}}}}", .{obj_pre});
    try P.add(&pubs, a, "$JS.API.STREAM.MSG.GET.OBJ_{s}{s}", .{ obj_pre, open });
    try P.add(&pubs, a, "$JS.API.DIRECT.GET.KV_{s}.>", .{kv_schemas});
    try P.add(&pubs, a, "$JS.API.DIRECT.GET.KV_{s}.$KV.{s}.{{{{tag(tenant)}}}}.>", .{ kv_gens, kv_gens });
    try P.add(&pubs, a, "$JS.API.DIRECT.GET.KV_{s}.$KV.{s}.{s}.>", .{ kv_gens, kv_gens, open });
    try P.add(&pubs, a, "$JS.API.DIRECT.GET.KV_{s}.$KV.{s}.{{{{name()}}}}", .{ kv_tenants, kv_tenants });
    try P.add(&pubs, a, "$JS.API.DIRECT.GET.OBJ_{s}{{{{tag(tenant)}}}}.>", .{obj_pre});
    try P.add(&pubs, a, "$JS.API.DIRECT.GET.OBJ_{s}{s}.>", .{ obj_pre, open });
    try P.add(&pubs, a, "$JS.API.DIRECT.GET.MUTATIONS.{s}.{{{{name()}}}}.>", .{subj_ack});
    try P.add(&pubs, a, "$JS.ACK.>", .{});

    try P.add(&subs, a, "{s}.{{{{name()}}}}.>", .{subj_ack});
    try P.add(&subs, a, "{s}.{{{{tag(tenant)}}}}.>", .{subj_cdc});
    try P.add(&subs, a, "{s}.{s}.>", .{ subj_cdc, open });
    try P.add(&subs, a, "$KV.{s}.>", .{kv_schemas});
    try P.add(&subs, a, "$KV.{s}.>", .{kv_gens});
    try P.add(&subs, a, "_INBOX.>", .{});

    return .{ .pub_json = try joinJson(a, pubs.items), .sub_json = try joinJson(a, subs.items) };
}

fn joinJson(a: std.mem.Allocator, items: [][]u8) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    for (items, 0..) |it, i| {
        if (i > 0) try buf.append(a, ',');
        try buf.append(a, '"');
        try buf.appendSlice(a, it);
        try buf.append(a, '"');
    }
    return buf.toOwnedSlice(a);
}

fn credsFile(a: std.mem.Allocator, jwt: []const u8, seed: []const u8) ![]u8 {
    return std.fmt.allocPrint(a,
        \\-----BEGIN NATS USER JWT-----
        \\{s}
        \\------END NATS USER JWT------
        \\
        \\************************* IMPORTANT *************************
        \\NKEY Seed printed below can be used to sign and prove identity.
        \\NKEYs are sensitive and should be treated as secrets.
        \\
        \\-----BEGIN USER NKEY SEED-----
        \\{s}
        \\------END USER NKEY SEED------
        \\
        \\*************************************************************
        \\
    , .{ jwt, seed });
}

/// Entry point. Returns the process exit code; prints everything it does.
pub fn run(io: std.Io, init: *const std.process.Init) u8 {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // ── self-contained by design (review: "no needs for these args") — the mode
    //    word and --force are the whole surface. Ports, directory and topology are
    //    the well-known defaults; a deployment that needs different ones edits the
    //    two generated files, which are plain text and theirs.
    var mode: Mode = .dev;
    var force = false;
    {
        var it = init.minimal.args.iterate();
        _ = it.next();
        while (it.next()) |arg| {
            if (std.mem.eql(u8, arg, "--init-nats")) continue;
            if (std.mem.eql(u8, arg, "--force")) {
                force = true;
            } else if (std.meta.stringToEnum(Mode, arg)) |m| {
                mode = m;
            } else return usageErr("unknown argument");
        }
    }
    const dir: []const u8 = "zb-nats";
    const nats_port: u32 = 4222;
    const ws_port: u32 = 8080;
    const http_port: u32 = 8222;

    std.Io.Dir.cwd().createDirPath(io, dir) catch |err| {
        out("🔴 could not create {s}: {}\n", .{ dir, err });
        return 1;
    };
    // store_dir must be ABSOLUTE — a relative path resolves against nats-server's CWD
    // at launch, which silently reuses stale JetStream state from wherever the server
    // happened to be started (the up.sh lesson, kept). realPath resolves the just-made
    // directory to its absolute form.
    const dir_abs = std.Io.Dir.cwd().realPathFileAlloc(io, dir, a) catch return 1;

    return switch (mode) {
        .dev => runDev(a, io, dir, dir_abs, force, nats_port, ws_port, http_port),
        .operator => runOperator(a, io, dir, dir_abs, force, nats_port, ws_port, http_port),
    };
}

fn usageErr(msg: []const u8) u8 {
    out("🔴 {s}\n  bridge --init-nats [dev|operator] [--force]\n", .{msg});
    return 1;
}

fn runDev(a: std.mem.Allocator, io: std.Io, dir: []const u8, dir_abs: []const u8, force: bool, nats_port: u32, ws_port: u32, http_port: u32) u8 {
    // The nkey is generated even though the open server ignores it: the SAME
    // .env.bridge then survives the upgrade to operator mode with only the conf
    // swapped — the seed is already in place for the server to authorize.
    const bridge_kp = genKey(io, .user) catch return 1;

    const conf = std.fmt.allocPrint(a,
        \\# Generated by `bridge --init-nats --mode dev` — DEV ONLY.
        \\#
        \\# ⚠️ This server is OPEN: no authorization block, anyone who can reach the port
        \\# owns every stream. That is the point — the whole stack in ten seconds, not a
        \\# JWT in sight — and exactly why this file must never reach production. The
        \\# production shape is `--init-nats --mode operator`.
        \\port: {d}
        \\http_port: {d}
        \\websocket {{
        \\  port: {d}
        \\  no_tls: true
        \\}}
        \\jetstream {{
        \\  store_dir: "{s}/nats-data"
        \\}}
        \\
    , .{ nats_port, http_port, ws_port, dir_abs }) catch return 1;

    const env = std.fmt.allocPrint(a,
        \\# Generated by `bridge --init-nats --mode dev` — DEV ONLY (open NATS, no JWT).
        \\DATABASE_READER_URL=postgres://bridge_reader:reader_password_changeme@127.0.0.1:5432/postgres
        \\DATABASE_WRITER_URL=postgres://bridge_writer:writer_password_changeme@127.0.0.1:5432/postgres
        \\NATS_URL=nats://127.0.0.1:{d}
        \\BRIDGE_CDC_SLOT=zb_slot
        \\BRIDGE_CDC_PUBLICATION=zb_pub
        \\BRIDGE_PORT=9090
        \\LOG_LEVEL=info
        \\
        \\# The bridge's nkey identity — unused by the OPEN dev server, but generated now
        \\# so the upgrade to operator mode is a conf swap, not a credential migration.
        \\NATS_BRIDGE_NKEY_PUB={s}
        \\NATS_BRIDGE_NKEY_SEED={s}
        \\
        \\# ZB_SIGNING_SEED deliberately absent: enrollment (/enroll) stays dark in dev,
        \\# which is the bridge's documented behaviour for a missing seed. Operator mode
        \\# fills it in.
        \\
    , .{ nats_port, bridge_kp.public(), bridge_kp.seed() }) catch return 1;

    const conf_path = std.fs.path.join(a, &.{ dir, "nats-server.conf" }) catch return 1;
    const env_path = std.fs.path.join(a, &.{ dir, ".env.bridge" }) catch return 1;
    writeFile(io, conf_path, conf, force) catch return 1;
    writeFile(io, env_path, env, force) catch return 1;

    out(
        \\✅ dev stack generated (OPEN server — dev only):
        \\   {s}/nats-server.conf     nats-server -c {s}/nats-server.conf
        \\   {s}/.env.bridge          set -a; . {s}/.env.bridge; set +a; ./bridge
        \\   Edit the two postgres URLs, run init.sql, and you are live. No JWT anywhere.
        \\
    , .{ dir, dir, dir, dir });
    return 0;
}

fn runOperator(a: std.mem.Allocator, io: std.Io, dir: []const u8, dir_abs: []const u8, force: bool, nats_port: u32, ws_port: u32, http_port: u32) u8 {
    // ── the topology: BUILT IN (§10ci) — the grants and the running bridge mint
    //    from the same embedded grammar, so they cannot disagree, and this command
    //    needs no file present anywhere.
    const owned = topology_mod.loadEmbedded(a) catch return 1;
    const topo: topology_mod.Topology = owned.topology;

    // ── every key, generated here ───────────────────────────────────────────────
    const op_kp = genKey(io, .operator) catch return 1;
    const sys_kp = genKey(io, .account) catch return 1;
    const acct_kp = genKey(io, .account) catch return 1;
    const sk_client = genKey(io, .account) catch return 1; // signing keys are account-type
    const sk_service = genKey(io, .account) catch return 1;
    const bridge_user = genKey(io, .user) catch return 1;

    var op_seed_kp = nats.nkeys.SeedKeyPair.fromSeed(op_kp.seed()) catch return 1;
    defer op_seed_kp.wipe();

    const now: i64 = @intCast(c.time(null));

    // ── operator JWT: self-signed, naming the system account. Not optional: the
    //    first live boot of a generated conf died with "Can't start JetStream: …
    //    system account not setup" — JetStream's internal subscriptions live on the
    //    system account, so operator mode without one is a server that cannot start.
    const op_claims = std.fmt.allocPrint(a,
        "{{\"jti\":\"__JTI__\",\"iat\":{d},\"iss\":\"{s}\",\"name\":\"ZeBridgeOp\",\"sub\":\"{s}\"," ++
            "\"nats\":{{\"system_account\":\"{s}\",\"type\":\"operator\",\"version\":2}}}}",
        .{ now, op_kp.public(), op_kp.public(), sys_kp.public() }) catch return 1;
    const op_jwt = jwt_mint.signClaims(a, &op_seed_kp, op_claims, "__JTI__") catch return 1;

    // ── the SYS account: minimal, and deliberately WITHOUT JetStream limits — the
    //    system account may not use JetStream, and nothing ever connects to it here.
    const sys_claims = std.fmt.allocPrint(a,
        "{{\"jti\":\"__JTI__\",\"iat\":{d},\"iss\":\"{s}\",\"name\":\"SYS\",\"sub\":\"{s}\",\"nats\":{{" ++
            "\"limits\":{{\"subs\":-1,\"data\":-1,\"payload\":-1,\"imports\":-1,\"exports\":-1,\"wildcards\":true,\"conn\":-1,\"leaf\":-1}}," ++
            "\"default_permissions\":{{\"pub\":{{}},\"sub\":{{}}}},\"authorization\":{{}},\"type\":\"account\",\"version\":2}}}}",
        .{ now, op_kp.public(), sys_kp.public() }) catch return 1;
    const sys_jwt = jwt_mint.signClaims(a, &op_seed_kp, sys_claims, "__JTI__") catch return 1;

    // ── account JWT: JetStream unlimited + the two SCOPED signing keys ──────────
    const allows = clientAllows(a, &topo) catch return 1;
    const acct_claims = std.fmt.allocPrint(a,
        "{{\"jti\":\"__JTI__\",\"iat\":{d},\"iss\":\"{s}\",\"name\":\"ZEBRIDGE\",\"sub\":\"{s}\",\"nats\":{{" ++
            "\"limits\":{{\"subs\":-1,\"data\":-1,\"payload\":-1,\"imports\":-1,\"exports\":-1,\"wildcards\":true," ++
            "\"conn\":-1,\"leaf\":-1,\"mem_storage\":-1,\"disk_storage\":-1,\"streams\":-1,\"consumer\":-1," ++
            "\"max_ack_pending\":-1,\"mem_max_stream_bytes\":-1,\"disk_max_stream_bytes\":-1}}," ++
            "\"signing_keys\":[" ++
            "{{\"kind\":\"user_scope\",\"key\":\"{s}\",\"role\":\"client\",\"template\":{{" ++
            "\"pub\":{{\"allow\":[{s}]}},\"sub\":{{\"allow\":[{s}]}},\"subs\":-1,\"data\":-1,\"payload\":-1}},\"description\":\"\"}}," ++
            "{{\"kind\":\"user_scope\",\"key\":\"{s}\",\"role\":\"service\",\"template\":{{" ++
            "\"pub\":{{\"allow\":[\"\\u003e\"]}},\"sub\":{{\"allow\":[\"\\u003e\"]}},\"subs\":-1,\"data\":-1,\"payload\":-1}},\"description\":\"\"}}]," ++
            "\"default_permissions\":{{\"pub\":{{}},\"sub\":{{}}}},\"authorization\":{{}},\"type\":\"account\",\"version\":2}}}}",
        .{ now, op_kp.public(), acct_kp.public(), sk_client.public(), allows.pub_json, allows.sub_json, sk_service.public() }) catch return 1;
    const acct_jwt = jwt_mint.signClaims(a, &op_seed_kp, acct_claims, "__JTI__") catch return 1;

    // ── the bridge user: a scoped user under the SERVICE key ────────────────────
    // Ten years: the bridge's own credential rotates with a redeploy, not a TTL.
    const bridge_jwt = jwt_mint.mint(a, sk_service.seed(), acct_kp.public(), "bridge", "service", bridge_user.public(), 10 * 365 * 24 * 3600, now) catch |err| {
        out("🔴 bridge user mint failed: {}\n", .{err});
        return 1;
    };
    const bridge_creds = credsFile(a, bridge_jwt, bridge_user.seed()) catch return 1;

    // ── files ───────────────────────────────────────────────────────────────────
    const conf = std.fmt.allocPrint(a,
        \\# Generated by `bridge --init-nats --mode operator` — self-contained, no nsc.
        \\# Operator "ZeBridgeOp"
        \\operator: {s}
        \\
        \\system_account: {s}
        \\
        \\resolver: MEMORY
        \\
        \\resolver_preload: {{
        \\  // Account "SYS"
        \\  {s}: {s}
        \\  // Account "ZEBRIDGE"
        \\  {s}: {s}
        \\}}
        \\
        \\port: {d}
        \\http_port: {d}
        \\websocket {{
        \\  port: {d}
        \\  no_tls: true
        \\}}
        \\jetstream {{
        \\  store_dir: "{s}/nats-data"
        \\}}
        \\
    , .{ op_jwt, sys_kp.public(), sys_kp.public(), sys_jwt, acct_kp.public(), acct_jwt, nats_port, http_port, ws_port, dir_abs }) catch return 1;

    const env = std.fmt.allocPrint(a,
        \\# Generated by `bridge --init-nats --mode operator` — the full JWT stack.
        \\DATABASE_READER_URL=postgres://bridge_reader:reader_password_changeme@127.0.0.1:5432/postgres
        \\DATABASE_WRITER_URL=postgres://bridge_writer:writer_password_changeme@127.0.0.1:5432/postgres
        \\NATS_URL=nats://127.0.0.1:{d}
        \\NATS_CREDS={s}/creds/bridge.creds
        \\BRIDGE_CDC_SLOT=zb_slot
        \\BRIDGE_CDC_PUBLICATION=zb_pub
        \\BRIDGE_PORT=9090
        \\LOG_LEVEL=info
        \\
        \\# Enrollment: the bridge as online signer (GET /enroll). The seed below is the
        \\# account's SCOPED client signing key — it can mint client-role users and
        \\# nothing else; the permission template lives in the account JWT, not here.
        \\ZB_SIGNING_SEED={s}
        \\ZB_ACCOUNT_PUB={s}
        \\ENROLL_JWT_TTL_SECONDS=86400
        \\
        \\# Operator seed — NOT read by the bridge. Keep it offline; it signs accounts.
        \\# ZB_OPERATOR_SEED={s}
        \\# Account identity seed — same: offline. Signs nothing day-to-day.
        \\# ZB_ACCOUNT_SEED={s}
        \\
    , .{ nats_port, dir_abs, sk_client.seed(), acct_kp.public(), op_kp.seed(), acct_kp.seed() }) catch return 1;

    const creds_dir = std.fs.path.join(a, &.{ dir, "creds" }) catch return 1;
    std.Io.Dir.cwd().createDirPath(io, creds_dir) catch return 1;
    const conf_path = std.fs.path.join(a, &.{ dir, "nats-server.conf" }) catch return 1;
    const env_path = std.fs.path.join(a, &.{ dir, ".env.bridge" }) catch return 1;
    const creds_path = std.fs.path.join(a, &.{ dir, "creds", "bridge.creds" }) catch return 1;
    writeFile(io, conf_path, conf, force) catch return 1;
    writeFile(io, env_path, env, force) catch return 1;
    writeFile(io, creds_path, bridge_creds, force) catch return 1;

    out(
        \\✅ operator stack generated — no nsc involved:
        \\   {s}/nats-server.conf   operator + ZEBRIDGE account (2 scoped signing keys), resolver preload
        \\   {s}/creds/bridge.creds the bridge's identity (service scope)
        \\   {s}/.env.bridge        NATS_CREDS + ZB_SIGNING_SEED wired for /enroll
        \\   Client onboarding is now ONLY the enrollment flow: invite row → GET /enroll →
        \\   creds. Nobody needs to understand accounts or claims.
        \\   Start:  nats-server -c {s}/nats-server.conf
        \\
    , .{ dir, dir, dir, dir });
    return 0;
}
