//! `bridge --init-nats`: the whole NATS identity stack, generated — no nsc (§10ch).
//!
//! The friction it removes: to run ZeBridge under operator mode you need an operator,
//! an account carrying three SCOPED signing keys (the client template with
//! `{{tag(tenant)}}` / `{{name()}}` substitutions, the responder template — a service
//! that answers queries and never writes, §10hk — and the service key), a bridge user
//! under the service scope, creds files, and a server conf with the resolver preload.
//! That was `nsc add operator` / `nsc add account` / a 90-line bootstrap script — a
//! wall for anyone who just wants to try the bridge.
//!
//! Two modes, and an update:
//!   dev              10 seconds flat: an OPEN server conf (JetStream on, no auth at
//!                    all) plus a matching .env.nats. No JWT in sight. Enrollment
//!                    stays off (no ZB_SIGNING_SEED) — the bridge already treats that
//!                    as "endpoint dark". Loudly marked dev-only.
//!   operator         the full stack, self-contained: every key generated here, the
//!                    operator and account JWTs minted by `jwt_mint.signClaims`, the
//!                    client template's subject list derived FROM THE TOPOLOGY —
//!                    grammar renames propagate instead of drifting from a shell
//!                    script — and .env.nats ready for enrollment (`ZB_SIGNING_SEED`
//!                    is the scoped client key, exactly what /enroll mints with).
//!                    Every seed also goes to operator.store (0600), which the bridge
//!                    never reads: the operator and account seeds live ONLY there.
//!   --update         after a grammar change: re-sign the account from operator.store
//!                    with the same keys, templates re-derived, revocations kept. Only
//!                    the account's preload line changes; issued creds stay valid.
//!   --rotate-client-key  the same re-signing with a NEW client signing key, for a leaked
//!                    ZB_SIGNING_SEED: after a reload NATS refuses every client JWT the old
//!                    key signed, forged or genuine, and genuine devices renew at once
//!                    (/renew proves the device key, not the old JWT). The responder and
//!                    service keys, the bridge's creds and the revocations stay as they are.
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

/// A key pair from its seed text — how `--update` gets back the keys `--init-nats` made.
fn keyFromSeed(seed_text: []const u8) !KeyPair {
    var kp = KeyPair{};
    if (seed_text.len > kp.seed_buf.len) return error.BadSeed;
    @memcpy(kp.seed_buf[0..seed_text.len], seed_text);
    kp.seed_len = seed_text.len;
    var skp = try nats.nkeys.SeedKeyPair.fromSeed(kp.seed());
    defer skp.wipe();
    kp.pub_len = skp.publicKeyText(&kp.pub_buf).len;
    return kp;
}

/// A file holding a seed: mode 0600, so another user on the host cannot read it.
fn writeSecret(io: std.Io, path: []const u8, bytes: []const u8, force: bool) !void {
    return writeFileMode(io, path, bytes, force, @enumFromInt(0o600));
}

fn writeFile(io: std.Io, path: []const u8, bytes: []const u8, force: bool) !void {
    return writeFileMode(io, path, bytes, force, .default_file);
}

fn writeFileMode(io: std.Io, path: []const u8, bytes: []const u8, force: bool, permissions: std.Io.File.Permissions) !void {
    if (!force) {
        if (std.Io.Dir.cwd().openFile(io, path, .{})) |f| {
            var fv = f;
            fv.close(io);
            out("🔴 refusing to overwrite {s} (pass --force to allow) — seeds are credentials\n", .{path});
            return error.WouldOverwrite;
        } else |_| {}
    }
    var f = try std.Io.Dir.cwd().createFile(io, path, .{ .permissions = permissions });
    defer f.close(io);
    try f.writeStreamingAll(io, bytes);
}

/// The scoped CLIENT template's allow lists, from the topology — the single grant
/// block every principal inherits (jwt-bootstrap.sh's list, ported; see PROTOCOL §7.4b
/// for why mutation verdicts are DIRECT.GET and never a consumer).
/// The two edge roles (§10hk). They share the read side — the streams, the KV buckets,
/// the seed objects, the acks, the JetStream API replies on the inbox. A client adds its
/// write side: mutations under its own name, its heartbeat key, asking its tenant's
/// services. A responder adds the answering side — subscribing `query.<tenant>.<name>`
/// for its tenants and publishing replies to any inbox — and nothing of the write side:
/// a service that answers from a replica cannot write to PostgreSQL through the bridge.
const Role = enum { client, responder };

/// The JetStream API prefix for a domain: none is the server's own `$JS.API.`; a name
/// is `$JS.<name>.API.` — what a client on a leaf node dials to reach the hub's
/// JetStream. The running bridge builds the same subjects from NATS_JS_DOMAIN
/// (`JetStreamOptions.domain`), and /enroll hands the name to every client.
fn jsApiPrefix(a: std.mem.Allocator, domain: ?[]const u8) ![]const u8 {
    const d = domain orelse return "$JS.API.";
    return std.fmt.allocPrint(a, "$JS.{s}.API.", .{d});
}

test "roleAllows names the API under the domain's prefix, or the plain one" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const owned = try topology_mod.loadEmbedded(aa);
    const plain = try roleAllows(aa, &owned.topology, .client, try jsApiPrefix(aa, null));
    try std.testing.expect(std.mem.find(u8, plain.pub_json, "\"$JS.API.INFO\"") != null);
    try std.testing.expect(std.mem.find(u8, plain.pub_json, "$JS.hub.") == null);
    const hub = try roleAllows(aa, &owned.topology, .responder, try jsApiPrefix(aa, "hub"));
    try std.testing.expect(std.mem.find(u8, hub.pub_json, "\"$JS.hub.API.INFO\"") != null);
    // roleAllows renders ONE prefix; roleAllowsFor adds the plain one beside a domain.
    try std.testing.expect(std.mem.find(u8, hub.pub_json, "$JS.API.") == null);
    // The ack subjects carry the domain too, and `$JS.ACK.>` still covers them.
    try std.testing.expect(std.mem.find(u8, hub.pub_json, "\"$JS.ACK.>\"") != null);
}

/// `js_api` is the JetStream API prefix the grants name — `$JS.API.` or, with a
/// domain, `$JS.<domain>.API.` (see `jsApiPrefix`): a client reaches JetStream only
/// through subjects under it, so the grants and the deployment must agree.
fn roleAllows(
    a: std.mem.Allocator,
    topo: *const topology_mod.Topology,
    role: Role,
    js_api: []const u8,
) !struct {
    pub_json: []u8,
    sub_json: []u8,
} {
    const cdc_pre = topo.cdc_stream_prefix; // "CDC_"
    const cdc_pub = topo.cdc_stream_public; // "CDC_PUBLIC"
    const kv_schemas = topo.kv_schemas;
    const kv_gens = topo.kv_generations;
    const kv_tenants = topo.kv_tenants;
    const kv_live = topo.kv_live;
    const obj_pre = topo.generation_bucket_prefix; // "gen-"
    const open = topo.open_tenant; // "_default"
    const subj_cdc = topo.subject_cdc_prefix; // "cdc"
    const subj_mut = topo.subject_mutations_prefix; // "mutation"
    const subj_ack = topo.mutation_ack_prefix; // "mutation_ack"
    const verdicts = topo.stream_verdicts; // "VERDICTS"
    const subj_query = topo.query_prefix; // "query" (§10hj: a service answers, a client asks)
    const res_pre = topo.results_bucket_prefix; // "res-" (§10hq: answers too large for one message)

    var pubs: std.ArrayList([]u8) = .empty;
    var subs: std.ArrayList([]u8) = .empty;
    const P = struct {
        fn add(
            list: *std.ArrayList([]u8),
            alloc: std.mem.Allocator,
            comptime fmt: []const u8,
            args: anytype,
        ) !void {
            try list.append(
                alloc,
                try std.fmt.allocPrint(alloc, fmt, args),
            );
        }
    };

    if (role == .client) try P.add(&pubs, a, "{s}.{{{{name()}}}}.>", .{subj_mut});

    try P.add(&pubs, a, "{s}INFO", .{js_api});

    inline for (.{ "CONSUMER.CREATE", "CONSUMER.INFO", "CONSUMER.MSG.NEXT" }) |op| {
        // tenant stream, public stream, the schemas KV — CREATE also bare (no filter).
        // NOT the generations KV (§10jr): a consumer reads the whole stream, so it let a
        // client list and watch every tenant's manifests; clients read theirs by DIRECT
        // GET of an exact, tenant-scoped key, which is all they need.
        if (std.mem.eql(u8, op, "CONSUMER.CREATE")) {
            try P.add(&pubs, a, "{s}{s}.{s}{{{{tag(tenant)}}}}", .{ js_api, op, cdc_pre });
            try P.add(&pubs, a, "{s}{s}.{s}", .{ js_api, op, cdc_pub });
            try P.add(&pubs, a, "{s}{s}.KV_{s}", .{ js_api, op, kv_schemas });
            try P.add(&pubs, a, "{s}{s}.OBJ_{s}{{{{tag(tenant)}}}}", .{ js_api, op, obj_pre });
            try P.add(&pubs, a, "{s}{s}.OBJ_{s}{s}", .{ js_api, op, obj_pre, open });
        }
        try P.add(&pubs, a, "{s}{s}.{s}{{{{tag(tenant)}}}}.>", .{ js_api, op, cdc_pre });
        try P.add(&pubs, a, "{s}{s}.{s}.>", .{ js_api, op, cdc_pub });
        try P.add(&pubs, a, "{s}{s}.KV_{s}.>", .{ js_api, op, kv_schemas });
        try P.add(&pubs, a, "{s}{s}.OBJ_{s}{{{{tag(tenant)}}}}.>", .{ js_api, op, obj_pre });
        try P.add(&pubs, a, "{s}{s}.OBJ_{s}{s}.>", .{ js_api, op, obj_pre, open });
    }
    try P.add(&pubs, a, "{s}STREAM.INFO.{s}{{{{tag(tenant)}}}}", .{ js_api, cdc_pre });
    try P.add(&pubs, a, "{s}STREAM.INFO.{s}", .{ js_api, cdc_pub });
    try P.add(&pubs, a, "{s}STREAM.INFO.KV_{s}", .{ js_api, kv_schemas });
    // §10jr: NOT KV_generations / KV_tenants — STREAM.INFO's `subjects_filter` (in the
    // body, where no grant reaches) lists every key: every tenant's manifests, every
    // principal. Clients bind those buckets without asking (nats.zig kvBind, nats.js
    // Kvm.open) and read their exact keys by direct get.
    try P.add(&pubs, a, "{s}STREAM.INFO.OBJ_{s}{{{{tag(tenant)}}}}", .{ js_api, obj_pre });
    try P.add(&pubs, a, "{s}STREAM.INFO.OBJ_{s}{s}", .{ js_api, obj_pre, open });
    try P.add(&pubs, a, "{s}STREAM.MSG.GET.KV_{s}", .{ js_api, kv_schemas });
    try P.add(&pubs, a, "{s}STREAM.MSG.GET.OBJ_{s}{{{{tag(tenant)}}}}", .{ js_api, obj_pre });
    try P.add(&pubs, a, "{s}STREAM.MSG.GET.OBJ_{s}{s}", .{ js_api, obj_pre, open });
    try P.add(&pubs, a, "{s}DIRECT.GET.KV_{s}.>", .{ js_api, kv_schemas });
    try P.add(&pubs, a, "{s}DIRECT.GET.KV_{s}.$KV.{s}.{{{{tag(tenant)}}}}.>", .{ js_api, kv_gens, kv_gens });
    try P.add(&pubs, a, "{s}DIRECT.GET.KV_{s}.$KV.{s}.{s}.>", .{ js_api, kv_gens, kv_gens, open });
    try P.add(&pubs, a, "{s}DIRECT.GET.KV_{s}.$KV.{s}.{{{{name()}}}}", .{ js_api, kv_tenants, kv_tenants });
    try P.add(&pubs, a, "{s}DIRECT.GET.OBJ_{s}{{{{tag(tenant)}}}}.>", .{ js_api, obj_pre });
    try P.add(&pubs, a, "{s}DIRECT.GET.OBJ_{s}{s}.>", .{ js_api, obj_pre, open });
    // §10hq: the ANSWER bucket of a tenant. A client READS it: an answer too large for
    // one message names an object, and the library fetches it so the host never sees the
    // difference. A responder also CREATES and WRITES it, below.
    inline for (.{ "STREAM.INFO", "DIRECT.GET", "CONSUMER.CREATE", "CONSUMER.INFO", "CONSUMER.MSG.NEXT", "STREAM.MSG.GET" }) |op| {
        try P.add(&pubs, a, "{s}{s}.OBJ_{s}{{{{tag(tenant)}}}}", .{ js_api, op, res_pre });
        try P.add(&pubs, a, "{s}{s}.OBJ_{s}{{{{tag(tenant)}}}}.>", .{ js_api, op, res_pre });
        try P.add(&pubs, a, "{s}{s}.OBJ_{s}{s}", .{ js_api, op, res_pre, open });
        try P.add(&pubs, a, "{s}{s}.OBJ_{s}{s}.>", .{ js_api, op, res_pre, open });
    }

    try P.add(&pubs, a, "$JS.ACK.>", .{});
    // Its own verdict marker, both roles: the revocation check at connect reads
    // `mutation_ack.<name>.revoked` (§10hl) — a responder is a principal too.
    try P.add(&pubs, a, "{s}DIRECT.GET.{s}.{s}.{{{{name()}}}}.>", .{ js_api, verdicts, subj_ack });
    switch (role) {
        .client => {
            // §10dc: the fleet heartbeat — a client may write ONLY its own key. With a
            // domain the put goes through `$JS.<domain>.API.$KV.` (§10lk): a leaf never
            // sees a bare `$KV.>`. roleAllowsFor merges this with the plain call's bare key.
            const kv_put: []const u8 = if (std.mem.eql(u8, js_api, "$JS.API.")) "" else js_api;
            try P.add(&pubs, a, "{s}$KV.{s}.{{{{tag(tenant)}}}}.{{{{name()}}}}", .{ kv_put, kv_live });
            try P.add(&pubs, a, "{s}$KV.{s}.{s}.{{{{name()}}}}", .{ kv_put, kv_live, open });
            // §10hj: a client may ASK its tenant's services (request/reply; the reply lands on
            // its own inbox, granted below).
            try P.add(&pubs, a, "{s}.{{{{tag(tenant)}}}}.>", .{subj_query});
            try P.add(&pubs, a, "{s}.{s}.>", .{ subj_query, open });
        },
        .responder => {
            // §10hk: a responder ANSWERS its tenants' queries — the reply goes to the
            // asker's inbox, whichever it is.
            try P.add(&pubs, a, "_INBOX.>", .{});
            // §10hq: and it WRITES the answers too large to send inline — creating the
            // tenant's answer bucket on first use, then publishing the object's chunks.
            try P.add(&pubs, a, "{s}STREAM.CREATE.OBJ_{s}{{{{tag(tenant)}}}}", .{ js_api, res_pre });
            try P.add(&pubs, a, "{s}STREAM.CREATE.OBJ_{s}{s}", .{ js_api, res_pre, open });
            try P.add(&pubs, a, "$O.{s}{{{{tag(tenant)}}}}.>", .{res_pre});
            try P.add(&pubs, a, "$O.{s}{s}.>", .{ res_pre, open });
            try P.add(&subs, a, "{s}.{{{{tag(tenant)}}}}.>", .{subj_query});
            try P.add(&subs, a, "{s}.{s}.>", .{ subj_query, open });
        },
    }
    // Its own verdict subject, both roles: libzb subscribes it at connect, before it knows
    // whether it will ever write; a responder's never carries anything (§10hl).
    try P.add(&subs, a, "{s}.{{{{name()}}}}.>", .{subj_ack});
    try P.add(&subs, a, "{s}.{{{{tag(tenant)}}}}.>", .{subj_cdc});
    try P.add(&subs, a, "{s}.{s}.>", .{ subj_cdc, open });
    try P.add(&subs, a, "$KV.{s}.>", .{kv_schemas});
    // §10hm: its OWN inbox, not the shared one. JetStream delivers pulled messages,
    // KV answers and object chunks to the reader's inbox, so `_INBOX.>` let any
    // principal read what every other one received (measured, §10fs). Every client
    // here sets its inbox prefix to `_INBOX.<principal>` to stay inside this grant.
    try P.add(&subs, a, "_INBOX.{{{{name()}}}}.>", .{});

    return .{
        .pub_json = try joinJson(a, pubs.items),
        .sub_json = try joinJson(a, subs.items),
    };
}

/// A role's grants: under `$JS.API.`, and with a domain under `$JS.<domain>.API.` too.
/// Both are needed, measured 2026-10-04 (NOTES §10lh): a client behind a leaf sends
/// `$JS.<domain>.API.…`, checked by the leaf before the link; a client connected to the
/// hub itself may send either, but the hub maps its own domain's prefix onto `$JS.API.`
/// and checks permissions on the result — silently: no violation is logged, the request
/// just never answers. So a domain-only grant served leaf clients and refused direct ones.
fn roleAllowsFor(
    a: std.mem.Allocator,
    topo: *const topology_mod.Topology,
    role: Role,
    js_domain: ?[]const u8,
) !struct { pub_json: []u8, sub_json: []u8 } {
    const own = try roleAllows(a, topo, role, try jsApiPrefix(a, js_domain));
    if (js_domain == null) return .{ .pub_json = own.pub_json, .sub_json = own.sub_json };
    const plain = try roleAllows(a, topo, role, try jsApiPrefix(a, null));
    return .{
        .pub_json = try mergeJsonLists(a, own.pub_json, plain.pub_json),
        .sub_json = try mergeJsonLists(a, own.sub_json, plain.sub_json),
    };
}

/// Two `"a","b"` lists as one, first-seen order, no duplicates (subjects hold no comma).
fn mergeJsonLists(a: std.mem.Allocator, x: []const u8, y: []const u8) ![]u8 {
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var buf: std.ArrayList(u8) = .empty;
    for ([_][]const u8{ x, y }) |list| {
        var it = std.mem.splitScalar(u8, list, ',');
        while (it.next()) |item| {
            if (item.len == 0) continue;
            const gop = try seen.getOrPut(a, item);
            if (gop.found_existing) continue;
            if (buf.items.len > 0) try buf.append(a, ',');
            try buf.appendSlice(a, item);
        }
    }
    return buf.toOwnedSlice(a);
}

test "a domain grants both prefixes, once each" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const owned = try topology_mod.loadEmbedded(aa);
    const both = try roleAllowsFor(aa, &owned.topology, .client, "hub");
    try std.testing.expect(std.mem.find(u8, both.pub_json, "\"$JS.hub.API.INFO\"") != null);
    try std.testing.expect(std.mem.find(u8, both.pub_json, "\"$JS.API.INFO\"") != null);
    // the lines without a prefix (mutations, acks, inboxes) appear once
    const mut = "\"mutation.{{name()}}.>\"";
    const first = std.mem.find(u8, both.pub_json, mut).?;
    try std.testing.expect(std.mem.findPos(u8, both.pub_json, first + 1, mut) == null);
    // the heartbeat key: through the domain's API, and bare for a server without one
    try std.testing.expect(std.mem.find(u8, both.pub_json, "\"$JS.hub.API.$KV.live.{{tag(tenant)}}.{{name()}}\"") != null);
    try std.testing.expect(std.mem.find(u8, both.pub_json, "\"$KV.live.{{tag(tenant)}}.{{name()}}\"") != null);
    // without a domain: the plain prefix alone
    const plain = try roleAllowsFor(aa, &owned.topology, .client, null);
    try std.testing.expect(std.mem.find(u8, plain.pub_json, "$JS.hub.") == null);
    try std.testing.expect(std.mem.find(u8, plain.pub_json, "$JS.API.$KV.") == null);
}

fn joinJson(
    a: std.mem.Allocator,
    items: [][]u8,
) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    for (items, 0..) |it, i| {
        if (i > 0) try buf.append(a, ',');
        try buf.append(a, '"');
        try buf.appendSlice(a, it);
        try buf.append(a, '"');
    }
    return buf.toOwnedSlice(a);
}

fn credsFile(
    a: std.mem.Allocator,
    jwt: []const u8,
    seed: []const u8,
) ![]u8 {
    return std.fmt.allocPrint(
        a,
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
    ,
        .{ jwt, seed },
    );
}

/// Entry point. Returns the process exit code; prints everything it does.
pub fn run(
    io: std.Io,
    init: *const std.process.Init,
) u8 {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // ── the mode word and --force are the core; directory, ports and the JetStream
    //    domain have flags, and everything else is the well-known defaults. The
    //    generated files are plain text and the deployment's to edit.
    var mode: Mode = .dev;
    var force = false;
    // `--js-domain NAME`: the deployment reaches JetStream across a leaf link. The
    // server conf declares the domain, the grants name `$JS.<NAME>.API.`, and
    // .env.nats carries NATS_JS_DOMAIN so the bridge and /enroll say the same.
    var js_domain: ?[]const u8 = null;
    // `--update [--store PATH]`: re-sign the account from the offline store (runUpdate).
    var update = false;
    // `--rotate-client-key`: the same re-signing, with a new client signing key.
    var rotate_client = false;
    var store_path: ?[]const u8 = null;
    // `--dir DIR` (§10kd): where the files live on THEIR host — written there, and the
    // absolute paths inside them (JetStream's store_dir, NATS_CREDS) point there. Run
    // it on the host that will use the files: `--dir /etc/zebridge`.
    var dir: []const u8 = "zb-nats";
    // `--port`, `--http-port`, `--ws-port`: the server's client, monitoring and WebSocket
    // ports, written into the conf (and the client one into NATS_URL), so a second NATS
    // beside another needs no hand edit.
    var nats_port: u32 = 4222;
    var http_port: u32 = 8222;
    var ws_port: u32 = 8080;
    {
        var it = init.minimal.args.iterate();
        _ = it.next();
        while (it.next()) |arg| {
            if (std.mem.eql(u8, arg, "--init-nats")) continue;
            if (std.mem.eql(u8, arg, "--force")) {
                force = true;
            } else if (std.mem.eql(u8, arg, "--update")) {
                update = true;
            } else if (std.mem.eql(u8, arg, "--rotate-client-key")) {
                rotate_client = true;
            } else if (std.mem.eql(u8, arg, "--store")) {
                store_path = it.next() orelse return usageErr("--store needs a path");
            } else if (std.mem.eql(u8, arg, "--dir")) {
                dir = it.next() orelse return usageErr("--dir needs a path");
                if (dir.len == 0) return usageErr("--dir needs a path");
            } else if (std.mem.eql(u8, arg, "--port")) {
                nats_port = portArg(it.next()) orelse return usageErr("--port needs a port number (1-65535)");
            } else if (std.mem.eql(u8, arg, "--http-port")) {
                http_port = portArg(it.next()) orelse return usageErr("--http-port needs a port number (1-65535)");
            } else if (std.mem.eql(u8, arg, "--ws-port")) {
                ws_port = portArg(it.next()) orelse return usageErr("--ws-port needs a port number (1-65535)");
            } else if (std.mem.eql(u8, arg, "--js-domain")) {
                const v = it.next() orelse return usageErr("--js-domain needs a name");
                for (v) |ch| if (ch == '.' or ch == ' ' or ch == '*' or ch == '>') return usageErr("--js-domain must be one subject token (no '.', ' ', '*', '>')");
                if (v.len == 0) return usageErr("--js-domain needs a name");
                js_domain = v;
            } else if (std.meta.stringToEnum(Mode, arg)) |m| {
                mode = m;
            } else return usageErr("unknown argument");
        }
    }
    if (rotate_client) {
        if (update) return usageErr("--rotate-client-key and --update are two runs: update first");
        if (js_domain != null) return usageErr("--rotate-client-key takes no --js-domain: add a domain with --update first");
        return runUpdate(io, dir, store_path, null, true);
    }
    if (update) {
        // `--js-domain` with `--update` ADDS a domain to a stack that has none (a leaf
        // joins a running deployment); changing one stays a new stack (runUpdate says so).
        return runUpdate(io, dir, store_path, js_domain, false);
    }
    if (store_path != null) return usageErr("--store goes with --update or --rotate-client-key");
    if (nats_port == http_port or nats_port == ws_port or http_port == ws_port)
        return usageErr("--port, --http-port and --ws-port must differ");

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
        .dev => runDev(
            a,
            io,
            dir,
            dir_abs,
            force,
            nats_port,
            ws_port,
            http_port,
            js_domain,
        ),
        .operator => runOperator(
            a,
            io,
            dir,
            dir_abs,
            force,
            nats_port,
            ws_port,
            http_port,
            js_domain,
        ),
    };
}

fn usageErr(msg: []const u8) u8 {
    out("🔴 {s}\n  bridge --init-nats [dev|operator] [--dir DIR] [--port N] [--http-port N] [--ws-port N] [--js-domain NAME] [--force]\n  bridge --init-nats --update [--dir DIR] [--store PATH] [--js-domain NAME]\n", .{msg});
    return 1;
}

fn portArg(v: ?[]const u8) ?u32 {
    const n = std.fmt.parseInt(u32, v orelse return null, 10) catch return null;
    return if (n >= 1 and n <= 65535) n else null;
}

/// What `--js-domain` renders: the conf's `domain:` line inside `jetstream {}`, and the
/// env's `NATS_JS_DOMAIN=` beside `NATS_URL`. Without the flag the conf line is empty and
/// the env carries the commented hint, so a generated stack reads the same either way.
const DomainLines = struct {
    conf: []const u8,
    env: []const u8,
};

fn domainLines(a: std.mem.Allocator, js_domain: ?[]const u8) !DomainLines {
    const d = js_domain orelse return .{
        .conf = "",
        .env = "# NATS_JS_DOMAIN=            # set when JetStream is reached across a leaf link (--js-domain)",
    };
    return .{
        .conf = try std.fmt.allocPrint(a, "  domain: {s}\n", .{d}),
        .env = try std.fmt.allocPrint(a, "NATS_JS_DOMAIN={s}", .{d}),
    };
}

fn runDev(
    a: std.mem.Allocator,
    io: std.Io,
    dir: []const u8,
    dir_abs: []const u8,
    force: bool,
    nats_port: u32,
    ws_port: u32,
    http_port: u32,
    js_domain: ?[]const u8,
) u8 {
    // The nkey is generated even though the open server ignores it: the SAME
    // .env.nats then survives the upgrade to operator mode with only the conf
    // swapped — the seed is already in place for the server to authorize.
    const bridge_kp = genKey(io, .user) catch return 1;
    const lines = domainLines(a, js_domain) catch return 1;
    const domain_line = lines.conf;
    const env_line = lines.env;

    const conf = std.fmt.allocPrint(
        a,
        \\# Generated by `bridge --init-nats dev` — DEV ONLY.
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
        \\{s}}}
        \\
    ,
        .{ nats_port, http_port, ws_port, dir_abs, domain_line },
    ) catch return 1;

    const env = std.fmt.allocPrint(
        a,
        \\# Generated by `bridge --init-nats dev` — DEV ONLY (open NATS, no JWT).
        \\# NATS only. The bridge also loads the DBA's .env.bridge, which names the database
        \\# and the bridge's own settings: DATABASE_READER_URL, DATABASE_WRITER_URL,
        \\# BRIDGE_CDC_SLOT, BRIDGE_CDC_PUBLICATION, and GENERATIONS_ENABLED=1 (the snapshots
        \\# clients seed from). The two files share no setting: load them in any order.
        \\NATS_URL=nats://127.0.0.1:{d}
        \\{s}
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
    ,
        .{ nats_port, env_line, bridge_kp.public(), bridge_kp.seed() },
    ) catch return 1;

    const conf_path = std.Io.Dir.path.join(a, &.{ dir, "nats-server.conf" }) catch return 1;
    const env_path = std.Io.Dir.path.join(a, &.{ dir, ".env.nats" }) catch return 1;
    writeFile(io, conf_path, conf, force) catch return 1;
    writeFile(io, env_path, env, force) catch return 1;

    out(
        \\✅ dev stack generated (OPEN server — dev only):
        \\   {s}/nats-server.conf     nats-server -c {s}/nats-server.conf
        \\   {s}/.env.nats            set -a; . {s}/.env.nats; . ./.env.bridge; set +a; ./bridge
        \\   .env.bridge is yours: the database URLs, the slot, the publication. No JWT anywhere.
        \\
    ,
        .{ dir, dir, dir, dir },
    );
    return 0;
}

/// The ZEBRIDGE account JWT: JetStream unlimited, and the three SCOPED signing keys, each
/// with its role template — the client and responder ones DERIVED from the topology
/// (grammar.json), so a grammar change is a `--update`, never a hand-edited list. Shared
/// by `operator` (new keys) and `--update` (the same keys, from the store).
fn accountJwt(
    a: std.mem.Allocator,
    topo: *const topology_mod.Topology,
    js_domain: ?[]const u8,
    now: i64,
    op_seed_kp: *nats.nkeys.SeedKeyPair,
    op_pub: []const u8,
    acct_pub: []const u8,
    sk_client_pub: []const u8,
    sk_responder_pub: []const u8,
    sk_service_pub: []const u8,
    revocations: []const u8,
) ![]const u8 {
    const allows = try roleAllowsFor(a, topo, .client, js_domain);
    const answers = try roleAllowsFor(a, topo, .responder, js_domain);
    const acct_claims = try std.fmt.allocPrint(
        a,
        "{{\"jti\":\"__JTI__\",\"iat\":{d},\"iss\":\"{s}\",\"name\":\"ZEBRIDGE\",\"sub\":\"{s}\",\"nats\":{{" ++
            "\"limits\":{{\"subs\":-1,\"data\":-1,\"payload\":-1,\"imports\":-1,\"exports\":-1,\"wildcards\":true," ++
            "\"conn\":-1,\"leaf\":-1,\"mem_storage\":-1,\"disk_storage\":-1,\"streams\":-1,\"consumer\":-1," ++
            "\"max_ack_pending\":-1,\"mem_max_stream_bytes\":-1,\"disk_max_stream_bytes\":-1}}," ++
            "\"signing_keys\":[" ++
            "{{\"kind\":\"user_scope\",\"key\":\"{s}\",\"role\":\"client\",\"template\":{{" ++
            "\"pub\":{{\"allow\":[{s}]}},\"sub\":{{\"allow\":[{s}]}},\"subs\":-1,\"data\":-1,\"payload\":-1}},\"description\":\"\"}}," ++
            "{{\"kind\":\"user_scope\",\"key\":\"{s}\",\"role\":\"responder\",\"template\":{{" ++
            "\"pub\":{{\"allow\":[{s}]}},\"sub\":{{\"allow\":[{s}]}},\"subs\":-1,\"data\":-1,\"payload\":-1}},\"description\":\"\"}}," ++
            "{{\"kind\":\"user_scope\",\"key\":\"{s}\",\"role\":\"service\",\"template\":{{" ++
            "\"pub\":{{\"allow\":[\"\\u003e\"]}},\"sub\":{{\"allow\":[\"\\u003e\"]}},\"subs\":-1,\"data\":-1,\"payload\":-1}},\"description\":\"\"}}]," ++
            "\"default_permissions\":{{\"pub\":{{}},\"sub\":{{}}}},\"authorization\":{{}},{s}\"type\":\"account\",\"version\":2}}}}",
        .{ now, op_pub, acct_pub, sk_client_pub, allows.pub_json, allows.sub_json, sk_responder_pub, answers.pub_json, answers.sub_json, sk_service_pub, revocations },
    );
    return jwt_mint.signClaims(a, op_seed_kp, acct_claims, "__JTI__");
}

fn runOperator(
    a: std.mem.Allocator,
    io: std.Io,
    dir: []const u8,
    dir_abs: []const u8,
    force: bool,
    nats_port: u32,
    ws_port: u32,
    http_port: u32,
    js_domain: ?[]const u8,
) u8 {
    // ── the topology: BUILT IN (§10ci) — the grants and the running bridge mint
    //    from the same embedded grammar, so they cannot disagree, and this command
    //    needs no file present anywhere.
    const owned = topology_mod.loadEmbedded(a) catch return 1;
    const topo: topology_mod.Topology = owned.topology;
    const lines = domainLines(a, js_domain) catch return 1;
    const domain_line = lines.conf;
    const env_line = lines.env;

    // ── every key, generated here ───────────────────────────────────────────────
    const op_kp = genKey(io, .operator) catch return 1;
    const sys_kp = genKey(io, .account) catch return 1;
    const acct_kp = genKey(io, .account) catch return 1;
    const sk_client = genKey(io, .account) catch return 1; // signing keys are account-type
    const sk_service = genKey(io, .account) catch return 1;
    const sk_responder = genKey(io, .account) catch return 1; // §10hk: mints the services that answer
    const bridge_user = genKey(io, .user) catch return 1;

    var op_seed_kp = nats.nkeys.SeedKeyPair.fromSeed(op_kp.seed()) catch return 1;
    defer op_seed_kp.wipe();

    const now: i64 = @intCast(c.time(null));

    // ── operator JWT: self-signed, naming the system account. Not optional: the
    //    first live boot of a generated conf died with "Can't start JetStream: …
    //    system account not setup" — JetStream's internal subscriptions live on the
    //    system account, so operator mode without one is a server that cannot start.
    const op_claims = std.fmt.allocPrint(
        a,
        "{{\"jti\":\"__JTI__\",\"iat\":{d},\"iss\":\"{s}\",\"name\":\"ZeBridgeOp\",\"sub\":\"{s}\"," ++
            "\"nats\":{{\"system_account\":\"{s}\",\"type\":\"operator\",\"version\":2}}}}",
        .{
            now,
            op_kp.public(),
            op_kp.public(),
            sys_kp.public(),
        },
    ) catch return 1;
    const op_jwt = jwt_mint.signClaims(a, &op_seed_kp, op_claims, "__JTI__") catch return 1;

    // ── the SYS account: minimal, and deliberately WITHOUT JetStream limits — the
    //    system account may not use JetStream, and nothing ever connects to it here.
    const sys_claims = std.fmt.allocPrint(
        a,
        "{{\"jti\":\"__JTI__\",\"iat\":{d},\"iss\":\"{s}\",\"name\":\"SYS\",\"sub\":\"{s}\",\"nats\":{{" ++
            "\"limits\":{{\"subs\":-1,\"data\":-1,\"payload\":-1,\"imports\":-1,\"exports\":-1,\"wildcards\":true,\"conn\":-1,\"leaf\":-1}}," ++
            "\"default_permissions\":{{\"pub\":{{}},\"sub\":{{}}}},\"authorization\":{{}},\"type\":\"account\",\"version\":2}}}}",
        .{
            now,
            op_kp.public(),
            sys_kp.public(),
        },
    ) catch return 1;
    const sys_jwt = jwt_mint.signClaims(a, &op_seed_kp, sys_claims, "__JTI__") catch return 1;

    // ── account JWT: JetStream unlimited + the three SCOPED signing keys ────────
    const acct_jwt = accountJwt(a, &topo, js_domain, now, &op_seed_kp, op_kp.public(), acct_kp.public(), sk_client.public(), sk_responder.public(), sk_service.public(), "") catch return 1;

    // ── the bridge user: a scoped user under the SERVICE key ────────────────────
    // Ten years: the bridge's own credential rotates with a redeploy, not a TTL.
    const bridge_jwt = jwt_mint.mint(
        a,
        sk_service.seed(),
        acct_kp.public(),
        "bridge",
        &.{"service"},
        bridge_user.public(),
        10 * 365 * 24 * 3600,
        now,
    ) catch |err| {
        out("🔴 bridge user mint failed: {}\n", .{err});
        return 1;
    };
    const bridge_creds = credsFile(
        a,
        bridge_jwt,
        bridge_user.seed(),
    ) catch return 1;

    // ── files ───────────────────────────────────────────────────────────────────
    const conf = std.fmt.allocPrint(
        a,
        \\# Generated by `bridge --init-nats operator` — self-contained, no nsc.
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
        \\{s}}}
        \\
    ,
        .{
            op_jwt,
            sys_kp.public(),
            sys_kp.public(),
            sys_jwt,
            acct_kp.public(),
            acct_jwt,
            nats_port,
            http_port,
            ws_port,
            dir_abs,
            domain_line,
        },
    ) catch return 1;

    const env = std.fmt.allocPrint(
        a,
        \\# Generated by `bridge --init-nats operator` — the full JWT stack.
        \\# NATS only. The bridge also loads the DBA's .env.bridge, which names the database
        \\# and the bridge's own settings: DATABASE_READER_URL, DATABASE_WRITER_URL,
        \\# BRIDGE_CDC_SLOT, BRIDGE_CDC_PUBLICATION, and GENERATIONS_ENABLED=1 (the snapshots
        \\# clients seed from). The two files share no setting: load them in any order.
        \\NATS_URL=nats://127.0.0.1:{d}
        \\NATS_CREDS={s}/creds/bridge.creds
        \\{s}
        \\
        \\# Enrollment: the bridge as online signer (GET /enroll). The seed below is the
        \\# account's SCOPED client signing key — it can mint client-role users and
        \\# nothing else; the permission template lives in the account JWT, not here.
        \\ZB_SIGNING_SEED={s}
        \\ZB_ACCOUNT_PUB={s}
        \\ENROLL_JWT_TTL_SECONDS=86400
        \\
        \\# Responders (§10hk): services that answer `query.<tenant>.<name>` from a replica.
        \\# Their signing seed is not here — the bridge never uses it. Mint one offline,
        \\# from the store:
        \\#   bridge --mint-responder --store operator.store --name pois --tenant kilo > pois.creds
        \\
        \\# The operator and account seeds are NOT here: they live in operator.store, which
        \\# the bridge never reads. Move it off this host; bring it back for `--update`.
        \\
    ,
        .{
            nats_port,
            dir_abs,
            env_line,
            sk_client.seed(),
            acct_kp.public(),
        },
    ) catch return 1;

    const creds_dir = std.Io.Dir.path.join(a, &.{ dir, "creds" }) catch return 1;
    std.Io.Dir.cwd().createDirPath(io, creds_dir) catch return 1;
    const conf_path = std.Io.Dir.path.join(a, &.{ dir, "nats-server.conf" }) catch return 1;
    const env_path = std.Io.Dir.path.join(a, &.{ dir, ".env.nats" }) catch return 1;
    const creds_path = std.Io.Dir.path.join(a, &.{ dir, "creds", "bridge.creds" }) catch return 1;
    // The store first: if it cannot be written, no conf may exist signed by keys
    // that nobody kept.
    const store = std.fmt.allocPrint(
        a,
        \\# Generated by `bridge --init-nats operator`. OFFLINE: every seed of the NATS
        \\# identity stack. The bridge never reads this file. `bridge --init-nats --update`
        \\# does: it re-signs the account with these same keys, so nothing issued breaks.
        \\OPERATOR_SEED={s}
        \\SYS_SEED={s}
        \\ACCOUNT_SEED={s}
        \\SK_CLIENT_SEED={s}
        \\SK_RESPONDER_SEED={s}
        \\SK_SERVICE_SEED={s}
        \\JS_DOMAIN={s}
        \\
    ,
        .{ op_kp.seed(), sys_kp.seed(), acct_kp.seed(), sk_client.seed(), sk_responder.seed(), sk_service.seed(), js_domain orelse "" },
    ) catch return 1;
    const store_path = std.Io.Dir.path.join(a, &.{ dir, "operator.store" }) catch return 1;
    writeSecret(io, store_path, store, force) catch return 1;
    writeFile(io, conf_path, conf, force) catch return 1;
    writeSecret(io, env_path, env, force) catch return 1;
    writeSecret(io, creds_path, bridge_creds, force) catch return 1;


    out(
        \\✅ operator stack generated — no nsc involved:
        \\   {s}/nats-server.conf   operator + ZEBRIDGE account (3 scoped signing keys: client, responder, service), resolver preload
        \\   {s}/creds/bridge.creds the bridge's identity (service scope)
        \\   {s}/.env.nats          NATS_URL, NATS_CREDS, ZB_SIGNING_SEED for /enroll — NATS only;
        \\                           the database and the bridge's settings stay in your .env.bridge
        \\   {s}/operator.store     every seed, 0600 — the bridge never reads it: move it OFF this host
        \\   Client onboarding is now ONLY the enrollment flow: invite row → GET /enroll →
        \\   creds. Nobody needs to understand accounts or claims.
        \\   Start:  nats-server -c {s}/nats-server.conf
        \\
    ,
        .{ dir, dir, dir, dir, dir },
    );
    return 0;
}

/// `bridge --init-nats --update`: the grammar changed (a table, a subject), so the role
/// templates must follow. It reads the offline store, re-derives the templates, and
/// re-signs the ZEBRIDGE account with the SAME keys — the operator, the account and the
/// three signing keys keep their public keys, so every creds file and device JWT already
/// issued stays valid. Only the account's line in the conf's resolver_preload changes;
/// hand edits elsewhere in the conf survive. The revocations `bridge --revoke --full`
/// wrote into the account are carried over: an update never un-revokes a key.
/// The offline store: a file, or `-` for standard input — piped from a password manager, so
/// the seeds never touch this host's disk (`keepassxc-cli attachment-export … - | bridge …`).
fn readStore(io: std.Io, a: std.mem.Allocator, path: []const u8) ![]u8 {
    if (std.mem.eql(u8, path, "-")) {
        var buf: [4096]u8 = undefined;
        var r = std.Io.File.stdin().reader(io, &buf);
        return r.interface.allocRemaining(a, .limited(1 << 16));
    }
    return std.Io.Dir.cwd().readFileAlloc(io, path, a, .limited(1 << 16));
}

fn runUpdate(io: std.Io, dir: []const u8, store_arg: ?[]const u8, add_domain: ?[]const u8, rotate_client: bool) u8 {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const store_path = store_arg orelse (std.Io.Dir.path.join(a, &.{ dir, "operator.store" }) catch return 1);
    const conf_path = std.Io.Dir.path.join(a, &.{ dir, "nats-server.conf" }) catch return 1;
    var store = readStore(io, a, store_path) catch {
        out("🔴 cannot read the store {s} (--store PATH if it lives elsewhere, - for standard input)\n", .{store_path});
        return 1;
    };
    var conf = std.Io.Dir.cwd().readFileAlloc(io, conf_path, a, .limited(1 << 20)) catch {
        out("🔴 cannot read {s}\n", .{conf_path});
        return 1;
    };

    const want = [_][]const u8{ "OPERATOR_SEED", "ACCOUNT_SEED", "SK_CLIENT_SEED", "SK_RESPONDER_SEED", "SK_SERVICE_SEED" };
    var keys: [want.len]KeyPair = undefined;
    for (want, 0..) |name, i| {
        const v = storeValue(store, name) orelse {
            out("🔴 {s} has no {s}\n", .{ store_path, name });
            return 1;
        };
        keys[i] = keyFromSeed(v) catch {
            out("🔴 {s} in {s} is not a valid seed\n", .{ name, store_path });
            return 1;
        };
    }
    const op_kp, const acct_kp, const old_client, const sk_responder, const sk_service = keys;
    // A rotation swaps only the client key: the account drops the old one, so NATS refuses
    // every user it signed once the server reloads.
    const sk_client = if (rotate_client) genKey(io, .account) catch return 1 else old_client;
    var domain = storeValue(store, "JS_DOMAIN") orelse "";
    var env_line: ?[]const u8 = null; // NATS_JS_DOMAIN=… for .env.nats, when the domain is added

    // `--js-domain`: add a domain to a stack that has none (a leaf joins a running
    // deployment). The grants gain the domain's prefix beside the plain one
    // (roleAllowsFor), the conf names the domain, .env.nats carries it.
    if (add_domain) |d| {
        if (domain.len > 0 and !std.mem.eql(u8, domain, d)) {
            out("🔴 this stack's JetStream domain is {s}: changing it would cut off the devices behind its leaves — that is a new stack (--init-nats --js-domain)\n", .{domain});
            return 1;
        }
        if (domain.len == 0) {
            domain = d;
            store = setStoreValue(a, store, "JS_DOMAIN", d) catch return 1;
            conf = addConfDomain(a, conf, d) orelse {
                out("🔴 {s} has no `jetstream {{` block to name the domain in\n", .{conf_path});
                return 1;
            };
            env_line = std.fmt.allocPrint(a, "NATS_JS_DOMAIN={s}", .{d}) catch return 1;
        }
    }
    const js_domain: ?[]const u8 = if (domain.len == 0) null else domain;

    // ── the account's current JWT in the conf, and a check it is OURS ───────────
    const needle = std.fmt.allocPrint(a, "{s}: ey", .{acct_kp.public()}) catch return 1;
    const at = std.mem.find(u8, conf, needle) orelse {
        out("🔴 the store's account {s} is not in {s} — wrong store for this conf?\n", .{ acct_kp.public(), conf_path });
        return 1;
    };
    const jwt_start = at + needle.len - 2;
    var jwt_end = jwt_start;
    while (jwt_end < conf.len and (std.ascii.isAlphanumeric(conf[jwt_end]) or conf[jwt_end] == '.' or conf[jwt_end] == '_' or conf[jwt_end] == '-')) jwt_end += 1;
    const old_claims = jwtClaims(a, conf[jwt_start..jwt_end]) orelse {
        out("🔴 the account JWT in {s} does not decode\n", .{conf_path});
        return 1;
    };
    const iss = std.fmt.allocPrint(a, "\"iss\":\"{s}\"", .{op_kp.public()}) catch return 1;
    if (std.mem.find(u8, old_claims, iss) == null) {
        out("🔴 the account JWT in {s} was not signed by the store's operator\n", .{conf_path});
        return 1;
    }
    const revocations = revocationsOf(old_claims);

    // ── re-derive and re-sign ────────────────────────────────────────────────────
    const owned = topology_mod.loadEmbedded(a) catch return 1;
    const topo: topology_mod.Topology = owned.topology;
    var op_seed_kp = nats.nkeys.SeedKeyPair.fromSeed(op_kp.seed()) catch return 1;
    defer op_seed_kp.wipe();
    const now: i64 = @intCast(c.time(null));
    const new_jwt = accountJwt(a, &topo, js_domain, now, &op_seed_kp, op_kp.public(), acct_kp.public(), sk_client.public(), sk_responder.public(), sk_service.public(), revocations) catch return 1;

    const new_claims = jwtClaims(a, new_jwt) orelse return 1;
    if (!rotate_client and std.mem.eql(u8, grantsOf(old_claims), grantsOf(new_claims))) {
        out("✅ {s} already matches the grammar — nothing to update\n", .{conf_path});
        return 0;
    }

    const new_conf = std.mem.concat(a, u8, &.{ conf[0..jwt_start], new_jwt, conf[jwt_end..] }) catch return 1;
    if (rotate_client) return finishRotation(io, a, dir, store_path, store, conf_path, new_conf, sk_client, revocations);
    writeFile(io, conf_path, new_conf, true) catch return 1;
    if (env_line != null) {
        // From standard input there is no file to write back: say the line to add.
        if (std.mem.eql(u8, store_path, "-")) {
            out("⚠️  add `JS_DOMAIN={s}` to your copy of the store (it was read from standard input)\n", .{domain});
        } else writeFile(io, store_path, store, true) catch return 1;
    }
    if (env_line) |line| {
        const env_path = std.Io.Dir.path.join(a, &.{ dir, ".env.nats" }) catch return 1;
        const env = std.Io.Dir.cwd().readFileAlloc(io, env_path, a, .limited(1 << 16)) catch {
            out("⚠️  {s} not found: set {s} in the bridge's environment yourself\n", .{ env_path, line });
            return 0;
        };
        writeFile(io, env_path, setEnvDomain(a, env, line) catch return 1, true) catch return 1;
    }
    out(
        \\✅ ZEBRIDGE account re-signed with the same keys ({s})
        \\   templates re-derived from the grammar; revocations carried over: {s}
        \\   Issued creds and device JWTs stay valid.
        \\
    , .{ conf_path, if (revocations.len == 0) "none" else "yes" });
    if (env_line != null) {
        out(
            \\   JetStream domain {s} added: the conf names it, .env.nats carries NATS_JS_DOMAIN, and the
            \\   grants allow $JS.{s}.API. (clients behind a leaf) beside $JS.API. (clients on this server).
            \\   Devices enrolled before it keep working; their next renewal hands them js_domain.
            \\   Restart NATS (a JetStream setting: a reload does not apply it), then the bridge.
            \\
        , .{ domain, domain });
    } else {
        out("   Now reload the server:\n     nats-server --signal reload\n", .{});
    }
    return 0;
}

/// The rotation's files, the store first: a conf naming a key that nobody kept would
/// leave the bridge unable to mint, and the next rotation unable to find its own key.
/// The new seed goes to the store and to .env.nats; when either is not a file here (the
/// store read from standard input, .env.nats on another host), its line is printed on
/// standard output for the operator to put in place, and the messages stay on stderr.
fn finishRotation(
    io: std.Io,
    a: std.mem.Allocator,
    dir: []const u8,
    store_path: []const u8,
    store: []const u8,
    conf_path: []const u8,
    new_conf: []const u8,
    sk_client: KeyPair,
    revocations: []const u8,
) u8 {
    var stdout_buf: [512]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &stdout_buf);
    const from_stdin = std.mem.eql(u8, store_path, "-");
    if (from_stdin) {
        stdout.interface.print("SK_CLIENT_SEED={s}\n", .{sk_client.seed()}) catch return 1;
    } else {
        const new_store = setStoreValue(a, store, "SK_CLIENT_SEED", sk_client.seed()) catch return 1;
        writeSecret(io, store_path, new_store, true) catch return 1;
    }
    writeFile(io, conf_path, new_conf, true) catch return 1;

    const env_path = std.Io.Dir.path.join(a, &.{ dir, ".env.nats" }) catch return 1;
    var env_written = false;
    if (std.Io.Dir.cwd().readFileAlloc(io, env_path, a, .limited(1 << 16))) |env| {
        const new_env = setStoreValue(a, env, "ZB_SIGNING_SEED", sk_client.seed()) catch return 1;
        writeSecret(io, env_path, new_env, true) catch return 1;
        env_written = true;
    } else |_| {
        stdout.interface.print("ZB_SIGNING_SEED={s}\n", .{sk_client.seed()}) catch return 1;
    }
    stdout.interface.flush() catch return 1;

    out(
        \\✅ client signing key replaced ({s})
        \\   new key {s}; the responder and service keys, the bridge's creds and the revocations ({s}) are unchanged
        \\
    , .{ conf_path, sk_client.public(), if (revocations.len == 0) "none" else "kept" });
    if (from_stdin) out("   ⚠️  put the SK_CLIENT_SEED line printed above in your copy of the store\n", .{});
    if (env_written) {
        out("   {s}: ZB_SIGNING_SEED replaced\n", .{env_path});
    } else out("   ⚠️  {s} not found here: set the ZB_SIGNING_SEED line printed above in the bridge's environment\n", .{env_path});
    out(
        \\   Then, together: reload NATS (nats-server --signal reload) and restart the bridge.
        \\   From the reload, NATS refuses every client JWT the old key signed; devices renew at once.
        \\
    , .{});
    return 0;
}

/// `NAME=value` set in the store's text: the line replaced, or appended.
fn setStoreValue(a: std.mem.Allocator, store: []const u8, name: []const u8, value: []const u8) ![]u8 {
    var out_buf: std.ArrayList(u8) = .empty;
    var found = false;
    var lines = std.mem.splitScalar(u8, store, '\n');
    var first = true;
    while (lines.next()) |raw| {
        if (!first) try out_buf.append(a, '\n');
        first = false;
        const line = std.mem.trim(u8, raw, " \t\r");
        const eq = std.mem.findScalar(u8, line, '=');
        if (line.len > 0 and line[0] != '#' and eq != null and std.mem.eql(u8, line[0..eq.?], name)) {
            try out_buf.print(a, "{s}={s}", .{ name, value });
            found = true;
        } else try out_buf.appendSlice(a, raw);
    }
    if (!found) {
        if (out_buf.items.len > 0 and out_buf.items[out_buf.items.len - 1] != '\n') try out_buf.append(a, '\n');
        try out_buf.print(a, "{s}={s}\n", .{ name, value });
    }
    return out_buf.toOwnedSlice(a);
}

/// The conf's `jetstream {` block gains `domain: <d>` (null when there is no such block).
fn addConfDomain(a: std.mem.Allocator, conf: []const u8, d: []const u8) ?[]u8 {
    const open = std.mem.find(u8, conf, "jetstream {") orelse return null;
    const eol = std.mem.findScalarPos(u8, conf, open, '\n') orelse return null;
    const line = std.fmt.allocPrint(a, "  domain: {s}\n", .{d}) catch return null;
    return std.mem.concat(a, u8, &.{ conf[0 .. eol + 1], line, conf[eol + 1 ..] }) catch null;
}

/// .env.nats with `NATS_JS_DOMAIN=…` in place of the commented hint (or a stale value).
fn setEnvDomain(a: std.mem.Allocator, env: []const u8, line: []const u8) ![]u8 {
    var out_buf: std.ArrayList(u8) = .empty;
    var found = false;
    var lines = std.mem.splitScalar(u8, env, '\n');
    var first = true;
    while (lines.next()) |raw| {
        if (!first) try out_buf.append(a, '\n');
        first = false;
        const t = std.mem.trim(u8, raw, " \t\r");
        if (std.mem.startsWith(u8, t, "NATS_JS_DOMAIN=") or std.mem.startsWith(u8, t, "# NATS_JS_DOMAIN=")) {
            try out_buf.appendSlice(a, line);
            found = true;
        } else try out_buf.appendSlice(a, raw);
    }
    if (!found) {
        if (out_buf.items.len > 0 and out_buf.items[out_buf.items.len - 1] != '\n') try out_buf.append(a, '\n');
        try out_buf.print(a, "{s}\n", .{line});
    }
    return out_buf.toOwnedSlice(a);
}

test "setStoreValue replaces a line or appends one" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const s1 = try setStoreValue(aa, "# seeds\nACCOUNT_SEED=SA\nJS_DOMAIN=\n", "JS_DOMAIN", "hub");
    try std.testing.expectEqualStrings("# seeds\nACCOUNT_SEED=SA\nJS_DOMAIN=hub\n", s1);
    const s2 = try setStoreValue(aa, s1, "NOTE", "1");
    try std.testing.expect(std.mem.endsWith(u8, s2, "JS_DOMAIN=hub\nNOTE=1\n"));
}

test "addConfDomain and setEnvDomain" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const conf = try std.testing.allocator.dupe(u8, "port: 4222\njetstream {\n  store_dir: \"/x\"\n}\n");
    defer std.testing.allocator.free(conf);
    try std.testing.expectEqualStrings("port: 4222\njetstream {\n  domain: hub\n  store_dir: \"/x\"\n}\n", addConfDomain(aa, conf, "hub").?);
    const env = try setEnvDomain(aa, "NATS_URL=tls://n:4222\n# NATS_JS_DOMAIN=            # set when…\n", "NATS_JS_DOMAIN=hub");
    try std.testing.expectEqualStrings("NATS_URL=tls://n:4222\nNATS_JS_DOMAIN=hub\n", env);
}

/// `NAME=value` from the store; comments and blank lines skipped.
fn storeValue(store: []const u8, name: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, store, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0 or line[0] == '#') continue;
        const eq = std.mem.findScalar(u8, line, '=') orelse continue;
        if (std.mem.eql(u8, line[0..eq], name)) return line[eq + 1 ..];
    }
    return null;
}

/// A JWT's claims segment, decoded.
fn jwtClaims(a: std.mem.Allocator, jwt: []const u8) ?[]const u8 {
    var it = std.mem.splitScalar(u8, jwt, '.');
    _ = it.next();
    const b64 = it.next() orelse return null;
    const dec = std.base64.url_safe_no_pad.Decoder;
    const n = dec.calcSizeForSlice(b64) catch return null;
    const buf = a.alloc(u8, n) catch return null;
    dec.decode(buf, b64) catch return null;
    return buf;
}

/// `"revocations":{…},` as it stands in the claims — the map holds only
/// `"U…":seconds` pairs, so the first `}` closes it — or "" when there is none.
fn revocationsOf(claims: []const u8) []const u8 {
    const start = std.mem.find(u8, claims, "\"revocations\":{") orelse return "";
    const close = std.mem.findScalarPos(u8, claims, start, '}') orelse return "";
    var end = close + 1;
    if (end < claims.len and claims[end] == ',') end += 1;
    return claims[start..end];
}

/// What an update can change: the signing keys and their templates, up to the end of
/// the claims. jti and iat differ on every signing and are left out.
fn grantsOf(claims: []const u8) []const u8 {
    const start = std.mem.find(u8, claims, "\"signing_keys\"") orelse return claims;
    return claims[start..];
}

test "an update keeps the store's keys and carries the revocations over" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const owned = try topology_mod.loadEmbedded(aa);
    const io = std.testing.io;
    const op = try genKey(io, .operator);
    const acct = try genKey(io, .account);
    const sk = try genKey(io, .account);
    // The store round trip: a key rebuilt from its seed has the same public key.
    const store = try std.fmt.allocPrint(aa, "# offline\nOPERATOR_SEED={s}\nJS_DOMAIN=\n", .{op.seed()});
    const back = try keyFromSeed(storeValue(store, "OPERATOR_SEED").?);
    try std.testing.expectEqualStrings(op.public(), back.public());
    try std.testing.expectEqualStrings("", storeValue(store, "JS_DOMAIN").?);

    var op_skp = try nats.nkeys.SeedKeyPair.fromSeed(op.seed());
    defer op_skp.wipe();
    const revoked = "\"revocations\":{\"UABC\":1700000000,\"UDEF\":1700000001},";
    const first = try accountJwt(aa, &owned.topology, null, 1, &op_skp, op.public(), acct.public(), sk.public(), sk.public(), sk.public(), revoked);
    const first_claims = jwtClaims(aa, first).?;
    try std.testing.expectEqualStrings(revoked, revocationsOf(first_claims));

    // Re-signed later with what it carried: the same grants, the same revocations.
    const again = try accountJwt(aa, &owned.topology, null, 2, &op_skp, op.public(), acct.public(), sk.public(), sk.public(), sk.public(), revocationsOf(first_claims));
    const again_claims = jwtClaims(aa, again).?;
    try std.testing.expectEqualStrings(grantsOf(first_claims), grantsOf(again_claims));
    // A domain change is a grants change.
    const hub = try accountJwt(aa, &owned.topology, "hub", 3, &op_skp, op.public(), acct.public(), sk.public(), sk.public(), sk.public(), "");
    try std.testing.expect(!std.mem.eql(u8, grantsOf(first_claims), grantsOf(jwtClaims(aa, hub).?)));
    try std.testing.expectEqualStrings("", revocationsOf(jwtClaims(aa, hub).?));
}

test "a client-key rotation swaps that key only, and keeps the revocations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const owned = try topology_mod.loadEmbedded(aa);
    const io = std.testing.io;
    const op = try genKey(io, .operator);
    const acct = try genKey(io, .account);
    const old_client = try genKey(io, .account);
    const new_client = try genKey(io, .account);
    const responder = try genKey(io, .account);
    const service = try genKey(io, .account);
    var op_skp = try nats.nkeys.SeedKeyPair.fromSeed(op.seed());
    defer op_skp.wipe();
    const revoked = "\"revocations\":{\"UABC\":1700000000},";

    const before = jwtClaims(aa, try accountJwt(aa, &owned.topology, null, 1, &op_skp, op.public(), acct.public(), old_client.public(), responder.public(), service.public(), revoked)).?;
    const after = jwtClaims(aa, try accountJwt(aa, &owned.topology, null, 2, &op_skp, op.public(), acct.public(), new_client.public(), responder.public(), service.public(), revocationsOf(before))).?;

    try std.testing.expect(std.mem.find(u8, after, old_client.public()) == null);
    try std.testing.expect(std.mem.find(u8, after, new_client.public()) != null);
    try std.testing.expect(std.mem.find(u8, after, responder.public()) != null);
    try std.testing.expect(std.mem.find(u8, after, service.public()) != null);
    try std.testing.expectEqualStrings(revoked, revocationsOf(after));
    // The store's line moves to the new seed; the rest of the store is left alone.
    const store = try std.fmt.allocPrint(aa, "OPERATOR_SEED={s}\nSK_CLIENT_SEED={s}\n", .{ op.seed(), old_client.seed() });
    const rotated = try setStoreValue(aa, store, "SK_CLIENT_SEED", new_client.seed());
    try std.testing.expectEqualStrings(new_client.seed(), storeValue(rotated, "SK_CLIENT_SEED").?);
    try std.testing.expectEqualStrings(op.seed(), storeValue(rotated, "OPERATOR_SEED").?);
}

/// `bridge --mint-responder --name N [--tenant T]… [--store PATH] [--ttl-days D]`
/// (§10kc): a responder's creds, printed on stdout. A responder is a service that
/// answers `query.<tenant>.<name>` from its replica; it reads like a client and never
/// writes. Its permissions are the responder signing key's TEMPLATE in the account
/// JWT — this names the user and tags its tenants, it cannot widen what the user may
/// do. The signing seed comes from the offline store, so this runs where the store
/// lives, never on the bridge host. One JWT implementation (`jwt_mint.mint`), and no
/// Python package to install (it replaced scripts/native/mint_responder.py).
pub fn mintResponder(io: std.Io, init: *const std.process.Init) u8 {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var store_path: []const u8 = "zb-nats/operator.store";
    var name: ?[]const u8 = null;
    var tenants: std.ArrayList([]const u8) = .empty;
    var ttl_days: i64 = 3650; // a service rotates with a redeploy, not a TTL
    {
        var it = init.minimal.args.iterate();
        _ = it.next();
        while (it.next()) |arg| {
            if (std.mem.eql(u8, arg, "--mint-responder")) continue;
            if (std.mem.eql(u8, arg, "--store")) {
                store_path = it.next() orelse return mintUsage("--store needs a path");
            } else if (std.mem.eql(u8, arg, "--name")) {
                name = it.next() orelse return mintUsage("--name needs a value");
            } else if (std.mem.eql(u8, arg, "--tenant")) {
                const t = it.next() orelse return mintUsage("--tenant needs a value");
                if (t.len == 0 or std.mem.findAny(u8, t, ".*> ") != null) return mintUsage("a tenant is one subject token (no '.', '*', '>', ' ')");
                tenants.append(a, t) catch return 1;
            } else if (std.mem.eql(u8, arg, "--ttl-days")) {
                const v = it.next() orelse return mintUsage("--ttl-days needs a number");
                ttl_days = std.fmt.parseInt(i64, v, 10) catch return mintUsage("--ttl-days needs a number");
                if (ttl_days < 1) return mintUsage("--ttl-days must be at least 1");
            } else return mintUsage("unknown argument");
        }
    }
    const who = name orelse return mintUsage("--name is required");
    // The principal alphabet invites use: a NATS subject token, a KV key and a
    // template expansion all accept it.
    for (who) |ch| if (!(std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '-')) return mintUsage("--name: letters, digits, '_' and '-' only");

    const store = readStore(io, a, store_path) catch {
        out("🔴 cannot read the store {s} (--store PATH, - for standard input)\n", .{store_path});
        return 1;
    };
    const sk_text = storeValue(store, "SK_RESPONDER_SEED") orelse {
        out("🔴 {s} has no SK_RESPONDER_SEED — is it an operator.store?\n", .{store_path});
        return 1;
    };
    const acct_text = storeValue(store, "ACCOUNT_SEED") orelse {
        out("🔴 {s} has no ACCOUNT_SEED — is it an operator.store?\n", .{store_path});
        return 1;
    };
    const sk = keyFromSeed(sk_text) catch {
        out("🔴 SK_RESPONDER_SEED in {s} is not a valid seed\n", .{store_path});
        return 1;
    };
    const acct = keyFromSeed(acct_text) catch {
        out("🔴 ACCOUNT_SEED in {s} is not a valid seed\n", .{store_path});
        return 1;
    };
    const user = genKey(io, .user) catch return 1;
    const now: i64 = @intCast(c.time(null));
    const jwt = jwt_mint.mint(a, sk.seed(), acct.public(), who, tenants.items, user.public(), ttl_days * 24 * 3600, now) catch |err| {
        out("🔴 mint failed: {}\n", .{err});
        return 1;
    };
    const creds = credsFile(a, jwt, user.seed()) catch return 1;
    std.Io.File.stdout().writeStreamingAll(io, creds) catch return 1;
    out("✅ responder '{s}' minted ({d} tenant tag(s), {d} days) — keep the creds file like any secret\n", .{ who, tenants.items.len, ttl_days });
    out("   user key {s} — file it with the creds: `bridge --revoke --key` revokes this identity\n", .{user.public()});
    return 0;
}

fn mintUsage(msg: []const u8) u8 {
    out("🔴 {s}\n  bridge --mint-responder --name NAME [--tenant T]... [--store PATH] [--ttl-days D] > NAME.creds\n", .{msg});
    return 1;
}

/// `bridge --mint-leaf --name N [--store PATH] [--ttl-days D]` (§10ll): the creds a leaf
/// node's remote presents to the hub. Not a scoped user: signed by the account key, its
/// permissions in its own JWT — what a client or a responder may do, for any tenant and
/// any name (each template becomes `*`). The leaf carries its devices' traffic and
/// nothing else: no stream admin, no `cdc.*` or generation writes, which the bridge's
/// own creds (`>`) handed to a second machine. Runs where the store lives; a grammar
/// change means minting again, like a responder's.
pub fn mintLeaf(io: std.Io, init: *const std.process.Init) u8 {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var store_path: []const u8 = "zb-nats/operator.store";
    var name: ?[]const u8 = null;
    var ttl_days: i64 = 3650; // a leaf rotates with a redeploy, not a TTL
    {
        var it = init.minimal.args.iterate();
        _ = it.next();
        while (it.next()) |arg| {
            if (std.mem.eql(u8, arg, "--mint-leaf")) continue;
            if (std.mem.eql(u8, arg, "--store")) {
                store_path = it.next() orelse return mintLeafUsage("--store needs a path");
            } else if (std.mem.eql(u8, arg, "--name")) {
                name = it.next() orelse return mintLeafUsage("--name needs a value");
            } else if (std.mem.eql(u8, arg, "--ttl-days")) {
                const v = it.next() orelse return mintLeafUsage("--ttl-days needs a number");
                ttl_days = std.fmt.parseInt(i64, v, 10) catch return mintLeafUsage("--ttl-days needs a number");
                if (ttl_days < 1) return mintLeafUsage("--ttl-days must be at least 1");
            } else return mintLeafUsage("unknown argument");
        }
    }
    const who = name orelse return mintLeafUsage("--name is required");
    for (who) |ch| if (!(std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '-')) return mintLeafUsage("--name: letters, digits, '_' and '-' only");

    const store = readStore(io, a, store_path) catch {
        out("🔴 cannot read the store {s} (--store PATH, - for standard input)\n", .{store_path});
        return 1;
    };
    const acct_text = storeValue(store, "ACCOUNT_SEED") orelse {
        out("🔴 {s} has no ACCOUNT_SEED — is it an operator.store?\n", .{store_path});
        return 1;
    };
    const acct = keyFromSeed(acct_text) catch {
        out("🔴 ACCOUNT_SEED in {s} is not a valid seed\n", .{store_path});
        return 1;
    };
    const domain = storeValue(store, "JS_DOMAIN") orelse "";
    const js_domain: ?[]const u8 = if (domain.len == 0) null else domain;

    const owned = topology_mod.loadEmbedded(a) catch return 1;
    const allows = leafAllows(a, &owned.topology, js_domain) catch return 1;
    const user = genKey(io, .user) catch return 1;
    const now: i64 = @intCast(c.time(null));
    const claims = std.fmt.allocPrint(
        a,
        "{{\"jti\":\"__JTI__\",\"iat\":{d},\"exp\":{d},\"iss\":\"{s}\",\"name\":\"{s}\",\"sub\":\"{s}\",\"nats\":{{" ++
            "\"pub\":{{\"allow\":[{s}]}},\"sub\":{{\"allow\":[{s}]}},\"subs\":-1,\"data\":-1,\"payload\":-1," ++
            "\"type\":\"user\",\"version\":2}}}}",
        .{ now, now + ttl_days * 24 * 3600, acct.public(), who, user.public(), allows.pub_json, allows.sub_json },
    ) catch return 1;
    var acct_seed_kp = nats.nkeys.SeedKeyPair.fromSeed(acct.seed()) catch return 1;
    defer acct_seed_kp.wipe();
    const jwt = jwt_mint.signClaims(a, &acct_seed_kp, claims, "__JTI__") catch |err| {
        out("🔴 mint failed: {}\n", .{err});
        return 1;
    };
    const creds = credsFile(a, jwt, user.seed()) catch return 1;
    std.Io.File.stdout().writeStreamingAll(io, creds) catch return 1;
    out("✅ leaf '{s}' minted ({s}, {d} days) — keep the creds file like any secret\n", .{ who, if (js_domain) |d| d else "no JetStream domain", ttl_days });
    out("   user key {s} — file it with the creds: `bridge --revoke --key` revokes this identity\n", .{user.public()});
    return 0;
}

fn mintLeafUsage(msg: []const u8) u8 {
    out("🔴 {s}\n  bridge --mint-leaf --name NAME [--store PATH] [--ttl-days D] > NAME.creds\n", .{msg});
    return 1;
}

/// What a leaf link may carry: the client's grants for any principal. A NATS wildcard is a
/// whole token, so a token holding a template (`CDC_{{tag(tenant)}}`) becomes `*`: reads
/// (consumers, info, direct gets) open to every stream, while writes, admin and CDC stay
/// shut. Clients only: a responder's `STREAM.CREATE.OBJ_res-…` would become "create any
/// stream", and no responder runs behind a leaf yet.
fn leafAllows(a: std.mem.Allocator, topo: *const topology_mod.Topology, js_domain: ?[]const u8) !struct { pub_json: []u8, sub_json: []u8 } {
    const cl = try roleAllowsFor(a, topo, .client, js_domain);
    // The hub announces a subscription to the leaf only when the leaf may publish to its
    // subject AS WRITTEN: the MUTATIONS stream listens on `mutation.>`, which
    // `mutation.*.>` does not cover, so without this line no write left the leaf (§10ll).
    // Each device's own JWT still holds it to `mutation.<itself>.>`.
    const stream_subjects = try std.fmt.allocPrint(a, "\"{s}.>\"", .{topo.subject_mutations_prefix});
    return .{
        .pub_json = try pruneCovered(a, try mergeJsonLists(a, try anyPrincipal(a, cl.pub_json), stream_subjects)),
        .sub_json = try pruneCovered(a, try anyPrincipal(a, cl.sub_json)),
    };
}

/// The list without the subjects a broader one already covers (`X.CDC_PUBLIC` under `X.*`):
/// the whole grant rides in the JWT, and every connect sends the JWT.
fn pruneCovered(a: std.mem.Allocator, list: []const u8) ![]u8 {
    var items: std.ArrayList([]const u8) = .empty;
    var it = std.mem.splitScalar(u8, list, ',');
    while (it.next()) |item| if (item.len >= 2) try items.append(a, item[1 .. item.len - 1]);
    var buf: std.ArrayList(u8) = .empty;
    for (items.items, 0..) |x, i| {
        const covered = for (items.items, 0..) |y, j| {
            if (i != j and !std.mem.eql(u8, x, y) and covers(y, x)) break true;
        } else false;
        if (covered) continue;
        if (buf.items.len > 0) try buf.append(a, ',');
        try buf.print(a, "\"{s}\"", .{x});
    }
    return buf.toOwnedSlice(a);
}

/// Whether the pattern `y` matches every subject the pattern `x` matches (NATS tokens).
fn covers(y: []const u8, x: []const u8) bool {
    var yt = std.mem.splitScalar(u8, y, '.');
    var xt = std.mem.splitScalar(u8, x, '.');
    while (yt.next()) |ytok| {
        const xtok = xt.next() orelse return false;
        if (std.mem.eql(u8, ytok, ">")) return true;
        if (std.mem.eql(u8, xtok, ">")) return false;
        if (std.mem.eql(u8, ytok, "*") or std.mem.eql(u8, ytok, xtok)) continue;
        return false;
    }
    return xt.next() == null;
}

test "covers: NATS token rules" {
    try std.testing.expect(covers("a.*", "a.b"));
    try std.testing.expect(covers("a.>", "a.b.c"));
    try std.testing.expect(covers("a.*.>", "a.b.>"));
    try std.testing.expect(!covers("a.*.>", "a.>"));
    try std.testing.expect(!covers("a.*", "a.>"));
    try std.testing.expect(!covers("a.*", "a.b.c"));
    try std.testing.expect(!covers("a.b", "a.*"));
}

/// Every token holding a template becomes `*` (a `"a","b"` list in, the same out, deduplicated).
fn anyPrincipal(a: std.mem.Allocator, list: []const u8) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    var items = std.mem.splitScalar(u8, list, ',');
    while (items.next()) |item| {
        if (item.len < 2) continue;
        if (buf.items.len > 0) try buf.append(a, ',');
        try buf.append(a, '"');
        var tokens = std.mem.splitScalar(u8, item[1 .. item.len - 1], '.');
        var first = true;
        while (tokens.next()) |tok| {
            if (!first) try buf.append(a, '.');
            first = false;
            try buf.appendSlice(a, if (std.mem.find(u8, tok, "{{") != null) "*" else tok);
        }
        try buf.append(a, '"');
    }
    return mergeJsonLists(a, buf.items, "");
}

test "a leaf link carries what its clients and responders may, for anyone, and no more" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const owned = try topology_mod.loadEmbedded(aa);
    const l = try leafAllows(aa, &owned.topology, "hub");
    // pruned: what `*` covers is not listed again, and the token stays small
    try std.testing.expect(std.mem.find(u8, l.pub_json, "CONSUMER.CREATE.CDC_PUBLIC") == null);
    try std.testing.expect(l.pub_json.len + l.sub_json.len < 2000);
    // no template left, no blanket grant
    try std.testing.expect(std.mem.find(u8, l.pub_json, "{{") == null);
    try std.testing.expect(std.mem.find(u8, l.sub_json, "{{") == null);
    try std.testing.expect(std.mem.find(u8, l.pub_json, "\">\"") == null);
    // the heartbeat of any client, through the domain and bare
    try std.testing.expect(std.mem.find(u8, l.pub_json, "\"$JS.hub.API.$KV.live.*.*\"") != null);
    try std.testing.expect(std.mem.find(u8, l.pub_json, "\"$KV.live.*.*\"") != null);
    // the MUTATIONS stream's own subject, so the hub announces it across the link
    try std.testing.expect(std.mem.find(u8, l.pub_json, "\"mutation.>\"") != null);
    // a template inside a token widens the whole token: no `CDC_*` (a literal name to NATS)
    try std.testing.expect(std.mem.find(u8, l.pub_json, "_*") == null);
    try std.testing.expect(std.mem.find(u8, l.pub_json, "\"$JS.hub.API.CONSUMER.MSG.NEXT.*.>\"") != null);
    // nobody on a leaf writes CDC, creates or deletes a stream
    try std.testing.expect(std.mem.find(u8, l.pub_json, "STREAM.CREATE") == null);
    try std.testing.expect(std.mem.find(u8, l.pub_json, "\"cdc.") == null);
    try std.testing.expect(std.mem.find(u8, l.pub_json, "STREAM.DELETE") == null);
}
