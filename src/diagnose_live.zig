//! `bridge --diagnose`, the part that looks beyond this process's own configuration:
//! the catalogue against the tables, the replication slots, the running bridge if there
//! is one, NATS, and what a fresh client would find there. Everything is READ-ONLY:
//! SELECTs, one GET on the bridge's own HTTP port, and in NATS stream info, KV gets and
//! object info. Nothing is created, edited or deleted.
//!
//! Two situations, one doctor: with the bridge STOPPED it says what a start would meet;
//! with it RUNNING it adds the runtime posture and the client contract (every table has
//! a schema, every principal a tenant entry, every chain its object). What depends on a
//! running bridge is skipped, and said to be, when none answers.

const std = @import("std");
const nats = @import("nats");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const Conf = @import("config.zig");
const nats_publisher = @import("nats_publisher.zig");
const topology_mod = @import("topology.zig");

const log = std.log.scoped(.diagnose);

pub const Inputs = struct {
    conn: *c.PGconn,
    /// init.core / init.write are applied (the caller checked); the catalogue checks need them.
    core_ok: bool,
    write_ok: bool,
    slot_name: []const u8,
    bridge_port: u16,
    /// Null when NATS_URL could not be read: reported, and the NATS checks skipped.
    endpoint: ?Conf.Nats.Endpoint,
    topo: *const topology_mod.Topology,
};

/// Returns the number of findings. Warnings are printed and not counted.
pub fn run(allocator: std.mem.Allocator, io: std.Io, in: Inputs) usize {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var findings: usize = 0;

    if (in.core_ok) findings += catalogueChecks(a, in.conn);
    const running = bridgeStatus(a, io, in.bridge_port, &findings);
    findings += slotChecks(a, in.conn, in.slot_name, running);

    const endpoint = in.endpoint orelse {
        findings += 1;
        log.err("🔴 NATS_URL could not be read: the NATS checks are skipped, and a bridge would not start", .{});
        return findings;
    };
    var publisher = nats_publisher.Publisher.init(allocator, .{ .endpoint = endpoint, .max_reconnect_attempts = 0 }, io) catch {
        findings += 1;
        log.err("🔴 could not set up a NATS client", .{});
        return findings;
    };
    defer publisher.deinit();
    publisher.connect() catch |err| {
        findings += 1;
        if (endpoint.user == null and endpoint.seed == null) {
            log.err("🔴 NATS refused at {s}:{d} ({s}): this environment carries no NATS credentials. NATS_CREDS is not set: load .env.nats in the same shell (sudo starts a clean environment, and the file is readable by its owner only).", .{ endpoint.host, endpoint.port, @errorName(err) });
        } else {
            log.err("🔴 NATS is not reachable at {s}:{d} ({s}) with this environment's credentials: a bridge started now would not connect. Check NATS_URL and NATS_CREDS in .env.nats.", .{ endpoint.host, endpoint.port, @errorName(err) });
        }
        return findings;
    };
    const js = publisher.js orelse return findings;
    log.info("✅ NATS answers at {s}:{d} with this environment's credentials", .{ endpoint.host, endpoint.port });

    const tenants = mappedTenants(a, in.conn, in.write_ok);
    findings += topologyChecks(a, js, in.topo, tenants, running);

    if (!running) {
        log.info("ℹ️ the bridge is not running: the client contract (schemas, tenant entries, chains) is checked when it is", .{});
        return findings;
    }
    if (in.core_ok) findings += clientContract(a, js, in.conn, in.topo, tenants, in.write_ok);
    return findings;
}

// ── 6. the catalogue against the tables ────────────────────────────────────────
// zebridge_check_all(): every catalogued table against its own catalogue row, the
// intent the DBA wrote with zebridge_enable. The same questions the boot's preflight
// asks, answered by the database.
fn catalogueChecks(a: std.mem.Allocator, conn: *c.PGconn) usize {
    const res = c.PQexec(conn, "SELECT tbl, check_name, status, detail FROM public.zebridge_check_all() ORDER BY tbl, check_name");
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        log.err("🔴 zebridge_check_all() failed: {s} — re-apply the init SQL (bridge --init-sql)", .{std.mem.span(c.PQresultErrorMessage(res))});
        return 1;
    }
    var errors: usize = 0;
    var warnings: usize = 0;
    var tables = std.StringHashMapUnmanaged(void).empty;
    const n: usize = @intCast(c.PQntuples(res));
    for (0..n) |i| {
        const row: c_int = @intCast(i);
        const tbl = std.mem.span(c.PQgetvalue(res, row, 0));
        const check = std.mem.span(c.PQgetvalue(res, row, 1));
        const status = std.mem.span(c.PQgetvalue(res, row, 2));
        const detail = std.mem.span(c.PQgetvalue(res, row, 3));
        tables.put(a, tbl, {}) catch {};
        if (std.mem.eql(u8, check, "summary")) continue;
        if (std.mem.eql(u8, status, "ERROR")) {
            errors += 1;
            log.err("🔴 '{s}' {s}: {s}", .{ tbl, check, detail });
        } else if (std.mem.eql(u8, status, "WARNING")) {
            warnings += 1;
            log.warn("⚠️ '{s}' {s}: {s}", .{ tbl, check, detail });
        }
    }
    if (errors == 0) log.info("✅ catalogue: {d} table(s) match their catalogue rows ({d} warning(s) above)", .{ tables.count(), warnings });
    return errors + crossTableAudits(conn);
}

/// What no single table's check can see: writes nothing bounds, tenants the sweeper
/// cannot reach, and the bridge instances registered in zebridge_limits.
fn crossTableAudits(conn: *c.PGconn) usize {
    var findings: usize = 0;

    // A tenant-scoped table with nothing bounding WRITES lets a principal forge another
    // tenant's rows. An unscoped publication is expected (the per-tenant stream bounds
    // reads: "CDC UNSCOPED"), and a catalogue-public table is unscoped by declaration.
    {
        const res = c.PQexec(conn, "SELECT a.publication, a.tbl, a.verdict FROM public.zebridge_audit_publications() a " ++
            "JOIN public.zebridge_catalogue cat ON cat.tbl = split_part(a.tbl, '.', 2) " ++
            "WHERE cat.tenant_col IS NOT NULL AND NOT public.zebridge_is_internal_table(cat.tbl) " ++
            "AND a.verdict NOT LIKE 'scoped%' AND a.verdict NOT LIKE 'internal%' AND a.verdict NOT LIKE 'CDC UNSCOPED%' ORDER BY 2");
        defer c.PQclear(res);
        if (c.PQresultStatus(res) == c.PGRES_TUPLES_OK) {
            const n: usize = @intCast(c.PQntuples(res));
            for (0..n) |i| {
                const row: c_int = @intCast(i);
                findings += 1;
                log.err("🔴 '{s}' (publication '{s}') is tenant-scoped but nothing bounds its writes: {s}. Re-run zebridge_enable(..., tenant_col => ...), which installs RLS and the write policy.", .{
                    std.mem.span(c.PQgetvalue(res, row, 1)), std.mem.span(c.PQgetvalue(res, row, 0)), std.mem.span(c.PQgetvalue(res, row, 2)),
                });
            }
            if (n == 0) log.info("✅ every tenant-scoped table has its writes bounded", .{});
        }
    }

    // A tenant the sweeper cannot reach keeps its tombstones for ever: the offline
    // window stops being bounded there.
    {
        const res = c.PQexec(conn, "SELECT tbl, tenant_id, verdict FROM public.zebridge_audit_sweeper() " ++
            "WHERE lower(verdict) NOT LIKE 'ok%' AND lower(verdict) NOT LIKE 'reachable%' ORDER BY 1, 2");
        defer c.PQclear(res);
        if (c.PQresultStatus(res) == c.PGRES_TUPLES_OK) {
            const n: usize = @intCast(c.PQntuples(res));
            for (0..n) |i| {
                const row: c_int = @intCast(i);
                log.warn("⚠️ the sweeper cannot reach '{s}' for tenant '{s}': {s}. Tombstones there are never reaped.", .{
                    std.mem.span(c.PQgetvalue(res, row, 0)), std.mem.span(c.PQgetvalue(res, row, 1)), std.mem.span(c.PQgetvalue(res, row, 2)),
                });
            }
        }
    }

    // The bridge instances that registered here, what their publications carry, and
    // whether they agree on the row budget (a shared table is guarded at the minimum).
    {
        const res = c.PQexec(conn, "SELECT l.slot, l.max_row_bytes::text, count(DISTINCT pt.tablename)::text " ++
            "FROM public.zebridge_limits l LEFT JOIN pg_publication_tables pt ON pt.pubname = l.publication " ++
            "GROUP BY 1, 2 ORDER BY 1");
        defer c.PQclear(res);
        if (c.PQresultStatus(res) == c.PGRES_TUPLES_OK) {
            const n: usize = @intCast(c.PQntuples(res));
            var first_budget: []const u8 = "";
            var disagree = false;
            for (0..n) |i| {
                const row: c_int = @intCast(i);
                const slot = std.mem.span(c.PQgetvalue(res, row, 0));
                const budget = std.mem.span(c.PQgetvalue(res, row, 1));
                const ntables = std.mem.span(c.PQgetvalue(res, row, 2));
                if (std.mem.eql(u8, ntables, "0")) {
                    findings += 1;
                    log.err("🔴 the instance on slot '{s}' replicates a publication with NO tables: it boots and delivers nothing. Add tables with zebridge_enable(..., publication => ...).", .{slot});
                }
                if (i == 0) first_budget = budget else if (!std.mem.eql(u8, budget, first_budget)) disagree = true;
            }
            if (disagree) log.warn("⚠️ the registered bridge instances disagree on the row budget: a shared table is guarded at the smallest", .{});
        }
    }
    return findings;
}

// ── 7. the replication slots ───────────────────────────────────────────────────
// An invalidated slot cannot resume. An inactive one keeps WAL for everyone, up to
// max_slot_wal_keep_size. This bridge's own slot is inactive exactly when it is stopped.
fn slotChecks(a: std.mem.Allocator, conn: *c.PGconn, own: []const u8, running: bool) usize {
    _ = a;
    const res = c.PQexec(conn, "SELECT slot_name, active::text, coalesce(wal_status, '?'), " ++
        "coalesce(pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), '?') " ++
        "FROM pg_replication_slots WHERE slot_type = 'logical' ORDER BY 1");
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        log.warn("⚠️ pg_replication_slots unreadable: {s}", .{std.mem.span(c.PQresultErrorMessage(res))});
        return 0;
    }
    var findings: usize = 0;
    var own_seen = false;
    const n: usize = @intCast(c.PQntuples(res));
    for (0..n) |i| {
        const row: c_int = @intCast(i);
        const name = std.mem.span(c.PQgetvalue(res, row, 0));
        const active = c.PQgetvalue(res, row, 1)[0] == 't';
        const wal_status = std.mem.span(c.PQgetvalue(res, row, 2));
        const retained = std.mem.span(c.PQgetvalue(res, row, 3));
        const mine = std.mem.eql(u8, name, own);
        if (mine) own_seen = true;
        if (!std.mem.eql(u8, wal_status, "reserved") and !std.mem.eql(u8, wal_status, "extended")) {
            findings += 1;
            log.err("🔴 slot '{s}' is {s}: it cannot resume{s}", .{ name, wal_status, if (mine) " — this bridge must start once with ZB_FEED_RESTART=1, and its clients re-seed" else "" });
        } else if (mine) {
            if (active) {
                log.info("✅ this bridge's slot '{s}' is active, holding {s} of WAL", .{ name, retained });
            } else if (running) {
                findings += 1;
                log.err("🔴 the bridge answers but its slot '{s}' is INACTIVE ({s} retained): no change is flowing", .{ name, retained });
            } else {
                log.info("ℹ️ this bridge's slot '{s}' is inactive, as the bridge is stopped: it holds {s} of WAL until the bridge resumes", .{ name, retained });
            }
        } else if (!active) {
            log.warn("⚠️ slot '{s}' is inactive, holding {s} of WAL: if nothing will resume it, drop it (bridge --drop-slot {s})", .{ name, retained, name });
        }
    }
    if (!own_seen) log.info("ℹ️ slot '{s}' does not exist yet: the bridge creates it at its first start", .{own});
    return findings;
}

// ── 8. is the bridge running? ──────────────────────────────────────────────────
// Its own /status, on loopback (BRIDGE_BIND binds loopback by default, and 0.0.0.0
// includes it). A refused connection means no bridge on that port.
fn bridgeStatus(a: std.mem.Allocator, io: std.Io, port: u16, findings: *usize) bool {
    const url = std.fmt.allocPrint(a, "http://127.0.0.1:{d}/status", .{port}) catch return false;
    var client: std.http.Client = .{ .allocator = a, .io = io };
    defer client.deinit();
    var body: std.Io.Writer.Allocating = .init(a);
    const res = client.fetch(.{ .location = .{ .url = url }, .response_writer = &body.writer, .keep_alive = false }) catch {
        log.info("ℹ️ no bridge answers on port {d}: checking what a start would meet", .{port});
        return false;
    };
    if (res.status != .ok) {
        findings.* += 1;
        log.err("🔴 something answers on port {d} but /status said {d}: is it this bridge?", .{ port, @intFromEnum(res.status) });
        return false;
    }
    const parsed = std.json.parseFromSliceLeaky(std.json.Value, a, body.written(), .{}) catch {
        log.warn("⚠️ the bridge's /status is not JSON: runtime posture skipped", .{});
        return true;
    };
    const o = if (parsed == .object) parsed.object else return true;
    const flag = struct {
        fn get(m: std.json.ObjectMap, k: []const u8) bool {
            const v = m.get(k) orelse return false;
            return v == .bool and v.bool;
        }
        fn int(m: std.json.ObjectMap, k: []const u8) i64 {
            const v = m.get(k) orelse return 0;
            return if (v == .integer) v.integer else 0;
        }
    };
    log.info("✅ the bridge is running on port {d}", .{port});
    if (!flag.get(o, "is_connected")) {
        findings.* += 1;
        log.err("🔴 the running bridge is NOT connected to NATS", .{});
    }
    const refused = flag.int(o, "refused_tables");
    if (refused > 0) log.warn("⚠️ {d} table(s) suspended by the bridge ({d} event(s) dropped): their clients are frozen; the reason is in $KV.schemas.<table> and the bridge log", .{ refused, flag.int(o, "refused_events_dropped") });
    const pg_rc = flag.int(o, "pg_reconnect_count");
    const nats_rc = flag.int(o, "nats_reconnect_count");
    if (pg_rc > 0 or nats_rc > 0) log.warn("⚠️ reconnects since boot: PostgreSQL {d}, NATS {d}", .{ pg_rc, nats_rc });
    return true;
}

/// The tenants that have a principal mapped to them: the ones with CDC streams and
/// chains clients follow. Empty without init.write.
fn mappedTenants(a: std.mem.Allocator, conn: *c.PGconn, write_ok: bool) []const []const u8 {
    if (!write_ok) return &.{};
    const res = c.PQexec(conn, "SELECT DISTINCT tenant_id FROM public.zebridge_user_tenants ORDER BY 1");
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) return &.{};
    const n: usize = @intCast(c.PQntuples(res));
    const out = a.alloc([]const u8, n) catch return &.{};
    for (0..n) |i| out[i] = a.dupe(u8, std.mem.span(c.PQgetvalue(res, @intCast(i), 0))) catch "";
    return out;
}

// ── 9. NATS: the streams and buckets ───────────────────────────────────────────
// The bridge creates them at boot, so with it stopped a missing one is a note; with
// it running, a finding.
fn topologyChecks(a: std.mem.Allocator, js: nats.JetStream, topo: *const topology_mod.Topology, tenants: []const []const u8, running: bool) usize {
    var findings: usize = 0;
    var wanted: std.ArrayList([]const u8) = .empty;
    wanted.append(a, topo.cdc_stream_public) catch {};
    for (tenants) |t| {
        // The open tenant's rows ride CDC_PUBLIC: the bridge skips it when it creates the
        // tenant streams (a `_default` invite maps someone into it), and so does this check.
        if (std.mem.eql(u8, t, topo.open_tenant)) continue;
        wanted.append(a, std.fmt.allocPrint(a, "{s}{s}", .{ topo.cdc_stream_prefix, t }) catch continue) catch {};
    }
    wanted.append(a, topo.stream_mutations) catch {};
    wanted.append(a, topo.stream_verdicts) catch {};
    for ([_][]const u8{ topo.kv_schemas, topo.kv_tenants, topo.kv_generations }) |b|
        wanted.append(a, std.fmt.allocPrint(a, "KV_{s}", .{b}) catch continue) catch {};

    var missing: usize = 0;
    for (wanted.items) |name| {
        var info = js.getStreamInfo(name) catch {
            missing += 1;
            if (running) {
                findings += 1;
                log.err("🔴 stream {s} is missing while the bridge runs", .{name});
            } else {
                log.info("ℹ️ stream {s} does not exist yet: the bridge creates it at boot", .{name});
            }
            continue;
        };
        defer info.deinit();
        const cfg = info.value.config;
        if (std.mem.eql(u8, name, topo.stream_mutations)) {
            if (cfg.discard != .new or cfg.max_msgs_per_subject <= 0 or !(cfg.discard_new_per_subject orelse false)) {
                findings += 1;
                log.err("🔴 {s} lacks its per-principal backlog cap (discard {s}, {d} per subject, per-subject discard {}): a flooding client evicts honest queued writes. The bridge creates it right; an older one must be recreated.", .{ name, @tagName(cfg.discard), cfg.max_msgs_per_subject, cfg.discard_new_per_subject orelse false });
            }
        } else if (std.mem.eql(u8, name, topo.stream_verdicts)) {
            if (cfg.discard != .old) log.warn("⚠️ {s} discards NEW when full: a burst of verdicts then drops the newest, and a revocation ban with them", .{name});
        }
    }
    if (missing == 0) log.info("✅ NATS: every stream and bucket exists ({d}: the public CDC stream, {d} tenant stream(s), MUTATIONS, VERDICTS, 3 KV buckets)", .{ wanted.items.len, tenants.len });
    return findings;
}

// ── 10. the client contract (bridge running) ───────────────────────────────────
// What a fresh client asks for: a schema per table, a tenant entry per principal, and
// a chain whose full object exists per (tenant, table) it may seed. Per-key gets only:
// listing would need consumer rights the auditor should not hold.
fn clientContract(a: std.mem.Allocator, js: nats.JetStream, conn: *c.PGconn, topo: *const topology_mod.Topology, tenants: []const []const u8, write_ok: bool) usize {
    var findings: usize = 0;
    const res = c.PQexec(conn, "SELECT tbl, coalesce(tenant_col::text, ''), generations::text FROM public.zebridge_catalogue ORDER BY tbl");
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) return 0;
    const n: usize = @intCast(c.PQntuples(res));

    // schemas
    var schemas = js.kvBucket(topo.kv_schemas) catch {
        log.err("🔴 KV {s} cannot be opened", .{topo.kv_schemas});
        return 1;
    };
    defer schemas.deinit();
    var no_schema: usize = 0;
    for (0..n) |i| {
        const tbl = std.mem.span(c.PQgetvalue(res, @intCast(i), 0));
        if (kvHas(&schemas, tbl)) continue;
        no_schema += 1;
        findings += 1;
        log.err("🔴 '{s}' has no published schema: a client cannot even build its table. Look for a refusal naming it in the bridge log.", .{tbl});
    }
    if (no_schema == 0) log.info("✅ every catalogue table has a published schema ({d})", .{n});

    // tenant entries
    if (write_ok) {
        var tkv = js.kvBucket(topo.kv_tenants) catch null;
        if (tkv) |*kv| {
            defer kv.deinit();
            const pres = c.PQexec(conn, "SELECT DISTINCT principal FROM public.zebridge_user_tenants ORDER BY 1");
            defer c.PQclear(pres);
            if (c.PQresultStatus(pres) == c.PGRES_TUPLES_OK) {
                const np: usize = @intCast(c.PQntuples(pres));
                var lacking: usize = 0;
                for (0..np) |i| {
                    const p = std.mem.span(c.PQgetvalue(pres, @intCast(i), 0));
                    if (kvHas(kv, p)) continue;
                    lacking += 1;
                    findings += 1;
                    log.err("🔴 principal '{s}' has no $KV.{s} entry: its client cannot resolve its tenants and reads nothing", .{ p, topo.kv_tenants });
                }
                if (lacking == 0) log.info("✅ every mapped principal has its tenant entry ({d})", .{np});
            }
        }
    }

    // chains
    var gens = js.kvBucket(topo.kv_generations) catch {
        log.err("🔴 KV {s} cannot be opened", .{topo.kv_generations});
        return findings + 1;
    };
    defer gens.deinit();
    var osm = js.objectStoreManager();
    var checked: usize = 0;
    var problems: usize = 0;
    for (0..n) |i| {
        const tbl = std.mem.span(c.PQgetvalue(res, @intCast(i), 0));
        const tenant_col = std.mem.span(c.PQgetvalue(res, @intCast(i), 1));
        if (c.PQgetvalue(res, @intCast(i), 2)[0] != 't') continue;
        const scoped = tenant_col.len > 0;
        const owners: []const []const u8 = if (scoped) tenants else &.{topo.open_tenant};
        for (owners) |tenant| {
            const key = std.fmt.allocPrint(a, "{s}.{s}", .{ tenant, tbl }) catch continue;
            var entry = gens.get(key) catch {
                if (scoped and !tenantHasRows(a, conn, tbl, tenant_col, tenant)) continue;
                problems += 1;
                findings += 1;
                log.err("🔴 no chain for {s}: a fresh client cannot seed it. A table enabled within one GENERATION_CADENCE_SECONDS is legitimately here; otherwise check GENERATIONS_ENABLED.", .{key});
                continue;
            };
            defer entry.deinit();
            if (entry.isDeleted()) continue;
            checked += 1;
            const man = std.json.parseFromSliceLeaky(std.json.Value, a, entry.value, .{}) catch {
                problems += 1;
                findings += 1;
                log.err("🔴 chain manifest {s} is not readable JSON", .{key});
                continue;
            };
            const full = if (man == .object) man.object.get("full") else null;
            const obj = if (full) |f| (if (f == .object) f.object.get("object") else null) else null;
            if (obj == null or obj.? != .string) continue;
            const bucket = std.fmt.allocPrint(a, "{s}{s}", .{ topo.generation_bucket_prefix, tenant }) catch continue;
            var store = osm.openStore(bucket) catch {
                problems += 1;
                findings += 1;
                log.err("🔴 chain {s} names object store {s}, which does not exist", .{ key, bucket });
                continue;
            };
            defer store.deinit();
            var oi = store.info(obj.?.string) catch {
                problems += 1;
                findings += 1;
                log.err("🔴 chain {s} points at {s}/{s}, which is missing: the chain was half-cleaned. Clear both sides (DELETE FROM zebridge_generations WHERE tbl = '{s}') and let the next cut rebuild it.", .{ key, bucket, obj.?.string, tbl });
                continue;
            };
            oi.deinit();
        }
    }
    if (problems == 0) log.info("✅ every chain a client may seed has its full object ({d})", .{checked});
    return findings;
}

fn kvHas(kv: anytype, key: []const u8) bool {
    var entry = kv.get(key) catch return false;
    defer entry.deinit();
    return !entry.isDeleted();
}

/// A mapped tenant with no row in a tenant-scoped table has nothing to seed: no chain
/// is expected for it. Asked of the database, every name passed as a parameter.
fn tenantHasRows(a: std.mem.Allocator, conn: *c.PGconn, tbl: []const u8, tenant_col: []const u8, tenant: []const u8) bool {
    const reg = std.fmt.allocPrintSentinel(a, "public.{s}", .{tbl}, 0) catch return true;
    const col = a.dupeSentinel(u8, tenant_col, 0) catch return true;
    const tz = a.dupeSentinel(u8, tenant, 0) catch return true;
    const params = [_]?[*:0]const u8{ reg.ptr, col.ptr, tz.ptr };
    const res = c.PQexecParams(conn, "SELECT $3 = ANY (ARRAY(SELECT t::text FROM public.zebridge_tenants_of(quote_ident($1)::regclass, $2::name) t))", 3, null, &params[0], null, null, 0);
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK or c.PQntuples(res) != 1) return true;
    return c.PQgetvalue(res, 0, 0)[0] == 't';
}
