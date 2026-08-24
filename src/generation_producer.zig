//! Generation producer — delta generations (NOTES.md §1.13, milestones 2 + 3).
//!
//! On a cadence, for each (table, tenant) pair in `GENERATION_RULES`, build the next
//! generation. Every generation after the first carries a **delta** — the rows whose
//! version moved past the previous cutoff, minus the clamp margin — and a **full** is
//! built at generation 1 and refreshed whenever the last one would age out of the kept
//! window, so the manifest's jump-in point never dangles. The producer's own memory is
//! `zebridge_generations` in PostgreSQL, never NATS read back ("the bridge never reads
//! its own output back").
//!
//! Artifacts per generation N of table T in bucket `gen-<tenant>`:
//!
//!   `T-gN-delta`   msgpack `{columns, rows, gen, kind, cutoff, prev_cutoff}` — rows
//!                  `WHERE version > prev_cutoff − margin`. The overlap is deliberate:
//!                  duplicates are absorbed by the client's version-guarded upsert,
//!                  gaps are unrecoverable. Tombstones ride as rows on tables whose
//!                  deletes are soft.
//!   `T-gN-full`    same shape, unfiltered, `kind: "full"` — only when (re)built.
//!
//! The KV pointer (`generations` bucket, key `<tenant>.<table>`) is the **chain
//! manifest**, swapped last: current gen/cutoffs, the full to jump in at, and the kept
//! deltas with their `(prev_cutoff, cutoff]` bounds. A client tracks its watermark —
//! never gen numbers: it applies, in order, every delta whose cutoff is past its
//! watermark, provided the oldest one reaches back to it; otherwise it takes the full
//! and the deltas after it. A pruned object between manifest read and fetch is a 404,
//! and the answer is re-read the manifest — fall back to the full — never a gap.
//!
//! The recipe, overlap-never-gap (proven by `scripts/scenarios/generations.py` for the
//! role and `genproducer.py` for this loop):
//!
//!   1. `SELECT pg_current_wal_lsn()` BEFORE the snapshot — the race window produces
//!      duplicates, never gaps;
//!   2. `BEGIN ISOLATION LEVEL REPEATABLE READ` + `set_config(zb.tenant, …)` — content
//!      scoped by the same RLS policy snapshots use;
//!   3. `cutoff_version` captured inside that transaction (`now()` = txn start), both
//!      content queries against the same snapshot;
//!   4. objects put (immutable, new names per generation), THEN the manifest, and only
//!      then the bookkeeping row — a crash anywhere in between leaves no row, so the
//!      next tick rebuilds the same generation: duplicate work, never a dangling
//!      pointer (the first live tick of milestone 2 proved the other order wrong);
//!   5. prune past the chain depth: PG rows first (the authority), then the objects
//!      they named.
//!
//! Skip-if-unchanged keeps "limited cron queries" honest: one cheap EXISTS against the
//! version column (same predicate as the delta filter, so passing it means the delta
//! is non-empty) before any building.

const std = @import("std");
const config = @import("config.zig");
const pg_conn = @import("pg_conn.zig");
const utils = @import("utils.zig");
const encoder_mod = @import("encoder.zig");
const nats = @import("nats");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;

const log = std.log.scoped(.generation_producer);

pub const GenerationProducer = struct {
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    should_stop: *std.atomic.Value(bool),
    io: std.Io,
    endpoint: config.Nats.Endpoint,
    /// table → list of tenants (GENERATION_RULES grammar, parsed like TENANT_RULES)
    rules: *const config.EventClassification.TransitionRules,
    /// table → [version_col, …] (SYNC_RULES); absent falls back to the default column
    sync_rules: *const config.EventClassification.TransitionRules,
    cadence_seconds: u64,
    chain_depth: u32,
    /// From topology.json `"generations".kv` — the manifest bucket.
    kv_bucket: []const u8,
    /// From topology.json `"generations".bucket_prefix` — `<prefix><tenant>` objects.
    bucket_prefix: []const u8,
    thread: ?std.Thread = null,

    pub fn init(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        should_stop: *std.atomic.Value(bool),
        io: std.Io,
        endpoint: config.Nats.Endpoint,
        rules: *const config.EventClassification.TransitionRules,
        sync_rules: *const config.EventClassification.TransitionRules,
        cadence_seconds: u64,
        chain_depth: u32,
        kv_bucket: []const u8,
        bucket_prefix: []const u8,
    ) GenerationProducer {
        return .{
            .allocator = allocator,
            .pg_config = pg_config,
            .should_stop = should_stop,
            .io = io,
            .endpoint = endpoint,
            .rules = rules,
            .sync_rules = sync_rules,
            .cadence_seconds = cadence_seconds,
            .chain_depth = chain_depth,
            .kv_bucket = kv_bucket,
            .bucket_prefix = bucket_prefix,
        };
    }

    pub fn start(self: *GenerationProducer) !void {
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    pub fn join(self: *GenerationProducer) void {
        if (self.thread) |t| t.join();
        self.thread = null;
    }

    fn run(self: *GenerationProducer) void {
        log.info("🧬 Generation producer started: {d} table(s), cadence {d}s, chain depth {d}", .{
            self.rules.count(), self.cadence_seconds, self.chain_depth,
        });
        // First tick immediately: an operator enabling generations should not wait a
        // full cadence to learn whether the configuration works.
        while (!self.should_stop.load(.acquire)) {
            self.tick() catch |err| log.err("🧬 generation tick failed: {}", .{err});
            var slept: u64 = 0;
            while (slept < self.cadence_seconds and !self.should_stop.load(.acquire)) : (slept += 1) {
                utils.sleep(1 * std.time.ns_per_s);
            }
        }
        log.info("🛑 Generation producer stopped", .{});
    }

    fn tick(self: *GenerationProducer) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        // Fresh connections per tick: at generation cadence the handshake cost is
        // noise, and a tick can never inherit a half-dead connection from the last one.
        const conninfo = try self.pg_config.connInfo(alloc, false);
        const pgc = c.PQconnectdb(conninfo.ptr) orelse return error.ConnectionFailed;
        defer c.PQfinish(pgc);
        if (c.PQstatus(pgc) != c.CONNECTION_OK) {
            log.err("🧬 PG connect failed: {s}", .{c.PQerrorMessage(pgc)});
            return error.ConnectionFailed;
        }

        var conn_nats = nats.Connection.init(self.allocator, self.io, .{
            .user = self.endpoint.user,
            .password = self.endpoint.pass,
            .nkey_seed = self.endpoint.seed,
        });
        defer conn_nats.deinit();
        const url = try std.fmt.allocPrint(alloc, "nats://{s}:{d}", .{ self.endpoint.host, self.endpoint.port });
        try conn_nats.connect(url);
        var js = conn_nats.jetstream(.{});

        var it = self.rules.iterator();
        while (it.next()) |entry| {
            const table = entry.key_ptr.*;
            const vcol = if (self.sync_rules.get(table)) |cols| cols[0] else config.Sync.default_version_column;
            for (entry.value_ptr.*) |tenant| {
                if (self.should_stop.load(.acquire)) return;
                self.buildOne(alloc, pgc, &js, table, tenant, vcol) catch |err| {
                    log.err("🧬 generation build failed for '{s}'/'{s}': {} — next cadence retries", .{ tenant, table, err });
                };
            }
        }
    }

    fn queryOne(pgc: *c.PGconn, sql: [:0]const u8, params: []const ?[*:0]const u8) !*c.PGresult {
        const res = c.PQexecParams(pgc, sql.ptr, @intCast(params.len), null, if (params.len > 0) params.ptr else null, null, null, 0) orelse return error.QueryFailed;
        const st = c.PQresultStatus(res);
        if (st != c.PGRES_TUPLES_OK and st != c.PGRES_COMMAND_OK) {
            log.err("🧬 query failed: {s}", .{c.PQerrorMessage(pgc)});
            c.PQclear(res);
            return error.QueryFailed;
        }
        return res;
    }

    /// msgpack `{columns, rows, gen, kind, cutoff, prev_cutoff?}` from a text-mode result.
    fn encodeContent(
        alloc: std.mem.Allocator,
        res: *c.PGresult,
        gen: i64,
        kind: []const u8,
        cutoff: []const u8,
        prev_cutoff: ?[]const u8,
        vcol: []const u8,
        out_rows: *usize,
    ) ![]const u8 {
        const nrows: usize = @intCast(c.PQntuples(res));
        const ncols: usize = @intCast(c.PQnfields(res));
        out_rows.* = nrows;

        var enc = encoder_mod.Encoder.init(alloc, .msgpack);
        var root = enc.createMap();
        var cols_arr = try enc.createArray(ncols);
        for (0..ncols) |i| {
            try cols_arr.setIndex(i, try enc.createString(std.mem.span(c.PQfname(res, @intCast(i)))));
        }
        try root.put(enc.allocator, "columns", cols_arr);
        var rows_arr = try enc.createArray(nrows);
        for (0..nrows) |r| {
            var row_arr = try enc.createArray(ncols);
            for (0..ncols) |col| {
                if (c.PQgetisnull(res, @intCast(r), @intCast(col)) == 1) {
                    try row_arr.setIndex(col, enc.createNull());
                } else {
                    try row_arr.setIndex(col, try enc.createString(std.mem.span(c.PQgetvalue(res, @intCast(r), @intCast(col)))));
                }
            }
            try rows_arr.setIndex(r, row_arr);
        }
        try root.put(enc.allocator, "rows", rows_arr);
        try root.put(enc.allocator, "gen", enc.createInt(gen));
        try root.put(enc.allocator, "kind", try enc.createString(kind));
        try root.put(enc.allocator, "cutoff", try enc.createString(cutoff));
        // The guard column for the client's version-guarded upsert — in-band, because the
        // schema descriptor does not carry version_column (a noted client-side gap) and
        // the producer is the one component that certainly knows it.
        try root.put(enc.allocator, "version_column", try enc.createString(vcol));
        if (prev_cutoff) |p| try root.put(enc.allocator, "prev_cutoff", try enc.createString(p));
        return try enc.encode(root);
    }

    fn buildOne(
        self: *GenerationProducer,
        alloc: std.mem.Allocator,
        pgc: *c.PGconn,
        js: *nats.JetStream,
        table: []const u8,
        tenant: []const u8,
        vcol: []const u8,
    ) !void {
        const table_z = try alloc.dupeZ(u8, table);
        const tenant_z = try alloc.dupeZ(u8, tenant);

        // ── last generation and last full, from the producer's own memory ────
        var last_gen: i64 = 0;
        var last_cutoff: ?[]const u8 = null;
        var last_full_gen: i64 = 0;
        {
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr };
            const res = try queryOne(pgc, "SELECT gen, cutoff_version::text FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 ORDER BY gen DESC LIMIT 1", &params);
            defer c.PQclear(res);
            if (c.PQntuples(res) > 0) {
                last_gen = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 0)), 10) catch 0;
                last_cutoff = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 1)));
            }
        }
        {
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr };
            const res = try queryOne(pgc, "SELECT gen FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND has_full ORDER BY gen DESC LIMIT 1", &params);
            defer c.PQclear(res);
            if (c.PQntuples(res) > 0) {
                last_full_gen = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 0)), 10) catch 0;
            }
        }

        // ── skip-if-unchanged: same predicate as the delta filter, so passing it
        // means the delta will be non-empty ──────────────────────────────────
        if (last_cutoff) |cut| {
            const cut_z = try alloc.dupeZ(u8, cut);
            const check = try utils.allocPrintZ(alloc,
                "SELECT EXISTS(SELECT 1 FROM \"{s}\" WHERE \"{s}\" > $1::timestamptz - interval '{s}')",
                .{ table, vcol, config.Sync.version_future_tolerance });
            const params = [_]?[*:0]const u8{cut_z.ptr};
            const res = try queryOne(pgc, check, &params);
            defer c.PQclear(res);
            if (std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, 0, 0)), "f")) {
                log.debug("🧬 '{s}'/'{s}': unchanged since g{d} — skipped", .{ tenant, table, last_gen });
                return;
            }
        }

        const gen = last_gen + 1;
        // A full at gen 1 (nothing to delta against, and the chain needs its jump-in
        // point), then refreshed BEFORE the last one can age out of the kept window
        // (gen > N − depth): rebuild at distance depth − 1 keeps it always inside.
        const build_delta = last_gen > 0;
        const build_full = last_full_gen == 0 or (gen - last_full_gen) >= @as(i64, self.chain_depth) - 1;

        // ── 1. LSN BEFORE the snapshot (overlap-never-gap) ───────────────────
        const lsn: []const u8 = blk: {
            const res = try queryOne(pgc, "SELECT pg_current_wal_lsn()::text", &.{});
            defer c.PQclear(res);
            break :blk try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 0)));
        };

        // ── 2. REPEATABLE READ + tenant scoping, same policy as snapshots ────
        {
            const res = try queryOne(pgc, "BEGIN ISOLATION LEVEL REPEATABLE READ", &.{});
            c.PQclear(res);
        }
        errdefer {
            const rb = c.PQexec(pgc, "ROLLBACK");
            c.PQclear(rb);
        }
        {
            // UTC for this transaction only: text-mode timestamptz then renders as
            // `YYYY-MM-DD HH:MM:SS.ffffff+00`, one character-level replace away from
            // the CDC wire format (`…T…Z`). Without this the session offset leaks into
            // artifacts and cutoffs, and the client's version guard — a STRING
            // comparison — would order `+02` text against `Z` text wrongly.
            const res = try queryOne(pgc, "SET LOCAL timezone TO 'UTC'", &.{});
            c.PQclear(res);
        }
        {
            const set_sql = "SELECT set_config('" ++ config.Sync.tenant_setting ++ "', $1, true)";
            const params = [_]?[*:0]const u8{tenant_z.ptr};
            const res = try queryOne(pgc, set_sql, &params);
            c.PQclear(res);
        }

        // cutoff_version from INSIDE the snapshot transaction (now() = txn start, the
        // instant both content reads are consistent with). The bookkeeping row is NOT
        // inserted yet — it becomes authoritative only once objects and manifest exist.
        const cutoff_version: []const u8 = blk: {
            const res = try queryOne(pgc, "SELECT now()::text", &.{});
            defer c.PQclear(res);
            break :blk try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 0)));
        };

        // ── 3. content: full and/or delta against the SAME snapshot ──────────
        var full_payload: ?[]const u8 = null;
        var full_rows: usize = 0;
        if (build_full) {
            const sql = try utils.allocPrintZ(alloc, "SELECT * FROM \"{s}\"", .{table});
            const res = try queryOne(pgc, sql, &.{});
            defer c.PQclear(res);
            full_payload = try encodeContent(alloc, res, gen, "full", cutoff_version, null, vcol, &full_rows);
        }
        var delta_payload: ?[]const u8 = null;
        var delta_rows: usize = 0;
        if (build_delta) {
            const sql = try utils.allocPrintZ(alloc,
                "SELECT * FROM \"{s}\" WHERE \"{s}\" > $1::timestamptz - interval '{s}'",
                .{ table, vcol, config.Sync.version_future_tolerance });
            const prev_z = try alloc.dupeZ(u8, last_cutoff.?);
            const params = [_]?[*:0]const u8{prev_z.ptr};
            const res = try queryOne(pgc, sql, &params);
            defer c.PQclear(res);
            delta_payload = try encodeContent(alloc, res, gen, "delta", cutoff_version, last_cutoff, vcol, &delta_rows);
        }
        {
            const res = try queryOne(pgc, "COMMIT", &.{});
            c.PQclear(res);
        }

        // ── 4. immutable objects first ───────────────────────────────────────
        const bucket = try std.fmt.allocPrint(alloc, "{s}{s}", .{ self.bucket_prefix, tenant });
        var osm = js.objectStoreManager();
        var store = osm.openStore(bucket) catch |err| blk: {
            if (err == error.StoreNotFound or err == error.StreamNotFound) {
                break :blk try osm.createStore(.{ .store_name = bucket, .description = "ZeBridge generations (NOTES.md §1.13)" });
            }
            return err;
        };
        defer store.deinit();
        if (full_payload) |p| {
            const name = try std.fmt.allocPrint(alloc, "{s}-g{d}-full", .{ table, gen });
            var r = try store.putBytes(name, p);
            r.deinit();
        }
        if (delta_payload) |p| {
            const name = try std.fmt.allocPrint(alloc, "{s}-g{d}-delta", .{ table, gen });
            var r = try store.putBytes(name, p);
            r.deinit();
        }

        // ── 5. the chain manifest, swapped last ──────────────────────────────
        // Built from the kept window's committed rows plus this generation in memory
        // (its row does not exist yet — see the ordering note above).
        var full_gen_m: i64 = 0;
        var full_cutoff_m: []const u8 = "";
        var deltas_json: std.ArrayList(u8) = .empty;
        {
            const keep_from = try utils.allocPrintZ(alloc, "{d}", .{gen - @as(i64, self.chain_depth)});
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, keep_from.ptr };
            const res = try queryOne(pgc,
                "SELECT gen, cutoff_version::text, COALESCE(prev_cutoff::text, ''), has_full " ++
                    "FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND gen > $3 ORDER BY gen", &params);
            defer c.PQclear(res);
            const n: usize = @intCast(c.PQntuples(res));
            for (0..n) |i| {
                const g = std.mem.span(c.PQgetvalue(res, @intCast(i), 0));
                const cutoff = std.mem.span(c.PQgetvalue(res, @intCast(i), 1));
                const prev = std.mem.span(c.PQgetvalue(res, @intCast(i), 2));
                const hasf = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, @intCast(i), 3)), "t");
                if (hasf) {
                    full_gen_m = std.fmt.parseInt(i64, g, 10) catch 0;
                    full_cutoff_m = try alloc.dupe(u8, cutoff);
                }
                if (prev.len > 0) {
                    const frag = try std.fmt.allocPrint(alloc,
                        "{s}{{\"gen\":{s},\"object\":\"{s}-g{s}-delta\",\"prev_cutoff\":\"{s}\",\"cutoff\":\"{s}\"}}",
                        .{ if (deltas_json.items.len > 0) "," else "", g, table, g, prev, cutoff });
                    try deltas_json.appendSlice(alloc, frag);
                }
            }
        }
        if (build_full) {
            full_gen_m = gen;
            full_cutoff_m = cutoff_version;
        }
        if (build_delta) {
            const frag = try std.fmt.allocPrint(alloc,
                "{s}{{\"gen\":{d},\"object\":\"{s}-g{d}-delta\",\"prev_cutoff\":\"{s}\",\"cutoff\":\"{s}\"}}",
                .{ if (deltas_json.items.len > 0) "," else "", gen, table, gen, last_cutoff.?, cutoff_version });
            try deltas_json.appendSlice(alloc, frag);
        }

        var kv = js.kvBucket(self.kv_bucket) catch blk: {
            var km = js.kvManager();
            break :blk try km.createBucket(.{ .bucket = self.kv_bucket, .history = 1 });
        };
        defer kv.deinit();
        const key = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ tenant, table });
        const manifest = try std.fmt.allocPrint(alloc,
            "{{\"gen\":{d},\"bucket\":\"{s}\",\"cutoff_version\":\"{s}\",\"cutoff_lsn\":\"{s}\"," ++
                "\"version_column\":\"{s}\"," ++
                "\"full\":{{\"gen\":{d},\"object\":\"{s}-g{d}-full\",\"cutoff\":\"{s}\"}},\"deltas\":[{s}]}}",
            .{ gen, bucket, cutoff_version, lsn, vcol, full_gen_m, table, full_gen_m, full_cutoff_m, deltas_json.items });
        _ = try kv.put(key, manifest, .{});

        // ── objects and manifest live: NOW the row becomes the producer's memory ──
        {
            const gen_str = try utils.allocPrintZ(alloc, "{d}", .{gen});
            const lsn_z = try alloc.dupeZ(u8, lsn);
            const cut_z = try alloc.dupeZ(u8, cutoff_version);
            const prev_z: ?[*:0]const u8 = if (last_cutoff) |p| (try alloc.dupeZ(u8, p)).ptr else null;
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, gen_str.ptr, cut_z.ptr, lsn_z.ptr, prev_z, if (build_full) "t" else "f" };
            const res = try queryOne(pgc,
                "INSERT INTO public.zebridge_generations (tenant, tbl, gen, cutoff_version, cutoff_lsn, prev_cutoff, has_full) " ++
                    "VALUES ($1, $2, $3, $4::timestamptz, $5::pg_lsn, $6::timestamptz, $7::boolean) " ++
                    "ON CONFLICT (tenant, tbl, gen) DO NOTHING", &params);
            c.PQclear(res);
        }

        // ── 6. prune past the chain depth: PG rows (authority), then objects ──
        if (gen > self.chain_depth) {
            const keep_from = try utils.allocPrintZ(alloc, "{d}", .{gen - @as(i64, self.chain_depth)});
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, keep_from.ptr };
            const res = try queryOne(pgc,
                "DELETE FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND gen <= $3 RETURNING gen", &params);
            defer c.PQclear(res);
            const pruned: usize = @intCast(c.PQntuples(res));
            for (0..pruned) |i| {
                const g = std.mem.span(c.PQgetvalue(res, @intCast(i), 0));
                // A generation has a delta, a full, or both; delete both names and let
                // the one that never existed 404 quietly.
                for ([_][]const u8{ "delta", "full" }) |kind| {
                    const old_name = try std.fmt.allocPrint(alloc, "{s}-g{s}-{s}", .{ table, g, kind });
                    store.delete(old_name) catch |err| {
                        if (err != error.ObjectNotFound) log.warn("🧬 could not delete pruned object {s}: {}", .{ old_name, err });
                    };
                }
            }
        }

        log.info("🧬 g{d} for '{s}'/'{s}': {s}{s}{s} → {s} (cutoff {s} @ {s})", .{
            gen,                                          tenant, table,
            if (build_delta) "delta" else "",             if (build_delta and build_full) "+" else "",
            if (build_full) "full" else "",               bucket, cutoff_version, lsn,
        });
        if (build_delta) log.debug("🧬   delta: {d} row(s), {d} bytes", .{ delta_rows, delta_payload.?.len });
        if (build_full) log.debug("🧬   full:  {d} row(s), {d} bytes", .{ full_rows, full_payload.?.len });
    }
};
